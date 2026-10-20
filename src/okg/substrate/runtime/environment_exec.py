"""Stdlib-only env-file loader for launchd service commands."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import re
import secrets
import shlex
import signal
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Mapping, Sequence


_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SERVICE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_LAUNCHD_SUPERVISION_SCHEMA = "okg.launchd_service_supervision/v1"


def load_environment_file(path: str | Path) -> dict[str, str]:
    target = Path(path).expanduser()
    if target.is_symlink() or not target.is_file():
        raise ValueError("environment file is missing or not regular")
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(
        target.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        tokens = shlex.split(line, comments=False, posix=True)
        if len(tokens) != 1 or "=" not in tokens[0]:
            raise ValueError(f"invalid environment assignment on line {line_number}")
        name, value = tokens[0].split("=", 1)
        if _NAME.fullmatch(name) is None:
            raise ValueError(f"invalid environment name on line {line_number}")
        values[name] = value
    return values


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="okg-environment-exec")
    parser.add_argument("--env-file", required=True)
    parser.add_argument("--supervision-receipt")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        print("error: command is required", file=sys.stderr)
        return 2
    try:
        loaded = load_environment_file(args.env_file)
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    environment = dict(os.environ)
    environment.update(loaded)
    if args.supervision_receipt:
        try:
            return _run_supervised_launchd_process(
                command,
                environment=environment,
                receipt_path=Path(args.supervision_receipt),
            )
        except (OSError, ValueError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
    os.execvpe(command[0], command, environment)
    return 0


def _run_supervised_launchd_process(
    command: Sequence[str],
    *,
    environment: Mapping[str, str],
    receipt_path: Path,
) -> int:
    """Proxy launchd signals and persist the exact child exit outcome."""
    service_id = environment.get("OKG_SERVICE_ID", "")
    if _SERVICE_ID.fullmatch(service_id) is None:
        raise ValueError("OKG_SERVICE_ID is invalid")
    if not receipt_path.is_absolute():
        raise ValueError("supervision receipt path must be absolute")
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    if receipt_path.parent.is_symlink():
        raise ValueError("supervision receipt directory must not be a symlink")
    if receipt_path.exists():
        if receipt_path.is_symlink() or not receipt_path.is_file():
            raise ValueError("supervision receipt must be a regular file")
        receipt_path.unlink()

    pending_signals: list[int] = []
    child: subprocess.Popen[bytes] | None = None

    def forward(signum: int, _frame: object) -> None:
        if child is None:
            pending_signals.append(signum)
            return
        try:
            child.send_signal(signum)
        except ProcessLookupError:
            pass

    prior_handlers = {
        signum: signal.signal(signum, forward)
        for signum in (signal.SIGTERM, signal.SIGINT)
    }
    nonce = secrets.token_urlsafe(32)
    child_environment = dict(environment)
    child_environment["OKG_SERVICE_LIFECYCLE_NONCE"] = nonce
    try:
        child = subprocess.Popen(list(command), env=child_environment)
        child_start = _wait_for_process_start_identity(child.pid)
        supervisor_start = _process_start_identity(os.getpid())
        if child_start is None or supervisor_start is None:
            raise ValueError("launchd process start identity is unavailable")
        running = {
            "schema": _LAUNCHD_SUPERVISION_SCHEMA,
            "state": "running",
            "service_id": service_id,
            "lifecycle_nonce": nonce,
            "supervisor_pid": os.getpid(),
            "supervisor_process_start_identity": supervisor_start,
            "child_pid": child.pid,
            "child_process_start_identity": child_start,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "exit_code": None,
            "term_signal": None,
        }
        _atomic_write_json(receipt_path, running)
        for signum in pending_signals:
            child.send_signal(signum)
        returncode = child.wait()
        terminal = {
            **running,
            "state": "exited",
            "exited_at": datetime.now(timezone.utc).isoformat(),
            "exit_code": returncode if returncode >= 0 else None,
            "term_signal": -returncode if returncode < 0 else None,
        }
        _atomic_write_json(receipt_path, terminal)
        return returncode if returncode >= 0 else 128 - returncode
    except BaseException as exc:
        if child is not None:
            try:
                _terminate_and_reap_child(child)
            except BaseException as cleanup_exc:
                exc.add_note(
                    "launchd child cleanup also failed: "
                    f"{type(cleanup_exc).__name__}: {cleanup_exc}"
                )
        raise
    finally:
        for signum, handler in prior_handlers.items():
            signal.signal(signum, handler)


def _terminate_and_reap_child(child: subprocess.Popen[bytes]) -> None:
    """Stop an exact wrapper child and wait until it cannot be orphaned."""
    if child.poll() is not None:
        child.wait()
        return
    try:
        child.terminate()
    except ProcessLookupError:
        child.wait(timeout=5)
        return
    try:
        child.wait(timeout=5)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait(timeout=5)


def _wait_for_process_start_identity(pid: int) -> str | None:
    deadline = time.monotonic() + 2.0
    while True:
        identity = _process_start_identity(pid)
        if identity is not None:
            return identity
        if time.monotonic() >= deadline:
            return None
        time.sleep(0.002)


def _process_start_identity(pid: int) -> str | None:
    stat_path = Path(f"/proc/{pid}/stat")
    if stat_path.is_file():
        try:
            remainder = stat_path.read_text(encoding="utf-8").rsplit(")", 1)[1]
            fields = remainder.split()
            return f"proc:{fields[19]}"
        except (OSError, IndexError):
            return None
    completed = subprocess.run(
        ["ps", "-p", str(pid), "-o", "lstart="],
        check=False,
        capture_output=True,
        text=True,
    )
    value = completed.stdout.strip()
    return f"ps:{value}" if completed.returncode == 0 and value else None


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, path)
        if stat.S_IMODE(path.stat().st_mode) != 0o600:
            raise ValueError("supervision receipt mode is invalid")
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
