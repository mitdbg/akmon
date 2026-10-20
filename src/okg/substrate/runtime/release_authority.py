"""Candidate-side provenance checks for a launcher-verified release."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from okg.substrate.deployments.environment import load_environment
from okg.substrate.errors import SealedReleaseInvalid
from okg.substrate.release_activation import (
    environment_release_status,
    load_active_release,
)
from okg.substrate.sealed_release import (
    validate_environment_release_paths,
    verify_sealed_release,
)


_RELEASE_ENVIRONMENT_KEYS = frozenset(
    {
        "OKG_ACTIVE_RELEASE_POINTER",
        "OKG_DATABASE_IDENTITY",
        "OKG_DEFINITION_DIGEST",
        "OKG_DEPLOYMENT",
        "OKG_DSN",
        "OKG_ENVIRONMENT_DESCRIPTOR",
        "OKG_ENVIRONMENT_DESCRIPTOR_DIGEST",
        "OKG_ENVIRONMENT_ID",
        "OKG_ENVIRONMENT_TIER",
        "OKG_RELEASE_ARTIFACT_ID",
        "OKG_RELEASE_INPUT_ID",
        "OKG_RELEASE_ROOT",
    }
)
_SEALED_MARKERS = frozenset(
    {
        "OKG_ACTIVE_RELEASE_POINTER",
        "OKG_ENVIRONMENT_DESCRIPTOR",
        "OKG_ENVIRONMENT_ID",
        "OKG_RELEASE_ARTIFACT_ID",
        "OKG_RELEASE_INPUT_ID",
        "OKG_RELEASE_ROOT",
    }
)
WORKER_AUTHORITY_SCHEMA = "okg.worker_release_authority/v1"
SERVICE_PYCACHE_ROOT_NAME = "service-pycache"
_PYCACHE_PREFIX_OPTION = "pycache_prefix="


def service_pycache_root(state_root: str | Path) -> Path:
    """Return the private parent of every per-launch bytecode cache (#2861)."""
    return Path(os.path.abspath(Path(state_root) / SERVICE_PYCACHE_ROOT_NAME))


def split_bytecode_isolation(argv: Sequence[str]) -> tuple[list[str], str | None]:
    """Split the release launcher's bytecode flags from an interpreter argv.

    The launcher starts every sealed service as ``-I -B -X pycache_prefix=<dir>
    <role arguments>``. Only that exact leading shape is split off; any other
    placement leaves the argv whole, so it cannot match a configured role.
    """
    values = [str(item) for item in argv]
    if (
        len(values) >= 4
        and values[:3] == ["-I", "-B", "-X"]
        and values[3].startswith(_PYCACHE_PREFIX_OPTION)
    ):
        return ["-I", *values[4:]], values[3][len(_PYCACHE_PREFIX_OPTION):]
    return values, None


def launched_interpreter_arguments(
    arguments: Sequence[str],
    pycache_prefix: object,
    *,
    pycache_root: str | Path,
) -> list[str]:
    """Rebuild the exact interpreter argv the launcher executed.

    ``pycache_prefix`` must be one normalized, absolute directory directly
    under ``pycache_root``; the live argv is then the role arguments with
    exactly ``-B -X pycache_prefix=<that directory>`` after ``-I``. A missing
    prefix fails closed: every sealed launch carries one (#2861). Raises
    ``ValueError`` for any other shape.
    """
    values = [str(item) for item in arguments]
    if pycache_prefix is None:
        raise ValueError("authority receipt records no bytecode cache prefix")
    if not isinstance(pycache_prefix, str) or not pycache_prefix:
        raise ValueError("bytecode cache prefix is not a path")
    if os.path.normpath(pycache_prefix) != pycache_prefix or not os.path.isabs(
        pycache_prefix
    ):
        raise ValueError("bytecode cache prefix is not a normalized absolute path")
    if Path(pycache_prefix).parent != Path(os.path.abspath(pycache_root)):
        raise ValueError("bytecode cache prefix is outside the private cache root")
    if not values or values[0] != "-I":
        raise ValueError("bytecode-isolated arguments do not start with -I")
    return [
        "-I",
        "-B",
        "-X",
        f"{_PYCACHE_PREFIX_OPTION}{pycache_prefix}",
        *values[1:],
    ]


def receipt_launched_arguments(
    raw: Mapping[str, Any],
    arguments: Sequence[str],
    *,
    state_root: str | Path,
) -> list[str]:
    """Return the live argv an authority receipt promises, or raise ``ValueError``.

    A receipt without a string ``pycache_prefix`` is refused: no sealed
    service may run without the launcher's bytecode isolation.
    """
    prefix = raw.get("pycache_prefix")
    if not isinstance(prefix, str):
        raise ValueError("authority receipt records no bytecode cache prefix")
    return launched_interpreter_arguments(
        arguments,
        prefix,
        pycache_root=service_pycache_root(state_root),
    )


def verify_worker_release_authority(
    *,
    deployment: str,
    dsn: str,
    environ: Mapping[str, str] | None = None,
    executable: str | Path | None = None,
    package_file: str | Path | None = None,
    isolated: bool | None = None,
) -> dict[str, Any] | None:
    """Verify launcher/runtime roots, or return ``None`` for tracking mode."""
    values = dict(os.environ if environ is None else environ)
    markers = sorted(key for key in _SEALED_MARKERS if values.get(key))
    if not markers:
        return None
    missing = sorted(key for key in _RELEASE_ENVIRONMENT_KEYS if not values.get(key))
    if missing:
        raise SealedReleaseInvalid(
            "sealed runtime authority environment is incomplete",
            data={"expected": sorted(_RELEASE_ENVIRONMENT_KEYS), "observed": missing},
        )
    isolated_runtime = bool(sys.flags.isolated) if isolated is None else isolated
    if not isolated_runtime:
        raise SealedReleaseInvalid(
            "sealed runtime did not start in isolated Python mode",
            data={"expected": "python -I", "observed": sys.flags.isolated},
        )
    # #2861: a sealed worker runs only as the launcher starts it, with -B and
    # a private cache prefix right after -I. Refuse any other start, the way
    # the status check refuses its receipt.
    arguments, pycache_prefix = split_bytecode_isolation(sys.orig_argv[1:])
    if pycache_prefix is None:
        raise SealedReleaseInvalid(
            "sealed runtime did not start with the launcher's bytecode isolation",
            data={
                "expected": "python -I -B -X pycache_prefix=<dir>",
                "observed": list(sys.orig_argv[1:5]),
            },
        )
    if not sys.flags.dont_write_bytecode or sys.pycache_prefix != pycache_prefix:
        raise SealedReleaseInvalid(
            "sealed runtime bytecode flags disagree with the interpreter state",
            data={
                "expected": pycache_prefix,
                "observed": sys.pycache_prefix,
                "dont_write_bytecode": sys.flags.dont_write_bytecode,
            },
        )

    artifact_root = Path(values["OKG_RELEASE_ROOT"]).resolve()
    release = verify_sealed_release(
        artifact_root,
        expected_artifact_id=values["OKG_RELEASE_ARTIFACT_ID"],
    )
    if release.inputs.input_id != values["OKG_RELEASE_INPUT_ID"]:
        raise SealedReleaseInvalid(
            "runtime release input identity does not match the launcher",
            data={
                "expected": release.inputs.input_id,
                "observed": values["OKG_RELEASE_INPUT_ID"],
            },
        )
    if (release.repo_root / ".git").exists():
        raise SealedReleaseInvalid(
            "sealed runtime repo contains editable Git authority",
            data={"observed": str(release.repo_root / ".git")},
        )

    active_path = Path(values["OKG_ACTIVE_RELEASE_POINTER"])
    active = load_active_release(active_path)
    assert active is not None
    _expect(active, "artifact_id", release.artifact_id)
    _expect(active, "artifact_root", str(release.artifact_root))
    _expect(active, "input_id", release.inputs.input_id)
    _expect(active, "environment_id", values["OKG_ENVIRONMENT_ID"])
    _expect(active, "tier", values["OKG_ENVIRONMENT_TIER"])
    _expect(active, "graph_deployment_name", deployment)
    _expect(active, "definition_digest", values["OKG_DEFINITION_DIGEST"])

    descriptor_input = Path(values["OKG_ENVIRONMENT_DESCRIPTOR"])
    if descriptor_input.is_symlink():
        raise SealedReleaseInvalid(
            "runtime environment descriptor must not be a symlink",
            data={"observed": str(descriptor_input)},
        )
    descriptor = descriptor_input.resolve()
    _require_within(descriptor, release.repo_root, "environment descriptor")
    if not descriptor.is_file():
        raise SealedReleaseInvalid(
            "runtime environment descriptor is missing or not regular",
            data={"observed": str(descriptor)},
        )
    descriptor_relative = active.get("descriptor_relative")
    if (
        not isinstance(descriptor_relative, str)
        or descriptor != (release.repo_root / descriptor_relative).resolve()
    ):
        raise SealedReleaseInvalid(
            "runtime environment descriptor path differs from active release",
        )
    descriptor_file_digest = (
        f"sha256:{hashlib.sha256(descriptor.read_bytes()).hexdigest()}"
    )
    _expect(active, "descriptor_file_digest", descriptor_file_digest)
    _expect(
        active,
        "descriptor_digest",
        values["OKG_ENVIRONMENT_DESCRIPTOR_DIGEST"],
    )
    try:
        environment = load_environment(descriptor, environ=values)
    except Exception as exc:  # noqa: BLE001 - normalize candidate config errors.
        raise SealedReleaseInvalid(
            f"runtime environment descriptor is invalid: {exc}",
            data={"observed": str(descriptor)},
        ) from exc
    validate_environment_release_paths(environment, release.deployment_repo_root)
    _expect(active, "descriptor_digest", environment.descriptor_digest)
    _expect(active, "definition_digest", environment.definition_digest)
    if environment.id != values["OKG_ENVIRONMENT_ID"]:
        raise SealedReleaseInvalid("runtime environment id differs from launcher")
    if environment.graph_deployment_name != deployment:
        raise SealedReleaseInvalid("runtime graph deployment differs from launcher")
    release_definition_digest = active.get(
        "release_definition_digest", environment.definition_digest
    )
    if release.inputs.definition_digest != release_definition_digest:
        raise SealedReleaseInvalid("runtime definition differs from sealed release")
    if environment.release.artifact_store != release.artifact_root.parent:
        raise SealedReleaseInvalid("runtime artifact store differs from sealed release")
    if environment.release.active_pointer != active_path.resolve():
        raise SealedReleaseInvalid("runtime active pointer differs from environment")

    database = active.get("database")
    if not isinstance(database, Mapping):
        raise SealedReleaseInvalid("active release database binding is missing")
    _expect(database, "name", environment.database.name)
    _expect(database, "dbos_identity", environment.database.dbos_identity)
    _expect(database, "dbos_identity", values["OKG_DATABASE_IDENTITY"])
    dsn_env = database.get("dsn_env")
    if not isinstance(dsn_env, str) or values.get(dsn_env) != dsn:
        raise SealedReleaseInvalid(
            "runtime DSN does not match the active environment secret",
        )
    if environment.database.dsn != dsn or values["OKG_DSN"] != dsn:
        raise SealedReleaseInvalid("runtime DSN differs from environment authority")

    executable_path = Path(executable or sys.executable).resolve()
    _require_within(executable_path, release.venv_root, "sys.executable")
    if package_file is None:
        import okg

        package_file = okg.__file__
    if package_file is None:
        raise SealedReleaseInvalid("okg package has no import root")
    package_path = Path(package_file).resolve()
    _require_within(package_path, release.venv_root, "okg.__file__")
    if package_path.is_relative_to(release.repo_root):
        raise SealedReleaseInvalid("editable okg package root is not launchable")
    if load_active_release(active_path) != active:
        raise SealedReleaseInvalid(
            "active release changed during worker authority verification",
            data={"observed": str(active_path)},
        )
    if not release.inputs.deployment_revision or not release.inputs.deployment_tree_sha:
        raise SealedReleaseInvalid(
            "sealed release repository provenance is unavailable",
            data={"artifact_id": release.artifact_id},
        )

    return {
        "mode": "sealed_release",
        "environment_id": environment.id,
        "tier": environment.tier,
        "deployment": deployment,
        "database": environment.database.name,
        "database_identity": environment.database.dbos_identity,
        "artifact_id": release.artifact_id,
        "artifact_root": str(release.artifact_root),
        "input_id": release.inputs.input_id,
        "code_revision": release.inputs.code_revision,
        "deployment_revision": release.inputs.deployment_revision,
        "deployment_tree_sha": release.inputs.deployment_tree_sha,
        "executable_tree_digest": release.executable_tree_digest,
        "executable": str(executable_path),
        "isolated": isolated_runtime,
        "package_file": str(package_path),
        "descriptor_path": str(descriptor),
        "definition_root": str(environment.manifest.deployment_dir),
        "sync_root": (
            str(environment.manifest.sync_root)
            if environment.manifest.sync_root is not None
            else None
        ),
        "definition_digest": environment.definition_digest,
        "active_pointer": str(active_path.resolve()),
        "authority_path": str(
            environment.runtime.state_root / "worker-authority.json"
        ),
        "arguments": arguments,
        "pycache_prefix": pycache_prefix,
        "pid": os.getpid(),
        "process_start_identity": process_start_identity(os.getpid()),
    }


def write_worker_release_authority(authority: Mapping[str, Any]) -> Path:
    """Persist the candidate's verified roots for supervisor status checks."""
    path = Path(str(authority.get("authority_path") or "")).expanduser()
    if not path.is_absolute():
        raise SealedReleaseInvalid("worker authority path is not absolute")
    payload = {
        "schema": WORKER_AUTHORITY_SCHEMA,
        "verified_at": datetime.now(timezone.utc).isoformat(),
        **dict(authority),
    }
    _atomic_write_json(path, payload)
    return path


def environment_worker_authority_status(
    environment: Any,
    *,
    environ: Mapping[str, str] | None = None,
    release_status: Mapping[str, Any] | None = None,
    expected_arguments: Sequence[str] | None = None,
    expected_pid: int | None = None,
) -> dict[str, Any]:
    """Validate the live worker's persisted sealed-root authority."""
    path = environment.runtime.state_root / "worker-authority.json"
    observed_release = (
        dict(release_status)
        if release_status is not None
        else environment_release_status(environment, environ=environ)
    )
    if not path.exists():
        return {
            "schema": "okg.environment_worker_authority_status/v1",
            "ok": False,
            "status": "missing",
            "environment_id": environment.id,
            "path": str(path),
            "release_status": observed_release,
        }
    reason_code: str | None = None
    try:
        if path.is_symlink() or not path.is_file():
            raise SealedReleaseInvalid("worker authority record is not regular")
        if stat.S_IMODE(path.stat().st_mode) != 0o600:
            raise SealedReleaseInvalid("worker authority record mode is not 0600")
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict) or raw.get("schema") != WORKER_AUTHORITY_SCHEMA:
            raise SealedReleaseInvalid("worker authority record has the wrong schema")
        if not observed_release.get("ok"):
            raise SealedReleaseInvalid("active release status is not valid")
        release_definition_digest = observed_release.get("definition_digest")
        if not _is_sha256_digest(release_definition_digest):
            raise SealedReleaseInvalid(
                "worker authority release definition digest is missing"
            )
        expected = {
            "environment_id": environment.id,
            "tier": environment.tier,
            "deployment": environment.graph_deployment_name,
            "database": environment.database.name,
            "database_identity": environment.database.dbos_identity,
            "artifact_id": observed_release.get("artifact_id"),
            "artifact_root": observed_release.get("artifact_root"),
            "definition_digest": release_definition_digest,
            "active_pointer": str(environment.release.active_pointer),
        }
        for field, wanted in expected.items():
            if raw.get(field) != wanted:
                raise SealedReleaseInvalid(
                    f"worker authority mismatch for {field}",
                    data={"expected": wanted, "observed": raw.get(field)},
                )
        artifact_root = Path(str(raw["artifact_root"])).resolve()
        for field in ("executable", "package_file", "definition_root"):
            candidate = Path(str(raw.get(field) or "")).resolve()
            _require_within(candidate, artifact_root, field)
        if not raw.get("isolated"):
            raise SealedReleaseInvalid("worker authority is not isolated")
        pid = raw.get("pid")
        if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
            raise SealedReleaseInvalid("worker authority pid is invalid")
        receipt_start = raw.get("process_start_identity")
        if not isinstance(receipt_start, str) or not receipt_start:
            raise SealedReleaseInvalid(
                "worker authority process start identity is invalid"
            )
        arguments = raw.get("arguments")
        if not isinstance(arguments, list) or not all(
            isinstance(item, str) for item in arguments
        ):
            raise SealedReleaseInvalid("worker authority arguments are invalid")
        if expected_arguments is not None and arguments != list(expected_arguments):
            raise SealedReleaseInvalid(
                "worker authority arguments do not match the configured worker role"
            )
        if not pid_alive(pid):
            reason_code = "worker_process_not_alive"
            raise SealedReleaseInvalid("worker authority process is not alive")
        observed_start = process_start_identity(pid)
        if not observed_start or observed_start != receipt_start:
            raise SealedReleaseInvalid("worker authority process identity changed")
        if expected_pid is not None and pid != expected_pid:
            raise SealedReleaseInvalid(
                "worker authority pid differs from the supervised worker process"
            )
        runtime_identity = process_runtime_identity(pid)
        if runtime_identity is None:
            raise SealedReleaseInvalid(
                "worker authority process runtime identity is unavailable"
            )
        observed_executable = Path(runtime_identity["executable"]).resolve()
        expected_executable = Path(str(raw["executable"])).resolve()
        if observed_executable != expected_executable:
            raise SealedReleaseInvalid(
                "worker authority process executable differs from sealed runtime"
            )
        try:
            launched_arguments = receipt_launched_arguments(
                raw,
                arguments,
                state_root=environment.runtime.state_root,
            )
        except ValueError as exc:
            raise SealedReleaseInvalid(
                f"worker authority bytecode isolation is invalid: {exc}"
            ) from exc
        if runtime_identity["argv"][1:] != launched_arguments:
            raise SealedReleaseInvalid(
                "worker authority process arguments differ from sealed runtime"
            )
        if process_start_identity(pid) != receipt_start:
            raise SealedReleaseInvalid(
                "worker authority process identity changed during readback"
            )
        return {
            "schema": "okg.environment_worker_authority_status/v1",
            "ok": True,
            "status": "healthy",
            "environment_id": environment.id,
            "path": str(path),
            "release_status": observed_release,
            "authority": raw,
        }
    except Exception as exc:  # noqa: BLE001 - status preserves the failure.
        status = {
            "schema": "okg.environment_worker_authority_status/v1",
            "ok": False,
            "status": "invalid",
            "environment_id": environment.id,
            "path": str(path),
            "release_status": observed_release,
            "error": type(exc).__name__,
            "message": str(exc),
        }
        if reason_code is not None:
            status["reason_code"] = reason_code
        return status


def _is_sha256_digest(value: Any) -> bool:
    if not isinstance(value, str) or not value.startswith("sha256:"):
        return False
    digest = value.removeprefix("sha256:")
    return len(digest) == 64 and all(char in "0123456789abcdef" for char in digest)


def _expect(raw: Mapping[str, Any], field: str, expected: Any) -> None:
    observed = raw.get(field)
    if observed != expected:
        raise SealedReleaseInvalid(
            f"runtime release binding mismatch for {field}",
            data={"expected": expected, "observed": observed},
        )


def _require_within(path: Path, root: Path, label: str) -> None:
    try:
        path.relative_to(root.resolve())
    except ValueError as exc:
        raise SealedReleaseInvalid(
            f"{label} is outside the sealed release",
            data={"expected": str(root), "observed": str(path)},
        ) from exc


def process_start_identity(pid: int) -> str | None:
    stat_path = Path(f"/proc/{pid}/stat")
    if stat_path.is_file():
        try:
            remainder = stat_path.read_text(encoding="utf-8").rsplit(")", 1)[1]
            fields = remainder.split()
            return f"proc:{fields[19]}"
        except (OSError, IndexError):
            return None
    try:
        proc = subprocess.run(
            ["ps", "-p", str(pid), "-o", "lstart="],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    value = proc.stdout.strip()
    return f"ps:{value}" if proc.returncode == 0 and value else None


# A launch returns as soon as the kernel closes the child's close-on-exec pipe in
# `begin_new_exec()`, but the child's `mm->arg_start`/`arg_end` are only set later,
# in `create_elf_tables()`. In that window `/proc/<pid>/exe` already resolves to the
# new image while `/proc/<pid>/cmdline` reads zero bytes. Measured on Linux 5.15 the
# window opens on 2.2% of spawns under two-core contention and stays open as long as
# 58ms, so a single read reports a live process as having no identity at all. Wait
# the window out instead of claiming an absence we have not observed. The budget is
# ~24x the measured worst case. It is normally spent only mid-exec, but a live
# process whose argv region is genuinely empty (an argv-rewriting daemon that
# cleared its title) does spend all of it before failing closed.
_MID_EXEC_ARGV_DEADLINE_SECONDS = 2.0
_MID_EXEC_ARGV_POLL_SECONDS = 0.002


def _denotes_dead_process(argv: list[str]) -> bool:
    """Report whether `ps` described a process with no live command line.

    `ps` prints a process's *status* in place of its argv once there is no argv
    left to print: a zombie appears as `[comm] <defunct>` — a trailing
    `<defunct>` marker — and a kernel thread as `[kworker/0:1]`, a bracketed
    comm and nothing else. Match those two shapes rather than a `<defunct>`
    token anywhere, so a live command line that merely carries the literal
    argument `<defunct>` mid-line still resolves. argv is non-empty here.
    """
    if argv[-1] == "<defunct>":
        return True
    # A bracketed argv[0] is ps's placeholder for a comm with no argv. A real
    # executable named `[` (the test builtin) is not caught: ps prints its argv,
    # where the closing `]` is a separate trailing argument, so argv[0] is `[`.
    return argv[0].startswith("[") and argv[0].endswith("]")


def _proc_runtime_identity(
    pid: int,
    executable_path: Path,
    command_path: Path,
) -> dict[str, Any] | None:
    """Read a live identity from /proc, waiting out the mid-exec argv window."""
    incarnation = process_start_identity(pid)
    deadline = time.monotonic() + _MID_EXEC_ARGV_DEADLINE_SECONDS
    waited = False
    while True:
        try:
            executable = str(executable_path.resolve(strict=True))
            argv = [
                token.decode("utf-8", errors="surrogateescape")
                for token in command_path.read_bytes().split(b"\0")
                if token
            ]
        except OSError:
            # The process is gone, or its /proc entry is unreadable. Either way the
            # identity is absent rather than pending, so fail closed immediately.
            return None
        if argv:
            if waited and process_start_identity(pid) != incarnation:
                # The pid was recycled while we waited, so this argv belongs to a
                # different process than the one the caller proved alive. Vouching
                # for it would answer a question nobody asked.
                return None
            return {"executable": executable, "argv": argv}
        # Empty argv behind a resolvable `exe` means the kernel has not published
        # this process's argv yet. `exe` is re-resolved every pass, so a process
        # that exits mid-wait drops its mm and ends the wait at the branch above
        # rather than spinning here to the deadline.
        if time.monotonic() >= deadline:
            return None
        waited = True
        time.sleep(_MID_EXEC_ARGV_POLL_SECONDS)


def process_runtime_identity(pid: int) -> dict[str, Any] | None:
    """Return the live executable and argv for a process without trusting a receipt."""
    executable_path = Path(f"/proc/{pid}/exe")
    command_path = Path(f"/proc/{pid}/cmdline")
    try:
        proc_readable = executable_path.exists() and command_path.is_file()
    except OSError:
        # The /proc entry exists but will not describe itself, which is what a
        # process owned by another user looks like. That is an identity we cannot
        # read, not one we can answer from `ps`, whose executable is derived from
        # a spoofable argv[0]. Decline rather than guess.
        return None
    if proc_readable:
        return _proc_runtime_identity(pid, executable_path, command_path)

    if sys.platform == "darwin":
        return _darwin_process_runtime_identity(pid)

    try:
        proc = subprocess.run(
            ["ps", "-ww", "-p", str(pid), "-o", "command="],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    command = proc.stdout.strip()
    if proc.returncode != 0 or not command:
        return None
    try:
        import shlex

        argv = shlex.split(command, posix=True)
    except ValueError:
        return None
    if not argv or _denotes_dead_process(argv):
        return None
    return {"executable": str(Path(argv[0]).resolve()), "argv": argv}


def _darwin_process_runtime_identity(pid: int) -> dict[str, Any] | None:
    """Read lossless argv from macOS KERN_PROCARGS2, failing closed on denial."""
    import ctypes

    ctl_kern = 1
    kern_procargs2 = 49
    mib = (ctypes.c_int * 3)(ctl_kern, kern_procargs2, pid)
    size = ctypes.c_size_t()
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.sysctl(mib, 3, None, ctypes.byref(size), None, 0) != 0 or size.value < 5:
        return None
    buffer = ctypes.create_string_buffer(size.value)
    if (
        libc.sysctl(
            mib,
            3,
            buffer,
            ctypes.byref(size),
            None,
            0,
        )
        != 0
    ):
        return None
    data = buffer.raw[: size.value]
    argc = int.from_bytes(
        data[: ctypes.sizeof(ctypes.c_int)],
        byteorder=sys.byteorder,
        signed=True,
    )
    if argc <= 0:
        return None
    payload = data[ctypes.sizeof(ctypes.c_int) :]
    executable_end = payload.find(b"\0")
    if executable_end <= 0:
        return None
    executable = payload[:executable_end].decode(
        "utf-8",
        errors="surrogateescape",
    )
    cursor = executable_end
    while cursor < len(payload) and payload[cursor] == 0:
        cursor += 1
    argv: list[str] = []
    for _ in range(argc):
        end = payload.find(b"\0", cursor)
        if end < 0:
            return None
        argv.append(
            payload[cursor:end].decode("utf-8", errors="surrogateescape")
        )
        cursor = end + 1
    if not argv:
        return None
    return {"executable": str(Path(executable).resolve()), "argv": argv}


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        if stat.S_IMODE(path.stat().st_mode) != 0o600:
            raise SealedReleaseInvalid("worker authority mode is not 0600")
    finally:
        temporary.unlink(missing_ok=True)
