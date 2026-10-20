"""A stopped ``okg mcp-serve`` never leaves its connection claim behind.

PACT lane-h-mcp-serve-stop-guard. A stdio server is stopped by closing stdin
and then SIGTERM. About 1 stop in 50-100, asyncio.run() teardown never
finishes (a cancelled task is never woken), the client SIGKILLs after 20 s,
run_server's ``finally`` never runs, and the 37-connection claim in
``okg.connection_reservations`` stays until the 300 s stale sweep. Two such
claims refuse the next start.

These tests drive the real CLI in a subprocess over stdio against a real
PostgreSQL database:

* a forced stuck teardown (``_stuck_teardown_serve.py``) plus SIGTERM: the
  claim row for that pid is gone within about a second, and the process exits
  with 143 within the guard's bound plus a margin;
* the same forced hang with the guard's bound raised out of reach: the claim
  is still released early, but the process is still alive after the bound,
  which proves the forced hang is real and only the guard ends it;
* SIGTERM while stdin is still open: the claim goes at once and the process
  exits 143 within the bound (interpreter exit can wait on the worker thread
  blocked reading stdin; the guard bounds that too);
* the normal stop (stdin close, then SIGTERM): fast exit, claim released, no
  watchdog exit.
"""

from __future__ import annotations

import json
import os
import selectors
import signal
import subprocess
import sys
import time
from pathlib import Path

import psycopg
import pytest

from okg.substrate.mcp.server import (
    MCP_STOP_GUARD_EXIT_MESSAGE,
    MCP_STOP_GUARD_SECONDS,
)

pytestmark = pytest.mark.postgres

_REPO_ROOT = Path(__file__).resolve().parents[3]
_LAUNCHER = Path(__file__).with_name("_stuck_teardown_serve.py")
_DEPLOYMENT = "stop-guard"
_APPLICATION_NAME = f"okg-mcp-{_DEPLOYMENT}"
_START_TIMEOUT_SECONDS = 120.0
#: Room for a loaded CI host on top of the 5 s guard bound.
_EXIT_MARGIN_SECONDS = 4.0
#: A normal stop takes about 0.3 s; anything near the guard bound is a regression.
_FAST_EXIT_SECONDS = 3.0
#: How long the claim may outlive SIGTERM.
_RELEASE_WITHIN_SECONDS = 1.5
_SIGTERM_EXIT = 128 + signal.SIGTERM


@pytest.fixture
def stop_guard_dsn(clean_runtime: str) -> str:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("DELETE FROM okg.connection_reservations")
    return clean_runtime


@pytest.fixture
def deployments_root(tmp_path: Path) -> Path:
    """A minimal deployment the CLI resolves; the database comes from --dsn."""
    root = tmp_path / "deployments"
    deployment = root / _DEPLOYMENT
    (deployment / "schemas").mkdir(parents=True)
    (deployment / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    (deployment / "deployment.yaml").write_text(
        f"name: {_DEPLOYMENT}\n"
        "description: stop-guard regression test\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n",
        encoding="utf-8",
    )
    return root


def _reservation_count(dsn: str, pid: int) -> int:
    # okg.connection_reservations lives in each database; this one is the
    # test's own, so parallel workers never see each other's rows.
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT count(*) FROM okg.connection_reservations "
            " WHERE application_name = %s AND pid = %s",
            (_APPLICATION_NAME, pid),
        ).fetchone()
    return int(row[0])


def _start(
    dsn: str,
    log_path: Path,
    deployments_root: Path,
    *,
    launcher: bool,
    stuck_task: bool = True,
    guard_disabled: bool = False,
    blocking_release: bool = False,
) -> subprocess.Popen:
    entry = [str(_LAUNCHER)] if launcher else ["-m", "okg.substrate.cli.main"]
    argv = [
        sys.executable,
        *entry,
        "mcp-serve",
        "--deployment",
        _DEPLOYMENT,
        "--dsn",
        dsn,
    ]
    env = dict(os.environ)
    # A fresh test database has no grants for the least-privilege okg_mcp
    # role, so the read pool uses the test superuser, as the other
    # real-process MCP tests do. The reservation and its release are the same.
    env["OKG_MCP_RO_DSN"] = dsn
    env["OKG_MCP_TRACE_ENABLED"] = "0"
    env["OKG_DEPLOYMENTS_DIR"] = str(deployments_root)
    env.pop("OKG_TEST_STOP_GUARD_DISABLED", None)
    env.pop("OKG_TEST_STOP_GUARD_NO_STUCK_TASK", None)
    env.pop("OKG_TEST_STOP_GUARD_BLOCKING_RELEASE", None)
    if blocking_release:
        env["OKG_TEST_STOP_GUARD_BLOCKING_RELEASE"] = "1"
    if guard_disabled:
        env["OKG_TEST_STOP_GUARD_DISABLED"] = "1"
    if not stuck_task:
        env["OKG_TEST_STOP_GUARD_NO_STUCK_TASK"] = "1"
    with log_path.open("wb") as log_handle:
        # start_new_session: the child can never signal the xdist worker.
        return subprocess.Popen(  # noqa: S603
            argv,
            cwd=str(_REPO_ROOT),
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log_handle,
            start_new_session=True,
        )


def _log(log_path: Path) -> str:
    try:
        return log_path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return "<no log>"


def _initialize(proc: subprocess.Popen, log_path: Path) -> None:
    """Complete the MCP handshake, so the stdio run loop is live."""
    assert proc.stdin is not None and proc.stdout is not None
    request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "stop-guard-test", "version": "0"},
        },
    }
    proc.stdin.write((json.dumps(request) + "\n").encode())
    proc.stdin.flush()
    selector = selectors.DefaultSelector()
    selector.register(proc.stdout, selectors.EVENT_READ)
    deadline = time.monotonic() + _START_TIMEOUT_SECONDS
    received = b""
    try:
        while b"\n" not in received:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not selector.select(remaining):
                raise AssertionError(
                    "okg mcp-serve did not answer initialize; exit="
                    f"{proc.poll()}; log tail:\n{_log(log_path)[-4000:]}"
                )
            chunk = os.read(proc.stdout.fileno(), 65536)
            if not chunk:
                raise AssertionError(
                    f"okg mcp-serve closed stdout (exit={proc.poll()}); "
                    f"log tail:\n{_log(log_path)[-4000:]}"
                )
            received += chunk
    finally:
        selector.close()
    response = json.loads(received.split(b"\n", 1)[0])
    assert response.get("id") == 1 and "result" in response, response
    notice = {"jsonrpc": "2.0", "method": "notifications/initialized"}
    proc.stdin.write((json.dumps(notice) + "\n").encode())
    proc.stdin.flush()


def _seconds_until_row_gone(dsn: str, pid: int) -> float | None:
    started = time.monotonic()
    while time.monotonic() - started < _RELEASE_WITHIN_SECONDS:
        if _reservation_count(dsn, pid) == 0:
            return time.monotonic() - started
        time.sleep(0.05)
    return None


def _reap(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.kill()
    proc.wait(timeout=20)
    for stream in (proc.stdin, proc.stdout):
        if stream is not None:
            try:
                stream.close()
            except OSError:
                pass


def _sigterm_and_expect_guard_exit(
    dsn: str, proc: subprocess.Popen, log_path: Path
) -> None:
    """SIGTERM a hanging server: claim gone at once, exit 143 at the bound."""
    _initialize(proc, log_path)
    assert _reservation_count(dsn, proc.pid) == 1
    sent = time.monotonic()
    proc.send_signal(signal.SIGTERM)
    assert _seconds_until_row_gone(dsn, proc.pid) is not None, (
        f"the claim row outlived SIGTERM by {_RELEASE_WITHIN_SECONDS}s; "
        f"log tail:\n{_log(log_path)[-4000:]}"
    )
    try:
        code = proc.wait(timeout=MCP_STOP_GUARD_SECONDS + _EXIT_MARGIN_SECONDS)
    except subprocess.TimeoutExpired:
        raise AssertionError(
            "the stop guard did not end the hanging process; log tail:\n"
            + _log(log_path)[-4000:]
        ) from None
    elapsed = time.monotonic() - sent
    log_text = _log(log_path)
    assert code == _SIGTERM_EXIT, (code, log_text[-4000:])
    # The watchdog, not a normal exit, ended this process, after the bound,
    # and left every thread's stack for diagnosis.
    assert MCP_STOP_GUARD_EXIT_MESSAGE in log_text, log_text[-4000:]
    assert "most recent call first" in log_text, log_text[-4000:]
    assert elapsed >= MCP_STOP_GUARD_SECONDS - 0.5, elapsed
    assert _reservation_count(dsn, proc.pid) == 0


def _sigterm_and_expect_hang(dsn: str, proc: subprocess.Popen, log_path: Path) -> None:
    """With the guard's bound out of reach, the process outlives the bound."""
    _initialize(proc, log_path)
    proc.send_signal(signal.SIGTERM)
    # The early release does not depend on the bound.
    assert _seconds_until_row_gone(dsn, proc.pid) is not None, _log(log_path)[-4000:]
    with pytest.raises(subprocess.TimeoutExpired):
        proc.wait(timeout=MCP_STOP_GUARD_SECONDS + _EXIT_MARGIN_SECONDS)
    assert proc.poll() is None, "the hang did not reproduce"


def test_stuck_teardown_releases_claim_at_once_and_exits_143(
    stop_guard_dsn: str, deployments_root: Path, tmp_path: Path
) -> None:
    log_path = tmp_path / "mcp-serve.log"
    proc = _start(stop_guard_dsn, log_path, deployments_root, launcher=True)
    try:
        _sigterm_and_expect_guard_exit(stop_guard_dsn, proc, log_path)
    finally:
        _reap(proc)


def test_stuck_teardown_without_guard_stays_alive_past_the_bound(
    stop_guard_dsn: str, deployments_root: Path, tmp_path: Path
) -> None:
    log_path = tmp_path / "mcp-serve.log"
    proc = _start(
        stop_guard_dsn,
        log_path,
        deployments_root,
        launcher=True,
        guard_disabled=True,
    )
    try:
        _sigterm_and_expect_hang(stop_guard_dsn, proc, log_path)
    finally:
        _reap(proc)
    assert MCP_STOP_GUARD_EXIT_MESSAGE not in _log(log_path)


def test_blocked_release_does_not_move_the_deadline(
    stop_guard_dsn: str, deployments_root: Path, tmp_path: Path
) -> None:
    """A release that hangs (a paused database) still exits 143 on time."""
    log_path = tmp_path / "mcp-serve.log"
    proc = _start(
        stop_guard_dsn,
        log_path,
        deployments_root,
        launcher=True,
        blocking_release=True,
    )
    try:
        _initialize(proc, log_path)
        sent = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        try:
            code = proc.wait(timeout=MCP_STOP_GUARD_SECONDS + _EXIT_MARGIN_SECONDS)
        except subprocess.TimeoutExpired:
            raise AssertionError(
                "a blocked release held the process past the bound; log tail:\n"
                + _log(log_path)[-4000:]
            ) from None
        elapsed = time.monotonic() - sent
    finally:
        _reap(proc)
    log_text = _log(log_path)
    assert code == _SIGTERM_EXIT, (code, log_text[-4000:])
    # Bound plus the 0.5 s diagnostics cap plus scheduling slack.
    assert MCP_STOP_GUARD_SECONDS - 0.5 <= elapsed < MCP_STOP_GUARD_SECONDS + 2.0, (
        elapsed
    )
    assert "connection claim release still in flight" in log_text, log_text[-4000:]


def test_sigterm_with_stdin_open_is_bounded(
    stop_guard_dsn: str, deployments_root: Path, tmp_path: Path
) -> None:
    """SIGTERM alone, stdin still open, the real CLI.

    Sometimes interpreter exit then waits on anyio's worker thread blocked
    reading stdin (seen on lab-host; a race, so it is not asserted). Either
    way the claim goes at once and the process exits 143 within the bound
    plus the margin: normally, or through the guard.
    """
    log_path = tmp_path / "mcp-serve.log"
    proc = _start(stop_guard_dsn, log_path, deployments_root, launcher=False)
    try:
        _initialize(proc, log_path)
        proc.send_signal(signal.SIGTERM)
        assert _seconds_until_row_gone(stop_guard_dsn, proc.pid) is not None, _log(
            log_path
        )[-4000:]
        code = proc.wait(timeout=MCP_STOP_GUARD_SECONDS + _EXIT_MARGIN_SECONDS)
    finally:
        _reap(proc)
    assert code == _SIGTERM_EXIT, (code, _log(log_path)[-4000:])
    assert _reservation_count(stop_guard_dsn, proc.pid) == 0


def test_normal_stop_is_fast_and_releases_the_claim(
    stop_guard_dsn: str, deployments_root: Path, tmp_path: Path
) -> None:
    log_path = tmp_path / "mcp-serve.log"
    proc = _start(stop_guard_dsn, log_path, deployments_root, launcher=False)
    try:
        _initialize(proc, log_path)
        assert _reservation_count(stop_guard_dsn, proc.pid) == 1
        assert proc.stdin is not None
        sent = time.monotonic()
        proc.stdin.close()
        proc.send_signal(signal.SIGTERM)
        code = proc.wait(timeout=MCP_STOP_GUARD_SECONDS + _EXIT_MARGIN_SECONDS)
        elapsed = time.monotonic() - sent
    finally:
        _reap(proc)

    log_text = _log(log_path)
    # Closing stdin can let the server finish before SIGTERM lands: it then
    # exits 0, or SIGTERM arrives after the handler is restored and the
    # default action ends it (-15). Otherwise SIGTERM unwinds it (143). All
    # three are the normal, unguarded exits this test must keep.
    assert code in {0, _SIGTERM_EXIT, -signal.SIGTERM}, (code, log_text[-4000:])
    assert elapsed < _FAST_EXIT_SECONDS, (elapsed, log_text[-4000:])
    assert MCP_STOP_GUARD_EXIT_MESSAGE not in log_text, log_text[-4000:]
    assert _reservation_count(stop_guard_dsn, proc.pid) == 0
