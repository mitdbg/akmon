"""Session advisory locks must not outlive the borrower that took them.

A session-level advisory lock belongs to the PostgreSQL session, and the
pool's rollback on return does not release it. Two leaks were seen:

* MCP `run_sql` ran ``SELECT pg_advisory_lock_shared(...)`` on the
  history-lease key; the lock stayed on the idle pooled connection and every
  other session's ``pg_try_advisory_xact_lock`` on that key returned false.
* The publisher took its session mutex, then its source-sync probe raised;
  only the "blocker found" branch released the lock, so every later publish
  failed with PublisherLeaseHeld until the pooled connection aged out.

These tests prove the pools clear session locks on return and the publisher
releases (or closes the session holding) its mutex on every failure path.
"""
from __future__ import annotations

import ast
import subprocess
import uuid
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg_pool import ConnectionPool

from okg.substrate.db.connection import connection_kwargs

REPO_ROOT = Path(__file__).resolve().parents[2]

#: The history-lease / compaction key from the incident (schema.sql).
_INCIDENT_CLASSID = 1326131011


def _key_is_free(dsn: str, classid: int, objid: int) -> bool:
    """Can a DIFFERENT session take the key exclusively right now?"""
    with psycopg.connect(dsn, connect_timeout=5) as probe:
        row = probe.execute(
            "SELECT pg_try_advisory_xact_lock(%s, %s)", (classid, objid),
        ).fetchone()
        probe.rollback()
    return bool(row[0])


def _fresh_objid() -> int:
    return uuid.uuid4().int % 2_000_000_000


def _publisher(deployment: str, probe: Any = None) -> Any:
    from okg.substrate.publisher.publisher import Publisher

    return Publisher(
        dsn="postgres://unused",
        deployment_name=deployment,
        coordination_mode="dbos",
        source_sync_priority_probe=probe,
    )


# ---------------------------------------------------------------------------
# Static guard: every pool okg builds carries the reset.
# ---------------------------------------------------------------------------

_POOL_ROOTS = (
    REPO_ROOT / "src" / "okg" / "substrate",
    REPO_ROOT / "src" / "okg" / "viz",
)


def _pool_calls() -> list[tuple[str, int, bool]]:
    found: list[tuple[str, int, bool]] = []
    for root in _POOL_ROOTS:
        for path in sorted(root.rglob("*.py")):
            source = path.read_text(encoding="utf-8", errors="replace")
            if "Pool(" not in source:
                continue
            for node in ast.walk(ast.parse(source)):
                if not isinstance(node, ast.Call):
                    continue
                func = node.func
                name = (
                    func.attr if isinstance(func, ast.Attribute)
                    else func.id if isinstance(func, ast.Name) else None
                )
                if name not in {"ConnectionPool", "AsyncConnectionPool"}:
                    continue
                has_reset = any(kw.arg == "reset" for kw in node.keywords)
                found.append(
                    (str(path.relative_to(REPO_ROOT)), node.lineno, has_reset)
                )
    return found


def test_every_connection_pool_resets_session_locks_on_return() -> None:
    calls = _pool_calls()
    files = {path for path, _, _ in calls}
    # The walk must find the three pools okg builds, or this guard is vacuous.
    assert {
        "src/okg/substrate/db/pool.py",
        "src/okg/substrate/mcp/server.py",
        "src/okg/viz/boot.py",
    } <= files, files
    missing = [f"{path}:{line}" for path, line, ok in calls if not ok]
    assert not missing, (
        "connection pool built without reset=release_session_advisory_locks: "
        + ", ".join(missing)
    )


# ---------------------------------------------------------------------------
# Pools: a lock left on a returned connection is released.
# ---------------------------------------------------------------------------


@pytest.mark.postgres
def test_shared_pool_releases_shared_lock_left_by_borrower(okg_migrated: str) -> None:
    from okg.substrate.db.pool import close_all, get_pool

    objid = _fresh_objid()
    close_all()
    try:
        pool = get_pool(okg_migrated)
        with pool.connection() as conn:
            conn.execute(
                "SELECT pg_advisory_lock_shared(%s, %s)",
                (_INCIDENT_CLASSID, objid),
            )
            assert not _key_is_free(okg_migrated, _INCIDENT_CLASSID, objid)
        assert _key_is_free(okg_migrated, _INCIDENT_CLASSID, objid)
    finally:
        close_all()


@pytest.mark.postgres
def test_mcp_pool_releases_shared_lock_left_by_run_sql(okg_migrated: str) -> None:
    from okg.substrate.mcp.server import _build_pool

    objid = _fresh_objid()
    pool = _build_pool(
        conninfo=okg_migrated, role="okg_test", min_size=1, max_size=1,
        open=True, name="okg-test-mcp-reset",
    )
    try:
        with pool.connection() as conn:
            pid = conn.info.backend_pid
            conn.execute(
                "SELECT pg_try_advisory_lock_shared(%s, %s)",
                (_INCIDENT_CLASSID, objid),
            )
        assert _key_is_free(okg_migrated, _INCIDENT_CLASSID, objid)
        # The reset kept the session: the same backend is reused.
        with pool.connection() as conn:
            assert conn.info.backend_pid == pid
    finally:
        pool.close()


class _StopAfterPool(Exception):
    """Raised by the stubbed seed connect: the viz pool already exists."""


@pytest.mark.postgres
def test_viz_pool_releases_lock_left_by_borrower(
    okg_migrated: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Build the pool through `viz.boot.load` itself, not a copy of its call."""
    from okg.viz import boot

    built: list[ConnectionPool] = []

    class _RecordingPool(ConnectionPool):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            built.append(self)

    def _stop(*_args: Any, **_kwargs: Any) -> Any:
        raise _StopAfterPool()

    monkeypatch.setattr(boot, "ConnectionPool", _RecordingPool)
    monkeypatch.setattr(boot, "_find_deployment_dir", lambda _name: tmp_path)
    monkeypatch.setattr(boot, "_load_manifest", lambda _dir: {})
    monkeypatch.setattr(boot, "_load_viz_config_for", lambda _m, _d: {})
    monkeypatch.setattr(boot, "db_connect", _stop)

    objid = _fresh_objid()
    with pytest.raises(_StopAfterPool):
        boot.load(dsn=okg_migrated, min_conns=1, max_conns=1)
    assert len(built) == 1
    pool = built[0]
    try:
        with pool.connection() as conn:
            conn.execute(
                "SELECT pg_advisory_lock_shared(%s, %s)",
                (_INCIDENT_CLASSID, objid),
            )
            assert not _key_is_free(okg_migrated, _INCIDENT_CLASSID, objid)
        assert _key_is_free(okg_migrated, _INCIDENT_CLASSID, objid)
    finally:
        pool.close()


@pytest.mark.postgres
def test_non_autocommit_pool_reset_leaves_connection_reusable(
    okg_migrated: str,
) -> None:
    """The reset's own statement must not leave the connection in a
    transaction; psycopg_pool would discard it every time if it did."""
    from okg.substrate.db.pool import release_session_advisory_locks

    objid = _fresh_objid()
    pool = ConnectionPool(
        conninfo=okg_migrated,
        min_size=1,
        max_size=1,
        kwargs={**connection_kwargs(dsn=okg_migrated), "autocommit": False},
        reset=release_session_advisory_locks,
        open=True,
        name="okg-test-nonautocommit-reset",
    )
    try:
        with pool.connection() as conn:
            pid = conn.info.backend_pid
            conn.execute(
                "SELECT pg_advisory_lock(%s, %s)", (_INCIDENT_CLASSID, objid),
            )
        assert _key_is_free(okg_migrated, _INCIDENT_CLASSID, objid)
        with pool.connection() as conn:
            assert conn.info.backend_pid == pid
    finally:
        pool.close()


# ---------------------------------------------------------------------------
# Publisher: the mutex is released on every failure path.
# ---------------------------------------------------------------------------


@pytest.mark.postgres
def test_publisher_probe_raising_after_acquire_releases_lock(
    okg_migrated: str,
) -> None:
    from okg.substrate.runtime.source_sync_priority import (
        publisher_publish_advisory_keys,
    )

    deployment = f"test-probe-raise-{uuid.uuid4().hex[:8]}"
    calls = iter([None])

    def probe() -> str | None:
        try:
            return next(calls)
        except StopIteration:
            raise psycopg.OperationalError("too many connections") from None

    pub = _publisher(deployment, probe)
    classid, objid = publisher_publish_advisory_keys(deployment)
    with psycopg.connect(okg_migrated, autocommit=True, connect_timeout=5) as conn:
        with pytest.raises(psycopg.OperationalError, match="too many"):
            pub._acquire_dbos_publish_lock(conn, wait_seconds=0)
        assert not conn.closed
        assert _key_is_free(okg_migrated, classid, objid)


@pytest.mark.postgres
def test_publisher_statement_timeout_failure_releases_lock(
    okg_migrated: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime.source_sync_priority import (
        publisher_publish_advisory_keys,
    )

    deployment = f"test-set-timeout-{uuid.uuid4().hex[:8]}"
    pub = _publisher(deployment)
    monkeypatch.setattr(pub, "_publish_statement_timeout_ms", lambda: 1000)

    def _fail(_conn: Any, _ms: int) -> None:
        raise psycopg.OperationalError("SET failed")

    monkeypatch.setattr(pub, "_set_statement_timeout", _fail)
    classid, objid = publisher_publish_advisory_keys(deployment)
    with psycopg.connect(okg_migrated, autocommit=True, connect_timeout=5) as conn:
        with pytest.raises(psycopg.OperationalError, match="SET failed"):
            with pub._publish_lock_on_connection(
                conn, mode="dbos", wait_seconds=0,
            ):
                pytest.fail("body must not run after a failed SET")
        assert _key_is_free(okg_migrated, classid, objid)


class _UnlockFails:
    """A real connection whose `pg_advisory_unlock` calls raise."""

    def __init__(
        self, real: psycopg.Connection, only_key: tuple[int, int] | None = None,
    ) -> None:
        self._real = real
        self._only_key = only_key

    def cursor(self) -> Any:
        real_cursor = self._real.cursor()
        only_key = self._only_key

        class _Cursor:
            def __enter__(self) -> "_Cursor":
                return self

            def __exit__(self, *_exc: object) -> bool:
                real_cursor.close()
                return False

            def execute(self, query: str, params: Any = None) -> Any:
                if "pg_advisory_unlock" in query and (
                    only_key is None or tuple(params or ()) == only_key
                ):
                    raise psycopg.OperationalError("server closed the connection")
                return real_cursor.execute(query, params)

            def fetchone(self) -> Any:
                return real_cursor.fetchone()

        return _Cursor()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


@pytest.mark.postgres
def test_publisher_failed_unlock_closes_the_connection(okg_migrated: str) -> None:
    deployment = f"test-unlock-fails-{uuid.uuid4().hex[:8]}"
    pub = _publisher(deployment)
    with psycopg.connect(okg_migrated, autocommit=True, connect_timeout=5) as conn:
        classid, objid = pub._acquire_dbos_publish_lock(conn, wait_seconds=0)
        assert not _key_is_free(okg_migrated, classid, objid)
        with pytest.raises(psycopg.OperationalError):
            pub._release_dbos_publish_lock(_UnlockFails(conn), (classid, objid))
        assert conn.closed
        assert _key_is_free(okg_migrated, classid, objid)


@pytest.mark.postgres
def test_publisher_probe_raising_then_unlock_failing_keeps_original_error(
    okg_migrated: str, caplog: pytest.LogCaptureFixture,
) -> None:
    """Release failing on the error path: log it, close, raise the original."""
    deployment = f"test-double-fail-{uuid.uuid4().hex[:8]}"
    calls = iter([None])

    def probe() -> str | None:
        try:
            return next(calls)
        except StopIteration:
            raise RuntimeError("probe exploded") from None

    from okg.substrate.runtime.source_sync_priority import (
        publisher_publish_advisory_keys,
    )

    pub = _publisher(deployment, probe)
    classid, objid = publisher_publish_advisory_keys(deployment)
    with psycopg.connect(
        okg_migrated, autocommit=True, connect_timeout=5,
    ) as real:
        # Only the publisher mutex's unlock fails; the source-sync intent
        # probe's own try-lock/unlock on another key still works.
        wrapped = _UnlockFails(real, only_key=(classid, objid))
        with caplog.at_level("ERROR"):
            with pytest.raises(RuntimeError, match="probe exploded"):
                pub._acquire_dbos_publish_lock(wrapped, wait_seconds=0)
        assert real.closed
        assert _key_is_free(okg_migrated, classid, objid)
    assert any(
        "release failed while handling an earlier error" in r.getMessage()
        and str([classid, objid]) in r.getMessage()
        for r in caplog.records
    )


@pytest.mark.postgres
def test_publisher_unlock_returning_false_closes_the_connection(
    okg_migrated: str,
) -> None:
    from okg.substrate.runtime.source_sync_priority import (
        publisher_publish_advisory_keys,
    )

    deployment = f"test-unlock-false-{uuid.uuid4().hex[:8]}"
    pub = _publisher(deployment)
    key = publisher_publish_advisory_keys(deployment)
    with psycopg.connect(okg_migrated, autocommit=True, connect_timeout=5) as conn:
        pub._release_dbos_publish_lock(conn, key)  # never acquired
        assert conn.closed


@pytest.mark.postgres
def test_publisher_normal_release_keeps_the_connection(okg_migrated: str) -> None:
    """The success path is unchanged: unlock, keep the session."""
    deployment = f"test-unlock-ok-{uuid.uuid4().hex[:8]}"
    pub = _publisher(deployment)
    with psycopg.connect(okg_migrated, autocommit=True, connect_timeout=5) as conn:
        with pub._publish_lock_on_connection(conn, mode="dbos", wait_seconds=0):
            pass
        assert not conn.closed
        classid, objid = pub._acquire_dbos_publish_lock(conn, wait_seconds=0)
        pub._release_dbos_publish_lock(conn, (classid, objid))
        assert not conn.closed
        assert _key_is_free(okg_migrated, classid, objid)


# ---------------------------------------------------------------------------
# Issue #3861: a client that hard-exits mid-statement frees its session lock.
# ---------------------------------------------------------------------------

_HARD_EXIT_CLIENT = """
import os, sys, threading
import psycopg
from okg.substrate.db.connection import connection_kwargs

dsn = os.environ["PROBE_DSN"]
conn = psycopg.connect(dsn, autocommit=True, **connection_kwargs(dsn=dsn))
conn.execute("SELECT pg_advisory_lock(%s, %s)", (int(sys.argv[1]), int(sys.argv[2])))
print("locked", flush=True)
# The worker's hard exit: os._exit, no connection close, mid-statement.
threading.Timer(2.0, lambda: os._exit(7)).start()
conn.execute("SELECT pg_sleep(40)")
"""


def _run_hard_exit_client(
    dsn: str, objid: int, check_interval_ms: str,
) -> subprocess.Popen[str]:
    import os
    import sys

    env = dict(os.environ)
    env["PROBE_DSN"] = dsn
    env["OKG_CLIENT_CONNECTION_CHECK_INTERVAL_MS"] = check_interval_ms
    proc = subprocess.Popen(
        [sys.executable, "-c", _HARD_EXIT_CLIENT, str(_INCIDENT_CLASSID), str(objid)],
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, env=env, start_new_session=True,
    )
    assert proc.stdout is not None
    assert proc.stdout.readline().strip() == "locked"
    return proc


def _wait_until_free(dsn: str, objid: int, seconds: float) -> bool:
    import time

    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if _key_is_free(dsn, _INCIDENT_CLASSID, objid):
            return True
        time.sleep(0.5)
    return False


@pytest.mark.postgres
def test_hard_exited_client_frees_its_session_lock_within_seconds(
    okg_migrated: str,
) -> None:
    objid = _fresh_objid()
    proc = _run_hard_exit_client(okg_migrated, objid, "")  # default interval
    try:
        assert proc.wait(timeout=15) == 7
        # The statement would run 40 s; with the check interval the backend
        # notices the closed socket and ends it (default 5 s).
        assert _wait_until_free(okg_migrated, objid, 15.0)
    finally:
        proc.kill()


@pytest.mark.postgres
def test_without_the_check_interval_a_hard_exited_client_keeps_its_lock(
    okg_migrated: str,
) -> None:
    """Negative control: the setting, not luck, frees the lock above."""
    objid = _fresh_objid()
    proc = _run_hard_exit_client(okg_migrated, objid, "0")  # disabled
    try:
        assert proc.wait(timeout=15) == 7
        assert not _wait_until_free(okg_migrated, objid, 12.0)
    finally:
        proc.kill()
        with psycopg.connect(okg_migrated, autocommit=True, connect_timeout=5) as c:
            c.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_locks "
                "WHERE locktype = 'advisory' AND classid::bigint = %s AND objid::bigint = %s "
                "AND granted",
                (_INCIDENT_CLASSID, objid),
            )
