"""Source-sync admission priority helpers.

Source-sync runs on its own DBOS queue, so a durable ``okg_source_sync`` row is
not always present at the exact moment a publisher decides whether to start a
large publish cycle. The admission intent advisory lock makes that in-flight
producer intent visible before the durable workflow enqueue commits.

A source run outside the runtime worker (``okg ingest --inline``, provision,
the bootstrap publish proof) holds the same advisory lock for its whole run
through :func:`source_sync_run_guard` (issue #3003). Every publisher that
honours the source-sync signal therefore waits for that run, and a second such
run waits behind it. The worker does not take the guard: its DBOS queue
already runs one sync at a time, and the guard checks that queue before it
starts.
"""

from __future__ import annotations

import contextvars
import hashlib
import logging
import os
import threading
import time
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, ContextManager, Iterator, Mapping

import psycopg
from okg.substrate.db.connection import connect as db_connect


SOURCE_SYNC_ADMISSION_INTENT_ID = "source-sync-admission-intent"
SOURCE_SYNC_RUN_WAIT_ENV = "OKG_SOURCE_SYNC_RUN_WAIT_SECONDS"
DEFAULT_SOURCE_SYNC_RUN_WAIT_SECONDS = 600.0
_WAIT_PROGRESS_LOG_SECONDS = 30.0


@dataclass(frozen=True)
class _HeldRunGuard:
    """This context's hold on a deployment's source-sync signal."""

    pid: int
    runtime_probe: Callable[[], str | None] | None
    # The session that holds the lock (in the guard's own process), and a
    # lock serialising the per-chunk liveness check across the run's threads
    # (#3003 round 3). A source child process has no holding session: it
    # looks the parent's session up in pg_stat_activity by ``pid`` AND
    # ``backend_start`` on a checker session instead (#3003 round 4).
    session: psycopg.Connection | None = None
    session_lock: threading.Lock | None = None
    backend_start: str | None = None
    dsn: str | None = None
    checker: list[psycopg.Connection] = field(default_factory=list)


# Deployment -> the run guard held in this context. A publish from inside the
# run reads it: it never waits on its own hold, it checks the hold is still
# there, and it checks the worker queue with the same probe as the guard.
_RUN_GUARDS: contextvars.ContextVar[Mapping[str, _HeldRunGuard]] = (
    contextvars.ContextVar("okg_source_sync_run_guards", default={})
)
PUBLISHER_ADVISORY_LOCK_HELD_ID = "publisher-advisory-lock-held"
_LOCAL_ADMISSION_LOCKS: dict[str, threading.Lock] = {}
_LOCAL_ADMISSION_LOCKS_GUARD = threading.Lock()


def _local_source_sync_admission_lock(deployment: str) -> threading.Lock:
    with _LOCAL_ADMISSION_LOCKS_GUARD:
        return _LOCAL_ADMISSION_LOCKS.setdefault(deployment, threading.Lock())


def publisher_publish_advisory_keys(deployment: str) -> tuple[int, int]:
    h = hashlib.md5(
        f"okg.publish.{deployment}".encode("utf-8"),
        usedforsecurity=False,
    ).digest()
    classid = int.from_bytes(h[0:4], "big", signed=True)
    objid = int.from_bytes(h[4:8], "big", signed=True)
    return classid, objid


def publisher_publish_intent_advisory_keys(deployment: str) -> tuple[int, int]:
    """Key a session holds SHARED while it waits for the publisher lock.

    The publisher lock is taken with ``pg_try_advisory_lock`` in a polling
    loop, so a waiter never shows in ``pg_locks`` as ungranted. Background
    work that takes the publisher lock briefly and repeatedly (the
    post-publish index rebuild, okg#3112) probes this key exclusively before
    each step and yields while any session holds it, so a waiting publish
    waits for one step at most.

    Naming assumption: the key text ``okg.publish.intent.<d>`` can equal the
    publisher key text ``okg.publish.<d2>`` only for a deployment named
    ``intent.<d>``. Deployment names are ``[a-z0-9-]`` (no dot), so the two
    key spaces never meet; widening the name alphabet must revisit this.
    """
    h = hashlib.md5(
        f"okg.publish.intent.{deployment}".encode("utf-8"),
        usedforsecurity=False,
    ).digest()
    classid = int.from_bytes(h[0:4], "big", signed=True)
    objid = int.from_bytes(h[4:8], "big", signed=True)
    return classid, objid


class PublisherWaitIntent:
    """Announce that this session waits for the publisher lock (see
    :func:`publisher_publish_intent_advisory_keys`).

    ``announce()`` takes the shared intent lock the first time a try for the
    publisher lock fails, so an uncontended acquisition runs no extra
    statement; ``release()`` drops it and never raises. The shared lock does
    not wait in practice: its only exclusive taker is a probe that releases
    it at once.
    """

    def __init__(self, conn: Any, deployment: str) -> None:
        self._conn = conn
        self._key = publisher_publish_intent_advisory_keys(deployment)
        self.held = False

    def announce(self) -> None:
        if self.held:
            return
        with self._conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock_shared(%s, %s)", self._key)
        self.held = True

    def release(self) -> None:
        """Drop the shared intent lock, or close the session that holds it.

        Mirrors ``Publisher._release_dbos_publish_lock``: an unlock that
        fails (the transaction failed) is retried after a rollback, and a
        lock that still cannot be released is not left on a live
        connection: the connection is closed, because a leaked intent lock
        would make every later index rebuild yield (``publish_waiting``)
        for as long as the connection lives. Never raises.
        """
        if not self.held:
            return
        self.held = False
        conn = self._conn
        if getattr(conn, "closed", False):
            return

        def _unlock() -> bool:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock_shared(%s, %s)", self._key)
                row = cur.fetchone()
            return bool(row[0]) if row is not None else False

        try:
            try:
                released = _unlock()
            except psycopg.Error:
                conn.rollback()
                released = _unlock()
        except Exception:  # noqa: BLE001 -- closing releases it; never masks
            logging.getLogger(__name__).exception(
                "publisher wait-intent unlock failed; closing the connection "
                "so the shared lock cannot outlive it: key=%s", list(self._key),
            )
            released = False
        if not released:
            logging.getLogger(__name__).error(
                "publisher wait-intent lock not released; closing the "
                "connection: key=%s", list(self._key),
            )
            try:
                conn.close()
            except Exception:  # noqa: BLE001 -- closing is the last resort
                pass


@contextmanager
def publisher_publish_lock_on_connection(
    *,
    conn: psycopg.Connection,
    deployment: str,
    wait_seconds: float = 30.0,
    operation: str = "effective-attribute repair",
) -> Iterator[None]:
    """Hold the same session mutex as the publisher on an existing connection.

    Lifecycle repair uses this while the worker is stopped so a manual publisher
    cannot race the derived-read-model transaction.  The wait is deliberately
    bounded: deployment automation must fail loudly instead of hanging forever.
    """
    classid, objid = publisher_publish_advisory_keys(deployment)
    deadline = time.monotonic() + max(0.0, wait_seconds)
    # A waiting session announces itself (okg#3112): the post-publish index
    # rebuild yields to it between steps.
    intent = PublisherWaitIntent(conn, deployment)
    try:
        while True:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s, %s)", (classid, objid))
                acquired = bool(cur.fetchone()[0])
            if acquired:
                break
            if time.monotonic() >= deadline:
                from okg.substrate.errors import SubstrateError

                raise SubstrateError(
                    f"publisher lock is held during {operation}",
                    data={
                        "deployment": deployment,
                        "advisory_key": [classid, objid],
                        "wait_seconds": wait_seconds,
                    },
                    fix_hint="wait for the active publisher to finish, then rerun okg up",
                )
            intent.announce()
            time.sleep(min(0.25, max(0.01, deadline - time.monotonic())))
    finally:
        intent.release()
    try:
        yield
    finally:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s, %s)", (classid, objid))


def source_sync_admission_intent_advisory_keys(
    deployment: str,
) -> tuple[int, int]:
    h = hashlib.md5(
        f"okg.source_sync.intent.{deployment}".encode("utf-8"),
        usedforsecurity=False,
    ).digest()
    classid = int.from_bytes(h[0:4], "big", signed=True)
    objid = int.from_bytes(h[4:8], "big", signed=True)
    return classid, objid


@contextmanager
def source_sync_admission_intent_lock(
    *,
    dsn: str,
    deployment: str,
    wait_seconds: float | None = None,
) -> Iterator[None]:
    """Hold the source-sync admission intent until enqueue is durable.

    DBOS scheduler callbacks can enter this path concurrently after a resume
    burst. Serialize those threads before opening PostgreSQL connections so the
    cross-process advisory mutex never becomes a one-connection-per-waiter
    convoy.

    ``wait_seconds=None`` (the runtime producers) waits on the lock without a
    bound, as before. An operator enqueue passes a bound (#3003): it waits
    while a command-line run holds the signal, logs progress, and then
    raises ``source_sync_already_running``.
    """
    classid, objid = source_sync_admission_intent_advisory_keys(deployment)
    if wait_seconds is None:
        with _local_source_sync_admission_lock(deployment):
            with db_connect(dsn, connect_timeout=5, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_advisory_lock(%s, %s)", (classid, objid),
                    )
                try:
                    yield
                finally:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT pg_advisory_unlock(%s, %s)",
                            (classid, objid),
                        )
        return
    wait = source_sync_run_wait_seconds(wait_seconds)
    started = time.monotonic()
    local = _local_source_sync_admission_lock(deployment)
    if not local.acquire(timeout=wait):
        raise _already_running(
            deployment, wait, SOURCE_SYNC_ADMISSION_INTENT_ID, [],
            action="enqueue",
        )
    try:
        with db_connect(dsn, connect_timeout=5, autocommit=True) as conn:
            _wait_for_intent_lock(
                conn, deployment, started=started, wait=wait,
                runtime_probe=None, action="enqueue",
            )
            try:
                yield
            finally:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_advisory_unlock(%s, %s)",
                        (classid, objid),
                    )
    finally:
        local.release()


def source_sync_admission_intent_workflow_id(
    *,
    dsn: str,
    deployment: str,
) -> str | None:
    with db_connect(dsn, connect_timeout=5) as conn:
        return source_sync_admission_intent_workflow_id_on_connection(
            conn=conn,
            deployment=deployment,
        )


def source_sync_admission_intent_workflow_id_on_connection(
    *,
    conn: psycopg.Connection,
    deployment: str,
) -> str | None:
    """Name who holds the source-sync signal, or None when it is free.

    Inside this context's own run guard, the run's own hold is not another
    sync. The run's hold must still be there: if its session died (killed
    backend, idle timeout, network drop) the run would otherwise publish
    unprotected, so this raises ``source_sync_signal_lost`` instead.
    """
    classid, objid = source_sync_admission_intent_advisory_keys(deployment)
    own = _RUN_GUARDS.get().get(deployment)
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s, %s)", (classid, objid))
        acquired = bool(cur.fetchone()[0])
        if acquired:
            cur.execute("SELECT pg_advisory_unlock(%s, %s)", (classid, objid))
            if own is not None:
                _raise_signal_lost(deployment, own.pid, [])
            return None
    if own is None:
        return SOURCE_SYNC_ADMISSION_INTENT_ID
    holders = _intent_holder_pids(conn, deployment)
    if holders != [own.pid]:
        _raise_signal_lost(deployment, own.pid, holders)
    # Held by this context's own run guard: not another sync.
    return None


def _raise_signal_lost(deployment: str, own_pid: int, holders: list[int]) -> None:
    from okg.substrate.errors import SourceSyncSignalLost

    raise SourceSyncSignalLost(
        "this run lost the deployment's source-sync signal "
        f"(deployment={deployment!r}); it stops before writing or publishing "
        "more, because another run or a scheduled publish may now proceed "
        "beside it",
        data={
            "deployment": deployment,
            "own_pid": own_pid,
            "holder_pids": holders,
        },
        fix_hint=(
            "the database session holding the signal ended (killed backend, "
            "idle timeout, or network drop); rerun the ingest"
        ),
    )


def ensure_run_guard_held(deployment: str | None) -> None:
    """Stop a run whose source-sync signal is gone, before its next write.

    Called before every chunk a source run commits. The guard's session holds
    the lock for its whole life (only the guard's own exit unlocks it, after
    the run and its children are done), so "the holding session is still the
    same live session" is "the lock is still held":

    * in the guard's own process, a round trip on that session;
    * in a source child, one indexed lookup of the parent's session in
      ``pg_stat_activity`` by ``pid`` AND ``backend_start`` (a reused pid has
      a new start time), instead of scanning the whole ``pg_locks`` table.

    Once the session is gone the run stops with ``source_sync_signal_lost``.
    That bounds the run's own later writes, not what it already wrote:
    everything written before the lock was lost, plus the chunk in flight,
    can be published by another run until the failed run's cleanup (or,
    with #2993, its run-base undo) completes. #2993's pending-undo guard
    refuses publishes only once that undo has started, i.e. after this check
    fires at the run's next chunk, so a publish in between can still include
    the writes. Outside a guard (the runtime worker, a standalone runner) it
    does nothing.
    """
    if deployment is None:
        return
    own = _RUN_GUARDS.get().get(deployment)
    if own is None or own.session_lock is None:
        return
    with own.session_lock:
        if own.session is not None:
            try:
                own.session.execute("SELECT 1")
            except psycopg.Error:
                _raise_signal_lost(deployment, own.pid, [])
            return
        if own.dsn is None or own.backend_start is None:
            return
        if not own.checker:
            own.checker.append(
                db_connect(own.dsn, connect_timeout=5, autocommit=True),
            )
        alive = own.checker[0].execute(
            "SELECT 1 FROM pg_stat_activity"
            " WHERE pid = %s AND backend_start = %s::timestamptz",
            (own.pid, own.backend_start),
        ).fetchone()
        if alive is None:
            _raise_signal_lost(deployment, own.pid, [])


def _session_backend_start(conn: psycopg.Connection) -> str:
    row = conn.execute(
        "SELECT backend_start::text FROM pg_stat_activity"
        " WHERE pid = pg_backend_pid()",
    ).fetchone()
    return str(row[0])


def run_guard_handoff(deployment: str) -> dict[str, Any] | None:
    """What a spawned source child needs to keep checking this run's hold."""
    own = _RUN_GUARDS.get().get(deployment)
    if own is None:
        return None
    return {
        "deployment": deployment,
        "pid": own.pid,
        "backend_start": own.backend_start,
    }


def adopt_run_guard(handoff: Mapping[str, Any] | None, *, dsn: str) -> None:
    """In a source child: check the parent's hold before every chunk.

    The child does not hold the lock; its chunks are written under the
    parent's hold, so ``ensure_run_guard_held`` checks that the parent's
    session (pid and start time) is still alive. The child never publishes.
    No handoff means the run holds no guard: the runtime worker, which is
    serialised by its own queue, or a standalone run.
    """
    if not handoff:
        logging.getLogger(__name__).debug(
            "source child: no source-sync run guard handed over "
            "(runtime worker or standalone run); no per-chunk check",
        )
        return
    deployment = str(handoff["deployment"])
    _RUN_GUARDS.set({
        **_RUN_GUARDS.get(),
        deployment: _HeldRunGuard(
            pid=int(handoff["pid"]),
            runtime_probe=None,
            session_lock=threading.Lock(),
            backend_start=str(handoff["backend_start"]),
            dsn=dsn,
        ),
    })


def own_run_guard_runtime_blocker(deployment: str) -> str | None:
    """The worker-queue check for a publish inside this context's run guard.

    Returns the id of a queued or running runtime source sync, or None. A
    runtime-disabled deployment has no probe and returns None.
    """
    own = _RUN_GUARDS.get().get(deployment)
    if own is None or own.runtime_probe is None:
        return None
    return own.runtime_probe()


def _intent_holder_pids(conn: psycopg.Connection, deployment: str) -> list[int]:
    classid, objid = source_sync_admission_intent_advisory_keys(deployment)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pid FROM pg_locks
             WHERE locktype = 'advisory' AND granted
               AND database = (
                   SELECT oid FROM pg_database
                    WHERE datname = current_database()
               )
               AND classid = (%s::int4)::oid AND objid = (%s::int4)::oid
               AND objsubid = 2
            """,
            (classid, objid),
        )
        return [int(row[0]) for row in cur.fetchall()]


def source_sync_run_guard_held(deployment: str) -> bool:
    """True when this context runs inside its own source-sync run guard."""
    return deployment in _RUN_GUARDS.get()


def source_sync_run_wait_seconds(explicit: float | None = None) -> float:
    """The bounded wait for another sync: flag, then env, then the default."""
    from okg.substrate.errors import SubstrateError

    if explicit is not None:
        value = float(explicit)
    else:
        raw = os.environ.get(SOURCE_SYNC_RUN_WAIT_ENV)
        if raw is None:
            return DEFAULT_SOURCE_SYNC_RUN_WAIT_SECONDS
        try:
            value = float(raw)
        except ValueError as exc:
            raise SubstrateError(
                f"{SOURCE_SYNC_RUN_WAIT_ENV} must be a number of seconds; "
                f"got {raw!r}",
                data={"env": SOURCE_SYNC_RUN_WAIT_ENV, "value": raw},
                fix_hint=f"set {SOURCE_SYNC_RUN_WAIT_ENV} to e.g. 600",
            ) from exc
    if value < 0:
        raise SubstrateError(
            f"source-sync wait must be >= 0 seconds; got {value}",
            data={"wait_seconds": value},
            fix_hint="pass 0 to fail at once, or a positive number of seconds",
        )
    return value


def runtime_source_sync_probe(
    *,
    dsn: str,
    deployment: str,
    deployment_dir: Path | str | None,
) -> Callable[[], str | None] | None:
    """Probe for the runtime worker's queued or running source sync.

    Only a runtime-enabled deployment has a worker draining that queue, so a
    disabled one is not probed (a stale row there would block every run). An
    unreadable manifest is probed: the check fails closed.
    """
    if deployment_dir is not None:
        import yaml

        try:
            raw = yaml.safe_load(
                (Path(deployment_dir) / "deployment.yaml").read_text(
                    encoding="utf-8",
                ),
            )
        except (OSError, ValueError, yaml.YAMLError):
            raw = None
        if isinstance(raw, dict):
            runtime = raw.get("runtime")
            if not (isinstance(runtime, dict) and runtime.get("enabled") is True):
                return None

    def probe() -> str | None:
        from okg.substrate.runtime.dbos_enqueue import (
            active_workflow_id_for_name,
        )
        from okg.substrate.runtime.partition_queue import source_sync_queue_name
        from okg.substrate.runtime.source_sync_constants import (
            SOURCE_SYNC_WORKFLOW_NAME,
        )

        return active_workflow_id_for_name(
            dsn=dsn,
            queue_name=source_sync_queue_name(deployment),
            workflow_name=SOURCE_SYNC_WORKFLOW_NAME,
        )

    return probe


def ingest_run_guard(
    *,
    dsn: str,
    deployment: str,
    deployment_dir: Path | str | None,
    hold: bool,
    wait_seconds: float | None,
) -> ContextManager[Any]:
    """The run guard for one ``run_ingest`` call; a no-op for the worker."""
    if not hold:
        return nullcontext()
    return source_sync_run_guard(
        dsn=dsn,
        deployment=deployment,
        wait_seconds=wait_seconds,
        runtime_probe=runtime_source_sync_probe(
            dsn=dsn, deployment=deployment, deployment_dir=deployment_dir,
        ),
    )


def _already_running(
    deployment: str,
    wait: float,
    blocker: str | None,
    holders: list[int],
    *,
    action: str,
) -> Exception:
    from okg.substrate.errors import SourceSyncAlreadyRunning

    doing = "enqueue a source sync" if action == "enqueue" else "start"
    if blocker == SOURCE_SYNC_ADMISSION_INTENT_ID:
        message = (
            "another source sync is running on deployment "
            f"{deployment!r} (holder pids {holders}); could not {doing} "
            f"after waiting {wait:.0f}s"
        )
        fix_hint = (
            "wait for the other ingest to finish, then rerun; or raise the "
            f"wait with --sync-wait-seconds or {SOURCE_SYNC_RUN_WAIT_ENV}"
        )
    else:
        message = (
            f"runtime source-sync workflow {blocker} is queued or running on "
            f"deployment {deployment!r}; could not {doing} after waiting "
            f"{wait:.0f}s"
        )
        fix_hint = (
            "if the runtime worker is running, wait for that workflow to "
            f"finish; if the worker is stopped, start it (`okg runtime worker "
            f"--deployment {deployment}`) or cancel the stranded workflow "
            f"(`okg runtime cancel-workflow {blocker} --deployment "
            f"{deployment}`), then rerun"
        )
    return SourceSyncAlreadyRunning(
        message,
        data={
            "deployment": deployment,
            "wait_seconds": wait,
            "blocking_id": blocker,
            "holder_pids": holders,
        },
        fix_hint=fix_hint,
    )


def _wait_for_intent_lock(
    conn: psycopg.Connection,
    deployment: str,
    *,
    started: float,
    wait: float,
    runtime_probe: Callable[[], str | None] | None,
    action: str,
    poll_seconds: float = 0.5,
) -> None:
    """Take the admission-intent lock on ``conn`` within ``wait`` seconds.

    With ``runtime_probe``, a queued or running worker sync also blocks; the
    lock is released while waiting, so the worker's own enqueues still pass.
    Logs when it starts waiting and every 30 s after.
    """
    classid, objid = source_sync_admission_intent_advisory_keys(deployment)
    deadline = started + wait
    log = logging.getLogger(__name__)
    next_log = started
    waited = False
    while True:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s, %s)", (classid, objid))
            acquired = bool(cur.fetchone()[0])
        blocker: str | None = SOURCE_SYNC_ADMISSION_INTENT_ID
        holders: list[int] = []
        if acquired:
            blocker = runtime_probe() if runtime_probe else None
            if blocker is None:
                if waited:
                    log.warning(
                        "source-sync signal acquired after %.1fs: "
                        "deployment=%s action=%s",
                        time.monotonic() - started, deployment, action,
                    )
                return
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s, %s)", (classid, objid))
        else:
            holders = _intent_holder_pids(conn, deployment)
        now = time.monotonic()
        if now >= deadline:
            raise _already_running(
                deployment, wait, blocker, holders, action=action,
            )
        if now >= next_log:
            log.warning(
                "source sync waiting for another sync: deployment=%s "
                "action=%s blocking=%s holder_pids=%s waited=%.0fs "
                "wait_seconds=%.0f",
                deployment, action, blocker, holders, now - started, wait,
            )
            next_log = now + _WAIT_PROGRESS_LOG_SECONDS
        waited = True
        time.sleep(min(poll_seconds, max(0.01, deadline - now)))


@contextmanager
def source_sync_run_guard(
    *,
    dsn: str,
    deployment: str,
    wait_seconds: float | None = None,
    runtime_probe: Callable[[], str | None] | None = None,
    poll_seconds: float = 0.5,
) -> Iterator[int]:
    """Hold the source-sync signal for a whole run outside the worker.

    Takes the admission-intent advisory lock on a dedicated session and keeps
    it until the run, including its own publish, returns. While another
    session holds it, or ``runtime_probe`` names a queued or running worker
    sync, waits up to ``wait_seconds`` and then raises
    ``source_sync_already_running``. It is a session lock: a killed process
    drops its session and PostgreSQL releases it. The session turns off
    ``idle_session_timeout`` so a quiet run cannot lose it; a publish inside
    the run checks the hold is still there. Re-entering for the same
    deployment in the same context reuses the held lock.
    """
    held = _RUN_GUARDS.get()
    if deployment in held:
        yield held[deployment].pid
        return
    wait = source_sync_run_wait_seconds(wait_seconds)
    classid, objid = source_sync_admission_intent_advisory_keys(deployment)
    started = time.monotonic()
    conn = db_connect(
        dsn,
        connect_timeout=5,
        autocommit=True,
        application_name="okg-source-sync-run-guard",
        options="-c idle_session_timeout=0",
    )
    try:
        _wait_for_intent_lock(
            conn, deployment, started=started, wait=wait,
            runtime_probe=runtime_probe, action="run",
            poll_seconds=poll_seconds,
        )
        pid = int(conn.info.backend_pid)
        token = _RUN_GUARDS.set({
            **held,
            deployment: _HeldRunGuard(
                pid=pid,
                runtime_probe=runtime_probe,
                session=conn,
                session_lock=threading.Lock(),
                backend_start=_session_backend_start(conn),
            ),
        })
        try:
            yield pid
        finally:
            _RUN_GUARDS.reset(token)
            _release_run_guard(conn, deployment, classid, objid, pid)
    finally:
        # Closing the session releases the lock even if the unlock failed.
        conn.close()


def _release_run_guard(
    conn: psycopg.Connection,
    deployment: str,
    classid: int,
    objid: int,
    pid: int,
) -> None:
    """Unlock without ever replacing the run's own result or error.

    A dead session (killed backend, network drop) already lost the lock; the
    failure is logged and the run's outcome stands.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s, %s)", (classid, objid))
            released = bool(cur.fetchone()[0])
    except psycopg.Error as exc:
        logging.getLogger(__name__).warning(
            "source-sync signal release failed; the session is gone, so the "
            "lock went with it: deployment=%s pid=%s error=%s: %s",
            deployment, pid, type(exc).__name__, exc,
        )
        return
    if not released:
        logging.getLogger(__name__).warning(
            "source-sync signal was not held at release: deployment=%s pid=%s",
            deployment, pid,
        )


def publisher_publish_lock_holder_id(
    *,
    dsn: str,
    deployment: str,
) -> str | None:
    classid, objid = publisher_publish_advisory_keys(deployment)
    with db_connect(dsn, connect_timeout=5) as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s, %s)", (classid, objid))
        acquired = bool(cur.fetchone()[0])
        if not acquired:
            return PUBLISHER_ADVISORY_LOCK_HELD_ID
        cur.execute("SELECT pg_advisory_unlock(%s, %s)", (classid, objid))
    return None
