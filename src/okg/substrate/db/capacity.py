"""Postgres connection capacity: what the server has, and whether we fit.

Two consumers, one implementation. The backup planner already probed
``max_connections`` / ``superuser_reserved_connections`` / active backends
inline to size a parallel ``pg_dump``; the boot admission check needs exactly
the same numbers to decide whether this process's declared concurrency fits.
Building a second, divergent probe was the specific thing
``req.unify-connection-budget.boot-admission`` prohibits.

**Two tiers, because one sum is a category error.**

*This process* is knowable at boot: the registry declares every width, so the
demand is computable and can be refused fail-closed before anything opens a
pool. That is what :func:`admit_process_demand` does.

*The host* is not. The MCP server is a separate process started per agent
session, several deployments' workers can share one Postgres, and none of them
tell each other. A worker that assumes it is alone is wrong under exactly the
agent concurrency this program exists to protect; a worker that assumes the
worst case refuses valid deployments. Host-wide accounting therefore has to be
a *reservation* each process records and re-checks, not a sum any one process
computes — see :func:`observed_headroom`, which reports what is actually free
right now rather than pretending to predict it.

Nothing here clamps. An over-subscribed declaration raises with the
overspending components named, because the alternative — quietly shrinking a
width — reproduces the mid-publish ``PoolTimeout`` attributed to whichever lane
happened to ask last.
"""
from __future__ import annotations

import atexit
import logging
import os
import secrets
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from okg.substrate.db.concurrency_budget import BudgetError, connection_demand

__all__ = [
    "RESERVATION_HEARTBEAT_INTERVAL_SECONDS",
    "RESERVATION_STALE_AFTER_SECONDS",
    "ReservationLifecycleError",
    "ServerCapacity",
    "admit_process_demand",
    "observed_headroom",
    "release_process_reservations",
    "release_reservation",
    "reserve_process_demand",
    "server_capacity",
    "usage_by_application",
]

#: A reservation older than this is ignored and swept. A crashed worker must
#: not hold its claim forever, or the restart of that same worker is refused
#: against its own corpse. Live processes refresh their heartbeat.
RESERVATION_STALE_AFTER_SECONDS = 300

#: Refresh at one fifth of the stale horizon. One write per minute per process
#: keeps the shared-table load small while leaving four missed writes of margin
#: before a live owner would become eligible for sweeping.
RESERVATION_HEARTBEAT_INTERVAL_SECONDS = 60

#: A heartbeat must finish well inside its interval. If the database cannot
#: accept refreshes for a whole lease, the owner self-fences before another
#: admission may sweep its row and under-count still-open pools.
_RESERVATION_HEARTBEAT_IO_TIMEOUT_SECONDS = 5

#: Serializes the read-then-claim so two simultaneous boots cannot both pass.
#: Without it each reads the other's absence and both are admitted — the exact
#: race a reservation exists to close.
_RESERVATION_LOCK_KEY = 0x0C0FFEE1

#: Refusals raised by the okg.connection_reservation_* functions (views.sql).
#: The MCP read role has no privilege on okg.connection_reservations and claims
#: through those functions instead; these SQLSTATEs are how a refusal comes
#: back. OKR01 is the budget refusal and OKR02 a demand above the MCP maximum,
#: both declaration problems that refuse startup as :class:`BudgetError`, the
#: same as the direct path. OKR03 (malformed claim) and OKR04 (the row is held
#: under another holder token) propagate as the database error they are.
_CLAIM_BUDGET_SQLSTATES = frozenset({"OKR01", "OKR02"})

log = logging.getLogger(__name__)


class ReservationLifecycleError(RuntimeError):
    """A recorded reservation could not acquire its process-local owner."""


@dataclass
class _ReservationOwner:
    """Process-local owner for one durable connection reservation."""

    dsn: str
    application_name: str
    pid: int
    heartbeat_interval_seconds: float
    stale_after_seconds: float
    lease_lost_handler: Callable[[str], None]
    stop: threading.Event
    last_success_monotonic: float
    thread: threading.Thread | None = None
    #: Set when the row was claimed through okg.connection_reservation_claim
    #: (the MCP read role). Heartbeat and release then go through the matching
    #: functions and present this token; None means direct table access.
    holder_token: str | None = None


_RESERVATION_OWNERS_LOCK = threading.Lock()
_RESERVATION_OWNERS: dict[tuple[str, str, int], _ReservationOwner] = {}

#: Holder tokens of rows this process claimed through the functions, by the
#: same key as the owners. Kept apart from the owners because a row recorded
#: with ``heartbeat_interval_seconds=None`` has no owner and still needs its
#: token to be released. Process memory only: never logged, never stored.
_HOLDER_TOKENS: dict[tuple[str, str, int], str] = {}

#: MCP re-claims in progress, by the same key: the cancel signal and the thread
#: running the re-claim. Release sets the signal, removes the entry and joins
#: the thread, so a re-claim never outlives the server's release.
_RECLAIM_CANCEL: dict[
    tuple[str, str, int], tuple[threading.Event, threading.Thread]
] = {}

#: Holder tokens of rows a lost MCP claim may have left behind, by the same
#: key. A token stays here until its row is released through the SECURITY
#: DEFINER function, by the re-claim loop or by release, whichever runs first.
_STALE_TOKENS: dict[tuple[str, str, int], list[str]] = {}

#: Keys whose rows this process writes through the MCP functions. Release of
#: such a key only ever calls okg.connection_reservation_release; the MCP read
#: role has no privilege for the direct DELETE the worker path uses.
_MCP_KEYS: set[tuple[str, str, int]] = set()

#: Application names whose release a caller asked for, and "*" for a request to
#: release everything. The test-suite leak reporter (tests/conftest.py) reads
#: this to tell a fixture that releases its own claim from a real leak.
_RELEASE_REQUESTS: set[str] = set()

#: While a re-claim keeps being refused for budget, log at most this often.
_REFUSED_RECLAIM_LOG_INTERVAL_SECONDS = 600.0


def _reservation_owner_key(
    dsn: str, application_name: str, pid: int | None = None,
) -> tuple[str, str, int]:
    return (dsn, application_name, os.getpid() if pid is None else pid)


def _exit_after_reservation_loss(reason: str) -> None:
    """Fail closed when this process can no longer prove it owns capacity.

    Raising from a background thread would terminate only that thread. A hard
    process exit is deliberate: once the lease can be swept, continuing to hold
    pools would recreate the host-wide under-count the reservation protects.
    """
    log.critical("connection reservation lost; terminating process: %s", reason)
    os._exit(70)


def _heartbeat_io_timeout_seconds(interval_seconds: float) -> int:
    return max(
        1,
        min(
            _RESERVATION_HEARTBEAT_IO_TIMEOUT_SECONDS,
            int(interval_seconds),
        ),
    )


def _reservation_lease_lost(
    owner: _ReservationOwner, *, reason: str,
) -> None:
    if owner.stop.is_set():
        return
    owner.stop.set()
    log.critical(
        "connection reservation lease lost for %s (pid %d): %s",
        owner.application_name,
        owner.pid,
        reason,
    )
    owner.lease_lost_handler(reason)


def _refresh_reservation(owner: _ReservationOwner) -> None:
    """Refresh one owned row without ever inserting or recreating it."""
    from okg.substrate.db.connection import connect as db_connect

    timeout_seconds = _heartbeat_io_timeout_seconds(
        owner.heartbeat_interval_seconds
    )
    try:
        with db_connect(
            owner.dsn,
            autocommit=True,
            connect_timeout=timeout_seconds,
            options=f"-c statement_timeout={timeout_seconds * 1000}",
            application_name="okg-reservation-heartbeat",
        ) as conn:
            with conn.cursor() as cur:
                if owner.holder_token is not None:
                    cur.execute(
                        "SELECT okg.connection_reservation_heartbeat("
                        "%s, %s, %s)",
                        (owner.application_name, owner.pid, owner.holder_token),
                    )
                    updated = int(cur.fetchone()[0])
                else:
                    cur.execute(
                        "UPDATE okg.connection_reservations "
                        "   SET heartbeat_at = now() "
                        " WHERE application_name = %s AND pid = %s",
                        (owner.application_name, owner.pid),
                    )
                    updated = cur.rowcount
    except Exception as exc:  # noqa: BLE001 - bounded retry until self-fence
        if owner.stop.is_set():
            return
        elapsed = time.monotonic() - owner.last_success_monotonic
        log.warning(
            "could not refresh connection reservation for %s (pid %d): %s; "
            "last successful heartbeat %.1fs ago",
            owner.application_name,
            owner.pid,
            exc,
            elapsed,
        )
        self_fence_after = (
            owner.stale_after_seconds - owner.heartbeat_interval_seconds
        )
        if elapsed >= self_fence_after:
            _reservation_lease_lost(
                owner,
                reason=(
                    f"no successful heartbeat for {elapsed:.1f}s "
                    f"(self-fence threshold {self_fence_after:.1f}s)"
                ),
            )
        return

    # Release sets stop before it deletes. If an UPDATE was already in flight,
    # zero rows is the expected result after that intentional delete and must
    # not trigger process self-fencing.
    if owner.stop.is_set():
        return
    if updated != 1:
        _reservation_lease_lost(
            owner,
            reason=(
                "heartbeat UPDATE no longer owns exactly one "
                f"(application_name, pid) row; updated={updated}"
            ),
        )
        return
    owner.last_success_monotonic = time.monotonic()


def _reservation_heartbeat_loop(owner: _ReservationOwner) -> None:
    self_fence_after = (
        owner.stale_after_seconds - owner.heartbeat_interval_seconds
    )
    while not owner.stop.is_set():
        elapsed = time.monotonic() - owner.last_success_monotonic
        remaining_before_fence = self_fence_after - elapsed
        if remaining_before_fence <= 0:
            _reservation_lease_lost(
                owner,
                reason=(
                    f"no successful heartbeat for {elapsed:.1f}s "
                    f"(self-fence threshold {self_fence_after:.1f}s)"
                ),
            )
            return
        # Failed database attempts consume part of the lease. Cap the next
        # wait by the remaining lease margin instead of sleeping a fresh full
        # interval after each attempt, which could drift past the 240s fence.
        if owner.stop.wait(
            min(owner.heartbeat_interval_seconds, remaining_before_fence)
        ):
            return
        if (
            time.monotonic() - owner.last_success_monotonic
            >= self_fence_after
        ):
            # Re-enter at the deadline so the lease-loss path runs instead of
            # starting an I/O attempt that could finish after the stale sweep.
            continue
        _refresh_reservation(owner)


def _new_reservation_heartbeat_thread(
    owner: _ReservationOwner,
) -> threading.Thread:
    return threading.Thread(
        target=_reservation_heartbeat_loop,
        args=(owner,),
        name=f"okg-reservation-heartbeat-{owner.application_name}",
        daemon=True,
    )


def _stop_reservation_owner(owner: _ReservationOwner) -> None:
    owner.stop.set()
    thread = owner.thread
    if thread is None or thread is threading.current_thread():
        return
    join_timeout = max(
        1.0,
        min(
            owner.heartbeat_interval_seconds,
            _RESERVATION_HEARTBEAT_IO_TIMEOUT_SECONDS + 1.0,
        ),
    )
    try:
        thread.join(timeout=join_timeout)
    except RuntimeError:
        # A thread whose start failed is not joinable. The row cleanup below
        # still runs and the original startup exception still propagates.
        return
    if thread.is_alive():
        log.warning(
            "connection reservation heartbeat for %s did not stop within "
            "%.1fs; deleting with UPDATE-only race safety",
            owner.application_name,
            join_timeout,
        )


def _start_reservation_owner(
    dsn: str,
    *,
    application_name: str,
    heartbeat_interval_seconds: float,
    stale_after_seconds: float,
    lease_lost_handler: Callable[[str], None],
    holder_token: str | None = None,
) -> None:
    owner = _ReservationOwner(
        dsn=dsn,
        application_name=application_name,
        pid=os.getpid(),
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        stale_after_seconds=stale_after_seconds,
        lease_lost_handler=lease_lost_handler,
        stop=threading.Event(),
        last_success_monotonic=time.monotonic(),
        holder_token=holder_token,
    )
    owner.thread = _new_reservation_heartbeat_thread(owner)
    key = _reservation_owner_key(dsn, application_name, owner.pid)
    with _RESERVATION_OWNERS_LOCK:
        previous = _RESERVATION_OWNERS.get(key)
        _RESERVATION_OWNERS[key] = owner
    if previous is not None:
        _stop_reservation_owner(previous)
    try:
        owner.thread.start()
    except BaseException as exc:
        # The row was already inserted. Lifecycle startup is atomic from the
        # caller's perspective: no owner means no durable claim remains.
        release_reservation(dsn, application_name=application_name)
        if not isinstance(exc, Exception):
            raise
        raise ReservationLifecycleError(
            "connection reservation was recorded, but its heartbeat owner "
            f"could not start: {exc}"
        ) from exc


@dataclass(frozen=True)
class ServerCapacity:
    """What the server reports about its own connection limits."""

    max_connections: int
    superuser_reserved: int
    active_client_backends: int

    @property
    def usable(self) -> int:
        """Connections a non-superuser client may hold in total."""
        return self.max_connections - self.superuser_reserved

    @property
    def available(self) -> int:
        """Connections free right now, i.e. usable minus what is in use.

        Includes this process's own existing backends, so a caller that has
        already opened connections sees them as consumed rather than free.

        The probe's OWN connection is excluded: it is counted by
        ``pg_stat_activity`` while the probe runs but is closed immediately
        after, so leaving it in would under-report by one and refuse a
        declaration that exactly fits. Never below zero.
        """
        return max(0, self.usable - max(0, self.active_client_backends - 1))


def server_capacity(dsn: str, *, connect_timeout: int = 5) -> ServerCapacity:
    """Probe the server's limits. One short-lived connection."""
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(
        dsn, connect_timeout=connect_timeout,
        application_name="okg-capacity-probe",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW max_connections")
            max_connections = int(cur.fetchone()[0])
            cur.execute("SHOW superuser_reserved_connections")
            reserved = int(cur.fetchone()[0])
            cur.execute(
                "SELECT count(*)::int FROM pg_stat_activity "
                "WHERE backend_type = 'client backend'"
            )
            active = int(cur.fetchone()[0])
    return ServerCapacity(
        max_connections=max_connections,
        superuser_reserved=reserved,
        active_client_backends=active,
    )


def usage_by_application(dsn: str, *, connect_timeout: int = 5) -> dict[str, int]:
    """Live client backends grouped by ``application_name``.

    Attribution is the difference between "the server is full" and "THIS
    component is holding them". Most substrate connections stamp an
    application_name (``okg-pool``, ``okg-llm-telemetry``,
    ``okg-publisher-dbos``, ``okg-capacity-probe``, ``okg-reservation``, the
    MCP server's own), but NOT all: a live probe of a real publish cycle
    found an ``<unnamed>`` bucket, so the breakdown is best-effort and its
    unnamed rows are reported as such rather than silently attributed.

    It measures what is connected NOW. It cannot see a process that has
    declared a width but not yet opened its pool, so two workers booting
    simultaneously can still each be admitted against the same free
    connections. Closing that needs a durable reservation each process records
    before opening anything; see the host-tier note in
    `unify-connection-budget`.
    """
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(
        dsn, connect_timeout=connect_timeout,
        application_name="okg-capacity-probe",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT coalesce(nullif(application_name, ''), '<unnamed>'), "
                "       count(*)::int "
                "  FROM pg_stat_activity "
                " WHERE backend_type = 'client backend' "
                " GROUP BY 1 ORDER BY 2 DESC"
            )
            return {str(row[0]): int(row[1]) for row in cur.fetchall()}


def observed_headroom(capacity: ServerCapacity, demand: int) -> int:
    """Connections left over if this process took its full declared demand.

    Negative means the process cannot fit alongside what is already connected.
    This is the honest host-wide signal: it measures what is actually in use
    rather than summing what other processes might declare.
    """
    return capacity.available - demand


def reserve_process_demand(
    dsn: str,
    *,
    application_name: str,
    deployment: str | None = None,
    capacity: ServerCapacity | None = None,
    include_mcp: bool = False,
    stale_after_seconds: float = RESERVATION_STALE_AFTER_SECONDS,
    heartbeat_interval_seconds: float | None = (
        RESERVATION_HEARTBEAT_INTERVAL_SECONDS
    ),
    lease_lost_handler: Callable[[str], None] | None = None,
    mcp_read_role: bool = False,
    demand: int | None = None,
) -> dict[str, int]:
    """Claim this process's declared demand host-wide, or refuse.

    The per-process tier (:func:`admit_process_demand`) asks "does my demand
    fit what is free right now". That is necessary and insufficient: two
    workers booting at the same moment each see the other's connections
    absent, and both are admitted against the same headroom. This records the
    claim BEFORE any pool opens, under an advisory lock so the read and the
    claim cannot interleave, and refuses when the sum of live claims plus this
    one exceeds what the server can serve.

    Returns the demand breakdown on success. Raises :class:`BudgetError`
    naming the other holders on refusal — an operator needs to know WHICH
    process is holding the connections, not merely that some are gone.

    A daemon heartbeat owns the successful row until
    :func:`release_reservation` stops it. Reservations staler than
    `stale_after_seconds` are ignored and swept for exits that cannot execute
    cleanup (SIGKILL, fatal crash, host loss). If heartbeat writes fail for a
    full lease, the default owner self-fences one interval before expiry so a
    still-running process never becomes invisible to later admissions.

    `heartbeat_interval_seconds=None` is an explicit test/primitive mode that
    records a row without starting lifecycle ownership. Production callers use
    the 60-second default against the 300-second stale horizon.

    `mcp_read_role=True` is for a DSN that connects as ``okg_mcp`` or a member
    of it (the MCP server's read DSN). That role holds no privilege on
    ``okg.connection_reservations``; the claim, heartbeat and release go
    through the ``okg.connection_reservation_*`` SECURITY DEFINER functions
    with a random holder token this process keeps in memory. The functions
    fix the stale horizon (300 s) and read the server's usable connection
    count themselves, so this mode refuses a ``capacity`` or a different
    ``stale_after_seconds`` instead of ignoring it. Every other caller keeps
    direct table access and is unchanged.

    `demand` replaces the declared sum with the exact number of connections
    the caller can open. It is for a process that serves one call and exits
    (an `okg` read command, whose pool has a hard maximum): such a caller
    passes `heartbeat_interval_seconds=None`, releases the claim when it
    ends, and a claim it could not release (the process was killed) stops
    counting after `stale_after_seconds`. Every other caller leaves it None.
    """
    if demand is not None and (
        isinstance(demand, bool) or not isinstance(demand, int) or demand <= 0
    ):
        raise ValueError("demand must be a positive integer")
    if mcp_read_role:
        if capacity is not None:
            raise ValueError(
                "mcp_read_role claims read the server's usable connection "
                "count server side; do not pass capacity"
            )
        if float(stale_after_seconds) != float(RESERVATION_STALE_AFTER_SECONDS):
            raise ValueError(
                "mcp_read_role claims use the server-side stale horizon of "
                f"{RESERVATION_STALE_AFTER_SECONDS}s; stale_after_seconds "
                "cannot be changed"
            )
    from okg.substrate.db.connection import connect as db_connect

    stale_after_seconds = float(stale_after_seconds)
    if stale_after_seconds <= 0:
        raise ValueError("stale_after_seconds must be positive")
    if heartbeat_interval_seconds is not None:
        heartbeat_interval_seconds = float(heartbeat_interval_seconds)
        if heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be positive")
        if heartbeat_interval_seconds * 2 > stale_after_seconds:
            raise ValueError(
                "heartbeat_interval_seconds must be at most half of "
                "stale_after_seconds"
            )

    exact_demand = demand
    demand = (
        connection_demand(include_mcp=include_mcp)
        if exact_demand is None
        else {"_total": exact_demand}
    )
    mine = demand["_total"]
    pid = os.getpid()
    if mcp_read_role:
        held, usable, holder_token = _claim_through_functions(
            dsn,
            application_name=application_name,
            deployment=deployment,
            demand=mine,
            pid=pid,
        )
        log.info(
            "reserved %d connections for %s (pid %d) through the MCP "
            "reservation functions; other live claims %d, server usable %d",
            mine, application_name, pid, held, usable,
        )
        if heartbeat_interval_seconds is not None:
            _start_reservation_owner(
                dsn,
                application_name=application_name,
                heartbeat_interval_seconds=heartbeat_interval_seconds,
                stale_after_seconds=stale_after_seconds,
                lease_lost_handler=(
                    lease_lost_handler
                    or _mcp_reclaim_handler(
                        dsn,
                        application_name=application_name,
                        deployment=deployment,
                        include_mcp=include_mcp,
                        heartbeat_interval_seconds=heartbeat_interval_seconds,
                    )
                ),
                holder_token=holder_token,
            )
        return demand

    cap = capacity if capacity is not None else server_capacity(dsn)

    with db_connect(
        dsn, autocommit=False, application_name="okg-reservation",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (_RESERVATION_LOCK_KEY,))
            cur.execute(
                "DELETE FROM okg.connection_reservations "
                " WHERE heartbeat_at < now() - make_interval(secs => %s)",
                (stale_after_seconds,),
            )
            cur.execute(
                "SELECT application_name, pid, declared_demand "
                "  FROM okg.connection_reservations "
                " WHERE NOT (application_name = %s AND pid = %s)",
                (application_name, pid),
            )
            others = [(str(r[0]), int(r[1]), int(r[2])) for r in cur.fetchall()]
            held = sum(d for _, _, d in others)

            if held + mine > cap.usable:
                holders = ", ".join(
                    f"{name}(pid {p})={d}" for name, p, d in sorted(others)
                ) or "none"
                conn.rollback()
                raise BudgetError(
                    f"host-wide connection reservations cannot fit this "
                    f"process: {application_name} needs {mine}, other live "
                    f"claims hold {held} [{holders}], and the server can serve "
                    f"{cap.usable} (max_connections={cap.max_connections} minus "
                    f"{cap.superuser_reserved} superuser-reserved). Short by "
                    f"{held + mine - cap.usable}. Stop one of the holders, "
                    f"narrow a declared width, or raise max_connections. "
                    f"Refused before opening any pool, so nothing was taken "
                    f"from the processes already running."
                )

            cur.execute(
                """
                INSERT INTO okg.connection_reservations
                    (application_name, pid, deployment_name, declared_demand,
                     claimed_at, heartbeat_at)
                VALUES (%s, %s, %s, %s, now(), now())
                ON CONFLICT (application_name, pid) DO UPDATE
                   SET declared_demand = EXCLUDED.declared_demand,
                       deployment_name = EXCLUDED.deployment_name,
                       heartbeat_at = now()
                """,
                (application_name, pid, deployment, mine),
            )
        conn.commit()

    log.info(
        "reserved %d connections for %s (pid %d); other live claims %d, "
        "server usable %d",
        mine, application_name, pid, held, cap.usable,
    )
    if heartbeat_interval_seconds is not None:
        _start_reservation_owner(
            dsn,
            application_name=application_name,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            stale_after_seconds=stale_after_seconds,
            lease_lost_handler=(
                lease_lost_handler or _exit_after_reservation_loss
            ),
        )
    return demand


def _claim_through_functions(
    dsn: str,
    *,
    application_name: str,
    deployment: str | None,
    demand: int,
    pid: int,
) -> tuple[int, int, str]:
    """Claim through okg.connection_reservation_claim; return
    (other live claims, server usable, holder token).

    A re-claim by this process reuses its token, because the function refuses
    a claim on a row held under any other token. The token is a bound
    parameter, never SQL text, so ``pg_stat_activity`` does not show it.
    """
    import psycopg

    from okg.substrate.db.connection import connect as db_connect

    key = _reservation_owner_key(dsn, application_name, pid)
    with _RESERVATION_OWNERS_LOCK:
        holder_token = _HOLDER_TOKENS.get(key) or secrets.token_hex(32)
    try:
        with db_connect(
            dsn, autocommit=False, application_name="okg-reservation",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT other_claims, usable "
                    "  FROM okg.connection_reservation_claim(%s, %s, %s, %s, %s)",
                    (application_name, pid, deployment, demand, holder_token),
                )
                held, usable = cur.fetchone()
            conn.commit()
    except psycopg.Error as exc:
        if exc.sqlstate in _CLAIM_BUDGET_SQLSTATES:
            raise BudgetError(
                exc.diag.message_primary or str(exc)
            ) from exc
        raise
    with _RESERVATION_OWNERS_LOCK:
        _HOLDER_TOKENS[key] = holder_token
        _MCP_KEYS.add(key)
    return int(held), int(usable), holder_token


def _mcp_reclaim_handler(
    dsn: str,
    *,
    application_name: str,
    deployment: str | None,
    include_mcp: bool,
    heartbeat_interval_seconds: float,
) -> Callable[[str], None]:
    """Lease-lost handler for an MCP read-role claim: re-claim, never exit.

    The worker keeps the hard exit, because a worker that runs on after its
    claim is gone takes connections a later admission counted as free. A
    read-only MCP server is different (see the reservation comment in
    mcp/server.py): stopping an agent's session over a bookkeeping row is
    worse than the risk the row manages. The two ways a live MCP claim is
    lost are ordinary. The row is swept after a laptop sleep (the heartbeat's
    monotonic clock stops, the database clock does not), or every heartbeat
    fails through a database outage longer than the self-fence window.

    So: log, unregister the stopped owner, release the old row through the
    function if it is still there, and claim again with a new holder token.
    Every failure is retried each heartbeat interval until the claim succeeds
    or the server releases:

    * an infrastructure failure is logged on every attempt;
    * a budget refusal is logged once with the fix, then at most once every
      ``_REFUSED_RECLAIM_LOG_INTERVAL_SECONDS`` while it lasts, and the
      recovery is logged once. Meanwhile the server keeps serving without a
      recorded claim, so later admissions count its connections as free until
      the claim fits again. This differs from startup on purpose: at startup
      a budget refusal refuses to start, because nothing is being served yet.
    """
    key = _reservation_owner_key(dsn, application_name)

    def _handle(reason: str) -> None:
        from okg.substrate.db.concurrency_budget import BudgetError

        with _RESERVATION_OWNERS_LOCK:
            if key not in _RESERVATION_OWNERS:
                return  # released while the lease was being lost
            # The stopped owner no longer represents a live claim.
            del _RESERVATION_OWNERS[key]
            cancel = threading.Event()
            _RECLAIM_CANCEL[key] = (cancel, threading.current_thread())
            old_token = _HOLDER_TOKENS.pop(key, None)
            if old_token is not None:
                _STALE_TOKENS.setdefault(key, []).append(old_token)
        log.warning(
            "MCP connection reservation for %s (pid %d) was lost (%s); "
            "re-claiming it. The server keeps serving.",
            application_name, key[2], reason,
        )
        refusals = 0
        last_refusal_logged: float | None = None
        try:
            while not cancel.is_set():
                _release_stale_tokens(dsn, application_name=application_name)
                try:
                    reserve_process_demand(
                        dsn,
                        application_name=application_name,
                        deployment=deployment,
                        include_mcp=include_mcp,
                        heartbeat_interval_seconds=heartbeat_interval_seconds,
                        mcp_read_role=True,
                    )
                except BudgetError as exc:
                    refusals += 1
                    now = time.monotonic()
                    if (
                        last_refusal_logged is None
                        or now - last_refusal_logged
                        >= _REFUSED_RECLAIM_LOG_INTERVAL_SECONDS
                    ):
                        last_refusal_logged = now
                        log.warning(
                            "MCP connection reservation for %s could not be "
                            "re-claimed (%d attempt%s so far): %s The server "
                            "keeps serving without a recorded reservation, so "
                            "later admissions count its connections as free, "
                            "and it retries every %.0fs until the claim fits. "
                            "Fix: stop another holder, narrow a declared "
                            "width, or raise max_connections.",
                            application_name, refusals,
                            "" if refusals == 1 else "s", exc,
                            heartbeat_interval_seconds,
                        )
                    cancel.wait(heartbeat_interval_seconds)
                    continue
                except Exception as exc:  # noqa: BLE001 - retried below
                    log.warning(
                        "MCP connection reservation for %s could not be "
                        "re-claimed (%s); retrying in %.0fs",
                        application_name, exc, heartbeat_interval_seconds,
                    )
                    cancel.wait(heartbeat_interval_seconds)
                    continue
                if cancel.is_set():
                    # Released while this claim was being recorded.
                    release_reservation(dsn, application_name=application_name)
                    return
                if refusals:
                    log.info(
                        "MCP connection reservation for %s re-claimed after "
                        "%d budget refusal%s",
                        application_name, refusals,
                        "" if refusals == 1 else "s",
                    )
                else:
                    log.info("MCP connection reservation for %s re-claimed",
                             application_name)
                return
        finally:
            with _RESERVATION_OWNERS_LOCK:
                entry = _RECLAIM_CANCEL.get(key)
                if entry is not None and entry[0] is cancel:
                    del _RECLAIM_CANCEL[key]

    return _handle


def _release_stale_tokens(dsn: str, *, application_name: str) -> bool:
    """Release every stale token's row for this key; True when none is left."""
    key = _reservation_owner_key(dsn, application_name)
    with _RESERVATION_OWNERS_LOCK:
        tokens = list(_STALE_TOKENS.get(key, ()))
    for token in tokens:
        if _release_through_function(
            dsn, application_name=application_name, holder_token=token,
        ):
            with _RESERVATION_OWNERS_LOCK:
                remaining = _STALE_TOKENS.get(key)
                if remaining is not None and token in remaining:
                    remaining.remove(token)
                    if not remaining:
                        del _STALE_TOKENS[key]
    with _RESERVATION_OWNERS_LOCK:
        return key not in _STALE_TOKENS


def _release_through_function(
    dsn: str, *, application_name: str, holder_token: str,
) -> bool:
    """Release one token's row through the function; True when the call ran.

    Bounded like the heartbeat: a connect timeout and a statement timeout, so
    a release stuck on a lock cannot hang the re-claim or the shutdown path.
    """
    from okg.substrate.db.connection import connect as db_connect

    try:
        with db_connect(
            dsn,
            autocommit=True,
            connect_timeout=_RESERVATION_HEARTBEAT_IO_TIMEOUT_SECONDS,
            options=(
                "-c statement_timeout="
                f"{_RESERVATION_HEARTBEAT_IO_TIMEOUT_SECONDS * 1000}"
            ),
            application_name="okg-reservation",
        ) as conn:
            conn.execute(
                "SELECT okg.connection_reservation_release(%s, %s, %s)",
                (application_name, os.getpid(), holder_token),
            )
        return True
    except Exception as exc:  # noqa: BLE001 - the stale sweep removes it anyway
        log.warning(
            "could not release MCP connection reservation for %s: %s; it will "
            "be swept after %ds", application_name, exc,
            RESERVATION_STALE_AFTER_SECONDS,
        )
        return False


def release_reservation(dsn: str, *, application_name: str) -> None:
    """Drop this process's claim. Best-effort: a missed release is swept by the
    staleness horizon, so failing here must not take down a shutdown path.

    An MCP-mode key is released only through okg.connection_reservation_release,
    for its current token and every stale token a lost claim left; the MCP
    read role has no privilege for the direct DELETE. A re-claim in progress
    is cancelled and its thread joined first.
    """
    from okg.substrate.db.connection import connect as db_connect

    key = _reservation_owner_key(dsn, application_name)
    with _RESERVATION_OWNERS_LOCK:
        reclaim = _RECLAIM_CANCEL.pop(key, None)
        if reclaim is not None:
            reclaim[0].set()
        owner = _RESERVATION_OWNERS.pop(key, None)
        holder_token = _HOLDER_TOKENS.pop(key, None)
        mcp_mode = key in _MCP_KEYS
    if owner is not None:
        _stop_reservation_owner(owner)
    if reclaim is not None and reclaim[1] is not threading.current_thread():
        # A cancelled wait returns at once; a claim call in flight finishes,
        # sees the cancel and releases what it recorded.
        reclaim[1].join(timeout=2 * _RESERVATION_HEARTBEAT_IO_TIMEOUT_SECONDS + 1)

    if mcp_mode:
        if holder_token is not None:
            _release_through_function(
                dsn, application_name=application_name,
                holder_token=holder_token,
            )
        _release_stale_tokens(dsn, application_name=application_name)
        with _RESERVATION_OWNERS_LOCK:
            if (
                key not in _RESERVATION_OWNERS
                and key not in _HOLDER_TOKENS
                and key not in _STALE_TOKENS
                and key not in _RECLAIM_CANCEL
            ):
                _MCP_KEYS.discard(key)
        return

    try:
        with db_connect(
            dsn,
            autocommit=True,
            connect_timeout=_RESERVATION_HEARTBEAT_IO_TIMEOUT_SECONDS,
            options=(
                "-c statement_timeout="
                f"{_RESERVATION_HEARTBEAT_IO_TIMEOUT_SECONDS * 1000}"
            ),
            application_name="okg-reservation",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM okg.connection_reservations "
                    " WHERE application_name = %s AND pid = %s",
                    (application_name, os.getpid()),
                )
    except Exception as exc:  # noqa: BLE001 — swept by staleness anyway
        log.warning(
            "could not release connection reservation for %s: %s; it will be "
            "swept after %ds", application_name, exc,
            RESERVATION_STALE_AFTER_SECONDS,
        )


def release_process_reservations(*, application_name: str) -> None:
    """Release this process's active reservation(s) for an application.

    Callers whose DSN is resolved inside a builder use this outer-lifecycle
    form. The process-local owner registry supplies the exact DSN and delegates
    to :func:`release_reservation`; absent ownership is an idempotent no-op.
    """
    pid = os.getpid()
    with _RESERVATION_OWNERS_LOCK:
        _RELEASE_REQUESTS.add(application_name)
        dsns = sorted({
            dsn
            for dsn, name, owner_pid in _process_keys()
            if name == application_name and owner_pid == pid
        })
    for dsn in dsns:
        release_reservation(dsn, application_name=application_name)


def _process_keys() -> set[tuple[str, str, int]]:
    """Every key this process holds any reservation state for. Call under
    ``_RESERVATION_OWNERS_LOCK``."""
    return {
        *_RESERVATION_OWNERS, *_HOLDER_TOKENS, *_RECLAIM_CANCEL, *_STALE_TOKENS,
    }


def _process_reservation_names() -> list[str]:
    """Application names this process still holds a reservation for."""
    pid = os.getpid()
    with _RESERVATION_OWNERS_LOCK:
        return sorted({
            application_name
            for _dsn, application_name, owner_pid in _process_keys()
            if owner_pid == pid
        })


def _release_owned_reservations() -> None:
    """Release everything this process holds, without recording a request."""
    pid = os.getpid()
    with _RESERVATION_OWNERS_LOCK:
        owned = sorted({
            (dsn, application_name)
            for dsn, application_name, owner_pid in _process_keys()
            if owner_pid == pid
        })
    for dsn, application_name in owned:
        release_reservation(dsn, application_name=application_name)


def _release_all_process_reservations() -> None:
    """Best-effort normal-interpreter-exit backstop."""
    with _RESERVATION_OWNERS_LOCK:
        _RELEASE_REQUESTS.add("*")
    _release_owned_reservations()


atexit.register(_release_all_process_reservations)


def admit_process_demand(
    dsn: str,
    *,
    capacity: ServerCapacity | None = None,
    include_mcp: bool = False,
) -> dict[str, int]:
    """Refuse to boot if this process's declared demand cannot fit.

    Returns the per-term demand breakdown on success. Raises
    :class:`BudgetError` naming the components and the arithmetic on failure —
    an operator should be able to act on the message without reading the
    registry.

    `capacity` is injectable so the arithmetic is testable without a server;
    production callers omit it and the server is probed.

    Note this checks demand against connections free *right now*. A process
    that boots into a busy server is refused, which is intended: booting
    anyway would take the connections it needs from whatever is already
    running, converting a clean refusal into someone else's outage.
    """
    demand = connection_demand(include_mcp=include_mcp)
    total = demand["_total"]
    cap = capacity if capacity is not None else server_capacity(dsn)
    headroom = observed_headroom(cap, total)

    if headroom < 0:
        breakdown = ", ".join(
            f"{name}={count}"
            for name, count in sorted(demand.items())
            if name != "_total"
        )
        holders = ""
        try:
            usage = usage_by_application(dsn)
            if usage:
                holders = " Currently connected, by application_name: " + ", ".join(
                    f"{app}={n}" for app, n in list(usage.items())[:8]
                ) + "."
        except Exception:  # noqa: BLE001 — diagnostic only; never mask the refusal
            holders = ""
        raise BudgetError(
            f"declared connection demand does not fit this server: this "
            f"process needs {total} ({breakdown}), but only {cap.available} "
            f"are free (max_connections={cap.max_connections} minus "
            f"{cap.superuser_reserved} superuser-reserved minus "
            f"{cap.active_client_backends} already-active client backends). "
            f"Short by {-headroom}.{holders} Narrow a declared width, raise "
            f"max_connections, or stop whatever is holding those connections. "
            f"Refused at boot rather than clamped, because "
            f"booting under-provisioned surfaces later as a PoolTimeout "
            f"mid-publish attributed to whichever lane asked last."
        )

    log.info(
        "connection admission ok: demand=%d available=%d headroom=%d "
        "(max_connections=%d, reserved=%d, active=%d)",
        total, cap.available, headroom, cap.max_connections,
        cap.superuser_reserved, cap.active_client_backends,
    )
    return demand
