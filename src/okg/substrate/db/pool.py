"""Process-shared psycopg ConnectionPool, keyed by DSN.

Motivated by `add-per-source-sync-perf-harness` measurement: the
ingest hot path was opening **12 fresh psycopg connections** per
commit (6 in SourceRunner, 2 in projector workers, ~4 in publisher),
each paying a ~11 ms TCP+TLS+auth handshake. That's ~137 ms of
pure overhead per commit before any work.

The runtime config defines `okg_write_pool_size: 4` and
`okg_read_pool_size: 8`, but no `ConnectionPool` was ever
instantiated against them — a pre-wired socket that was never
plugged in. This module is the plug.

Design:

  * One pool per DSN (so multiple deployments coexist).
  * `min_size=1` so the first ingest pays one handshake; subsequent
    ones reuse warm connections.
  * `max_size` defaults to 8, sized to fit the publisher's parallel
    projector workers + the SourceRunner + the lease heartbeat.
  * `autocommit=True` by default — most substrate helpers run
    SELECT/INSERT/UPSERT and don't need explicit transactions;
    those that do (e.g. `_flush_chunk`) wrap their body in
    `with conn.transaction():` which works fine on an
    autocommit=True connection.
  * Pool lifecycle is process-bound. `close_all()` is the
    teardown hook (call from worker shutdown / bench cleanup).

Callers do:

    from okg.substrate.db.pool import get_pool

    pool = get_pool(dsn)
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(...)

The `with` block returns the connection to the pool on exit —
subsequent `pool.connection()` calls reuse it after a lightweight
round-trip health check, so connections severed by a database restart
are discarded before callers receive them.
"""
from __future__ import annotations

import logging
import sys
import threading
from contextlib import contextmanager
from typing import Iterator, Optional

from psycopg import Connection
from psycopg_pool import ConnectionPool, PoolTimeout

from okg.substrate.db.concurrency_budget import (
    BudgetError,
    general_admission_width,
    reserved_slots,
    width,
)
from okg.substrate.db.connection import (
    DEFAULT_CONNECT_TIMEOUT_SECONDS,
    _redact,
    connection_kwargs,
)
from okg.substrate.transform.commit import clear_commit_witness_on_return

__all__ = [
    "GENERAL",
    "RESERVED",
    "PoolReservationExhausted",
    "close_all",
    "configure_source_process_pool",
    "get_pool",
    "pooled_connection",
    "release_session_advisory_locks",
    "reset_returned_connection",
]

log = logging.getLogger(__name__)

_POOLS: dict[str, ConnectionPool] = {}
_LOCK = threading.Lock()

#: The term every pool in this process takes its width from. A source child
#: process (lane H stage 1) switches it to `ingest.source_process` before it
#: opens any connection: the child builds no shared pool and reserves nothing
#: for publishing, because no publish runs in it.
_POOL_TERM = "db.shared_pool"

#: `pooled_connection` purposes.
#:
#: RESERVED is the publish path — the publisher cycle and the invariant
#: gates it runs. GENERAL is every other borrower: ingest lanes, source
#: adapters, scheduled maintenance work.
RESERVED = "reserved"
GENERAL = "general"

#: How long a GENERAL borrower waits for a slot before it is refused. Chosen
#: to be shorter than psycopg_pool's own ~30 s checkout timeout so contention
#: surfaces as a named refusal naming the pool and the reservation, rather
#: than as an unattributed PoolTimeout in whichever lane asked last.
GENERAL_SLOT_WAIT_SECONDS = 10.0

#: Guarded by `_LOCK`. One admission semaphore per DSN, rebuilt whenever the
#: pool object behind that DSN is replaced (a closed pool is reopened at a
#: possibly different width).
_ADMISSION: dict[str, tuple[ConnectionPool, threading.BoundedSemaphore, int]] = {}

#: Per-thread depth of held general admission slots, keyed by DSN. A
#: source holds one slot for its whole run and its adapter borrows a
#: second connection inside that run; the budget counts those as ONE
#: lane (a source child's `ingest.source_process` pool holds both), so the
#: nested borrow reuses the slot instead of waiting for one the parent is
#: itself holding.
_HELD = threading.local()


def release_session_advisory_locks(conn: Connection) -> None:
    """Pool `reset=` callback: drop every session advisory lock on return.

    A session-level advisory lock (`pg_advisory_lock`, `pg_try_advisory_lock`,
    their `_shared` forms) belongs to the database session, not to the
    borrower. The pool's rollback on return does not release it, so a lock a
    borrower forgot, or could not release because it raised first, stayed
    held on an idle pooled connection until that connection aged out -- an
    hour by default. Two such leaks were seen: an MCP `run_sql` query that
    took `pg_advisory_lock_shared` on the history-lease key, and a publisher
    whose source-sync probe raised after it took the publish mutex.

    No okg code holds a session lock across two checkouts: every user takes
    and releases it on one borrowed connection. So nothing legitimate is held
    when a connection comes back, and clearing all of them is safe.

    If this raises, psycopg_pool closes the connection instead of reusing it;
    a closed session releases its locks. The connection is left idle: a
    non-autocommit connection's implicit transaction is rolled back, which
    does not re-take the locks (session lock release is not transactional).
    """
    conn.execute("SELECT pg_advisory_unlock_all()")
    if not conn.autocommit:
        conn.rollback()


def reset_returned_connection(conn: Connection) -> None:
    """Pool `reset=` callback: a returned connection carries no session state
    from its borrower.

    First the borrower's session advisory locks
    (:func:`release_session_advisory_locks`), then its commit-witness stamp
    (:func:`clear_commit_witness_on_return`, issue #2965), so the next
    borrower's unrelated fact writes are not accepted as that program's. Each
    step closes its own transaction: the clear commits, and no rollback runs
    after it, because a rollback there could undo the clear. Nothing is caught:
    if either step raises, psycopg_pool closes the connection, and closing the
    session drops both the locks and the stamp.
    """
    release_session_advisory_locks(conn)
    clear_commit_witness_on_return(conn)


class PoolReservationExhausted(RuntimeError):
    """A general borrower asked for a connection the publish cycle holds back.

    Raised instead of waiting out the pool's own checkout timeout, and never
    downgraded into an unreserved borrow: the reservation exists so a publish
    cycle that has run for hours does not die at its final preflight because
    routine background work took the last connection.
    """


def _general_admission(
    pool: ConnectionPool, dsn: str
) -> threading.BoundedSemaphore:
    """Return the admission semaphore bounding GENERAL borrowers on `pool`.

    Sized by :func:`general_admission_width`, which subtracts both the publish
    reservation and the borrowers that admit themselves elsewhere. Sizing it
    as `max_size - reserved` would oversubscribe the pool instead: the publish
    side holds the publisher cycle connection AND its invariant-gate workers,
    and telemetry holds connections behind its own semaphore.

    A pool opened at a narrower non-default width is bounded by that width, so
    an explicitly small pool is never handed more borrowers than it has
    connections.
    """
    if _POOL_TERM != "db.shared_pool":
        # A source child: no publish cycle runs here, so nothing is held back.
        allowed = pool.max_size
    else:
        allowed = min(
            general_admission_width("db.shared_pool"),
            max(1, pool.max_size - reserved_slots("db.shared_pool")),
        )
    with _LOCK:
        entry = _ADMISSION.get(dsn)
        if entry is not None and entry[0] is pool and entry[2] == allowed:
            return entry[1]
        sem = threading.BoundedSemaphore(allowed)
        _ADMISSION[dsn] = (pool, sem, allowed)
        return sem


@contextmanager
def pooled_connection(
    dsn: str,
    *,
    purpose: str,
    timeout: float | None = None,
    **pool_kwargs,
) -> Iterator[Connection]:
    """Borrow a pooled connection under the publish-cycle reservation.

    `purpose=RESERVED` borrows directly — the publish path may use every slot
    in the pool. `purpose=GENERAL` must first take an admission slot from the
    semaphore returned by :func:`_general_admission`, which is short by the
    declared reservation, so general borrowers can never occupy the whole
    pool. A general borrower that cannot get a slot within
    :data:`GENERAL_SLOT_WAIT_SECONDS` is refused with
    :class:`PoolReservationExhausted`.

    This is the runtime half of `_POOL_RESERVATIONS`. Without it the
    reservation is only arithmetic checked at boot: nothing stopped ingest
    lanes and long-running scheduled workflows from holding every connection
    while a publish preflight waited 30 s and then raised a bare PoolTimeout
    that named no cause (okg-workspace, 2026-09-04: a five-hour repair run
    lost all its staged work that way).
    """
    if purpose not in (RESERVED, GENERAL):
        raise ValueError(
            f"unknown pooled_connection purpose {purpose!r}; "
            f"expected {RESERVED!r} or {GENERAL!r}"
        )
    pool = get_pool(dsn, **pool_kwargs)
    if _POOL_TERM != "db.shared_pool":
        # A source child: one small pool, no publish reservation. Every
        # borrow -- nested ones and the run lock's included -- waits at most
        # GENERAL_SLOT_WAIT_SECONDS and then fails with a named refusal,
        # never psycopg_pool's anonymous 30 s PoolTimeout.
        wait = GENERAL_SLOT_WAIT_SECONDS if timeout is None else timeout
        cm = pool.connection(timeout=wait)
        try:
            conn = cm.__enter__()
        except PoolTimeout:
            raise PoolReservationExhausted(
                f"no connection on the source child's pool {pool.name} within "
                f"{wait:g}s: the pool is {pool.max_size} wide "
                f"(ingest.source_process) and every slot is held. A source "
                f"that needs more connections gets a wider "
                f"ingest.source_process term."
            ) from None
        try:
            yield conn
        except BaseException:
            if not cm.__exit__(*sys.exc_info()):
                raise
        else:
            cm.__exit__(None, None, None)
        return
    # Only pass `timeout` when a caller asked for one: psycopg_pool applies
    # its own default otherwise, and calling `connection()` with no argument
    # keeps this a drop-in replacement for the plain borrow it replaced.
    borrow_kwargs = {} if timeout is None else {"timeout": timeout}
    if purpose == RESERVED:
        with pool.connection(**borrow_kwargs) as conn:
            yield conn
        return

    depths = getattr(_HELD, "depths", None)
    if depths is None:
        depths = _HELD.depths = {}
    if depths.get(dsn, 0) > 0:
        # Already inside a general borrow on this thread. Reuse the slot: a
        # source adapter borrowing inside its own source run must not wait
        # for a slot its own run is holding.
        depths[dsn] += 1
        try:
            with pool.connection(**borrow_kwargs) as conn:
                yield conn
        finally:
            depths[dsn] -= 1
        return

    slot_wait = GENERAL_SLOT_WAIT_SECONDS if timeout is None else timeout
    sem = _general_admission(pool, dsn)
    if not sem.acquire(timeout=slot_wait):
        raise PoolReservationExhausted(
            f"no general connection slot on {pool.name} within "
            f"{slot_wait:g}s: the pool is {pool.max_size} wide, "
            f"{reserved_slots('db.shared_pool')} connections are held back "
            f"for the publish cycle, and at most "
            f"{general_admission_width('db.shared_pool')} general borrows "
            f"run at once. Something is holding connections for a long time "
            f"— check for overlapping scheduled workflows and long source "
            f"runs — or widen the db.shared_pool term."
        )
    depths[dsn] = 1
    try:
        with pool.connection(**borrow_kwargs) as conn:
            yield conn
    finally:
        depths[dsn] -= 1
        sem.release()


def get_pool(
    dsn: str,
    *,
    max_size: int | None = None,
    autocommit: bool = True,
    application_name: Optional[str] = None,
    connect_timeout: int | None = DEFAULT_CONNECT_TIMEOUT_SECONDS,
) -> ConnectionPool:
    """Return a process-shared ConnectionPool for the given DSN.

    Sizing comes from the `db.shared_pool` concurrency term, not from
    whichever caller happened to construct the pool first. `max_size`
    remains accepted for callers that genuinely need a different size,
    but a request that CANNOT be honored now REFUSES rather than
    silently returning a differently-sized pool.

    Previously this returned the existing pool regardless of a later,
    different `max_size` — so a caller asking for 16 silently got 8, a
    divergence between what was asked for and what was in use with no
    signal at either end. Resizing a live pool is unsafe, so the
    honest option of the two is to refuse.

    `application_name` is set on every new connection the pool
    opens, so `pg_stat_activity` shows which substrate component
    owns each connection. Defaults to "okg-pool".
    """
    effective_max = width(_POOL_TERM) if max_size is None else max_size
    with _LOCK:
        pool = _POOLS.get(dsn)
        if pool is not None:
            if not pool.closed:
                existing = pool.max_size
                if max_size is not None and max_size != existing:
                    raise BudgetError(
                        f"pool for this DSN is already open with "
                        f"max_size={existing}, but max_size={max_size} was "
                        f"requested. A live pool cannot be resized safely, "
                        f"and silently handing back the existing size is the "
                        f"divergence this refusal exists to prevent. Declare "
                        f"the width on the `db.shared_pool` term instead."
                    )
                return pool
            _POOLS.pop(dsn, None)
        kwargs: dict = {
            # `dsn` is load-bearing, not decorative: an `options` keyword
            # REPLACES an `options` carried in the connection string, so the
            # factory must read the DSN's own options before adding its own
            # (issue #1310). The idle-in-transaction bound resolves from
            # OKG_DEPLOYMENT here -- a pool knows a DSN, not a deployment name.
            **connection_kwargs(connect_timeout=connect_timeout, dsn=dsn),
            "autocommit": autocommit,
        }
        if application_name:
            kwargs["application_name"] = application_name
        # min_size=2 (was 1): pre-open two connections at pool
        # construction so the first ingest doesn't pay a synchronous
        # ~20 ms `Condition.wait` while psycopg_pool's background
        # thread opens the second one. Measured on the
        # palimpzest-tier1-100 ingest flamegraph: `_acquire_conn`
        # spent 21 ms in `WaitingClient.wait` on every first request,
        # because two callers (the SourceRunner's `_load_prior_cursor`
        # and the `_dedupe_against_persisted` SQL execute) come in
        # within ms of each other.
        pool = ConnectionPool(
            conninfo=dsn,
            min_size=2,
            max_size=effective_max,
            kwargs=kwargs,
            # Validate idle connections at checkout so a database restart
            # cannot hand a severed backend to runtime or ingest callers.
            # The probe is one round-trip per checkout; issue #960 measured
            # its local Postgres cost before enabling it on this shared pool.
            check=ConnectionPool.check_connection,
            # Release any session advisory lock a borrower left behind and
            # clear any commit-witness stamp it left, or close the connection
            # if either fails (see the callback).
            reset=reset_returned_connection,
            open=True,
            name=f"okg-pool-{len(_POOLS)}",
        )
        # `open=True` returns immediately; psycopg_pool opens min_size
        # connections in a background thread. Synchronously block here
        # until they're ready — without this, the FIRST acquire pays
        # ~25-30 ms in `Condition.wait` (observed on pyinstrument
        # flamegraph). 1-second timeout matches the substrate's
        # general connection-open budget.
        try:
            pool.wait(timeout=5.0)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "pool.wait() did not reach min_size in 5s "
                "(continuing; first acquire may block): %s",
                exc,
            )
        _POOLS[dsn] = pool
        log.info(
            "opened ConnectionPool (dsn-redacted, max_size=%d, "
            "autocommit=%s)", effective_max, autocommit,
        )
        return pool


def configure_source_process_pool() -> None:
    """Size every pool in this process from `ingest.source_process`.

    Called once, first thing in a source child process, before any pool
    exists. Refuses when a pool is already open: resizing a live pool is the
    divergence `get_pool` refuses too.
    """
    global _POOL_TERM
    with _LOCK:
        if any(not pool.closed for pool in _POOLS.values()):
            raise BudgetError(
                "configure_source_process_pool() must run before any pool "
                "opens; a live pool cannot be resized"
            )
        _POOL_TERM = "ingest.source_process"
        _ADMISSION.clear()


def close_all() -> None:
    """Close every pool. Idempotent. Call from worker shutdown +
    bench teardown to release backend slots cleanly."""
    with _LOCK:
        for dsn, pool in list(_POOLS.items()):
            try:
                pool.close()
            except Exception:  # noqa: BLE001 — shutdown cleanup must continue across other pools even if one fails; log full traceback.
                # The _POOLS key is the full DSN incl. password; redact
                # it (the substrate never logs the password — see
                # connection.py). Per `harden-okg-network-and-secrets`.
                log.exception("pool.close() failed for %s", _redact(dsn))
        _POOLS.clear()
        # Admission slots belong to the pools that were just closed. Leaving
        # them would hand the next pool a semaphore sized for the old width.
        _ADMISSION.clear()
