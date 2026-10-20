"""The one place a parallel lane's width is declared.

Before this module, every parallel lane picked its own width in its own file —
ingest sources capped at 6 in `ingest/service.py`, the shared pool at 8 here in
`db/pool.py`, DBOS at 6 in `runtime/config.py`, MCP at 16 in `mcp/server.py`,
chronos executors from bare environment variables — and nothing summed them.
Exactly one of those sites reasoned about total connection demand, and only for
its own lane. Over-subscription therefore surfaced as a `PoolTimeout` in the
middle of a publish, attributed to whichever lane happened to ask last rather
than to the over-declaration that caused it.

A **term** is one lane's declared width. Every parallel lane draws its width
from a term here; none may define a bare module-level constant. Each term also
declares `connections_per_worker`, so the process's total connection demand is
computable by construction rather than by audit.

Two properties this module exists to keep decidable:

  1. every declared term has a consumer (an unread term is a dead knob, the
     defect that survived the chronos-native cutover as `n_phase_a_workers`);
  2. every pool/executor construction site takes its width from a term.

Both are asserted by `tests/substrate/test_concurrency_budget_guard.py` over
`src/` with the AST, never over `tests/` — a test asserting a knob's value
would otherwise make that knob look "read".

**No silent clamping.** A width that is unparseable, out of range, or
unsatisfiable raises. It is never quietly coerced to a default or to 1: a
default-value-as-fallback is a P0 in this project, and clamping is the shape it
takes for concurrency knobs (the deleted `partition_by_source` did exactly this
with `max(1, n_batches)`).

Scope note: this covers the *runtime* process — publisher, ingest, enrichers,
gates, MCP, chronos. Offline experiment tooling under `okg/bench/` opens its
own connections and is deliberately outside the budget; it does not run
alongside a publish cycle.
"""
from __future__ import annotations

import os
import threading
from dataclasses import dataclass, field

__all__ = [
    "BudgetError",
    "ConcurrencyTerm",
    "check_feasibility",
    "connection_demand",
    "declared_terms",
    "general_admission_width",
    "reserved_slots",
    "term",
    "width",
]

# A width above this is refused outright. It is not a tuning ceiling — it is a
# typo guard, so `workers: 800` fails loudly at declaration instead of at
# `PoolTimeout` under load.
_MAX_DECLARABLE_WIDTH = 64


class BudgetError(RuntimeError):
    """A width is missing, malformed, out of range, or unsatisfiable.

    Always raised, never downgraded to a warning-and-default: the whole point
    of the registry is that an unsatisfiable declaration stops the process at
    the point of declaration rather than becoming a mid-publish failure
    attributed to an innocent lane.
    """


@dataclass(frozen=True)
class ConcurrencyTerm:
    """One lane's declared width and what it costs in connections."""

    name: str
    default_width: int
    connections_per_worker: int
    env_var: str | None = None
    description: str = ""
    #: The pool this lane BORROWS from, if any. A borrower adds no
    #: connections of its own — its workers occupy connections that the pool
    #: already accounts for — so it contributes a FEASIBILITY constraint
    #: (its width must fit inside the pool) rather than a term in the
    #: connection sum. Conflating the two double-counts: summing an ingest
    #: layer of 6 on top of a shared pool of 8 reports 14 where the real
    #: demand is 8, which would make a fail-closed boot check refuse
    #: deployments that fit.
    borrows_from: str | None = None
    #: Terms whose workers do NOT hold a connection for their whole run (a
    #: provider round-trip, say) declare 0 and are excluded from the
    #: connection sum while still bounding occupancy.
    counts_against_connections: bool = field(default=True)
    #: Terms whose connections are opened by OTHER processes this one starts
    #: (a source child). They are admitted at each start against the live
    #: server count (`ingest/source_jobs.py`), never summed into this
    #: process's own demand.
    child_process_only: bool = field(default=False)

    def __post_init__(self) -> None:
        _validate_width(self.default_width, f"{self.name}.default_width")
        if self.connections_per_worker < 0:
            raise BudgetError(
                f"{self.name}: connections_per_worker must be >= 0, "
                f"got {self.connections_per_worker}"
            )


def _validate_width(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise BudgetError(
            f"{label}: width must be an int, got {value!r} "
            f"({type(value).__name__}). Not coerced — declare it correctly."
        )
    if value < 1:
        raise BudgetError(
            f"{label}: width must be >= 1, got {value}. A width of 0 or "
            f"negative is not clamped to 1; it is a declaration error."
        )
    if value > _MAX_DECLARABLE_WIDTH:
        raise BudgetError(
            f"{label}: width {value} exceeds the maximum declarable width "
            f"{_MAX_DECLARABLE_WIDTH}. Raise the ceiling deliberately if this "
            f"is intended; it exists to catch typos before they become "
            f"PoolTimeouts."
        )
    return value


#: The registry. Adding a parallel lane means adding a term here — which is
#: what keeps the total computable rather than discoverable by audit.
_TERMS: dict[str, ConcurrencyTerm] = {
    t.name: t
    for t in (
        ConcurrencyTerm(
            name="db.shared_pool",
            default_width=12,
            connections_per_worker=1,
            description=(
                "psycopg ConnectionPool shared by publisher, gates, ingest "
                "and projection. Its max_size IS this width. Raised TWICE, each "
                "time because the feasibility check refused the shipped "
                "defaults rather than letting them surface as a mid-publish "
                "PoolTimeout: 8 -> 10 when telemetry became a borrower (8 left "
                "only 6 usable after the publish reservation, which ingest "
                "alone consumed), and 10 -> 12 when gate.invariants became one "
                "(ingest 6 + telemetry 2 + gate 2 = 10, which needs 12 with the "
                "reservation held back). Every borrower declared is a real claim "
                "on this number — which is exactly why the relation is declared "
                "instead of assumed."
            ),
        ),
        ConcurrencyTerm(
            name="ingest.source_layer",
            default_width=6,
            connections_per_worker=0,
            counts_against_connections=False,
            description=(
                "Source child processes one source-sync step runs at once "
                "(each source runs in its own process and "
                "starts when its predecessors finish). A child opens its own "
                "connections -- `ingest.source_process` plus interval GC plus "
                "compensation -- which the coordinator admits against the "
                "live server count before each start, so this term bounds "
                "processes, not this process's connections."
            ),
        ),
        ConcurrencyTerm(
            name="ingest.source_process",
            default_width=4,
            connections_per_worker=1,
            child_process_only=True,
            description=(
                "The one pool inside a source child process: the run "
                "connection, one nested adapter or graph-store borrow, one "
                "spare, and the connection that holds the per-source run "
                "lock (counted here, never opened beside the pool). Every "
                "borrow waits at most 10 s, then fails by name. A child "
                "builds no shared pool, reserves nothing for publishing and "
                "starts no DBOS. A source that needs more gets a wider term, "
                "named here."
            ),
        ),
        ConcurrencyTerm(
            name="runtime.dbos_pool",
            default_width=6,
            connections_per_worker=1,
            description="DBOS's own pool; separate from db.shared_pool.",
        ),
        ConcurrencyTerm(
            name="mcp.read_pool",
            default_width=16,
            connections_per_worker=1,
            description=(
                "One MCP server process's read pool. NOTE this is per "
                "process and agent sessions start their own, so the host-wide "
                "total is not knowable at worker boot — see the reservation "
                "tier in unify-connection-budget."
            ),
        ),
        ConcurrencyTerm(
            name="chronos.schema_index",
            default_width=2,
            connections_per_worker=1,
            env_var="CHRONOS_ASYNC_SCHEMA_INDEX_WORKERS",
            description="Async schema-index executor; connects outside the pool.",
        ),
        ConcurrencyTerm(
            name="chronos.interval_gc",
            default_width=1,
            connections_per_worker=1,
            env_var="CHRONOS_INTERVAL_GC_WORKERS",
            description="Interval GC executor; connects outside the pool.",
        ),
        ConcurrencyTerm(
            name="enrichment.model_calls",
            default_width=4,
            connections_per_worker=0,
            counts_against_connections=False,
            description=(
                "Per-item model calls inside the publish cycle (the four LLM "
                "extractors, the invariant judge, captioning, VLM tables). "
                "Workers are provider round-trips holding no connection — the "
                "phase-2 bookkeeping runs on the cycle's own thread — so this "
                "bounds provider occupancy, not connections. Spend and request "
                "rate are bounded separately: width does not cap cost."
            ),
        ),
        ConcurrencyTerm(
            name="live.adapter_calls",
            default_width=8,
            connections_per_worker=0,
            counts_against_connections=False,
            env_var="OKG_LIVE_ADAPTER_CALLS",
            description=(
                "Live-relation adapter calls in flight in one MCP process. "
                "Holds NO database connection: the graph read "
                "closes before the adapter runs and the call sample is "
                "written afterwards in its own short transaction, so a live "
                "call occupies a thread and a socket to somebody else's "
                "system, never one of ours. It is declared here anyway "
                "because the thread is the scarce resource: a hung adapter "
                "is ABANDONED at its deadline and keeps its thread until the "
                "far side answers, so without a bound one slow endpoint "
                "parks threads until the process dies. Exhaustion is refused "
                "by name rather than queued, so the pressure is visible."
            ),
        ),
        ConcurrencyTerm(
            name="views.recheck_judge",
            default_width=8,
            connections_per_worker=0,
            counts_against_connections=False,
            description=(
                "Recheck judge fan-out: the substrate's ONE pre-existing "
                "parallel model-call lane. Its workers are provider "
                "round-trips holding no connection, so this bounds provider "
                "concurrency rather than connections; the telemetry each call "
                "emits is bounded separately by llm.telemetry."
            ),
        ),
        ConcurrencyTerm(
            name="embeddings.batch",
            default_width=4,
            connections_per_worker=0,
            counts_against_connections=False,
            description=(
                "Per-node embed calls within one embed batch. Each worker is a "
                "provider round-trip plus its own short chronos session, not a "
                "borrow from db.shared_pool, so this bounds provider occupancy. "
                "The embed workflow is separate from the publish workflow and "
                "writes no store in the modeled publish commit set."
            ),
        ),
        ConcurrencyTerm(
            name="gate.invariants",
            default_width=2,
            connections_per_worker=0,
            borrows_from="db.shared_pool",
            description=(
                "Publish-gate invariants evaluated concurrently within a pass. "
                "Each holds a pooled connection for its query, BORROWED from "
                "db.shared_pool that the publish cycle itself draws from — a "
                "PoolTimeout in the gate is not caught and crashes the publish, "
                "so the default is deliberately small. This width bounds the "
                "achievable speedup: near the width, not near the invariant "
                "count."
            ),
        ),
        ConcurrencyTerm(
            name="llm.telemetry",
            default_width=2,
            connections_per_worker=0,
            borrows_from="db.shared_pool",
            counts_against_connections=False,
            description=(
                "Concurrent okg.llm_calls writes. Telemetry BORROWS from "
                "db.shared_pool rather than opening its own connection, so "
                "connections_per_worker is 0 — counting it again would "
                "double-count the same connection. The width is a CAP on "
                "telemetry's share of that pool, so a wide model-call "
                "fan-out cannot starve the publish cycle it runs inside."
            ),
        ),
    )
}

_OVERRIDES: dict[str, int] = {}
_LOCK = threading.Lock()


def term(name: str) -> ConcurrencyTerm:
    """Return a declared term, or raise if it does not exist."""
    try:
        return _TERMS[name]
    except KeyError:
        raise BudgetError(
            f"unknown concurrency term {name!r}. Declared terms: "
            f"{', '.join(sorted(_TERMS))}. Add the term rather than "
            f"introducing a bare constant."
        ) from None


def width(name: str) -> int:
    """The effective width for a term: override, else env var, else default.

    Every parallel lane calls this instead of naming a number. A malformed
    override or env value raises rather than falling back.
    """
    t = term(name)
    with _LOCK:
        if name in _OVERRIDES:
            return _OVERRIDES[name]
    if t.env_var:
        raw = os.environ.get(t.env_var)
        if raw is not None and raw.strip():
            try:
                parsed = int(raw)
            except ValueError:
                raise BudgetError(
                    f"{t.env_var}={raw!r} is not an integer; refusing to fall "
                    f"back to the default width for {name!r}."
                ) from None
            return _validate_width(parsed, f"{name} (from {t.env_var})")
    return t.default_width


def set_override(name: str, value: int) -> None:
    """Declare a width for this process (the manifest's entry point)."""
    t = term(name)
    _validate_width(value, f"{t.name} (override)")
    with _LOCK:
        _OVERRIDES[name] = value


def clear_overrides() -> None:
    """Drop all overrides. Test hook; not part of the runtime contract."""
    with _LOCK:
        _OVERRIDES.clear()


def declared_terms() -> tuple[ConcurrencyTerm, ...]:
    return tuple(_TERMS[name] for name in sorted(_TERMS))


#: Connections a pool must keep free for the work it exists to serve. The
#: publish cycle draws from db.shared_pool alongside every borrower, and a
#: PoolTimeout there costs a whole cycle — far more than a throttled lane.
_POOL_RESERVATIONS: dict[str, int] = {"db.shared_pool": 2}


#: Borrowers of a pool that do NOT pass through the shared general-admission
#: gate in `okg.substrate.db.pool.pooled_connection`, and why.
#:
#: - `gate.invariants` runs ON the publish path. It borrows with
#:   `purpose=RESERVED`, so its connections come out of the same side of the
#:   pool as the publisher cycle itself.
#: - `llm.telemetry` admits itself through its own semaphore in
#:   `okg.substrate.llm.tracker`, sized from its own width.
#:
#: Their widths must therefore be subtracted before the general gate is
#: sized, or the gate would hand out slots those lanes have already claimed.
_SELF_ADMITTED_BORROWERS: dict[str, tuple[str, ...]] = {
    "db.shared_pool": ("gate.invariants", "llm.telemetry"),
}


def general_admission_width(pool_name: str) -> int:
    """Concurrent borrowers the shared general-admission gate may admit.

    The pool width, less the connections reserved for the publish cycle, less
    the borrowers that admit themselves elsewhere (:data:`_SELF_ADMITTED_BORROWERS`).

    Deriving it rather than writing `max_size - reserved` matters: the publish
    path holds more than the reservation — the publisher's own cycle
    connection PLUS `gate.invariants` workers — so a gate sized only against
    the reservation would hand out slots that are already spoken for, and the
    pool would be oversubscribed by exactly the lanes this is meant to bound.

    Floors at 1. A budget too tight to admit anyone must still make progress;
    the misdeclaration is caught by :func:`check_feasibility` at boot, which
    refuses rather than clamps.
    """
    total = width(pool_name) - reserved_slots(pool_name)
    for name in _SELF_ADMITTED_BORROWERS.get(pool_name, ()):
        total -= width(name)
    return max(1, total)


def reserved_slots(pool_name: str) -> int:
    """Connections held back from general borrowers for the publish cycle.

    :func:`check_feasibility` subtracts this at DECLARATION time, so a width
    combination that could not fit is refused at boot. That arithmetic alone
    does not keep a slot free at runtime: an ingest lane or a long scheduled
    workflow can still hold the last connection while the publish cycle waits
    out its checkout timeout. `okg.substrate.db.pool.pooled_connection`
    enforces the same number at checkout, and reads it from here so the two
    halves cannot drift apart.

    Returns 0 for a pool with no declared reservation.
    """
    return _POOL_RESERVATIONS.get(pool_name, 0)


def check_feasibility() -> None:
    """Refuse width combinations that are guaranteed to exhaust a pool.

    Validating each width in isolation is not enough: every declared width can
    be individually legal while their SUM cannot fit. Ingest at 6 and telemetry
    at 4 against a pool of 8 is six borrowed connections too many, and nothing
    detects it until a source dies on `PoolTimeout` mid-publish and the whole
    downstream layer is skipped.

    Raises rather than clamping. Clamping is the shape default-value-as-
    fallback takes for concurrency, and it would hide the misdeclaration it is
    meant to surface.
    """
    for pool_name, reserved in _POOL_RESERVATIONS.items():
        pool_width = width(pool_name)
        borrowers = {
            t.name: width(t.name)
            for t in declared_terms()
            if t.borrows_from == pool_name
        }
        demanded = sum(borrowers.values())
        available = pool_width - reserved
        if demanded > available:
            detail = ", ".join(f"{n}={w}" for n, w in sorted(borrowers.items()))
            raise BudgetError(
                f"{pool_name} cannot satisfy its borrowers: {detail} demand "
                f"{demanded} connections, but the pool is {pool_width} with "
                f"{reserved} reserved for the publish cycle, leaving "
                f"{available}. Widen {pool_name} or narrow the lanes. This is "
                f"refused at declaration rather than clamped, because the "
                f"alternative is a PoolTimeout mid-publish attributed to "
                f"whichever lane happened to ask last."
            )


def connection_demand(*, include_mcp: bool = False) -> dict[str, int]:
    """Per-term Postgres connection demand for THIS process, plus the total.

    Only INDEPENDENT pools contribute. A term that declares `borrows_from`
    occupies connections its pool already owns, so counting it again inflates
    the total — which matters because the boot admission check is fail-closed:
    an inflated sum refuses deployments that actually fit. Borrower widths are
    checked by :func:`check_feasibility` instead.

    `include_mcp` is off by default and that is the honest default: the MCP
    read pool belongs to separate, independently-started processes, so a
    worker's boot-time sum cannot know how many exist. Counting one is wrong
    under agent concurrency; counting a worst case refuses valid deployments.
    Host-wide accounting is a reservation, not a sum.
    """
    per_term: dict[str, int] = {}
    for t in declared_terms():
        if (
            not t.counts_against_connections
            or t.borrows_from is not None
            or t.child_process_only
        ):
            continue
        if t.name == "mcp.read_pool" and not include_mcp:
            continue
        per_term[t.name] = width(t.name) * t.connections_per_worker
    per_term["_total"] = sum(v for k, v in per_term.items() if k != "_total")
    return per_term
