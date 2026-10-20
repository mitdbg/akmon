"""Phase B gate evaluator (M5.2).

Per design.md §720: between Phase A (projection) and the
pointer flip, the publisher evaluates invariants as read-only
queries. Error-severity failures block the generation, whether
source-local or cross-source. Warn-severity failures surface in
`invariant_summary` but never block.

Design properties the evaluator upholds:

  1. **Read-only.** Each invariant runs under
     `SET LOCAL TRANSACTION READ ONLY` so even a SQL-write that
     slipped past the loader's regex aborts at the DB.
  2. **Per-invariant timeout.** `statement_timeout` is set per
     invariant inside its own short txn; timeout is treated
     as failure (result recorded with `timed_out=True`).
  3. **Cross-source total budget.** `total_timeout_ms` caps
     the whole cross-source gate phase; exceeding it blocks
     the generation with `blocked_reason=gate_timeout`.
  4. **Outside the publish txn.** The evaluator opens its own
     connection; `publisher.run_gates` is called at a point
     where no publish txn is in flight (the advisory-lock conn
     is idle between Phase A's pool-join and Phase C's pointer
     flip).
  5. **Violator sample = invariant body's result.** The SQL
     body is run AS-IS, so the invariant author controls the
     sample shape. A default LIMIT 100 is appended iff the body
     doesn't already contain a LIMIT token.

Source-scoped evaluation (issue #1427):
  - A failing source-scoped ERROR invariant adds its source to
    `phase_b.excluded_sources` AND blocks the generation with
    `blocked_reason=source_local_error_invariant_failed`. The
    exclusion used to be carried out by Phase C, which discarded
    the source's `:g_next` projection rows before the pointer
    flip; Phase C was retired in the chronos cutover and the
    Chronos-native publisher checkpoints the whole branch head,
    so there is no per-source discard for the exclusion to mean.
    An exclusion nobody can carry out has to block, or the
    failing source's rows publish behind a gate that fired.
  - The same holds for a source excluded by Phase A: any
    non-empty `excluded_sources` blocks.

Returned PhaseBResult shape is unchanged from M1 (already
defined in types.py). M5.2 populates `source_local`,
`cross_source`, `excluded_sources`, `blocked`, `blocked_reason`,
`total_duration_ms`, `total_budget_exceeded`.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable, Mapping, Optional

import psycopg

from okg.substrate.catalog.invariant_graph_read import (
    graph_branch_checkout,
    prepare_invariant_sql,
    to_checkout_sql,
)
from okg.substrate.catalog.invariants import DEFAULT_TIMEOUT_MS
from okg.substrate.db.pool import RESERVED, pooled_connection
from okg.substrate.errors import SubstrateError
from okg.substrate.types import (
    GateInvariantResult,
    PhaseAResult,
    PhaseBResult,
    ReservedGeneration,
    SourceProjectionStatus,
)


log = logging.getLogger(__name__)


# Default caps (override via Publisher kwargs or per-invariant).
DEFAULT_CROSS_BUDGET_MS = 30_000
DEFAULT_SAMPLE_LIMIT = 100
# Planning is cheap but not free (GEQO on a wide join), and the whole point of
# the preflight is that it costs milliseconds rather than the count it precedes.
PREFLIGHT_TIMEOUT_MS = 5_000
# Failures that are the environment's, not the invariant's. Reporting one of
# these as `invariant_preflight_failed` would name an innocent invariant and
# tell the operator to rewrite correct SQL.
_UNATTRIBUTABLE_PREFLIGHT_ERRORS = (
    psycopg.OperationalError,
    psycopg.errors.AdminShutdown,
    psycopg.errors.InsufficientResources,
)


# `_ensure_limit` removed at M5.5 (von Neumann review §1).
# The LIMIT is now applied in the wrapped form of the invariant
# body — see `_run_one_invariant`. This ensures the reported
# `violator_count` is the TRUE total, not the truncated sample.


def _load_invariants_for_cv(
    conn: psycopg.Connection, catalog_version_id: int,
    *,
    kinds: tuple[str, ...] = ("sql",),
) -> list[dict[str, Any]]:
    """Read the invariant rows installed at this CV.

    Per `add-semantic-llm-judge-invariants` (2026-04-29), the
    table also stores `kind in ('sql', 'llm_judge')`. The Phase B
    gate consumes only `kind='sql'` rows (default) — LLM-judge
    invariants run outside the gate via
    `okg run --include-llm-invariants`. Pass an explicit `kinds`
    to read other kinds (e.g. `('llm_judge',)` for the judge
    runner).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT name, severity, scope, source, sql_body, timeout_ms,
                   kind, candidates_sql, judge_prompt, judge_model,
                   sample_cap, fire_on,
                   judge_spend_cap_usd, judge_spend_estimate_usd,
                   concept_scope, requires_sources
              FROM okg.invariants
             WHERE catalog_version_id = %s
               AND kind = ANY(%s)
             ORDER BY scope, name
            """,
            (catalog_version_id, list(kinds)),
        )
        return [
            {
                "name": r[0],
                "severity": r[1],
                "scope": r[2],
                "source": r[3],
                "sql_body": r[4],
                "timeout_ms": r[5],
                "kind": r[6],
                "candidates_sql": r[7],
                "judge_prompt": r[8],
                "judge_model": r[9],
                "sample_cap": r[10],
                "fire_on": list(r[11]) if r[11] else None,
                # Judge-lane spend declaration; NULL = spend-unbounded
                # (reported by the runner, never silently defaulted).
                # NUMERIC arrives as Decimal; the spend seam does float
                # arithmetic against float estimates.
                "judge_spend_cap_usd": (
                    float(r[12]) if r[12] is not None else None
                ),
                "judge_spend_estimate_usd": (
                    float(r[13]) if r[13] is not None else None
                ),
                # `add-concept-scoped-invariants`: NULL → unscoped.
                "concept_scope": r[14],
                # Issue #3624: NULL → always evaluated.
                "requires_sources": list(r[15]) if r[15] else None,
            }
            for r in cur.fetchall()
        ]


def _run_one_invariant(
    dsn: str,
    inv: dict[str, Any],
    *,
    generation_id: int,
    excluded_sources: Optional[list[str]] = None,
    branch_name: str = "default",
) -> GateInvariantResult:
    """Execute one invariant in its own short txn; return the
    result as a GateInvariantResult.

    M5.5 P0.3 fix (von Neumann review §1): violator_count is
    the TRUE count of matching rows, not the truncated sample
    count.

    Count and sample are deliberately separate statements in the
    same read-only transaction. The prior single-statement wrapper
    materialized every projected result column for every violator
    before counting. That is expensive for JSONB-heavy invariants
    whose sample only needs the first N rows. Separate statements
    let PostgreSQL optimize `count(*)` against the predicate, then
    evaluate the invariant's select list only for the capped sample.
    """
    timeout_ms = inv["timeout_ms"] or DEFAULT_TIMEOUT_MS
    count_sql, sample_sql = _invariant_statements(inv)

    violator_count = 0
    violators: list[dict[str, Any]] = []
    timed_out = False
    t0 = time.monotonic()
    try:
        # Use the process-shared pool; saves the ~10-15 ms TCP+TLS
        # handshake per gate evaluation. The pool is autocommit=True
        # by default, but the gate needs a transaction for SET LOCAL +
        # READ ONLY semantics. `conn.transaction()` flips autocommit
        # off for the with-block and commits/rolls back at exit —
        # `SET LOCAL` then properly scopes to that transaction and is
        # discarded when the conn returns to the pool.
        with pooled_connection(dsn, purpose=RESERVED) as conn, \
                conn.transaction(), conn.cursor() as cur:
            cur.execute(
                f"SET LOCAL statement_timeout = {int(timeout_ms)}"
            )
            cur.execute("SET LOCAL TRANSACTION READ ONLY")
            params = {
                "pin": generation_id,
                "excluded_sources": excluded_sources or [],
            }
            # A body that reads the Chronos graph cannot run on this bare
            # connection: okg.v_nodes/okg.v_edges raise the honest-read guard
            # and okg.graph_nodes/okg.graph_edges report a confident zero
            # (issue #1456). `prepare_invariant_sql` rewrites those statements
            # onto the physical graph tables so the checkout below can scope
            # them to the branch head; a non-graph body comes back unchanged
            # and takes the same plain-cursor path it always did. Both SET
            # LOCALs above bind the connection, so the checkout inherits this
            # invariant's timeout and the read-only transaction.
            count_ready, count_pinned = prepare_invariant_sql(count_sql, conn)
            session = (
                graph_branch_checkout(conn, branch_name=branch_name)
                if count_pinned else None
            )

            if session is not None:
                rows = session.query(to_checkout_sql(count_ready), params)
                violator_count = int(
                    (rows[0]["violator_count"] if rows else 0) or 0
                )
            else:
                cur.execute(count_ready, params)
                row = cur.fetchone()
                if row is not None:
                    violator_count = int(row[0] or 0)
            if violator_count:
                elapsed_ms = int((time.monotonic() - t0) * 1000)
                remaining_ms = max(1, int(timeout_ms) - elapsed_ms)
                cur.execute(
                    f"SET LOCAL statement_timeout = {remaining_ms}"
                )
                sample_ready, sample_pinned = prepare_invariant_sql(
                    sample_sql, conn,
                )
                if sample_pinned:
                    if session is None:
                        session = graph_branch_checkout(
                            conn, branch_name=branch_name,
                        )
                    rows = session.query(
                        to_checkout_sql(sample_ready), params,
                    )
                    sample = (rows[0]["sample"] if rows else None) or []
                else:
                    cur.execute(sample_ready, params)
                    row = cur.fetchone()
                    sample = (row[0] if row is not None else None) or []
                violators = [
                    dict(item) if isinstance(item, dict) else item
                    for item in sample
                ]
    except psycopg.errors.QueryCanceled:
        timed_out = True
        log.warning(
            "invariant %s timed out after %d ms",
            inv["name"], timeout_ms,
        )
    duration_ms = int((time.monotonic() - t0) * 1000)

    return GateInvariantResult(
        name=inv["name"],
        scope=inv["scope"],
        source=inv["source"],
        severity=inv["severity"],
        passed=(not timed_out) and (violator_count == 0),
        violators=violators,
        violator_count=violator_count,
        duration_ms=duration_ms,
        timed_out=timed_out,
    )


def _invariant_statements(inv: dict[str, Any]) -> tuple[str, str]:
    """Return the (count, sample) statements the gate executes for one invariant.

    Shared with `preflight_invariants` so the planner sees exactly the SQL the
    gate will run. The sample statement is executed only when the count finds
    violators, so a body that is valid under the count wrapper but invalid under
    the sample wrapper would otherwise stay latent until the day the invariant
    first fires -- the preflight plans both.
    """
    body = inv["sql_body"].rstrip().rstrip(";")
    count_sql = f"SELECT count(*) AS violator_count FROM ({body}) _inv"
    sample_sql = f"""
        SELECT COALESCE(array_agg(row_json), ARRAY[]::jsonb[]) AS sample
          FROM (
            SELECT to_jsonb(_s) AS row_json
              FROM ({body}) _s
             LIMIT {int(DEFAULT_SAMPLE_LIMIT)}
          ) _sample
    """
    return count_sql, sample_sql


def preflight_invariants(
    dsn: str,
    *,
    catalog_version_id: int,
    generation_id: int,
) -> Optional[dict[str, Any]]:
    """Plan every invariant the publish gate will run. Return a block payload.

    Issue #1456: the cycle used to discover an unrunnable invariant only at
    `evaluate_gates`, which on a first publish is after the full graph counts,
    the extractor, the enrichers and the derived-fact stages have all been paid
    for -- 3,437 seconds of publisher lock in the reported incident, with no
    checkpoint written. Planning the same statements costs milliseconds, so it
    runs first.

    Each statement is put through `prepare_invariant_sql` (which maps the graph
    views onto `okg.graph_nodes` / `okg.graph_edges`) and then EXPLAINed. EXPLAIN
    resolves relations, columns and syntax without executing: `okg.require_graph_checkout`
    is VOLATILE so the planner never folds it, and after the rewrite the guard arm
    is not in the statement at all.

    Scope is `kind='sql'` -- exactly what `evaluate_gates` runs. `llm_judge` row
    sources run outside the publish cycle via `okg run --include-llm-invariants`,
    where a failure is advisory; they must not gain the power to block a publish.

    Returns None when every invariant plans, or a payload naming the first
    invariant that did not; the caller decides what to do with it. Only errors
    that are genuinely attributable to the invariant are reported that way --
    a lost connection or an administrative shutdown is re-raised untouched,
    because blaming a named invariant for those would send the operator to
    correct SQL that is fine.
    """
    # The preflight takes its OWN pooled connection rather than borrowing the
    # publisher's. `Publisher.cycle(conn=...)` is called with a non-autocommit
    # connection by merge-back and by ingest, and on such a connection
    # `conn.transaction()` opens a SAVEPOINT -- whose RELEASE keeps `SET LOCAL`
    # GUCs, because they revert only on rollback. Borrowing would therefore leak
    # READ ONLY and the 5s planner timeout into the rest of the caller's
    # transaction, and the next thing that transaction does on a first publish
    # is the full graph count: it would abort at 5 seconds, breaking exactly the
    # phase this preflight exists to protect.
    with pooled_connection(dsn, purpose=RESERVED) as conn:
        invs = _load_invariants_for_cv(conn, catalog_version_id)
        if not invs:
            return None
        params = {"pin": generation_id, "excluded_sources": []}
        for inv in invs:
            for wrapper, statement in zip(
                ("count", "sample"), _invariant_statements(inv),
            ):
                try:
                    with conn.transaction():
                        with conn.cursor() as cur:
                            cur.execute("SET LOCAL TRANSACTION READ ONLY")
                            cur.execute(
                                f"SET LOCAL statement_timeout = "
                                f"{int(PREFLIGHT_TIMEOUT_MS)}"
                            )
                            # Inside the timeout: the rewrite reads
                            # pg_get_viewdef for composite deployment views, so
                            # it is database work too and must be bounded by the
                            # same budget.
                            prepared, _pinned = prepare_invariant_sql(
                                statement, conn,
                            )
                            cur.execute(f"EXPLAIN {prepared}", params)
                            cur.fetchall()
                except _UNATTRIBUTABLE_PREFLIGHT_ERRORS:
                    # Not this invariant's fault. Let it propagate as itself
                    # rather than mislabel infrastructure failure as an
                    # authoring defect.
                    raise
                except (psycopg.Error, SubstrateError) as exc:
                    log.warning(
                        "invariant %s: %s statement failed preflight (%s: %s)",
                        inv["name"], wrapper, type(exc).__name__, exc,
                    )
                    timed_out = isinstance(exc, psycopg.errors.QueryCanceled)
                    return {
                        "reason": (
                            "invariant_preflight_timeout" if timed_out
                            else "invariant_preflight_failed"
                        ),
                        "invariant": inv["name"],
                        "statement": wrapper,
                        "error_class": type(exc).__name__,
                        "error": str(exc).strip(),
                    }
    return None


def _apply_source_requirements(
    invs: list[dict[str, Any]],
    source_states: Optional[Mapping[str, bool]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """Split off invariants whose required sources are off (issue #3624).

    Returns (invariants to evaluate, skipped entries, names evaluated
    without a registry to check against).
    """
    evaluate: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    unchecked: list[str] = []
    for inv in invs:
        required = inv.get("requires_sources") or []
        if not required:
            evaluate.append(inv)
            continue
        if source_states is None:
            evaluate.append(inv)
            unchecked.append(inv["name"])
            continue
        off = [s for s in required if not source_states.get(s, False)]
        if not off:
            evaluate.append(inv)
            continue
        not_registered = [s for s in off if s not in source_states]
        skipped.append({
            "name": inv["name"],
            "severity": inv["severity"],
            "scope": inv["scope"],
            "requires_sources": list(required),
            "off_sources": off,
            "reason": (
                "required source off in this deployment: "
                + ", ".join(
                    f"{s} (not registered)" if s in not_registered
                    else f"{s} (enabled_by_default: false)"
                    for s in off
                )
            ),
        })
    return evaluate, skipped, sorted(unchecked)


def evaluate_gates(
    dsn: str,
    g: ReservedGeneration,
    phase_a: PhaseAResult,
    *,
    total_timeout_ms: int = DEFAULT_CROSS_BUDGET_MS,
    branch_name: str = "default",
    source_states: Optional[Callable[[], Optional[Mapping[str, bool]]]] = None,
) -> PhaseBResult:
    """Run every invariant installed at `g.catalog_version_id`.

    `source_states` is called, only when an installed invariant declares
    `requires_sources`, to map each source in the deployment's registry to
    whether it is on; a deployment that uses no such invariant never reads
    its registry here. An invariant with `requires_sources` naming a source
    that is off (or not registered) is skipped and listed in
    `skipped_invariants` (issue #3624). `None` means the registry could
    not be read: such invariants are evaluated as usual and listed in
    `unchecked_source_requirements`, so a missing registry never turns a
    check into a silent pass.

    Two phases (M5.5 P0.4 fix, Dijkstra review §3):
      1. **Source-local pass.** Run all source-scoped
         invariants. Record which sources fail so their pending
         rows are excluded.
      2. **Cross-source pass.** Run all cross-source invariants
         AGAINST THE POST-EXCLUSION SNAPSHOT. This means the
         cross-source invariant bodies receive an additional
         `:excluded_sources` parameter (a text[]) so they can
         filter out rows that won't exist post-publish. A
         body that doesn't reference `:excluded_sources` runs
         against pre-trim data and is the author's
         responsibility.

    Prior behavior ran invariants in declaration order, so a
    cross-source invariant could see rows from a
    Phase-A-excluded source that would be discarded post-publish
    — false-pass or false-fail.

    The source-local pass does NOT consult total_timeout_ms —
    source-local gates are per-source bounded by their
    per-invariant statement_timeout. The total budget caps the
    cross-source pass only.

    Warn-severity results are recorded but never block or
    exclude.
    """
    # Phase A may have already excluded sources (failed batches).
    excluded: dict[str, str] = {}
    for src, status in phase_a.per_source_status.items():
        if status == SourceProjectionStatus.FAILED:
            excluded[src] = "phase_a_projection_failed"

    # Read the invariants table via the process-shared pool — saves
    # the ~10-15 ms TCP+TLS handshake per cycle. The pool defaults to
    # autocommit=True, which matches the prior behavior for this
    # single-statement read.
    with pooled_connection(dsn, purpose=RESERVED) as meta_conn:
        invs = _load_invariants_for_cv(
            meta_conn, g.catalog_version_id,
        )
    needs_states = any(inv.get("requires_sources") for inv in invs)
    invs, skipped_invariants, unchecked = _apply_source_requirements(
        invs,
        source_states() if (needs_states and source_states is not None)
        else None,
    )

    def _evaluate_invariants(
        dsn_: str,
        invariants: list,
        *,
        generation_id: int,
        excluded_sources: list[str] | None = None,
    ) -> list[GateInvariantResult]:
        """Evaluate a pass's invariants, concurrently, IN INPUT ORDER.

        Order is a correctness property, not a nicety: callers zip results back
        against their invariant list to attribute failures and exclusions, so a
        reordered result list would blame the wrong invariant and exclude the
        wrong source.
        """
        from concurrent.futures import ThreadPoolExecutor

        from okg.substrate.db.concurrency_budget import width as _width

        if not invariants:
            return []
        gate_width = min(_width("gate.invariants"), len(invariants))

        def _one(inv):
            kwargs = {
                "generation_id": generation_id,
                "branch_name": branch_name,
            }
            if excluded_sources is not None:
                kwargs["excluded_sources"] = excluded_sources
            return _run_one_invariant(dsn_, inv, **kwargs)

        if gate_width <= 1 or len(invariants) == 1:
            return [_one(inv) for inv in invariants]
        with ThreadPoolExecutor(
            max_workers=gate_width, thread_name_prefix="okg-gate-inv",
        ) as pool:
            # `map` preserves input order.
            return list(pool.map(_one, invariants))

    source_local: dict[str, list[GateInvariantResult]] = {}
    cross_source: list[GateInvariantResult] = []
    any_cross_error_failed = False
    total_budget_exceeded = False
    t_phase_b_start = time.monotonic()

    # ---- Phase 1: source-local pass ----
    # Invariants WITHIN a pass are independent read-only queries over the same
    # pinned candidate generation, so they evaluate concurrently. The two-phase
    # ORDERING is preserved: this pass completes before the cross-source pass,
    # because the cross pass reads the exclusions this one produces.
    #
    # Width comes from `gate.invariants`, which borrows from the same pool the
    # publish cycle uses. A PoolTimeout here is NOT caught below (only
    # QueryCanceled is), so an over-wide fan-out would crash the publish with an
    # unattributed pool error — which is why the default is small and why the
    # achievable speedup is bounded near the width, not near the invariant count.
    source_invs = [
        inv for inv in invs
        if inv["scope"] == "source" and inv["source"] not in excluded
    ]
    for inv, result in zip(
        source_invs,
        _evaluate_invariants(dsn, source_invs, generation_id=g.generation_id),
    ):
        source_local.setdefault(inv["source"], []).append(result)
        if inv["severity"] == "error" and not result.passed:
            excluded.setdefault(
                inv["source"],
                "source_local_gate_failed:" + inv["name"],
            )

    # ---- Phase 2: deployment cross-source pass ----
    # The legacy substrate-integrity audit (no_dangling / narrowing over
    # the legacy live node/edge tables) was retired in the chronos cutover
    # (cascade step 5): the live tables are empty under chronos-native
    # publishing, so the audit read nothing and was vacuous. The same
    # invariants — no_dangling + narrowing — are now enforced over the
    # Chronos graph (okg.graph_nodes / okg.graph_edges) by the always-on
    # TransformConsistencyGate evaluated in `_cycle_chronos_native`. The Phase
    # B gate carries no built-in integrity results.
    integrity_results: list[GateInvariantResult] = []

    # The total budget timer covers deployment-authored cross-source
    # invariants only.
    # (No cross-pass start timer: the budget is a CPU-sum over per-invariant
    # durations now, not wall-clock from a pass-start marker.)

    # BUDGET SEMANTICS: CPU-SUM, chosen deliberately.
    #
    # The serial loop consumed the total budget as a PREFIX — it checked elapsed
    # wall-clock between invariants and `break`, so `cross_source` held a prefix
    # of the list and an over-budget generation BLOCKED with `gate_timeout`.
    # Under concurrent dispatch that checkpoint cannot exist: everything is
    # already in flight, and wall-clock collapses toward the slowest invariant.
    #
    # Interpreting the budget as WALL-CLOCK would therefore silently retire a
    # blocking condition — generations that used to block would publish, purely
    # because dispatch changed. That is a real behavior change and the PACT
    # forbids losing it by accident, so the budget now bounds the SUM of
    # per-invariant durations. The same total work trips the same budget it
    # always did; only the latency to discover it improves. `blocked_reason`
    # names the basis so the runbook and the operator agree on what was
    # measured.
    cross_invs = [inv for inv in invs if inv["scope"] == "cross"]
    cross_results = _evaluate_invariants(
        dsn, cross_invs,
        generation_id=g.generation_id,
        excluded_sources=sorted(excluded.keys()),
    )
    cpu_sum_ms = 0.0
    for inv, result in zip(cross_invs, cross_results):
        cross_source.append(result)
        cpu_sum_ms += float(getattr(result, "duration_ms", 0) or 0)
        if inv["severity"] == "error" and not result.passed:
            any_cross_error_failed = True
    if cpu_sum_ms > total_timeout_ms:
        total_budget_exceeded = True

    total_duration_ms = int(
        (time.monotonic() - t_phase_b_start) * 1000
    )

    # ---- Exclusion is not enforceable, so an exclusion must block ----
    #
    # Issue #1427. `excluded` used to be a promise that Phase C would discard
    # the excluded source's `:g_next` projection rows before the pointer flip.
    # Phase C was retired in the chronos cutover: the Chronos-native publisher
    # checkpoints the whole branch head and has no per-source row discard, so
    # nothing acts on this dict. A source-local ERROR invariant therefore used
    # to fire, name its source, and then watch every one of that source's rows
    # get published anyway -- with a receipt that reported no exclusions at all.
    #
    # Blocking is the sound reading of an exclusion nobody can carry out: it is
    # exactly what a cross-source error already does, it needs no new publish
    # machinery, and it never lets unvalidated rows reach a reader. Atomic
    # per-source exclusion would mean retracting a source's facts from the
    # branch inside the publish transaction -- a large change to the write path
    # for a case the operator has to look at either way.
    #
    # WARN severity is untouched: it never enters `excluded`, so it never
    # blocks. The two entry kinds are reported separately because they have
    # different causes and different fixes.
    gate_excluded = {
        src: reason for src, reason in excluded.items()
        if reason.startswith("source_local_gate_failed:")
    }
    other_excluded = {
        src: reason for src, reason in excluded.items()
        if src not in gate_excluded
    }

    # Compose the blocked decision + reason.
    blocked = (
        any_cross_error_failed
        or total_budget_exceeded
        or bool(excluded)
    )
    blocked_reason: Optional[dict[str, Any]] = None
    if total_budget_exceeded:
        blocked_reason = {
            "reason": "gate_timeout",
            "total_timeout_ms": total_timeout_ms,
            "elapsed_ms": total_duration_ms,
            # Which quantity was compared against the budget. Invariants now
            # evaluate concurrently, so WALL-CLOCK no longer measures the work
            # done — `cpu_sum_ms` is the sum of per-invariant durations and is
            # what the budget bounds. Named explicitly so an operator reading
            # `elapsed_ms` does not conclude the budget was wall-clock.
            "budget_basis": "cpu_sum",
            "cpu_sum_ms": int(cpu_sum_ms),
        }
    elif any_cross_error_failed:
        failed = [
            {
                "name": r.name,
                "violator_count": r.violator_count,
                "timed_out": r.timed_out,
                "sample": r.violators[:5],
            }
            for r in cross_source
            if r.severity == "error" and not r.passed
        ]
        blocked_reason = {
            "reason": "cross_source_error_invariant_failed",
            "failed_invariants": failed,
        }
    elif gate_excluded:
        def _source_entry(src: str, reason: str) -> dict[str, Any]:
            failures = [
                r for r in source_local[src]
                if r.severity == "error" and not r.passed
            ]
            return {
                "source": src,
                "exclusion_reason": reason,
                # `excluded` records the FIRST failing invariant (setdefault),
                # so name it as such; `failed_invariants` below is the whole
                # set and is the authoritative field.
                "first_invariant": reason.split(":", 1)[1],
                # A timed-out invariant reports violator_count 0. Without this
                # flag the entry would read as "an error invariant failed with
                # nothing to show", when the truth is that it never finished.
                # Blocking on it is the same fail-closed reading a timed-out
                # CROSS-source error invariant already gets.
                "timed_out": bool(failures) and all(
                    r.timed_out for r in failures
                ),
                "failed_invariants": [
                    {
                        "name": r.name,
                        "violator_count": r.violator_count,
                        "timed_out": r.timed_out,
                        "sample": r.violators[:5],
                    }
                    for r in failures
                ],
            }

        _entries = [
            _source_entry(src, reason)
            for src, reason in sorted(gate_excluded.items())
        ]
        blocked_reason = {
            "reason": "source_local_error_invariant_failed",
            # True only when every excluded source was excluded by an invariant
            # that timed out: the generation is blocked because the gate could
            # not be evaluated, not because a violation was found.
            "all_unevaluated": bool(_entries) and all(
                e["timed_out"] for e in _entries
            ),
            # Sources excluded before the gate ran (phase A). They stay in
            # excluded_sources either way, so the reason has to name them or
            # the receipt would list a source the reason never explains.
            "also_excluded": [
                {"source": src, "exclusion_reason": reason}
                for src, reason in sorted(other_excluded.items())
            ],
            "failed_sources": _entries,
            "detail": (
                "the publisher cannot exclude one source from a Chronos "
                "checkpoint, so a failing source-local error invariant blocks "
                "the generation"
            ),
        }
    elif other_excluded:
        blocked_reason = {
            "reason": "excluded_source_not_enforceable",
            "failed_sources": [
                {"source": src, "exclusion_reason": reason}
                for src, reason in sorted(other_excluded.items())
            ],
            "detail": (
                "a source was excluded before the gate ran and the publisher "
                "has no way to drop only that source, so the generation blocks"
            ),
        }

    return PhaseBResult(
        source_local=source_local,
        cross_source=cross_source,
        excluded_sources=excluded,
        blocked=blocked,
        blocked_reason=blocked_reason,
        total_duration_ms=total_duration_ms,
        total_budget_exceeded=total_budget_exceeded,
        substrate_integrity=integrity_results,
        skipped_invariants=skipped_invariants,
        unchecked_source_requirements=unchecked,
    )
