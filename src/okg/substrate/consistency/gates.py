"""Publish-gate algebra for consistency-check runs."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Literal, Mapping, Protocol
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.consistency.observations import (
    EXACT_COVERAGE_BASES,
    ConsistencyCheckDefinition,
)

log = logging.getLogger(__name__)


# Evaluation HEALTH only -- finding presence is carried in the *_count fields,
# the publish decision in ``blocks_publish`` (consistency-blocking-policy
# task.policy.result-fields). ``evaluated`` = the check ran cleanly over complete
# coverage, with OR without open findings (this replaces the old ``pass``/``fail``
# pair, which conflated "ran cleanly" with "has findings"). The ``blocked_*``
# values report operational health and are unchanged.
ConsistencyGateResult = Literal[
    "evaluated",
    "blocked_incomplete",
    "blocked_timeout",
    "blocked_error",
]


# Backpressure probing breaker (consistency-gate-durability task.backpressure.probe).
# A gate degraded under ``block_without_evaluator`` short-circuits, but every
# ``BACKPRESSURE_PROBE_EVERY`` short-circuits it RE-RUNS its evaluator once (a
# probe). A passing probe breaks the consecutive-operational-blocked streak and
# clears degradation with no config change; a failing probe re-enters
# short-circuit for the next interval (exactly one probe per interval). The env
# override lets operators retune the cadence without re-authoring config; a
# per-gate ``backpressure.probe_every`` config key wins over the env, which wins
# over this constant (resolution lives in the SQL gate).
DEFAULT_BACKPRESSURE_PROBE_EVERY = 5
BACKPRESSURE_PROBE_EVERY_ENV = "OKG_CONSISTENCY_BACKPRESSURE_PROBE_EVERY"

# The short-circuit rows the probe cadence counts: a degraded gate that skipped
# its evaluator persists a ``blocked_error`` with this reason. Real operational
# failures (gate_error / gate_timeout / *_incomplete) are NOT short-circuits.
_BACKPRESSURE_SHORT_CIRCUIT_REASON = "gate_degraded_backpressure"


@dataclass(frozen=True)
class ConsistencyPublishGateEvaluation:
    """Persisted publish-gate decision for one consistency run."""

    run_id: UUID
    check_id: str
    target_generation_id: int
    gate_name: str
    result: ConsistencyGateResult
    blocks_publish: bool
    reason: str
    open_observation_count: int
    coverage_complete: bool | None
    coverage_basis: str | None
    budget_ms: int | None
    elapsed_ms: int
    preflight: Mapping[str, Any] = field(default_factory=dict)
    details: Mapping[str, Any] = field(default_factory=dict)
    # Finding counts, first-class (task.policy.result-fields), separate from the
    # evaluation-health ``result``. ``newly_opened_count`` = findings this run
    # opened; ``resolved_count`` = findings this run resolved. Default 0 for
    # producers that do not track the observation ledger (e.g. the structural
    # transform gate).
    newly_opened_count: int = 0
    resolved_count: int = 0
    # Backpressure PROBE marker (task.backpressure.probe): true iff this run was a
    # probe -- an evaluator re-run a degraded gate performed instead of
    # short-circuiting. Persisted so the health summary counts probes separately
    # and the probe cadence never permanently latches. False for ordinary runs.
    is_probe: bool = False


@dataclass(frozen=True)
class ConsistencyGateHealth:
    """Recent gate-health summary for operator backpressure."""

    check_id: str
    gate_name: str
    total_runs: int
    p50_elapsed_ms: int
    p99_elapsed_ms: int
    timeout_rate: Decimal
    error_rate: Decimal
    blocked_rate: Decimal
    operational_blocked_rate: Decimal
    consecutive_blocked: int
    consecutive_operational_blocked: int
    max_consecutive_blocked: int
    degraded: bool
    degraded_reason: str | None = None
    # Backpressure probing-breaker lifecycle (task.backpressure.probe). These
    # distinguish probe outcomes from ordinary runs and drive the probe cadence:
    #   * ``probe_every`` -- short-circuits between probes (the configured interval).
    #   * ``short_circuits_since_probe`` -- consecutive short-circuit rows since the
    #     most recent probe (or since degradation onset); resets at each probe.
    #   * ``should_probe`` -- true when a degraded gate should RE-RUN its evaluator
    #     this cycle (``short_circuits_since_probe >= probe_every``) instead of
    #     short-circuiting again.
    #   * ``probe_run_count`` / ``probe_pass_count`` / ``probe_block_count`` -- probe
    #     rows in the window, split by outcome (pass = a clean ``evaluated`` probe
    #     that clears degradation; block = a probe that still failed operationally).
    probe_every: int = DEFAULT_BACKPRESSURE_PROBE_EVERY
    short_circuits_since_probe: int = 0
    should_probe: bool = False
    probe_run_count: int = 0
    probe_pass_count: int = 0
    probe_block_count: int = 0


class ConsistencyPublishGate(Protocol):
    """Publisher extension point for a blocking consistency check.

    Implementations own candidate generation/evaluation for the pending
    generation and return the persisted gate decision. The publisher only
    folds the decision into Phase B admission.
    """

    name: str

    def evaluate(
        self,
        conn: psycopg.Connection[Any],
        *,
        target_generation_id: int,
        catalog_version_id: int,
        deployment_name: str,
        branch_name: str = "default",
    ) -> ConsistencyPublishGateEvaluation:
        """Run the consistency check for a pending generation.

        ``branch_name`` is the branch being published; a graph-reading gate MUST
        evaluate THIS branch's head (not always ``default``) so a fork publish
        validates the fork's own delta (worktree-fork-writes: fork-publish-soundness).
        """


def evaluate_consistency_publish_gate(
    conn: psycopg.Connection[Any],
    definition: ConsistencyCheckDefinition,
    *,
    target_generation_id: int,
    run_id: UUID,
    gate_name: str | None = None,
    budget_ms: int | None = None,
    elapsed_ms: int = 0,
    preflight: Mapping[str, Any] | None = None,
    timed_out: bool = False,
    error: Mapping[str, Any] | str | None = None,
    backpressure: Mapping[str, Any] | None = None,
    blocks_publish: bool | None = None,
    block_on: str = "any_open",
    block_on_min_severity: str | None = None,
    is_probe: bool = False,
) -> ConsistencyPublishGateEvaluation:
    """Evaluate and persist a blocking gate decision for a check run.

    ``result`` reports EVALUATION HEALTH only (task.policy.result-fields):
    ``evaluated`` when the check ran cleanly over complete coverage (with OR
    without open findings), else an operational ``blocked_*``. Finding presence
    is carried in the ``*_count`` fields (open / newly-opened / resolved) and the
    publish decision in ``blocks_publish`` -- none of them in ``result``.

    ``block_on`` is the declarative blocking policy over findings
    (consistency-blocking-policy), applied only when ``blocks_publish`` is
    ``None`` (a deriving/blocking gate — an explicit ``True``/``False`` still
    wins unchanged):

    * ``any_open`` (default) reproduces the historical contract exactly — a
      clean evaluation with any open finding blocks, and every operational
      ``blocked_*`` blocks (equivalently, the pre-split ``blocks_publish =
      result != "pass"``).
    * ``new_findings`` blocks only when the run OPENS an observation this
      generation (a ``finding_event='observed'`` row at
      ``target_generation_id`` — not open at the previous published
      generation, per the bounded-event-growth invariant), optionally scoped
      to ``block_on_min_severity``. A pre-existing open backlog no longer
      blocks; an operational failure (timeout / error / incomplete coverage)
      still blocks.
    * ``never`` never blocks on findings; operational failures still block.

    This amends the V1 consistency-checks contract (openspec consistency-checks
    spec.md ~294-296: "every result other than ``pass`` MUST block") for the
    clean-evaluation-with-open-findings case; the operational ``blocked_*`` cases
    still always block.
    """

    gate = gate_name or definition.check_id
    _validate_budget(budget_ms=budget_ms, elapsed_ms=elapsed_ms)
    preflight_payload = dict(preflight or {})

    with conn.transaction():
        with conn.cursor() as cur:
            _assert_run_matches(
                cur,
                run_id=run_id,
                definition_id=definition.check_id,
                target_generation_id=target_generation_id,
            )

            coverage = _load_coverage(cur, run_id)
            open_count, open_sample = _open_observation_sample(
                cur,
                definition.check_id,
                target_generation_id,
            )
            # Surface the qualification facet (spec §9.10) on every sampled
            # finding: mandatory iff an open instance forces this gate to
            # block under the declared policy. Derived here — from the same
            # inputs the blocking decision uses — never persisted
            # independently, so it cannot drift from the gate. Under
            # ``new_findings`` the gate blocks only on findings THIS run
            # opened, so the facet needs each sampled finding's
            # newly-opened-ness — resolved exactly for the sampled ids
            # (never inferred from a capped sample).
            if block_on == "new_findings" and open_sample:
                newly_opened_ids = _observed_this_run(
                    cur,
                    definition.check_id,
                    run_id,
                    [row["observation_id"] for row in open_sample],
                )
            else:
                newly_opened_ids = frozenset()
            for sampled in open_sample:
                sampled["mandatory"] = finding_is_mandatory(
                    block_on=block_on,
                    severity=sampled["severity"],
                    block_on_min_severity=block_on_min_severity,
                    blocks_publish=blocks_publish,
                    newly_opened=(
                        sampled["observation_id"] in newly_opened_ids
                        if block_on == "new_findings"
                        else True
                    ),
                )

            # Finding-count fields (task.policy.result-fields): what THIS run did
            # to the observation ledger, independent of evaluation health or the
            # blocking policy. Unfiltered by severity -- the field is the true
            # tally; severity filtering is a policy concern (handled below). Read
            # from the ledger unconditionally so an operational run that wrote
            # partial rows reports them honestly (no default-value fallback).
            newly_opened_count, newly_opened_sample = _newly_opened_this_run(
                cur, definition.check_id, run_id, None
            )
            resolved_count = _resolved_this_run(cur, definition.check_id, run_id)

            if timed_out:
                result: ConsistencyGateResult = "blocked_timeout"
                reason = "gate_timeout"
                details: dict[str, Any] = {"open_observations": open_sample}
            elif backpressure is not None:
                result = "blocked_error"
                reason = "gate_degraded_backpressure"
                details = {
                    "backpressure": dict(backpressure),
                    "open_observations": open_sample,
                }
            elif error is not None:
                result = "blocked_error"
                reason = "gate_error"
                details = {
                    "error": error if isinstance(error, str) else dict(error),
                    "open_observations": open_sample,
                }
            elif coverage is None:
                result = "blocked_incomplete"
                reason = "missing_candidate_coverage"
                details = {"open_observations": open_sample}
            elif not _coverage_is_exact(coverage):
                # Genuinely incomplete: a non-exact basis (heuristic / sampled /
                # incomplete) or cache-miss gaps. NOT a scoped-exact run.
                result = "blocked_incomplete"
                reason = "candidate_coverage_incomplete"
                details = {
                    "coverage": _coverage_details(coverage),
                    "open_observations": open_sample,
                }
            else:
                # Ran cleanly and EXACTLY over what it attempted -- universe-exact
                # OR engine-certified scoped-exact (scoped + an exact basis;
                # task.scope.first-class-basis-and-health). Both are
                # evaluation-HEALTHY, so an honest incremental run is no longer
                # mislabeled blocked_incomplete. The finding tally lives in the
                # *_count fields; ``reason`` records the finding-present nuance for
                # operators (and scoped-ness is auditable in the coverage
                # details), but ``result`` is pure health.
                result = "evaluated"
                if open_count:
                    reason = "open_observations"
                    details = {
                        "coverage": _coverage_details(coverage),
                        "open_observations": open_sample,
                    }
                else:
                    reason = "complete_no_open_observations"
                    details = {"coverage": _coverage_details(coverage)}

            if blocks_publish is None:
                # Deriving/blocking-gate path: the declarative policy governs
                # whether findings block. Operational results (timeout / error
                # / incomplete coverage) always block regardless of policy.
                operational = result in (
                    "blocked_timeout",
                    "blocked_error",
                    "blocked_incomplete",
                )
                details["block_on"] = block_on
                if block_on_min_severity is not None:
                    details["block_on_min_severity"] = block_on_min_severity
                # A clean evaluation with open findings is the only case the
                # policy governs (``not operational and open_count`` is exactly
                # the old ``result == 'fail'`` set). ``any_open`` then blocks on
                # any open finding, reproducing the historical contract.
                if not operational and open_count:
                    if block_on == "never":
                        finding_blocks = False
                    elif block_on == "new_findings":
                        # The block decision is severity-filtered; the
                        # ``newly_opened_count`` FIELD above is the unfiltered
                        # tally, so reuse it only when no severity floor applies.
                        if block_on_min_severity is None:
                            blocking_new_count = newly_opened_count
                            blocking_new_sample = newly_opened_sample
                        else:
                            blocking_new_count, blocking_new_sample = (
                                _newly_opened_this_run(
                                    cur,
                                    definition.check_id,
                                    run_id,
                                    block_on_min_severity,
                                )
                            )
                        finding_blocks = blocking_new_count > 0
                        details["new_findings_count"] = blocking_new_count
                        if finding_blocks:
                            details["new_findings"] = blocking_new_sample
                    else:  # any_open
                        finding_blocks = True
                else:
                    finding_blocks = False
                derived_blocks_publish = operational or finding_blocks
            else:
                derived_blocks_publish = bool(blocks_publish)

            evaluation = ConsistencyPublishGateEvaluation(
                run_id=run_id,
                check_id=definition.check_id,
                target_generation_id=target_generation_id,
                gate_name=gate,
                result=result,
                blocks_publish=derived_blocks_publish,
                reason=reason,
                open_observation_count=open_count,
                coverage_complete=(
                    None if coverage is None else bool(coverage["coverage_complete"])
                ),
                coverage_basis=(
                    None if coverage is None else str(coverage["coverage_basis"])
                ),
                budget_ms=budget_ms,
                elapsed_ms=elapsed_ms,
                preflight=preflight_payload,
                details=details,
                newly_opened_count=newly_opened_count,
                resolved_count=resolved_count,
                is_probe=is_probe,
            )
            _persist_gate_evaluation(cur, evaluation)
            return evaluation


def persist_precomputed_gate_evaluation(
    conn: psycopg.Connection[Any],
    evaluation: ConsistencyPublishGateEvaluation,
) -> ConsistencyPublishGateEvaluation:
    """Persist a gate evaluation whose verdict the PRODUCER already decided.

    Unlike :func:`evaluate_consistency_publish_gate` -- which DERIVES ``result``
    and ``blocks_publish`` from the run's coverage row and observation ledger --
    this writes the caller's evaluation verbatim. It is for structural gates that
    reach their verdict WITHOUT the observation ledger (e.g.
    ``TransformConsistencyGate``: a point-in-time antijoin over dangling /
    narrowing / node-subtype violations that has no coverage row and no
    observation_current rows). Routing such a gate through
    ``evaluate_consistency_publish_gate`` would mislabel its clean run
    ``blocked_incomplete`` (no coverage row) and re-derive a different block
    decision; this helper preserves the producer's decision EXACTLY and only adds
    durability.

    The run FK is validated first (the row must reference a real
    ``derived_artifact_runs`` row for this check + generation), so a fabricated
    ``run_id`` fails loudly rather than orphaning a gate row.
    """

    with conn.transaction():
        with conn.cursor() as cur:
            _assert_run_matches(
                cur,
                run_id=evaluation.run_id,
                definition_id=evaluation.check_id,
                target_generation_id=evaluation.target_generation_id,
            )
            _persist_gate_evaluation(cur, evaluation)
    return evaluation


def consistency_gate_summary(
    evaluation: ConsistencyPublishGateEvaluation,
) -> dict[str, Any]:
    """JSON-friendly operator summary for one persisted evaluation."""

    return {
        "check_id": evaluation.check_id,
        "gate_name": evaluation.gate_name,
        "target_generation_id": evaluation.target_generation_id,
        "run_id": str(evaluation.run_id),
        "result": evaluation.result,
        "blocks_publish": evaluation.blocks_publish,
        "reason": evaluation.reason,
        "is_probe": evaluation.is_probe,
        "open_observation_count": evaluation.open_observation_count,
        "newly_opened_count": evaluation.newly_opened_count,
        "resolved_count": evaluation.resolved_count,
        "coverage_complete": evaluation.coverage_complete,
        "coverage_basis": evaluation.coverage_basis,
        "budget_ms": evaluation.budget_ms,
        "elapsed_ms": evaluation.elapsed_ms,
        "preflight": dict(evaluation.preflight),
        "details": dict(evaluation.details),
    }


def summarize_consistency_gate_health(
    conn: psycopg.Connection[Any],
    *,
    check_id: str,
    gate_name: str,
    lookback: int = 20,
    max_consecutive_blocked: int = 2,
    probe_every: int = DEFAULT_BACKPRESSURE_PROBE_EVERY,
) -> ConsistencyGateHealth:
    """Summarize recent gate outcomes for backpressure decisions.

    ``probe_every`` is the probing-breaker cadence (task.backpressure.probe): a
    degraded gate re-runs its evaluator (a probe) once every ``probe_every``
    short-circuited runs. ``should_probe`` in the returned health is the decision
    the gate reads to break out of the short-circuit latch; a passing probe
    breaks the ``consecutive_operational_blocked`` streak and clears ``degraded``
    on the next summary. A non-positive ``probe_every`` disables probing (the gate
    stays a pure latch — the pre-probe behavior).
    """

    if lookback <= 0:
        raise ValueError("lookback must be positive")
    if max_consecutive_blocked < 0:
        raise ValueError("max_consecutive_blocked must be non-negative")

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT result, blocks_publish, elapsed_ms, reason, is_probe
              FROM okg.consistency_gate_results
             WHERE check_id = %(check_id)s
               AND gate_name = %(gate_name)s
             ORDER BY created_at DESC, target_generation_id DESC
             LIMIT %(lookback)s
            """,
            {
                "check_id": check_id,
                "gate_name": gate_name,
                "lookback": lookback,
            },
        )
        rows = cur.fetchall()

    results = [str(row[0]) for row in rows]
    blocked = [bool(row[1]) for row in rows]
    elapsed = [int(row[2] or 0) for row in rows]
    reasons = [str(row[3] or "") for row in rows]
    is_probe_flags = [bool(row[4]) for row in rows]
    consecutive_blocked = 0
    for is_blocked in blocked:
        if not is_blocked:
            break
        consecutive_blocked += 1
    operational_results = {
        "blocked_incomplete",
        "blocked_timeout",
        "blocked_error",
    }
    consecutive_operational_blocked = 0
    for result in results:
        if result not in operational_results:
            break
        consecutive_operational_blocked += 1

    # Probe cadence: count consecutive SHORT-CIRCUIT rows (a degraded gate that
    # skipped its evaluator: blocked_error with the backpressure reason and NOT a
    # probe) newest-first, stopping at the first non-short-circuit row. Because a
    # probe row is never a short-circuit, this resets to 0 at the most recent
    # probe -- so a failing probe re-enters short-circuit for the NEXT full
    # interval (exactly one probe per interval) and never latches.
    short_circuits_since_probe = 0
    for result, reason, was_probe in zip(results, reasons, is_probe_flags):
        if (
            not was_probe
            and result == "blocked_error"
            and reason == _BACKPRESSURE_SHORT_CIRCUIT_REASON
        ):
            short_circuits_since_probe += 1
        else:
            break

    total = len(rows)
    timeout_count = sum(1 for result in results if result == "blocked_timeout")
    error_count = sum(1 for result in results if result == "blocked_error")
    blocked_count = sum(1 for is_blocked in blocked if is_blocked)
    operational_blocked_count = sum(
        1 for result in results if result in operational_results
    )
    # Probe outcomes, counted separately from ordinary runs (task.backpressure.probe:
    # "distinguishes probe outcomes"). A probe that came back ``evaluated`` cleared
    # the operational streak (a pass); a probe that stayed operational blocked.
    probe_run_count = sum(1 for was_probe in is_probe_flags if was_probe)
    probe_pass_count = sum(
        1
        for result, was_probe in zip(results, is_probe_flags)
        if was_probe and result not in operational_results
    )
    probe_block_count = probe_run_count - probe_pass_count
    degraded = (
        max_consecutive_blocked > 0
        and consecutive_operational_blocked > max_consecutive_blocked
    )
    # short_circuits_since_probe is capped by the LIMIT lookback window, so a
    # probe_every GREATER than lookback would make should_probe UNREACHABLE --
    # silently re-creating the permanent latch this breaker exists to remove
    # (adversarial review F1; insidiously triggered by lowering lookback below
    # the default probe_every). Clamp the effective cadence to lookback and warn
    # loudly: the probe then fires at worst every `lookback` short-circuits.
    effective_probe_every = probe_every
    if lookback < probe_every:
        log.warning(
            "backpressure probe_every=%d exceeds health lookback=%d for "
            "check_id=%s gate=%s; clamping the probe cadence to lookback so the "
            "breaker cannot silently latch (raise lookback or lower probe_every)",
            probe_every, lookback, check_id, gate_name,
        )
        effective_probe_every = lookback
    should_probe = (
        degraded
        and effective_probe_every > 0
        and short_circuits_since_probe >= effective_probe_every
    )

    return ConsistencyGateHealth(
        check_id=check_id,
        gate_name=gate_name,
        total_runs=total,
        p50_elapsed_ms=_percentile(elapsed, 0.50),
        p99_elapsed_ms=_percentile(elapsed, 0.99),
        timeout_rate=_rate(timeout_count, total),
        error_rate=_rate(error_count, total),
        blocked_rate=_rate(blocked_count, total),
        operational_blocked_rate=_rate(operational_blocked_count, total),
        consecutive_blocked=consecutive_blocked,
        consecutive_operational_blocked=consecutive_operational_blocked,
        max_consecutive_blocked=max_consecutive_blocked,
        degraded=degraded,
        degraded_reason=(
            "consecutive_operational_blocks" if degraded else None
        ),
        probe_every=effective_probe_every,
        short_circuits_since_probe=short_circuits_since_probe,
        should_probe=should_probe,
        probe_run_count=probe_run_count,
        probe_pass_count=probe_pass_count,
        probe_block_count=probe_block_count,
    )


def _assert_run_matches(
    cur: psycopg.Cursor[Any],
    *,
    run_id: UUID,
    definition_id: str,
    target_generation_id: int,
) -> None:
    cur.execute(
        """
        SELECT definition_id, target_generation_id
          FROM okg.derived_artifact_runs
         WHERE run_id = %(run_id)s
        """,
        {"run_id": run_id},
    )
    row = cur.fetchone()
    if row is None:
        raise ValueError("run_id does not reference a derived artifact run")

    run_definition_id, run_generation_id = row
    if run_definition_id != definition_id:
        raise ValueError(
            "run_id belongs to definition "
            f"{run_definition_id}, expected {definition_id}"
        )
    if int(run_generation_id) != target_generation_id:
        raise ValueError(
            "run_id targets generation "
            f"{run_generation_id}, expected {target_generation_id}"
        )


def _load_coverage(
    cur: psycopg.Cursor[Any],
    run_id: UUID,
) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT coverage_complete, coverage_basis, eligible_input_count,
               candidate_count, excluded_input_counts,
               missing_text_count, missing_embedding_count,
               all_pairs_upper_bound, reduction_ratio,
               evaluator_invocation_count, scoped, scope_subject_count
          FROM okg.consistency_candidate_coverage
         WHERE run_id = %(run_id)s
        """,
        {"run_id": run_id},
    )
    row = cur.fetchone()
    if row is None:
        return None
    return {
        "coverage_complete": row[0],
        "coverage_basis": row[1],
        "eligible_input_count": row[2],
        "candidate_count": row[3],
        "excluded_input_counts": row[4] or {},
        "missing_text_count": row[5],
        "missing_embedding_count": row[6],
        "all_pairs_upper_bound": row[7],
        "reduction_ratio": row[8],
        "evaluator_invocation_count": row[9],
        "scoped": bool(row[10]),
        "scope_subject_count": int(row[11] or 0),
    }


def _open_observation_sample(
    cur: psycopg.Cursor[Any],
    check_id: str,
    target_generation_id: int,
) -> tuple[int, list[dict[str, Any]]]:
    cur.execute(
        """
        SELECT count(*)
          FROM okg.observation_current
         WHERE check_id = %(check_id)s
           AND finding_status = 'open'
           AND latest_generation_id <= %(target_generation_id)s
        """,
        {
            "check_id": check_id,
            "target_generation_id": target_generation_id,
        },
    )
    count = int(cur.fetchone()[0] or 0)
    if count == 0:
        return count, []

    cur.execute(
        """
        SELECT observation_id, candidate_id, input_scope,
               predicate_key, severity, confidence
          FROM okg.observation_current
         WHERE check_id = %(check_id)s
           AND finding_status = 'open'
           AND latest_generation_id <= %(target_generation_id)s
         ORDER BY severity NULLS LAST, observation_id
         LIMIT 5
        """,
        {
            "check_id": check_id,
            "target_generation_id": target_generation_id,
        },
    )
    sample = [
        {
            "observation_id": row[0],
            "candidate_id": row[1],
            "input_scope": row[2],
            "predicate_key": row[3],
            "severity": row[4],
            "confidence": None if row[5] is None else str(row[5]),
        }
        for row in cur.fetchall()
    ]
    return count, sample


_SEVERITY_RANK = {"low": 0, "medium": 1, "high": 2}


def finding_is_mandatory(
    *,
    block_on: str,
    severity: str | None,
    block_on_min_severity: str | None = None,
    blocks_publish: bool | None = None,
    newly_opened: bool = True,
) -> bool:
    """The Concord "qualified" predicate's per-finding facet (spec §9.10).

    A finding is *mandatory* (the paper's infinite-cost violation) iff
    leaving an open instance of it unresolved forces the publish gate to
    block under the check's declared blocking config plus the deployment
    policy. This is a pure derivation over the SAME inputs
    :func:`evaluate_consistency_publish_gate` uses — no new persisted
    state, so the facet cannot drift from the gate; the qualification
    witnesses assert their agreement through the real gate path,
    including the backlog-under-``new_findings`` case.

    Mirrors the gate exactly:

    - An explicit deployment ``blocks_publish`` override wins in both
      directions (advisory ⇒ nothing is mandatory; forced ⇒ everything is).
    - ``never`` ⇒ not mandatory; ``any_open`` ⇒ mandatory.
    - ``new_findings`` ⇒ mandatory iff the finding is ``newly_opened``
      (opened by the current run — exactly the set the gate blocks on),
      severity-gated by ``block_on_min_severity`` when declared. A
      pre-existing open backlog is NOT mandatory: the gate publishes over
      it, and stamping it mandatory would be dishonest telemetry. The
      parameter defaults to ``True`` for prospective use ("would a new
      instance of this finding be mandatory?" — the truth-table view);
      the gate stamp passes each sampled finding's actual
      newly-opened-ness. An unknown or NULL severity ranks MAX
      (fail-safe): it is never silently non-mandatory.
    """
    if blocks_publish is not None:
        return bool(blocks_publish)
    if block_on == "never":
        return False
    if block_on == "any_open":
        return True
    if block_on == "new_findings":
        if not newly_opened:
            return False
        if block_on_min_severity is None:
            return True
        # Case-fold both sides, matching the gate SQL's lower(severity) —
        # severity is unconstrained text, and 'Low' must rank as 'low',
        # not as unknown. A floor that is unknown AFTER folding raises:
        # the gate's own SQL silently coerces such a floor to rank 0
        # (a default-value fallback, the pre-existing wart); this facet
        # surfaces the malformed declaration loudly instead.
        floor = _SEVERITY_RANK.get(block_on_min_severity.lower())
        if floor is None:
            raise ValueError(
                f"unknown block_on_min_severity {block_on_min_severity!r}; "
                f"expected one of {sorted(_SEVERITY_RANK)} (a malformed "
                f"blocking floor must not silently decide qualification)"
            )
        rank = (
            _SEVERITY_RANK.get(severity.lower())
            if severity is not None
            else None
        )
        if rank is None:
            return True
        return rank >= floor
    raise ValueError(
        f"unknown block_on policy {block_on!r}; expected "
        f"'never' | 'new_findings' | 'any_open'"
    )


def _observed_this_run(
    cur: psycopg.Cursor[Any],
    check_id: str,
    run_id: UUID,
    observation_ids: list[str],
) -> frozenset[str]:
    """Which of ``observation_ids`` did THIS run open (``observed`` event)?

    Exact membership for the given ids — used to stamp the §9.10 mandatory
    facet on the sampled open findings under ``new_findings``, where only
    findings the current run opened can block. Queries by id, never by a
    capped sample, so the answer is precise for every id asked about.
    """
    if not observation_ids:
        return frozenset()
    cur.execute(
        """
        SELECT DISTINCT observation_id
          FROM okg.observation_events
         WHERE check_id = %(check_id)s
           AND run_id = %(run_id)s
           AND finding_event = 'observed'
           AND observation_id = ANY(%(observation_ids)s)
        """,
        {
            "check_id": check_id,
            "run_id": run_id,
            "observation_ids": observation_ids,
        },
    )
    return frozenset(str(row[0]) for row in cur.fetchall())


def _newly_opened_this_run(
    cur: psycopg.Cursor[Any],
    check_id: str,
    run_id: UUID,
    min_severity: str | None,
) -> tuple[int, list[dict[str, Any]]]:
    """Findings THIS run (``run_id``) OPENED.

    A ``finding_event='observed'`` row this run is exactly a newly-opened /
    reopened / content-changed finding: the bounded-event-growth invariant
    suppresses the re-``observed`` event for a still-open, unchanged finding,
    so a pre-existing open backlog emits no ``observed`` row this run. Returns
    the count of DISTINCT observation_id and a small capped sample.

    Scoped by ``run_id``, NOT ``target_generation_id``: consecutive / no-op /
    fork-replay publishes can resolve to the SAME compat generation id, so a
    generation-scoped read would count a prior run's new findings; the run's
    own ``observed`` rows are collision-proof.

    ``min_severity`` (if given) keeps only rows whose severity rank is at least
    the threshold (low=0 / medium=1 / high=2). An unrecognized or NULL severity
    is treated as MAXIMUM rank (blocks) — a genuinely new finding is never
    silently let through for lack of a severity label (fail-safe).
    """

    min_rank = _SEVERITY_RANK.get(min_severity or "", 0) if min_severity else None
    severity_rank_sql = (
        "CASE lower(severity) "
        "WHEN 'low' THEN 0 WHEN 'medium' THEN 1 WHEN 'high' THEN 2 "
        "ELSE 2 END"  # unknown/NULL -> max: never silently drop a new finding
    )
    filter_sql = ""
    params: dict[str, Any] = {
        "check_id": check_id,
        "run_id": run_id,
    }
    if min_rank is not None:
        filter_sql = f"AND {severity_rank_sql} >= %(min_rank)s"
        params["min_rank"] = min_rank

    cur.execute(
        f"""
        SELECT count(DISTINCT observation_id)
          FROM okg.observation_events
         WHERE check_id = %(check_id)s
           AND run_id = %(run_id)s
           AND finding_event = 'observed'
           {filter_sql}
        """,
        params,
    )
    count = int(cur.fetchone()[0] or 0)
    if count == 0:
        return count, []

    cur.execute(
        f"""
        SELECT DISTINCT ON (observation_id)
               observation_id, normalized_predicate, input_scope, severity
          FROM okg.observation_events
         WHERE check_id = %(check_id)s
           AND run_id = %(run_id)s
           AND finding_event = 'observed'
           {filter_sql}
         ORDER BY observation_id, event_seq DESC
         LIMIT 5
        """,
        params,
    )
    sample = [
        {
            "observation_id": row[0],
            "normalized_predicate": row[1],
            "input_scope": row[2],
            "severity": row[3],
        }
        for row in cur.fetchall()
    ]
    return count, sample


def _resolved_this_run(
    cur: psycopg.Cursor[Any],
    check_id: str,
    run_id: UUID,
) -> int:
    """Count findings THIS run (``run_id``) RESOLVED.

    A ``finding_event='resolved'`` row this run marks an observation that was
    open and is now closed (the IR executor emits one per resolution). Scoped by
    ``run_id`` (not ``target_generation_id``) for the same compat-collision
    reason as :func:`_newly_opened_this_run`. Unfiltered by severity -- the
    resolved tally is a first-class count field, not a policy input.
    """

    cur.execute(
        """
        SELECT count(DISTINCT observation_id)
          FROM okg.observation_events
         WHERE check_id = %(check_id)s
           AND run_id = %(run_id)s
           AND finding_event = 'resolved'
        """,
        {"check_id": check_id, "run_id": run_id},
    )
    return int(cur.fetchone()[0] or 0)


# Coverage bases the engine certifies as EXACT over what the run attempted
# (the whole universe when unscoped, the derived subset when scoped). A
# scoped run with an exact basis is scoped-EXACT (healthy); a non-exact basis
# (heuristic / sampled_monitoring / incomplete) is genuinely incomplete.
# Shared with the producers via observations.EXACT_COVERAGE_BASES so a gate row
# and its coverage row cannot disagree about what "exact" means.
_EXACT_COVERAGE_BASES = EXACT_COVERAGE_BASES


def _coverage_is_exact(coverage: Mapping[str, Any]) -> bool:
    """True iff the run exactly covered what it attempted.

    Universe-exact (``coverage_complete``) OR engine-certified scoped-exact (the
    run was ``scoped`` and its ``coverage_basis`` is exact — exact over the
    derived subset, not universe-complete). This is the gate-health signal
    (task.scope.first-class-basis-and-health): a scoped-exact run is HEALTHY,
    not ``blocked_incomplete``.
    """

    if coverage["coverage_complete"]:
        # Universe-exact: the DB CHECK already guarantees no missing counts here.
        return True
    # Scoped-exact: scoped AND an exact basis AND no gaps WITHIN the scope. A
    # scoped run that skipped subjects (cache-miss text / embedding gaps) is
    # scoped-INCOMPLETE, not scoped-exact -- never certify it healthy. The
    # coverage_complete=>no-missing CHECK does NOT constrain a scoped run
    # (coverage_complete=false), so the gate must check the counts itself. The
    # current IR producer never emits missing>0 on a scoped run (pure graph
    # antijoin), so this is defense-in-depth against a future scoped producer
    # that touches text / embeddings (no silent false-healthy).
    return (
        bool(coverage.get("scoped"))
        and coverage["coverage_basis"] in _EXACT_COVERAGE_BASES
        and int(coverage.get("missing_text_count") or 0) == 0
        and int(coverage.get("missing_embedding_count") or 0) == 0
    )


def _coverage_details(coverage: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "coverage_complete": bool(coverage["coverage_complete"]),
        "coverage_basis": coverage["coverage_basis"],
        "scoped": bool(coverage.get("scoped")),
        "scope_subject_count": int(coverage.get("scope_subject_count") or 0),
        "scoped_exact": _coverage_is_exact(coverage)
        and not bool(coverage["coverage_complete"]),
        "eligible_input_count": int(coverage["eligible_input_count"]),
        "candidate_count": int(coverage["candidate_count"]),
        "excluded_input_counts": dict(coverage["excluded_input_counts"]),
        "missing_text_count": int(coverage["missing_text_count"]),
        "missing_embedding_count": int(coverage["missing_embedding_count"]),
        "all_pairs_upper_bound": int(coverage["all_pairs_upper_bound"]),
        "reduction_ratio": str(coverage["reduction_ratio"]),
        "evaluator_invocation_count": int(
            coverage["evaluator_invocation_count"]
        ),
    }


def _persist_gate_evaluation(
    cur: psycopg.Cursor[Any],
    evaluation: ConsistencyPublishGateEvaluation,
) -> None:
    cur.execute(
        """
        INSERT INTO okg.consistency_gate_results (
          run_id, check_id, target_generation_id, gate_name,
          result, blocks_publish, reason, open_observation_count,
          newly_opened_count, resolved_count, is_probe,
          coverage_complete, coverage_basis, budget_ms, elapsed_ms,
          preflight, details
        ) VALUES (
          %(run_id)s, %(check_id)s, %(target_generation_id)s,
          %(gate_name)s, %(result)s, %(blocks_publish)s, %(reason)s,
          %(open_observation_count)s,
          %(newly_opened_count)s, %(resolved_count)s, %(is_probe)s,
          %(coverage_complete)s,
          %(coverage_basis)s, %(budget_ms)s, %(elapsed_ms)s,
          %(preflight)s, %(details)s
        )
        ON CONFLICT (run_id, gate_name) DO UPDATE SET
          check_id = EXCLUDED.check_id,
          target_generation_id = EXCLUDED.target_generation_id,
          gate_name = EXCLUDED.gate_name,
          result = EXCLUDED.result,
          blocks_publish = EXCLUDED.blocks_publish,
          reason = EXCLUDED.reason,
          open_observation_count = EXCLUDED.open_observation_count,
          newly_opened_count = EXCLUDED.newly_opened_count,
          resolved_count = EXCLUDED.resolved_count,
          is_probe = EXCLUDED.is_probe,
          coverage_complete = EXCLUDED.coverage_complete,
          coverage_basis = EXCLUDED.coverage_basis,
          budget_ms = EXCLUDED.budget_ms,
          elapsed_ms = EXCLUDED.elapsed_ms,
          preflight = EXCLUDED.preflight,
          details = EXCLUDED.details,
          updated_at = now()
        """,
        {
            "run_id": evaluation.run_id,
            "check_id": evaluation.check_id,
            "target_generation_id": evaluation.target_generation_id,
            "gate_name": evaluation.gate_name,
            "result": evaluation.result,
            "blocks_publish": evaluation.blocks_publish,
            "reason": evaluation.reason,
            "open_observation_count": evaluation.open_observation_count,
            "newly_opened_count": evaluation.newly_opened_count,
            "resolved_count": evaluation.resolved_count,
            "is_probe": evaluation.is_probe,
            "coverage_complete": evaluation.coverage_complete,
            "coverage_basis": evaluation.coverage_basis,
            "budget_ms": evaluation.budget_ms,
            "elapsed_ms": evaluation.elapsed_ms,
            "preflight": Jsonb(dict(evaluation.preflight)),
            "details": Jsonb(dict(evaluation.details)),
        },
    )


def _validate_budget(
    *,
    budget_ms: int | None,
    elapsed_ms: int,
) -> None:
    if budget_ms is not None and budget_ms < 0:
        raise ValueError("budget_ms must be non-negative")
    if elapsed_ms < 0:
        raise ValueError("elapsed_ms must be non-negative")


def _percentile(values: list[int], fraction: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    idx = min(
        len(ordered) - 1,
        max(0, int(round((len(ordered) - 1) * fraction))),
    )
    return ordered[idx]


def _rate(count: int, total: int) -> Decimal:
    if total == 0:
        return Decimal("0")
    return (Decimal(count) / Decimal(total)).quantize(Decimal("0.000001"))
