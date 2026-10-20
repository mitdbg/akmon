"""Consistency-check candidates and observation lifecycle.

This is the first executable slice of the consistency-check contract.
It intentionally does not run evaluators. It gives evaluators a durable
substrate target: deterministic candidate ids, deterministic
observation ids, append-only observation events, and a current-state
projection with split finding/evaluation status.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
import hashlib
from math import ceil
from typing import Any, Literal, Mapping, Sequence
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.derived_artifacts import stable_hash


ConfidenceType = Literal[
    "deterministic_boolean",
    "calibrated_probability",
    "thresholded_score",
    "ordinal_label",
]
CoverageStatus = Literal["candidate", "excluded", "not_evaluated"]
CoverageBasis = Literal[
    "exact_postgres",
    "exact_snapshot",
    "heuristic",
    "sampled_monitoring",
    "incomplete",
]
EvaluationEvent = Literal[
    "evaluated",
    "not_evaluated",
    "coverage_incomplete",
    "error",
]
FindingEvent = Literal["observed", "resolved", "reopened"]
FindingStatus = Literal["absent", "open", "resolved"]
_ObservationCurrentStage = Literal[
    "okg_bulk_observation_events",
    "okg_sql_native_observation_events",
    "okg_observation_current_dedupe_keys",
]


CHECK_KINDS = frozenset({
    "rule_based",
    "security",
    "statistical",
    "semantic",
})


@dataclass(frozen=True)
class ConsistencyCheckDefinition:
    """Versioned definition metadata for a consistency check.

    ``check_kind`` is the AKMON Phase 7 integrity taxonomy:

      * ``rule_based`` — deterministic catalog / narrowing rule.
      * ``security`` — content / policy gate; pre-publish blocker.
      * ``statistical`` — drift / coverage / distribution observation.
      * ``semantic`` — LLM-judge invariant.

    None is permitted for legacy registrations; the doctor surface
    surfaces NULL rows so deployments can audit coverage.
    """

    check_id: str
    deployment_name: str
    predicate_family: str
    candidate_rule_hash: str
    evaluator_ref: str
    evaluator_hash: str
    confidence_type: ConfidenceType
    compute_hash: str
    output_schema_hash: str
    renderer_hash: str
    dirty_rule_hash: str
    coverage_contract: Mapping[str, Any] = field(default_factory=dict)
    severity_policy: Mapping[str, Any] = field(default_factory=dict)
    check_kind: str | None = None

    def __post_init__(self) -> None:
        if self.check_kind is not None and self.check_kind not in CHECK_KINDS:
            raise ValueError(
                f"check_kind must be one of {sorted(CHECK_KINDS)} "
                f"or None; got {self.check_kind!r}"
            )


@dataclass(frozen=True)
class ConsistencyCandidate:
    """One durable candidate for a consistency check run."""

    input_scope: str
    blocking_rule_hash: str
    participant_roles: Mapping[str, str]
    predicate_key: str
    generation_policy: str
    coverage_status: CoverageStatus = "candidate"
    exclusion_reason: str | None = None
    candidate_payload: Mapping[str, Any] = field(default_factory=dict)
    symmetric_predicate: bool = False


@dataclass(frozen=True)
class ObservationEvent:
    """Append-only observation lifecycle event."""

    observation_id: str
    candidate_id: str
    input_scope: str
    predicate_key: str
    normalized_predicate: str
    normalized_values: Mapping[str, Any]
    participant_roles: Mapping[str, str]
    finding_event: FindingEvent | None = None
    evaluation_event: EvaluationEvent | None = None
    severity: str | None = None
    confidence: Decimal | float | str | None = None
    confidence_type: ConfidenceType | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)
    payload: Mapping[str, Any] = field(default_factory=dict)
    coverage_complete: bool = True
    symmetric_predicate: bool = False


@dataclass(frozen=True)
class ConsistencyCandidateObservation:
    """Prepared candidate plus optional observation event for bulk writes."""

    candidate: ConsistencyCandidate
    candidate_id: str
    event: ObservationEvent | None = None


class HistoricalCandidateIdentityUnverifiable(ValueError):
    """A resolution cannot prove its candidate against durable history.

    Historical candidate ids include definition hashes that are overwritten
    when a stable check id is re-registered.  Resolution therefore validates
    the identity-bearing fields that remain on every persisted candidate row;
    it never substitutes the current definition or accepts an unchecked id.
    """


# --- scope identity ---------------------------------------------------------
# A scoped run that persists only a COUNT tells a reader that some subjects
# went unevaluated but never WHICH, so the complement -- the unevaluated set,
# the thing an operator needs in order to judge whether the verdict is safe to
# act on -- is not recoverable from the published generation. Every scoped path
# persists IDENTITY through this helper: the ids themselves, plus a digest over
# the full sorted set so a truncated list is still attributable.
#
# The cap exists so one pathological delta cannot bloat a JSONB column. When it
# binds it is disclosed LOUDLY (``*_ids_truncated: true`` alongside the true
# count and the digest) -- a partial list is never presented as the whole scope.
SCOPE_ID_LIMIT = 10_000

# A digest nobody can recompute is not identity, it is decoration. The algorithm
# is therefore (a) named in a constant, (b) persisted BESIDE every digest so a
# reader knows which function to run, and (c) implemented once for Python
# (``scope_digest``) and once for SQL (``scope_digest_sql``) from the same
# specification, with a test that the two agree byte-for-byte.
#
# The sort is over the UTF-8 CODEPOINT order of the stringified ids. Postgres
# `ORDER BY <text>` uses the database COLLATION, which for `en_US.UTF-8` sorts
# mixed-case and punctuated identifiers differently from `C` -- the same subject
# set would digest to two different values depending on where the check ran, and
# to a third under Python's codepoint sort. `scope_digest_sql` forces
# `COLLATE "C"` (byte order over UTF-8 == codepoint order) so all three agree.
SCOPE_DIGEST_ALGORITHM = (
    "sha256_hex(utf8(join(sort_codepoint(map(str, ids)), '\\n')))"
)


def scope_digest(ids: Sequence[Any]) -> str:
    """The canonical scope digest over the FULL id set (never the capped list)."""

    values = sorted(str(value) for value in ids)
    return hashlib.sha256("\n".join(values).encode("utf-8")).hexdigest()


def scope_digest_sql(column: str) -> str:
    """``scope_digest`` as a SQL aggregate expression over ``column``.

    Collation-independent by construction (``COLLATE "C"``), and empty-set-safe:
    the COALESCE makes an empty scope digest the sha256 of the empty string,
    exactly as ``scope_digest([])`` does.
    """

    return (
        "encode(sha256(convert_to(COALESCE("
        f"string_agg({column}::text, E'\\n' ORDER BY {column}::text COLLATE \"C\")"
        ", ''), 'UTF8')), 'hex')"
    )


def scope_identity(name: str, ids: Sequence[Any]) -> dict[str, Any]:
    """Identity (not merely cardinality) for one scoped subject set.

    The persisted ids are the SAME strings the digest is taken over, so a reader
    can recompute the digest from the list when the cap did not bind.
    """

    values = [str(value) for value in ids]
    return {
        f"{name}_ids": values[:SCOPE_ID_LIMIT],
        f"{name}_count": len(values),
        f"{name}_ids_truncated": len(values) > SCOPE_ID_LIMIT,
        f"{name}_digest_sha256": scope_digest(values),
        f"{name}_digest_algorithm": SCOPE_DIGEST_ALGORITHM,
    }


# --- complement recipe ------------------------------------------------------
# The recipe tells a reader how to compute the UNEVALUATED complement. When the
# id list is CAPPED the subtraction is wrong in the dangerous direction: it
# reports thousands of subjects that WERE evaluated as unevaluated. A recipe
# that produces a wrong answer is worse than no recipe, so a truncated scope
# gets an explicit refusal naming the digest instead of a subtraction.
_TRUNCATED_RECIPE = (
    "NOT COMPUTABLE from this row: {ids_field} is capped at {limit} of "
    "{count} evaluated subjects, so subtracting it would report evaluated "
    "subjects as UNEVALUATED. Use {digest_field} (which covers the full set) "
    "to attribute the scope, or re-run the check unscoped."
)


def scope_complement_recipe(
    name: str,
    identity: Mapping[str, Any],
    *,
    universe: str,
    ids_path: str | None = None,
) -> str:
    """How to compute the unevaluated complement -- or why it cannot be.

    ``universe`` names the superset (e.g. "live edge_ids at this generation").
    ``ids_path`` is how the reader addresses the id list from the row root;
    it defaults to ``<name>_ids``.
    """

    ids_field = ids_path or f"{name}_ids"
    if identity[f"{name}_ids_truncated"]:
        return _TRUNCATED_RECIPE.format(
            ids_field=ids_field,
            limit=SCOPE_ID_LIMIT,
            count=identity[f"{name}_count"],
            digest_field=f"{name}_digest_sha256",
        )
    return f"unevaluated = {universe} MINUS {ids_field}"


@dataclass(frozen=True)
class CandidateCoverageReport:
    """Auditable candidate-generation coverage for one check run."""

    universe_key: str
    coverage_basis: CoverageBasis
    coverage_complete: bool
    eligible_input_count: int
    candidate_count: int
    # Scope-restriction is the independent ground truth the write boundary
    # checks coverage_complete against: a run evaluated over a DERIVED subset
    # (incremental scope) is exact over that subset but NOT universe-complete,
    # so it may never claim coverage_complete. First-class (not buried in the
    # universe_key string) so consuming surfaces can audit scoped-ness.
    # ``scope_subject_count`` is how many subjects that scope covered; both
    # persist to first-class ``scoped`` / ``scope_subject_count`` columns
    # (task.scope.first-class-basis-and-health) that the gate algebra, doctor,
    # and operator read directly.
    scope_restricted: bool = False
    scope_subject_count: int = 0
    excluded_input_counts: Mapping[str, int] = field(default_factory=dict)
    missing_text_count: int = 0
    missing_embedding_count: int = 0
    candidate_count_by_block: Mapping[str, int] = field(default_factory=dict)
    block_count: int | None = None
    largest_block_size: int | None = None
    p95_block_size: Decimal | float | int | str | None = None
    all_pairs_upper_bound: int | None = None
    reduction_ratio: Decimal | float | int | str | None = None
    evaluator_invocation_count: int = 0
    evaluator_cost: Mapping[str, Any] = field(default_factory=dict)
    false_negative_audit: Mapping[str, Any] = field(default_factory=dict)
    payload: Mapping[str, Any] = field(default_factory=dict)


# Coverage bases the engine certifies as EXACT over what the run attempted (the
# whole universe when unscoped, the derived subset when scoped).
EXACT_COVERAGE_BASES = ("exact_postgres", "exact_snapshot")


def coverage_is_scoped_exact(report: CandidateCoverageReport) -> bool:
    """True iff this report is scoped-EXACT: exact over its scope, with no gaps.

    The single definition of the scoped-exact predicate. ``gates._coverage_is_exact``
    applies it to a persisted coverage ROW; a producer applies it to the report it
    is about to persist, so it can disclose ``scoped_exact`` on the gate row --
    the field a reader that never joins to ``consistency_candidate_coverage``
    (e.g. the differential-replay proof) needs in order to tell a scoped-exact
    run apart from a genuinely incomplete one.
    """

    return (
        bool(report.scope_restricted)
        and not bool(report.coverage_complete)
        and report.coverage_basis in EXACT_COVERAGE_BASES
        and int(report.missing_text_count or 0) == 0
        and int(report.missing_embedding_count or 0) == 0
    )


def canonical_participant_anchors(
    participant_roles: Mapping[str, str],
    *,
    symmetric_predicate: bool = False,
) -> list[dict[str, str]]:
    """Return canonical participant anchors for identity hashing."""

    if symmetric_predicate:
        anchors = sorted(str(anchor) for anchor in participant_roles.values())
        return [
            {"role": f"participant_{idx}", "anchor": anchor}
            for idx, anchor in enumerate(anchors)
        ]
    return [
        {"role": str(role), "anchor": str(participant_roles[role])}
        for role in sorted(participant_roles)
    ]


def candidate_id_for(
    definition: ConsistencyCheckDefinition,
    candidate: ConsistencyCandidate,
) -> str:
    """Derive the deterministic candidate id."""

    digest = stable_hash(
        {
            "kind": "consistency_candidate_v1",
            "check_id": definition.check_id,
            "candidate_rule_hash": definition.candidate_rule_hash,
            "evaluator_hash": definition.evaluator_hash,
            "input_scope": candidate.input_scope,
            "blocking_rule_hash": candidate.blocking_rule_hash,
            "participant_anchors": canonical_participant_anchors(
                candidate.participant_roles,
                symmetric_predicate=candidate.symmetric_predicate,
            ),
            "predicate_key": candidate.predicate_key,
            "generation_policy": candidate.generation_policy,
        }
    )
    return f"candidate:{digest}"


def observation_id_for(
    definition: ConsistencyCheckDefinition,
    *,
    candidate_id: str,
    input_scope: str,
    participant_roles: Mapping[str, str],
    normalized_predicate: str,
    normalized_values: Mapping[str, Any],
    source_revision_policy: str,
    symmetric_predicate: bool = False,
) -> str:
    """Derive the deterministic observation id."""

    digest = stable_hash(
        {
            "kind": "consistency_observation_v1",
            "check_id": definition.check_id,
            "candidate_rule_hash": definition.candidate_rule_hash,
            "evaluator_hash": definition.evaluator_hash,
            "candidate_id": candidate_id,
            "input_scope": input_scope,
            "participant_anchors": canonical_participant_anchors(
                participant_roles,
                symmetric_predicate=symmetric_predicate,
            ),
            "normalized_predicate": normalized_predicate,
            "normalized_values": normalized_values,
            "source_revision_policy": source_revision_policy,
        }
    )
    return f"observation:{digest}"


def register_consistency_check(
    conn: psycopg.Connection[Any],
    definition: ConsistencyCheckDefinition,
) -> None:
    """Upsert consistency-check and shared derived-artifact metadata."""

    with conn.transaction():
        with conn.cursor() as cur:
            _upsert_consistency_check(cur, definition)


def start_consistency_run(
    conn: psycopg.Connection[Any],
    definition: ConsistencyCheckDefinition,
    *,
    target_generation_id: int,
    trigger_kind: str = "manual",
    metrics: Mapping[str, Any] | None = None,
    dependency_manifest_ids: Sequence[UUID] = (),
) -> UUID:
    """Create a running consistency-check run."""

    with conn.transaction():
        with conn.cursor() as cur:
            _upsert_consistency_check(cur, definition)
            cur.execute(
                """
                INSERT INTO okg.derived_artifact_runs (
                  definition_id, target_generation_id, status,
                  trigger_kind, compute_hash, output_schema_hash,
                  renderer_hash, dirty_rule_hash,
                  dependency_manifest_ids, metrics
                ) VALUES (
                  %(definition_id)s, %(target_generation_id)s, 'running',
                  %(trigger_kind)s, %(compute_hash)s,
                  %(output_schema_hash)s, %(renderer_hash)s,
                  %(dirty_rule_hash)s, %(dependency_manifest_ids)s,
                  %(metrics)s
                )
                RETURNING run_id
                """,
                {
                    "definition_id": definition.check_id,
                    "target_generation_id": target_generation_id,
                    "trigger_kind": trigger_kind,
                    "compute_hash": definition.compute_hash,
                    "output_schema_hash": definition.output_schema_hash,
                    "renderer_hash": definition.renderer_hash,
                    "dirty_rule_hash": definition.dirty_rule_hash,
                    "dependency_manifest_ids": list(dependency_manifest_ids),
                    "metrics": Jsonb(dict(metrics or {})),
                },
            )
            (run_id,) = cur.fetchone()
            return run_id


# derived_artifact_runs.status ∈ {running, completed, failed,
# carried_forward, blocked}; only 'running' is non-terminal (in-flight). A run
# that finished — success, failure, or block — has left 'running'.
_NON_TERMINAL_RUN_STATUS = "running"


def summarize_consistency_run_multiplicity(
    conn: psycopg.Connection[Any],
    *,
    definition_id: str,
    target_generation_id: int,
) -> dict[str, Any]:
    """Operator-visible run count for one (check, target generation).

    The consistency-rerun multiplicity contract is *documented multiplicity*,
    not uniqueness: each rerun invocation creates exactly one new
    ``derived_artifact_runs`` row per check, so N reruns leave N runs — every
    one of which MUST reach a terminal status. History is preserved (no
    upsert / uniqueness collapse), so both/all runs' coverage and gate rows
    survive. This count is what makes that multiplicity *bounded and
    auditable*: ``runs_non_terminal`` is the orphan gauge — runs still
    ``'running'``. A normal run drives itself to a terminal status, so a
    non-zero ``runs_non_terminal`` means a crashed, killed, or still-in-flight
    run, and it is detectable here rather than accumulating silently.

    Returns ``runs_total`` / ``runs_terminal`` / ``runs_non_terminal`` plus a
    per-status ``by_status`` breakdown. The COUNT is deliberately not wrapped
    in a swallow-the-error guard: a failing count query surfaces to the caller
    (a default-value-as-fallback would hide a real DB fault).
    """

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, count(*)
              FROM okg.derived_artifact_runs
             WHERE definition_id = %(definition_id)s
               AND target_generation_id = %(target_generation_id)s
             GROUP BY status
            """,
            {
                "definition_id": definition_id,
                "target_generation_id": int(target_generation_id),
            },
        )
        by_status = {str(row[0]): int(row[1]) for row in cur.fetchall()}

    non_terminal = int(by_status.get(_NON_TERMINAL_RUN_STATUS, 0))
    total = sum(by_status.values())
    return {
        "definition_id": definition_id,
        "target_generation_id": int(target_generation_id),
        "runs_total": total,
        "runs_terminal": total - non_terminal,
        "runs_non_terminal": non_terminal,
        "by_status": dict(sorted(by_status.items())),
    }


def summarize_consistency_scope_decisions(
    conn: psycopg.Connection[Any],
    *,
    deployment_name: str,
    check_id: str | None = None,
    lookback: int = 20,
) -> dict[str, Any]:
    """Per-check scope-decision report — the locality observability surface.

    For every IR check run the engine records a scope decision in
    ``okg.consistency_candidate_coverage``: the ``scoped`` first-class column
    (scope-restricted incremental subset vs. a full universe sweep) plus, in the
    ``payload`` JSONB, ``scope_mode`` (``subject_local`` /
    ``node_with_incident_edges`` / ``full``), ``scope_plan_reason`` (WHY the
    :mod:`.ir_locality` analyzer chose that mode — e.g.
    ``aggregation_over_other_rows`` for a GROUP program the analyzer refuses to
    scope) and ``scope_reason`` (the runtime cause — ``explicit_scope_subject_ids``,
    ``dirty_scope_unavailable``, ``antientropy_cadence_sweep``, ...).

    This reads that decision back, aggregated PER CHECK over the most recent
    ``lookback`` runs, so "which checks never run incrementally and why" is a
    measurement rather than an archaeology exercise. It reads only — it does
    NOT change any analyzer fallback decision.

    Scoping: joins ``okg.consistency_check_definitions`` so the report covers
    exactly the checks registered under ``deployment_name`` (optionally a single
    ``check_id``). Recency is the coverage row's own ``created_at`` — the moment
    the scope decision was persisted, written 1:1 with the run (``run_id`` is the
    coverage PK). ``okg.derived_artifact_runs`` carries ``started_at`` /
    ``completed_at`` but no ``created_at`` column, so the coverage timestamp is
    the authoritative scope-decision recency signal and no run-table join is
    needed to obtain it.

    Returns ``{deployment_name, check_id, lookback, checks}`` where ``checks``
    maps ``check_id`` -> a report with:

    * ``total_runs`` — runs seen within the lookback window;
    * ``scoped_run_count`` / ``full_run_count``;
    * ``by_mode`` — the ``{scope_mode: count}`` distribution;
    * ``by_full_reason`` — the ``{scope_plan_reason: count}`` distribution over
      the FULL runs only ("why it went full"); for an analyzer-forced-full check
      this is the true full-cause, while a scopeable check that ran full for a
      runtime reason carries that reason per-run under ``recent[].scope_reason``;
    * ``recent`` — the last-N raw decisions (``created_at``, ``run_id``,
      ``target_generation_id``, ``scoped``, ``scope_mode``, ``scope_plan_reason``,
      ``scope_reason``) for drill-down.

    The query is deliberately NOT wrapped in a swallow-the-error guard: a failing
    read surfaces to the caller (a default-zero fallback would hide a real DB
    fault — the P0 silent-fallback class).
    """

    if lookback <= 0:
        raise ValueError("lookback must be positive")

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT check_id, run_id, target_generation_id, scoped,
                   created_at, scope_mode, scope_plan_reason, scope_reason
              FROM (
                SELECT
                    cov.check_id                       AS check_id,
                    cov.run_id                         AS run_id,
                    cov.target_generation_id           AS target_generation_id,
                    cov.scoped                          AS scoped,
                    cov.created_at                      AS created_at,
                    cov.payload->>'scope_mode'         AS scope_mode,
                    cov.payload->>'scope_plan_reason'  AS scope_plan_reason,
                    cov.payload->>'scope_reason'       AS scope_reason,
                    row_number() OVER (
                        PARTITION BY cov.check_id
                        ORDER BY cov.created_at DESC, cov.run_id DESC
                    )                                  AS rn
                  FROM okg.consistency_candidate_coverage cov
                  JOIN okg.consistency_check_definitions def
                    ON def.check_id = cov.check_id
                 WHERE def.deployment_name = %(deployment_name)s
                   AND (%(check_id)s::text IS NULL OR cov.check_id = %(check_id)s)
              ) ranked
             WHERE rn <= %(lookback)s
             ORDER BY check_id, created_at DESC, run_id DESC
            """,
            {
                "deployment_name": deployment_name,
                "check_id": check_id,
                "lookback": int(lookback),
            },
        )
        rows = cur.fetchall()

    checks: dict[str, dict[str, Any]] = {}
    for (
        cid,
        run_id,
        target_generation_id,
        scoped,
        created_at,
        scope_mode,
        scope_plan_reason,
        scope_reason,
    ) in rows:
        report = checks.setdefault(
            str(cid),
            {
                "check_id": str(cid),
                "total_runs": 0,
                "scoped_run_count": 0,
                "full_run_count": 0,
                "by_mode": {},
                "by_full_reason": {},
                "recent": [],
            },
        )
        is_scoped = bool(scoped)
        report["total_runs"] += 1
        if is_scoped:
            report["scoped_run_count"] += 1
        else:
            report["full_run_count"] += 1
        # `scope_mode` rides in the IR coverage payload; label a row that lacks
        # it (e.g. a non-IR SQL check sharing the deployment) from the
        # first-class `scoped` column rather than dropping it.
        mode_label = scope_mode if scope_mode is not None else (
            "scoped" if is_scoped else "full"
        )
        report["by_mode"][mode_label] = report["by_mode"].get(mode_label, 0) + 1
        if not is_scoped:
            reason_label = (
                scope_plan_reason if scope_plan_reason is not None else "unrecorded"
            )
            report["by_full_reason"][reason_label] = (
                report["by_full_reason"].get(reason_label, 0) + 1
            )
        report["recent"].append(
            {
                "created_at": created_at,
                "run_id": str(run_id),
                "target_generation_id": int(target_generation_id),
                "scoped": is_scoped,
                "scope_mode": scope_mode,
                "scope_plan_reason": scope_plan_reason,
                "scope_reason": scope_reason,
            }
        )

    for report in checks.values():
        report["by_mode"] = dict(sorted(report["by_mode"].items()))
        report["by_full_reason"] = dict(sorted(report["by_full_reason"].items()))

    return {
        "deployment_name": deployment_name,
        "check_id": check_id,
        "lookback": int(lookback),
        "checks": checks,
    }


def complete_consistency_run(
    conn: psycopg.Connection[Any],
    *,
    run_id: UUID,
    metrics: Mapping[str, Any] | None = None,
) -> None:
    """Mark a consistency-check run complete."""

    with conn.transaction():
        with conn.cursor() as cur:
            if metrics is None:
                cur.execute(
                    """
                    UPDATE okg.derived_artifact_runs
                       SET status = 'completed',
                           completed_at = now()
                     WHERE run_id = %(run_id)s
                    """,
                    {"run_id": run_id},
                )
            else:
                cur.execute(
                    """
                    UPDATE okg.derived_artifact_runs
                       SET status = 'completed',
                           metrics = %(metrics)s,
                           completed_at = now()
                     WHERE run_id = %(run_id)s
                    """,
                    {"run_id": run_id, "metrics": Jsonb(dict(metrics))},
                )


def record_candidate(
    conn: psycopg.Connection[Any],
    definition: ConsistencyCheckDefinition,
    *,
    target_generation_id: int,
    run_id: UUID | None,
    candidate: ConsistencyCandidate,
) -> str:
    """Persist a deterministic candidate record."""

    candidate_id = candidate_id_for(definition, candidate)
    participant_anchors = canonical_participant_anchors(
        candidate.participant_roles,
        symmetric_predicate=candidate.symmetric_predicate,
    )
    with conn.transaction():
        with conn.cursor() as cur:
            _upsert_consistency_check(cur, definition)
            if run_id is not None:
                _assert_run_matches(
                    cur,
                    run_id=run_id,
                    definition_id=definition.check_id,
                    target_generation_id=target_generation_id,
                )
            cur.execute(
                """
                INSERT INTO okg.consistency_candidates (
                  check_id, target_generation_id, candidate_id, run_id,
                  input_scope, blocking_rule_hash, participant_roles,
                  participant_anchors, predicate_key, generation_policy,
                  coverage_status, exclusion_reason, candidate_payload
                ) VALUES (
                  %(check_id)s, %(target_generation_id)s,
                  %(candidate_id)s, %(run_id)s, %(input_scope)s,
                  %(blocking_rule_hash)s, %(participant_roles)s,
                  %(participant_anchors)s, %(predicate_key)s,
                  %(generation_policy)s, %(coverage_status)s,
                  %(exclusion_reason)s, %(candidate_payload)s
                )
                ON CONFLICT (
                  check_id, target_generation_id, candidate_id
                ) DO UPDATE SET
                  run_id = EXCLUDED.run_id,
                  coverage_status = EXCLUDED.coverage_status,
                  exclusion_reason = EXCLUDED.exclusion_reason,
                  candidate_payload = EXCLUDED.candidate_payload
                """,
                {
                    "check_id": definition.check_id,
                    "target_generation_id": target_generation_id,
                    "candidate_id": candidate_id,
                    "run_id": run_id,
                    "input_scope": candidate.input_scope,
                    "blocking_rule_hash": candidate.blocking_rule_hash,
                    "participant_roles": Jsonb(dict(candidate.participant_roles)),
                    "participant_anchors": Jsonb(participant_anchors),
                    "predicate_key": candidate.predicate_key,
                    "generation_policy": candidate.generation_policy,
                    "coverage_status": candidate.coverage_status,
                    "exclusion_reason": candidate.exclusion_reason,
                    "candidate_payload": Jsonb(dict(candidate.candidate_payload)),
                },
            )
    return candidate_id


def record_candidate_observations_bulk(
    conn: psycopg.Connection[Any],
    definition: ConsistencyCheckDefinition,
    *,
    target_generation_id: int,
    run_id: UUID,
    records: Sequence[ConsistencyCandidateObservation],
) -> int:
    """Persist deterministic candidates and observation events in bulk.

    This preserves ``record_candidate`` + ``append_observation_event`` semantics
    while avoiding per-row transactions and per-row current-state lookups.
    """

    if not records:
        return 0

    prepared_candidates: list[dict[str, Any]] = []
    events: list[ObservationEvent] = []
    resolutions: list[ConsistencyCandidateObservation] = []
    for idx, record in enumerate(records):
        resolving_existing = (
            record.event is not None
            and record.event.finding_event == "resolved"
        )
        if resolving_existing:
            resolutions.append(record)
        else:
            expected_candidate_id = candidate_id_for(
                definition,
                record.candidate,
            )
            if record.candidate_id != expected_candidate_id:
                raise ValueError(
                    "bulk candidate_id does not match deterministic id "
                    f"at row {idx}"
                )
            prepared_candidates.append(
                _bulk_candidate_row(
                    definition,
                    target_generation_id=target_generation_id,
                    run_id=run_id,
                    stage_order=idx,
                    candidate=record.candidate,
                    candidate_id=record.candidate_id,
                )
            )
        if record.event is not None:
            _validate_event(record.event)
            if record.event.candidate_id != record.candidate_id:
                raise ValueError(
                    "bulk event candidate_id does not match candidate "
                    f"at row {idx}"
                )
            events.append(record.event)

    with conn.transaction():
        with conn.cursor() as cur:
            _upsert_consistency_check(cur, definition)
            _assert_run_matches(
                cur,
                run_id=run_id,
                definition_id=definition.check_id,
                target_generation_id=target_generation_id,
            )
            _copy_bulk_candidates(cur, prepared_candidates)
            cur.execute(
                """
                INSERT INTO okg.consistency_candidates (
                  check_id, target_generation_id, candidate_id, run_id,
                  input_scope, blocking_rule_hash, participant_roles,
                  participant_anchors, predicate_key, generation_policy,
                  coverage_status, exclusion_reason, candidate_payload
                )
                SELECT DISTINCT ON (candidate_id)
                  check_id, target_generation_id, candidate_id, run_id,
                  input_scope, blocking_rule_hash, participant_roles,
                  participant_anchors, predicate_key, generation_policy,
                  coverage_status, exclusion_reason, candidate_payload
                  FROM pg_temp.okg_bulk_consistency_candidates
                 ORDER BY candidate_id, stage_order DESC
                ON CONFLICT (
                  check_id, target_generation_id, candidate_id
                ) DO UPDATE SET
                  run_id = EXCLUDED.run_id,
                  coverage_status = EXCLUDED.coverage_status,
                  exclusion_reason = EXCLUDED.exclusion_reason,
                  candidate_payload = EXCLUDED.candidate_payload
                """,
            )

            if not events:
                return 0

            current_by_id = _fetch_current_observations(
                cur,
                [event.observation_id for event in events],
            )
            historical_candidates = _fetch_historical_candidate_identities(
                cur,
                check_id=definition.check_id,
                candidate_ids=[record.candidate_id for record in resolutions],
            )
            for record in resolutions:
                assert record.event is not None
                _validate_existing_resolution(
                    definition,
                    record,
                    current_by_id.get(record.event.observation_id),
                    historical_candidates.get(record.candidate_id, []),
                )
            cur.execute(
                """
                SELECT COALESCE(max(event_seq), 0)
                  FROM okg.observation_events
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": run_id},
            )
            (base_event_seq,) = cur.fetchone()
            event_rows: list[dict[str, Any]] = []
            for idx, event in enumerate(events):
                event_row = _bulk_event_row(
                    definition,
                    target_generation_id=target_generation_id,
                    run_id=run_id,
                    stage_order=idx,
                    event_seq=int(base_event_seq) + idx + 1,
                    event=event,
                    current=current_by_id.get(event.observation_id),
                )
                event_rows.append(event_row)
                current_by_id[event.observation_id] = {
                    "finding_status": event_row["current_finding_status"],
                    "evaluation_status": (
                        event_row["current_evaluation_status"]
                    ),
                    "severity": event_row["severity"],
                    "confidence": event_row["confidence"],
                    "confidence_type": event_row["confidence_type"],
                    "evidence": event_row["current_evidence"],
                    "payload": event_row["current_payload"],
                }
            _copy_bulk_events(cur, event_rows)
            cur.execute(
                """
                WITH inserted AS (
                  INSERT INTO okg.observation_events (
                    observation_id, check_id, candidate_id,
                    target_generation_id, run_id, event_seq,
                    finding_event, evaluation_event, input_scope,
                    predicate_key, normalized_predicate, normalized_values,
                    participant_roles, participant_anchors, severity,
                    confidence, confidence_type, evidence, payload,
                    coverage_complete
                  )
                  SELECT
                    s.observation_id, s.check_id, s.candidate_id,
                    s.target_generation_id, s.run_id, s.event_seq,
                    s.finding_event, s.evaluation_event, s.input_scope,
                    s.predicate_key, s.normalized_predicate,
                    s.normalized_values, s.participant_roles,
                    s.participant_anchors, s.severity, s.confidence,
                    s.confidence_type, s.event_evidence, s.event_payload,
                    s.coverage_complete
                    FROM pg_temp.okg_bulk_observation_events s
                   WHERE NOT (
                     s.finding_event = 'observed'
                     AND s.evaluation_event IS NULL
                     AND s.current_finding_status = 'open'
                     AND EXISTS (
                       SELECT 1 FROM okg.observation_current oc
                        WHERE oc.observation_id = s.observation_id
                          AND oc.finding_status = 'open'
                          AND oc.evaluation_status
                              IS NOT DISTINCT FROM s.current_evaluation_status
                          AND oc.severity
                              IS NOT DISTINCT FROM s.severity
                          AND oc.confidence
                              IS NOT DISTINCT FROM s.confidence
                          AND oc.confidence_type
                              IS NOT DISTINCT FROM s.confidence_type
                          AND oc.evidence = s.current_evidence
                          AND oc.payload  = s.current_payload
                     )
                   )
                   ORDER BY s.event_seq
                  RETURNING event_seq, event_id
                ),
                latest AS (
                  SELECT DISTINCT ON (event_rows.observation_id)
                    event_rows.*,
                    inserted.event_id
                    FROM pg_temp.okg_bulk_observation_events event_rows
                    JOIN inserted USING (event_seq)
                   ORDER BY event_rows.observation_id,
                            event_rows.event_seq DESC
                )
                INSERT INTO okg.observation_current (
                  observation_id, check_id, candidate_id,
                  latest_generation_id, latest_run_id, latest_event_id,
                  finding_status, evaluation_status, input_scope,
                  predicate_key, normalized_predicate, normalized_values,
                  participant_roles, participant_anchors, severity,
                  confidence, confidence_type, evidence, payload,
                  updated_at
                )
                SELECT
                  observation_id, check_id, candidate_id,
                  target_generation_id, run_id, event_id,
                  current_finding_status, current_evaluation_status,
                  input_scope, predicate_key, normalized_predicate,
                  normalized_values, participant_roles, participant_anchors,
                  severity, confidence, confidence_type, current_evidence,
                  current_payload, now()
                  FROM latest
                ON CONFLICT (observation_id) DO UPDATE SET
                  check_id = EXCLUDED.check_id,
                  candidate_id = EXCLUDED.candidate_id,
                  latest_generation_id = EXCLUDED.latest_generation_id,
                  latest_run_id = EXCLUDED.latest_run_id,
                  latest_event_id = EXCLUDED.latest_event_id,
                  finding_status = EXCLUDED.finding_status,
                  evaluation_status = EXCLUDED.evaluation_status,
                  input_scope = EXCLUDED.input_scope,
                  predicate_key = EXCLUDED.predicate_key,
                  normalized_predicate = EXCLUDED.normalized_predicate,
                  normalized_values = EXCLUDED.normalized_values,
                  participant_roles = EXCLUDED.participant_roles,
                  participant_anchors = EXCLUDED.participant_anchors,
                  severity = EXCLUDED.severity,
                  confidence = EXCLUDED.confidence,
                  confidence_type = EXCLUDED.confidence_type,
                  evidence = EXCLUDED.evidence,
                  payload = EXCLUDED.payload,
                  updated_at = now()
                """,
            )
            _dedupe_observation_current_for_staged_events(
                cur,
                "okg_bulk_observation_events",
            )
            return len(events)


def _dedupe_observation_current_for_staged_events(
    cur: psycopg.Cursor[Any],
    stage_table: _ObservationCurrentStage,
) -> int:
    """Keep one current row for each logical finding staged by a run.

    Candidate and observation identifiers are intentionally versioned by check
    definition identity. The current-state projection is a review surface, so
    it collapses those versions by the logical finding key while preserving the
    append-only observation_events history.
    """

    if stage_table not in {
        "okg_bulk_observation_events",
        "okg_sql_native_observation_events",
        "okg_observation_current_dedupe_keys",
    }:
        raise ValueError(f"unexpected observation dedupe stage: {stage_table}")
    cur.execute(
        f"""
        WITH staged_keys AS (
          SELECT DISTINCT
            check_id,
            input_scope,
            predicate_key,
            normalized_predicate,
            normalized_values,
            participant_anchors
            FROM pg_temp.{stage_table}
        ),
        ranked_current AS (
          SELECT
            oc.observation_id,
            row_number() OVER (
              PARTITION BY
                oc.check_id,
                oc.input_scope,
                oc.predicate_key,
                oc.normalized_predicate,
                oc.normalized_values,
                oc.participant_anchors
              ORDER BY
                oc.latest_generation_id DESC,
                oc.updated_at DESC,
                oc.latest_event_id DESC,
                oc.latest_run_id::text DESC,
                oc.observation_id DESC
            ) AS logical_rank
            FROM okg.observation_current oc
            JOIN staged_keys staged
              ON oc.check_id = staged.check_id
             AND oc.input_scope = staged.input_scope
             AND oc.predicate_key = staged.predicate_key
             AND oc.normalized_predicate = staged.normalized_predicate
             AND oc.normalized_values = staged.normalized_values
             AND oc.participant_anchors = staged.participant_anchors
        )
        DELETE FROM okg.observation_current oc
              USING ranked_current ranked
         WHERE oc.observation_id = ranked.observation_id
           AND ranked.logical_rank > 1
        """,
    )
    return cur.rowcount


def _bulk_candidate_row(
    definition: ConsistencyCheckDefinition,
    *,
    target_generation_id: int,
    run_id: UUID,
    stage_order: int,
    candidate: ConsistencyCandidate,
    candidate_id: str,
) -> dict[str, Any]:
    return {
        "stage_order": stage_order,
        "check_id": definition.check_id,
        "target_generation_id": target_generation_id,
        "candidate_id": candidate_id,
        "run_id": run_id,
        "input_scope": candidate.input_scope,
        "blocking_rule_hash": candidate.blocking_rule_hash,
        "participant_roles": dict(candidate.participant_roles),
        "participant_anchors": canonical_participant_anchors(
            candidate.participant_roles,
            symmetric_predicate=candidate.symmetric_predicate,
        ),
        "predicate_key": candidate.predicate_key,
        "generation_policy": candidate.generation_policy,
        "coverage_status": candidate.coverage_status,
        "exclusion_reason": candidate.exclusion_reason,
        "candidate_payload": dict(candidate.candidate_payload),
    }


def _fetch_current_observations(
    cur: psycopg.Cursor[Any],
    observation_ids: Sequence[str],
) -> dict[str, dict[str, Any]]:
    unique_ids = list(dict.fromkeys(observation_ids))
    if not unique_ids:
        return {}
    cur.execute(
        """
        SELECT observation_id, check_id, candidate_id, input_scope,
               predicate_key, normalized_predicate, normalized_values,
               participant_roles, participant_anchors, finding_status,
               evaluation_status, severity, confidence, confidence_type,
               evidence, payload
          FROM okg.observation_current
         WHERE observation_id = ANY(%(observation_ids)s)
        """,
        {"observation_ids": unique_ids},
    )
    return {
        row[0]: {
            "check_id": row[1],
            "candidate_id": row[2],
            "input_scope": row[3],
            "predicate_key": row[4],
            "normalized_predicate": row[5],
            "normalized_values": row[6] or {},
            "participant_roles": row[7] or {},
            "participant_anchors": row[8] or [],
            "finding_status": row[9],
            "evaluation_status": row[10],
            "severity": row[11],
            "confidence": row[12],
            "confidence_type": row[13],
            "evidence": row[14] or {},
            "payload": row[15] or {},
        }
        for row in cur.fetchall()
    }


def _fetch_historical_candidate_identities(
    cur: psycopg.Cursor[Any],
    *,
    check_id: str,
    candidate_ids: Sequence[str],
) -> dict[str, list[dict[str, Any]]]:
    """Return every persisted identity row for the requested candidates.

    One deterministic candidate may legitimately recur in several generations.
    Callers must compare all rows rather than selecting whichever generation is
    newest, because a contradictory row is evidence corruption, not a fallback
    choice.
    """

    unique_ids = list(dict.fromkeys(candidate_ids))
    if not unique_ids:
        return {}
    cur.execute(
        """
        SELECT candidate_id, input_scope, blocking_rule_hash,
               participant_roles, participant_anchors, predicate_key,
               generation_policy
          FROM okg.consistency_candidates
         WHERE check_id = %(check_id)s
           AND candidate_id = ANY(%(candidate_ids)s)
         ORDER BY candidate_id, target_generation_id, created_at
        """,
        {"check_id": check_id, "candidate_ids": unique_ids},
    )
    identities: dict[str, list[dict[str, Any]]] = {}
    for row in cur.fetchall():
        identities.setdefault(str(row[0]), []).append(
            {
                "input_scope": row[1],
                "blocking_rule_hash": row[2],
                "participant_roles": row[3] or {},
                "participant_anchors": row[4] or [],
                "predicate_key": row[5],
                "generation_policy": row[6],
            }
        )
    return identities


def _validate_existing_resolution(
    definition: ConsistencyCheckDefinition,
    record: ConsistencyCandidateObservation,
    current: Mapping[str, Any] | None,
    historical_candidates: Sequence[Mapping[str, Any]],
) -> None:
    """Validate a resolution against persisted candidate provenance.

    Candidate identifiers include check-definition hashes. A definition
    revision therefore cannot re-derive an older open candidate's identifier,
    but it must still be able to resolve that finding. Resolution records do
    not mint a candidate for the new generation; they append to the existing
    observation only after every still-durable identity field agrees across
    the candidate history, the current open observation, and the event.
    """
    event = record.event
    assert event is not None
    if current is None:
        raise HistoricalCandidateIdentityUnverifiable(
            "historical candidate identity unverifiable: resolved event "
            "requires an existing current observation"
        )
    if current.get("finding_status") != "open":
        raise HistoricalCandidateIdentityUnverifiable(
            "historical candidate identity unverifiable: current observation "
            f"{event.observation_id!r} is not open"
        )
    expected = {
        "check_id": definition.check_id,
        "candidate_id": event.candidate_id,
        "input_scope": event.input_scope,
        "predicate_key": event.predicate_key,
        "normalized_predicate": event.normalized_predicate,
        "normalized_values": dict(event.normalized_values),
        "participant_roles": dict(event.participant_roles),
        "participant_anchors": canonical_participant_anchors(
            event.participant_roles,
            symmetric_predicate=event.symmetric_predicate,
        ),
    }
    mismatched = [
        field for field, value in expected.items() if current.get(field) != value
    ]
    if mismatched:
        raise HistoricalCandidateIdentityUnverifiable(
            "historical candidate identity unverifiable: resolved event does "
            "not match current observation identity: " + ", ".join(sorted(mismatched))
        )

    if not historical_candidates:
        raise HistoricalCandidateIdentityUnverifiable(
            "historical candidate identity unverifiable: no persisted "
            f"candidate row for check {definition.check_id!r}, "
            f"candidate {record.candidate_id!r}"
        )
    historical_fingerprints = {
        stable_hash(dict(identity)) for identity in historical_candidates
    }
    if len(historical_fingerprints) != 1:
        raise HistoricalCandidateIdentityUnverifiable(
            "historical candidate identity unverifiable: persisted candidate "
            f"rows contradict for check {definition.check_id!r}, "
            f"candidate {record.candidate_id!r}"
        )

    supplied_candidate = {
        "input_scope": record.candidate.input_scope,
        "blocking_rule_hash": record.candidate.blocking_rule_hash,
        "participant_roles": dict(record.candidate.participant_roles),
        "participant_anchors": canonical_participant_anchors(
            record.candidate.participant_roles,
            symmetric_predicate=record.candidate.symmetric_predicate,
        ),
        "predicate_key": record.candidate.predicate_key,
        "generation_policy": record.candidate.generation_policy,
    }
    persisted_candidate = dict(historical_candidates[0])
    candidate_mismatches = [
        field
        for field, value in supplied_candidate.items()
        if persisted_candidate.get(field) != value
    ]
    if candidate_mismatches:
        raise HistoricalCandidateIdentityUnverifiable(
            "historical candidate identity unverifiable: supplied candidate "
            "does not match persisted identity: "
            + ", ".join(sorted(candidate_mismatches))
        )


def _bulk_event_row(
    definition: ConsistencyCheckDefinition,
    *,
    target_generation_id: int,
    run_id: UUID,
    stage_order: int,
    event_seq: int,
    event: ObservationEvent,
    current: Mapping[str, Any] | None,
) -> dict[str, Any]:
    old_finding = (
        current.get("finding_status") if current is not None else "absent"
    )
    old_evaluation = (
        current.get("evaluation_status")
        if current is not None
        else "not_evaluated"
    )
    old_severity = current.get("severity") if current is not None else None
    old_confidence = (
        current.get("confidence") if current is not None else None
    )
    old_confidence_type = (
        current.get("confidence_type")
        if current is not None
        else definition.confidence_type
    )
    old_evidence = current.get("evidence") if current is not None else {}
    old_payload = current.get("payload") if current is not None else {}

    new_finding = _next_finding_status(
        old_finding,
        event.finding_event,
        event.coverage_complete,
    )
    new_evaluation = event.evaluation_event or old_evaluation
    severity = event.severity if event.severity is not None else old_severity
    confidence = (
        _decimal_or_none(event.confidence)
        if event.confidence is not None
        else old_confidence
    )
    confidence_type = event.confidence_type or old_confidence_type
    current_evidence = (
        dict(event.evidence) if event.evidence else dict(old_evidence)
    )
    current_payload = (
        dict(event.payload) if event.payload else dict(old_payload)
    )
    participant_roles = dict(event.participant_roles)
    participant_anchors = canonical_participant_anchors(
        participant_roles,
        symmetric_predicate=event.symmetric_predicate,
    )
    return {
        "stage_order": stage_order,
        "observation_id": event.observation_id,
        "check_id": definition.check_id,
        "candidate_id": event.candidate_id,
        "target_generation_id": target_generation_id,
        "run_id": run_id,
        "event_seq": event_seq,
        "finding_event": event.finding_event,
        "evaluation_event": event.evaluation_event,
        "input_scope": event.input_scope,
        "predicate_key": event.predicate_key,
        "normalized_predicate": event.normalized_predicate,
        "normalized_values": dict(event.normalized_values),
        "participant_roles": participant_roles,
        "participant_anchors": participant_anchors,
        "severity": severity,
        "confidence": confidence,
        "confidence_type": confidence_type,
        "event_evidence": dict(event.evidence),
        "event_payload": dict(event.payload),
        "coverage_complete": event.coverage_complete,
        "current_finding_status": new_finding,
        "current_evaluation_status": new_evaluation,
        "current_evidence": current_evidence,
        "current_payload": current_payload,
    }


def _copy_bulk_candidates(
    cur: psycopg.Cursor[Any],
    rows: Sequence[Mapping[str, Any]],
) -> None:
    cur.execute("DROP TABLE IF EXISTS pg_temp.okg_bulk_consistency_candidates")
    cur.execute(
        """
        CREATE TEMP TABLE okg_bulk_consistency_candidates (
          stage_order int NOT NULL,
          check_id text NOT NULL,
          target_generation_id bigint NOT NULL,
          candidate_id text NOT NULL,
          run_id uuid,
          input_scope text NOT NULL,
          blocking_rule_hash text NOT NULL,
          participant_roles jsonb NOT NULL,
          participant_anchors jsonb NOT NULL,
          predicate_key text NOT NULL,
          generation_policy text NOT NULL,
          coverage_status text NOT NULL,
          exclusion_reason text,
          candidate_payload jsonb NOT NULL
        ) ON COMMIT DROP
        """,
    )
    with cur.copy(
        """
        COPY okg_bulk_consistency_candidates (
          stage_order, check_id, target_generation_id, candidate_id, run_id,
          input_scope, blocking_rule_hash, participant_roles,
          participant_anchors, predicate_key, generation_policy,
          coverage_status, exclusion_reason, candidate_payload
        ) FROM STDIN
        """
    ) as copy:
        for row in rows:
            copy.write_row((
                row["stage_order"],
                row["check_id"],
                row["target_generation_id"],
                row["candidate_id"],
                row["run_id"],
                row["input_scope"],
                row["blocking_rule_hash"],
                Jsonb(row["participant_roles"]),
                Jsonb(row["participant_anchors"]),
                row["predicate_key"],
                row["generation_policy"],
                row["coverage_status"],
                row["exclusion_reason"],
                Jsonb(row["candidate_payload"]),
            ))


def _copy_bulk_events(
    cur: psycopg.Cursor[Any],
    rows: Sequence[Mapping[str, Any]],
) -> None:
    cur.execute("DROP TABLE IF EXISTS pg_temp.okg_bulk_observation_events")
    cur.execute(
        """
        CREATE TEMP TABLE okg_bulk_observation_events (
          stage_order int NOT NULL,
          observation_id text NOT NULL,
          check_id text NOT NULL,
          candidate_id text NOT NULL,
          target_generation_id bigint NOT NULL,
          run_id uuid NOT NULL,
          event_seq int NOT NULL,
          finding_event text,
          evaluation_event text,
          input_scope text NOT NULL,
          predicate_key text NOT NULL,
          normalized_predicate text NOT NULL,
          normalized_values jsonb NOT NULL,
          participant_roles jsonb NOT NULL,
          participant_anchors jsonb NOT NULL,
          severity text,
          confidence numeric(8, 6),
          confidence_type text,
          event_evidence jsonb NOT NULL,
          event_payload jsonb NOT NULL,
          coverage_complete boolean NOT NULL,
          current_finding_status text NOT NULL,
          current_evaluation_status text NOT NULL,
          current_evidence jsonb NOT NULL,
          current_payload jsonb NOT NULL
        ) ON COMMIT DROP
        """,
    )
    with cur.copy(
        """
        COPY okg_bulk_observation_events (
          stage_order, observation_id, check_id, candidate_id,
          target_generation_id, run_id, event_seq, finding_event,
          evaluation_event, input_scope, predicate_key,
          normalized_predicate, normalized_values, participant_roles,
          participant_anchors, severity, confidence, confidence_type,
          event_evidence, event_payload, coverage_complete,
          current_finding_status, current_evaluation_status,
          current_evidence, current_payload
        ) FROM STDIN
        """
    ) as copy:
        for row in rows:
            copy.write_row((
                row["stage_order"],
                row["observation_id"],
                row["check_id"],
                row["candidate_id"],
                row["target_generation_id"],
                row["run_id"],
                row["event_seq"],
                row["finding_event"],
                row["evaluation_event"],
                row["input_scope"],
                row["predicate_key"],
                row["normalized_predicate"],
                Jsonb(row["normalized_values"]),
                Jsonb(row["participant_roles"]),
                Jsonb(row["participant_anchors"]),
                row["severity"],
                row["confidence"],
                row["confidence_type"],
                Jsonb(row["event_evidence"]),
                Jsonb(row["event_payload"]),
                row["coverage_complete"],
                row["current_finding_status"],
                row["current_evaluation_status"],
                Jsonb(row["current_evidence"]),
                Jsonb(row["current_payload"]),
            ))


def record_candidate_coverage(
    conn: psycopg.Connection[Any],
    definition: ConsistencyCheckDefinition,
    *,
    target_generation_id: int,
    run_id: UUID,
    report: CandidateCoverageReport,
) -> None:
    """Persist auditable coverage metadata for a candidate run."""

    normalized = _normalize_coverage_report(report)
    with conn.transaction():
        with conn.cursor() as cur:
            _upsert_consistency_check(cur, definition)
            _assert_run_matches(
                cur,
                run_id=run_id,
                definition_id=definition.check_id,
                target_generation_id=target_generation_id,
            )
            cur.execute(
                """
                INSERT INTO okg.consistency_candidate_coverage (
                  run_id, check_id, target_generation_id, universe_key,
                  coverage_basis, coverage_complete,
                  scoped, scope_subject_count,
                  eligible_input_count, excluded_input_counts,
                  missing_text_count, missing_embedding_count,
                  block_count, candidate_count, candidate_count_by_block,
                  largest_block_size, p95_block_size,
                  all_pairs_upper_bound, reduction_ratio,
                  evaluator_invocation_count, evaluator_cost,
                  false_negative_audit, payload
                ) VALUES (
                  %(run_id)s, %(check_id)s, %(target_generation_id)s,
                  %(universe_key)s, %(coverage_basis)s,
                  %(coverage_complete)s,
                  %(scoped)s, %(scope_subject_count)s,
                  %(eligible_input_count)s,
                  %(excluded_input_counts)s, %(missing_text_count)s,
                  %(missing_embedding_count)s, %(block_count)s,
                  %(candidate_count)s, %(candidate_count_by_block)s,
                  %(largest_block_size)s, %(p95_block_size)s,
                  %(all_pairs_upper_bound)s, %(reduction_ratio)s,
                  %(evaluator_invocation_count)s, %(evaluator_cost)s,
                  %(false_negative_audit)s, %(payload)s
                )
                ON CONFLICT (run_id) DO UPDATE SET
                  check_id = EXCLUDED.check_id,
                  target_generation_id = EXCLUDED.target_generation_id,
                  universe_key = EXCLUDED.universe_key,
                  coverage_basis = EXCLUDED.coverage_basis,
                  coverage_complete = EXCLUDED.coverage_complete,
                  scoped = EXCLUDED.scoped,
                  scope_subject_count = EXCLUDED.scope_subject_count,
                  eligible_input_count = EXCLUDED.eligible_input_count,
                  excluded_input_counts = EXCLUDED.excluded_input_counts,
                  missing_text_count = EXCLUDED.missing_text_count,
                  missing_embedding_count = EXCLUDED.missing_embedding_count,
                  block_count = EXCLUDED.block_count,
                  candidate_count = EXCLUDED.candidate_count,
                  candidate_count_by_block = EXCLUDED.candidate_count_by_block,
                  largest_block_size = EXCLUDED.largest_block_size,
                  p95_block_size = EXCLUDED.p95_block_size,
                  all_pairs_upper_bound = EXCLUDED.all_pairs_upper_bound,
                  reduction_ratio = EXCLUDED.reduction_ratio,
                  evaluator_invocation_count =
                    EXCLUDED.evaluator_invocation_count,
                  evaluator_cost = EXCLUDED.evaluator_cost,
                  false_negative_audit = EXCLUDED.false_negative_audit,
                  payload = EXCLUDED.payload,
                  updated_at = now()
                """,
                {
                    "run_id": run_id,
                    "check_id": definition.check_id,
                    "target_generation_id": target_generation_id,
                    "universe_key": normalized["universe_key"],
                    "coverage_basis": normalized["coverage_basis"],
                    "coverage_complete": normalized["coverage_complete"],
                    "scoped": normalized["scope_restricted"],
                    "scope_subject_count": normalized["scope_subject_count"],
                    "eligible_input_count": normalized["eligible_input_count"],
                    "excluded_input_counts": Jsonb(
                        normalized["excluded_input_counts"]
                    ),
                    "missing_text_count": normalized["missing_text_count"],
                    "missing_embedding_count": normalized[
                        "missing_embedding_count"
                    ],
                    "block_count": normalized["block_count"],
                    "candidate_count": normalized["candidate_count"],
                    "candidate_count_by_block": Jsonb(
                        normalized["candidate_count_by_block"]
                    ),
                    "largest_block_size": normalized["largest_block_size"],
                    "p95_block_size": normalized["p95_block_size"],
                    "all_pairs_upper_bound": normalized[
                        "all_pairs_upper_bound"
                    ],
                    "reduction_ratio": normalized["reduction_ratio"],
                    "evaluator_invocation_count": normalized[
                        "evaluator_invocation_count"
                    ],
                    "evaluator_cost": Jsonb(normalized["evaluator_cost"]),
                    "false_negative_audit": Jsonb(
                        normalized["false_negative_audit"]
                    ),
                    "payload": Jsonb(normalized["payload"]),
                },
            )


def append_observation_event(
    conn: psycopg.Connection[Any],
    definition: ConsistencyCheckDefinition,
    *,
    target_generation_id: int,
    run_id: UUID,
    event: ObservationEvent,
) -> int:
    """Append one observation event and update current projection."""

    _validate_event(event)
    with conn.transaction():
        with conn.cursor() as cur:
            _upsert_consistency_check(cur, definition)
            _assert_run_matches(
                cur,
                run_id=run_id,
                definition_id=definition.check_id,
                target_generation_id=target_generation_id,
            )
            cur.execute(
                """
                SELECT finding_status, evaluation_status, severity,
                       confidence, confidence_type, evidence, payload
                  FROM okg.observation_current
                 WHERE observation_id = %(observation_id)s
                """,
                {"observation_id": event.observation_id},
            )
            row = cur.fetchone()
            if row is None:
                old_finding: FindingStatus = "absent"
                old_evaluation: EvaluationEvent = "not_evaluated"
                old_severity = None
                old_confidence = None
                old_confidence_type = definition.confidence_type
                old_evidence: Mapping[str, Any] = {}
                old_payload: Mapping[str, Any] = {}
            else:
                old_finding = row[0]
                old_evaluation = row[1]
                old_severity = row[2]
                old_confidence = row[3]
                old_confidence_type = row[4]
                old_evidence = row[5] or {}
                old_payload = row[6] or {}

            new_finding = _next_finding_status(
                old_finding,
                event.finding_event,
                event.coverage_complete,
            )
            new_evaluation = event.evaluation_event or old_evaluation
            severity = event.severity if event.severity is not None else old_severity
            confidence = (
                _decimal_or_none(event.confidence)
                if event.confidence is not None
                else old_confidence
            )
            confidence_type = event.confidence_type or old_confidence_type
            evidence = dict(event.evidence) if event.evidence else old_evidence
            payload = dict(event.payload) if event.payload else old_payload

            cur.execute(
                """
                SELECT COALESCE(max(event_seq), 0) + 1
                  FROM okg.observation_events
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": run_id},
            )
            (event_seq,) = cur.fetchone()
            participant_anchors = canonical_participant_anchors(
                event.participant_roles,
                symmetric_predicate=event.symmetric_predicate,
            )
            cur.execute(
                """
                INSERT INTO okg.observation_events (
                  observation_id, check_id, candidate_id,
                  target_generation_id, run_id, event_seq,
                  finding_event, evaluation_event, input_scope,
                  predicate_key, normalized_predicate, normalized_values,
                  participant_roles, participant_anchors, severity,
                  confidence, confidence_type, evidence, payload,
                  coverage_complete
                ) VALUES (
                  %(observation_id)s, %(check_id)s, %(candidate_id)s,
                  %(target_generation_id)s, %(run_id)s, %(event_seq)s,
                  %(finding_event)s, %(evaluation_event)s, %(input_scope)s,
                  %(predicate_key)s, %(normalized_predicate)s,
                  %(normalized_values)s, %(participant_roles)s,
                  %(participant_anchors)s, %(severity)s, %(confidence)s,
                  %(confidence_type)s, %(evidence)s, %(payload)s,
                  %(coverage_complete)s
                )
                RETURNING event_id
                """,
                {
                    "observation_id": event.observation_id,
                    "check_id": definition.check_id,
                    "candidate_id": event.candidate_id,
                    "target_generation_id": target_generation_id,
                    "run_id": run_id,
                    "event_seq": event_seq,
                    "finding_event": event.finding_event,
                    "evaluation_event": event.evaluation_event,
                    "input_scope": event.input_scope,
                    "predicate_key": event.predicate_key,
                    "normalized_predicate": event.normalized_predicate,
                    "normalized_values": Jsonb(dict(event.normalized_values)),
                    "participant_roles": Jsonb(dict(event.participant_roles)),
                    "participant_anchors": Jsonb(participant_anchors),
                    "severity": severity,
                    "confidence": confidence,
                    "confidence_type": confidence_type,
                    "evidence": Jsonb(dict(event.evidence)),
                    "payload": Jsonb(dict(event.payload)),
                    "coverage_complete": event.coverage_complete,
                },
            )
            (event_id,) = cur.fetchone()
            cur.execute(
                """
                INSERT INTO okg.observation_current (
                  observation_id, check_id, candidate_id,
                  latest_generation_id, latest_run_id, latest_event_id,
                  finding_status, evaluation_status, input_scope,
                  predicate_key, normalized_predicate, normalized_values,
                  participant_roles, participant_anchors, severity,
                  confidence, confidence_type, evidence, payload,
                  updated_at
                ) VALUES (
                  %(observation_id)s, %(check_id)s, %(candidate_id)s,
                  %(target_generation_id)s, %(run_id)s, %(event_id)s,
                  %(finding_status)s, %(evaluation_status)s, %(input_scope)s,
                  %(predicate_key)s, %(normalized_predicate)s,
                  %(normalized_values)s, %(participant_roles)s,
                  %(participant_anchors)s, %(severity)s, %(confidence)s,
                  %(confidence_type)s, %(evidence)s, %(payload)s, now()
                )
                ON CONFLICT (observation_id) DO UPDATE SET
                  check_id = EXCLUDED.check_id,
                  candidate_id = EXCLUDED.candidate_id,
                  latest_generation_id = EXCLUDED.latest_generation_id,
                  latest_run_id = EXCLUDED.latest_run_id,
                  latest_event_id = EXCLUDED.latest_event_id,
                  finding_status = EXCLUDED.finding_status,
                  evaluation_status = EXCLUDED.evaluation_status,
                  input_scope = EXCLUDED.input_scope,
                  predicate_key = EXCLUDED.predicate_key,
                  normalized_predicate = EXCLUDED.normalized_predicate,
                  normalized_values = EXCLUDED.normalized_values,
                  participant_roles = EXCLUDED.participant_roles,
                  participant_anchors = EXCLUDED.participant_anchors,
                  severity = EXCLUDED.severity,
                  confidence = EXCLUDED.confidence,
                  confidence_type = EXCLUDED.confidence_type,
                  evidence = EXCLUDED.evidence,
                  payload = EXCLUDED.payload,
                  updated_at = now()
                """,
                {
                    "observation_id": event.observation_id,
                    "check_id": definition.check_id,
                    "candidate_id": event.candidate_id,
                    "target_generation_id": target_generation_id,
                    "run_id": run_id,
                    "event_id": event_id,
                    "finding_status": new_finding,
                    "evaluation_status": new_evaluation,
                    "input_scope": event.input_scope,
                    "predicate_key": event.predicate_key,
                    "normalized_predicate": event.normalized_predicate,
                    "normalized_values": Jsonb(dict(event.normalized_values)),
                    "participant_roles": Jsonb(dict(event.participant_roles)),
                    "participant_anchors": Jsonb(participant_anchors),
                    "severity": severity,
                    "confidence": confidence,
                    "confidence_type": confidence_type,
                    "evidence": Jsonb(dict(evidence)),
                    "payload": Jsonb(dict(payload)),
                },
            )
            cur.execute(
                "DROP TABLE IF EXISTS pg_temp.okg_observation_current_dedupe_keys"
            )
            cur.execute(
                """
                CREATE TEMP TABLE okg_observation_current_dedupe_keys
                ON COMMIT DROP AS
                SELECT
                  %(check_id)s::text AS check_id,
                  %(input_scope)s::text AS input_scope,
                  %(predicate_key)s::text AS predicate_key,
                  %(normalized_predicate)s::text AS normalized_predicate,
                  %(normalized_values)s::jsonb AS normalized_values,
                  %(participant_anchors)s::jsonb AS participant_anchors
                """,
                {
                    "check_id": definition.check_id,
                    "input_scope": event.input_scope,
                    "predicate_key": event.predicate_key,
                    "normalized_predicate": event.normalized_predicate,
                    "normalized_values": Jsonb(dict(event.normalized_values)),
                    "participant_anchors": Jsonb(participant_anchors),
                },
            )
            _dedupe_observation_current_for_staged_events(
                cur,
                "okg_observation_current_dedupe_keys",
            )
            return int(event_id)


def _upsert_consistency_check(
    cur: psycopg.Cursor[Any],
    definition: ConsistencyCheckDefinition,
) -> None:
    cur.execute(
        """
        INSERT INTO okg.derived_artifact_definitions (
          definition_id, artifact_kind, deployment_name, logical_key,
          compute_hash, output_schema_hash, renderer_hash,
          dirty_rule_hash, renderer_ref, output_contract, dirty_rule
        ) VALUES (
          %(definition_id)s, 'consistency_check', %(deployment_name)s,
          %(logical_key)s, %(compute_hash)s, %(output_schema_hash)s,
          %(renderer_hash)s, %(dirty_rule_hash)s, %(renderer_ref)s,
          %(output_contract)s, %(dirty_rule)s
        )
        ON CONFLICT (definition_id) DO UPDATE SET
          artifact_kind = 'consistency_check',
          deployment_name = EXCLUDED.deployment_name,
          logical_key = EXCLUDED.logical_key,
          compute_hash = EXCLUDED.compute_hash,
          output_schema_hash = EXCLUDED.output_schema_hash,
          renderer_hash = EXCLUDED.renderer_hash,
          dirty_rule_hash = EXCLUDED.dirty_rule_hash,
          renderer_ref = EXCLUDED.renderer_ref,
          output_contract = EXCLUDED.output_contract,
          dirty_rule = EXCLUDED.dirty_rule,
          updated_at = now()
        """,
        {
            "definition_id": definition.check_id,
            "deployment_name": definition.deployment_name,
            "logical_key": definition.predicate_family,
            "compute_hash": definition.compute_hash,
            "output_schema_hash": definition.output_schema_hash,
            "renderer_hash": definition.renderer_hash,
            "dirty_rule_hash": definition.dirty_rule_hash,
            "renderer_ref": "okg.renderer.observation.v1",
            "output_contract": Jsonb(
                {
                    "type": "observation_lifecycle",
                    "confidence_type": definition.confidence_type,
                }
            ),
            "dirty_rule": Jsonb(dict(definition.coverage_contract)),
        },
    )
    cur.execute(
        """
        INSERT INTO okg.consistency_check_definitions (
          check_id, deployment_name, predicate_family,
          candidate_rule_hash, evaluator_ref, evaluator_hash,
          confidence_type, coverage_contract, severity_policy,
          check_kind
        ) VALUES (
          %(check_id)s, %(deployment_name)s, %(predicate_family)s,
          %(candidate_rule_hash)s, %(evaluator_ref)s, %(evaluator_hash)s,
          %(confidence_type)s, %(coverage_contract)s, %(severity_policy)s,
          %(check_kind)s
        )
        ON CONFLICT (check_id) DO UPDATE SET
          deployment_name = EXCLUDED.deployment_name,
          predicate_family = EXCLUDED.predicate_family,
          candidate_rule_hash = EXCLUDED.candidate_rule_hash,
          evaluator_ref = EXCLUDED.evaluator_ref,
          evaluator_hash = EXCLUDED.evaluator_hash,
          confidence_type = EXCLUDED.confidence_type,
          coverage_contract = EXCLUDED.coverage_contract,
          severity_policy = EXCLUDED.severity_policy,
          check_kind = EXCLUDED.check_kind,
          updated_at = now()
        """,
        {
            "check_id": definition.check_id,
            "deployment_name": definition.deployment_name,
            "predicate_family": definition.predicate_family,
            "candidate_rule_hash": definition.candidate_rule_hash,
            "evaluator_ref": definition.evaluator_ref,
            "evaluator_hash": definition.evaluator_hash,
            "confidence_type": definition.confidence_type,
            "coverage_contract": Jsonb(dict(definition.coverage_contract)),
            "severity_policy": Jsonb(dict(definition.severity_policy)),
            "check_kind": definition.check_kind,
        },
    )


def _validate_event(event: ObservationEvent) -> None:
    has_finding = event.finding_event is not None
    has_evaluation = event.evaluation_event is not None
    if has_finding == has_evaluation:
        raise ValueError(
            "observation event must set exactly one of finding_event "
            "or evaluation_event"
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


def _normalize_coverage_report(
    report: CandidateCoverageReport,
) -> dict[str, Any]:
    excluded_input_counts = _normalize_count_mapping(
        "excluded_input_counts",
        report.excluded_input_counts,
    )
    candidate_count_by_block = _normalize_count_mapping(
        "candidate_count_by_block",
        report.candidate_count_by_block,
    )

    block_sizes = list(candidate_count_by_block.values())
    block_count = (
        report.block_count
        if report.block_count is not None
        else len(candidate_count_by_block)
    )
    largest_block_size = (
        report.largest_block_size
        if report.largest_block_size is not None
        else (max(block_sizes) if block_sizes else 0)
    )
    p95_block_size = (
        _decimal_or_none(report.p95_block_size)
        if report.p95_block_size is not None
        else _p95(block_sizes)
    )
    all_pairs_upper_bound = (
        report.all_pairs_upper_bound
        if report.all_pairs_upper_bound is not None
        else _all_pairs_upper_bound(report.eligible_input_count)
    )
    reduction_ratio = (
        _decimal_or_none(report.reduction_ratio)
        if report.reduction_ratio is not None
        else _reduction_ratio(
            report.candidate_count,
            all_pairs_upper_bound,
        )
    )

    normalized = {
        "universe_key": report.universe_key,
        "coverage_basis": report.coverage_basis,
        "coverage_complete": report.coverage_complete,
        "eligible_input_count": report.eligible_input_count,
        "excluded_input_counts": excluded_input_counts,
        "missing_text_count": report.missing_text_count,
        "missing_embedding_count": report.missing_embedding_count,
        "block_count": block_count,
        "candidate_count": report.candidate_count,
        "candidate_count_by_block": candidate_count_by_block,
        "largest_block_size": largest_block_size,
        "p95_block_size": p95_block_size,
        "all_pairs_upper_bound": all_pairs_upper_bound,
        "reduction_ratio": reduction_ratio,
        "evaluator_invocation_count": report.evaluator_invocation_count,
        "evaluator_cost": dict(report.evaluator_cost),
        "false_negative_audit": dict(report.false_negative_audit),
        "payload": dict(report.payload),
        # Scoped basis: the write-boundary guard reads scope_restricted (a
        # scope-restricted run may not claim coverage_complete), and both persist
        # to the first-class ``scoped`` / ``scope_subject_count`` columns the gate
        # algebra, doctor, and operator read directly. A full run leaves them
        # False / 0 — never a false negative.
        "scope_restricted": report.scope_restricted,
        "scope_subject_count": int(report.scope_subject_count),
    }
    _validate_coverage(normalized)
    return normalized


def _normalize_count_mapping(
    name: str,
    counts: Mapping[str, int],
) -> dict[str, int]:
    normalized = {str(key): int(value) for key, value in counts.items()}
    negatives = {
        key: value for key, value in normalized.items()
        if value < 0
    }
    if negatives:
        raise ValueError(f"{name} contains negative counts: {negatives}")
    return dict(sorted(normalized.items()))


def _validate_coverage(report: Mapping[str, Any]) -> None:
    count_fields = (
        "eligible_input_count",
        "missing_text_count",
        "missing_embedding_count",
        "block_count",
        "candidate_count",
        "largest_block_size",
        "all_pairs_upper_bound",
        "evaluator_invocation_count",
    )
    for field_name in count_fields:
        if int(report[field_name]) < 0:
            raise ValueError(f"{field_name} must be non-negative")
    if report["p95_block_size"] < 0:
        raise ValueError("p95_block_size must be non-negative")
    if report["reduction_ratio"] < 0:
        raise ValueError("reduction_ratio must be non-negative")

    if report["coverage_complete"] and report["coverage_basis"] not in {
        "exact_postgres",
        "exact_snapshot",
    }:
        raise ValueError(
            "coverage_complete requires exact_postgres or exact_snapshot "
            "coverage_basis"
        )
    if report["coverage_complete"] and report.get("scope_restricted"):
        # "exact over my derived scope" is not "universe-complete". A
        # scope-restricted run evaluated only the changed subjects; claiming
        # coverage_complete would tell the gate algebra and doctor the whole
        # universe was checked. Reject the dishonest claim at the boundary.
        raise ValueError(
            "a scope-restricted run cannot claim coverage_complete "
            "(exact over a derived scope is not universe-complete); set "
            "coverage_complete=False or run a full sweep"
        )
    if report["coverage_complete"] and (
        report["missing_text_count"] > 0
        or report["missing_embedding_count"] > 0
    ):
        raise ValueError(
            "coverage_complete requires zero missing text and embedding counts"
        )

    block_sum = sum(report["candidate_count_by_block"].values())
    if block_sum and block_sum != report["candidate_count"]:
        raise ValueError(
            "candidate_count must equal sum(candidate_count_by_block) "
            "when block counts are provided"
        )


def _all_pairs_upper_bound(eligible_input_count: int) -> int:
    _require_non_negative("eligible_input_count", eligible_input_count)
    return eligible_input_count * max(eligible_input_count - 1, 0) // 2


def _p95(values: Sequence[int]) -> Decimal:
    if not values:
        return Decimal("0")
    sorted_values = sorted(values)
    idx = ceil(0.95 * len(sorted_values)) - 1
    return Decimal(sorted_values[idx])


def _reduction_ratio(
    candidate_count: int,
    all_pairs_upper_bound: int,
) -> Decimal:
    _require_non_negative("candidate_count", candidate_count)
    _require_non_negative("all_pairs_upper_bound", all_pairs_upper_bound)
    if all_pairs_upper_bound == 0:
        return Decimal("0")
    return (
        Decimal(candidate_count) / Decimal(all_pairs_upper_bound)
    ).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)


def _require_non_negative(name: str, value: int) -> None:
    if value < 0:
        raise ValueError(f"{name} must be non-negative")


def _next_finding_status(
    current: FindingStatus,
    event: FindingEvent | None,
    coverage_complete: bool,
) -> FindingStatus:
    if event is None:
        return current
    if event == "resolved" and not coverage_complete:
        raise ValueError("resolved finding event requires complete coverage")
    transitions: dict[tuple[FindingStatus, FindingEvent], FindingStatus] = {
        ("absent", "observed"): "open",
        ("open", "observed"): "open",
        ("open", "resolved"): "resolved",
        ("resolved", "observed"): "open",
        ("resolved", "reopened"): "open",
        ("resolved", "resolved"): "resolved",
    }
    try:
        return transitions[(current, event)]
    except KeyError as exc:
        raise ValueError(
            f"invalid observation finding transition: {current} + {event}"
        ) from exc


def _decimal_or_none(value: Decimal | float | str | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))
