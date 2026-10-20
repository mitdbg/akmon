"""The three-way equivalence harness over a recorded series of real updates.

PACT ``consistency-complete-equivalence``, task.harness.live-run
(req.harness.live-series). Normative semantics in
docs/develop/architecture/consistency.md §9.1 (the same-result contract: the
differential run must return EXACTLY the resolved state complete execution
returns for the same snapshot; equality is exact, no tolerance tier) and §9.7
(the complete-execution reference, the dependency baseline, and named
exclusions).

What this module does
---------------------
:func:`run_equivalence_series` JOINS the three components this child already
landed — it invents no new equivalence logic:

1. the **differential production path** — each recorded update's fact payload is
   applied to the live production DB and a REAL publish cycle runs
   (``build_deployment_publisher`` + ``Publisher.cycle``), exactly the
   idiom of ``tests/substrate/consistency/test_claim_plane_wire.py``;
2. the **complete reference** and the **dependency baseline** — for the
   CUMULATIVE pinned snapshot (the fold of every update so far, op='I' survivors
   after op='R' retractions), each in its own FRESH scratch DB, via
   :func:`okg.substrate.qa.complete_reference.run_complete_reference` /
   :func:`run_dependency_baseline`;
3. the **single equivalence differ** — all three resolved states are snapshotted
   with :class:`okg.substrate.consistency.equivalence_differ.ResolvedStateSnapshot`
   and two verdicts are rendered per update with
   :func:`...equivalence_differ.diff_resolved_states`: differential-vs-reference
   and baseline-vs-reference. Each run's replay exclusions are threaded into the
   comparisons it participates in, so a replay miss turns the update EXCLUDED
   (never counted as agreement, §9.7).

Every verdict is PERSISTED into ``okg.equivalence_verdicts`` (queryable per
generation with :func:`load_series_verdicts`). ``release_blocking`` is
structural — a DB CHECK ties it to ``verdict = 'mismatch'`` — so a mismatch in
the real series is a release-blocking finding for the reuse machinery, not a
statistic.

This module is a caller of the differ; it does NOT re-implement diffing,
extraction, or tolerance. The differ remains the repo's only equivalence
surface (req.harness.live-series).
"""
from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from okg.substrate.consistency.equivalence_differ import (
    VERDICT_EQUAL,
    VERDICT_EXCLUDED,
    VERDICT_MISMATCH,
    CertifiedDifferences,
    EquivalenceVerdict,
    ResolvedStateSnapshot,
    diff_resolved_states,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.llm.outcomes import ReplayExclusion
from okg.substrate.qa.complete_reference import (
    CompleteReferenceResult,
    OutcomeProbe,
    OutcomeReplaySource,
    run_complete_reference,
    run_dependency_baseline,
)
from okg.substrate.telemetry.work_ledger import (
    ModelSpend,
    UnitPricing,
    WorkLedgerRow,
    build_rows_from_cycle_output,
    build_rows_from_reference_result,
    model_call_spend,
    record_work_ledger,
    repair_solved_count,
)

# --------------------------------------------------------------------------- #
# Vocabulary (fixed by the schema CHECK constraints + the ratified adjudication)#
# --------------------------------------------------------------------------- #
EQUIVALENCE_VERDICTS_TABLE = "okg.equivalence_verdicts"

COMPARISON_DIFFERENTIAL = "differential_vs_reference"
COMPARISON_BASELINE = "baseline_vs_reference"
_COMPARISONS = frozenset({COMPARISON_DIFFERENTIAL, COMPARISON_BASELINE})

# The persisted lowercase verdict tokens (the differ renders UPPERCASE
# EQUAL/MISMATCH/EXCLUDED; the table stores equal/mismatch/excluded — the schema
# CHECK enforces the set). Mapped 1:1, never invented.
VERDICT_EQUAL_TOKEN = "equal"
VERDICT_MISMATCH_TOKEN = "mismatch"
VERDICT_EXCLUDED_TOKEN = "excluded"
_VERDICT_TOKEN: dict[str, str] = {
    VERDICT_EQUAL: VERDICT_EQUAL_TOKEN,
    VERDICT_MISMATCH: VERDICT_MISMATCH_TOKEN,
    VERDICT_EXCLUDED: VERDICT_EXCLUDED_TOKEN,
}
_STORED_VERDICTS = frozenset(_VERDICT_TOKEN.values())


# --------------------------------------------------------------------------- #
# Inputs + result shapes                                                       #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SeriesUpdate:
    """One recorded update — an explicit pinned fact payload.

    ``facts`` is THIS update's payload applied to the production DB: op='I'
    inserts (a claim, a policy re-affirmation) and op='R' retractions (a graph
    deletion). The series ACCUMULATES — the cumulative NET snapshot (the fold of
    I/R across all updates so far) is what the complete reference and dependency
    baseline run over in fresh scratch DBs; a fresh scaffold never sees an op='R'
    for a node it never held.

    ``outcome_probe`` is OPTIONAL: it exercises the replay/exclusion contract on
    the reference + baseline runs (a probe whose key is absent from the recorded
    outcomes surfaces a NAMED :class:`ReplayExclusion`, turning the update
    EXCLUDED — never a silent fresh sample). Production runs never take a probe;
    the production path samples fresh and records, it does not replay.
    """

    facts: Sequence[Any]  # Sequence[NodeFact]
    label: str = ""
    outcome_probe: OutcomeProbe | None = None


@dataclass(frozen=True)
class VerdictRecord:
    """One ``okg.equivalence_verdicts`` row — the persisted verdict + evidence.

    ``release_blocking`` mirrors the DB CHECK STRUCTURALLY: it is ``True`` iff
    ``verdict == 'mismatch'``. The constructor enforces every schema invariant so
    a malformed record cannot be built (and thus cannot be stored)."""

    deployment: str
    update_index: int
    production_generation: str
    reference_ref: str
    comparison: str
    verdict: str
    release_blocking: bool
    artifact: dict[str, Any] | None
    work_summary: dict[str, Any] | None
    # The §9.7 EQUAL-with-certified-differences facet: the per-object divergences a
    # VALID reuse certificate licensed, or ``None`` for a plain equal / mismatch /
    # excluded row. Non-null ONLY on an ``equal`` row (a DB CHECK enforces it), so a
    # certified-equal is never silently indistinguishable from a plain-equal.
    certified_differences: list[dict[str, Any]] | None = None

    def __post_init__(self) -> None:
        if self.comparison not in _COMPARISONS:
            raise SubstrateError(
                "equivalence_series: comparison must be one of "
                f"{sorted(_COMPARISONS)}, got {self.comparison!r}"
            )
        if self.verdict not in _STORED_VERDICTS:
            raise SubstrateError(
                "equivalence_series: verdict must be one of "
                f"{sorted(_STORED_VERDICTS)}, got {self.verdict!r}"
            )
        if self.release_blocking != (self.verdict == VERDICT_MISMATCH_TOKEN):
            raise SubstrateError(
                "equivalence_series: release_blocking must be True iff the "
                "verdict is a mismatch (structural, mirrors the DB CHECK)",
                data={"verdict": self.verdict,
                      "release_blocking": self.release_blocking},
            )
        if self.certified_differences and self.verdict != VERDICT_EQUAL_TOKEN:
            raise SubstrateError(
                "equivalence_series: certified_differences rides ONLY on an equal "
                "verdict (§9.7: a mismatch / excluded update licenses nothing)",
                data={"verdict": self.verdict},
            )

    @property
    def is_certified_equal(self) -> bool:
        return self.verdict == VERDICT_EQUAL_TOKEN and bool(
            self.certified_differences
        )


@dataclass(frozen=True)
class UpdateOutcome:
    """The rendered verdicts + snapshots + row counts for one update."""

    update_index: int
    label: str
    production_generation: str          # compat generation token (production)
    production_chronos_generation: str  # Chronos checkpoint id (production)
    reference_ref: str                  # compat generation token (reference)
    reference_chronos_ref: str
    differential_verdict: EquivalenceVerdict
    baseline_verdict: EquivalenceVerdict
    reference_snapshot: ResolvedStateSnapshot
    differential_snapshot: ResolvedStateSnapshot
    baseline_snapshot: ResolvedStateSnapshot
    records: tuple[VerdictRecord, VerdictRecord]  # (differential, baseline)
    # Row counts of the production (differential) resolved state, for reporting.
    effective_count: int
    resolution_count: int
    selection_count: int
    finding_count: int


@dataclass(frozen=True)
class SeriesResult:
    """The whole series' outcomes, with distribution helpers for the report."""

    deployment: str
    outcomes: tuple[UpdateOutcome, ...]

    def all_records(self) -> list[VerdictRecord]:
        out: list[VerdictRecord] = []
        for outcome in self.outcomes:
            out.extend(outcome.records)
        return out

    def verdict_distribution(self) -> dict[str, int]:
        """{equal, mismatch, excluded} counts over EVERY rendered verdict.

        Excluded verdicts live in their OWN bucket — they are never folded into
        the equal count (§9.7)."""
        dist = {
            VERDICT_EQUAL_TOKEN: 0,
            VERDICT_MISMATCH_TOKEN: 0,
            VERDICT_EXCLUDED_TOKEN: 0,
        }
        for record in self.all_records():
            dist[record.verdict] += 1
        return dist

    def production_generations(self) -> list[str]:
        return [o.production_generation for o in self.outcomes]

    def mismatch_records(self) -> list[VerdictRecord]:
        return [r for r in self.all_records()
                if r.verdict == VERDICT_MISMATCH_TOKEN]

    def excluded_records(self) -> list[VerdictRecord]:
        return [r for r in self.all_records()
                if r.verdict == VERDICT_EXCLUDED_TOKEN]


# --------------------------------------------------------------------------- #
# Deployment name + cumulative net fold                                        #
# --------------------------------------------------------------------------- #
def _deployment_name(deployment_dir: Path) -> str:
    from okg.substrate import fastyaml

    manifest = deployment_dir / "deployment.yaml"
    if not manifest.is_file():
        raise SubstrateError(
            f"equivalence_series: deployment.yaml not found under "
            f"{deployment_dir!r}"
        )
    with manifest.open() as fh:
        doc = fastyaml.safe_load(fh) or {}
    name = doc.get("name") if isinstance(doc, dict) else None
    if not name:
        raise SubstrateError(
            f"equivalence_series: deployment.yaml at {manifest!r} declares no name"
        )
    return str(name)


def _fold_net_snapshot(cumulative: dict[str, Any], facts: Sequence[Any]) -> None:
    """Fold this update's facts into the cumulative NET node snapshot in place.

    op='I' upserts a node by node_id (a later revision replaces the earlier one);
    op='R' removes it. ``cumulative.values()`` is therefore the surviving op='I'
    set — exactly what a from-scratch reference should ingest for the pinned
    snapshot, and never an op='R' for a node the fresh scaffold never held."""
    for fact in facts:
        node_id = getattr(fact, "node_id", None)
        op = getattr(fact, "op", "I")
        if node_id is None:
            raise SubstrateError(
                "equivalence_series: a series fact carries no node_id",
                data={"fact": repr(fact)},
            )
        node_id = str(node_id)
        if op == "R":
            cumulative.pop(node_id, None)
        elif op == "I":
            cumulative[node_id] = fact
        else:
            raise SubstrateError(
                f"equivalence_series: unsupported fact op {op!r} (expected "
                "'I' or 'R')",
                data={"node_id": node_id},
            )


# --------------------------------------------------------------------------- #
# Production (differential) path                                               #
# --------------------------------------------------------------------------- #
def _apply_to_production(dsn: str, facts: Sequence[Any], *, deployment: str) -> None:
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.graph_store import ChronosGraphStore

    with db_connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(list(facts), source=deployment, strict_endpoints=False)


def _publish_production(
    dsn: str, *, deployment: str, deployment_dir: Path
) -> dict[str, Any]:
    from okg.substrate.publisher.factory import build_deployment_publisher
    from okg.substrate.publisher.deployment_stack import (
        build_deployment_publisher_stack,
    )

    stack = build_deployment_publisher_stack(
        dsn=dsn, deployment_name=deployment, deployment_dir=deployment_dir,
    )
    out = build_deployment_publisher(
        dsn=dsn,
        deployment_name=deployment,
        deployment_dir=deployment_dir,
        command="qa equivalence series",
        stack=stack,
    ).cycle()
    status = out.get("status")
    if status != "published":
        raise SubstrateError(
            "equivalence_series: production publish did not reach 'published' "
            f"(status={status!r}, blocked_reason={out.get('blocked_reason')!r}); "
            "the differential run produced no generation to compare",
            data={"deployment": deployment},
        )
    return out


def _extract_snapshot(
    dsn: str, *, deployment: str, compat_gen: int | str, chronos_gen: int | str,
) -> ResolvedStateSnapshot:
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn, autocommit=True) as conn:
        return ResolvedStateSnapshot.extract(
            conn,
            deployment=deployment,
            generation_id=compat_gen,
            chronos_generation_id=chronos_gen,
        )


def _read_check_coverage(dsn: str, *, compat_generation_id: int) -> list[dict[str, Any]]:
    """The post-publish check coverage rows for a generation (honest work fact)."""
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT check_id, scoped, coverage_complete, scope_subject_count
              FROM okg.consistency_candidate_coverage
             WHERE target_generation_id = %s
             ORDER BY check_id, run_id
            """,
            (int(compat_generation_id),),
        )
        return [
            {
                "check_id": r[0],
                "scoped": bool(r[1]),
                "coverage_complete": bool(r[2]),
                "scope_subject_count": int(r[3]),
            }
            for r in cur.fetchall()
        ]


# --------------------------------------------------------------------------- #
# Charged-work summaries — store what the runs already carry, invent nothing.  #
# --------------------------------------------------------------------------- #
def _production_work(
    out: Mapping[str, Any], dsn: str, compat_gen: int
) -> dict[str, Any]:
    """The differential candidate's charged-work facts from the cycle output.

    The full charged-work ledger is consistency-maintenance-accounting's task;
    this records exactly the per-derivation modes / causes the cycle output
    already carries plus the post-publish coverage rows. No timing is invented —
    only keys the cycle summary actually holds are copied."""
    modes: dict[str, Any] = {}
    for derivation_id, summary in sorted(
        (out.get("maintained_derivations") or {}).items()
    ):
        summary = summary or {}
        modes[str(derivation_id)] = {
            "status": summary.get("status"),
            "mode": summary.get("mode"),
            "full_reason": summary.get("full_reason"),
            "dirty_bucket_count": summary.get("dirty_bucket_count"),
            "rows_written": summary.get("rows_written"),
            "rows_deleted": summary.get("rows_deleted"),
            "selection_rows": summary.get("selection_rows"),
        }
    return {
        "kind": "differential",
        "reuse_disabled": False,
        "derivation_modes": modes,
        "check_coverage": _read_check_coverage(dsn, compat_generation_id=compat_gen),
    }


def _result_work(result: CompleteReferenceResult) -> dict[str, Any]:
    """The reference/baseline candidate's charged-work facts from the result.

    Reads only the public execution evidence the runner already carries
    (per-derivation modes/causes, coverage, exclusions); the resolved-state dump
    is deliberately NOT stored here (it is what the differ already compared)."""
    return {
        "kind": result.mode,
        "reuse_disabled": result.reuse_disabled,
        "derivation_modes": {
            k: v.to_canonical_dict()
            for k, v in sorted(result.derivation_modes.items())
        },
        "check_coverage": [
            c.to_canonical_dict()
            for c in sorted(result.check_coverage, key=lambda c: c.check_id)
        ],
        "excluded": result.excluded,
        "exclusions": [
            {"step_id": e.step_id, "key_digest": e.key_digest,
             "fragment_id": e.fragment_id}
            for e in result.exclusions
        ],
    }


# --------------------------------------------------------------------------- #
# Charged-work ledger (task.accounting.ledger) — join each run's own DB.        #
# --------------------------------------------------------------------------- #
def _charged_spend(
    dsn: str, *, deployment: str, generation_id: int | str
) -> tuple[ModelSpend, int]:
    """Read one run's charged model spend + freshly-solved repair count from the
    DB the run executed against (production, or a still-live scratch DB).

    The join is frozen into the production ledger row so a scratch DB dropped
    after a validation run does not take its charged spend with it (§9.9)."""
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn, autocommit=True) as conn:
        spend = model_call_spend(
            conn, deployment=deployment, generation_id=generation_id
        )
        repairs = repair_solved_count(
            conn, deployment=deployment, generation_id=generation_id
        )
    return spend, repairs


# --------------------------------------------------------------------------- #
# Verdict -> record                                                            #
# --------------------------------------------------------------------------- #
def _record_for(
    verdict: EquivalenceVerdict,
    *,
    deployment: str,
    update_index: int,
    production_generation: int | str,
    reference_ref: int | str,
    comparison: str,
    candidate_work: dict[str, Any],
    reference_work: dict[str, Any],
) -> VerdictRecord:
    if verdict.verdict == VERDICT_MISMATCH:
        artifact: dict[str, Any] | None = verdict.artifact.to_json_dict()
    elif verdict.verdict == VERDICT_EXCLUDED:
        artifact = {"excluded_keys": [dict(k) for k in verdict.excluded_keys]}
    else:
        artifact = None
    certified = (
        [dict(d) for d in verdict.certified_differences]
        if verdict.certified_differences else None
    )
    return VerdictRecord(
        deployment=deployment,
        update_index=update_index,
        production_generation=str(production_generation),
        reference_ref=str(reference_ref),
        comparison=comparison,
        verdict=_VERDICT_TOKEN[verdict.verdict],
        release_blocking=(verdict.verdict == VERDICT_MISMATCH),
        artifact=artifact,
        work_summary={"candidate": candidate_work, "reference": reference_work},
        certified_differences=certified,
    )


# --------------------------------------------------------------------------- #
# Storage + query                                                             #
# --------------------------------------------------------------------------- #
def store_equivalence_verdicts(
    conn: Any, records: Sequence[VerdictRecord]
) -> int:
    """Persist verdict records into ``okg.equivalence_verdicts``.

    Delete-then-insert per (deployment_name, production_generation) in one
    transaction (mirroring ``repair_materialization.persist_repair_selections``):
    re-running a series' generation replaces its own rows and nothing else. The
    DB CHECK constraints reject any malformed row (they mirror
    :class:`VerdictRecord`'s own invariants). Returns the count written."""
    records = list(records)
    if not records:
        return 0
    groups: dict[tuple[str, str], list[VerdictRecord]] = {}
    for record in records:
        groups.setdefault(
            (record.deployment, record.production_generation), []
        ).append(record)
    written = 0
    with conn.transaction(), conn.cursor() as cur:
        for (deployment, generation), group in groups.items():
            cur.execute(
                f"DELETE FROM {EQUIVALENCE_VERDICTS_TABLE} "
                "WHERE deployment_name = %s AND production_generation = %s",
                (deployment, generation),
            )
            for record in group:
                cur.execute(
                    f"INSERT INTO {EQUIVALENCE_VERDICTS_TABLE} "
                    "(deployment_name, update_index, production_generation, "
                    " reference_ref, comparison, verdict, release_blocking, "
                    " artifact, work_summary, certified_differences) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, "
                    " %s::jsonb)",
                    (
                        record.deployment,
                        record.update_index,
                        record.production_generation,
                        record.reference_ref,
                        record.comparison,
                        record.verdict,
                        record.release_blocking,
                        json.dumps(record.artifact)
                        if record.artifact is not None else None,
                        json.dumps(record.work_summary)
                        if record.work_summary is not None else None,
                        json.dumps(record.certified_differences)
                        if record.certified_differences is not None else None,
                    ),
                )
                written += 1
    return written


def load_series_verdicts(conn: Any, deployment: str) -> list[VerdictRecord]:
    """Load every stored equivalence verdict for a deployment, ordered.

    The queryable read surface the live-series task promises: per generation, per
    comparison, with the named artifact and the structural release_blocking flag."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT deployment_name, update_index, production_generation, reference_ref,
                   comparison, verdict, release_blocking, artifact, work_summary,
                   certified_differences
              FROM {EQUIVALENCE_VERDICTS_TABLE}
             WHERE deployment_name = %s
             ORDER BY update_index, production_generation, comparison
            """,
            (deployment,),
        )
        rows = cur.fetchall()
    out: list[VerdictRecord] = []
    for (
        dep, update_index, generation, reference_ref, comparison, verdict,
        release_blocking, artifact, work_summary, certified_differences,
    ) in rows:
        out.append(
            VerdictRecord(
                deployment=str(dep),
                update_index=int(update_index),
                production_generation=str(generation),
                reference_ref=str(reference_ref),
                comparison=str(comparison),
                verdict=str(verdict),
                release_blocking=bool(release_blocking),
                artifact=dict(artifact) if artifact is not None else None,
                work_summary=dict(work_summary) if work_summary is not None else None,
                certified_differences=(
                    list(certified_differences)
                    if certified_differences is not None else None
                ),
            )
        )
    return out


# --------------------------------------------------------------------------- #
# The series runner                                                            #
# --------------------------------------------------------------------------- #
def _dedupe_exclusions(
    *groups: Sequence[ReplayExclusion],
) -> list[ReplayExclusion]:
    seen: set[tuple[str, str, str]] = set()
    out: list[ReplayExclusion] = []
    for group in groups:
        for exclusion in group:
            key = (exclusion.step_id, exclusion.key_digest, exclusion.fragment_id)
            if key not in seen:
                seen.add(key)
                out.append(exclusion)
    return out


def run_equivalence_series(
    updates: Sequence[SeriesUpdate],
    *,
    deployment_dir: Path | str,
    production_dsn: str,
    scratch_dsn_factory: Callable[[], str],
    outcome_source: OutcomeReplaySource | None = None,
    persist: bool = True,
) -> SeriesResult:
    """Run the three-way harness over a recorded series and render/persist
    per-update verdicts.

    For each update, in order:

    1. apply the update's fact payload to ``production_dsn`` and run the REAL
       differential publish cycle;
    2. fold the payload into the cumulative NET snapshot and run the complete
       reference + dependency baseline over it, each in a fresh scratch DB from
       ``scratch_dsn_factory``;
    3. snapshot all three resolved states with :class:`ResolvedStateSnapshot`;
    4. render the differential-vs-reference and baseline-vs-reference verdicts
       with :func:`diff_resolved_states`, threading each run's replay exclusions;
    5. build the two :class:`VerdictRecord` rows.

    When ``persist`` is True (default), every verdict is written to
    ``okg.equivalence_verdicts`` on ``production_dsn`` at the end (queryable with
    :func:`load_series_verdicts`).

    ``outcome_source`` overrides the replayed model outcomes; by default the
    reference/baseline replay exactly what production recorded
    (``OutcomeReplaySource.from_dsn(production_dsn, deployment)``) — for a
    deployment with no model-backed step this is an explicit empty pin.
    """
    from okg.substrate.db.connection import connect as db_connect

    updates = list(updates)
    if not updates:
        raise SubstrateError("equivalence_series: the series has no updates")

    dep_dir = Path(deployment_dir)
    deployment = _deployment_name(dep_dir)

    cumulative: dict[str, Any] = {}
    outcomes: list[UpdateOutcome] = []
    records: list[VerdictRecord] = []
    ledger_rows: list[WorkLedgerRow] = []

    for index, update in enumerate(updates):
        facts = list(update.facts)
        if not facts:
            raise SubstrateError(
                f"equivalence_series: update {index} has an empty fact payload",
                data={"index": index, "label": update.label},
            )

        # 1) apply this update to production + the REAL differential publish.
        _apply_to_production(production_dsn, facts, deployment=deployment)
        out = _publish_production(
            production_dsn, deployment=deployment, deployment_dir=dep_dir
        )
        prod_compat = int(out["compat_generation_id"])
        prod_chronos = str(out["generation_id"])

        # 2) fold into the cumulative NET snapshot for the from-scratch runs.
        _fold_net_snapshot(cumulative, facts)
        net_facts = list(cumulative.values())
        if not net_facts:
            raise SubstrateError(
                f"equivalence_series: update {index} left an empty cumulative "
                "snapshot (a reference cannot run against no facts)",
                data={"index": index, "label": update.label},
            )

        # Replay what production recorded (cumulative), unless overridden.
        source = (
            outcome_source
            if outcome_source is not None
            else OutcomeReplaySource.from_dsn(production_dsn, deployment)
        )

        # 3) complete reference + dependency baseline in fresh scratch DBs.
        reference_dsn = scratch_dsn_factory()
        reference_result = run_complete_reference(
            net_facts,
            deployment_dir=dep_dir,
            scratch_dsn=reference_dsn,
            outcome_store_source=source,
            outcome_probe=update.outcome_probe,
        )
        baseline_dsn = scratch_dsn_factory()
        baseline_result = run_dependency_baseline(
            net_facts,
            deployment_dir=dep_dir,
            scratch_dsn=baseline_dsn,
            outcome_store_source=source,
            outcome_probe=update.outcome_probe,
        )

        # 4) snapshot all three resolved states through the single differ surface.
        differential_snapshot = _extract_snapshot(
            production_dsn, deployment=deployment,
            compat_gen=prod_compat, chronos_gen=prod_chronos,
        )
        reference_snapshot = _extract_snapshot(
            reference_dsn, deployment=deployment,
            compat_gen=reference_result.compat_generation_id,
            chronos_gen=reference_result.generation_id,
        )
        baseline_snapshot = _extract_snapshot(
            baseline_dsn, deployment=deployment,
            compat_gen=baseline_result.compat_generation_id,
            chronos_gen=baseline_result.generation_id,
        )

        # 5) render the two verdicts, threading each run's replay exclusions.
        #    The production path samples fresh (no replay), so differential-vs-
        #    reference threads only the reference's exclusions; baseline-vs-
        #    reference threads both replay runs' exclusions.
        differential_verdict = diff_resolved_states(
            reference_snapshot,
            differential_snapshot,
            exclusions=_dedupe_exclusions(reference_result.exclusions),
        )
        baseline_verdict = diff_resolved_states(
            reference_snapshot,
            baseline_snapshot,
            exclusions=_dedupe_exclusions(
                reference_result.exclusions, baseline_result.exclusions
            ),
        )

        reference_work = _result_work(reference_result)
        differential_record = _record_for(
            differential_verdict,
            deployment=deployment,
            update_index=index,
            production_generation=prod_compat,
            reference_ref=reference_result.compat_generation_id,
            comparison=COMPARISON_DIFFERENTIAL,
            candidate_work=_production_work(out, production_dsn, prod_compat),
            reference_work=reference_work,
        )
        baseline_record = _record_for(
            baseline_verdict,
            deployment=deployment,
            update_index=index,
            production_generation=prod_compat,
            reference_ref=reference_result.compat_generation_id,
            comparison=COMPARISON_BASELINE,
            candidate_work=_result_work(baseline_result),
            reference_work=reference_work,
        )

        outcomes.append(
            UpdateOutcome(
                update_index=index,
                label=update.label,
                production_generation=str(prod_compat),
                production_chronos_generation=prod_chronos,
                reference_ref=str(reference_result.compat_generation_id),
                reference_chronos_ref=str(reference_result.generation_id),
                differential_verdict=differential_verdict,
                baseline_verdict=baseline_verdict,
                reference_snapshot=reference_snapshot,
                differential_snapshot=differential_snapshot,
                baseline_snapshot=baseline_snapshot,
                records=(differential_record, baseline_record),
                effective_count=len(differential_snapshot.effective_claims),
                resolution_count=len(differential_snapshot.resolution_outcomes),
                selection_count=len(differential_snapshot.selections),
                finding_count=len(differential_snapshot.open_findings),
            )
        )
        records.extend((differential_record, baseline_record))

        # Charged-work ledger for all three executions (task.accounting.ledger).
        # This sibling is reuse-DISABLED, so the differential path runs no reuse
        # pass — effect analysis / certificate validation render as LOUD GAPS on
        # the differential rows (honest: uninstrumented, never a silent zero).
        diff_spend, diff_repairs = _charged_spend(
            production_dsn, deployment=deployment, generation_id=prod_compat
        )
        ledger_rows.extend(build_rows_from_cycle_output(
            deployment=deployment, generation_id=prod_compat, cycle_output=out,
            model_spend=diff_spend, repair_solved=diff_repairs,
            reuse_pass_result=None,
        ))
        ref_spend, ref_repairs = _charged_spend(
            reference_dsn, deployment=deployment,
            generation_id=reference_result.compat_generation_id,
        )
        ledger_rows.extend(build_rows_from_reference_result(
            deployment=deployment, anchor_generation_id=prod_compat,
            result=reference_result, model_spend=ref_spend,
            repair_solved=ref_repairs,
        ))
        base_spend, base_repairs = _charged_spend(
            baseline_dsn, deployment=deployment,
            generation_id=baseline_result.compat_generation_id,
        )
        ledger_rows.extend(build_rows_from_reference_result(
            deployment=deployment, anchor_generation_id=prod_compat,
            result=baseline_result, model_spend=base_spend,
            repair_solved=base_repairs,
        ))

    if persist:
        with db_connect(production_dsn, autocommit=True) as conn:
            store_equivalence_verdicts(conn, records)
            record_work_ledger(conn, ledger_rows)

    return SeriesResult(deployment=deployment, outcomes=tuple(outcomes))


# --------------------------------------------------------------------------- #
# Reuse-governed series — the live-proof sibling (task.reuse.live-proof).      #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ReuseConeResult:
    """What a per-update scheduler cone pass produced.

    ``reuse_pass`` is the :func:`...reuse_pass_workflow.run_reuse_pass_in_cycle`
    summary (terminal state + skip/run outcomes + recorded certificate digests).
    ``certified_differences`` is the §9.7 licence the pass's VALID certificates
    grant — the buckets the differential run therefore never extracted a loser
    into (``None`` for an update with no dominance skip). ``skipped_producer_step``
    names the producer step the DIFFERENTIAL run did not execute (the changed-
    inputs skip), or ``None``."""

    reuse_pass: dict[str, Any]
    certified_differences: CertifiedDifferences | None = None
    skipped_producer_step: str | None = None


@dataclass(frozen=True)
class ReuseGovernedUpdate:
    """One recorded update in a SCHEDULER-GOVERNED series (task.reuse.live-proof).

    ``production_facts`` are applied to the production DB and materialized by the
    real differential publish. ``snapshot_extra_facts`` are the CHANGED-INPUTS-SKIP
    claims: NOT applied to production (the scheduler skipped their producer step,
    so the differential run never extracts them) but ADDED to the cumulative net
    the complete reference + dependency baseline run over — so the reference
    contains the superseded loser the differential lacks (§9.7). Empty on every
    non-skip update. ``run_cone`` runs the reuse scheduler pass over the published
    generation (recording certificates) and returns the :class:`ReuseConeResult`
    whose certified-differences license the differential run's never-extracted
    loser; it is called ``run_cone(production_dsn, deployment, production_generation)``.
    """

    label: str
    production_facts: Sequence[Any]
    snapshot_extra_facts: Sequence[Any] = ()
    run_cone: Callable[[str, str, str], ReuseConeResult] | None = None
    outcome_probe: OutcomeProbe | None = None


@dataclass(frozen=True)
class ReuseGovernedOutcome:
    """The rendered verdicts + reuse-pass facts for one scheduler-governed update."""

    update_index: int
    label: str
    production_generation: str
    production_chronos_generation: str
    reference_ref: str
    reference_chronos_ref: str
    baseline_ref: str
    differential_verdict: EquivalenceVerdict
    baseline_verdict: EquivalenceVerdict
    reference_snapshot: ResolvedStateSnapshot
    differential_snapshot: ResolvedStateSnapshot
    baseline_snapshot: ResolvedStateSnapshot
    records: tuple[VerdictRecord, VerdictRecord]
    reuse_pass: dict[str, Any] | None
    certified_differences: CertifiedDifferences | None
    skipped_producer_step: str | None
    # Structural proof of a genuine skip: the differential run's effective set is a
    # STRICT subset of the reference's (it never extracted the skipped loser), and
    # the dependency baseline's effective set EQUALS the reference's (it DID run the
    # step). True only on the genuine-skip update.
    baseline_ran_skipped_step: bool
    effective_count: int
    resolution_count: int
    selection_count: int
    finding_count: int

    @property
    def is_certified_equal(self) -> bool:
        return self.differential_verdict.is_certified_equal


@dataclass(frozen=True)
class ReuseGovernedResult:
    """The whole scheduler-governed series' outcomes + distribution helpers."""

    deployment: str
    outcomes: tuple[ReuseGovernedOutcome, ...]

    def all_records(self) -> list[VerdictRecord]:
        out: list[VerdictRecord] = []
        for outcome in self.outcomes:
            out.extend(outcome.records)
        return out

    def verdict_distribution(self) -> dict[str, int]:
        """{equal, mismatch, excluded} over every rendered verdict, plus a
        ``certified_equal`` count (a subset of ``equal`` — the §9.7 facet). Excluded
        verdicts live in their own bucket, never folded into equal (§9.7)."""
        dist = {
            VERDICT_EQUAL_TOKEN: 0,
            VERDICT_MISMATCH_TOKEN: 0,
            VERDICT_EXCLUDED_TOKEN: 0,
            "certified_equal": 0,
        }
        for record in self.all_records():
            dist[record.verdict] += 1
            if record.is_certified_equal:
                dist["certified_equal"] += 1
        return dist

    def production_generations(self) -> list[str]:
        return [o.production_generation for o in self.outcomes]

    def certified_equal_outcomes(self) -> list[ReuseGovernedOutcome]:
        return [o for o in self.outcomes if o.is_certified_equal]

    def mismatch_records(self) -> list[VerdictRecord]:
        return [r for r in self.all_records()
                if r.verdict == VERDICT_MISMATCH_TOKEN]


def run_reuse_governed_series(
    updates: Sequence[ReuseGovernedUpdate],
    *,
    deployment_dir: Path | str,
    production_dsn: str,
    scratch_dsn_factory: Callable[[], str],
    outcome_source: OutcomeReplaySource | None = None,
    persist: bool = True,
    # The deployment's declared USD-per-work-unit price table for the
    # deterministic categories. None keeps today's behaviour: every deterministic
    # category is UNPRICED (a loud null in the savings report), never a silent
    # zero. An E3 ablation declares one table and shares it across every arm.
    pricing: UnitPricing | None = None,
) -> ReuseGovernedResult:
    """Run the three-way harness over a SCHEDULER-GOVERNED series (task.reuse.live-proof).

    The same-result payoff. Like :func:`run_equivalence_series` but the differential
    production path is reuse-ENABLED: after each real publish the update's
    ``run_cone`` runs the reuse scheduler pass (recording certificates). The
    CENTREPIECE changed-inputs skip is realized honestly — a claim whose producer
    step the scheduler skips behind a bounded-dominance certificate is NOT applied
    to production (``snapshot_extra_facts``, never ``production_facts``), so the
    differential run's effective set genuinely lacks it, while the complete
    reference + dependency baseline run over the cumulative net that INCLUDES it
    (the baseline runs the step; the reference contains the superseded loser).

    For each update, in order: apply ``production_facts`` + real differential
    publish; run ``run_cone`` (records certificates, yields the §9.7 certified
    differences); fold ``production_facts`` + ``snapshot_extra_facts`` into the
    cumulative net and run the complete reference + dependency baseline in fresh
    scratch DBs; snapshot all three; render the differential-vs-reference verdict
    THROUGH the certified-differences licence (a licensed divergence is
    EQUAL-with-certified-differences, never a silent plain-equal) and the
    baseline-vs-reference verdict plainly; persist both. A MISMATCH in the real
    series is a release-blocking defect, never a relaxation."""
    from okg.substrate.db.connection import connect as db_connect

    updates = list(updates)
    if not updates:
        raise SubstrateError("equivalence_series: the reuse-governed series has no updates")

    dep_dir = Path(deployment_dir)
    deployment = _deployment_name(dep_dir)

    cumulative: dict[str, Any] = {}
    outcomes: list[ReuseGovernedOutcome] = []
    records: list[VerdictRecord] = []
    ledger_rows: list[WorkLedgerRow] = []

    for index, update in enumerate(updates):
        production_facts = list(update.production_facts)
        if not production_facts:
            raise SubstrateError(
                f"equivalence_series: reuse-governed update {index} has an empty "
                "production fact payload (a differential publish needs a delta)",
                data={"index": index, "label": update.label},
            )

        # 1) apply the differential (production) facts + the REAL publish.
        _apply_to_production(production_dsn, production_facts, deployment=deployment)
        out = _publish_production(
            production_dsn, deployment=deployment, deployment_dir=dep_dir
        )
        prod_compat = int(out["compat_generation_id"])
        prod_chronos = str(out["generation_id"])

        # 2) run the scheduler cone pass over the published generation.
        cone: ReuseConeResult | None = None
        if update.run_cone is not None:
            cone = update.run_cone(production_dsn, deployment, str(prod_compat))
        certified = cone.certified_differences if cone is not None else None

        # 3) fold BOTH the production facts and the skipped (withheld) facts into
        #    the cumulative net — the reference / baseline run over the FULL update,
        #    so the reference contains the loser the differential skipped.
        _fold_net_snapshot(
            cumulative, [*production_facts, *update.snapshot_extra_facts]
        )
        net_facts = list(cumulative.values())
        if not net_facts:
            raise SubstrateError(
                f"equivalence_series: reuse-governed update {index} left an empty "
                "cumulative snapshot",
                data={"index": index, "label": update.label},
            )

        source = (
            outcome_source
            if outcome_source is not None
            else OutcomeReplaySource.from_dsn(production_dsn, deployment)
        )

        # 4) complete reference + dependency baseline over the cumulative net.
        reference_dsn = scratch_dsn_factory()
        reference_result = run_complete_reference(
            net_facts, deployment_dir=dep_dir, scratch_dsn=reference_dsn,
            outcome_store_source=source, outcome_probe=update.outcome_probe,
        )
        baseline_dsn = scratch_dsn_factory()
        baseline_result = run_dependency_baseline(
            net_facts, deployment_dir=dep_dir, scratch_dsn=baseline_dsn,
            outcome_store_source=source, outcome_probe=update.outcome_probe,
        )

        # 5) snapshot all three resolved states.
        differential_snapshot = _extract_snapshot(
            production_dsn, deployment=deployment,
            compat_gen=prod_compat, chronos_gen=prod_chronos,
        )
        reference_snapshot = _extract_snapshot(
            reference_dsn, deployment=deployment,
            compat_gen=reference_result.compat_generation_id,
            chronos_gen=reference_result.generation_id,
        )
        baseline_snapshot = _extract_snapshot(
            baseline_dsn, deployment=deployment,
            compat_gen=baseline_result.compat_generation_id,
            chronos_gen=baseline_result.generation_id,
        )

        # 6) render the two verdicts. The differential-vs-reference verdict is
        #    rendered THROUGH the §9.7 certified-differences licence, so a
        #    dominance-covered never-extracted loser is EQUAL-with-certified-
        #    differences (never a silent plain-equal); the baseline ran the step so
        #    its verdict is plain.
        differential_verdict = diff_resolved_states(
            reference_snapshot, differential_snapshot,
            exclusions=_dedupe_exclusions(reference_result.exclusions),
            certified_differences=certified,
        )
        baseline_verdict = diff_resolved_states(
            reference_snapshot, baseline_snapshot,
            exclusions=_dedupe_exclusions(
                reference_result.exclusions, baseline_result.exclusions
            ),
        )

        # Structural proof of a genuine skip: the differential's effective set is a
        # strict subset of the reference's, while the baseline's equals it.
        ref_eff = set(reference_snapshot.effective_claims)
        diff_eff = set(differential_snapshot.effective_claims)
        base_eff = set(baseline_snapshot.effective_claims)
        baseline_ran_skipped_step = bool(
            update.snapshot_extra_facts
        ) and (diff_eff < ref_eff) and (base_eff == ref_eff)

        reference_work = _result_work(reference_result)
        differential_record = _record_for(
            differential_verdict, deployment=deployment, update_index=index,
            production_generation=prod_compat,
            reference_ref=reference_result.compat_generation_id,
            comparison=COMPARISON_DIFFERENTIAL,
            candidate_work=_production_work(out, production_dsn, prod_compat),
            reference_work=reference_work,
        )
        baseline_record = _record_for(
            baseline_verdict, deployment=deployment, update_index=index,
            production_generation=prod_compat,
            reference_ref=reference_result.compat_generation_id,
            comparison=COMPARISON_BASELINE,
            candidate_work=_result_work(baseline_result),
            reference_work=reference_work,
        )

        outcomes.append(
            ReuseGovernedOutcome(
                update_index=index, label=update.label,
                production_generation=str(prod_compat),
                production_chronos_generation=prod_chronos,
                reference_ref=str(reference_result.compat_generation_id),
                reference_chronos_ref=str(reference_result.generation_id),
                baseline_ref=str(baseline_result.compat_generation_id),
                differential_verdict=differential_verdict,
                baseline_verdict=baseline_verdict,
                reference_snapshot=reference_snapshot,
                differential_snapshot=differential_snapshot,
                baseline_snapshot=baseline_snapshot,
                records=(differential_record, baseline_record),
                reuse_pass=cone.reuse_pass if cone is not None else None,
                certified_differences=certified,
                skipped_producer_step=(
                    cone.skipped_producer_step if cone is not None else None
                ),
                baseline_ran_skipped_step=baseline_ran_skipped_step,
                effective_count=len(differential_snapshot.effective_claims),
                resolution_count=len(differential_snapshot.resolution_outcomes),
                selection_count=len(differential_snapshot.selections),
                finding_count=len(differential_snapshot.open_findings),
            )
        )
        records.extend((differential_record, baseline_record))

        # Charged-work ledger for all three executions of this update
        # (task.accounting.ledger). The differential row set reads the reuse
        # pass' result (measured effect analysis / certificate validation) when
        # a cone ran; the two from-scratch validation runs write validation=TRUE
        # rows anchored to THIS update's production generation.
        reuse_pass_result = (
            cone.reuse_pass.get("result")
            if cone is not None and isinstance(cone.reuse_pass, Mapping)
            else None
        )
        diff_spend, diff_repairs = _charged_spend(
            production_dsn, deployment=deployment, generation_id=prod_compat
        )
        ledger_rows.extend(build_rows_from_cycle_output(
            deployment=deployment, generation_id=prod_compat, cycle_output=out,
            model_spend=diff_spend, repair_solved=diff_repairs,
            reuse_pass_result=reuse_pass_result, pricing=pricing,
        ))
        ref_spend, ref_repairs = _charged_spend(
            reference_dsn, deployment=deployment,
            generation_id=reference_result.compat_generation_id,
        )
        ledger_rows.extend(build_rows_from_reference_result(
            deployment=deployment, anchor_generation_id=prod_compat,
            result=reference_result, model_spend=ref_spend,
            repair_solved=ref_repairs, pricing=pricing,
        ))
        base_spend, base_repairs = _charged_spend(
            baseline_dsn, deployment=deployment,
            generation_id=baseline_result.compat_generation_id,
        )
        ledger_rows.extend(build_rows_from_reference_result(
            deployment=deployment, anchor_generation_id=prod_compat,
            result=baseline_result, model_spend=base_spend,
            repair_solved=base_repairs, pricing=pricing,
        ))

    if persist:
        with db_connect(production_dsn, autocommit=True) as conn:
            store_equivalence_verdicts(conn, records)
            record_work_ledger(conn, ledger_rows)

    return ReuseGovernedResult(deployment=deployment, outcomes=tuple(outcomes))


__all__ = [
    "COMPARISON_BASELINE",
    "COMPARISON_DIFFERENTIAL",
    "EQUIVALENCE_VERDICTS_TABLE",
    "SeriesResult",
    "SeriesUpdate",
    "UpdateOutcome",
    "VerdictRecord",
    "ReuseConeResult",
    "ReuseGovernedUpdate",
    "ReuseGovernedOutcome",
    "ReuseGovernedResult",
    "load_series_verdicts",
    "run_equivalence_series",
    "run_reuse_governed_series",
    "store_equivalence_verdicts",
]
