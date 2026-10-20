#!/usr/bin/env python3
"""Run the merge-back TLA+ safety models with TLC."""

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

import yaml

from okg.substrate.formal_tlc import (
    TLCProperty,
    TLCReportResult,
    TLCSetupError,
    run_tlc_formal_report,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "merge_back"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "merge-back-safety"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_merge_back_safety.py "
    "--out .scratch/formal/merge-back-safety"
)

STAGE_LAND_TESTS = (
    "tests/substrate/test_merge_back_orchestrator.py::"
    "test_merge_back_clean_fork_lands_on_default",
    "tests/substrate/test_merge_back_orchestrator.py::"
    "test_merge_back_composes_promote_scope_restamp_partition",
    "tests/substrate/test_merge_back_orchestrator.py::"
    "test_merge_back_gate_violation_blocks_default_unchanged",
    "tests/substrate/test_merge_back_orchestrator.py::"
    "test_merge_back_holds_default_write_lock_through_land_publish",
    "tests/substrate/test_merge_back_orchestrator.py::"
    "test_merge_back_replays_source_only_and_derives_once_on_land",
    "tests/substrate/test_merge_back_orchestrator.py::"
    "test_gate_side_effect_cleanup_keeps_foreign_derived_facts",
)
RESTAMP_TESTS = (
    "tests/substrate/test_merge_restamp.py::test_null_source_refused",
    "tests/substrate/test_merge_restamp.py::test_reserved_system_source_refused",
    "tests/substrate/test_merge_restamp.py::"
    "test_restamp_row_normalizes_all_provenance_and_strips_promote_intent",
    "tests/substrate/test_merge_restamp.py::"
    "test_merge_producer_id_reads_back_untrusted",
    "tests/substrate/test_merge_restamp.py::"
    "test_reserved_is_superset_of_system_prefixes",
    "tests/substrate/test_merge_restamp.py::"
    "test_system_prefixes_read_trusted_and_null_too",
    "tests/substrate/test_merge_restamp.py::"
    "test_derived_prefixes_read_untrusted_but_still_refused",
    "tests/substrate/test_merge_back_orchestrator.py::"
    "test_merge_back_blocks_whole_merge_on_unmergeable_source",
)
PARTITION_TESTS = (
    "tests/substrate/test_merge_partition.py::"
    "test_pact_accept_conflict_illegal_bucketing",
    "tests/substrate/test_merge_partition.py::test_total_coverage_no_silent_loss",
    "tests/substrate/test_merge_partition.py::"
    "test_node_with_no_subtype_is_unclassifiable_not_silently_accepted",
    "tests/substrate/test_merge_partition.py::test_partition_over_real_fork_preview",
    "tests/substrate/test_merge_back_orchestrator.py::"
    "test_merge_back_composes_promote_scope_restamp_partition",
    "tests/substrate/test_merge_back_orchestrator.py::"
    "test_merge_back_quarantines_conflicting_durable_rows",
    "tests/substrate/test_merge_back_orchestrator.py::"
    "test_merge_back_stage_quarantine_fault_rolls_back_both_writes",
)
ROLLBACK_TESTS = (
    "tests/substrate/test_merge_back_orchestrator.py::"
    "test_merge_back_rolls_default_back_when_land_publish_blocks",
    "tests/substrate/test_merge_back_orchestrator.py::"
    "test_merge_back_rolls_back_modified_node_and_edge_live_rows",
)


@dataclass(frozen=True)
class MergeBackModel:
    model: str
    config: str
    property: TLCProperty
    # Vacuity floor: the measured distinct-state count of the current model
    # (these are exact deterministic counts for the fixed configs). A future
    # edit that collapses a model back to a smaller script fails closed.
    min_distinct_states: int = 2


MODEL_CHECKS = (
    MergeBackModel(
        "MergeBackReplay.tla",
        "MergeBackReplay.cfg",
        TLCProperty(
            "formal.merge-back.stage-land-replay-equivalence",
            (
                "TLC verified merge-back staging isolation, gate/land "
                "replay equivalence, target-advance blocking, gate-derived "
                "row exclusion, and no phantom retraction after LAND."
            ),
            refinement_tests=STAGE_LAND_TESTS,
            expected_invariants=(
                "TypeOK",
                "DefaultIsolatedUntilGatePass",
                "GateValidatesStagedAcceptedDelta",
                "LandReplayEqualsGatedSourceDelta",
                "GateBlockedLeavesDefaultUnchanged",
                "TargetAdvanceBlocksLand",
                "GateDerivedRowsAreNotReplayed",
                "NoPhantomRetractionAfterLand",
                "MergeBackReplaySafety",
            ),
        ),
        min_distinct_states=10,
    ),
    MergeBackModel(
        "MergeBackRestamp.tla",
        "MergeBackRestamp.cfg",
        TLCProperty(
            "formal.merge-back.trust-restamp-antilaundering",
            (
                "TLC verified merge-back restamp refuses laundering sources, "
                "stamps merge provenance as untrusted, and prevents fork "
                "identity or promote markers from landing on default."
            ),
            refinement_tests=RESTAMP_TESTS,
            expected_invariants=(
                "TypeOK",
                "ReservedOrNullSourcesBlockBeforeStage",
                "MergeProducerClassifiesUntrusted",
                "SourceFieldRestamped",
                "SourceRecordIdRestamped",
                "SourceRevisionRestamped",
                "ForkProvenanceIdentityDoesNotLand",
                "PromoteMarkerDoesNotLand",
                "NoForkInputYieldsTrustedDefaultSource",
                "MergeBackRestampSafety",
            ),
        ),
        min_distinct_states=12,
    ),
    MergeBackModel(
        "MergeBackPartitionAtomicity.tla",
        "MergeBackPartitionAtomicity.cfg",
        TLCProperty(
            "formal.merge-back.quarantine-partition-atomicity",
            (
                "TLC verified merge-back partition totality, conflict and "
                "illegal-row quarantine, unclassifiable-row blocking, and "
                "atomic staging/quarantine commit behavior."
            ),
            refinement_tests=PARTITION_TESTS,
            expected_invariants=(
                "TypeOK",
                "AcceptedQuarantinedDisjoint",
                "AcceptedQuarantinedCoverDurablePreview",
                "ConflictsAlwaysQuarantined",
                "IllegalRowsQuarantined",
                "UnclassifiableRowsBlock",
                "StageAndQuarantineCommitAtomically",
                "NoDurableWritesBeforeCommit",
                "CrashLeavesNeitherPartialWrite",
                "MergeBackPartitionAtomicitySafety",
            ),
        ),
        min_distinct_states=14,
    ),
    MergeBackModel(
        "MergeBackLandRollback.tla",
        "MergeBackLandRollback.cfg",
        TLCProperty(
            "formal.merge-back.land-block-rollback",
            (
                "TLC verified blocked LAND rollback restores default's live "
                "node/edge read set and does not merely restore the published "
                "generation pointer."
            ),
            refinement_tests=ROLLBACK_TESTS,
            expected_invariants=(
                "TypeOK",
                "PreLandLiveSetSnapshotCaptured",
                "BlockedLandRestoresDefaultLiveSet",
                "RollbackIncompleteNeverSilent",
                "CleanRollbackDeletesBlockedGeneration",
                "PublishedPointerRestorationIsNotSufficient",
                "LandBlockRollbackSafety",
            ),
        ),
        min_distinct_states=6,
    ),
)
PROPERTIES = tuple(check.property for check in MODEL_CHECKS)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help="directory for TLC logs and formal-report.yaml",
    )
    parser.add_argument(
        "--checker-command",
        default=DEFAULT_CHECKER_COMMAND,
        help="PACT formal binding checker_command represented by this report",
    )
    parser.add_argument(
        "--refinement-test",
        action="append",
        default=[],
        dest="extra_refinement_tests",
        help="additional refinement test ref included on each result row",
    )
    parser.add_argument(
        "--property-id",
        action="append",
        default=[],
        dest="property_ids",
        help="property_id row to emit; repeat to emit multiple bound rows",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=300,
        help="maximum seconds to allow each TLC run",
    )
    args = parser.parse_args(argv)

    selected = _select_model_checks(args.property_ids)
    args.out.mkdir(parents=True, exist_ok=True)

    rows: list[dict] = []
    failures: list[TLCReportResult] = []
    try:
        for check in selected:
            result = run_tlc_formal_report(
                repo_root=REPO_ROOT,
                model_dir=MODEL_DIR,
                model=MODEL_DIR / check.model,
                config=MODEL_DIR / check.config,
                out_dir=args.out / Path(check.model).stem,
                properties=(check.property,),
                expected_properties=(check.property,),
                checker_command=args.checker_command,
                extra_refinement_tests=args.extra_refinement_tests,
                timeout_seconds=args.timeout_seconds,
                min_distinct_states=check.min_distinct_states,
            )
            rows.extend(result.rows)
            if result.run_failed:
                failures.append(result)
    except TLCSetupError as exc:
        print(f"formal-merge-back-safety: {exc}", file=sys.stderr)
        return exc.exit_code

    report_path = args.out / "formal-report.yaml"
    report_path.write_text(
        yaml.safe_dump({"results": rows}, sort_keys=False),
        encoding="utf-8",
    )
    if failures:
        failed = ", ".join(result.stdout_path.parent.name for result in failures)
        print(
            "formal-merge-back-safety: TLC failed for "
            f"{failed}; see logs under {args.out}",
            file=sys.stderr,
        )
        return failures[0].exit_code or 1
    print(f"formal-merge-back-safety: TLC passed; report at {report_path}")
    return 0


def _select_model_checks(property_ids: list[str]) -> tuple[MergeBackModel, ...]:
    if not property_ids:
        return MODEL_CHECKS
    by_id = {check.property.property_id: check for check in MODEL_CHECKS}
    unknown = sorted(set(property_ids) - set(by_id))
    if unknown:
        raise SystemExit(f"unknown property id(s): {', '.join(unknown)}")
    return tuple(by_id[property_id] for property_id in property_ids)


if __name__ == "__main__":
    raise SystemExit(main())
