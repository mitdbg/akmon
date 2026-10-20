#!/usr/bin/env python3
"""Run the targeted-enricher-backfill TLA+ model with TLC."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from okg.substrate.formal_tlc import (
    TLCProperty,
    TLCSetupError,
    run_tlc_formal_report,
    select_properties,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "enricher_backfill"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "targeted-enricher-backfill"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_enricher_backfill.py "
    "--out .scratch/formal/targeted-enricher-backfill"
)
REFINEMENT_TESTS_BY_PROPERTY = {
    "TargetedBackfillIsolation": [
        "tests/substrate/cli/test_enricher_backfill_cli.py::" +
        "test_enricher_backfill_cli_selects_one_registered_enricher",
        "tests/substrate/cli/test_enricher_backfill_cli.py::" +
        "test_enricher_backfill_cli_rejects_duplicate_names_before_writes",
        "tests/substrate/test_enricher_backfill.py::" +
        "test_backfill_rejects_unmarked_enricher_before_writes",
    ],
    "ChunkProgressRetrySafety": [
        "tests/substrate/test_enricher_backfill.py::" +
        "test_backfill_processes_limit_in_resumable_chunks",
        "tests/substrate/test_enricher_backfill.py::" +
        "test_backfill_rejected_admission_does_not_advance_progress",
        "tests/substrate/test_enricher_backfill.py::" +
        "test_backfill_rejects_same_source_out_of_chunk_edges",
    ],
    "PinnedFrontierSafety": [
        "tests/substrate/test_enricher_backfill.py::" +
        "test_backfill_progress_uses_pinned_start_frontier",
    ],
}
EXPECTED_INVARIANTS = (
    "TypeOK",
    "TargetedBackfillIsolation",
    "ChunkProgressRetrySafety",
    "PinnedFrontierSafety",
    "TargetedEnricherBackfillContract",
)
PROPERTIES = [
    TLCProperty(
        "TargetedBackfillIsolation",
        (
            "TLC verified targeted backfill invokes only the selected enricher "
            "and no unrelated sources, enrichers, views, lenses, or consistency "
            "surfaces; missing, ambiguous, or non-chunk-safe selection writes "
            "nothing."
        ),
        refinement_tests=tuple(
            REFINEMENT_TESTS_BY_PROPERTY["TargetedBackfillIsolation"]
        ),
        expected_invariants=EXPECTED_INVARIANTS,
    ),
    TLCProperty(
        "ChunkProgressRetrySafety",
        (
            "TLC verified chunk bounds, per-input completion, rejected-admission "
            "and out-of-chunk retry safety, and global progress advancing only "
            "after a drained worklist with no failed inputs."
        ),
        refinement_tests=tuple(
            REFINEMENT_TESTS_BY_PROPERTY["ChunkProgressRetrySafety"]
        ),
        expected_invariants=EXPECTED_INVARIANTS,
    ),
    TLCProperty(
        "PinnedFrontierSafety",
        (
            "TLC verified normal publish progress advances only to the pinned "
            "input frontier, so concurrent inputs stay outside the command work "
            "and remain visible to later incremental work."
        ),
        refinement_tests=tuple(
            REFINEMENT_TESTS_BY_PROPERTY["PinnedFrontierSafety"]
        ),
        expected_invariants=EXPECTED_INVARIANTS,
    ),
]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help="directory for TLC logs and formal-report.yaml",
    )
    parser.add_argument(
        "--model",
        type=Path,
        default=MODEL_DIR / "TargetedEnricherBackfill.tla",
        help="TLA+ module path; relative paths resolve under formal/enricher_backfill",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=MODEL_DIR / "TargetedEnricherBackfill.cfg",
        help="TLC config path; relative paths resolve under formal/enricher_backfill",
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
        help="maximum seconds to allow TLC to run",
    )
    args = parser.parse_args(argv)

    try:
        result = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=args.model,
            config=args.config,
            out_dir=args.out,
            properties=select_properties(PROPERTIES, args.property_ids),
            expected_properties=PROPERTIES,
            checker_command=args.checker_command,
            extra_refinement_tests=args.extra_refinement_tests,
            timeout_seconds=args.timeout_seconds,
        )
    except TLCSetupError as exc:
        print(f"formal-enricher-backfill: {exc}", file=sys.stderr)
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-enricher-backfill: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(
        "formal-enricher-backfill: TLC passed; report at "
        f"{result.report_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
