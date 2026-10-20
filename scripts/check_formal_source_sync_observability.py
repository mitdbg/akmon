#!/usr/bin/env python3
"""Run the source-sync observability TLA+ model with TLC."""
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
MODEL_DIR = REPO_ROOT / "formal" / "source_sync"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "source-sync-observability"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_source_sync_observability.py "
    "--out .scratch/formal/source-sync-observability "
    "--property-id AmplificationUsesMeasuredTerms"
)
REFINEMENT_TESTS_BY_PROPERTY = {
    "AmplificationUsesMeasuredTerms": [
        "tests/substrate/source_sync/test_source_sync_measurements.py::" +
        "test_amplification_uses_measured_terms",
        "tests/substrate/source_sync/test_source_sync_measurements.py::" +
        "test_measurement_gaps_surface_as_failed_observability",
        "tests/substrate/source_sync/test_source_sync_measurements.py::" +
        "test_harness_declared_expected_delta_is_valid_observability_input",
        "tests/substrate/source_sync/test_source_sync_measurements.py::" +
        "test_unknown_expected_delta_source_fails_validation",
    ],
}
PROPERTIES = [
    TLCProperty(
        "AmplificationUsesMeasuredTerms",
        "TLC verified source-sync observability readiness depends on pre-run expected deltas, measured work terms, read-side visibility, and warning/error severity for gaps.",
        refinement_tests=tuple(
            REFINEMENT_TESTS_BY_PROPERTY["AmplificationUsesMeasuredTerms"]
        ),
        expected_invariants=(
            "AmplificationUsesMeasuredTerms",
            "MeasurementGapsSurfaceAsWarningsOrErrors",
            "SelfConfirmingExpectedDeltaNotReady",
            "OverAmplificationWarnsOrBlocks",
            "SourceSyncObservabilityContract",
        ),
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
        default=MODEL_DIR / "SourceSyncObservability.tla",
        help="TLA+ module path; relative paths resolve under formal/source_sync",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=MODEL_DIR / "SourceSyncObservability.cfg",
        help="TLC config path; relative paths resolve under formal/source_sync",
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
        print(
            f"formal-source-sync-observability: {exc}",
            file=sys.stderr,
        )
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-source-sync-observability: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(
        "formal-source-sync-observability: TLC passed; report at "
        f"{result.report_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
