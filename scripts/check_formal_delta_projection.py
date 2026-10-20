#!/usr/bin/env python3
"""Run the source-sync DeltaProjection TLA+ model with TLC."""
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
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "delta-projection"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_delta_projection.py "
    "--out .scratch/formal/delta-projection"
)
REFINEMENT_TESTS_BY_PROPERTY = {
    "RetractionDoesNotLeaveLiveProvenance": [
        "tests/substrate/source_sync/test_delta_projection.py::" +
        "test_retraction_does_not_leave_live_provenance",
    ],
    "DirtyScopesCoverChangedDependents": [
        "tests/substrate/source_sync/test_dirty_scope_refresh.py::" +
        "test_dirty_scopes_cover_changed_dependents",
        "tests/substrate/source_sync/test_dirty_scope_refresh.py::" +
        "test_unavailable_lookup_falls_back_full_and_preserves_dirty_scope",
    ],
}
PROPERTIES = [
    TLCProperty(
        "RetractionDoesNotLeaveLiveProvenance",
        "TLC verified retracted source records cannot remain live after projection.",
        refinement_tests=tuple(
            REFINEMENT_TESTS_BY_PROPERTY["RetractionDoesNotLeaveLiveProvenance"]
        ),
        expected_invariants=(
            "ProjectionRetractionsAreFaithful",
            "RetractionDoesNotLeaveLiveProvenance",
            "DeltaProjectionContract",
        ),
    ),
    TLCProperty(
        "DirtyScopesCoverChangedDependents",
        "TLC verified dirty-scope planning covers every changed dependent output.",
        refinement_tests=tuple(
            REFINEMENT_TESTS_BY_PROPERTY["DirtyScopesCoverChangedDependents"]
        ),
        expected_invariants=(
            "LookupPreservesCoveredDirtyOutputs",
            "DirtyScopesCoverChangedDependents",
            "NoChangedDependentReused",
            "UnavailableLookupForcesFullFallback",
            "MixedUnavailablePreservesCoveredDirtyScopes",
            "DirtyPlanAccountsForUniverse",
            "DeltaProjectionContract",
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
        default=MODEL_DIR / "DeltaProjection.tla",
        help="TLA+ module path; relative paths resolve under formal/source_sync",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=MODEL_DIR / "DeltaProjection.cfg",
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
            f"formal-delta-projection: {exc}",
            file=sys.stderr,
        )
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-delta-projection: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(f"formal-delta-projection: TLC passed; report at {result.report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
