#!/usr/bin/env python3
"""Run the source-sync SourceDeltaContract TLA+ model with TLC."""
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
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "source-delta-contract"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_source_delta_contract.py "
    "--out .scratch/formal/source-delta-contract"
)
REFINEMENT_TESTS_BY_PROPERTY = {
    "TombstonesLandCanonical": [
        "tests/substrate/source_sync/test_delta_batch_contract.py::" +
        "test_record_identity_is_stable",
        "tests/substrate/test_record_set_canonical_retractions.py::" +
        "test_disappeared_aliased_record_retracts_the_canonical_node",
    ],
    "RetractionsAreComplete": [
        "tests/substrate/source_sync/test_delta_batch_contract.py::" +
        "test_retractions_are_complete",
        "tests/substrate/test_source_runner_delta_contract.py::" +
        "test_readiness_source_emits_delta_contract",
    ],
    "ChurnKeepsRecordLive": [
        "tests/substrate/test_record_set_canonical_retractions.py::" +
        "test_alias_churn_within_surviving_record_keeps_node_live",
    ],
}
EXPECTED_INVARIANTS = (
    "TypeOK",
    "TombstonesLandCanonical",
    "RetractionsAreComplete",
    "ChurnKeepsRecordLive",
    "SourceDeltaContract",
)
PROPERTIES = [
    TLCProperty(
        "TombstonesLandCanonical",
        "TLC verified synthesized retractions are canonicalized at the flush, so tombstones land under the stable record identity the graph holds (never a raw alias no-op).",
        refinement_tests=tuple(
            REFINEMENT_TESTS_BY_PROPERTY["TombstonesLandCanonical"]
        ),
        expected_invariants=EXPECTED_INVARIANTS,
    ),
    TLCProperty(
        "ChurnKeepsRecordLive",
        "TLC verified alias churn within a surviving record leaves the canonical node live: retractions flush before the run's inserts, so the insert is the latest fact.",
        refinement_tests=tuple(
            REFINEMENT_TESTS_BY_PROPERTY["ChurnKeepsRecordLive"]
        ),
        expected_invariants=EXPECTED_INVARIANTS,
    ),
    TLCProperty(
        "RetractionsAreComplete",
        "TLC verified accepted complete source deltas include every explicit or scope-derived tombstone in the emitted retraction set.",
        refinement_tests=tuple(
            REFINEMENT_TESTS_BY_PROPERTY["RetractionsAreComplete"]
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
        default=MODEL_DIR / "SourceDeltaContract.tla",
        help="TLA+ module path; relative paths resolve under formal/source_sync",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=MODEL_DIR / "SourceDeltaContract.cfg",
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
            f"formal-source-delta-contract: {exc}",
            file=sys.stderr,
        )
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-source-delta-contract: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(
        "formal-source-delta-contract: TLC passed; report at "
        f"{result.report_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
