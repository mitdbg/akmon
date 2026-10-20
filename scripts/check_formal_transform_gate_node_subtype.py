#!/usr/bin/env python3
"""Run the TransformGateNodeSubtypeScope TLA+ model with TLC."""
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
MODEL_DIR = REPO_ROOT / "formal" / "consistency"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "transform-gate-node-subtype"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_transform_gate_node_subtype.py "
    "--out .scratch/formal/transform-gate-node-subtype"
)
REFINEMENT_TESTS = (
    "tests/substrate/test_chronos_narrowing_gate.py::"
    "test_chronos_gate_blocks_node_with_unknown_subtype",
    "tests/substrate/test_chronos_narrowing_gate.py::"
    "test_chronos_gate_passes_known_node_subtype",
    "tests/substrate/test_fork_publish.py::"
    "test_fork_first_publish_blocks_unknown_node_subtype",
    "tests/substrate/test_fork_publish.py::"
    "test_fork_first_publish_blocks_retyped_inherited_node_to_unknown_subtype",
)
PROPERTIES = [
    TLCProperty(
        "TransformGateNodeSubtypeScopeSoundness",
        "TLC verified transform gate node-subtype scope selection and invalid-node blocking.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "CandidateNodeSelectionMatchesDirtyScope",
            "PreviousCleanDeltaCoversInvalidNodes",
            "InvalidDirtyNodeCannotPublish",
            "UnsafeDirtyScopeFallsBackFull",
            "NoInvalidPostNodePublishes",
            "CleanPostNodesPass",
            "TransformGateNodeSubtypeScopeSoundness",
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
        default=MODEL_DIR / "TransformGateNodeSubtypeScope.tla",
        help="TLA+ module path; relative paths resolve under formal/consistency",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=MODEL_DIR / "TransformGateNodeSubtypeScope.cfg",
        help="TLC config path; relative paths resolve under formal/consistency",
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
            f"formal-transform-gate-node-subtype: {exc}",
            file=sys.stderr,
        )
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-transform-gate-node-subtype: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(
        "formal-transform-gate-node-subtype: TLC passed; report at "
        f"{result.report_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
