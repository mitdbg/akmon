#!/usr/bin/env python3
"""Run the AltheaOptimizerSafety TLA+ model with TLC."""
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
MODEL_DIR = REPO_ROOT / "formal" / "althea"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "althea-optimizer-safety"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_althea_optimizer_safety.py "
    "--out .scratch/formal/althea-optimizer-safety"
)
REFINEMENT_TESTS = (
    "tests/optimizer/test_formal_search.py::"
    "test_formal_checker_accepts_valid_search_result",
    "tests/optimizer/test_formal_search.py::"
    "test_formal_checker_rejects_selected_candidate_outside_admitted_universe",
    "tests/optimizer/test_formal_search.py::"
    "test_formal_checker_rejects_materialization_without_selection",
    "tests/optimizer/test_formal_search.py::"
    "test_formal_checker_rejects_non_beneficial_candidate",
    "tests/optimizer/test_formal_search.py::"
    "test_formal_checker_rejects_inadmissible_ir_score",
    "tests/optimizer/test_formal_search.py::"
    "test_formal_checker_rejects_conflicting_candidates",
    "tests/optimizer/test_formal_search.py::"
    "test_formal_checker_rejects_base_graph_attr_mutation",
    "tests/optimizer/test_formal_search.py::test_optimizer_rejects_base_graph_mutation",
)
PROPERTIES = [
    TLCProperty(
        "AltheaOptimizerSafetyKernel",
        (
            "TLC verified candidate admission, search selection, materialization, "
            "conflict exclusion, eligibility, non-negative cost, and additive "
            "overlay preservation."
        ),
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "TypeOK",
            "SearchCostNonNegative",
            "SelectedCandidatesAreAdmitted",
            "SelectedCandidatesAreEligible",
            "SearchConflictExclusion",
            "NoAdmissionBypass",
            "MaterializationRequiresSearchSelection",
            "BaseNodeAttrsPreserved",
            "BaseEdgeAttrsPreserved",
            "AdditiveOverlayPreservesBase",
            "AltheaOptimizerSafetyKernel",
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
        default=MODEL_DIR / "AltheaOptimizerSafety.tla",
        help="TLA+ module path; relative paths resolve under formal/althea",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=MODEL_DIR / "AltheaOptimizerSafety.cfg",
        help="TLC config path; relative paths resolve under formal/althea",
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
        print(f"formal-althea-optimizer-safety: {exc}", file=sys.stderr)
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-althea-optimizer-safety: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(
        "formal-althea-optimizer-safety: TLC passed; report at "
        f"{result.report_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
