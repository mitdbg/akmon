#!/usr/bin/env python3
"""Run the PACT mutation-safety TLA+ model with TLC."""
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
MODEL_DIR = REPO_ROOT / "formal" / "pact"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "pact-mutation-safety"
DEFAULT_CHECKER_COMMAND = (
    "uv run python scripts/check_formal_pact_mutation.py "
    "--out .scratch/formal/pact-mutation-safety"
)
REFINEMENT_TESTS_BY_PROPERTY = {
    "mutation-safety.no-lost-completed-mutation": [
        "tests/pact/test_cli.py::test_apply_edit_serializes_parallel_manifest_writes",
    ],
    "mutation-safety.plan-effect-at-most-once": [
        "tests/pact/test_cli.py::test_edit_plan_apply_serializes_stale_replay_check",
    ],
}
PROPERTIES = [
    TLCProperty(
        "mutation-safety.no-lost-completed-mutation",
        "TLC verified completed manifest mutations are not lost.",
        refinement_tests=tuple(
            REFINEMENT_TESTS_BY_PROPERTY[
                "mutation-safety.no-lost-completed-mutation"
            ]
        ),
        expected_invariants=("NoLostCompletedMutation",),
    ),
    TLCProperty(
        "mutation-safety.plan-effect-at-most-once",
        "TLC verified concurrent edit-plan replay has at most one effect.",
        refinement_tests=tuple(
            REFINEMENT_TESTS_BY_PROPERTY["mutation-safety.plan-effect-at-most-once"]
        ),
        expected_invariants=("PlanEffectAtMostOnce",),
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
        default=MODEL_DIR / "MutationSafety.tla",
        help="TLA+ module path; relative paths resolve under formal/pact",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=MODEL_DIR / "MutationSafety.cfg",
        help="TLC config path; relative paths resolve under formal/pact",
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
            f"formal-pact-mutation: {exc}",
            file=sys.stderr,
        )
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-pact-mutation: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(f"formal-pact-mutation: TLC passed; report at {result.report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
