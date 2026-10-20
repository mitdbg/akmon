#!/usr/bin/env python3
"""Run the runtime replaceable-workflow cutover TLA+ model with TLC."""
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
MODEL_DIR = REPO_ROOT / "formal" / "runtime"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "runtime-cutover"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_runtime_cutover.py "
    "--out .scratch/formal/runtime-cutover"
)
REFINEMENT_TESTS = (
    "tests/substrate/runtime/test_orphan_schedule_cleanup.py::"
    "test_cutover_cancels_exact_replaceable_scheduled_state_matrix",
    "tests/substrate/runtime/test_orphan_schedule_cleanup.py::"
    "test_cutover_refuses_custom_and_started_pending_with_typed_diagnostic",
    "tests/substrate/runtime/test_orphan_schedule_cleanup.py::"
    "test_cutover_cleanup_failure_rolls_back_all_replaceable_rows",
)
PROPERTIES = [
    TLCProperty(
        "ReplaceableCutoverSafety",
        "TLC verified exact replaceable cleanup, protected started PENDING, "
        "fail-closed readiness, rollback safety, and eligible-cutover progress.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=("ReplaceableCutoverSafety",),
        expected_temporal_properties=(
            "EligibleSuccessfulCutoverLeadsToReady",
        ),
    )
]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument(
        "--model", type=Path, default=MODEL_DIR / "ReplaceableCutover.tla"
    )
    parser.add_argument(
        "--config", type=Path, default=MODEL_DIR / "ReplaceableCutover.cfg"
    )
    parser.add_argument("--checker-command", default=DEFAULT_CHECKER_COMMAND)
    parser.add_argument(
        "--refinement-test", action="append", default=[], dest="extra_refinement_tests"
    )
    parser.add_argument(
        "--property-id", action="append", default=[], dest="property_ids"
    )
    parser.add_argument("--timeout-seconds", type=int, default=300)
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
        print(f"formal-runtime-cutover: {exc}", file=sys.stderr)
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-runtime-cutover: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(f"formal-runtime-cutover: TLC passed; report at {result.report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
