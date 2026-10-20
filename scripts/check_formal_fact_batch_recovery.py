#!/usr/bin/env python3
"""Run the FactBatch recovery TLA+ model and emit strict PACT evidence."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from okg.substrate.formal_tlc import (
    TLCProperty,
    TLCSetupError,
    run_tlc_formal_report,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "fact_batch_recovery"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "fact-batch-recovery"
PROPERTY = TLCProperty(
    "formal.producer-batch.recovery",
    (
        "TLC verified marker/receipt ordering, prepared and compensating "
        "terminal blocking, compensation-before-failure, and "
        "Publisher-only visible state."
    ),
    refinement_tests=("tests/substrate/fact_batch/test_recovery.py",),
    expected_invariants=(
        "MarkerNeverAhead",
        "TerminalAcceptanceSafe",
        "TerminalFailureCompensated",
        "PublishedOnlyAfterAcceptance",
        "PreparedBlocksTerminal",
        "CompensatingBlocksTerminal",
    ),
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args(argv)
    checker_command = (
        "uv run --extra dev python scripts/check_formal_fact_batch_recovery.py "
        f"--out {args.out}"
    )
    try:
        result = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=MODEL_DIR / "FactBatchRecovery.tla",
            config=MODEL_DIR / "FactBatchRecovery.cfg",
            out_dir=args.out,
            properties=[PROPERTY],
            expected_properties=[PROPERTY],
            checker_command=checker_command,
            timeout_seconds=args.timeout_seconds,
            min_distinct_states=12,
        )
    except TLCSetupError as exc:
        print(f"formal-fact-batch-recovery: {exc}", file=sys.stderr)
        return exc.exit_code
    if result.run_failed:
        print(
            f"formal-fact-batch-recovery: failed; see {result.stdout_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(
        "formal-fact-batch-recovery: passed; report at "
        f"{result.report_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
