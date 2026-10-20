#!/usr/bin/env python3
"""Run the issue-746 typed outcome TLC model and emit PACT evidence."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from okg.substrate.formal_tlc import TLCProperty, TLCSetupError, run_tlc_formal_report

ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = ROOT / "formal" / "consistency"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=ROOT / ".scratch/formal/typed-outcome")
    parser.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args(argv)
    props = [TLCProperty(
        "TypedOutcomeSafety",
        "TLC exhaustively checked the five-category refinement table, semantic-vs-"
        "applicability uncertainty separation, unresolved-key category, actionability, "
        "and explicit legacy adapter.",
        refinement_tests=("tests/substrate/consistency/test_consistency_outcome_contract.py",),
        expected_invariants=(
            "TypeOK", "UnknownSemanticNeverConflict", "UnknownApplicabilityDistinct",
            "UnresolvedKeyNotComparable", "ConflictRequiresProof", "AdapterExplicit",
            "TypedOutcomeSafety",
        ),
    )]
    try:
        result = run_tlc_formal_report(
            repo_root=ROOT,
            model_dir=MODEL_DIR,
            model=MODEL_DIR / "TypedOutcomeContract.tla",
            config=MODEL_DIR / "TypedOutcomeContract.cfg",
            out_dir=args.out.resolve(),
            properties=props,
            expected_properties=props,
            checker_command="uv run python scripts/check_formal_typed_outcome.py --out .scratch/formal/typed-outcome",
            timeout_seconds=args.timeout_seconds,
        )
    except TLCSetupError as exc:
        print(f"formal-typed-outcome: {exc}", file=sys.stderr)
        return exc.exit_code
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "formal-report.yaml").write_text(
        yaml.safe_dump({"results": result.rows}, sort_keys=False), encoding="utf-8"
    )
    if result.run_failed:
        print(f"formal-typed-outcome: TLC failed; see {result.stdout_path}", file=sys.stderr)
        return result.exit_code or 1
    print(f"formal-typed-outcome: TLC passed; report at {args.out / 'formal-report.yaml'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
