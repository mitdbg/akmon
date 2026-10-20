#!/usr/bin/env python3
"""Run the typed-applicability safety model and emit strict PACT evidence."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from okg.substrate.formal_tlc import TLCProperty, TLCSetupError, run_tlc_formal_report


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "consistency"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "typed-applicability"
DEFAULT_MODEL = MODEL_DIR / "TypedApplicability.tla"
DEFAULT_CONFIG = MODEL_DIR / "TypedApplicability.cfg"
DEFAULT_CHECKER_COMMAND = (
    "uv run python scripts/check_formal_typed_applicability.py "
    "--out .scratch/formal/typed-applicability"
)
REFINEMENT_TESTS = (
    "tests/substrate/consistency/test_typed_applicability.py",
)
PROPERTIES = [
    TLCProperty(
        "TypedApplicabilitySafety",
        "TLC exhaustively verified the two-axis lower/upper-bound outcome table, "
        "unknown-not-definite, upper-disjoint-no-finding, partial-overlap-only-"
        "possible, explicit-unbounded definite capability, conjunctive axes, and "
        "complete legacy heading adapter equivalence including known-empty.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "TypeOK",
            "UnknownNeverDefinite",
            "UpperDisjointNeverFinds",
            "LowerDisjointUpperOverlapIsPossible",
            "UnboundedKnownIsDefiniteCapable",
            "ConjunctiveAcrossAxes",
            "V1AdapterEquivalent",
            "UnboundedWitnessExists",
            "TypedApplicabilitySafety",
        ),
    )
]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args(argv)

    try:
        result = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=DEFAULT_MODEL,
            config=DEFAULT_CONFIG,
            out_dir=args.out.resolve(),
            properties=PROPERTIES,
            expected_properties=PROPERTIES,
            checker_command=DEFAULT_CHECKER_COMMAND,
            timeout_seconds=args.timeout_seconds,
        )
    except TLCSetupError as exc:
        print(f"formal-typed-applicability: {exc}", file=sys.stderr)
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-typed-applicability: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1

    args.out.mkdir(parents=True, exist_ok=True)
    report_path = args.out / "formal-report.yaml"
    report_path.write_text(
        yaml.safe_dump({"results": result.rows}, sort_keys=False),
        encoding="utf-8",
    )
    print(f"formal-typed-applicability: TLC passed; report at {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
