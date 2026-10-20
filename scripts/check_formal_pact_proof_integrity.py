#!/usr/bin/env python3
"""Run the PACT proof-integrity TLA+ safety model with TLC."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from okg.substrate.formal_tlc import TLCProperty, TLCSetupError, run_tlc_formal_report


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "pact"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "pact-proof-integrity"
DEFAULT_MODEL = MODEL_DIR / "PactProofIntegrity.tla"
DEFAULT_CONFIG = MODEL_DIR / "PactProofIntegrity.cfg"
REFINEMENT_TESTS = (
    "tests/pact/test_acceptance_arm_binding.py::test_conjunctive_arm_can_bind_by_one_distinct_evidence_type",
    "tests/pact/test_acceptance_arm_binding.py::test_three_arms_cannot_share_only_two_witness_domains",
    "tests/pact/test_integration_acceptance_validation.py::test_unsatisfied_owner_blocks_program_completion",
    "tests/pact/test_integration_acceptance_validation.py::test_satisfied_child_owner_passes",
    "tests/pact/test_slow_verification_commands.py::test_slow_file_without_marker_override_is_refused",
    "tests/pact/test_slow_verification_commands.py::test_explicit_slow_marker_override_is_accepted",
    "tests/pact/test_slow_verification_commands.py::test_unattested_historical_row_is_visible_but_does_not_satisfy",
    "tests/pact/test_slow_verification_commands.py::test_exact_junit_identity_satisfies_the_named_test",
)
PROPERTY = TLCProperty(
    "PactProofIntegrity",
    (
        "TLC verified completion requires a distinct witness for every "
        "acceptance arm, proven integration owners, and executed named tests."
    ),
    refinement_tests=REFINEMENT_TESTS,
    expected_invariants=(
        "TypeOK",
        "CompletionRequiresBoundArms",
        "CompletionRequiresIntegrationOwner",
        "CompletionRequiresNamedTestExecution",
        "ProofIntegrity",
    ),
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--timeout-seconds", type=int, default=300)
    args = parser.parse_args(argv)
    checker_command = (
        "uv run --extra dev python scripts/check_formal_pact_proof_integrity.py "
        f"--out {args.out}"
    )
    try:
        result = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=DEFAULT_MODEL,
            config=DEFAULT_CONFIG,
            out_dir=args.out,
            properties=[PROPERTY],
            expected_properties=[PROPERTY],
            checker_command=checker_command,
            timeout_seconds=args.timeout_seconds,
        )
    except TLCSetupError as exc:
        print(f"formal-pact-proof-integrity: {exc}", file=sys.stderr)
        return exc.exit_code
    if result.run_failed:
        print(
            f"formal-pact-proof-integrity: TLC failed; see {result.stdout_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(f"formal-pact-proof-integrity: TLC passed; report at {result.report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
