#!/usr/bin/env python3
"""Run the DerivationParity TLA+ model and its drift counterexample with TLC."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from okg.substrate.formal_tlc import (
    TLCProperty,
    TLCSetupError,
    run_tlc_formal_report,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "derivation_parity"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "derivation-parity"
DEFAULT_MODEL = MODEL_DIR / "DerivationParity.tla"
DEFAULT_CONFIG = MODEL_DIR / "DerivationParity.cfg"
DEFAULT_DRIFT_CONFIG = MODEL_DIR / "DerivationParityDrift.cfg"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_derivation_parity.py "
    "--out .scratch/formal/derivation-parity"
)
REFINEMENT_TESTS = (
    "tests/substrate/test_deployment_maintained_derivation_consumers.py",
    "tests/substrate/test_maintained_derivation_interval_storage.py",
)
SAFETY_PROPERTIES = [
    TLCProperty(
        "IncrementalEqualsBatchForLinearDerivation",
        "TLC verified that, for deterministic linear derivations, the kept "
        "incremental interval-fold (close old contribution, open new) stays "
        "equal to an independent batch recompute over the accumulated delta "
        "stream after every accepted delta.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "TypeOK",
            "BaseIsRecompute",
            "IncrementalEqualsBatchForLinearDerivation",
            "DeterministicLinearSafety",
        ),
    ),
]
DRIFT_PROPERTIES = [
    TLCProperty(
        "DerivationParityDriftCounterexample",
        "TLC produced the expected counterexample when the incremental fold opens "
        "a new interval without closing the old one: kept double-counts the prior "
        "contribution and diverges from the batch recompute via a multi-step "
        "trace, proving the parity invariant detects drift by transition rather "
        "than by an initial-state mismatch.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=("IncrementalEqualsBatchForLinearDerivation",),
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
        default=DEFAULT_MODEL,
        help="TLA+ module path; relative paths resolve under formal/derivation_parity",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="positive TLC config path; relative paths resolve under formal/derivation_parity",
    )
    parser.add_argument(
        "--drift-config",
        type=Path,
        default=DEFAULT_DRIFT_CONFIG,
        help="counterexample TLC config path; relative paths resolve under formal/derivation_parity",
    )
    parser.add_argument(
        "--checker-command",
        default=DEFAULT_CHECKER_COMMAND,
        help="PACT formal binding checker_command represented by this report",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=120,
        help="maximum seconds per TLC run",
    )
    args = parser.parse_args(argv)

    out_dir = args.out.resolve()
    try:
        safety = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=args.model,
            config=args.config,
            out_dir=out_dir / "safety",
            properties=SAFETY_PROPERTIES,
            expected_properties=SAFETY_PROPERTIES,
            checker_command=args.checker_command,
            timeout_seconds=args.timeout_seconds,
        )
        if safety.run_failed:
            print(
                "formal-derivation-parity: safety TLC failed; see "
                f"{safety.stdout_path} and {safety.stderr_path}",
                file=sys.stderr,
            )
            return safety.exit_code or 1
        drift = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=args.model,
            config=args.drift_config,
            out_dir=out_dir / "drift",
            properties=DRIFT_PROPERTIES,
            expected_properties=DRIFT_PROPERTIES,
            checker_command=args.checker_command,
            expect_counterexample=True,
            timeout_seconds=args.timeout_seconds,
        )
    except TLCSetupError as exc:
        print(f"formal-derivation-parity: {exc}", file=sys.stderr)
        return exc.exit_code
    if drift.run_failed:
        print(
            "formal-derivation-parity: drift counterexample check failed; see "
            f"{drift.stdout_path} and {drift.stderr_path}",
            file=sys.stderr,
        )
        return drift.exit_code or 1

    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "formal-report.yaml"
    report_path.write_text(
        yaml.safe_dump(
            {"results": [*safety.rows, *drift.rows]},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    print(f"formal-derivation-parity: TLC passed; report at {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
