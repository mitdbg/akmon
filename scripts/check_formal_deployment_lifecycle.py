#!/usr/bin/env python3
"""Run the deployment-lifecycle safety and recovery model with TLC."""

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
MODEL_DIR = REPO_ROOT / "formal" / "deployment_lifecycle"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "deployment-lifecycle"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_deployment_lifecycle.py"
)
REFINEMENT_TESTS = ("tests/pact/test_deployment_lifecycle_model_contract.py",)
PROPERTIES = [
    TLCProperty(
        "DeploymentLifecycleSafetyAndRecovery",
        "TLC verified phase-local invalidation, completed-effect replay safety, "
        "preflight-before-effect ordering, uncertain-effect blocking, and clean "
        "terminal service/resource recovery.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "TypeInvariant",
            "NoDisruptiveEffectBeforePreflight",
            "NoMutationBeforePreflight",
            "CompletedPhaseReplayRequiresInvalidation",
            "InputChangeInvalidatesOnlyDependents",
            "CleanTerminalRestoresPriorServiceState",
            "CleanTerminalOwnsNoDisposableResources",
            "UnknownEffectCannotBeClean",
            "CleanupFailureCannotBeClean",
            "CancellationRequiresCompensation",
            "UncertainCompletionRequiresReceipt",
        ),
    ),
]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument(
        "--model",
        type=Path,
        default=MODEL_DIR / "DeploymentLifecycle.tla",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=MODEL_DIR / "DeploymentLifecycle.cfg",
    )
    parser.add_argument("--checker-command", default=DEFAULT_CHECKER_COMMAND)
    parser.add_argument(
        "--refinement-test",
        action="append",
        default=[],
        dest="extra_refinement_tests",
    )
    parser.add_argument(
        "--property-id",
        action="append",
        default=[],
        dest="property_ids",
    )
    parser.add_argument("--timeout-seconds", type=int, default=120)
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
            min_distinct_states=100,
        )
    except TLCSetupError as exc:
        print(f"formal-deployment-lifecycle: {exc}", file=sys.stderr)
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-deployment-lifecycle: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(f"formal-deployment-lifecycle: TLC passed; report at {result.report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
