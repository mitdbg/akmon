#!/usr/bin/env python3
"""Model-check deployment apply safety and non-vacuity witnesses."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from okg.substrate.formal_tlc import TLCProperty, TLCSetupError, run_tlc_formal_report


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "runtime"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "deployment-apply"
DEFAULT_COMMAND = (
    "uv run --extra dev python scripts/check_formal_deployment_apply.py "
    "--out .scratch/formal/deployment-apply"
)
REFINEMENT = ("tests/substrate/cli/test_deployment_apply.py",)
SAFETY = TLCProperty(
    "DeploymentApplySafety",
    "TLC verified ordered digest-bound receipts, single registration, exact worker acknowledgment, and honest readiness.",
    refinement_tests=REFINEMENT,
    expected_invariants=(
        "OrderedStages", "NoStaleReceipt", "NoStaleMutation",
        "NoDuplicateSourceRegistration", "ExactWorkerAck",
        "FailureNeverReady", "NoOpMeansConverged", "DeploymentApplySafety",
    ),
)
WITNESSES = (
    (
        "stale-plan", "DeploymentApplyStalePlanCounterexample.cfg",
        TLCProperty("DeploymentApplyStalePlanWitness", "Expected stale-plan counterexample.", refinement_tests=REFINEMENT, expected_invariants=("NoStaleMutation",)),
    ),
    (
        "skipped-stage", "DeploymentApplySkippedStageCounterexample.cfg",
        TLCProperty("DeploymentApplySkippedStageWitness", "Expected skipped-stage counterexample.", refinement_tests=REFINEMENT, expected_invariants=("OrderedStages",)),
    ),
    (
        "duplicate-action", "DeploymentApplyDuplicateActionCounterexample.cfg",
        TLCProperty("DeploymentApplyDuplicateActionWitness", "Expected duplicate source-action counterexample.", refinement_tests=REFINEMENT, expected_invariants=("NoDuplicateSourceRegistration",)),
    ),
    (
        "false-worker-ack", "DeploymentApplyFalseWorkerAckCounterexample.cfg",
        TLCProperty("DeploymentApplyFalseWorkerAckWitness", "Expected false worker-ack counterexample.", refinement_tests=REFINEMENT, expected_invariants=("ExactWorkerAck",)),
    ),
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args(argv)
    try:
        safety = run_tlc_formal_report(
            repo_root=REPO_ROOT, model_dir=MODEL_DIR,
            model=MODEL_DIR / "DeploymentApply.tla",
            config=MODEL_DIR / "DeploymentApply.cfg",
            out_dir=args.out / "safety", properties=[SAFETY],
            expected_properties=[SAFETY], checker_command=DEFAULT_COMMAND,
            timeout_seconds=args.timeout_seconds, min_distinct_states=10,
        )
        if safety.run_failed:
            print(f"formal-deployment-apply: safety failed; see {safety.stdout_path}", file=sys.stderr)
            return safety.exit_code or 1
        rows = list(safety.rows)
        for label, config_name, prop in WITNESSES:
            result = run_tlc_formal_report(
                repo_root=REPO_ROOT, model_dir=MODEL_DIR,
                model=MODEL_DIR / "DeploymentApply.tla",
                config=MODEL_DIR / config_name,
                out_dir=args.out / label, properties=[prop],
                expected_properties=[prop], checker_command=DEFAULT_COMMAND,
                expect_counterexample=True,
                timeout_seconds=args.timeout_seconds,
            )
            if result.run_failed:
                print(f"formal-deployment-apply: {label} failed; see {result.stdout_path}", file=sys.stderr)
                return result.exit_code or 1
            rows.extend(result.rows)
    except TLCSetupError as exc:
        print(f"formal-deployment-apply: {exc}", file=sys.stderr)
        return exc.exit_code
    args.out.mkdir(parents=True, exist_ok=True)
    report = args.out / "formal-report.yaml"
    report.write_text(yaml.safe_dump({"results": rows}, sort_keys=False), encoding="utf-8")
    print(f"formal-deployment-apply: TLC passed; report at {report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
