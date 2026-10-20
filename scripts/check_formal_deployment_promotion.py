#!/usr/bin/env python3
"""Run the deployment-promotion TLA+ model with TLC."""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

from okg.substrate.formal_tlc import (
    TLCProperty,
    TLCSetupError,
    find_tla2tools_jar,
    run_tlc_formal_report,
    select_properties,
    tlc_jvm_args,
    tlc_workers,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "deployment_promotion"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "deployment-promotion"
DEFAULT_CHECKER_COMMAND = (
    "java -cp .tools/tla2tools.jar tlc2.TLC -config "
    "formal/deployment_promotion/DeploymentPromotion.cfg "
    "formal/deployment_promotion/DeploymentPromotion.tla"
)
REFINEMENT_TESTS = (
    "tests/pact/test_deployment_promotion_model_contract.py::"
    "test_backup_is_created_only_after_fencing_and_drain",
    "tests/pact/test_deployment_promotion_model_contract.py::"
    "test_invalid_backup_has_explicit_fail_closed_recovery",
    "tests/pact/test_deployment_promotion_model_contract.py::"
    "test_candidate_activation_requires_verified_backup",
    "tests/pact/test_deployment_promotion_model_contract.py::"
    "test_all_production_mutation_boundaries_are_explicit",
    "tests/pact/test_deployment_promotion_model_contract.py::"
    "test_rollback_restores_database_before_previous_release",
    "tests/pact/test_deployment_promotion_model_contract.py::"
    "test_recovery_property_has_load_bearing_fairness",
    "tests/pact/test_deployment_promotion_model_contract.py::"
    "test_tlc_config_checks_all_named_safety_invariants",
    "tests/pact/test_deployment_promotion_model_contract.py::"
    "test_removed_guards_have_isolated_counterexample_witnesses",
    "tests/pact/test_deployment_promotion_model_contract.py::"
    "test_witness_runner_rejects_named_text_with_setup_exit",
    "tests/pact/test_deployment_promotion_model_contract.py::"
    "test_witness_runner_records_byte_timeout_as_failure",
)
WITNESSES = (
    (
        "backup-guard",
        "DeploymentPromotionBackupGuardWitness.cfg",
        "MutationRequiresVerifiedPostFenceBackup",
    ),
    (
        "lock-guard",
        "DeploymentPromotionLockGuardWitness.cfg",
        "OneActivePromotion",
    ),
    (
        "writer-ready-guard",
        "DeploymentPromotionWriterReadyGuardWitness.cfg",
        "RollbackReopensOnlyPreviousState",
    ),
    (
        "restore-order-guard",
        "DeploymentPromotionRestoreOrderGuardWitness.cfg",
        "PreviousArtifactRequiresRestoredDatabase",
    ),
)
PROPERTIES = [
    TLCProperty(
        "DeploymentPromotionSafety",
        "TLC verified serialized staging-to-production promotion, post-fence "
        "backup validation, boundary rollback, fail-closed recovery, and fair "
        "recovery convergence.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "TypeInvariant",
            "NoProductionMutationBeforeStagingReady",
            "OneActivePromotion",
            "MutationRequiresVerifiedPostFenceBackup",
            "MutationBoundaryRequiresLockAndFence",
            "WritersOpenOnlyInStableState",
            "CompletedUsesApprovedArtifact",
            "RollbackReopensOnlyPreviousState",
            "PreviousArtifactRequiresRestoredDatabase",
            "InvalidBackupNeverMutates",
            "InvalidBackupRetainsFenceUntilReady",
            "RecoveryBlockedKeepsWritersFenced",
        ),
        expected_temporal_properties=(
            "RecoveryConverges",
            "RecoveryWithoutInjectedFailureSucceeds",
        ),
    ),
]


def _run_witnesses(*, out_dir: Path, timeout_seconds: int) -> int:
    jar = find_tla2tools_jar(REPO_ROOT)
    java = shutil.which("java")
    if jar is None or java is None:
        print(
            "formal-deployment-promotion: Java and tla2tools.jar are required",
            file=sys.stderr,
        )
        return 2

    out_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, object]] = []
    for name, config_name, expected_invariant in WITNESSES:
        metadir = out_dir / f"tlc-meta-{name}-{uuid.uuid4().hex}"
        # No -workers here, as before: TLC defaults to a single worker, which
        # is already machine-friendly. Only the heap needed bounding.
        command = [
            java,
            *tlc_jvm_args(tlc_workers()),
            "-cp",
            str(jar),
            "tlc2.TLC",
            "-config",
            str(MODEL_DIR / config_name),
            "-metadir",
            str(metadir),
            str(MODEL_DIR / "DeploymentPromotionCounterexample.tla"),
        ]
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
            output = completed.stdout + completed.stderr
            matched = (
                f"Invariant {expected_invariant} is violated" in output
                and completed.returncode == 12
            )
            exit_code = completed.returncode
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout.decode(errors="replace") if isinstance(exc.stdout, bytes) else (exc.stdout or "")
            stderr = exc.stderr.decode(errors="replace") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
            output = stdout + stderr
            matched = False
            exit_code = 124
        log_path = out_dir / f"{name}.log"
        log_path.write_text(output, encoding="utf-8")
        results.append(
            {
                "name": name,
                "config": config_name,
                "expected_invariant": expected_invariant,
                "matched": matched,
                "exit_code": exit_code,
                "artifact": str(log_path),
            }
        )

    ok = all(result["matched"] is True for result in results)
    summary_path = out_dir / "witnesses.json"
    summary_path.write_text(
        json.dumps({"ok": ok, "witnesses": results}, indent=2) + "\n",
        encoding="utf-8",
    )
    if not ok:
        print(
            f"formal-deployment-promotion: witness check failed; see {summary_path}",
            file=sys.stderr,
        )
        return 1
    print(f"formal-deployment-promotion: all witnesses passed; report at {summary_path}")
    return 0


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
        default=MODEL_DIR / "DeploymentPromotion.tla",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=MODEL_DIR / "DeploymentPromotion.cfg",
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
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument(
        "--witnesses-only",
        action="store_true",
        help="run the retained guard-removal counterexamples and emit witnesses.json",
    )
    args = parser.parse_args(argv)

    if args.witnesses_only:
        return _run_witnesses(
            out_dir=args.out,
            timeout_seconds=args.timeout_seconds,
        )

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
        print(f"formal-deployment-promotion: {exc}", file=sys.stderr)
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-deployment-promotion: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(f"formal-deployment-promotion: TLC passed; report at {result.report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
