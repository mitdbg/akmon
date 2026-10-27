#!/usr/bin/env python3
"""Run the deployment-backup policy model with TLC."""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from okg.substrate.formal_tlc import (
    TLCProperty,
    TLCSetupError,
    run_tlc_formal_report,
    select_properties,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "deployment_backup_policy"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "deployment-backup-policy"
DEFAULT_CHECKER_COMMAND = (
    "java -cp .tools/tla2tools.jar tlc2.TLC -config "
    "formal/deployment_backup_policy/DeploymentBackupPolicy.cfg "
    "formal/deployment_backup_policy/DeploymentBackupPolicy.tla"
)
REFINEMENT_TESTS = (
    "tests/pact/test_deployment_backup_policy_model_contract.py::"
    "test_current_archive_is_required_before_mutation",
    "tests/pact/test_deployment_backup_policy_model_contract.py::"
    "test_destructive_and_production_paths_require_full_restore",
    "tests/pact/test_deployment_backup_policy_model_contract.py::"
    "test_no_physical_rollback_requires_closed_effect_inventory",
    "tests/pact/test_deployment_backup_policy_model_contract.py::"
    "test_every_effect_inventory_field_is_load_bearing",
    "tests/pact/test_deployment_backup_policy_model_contract.py::"
    "test_every_disaster_recovery_field_is_load_bearing",
    "tests/pact/test_deployment_backup_policy_model_contract.py::"
    "test_missing_dr_receipt_refuses_instead_of_dumping",
    "tests/pact/test_deployment_backup_policy_model_contract.py::"
    "test_schema_or_policy_changes_force_current_restore",
    "tests/pact/test_deployment_backup_policy_model_contract.py::"
    "test_unknown_or_unreadable_plans_refuse",
    "tests/pact/test_deployment_backup_policy_model_contract.py::"
    "test_external_effects_force_current_restore",
    "tests/pact/test_deployment_backup_policy_model_contract.py::"
    "test_external_effects_never_take_the_no_physical_rollback_path",
    "tests/pact/test_deployment_backup_policy_model_contract.py::"
    "test_external_effect_is_a_known_physical_effect_in_the_model",
    "tests/pact/test_deployment_backup_policy_model_contract.py::"
    "test_effect_classification_partitions_every_field",
    "tests/pact/test_deployment_backup_policy_model_contract.py::"
    "test_candidate_planner_must_be_trusted",
    "tests/pact/test_deployment_backup_policy_model_contract.py::"
    "test_target_plan_drift_refuses",
    "tests/pact/test_deployment_backup_policy_model_contract.py::"
    "test_malformed_core_policy_booleans_refuse_mutation",
    "tests/pact/test_deployment_backup_policy_model_contract.py::"
    "test_config_checks_physical_rollback_invariants",
)
PROPERTIES = [
    TLCProperty(
        "DeploymentPhysicalRollbackAdmissionSafety",
        "TLC verified that physical and externally-reaching plans require a "
        "current restored archive, closed routine plans require independent "
        "disaster-recovery proof, and untrusted, unreadable, or drifted plans "
        "never mutate.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "TypeInvariant",
            "MutationRequiresRecoveryEvidence",
            "CurrentRestoreRequiresCurrentArchive",
            "PhysicalPlanMutationRequiresCurrentRestore",
            "RoutineAdmissionRequiresClosedEffects",
            "EffectClassificationPartitionsFailures",
            "UntrustedPlanNeverMutates",
            "ExternalEffectMutationRequiresCurrentRestore",
            "ExternalEffectNeverTakesRoutineAdmission",
            "TargetPlanDriftNeverMutates",
            "MissingRoutineRecoveryNeverMutates",
            "NoPhysicalRollbackSkipsCurrentArchive",
        ),
    ),
]

# Non-vacuity witness (moved here from a pytest that no per-PR lane could run:
# none has Java). Two of the external-effect invariants are ENTAILED by the
# others plus the classification, so a model that drops "external_effect" from
# the forced-current-restore class still passes them vacuously.
# EffectClassificationPartitionsFailures is the one that can fail, and this
# witness proves it does; without it the classification is documentation.
DECLASSIFY_EXTERNAL_EFFECT = "declassify-external-effect"
_DECLASSIFY_FROM = '    "nomos_changed", "external_effect"\n'
_DECLASSIFY_TO = '    "nomos_changed"\n'
COUNTEREXAMPLE_PROPERTIES = {
    DECLASSIFY_EXTERNAL_EFFECT: (
        TLCProperty(
            "DeclassifiedExternalEffectCounterexample",
            "TLC refuted EffectClassificationPartitionsFailures on the model "
            "with external_effect dropped from the forced-current-restore "
            "class, so the partition invariant fires rather than being listed.",
            refinement_tests=(
                "tests/pact/test_deployment_backup_policy_model_contract.py::"
                "test_effect_classification_partitions_every_field",
                "tests/pact/test_deployment_backup_policy_model_contract.py::"
                "test_external_effect_is_a_known_physical_effect_in_the_model",
            ),
            expected_invariants=("EffectClassificationPartitionsFailures",),
        ),
    ),
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument(
        "--model",
        type=Path,
        default=MODEL_DIR / "DeploymentBackupPolicy.tla",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=MODEL_DIR / "DeploymentBackupPolicy.cfg",
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
    parser.add_argument(
        "--witness-id",
        choices=sorted(COUNTEREXAMPLE_PROPERTIES),
        default=None,
        help="run a registered non-vacuity witness (needs --expect-counterexample)",
    )
    parser.add_argument(
        "--expect-counterexample",
        action="store_true",
        help="succeed only when TLC refutes the witness's named invariant",
    )
    parser.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args(argv)

    if bool(args.witness_id) != args.expect_counterexample:
        parser.error("--witness-id and --expect-counterexample go together")
    model_dir = MODEL_DIR
    model = args.model
    config = args.config
    properties = PROPERTIES
    checker_command = args.checker_command
    if args.witness_id:
        properties = list(COUNTEREXAMPLE_PROPERTIES[args.witness_id])
        model_dir, model, config = _write_declassified_model(args.out)
        checker_command = (
            "uv run --extra dev python "
            "scripts/check_formal_deployment_backup_policy.py "
            f"--witness-id {args.witness_id} --expect-counterexample"
        )

    try:
        result = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=model_dir,
            model=model,
            config=config,
            out_dir=args.out,
            properties=select_properties(properties, args.property_ids),
            expected_properties=properties,
            checker_command=checker_command,
            extra_refinement_tests=args.extra_refinement_tests,
            expect_counterexample=args.expect_counterexample,
            timeout_seconds=args.timeout_seconds,
            include_checker_output=args.expect_counterexample,
        )
    except TLCSetupError as exc:
        print(f"formal-deployment-backup-policy: {exc}", file=sys.stderr)
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-deployment-backup-policy: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    if args.expect_counterexample:
        print(
            "formal-deployment-backup-policy: expected counterexample "
            f"captured; report at {result.report_path}"
        )
        return 0
    print(
        f"formal-deployment-backup-policy: TLC passed; report at {result.report_path}"
    )
    return 0


def _write_declassified_model(out_dir: Path) -> tuple[Path, Path, Path]:
    """Copy the model with external_effect dropped from the forced class.

    The copy keeps the module's file name (TLC requires it to match the
    MODULE line) in its own directory under the run's output.
    """
    witness_dir = out_dir.resolve() / "declassified-model"
    witness_dir.mkdir(parents=True, exist_ok=True)
    text = (MODEL_DIR / "DeploymentBackupPolicy.tla").read_text(encoding="utf-8")
    mutated = text.replace(_DECLASSIFY_FROM, _DECLASSIFY_TO)
    if mutated == text:
        raise SystemExit(
            "formal-deployment-backup-policy: model shape changed; "
            "update the declassify-external-effect witness"
        )
    model = witness_dir / "DeploymentBackupPolicy.tla"
    model.write_text(mutated, encoding="utf-8")
    config = witness_dir / "DeploymentBackupPolicy.cfg"
    shutil.copy(MODEL_DIR / "DeploymentBackupPolicy.cfg", config)
    return witness_dir, model, config


if __name__ == "__main__":
    raise SystemExit(main())
