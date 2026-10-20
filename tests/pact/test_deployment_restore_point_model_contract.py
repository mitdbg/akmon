from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = ROOT / "formal" / "deployment_restore_point"
MODEL = MODEL_DIR / "DeploymentRestorePoint.tla"
CONFIG = MODEL_DIR / "DeploymentRestorePoint.cfg"
COUNTEREXAMPLE = MODEL_DIR / "DeploymentRestorePointCounterexample.tla"
CHECKER = ROOT / "scripts" / "check_formal_deployment_restore_point.py"
SUITE = ROOT / "scripts" / "run_formal_suite.py"
UPGRADE = ROOT / "src" / "okg" / "substrate" / "deployment_upgrade.py"


RUNTIME_PHASES = (
    "preflight",
    "backup",
    "stop_services",
    "sync_code",
    "dependency_sync",
    "migrate",
    "catalog_apply",
    "nomos_baseline",
    "generation_recovery",
    "attribute_read_model",
    "restart_services",
    "source_convergence",
    "smoke_publish",
    "readiness",
)


def test_model_names_every_runtime_phase() -> None:
    model = MODEL.read_text(encoding="utf-8")
    for phase in RUNTIME_PHASES:
        assert f'"{phase}"' in model
    assert "Prerequisites(p)" in model
    assert 'ProviderMutationPhases == {"smoke_publish"}' in model


def test_config_checks_restore_target_and_external_safety() -> None:
    model = MODEL.read_text(encoding="utf-8")
    config = CONFIG.read_text(encoding="utf-8")
    for invariant in (
        "MutationRequiresExactTargetAndVerifiedRestore",
        "TargetSubstitutionRequiresExplicitAcknowledgement",
        "ExternalEffectNeverClaimsReversible",
        "UncertainExternalEffectIsNeverReplayed",
        "SkipBackupNeverAuthorizesMutation",
        "UncertainPhaseCannotSucceed",
        "AttemptGuaranteesRecorded",
        "EffectUncertaintyIsRecorded",
    ):
        assert invariant in model
        assert invariant in config


def test_backup_cannot_manufacture_restore_authority() -> None:
    model = MODEL.read_text(encoding="utf-8")
    finish = model.split("FinishPhase(p) ==", 1)[1].split("SkipPhase(p) ==", 1)[0]
    mutation = model.split(
        "MutationRequiresExactTargetAndVerifiedRestore ==", 1
    )[1].split("TargetSubstitutionRequiresExplicitAcknowledgement ==", 1)[0]
    assert '(p = "backup" => restoreReceiptExact)' in finish
    assert "restoreVerified /\\ restoreReceiptExact" in mutation
    assert model.count(
        '(uncertainPhase = "backup" => restoreReceiptExact)'
    ) >= 2
    assert "(restoreVerified => restoreReceiptExact)" in model


def test_guard_removal_witnesses_are_tracked_and_exact() -> None:
    counterexample = COUNTEREXAMPLE.read_text(encoding="utf-8")
    checker = CHECKER.read_text(encoding="utf-8")
    witnesses = {
        "MissingRestoreCounterexample.cfg": "UnsafeMutateWithoutRestore",
        "TargetSubstitutionCounterexample.cfg": "UnsafeMutateSubstitutedTarget",
        "ExternalReplayCounterexample.cfg": "UnsafeReplayExternalSmoke",
        "SkipBackupCounterexample.cfg": "UnsafeSkipBackupAuthority",
        "ExternalDisclosureCounterexample.cfg": "UnsafeHideExternalEffect",
        "IndependentHandoffPositive.cfg": "IndependentHandoffUnreachable",
    }
    for config_name, action in witnesses.items():
        assert action in counterexample
        assert config_name in checker
        assert (MODEL_DIR / config_name).is_file()
    assert "expect_counterexample=True" in checker
    assert "min_distinct_states=100" in checker


def test_checker_is_registered_in_formal_suite() -> None:
    suite = SUITE.read_text(encoding="utf-8")
    assert 'CheckerSpec("check_formal_deployment_restore_point.py"' in suite


def test_exact_observation_abstraction_refines_to_durable_attempt_records() -> None:
    model = MODEL.read_text(encoding="utf-8")
    source = UPGRADE.read_text(encoding="utf-8")
    assert 'exactObservationKind = "matching_terminal_receipt"' in model
    assert 'exactObservationKind = "matching_no_effect_proof"' in model
    assert "RecordTerminalFailure" in model
    failure = model.split("RecordTerminalFailure ==", 1)[1].split("Interrupt ==", 1)[0]
    assert "terminalReceipts'" not in failure
    for field in (
        '"attempt_id"',
        '"status"',
        '"guarantees"',
        '"effect_may_have_happened"',
        '"terminal_receipt"',
        '"reconcile"',
        '"superseded_by"',
        '"recovery_command"',
        '"safety_plan_digest"',
        '"resolved_digest"',
        '"archive_digest"',
        '"effect_digest"',
    ):
        assert field in source
