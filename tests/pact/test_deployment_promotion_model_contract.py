import importlib.util
import json
import subprocess
from types import SimpleNamespace
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = ROOT / "formal/deployment_promotion"
MODEL = MODEL_DIR / "DeploymentPromotion.tla"
CONFIG = MODEL_DIR / "DeploymentPromotion.cfg"
COUNTEREXAMPLE = MODEL_DIR / "DeploymentPromotionCounterexample.tla"


def _checker_module():
    path = ROOT / "scripts/check_formal_deployment_promotion.py"
    spec = importlib.util.spec_from_file_location("deployment_promotion_checker", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _action(model: str, name: str, next_name: str) -> str:
    start = model.index(f"{name} ==")
    end = model.index(f"\n{next_name} ==", start)
    return model[start:end]


def test_backup_is_created_only_after_fencing_and_drain() -> None:
    model = MODEL.read_text()
    action = _action(model, "CreateBackup", "VerifyBackup")

    assert 'phase = "drained"' in action
    assert "lockHeld /\\ writersFenced /\\ queuesDrained" in action


def test_invalid_backup_has_explicit_fail_closed_recovery() -> None:
    model = MODEL.read_text()
    reject = _action(model, "RejectBackup", "ActivateCandidate")
    prove_old = _action(
        model, "ProveUnmutatedPreviousReady", "CompleteRollback"
    )

    assert 'phase\' = "backup_invalid"' in reject
    assert "backupRejected' = TRUE" in reject
    assert "~productionMutated" in prove_old
    assert 'activeArtifact = "old" /\\ databaseVersion = "old"' in prove_old
    assert "InvalidBackupRetainsFenceUntilReady" in model
    assert "writersFenced /\\ ~writersOpen" in model


def test_candidate_activation_requires_verified_backup() -> None:
    model = MODEL.read_text()
    action = _action(model, "ActivateCandidate", "ApplyDatabaseMutation")

    assert "backupVerified" in action
    assert "writersFenced" in action
    assert "queuesDrained" in action


def test_all_production_mutation_boundaries_are_explicit() -> None:
    model = MODEL.read_text()
    transitions = (
        ("ActivateCandidate", "ApplyDatabaseMutation", None, "release"),
        ("ApplyDatabaseMutation", "ActivatePolicy", "release", "database"),
        ("ActivatePolicy", "RestartServices", "database", "policy"),
        ("RestartServices", "PublishGeneration", "policy", "services"),
        ("PublishGeneration", "CheckProductionReadiness", "services", "publish"),
        ("CheckProductionReadiness", "ProductionReady", "publish", "readiness"),
    )

    for action_name, next_name, before, after in transitions:
        action = _action(model, action_name, next_name)
        if before is not None:
            assert f'mutationBoundary = "{before}"' in action
        assert f'mutationBoundary\' = "{after}"' in action

    rollback = _action(model, "RequireRollback", "RestoreDatabase")
    assert 'phase \\in {"mutating", "production_ready"}' in rollback
    assert 'mutationBoundary \\in MutationBoundaries \\ {"none"}' in rollback


def test_rollback_restores_database_before_previous_release() -> None:
    model = MODEL.read_text()
    restore = _action(model, "RestoreDatabase", "ReactivatePrevious")
    reactivate = _action(model, "ReactivatePrevious", "ProvePreviousReady")

    assert 'phase\' = "database_restored"' in restore
    assert 'phase = "database_restored"' in reactivate
    assert 'activeArtifact\' = "old"' in reactivate


def test_recovery_property_has_load_bearing_fairness() -> None:
    model = MODEL.read_text()
    config = CONFIG.read_text()

    assert "WF_vars(RecoveryAdvance)" in model
    assert "RecoveryConverges ==" in model
    assert "RecoveryWithoutInjectedFailureSucceeds ==" in model
    assert 'recoveryFailure = "none"' in model
    assert "PROPERTIES\n  RecoveryConverges" in config
    assert "  RecoveryWithoutInjectedFailureSucceeds" in config


def test_tlc_config_checks_all_named_safety_invariants() -> None:
    config = CONFIG.read_text()

    for invariant in (
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
    ):
        assert invariant in config


def test_removed_guards_have_isolated_counterexample_witnesses() -> None:
    module = COUNTEREXAMPLE.read_text()
    witnesses = {
        "DeploymentPromotionBackupGuardWitness.cfg": (
            "BackupGuardSpec",
            "MutationRequiresVerifiedPostFenceBackup",
            "UnsafeActivateWithoutVerifiedBackup",
        ),
        "DeploymentPromotionLockGuardWitness.cfg": (
            "LockGuardSpec",
            "OneActivePromotion",
            "UnsafeConcurrentAcquire",
        ),
        "DeploymentPromotionWriterReadyGuardWitness.cfg": (
            "WriterReadyGuardSpec",
            "RollbackReopensOnlyPreviousState",
            "UnsafeReopenBeforePreviousReady",
        ),
        "DeploymentPromotionRestoreOrderGuardWitness.cfg": (
            "RestoreOrderGuardSpec",
            "PreviousArtifactRequiresRestoredDatabase",
            "UnsafeReactivateBeforeDatabaseRestore",
        ),
    }

    for filename, (specification, invariant, unsafe_action) in witnesses.items():
        config = (MODEL_DIR / filename).read_text()
        assert f"SPECIFICATION {specification}" in config
        assert f"  {invariant}" in config
        assert unsafe_action in module


def test_witness_runner_rejects_named_text_with_setup_exit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    checker = _checker_module()
    monkeypatch.setattr(checker, "find_tla2tools_jar", lambda _root: Path("jar"))
    monkeypatch.setattr(checker.shutil, "which", lambda _name: "/usr/bin/java")
    all_named = "\n".join(
        f"Error: Invariant {invariant} is violated"
        for _, _, invariant in checker.WITNESSES
    )
    monkeypatch.setattr(
        checker.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=2,
            stdout=all_named,
            stderr="setup failed",
        ),
    )

    assert checker._run_witnesses(out_dir=tmp_path, timeout_seconds=1) == 1
    summary = json.loads((tmp_path / "witnesses.json").read_text())
    assert summary["ok"] is False
    assert all(witness["matched"] is False for witness in summary["witnesses"])


def test_witness_runner_records_byte_timeout_as_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    checker = _checker_module()
    monkeypatch.setattr(checker, "find_tla2tools_jar", lambda _root: Path("jar"))
    monkeypatch.setattr(checker.shutil, "which", lambda _name: "/usr/bin/java")

    def timeout(*_args, **_kwargs):
        raise subprocess.TimeoutExpired(
            cmd="tlc",
            timeout=1,
            output=b"partial stdout",
            stderr=b"partial stderr",
        )

    monkeypatch.setattr(checker.subprocess, "run", timeout)

    assert checker._run_witnesses(out_dir=tmp_path, timeout_seconds=1) == 1
    summary = json.loads((tmp_path / "witnesses.json").read_text())
    assert summary["ok"] is False
    assert all(witness["exit_code"] == 124 for witness in summary["witnesses"])
    assert "partial stdout" in (tmp_path / "backup-guard.log").read_text()
