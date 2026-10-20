from dataclasses import fields, replace
from pathlib import Path

import pytest

from okg.substrate.deployment_backup_policy import (
    FULL_RESTORE,
    NO_PHYSICAL_ROLLBACK,
    REFUSE,
    BackupPolicyInput,
    DisasterRecoveryChecks,
    EffectInventoryChecks,
    PhysicalRollbackPolicyInput,
    RestoreDrillChecks,
    parse_disaster_recovery_checks,
    parse_effect_inventory_checks,
    parse_restore_drill_checks,
    select_backup_verification,
    select_physical_rollback_policy,
)


ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = ROOT / "formal" / "deployment_backup_policy"
MODEL = MODEL_DIR / "DeploymentBackupPolicy.tla"
CONFIG = MODEL_DIR / "DeploymentBackupPolicy.cfg"


def _action(model: str, name: str, next_name: str) -> str:
    start = model.index(f"{name} ==")
    end = model.index(f"\n{next_name} ==", start)
    return model[start:end]


def _all_true(check_type: type[object]) -> dict[str, bool]:
    return {item.name: True for item in fields(check_type)}


def _effects(**changes: object) -> EffectInventoryChecks:
    value = EffectInventoryChecks(**_all_true(EffectInventoryChecks))
    return replace(value, **changes)


def _recovery(**changes: object) -> DisasterRecoveryChecks:
    value = DisasterRecoveryChecks(**_all_true(DisasterRecoveryChecks))
    return replace(value, **changes)


def _input(**changes: object) -> PhysicalRollbackPolicyInput:
    value = PhysicalRollbackPolicyInput(
        policy_mode="risk_admitted",
        release_channel="dev",
        readiness_profile="dogfood",
        deployment_class="dogfood_workspace",
        destructive_confirmed=False,
        effects=_effects(),
        recovery=_recovery(),
    )
    return replace(value, **changes)


def test_current_archive_is_required_before_mutation() -> None:
    model = MODEL.read_text(encoding="utf-8")
    action = _action(model, "BeginMutation", "Complete")
    forced = select_physical_rollback_policy(
        _input(effects=_effects(no_pending_migrations=False))
    )

    assert forced.mode == FULL_RESTORE
    assert 'phase \\in {"full_verified", "routine_admitted"}' in action
    assert "THEN /\\ archiveValid /\\ fullRestoreVerified" in action
    assert "CurrentRestoreRequiresCurrentArchive" in model


@pytest.mark.parametrize(
    "changes",
    [
        {"policy_mode": "current_full_restore"},
        {"release_channel": "prod"},
        {"readiness_profile": "general"},
        {"deployment_class": "production"},
        {"destructive_confirmed": True},
    ],
)
def test_destructive_and_production_paths_require_full_restore(
    changes: dict[str, object],
) -> None:
    decision = select_physical_rollback_policy(_input(**changes))

    assert decision.mode == FULL_RESTORE


def test_no_physical_rollback_requires_closed_effect_inventory() -> None:
    model = MODEL.read_text(encoding="utf-8")
    decision = select_physical_rollback_policy(_input())
    admission = _action(
        model,
        "AdmitRoutineNoPhysicalRollback",
        "CreateCurrentArchive",
    )

    assert decision.mode == NO_PHYSICAL_ROLLBACK
    assert decision.no_physical_rollback_required is True
    assert "/\\ RoutineEligible" in admission
    assert "/\\ RecoveryCompatible" in admission
    assert "RoutineAdmissionRequiresClosedEffects" in model
    assert "NoPhysicalRollbackSkipsCurrentArchive" in model


@pytest.mark.parametrize(
    "field_name",
    [item.name for item in fields(EffectInventoryChecks)],
)
def test_every_effect_inventory_field_is_load_bearing(field_name: str) -> None:
    from okg.substrate.deployment_backup_policy import (
        _FORCED_CURRENT_BACKUP_EFFECTS,
    )

    decision = select_physical_rollback_policy(
        _input(effects=_effects(**{field_name: False}))
    )
    expected = (
        FULL_RESTORE
        if field_name in _FORCED_CURRENT_BACKUP_EFFECTS
        else REFUSE
    )

    assert decision.mode != NO_PHYSICAL_ROLLBACK
    assert decision.mode == expected
    assert f"effect.{field_name}" in decision.reasons


@pytest.mark.slow
def test_declassifying_external_effect_fails_the_checker(tmp_path: Path) -> None:
    """The partition invariant must actually fire, not just be listed.

    Naming a guarantee as an invariant proves nothing on its own:
    ``ExternalEffectMutationRequiresCurrentRestore`` and
    ``ExternalEffectNeverTakesRoutineAdmission`` are both ENTAILED by the
    existing invariants plus the classification, so declassifying
    ``external_effect`` leaves them holding vacuously and TLC still passes.
    ``EffectClassificationPartitionsFailures`` is the one that can fail, and
    this run is the proof that it does.  Without it this whole model change is
    documentation.
    """
    import shutil
    import subprocess

    from okg.substrate.formal_tlc import tlc_jvm_args, tlc_workers

    from tests.helpers.tlc import TLA_JAR, TLC_AVAILABLE, assert_tlc_ran, tlc_skip_reason

    if not TLC_AVAILABLE:
        pytest.skip(tlc_skip_reason())
    assert TLA_JAR is not None  # narrowed by the TLC_AVAILABLE gate above
    jar = TLA_JAR
    model = tmp_path / "DeploymentBackupPolicy.tla"
    config = tmp_path / "DeploymentBackupPolicy.cfg"
    shutil.copy(CONFIG, config)
    text = MODEL.read_text(encoding="utf-8")
    mutated = text.replace(
        '    "nomos_changed", "external_effect"\n', '    "nomos_changed"\n'
    )
    assert mutated != text, "model shape changed; update this witness"
    model.write_text(mutated, encoding="utf-8")

    workers = tlc_workers()
    result = subprocess.run(
        [
            "java",
            # Bound heap and thread pools through the shared profile: an
            # uncapped TLC still proves the same properties, it just takes the
            # whole machine while doing it.
            *tlc_jvm_args(workers),
            "-cp", str(jar), "tlc2.TLC",
            "-config", str(config), "-workers", workers, str(model),
        ],
        capture_output=True,
        text=True,
        cwd=ROOT,
        timeout=900,
    )

    assert_tlc_ran(result)
    assert result.returncode != 0, "declassified model must not pass TLC"
    assert (
        "EffectClassificationPartitionsFailures" in result.stdout
    ), result.stdout[-2000:]


def test_effect_classification_partitions_every_field() -> None:
    """No effect may be unclassified, and none may be in both classes.

    A field in neither tuple would be silently ignored by
    ``select_physical_rollback_policy`` — a check that reads as enforced while
    enforcing nothing.
    """
    from okg.substrate.deployment_backup_policy import (
        _FORCED_CURRENT_BACKUP_EFFECTS,
        _REFUSAL_EFFECTS,
    )

    declared = {item.name for item in fields(EffectInventoryChecks)}
    forced = set(_FORCED_CURRENT_BACKUP_EFFECTS)
    refusing = set(_REFUSAL_EFFECTS)

    assert forced & refusing == set()
    assert forced | refusing == declared


@pytest.mark.parametrize(
    "field_name",
    [item.name for item in fields(DisasterRecoveryChecks)],
)
def test_every_disaster_recovery_field_is_load_bearing(field_name: str) -> None:
    model = MODEL.read_text(encoding="utf-8")
    decision = select_physical_rollback_policy(
        _input(recovery=_recovery(**{field_name: False}))
    )

    assert decision.mode == REFUSE
    assert f"disaster_recovery.{field_name}" in decision.reasons
    assert f'"{field_name}"' in model


def test_missing_dr_receipt_refuses_instead_of_dumping() -> None:
    decision = select_physical_rollback_policy(_input(recovery=None))

    assert decision.mode == REFUSE
    assert decision.reasons == ("disaster_recovery_missing",)


@pytest.mark.parametrize("malformed", ["false", 1, None, [], {}])
def test_malformed_disaster_recovery_field_refuses(malformed: object) -> None:
    raw = _recovery().to_dict()
    raw["private_mode"] = malformed
    recovery, errors = parse_disaster_recovery_checks(raw)
    decision = select_physical_rollback_policy(
        _input(recovery=recovery, recovery_errors=errors)
    )

    assert recovery is None
    assert errors == ("disaster_recovery_field_not_boolean:private_mode",)
    assert decision.mode == REFUSE
    assert decision.reasons == errors


def test_schema_or_policy_changes_force_current_restore() -> None:
    for field_name in (
        "no_pending_migrations",
        "no_extension_changes",
        "catalog_unchanged",
        "nomos_unchanged",
    ):
        decision = select_physical_rollback_policy(
            _input(effects=_effects(**{field_name: False}))
        )
        assert decision.mode == FULL_RESTORE


def test_external_effects_force_current_restore() -> None:
    """External reach must buy a restore point, not a dead end.

    A smoke publish that calls an external provider cannot roll those calls
    back, but it can still roll back its database.  Refusing instead left a
    deployment with an LLM-bearing source no supported path to a known-good
    state at all (issue 1013).
    """
    decision = select_physical_rollback_policy(
        _input(effects=_effects(no_external_effects=False))
    )

    assert decision.mode == FULL_RESTORE
    assert "effect.no_external_effects" in decision.reasons
    # It must not be laundered into the routine no-physical-rollback path.
    assert decision.mode != NO_PHYSICAL_ROLLBACK


def test_external_effects_never_take_the_no_physical_rollback_path() -> None:
    """Even a perfect dogfood posture cannot skip the archive when external."""
    decision = select_physical_rollback_policy(
        _input(
            policy_mode="risk_admitted",
            release_channel="dev",
            readiness_profile="dogfood",
            deployment_class="dogfood_workspace",
            destructive_confirmed=False,
            effects=_effects(no_external_effects=False),
            recovery=_recovery(),
        )
    )

    assert decision.mode == FULL_RESTORE


@pytest.mark.parametrize(
    "field_name",
    [
        "target_revision_pinned",
        "plan_readable",
        "smoke_source_versioned",
        "attribute_read_model_rebuildable",
    ],
)
def test_unknown_or_unreadable_plans_refuse(field_name: str) -> None:
    decision = select_physical_rollback_policy(
        _input(effects=_effects(**{field_name: False}))
    )

    assert decision.mode == REFUSE
    assert decision.reasons == (f"effect.{field_name}",)


def test_external_effect_is_a_known_physical_effect_in_the_model() -> None:
    """The TLA+ model must classify external reach the same way the kernel does.

    Both sides must agree that external reach forces a current verified full
    restore rather than refusing, or the proof covers a system nobody runs.
    """
    from okg.substrate.deployment_backup_policy import (
        _FORCED_CURRENT_BACKUP_EFFECTS,
        _REFUSAL_EFFECTS,
    )

    model = MODEL.read_text(encoding="utf-8")
    known = _action(model, "KnownPhysicalEffects", "RefusedEffects")
    refused = _action(model, "RefusedEffects", "RecoveryFailures")

    assert '"external_effect"' in known
    assert '"external_effect"' not in refused
    assert "no_external_effects" in _FORCED_CURRENT_BACKUP_EFFECTS
    assert "no_external_effects" not in _REFUSAL_EFFECTS
    assert "ExternalEffectMutationRequiresCurrentRestore" in model
    assert "ExternalEffectMutationRequiresCurrentRestore" in CONFIG.read_text(
        encoding="utf-8"
    )


def test_candidate_planner_must_be_trusted() -> None:
    decision = select_physical_rollback_policy(
        _input(effects=_effects(planner_trusted=False))
    )

    assert decision.mode == REFUSE
    assert decision.reasons == ("effect.planner_trusted",)


def test_target_plan_drift_refuses() -> None:
    model = MODEL.read_text(encoding="utf-8")
    selected = select_physical_rollback_policy(_input())
    drifted = select_physical_rollback_policy(
        _input(effects=_effects(effect_digest_stable=False))
    )

    assert selected.mode == NO_PHYSICAL_ROLLBACK
    assert drifted.mode == REFUSE
    assert "IntroduceTargetPlanDrift" in model
    assert "TargetPlanDriftNeverMutates" in model


@pytest.mark.parametrize("malformed", ["false", 1, None, []])
def test_malformed_effect_inventory_boolean_refuses(malformed: object) -> None:
    raw = _effects().to_dict()
    raw["planner_trusted"] = malformed
    effects, errors = parse_effect_inventory_checks(raw)
    decision = select_physical_rollback_policy(
        _input(effects=effects, effect_errors=errors)
    )

    assert effects is None
    assert errors == ("effect_inventory_field_not_boolean:planner_trusted",)
    assert decision.mode == REFUSE


@pytest.mark.parametrize("malformed", ["false", 1, None, []])
def test_malformed_core_policy_booleans_refuse_mutation(
    malformed: object,
) -> None:
    decision = select_physical_rollback_policy(_input(destructive_confirmed=malformed))

    assert decision.mode == REFUSE
    assert decision.reasons == ("policy_field_not_boolean:destructive_confirmed",)


def test_config_checks_physical_rollback_invariants() -> None:
    config = CONFIG.read_text(encoding="utf-8")

    for invariant in (
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
    ):
        assert invariant in config


def test_legacy_restore_drill_parser_remains_strict() -> None:
    checks = RestoreDrillChecks(**_all_true(RestoreDrillChecks))
    raw = checks.to_dict()
    raw["private_mode"] = "true"
    parsed, errors = parse_restore_drill_checks(raw)

    assert parsed is None
    assert errors == ("receipt_field_not_boolean:private_mode",)

    legacy = BackupPolicyInput(
        policy_mode="restore_drill",
        release_channel="dev",
        readiness_profile="dogfood",
        deployment_class="dogfood_workspace",
        preflight_readable=True,
        current_archive_valid=True,
        destructive_confirmed=False,
        candidate_plan_stable=True,
        policy_stable=True,
        receipt=checks,
    )
    assert select_backup_verification(legacy).reuse_allowed is True
