"""Pure policy kernel for deployment-up backup verification.

The lifecycle code gathers database, manifest, archive, and receipt evidence;
this module makes the fail-closed decision without performing I/O. Keeping the
decision pure lets the TLA+ refinement tests exercise the exact production
guards instead of a second, test-only interpretation.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any, Mapping


FULL_RESTORE = "current_full_restore"
ROUTINE_REUSE = "routine_restore_drill_reused"
NO_PHYSICAL_ROLLBACK = "no_physical_rollback_required"
REFUSE = "refuse"


@dataclass(frozen=True)
class EffectInventoryChecks:
    """Closed target-revision effect inventory used before backup work.

    The field names are intentionally claims rather than effect labels: every
    value must be the exact boolean ``True`` before the no-physical-rollback
    path can be admitted.  Known database changes and external reach select the
    current-backup path; authority, readability, and digest failures refuse.
    """

    target_revision_pinned: bool
    planner_trusted: bool
    plan_readable: bool
    no_pending_migrations: bool
    no_extension_changes: bool
    catalog_unchanged: bool
    nomos_unchanged: bool
    smoke_source_versioned: bool
    no_external_effects: bool
    effect_digest_stable: bool

    def __post_init__(self) -> None:
        _require_exact_booleans(self, label="effect-inventory checks")

    def to_dict(self) -> dict[str, bool]:
        return {item.name: getattr(self, item.name) for item in fields(self)}


@dataclass(frozen=True)
class DisasterRecoveryChecks:
    """Identity and freshness proof created outside ``deployment up``."""

    typed_schema: bool
    deployment_scoped_path: bool
    private_mode: bool
    current_owner: bool
    archive_digest_bound: bool
    same_archive_restored: bool
    archive_fresh: bool
    restore_proof_fresh: bool
    deployment_equal: bool
    host_equal: bool
    port_equal: bool
    database_equal: bool
    database_owner_equal: bool
    cluster_system_identifier_equal: bool
    server_version_equal: bool
    dump_tool_major_equal: bool
    restore_tool_major_equal: bool
    backup_tool_executable_identity_equal: bool
    extension_versions_equal: bool
    # True when the restore proof used TimescaleDB's pre/post procedure and
    # covers the target's current hypertable set, when the target has no
    # TimescaleDB at all, or when the deployment explicitly declared
    # timescale_restore_risk_admitted (recorded, never silent). The proof
    # evidence is computed in deployment_upgrade's receipt evidence
    # collectors; a receipt without the proof block fails this on any
    # hypertable-bearing target.
    timescale_restore_proven_or_risk_admitted: bool
    schema_lineage_equal: bool
    recovery_point_objective_declared: bool

    def __post_init__(self) -> None:
        _require_exact_booleans(self, label="disaster-recovery checks")

    def to_dict(self) -> dict[str, bool]:
        return {item.name: getattr(self, item.name) for item in fields(self)}

    @property
    def compatible(self) -> bool:
        return all(self.to_dict().values())


def _require_exact_booleans(value: object, *, label: str) -> None:
    malformed = [
        item.name
        for item in fields(value)
        if type(getattr(value, item.name)) is not bool
    ]
    if malformed:
        raise TypeError(f"{label} must be exact booleans: " + ", ".join(malformed))


def _parse_exact_checks(
    value: object,
    check_type: type[EffectInventoryChecks] | type[DisasterRecoveryChecks],
    *,
    prefix: str,
) -> tuple[EffectInventoryChecks | DisasterRecoveryChecks | None, tuple[str, ...]]:
    if not isinstance(value, Mapping):
        return None, (f"{prefix}_not_object",)
    expected = {item.name for item in fields(check_type)}
    observed = {str(key) for key in value}
    errors = [f"{prefix}_field_missing:{name}" for name in sorted(expected - observed)]
    errors.extend(
        f"{prefix}_field_unexpected:{name}" for name in sorted(observed - expected)
    )
    errors.extend(
        f"{prefix}_field_not_boolean:{name}"
        for name in sorted(expected & observed)
        if type(value.get(name)) is not bool
    )
    if errors:
        return None, tuple(errors)
    return check_type(**{name: value[name] for name in expected}), ()


def parse_effect_inventory_checks(
    value: object,
) -> tuple[EffectInventoryChecks | None, tuple[str, ...]]:
    checks, errors = _parse_exact_checks(
        value, EffectInventoryChecks, prefix="effect_inventory"
    )
    return checks if isinstance(checks, EffectInventoryChecks) else None, errors


def parse_disaster_recovery_checks(
    value: object,
) -> tuple[DisasterRecoveryChecks | None, tuple[str, ...]]:
    checks, errors = _parse_exact_checks(
        value, DisasterRecoveryChecks, prefix="disaster_recovery"
    )
    return checks if isinstance(checks, DisasterRecoveryChecks) else None, errors


@dataclass(frozen=True)
class PhysicalRollbackPolicyInput:
    policy_mode: str
    release_channel: str | None
    readiness_profile: str
    deployment_class: str | None
    destructive_confirmed: bool
    effects: EffectInventoryChecks | None
    recovery: DisasterRecoveryChecks | None
    effect_errors: tuple[str, ...] = ()
    recovery_errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class PhysicalRollbackPolicyDecision:
    mode: str
    reasons: tuple[str, ...]
    checks: dict[str, Any]

    @property
    def no_physical_rollback_required(self) -> bool:
        return self.mode == NO_PHYSICAL_ROLLBACK


# Effects that make a full undo impossible, so the run must hold a current,
# restore-verified archive before it mutates.  ``no_external_effects`` belongs
# here rather than in the refusal set: a smoke publish that calls an external
# provider cannot roll those calls back, but it can still roll back its
# database, and that restore point is the guarantee the backup phase exists to
# provide.  Refusing instead left such a deployment with no supported path to a
# known-good state at all.
_FORCED_CURRENT_BACKUP_EFFECTS = (
    "no_pending_migrations",
    "no_extension_changes",
    "catalog_unchanged",
    "nomos_unchanged",
    "no_external_effects",
)
# Claims about whether the target plan can be trusted, read, and rebuilt at
# all.  A failure here means the run does not know what it is about to do, so
# no archive can make the mutation safe.
_REFUSAL_EFFECTS = (
    "target_revision_pinned",
    "planner_trusted",
    "plan_readable",
    "smoke_source_versioned",
    "effect_digest_stable",
)


def select_physical_rollback_policy(
    value: PhysicalRollbackPolicyInput,
) -> PhysicalRollbackPolicyDecision:
    """Select no physical rollback, current full restore, or refusal.

    This decision runs before ``pg_dump``.  Missing disaster-recovery proof on
    an otherwise-safe routine plan refuses quickly instead of silently starting
    an expensive dump.  Known physical database changes and effects that reach
    outside the deployment select the forced path; unknown or untrusted effects
    refuse even if a backup happens to exist.
    """
    if type(value.destructive_confirmed) is not bool:
        return PhysicalRollbackPolicyDecision(
            mode=REFUSE,
            reasons=("policy_field_not_boolean:destructive_confirmed",),
            checks={"destructive_confirmed": value.destructive_confirmed},
        )
    posture = {
        "policy_mode_risk_admitted": value.policy_mode == "risk_admitted",
        "release_channel_dev": value.release_channel == "dev",
        "readiness_profile_dogfood": value.readiness_profile == "dogfood",
        "deployment_class_dogfood": value.deployment_class == "dogfood_workspace",
        "no_destructive_confirmation": value.destructive_confirmed is False,
    }
    effect_checks = value.effects.to_dict() if value.effects is not None else {}
    recovery_checks = value.recovery.to_dict() if value.recovery is not None else {}
    checks: dict[str, Any] = {
        **posture,
        "effects_present": value.effects is not None,
        "effect_errors": list(value.effect_errors),
        "effects": effect_checks,
        "recovery_present": value.recovery is not None,
        "recovery_errors": list(value.recovery_errors),
        "recovery": recovery_checks,
    }
    if value.effects is None:
        return PhysicalRollbackPolicyDecision(
            mode=REFUSE,
            reasons=value.effect_errors or ("effect_inventory_missing",),
            checks=checks,
        )
    refusal_reasons = [
        f"effect.{name}" for name in _REFUSAL_EFFECTS if not effect_checks[name]
    ]
    refusal_reasons.extend(value.effect_errors)
    if refusal_reasons:
        return PhysicalRollbackPolicyDecision(
            mode=REFUSE,
            reasons=tuple(refusal_reasons),
            checks=checks,
        )
    forced_reasons = [name for name, passed in posture.items() if not passed]
    forced_reasons.extend(
        f"effect.{name}"
        for name in _FORCED_CURRENT_BACKUP_EFFECTS
        if not effect_checks[name]
    )
    if forced_reasons:
        return PhysicalRollbackPolicyDecision(
            mode=FULL_RESTORE,
            reasons=tuple(forced_reasons),
            checks=checks,
        )
    if value.recovery is None:
        return PhysicalRollbackPolicyDecision(
            mode=REFUSE,
            reasons=value.recovery_errors or ("disaster_recovery_missing",),
            checks=checks,
        )
    recovery_reasons = list(value.recovery_errors)
    recovery_reasons.extend(
        f"disaster_recovery.{name}"
        for name, passed in recovery_checks.items()
        if not passed
    )
    if recovery_reasons:
        return PhysicalRollbackPolicyDecision(
            mode=REFUSE,
            reasons=tuple(recovery_reasons),
            checks=checks,
        )
    return PhysicalRollbackPolicyDecision(
        mode=NO_PHYSICAL_ROLLBACK,
        reasons=("closed routine effects and disaster-recovery proof passed",),
        checks=checks,
    )


@dataclass(frozen=True)
class RestoreDrillChecks:
    deployment_scoped_path: bool
    typed_schema: bool
    private_mode: bool
    current_owner: bool
    fresh: bool
    deployment_equal: bool
    host_equal: bool
    port_equal: bool
    database_equal: bool
    database_owner_equal: bool
    cluster_system_identifier_equal: bool
    server_version_equal: bool
    extension_versions_equal: bool
    schema_migrations_equal: bool
    candidate_plan_equal: bool
    archive_format_equal: bool
    dump_tool_major_equal: bool
    restore_tool_major_equal: bool
    # Same claim as the disaster-recovery counterpart: the drill's restore
    # proof used TimescaleDB's pre/post procedure and covers the current
    # hypertable set, or the target has no TimescaleDB, or the deployment
    # explicitly admitted the risk (recorded, never silent).
    timescale_restore_proven_or_risk_admitted: bool
    observed_restored_size_valid: bool

    def __post_init__(self) -> None:
        malformed = [
            item.name
            for item in fields(self)
            if type(getattr(self, item.name)) is not bool
        ]
        if malformed:
            raise TypeError(
                "restore-drill checks must be exact booleans: " + ", ".join(malformed)
            )

    def to_dict(self) -> dict[str, bool]:
        return {item.name: getattr(self, item.name) for item in fields(self)}

    @property
    def compatible(self) -> bool:
        return all(self.to_dict().values())


@dataclass(frozen=True)
class BackupPolicyInput:
    policy_mode: str
    release_channel: str | None
    readiness_profile: str
    deployment_class: str | None
    preflight_readable: bool
    current_archive_valid: bool
    destructive_confirmed: bool
    candidate_plan_stable: bool
    policy_stable: bool
    receipt: RestoreDrillChecks | None
    receipt_errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class BackupPolicyDecision:
    mode: str
    reasons: tuple[str, ...]
    checks: dict[str, Any]

    @property
    def reuse_allowed(self) -> bool:
        return self.mode == ROUTINE_REUSE


def parse_restore_drill_checks(
    value: object,
) -> tuple[RestoreDrillChecks | None, tuple[str, ...]]:
    """Parse a receipt check map without truthiness coercion."""
    if not isinstance(value, Mapping):
        return None, ("receipt_not_object",)
    expected = {item.name for item in fields(RestoreDrillChecks)}
    observed = {str(key) for key in value}
    errors = [f"receipt_field_missing:{name}" for name in sorted(expected - observed)]
    errors.extend(
        f"receipt_field_unexpected:{name}" for name in sorted(observed - expected)
    )
    errors.extend(
        f"receipt_field_not_boolean:{name}"
        for name in sorted(expected & observed)
        if type(value.get(name)) is not bool
    )
    if errors:
        return None, tuple(errors)
    return RestoreDrillChecks(**{name: value[name] for name in expected}), ()


def select_backup_verification(value: BackupPolicyInput) -> BackupPolicyDecision:
    """Select current full restore, compatible routine reuse, or refusal.

    A missing/mismatched receipt never skips verification: it selects the
    safer current full restore and reports every failed field. Unreadable
    preflight or an invalid current archive refuses mutation entirely.
    """
    malformed_boolean_fields = tuple(
        name
        for name in (
            "preflight_readable",
            "current_archive_valid",
            "destructive_confirmed",
            "candidate_plan_stable",
            "policy_stable",
        )
        if type(getattr(value, name)) is not bool
    )
    if malformed_boolean_fields:
        return BackupPolicyDecision(
            mode=REFUSE,
            reasons=tuple(
                f"policy_field_not_boolean:{name}" for name in malformed_boolean_fields
            ),
            checks={
                "malformed_boolean_fields": list(malformed_boolean_fields),
                "receipt_present": value.receipt is not None,
                "receipt_errors": list(value.receipt_errors),
            },
        )

    posture_checks = {
        "policy_mode_routine": value.policy_mode == "restore_drill",
        "release_channel_dev": value.release_channel == "dev",
        "readiness_profile_dogfood": value.readiness_profile == "dogfood",
        "deployment_class_dogfood": value.deployment_class == "dogfood_workspace",
        "preflight_readable": value.preflight_readable is True,
        "current_archive_valid": value.current_archive_valid is True,
        "no_destructive_confirmation": value.destructive_confirmed is False,
        "candidate_plan_stable": value.candidate_plan_stable is True,
        "policy_stable": value.policy_stable is True,
    }
    receipt_checks = value.receipt.to_dict() if value.receipt is not None else {}
    checks: dict[str, Any] = {
        **posture_checks,
        "receipt_present": value.receipt is not None,
        "receipt_errors": list(value.receipt_errors),
        "receipt": receipt_checks,
    }
    if value.preflight_readable is not True:
        return BackupPolicyDecision(
            mode=REFUSE,
            reasons=("migration preflight is unreadable",),
            checks=checks,
        )
    if value.current_archive_valid is not True:
        return BackupPolicyDecision(
            mode=REFUSE,
            reasons=("current rollback archive is invalid",),
            checks=checks,
        )

    full_restore_reasons: list[str] = []
    for name in (
        "policy_mode_routine",
        "release_channel_dev",
        "readiness_profile_dogfood",
        "deployment_class_dogfood",
        "no_destructive_confirmation",
        "candidate_plan_stable",
        "policy_stable",
    ):
        if not posture_checks[name]:
            full_restore_reasons.append(name)
    if value.receipt is None:
        full_restore_reasons.extend(value.receipt_errors or ("receipt_missing",))
    else:
        full_restore_reasons.extend(value.receipt_errors)
        full_restore_reasons.extend(
            f"receipt.{name}" for name, passed in receipt_checks.items() if not passed
        )
    if full_restore_reasons:
        return BackupPolicyDecision(
            mode=FULL_RESTORE,
            reasons=tuple(full_restore_reasons),
            checks=checks,
        )
    return BackupPolicyDecision(
        mode=ROUTINE_REUSE,
        reasons=("all routine dogfood and restore-drill checks passed",),
        checks=checks,
    )
