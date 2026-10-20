"""Pure deployment-lifecycle contract used by formal refinement tests.

The orchestration integration lands in the phase-engine task.  This module is
deliberately effect-free: it is the executable statement of which completed
phases remain valid after a semantic input change and what constitutes a clean
terminal lifecycle state.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from typing import Any, Final, Iterable, Literal, Mapping


LifecyclePhase = Literal[
    "preflight",
    "backup",
    "quiesce",
    "migration",
    "catalog",
    "smoke",
    "readiness",
]
LifecycleInput = Literal[
    "preflight",
    "backup",
    "migration",
    "catalog",
    "smoke",
    "readiness",
]
TerminalOutcome = Literal["succeeded", "failed", "blocked"]

PHASE_ORDER: Final[tuple[LifecyclePhase, ...]] = (
    "preflight",
    "backup",
    "quiesce",
    "migration",
    "catalog",
    "smoke",
    "readiness",
)

PHASE_DEPENDENCIES: Final[dict[LifecyclePhase, frozenset[LifecyclePhase]]] = {
    "preflight": frozenset(),
    "backup": frozenset({"preflight"}),
    "quiesce": frozenset({"preflight", "backup"}),
    "migration": frozenset({"preflight", "backup", "quiesce"}),
    "catalog": frozenset({"preflight", "backup", "quiesce", "migration"}),
    "smoke": frozenset({"preflight", "backup", "quiesce", "migration", "catalog"}),
    "readiness": frozenset(PHASE_ORDER[:-1]),
}

INPUT_INVALIDATIONS: Final[dict[LifecycleInput, frozenset[LifecyclePhase]]] = {
    "preflight": frozenset(PHASE_ORDER),
    "backup": frozenset(PHASE_ORDER[1:]),
    "migration": frozenset(PHASE_ORDER[3:]),
    "catalog": frozenset(PHASE_ORDER[4:]),
    "smoke": frozenset(PHASE_ORDER[5:]),
    "readiness": frozenset(PHASE_ORDER[6:]),
}


def invalidated_phases(
    changed_inputs: Iterable[LifecycleInput],
) -> frozenset[LifecyclePhase]:
    """Return exactly the phases transitively dependent on changed inputs."""

    invalidated: set[LifecyclePhase] = set()
    for changed_input in changed_inputs:
        invalidated.update(INPUT_INVALIDATIONS[changed_input])
    return frozenset(invalidated)


def valid_completed_phases(
    completed: Iterable[LifecyclePhase],
    *,
    changed_inputs: Iterable[LifecycleInput] = (),
) -> frozenset[LifecyclePhase]:
    """Retain completed receipts whose load-bearing inputs did not change."""

    return frozenset(completed) - invalidated_phases(changed_inputs)


def phases_to_run(
    completed: Iterable[LifecyclePhase],
    *,
    changed_inputs: Iterable[LifecycleInput] = (),
) -> tuple[LifecyclePhase, ...]:
    """Plan only invalid or incomplete phases in dependency order."""

    valid = valid_completed_phases(
        completed,
        changed_inputs=changed_inputs,
    )
    return tuple(phase for phase in PHASE_ORDER if phase not in valid)


def mutation_allowed(valid_completed: Iterable[LifecyclePhase]) -> bool:
    """Mutation is admissible only after current preflight and backup proof."""

    valid = frozenset(valid_completed)
    return {"preflight", "backup", "quiesce"}.issubset(valid)


@dataclass(frozen=True)
class LifecycleResourceState:
    """Effects that must be clean before a lifecycle may claim termination."""

    prior_service_running: bool
    service_running: bool
    child_process_running: bool = False
    disposable_database_present: bool = False
    temporary_archive_present: bool = False
    cleanup_obligations: frozenset[str] = frozenset()
    uncertain_effects: frozenset[LifecyclePhase] = frozenset()


def terminal_state_is_clean(
    outcome: TerminalOutcome,
    resources: LifecycleResourceState,
) -> bool:
    """Return whether a claimed terminal outcome has discharged every effect."""

    if outcome == "blocked" or resources.uncertain_effects:
        return False
    return (
        resources.service_running == resources.prior_service_running
        and not resources.child_process_running
        and not resources.disposable_database_present
        and not resources.temporary_archive_present
        and not resources.cleanup_obligations
    )


# ``deployment_upgrade`` has a richer execution contract than the seven
# abstract phases above.  These names and helpers are intentionally pure so a
# persisted receipt can be checked without importing a phase implementation or
# replaying an expensive effect.
UP_PHASE_ORDER: Final[tuple[str, ...]] = (
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

# Dependency closure is semantic, not merely positional.  Code/dependency
# synchronization does not invalidate a proven database backup or migration;
# source publication does depend on the new code.  The explicit closures keep
# option drift from replaying unrelated expensive work.
UP_PHASE_DEPENDENTS: Final[dict[str, frozenset[str]]] = {
    "preflight": frozenset(UP_PHASE_ORDER),
    "backup": frozenset(UP_PHASE_ORDER[1:]),
    "stop_services": frozenset({"stop_services", "restart_services", "source_convergence", "smoke_publish", "readiness"}),
    "sync_code": frozenset({"sync_code", "dependency_sync", "source_convergence", "smoke_publish", "readiness"}),
    "dependency_sync": frozenset({"dependency_sync", "source_convergence", "smoke_publish", "readiness"}),
    "migrate": frozenset(UP_PHASE_ORDER[5:]),
    "catalog_apply": frozenset(UP_PHASE_ORDER[6:]),
    "nomos_baseline": frozenset(UP_PHASE_ORDER[7:]),
    "generation_recovery": frozenset(UP_PHASE_ORDER[8:]),
    "attribute_read_model": frozenset(UP_PHASE_ORDER[9:]),
    "restart_services": frozenset(UP_PHASE_ORDER[10:]),
    "source_convergence": frozenset(UP_PHASE_ORDER[11:]),
    "smoke_publish": frozenset(UP_PHASE_ORDER[12:]),
    "readiness": frozenset({"readiness"}),
}


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(value[key]) for key in sorted(value)}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_json_safe(item) for item in value]
    if isinstance(value, os.PathLike):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)

def canonical_phase_fingerprint(payload: Mapping[str, Any]) -> str:
    """Return a stable digest for one phase's semantic inputs."""

    encoded = json.dumps(
        _json_safe(payload), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def phase_fingerprints(
    *,
    options: Any,
    target_key: Mapping[str, Any],
    lifecycle_plan_digest: str | None = None,
) -> dict[str, str]:
    """Build phase-local semantic fingerprints.

    Each digest contains only inputs that can change that phase's result.  In
    particular, smoke ``include`` and readiness profile are downstream-only;
    they are never part of backup, migration, or service fingerprints.
    """

    # Do not feed the whole target mapping to every phase.  A target revision
    # change is relevant to code/source publication, while a backup receipt is
    # keyed by database identity and backup policy; mixing them caused a late
    # failure to replay an otherwise proven dump.  Keep the identity tuple
    # common, then add only the phase's load-bearing target inputs below.
    identity = {
        key: target_key.get(key)
        for key in (
            "deployment", "authority_mode", "host", "port", "database",
            "identity", "expected_database",
        )
        if key in target_key
    }
    revision = target_key.get("code_revision")
    manifest_digest = target_key.get("manifest_digest")
    deployment_revision = target_key.get("deployment_revision")
    plan_digest = lifecycle_plan_digest or target_key.get("lifecycle_plan_digest")
    policy = {
        "skip_backup": bool(getattr(options, "skip_backup", False)),
        "skip_migrate": bool(getattr(options, "skip_migrate", False)),
        "skip_destructive_migrations": bool(
            getattr(options, "skip_destructive_migrations", False)
        ),
        "confirm_destructive_migrations": bool(
            getattr(options, "confirm_destructive_migrations", False)
        ),
    }
    phase_inputs: dict[str, Mapping[str, Any]] = {
        "preflight": {
            "target_identity": identity,
        },
        "backup": {
            "target_identity": identity,
            "skip_backup": policy["skip_backup"],
            "backup_keep": getattr(options, "backup_keep", 1),
            "backup_policy_digest": target_key.get("backup_policy_digest"),
        },
        "stop_services": {
            "apply_services": bool(getattr(options, "apply_services", True)),
        },
        "sync_code": {
            "target_revision": revision,
            "deployment_revision": deployment_revision,
            "sync_code": bool(getattr(options, "sync_code", True)),
        },
        "dependency_sync": {
            "target_revision": revision,
            "sync_deps": bool(getattr(options, "sync_deps", True)),
        },
        "migrate": {
            "target_identity": identity,
            "lifecycle_plan_digest": plan_digest,
            "migration_policy": {
                key: policy[key]
                for key in (
                    "skip_migrate", "skip_destructive_migrations",
                    "confirm_destructive_migrations",
                )
            },
        },
        "catalog_apply": {
            "target_identity": identity,
            "manifest_digest": manifest_digest,
            "lifecycle_plan_digest": plan_digest,
        },
        "nomos_baseline": {
            "deployment": target_key.get("deployment"),
            "manifest_digest": manifest_digest,
        },
        "generation_recovery": {"deployment": target_key.get("deployment")},
        "attribute_read_model": {
            "deployment": target_key.get("deployment"),
            "manifest_digest": manifest_digest,
        },
        "restart_services": {
            "apply_services": bool(getattr(options, "apply_services", True)),
        },
        "source_convergence": {
            "deployment": target_key.get("deployment"),
            "target_revision": revision,
        },
        "smoke_publish": {
            "deployment": target_key.get("deployment"),
            "target_revision": revision,
            "include": list(getattr(options, "include", ()) or ()),
        },
        "readiness": {
            "deployment": target_key.get("deployment"),
            "profile": getattr(options, "readiness_profile", "general"),
        },
    }
    return {
        phase: canonical_phase_fingerprint({"phase": phase, **dict(inputs)})
        for phase, inputs in phase_inputs.items()
    }


def invalidated_up_phases(
    previous: Mapping[str, Any],
    current: Mapping[str, Any],
) -> frozenset[str]:
    """Invalidate a phase and every later phase when its digest changes."""

    changed: set[str] = set()
    for phase in UP_PHASE_ORDER:
        if previous.get(phase) != current.get(phase):
            changed.update(UP_PHASE_DEPENDENTS[phase])
    return frozenset(changed)


def integrated_deployment_gate(
    phase_results: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Compose independent backup-performance and source-readiness proof."""

    rows = [row for row in phase_results if isinstance(row, Mapping)]

    def latest(name: str) -> Mapping[str, Any] | None:
        return next(
            (row for row in reversed(rows) if row.get("phase") == name), None
        )

    backup = latest("backup")
    smoke = latest("smoke_publish")
    readiness = latest("readiness")
    backup_phase_ok = bool(
        backup
        and backup.get("ok") is True
        and not backup.get("planned")
        and not backup.get("skipped")
        and (
            backup.get("verification_mode")
            or backup.get("receipt_reused") is True
            or backup.get("restore_verified") is True
        )
    )
    smoke_generation = smoke.get("smoke_generation_id") if smoke else None
    readiness_generation = (
        readiness.get("readiness_generation_id") if readiness else None
    )
    generation_match = (
        smoke_generation is not None
        and readiness_generation is not None
        and smoke_generation == readiness_generation
    )
    backup_performance = {
        "status": "passed" if backup_phase_ok else "unavailable",
        "phase_ok": backup_phase_ok,
        "duration_seconds": backup.get("duration_seconds") if backup else None,
        "dump_duration_seconds": backup.get("dump_duration_seconds") if backup else None,
        "budget_checks": dict(backup.get("budget_checks") or {}) if backup else {},
    }
    source_readiness = {
        "status": (
            "passed"
            if (
                smoke and smoke.get("ok") is True
                and readiness and readiness.get("ok") is True
                and generation_match
            )
            else "unavailable"
        ),
        "smoke_ok": smoke.get("ok") is True if smoke else False,
        "readiness_ok": readiness.get("ok") is True if readiness else False,
        "smoke_generation_id": (
            smoke_generation
        ),
        "readiness_generation_id": (
            readiness_generation
        ),
        "generation_match": generation_match,
    }
    return {
        "schema": "okg.deployment_up.integrated_gate.v1",
        "ok": (
            backup_performance["phase_ok"]
            and source_readiness["smoke_ok"]
            and source_readiness["readiness_ok"]
            and generation_match
        ),
        "backup_performance": backup_performance,
        "source_readiness": source_readiness,
    }
