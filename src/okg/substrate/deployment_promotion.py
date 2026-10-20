"""Durable, evidence-bound promotion of one sealed deployment release."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, fields
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterator, Mapping

from okg.substrate.deployments.environment import (
    DeploymentEnvironment,
    environment_resource_identities,
    shared_definition_substance_problems,
)
from okg.substrate.errors import DeploymentEnvironmentInvalid, SubstrateError
from okg.substrate.sealed_release import SealedRelease, verify_sealed_release

if TYPE_CHECKING:
    from okg.substrate.deployment_product_lifecycle import ProductApplyRequest


PROMOTION_SCHEMA = "okg.deployment_promotion/v1"
APPROVAL_SCHEMA_V2 = "okg.deployment_promotion_approval/v2"
APPROVAL_SCHEMA_V3 = "okg.deployment_promotion_approval/v3"
# Compatibility name retained for existing v2 callers.
APPROVAL_SCHEMA = APPROVAL_SCHEMA_V2
PRODUCT_ADMISSION_SCHEMA = "okg.deployment_product.plan_admission/v1"
PRODUCT_ADMISSION_JOURNAL_MODE = "okg.deployment_product.admission_only/v1"
MUTATION_BOUNDARIES = (
    "release",
    "database",
    "policy",
    "services",
    "publish",
    "readiness",
)


Step = Callable[["PromotionContext"], Mapping[str, Any]]
OperationInspector = Callable[
    ["PromotionContext", str, str], Mapping[str, Any] | None
]


@dataclass(frozen=True)
class PromotionApproval:
    promotion_id: str
    artifact_id: str
    artifact_input_id: str
    staging_environment_id: str
    production_environment_id: str
    staging_descriptor_digest: str
    production_descriptor_digest: str
    # The digest of the definition the release was sealed from (staging).
    # Production intentionally has a distinct whole-definition digest.
    definition_digest: str
    production_definition_digest: str
    staging_evidence_digest: str
    supervision_evidence_digest: str
    staging_service_plan_digest: str
    production_service_plan_digest: str
    production_resources_digest: str
    approved_at: str
    expires_at: str
    approved_by: str

    def to_dict(self) -> dict[str, Any]:
        return {"schema": APPROVAL_SCHEMA_V2, **self.__dict__}

    @property
    def digest(self) -> str:
        return _digest(self.to_dict())


@dataclass(frozen=True)
class ProductPromotionApprovalV3(PromotionApproval):
    """Product authority admitted to a journal, but not yet executable."""

    transition_kind: str
    expected_previous_artifact_id: str | None
    request_digest: str
    product_plan_digest: str
    lock_digest: str
    composition_digest: str

    def __post_init__(self) -> None:
        for field in ("approved_at", "expires_at"):
            canonical = (
                _parse_time(getattr(self, field), field)
                .isoformat()
                .replace("+00:00", "Z")
            )
            object.__setattr__(self, field, canonical)

    def to_dict(self) -> dict[str, Any]:
        return {"schema": APPROVAL_SCHEMA_V3, **self.__dict__}

    @property
    def digest(self) -> str:
        from okg.substrate.deployment_product_lifecycle import approval_digest

        return approval_digest(self)


PromotionApprovalValue = PromotionApproval | ProductPromotionApprovalV3


def product_promotion_approval_from_mapping(
    raw: Mapping[str, Any],
) -> ProductPromotionApprovalV3:
    if not isinstance(raw, Mapping) or raw.get("schema") != APPROVAL_SCHEMA_V3:
        raise _error("product promotion approval has the wrong schema")
    names = {field.name for field in fields(ProductPromotionApprovalV3)}
    if set(raw) != names | {"schema"}:
        raise _error("product promotion approval fields changed")
    values: dict[str, Any] = {}
    for name in names:
        value = raw[name]
        if name == "expected_previous_artifact_id" and value is None:
            values[name] = None
        elif isinstance(value, str):
            values[name] = value
        else:
            raise _error(
                "product promotion approval field has the wrong type",
                field=name,
            )
    approval = ProductPromotionApprovalV3(**values)
    _validate_promotion_id(approval.promotion_id)
    _validate_product_approval(approval)
    return approval


def load_promotion_approval(path: str | Path) -> PromotionApprovalValue:
    target = Path(path)
    if target.is_symlink() or not target.is_file():
        raise _error("promotion approval is not a regular file", path=str(target))
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise _error("promotion approval is unreadable", path=str(target)) from exc
    if not isinstance(raw, dict) or raw.get("schema") not in {
        APPROVAL_SCHEMA_V2,
        APPROVAL_SCHEMA_V3,
    }:
        raise _error("promotion approval has the wrong schema", path=str(target))
    if raw["schema"] == APPROVAL_SCHEMA_V3:
        from okg.substrate.deployment_product_lifecycle import (
            load_product_promotion_approval,
        )

        return load_product_promotion_approval(target)
    approval_type = PromotionApproval
    names = {field.name for field in fields(approval_type)}
    if set(raw) != names | {"schema"}:
        raise _error("promotion approval fields changed", path=str(target))
    values: dict[str, Any] = {}
    for name in names:
        value = raw[name]
        if name == "expected_previous_artifact_id" and value is None:
            values[name] = None
        elif isinstance(value, str):
            values[name] = value
        else:
            raise _error(
                "promotion approval field has the wrong type",
                path=str(target),
                field=name,
            )
    approval = approval_type(**values)
    _validate_promotion_id(approval.promotion_id)
    return approval


def save_promotion_approval(
    path: str | Path, approval: PromotionApprovalValue
) -> None:
    if isinstance(approval, ProductPromotionApprovalV3):
        raise _error(
            "persist product approval v3 through the product lifecycle authority"
        )
    target = Path(path)
    if target.exists():
        existing = load_promotion_approval(target)
        if existing != approval:
            raise _error("refusing to replace a different promotion approval")
        return
    _write_state(target, approval.to_dict())


def create_product_promotion_approval(
    *,
    base: PromotionApproval,
    transition_kind: str,
    expected_previous_artifact_id: str | None,
    request_digest: str,
    product_plan_digest: str,
    lock_digest: str,
    composition_digest: str,
) -> ProductPromotionApprovalV3:
    """Extend one exact v2 approval with product-plan authority."""

    if type(base) is not PromotionApproval:
        raise _error("product approval requires an exact v2 base approval")
    approval = ProductPromotionApprovalV3(
        **base.__dict__,
        transition_kind=transition_kind,
        expected_previous_artifact_id=expected_previous_artifact_id,
        request_digest=request_digest,
        product_plan_digest=product_plan_digest,
        lock_digest=lock_digest,
        composition_digest=composition_digest,
    )
    _validate_product_approval(approval)
    return approval


@dataclass(frozen=True)
class PromotionOperations:
    capture_previous_health: Step
    fence_writers: Step
    drain_work: Step
    capture_previous_release: Step
    create_backup: Step
    verify_backup: Step
    activate_release: Step
    migrate_database: Step
    activate_policy: Step
    restart_services: Step
    publish_generation: Step
    prove_production_ready: Step
    restore_backup: Step
    activate_previous_release: Step
    restart_previous_services: Step
    prove_previous_ready: Step
    reopen_writers: Step
    inspect_operation: OperationInspector | None = None


@dataclass
class PromotionContext:
    approval: PromotionApproval
    staging: DeploymentEnvironment
    production: DeploymentEnvironment
    release: SealedRelease
    state_path: Path
    state: dict[str, Any]
    inspect_operation: OperationInspector | None = None


@dataclass(frozen=True)
class _StepFailure(Exception):
    step: str
    details: Mapping[str, Any]

    def __str__(self) -> str:
        return f"promotion step {self.step!r} failed"


def create_promotion_approval(
    *,
    staging: DeploymentEnvironment,
    production: DeploymentEnvironment,
    release: SealedRelease,
    staging_readiness: Mapping[str, Any],
    supervision_evidence: Mapping[str, Any],
    staging_service_plan: Mapping[str, Any],
    production_service_plan: Mapping[str, Any],
    approved_by: str,
    approved_at: datetime,
    max_age_seconds: int,
    promotion_id: str | None = None,
) -> PromotionApproval:
    """Bind one fresh staging result to exact production resources and bytes."""
    actor = approved_by.strip()
    if not actor:
        raise _error("promotion approval requires an actor")
    if staging.tier != "staging" or production.tier != "production":
        raise _error(
            "promotion requires staging and production environment tiers",
            staging_tier=staging.tier,
            production_tier=production.tier,
        )
    validate_staging_promotion_posture(staging)
    # Staging and production deliberately hold different definition files
    # (staging's is hardened to deny external mutation and export, commit
    # 97b96750a), so whole-definition digests can never be equal. The
    # promotable invariant is shared SUBSTANCE: identical assets and graph
    # semantics, with only per-tier identity/policy allowed to differ.
    substance_problems = shared_definition_substance_problems(
        staging.definition_substance,
        production.definition_substance,
    )
    if substance_problems:
        raise _error(
            "staging and production shared foundation substance differs",
            problems=list(substance_problems),
        )
    verified = verify_sealed_release(
        release.artifact_root,
        expected_inputs=release.inputs,
        expected_artifact_id=release.artifact_id,
    )
    if verified.inputs.definition_digest != staging.definition_digest:
        raise _error(
            "sealed release definition differs from the staging environment"
            " it was sealed from",
            release_definition_digest=verified.inputs.definition_digest,
            staging_definition_digest=staging.definition_digest,
        )
    if verified.artifact_root.parent != production.release.artifact_store.resolve():
        raise _error(
            "approved release is outside the production artifact store",
            expected=str(production.release.artifact_store.resolve()),
            observed=str(verified.artifact_root.parent),
        )
    approved = _utc(approved_at)
    if max_age_seconds <= 0:
        raise _error("promotion approval max age must be positive")
    readiness_digest = _validate_fresh_evidence(
        staging_readiness,
        label="staging readiness",
        schema="okg.environment_readiness/v1",
        environment_id=staging.id,
        artifact_id=verified.artifact_id,
        now=approved,
        max_age_seconds=max_age_seconds,
    )
    supervision_digest = _validate_fresh_evidence(
        supervision_evidence,
        label="service supervision",
        schema="okg.environment_service_supervision_evidence/v1",
        environment_id=staging.id,
        artifact_id=verified.artifact_id,
        now=approved,
        max_age_seconds=max_age_seconds,
    )
    _validate_staging_readiness_contract(
        staging_readiness,
        environment=staging,
    )
    _validate_supervision_evidence_contract(
        supervision_evidence,
        environment=staging,
    )
    if staging_readiness.get("status") != "ready":
        raise _error("staging readiness is not in the ready state")
    if supervision_evidence.get("status") != "completed":
        raise _error("service supervision evidence is not completed")
    staging_plan_digest = _validate_service_plan(
        staging_service_plan,
        environment=staging,
    )
    production_plan_digest = _validate_service_plan(
        production_service_plan,
        environment=production,
    )
    if supervision_evidence.get("plan_digest") != staging_service_plan.get(
        "plan_digest"
    ):
        raise _error(
            "service supervision evidence does not match the staged topology",
            expected=staging_service_plan.get("plan_digest"),
            observed=supervision_evidence.get("plan_digest"),
        )
    resources = [item.to_dict() for item in environment_resource_identities(production)]
    resources_digest = _digest(resources)
    evidence_expires_at = min(
        _parse_time(staging_readiness.get("captured_at"), "staging readiness captured_at"),
        _parse_time(
            supervision_evidence.get("captured_at"),
            "service supervision captured_at",
        ),
    )
    expires_at = datetime.fromtimestamp(
        evidence_expires_at.timestamp() + max_age_seconds, tz=timezone.utc
    )
    identity = {
        "artifact_id": verified.artifact_id,
        "artifact_input_id": verified.inputs.input_id,
        "staging_environment_id": staging.id,
        "production_environment_id": production.id,
        "staging_descriptor_digest": staging.descriptor_digest,
        "production_descriptor_digest": production.descriptor_digest,
        "definition_digest": verified.inputs.definition_digest,
        "production_definition_digest": production.definition_digest,
        "staging_evidence_digest": readiness_digest,
        "supervision_evidence_digest": supervision_digest,
        "staging_service_plan_digest": staging_plan_digest,
        "production_service_plan_digest": production_plan_digest,
        "production_resources_digest": resources_digest,
        "approved_at": approved.isoformat(),
        "expires_at": expires_at.isoformat(),
        "approved_by": actor,
    }
    resolved_id = promotion_id or _digest(identity)
    _validate_promotion_id(resolved_id)
    return PromotionApproval(promotion_id=resolved_id, **identity)


def run_deployment_promotion(
    *,
    approval: PromotionApproval,
    staging: DeploymentEnvironment,
    production: DeploymentEnvironment,
    release: SealedRelease,
    staging_readiness: Mapping[str, Any],
    supervision_evidence: Mapping[str, Any],
    staging_service_plan: Mapping[str, Any],
    production_service_plan: Mapping[str, Any],
    operations: PromotionOperations,
    now: datetime,
    state_path: Path | None = None,
) -> dict[str, Any]:
    """Run or resume one promotion, rolling back every post-mutation failure."""
    if isinstance(approval, ProductPromotionApprovalV3):
        raise _error(
            "product approval v3 requires the production apply/resume controller",
            promotion_id=approval.promotion_id,
        )
    _validate_promotion_id(approval.promotion_id)
    path = state_path or (
        production.runtime.state_root
        / "promotions"
        / f"{approval.promotion_id}.json"
    )
    lock_path = production.runtime.state_root / "promotions" / "promotion.lock"
    with _promotion_lock(lock_path):
        identity = _state_identity(approval)
        state = _load_state(path)
        if state is not None and state.get("product_lifecycle_mode") == (
            PRODUCT_ADMISSION_JOURNAL_MODE
        ):
            raise _error(
                "admission-only product journal requires the production controller",
                promotion_id=approval.promotion_id,
                state_path=str(path),
            )
        recovery_only = False
        if state is None:
            verified = _validate_execution_binding(
                approval=approval,
                staging=staging,
                production=production,
                release=release,
                staging_readiness=staging_readiness,
                supervision_evidence=supervision_evidence,
                staging_service_plan=staging_service_plan,
                production_service_plan=production_service_plan,
                now=now,
            )
            state = {
                "schema": PROMOTION_SCHEMA,
                **identity,
                "status": "approved",
                "completed_transitions": [],
                "transition_results": {},
                "operation_intents": {},
                "mutation_boundary": "none",
                "writers_fenced": False,
                "database_changed": False,
                "updated_at": _utc(now).isoformat(),
            }
            _write_state(path, state)
        else:
            mismatches = {
                key: {"expected": value, "observed": state.get(key)}
                for key, value in identity.items()
                if state.get(key) != value
            }
            if mismatches:
                raise _error(
                    "promotion resume identity changed; create a new promotion",
                    mismatches=mismatches,
                    state_path=str(path),
                )
            if state.get("status") in {
                "aborted",
                "completed",
                "failed",
                "rolled_back",
            }:
                return _result(state, path)
            state.setdefault("operation_intents", {})
            recovery_only = (
                state.get("status")
                in {"mutating", "rollback_required", "aborting", "recovery_blocked"}
                or state.get("mutation_boundary") != "none"
            )
            verified = _validate_recovery_binding(
                approval=approval,
                staging=staging,
                production=production,
                release=release,
            )

        context = PromotionContext(
            approval=approval,
            staging=staging,
            production=production,
            release=verified,
            state_path=path,
            state=state,
            inspect_operation=operations.inspect_operation,
        )
        if recovery_only:
            interrupted = _StepFailure(
                str((state.get("failure") or {}).get("step") or "interrupted_promotion"),
                {
                    "ok": False,
                    "reason": "interrupted promotion resumes through recovery only",
                    "prior_failure": state.get("failure"),
                    "prior_blocker": state.get("blocker"),
                },
            )
            if state.get("mutation_boundary") == "none":
                return _abort_before_mutation(
                    context, operations, failure=interrupted, now=now
                )
            return _rollback_after_mutation(
                context, operations, failure=interrupted, now=now
            )
        try:
            _run_pre_mutation(context, operations, now=now)
            _run_mutations(context, operations, now=now)
            _run_step(context, "reopen_writers", operations.reopen_writers, now=now)
            state.update(
                {
                    "status": "completed",
                    "writers_fenced": False,
                    "updated_at": _utc(now).isoformat(),
                }
            )
            _write_state(path, state)
            return _result(state, path)
        except _StepFailure as failure:
            if state.get("mutation_boundary") == "none":
                return _abort_before_mutation(
                    context,
                    operations,
                    failure=failure,
                    now=now,
                )
            return _rollback_after_mutation(
                context,
                operations,
                failure=failure,
                now=now,
            )


def admit_product_promotion_journal(
    *,
    request: ProductApplyRequest,
    production: DeploymentEnvironment,
    now: datetime,
    state_path: Path | None = None,
) -> dict[str, Any]:
    """Persist only an admitted v3 journal identity; never run effects.

    The production apply/resume controller and paired receipt verifier are a
    later owned slice.  This boundary therefore records an ``approved``
    operation with empty transition collections and cannot report success.
    """

    from okg.substrate.deployment_product_lifecycle import (
        ProductApplyRequest,
        admit_product_apply_request,
        approval_digest,
        load_product_promotion_approval,
    )

    if not isinstance(request, ProductApplyRequest):
        raise _error("product journal admission requires ProductApplyRequest")
    _validate_promotion_id(request.idempotency_key)
    current = _utc(now)
    path = state_path or (
        production.runtime.state_root
        / "promotions"
        / f"{request.idempotency_key}.json"
    )
    lock_path = production.runtime.state_root / "promotions" / "promotion.lock"
    with _promotion_lock(lock_path):
        admission = admit_product_apply_request(request, now=current)
        approval = load_product_promotion_approval(
            request.promotion_approval_path
        )
        if approval_digest(approval) != request.promotion_approval_digest:
            raise _error("product promotion approval changed during admission")
        _validate_product_approval(approval)
        if approval.transition_kind != "first_install":
            raise _error(
                "bounded product journal admission supports first install only",
                transition_kind=approval.transition_kind,
            )
        if current > _parse_time(approval.expires_at, "approval expiry"):
            raise _error(
                "product promotion approval is stale; create a new promotion",
                promotion_id=approval.promotion_id,
                expires_at=approval.expires_at,
                observed_at=current.isoformat(),
            )
        if (
            production.id != approval.production_environment_id
            or production.descriptor_digest != approval.production_descriptor_digest
            or production.definition_digest != approval.production_definition_digest
        ):
            raise _error(
                "product approval target authority changed",
                promotion_id=approval.promotion_id,
            )
        normalized_admission = _normalize_product_admission(approval, admission)
        identity = _product_state_identity(approval, normalized_admission)
        from okg.substrate.release_activation import release_activation_lock

        with release_activation_lock(
            production.release.active_pointer,
            exclusive=False,
        ):
            release_observation = _observe_product_release_pointers(
                approval=approval,
                production=production,
            )
            existing = _load_state(path)
            if existing is not None:
                _validate_admission_only_state(
                    existing,
                    identity=identity,
                    release_observation=release_observation,
                    state_path=path,
                )
                return _admission_result(existing, path)
            state: dict[str, Any] = {
                "schema": PROMOTION_SCHEMA,
                "product_lifecycle_mode": PRODUCT_ADMISSION_JOURNAL_MODE,
                "controller_status": "unavailable",
                **identity,
                "release_observation": release_observation,
                "status": "product_admitted",
                "completed_transitions": [],
                "transition_results": {},
                "operation_intents": {},
                "mutation_boundary": "none",
                "writers_fenced": False,
                "database_changed": False,
                "updated_at": current.isoformat(),
            }
            _validate_admission_only_state(
                state,
                identity=identity,
                release_observation=release_observation,
                state_path=path,
            )
            _write_state(path, state)
            return _admission_result(state, path)


def read_deployment_promotion(
    *,
    production: DeploymentEnvironment,
    promotion_id: str,
    state_path: Path | None = None,
) -> dict[str, Any]:
    """Read one journal under the same target lock used for admission/effects."""

    _validate_promotion_id(promotion_id)
    path = state_path or (
        production.runtime.state_root / "promotions" / f"{promotion_id}.json"
    )
    lock_path = production.runtime.state_root / "promotions" / "promotion.lock"
    with _promotion_lock(lock_path):
        state = _load_state(path)
        if state is None:
            raise _error(
                "promotion state does not exist",
                promotion_id=promotion_id,
                state_path=str(path),
            )
        if state.get("promotion_id") != promotion_id:
            raise _error(
                "promotion state identity changed",
                expected=promotion_id,
                observed=state.get("promotion_id"),
                state_path=str(path),
            )
        if (
            state.get("environment_id") != production.id
            or state.get("target_environment_id") != production.id
        ):
            raise _error(
                "promotion state belongs to a different production environment",
                expected=production.id,
                observed_environment=state.get("environment_id"),
                observed_target=state.get("target_environment_id"),
                state_path=str(path),
            )
        # Detach the caller's snapshot before releasing the authority lock.
        return json.loads(json.dumps(state, sort_keys=True))


def _run_pre_mutation(
    context: PromotionContext,
    operations: PromotionOperations,
    *,
    now: datetime,
) -> None:
    _run_step(
        context,
        "capture_previous_release",
        operations.capture_previous_release,
        now=now,
    )
    previous = context.state["transition_results"]["capture_previous_release"]
    if not previous.get("artifact_id"):
        raise _StepFailure(
            "capture_previous_release",
            {"ok": False, "reason": "previous artifact id is missing"},
        )
    _run_step(
        context,
        "capture_previous_health",
        operations.capture_previous_health,
        now=now,
    )
    health = context.state["transition_results"]["capture_previous_health"]
    if (
        health.get("artifact_id") != previous.get("artifact_id")
        or not health.get("generation_id")
        or not isinstance(health.get("health_ok"), bool)
    ):
        raise _StepFailure(
            "capture_previous_health",
            {
                "ok": False,
                "reason": "generation-pinned previous health baseline is incomplete",
                "expected_artifact_id": previous.get("artifact_id"),
                "observed_artifact_id": health.get("artifact_id"),
                "generation_id": health.get("generation_id"),
            },
        )
    context.state["writers_fenced"] = True
    _write_state(context.state_path, context.state)
    _run_step(context, "fence_writers", operations.fence_writers, now=now)
    _run_step(context, "drain_work", operations.drain_work, now=now)
    _run_step(context, "create_backup", operations.create_backup, now=now)
    _run_step(context, "verify_backup", operations.verify_backup, now=now)
    backup = context.state["transition_results"]["create_backup"]
    verification = context.state["transition_results"]["verify_backup"]
    required = ("restore_verified", "schema_ok", "ownership_ok")
    missing = [field for field in required if verification.get(field) is not True]
    if missing:
        raise _StepFailure(
            "verify_backup",
            {"ok": False, "reason": "backup verification incomplete", "missing": missing},
        )
    if verification.get("backup_id") != backup.get("backup_id"):
        raise _StepFailure(
            "verify_backup",
            {
                "ok": False,
                "reason": "backup verification proved a different backup",
                "expected": backup.get("backup_id"),
                "observed": verification.get("backup_id"),
            },
        )


def _run_mutations(
    context: PromotionContext,
    operations: PromotionOperations,
    *,
    now: datetime,
) -> None:
    steps: tuple[tuple[str, str, Step], ...] = (
        ("database", "migrate_database", operations.migrate_database),
        ("policy", "activate_policy", operations.activate_policy),
        ("release", "activate_release", operations.activate_release),
        ("services", "restart_services", operations.restart_services),
        ("publish", "publish_generation", operations.publish_generation),
        ("readiness", "prove_production_ready", operations.prove_production_ready),
    )
    for boundary, name, callback in steps:
        if name in context.state["completed_transitions"]:
            continue
        context.state.update(
            {
                "status": "mutating",
                "mutation_boundary": boundary,
                "database_changed": (
                    context.state.get("database_changed") or boundary == "database"
                ),
                "updated_at": _utc(now).isoformat(),
            }
        )
        _write_state(context.state_path, context.state)
        _run_step(context, name, callback, now=now)
        result = context.state["transition_results"][name]
        if name in {"activate_release", "prove_production_ready"} and result.get(
            "artifact_id"
        ) != context.approval.artifact_id:
            raise _StepFailure(
                name,
                {
                    "ok": False,
                    "reason": "operation reported the wrong candidate artifact",
                    "expected": context.approval.artifact_id,
                    "observed": result.get("artifact_id"),
                },
            )


def _abort_before_mutation(
    context: PromotionContext,
    operations: PromotionOperations,
    *,
    failure: _StepFailure,
    now: datetime,
) -> dict[str, Any]:
    state = context.state
    state.update(
        {
            "status": "aborting",
            "failure": _failure_payload(failure),
            "updated_at": _utc(now).isoformat(),
        }
    )
    _write_state(context.state_path, state)
    if not state.get("writers_fenced"):
        state["status"] = "failed"
        _write_state(context.state_path, state)
        return _result(state, context.state_path)
    try:
        _run_step(
            context,
            "restart_previous_services",
            operations.restart_previous_services,
            now=now,
        )
        _run_step(
            context,
            "prove_previous_ready",
            operations.prove_previous_ready,
            now=now,
        )
        _run_step(context, "reopen_writers", operations.reopen_writers, now=now)
    except _StepFailure as recovery_failure:
        return _block_recovery(context, recovery_failure, now=now)
    state.update(
        {
            "status": "aborted",
            "writers_fenced": False,
            "updated_at": _utc(now).isoformat(),
        }
    )
    _write_state(context.state_path, state)
    return _result(state, context.state_path)


def validate_staging_promotion_posture(
    environment: DeploymentEnvironment,
) -> None:
    """Require the staging definition's non-exporting, no-live-call posture."""
    if environment.tier != "staging":
        raise _error(
            "promotion posture check requires a staging environment",
            environment_id=environment.id,
            observed_tier=environment.tier,
        )
    nomos = getattr(environment.manifest, "nomos", None) or {}
    posture = nomos.get("deployment_posture") if isinstance(nomos, Mapping) else None
    posture = posture if isinstance(posture, Mapping) else {}
    expected = {
        "export_policy": "no_export",
        "live_call_default": "deny",
        "allowed_exportability": ["no_export"],
    }
    mismatches = {
        field: {"expected": wanted, "observed": posture.get(field)}
        for field, wanted in expected.items()
        if posture.get(field) != wanted
    }
    if mismatches:
        raise _error(
            "staging deployment posture is not hardened",
            environment_id=environment.id,
            mismatches=mismatches,
        )


def _rollback_after_mutation(
    context: PromotionContext,
    operations: PromotionOperations,
    *,
    failure: _StepFailure,
    now: datetime,
) -> dict[str, Any]:
    state = context.state
    state.update(
        {
            "status": "rollback_required",
            "failure": _failure_payload(failure),
            "updated_at": _utc(now).isoformat(),
        }
    )
    _write_state(context.state_path, state)
    try:
        if state.get("database_changed"):
            _run_step(context, "restore_backup", operations.restore_backup, now=now)
        _run_step(
            context,
            "activate_previous_release",
            operations.activate_previous_release,
            now=now,
        )
        _require_previous_artifact(context, "activate_previous_release")
        _run_step(
            context,
            "restart_previous_services",
            operations.restart_previous_services,
            now=now,
        )
        _run_step(
            context,
            "prove_previous_ready",
            operations.prove_previous_ready,
            now=now,
        )
        _require_previous_artifact(context, "prove_previous_ready")
        _run_step(context, "reopen_writers", operations.reopen_writers, now=now)
    except _StepFailure as recovery_failure:
        return _block_recovery(context, recovery_failure, now=now)
    state.update(
        {
            "status": "rolled_back",
            "writers_fenced": False,
            "updated_at": _utc(now).isoformat(),
        }
    )
    _write_state(context.state_path, state)
    return _result(state, context.state_path)


def _block_recovery(
    context: PromotionContext,
    failure: _StepFailure,
    *,
    now: datetime,
) -> dict[str, Any]:
    completed = set(context.state.get("completed_transitions") or [])
    context.state.update(
        {
            "status": "recovery_blocked",
            "writers_fenced": True,
            "blocker": _failure_payload(failure),
            "recovery_dispositions": {
                "database": (
                    "restored"
                    if "restore_backup" in completed
                    else "unchanged"
                    if not context.state.get("database_changed")
                    else "not_restored"
                ),
                "release": (
                    "restored"
                    if "activate_previous_release" in completed
                    else "not_restored"
                ),
                "services": (
                    "restored"
                    if "restart_previous_services" in completed
                    else "not_restored"
                ),
                "persistent_health": (
                    "clean"
                    if "prove_previous_ready" in completed
                    else "failed"
                    if failure.step == "prove_previous_ready"
                    else "not_proven"
                ),
                "writers": "fenced",
            },
            "updated_at": _utc(now).isoformat(),
        }
    )
    _write_state(context.state_path, context.state)
    return _result(context.state, context.state_path)


def _run_step(
    context: PromotionContext,
    name: str,
    callback: Step,
    *,
    now: datetime,
) -> None:
    state = context.state
    if name in state["completed_transitions"]:
        return
    intents = state.setdefault("operation_intents", {})
    intent = intents.get(name)
    if (
        not isinstance(intent, dict)
        or intent.get("status") == "failed"
        or intent.get("last_error") is not None
    ):
        attempt = int((intent or {}).get("attempt") or 0) + 1
        operation_id = _digest(
            {
                "promotion_id": context.approval.promotion_id,
                "operation": name,
                "attempt": attempt,
            }
        )
        intent = {
            "operation": name,
            "operation_id": operation_id,
            "attempt": attempt,
            "status": "started",
            "started_at": _utc(now).isoformat(),
        }
        intents[name] = intent
        state["active_operation"] = dict(intent)
        _write_state(context.state_path, state)
        result_source = "execute"
    else:
        operation_id = str(intent.get("operation_id") or "")
        state["active_operation"] = dict(intent)
        result_source = "inspect"
    try:
        if result_source == "inspect":
            if context.inspect_operation is not None:
                observed = context.inspect_operation(context, name, operation_id)
            else:
                observed = None
            if observed is None:
                raise _StepFailure(
                    name,
                    {
                        "ok": False,
                        "reason": "operation outcome is ambiguous after interruption",
                        "operation_id": operation_id,
                    },
                )
            result = dict(observed)
        else:
            result = dict(callback(context))
    except _StepFailure:
        raise
    except Exception as exc:  # noqa: BLE001 - operation failures become state.
        error = _operation_error(exc)
        intent.update(
            {
                "status": "failed",
                "last_error": error,
            }
        )
        _write_state(context.state_path, state)
        raise _StepFailure(
            name,
            {"ok": False, **error},
        ) from exc
    if result.get("ok") is not True:
        intent.update({"status": "failed", "receipt": result})
        _write_state(context.state_path, state)
        raise _StepFailure(name, result)
    _validate_operation_receipt(context, name, operation_id, result)
    intent.update(
        {
            "status": "completed",
            "receipt_digest": result["receipt_digest"],
            "completed_at": _utc(now).isoformat(),
        }
    )
    state["transition_results"][name] = result
    state["completed_transitions"].append(name)
    state["completed_transition"] = name
    state["updated_at"] = _utc(now).isoformat()
    _write_state(context.state_path, state)


def _validate_operation_receipt(
    context: PromotionContext,
    name: str,
    operation_id: str,
    result: Mapping[str, Any],
) -> None:
    expected = {
        "operation": name,
        "operation_id": operation_id,
        "authoritative": True,
    }
    mismatches = {
        field: {"expected": value, "observed": result.get(field)}
        for field, value in expected.items()
        if result.get(field) != value
    }
    digest = result.get("receipt_digest")
    if not isinstance(digest, str) or not digest.startswith("sha256:"):
        mismatches["receipt_digest"] = {
            "expected": "sha256:<receipt>",
            "observed": digest,
        }
    else:
        receipt_body = dict(result)
        receipt_body.pop("receipt_digest", None)
        expected_digest = _digest(receipt_body)
        if digest != expected_digest:
            mismatches["receipt_digest"] = {
                "expected": expected_digest,
                "observed": digest,
            }
    if name == "fence_writers":
        fence_expected = {
            "deployment_name": context.production.graph_deployment_name,
            "promotion_id": context.approval.promotion_id,
            "status": "fenced",
        }
        mismatches.update(
            {
                field: {"expected": value, "observed": result.get(field)}
                for field, value in fence_expected.items()
                if result.get(field) != value
            }
        )
    elif name == "drain_work":
        if result.get("drained") is not True or result.get("pending_workflows") != 0:
            mismatches["drain"] = {
                "expected": {"drained": True, "pending_workflows": 0},
                "observed": {
                    "drained": result.get("drained"),
                    "pending_workflows": result.get("pending_workflows"),
                },
            }
    elif name == "create_backup":
        for field in ("backup_id", "backup_digest", "backup_path"):
            if not result.get(field):
                mismatches[field] = {"expected": "non-empty", "observed": result.get(field)}
    elif name == "restore_backup":
        backup = context.state["transition_results"].get("create_backup") or {}
        if result.get("backup_id") != backup.get("backup_id"):
            mismatches["backup_id"] = {
                "expected": backup.get("backup_id"),
                "observed": result.get("backup_id"),
            }
    elif name == "reopen_writers" and result.get("released") is not True:
        mismatches["released"] = {"expected": True, "observed": result.get("released")}
    if mismatches:
        raise _StepFailure(
            name,
            {"ok": False, "reason": "operation receipt is not authoritative", "mismatches": mismatches},
        )


def _operation_error(exc: Exception) -> dict[str, Any]:
    error: dict[str, Any] = {
        "error": type(exc).__name__,
        "message": str(exc),
    }
    if isinstance(exc, SubstrateError):
        error.update({"code": exc.code, "data": dict(exc.data)})
        if exc.fix_hint:
            error["fix_hint"] = exc.fix_hint
    return error


def _validate_execution_binding(
    *,
    approval: PromotionApproval,
    staging: DeploymentEnvironment,
    production: DeploymentEnvironment,
    release: SealedRelease,
    staging_readiness: Mapping[str, Any],
    supervision_evidence: Mapping[str, Any],
    staging_service_plan: Mapping[str, Any],
    production_service_plan: Mapping[str, Any],
    now: datetime,
) -> SealedRelease:
    current = _utc(now)
    if current > _parse_time(approval.expires_at, "approval expiry"):
        raise _error(
            "promotion approval is stale; create a new promotion",
            promotion_id=approval.promotion_id,
            expires_at=approval.expires_at,
            observed_at=current.isoformat(),
        )
    verified = verify_sealed_release(
        release.artifact_root,
        expected_inputs=release.inputs,
        expected_artifact_id=approval.artifact_id,
    )
    validate_staging_promotion_posture(staging)
    substance_problems = shared_definition_substance_problems(
        staging.definition_substance,
        production.definition_substance,
    )
    if substance_problems:
        raise _error(
            "staging and production shared foundation substance differs",
            problems=list(substance_problems),
        )
    checks = {
        "artifact_input_id": (approval.artifact_input_id, verified.inputs.input_id),
        "staging_environment_id": (approval.staging_environment_id, staging.id),
        "production_environment_id": (
            approval.production_environment_id,
            production.id,
        ),
        "staging_descriptor_digest": (
            approval.staging_descriptor_digest,
            staging.descriptor_digest,
        ),
        "production_descriptor_digest": (
            approval.production_descriptor_digest,
            production.descriptor_digest,
        ),
        "definition_digest": (approval.definition_digest, staging.definition_digest),
        "release_definition_digest": (
            approval.definition_digest,
            verified.inputs.definition_digest,
        ),
        "production_definition_digest": (
            approval.production_definition_digest,
            production.definition_digest,
        ),
        "staging_evidence_digest": (
            approval.staging_evidence_digest,
            _digest(staging_readiness),
        ),
        "supervision_evidence_digest": (
            approval.supervision_evidence_digest,
            _digest(supervision_evidence),
        ),
        "staging_service_plan_digest": (
            approval.staging_service_plan_digest,
            _validate_service_plan(staging_service_plan, environment=staging),
        ),
        "production_service_plan_digest": (
            approval.production_service_plan_digest,
            _validate_service_plan(production_service_plan, environment=production),
        ),
        "production_resources_digest": (
            approval.production_resources_digest,
            _digest(
                [
                    item.to_dict()
                    for item in environment_resource_identities(production)
                ]
            ),
        ),
    }
    mismatches = {
        field: {"expected": expected, "observed": observed}
        for field, (expected, observed) in checks.items()
        if expected != observed
    }
    if mismatches:
        raise _error(
            "promotion artifact, evidence, topology, or resource identity changed",
            promotion_id=approval.promotion_id,
            mismatches=mismatches,
        )
    return verified


def _validate_recovery_binding(
    *,
    approval: PromotionApproval,
    staging: DeploymentEnvironment,
    production: DeploymentEnvironment,
    release: SealedRelease,
) -> SealedRelease:
    """Verify immutable recovery identity without freshness-gating rollback."""
    verified = verify_sealed_release(
        release.artifact_root,
        expected_inputs=release.inputs,
        expected_artifact_id=approval.artifact_id,
    )
    validate_staging_promotion_posture(staging)
    substance_problems = shared_definition_substance_problems(
        staging.definition_substance,
        production.definition_substance,
    )
    if substance_problems:
        raise _error(
            "staging and production shared foundation substance differs",
            problems=list(substance_problems),
        )
    checks = {
        "artifact_input_id": (approval.artifact_input_id, verified.inputs.input_id),
        "staging_environment_id": (approval.staging_environment_id, staging.id),
        "staging_descriptor_digest": (
            approval.staging_descriptor_digest,
            staging.descriptor_digest,
        ),
        "production_environment_id": (
            approval.production_environment_id,
            production.id,
        ),
        "production_descriptor_digest": (
            approval.production_descriptor_digest,
            production.descriptor_digest,
        ),
        "definition_digest": (
            approval.definition_digest,
            verified.inputs.definition_digest,
        ),
        "production_definition_digest": (
            approval.production_definition_digest,
            production.definition_digest,
        ),
        "production_resources_digest": (
            approval.production_resources_digest,
            _digest(
                [
                    item.to_dict()
                    for item in environment_resource_identities(production)
                ]
            ),
        ),
    }
    mismatches = {
        field: {"expected": expected, "observed": observed}
        for field, (expected, observed) in checks.items()
        if expected != observed
    }
    if mismatches:
        raise _error(
            "promotion recovery identity changed",
            promotion_id=approval.promotion_id,
            mismatches=mismatches,
        )
    return verified


def _validate_fresh_evidence(
    evidence: Mapping[str, Any],
    *,
    label: str,
    schema: str,
    environment_id: str,
    artifact_id: str,
    now: datetime,
    max_age_seconds: int,
) -> str:
    if evidence.get("schema") != schema:
        raise _error(
            f"{label} has the wrong schema",
            expected=schema,
            observed=evidence.get("schema"),
        )
    if evidence.get("ok") is not True:
        raise _error(f"{label} did not pass", evidence=dict(evidence))
    if evidence.get("environment_id") != environment_id:
        raise _error(
            f"{label} belongs to a different environment",
            expected=environment_id,
            observed=evidence.get("environment_id"),
        )
    observed_artifact = (
        evidence.get("artifact_id")
        or (evidence.get("release") or {}).get("artifact_id")
    )
    if observed_artifact != artifact_id:
        raise _error(
            f"{label} belongs to a different artifact",
            expected=artifact_id,
            observed=observed_artifact,
        )
    captured = _parse_time(evidence.get("captured_at"), f"{label} captured_at")
    age = (now - captured).total_seconds()
    if age < 0 or age > max_age_seconds:
        raise _error(
            f"{label} is stale",
            captured_at=captured.isoformat(),
            observed_at=now.isoformat(),
            age_seconds=age,
            max_age_seconds=max_age_seconds,
        )
    return _digest(evidence)


def _validate_staging_readiness_contract(
    evidence: Mapping[str, Any],
    *,
    environment: DeploymentEnvironment,
) -> None:
    required_checks = {
        "release.active",
        "processes.authority",
        "services.installation",
        "database.migrations",
        "generation.latest_published",
        "queues.bounds",
        "sources.freshness",
        "doctor.okg_workspace",
        "pact.prerequisites",
        "ui.http",
        "webhook.owner",
        "nomos.read_path",
        "mcp.registration",
        "mcp.release_authority",
        "mcp.inspect",
        "mcp.search",
        "mcp.generation_alignment",
    }
    if environment.readiness.requires_clone_rebind:
        required_checks.add("clone.regeneration")
    checks = evidence.get("checks")
    if not isinstance(checks, list):
        raise _error("staging readiness has no check inventory")
    by_id = {
        str(row.get("id")): row
        for row in checks
        if isinstance(row, Mapping) and row.get("id")
    }
    missing = sorted(required_checks - set(by_id))
    failed = sorted(
        check_id
        for check_id in required_checks & set(by_id)
        if by_id[check_id].get("status") != "pass"
        or by_id[check_id].get("ok") is False
    )
    if missing or failed:
        raise _error(
            "staging readiness is incomplete",
            missing_checks=missing,
            failed_checks=failed,
        )
    queues = evidence.get("queues") or {}
    source_freshness = evidence.get("source_freshness") or {}
    doctor = evidence.get("doctor") or {}
    prerequisites = evidence.get("prerequisite_pacts") or {}
    generation = evidence.get("generation") or {}
    clone_regeneration = evidence.get("clone_regeneration") or {}
    expected_clone_path = environment.runtime.state_root / "clone-rebind.json"
    required_regeneration_classes = {
        "database_credentials",
        "nomos_baseline",
        "source_progress",
        "dbos_schedules",
        "mcp_registration",
        "readiness_evidence",
    }
    regeneration_rows = clone_regeneration.get("regenerate") or ()
    regeneration_by_class = {
        str(row.get("class")): row
        for row in regeneration_rows
        if isinstance(row, Mapping) and row.get("class")
    }
    clone_regeneration_invalid = bool(
        environment.readiness.requires_clone_rebind
        and (
            clone_regeneration.get("ok") is not True
            or clone_regeneration.get("status") != "completed"
            or clone_regeneration.get("path") != str(expected_clone_path)
            or set(regeneration_by_class) != required_regeneration_classes
            or any(
                row.get("status") != "completed"
                or int(row.get("rows", row.get("resource_count", 0)) or 0) <= 0
                for row in regeneration_by_class.values()
            )
        )
    )
    if (
        evidence.get("evidence_path")
        != str(environment.readiness.evidence_path)
        or generation.get("status") != "published"
        or generation.get("deployment_name")
        != environment.graph_deployment_name
        or not generation.get("generation_id")
        or int(generation.get("node_count") or 0) <= 0
        or int(generation.get("edge_count") or 0) <= 0
        or queues.get("ok") is False
        or int(queues.get("active_queue_depth", 1)) > 0
        or int(queues.get("dlq_count", 1)) > 0
        or source_freshness.get("ok") is not True
        or source_freshness.get("missing")
        or source_freshness.get("stale")
        or doctor.get("ok") is not True
        or prerequisites.get("ok") is not True
        or clone_regeneration_invalid
    ):
        raise _error(
            "staging operational readiness is not promotion-safe",
            queues=queues,
            source_freshness=source_freshness,
            doctor=doctor,
            prerequisite_pacts=prerequisites,
            clone_regeneration=clone_regeneration,
        )


def _validate_supervision_evidence_contract(
    evidence: Mapping[str, Any],
    *,
    environment: DeploymentEnvironment,
) -> None:
    authority = evidence.get("process_authority") or {}
    installation = evidence.get("service_installation") or {}
    results = evidence.get("results") or []
    expected_path = (
        environment.runtime.state_root
        / "supervisor/promotion-supervision.json"
    )
    if (
        evidence.get("evidence_path") != str(expected_path)
        or not evidence.get("plan_digest")
        or evidence.get("action") not in {"start", "restart"}
        or not results
        or any(int(row.get("returncode", 1)) != 0 for row in results)
        or authority.get("ok") is not True
        or authority.get("status") != "healthy"
        or installation.get("ok") is not True
        or installation.get("status") != "current"
    ):
        raise _error(
            "service supervision evidence is incomplete",
            plan_digest=evidence.get("plan_digest"),
            action=evidence.get("action"),
            process_authority=authority,
            service_installation=installation,
        )


def _validate_service_plan(
    plan: Mapping[str, Any],
    *,
    environment: DeploymentEnvironment,
) -> str:
    from okg.substrate.deployments import DeploymentConfigError
    from okg.substrate.environment_services import (
        _validate_service_plan as validate_canonical_service_plan,
    )

    try:
        validate_canonical_service_plan(environment, plan)
    except DeploymentConfigError as exc:
        raise _error(
            "service plan failed canonical validation",
            environment_id=environment.id,
            reason=str(exc),
        ) from exc
    if plan.get("ok") is not True or plan.get("environment_id") != environment.id:
        raise _error(
            "service plan is invalid or belongs to another environment",
            environment_id=environment.id,
        )
    digest = plan.get("plan_digest")
    if not isinstance(digest, str) or not digest.startswith("sha256:"):
        raise _error("service plan has no content digest", environment_id=environment.id)
    artifacts = [item for item in plan.get("artifacts") or () if isinstance(item, Mapping)]
    launcher = next((item for item in artifacts if item.get("kind") == "launcher"), None)
    worker = next((item for item in artifacts if item.get("kind") == "worker_service"), None)
    if launcher is None or worker is None:
        raise _error(
            "service plan omits the trusted launcher or worker service",
            environment_id=environment.id,
        )
    launcher_path = str(launcher.get("path") or "")
    worker_content = str(worker.get("content") or "")
    if not launcher_path or launcher_path not in worker_content:
        raise _error(
            "worker service does not enter the trusted release launcher",
            environment_id=environment.id,
            launcher_path=launcher_path,
        )
    return digest


def _require_previous_artifact(context: PromotionContext, step: str) -> None:
    previous = context.state["transition_results"].get("capture_previous_release") or {}
    result = context.state["transition_results"].get(step) or {}
    expected = previous.get("artifact_id")
    if expected and result.get("artifact_id") != expected:
        raise _StepFailure(
            step,
            {
                "ok": False,
                "reason": "operation reported the wrong previous artifact",
                "expected": expected,
                "observed": result.get("artifact_id"),
            },
        )


def _sha256_value(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", value):
        raise _error(f"{field} must be an exact sha256 digest", field=field)
    return value


def _validate_product_approval(approval: ProductPromotionApprovalV3) -> None:
    if approval.transition_kind not in {"first_install", "upgrade"}:
        raise _error(
            "product promotion approval has an invalid transition kind",
            transition_kind=approval.transition_kind,
        )
    if approval.transition_kind == "first_install":
        if approval.expected_previous_artifact_id is not None:
            raise _error(
                "first-install approval cannot name a previous artifact",
                expected_previous_artifact_id=approval.expected_previous_artifact_id,
            )
    elif approval.expected_previous_artifact_id is None:
        raise _error("upgrade approval requires the expected active artifact")
    else:
        _sha256_value(
            approval.expected_previous_artifact_id,
            field="expected_previous_artifact_id",
        )
    for field in (
        "artifact_id",
        "artifact_input_id",
        "staging_descriptor_digest",
        "production_descriptor_digest",
        "definition_digest",
        "production_definition_digest",
        "staging_evidence_digest",
        "supervision_evidence_digest",
        "staging_service_plan_digest",
        "production_service_plan_digest",
        "production_resources_digest",
        "request_digest",
        "product_plan_digest",
        "lock_digest",
        "composition_digest",
    ):
        _sha256_value(getattr(approval, field), field=field)
    if not approval.approved_by.strip():
        raise _error("product promotion approval requires an actor")
    if _parse_time(approval.approved_at, "approval time") >= _parse_time(
        approval.expires_at, "approval expiry"
    ):
        raise _error("product promotion approval expiry must follow approval time")


def _normalize_product_admission(
    approval: ProductPromotionApprovalV3,
    admission: Mapping[str, Any],
) -> dict[str, Any]:
    expected_fields = {
        "schema",
        "request_digest",
        "approval_digest",
        "admission_digest",
        "product_plan_digest",
        "lock_digest",
        "composition_digest",
        "definition_id",
        "definition_digest",
        "sealed_artifact_digest",
        "target_environment_id",
        "transition_kind",
        "package_transport_digest",
        "package_selection_digest",
        "package_selection",
        "package_artifact_bindings",
    }
    if not isinstance(admission, Mapping):
        raise _error("product journal requires an exact plan admission")
    if set(admission) != expected_fields:
        raise _error(
            "product admission fields changed",
            missing=sorted(expected_fields - set(admission)),
            unexpected=sorted(set(admission) - expected_fields),
        )
    if admission.get("schema") != PRODUCT_ADMISSION_SCHEMA:
        raise _error(
            "product admission has the wrong schema",
            observed=admission.get("schema"),
        )
    normalized: dict[str, Any] = {}
    string_fields = expected_fields - {
        "package_selection",
        "package_artifact_bindings",
    }
    for field in string_fields:
        value = admission[field]
        if not isinstance(value, str) or not value:
            raise _error("product admission field has the wrong type", field=field)
        normalized[field] = value
    for field in string_fields - {
        "schema",
        "definition_id",
        "target_environment_id",
        "transition_kind",
    }:
        _sha256_value(normalized[field], field=field)
    from okg.substrate.deployment_product_lifecycle import (
        admission_digest,
        approval_digest,
        package_selection_from_mapping,
        require_package_artifact_bindings_match_selection,
    )

    selection = package_selection_from_mapping(admission["package_selection"])
    bindings = require_package_artifact_bindings_match_selection(
        admission["package_artifact_bindings"],
        selection,
    )
    normalized["package_selection"] = selection.to_mapping()
    normalized["package_artifact_bindings"] = [
        item.to_mapping() for item in bindings
    ]
    expected_admission_digest = admission_digest(
        request_digest=normalized["request_digest"],
        approval_digest=normalized["approval_digest"],
    )
    checks = {
        "approval_digest": approval_digest(approval),
        "admission_digest": expected_admission_digest,
        "request_digest": approval.request_digest,
        "product_plan_digest": approval.product_plan_digest,
        "lock_digest": approval.lock_digest,
        "composition_digest": approval.composition_digest,
        "target_environment_id": approval.production_environment_id,
        "transition_kind": approval.transition_kind,
        "definition_digest": approval.production_definition_digest,
        "sealed_artifact_digest": approval.artifact_id,
        "package_transport_digest": selection.transport_digest,
        "package_selection_digest": selection.digest,
    }
    mismatches = {
        field: {"expected": expected, "observed": normalized.get(field)}
        for field, expected in checks.items()
        if normalized.get(field) != expected
    }
    if mismatches:
        raise _error(
            "product admission identity changed",
            promotion_id=approval.promotion_id,
            mismatches=mismatches,
        )
    return normalized


def _observe_product_release_pointers(
    *,
    approval: ProductPromotionApprovalV3,
    production: DeploymentEnvironment,
) -> dict[str, Any]:
    from okg.substrate.release_activation import load_active_release

    active = load_active_release(production.release.active_pointer, required=False)
    previous = load_active_release(production.release.previous_pointer, required=False)
    active_artifact = active.get("artifact_id") if active is not None else None
    previous_artifact = previous.get("artifact_id") if previous is not None else None
    if active is None and previous is not None:
        raise _error(
            "previous release pointer exists without an active release",
            previous_artifact_id=previous_artifact,
        )
    if approval.transition_kind == "first_install":
        if active is not None or previous is not None:
            raise _error(
                "first install requires active and previous release pointers absent",
                active_artifact_id=active_artifact,
                previous_artifact_id=previous_artifact,
            )
    elif active_artifact != approval.expected_previous_artifact_id:
        raise _error(
            "upgrade active release differs from the approved previous artifact",
            expected=approval.expected_previous_artifact_id,
            observed=active_artifact,
        )
    return {
        "transition_kind": approval.transition_kind,
        "active_artifact_id": active_artifact,
        "previous_artifact_id": previous_artifact,
        "active_absent": active is None,
        "previous_absent": previous is None,
    }


def _product_state_identity(
    approval: ProductPromotionApprovalV3,
    admission: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "environment_id": approval.production_environment_id,
        "promotion_id": approval.promotion_id,
        "approval_digest": approval.digest,
        "artifact_id": approval.artifact_id,
        "artifact_input_id": approval.artifact_input_id,
        "staging_descriptor_digest": approval.staging_descriptor_digest,
        "production_descriptor_digest": approval.production_descriptor_digest,
        "supervision_evidence_digest": approval.supervision_evidence_digest,
        "production_service_plan_digest": approval.production_service_plan_digest,
        "production_resources_digest": approval.production_resources_digest,
        "product_admission": dict(admission),
        "request_digest": admission["request_digest"],
        "admission_digest": admission["admission_digest"],
        "product_plan_digest": admission["product_plan_digest"],
        "lock_digest": admission["lock_digest"],
        "composition_digest": admission["composition_digest"],
        "definition_id": admission["definition_id"],
        "definition_digest": admission["definition_digest"],
        "sealed_artifact_digest": admission["sealed_artifact_digest"],
        "target_environment_id": admission["target_environment_id"],
        "package_transport_digest": admission["package_transport_digest"],
        "package_selection_digest": admission["package_selection_digest"],
        "package_selection": dict(admission["package_selection"]),
        "package_artifact_bindings": [
            dict(item) for item in admission["package_artifact_bindings"]
        ],
        "transition_kind": admission["transition_kind"],
    }


def _validate_admission_only_state(
    state: Mapping[str, Any],
    *,
    identity: Mapping[str, Any],
    release_observation: Mapping[str, Any],
    state_path: Path,
) -> None:
    fixed: dict[str, Any] = {
        "schema": PROMOTION_SCHEMA,
        "product_lifecycle_mode": PRODUCT_ADMISSION_JOURNAL_MODE,
        "controller_status": "unavailable",
        **identity,
        "release_observation": dict(release_observation),
        "status": "product_admitted",
        "completed_transitions": [],
        "transition_results": {},
        "operation_intents": {},
        "mutation_boundary": "none",
        "writers_fenced": False,
        "database_changed": False,
    }
    expected_fields = set(fixed) | {"updated_at"}
    if set(state) != expected_fields:
        raise _error(
            "admission-only product journal fields changed",
            missing=sorted(expected_fields - set(state)),
            unexpected=sorted(set(state) - expected_fields),
            state_path=str(state_path),
        )
    mismatches = {
        key: {"expected": value, "observed": state.get(key)}
        for key, value in fixed.items()
        if state.get(key) != value
    }
    if mismatches:
        raise _error(
            "admission-only product journal authority changed",
            mismatches=mismatches,
            state_path=str(state_path),
        )
    _parse_time(state.get("updated_at"), "product journal updated_at")


def _admission_result(state: Mapping[str, Any], state_path: Path) -> dict[str, Any]:
    return {
        "schema": PROMOTION_SCHEMA,
        "ok": False,
        "status": "product_admitted",
        "controller_status": "unavailable",
        "promotion_id": state["promotion_id"],
        "environment_id": state["environment_id"],
        "transition_kind": state["transition_kind"],
        "request_digest": state["request_digest"],
        "admission_digest": state["admission_digest"],
        "state_path": str(state_path),
    }


def _state_identity(approval: PromotionApproval) -> dict[str, Any]:
    return {
        "environment_id": approval.production_environment_id,
        "promotion_id": approval.promotion_id,
        "approval_digest": approval.digest,
        "artifact_id": approval.artifact_id,
        "artifact_input_id": approval.artifact_input_id,
        "staging_descriptor_digest": approval.staging_descriptor_digest,
        "production_descriptor_digest": approval.production_descriptor_digest,
        "supervision_evidence_digest": approval.supervision_evidence_digest,
        "production_service_plan_digest": approval.production_service_plan_digest,
        "production_resources_digest": approval.production_resources_digest,
    }


def _result(state: Mapping[str, Any], state_path: Path) -> dict[str, Any]:
    status = str(state.get("status") or "unknown")
    return {
        "schema": PROMOTION_SCHEMA,
        "ok": status == "completed",
        "status": status,
        "promotion_id": state.get("promotion_id"),
        "environment_id": state.get("environment_id"),
        "artifact_id": state.get("artifact_id"),
        "mutation_boundary": state.get("mutation_boundary"),
        "writers_fenced": state.get("writers_fenced"),
        "completed_transitions": list(state.get("completed_transitions") or ()),
        "failure": state.get("failure"),
        "blocker": state.get("blocker"),
        "recovery_dispositions": state.get("recovery_dispositions"),
        "state_path": str(state_path),
    }


def _failure_payload(failure: _StepFailure) -> dict[str, Any]:
    return {"step": failure.step, "details": dict(failure.details)}


def _load_state(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise _error("promotion state is not a regular file", state_path=str(path))
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise _error(
            "promotion state is unreadable",
            state_path=str(path),
            error=str(exc),
        ) from exc
    if not isinstance(raw, dict) or raw.get("schema") != PROMOTION_SCHEMA:
        raise _error("promotion state has the wrong schema", state_path=str(path))
    return raw


def _write_state(path: Path, state: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def _promotion_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise _error(
                "another promotion owns the production environment lock",
                lock_path=str(path),
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _parse_time(value: Any, label: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise _error(f"{label} is required")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise _error(f"{label} is invalid", observed=value) from exc
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise _error("promotion timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _validate_promotion_id(value: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", value):
        raise _error(
            "promotion id is invalid",
            promotion_id=value,
            allowed="1-128 ASCII letters, digits, dot, underscore, colon, or hyphen",
        )


def _digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _error(message: str, **data: Any) -> DeploymentEnvironmentInvalid:
    return DeploymentEnvironmentInvalid(
        message,
        data=data,
        fix_hint="create a new promotion from fresh staging and supervision evidence",
    )
