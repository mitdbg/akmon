"""Immutable records used by the external-action lifecycle."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from .action_contracts import format_timestamp


def _copy(value: Any) -> Any:
    """Return a JSON-shaped copy without exposing mutable store state."""

    if isinstance(value, Mapping):
        return {key: _copy(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_copy(child) for child in value]
    return value


def _command_fields(request: Mapping[str, Any]) -> dict[str, Any]:
    """Copy a command while omitting optional SQL ``NULL`` keys.

    The fixed SQL functions use JSON key presence for a few optional signed
    fields (for example ``previous_event_hash``).  Sending those keys with a
    JSON null would incorrectly activate the check, so absence is canonical.
    Required fields are validated by the SQL function and by the service.
    """

    return {key: _copy(value) for key, value in request.items() if value is not None}


@dataclass(frozen=True, slots=True)
class Actor:
    principal: str
    separation_subject: str
    authentication_audit_ref: str | None = None
    nomos_decision_ref: str | None = None
    nomos_provider_id: str | None = None
    nomos_subject_binding_id: str | None = None
    authentication_audit: Mapping[str, Any] | None = None

    @property
    def authentication_audit_id(self) -> str | None:
        """Canonical SQL name for the retained authentication audit."""

        return self.authentication_audit_ref

    @property
    def nomos_decision_id(self) -> str | None:
        """Canonical SQL name for the retained Nomos decision."""

        return self.nomos_decision_ref

    def with_authorization(
        self,
        *,
        authentication_audit_id: str | None = None,
        nomos_decision_id: str | None = None,
    ) -> "Actor":
        """Return an actor carrying transaction-bound authorization refs."""

        return type(self)(
            self.principal,
            self.separation_subject,
            authentication_audit_id or self.authentication_audit_ref,
            nomos_decision_id or self.nomos_decision_ref,
            self.nomos_provider_id,
            self.nomos_subject_binding_id,
            self.authentication_audit,
        )

    @classmethod
    def coerce(cls, value: "Actor | Mapping[str, Any] | str", *, subject: str | None = None) -> "Actor":
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            return cls(value, subject or value)
        if isinstance(value, Mapping):
            principal = value.get("principal", value.get("actor_principal"))
            stable = value.get("separation_subject", value.get("actor_separation_subject"))
            if stable is None:
                stable = subject or principal
            return cls(
                principal=str(principal), separation_subject=str(stable),
                authentication_audit_ref=value.get("authentication_audit_ref", value.get("authentication_audit_id", value.get("auth_audit_ref"))),
                nomos_decision_ref=value.get("nomos_decision_ref", value.get("nomos_decision_id")),
                nomos_provider_id=value.get("nomos_provider_id", value.get("provider_id")),
                nomos_subject_binding_id=value.get("nomos_subject_binding_id", value.get("subject_binding_id")),
                authentication_audit=value.get("authentication_audit", value.get("auth_audit")),
            )
        raise TypeError("actor must be an Actor, mapping, or principal string")


@dataclass(frozen=True, slots=True)
class PayloadVersion:
    proposal_id: str
    version: int
    payload_hash: str
    canonical_payload: bytes
    parent_payload_hash: str | None = None
    difference: tuple[str, ...] = ()
    created_at: datetime | str | None = None

    @property
    def payload(self) -> Any:
        import json

        return json.loads(self.canonical_payload)

    def to_projection(self, *, include_payload: bool = False) -> dict[str, Any]:
        result: dict[str, Any] = {
            "proposal_id": self.proposal_id,
            "payload_version": self.version,
            "payload_hash": self.payload_hash,
            "parent_payload_hash": self.parent_payload_hash,
            "difference": list(self.difference),
        }
        if include_payload:
            result["canonical_payload"] = self.canonical_payload.decode("utf-8")
        if self.created_at is not None:
            result["created_at"] = format_timestamp(self.created_at)
        return result


@dataclass(frozen=True, slots=True)
class Decision:
    decision_id: str
    proposal_id: str
    verdict: str
    lifecycle_version: int
    payload_version: int
    payload_hash: str
    approver_principal: str
    approver_separation_subject: str
    policy_digest: str | None
    execute_before: datetime | str | None = None
    reason_code: str | None = None
    decision_binding_hash: str | None = None
    created_at: datetime | str | None = None
    authentication_audit_id: str | None = None
    nomos_decision_id: str | None = None

    def to_projection(self) -> dict[str, Any]:
        result = {
            "decision_id": self.decision_id,
            "proposal_id": self.proposal_id,
            "verdict": self.verdict,
            "approved_lifecycle_version": self.lifecycle_version,
            "approved_payload_version": self.payload_version,
            "approved_payload_hash": self.payload_hash,
            "approver_principal": self.approver_principal,
            "approver_separation_subject": self.approver_separation_subject,
            "policy_digest": self.policy_digest,
            "execute_before": format_timestamp(self.execute_before) if self.execute_before is not None else None,
            "reason_code": self.reason_code,
            "decision_binding_hash": self.decision_binding_hash,
            "authentication_audit_id": self.authentication_audit_id,
            "nomos_decision_id": self.nomos_decision_id,
        }
        if self.created_at is not None:
            result["created_at"] = format_timestamp(self.created_at)
        return result


@dataclass(frozen=True, slots=True)
class ExecutionClaim:
    execution_id: str
    proposal_id: str
    decision_id: str
    worker_principal: str
    worker_separation_subject: str
    lifecycle_version: int
    approved_payload_version: int
    approved_payload_hash: str
    canonical_payload: bytes
    execution_epoch: int
    execution_epoch_lease_digest: str | None
    action_type: str
    target_ref: str
    action_schema_digest: str
    action_contract_digest: str
    executor_contract_digest: str
    payload_renderer_digest: str
    governed_tool_binding_digest: str
    provider_connection_contract_digest: str
    provider_capability_digest: str
    provider_operation_id: str
    provider_operation_descriptor_digest: str
    provider_idempotency_key: str
    policy_digest: str | None
    execute_before: datetime | str
    claimed_at: datetime | str
    decision_binding_hash: str
    execution_binding_hash: str
    claim_idempotency_key_hash: str
    handoff_receipt: Mapping[str, Any] | None = None
    authentication_audit_id: str | None = None
    nomos_decision_id: str | None = None
    request_hash: str | None = None
    handoff_protected_b64: str | None = None
    handoff_payload_b64: str | None = None
    handoff_signature_b64: str | None = None
    handoff_receipt_digest: str | None = None

    @property
    def payload(self) -> Any:
        import json

        return json.loads(self.canonical_payload)

    def to_projection(self, *, include_payload: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "proposal_id": self.proposal_id, "decision_id": self.decision_id, "execution_id": self.execution_id,
            "worker_principal": self.worker_principal, "worker_separation_subject": self.worker_separation_subject,
            "lifecycle_version": self.lifecycle_version, "execution_epoch": self.execution_epoch,
            "execution_epoch_lease_digest": self.execution_epoch_lease_digest, "action_type": self.action_type,
            "target_ref": self.target_ref, "action_schema_digest": self.action_schema_digest,
            "action_contract_digest": self.action_contract_digest, "executor_contract_digest": self.executor_contract_digest,
            "payload_renderer_digest": self.payload_renderer_digest, "governed_tool_binding_digest": self.governed_tool_binding_digest,
            "provider_connection_contract_digest": self.provider_connection_contract_digest,
            "provider_capability_digest": self.provider_capability_digest, "provider_operation_id": self.provider_operation_id,
            "provider_operation_descriptor_digest": self.provider_operation_descriptor_digest,
            "approved_payload_version": self.approved_payload_version, "approved_payload_hash": self.approved_payload_hash,
            "provider_idempotency_key": self.provider_idempotency_key, "policy_digest": self.policy_digest,
            "execute_before": format_timestamp(self.execute_before), "claimed_at": format_timestamp(self.claimed_at),
            "decision_binding_hash": self.decision_binding_hash, "execution_binding_hash": self.execution_binding_hash,
            "authentication_audit_id": self.authentication_audit_id,
            "nomos_decision_id": self.nomos_decision_id,
            "handoff_receipt_digest": self.handoff_receipt_digest,
        }
        if include_payload:
            result["canonical_payload"] = self.canonical_payload.decode("utf-8")
        if self.handoff_receipt is not None:
            result["okg_claim_handoff_receipt"] = _copy(self.handoff_receipt)
        for name in ("handoff_protected_b64", "handoff_payload_b64", "handoff_signature_b64"):
            value = getattr(self, name)
            if value is not None:
                result[name] = value
        return result


@dataclass(frozen=True, slots=True)
class Outcome:
    execution_id: str
    effect_instance_key: str
    worker_or_reconciler_separation_subject: str
    outcome: str
    outcome_reason_code: str
    provider_receipt_digest: str | None
    recovery_journal_digest: str
    owner_unavailability_attestation_digest: str | None
    outcome_request_hash: str
    recorded_at: datetime | str
    authentication_audit_id: str | None = None
    nomos_decision_id: str | None = None

    def to_projection(self) -> dict[str, Any]:
        return {
            "execution_id": self.execution_id, "effect_instance_key": self.effect_instance_key,
            "worker_or_reconciler_separation_subject": self.worker_or_reconciler_separation_subject,
            "outcome": self.outcome, "outcome_reason_code": self.outcome_reason_code,
            "provider_receipt_digest": self.provider_receipt_digest,
            "recovery_journal_digest": self.recovery_journal_digest,
            "owner_unavailability_attestation_digest": self.owner_unavailability_attestation_digest,
            "outcome_request_hash": self.outcome_request_hash, "recorded_at": format_timestamp(self.recorded_at),
            "authentication_audit_id": self.authentication_audit_id,
            "nomos_decision_id": self.nomos_decision_id,
        }


@dataclass(frozen=True, slots=True)
class AuditEvent:
    proposal_id: str | None
    execution_epoch: int
    effect_instance_key: str
    sequence: int
    lifecycle_version: int
    event_type: str
    actor_kind: str
    actor_principal: str | None
    actor_separation_subject: str | None
    authentication_audit_ref: str | None
    nomos_decision_ref: str | None
    deadline_kind: str | None
    policy_digest: str | None
    before_review_state: str | None
    after_review_state: str | None
    before_execution_state: str | None
    after_execution_state: str | None
    payload_version: int | None
    payload_hash: str | None
    decision_id: str | None
    decision_binding_hash: str | None
    execution_id: str | None
    execution_binding_hash: str | None
    transition_reason_code: str | None
    outcome: str | None
    outcome_reason_code: str | None
    outcome_request_hash: str | None
    restore_recovery_digest: str | None
    effective_at: datetime | str | None
    recorded_at: datetime | str
    previous_event_hash: str | None
    event_hash: str

    def to_projection(self) -> dict[str, Any]:
        result = {
            "proposal_id": self.proposal_id, "execution_epoch": self.execution_epoch,
            "effect_instance_key": self.effect_instance_key, "sequence": self.sequence,
            "lifecycle_version": self.lifecycle_version, "event_type": self.event_type,
            "actor_kind": self.actor_kind, "actor_principal": self.actor_principal,
            "actor_separation_subject": self.actor_separation_subject,
            "authentication_audit_ref": self.authentication_audit_ref, "nomos_decision_ref": self.nomos_decision_ref,
            "deadline_kind": self.deadline_kind, "policy_digest": self.policy_digest,
            "before_review_state": self.before_review_state, "after_review_state": self.after_review_state,
            "before_execution_state": self.before_execution_state, "after_execution_state": self.after_execution_state,
            "payload_version": self.payload_version, "payload_hash": self.payload_hash,
            "decision_id": self.decision_id, "decision_binding_hash": self.decision_binding_hash,
            "execution_id": self.execution_id, "execution_binding_hash": self.execution_binding_hash,
            "transition_reason_code": self.transition_reason_code, "outcome": self.outcome,
            "outcome_reason_code": self.outcome_reason_code, "outcome_request_hash": self.outcome_request_hash,
            "restore_recovery_digest": self.restore_recovery_digest,
            "effective_at": format_timestamp(self.effective_at) if self.effective_at is not None else None,
            "recorded_at": format_timestamp(self.recorded_at), "previous_event_hash": self.previous_event_hash,
        }
        result["event_hash"] = self.event_hash
        return result


@dataclass(frozen=True, slots=True)
class Proposal:
    proposal_id: str
    deployment: str
    execution_epoch: int
    effect_instance_key: str
    submission_content_hash: str
    proposal_request_hash: str
    proposer_principal: str
    proposer_separation_subject: str
    automation_id: str
    automation_version: str
    run_id: str
    correlation_id: str
    action_type: str
    target_ref: str
    risk_tier: str
    action_schema_digest: str
    action_contract_digest: str
    executor_contract_digest: str
    payload_renderer_digest: str
    governed_tool_binding_digest: str
    provider_connection_contract_digest: str
    provider_capability_digest: str
    provider_operation_id: str
    provider_operation_descriptor_digest: str
    original_payload_hash: str
    current_payload_hash: str
    payload_version: int
    lifecycle_version: int
    review_state: str
    execution_state: str
    requested_review_expires_at: datetime | str
    effective_review_expires_at: datetime | str
    summary_code: str
    evidence_digests: tuple[str, ...]
    current_decision_id: str | None = None
    current_decision_binding_hash: str | None = None
    execute_before: datetime | str | None = None
    created_at: datetime | str | None = None
    updated_at: datetime | str | None = None
    submission_idempotency_key_hash: str | None = None
    action_contract_record: Mapping[str, Any] | None = None
    policy_digest: str | None = None
    authentication_audit_id: str | None = None
    nomos_decision_id: str | None = None

    @property
    def etag(self) -> str:
        return f'"lifecycle:{self.lifecycle_version};payload-version:{self.payload_version};payload:{self.current_payload_hash}"'

    def to_projection(self, *, include_payload: bool = False, payload: bytes | None = None) -> dict[str, Any]:
        result: dict[str, Any] = {
            "proposal_id": self.proposal_id, "deployment": self.deployment, "review_state": self.review_state,
            "execution_state": self.execution_state, "proposer_principal": self.proposer_principal,
            "proposer_separation_subject": self.proposer_separation_subject, "lifecycle_version": self.lifecycle_version,
            "payload_version": self.payload_version, "execution_epoch": self.execution_epoch,
            "effect_instance_key": self.effect_instance_key, "submission_content_hash": self.submission_content_hash,
            "original_payload_hash": self.original_payload_hash, "current_payload_hash": self.current_payload_hash,
            "proposal_request_hash": self.proposal_request_hash, "action_type": self.action_type,
            "target_ref": self.target_ref, "risk_tier": self.risk_tier, "action_schema_digest": self.action_schema_digest,
            "action_contract_digest": self.action_contract_digest, "executor_contract_digest": self.executor_contract_digest,
            "payload_renderer_digest": self.payload_renderer_digest, "governed_tool_binding_digest": self.governed_tool_binding_digest,
            "provider_connection_contract_digest": self.provider_connection_contract_digest,
            "provider_capability_digest": self.provider_capability_digest, "provider_operation_id": self.provider_operation_id,
            "provider_operation_descriptor_digest": self.provider_operation_descriptor_digest,
            "requested_review_expires_at": format_timestamp(self.requested_review_expires_at),
            "effective_review_expires_at": format_timestamp(self.effective_review_expires_at), "summary_code": self.summary_code,
            "evidence_digests": list(self.evidence_digests), "decision_id": self.current_decision_id,
            "decision_binding_hash": self.current_decision_binding_hash,
            "execute_before": format_timestamp(self.execute_before) if self.execute_before is not None else None,
            "policy_digest": self.policy_digest,
            "authentication_audit_id": self.authentication_audit_id,
            "nomos_decision_id": self.nomos_decision_id,
            "etag": self.etag,
        }
        if self.created_at is not None:
            result["created_at"] = format_timestamp(self.created_at)
        if self.updated_at is not None:
            result["updated_at"] = format_timestamp(self.updated_at)
        if include_payload and payload is not None:
            result["canonical_payload"] = payload.decode("utf-8")
        return result


@dataclass(frozen=True, slots=True)
class ServiceResult:
    """A safe response snapshot, also convenient for HTTP adapters."""

    value: Any
    status: int = 200
    replay: bool = False
    code: str | None = None
    response_body: bytes = b""

    def __getattr__(self, name: str) -> Any:
        value = object.__getattribute__(self, "value")
        if hasattr(value, name):
            return getattr(value, name)
        if isinstance(value, Mapping) and name in value:
            return value[name]
        raise AttributeError(name)

    def to_dict(self) -> dict[str, Any]:
        if hasattr(self.value, "to_projection"):
            return self.value.to_projection()
        if isinstance(self.value, Mapping):
            return _copy(self.value)
        return {"value": self.value}


@dataclass(frozen=True, slots=True)
class AuthorizationRefs:
    """References that must be committed with an authenticated transition."""

    authentication_audit_id: str | None
    nomos_decision_id: str | None
    policy_digest: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        return {
            "authentication_audit_id": self.authentication_audit_id,
            "nomos_decision_id": self.nomos_decision_id,
            "policy_digest": self.policy_digest,
        }


@dataclass(frozen=True, slots=True)
class SubmitCommand:
    """Validated submit command sent to the fixed PostgreSQL RPC."""

    request: Mapping[str, Any]
    actor: Actor
    authorization: AuthorizationRefs

    def to_mapping(self) -> dict[str, Any]:
        return {
            **_command_fields(self.request),
            "proposer_principal": self.actor.principal,
            "proposer_separation_subject": self.actor.separation_subject,
            **self.authorization.to_mapping(),
        }


@dataclass(frozen=True, slots=True)
class ReviewTransitionCommand:
    """Validated edit/defer/decision/cancel command."""

    request: Mapping[str, Any]
    actor: Actor
    authorization: AuthorizationRefs

    def to_mapping(self) -> dict[str, Any]:
        return {
            **_command_fields(self.request),
            "actor_principal": self.actor.principal,
            "actor_separation_subject": self.actor.separation_subject,
            **self.authorization.to_mapping(),
        }


@dataclass(frozen=True, slots=True)
class ClaimCommand:
    """Validated first execution-claim command."""

    request: Mapping[str, Any]
    actor: Actor
    authorization: AuthorizationRefs

    def to_mapping(self) -> dict[str, Any]:
        return {
            **_command_fields(self.request),
            "worker_principal": self.actor.principal,
            "worker_separation_subject": self.actor.separation_subject,
            **self.authorization.to_mapping(),
        }


@dataclass(frozen=True, slots=True)
class OutcomeCommand:
    """Validated terminal outcome command."""

    request: Mapping[str, Any]
    actor: Actor
    authorization: AuthorizationRefs

    def to_mapping(self) -> dict[str, Any]:
        return {
            **_command_fields(self.request),
            "worker_or_reconciler_separation_subject": self.actor.separation_subject,
            **self.authorization.to_mapping(),
        }


@dataclass(frozen=True, slots=True)
class OwnerUnavailabilityCommand:
    """Validated signed owner-unavailability attestation command."""

    request: Mapping[str, Any]
    actor: Actor
    authorization: AuthorizationRefs

    def to_mapping(self) -> dict[str, Any]:
        authorization = self.authorization.to_mapping()
        authorization.pop("policy_digest", None)
        command = _command_fields(self.request)
        # The signed attestation identifies the unavailable claim owner and
        # the authenticated submitter separately.  The actor is the safe
        # default for narrow callers, but must not overwrite those verified
        # envelope fields when the service has already decomposed them.
        command.setdefault("claim_owner_separation_subject", self.actor.separation_subject)
        command.setdefault("authenticated_submitter_separation_subject", self.actor.separation_subject)
        return {**command, **authorization}


__all__ = [
    "Actor", "AuthorizationRefs", "SubmitCommand", "ReviewTransitionCommand", "ClaimCommand",
    "OutcomeCommand", "OwnerUnavailabilityCommand", "PayloadVersion", "Decision", "ExecutionClaim",
    "Outcome", "AuditEvent", "Proposal", "ServiceResult"
]
