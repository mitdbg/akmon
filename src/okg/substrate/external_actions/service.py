"""Deterministic external-action review state machine.

The service is intentionally provider-blind.  It creates a reviewed handoff
for a worker and records the worker's report; it never performs an effect.
All public mutations run under one store lock, matching the proposal/chain
transaction lock order used by the SQL implementation.
"""

from __future__ import annotations

import copy
import base64
import inspect
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

from okg.substrate.db.unit_of_work import committed_refusal

from .action_contracts import (
    ActionContract,
    ActionContractError,
    decision_binding_hash,
    digest_value,
    effect_instance_key,
    event_hash,
    execution_binding_hash,
    format_timestamp,
    idempotency_key_hash,
    outcome_request_hash,
    provider_idempotency_key,
    provider_idempotency_key_digest,
    submission_content_hash,
    transition_request_hash,
    validate_digest,
)
from .models import (
    Actor,
    AuditEvent,
    AuthorizationRefs,
    ClaimCommand,
    Decision,
    ExecutionClaim,
    Outcome,
    OutcomeCommand,
    OwnerUnavailabilityCommand,
    PayloadVersion,
    Proposal,
    ReviewTransitionCommand,
    ServiceResult,
    SubmitCommand,
)
from .store import ExternalActionStoreError, InMemoryExternalActionStore, TransitionRecord, new_ulid
from .signing import (
    SigningError,
    SigningKey,
    b64url_decode,
    domain_digest,
    parse_timestamp,
    strict_json_loads as strict_signed_json_loads,
    verify_detached_envelope,
)
from .restore_fence import (
    RestoreFenceError,
    resolve_restore_reference,
    verify_restore_envelope,
    verify_restore_proof,
)


_DIGEST_FIELDS = (
    "action_schema_digest", "action_contract_digest", "executor_contract_digest", "payload_renderer_digest",
    "governed_tool_binding_digest", "provider_connection_contract_digest", "provider_capability_digest",
    "provider_operation_descriptor_digest",
)
_EXECUTION_SUPPORT_FIELDS = {
    "supported_action_schema_digest": "action_schema_digest",
    "supported_action_contract_digest": "action_contract_digest",
    "supported_executor_contract_digest": "executor_contract_digest",
    "supported_payload_renderer_digest": "payload_renderer_digest",
    "supported_governed_tool_binding_digest": "governed_tool_binding_digest",
    "supported_provider_connection_contract_digest": "provider_connection_contract_digest",
    "supported_provider_capability_digest": "provider_capability_digest",
    "supported_provider_operation_id": "provider_operation_id",
    "supported_provider_operation_descriptor_digest": "provider_operation_descriptor_digest",
}
_CLAIM_HANDOFF_FIELDS = (
    "deployment",
    "execution_epoch",
    "execution_epoch_lease_digest",
    "effect_instance_key",
    "submission_content_hash",
    "proposal_id",
    "proposal_request_hash",
    "original_payload_hash",
    "proposer_separation_subject",
    "approved_lifecycle_version",
    "approved_payload_version",
    "approved_payload_hash",
    "action_contract_digest",
    "policy_digest",
    "decision_id",
    "decision_binding_hash",
    "decision_event_hash",
    "approver_separation_subject",
    "approver_authentication_audit_ref",
    "approver_nomos_decision_ref",
    "execution_id",
    "execution_binding_hash",
    "claim_event_hash",
    "worker_separation_subject",
    "worker_authentication_audit_ref",
    "worker_nomos_decision_ref",
    "event_chain_head_hash",
    "claimed_at",
    "execute_before",
)
_CLAIM_PREPARATION_FIELDS = {
    "claimed_at",
    "previous_event_hash",
    "decision_event_hash",
    "claim_event_hash",
    "event_chain_head_hash",
    "claim_lifecycle_version",
    "event_sequence",
}
_RESTORE_COMPLETION_FIELDS = (
    "deployment", "restore_id", "restore_attestation_envelope_digest",
    "pre_restore_event_root_digest", "pre_restore_event_head_count",
    "restored_event_root_digest", "restored_event_head_count",
    "recovery_set_digest", "recovery_count", "completed_at",
)


class DurableNomosAuthorizer:
    """Concrete same-connection adapter for the production Nomos boundary.

    The HTTP adapter needs a callable ``(operation, actor, ...)`` authority,
    while the canonical Nomos runtime entry point takes a typed request and a
    PostgreSQL connection.  This bridge owns that translation.  It never
    opens a connection or evaluates a local policy; the store-bound adapter
    performs both the active-policy read and decision persistence on the
    caller's transaction.  Missing policy SQL therefore remains a hard
    authority failure.
    """

    def __init__(self, store: Any, *, deployment: str | None = None, scope: str = "external_action") -> None:
        self.store = store
        self.deployment = deployment or getattr(store, "deployment", None)
        self.scope = scope

    def _connection(self, connection: Any = None) -> Any:
        expected = getattr(self.store, "connection", None)
        if expected is None:
            raise ExternalActionError(
                "review_authority_unavailable",
                "same-connection Nomos storage is unavailable",
                status=503,
            )
        if connection is not None and connection is not expected:
            raise ExternalActionError(
                "review_authority_unavailable",
                "Nomos authority connection mismatch",
                status=503,
            )
        return expected

    def evaluate_runtime_boundary_with_active_policy(
        self,
        connection: Any,
        *,
        deployment_name: str,
        scope: str,
        request: Any,
        target: Mapping[str, Any] | None = None,
        require_active: bool = True,
        enforcement_mode: str = "enforce",
        storage_adapter: Any = None,
        **kwargs: Any,
    ) -> Any:
        """Delegate to the canonical evaluator from ``nomos.store``."""

        del kwargs
        connection = self._connection(connection)
        if self.deployment is not None and deployment_name != self.deployment:
            raise ExternalActionError(
                "review_authority_unavailable",
                "Nomos deployment does not match the durable store",
                status=503,
            )
        if scope != self.scope:
            raise ExternalActionError(
                "review_authority_unavailable",
                "Nomos scope is not bound to external actions",
                status=503,
            )
        if storage_adapter is None:
            storage_adapter = getattr(self.store, "nomos_storage_adapter", None)
        if storage_adapter is None:
            raise ExternalActionError(
                "review_authority_unavailable",
                "same-connection Nomos storage is unavailable",
                status=503,
            )
        # This import is intentionally from nomos.store: that module owns the
        # active-policy + decision-persistence transaction boundary.  The
        # request dataclass itself is defined in runtime_enforcement.
        from okg.substrate.nomos.store import evaluate_runtime_boundary_with_active_policy

        return evaluate_runtime_boundary_with_active_policy(
            connection,
            deployment_name=deployment_name,
            scope=scope,
            request=request,
            target=target,
            require_active=require_active,
            enforcement_mode=enforcement_mode,
            storage_adapter=storage_adapter,
        )

    def __call__(
        self,
        operation: str,
        actor: Actor,
        *,
        transaction: Any = None,
        storage_adapter: Any = None,
        context: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Adapt the HTTP authorization callback to the canonical runtime API."""

        from okg.substrate.nomos.runtime_enforcement import (
            NomosRuntimeRequest,
            surface_for_action,
        )

        context_value = dict(context or {})
        deployment = context_value.get("deployment", self.deployment)
        if not isinstance(deployment, str) or not deployment:
            raise ExternalActionError(
                "review_authority_unavailable",
                "Nomos deployment is unavailable",
                status=503,
            )
        if not actor.nomos_provider_id or not actor.nomos_subject_binding_id:
            raise ExternalActionError(
                "review_authority_unavailable",
                "Nomos identity binding unavailable",
                status=503,
            )
        bind_identity = getattr(self.store, "bind_nomos_identity", None)
        if not callable(bind_identity):
            raise ExternalActionError(
                "review_authority_unavailable",
                "Nomos identity binding unavailable",
                status=503,
            )
        bind_identity(
            provider_id=actor.nomos_provider_id,
            subject_binding_id=actor.nomos_subject_binding_id,
        )
        request = NomosRuntimeRequest(
            surface=surface_for_action(operation, fallback_surface=operation),
            subject=actor.separation_subject,
            resource={"operation": operation},
            classification=context_value.get("classification", "internal"),
            context=context_value,
        )
        outcome = self.evaluate_runtime_boundary_with_active_policy(
            self._connection(transaction),
            deployment_name=deployment,
            scope=self.scope,
            request=request,
            target={"kind": "external_action", "operation": operation},
            require_active=True,
            enforcement_mode="enforce",
            storage_adapter=storage_adapter,
        )
        decision = getattr(outcome, "decision", None)
        return {
            "allowed": getattr(outcome, "allowed", False) is True,
            "effect": getattr(outcome, "final_effect", None),
            "obligations_performed": not bool(getattr(outcome, "missing_obligations", ())),
            "decision_id": getattr(decision, "decision_id", None),
            "policy_digest": getattr(decision, "policy_fingerprint", None),
        }


class ExternalActionError(ValueError):
    """Stable, safe error returned by a lifecycle operation."""

    _STATUS = {
        "authentication_required": 401, "sender_constraint_required": 401, "proposal_not_found": 404,
        "invalid_proposal": 422, "review_request_too_large": 413, "proposal_precondition_required": 428,
        "idempotency_conflict": 409, "stale_proposal_lifecycle": 409, "stale_payload_binding": 409,
        "illegal_proposal_transition": 409, "proposer_approver_separation_required": 403,
        "execution_already_claimed": 409, "outcome_already_recorded": 409, "proposal_expired": 410,
        "review_authority_unavailable": 503, "execution_epoch_authority_unavailable": 503,
        "external_action_restore_quarantined": 409, "attestation_replay_conflict": 409,
        "incomplete_restore_coverage": 409, "restore_reconciliation_conflict": 409,
    }

    def __init__(self, code: str, message: str | None = None, *, status: int | None = None, detail: Mapping[str, Any] | None = None) -> None:
        self.code = code
        self.status = status if status is not None else self._STATUS.get(code, 409)
        # Never interpolate payloads or credential-like values into a refusal.
        self.detail = {"code": code, **(dict(detail) if detail else {})}
        super().__init__(message or code)


LifecycleError = ExternalActionError


def _utc(value: datetime | str) -> datetime:
    if isinstance(value, str):
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z", value):
            raise ExternalActionError("invalid_proposal", "timestamp must use six fractional digits and UTC Z")
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ExternalActionError("invalid_proposal", "timestamp is invalid") from exc
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ExternalActionError("invalid_proposal", "timestamp must include a timezone")
    return value.astimezone(timezone.utc).replace(microsecond=value.astimezone(timezone.utc).microsecond)


def _result(value: Any, *, status: int = 200, replay: bool = False) -> ServiceResult:
    if hasattr(value, "to_projection"):
        body = value.to_projection()
    elif isinstance(value, Mapping):
        body = copy.deepcopy(dict(value))
    else:
        body = {"value": value}
    from .action_contracts import canonical_json_bytes

    return ServiceResult(value=value, status=status, replay=replay, response_body=canonical_json_bytes(body))


def _safe_response_status(value: Any, *, default: int) -> int:
    """Accept only an HTTP status from a typed RPC response.

    Restore RPCs also return a domain ``status`` string such as
    ``collecting_checkpoints`` or ``complete``.  That field is not an HTTP
    status and must never be coerced with ``int()``.
    """

    if isinstance(value, int) and not isinstance(value, bool) and 100 <= value <= 599:
        return value
    return default


def _committed_claim_expiry() -> ExternalActionError:
    """Return the late-claim refusal whose expiry record must commit.

    The durable store lock commits the service transaction when the escaping
    error is a ``committed_refusal`` (``okg.substrate.db.unit_of_work``), so
    the ``approval_expired`` event written by the expiry transition survives
    the 410 instead of rolling back with it (issue #1925).
    """

    return committed_refusal(
        ExternalActionError("proposal_expired", "execution deadline reached", status=410)
    )


class ExternalActionService:
    """Review/claim/outcome operations backed by an atomic store."""

    def __init__(
        self,
        store: Any = None,
        *,
        authorize: Any = None,
        nomos_evaluator: Any = None,
        handoff_signer: Callable[[Mapping[str, Any]], Mapping[str, str]] | None = None,
        handoff_verification_key: SigningKey | None = None,
        owner_attestation_keys: Mapping[str, SigningKey] | None = None,
        owner_attestation_maximum_lifetime_seconds: int | None = None,
        claim_owner_status_verifier: Callable[..., Mapping[str, Any]] | None = None,
        action_contracts: Mapping[str, ActionContract | Mapping[str, Any]] | None = None,
        restore_envelope_resolver: Callable[..., Any] | Mapping[str, Any] | None = None,
        restore_fence_config: Any = None,
        id_factory: Callable[[], str] = new_ulid,
        policy_digest: str | None = None,
    ) -> None:
        self.store = store or InMemoryExternalActionStore(policy_digest=policy_digest)
        # A durable store always gets a concrete Nomos bridge.  The bridge is
        # deliberately strict: if active-policy SQL is unavailable, the first
        # request receives a 503 rather than falling back to a local allow.
        if getattr(self.store, "is_durable", False) is True and authorize is None and nomos_evaluator is None:
            authorize = DurableNomosAuthorizer(self.store)
        self.authorize = authorize
        self.nomos_evaluator = nomos_evaluator
        # The OKG handoff is a server-owned signed artifact.  Keep the key
        # boundary injected: this package must not invent or accept private
        # key material from a worker request.  Durable claims fail closed when
        # the deployment has not supplied this boundary.
        self.handoff_signer = handoff_signer
        if handoff_verification_key is not None and (
            not isinstance(handoff_verification_key, SigningKey)
            or handoff_verification_key.purpose != "claim_and_restore_completion"
            or handoff_verification_key.state != "current"
        ):
            raise ValueError("claim handoff verification key must be the current deployment key")
        self.handoff_verification_key = handoff_verification_key
        self.owner_attestation_keys = dict(owner_attestation_keys or {})
        if owner_attestation_maximum_lifetime_seconds is not None and (
            type(owner_attestation_maximum_lifetime_seconds) is not int
            or not 1 <= owner_attestation_maximum_lifetime_seconds <= 86_400
        ):
            raise ValueError("owner attestation maximum lifetime must be in 1..86400 seconds")
        self.owner_attestation_maximum_lifetime_seconds = owner_attestation_maximum_lifetime_seconds
        self.claim_owner_status_verifier = claim_owner_status_verifier
        self.action_contracts = dict(action_contracts or {})
        # Restore evidence is held by a deployment-owned authority.  The
        # service accepts an explicit resolver for production and a mapping
        # only for deterministic tests; it never interprets request refs as
        # filesystem paths or URLs.
        self.restore_envelope_resolver = restore_envelope_resolver
        self.restore_fence_config = restore_fence_config or getattr(self.store, "restore_fence_config", None)
        self.id_factory = id_factory
        self.policy_digest = policy_digest if policy_digest is not None else self.store.policy_digest
        self._claim_idempotency: dict[tuple[str, str, str], tuple[str, str]] = {}
        self._outcome_idempotency: dict[tuple[str, str, str], tuple[str, str]] = {}

    # ----- common helpers -------------------------------------------------

    def _actor(self, actor: Actor | Mapping[str, Any] | str, *, subject: str | None = None) -> Actor:
        try:
            result = Actor.coerce(actor, subject=subject)
        except (TypeError, ValueError) as exc:
            raise ExternalActionError("authentication_required", "authenticated principal required", status=401) from exc
        if not re.fullmatch(r"[A-Za-z0-9._:+\-]{1,256}", result.principal) or not re.fullmatch(r"[A-Za-z0-9._:+\-]{1,256}", result.separation_subject):
            raise ExternalActionError("authentication_required", "invalid authenticated principal", status=401)
        return result

    def _is_durable(self) -> bool:
        return bool(getattr(self.store, "is_durable", False))

    def _persisted_service_mode(self, deployment: str | None = None) -> str:
        """Read the restore fence mode from the lock-local durable snapshot."""

        if not self._is_durable():
            return "normal"
        value = None
        getter = getattr(self.store, "restore_fence", None)
        if callable(getter):
            try:
                value = getter(deployment=deployment or getattr(self.store, "deployment", None))
            except TypeError:
                value = getter(deployment or getattr(self.store, "deployment", None))
        if value is None:
            fences = getattr(self.store, "restore_fences", None)
            if fences is not None and callable(getattr(fences, "get", None)):
                value = fences.get(deployment or getattr(self.store, "deployment", ""))
        mode = value.get("service_mode") if isinstance(value, Mapping) else None
        if mode in {"normal", "restore_only"}:
            return mode
        # A durable store without a persisted fence is not safe to mutate.
        raise ExternalActionError(
            "review_authority_unavailable",
            "persisted restore-fence mode is unavailable",
            status=503,
        )

    @staticmethod
    def _decision_field(value: Any, *names: str) -> Any:
        if isinstance(value, Mapping):
            for name in names:
                if value.get(name) is not None:
                    return value[name]
            return None
        for name in names:
            candidate = getattr(value, name, None)
            if candidate is not None:
                return candidate
        return None

    @staticmethod
    def _auth_evidence(actor: Actor) -> Mapping[str, Any]:
        """Return the verifier-owned identity evidence carried by an actor.

        A durable lifecycle actor cannot be reconstructed from the four
        stable identity strings alone.  The HTTP boundary puts the redacted
        ``AuthAudit`` projection on ``Actor.authentication_audit``; direct
        service callers must provide the same mapping.  This check is kept
        here as a second boundary so a custom transport cannot accidentally
        bypass the credential issuance and re-enable fences.
        """

        value = actor.authentication_audit
        if hasattr(value, "as_dict"):
            value = value.as_dict()
        if not isinstance(value, Mapping):
            raise ExternalActionError(
                "review_authority_unavailable",
                "verified authentication evidence is unavailable",
                status=503,
            )
        required = {
            "nomos_provider_id", "nomos_subject_binding_id", "authentication_method_code",
            "sender_key_or_certificate_thumbprint", "issuer_or_trust_domain_digest",
            "provider_status", "binding_status", "provider_updated_at", "binding_updated_at",
            "provider_credential_generation_digest", "binding_credential_generation_digest",
        }
        if any(value.get(key) is None for key in required):
            raise ExternalActionError(
                "review_authority_unavailable",
                "verified authentication evidence is incomplete",
                status=503,
            )
        if value.get("nomos_provider_id") != actor.nomos_provider_id or value.get("nomos_subject_binding_id") != actor.nomos_subject_binding_id:
            raise ExternalActionError(
                "review_authority_unavailable",
                "verified authentication evidence is not actor-bound",
                status=503,
            )
        if value.get("provider_status") != "active" or value.get("binding_status") != "active":
            raise ExternalActionError(
                "authentication_required",
                "authenticated identity is not active",
                status=401,
            )
        try:
            _utc(value["provider_updated_at"])
            _utc(value["binding_updated_at"])
        except (ExternalActionError, TypeError, ValueError) as exc:
            raise ExternalActionError(
                "review_authority_unavailable",
                "verified authentication barrier is invalid",
                status=503,
            ) from exc
        method = value.get("authentication_method_code")
        if method in {"jwt", "jwt_dpop"}:
            if value.get("token_issued_at") is None:
                raise ExternalActionError(
                    "review_authority_unavailable",
                    "JWT issuance evidence is unavailable",
                    status=503,
                )
            try:
                _utc(value["token_issued_at"])
            except (ExternalActionError, TypeError, ValueError) as exc:
                raise ExternalActionError(
                    "review_authority_unavailable",
                    "JWT issuance evidence is invalid",
                    status=503,
                ) from exc
        elif method == "mtls":
            if value.get("certificate_not_before") is None or value.get("certificate_thumbprint") is None:
                raise ExternalActionError(
                    "review_authority_unavailable",
                    "client-certificate issuance evidence is unavailable",
                    status=503,
                )
            try:
                _utc(value["certificate_not_before"])
            except (ExternalActionError, TypeError, ValueError) as exc:
                raise ExternalActionError(
                    "review_authority_unavailable",
                    "client-certificate issuance evidence is invalid",
                    status=503,
                ) from exc
        else:
            raise ExternalActionError(
                "review_authority_unavailable",
                "unsupported authenticated identity method",
                status=503,
            )
        # A generation is the credential-version fence used by the provider
        # and subject-binding records.  At least one current generation and
        # the presented certificate generation (when mTLS is used) must be
        # retained; a thumbprint by itself cannot prevent old credentials
        # from reviving after disable -> reenable.
        generations = (
            value.get("provider_credential_generation_digest"),
            value.get("binding_credential_generation_digest"),
        )
        if any(not isinstance(generation, str) or not generation for generation in generations):
            raise ExternalActionError(
                "review_authority_unavailable",
                "credential generation evidence is unavailable",
                status=503,
            )
        if method == "mtls" and not isinstance(value.get("presented_credential_generation_digest"), str):
            raise ExternalActionError(
                "review_authority_unavailable",
                "presented credential generation evidence is unavailable",
                status=503,
            )
        return value

    def _revalidate_durable_evidence(
        self,
        actor: Actor,
        *,
        operation: str,
        deployment: str,
        authentication_audit_id: str,
        nomos_decision_id: str,
        transition_digest: str,
    ) -> None:
        """Recheck identity barriers immediately before a semantic RPC.

        The fixed SQL authority snapshot acquires the deployment lock and
        resolves the current provider and subject-binding rows.  Python
        validates the closed projection returned by that call against the
        authenticated Actor, while SQL owns the final row-level race check.
        Missing or incomplete output is an unavailable authority rather than
        an implicit allow.
        """

        evidence = self._auth_evidence(actor)
        verifier = getattr(self.store, "verify_nomos_authority", None)
        if not callable(verifier):
            raise ExternalActionError(
                "review_authority_unavailable",
                "durable identity barrier authority is unavailable",
                status=503,
            )
        try:
            snapshot = verifier(
                deployment=deployment,
                authentication_audit_id=authentication_audit_id,
                nomos_decision_id=nomos_decision_id,
                expected_action=operation,
                expected_transition_digest=transition_digest,
            )
        except Exception as exc:
            raise ExternalActionError(
                "review_authority_unavailable",
                "durable identity barrier authority is unavailable",
                status=503,
            ) from exc
        if not isinstance(snapshot, Mapping):
            raise ExternalActionError(
                "review_authority_unavailable",
                "durable identity barrier projection is unavailable",
                status=503,
            )
        required = {
            "deployment", "authentication_audit_id", "nomos_decision_id",
            "nomos_provider_id", "nomos_subject_binding_id", "separation_subject",
            "policy_digest", "action", "effect", "provider_status", "binding_status",
            "provider_updated_at", "binding_updated_at",
            "provider_credential_generation_digest", "binding_credential_generation_digest",
        }
        if any(snapshot.get(key) is None for key in required):
            raise ExternalActionError(
                "review_authority_unavailable",
                "durable identity barrier projection is incomplete",
                status=503,
            )
        if (
            snapshot["deployment"] != deployment
            or snapshot["authentication_audit_id"] != authentication_audit_id
            or snapshot["nomos_decision_id"] != nomos_decision_id
            or snapshot["nomos_provider_id"] != actor.nomos_provider_id
            or snapshot["nomos_subject_binding_id"] != actor.nomos_subject_binding_id
            or snapshot["separation_subject"] != actor.separation_subject
            or snapshot["action"] != operation
            or snapshot["effect"] != "allow"
            or (
                isinstance(self.policy_digest, str)
                and snapshot["policy_digest"] != self.policy_digest
            )
            or snapshot["provider_status"] != "active"
            or snapshot["binding_status"] != "active"
            or snapshot["provider_credential_generation_digest"]
            != evidence["provider_credential_generation_digest"]
            or snapshot["binding_credential_generation_digest"]
            != evidence["binding_credential_generation_digest"]
        ):
            raise ExternalActionError(
                "review_authority_unavailable",
                "durable identity barrier is stale",
                status=503,
            )
        try:
            if _utc(snapshot["provider_updated_at"]) != _utc(evidence["provider_updated_at"]):
                raise ValueError("provider barrier changed")
            if _utc(snapshot["binding_updated_at"]) != _utc(evidence["binding_updated_at"]):
                raise ValueError("binding barrier changed")
        except (ExternalActionError, TypeError, ValueError) as exc:
            raise ExternalActionError(
                "review_authority_unavailable",
                "durable identity barrier is stale",
                status=503,
            ) from exc

    def _authorize(self, operation: str, actor: Actor, *, context: Mapping[str, Any] | None = None) -> Actor:
        """Authorize, recording both evidence references on durable calls.

        The durable path intentionally does not interpret a bare boolean as a
        permission.  The callback must either be a strict Nomos evaluator or
        return its decision and durable references; this makes a missing
        runtime policy an unavailable authority rather than an allow-all.
        """

        if not self._is_durable():
            if self.authorize is None:
                return actor
            try:
                allowed = self.authorize(operation, actor)
            except Exception as exc:
                raise ExternalActionError("review_authority_unavailable", "review authority unavailable", status=503) from exc
            if allowed is not True:
                raise ExternalActionError("unauthorized", "operation is not authorized", status=403)
            return actor
        if self.authorize is None and self.nomos_evaluator is None:
            raise ExternalActionError("review_authority_unavailable", "review authority unavailable", status=503)

        transaction = getattr(self.store, "connection", None)
        adapter = getattr(self.store, "nomos_storage_adapter", None)
        if transaction is None or adapter is None:
            raise ExternalActionError(
                "review_authority_unavailable",
                "same-connection Nomos storage is unavailable",
                status=503,
            )
        context_value = dict(context or {})
        # Require the complete verifier projection before appending an audit
        # row or evaluating policy.  Stable identity strings are not enough
        # to cross a durable lifecycle boundary.
        authentication_evidence = self._auth_evidence(actor)
        # Provider and subject-binding ids are verified-authentication output,
        # never request-body fields. Bind them before the policy RPC so its
        # locked bundle/identity barrier is for this exact principal.
        bind_identity = getattr(self.store, "bind_nomos_identity", None)
        if callable(bind_identity) and (not actor.nomos_provider_id or not actor.nomos_subject_binding_id):
            raise ExternalActionError("review_authority_unavailable", "Nomos identity binding unavailable", status=503)
        if actor.nomos_provider_id or actor.nomos_subject_binding_id:
            if not callable(bind_identity) or not actor.nomos_provider_id or not actor.nomos_subject_binding_id:
                raise ExternalActionError("review_authority_unavailable", "Nomos identity binding unavailable", status=503)
            try:
                bind_identity(
                    provider_id=actor.nomos_provider_id,
                    subject_binding_id=actor.nomos_subject_binding_id,
                )
            except Exception as exc:
                raise ExternalActionError("review_authority_unavailable", "Nomos identity binding unavailable", status=503) from exc
        deployment = context_value.get(
            "deployment", getattr(self.store, "deployment", None)
        )
        transition_digest = context_value.get("request_hash")
        begin_verifier = getattr(
            self.store, "begin_verifier_authorization", None
        )
        if not callable(begin_verifier):
            raise ExternalActionError(
                "review_authority_unavailable",
                "verifier assertion authority unavailable",
                status=503,
            )
        try:
            begin_verifier(
                deployment=deployment,
                operation=operation,
                transition_digest=transition_digest,
                actor=actor,
                authentication_evidence=authentication_evidence,
            )
        except Exception as exc:
            raise ExternalActionError(
                "review_authority_unavailable",
                "verifier assertion authority unavailable",
                status=503,
            ) from exc
        # Do not insert authentication evidence yet. Strict Nomos evaluation
        # must persist its decision first, then the audit row references it.
        bound_actor = actor

        evaluator = self.nomos_evaluator
        if evaluator is None and hasattr(self.authorize, "evaluate_runtime_boundary_with_active_policy"):
            evaluator = self.authorize
        try:
            if evaluator is not None:
                canonical_evaluator = getattr(
                    evaluator, "evaluate_runtime_boundary_with_active_policy", None
                )
                if (
                    getattr(evaluator, "__name__", "")
                    == "evaluate_runtime_boundary_with_active_policy"
                    or getattr(canonical_evaluator, "__name__", "")
                    == "evaluate_runtime_boundary_with_active_policy"
                ):
                    # The repository's canonical evaluator takes a typed
                    # NomosRuntimeRequest and a connection as positional
                    # inputs.  Keep this call explicit so it cannot silently
                    # drop the active-policy or same-transaction guarantees.
                    from okg.substrate.nomos.runtime_enforcement import (
                        NomosRuntimeRequest,
                        surface_for_action,
                    )

                    outcome = (
                        canonical_evaluator
                        if callable(canonical_evaluator)
                        else evaluator
                    )(
                        transaction,
                        deployment_name=context_value.get("deployment", getattr(self.store, "deployment", "")),
                        scope="external_action",
                        request=NomosRuntimeRequest(
                            surface=surface_for_action(operation, fallback_surface=operation),
                            subject=bound_actor.separation_subject,
                            resource=context_value.get("run_authorization_target", {"operation": operation}),
                            classification=context_value.get("classification", "internal"),
                            context=context_value,
                        ),
                        target=context_value.get("run_authorization_target", {"kind": "external_action", "operation": operation}),
                        require_active=True,
                        enforcement_mode="enforce",
                        storage_adapter=adapter,
                    )
                    decision = outcome
                else:
                    from .authorization import authorize as authorize_with_nomos

                    decision = authorize_with_nomos(
                        evaluator,
                        operation=operation,
                        principal=bound_actor,
                        deployment=context_value.get("deployment", getattr(self.store, "deployment", "")),
                        transaction=transaction,
                        storage_adapter=adapter,
                        context=context_value,
                    )
            else:
                callback = self.authorize
                try:
                    decision = callback(
                        operation,
                        bound_actor,
                        transaction=transaction,
                        storage_adapter=adapter,
                        context=context_value,
                    )
                except TypeError:
                    # Existing deterministic test callbacks retain their
                    # narrow two-argument shape.  Durable callbacks still
                    # must return a decision carrying a Nomos reference.
                    decision = callback(operation, bound_actor)
        except ExternalActionError:
            raise
        except ExternalActionStoreError as exc:
            if exc.code == "nomos_policy_denied":
                raise ExternalActionError("unauthorized", "operation is not authorized", status=403) from exc
            raise ExternalActionError("review_authority_unavailable", "review authority unavailable", status=503) from exc
        except Exception as exc:
            raise ExternalActionError("review_authority_unavailable", "review authority unavailable", status=503) from exc
        if hasattr(decision, "decision") and hasattr(decision, "allowed"):
            # NomosRuntimeOutcome keeps the effect and obligation result on
            # the outcome and the durable decision id/fingerprint on the
            # nested PolicyDecision.
            nested = decision.decision
            decision = {
                "allowed": decision.allowed,
                "effect": getattr(decision, "final_effect", None),
                "obligations_performed": not bool(getattr(decision, "missing_obligations", ())),
                "decision_id": getattr(nested, "decision_id", None),
                "policy_digest": getattr(nested, "policy_fingerprint", None),
            }
        decision_id = self._decision_field(decision, "nomos_decision_id", "decision_id", "id")
        if not isinstance(decision_id, str) or not decision_id:
            raise ExternalActionError("review_authority_unavailable", "Nomos decision unavailable", status=503)
        policy_digest = self._decision_field(
            decision, "policy_digest", "policy_fingerprint"
        )
        if self._is_durable() and (
            not isinstance(policy_digest, str) or not policy_digest
        ):
            # A durable semantic row must identify the active policy bundle
            # that authorized it.  A callback that only says "allow" is not a
            # production Nomos decision and must not reach a transition RPC.
            raise ExternalActionError(
                "review_authority_unavailable",
                "Nomos policy digest unavailable",
                status=503,
            )
        if isinstance(policy_digest, str):
            self.policy_digest = policy_digest
        # Authentication evidence is appended only after strict evaluation
        # has persisted its Nomos decision through this same transaction.
        # An already-bound audit id is accepted only as an explicit upstream
        # integration; semantic SQL transitions revalidate its Nomos ref.
        audit_id = bound_actor.authentication_audit_id
        if audit_id and bound_actor.nomos_decision_id not in {None, decision_id}:
            raise ExternalActionError("review_authority_unavailable", "authentication audit is bound to a different Nomos decision", status=503)
        if audit_id and bound_actor.nomos_decision_id is None:
            # A pre-decision audit row cannot be safely updated through the
            # fixed append-only RPC. The HTTP adapter must defer audit insert
            # until this point instead of handing us only its row id.
            raise ExternalActionError("review_authority_unavailable", "authentication audit lacks its Nomos decision binding", status=503)
        if not audit_id:
            raw_audit = bound_actor.authentication_audit
            if hasattr(raw_audit, "as_dict"):
                raw_audit = raw_audit.as_dict()
            if raw_audit is not None and not isinstance(raw_audit, Mapping):
                raise ExternalActionError("review_authority_unavailable", "authentication audit unavailable", status=503)
            audit_payload = dict(raw_audit or {})
            # HTTP authentication is intentionally deferred until after the
            # Nomos decision.  Lifecycle callers do not all carry the HTTP
            # request id in their operation context, so retain the id from
            # that deferred audit unless a lifecycle context supplies a
            # non-null replacement.
            request_id = context_value.get("request_id")
            if request_id is None:
                request_id = audit_payload.get("request_id")
            audit_payload.update({
                "deployment": context_value.get("deployment", getattr(self.store, "deployment", None)),
                "principal": bound_actor.principal,
                "separation_subject": bound_actor.separation_subject,
                "request_id": request_id,
                "nomos_decision_id": decision_id,
            })
            try:
                audit = self.store.record_auth_audit(audit_payload)
            except Exception as exc:
                raise ExternalActionError("review_authority_unavailable", "review authority unavailable", status=503) from exc
            audit_id = (
                self._decision_field(audit, "authentication_audit_id", "auth_audit_id", "id")
                or (audit if isinstance(audit, str) else None)
            )
        if not isinstance(audit_id, str) or not audit_id:
            raise ExternalActionError("review_authority_unavailable", "authentication audit unavailable", status=503)
        allowed = self._decision_field(decision, "allowed")
        effect = self._decision_field(decision, "effect", "decision_effect")
        obligations = self._decision_field(decision, "obligations_performed")
        if allowed is not True or (effect is not None and effect != "allow") or (obligations is not None and obligations is not True):
            if allowed is False:
                raise ExternalActionError("unauthorized", "operation is not authorized", status=403)
            raise ExternalActionError("review_authority_unavailable", "review authority unavailable", status=503)
        if not isinstance(deployment, str) or not deployment:
            raise ExternalActionError("review_authority_unavailable", "review deployment is unavailable", status=503)
        self._revalidate_durable_evidence(
            bound_actor,
            operation=operation,
            deployment=deployment,
            authentication_audit_id=audit_id,
            nomos_decision_id=decision_id,
            transition_digest=transition_digest,
        )
        return bound_actor.with_authorization(
            authentication_audit_id=audit_id,
            nomos_decision_id=decision_id,
        )

    def _authorization(self, actor: Actor) -> AuthorizationRefs:
        refs = AuthorizationRefs(
            actor.authentication_audit_id,
            actor.nomos_decision_id,
            self.policy_digest,
        )
        if self._is_durable() and (
            not refs.authentication_audit_id
            or not refs.nomos_decision_id
            or not refs.policy_digest
        ):
            raise ExternalActionError("review_authority_unavailable", "review authority unavailable", status=503)
        return refs

    def _refresh_durable(self) -> None:
        refresh = getattr(self.store, "_refresh_snapshot", None)
        if callable(refresh):
            refresh()

    def _durable_result(
        self,
        raw: Any,
        *,
        proposal_id: str | None = None,
        status: int = 200,
        replay: bool = False,
    ) -> ServiceResult:
        """Turn a typed RPC response into the normal projection result."""

        self._refresh_durable()
        if isinstance(raw, ServiceResult):
            return replace(raw, replay=raw.replay or replay)
        raw_mapping = raw if isinstance(raw, Mapping) else {}
        returned_id = self._decision_field(raw, "proposal_id", "id")
        # SQL allocates a candidate id before checking the durable submit
        # idempotency key.  On replay it returns the original proposal id, so
        # prefer the RPC's authoritative id over the newly generated one.
        candidate = (returned_id if isinstance(returned_id, str) else proposal_id)
        nested = raw_mapping.get("proposal") if isinstance(raw_mapping, Mapping) else None
        if isinstance(nested, Mapping):
            candidate = candidate or nested.get("proposal_id")
        if candidate:
            proposal = self.store.get(candidate)
            if isinstance(proposal, Proposal):
                response_replay = raw_mapping.get("replay", raw_mapping.get("idempotent_replay", replay))
                response_status = raw_mapping.get("status", raw_mapping.get("response_status", 200 if response_replay else status))
                return self._projection_result(proposal, status=_safe_response_status(response_status, default=200 if response_replay else status), replay=bool(response_replay))
        response_replay = raw_mapping.get("replay", raw_mapping.get("idempotent_replay", replay)) if raw_mapping else replay
        response_status = (
            raw_mapping.get("status", raw_mapping.get("response_status", 200 if response_replay else status))
            if raw_mapping else status
        )
        value = nested if isinstance(nested, Mapping) else raw
        return _result(value, status=_safe_response_status(response_status, default=200 if response_replay else status), replay=bool(response_replay))

    def _durable_record_result(self, raw: Any, record: Any, *, status: int = 201) -> ServiceResult:
        self._refresh_durable()
        if isinstance(raw, ServiceResult):
            return replace(raw, replay=raw.replay)
        raw_mapping = raw if isinstance(raw, Mapping) else {}
        response_replay = raw_mapping.get("replay", raw_mapping.get("idempotent_replay", False))
        response_status = raw_mapping.get(
            "status", raw_mapping.get("response_status", 200 if response_replay else status)
        )
        # SQL retains the exact safe response bytes in its idempotency row.
        # Reconstructing from the refreshed projection would turn a historical
        # replay into the proposal's current state.  Preserve those bytes
        # verbatim when the RPC returns them.
        response_body = raw_mapping.get("response_body_jcs", raw_mapping.get("response_body"))
        if isinstance(response_body, Mapping) and set(response_body) == {"__okg_bytes_b64"}:
            import base64

            try:
                response_body = base64.b64decode(response_body["__okg_bytes_b64"], validate=True)
            except (TypeError, ValueError) as exc:
                raise ExternalActionError("review_authority_unavailable", "durable response is unavailable", status=503) from exc
        if isinstance(response_body, str):
            response_body = response_body.encode("utf-8")
        if isinstance(response_body, bytes) and response_body:
            return ServiceResult(
                value=record if record is not None else raw,
                status=_safe_response_status(response_status, default=200 if response_replay else status),
                replay=bool(response_replay),
                response_body=response_body,
            )
        return _result(record if record is not None else raw, status=_safe_response_status(response_status, default=200 if response_replay else status), replay=bool(response_replay))

    @staticmethod
    def _decode_claim_payload(value: Any) -> bytes | None:
        """Decode the canonical payload returned by the claim RPC.

        PostgreSQL JSON responses represent bytes with the store's closed
        ``__okg_bytes_b64`` wrapper.  The claim response is the authority for
        the bytes handed to a worker; this helper intentionally does not read
        the snapshot or payload-version cache.
        """

        if value is None:
            return None
        if isinstance(value, Mapping):
            if set(value) != {"__okg_bytes_b64"} or not isinstance(value.get("__okg_bytes_b64"), str):
                raise ExternalActionError(
                    "review_authority_unavailable",
                    "claim authority returned an invalid canonical payload",
                    status=503,
                )
            try:
                value = base64.b64decode(value["__okg_bytes_b64"], validate=True)
            except (TypeError, ValueError) as exc:
                raise ExternalActionError(
                    "review_authority_unavailable",
                    "claim authority returned an invalid canonical payload",
                    status=503,
                ) from exc
        if isinstance(value, str):
            value = value.encode("utf-8")
        if not isinstance(value, bytes):
            raise ExternalActionError(
                "review_authority_unavailable",
                "claim authority returned an invalid canonical payload",
                status=503,
            )
        try:
            parsed = strict_signed_json_loads(value)
            from .action_contracts import canonical_json_bytes

            if canonical_json_bytes(parsed) != value:
                raise ValueError("payload is not canonical")
        except (SigningError, ValueError, TypeError) as exc:
            raise ExternalActionError(
                "review_authority_unavailable",
                "claim authority returned an invalid canonical payload",
                status=503,
            ) from exc
        return value

    def _durable_claim_result(self, raw: Any, *, status: int) -> ServiceResult:
        """Build a claim response exclusively from the claim RPC projection."""

        if isinstance(raw, ServiceResult):
            return raw
        if not isinstance(raw, Mapping):
            raise ExternalActionError(
                "review_authority_unavailable",
                "claim authority returned an invalid projection",
                status=503,
            )
        response = dict(raw)
        payload = response.get("canonical_payload")
        if payload is None:
            response_body = response.get("response_body_jcs", response.get("response_body"))
            if isinstance(response_body, Mapping) and set(response_body) == {"__okg_bytes_b64"}:
                response_body = self._decode_claim_payload(response_body)
            if isinstance(response_body, str):
                response_body = response_body.encode("utf-8")
            if isinstance(response_body, bytes):
                try:
                    parsed = strict_signed_json_loads(response_body)
                except SigningError as exc:
                    raise ExternalActionError(
                        "review_authority_unavailable",
                        "claim authority returned an invalid response",
                        status=503,
                    ) from exc
                if isinstance(parsed, Mapping):
                    payload = parsed.get("canonical_payload")
        decoded = self._decode_claim_payload(payload)
        if decoded is None:
            raise ExternalActionError(
                "review_authority_unavailable",
                "claim authority did not return the approved canonical payload",
                status=503,
            )
        # Keep the service value JSON-shaped for the HTTP adapter while
        # preserving the exact bytes in the response body's canonical string.
        response["canonical_payload"] = decoded.decode("utf-8")
        response_replay = response.get("replay", response.get("idempotent_replay", False))
        response_status = response.get(
            "status", response.get("response_status", 200 if response_replay else status)
        )
        response_body = response.get("response_body_jcs", response.get("response_body"))
        if isinstance(response_body, Mapping) and set(response_body) == {"__okg_bytes_b64"}:
            try:
                response_body = base64.b64decode(response_body["__okg_bytes_b64"], validate=True)
            except (TypeError, ValueError) as exc:
                raise ExternalActionError(
                    "review_authority_unavailable",
                    "claim authority returned an invalid response",
                    status=503,
                ) from exc
        if isinstance(response_body, str):
            response_body = response_body.encode("utf-8")
        if isinstance(response_body, bytes) and response_body:
            return ServiceResult(
                value=response,
                status=_safe_response_status(response_status, default=200 if response_replay else status),
                replay=bool(response_replay),
                response_body=response_body,
            )
        return _result(
            response,
            status=_safe_response_status(response_status, default=200 if response_replay else status),
            replay=bool(response_replay),
        )

    def _contract(self, value: ActionContract | Mapping[str, Any]) -> ActionContract:
        if self.action_contracts and isinstance(value, Mapping):
            requested_type = value.get("action_type")
            pinned = self.action_contracts.get(requested_type) if isinstance(requested_type, str) else None
            if pinned is None:
                raise ExternalActionError("review_authority_unavailable", "action contract is not server-pinned", status=503)
            try:
                contract = pinned if isinstance(pinned, ActionContract) else ActionContract.from_mapping(pinned)
            except ActionContractError as exc:
                raise ExternalActionError("review_authority_unavailable", "action contract registry unavailable", status=503) from exc
            supplied_digest = value.get("action_contract_digest")
            if supplied_digest is not None and supplied_digest != contract.action_contract_digest:
                raise ExternalActionError("invalid_proposal", "action contract digest mismatch")
            return contract
        if self.action_contracts and isinstance(value, ActionContract):
            pinned = self.action_contracts.get(value.action_type)
            if pinned is None:
                raise ExternalActionError("review_authority_unavailable", "action contract is not server-pinned", status=503)
            pinned_contract = pinned if isinstance(pinned, ActionContract) else ActionContract.from_mapping(pinned)
            if pinned_contract.action_contract_digest != value.action_contract_digest:
                raise ExternalActionError("invalid_proposal", "action contract is not server-pinned")
            return pinned_contract
        if isinstance(value, ActionContract):
            return value
        try:
            return ActionContract.from_mapping(value)
        except ActionContractError as exc:
            raise ExternalActionError(exc.code, "invalid action contract") from exc

    def _stored_contract(self, proposal: Proposal) -> ActionContract | None:
        """Resolve the immutable contract retained with a proposal.

        Durable snapshots may expose either the in-memory registry object or
        its JSON mapping.  Reporting and cancellation must not require a live
        registry, but they must never fall back to an unbound caller contract.
        """

        candidate = self.store.contracts.get(proposal.action_contract_digest)
        if candidate is None:
            candidate = proposal.action_contract_record
        if candidate is None:
            return None
        try:
            if isinstance(candidate, ActionContract):
                contract = candidate
            else:
                contract = ActionContract.from_mapping(candidate)
        except (ActionContractError, TypeError, ValueError) as exc:
            raise ExternalActionError("review_authority_unavailable", "stored action contract is unavailable", status=503) from exc
        if contract.action_contract_digest != proposal.action_contract_digest:
            raise ExternalActionError("review_authority_unavailable", "stored action contract is not bound", status=503)
        return contract

    def _existing_submit_proposal(
        self, deployment: str, separation_subject: str, key_hash: str
    ) -> Proposal | None:
        """Find a durable submit replay before consulting the live registry.

        A committed replay is authenticated by its stored request hash and
        retained contract record.  It must remain convergent after an action
        registry rotation or temporary registry outage; only a new submit
        needs the deployment's current contract authority.
        """

        with self.store.lock:
            if self._is_durable():
                self._refresh_durable()
            records = getattr(self.store, "submit_idempotency", None)
            getter = getattr(records, "get", None)
            if not callable(getter):
                return None
            existing = getter((deployment, separation_subject, key_hash))
            if existing is None:
                return None
            proposal_id = None
            if isinstance(existing, Mapping):
                proposal_id = existing.get("proposal_id")
            elif isinstance(existing, (tuple, list)) and existing:
                proposal_id = existing[0]
            if not isinstance(proposal_id, str):
                raise ExternalActionError(
                    "review_authority_unavailable",
                    "stored submit replay is unavailable",
                    status=503,
                )
            proposal = self.store.get(proposal_id)
            if not isinstance(proposal, Proposal):
                raise ExternalActionError(
                    "review_authority_unavailable",
                    "stored submit replay is unavailable",
                    status=503,
                )
            return proposal

    @staticmethod
    def _idempotency_hash(value: str) -> str:
        try:
            return idempotency_key_hash(value)
        except ActionContractError as exc:
            raise ExternalActionError("invalid_proposal", "invalid idempotency key") from exc

    def _proposal(self, proposal_id: str) -> Proposal:
        proposal = self.store.get(proposal_id)
        if proposal is None:
            raise ExternalActionError("proposal_not_found", "proposal not found", status=404)
        return proposal

    def _payload(self, proposal_id: str, version: int | None = None) -> PayloadVersion:
        payload = self.store.get_payload(proposal_id, version)
        if payload is None:
            raise ExternalActionError("invalid_proposal", "payload version unavailable")
        return payload

    def _recorded_at(self) -> datetime:
        return self.store.now()

    def _event(
        self,
        proposal: Proposal,
        *,
        event_type: str,
        actor: Actor | None,
        lifecycle_version: int,
        before_review_state: str | None,
        after_review_state: str | None,
        before_execution_state: str | None,
        after_execution_state: str | None,
        payload_version: int | None = None,
        payload_hash: str | None = None,
        decision_id: str | None = None,
        decision_binding: str | None = None,
        execution_id: str | None = None,
        execution_binding: str | None = None,
        transition_reason_code: str | None = None,
        outcome: str | None = None,
        outcome_reason_code: str | None = None,
        outcome_request: str | None = None,
        deadline_kind: str | None = None,
        effective_at: datetime | None = None,
        restore_recovery_digest: str | None = None,
    ) -> AuditEvent:
        events = self.store.events[proposal.effect_instance_key]
        sequence = len(events) + 1
        previous = events[-1].event_hash if events else None
        recorded = self._recorded_at()
        fields = {
            "proposal_id": proposal.proposal_id, "execution_epoch": proposal.execution_epoch,
            "effect_instance_key": proposal.effect_instance_key, "sequence": sequence,
            "lifecycle_version": lifecycle_version, "event_type": event_type,
            "actor_kind": "authenticated_principal" if actor else "system_clock",
            "actor_principal": actor.principal if actor else None,
            "actor_separation_subject": actor.separation_subject if actor else None,
            "authentication_audit_ref": actor.authentication_audit_ref if actor else None,
            "nomos_decision_ref": actor.nomos_decision_ref if actor else None,
            "deadline_kind": deadline_kind, "policy_digest": self.policy_digest,
            "before_review_state": before_review_state, "after_review_state": after_review_state,
            "before_execution_state": before_execution_state, "after_execution_state": after_execution_state,
            "payload_version": payload_version, "payload_hash": payload_hash,
            "decision_id": decision_id, "decision_binding_hash": decision_binding,
            "execution_id": execution_id, "execution_binding_hash": execution_binding,
            "transition_reason_code": transition_reason_code, "outcome": outcome,
            "outcome_reason_code": outcome_reason_code, "outcome_request_hash": outcome_request,
            "restore_recovery_digest": restore_recovery_digest,
            "effective_at": format_timestamp(effective_at) if effective_at else None,
            "recorded_at": format_timestamp(recorded), "previous_event_hash": previous,
        }
        event = AuditEvent(**fields, event_hash=event_hash(fields))
        events.append(event)
        return event

    def _projection_result(self, proposal: Proposal, *, status: int, replay: bool = False, include_payload: bool = False) -> ServiceResult:
        payload = self._payload(proposal.proposal_id).canonical_payload if include_payload else None
        return _result_value(proposal, status=status, replay=replay, include_payload=include_payload, payload=payload)

    def _materialize_expiry(self, proposal: Proposal) -> Proposal:
        if self._is_durable():
            materialize = getattr(self.store, "materialize_expiry", None)
            if callable(materialize):
                refreshed = materialize(proposal.proposal_id)
                return refreshed or self.store.get(proposal.proposal_id) or proposal
            raise ExternalActionError("review_authority_unavailable", "expiry authority unavailable", status=503)
        now = self._recorded_at()
        if proposal.review_state in {"pending", "edited"} and now >= _utc(proposal.effective_review_expires_at):
            before_review, before_execution = proposal.review_state, proposal.execution_state
            proposal = replace(proposal, review_state="expired", execution_state="unavailable", lifecycle_version=proposal.lifecycle_version + 1, updated_at=now)
            self.store.proposals[proposal.proposal_id] = proposal
            self._event(proposal, event_type="review_expired", actor=None, lifecycle_version=proposal.lifecycle_version,
                        before_review_state=before_review, after_review_state="expired", before_execution_state=before_execution,
                        after_execution_state="unavailable", deadline_kind="review_expires_at", effective_at=_utc(proposal.effective_review_expires_at))
        elif proposal.review_state == "approved" and proposal.execution_state == "ready" and proposal.execute_before is not None and now >= _utc(proposal.execute_before):
            before_review, before_execution = proposal.review_state, proposal.execution_state
            proposal = replace(proposal, review_state="expired", execution_state="unavailable", lifecycle_version=proposal.lifecycle_version + 1, updated_at=now)
            self.store.proposals[proposal.proposal_id] = proposal
            self._event(proposal, event_type="approval_expired", actor=None, lifecycle_version=proposal.lifecycle_version,
                        before_review_state=before_review, after_review_state="expired", before_execution_state=before_execution,
                        after_execution_state="unavailable", payload_version=proposal.payload_version, payload_hash=proposal.current_payload_hash,
                        decision_id=proposal.current_decision_id, decision_binding=proposal.current_decision_binding_hash,
                        deadline_kind="execute_before", effective_at=_utc(proposal.execute_before))
        return proposal

    def _check_match(self, proposal: Proposal, *, if_match: str | None, expected_lifecycle_version: int | None, expected_payload_version: int | None, expected_payload_hash: str | None) -> None:
        if if_match is None and expected_lifecycle_version is None and expected_payload_version is None and expected_payload_hash is None:
            raise ExternalActionError("proposal_precondition_required", "If-Match is required", status=428)
        if if_match is not None:
            if not re.fullmatch(r'"lifecycle:(0|[1-9][0-9]*);payload-version:(0|[1-9][0-9]*);payload:(sha256:[0-9a-f]{64})"', if_match):
                raise ExternalActionError("proposal_precondition_required", "If-Match is invalid", status=428)
            expected = proposal.etag
            if if_match != expected:
                match = re.match(r'^"lifecycle:(\d+);payload-version:(\d+);payload:(sha256:[0-9a-f]{64})"$', if_match)
                if match and int(match.group(1)) == proposal.lifecycle_version and int(match.group(2)) == proposal.payload_version and match.group(3) != proposal.current_payload_hash:
                    raise ExternalActionError("stale_payload_binding", "payload binding is stale", detail={"current_payload_hash": proposal.current_payload_hash})
                raise ExternalActionError("stale_proposal_lifecycle", "proposal lifecycle is stale", detail={"current_lifecycle_version": proposal.lifecycle_version})
            return
        if expected_lifecycle_version is not None and expected_lifecycle_version != proposal.lifecycle_version:
            raise ExternalActionError("stale_proposal_lifecycle", "proposal lifecycle is stale", detail={"current_lifecycle_version": proposal.lifecycle_version})
        if expected_payload_version is not None and expected_payload_version != proposal.payload_version:
            raise ExternalActionError("stale_payload_binding", "payload version is stale")
        if expected_payload_hash is not None and expected_payload_hash != proposal.current_payload_hash:
            raise ExternalActionError("stale_payload_binding", "payload hash is stale")

    @staticmethod
    def _etag_expectations(if_match: str | None) -> tuple[int | None, int | None, str | None]:
        """Return the canonical expectation tuple represented by an ETag.

        The transition idempotency hash must bind the complete conditional
        request, including an If-Match header.  Keeping this parsing separate
        from ``_check_match`` also preserves the latter's precise stale-payload
        versus stale-lifecycle error classification.
        """

        if if_match is None:
            return None, None, None
        match = re.fullmatch(r'"lifecycle:(\d+);payload-version:(\d+);payload:(sha256:[0-9a-f]{64})"', if_match)
        if match is None:
            return None, None, None
        return int(match.group(1)), int(match.group(2)), match.group(3)

    def _transition_record(
        self, proposal: Proposal, actor: Actor, operation: str, key_hash: str
    ) -> TransitionRecord | None:
        for key in (
            (proposal.deployment, proposal.proposal_id, actor.separation_subject, key_hash),
            (proposal.deployment, proposal.proposal_id, actor.separation_subject + ":" + key_hash),
        ):
            candidate = self.store.transition_idempotency.get(key)
            if candidate is not None:
                return candidate
        return None

    def _transition_replay(self, proposal: Proposal, actor: Actor, operation: str, key_hash: str, request_hash: str) -> ServiceResult | None:
        row = self._transition_record(proposal, actor, operation, key_hash)
        if row is None:
            return None
        if row.transition_request_hash != request_hash:
            raise ExternalActionError("idempotency_conflict", "idempotency key was reused with a different request")
        return replace(row.result, replay=True)

    def _store_transition(self, proposal: Proposal, actor: Actor, operation: str, key_hash: str, request_hash: str, result: ServiceResult) -> ServiceResult:
        key = (proposal.deployment, proposal.proposal_id, actor.separation_subject + ":" + key_hash)
        self.store.transition_idempotency[key] = TransitionRecord(proposal.deployment, proposal.proposal_id, operation, actor.separation_subject, key_hash, request_hash, result)
        return result

    @staticmethod
    def _reason(contract: ActionContract, reason_code: str | None, *, required: bool = True) -> str | None:
        if reason_code is None and not required:
            return None
        if not isinstance(reason_code, str) or reason_code not in contract.transition_reason_codes:
            raise ExternalActionError("invalid_proposal", "reason code is not allowed")
        return reason_code

    def _handoff_envelope(self, payload: Mapping[str, Any]) -> dict[str, str]:
        """Obtain a server-signed claim handoff through an injected boundary.

        The worker and HTTP request never supply these bytes.  Deployments
        must inject a signer backed by their key service; an absent signer is
        an authority outage, not permission to accept caller-provided JWS
        material.
        """

        key = self.handoff_verification_key
        if not callable(self.handoff_signer) or not isinstance(key, SigningKey):
            raise ExternalActionError(
                "review_authority_unavailable",
                "claim handoff signer is unavailable",
                status=503,
            )
        try:
            value = self.handoff_signer(dict(payload))
        except Exception as exc:
            raise ExternalActionError(
                "review_authority_unavailable",
                "claim handoff signer is unavailable",
                status=503,
            ) from exc
        if not isinstance(value, Mapping) or set(value) != {
            "protected_b64", "payload_b64", "signature_b64", "envelope_digest"
        } or not all(isinstance(item, str) and item for item in value.values()):
            raise ExternalActionError(
                "review_authority_unavailable",
                "claim handoff signer returned an invalid envelope",
                status=503,
            )
        try:
            claimed_at = parse_timestamp(payload["claimed_at"])
            if key.not_before is not None and claimed_at < parse_timestamp(key.not_before):
                raise SigningError("claim handoff key is not yet valid")
            if key.not_after is not None and claimed_at >= parse_timestamp(key.not_after):
                raise SigningError("claim handoff key has expired")
            verified_payload = verify_detached_envelope(
                value,
                key.public_key_b64,
                expected_kid=key.kid,
                expected_typ="okg-claim-handoff+jws",
                expected_domain="okg-external-action-claim-handoff-receipt-envelope-v1",
                expected_fields=_CLAIM_HANDOFF_FIELDS,
            )
            if verified_payload != dict(payload):
                raise SigningError("claim handoff signer changed the closed payload")
        except (KeyError, TypeError, ValueError, SigningError) as exc:
            raise ExternalActionError(
                "review_authority_unavailable",
                "claim handoff signer returned an invalid envelope",
                status=503,
            ) from exc
        return dict(value)

    def _completion_envelope(self, payload: Mapping[str, Any]) -> dict[str, str]:
        """Sign the database-chosen restore completion projection.

        Finalization calls this only after the store has prepared the exact
        post-recovery roots, count, and database completion time on the active
        transaction.  The request cannot provide any of those values and the
        returned receipt is verified immediately so a broken key service
        rolls the transaction back rather than clearing quarantine.
        """

        key = self.handoff_verification_key
        if not callable(self.handoff_signer) or not isinstance(key, SigningKey):
            raise ExternalActionError(
                "review_authority_unavailable",
                "restore completion signer is unavailable",
                status=503,
            )
        try:
            value = self.handoff_signer(dict(payload))
        except Exception as exc:
            raise ExternalActionError(
                "review_authority_unavailable",
                "restore completion signer is unavailable",
                status=503,
            ) from exc
        if not isinstance(value, Mapping) or set(value) != {
            "protected_b64", "payload_b64", "signature_b64", "envelope_digest"
        } or not all(isinstance(item, str) and item for item in value.values()):
            raise ExternalActionError(
                "review_authority_unavailable",
                "restore completion signer returned an invalid envelope",
                status=503,
            )
        try:
            completed_at = parse_timestamp(payload["completed_at"])
            if key.not_before is not None and completed_at < parse_timestamp(key.not_before):
                raise SigningError("restore completion key is not yet valid")
            if key.not_after is not None and completed_at >= parse_timestamp(key.not_after):
                raise SigningError("restore completion key has expired")
            verified_payload = verify_detached_envelope(
                value,
                key.public_key_b64,
                expected_kid=key.kid,
                expected_typ="okg-restore-completion+jws",
                expected_domain="okg-external-action-restore-completion-receipt-envelope-v1",
                expected_fields=_RESTORE_COMPLETION_FIELDS,
            )
            if verified_payload != dict(payload):
                raise SigningError("restore completion signer changed the closed payload")
        except (KeyError, TypeError, ValueError, SigningError) as exc:
            raise ExternalActionError(
                "review_authority_unavailable",
                "restore completion signer returned an invalid envelope",
                status=503,
            ) from exc
        return dict(value)

    def _verify_owner_attestation_envelope(
        self,
        envelope: Mapping[str, Any],
        *,
        execution: ExecutionClaim,
        actor: Actor,
        allow_historical_replay: bool,
    ) -> tuple[dict[str, Any], SigningKey, str]:
        """Verify and decompose one operator-signed owner attestation."""

        if not isinstance(envelope, Mapping) or set(envelope) != {
            "protected_b64", "payload_b64", "signature_b64", "envelope_digest"
        }:
            raise ExternalActionError("invalid_proposal", "owner attestation envelope is invalid")
        try:
            protected = strict_signed_json_loads(
                b64url_decode(envelope["protected_b64"], field="protected_b64")
            )
            if not isinstance(protected, Mapping) or set(protected) != {"alg", "kid", "typ"}:
                raise SigningError("owner attestation protected header is invalid")
            kid = protected.get("kid")
            if not isinstance(kid, str):
                raise SigningError("owner attestation signing key is invalid")
            key = self.owner_attestation_keys.get(kid)
            if (
                not isinstance(key, SigningKey)
                or key.purpose != "owner_unavailability_attestation"
                or key.algorithm != "EdDSA"
                or key.state == "revoked_compromised"
                or (key.state != "current" and not allow_historical_replay)
            ):
                raise SigningError("owner attestation signing key is unavailable")
            payload = verify_detached_envelope(
                envelope,
                key.public_key_b64,
                expected_kid=kid,
                expected_typ="application/okg-external-action-owner-unavailability-attestation+jws",
                expected_domain="okg-external-action-owner-unavailability-attestation-envelope-v1",
                expected_fields=(
                    "deployment", "execution_id", "claim_owner_separation_subject",
                    "reconcile_not_before", "issued_at", "expires_at", "nonce",
                    "signer_separation_subject",
                ),
            )
            issued_at = parse_timestamp(payload["issued_at"])
            expires_at = parse_timestamp(payload["expires_at"])
            reconcile_not_before = parse_timestamp(payload["reconcile_not_before"])
            key_not_before = parse_timestamp(key.not_before) if key.not_before is not None else None
            key_not_after = parse_timestamp(key.not_after) if key.not_after is not None else None
        except (KeyError, TypeError, ValueError, SigningError) as exc:
            raise ExternalActionError("invalid_proposal", "owner attestation envelope is invalid") from exc

        proposal = self._proposal(execution.proposal_id)
        contract = self._stored_contract(proposal)
        if contract is None:
            raise ExternalActionError(
                "review_authority_unavailable",
                "stored action contract is unavailable",
                status=503,
            )
        expected_reconcile_at = _utc(execution.claimed_at) + timedelta(
            seconds=contract.review_and_execution_limits.reconcile_not_before_seconds
        )
        now = self._recorded_at()
        maximum_lifetime = self.owner_attestation_maximum_lifetime_seconds
        nonce = payload["nonce"]
        signer = payload["signer_separation_subject"]
        if (
            payload["deployment"] != proposal.deployment
            or payload["execution_id"] != execution.execution_id
            or payload["claim_owner_separation_subject"] != execution.worker_separation_subject
            or payload["reconcile_not_before"] != format_timestamp(expected_reconcile_at)
            or reconcile_not_before != expected_reconcile_at
            or issued_at < reconcile_not_before
            or issued_at > now
            or expires_at <= issued_at
            or type(maximum_lifetime) is not int
            or expires_at - issued_at > timedelta(seconds=maximum_lifetime)
            or (not allow_historical_replay and now >= expires_at)
            or not isinstance(nonce, str)
            or not re.fullmatch(r"[A-Za-z0-9._:+/\-]{16,128}", nonce)
            or not isinstance(signer, str)
            or signer != actor.separation_subject
            or signer == execution.worker_separation_subject
            or (key_not_before is not None and issued_at < key_not_before)
            or (key_not_after is not None and issued_at >= key_not_after)
        ):
            raise ExternalActionError("invalid_proposal", "owner attestation is not valid")
        return payload, key, kid

    def _require_claim_owner_unavailable(
        self,
        *,
        execution: ExecutionClaim,
        proposal: Proposal,
        reconciler: Actor,
        attestation_digest: str,
    ) -> None:
        attestation = self.store.attestations.get(attestation_digest)
        if not isinstance(attestation, Mapping):
            raise ExternalActionError("invalid_proposal", "owner unavailability attestation is not bound")
        try:
            reconcile_not_before = _utc(attestation["reconcile_not_before"])
            expires_at = _utc(attestation["expires_at"])
            signer = attestation["signer_separation_subject"]
        except (KeyError, TypeError, ValueError, ExternalActionError) as exc:
            raise ExternalActionError("invalid_proposal", "owner unavailability attestation is not bound") from exc
        now = self._recorded_at()
        if (
            attestation.get("execution_id") != execution.execution_id
            or attestation.get("claim_owner_separation_subject") != execution.worker_separation_subject
            or now < reconcile_not_before
            or now >= expires_at
            or signer in {execution.worker_separation_subject, reconciler.separation_subject}
        ):
            raise ExternalActionError("invalid_proposal", "owner unavailability attestation is not current")

        verifier = self.claim_owner_status_verifier
        if verifier is None:
            verifier = getattr(self.store, "verify_claim_owner_unavailable", None)
        if not callable(verifier) or not execution.authentication_audit_id:
            raise ExternalActionError(
                "review_authority_unavailable",
                "claim owner identity authority is unavailable",
                status=503,
            )
        try:
            status = verifier(
                deployment=proposal.deployment,
                execution_id=execution.execution_id,
                authentication_audit_id=execution.authentication_audit_id,
                claim_owner_separation_subject=execution.worker_separation_subject,
                claimed_at=format_timestamp(_utc(execution.claimed_at)),
                observed_at=format_timestamp(now),
            )
        except Exception as exc:
            raise ExternalActionError(
                "review_authority_unavailable",
                "claim owner identity authority is unavailable",
                status=503,
            ) from exc
        if not isinstance(status, Mapping) or set(status) != {
            "authentication_audit_id", "unavailable", "updated_at", "active_replacement"
        }:
            raise ExternalActionError(
                "review_authority_unavailable",
                "claim owner identity authority is unavailable",
                status=503,
            )
        try:
            updated_at = _utc(status["updated_at"])
        except (TypeError, ValueError, ExternalActionError) as exc:
            raise ExternalActionError(
                "review_authority_unavailable",
                "claim owner identity authority is unavailable",
                status=503,
            ) from exc
        if (
            status["authentication_audit_id"] != execution.authentication_audit_id
            or status["unavailable"] is not True
            or status["active_replacement"] is not False
            or updated_at <= _utc(execution.claimed_at)
        ):
            raise ExternalActionError("illegal_proposal_transition", "claim owner is not durably unavailable")

    # ----- proposal lifecycle --------------------------------------------

    def run_record(self, **kwargs: Any) -> Any:
        """Register/read immutable facts without running an automation."""
        from .run_records import RunRecords

        return RunRecords(self).execute(**kwargs)

    def submit(
        self,
        *,
        deployment: str,
        actor: Actor | Mapping[str, Any] | str | None = None,
        proposer: Actor | Mapping[str, Any] | str | None = None,
        idempotency_key: str,
        automation_id: str,
        automation_version: str,
        run_id: str,
        correlation_id: str,
        action_type: str,
        target_ref: str,
        risk_tier: str,
        action_schema_digest: str,
        action_contract: ActionContract | Mapping[str, Any],
        requested_review_expires_at: datetime | str,
        summary_code: str,
        payload: Any,
        evidence_digests: Sequence[str] = (),
        execution_epoch: int | None = None,
        action_contract_digest_value: str | None = None,
        action_contract_digest: str | None = None,
    ) -> ServiceResult:
        actor = self._actor(actor if actor is not None else proposer)  # type: ignore[arg-type]
        # Resolve an existing submit key before asking the current action
        # registry for a contract.  Registry updates must not make a
        # committed idempotent replay impossible; the retained proposal's
        # immutable contract is the authority for that comparison.  A new
        # key still takes the live deployment-pinned registry path below.
        key_hash = self._idempotency_hash(idempotency_key)
        replay_proposal = self._existing_submit_proposal(
            deployment, actor.separation_subject, key_hash
        )
        contract = (
            self._stored_contract(replay_proposal)
            if replay_proposal is not None
            else self._contract(action_contract)
        )
        if contract is None:
            raise ExternalActionError(
                "review_authority_unavailable",
                "stored action contract is unavailable",
                status=503,
            )
        supplied_digest = action_contract_digest_value or action_contract_digest or (action_contract.get("action_contract_digest") if isinstance(action_contract, Mapping) else None)
        expected_contract_digest = contract.action_contract_digest
        if supplied_digest is not None and supplied_digest != expected_contract_digest:
            raise ExternalActionError("invalid_proposal", "action contract digest mismatch")
        if action_type != contract.action_type:
            raise ExternalActionError("invalid_proposal", "action type does not match action contract")
        if action_schema_digest != contract.action_schema_digest:
            raise ExternalActionError("invalid_proposal", "action schema digest mismatch")
        if not isinstance(target_ref, str) or len(target_ref.encode("utf-8")) > 256:
            raise ExternalActionError("invalid_proposal", "target reference is too long")
        try:
            for field, value, limit in (
                ("automation_id", automation_id, 128), ("run_id", run_id, 128),
                ("correlation_id", correlation_id, 128), ("automation_version", automation_version, 64),
            ):
                if not isinstance(value, str) or not 1 <= len(value) <= limit or not re.fullmatch(r"[A-Za-z0-9._:+\-]+", value):
                    raise ActionContractError(f"invalid {field}", code="invalid_proposal")
            canonical_payload = contract.validate_submission(target_ref=target_ref, risk_tier=risk_tier, summary_code=summary_code, payload=payload)
            for digest in evidence_digests:
                validate_digest(digest, field="evidence_digest")
            evidence = tuple(sorted(set(evidence_digests)))
        except ActionContractError as exc:
            raise ExternalActionError(exc.code if exc.code in {"review_authority_unavailable", "payload_limit"} else "invalid_proposal", "invalid proposal") from exc
        requested = _utc(requested_review_expires_at)
        limits = contract.review_and_execution_limits
        epoch = self.store.execution_epoch if execution_epoch is None else execution_epoch
        payload_hash = digest_value("okg-external-action-payload-v1", strict_payload(payload))
        content_fields = {
            "automation_id": automation_id, "automation_version": automation_version, "run_id": run_id,
            "correlation_id": correlation_id, "action_type": action_type, "target_ref": target_ref, "risk_tier": risk_tier,
            "action_schema_digest": action_schema_digest, "action_contract_digest": expected_contract_digest,
            "original_payload_hash": payload_hash, "requested_review_expires_at": format_timestamp(requested),
            "summary_code": summary_code, "evidence_digests": list(evidence),
        }
        content_hash = submission_content_hash(content_fields)
        stable_effect = effect_instance_key(deployment, actor.separation_subject, key_hash)
        request_fields = {
            "deployment": deployment, "execution_epoch": epoch, "effect_instance_key": stable_effect,
            "submission_content_hash": content_hash, "proposer_separation_subject": actor.separation_subject,
            "automation_id": automation_id, "automation_version": automation_version, "run_id": run_id,
            "correlation_id": correlation_id, "action_type": action_type, "target_ref": target_ref, "risk_tier": risk_tier,
            "action_schema_digest": action_schema_digest, "action_contract_digest": expected_contract_digest,
            "executor_contract_digest": contract.executor_contract_digest, "payload_renderer_digest": contract.payload_renderer_digest,
            "governed_tool_binding_digest": contract.governed_tool_binding_digest,
            "provider_connection_contract_digest": contract.provider_connection_contract_digest,
            "provider_capability_digest": contract.provider_capability_digest, "provider_operation_id": contract.provider_operation_id,
            "provider_operation_descriptor_digest": contract.provider_operation_descriptor_digest,
            "original_payload_hash": payload_hash, "requested_review_expires_at": format_timestamp(requested),
            "summary_code": summary_code, "evidence_digests": list(evidence),
        }
        request_hash = digest_value("okg-external-action-proposal-request-v1", request_fields)
        with self.store.lock:
            actor = self._authorize(
                "external_action.propose",
                actor,
                context={"deployment": deployment, "request_hash": request_hash, "surface": "external_actions"},
            )
            authorization = self._authorization(actor)
            if self._is_durable() and (
                callable(getattr(self.store, "restore_fence", None))
                or hasattr(self.store, "restore_fences")
            ) and self._persisted_service_mode(deployment) == "restore_only":
                # A submit replay is allowed to reach the fixed SQL command;
                # a new key is rejected before any lifecycle RPC.  SQL still
                # performs the authoritative request-hash comparison.
                records = getattr(self.store, "submit_idempotency", None)
                existing = records.get((deployment, actor.separation_subject, key_hash)) if callable(getattr(records, "get", None)) else None
                if existing is None:
                    raise ExternalActionError(
                        "external_action_restore_quarantined",
                        "external action lifecycle is quarantined for restore",
                        status=409,
                    )
            now = self._recorded_at()
            effective = min(
                requested,
                now + timedelta(seconds=limits.maximum_review_lifetime_seconds),
            )
            if self._is_durable():
                proposal_id = self.id_factory()
                command_request = {
                    **request_fields,
                    "proposal_id": proposal_id,
                    "submission_request_hash": request_hash,
                    "submission_idempotency_key_hash": key_hash,
                    "canonical_payload": canonical_payload,
                    "parent_payload_hash": None,
                    "difference_field_codes": [],
                    "action_contract_record": contract.to_mapping(),
                    "policy_digest": self.policy_digest,
                    "requested_review_expires_at": format_timestamp(requested),
                    "effective_review_expires_at": format_timestamp(effective),
                    "summary_code": summary_code,
                    "review_state": "pending",
                    "execution_state": "unavailable",
                    "lifecycle_version": 1,
                    "payload_version": 1,
                    "current_payload_hash": payload_hash,
                }
                raw = self.store.submit_command(SubmitCommand(command_request, actor, authorization))
                return self._durable_result(raw, proposal_id=proposal_id, status=201)
            existing = self.store.submit_idempotency.get((deployment, actor.separation_subject, key_hash))
            if existing is not None:
                existing_id, existing_hash = existing
                if existing_hash != request_hash:
                    raise ExternalActionError("idempotency_conflict", "idempotency key was reused with a different request")
                return self._projection_result(self._proposal(existing_id), status=200, replay=True)
            if effective <= now:
                raise ExternalActionError("invalid_proposal", "review expiry must be in the future")
            if epoch != self.store.execution_epoch:
                raise ExternalActionError(
                    "execution_epoch_authority_unavailable",
                    "execution epoch is not current",
                    status=503,
                )
            proposal_id = self.id_factory()
            proposal = Proposal(
                proposal_id=proposal_id, deployment=deployment, execution_epoch=epoch, effect_instance_key=stable_effect,
                submission_content_hash=content_hash, proposal_request_hash=request_hash, proposer_principal=actor.principal,
                proposer_separation_subject=actor.separation_subject, automation_id=automation_id, automation_version=automation_version,
                run_id=run_id, correlation_id=correlation_id, action_type=action_type, target_ref=target_ref, risk_tier=risk_tier,
                action_schema_digest=action_schema_digest, action_contract_digest=expected_contract_digest,
                executor_contract_digest=contract.executor_contract_digest, payload_renderer_digest=contract.payload_renderer_digest,
                governed_tool_binding_digest=contract.governed_tool_binding_digest,
                provider_connection_contract_digest=contract.provider_connection_contract_digest,
                provider_capability_digest=contract.provider_capability_digest, provider_operation_id=contract.provider_operation_id,
                provider_operation_descriptor_digest=contract.provider_operation_descriptor_digest,
                original_payload_hash=payload_hash, current_payload_hash=payload_hash, payload_version=1, lifecycle_version=1,
                review_state="pending", execution_state="unavailable", requested_review_expires_at=requested,
                effective_review_expires_at=effective, summary_code=summary_code, evidence_digests=evidence,
                created_at=now, updated_at=now,
                submission_idempotency_key_hash=key_hash,
                action_contract_record=contract.to_mapping(),
                policy_digest=self.policy_digest,
                authentication_audit_id=actor.authentication_audit_id,
                nomos_decision_id=actor.nomos_decision_id,
            )
            self.store.proposals[proposal_id] = proposal
            self.store.contracts[expected_contract_digest] = contract
            self.store.payload_versions[proposal_id].append(PayloadVersion(proposal_id, 1, payload_hash, canonical_payload, created_at=now))
            self.store.submit_idempotency[(deployment, actor.separation_subject, key_hash)] = (proposal_id, request_hash)
            self._event(proposal, event_type="submitted", actor=actor, lifecycle_version=1,
                        before_review_state=None, after_review_state="pending", before_execution_state=None,
                        after_execution_state="unavailable", payload_version=1, payload_hash=payload_hash)
            return self._projection_result(proposal, status=201)

    propose = submit

    def _transition(
        self,
        proposal_id: str,
        *,
        actor: Actor | Mapping[str, Any] | str,
        idempotency_key: str,
        operation: str,
        if_match: str | None = None,
        expected_lifecycle_version: int | None = None,
        expected_payload_version: int | None = None,
        expected_payload_hash: str | None = None,
        replacement_payload: Any | None = None,
        execute_before: datetime | str | None = None,
        reason_code: str | None = None,
        expected_decision_id: str | None = None,
        expected_decision_binding_hash: str | None = None,
        action_contract: ActionContract | Mapping[str, Any] | None = None,
    ) -> ServiceResult:
        actor = self._actor(actor)
        key_hash = self._idempotency_hash(idempotency_key)
        with self.store.lock:
            if self._is_durable():
                # Refresh before replay lookup so a process restart can return
                # the committed historical response before state/expiry
                # guards.  The refresh is read-only and remains inside the
                # store's lifecycle lock.
                self._refresh_durable()
            proposal = self._proposal(proposal_id)
            replay_record = self._transition_record(proposal, actor, operation, key_hash)
            if replay_record is not None:
                # A replay uses the proposal's immutable contract record.  Do
                # not require the current registry to retain the historical
                # action type or digest merely to compare the stored request.
                contract = self._stored_contract(proposal)
                if contract is None:
                    raise ExternalActionError(
                        "review_authority_unavailable",
                        "stored action contract is unavailable",
                        status=503,
                    )
                supplied_type = getattr(action_contract, "action_type", None)
                supplied_digest = getattr(action_contract, "action_contract_digest", None)
                if isinstance(action_contract, Mapping):
                    supplied_type = action_contract.get("action_type", supplied_type)
                    supplied_digest = action_contract.get("action_contract_digest", supplied_digest)
                if supplied_type is not None and supplied_type != contract.action_type:
                    raise ExternalActionError("invalid_proposal", "action contract type mismatch")
                if supplied_digest is not None and supplied_digest != contract.action_contract_digest:
                    raise ExternalActionError("invalid_proposal", "action contract digest mismatch")
            else:
                contract = self._contract(action_contract) if action_contract is not None else None
            etag_lifecycle, etag_payload_version, etag_payload_hash = self._etag_expectations(if_match)
            try:
                replacement_hash = (
                    digest_value("okg-external-action-payload-v1", strict_payload(replacement_payload))
                    if replacement_payload is not None else None
                )
            except ActionContractError as exc:
                raise ExternalActionError("invalid_proposal", "replacement payload is invalid") from exc
            request_hash = transition_request_hash({
                "operation": operation,
                "action_contract_digest": contract.action_contract_digest if contract is not None else proposal.action_contract_digest,
                "expected_lifecycle_version": etag_lifecycle if etag_lifecycle is not None else expected_lifecycle_version,
                "expected_payload_version": etag_payload_version if etag_payload_version is not None else expected_payload_version,
                "expected_payload_hash": etag_payload_hash if etag_payload_hash is not None else expected_payload_hash,
                "expected_decision_id": expected_decision_id, "expected_decision_binding_hash": expected_decision_binding_hash,
                "replacement_payload_hash": replacement_hash,
                "execute_before": format_timestamp(_utc(execute_before)) if execute_before is not None else None,
                "reason_code": reason_code,
            })
            actor = self._authorize(
                "external_action.cancel" if operation == "cancel" else "external_action.decide",
                actor,
                context={"deployment": proposal.deployment, "request_hash": request_hash, "surface": "external_actions"},
            )
            authorization = self._authorization(actor)
            replay = self._transition_replay(proposal, actor, operation, key_hash, request_hash)
            if replay is not None:
                return replay
            if self._persisted_service_mode(proposal.deployment) == "restore_only":
                # Exact committed replays are returned above.  A new
                # transition is prohibited while the restored database is
                # quarantined, before expiry or current-state mutation.
                raise ExternalActionError(
                    "external_action_restore_quarantined",
                    "external action lifecycle is quarantined for restore",
                    status=409,
                )
            was_expired = proposal.review_state == "expired"
            proposal = self._materialize_expiry(proposal)
            if proposal.review_state == "expired":
                refusal = ExternalActionError("proposal_expired", "proposal has expired", status=410)
                # This call just recorded the expiry; commit that record with
                # the refusal.  A plain raise rolls the review_expired event
                # back with the durable store's transaction (issue #2342).
                raise refusal if was_expired else committed_refusal(refusal)
            self._check_match(proposal, if_match=if_match, expected_lifecycle_version=expected_lifecycle_version,
                              expected_payload_version=expected_payload_version, expected_payload_hash=expected_payload_hash)
            if contract is not None and contract.action_contract_digest != proposal.action_contract_digest:
                raise ExternalActionError("invalid_proposal", "action contract changed")
            now = self._recorded_at()
            # Perform the same closed guards for the durable path that the
            # in-memory reference applies.  The SQL RPC repeats these checks
            # under its row lock; these values are sent as explicit command
            # fields so the two implementations cannot silently diverge.
            canonical, new_hash, changed = None, None, ()
            checked_reason = None
            if operation in {"edit", "defer", "approve", "approve_with_edit", "reject"}:
                if proposal.review_state not in {"pending", "edited"}:
                    raise ExternalActionError("illegal_proposal_transition", "proposal cannot be reviewed")
                if actor.separation_subject == proposal.proposer_separation_subject:
                    raise ExternalActionError("proposer_approver_separation_required", "proposer cannot approve", status=403)
                if now >= _utc(proposal.effective_review_expires_at):
                    raise ExternalActionError("proposal_expired", "review deadline reached", status=410)
                if contract is None:
                    raise ExternalActionError("review_authority_unavailable", "action contract is required", status=503)
                checked_reason = self._reason(contract, reason_code, required=operation != "edit")
                if operation in {"edit", "approve_with_edit"}:
                    if replacement_payload is None:
                        raise ExternalActionError("invalid_proposal", "replacement payload required")
                    try:
                        canonical = contract.validate_payload(replacement_payload)
                    except ActionContractError as exc:
                        raise ExternalActionError("invalid_proposal", "replacement payload is invalid") from exc
                    new_hash = digest_value("okg-external-action-payload-v1", strict_payload(replacement_payload))
                    changed = tuple(sorted(self._diff_codes(self._payload(proposal.proposal_id).payload, replacement_payload, contract)))
                    if not changed or any(code not in contract.diff_field_codes for code in changed):
                        raise ExternalActionError("invalid_proposal", "replacement payload must change a contract field")
                if operation in {"approve", "approve_with_edit"}:
                    self._approval_deadline(proposal, contract, execute_before)
                if self._is_durable():
                    command_request = {
                        "deployment": proposal.deployment,
                        "proposal_id": proposal_id,
                        "operation": operation,
                        "idempotency_key_hash": key_hash,
                        "transition_request_hash": request_hash,
                        "expected_lifecycle_version": etag_lifecycle if etag_lifecycle is not None else expected_lifecycle_version,
                        "expected_payload_version": etag_payload_version if etag_payload_version is not None else expected_payload_version,
                        "expected_payload_hash": etag_payload_hash if etag_payload_hash is not None else expected_payload_hash,
                        "expected_decision_id": expected_decision_id,
                        "expected_decision_binding_hash": expected_decision_binding_hash,
                        "replacement_payload": replacement_payload,
                        "canonical_payload": canonical,
                        "replacement_payload_hash": new_hash,
                        # The fixed SQL v1 command calls this closed field
                        # ``result_event_ids``; its edit branch stores the
                        # supplied list as redacted difference metadata.  A
                        # future SQL revision may rename it, but this keeps
                        # the service compatible with the installed RPC.
                        "result_event_ids": list(changed),
                        "payload_version": proposal.payload_version,
                        "execute_before": format_timestamp(_utc(execute_before)) if execute_before is not None else None,
                        "reason_code": checked_reason,
                        "decision_id": self.id_factory() if operation in {"approve", "approve_with_edit", "reject"} else None,
                        "decision_binding_hash": None,
                        "event_hash": None,
                        "previous_event_hash": None,
                        "response_status": 200,
                        "action_contract_record": contract.to_mapping() if contract is not None else None,
                        "action_contract_digest": proposal.action_contract_digest,
                        "policy_digest": self.policy_digest,
                    }
                    raw = self.store.review_transition_command(ReviewTransitionCommand(command_request, actor, authorization))
                    return self._durable_result(raw, proposal_id=proposal_id, status=200)
                if operation == "edit":
                    proposal = self._append_edit(proposal, actor, canonical, new_hash, changed)
                    result = self._projection_result(proposal, status=200)
                    return self._store_transition(proposal, actor, operation, key_hash, request_hash, result)
                if operation == "approve_with_edit":
                    # Validate the approval deadline before appending the edit.
                    # The combined operation promises an atomic edit+approval;
                    # an invalid deadline must therefore leave no new payload
                    # version or edit event behind.
                    self._approval_deadline(proposal, contract, execute_before)
                    proposal = self._append_edit(proposal, actor, canonical, new_hash, changed)
                if operation in {"approve", "approve_with_edit"}:
                    proposal = self._append_approval(proposal, actor, contract, execute_before, checked_reason)
                elif operation == "reject":
                    proposal = self._append_rejection(proposal, actor, checked_reason)
                else:  # defer
                    proposal = self._append_defer(proposal, actor, checked_reason)
                result = self._projection_result(proposal, status=200)
                return self._store_transition(proposal, actor, operation, key_hash, request_hash, result)
            if operation == "cancel":
                if actor.separation_subject != proposal.proposer_separation_subject:
                    raise ExternalActionError("illegal_proposal_transition", "only the proposer may cancel")
                if proposal.review_state not in {"pending", "edited", "approved"} or self.store.get_execution(proposal.proposal_id) is not None:
                    raise ExternalActionError("illegal_proposal_transition", "proposal cannot be cancelled")
                # Cancellation is bound to the stored immutable contract; it
                # does not require the action registry to be readable again.
                contract = contract or self._stored_contract(proposal)
                if contract is None:
                    raise ExternalActionError("review_authority_unavailable", "action contract is required", status=503)
                checked_reason = self._reason(contract, reason_code)
                deadline = _utc(proposal.execute_before) if proposal.review_state == "approved" and proposal.execute_before else _utc(proposal.effective_review_expires_at)
                if now >= deadline:
                    raise ExternalActionError("proposal_expired", "cancellation deadline reached", status=410)
                if proposal.review_state == "approved":
                    if expected_decision_id != proposal.current_decision_id or expected_decision_binding_hash != proposal.current_decision_binding_hash:
                        raise ExternalActionError("stale_payload_binding", "approval binding is stale")
                if self._is_durable():
                    command_request = {
                        "deployment": proposal.deployment,
                        "proposal_id": proposal_id,
                        "operation": operation,
                        "idempotency_key_hash": key_hash,
                        "transition_request_hash": request_hash,
                        "expected_lifecycle_version": etag_lifecycle if etag_lifecycle is not None else expected_lifecycle_version,
                        "expected_payload_version": etag_payload_version if etag_payload_version is not None else expected_payload_version,
                        "expected_payload_hash": etag_payload_hash if etag_payload_hash is not None else expected_payload_hash,
                        "expected_decision_id": expected_decision_id,
                        "expected_decision_binding_hash": expected_decision_binding_hash,
                        "replacement_payload": None,
                        "canonical_payload": None,
                        "replacement_payload_hash": None,
                        "difference_field_codes": [],
                        "payload_version": proposal.payload_version,
                        "execute_before": None,
                        "reason_code": checked_reason,
                        "decision_id": None,
                        "decision_binding_hash": None,
                        "event_hash": None,
                        "previous_event_hash": None,
                        "response_status": 200,
                        "action_contract_record": contract.to_mapping(),
                        "action_contract_digest": proposal.action_contract_digest,
                        "policy_digest": self.policy_digest,
                    }
                    raw = self.store.review_transition_command(ReviewTransitionCommand(command_request, actor, authorization))
                    return self._durable_result(raw, proposal_id=proposal_id, status=200)
                proposal = replace(proposal, review_state="cancelled", execution_state="unavailable", lifecycle_version=proposal.lifecycle_version + 1, updated_at=now)
                self.store.proposals[proposal.proposal_id] = proposal
                self._event(proposal, event_type="cancelled", actor=actor, lifecycle_version=proposal.lifecycle_version,
                            before_review_state="approved" if proposal.current_decision_id else "pending", after_review_state="cancelled",
                            before_execution_state="ready" if proposal.current_decision_id else "unavailable", after_execution_state="unavailable",
                            payload_version=proposal.payload_version, payload_hash=proposal.current_payload_hash,
                            decision_id=proposal.current_decision_id, decision_binding=proposal.current_decision_binding_hash,
                            transition_reason_code=checked_reason)
                result = self._projection_result(proposal, status=200)
                return self._store_transition(proposal, actor, operation, key_hash, request_hash, result)
            raise ExternalActionError("illegal_proposal_transition", "unknown lifecycle operation")

    def _append_edit(self, proposal: Proposal, actor: Actor, canonical: bytes | None, payload_hash: str | None, difference: Sequence[str]) -> Proposal:
        assert canonical is not None and payload_hash is not None
        now = self._recorded_at()
        version = proposal.payload_version + 1
        self.store.payload_versions[proposal.proposal_id].append(PayloadVersion(proposal.proposal_id, version, payload_hash, canonical, proposal.current_payload_hash, tuple(difference), now))
        next_proposal = replace(proposal, review_state="edited", current_payload_hash=payload_hash, payload_version=version,
                                lifecycle_version=proposal.lifecycle_version + 1, updated_at=now)
        self.store.proposals[proposal.proposal_id] = next_proposal
        self._event(next_proposal, event_type="edited", actor=actor, lifecycle_version=next_proposal.lifecycle_version,
                    before_review_state=proposal.review_state, after_review_state="edited", before_execution_state=proposal.execution_state,
                    after_execution_state="unavailable", payload_version=version, payload_hash=payload_hash)
        return next_proposal

    def _append_defer(self, proposal: Proposal, actor: Actor, reason: str | None) -> Proposal:
        now = self._recorded_at()
        next_proposal = replace(proposal, lifecycle_version=proposal.lifecycle_version + 1, updated_at=now)
        self.store.proposals[proposal.proposal_id] = next_proposal
        self._event(next_proposal, event_type="deferred", actor=actor, lifecycle_version=next_proposal.lifecycle_version,
                    before_review_state=proposal.review_state, after_review_state=proposal.review_state,
                    before_execution_state=proposal.execution_state, after_execution_state=proposal.execution_state,
                    payload_version=proposal.payload_version, payload_hash=proposal.current_payload_hash, transition_reason_code=reason)
        return next_proposal

    def _approval_deadline(self, proposal: Proposal, contract: ActionContract, execute_before: datetime | str | None) -> datetime:
        if execute_before is None:
            raise ExternalActionError("invalid_proposal", "execute_before is required")
        deadline = _utc(execute_before)
        now = self._recorded_at()
        upper = min(_utc(proposal.effective_review_expires_at), now + timedelta(seconds=contract.review_and_execution_limits.maximum_execution_window_seconds))
        if deadline <= now or deadline > upper:
            raise ExternalActionError("invalid_proposal", "execute_before is outside the allowed window")
        return deadline

    def _append_approval(self, proposal: Proposal, actor: Actor, contract: ActionContract, execute_before: datetime | str | None, reason: str | None) -> Proposal:
        deadline = self._approval_deadline(proposal, contract, execute_before)
        now = self._recorded_at()
        decision_id = self.id_factory()
        lifecycle = proposal.lifecycle_version + 1
        binding = {
            "deployment": proposal.deployment, "execution_epoch": proposal.execution_epoch, "effect_instance_key": proposal.effect_instance_key,
            "proposal_id": proposal.proposal_id, "decision_id": decision_id, "approved_lifecycle_version": lifecycle,
            "approved_payload_version": proposal.payload_version, "approved_payload_hash": proposal.current_payload_hash,
            "action_type": proposal.action_type, "target_ref": proposal.target_ref, "action_schema_digest": proposal.action_schema_digest,
            "action_contract_digest": proposal.action_contract_digest, "executor_contract_digest": proposal.executor_contract_digest,
            "payload_renderer_digest": proposal.payload_renderer_digest, "governed_tool_binding_digest": proposal.governed_tool_binding_digest,
            "provider_connection_contract_digest": proposal.provider_connection_contract_digest, "provider_capability_digest": proposal.provider_capability_digest,
            "provider_operation_id": proposal.provider_operation_id, "provider_operation_descriptor_digest": proposal.provider_operation_descriptor_digest,
            "policy_digest": self.policy_digest, "execute_before": format_timestamp(deadline),
        }
        binding_hash = decision_binding_hash(binding)
        decision = Decision(decision_id, proposal.proposal_id, "approve", lifecycle, proposal.payload_version, proposal.current_payload_hash,
                            actor.principal, actor.separation_subject, self.policy_digest, deadline, reason, binding_hash, now,
                            authentication_audit_id=actor.authentication_audit_id,
                            nomos_decision_id=actor.nomos_decision_id)
        self.store.decisions[proposal.proposal_id].append(decision)
        next_proposal = replace(proposal, review_state="approved", execution_state="ready", lifecycle_version=lifecycle,
                                current_decision_id=decision_id, current_decision_binding_hash=binding_hash, execute_before=deadline, updated_at=now)
        self.store.proposals[proposal.proposal_id] = next_proposal
        self._event(next_proposal, event_type="approved", actor=actor, lifecycle_version=lifecycle,
                    before_review_state=proposal.review_state, after_review_state="approved", before_execution_state=proposal.execution_state,
                    after_execution_state="ready", payload_version=proposal.payload_version, payload_hash=proposal.current_payload_hash,
                    decision_id=decision_id, decision_binding=binding_hash, transition_reason_code=reason)
        return next_proposal

    def _append_rejection(self, proposal: Proposal, actor: Actor, reason: str | None) -> Proposal:
        now = self._recorded_at()
        decision_id = self.id_factory()
        lifecycle = proposal.lifecycle_version + 1
        self.store.decisions[proposal.proposal_id].append(Decision(decision_id, proposal.proposal_id, "reject", lifecycle,
                                                                    proposal.payload_version, proposal.current_payload_hash, actor.principal,
                                                                    actor.separation_subject, self.policy_digest, reason_code=reason, created_at=now,
                                                                    authentication_audit_id=actor.authentication_audit_id,
                                                                    nomos_decision_id=actor.nomos_decision_id))
        next_proposal = replace(proposal, review_state="rejected", execution_state="unavailable", lifecycle_version=lifecycle,
                                current_decision_id=decision_id, updated_at=now)
        self.store.proposals[proposal.proposal_id] = next_proposal
        self._event(next_proposal, event_type="rejected", actor=actor, lifecycle_version=lifecycle,
                    before_review_state=proposal.review_state, after_review_state="rejected", before_execution_state=proposal.execution_state,
                    after_execution_state="unavailable", payload_version=proposal.payload_version, payload_hash=proposal.current_payload_hash,
                    decision_id=decision_id, transition_reason_code=reason)
        return next_proposal

    def edit(self, proposal_id: str, *, actor: Actor | Mapping[str, Any] | str, idempotency_key: str, replacement_payload: Any, action_contract: ActionContract | Mapping[str, Any], if_match: str | None = None, expected_lifecycle_version: int | None = None, expected_payload_version: int | None = None, expected_payload_hash: str | None = None) -> ServiceResult:
        return self._transition(proposal_id, actor=actor, idempotency_key=idempotency_key, operation="edit", replacement_payload=replacement_payload, action_contract=action_contract, if_match=if_match, expected_lifecycle_version=expected_lifecycle_version, expected_payload_version=expected_payload_version, expected_payload_hash=expected_payload_hash)

    def decide(self, proposal_id: str, *, actor: Actor | Mapping[str, Any] | str, idempotency_key: str, verdict: str, action_contract: ActionContract | Mapping[str, Any], execute_before: datetime | str | None = None, reason_code: str | None = None, replacement_payload: Any | None = None, if_match: str | None = None, expected_lifecycle_version: int | None = None, expected_payload_version: int | None = None, expected_payload_hash: str | None = None) -> ServiceResult:
        operation = "approve_with_edit" if verdict == "approve_with_edit" else verdict
        if operation not in {"approve", "approve_with_edit", "reject", "defer"}:
            raise ExternalActionError("invalid_proposal", "invalid decision verdict")
        return self._transition(proposal_id, actor=actor, idempotency_key=idempotency_key, operation=operation, replacement_payload=replacement_payload, action_contract=action_contract, execute_before=execute_before, reason_code=reason_code, if_match=if_match, expected_lifecycle_version=expected_lifecycle_version, expected_payload_version=expected_payload_version, expected_payload_hash=expected_payload_hash)

    def approve(self, proposal_id: str, **kwargs: Any) -> ServiceResult:
        return self.decide(proposal_id, verdict="approve", **kwargs)

    def approve_with_edit(self, proposal_id: str, **kwargs: Any) -> ServiceResult:
        return self.decide(proposal_id, verdict="approve_with_edit", **kwargs)

    def reject(self, proposal_id: str, **kwargs: Any) -> ServiceResult:
        return self.decide(proposal_id, verdict="reject", **kwargs)

    def defer(self, proposal_id: str, **kwargs: Any) -> ServiceResult:
        return self.decide(proposal_id, verdict="defer", **kwargs)

    def cancel(self, proposal_id: str, *, actor: Actor | Mapping[str, Any] | str, idempotency_key: str, action_contract: ActionContract | Mapping[str, Any], reason_code: str, expected_decision_id: str | None = None, expected_decision_binding_hash: str | None = None, if_match: str | None = None, expected_lifecycle_version: int | None = None, expected_payload_version: int | None = None, expected_payload_hash: str | None = None) -> ServiceResult:
        return self._transition(proposal_id, actor=actor, idempotency_key=idempotency_key, operation="cancel", action_contract=action_contract, reason_code=reason_code, expected_decision_id=expected_decision_id, expected_decision_binding_hash=expected_decision_binding_hash, if_match=if_match, expected_lifecycle_version=expected_lifecycle_version, expected_payload_version=expected_payload_version, expected_payload_hash=expected_payload_hash)

    # ----- worker claim and outcome --------------------------------------

    def claim(self, proposal_id: str, *, actor: Actor | Mapping[str, Any] | str, idempotency_key: str, decision_id: str, approved_lifecycle_version: int, approved_payload_version: int, approved_payload_hash: str, supported_action_schema_digest: str, supported_action_contract_digest: str, supported_executor_contract_digest: str, supported_payload_renderer_digest: str, supported_governed_tool_binding_digest: str, supported_provider_connection_contract_digest: str, supported_provider_capability_digest: str, supported_provider_operation_id: str, supported_provider_operation_descriptor_digest: str, if_match: str | None = None, handoff_protected_b64: str | None = None, handoff_payload_b64: str | None = None, handoff_signature_b64: str | None = None, handoff_receipt_digest: str | None = None, handoff_receipt: Mapping[str, Any] | None = None) -> ServiceResult:
        actor = self._actor(actor)
        if any(value is not None for value in (handoff_protected_b64, handoff_payload_b64, handoff_signature_b64, handoff_receipt_digest, handoff_receipt)):
            raise ExternalActionError("invalid_proposal", "claim handoff is server generated")
        with self.store.lock:
            if self._is_durable():
                self._refresh_durable()
            proposal = self._proposal(proposal_id)
            key_hash = self._idempotency_hash(idempotency_key)
            if self._is_durable() and if_match is None:
                raise ExternalActionError("proposal_precondition_required", "If-Match is required", status=428)
            if if_match is not None and self._etag_expectations(if_match)[0] is None:
                raise ExternalActionError("proposal_precondition_required", "If-Match is invalid", status=428)
            support = {
                "worker_separation_subject": actor.separation_subject, "decision_id": decision_id,
                "approved_lifecycle_version": approved_lifecycle_version, "approved_payload_version": approved_payload_version,
                "approved_payload_hash": approved_payload_hash, "supported_action_schema_digest": supported_action_schema_digest,
                "supported_action_contract_digest": supported_action_contract_digest, "supported_executor_contract_digest": supported_executor_contract_digest,
                "supported_payload_renderer_digest": supported_payload_renderer_digest, "supported_governed_tool_binding_digest": supported_governed_tool_binding_digest,
                "supported_provider_connection_contract_digest": supported_provider_connection_contract_digest,
                "supported_provider_capability_digest": supported_provider_capability_digest, "supported_provider_operation_id": supported_provider_operation_id,
                "supported_provider_operation_descriptor_digest": supported_provider_operation_descriptor_digest,
            }
            # The durable claim RPC binds the transport ETag to the approved
            # lifecycle/payload tuple.  Include the raw canonical header in
            # this digest so Python and SQL reject a replay that changes the
            # conditional write fence, even if all support fields match.
            support["if_match"] = if_match
            request_hash = transition_request_hash(support)
            actor = self._authorize(
                "external_action.execute",
                actor,
                context={"deployment": proposal.deployment, "request_hash": request_hash, "surface": "external_actions"},
            )
            authorization = self._authorization(actor)
            if self._is_durable():
                existing = self.store.get_execution(proposal_id)
                if existing is not None:
                    if (
                        existing.worker_separation_subject != actor.separation_subject
                        or existing.claim_idempotency_key_hash != key_hash
                        or existing.request_hash != request_hash
                    ):
                        raise ExternalActionError("execution_already_claimed", "execution is already claimed")
                    claim_event = next(
                        (
                            event
                            for event in self.store.get_events(proposal.effect_instance_key)
                            if event.event_type == "execution_claimed"
                            and event.execution_id == existing.execution_id
                        ),
                        None,
                    )
                    decision_event = next(
                        (
                            event
                            for event in reversed(self.store.get_events(proposal.effect_instance_key))
                            if event.event_type == "approved"
                            and event.decision_id == existing.decision_id
                        ),
                        None,
                    )
                    if claim_event is None or decision_event is None:
                        raise ExternalActionError(
                            "review_authority_unavailable",
                            "stored claim audit chain is unavailable",
                            status=503,
                        )
                    replay_request = {
                        "deployment": proposal.deployment,
                        "proposal_id": proposal_id,
                        "decision_id": existing.decision_id,
                        "worker_principal": actor.principal,
                        "worker_separation_subject": actor.separation_subject,
                        "idempotency_key_hash": key_hash,
                        "request_hash": request_hash,
                        "execution_id": existing.execution_id,
                        "approved_lifecycle_version": existing.lifecycle_version - 1,
                        "approved_payload_version": existing.approved_payload_version,
                        "approved_payload_hash": existing.approved_payload_hash,
                        **{
                            field: support[field]
                            for field in _EXECUTION_SUPPORT_FIELDS
                        },
                        "execution_epoch_lease_digest": existing.execution_epoch_lease_digest,
                        "execution_binding_hash": existing.execution_binding_hash,
                        "provider_idempotency_key": existing.provider_idempotency_key,
                        "provider_idempotency_key_digest": provider_idempotency_key_digest({
                            "deployment": proposal.deployment,
                            "effect_instance_key": proposal.effect_instance_key,
                            "approved_payload_hash": existing.approved_payload_hash,
                            "action_contract_digest": existing.action_contract_digest,
                            "provider_connection_contract_digest": existing.provider_connection_contract_digest,
                            "provider_operation_id": existing.provider_operation_id,
                            "provider_operation_descriptor_digest": existing.provider_operation_descriptor_digest,
                        }),
                        "policy_digest": existing.policy_digest,
                        "handoff_protected_b64": existing.handoff_protected_b64,
                        "handoff_payload_b64": existing.handoff_payload_b64,
                        "handoff_signature_b64": existing.handoff_signature_b64,
                        "okg_claim_handoff_receipt_digest": existing.handoff_receipt_digest,
                        "event_hash": claim_event.event_hash,
                        "previous_event_hash": claim_event.previous_event_hash,
                        "claimed_at": format_timestamp(_utc(claim_event.recorded_at)),
                        "decision_event_hash": decision_event.event_hash,
                        "claim_event_hash": claim_event.event_hash,
                        "event_chain_head_hash": claim_event.event_hash,
                        "claim_lifecycle_version": claim_event.lifecycle_version,
                        "event_sequence": claim_event.sequence,
                        "if_match": if_match,
                    }
                    if not all(replay_request.get(field) for field in (
                        "handoff_protected_b64", "handoff_payload_b64", "handoff_signature_b64",
                        "okg_claim_handoff_receipt_digest",
                    )):
                        raise ExternalActionError("review_authority_unavailable", "stored claim handoff is unavailable", status=503)
                    raw = self.store.claim_command(ClaimCommand(replay_request, actor, authorization))
                    return self._durable_claim_result(raw, status=200)
                if (
                    self._is_durable()
                    and (
                        callable(getattr(self.store, "restore_fence", None))
                        or hasattr(self.store, "restore_fences")
                    )
                    and self._persisted_service_mode(proposal.deployment) == "restore_only"
                ):
                    raise ExternalActionError(
                        "external_action_restore_quarantined",
                        "external action lifecycle is quarantined for restore",
                        status=409,
                    )
                # Record an elapsed execution deadline before any other check
                # can refuse the claim, and commit it with the refusal: a
                # plain raise here rolls the approval_expired event back
                # with the service transaction (issue #1925).
                if (
                    proposal.review_state == "approved"
                    and proposal.execution_state == "ready"
                    and proposal.execute_before is not None
                    and self._recorded_at() >= _utc(proposal.execute_before)
                ):
                    proposal = self._materialize_expiry(proposal)
                    if proposal.review_state == "expired":
                        raise _committed_claim_expiry()
                if proposal.review_state == "expired":
                    raise ExternalActionError("proposal_expired", "approval has expired", status=410)
                self._check_match(proposal, if_match=if_match, expected_lifecycle_version=None,
                                  expected_payload_version=None, expected_payload_hash=None)
                if proposal.review_state != "approved" or proposal.execution_state != "ready":
                    raise ExternalActionError("illegal_proposal_transition", "proposal is not ready to claim")
                if proposal.execute_before is None or self._recorded_at() >= _utc(proposal.execute_before):
                    raise ExternalActionError("proposal_expired", "execution deadline reached", status=410)
                decision = self.store.get_decision(proposal_id, decision_id)
                if decision is None or decision.verdict not in {"approve", "approved"} or decision.decision_binding_hash != proposal.current_decision_binding_hash:
                    raise ExternalActionError("stale_payload_binding", "decision binding is stale")
                if approved_lifecycle_version != proposal.lifecycle_version or approved_payload_version != proposal.payload_version:
                    raise ExternalActionError("stale_proposal_lifecycle", "approval lifecycle is stale")
                if approved_payload_hash != proposal.current_payload_hash:
                    raise ExternalActionError("stale_payload_binding", "approved payload is stale")
                actual_support = {
                    "supported_action_schema_digest": proposal.action_schema_digest,
                    "supported_action_contract_digest": proposal.action_contract_digest,
                    "supported_executor_contract_digest": proposal.executor_contract_digest,
                    "supported_payload_renderer_digest": proposal.payload_renderer_digest,
                    "supported_governed_tool_binding_digest": proposal.governed_tool_binding_digest,
                    "supported_provider_connection_contract_digest": proposal.provider_connection_contract_digest,
                    "supported_provider_capability_digest": proposal.provider_capability_digest,
                    "supported_provider_operation_id": proposal.provider_operation_id,
                    "supported_provider_operation_descriptor_digest": proposal.provider_operation_descriptor_digest,
                }
                if any(support[key] != expected for key, expected in actual_support.items()):
                    raise ExternalActionError("stale_payload_binding", "worker effect binding is not supported")
                execution_id = self.id_factory()
                lease_digest = self.store.execution_epoch_lease_digest
                if not isinstance(lease_digest, str) or not lease_digest:
                    raise ExternalActionError("execution_epoch_authority_unavailable", "execution epoch lease is unavailable", status=503)
                execution_binding = {
                    "deployment": proposal.deployment, "execution_epoch": proposal.execution_epoch,
                    "execution_epoch_lease_digest": lease_digest, "effect_instance_key": proposal.effect_instance_key,
                    "proposal_id": proposal.proposal_id, "decision_id": decision_id, "execution_id": execution_id,
                    "worker_separation_subject": actor.separation_subject,
                    "approved_lifecycle_version": proposal.lifecycle_version,
                    "approved_payload_version": approved_payload_version, "approved_payload_hash": approved_payload_hash,
                    "action_type": proposal.action_type, "target_ref": proposal.target_ref,
                    "action_schema_digest": proposal.action_schema_digest, "action_contract_digest": proposal.action_contract_digest,
                    "executor_contract_digest": proposal.executor_contract_digest, "payload_renderer_digest": proposal.payload_renderer_digest,
                    "governed_tool_binding_digest": proposal.governed_tool_binding_digest,
                    "provider_connection_contract_digest": proposal.provider_connection_contract_digest,
                    "provider_capability_digest": proposal.provider_capability_digest,
                    "provider_operation_id": proposal.provider_operation_id,
                    "provider_operation_descriptor_digest": proposal.provider_operation_descriptor_digest,
                    "policy_digest": self.policy_digest,
                }
                execution_hash = execution_binding_hash(execution_binding)
                provider_binding = {
                    "deployment": proposal.deployment, "effect_instance_key": proposal.effect_instance_key,
                    "approved_payload_hash": approved_payload_hash, "action_contract_digest": proposal.action_contract_digest,
                    "provider_connection_contract_digest": proposal.provider_connection_contract_digest,
                    "provider_operation_id": proposal.provider_operation_id,
                    "provider_operation_descriptor_digest": proposal.provider_operation_descriptor_digest,
                }
                provider_key = provider_idempotency_key(provider_binding)
                provider_key_digest = provider_idempotency_key_digest(provider_binding)
                prepare_claim = getattr(self.store, "prepare_claim", None)
                if not callable(prepare_claim):
                    raise ExternalActionError(
                        "review_authority_unavailable",
                        "claim preparation authority is unavailable",
                        status=503,
                    )
                preparation_request = {
                    "deployment": proposal.deployment,
                    "proposal_id": proposal_id,
                    "decision_id": decision_id,
                    "execution_id": execution_id,
                    "worker_principal": actor.principal,
                    "worker_separation_subject": actor.separation_subject,
                    "approved_lifecycle_version": approved_lifecycle_version,
                    "approved_payload_version": approved_payload_version,
                    "approved_payload_hash": approved_payload_hash,
                    "execution_epoch_lease_digest": lease_digest,
                    "execution_binding_hash": execution_hash,
                    "request_hash": request_hash,
                    "authentication_audit_id": actor.authentication_audit_id,
                    "nomos_decision_id": actor.nomos_decision_id,
                    "policy_digest": self.policy_digest,
                }
                try:
                    preparation = prepare_claim(preparation_request)
                except ExternalActionStoreError as exc:
                    # The deadline passed between the check above and the
                    # database's own check.  The RPC recorded the expiry and
                    # returned a closed refusal, so commit that record.
                    if exc.code == "proposal_expired":
                        raise _committed_claim_expiry() from exc
                    raise ExternalActionError(
                        "review_authority_unavailable",
                        "claim preparation authority is unavailable",
                        status=503,
                    ) from exc
                except Exception as exc:
                    raise ExternalActionError(
                        "review_authority_unavailable",
                        "claim preparation authority is unavailable",
                        status=503,
                    ) from exc
                if not isinstance(preparation, Mapping) or set(preparation) != _CLAIM_PREPARATION_FIELDS:
                    raise ExternalActionError(
                        "review_authority_unavailable",
                        "claim preparation authority returned an invalid projection",
                        status=503,
                    )
                try:
                    claimed_at = parse_timestamp(preparation["claimed_at"])
                    for field in ("decision_event_hash", "claim_event_hash", "event_chain_head_hash"):
                        validate_digest(preparation[field], field=field)
                    previous_event_hash = preparation["previous_event_hash"]
                    if previous_event_hash is not None:
                        validate_digest(previous_event_hash, field="previous_event_hash")
                except (KeyError, TypeError, ValueError, SigningError, ActionContractError) as exc:
                    raise ExternalActionError(
                        "review_authority_unavailable",
                        "claim preparation authority returned an invalid projection",
                        status=503,
                    ) from exc
                if (
                    type(preparation["claim_lifecycle_version"]) is not int
                    or preparation["claim_lifecycle_version"] != proposal.lifecycle_version + 1
                    or type(preparation["event_sequence"]) is not int
                    or preparation["event_sequence"] <= 0
                    or preparation["event_chain_head_hash"] != preparation["claim_event_hash"]
                    or claimed_at >= _utc(proposal.execute_before)
                ):
                    raise ExternalActionError(
                        "review_authority_unavailable",
                        "claim preparation authority returned an inconsistent projection",
                        status=503,
                    )
                handoff_payload = {
                    "deployment": proposal.deployment,
                    "execution_epoch": proposal.execution_epoch,
                    "execution_epoch_lease_digest": lease_digest,
                    "effect_instance_key": proposal.effect_instance_key,
                    "submission_content_hash": proposal.submission_content_hash,
                    "proposal_id": proposal.proposal_id,
                    "proposal_request_hash": proposal.proposal_request_hash,
                    "original_payload_hash": proposal.original_payload_hash,
                    "proposer_separation_subject": proposal.proposer_separation_subject,
                    "approved_lifecycle_version": proposal.lifecycle_version,
                    "approved_payload_version": approved_payload_version,
                    "approved_payload_hash": approved_payload_hash,
                    "action_contract_digest": proposal.action_contract_digest,
                    "policy_digest": self.policy_digest,
                    "decision_id": decision_id,
                    "decision_binding_hash": decision.decision_binding_hash,
                    "decision_event_hash": preparation["decision_event_hash"],
                    "approver_separation_subject": decision.approver_separation_subject,
                    "approver_authentication_audit_ref": decision.authentication_audit_id,
                    "approver_nomos_decision_ref": decision.nomos_decision_id,
                    "execution_id": execution_id,
                    "execution_binding_hash": execution_hash,
                    "claim_event_hash": preparation["claim_event_hash"],
                    "worker_separation_subject": actor.separation_subject,
                    "worker_authentication_audit_ref": actor.authentication_audit_id,
                    "worker_nomos_decision_ref": actor.nomos_decision_id,
                    "event_chain_head_hash": preparation["event_chain_head_hash"],
                    "claimed_at": preparation["claimed_at"],
                    "execute_before": format_timestamp(_utc(proposal.execute_before)),
                }
                handoff = self._handoff_envelope(handoff_payload)
                command_request = {
                    "deployment": proposal.deployment,
                    "proposal_id": proposal_id,
                    "decision_id": decision_id,
                    "worker_principal": actor.principal,
                    "worker_separation_subject": actor.separation_subject,
                    "idempotency_key_hash": key_hash,
                    "request_hash": request_hash,
                    "execution_id": execution_id,
                    "approved_lifecycle_version": approved_lifecycle_version,
                    "approved_payload_version": approved_payload_version,
                    "approved_payload_hash": approved_payload_hash,
                    **{
                        field: support[field]
                        for field in _EXECUTION_SUPPORT_FIELDS
                    },
                    "execution_epoch_lease_digest": lease_digest,
                    "execution_binding_hash": execution_hash,
                    "provider_idempotency_key": provider_key,
                    "provider_idempotency_key_digest": provider_key_digest,
                    "policy_digest": self.policy_digest,
                    "handoff_protected_b64": handoff["protected_b64"],
                    "handoff_payload_b64": handoff["payload_b64"],
                    "handoff_signature_b64": handoff["signature_b64"],
                    "okg_claim_handoff_receipt_digest": handoff["envelope_digest"],
                    "claimed_at": preparation["claimed_at"],
                    "event_hash": preparation["claim_event_hash"],
                    "previous_event_hash": previous_event_hash,
                    "decision_event_hash": preparation["decision_event_hash"],
                    "claim_event_hash": preparation["claim_event_hash"],
                    "event_chain_head_hash": preparation["event_chain_head_hash"],
                    "claim_lifecycle_version": preparation["claim_lifecycle_version"],
                    "event_sequence": preparation["event_sequence"],
                    # SQL binds this raw canonical ETag to the approved
                    # lifecycle/payload tuple before recording the claim.
                    "if_match": if_match,
                }
                try:
                    raw = self.store.claim_command(ClaimCommand(command_request, actor, authorization))
                except ExternalActionStoreError as exc:
                    if exc.code == "proposal_expired":
                        raise _committed_claim_expiry() from exc
                    raise
                return self._durable_claim_result(raw, status=201)
            existing_claim = self.store.get_execution(proposal_id)
            if existing_claim is not None:
                if existing_claim.worker_separation_subject == actor.separation_subject and existing_claim.claim_idempotency_key_hash == key_hash:
                    expected = self._claim_idempotency.get((proposal_id, actor.separation_subject, key_hash))
                    if expected is not None and expected[1] != request_hash:
                        raise ExternalActionError("idempotency_conflict", "claim key was reused with a different request")
                    return _result(existing_claim, status=200, replay=True)
                raise ExternalActionError("execution_already_claimed", "execution is already claimed")
            previous = self._claim_idempotency.get((proposal_id, actor.separation_subject, key_hash))
            if previous is not None and previous[1] != request_hash:
                raise ExternalActionError("idempotency_conflict", "claim key was reused with a different request")
            proposal = self._materialize_expiry(proposal)
            self._check_match(proposal, if_match=if_match, expected_lifecycle_version=None,
                              expected_payload_version=None, expected_payload_hash=None)
            if proposal.review_state != "approved" or proposal.execution_state != "ready":
                if proposal.review_state == "expired":
                    raise ExternalActionError("proposal_expired", "approval has expired", status=410)
                raise ExternalActionError("illegal_proposal_transition", "proposal is not ready to claim")
            if proposal.execute_before is None or self._recorded_at() >= _utc(proposal.execute_before):
                raise ExternalActionError("proposal_expired", "execution deadline reached", status=410)
            decision = self.store.get_decision(proposal_id, decision_id)
            if decision is None or decision.verdict != "approve" or decision.decision_binding_hash != proposal.current_decision_binding_hash:
                raise ExternalActionError("stale_payload_binding", "decision binding is stale")
            if approved_lifecycle_version != proposal.lifecycle_version or approved_payload_version != proposal.payload_version:
                raise ExternalActionError("stale_proposal_lifecycle", "approval lifecycle is stale")
            if approved_payload_hash != proposal.current_payload_hash:
                raise ExternalActionError("stale_payload_binding", "approved payload is stale")
            actual_support = {
                "supported_action_schema_digest": proposal.action_schema_digest, "supported_action_contract_digest": proposal.action_contract_digest,
                "supported_executor_contract_digest": proposal.executor_contract_digest, "supported_payload_renderer_digest": proposal.payload_renderer_digest,
                "supported_governed_tool_binding_digest": proposal.governed_tool_binding_digest,
                "supported_provider_connection_contract_digest": proposal.provider_connection_contract_digest,
                "supported_provider_capability_digest": proposal.provider_capability_digest, "supported_provider_operation_id": proposal.provider_operation_id,
                "supported_provider_operation_descriptor_digest": proposal.provider_operation_descriptor_digest,
            }
            if any(support[key] != expected for key, expected in actual_support.items()):
                raise ExternalActionError("stale_payload_binding", "worker effect binding is not supported")
            payload = self._payload(proposal_id)
            execution_id = self.id_factory()
            claimed_at = self._recorded_at()
            lease_digest = self.store.execution_epoch_lease_digest
            execution_binding = {
                "deployment": proposal.deployment, "execution_epoch": proposal.execution_epoch, "execution_epoch_lease_digest": lease_digest,
                "effect_instance_key": proposal.effect_instance_key, "proposal_id": proposal.proposal_id, "decision_id": decision_id,
                "execution_id": execution_id, "worker_separation_subject": actor.separation_subject,
                "approved_lifecycle_version": proposal.lifecycle_version, "approved_payload_version": payload.version,
                "approved_payload_hash": payload.payload_hash, "action_type": proposal.action_type, "target_ref": proposal.target_ref,
                "action_schema_digest": proposal.action_schema_digest, "action_contract_digest": proposal.action_contract_digest,
                "executor_contract_digest": proposal.executor_contract_digest, "payload_renderer_digest": proposal.payload_renderer_digest,
                "governed_tool_binding_digest": proposal.governed_tool_binding_digest,
                "provider_connection_contract_digest": proposal.provider_connection_contract_digest,
                "provider_capability_digest": proposal.provider_capability_digest, "provider_operation_id": proposal.provider_operation_id,
                "provider_operation_descriptor_digest": proposal.provider_operation_descriptor_digest, "policy_digest": self.policy_digest,
            }
            execution_hash = execution_binding_hash(execution_binding)
            provider_key = provider_idempotency_key({
                "deployment": proposal.deployment, "effect_instance_key": proposal.effect_instance_key,
                "approved_payload_hash": payload.payload_hash, "action_contract_digest": proposal.action_contract_digest,
                "provider_connection_contract_digest": proposal.provider_connection_contract_digest,
                "provider_operation_id": proposal.provider_operation_id, "provider_operation_descriptor_digest": proposal.provider_operation_descriptor_digest,
            })
            execution = ExecutionClaim(execution_id, proposal_id, decision_id, actor.principal, actor.separation_subject,
                                       proposal.lifecycle_version + 1, payload.version, payload.payload_hash, payload.canonical_payload,
                                       proposal.execution_epoch, lease_digest, proposal.action_type, proposal.target_ref, proposal.action_schema_digest,
                                       proposal.action_contract_digest, proposal.executor_contract_digest, proposal.payload_renderer_digest,
                                       proposal.governed_tool_binding_digest, proposal.provider_connection_contract_digest,
                                       proposal.provider_capability_digest, proposal.provider_operation_id,
                                       proposal.provider_operation_descriptor_digest, provider_key, self.policy_digest,
                                       proposal.execute_before, claimed_at, decision.decision_binding_hash or "", execution_hash, key_hash,
                                       handoff_receipt=handoff_receipt,
                                       authentication_audit_id=actor.authentication_audit_id,
                                       nomos_decision_id=actor.nomos_decision_id,
                                       request_hash=request_hash,
                                       handoff_protected_b64=handoff_protected_b64,
                                       handoff_payload_b64=handoff_payload_b64,
                                       handoff_signature_b64=handoff_signature_b64,
                                       handoff_receipt_digest=handoff_receipt_digest)
            next_proposal = replace(proposal, execution_state="claimed", lifecycle_version=proposal.lifecycle_version + 1, updated_at=claimed_at)
            self.store.proposals[proposal_id] = next_proposal
            self.store.executions[proposal_id] = execution
            self._claim_idempotency[(proposal_id, actor.separation_subject, key_hash)] = (execution_id, request_hash)
            self._event(next_proposal, event_type="execution_claimed", actor=actor, lifecycle_version=next_proposal.lifecycle_version,
                        before_review_state=proposal.review_state, after_review_state=proposal.review_state,
                        before_execution_state="ready", after_execution_state="claimed", payload_version=payload.version,
                        payload_hash=payload.payload_hash, decision_id=decision_id, decision_binding=decision.decision_binding_hash,
                        execution_id=execution_id, execution_binding=execution_hash)
            return _result(execution, status=201)

    claim_execution = claim

    def report_outcome(self, execution_id: str, *, actor: Actor | Mapping[str, Any] | str, idempotency_key: str, effect_instance_key_value: str | None = None, outcome: str, outcome_reason_code: str, provider_receipt_digest: str | None, recovery_journal_digest: str, owner_unavailability_attestation_digest: str | None = None, if_match: str | None = None, reconciler: bool | None = None, proposal_id: str | None = None) -> ServiceResult:
        actor = self._actor(actor)
        with self.store.lock:
            execution = next((item for item in self.store.executions.values() if item.execution_id == execution_id), None)
            if execution is None:
                raise ExternalActionError("proposal_not_found", "execution not found", status=404)
            if proposal_id is not None and execution.proposal_id != proposal_id:
                # The URL contains both identifiers.  Do not let a valid
                # execution id be rebound through a different proposal path.
                raise ExternalActionError("proposal_not_found", "execution not found", status=404)
            proposal = self._proposal(execution.proposal_id)
            # Reconciler status is a fact of the retained claim owner.  A
            # caller-provided boolean must never select the privileged path.
            derived_reconciler = actor.separation_subject != execution.worker_separation_subject
            del reconciler
            reconciler = derived_reconciler
            key_hash = self._idempotency_hash(idempotency_key)
            expected_lifecycle_version, expected_payload_version, expected_payload_hash = self._etag_expectations(if_match)
            if self._is_durable() and if_match is None:
                raise ExternalActionError("proposal_precondition_required", "If-Match is required", status=428)
            if if_match is not None and expected_lifecycle_version is None:
                raise ExternalActionError("proposal_precondition_required", "If-Match is invalid", status=428)
            request_fields = {
                "execution_id": execution_id, "effect_instance_key": proposal.effect_instance_key,
                "worker_or_reconciler_separation_subject": actor.separation_subject, "outcome": outcome,
                "outcome_reason_code": outcome_reason_code, "provider_receipt_digest": provider_receipt_digest,
                "recovery_journal_digest": recovery_journal_digest, "owner_unavailability_attestation_digest": owner_unavailability_attestation_digest,
                "if_match": if_match,
            }
            req_hash = outcome_request_hash(request_fields)
            try:
                actor = self._authorize(
                    "external_action.reconcile" if reconciler else "external_action.report",
                    actor,
                    context={"deployment": proposal.deployment, "request_hash": req_hash, "surface": "external_actions"},
                )
            except ExternalActionError as exc:
                # A non-owner cannot learn whether an execution has an
                # outcome, is eligible for reconciliation, or merely has an
                # invalid semantic report.  Preserve the owner's explicit
                # authorization error, but make the reconciler path opaque.
                if reconciler and exc.code == "unauthorized":
                    raise ExternalActionError("proposal_not_found", "execution not found", status=404) from exc
                raise
            # Only after owner/reconciler authorization may semantic report
            # errors be returned.  This ordering prevents an unauthorized
            # non-owner from using outcome vocabulary or effect bindings as
            # an oracle about the retained execution.
            if reconciler and (actor.separation_subject == execution.worker_separation_subject or outcome != "indeterminate"):
                raise ExternalActionError("illegal_proposal_transition", "reconciler may only record indeterminate")
            if effect_instance_key_value is not None and effect_instance_key_value != proposal.effect_instance_key:
                raise ExternalActionError("stale_payload_binding", "effect binding is stale")
            authorization = self._authorization(actor)
            # Apply the immutable stored contract before crossing the durable
            # RPC boundary.  This protects the SQL path from arbitrary reason
            # text, receipt combinations, and forged reconciler states.
            if not isinstance(outcome, str) or outcome not in {"succeeded", "failed", "indeterminate"}:
                raise ExternalActionError("invalid_proposal", "invalid outcome")
            try:
                validate_digest(recovery_journal_digest, field="recovery_journal_digest")
                if provider_receipt_digest is not None:
                    validate_digest(provider_receipt_digest, field="provider_receipt_digest")
                if owner_unavailability_attestation_digest is not None:
                    validate_digest(owner_unavailability_attestation_digest, field="owner_unavailability_attestation_digest")
            except ActionContractError as exc:
                raise ExternalActionError("invalid_proposal", "outcome digest is invalid") from exc
            existing_outcome = self.store.get_outcome(execution_id)
            if existing_outcome is not None:
                if (
                    getattr(existing_outcome, "worker_or_reconciler_separation_subject", None)
                    != actor.separation_subject
                    or getattr(existing_outcome, "outcome_request_hash", None) != req_hash
                ):
                    raise ExternalActionError("outcome_already_recorded", "a terminal outcome already exists")
                if self._is_durable():
                    replay_request = {
                        **request_fields,
                        "deployment": proposal.deployment,
                        "execution_id": execution_id,
                        "idempotency_key_hash": key_hash,
                        "outcome_request_hash": req_hash,
                        "reconciler": derived_reconciler,
                        "policy_digest": self.policy_digest,
                        "expected_lifecycle_version": expected_lifecycle_version,
                        "expected_payload_version": expected_payload_version,
                        "expected_payload_hash": expected_payload_hash,
                    }
                    raw = self.store.outcome_command(OutcomeCommand(replay_request, actor, authorization))
                    return self._durable_record_result(raw, existing_outcome, status=200)
                return _result(existing_outcome, status=200, replay=True)
            if self._is_durable() or if_match is not None:
                self._check_match(
                    proposal,
                    if_match=if_match,
                    expected_lifecycle_version=expected_lifecycle_version,
                    expected_payload_version=expected_payload_version,
                    expected_payload_hash=expected_payload_hash,
                )
            contract = self._stored_contract(proposal)
            if contract is None:
                raise ExternalActionError("review_authority_unavailable", "stored action contract is unavailable", status=503)
            rule = contract.outcome_reason_rules.get(outcome_reason_code)
            if rule is None or outcome not in rule["allowed_outcomes"]:
                raise ExternalActionError("invalid_proposal", "outcome reason does not allow this outcome")
            receipt_required = rule["provider_receipt"] == "required"
            if receipt_required != (provider_receipt_digest is not None):
                raise ExternalActionError("invalid_proposal", "provider receipt presence is not allowed")
            if outcome == "succeeded" and provider_receipt_digest is None:
                raise ExternalActionError("invalid_proposal", "success requires a provider receipt")
            if reconciler and (outcome != "indeterminate" or provider_receipt_digest is not None):
                raise ExternalActionError("invalid_proposal", "reconciler cannot supply a provider receipt")
            if not reconciler and owner_unavailability_attestation_digest is not None:
                raise ExternalActionError("invalid_proposal", "claim owner reports cannot use an unavailability attestation")
            if reconciler:
                if not owner_unavailability_attestation_digest:
                    raise ExternalActionError("invalid_proposal", "reconciler attestation is required")
                self._require_claim_owner_unavailable(
                    execution=execution,
                    proposal=proposal,
                    reconciler=actor,
                    attestation_digest=owner_unavailability_attestation_digest,
                )
            if self._is_durable():
                command_request = {
                    **request_fields,
                    "deployment": proposal.deployment,
                    "execution_id": execution_id,
                    "idempotency_key_hash": key_hash,
                    "outcome_request_hash": req_hash,
                    "reconciler": derived_reconciler,
                    "policy_digest": self.policy_digest,
                    "expected_lifecycle_version": expected_lifecycle_version,
                    "expected_payload_version": expected_payload_version,
                    "expected_payload_hash": expected_payload_hash,
                }
                raw = self.store.outcome_command(OutcomeCommand(command_request, actor, authorization))
                return self._durable_record_result(raw, self.store.get_outcome(execution_id), status=201)
            idem = self._outcome_idempotency.get((execution_id, actor.separation_subject, key_hash))
            if idem is not None:
                if idem[1] != req_hash:
                    raise ExternalActionError("idempotency_conflict", "outcome key was reused with a different request")
                existing = self.store.get_outcome(execution_id)
                if existing is not None:
                    return _result(existing, status=200, replay=True)
            existing = self.store.get_outcome(execution_id)
            if existing is not None:
                if actor.separation_subject == execution.worker_separation_subject and existing.outcome_request_hash == req_hash:
                    return _result(existing, status=200, replay=True)
                raise ExternalActionError("outcome_already_recorded", "a terminal outcome already exists")
            now = self._recorded_at()
            recorded = Outcome(execution_id, proposal.effect_instance_key, actor.separation_subject, outcome, outcome_reason_code,
                               provider_receipt_digest, recovery_journal_digest, owner_unavailability_attestation_digest, req_hash, now,
                               authentication_audit_id=actor.authentication_audit_id,
                               nomos_decision_id=actor.nomos_decision_id)
            self.store.outcomes[execution_id] = recorded
            next_proposal = replace(proposal, execution_state=outcome, lifecycle_version=proposal.lifecycle_version + 1, updated_at=now)
            self.store.proposals[proposal.proposal_id] = next_proposal
            self._outcome_idempotency[(execution_id, actor.separation_subject, key_hash)] = (execution_id, req_hash)
            self._event(next_proposal, event_type="outcome_reported", actor=actor, lifecycle_version=next_proposal.lifecycle_version,
                        before_review_state=proposal.review_state, after_review_state=proposal.review_state,
                        before_execution_state=proposal.execution_state, after_execution_state=outcome,
                        payload_version=proposal.payload_version, payload_hash=proposal.current_payload_hash,
                        decision_id=proposal.current_decision_id, decision_binding=proposal.current_decision_binding_hash,
                        execution_id=execution.execution_id, execution_binding=execution.execution_binding_hash,
                        outcome=outcome, outcome_reason_code=outcome_reason_code, outcome_request=req_hash)
            return _result(recorded, status=201)

    report = report_outcome
    outcome = report_outcome

    # ----- restore reconciliation ----------------------------------------

    def _restore_resolver(self) -> Callable[..., Any] | Mapping[str, Any] | None:
        resolver = self.restore_envelope_resolver
        if resolver is None:
            resolver = getattr(self.store, "resolve_restore_envelope", None)
        return resolver

    def _restore_config(self) -> Any:
        config = self.restore_fence_config
        if config is None:
            raise ExternalActionError("review_authority_unavailable", "restore verification key history is unavailable", status=503)
        return config

    @staticmethod
    def _restore_hash(domain: str, value: Mapping[str, Any]) -> str:
        try:
            return domain_digest(domain, value)
        except SigningError as exc:
            raise ExternalActionError("invalid_proposal", "restore request is not canonical") from exc

    def _verify_restore_envelopes(self, body: Mapping[str, Any], *, now: datetime | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
        """Resolve and verify all six create envelopes and their bindings."""

        resolver = self._restore_resolver()
        config = self._restore_config()
        specs = (
            ("old_epoch_lease_envelope_ref", "old_epoch_lease", "execution_epoch_lease", False),
            ("new_epoch_lease_envelope_ref", "new_epoch_lease", "execution_epoch_lease", True),
            ("worker_quiescence_envelope_ref", "worker_quiescence", "worker_quiescence", True),
            ("provider_authority_fence_envelope_ref", "provider_authority_fence", "provider_authority_fence", True),
            ("ledger_fence_envelope_ref", "ledger_fence", "ledger_fence", True),
            ("restore_attestation_envelope_ref", "restore_attestation", "restore_attestation", True),
        )
        verified: dict[str, Any] = {}
        try:
            for field, name, purpose, live in specs:
                ref = resolve_restore_reference(body[field], resolver=resolver, purpose=purpose)
                verified[name] = verify_restore_envelope(ref, config=config, purpose=purpose, now=now, require_current_time=live)
        except (KeyError, RestoreFenceError) as exc:
            code = getattr(exc, "code", "invalid_restore_checkpoint")
            status = 503 if code == "review_authority_unavailable" else 422
            raise ExternalActionError(code, "restore evidence failed verification", status=status) from exc
        old = verified["old_epoch_lease"]
        new = verified["new_epoch_lease"]
        worker = verified["worker_quiescence"]
        provider = verified["provider_authority_fence"]
        ledger = verified["ledger_fence"]
        attestation = verified["restore_attestation"]
        deployment = body.get("deployment", getattr(self.store, "deployment", None))
        if not isinstance(deployment, str) or not deployment:
            raise ExternalActionError("review_authority_unavailable", "restore deployment is unavailable", status=503)
        if attestation.payload.get("deployment") != deployment or attestation.payload.get("restore_id") != body.get("restore_id"):
            raise ExternalActionError("invalid_restore_checkpoint", "restore attestation identity binding is invalid", status=422)
        old_epoch, new_epoch = old.payload.get("epoch"), new.payload.get("epoch")
        if not isinstance(old_epoch, int) or isinstance(old_epoch, bool) or not isinstance(new_epoch, int) or isinstance(new_epoch, bool) or new_epoch <= old_epoch:
            raise ExternalActionError("invalid_restore_checkpoint", "restore epochs are not an advancing pair", status=422)
        if attestation.payload.get("old_epoch_lease_digest") != old.envelope_digest or attestation.payload.get("new_epoch_lease_digest") != new.envelope_digest or attestation.payload.get("old_epoch") != old_epoch or attestation.payload.get("new_epoch") != new_epoch:
            raise ExternalActionError("invalid_restore_checkpoint", "restore attestation lease binding is invalid", status=422)
        if (
            new.payload.get("lease_sequence") != 1
            or new.payload.get("previous_lease_digest") is not None
            or new.payload.get("previous_epoch_digest") != old.envelope_digest
        ):
            raise ExternalActionError("invalid_restore_checkpoint", "new restore lease does not name the old epoch predecessor", status=422)
        for item in (worker, provider, ledger):
            if item.payload.get("deployment") != deployment or item.payload.get("old_epoch") != old_epoch or item.payload.get("epoch_rotation_lease_digest") != new.envelope_digest:
                raise ExternalActionError("invalid_restore_checkpoint", "restore fence binding is invalid", status=422)
        if ledger.payload.get("worker_quiescence_envelope_digest") != worker.envelope_digest or ledger.payload.get("provider_authority_fence_envelope_digest") != provider.envelope_digest or attestation.payload.get("ledger_fence_envelope_digest") != ledger.envelope_digest:
            raise ExternalActionError("invalid_restore_checkpoint", "restore ledger fence binding is invalid", status=422)
        if (
            attestation.payload.get("fenced_ledger_root_digest") != ledger.payload.get("fenced_ledger_root_digest")
            or attestation.payload.get("fenced_ledger_entry_count") != ledger.payload.get("fenced_ledger_entry_count")
        ):
            raise ExternalActionError("invalid_restore_checkpoint", "restore attestation ledger root is not bound", status=422)
        subjects = [worker.payload.get("signer_separation_subject"), provider.payload.get("signer_separation_subject"), ledger.payload.get("signer_separation_subject"), attestation.payload.get("signer_separation_subject")]
        if any(not isinstance(subject, str) for subject in subjects) or len(set(subjects)) != len(subjects):
            raise ExternalActionError("proposer_approver_separation_required", "restore signer separation is required", status=403)
        for item in (worker, provider, ledger, attestation):
            for field in ("fenced_ledger_entry_count", "worker_instance_count", "provider_authority_count", "effect_inventory_count", "worker_checkpoint_count", "pre_restore_event_head_count"):
                value = item.payload.get(field)
                if value is not None and (not isinstance(value, int) or isinstance(value, bool) or value < 0):
                    raise ExternalActionError("invalid_restore_checkpoint", "restore evidence count is invalid", status=422)
        command = {
            "deployment": deployment, "restore_id": body["restore_id"], "old_epoch": old_epoch, "new_epoch": new_epoch,
            "old_epoch_lease_digest": old.envelope_digest, "new_epoch_lease_digest": new.envelope_digest,
            "ledger_fence_envelope_digest": ledger.envelope_digest, "fenced_ledger_root_digest": ledger.payload["fenced_ledger_root_digest"],
            "fenced_ledger_entry_count": ledger.payload["fenced_ledger_entry_count"], "restored_snapshot_digest": attestation.payload["restored_snapshot_digest"],
            "pre_restore_event_root_digest": attestation.payload["pre_restore_event_root_digest"], "pre_restore_event_head_count": attestation.payload["pre_restore_event_head_count"],
            "effect_inventory_digest": attestation.payload["effect_inventory_digest"], "effect_inventory_count": attestation.payload["effect_inventory_count"],
            "worker_checkpoint_set_digest": attestation.payload["worker_checkpoint_set_digest"], "worker_checkpoint_count": attestation.payload["worker_checkpoint_count"],
            "issued_at": attestation.payload["issued_at"], "expires_at": attestation.payload["expires_at"], "nonce": attestation.payload["nonce"],
            "signer_separation_subject": attestation.payload["signer_separation_subject"], "protected_b64": attestation.envelope["protected_b64"],
            "payload_b64": attestation.envelope["payload_b64"], "signature_b64": attestation.envelope["signature_b64"],
            "signer_key_id_digest": domain_digest("okg-external-action-restore-signing-key-id-v1", {"kid": attestation.key_id, "purpose": "restore_attestation"}),
            "envelope_digest": attestation.envelope_digest,
            "restore_evidence": {name: {"envelope": dict(value.envelope), "payload": dict(value.payload), "envelope_digest": value.envelope_digest} for name, value in verified.items()},
        }
        return command, verified

    def _normalise_restore_create(self, request: Mapping[str, Any]) -> dict[str, Any]:
        allowed = {"restore_id", "old_epoch_lease_envelope_ref", "new_epoch_lease_envelope_ref", "worker_quiescence_envelope_ref", "provider_authority_fence_envelope_ref", "ledger_fence_envelope_ref", "restore_attestation_envelope_ref"}
        if not isinstance(request, Mapping) or set(request) != allowed:
            raise ExternalActionError("invalid_restore_checkpoint", "restore create request is not closed", status=422)
        command, verified = self._verify_restore_envelopes(request)
        inventory = self._restore_inventory(request["restore_id"], deployment=command["deployment"])
        # The deployment authority returns the surviving old-epoch database
        # keys.  The signed attestation's inventory is deliberately *not*
        # required to equal that base set: its closed digest/count cover the
        # database keys UNION every complete positive ledger proof.  The SQL
        # finalize command derives and checks that union after all immutable
        # checkpoint entries have been staged.
        command["effect_inventory"] = inventory
        command.update({
            "old_epoch_lease_envelope_digest": verified["old_epoch_lease"].envelope_digest,
            "new_epoch_lease_envelope_digest": verified["new_epoch_lease"].envelope_digest,
            "worker_quiescence_envelope_digest": verified["worker_quiescence"].envelope_digest,
            "provider_authority_fence_envelope_digest": verified["provider_authority_fence"].envelope_digest,
        })
        # References are transport locators, not request identity.  Hash the
        # six verified envelope digests so equivalent closed references cannot
        # collide or diverge merely because their spelling changed.
        command["request_hash"] = self._restore_hash(
            "okg-external-action-restore-create-request-v1",
            {
                "restore_id": request["restore_id"],
                "old_epoch_lease_envelope_digest": verified["old_epoch_lease"].envelope_digest,
                "new_epoch_lease_envelope_digest": verified["new_epoch_lease"].envelope_digest,
                "worker_quiescence_envelope_digest": verified["worker_quiescence"].envelope_digest,
                "provider_authority_fence_envelope_digest": verified["provider_authority_fence"].envelope_digest,
                "ledger_fence_envelope_digest": verified["ledger_fence"].envelope_digest,
                "restore_attestation_envelope_digest": verified["restore_attestation"].envelope_digest,
            },
        )
        return command

    def _restore_inventory(self, restore_id: str, *, deployment: str) -> list[str]:
        """Read the exact restored effect inventory from deployment authority."""

        method = None
        for name in ("restore_effect_inventory", "restore_inventory", "get_restore_inventory"):
            candidate = getattr(self.store, name, None)
            if callable(candidate):
                method = candidate
                break
        if method is None:
            raise ExternalActionError("review_authority_unavailable", "restore inventory authority is unavailable", status=503)
        try:
            value = method(deployment=deployment, restore_id=restore_id)
        except TypeError:
            value = method(restore_id)
        authority_digest = authority_count = None
        if isinstance(value, Mapping):
            authority_digest = value.get("digest", value.get("effect_inventory_digest"))
            authority_count = value.get("count", value.get("effect_inventory_count"))
            value = value.get("sorted_effect_instance_keys", value.get("effect_instance_keys"))
        if not isinstance(value, (list, tuple)) or any(not isinstance(key, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", key) for key in value):
            raise ExternalActionError("review_authority_unavailable", "restore inventory authority returned an invalid projection", status=503)
        inventory = list(value)
        if inventory != sorted(inventory, key=lambda key: key.encode("utf-8")) or len(set(inventory)) != len(inventory):
            raise ExternalActionError("review_authority_unavailable", "restore inventory authority returned an unsorted projection", status=503)
        if authority_count is not None and authority_count != len(inventory):
            raise ExternalActionError("review_authority_unavailable", "restore inventory authority returned a wrong count", status=503)
        if authority_digest is not None and authority_digest != domain_digest(
            "okg-external-action-restore-inventory-v1",
            {"sorted_effect_instance_keys": inventory, "count": len(inventory)},
        ):
            raise ExternalActionError("review_authority_unavailable", "restore inventory authority returned a wrong digest", status=503)
        return inventory

    def _restore_session(self, restore_id: str) -> Mapping[str, Any]:
        """Return the database-owned restore bindings for one batch.

        A worker checkpoint is not self-authorizing: its old epoch, fenced
        root, and ledger fence must agree with the immutable create session.
        The lookup is intentionally a projection method so a PostgreSQL
        adapter can implement it with a fixed SECURITY DEFINER read rather
        than exposing restore tables to this service.
        """

        method = getattr(self.store, "restore_session_context", None)
        value = None
        if callable(method):
            try:
                value = method(restore_id=restore_id)
            except TypeError:
                value = method(restore_id)
        if value is None:
            sessions = getattr(self.store, "restore_sessions", None)
            getter = getattr(sessions, "get", None)
            if callable(getter):
                value = getter(restore_id)
        if value is None:
            raise ExternalActionError(
                "review_authority_unavailable",
                "restore session authority is unavailable",
                status=503,
            )
        if isinstance(value, Mapping):
            result = dict(value)
        else:
            result = {
                name: getattr(value, name, None)
                for name in (
                    "deployment", "restore_id", "status", "restore_attestation_envelope_digest",
                    "old_epoch", "new_epoch", "ledger_fence_envelope_digest",
                    "fenced_ledger_root_digest", "fenced_ledger_entry_count",
                )
            }
        required = (
            "restore_attestation_envelope_digest", "old_epoch",
            "ledger_fence_envelope_digest", "fenced_ledger_root_digest",
            "fenced_ledger_entry_count",
        )
        if any(name not in result or result[name] is None for name in required):
            raise ExternalActionError(
                "review_authority_unavailable",
                "restore session has incomplete immutable bindings",
                status=503,
            )
        return result

    def _restore_action_contract(self, payload: Mapping[str, Any]) -> ActionContract:
        """Resolve the immutable contract bound by a worker checkpoint."""

        digest = payload.get("action_contract_digest")
        if not isinstance(digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise RestoreFenceError("checkpoint has no action contract binding", code="invalid_restore_checkpoint")
        candidate = None
        proposal_id = payload.get("proposal_id")
        if isinstance(proposal_id, str):
            proposal = self.store.get(proposal_id)
            if proposal is not None:
                if getattr(proposal, "action_contract_digest", None) != digest:
                    raise RestoreFenceError("checkpoint action contract binding is stale", code="invalid_restore_checkpoint")
                candidate = self.store.contracts.get(digest)
                if candidate is None:
                    candidate = getattr(proposal, "action_contract_record", None)
        if candidate is None:
            contracts = getattr(self.store, "contracts", None)
            if contracts is not None and callable(getattr(contracts, "get", None)):
                candidate = contracts.get(digest)
        if candidate is None:
            for value in self.action_contracts.values():
                if getattr(value, "action_contract_digest", None) == digest:
                    candidate = value
                    break
                if isinstance(value, Mapping):
                    try:
                        parsed = ActionContract.from_mapping(value)
                    except (ActionContractError, TypeError, ValueError):
                        continue
                    if parsed.action_contract_digest == digest:
                        candidate = parsed
                        break
        if candidate is None:
            raise RestoreFenceError("checkpoint action contract is unavailable", code="review_authority_unavailable")
        try:
            contract = candidate if isinstance(candidate, ActionContract) else ActionContract.from_mapping(candidate)
        except (ActionContractError, TypeError, ValueError) as exc:
            raise RestoreFenceError("checkpoint action contract is invalid", code="review_authority_unavailable") from exc
        if contract.action_contract_digest != digest:
            raise RestoreFenceError("checkpoint action contract digest is invalid", code="invalid_restore_checkpoint")
        return contract

    def _restore_checkpoint_classification(self, payload: Mapping[str, Any], proof_kind: str) -> str:
        """Validate phase/nullability and derive the closed recovery class."""

        phase = payload.get("journal_phase")
        if phase not in {
            "database_only_not_started", "claimed_before_ledger_begin",
            "effect_starting", "provider_receipt_observed", "outcome_reported",
        }:
            raise RestoreFenceError("checkpoint journal phase is invalid", code="invalid_restore_checkpoint")

        def require_null(*names: str) -> None:
            if any(payload.get(name) is not None for name in names):
                raise RestoreFenceError("checkpoint phase has an illegal field", code="invalid_restore_checkpoint")

        def require_value(*names: str) -> None:
            if any(payload.get(name) is None for name in names):
                raise RestoreFenceError("checkpoint phase is missing a required field", code="invalid_restore_checkpoint")

        if phase in {"database_only_not_started", "claimed_before_ledger_begin"}:
            if proof_kind != "nonmembership":
                raise RestoreFenceError("database-only checkpoint requires nonmembership proof", code="invalid_restore_checkpoint")
            if payload.get("positive_proof_digest") is not None:
                raise RestoreFenceError("nonmembership checkpoint has a positive proof", code="invalid_restore_checkpoint")
            # Even a database-only/orphan recovery must retain the immutable
            # external attempt identity and a signed checkpoint time.  SQL
            # uses the submission hash to build the recovery/tombstone chain;
            # accepting null here would defer a malformed checkpoint until
            # finalize and could not produce a complete audit record.
            require_value("submission_content_hash", "checkpoint_issued_at")
            require_null(
                "ledger_entry_digest", "provider_idempotency_key_digest", "effect_started_at",
                "provider_receipt_digest", "outcome", "outcome_reason_code",
                "outcome_recorded_at", "journal_digest",
            )
            if phase == "database_only_not_started":
                # A surviving proposal may be named, but its proposal fields
                # are all-or-nothing; no decision, claim, or handoff may be
                # invented for a database-only row.
                proposal_fields = (
                    "proposal_id", "proposal_request_hash", "original_payload_hash",
                    "action_contract_digest",
                )
                if payload.get("proposal_id") is None:
                    require_null(*proposal_fields[1:])
                else:
                    require_value(*proposal_fields)
                require_null(
                    "decision_id", "decision_binding_hash", "execution_id",
                    "execution_binding_hash", "approved_payload_hash",
                    "okg_claim_handoff_receipt_digest", "pre_restore_effect_event_head_hash",
                )
            else:
                # Claimed-before-ledger-begin is a stronger checkpoint: the
                # exact decision/execution/handoff chain must survive, while
                # all ledger/start/receipt/outcome fields remain null.
                require_value(
                    "proposal_id", "proposal_request_hash", "original_payload_hash",
                    "approved_payload_hash", "action_contract_digest", "decision_id",
                    "decision_binding_hash", "execution_id", "execution_binding_hash",
                    "okg_claim_handoff_receipt_digest", "pre_restore_effect_event_head_hash",
                )
            return "database_restore_no_effect_started"

        if proof_kind != "positive":
            raise RestoreFenceError("ledger journal checkpoint requires positive proof", code="invalid_restore_checkpoint")
        if payload.get("nonmembership_proof_digest") is not None:
            raise RestoreFenceError("positive checkpoint has a nonmembership proof", code="invalid_restore_checkpoint")
        require_value(
            "ledger_entry_digest", "provider_idempotency_key_digest", "effect_started_at", "checkpoint_issued_at",
            "journal_digest", "submission_content_hash", "proposal_id",
            "proposal_request_hash", "original_payload_hash", "approved_payload_hash",
            "action_contract_digest", "decision_id", "decision_binding_hash",
            "execution_id", "execution_binding_hash", "okg_claim_handoff_receipt_digest",
            "pre_restore_effect_event_head_hash",
        )
        if phase == "effect_starting":
            require_null("outcome", "outcome_reason_code", "outcome_recorded_at")
            require_null("provider_receipt_digest")
            return "database_restore_effect_uncertain"
        if phase == "provider_receipt_observed":
            require_null("outcome", "outcome_reason_code", "outcome_recorded_at")
            require_value("provider_receipt_digest")
            return "database_restore_succeeded"

        # outcome_reported is terminal only when the signed outcome says so;
        # an indeterminate report remains uncertain.  A successful report
        # must carry its provider receipt, while failed/indeterminate reports
        # must not smuggle one into the recovery record.
        require_value("outcome", "outcome_reason_code", "outcome_recorded_at")
        outcome = payload["outcome"]
        if not isinstance(outcome, str):
            raise RestoreFenceError("checkpoint outcome is invalid", code="invalid_restore_checkpoint")
        contract = self._restore_action_contract(payload)
        reason = payload["outcome_reason_code"]
        rule = contract.outcome_reason_rules.get(reason) if isinstance(reason, str) else None
        if not isinstance(rule, Mapping) or outcome not in rule.get("allowed_outcomes", ()):
            raise RestoreFenceError("checkpoint outcome reason is not allowed by its action contract", code="invalid_restore_checkpoint")
        receipt_required = rule.get("provider_receipt") == "required"
        has_receipt = payload.get("provider_receipt_digest") is not None
        if receipt_required != has_receipt:
            raise RestoreFenceError("checkpoint provider receipt does not match its action contract", code="invalid_restore_checkpoint")
        if outcome == "succeeded":
            return "database_restore_succeeded"
        if outcome == "failed":
            return "database_restore_failed"
        if outcome == "indeterminate":
            return "database_restore_effect_uncertain"
        raise RestoreFenceError("checkpoint outcome is invalid", code="invalid_restore_checkpoint")

    def _normalise_restore_batch(self, request: Mapping[str, Any]) -> dict[str, Any]:
        allowed = {"restore_id", "batch_index", "checkpoints"}
        if not isinstance(request, Mapping) or set(request) != allowed:
            raise ExternalActionError("invalid_restore_checkpoint", "restore batch request is not closed", status=422)
        index, checkpoints = request.get("batch_index"), request.get("checkpoints")
        if not isinstance(index, int) or isinstance(index, bool) or not 0 <= index <= 2**31 - 1 or not isinstance(checkpoints, list) or not 1 <= len(checkpoints) <= 256:
            raise ExternalActionError("invalid_restore_checkpoint", "restore batch size or index is invalid", status=422)
        resolver, config = self._restore_resolver(), self._restore_config()
        session = self._restore_session(request["restore_id"])
        session_attestation = session["restore_attestation_envelope_digest"]
        if not isinstance(session_attestation, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", session_attestation):
            raise ExternalActionError("review_authority_unavailable", "restore session attestation binding is invalid", status=503)
        rows, keys = [], []
        try:
            for item in checkpoints:
                if not isinstance(item, Mapping) or set(item) != {"effect_instance_key", "checkpoint_envelope_ref", "proof_kind", "proof_ref"}:
                    raise RestoreFenceError("checkpoint item is not closed", code="invalid_restore_checkpoint")
                key, kind = item["effect_instance_key"], item["proof_kind"]
                if not isinstance(key, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", key) or kind not in {"positive", "nonmembership"}:
                    raise RestoreFenceError("checkpoint identity is invalid", code="invalid_restore_checkpoint")
                envelope = verify_restore_envelope(resolve_restore_reference(item["checkpoint_envelope_ref"], resolver=resolver, purpose="worker_checkpoint"), config=config, purpose="worker_checkpoint")
                proof = resolve_restore_reference(item["proof_ref"], resolver=resolver, purpose=f"{kind}_proof")
                checkpoint = envelope.payload
                session_deployment = session.get("deployment") or getattr(self.store, "deployment", None)
                if not isinstance(session_deployment, str) or not session_deployment:
                    raise RestoreFenceError("restore session has no deployment binding", code="review_authority_unavailable")
                expected_checkpoint_binding = {
                    "deployment": session_deployment,
                    "execution_epoch": session["old_epoch"],
                    "ledger_fence_envelope_digest": session["ledger_fence_envelope_digest"],
                    "fenced_ledger_root_digest": session["fenced_ledger_root_digest"],
                    "fenced_ledger_entry_count": session["fenced_ledger_entry_count"],
                }
                if any(checkpoint.get(field) != value for field, value in expected_checkpoint_binding.items()):
                    raise RestoreFenceError("checkpoint is not bound to the restore session", code="invalid_restore_checkpoint")
                checked = verify_restore_proof(proof, proof_kind=kind, effect_key=key, root=session["fenced_ledger_root_digest"], count=session["fenced_ledger_entry_count"])
                digest_field = "positive_proof_digest" if kind == "positive" else "nonmembership_proof_digest"
                proof_digest = domain_digest(f"okg-external-action-restore-{kind}-proof-v1", checked)
                if checkpoint.get("effect_instance_key") != key or checkpoint.get("proof_kind") != kind or checkpoint.get(digest_field) != proof_digest:
                    raise RestoreFenceError("checkpoint envelope and proof are not bound", code="invalid_restore_checkpoint")
                if kind == "positive" and checkpoint.get("ledger_entry_digest") != checked.get("ledger_entry_digest"):
                    raise RestoreFenceError("checkpoint ledger entry is not bound to its proof", code="invalid_restore_checkpoint")
                keys.append(key)
                phase_classification = self._restore_checkpoint_classification(checkpoint, kind)
                checkpoint_payload = dict(checkpoint)
                # Classification is derived from the verified phase; it is an
                # internal SQL recovery projection, not part of the signed
                # worker payload.
                checkpoint_payload["classification"] = phase_classification
                rows.append({"effect_instance_key": key, "checkpoint_envelope_digest": envelope.envelope_digest, "proof_digest": proof_digest, "checkpoint_payload": checkpoint_payload, "checkpoint_envelope": dict(envelope.envelope), "proof": dict(checked)})
        except (KeyError, RestoreFenceError, SigningError, TypeError, ValueError) as exc:
            code = getattr(exc, "code", "invalid_restore_checkpoint")
            status = 503 if code == "review_authority_unavailable" else 422
            raise ExternalActionError(code, "restore checkpoint evidence failed verification", status=status) from exc
        if keys != sorted(keys, key=lambda value: value.encode("utf-8")) or len(set(keys)) != len(keys):
            raise ExternalActionError("invalid_restore_checkpoint", "restore batch keys must be sorted and unique", status=422)
        command = {
            "restore_id": request["restore_id"],
            "batch_index": index,
            "restore_attestation_envelope_digest": session_attestation,
            # Keep the exact verified evidence beside its parsed projection;
            # the SQL boundary may persist only the closed projection, but it
            # must never be forced to reconstruct or trust caller references.
            "checkpoints": [
                {key: row[key] for key in (
                    "effect_instance_key", "checkpoint_envelope_digest", "proof_digest",
                    "checkpoint_payload", "checkpoint_envelope", "proof",
                )}
                for row in rows
            ],
        }
        command["request_hash"] = self._restore_hash(
            "okg-external-action-restore-batch-request-v1",
            {
                "restore_id": request["restore_id"],
                "batch_index": index,
                "checkpoints": [
                    {
                        "effect_instance_key": row["effect_instance_key"],
                        "checkpoint_envelope_digest": row["checkpoint_envelope_digest"],
                        "proof_digest": row["proof_digest"],
                    }
                    for row in rows
                ],
            },
        )
        return command

    def _restore_session_attestation(self, restore_id: str) -> str:
        value = self._restore_session(restore_id).get("restore_attestation_envelope_digest")
        if not isinstance(value, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", value):
            raise ExternalActionError("review_authority_unavailable", "restore session attestation binding is unavailable", status=503)
        return value

    def _normalise_restore_finalize(self, request: Mapping[str, Any]) -> dict[str, Any]:
        allowed = {"restore_id", "restore_attestation_envelope_digest", "expected_inventory_digest", "expected_inventory_count", "expected_checkpoint_set_digest", "expected_checkpoint_count"}
        if not isinstance(request, Mapping) or set(request) != allowed:
            raise ExternalActionError("invalid_restore_checkpoint", "restore finalize request is not closed", status=422)
        for field in ("restore_attestation_envelope_digest", "expected_inventory_digest", "expected_checkpoint_set_digest"):
            if not isinstance(request[field], str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", request[field]):
                raise ExternalActionError("invalid_restore_checkpoint", "restore finalize digest is invalid", status=422)
        for field in ("expected_inventory_count", "expected_checkpoint_count"):
            if not isinstance(request[field], int) or isinstance(request[field], bool) or not 0 <= request[field] <= 2**31 - 1:
                raise ExternalActionError("invalid_restore_checkpoint", "restore finalize count is invalid", status=422)
        command = dict(request)
        command["request_hash"] = self._restore_hash("okg-external-action-restore-finalize-request-v1", {field: request[field] for field in allowed if field != "restore_id"})
        return command

    def _restore_rpc(self, function: str, *, actor: Actor | Mapping[str, Any] | str, request: Mapping[str, Any], idempotency_key: str | None = None) -> ServiceResult:
        if not self._is_durable():
            raise ExternalActionError("review_authority_unavailable", "restore authority unavailable", status=503)
        bound_actor = self._actor(actor)
        # Idempotency ownership is always verifier output plus a server hash;
        # a restore request may not select either identity field.
        idem_key = idempotency_key
        if not isinstance(idem_key, str):
            raise ExternalActionError("invalid_proposal", "restore Idempotency-Key is required", status=422)
        with self.store.lock:
            # Normalize and resolve all lock-scoped projections while the
            # durable store transaction is open.  In particular, a batch's
            # session binding cannot be read from a stale pre-lock snapshot.
            if function == "external_action_restore_create":
                command = self._normalise_restore_create(request)
            elif function == "external_action_restore_batch":
                command = self._normalise_restore_batch(request)
            else:
                command = self._normalise_restore_finalize(request)
            deployment = command.get("deployment", getattr(self.store, "deployment", None))
            if not isinstance(deployment, str) or not deployment:
                raise ExternalActionError("review_authority_unavailable", "restore deployment is unavailable", status=503)
            command["deployment"] = deployment
            command["operator_separation_subject"] = bound_actor.separation_subject
            command["idempotency_key_hash"] = self._idempotency_hash(idem_key)
            request_hash = command.get("request_hash")
            bound_actor = self._authorize(
                "external_action.restore_reconcile",
                bound_actor,
                context={"deployment": deployment, "request_hash": request_hash, "surface": "external_actions"},
            )
            command.update(self._authorization(bound_actor).to_mapping())
            try:
                if function == "external_action_restore_create":
                    raw = self.store.restore_create(command)
                elif function == "external_action_restore_batch":
                    raw = self.store.restore_batch(command)
                else:
                    raw = None
            except ExternalActionStoreError as exc:
                # The fixed SQL restore coverage RPC uses a serialization
                # error for a missing/extra checkpoint set.  Preserve the
                # public typed 409 shape while keeping every other database
                # error an unavailable authority.
                if function == "external_action_restore_finalize" and (
                    getattr(exc, "code", None) == "incomplete_restore_coverage"
                    or "coverage" in str(exc).lower()
                ):
                    raise ExternalActionError(
                        "incomplete_restore_coverage",
                        "restore checkpoint coverage is incomplete",
                        status=409,
                        detail={"quarantine_state": "blocked"},
                    ) from exc
                if getattr(exc, "code", None) == "restore_terminal_conflict":
                    raise ExternalActionError(
                        "restore_reconciliation_conflict",
                        "restore reconciliation conflicts with retained state",
                        status=409,
                        detail={"classification": "database_restore_terminal_conflict"},
                    ) from exc
                if getattr(exc, "code", None) == "restore_reconciliation_conflict":
                    raise ExternalActionError(
                        "restore_reconciliation_conflict",
                        "restore reconciliation conflicts with retained state",
                        status=409,
                    ) from exc
                raise ExternalActionError(
                    "review_authority_unavailable",
                    "restore authority is unavailable",
                    status=503,
                ) from exc
            if function == "external_action_restore_finalize":
                # The prepare RPC locks and derives every mutable value which
                # the completion receipt covers.  Python signs only that
                # result and immediately calls finalize on the same active
                # transaction; a signing or commit error therefore rolls back
                # recoveries, events, the receipt, and quarantine clearance.
                prepare_request = {
                    "deployment": deployment,
                    "restore_id": command["restore_id"],
                    "restore_attestation_envelope_digest": command["restore_attestation_envelope_digest"],
                    "expected_inventory_digest": command["expected_inventory_digest"],
                    "expected_inventory_count": command["expected_inventory_count"],
                    "expected_checkpoint_set_digest": command["expected_checkpoint_set_digest"],
                    "expected_checkpoint_count": command["expected_checkpoint_count"],
                    # Replay identity must reach prepare before coverage and
                    # mode guards.  An exact terminal replay returns the
                    # retained bytes without resigning; a key/request/operator
                    # collision is refused without changing state.
                    "operator_separation_subject": command["operator_separation_subject"],
                    "idempotency_key_hash": command["idempotency_key_hash"],
                    "request_hash": command["request_hash"],
                }
                try:
                    prepared = self.store.restore_prepare(prepare_request)
                except ExternalActionStoreError as exc:
                    if getattr(exc, "code", None) == "incomplete_restore_coverage" or "coverage" in str(exc).lower():
                        raise ExternalActionError(
                            "incomplete_restore_coverage",
                            "restore checkpoint coverage is incomplete",
                            status=409,
                            detail={"quarantine_state": "blocked"},
                        ) from exc
                    if getattr(exc, "code", None) == "restore_terminal_conflict":
                        raise ExternalActionError(
                            "restore_reconciliation_conflict",
                            "restore reconciliation conflicts with retained state",
                            status=409,
                            detail={"classification": "database_restore_terminal_conflict"},
                        ) from exc
                    if getattr(exc, "code", None) == "restore_reconciliation_conflict":
                        raise ExternalActionError(
                            "restore_reconciliation_conflict",
                            "restore reconciliation conflicts with retained state",
                            status=409,
                        ) from exc
                    raise ExternalActionError(
                        "review_authority_unavailable",
                        "restore prepare authority is unavailable",
                        status=503,
                    ) from exc
                except AttributeError as exc:
                    raise ExternalActionError(
                        "review_authority_unavailable",
                        "restore prepare authority is unavailable",
                        status=503,
                    ) from exc
                if not isinstance(prepared, Mapping):
                    raise ExternalActionError(
                        "review_authority_unavailable",
                        "restore prepare authority returned no projection",
                        status=503,
                    )
                # A store may answer an exact terminal replay directly.  It
                # must carry the retained canonical response and must not
                # cause Python to sign a new receipt.
                if prepared.get("replay") is True:
                    if not isinstance(prepared.get("response_body_jcs", prepared.get("response_body")), (bytes, str, Mapping)):
                        raise ExternalActionError(
                            "review_authority_unavailable",
                            "restore replay response is unavailable",
                            status=503,
                        )
                    return self._durable_record_result(prepared, None, status=200)
                required_prepared = {
                    "deployment", "restore_id", "restore_attestation_envelope_digest",
                    "pre_restore_event_root_digest", "pre_restore_event_head_count",
                    "restored_event_root_digest", "restored_event_head_count",
                    "recovery_set_digest", "recovery_count", "completed_at",
                }
                if not required_prepared.issubset(prepared):
                    raise ExternalActionError(
                        "review_authority_unavailable",
                        "restore prepare projection is incomplete",
                        status=503,
                    )
                projection = {
                    field: prepared[field]
                    for field in _RESTORE_COMPLETION_FIELDS
                }
                try:
                    parse_timestamp(projection["completed_at"])
                    for field in (
                        "restore_attestation_envelope_digest", "pre_restore_event_root_digest",
                        "restored_event_root_digest", "recovery_set_digest",
                    ):
                        if not isinstance(projection[field], str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", projection[field]):
                            raise ValueError("restore completion digest is invalid")
                    for field in ("pre_restore_event_head_count", "restored_event_head_count", "recovery_count"):
                        if not isinstance(projection[field], int) or isinstance(projection[field], bool) or projection[field] < 0:
                            raise ValueError("restore completion count is invalid")
                except (KeyError, TypeError, ValueError) as exc:
                    raise ExternalActionError(
                        "review_authority_unavailable",
                        "restore prepare projection is invalid",
                        status=503,
                    ) from exc
                receipt = self._completion_envelope(projection)
                unsigned = {
                    key: receipt[key]
                    for key in ("protected_b64", "payload_b64", "signature_b64")
                }
                final_command = dict(command)
                final_command.update({
                    "completion_receipt_envelope_digest": receipt["envelope_digest"],
                    "completion_receipt_protected_b64": receipt["protected_b64"],
                    "completion_receipt_payload_b64": receipt["payload_b64"],
                    "completion_receipt_signature_b64": receipt["signature_b64"],
                    "completion_receipt_projection": projection,
                    "completion_receipt_projection_digest": domain_digest(
                        "okg-external-action-restore-completion-receipt-v1", projection
                    ),
                })
                # Keep the unsigned local value named for readability and to
                # make it explicit that no private key or caller digest is
                # passed through the RPC.
                del unsigned
                raw = self.store.restore_finalize(final_command)
            if function == "external_action_restore_batch" and isinstance(raw, Mapping):
                # SQL returns the count for the just-uploaded batch.  The API
                # contract reports the cumulative staged count, so derive it
                # from the refreshed immutable staging projection when the
                # store exposes it.  Classifications are included only when
                # this Python boundary has the verified checkpoint payload;
                # no caller-supplied classification is accepted.
                # Newer snapshot RPCs expose one row per entry.  Prefer that
                # projection because SQL deliberately strips the internal
                # classification marker from the persisted batch header; the
                # classification can be deterministically recomputed from the
                # signed payload and pinned action contract here.
                entries_projection = getattr(self.store, "restore_checkpoint_entries", None)
                staging = getattr(self.store, "restore_checkpoint_staging", None)
                total = 0
                classifications: dict[str, int] = {}
                entry_values = getattr(entries_projection, "items", lambda: ())()
                if entry_values:
                    for entry_key, entry in entry_values:
                        if not isinstance(entry_key, tuple) or not entry_key or entry_key[0] != command.get("restore_id"):
                            continue
                        if not isinstance(entry, Mapping):
                            continue
                        total += 1
                        classification = entry.get("classification")
                        if not isinstance(classification, str):
                            try:
                                classification = self._restore_checkpoint_classification(
                                    entry, entry.get("proof_kind")
                                )
                            except RestoreFenceError as exc:
                                raise ExternalActionError(
                                    "review_authority_unavailable",
                                    "stored restore checkpoint projection is invalid",
                                    status=503,
                                ) from exc
                        classifications[classification] = classifications.get(classification, 0) + 1
                else:
                    values = getattr(staging, "values", lambda: ())()
                    for item in values:
                        if not isinstance(item, Mapping) or item.get("restore_id") != command.get("restore_id"):
                            continue
                        entries = item.get("checkpoint_payload")
                        if not isinstance(entries, list):
                            continue
                        total += len(entries)
                        for entry in entries:
                            if not isinstance(entry, Mapping):
                                continue
                            classification = entry.get("classification")
                            if not isinstance(classification, str):
                                try:
                                    classification = self._restore_checkpoint_classification(
                                        entry, entry.get("proof_kind")
                                    )
                                except RestoreFenceError as exc:
                                    raise ExternalActionError(
                                        "review_authority_unavailable",
                                        "stored restore checkpoint projection is invalid",
                                        status=503,
                                    ) from exc
                            classifications[classification] = classifications.get(classification, 0) + 1
                if total:
                    raw = dict(raw)
                    raw["accepted_checkpoint_count"] = total
                    if classifications:
                        raw["classifications"] = classifications
            return self._durable_record_result(
                raw,
                None,
                status=200 if function == "external_action_restore_finalize" else 201,
            )

    def restore_create(self, *, actor: Actor | Mapping[str, Any] | str, request: Mapping[str, Any], idempotency_key: str | None = None) -> ServiceResult:
        return self._restore_rpc("external_action_restore_create", actor=actor, request=request, idempotency_key=idempotency_key)

    create_restore_reconciliation = restore_create

    def restore_batch(self, *, actor: Actor | Mapping[str, Any] | str, restore_id: str | None = None, batch_index: int | None = None, batch: Mapping[str, Any] | None = None, request: Mapping[str, Any] | None = None, idempotency_key: str | None = None) -> ServiceResult:
        command = dict(request or batch or {})
        if restore_id is not None:
            if "restore_id" in command and command["restore_id"] != restore_id:
                raise ExternalActionError("invalid_restore_checkpoint", "path restore id does not match request", status=422)
            command["restore_id"] = restore_id
        if batch_index is not None:
            if "batch_index" in command and command["batch_index"] != batch_index:
                raise ExternalActionError("invalid_restore_checkpoint", "path batch index does not match request", status=422)
            command["batch_index"] = batch_index
        return self._restore_rpc("external_action_restore_batch", actor=actor, request=command, idempotency_key=idempotency_key)

    upload_checkpoint_batch = restore_batch

    def restore_finalize(self, *, actor: Actor | Mapping[str, Any] | str, restore_id: str | None = None, request: Mapping[str, Any], idempotency_key: str | None = None) -> ServiceResult:
        command = dict(request)
        if restore_id is not None:
            if "restore_id" in command and command["restore_id"] != restore_id:
                raise ExternalActionError("invalid_restore_checkpoint", "path restore id does not match request", status=422)
            command["restore_id"] = restore_id
        return self._restore_rpc("external_action_restore_finalize", actor=actor, request=command, idempotency_key=idempotency_key)

    finalize_restore_reconciliation = restore_finalize

    # ----- reads and audit projections -----------------------------------

    @staticmethod
    def _read_resource(operation: str, *, proposal_id: str | None, effect_instance_key: str | None, deployment: str | None) -> str:
        if operation in {"read", "read_payload", "timeline"} and isinstance(proposal_id, str):
            return proposal_id
        if operation == "recovery_record" and isinstance(effect_instance_key, str):
            return effect_instance_key
        if operation == "list" and isinstance(deployment, str):
            return deployment
        raise ExternalActionError("invalid_proposal", "read resource is unavailable")

    @staticmethod
    def _read_hash(operation: str, resource: str, include_payload: bool) -> str:
        if operation not in {"read", "read_payload", "list", "timeline", "recovery_record"}:
            raise ExternalActionError("invalid_proposal", "read operation is invalid")
        if include_payload != (operation == "read_payload"):
            raise ExternalActionError("invalid_proposal", "read payload binding is invalid")
        try:
            return transition_request_hash(
                {
                    "operation": operation,
                    "resource": resource,
                    "include_payload": include_payload,
                }
            )
        except ActionContractError as exc:
            raise ExternalActionError("review_authority_unavailable", "read authority is unavailable", status=503) from exc

    @staticmethod
    def _read_action(operation: str) -> str:
        return "external_action.read_payload" if operation == "read_payload" else "external_action.read"

    def _authorized_read_rpc(self, request: Mapping[str, Any], *, actor: Actor, authorization: AuthorizationRefs | None = None) -> Any:
        method = getattr(self.store, "authorized_read", None)
        if not callable(method):
            raise ExternalActionError(
                "review_authority_unavailable",
                "durable read authority is unavailable",
                status=503,
            )
        kwargs = dict(request)
        kwargs["actor"] = actor
        if authorization is not None:
            kwargs["authorization"] = authorization
        try:
            parameters = inspect.signature(method).parameters
        except (TypeError, ValueError):
            parameters = {}
        if not any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()):
            kwargs = {key: value for key, value in kwargs.items() if key in parameters}
        try:
            result = method(**kwargs)
        except ExternalActionError:
            raise
        except Exception as exc:
            raise ExternalActionError(
                "review_authority_unavailable",
                "durable read authority is unavailable",
                status=503,
            ) from exc
        if inspect.isawaitable(result):
            raise ExternalActionError(
                "review_authority_unavailable",
                "durable read authority is unavailable",
                status=503,
            )
        return result

    def authorized_read(
        self,
        *,
        operation: str,
        actor: Actor | Mapping[str, Any] | str,
        request_hash: str,
        resource: str | None = None,
        deployment: str | None = None,
        proposal_id: str | None = None,
        effect_instance_key: str | None = None,
        include_payload: bool = False,
        state: str | None = None,
        limit: int | None = None,
    ) -> ServiceResult:
        """Authorize and return one closed read projection.

        Durable reads cross a dedicated store RPC after Nomos authorization.
        This prevents a mapped login from turning the broad snapshot cache
        into a payload disclosure surface.  The request digest is checked
        here before it reaches the store so every durable read has the exact
        transition-request domain and route binding.
        """

        actor_value = self._actor(actor)
        deployment_value = deployment or getattr(self.store, "deployment", None)
        resource_value = resource or self._read_resource(
            operation,
            proposal_id=proposal_id,
            effect_instance_key=effect_instance_key,
            deployment=deployment_value,
        )
        expected_hash = self._read_hash(operation, resource_value, include_payload)
        if request_hash != expected_hash:
            raise ExternalActionError("invalid_proposal", "read request binding is invalid")
        action = self._read_action(operation)
        with self.store.lock:
            bound_actor = self._authorize(
                action,
                actor_value,
                context={
                    "deployment": deployment_value,
                    "request_hash": request_hash,
                    "surface": "external_actions",
                },
            )
            if self._is_durable():
                raw = self._authorized_read_rpc(
                    {
                        "operation": operation,
                        "deployment": deployment_value,
                        "resource": resource_value,
                        "request_hash": request_hash,
                        "proposal_id": proposal_id,
                        "effect_instance_key": effect_instance_key,
                        "include_payload": include_payload,
                        "state": state,
                        "limit": limit,
                    },
                    actor=bound_actor,
                    authorization=self._authorization(bound_actor),
                )
                if isinstance(raw, ServiceResult):
                    return raw
                if not isinstance(raw, Mapping):
                    raise ExternalActionError(
                        "review_authority_unavailable",
                        "durable read authority returned an invalid projection",
                        status=503,
                    )
                if (
                    (operation in {"read", "read_payload"} and raw.get("proposal") is None)
                    or (operation == "timeline" and raw.get("events") is None)
                    or (operation == "recovery_record" and raw.get("recovery_record") is None)
                ):
                    raise ExternalActionError("proposal_not_found", "read resource was not found", status=404)
                projection = dict(raw)
                containers = [projection]
                proposal_projection = projection.get("proposal")
                if isinstance(proposal_projection, Mapping):
                    proposal_projection = dict(proposal_projection)
                    projection["proposal"] = proposal_projection
                    containers.append(proposal_projection)
                for container in containers:
                    canonical_payload = container.get("canonical_payload")
                    if isinstance(canonical_payload, Mapping) and set(canonical_payload) == {"__okg_bytes_b64"}:
                        canonical_payload = self._decode_claim_payload(canonical_payload)
                        container["canonical_payload"] = canonical_payload.decode("utf-8")
                    elif isinstance(canonical_payload, bytes):
                        try:
                            container["canonical_payload"] = canonical_payload.decode("utf-8")
                        except UnicodeDecodeError as exc:
                            raise ExternalActionError(
                                "review_authority_unavailable",
                                "durable read authority returned an invalid payload",
                                status=503,
                            ) from exc
                return _result(projection, status=200)

            if operation in {"read", "read_payload"}:
                proposal = self._materialize_expiry(self._proposal(proposal_id or ""))
                return self._projection_result(proposal, status=200, include_payload=include_payload)
            if operation == "list":
                result = []
                for proposal in self.store.all_proposals(deployment=deployment_value):
                    proposal = self._materialize_expiry(proposal)
                    if state is None or proposal.review_state == state or proposal.execution_state == state:
                        result.append(proposal.to_projection())
                if limit is not None:
                    result = result[:limit]
                return _result({"proposals": result}, status=200)
            if operation == "timeline":
                proposal = self._materialize_expiry(self._proposal(proposal_id or ""))
                return _result(
                    {"events": [event.to_projection() for event in self.store.get_events(proposal.effect_instance_key)]},
                    status=200,
                )
            raise ExternalActionError(
                "review_authority_unavailable",
                "recovery read authority is unavailable",
                status=503,
            )

    def read(
        self,
        proposal_id: str,
        *,
        include_payload: bool = False,
        actor: Actor | Mapping[str, Any] | str | None = None,
        request_hash: str | None = None,
        deployment: str | None = None,
    ) -> ServiceResult:
        if self._is_durable():
            if actor is None or request_hash is None:
                raise ExternalActionError(
                    "review_authority_unavailable",
                    "durable read authorization is unavailable",
                    status=503,
                )
            operation = "read_payload" if include_payload else "read"
            return self.authorized_read(
                operation=operation,
                actor=actor,
                request_hash=request_hash,
                resource=proposal_id,
                deployment=deployment,
                proposal_id=proposal_id,
                include_payload=include_payload,
            )
        with self.store.lock:
            proposal = self._materialize_expiry(self._proposal(proposal_id))
            return self._projection_result(proposal, status=200, include_payload=include_payload)

    get = read

    def list(
        self,
        *,
        deployment: str | None = None,
        state: str | None = None,
        actor: Actor | Mapping[str, Any] | str | None = None,
        request_hash: str | None = None,
        limit: int | None = None,
    ) -> tuple[dict[str, Any], ...] | ServiceResult:
        if self._is_durable():
            if actor is None or request_hash is None:
                raise ExternalActionError(
                    "review_authority_unavailable",
                    "durable read authorization is unavailable",
                    status=503,
                )
            return self.authorized_read(
                operation="list",
                actor=actor,
                request_hash=request_hash,
                resource=deployment or getattr(self.store, "deployment", None),
                deployment=deployment,
                include_payload=False,
                state=state,
                limit=limit,
            )
        with self.store.lock:
            result = []
            for proposal in self.store.all_proposals(deployment=deployment):
                proposal = self._materialize_expiry(proposal)
                if state is None or proposal.review_state == state or proposal.execution_state == state:
                    result.append(proposal.to_projection())
            if limit is not None:
                result = result[:limit]
            return tuple(result)

    def timeline(
        self,
        proposal_id: str,
        *,
        actor: Actor | Mapping[str, Any] | str | None = None,
        request_hash: str | None = None,
        deployment: str | None = None,
    ) -> tuple[dict[str, Any], ...] | ServiceResult:
        if self._is_durable():
            if actor is None or request_hash is None:
                raise ExternalActionError(
                    "review_authority_unavailable",
                    "durable read authorization is unavailable",
                    status=503,
                )
            return self.authorized_read(
                operation="timeline",
                actor=actor,
                request_hash=request_hash,
                resource=proposal_id,
                deployment=deployment,
                proposal_id=proposal_id,
                include_payload=False,
            )
        with self.store.lock:
            proposal = self._materialize_expiry(self._proposal(proposal_id))
            return tuple(event.to_projection() for event in self.store.get_events(proposal.effect_instance_key))

    def recovery_record(
        self,
        effect_instance_key: str,
        *,
        actor: Actor | Mapping[str, Any] | str | None = None,
        request_hash: str | None = None,
        deployment: str | None = None,
    ) -> ServiceResult:
        if actor is None or request_hash is None:
            raise ExternalActionError(
                "review_authority_unavailable",
                "recovery read authorization is unavailable",
                status=503,
            )
        return self.authorized_read(
            operation="recovery_record",
            actor=actor,
            request_hash=request_hash,
            resource=effect_instance_key,
            deployment=deployment,
            effect_instance_key=effect_instance_key,
            include_payload=False,
        )

    get_recovery_record = recovery_record

    def decisions(self, proposal_id: str) -> tuple[Decision, ...]:
        with self.store.lock:
            self._proposal(proposal_id)
            return tuple(self.store.decisions.get(proposal_id, ()))

    def payload_versions(self, proposal_id: str) -> tuple[PayloadVersion, ...]:
        with self.store.lock:
            self._proposal(proposal_id)
            return tuple(self.store.payload_versions.get(proposal_id, ()))

    def create_owner_unavailability_attestation(self, *, execution_id: str, actor: Actor | Mapping[str, Any] | str, attestation: Mapping[str, Any], idempotency_key: str, proposal_id: str | None = None) -> ServiceResult:
        actor = self._actor(actor)
        with self.store.lock:
            if self._is_durable():
                self._refresh_durable()
            execution = next((item for item in self.store.executions.values() if item.execution_id == execution_id), None)
            if execution is None:
                raise ExternalActionError("proposal_not_found", "execution not found", status=404)
            if proposal_id is not None and execution.proposal_id != proposal_id:
                raise ExternalActionError("proposal_not_found", "execution not found", status=404)
            key_hash = self._idempotency_hash(idempotency_key)
            envelope = copy.deepcopy(dict(attestation))
            if set(envelope) != {"protected_b64", "payload_b64", "signature_b64", "envelope_digest"}:
                raise ExternalActionError("invalid_proposal", "owner attestation envelope is invalid")
            try:
                validate_digest(envelope["envelope_digest"], field="envelope_digest")
            except (KeyError, ActionContractError) as exc:
                raise ExternalActionError("invalid_proposal", "owner attestation envelope is invalid") from exc
            envelope_digest = envelope["envelope_digest"]
            request_hash = digest_value(
                "okg-external-action-transition-request-v1",
                {
                    "protected_b64": envelope["protected_b64"],
                    "payload_b64": envelope["payload_b64"],
                    "signature_b64": envelope["signature_b64"],
                },
            )
            actor = self._authorize(
                "external_action.attest_owner_unavailable",
                actor,
                context={"deployment": getattr(execution, "deployment", None) or self._proposal(execution.proposal_id).deployment, "request_hash": request_hash, "surface": "external_actions"},
            )
            authorization = self._authorization(actor)
            existing = self.store.attestations.get(envelope_digest)
            verified, key, kid = self._verify_owner_attestation_envelope(
                envelope,
                execution=execution,
                actor=actor,
                allow_historical_replay=isinstance(existing, Mapping),
            )
            if existing is not None:
                if (
                    not isinstance(existing, Mapping)
                    or existing.get("request_hash") != request_hash
                    or existing.get("idempotency_key_hash") != key_hash
                    or existing.get("authenticated_submitter_separation_subject") != actor.separation_subject
                ):
                    raise ExternalActionError("attestation_replay_conflict", "attestation replay conflicts")
                if self._is_durable():
                    # Let the fixed RPC return the exact retained response
                    # bytes. It resolves replay before current-time checks.
                    pass
                else:
                    response_body = existing.get("response_body_jcs")
                    if not isinstance(response_body, bytes):
                        raise ExternalActionError(
                            "review_authority_unavailable",
                            "stored attestation response is unavailable",
                            status=503,
                        )
                    return ServiceResult(
                        value={key: existing[key] for key in ("protected_b64", "payload_b64", "signature_b64", "envelope_digest")},
                        status=200,
                        replay=True,
                        response_body=response_body,
                    )
            elif key.state != "current":
                raise ExternalActionError("invalid_proposal", "owner attestation signing key is not current")
            for retained in self.store.attestations.values():
                if (
                    isinstance(retained, Mapping)
                    and retained.get("signer_separation_subject") == verified["signer_separation_subject"]
                    and retained.get("nonce") == verified["nonce"]
                    and retained.get("envelope_digest") != envelope_digest
                ):
                    raise ExternalActionError("attestation_replay_conflict", "attestation nonce was reused")
            from .action_contracts import canonical_json_bytes

            response_projection = {
                "protected_b64": envelope["protected_b64"],
                "payload_b64": envelope["payload_b64"],
                "signature_b64": envelope["signature_b64"],
                "envelope_digest": envelope_digest,
            }
            signer_key_id_digest = domain_digest(
                "okg-external-action-owner-unavailability-signing-key-id-v1",
                {"kid": kid, "purpose": "owner_unavailability_attestation"},
            )
            record = {
                **verified,
                **response_projection,
                "authenticated_submitter_separation_subject": actor.separation_subject,
                "idempotency_key_hash": key_hash,
                "request_hash": request_hash,
                "response_status": 201,
                "response_body_jcs": canonical_json_bytes(response_projection),
                "signer_key_id_digest": signer_key_id_digest,
                "authentication_audit_id": actor.authentication_audit_id,
                "nomos_decision_id": actor.nomos_decision_id,
            }
            if self._is_durable():
                command_request = {
                    key: record[key]
                    for key in (
                        "deployment", "execution_id", "claim_owner_separation_subject",
                        "reconcile_not_before", "issued_at", "expires_at", "nonce",
                        "signer_separation_subject", "authenticated_submitter_separation_subject",
                        "idempotency_key_hash", "request_hash", "response_status",
                        "response_body_jcs", "protected_b64", "payload_b64", "signature_b64",
                        "signer_key_id_digest", "envelope_digest",
                    )
                }
                raw = self.store.owner_unavailability_command(OwnerUnavailabilityCommand(command_request, actor, authorization))
                self._refresh_durable()
                return self._durable_record_result(raw, self.store.attestations.get(envelope_digest), status=201)
            self.store.attestations[envelope_digest] = record
            return ServiceResult(
                value=response_projection,
                status=201,
                replay=False,
                response_body=record["response_body_jcs"],
            )

    attest_owner_unavailability = create_owner_unavailability_attestation

    @staticmethod
    def _diff_codes(before: Any, after: Any, contract: ActionContract) -> set[str]:
        if not isinstance(before, Mapping) or not isinstance(after, Mapping):
            return set(contract.diff_field_codes)
        changed: set[str] = set()
        for key in set(before) | set(after):
            if before.get(key) != after.get(key):
                candidates = [code for code in contract.diff_field_codes if key.lower() in code.lower() or code.lower().endswith(key.lower())]
                changed.add(candidates[0] if candidates else (contract.diff_field_codes[0] if contract.diff_field_codes else "changed"))
        return changed


def strict_payload(payload: Any) -> Any:
    """Round-trip through the strict canonicalizer to prevent mutable aliases."""

    from .action_contracts import strict_json_loads, canonical_json_bytes

    return strict_json_loads(canonical_json_bytes(payload))


def _result_value(proposal: Proposal, *, status: int, replay: bool, include_payload: bool, payload: bytes | None) -> ServiceResult:
    body = proposal.to_projection(include_payload=include_payload, payload=payload)
    from .action_contracts import canonical_json_bytes

    return ServiceResult(value=proposal, status=status, replay=replay, response_body=canonical_json_bytes(body))


__all__ = ["ExternalActionError", "LifecycleError", "ExternalActionService"]
