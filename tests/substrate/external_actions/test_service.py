from __future__ import annotations

from datetime import datetime, timezone
from collections import defaultdict
from pathlib import Path
import threading
from types import SimpleNamespace

import yaml
import pytest

from okg.substrate.external_actions import (
    Actor,
    ExternalActionError,
    ExternalActionService,
    InMemoryExternalActionStore,
)
from okg.substrate.external_actions.action_contracts import ActionContract, outcome_request_hash, transition_request_hash
from okg.substrate.external_actions.models import (
    AuthorizationRefs,
    ClaimCommand,
    OutcomeCommand,
    OwnerUnavailabilityCommand,
    ReviewTransitionCommand,
    SubmitCommand,
)
from okg.substrate.external_actions.service import DurableNomosAuthorizer
from okg.substrate.external_actions.store import ExternalActionStoreError, PostgresExternalActionStore


ROOT = Path(__file__).resolve().parents[3]
VECTORS = ROOT / "tests/fixtures/external_actions/hash-vectors.yaml"


@pytest.fixture()
def fixture():
    document = yaml.safe_load(VECTORS.read_text())
    values = {row["id"]: row for row in document["vectors"]}
    return values, ActionContract.from_mapping(values["action_contract"]["value"])


def _service() -> ExternalActionService:
    store = InMemoryExternalActionStore(
        now=datetime(2026, 8, 26, 14, tzinfo=timezone.utc), execution_epoch=17,
        policy_digest="sha256:" + "3" * 64,
    )
    return ExternalActionService(store)


def _submit(service, values, contract):
    return service.submit(
        deployment="support-prod",
        actor=Actor("oidc:automation.example:service:triage-bot", "subject:automation:triage-bot", "auth:submit", "nomos:submit"),
        idempotency_key="propose-triage-run-417",
        automation_id="ticket-triage", automation_version="3.2.0", run_id="run-417", correlation_id="incident-99",
        action_type="support.ticket.update/v1", target_ref="ticket:T-42", risk_tier="high",
        action_schema_digest=values["action_schema"]["digest"], action_contract=contract,
        requested_review_expires_at="2026-08-26T15:30:00.000000Z", summary_code="resolve_after_deploy",
        payload=values["payload_original"]["value"], evidence_digests=["sha256:" + "a" * 64],
    )


def test_submission_is_immutable_and_exact_replay_is_a_noop(fixture):
    values, contract = fixture
    service = _service()
    first = _submit(service, values, contract)
    counts = service.store.counts()
    replay = _submit(service, values, contract)
    assert first.proposal_id == replay.proposal_id
    assert replay.replay is True and replay.status == 200
    assert service.store.counts() == counts
    service.store.set_now(datetime(2026, 8, 26, 16, 0, tzinfo=timezone.utc))
    late_replay = _submit(service, values, contract)
    assert late_replay.replay is True
    assert late_replay.proposal_id == first.proposal_id
    assert service.store.counts() == counts
    with pytest.raises(ExternalActionError) as error:
        service.submit(
            deployment="support-prod", actor=Actor("oidc:automation.example:service:triage-bot", "subject:automation:triage-bot"),
            idempotency_key="propose-triage-run-417", automation_id="ticket-triage", automation_version="3.2.0",
            run_id="run-417", correlation_id="incident-99", action_type="support.ticket.update/v1", target_ref="ticket:T-42",
            risk_tier="high", action_schema_digest=values["action_schema"]["digest"], action_contract=contract,
            requested_review_expires_at="2026-08-26T15:30:00.000000Z", summary_code="resolve_after_deploy",
            payload={"ticket": "T-42", "status": "pending", "comment": "changed"}, evidence_digests=["sha256:" + "a" * 64],
        )
    assert error.value.code == "idempotency_conflict"


def test_submit_replay_uses_retained_contract_after_registry_rotation(fixture):
    values, contract = fixture
    service = _service()
    service.action_contracts = {contract.action_type: contract}
    first = _submit(service, values, contract)

    # The deployment registry no longer exposes the original action type.
    # The committed key must still converge using the proposal's retained
    # contract record; a new key would still fail closed at _contract().
    service.action_contracts = {"other.action/v1": contract}
    replay = _submit(service, values, contract.to_mapping())
    assert replay.replay is True
    assert replay.proposal_id == first.proposal_id


def test_edit_approve_claim_and_first_outcome_win(fixture):
    values, contract = fixture
    service = _service()
    proposal = _submit(service, values, contract)
    reviewer = Actor("oidc:login.example:user:operator-7", "subject:human:operator-7", "auth:review", "nomos:review")
    edited = service.edit(proposal.proposal_id, actor=reviewer, idempotency_key="operator-7-edit-417",
                          replacement_payload=values["payload_edited"]["value"], action_contract=contract,
                          if_match=proposal.value.etag)
    approved = service.approve(edited.proposal_id, actor=reviewer, idempotency_key="operator-7-approve-2",
                               action_contract=contract, execute_before="2026-08-26T15:00:00.000000Z",
                               reason_code="verified_nonterminal_update", if_match=edited.value.etag)
    worker = Actor("mtls:workers.example:ticket-worker-1", "subject:worker:ticket-service", "auth:worker", "nomos:worker")
    claim_kwargs = dict(
        actor=worker, idempotency_key="ticket-worker-1-claim-417", decision_id=approved.current_decision_id,
        approved_lifecycle_version=approved.lifecycle_version, approved_payload_version=approved.payload_version,
        approved_payload_hash=approved.current_payload_hash, supported_action_schema_digest=contract.action_schema_digest,
        if_match=approved.value.etag,
        supported_action_contract_digest=contract.action_contract_digest, supported_executor_contract_digest=contract.executor_contract_digest,
        supported_payload_renderer_digest=contract.payload_renderer_digest, supported_governed_tool_binding_digest=contract.governed_tool_binding_digest,
        supported_provider_connection_contract_digest=contract.provider_connection_contract_digest,
        supported_provider_capability_digest=contract.provider_capability_digest, supported_provider_operation_id=contract.provider_operation_id,
        supported_provider_operation_descriptor_digest=contract.provider_operation_descriptor_digest,
    )
    claim = service.claim(approved.proposal_id, **claim_kwargs)
    assert claim.canonical_payload == values["payload_edited"]["canonical_jcs"].encode()
    replay = service.claim(approved.proposal_id, **claim_kwargs)
    assert replay.replay is True and replay.execution_id == claim.execution_id
    outcome = service.report_outcome(claim.execution_id, actor=worker, idempotency_key="worker-outcome-417",
                                     outcome="succeeded", outcome_reason_code="provider_receipt_observed",
                                     provider_receipt_digest="sha256:" + "c" * 64,
                                     recovery_journal_digest="sha256:" + "d" * 64)
    assert outcome.outcome == "succeeded"
    with pytest.raises(ExternalActionError) as error:
        service.report_outcome(claim.execution_id, actor=worker, idempotency_key="worker-outcome-conflict",
                               outcome="failed", outcome_reason_code="provider_explicit_failure",
                               provider_receipt_digest="sha256:" + "e" * 64,
                               recovery_journal_digest="sha256:" + "f" * 64)
    assert error.value.code == "outcome_already_recorded"


def test_outcome_if_match_is_checked_and_bound_into_request_hash(fixture):
    values, contract = fixture
    service = _service()
    proposal = _submit(service, values, contract)
    reviewer = Actor("oidc:login.example:user:operator-7", "subject:human:operator-7", "auth:review", "nomos:review")
    approved = service.approve(
        proposal.proposal_id,
        actor=reviewer,
        idempotency_key="operator-7-approve-outcome-417",
        action_contract=contract,
        execute_before="2026-08-26T15:00:00.000000Z",
        reason_code="verified_nonterminal_update",
        if_match=proposal.value.etag,
    )
    worker = Actor("mtls:workers.example:ticket-worker-1", "subject:worker:ticket-service", "auth:worker", "nomos:worker")
    claim_kwargs = dict(
        actor=worker,
        idempotency_key="ticket-worker-outcome-claim-417",
        decision_id=approved.current_decision_id,
        approved_lifecycle_version=approved.lifecycle_version,
        approved_payload_version=approved.payload_version,
        approved_payload_hash=approved.current_payload_hash,
        supported_action_schema_digest=contract.action_schema_digest,
        supported_action_contract_digest=contract.action_contract_digest,
        supported_executor_contract_digest=contract.executor_contract_digest,
        supported_payload_renderer_digest=contract.payload_renderer_digest,
        supported_governed_tool_binding_digest=contract.governed_tool_binding_digest,
        supported_provider_connection_contract_digest=contract.provider_connection_contract_digest,
        supported_provider_capability_digest=contract.provider_capability_digest,
        supported_provider_operation_id=contract.provider_operation_id,
        supported_provider_operation_descriptor_digest=contract.provider_operation_descriptor_digest,
        if_match=approved.value.etag,
    )
    claim = service.claim(approved.proposal_id, **claim_kwargs)
    # The approval ETag is stale as soon as the claim advances lifecycle.
    with pytest.raises(ExternalActionError) as stale:
        service.report_outcome(
            claim.execution_id,
            actor=worker,
            idempotency_key="worker-outcome-stale-417",
            outcome="succeeded",
            outcome_reason_code="provider_receipt_observed",
            provider_receipt_digest="sha256:" + "c" * 64,
            recovery_journal_digest="sha256:" + "d" * 64,
            if_match=approved.value.etag,
        )
    assert stale.value.code == "stale_proposal_lifecycle"

    current_etag = service.store.get(claim.proposal_id).etag
    outcome = service.report_outcome(
        claim.execution_id,
        actor=worker,
        idempotency_key="worker-outcome-bound-417",
        outcome="succeeded",
        outcome_reason_code="provider_receipt_observed",
        provider_receipt_digest="sha256:" + "c" * 64,
        recovery_journal_digest="sha256:" + "d" * 64,
        if_match=current_etag,
    )
    assert outcome.value.outcome_request_hash == outcome_request_hash(
        {
            "execution_id": claim.execution_id,
            "effect_instance_key": proposal.effect_instance_key,
            "worker_or_reconciler_separation_subject": worker.separation_subject,
            "outcome": "succeeded",
            "outcome_reason_code": "provider_receipt_observed",
            "provider_receipt_digest": "sha256:" + "c" * 64,
            "recovery_journal_digest": "sha256:" + "d" * 64,
            "owner_unavailability_attestation_digest": None,
            "if_match": current_etag,
        }
    )


def test_durable_claim_hash_and_payload_come_from_the_closed_contract_rpc(fixture):
    values, contract = fixture
    service = _service()
    proposal = _submit(service, values, contract)
    reviewer = Actor("oidc:login.example:user:operator-7", "subject:human:operator-7", "auth:review", "nomos:review")
    approved = service.approve(
        proposal.proposal_id,
        actor=reviewer,
        idempotency_key="operator-7-approve-durable-417",
        action_contract=contract,
        execute_before="2026-08-26T15:00:00.000000Z",
        reason_code="verified_nonterminal_update",
        if_match=proposal.value.etag,
    )
    worker = Actor("mtls:workers.example:ticket-worker-1", "subject:worker:ticket-service", "auth:worker", "nomos:worker")
    store = service.store
    store.is_durable = True
    store.deployment = "support-prod"
    store.execution_epoch_lease_digest = "sha256:" + "e" * 64
    store.restore_fences = {"support-prod": {"service_mode": "normal", "quarantine_state": "clear", "highest_accepted_epoch": 17}}
    calls = {}
    store.get_payload = lambda *_args, **_kwargs: pytest.fail("durable claim must not read payload from the snapshot")
    store.prepare_claim = lambda request: {
        "claimed_at": "2026-08-26T14:01:00.000000Z",
        "previous_event_hash": service.store.events[proposal.effect_instance_key][-1].event_hash,
        "decision_event_hash": service.store.events[proposal.effect_instance_key][-1].event_hash,
        "claim_event_hash": "sha256:" + "f" * 64,
        "event_chain_head_hash": "sha256:" + "f" * 64,
        "claim_lifecycle_version": approved.lifecycle_version + 1,
        "event_sequence": len(service.store.events[proposal.effect_instance_key]) + 1,
    }

    def claim_command(command):
        calls["command"] = command
        return {
            "replay": False,
            "status": 201,
            "proposal_id": proposal.proposal_id,
            "execution_id": "01J00000000000000000000001",
            "canonical_payload": values["payload_original"]["canonical_jcs"].encode(),
        }

    store.claim_command = claim_command
    service._authorize = lambda _operation, actor, *, context=None: actor
    service._handoff_envelope = lambda _payload: {
        "protected_b64": "protected",
        "payload_b64": "payload",
        "signature_b64": "signature",
        "envelope_digest": "sha256:" + "a" * 64,
    }
    claim = service.claim(
        proposal.proposal_id,
        actor=worker,
        idempotency_key="ticket-worker-durable-claim-417",
        decision_id=approved.current_decision_id,
        approved_lifecycle_version=approved.lifecycle_version,
        approved_payload_version=approved.payload_version,
        approved_payload_hash=approved.current_payload_hash,
        supported_action_schema_digest=contract.action_schema_digest,
        supported_action_contract_digest=contract.action_contract_digest,
        supported_executor_contract_digest=contract.executor_contract_digest,
        supported_payload_renderer_digest=contract.payload_renderer_digest,
        supported_governed_tool_binding_digest=contract.governed_tool_binding_digest,
        supported_provider_connection_contract_digest=contract.provider_connection_contract_digest,
        supported_provider_capability_digest=contract.provider_capability_digest,
        supported_provider_operation_id=contract.provider_operation_id,
        supported_provider_operation_descriptor_digest=contract.provider_operation_descriptor_digest,
        if_match=approved.value.etag,
    )
    support = {
        "worker_separation_subject": worker.separation_subject,
        "decision_id": approved.current_decision_id,
        "approved_lifecycle_version": approved.lifecycle_version,
        "approved_payload_version": approved.payload_version,
        "approved_payload_hash": approved.current_payload_hash,
        "supported_action_schema_digest": contract.action_schema_digest,
        "supported_action_contract_digest": contract.action_contract_digest,
        "supported_executor_contract_digest": contract.executor_contract_digest,
        "supported_payload_renderer_digest": contract.payload_renderer_digest,
        "supported_governed_tool_binding_digest": contract.governed_tool_binding_digest,
        "supported_provider_connection_contract_digest": contract.provider_connection_contract_digest,
        "supported_provider_capability_digest": contract.provider_capability_digest,
        "supported_provider_operation_id": contract.provider_operation_id,
        "supported_provider_operation_descriptor_digest": contract.provider_operation_descriptor_digest,
        "if_match": approved.value.etag,
    }
    assert calls["command"].request["request_hash"] == transition_request_hash(support)
    assert calls["command"].request["if_match"] == approved.value.etag
    assert all(
        calls["command"].request[field] == value
        for field, value in support.items()
        if field.startswith("supported_")
    )
    assert claim.canonical_payload == values["payload_original"]["canonical_jcs"]


def test_transition_replay_uses_retained_contract_after_registry_rotation(fixture):
    values, contract = fixture
    service = _service()
    service.action_contracts = {contract.action_type: contract}
    proposal = _submit(service, values, contract)
    reviewer = Actor("oidc:login.example:user:operator-7", "subject:human:operator-7", "auth:review", "nomos:review")
    first = service.edit(
        proposal.proposal_id,
        actor=reviewer,
        idempotency_key="operator-7-edit-replay-417",
        replacement_payload=values["payload_edited"]["value"],
        action_contract=contract,
        if_match=proposal.value.etag,
    )

    service.action_contracts = {"other.action/v1": contract}
    replay = service.edit(
        proposal.proposal_id,
        actor=reviewer,
        idempotency_key="operator-7-edit-replay-417",
        replacement_payload=values["payload_edited"]["value"],
        action_contract=contract.to_mapping(),
        if_match=proposal.value.etag,
    )
    assert replay.replay is True
    assert replay.proposal_id == first.proposal_id


def test_proposer_cannot_approve_and_expiry_is_materialized(fixture):
    values, contract = fixture
    service = _service()
    proposal = _submit(service, values, contract)
    with pytest.raises(ExternalActionError) as error:
        service.approve(proposal.proposal_id, actor=Actor("oidc:automation.example:service:triage-bot", "subject:automation:triage-bot"),
                        idempotency_key="proposer-self-approve", action_contract=contract,
                        execute_before="2026-08-26T15:00:00.000000Z", reason_code="verified_nonterminal_update",
                        if_match=proposal.value.etag)
    assert error.value.code == "proposer_approver_separation_required"
    service.store.set_now(datetime(2026, 8, 26, 15, 31, tzinfo=timezone.utc))
    expired = service.read(proposal.proposal_id)
    assert expired.review_state == "expired"
    assert service.store.counts()["events"] == 2


class _DurableCommandDouble:
    """Small SQL-RPC seam double; it deliberately has no dict write path."""

    is_durable = True
    deployment = "support-prod"
    execution_epoch = 17
    execution_epoch_lease_digest = "sha256:" + "e" * 64
    policy_digest = "sha256:" + "3" * 64
    connection = object()
    nomos_storage_adapter = object()

    def __init__(self):
        self.lock = threading.RLock()
        self.calls = []

    @staticmethod
    def _authority(**kwargs):
        return {
            "deployment": kwargs["deployment"],
            "authentication_audit_id": kwargs["authentication_audit_id"],
            "nomos_decision_id": kwargs["nomos_decision_id"],
            "nomos_provider_id": "provider",
            "nomos_subject_binding_id": "binding",
            "separation_subject": "subject:automation:triage-bot",
            "policy_digest": "sha256:" + "4" * 64,
            "action": kwargs["expected_action"],
            "effect": "allow",
            "provider_status": "active",
            "binding_status": "active",
            "provider_updated_at": "2026-08-26T14:00:00.000000Z",
            "binding_updated_at": "2026-08-26T14:00:00.000000Z",
            "provider_credential_generation_digest": "generation-1",
            "binding_credential_generation_digest": "generation-1",
        }

    def verify_nomos_authority(self, **kwargs):
        return self._authority(**kwargs)

    def bind_nomos_identity(self, **_kwargs):
        return None

    def begin_verifier_authorization(self, **kwargs):
        self.calls.append(("verifier_assertion", kwargs))

    def now(self):
        return datetime(2026, 8, 26, 14, tzinfo=timezone.utc)

    def record_auth_audit(self, audit):
        self.calls.append(("auth_audit", audit))
        return {"authentication_audit_id": "auth-audit-1"}

    def submit_command(self, command):
        assert isinstance(command, SubmitCommand)
        self.calls.append(("submit", command))
        return {"proposal_id": command.request["proposal_id"], "status": 201}

    def get(self, proposal_id):
        return None


def test_durable_submit_uses_typed_rpc_and_same_transaction_authority(fixture):
    values, contract = fixture
    store = _DurableCommandDouble()
    seen = {}

    def authorize(operation, actor, **kwargs):
        seen.update(operation=operation, transaction=kwargs["transaction"], actor=actor)
        return {"allowed": True, "effect": "allow", "obligations_performed": True,
                "decision_id": "nomos-decision-1", "policy_digest": "sha256:" + "4" * 64}

    service = ExternalActionService(store, authorize=authorize)
    result = service.submit(
        deployment="support-prod", actor=Actor(
            "oidc:automation.example:service:triage-bot",
            "subject:automation:triage-bot",
            nomos_provider_id="provider",
            nomos_subject_binding_id="binding",
            authentication_audit={
                "nomos_provider_id": "provider", "nomos_subject_binding_id": "binding",
                "authentication_method_code": "jwt_dpop",
                "sender_key_or_certificate_thumbprint": "thumb",
                "issuer_or_trust_domain_digest": "sha256:" + "a" * 64,
                "provider_status": "active", "binding_status": "active",
                "provider_updated_at": "2026-08-26T14:00:00.000000Z",
                "binding_updated_at": "2026-08-26T14:00:00.000000Z",
                "provider_credential_generation_digest": "generation-1",
                "binding_credential_generation_digest": "generation-1",
                "token_issued_at": "2026-08-26T14:00:01.000000Z",
                "credential_generation_digest": "generation-1",
            },
        ),
        idempotency_key="propose-triage-run-417", automation_id="ticket-triage", automation_version="3.2.0",
        run_id="run-417", correlation_id="incident-99", action_type="support.ticket.update/v1", target_ref="ticket:T-42",
        risk_tier="high", action_schema_digest=values["action_schema"]["digest"], action_contract=contract,
        requested_review_expires_at="2026-08-26T15:30:00.000000Z", summary_code="resolve_after_deploy",
        payload=values["payload_original"]["value"], evidence_digests=["sha256:" + "a" * 64],
    )
    assert result.status == 201 and result.value["proposal_id"]
    assert seen["operation"] == "external_action.propose"
    assert seen["transaction"] is store.connection
    command = next(value for name, value in store.calls if name == "submit")
    assert [name for name, _ in store.calls] == [
        "verifier_assertion",
        "auth_audit",
        "submit",
    ]
    verifier = store.calls[0][1]
    assert verifier["operation"] == "external_action.propose"
    assert verifier["transition_digest"] == command.request["submission_request_hash"]
    assert next(value for name, value in store.calls if name == "auth_audit")["nomos_decision_id"] == "nomos-decision-1"
    assert command.authorization.authentication_audit_id == "auth-audit-1"
    assert command.authorization.nomos_decision_id == "nomos-decision-1"
    assert command.request["action_contract_record"] == contract.to_mapping()


def test_durable_authorization_preserves_deferred_audit_request_id():
    store = _DurableCommandDouble()

    def authorize(operation, actor, **kwargs):
        return {
            "allowed": True,
            "effect": "allow",
            "obligations_performed": True,
            "decision_id": "nomos-decision-1",
            "policy_digest": "sha256:" + "4" * 64,
        }

    service = ExternalActionService(store, authorize=authorize)
    actor = Actor(
        "oidc:automation.example:service:triage-bot",
        "subject:automation:triage-bot",
        nomos_provider_id="provider",
        nomos_subject_binding_id="binding",
        authentication_audit={
            "request_id": "http-request-417",
            "auth_config_digest": "sha256:" + "a" * 64,
            "nomos_provider_id": "provider", "nomos_subject_binding_id": "binding",
            "authentication_method_code": "jwt_dpop",
            "sender_key_or_certificate_thumbprint": "thumb",
            "issuer_or_trust_domain_digest": "sha256:" + "a" * 64,
            "provider_status": "active", "binding_status": "active",
            "provider_updated_at": "2026-08-26T14:00:00.000000Z",
            "binding_updated_at": "2026-08-26T14:00:00.000000Z",
            "provider_credential_generation_digest": "generation-1",
            "binding_credential_generation_digest": "generation-1",
            "token_issued_at": "2026-08-26T14:00:01.000000Z",
            "credential_generation_digest": "generation-1",
        },
    )

    service._authorize(
        "external_action.propose",
        actor,
        context={"deployment": "support-prod", "request_hash": "sha256:" + "b" * 64},
    )

    audit = next(value for name, value in store.calls if name == "auth_audit")
    assert audit["request_id"] == "http-request-417"


def test_postgres_startup_lease_command_carries_verified_signer_digest_and_chain():
    class LeaseBucket(list):
        def _load(self, value):
            self.append(value)

    store = object.__new__(PostgresExternalActionStore)
    store.leases = defaultdict(LeaseBucket)
    captured = {}

    def typed(function, request):
        captured.update(function=function, request=dict(request))
        return {"replay": False}

    store._typed = typed
    lease = SimpleNamespace(
        payload={
            "deployment": "support-prod",
            "epoch": 17,
            "lease_sequence": 1,
            "previous_lease_digest": None,
            "previous_epoch_digest": None,
            "issued_at": "2026-08-26T14:00:00.000000Z",
            "expires_at": "2026-08-26T15:00:00.000000Z",
            "signer_separation_subject": "subject:lease-signer",
            "nonce": "lease-nonce",
        },
        envelope={
            "protected_b64": "protected",
            "payload_b64": "payload",
            "signature_b64": "signature",
        },
        envelope_digest="sha256:" + "e" * 64,
    )
    signer_digest = "sha256:" + "f" * 64
    chain_digest = "sha256:" + "a" * 64

    store.insert_lease(
        lease=lease,
        signer_key_id_digest=signer_digest,
        verification_source="signed_startup_gate",
        verification_config_digest="sha256:" + "b" * 64,
        verification_login_binding_digest="sha256:" + "c" * 64,
        lease_chain_digest=chain_digest,
    )

    assert captured["function"] == "external_action_store_insert_lease"
    assert captured["request"]["signer_key_id_digest"] == signer_digest
    assert captured["request"]["lease_chain_digest"] == chain_digest
    assert "authentication_audit_id" not in captured["request"]
    assert "nomos_decision_id" not in captured["request"]
    assert store.leases["support-prod"] == [lease]

    with pytest.raises(ExternalActionStoreError, match="signer key id digest"):
        store.insert_lease(lease=SimpleNamespace(**{**lease.__dict__, "envelope_digest": "sha256:" + "d" * 64}))


def test_durable_service_refuses_missing_authority(fixture):
    values, contract = fixture
    store = _DurableCommandDouble()
    service = ExternalActionService(store)
    with pytest.raises(ExternalActionError) as error:
        service.submit(
            deployment="support-prod", actor=Actor("oidc:automation.example:service:triage-bot", "subject:automation:triage-bot"),
            idempotency_key="propose-triage-run-417", automation_id="ticket-triage", automation_version="3.2.0",
            run_id="run-417", correlation_id="incident-99", action_type="support.ticket.update/v1", target_ref="ticket:T-42",
            risk_tier="high", action_schema_digest=values["action_schema"]["digest"], action_contract=contract,
            requested_review_expires_at="2026-08-26T15:30:00.000000Z", summary_code="resolve_after_deploy",
            payload=values["payload_original"]["value"], evidence_digests=["sha256:" + "a" * 64],
        )
    assert error.value.code == "review_authority_unavailable"


def test_durable_record_result_preserves_rpc_response_bytes() -> None:
    service = ExternalActionService(InMemoryExternalActionStore())
    body = b'{"review_state":"pending","lifecycle_version":1}'
    result = service._durable_record_result(
        {"replay": True, "response_status": 200, "response_body_jcs": body},
        {"review_state": "approved"},
        status=201,
    )
    assert result.replay is True
    assert result.response_body == body


def test_durable_nomos_authorizer_adapts_http_callback_to_store_boundary(monkeypatch):
    connection = object()
    adapter = object()
    seen = {}
    store = SimpleNamespace(
        connection=connection,
        deployment="support-prod",
        nomos_storage_adapter=adapter,
        bind_nomos_identity=lambda **kwargs: seen.update(identity=kwargs),
    )

    def evaluate(connection_value, **kwargs):
        seen.update(connection=connection_value, **kwargs)
        decision = SimpleNamespace(decision_id="nomos-1", policy_fingerprint="sha256:" + "a" * 64)
        return SimpleNamespace(decision=decision, allowed=True, final_effect="allow", missing_obligations=())

    import okg.substrate.nomos.store as nomos_store

    monkeypatch.setattr(nomos_store, "evaluate_runtime_boundary_with_active_policy", evaluate)
    authorizer = DurableNomosAuthorizer(store)
    result = authorizer(
        "external_action.propose",
        Actor("principal:proposer", "subject:proposer", nomos_provider_id="provider-1", nomos_subject_binding_id="binding-1"),
        transaction=connection,
        context={"deployment": "support-prod", "request_hash": "sha256:" + "b" * 64},
    )
    assert result == {
        "allowed": True,
        "effect": "allow",
        "obligations_performed": True,
        "decision_id": "nomos-1",
        "policy_digest": "sha256:" + "a" * 64,
    }
    assert seen["connection"] is connection
    assert seen["require_active"] is True
    assert seen["enforcement_mode"] == "enforce"
    assert seen["storage_adapter"] is adapter


def test_postgres_policy_rpc_is_parsed_and_carries_identity_barrier():
    store = object.__new__(PostgresExternalActionStore)
    connection = object()
    store._conn = connection
    store.deployment = "support-prod"
    store._nomos_provider_id = "provider-1"
    store._nomos_subject_binding_id = "binding-1"
    store._nomos_authority = None
    seen = {}
    fingerprint = "sha256:" + "a" * 64
    store._verifier_call_in_transaction = lambda function, params: seen.update(function=function, params=params) or {
        "policy_bundle_id": "bundle-1",
        "policy_version": "1",
        "policy_fingerprint": fingerprint,
        "provider_id": "provider-1",
        "provider_status": "active",
        "provider_updated_at": "2026-08-26T14:00:00Z",
        "binding_id": "binding-1",
        "binding_status": "active",
        "binding_updated_at": "2026-08-26T14:00:00Z",
        "authority_barrier": {"policy_fingerprint": fingerprint},
        "bundle": {
            "policy_bundle_id": "bundle-1",
            "policy_version": "1",
            "policy_fingerprint": fingerprint,
            "compiler_version": "test",
            "source_generation_id": None,
            "scope": {},
            "default_effect": "deny",
            "reason_code_catalog": {},
            "effective_from": None,
            "effective_until": None,
            "compiled_at": "2026-08-26T14:00:00Z",
            "rules": [{"rule_id": "allow-read", "effect": "allow", "actions": ["graph.read"]}],
        },
    }
    bundle = store.load_active_policy_bundle(connection, deployment_name="support-prod", scope="external_action")
    assert bundle.policy_bundle_id == "bundle-1"
    assert seen == {"function": "external_action_nomos_policy_bundle", "params": ("external_action", "provider-1", "binding-1")}
    assert store._nomos_authority["policy_fingerprint"] == fingerprint


def test_typed_commands_emit_only_the_role_specific_rpc_envelope():
    actor = Actor("principal:worker", "subject:worker")
    authorization = AuthorizationRefs("audit-1", "decision-1", "sha256:" + "a" * 64)
    cases = (
        SubmitCommand({"deployment": "support-prod"}, actor, authorization).to_mapping(),
        ReviewTransitionCommand({"deployment": "support-prod"}, actor, authorization).to_mapping(),
        ClaimCommand({"deployment": "support-prod"}, actor, authorization).to_mapping(),
        OutcomeCommand({"deployment": "support-prod"}, actor, authorization).to_mapping(),
        OwnerUnavailabilityCommand({"deployment": "support-prod"}, actor, authorization).to_mapping(),
    )
    assert all("actor" not in command for command in cases)
    assert cases[0]["proposer_principal"] == actor.principal
    assert cases[1]["actor_separation_subject"] == actor.separation_subject
    assert cases[2]["worker_principal"] == actor.principal
    assert cases[3]["worker_or_reconciler_separation_subject"] == actor.separation_subject
    assert cases[4]["claim_owner_separation_subject"] == actor.separation_subject
    # The owner-attestation SQL function deliberately has no policy_digest
    # argument; authorization still carries the auth/Nomos pair.
    assert "policy_digest" not in cases[4]
    owner = OwnerUnavailabilityCommand(
        {
            "claim_owner_separation_subject": "subject:unavailable-worker",
            "authenticated_submitter_separation_subject": "subject:reconciler",
        },
        actor,
        authorization,
    ).to_mapping()
    assert owner["claim_owner_separation_subject"] == "subject:unavailable-worker"
    assert owner["authenticated_submitter_separation_subject"] == "subject:reconciler"


_EXECUTE_BEFORE = "2026-08-26T15:00:00.000000Z"
_REVIEWER = Actor(
    "oidc:login.example:user:operator-7", "subject:human:operator-7",
    "auth:review", "nomos:review",
)
_WORKER = Actor(
    "mtls:workers.example:ticket-worker-1", "subject:worker:ticket-service",
    "auth:worker", "nomos:worker",
)


def _approve_with_execution_deadline(service, values, contract, *, key: str):
    """Submit and approve one proposal whose execution deadline is 15:00Z.

    The review deadline stays at 15:30Z, so a clock between the two can only
    reach the ``execute_before`` branch of expiry materialization.
    """

    proposal = _submit(service, values, contract)
    return service.approve(
        proposal.proposal_id, actor=_REVIEWER, idempotency_key=key,
        action_contract=contract, execute_before=_EXECUTE_BEFORE,
        reason_code="verified_nonterminal_update", if_match=proposal.value.etag,
    )


def _claim_kwargs(approved, contract, *, key: str, if_match: str):
    return dict(
        actor=_WORKER, idempotency_key=key, decision_id=approved.current_decision_id,
        approved_lifecycle_version=approved.lifecycle_version,
        approved_payload_version=approved.payload_version,
        approved_payload_hash=approved.current_payload_hash,
        supported_action_schema_digest=contract.action_schema_digest,
        supported_action_contract_digest=contract.action_contract_digest,
        supported_executor_contract_digest=contract.executor_contract_digest,
        supported_payload_renderer_digest=contract.payload_renderer_digest,
        supported_governed_tool_binding_digest=contract.governed_tool_binding_digest,
        supported_provider_connection_contract_digest=contract.provider_connection_contract_digest,
        supported_provider_capability_digest=contract.provider_capability_digest,
        supported_provider_operation_id=contract.provider_operation_id,
        supported_provider_operation_descriptor_digest=contract.provider_operation_descriptor_digest,
        if_match=if_match,
    )


def _event_types(service, proposal_id) -> list[str]:
    return [event["event_type"] for event in service.timeline(proposal_id)]


def test_execution_deadline_expiry_refuses_the_claim_and_is_recorded_once(fixture):
    """An approved proposal whose ``execute_before`` elapsed is never executed.

    The review deadline has not elapsed, so ``review_expired`` must not fire;
    the timeline must carry exactly one ``approval_expired`` event bound to the
    ``execute_before`` deadline, and a repeated claim must add no second one.
    """

    values, contract = fixture
    service = _service()
    approved = _approve_with_execution_deadline(
        service, values, contract, key="operator-7-approve-execution-deadline",
    )
    assert approved.review_state == "approved" and approved.execution_state == "ready"

    # Deterministic clock: past the 15:00Z execution deadline, short of the
    # 15:30Z review deadline.  Nothing in this test sleeps.
    service.store.set_now(datetime(2026, 8, 26, 15, 1, tzinfo=timezone.utc))

    # The worker still holds the approval ETag, which the expiry transition
    # invalidates as it bumps the lifecycle version.  This particular code is
    # order-sensitive: claim() materializes expiry, then checks If-Match, then
    # checks the review state.  Reordering those two checks would legitimately
    # turn this into proposal_expired; that is not the regression this test
    # guards.  The invariants that matter are the event assertions below.
    with pytest.raises(ExternalActionError) as stale:
        service.claim(
            approved.proposal_id,
            **_claim_kwargs(approved, contract, key="worker-claim-after-deadline",
                            if_match=approved.value.etag),
        )
    assert stale.value.code == "stale_proposal_lifecycle"

    expired = service.store.get(approved.proposal_id)
    assert expired.review_state == "expired"
    assert expired.execution_state == "unavailable"
    assert expired.lifecycle_version == approved.lifecycle_version + 1

    timeline = service.timeline(approved.proposal_id)
    deadline_events = [
        event for event in timeline if event["event_type"] == "approval_expired"
    ]
    assert len(deadline_events) == 1
    event = deadline_events[0]
    assert event["deadline_kind"] == "execute_before"
    assert event["effective_at"] == _EXECUTE_BEFORE
    assert event["actor_kind"] == "system_clock"
    assert event["before_review_state"] == "approved"
    assert event["after_review_state"] == "expired"
    assert event["before_execution_state"] == "ready"
    assert event["after_execution_state"] == "unavailable"
    assert event["decision_id"] == approved.current_decision_id
    assert "review_expired" not in _event_types(service, approved.proposal_id)

    # A worker that re-reads the proposal is refused for the expiry itself.
    with pytest.raises(ExternalActionError) as refused:
        service.claim(
            approved.proposal_id,
            **_claim_kwargs(approved, contract, key="worker-claim-after-deadline-2",
                            if_match=expired.etag),
        )
    assert refused.value.code == "proposal_expired"
    assert refused.value.status == 410

    # A second worker identity is refused the same way and, because expiry is
    # a property of the proposal rather than of one claimant, still adds no
    # second event.
    second_worker = Actor(
        "mtls:workers.example:ticket-worker-2", "subject:worker:ticket-service-2",
        "auth:worker", "nomos:worker",
    )
    with pytest.raises(ExternalActionError) as other:
        service.claim(
            approved.proposal_id,
            **{
                **_claim_kwargs(approved, contract, key="worker-2-claim-after-deadline",
                                if_match=expired.etag),
                "actor": second_worker,
            },
        )
    assert other.value.code == "proposal_expired"

    # Expiry is recorded once: the repeated claims added no second event.
    assert _event_types(service, approved.proposal_id).count("approval_expired") == 1
    assert service.store.counts()["executions"] == 0
    assert service.store.counts()["outcomes"] == 0
    assert service.store.get_execution(approved.proposal_id) is None


def test_claim_before_the_execution_deadline_executes_and_survives_it(fixture):
    """Control: the same proposal claimed before 15:00Z still executes.

    A claimed proposal has left the ``ready`` execution state, so a later clock
    past ``execute_before`` must not expire work already handed to a worker.
    """

    values, contract = fixture
    service = _service()
    approved = _approve_with_execution_deadline(
        service, values, contract, key="operator-7-approve-execution-control",
    )
    service.store.set_now(datetime(2026, 8, 26, 14, 59, tzinfo=timezone.utc))
    claim = service.claim(
        approved.proposal_id,
        **_claim_kwargs(approved, contract, key="worker-claim-before-deadline",
                        if_match=approved.value.etag),
    )
    assert claim.canonical_payload == values["payload_original"]["canonical_jcs"].encode()
    assert service.store.get_execution(approved.proposal_id) is not None
    assert "approval_expired" not in _event_types(service, approved.proposal_id)

    service.store.set_now(datetime(2026, 8, 26, 15, 1, tzinfo=timezone.utc))
    outcome = service.report_outcome(
        claim.execution_id, actor=_WORKER, idempotency_key="worker-outcome-control",
        outcome="succeeded", outcome_reason_code="provider_receipt_observed",
        provider_receipt_digest="sha256:" + "c" * 64,
        recovery_journal_digest="sha256:" + "d" * 64,
    )
    assert outcome.outcome == "succeeded"
    assert "approval_expired" not in _event_types(service, approved.proposal_id)
