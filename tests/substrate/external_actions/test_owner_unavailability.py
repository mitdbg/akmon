from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from okg.substrate.external_actions import Actor, ExternalActionError, ExternalActionService
from okg.substrate.external_actions.action_contracts import ActionContract, format_timestamp
from okg.substrate.external_actions.signing import SigningKey, build_detached_envelope, generate_keypair
from okg.substrate.external_actions.store import InMemoryExternalActionStore


ROOT = Path(__file__).resolve().parents[3]
VECTORS = ROOT / "tests/fixtures/external_actions/hash-vectors.yaml"
ATT_TYP = "application/okg-external-action-owner-unavailability-attestation+jws"
ATT_DOMAIN = "okg-external-action-owner-unavailability-attestation-envelope-v1"


def _lifecycle():
    document = yaml.safe_load(VECTORS.read_text(encoding="utf-8"))
    values = {row["id"]: row for row in document["vectors"]}
    contract = ActionContract.from_mapping(values["action_contract"]["value"])
    private, public = generate_keypair()
    key = SigningKey(
        kid="support-prod/owner-attester/v1",
        purpose="owner_unavailability_attestation",
        algorithm="EdDSA",
        public_key_b64=public,
        state="current",
    )
    provider_calls: list[object] = []

    def identity_status(**request):
        return {
            "authentication_audit_id": request["authentication_audit_id"],
            "unavailable": True,
            "updated_at": "2026-08-27T14:00:01.000000Z",
            "active_replacement": False,
        }

    store = InMemoryExternalActionStore(
        now=datetime(2026, 8, 26, 14, tzinfo=timezone.utc),
        execution_epoch=17,
        execution_epoch_lease_digest="sha256:" + "9" * 64,
        policy_digest="sha256:" + "3" * 64,
    )
    service = ExternalActionService(
        store,
        owner_attestation_keys={key.kid: key},
        owner_attestation_maximum_lifetime_seconds=900,
        claim_owner_status_verifier=identity_status,
    )
    submitted = service.submit(
        deployment="support-prod",
        actor=Actor("proposer", "subject:proposer", "auth:proposer", "nomos:proposer"),
        idempotency_key="owner-attestation-submit-417",
        automation_id="ticket-triage",
        automation_version="3.2.0",
        run_id="run-417",
        correlation_id="incident-99",
        action_type=contract.action_type,
        target_ref="ticket:T-42",
        risk_tier="high",
        action_schema_digest=contract.action_schema_digest,
        action_contract=contract,
        requested_review_expires_at="2026-08-26T15:30:00.000000Z",
        summary_code="resolve_after_deploy",
        payload=values["payload_original"]["value"],
    )
    reviewer = Actor("reviewer", "subject:reviewer", "auth:reviewer", "nomos:reviewer")
    approved = service.approve(
        submitted.proposal_id,
        actor=reviewer,
        idempotency_key="owner-attestation-approve-417",
        action_contract=contract,
        execute_before="2026-08-26T15:00:00.000000Z",
        reason_code="verified_nonterminal_update",
        if_match=submitted.value.etag,
    )
    worker = Actor("worker", "subject:worker", "auth:worker", "nomos:worker")
    claim = service.claim(
        approved.proposal_id,
        actor=worker,
        idempotency_key="owner-attestation-claim-417",
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
    store.advance(days=1)
    return service, store, contract, claim.value, private, key, provider_calls


def _envelope(claim, private: str, key: SigningKey, *, signer: str = "subject:attester", issued_offset: int = 0):
    reconcile_at = datetime(2026, 8, 27, 14, tzinfo=timezone.utc)
    issued_at = reconcile_at + timedelta(seconds=issued_offset)
    return build_detached_envelope(
        {
            "deployment": "support-prod",
            "execution_id": claim.execution_id,
            "claim_owner_separation_subject": claim.worker_separation_subject,
            "reconcile_not_before": format_timestamp(reconcile_at),
            "issued_at": format_timestamp(issued_at),
            "expires_at": format_timestamp(issued_at + timedelta(minutes=10)),
            "nonce": "attestation-nonce-417",
            "signer_separation_subject": signer,
        },
        private,
        kid=key.kid,
        typ=ATT_TYP,
        domain=ATT_DOMAIN,
    )


def test_signed_attestation_and_reconcile_are_bound_without_provider_io() -> None:
    service, store, _, claim, private, key, provider_calls = _lifecycle()
    envelope = _envelope(claim, private, key)
    attester = Actor("attester", "subject:attester", "auth:attester", "nomos:attester")

    stored = service.create_owner_unavailability_attestation(
        execution_id=claim.execution_id,
        actor=attester,
        attestation=envelope,
        idempotency_key="owner-attestation-create-417",
    )
    replay = service.create_owner_unavailability_attestation(
        execution_id=claim.execution_id,
        actor=attester,
        attestation=envelope,
        idempotency_key="owner-attestation-create-417",
    )
    assert replay.replay is True
    assert replay.response_body == stored.response_body

    reconciler = Actor("reconciler", "subject:reconciler", "auth:reconciler", "nomos:reconciler")
    outcome = service.report_outcome(
        claim.execution_id,
        actor=reconciler,
        idempotency_key="owner-reconcile-outcome-417",
        outcome="indeterminate",
        outcome_reason_code="claim_owner_unavailable",
        provider_receipt_digest=None,
        recovery_journal_digest="sha256:" + "d" * 64,
        owner_unavailability_attestation_digest=envelope["envelope_digest"],
    )
    assert outcome.outcome == "indeterminate"
    store.advance(minutes=11)
    replay_outcome = service.report_outcome(
        claim.execution_id,
        actor=reconciler,
        idempotency_key="owner-reconcile-outcome-417",
        outcome="indeterminate",
        outcome_reason_code="claim_owner_unavailable",
        provider_receipt_digest=None,
        recovery_journal_digest="sha256:" + "d" * 64,
        owner_unavailability_attestation_digest=envelope["envelope_digest"],
    )
    assert replay_outcome.replay is True
    assert provider_calls == []


@pytest.mark.parametrize("mutation", ["signature", "owner_signer", "future_issued"])
def test_invalid_attestation_creates_no_record(mutation: str) -> None:
    service, _, _, claim, private, key, _ = _lifecycle()
    envelope = _envelope(
        claim,
        private,
        key,
        signer="subject:worker" if mutation == "owner_signer" else "subject:attester",
        issued_offset=1 if mutation == "future_issued" else 0,
    )
    if mutation == "signature":
        envelope["signature_b64"] = ("A" if envelope["signature_b64"][0] != "A" else "B") + envelope["signature_b64"][1:]
    actor = Actor(
        "attester",
        "subject:worker" if mutation == "owner_signer" else "subject:attester",
        "auth:attester",
        "nomos:attester",
    )
    with pytest.raises(ExternalActionError):
        service.create_owner_unavailability_attestation(
            execution_id=claim.execution_id,
            actor=actor,
            attestation=envelope,
            idempotency_key="owner-attestation-invalid-417",
        )
    assert service.store.attestations == {}


def test_reconciler_must_be_distinct_and_identity_revocation_is_mandatory() -> None:
    service, _, _, claim, private, key, _ = _lifecycle()
    envelope = _envelope(claim, private, key)
    attester = Actor("attester", "subject:attester", "auth:attester", "nomos:attester")
    service.create_owner_unavailability_attestation(
        execution_id=claim.execution_id,
        actor=attester,
        attestation=envelope,
        idempotency_key="owner-attestation-create-418",
    )
    with pytest.raises(ExternalActionError):
        service.report_outcome(
            claim.execution_id,
            actor=attester,
            idempotency_key="owner-reconcile-signer-conflict-418",
            outcome="indeterminate",
            outcome_reason_code="claim_owner_unavailable",
            provider_receipt_digest=None,
            recovery_journal_digest="sha256:" + "f" * 64,
            owner_unavailability_attestation_digest=envelope["envelope_digest"],
        )
    service.claim_owner_status_verifier = None
    with pytest.raises(ExternalActionError) as error:
        service.report_outcome(
            claim.execution_id,
            actor=Actor("reconciler", "subject:reconciler", "auth:reconciler", "nomos:reconciler"),
            idempotency_key="owner-reconcile-outcome-418",
            outcome="indeterminate",
            outcome_reason_code="claim_owner_unavailable",
            provider_receipt_digest=None,
            recovery_journal_digest="sha256:" + "e" * 64,
            owner_unavailability_attestation_digest=envelope["envelope_digest"],
        )
    assert error.value.code == "review_authority_unavailable"
