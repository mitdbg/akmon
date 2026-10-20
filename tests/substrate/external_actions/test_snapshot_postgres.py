"""The durable snapshot reports the committed lifecycle (issue #2341).

``okg.external_action_store_snapshot`` rebuilds a PostgreSQL store's process
state. It used to return each row's ``store_record``: the original submit,
review, claim or outcome command, which no later transition updates. After an
approval a freshly hydrated store therefore saw the proposal as pending at
lifecycle version 1, and the decision record lacked ``lifecycle_version`` and
``payload_hash``, so it did not load at all; nor did the execution record.
These tests drive the real HTTP API and ``ExternalActionService`` on
PostgreSQL and hydrate a fresh store after every step.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from functools import partial
from uuid import uuid4

import psycopg
import pytest

from okg.substrate.external_actions import (
    Actor,
    ExternalActionService,
    InMemoryExternalActionStore,
)
from okg.substrate.external_actions.action_contracts import idempotency_key_hash
from okg.substrate.external_actions.signing import (
    SigningKey,
    build_detached_envelope,
    generate_keypair,
)
from okg.substrate.external_actions.store import PostgresExternalActionStore
from tests.substrate.external_actions import test_run_records_postgres as run_records
from tests.substrate.external_actions.test_run_records_postgres import (  # noqa: F401
    DEPLOYMENT,
    ISSUER,
    SUBJECT,
    run_database,
    run_fixture,
)
from tests.substrate.external_actions.test_store_postgres import (
    _TEST_VERIFIER_CAPABILITY,
)


pytestmark = pytest.mark.postgres

APPROVER = "subject:snapshot-approver"
WORKER = "subject:snapshot-worker"
RECEIPT = "sha256:" + "c" * 64
JOURNAL = "sha256:" + "d" * 64


def _timestamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@pytest.fixture
def lifecycle(run_fixture, monkeypatch):  # noqa: F811
    """A run fixture whose service can sign claims, plus two more subjects."""

    f = run_fixture
    private_key, public_key = generate_keypair()
    key = SigningKey(
        kid="snapshot-okg-review",
        purpose="claim_and_restore_completion",
        algorithm="EdDSA",
        public_key_b64=public_key,
        state="current",
    )

    def sign(payload):
        return build_detached_envelope(
            payload,
            private_key,
            kid=key.kid,
            typ="okg-claim-handoff+jws",
            domain="okg-external-action-claim-handoff-receipt-envelope-v1",
        )

    monkeypatch.setattr(
        run_records,
        "ExternalActionService",
        partial(ExternalActionService, handoff_signer=sign, handoff_verification_key=key),
    )
    f.bindings = {SUBJECT: f.binding}
    with psycopg.connect(f.dsn) as admin:
        for subject in (APPROVER, WORKER):
            binding = "binding-" + uuid4().hex
            admin.execute(
                "INSERT INTO okg.nomos_subject_bindings (deployment_name,binding_id,provider_id,issuer,claim_path,claim_value,subject,scope,status,metadata) VALUES (%s,%s,%s,%s,'sub',%s,%s,'external_action','active',%s::jsonb)",
                (
                    DEPLOYMENT,
                    binding,
                    f.provider,
                    ISSUER,
                    binding,
                    subject,
                    json.dumps({"credential_generation_digest": f.digest}),
                ),
            )
            f.bindings[subject] = binding
    f.policy(
        actions=[
            "external_automation.register",
            "external_automation.read",
            "external_automation.link",
            "external_action.read",
            "external_action.propose",
            "external_action.decide",
            "external_action.execute",
            "external_action.report",
        ],
        subjects=[SUBJECT, APPROVER, WORKER],
    )
    return f


def _as(f, subject, client, method, path, body, *, if_match=None):
    """Send one signed request as ``subject``."""

    f.binding = f.bindings[subject]
    try:
        headers = f.headers(method, "https://run-api.example" + path)
    finally:
        f.binding = f.bindings[SUBJECT]
    headers["Idempotency-Key"] = "snapshot-" + uuid4().hex
    if if_match is not None:
        headers["If-Match"] = if_match
    return client.request(method, path, json=body, headers=headers)


def _hydrated(f, proposal_id):
    """Rebuild a fresh store from the snapshot RPC and read one proposal."""

    store = PostgresExternalActionStore(
        f.runtime_dsn,
        deployment=DEPLOYMENT,
        verifier_capability=_TEST_VERIFIER_CAPABILITY,
    )
    try:
        with store.lock:
            proposal = store.get(proposal_id)
            decision = store.get_decision(proposal_id)
            execution = store.get_execution(proposal_id)
            outcome = (
                store.get_outcome(execution.execution_id)
                if execution is not None
                else None
            )
    finally:
        store.connection.close()
    return proposal, decision, execution, outcome


def _columns(f, proposal_id):
    with psycopg.connect(f.dsn) as observer:
        return observer.execute(
            "SELECT review_state, execution_state, lifecycle_version,"
            " payload_version, current_payload_hash, current_decision_id"
            " FROM okg.external_action_proposals"
            " WHERE deployment_name = %s AND proposal_id = %s",
            (DEPLOYMENT, proposal_id),
        ).fetchone()


def _state(proposal):
    return (
        proposal.review_state,
        proposal.execution_state,
        proposal.lifecycle_version,
        proposal.payload_version,
    )


def _in_memory_states(values, contract):
    """The same submit, approve, claim and outcome on the in-memory store."""

    service = ExternalActionService(
        InMemoryExternalActionStore(
            now=datetime(2026, 8, 26, 14, tzinfo=timezone.utc),
            execution_epoch=1,
            policy_digest="sha256:" + "3" * 64,
        )
    )
    proposer = Actor("proposer", "subject:proposer", "auth:submit", "nomos:submit")
    approver = Actor("approver", "subject:approver", "auth:review", "nomos:review")
    worker = Actor("worker", "subject:worker", "auth:worker", "nomos:worker")
    submitted = service.submit(
        deployment="support-prod", actor=proposer, idempotency_key="snapshot-submit-2341",
        automation_id="triage", automation_version="1", run_id="run-1",
        correlation_id="corr", action_type=contract.action_type,
        target_ref="ticket:T-42", risk_tier="high",
        action_schema_digest=contract.action_schema_digest,
        action_contract=contract,
        requested_review_expires_at="2026-08-26T15:00:00.000000Z",
        summary_code="resolve_after_deploy",
        payload=values["payload_original"]["value"],
        evidence_digests=["sha256:" + "a" * 64],
    )
    store = service.store
    proposal_id = submitted.proposal_id
    states = {"submit": _state(store.get(proposal_id))}
    approved = service.approve(
        proposal_id, actor=approver, idempotency_key="snapshot-approve-2341",
        action_contract=contract,
        execute_before="2026-08-26T14:30:00.000000Z",
        reason_code="verified_nonterminal_update",
        if_match=submitted.value.etag,
    )
    states["approve"] = _state(store.get(proposal_id))
    decision = store.get_decision(proposal_id)
    states["decision"] = (decision.verdict, decision.lifecycle_version, decision.payload_version)
    claim = service.claim(
        proposal_id, actor=worker, idempotency_key="snapshot-claim-2341",
        **_claim_body(approved.value.to_projection(), contract),
        if_match=approved.value.etag,
    )
    states["claim"] = _state(store.get(proposal_id))
    execution = store.get_execution(proposal_id)
    states["execution"] = _execution_facts(
        execution, decision, approved.value.lifecycle_version,
        values["payload_original"]["canonical_jcs"].encode(), "snapshot-claim-2341",
    )
    service.report_outcome(
        claim.execution_id, actor=worker, idempotency_key="snapshot-outcome-2341",
        outcome="succeeded", outcome_reason_code="provider_receipt_observed",
        provider_receipt_digest=RECEIPT, recovery_journal_digest=JOURNAL,
    )
    states["outcome"] = _state(store.get(proposal_id))

    rejected_submit = service.submit(
        deployment="support-prod", actor=proposer, idempotency_key="snapshot-submit-reject-2341",
        automation_id="triage", automation_version="1", run_id="run-2",
        correlation_id="corr", action_type=contract.action_type,
        target_ref="ticket:T-43", risk_tier="high",
        action_schema_digest=contract.action_schema_digest,
        action_contract=contract,
        requested_review_expires_at="2026-08-26T15:00:00.000000Z",
        summary_code="resolve_after_deploy",
        payload=values["payload_original"]["value"],
        evidence_digests=["sha256:" + "a" * 64],
    )
    service.reject(
        rejected_submit.proposal_id, actor=approver, idempotency_key="snapshot-reject-2341",
        action_contract=contract, reason_code="unsafe_terminal_status",
        if_match=rejected_submit.value.etag,
    )
    rejected = store.get(rejected_submit.proposal_id)
    rejected_decision = store.get_decision(rejected_submit.proposal_id)
    states["reject"] = _state(rejected)
    states["reject_decision"] = (
        rejected_decision.verdict, rejected_decision.lifecycle_version,
        rejected_decision.payload_version,
        rejected_decision.decision_id == rejected.current_decision_id,
    )
    return states


def _execution_facts(execution, decision, approved_lifecycle_version, payload, key):
    """Facts about a claimed execution that must hold on either store."""

    return {
        "lifecycle_version": execution.lifecycle_version,
        "claim_lifecycle_is_approved_plus_one": (
            execution.lifecycle_version == approved_lifecycle_version + 1
        ),
        "canonical_payload_is_submitted_bytes": execution.canonical_payload == payload,
        "decision_binding_is_the_decision": (
            execution.decision_binding_hash == decision.decision_binding_hash
            and bool(execution.decision_binding_hash)
        ),
        "claim_idempotency_hash_is_the_key": (
            execution.claim_idempotency_key_hash == idempotency_key_hash(key)
        ),
        "provider_idempotency_key_present": bool(execution.provider_idempotency_key),
        "execution_binding_present": bool(execution.execution_binding_hash),
    }


def _claim_body(approved, contract):
    return {
        "decision_id": approved["decision_id"],
        "approved_lifecycle_version": approved["lifecycle_version"],
        "approved_payload_version": approved["payload_version"],
        "approved_payload_hash": approved["current_payload_hash"],
        "supported_action_schema_digest": contract.action_schema_digest,
        "supported_action_contract_digest": contract.action_contract_digest,
        "supported_executor_contract_digest": contract.executor_contract_digest,
        "supported_payload_renderer_digest": contract.payload_renderer_digest,
        "supported_governed_tool_binding_digest": contract.governed_tool_binding_digest,
        "supported_provider_connection_contract_digest": contract.provider_connection_contract_digest,
        "supported_provider_capability_digest": contract.provider_capability_digest,
        "supported_provider_operation_id": contract.provider_operation_id,
        "supported_provider_operation_descriptor_digest": contract.provider_operation_descriptor_digest,
    }


def _submit(f, client):
    run = f.body()
    registered = _as(f, SUBJECT, client, "POST", f.base, run)
    assert registered.status_code == 201, registered.json()
    proposal = f.propose(client, run)
    return proposal.json()["proposal_id"], proposal


def _approve(f, client, proposal_id, proposal):
    approved = _as(
        f, APPROVER, client, "POST", _actions(f) + "/" + proposal_id + "/decisions",
        {
            "verdict": "approve",
            "reason_code": "verified_nonterminal_update",
            "execute_before": _timestamp(
                datetime.now(timezone.utc) + timedelta(minutes=30)
            ),
        },
        if_match=proposal.headers["etag"],
    )
    assert approved.status_code == 200, approved.json()
    return approved


def _actions(f):
    return f.base.replace("external-automation-runs", "external-actions")


def test_snapshot_after_approval_reports_the_committed_decision(lifecycle):
    """After approve a fresh store sees approved, version 2, and the decision."""

    f = lifecycle
    with f.client() as client:
        proposal_id, proposal = _submit(f, client)
        approved = _approve(f, client, proposal_id, proposal)

    proposal, decision, execution, _ = _hydrated(f, proposal_id)
    assert _columns(f, proposal_id)[:3] == ("approved", "ready", 2)
    assert (proposal.review_state, proposal.lifecycle_version) == ("approved", 2)
    assert proposal.execution_state == "ready"
    assert proposal.current_decision_id == approved.json()["decision_id"]
    assert decision is not None
    assert decision.decision_id == proposal.current_decision_id
    assert decision.verdict == "approve"
    assert decision.lifecycle_version == 2
    assert decision.payload_version == 1
    assert decision.payload_hash == proposal.current_payload_hash
    assert decision.authentication_audit_id and decision.nomos_decision_id
    assert execution is None


def test_snapshot_columns_win_over_a_stale_stored_record(lifecycle):
    """A store_record that still says pending cannot override the columns."""

    f = lifecycle
    with f.client() as client:
        proposal_id, proposal = _submit(f, client)
        _approve(f, client, proposal_id, proposal)
    with psycopg.connect(f.dsn) as admin:
        stale = admin.execute(
            "SELECT store_record ->> 'review_state',"
            " (store_record ->> 'lifecycle_version')::bigint"
            " FROM okg.external_action_proposals"
            " WHERE deployment_name = %s AND proposal_id = %s",
            (DEPLOYMENT, proposal_id),
        ).fetchone()
        decision_record = admin.execute(
            "SELECT store_record ? 'lifecycle_version', store_record ? 'payload_hash'"
            " FROM okg.external_action_decisions"
            " WHERE deployment_name = %s AND proposal_id = %s",
            (DEPLOYMENT, proposal_id),
        ).fetchone()
    # The stored records are the commands, not the committed state: the
    # proposal record carries no approved state and the decision record has
    # neither field Decision requires. Only the columns hold the truth.
    assert stale != ("approved", 2)
    assert decision_record == (False, False)
    hydrated, decision, _, _ = _hydrated(f, proposal_id)
    assert _state(hydrated) == ("approved", "ready", 2, 1)
    assert (decision.lifecycle_version, decision.payload_hash) == (
        2, hydrated.current_payload_hash
    )


def test_durable_lifecycle_snapshot_matches_the_in_memory_service(lifecycle):
    """Submit, approve, claim and outcome: each fresh snapshot matches memory."""

    f = lifecycle
    expected = _in_memory_states(f.values, f.contract)
    observed = {}
    with f.client() as client:
        proposal_id, proposal = _submit(f, client)
        hydrated, decision, _, _ = _hydrated(f, proposal_id)
        observed["submit"] = _state(hydrated)
        assert decision is None

        approved = _approve(f, client, proposal_id, proposal)
        hydrated, decision, execution, _ = _hydrated(f, proposal_id)
        observed["approve"] = _state(hydrated)
        observed["decision"] = (
            decision.verdict, decision.lifecycle_version, decision.payload_version
        )
        assert execution is None

        claim = _as(
            f, WORKER, client, "POST",
            _actions(f) + "/" + proposal_id + "/execution-claims",
            _claim_body(approved.json(), f.contract),
            if_match=approved.headers["etag"],
        )
        assert claim.status_code in (200, 201), claim.json()
        execution_id = claim.json()["execution_id"]
        hydrated, decision, execution, outcome = _hydrated(f, proposal_id)
        observed["claim"] = _state(hydrated)
        assert execution is not None and execution.execution_id == execution_id
        assert execution.decision_id == decision.decision_id
        assert outcome is None
        observed["execution"] = _execution_facts(
            execution, decision, approved.json()["lifecycle_version"],
            f.values["payload_original"]["canonical_jcs"].encode(),
            claim.request.headers["Idempotency-Key"],
        )
        # The loaded execution must agree with what the claim RPC returned to
        # the worker on every field both carry, including the hashes the
        # snapshot overlay now supplies. The claim response has no
        # decision_binding_hash; _execution_facts checks it against the
        # loaded decision instead.
        response = claim.json()
        loaded = execution.to_projection()
        shared = sorted(set(loaded) & set(response))
        for field in (
            "lifecycle_version", "canonical_payload",
            "provider_idempotency_key", "execution_binding_hash",
        ):
            assert field in shared, field
        assert loaded["handoff_receipt_digest"] == response["okg_claim_handoff_receipt_digest"]
        assert {key: loaded[key] for key in shared} == {
            key: response[key] for key in shared
        }

        reported = _as(
            f, WORKER, client, "POST",
            _actions(f) + "/" + proposal_id + "/execution-claims/" + execution_id + "/outcome",
            {
                "outcome": "succeeded",
                "outcome_reason_code": "provider_receipt_observed",
                "provider_receipt_digest": RECEIPT,
                "recovery_journal_digest": JOURNAL,
            },
            if_match=hydrated.etag,
        )
        assert reported.status_code in (200, 201), reported.json()
        hydrated, _, _, outcome = _hydrated(f, proposal_id)
        observed["outcome"] = _state(hydrated)
        assert outcome is not None and outcome.outcome == "succeeded"
        assert outcome.execution_id == execution_id
        assert outcome.outcome_request_hash == reported.json()["outcome_request_hash"]

        rejected_id, rejected_proposal = _submit(f, client)
        rejected_response = _as(
            f, APPROVER, client, "POST", _actions(f) + "/" + rejected_id + "/decisions",
            {"verdict": "reject", "reason_code": "unsafe_terminal_status"},
            if_match=rejected_proposal.headers["etag"],
        )
        assert rejected_response.status_code == 200, rejected_response.json()
        rejected, rejected_decision, rejected_execution, _ = _hydrated(f, rejected_id)
        observed["reject"] = _state(rejected)
        observed["reject_decision"] = (
            rejected_decision.verdict, rejected_decision.lifecycle_version,
            rejected_decision.payload_version,
            rejected_decision.decision_id == rejected.current_decision_id,
        )
        assert rejected_execution is None

    assert observed == expected
    assert _columns(f, proposal_id)[:4] == observed["outcome"]
