"""Separate-process acceptance proof for external-action review.

The fixture runs the production review service/API in one process and a fake
worker, provider, and effect ledger in their own processes.  No provider
address, client, or connection is available to the review process.  The
fixture is intentionally deterministic: IDs, database time, payloads, and
provider receipts are all fixed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import pytest
import yaml

from okg.substrate.external_actions.action_contracts import ActionContract, canonical_json_bytes

from .external_actions.fake_effect_ledger import FakeEffectLedgerProcess
from .external_actions.fake_provider import FakeProviderProcess
from .external_actions.fake_provider_authority import FakeProviderAuthority
from .external_actions.fake_worker import FakeWorkerProcess
from .external_actions.fake_worker_control_plane import FakeWorkerControlPlane
from .external_actions.fake_epoch_authority import FakeEpochAuthority
from .external_actions.review_api import DEPLOYMENT, ReviewAPIProcess


ROOT = Path(__file__).resolve().parents[2]
VECTORS = ROOT / "tests/fixtures/external_actions/hash-vectors.yaml"


def _fixture() -> tuple[dict[str, Any], ActionContract]:
    document = yaml.safe_load(VECTORS.read_text(encoding="utf-8"))
    values = {row["id"]: row for row in document["vectors"]}
    return values, ActionContract.from_mapping(values["action_contract"]["value"])


class ReviewFixture:
    def __init__(self, contract: ActionContract) -> None:
        self.contract = contract
        self.review = ReviewAPIProcess(contract.to_mapping())
        self.provider = FakeProviderProcess()
        self.ledger = FakeEffectLedgerProcess()
        self.workers: list[FakeWorkerProcess] = []

    def worker(self, identity: str = "worker") -> FakeWorkerProcess:
        worker = FakeWorkerProcess(
            review_address=self.review.address,
            provider_address=self.provider.address,
            ledger_address=self.ledger.address,
            contract_mapping=self.contract.to_mapping(),
            identity=identity,
        )
        self.workers.append(worker)
        return worker

    def close(self) -> None:
        for worker in reversed(self.workers):
            worker.close()
        self.ledger.close()
        self.provider.close()
        self.review.close()


@pytest.fixture(params=("first-clean-run", "repeat-clean-run"))
def fixture(request: pytest.FixtureRequest) -> ReviewFixture:
    assert request.param in {"first-clean-run", "repeat-clean-run"}
    _, contract = _fixture()
    value = ReviewFixture(contract)
    try:
        yield value
    finally:
        value.close()


def _submit(fixture: ReviewFixture, *, identity: str = "proposer", key: str = "submit-proposal-417", expires: str = "2026-08-26T15:30:00.000000Z") -> Mapping[str, Any]:
    _, contract = _fixture()
    body = {
        "automation_id": "ticket-triage",
        "automation_version": "3.2.0",
        "run_id": "run-417",
        "correlation_id": "incident-99",
        "action_type": contract.action_type,
        "target_ref": "ticket:T-42",
        "risk_tier": "high",
        "action_schema_digest": contract.action_schema_digest,
        "action_contract_digest": contract.action_contract_digest,
        "requested_review_expires_at": expires,
        "summary_code": "resolve_after_deploy",
        "evidence_digests": ["sha256:" + "a" * 64],
        "payload": {
            "ticket": "T-42",
            "status": "resolved",
            "comment": "Deploy build 417",
        },
    }
    return fixture.review.request(
        method="POST",
        path=f"/v1/deployments/{DEPLOYMENT}/external-actions",
        identity=identity,
        body=body,
        headers={"Idempotency-Key": key},
    )


def _edit(fixture: ReviewFixture, proposal: Mapping[str, Any], *, key: str = "edit-proposal-417") -> Mapping[str, Any]:
    return fixture.review.request(
        method="POST",
        path=f"/v1/deployments/{DEPLOYMENT}/external-actions/{proposal['proposal_id']}/edits",
        identity="approver",
        body={
            "replacement_payload": {
                "ticket": "T-42",
                "status": "pending",
                "comment": "Deploy build 417 after final verification",
            },
        },
        headers={"Idempotency-Key": key, "If-Match": proposal["etag"]},
    )


def _approve(fixture: ReviewFixture, proposal: Mapping[str, Any], *, key: str = "approve-proposal-417") -> Mapping[str, Any]:
    return fixture.review.request(
        method="POST",
        path=f"/v1/deployments/{DEPLOYMENT}/external-actions/{proposal['proposal_id']}/decisions",
        identity="approver",
        body={
            "verdict": "approve",
            "execute_before": "2026-08-26T15:00:00.000000Z",
            "reason_code": "verified_nonterminal_update",
        },
        headers={"Idempotency-Key": key, "If-Match": proposal["etag"]},
    )


def _read(fixture: ReviewFixture, proposal_id: str, *, identity: str = "approver") -> Mapping[str, Any]:
    return fixture.review.request(
        method="GET",
        path=f"/v1/deployments/{DEPLOYMENT}/external-actions/{proposal_id}",
        identity=identity,
    )


def _payload(fixture: ReviewFixture, proposal_id: str) -> Mapping[str, Any]:
    return fixture.review.request(
        method="GET",
        path=f"/v1/deployments/{DEPLOYMENT}/external-actions/{proposal_id}/payload",
        identity="approver",
    )


def test_separate_worker_executes_only_edited_payload_and_exact_retries(fixture: ReviewFixture) -> None:
    first = _submit(fixture)
    assert first["status"] == 201
    proposal = first["json"]
    before_conflict = fixture.review.state()

    exact = _submit(fixture)
    assert exact["status"] == 200
    assert exact["json"] == proposal
    assert fixture.review.state()["counts"] == before_conflict["counts"]

    conflict = _submit(fixture, key="submit-proposal-417", expires="2026-08-26T15:30:00.000000Z")
    # The immutable request is changed below; the same key must conflict and
    # the test asserts the stored bytes remain unchanged.
    conflict_body = {
        "automation_id": "ticket-triage",
        "automation_version": "3.2.0",
        "run_id": "run-417",
        "correlation_id": "incident-99",
        "action_type": fixture.contract.action_type,
        "target_ref": "ticket:T-42",
        "risk_tier": "high",
        "action_schema_digest": fixture.contract.action_schema_digest,
        "action_contract_digest": fixture.contract.action_contract_digest,
        "requested_review_expires_at": "2026-08-26T15:30:00.000000Z",
        "summary_code": "resolve_after_deploy",
        "evidence_digests": ["sha256:" + "a" * 64],
        "payload": {"ticket": "T-42", "status": "resolved", "comment": "changed"},
    }
    conflict = fixture.review.request(
        method="POST",
        path=f"/v1/deployments/{DEPLOYMENT}/external-actions",
        identity="proposer",
        body=conflict_body,
        headers={"Idempotency-Key": "submit-proposal-417"},
    )
    assert conflict["status"] == 409 and conflict["json"]["code"] == "idempotency_conflict"
    assert fixture.review.state()["counts"] == before_conflict["counts"]

    own_approval = fixture.review.request(
        method="POST",
        path=f"/v1/deployments/{DEPLOYMENT}/external-actions/{proposal['proposal_id']}/decisions",
        identity="proposer",
        body={"verdict": "approve", "execute_before": "2026-08-26T15:00:00.000000Z", "reason_code": "verified_nonterminal_update"},
        headers={"Idempotency-Key": "proposer-approval-417", "If-Match": proposal["etag"]},
    )
    assert own_approval["status"] == 403
    assert own_approval["json"]["code"] == "proposer_approver_separation_required"
    alias_approval = fixture.review.request(
        method="POST",
        path=f"/v1/deployments/{DEPLOYMENT}/external-actions/{proposal['proposal_id']}/decisions",
        identity="proposer-alias",
        body={"verdict": "approve", "execute_before": "2026-08-26T15:00:00.000000Z", "reason_code": "verified_nonterminal_update"},
        headers={"Idempotency-Key": "proposer-alias-approval-417", "If-Match": proposal["etag"]},
    )
    assert alias_approval["status"] == 403
    assert alias_approval["json"]["code"] == "proposer_approver_separation_required"

    edited = _edit(fixture, proposal)
    assert edited["status"] == 200
    current = edited["json"]
    stale = _approve(fixture, proposal, key="stale-approval-417")
    assert stale["status"] == 409 and stale["json"]["code"] == "stale_proposal_lifecycle"
    approved = _approve(fixture, current)
    assert approved["status"] == 200
    assert approved["json"]["review_state"] == "approved"

    worker = fixture.worker()
    execution = worker.execute(proposal["proposal_id"])
    assert execution["claim_response"]["status"] == 201
    assert execution["report"]["status"] == 201
    assert execution["report"]["json"]["outcome"] == "succeeded"
    observations = fixture.provider.snapshot()["observations"]
    assert len(observations) == 1
    assert observations[0]["request"]["body"] == canonical_json_bytes({
        "ticket": "T-42",
        "status": "pending",
        "comment": "Deploy build 417 after final verification",
    }).decode()
    assert "resolved" not in observations[0]["request"]["body"]
    assert fixture.review.state()["counts"]["executions"] == 1
    assert fixture.review.state()["counts"]["outcomes"] == 1

    duplicate_outcome = worker.command({"op": "duplicate_outcome"})
    assert duplicate_outcome["status"] == 200
    assert duplicate_outcome["headers"].get("idempotent-replay") == "true"
    retry = worker.execute(proposal["proposal_id"])
    assert retry["claim_response"]["status"] == 200
    assert retry["provider_calls"] == 0
    assert len(fixture.provider.snapshot()["observations"]) == 1


def test_reject_and_expiry_are_terminal_and_materialized_once(fixture: ReviewFixture) -> None:
    first = _submit(fixture, key="reject-proposal-417")
    proposal = first["json"]
    rejected = fixture.review.request(
        method="POST",
        path=f"/v1/deployments/{DEPLOYMENT}/external-actions/{proposal['proposal_id']}/decisions",
        identity="approver",
        body={"verdict": "reject", "reason_code": "unsafe_terminal_status"},
        headers={"Idempotency-Key": "reject-decision-417", "If-Match": proposal["etag"]},
    )
    assert rejected["status"] == 200
    assert rejected["json"]["review_state"] == "rejected"
    retry = fixture.review.request(
        method="POST",
        path=f"/v1/deployments/{DEPLOYMENT}/external-actions/{proposal['proposal_id']}/decisions",
        identity="approver",
        body={"verdict": "reject", "reason_code": "unsafe_terminal_status"},
        headers={"Idempotency-Key": "reject-decision-417", "If-Match": proposal["etag"]},
    )
    assert retry["status"] == 200 and retry["headers"].get("idempotent-replay") == "true"

    second = _submit(fixture, key="expiry-proposal-417", expires="2026-08-26T14:01:00.000000Z")
    expired_id = second["json"]["proposal_id"]
    fixture.review.command({"op": "advance", "seconds": 61})
    expired = _read(fixture, expired_id)
    assert expired["status"] == 200 and expired["json"]["review_state"] == "expired"
    timeline = fixture.review.command({"op": "timeline", "proposal_id": expired_id})["events"]
    assert [event["event_type"] for event in timeline].count("review_expired") == 1
    late_edit = _edit(fixture, expired["json"], key="late-edit-case-417")
    assert late_edit["status"] == 410 and late_edit["json"]["code"] == "proposal_expired"
    timeline_again = fixture.review.command({"op": "timeline", "proposal_id": expired_id})["events"]
    assert len(timeline_again) == len(timeline)


def test_execution_deadline_expiry_refuses_the_worker_and_is_recorded_once(
    fixture: ReviewFixture,
) -> None:
    """An approved proposal past ``execute_before`` never reaches the provider.

    ``_approve`` sets the execution deadline to 15:00Z and ``_submit`` leaves
    the review deadline at 15:30Z, so the clock below can only reach the
    execution-deadline branch.  The provider is a separate process here, so
    ``provider_calls`` is real evidence that the payload was not executed.
    """

    proposal = _submit(fixture, key="execution-deadline-proposal-417")["json"]
    approved = _approve(fixture, proposal, key="execution-deadline-approve-417")["json"]
    assert approved["review_state"] == "approved"
    proposal_id = approved["proposal_id"]

    # The fixture clock starts at 14:00Z; 3,660 seconds lands on 15:01Z.
    fixture.review.command({"op": "advance", "seconds": 3660})

    worker = fixture.worker()
    refused = worker.execute(proposal_id, idempotency_key="execution-deadline-claim-417")
    assert refused["provider_calls"] == 0
    assert refused["claim"]["status"] == 410
    assert refused["claim"]["json"]["code"] == "proposal_expired"
    assert fixture.provider.snapshot()["observations"] == []

    timeline = fixture.review.command({"op": "timeline", "proposal_id": proposal_id})["events"]
    event_types = [event["event_type"] for event in timeline]
    assert event_types.count("approval_expired") == 1
    assert "review_expired" not in event_types
    expiry = next(
        event for event in timeline if event["event_type"] == "approval_expired"
    )
    assert expiry["deadline_kind"] == "execute_before"
    assert expiry["before_review_state"] == "approved"
    assert expiry["after_review_state"] == "expired"
    assert expiry["before_execution_state"] == "ready"
    assert expiry["after_execution_state"] == "unavailable"

    retry = worker.execute(proposal_id, idempotency_key="execution-deadline-claim-417-2")
    assert retry["provider_calls"] == 0
    timeline_after_retry = fixture.review.command(
        {"op": "timeline", "proposal_id": proposal_id}
    )["events"]
    assert len(timeline_after_retry) == len(timeline)
    assert fixture.provider.snapshot()["observations"] == []


def test_response_loss_is_indeterminate_and_contradictory_outcome_is_refused(fixture: ReviewFixture) -> None:
    proposal = _submit(fixture, key="uncertain-proposal-417")["json"]
    proposal = _edit(fixture, proposal, key="uncertain-edit-417")["json"]
    proposal = _approve(fixture, proposal, key="uncertain-approve-417")["json"]
    fixture.provider.set_mode("response_loss")
    worker = fixture.worker()
    result = worker.execute(proposal["proposal_id"], idempotency_key="uncertain-claim-417", provider_mode="response_loss")
    assert result["report"]["status"] == 201
    assert result["report"]["json"]["outcome"] == "indeterminate"
    assert result["report"]["json"]["provider_receipt_digest"] is None
    assert len(fixture.provider.snapshot()["observations"]) == 1

    duplicate = worker.command({"op": "duplicate_outcome"})
    assert duplicate["status"] == 200
    assert len(fixture.provider.snapshot()["observations"]) == 1
    current = _read(fixture, proposal["proposal_id"])["json"]
    contradictory = fixture.review.request(
        method="POST",
        path=f"/v1/deployments/{DEPLOYMENT}/external-actions/{proposal['proposal_id']}/execution-claims/{result['claim']['execution_id']}/outcome",
        identity="worker",
        body={
            "outcome": "succeeded",
            "outcome_reason_code": "provider_receipt_observed",
            "provider_receipt_digest": "sha256:" + "f" * 64,
            "recovery_journal_digest": "sha256:" + "e" * 64,
        },
        headers={"Idempotency-Key": "contradictory-outcome-417", "If-Match": current["etag"]},
    )
    assert contradictory["status"] == 409
    assert contradictory["json"]["code"] == "outcome_already_recorded"


def test_explicit_provider_failure_is_reported_once_and_okg_never_calls_provider(fixture: ReviewFixture) -> None:
    # Before a worker exists, the review process has no provider endpoint and
    # the separate provider has observed no connection/request.
    assert fixture.provider.snapshot()["count"] == 0
    assert fixture.review.state()["counts"]["proposals"] == 0
    proposal = _submit(fixture, key="failure-proposal-417")["json"]
    proposal = _approve(fixture, proposal, key="failure-approve-417")["json"]
    fixture.provider.set_mode("explicit_failure")
    result = fixture.worker().execute(proposal["proposal_id"], idempotency_key="failure-claim-417", provider_mode="explicit_failure")
    assert result["report"]["status"] == 201
    assert result["report"]["json"]["outcome"] == "failed"
    assert result["report"]["json"]["outcome_reason_code"] == "provider_explicit_failure"
    assert fixture.provider.snapshot()["count"] == 1
    assert fixture.review.state()["counts"]["outcomes"] == 1


def test_claim_owner_wins_deterministic_claim_barrier_and_loser_gets_no_payload(fixture: ReviewFixture) -> None:
    proposal = _submit(fixture, key="race-proposal-417")["json"]
    proposal = _approve(fixture, proposal, key="race-approve-417")["json"]
    first = fixture.worker()
    paused = first.execute(proposal["proposal_id"], idempotency_key="race-claim-case-417", pause_before_ledger=True)
    assert paused["status"] == 202
    loser = fixture.worker("worker-2").execute(proposal["proposal_id"], idempotency_key="race-claim-2-417")
    assert loser["claim"]["status"] == 409
    assert loser["claim"]["json"]["code"] == "execution_already_claimed"
    assert "canonical_payload" not in loser["claim"]["json"]
    assert fixture.provider.snapshot()["count"] == 0
    resumed = first.resume(proposal["proposal_id"])
    assert resumed["report"]["status"] == 201
    assert fixture.provider.snapshot()["count"] == 1


def test_restore_matrix_never_repeats_effect(fixture: ReviewFixture) -> None:
    """Exercise both post-effect restore replay and claim/rotation fencing."""
    proposal = _submit(fixture, key="restore-proposal-417")["json"]
    proposal = _edit(fixture, proposal, key="restore-edit-417")["json"]
    proposal = _approve(fixture, proposal, key="restore-approve-417")["json"]
    fixture.review.command({"op": "save_snapshot", "snapshot_id": "approved"})

    worker = fixture.worker()
    completed = worker.execute(proposal["proposal_id"], idempotency_key="restore-claim-417")
    assert completed["report"]["status"] == 201
    assert fixture.provider.snapshot()["count"] == 1

    # A supported restore rotates the external epoch outside the review DB,
    # then restores the DB snapshot.  The old effect key remains in the
    # ledger, while the restored DB has no claim; either replay or fencing is
    # safe, and neither path reaches the provider a second time.
    fixture.review.command({"op": "restore_snapshot", "snapshot_id": "approved"})
    fixture.ledger.rotate_epoch(18)
    retry = fixture.worker("worker-2").execute(proposal["proposal_id"], idempotency_key="restore-claim-417")
    assert retry["claim_response"]["status"] == 201
    assert retry["ledger"]["accepted"] is False
    assert retry["ledger"]["error"] == "execution_epoch_fenced"
    assert fixture.provider.snapshot()["count"] == 1

    # The externally owned control planes prove that an old worker and grant
    # cannot resume after the same rotation.
    epoch = FakeEpochAuthority()
    workers = FakeWorkerControlPlane()
    providers = FakeProviderAuthority()
    workers.register("worker-1")
    providers.grant("grant-17")
    old_epoch = epoch.epoch
    new_epoch = epoch.rotate()
    workers.fence_epoch({"worker-1"})
    providers.fence_epoch({"grant-17"})
    assert old_epoch == 17 and new_epoch == 18
    assert not workers.can_resume("worker-1")
    assert not providers.can_use("grant-17")

    # A claim made before ledger begin is also fenced at the same barrier.
    second = _submit(fixture, key="restore-paused-417")["json"]
    second = _approve(fixture, second, key="restore-paused-approve-417")["json"]
    paused_worker = fixture.worker()
    paused = paused_worker.execute(second["proposal_id"], idempotency_key="restore-paused-claim-417", pause_before_ledger=True)
    assert paused["status"] == 202
    fenced = paused_worker.resume(second["proposal_id"])
    assert fenced["ledger"]["error"] == "execution_epoch_fenced"
    assert fixture.provider.snapshot()["count"] == 1
