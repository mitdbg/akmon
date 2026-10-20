from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.substrate.exchange.refinement import (
    PorosRefinementError,
    build_refinement_state_from_trace,
)


def _valid_trace() -> dict:
    return json.loads(
        Path("formal/poros/fixtures/two-okg-trace.json").read_text(
            encoding="utf-8"
        )
    )


def test_poros_refinement_fixture_maps_to_model_state() -> None:
    state = build_refinement_state_from_trace(_valid_trace())

    assert state["model"] == "formal/poros/PorosExchange.tla"
    assert state["outbox"] == ["poros-msg-1"]
    assert state["inbox"] == ["poros-msg-1"]
    assert state["verified"] == ["poros-msg-1"]
    assert state["admissions"] == ["poros-msg-1"]
    assert state["publishedMessages"] == ["poros-msg-1"]
    assert state["liveRows"] == ["node:poros-demo:context:poros-msg-1"]
    assert state["deliveryAttempts"]["poros-msg-1"][0]["body_sha256"] == (
        "sha256:body-1"
    )
    assert state["deliveryAttempts"]["poros-msg-1"][0]["envelope_sha256"] == (
        "sha256:envelope-1"
    )
    assert state["rowSenderGeneration"][
        "node:poros-demo:context:poros-msg-1"
    ] == "sender-gen-7"
    assert state["rowReceiverGeneration"][
        "node:poros-demo:context:poros-msg-1"
    ] == "receiver-gen-3"


def test_poros_refinement_accepts_receipt_without_publication() -> None:
    trace = _valid_trace()
    trace["nomos_decisions"] = []
    trace["admissions"] = []
    trace["generations"] = []
    trace["live_rows"] = []
    trace["proofs"] = []

    state = build_refinement_state_from_trace(trace)

    assert state["receipts"]["poros-msg-1"]["status"] == "accepted_for_admission"
    assert state["publishedMessages"] == []
    assert state["liveRows"] == []


def test_poros_refinement_rejects_admission_without_nomos_decision() -> None:
    trace = _valid_trace()
    trace["nomos_decisions"] = []

    with pytest.raises(PorosRefinementError, match="missing nomosDecisions"):
        build_refinement_state_from_trace(trace)


def test_poros_refinement_rejects_receipt_body_hash_mismatch() -> None:
    trace = _valid_trace()
    trace["receipts"][0]["received_body_sha256"] = "sha256:different"

    with pytest.raises(PorosRefinementError, match="receipt/outbox body hash"):
        build_refinement_state_from_trace(trace)


def test_poros_refinement_rejects_delivery_attempt_body_hash_mismatch() -> None:
    trace = _valid_trace()
    trace["delivery_attempts"][0]["body_sha256"] = "sha256:different"

    with pytest.raises(PorosRefinementError, match="delivery attempt.*body hash"):
        build_refinement_state_from_trace(trace)


def test_poros_refinement_rejects_quarantined_live_row() -> None:
    trace = _valid_trace()
    trace["nomos_decisions"][0]["effect"] = "quarantine"

    with pytest.raises(PorosRefinementError, match="allow or promotion decision"):
        build_refinement_state_from_trace(trace)


def test_poros_refinement_accepts_promoted_reviewed_message() -> None:
    trace = _valid_trace()
    trace["nomos_decisions"][0]["effect"] = "review"
    trace["promotion_decisions"] = [
        {
            "message_id": "poros-msg-1",
            "decision_id": "nomos:promotion:poros-msg-1",
            "effect": "allow",
            "input_hash": "sha256:promotion-input-1",
            "policy_bundle_id": "nomos:bundle:poros-demo",
            "policy_fingerprint": "sha256:promotion-policy-1",
            "uses_receiver_policy": True,
            "classification": "public",
            "obligations": [],
            "reason_code": "promotion_allowed",
        }
    ]

    state = build_refinement_state_from_trace(trace)

    assert state["blockedState"] == []
    assert state["promotionDecisions"]["poros-msg-1"]["effect"] == "allow"
    assert state["publishedMessages"] == ["poros-msg-1"]


def test_poros_refinement_rejects_live_row_without_verified_inbox() -> None:
    trace = _valid_trace()
    trace["inbox"][0]["status"] = "rejected_preflight"
    trace["inbox"][0]["trusted"] = False

    with pytest.raises(PorosRefinementError, match="lacks verified preflight"):
        build_refinement_state_from_trace(trace)


def test_poros_refinement_rejects_live_row_without_allow_or_promotion() -> None:
    trace = _valid_trace()
    trace["nomos_decisions"][0]["effect"] = "review"

    with pytest.raises(
        PorosRefinementError,
        match="allow or promotion decision",
    ):
        build_refinement_state_from_trace(trace)


def test_poros_refinement_rejects_live_row_without_admission() -> None:
    trace = _valid_trace()
    trace["admissions"] = []

    with pytest.raises(PorosRefinementError, match="lacks admission evidence"):
        build_refinement_state_from_trace(trace)


def test_poros_refinement_rejects_live_row_without_published_generation() -> None:
    trace = _valid_trace()
    trace["generations"] = []

    with pytest.raises(PorosRefinementError, match="unpublished message"):
        build_refinement_state_from_trace(trace)


def test_poros_refinement_rejects_protected_public_proof_without_redaction() -> None:
    trace = _valid_trace()
    trace["outbox"][0]["classification"] = "private"
    trace["nomos_decisions"][0]["classification"] = "private"
    trace["proofs"][0]["classification"] = "private"

    with pytest.raises(PorosRefinementError, match="protected public proof"):
        build_refinement_state_from_trace(trace)


def test_poros_refinement_accepts_protected_public_proof_with_redaction() -> None:
    trace = _valid_trace()
    trace["outbox"][0]["classification"] = "private"
    trace["nomos_decisions"][0]["classification"] = "private"
    trace["nomos_decisions"][0]["obligations"] = [{"type": "redaction"}]
    trace["proofs"][0]["classification"] = "private"
    trace["proofs"][0]["redacted"] = True

    state = build_refinement_state_from_trace(trace)

    assert state["nomosDecisions"]["poros-msg-1"]["hasRedaction"] is True


def test_poros_refinement_rejects_sender_hint_broadening_policy() -> None:
    trace = _valid_trace()
    trace["outbox"][0]["sender_hint"] = True
    trace["outbox"][0]["receiver_authorized"] = False

    with pytest.raises(PorosRefinementError, match="sender hint broadened"):
        build_refinement_state_from_trace(trace)


def test_poros_refinement_rejects_unsupported_trace_fields() -> None:
    trace = _valid_trace()
    trace["outbox"][0]["unexpected"] = "silently ignoring this would be unsafe"

    with pytest.raises(PorosRefinementError, match="unsupported fields"):
        build_refinement_state_from_trace(trace)


def test_poros_refinement_rejects_unsupported_effects() -> None:
    trace = _valid_trace()
    trace["unsupported"] = ["remote_sql"]

    with pytest.raises(PorosRefinementError, match="unsupported effects"):
        build_refinement_state_from_trace(trace)
