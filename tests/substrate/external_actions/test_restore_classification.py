"""Parity checks for the Python and SQL restore checkpoint classifiers."""

from __future__ import annotations

import json

import psycopg
import pytest

from okg.substrate.external_actions import ExternalActionService, InMemoryExternalActionStore


pytestmark = pytest.mark.postgres


def _receipt_observed_checkpoint() -> dict[str, str]:
    digest = "sha256:" + ("a" * 64)
    return {
        "journal_phase": "provider_receipt_observed",
        "ledger_entry_digest": digest,
        "provider_idempotency_key_digest": digest,
        "effect_started_at": "2026-08-27T12:00:00.000000Z",
        "provider_receipt_digest": digest,
        "checkpoint_issued_at": "2026-08-27T12:01:00.000000Z",
        "journal_digest": digest,
        "submission_content_hash": digest,
        "proposal_id": "proposal-1",
        "proposal_request_hash": digest,
        "original_payload_hash": digest,
        "approved_payload_hash": digest,
        "action_contract_digest": digest,
        "decision_id": "decision-1",
        "decision_binding_hash": digest,
        "execution_id": "execution-1",
        "execution_binding_hash": digest,
        "okg_claim_handoff_receipt_digest": digest,
        "pre_restore_effect_event_head_hash": digest,
    }


def test_provider_receipt_classification_matches_python_and_sql(okg_migrated: str) -> None:
    checkpoint = _receipt_observed_checkpoint()
    service = ExternalActionService(InMemoryExternalActionStore())

    python_classification = service._restore_checkpoint_classification(checkpoint, "positive")
    with psycopg.connect(okg_migrated) as connection:
        sql_classification = connection.execute(
            "SELECT okg.external_action_restore_classification(%s::jsonb, 'positive')",
            (json.dumps(checkpoint, separators=(",", ":")),),
        ).fetchone()[0]

    assert python_classification == sql_classification == "database_restore_succeeded"
