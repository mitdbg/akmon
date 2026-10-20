from __future__ import annotations

from okg.substrate.external_actions.store import (
    PostgresExternalActionStore,
    TransitionRecord,
)


def test_transition_snapshot_retains_the_exact_response_bytes() -> None:
    response = b'{"review_state":"pending"}'

    record = PostgresExternalActionStore._coerce_record(
        {
            "deployment": "support-prod",
            "proposal_id": "01J00000000000000000000000",
            "operation": "edit",
            "actor_separation_subject": "subject:reviewer",
            "idempotency_key_hash": "sha256:" + "a" * 64,
            "transition_request_hash": "sha256:" + "b" * 64,
            "response_status": 200,
            "response_body_jcs": response,
            "response_projection": {
                "review_state": "pending",
                "response_status": 200,
            },
        },
        TransitionRecord,
    )

    assert record["result"].response_body == response
    assert record["result"].value["review_state"] == "pending"
