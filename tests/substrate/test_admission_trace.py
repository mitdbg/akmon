"""Admission trace/refinement tests."""
from __future__ import annotations

from uuid import uuid4

import psycopg
import pytest

from okg.substrate.admission import ensure_envelope, record_event
from okg.substrate.admission_trace import (
    load_admission_traces,
    validate_admission_traces,
)


pytestmark = pytest.mark.postgres


def test_review_admission_trace_refines_model(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows,
        "_resolve_dsn",
        lambda _deployment: clean_runtime,
    )
    submitted = admission_workflows._run_submit(
        deployment="trace",
        payload={
            "node": {
                "node_id": "trace:node",
                "subtype": "document",
                "attrs": {"text": "trace"},
            },
        },
        producer_id="agent:trace",
        producer_kind="agent",
        intent="review_proposal",
        idempotency_key="trace-key",
        authority=None,
        check_summary=None,
    )
    admission_workflows._run_decide(
        deployment="trace",
        admission_id=submitted["admission_id"],
        decision="accept",
        actor="reviewer",
        reason="fixture",
        verdict=None,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        traces = load_admission_traces(
            conn,
            deployment_name="trace",
            admission_id=submitted["admission_id"],
        )
        result = validate_admission_traces(traces)

    assert result.ok is True
    assert result.actions == (
        "Submit",
        "RecoverRefs",
        "SubmitReview",
        "RecoverRefs",
        "Accept",
    )


def test_unmodeled_transition_fails_refinement(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.transaction():
            envelope = ensure_envelope(
                conn,
                deployment_name="trace_bad",
                producer_id="agent:bad",
                producer_kind="agent",
                intent="review_proposal",
                admission_mode="review",
                status="pending",
                idempotency_key=f"bad-{uuid4()}",
            )
            record_event(
                conn,
                admission_id=envelope.admission_id,
                event_type="published",
                reason="impossible_without_accept",
            )
        traces = load_admission_traces(
            conn,
            deployment_name="trace_bad",
            admission_id=envelope.admission_id,
        )
        result = validate_admission_traces(traces)

    assert result.ok is False
    assert result.errors[0]["code"] == "unmodeled_admission_transition"
