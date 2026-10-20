"""Integration test for the `/viz/api/traces` query-trace listing endpoint.

Home reads this route for its recent-query activity list. Here we verify the
wiring against a real substrate: the endpoint reads ``okg.mcp_trace_events``
rows, splits a process session into its explicit question-label slices, and
reports per-slice counts, the derived summary and the honest status.

The `/viz/api/trace` per-session detail endpoint these tests also covered, and
the `/viz/api/workflow` span-timeline endpoint, were removed with the Trace and
Performance views in issue #1924.
"""
from __future__ import annotations

import json

import psycopg
import pytest

pytest.importorskip("fastapi")


# Reuse the module-scoped seeded_app fixture from the sibling smoke
# test module — it builds a TestClient over a viz wired to a seeded
# substrate (devops template). Module-scoped so the publish cycle
# happens once.
from .test_viz_substrate import seeded_app  # noqa: F401

pytestmark = pytest.mark.postgres


def _insert_event(
    conn: psycopg.Connection,
    *,
    session_id: str,
    deployment_name: str,
    tool_name: str,
    request_json: dict | None,
    response_json: dict | None,
    duration_ms: int = 1,
    error_text: str | None = None,
    question_label: str | None = None,
    event_time_offset_seconds: int = 0,
) -> None:
    """Insert one trace row at ``now() + offset`` so tests can
    control event_time ordering deterministically."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.mcp_trace_events (
              session_id, deployment_name, tool_name,
              request_json, response_json, error_text,
              duration_ms, question_label, event_time
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                    now() + (%s || ' seconds')::interval)
            """,
            (
                session_id, deployment_name, tool_name,
                json.dumps(request_json) if request_json is not None else None,
                json.dumps(response_json) if response_json is not None else None,
                error_text, duration_ms, question_label,
                event_time_offset_seconds,
            ),
        )


def test_trace_listing_keeps_query_slices_and_graphless_failures(
    seeded_app,
) -> None:
    client, ctx = seeded_app
    sid = "test-query-slice-listing"
    with psycopg.connect(ctx.dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM okg.mcp_trace_events WHERE session_id = %s",
                (sid,),
            )
        _insert_event(
            conn,
            session_id=sid,
            deployment_name="default",
            tool_name="inspect",
            request_json={"args": [], "kwargs": {"node_id": "person:alice"}},
            response_json={"node_id": "person:alice", "subtype": "person"},
            duration_ms=8,
            question_label="Who is Alice?",
            event_time_offset_seconds=20,
        )
        _insert_event(
            conn,
            session_id=sid,
            deployment_name="default",
            tool_name="search",
            request_json={"args": [], "kwargs": {"query": "alice"}},
            response_json={
                "candidates": [
                    {"node_id": "person:alice", "score": 0.9},
                ],
            },
            duration_ms=5,
            question_label="Who is Alice?",
            event_time_offset_seconds=20.5,
        )
        _insert_event(
            conn,
            session_id=sid,
            deployment_name="default",
            tool_name="search",
            request_json={"args": [], "kwargs": {"query": "broken projector"}},
            response_json=None,
            duration_ms=21,
            error_text="search failed",
            event_time_offset_seconds=21,
        )
        _insert_event(
            conn,
            session_id=sid,
            deployment_name="default",
            tool_name="source_status",
            request_json={"args": [], "kwargs": {}},
            response_json={"sources": []},
            duration_ms=3,
            event_time_offset_seconds=22,
        )

    response = client.get("/viz/api/traces")
    assert response.status_code == 200, response.text
    items = [
        item for item in response.json()["items"]
        if item["session_id"] == sid
    ]
    assert len(items) == 2
    assert len({item["query_id"] for item in items}) == 2

    explicit = next(item for item in items if item["question_label"])
    assert explicit["query_summary"] == "Who is Alice?"
    assert explicit["label_source"] == "explicit"
    assert explicit["graph_call_count"] == 2
    assert explicit["status"] == "ok"

    unlabeled = next(item for item in items if item["question_label"] is None)
    assert unlabeled["query_summary"] == "Search · broken projector"
    assert unlabeled["label_source"] == "derived"
    assert unlabeled["tool_call_count"] == 2
    assert unlabeled["error_count"] == 1
    assert unlabeled["graph_call_count"] == 0
    assert unlabeled["status"] == "partial"
    assert unlabeled["total_duration_ms"] == 24
