from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from okg.substrate.telemetry import (
    TelemetrySpan,
    new_span_id,
    new_trace_id,
    record_span,
)


pytestmark = pytest.mark.postgres


def test_unified_telemetry_tables_and_views_exist(okg_migrated: str) -> None:
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT tablename
              FROM pg_tables
             WHERE schemaname = 'okg'
               AND tablename = ANY(%s)
            """,
            (
                [
                    "telemetry_traces",
                    "telemetry_spans",
                    "telemetry_events",
                    "telemetry_metrics",
                    "telemetry_artifacts",
                ],
            ),
        )
        tables = {row[0] for row in cur.fetchall()}
        assert tables == {
            "telemetry_traces",
            "telemetry_spans",
            "telemetry_events",
            "telemetry_metrics",
            "telemetry_artifacts",
        }
        cur.execute(
            """
            SELECT table_name
              FROM information_schema.views
             WHERE table_schema = 'okg'
               AND table_name = ANY(%s)
            """,
            (
                [
                    "v_telemetry_trace_summary",
                    "v_telemetry_span_tree",
                ],
            ),
        )
        views = {row[0] for row in cur.fetchall()}
        assert views == {
            "v_telemetry_trace_summary",
            "v_telemetry_span_tree",
        }


def test_record_span_writes_trace_span_and_summary(clean_runtime: str) -> None:
    trace_id = new_trace_id()
    span_id = new_span_id()
    now = datetime.now(timezone.utc)
    with psycopg.connect(clean_runtime) as conn:
        record_span(
            conn,
            TelemetrySpan(
                trace_id=trace_id,
                span_id=span_id,
                span_name="source.git-files",
                span_kind="internal",
                status_code="ok",
                deployment_name="default",
                branch_name="default",
                source_name="git-files",
                workflow_id="wf-1",
                run_id="run-1",
                started_at=now,
                ended_at=now,
                attributes={"component": "source-sync"},
                payload_summary={"records": 3},
                root_name="source.git-files",
                root_kind="source_run",
            ),
        )
        conn.commit()

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT root_name, root_kind, span_count, slowest_span_name
              FROM okg.v_telemetry_trace_summary
             WHERE trace_id = %s
            """,
            (trace_id,),
        )
        row = cur.fetchone()
        assert row == ("source.git-files", "source_run", 1, "source.git-files")
        cur.execute(
            """
            SELECT source_name, workflow_id, run_id,
                   attributes->>'component',
                   (payload_summary->>'records')::int
              FROM okg.v_telemetry_span_tree
             WHERE trace_id = %s AND span_id = %s
            """,
            (trace_id, span_id),
        )
        assert cur.fetchone() == (
            "git-files",
            "wf-1",
            "run-1",
            "source-sync",
            3,
        )


def test_payload_summary_is_bounded(clean_runtime: str) -> None:
    trace_id = new_trace_id()
    span_id = new_span_id()
    now = datetime.now(timezone.utc)
    with psycopg.connect(clean_runtime) as conn:
        record_span(
            conn,
            TelemetrySpan(
                trace_id=trace_id,
                span_id=span_id,
                span_name="mcp.query",
                deployment_name="default",
                started_at=now,
                ended_at=now,
                payload_summary={"raw": "x" * 9000},
            ),
        )
        conn.commit()

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT payload_summary->>'payload_truncated',
                   (payload_summary->>'payload_original_bytes')::int
              FROM okg.telemetry_spans
             WHERE trace_id = %s
            """,
            (trace_id,),
        )
        truncated, original_bytes = cur.fetchone()
        assert truncated == "true"
        assert original_bytes > 8192


def test_trace_summary_preserves_error_and_wall_duration(
    clean_runtime: str,
) -> None:
    trace_id = new_trace_id()
    now = datetime.now(timezone.utc)
    with psycopg.connect(clean_runtime) as conn:
        record_span(
            conn,
            TelemetrySpan(
                trace_id=trace_id,
                span_id=new_span_id(),
                span_name="source.failed",
                status_code="error",
                status_message="boom",
                deployment_name="default",
                started_at=now,
                ended_at=now + timedelta(milliseconds=10),
            ),
        )
        record_span(
            conn,
            TelemetrySpan(
                trace_id=trace_id,
                span_id=new_span_id(),
                span_name="mcp.after",
                status_code="ok",
                deployment_name="default",
                started_at=now + timedelta(milliseconds=20),
                ended_at=now + timedelta(milliseconds=30),
            ),
        )
        conn.commit()

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status_code, status_message, duration_ms
              FROM okg.telemetry_traces
             WHERE deployment_name = 'default'
               AND trace_id = %s
            """,
            (trace_id,),
        )
        status_code, status_message, duration_ms = cur.fetchone()
    assert status_code == "error"
    assert status_message == "boom"
    assert duration_ms == 30
