from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import uuid

import psycopg
import pytest

from okg.substrate.cli.main import main
from okg.substrate.telemetry import (
    TelemetryMetric,
    TelemetrySpan,
    new_span_id,
    new_trace_id,
    record_metric,
    record_span,
)


pytestmark = pytest.mark.postgres


def _seed_profile_trace(dsn: str, *, deployment: str) -> str:
    trace_id = new_trace_id()
    root_span = new_span_id()
    child_span = new_span_id()
    now = datetime.now(timezone.utc)
    with psycopg.connect(dsn) as conn:
        record_span(
            conn,
            TelemetrySpan(
                trace_id=trace_id,
                span_id=root_span,
                span_name="source.profile-bundle",
                span_kind="internal",
                deployment_name=deployment,
                source_name="profile-bundle",
                run_id="profile-run-1",
                started_at=now,
                ended_at=now + timedelta(milliseconds=35),
                payload_summary={"records": 2},
                root_name="source.profile-bundle",
                root_kind="source_run",
            ),
        )
        record_span(
            conn,
            TelemetrySpan(
                trace_id=trace_id,
                span_id=child_span,
                parent_span_id=root_span,
                span_name="mcp.inspect",
                span_kind="server",
                deployment_name=deployment,
                tool_name="inspect",
                session_id="profile-session",
                started_at=now + timedelta(milliseconds=5),
                ended_at=now + timedelta(milliseconds=18),
                payload_summary={
                    "response_bytes": 128,
                    "query_plan_summary": "cost=1.0..2.0 rows=3",
                },
            ),
        )
        record_metric(
            conn,
            TelemetryMetric(
                metric_name="telemetry.overhead.write_ms",
                metric_kind="gauge",
                value_double=3.25,
                unit="ms",
                deployment_name=deployment,
                trace_id=trace_id,
                span_id=child_span,
                labels={"scenario": "profile-test"},
            ),
        )
        conn.commit()
    return trace_id


def test_telemetry_profile_bundle_cli_writes_speedscope_and_report(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    deployment = f"profile-bundle-{uuid.uuid4().hex[:8]}"
    trace_id = _seed_profile_trace(clean_runtime, deployment=deployment)
    monkeypatch.setenv("OKG_PROFILE_DIR", str(tmp_path / "profiles"))

    rc = main([
        "telemetry", "profile",
        "--deployment", deployment,
        "--dsn", clean_runtime,
        "--trace-id", trace_id,
        "--deep",
        # Building a bundle writes four files and inserts artifact rows, so the
        # command asks for the operator's authority to do it.
        "--apply",
        "--json",
    ])
    assert rc == 0
    bundle = json.loads(capsys.readouterr().out)
    assert bundle["schema"] == "okg.telemetry.profile_bundle.v1"
    assert bundle["deep"] is True
    assert bundle["span_count"] == 2
    assert bundle["critical_path"]["span_name"] == "source.profile-bundle"
    assert bundle["query_timings"][0]["query_plan_summary"].startswith("cost=")

    artifact_dir = Path(bundle["artifact_dir"])
    speedscope = json.loads((artifact_dir / "speedscope.json").read_text())
    assert speedscope["profiles"][0]["type"] == "evented"
    assert len(speedscope["shared"]["frames"]) == 2
    assert {artifact["artifact_kind"] for artifact in bundle["artifacts"]} == {
        "profile-bundle",
        "profile-report",
        "speedscope",
    }
    assert any(
        item["collector"] == "postgres-flamegraph"
        and item["status"] == "unavailable"
        for item in bundle["collector_availability"]
    )
    assert any(
        item["collector"] == "python-sampler"
        and item["status"] == "unavailable"
        and item["reason"] == "python_sampler_not_configured"
        for item in bundle["collector_availability"]
    )

    rc = main([
        "telemetry", "profile", "report",
        "--deployment", deployment,
        "--dsn", clean_runtime,
        "--trace-id", trace_id,
        "--apply",
        "--json",
    ])
    assert rc == 0
    report = json.loads(capsys.readouterr().out)
    assert report["schema"] == "okg.telemetry.profile_report.v1"
    assert report["artifact_dir"] == str(artifact_dir)
    assert report["critical_path"]["duration_ms"] == 35
    assert report["overhead"]["metric_count"] == 1

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT artifact_kind, collector_status, summary
              FROM okg.telemetry_artifacts
             WHERE deployment_name = %s
               AND trace_id = %s
            """,
            (deployment, trace_id),
        )
        rows = cur.fetchall()
    assert len(rows) == 3
    assert {row[0] for row in rows} == {
        "profile-bundle",
        "profile-report",
        "speedscope",
    }
    assert {row[1] for row in rows} == {"available"}
    speedscope_summary = next(row[2] for row in rows if row[0] == "speedscope")
    assert speedscope_summary["profile_type"] == "evented"
    assert speedscope_summary["python_samples"] is False
