from __future__ import annotations

import json
from pathlib import Path

import psycopg
import pytest

from okg.substrate.cli.main import main
from okg.substrate.db.pool import get_pool
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.base import SourceRun
from okg.substrate.llm.tracker import record_llm_call
from okg.substrate.mcp.tracing import trace_tool_call
from okg.substrate.publisher.publisher import Publisher
from okg.substrate.telemetry import (
    TelemetryMetric,
    record_metric,
    use_trace,
)
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


class _LiveProofAdapter:
    name = "telemetry-live-proof"
    profile = "discovery_crawl"

    def run(self, run_id: str, *, mode: str = "cursor") -> SourceRun:
        return SourceRun(
            facts=[],
            completed_scope=True,
            next_cursor={"run_id": run_id, "status": "live-proof"},
            run_mode=mode,
        )


def test_isolated_source_to_read_live_proof(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    deployment = "default"
    monkeypatch.setenv("OKG_PROFILE_DIR", str(tmp_path / "profiles"))

    with psycopg.connect(clean_runtime) as conn:
        seed_catalog_version(conn)
        conn.commit()

    runner = SourceRunner(
        clean_runtime,
        source_name=_LiveProofAdapter.name,
        deployment_name=deployment,
        write_chronos_graph=False,
        write_fact_log=True,
    )
    run_result = runner.run(_LiveProofAdapter())
    trace_id = run_result.trace_id
    assert trace_id

    publisher = Publisher(clean_runtime, deployment, catalog_identity=bind_test_catalog_writer(clean_runtime, deployment))
    publish_result = publisher.cycle()
    assert publish_result["status"] == "published"
    generation_id = publish_result["generation_id"]

    trace_tool_call(
        get_pool(clean_runtime),
        deployment_name=deployment,
        tool_name="inspect",
        request={"target": {"kind": "graph"}},
        response={
            "node_count": 0,
            "total_count": 1,
            "query_plan_summary": "cost=1.0..2.0 rows=1",
        },
        duration_ms=9,
        generation_id=generation_id,
        session_id="live-proof-session",
    )

    record_llm_call(
        clean_runtime,
        caller="telemetry-live-proof",
        model="test-model",
        fragment_id="fragment-live",
        prompt_tokens=3,
        completion_tokens=4,
        total_tokens=7,
        cost_usd=0.003,
        latency_ms=13,
        success=True,
        deployment=deployment,
        generation_id=generation_id,
        extras={"proof": "live"},
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_metric(
            conn,
            TelemetryMetric(
                metric_name="telemetry.overhead.write_ms",
                metric_kind="gauge",
                value_double=2.5,
                unit="ms",
                deployment_name=deployment,
                trace_id=trace_id,
                labels={"scenario": "live-proof"},
            ),
        )

    rc = main([
        "telemetry", "profile",
        "--deployment", deployment,
        "--dsn", clean_runtime,
        "--trace-id", trace_id,
        "--deep",
        # `profile` writes a profile artifact, and CLI write families
        # require an explicit --apply (#2019).
        "--apply",
        "--json",
    ])
    assert rc == 0
    profile_bundle = json.loads(capsys.readouterr().out)
    assert profile_bundle["trace_id"] == trace_id
    assert Path(profile_bundle["artifact_dir"], "speedscope.json").is_file()

    rc = main([
        "telemetry", "trace", "show",
        "--deployment", deployment,
        "--dsn", clean_runtime,
        trace_id,
        "--include-profile",
        "--json",
    ])
    assert rc == 0
    trace = json.loads(capsys.readouterr().out)
    assert trace["trace"]["generation_id"] == str(generation_id)
    assert trace["trace"]["artifact_count"] == 3
    span_names = {span["span_name"] for span in trace["spans"]}
    assert span_names >= {
        "source.telemetry-live-proof",
        "publisher.cycle",
        "mcp.inspect",
        "llm.telemetry-live-proof",
    }
    assert "source.telemetry-live-proof.adapter_run" in span_names
    assert "publisher.cycle.catalog_version" in span_names
    assert any(
        name.startswith("publisher.cycle.checkpoint_generation.operation.")
        for name in span_names
    )
    assert any(
        span.get("span_kind") == "client"
        and span.get("attributes", {}).get("db.system.name") == "postgresql"
        for span in trace["spans"]
    )
    assert any(
        segment["reason"] == "source_runner.partial_context"
        for segment in trace["unavailable_segments"]
    )

    rc = main([
        "telemetry", "metrics",
        "--deployment", deployment,
        "--dsn", clean_runtime,
        "--since", "1h",
        "--json",
    ])
    assert rc == 0
    metrics = json.loads(capsys.readouterr().out)
    by_name = {metric["name"]: metric for metric in metrics["metrics"]}
    assert by_name["telemetry.spans.count"]["value"] >= 3
    assert by_name["telemetry.overhead.latest_write_ms"]["status"] == "ok"

    rc = main([
        "telemetry", "report",
        "--deployment", deployment,
        "--dsn", clean_runtime,
        "--trace-id", trace_id,
        "--json",
    ])
    assert rc == 0
    report = json.loads(capsys.readouterr().out)
    assert report["trace_id"] == trace_id
    assert report["profile"]["critical_path"]["span_name"].startswith(
        ("llm.", "publisher.", "source.")
    )
    assert report["artifact_count"] == 3

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT generation_id, status_code, parent_span_id
              FROM okg.telemetry_spans
             WHERE trace_id = %s
               AND span_name = 'publisher.cycle'
            """,
            (trace_id,),
        )
        assert cur.fetchone() == (
            str(generation_id),
            "ok",
            run_result.telemetry_span_id,
        )
        cur.execute(
            """
            SELECT count(*)
              FROM okg.telemetry_spans child
              LEFT JOIN okg.telemetry_spans parent
                ON parent.deployment_name = child.deployment_name
               AND parent.trace_id = child.trace_id
               AND parent.span_id = child.parent_span_id
             WHERE child.deployment_name = %s
               AND child.trace_id = %s
               AND child.parent_span_id IS NOT NULL
               AND parent.span_id IS NULL
            """,
            (deployment, trace_id),
        )
        assert cur.fetchone()[0] == 0
        cur.execute(
            """
            SELECT value
              FROM okg.source_progress
             WHERE deployment = %s
               AND source_name = %s
               AND key = 'sync.last_run'
            """,
            (deployment, _LiveProofAdapter.name),
        )
        progress = json.loads(cur.fetchone()[0])
    assert progress["trace_id"] == trace_id
    assert progress["run_id"] == run_result.run_id


def test_nested_source_context_records_bridge_without_reusing_source_span(
    clean_runtime: str,
) -> None:
    deployment = "default"
    with psycopg.connect(clean_runtime) as conn:
        seed_catalog_version(conn)
        conn.commit()

    with use_trace() as root_identity:
        first = SourceRunner(
            clean_runtime,
            source_name="telemetry-nested-a",
            deployment_name=deployment,
            write_chronos_graph=False,
            write_fact_log=True,
        ).run(_LiveProofAdapter())
        second = SourceRunner(
            clean_runtime,
            source_name="telemetry-nested-b",
            deployment_name=deployment,
            write_chronos_graph=False,
            write_fact_log=True,
        ).run(_LiveProofAdapter())

    assert first.trace_id == root_identity.trace_id
    assert second.trace_id == root_identity.trace_id
    assert first.telemetry_span_id != root_identity.span_id
    assert second.telemetry_span_id != root_identity.span_id
    assert first.telemetry_span_id != second.telemetry_span_id

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT span_name, parent_span_id
              FROM okg.telemetry_spans
             WHERE deployment_name = %s
               AND trace_id = %s
               AND span_id = %s
            """,
            (deployment, root_identity.trace_id, root_identity.span_id),
        )
        assert cur.fetchone() == ("ingest.run", None)

        cur.execute(
            """
            SELECT span_id, parent_span_id
              FROM okg.telemetry_spans
             WHERE deployment_name = %s
               AND trace_id = %s
               AND span_name IN (
                 'source.telemetry-nested-a',
                 'source.telemetry-nested-b'
               )
             ORDER BY span_name
            """,
            (deployment, root_identity.trace_id),
        )
        source_rows = cur.fetchall()
        assert source_rows == [
            (first.telemetry_span_id, root_identity.span_id),
            (second.telemetry_span_id, root_identity.span_id),
        ]

        cur.execute(
            """
            SELECT count(*)
              FROM okg.telemetry_spans child
              LEFT JOIN okg.telemetry_spans parent
                ON parent.deployment_name = child.deployment_name
               AND parent.trace_id = child.trace_id
               AND parent.span_id = child.parent_span_id
             WHERE child.deployment_name = %s
               AND child.trace_id = %s
               AND child.parent_span_id IS NOT NULL
               AND parent.span_id IS NULL
            """,
            (deployment, root_identity.trace_id),
        )
        assert cur.fetchone()[0] == 0


def test_source_runner_deep_span_capture_can_be_disabled(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = "default"
    with psycopg.connect(clean_runtime) as conn:
        seed_catalog_version(conn)
        conn.commit()

    monkeypatch.setenv("OKG_TELEMETRY_DEEP_SPANS", "off")
    result = SourceRunner(
        clean_runtime,
        source_name="telemetry-shallow-toggle",
        deployment_name=deployment,
        write_chronos_graph=False,
        write_fact_log=True,
    ).run(_LiveProofAdapter())

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT span_name
              FROM okg.telemetry_spans
             WHERE deployment_name = %s
               AND trace_id = %s
             ORDER BY span_name
            """,
            (deployment, result.trace_id),
        )
        span_names = [row[0] for row in cur.fetchall()]

    assert span_names == ["source.telemetry-shallow-toggle"]
