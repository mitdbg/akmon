"""Operator-facing reads over the unified telemetry store.

What remains here is what still has a caller: the `okg telemetry` CLI
(`trace list` / `trace show` / `report`) and the span-window composer
`telemetry_coverage`, which that CLI's `report` reads.

`performance_snapshot`, `telemetry_metric_series`,
`telemetry_metric_distribution`, `aggregate_flamegraph` and
`telemetry_artifact_file` served only the seven `/viz/api/telemetry/*` routes
behind the Performance view. They and their tests were removed in issue #1924.
`telemetry_timeline` went the same way: `/viz/api/telemetry/timeline` was its
only caller, so it and the assertions that covered only it are gone too.
"""
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
from okg.substrate.telemetry.operator import (
    telemetry_coverage,
    trace_show,
)
from okg.substrate.telemetry.profile import build_profile_bundle


pytestmark = pytest.mark.postgres


def _seed_operator_trace(
    dsn: str,
    *,
    deployment: str,
    profile_dir: Path,
) -> str:
    trace_id = new_trace_id()
    generation_id = (
        f"okg:{deployment}:branch:default:gen:"
        f"20260629T000000000000Z:{trace_id[-12:]}"
    )
    source_span = new_span_id()
    mcp_span = new_span_id()
    publisher_span = new_span_id()
    now = datetime.now(timezone.utc)
    with psycopg.connect(dsn, autocommit=True) as conn:
        record_span(
            conn,
            TelemetrySpan(
                trace_id=trace_id,
                span_id=source_span,
                span_name="source.operator",
                span_kind="internal",
                deployment_name=deployment,
                generation_id=generation_id,
                source_name="operator",
                run_id="operator-run",
                started_at=now,
                ended_at=now + timedelta(milliseconds=50),
                payload_summary={"records": 1},
                root_name="source.operator",
                root_kind="source_run",
            ),
        )
        record_span(
            conn,
            TelemetrySpan(
                trace_id=trace_id,
                span_id=mcp_span,
                parent_span_id=source_span,
                span_name="mcp.inspect",
                span_kind="server",
                deployment_name=deployment,
                generation_id=generation_id,
                tool_name="inspect",
                session_id="operator-session",
                started_at=now + timedelta(milliseconds=10),
                ended_at=now + timedelta(milliseconds=25),
                payload_summary={"response_bytes": 256},
                unavailable_reason="query_timing_collector_not_configured",
            ),
        )
        record_span(
            conn,
            TelemetrySpan(
                trace_id=trace_id,
                span_id=publisher_span,
                parent_span_id=source_span,
                span_name="publisher.cycle",
                span_kind="internal",
                deployment_name=deployment,
                generation_id=generation_id,
                started_at=now + timedelta(milliseconds=50),
                ended_at=now + timedelta(milliseconds=170),
                payload_summary={
                    "timings_ms": {
                        "enrichers_ms": 31.5,
                        "view_stack_ms": 42.25,
                        "native_views_ms": 20.0,
                        "attribute_effective_view_materialize_ms": 7.5,
                        "consistency_gates_ms": 9.0,
                        "checkpoint_generation_ms": 3.0,
                    },
                },
            ),
        )
        record_metric(
            conn,
            TelemetryMetric(
                metric_name="telemetry.overhead.write_ms",
                metric_kind="gauge",
                value_double=2.0,
                unit="ms",
                deployment_name=deployment,
                trace_id=trace_id,
                span_id=mcp_span,
                labels={"scenario": "operator"},
            ),
        )
        build_profile_bundle(
            conn,
            deployment=deployment,
            trace_id=trace_id,
            deep=True,
        )
    assert profile_dir.exists()
    return trace_id


def test_operator_trace_list_show_and_report(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    deployment = f"operator-surface-{uuid.uuid4().hex[:8]}"
    profile_dir = tmp_path / "profiles"
    monkeypatch.setenv("OKG_PROFILE_DIR", str(profile_dir))
    trace_id = _seed_operator_trace(
        clean_runtime,
        deployment=deployment,
        profile_dir=profile_dir,
    )

    rc = main([
        "telemetry", "trace", "list",
        "--deployment", deployment,
        "--dsn", clean_runtime,
        "--slow",
        "--json",
    ])
    assert rc == 0
    listed = json.loads(capsys.readouterr().out)
    assert listed["schema"] == "okg.telemetry.trace_list.v1"
    assert listed["traces"][0]["trace_id"] == trace_id
    assert listed["traces"][0]["slowest_span_name"] == "publisher.cycle"

    rc = main([
        "telemetry", "trace", "show",
        "--deployment", deployment,
        "--dsn", clean_runtime,
        trace_id,
        "--include-profile",
        "--json",
    ])
    assert rc == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["schema"] == "okg.telemetry.trace_show.v1"
    assert shown["trace"]["artifact_count"] == 3
    assert shown["profile_report"]["critical_path"]["span_name"] == (
        "publisher.cycle"
    )
    assert shown["unavailable_segments"][0]["reason"] == (
        "query_timing_collector_not_configured"
    )

    rc = main([
        "telemetry", "report",
        "--deployment", deployment,
        "--dsn", clean_runtime,
        "--trace-id", trace_id,
        "--json",
    ])
    assert rc == 0
    report = json.loads(capsys.readouterr().out)
    assert report["schema"] == "okg.telemetry.operator_report.v1"
    assert report["slowest_stage"]["span_name"] == "publisher.cycle"
    assert report["artifact_count"] == 3
    metric_names = {metric["name"] for metric in report["metrics"]["selected"]}
    assert "telemetry.overhead.latest_write_ms" in metric_names
    assert "queue.dlq.count" in metric_names
    assert any(
        metric["status"] == "unavailable"
        for metric in report["metrics"]["selected"]
    )


def test_trace_show_is_scoped_by_deployment(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trace_id = new_trace_id()
    now = datetime.now(timezone.utc)
    monkeypatch.setenv("OKG_PROFILE_DIR", str(tmp_path / "profiles"))
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_span(
            conn,
            TelemetrySpan(
                trace_id=trace_id,
                span_id=new_span_id(),
                span_name="source.alpha",
                deployment_name="alpha",
                source_name="alpha",
                started_at=now,
                ended_at=now + timedelta(milliseconds=5),
            ),
        )
        record_span(
            conn,
            TelemetrySpan(
                trace_id=trace_id,
                span_id=new_span_id(),
                span_name="source.beta",
                deployment_name="beta",
                source_name="beta",
                started_at=now,
                ended_at=now + timedelta(milliseconds=25),
            ),
        )
        build_profile_bundle(conn, deployment="beta", trace_id=trace_id)

    with psycopg.connect(clean_runtime) as conn:
        alpha = trace_show(
            conn,
            deployment="alpha",
            trace_id=trace_id,
            include_profile=False,
        )
        beta = trace_show(
            conn,
            deployment="beta",
            trace_id=trace_id,
            include_profile=False,
        )

    assert alpha["trace"]["span_count"] == 1
    assert alpha["trace"]["slowest_span_name"] == "source.alpha"
    assert alpha["trace"]["artifact_count"] == 0
    assert [span["span_name"] for span in alpha["spans"]] == ["source.alpha"]
    assert alpha["artifacts"] == []

    assert beta["trace"]["span_count"] == 1
    assert beta["trace"]["slowest_span_name"] == "source.beta"
    assert beta["trace"]["artifact_count"] == 3
    assert [span["span_name"] for span in beta["spans"]] == ["source.beta"]


def test_scoped_coverage_filters_workflow_and_component(
    clean_runtime: str,
) -> None:
    deployment = f"operator-scope-{uuid.uuid4().hex[:8]}"
    now = datetime.now(timezone.utc)
    trace_a = new_trace_id()
    trace_b = new_trace_id()
    root_a = new_span_id()
    root_b = new_span_id()
    mixed_scope_span = new_span_id()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for trace_id, root_span, workflow_id, source, offset_ms in (
            (trace_a, root_a, "wf-a", "scoped-a", 0),
            (trace_b, root_b, "wf-b", "scoped-b", 5),
        ):
            start = now + timedelta(milliseconds=offset_ms)
            generation_id = f"gen-{workflow_id}"
            record_span(
                conn,
                TelemetrySpan(
                    trace_id=trace_id,
                    span_id=root_span,
                    span_name=f"source.{source}",
                    deployment_name=deployment,
                    generation_id=generation_id,
                    source_name=source,
                    workflow_id=workflow_id,
                    run_id=f"run-{workflow_id}",
                    started_at=start,
                    ended_at=start + timedelta(milliseconds=40),
                    root_name=f"source.{source}",
                    root_kind="source_run",
                ),
            )
            record_span(
                conn,
                TelemetrySpan(
                    trace_id=trace_id,
                    span_id=new_span_id(),
                    parent_span_id=root_span,
                    span_name=f"source.{source}.flush.chronos_apply",
                    span_kind="client",
                    deployment_name=deployment,
                    generation_id=generation_id,
                    source_name=source,
                    workflow_id=workflow_id,
                    run_id=f"run-{workflow_id}",
                    started_at=start + timedelta(milliseconds=10),
                    ended_at=start + timedelta(milliseconds=30),
                    attributes={
                        "okg.component": "publisher",
                        "okg.subcomponent": "chronos",
                        "db.system.name": "postgresql",
                        "db.operation.name": "EXECUTE",
                        "db.namespace": "okg",
                    },
                    root_name=f"source.{source}",
                    root_kind="source_run",
                ),
            )
        record_span(
            conn,
            TelemetrySpan(
                trace_id=trace_a,
                span_id=mixed_scope_span,
                parent_span_id=root_a,
                span_name="source.scoped-a.foreign_workflow_child",
                span_kind="internal",
                deployment_name=deployment,
                generation_id="gen-wf-b",
                source_name="scoped-a",
                workflow_id="wf-b",
                run_id="run-wf-b-mixed",
                started_at=now + timedelta(milliseconds=31),
                ended_at=now + timedelta(milliseconds=36),
                root_name="source.scoped-a",
                root_kind="source_run",
            ),
        )

    window_start = now - timedelta(milliseconds=1)
    window_end = now + timedelta(milliseconds=100)
    with psycopg.connect(clean_runtime) as conn:
        workflow_coverage = telemetry_coverage(
            conn,
            deployment=deployment,
            start_at=window_start,
            end_at=window_end,
            scope="workflow",
            workflow_id="wf-a",
        )
        component_coverage = telemetry_coverage(
            conn,
            deployment=deployment,
            start_at=window_start,
            end_at=window_end,
            scope="component",
            component="chronos",
        )

    assert workflow_coverage["schema"] == "okg.telemetry.coverage.v1"
    assert workflow_coverage["scope"] == "workflow"
    assert workflow_coverage["filters"] == {"workflow_id": "wf-a"}
    assert workflow_coverage["trace_count"] == 1
    assert workflow_coverage["span_count"] == 2
    assert workflow_coverage["summary"]["parent_duration_ms"] == 40.0
    assert workflow_coverage["summary"]["direct_child_covered_ms"] == 20.0
    assert workflow_coverage["summary"]["self_unattributed_ms"] == 20.0
    assert workflow_coverage["summary"]["relationship_parent_duration_ms"] == 60.0
    assert workflow_coverage["summary"]["relationship_direct_child_covered_ms"] == 20.0
    coverage_root = next(
        row for row in workflow_coverage["records"]
        if row["span_name"] == "source.scoped-a"
    )
    assert coverage_root["parent_duration_ms"] == 40.0
    assert coverage_root["direct_child_covered_ms"] == 20.0
    assert coverage_root["self_unattributed_ms"] == 20.0
    assert coverage_root["coverage_pct"] == 50.0
    assert coverage_root["gap_count"] == 2
    assert [gap["cause"] for gap in coverage_root["gaps"]] == [
        "before_first_child",
        "after_last_child",
    ]
    assert coverage_root["children"][0]["span_name"] == (
        "source.scoped-a.flush.chronos_apply"
    )

    assert component_coverage["scope"] == "component"
    assert component_coverage["filters"] == {"component": "chronos"}
    assert component_coverage["span_count"] == 2
    assert component_coverage["summary"]["parent_duration_ms"] == 40.0
    assert {
        row["span_name"] for row in component_coverage["records"]
    } == {
        "source.scoped-a.flush.chronos_apply",
        "source.scoped-b.flush.chronos_apply",
    }
    # A scoped read keeps only part of a trace, so a selected span whose parent
    # was filtered out must be reparented to a root and must say which parent it
    # lost, or the coverage tree would silently claim the child had no parent.
    assert {
        row["parent_span_id"] for row in component_coverage["records"]
    } == {None}
    assert {
        row["detached_parent_span_id"] for row in component_coverage["records"]
    } == {root_a, root_b}


def test_coverage_detaches_a_parent_outside_the_component_scope(
    clean_runtime: str,
) -> None:
    deployment = f"operator-detach-{uuid.uuid4().hex[:8]}"
    now = datetime.now(timezone.utc)
    trace_id = new_trace_id()
    root_span = new_span_id()
    child_span = new_span_id()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_span(
            conn,
            TelemetrySpan(
                trace_id=trace_id,
                span_id=root_span,
                span_name="source.detach-root",
                deployment_name=deployment,
                source_name="detach-test",
                started_at=now,
                ended_at=now + timedelta(milliseconds=40),
                root_name="source.detach-root",
                root_kind="source_run",
            ),
        )
        record_span(
            conn,
            TelemetrySpan(
                trace_id=trace_id,
                span_id=child_span,
                parent_span_id=root_span,
                span_name="source.detach-root.flush.chronos_apply",
                span_kind="client",
                deployment_name=deployment,
                source_name="detach-test",
                started_at=now + timedelta(milliseconds=10),
                ended_at=now + timedelta(milliseconds=30),
                attributes={
                    "okg.component": "publisher",
                    "okg.subcomponent": "chronos",
                },
                root_name="source.detach-root",
                root_kind="source_run",
            ),
        )

    with psycopg.connect(clean_runtime) as conn:
        coverage = telemetry_coverage(
            conn,
            deployment=deployment,
            start_at=now - timedelta(milliseconds=1),
            end_at=now + timedelta(milliseconds=100),
            scope="component",
            component="chronos",
        )

    assert coverage["span_count"] == 1
    record = coverage["records"][0]
    assert record["span_name"] == "source.detach-root.flush.chronos_apply"
    # The parent is outside the component scope, so it is not selected. The
    # child becomes a root of the returned tree and keeps the id it lost.
    assert record["parent_span_id"] is None
    assert record["detached_parent_span_id"] == root_span
    # Detached children are roots, so they count towards the headline totals.
    assert coverage["summary"]["parent_duration_ms"] == 20.0


def test_coverage_window_uses_overlap_membership_not_containment(
    clean_runtime: str,
) -> None:
    deployment = f"operator-window-overlap-{uuid.uuid4().hex[:8]}"
    window_end = datetime.now(timezone.utc)
    window_start = window_end - timedelta(hours=1)
    crossing_trace_id = new_trace_id()
    inside_trace_id = new_trace_id()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for trace_id, started_at, ended_at, name in (
            (
                crossing_trace_id,
                window_start - timedelta(minutes=5),
                window_start + timedelta(minutes=5),
                "source.crossing-window",
            ),
            (
                inside_trace_id,
                window_start + timedelta(minutes=10),
                window_start + timedelta(minutes=11),
                "source.inside-window",
            ),
        ):
            record_span(
                conn,
                TelemetrySpan(
                    trace_id=trace_id,
                    span_id=new_span_id(),
                    span_name=name,
                    span_kind="internal",
                    deployment_name=deployment,
                    source_name="window-test",
                    started_at=started_at,
                    ended_at=ended_at,
                    root_name=name,
                    root_kind="source_run",
                ),
            )

    with psycopg.connect(clean_runtime) as conn:
        coverage = telemetry_coverage(
            conn,
            deployment=deployment,
            start_at=window_start,
            end_at=window_end,
        )

    # A span that starts before the window and ends inside it overlaps the
    # window and belongs to it. Containment would drop it and the operator
    # would read a window that silently omits the work already running.
    assert coverage["candidate_span_count"] == 2
    assert coverage["span_count"] == 2
    assert {
        row["span_name"] for row in coverage["records"]
    } == {"source.crossing-window", "source.inside-window"}


def test_coverage_excludes_spans_that_end_at_window_start(
    clean_runtime: str,
) -> None:
    deployment = f"operator-window-boundary-{uuid.uuid4().hex[:8]}"
    trace_id = new_trace_id()
    boundary_span = new_span_id()
    active_span = new_span_id()
    now = datetime.now(timezone.utc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_span(
            conn,
            TelemetrySpan(
                trace_id=trace_id,
                span_id=boundary_span,
                span_name="source.boundary-before-window",
                deployment_name=deployment,
                workflow_id="wf-boundary",
                started_at=now - timedelta(milliseconds=50),
                ended_at=now,
                root_name="source.boundary-before-window",
                root_kind="source_run",
            ),
        )
        record_span(
            conn,
            TelemetrySpan(
                trace_id=trace_id,
                span_id=active_span,
                span_name="source.active-in-window",
                deployment_name=deployment,
                workflow_id="wf-boundary",
                started_at=now + timedelta(milliseconds=1),
                ended_at=now + timedelta(milliseconds=11),
                root_name="source.active-in-window",
                root_kind="source_run",
            ),
        )

    with psycopg.connect(clean_runtime) as conn:
        coverage = telemetry_coverage(
            conn,
            deployment=deployment,
            start_at=now,
            end_at=now + timedelta(milliseconds=20),
            scope="workflow",
            workflow_id="wf-boundary",
        )

    assert coverage["span_count"] == 1
    assert coverage["candidate_span_count"] == 1
    assert coverage["record_count"] == 1
    assert [record["span_id"] for record in coverage["records"]] == [active_span]
