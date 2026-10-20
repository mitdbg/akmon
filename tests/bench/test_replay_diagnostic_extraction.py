from __future__ import annotations

import json
import base64
import pickle
from datetime import datetime, timezone

import pytest

from okg.bench.extract import (
    _aggregate_publish_timings,
    _aggregate_source_runs,
    _diagnostic_measurements,
    _decode_dbos_workflow_output,
    _pair_replay_workflow,
    _publish_run_summary,
    _publish_timing_from_progress_row,
    _replay_timing_from_raw_row,
    _replay_workflow_timing,
    _source_run_from_progress_row,
    _stage_bottleneck,
    _source_run_summary,
    _workflow_ingest_report_fields,
)
from okg.bench.orchestrator import (
    Experiment,
    Repo,
    Replay,
    _build_profile_run_from_source_run,
    _profile_measurements_dry_run,
    _select_diagnostic_source_run,
    _write_profile_drilldown,
)


def test_aggregate_source_runs_ranks_sources_and_phases() -> None:
    runs = [
        {
            "updated_ms": 100,
            "source_name": "svc.git-files",
            "registry_source_name": "git-files",
            "run_id": "files-1",
            "total_ms": 40.0,
            "source_runner_ms": 35.0,
            "slowest_phase": "source_runner.flush.dedupe_persisted",
            "slowest_phase_ms": 20.0,
            "timings_ms": {
                "total": 40.0,
                "source_runner": 35.0,
                "source_runner.adapter_run": 15.0,
                "source_runner.flush.dedupe_persisted": 20.0,
            },
        },
        {
            "updated_ms": 150,
            "source_name": "svc.git-history",
            "registry_source_name": "git-history",
            "run_id": "history-1",
            "total_ms": 200.0,
            "source_runner_ms": 190.0,
            "slowest_phase": "source_runner.adapter_run",
            "slowest_phase_ms": 180.0,
            "timings_ms": {
                "total": 200.0,
                "source_runner": 190.0,
                "source_runner.adapter_run": 180.0,
                "source_runner.flush.dedupe_persisted": 8.0,
            },
        },
        {
            "updated_ms": 300,
            "source_name": "svc.git-history",
            "registry_source_name": "git-history",
            "run_id": "history-late",
            "total_ms": 999.0,
            "source_runner_ms": 990.0,
            "slowest_phase": "source_runner.adapter_run",
            "slowest_phase_ms": 990.0,
            "timings_ms": {
                "total": 999.0,
                "source_runner.adapter_run": 990.0,
            },
        },
    ]

    out = _aggregate_source_runs(runs, start_ms=0, end_ms=200)

    assert out["source_diag_status"] == "available"
    assert out["source_diag_reason"] == "time_window"
    assert out["source_run_count"] == 2
    assert out["source_total_ms"] == pytest.approx(240.0)
    assert out["slowest_source"] == "git-history"
    assert out["slowest_phase"] == "source_runner.adapter_run"
    assert out["slowest_phase_ms"] == pytest.approx(180.0)
    assert out["top_sources"].startswith("git-history=200.000")
    assert "source_runner.adapter_run=195.000" in out["top_phases"]


def test_aggregate_source_runs_reports_missing_window() -> None:
    out = _aggregate_source_runs([], start_ms=None, end_ms=200)

    assert out["source_diag_status"] == "unavailable"
    assert out["source_diag_reason"] == "missing_time_window"
    assert out["source_run_count"] == 0


def test_source_run_from_progress_row_extracts_hot_phase() -> None:
    payload = {
        "source_name": "svc.git-files",
        "registry_source_name": "git-files",
        "run_id": "run-1",
        "status": "OK",
        "timing_status": "available",
        "timings_ms": {
            "total": 200,
            "source_runner": 120,
            "source_runner.adapter_run": "35.5",
            "source_runner.flush.dedupe_persisted": 50,
        },
        "nodes_appended": 1,
        "edges_appended": 2,
        "output_count": 3,
    }

    out = _source_run_from_progress_row(
        deployment_name="svc",
        source_progress_name="svc.git-files",
        key="sync.stage_timings.run.run-1",
        value=json.dumps(payload),
        updated_at=datetime(2026, 6, 9, 13, 0, tzinfo=timezone.utc),
    )

    assert out is not None
    assert out["registry_source_name"] == "git-files"
    assert out["run_id"] == "run-1"
    assert out["updated_ms"] is not None
    assert out["total_ms"] == pytest.approx(200.0)
    # `source_runner` is the whole-call rollup that `_run_source` stamps around
    # every `source_runner.<label>` beneath it, so it can never lose and never
    # discriminates. The hot phase is the slowest TERMINAL phase (issue #1325
    # item 4).
    assert out["slowest_phase"] == "source_runner.flush.dedupe_persisted"
    assert out["slowest_phase_ms"] == pytest.approx(50.0)
    assert out["nodes_appended"] == 1
    assert out["edges_appended"] == 2


def test_replay_timing_from_raw_row_preserves_benchmark_attribution() -> None:
    out = _replay_timing_from_raw_row({
        "bench_reset_started_epoch_ms": "1000",
        "bench_reset_completed_epoch_ms": "1042",
        "bench_prev_gen_query_ms": "0.25",
        "bench_reset_ms": "42.5",
        "bench_wait_new_gen_ms": "987.75",
        "bench_generation_notify_count": "1",
        "bench_latest_gen_poll_count": "83",
        "bench_latest_gen_query_ms": "11.2",
        "bench_latest_gen_query_max_ms": "0.9",
        "bench_queue_drain_ms": "",
        "bench_queue_poll_count": "0",
        "bench_queue_query_ms": "0",
        "bench_gen_metrics_ms": "1.5",
        "bench_db_size_ms": "3.25",
    })

    assert out["bench_reset_started_epoch_ms"] == pytest.approx(1000)
    assert out["bench_reset_completed_epoch_ms"] == pytest.approx(1042)
    assert out["bench_prev_gen_query_ms"] == pytest.approx(0.25)
    assert out["bench_reset_ms"] == pytest.approx(42.5)
    assert out["bench_wait_new_gen_ms"] == pytest.approx(987.75)
    assert out["bench_generation_notify_count"] == 1
    assert out["bench_latest_gen_poll_count"] == 83
    assert out["bench_queue_drain_ms"] is None
    assert out["bench_queue_poll_count"] == 0


def test_replay_timing_from_legacy_raw_row_marks_fields_missing() -> None:
    out = _replay_timing_from_raw_row({})

    assert all(value is None for value in out.values())


def test_replay_workflow_timing_derives_producer_boundaries() -> None:
    out = _replay_workflow_timing(
        replay_timing={"bench_reset_started_epoch_ms": 1_000.0},
        workflow={"created": 1_275, "started": 1_325},
        generation_published_ms=2_000,
    )

    assert out["bench_reset_to_workflow_created_ms"] == pytest.approx(275)
    assert out["bench_reset_to_workflow_started_ms"] == pytest.approx(325)
    assert out["bench_workflow_created_to_published_ms"] == pytest.approx(725)
    assert out["bench_workflow_started_to_published_ms"] == pytest.approx(675)


def test_replay_workflow_timing_keeps_legacy_rows_missing() -> None:
    out = _replay_workflow_timing(
        replay_timing={},
        workflow={"created": 1_275, "started": 1_325},
        generation_published_ms=2_000,
    )

    assert all(value is None for value in out.values())


def test_pair_replay_workflow_uses_reset_publish_window() -> None:
    old_workflow = {
        "workflow_uuid": "old",
        "created": 900,
        "started": 920,
    }
    current_workflow = {
        "workflow_uuid": "current",
        "created": 1_075,
        "started": 1_125,
    }

    workflow, reason = _pair_replay_workflow(
        [old_workflow, current_workflow],
        replay_timing={"bench_reset_started_epoch_ms": 1_000.0},
        generation_published_ms=1_400,
        previous_generation_published_ms=950,
    )

    assert workflow is current_workflow
    assert reason == "time_window"


def test_pair_replay_workflow_marks_two_workflows_in_one_window() -> None:
    # A split sync: two successful workflows land in one commit's window.
    # The earliest still supplies timing, but the reason tells the count gate
    # that its query_count covers only part of the commit.
    first = {"workflow_uuid": "a", "created": 1_050, "started": 1_060}
    second = {"workflow_uuid": "b", "created": 1_200, "started": 1_210}

    workflow, reason = _pair_replay_workflow(
        [second, first],
        replay_timing={"bench_reset_started_epoch_ms": 1_000.0},
        generation_published_ms=1_400,
        previous_generation_published_ms=950,
    )

    assert workflow is first
    assert reason == "ambiguous_time_window"


def test_pair_replay_workflow_rejects_pre_reset_workflow() -> None:
    workflow, reason = _pair_replay_workflow(
        [{
            "workflow_uuid": "old",
            "created": 900,
            "started": 920,
        }],
        replay_timing={"bench_reset_started_epoch_ms": 1_000.0},
        generation_published_ms=1_400,
        previous_generation_published_ms=950,
    )

    assert workflow is None
    assert reason == "workflow_not_in_replay_window"


def test_decode_dbos_workflow_output_reads_py_pickle_payload() -> None:
    payload = {"report": {"timings_ms": {"source_wall": 123.5}}}
    encoded = base64.b64encode(pickle.dumps(payload)).decode("ascii")

    assert _decode_dbos_workflow_output(
        encoded,
        serialization="py_pickle",
    ) == payload


def test_workflow_ingest_report_fields_extracts_numeric_timings() -> None:
    payload = {
        "report": {
            "query_count": 451,
            "timings_ms": {
                "total": 300.25,
                "source_wall": 120,
                "source_reported_total": 180,
                "ignored": "not-a-number",
            },
            "source_sync_workflow_timings_ms": {
                "pre_ingest_setup": 42.5,
                "run_ingest": 300.25,
                "ignored": "not-a-number",
            },
            "layer_timings": [{
                "index": 1,
                "sources": ["git-files"],
                "source_wall_ms": 120,
            }],
            "results": [{
                "name": "git-files",
                "run_id": "run-1",
                "status": "OK",
            }],
            "publishes": [{
                "label": "after-layer-1",
                "status": "skipped",
                "publish_skipped": True,
                "reason": "no_source_work",
            }],
        },
    }
    encoded = base64.b64encode(pickle.dumps(payload)).decode("ascii")

    (
        timings,
        workflow_timings,
        layers,
        results,
        publishes,
        query_count,
    ) = _workflow_ingest_report_fields(encoded, serialization="py_pickle")

    assert query_count == 451

    assert timings == {
        "total": pytest.approx(300.25),
        "source_wall": pytest.approx(120),
        "source_reported_total": pytest.approx(180),
    }
    assert workflow_timings == {
        "pre_ingest_setup": pytest.approx(42.5),
        "run_ingest": pytest.approx(300.25),
    }
    assert layers == [{
        "index": 1,
        "sources": ["git-files"],
        "source_wall_ms": 120,
    }]
    assert results == [{
        "name": "git-files",
        "run_id": "run-1",
        "status": "OK",
    }]
    assert publishes == [{
        "label": "after-layer-1",
        "status": "skipped",
        "publish_skipped": True,
        "reason": "no_source_work",
    }]


def test_publish_timing_from_progress_row_extracts_view_cost() -> None:
    payload = {
        "schema": "okg.publish.stage_timings.run.v1",
        "label": "after-layer-2",
        "status": "published",
        "generation_id": "okg:svc:gen:1",
        "timings_ms": {
            "total": 200,
            "native_views": 80,
            "view_derived_fact_apply": "30.5",
            "checkpoint_generation": 10,
        },
        "total_duration_ms": 210,
    }

    out = _publish_timing_from_progress_row(
        key="sync.publish_stage_timings.run.okg:svc:gen:1",
        value=json.dumps(payload),
        updated_at=datetime(2026, 6, 9, 13, 0, tzinfo=timezone.utc),
    )

    assert out is not None
    assert out["label"] == "after-layer-2"
    assert out["status"] == "published"
    assert out["generation_id"] == "okg:svc:gen:1"
    assert out["total_ms"] == pytest.approx(210.0)
    assert out["view_materialization_ms"] == pytest.approx(110.5)
    assert out["timings_ms"]["native_views"] == pytest.approx(80.0)
    assert out["timings_ms"]["checkpoint_generation"] == pytest.approx(10.0)


def test_aggregate_publish_timings_splits_final_intermediate_and_skipped() -> None:
    timings = [
        {
            "updated_ms": 100,
            "label": "after-layer-1",
            "status": "published",
            "generation_id": "gen-1",
            "publish_skipped": False,
            "total_ms": 40.0,
            "view_materialization_ms": 20.0,
            "timings_ms": {
                "native_views": 20.0,
                "checkpoint_generation": 10.0,
            },
        },
        {
            "updated_ms": 150,
            "label": "after-layer-2",
            "status": "skipped",
            "generation_id": None,
            "publish_skipped": True,
            "total_ms": None,
            "view_materialization_ms": 0.0,
            "timings_ms": {},
        },
        {
            "updated_ms": 200,
            "label": "after-layer-3",
            "status": "published",
            "generation_id": "gen-final",
            "publish_skipped": False,
            "total_ms": 60.0,
            "view_materialization_ms": 25.0,
            "timings_ms": {
                "native_views": 25.0,
                "checkpoint_generation": 15.0,
            },
        },
    ]

    out = _aggregate_publish_timings(
        timings,
        start_ms=0,
        end_ms=250,
        final_gen_id="gen-final",
    )

    assert out["publish_diag_status"] == "available"
    assert out["publish_event_count"] == 3
    assert out["publish_skipped_count"] == 1
    assert out["published_count"] == 2
    assert out["intermediate_publish_count"] == 1
    assert out["publish_total_ms"] == pytest.approx(100.0)
    assert out["final_publish_total_ms"] == pytest.approx(60.0)
    assert out["intermediate_publish_total_ms"] == pytest.approx(40.0)
    assert out["view_materialization_ms"] == pytest.approx(45.0)
    assert out["intermediate_view_materialization_ms"] == pytest.approx(20.0)
    assert "after-layer-2=1" in out["publish_labels"]
    assert out["publishes"][0]["timings_ms"]["native_views"] == pytest.approx(20.0)


def test_publish_run_summary_reports_stage_timings_and_ignores_counters() -> None:
    summary = _publish_run_summary([
        {
            "status": "published",
            "label": "after-layer-1",
            "publish_skipped": False,
            "total_ms": 100.0,
            "timings_ms": {
                "native_views": 40.0,
                "checkpoint_generation": 20.0,
                "payload_bytes": 1_000_000.0,
            },
        },
        {
            "status": "published",
            "label": "after-layer-1",
            "publish_skipped": False,
            "total_ms": 150.0,
            "timings_ms": {
                "native_views": 60.0,
                "checkpoint_generation": 30.0,
                "rows_written": 50_000.0,
            },
        },
        {
            "status": "skipped",
            "label": "after-layer-2",
            "publish_skipped": True,
            "total_ms": None,
            "timings_ms": {"native_views": 999.0},
        },
    ])

    assert summary["publish_event_count"] == 3
    assert summary["published_count"] == 2
    assert summary["publish_skipped_count"] == 1
    assert summary["labels"]["after-layer-1"]["sum_ms"] == pytest.approx(250.0)
    assert summary["stages"]["native_views"]["max_ms"] == pytest.approx(60.0)
    assert "payload_bytes" not in summary["stages"]
    assert "rows_written" not in summary["stages"]


def test_aggregate_publish_timings_handles_progress_checkpoint_lag() -> None:
    timings = [
        {
            "updated_ms": 110,
            "label": "after-layer-2",
            "status": "published",
            "generation_id": "gen-prev",
            "publish_skipped": False,
            "total_ms": 30.0,
            "view_materialization_ms": 10.0,
        },
        {
            "updated_ms": 150,
            "label": "after-layer-1",
            "status": "published",
            "generation_id": "gen-mid",
            "publish_skipped": False,
            "total_ms": 40.0,
            "view_materialization_ms": 20.0,
        },
        {
            "updated_ms": 260,
            "label": "after-layer-2",
            "status": "published",
            "generation_id": "gen-final",
            "publish_skipped": False,
            "total_ms": 60.0,
            "view_materialization_ms": 25.0,
        },
    ]

    out = _aggregate_publish_timings(
        timings,
        start_ms=100,
        end_ms=250,
        final_gen_id="gen-final",
        previous_gen_id="gen-prev",
    )

    assert out["publish_event_count"] == 2
    assert out["published_count"] == 2
    assert out["intermediate_publish_count"] == 1
    assert out["publish_total_ms"] == pytest.approx(100.0)
    assert out["final_publish_total_ms"] == pytest.approx(60.0)
    assert out["intermediate_publish_total_ms"] == pytest.approx(40.0)
    assert [item["generation_id"] for item in out["publishes"]] == [
        "gen-mid",
        "gen-final",
    ]


def test_source_run_summary_reports_percentiles() -> None:
    runs = [
        {
            "registry_source_name": "git-files",
            "total_ms": 10.0,
            "timings_ms": {"source_runner.adapter_run": 5.0},
        },
        {
            "registry_source_name": "git-files",
            "total_ms": 20.0,
            "timings_ms": {"source_runner.adapter_run": 15.0},
        },
        {
            "registry_source_name": "git-history",
            "total_ms": 100.0,
            "timings_ms": {"source_runner.stream_walk": 60.0},
        },
    ]

    summary = _source_run_summary(runs)

    assert summary["sources"]["git-files"]["n"] == 2
    assert summary["sources"]["git-files"]["p50_ms"] == pytest.approx(10.0)
    assert summary["sources"]["git-files"]["p95_ms"] == pytest.approx(10.0)
    assert summary["sources"]["git-history"]["max_ms"] == pytest.approx(100.0)
    assert (
        summary["phases"]["source_runner.adapter_run"]["max_ms"]
        == pytest.approx(15.0)
    )


def test_diagnostic_measurements_are_profile_measurement_compatible() -> None:
    summary = {
        "sources": {
            "git-history": {
                "n": 3,
                "p50_ms": 10.0,
                "p95_ms": 20.0,
                "max_ms": 30.0,
            },
        },
        "phases": {
            "source_runner.adapter_run": {
                "n": 3,
                "p50_ms": 4.0,
                "p95_ms": 8.0,
                "max_ms": 12.0,
            },
        },
    }

    measurements = _diagnostic_measurements(
        exp_id="palimpzest-tier1",
        summary=summary,
        publish_summary={
            "labels": {
                "after-layer-1": {
                    "n": 2,
                    "p50_ms": 80.0,
                    "p95_ms": 120.0,
                    "max_ms": 140.0,
                    "sum_ms": 220.0,
                },
            },
            "stages": {
                "native_views": {
                    "n": 2,
                    "p50_ms": 20.0,
                    "p95_ms": 40.0,
                    "max_ms": 45.0,
                    "sum_ms": 65.0,
                },
            },
        },
    )

    by_metric = {
        (row["scenario"], row["metric"]): row
        for row in measurements
    }
    source_p95 = by_metric[
        ("source_sync.git-history", "source_total_p95_ms")
    ]
    phase_p95 = by_metric[
        ("source_sync.phase.source_runner.adapter_run", "phase_p95_ms")
    ]
    publish_stage_p95 = by_metric[
        ("source_sync.publish.stage.native_views", "publish_stage_p95_ms")
    ]
    assert source_p95["source_name"] == "git-history"
    assert source_p95["percentile"] == "p95"
    assert phase_p95["stage_name"] == "source_runner.adapter_run"
    assert phase_p95["status"] == "informational"
    assert publish_stage_p95["stage_name"] == "native_views"
    assert publish_stage_p95["value"] == pytest.approx(40.0)


def test_select_diagnostic_source_run_picks_slowest_matching_source() -> None:
    diagnostics = {
        "per_commit": [
            _diagnostic_commit(
                i=1,
                commit="aaa111",
                source="git-history",
                run_id="fast",
                total_ms=10.0,
            ),
            _diagnostic_commit(
                i=2,
                commit="bbb222",
                source="git-history",
                run_id="slow",
                total_ms=200.0,
            ),
            _diagnostic_commit(
                i=3,
                commit="ccc333",
                source="git-files",
                run_id="files",
                total_ms=999.0,
            ),
        ],
    }

    commit_row, run = _select_diagnostic_source_run(
        diagnostics,
        source="git-history",
        commit=None,
        run_id=None,
    )

    assert commit_row["commit"] == "bbb222"
    assert run["run_id"] == "slow"


def test_source_run_profile_bundle_projects_through_profile_measurements(
    tmp_path,
) -> None:
    exp = Experiment(
        id="palimpzest-tier1",
        repo=Repo(name="palimpzest", url="https://example.test/repo.git"),
        source_set="tier1",
        replay=Replay(strategy="first_n", n=1),
    )
    commit_row = _diagnostic_commit(
        i=7,
        commit="abc123",
        source="git-history",
        run_id="run-history",
        total_ms=250.0,
    )
    run = commit_row["source_runs"][0]

    profile = _build_profile_run_from_source_run(exp, commit_row, run)

    assert profile.target_kind == "source_sync_run"
    assert profile.source_sync["commit"] == "abc123"
    assert {stage.name for stage in profile.stages} >= {
        "source_runner",
        "source_runner.adapter_run",
    }
    assert profile.evidence["python_flamegraph"].status == "unavailable"

    bundle = _write_profile_drilldown(
        exp,
        commit_row,
        run,
        profile_root=tmp_path / "profiles",
    )
    dry_run = _profile_measurements_dry_run(exp, bundle / "profile.json")

    assert (bundle / "profile.json").is_file()
    assert dry_run["ok"] is True
    assert dry_run["node_counts"]["profile_run"] == 1
    assert dry_run["node_counts"]["profile_stage"] >= 2


def _diagnostic_commit(
    *,
    i: int,
    commit: str,
    source: str,
    run_id: str,
    total_ms: float,
) -> dict:
    return {
        "i": i,
        "commit": commit,
        "gen_id": f"okg:test:gen:{i}",
        "workflow_uuid": f"wf-{i}",
        "window_start_ms": i * 1000,
        "window_end_ms": i * 1000 + 500,
        "source_total_ms": total_ms,
        "top_sources": f"{source}={total_ms:.3f}",
        "top_phases": (
            "source_runner=200.000;"
            "source_runner.adapter_run=150.000"
        ),
        "source_runs": [{
            "source_name": f"palimpzest-tier1.{source}",
            "registry_source_name": source,
            "run_id": run_id,
            "status": "OK",
            "run_mode": "cursor",
            "timing_status": "available",
            "updated_at": "2026-06-09T13:00:00+00:00",
            "input_count": 1,
            "output_count": 3,
            "nodes_appended": 2,
            "edges_appended": 1,
            "nodes_retracted": 0,
            "edges_retracted": 0,
            "total_ms": total_ms,
            "source_runner_ms": 200.0,
            "slowest_phase": "source_runner.adapter_run",
            "slowest_phase_ms": 150.0,
            "timings_ms": {
                "total": total_ms,
                "source_runner": 200.0,
                "source_runner.adapter_run": 150.0,
                "source_runner.flush_final": 30.0,
            },
            "source_stats": {"changed_paths": 1},
        }],
    }


def test_stage_bottleneck_skips_rollups_and_backend_detail() -> None:
    """Issue #1325 item 4: rank terminal phases, not wrappers.

    `source_runner`, `stream_walk`, `flush_chunks`, `flush_final` and `flush`
    all wrap finer keys, so each is >= what it contains and would win every
    comparison. Depth-4 keys are Chronos backend sub-measurements, which are not
    a partition of their phase.
    """
    phase, ms = _stage_bottleneck({
        "total": 21004.0,
        "source_runner": 20950.0,
        "source_runner.total": 20950.0,
        "source_runner.stream_walk": 15300.0,
        "source_runner.flush_chunks": 15100.0,
        "source_runner.flush_final": 800.0,
        "source_runner.flush": 14820.0,
        "source_runner.flush.chronos_apply": 14500.0,
        "source_runner.flush.chronos_apply.node_copy": 20000.0,
        "source_runner.adapter_run": 5200.0,
    })

    assert phase == "source_runner.flush.chronos_apply"
    assert ms == pytest.approx(14500.0)


def test_stage_bottleneck_reports_nothing_without_a_breakdown() -> None:
    assert _stage_bottleneck({
        "total": 300.0,
        "source_runner": 280.0,
        "source_runner.total": 280.0,
    }) == (None, None)
