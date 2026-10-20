"""Unit tests for the publish-cycle phase rollup (bench-okg-internals
task.internal-metrics). The live-run column population + the
phases-sum-within-tolerance check are proven by a real tinydb run; these lock
the pure mapping logic: which timings_ms keys roll into which reported phase,
and that an absent phase is NaN (None), not silently 0."""
from __future__ import annotations

import okg.bench.extract as extract
import okg.bench.orchestrator as orchestrator


def _payload(timings, total=None):
    p = {"timings_ms": timings}
    if total is not None:
        p["total_duration_ms"] = total
    return p


def test_rollup_maps_each_phase_to_its_components():
    timings = {
        "extractor": 1.0, "derived_fact_load": 2.0, "derived_fact_apply": 3.0,
        "enrichers": 10.0,
        "gates": 4.0, "consistency_gates": 6.0,
        "checkpoint_generation": 5.0, "notify": 1.0,
        "native_views": 100.0, "lenses": 20.0, "view_derived_fact_load": 5.0,
        "view_cite_retractions": 2.0, "view_derived_fact_apply": 3.0,
        "total": 300.0,
    }
    r = extract._rollup_publish_phases(_payload(timings))
    assert r["projection_ms"] == 6.0          # 1+2+3
    assert r["enrichment_ms"] == 10.0
    assert r["gate_ms"] == 10.0               # 4+6
    assert r["publish_ms"] == 6.0             # 5+1
    assert r["view_materialization_ms"] == 130.0  # 100+20+5+2+3
    assert r["publish_total_ms"] == 300.0


def test_maintained_derivation_ms_maps_to_console_count_rollup():
    # console-count-rollup (#444 slice 2): the maintained-derivation publish-stage
    # cost rolls up the flat `console_count_rollup` timings_ms key.
    r = extract._rollup_publish_phases(_payload(
        {"console_count_rollup": 7.5, "total": 50.0}))
    assert r["maintained_derivation_ms"] == 7.5


def test_maintained_derivation_ms_absent_is_none_not_zero():
    # the rollup didn't run this cycle (flag off) => NaN (None), not 0
    r = extract._rollup_publish_phases(_payload(
        {"extractor": 1.0, "total": 50.0}))
    assert r["maintained_derivation_ms"] is None


def test_absent_phase_is_none_not_zero():
    # no enrichers ran => enrichment is NaN (None), not 0
    r = extract._rollup_publish_phases(_payload(
        {"extractor": 1.0, "gates": 2.0, "total": 50.0}))
    assert r["enrichment_ms"] is None
    assert r["view_materialization_ms"] is None
    assert r["projection_ms"] == 1.0
    assert r["gate_ms"] == 2.0


def test_empty_or_none_payload_all_none():
    for p in (None, {}, {"timings_ms": {}}):
        r = extract._rollup_publish_phases(p)
        assert all(v is None for v in r.values())


def test_total_falls_back_to_total_duration_ms():
    r = extract._rollup_publish_phases(_payload({"extractor": 1.0}, total=42.0))
    assert r["publish_total_ms"] == 42.0


def test_aggregate_source_runs_flags_missing_timing_not_silent_zero():
    # A selected run with NO timing must NOT be summed as 0 under an
    # "available" label (the P0 silent-fallback). It is excluded from the sum
    # and the window is flagged partial-coverage with a named reason.
    runs = [
        {"updated_ms": 100, "total_ms": 50.0, "source_name": "a",
         "timings_ms": {}},
        {"updated_ms": 150, "total_ms": None, "source_runner_ms": None,
         "source_name": "b", "timings_ms": {}},  # missing timing
    ]
    agg = extract._aggregate_source_runs(runs, start_ms=0, end_ms=200)
    assert agg["source_diag_status"] == "partial"
    assert "missing_timing" in agg["source_diag_reason"]
    assert agg["source_total_ms"] == 50.0  # the timing-less run is excluded


def test_aggregate_source_runs_available_when_all_timed():
    runs = [
        {"updated_ms": 100, "total_ms": 50.0, "source_name": "a",
         "timings_ms": {}},
        {"updated_ms": 150, "source_runner_ms": 30.0, "source_name": "b",
         "timings_ms": {}},
    ]
    agg = extract._aggregate_source_runs(runs, start_ms=0, end_ms=200)
    assert agg["source_diag_status"] == "available"
    assert agg["source_total_ms"] == 80.0


def test_source_run_bottleneck_ignores_non_duration_gauges():
    phase, ms = extract._stage_bottleneck({
        "source_runner.adapter_run": 12.0,
        "source_runner.flush.marker_batch_size": 100.0,
        "source_runner.flush.graph_apply_fact_count_max": 5000.0,
        "source_runner.flush.count": 7.0,
    })

    assert phase == "source_runner.adapter_run"
    assert ms == 12.0


def test_source_run_summary_excludes_non_duration_gauges():
    summary = extract._source_run_summary([
        {
            "source_name": "demo",
            "total_ms": 20.0,
            "timings_ms": {
                "source_runner.adapter_run": 12.0,
                "source_runner.flush.graph_apply_fact_count": 5000.0,
                "source_runner.flush.marker_batch_size": 100.0,
                "source_runner.flush.chronos_apply.node_change_fields.name": 42.0,
                "source_runner.flush.chronos_apply.node_change_class.total": 99.0,
            },
        },
    ])

    assert "source_runner.adapter_run" in summary["phases"]
    assert "source_runner.flush.graph_apply_fact_count" not in summary["phases"]
    assert "source_runner.flush.marker_batch_size" not in summary["phases"]
    assert (
        "source_runner.flush.chronos_apply.node_change_fields.name"
        not in summary["phases"]
    )
    assert (
        "source_runner.flush.chronos_apply.node_change_class.total"
        not in summary["phases"]
    )


def test_semantic_graph_rows_strip_lsp_run_local_edge_metadata():
    rows = [
        {
            "src": "symbol:demo:caller",
            "dst": "symbol:demo:callee",
            "edge_type": "calls",
            "attrs": {
                "semantic_target_key": "demo:callee",
                "source_frontier": {"git": "abc"},
                "workspace_fingerprint": "workspace-a",
            },
        },
        {
            "src": "symbol:demo:caller",
            "dst": "symbol:demo:callee",
            "edge_type": "calls",
            "attrs": {
                "semantic_target_key": "demo:callee",
                "source_frontier": {"git": "def"},
                "workspace_fingerprint": "workspace-b",
            },
        },
    ]

    normalized = orchestrator._semantic_graph_edge_rows(rows)

    assert normalized == [
        {
            "src": "symbol:demo:caller",
            "dst": "symbol:demo:callee",
            "edge_type": "calls",
            "attrs": {"semantic_target_key": "demo:callee"},
        },
        {
            "src": "symbol:demo:caller",
            "dst": "symbol:demo:callee",
            "edge_type": "calls",
            "attrs": {"semantic_target_key": "demo:callee"},
        },
    ]


def test_reported_phases_never_exceed_total():
    # invariant the live verification relies on: the named phases are a SUBSET
    timings = {
        "extractor": 1.0, "enrichers": 2.0, "gates": 1.0,
        "checkpoint_generation": 1.0, "native_views": 5.0,
        "store_setup": 3.0, "branch_checkout": 2.0,  # unreported phases
        "total": 15.0,
    }
    r = extract._rollup_publish_phases(_payload(timings))
    named = sum(v for k, v in r.items()
                if k != "publish_total_ms" and v is not None)
    assert named <= r["publish_total_ms"]
