"""Source-sync measurement contracts bound to formal observability."""
from __future__ import annotations

from okg.substrate.source_sync_benchmark import (
    build_source_sync_benchmark_artifact,
    validate_source_sync_benchmark,
)


def test_amplification_uses_measured_terms() -> None:
    debug_doc = _debug_workflow_payload()
    source = debug_doc["source_sync"]["stage_timing"]["sources"][0]
    source["source_stats"] = {"records": 9}

    artifact = build_source_sync_benchmark_artifact(
        debug_doc,
        deployment="okg-workspace",
        repo="okg",
        scenario="source_sync.pact_debug",
        expected_delta={"nodes_added": 2, "edges_added": 1},
        observed_delta={"nodes_added": 2, "edges_added": 1},
        query_visible=True,
        query_visibility_latency_ms=42,
        queue={"queue_depth": 0, "dlq_count": 0},
    )
    report = validate_source_sync_benchmark(
        artifact,
        artifact="reports/source-sync-capture.json",
        deployment="okg-workspace",
        known_sources=("pact",),
    )
    metrics = {
        row["metric"]: row
        for row in report["measurements"]
    }

    assert report["ok"] is True
    assert artifact["capture"]["expected_delta_source"] == "explicit"
    assert artifact["work"]["source_input_count"] == 9.0
    assert artifact["work"]["source_output_count"] == 3.0
    assert artifact["work"]["observed_delta_units"] == 3.0
    assert artifact["work"]["work_amplification_ratio"] == 1.0
    assert metrics["work_amplification_ratio"]["value"] == (
        artifact["work"]["source_output_count"]
        / artifact["work"]["observed_delta_units"]
    )
    assert metrics["work_amplification_ratio"]["status"] == "passed"
    assert metrics["expected_delta_consistency"]["status"] == "passed"


def test_measurement_gaps_surface_as_failed_observability() -> None:
    missing_expected = _valid_benchmark_artifact()
    missing_expected["expected_delta"] = {}
    missing_report = validate_source_sync_benchmark(
        missing_expected,
        artifact="reports/source-sync-capture.json",
        deployment="okg-workspace",
        known_sources=("pact",),
    )

    assert missing_report["ok"] is False
    assert "missing_expected_delta" in {
        error["code"] for error in missing_report["errors"]
    }
    assert _metric(missing_report, "expected_delta_consistency")["status"] == (
        "failed"
    )

    self_confirming = _valid_benchmark_artifact()
    self_confirming["capture"]["expected_delta_source"] = (
        "observed_delta_default"
    )
    self_confirming_report = validate_source_sync_benchmark(
        self_confirming,
        artifact="reports/source-sync-capture.json",
        deployment="okg-workspace",
        known_sources=("pact",),
    )

    assert self_confirming_report["ok"] is False
    assert "self_confirming_expected_delta" in {
        error["code"] for error in self_confirming_report["errors"]
    }
    assert _metric(self_confirming_report, "expected_delta_consistency")[
        "status"
    ] == "failed"


def test_harness_declared_expected_delta_is_valid_observability_input() -> None:
    harness_declared = _valid_benchmark_artifact()
    harness_declared["capture"]["expected_delta_source"] = "harness_declared"

    report = validate_source_sync_benchmark(
        harness_declared,
        artifact="reports/source-sync-capture.json",
        deployment="okg-workspace",
        known_sources=("pact",),
    )

    assert report["ok"] is True
    assert _metric(report, "expected_delta_consistency")["status"] == "passed"


def test_unknown_expected_delta_source_fails_validation() -> None:
    invalid_source = _valid_benchmark_artifact()
    invalid_source["capture"]["expected_delta_source"] = "posthoc_guess"

    report = validate_source_sync_benchmark(
        invalid_source,
        artifact="reports/source-sync-capture.json",
        deployment="okg-workspace",
        known_sources=("pact",),
    )

    assert report["ok"] is False
    assert "invalid_expected_delta_source" in {
        error["code"] for error in report["errors"]
    }


def _metric(report: dict[str, object], metric: str) -> dict[str, object]:
    return next(
        row for row in report["measurements"]
        if row["metric"] == metric
    )


def _valid_benchmark_artifact() -> dict[str, object]:
    return build_source_sync_benchmark_artifact(
        _debug_workflow_payload(),
        deployment="okg-workspace",
        repo="okg",
        scenario="source_sync.pact_debug",
        expected_delta={"nodes_added": 2, "edges_added": 1},
        observed_delta={"nodes_added": 2, "edges_added": 1},
        query_visible=True,
        query_visibility_latency_ms=42,
        queue={"queue_depth": 0, "dlq_count": 0},
    )


def _debug_workflow_payload() -> dict[str, object]:
    return {
        "workflow_id": "wf-source-sync-1",
        "name": "okg_source_sync",
        "status": "SUCCESS",
        "created_at": "2026-06-03T14:00:00.500000+00:00",
        "completed_at": "2026-06-03T14:00:06+00:00",
        "lifetime_ms": 6000,
        "profile_path": "profiles/okg-workspace/source-sync-1/profile.json",
        "dbos": {
            "deduplication_id": "dedupe-1",
            "application_version": "app-v1",
        },
        "source_sync": {
            "decode_status": "decoded",
            "deployment": "okg-workspace",
            "sources": ["pact"],
            "run_mode": "scope_complete",
            "event_count": 1,
            "event_sample": [{
                "event_id": "event-1",
                "source": "pact",
                "trigger_kind": "fs_event",
                "observed_at": "2026-06-03T14:00:00+00:00",
            }],
            "queue_dwell_ms": 250,
            "dbos": {
                "queue_name": "okg-okg-workspace-source-sync",
            },
            "stage_timing": {
                "status": "available",
                "source_count": 1,
                "timed_source_count": 1,
                "sources": [{
                    "name": "pact",
                    "status": "OK",
                    "run_mode": "scope_complete",
                    "timing_status": "available",
                    "timings_ms": {
                        "admission_prepare": 10,
                        "source_runner": 1000,
                        "admission_finalize": 20,
                        "total": 1030,
                    },
                    "nodes_appended": 2,
                    "edges_appended": 1,
                    "source_stats": {"records": 2},
                }],
                "publishes": [{
                    "label": "after-layer-1",
                    "status": "published",
                    "generation_id": (
                        "okg:okg-workspace:branch:default:gen:test"
                    ),
                    "total_duration_ms": 3000,
                    "nodes_added": 2,
                    "edges_added": 1,
                    "enrichers": [{
                        "source": "_enricher:pact",
                        "status": "accepted",
                        "record_count": 5,
                    }],
                }],
            },
        },
    }
