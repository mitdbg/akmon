from __future__ import annotations

import json
from pathlib import Path


BENCH_DIR = Path(__file__).resolve().parents[3] / "bench"
ROOT = BENCH_DIR.parent
import okg.bench.orchestrator as orchestrator  # noqa: E402


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_psql_helper_uses_psycopg_autocommit_and_splits_statements(monkeypatch):
    executed: list[str] = []
    connect_kwargs: dict = {}

    class Cursor:
        description = None

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def execute(self, statement: str) -> None:
            executed.append(statement)
            self.description = [("datname",)] if statement.startswith("SELECT") else None

        def fetchall(self):
            return [("okg_bench_demo",)]

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def cursor(self):
            return Cursor()

    def connect(dsn: str, **kwargs):
        connect_kwargs.update({"dsn": dsn, **kwargs})
        return Connection()

    monkeypatch.setattr(orchestrator.psycopg, "connect", connect)

    out = orchestrator._psql("SELECT datname FROM pg_database; CREATE DATABASE x;", dsn="postgres://example")

    assert connect_kwargs == {"dsn": "postgres://example", "autocommit": True}
    assert executed == ["SELECT datname FROM pg_database", "CREATE DATABASE x"]
    assert out == "okg_bench_demo"


def test_static_report_renders_ci_benchmark_artifacts(tmp_path: Path):
    _write_json(
        tmp_path / "count_gate.json",
        {
            "ok": True,
            "experiment_id": "tinydb-code-lsp-head10",
            "replay_strategy": "head_only",
            "blocking_violation_count": 0,
            "violations": [],
            "budgets": {"max_queries_per_changed_file": 745.0},
            "metrics": {
                "measured_commit_count": 10,
                "query_counts": {
                    "queries_per_changed_file": {"max_ms": 596.0},
                },
            },
        },
    )
    _write_json(
        tmp_path / "sweep_summary.json",
        {
            "n_cells": 5,
            "pin": {"reproducible": True},
            "metric_coverage_all_present": True,
            "branching_ok": True,
            "cells": ["tdb-enr-en0"],
            "measurements": [{"cell": "tdb-enr-en0", "metric_name": "e2e_ms_mean"}],
            "comparison_groups": {
                "tdb-enr": {"ok": True, "swept_axis": "enrichers"},
            },
            "coverage": {
                "tdb-enr-en0": {
                    "csv": True,
                    "all_metrics_present": True,
                    "read_side": True,
                },
            },
        },
    )
    _write_json(
        tmp_path / "tdb-enr.comparison.json",
        {
            "ok": True,
            "swept_axis": "enrichers",
            "cells": [
                {
                    "cell_id": "tdb-enr-en0",
                    "axis_value": "none",
                    "is_baseline": True,
                    "profile": {
                        "e2e_ms_mean": 99.0,
                        "final_edges_live": 700,
                    },
                },
            ],
        },
    )
    _write_json(
        tmp_path / "engsens.sensitivity_summary.json",
        {
            "spec": {"id": "engsens"},
            "baseline_cell": "engsens-base",
            "n_cells": 1,
            "measurements": [
                {"cell": "engsens-base", "metric_name": "e2e_ms_mean", "value": 10.0},
            ],
            "cells": [
                {
                    "cell": "engsens-base",
                    "axis": "baseline",
                    "axis_value": "baseline",
                    "profile": {
                        "commits": 10,
                        "e2e_ms_mean": 123.0,
                        "e2e_ms_p95": 234.0,
                        "final_nodes_live": 500,
                        "final_edges_live": 700,
                        "read_side_p50": {"aggregate_by_type": 1.5},
                    },
                },
            ],
        },
    )
    _write_json(
        tmp_path / "tinydb-lsp-enr-10.rebuild_equivalence.json",
        {
            "ok": True,
            "experiment_id": "tinydb-lsp-enr-10",
            "checks": {"node_digest_equal": True, "edge_digest_equal": True},
            "incremental": {
                "graph": {
                    "nodes": {"count": 12},
                    "edges": {"count": 34},
                },
            },
        },
    )
    _write_json(
        tmp_path / "tinydb-code-lsp-head10.diagnostics.json",
        {
            "schema": "okg.bench.source_sync_diagnostics.v1",
            "experiment_id": "tinydb-code-lsp-head10",
            "per_commit": [
                {
                    "i": 1,
                    "commit": "abc1234",
                    "query_count": 24,
                    "source_total_ms": 125.0,
                    "publish_total_ms": 75.0,
                    "slowest_phase": "source_runner.flush",
                    "slowest_phase_ms": 80.0,
                    "source_runs": [
                        {
                            "registry_source_name": "git-files",
                            "total_ms": 125.0,
                            "timings_ms": {
                                "total": 125.0,
                                "source_runner": 110.0,
                                "source_runner.flush": 80.0,
                                "source_runner.adapter_run": 20.0,
                            },
                        },
                    ],
                    "publishes": [
                        {
                            "label": "commit=1",
                            "total_ms": 75.0,
                            "timings_ms": {
                                "total": 75.0,
                                "consistency_gates": 40.0,
                                "checkpoint_generation": 20.0,
                            },
                        },
                    ],
                },
            ],
        },
    )
    _write_json(
        tmp_path / "tdb-enr-en0.diagnostics.json",
        {
            "schema": "okg.bench.source_sync_diagnostics.v1",
            "experiment_id": "tdb-enr-en0",
            "per_commit": [
                {
                    "i": 1,
                    "commit": "def5678",
                    "query_count": 18,
                    "source_total_ms": 90.0,
                    "publish_total_ms": 40.0,
                    "slowest_phase": "source_runner.adapter_run",
                    "slowest_phase_ms": 50.0,
                    "source_runs": [
                        {
                            "registry_source_name": "git-files",
                            "total_ms": 90.0,
                            "timings_ms": {
                                "total": 90.0,
                                "source_runner": 75.0,
                                "source_runner.adapter_run": 50.0,
                                "source_runner.flush": 20.0,
                            },
                        },
                    ],
                    "publishes": [
                        {
                            "label": "commit=1",
                            "total_ms": 40.0,
                            "timings_ms": {
                                "total": 40.0,
                                "consistency_gates": 22.0,
                                "checkpoint_generation": 8.0,
                            },
                        },
                    ],
                },
            ],
        },
    )
    _write_json(
        tmp_path / "engsens-base.diagnostics.json",
        {
            "schema": "okg.bench.source_sync_diagnostics.v1",
            "experiment_id": "engsens-base",
            "per_commit": [
                {
                    "i": 1,
                    "commit": "fed9876",
                    "query_count": 12,
                    "source_total_ms": 40.0,
                    "publish_total_ms": 20.0,
                    "slowest_phase": "source_runner.adapter_run",
                    "slowest_phase_ms": 30.0,
                    "source_runs": [
                        {
                            "registry_source_name": "git-files",
                            "total_ms": 40.0,
                            "timings_ms": {
                                "total": 40.0,
                                "source_runner": 35.0,
                                "source_runner.adapter_run": 30.0,
                            },
                        },
                    ],
                    "publishes": [
                        {
                            "label": "commit=1",
                            "total_ms": 20.0,
                            "timings_ms": {
                                "total": 20.0,
                                "checkpoint_generation": 8.0,
                            },
                        },
                    ],
                },
            ],
        },
    )

    report_path, summary = orchestrator.write_static_bench_report(results_dir=tmp_path)
    html = report_path.read_text(encoding="utf-8")

    assert summary["ok"] is True
    assert summary["missing"] == []
    assert "Run summary" in html
    assert "Measured commits" in html
    assert "tinydb-code-lsp-head10" in html
    assert "Incremental truth" in html
    assert "Tinydb internals sweep" in html
    assert "Differential engine sensitivity" in html
    assert "Delta E2E" in html
    assert "engsens-base" in html
    assert "count_gate.json" in html
    assert "Artifacts" in html
    assert "Readable summaries" in html
    assert "Timing profile" in html
    assert "The flamegraph viewer over the benchmark timing stats" in html
    assert "AggregateProfileFlamegraph" in html
    assert "timing-profile.aggregate_flamegraph.json" in html
    assert "flamegraph-viewer/bench-flamegraph.html" in html
    assert "Timing flamegraphs" in html
    assert "flamegraph-viewer/tdb-enr-en0.html" in html
    assert "tdb-enr-en0.aggregate_flamegraph.json" in html
    assert "Sweep matrix" in html
    assert "Open tdb-enr-en0 flamegraph" in html
    assert "Sensitivity matrix" in html
    assert "Open engsens-base flamegraph" in html
    assert "Incremental vs rebuild checks" in html
    assert "correctness guard, not a timing case" in html
    assert "Open tinydb-code-lsp-head10 flamegraph" not in html
    assert "summary-strip" in html
    assert "Sweep details" in html
    assert "flamegraph-row" not in html
    assert "flamegraph-track" not in html
    assert summary["states"]["timing_profile"]["available"] is True
    assert summary["states"]["timing_profile"]["payload"].endswith(
        "timing-profile.aggregate_flamegraph.json"
    )
    assert summary["states"]["timing_flamegraphs"]["navigation_count"] == 4
    payload_path = tmp_path / "timing-profile.aggregate_flamegraph.json"
    viewer_path = tmp_path / "flamegraph-viewer" / "bench-flamegraph.html"
    sweep_payload_path = tmp_path / "tdb-enr-en0.aggregate_flamegraph.json"
    sweep_viewer_path = tmp_path / "flamegraph-viewer" / "tdb-enr-en0.html"
    manifest_path = tmp_path / "flamegraph-viewer" / "manifest.json"
    assert payload_path.exists()
    assert viewer_path.exists()
    assert sweep_payload_path.exists()
    assert sweep_viewer_path.exists()
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert [entry["id"] for entry in manifest] == [
        "timing-profile",
        "engsens-base",
        "tdb-enr-en0",
        "tinydb-code-lsp-head10",
    ]
    assert manifest[0]["viewer"] == "bench-flamegraph.html"
    assert manifest[0]["report"] == "../index.html#timing-profile"
    assert manifest[1]["viewer"] == "engsens-base.html"
    assert manifest[1]["report"] == "../index.html#sensitivity"
    assert manifest[2]["viewer"] == "tdb-enr-en0.html"
    assert manifest[2]["report"] == "../index.html#sweep"
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    assert payload["schema"] == "okg.telemetry.aggregate_flamegraph.v1"
    assert payload["source"] == "spans"
    assert payload["profile_type"] == "evented"
    assert payload["status"] == "available"
    assert payload["root"]["children"]
    payload_text = json.dumps(payload)
    assert "source.git-files.source_runner.flush" in payload_text
    assert "publisher.commit=1.consistency_gates" in payload_text
    viewer_html = viewer_path.read_text(encoding="utf-8")
    assert 'id="okg-flamegraph-payload"' in viewer_html
    assert '<link rel="icon" href="data:,">' in viewer_html
    assert 'id="okg-flamegraph-manifest"' in viewer_html
    assert "../index.html#timing-profile" in viewer_html
    assert '"viewer": "bench-flamegraph.html"' in viewer_html
    assert '"viewer": "tdb-enr-en0.html"' in viewer_html
    assert "bench-flamegraph.js" in viewer_html
    assert "bench-flamegraph.js?v=" in viewer_html
    assert "bench-flamegraph.css?v=" in viewer_html
    assert "okg.telemetry.aggregate_flamegraph.v1" in viewer_html
    sweep_viewer_html = sweep_viewer_path.read_text(encoding="utf-8")
    assert 'id="okg-flamegraph-manifest"' in sweep_viewer_html
    assert "../index.html#sweep" in sweep_viewer_html


def test_static_report_preserves_missing_artifact_state(tmp_path: Path):
    report_path, summary = orchestrator.write_static_bench_report(results_dir=tmp_path)
    html = report_path.read_text(encoding="utf-8")

    assert summary["ok"] is False
    assert set(summary["missing"]) == {"count_gate", "sweep", "sensitivity"}
    assert "MISSING" in html
    assert "count_gate.json is missing" in html
    assert "sweep_summary.json is missing" in html
    assert "no *.sensitivity_summary.json artifact found" in html


def test_static_report_marks_empty_sensitivity_summary_not_ok(tmp_path: Path):
    _write_json(
        tmp_path / "count_gate.json",
        {
            "ok": True,
            "experiment_id": "tinydb-code-lsp-head10",
            "violations": [],
            "budgets": {"max_queries_per_changed_file": 745.0},
            "metrics": {"query_counts": {"queries_per_changed_file": {"max_ms": 596.0}}},
        },
    )
    _write_json(
        tmp_path / "sweep_summary.json",
        {
            "n_cells": 1,
            "pin": {"reproducible": True},
            "metric_coverage_all_present": True,
            "branching_ok": True,
            "measurements": [{"cell": "tdb-enr-en0", "metric_name": "e2e_ms_mean"}],
            "comparison_groups": {"tdb-enr": {"ok": True, "swept_axis": "enrichers"}},
            "coverage": {},
        },
    )
    _write_json(
        tmp_path / "engsens.sensitivity_summary.json",
        {
            "spec": {"id": "engsens"},
            "baseline_cell": "engsens-base",
            "n_cells": 0,
            "measurements": [],
            "cells": [],
        },
    )

    report_path, summary = orchestrator.write_static_bench_report(results_dir=tmp_path)
    html = report_path.read_text(encoding="utf-8")

    assert summary["ok"] is False
    assert summary["states"]["sensitivity"]["ok"] is False
    assert "empty or partial artifact" in html
