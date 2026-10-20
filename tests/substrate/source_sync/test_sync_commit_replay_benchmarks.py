from __future__ import annotations

import importlib
import json
from collections.abc import Iterator
from pathlib import Path

import pytest
import yaml

from tests.substrate._deployment_views import isolated_deployment_views


REPO = Path(__file__).resolve().parents[3]
DEPLOYMENT_DIR = REPO / "deployments" / "okg-workspace"
REGISTRY_PATH = DEPLOYMENT_DIR / "views_registry.yaml"
MODULE_PATH = DEPLOYMENT_DIR / "views" / "sync_commit_replay_benchmarks.py"


@pytest.fixture(autouse=True, scope="module")
def _deployment_on_sys_path() -> Iterator[None]:
    # A plain sys.path insert is a no-op when `import views` is already
    # cached to a foreign package (the cause of the CI-only
    # `No module named 'views.sync_commit_replay_benchmarks'` /
    # `module 'views' has no attribute 'VIEW_REGISTRY'` failures under
    # pytest-xdist, where one worker imports a scaffolded deployment's
    # `views` before this module runs). The helper hides cached `views`
    # modules, puts DEPLOYMENT_DIR first, and restores sys.path and
    # sys.modules on teardown so this module leaks nothing either.
    with isolated_deployment_views(DEPLOYMENT_DIR):
        yield


def test_registry_declares_sync_commit_replay_benchmark_view() -> None:
    data = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))
    entries = data.get("views") or []
    entry = next(
        (
            item for item in entries
            if item["view_def_id"]
            == "okg_workspace.sync_commit_replay_benchmarks"
        ),
        None,
    )
    assert entry is not None
    assert entry["view_def_factory"] == (
        "views.sync_commit_replay_benchmarks:"
        "sync_commit_replay_benchmarks_view_def"
    )
    assert entry["build_corpus"] == (
        "views.sync_commit_replay_benchmarks:build_corpus"
    )


def test_module_exports_required_symbols() -> None:
    assert MODULE_PATH.is_file()
    mod = importlib.import_module("views.sync_commit_replay_benchmarks")
    for name in (
        "REPLAY_ROWS_SQL",
        "REPLAY_ROWS_TEMP_TABLE",
        "VIEW_DEF_HASH",
        "VIEW_ID",
        "build_corpus",
        "build_replay_benchmark_rows",
        "sync_commit_replay_benchmarks_assembler",
        "sync_commit_replay_benchmarks_planner",
        "sync_commit_replay_benchmarks_view_def",
    ):
        assert hasattr(mod, name)
    assert mod.VIEW_ID == "okg_workspace.sync_commit_replay_benchmarks"
    assert mod.VIEW_DEF_HASH.startswith(
        "okg_workspace_sync_commit_replay_benchmarks:"
    )


def test_view_def_declares_relation_with_required_columns() -> None:
    from okg.views.planners.base import ViewDefinition

    mod = importlib.import_module("views.sync_commit_replay_benchmarks")
    view_def = mod.sync_commit_replay_benchmarks_view_def()
    assert isinstance(view_def, ViewDefinition)
    assert view_def.view_id == mod.VIEW_ID

    slot_names = {template.slot_name for template in view_def.output_templates}
    assert {"markdown", "json", "relation"} <= slot_names
    template = next(
        template for template in view_def.output_templates
        if template.slot_name == "relation"
    )
    column_names = {c["name"] for c in template.config["column_contracts"]}
    expected = {
        "experiment_id",
        "repo_name",
        "source_set",
        "replay_strategy",
        "commit_count",
        "p50_ms",
        "p95_ms",
        "max_ms",
        "throughput_commits_per_second",
        "outlier_count",
        "artifact_csv",
        "artifact_diagnostics",
        "latest_generation_id",
        "readiness_context_generation_id",
        "source_health_view_id",
        "source_health_command",
        "top_source_by_p95",
        "top_phase_by_p95",
    }
    assert expected <= column_names


def test_workspace_registry_exposes_replay_benchmark_factory() -> None:
    import views

    assert "okg_workspace.sync_commit_replay_benchmarks" in views.VIEW_REGISTRY


def test_replay_rows_summarize_csv_diagnostics_and_source_health_context(
    tmp_path: Path,
) -> None:
    mod = importlib.import_module("views.sync_commit_replay_benchmarks")
    manifest = tmp_path / "manifest.yaml"
    results = tmp_path / "results"
    results.mkdir()
    manifest.write_text(
        yaml.safe_dump({
            "experiments": [{
                "id": "demo-tier1-tail-10",
                "repo": {"name": "demo"},
                "source_set": "tier1",
                "replay": {"strategy": "tail_window", "n": 10},
            }]
        }),
        encoding="utf-8",
    )
    (results / "demo-tier1-tail-10.csv").write_text(
        "\n".join([
            "i,commit,e2e_ms,nodes_live,edges_live,gen_id,db_size_bytes",
            "8,aaa111,100,10,20,gen-8,1000",
            "9,bbb222,200,11,21,gen-9,1100",
            "10,ccc333,1300,12,22,gen-10,1200",
        ])
        + "\n",
        encoding="utf-8",
    )
    (results / "demo-tier1-tail-10.diagnostics.json").write_text(
        json.dumps({
            "schema": "okg.bench.source_sync_diagnostics.v1",
            "source_run_count": 4,
            "row_status_counts": {"available": 3},
            "measurements": [{"metric": "source_total_p95_ms"}],
            "source_run_summary": {
                "sources": {
                    "git-files": {"p95_ms": 500, "max_ms": 700, "n": 2},
                    "code-structure": {"p95_ms": 1500, "max_ms": 2000, "n": 2},
                },
                "phases": {
                    "source_runner": {"p95_ms": 1300, "n": 4},
                    "source_runner.flush_final": {"p95_ms": 900, "n": 4},
                },
            },
        }),
        encoding="utf-8",
    )

    rows = mod.build_replay_benchmark_rows(
        manifest_path=manifest,
        results_dir=results,
        readiness_context_generation_id=123,
        outlier_threshold_ms=1000,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["experiment_id"] == "demo-tier1-tail-10"
    assert row["repo_name"] == "demo"
    assert row["source_set"] == "tier1"
    assert row["replay_strategy"] == "tail_window"
    assert row["replay_n"] == 10
    assert row["commit_count"] == 3
    assert row["first_commit_index"] == 8
    assert row["last_commit_index"] == 10
    assert row["p50_ms"] == pytest.approx(200)
    assert row["p95_ms"] == pytest.approx(1300)
    assert row["max_ms"] == pytest.approx(1300)
    assert row["throughput_commits_per_second"] == pytest.approx(1.875)
    assert row["outlier_count"] == 1
    assert row["latest_commit"] == "ccc333"
    assert row["latest_generation_id"] == "gen-10"
    assert row["latest_nodes_live"] == 12
    assert row["latest_edges_live"] == 22
    assert row["source_run_count"] == 4
    assert row["measurement_count"] == 1
    assert row["row_status_counts"] == '{"available": 3}'
    assert row["top_source_by_p95"] == "code-structure"
    assert row["top_source_p95_ms"] == pytest.approx(1500)
    assert row["top_phase_by_p95"] == "source_runner"
    assert row["top_phase_p95_ms"] == pytest.approx(1300)
    assert row["diagnostics_status"] == "available"
    assert row["source_health_view_id"] == "okg_workspace.source_performance_health"
    assert "okg_workspace.source_performance_health" in row["source_health_command"]
    assert row["readiness_context_generation_id"] == 123
    assert row["status"] == "over_budget"
