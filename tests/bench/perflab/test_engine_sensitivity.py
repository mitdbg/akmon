from __future__ import annotations

import json
from pathlib import Path

import pytest


BENCH_DIR = Path(__file__).resolve().parents[3] / "bench"
ROOT = BENCH_DIR.parent
import okg.bench.engine_sensitivity as sensitivity  # noqa: E402
import okg.bench.extract as extract  # noqa: E402
import okg.bench.orchestrator as orchestrator  # noqa: E402


def _minimal_spec(**overrides):
    spec = {
        "schema": sensitivity.SCHEMA,
        "id": "sens",
        "mode": "one_factor_at_a_time",
        "baseline": {
            "repo": {"name": "tinydb", "url": "https://example/tinydb.git"},
            "source_set": "files+code",
            "replay": {"strategy": "first_n", "n": 10},
            "config": {
                "enrichers": "none",
                "views": False,
                "chronos_continuation_pct": 95,
                "embedding": False,
            },
        },
        "axes": {
            "workload": {"source_set": ["files-only", "files+code"]},
            "engine": {"views": [False, True]},
        },
    }
    spec.update(overrides)
    return spec


def test_shipped_sensitivity_config_expands_one_factor_matrix():
    plan = sensitivity.build_sensitivity_plan(BENCH_DIR / "engine_sensitivity.yaml")
    ids = {cell["id"] for cell in plan["cells"]}

    assert plan["spec"]["mode"] == "one_factor_at_a_time"
    assert plan["baseline_cell"] == "engsens-base"
    assert plan["cell_count"] == 9
    assert {
        "engsens-base",
        "engsens-src-fo",
        "engsens-src-t1",
        "engsens-r-fn5",
        "engsens-r-fn25",
        "engsens-endecl",
        "engsens-enall",
        "engsens-vw0",
        "engsens-chr50",
    } <= ids
    assert "branching" in plan["reports"]
    assert "--run-branching-report" in plan["reports"]["branching"]["command"]


def test_orchestrator_parses_sensitivity_cells_as_experiments():
    plan, experiments = orchestrator.build_engine_sensitivity_plan(
        BENCH_DIR / "engine_sensitivity.yaml",
    )

    assert len(experiments) == plan["cell_count"]
    assert {exp.id for exp in experiments} == {cell["id"] for cell in plan["cells"]}
    assert all(isinstance(exp, orchestrator.Experiment) for exp in experiments)


def test_orchestrator_writes_sensitivity_regression_summary(tmp_path, monkeypatch):
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", tmp_path)
    spec = _minimal_spec()
    plan = sensitivity.build_sensitivity_plan_from_spec(spec)
    experiments = [
        orchestrator._parse_experiment(cell["experiment"])
        for cell in plan["cells"][:2]
    ]
    for i, exp in enumerate(experiments, start=1):
        (tmp_path / f"{exp.id}.csv").write_text(
            "\n".join(
                [
                    "i,e2e_ms,source_total_ms,source_run_count,projection_ms," +
                    "enrichment_ms,gate_ms,publish_ms,view_materialization_ms," +
                    "publish_total_ms,edges_added,nodes_added,edges_live," +
                    "nodes_live,db_size_bytes,peak_rss_bytes,chronos_segments",
                    f"1,{100 * i},{10 * i},2,3,,4,5,6,15,7,8,70,80,900,1000,1",
                    f"2,{200 * i},{20 * i},2,4,,5,6,7,18,9,10,90,100,950,1100,1",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (tmp_path / f"{exp.id}.read_side.json").write_text(
            json.dumps(
                {
                    "ok": True,
                    "reads": {
                        "aggregate_by_type": {
                            "status": "ok",
                            "p50_ms": 1.5 * i,
                        },
                    },
                }
            ),
            encoding="utf-8",
        )

    summary_json, summary_md = orchestrator._write_sensitivity_summary(
        plan,
        experiments,
    )
    doc = json.loads(summary_json.read_text(encoding="utf-8"))
    metrics = {
        (m["cell"], m["metric_name"]): m
        for m in doc["measurements"]
    }

    assert summary_json.name == "sens.sensitivity_summary.json"
    assert summary_md.name == "sens.sensitivity_summary.md"
    assert doc["schema"] == orchestrator.SENSITIVITY_SUMMARY_SCHEMA
    assert doc["selected_cells"] == [exp.id for exp in experiments]
    assert metrics[(experiments[0].id, "source_total_ms_mean")]["value"] == 15.0
    assert metrics[(experiments[0].id, "read_p50_aggregate_by_type")]["unit"] == "ms"


def test_extract_resolves_sensitivity_cells():
    exp = extract._experiment("engsens-base")

    assert exp.id == "engsens-base"
    assert exp.db_name == "okg_bench_engsens_base"


def test_cartesian_mode_expands_all_combinations():
    spec = _minimal_spec(
        mode="cartesian",
        reports={"branching": {"enabled": True}},
    )
    plan = sensitivity.build_sensitivity_plan_from_spec(spec)

    assert plan["cell_count"] == 4
    cells = plan["cells"]
    ids = {cell["id"] for cell in cells}
    assert sum(1 for cell in cells if cell["baseline"]) == 1
    assert {
        "sens-base",
        "sens-src-fo-vw0",
        "sens-src-fo-vw1",
        "sens-src-fc-vw1",
    } == ids
    assert plan["baseline_cell"] == "sens-base"
    assert plan["reports"]["branching"]["cell"] in ids
    assert "--cell sens-base" in plan["reports"]["branching"]["command"]


def test_unavailable_projection_batch_axis_is_rejected():
    spec = _minimal_spec(
        axes={
            "engine": {
                "projection_batch": [500, 2000],
            },
        },
    )

    with pytest.raises(ValueError, match="projection_batch.*unavailable"):
        sensitivity.build_sensitivity_plan_from_spec(spec)


def test_unknown_workload_axis_is_rejected():
    spec = _minimal_spec(axes={"workload": {"bogus": [1]}})

    with pytest.raises(ValueError, match="unknown workload axis"):
        sensitivity.build_sensitivity_plan_from_spec(spec)


def test_dbos_id_cap_is_checked_before_runtime():
    spec = _minimal_spec(id="x" * 22)

    with pytest.raises(ValueError, match="DBOS-safe cell tags"):
        sensitivity.build_sensitivity_plan_from_spec(spec)
