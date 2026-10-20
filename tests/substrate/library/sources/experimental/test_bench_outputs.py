"""Tests for BenchOutputsSource — JSON / YAML / CSV emission."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.experimental import (
    BenchOutputsSource,
    ResultSchemaError,
)


_SCHEMA = {
    "experiment_ref": "experiment",
    "metric_name": "metric",
    "metric_value": "value",
    "metric_unit": "unit",
}


def _run(src: BenchOutputsSource) -> tuple[list[NodeFact], list[EdgeFact]]:
    run = src.run("r1", mode="release_new")
    nodes: list[NodeFact] = []
    edges: list[EdgeFact] = []
    for fact in run.facts:
        if isinstance(fact, NodeFact):
            nodes.append(fact)
        elif isinstance(fact, EdgeFact):
            edges.append(fact)
    return nodes, edges


# ---------- CSV ----------


def test_csv_results_emit_result_nodes(tmp_path: Path) -> None:
    bench_dir = tmp_path / "bench"
    bench_dir.mkdir()
    with (bench_dir / "results.csv").open("w") as f:
        writer = csv.writer(f)
        writer.writerow(["experiment", "metric", "value", "unit"])
        writer.writerow(["exp:kramabench", "speedup", "1.42", "x"])
        writer.writerow(["exp:kramabench", "latency_ms", "14", "ms"])

    src = BenchOutputsSource(
        results_glob="bench/*.csv",
        result_schema=_SCHEMA,
        base_dir=tmp_path,
    )
    nodes, edges = _run(src)
    assert len(nodes) == 2
    n = nodes[0]
    assert n.subtype == "result"
    assert n.attrs["experiment_ref"] == "exp:kramabench"
    assert n.attrs["metric_name"] == "speedup"
    assert n.attrs["metric_value"] == 1.42
    assert n.attrs["metric_unit"] == "x"

    # Each row also emits a Trial -> Result produces edge.
    assert all(e.edge_type == "produces" for e in edges)
    assert all(e.dst.startswith("result:") for e in edges)
    # Default trial id is `trial:<experiment-slug>:default`.
    assert all(e.src == "trial:kramabench:default" for e in edges)


# ---------- JSON ----------


def test_json_list_emits_result_nodes(tmp_path: Path) -> None:
    bench_dir = tmp_path / "bench"
    bench_dir.mkdir()
    (bench_dir / "results.json").write_text(json.dumps([
        {
            "experiment": "exp:k",
            "metric": "speedup",
            "value": 1.05,
            "unit": "x",
        },
        {
            "experiment": "exp:k",
            "metric": "accuracy",
            "value": 0.96,
            "unit": "%",
        },
    ]))

    src = BenchOutputsSource(
        results_glob="bench/*.json",
        result_schema=_SCHEMA,
        base_dir=tmp_path,
    )
    nodes, _ = _run(src)
    assert len(nodes) == 2
    assert {n.attrs["metric_name"] for n in nodes} == {"speedup", "accuracy"}


def test_json_with_results_key(tmp_path: Path) -> None:
    bench_dir = tmp_path / "bench"
    bench_dir.mkdir()
    (bench_dir / "results.json").write_text(json.dumps({
        "results": [
            {"experiment": "exp:k", "metric": "speedup", "value": 1.0},
        ],
    }))
    src = BenchOutputsSource(
        results_glob="bench/*.json",
        result_schema={k: v for k, v in _SCHEMA.items() if k != "metric_unit"},
        base_dir=tmp_path,
    )
    nodes, _ = _run(src)
    assert len(nodes) == 1


# ---------- YAML ----------


def test_yaml_list_emits_result_nodes(tmp_path: Path) -> None:
    bench_dir = tmp_path / "bench"
    bench_dir.mkdir()
    (bench_dir / "results.yaml").write_text(yaml.safe_dump([
        {"experiment": "exp:k", "metric": "speedup", "value": 2.0},
        {"experiment": "exp:k", "metric": "speedup", "value": 0.9},
    ]))
    src = BenchOutputsSource(
        results_glob="bench/*.yaml",
        result_schema={k: v for k, v in _SCHEMA.items() if k != "metric_unit"},
        base_dir=tmp_path,
    )
    nodes, _ = _run(src)
    assert len(nodes) == 2
    values = sorted(n.attrs["metric_value"] for n in nodes)
    assert values == [0.9, 2.0]


# ---------- mixed file types ----------


def test_multiple_formats_compose(tmp_path: Path) -> None:
    bench_dir = tmp_path / "bench"
    bench_dir.mkdir()
    (bench_dir / "results.json").write_text(json.dumps([
        {"experiment": "exp:k", "metric": "m1", "value": 1.0},
    ]))
    (bench_dir / "results.yaml").write_text(yaml.safe_dump([
        {"experiment": "exp:k", "metric": "m2", "value": 2.0},
    ]))
    with (bench_dir / "results.csv").open("w") as f:
        writer = csv.writer(f)
        writer.writerow(["experiment", "metric", "value"])
        writer.writerow(["exp:k", "m3", "3.0"])

    src = BenchOutputsSource(
        results_glob="bench/*",
        result_schema={k: v for k, v in _SCHEMA.items() if k != "metric_unit"},
        base_dir=tmp_path,
    )
    nodes, _ = _run(src)
    metrics = {n.attrs["metric_name"] for n in nodes}
    assert metrics == {"m1", "m2", "m3"}


def test_unknown_extension_silently_skipped(tmp_path: Path) -> None:
    bench_dir = tmp_path / "bench"
    bench_dir.mkdir()
    (bench_dir / "stranger.tsv").write_text("experiment\tmetric\tvalue\nexp:k\tm\t1.0\n")
    src = BenchOutputsSource(
        results_glob="bench/*",
        result_schema={k: v for k, v in _SCHEMA.items() if k != "metric_unit"},
        base_dir=tmp_path,
    )
    nodes, _ = _run(src)
    assert nodes == []


# ---------- schema validation ----------


def test_missing_required_schema_key_raises_at_init(tmp_path: Path) -> None:
    with pytest.raises(ResultSchemaError) as exc_info:
        BenchOutputsSource(
            results_glob="bench/*.csv",
            result_schema={"metric_name": "m"},  # missing experiment_ref + metric_value
            base_dir=tmp_path,
        )
    assert "experiment_ref" in exc_info.value.message or (
        "experiment_ref" in (exc_info.value.fix_hint or "")
    )


def test_unknown_schema_key_raises_at_init(tmp_path: Path) -> None:
    with pytest.raises(ResultSchemaError):
        BenchOutputsSource(
            results_glob="bench/*.csv",
            result_schema={
                **_SCHEMA,
                "made_up_field": "foo",
            },
            base_dir=tmp_path,
        )


def test_row_missing_required_field_raises(tmp_path: Path) -> None:
    bench_dir = tmp_path / "bench"
    bench_dir.mkdir()
    with (bench_dir / "broken.csv").open("w") as f:
        writer = csv.writer(f)
        writer.writerow(["experiment", "metric", "value"])
        writer.writerow(["", "speedup", "1.0"])  # missing experiment

    src = BenchOutputsSource(
        results_glob="bench/*.csv",
        result_schema={k: v for k, v in _SCHEMA.items() if k != "metric_unit"},
        base_dir=tmp_path,
    )
    with pytest.raises(ResultSchemaError) as exc_info:
        list(src.run("r", mode="release_new").facts)
    assert "experiment_ref" in exc_info.value.message


def test_non_numeric_value_raises(tmp_path: Path) -> None:
    bench_dir = tmp_path / "bench"
    bench_dir.mkdir()
    with (bench_dir / "broken.csv").open("w") as f:
        writer = csv.writer(f)
        writer.writerow(["experiment", "metric", "value"])
        writer.writerow(["exp:k", "speedup", "not-a-number"])

    src = BenchOutputsSource(
        results_glob="bench/*.csv",
        result_schema={k: v for k, v in _SCHEMA.items() if k != "metric_unit"},
        base_dir=tmp_path,
    )
    with pytest.raises(ResultSchemaError) as exc_info:
        list(src.run("r", mode="release_new").facts)
    assert "metric_value" in exc_info.value.message


# ---------- result_id determinism ----------


def test_result_id_is_deterministic(tmp_path: Path) -> None:
    bench_dir = tmp_path / "bench"
    bench_dir.mkdir()
    csv_path = bench_dir / "r.csv"
    with csv_path.open("w") as f:
        writer = csv.writer(f)
        writer.writerow(["experiment", "metric", "value"])
        writer.writerow(["exp:k", "speedup", "1.0"])

    schema = {k: v for k, v in _SCHEMA.items() if k != "metric_unit"}

    src_a = BenchOutputsSource(
        results_glob="bench/*.csv",
        result_schema=schema,
        base_dir=tmp_path,
    )
    src_b = BenchOutputsSource(
        results_glob="bench/*.csv",
        result_schema=schema,
        base_dir=tmp_path,
    )
    nodes_a, _ = _run(src_a)
    nodes_b, _ = _run(src_b)
    assert nodes_a[0].node_id == nodes_b[0].node_id
    assert nodes_a[0].node_id.startswith("result:")


# ---------- optional step ----------


def test_metric_step_optional_integer(tmp_path: Path) -> None:
    bench_dir = tmp_path / "bench"
    bench_dir.mkdir()
    with (bench_dir / "r.csv").open("w") as f:
        writer = csv.writer(f)
        writer.writerow(["experiment", "metric", "value", "step"])
        writer.writerow(["exp:k", "loss", "0.1", "100"])

    src = BenchOutputsSource(
        results_glob="bench/*.csv",
        result_schema={
            "experiment_ref": "experiment",
            "metric_name": "metric",
            "metric_value": "value",
            "metric_step": "step",
        },
        base_dir=tmp_path,
    )
    nodes, _ = _run(src)
    assert nodes[0].attrs["metric_step"] == 100
