"""Tests for ExperimentRecordsSource."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.experimental import (
    ExperimentRecordsSource,
    PredicateParseError,
)
from okg.substrate.library.sources.experimental.experiment_records import (
    ExperimentRecordParseError,
)


def _write_yaml(dir_: Path, name: str, body: str) -> Path:
    path = dir_ / name
    path.write_text(textwrap.dedent(body).lstrip("\n"))
    return path


def _run_source(src: ExperimentRecordsSource) -> tuple[list, list]:
    """Materialise the source's facts; split into nodes + edges."""
    run = src.run("run-1", mode="release_new")
    nodes: list[NodeFact] = []
    edges: list[EdgeFact] = []
    for fact in run.facts:
        if isinstance(fact, NodeFact):
            nodes.append(fact)
        elif isinstance(fact, EdgeFact):
            edges.append(fact)
    return nodes, edges


# ---------- happy path ----------


def test_emits_experiment_node_with_attrs(tmp_path: Path) -> None:
    records_dir = tmp_path / "experiments"
    records_dir.mkdir()
    _write_yaml(
        records_dir,
        "kramabench-2026-05-15.yaml",
        """
        name: kramabench-2026-05-15
        title: KramaBench OKG vs monolith comparison
        design: Run KramaBench v1 against OKG-backed and monolithic baselines.
        kind: benchmark
        status: complete
        """,
    )

    src = ExperimentRecordsSource(
        records_path="experiments/*.yaml",
        base_dir=tmp_path,
    )
    nodes, edges = _run_source(src)
    assert len(nodes) == 1
    n = nodes[0]
    assert n.subtype == "experiment"
    assert n.node_id == "exp:kramabench-2026-05-15"
    assert n.attrs["slug"] == "kramabench-2026-05-15"
    assert n.attrs["title"] == "KramaBench OKG vs monolith comparison"
    assert n.attrs["kind"] == "benchmark"
    assert n.attrs["status"] == "complete"
    assert edges == []


def test_emits_typed_edges_from_frontmatter(tmp_path: Path) -> None:
    records_dir = tmp_path / "experiments"
    records_dir.mkdir()
    _write_yaml(
        records_dir,
        "k.yaml",
        """
        name: kramabench-2026-05-15
        tests_hypothesis: hyp:kramabench-okg-faster-than-monolith
        uses_dataset: ds:kramabench-v1
        concerns_concept: concept:bitemporal-data
        ran_on_compute_job:
          cluster: cluster01
          job_id: 1287
        success_condition:
          metric: speedup
          predicate: ">= 1.0"
        """,
    )

    src = ExperimentRecordsSource(
        records_path="experiments/*.yaml",
        base_dir=tmp_path,
    )
    nodes, edges = _run_source(src)
    assert len(nodes) == 1
    n = nodes[0]
    assert "success_condition" in n.attrs
    # Stored as JSON string for downstream matcher access.
    import json
    sc = json.loads(n.attrs["success_condition"])
    assert sc == {"metric": "speedup", "predicate": ">= 1.0"}

    edge_pairs = sorted(
        (e.src, e.edge_type, e.dst) for e in edges
    )
    assert edge_pairs == sorted([
        (
            "exp:kramabench-2026-05-15",
            "tests",
            "hyp:kramabench-okg-faster-than-monolith",
        ),
        (
            "exp:kramabench-2026-05-15",
            "uses",
            "ds:kramabench-v1",
        ),
        (
            "exp:kramabench-2026-05-15",
            "concerns",
            "concept:bitemporal-data",
        ),
        (
            "exp:kramabench-2026-05-15",
            "ran_on",
            "job:cluster01:1287",
        ),
    ])


def test_record_set_includes_emitted_edges(tmp_path: Path) -> None:
    records_dir = tmp_path / "experiments"
    records_dir.mkdir()
    _write_yaml(
        records_dir,
        "k.yaml",
        """
        name: kramabench
        tests_hypothesis: hyp:kramabench
        """,
    )
    src = ExperimentRecordsSource(
        records_path="experiments/*.yaml",
        base_dir=tmp_path,
    )
    run = src.run("run-1", mode="release_new")
    assert run.record_set is not None
    assert "exp:kramabench" in run.record_set.records
    emission = run.record_set.records["exp:kramabench"]
    assert emission.primary is not None
    assert emission.primary.subtype == "experiment"
    edge_keys = {e.identity() for e in emission.edges}
    assert ("exp:kramabench", "tests", "hyp:kramabench") in edge_keys


def test_optional_fields_omitted(tmp_path: Path) -> None:
    """Missing optional frontmatter keys must NOT emit phantom edges."""
    records_dir = tmp_path / "experiments"
    records_dir.mkdir()
    _write_yaml(
        records_dir,
        "k.yaml",
        """
        name: minimal
        """,
    )
    src = ExperimentRecordsSource(
        records_path="experiments/*.yaml",
        base_dir=tmp_path,
    )
    nodes, edges = _run_source(src)
    assert len(nodes) == 1
    assert edges == []


def test_multiple_files_emit_in_sorted_order(tmp_path: Path) -> None:
    records_dir = tmp_path / "experiments"
    records_dir.mkdir()
    _write_yaml(records_dir, "b.yaml", "name: bravo\n")
    _write_yaml(records_dir, "a.yaml", "name: alpha\n")
    src = ExperimentRecordsSource(
        records_path="experiments/*.yaml",
        base_dir=tmp_path,
    )
    nodes, _ = _run_source(src)
    assert [n.node_id for n in nodes] == ["exp:alpha", "exp:bravo"]


# ---------- failure modes ----------


def test_missing_name_raises(tmp_path: Path) -> None:
    records_dir = tmp_path / "experiments"
    records_dir.mkdir()
    _write_yaml(records_dir, "k.yaml", "title: Nameless\n")
    src = ExperimentRecordsSource(
        records_path="experiments/*.yaml",
        base_dir=tmp_path,
    )
    with pytest.raises(ExperimentRecordParseError) as exc_info:
        list(src.run("r", mode="release_new").facts)
    assert "name" in exc_info.value.message


def test_bad_predicate_raises_loud(tmp_path: Path) -> None:
    records_dir = tmp_path / "experiments"
    records_dir.mkdir()
    _write_yaml(
        records_dir,
        "k.yaml",
        """
        name: k
        success_condition:
          metric: speedup
          predicate: "median > 1 AND std < 0.1"
        """,
    )
    src = ExperimentRecordsSource(
        records_path="experiments/*.yaml",
        base_dir=tmp_path,
    )
    with pytest.raises(PredicateParseError) as exc_info:
        list(src.run("r", mode="release_new").facts)
    assert "experiment" in exc_info.value.message
    assert "supported predicates" in (exc_info.value.fix_hint or "")


def test_malformed_yaml_raises(tmp_path: Path) -> None:
    records_dir = tmp_path / "experiments"
    records_dir.mkdir()
    (records_dir / "broken.yaml").write_text("name: k\n: nope\n")
    src = ExperimentRecordsSource(
        records_path="experiments/*.yaml",
        base_dir=tmp_path,
    )
    with pytest.raises(ExperimentRecordParseError):
        list(src.run("r", mode="release_new").facts)


def test_ran_on_compute_job_requires_both_fields(tmp_path: Path) -> None:
    records_dir = tmp_path / "experiments"
    records_dir.mkdir()
    _write_yaml(
        records_dir,
        "k.yaml",
        """
        name: k
        ran_on_compute_job:
          cluster: cluster01
        """,
    )
    src = ExperimentRecordsSource(
        records_path="experiments/*.yaml",
        base_dir=tmp_path,
    )
    with pytest.raises(ExperimentRecordParseError) as exc_info:
        list(src.run("r", mode="release_new").facts)
    assert "job_id" in exc_info.value.message


def test_success_condition_must_be_mapping(tmp_path: Path) -> None:
    records_dir = tmp_path / "experiments"
    records_dir.mkdir()
    _write_yaml(
        records_dir,
        "k.yaml",
        """
        name: k
        success_condition: "metric: speedup, predicate: >= 1.0"
        """,
    )
    src = ExperimentRecordsSource(
        records_path="experiments/*.yaml",
        base_dir=tmp_path,
    )
    with pytest.raises(ExperimentRecordParseError) as exc_info:
        list(src.run("r", mode="release_new").facts)
    assert "success_condition" in exc_info.value.message


def test_metadata_attrs_round_trip(tmp_path: Path) -> None:
    records_dir = tmp_path / "experiments"
    records_dir.mkdir()
    _write_yaml(
        records_dir,
        "k.yaml",
        """
        name: meta-test
        title: A title
        hypothesis_ref: hyp:meta
        """,
    )
    src = ExperimentRecordsSource(
        records_path="experiments/*.yaml",
        base_dir=tmp_path,
    )
    nodes, _ = _run_source(src)
    assert nodes[0].attrs["hypothesis_ref"] == "hyp:meta"
    assert nodes[0].attrs["title"] == "A title"
