"""Ingest bench sweep_summary.json into okg-ci BenchMeasurement nodes
(PACT ingest-bench-into-okg-ci). The live okg-ci publish + regression query is
the end-to-end proof; these lock the source emission + the mirror gate."""
from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.test_reports import TestReportSource
from tests.mirror_scope import private_path

_SHA = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"


def _summary() -> dict:
    return {
        "schema": "okg.bench.sweep_summary.v1",
        "measurements": [
            {"cell": "tdb-enr-enall", "config_axis": "enrichers",
             "axis_value": "all", "metric_name": "enrichment_ms_mean",
             "value": 28.9, "unit": "ms"},
            {"cell": "tdb-enr-en0", "config_axis": "enrichers",
             "axis_value": "none", "metric_name": "enrichment_ms_mean",
             "value": None, "unit": "ms"},  # absent -> must be skipped
            {"cell": "tdb-branch", "config_axis": "branching",
             "axis_value": "branch", "metric_name": "compact_ms",
             "value": 158.8, "unit": "ms"},
        ],
    }


def _write_summary(tmp_path: Path, doc: dict) -> Path:
    p = tmp_path / "runs" / _SHA / "bench-internals" / "bench" / "sweep_summary.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(doc), encoding="utf-8")
    return p


def _write_sensitivity_summary(tmp_path: Path, doc: dict) -> Path:
    p = (
        tmp_path / "runs" / _SHA / "bench-internals" / "bench"
        / "engsens.sensitivity_summary.json"
    )
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(doc), encoding="utf-8")
    return p


def test_bench_facts_emits_measurement_nodes_and_commit_edges(tmp_path):
    src = TestReportSource(repo="okg", repo_root=tmp_path)
    p = _write_summary(tmp_path, _summary())
    facts = src._bench_facts(p, _summary(), sha=_SHA)
    nodes = [f for f in facts if isinstance(f, NodeFact)]
    edges = [f for f in facts if isinstance(f, EdgeFact)]

    # 2 valid measurements; the None-value one is skipped (no 0/placeholder).
    assert len(nodes) == 2
    by = {(n.attrs["cell"], n.attrs["metric_name"]): n for n in nodes}
    assert ("tdb-enr-en0", "enrichment_ms_mean") not in by

    enall = by[("tdb-enr-enall", "enrichment_ms_mean")]
    assert enall.subtype == "bench_measurement"
    assert enall.attrs["value"] == 28.9
    assert enall.attrs["unit"] == "ms"
    assert enall.attrs["head_sha"] == _SHA
    assert enall.attrs["config_axis"] == "enrichers"

    # each measurement gets an `affects` edge to its commit (regression traversal)
    assert len(edges) == 2
    assert all(e.edge_type == "affects" for e in edges)
    assert all(e.dst == f"commit:okg:{_SHA}" for e in edges)


def test_sha_derived_from_runs_path(tmp_path):
    src = TestReportSource(repo="okg", repo_root=tmp_path, sha_from_path=True,
                           infer_sha=False)
    p = _write_summary(tmp_path, _summary())
    assert src._sha_for_artifact(p, fallback=None) == _SHA


def test_json_dispatch_routes_bench_schema(tmp_path):
    # the generic JSON dispatch must recognize the bench schema
    src = TestReportSource(repo="okg", repo_root=tmp_path)
    p = _write_summary(tmp_path, _summary())
    facts = src._facts_for_artifact(p, sha=_SHA)
    assert any(isinstance(f, NodeFact) and f.subtype == "bench_measurement"
               for f in facts)


def test_json_dispatch_routes_sensitivity_bench_schema(tmp_path):
    src = TestReportSource(repo="okg", repo_root=tmp_path)
    doc = _summary() | {"schema": "okg.bench.sensitivity_summary.v1"}
    p = _write_sensitivity_summary(tmp_path, doc)
    facts = src._facts_for_artifact(p, sha=_SHA)
    assert any(isinstance(f, NodeFact) and f.subtype == "bench_measurement"
               for f in facts)


# --- mirror gate -----------------------------------------------------------

# deployments/okg-ci/ is this repository's own deployment. The mirror module
# is loaded by a fixture, not at import, so the source tests above still run in
# the public copy, which skips the tests below (tests/mirror_scope.py).
@pytest.fixture(scope="module")
def _mirror():
    path = private_path("deployments/okg-ci/mirror_ci_results.py")
    spec = importlib.util.spec_from_file_location("okgci_mirror", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["okgci_mirror"] = module
    spec.loader.exec_module(module)
    return module


def test_mirror_accepts_ci_bench_lane(_mirror):
    assert _mirror._safe_lane("ci-bench-internals") == "bench-internals"
    assert _mirror._safe_lane("bench-internals") is None  # missing ci- prefix


def test_mirror_glob_finds_sweep_summary(tmp_path):
    # the artifact preserves the uploaded bench/results/ path; the mirror globs
    # **/sweep_summary.json to find it regardless of nesting.
    art = tmp_path / "ci-bench-internals" / "bench" / "results"
    art.mkdir(parents=True)
    (art / "sweep_summary.json").write_text("{}", encoding="utf-8")
    found = list((tmp_path / "ci-bench-internals").glob("**/sweep_summary.json"))
    assert len(found) == 1


def test_mirror_glob_finds_sensitivity_summary(tmp_path, _mirror):
    # sensitivity summaries use the same BenchMeasurement ingestion shape as
    # sweep summaries, under an honest file name/schema.
    art = tmp_path / "ci-bench-internals" / "bench" / "results"
    art.mkdir(parents=True)
    (art / "engsens.sensitivity_summary.json").write_text("{}", encoding="utf-8")
    (art / "engsens.sensitivity_summary.md").write_text("{}", encoding="utf-8")
    found = _mirror._bench_summary_files(tmp_path / "ci-bench-internals")
    assert {p.name for p in found} == {
        "engsens.sensitivity_summary.json",
        "engsens.sensitivity_summary.md",
    }


# --- off-git transient cache (scalable-ci-test-result-ingestion) ------------


def test_mirror_prune_ingested_removes_only_graph_runs(tmp_path, _mirror):
    # a run whose (sha, run_id) pair already has a test_run in the graph is dead
    # weight in the cache -> pruned; an un-ingested run is kept (it still needs
    # ingesting). Identity is the PAIR: several workflow runs share one sha.
    sha_in, sha_out = "a" * 40, "b" * 40
    for index, sha in enumerate((sha_in, sha_out)):
        lane = tmp_path / "runs" / sha / "fast"
        (lane / "junit").mkdir(parents=True)
        (lane / "junit" / "r.xml").write_text(
            f'<testsuite name="{sha}"/>', encoding="utf-8"
        )
        _mirror._write_artifact_manifest(
            lane_dir=lane,
            run_obj={"databaseId": 700 + index, "workflowName": "Tests"},
            sha=sha,
            run_id=str(700 + index),
            artifact_name="ci-fast",
            artifact_meta=None,
            lane="fast",
            content_kinds={"junit"},
        )
    index = _mirror.GraphIndex(
        lanes={(sha_in, 700, 1, "fast")},
        report_hashes={
            _mirror._report_text_hash(tmp_path / "runs" / sha_in / "fast"
                                      / "junit" / "r.xml")
        },
    )
    assert _mirror.archive_ingested(
        tmp_path, index, _mirror.Unremovable(tmp_path)
    ) == 1
    assert not (tmp_path / "runs" / sha_in).exists()
    assert (tmp_path / "history" / sha_in / "fast").is_dir()
    assert (tmp_path / "runs" / sha_out).exists()


def test_mirror_cache_cap_evicts_oldest_history_and_never_waiting_runs(tmp_path, _mirror):
    runs, history = tmp_path / "runs", tmp_path / "history"
    waiting = [f"{i:040x}" for i in range(3)]
    kept = [f"{i:040x}" for i in range(10, 15)]
    for root, shas in ((runs, waiting), (history, kept)):
        for i, s in enumerate(shas):
            d = root / s
            d.mkdir(parents=True)
            (d / "r.xml").write_text("x" * 100, encoding="utf-8")
            os.utime(d, (1000 + i, 1000 + i))  # ascending mtime -> last newest
    # Room for the three waiting dirs plus the two newest history dirs.
    evicted, _ = _mirror.enforce_cache_cap(
        tmp_path, 500, _mirror.Unremovable(tmp_path)
    )
    assert evicted == 3
    assert {d.name for d in history.iterdir()} == set(kept[-2:])
    assert {d.name for d in runs.iterdir()} == set(waiting)


def test_mirror_run_skips_an_already_ingested_run(tmp_path, monkeypatch, _mirror):
    # the ingested-skip is an early return BEFORE any gh download, and it is
    # keyed by (sha, run_id, run_attempt) -- a sha alone can never authorize the
    # skip -- and it fires only once EVERY lane the run published is ingested.
    sha = "c" * 40
    calls = []
    monkeypatch.setattr(_mirror, "_run",
                        lambda cmd, **_kw: calls.append(cmd) or (_ for _ in ()).throw(
                            AssertionError(f"downloaded despite skip: {cmd}")))
    monkeypatch.setattr(_mirror, "_published_ci_lanes", lambda _rid: {"fast"})
    run = {"headSha": sha, "databaseId": 1, "status": "completed",
           "conclusion": "success", "workflowName": "Tests"}
    n = _mirror.mirror_run(run, tmp_path, tmp_path / "tmp", {(sha, 1, 1): {"fast"}})
    assert n == 0
    assert calls == []
    assert not (tmp_path / "runs" / sha).exists()


def test_mirror_run_skips_in_progress_run(tmp_path, _mirror):
    # an in-progress run has only its finished lanes uploaded; downloading it
    # would stage a partial lane set -> never download until completed.
    sha = "d" * 40
    n = _mirror.mirror_run({"headSha": sha, "databaseId": 2, "status": "in_progress"},
                           tmp_path, tmp_path / "tmp", {})
    assert n == 0
    assert not (tmp_path / "runs" / sha).exists()


def test_mirror_run_skips_cancelled_run(tmp_path, _mirror):
    # a cancelled run is a partial lane set for an abandoned (re-pushed) sha;
    # ingesting it would then prune the partial set as if complete.
    sha = "e" * 40
    n = _mirror.mirror_run(
        {"headSha": sha, "databaseId": 3, "status": "completed",
         "conclusion": "cancelled"}, tmp_path, tmp_path / "tmp", {})
    assert n == 0
    assert not (tmp_path / "runs" / sha).exists()


def test_mirror_safe_copy_rejects_symlink_source(tmp_path, _mirror):
    # a symlink in a CI artifact is never legitimate; following it would read an
    # arbitrary host file into the cache. Reject it.
    dl = tmp_path / "dl"
    dl.mkdir()
    secret = tmp_path / "secret.xml"
    secret.write_text("<secret/>", encoding="utf-8")
    link = dl / "results.xml"
    link.symlink_to(secret)  # leaf name matches the junit glob
    base = (tmp_path / "base")
    base.mkdir()
    dest = base / "out.xml"
    assert _mirror._safe_copy(link, dest, base.resolve(), dl) is False
    assert not dest.exists()


def test_mirror_safe_copy_allows_real_file_in_download(tmp_path, _mirror):
    dl = tmp_path / "dl"
    (dl / "sub").mkdir(parents=True)
    src = dl / "sub" / "r.xml"
    src.write_text("<ok/>", encoding="utf-8")
    base = tmp_path / "base"
    base.mkdir()
    dest = base / "r.xml"
    assert _mirror._safe_copy(src, dest, base.resolve(), dl) is True
    assert dest.read_text(encoding="utf-8") == "<ok/>"


def test_mirror_is_off_git_and_graph_pruned(_mirror):
    # the rewrite dropped ALL git operations -- the durable store is the OKG
    # graph, not the ci-results branch.
    assert not hasattr(_mirror, "ensure_worktree")
    for fn in ("ingested_index", "archive_ingested", "enforce_cache_cap"):
        assert callable(getattr(_mirror, fn))
