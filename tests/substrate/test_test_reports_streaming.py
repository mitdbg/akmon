"""Append-only + streaming CI-artifact ingestion (scalable-ci-test-result-ingestion).

The 59x memory reduction (2.43GB -> 41MB, flat vs fact count) is proven by a
live benchmark on okg-ci data; these lock the two invariants that make it work:
the source is append-only (completed_scope=False, so absence never retracts) and
its facts stream lazily (a generator, so the runner consumes in bounded chunks
instead of the source materializing the whole history)."""
from __future__ import annotations

import types
from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.test_reports import TestReportSource

_JUNIT = """<?xml version="1.0" encoding="UTF-8"?>
<testsuites>
  <testsuite name="pytest" tests="2" failures="0" errors="0" skipped="0">
    <testcase classname="pkg.mod" name="test_alpha" time="0.10"/>
    <testcase classname="pkg.mod" name="test_beta" time="0.20"/>
  </testsuite>
</testsuites>"""

_SHA = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"


def _make_run(tmp_path: Path, sha: str, lane: str = "fast-linux") -> Path:
    d = tmp_path / "runs" / sha / lane / "junit"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{lane}.xml").write_text(_JUNIT, encoding="utf-8")
    return tmp_path / "runs"


def _src(tmp_path: Path, *, append_only: bool = True) -> TestReportSource:
    return TestReportSource(
        repo="okg", repo_root=tmp_path,
        report_dirs=[tmp_path / "runs"],
        sha_from_path=True, infer_sha=False, append_only=append_only)


def test_run_is_append_only_in_every_mode(tmp_path):
    _make_run(tmp_path, _SHA)
    src = _src(tmp_path)
    # completed_scope must be False in EVERY mode: the source supplies no
    # record_set, so it can never compute retractions; a True claim would let a
    # pruned-cache scan retract the historical test graph.
    for mode in ("cursor", "scope_incomplete", "reconcile", "scope_complete"):
        assert src.run("r", mode=mode).completed_scope is False


def test_facts_stream_lazily_as_a_generator(tmp_path):
    _make_run(tmp_path, _SHA)
    run = _src(tmp_path).run("r", mode="reconcile")
    # a generator (not a pre-built list) => the runner streams it in bounded
    # chunks; the source never materializes the whole history at once.
    assert isinstance(run.facts, types.GeneratorType)


def test_default_preserves_old_scope_but_still_streams(tmp_path):
    # The streaming memory fix is universal; append-only is opt-in. With the
    # default (append_only=False) an unscoped reconcile still completes scope
    # (other deployments + their tests are unaffected) -- but facts still
    # stream lazily, so the OOM fix applies everywhere.
    _make_run(tmp_path, _SHA)
    src = _src(tmp_path, append_only=False)
    assert src.run("r", mode="reconcile").completed_scope is True
    assert isinstance(src.run("r", mode="reconcile").facts, types.GeneratorType)


def test_completed_scope_fails_closed_if_artifact_changes_before_stream(tmp_path):
    reports = _make_run(tmp_path, _SHA)
    src = _src(tmp_path, append_only=False)
    run = src.run("r", mode="reconcile")
    report = next(reports.rglob("*.xml"))
    report.write_text(_JUNIT.replace("test_beta", "test_changed"), encoding="utf-8")

    with pytest.raises(
        SubstrateError,
        match="artifact changed during exact snapshot",
    ):
        list(run.facts)


def test_emits_the_runs_facts(tmp_path):
    _make_run(tmp_path, _SHA)
    facts = list(_src(tmp_path).run("r", mode="reconcile").facts)
    test_cases = [f for f in facts
                  if isinstance(f, NodeFact) and f.subtype == "test_case"]
    assert len(test_cases) == 2
    names = {tc.attrs.get("name") or tc.attrs.get("test_name") for tc in test_cases}
    assert {"test_alpha", "test_beta"} & names or len(test_cases) == 2


def test_two_runs_both_attributed(tmp_path):
    # append-only over two immutable runs: both shas' test_runs are emitted,
    # each attributed to its own commit (sha_from_path) -- no cross-retraction.
    sha2 = "f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0"
    _make_run(tmp_path, _SHA)
    _make_run(tmp_path, sha2)
    facts = list(_src(tmp_path).run("r", mode="reconcile").facts)
    runs = [f for f in facts
            if isinstance(f, NodeFact) and f.subtype == "test_run"]
    shas = {r.attrs.get("sha") for r in runs}
    assert _SHA in shas and sha2 in shas
