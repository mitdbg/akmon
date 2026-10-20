"""JUnitXmlSource — JUnit XML drop directory ingestion.

Per `add-ci-test-source-adapter` (2026-04-26).
"""
from __future__ import annotations

import pytest

pytest.importorskip("defusedxml")

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import (
    EdgeFact, NodeFact,
)
from okg.substrate.library.sources.junit_xml import (
    JUnitXmlSource,
)


_SAMPLE_XML = """<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="pytest" tests="2" failures="1" errors="0" skipped="0" time="0.05">
    <testcase classname="tests.foo" name="test_passes" time="0.01" file="tests/foo.py"/>
    <testcase classname="tests.foo" name="test_fails" time="0.04" file="tests/foo.py">
      <failure message="boom">stack trace</failure>
    </testcase>
  </testsuite>
</testsuites>
"""


def _drop(tmp_path):
    d = tmp_path / "junit"
    d.mkdir()
    (d / "results.xml").write_text(_SAMPLE_XML)
    return d


def _drop_two(tmp_path):
    d = tmp_path / "junit"
    d.mkdir()
    (d / "alpha.xml").write_text(_SAMPLE_XML)
    (d / "beta.xml").write_text(
        _SAMPLE_XML.replace("tests.foo", "tests.beta"),
    )
    return d


def test_emits_test_run_and_test_cases(tmp_path):
    src = JUnitXmlSource(
        drop_dir=_drop(tmp_path),
        sha="deadbeefcafe",
        suite_name="pytest",
    )
    facts = list(src.run("rid", mode="scope_complete").facts)
    nodes = [f for f in facts if isinstance(f, NodeFact)]
    edges = [f for f in facts if isinstance(f, EdgeFact)]

    subtypes = {n.subtype for n in nodes}
    assert {"test_report", "test_run", "test_case", "runtime_event"} <= subtypes

    runs = [n for n in nodes if n.subtype == "test_run"]
    assert len(runs) == 1
    assert runs[0].attrs["n_passed"] == 1
    assert runs[0].attrs["n_failed"] == 1
    assert runs[0].attrs["sha"] == "deadbeefcafe"

    cases = [n for n in nodes if n.subtype == "test_case"]
    statuses = {c.attrs["status"] for c in cases}
    assert statuses == {"passed", "failed"}

    # Failure carries message.
    failed = [c for c in cases if c.attrs["status"] == "failed"][0]
    assert "boom" in failed.attrs["message"]
    reports = [n for n in nodes if n.subtype == "test_report"]
    assert len(reports) == 1
    events = [n for n in nodes if n.subtype == "runtime_event"]
    assert len(events) == 1

    # `affects` edge: test_run → commit:okg:<sha>.
    affects_edges = [e for e in edges if e.edge_type == "affects"]
    assert len(affects_edges) == 1
    assert affects_edges[0].dst == "commit:okg:deadbeefcafe"
    assert any(
        e.edge_type == "contains"
        and e.src.startswith("test-report:")
        and e.dst.startswith("test-case:")
        for e in edges
    )
    assert any(
        e.edge_type == "contains"
        and e.src.startswith("test-report:")
        and e.dst.startswith("runtime-event:")
        for e in edges
    )


def test_no_sha_omits_commit_edge(tmp_path):
    src = JUnitXmlSource(drop_dir=_drop(tmp_path))  # no sha, no repo_root
    facts = list(src.run("rid", mode="scope_complete").facts)
    edges = [f for f in facts if isinstance(f, EdgeFact)]
    assert not any(e.edge_type == "affects" for e in edges)


def test_missing_drop_dir_raises():
    src = JUnitXmlSource(drop_dir="/nonexistent/path")
    with pytest.raises(SubstrateError):
        list(src.run("rid", mode="scope_complete").facts)


def test_file_attribute_emits_source_file_edge(tmp_path):
    src = JUnitXmlSource(
        drop_dir=_drop(tmp_path),
        repo_root=tmp_path,  # so _relpath works
    )
    facts = list(src.run("rid", mode="scope_complete").facts)
    edges = [f for f in facts if isinstance(f, EdgeFact)]
    file_refs = [
        e for e in edges
        if e.edge_type == "references" and e.dst.startswith("file:")
    ]
    assert len(file_refs) == 2  # both test cases have file= attr


def test_scoped_run_limits_junit_xml_to_requested_file(tmp_path):
    drop = _drop_two(tmp_path)
    src = JUnitXmlSource(
        drop_dir=drop,
        repo_root=tmp_path,
        sha="deadbeefcafe",
    )

    run = src.run(
        "rid",
        mode="scope_incomplete",
        sync_scope={"scope_keys": ["beta.xml", "../escape.xml"]},
    )
    reports = [
        f for f in run.facts
        if isinstance(f, NodeFact) and f.subtype == "test_report"
    ]

    assert run.completed_scope is False
    assert [node.attrs["path"] for node in reports] == ["junit/beta.xml"]


def test_scoped_run_accepts_repo_relative_junit_xml_path(tmp_path):
    drop = _drop_two(tmp_path)
    src = JUnitXmlSource(
        drop_dir=drop,
        repo_root=tmp_path,
        sha="deadbeefcafe",
    )

    run = src.run(
        "rid",
        mode="scope_incomplete",
        sync_scope={"scope_keys": ["junit/alpha.xml"]},
    )
    reports = [
        f for f in run.facts
        if isinstance(f, NodeFact) and f.subtype == "test_report"
    ]

    assert [node.attrs["path"] for node in reports] == ["junit/alpha.xml"]
