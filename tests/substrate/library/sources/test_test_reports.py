from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

pytest.importorskip("defusedxml")

from okg.substrate.library.sources.base import EdgeFact, NodeFact


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_test_reports_declares_verified_empty_artifact_scope(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.test_reports import TestReportSource

    repo = tmp_path / "repo"
    repo.mkdir()
    run = TestReportSource(
        repo="demo",
        repo_root=repo,
    ).run("run-empty", mode="scope_complete")

    assert list(run.facts) == []
    assert run.completed_scope is True
    assert run.record_set is not None
    assert run.record_set.records == {}


@pytest.mark.parametrize("mode", ["cursor", "scope_complete"])
def test_test_reports_missing_repo_root_fails_the_run(
    tmp_path: Path, mode: str
) -> None:
    from okg.substrate.errors import SourceRootMissing
    from okg.substrate.library.sources.test_reports import TestReportSource

    missing = tmp_path / "absent-root"
    source = TestReportSource(
        repo="demo",
        repo_root=missing,
        report_dirs=["reports"],
        name="ci_reports",
    )

    with pytest.raises(SourceRootMissing) as raised:
        source.run("run-missing-root", mode=mode)

    assert raised.value.code == "source_root_missing"
    assert raised.value.data == {
        "source": "ci_reports",
        "setting": "repo_root",
        "path": str(missing.resolve()),
    }
    assert "ci_reports" in str(raised.value)
    assert "repo_root" in str(raised.value)
    assert str(missing.resolve()) in str(raised.value)
    assert raised.value.fix_hint == SourceRootMissing.FIX_HINT
    # The change probe enumerates the same root; it must not hand the runner
    # a stable "nothing here" token for a missing root, so it forces a read.
    with pytest.raises(SourceRootMissing):
        source.change_probe.build_token()
    verdict = source.change_probe.probe(cursor=None, sync_scope=None)
    assert verdict.changed is True
    assert not verdict.token


def test_test_reports_probe_does_not_skip_after_root_is_deleted(
    tmp_path: Path,
) -> None:
    """A token saved while the root was empty must not let the runner skip.

    Before the fix a deleted root enumerated as zero artifacts, exactly like
    the empty root it replaced, so the saved token matched and the runner
    skipped the read on every later poll.
    """
    import shutil

    from okg.substrate.library.sources.change_probe import CURSOR_TOKEN_KEY
    from okg.substrate.library.sources.test_reports import TestReportSource

    root = tmp_path / "empty-report-root"
    root.mkdir()
    source = TestReportSource(repo="demo", repo_root=root, name="ci_reports")
    saved = source.change_probe.build_token()
    assert saved

    shutil.rmtree(root)
    verdict = source.change_probe.probe(
        cursor={CURSOR_TOKEN_KEY: saved},
        sync_scope=None,
    )

    assert verdict.changed is True
    assert verdict.reason == "probe_error:SourceRootMissing"
    assert not verdict.token


def test_test_reports_root_that_is_a_file_fails_the_run(tmp_path: Path) -> None:
    from okg.substrate.errors import SourceRootMissing
    from okg.substrate.library.sources.test_reports import TestReportSource

    not_a_dir = _write(tmp_path / "root.txt", "x")

    with pytest.raises(SourceRootMissing):
        TestReportSource(repo="demo", repo_root=not_a_dir).run("run-file-root")


def test_append_only_test_reports_decline_probe_short_circuit_authority(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.test_reports import TestReportSource

    repo = tmp_path / "repo"
    repo.mkdir()
    source = TestReportSource(
        repo="demo",
        repo_root=repo,
        append_only=True,
    )

    run = source.run("append-only", mode="scope_complete")

    assert source.probe_short_circuit_safe is False
    assert run.completed_scope is False
    assert run.record_set is None


def test_test_report_facts_ignore_filesystem_timestamp_churn(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.test_reports import TestReportSource

    repo = tmp_path / "repo"
    junit = _write(
        repo / "reports" / "junit.xml",
        """<testsuite name="pytest">
  <testcase classname="tests.test_demo" name="test_ok"/>
</testsuite>
""",
    )
    source = TestReportSource(
        repo="demo",
        repo_root=repo,
        artifact_paths=[junit],
    )

    first_run = source.run("acquisition-a", mode="scope_complete")
    before = list(first_run.facts)
    assert first_run.record_set is not None
    assert "reports/junit.xml" in first_run.record_set.records
    assert first_run.applied_token_scope == "global"
    assert first_run.applied_token_authority == "exact"
    assert first_run.applied_raw_token == source.change_probe.build_token()
    os.utime(junit, (2_000_000_000, 2_000_000_000))
    second_run = source.run("acquisition-b", mode="scope_complete")
    after = list(second_run.facts)
    assert second_run.applied_raw_token == source.change_probe.build_token()

    assert after == before
    assert all("mtime" not in fact.source_revision for fact in after)
    report = next(
        fact
        for fact in after
        if isinstance(fact, NodeFact) and fact.subtype == "test_report"
    )
    assert "observed_at" not in report.attrs
    assert "created_at" not in report.attrs


def test_test_report_source_emits_junit_failure_and_coverage(tmp_path: Path) -> None:
    from okg.substrate.library.sources.test_reports import TestReportSource

    repo = tmp_path / "repo"
    _write(repo / "src" / "app.py", "def fail():\n    raise RuntimeError('boom')\n")
    junit = _write(
        repo / "reports" / "junit.xml",
        f"""<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="pytest" tests="2" failures="1" errors="0" skipped="0" time="0.05">
    <testcase classname="tests.test_app" name="test_passes" time="0.01" file="tests/test_app.py"/>
    <testcase classname="tests.test_app" name="test_fails" time="0.04" file="tests/test_app.py">
      <properties>
        <property name="openspec_criterion" value="crit:demo-change:0"/>
      </properties>
      <failure message="boom">Traceback (most recent call last):
  File "{repo / 'src' / 'app.py'}", line 2, in fail
    raise RuntimeError('boom')
RuntimeError: boom</failure>
    </testcase>
  </testsuite>
</testsuites>
""",
    )
    coverage = _write(
        repo / "reports" / "coverage.xml",
        """<?xml version="1.0" ?>
<coverage line-rate="0.5" branch-rate="0.25">
  <packages>
    <package name="src">
      <classes>
        <class name="app.py" filename="src/app.py">
          <lines>
            <line number="1" hits="1"/>
            <line number="2" hits="0"/>
          </lines>
        </class>
      </classes>
    </package>
  </packages>
</coverage>
""",
    )

    source = TestReportSource(
        repo="demo",
        repo_root=repo,
        artifact_paths=[junit],
        coverage_paths=[coverage],
        suite_name="pytest",
        sha="deadbeef",
    )
    facts = list(source.run("run-1", mode="scope_complete").facts)

    nodes = [f for f in facts if isinstance(f, NodeFact)]
    edges = [f for f in facts if isinstance(f, EdgeFact)]
    subtypes = {f.subtype for f in nodes}

    assert {
        "test_report",
        "test_run",
        "test_case",
        "runtime_event",
        "stack_frame",
        "coverage_report",
    } <= subtypes

    reports = [f for f in nodes if f.subtype == "test_report"]
    assert len(reports) == 1
    assert reports[0].attrs["format"] == "junit_xml"
    assert reports[0].attrs["artifact_hash"]
    assert "observed_at" not in reports[0].attrs
    assert "mtime" not in reports[0].attrs
    assert reports[0].attrs["sha"] == "deadbeef"
    failed_case = [
        f for f in nodes
        if f.subtype == "test_case" and f.attrs["status"] == "failed"
    ][0]
    assert failed_case.attrs["criteria_refs"] == ["crit:demo-change:0"]
    assert failed_case.attrs["properties"]["openspec_criterion"] == "crit:demo-change:0"
    coverage_reports = [f for f in nodes if f.subtype == "coverage_report"]
    assert len(coverage_reports) == 1
    assert coverage_reports[0].attrs["line_rate"] == 0.5
    assert coverage_reports[0].attrs["missing_lines"] == {"src/app.py": [2]}
    assert coverage_reports[0].attrs["artifact_hash"]
    assert "observed_at" not in coverage_reports[0].attrs
    assert "mtime" not in coverage_reports[0].attrs
    assert coverage_reports[0].attrs["sha"] == "deadbeef"

    runtime_events = [f for f in nodes if f.subtype == "runtime_event"]
    assert len(runtime_events) == 1
    assert "boom" in runtime_events[0].attrs["message"]
    frames = [f for f in nodes if f.subtype == "stack_frame"]
    assert len(frames) == 1
    assert frames[0].attrs["path"] == "src/app.py"
    assert frames[0].attrs["line"] == 2

    edge_patterns = {(e.src.split(":", 1)[0], e.edge_type, e.dst.split(":", 1)[0]) for e in edges}
    assert ("test-report", "contains", "test-run") in edge_patterns
    assert ("test-report", "contains", "test-case") in edge_patterns
    assert ("test-report", "contains", "runtime-event") in edge_patterns
    assert ("runtime-event", "contains", "stack-frame") in edge_patterns
    assert any(e.edge_type == "points_to" and e.dst == "file:demo:src/app.py" for e in edges)
    assert any(e.edge_type == "references" and e.src.startswith("coverage-report:") for e in edges)


def test_test_report_source_emits_coverage_json(tmp_path: Path) -> None:
    from okg.substrate.library.sources.test_reports import TestReportSource

    repo = tmp_path / "repo"
    coverage = _write(
        repo / "reports" / "coverage.json",
        json.dumps({
            "meta": {"version": "7.0"},
            "totals": {
                "covered_lines": 3,
                "num_statements": 4,
                "percent_covered": 75.0,
            },
            "files": {
                "src/app.py": {
                    "executed_lines": [1, 2, 4],
                    "missing_lines": [3],
                },
            },
        }),
    )

    source = TestReportSource(
        repo="demo",
        repo_root=repo,
        coverage_paths=[coverage],
    )
    facts = list(source.run("run-1").facts)

    coverage_reports = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "coverage_report"
    ]
    assert len(coverage_reports) == 1
    assert coverage_reports[0].attrs["format"] == "coverage_json"
    assert coverage_reports[0].attrs["line_rate"] == 0.75
    assert any(
        isinstance(f, EdgeFact)
        and f.src == coverage_reports[0].node_id
        and f.dst == "file:demo:src/app.py"
        and f.attrs == {
            "covered_line_count": 3,
            "missing_line_count": 1,
            "has_missing_lines": True,
        }
        for f in facts
    )


def test_coverage_reference_edge_attrs_are_bounded(tmp_path: Path) -> None:
    from okg.substrate.library.sources.test_reports import TestReportSource

    repo = tmp_path / "repo"
    coverage = _write_coverage_json(
        repo / "reports" / "coverage.json",
        covered=8_000,
        statements=12_000,
    )

    source = TestReportSource(
        repo="demo",
        repo_root=repo,
        coverage_paths=[coverage],
    )
    facts = list(source.run("run-1").facts)
    edge = next(
        f
        for f in facts
        if isinstance(f, EdgeFact)
        and f.edge_type == "references"
        and f.src.startswith("coverage-report:")
    )

    assert edge.attrs == {
        "covered_line_count": 8000,
        "missing_line_count": 4000,
        "has_missing_lines": True,
    }


def test_test_report_source_scopes_to_configured_artifact_path(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.test_reports import TestReportSource

    repo = tmp_path / "repo"
    junit_a = _write_junit(repo / "reports" / "junit-a.xml", name="a")
    junit_b = _write_junit(repo / "reports" / "junit-b.xml", name="b")
    coverage_a = _write_coverage_json(
        repo / "reports" / "coverage-a.json",
        covered=1,
        statements=2,
    )
    coverage_b = _write_coverage_json(
        repo / "reports" / "coverage-b.json",
        covered=3,
        statements=4,
    )
    source = TestReportSource(
        repo="demo",
        repo_root=repo,
        artifact_paths=[junit_a, junit_b],
        coverage_paths=[coverage_a, coverage_b],
        sha="deadbeef",
    )

    junit_run = source.run(
        "run-junit",
        mode="scope_incomplete",
        sync_scope={
            "scope_keys": [
                junit_b.relative_to(repo).as_posix(),
                "../escape.xml",
            ],
        },
    )
    junit_nodes = [
        f for f in junit_run.facts
        if isinstance(f, NodeFact) and f.subtype == "test_report"
    ]
    assert junit_run.completed_scope is False
    assert [node.attrs["path"] for node in junit_nodes] == [
        "reports/junit-b.xml",
    ]

    coverage_run = source.run(
        "run-coverage",
        mode="scope_incomplete",
        sync_scope={"scope_keys": [coverage_b.relative_to(repo).as_posix()]},
    )
    coverage_nodes = [
        f for f in coverage_run.facts
        if isinstance(f, NodeFact) and f.subtype == "coverage_report"
    ]
    assert coverage_run.completed_scope is False
    assert [node.attrs["path"] for node in coverage_nodes] == [
        "reports/coverage-b.json",
    ]


def test_test_report_source_ignores_unconfigured_scoped_artifact(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.test_reports import TestReportSource

    repo = tmp_path / "repo"
    configured = _write_junit(repo / "reports" / "junit-a.xml", name="a")
    unconfigured = _write_junit(repo / "other" / "junit-b.xml", name="b")
    source = TestReportSource(
        repo="demo",
        repo_root=repo,
        artifact_paths=[configured],
        sha="deadbeef",
    )

    run = source.run(
        "run-unconfigured",
        mode="scope_incomplete",
        sync_scope={"scope_keys": [unconfigured.relative_to(repo).as_posix()]},
    )

    assert list(run.facts) == []
    assert run.completed_scope is False


_SHA_A = "0123456789abcdef0123456789abcdef01234567"
_SHA_B = "fedcba9876543210fedcba9876543210fedcba98"


def test_sha_from_path_attributes_per_run_sha(tmp_path: Path) -> None:
    from okg.substrate.library.sources.test_reports import TestReportSource

    repo = tmp_path / "repo"
    junit_a = _write_junit(
        repo / "mirror" / "runs" / _SHA_A / "fast-linux" / "junit.xml", name="a",
    )
    junit_b = _write_junit(
        repo / "mirror" / "runs" / _SHA_B / "smoke" / "junit.xml", name="b",
    )
    source = TestReportSource(
        repo="demo",
        repo_root=repo,
        artifact_paths=[junit_a, junit_b],
        infer_sha=False,
        sha_from_path=True,
    )
    facts = list(source.run("run-1", mode="scope_complete").facts)

    run_sha = {
        f.attrs["path"]: f.attrs["sha"]
        for f in facts
        if isinstance(f, NodeFact) and f.subtype == "test_report"
    }
    assert run_sha[f"mirror/runs/{_SHA_A}/fast-linux/junit.xml"] == _SHA_A
    assert run_sha[f"mirror/runs/{_SHA_B}/smoke/junit.xml"] == _SHA_B

    affects = {
        e.dst
        for e in facts
        if isinstance(e, EdgeFact) and e.edge_type == "affects"
    }
    assert f"commit:demo:{_SHA_A}" in affects
    assert f"commit:demo:{_SHA_B}" in affects

    # idempotent: re-running over the same files yields identical node ids
    again = {
        f.node_id
        for f in source.run("run-2", mode="scope_complete").facts
        if isinstance(f, NodeFact)
    }
    first = {f.node_id for f in facts if isinstance(f, NodeFact)}
    assert first == again


def test_github_actions_artifact_manifest_projects_exact_lineage(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.test_reports import TestReportSource

    repo = tmp_path / "repo"
    lane = repo / "mirror" / "runs" / _SHA_A / "fast-linux"
    _write(
        lane / ".okg-ci-artifact.json",
        json.dumps({
            "schema": "okg-ci.artifact-manifest.v1",
            "repo": "owner/repo",
            "sha": _SHA_A,
            "run_id": 987654321,
            "run_attempt": 2,
            "workflow_name": "CI",
            "workflow_id": 1234,
            "head_branch": "feature/exact-lineage",
            "event": "pull_request",
            "created_at": "2026-06-17T10:00:00Z",
            "updated_at": "2026-06-17T10:05:00Z",
            "artifact_name": "ci-fast-linux",
            "artifact_id": 4567,
            "artifact_node_id": "ci-artifact:github:owner/repo:4567",
            "lane": "fast-linux",
            "content_kinds": ["junit", "coverage"],
            "lineage_status": "exact",
        }),
    )
    _write_junit(lane / "junit" / "junit.xml", name="lineage")
    _write_coverage_json(
        lane / "coverage" / "coverage.json",
        covered=3,
        statements=4,
    )

    source = TestReportSource(
        repo="demo",
        repo_root=repo,
        report_dirs=[repo / "mirror" / "runs"],
        coverage_dirs=[repo / "mirror" / "runs"],
        infer_sha=False,
        sha_from_path=True,
    )
    facts = list(source.run("run-lineage", mode="scope_complete").facts)

    reports = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "test_report"
    ]
    assert len(reports) == 1
    report = reports[0]
    assert report.attrs["sha"] == _SHA_A
    assert report.attrs["lineage_status"] == "exact"
    assert report.attrs["run_id"] == 987654321
    assert report.attrs["workflow_run_id"] == 987654321
    assert report.attrs["run_attempt"] == 2
    assert report.attrs["artifact_id"] == 4567
    assert report.attrs["artifact_node_id"] == "ci-artifact:github:owner/repo:4567"
    assert report.attrs["lane"] == "fast-linux"

    test_runs = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "test_run"
    ]
    assert len(test_runs) == 1
    assert test_runs[0].attrs["run_id"].startswith("test-run:")
    # The three attributes the mirror's ingested index reads back as identity.
    # A re-run keeps the same workflow_run_id, so without run_attempt on the
    # node the mirror would read attempt 2's evidence as already ingested and
    # discard it.
    assert test_runs[0].attrs["workflow_run_id"] == 987654321
    assert test_runs[0].attrs["run_attempt"] == 2
    assert test_runs[0].attrs["lane"] == "fast-linux"
    assert test_runs[0].attrs["artifact_id"] == 4567

    test_cases = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "test_case"
    ]
    assert test_cases[0].attrs["workflow_run_id"] == 987654321
    assert test_cases[0].attrs["artifact_name"] == "ci-fast-linux"

    coverage_reports = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "coverage_report"
    ]
    assert len(coverage_reports) == 1
    assert coverage_reports[0].attrs["artifact_id"] == 4567

    artifact_nodes = [
        f for f in facts
        if isinstance(f, NodeFact)
        and f.subtype == "ci_artifact"
        and f.node_id == "ci-artifact:github:owner/repo:4567"
    ]
    assert len(artifact_nodes) == 1
    assert artifact_nodes[0].attrs["repo"] == "owner/repo"
    assert artifact_nodes[0].attrs["run_id"] == 987654321
    assert artifact_nodes[0].attrs["run_attempt"] == 2
    assert artifact_nodes[0].attrs["name"] == "ci-fast-linux"
    assert artifact_nodes[0].attrs["content_kinds"] == ["junit", "coverage"]

    artifact_edges = [
        f for f in facts
        if isinstance(f, EdgeFact)
        and f.edge_type == "references"
        and f.dst == "ci-artifact:github:owner/repo:4567"
    ]
    assert {edge.src.split(":", 1)[0] for edge in artifact_edges} == {
        "test-report",
        "coverage-report",
    }
    assert all(edge.attrs["lineage_status"] == "exact" for edge in artifact_edges)

    progress = json.loads(
        source.progress_after_run()["diagnostics.test_reports.last_run"]
    )
    assert progress["artifacts_seen"] == 2
    assert progress["artifacts_parsed"] == 2
    assert progress["test_facts_appended"] >= 3
    assert progress["coverage_facts_appended"] == 1
    assert progress["latest_workflow_observed_at"] == "2026-06-17T10:05:00Z"
    assert progress["lineage_status_counts"] == {"exact": 2}


def test_run_cache_report_without_manifest_is_explicitly_degraded(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.test_reports import TestReportSource

    repo = tmp_path / "repo"
    junit = _write_junit(
        repo / "mirror" / "runs" / _SHA_A / "smoke" / "junit" / "junit.xml",
        name="missing_manifest",
    )
    source = TestReportSource(
        repo="demo",
        repo_root=repo,
        artifact_paths=[junit],
        infer_sha=False,
        sha_from_path=True,
    )

    facts = list(source.run("run-missing-manifest").facts)
    report = next(
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "test_report"
    )

    assert report.attrs["sha"] == _SHA_A
    assert report.attrs["lineage_status"] == "missing_artifact_manifest"
    assert report.attrs["lane"] == "smoke"
    assert not any(
        isinstance(f, EdgeFact)
        and f.edge_type == "references"
        and f.dst.startswith("ci-artifact:")
        for f in facts
    )


def test_artifact_publication_gap_manifest_is_ingested_as_degraded_lineage(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.test_reports import TestReportSource

    repo = tmp_path / "repo"
    lane = repo / "mirror" / "runs" / _SHA_A / "artifact-publication"
    _write(
        lane / ".okg-ci-artifact.json",
        json.dumps({
            "schema": "okg-ci.artifact-manifest.v1",
            "repo": "owner/repo",
            "sha": _SHA_A,
            "run_id": 24680,
            "run_attempt": 1,
            "workflow_name": "Tests",
            "head_branch": "dev",
            "event": "push",
            "created_at": "2026-07-06T16:30:00Z",
            "updated_at": "2026-07-06T16:35:00Z",
            "artifact_name": "artifact-publication-degraded",
            "artifact_node_id": (
                "ci-artifact:github:owner/repo:run-24680-artifact-publication-degraded"
            ),
            "lane": "artifact-publication",
            "content_kinds": ["evidence_gap"],
            "lineage_status": "artifact_publication_degraded",
            "artifact_publication_status": "degraded",
            "artifact_publication_reason": "Artifact storage quota has been hit.",
        }),
    )
    _write_junit(lane / "junit" / "artifact-publication.xml", name="artifact_gap")

    source = TestReportSource(
        repo="demo",
        repo_root=repo,
        report_dirs=[repo / "mirror" / "runs"],
        infer_sha=False,
        sha_from_path=True,
    )
    facts = list(source.run("run-artifact-gap").facts)

    report = next(
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "test_report"
    )
    assert report.attrs["lineage_status"] == "artifact_publication_degraded"
    assert report.attrs["artifact_publication_status"] == "degraded"
    assert report.attrs["artifact_publication_reason"] == (
        "Artifact storage quota has been hit."
    )

    artifact = next(
        f for f in facts
        if isinstance(f, NodeFact)
        and f.subtype == "ci_artifact"
        and f.node_id.endswith("artifact-publication-degraded")
    )
    assert artifact.attrs["lineage_status"] == "artifact_publication_degraded"
    assert artifact.attrs["content_kinds"] == ["evidence_gap"]

    edge = next(
        f for f in facts
        if isinstance(f, EdgeFact)
        and f.edge_type == "references"
        and f.dst == artifact.node_id
    )
    assert edge.attrs["lineage_status"] == "artifact_publication_degraded"


def test_sha_from_path_falls_back_without_segment(tmp_path: Path) -> None:
    from okg.substrate.library.sources.test_reports import TestReportSource

    repo = tmp_path / "repo"
    junit = _write_junit(repo / "reports" / "junit.xml", name="x")
    source = TestReportSource(
        repo="demo",
        repo_root=repo,
        artifact_paths=[junit],
        infer_sha=False,
        sha_from_path=True,
        sha="fallbackcafe",
    )
    reports = [
        f for f in source.run("r").facts
        if isinstance(f, NodeFact) and f.subtype == "test_report"
    ]
    assert reports[0].attrs["sha"] == "fallbackcafe"


def test_sha_from_path_disabled_is_backwards_compatible(tmp_path: Path) -> None:
    from okg.substrate.library.sources.test_reports import TestReportSource

    repo = tmp_path / "repo"
    junit = _write_junit(
        repo / "mirror" / "runs" / _SHA_A / "fast-linux" / "junit.xml", name="a",
    )
    source = TestReportSource(
        repo="demo",
        repo_root=repo,
        artifact_paths=[junit],
        sha="deadbeef",
    )
    reports = [
        f for f in source.run("r").facts
        if isinstance(f, NodeFact) and f.subtype == "test_report"
    ]
    # default-off (sha_from_path=False): uses self.sha, NOT the path sha
    assert reports[0].attrs["sha"] == "deadbeef"


def _write_junit(path: Path, *, name: str) -> Path:
    return _write(
        path,
        f"""<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="pytest" tests="1" failures="0" errors="0" skipped="0" time="0.01">
    <testcase classname="tests.{name}" name="test_{name}" time="0.01" file="tests/test_{name}.py"/>
  </testsuite>
</testsuites>
""",
    )


def _write_coverage_json(
    path: Path,
    *,
    covered: int,
    statements: int,
) -> Path:
    return _write(
        path,
        json.dumps({
            "totals": {
                "covered_lines": covered,
                "num_statements": statements,
                "percent_covered": (covered / statements) * 100,
            },
            "files": {
                "src/app.py": {
                    "executed_lines": list(range(1, covered + 1)),
                    "missing_lines": list(range(covered + 1, statements + 1)),
                },
            },
        }),
    )
