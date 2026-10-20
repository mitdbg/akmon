from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Iterable

import psycopg
import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytest.importorskip("defusedxml")
pytestmark = pytest.mark.postgres


def test_test_report_source_scoped_sync_publishes_target_artifacts(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.test_reports import TestReportSource
    from okg.substrate.publisher import Publisher

    repo = tmp_path / "repo"
    _write(repo / "src" / "app.py", "def app():\n    return 1\n")
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

    def _source() -> TestReportSource:
        return TestReportSource(
            repo="demo",
            repo_root=repo,
            artifact_paths=[junit_a, junit_b],
            coverage_paths=[coverage_a, coverage_b],
            sha="deadbeef",
        )

    _seed_catalog_and_external_nodes(clean_runtime, _source().run("preview").facts)

    runner = SourceRunner(clean_runtime, source_name="test.test-reports")
    baseline = runner.run(_source(), mode="scope_complete")
    assert baseline.completed_scope is True
    first_publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert first_publish["status"] == "published", first_publish

    _write_junit(junit_b, name="b2")
    scoped_junit = runner.run(
        _source(),
        mode="scope_incomplete",
        sync_scope={"scope_keys": [junit_b.relative_to(repo).as_posix()]},
    )
    assert scoped_junit.completed_scope is False
    second_publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert second_publish["status"] == "published", second_publish
    assert _node_record_paths_for_run(
        clean_runtime,
        run_id=scoped_junit.run_id,
        source="test.test-reports",
        subtype="test_report",
    ) == ["reports/junit-b.xml"]
    assert _node_record_paths_for_run(
        clean_runtime,
        run_id=scoped_junit.run_id,
        source="test.test-reports",
        subtype="coverage_report",
    ) == []

    _write_coverage_json(coverage_b, covered=4, statements=4)
    scoped_coverage = runner.run(
        _source(),
        mode="scope_incomplete",
        sync_scope={"scope_keys": [coverage_b.relative_to(repo).as_posix()]},
    )
    assert scoped_coverage.completed_scope is False
    third_publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert third_publish["status"] == "published", third_publish
    assert _node_record_paths_for_run(
        clean_runtime,
        run_id=scoped_coverage.run_id,
        source="test.test-reports",
        subtype="coverage_report",
    ) == ["reports/coverage-b.json"]
    assert _latest_generation_status(clean_runtime) == "published"
    assert _live_node_count(clean_runtime, "test_report") >= 2
    assert _live_node_count(clean_runtime, "coverage_report") >= 2
    assert _queued_jobs_by_kind(clean_runtime).get("ingest", 0) == 0
    assert _dlq_count(clean_runtime) == 0


def test_junit_xml_source_scoped_sync_publishes_target_report(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.junit_xml import JUnitXmlSource
    from okg.substrate.publisher import Publisher

    drop = tmp_path / "junit"
    junit_a = _write_junit(drop / "alpha.xml", name="alpha")
    junit_b = _write_junit(drop / "beta.xml", name="beta")

    def _source() -> JUnitXmlSource:
        return JUnitXmlSource(
            drop_dir=drop,
            repo_root=tmp_path,
            repo="demo",
            sha="deadbeef",
        )

    _seed_catalog_and_external_nodes(clean_runtime, _source().run("preview").facts)

    runner = SourceRunner(clean_runtime, source_name="test.junit-xml")
    baseline = runner.run(_source(), mode="scope_complete")
    assert baseline.completed_scope is True
    first_publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert first_publish["status"] == "published", first_publish

    _write_junit(junit_b, name="beta2")
    scoped = runner.run(
        _source(),
        mode="scope_incomplete",
        sync_scope={"scope_keys": [junit_b.relative_to(drop).as_posix()]},
    )
    assert scoped.completed_scope is False
    second_publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert second_publish["status"] == "published", second_publish

    assert _node_record_paths_for_run(
        clean_runtime,
        run_id=scoped.run_id,
        source="test.junit-xml",
        subtype="test_report",
    ) == ["junit/beta.xml"]
    assert _latest_generation_status(clean_runtime) == "published"
    assert _live_node_count(clean_runtime, "test_report") >= 2
    assert _queued_jobs_by_kind(clean_runtime).get("ingest", 0) == 0
    assert _dlq_count(clean_runtime) == 0
    assert junit_a.is_file()


def test_security_analysis_source_scoped_sync_publishes_target_artifacts(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.security_analysis import (
        SecurityAnalysisSource,
    )
    from okg.substrate.publisher import Publisher

    repo = tmp_path / "repo"
    _write(repo / "src" / "app.py", "def app():\n    return 1\n")
    semgrep_a = _write_semgrep(
        repo / "reports" / "semgrep-a.json",
        fingerprint="sg-a",
    )
    semgrep_b = _write_semgrep(
        repo / "reports" / "semgrep-b.json",
        fingerprint="sg-b",
    )
    gitleaks_a = _write_gitleaks(
        repo / "reports" / "gitleaks-a.json",
        fingerprint="gl-a",
    )
    gitleaks_b = _write_gitleaks(
        repo / "reports" / "gitleaks-b.json",
        fingerprint="gl-b",
    )

    def _source() -> SecurityAnalysisSource:
        return SecurityAnalysisSource(
            repo="demo",
            repo_root=repo,
            artifact_paths=[semgrep_a, semgrep_b, gitleaks_a, gitleaks_b],
        )

    _seed_catalog_and_external_nodes(clean_runtime, _source().run("preview").facts)

    runner = SourceRunner(clean_runtime, source_name="test.security-analysis")
    baseline = runner.run(_source(), mode="scope_complete")
    assert baseline.completed_scope is True
    first_publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert first_publish["status"] == "published", first_publish

    _write_semgrep(semgrep_b, fingerprint="sg-b2")
    scoped_security = runner.run(
        _source(),
        mode="scope_incomplete",
        sync_scope={"scope_keys": [semgrep_b.relative_to(repo).as_posix()]},
    )
    assert scoped_security.completed_scope is False
    second_publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert second_publish["status"] == "published", second_publish
    assert _node_record_values_for_run(
        clean_runtime,
        run_id=scoped_security.run_id,
        source="test.security-analysis",
        subtype="security_finding",
        field="artifact",
    ) == ["reports/semgrep-b.json"]
    assert _node_record_values_for_run(
        clean_runtime,
        run_id=scoped_security.run_id,
        source="test.security-analysis",
        subtype="secret_finding",
        field="artifact",
    ) == []

    _write_gitleaks(gitleaks_b, fingerprint="gl-b2")
    scoped_secret = runner.run(
        _source(),
        mode="scope_incomplete",
        sync_scope={"scope_keys": [gitleaks_b.relative_to(repo).as_posix()]},
    )
    assert scoped_secret.completed_scope is False
    third_publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert third_publish["status"] == "published", third_publish
    assert _node_record_values_for_run(
        clean_runtime,
        run_id=scoped_secret.run_id,
        source="test.security-analysis",
        subtype="secret_finding",
        field="artifact",
    ) == ["reports/gitleaks-b.json"]
    assert _latest_generation_status(clean_runtime) == "published"
    assert _live_node_count(clean_runtime, "security_finding") >= 2
    assert _live_node_count(clean_runtime, "secret_finding") >= 2
    assert _queued_jobs_by_kind(clean_runtime).get("ingest", 0) == 0
    assert _dlq_count(clean_runtime) == 0


def test_profile_measurement_source_scoped_sync_publishes_target_artifacts(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.profile_measurements import (
        ProfileMeasurementSource,
    )
    from okg.substrate.publisher import Publisher

    repo = tmp_path / "repo"
    profile_a = _write_profile(
        repo / "profiles" / "okg-workspace" / "run-a" / "profile.json",
        run_id="run-a",
        duration_ms=700,
    )
    profile_b = _write_profile(
        repo / "profiles" / "okg-workspace" / "run-b" / "profile.json",
        run_id="run-b",
        duration_ms=800,
    )
    benchmark_a = _write_benchmark(
        repo / "reports" / "benchmarks-a.json",
        profile_run_id="profile-run:okg-workspace:run-a",
        value=700.0,
    )
    benchmark_b = _write_benchmark(
        repo / "reports" / "benchmarks-b.json",
        profile_run_id="profile-run:okg-workspace:run-b",
        value=800.0,
    )

    def _source() -> ProfileMeasurementSource:
        return ProfileMeasurementSource(
            deployment="okg-workspace",
            repo="demo",
            repo_root=repo,
            artifact_globs=["profiles/okg-workspace/*/profile.json"],
            benchmark_globs=["reports/**/*benchmark*.json"],
            performance_budgets=[{
                "budget_id": "performance-budget:explicit:publish-cycle",
                "scope": "publish_cycle",
                "metric": "duration_ms",
                "threshold": 1000.0,
                "unit": "ms",
                "operator": "<=",
            }],
        )

    _seed_catalog_and_external_nodes(clean_runtime, _source().run("preview").facts)

    runner = SourceRunner(clean_runtime, source_name="test.profile-measurements")
    baseline = runner.run(_source(), mode="scope_complete")
    assert baseline.completed_scope is True
    first_publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert first_publish["status"] == "published", first_publish

    _write_profile(profile_b, run_id="run-b", duration_ms=900)
    scoped_profile = runner.run(
        _source(),
        mode="scope_incomplete",
        sync_scope={"scope_keys": [profile_b.relative_to(repo).as_posix()]},
    )
    assert scoped_profile.completed_scope is False
    second_publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert second_publish["status"] == "published", second_publish
    assert _node_record_values_for_run(
        clean_runtime,
        run_id=scoped_profile.run_id,
        source="test.profile-measurements",
        subtype="profile_run",
        field="artifact",
    ) == ["profiles/okg-workspace/run-b/profile.json"]
    assert _node_record_values_for_run(
        clean_runtime,
        run_id=scoped_profile.run_id,
        source="test.profile-measurements",
        subtype="benchmark_measurement",
        field="artifact",
    ) == []
    assert _live_node_count(clean_runtime, "performance_budget") == 1

    _write_benchmark(
        benchmark_b,
        profile_run_id="profile-run:okg-workspace:run-b",
        value=850.0,
    )
    scoped_benchmark = runner.run(
        _source(),
        mode="scope_incomplete",
        sync_scope={"scope_keys": [benchmark_b.relative_to(repo).as_posix()]},
    )
    assert scoped_benchmark.completed_scope is False
    third_publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert third_publish["status"] == "published", third_publish
    assert _node_record_values_for_run(
        clean_runtime,
        run_id=scoped_benchmark.run_id,
        source="test.profile-measurements",
        subtype="benchmark_measurement",
        field="artifact",
    ) == ["reports/benchmarks-b.json"]
    assert _node_record_values_for_run(
        clean_runtime,
        run_id=scoped_benchmark.run_id,
        source="test.profile-measurements",
        subtype="profile_run",
        field="artifact",
    ) == []
    assert _node_attr_values_for_run(
        clean_runtime,
        run_id=scoped_benchmark.run_id,
        source="test.profile-measurements",
        subtype="benchmark_measurement",
        field="threshold",
    ) == ["1000.0"]
    assert _latest_generation_status(clean_runtime) == "published"
    assert _live_node_count(clean_runtime, "profile_run") >= 2
    assert _live_node_count(clean_runtime, "benchmark_measurement") >= 2
    assert _live_node_count(clean_runtime, "performance_budget") == 1
    assert _queued_jobs_by_kind(clean_runtime).get("ingest", 0) == 0
    assert _dlq_count(clean_runtime) == 0
    assert profile_a.is_file()
    assert benchmark_a.is_file()


def test_self_play_report_source_scoped_sync_publishes_target_report(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.self_play import SelfPlayReportSource
    from okg.substrate.publisher import Publisher

    repo = tmp_path / "repo"
    report_a = _write_self_play_report(
        repo / "deployments" / "okg-workspace" / "self_play" / "reports" / "a.json",
        scenario_id="scenario-a",
        generation_id=9,
        score=0.5,
    )
    report_b = _write_self_play_report(
        repo / "deployments" / "okg-workspace" / "self_play" / "reports" / "b.json",
        scenario_id="scenario-b",
        generation_id=10,
        score=0.6,
    )

    def _source() -> SelfPlayReportSource:
        return SelfPlayReportSource(
            repo="demo",
            repo_root=repo,
            report_globs=["deployments/okg-workspace/self_play/reports/*.json"],
            deployment="okg-workspace",
        )

    _seed_catalog_and_external_nodes(clean_runtime, _source().run("preview").facts)

    runner = SourceRunner(clean_runtime, source_name="test.self-play-reports")
    baseline = runner.run(_source(), mode="scope_complete")
    assert baseline.completed_scope is True
    first_publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert first_publish["status"] == "published", first_publish

    _write_self_play_report(
        report_b,
        scenario_id="scenario-b",
        generation_id=10,
        score=0.4,
    )
    scoped = runner.run(
        _source(),
        mode="scope_incomplete",
        sync_scope={"scope_keys": [report_b.relative_to(repo).as_posix()]},
    )
    assert scoped.completed_scope is False
    second_publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert second_publish["status"] == "published", second_publish
    assert _node_record_values_for_run(
        clean_runtime,
        run_id=scoped.run_id,
        source="test.self-play-reports",
        subtype="self_play_run",
        field="path",
    ) == ["deployments/okg-workspace/self_play/reports/b.json"]
    assert _node_record_values_for_run(
        clean_runtime,
        run_id=scoped.run_id,
        source="test.self-play-reports",
        subtype="self_play_scenario",
        field="path",
    ) == ["deployments/okg-workspace/self_play/reports/b.json"]
    assert _node_record_values_for_run(
        clean_runtime,
        run_id=scoped.run_id,
        source="test.self-play-reports",
        subtype="self_play_finding",
        field="path",
    ) == ["deployments/okg-workspace/self_play/reports/b.json"]
    assert _latest_generation_status(clean_runtime) == "published"
    assert _live_node_count(clean_runtime, "self_play_run") >= 2
    assert _live_node_count(clean_runtime, "self_play_scenario") >= 2
    assert _live_node_count(clean_runtime, "self_play_finding") >= 2
    assert _queued_jobs_by_kind(clean_runtime).get("ingest", 0) == 0
    assert _dlq_count(clean_runtime) == 0
    assert report_a.is_file()


def _seed_catalog_and_external_nodes(
    dsn: str,
    facts: Iterable[NodeFact | EdgeFact],
) -> None:
    materialized = list(facts)
    node_subtypes = {
        fact.node_id: fact.subtype
        for fact in materialized
        if isinstance(fact, NodeFact)
    }
    stub_nodes: dict[str, str] = {}
    for fact in materialized:
        if not isinstance(fact, EdgeFact):
            continue
        for node_id in (fact.src, fact.dst):
            if node_id in node_subtypes:
                continue
            subtype = _subtype_for_node_id(node_id)
            assert subtype is not None, node_id
            node_subtypes[node_id] = subtype
            stub_nodes[node_id] = subtype

    narrowings = {
        (node_subtypes[fact.src], fact.edge_type, node_subtypes[fact.dst])
        for fact in materialized
        if isinstance(fact, EdgeFact)
        and fact.src in node_subtypes
        and fact.dst in node_subtypes
    }
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
        for subtype in sorted(set(node_subtypes.values())):
            cur.execute(
                "INSERT INTO okg.subtypes (name, catalog_version_id) "
                "VALUES (%s, %s)",
                (subtype, cv),
            )
            cur.execute(
                """
                INSERT INTO okg.subtype_ancestors
                  (catalog_version_id, subtype_name, ancestor_name, depth)
                VALUES (%s, %s, %s, 0)
                ON CONFLICT DO NOTHING
                """,
                (cv, subtype, subtype),
            )
        for parent, edge_type, child in sorted(narrowings):
            cur.execute(
                "INSERT INTO okg.narrowings "
                "(parent_subtype, edge_type, child_subtype, "
                " catalog_version_id) "
                "VALUES (%s, %s, %s, %s)",
                (parent, edge_type, child, cv),
            )
        for node_id, subtype in sorted(stub_nodes.items()):
            stub_run_id = str(uuid.uuid4())
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES (%s, %s, %s::jsonb, 'test.external-stubs',
                        %s::jsonb, %s::jsonb, now(), %s, %s, 'I')
                """,
                (
                    node_id,
                    subtype,
                    json.dumps({"text": node_id}),
                    json.dumps({"node_id": node_id}),
                    json.dumps({"stub": True}),
                    f"stub:{node_id}",
                    stub_run_id,
                ),
            )


def _subtype_for_node_id(node_id: str) -> str | None:
    prefix_map = {
        "commit:": "git_commit",
        "coverage-report:": "coverage_report",
        "file:": "source_file",
        "runtime-event:": "runtime_event",
        "stack-frame:": "stack_frame",
        "test-case:": "test_case",
        "test-report:": "test_report",
        "test-run:": "test_run",
    }
    for prefix, subtype in prefix_map.items():
        if node_id.startswith(prefix):
            return subtype
    return None


def _live_graph_for_run(dsn: str):
    """Check out the latest published `test` generation's live graph.

    Chronos cutover (6bb637f0a + write_fact_log default False): the
    canonical node graph is the Chronos interval-backed live table
    reached via `checkout_generation`, not the raw `okg.node_facts`
    fact log (now empty) nor the empty top-level `okg.graph_nodes`
    table. Per-run assertions read through the published generation,
    mirroring `_live_node_count`.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    latest = store.list_generations(
        deployment_name="test",
        status="published",
    )[-1]
    return store, store.checkout_generation(latest.checkpoint_id)


def _node_record_paths_for_run(
    dsn: str,
    *,
    run_id: str,
    source: str,
    subtype: str,
) -> list[str]:
    return _node_record_values_for_run(
        dsn,
        run_id=run_id,
        source=source,
        subtype=subtype,
        field="path",
    )


def _json_field_values_for_run(
    dsn: str,
    *,
    column: str,
    field: str,
    run_id: str,
    source: str,
    subtype: str,
) -> list[str]:
    # `field` is a trusted test-internal JSON key (path / artifact /
    # threshold). The Chronos query layer rewrites the `->>` operator
    # into json_extract_path_text(), which rejects a bound parameter
    # for the key — so the key is interpolated, guarded to an
    # identifier-shaped token to stay injection-safe.
    if not field.replace("_", "").isalnum():
        raise ValueError(f"refusing non-identifier JSON key {field!r}")
    store, graph = _live_graph_for_run(dsn)
    try:
        rows = graph.query(
            f"""
            SELECT DISTINCT {column}->>'{field}' AS value
              FROM okg.graph_nodes
             WHERE run_id = :run_id
               AND source = :source
               AND subtype = :subtype
             ORDER BY 1
            """,
            {
                "run_id": run_id,
                "source": source,
                "subtype": subtype,
            },
        )
        return [str(row["value"]) for row in rows]
    finally:
        store.close()


def _node_record_values_for_run(
    dsn: str,
    *,
    run_id: str,
    source: str,
    subtype: str,
    field: str,
) -> list[str]:
    return _json_field_values_for_run(
        dsn,
        column="source_record_id",
        field=field,
        run_id=run_id,
        source=source,
        subtype=subtype,
    )


def _node_attr_values_for_run(
    dsn: str,
    *,
    run_id: str,
    source: str,
    subtype: str,
    field: str,
) -> list[str]:
    return _json_field_values_for_run(
        dsn,
        column="attrs",
        field=field,
        run_id=run_id,
        source=source,
        subtype=subtype,
    )


def _latest_generation_status(dsn: str) -> str:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        generations = store.list_generations(
            deployment_name="test",
            status="published",
        )
        assert generations
        return str(generations[-1].metadata["status"])
    finally:
        store.close()


def _live_node_count(dsn: str, subtype: str) -> int:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        latest = store.list_generations(
            deployment_name="test",
            status="published",
        )[-1]
        graph = store.checkout_generation(latest.checkpoint_id)
        return int(graph.query(
            """
            SELECT count(*) AS count
              FROM okg.graph_nodes
             WHERE subtype = :subtype
            """,
            {"subtype": subtype},
        )[0]["count"])
    finally:
        store.close()


def _queued_jobs_by_kind(dsn: str) -> dict[str, int]:
    from okg.substrate.runtime_queue_health import summarize_runtime_queue_health

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        summary = summarize_runtime_queue_health(cur)
    return {
        str(kind): int(count)
        for kind, count in summary["queue_depth"].items()
        if int(count)
    }


def _dlq_count(dsn: str) -> int:
    from okg.substrate.runtime_queue_health import summarize_runtime_queue_health

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        summary = summarize_runtime_queue_health(cur)
    return int(summary["dlq_count"])


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _write_junit(path: Path, *, name: str) -> Path:
    return _write(
        path,
        f"""<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="pytest" tests="1" failures="0" errors="0" skipped="0" time="0.01">
    <testcase classname="tests.{name}" name="test_{name}" time="0.01" file="src/app.py"/>
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


def _write_json(path: Path, payload: object) -> Path:
    return _write(path, json.dumps(payload, indent=2))


def _write_semgrep(path: Path, *, fingerprint: str) -> Path:
    return _write_json(
        path,
        {
            "version": "1.2.3",
            "results": [
                {
                    "check_id": "python.lang.security.audit.demo",
                    "path": "src/app.py",
                    "start": {"line": 1},
                    "end": {"line": 1},
                    "extra": {
                        "severity": "WARNING",
                        "message": f"demo finding {fingerprint}",
                        "fingerprint": fingerprint,
                        "metadata": {"confidence": "high"},
                    },
                },
            ],
        },
    )


def _write_gitleaks(path: Path, *, fingerprint: str) -> Path:
    return _write_json(
        path,
        [
            {
                "RuleID": "generic-api-key",
                "File": "src/app.py",
                "StartLine": 1,
                "Fingerprint": fingerprint,
                "Entropy": 4.2,
                "Match": "token=raw-super-secret-token",
                "Secret": "raw-super-secret-token",
                "Commit": "abc",
            },
        ],
    )


def _write_profile(
    path: Path,
    *,
    run_id: str,
    duration_ms: int,
) -> Path:
    return _write_json(
        path,
        {
            "profile_run_id": run_id,
            "deployment": "okg-workspace",
            "target_kind": "publish",
            "command": "okg run --once --deployment okg-workspace",
            "started_at": "2026-05-02T12:00:00Z",
            "duration_ms": duration_ms,
            "generation_id": 42,
            "stages": [
                {
                    "name": f"publisher.{run_id}",
                    "duration_ms": duration_ms,
                }
            ],
        },
    )


def _write_benchmark(
    path: Path,
    *,
    profile_run_id: str,
    value: float,
) -> Path:
    return _write_json(
        path,
        {
            "profile_run_id": profile_run_id,
            "generation_id": 42,
            "benchmarks": [
                {
                    "scenario": "publish_cycle",
                    "metric": "duration_ms",
                    "value": value,
                    "unit": "ms",
                    "baseline_value": 1000.0,
                }
            ],
        },
    )


def _write_self_play_report(
    path: Path,
    *,
    scenario_id: str,
    generation_id: int,
    score: float,
) -> Path:
    return _write_json(
        path,
        {
            "report_format": "okg-workspace-self-play/v1",
            "deployment": "okg-workspace",
            "generation_id": generation_id,
            "catalog_version_id": 4,
            "created_at": "2026-05-02T21:00:00Z",
            "graph_summary": {"node_count": 100, "edge_count": 200},
            "scenario_results": [
                {
                    "scenario_id": scenario_id,
                    "question_class": "code_lookup",
                    "substrate_stage_target": "read_side_surfacing",
                    "question": "Where is run_sql implemented?",
                    "answerability": {"score": score},
                    "expected_evidence_subtypes": ["code_symbol"],
                    "observed_subtype_counts": {"code_symbol": 1},
                    "evidence_edge_paths": [],
                    "evidence_node_ids": [],
                    "tool_calls": [],
                },
            ],
        },
    )
