from __future__ import annotations

import json
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


def _write_json(path: Path, payload: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def _activate_generation_publish_allow(dsn: str, *, deployment_name: str) -> None:
    """Activate an allow-publish Nomos bundle for an enforced deployment.

    Mirrors `test_publisher._activate_generation_publish_policy`: the
    publish gate evaluates surface `generation.publish` with subject
    `okg_service:publisher` under scope `deployment:<name>`. Without an
    active bundle, an enforce-mode deployment default-denies the publish.
    """
    from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph

    scope = f"deployment:{deployment_name}"
    bundle = compile_policy_graph({
        "policy_bundle_id": f"nomos:test:generation-publish:{deployment_name}",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [{
            "rule_id": "allow_publisher_generation_publish",
            "effect": "allow",
            "actions": ["generation.publish"],
            "subjects": ["okg_service:publisher"],
            "priority": 1,
            "reason_code": "nomos.test.allow_generation_publish",
        }],
    })
    with psycopg.connect(dsn) as conn:
        # bootstrap=True breaks the activation chicken-and-egg: under
        # enforce mode, `policy.activate` itself default-denies with no
        # active bundle. Bootstrap is allowed only for the first
        # `policy.activate` on a scope (refused once a bundle exists).
        activate_policy_bundle(
            conn,
            deployment_name=deployment_name,
            bundle=bundle,
            scope=scope,
            activated_by="test:operator",
            bootstrap=True,
        )
        conn.commit()


def test_profile_measurement_source_imports_profiles_and_benchmarks(tmp_path: Path) -> None:
    from okg.substrate.library.sources.profile_measurements import (
        ProfileMeasurementSource,
    )

    repo = tmp_path / "repo"
    artifacts = _profile_artifacts(repo)
    benchmark = _benchmark_artifact(repo, "profile-run:okg-workspace:run-abc")

    source = ProfileMeasurementSource(
        deployment="okg-workspace",
        repo="demo",
        repo_root=repo,
        artifact_paths=artifacts,
        benchmark_paths=[benchmark],
    )
    run = source.run("run-1", mode="scope_complete")
    facts = list(run.facts)
    nodes = [fact for fact in facts if isinstance(fact, NodeFact)]
    edges = [fact for fact in facts if isinstance(fact, EdgeFact)]
    subtypes = {node.subtype for node in nodes}

    assert {fact.source_record_id.get("repo") for fact in facts} == {"demo"}
    assert all(fact.source_record_id.get("artifact") for fact in facts)
    assert run.completed_scope is True
    assert run.record_set is not None
    assert {
        "profile_run",
        "profile_stage",
        "profile_query",
        "profile_hotspot",
        "benchmark_measurement",
    } <= subtypes

    profile = next(
        node for node in nodes
        if node.subtype == "profile_run"
        and node.node_id == "profile-run:okg-workspace:run-abc"
    )
    assert profile.node_id == "profile-run:okg-workspace:run-abc"
    assert profile.attrs["target_kind"] == "publish"
    assert profile.attrs["command"] == "okg run --once --deployment okg-workspace"
    assert profile.attrs["generation_id"] == 42
    assert profile.attrs["artifact"] == "profiles/okg-workspace/run-abc/profile.json"
    assert profile.attrs["workflow_id"] == "wf-profile-run-abc"
    assert profile.attrs["owner_id"] == "workflow:source_sync:okg-workspace"
    assert profile.attrs["source_sync"]["sources"] == ["pact"]
    assert profile.attrs["collector_evidence"]["rootless_speedscope"]["status"] == "available"
    assert profile.attrs["artifacts"][0]["kind"] == "rootless-speedscope"

    stages = [node for node in nodes if node.subtype == "profile_stage"]
    queries = [node for node in nodes if node.subtype == "profile_query"]
    hotspots = [node for node in nodes if node.subtype == "profile_hotspot"]
    assert {stage.attrs["name"] for stage in stages} >= {
        "publisher.phase_a",
        "publisher.gates",
    }
    source_stage = next(
        stage for stage in stages
        if stage.attrs["name"] == "source.pact.run"
    )
    assert source_stage.attrs["source_name"] == "pact"
    assert source_stage.attrs["workflow_id"] == "wf-profile-run-abc"
    assert source_stage.attrs["input_count"] == 2
    assert source_stage.attrs["output_count"] == 3
    assert source_stage.attrs["timing_status"] == "available"
    assert source_stage.attrs["timing_unavailable_reason"] is None
    assert source_stage.attrs["unavailable_reason"] is None
    assert source_stage.attrs["nodes_appended"] == 1
    assert source_stage.attrs["edges_appended"] == 2
    assert source_stage.attrs["timings_ms"]["source_runner"] == 70
    assert source_stage.attrs["source_stats"] == {"records": 3}
    assert {query.attrs["fingerprint"] for query in queries} >= {"select_nodes"}
    assert {"stage", "query", "frame", "scalene", "memray"} <= {
        hotspot.attrs["kind"] for hotspot in hotspots
    }
    assert any(hotspot.attrs["symbol_hint"] == "project_phase_a" for hotspot in hotspots)

    measurement = next(node for node in nodes if node.subtype == "benchmark_measurement")
    assert measurement.attrs["scenario"] == "publish_cycle"
    assert measurement.attrs["metric"] == "duration_ms"
    assert measurement.attrs["status"] == "passed"
    assert measurement.attrs["profile_run_id"] == "profile-run:okg-workspace:run-abc"
    assert measurement.attrs["artifact"] == "reports/benchmarks.json"

    edge_patterns = {(edge.src, edge.edge_type, edge.dst) for edge in edges}
    assert any(
        src == "profile-run:okg-workspace:run-abc"
        and edge_type == "contains"
        and dst.startswith("profile-stage:")
        for src, edge_type, dst in edge_patterns
    )
    assert any(
        src == "profile-run:okg-workspace:run-abc"
        and edge_type == "contains"
        and dst.startswith("profile-query:")
        for src, edge_type, dst in edge_patterns
    )
    assert any(
        src == "profile-run:okg-workspace:run-abc"
        and edge_type == "contains"
        and dst.startswith("profile-hotspot:")
        for src, edge_type, dst in edge_patterns
    )
    assert any(
        src.startswith("benchmark:")
        and edge_type == "references"
        and dst == "profile-run:okg-workspace:run-abc"
        for src, edge_type, dst in edge_patterns
    )


def test_profile_measurement_source_ingests_budgets_and_marks_failures(tmp_path: Path) -> None:
    from okg.substrate.library.sources.profile_measurements import (
        ProfileMeasurementSource,
    )

    repo = tmp_path / "repo"
    benchmark = _budgeted_benchmark_artifact(repo, 1200.0)
    config = _budget_config(repo, threshold=1000.0)
    openspec_root = _openspec_budget_fixture(repo, threshold=900.0)

    source = ProfileMeasurementSource(
        deployment="okg-workspace",
        repo="demo",
        repo_root=repo,
        benchmark_paths=[benchmark],
        deployment_config_path=config,
        openspec_roots=[openspec_root],
    )
    facts = list(source.run("run-1", mode="scope_complete").facts)
    nodes = [fact for fact in facts if isinstance(fact, NodeFact)]
    edges = [fact for fact in facts if isinstance(fact, EdgeFact)]

    assert {fact.source_record_id.get("repo") for fact in facts} == {"demo"}
    assert all(fact.source_record_id.get("artifact") for fact in facts)
    budgets = [node for node in nodes if node.subtype == "performance_budget"]
    assert {budget.attrs["scope"] for budget in budgets} == {"publish_cycle"}
    assert {budget.attrs["threshold"] for budget in budgets} == {1000.0, 900.0}
    measurement = next(node for node in nodes if node.subtype == "benchmark_measurement")
    assert measurement.attrs["status"] == "failed"
    assert measurement.attrs["threshold"] == 1000.0
    assert {
        ("references", budget.node_id)
        for budget in budgets
    } <= {
        (edge.edge_type, edge.dst)
        for edge in edges
        if edge.src == measurement.node_id
    }
    assert any(
        edge.edge_type == "budgeted_by"
        and edge.dst == "crit:perf-budget:quality-evidence-graph:added:0"
        for edge in edges
    )


def test_profile_measurement_source_scopes_to_configured_artifacts(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.profile_measurements import (
        ProfileMeasurementSource,
    )

    repo = tmp_path / "repo"
    _write_profile(repo / "profiles" / "okg-workspace" / "run-a" / "profile.json", run_id="run-a")
    profile_b = _write_profile(
        repo / "profiles" / "okg-workspace" / "run-b" / "profile.json",
        run_id="run-b",
    )
    _write_benchmark(
        repo / "reports" / "benchmarks-a.json",
        profile_run_id="profile-run:okg-workspace:run-a",
        value=700.0,
    )
    benchmark_b = _write_benchmark(
        repo / "reports" / "benchmarks-b.json",
        profile_run_id="profile-run:okg-workspace:run-b",
        value=800.0,
    )
    unconfigured = _write_profile(
        repo / "other" / "profile.json",
        run_id="run-escape",
    )
    source = ProfileMeasurementSource(
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

    profile_run = source.run(
        "run-profile",
        mode="scope_incomplete",
        sync_scope={
            "scope_keys": [
                profile_b.relative_to(repo).as_posix(),
                "../escape.json",
            ],
        },
    )
    profile_nodes = [
        fact for fact in profile_run.facts
        if isinstance(fact, NodeFact)
    ]
    assert profile_run.completed_scope is False
    assert {
        node.attrs.get("artifact")
        for node in profile_nodes
        if node.subtype == "profile_run"
    } == {"profiles/okg-workspace/run-b/profile.json"}
    assert not any(node.subtype == "benchmark_measurement" for node in profile_nodes)
    assert any(node.subtype == "performance_budget" for node in profile_nodes)

    benchmark_run = source.run(
        "run-benchmark",
        mode="scope_incomplete",
        sync_scope={"scope_keys": [benchmark_b.relative_to(repo).as_posix()]},
    )
    benchmark_nodes = [
        fact for fact in benchmark_run.facts
        if isinstance(fact, NodeFact)
    ]
    assert benchmark_run.completed_scope is False
    assert {
        node.attrs.get("artifact")
        for node in benchmark_nodes
        if node.subtype == "benchmark_measurement"
    } == {"reports/benchmarks-b.json"}
    assert not any(node.subtype == "profile_run" for node in benchmark_nodes)
    assert any(node.subtype == "performance_budget" for node in benchmark_nodes)

    ignored = source.run(
        "run-ignored",
        mode="scope_incomplete",
        sync_scope={"scope_keys": [unconfigured.relative_to(repo).as_posix()]},
    )
    assert list(ignored.facts) == []
    assert ignored.completed_scope is False


def test_profile_measurements_skips_unchanged_scoped_artifact(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.profile_measurements import (
        ProfileMeasurementSource,
    )

    repo = tmp_path / "repo"
    profile = _write_profile(
        repo / "profiles" / "okg-workspace" / "run-a" / "profile.json",
        run_id="run-a",
    )
    source = ProfileMeasurementSource(
        deployment="okg-workspace",
        repo="demo",
        repo_root=repo,
        artifact_globs=["profiles/okg-workspace/*/profile.json"],
        performance_budgets=[{
            "budget_id": "performance-budget:explicit:publish-cycle",
            "scope": "publish_cycle",
            "metric": "duration_ms",
            "threshold": 1000.0,
            "unit": "ms",
            "operator": "<=",
        }],
    )
    scope = {"scope_keys": [profile.relative_to(repo).as_posix()]}

    first = source.run("run-1", mode="scope_incomplete", sync_scope=scope)
    first_facts = list(first.facts)

    assert any(
        isinstance(fact, NodeFact)
        and fact.subtype == "profile_run"
        and fact.attrs["artifact"] == "profiles/okg-workspace/run-a/profile.json"
        for fact in first_facts
    )
    assert source.last_run_stats["scope_status"] == "scoped"
    assert source.last_run_stats["artifacts_parsed"] == 1
    assert source.last_run_stats["artifacts_skipped_unchanged"] == 0
    assert first.next_cursor is not None
    assert (
        "profiles/okg-workspace/run-a/profile.json"
        in first.next_cursor["artifact_frontier"]
    )

    repeated = ProfileMeasurementSource(
        deployment="okg-workspace",
        repo="demo",
        repo_root=repo,
        artifact_globs=["profiles/okg-workspace/*/profile.json"],
        performance_budgets=source.performance_budgets,
    )
    second = repeated.run(
        "run-2",
        mode="scope_incomplete",
        cursor=first.next_cursor,
        sync_scope=scope,
    )

    assert list(second.facts) == []
    assert second.completed_scope is False
    assert repeated.last_run_stats["scope_status"] == "scoped_unchanged_noop"
    assert repeated.last_run_stats["artifacts_considered"] == 1
    assert repeated.last_run_stats["artifacts_parsed"] == 0
    assert repeated.last_run_stats["artifacts_skipped_unchanged"] == 1
    assert repeated.last_run_stats["emitted_nodes"] == 0
    assert repeated.last_run_stats["emitted_edges"] == 0

    _write_profile(profile, run_id="run-a-changed")
    changed = ProfileMeasurementSource(
        deployment="okg-workspace",
        repo="demo",
        repo_root=repo,
        artifact_globs=["profiles/okg-workspace/*/profile.json"],
        performance_budgets=source.performance_budgets,
    )
    third = changed.run(
        "run-3",
        mode="scope_incomplete",
        cursor=second.next_cursor,
        sync_scope=scope,
    )
    third_facts = list(third.facts)

    assert any(
        isinstance(fact, NodeFact)
        and fact.subtype == "profile_run"
        and fact.node_id == "profile-run:okg-workspace:run-a-changed"
        for fact in third_facts
    )
    assert changed.last_run_stats["scope_status"] == "scoped"
    assert changed.last_run_stats["artifacts_parsed"] == 1
    assert changed.last_run_stats["artifacts_skipped_unchanged"] == 0


@pytest.mark.postgres
def test_profile_measurement_source_publish_projects_profile_nodes(
    clean_runtime: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.openspec import OpenSpecSource
    from okg.substrate.library.sources.profile_measurements import (
        ProfileMeasurementSource,
    )
    from okg.substrate.publisher import Publisher

    repo = tmp_path / "repo"
    artifacts = _profile_artifacts(repo)
    benchmark = _budgeted_benchmark_artifact(repo, 1200.0)
    config = _budget_config(repo, threshold=1000.0)
    openspec_root = _openspec_budget_fixture(repo, threshold=900.0)

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-profile-measurements",
        "description": "profile evidence publish fixture",
        "modules": [
            "git_graph",
            "openspec",
            "agent_sessions",
            "compute_env",
            "code_graph",
            "ci",
            "quality_evidence",
        ],
    }))
    (dep / "schemas").mkdir()

    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, composed)

    # The real `okg-workspace` deployment manifest declares
    # `nomos.runtime_enforcement: enforce`, so the generation.publish
    # boundary is gated. The ephemeral test DB has no active bundle, so
    # the publish default-denies (nomos.default_deny.no_active_bundle).
    # Seed an allow-publish bundle for okg-workspace so the publish
    # proceeds exactly as a governed production deployment would.
    _activate_generation_publish_allow(clean_runtime, deployment_name="okg-workspace")

    SourceRunner(clean_runtime, source_name="fixture.openspec").run(
        OpenSpecSource(repo="demo", openspec_root=openspec_root),
        mode="scope_complete",
    )
    SourceRunner(clean_runtime, source_name="fixture.profile-measurements").run(
        ProfileMeasurementSource(
            deployment="okg-workspace",
            repo="demo",
            repo_root=repo,
            artifact_paths=artifacts,
            benchmark_paths=[benchmark],
            deployment_config_path=config,
            openspec_roots=[openspec_root],
        ),
        mode="scope_complete",
    )
    out = Publisher(clean_runtime, "okg-workspace", catalog_identity=bind_test_catalog_writer(clean_runtime, "okg-workspace")).cycle()
    assert out["status"] == "published", out

    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        graph = store.checkout_generation(out["generation_id"])
        counts = {
            str(row["subtype"]): int(row["count"])
            for row in graph.query(
                """
                SELECT subtype, count(*) AS count
                  FROM okg.graph_nodes
                 WHERE subtype = ANY(:subtypes)
                 GROUP BY subtype
                """,
                {"subtypes": [
                    "profile_run",
                    "profile_stage",
                    "profile_query",
                    "profile_hotspot",
                    "benchmark_measurement",
                    "performance_budget",
                ]},
            )
        }
        edge_counts = {
            (str(row["src_subtype"]), str(row["edge_type"]), str(row["dst_subtype"])): int(row["count"])
            for row in graph.query(
                """
                SELECT s.subtype AS src_subtype,
                       e.edge_type,
                       d.subtype AS dst_subtype,
                       count(*) AS count
                  FROM okg.graph_edges e
                  JOIN okg.graph_nodes s ON s.node_id = e.src
                  JOIN okg.graph_nodes d ON d.node_id = e.dst
                 WHERE s.subtype IN (
                         'profile_run',
                         'benchmark_measurement',
                         'performance_budget'
                       )
                 GROUP BY 1, 2, 3
                """
            )
        }
    finally:
        store.close()

    assert counts["profile_run"] >= 4
    assert counts["profile_stage"] >= 2
    assert counts["profile_query"] >= 1
    assert counts["profile_hotspot"] >= 5
    assert counts["benchmark_measurement"] == 1
    assert counts["performance_budget"] == 2
    assert edge_counts[("profile_run", "contains", "profile_stage")] >= 2
    assert edge_counts[("profile_run", "contains", "profile_query")] >= 1
    assert edge_counts[("profile_run", "contains", "profile_hotspot")] >= 5
    assert edge_counts[("benchmark_measurement", "references", "performance_budget")] == 2
    assert edge_counts[("performance_budget", "budgeted_by", "acceptance_criterion")] == 1

    from okg.substrate.cli.main import main

    rc = main([
        "doctor",
        "--check",
        "okg-workspace",
        "--deployment",
        "okg-workspace",
        "--dsn",
        clean_runtime,
        "--json",
    ])
    assert rc == 1
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["status"] == "FAIL"
    performance_failure = next(
        failure for failure in payload["failures"]
        if failure["code"] == "performance_budget_failed"
    )
    sample = performance_failure["samples"][0]
    assert sample["scenario"] == "publish_cycle"
    assert sample["value"] == 1200.0
    assert sample["threshold"] in {900.0, 1000.0}


def _profile_artifacts(repo: Path) -> list[Path]:
    return [
        _write_json(
            repo / "profiles" / "okg-workspace" / "run-abc" / "profile.json",
            {
                "profile_run_id": "run-abc",
                "deployment": "okg-workspace",
                "target_kind": "publish",
                "command": "okg run --once --deployment okg-workspace",
                "started_at": "2026-05-02T12:00:00Z",
                "duration_ms": 1200,
                "generation_id": 42,
                "request_path": "wf-profile-run-abc",
                "owner_id": "workflow:source_sync:okg-workspace",
                "owner_meta": {"task_id": "task.source-sync-performance"},
                "source_sync": {
                    "sources": ["pact"],
                    "run_mode": "scope_complete",
                    "event_count": 1,
                },
                "policy": {"mode": "deep"},
                "evidence": {"rootless_speedscope": {"status": "available"}},
                "artifacts": [{
                    "path": "profiles/okg-workspace/run-abc/speedscope.json",
                    "media_type": "application/json",
                    "kind": "rootless-speedscope",
                }],
                "summary": {
                    "bottleneck_class": "db",
                    "top_stage": "publisher.phase_a",
                    "top_query": "select_nodes",
                },
                "stages": [
                    {
                        "name": "publisher.phase_a",
                        "started_at_ns": 1,
                        "duration_ns": 600_000_000,
                    },
                    {
                        "name": "source.pact.run",
                        "started_at_ns": 2,
                        "duration_ms": 75,
                        "attrs": {
                            "source_name": "pact",
                            "workflow_id": "wf-profile-run-abc",
                            "input_count": 2,
                            "output_count": 3,
                            "timing_status": "available",
                            "timing_unavailable_reason": None,
                            "unavailable_reason": None,
                            "nodes_appended": 1,
                            "edges_appended": 2,
                            "retract_n": 0,
                            "retract_e": 0,
                            "timings_ms": {
                                "admission_prepare": 1,
                                "source_runner": 70,
                                "admission_finalize": 4,
                                "total": 75,
                            },
                            "source_stats": {"records": 3},
                        },
                    },
                    {
                        "name": "publisher.gates",
                        "started_at_ns": 3,
                        "duration_ms": 25,
                    },
                ],
                "queries": [
                    {
                        "fingerprint": "select_nodes",
                        "stage": "publisher.phase_a",
                        "duration_ns": 250_000_000,
                        "rowcount": 12,
                        "sql_shape": "SELECT * FROM okg.nodes_live",
                    }
                ],
            },
        ),
        _write_json(
            repo / "profiles" / "okg-workspace" / "run-abc" / "py-spy-speedscope.json",
            {
                "$schema": "https://www.speedscope.app/file-format-schema.json",
                "shared": {
                    "frames": [
                        {"name": "project_phase_a", "file": "src/okg/substrate/publisher/publisher.py", "line": 1398},
                        {"name": "run_enrichers", "file": "src/okg/substrate/publisher/publisher.py", "line": 1404},
                    ]
                },
                "profiles": [
                    {
                        "type": "sampled",
                        "unit": "milliseconds",
                        "samples": [[0], [0], [1]],
                        "weights": [10, 20, 5],
                    }
                ],
            },
        ),
        _write_json(
            repo / "profiles" / "okg-workspace" / "run-abc" / "scalene.json",
            {
                "profiler": "scalene",
                "files": {
                    "src/okg/substrate/projection/projector.py": {
                        "functions": [
                            {"name": "partition_by_source", "line": 51, "total_time": 15.0}
                        ]
                    }
                },
            },
        ),
        _write_json(
            repo / "profiles" / "okg-workspace" / "run-abc" / "memray.json",
            {
                "profiler": "memray",
                "hotspots": [
                    {
                        "function": "copy_node_facts",
                        "file": "src/okg/substrate/ingest/runner.py",
                        "line": 410,
                        "allocated": 8192,
                    }
                ],
            },
        ),
    ]


def _write_profile(path: Path, *, run_id: str) -> Path:
    return _write_json(
        path,
        {
            "profile_run_id": run_id,
            "deployment": "okg-workspace",
            "target_kind": "publish",
            "command": "okg run --once --deployment okg-workspace",
            "started_at": "2026-05-02T12:00:00Z",
            "duration_ms": 1200,
            "generation_id": 42,
            "stages": [
                {
                    "name": f"publisher.{run_id}",
                    "duration_ms": 50,
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


def _benchmark_artifact(repo: Path, profile_run_id: str) -> Path:
    return _write_json(
        repo / "reports" / "benchmarks.json",
        {
            "profile_run_id": profile_run_id,
            "generation_id": 42,
            "benchmarks": [
                {
                    "scenario": "publish_cycle",
                    "metric": "duration_ms",
                    "value": 1200.0,
                    "unit": "ms",
                    "threshold": 1500.0,
                    "operator": "<=",
                    "baseline_value": 1800.0,
                }
            ],
        },
    )


def _budgeted_benchmark_artifact(repo: Path, value: float) -> Path:
    return _write_json(
        repo / "reports" / "budgeted-benchmarks.json",
        {
            "profile_run_id": "profile-run:okg-workspace:run-abc",
            "generation_id": 42,
            "benchmarks": [
                {
                    "scenario": "publish_cycle",
                    "metric": "duration_ms",
                    "value": value,
                    "unit": "ms",
                    "baseline_value": 1800.0,
                }
            ],
        },
    )


def _budget_config(repo: Path, *, threshold: float) -> Path:
    return _write_json(
        repo / "deployment.yaml",
        {
            "name": "okg-workspace",
            "performance_budgets": [
                {
                    "budget_id": "performance-budget:deployment:publish-cycle",
                    "scope": "publish_cycle",
                    "metric": "duration_ms",
                    "threshold": threshold,
                    "unit": "ms",
                    "operator": "<=",
                }
            ],
        },
    )


def _openspec_budget_fixture(repo: Path, *, threshold: float) -> Path:
    root = repo / "openspec"
    change = root / "changes" / "perf-budget"
    (change / "specs" / "quality-evidence-graph").mkdir(parents=True, exist_ok=True)
    (change / "proposal.md").write_text(
        """# Change: Performance Budget Fixture

## Why
Need budget evidence.

## What Changes
Adds a performance-sensitive criterion.

## Impact
Fixture only.
""",
        encoding="utf-8",
    )
    (change / "specs" / "quality-evidence-graph" / "spec.md").write_text(
        f"""## ADDED Requirements

### Requirement: Publish budget

The publish cycle SHALL stay under the configured threshold.

<!-- okg:performance-budget {{"scope": "publish_cycle", "metric": "duration_ms", "threshold": {threshold}, "unit": "ms", "operator": "<=", "criterion_id": "crit:perf-budget:quality-evidence-graph:added:0"}} -->

#### Scenario: publish is measured

- **WHEN** the benchmark runs
- **THEN** the budget is evaluated
""",
        encoding="utf-8",
    )
    return root
