from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest

from okg.substrate.derived_artifacts import (
    RelationArtifactDefinition,
    ViewArtifactDefinition,
    register_jsonb_relation,
    register_view_artifact_definition,
    stable_hash,
)
from okg.substrate.cli.main import main
from okg.substrate.source_sync_microbench import (
    MICROBENCH_FULL_PIPELINE_PROGRESS_KEY,
    MICROBENCH_LEDGER_DIRTY_DEPENDENCY_KIND,
    MICROBENCH_LEDGER_DIRTY_REASON,
    MICROBENCH_LEDGER_RELATION_DEFINITION_ID,
    MICROBENCH_LEDGER_VIEW_DEFINITION_ID,
    MICROBENCH_LEDGER_VIEW_ID,
    _microbench_event_plan,
    _persist_microbench_full_pipeline_progress,
    build_source_sync_microbenchmark_report,
    build_microbenchmark_inventory,
    microbenchmark_inventory_relation_rows,
)
from okg.substrate.source_sync_optimization_ledger import (
    build_source_optimization_ledger,
    source_optimization_ledger_relation_rows,
)
from okg.substrate.sources.registry import load_source_registry


def _gate_observability_pass() -> dict[str, object]:
    return {
        "expected_delta_source": "explicit",
        "expected_delta_units": 1.0,
        "observed_delta_units": 1.0,
        "source_work_units": 1.0,
        "work_amplification_ratio": 1.0,
        "work_amplification_budget": 3.0,
        "observability_status": "pass",
        "observability_gaps": [],
    }


def test_microbenchmark_inventory_classifies_sources_and_scenarios(
    tmp_path: Path,
) -> None:
    dep = _write_microbench_deployment(tmp_path)
    registry = load_source_registry(dep / "source_registry.yaml")

    inventory = build_microbenchmark_inventory(
        deployment="sync-demo",
        deployment_dir=dep,
        registry=registry,
        runtime_enabled=True,
        runtime_source_sync={
            "watch": {"enabled": True},
            "poll": {"enabled": True},
            "webhook": {"enabled": False},
        },
        include_live=False,
        enrichers=[
            {
                "class": "okg.substrate.library.linkers.example.ExampleLinker",
                "config": {"name": "example"},
            }
        ],
    )

    assert inventory["schema"] == "okg.source_sync.microbench.inventory.v1"
    assert inventory["source_count"] == 3
    assert inventory["enricher_count"] == 1
    by_name = {row["source_name"]: row for row in inventory["sources"]}
    assert by_name["docs"]["coverage_class"] == "useful_automatic"
    assert by_name["docs"]["scope_support_status"] == "declared"
    assert by_name["docs"]["producer_status"] == "configured"
    assert by_name["docs"]["expected_producer_count"] == 2
    assert by_name["docs"]["benchmark_scenarios"] == [
        "source-only",
        "full-pipeline",
    ]
    assert by_name["manual"]["coverage_class"] == "manual_full_rebuild"
    assert by_name["legacy"]["coverage_class"] == "unsupported"
    assert by_name["legacy"]["blocker"] == "source_sync_not_declared"

    relation_rows = microbenchmark_inventory_relation_rows(inventory)
    row_keys = {row["row_key"] for row in relation_rows}
    assert {
        "source:docs",
        "source:manual",
        "source:legacy",
        "enricher:example",
    } <= row_keys


def test_microbenchmark_inventory_accepts_unscoped_poll_sources(
    tmp_path: Path,
) -> None:
    dep = _write_microbench_deployment(tmp_path)
    registry_path = dep / "source_registry.yaml"
    text = registry_path.read_text(encoding="utf-8")
    registry_path.write_text(
        text.replace(
            "  legacy:\n",
            "  poller:\n"
            "    module: okg.substrate.library.sources.bootstrap_fixture\n"
            "    class: BootstrapFixtureSource\n"
            "    ownership_id: sync-demo.poller\n"
            "    params:\n"
            f"      fixture_path: {dep / 'docs-fixture.json'}\n"
            "    source_class: mutable_api\n"
            "    record_identity_kind: remote_id\n"
            "    record_identity_fields: [id]\n"
            "    source_revision_kind: updated_at\n"
            "    deletion_semantics: missing_from_completed_scope\n"
            "    publication_mode: published_generation\n"
            "    sync:\n"
            "      triggers: [poll, manual, reconcile]\n"
            "      default_event_mode: reconcile\n"
            "      reconcile_mode: reconcile\n"
            "      deletes_require_reconcile: true\n"
            "      poll:\n"
            "        probe: change_probe\n"
            "        interval_seconds: 60\n"
            "  legacy:\n",
        ),
        encoding="utf-8",
    )
    registry = load_source_registry(registry_path)

    inventory = build_microbenchmark_inventory(
        deployment="sync-demo",
        deployment_dir=dep,
        registry=registry,
        runtime_enabled=True,
        runtime_source_sync={
            "watch": {"enabled": True},
            "poll": {"enabled": True},
            "webhook": {"enabled": False},
        },
        include_live=False,
    )

    by_name = {row["source_name"]: row for row in inventory["sources"]}
    poller = by_name["poller"]
    assert poller["coverage_class"] == "useful_automatic"
    assert poller["scope_support_status"] == "unscoped_poll_reconcile"
    assert poller["blocker"] is None
    assert poller["benchmark_scenarios"] == ["source-only", "full-pipeline"]


def test_sync_microbench_inventory_cli_emits_registry_matrix(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_microbench_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))

    rc = main(
        [
            "--json",
            "sync",
            "microbench",
            "inventory",
            "--deployment",
            "sync-demo",
            "--no-live",
        ]
    )

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["deployment"] == "sync-demo"
    assert out["live_state"]["status"] == "not_loaded"
    by_name = {row["source_name"]: row for row in out["sources"]}
    assert by_name["docs"]["trigger_kinds"] == [
        "fs_event",
        "manual",
        "poll",
        "reconcile",
    ]
    assert by_name["docs"]["default_event_mode"] == "scope_incomplete"
    assert by_name["docs"]["reconcile_mode"] == "scope_complete"
    assert by_name["docs"]["scope_accepts"] == ["path"]
    assert by_name["manual"]["benchmark_scenarios"] == ["source-only"]


def test_sync_microbench_inventory_uses_manifest_sync_root_for_registry(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    dep = _write_microbench_deployment(tmp_path)
    sync_root = tmp_path / "sync-root"
    sync_root.mkdir()
    registry_path = dep / "source_registry.yaml"
    registry_path.write_text(
        registry_path.read_text(encoding="utf-8")
        .replace(
            f"        roots: [{dep}]\n",
            "        roots: ['${repo_root}']\n",
            1,
        )
        .replace(
            "      fixture_path:",
            "      repo_root: ${repo_root}\n      fixture_path:",
            1,
        ),
        encoding="utf-8",
    )
    deployment_path = dep / "deployment.yaml"
    deployment_path.write_text(
        deployment_path.read_text(encoding="utf-8")
        + f"sync_root: {sync_root}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    captured = {}

    def fake_build_inventory(**kwargs):
        registry = kwargs["registry"]
        docs = registry["docs"]
        captured["repo_root"] = docs.raw["params"]["repo_root"]
        captured["watch_roots"] = list(docs.sync.watch.roots)
        return {
            "schema": "okg.source_sync.microbench.inventory.v1",
            "deployment": kwargs["deployment"],
            "live_state": {"status": "not_loaded"},
            "source_count": 0,
            "enricher_count": 0,
            "sources": [],
            "enrichers": [],
        }

    monkeypatch.setattr(
        "okg.substrate.source_sync_microbench.build_microbenchmark_inventory",
        fake_build_inventory,
    )

    rc = main(
        [
            "--json",
            "sync",
            "microbench",
            "inventory",
            "--deployment",
            "sync-demo",
            "--no-live",
        ]
    )

    assert rc == 0
    assert json.loads(capsys.readouterr().out)["deployment"] == "sync-demo"
    assert captured == {
        "repo_root": str(sync_root),
        "watch_roots": [str(sync_root)],
    }


def test_sync_microbench_inventory_fails_closed_when_manifest_unresolved(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_microbench_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))

    def fail_resolve(_deployment: str):
        raise RuntimeError("manifest boom")

    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        fail_resolve,
    )

    rc = main(
        [
            "--json",
            "sync",
            "microbench",
            "inventory",
            "--deployment",
            "sync-demo",
            "--no-live",
        ]
    )

    assert rc == 1
    err = json.loads(capsys.readouterr().err)["error"]
    assert err["message"] == (
        "could not resolve deployment manifest for microbench"
    )
    assert err["data"]["deployment"] == "sync-demo"
    assert "manifest boom" in err["data"]["error"]


def test_load_source_registry_repo_root_override_expands_repo_anchor(
    tmp_path: Path,
) -> None:
    dep = _write_microbench_deployment(tmp_path)
    sync_root = tmp_path / "sync-root"
    sync_root.mkdir()
    registry_path = dep / "source_registry.yaml"
    registry_path.write_text(
        registry_path.read_text(encoding="utf-8")
        .replace(
            f"        roots: [{dep}]\n",
            "        roots: ['${repo_root}']\n",
            1,
        )
        .replace(
            "      fixture_path:",
            "      repo_root: ${repo_root}\n      fixture_path:",
            1,
        ),
        encoding="utf-8",
    )

    registry = load_source_registry(registry_path, repo_root_override=sync_root)

    assert registry["docs"].raw["params"]["repo_root"] == str(sync_root)
    assert registry["docs"].sync.watch.roots == (str(sync_root),)


def test_source_sync_microbench_named_source_sets_exclude_policy_sources(
    tmp_path: Path,
) -> None:
    dep = _write_microbench_deployment(tmp_path)
    registry = load_source_registry(dep / "source_registry.yaml")
    calls = []

    def fake_run_ingest(options):
        calls.append(tuple(options.include or ()))
        return SimpleNamespace(
            any_failed=False,
            report={
                "results": [
                    {
                        "name": next(iter(options.include)),
                        "status": "OK",
                        "run_mode": "scope_incomplete",
                        "nodes_appended": 0,
                        "edges_appended": 0,
                        "retract_n": 0,
                        "retract_e": 0,
                        "timings_ms": {"source_runner": 1.0, "total": 2.0},
                        "source_stats": {},
                        "sync_scope": {},
                    }
                ],
                "publishes": [],
            },
        )

    automatic = build_source_sync_microbenchmark_report(
        deployment="sync-demo",
        deployment_dir=dep,
        registry=registry,
        dsn="postgres://unused",
        scenario="single-delta",
        sources="automatic",
        repeat=1,
        pipeline_modes=("source-only",),
        run_ingest_fn=fake_run_ingest,
    )
    policy = build_source_sync_microbenchmark_report(
        deployment="sync-demo",
        deployment_dir=dep,
        registry=registry,
        dsn="postgres://unused",
        scenario="single-delta",
        sources="policy-wave",
        repeat=1,
        pipeline_modes=("source-only",),
        run_ingest_fn=fake_run_ingest,
    )
    supporting = build_source_sync_microbenchmark_report(
        deployment="sync-demo",
        deployment_dir=dep,
        registry=registry,
        dsn="postgres://unused",
        scenario="single-delta",
        sources="supporting-wave",
        repeat=1,
        pipeline_modes=("source-only",),
        run_ingest_fn=fake_run_ingest,
    )

    assert automatic["sources"] == ["docs"]
    assert policy["sources"] == ["manual"]
    assert supporting["sources"] == ["docs"]
    assert ("docs",) in calls
    assert ("manual",) in calls


def test_source_sync_microbench_wave_sets_follow_readiness_not_static_hot_list(
    tmp_path: Path,
    monkeypatch,
) -> None:
    dep = _write_microbench_deployment(tmp_path)
    registry_path = dep / "source_registry.yaml"
    text = registry_path.read_text(encoding="utf-8")
    registry_path.write_text(
        text.replace(
            "  legacy:\n",
            "  package-manifests:\n"
            "    module: okg.substrate.library.sources.bootstrap_fixture\n"
            "    class: BootstrapFixtureSource\n"
            "    ownership_id: sync-demo.package-manifests\n"
            "    params:\n"
            f"      fixture_path: {dep / 'docs-fixture.json'}\n"
            "    source_class: discovery_crawl\n"
            "    record_identity_kind: scoped_locator\n"
            "    record_identity_fields: [path]\n"
            "    source_revision_kind: content_hash\n"
            "    deletion_semantics: missing_from_completed_scope\n"
            "    publication_mode: published_generation\n"
            "    sync:\n"
            "      watch:\n"
            f"        roots: [{dep}]\n"
            "        include_globs: ['docs/**/*.md']\n"
            "      triggers: [fs_event, manual, reconcile]\n"
            "      default_event_mode: scope_incomplete\n"
            "      reconcile_mode: scope_complete\n"
            "      scope:\n"
            "        accepts: [path]\n"
            "        unsupported_fallback: scope_complete\n"
            "  legacy:\n",
        ),
        encoding="utf-8",
    )
    registry = load_source_registry(registry_path)
    monkeypatch.setattr(
        "okg.substrate.source_sync_microbench.READY_SOURCES",
        frozenset({"docs"}),
    )
    calls = []

    def fake_run_ingest(options):
        calls.append(tuple(options.include or ()))
        return SimpleNamespace(
            any_failed=False,
            report={
                "results": [
                    {
                        "name": next(iter(options.include)),
                        "status": "OK",
                        "run_mode": "scope_incomplete",
                        "nodes_appended": 0,
                        "edges_appended": 0,
                        "retract_n": 0,
                        "retract_e": 0,
                        "timings_ms": {"source_runner": 1.0, "total": 2.0},
                        "source_stats": {},
                        "sync_scope": {},
                    }
                ],
                "publishes": [],
            },
        )

    hot = build_source_sync_microbenchmark_report(
        deployment="sync-demo",
        deployment_dir=dep,
        registry=registry,
        dsn="postgres://unused",
        scenario="single-delta",
        sources="hot-wave",
        repeat=1,
        pipeline_modes=("source-only",),
        run_ingest_fn=fake_run_ingest,
    )
    supporting = build_source_sync_microbenchmark_report(
        deployment="sync-demo",
        deployment_dir=dep,
        registry=registry,
        dsn="postgres://unused",
        scenario="single-delta",
        sources="supporting-wave",
        repeat=1,
        pipeline_modes=("source-only",),
        run_ingest_fn=fake_run_ingest,
    )

    assert hot["sources"] == ["docs"]
    assert supporting["sources"] == ["package-manifests"]
    assert ("docs",) in calls
    assert ("package-manifests",) in calls


def test_microbench_path_sampler_falls_back_to_configured_artifacts(
    tmp_path: Path,
) -> None:
    packets = tmp_path / "deployments" / "sync-demo" / "fixtures" / "memory"
    packets.mkdir(parents=True)
    (packets / "seed.jsonl").write_text("{}\n", encoding="utf-8")
    entry = SimpleNamespace(
        raw={
            "params": {
                "repo_root": str(tmp_path),
                "packet_dirs": ["${deployment_dir}/fixtures/memory"],
            }
        },
        ownership_id="sync-demo.agent-memory",
        sync=SimpleNamespace(
            scope=SimpleNamespace(accepts=("path",)),
            watch=SimpleNamespace(
                roots=(str(tmp_path),),
                include_globs=(".artifacts/agent_memory/**/*.jsonl",),
                exclude_globs=(),
            ),
        ),
    )

    plan = _microbench_event_plan(
        deployment="sync-demo",
        source_name="agent-memory",
        entry=entry,
        deployment_dir=tmp_path / "deployments" / "sync-demo",
        dsn="postgres://unused",
        scenario="single-delta",
        scope_key=None,
    )

    assert plan["scope_status"] == "sampled_path_scope_key"
    assert plan["scope_key"] == "seed.jsonl"
    assert plan["event_count"] == 1


def test_source_sync_microbench_no_change_samples_scoped_zero_delta(
    tmp_path: Path,
) -> None:
    dep = _write_microbench_deployment(tmp_path)
    subprocess.run(["git", "-C", str(dep), "init"], check=True)
    subprocess.run(["git", "-C", str(dep), "add", "docs/guide.md"], check=True)
    registry = load_source_registry(dep / "source_registry.yaml")
    calls = []

    def fake_run_ingest(options):
        calls.append(options)
        events = tuple(options.source_events.get("docs", ()))
        scope_keys = [event.scope_key for event in events if event.scope_key]
        return SimpleNamespace(
            any_failed=False,
            report={
                "timings_ms": {
                    "total": 75.0 if options.publish else 25.0,
                    "source_wall": 21.0,
                    "source_reported_total": 21.0,
                    "publish_wall": 35.0 if options.publish else 0.0,
                    "publish_reported_total": 35.0 if options.publish else 0.0,
                    "orchestration_overhead": 19.0 if options.publish else 4.0,
                },
                "results": [
                    {
                        "name": "docs",
                        "status": "OK",
                        "run_mode": "scope_incomplete",
                        "nodes_appended": 0,
                        "edges_appended": 0,
                        "retract_n": 0,
                        "retract_e": 0,
                        "timings_ms": {"source_runner": 2.0, "total": 5.0},
                        "source_stats": {},
                        "sync_scope": {
                            "source_name": "docs",
                            "event_count": len(events),
                            "scope_keys": scope_keys,
                        },
                    }
                ],
                "publishes": (
                    [
                        {
                            "status": "published",
                            "generation_id": "gen-1",
                            "timings_ms": {"total": 3.0},
                        }
                    ]
                    if options.publish
                    else []
                ),
            },
        )

    report = build_source_sync_microbenchmark_report(
        deployment="sync-demo",
        deployment_dir=dep,
        registry=registry,
        dsn="postgres://unused",
        scenario="no-change",
        sources=["docs"],
        repeat=1,
        pipeline_modes=("source-only", "full-pipeline"),
        run_ingest_fn=fake_run_ingest,
    )

    assert report["ok"] is True
    assert report["sample_count"] == 2
    assert report["row_count"] == 2
    assert [call.publish for call in calls] == [False, True]
    for row in report["rows"]:
        assert row.get("no_event_noop") is not True
        assert row["event_plan"]["event_count"] == 1
        assert row["event_plan"]["scope_key"] == "docs/guide.md"
        assert row["source_runner_ms"]["p95"] == 2.0
        assert row["rows_appended_total"] == 0


def test_microbench_unscoped_forces_manual_event(
    tmp_path: Path,
) -> None:
    dep = _write_microbench_deployment(tmp_path)
    registry = load_source_registry(dep / "source_registry.yaml")

    plan = _microbench_event_plan(
        deployment="sync-demo",
        source_name="docs",
        entry=registry["docs"],
        deployment_dir=dep,
        dsn="postgres://unused",
        scenario="no-change",
        scope_key=None,
        unscoped=True,
    )

    assert plan["scope_status"] == "forced_unscoped"
    assert plan["scope_kind"] == "none"
    assert plan["scope_key"] is None
    assert plan["event_count"] == 1
    assert plan["events"][0]["trigger_kind"] == "manual"
    assert plan["events"][0]["scope_key"] is None


def test_source_optimization_ledger_prioritizes_live_failures() -> None:
    inventory = {
        "schema": "okg.source_sync.microbench.inventory.v1",
        "deployment": "okg-workspace",
        "generated_at": "2026-06-05T00:00:00+00:00",
        "source_count": 4,
        "enricher_count": 1,
        "live_state": {"status": "loaded"},
        "coverage_counts": {"automatic_hot_path": 2, "useful_automatic": 2},
        "sources": [
            {
                "source_name": "package-manifests",
                "owner": "okg-workspace.package-manifests",
                "coverage_class": "useful_automatic",
                "readiness_required": False,
                "latest_source_runner_ms": 2106.0,
                "latest_nodes_appended": 0,
                "latest_edges_appended": 0,
                "latest_dedupe_drops_node": 14,
                "latest_dedupe_drops_edge": 14,
                "scope_support_status": "not_declared",
                "cursor_support_status": "completed_scope_requires_audit",
                "producer_status": "configured",
                "benchmark_scenarios": ["source-only", "full-pipeline"],
                "blocker": "scope_support_missing",
            },
            {
                "source_name": "code-structure",
                "owner": "okg-workspace.code-structure",
                "coverage_class": "automatic_hot_path",
                "readiness_required": True,
                "latest_source_runner_ms": 200.0,
                "latest_stage_total_ms": 2300.0,
                "latest_stage_source_state_snapshot_ms": 920.0,
                "latest_stage_source_runner_ms": 200.0,
                "latest_stage_admission_finalize_ms": 1900.0,
                "latest_nodes_appended": 0,
                "latest_edges_appended": 0,
                "latest_dedupe_drops_node": 0,
                "latest_dedupe_drops_edge": 0,
                "scope_support_status": "declared",
                "cursor_support_status": "completed_scope_requires_audit",
                "producer_status": "configured",
                "benchmark_scenarios": ["source-only", "full-pipeline"],
                "blocker": None,
            },
            {
                "source_name": "agent-sessions",
                "owner": "okg-workspace.agent-sessions",
                "coverage_class": "automatic_hot_path",
                "readiness_required": True,
                "latest_source_runner_ms": 51190.0,
                "latest_source_files_seen": 29,
                "latest_source_files_scanned": 4,
                "latest_source_files_scanned_full": 3,
                "latest_source_files_scanned_append_delta": 1,
                "latest_source_files_skipped_unchanged": 25,
                "latest_source_files_skipped_cwd_filter": 3,
                "latest_source_bytes_delta": 227828058,
                "latest_source_lines_delta": 55178,
                "latest_nodes_appended": 892,
                "latest_edges_appended": 42,
                "latest_dedupe_drops_node": 2,
                "latest_dedupe_drops_edge": 0,
                "scope_support_status": "declared",
                "cursor_support_status": "completed_scope_requires_audit",
                "producer_status": "configured",
                "benchmark_scenarios": ["source-only", "full-pipeline"],
                "blocker": None,
            },
            {
                "source_name": "doc-corpus",
                "owner": "okg-workspace.doc-corpus",
                "coverage_class": "useful_automatic",
                "readiness_required": False,
                "latest_source_runner_ms": None,
                "scope_support_status": "declared",
                "cursor_support_status": "completed_scope_requires_audit",
                "producer_status": "configured",
                "benchmark_scenarios": ["source-only", "full-pipeline"],
                "blocker": None,
            },
        ],
        "enrichers": [
            {
                "name": "ProfileHotspotLinker",
                "owner": "_enricher:ProfileHotspotLinker",
                "coverage_class": "source_backed_enricher",
                "scope_support_status": "needs_dirty_scope_audit",
                "benchmark_scenarios": ["full-pipeline"],
                "blocker": None,
            }
        ],
    }

    ledger = build_source_optimization_ledger(inventory)
    rows = {row["name"]: row for row in ledger["rows"]}

    assert ledger["schema"] == ("okg.source_sync.microbench.optimization_ledger.v1")
    assert ledger["row_count"] == 5
    assert rows["agent-sessions"]["priority_rank"] < rows["doc-corpus"]["priority_rank"]
    assert rows["agent-sessions"]["optimization_status"] == "failing_budget"
    assert rows["agent-sessions"]["amplification_signal"] == "high_runner_time"
    assert rows["agent-sessions"]["root_cause_class"] == ("high_source_runner_latency")
    assert rows["code-structure"]["amplification_signal"] == ("bounded_latest_sample")
    assert rows["code-structure"]["bottleneck_stage"] == ("admission.finalize")
    assert rows["code-structure"]["root_cause_class"] == ("ingest_stage_overhead")
    assert rows["code-structure"]["fix_decision"] == "eliminate_stage_overhead"
    assert rows["code-structure"]["latest_stage_total_ms"] == 2300.0
    assert rows["code-structure"]["latest_stage_source_runner_ms"] == 200.0
    assert rows["code-structure"]["latest_stage_admission_finalize_ms"] == 1900.0
    assert rows["package-manifests"]["optimization_status"] == ("blocked_inventory")
    assert rows["package-manifests"]["optimization_wave"] == ("supporting_wave")
    assert rows["package-manifests"]["amplification_signal"] == (
        "dedupe_drops_after_source_work"
    )
    assert rows["package-manifests"]["fix_decision"] == (
        "add_scope_support_or_reclassify"
    )
    assert rows["doc-corpus"]["optimization_status"] == "needs_benchmark"
    assert rows["ProfileHotspotLinker"]["optimization_wave"] == "enricher_wave"

    relation_rows = source_optimization_ledger_relation_rows(ledger)
    relation_by_key = {row["row_key"]: row for row in relation_rows}
    assert relation_by_key["source:agent-sessions"]["priority_rank"] == 1
    assert (
        relation_by_key["source:agent-sessions"]["latest_source_files_scanned_full"]
        == 3
    )
    assert (
        relation_by_key["source:agent-sessions"]["latest_source_bytes_delta"]
        == 227828058
    )
    assert relation_by_key["source:code-structure"]["latency_budget_ms"] == 1000
    assert relation_by_key["source:code-structure"]["latest_stage_total_ms"] == 2300.0
    assert (
        relation_by_key["source:code-structure"]["latest_stage_source_runner_ms"]
        == 200.0
    )


def test_source_optimization_ledger_keeps_manual_hot_list_sources_in_policy_wave() -> (
    None
):
    inventory = {
        "schema": "okg.source_sync.microbench.inventory.v1",
        "deployment": "okg-workspace",
        "generated_at": "2026-06-05T00:00:00+00:00",
        "source_count": 1,
        "enricher_count": 0,
        "live_state": {"status": "loaded"},
        "coverage_counts": {"manual_full_rebuild": 1},
        "sources": [
            {
                "source_name": "code-data-flow",
                "owner": "okg-workspace.code-data-flow",
                "coverage_class": "manual_full_rebuild",
                "readiness_required": False,
                "latest_source_runner_ms": 6033.718,
                "latest_stage_total_ms": 6335.819,
                "latest_stage_source_runner_ms": 6033.718,
                "latest_nodes_appended": 534,
                "latest_edges_appended": 2980,
                "latest_dedupe_drops_node": 3124,
                "latest_dedupe_drops_edge": 3103,
                "scope_support_status": "not_declared",
                "cursor_support_status": "completed_scope_requires_audit",
                "producer_status": "manual_only",
                "benchmark_scenarios": ["source-only"],
                "blocker": None,
            },
        ],
        "enrichers": [],
    }

    ledger = build_source_optimization_ledger(inventory)
    row = ledger["rows"][0]

    assert row["name"] == "code-data-flow"
    assert row["optimization_wave"] == "policy_wave"
    assert row["latency_status"] == "policy_classification_required"
    assert row["optimization_status"] == "confirm_policy"
    assert row["fix_decision"] == "confirm_manual_or_disabled_policy"


def test_source_optimization_ledger_reports_full_emission_zero_append() -> None:
    inventory = {
        "schema": "okg.source_sync.microbench.inventory.v1",
        "deployment": "okg-workspace",
        "generated_at": "2026-06-08T00:00:00+00:00",
        "source_count": 1,
        "enricher_count": 0,
        "live_state": {"status": "loaded"},
        "coverage_counts": {"automatic_hot_path": 1},
        "sources": [
            {
                "source_name": "pact",
                "owner": "okg-workspace.pact",
                "coverage_class": "automatic_hot_path",
                "readiness_required": True,
                "latest_source_runner_ms": 250.0,
                "latest_stage_total_ms": 300.0,
                "latest_source_facts_evaluated": 900,
                "latest_source_facts_emitted": 900,
                "latest_source_facts_filtered_unchanged": 0,
                "latest_nodes_appended": 0,
                "latest_edges_appended": 0,
                "latest_dedupe_drops_node": 0,
                "latest_dedupe_drops_edge": 0,
                "scope_support_status": "declared",
                "cursor_support_status": "completed_scope_requires_audit",
                "producer_status": "configured",
                "benchmark_scenarios": ["source-only", "full-pipeline"],
                "blocker": None,
            }
        ],
        "enrichers": [],
    }

    ledger = build_source_optimization_ledger(inventory)
    row = ledger["rows"][0]

    assert row["amplification_signal"] == "full_emission_zero_append"
    assert row["root_cause_class"] == "source_reemits_stable_facts"
    assert row["bottleneck_stage"] == "source_runner.full_emission_zero_append"
    assert row["fix_decision"] == "stop_reemitting_before_dedupe"
    relation_row = source_optimization_ledger_relation_rows(ledger)[0]
    assert relation_row["latest_source_facts_emitted"] == 900
    assert relation_row["latest_source_facts_filtered_unchanged"] == 0


def test_source_optimization_ledger_accepts_full_pipeline_proof() -> None:
    inventory = {
        "schema": "okg.source_sync.microbench.inventory.v1",
        "deployment": "okg-workspace",
        "generated_at": "2026-06-05T00:00:00+00:00",
        "source_count": 1,
        "enricher_count": 0,
        "live_state": {"status": "loaded"},
        "coverage_counts": {"automatic_hot_path": 1},
        "sources": [
            {
                "source_name": "pact",
                "owner": "okg-workspace.pact",
                "coverage_class": "automatic_hot_path",
                "readiness_required": True,
                "latest_source_runner_ms": 120.0,
                "latest_stage_total_ms": 240.0,
                "latest_nodes_appended": 1,
                "latest_edges_appended": 1,
                "latest_dedupe_drops_node": 0,
                "latest_dedupe_drops_edge": 0,
                "latest_full_pipeline_scenario": "single-delta",
                "latest_full_pipeline_ok": True,
                "latest_full_pipeline_latency_p95_ms": 825.0,
                "latest_full_pipeline_rows_appended": 2,
                "latest_full_pipeline_changed_graph_rows": 3,
                "latest_full_pipeline_expected_delta_source": "explicit",
                "latest_full_pipeline_expected_delta_units": 3.0,
                "latest_full_pipeline_observed_delta_units": 3.0,
                "latest_full_pipeline_source_work_units": 2.0,
                "latest_full_pipeline_work_amplification_ratio": 0.666667,
                "latest_full_pipeline_work_amplification_budget": 3.0,
                "latest_full_pipeline_observability_status": "pass",
                "latest_full_pipeline_observability_gaps": [],
                "latest_full_pipeline_top_widening_stage": "native_views",
                "latest_full_pipeline_top_widening_owner": "native_views",
                "latest_full_pipeline_top_widening_reason": (
                    "declared_view_dependency_scope"
                ),
                "latest_full_pipeline_top_widening_ms": 90.0,
                "latest_full_pipeline_lock_wait_p95_ms": 3900.0,
                "latest_full_pipeline_lock_hold_p95_ms": 120.0,
                "latest_full_pipeline_environment_contention": True,
                "latest_full_pipeline_environment_stage": "publisher.lock_acquire",
                "latest_full_pipeline_environment_reason": (
                    "concurrent_publisher_lock_contention"
                ),
                "latest_full_pipeline_readiness_impact": "within_budget",
                "latest_full_pipeline_artifact": "/tmp/microbench.json",
                "latest_full_pipeline_generated_at": ("2026-06-05T00:00:01+00:00"),
                "scope_support_status": "declared",
                "cursor_support_status": "completed_scope_requires_audit",
                "producer_status": "configured",
                "benchmark_scenarios": ["source-only", "full-pipeline"],
                "blocker": None,
            }
        ],
        "enrichers": [],
    }

    ledger = build_source_optimization_ledger(inventory)
    row = ledger["rows"][0]

    assert row["optimization_status"] == "passing_full_pipeline_benchmark"
    assert row["proof_status"] == "full_pipeline_benchmark_passed"
    assert row["next_action"] == "monitor_live_full_pipeline"
    assert row["latest_full_pipeline_latency_p95_ms"] == 825.0
    assert row["latest_full_pipeline_expected_delta_source"] == "explicit"
    assert row["latest_full_pipeline_expected_delta_units"] == 3.0
    assert row["latest_full_pipeline_observed_delta_units"] == 3.0
    assert row["latest_full_pipeline_work_amplification_ratio"] == 0.666667
    assert row["latest_full_pipeline_observability_status"] == "pass"
    relation_row = source_optimization_ledger_relation_rows(ledger)[0]
    assert relation_row["latest_full_pipeline_scenario"] == "single-delta"
    assert relation_row["latest_full_pipeline_changed_graph_rows"] == 3
    assert relation_row["latest_full_pipeline_expected_delta_source"] == "explicit"
    assert relation_row["latest_full_pipeline_expected_delta_units"] == 3.0
    assert relation_row["latest_full_pipeline_observed_delta_units"] == 3.0
    assert relation_row["latest_full_pipeline_source_work_units"] == 2.0
    assert relation_row["latest_full_pipeline_work_amplification_ratio"] == 0.666667
    assert relation_row["latest_full_pipeline_work_amplification_budget"] == 3.0
    assert relation_row["latest_full_pipeline_observability_status"] == "pass"
    assert relation_row["latest_full_pipeline_observability_gaps"] == []
    assert relation_row["latest_full_pipeline_top_widening_stage"] == ("native_views")
    assert relation_row["latest_full_pipeline_top_widening_owner"] == ("native_views")
    assert relation_row["latest_full_pipeline_top_widening_reason"] == (
        "declared_view_dependency_scope"
    )
    assert relation_row["latest_full_pipeline_lock_wait_p95_ms"] == 3900.0
    assert relation_row["latest_full_pipeline_lock_hold_p95_ms"] == 120.0
    assert relation_row["latest_full_pipeline_environment_contention"] is True
    assert relation_row["latest_full_pipeline_environment_stage"] == (
        "publisher.lock_acquire"
    )
    assert relation_row["latest_full_pipeline_environment_reason"] == (
        "concurrent_publisher_lock_contention"
    )


def test_source_optimization_ledger_carries_no_change_dominant_proof() -> None:
    inventory = {
        "schema": "okg.source_sync.microbench.inventory.v1",
        "deployment": "okg-workspace",
        "generated_at": "2026-06-05T00:00:00+00:00",
        "source_count": 1,
        "enricher_count": 0,
        "live_state": {"status": "loaded"},
        "coverage_counts": {"automatic_hot_path": 1},
        "sources": [{
            "source_name": "git-files",
            "owner": "okg-workspace.git-files",
            "coverage_class": "automatic_hot_path",
            "readiness_required": True,
            "latest_source_runner_ms": 42.0,
            "latest_stage_total_ms": 83.0,
            "latest_nodes_appended": 0,
            "latest_edges_appended": 0,
            "latest_dedupe_drops_node": 0,
            "latest_dedupe_drops_edge": 0,
            "latest_full_pipeline_scenario": "single-delta",
            "latest_full_pipeline_ok": True,
            "latest_full_pipeline_latency_p95_ms": 725.0,
            "latest_full_pipeline_rows_appended": 0,
            "latest_full_pipeline_changed_graph_rows": 0,
            "latest_full_pipeline_expected_delta_source": "harness_declared",
            "latest_full_pipeline_expected_delta_units": 0.0,
            "latest_full_pipeline_observed_delta_units": 0.0,
            "latest_full_pipeline_source_work_units": 0.0,
            "latest_full_pipeline_work_amplification_ratio": None,
            "latest_full_pipeline_work_amplification_budget": 3.0,
            "latest_full_pipeline_observability_status": "pass",
            "latest_full_pipeline_observability_gaps": [],
            "latest_full_pipeline_no_change_dominant": True,
            "latest_full_pipeline_readiness_impact": "within_budget",
            "latest_full_pipeline_artifact": "/tmp/microbench.json",
            "scope_support_status": "declared",
            "cursor_support_status": "incremental_verified",
            "producer_status": "configured",
            "benchmark_scenarios": ["source-only", "full-pipeline"],
            "blocker": None,
        }],
        "enrichers": [],
    }

    ledger = build_source_optimization_ledger(inventory)
    row = ledger["rows"][0]
    relation_row = source_optimization_ledger_relation_rows(ledger)[0]

    assert row["optimization_status"] == "passing_full_pipeline_benchmark"
    assert row["latest_full_pipeline_no_change_dominant"] is True
    assert relation_row["latest_full_pipeline_no_change_dominant"] is True


@pytest.mark.postgres
def test_full_pipeline_progress_dirties_ledger_view(
    clean_runtime: str,
) -> None:
    gen = 201_337
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _register_ledger_artifact_definitions(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.graph_generations (
                  generation_id, deployment_name, status, catalog_version_id,
                  published_at, correlation_id
                ) VALUES (
                  %(generation_id)s, 'okg-workspace', 'published', 1,
                  now(), 'test:microbench-ledger-dirty'
                )
                """,
                {"generation_id": gen},
            )

    _persist_microbench_full_pipeline_progress(
        dsn=clean_runtime,
        deployment="okg-workspace",
        generated_at="2026-06-07T00:00:00+00:00",
        artifact="/tmp/proof.json",
        rows=[
            {
                "source_name": "agent-sessions",
                "scenario": "single-delta",
                "pipeline_mode": "full-pipeline",
                "ok": True,
                "sample_count": 1,
                "latency_ms": {"p95": 750.0},
                "source_runner_ms": {"p95": 25.0},
                "pipeline_overhead_ms": {"p95": 725.0},
                "rows_appended_total": 2,
                "nodes_appended_total": 1,
                "edges_appended_total": 1,
                "publish_scope": {
                    "changed_graph_rows": 2,
                    "top_widening_stage": "native_views",
                    "top_widening_owner": "native_views",
                    "top_widening_reason": "declared_view_dependency_scope",
                    "top_widening_elapsed_p95_ms": 50.0,
                    "readiness_impact": "within_budget",
                },
                "event_plan": {
                    "scope_status": "provided_scope_key",
                    "scope_key": "a.md",
                },
            }
        ],
    )

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT source_name, value
              FROM okg.source_progress
             WHERE deployment = 'okg-workspace'
               AND key = %(key)s
            """,
            {"key": MICROBENCH_FULL_PIPELINE_PROGRESS_KEY},
        )
        progress = cur.fetchall()
        assert [row[0] for row in progress] == [
            "okg-workspace.agent-sessions",
        ]
        progress_payload = json.loads(progress[0][1])
        assert progress_payload["schema"] == (
            "okg.source_sync.microbench.full_pipeline.last_run.v1"
        )
        assert progress_payload["top_widening_owner"] == "native_views"
        assert progress_payload["top_widening_reason"] == (
            "declared_view_dependency_scope"
        )
        cur.execute(
            """
            SELECT definition_id, target_generation_id, scope_key,
                   dirty_reason, dependency_kind, dependency_ref,
                   consumed_run_id
              FROM okg.derived_dirty_scopes
             WHERE definition_id IN (
               %(view_definition_id)s, %(relation_definition_id)s
             )
             ORDER BY definition_id, scope_key
            """,
            {
                "view_definition_id": MICROBENCH_LEDGER_VIEW_DEFINITION_ID,
                "relation_definition_id": (MICROBENCH_LEDGER_RELATION_DEFINITION_ID),
            },
        )
        rows = cur.fetchall()

    assert set(rows) == {
        (
            MICROBENCH_LEDGER_VIEW_DEFINITION_ID,
            gen,
            f"publisher:view:{MICROBENCH_LEDGER_VIEW_ID}",
            MICROBENCH_LEDGER_DIRTY_REASON,
            MICROBENCH_LEDGER_DIRTY_DEPENDENCY_KIND,
            (f"source_progress:okg-workspace:{MICROBENCH_FULL_PIPELINE_PROGRESS_KEY}"),
            None,
        ),
        (
            MICROBENCH_LEDGER_RELATION_DEFINITION_ID,
            gen,
            "agent-sessions",
            MICROBENCH_LEDGER_DIRTY_REASON,
            MICROBENCH_LEDGER_DIRTY_DEPENDENCY_KIND,
            (
                "source_progress:okg-workspace.agent-sessions:"
                f"{MICROBENCH_FULL_PIPELINE_PROGRESS_KEY}"
            ),
            None,
        ),
    }


def test_sync_microbench_ledger_cli_emits_optimization_rows(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_microbench_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))

    rc = main(
        [
            "--json",
            "sync",
            "microbench",
            "ledger",
            "--deployment",
            "sync-demo",
            "--no-live",
        ]
    )

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["schema"] == "okg.source_sync.microbench.optimization_ledger.v1"
    assert out["row_count"] == 4
    rows = {row["name"]: row for row in out["rows"]}
    assert rows["docs"]["optimization_status"] == "needs_benchmark"
    assert rows["manual"]["optimization_status"] == "confirm_policy"
    assert rows["legacy"]["optimization_status"] == "blocked_inventory"


def test_source_sync_microbench_report_runs_real_ingest_contract(
    tmp_path: Path,
) -> None:
    dep = _write_microbench_deployment(tmp_path)
    registry = load_source_registry(dep / "source_registry.yaml")
    calls = []

    def fake_run_ingest(options):
        calls.append(options)
        events = tuple(options.source_events.get("docs", ()))
        scope_keys = [event.scope_key for event in events if event.scope_key]
        timings = {
            "admission_prepare": 1.0,
            "import": 2.0,
            "construct": 3.0,
            "source_state_snapshot": 4.0,
            "source_runner": 5.0,
            "admission_finalize": 6.0,
            "total": 21.0,
        }
        return SimpleNamespace(
            any_failed=False,
            report={
                "timings_ms": {
                    "total": 75.0 if options.publish else 25.0,
                    "source_wall": 21.0,
                    "source_reported_total": 21.0,
                    "publish_wall": 35.0 if options.publish else 0.0,
                    "publish_reported_total": 35.0 if options.publish else 0.0,
                    "orchestration_overhead": 19.0 if options.publish else 4.0,
                },
                "results": [
                    {
                        "name": "docs",
                        "status": "OK",
                        "run_mode": "scope_incomplete",
                        "nodes_appended": 1,
                        "edges_appended": 2,
                        "retract_n": 0,
                        "retract_e": 0,
                        "timings_ms": timings,
                        "source_stats": {
                            "files_seen": 1,
                            "files_scanned": 1,
                            "bytes_delta": 12,
                        },
                        "sync_scope": {
                            "source_name": "docs",
                            "event_count": len(events),
                            "scope_keys": scope_keys,
                        },
                    }
                ],
                "publishes": (
                    [
                        {
                            "status": "published",
                            "generation_id": "gen-1",
                            "nodes_added": 1,
                            "edges_added": 2,
                            "timings_ms": {
                                "consistency_gates": 7.0,
                                "native_views": 11.0,
                                "checkpoint_generation": 13.0,
                                "total": 35.0,
                            },
                        }
                    ]
                    if options.publish
                    else []
                ),
            },
        )

    report = build_source_sync_microbenchmark_report(
        deployment="sync-demo",
        deployment_dir=dep,
        registry=registry,
        dsn="postgres://unused",
        scenario="single-delta",
        sources=["docs"],
        repeat=1,
        pipeline_modes=("source-only", "full-pipeline"),
        run_ingest_fn=fake_run_ingest,
        expected_delta={"nodes_added": 1, "edges_added": 2},
        output_path=tmp_path / "microbench.json",
    )

    assert report["schema"] == "okg.source_sync.microbench.run.v1"
    assert report["ok"] is True
    assert report["row_count"] == 2
    assert report["sample_count"] == 2
    assert report["artifact"].endswith("microbench.json")
    assert Path(report["artifact"]).is_file()
    assert report["expected_delta"] == {"nodes_added": 1, "edges_added": 2}
    assert report["expected_delta_source"] == "explicit"
    assert [call.publish for call in calls] == [False, True]
    assert calls[0].include == {"docs"}
    assert calls[0].include_predecessors is False
    assert calls[0].source_events["docs"][0].scope_key == "docs/guide.md"
    row_by_mode = {row["pipeline_mode"]: row for row in report["rows"]}
    assert row_by_mode["source-only"]["event_plan"]["scope_kind"] == "path"
    assert row_by_mode["source-only"]["event_plan"]["scope_key"] == ("docs/guide.md")
    assert row_by_mode["full-pipeline"]["nodes_appended_total"] == 1
    assert row_by_mode["full-pipeline"]["edges_appended_total"] == 2
    assert row_by_mode["full-pipeline"]["expected_delta"] == {
        "nodes_added": 1,
        "edges_added": 2,
    }
    assert row_by_mode["full-pipeline"]["expected_delta_source"] == "explicit"
    assert row_by_mode["full-pipeline"]["expected_delta_units"] == 3.0
    assert row_by_mode["full-pipeline"]["observed_delta_units"] == 3.0
    assert row_by_mode["full-pipeline"]["source_work_units"] == 3.0
    assert row_by_mode["full-pipeline"]["work_amplification_ratio"] == 1.0
    assert row_by_mode["full-pipeline"]["work_amplification_budget"] == 3.0
    assert row_by_mode["full-pipeline"]["observability_status"] == "pass"
    assert row_by_mode["full-pipeline"]["observability_gaps"] == []
    assert row_by_mode["full-pipeline"]["stage_p95_ms"]["source_runner"] == 5.0
    assert (
        row_by_mode["full-pipeline"]["publish_stage_p95_ms"]["consistency_gates"] == 7.0
    )
    assert row_by_mode["full-pipeline"]["publish_stage_p95_ms"]["native_views"] == 11.0
    assert row_by_mode["full-pipeline"]["publish_unattributed_ms"]["p95"] == 4.0
    assert report["samples"][1]["publish_timings_ms"]["checkpoint_generation"] == 13.0
    assert report["samples"][1]["publish_unattributed_ms"] == 4.0
    assert report["samples"][1]["ingest_timings_ms"]["publish_wall"] == 35.0
    assert report["samples"][1]["pipeline_overhead_ms"] == 19.0


def test_source_sync_microbench_uses_harness_declared_expected_delta(
    tmp_path: Path,
) -> None:
    dep = _write_microbench_deployment(tmp_path)
    registry_path = dep / "source_registry.yaml"
    registry_path.write_text(
        registry_path.read_text(encoding="utf-8").replace(
            "    sync:\n",
            "    microbench:\n"
            "      expected_delta:\n"
            "        single-delta:\n"
            "          nodes_added: 1\n"
            "          edges_added: 2\n"
            "    sync:\n",
            1,
        ),
        encoding="utf-8",
    )
    registry = load_source_registry(registry_path)
    output_path = tmp_path / "harness-declared-microbench.json"
    sync_root = tmp_path / "sync-root"

    def fake_run_ingest(options):
        assert options.source_registry_repo_root_override == sync_root
        return SimpleNamespace(
            any_failed=False,
            report={
                "timings_ms": {
                    "total": 75.0,
                    "source_wall": 21.0,
                    "source_reported_total": 21.0,
                    "publish_wall": 35.0,
                    "publish_reported_total": 35.0,
                    "orchestration_overhead": 19.0,
                },
                "results": [
                    {
                        "name": "docs",
                        "status": "OK",
                        "run_mode": "scope_incomplete",
                        "nodes_appended": 1,
                        "edges_appended": 2,
                        "timings_ms": {
                            "source_runner": 5.0,
                            "total": 21.0,
                        },
                        "source_stats": {},
                        "sync_scope": {
                            "source_name": "docs",
                            "event_count": len(options.source_events.get("docs", ())),
                            "scope_keys": ["docs/guide.md"],
                        },
                    }
                ],
                "publishes": [
                    {
                        "status": "published",
                        "generation_id": "gen-1",
                        "nodes_added": 1,
                        "edges_added": 2,
                        "timings_ms": {"total": 35.0},
                    }
                ],
            },
        )

    report = build_source_sync_microbenchmark_report(
        deployment="sync-demo",
        deployment_dir=dep,
        registry=registry,
        dsn="postgres://unused",
        scenario="single-delta",
        sources=["docs"],
        repeat=1,
        pipeline_modes=("full-pipeline",),
        run_ingest_fn=fake_run_ingest,
        source_registry_repo_root_override=sync_root,
        output_path=output_path,
    )

    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    assert report["artifact"] == str(output_path)
    assert artifact["artifact"] == str(output_path)
    assert artifact["rows"][0]["expected_delta_source"] == "harness_declared"
    assert report["expected_delta"] == {}
    assert report["expected_delta_source"] == "harness_declared"
    assert report["declared_expected_delta_by_source"] == {
        "docs": {"nodes_added": 1, "edges_added": 2},
    }
    row = report["rows"][0]
    assert row["expected_delta"] == {"nodes_added": 1, "edges_added": 2}
    assert row["expected_delta_source"] == "harness_declared"
    assert row["expected_delta_units"] == 3.0
    assert row["observed_delta_units"] == 3.0
    assert row["observability_status"] == "pass"
    assert row["observability_gaps"] == []


def test_source_sync_microbench_quarantines_all_source_full_pipeline(
    tmp_path: Path,
    monkeypatch,
) -> None:
    dep = _write_microbench_deployment(tmp_path)
    registry = load_source_registry(dep / "source_registry.yaml")
    calls = []
    monkeypatch.setattr(
        "okg.substrate.source_sync_microbench.FULL_PIPELINE_ALL_SOURCE_QUARANTINE",
        frozenset({"docs"}),
    )

    def fake_run_ingest(options):
        calls.append((tuple(options.include or ()), options.publish))
        return SimpleNamespace(
            any_failed=False,
            report={
                "results": [
                    {
                        "name": next(iter(options.include)),
                        "status": "OK",
                        "run_mode": "scope_incomplete",
                        "nodes_appended": 0,
                        "edges_appended": 0,
                        "retract_n": 0,
                        "retract_e": 0,
                        "timings_ms": {"source_runner": 1.0, "total": 2.0},
                        "source_stats": {},
                        "sync_scope": {},
                    }
                ],
                "publishes": (
                    [{"status": "published", "generation_id": "gen-1"}]
                    if options.publish
                    else []
                ),
            },
        )

    report = build_source_sync_microbenchmark_report(
        deployment="sync-demo",
        deployment_dir=dep,
        registry=registry,
        dsn="postgres://unused",
        scenario="single-delta",
        sources="all",
        repeat=1,
        pipeline_modes=("source-only", "full-pipeline"),
        run_ingest_fn=fake_run_ingest,
    )

    docs_full = [
        row
        for row in report["rows"]
        if row["source_name"] == "docs" and row["pipeline_mode"] == "full-pipeline"
    ][0]
    assert docs_full["skipped"] is True
    assert docs_full["skip_reason"] == "quarantined_full_pipeline_all_source"
    assert report["skipped_row_count"] == 1
    assert (("docs",), True) not in calls
    assert (("docs",), False) in calls


def test_sync_microbench_cli_runs_default_benchmark_command(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_microbench_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    monkeypatch.setattr(
        "okg.substrate.cli.main._resolve_dsn",
        lambda args: "postgres://unused",
    )
    monkeypatch.setattr(
        "okg.substrate.cli.main._deployment_runtime_enabled",
        lambda deployment: False,
    )

    captured = {}

    def fake_build_report(**kwargs):
        captured.update(kwargs)
        return {
            "schema": "okg.source_sync.microbench.run.v1",
            "deployment": kwargs["deployment"],
            "scenario": kwargs["scenario"],
            "ok": True,
            "rows": [],
        }

    monkeypatch.setattr(
        "okg.substrate.source_sync_microbench.build_source_sync_microbenchmark_report",
        fake_build_report,
    )

    rc = main(
        [
            "--json",
            "sync",
            "microbench",
            "--deployment",
            "sync-demo",
            "--scenario",
            "single-delta",
            "--sources",
            "docs",
            "--repeat",
            "3",
            "--pipeline-mode",
            "source-only",
            "--expected-delta-json",
            '{"nodes_added":1,"edges_added":2}',
            "--unscoped",
        ]
    )

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["schema"] == "okg.source_sync.microbench.run.v1"
    assert captured["deployment"] == "sync-demo"
    assert captured["scenario"] == "single-delta"
    assert captured["sources"] == "docs"
    assert captured["repeat"] == 3
    assert captured["pipeline_modes"] == ("source-only",)
    assert captured["expected_delta"] == {"nodes_added": 1, "edges_added": 2}
    assert captured["unscoped"] is True


def test_sync_microbench_gate_cli_accepts_under_budget_rows(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_microbench_deployment(tmp_path)
    output_path = tmp_path / "gate.json"
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    monkeypatch.setattr(
        "okg.substrate.cli.main._resolve_dsn",
        lambda args: "postgres://unused",
    )
    monkeypatch.setattr(
        "okg.substrate.cli.main._deployment_runtime_enabled",
        lambda deployment: False,
    )

    captured = {}

    def fake_build_report(**kwargs):
        captured.update(kwargs)
        return {
            "schema": "okg.source_sync.microbench.run.v1",
            "deployment": kwargs["deployment"],
            "scenario": kwargs["scenario"],
            "ok": True,
            "rows": [
                {
                    "source_name": "docs",
                    "pipeline_mode": "full-pipeline",
                    "ok": True,
                    "sample_count": 1,
                    "latency_ms": {"p95": 750.0},
                    **_gate_observability_pass(),
                }
            ],
        }

    monkeypatch.setattr(
        "okg.substrate.source_sync_microbench.build_source_sync_microbenchmark_report",
        fake_build_report,
    )

    rc = main(
        [
            "sync",
            "microbench",
            "gate",
            "--deployment",
            "sync-demo",
            "--scenario",
            "no-change",
            "--sources",
            "docs",
            "--pipeline-mode",
            "full-pipeline",
            "--max-p95-ms",
            "1000",
            "--expected-delta-json",
            '{"nodes_added":1}',
            "--output",
            str(output_path),
            "--json",
        ]
    )

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is True
    assert out["gate"]["ok"] is True
    assert out["gate"]["checked_row_count"] == 1
    assert out["gate"]["violation_count"] == 0
    assert captured["expected_delta"] == {"nodes_added": 1}
    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["gate"]["ok"] is True


def test_sync_microbench_gate_cli_fails_observability_gaps(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_microbench_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    monkeypatch.setattr(
        "okg.substrate.cli.main._resolve_dsn",
        lambda args: "postgres://unused",
    )
    monkeypatch.setattr(
        "okg.substrate.cli.main._deployment_runtime_enabled",
        lambda deployment: False,
    )

    def fake_build_report(**kwargs):
        return {
            "schema": "okg.source_sync.microbench.run.v1",
            "deployment": kwargs["deployment"],
            "scenario": kwargs["scenario"],
            "ok": True,
            "rows": [
                {
                    "source_name": "docs",
                    "pipeline_mode": "full-pipeline",
                    "ok": True,
                    "sample_count": 1,
                    "latency_ms": {"p95": 750.0},
                    "expected_delta_source": "missing_or_empty",
                    "expected_delta_units": None,
                    "observed_delta_units": 1.0,
                    "source_work_units": 1.0,
                    "work_amplification_ratio": 1.0,
                    "work_amplification_budget": 3.0,
                    "observability_status": "fail",
                    "observability_gaps": ["missing_expected_delta"],
                }
            ],
        }

    monkeypatch.setattr(
        "okg.substrate.source_sync_microbench.build_source_sync_microbenchmark_report",
        fake_build_report,
    )

    rc = main(
        [
            "sync",
            "microbench",
            "gate",
            "--deployment",
            "sync-demo",
            "--sources",
            "docs",
            "--pipeline-mode",
            "full-pipeline",
            "--max-p95-ms",
            "1000",
            "--json",
        ]
    )

    assert rc == 1
    out = json.loads(capsys.readouterr().out)
    assert out["gate"]["violations"] == [
        {
            "source_name": "docs",
            "pipeline_mode": "full-pipeline",
            "reason": "missing_expected_delta",
            "observability_status": "fail",
            "observability_gaps": ["missing_expected_delta"],
            "expected_delta_source": "missing_or_empty",
            "expected_delta_units": None,
            "observed_delta_units": 1.0,
            "source_work_units": 1.0,
            "work_amplification_ratio": 1.0,
            "work_amplification_budget": 3.0,
        }
    ]


def test_sync_microbench_gate_cli_fails_over_budget_rows(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_microbench_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    monkeypatch.setattr(
        "okg.substrate.cli.main._resolve_dsn",
        lambda args: "postgres://unused",
    )
    monkeypatch.setattr(
        "okg.substrate.cli.main._deployment_runtime_enabled",
        lambda deployment: False,
    )

    def fake_build_report(**kwargs):
        return {
            "schema": "okg.source_sync.microbench.run.v1",
            "deployment": kwargs["deployment"],
            "scenario": kwargs["scenario"],
            "ok": True,
            "rows": [
                {
                    "source_name": "docs",
                    "pipeline_mode": "full-pipeline",
                    "ok": True,
                    "sample_count": 1,
                    "latency_ms": {"p95": 1200.0},
                    **_gate_observability_pass(),
                }
            ],
        }

    monkeypatch.setattr(
        "okg.substrate.source_sync_microbench.build_source_sync_microbenchmark_report",
        fake_build_report,
    )

    rc = main(
        [
            "sync",
            "microbench",
            "gate",
            "--deployment",
            "sync-demo",
            "--sources",
            "docs",
            "--pipeline-mode",
            "full-pipeline",
            "--max-p95-ms",
            "1000",
            "--json",
        ]
    )

    assert rc == 1
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is False
    assert out["gate"]["ok"] is False
    assert out["gate"]["violations"] == [
        {
            "source_name": "docs",
            "pipeline_mode": "full-pipeline",
            "reason": "p95_budget_exceeded",
            "p95_ms": 1200.0,
            "max_p95_ms": 1000.0,
        }
    ]


def test_sync_microbench_gate_cli_fails_no_event_rows(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_microbench_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    monkeypatch.setattr(
        "okg.substrate.cli.main._resolve_dsn",
        lambda args: "postgres://unused",
    )
    monkeypatch.setattr(
        "okg.substrate.cli.main._deployment_runtime_enabled",
        lambda deployment: False,
    )

    def fake_build_report(**kwargs):
        return {
            "schema": "okg.source_sync.microbench.run.v1",
            "deployment": kwargs["deployment"],
            "scenario": kwargs["scenario"],
            "ok": True,
            "rows": [
                {
                    "source_name": "docs",
                    "pipeline_mode": "full-pipeline",
                    "ok": True,
                    "sample_count": 0,
                    "no_event_noop": True,
                    "event_plan": {"scope_status": "missing_path_scope_sample"},
                    "latency_ms": {"p95": 0.0},
                }
            ],
        }

    monkeypatch.setattr(
        "okg.substrate.source_sync_microbench.build_source_sync_microbenchmark_report",
        fake_build_report,
    )

    rc = main(
        [
            "sync",
            "microbench",
            "gate",
            "--deployment",
            "sync-demo",
            "--sources",
            "docs",
            "--pipeline-mode",
            "full-pipeline",
            "--json",
        ]
    )

    assert rc == 1
    out = json.loads(capsys.readouterr().out)
    assert out["gate"]["violations"] == [
        {
            "source_name": "docs",
            "pipeline_mode": "full-pipeline",
            "reason": "no_real_samples",
            "sample_count": 0,
            "scope_status": "missing_path_scope_sample",
        }
    ]


def test_sync_microbench_gate_cli_blocks_when_source_sync_not_idle(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_microbench_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    monkeypatch.setattr(
        "okg.substrate.cli.main._resolve_dsn",
        lambda args: "postgres://unused",
    )
    monkeypatch.setattr(
        "okg.substrate.cli.main._deployment_runtime_enabled",
        lambda deployment: True,
    )
    monkeypatch.setattr(
        "okg.substrate.cli.main._wait_for_idle_source_sync_runtime",
        lambda *, deployment, dsn, wait_seconds: {
            "ok": False,
            "reason": "source_sync_not_idle",
            "deployment": deployment,
            "wait_seconds": wait_seconds,
            "latest": {
                "queue_name": "okg-sync-demo-source-sync",
                "active_count": 1,
                "active_rows": [{"status": "PENDING", "count": 1}],
            },
        },
    )

    def fail_build_report(**kwargs):  # noqa: ARG001
        raise AssertionError("gate should stop before sampling")

    monkeypatch.setattr(
        "okg.substrate.source_sync_microbench.build_source_sync_microbenchmark_report",
        fail_build_report,
    )

    rc = main(
        [
            "sync",
            "microbench",
            "gate",
            "--deployment",
            "sync-demo",
            "--sources",
            "docs",
            "--pipeline-mode",
            "full-pipeline",
            "--idle-source-sync-wait-seconds",
            "0",
            "--json",
        ]
    )

    assert rc == 1
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is False
    assert out["sample_count"] == 0
    assert out["gate"]["violations"][0]["reason"] == "source_sync_not_idle"
    assert out["gate"]["violations"][0]["environment"] is True


def test_sync_microbench_gate_cli_classifies_publish_lock_contention(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_microbench_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    monkeypatch.setattr(
        "okg.substrate.cli.main._resolve_dsn",
        lambda args: "postgres://unused",
    )
    monkeypatch.setattr(
        "okg.substrate.cli.main._deployment_runtime_enabled",
        lambda deployment: False,
    )

    def fake_build_report(**kwargs):
        return {
            "schema": "okg.source_sync.microbench.run.v1",
            "deployment": kwargs["deployment"],
            "scenario": kwargs["scenario"],
            "ok": True,
            "rows": [
                {
                    "source_name": "docs",
                    "pipeline_mode": "full-pipeline",
                    "ok": True,
                    "sample_count": 1,
                    "latency_ms": {"p95": 4200.0},
                    "publish_stage_p95_ms": {"locked_connection": 3900.0},
                    **_gate_observability_pass(),
                }
            ],
        }

    monkeypatch.setattr(
        "okg.substrate.source_sync_microbench.build_source_sync_microbenchmark_report",
        fake_build_report,
    )

    rc = main(
        [
            "sync",
            "microbench",
            "gate",
            "--deployment",
            "sync-demo",
            "--sources",
            "docs",
            "--pipeline-mode",
            "full-pipeline",
            "--max-p95-ms",
            "1000",
            "--json",
        ]
    )

    assert rc == 1
    out = json.loads(capsys.readouterr().out)
    assert out["gate"]["violations"] == [
        {
            "source_name": "docs",
            "pipeline_mode": "full-pipeline",
            "reason": "publisher_lock_contention",
            "environment": True,
            "p95_ms": 4200.0,
            "max_p95_ms": 1000.0,
            "lock_wait_p95_ms": 3900.0,
        }
    ]


def test_microbench_agent_sessions_samples_warm_cursor_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "claude"
    transcript = root / "project" / "session.jsonl"
    transcript.parent.mkdir(parents=True)
    transcript.write_text('{"a": 1}\n{"b": 2}\n', encoding="utf-8")
    entry = SimpleNamespace(
        ownership_id="sync-demo.agent-sessions",
        sync=SimpleNamespace(
            scope=SimpleNamespace(accepts=("path",)),
            watch=SimpleNamespace(
                roots=(str(root),),
                include_globs=("**/*.jsonl",),
                exclude_globs=(),
            ),
        ),
    )

    monkeypatch.setattr(
        "okg.substrate.source_sync_microbench._load_source_watermark_cursor",
        lambda dsn, source_names: {
            "project/session.jsonl": {
                "append_cursor": {
                    "schema": "okg.agent_sessions.append_cursor.v1",
                    "append_safe": True,
                    "bytes": 9,
                    "last_line_hash": "hash",
                }
            }
        },
    )

    plan = _microbench_event_plan(
        deployment="sync-demo",
        source_name="agent-sessions",
        entry=entry,
        deployment_dir=tmp_path,
        dsn="postgres://unused",
        scenario="single-delta",
        scope_key=None,
    )

    assert plan["scope_status"] == "sampled_warm_cursor_scope_key"
    assert plan["scope_key"] == "project/session.jsonl"
    assert plan["event_count"] == 1


def test_microbench_agent_sessions_no_change_samples_matching_cursor_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "claude"
    stale = root / "project" / "stale.jsonl"
    idle = root / "project" / "idle.jsonl"
    stale.parent.mkdir(parents=True)
    stale.write_text('{"a": 1}\n{"b": 2}\n', encoding="utf-8")
    idle.write_text('{"a": 1}\n', encoding="utf-8")
    stale_stat = stale.stat()
    idle_stat = idle.stat()
    entry = SimpleNamespace(
        ownership_id="sync-demo.agent-sessions",
        sync=SimpleNamespace(
            scope=SimpleNamespace(accepts=("path",)),
            watch=SimpleNamespace(
                roots=(str(root),),
                include_globs=("**/*.jsonl",),
                exclude_globs=(),
            ),
        ),
    )

    monkeypatch.setattr(
        "okg.substrate.source_sync_microbench._load_source_watermark_cursor",
        lambda dsn, source_names: {
            "project/stale.jsonl": {
                "append_cursor": {
                    "schema": "okg.agent_sessions.append_cursor.v1",
                    "append_safe": True,
                    "bytes": 9,
                    "mtime_ns": stale_stat.st_mtime_ns,
                    "last_line_hash": "hash",
                }
            },
            "project/idle.jsonl": {
                "append_cursor": {
                    "schema": "okg.agent_sessions.append_cursor.v1",
                    "append_safe": True,
                    "bytes": idle_stat.st_size,
                    "mtime_ns": idle_stat.st_mtime_ns,
                    "last_line_hash": "hash",
                }
            },
        },
    )

    plan = _microbench_event_plan(
        deployment="sync-demo",
        source_name="agent-sessions",
        entry=entry,
        deployment_dir=tmp_path,
        dsn="postgres://unused",
        scenario="no-change",
        scope_key=None,
    )

    assert plan["scope_status"] == "sampled_warm_cursor_scope_key"
    assert plan["scope_key"] == "project/idle.jsonl"
    assert plan["event_count"] == 1


def test_microbench_agent_sessions_zero_expected_single_delta_samples_no_change(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "claude"
    workspace = Path("/home/example/projects/workspace")
    unrelated = root / "-home-example-projects-other" / "idle.jsonl"
    stale = root / "-home-example-projects-workspace" / "stale.jsonl"
    idle = root / "-home-example-projects-workspace" / "idle.jsonl"
    stale.parent.mkdir(parents=True)
    unrelated.parent.mkdir(parents=True)
    unrelated.write_text('{"z": 1}\n', encoding="utf-8")
    stale.write_text('{"a": 1}\n{"b": 2}\n', encoding="utf-8")
    idle.write_text('{"a": 1}\n', encoding="utf-8")
    unrelated_stat = unrelated.stat()
    stale_stat = stale.stat()
    idle_stat = idle.stat()
    entry = SimpleNamespace(
        ownership_id="sync-demo.agent-sessions",
        raw={
            "params": {"cwd_filter": [str(workspace)]},
            "microbench": {
                "expected_delta": {
                    "single-delta": {
                        "nodes_updated": 0,
                        "edges_updated": 0,
                    }
                }
            }
        },
        sync=SimpleNamespace(
            scope=SimpleNamespace(accepts=("path",)),
            watch=SimpleNamespace(
                roots=(str(root),),
                include_globs=("**/*.jsonl",),
                exclude_globs=(),
            ),
        ),
    )

    monkeypatch.setattr(
        "okg.substrate.source_sync_microbench._load_source_watermark_cursor",
        lambda dsn, source_names: {
            "-home-example-projects-other/idle.jsonl": {
                "append_cursor": {
                    "schema": "okg.agent_sessions.append_cursor.v1",
                    "append_safe": True,
                    "bytes": unrelated_stat.st_size,
                    "mtime_ns": unrelated_stat.st_mtime_ns,
                    "last_line_hash": "hash",
                }
            },
            "-home-example-projects-workspace/stale.jsonl": {
                "append_cursor": {
                    "schema": "okg.agent_sessions.append_cursor.v1",
                    "append_safe": True,
                    "bytes": 9,
                    "mtime_ns": stale_stat.st_mtime_ns,
                    "last_line_hash": "hash",
                }
            },
            "-home-example-projects-workspace/idle.jsonl": {
                "append_cursor": {
                    "schema": "okg.agent_sessions.append_cursor.v1",
                    "append_safe": True,
                    "bytes": idle_stat.st_size,
                    "mtime_ns": idle_stat.st_mtime_ns,
                    "last_line_hash": "hash",
                }
            },
        },
    )

    plan = _microbench_event_plan(
        deployment="sync-demo",
        source_name="agent-sessions",
        entry=entry,
        deployment_dir=tmp_path,
        dsn="postgres://unused",
        scenario="single-delta",
        scope_key=None,
    )

    assert plan["scope_status"] == "sampled_warm_cursor_scope_key"
    assert plan["scope_key"] == "-home-example-projects-workspace/idle.jsonl"
    assert plan["event_count"] == 1


def test_microbench_zero_expected_path_source_prefers_tracked_scope_key(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".untracked-local").write_text("local\n", encoding="utf-8")
    (repo / "README.md").write_text("# Readme\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "init"], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "README.md"], check=True)
    entry = SimpleNamespace(
        ownership_id="sync-demo.docs",
        raw={
            "params": {"repo_root": str(repo)},
            "microbench": {
                "expected_delta": {
                    "single-delta": {
                        "nodes_updated": 0,
                        "edges_updated": 0,
                    }
                }
            },
        },
        sync=SimpleNamespace(
            scope=SimpleNamespace(accepts=("path",)),
            watch=SimpleNamespace(
                roots=(str(repo),),
                include_globs=("**/*",),
                exclude_globs=(),
            ),
        ),
    )

    plan = _microbench_event_plan(
        deployment="sync-demo",
        source_name="docs",
        entry=entry,
        deployment_dir=tmp_path,
        dsn="postgres://unused",
        scenario="single-delta",
        scope_key=None,
    )

    assert plan["scope_status"] == "sampled_path_scope_key"
    assert plan["scope_key"] == "README.md"
    assert plan["event_count"] == 1


def test_microbench_zero_expected_path_source_requires_tracked_scope_key(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".untracked-local").write_text("local\n", encoding="utf-8")
    (repo / "README.md").write_text("# Readme\n", encoding="utf-8")
    entry = SimpleNamespace(
        ownership_id="sync-demo.docs",
        raw={
            "params": {"repo_root": str(repo)},
            "microbench": {
                "expected_delta": {
                    "single-delta": {
                        "nodes_updated": 0,
                        "edges_updated": 0,
                    }
                }
            },
        },
        sync=SimpleNamespace(
            scope=SimpleNamespace(accepts=("path",)),
            watch=SimpleNamespace(
                roots=(str(repo),),
                include_globs=("**/*",),
                exclude_globs=(),
            ),
        ),
    )

    def fail_git(*_args, **_kwargs):
        raise RuntimeError("not a git repo")

    monkeypatch.setattr(
        "okg.substrate.library.sources._git_helpers.run_git",
        fail_git,
    )

    plan = _microbench_event_plan(
        deployment="sync-demo",
        source_name="docs",
        entry=entry,
        deployment_dir=tmp_path,
        dsn="postgres://unused",
        scenario="single-delta",
        scope_key=None,
    )

    assert plan["scope_status"] == "missing_path_scope_sample"
    assert plan["scope_key"] is None
    assert plan["event_count"] == 0
    assert plan["events"] == []


def test_microbench_zero_expected_repo_subdir_infers_tracked_scope_key(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    pact_root = repo / "pact"
    pact_file = pact_root / "changes" / "demo" / "pact.yaml"
    pact_file.parent.mkdir(parents=True)
    pact_file.write_text("change:\n  id: demo\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "init"], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "pact/changes/demo/pact.yaml"], check=True)
    entry = SimpleNamespace(
        ownership_id="sync-demo.pact",
        raw={
            "params": {"pact_root": str(pact_root), "repo": "okg"},
            "microbench": {
                "expected_delta": {
                    "single-delta": {
                        "nodes_updated": 0,
                        "edges_updated": 0,
                    }
                }
            },
        },
        sync=SimpleNamespace(
            scope=SimpleNamespace(accepts=("path",)),
            watch=SimpleNamespace(
                roots=(str(pact_root),),
                include_globs=("changes/*/pact.yaml",),
                exclude_globs=(),
            ),
        ),
    )

    plan = _microbench_event_plan(
        deployment="sync-demo",
        source_name="pact",
        entry=entry,
        deployment_dir=tmp_path,
        dsn="postgres://unused",
        scenario="single-delta",
        scope_key=None,
    )

    assert plan["scope_status"] == "sampled_path_scope_key"
    assert plan["scope_key"] == "changes/demo/pact.yaml"
    assert plan["event_count"] == 1


def test_microbench_codex_sessions_zero_expected_uses_warm_cursor_blob(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "codex" / "sessions"
    workspace = tmp_path / "workspace"
    transcript = root / "2026" / "06" / "30" / "session.jsonl"
    transcript.parent.mkdir(parents=True)
    workspace.mkdir()
    transcript.write_text('{"type":"session_meta","cwd":"%s"}\n' % workspace, encoding="utf-8")
    stat = transcript.stat()
    entry = SimpleNamespace(
        ownership_id="sync-demo.codex-sessions",
        raw={
            "params": {"cwd_filter": [str(workspace)]},
            "microbench": {
                "expected_delta": {
                    "single-delta": {
                        "nodes_updated": 0,
                        "edges_updated": 0,
                    }
                }
            },
        },
        sync=SimpleNamespace(
            scope=SimpleNamespace(accepts=("path",)),
            watch=SimpleNamespace(
                roots=(str(root),),
                include_globs=("**/*.jsonl",),
                exclude_globs=(),
            ),
        ),
    )

    monkeypatch.setattr(
        "okg.substrate.source_sync_microbench._load_source_watermark_cursor",
        lambda dsn, source_names: {
            "transcripts": {
                str(transcript.resolve()): {
                    "append_cursor": {
                        "schema": "okg.codex_sessions.append_cursor.v1",
                        "append_safe": True,
                        "bytes": stat.st_size,
                        "mtime_ns": stat.st_mtime_ns,
                        "last_line_hash": "hash",
                        "meta": {"cwd": str(workspace)},
                    }
                }
            }
        },
    )

    plan = _microbench_event_plan(
        deployment="sync-demo",
        source_name="codex-sessions",
        entry=entry,
        deployment_dir=tmp_path,
        dsn="postgres://unused",
        scenario="single-delta",
        scope_key=None,
    )

    assert plan["scope_status"] == "sampled_warm_cursor_scope_key"
    assert plan["scope_key"] == str(transcript.resolve())
    assert plan["event_count"] == 1


def test_microbench_agent_sessions_no_change_rejects_stale_cursor_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "claude"
    transcript = root / "project" / "session.jsonl"
    transcript.parent.mkdir(parents=True)
    transcript.write_text('{"a": 1}\n{"b": 2}\n', encoding="utf-8")
    stat = transcript.stat()
    entry = SimpleNamespace(
        ownership_id="sync-demo.agent-sessions",
        sync=SimpleNamespace(
            scope=SimpleNamespace(accepts=("path",)),
            watch=SimpleNamespace(
                roots=(str(root),),
                include_globs=("**/*.jsonl",),
                exclude_globs=(),
            ),
        ),
    )

    monkeypatch.setattr(
        "okg.substrate.source_sync_microbench._load_source_watermark_cursor",
        lambda dsn, source_names: {
            "project/session.jsonl": {
                "append_cursor": {
                    "schema": "okg.agent_sessions.append_cursor.v1",
                    "append_safe": True,
                    "bytes": 9,
                    "mtime_ns": stat.st_mtime_ns,
                    "last_line_hash": "hash",
                }
            }
        },
    )

    plan = _microbench_event_plan(
        deployment="sync-demo",
        source_name="agent-sessions",
        entry=entry,
        deployment_dir=tmp_path,
        dsn="postgres://unused",
        scenario="no-change",
        scope_key=None,
    )

    assert plan["scope_status"] == "missing_warm_cursor_scope_sample"
    assert plan["event_count"] == 0
    assert plan["events"] == []


def test_microbench_agent_sessions_missing_warm_cursor_has_no_event(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "claude"
    root.mkdir()
    entry = SimpleNamespace(
        ownership_id="sync-demo.agent-sessions",
        sync=SimpleNamespace(
            scope=SimpleNamespace(accepts=("path",)),
            watch=SimpleNamespace(
                roots=(str(root),),
                include_globs=("**/*.jsonl",),
                exclude_globs=(),
            ),
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_microbench._load_source_watermark_cursor",
        lambda dsn, source_names: {},
    )

    plan = _microbench_event_plan(
        deployment="sync-demo",
        source_name="agent-sessions",
        entry=entry,
        deployment_dir=tmp_path,
        dsn="postgres://unused",
        scenario="single-delta",
        scope_key=None,
    )

    assert plan["scope_status"] == "missing_warm_cursor_scope_sample"
    assert plan["event_count"] == 0
    assert plan["events"] == []


def _register_ledger_artifact_definitions(conn) -> None:
    view_contract = {"slots": {"markdown": "text/markdown"}}
    view_dirty_rule = {
        "mode": "publisher_conservative_full_refresh",
        "scope_key": "publisher:view:<view_id>",
    }
    relation_contract = {
        "source": "view_output_template",
        "logical_relation_id": ("okg_workspace.source_optimization_ledger.rows"),
        "columns": [{"name": "name", "data_type": "text"}],
        "scope": {"scope_key_column": "name"},
    }
    relation_dirty_rule = {
        "mode": "publisher_conservative_sql_relation_refresh",
        "dependencies": ["node_facts", "edge_facts", "view_definition"],
    }
    register_view_artifact_definition(
        conn,
        ViewArtifactDefinition(
            definition_id=MICROBENCH_LEDGER_VIEW_DEFINITION_ID,
            deployment_name="okg-workspace",
            view_id=MICROBENCH_LEDGER_VIEW_ID,
            compute_hash=stable_hash({"view": MICROBENCH_LEDGER_VIEW_ID}),
            output_schema_hash=stable_hash(view_contract),
            renderer_hash=stable_hash({"renderer": "markdown"}),
            dirty_rule_hash=stable_hash(view_dirty_rule),
            output_contract=view_contract,
            dirty_rule=view_dirty_rule,
        ),
    )
    register_jsonb_relation(
        conn,
        RelationArtifactDefinition(
            definition_id=MICROBENCH_LEDGER_RELATION_DEFINITION_ID,
            deployment_name="okg-workspace",
            logical_relation_id=("okg_workspace.source_optimization_ledger.rows"),
            compute_hash=stable_hash({"view": MICROBENCH_LEDGER_VIEW_ID}),
            output_schema_hash=stable_hash(relation_contract),
            renderer_hash=stable_hash({"renderer": "sql-jsonb"}),
            dirty_rule_hash=stable_hash(relation_dirty_rule),
            output_contract=relation_contract,
            dirty_rule=relation_dirty_rule,
        ),
    )


def _write_microbench_deployment(tmp_path: Path) -> Path:
    dep = tmp_path / "deployments" / "sync-demo"
    dep.mkdir(parents=True)
    (dep / "schemas").mkdir()
    (dep / "docs").mkdir()
    (dep / "docs" / "guide.md").write_text("# Guide\n")
    fixture = dep / "docs-fixture.json"
    fixture.write_text(json.dumps({"nodes": [], "edges": []}))
    (dep / "deployment.yaml").write_text(
        "name: sync-demo\n"
        "description: source sync microbench fixture\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "runtime:\n"
        "  enabled: true\n"
        "  source_sync:\n"
        "    watch:\n"
        "      enabled: true\n"
        "    poll:\n"
        "      enabled: true\n"
        "    webhook:\n"
        "      enabled: false\n"
        "enrichers:\n"
        "  - class: okg.substrate.library.linkers.example.ExampleLinker\n"
        "    config:\n"
        "      name: example\n"
    )
    (dep / "source_registry.yaml").write_text(
        "sources:\n"
        "  docs:\n"
        "    module: okg.substrate.library.sources.bootstrap_fixture\n"
        "    class: BootstrapFixtureSource\n"
        "    ownership_id: sync-demo.docs\n"
        "    params:\n"
        f"      fixture_path: {fixture}\n"
        "    source_class: discovery_crawl\n"
        "    record_identity_kind: scoped_locator\n"
        "    record_identity_fields: [path]\n"
        "    source_revision_kind: content_hash\n"
        "    deletion_semantics: missing_from_completed_scope\n"
        "    publication_mode: published_generation\n"
        "    sync:\n"
        "      watch:\n"
        f"        roots: [{dep}]\n"
        "        include_globs: ['docs/**/*.md']\n"
        "      triggers: [fs_event, manual, poll, reconcile]\n"
        "      default_event_mode: scope_incomplete\n"
        "      reconcile_mode: scope_complete\n"
        "      scope:\n"
        "        accepts: [path]\n"
        "        unsupported_fallback: scope_complete\n"
        "      poll:\n"
        "        probe: change_probe\n"
        "        interval_seconds: 60\n"
        "  manual:\n"
        "    module: okg.substrate.library.sources.bootstrap_fixture\n"
        "    class: BootstrapFixtureSource\n"
        "    ownership_id: sync-demo.manual\n"
        "    params:\n"
        f"      fixture_path: {fixture}\n"
        "    source_class: discovery_crawl\n"
        "    record_identity_kind: scoped_locator\n"
        "    record_identity_fields: [path]\n"
        "    source_revision_kind: content_hash\n"
        "    deletion_semantics: missing_from_completed_scope\n"
        "    publication_mode: published_generation\n"
        "    sync:\n"
        "      triggers: [manual]\n"
        "      default_event_mode: scope_complete\n"
        "      reconcile_mode: scope_complete\n"
        "  legacy:\n"
        "    ownership_id: sync-demo.legacy\n"
    )
    return dep
