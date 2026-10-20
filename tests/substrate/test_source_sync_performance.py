from __future__ import annotations

import json
import sys
import types
import uuid
from datetime import datetime, timezone

import pytest
import yaml

from tests.pytest.source_jobs_inline import use_inline_source_jobs


@pytest.fixture(autouse=True)
def _dummy_dsn_takes_no_source_sync_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The #3003 run guard needs a real session; these tests have none."""
    from contextlib import nullcontext

    from okg.substrate.ingest import service

    real_guard = service.ingest_run_guard

    def guard(**kwargs: object) -> object:
        if kwargs["dsn"] == "postgres://unused":
            return nullcontext()
        return real_guard(**kwargs)

    monkeypatch.setattr(service, "ingest_run_guard", guard)


def _write_deferred_manifest(deployment_dir, *, name: str) -> None:
    deployment_dir.mkdir(parents=True, exist_ok=True)
    (deployment_dir / "deployment.yaml").write_text(yaml.safe_dump({
        "name": name,
        "postgres": {"dsn": "postgres://unused", "database": name},
        "nomos": {"rollout": {
            "status": "deferred",
            "owner": {"name": f"{name}-test", "contact": "tests@okg.example"},
            "review_by": "2099-01-01",
        }},
    }))


def test_threaded_source_execution_records_per_source_timing(
    monkeypatch,
    tmp_path,
) -> None:
    from okg.substrate.ingest import service
    from okg.substrate.ingest.runner import SourceRunContext

    module = types.ModuleType("okg_test_source_sync_perf_source")

    class _Adapter:
        def __init__(self, source_name: str, graph_branch: str):
            self.source_name = source_name
            self.graph_branch = graph_branch

        last_run_stats = {"records": 2}

    module.FakeSource = _Adapter
    monkeypatch.setitem(
        sys.modules,
        "okg_test_source_sync_perf_source",
        module,
    )

    run_context = SourceRunContext(
        run_id=str(uuid.uuid4()),
        observed_at=datetime.now(timezone.utc),
        admission_id="admission-1",
    )

    monkeypatch.setattr(
        service,
        "_prepare_source_admission",
        lambda **kwargs: (
            run_context,
            {"workflow_id": "wf-source-sync"},
            "accepted",
        ),
    )
    finalize_calls = []

    def _finalize_source_admission(**kwargs):
        finalize_calls.append(dict(kwargs))
        return ["ref-1", "ref-2"]

    monkeypatch.setattr(
        service,
        "_finalize_source_admission",
        _finalize_source_admission,
    )

    class _RunnerResult:
        nodes_appended = 3
        edges_appended = 4
        retractions_node = 0
        retractions_edge = 0
        rejected_fact_count = 0

    class _Runner:
        def __init__(self, *args, **kwargs):
            pass

        @staticmethod
        def _adapter_probe_short_circuit_safe(adapter):
            return False

        def run(self, adapter, **kwargs):
            assert kwargs["mode"] == "scope_complete"
            assert adapter.source_name == "okg-workspace.fake-source"
            assert adapter.graph_branch == "default"
            return _RunnerResult()

    monkeypatch.setattr(service, "SourceRunner", _Runner)
    monkeypatch.setattr(
        service,
        "_snapshot_source_state",
        lambda *args, **kwargs: None,
    )
    _write_deferred_manifest(tmp_path, name="okg-workspace")

    options = service.IngestOptions(
        dsn="postgres://unused",
        deployment="okg-workspace",
        deployment_dir=tmp_path,
        source_events={},
        publish=False,
    )
    entry = types.SimpleNamespace(
        name="fake-source",
        raw={
            "module": "okg_test_source_sync_perf_source",
            "class": "FakeSource",
            "params": {},
        },
        ownership_id="okg-workspace.fake-source",
        admission_policy=None,
    )

    result = service._run_source(options, entry, "fake-source")

    assert result["status"] == "OK"
    assert result["timing_status"] == "available"
    assert result["timings_ms"]["total"] >= 0
    assert {
        "admission_prepare",
        "import",
        "construct",
        "source_runner",
        "admission_finalize",
        "total",
    } <= set(result["timings_ms"])
    assert result["source_stats"] == {"records": 2}
    assert finalize_calls[0]["observed_output_count"] == 7


def test_zero_output_admission_verdict_records_no_fact_payload() -> None:
    from okg.substrate.ingest.service import _zero_output_admission_verdict

    verdict = _zero_output_admission_verdict(
        source="okg-workspace.code-structure",
        run_id="run-1",
        admission_mode="fast_track",
        completed_scope=True,
        source_health_status="ok",
        run_mode="no_change",
        next_cursor={"probe": "unchanged"},
    )

    assert verdict["fact_ref_count"] == 0
    assert verdict["producer_policy"] == {
        "status": "matched",
        "matched_clause_ids": [],
        "failed_clause_ids": [],
        "record_counts": {"node": 0, "edge": 0},
        "reason": "unchanged",
        "source_health_status": "ok",
        "completed_scope": True,
        "run_mode": "no_change",
        "terminal_state_authorized": True,
    }


def test_parallel_source_execution_records_profile_stages_and_outputs(
    monkeypatch,
    tmp_path,
) -> None:
    from okg.substrate.ingest import service
    from okg.substrate.ingest.runner import SourceRunContext

    module = types.ModuleType("okg_test_source_sync_parallel_source")

    class _Adapter:
        def __init__(self, label: str):
            self.label = label
            self.last_run_stats = {"records": 1, "label": label}

    module.FakeSource = _Adapter
    monkeypatch.setitem(
        sys.modules,
        "okg_test_source_sync_parallel_source",
        module,
    )

    deployment_dir = tmp_path / "deployments" / "svc"
    _write_deferred_manifest(deployment_dir, name="svc")
    source_entry = {
        "source_class": "discovery_crawl",
        "record_identity_kind": "scoped_locator",
        "record_identity_fields": ["id"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
        "module": "okg_test_source_sync_parallel_source",
        "class": "FakeSource",
    }
    (deployment_dir / "source_registry.yaml").write_text(yaml.safe_dump({
        "sources": {
            "first": {
                **source_entry,
                "ownership_id": "svc.first",
                "params": {"label": "first"},
            },
            "second": {
                **source_entry,
                "ownership_id": "svc.second",
                "params": {"label": "second"},
            },
        },
    }))

    def _prepare_source_admission(**kwargs):
        source_name = kwargs["source_registry_name"]
        return (
            SourceRunContext(
                run_id=str(uuid.uuid4()),
                observed_at=datetime.now(timezone.utc),
                admission_id=f"admission-{source_name}",
            ),
            {"workflow_id": f"wf-{source_name}"},
            "accepted",
        )

    monkeypatch.setattr(
        service,
        "_prepare_source_admission",
        _prepare_source_admission,
    )
    monkeypatch.setattr(
        service,
        "_finalize_source_admission",
        lambda **kwargs: [f"ref-{kwargs['source']}"],
    )

    # Parallel execution is proven with real source processes in
    # tests/substrate/ingest/test_source_jobs.py; this test pins the per-source
    # profile stages and progress rows, so it runs the stubbed sources in this
    # process.
    use_inline_source_jobs(monkeypatch)

    class _RunnerResult:
        def __init__(self, label: str):
            self.nodes_appended = 1 if label == "first" else 2
            self.edges_appended = 2 if label == "first" else 3
            self.retractions_node = 0
            self.retractions_edge = 0
            self.rejected_fact_count = 0
            self.timings_ms = {
                "adapter_run": 1.25,
                "flush.chronos_apply": 2.5,
                "total": 4.0,
            }

    stage_progress_rows = []

    class _Runner:
        def __init__(self, *args, **kwargs):
            pass

        def run(self, adapter, **kwargs):
            assert kwargs["mode"] == "scope_complete"
            return _RunnerResult(adapter.label)

    monkeypatch.setattr(service, "SourceRunner", _Runner)
    # Diagnostic progress rows are written by the module-level
    # `upsert_source_progress`, not by a throwaway SourceRunner instance: the
    # runner method holds its connection for `run()` to release, and `run()`
    # is never entered on that path.
    coverage_progress_rows = []

    def _capture_progress(*, dsn, deployment, source_name, progress):
        if "sync.stage_timings.last_run" in progress:
            stage_progress_rows.append(dict(progress))
        else:
            coverage_progress_rows.append(dict(progress))

    monkeypatch.setattr(service, "upsert_source_progress", _capture_progress)
    monkeypatch.setattr(
        service,
        "_snapshot_source_state",
        lambda *args, **kwargs: None,
    )

    captured_runs = []
    monkeypatch.setattr(
        "okg.substrate.profiling.context.write_profile_bundle",
        lambda run: captured_runs.append(run),
    )

    outcome = service.run_ingest(
        service.IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=deployment_dir,
            include={"first", "second"},
            include_predecessors=False,
            source_events={},
            publish=False,
            profile_force_deep=True,
            max_source_workers=2,
        ),
    )

    assert outcome.any_failed is False
    by_name = {
        result["name"]: result
        for result in outcome.report["results"]
    }
    assert outcome.report["timing_status"] == "available"
    assert outcome.report["timings_ms"]["total"] >= 0
    assert outcome.report["timings_ms"]["source_wall"] >= 0
    assert outcome.report["timings_ms"]["source_reported_total"] >= 0
    assert outcome.report["layer_timings"][0]["sources"] == [
        "first", "second",
    ]
    assert outcome.report["layer_timings"][0]["publish_status"] == "disabled"
    assert by_name["first"]["nodes_appended"] == 1
    assert by_name["first"]["edges_appended"] == 2
    assert by_name["first"]["source_stats"] == {
        "records": 1,
        "label": "first",
    }
    assert by_name["second"]["nodes_appended"] == 2
    assert by_name["second"]["edges_appended"] == 3
    assert by_name["second"]["source_stats"] == {
        "records": 1,
        "label": "second",
    }

    assert captured_runs
    stage_by_name = {
        stage.name: stage
        for stage in captured_runs[0].stages
    }
    assert {
        "source.first.run",
        "source.second.run",
    } <= set(stage_by_name)
    first_attrs = stage_by_name["source.first.run"].attrs
    assert first_attrs["source_name"] == "first"
    assert first_attrs["ownership_id"] == "svc.first"
    assert first_attrs["deployment"] == "svc"
    assert first_attrs["input_count"] == 0
    assert first_attrs["output_count"] == 3
    assert first_attrs["nodes_appended"] == 1
    assert first_attrs["edges_appended"] == 2
    assert first_attrs["timing_status"] == "available"
    assert first_attrs["timing_unavailable_reason"] is None
    assert "source_runner" in first_attrs["timings_ms"]
    assert first_attrs["timings_ms"]["source_runner.adapter_run"] == 1.25
    assert (
        first_attrs["timings_ms"]["source_runner.flush.chronos_apply"] == 2.5
    )
    assert first_attrs["source_stats"] == {"records": 1, "label": "first"}

    second_attrs = stage_by_name["source.second.run"].attrs
    assert second_attrs["source_name"] == "second"
    assert second_attrs["ownership_id"] == "svc.second"
    assert second_attrs["deployment"] == "svc"
    assert second_attrs["input_count"] == 0
    assert second_attrs["output_count"] == 5
    assert second_attrs["nodes_appended"] == 2
    assert second_attrs["edges_appended"] == 3
    assert second_attrs["timing_status"] == "available"
    assert second_attrs["timing_unavailable_reason"] is None
    assert "source_runner" in second_attrs["timings_ms"]
    assert second_attrs["source_stats"] == {"records": 1, "label": "second"}
    # Every finished source also records whether it passed its record-set
    # coverage check, so the watcher can tell a failing source from a healthy
    # one (issue #1599).
    coverage_payloads = [
        json.loads(row["sync.record_set_coverage"])
        for row in coverage_progress_rows
    ]
    assert {payload["source_name"] for payload in coverage_payloads} == {
        "svc.first", "svc.second",
    }
    assert all(payload["failing"] is False for payload in coverage_payloads)

    assert len(stage_progress_rows) == 2
    payloads = [
        json.loads(row["sync.stage_timings.last_run"])
        for row in stage_progress_rows
    ]
    payload_by_source = {
        payload["registry_source_name"]: payload
        for payload in payloads
    }
    assert set(payload_by_source) == {"first", "second"}
    assert payload_by_source["first"]["schema"] == (
        "okg.source_sync.stage_timings.last_run.v1"
    )
    assert payload_by_source["first"]["source_name"] == "svc.first"
    assert payload_by_source["first"]["output_count"] == 3
    assert "source_runner" in payload_by_source["first"]["timings_ms"]
    assert payload_by_source["first"]["timings_ms"][
        "source_runner.adapter_run"
    ] == 1.25
    assert payload_by_source["second"]["source_name"] == "svc.second"
    assert payload_by_source["second"]["output_count"] == 5


def test_ingest_report_summarizes_publish_timing(
    monkeypatch,
    tmp_path,
) -> None:
    from okg.substrate.ingest import service
    from okg.substrate.ingest.runner import SourceRunContext

    module = types.ModuleType("okg_test_source_sync_publish_timing")

    class _Adapter:
        last_run_stats = {"records": 1}

    module.FakeSource = _Adapter
    monkeypatch.setitem(
        sys.modules,
        "okg_test_source_sync_publish_timing",
        module,
    )

    run_context = SourceRunContext(
        run_id=str(uuid.uuid4()),
        observed_at=datetime.now(timezone.utc),
        admission_id="admission-publish-timing",
    )
    monkeypatch.setattr(
        service,
        "_prepare_source_admission",
        lambda **kwargs: (
            run_context,
            {"workflow_id": "wf-source-sync"},
            "accepted",
        ),
    )
    monkeypatch.setattr(
        service,
        "_finalize_source_admission",
        lambda **kwargs: ["ref-1"],
    )

    class _RunnerResult:
        nodes_appended = 1
        edges_appended = 1
        retractions_node = 0
        retractions_edge = 0
        rejected_fact_count = 0

    class _Runner:
        def __init__(self, *args, **kwargs):
            pass

        def run(self, adapter, **kwargs):
            return _RunnerResult()

        def _upsert_progress(self, progress):
            # Mirror the production SourceRunner interface: run_ingest constructs a
            # SourceRunner to upsert stage-timing progress as a diagnostic side
            # effect. The real DB write is irrelevant to this in-memory report test.
            pass

    monkeypatch.setattr(service, "SourceRunner", _Runner)
    monkeypatch.setattr(
        service,
        "_snapshot_source_state",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        service,
        "_staged_source_redrive_state",
        lambda options: {
            "staged_work_present": False,
            "latest_checkpoint_id": None,
            "latest_checkpoint_status": None,
            "latest_published_checkpoint_id": None,
            "branch_mutation_token": "sha256:no-staged-work",
        },
    )
    monkeypatch.setattr(
        service,
        "_publish",
        lambda options, label, _dirty_hint=None, _lineage_hint=None, ingest_completeness=None: {
            "label": label,
            "status": "published",
            "total_duration_ms": 1234.5,
            "generation_id": "okg:gen:test",
        },
    )

    use_inline_source_jobs(monkeypatch)
    deployment_dir = tmp_path / "deployments" / "svc"
    _write_deferred_manifest(deployment_dir, name="svc")
    (deployment_dir / "source_registry.yaml").write_text(yaml.safe_dump({
        "sources": {
            "fake-source": {
                "source_class": "discovery_crawl",
                "record_identity_kind": "scoped_locator",
                "record_identity_fields": ["id"],
                "source_revision_kind": "content_hash",
                "deletion_semantics": "missing_from_completed_scope",
                "publication_mode": "published_generation",
                "module": "okg_test_source_sync_publish_timing",
                "class": "FakeSource",
                "ownership_id": "svc.fake-source",
                "params": {},
            },
        },
    }))

    outcome = service.run_ingest(
        service.IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=deployment_dir,
            include={"fake-source"},
            include_predecessors=False,
            source_events={},
            publish=True,
        ),
    )

    assert outcome.any_failed is False
    assert outcome.report["timing_status"] == "available"
    assert outcome.report["timings_ms"]["publish_reported_total"] == 1234.5
    assert outcome.report["timings_ms"]["publish_wall"] >= 0
    # Under perf-reimpl's publish-once-per-commit collapse, each source layer
    # defers its publish (publish_status="deferred_to_commit", 0.0 reported) and a
    # single trailing "commit" layer carries the real publish.
    assert outcome.report["layer_timings"] == [
        {
            "index": 1,
            "sources": ["fake-source"],
            "scheduled_sources": ["fake-source"],
            "dependency_blocked_sources": [],
            "source_wall_ms": outcome.report["layer_timings"][0]["source_wall_ms"],
            "source_reported_total_ms": (
                outcome.report["layer_timings"][0]["source_reported_total_ms"]
            ),
            "publish_wall_ms": 0.0,
            "publish_reported_ms": 0.0,
            "publish_status": "deferred_to_commit",
        },
        {
            "index": "commit",
            "sources": [],
            "source_wall_ms": 0.0,
            "source_reported_total_ms": 0.0,
            "publish_wall_ms": outcome.report["layer_timings"][1]["publish_wall_ms"],
            "publish_reported_ms": 1234.5,
            "publish_status": "published",
        },
    ]


def test_publish_path_stage_attribution(monkeypatch, tmp_path) -> None:
    from okg.substrate import source_sync_microbench as microbench

    captured_options = []

    def _run_ingest(options):
        captured_options.append(options)
        return types.SimpleNamespace(
            any_failed=False,
            report={
                "results": [{
                    "name": "pact",
                    "status": "OK",
                    "timings_ms": {
                        "source_runner": 10.0,
                        "total": 15.0,
                    },
                    "source_stats": {"files_seen": 1},
                    "nodes_appended": 1,
                    "edges_appended": 0,
                    "retract_n": 0,
                    "retract_e": 0,
                    "run_mode": "scope_complete",
                }],
                "publishes": [{
                    "label": "after-layer-1",
                    "status": "published",
                    "generation_id": "okg:gen:test",
                    "timings_ms": {
                        "locked_connection": 1.1,
                        "enrichers": 12.5,
                        "consistency_gates": 7.0,
                        "native_views": 5.0,
                        "checkpoint_generation": 2.0,
                        "total": 30.0,
                    },
                    "timing_details": {
                        "schema": "okg.publisher.timing_details.v1",
                        "top_level_ms": {
                            "locked_connection": 1.1,
                            "enrichers": 12.5,
                            "consistency_gates": 7.0,
                            "native_views": 5.0,
                            "checkpoint_generation": 2.0,
                            "total": 30.0,
                        },
                        "flat_ms": {
                            "stage.locked_connection": 1.1,
                            "stage.enrichers": 12.5,
                            "stage.consistency_gates": 7.0,
                            "stage.native_views": 5.0,
                            "stage.checkpoint_generation": 2.0,
                            "stage.total": 30.0,
                            "lock.acquire": 1.1,
                            "lock.hold": 88.0,
                            "enricher._enricher_setup.total": 12.5,
                            "consistency_gate.no_dangling.elapsed": 7.0,
                            "native_view.source_health.elapsed": 5.0,
                        },
                        "lock": {
                            "coordination_mode": "dbos",
                            "acquire_ms": 1.1,
                            "hold_ms": 88.0,
                        },
                        "enrichers": [{
                            "producer_id": "_enricher_setup",
                            "timings_ms": {"total": 12.5},
                        }],
                        "consistency_gates": [{
                            "check_id": "no_dangling",
                            "elapsed_ms": 7.0,
                        }],
                        "native_views": [{
                            "view_id": "source_health",
                            "elapsed_ms": 5.0,
                        }],
                        "lenses": [],
                    },
                }],
            },
        )

    monkeypatch.setattr(
        microbench,
        "_measure_graph_read_visibility",
        lambda dsn: {
            "status": "ok",
            "elapsed_ms": 4.2,
            "row_observed": True,
        },
    )

    event = microbench._source_sync_event(
        deployment="okg-workspace",
        source_name="pact",
        trigger_kind="fs_event",
        scope_key="pact/changes/example/pact.yaml",
        reason="test",
    ).to_dict()
    sample = microbench._run_microbench_sample(
        deployment="okg-workspace",
        deployment_dir=tmp_path,
        dsn="postgres://unused",
        source_name="pact",
        scenario="single-delta",
        pipeline_mode=microbench.FULL_PIPELINE_SCENARIO,
        iteration=1,
        repeat=1,
        run_mode="scope_complete",
        include_predecessors=False,
        include_dependents=False,
        dependent_fanout=None,
        max_source_workers=1,
        event_plan={
            "scenario": "single-delta",
            "event_count": 1,
            "scope_kind": "path",
            "scope_key": "pact/changes/example/pact.yaml",
            "scope_status": "provided_scope_key",
            "events": [event],
        },
        run_ingest_fn=_run_ingest,
    )

    assert sample["ok"] is True
    assert captured_options[0].publish is True
    flat = sample["publish_timing_details_flat_ms"]
    assert flat["lock.acquire"] == 1.1
    assert flat["lock.hold"] == 88.0
    assert flat["enricher._enricher_setup.total"] == 12.5
    assert flat["consistency_gate.no_dangling.elapsed"] == 7.0
    assert flat["native_view.source_health.elapsed"] == 5.0
    assert flat["graph_read_visibility"] == 4.2
    assert sample["publish_timing_details"]["per_publish"][0][
        "timing_details"
    ]["lock"]["hold_ms"] == 88.0

    row = microbench._summarize_microbench_samples(
        deployment="okg-workspace",
        source_name="pact",
        scenario="single-delta",
        pipeline_mode=microbench.FULL_PIPELINE_SCENARIO,
        run_mode="scope_complete",
        event_plan={"events": []},
        samples=[sample],
    )

    assert row["publish_stage_p95_ms"]["enrichers"] == 12.5
    assert row["publish_timing_details_p95_ms"]["lock.hold"] == 88.0
    assert row["publish_timing_details_p95_ms"][
        "enricher._enricher_setup.total"
    ] == 12.5
    assert row["publish_timing_details_p95_ms"][
        "consistency_gate.no_dangling.elapsed"
    ] == 7.0
    assert row["publish_timing_details_p95_ms"][
        "native_view.source_health.elapsed"
    ] == 5.0
    assert row["graph_read_visibility_ms"]["p95"] == 4.2
    assert row["publish_scope"]["publish_lock_wait_p95_ms"] == 1.1
    assert row["publish_scope"]["publish_lock_hold_p95_ms"] == 88.0
    assert row["publish_scope"]["environment_contention"] is False


def test_publish_path_reports_delta_scope_widening(
    monkeypatch,
    tmp_path,
) -> None:
    from okg.substrate import source_sync_microbench as microbench

    def _run_ingest(options):
        return types.SimpleNamespace(
            any_failed=False,
            report={
                "results": [{
                    "name": "pact",
                    "status": "OK",
                    "timings_ms": {
                        "source_runner": 12.0,
                        "total": 20.0,
                    },
                    "source_stats": {"files_seen": 1, "files_scanned": 1},
                    "nodes_appended": 1,
                    "edges_appended": 1,
                    "retract_n": 0,
                    "retract_e": 0,
                    "run_mode": "scope_complete",
                    "sync_scope": {
                        "scope_keys": ["pact/changes/example/pact.yaml"],
                    },
                }],
                "publishes": [{
                    "label": "after-layer-1",
                    "status": "published",
                    "generation_id": "okg:gen:delta",
                    "nodes_added": 1,
                    "nodes_retracted": 0,
                    "nodes_updated": 0,
                    "edges_added": 2,
                    "edges_retracted": 0,
                    "edges_updated": 0,
                    "timings_ms": {
                        "delta_counts_before_views": 3.0,
                        "enrichers": 8.0,
                        "consistency_gates": 5.0,
                        "native_views": 7.0,
                        "checkpoint_generation": 2.0,
                        "total": 40.0,
                    },
                    "timing_details": {
                        "schema": "okg.publisher.timing_details.v1",
                        "top_level_ms": {
                            "delta_counts_before_views": 3.0,
                            "enrichers": 8.0,
                            "consistency_gates": 5.0,
                            "native_views": 7.0,
                            "checkpoint_generation": 2.0,
                            "total": 40.0,
                        },
                        "flat_ms": {
                            "stage.delta_counts_before_views": 3.0,
                            "stage.enrichers": 8.0,
                            "stage.consistency_gates": 5.0,
                            "stage.native_views": 7.0,
                            "stage.checkpoint_generation": 2.0,
                            "stage.total": 40.0,
                            "enricher.Linker.total": 8.0,
                            "consistency_gate.no_dangling.elapsed": 5.0,
                            "native_view.source_optimization.elapsed": 7.0,
                        },
                        "lock": {},
                        "enrichers": [{
                            "producer_id": "Linker",
                            "status": "success",
                            "dirty_node_count": 1,
                            "dirty_edge_count": 2,
                            "timings_ms": {"total": 8.0},
                        }],
                        "consistency_gates": [{
                            "check_id": "no_dangling",
                            "elapsed_ms": 5.0,
                        }],
                        "native_views": [{
                            "view_id": "source_optimization",
                            "fragments_refreshed": 1,
                            "elapsed_ms": 7.0,
                        }],
                        "lenses": [],
                    },
                }],
            },
        )

    monkeypatch.setattr(
        microbench,
        "_measure_graph_read_visibility",
        lambda dsn: {
            "status": "ok",
            "elapsed_ms": 1.0,
            "row_observed": True,
        },
    )
    event = microbench._source_sync_event(
        deployment="okg-workspace",
        source_name="pact",
        trigger_kind="fs_event",
        scope_key="pact/changes/example/pact.yaml",
        reason="test",
    ).to_dict()

    sample = microbench._run_microbench_sample(
        deployment="okg-workspace",
        deployment_dir=tmp_path,
        dsn="postgres://unused",
        source_name="pact",
        scenario="single-delta",
        pipeline_mode=microbench.FULL_PIPELINE_SCENARIO,
        iteration=1,
        repeat=1,
        run_mode="scope_complete",
        include_predecessors=False,
        include_dependents=False,
        dependent_fanout=None,
        max_source_workers=1,
        event_plan={
            "scenario": "single-delta",
            "event_count": 1,
            "scope_kind": "path",
            "scope_key": "pact/changes/example/pact.yaml",
            "scope_status": "provided_scope_key",
            "events": [event],
        },
        run_ingest_fn=_run_ingest,
    )
    row = microbench._summarize_microbench_samples(
        deployment="okg-workspace",
        source_name="pact",
        scenario="single-delta",
        pipeline_mode=microbench.FULL_PIPELINE_SCENARIO,
        run_mode="scope_complete",
        event_plan={
            "event_count": 1,
            "scope_kind": "path",
            "scope_key": "pact/changes/example/pact.yaml",
            "scope_status": "provided_scope_key",
        },
        samples=[sample],
    )

    publish_scope = row["publish_scope"]
    assert publish_scope["changed_inputs"] == {
        "event_count": 1,
        "revision_hint": None,
        "scope_kind": "path",
        "scope_key": "pact/changes/example/pact.yaml",
        "scope_status": "provided_scope_key",
    }
    assert publish_scope["changed_graph_rows"] == 3
    assert publish_scope["source_rows_appended_total"] == 2
    assert publish_scope["readiness_impact"] == "within_budget"
    assert publish_scope["max_enricher_dirty_nodes"] == 1
    assert publish_scope["max_enricher_dirty_edges"] == 2
    assert publish_scope["max_view_refreshed_fragments"] == 1
    widened_by_stage = {
        item["stage"]: item for item in publish_scope["widened_stages"]
    }
    assert widened_by_stage["enrichers"]["reason"] == (
        "declared_enricher_dependency_scope"
    )
    assert widened_by_stage["enrichers"]["owner"] == "enrichers"
    assert widened_by_stage["native_views"]["reason"] == (
        "declared_view_dependency_scope"
    )
    assert widened_by_stage["delta_counts_before_views"]["reason"] == (
        "graph_delta_accounting"
    )
    assert publish_scope["top_widening_stage"] == "enrichers"
    assert publish_scope["top_widening_owner"] == "enrichers"
    assert publish_scope["top_widening_reason"] == (
        "declared_enricher_dependency_scope"
    )
