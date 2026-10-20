from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone


def test_source_sync_debug_workflow_decodes_inputs_and_stage_timings(
    monkeypatch,
    capsys,
) -> None:
    from okg.substrate.cli import main as cli

    created = datetime(2026, 6, 3, 14, 0, 0, tzinfo=timezone.utc)
    first_step = datetime(2026, 6, 3, 14, 0, 1, tzinfo=timezone.utc)
    completed = datetime(2026, 6, 3, 14, 0, 5, tzinfo=timezone.utc)

    class _Report:
        workflow_id = "wf-source-sync-debug"
        name = "okg_source_sync"
        status = "SUCCESS"
        created_at = created.isoformat()
        updated_at = completed.isoformat()
        started_at = created.isoformat()
        completed_at = completed.isoformat()
        recovery_attempts = 0
        queue_name = "okg-okg-workspace-source-sync"
        steps = [{
            "step_id": 1,
            "name": "okg_source_sync_setup_done",
            "started_at": first_step.isoformat(),
            "completed_at": first_step.isoformat(),
        }]
        inputs = "serialized-inputs"
        serialization = "json"
        deduplication_id = "dedupe-1"
        executor_id = "executor-1"
        application_version = "app-v1"
        output = {
            "any_failed": False,
            "report": {
                "results": [{
                    "name": "pact",
                    "status": "OK",
                    "run_mode": "scope_complete",
                    "elapsed_s": 1.25,
                    "timing_status": "available",
                    "timings_ms": {
                        "admission_prepare": 10.0,
                        "import": 1.0,
                        "construct": 2.0,
                        "source_runner": 1000.0,
                        "admission_finalize": 20.0,
                        "total": 1033.0,
                    },
                    "nodes_appended": 3,
                    "edges_appended": 4,
                    "source_stats": {"records": 7},
                }],
                "publishes": [{
                    "label": "after-layer-1",
                    "status": "published",
                    "generation_id": 123,
                }],
            },
        }
        error = None

    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_debug_workflow",
        lambda dsn, workflow_id: _Report(),
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.source_sync_payload_from_dbos_inputs",
        lambda deployment, inputs, serialization: {
            "deployment": deployment,
            "sources": ["pact"],
            "run_mode": "scope_complete",
            "events": [{
                "event_id": "event-1",
                "source_name": "pact",
                "trigger_kind": "fs_event",
                "reason": "watch:modified",
                "observed_at": "2026-06-03T13:59:59+00:00",
                "scope_key": "pact/changes/x/pact.yaml",
            }],
            "publish": True,
            "include_predecessors": True,
            "include_dependents": True,
            "dependent_fanout": "closure",
            "profile_force_deep": True,
            "profile_collectors": ["rootless-speedscope"],
        },
    )
    monkeypatch.setattr(cli, "_find_profile_for_workflow", lambda workflow_id: "/tmp/profile.json")
    monkeypatch.setattr(cli, "_read_publisher_phase_events", lambda dsn, workflow_id: [])

    class _Manifest:
        runtime = type("R", (), {"enabled": True})()
        postgres_dsn = "postgres://unused"

    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda name: _Manifest(),
    )
    monkeypatch.setattr(
        cli,
        "_resolve_deployment_dsn",
        lambda args: "postgres://unused",
    )

    rc = cli._cmd_runtime(
        argparse.Namespace(
            rt_subcmd="debug-workflow",
            deployment="okg-workspace",
            dsn="postgres://unused",
            workflow_id="wf-source-sync-debug",
        ),
        as_json=True,
    )

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["work_ms"] is None
    assert out["queue_wait_ms"] is None
    assert out["dbos"]["deduplication_id"] == "dedupe-1"
    assert out["source_sync"]["decode_status"] == "decoded"
    assert out["source_sync"]["sources"] == ["pact"]
    assert out["source_sync"]["event_count"] == 1
    assert out["source_sync"]["event_sample"][0]["event_id"] == "event-1"
    assert out["source_sync"]["event_sample"][0]["source"] == "pact"
    assert (
        out["source_sync"]["event_sample"][0]["trigger_reason"]
        == "watch:modified"
    )
    assert (
        out["source_sync"]["event_sample"][0]["observed_at"]
        == "2026-06-03T13:59:59+00:00"
    )
    assert out["source_sync"]["event_sample"][0]["scope_sample"] == [
        "pact/changes/x/pact.yaml",
    ]
    assert out["source_sync"]["queue_dwell_ms"] == 1000
    assert out["source_sync"]["profile_availability"] == {
        "status": "available",
        "path": "/tmp/profile.json",
    }
    timing = out["source_sync"]["stage_timing"]
    assert timing["status"] == "available"
    assert timing["unavailable_reason"] is None
    assert timing["timed_source_count"] == 1
    assert timing["sources"][0]["timings_ms"]["source_runner"] == 1000.0
    assert timing["sources"][0]["timing_unavailable_reason"] is None


def test_source_sync_debug_workflow_reports_active_queue_dwell(
    monkeypatch,
    capsys,
) -> None:
    from okg.substrate.cli import main as cli

    created = datetime(2000, 1, 1, tzinfo=timezone.utc)

    class _Report:
        workflow_id = "wf-source-sync-active"
        name = "okg_source_sync"
        status = "ENQUEUED"
        created_at = created.isoformat()
        updated_at = created.isoformat()
        started_at = created.isoformat()
        completed_at = None
        recovery_attempts = 0
        queue_name = "okg-okg-workspace-source-sync"
        steps = []
        inputs = "serialized-inputs"
        serialization = "json"
        deduplication_id = "dedupe-active"
        executor_id = None
        application_version = "app-v1"
        output = None
        error = None

    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_debug_workflow",
        lambda dsn, workflow_id: _Report(),
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.source_sync_payload_from_dbos_inputs",
        lambda deployment, inputs, serialization: {
            "deployment": deployment,
            "sources": ["agent-sessions"],
            "run_mode": "scope_incomplete",
            "events": [{"trigger_kind": "poll"}],
            "publish": True,
            "include_predecessors": True,
            "include_dependents": False,
            "dependent_fanout": "none",
            "profile_force_deep": False,
            "profile_collectors": [],
        },
    )
    monkeypatch.setattr(cli, "_find_profile_for_workflow", lambda workflow_id: None)
    monkeypatch.setattr(cli, "_read_publisher_phase_events", lambda dsn, workflow_id: [])

    class _Manifest:
        runtime = type("R", (), {"enabled": True})()
        postgres_dsn = "postgres://unused"

    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda name: _Manifest(),
    )
    monkeypatch.setattr(
        cli,
        "_resolve_deployment_dsn",
        lambda args: "postgres://unused",
    )

    rc = cli._cmd_runtime(
        argparse.Namespace(
            rt_subcmd="debug-workflow",
            deployment="okg-workspace",
            dsn="postgres://unused",
            workflow_id="wf-source-sync-active",
        ),
        as_json=True,
    )

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["source_sync"]["stage_timing"]["status"] == "missing"
    assert (
        out["source_sync"]["stage_timing"]["unavailable_reason"]
        == "workflow_not_finished"
    )
    assert out["source_sync"]["queue_dwell_ms"] > 0
