from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from okg.substrate.cli.main import main
from okg.substrate.db.management import _dbos_backlog_sample
from okg.substrate.source_sync import SourceSyncEvent, SourceSyncJob
from okg.substrate.source_sync_enqueue import (
    _serialize_source_sync_dbos_args,
    _source_sync_workflow_args,
)


def _env_payload(tmp_path: Path) -> dict:
    return {
        "ok": True,
        "deployment": "okg-workspace",
        "deployment_dir": str(tmp_path),
        "postgres": {
            "declared": True,
            "resolved": True,
            "required_env": ["OKG_DSN"],
            "resolved_dsn": "<redacted-dsn>",
        },
        "environment": [],
        "unresolved": [],
        "recommended_commands": [],
    }


def test_dogfood_status_explains_stale_dbos_source_sync_recovery(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    source_sync_status = {
        "schema": "okg.source_sync.status.v1",
        "deployment": "okg-workspace",
        "ok": False,
        "status": "blocked",
        "blockers": [{"code": "source_sync_runtime_unhealthy"}],
        "runtime_liveness": {
            "ok": False,
            "status": "blocked",
            "worker": {
                "ok": False,
                "status": "stale",
                "code": "runtime_services_overlay_stale",
            },
        },
        "source_freshness": {
            "ok": False,
            "missing": [],
            "stale": ["okg-workspace.codex-sessions"],
        },
        "wakeups": {
            "ok": True,
            "status": "healthy",
            "dbos_workflows": {
                "available": True,
                "queue_name": "okg-okg-workspace",
                "active_count": 30,
                "active_rows": [
                    {"status": "ENQUEUED", "count": 29},
                    {"status": "PENDING", "count": 1},
                ],
                "terminal_history_count": 180,
                "oldest_active_age_seconds": 900,
                "newest_active_age_seconds": 30,
                "stale_after_seconds": 600,
                "stale_replaceable_work_present": True,
            },
        },
    }

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_env_status",
        lambda args, *, show_values: (
            _env_payload(tmp_path),
            "postgres://okg/test",
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_live_checks",
        lambda **kwargs: {
            "blockers": [{
                "id": "sources.sync_liveness",
                "status": "blocker",
                "ok": False,
                "message": "source-sync runtime is stale",
                "details": source_sync_status,
            }],
            "warnings": [],
            "generation": {
                "generation_id": "okg:okg-workspace:gen:test",
                "status": "published",
            },
            "mcp": {
                "ok": True,
                "pinned_generation_id": "okg:okg-workspace:gen:test",
            },
            "source_sync_status": source_sync_status,
        },
    )

    rc = main(["dogfood", "status", "--deployment", "okg-workspace", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["workflow_recovery"] == {
        "ok": False,
        "status": "stale_replaceable_work_present",
        "workflow_name": "okg_source_sync",
        "queue_name": "okg-okg-workspace",
        "active_queue_names": [],
        "active_count": 30,
        "active_rows": [
            {"status": "ENQUEUED", "count": 29},
            {"status": "PENDING", "count": 1},
        ],
        "terminal_history_count": 180,
        "pending_count": 0,
        "waiting_count": 0,
        "fresh_pending_count": 0,
        "running_pending_count": 0,
        "replaceable_waiting_count": 0,
        "oldest_active_age_seconds": 900,
        "newest_active_age_seconds": 30,
        "oldest_pending_update_age_seconds": None,
        "oldest_waiting_age_seconds": None,
        "oldest_replaceable_waiting_age_seconds": None,
        "stale_pending_work_present": False,
        "stale_waiting_work_present": False,
        "stale_replaceable_work_present": True,
        "stale_after_seconds": 600,
        "safe_recovery": "dry_run_then_confirm_apply",
        "dry_run_command": (
            "env -u OKG_DSN uv run okg db dbos-gc --deployment okg-workspace "
            "--stale-after-seconds 600 --queue-name okg-okg-workspace "
            "--workflow-name okg_source_sync --status ENQUEUED --json"
        ),
        "apply_command": (
            "env -u OKG_DSN uv run okg db dbos-gc --deployment okg-workspace "
            "--stale-after-seconds 600 --queue-name okg-okg-workspace "
            "--workflow-name okg_source_sync --status ENQUEUED "
            "--confirm --apply --json"
        ),
    }
    assert payload["recovery_commands"]["source_sync_dbos_gc_apply"] == (
        "env -u OKG_DSN uv run okg db dbos-gc --deployment okg-workspace "
        "--stale-after-seconds 600 --queue-name okg-okg-workspace "
        "--workflow-name okg_source_sync --status ENQUEUED "
        "--confirm --apply --json"
    )


def test_dbos_backlog_sample_decodes_source_sync_row_context() -> None:
    job = SourceSyncJob(
        deployment="okg-workspace",
        sources=("git-files",),
        run_mode="scope_complete",
        events=(
            SourceSyncEvent(
                deployment="okg-workspace",
                source_name="git-files",
                trigger_kind="fs_event",
                scope_key="src/okg/substrate/cli/dogfood.py",
                event_id="evt-1",
            ),
        ),
        include_dependents=True,
        dependent_fanout="closure",
        publish=True,
    )
    inputs, serialization = _serialize_source_sync_dbos_args(
        args=_source_sync_workflow_args(job),
        serialization=None,
    )

    sample = _dbos_backlog_sample(
        {
            "workflow_uuid": "wf-stale",
            "status": "PENDING",
            "queue_name": "okg-okg-workspace",
            "name": "okg_source_sync",
            "stale_raw": datetime.now(timezone.utc) - timedelta(hours=2),
            "executor_id": "local-worker-1",
            "deduplication_id": (
                "sync:okg-workspace:watch:scope_complete:"
                "git-files:closure:bucket"
            ),
            "application_version": "app-v1",
            "recovery_attempts": 1,
            "inputs": inputs,
            "serialization": serialization,
        },
        "updated_at",
        deployment="okg-workspace",
    )

    assert sample["workflow_uuid"] == "wf-stale"
    assert sample["status"] == "PENDING"
    assert sample["executor_id"] == "local-worker-1"
    assert sample["deduplication_id"].startswith(
        "sync:okg-workspace:watch:scope_complete:git-files"
    )
    assert sample["observed_age_seconds"] >= 7_100
    assert sample["recovery_action"] == "cancel_stale_replaceable_workflow"
    source_sync = sample["source_sync"]
    assert {
        key: source_sync[key]
        for key in (
            "workflow_name",
            "decode_status",
            "deployment",
            "sources",
            "run_mode",
            "publish",
            "include_dependents",
            "include_predecessors",
            "dependent_fanout",
            "event_count",
            "scope_keys_sample",
        )
    } == {
        "workflow_name": "okg_source_sync",
        "decode_status": "decoded",
        "deployment": "okg-workspace",
        "sources": ["git-files"],
        "run_mode": "scope_complete",
        "publish": True,
        "include_dependents": True,
        "include_predecessors": True,
        "dependent_fanout": "closure",
        "event_count": 1,
        "scope_keys_sample": ["src/okg/substrate/cli/dogfood.py"],
    }
    assert source_sync["stage_timing"] == {
        "queue_wait": {
            "status": "observable",
            "source": "dbos.workflow_status.created_at_or_updated_at",
        },
        "setup": {
            "status": "observable",
            "source": "dbos.operation_outputs.okg_source_sync_setup_done",
        },
        "source_execution": {
            "status": "pending",
            "source": "workflow_output.report.results[].elapsed_s",
        },
        "source_stage": {
            "status": "pending",
            "source": "workflow_output.report.results[].stage",
        },
        "publish": {
            "status": "pending",
            "source": "workflow_output.report.publishes[]",
        },
    }
