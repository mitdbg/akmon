from __future__ import annotations

from okg.substrate.source_sync_convergence import build_convergence_status


def _inventory() -> list[dict]:
    return [
        {
            "deployment": "demo",
            "source_name": "pact",
            "runtime": {
                "enabled": True,
                "source_sync_enabled": True,
            },
            "sync": {
                "declared": True,
                "triggers": ["fs_event", "poll", "manual", "reconcile"],
                "default_event_mode": "scope_incomplete",
                "reconcile_mode": "scope_complete",
                "reconcile_run_mode": "scope_complete",
            },
            "freshness_policy": {
                "readiness_required": True,
                "readiness_role": "agent_context_required",
                "completed_scope_required_for_readiness": True,
                "completed_scope_capable": True,
            },
            "constraints": {
                "manual_required": False,
                "credential_required": False,
            },
        }
    ]


def _inventory_row(source_name: str) -> dict:
    row = dict(_inventory()[0])
    row["source_name"] = source_name
    return row


def _freshness(status: str) -> dict:
    return {
        "ok": status == "fresh",
        "missing": [] if status != "missing" else ["demo.pact"],
        "stale": [] if status != "stale" else ["demo.pact"],
        "groups": {
            "pact": {
                "ok": status == "fresh",
                "sources": [{"source": "demo.pact", "status": status}],
            }
        },
    }


def test_blocked_source_reports_planned_action_stage(
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.source-sync-convergence.stage-tracking "
        "test.source-sync-convergence.stage-tracking",
    )
    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=_inventory(),
        source_freshness=_freshness("stale"),
    )

    pact = convergence["sources"][0]
    assert pact["state"] == "blocked"
    assert pact["blocking_stage"] == "planned_action"
    assert pact["stages"]["planned_action"]["status"] == "required"
    assert pact["stages"]["dbos_workflow"]["status"] == "not_started"


def test_stuck_recovery_is_reported_as_blocked(record_property) -> None:
    record_property(
        "requirements",
        "req.source-sync-convergence.stage-tracking "
        "test.source-sync-convergence.stage-tracking",
    )
    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=_inventory(),
        source_freshness=_freshness("stale"),
        active_work_by_source={
            "pact": {
                "workflow_id": "wf-stale",
                "status": "ENQUEUED",
                "run_mode": "scope_complete",
                "stale": True,
                "priority": 20,
                "age_seconds": 900,
                "stale_after_seconds": 600,
            }
        },
    )

    pact = convergence["sources"][0]
    assert convergence["state"] == "stuck"
    assert pact["state"] == "stuck"
    assert pact["blocking_stage"] == "dbos_workflow"
    assert pact["stages"]["dbos_workflow"]["status"] == "stale"
    assert pact["stages"]["dbos_workflow"]["workflow_id"] == "wf-stale"
    assert pact["stages"]["dbos_workflow"]["workflow_status"] == "ENQUEUED"
    assert pact["stages"]["dbos_workflow"]["priority"] == 20
    assert pact["recovery_action"]["action_kind"] == "requeue_protected_workflow"
    assert pact["recovery_action"]["command"] == (
        "env -u OKG_DSN uv run okg ingest --deployment demo "
        "--include pact --mode scope_complete --enqueue --json"
    )


def test_next_action_prefers_actionable_stale_blocker_over_observe(
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.source-sync-recovery-lock-hygiene.operator-surface "
        "test.source-sync-recovery-lock-hygiene.operator-surface",
    )
    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=[
            _inventory_row("code-structure"),
            _inventory_row("pact"),
        ],
        source_freshness=_freshness("stale"),
        active_work_by_source={
            "code-structure": {
                "workflow_id": "wf-observe",
                "status": "ENQUEUED",
                "run_mode": "scope_complete",
                "stale": False,
                "priority": 20,
            },
            "pact": {
                "workflow_id": "wf-stale",
                "status": "ENQUEUED",
                "run_mode": "scope_complete",
                "stale": True,
                "priority": 20,
                "age_seconds": 900,
                "stale_after_seconds": 600,
            },
        },
    )

    assert convergence["state"] == "stuck"
    assert convergence["next_action"] == {
        "code": "requeue_protected_workflow",
        "owner": "operator",
        "source_name": "pact",
        "command": (
            "env -u OKG_DSN uv run okg ingest --deployment demo "
            "--include pact --mode scope_complete --enqueue --json"
        ),
    }


def test_stale_started_pending_recovery_reports_restart_command(
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.source-sync-recovery-lock-hygiene.bounded-recovery "
        "test.source-sync-recovery-lock-hygiene.convergence-state",
    )
    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=_inventory(),
        source_freshness=_freshness("stale"),
        active_work_by_source={
            "pact": {
                "workflow_id": "wf-pending",
                "status": "PENDING",
                "run_mode": "scope_complete",
                "stale": True,
                "priority": 10,
                "age_seconds": 900,
                "stale_after_seconds": 600,
            }
        },
    )

    pact = convergence["sources"][0]
    assert convergence["state"] == "stuck"
    assert pact["recovery_action"]["action_kind"] == "restart_started_workflow"
    assert pact["recovery_action"]["command"] == (
        "env -u OKG_DSN uv run okg dogfood restart "
        "--deployment demo --apply --json"
    )


def test_stale_replaceable_waiting_recovery_reports_gc_command(
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.source-sync-recovery-lock-hygiene.bounded-recovery "
        "test.source-sync-recovery-lock-hygiene.convergence-state",
    )
    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=_inventory(),
        source_freshness=_freshness("stale"),
        active_work_by_source={
            "pact": {
                "workflow_id": "wf-replaceable",
                "status": "ENQUEUED",
                "run_mode": "scope_complete",
                "stale": True,
                "priority": 100,
                "age_seconds": 900,
                "stale_after_seconds": 600,
            }
        },
    )

    pact = convergence["sources"][0]
    assert pact["recovery_action"]["action_kind"] == "cancel_replaceable_workflow"
    assert "--status ENQUEUED" in pact["recovery_action"]["command"]
    assert "--workflow-name okg_source_sync" in pact["recovery_action"]["command"]


def test_publish_and_mcp_visibility_are_distinct_stages(
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.source-sync-convergence.stage-tracking "
        "test.source-sync-convergence.stage-tracking",
    )
    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=_inventory(),
        source_freshness=_freshness("fresh"),
        generation={"status": "published", "generation_id": "g1"},
        mcp={"ok": False},
    )

    pact = convergence["sources"][0]
    assert pact["stages"]["admission_publish"] == {
        "status": "published",
        "generation_id": "g1",
    }
    assert pact["stages"]["mcp_visible_generation"] == {
        "status": "not_observed",
        "generation_id": None,
    }
