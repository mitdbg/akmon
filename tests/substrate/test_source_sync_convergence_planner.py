from __future__ import annotations

from okg.substrate.source_sync_convergence import build_convergence_status


def _inventory(*, manual_required: bool = False) -> list[dict]:
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
                "manual_required": manual_required,
                "credential_required": False,
            },
        }
    ]


def _freshness(status: str = "stale") -> dict:
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


def test_stale_readiness_source_gets_completed_scope_reconcile(
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.source-sync-convergence.recovery-planner "
        "test.source-sync-convergence.recovery-planner",
    )
    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=_inventory(),
        source_freshness=_freshness("stale"),
    )

    action = convergence["recovery_plan"]["actions"][0]
    assert convergence["state"] == "blocked"
    assert action["action_kind"] == "completed_scope_reconcile"
    assert action["source_name"] == "pact"
    assert action["run_mode"] == "scope_complete"
    assert action["deduplication_key"] == (
        "sync:demo:convergence:pact:scope_complete"
    )
    assert "--include pact" in action["command"]
    assert "--enqueue" in action["command"]
    assert "--inline" not in action["command"]


def test_active_completed_scope_work_is_observed_not_duplicated(
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.source-sync-convergence.recovery-planner "
        "test.source-sync-convergence.recovery-planner",
    )
    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=_inventory(),
        source_freshness=_freshness("stale"),
        active_work_by_source={
            "pact": {
                "workflow_id": "wf-complete",
                "status": "PENDING",
                "run_mode": "scope_complete",
                "stale": False,
            }
        },
    )

    action = convergence["recovery_plan"]["actions"][0]
    assert convergence["state"] == "converging"
    assert action["action_kind"] == "observe_active_completed_scope_recovery"
    assert action["workflow_id"] == "wf-complete"


def test_manual_or_credential_source_gets_operator_action(
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.source-sync-convergence.recovery-planner "
        "test.source-sync-convergence.recovery-planner",
    )
    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=_inventory(manual_required=True),
        source_freshness=_freshness("missing"),
    )

    action = convergence["recovery_plan"]["actions"][0]
    assert convergence["state"] == "blocked"
    assert action["action_kind"] == "manual_or_credential_action"
    assert convergence["next_action"]["owner"] == "operator"
