from __future__ import annotations

from okg.substrate.source_sync_status import (
    SOURCE_SYNC_STATUS_SCHEMA,
    build_source_sync_status_contract,
    source_sync_status_for_source,
)


def test_status_contract_carries_same_convergence_object_to_source_slice(
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.source-sync-convergence.status-surface "
        "test.source-sync-convergence.status-surface",
    )
    source_inventory = [
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
    status = build_source_sync_status_contract(
        {
            "deployment": "demo",
            "producers": [],
            "expected_producers": [
                {
                    "source_name": "pact",
                    "producer": "watch",
                    "status": "healthy",
                }
            ],
            "disabled_producers": [],
            "missing_producers": [],
            "stale_producers": [],
            "config_warnings": [],
            "queue_depth": [],
            "dbos_queue_depth": {"active_count": 0},
            "dlq_count": 0,
        },
        source_freshness={
            "ok": False,
            "missing": [],
            "stale": ["demo.pact"],
            "groups": {},
        },
        source_inventory=source_inventory,
        active_work_by_source={
            "pact": {
                "workflow_id": "wf-complete",
                "status": "PENDING",
                "run_mode": "scope_complete",
                "stale": False,
            }
        },
    )
    mcp_slice = source_sync_status_for_source(status, "pact")

    assert status["schema"] == SOURCE_SYNC_STATUS_SCHEMA
    assert status["convergence"]["state"] == "converging"
    assert mcp_slice["schema"] == status["schema"]
    assert mcp_slice["convergence"]["source_name"] == "pact"
    assert mcp_slice["convergence"]["state"] == "converging"
    assert mcp_slice["convergence"]["active_work"]["workflow_id"] == (
        "wf-complete"
    )
