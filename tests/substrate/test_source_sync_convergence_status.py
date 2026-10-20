from __future__ import annotations

from types import SimpleNamespace

import pytest

from okg.substrate.source_sync_convergence import (
    SOURCE_SYNC_CONVERGENCE_SCHEMA,
    build_convergence_status,
    source_inventory_row,
)


def _pact_inventory() -> list[dict]:
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


def test_explicit_scope_incomplete_reconcile_is_not_completed_scope_capable() -> None:
    sync = SimpleNamespace(
        triggers=["poll", "manual", "reconcile"],
        default_event_mode="scope_incomplete",
        reconcile_mode="scope_incomplete",
        deletes_require_reconcile=True,
    )
    entry = SimpleNamespace(
        sync=sync,
        raw={"params": {"append_only": True}},
        source_class="discovery_crawl",
        ownership_id="demo.test-reports",
        enabled_by_default=True,
        credential_refs=[],
    )

    row = source_inventory_row(
        deployment="demo",
        source_name="test-reports",
        entry=entry,
        runtime_source_sync={"poll": {"enabled": True}},
        runtime_enabled=True,
    )

    assert row["freshness_policy"]["completed_scope_capable"] is False


def _freshness(status: str) -> dict:
    return {
        "ok": status == "fresh",
        "missing": [] if status != "missing" else ["demo.pact"],
        "stale": [] if status != "stale" else ["demo.pact"],
        "groups": {
            "pact": {
                "ok": status == "fresh",
                "sources": [
                    {
                        "source": "demo.pact",
                        "status": status,
                        "last_reconcile_at": "2026-06-04T00:00:00+00:00",
                        "age_seconds": 7200,
                    }
                ],
            }
        },
    }


def test_incomplete_work_cannot_satisfy_completed_scope_freshness() -> None:
    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=_pact_inventory(),
        source_freshness=_freshness("stale"),
        active_work_by_source={
            "pact": {
                "workflow_id": "wf-incomplete",
                "status": "ENQUEUED",
                "run_mode": "scope_incomplete",
                "stale": False,
            }
        },
    )

    pact = convergence["sources"][0]
    assert convergence["schema"] == SOURCE_SYNC_CONVERGENCE_SCHEMA
    assert convergence["state"] == "blocked"
    assert pact["state"] == "blocked"
    assert pact["reason"] == (
        "active_incomplete_work_does_not_satisfy_completed_freshness"
    )
    assert pact["blocking_stage"] == "completed_scope_watermark"
    assert pact["recovery_action"]["action_kind"] == "completed_scope_reconcile"
    assert pact["recovery_action"]["run_mode"] == "scope_complete"


def test_completed_scope_recovery_is_converging_until_watermark_moves() -> None:
    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=_pact_inventory(),
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

    pact = convergence["sources"][0]
    assert convergence["state"] == "converging"
    assert pact["state"] == "converging"
    assert pact["recovery_action"] == {
        "action_kind": "observe_active_completed_scope_recovery",
        "source_name": "pact",
        "workflow_id": "wf-complete",
        "run_mode": "scope_complete",
        "expected_watermark_movement": "last_reconcile_at_moves",
        "expected_publish_visibility": "new_published_generation_visible_to_mcp",
    }


def test_convergence_reports_selected_relevant_active_work_candidates() -> None:
    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=_pact_inventory(),
        source_freshness=_freshness("stale"),
        active_work_by_source={
            "pact": {
                "workflow_id": "wf-complete",
                "status": "ENQUEUED",
                "run_mode": "scope_complete",
                "stale": False,
                "candidate_count": 2,
                "selection_reason": "preferred_fresh_completed_scope",
                "candidate_workflow_ids": [
                    "wf-incomplete",
                    "wf-complete",
                ],
                "active_work_candidates": [
                    {
                        "workflow_id": "wf-incomplete",
                        "status": "ENQUEUED",
                        "run_mode": "scope_incomplete",
                        "stale": True,
                    },
                    {
                        "workflow_id": "wf-complete",
                        "status": "ENQUEUED",
                        "run_mode": "scope_complete",
                        "stale": False,
                    },
                ],
            }
        },
    )

    pact = convergence["sources"][0]
    assert convergence["state"] == "converging"
    assert pact["state"] == "converging"
    assert pact["active_work"]["workflow_id"] == "wf-complete"
    assert pact["active_work"]["candidate_count"] == 2
    assert pact["active_work"]["selection_reason"] == (
        "preferred_fresh_completed_scope"
    )
    assert pact["active_work"]["candidate_workflow_ids"] == [
        "wf-incomplete",
        "wf-complete",
    ]


def test_fresh_completed_scope_watermark_is_ready_despite_incomplete_work() -> None:
    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=_pact_inventory(),
        source_freshness=_freshness("fresh"),
        active_work_by_source={
            "pact": {
                "workflow_id": "wf-incomplete",
                "status": "ENQUEUED",
                "run_mode": "scope_incomplete",
                "stale": False,
            }
        },
        generation={"status": "published", "generation_id": "g1"},
        mcp={"ok": True, "pinned_generation_id": "g1"},
    )

    pact = convergence["sources"][0]
    assert convergence["state"] == "ready"
    assert pact["state"] == "fresh"
    assert pact["stages"]["completed_scope_watermark"]["status"] == "fresh"
    assert pact["stages"]["mcp_visible_generation"] == {
        "status": "ok",
        "generation_id": "g1",
    }


@pytest.mark.parametrize(
    ("generation", "mcp", "blocking_stage"),
    [
        (
            {"status": "missing", "generation_id": None},
            {"ok": True, "pinned_generation_id": "g-prior"},
            "admission_publish",
        ),
        (
            {"status": "published", "generation_id": "g1"},
            {"ok": False, "reason": "required_authority_unavailable"},
            "mcp_visible_generation",
        ),
    ],
)
def test_fresh_source_does_not_reenqueue_for_downstream_blocker(
    generation: dict,
    mcp: dict,
    blocking_stage: str,
) -> None:
    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=_pact_inventory(),
        source_freshness=_freshness("fresh"),
        generation=generation,
        mcp=mcp,
    )

    pact = convergence["sources"][0]
    assert convergence["state"] == "blocked"
    assert pact["blocking_stage"] == blocking_stage
    assert pact["recovery_action"] == {
        "action_kind": "no_op",
        "source_name": "pact",
        "reason": "source_fresh_downstream_readiness_blocked",
    }
    assert convergence["recovery_plan"]["action_count"] == 0
    assert convergence["next_action"] == {
        "code": "diagnose_blocking_stage",
        "owner": "maintainer",
        "source_name": "pact",
        "blocking_stage": blocking_stage,
    }
