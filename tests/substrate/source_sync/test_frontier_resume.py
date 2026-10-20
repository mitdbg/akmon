from __future__ import annotations

from okg.substrate.source_sync_convergence import (
    build_convergence_status,
    source_convergence_for_source,
)


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
                "triggers": ["manual", "reconcile"],
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
                        "last_reconcile_at": "2026-06-13T19:00:00+00:00",
                        "age_seconds": 30 if status == "fresh" else 7200,
                    }
                ],
            }
        },
    }


def _pact_source(convergence: dict) -> dict:
    return source_convergence_for_source(convergence, "pact")


def test_active_completed_scope_work_is_converging_not_complete() -> None:
    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=_inventory(),
        source_freshness=_freshness("stale"),
        active_work_by_source={
            "pact": {
                "workflow_id": "wf-resume",
                "status": "PENDING",
                "run_mode": "scope_complete",
                "stale": False,
            }
        },
        generation={"status": "published", "generation_id": "g1"},
        mcp={"ok": True, "pinned_generation_id": "g1"},
    )

    pact = _pact_source(convergence)
    assert convergence["state"] == "converging"
    assert convergence["ok"] is False
    assert pact["state"] == "converging"
    assert pact["reason"] == "completed_scope_recovery_active"
    assert pact["stages"]["dbos_workflow"]["status"] == "active"
    assert pact["stages"]["completed_scope_watermark"]["status"] == "stale"


def test_fresh_watermark_without_publish_is_not_ready() -> None:
    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=_inventory(),
        source_freshness=_freshness("fresh"),
        generation={"status": "blocked", "generation_id": "g1"},
        mcp={"ok": True, "pinned_generation_id": "g1"},
    )

    pact = _pact_source(convergence)
    assert convergence["state"] == "blocked"
    assert pact["state"] == "blocked"
    assert pact["reason"] == "published_generation_not_observed"
    assert pact["blocking_stage"] == "admission_publish"
    assert pact["stages"]["completed_scope_watermark"]["status"] == "fresh"


def test_published_generation_without_mcp_visibility_is_not_ready() -> None:
    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=_inventory(),
        source_freshness=_freshness("fresh"),
        generation={"status": "published", "generation_id": "g1"},
        mcp={"ok": False},
    )

    pact = _pact_source(convergence)
    assert convergence["state"] == "blocked"
    assert pact["state"] == "blocked"
    assert pact["reason"] == "published_generation_not_visible_to_read_side"
    assert pact["blocking_stage"] == "mcp_visible_generation"
    assert pact["stages"]["admission_publish"]["status"] == "published"


def test_frontier_ready_after_watermark_publish_and_mcp_visibility() -> None:
    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=_inventory(),
        source_freshness=_freshness("fresh"),
        generation={"status": "published", "generation_id": "g1"},
        mcp={"ok": True, "pinned_generation_id": "g1"},
    )

    pact = _pact_source(convergence)
    assert convergence["state"] == "ready"
    assert convergence["ok"] is True
    assert pact["state"] == "fresh"
    assert pact["stages"]["completed_scope_watermark"]["status"] == "fresh"
    assert pact["stages"]["admission_publish"]["status"] == "published"
    assert pact["stages"]["mcp_visible_generation"] == {
        "status": "ok",
        "generation_id": "g1",
    }
