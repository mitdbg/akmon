from __future__ import annotations

from okg.substrate.source_sync_convergence import build_convergence_status


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


def test_scope_incomplete_does_not_satisfy_completed_freshness(
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.source-sync-convergence.state-model "
        "test.source-sync-convergence.state-model",
    )
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
    assert convergence["state"] == "blocked"
    assert pact["state"] == "blocked"
    assert pact["reason"] == (
        "active_incomplete_work_does_not_satisfy_completed_freshness"
    )
    assert pact["blocking_stage"] == "completed_scope_watermark"


def test_ready_requires_watermark_and_published_generation(
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.source-sync-convergence.state-model "
        "test.source-sync-convergence.state-model",
    )
    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=_pact_inventory(),
        source_freshness=_freshness("fresh"),
        generation={"status": "published", "generation_id": "g1"},
        mcp={"ok": True, "pinned_generation_id": "g1"},
    )

    pact = convergence["sources"][0]
    assert convergence["state"] == "ready"
    assert convergence["ok"] is True
    assert pact["stages"]["completed_scope_watermark"]["status"] == "fresh"
    assert pact["stages"]["admission_publish"]["status"] == "published"
    assert pact["stages"]["mcp_visible_generation"] == {
        "status": "ok",
        "generation_id": "g1",
    }


def test_ready_uses_registry_ownership_identity_for_freshness() -> None:
    inventory = _pact_inventory()
    inventory[0]["ownership_id"] = "shared.pact"
    freshness = _freshness("fresh")
    freshness["groups"]["pact"]["sources"][0]["source"] = "shared.pact"

    convergence = build_convergence_status(
        deployment="demo",
        source_inventory=inventory,
        source_freshness=freshness,
        generation={"status": "published", "generation_id": "g1"},
        mcp={"ok": True, "pinned_generation_id": "g1"},
    )

    assert convergence["ok"] is True
    assert [row["source_name"] for row in convergence["sources"]] == ["pact"]
    assert convergence["sources"][0]["freshness"]["source"] == "shared.pact"
