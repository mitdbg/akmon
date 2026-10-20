"""The operator UI's read identity (PACT operator-ui-search-view)."""
from __future__ import annotations

from types import SimpleNamespace

from okg.substrate.nomos.read_filter import session_read_identity
from okg.substrate.nomos.runtime_enforcement import nomos_runtime_production_bridge_inventory
from okg.substrate.nomos.store import deployment_baseline_policy_graph
from okg.substrate.operator_read import (
    LOCAL_OPERATOR_BOUNDARY_KIND,
    LOCAL_OPERATOR_READ_ACTION,
    LOCAL_OPERATOR_READ_SURFACE,
)


def test_a_session_without_a_read_identity_keeps_the_mcp_identity() -> None:
    assert session_read_identity(SimpleNamespace(), action=None, surface=None) == (
        "graph.read.mcp",
        "mcp",
    )


def test_a_session_names_the_surface_its_reads_come_through() -> None:
    session = SimpleNamespace(
        nomos_read_action=LOCAL_OPERATOR_READ_ACTION,
        nomos_read_surface=LOCAL_OPERATOR_READ_SURFACE,
    )

    assert session_read_identity(session, action=None, surface=None) == (
        "graph.read.local_operator",
        "local_operator",
    )


def test_an_explicit_action_wins_over_the_session() -> None:
    session = SimpleNamespace(
        nomos_read_action=LOCAL_OPERATOR_READ_ACTION,
        nomos_read_surface=LOCAL_OPERATOR_READ_SURFACE,
    )

    assert session_read_identity(
        session, action="graph.read.sql_admin", surface="mcp.tool.call",
    ) == ("graph.read.sql_admin", "mcp.tool.call")


def test_the_baseline_policy_grants_the_operator_ui_boundary_to_read_principals() -> None:
    policy = deployment_baseline_policy_graph(
        {
            "name": "demo",
            "nomos": {
                "runtime_enforcement": "enforce",
                "deployment_posture": {"read_principals": ["human_operator"]},
            },
        },
        deployment_name="demo",
        scope="deployment:demo",
        activated_by="subject:test",
        source_names=["docs"],
    )

    boundary_rules = [
        rule for rule in policy["rules"]
        if LOCAL_OPERATOR_READ_ACTION in rule["actions"]
        and rule["match"].get("resource_kind") == LOCAL_OPERATOR_BOUNDARY_KIND
    ]
    assert len(boundary_rules) == 1
    assert boundary_rules[0]["effect"] == "allow"
    assert "subject:human_operator" in boundary_rules[0]["subjects"]


def test_the_runtime_inventory_reports_the_operator_ui_surface_wired() -> None:
    rows = {
        row["surface"]: row for row in nomos_runtime_production_bridge_inventory()
    }

    assert rows["graph.read.local_operator"]["status"] == "wired"
    assert rows["graph.read.local_operator"]["verification"] == (
        "tests/substrate/viz/test_operator_record_reads.py"
    )
