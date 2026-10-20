"""Source-admin tools are explicit write/admin MCP tools, not core AKMON
read operators.
"""
from __future__ import annotations


def test_source_admin_tools_are_not_default_operator_surface() -> None:
    from okg.substrate.mcp.operators import OPERATOR_TOOLS, WRITE_ADMIN_TOOLS

    for tool in (
        "list_sources", "add_source", "remove_source", "source_status",
        "discover_sources", "propose_setup", "commit_setup",
        "connector_route_status", "connector_ingest_batch",
    ):
        assert tool not in OPERATOR_TOOLS
        assert tool in WRITE_ADMIN_TOOLS
