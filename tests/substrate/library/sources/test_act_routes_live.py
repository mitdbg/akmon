"""Env-gated live verification for adopt-act-mcp-routes.

Skipped unless OKG_LIVE_ACT_MCP=1 with a real internal Google
OAuth app (workspace-mcp's env: GOOGLE_OAUTH_CLIENT_ID/SECRET) and
`uvx workspace-mcp` available. Until those exist on a host this is
an environment blocker on task.act.roundtrip, not a waiver.
"""
from __future__ import annotations

import json
import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("OKG_LIVE_ACT_MCP") != "1",
    reason="live workspace-mcp test is env-gated (OKG_LIVE_ACT_MCP=1)",
)


def test_workspace_mcp_stateless_roundtrip() -> None:
    from okg.substrate.cli.connector_security import find_token_leaks
    from okg.substrate.library.sources.discovery.act_routes import (
        activate_act_route,
    )
    from okg.substrate.library.sources.mcp_composed.client import McpClient

    route = activate_act_route(
        "google_workspace",
        declared_modes={
            "auth": "oauth2.1_multi_user",
            "credential_store": "stateless_memory",
            "tool_scope": "read_only",
        },
        operator_approved=os.environ.get("OKG_ACT_APPROVED") == "1",
    )
    # Stateless mode + calendar-only tools, per the pinned route.
    cmd = [
        "uvx", "workspace-mcp",
        "--tools", "calendar",
        "--read-only",
    ]
    outputs: list[str] = []
    with McpClient(cmd, env={**os.environ, "WORKSPACE_MCP_STATELESS_MODE": "true"}) as cli:
        tools = cli.list_tools()
        assert any("calendar" in t["name"].lower() for t in tools)
        listed = cli.call_tool(
            "list_calendars", {"user_google_email": os.environ["OKG_LIVE_GOOGLE_EMAIL"]},
        )
        outputs.append(json.dumps(listed, default=str))

    tokens = [
        os.environ[var]
        for var in ("GOOGLE_OAUTH_CLIENT_SECRET",)
        if os.environ.get(var)
    ]
    assert find_token_leaks(
        payloads=[(f"live:{i}", t) for i, t in enumerate(outputs)],
        tokens=tokens,
    ) == []
    assert route.route_id == "google-workspace:act_mcp:workspace-mcp"
