"""adopt-act-mcp-routes: the act surface is approval-gated, pinned
to hardened modes, refused loudly on unmet conditions, structurally
separate from first-setup ingest routes — and the round-trip leaks
no token material."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

from okg.substrate.library.sources.discovery import (
    ConnectorRoute,
    select_connector_route,
)
from okg.substrate.library.sources.discovery.act_routes import (
    VETTED_ACT_ROUTES,
    ActRouteApprovalError,
    ActRouteModeError,
    activate_act_route,
)
from tests.substrate._fake_credentials import fake_credential

_WORKSPACE_MODES_OK = {
    "auth": "oauth2.1_multi_user",
    "credential_store": "stateless_memory",
    "tool_scope": "read_only",
}

_MS365_MODES_OK = {
    "credential_store": "os_keychain",
    "transport_auth": "bearer",
    "tool_scope": "allowed_scopes",
}


def test_unpinned_mode_refused_with_named_condition() -> None:
    """Each pinned condition, when unmet, refuses naming THAT
    condition — no silent downgrade to a weaker mode."""
    cases = [
        ("google_workspace", _WORKSPACE_MODES_OK, "auth", "single_user"),
        (
            "google_workspace", _WORKSPACE_MODES_OK,
            "credential_store", "plaintext_json",
        ),
        ("google_workspace", _WORKSPACE_MODES_OK, "tool_scope", "full"),
        ("ms365", _MS365_MODES_OK, "credential_store", "file_0600"),
        ("ms365", _MS365_MODES_OK, "transport_auth", "none"),
    ]
    for name, good_modes, condition, bad_value in cases:
        declared = {**good_modes, condition: bad_value}
        with pytest.raises(ActRouteModeError) as exc_info:
            activate_act_route(
                name, declared_modes=declared, operator_approved=True,
            )
        message = str(exc_info.value)
        assert condition in message, (name, condition)
        assert repr(bad_value) in message
        assert condition in exc_info.value.unmet


def test_missing_condition_counts_as_unmet() -> None:
    declared = dict(_WORKSPACE_MODES_OK)
    declared.pop("credential_store")
    with pytest.raises(ActRouteModeError) as exc_info:
        activate_act_route(
            "google_workspace",
            declared_modes=declared,
            operator_approved=True,
        )
    assert "credential_store" in exc_info.value.unmet


def test_fully_pinned_modes_activate() -> None:
    route = activate_act_route(
        "google_workspace",
        declared_modes=_WORKSPACE_MODES_OK,
        operator_approved=True,
    )
    assert route.server == "taylorwilsdon/google_workspace_mcp"
    ms = activate_act_route(
        "ms365",
        declared_modes={
            **_MS365_MODES_OK, "credential_store": "azure_key_vault",
        },
        operator_approved=True,
    )
    assert ms.server == "softeria/ms-365-mcp-server"


def test_act_route_requires_post_setup_operator_approval() -> None:
    """Approval is the first gate: even fully pinned modes refuse
    without it — and first-setup ingest selection cannot see act
    routes at all (they are not ConnectorRoutes; the read-only
    constructor constraint stands)."""
    with pytest.raises(ActRouteApprovalError):
        activate_act_route(
            "google_workspace",
            declared_modes=_WORKSPACE_MODES_OK,
            operator_approved=False,
        )
    # Structural separation: a write-capable ConnectorRoute cannot
    # even be constructed, so propose_setup's route pool is
    # read-only by type.
    with pytest.raises(ValueError, match="supports_write"):
        ConnectorRoute(
            source="gcal",
            route_id="gcal:provider_mcp:google",
            route_kind="provider_mcp",
            provider="google",
            label="write route",
            auth_owner="provider",
            trust_tier="provider_maintained",
            data_path="x",
            supports_write=True,
        )
    # And the act registry itself contains no first-setup route ids.
    ingest_selection = select_connector_route([])
    assert ingest_selection is None
    assert all(
        ":act_mcp:" in route.route_id
        for route in VETTED_ACT_ROUTES.values()
    )


def test_unknown_act_route_refused() -> None:
    with pytest.raises(ValueError, match="unknown act route"):
        activate_act_route(
            "composio", declared_modes={}, operator_approved=True,
        )


def test_route_status_reports_act_routes() -> None:
    """connector_route_status's act section: pinned modes, custody,
    and the honest not-activated approval state, per provider."""
    from okg.substrate.mcp.connector_ingest import _act_routes_for_source

    for source_id, expected_server in [
        ("gmail_api", "taylorwilsdon/google_workspace_mcp"),
        ("google_calendar_api", "taylorwilsdon/google_workspace_mcp"),
        ("outlook_mail_api", "softeria/ms-365-mcp-server"),
        ("outlook_calendar_api", "softeria/ms-365-mcp-server"),
    ]:
        rows = _act_routes_for_source(source_id)
        assert len(rows) == 1, source_id
        row = rows[0]
        assert row["server"] == expected_server
        assert row["operator_approved"] is False
        assert row["activation_state"] == "available_not_activated"
        assert row["pinned_modes"]
        assert row["token_custody"]
    assert _act_routes_for_source("slack_api") == []


def test_read_then_create_roundtrip_no_token_in_output() -> None:
    """The act round-trip through a real MCP stdio client: read
    events, create one, read again — with a sentinel credential
    held server-side that must never appear in any tool output."""
    from okg.substrate.library.sources.mcp_composed.client import McpClient

    sentinel = fake_credential("ya29.", "ACT-SENTINEL-TOKEN-MATERIAL-9001")
    stub = Path(__file__).parent / "act_route_stub_server.py"
    env = {**os.environ, "ACT_STUB_TOKEN": sentinel}

    route = activate_act_route(
        "google_workspace",
        declared_modes=_WORKSPACE_MODES_OK,
        operator_approved=True,
    )
    outputs: list[str] = []
    with McpClient([sys.executable, str(stub)], env=env) as cli:
        tools = cli.list_tools()
        assert {t["name"] for t in tools} == set(route.allowed_tools)
        read1 = cli.call_tool("calendar_read", {})
        outputs.append(json.dumps(read1, default=str))
        created = cli.call_tool("calendar_create", {
            "title": "okg: thesis defense hold",
            "start": "2026-06-20T15:00:00Z",
        })
        outputs.append(json.dumps(created, default=str))
        read2 = cli.call_tool("calendar_read", {})
        outputs.append(json.dumps(read2, default=str))

    assert "thesis defense hold" in outputs[2]
    from okg.substrate.cli.connector_security import find_token_leaks
    leaks = find_token_leaks(
        payloads=[(f"act:{i}", text) for i, text in enumerate(outputs)],
        tokens=[sentinel],
    )
    assert leaks == [], "act round-trip leaked token material"
