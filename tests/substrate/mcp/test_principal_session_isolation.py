"""Request identity and continuation regressions without a database."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
import asyncio

import pytest

from okg.substrate.mcp import continuation, tools
from okg.substrate.mcp.principal_mapping import (
    PrincipalMapping,
    header_claim_for_request,
)
from okg.substrate.mcp.session import Session


MAPPING = PrincipalMapping(
    "provider", "issuer", "audience", "sub", "x-user", "mcp_client"
)


def test_verified_token_copy_precedes_sdk_auth_context_and_is_request_local():
    from mcp.server.auth.middleware.auth_context import (
        AuthContextMiddleware,
        get_access_token,
    )
    from mcp.server.auth.middleware.bearer_auth import (
        AuthenticatedUser,
        authorization_context,
    )
    from mcp.server.auth.provider import AccessToken
    from okg.substrate.mcp.principal_session import PrincipalSessionMiddleware

    token = AccessToken(
        token="test-verified-placeholder",
        client_id="trusted-app",
        scopes=["read"],
        claims={"iss": "token-issuer", "nested": {"x": 1}},
    )
    user = AuthenticatedUser(token)
    observed = []

    async def endpoint(scope, receive, send):
        local = scope["user"].access_token
        assert get_access_token() is local
        assert local is not token
        observed.append(authorization_context(scope["user"]))
        local.claims["nested"]["x"] = 99
        await asyncio.sleep(0)
        assert get_access_token() is local

    async def scenario():
        app = PrincipalSessionMiddleware(
            AuthContextMiddleware(endpoint), deployment_name="fixture", mapping=MAPPING
        )

        async def request(name):
            scope = {
                "type": "http",
                "headers": [(b"x-user", name.encode())],
                "user": user,
            }
            await app(scope, None, None)
            assert scope["user"] is user

        await asyncio.gather(request("alice"), request("bob"), request("alice"))
        assert get_access_token() is None

    asyncio.run(scenario())
    assert observed[0] == observed[2] and observed[0] != observed[1]
    assert all(
        item["client_id"] == "trusted-app" and item["issuer"] == "token-issuer"
        for item in observed
    )
    assert token.subject is None and token.claims["nested"] == {"x": 1}


@pytest.mark.parametrize(
    "field", ["provider_id", "issuer", "audience", "principal_kind"]
)
def test_session_owner_is_qualified_by_mapping(field):
    from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
    from mcp.server.auth.provider import AccessToken
    from okg.substrate.mcp.principal_session import PrincipalSessionMiddleware

    observed = []

    async def endpoint(scope, receive, send):
        observed.append(scope["user"].access_token.subject)

    async def scenario():
        scope = {
            "type": "http",
            "headers": [(b"x-user", b"alice")],
            "user": AuthenticatedUser(
                AccessToken(token="placeholder", client_id="app", scopes=[])
            ),
        }
        for mapping, deployment in (
            (MAPPING, "first"),
            (replace(MAPPING, **{field: "other"}), "first"),
            (MAPPING, "second"),
        ):
            await PrincipalSessionMiddleware(
                endpoint, deployment_name=deployment, mapping=mapping
            )(scope, None, None)

    asyncio.run(scenario())
    assert len(set(observed)) == 3


def test_dispatch_refuses_before_sdk_schema_lookup():
    from mcp import types
    from okg.substrate.errors import SubstrateError
    from okg.substrate.mcp.response_bounds import ByteBoundedFastMCP
    from okg.substrate.mcp.principal_session import install_dispatch_gate

    mcp = ByteBoundedFastMCP("gate-proof")

    def refuse(name):
        raise SubstrateError("principal refused")

    mcp._okg_dispatch_principal_gate = refuse
    install_dispatch_gate(mcp, refuse)

    async def forbidden(name):
        pytest.fail("unauthorized dispatch reached schema lookup")

    mcp._mcp_server._get_cached_tool_definition = forbidden

    async def scenario():
        request = types.CallToolRequest(
            method="tools/call",
            params=types.CallToolRequestParams(name="inspect", arguments={}),
        )
        result = await mcp._mcp_server.request_handlers[types.CallToolRequest](request)
        assert result.root.isError
        with pytest.raises(SubstrateError):
            await mcp._mcp_server.request_handlers[types.ListToolsRequest](
                types.ListToolsRequest(method="tools/list")
            )

    asyncio.run(scenario())


def test_installed_dispatch_gates_an_allowed_call_at_both_boundaries():
    from mcp import types
    from okg.substrate.mcp.operator_models import OperatorResult
    from okg.substrate.mcp.principal_session import install_dispatch_gate
    from okg.substrate.mcp.response_bounds import ByteBoundedFastMCP

    mcp = ByteBoundedFastMCP("gate-count-proof")
    checked: list[str] = []

    def allow(name):
        checked.append(name)

    @mcp.tool(name="inspect", structured_output=True)
    def inspect(target: dict) -> OperatorResult:
        return OperatorResult.model_validate(target)

    mcp._okg_dispatch_principal_gate = allow
    install_dispatch_gate(mcp, allow)

    async def scenario():
        request = types.CallToolRequest(
            method="tools/call",
            params=types.CallToolRequestParams(
                name="inspect", arguments={"target": {"kind": "surface"}}
            ),
        )
        result = await mcp._mcp_server.request_handlers[types.CallToolRequest](request)
        assert result.root.isError is False

    asyncio.run(scenario())
    assert checked.count("inspect") == 2


def test_multi_user_legacy_sse_refuses_before_server_start(monkeypatch):
    from okg.substrate.errors import SubstrateError
    from okg.substrate.mcp.server import run_server
    from okg.substrate.mcp.response_bounds import ByteBoundedFastMCP

    with pytest.raises(SubstrateError, match="legacy SSE"):
        run_server(
            transport="sse", multi_user_principals=True, auth_token_env="MCP_TEST_TOKEN"
        )
    mcp = ByteBoundedFastMCP("sse-proof")
    mcp._okg_principal_mapping = MAPPING
    with pytest.raises(RuntimeError, match="legacy SSE"):
        mcp.sse_app()
    mcp._okg_principal_mapping = None
    assert mcp.sse_app().routes
    from okg.substrate.mcp import server
    from okg.substrate.db import capacity

    calls = []
    monkeypatch.setattr(mcp, "run", lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(server, "build_server", lambda **kwargs: mcp)
    monkeypatch.setattr(capacity, "release_process_reservations", lambda **kwargs: None)
    run_server(transport="sse")
    assert calls == [{"transport": "sse"}]


def _session(user="alice", *, now=None):
    return Session(
        deployment_name="fixture",
        generation_id="1",
        catalog_version_id=1,
        db_fingerprint="database",
        nomos_multi_user=True,
        nomos_identity_claim=header_claim_for_request(
            MAPPING, {"x-user": user}, transport_identity="mcp.transport:app", now=now
        ),
    )


def test_same_principal_continuation_survives_fresh_verification(monkeypatch):
    monkeypatch.setattr(continuation, "load_active_policy_bundle", lambda *a, **k: None)
    now = datetime(2026, 9, 9, tzinfo=UTC)
    first = _session(now=now)
    fresh = _session(now=now + timedelta(seconds=1))
    before = dict(first.nomos_identity_claim)

    def binding(session):
        return continuation.continuation_binding(
            None,
            session,
            kind="subtype_summary",
            subtype="page",
            sort="node_id:asc",
            projection={},
        )

    token = continuation.issue_continuation(
        binding(first), cursor={"after_node_id": "a"}
    )
    assert continuation.consume_continuation(
        token, expected_binding=binding(fresh)
    ) == {"after_node_id": "a"}
    assert first.nomos_identity_claim == before
    for other in (
        _session("bob", now=now),
        replace(fresh, nomos_access_context={"purpose": "different"}),
    ):
        with pytest.raises(continuation.ContinuationTokenInvalid):
            continuation.consume_continuation(token, expected_binding=binding(other))


def test_path_continuation_refuses_changed_start(monkeypatch):
    """Exercise real path binding/issuance/consumption around a fixed scan."""
    from okg.substrate.mcp import provenance

    monkeypatch.setattr(continuation, "load_active_policy_bundle", lambda *a, **k: None)
    monkeypatch.setattr(tools, "assert_session_matches", lambda *a, **k: None)
    monkeypatch.setattr(tools, "validate_path_edge_types", lambda *a, **k: {})
    monkeypatch.setattr(tools, "start_node_visibility", lambda *a, **k: True)
    monkeypatch.setattr(tools, "deployment_nomos_enforcement_mode", lambda *a: "audit")
    monkeypatch.setattr(
        tools, "apply_effective_attrs_to_rows", lambda conn, rows, **k: rows
    )
    monkeypatch.setattr(
        provenance, "serialize_node_read_rows", lambda conn, rows, **k: rows
    )
    rows = [
        {"node_id": name, "subtype": "page", "attrs": {}, "provenance": {}}
        for name in ("a", "b")
    ]
    monkeypatch.setattr(tools, "scan_visible_path", lambda *a, **k: (rows, True, False))
    session = _session(now=datetime(2026, 9, 9, tzinfo=UTC))
    first = tools.expand_path(
        None, session, "start-a", hops=[("edge", "out")], limit=1, select=["node_id"]
    )
    token = first["continuation_token"]
    tools.expand_path(
        None,
        session,
        "start-a",
        hops=[("edge", "out")],
        limit=1,
        select=["node_id"],
        continuation_token=token,
    )
    with pytest.raises(continuation.ContinuationTokenInvalid):
        tools.expand_path(
            None,
            session,
            "start-b",
            hops=[("edge", "out")],
            limit=1,
            select=["node_id"],
            continuation_token=token,
        )
