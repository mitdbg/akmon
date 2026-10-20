"""Principal isolation on real HTTP sessions using an owned PostgreSQL fixture."""

import asyncio
from datetime import timedelta
import subprocess
import sys

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
import psycopg
import pytest

from tests.substrate.mcp._result_payload import (
    json_client_sessions,
    payload as _result_payload,
)

from tests.substrate.mcp.test_multi_user_http_e2e import (
    DEPLOYMENT,
    PRINCIPAL_A,
    PRINCIPAL_B,
    PRIVATE_NODE,
    PUBLIC_NODE,
    _FILTER_ARGUMENTS,
    _TOKEN_ENV,
    _REPO_ROOT,
    _free_port,
    _headers,
    _node_ids,
    _server_env,
    _stop_server,
    _wait_until_listening,
    two_principal_dsn as two_principal_dsn,
)
from tests.substrate.mcp.test_tool_list_expansion_e2e import _Notifications

pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch: pytest.MonkeyPatch) -> None:
    """These tests assert on the dictionary, so every client call asks for
    the JSON result format (issue #3059: the default is one text block)."""
    json_client_sessions(monkeypatch)


@pytest.fixture(scope="module")
def isolation_server(two_principal_dsn, tmp_path_factory):
    port = _free_port()
    log_path = tmp_path_factory.mktemp("principal-sessions") / "server.log"
    argv = [
        sys.executable,
        "-m",
        "okg.substrate.cli.main",
        "mcp-serve",
        "--deployment",
        DEPLOYMENT,
        "--dsn",
        two_principal_dsn,
        "--transport",
        "streamable-http",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--auth-token-env",
        _TOKEN_ENV,
        "--multi-user-principals",
        "--tools",
        "discovery",
        "--tools-ceiling",
        "reader",
    ]
    with log_path.open("wb") as log:
        proc = subprocess.Popen(
            argv,
            cwd=_REPO_ROOT,
            env=_server_env(two_principal_dsn),
            stdout=log,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    try:
        assert _wait_until_listening(proc, port, log_path), log_path.read_text()[-3000:]
        yield f"http://127.0.0.1:{port}/mcp"
    finally:
        _stop_server(proc)


def _payload(result):
    return _result_payload(result)


def test_interleaved_sessions_refuse_cross_user_transport_reuse(
    isolation_server, two_principal_dsn, monkeypatch
):
    async def scenario():
        notes_a, notes_b = _Notifications(), _Notifications()
        async with streamablehttp_client(
            isolation_server, headers=_headers(PRINCIPAL_A)
        ) as (ra, wa, id_a):
            async with ClientSession(
                ra,
                wa,
                message_handler=notes_a.handle,
                read_timeout_seconds=timedelta(seconds=20),
            ) as a:
                await a.initialize()
                async with streamablehttp_client(
                    isolation_server, headers=_headers(PRINCIPAL_B)
                ) as (rb, wb, id_b):
                    async with ClientSession(
                        rb,
                        wb,
                        message_handler=notes_b.handle,
                        read_timeout_seconds=timedelta(seconds=20),
                    ) as b:
                        await b.initialize()
                        assert id_a() and id_a() != id_b()
                        assert {t.name for t in (await a.list_tools()).tools} == {
                            "inspect",
                            "search",
                            "aggregate",
                        }
                        await a.call_tool(
                            "inspect",
                            {"target": {"kind": "surface", "action": "expand"}},
                        )
                        assert "filter" in {
                            t.name for t in (await a.list_tools()).tools
                        }
                        assert "filter" not in {
                            t.name for t in (await b.list_tools()).tools
                        }
                        assert (
                            notes_a.tool_list_changed == 1
                            and notes_b.tool_list_changed == 0
                        )
                        # Readiness preserves this session's projection and describes
                        # only observed route wrapping, not a live credential probe.
                        for client_session, expanded in ((a, True), (b, False)):
                            surface = _payload(
                                await client_session.call_tool(
                                    "inspect", {"target": {"kind": "surface"}}
                                )
                            )
                            readiness = surface["principal_readiness"]
                            assert readiness == {
                                "runtime_mode": "multi_user",
                                "credential_verifier": "configured",
                                "mapping": {
                                    "status": "configured",
                                    "source": "startup_parsed",
                                },
                                "provider_validation": {"status": "validated"},
                                "http_protection": "auth_wrapper_observed",
                                "http_credential_probe": "unmeasured",
                            }
                            assert (
                                surface["tool_surface"]["expansion"]["expanded"]
                                is expanded
                            )
                        # The SDK must reject before any list/call/expansion dispatch,
                        # and a forged DELETE must leave the legitimate session live.
                        async with httpx.AsyncClient(timeout=10) as client:
                            headers = {
                                **_headers(PRINCIPAL_B),
                                "Mcp-Session-Id": id_a(),
                                "Mcp-Protocol-Version": "2025-11-25",
                                "Accept": "application/json, text/event-stream",
                            }
                            bodies = [
                                {"method": "tools/list"},
                                {
                                    "method": "tools/call",
                                    "params": {
                                        "name": "filter",
                                        "arguments": _FILTER_ARGUMENTS,
                                    },
                                },
                                {
                                    "method": "tools/call",
                                    "params": {
                                        "name": "inspect",
                                        "arguments": {
                                            "target": {
                                                "kind": "surface",
                                                "action": "expand",
                                            }
                                        },
                                    },
                                },
                            ]
                            for index, body in enumerate(bodies):
                                response = await client.post(
                                    isolation_server,
                                    headers=headers,
                                    json={"jsonrpc": "2.0", "id": index + 1, **body},
                                )
                                assert response.status_code == 404, response.text
                            for method in ("GET", "DELETE"):
                                response = await client.request(
                                    method, isolation_server, headers=headers
                                )
                                assert response.status_code == 404, response.text
                        assert notes_b.tool_list_changed == 0
                        await b.call_tool(
                            "inspect",
                            {"target": {"kind": "surface", "action": "expand"}},
                        )
                        for _ in range(3):
                            result_a, result_b = await asyncio.gather(
                                a.call_tool("filter", _FILTER_ARGUMENTS),
                                b.call_tool("filter", _FILTER_ARGUMENTS),
                            )
                            assert _node_ids(_payload(result_a)) == {
                                PRIVATE_NODE,
                                PUBLIC_NODE,
                            }
                            assert _node_ids(_payload(result_b)) == {PUBLIC_NODE}
                        assert (
                            notes_a.tool_list_changed == notes_b.tool_list_changed == 1
                        )
        # A newly initialized principal starts narrow after both old sessions close.
        async with streamablehttp_client(
            isolation_server, headers=_headers(PRINCIPAL_A)
        ) as (read, write, new_id):
            async with ClientSession(read, write) as fresh:
                await fresh.initialize()
                assert new_id() != id_a()
                assert "filter" not in {
                    t.name for t in (await fresh.list_tools()).tools
                }

    asyncio.run(scenario())

    # A failed provider SELECT must not abort this caller's transaction or
    # roll back its preceding work before the ordinary graph probe continues.
    from okg.substrate.mcp.principal_readiness import (
        load_readiness_providers,
        offline_principal_readiness,
        runtime_principal_readiness,
    )
    from okg.substrate.mcp.server import resolve_principal_mapping
    from okg.substrate.mcp.http_auth import StaticTokenVerifier, build_auth_settings
    from mcp.server.fastmcp import FastMCP
    import okg.substrate.nomos.identity_store as identity_store

    def broken_provider_select(conn, **kwargs):
        conn.execute("SELECT 1 / 0")

    with psycopg.connect(two_principal_dsn) as conn:
        conn.execute("CREATE TEMP TABLE readiness_caller_work (value integer)")
        conn.execute("INSERT INTO readiness_caller_work VALUES (7)")
        with monkeypatch.context() as patch:
            patch.setattr(
                identity_store, "load_identity_providers", broken_provider_select
            )
            report = offline_principal_readiness(conn, DEPLOYMENT)
            assert report["provider_validation"] == {
                "status": "unmeasured",
                "reason": "provider_probe_unavailable",
            }
            unserved = FastMCP(
                "transaction-proof",
                token_verifier=StaticTokenVerifier("test"),
                auth=build_auth_settings(),
            )
            unserved._okg_multi_user_principals = True
            unserved._okg_principal_mapping = resolve_principal_mapping(DEPLOYMENT)
            live = runtime_principal_readiness(
                unserved,
                deployment=DEPLOYMENT,
                load_providers=lambda: load_readiness_providers(conn, DEPLOYMENT),
            )
            assert live["provider_validation"] == report["provider_validation"]
        assert conn.execute("SELECT value FROM readiness_caller_work").fetchone() == (
            7,
        )
        from okg.substrate.mcp.session import open_session
        from okg.substrate.mcp.tools import inspect_graph

        session = open_session(conn, deployment_name=DEPLOYMENT)
        assert inspect_graph(conn, session)["node_count"] == 2


def test_revoked_binding_refuses_existing_session_list_and_call(
    isolation_server, two_principal_dsn
):
    async def scenario():
        async with streamablehttp_client(
            isolation_server, headers=_headers(PRINCIPAL_A)
        ) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                await session.list_tools()
                with psycopg.connect(two_principal_dsn, autocommit=True) as conn:
                    conn.execute(
                        "UPDATE okg.nomos_subject_bindings SET status='disabled' WHERE deployment_name=%s AND claim_value=%s",
                        (DEPLOYMENT, PRINCIPAL_A),
                    )
                try:
                    from mcp.shared.exceptions import McpError

                    with pytest.raises(McpError):
                        await session.list_tools()
                    refusal = _payload(
                        await session.call_tool(
                            "inspect",
                            {"target": {"kind": "surface", "action": "expand"}},
                        )
                    )
                    assert refusal["status"] == "refused", refusal
                finally:
                    with psycopg.connect(two_principal_dsn, autocommit=True) as conn:
                        conn.execute(
                            "UPDATE okg.nomos_subject_bindings SET status='active' WHERE deployment_name=%s AND claim_value=%s",
                            (DEPLOYMENT, PRINCIPAL_A),
                        )
                assert "filter" not in {
                    t.name for t in (await session.list_tools()).tools
                }

    asyncio.run(scenario())


def test_same_user_http_continuation_and_cross_user_refusal(isolation_server):
    async def scenario():
        target = {"kind": "subtype", "subtype": "page", "page_size": 1}
        async with streamablehttp_client(
            isolation_server, headers=_headers(PRINCIPAL_A)
        ) as (read, write, _):
            async with ClientSession(read, write) as a:
                await a.initialize()
                first = _payload(await a.call_tool("inspect", {"target": target}))
                token = first.get("continuation_token")
                assert token, first
                same = _payload(
                    await a.call_tool(
                        "inspect", {"target": {**target, "continuation_token": token}}
                    )
                )
                assert same.get("status") != "refused", same
                async with streamablehttp_client(
                    isolation_server, headers=_headers(PRINCIPAL_B)
                ) as (rb, wb, _):
                    async with ClientSession(rb, wb) as b:
                        await b.initialize()
                        other = _payload(
                            await b.call_tool(
                                "inspect",
                                {"target": {**target, "continuation_token": token}},
                            )
                        )
                        assert other["error"]["code"] == "continuation_token_invalid", (
                            other
                        )

    asyncio.run(scenario())


def test_grant_revocation_changes_next_read_with_binding_active(
    isolation_server, two_principal_dsn
):
    from tests.substrate.mcp.test_multi_user_http_e2e import (
        _decision_watermark,
        _decisions_since,
    )

    async def scenario():
        async with streamablehttp_client(
            isolation_server, headers=_headers(PRINCIPAL_A)
        ) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                await session.call_tool(
                    "inspect", {"target": {"kind": "surface", "action": "expand"}}
                )
                before = _payload(await session.call_tool("filter", _FILTER_ARGUMENTS))
                assert _node_ids(before) == {PUBLIC_NODE, PRIVATE_NODE}
                watermark = _decision_watermark(two_principal_dsn)
                with psycopg.connect(two_principal_dsn, autocommit=True) as conn:
                    changed = conn.execute(
                        "UPDATE okg.nomos_permission_grants SET status='disabled' WHERE deployment_name=%s AND grant_id=%s RETURNING grant_id",
                        (DEPLOYMENT, "grant:subject-chat-alice:google_drive"),
                    ).fetchall()
                    assert len(changed) == 1
                    assert conn.execute(
                        "SELECT status FROM okg.nomos_subject_bindings WHERE deployment_name=%s AND claim_value=%s",
                        (DEPLOYMENT, PRINCIPAL_A),
                    ).fetchone() == ("active",)
                try:
                    assert "filter" in {
                        t.name for t in (await session.list_tools()).tools
                    }
                    after = _payload(
                        await session.call_tool("filter", _FILTER_ARGUMENTS)
                    )
                    assert _node_ids(after) == {PUBLIC_NODE}, after
                    decisions = _decisions_since(two_principal_dsn, watermark)
                    assert any(
                        row["reason_code"] == "nomos.identity.no_permission_grant"
                        for row in decisions
                    ), decisions
                finally:
                    with psycopg.connect(two_principal_dsn, autocommit=True) as conn:
                        conn.execute(
                            "UPDATE okg.nomos_permission_grants SET status='active' WHERE deployment_name=%s AND grant_id=%s",
                            (DEPLOYMENT, "grant:subject-chat-alice:google_drive"),
                        )
                restored = _payload(
                    await session.call_tool("filter", _FILTER_ARGUMENTS)
                )
                assert _node_ids(restored) == {PUBLIC_NODE, PRIVATE_NODE}

    asyncio.run(scenario())


def test_refresh_changes_only_owners_generation_and_audit(
    isolation_server, two_principal_dsn
):
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer
    from tests.substrate.mcp.test_multi_user_http_e2e import (
        _decision_watermark,
        _decisions_since,
        SUBJECT_A,
        SUBJECT_B,
    )

    async def scenario():
        watermark = _decision_watermark(two_principal_dsn)
        async with streamablehttp_client(
            isolation_server, headers=_headers(PRINCIPAL_A)
        ) as (ra, wa, _):
            async with ClientSession(ra, wa) as a:
                await a.initialize()
                async with streamablehttp_client(
                    isolation_server, headers=_headers(PRINCIPAL_B)
                ) as (rb, wb, _):
                    async with ClientSession(rb, wb) as b:
                        await b.initialize()

                        async def state(session):
                            return _payload(
                                await session.call_tool(
                                    "inspect", {"target": {"kind": "session"}}
                                )
                            )

                        first_a, first_b = await asyncio.gather(state(a), state(b))
                        old = first_a["generation_id"]
                        assert first_b["generation_id"] == old
                        assert (
                            first_a["logical_session_id"]
                            != first_b["logical_session_id"]
                        )
                        with psycopg.connect(
                            two_principal_dsn, autocommit=True
                        ) as conn:
                            ChronosGraphStore.from_connection(conn).apply_facts(
                                [
                                    NodeFact(
                                        PUBLIC_NODE,
                                        "page",
                                        {
                                            "classification": "public",
                                            "text": "new generation",
                                        },
                                        {"id": PUBLIC_NODE},
                                        {"v": 2},
                                    )
                                ],
                                source="public_docs",
                            )
                        published = Publisher(
                            two_principal_dsn,
                            DEPLOYMENT,
                            catalog_identity=bind_test_catalog_writer(
                                two_principal_dsn, DEPLOYMENT
                            ),
                        ).cycle()
                        assert published["status"] == "published", published
                        assert str(published["generation_id"]) != str(old)
                        refreshed = _payload(
                            await a.call_tool(
                                "inspect",
                                {"target": {"kind": "session", "action": "refresh"}},
                            )
                        )
                        assert str(refreshed["pinned_generation_id"]) == str(
                            published["generation_id"]
                        )
                        later_a, later_b = await asyncio.gather(state(a), state(b))
                        assert (
                            later_a["logical_session_id"]
                            == first_a["logical_session_id"]
                        )
                        assert (
                            later_b["logical_session_id"]
                            == first_b["logical_session_id"]
                        )
                        assert later_b["generation_id"] == old
                        assert later_a["generation_id"] != old
        decisions = _decisions_since(two_principal_dsn, watermark)
        allowed = [
            row
            for row in decisions
            if row["effect"] == "allow" and row["action"] == "mcp.tool.call"
        ]
        assert {row["subject"] for row in allowed} == {SUBJECT_A, SUBJECT_B}

    asyncio.run(scenario())
