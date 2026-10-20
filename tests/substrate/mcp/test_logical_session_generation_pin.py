"""Issue #1220: logical MCP calls must not drift across generations."""

from __future__ import annotations

from tests.substrate.mcp._result_payload import (
    payload as _result_payload,
)

import gc
import json
import threading
from typing import Callable
import weakref

import anyio
import psycopg
import pytest
from mcp.shared.memory import create_connected_server_and_client_session
from starlette.testclient import TestClient

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp.logical_session import LogicalSessionRegistry
from okg.substrate.mcp.response_bounds import (
    ByteBoundedFastMCP,
    bound_read_tool,
    public_call_tool_result_bytes,
)
from okg.substrate.mcp.server import build_server
import okg.substrate.mcp.tracing as tracing_module
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402
from tests.substrate.mcp._result_payload import (
    call_json,
    json_client_sessions,
)



@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch):
    """Program-side test: every call here reads the JSON result format
    (issue #3059), through request `_meta` or the in-process scope."""
    json_client_sessions(monkeypatch)


_ACCEPT = "application/json, text/event-stream"
_BASE_URL = "http://127.0.0.1:8123"
_SUBTYPE = "session_marker"
_ROOT = "marker:root"
_A = "marker:a"
_B = "marker:b"


@pytest.fixture
def mcp_role_login(clean_runtime: str, okg_mcp_can_log_in: str) -> None:
    """``okg_mcp`` can log in.

    Set once per session under the cluster role lock and never reverted
    per test; see ``okg_mcp_login_enabled`` in tests/substrate/conftest.py.
    """


def _node(node_id: str, text: str, *, wide: bool = False) -> NodeFact:
    attrs = {"text": text, "phase": "A" if node_id != _B else "B"}
    if wide:
        attrs["blob"] = "x" * 1_500
    return NodeFact(
        node_id,
        _SUBTYPE,
        attrs,
        {"id": node_id},
        {"v": 1},
    )


def _publish_a(dsn: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=[_SUBTYPE],
            narrowings=[(_SUBTYPE, "relates", _SUBTYPE)],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                _node(_ROOT, "alpha-session-marker root"),
                _node(_A, "alpha-session-marker primary"),
                *[
                    _node(
                        f"marker:wide:{index}",
                        f"alpha-session-marker wide {index}",
                        wide=True,
                    )
                    for index in range(4)
                ],
                EdgeFact(
                    _ROOT,
                    _A,
                    "relates",
                    source_record_id={"id": "edge:a"},
                    source_revision={"v": 1},
                ),
            ],
            source="issue-1220",
            strict_endpoints=True,
        )
    result = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert result["status"] == "published", result
    return str(result["generation_id"])


def _publish_b(dsn: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                _node(_B, "beta-session-marker newly-published"),
                EdgeFact(
                    _ROOT,
                    _B,
                    "relates",
                    source_record_id={"id": "edge:b"},
                    source_revision={"v": 1},
                ),
            ],
            source="issue-1220",
            strict_endpoints=True,
        )
    result = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert result["status"] == "published", result
    return str(result["generation_id"])


def _assert_published(dsn: str, *generation_ids: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            """
            SELECT id, status
              FROM okg.v_generations
             WHERE id = ANY(%s)
            """,
            (list(generation_ids),),
        ).fetchall()
    assert {str(generation): status for generation, status in rows} == {
        generation_id: "published" for generation_id in generation_ids
    }


def _payload(result) -> dict:
    return _result_payload(result)


def _assert_transport_pin(
    payload: dict,
    *,
    generation_id: str,
    logical_session_id: str,
) -> None:
    assert payload["pinned_generation_id"] == generation_id
    assert payload["logical_session_id"] == logical_session_id
    assert payload["pin_scope"] == "transport_session"
    # The compact MCP wire leaves `session_default_generation_id` out when it
    # equals `pinned_generation_id` (issue #2674), and says so by that rule.
    assert payload.get(
        "session_default_generation_id", payload["pinned_generation_id"],
    ) == generation_id


def _terminate_idle_mcp_backends(dsn: str) -> set[int]:
    """Terminate only this disposable DB's okg_mcp backends."""

    with psycopg.connect(dsn, autocommit=True) as admin:
        pids = {
            int(row[0])
            for row in admin.execute(
                """
                SELECT pid
                  FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND usename = 'okg_mcp'
                   AND pid <> pg_backend_pid()
                """
            ).fetchall()
        }
        for pid in pids:
            assert admin.execute(
                "SELECT pg_terminate_backend(%s)", (pid,)
            ).fetchone() == (True,)
    return pids


@pytest.mark.postgres
def test_real_public_session_pin_survives_publish_override_refresh_and_recovery(
    clean_runtime: str,
    mcp_role_login,
    monkeypatch: pytest.MonkeyPatch,
    record_property: Callable[[str, object], None],
) -> None:
    """One real client analysis never silently mixes published snapshots."""

    del mcp_role_login
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.setattr(
        "okg.substrate.mcp.server.LATEST_PIN_CACHE_TTL_S",
        60.0,
    )
    generation_a = _publish_a(clean_runtime)
    record_property("published_generation_a", generation_a)
    server = build_server(
        dsn=clean_runtime,
        deployment_name="default",
        min_conns=1,
        max_conns=2,
        auto_advance="never",
    )

    async def drive() -> None:
        try:
            async with create_connected_server_and_client_session(server) as client_a:
                listed = {
                    tool.name: tool for tool in (await client_a.list_tools()).tools
                }
                for tool_name in (
                    "inspect",
                    "search",
                    "expand",
                    "filter",
                    "map",
                    "aggregate",
                    "query",
                ):
                    assert "ctx" not in listed[tool_name].inputSchema["properties"]

                first = _payload(
                    await client_a.call_tool("inspect", {"target": {"kind": "graph"}})
                )
                logical_a = first["logical_session_id"]
                assert isinstance(logical_a, str) and logical_a
                _assert_transport_pin(
                    first,
                    generation_id=generation_a,
                    logical_session_id=logical_a,
                )
                assert first["node_count"] == 6

                generation_b = _publish_b(clean_runtime)
                _assert_published(clean_runtime, generation_a, generation_b)
                record_property("published_generation_b", generation_b)

                calls = {
                    "inspect": await client_a.call_tool(
                        "inspect", {"target": {"kind": "graph"}}
                    ),
                    "search": await client_a.call_tool(
                        "search",
                        {"query": "alpha-session-marker", "limit": 20},
                    ),
                    "expand": await client_a.call_tool(
                        "expand",
                        {
                            "input": {"node_id": _ROOT},
                            "edge_type": "relates",
                            "select": ["dst"],
                            "limit": 20,
                        },
                    ),
                    "filter": await client_a.call_tool(
                        "filter",
                        {
                            "input": {"kind": "nodes", "subtype": _SUBTYPE},
                            "predicate": {},
                            "select": ["node_id"],
                            "limit": 20,
                        },
                    ),
                    "map": await client_a.call_tool(
                        "map",
                        {
                            "input": {"kind": "nodes", "subtype": _SUBTYPE},
                            "projection": ["node_id"],
                            "limit": 20,
                        },
                    ),
                    "aggregate": await client_a.call_tool(
                        "aggregate",
                        {
                            "input": {"kind": "nodes", "subtype": _SUBTYPE},
                            "by": ["subtype"],
                            "metrics": ["count"],
                        },
                    ),
                    "query": await client_a.call_tool(
                        "query",
                        {
                            "sql": (
                                "SELECT node_id FROM okg.v_nodes "
                                "WHERE subtype = 'session_marker' ORDER BY node_id"
                            )
                        },
                    ),
                }
                payloads = {name: _payload(result) for name, result in calls.items()}
                for operator_name, payload in payloads.items():
                    assert "pinned_generation_id" in payload, (
                        operator_name,
                        payload,
                    )
                    _assert_transport_pin(
                        payload,
                        generation_id=generation_a,
                        logical_session_id=logical_a,
                    )

                assert payloads["inspect"]["node_count"] == 6
                assert _B not in {
                    row["node_id"] for row in payloads["search"]["results"]
                }
                assert _A in {row["node_id"] for row in payloads["search"]["results"]}
                assert [row["dst"] for row in payloads["expand"]["neighbors"]] == [_A]
                expected_ids = sorted(
                    [_ROOT, _A, *[f"marker:wide:{index}" for index in range(4)]]
                )
                assert (
                    sorted(row["node_id"] for row in payloads["filter"]["items"])
                    == expected_ids
                )
                assert (
                    sorted(row["node_id"] for row in payloads["map"]["items"])
                    == expected_ids
                )
                assert payloads["aggregate"]["groups"] == [
                    {"subtype": _SUBTYPE, "count": 6}
                ]
                assert sorted(row[0] for row in payloads["query"]["rows"]) == (
                    expected_ids
                )

                # A native byte-bound retry remains the original omitted call:
                # it keeps the transport-session scope and default rather than
                # leaking its internal explicit rerun into the public envelope.
                bounded = _payload(
                    await client_a.call_tool(
                        "filter",
                        {
                            "input": {"kind": "nodes", "subtype": _SUBTYPE},
                            "predicate": {},
                            "select": ["node_id", "attrs"],
                            "limit": 6,
                            "max_bytes": 4_096,
                        },
                    )
                )
                _assert_transport_pin(
                    bounded,
                    generation_id=generation_a,
                    logical_session_id=logical_a,
                )
                assert bounded["truncation_reason"] == "response_byte_budget"
                assert bounded["effective_limit"] < 6
                assert public_call_tool_result_bytes(bounded) <= 4_096

                invalid_budget = _payload(
                    await client_a.call_tool(
                        "filter",
                        {
                            "input": {"kind": "nodes"},
                            "predicate": {},
                            "max_bytes": "not-an-int",
                        },
                    )
                )
                # `max_bytes` is typed `Optional[int]` since issue #2156, so a
                # non-integer is refused by the declared input model, naming
                # the field, before the budget check runs.
                assert invalid_budget["error"]["code"] == (
                    "invalid_operator_arguments"
                )
                assert "max_bytes" in invalid_budget["error"]["data"][
                    "invalid_fields"
                ]
                assert invalid_budget["logical_session_id"] == logical_a
                assert invalid_budget["pin_scope"] == "transport_session"
                assert invalid_budget["session_default_generation_id"] == generation_a
                assert "pinned_generation_id" not in invalid_budget

                # A new SDK ServerSession ignores the stale 60-second latest
                # cache, gets B, and never shares A's logical id/default.
                async with create_connected_server_and_client_session(
                    server
                ) as client_b:
                    fresh = _payload(
                        await client_b.call_tool(
                            "query",
                            {
                                "sql": (
                                    "SELECT node_id FROM okg.v_nodes "
                                    "WHERE subtype = 'session_marker' ORDER BY node_id"
                                )
                            },
                        )
                    )
                    logical_b = fresh["logical_session_id"]
                    assert logical_b != logical_a
                    _assert_transport_pin(
                        fresh,
                        generation_id=generation_b,
                        logical_session_id=logical_b,
                    )
                    assert _B in {row[0] for row in fresh["rows"]}

                still_a = _payload(
                    await client_a.call_tool("inspect", {"target": {"kind": "graph"}})
                )
                _assert_transport_pin(
                    still_a,
                    generation_id=generation_a,
                    logical_session_id=logical_a,
                )

                # Replacing every idle pooled backend cannot change the logical
                # transport pin held above the pool.
                assert _terminate_idle_mcp_backends(clean_runtime)
                after_replacement = _payload(
                    await client_a.call_tool("inspect", {"target": {"kind": "graph"}})
                )
                _assert_transport_pin(
                    after_replacement,
                    generation_id=generation_a,
                    logical_session_id=logical_a,
                )

                explicit_b = _payload(
                    await client_a.call_tool(
                        "query",
                        {
                            "sql": "SELECT node_id FROM okg.v_nodes ORDER BY node_id",
                            "generation_id": generation_b,
                        },
                    )
                )
                assert explicit_b["pinned_generation_id"] == generation_b
                assert explicit_b["logical_session_id"] == logical_a
                assert explicit_b["pin_scope"] == "explicit_call"
                assert explicit_b["session_default_generation_id"] == generation_a
                assert _B in {row[0] for row in explicit_b["rows"]}

                after_explicit = _payload(
                    await client_a.call_tool("inspect", {"target": {"kind": "graph"}})
                )
                _assert_transport_pin(
                    after_explicit,
                    generation_id=generation_a,
                    logical_session_id=logical_a,
                )

                import okg.substrate.mcp.session as session_module

                original_resolve = session_module.resolve_generation

                def fail_refresh(*args, **kwargs):
                    raise psycopg.OperationalError("synthetic latest lookup failure")

                monkeypatch.setattr(session_module, "resolve_generation", fail_refresh)
                failed_refresh = _payload(
                    await client_a.call_tool(
                        "inspect",
                        {"target": {"kind": "session", "action": "refresh"}},
                    )
                )
                assert failed_refresh["error"]["code"] == "operator_execution_failed"
                assert failed_refresh["session_default_generation_id"] == generation_a
                assert failed_refresh["logical_session_id"] == logical_a
                monkeypatch.setattr(
                    session_module,
                    "resolve_generation",
                    original_resolve,
                )

                after_failed_refresh = _payload(
                    await client_a.call_tool("inspect", {"target": {"kind": "graph"}})
                )
                _assert_transport_pin(
                    after_failed_refresh,
                    generation_id=generation_a,
                    logical_session_id=logical_a,
                )

                refresh_resolutions = 0

                def count_refresh(*args, **kwargs):
                    nonlocal refresh_resolutions
                    refresh_resolutions += 1
                    return original_resolve(*args, **kwargs)

                monkeypatch.setattr(session_module, "resolve_generation", count_refresh)
                refreshed = _payload(
                    await client_a.call_tool(
                        "inspect",
                        {
                            "target": {"kind": "session", "action": "refresh"},
                            "max_bytes": 4_096,
                        },
                    )
                )
                monkeypatch.setattr(
                    session_module,
                    "resolve_generation",
                    original_resolve,
                )
                assert refresh_resolutions == 1
                assert refreshed["old_generation_id"] == generation_a
                assert refreshed["new_generation_id"] == generation_b
                _assert_transport_pin(
                    refreshed,
                    generation_id=generation_b,
                    logical_session_id=logical_a,
                )

                after_refresh = _payload(
                    await client_a.call_tool("inspect", {"target": {"kind": "graph"}})
                )
                _assert_transport_pin(
                    after_refresh,
                    generation_id=generation_b,
                    logical_session_id=logical_a,
                )
                assert after_refresh["node_count"] == 7

                conflict = _payload(
                    await client_a.call_tool(
                        "inspect",
                        {
                            "target": {"kind": "session", "action": "refresh"},
                            "generation_id": generation_a,
                        },
                    )
                )
                assert conflict["error"]["code"] == (
                    "mcp_session_refresh_generation_conflict"
                )
                assert conflict["pin_scope"] == "explicit_call"
                assert conflict["session_default_generation_id"] == generation_b

                # A failed first bind leaves no pin; the public refusal creates
                # the next canonical empty state, and retry binds exactly once.
                fail_first = True

                def fail_first_resolve(*args, **kwargs):
                    nonlocal fail_first
                    if fail_first:
                        fail_first = False
                        raise psycopg.OperationalError("synthetic first-bind failure")
                    return original_resolve(*args, **kwargs)

                monkeypatch.setattr(
                    session_module,
                    "resolve_generation",
                    fail_first_resolve,
                )
                async with create_connected_server_and_client_session(
                    server
                ) as client_c:
                    empty_state = _payload(
                        await client_c.call_tool(
                            "filter",
                            {
                                "input": {"kind": "nodes"},
                                "predicate": {},
                                "max_bytes": "not-an-int",
                            },
                        )
                    )
                    assert empty_state["session_default_generation_id"] is None
                    empty_logical_id = empty_state["logical_session_id"]
                    failed_first = _payload(
                        await client_c.call_tool(
                            "inspect", {"target": {"kind": "graph"}}
                        )
                    )
                    assert failed_first["error"]["code"] == (
                        "operator_execution_failed"
                    )
                    assert failed_first["session_default_generation_id"] is None
                    logical_c = failed_first["logical_session_id"]
                    assert logical_c != empty_logical_id
                    retried = _payload(
                        await client_c.call_tool(
                            "inspect", {"target": {"kind": "graph"}}
                        )
                    )
                    _assert_transport_pin(
                        retried,
                        generation_id=generation_b,
                        logical_session_id=logical_c,
                    )
                monkeypatch.setattr(
                    session_module,
                    "resolve_generation",
                    original_resolve,
                )

            # Direct calls have no transport identity. Omitted calls warn and
            # refresh refuses; a non-null explicit pin remains usable.
            direct_inspect = server._tool_manager._tools["inspect"].fn
            request_scoped = direct_inspect(target={"kind": "graph"})
            assert request_scoped["logical_session_id"] is None
            assert request_scoped["pin_scope"] == "request"
            assert request_scoped["warning"]["code"] == (
                "mcp_session_identity_unavailable"
            )
            explicit_direct = direct_inspect(
                target={"kind": "graph"},
                generation_id=generation_a,
            )
            assert explicit_direct["pin_scope"] == "explicit_call"
            assert "warning" not in explicit_direct
            refused_refresh = direct_inspect(
                target={"kind": "session", "action": "refresh"}
            )
            assert refused_refresh["error"]["code"] == (
                "mcp_session_refresh_identity_unavailable"
            )
            assert refused_refresh["pin_scope"] == "request"
        finally:
            server._okg_pool.close()

    anyio.run(drive)


def _rpc(method: str, params: dict | None = None, rid: int | None = 1) -> dict:
    payload: dict = {"jsonrpc": "2.0", "method": method}
    if rid is not None:
        payload["id"] = rid
    if params is not None:
        payload["params"] = params
    return payload


def _sse_payload(response) -> dict:
    for line in response.text.splitlines():
        if line.startswith("data: "):
            return json.loads(line[6:])
    return response.json()


def _handshake(client: TestClient) -> dict[str, str]:
    headers = {"Accept": _ACCEPT}
    initialized = client.post(
        "/mcp",
        json=_rpc(
            "initialize",
            {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "issue-1220", "version": "1"},
            },
        ),
        headers=headers,
    )
    assert initialized.status_code == 200, initialized.text
    session_id = initialized.headers.get("mcp-session-id")
    if session_id:
        headers["mcp-session-id"] = session_id
    acknowledged = client.post(
        "/mcp",
        json=_rpc("notifications/initialized", rid=None),
        headers=headers,
    )
    assert acknowledged.status_code == 202, acknowledged.text
    return headers


def _http_tool(
    client: TestClient,
    headers: dict[str, str],
    name: str,
    arguments: dict,
    *,
    rid: int,
) -> dict:
    rpc_payload = _sse_payload(
        client.post(
            "/mcp",
            json=_rpc(
                "tools/call",
                # A program-side read: the JSON result format (issue #3059).
                {"name": name, "arguments": arguments,
                 "_meta": {"okg/result_format": "json"}},
                rid=rid,
            ),
            headers=headers,
        )
    )
    # Since issue #2010 the payload rides in structuredContent and the
    # content list is empty; older servers put a JSON copy in the text block.
    result = rpc_payload["result"]
    structured = result.get("structuredContent")
    if structured is not None:
        return structured
    return json.loads(result["content"][0]["text"])


@pytest.mark.postgres
def test_stateful_and_stateless_streamable_http_identity_contract(
    clean_runtime: str,
    mcp_role_login,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del mcp_role_login
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    generation = _publish_a(clean_runtime)

    stateful = build_server(
        dsn=clean_runtime,
        min_conns=1,
        max_conns=2,
    )
    try:
        with TestClient(
            stateful.streamable_http_app(),
            base_url=_BASE_URL,
        ) as client:
            headers_a = _handshake(client)
            first = _http_tool(
                client,
                headers_a,
                "inspect",
                {"target": {"kind": "graph"}},
                rid=2,
            )
            generation_b = _publish_b(clean_runtime)
            second = _http_tool(
                client,
                headers_a,
                "inspect",
                {"target": {"kind": "graph"}},
                rid=3,
            )
            assert first["logical_session_id"] == second["logical_session_id"]
            assert first["pinned_generation_id"] == generation
            assert second["pinned_generation_id"] == generation
            assert first["pin_scope"] == "transport_session"
            assert first["logical_session_id"] != headers_a.get("mcp-session-id")

            headers_b = _handshake(client)
            fresh = _http_tool(
                client,
                headers_b,
                "inspect",
                {"target": {"kind": "graph"}},
                rid=4,
            )
            assert fresh["logical_session_id"] != first["logical_session_id"]
            assert fresh["pinned_generation_id"] == generation_b

            unknown = client.post(
                "/mcp",
                json=_rpc(
                    "tools/call",
                    {
                        "name": "inspect",
                        "arguments": {"target": {"kind": "graph"}},
                    },
                    rid=5,
                ),
                headers={
                    "Accept": _ACCEPT,
                    "mcp-session-id": "expired-or-restarted-session",
                },
            )
            assert unknown.status_code == 404
            assert unknown.json()["error"]["message"] == "Session not found"
    finally:
        stateful._okg_pool.close()

    stateless = build_server(
        dsn=clean_runtime,
        min_conns=1,
        max_conns=2,
    )
    stateless.settings.stateless_http = True
    try:
        with TestClient(
            stateless.streamable_http_app(),
            base_url=_BASE_URL,
        ) as client:
            headers = _handshake(client)
            omitted = _http_tool(
                client,
                headers,
                "inspect",
                {"target": {"kind": "graph"}},
                rid=6,
            )
            assert omitted["logical_session_id"] is None
            assert omitted["pin_scope"] == "request"
            assert omitted["warning"]["code"] == ("mcp_session_identity_unavailable")

            explicit = _http_tool(
                client,
                headers,
                "inspect",
                {
                    "target": {"kind": "graph"},
                    "generation_id": generation,
                },
                rid=7,
            )
            assert explicit["pin_scope"] == "explicit_call"
            assert "warning" not in explicit

            refresh = _http_tool(
                client,
                headers,
                "inspect",
                {"target": {"kind": "session", "action": "refresh"}},
                rid=8,
            )
            assert refresh["error"]["code"] == (
                "mcp_session_refresh_identity_unavailable"
            )
    finally:
        stateless._okg_pool.close()


@pytest.mark.postgres
def test_concurrent_public_first_calls_converge_on_one_binding(
    clean_runtime: str,
    mcp_role_login,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del mcp_role_login
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    generation = _publish_a(clean_runtime)
    server = build_server(
        dsn=clean_runtime,
        min_conns=1,
        max_conns=2,
    )

    import okg.substrate.mcp.session as session_module

    original_resolve = session_module.resolve_generation
    latest_resolutions = 0
    resolution_lock = threading.Lock()

    def count_latest(*args, **kwargs):
        nonlocal latest_resolutions
        if kwargs.get("requested") is None:
            with resolution_lock:
                latest_resolutions += 1
        return original_resolve(*args, **kwargs)

    monkeypatch.setattr(session_module, "resolve_generation", count_latest)

    async def drive() -> None:
        try:
            async with create_connected_server_and_client_session(server) as client:
                start = anyio.Event()
                results: dict[str, dict] = {}

                async def call(name: str) -> None:
                    await start.wait()
                    results[name] = _payload(
                        await client.call_tool(
                            "query",
                            {
                                "sql": (
                                    "SELECT node_id FROM okg.v_nodes "
                                    "WHERE subtype = 'session_marker' "
                                    "ORDER BY node_id"
                                )
                            },
                        )
                    )

                async with anyio.create_task_group() as task_group:
                    task_group.start_soon(call, "left")
                    task_group.start_soon(call, "right")
                    start.set()

                left, right = results["left"], results["right"]
                assert left["pinned_generation_id"] == generation
                assert right["pinned_generation_id"] == generation
                assert left["logical_session_id"] == right["logical_session_id"]
                assert left["rows"] == right["rows"]
                assert _A in {row[0] for row in left["rows"]}
                assert latest_resolutions == 1
        finally:
            server._okg_pool.close()

    anyio.run(drive)


def test_registry_invalidated_waiter_restarts_and_converges() -> None:
    """A-fails/B-waits/C-arrives cannot bind through an orphaned value."""

    class Identity:
        pass

    identity = Identity()
    registry = LogicalSessionRegistry()
    b_looked_up = threading.Event()
    c_finished = threading.Event()
    results: dict[str, tuple[str, object | None]] = {}
    original_lookup = registry._lookup_or_create

    def observed_lookup(key):
        state, created = original_lookup(key)
        if threading.current_thread().name == "waiter-b":
            b_looked_up.set()
        return state, created

    registry._lookup_or_create = observed_lookup  # type: ignore[method-assign]

    def bind(name: str, *, finished: threading.Event | None = None) -> None:
        with registry.locked(identity) as lease:
            if lease.state.default_generation_id is None:
                lease.state.default_generation_id = "generation:new"
            results[name] = (
                lease.state.logical_session_id,
                lease.state.default_generation_id,
            )
        if finished is not None:
            finished.set()

    with registry.locked(identity) as failed:
        waiter_b = threading.Thread(
            target=bind,
            args=("b",),
            name="waiter-b",
        )
        waiter_b.start()
        assert b_looked_up.wait(timeout=5), "waiter B never looked up old value"
        registry.invalidate_locked(identity, failed.state)

        caller_c = threading.Thread(
            target=bind,
            args=("c",),
            kwargs={"finished": c_finished},
            name="caller-c",
        )
        caller_c.start()
        assert c_finished.wait(timeout=5), "caller C never installed replacement"

    waiter_b.join(timeout=5)
    caller_c.join(timeout=5)
    assert not waiter_b.is_alive()
    assert not caller_c.is_alive()
    assert results["b"] == results["c"]
    assert results["b"][1] == "generation:new"


def test_registry_rejects_unusable_identity_and_releases_weak_entry() -> None:
    registry = LogicalSessionRegistry()
    assert registry.supports([]) is False

    class Identity:
        pass

    identity = Identity()
    identity_ref = weakref.ref(identity)
    snapshot = registry.snapshot(identity)
    assert snapshot.logical_session_id
    assert len(registry) == 1

    del identity
    gc.collect()
    assert identity_ref() is None
    assert len(registry) == 0


def test_tracing_wrapper_keeps_injected_context_out_of_public_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "1")
    server = build_server(
        dsn="postgres://postgres:okg@127.0.0.1:1/okg_missing",
        min_conns=0,
        max_conns=1,
    )

    async def drive() -> None:
        try:
            async with create_connected_server_and_client_session(server) as client:
                listed = {tool.name: tool for tool in (await client.list_tools()).tools}
                for tool_name in (
                    "inspect",
                    "search",
                    "expand",
                    "filter",
                    "map",
                    "aggregate",
                    "query",
                ):
                    assert "ctx" not in listed[tool_name].inputSchema["properties"]
        finally:
            server._okg_pool.close()

    anyio.run(drive)


def test_byte_bound_measures_the_logical_session_envelope() -> None:
    """Metadata added at the public boundary participates in native retries."""

    def raw_filter(
        *,
        limit: int = 2,
        offset: int = 0,
        generation_id: str | None = None,
        max_bytes: int = 4_096,
    ) -> dict:
        del generation_id, max_bytes
        return {
            "items": [{"blob": "x" * 1_650} for _ in range(limit)],
            "limit": limit,
            "offset": offset,
            "next_offset": offset + limit,
            "pinned_generation_id": "generation:a",
        }

    def logical_envelope(payload: dict, _arguments: dict) -> dict:
        # The text form no longer prints `logical_session_id` (PACT
        # mcp-result-text-diet-3076), so the boundary metadata that must be
        # measured here is a session default that differs from the pin,
        # which both forms print.
        return {
            **payload,
            "logical_session_id": "logical:session",
            "pin_scope": "transport_session",
            "session_default_generation_id": "generation:" + "b" * 800,
        }

    wrapped = bound_read_tool(
        "filter",
        raw_filter,
        measure_response=logical_envelope,
    )
    result = wrapped(limit=2, max_bytes=4_096)
    public_result = logical_envelope(result, {})

    assert result["effective_limit"] == 1
    assert result["truncation_reason"] == "response_byte_budget"
    assert public_call_tool_result_bytes(public_result) <= 4_096


def test_typed_error_is_retrimmed_after_logical_metadata() -> None:
    server = ByteBoundedFastMCP("typed-error-bound")

    @server.tool(name="inspect", structured_output=False)
    def failing_inspect(max_bytes: int = 4_096) -> dict:
        del max_bytes
        raise SubstrateError(
            "",
            fix_hint="h" * 704,
            data={f"safe_{index}": "d" * 700 for index in range(4)},
        )

    server._okg_logical_session_stamper = (  # type: ignore[attr-defined]
        lambda payload, **_kwargs: {
            **payload,
            "logical_session_id": "logical-session-00000000000000000001",
            "pin_scope": "transport_session",
            "session_default_generation_id": "generation:published:a",
        }
    )

    async def drive():
        return await call_json(server, "inspect", {"max_bytes": 4_096})

    result = anyio.run(drive)
    payload = _result_payload(result)
    encoded = result.model_dump_json(by_alias=True, exclude_none=True).encode()

    assert len(encoded) <= 4_096
    assert payload["error"]["code"] == "substrate_error"
    assert payload["logical_session_id"].startswith("logical-session-")
    assert payload["pin_scope"] == "transport_session"


def test_tracing_redacts_a_positionally_supplied_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "1")
    traced_requests: list[dict] = []
    logged_args: list[dict] = []
    monkeypatch.setattr(
        tracing_module,
        "trace_tool_call",
        lambda *_args, **kwargs: traced_requests.append(kwargs["request"]),
    )
    monkeypatch.setattr(
        tracing_module,
        "log_call_to_log",
        lambda *_args, **kwargs: logged_args.append(kwargs["args"]),
    )

    def raw_search(query: str, ctx: object | None = None) -> dict:
        del ctx
        return {"query": query, "results": []}

    wrapped = tracing_module.make_tracer(
        object(),  # type: ignore[arg-type]
        deployment_name="default",
    )("search")(raw_search)
    result = wrapped("needle", object())

    assert result["query"] == "needle"
    assert traced_requests == [{"args": ("needle",), "kwargs": {}}]
    assert logged_args == [{}]
