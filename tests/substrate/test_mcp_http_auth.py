"""Bearer authentication for the streamable-HTTP MCP transport (PACT mcp-http-auth).

Every 401 arm here drives the ASGI app produced by the SAME build path
``run_server`` hands to ``mcp.run()``: ``FastMCP.run`` is patched to capture the
constructed instance, and the app is then taken off that instance. A credential
that resolved but was never wired therefore FAILS these tests instead of passing
them, which is the point -- "the flag was accepted" is not evidence that the
endpoint authenticates.

These tests are hermetic. ``OKG_MCP_SURFACE_PROBE=1`` makes ``build_server``
construct its connection pool unopened, so the whole registered surface exists
without a database. That is sound for auth: a 401 is decided in
``RequireAuthMiddleware`` before the request reaches the session manager, let
alone the graph. The one arm that must see real rows
(``test_valid_bearer_token_search_returns_rows_with_generation_id``) is marked
``postgres`` and needs the shared runtime fixture.
"""
from __future__ import annotations

import contextlib
import json
import logging

import pytest

pytest.importorskip("mcp")
pytest.importorskip("httpx")

from mcp.server.fastmcp import FastMCP  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

import okg.substrate.mcp.server as server_mod  # noqa: E402
from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.substrate.mcp import http_auth  # noqa: E402


postgres = pytest.mark.postgres
token_supply = pytest.mark.token_supply

#: Unreachable by construction (port 1). Under OKG_MCP_SURFACE_PROBE the pool is
#: never opened, so nothing dials it; naming a dead endpoint makes an accidental
#: connection attempt fail loudly rather than reach a real graph.
_DEAD_DSN = "postgresql://postgres:postgres@127.0.0.1:1/nodb"

#: The Host the served app accepts. FastMCP's DNS-rebinding protection rejects
#: TestClient's default "testserver" Host with 421 before the app sees it.
_BASE_URL = "http://127.0.0.1:8123"

_ACCEPT = "application/json, text/event-stream"

_TOKEN_ENV = "OKG_MCP_AUTH_TOKEN"
_TOKEN = "0f3c8ab21d7e4f6a9b5c0d1e2f3a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c"
_WRONG_TOKEN = "9a8b7c6d5e4f30291827364554637281900f1e2d3c4b5a69788796a5b4c3d2e1"

#: The two unauthenticated arms the requirement names. They are exercised
#: together against every method so "wrong token" can never diverge from
#: "no header" on some method nobody checked.
_UNAUTH_ARMS = {
    "missing-header": {"Accept": _ACCEPT},
    "wrong-token": {"Accept": _ACCEPT, "Authorization": f"Bearer {_WRONG_TOKEN}"},
}

#: Every read operator build_server registers. The refusal must cover ALL of
#: them, so "the read surface was open anyway" is never a path through.
_READ_OPERATORS = (
    "aggregate",
    "expand",
    "filter",
    "inspect",
    "map",
    "query",
    "search",
)


def _rpc(method: str, params: dict | None = None, rid: int | None = 1) -> dict:
    body: dict = {"jsonrpc": "2.0", "method": method}
    if rid is not None:
        body["id"] = rid
    if params is not None:
        body["params"] = params
    return body


def _sse_payload(response) -> dict:
    """Decode one JSON-RPC payload from a streamable-HTTP response.

    The transport answers with text/event-stream by default; keep the served
    configuration exactly as run_server leaves it rather than flipping
    json_response just to make the test easier to read.
    """
    for line in response.text.splitlines():
        if line.startswith("data: "):
            return json.loads(line[6:])
    return response.json()


@contextlib.contextmanager
def _served(
    monkeypatch,
    *,
    auth_token_env: str | None = _TOKEN_ENV,
    token_value: str | None = _TOKEN,
    allow_writes: bool = False,
    host: str = "127.0.0.1",
):
    """Run run_server up to the point it would block, and yield its app.

    Yields ``(mcp, app)`` where ``app`` is what ``mcp.run()`` would have served.
    """
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    monkeypatch.delenv(_TOKEN_ENV, raising=False)
    if auth_token_env is not None and token_value is not None:
        monkeypatch.setenv(auth_token_env, token_value)

    captured: dict = {}

    def _capture_run(self, *_a, **_kw):  # noqa: ANN001
        captured["mcp"] = self

    monkeypatch.setattr(FastMCP, "run", _capture_run, raising=True)
    server_mod.run_server(
        transport="streamable-http",
        host=host,
        port=8123,
        dsn=_DEAD_DSN,
        deployment_name="default",
        allow_writes=allow_writes,
        auth_token_env=auth_token_env,
    )
    mcp = captured["mcp"]
    assert mcp is not None, "run_server never reached mcp.run()"
    try:
        yield mcp, mcp.streamable_http_app()
    finally:
        with contextlib.suppress(Exception):
            mcp._okg_pool.close()
        if getattr(mcp, "_okg_write_pool", None) is not None:
            with contextlib.suppress(Exception):
                mcp._okg_write_pool.close()


def _handshake(client: TestClient, headers: dict) -> dict:
    """Complete the MCP initialize handshake, returning session-bearing headers."""
    init = client.post(
        "/mcp",
        json=_rpc(
            "initialize",
            {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "pytest", "version": "0"},
            },
        ),
        headers=headers,
    )
    assert init.status_code == 200, init.text
    session_headers = dict(headers)
    session_id = init.headers.get("mcp-session-id")
    if session_id:
        session_headers["mcp-session-id"] = session_id
    ack = client.post(
        "/mcp",
        json=_rpc("notifications/initialized", rid=None),
        headers=session_headers,
    )
    assert ack.status_code == 202, ack.text
    return session_headers


def _assert_is_bare_401(response) -> None:
    """A refusal carries a challenge and NOTHING else.

    A 200 with an empty result set is not a refusal, and neither is a 401 whose
    body happens to carry a tool listing or graph rows. Assert on both halves so
    an empty-but-successful response can never read as "authentication worked".
    """
    assert response.status_code == 401, response.text
    assert response.headers.get("www-authenticate", "").startswith("Bearer ")
    body = response.text
    payload = json.loads(body)
    # No JSON-RPC envelope, so no tool result of any kind can be present.
    assert "result" not in payload
    assert "jsonrpc" not in payload
    assert set(payload) == {"error", "error_description"}
    assert payload["error"] == "invalid_token"
    for leak in ("tools", "resources", "node_id", "generation_id", "rows"):
        assert leak not in body, f"refusal body leaked {leak!r}: {body!r}"


# ── unauthenticated calls are refused across the whole surface ────────


def test_unauthenticated_tools_call_returns_401_and_no_tool_result(monkeypatch):
    with _served(monkeypatch) as (_mcp, app):
        with TestClient(app, base_url=_BASE_URL) as client:
            response = client.post(
                "/mcp",
                json=_rpc(
                    "tools/call",
                    {"name": "search", "arguments": {"query": "pact"}},
                ),
                headers={"Accept": _ACCEPT},
            )
    _assert_is_bare_401(response)


def test_unauthenticated_tools_list_returns_401(monkeypatch):
    with _served(monkeypatch) as (_mcp, app):
        with TestClient(app, base_url=_BASE_URL) as client:
            response = client.post(
                "/mcp", json=_rpc("tools/list"), headers={"Accept": _ACCEPT}
            )
    _assert_is_bare_401(response)


def test_unauthenticated_initialize_returns_401(monkeypatch):
    """Refusal covers the very first message, so no session is ever created."""
    with _served(monkeypatch) as (_mcp, app):
        with TestClient(app, base_url=_BASE_URL) as client:
            response = client.post(
                "/mcp",
                json=_rpc(
                    "initialize",
                    {
                        "protocolVersion": "2025-06-18",
                        "capabilities": {},
                        "clientInfo": {"name": "pytest", "version": "0"},
                    },
                ),
                headers={"Accept": _ACCEPT},
            )
    _assert_is_bare_401(response)
    assert response.headers.get("mcp-session-id") is None


def test_unauthenticated_resources_list_and_read_return_401(monkeypatch):
    """Resources ride the same route as tools; a per-method check would leak them.

    build_server registers the schema-digest and readside-metadata resources on
    /mcp. The test first proves those resources really exist on the
    authenticated server, so the 401 is hiding something real rather than
    passing because the URI is unknown.
    """
    with _served(monkeypatch) as (_mcp, app):
        with TestClient(app, base_url=_BASE_URL) as client:
            auth = {
                "Accept": _ACCEPT,
                "Authorization": f"Bearer {_TOKEN}",
            }
            session_headers = _handshake(client, auth)
            listed = _sse_payload(
                client.post(
                    "/mcp", json=_rpc("resources/list", rid=2),
                    headers=session_headers,
                )
            )
            uris = [r["uri"] for r in listed["result"]["resources"]]
            assert any("schema-digest" in u for u in uris), uris
            assert any("readside-metadata" in u for u in uris), uris

            unauth = {"Accept": _ACCEPT}
            refused_list = client.post(
                "/mcp", json=_rpc("resources/list", rid=3), headers=unauth
            )
            refused_read = client.post(
                "/mcp",
                json=_rpc("resources/read", {"uri": uris[0]}, rid=4),
                headers=unauth,
            )
    _assert_is_bare_401(refused_list)
    _assert_is_bare_401(refused_read)


def test_malformed_authorization_header_returns_401_not_400(monkeypatch):
    """A raised AuthenticationError would become HTTP 400 and break the contract.

    The verifier returns None for every rejection instead, so malformed shapes
    land on the same 401 path as a missing header.
    """
    malformed = [
        "Basic dXNlcjpwYXNz",
        "Bearer",
        "Bearer ",
        f"bearer{_TOKEN}",
        "Bearer a b c",
        "\x00\x01",
        f"Bearer {_TOKEN} extra",
    ]
    responses = []
    with _served(monkeypatch) as (_mcp, app):
        with TestClient(app, base_url=_BASE_URL) as client:
            for header in malformed:
                responses.append(
                    (
                        header,
                        client.post(
                            "/mcp",
                            json=_rpc("tools/list"),
                            headers={"Accept": _ACCEPT, "Authorization": header},
                        ),
                    )
                )
    for header, response in responses:
        assert response.status_code == 401, (header, response.status_code)
        _assert_is_bare_401(response)


def test_bad_bearer_token_returns_401_indistinguishable_from_missing(monkeypatch):
    """A wrong token and an absent one must not be told apart by the response."""
    with _served(monkeypatch) as (_mcp, app):
        with TestClient(app, base_url=_BASE_URL) as client:
            missing = client.post(
                "/mcp", json=_rpc("tools/list"), headers={"Accept": _ACCEPT}
            )
            wrong = client.post(
                "/mcp",
                json=_rpc("tools/list"),
                headers={
                    "Accept": _ACCEPT,
                    "Authorization": f"Bearer {_WRONG_TOKEN}",
                },
            )
    _assert_is_bare_401(missing)
    _assert_is_bare_401(wrong)
    assert wrong.status_code == missing.status_code
    assert wrong.content == missing.content
    for header in ("www-authenticate", "content-type", "content-length"):
        assert wrong.headers.get(header) == missing.headers.get(header), header


def _registered_resource_uris(client) -> list[str]:
    """Resource URIs, discovered through an AUTHENTICATED listing.

    Reading a URI that does not exist would return 401 for the wrong reason, so
    the resources/read refusal below is aimed at a resource the server really
    registers.
    """
    session = _handshake(
        client, {"Accept": _ACCEPT, "Authorization": f"Bearer {_TOKEN}"}
    )
    listed = _sse_payload(
        client.post("/mcp", json=_rpc("resources/list", rid=2), headers=session)
    )
    uris = [r["uri"] for r in listed["result"]["resources"]]
    assert uris, "no resources registered; the resources/read arm would be vacuous"
    return uris


def _surface_probes(uri: str) -> list[tuple[str, dict | None]]:
    """The five JSON-RPC methods that share the guarded /mcp route."""
    return [
        (
            "initialize",
            {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "pytest", "version": "0"},
            },
        ),
        ("tools/list", None),
        ("tools/call", {"name": "search", "arguments": {"query": "pact"}}),
        ("resources/list", None),
        ("resources/read", {"uri": uri}),
    ]


def test_unauthenticated_refusal_is_uniform_across_every_method_and_arm(monkeypatch):
    """Both arms x all five methods, not one arm sampled on one method.

    The per-method tests above drive the missing-header arm, and the
    indistinguishability test drives the wrong-token arm on `tools/list` alone.
    A per-method or per-arm divergence anywhere else in the surface would slip
    between them, so the whole 5x2 grid is asserted here in one place.
    """
    refusals: dict[tuple[str, str], object] = {}
    with _served(monkeypatch) as (_mcp, app):
        with TestClient(app, base_url=_BASE_URL) as client:
            probes = _surface_probes(_registered_resource_uris(client)[0])
            for method, params in probes:
                for arm, headers in _UNAUTH_ARMS.items():
                    refusals[(method, arm)] = client.post(
                        "/mcp", json=_rpc(method, params, rid=9), headers=headers
                    )

    methods = [method for method, _params in probes]
    assert len(refusals) == len(methods) * len(_UNAUTH_ARMS) == 10
    for (method, arm), response in refusals.items():
        _assert_is_bare_401(response)
        # Not even a session is handed out: a refused initialize must not
        # leave a session id the caller could reuse.
        assert response.headers.get("mcp-session-id") is None, (method, arm)

    # A wrong token is indistinguishable from an absent one on EVERY method,
    # not only on the one method the earlier test samples.
    for method in methods:
        missing = refusals[(method, "missing-header")]
        wrong = refusals[(method, "wrong-token")]
        assert wrong.status_code == missing.status_code, method
        assert wrong.content == missing.content, method
        for header in ("www-authenticate", "content-type", "content-length"):
            assert wrong.headers.get(header) == missing.headers.get(header), (
                method,
                header,
            )


def test_unauthenticated_tools_call_is_refused_for_every_read_operator(monkeypatch):
    """"Read operators are open anyway" is not a path through the refusal."""
    refusals: dict[tuple[str, str], object] = {}
    with _served(monkeypatch) as (_mcp, app):
        with TestClient(app, base_url=_BASE_URL) as client:
            session = _handshake(
                client, {"Accept": _ACCEPT, "Authorization": f"Bearer {_TOKEN}"}
            )
            payload = _sse_payload(
                client.post("/mcp", json=_rpc("tools/list", rid=2), headers=session)
            )
            registered = {t["name"] for t in payload["result"]["tools"]}
            for name in sorted(registered):
                for arm, headers in _UNAUTH_ARMS.items():
                    refusals[(name, arm)] = client.post(
                        "/mcp",
                        json=_rpc(
                            "tools/call", {"name": name, "arguments": {}}, rid=3
                        ),
                        headers=headers,
                    )

    # Non-vacuous: the operators being refused are really registered here.
    assert set(_READ_OPERATORS) <= registered, registered
    assert len(refusals) == len(registered) * len(_UNAUTH_ARMS)
    for (name, arm), response in refusals.items():
        assert response.status_code == 401, (name, arm, response.text)
        _assert_is_bare_401(response)


def test_unauthenticated_calls_execute_no_tool_and_read_no_resource(monkeypatch):
    """The refusal lands BEFORE dispatch, not on an empty result after it.

    A 200 carrying an empty result set is not a refusal, and neither is a 401
    produced after the operator already ran. Spies sit on the four FastMCP
    dispatch entry points; the positive control at the end fires the same spies
    on an authenticated call, so an empty `dispatched` list cannot be the
    silence of a spy that never worked.
    """
    dispatched: list[str] = []
    for entry_point in ("call_tool", "list_tools", "read_resource", "list_resources"):
        original = getattr(FastMCP, entry_point)

        def _spy(self, *args, _name=entry_point, _original=original, **kwargs):
            dispatched.append(_name)
            return _original(self, *args, **kwargs)

        monkeypatch.setattr(FastMCP, entry_point, _spy, raising=True)

    with _served(monkeypatch) as (_mcp, app):
        with TestClient(app, base_url=_BASE_URL) as client:
            probes = _surface_probes("okg://schema-digest")
            for method, params in probes:
                for headers in _UNAUTH_ARMS.values():
                    response = client.post(
                        "/mcp", json=_rpc(method, params, rid=4), headers=headers
                    )
                    _assert_is_bare_401(response)
            assert dispatched == [], dispatched

            # Positive control on the SAME spies.
            session = _handshake(
                client, {"Accept": _ACCEPT, "Authorization": f"Bearer {_TOKEN}"}
            )
            client.post("/mcp", json=_rpc("tools/list", rid=5), headers=session)
    assert "list_tools" in dispatched, dispatched


def test_served_app_wraps_mcp_route_in_auth_middleware(monkeypatch):
    """Auth-active means OBSERVABLE ON THE SERVED APP, not "a flag was passed".

    The negative control matters as much as the positive one: without
    --auth-token-env the same build path must leave the route unwrapped, so a
    green assertion here cannot come from a check that is true either way.
    """
    with _served(monkeypatch) as (mcp, app):
        assert http_auth.server_has_token_verifier(mcp) is True
        assert http_auth.mcp_route_is_auth_wrapped(app) is True
        # resource_server_url=None: no unauthenticated protected-resource
        # metadata route is published alongside the guarded one.
        paths = {getattr(route, "path", None) for route in app.routes}
        assert not any(
            path and "well-known" in path for path in paths
        ), paths

    with _served(monkeypatch, auth_token_env=None) as (mcp, app):
        assert http_auth.server_has_token_verifier(mcp) is False
        assert http_auth.mcp_route_is_auth_wrapped(app) is False


def test_guard_auth_cells_read_auth_active_off_the_real_served_app(monkeypatch):
    """Close the seam between the guard matrix and the real build path.

    tests/substrate/test_mcp_transport.py proves all eight cells against a fake
    whose `_token_verifier` MIRRORS what build_server is believed to install.
    That fake would keep passing if build_server stopped wiring the verifier --
    the matrix would be measuring its own assumption. Here the REAL build path
    runs and the guard's auth-active input is taken from the served ASGI app.
    """
    for allow_writes in (False, True):
        for host in ("127.0.0.1", "0.0.0.0"):
            cell = (allow_writes, host)
            with _served(
                monkeypatch, allow_writes=allow_writes, host=host
            ) as (mcp, app):
                assert http_auth.server_has_token_verifier(mcp) is True, cell
                auth_active = http_auth.mcp_route_is_auth_wrapped(app)
                assert auth_active is True, cell
                # All four authenticated cells serve: no refusal, and no
                # warning whose text would now assert a falsehood.
                assert (
                    server_mod._write_transport_guard(
                        allow_writes=allow_writes,
                        transport="streamable-http",
                        host=host,
                        auth_active=auth_active,
                    )
                    is None
                ), cell
                assert (
                    server_mod._readonly_transport_warning(
                        allow_writes=allow_writes,
                        transport="streamable-http",
                        host=host,
                        auth_active=auth_active,
                    )
                    is None
                ), cell

    # Negative control through the SAME build path: drop the flag and the
    # routable read-only cell goes back to warning. Without this, the four
    # assertions above would also hold for a guard that ignored its input.
    with _served(monkeypatch, auth_token_env=None, host="0.0.0.0") as (_mcp, app):
        auth_active = http_auth.mcp_route_is_auth_wrapped(app)
        assert auth_active is False
        assert (
            server_mod._readonly_transport_warning(
                allow_writes=False,
                transport="streamable-http",
                host="0.0.0.0",
                auth_active=auth_active,
            )
            is not None
        )


# ── a valid token reaches the full surface, unchanged ─────────────────


def test_valid_bearer_token_lists_full_operator_surface(monkeypatch):
    """Authentication decides whether a call is served, never what it may see."""
    with _served(monkeypatch) as (_mcp, app):
        with TestClient(app, base_url=_BASE_URL) as client:
            headers = _handshake(
                client,
                {"Accept": _ACCEPT, "Authorization": f"Bearer {_TOKEN}"},
            )
            payload = _sse_payload(
                client.post(
                    "/mcp", json=_rpc("tools/list", rid=2), headers=headers
                )
            )
    read_only_surface = {t["name"] for t in payload["result"]["tools"]}
    assert read_only_surface == {
        "aggregate",
        "expand",
        "filter",
        "inspect",
        "map",
        "query",
        "run",
        "search",
    }

    # Write posture: the same authenticated listing gains the write and
    # source-admin tools, so auth did not narrow the surface in either posture.
    with _served(monkeypatch, allow_writes=True) as (_mcp, app):
        with TestClient(app, base_url=_BASE_URL) as client:
            headers = _handshake(
                client,
                {"Accept": _ACCEPT, "Authorization": f"Bearer {_TOKEN}"},
            )
            payload = _sse_payload(
                client.post(
                    "/mcp", json=_rpc("tools/list", rid=2), headers=headers
                )
            )
    write_surface = {t["name"] for t in payload["result"]["tools"]}
    assert read_only_surface < write_surface
    assert {"record", "propose"} <= write_surface
    assert {"add_source", "remove_source", "list_sources"} <= write_surface


@postgres
def test_valid_bearer_token_search_returns_rows_with_generation_id(
    clean_runtime: str, monkeypatch
):
    """An authenticated read returns REAL rows pinned to a published generation.

    A tool listing without an executed, non-empty call does not prove the
    surface works -- an empty or erroring call surface that reads as
    "connected" is the failure mode, not a pass.

    Since read stack 1 (#3821, PACT read-passage-rows-3666) the default text
    reply carries no generation id. A program that needs the pin asks for
    the JSON result format (request ``_meta`` ``okg/result_format: json``)
    and reads ``pinned_generation_id`` from it, so this call does the same.
    """
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    # The read path refuses a checkpoint whose catalog_version_id is NULL
    # (generation_resolver._required_catalog_version_id ->
    # GenerationPinInvalid "refusing to substitute global catalog authority"),
    # so the checkpoint must pin an applied catalog version the way every
    # other graph-reading test seeds one. Without this the authenticated call
    # returns isError=true and this test can never observe rows -- which is
    # the "empty or erroring call surface reading as connected" failure the
    # requirement rules out, arriving as a failure instead of a false pass.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now()) RETURNING catalog_version_id"
            )
            catalog_version_id = cur.fetchone()[0]

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=True)
    try:
        store.apply_facts(
            [NodeFact("doc:auth", "document", {"text": "pact"}, {}, {})],
            branch="default",
            source="auth.src",
        )
        generation_id = store.checkpoint_generation(
            deployment_name="default",
            branch="default",
            catalog_version_id=catalog_version_id,
        )
    finally:
        store.close()

    monkeypatch.delenv("OKG_MCP_SURFACE_PROBE", raising=False)
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.setenv(_TOKEN_ENV, _TOKEN)
    captured: dict = {}
    monkeypatch.setattr(
        FastMCP, "run", lambda self, *a, **kw: captured.__setitem__("mcp", self)
    )
    server_mod.run_server(
        transport="streamable-http",
        host="127.0.0.1",
        port=8123,
        dsn=clean_runtime,
        deployment_name="default",
        auth_token_env=_TOKEN_ENV,
    )
    mcp = captured["mcp"]
    try:
        app = mcp.streamable_http_app()
        with TestClient(app, base_url=_BASE_URL) as client:
            headers = _handshake(
                client,
                {"Accept": _ACCEPT, "Authorization": f"Bearer {_TOKEN}"},
            )
            payload = _sse_payload(
                client.post(
                    "/mcp",
                    json=_rpc(
                        "tools/call",
                        {
                            "name": "search",
                            "arguments": {"query": "pact"},
                            "_meta": {"okg/result_format": "json"},
                        },
                        rid=2,
                    ),
                    headers=headers,
                )
            )
    finally:
        mcp._okg_pool.close()

    body = json.dumps(payload)
    assert payload["result"].get("isError") is not True, body
    result = json.loads(payload["result"]["content"][0]["text"])
    assert "doc:auth" in [row.get("node_id") for row in result["results"]], body
    assert str(result.get("pinned_generation_id")) == str(generation_id), body


# ── token supply is operator environment, never manifest or argv ──────


@token_supply
def test_cli_refuses_literal_token_argument_and_names_env_var_fix(capsys):
    """argv is readable by other local processes and lands in shell history."""
    from okg.substrate.cli.main import _build_parser, _cmd_mcp_serve

    parser = _build_parser()
    args = parser.parse_args(
        [
            "mcp-serve",
            "--deployment",
            "default",
            "--transport",
            "streamable-http",
            "--auth-token",
            "s3cr3t-literal-value",
        ]
    )
    assert args.auth_token == "s3cr3t-literal-value"
    rc = _cmd_mcp_serve(args, as_json=True)
    assert rc != 0
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    # The refusal names the env-var form as the fix rather than accepting the
    # literal with a warning, and never echoes the value it refused.
    assert "--auth-token-env" in combined
    assert "s3cr3t-literal-value" not in combined

    # --help documents the env-var form and, being help, carries no value.
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["mcp-serve", "--help"])
    help_text = capsys.readouterr().out
    assert "--auth-token-env VARIABLE_NAME" in help_text
    assert "Always refused" in help_text
    assert "s3cr3t-literal-value" not in help_text


@token_supply
def test_token_resolves_from_named_env_var(monkeypatch):
    monkeypatch.setenv("OKG_TEST_MCP_TOKEN", _TOKEN)
    assert http_auth.resolve_auth_token("OKG_TEST_MCP_TOKEN") == _TOKEN

    # Unset is fail-closed, and the refusal names the VARIABLE, not a value.
    monkeypatch.delenv("OKG_TEST_MCP_TOKEN", raising=False)
    with pytest.raises(SubstrateError) as exc:
        http_auth.resolve_auth_token("OKG_TEST_MCP_TOKEN")
    assert "OKG_TEST_MCP_TOKEN" in str(exc.value)
    assert exc.value.data["reason"] == "unset"

    # The flag takes a NAME; a value shaped like a token is not a name.
    with pytest.raises(SubstrateError) as exc:
        http_auth.resolve_auth_token("sk-live-abc.def=ghi")
    assert "environment-variable name" in str(exc.value)


@token_supply
def test_invalid_auth_token_env_argument_is_not_echoed(capsys):
    """`--auth-token-env $TOKEN` must not copy the token into the refusal.

    Shell habit expands the variable the flag wanted NAMED, so the rejected
    argument is very often the token itself. Echoing it back would put the
    secret in the error message, the JSON `data` payload, logs, and any
    evidence artifact that captures them.
    """
    import argparse

    from okg.substrate.cli.main import _cmd_mcp_serve

    with pytest.raises(SubstrateError) as exc:
        http_auth.resolve_auth_token(_TOKEN)
    assert _TOKEN not in str(exc.value)
    assert _TOKEN not in repr(exc.value.data)
    assert _TOKEN not in str(exc.value.fix_hint)
    assert exc.value.data["auth_token_env"] == "<withheld>"

    # ...and the same holds end to end through the CLI's JSON error envelope.
    args = argparse.Namespace(
        deployment="default", dsn=None, embedder=None, branch="default",
        allow_writes=False, transport="streamable-http", host="127.0.0.1",
        port=8123, allowed_host=[], auth_token_env=_TOKEN, auth_token=None,
    )
    assert _cmd_mcp_serve(args, as_json=True) != 0
    emitted = capsys.readouterr()
    assert _TOKEN not in emitted.out + emitted.err


@token_supply
def test_ambient_token_variable_alone_does_not_activate_auth(monkeypatch):
    """One activation mechanism: the flag. No ambient variable flips the posture.

    OKG_MCP_ALLOW_WRITES already enables writes from the environment; if a
    fixed variable could also switch authentication ON (or, read the other
    way, if its absence could switch it OFF), the whole security posture would
    change with nothing in argv to audit.
    """
    with _served(monkeypatch, auth_token_env=None, token_value=None) as (mcp, app):
        monkeypatch.setenv(_TOKEN_ENV, _TOKEN)
        assert http_auth.server_has_token_verifier(mcp) is False
        assert http_auth.mcp_route_is_auth_wrapped(app) is False


@token_supply
@pytest.mark.parametrize("value", ["", " ", "\t", "\n", "   \n\t "])
def test_whitespace_only_token_is_unresolvable(monkeypatch, value):
    monkeypatch.setenv("OKG_TEST_MCP_TOKEN", value)
    with pytest.raises(SubstrateError) as exc:
        http_auth.resolve_auth_token("OKG_TEST_MCP_TOKEN")
    assert exc.value.data["reason"] == "empty"
    # A verifier can never be built around such a value either, so no path
    # ends with the empty string authenticating.
    with pytest.raises(SubstrateError):
        http_auth.StaticTokenVerifier(value)


@token_supply
def test_token_comparison_uses_compare_digest(monkeypatch):
    """Naive equality leaks the token by timing.

    "Indistinguishable by the response" does not cover a timing side channel,
    so assert the comparison actually ROUTES THROUGH compare_digest: a spy that
    returns False must make a correct token fail, which an `==` implementation
    would not do.
    """
    import anyio

    verifier = http_auth.StaticTokenVerifier(_TOKEN)
    assert anyio.run(verifier.verify_token, _TOKEN) is not None
    assert anyio.run(verifier.verify_token, _WRONG_TOKEN) is None

    calls: list[tuple[bytes, bytes]] = []

    def _spy(a, b):
        calls.append((a, b))
        return False

    monkeypatch.setattr(http_auth.hmac, "compare_digest", _spy)
    assert anyio.run(verifier.verify_token, _TOKEN) is None
    assert calls, "verify_token did not go through hmac.compare_digest"
    assert calls[0][0] == _TOKEN.encode("utf-8")

    # And a spy that accepts makes even a wrong token pass -- proving the
    # decision is the compare_digest result, not an incidental equality check.
    monkeypatch.setattr(http_auth.hmac, "compare_digest", lambda a, b: True)
    assert anyio.run(verifier.verify_token, _WRONG_TOKEN) is not None


@token_supply
def test_manifest_auth_token_env_key_is_typed_and_literal_key_raises(tmp_path):
    """The manifest rule is STRUCTURAL: no is-this-a-credential value heuristic.

    Two halves, both enforced by the schema's unknown-key rejection rather than
    by inspecting values:

    1. A literal-credential key is a load-time DeploymentConfigError.
    2. The only key this surface will ever accept is chat.mcp.auth_token_env,
       and it carries a VARIABLE NAME -- resolve_auth_token refuses anything
       that is not a valid environment-variable name, so even a manifest that
       smuggled a token into that key would be refused rather than used.

    The typed `chat` block itself is owned by the sibling chat-manifest-block
    PACT and has not landed; until it does, EVERY chat block is refused at load,
    which is the fail-closed direction. The tripwire assertion below fails the
    day `chat` becomes a known key, forcing whoever lands it to replace this
    with the round-trip assertion rather than letting it quietly go vacuous.
    """
    from okg.substrate.deployments import (
        DeploymentConfigError,
        DeploymentManifest,
        _KNOWN_TOP_LEVEL_KEYS,
    )

    def _write(block: str) -> object:
        deployment = tmp_path / block[:12].strip().replace(":", "_")
        (deployment / "schemas").mkdir(parents=True, exist_ok=True)
        (deployment / "source_registry.yaml").write_text("{}\n", encoding="utf-8")
        path = deployment / "deployment.yaml"
        path.write_text(
            "name: auth-fixture\n"
            "description: fixture\n"
            "schema_dir: schemas\n"
            "source_registry: source_registry.yaml\n" + block,
            encoding="utf-8",
        )
        return path

    literal = _write("chat:\n  mcp:\n    auth_token: s3cr3t-literal-value\n")
    with pytest.raises(DeploymentConfigError) as exc:
        DeploymentManifest.from_yaml(literal)
    assert "chat" in str(exc.value)

    # A manifest that declares nothing chat-shaped still loads unchanged.
    plain = _write("")
    assert DeploymentManifest.from_yaml(plain).name == "auth-fixture"

    # The tripwire this replaced fired when chat-manifest-block landed: chat
    # is now a typed block, so the round-trip it demanded is asserted here.
    # The literal-token rejection above is what actually matters and is
    # re-verified against the typed loader, not a bare unknown-key refusal.
    assert "chat" in _KNOWN_TOP_LEVEL_KEYS

    # The round trip is asserted through the block parser rather than a whole
    # deployment fixture: an enabled chat must declare its full required set
    # AND `enabled: false` must stand alone, so a manifest carrying only the
    # one field under test is illegal by design (both refusals are
    # chat-manifest-block's, and its own suite owns them).
    from okg.substrate.deployments.chat_block import parse_chat_block

    block = parse_chat_block(
        {
            "chat": {
                "enabled": True,
                # No `theme`: the recorded knob census measured it as not
                # drivable at the pinned Open WebUI version, so chat.ui no
                # longer admits it (chat-manifest-block owns that refusal).
                "ui": {
                    "site_name": "auth-fixture",
                    "banner": "auth fixture banner",
                },
                "models": {"builtin": ["openai/gpt-5"], "default": "openai/gpt-5"},
                "preset": {
                    "display_name": "auth-fixture",
                    "description": "fixture",
                    "system_prompt_ref": "chat/system_prompt.md",
                    "suggestions": ["hello?"],
                },
                "search_profile": "analyst",
                "mcp": {
                    "port": 8101,
                    "tools": ["search"],
                    "auth_token_env": "OKG_MCP_AUTH_TOKEN",
                },
            },
            "search": {"profiles": {"analyst": {"subtypes": ["document"]}}},
        },
        admitted_tools=("search",),
    )
    assert block is not None and block.mcp is not None
    # The manifest carries the VARIABLE NAME, never a credential: the
    # no-secrets rule (design.md section 4) enforced by the type, with the
    # value surviving the round trip unchanged.
    assert block.mcp.auth_token_env == "OKG_MCP_AUTH_TOKEN"

    # The typed key's VALUE is a variable name, enforced by the same resolver
    # the CLI flag uses. A literal token in that slot is refused, not consumed.
    with pytest.raises(SubstrateError):
        http_auth.resolve_auth_token("s3cr3t-literal-value")


@token_supply
def test_startup_log_names_env_var_not_token_value(monkeypatch, caplog):
    """The startup log names the variable; the value never reaches a log record."""
    with caplog.at_level(logging.INFO, logger=server_mod.log.name):
        with _served(monkeypatch) as (_mcp, _app):
            pass
    serving = [r for r in caplog.records if "serving MCP over" in r.getMessage()]
    assert serving, [r.getMessage() for r in caplog.records]
    message = serving[0].getMessage()
    assert _TOKEN_ENV in message
    assert "bearer authentication required" in message
    for record in caplog.records:
        rendered = record.getMessage()
        assert _TOKEN not in rendered, rendered
        assert _TOKEN not in str(record.args), record.args
