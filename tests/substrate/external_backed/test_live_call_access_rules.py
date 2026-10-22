"""A live relation call is refused while access rules are on (issue #3923).

Security fix. Under Nomos read enforcement in `audit` or `enforce` mode,
`expand(live=...)` used to reach the outside system: the only Nomos check on
that path is the per-action live gate, which decides as the deployment's
service principal, not as the reader. The call is now refused by name, as
`live_refused_under_access_rules`, with ZERO adapter calls, when the rules are
on, and also when whether they are on cannot be told ("cannot be told" is never
read as "off"). A page of an answer kept while the rules were off
(`expand(next=<token>)`) is refused the same way once they are on.

Each test drives the REGISTERED `expand` tool on a real publish, with a
counting adapter and the live gate stubbed to ALLOW, so the refusal under test
cannot be the gate's. The anchor node comes from a trusted source and a
read-allow bundle is active, so the anchor is visible to the reader: a refusal
for any other reason (`node_not_visible`, `live_relation_undeclared`) fails.
"""
from __future__ import annotations

import uuid
from typing import Any

import anyio
import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import (
    SubtypeSpec,
    bind_test_catalog_writer,
    seed_catalog,
)
from tests.substrate.external_backed.test_live_relation_call_tool import (
    CountingAdapter,
    _close_built_servers,  # noqa: F401  (autouse fixture)
    _declare_live_relation,
    _gate,
    _json_result_format,  # noqa: F401  (autouse fixture)
    _server,
)
from tests.substrate.mcp._result_payload import payload

pytestmark = pytest.mark.postgres

READER = "subject:live-reader"
MAIN = "system:analyst_search_fixture"
CODE = "live_refused_under_access_rules"
MESSAGE = "live reads are refused while access rules are on (audit/enforce)"
#: The refusals this fix must not be mistaken for: either means the anchor or
#: the relation was the problem, not the access rules.
NOT_THE_REFUSAL_WE_WANT = {"live_relation_undeclared", "node_not_visible"}
#: `OKG_NOMOS_RUNTIME_ENFORCEMENT` token for each mode ("1" is `enforce`).
ENV_TOKEN = {"off": "off", "audit": "audit", "enforce": "1"}


def _fixture_trusted(dsn: str) -> str:
    """One `page` hub that declares a live relation, from a trusted source."""
    sfx = uuid.uuid4().hex[:8]
    hub, peer = f"n:hub-{sfx}", f"n:peer-{sfx}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=[SubtypeSpec("page")],
                     narrowings=[("page", "cites", "page")])
        ChronosGraphStore.from_connection(conn).apply_facts([
            NodeFact(hub, "page", {"text": "hub"}, {"id": hub}, {"v": 1}),
            NodeFact(peer, "page", {"text": "peer"}, {"id": peer}, {"v": 1}),
            EdgeFact(src=peer, dst=hub, edge_type="cites", attrs={},
                     source_record_id={"src": peer, "dst": hub},
                     source_revision={"v": 1}),
        ], source=MAIN, strict_endpoints=True)
    Publisher(dsn, "default",
              catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    with psycopg.connect(dsn, autocommit=True) as conn:
        _declare_live_relation(conn)
    from tests.substrate.mcp.test_search_list import _trust

    _trust(dsn, "default", MAIN)
    return hub


def _allow_reads(dsn: str) -> None:
    """A bundle that lets READER read the graph and call tools."""
    from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph

    bundle = compile_policy_graph({
        "policy_bundle_id": f"nomos:test:livexp:{uuid.uuid4().hex[:8]}",
        "policy_version": "v1", "default_effect": "deny",
        "rules": [{"rule_id": "allow_read", "effect": "allow",
                   "actions": ["graph.read.mcp", "mcp.tool.call"],
                   "subjects": [READER], "priority": 100}],
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn, deployment_name="default", bundle=bundle,
            scope="deployment:default", activated_by="test:operator")
        conn.commit()


def _rules(monkeypatch: pytest.MonkeyPatch, dsn: str, mode: str) -> None:
    """Put the deployment's access rules in `mode` for the calls that follow."""
    if mode != "off":
        _allow_reads(dsn)
        monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", ENV_TOKEN[mode])


@pytest.fixture
def live_store():
    """A fresh store for kept live answers, so a test keeps nothing past its end."""
    from okg.substrate.mcp import live_result_sets

    fresh = live_result_sets.new_store()
    previous = live_result_sets.install_live_store(fresh)
    try:
        yield fresh
    finally:
        live_result_sets.install_live_store(previous)


def _serve(server: Any, arguments: dict[str, Any]) -> Any:
    """One `expand` call on the registered tool."""
    from okg.substrate.mcp.result_text import result_format_scope

    async def drive() -> Any:
        with result_format_scope("json"):
            return await server.call_tool("expand", arguments)

    return anyio.run(drive)


def _live_expand(server: Any, hub: str, **extra: Any) -> Any:
    return _serve(server, {
        "input": {"node_id": hub}, "direction": "in",
        "live": {"relation": "hits", "args": {"q": "x"}}, **extra,
    })


def _plain_expand(server: Any, hub: str) -> dict:
    return payload(_serve(server, {"input": {"node_id": hub}, "direction": "in"}))


def _assert_refused(result: Any, adapter: CountingAdapter, *, reason: str) -> None:
    body = payload(result)
    err = body.get("error") or {}
    assert result.isError is True, f"not refused: {str(body)[:400]}"
    assert err.get("code") not in NOT_THE_REFUSAL_WE_WANT, (
        f"refused for the wrong reason: {err}")
    assert err.get("code") == CODE, err
    assert MESSAGE in err.get("message", ""), err
    assert (err.get("data") or {}).get("reason") == reason, err
    assert "external_live" not in body
    assert len(adapter.calls) == 0, (
        f"the adapter ran {len(adapter.calls)} time(s) on a refused call")


@pytest.fixture
def world(clean_runtime: str, monkeypatch, tmp_path):
    """A trusted anchor, a counting adapter, and a server whose live gate allows."""
    hub = _fixture_trusted(clean_runtime)
    adapter = CountingAdapter()
    # The access rules are off while the server is built; each test turns
    # them on (or leaves them off) afterwards, as an operator can.
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    server = _server(clean_runtime, adapter=adapter,
                     monkeypatch=monkeypatch, tmp_path=tmp_path)
    server._okg_live_gate = _gate(True)
    return {"dsn": clean_runtime, "hub": hub, "adapter": adapter, "server": server}


def test_the_control_with_access_rules_off_makes_one_adapter_call(
    world, monkeypatch,
) -> None:
    """The anchor and the relation work; only the access-rule refusal is new."""
    _rules(monkeypatch, world["dsn"], "off")
    result = _live_expand(world["server"], world["hub"])
    body = payload(result)
    assert result.isError is False, body
    assert len(world["adapter"].calls) == 1
    assert body["external_live"]["state"] == "ok"


@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_a_live_expand_is_refused_when_access_rules_are_on(
    world, monkeypatch, mode: str,
) -> None:
    _rules(monkeypatch, world["dsn"], mode)
    # Precondition: the anchor is visible to this reader in this mode, so the
    # refusal below is not a missing node.
    plain = _plain_expand(world["server"], world["hub"])
    assert [r["node_id"] for r in plain.get("neighbors", [])], (
        f"anchor not visible under {mode}: {str(plain)[:400]}")

    result = _live_expand(world["server"], world["hub"])

    _assert_refused(result, world["adapter"], reason="access_rules_on")
    assert payload(result)["error"]["data"]["relation"] == "hits"


def test_the_refusal_comes_before_the_anchor_is_read(world, monkeypatch) -> None:
    """A node that does not exist gets the access-rule refusal too, not
    `node_not_visible`: nothing is read before the check."""
    _rules(monkeypatch, world["dsn"], "audit")
    result = _live_expand(world["server"], "n:no-such-node")
    _assert_refused(result, world["adapter"], reason="access_rules_on")


class _Unreadable(Exception):
    """Stands for any failure to read the deployment's access-rule state."""


def _state_unreadable(monkeypatch: pytest.MonkeyPatch, server: Any, how: str) -> None:
    if how == "no_deployment_name":
        # The server does not know which deployment's rules to ask about.
        monkeypatch.setattr(server, "_okg_read_deployment_name", None)
    elif how == "state_read_raises":
        from okg.substrate.nomos import runtime_enforcement

        def boom(_name: str) -> bool:
            raise _Unreadable("manifest unreadable")

        monkeypatch.setattr(runtime_enforcement, "deployment_requires_nomos_runtime", boom)
    elif how == "invalid_mode_token":
        monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "sometimes")
    elif how == "server_has_no_answer":
        # A server built without the state read at all.
        monkeypatch.setattr(server, "_okg_access_rules_state", None, raising=False)
    else:  # pragma: no cover
        raise AssertionError(how)


@pytest.mark.parametrize("how", [
    "no_deployment_name", "state_read_raises", "invalid_mode_token",
    "server_has_no_answer",
])
def test_a_live_expand_is_refused_when_the_rules_state_cannot_be_told(
    world, monkeypatch, how: str,
) -> None:
    _state_unreadable(monkeypatch, world["server"], how)

    result = _live_expand(world["server"], world["hub"])

    _assert_refused(result, world["adapter"], reason="access_rules_unknown")


def _cut_live_expand(world: dict[str, Any]) -> dict[str, Any]:
    """A live expand whose answer does not fit the reply, so the server keeps
    it and names a `next` token. Runs with the access rules off."""
    result = _live_expand(world["server"], world["hub"], max_bytes=16_384)
    body = payload(result)
    assert result.isError is False, body
    live = body["external_live"]
    assert live["truncated"] is True and live.get("next"), live
    return live


@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_the_next_page_of_a_kept_live_answer_is_refused_once_rules_are_on(
    clean_runtime: str, monkeypatch, tmp_path, live_store, mode: str,
) -> None:
    hub = _fixture_trusted(clean_runtime)
    adapter = CountingAdapter(rows=[
        {"name": f"row-{i:05d}", "size": i, "note": "n" * 40} for i in range(2_000)
    ])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    server = _server(clean_runtime, adapter=adapter,
                     monkeypatch=monkeypatch, tmp_path=tmp_path)
    server._okg_live_gate = _gate(True)
    world = {"dsn": clean_runtime, "hub": hub, "adapter": adapter, "server": server}

    first = _cut_live_expand(world)
    assert len(adapter.calls) == 1
    # Control: with the rules still off, the token serves the next page and
    # asks the live system nothing.
    page = _serve(server, {"next": first["next"]})
    assert page.isError is False, payload(page)
    assert payload(page)["result_kind"] == "live_page"
    assert len(adapter.calls) == 1

    # The operator turns access rules on. The same token is refused.
    _rules(monkeypatch, clean_runtime, mode)
    refused = _serve(server, {"next": first["next"]})

    err = payload(refused).get("error") or {}
    assert refused.isError is True, str(payload(refused))[:400]
    assert err.get("code") == CODE, err
    assert MESSAGE in err.get("message", ""), err
    assert "external_live" not in payload(refused)
    assert len(adapter.calls) == 1, "the refused page made no adapter call"


@pytest.mark.parametrize("tool,extra", [
    ("filter", {"limit": 5}),
    ("map", {"limit": 5}),
    ("aggregate", {"by": [], "metrics": ["count"]}),
])
@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_a_kept_live_answer_is_not_read_by_id_once_rules_are_on(
    world, monkeypatch, live_store, tool: str, extra: dict[str, Any], mode: str,
) -> None:
    """#3666 review M2. A set kept by `expand(live=..., retain=true)` while
    the rules were off is not read by filter, map or aggregate once the
    operator turns them on: refused by name, with no row and no call."""
    kept = payload(_serve(world["server"], {
        "input": {"node_id": world["hub"]}, "direction": "in",
        "live": {"relation": "hits", "args": {"q": "x"}, "retain": True},
    }))
    ref = kept["external_live"]["result_reference"]["id"]
    arguments = {"input": {"ref": ref}, **extra}

    def read() -> Any:
        from okg.substrate.mcp.result_text import result_format_scope

        async def drive() -> Any:
            with result_format_scope("json"):
                return await world["server"].call_tool(tool, arguments)

        return anyio.run(drive)

    control = read()
    assert control.isError is False, payload(control)
    assert len(world["adapter"].calls) == 1

    _rules(monkeypatch, world["dsn"], mode)
    refused = read()

    body = payload(refused)
    err = body.get("error") or {}
    assert refused.isError is True, str(body)[:400]
    assert err.get("code") == CODE, err
    assert MESSAGE in err.get("message", ""), err
    assert (err.get("data") or {}).get("tool_called") == tool, err
    assert not body.get("items") and not body.get("groups"), "no kept row served"
    assert len(world["adapter"].calls) == 1, "the refused read made no adapter call"
