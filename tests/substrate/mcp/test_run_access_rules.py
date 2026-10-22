"""`run` under access rules (Nomos audit or enforce): refused before any step,
and no composed statement is built or run (issue #3666, PACT
run-composed-3666, req.run-composed-3666.access-rules).

A spy on the server's run backend records every composed read (a set, a
value list, a held set, a count, the run's pin); every step form is driven
under each mode and the spy must stay empty.
"""
from __future__ import annotations

import json

import anyio
import pytest

from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import make_search_deployment, node, publish_nodes

pytestmark = pytest.mark.postgres

SUBTYPES = [SubtypeSpec("item"), SubtypeSpec("doc")]
BACKEND_METHODS = (
    "current_generation", "step_set", "ids_set", "values_sql", "materialize", "count",
    "join_far", "join_facts",
)


def _corpus(tmp_path, dsn: str) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    nodes = [node(f"item:{i:04d}", "item", f"item {i}", bucket=i % 210) for i in range(240)]
    nodes += [node(f"doc:{i:02d}", "doc", f"zebra page {i}", topic=f"t{i % 3}") for i in range(12)]
    publish_nodes(dsn, deployment, subtypes=SUBTYPES, nodes=nodes)
    return deployment


FORMS = {
    "count line": [
        {"filter": {"where": {"type": "doc"}}},
        {"aggregate": {"where": "$1", "by": ["attrs.topic"], "metrics": ["count"]}},
    ],
    "row cap": [{"filter": {"where": {"type": "item"}}}],
    "show false": [
        {"filter": {"where": {"type": "item"}}, "show": False},
        {"map": {"where": {"type": "doc", "attrs.topic": "$1.attrs.bucket"}}},
    ],
    "a cut": [{"filter": {"where": {"type": "doc"}, "limit": 2}}],
    "its next call": [{"filter": {"where": {"type": "doc"}, "limit": 2, "offset": 2}}],
    "a search set": [
        {"search": {"query": "zebra", "method": "grep"}},
        {"filter": {"where": "$1"}},
    ],
    "a link join": [
        {"filter": {"where": {"type": "doc"}}},
        {"expand": {"from": "$1", "edge_type": "cites", "direction": "out"}},
    ],
}


@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_every_run_form_is_refused_with_no_composed_statement(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch, mode,
):
    from mcp.shared.memory import create_connected_server_and_client_session

    from okg.substrate.mcp import run_steps
    from okg.substrate.mcp import server as srv

    mcp = srv.build_server(dsn=clean_runtime, deployment_name=_corpus(tmp_path, clean_runtime))
    composed: list[str] = []
    backend = mcp._okg_run_backend
    for name in BACKEND_METHODS:
        real = getattr(backend, name)

        def spy(*args, _name=name, _real=real, **kwargs):
            composed.append(_name)
            return _real(*args, **kwargs)

        monkeypatch.setattr(backend, name, spy)
    real_bind = run_steps.bind_call

    def bind_spy(*args, **kwargs):
        composed.append("bind_call")
        return real_bind(*args, **kwargs)

    monkeypatch.setattr(run_steps, "bind_call", bind_spy)
    try:
        monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
        requests = [("run", {"steps": steps}) for steps in FORMS.values()]
        requests.append(("run", {"steps": FORMS["its next call"], "generation_id": 1}))
        requests += [
            ("filter", {"where": "$1"}),
            ("filter", {"where": "$r1"}),
            ("aggregate", {"where": {"op": "and", "clauses": ["$2", {"type": "doc"}]},
                           "by": ["subtype"], "metrics": ["count"]}),
        ]
        out: list = []

        async def drive() -> None:
            async with create_connected_server_and_client_session(mcp) as client:
                for tool, arguments in requests:
                    for fmt in ("text", "json"):
                        out.append((tool, await client.call_tool(
                            tool, arguments, meta={"okg/result_format": fmt},
                        )))

        anyio.run(drive)
        for tool, result in out:
            text = result.content[0].text
            assert result.isError, (tool, text)
            assert "doc:0" not in text and "item:0" not in text, text
            if tool == "run":
                assert "access rules" in text
                assert "send the first step as its own call" in text
        assert composed == [], composed
        # The first step offered as its own call is checked by the rules:
        # it is answered (or refused) as a plain filter, with no composition.
        hint = json.loads(out[1][1].content[0].text)["error"]["fix_hint"]
        assert 'filter(where={"type":"doc"})' in hint
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def _offered_call(hint: str) -> tuple[str, dict]:
    """The call a hint offers after "check: ", as (tool, arguments)."""
    text = hint.split("check: ", 1)[1]
    tool, inner = text.split("(", 1)
    inner, arguments, decoder = inner[:-1], {}, json.JSONDecoder()
    while inner:
        key, rest = inner.split("=", 1)
        arguments[key], end = decoder.raw_decode(rest)
        inner = rest[end:].removeprefix(", ")
    return tool, arguments


ARGUMENT_CODES = {"unknown_operator_argument", "invalid_operator_arguments"}


@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_the_first_step_hint_offers_only_a_call_that_is_taken(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch, mode,
):
    """FAILS BEFORE (gate 1 on the run head): a first step with an argument
    its tool does not declare was echoed into the hint as
    `filter(bogus_argument=1, where=..)`, a call refused with
    unknown_operator_argument. Now that step offers no call and names the
    argument; a good first step offers its call, and the offered call,
    sent as written, is not refused for its arguments."""
    from mcp.shared.memory import create_connected_server_and_client_session

    from okg.substrate.mcp import server as srv

    mcp = srv.build_server(dsn=clean_runtime, deployment_name=_corpus(tmp_path, clean_runtime))
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    bad = {"filter": {"bogus_argument": 1, "where": {"type": "doc"}}}
    good = {"filter": {"where": {"type": "doc"}, "limit": 3}}
    out: dict = {}

    async def call(client, tool, arguments):
        result = await client.call_tool(tool, arguments, meta={"okg/result_format": "json"})
        return result.isError, json.loads(result.content[0].text)

    async def drive() -> None:
        async with create_connected_server_and_client_session(mcp) as client:
            for name, step in (("bad", bad), ("good", good)):
                out[name] = await call(client, "run", {"steps": [step, {"filter": {
                    "where": "$1"}}]})
            out["bad alone"] = await call(client, "filter", bad["filter"])
            tool, arguments = _offered_call(out["good"][1]["error"]["fix_hint"])
            out["offered"] = await call(client, tool, arguments)
            out["offered as"] = (tool, arguments)

    try:
        anyio.run(drive)
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    for name in ("bad", "good"):
        is_error, payload = out[name]
        assert is_error and payload["error"]["code"] == "run_refused_under_access_rules"
    bad_hint = out["bad"][1]["error"]["fix_hint"]
    assert "bogus_argument" in bad_hint and "check: " not in bad_hint and "filter(" not in bad_hint
    # `where` is read by the grammar rewrite: the hint never asks to drop it.
    assert "'where'" not in bad_hint and '"where"' not in bad_hint, bad_hint
    assert bad_hint.startswith("filter does not accept 'bogus_argument';"), bad_hint
    # The echoed call would have been refused for its argument.
    assert out["bad alone"][1]["error"]["code"] == "unknown_operator_argument"
    assert out["offered as"] == ("filter", good["filter"])
    is_error, payload = out["offered"]
    assert not is_error or payload["error"]["code"] not in ARGUMENT_CODES, payload
