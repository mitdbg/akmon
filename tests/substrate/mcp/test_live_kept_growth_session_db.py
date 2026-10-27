"""Growth of a kept live set on a real client session (issue #4050).

PACT live-kept-growth-session-4050. A later call that names a field a kept
live-type set lacks grows it: only that field is fetched, for the held nodes.
The resolver runs before the tool body, where the server's current MCP
context is not set yet, so its kept-set lookup read "no session", took the
owner for another session, and skipped growth: the column came back null on
every row, with no upstream call and no note.

These tests drive the server through a real in-memory MCP client
(`create_connected_server_and_client_session`), so each call has the logical
session a real client has. Nothing replaces the server's session lookup.
"""
from __future__ import annotations

import json
from typing import Any, Iterator

import pytest

from okg.substrate.mcp import live_result_sets
from tests.substrate.mcp.test_live_graph_keep import Clock, build_lab

pytestmark = pytest.mark.postgres

JSON_META = {"okg/result_format": "json"}
USAGE = "example_get_site_usage_by_source"
GROW = ["node_id", "attrs.static_total"]


@pytest.fixture
def lab(clean_runtime: str, tmp_path, monkeypatch) -> Iterator[Any]:
    clock = Clock()
    previous = live_result_sets.install_live_store(live_result_sets.new_store(clock=clock))
    try:
        with build_lab(clean_runtime, tmp_path, monkeypatch) as built:
            built.clock = clock
            yield built
    finally:
        live_result_sets.install_live_store(previous)


async def _call(client: Any, tool: str, arguments: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    result = await client.call_tool(tool, arguments, meta=JSON_META)
    return bool(result.isError), json.loads(result.content[0].text)


def _sessions(lab: Any, body: Any) -> Any:
    """Run `body(first, second)` with two real client sessions of one server."""
    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    out: dict[str, Any] = {}

    async def run() -> None:
        async with create_connected_server_and_client_session(lab.mcp) as first:
            async with create_connected_server_and_client_session(lab.mcp) as second:
                out["value"] = await body(first, second)

    anyio.run(run)
    return out["value"]


async def _kept_cut_read(client: Any) -> str:
    failed, body = await _call(client, "filter", {"where": {"type": "site"}, "limit": 5})
    assert not failed and body["truncated"] is True, json.dumps(body)[:1500]
    return str(body["result_reference"]["id"])


def test_growth_runs_on_a_real_client_session(lab) -> None:
    """MF: a cut filter is kept; map over it naming a field the set lacks
    fetches that field for every held node, through a real session. On dev:
    0 usage calls and the column null on all 25 rows, with no error."""
    lab.example.set_sites(25)

    async def body(client: Any, _second: Any) -> Any:
        ref = await _kept_cut_read(client)
        before = lab.example.made(USAGE)
        failed, grown = await _call(client, "map", {"input": {"ref": ref}, "select": GROW})
        return ref, failed, grown, lab.example.made(USAGE) - before

    ref, failed, grown, calls = _sessions(lab, body)
    assert not failed, json.dumps(grown)[:1500]
    assert calls == 25, "one usage call per held node"
    assert len(grown["items"]) == 25
    assert {item["attrs_static_total"] for item in grown["items"]} == {100}
    assert grown["grown_from"] == ref


def test_another_session_of_the_same_reader_is_still_refused(lab) -> None:
    """Another logical session hears `unknown`, exactly as an id never issued,
    with no upstream call, and the owner's set survives the probe.
    Re-pinned by PACT read-live-handles (lead decision Q1): on dev it was
    `binding_mismatch`, `field: session`, and the set was discarded."""
    lab.example.set_sites(25)

    async def body(first: Any, second: Any) -> Any:
        ref = await _kept_cut_read(first)
        before = lab.example.made(USAGE)
        failed, refused = await _call(second, "map", {"input": {"ref": ref}, "select": GROW})
        calls = lab.example.made(USAGE) - before
        owner = await _call(first, "map", {"input": ref, "select": ["node_id", "attrs.kind"]})
        return failed, refused, calls, owner

    failed, refused, calls, (owner_failed, owner) = _sessions(lab, body)
    assert failed, json.dumps(refused)[:1500]
    error = refused["error"]
    assert error["code"] == "result_reference_invalid"
    assert error["data"]["reason"] == "unknown"
    assert calls == 0
    assert not owner_failed and len(owner["items"]) == 25, "the owner's set survives"


def test_another_reader_is_still_refused_as_an_unknown_id(lab) -> None:
    """Unchanged from dev: another reader (another `authority`) in the same
    session hears `unknown`, and no upstream call is made."""
    lab.example.set_sites(25)

    async def body(client: Any, _second: Any) -> Any:
        ref = await _kept_cut_read(client)
        before = lab.example.made(USAGE)
        failed, refused = await _call(client, "map", {
            "input": {"ref": ref}, "select": GROW, "authority": {"note": "another reader"},
        })
        return failed, refused, lab.example.made(USAGE) - before

    failed, refused, calls = _sessions(lab, body)
    assert failed and refused["error"]["data"]["reason"] == "unknown", json.dumps(refused)[:1500]
    assert calls == 0


def test_a_field_growth_did_not_fetch_is_refused_by_name_not_served_null(lab) -> None:
    """MF: when growth is skipped (here: the growth lookup answers nothing,
    standing in for any skipped growth), a read naming a field no kept row
    holds is refused `field_not_fetched`, never served with null values.
    On dev: served, the column null on every row."""
    lab.example.set_sites(25)
    lab.mcp._okg_live_kept_set = lambda *args, **kwargs: None  # type: ignore[attr-defined]

    async def body(client: Any, _second: Any) -> Any:
        ref = await _kept_cut_read(client)
        before = lab.example.made(USAGE)
        refusal = await _call(client, "map", {"input": {"ref": ref}, "select": GROW})
        held = await _call(client, "map", {"input": {"ref": ref}, "select": ["node_id", "attrs.kind"]})
        return refusal, held, lab.example.made(USAGE) - before

    (failed, refused), (held_failed, held), calls = _sessions(lab, body)
    assert failed, json.dumps(refused)[:1500]
    error = refused["error"]
    assert error["code"] == "live_read_refused" and error["data"]["reason"] == "field_not_fetched"
    assert error["data"]["fields"] == ["attrs.static_total"]
    assert calls == 0
    # A field the kept rows hold is still served.
    assert not held_failed and len(held["items"]) == 25


# ---- PACT read-live-handles through real sessions (review F1, lead decision Q1)


def test_a_string_handle_grows_on_a_real_client_session(lab) -> None:
    """3a-i test 13 through a real session: every live read is kept, and
    `map(input="<handle>")` naming a field the set lacks fetches it."""
    lab.example.set_sites(25)

    async def body(client: Any, _second: Any) -> Any:
        failed, first = await _call(client, "filter", {"where": {"type": "site"}})
        assert not failed, json.dumps(first)[:1500]
        handle = first["handle"]["id"]
        before = lab.example.made(USAGE)
        failed, grown = await _call(client, "map", {"input": handle, "select": GROW})
        return failed, grown, lab.example.made(USAGE) - before

    failed, grown, calls = _sessions(lab, body)
    assert not failed, json.dumps(grown)[:1500]
    assert calls == 25
    assert {item["attrs_static_total"] for item in grown["items"]} == {100}


def test_another_session_hears_unknown_for_an_expired_handle_and_the_owner_expired(lab) -> None:
    """Lead decision Q1 through real sessions: after expiry another session
    still hears `unknown`; the owner hears `expired` with its retry call."""
    lab.example.set_sites(5)

    async def body(first: Any, second: Any) -> Any:
        failed, kept = await _call(first, "filter", {"where": {"type": "site"}})
        assert not failed, json.dumps(kept)[:1500]
        handle = kept["handle"]["id"]
        lab.clock.now += 601
        other = await _call(second, "filter", {"input": handle})
        owner = await _call(first, "filter", {"input": handle})
        return other, owner

    (other_failed, other), (owner_failed, owner) = _sessions(lab, body)
    assert other_failed and other["error"]["data"]["reason"] == "unknown"
    assert owner_failed and owner["error"]["data"]["reason"] == "expired"
    assert owner["error"]["fix_hint"].startswith("retry: filter(")


def test_a_sort_on_a_field_growth_did_not_fetch_is_refused_by_name(lab) -> None:
    """Review N1: a sort on a field no kept row holds, when growth is skipped,
    is refused `field_not_fetched` (its direction split off), never sorted
    on null values."""
    lab.example.set_sites(25)
    lab.mcp._okg_live_kept_set = lambda *args, **kwargs: None  # type: ignore[attr-defined]

    async def body(client: Any, _second: Any) -> Any:
        ref = await _kept_cut_read(client)
        before = lab.example.made(USAGE)
        refusal = await _call(
            client, "filter",
            {"input": {"ref": ref}, "order_by": "attrs.static_total desc", "limit": 5},
        )
        return refusal, lab.example.made(USAGE) - before

    (failed, refused), calls = _sessions(lab, body)
    assert failed, json.dumps(refused)[:1500]
    error = refused["error"]
    assert error["code"] == "live_read_refused" and error["data"]["reason"] == "field_not_fetched"
    assert error["data"]["fields"] == ["attrs.static_total"]
    assert calls == 0


@pytest.mark.parametrize("tool,arguments", [
    ("filter", {"order_by": "-attrs.static_total", "limit": 5}),
    ("filter", {"where": {"attrs.static_total": {"op": "gt", "value": 1}}}),
    ("map", {"select": ["node_id", "attrs.static_total"]}),
    ("aggregate", {"by": ["attrs.kind"], "metrics": ["sum(attrs.static_total)"]}),
])
def test_a_handle_whose_growth_was_skipped_is_refused_on_every_read_path(
    lab, tool, arguments,
) -> None:
    """PACT read-live-handles paths (a string handle, order_by, condition,
    select, metrics) when growth is skipped: refused `field_not_fetched`
    with zero upstream calls, never served null."""
    lab.example.set_sites(25)
    lab.mcp._okg_live_kept_set = lambda *args, **kwargs: None  # type: ignore[attr-defined]

    async def body(client: Any, _second: Any) -> Any:
        failed, first = await _call(client, "filter", {"where": {"type": "site"}})
        assert not failed, json.dumps(first)[:1500]
        before = lab.example.total()
        refusal = await _call(client, tool, {"input": first["handle"]["id"], **arguments})
        return refusal, lab.example.total() - before

    (failed, refused), calls = _sessions(lab, body)
    assert failed, json.dumps(refused)[:1500]
    assert refused["error"]["data"]["reason"] == "field_not_fetched"
    assert refused["error"]["data"]["fields"] == ["attrs.static_total"]
    assert calls == 0


OPEN = "example_list_site_attributes"


def _distinct_totals(lab: Any) -> None:
    """Usage totals that differ per site (site i: 1000 + i), so a sort shows."""
    runners = lab.mcp._okg_live_relation_runners
    recording = runners[USAGE]

    def usage(**kwargs: Any) -> Any:
        rows = recording(**kwargs)
        name = kwargs["args"]["site"]
        return [{**row, "total": 1000 + int(name[-4:])} for row in rows]

    runners[USAGE] = usage


@pytest.mark.parametrize("order_by,descending", [
    ("attrs.static_total desc", True), ("attrs.static_total asc", False),
    ("-attrs.static_total", True), ("attrs.static_total DESC", True),
])
def test_a_sort_on_a_missing_field_grows_that_field_and_sorts(lab, order_by, descending) -> None:
    """Review R3-1 (#4054): every spelling of a sort direction grows the
    field itself (the usage relation, one call per node; never the open
    fields with "static_total desc" as a key) and the page is sorted."""
    lab.example.set_sites(25)
    _distinct_totals(lab)

    async def body(client: Any, _second: Any) -> Any:
        failed, first = await _call(client, "filter", {"where": {"type": "site"}})
        assert not failed, json.dumps(first)[:1500]
        usage, opened = lab.example.made(USAGE), lab.example.made(OPEN)
        result = await _call(client, "filter", {
            "input": first["handle"]["id"], "order_by": order_by, "limit": 3,
        })
        return result, lab.example.made(USAGE) - usage, lab.example.made(OPEN) - opened

    (failed, page), usage_calls, open_calls = _sessions(lab, body)
    assert not failed, json.dumps(page)[:1500]
    assert usage_calls == 25 and open_calls == 0
    totals = [item["attrs"]["static_total"] for item in page["items"]]
    assert totals == ([1024, 1023, 1022] if descending else [1000, 1001, 1002])


def test_a_sort_on_a_field_no_row_holds_after_growth_is_refused(lab) -> None:
    """Review R3-1: when the open fields were read, a key they did not carry
    cannot be fetched; a sort on it is refused by name, never served in input
    order as if sorted."""
    lab.example.set_sites(25)

    async def body(client: Any, _second: Any) -> Any:
        failed, first = await _call(client, "filter", {
            "where": {"type": "site"}, "select": ["node_id", "attrs.tier"],
        })
        assert not failed, json.dumps(first)[:1500]
        before = lab.example.total()
        result = await _call(client, "filter", {
            "input": first["handle"]["id"], "order_by": "attrs.no_such_key desc", "limit": 3,
        })
        return result, lab.example.total() - before

    (failed, refused), calls = _sessions(lab, body)
    assert failed, json.dumps(refused)[:1500]
    assert refused["error"]["data"]["reason"] in ("order_by_field_not_held", "key_absent_on_every_row")
    assert calls == 0
