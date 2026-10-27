"""Live handles through the tools (PACT read-live-handles, PR 3a-i).

Every live set read returns a reusable handle; the next call reads it with
`input="<handle>"` and makes no upstream call. Each assertion counts the
adapter calls a client's call really made, against the recording fake system
of `test_live_graph_keep.py` (typed sets) and `test_live_result_sets.py` (raw
`expand(live=...)` answers), through the server's own `call_tool`.

MF marks a test that fails on the stacked base (part 2, #4002, #3989 merged)
for the reason its docstring gives.
"""
from __future__ import annotations

import contextlib
import json
from typing import Any, Iterator

import pytest

from okg.substrate.mcp import live_result_sets
from tests.substrate.mcp.test_live_graph_keep import (
    KEEP_AGG,
    SESSION,
    STATELESS,
    Clock,
    _deny_expand_only,
    _gate,
    build_lab,
    unfence,
)
from tests.substrate.mcp.test_live_result_sets import (  # noqa: F401 - fixtures
    _ok,
    _refused,
    _serve,
    live_store,
    world,
)

pytestmark = pytest.mark.postgres

SITES = {"where": {"type": "site"}}


def _handles() -> Any:
    from okg.substrate.mcp import handles

    return handles


def _fill_process_budget(handles: Any) -> list[int]:
    """Hold the whole in-flight budget, as calls in flight would: one call
    holds at most the per-call bound, so it takes two."""
    budget = handles.decode_budget
    tokens = []
    while budget.in_flight() < budget.per_process_bytes:
        tokens.append(budget.reserve(
            min(budget.per_call_bytes, budget.per_process_bytes - budget.in_flight()),
        ))
    return tokens


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def lab(clean_runtime: str, tmp_path, monkeypatch, clock) -> Iterator[Any]:
    previous = live_result_sets.install_live_store(live_result_sets.new_store(clock=clock))
    try:
        with build_lab(clean_runtime, tmp_path, monkeypatch) as built:
            # The logical session a read runs under: the test's, both when a
            # handle is minted and when it is read.
            built.mcp._okg_live_session = lambda context=None: (  # type: ignore[attr-defined]
                None if STATELESS.get() else SESSION.get(), STATELESS.get(),
            )
            yield built
    finally:
        live_result_sets.install_live_store(previous)


@contextlib.contextmanager
def _session(name: Any) -> Iterator[None]:
    token = SESSION.set(name)
    try:
        yield
    finally:
        SESSION.reset(token)


def _handle(body: dict[str, Any]) -> str:
    handle = body.get("handle")
    assert isinstance(handle, dict) and str(handle.get("id", "")).startswith("l1."), (
        json.dumps(body)[:1500]
    )
    return str(handle["id"])


def _text(lab: Any, tool: str, arguments: dict[str, Any]) -> str:
    from tests.substrate.mcp._result_payload import result_text

    result = lab.call(tool, arguments, fmt="text")
    assert result.isError is False, result.content[0].text[:2000]
    return result_text(result)


# ============================================================ 1: every read keeps


def test_every_live_set_read_returns_a_handle(lab) -> None:
    """MF 1: an uncut first read of at most 20 calls carries a handle.
    Base: not kept ("not_needed")."""
    lab.example.set_sites(5)
    filtered = lab.ok("filter", SITES)
    assert lab.example.total() == 1
    assert filtered["handle"]["rows"] == 5 and filtered["handle"]["order"] == "input order"
    _handle(filtered)
    grouped = lab.ok("aggregate", KEEP_AGG)
    groups = _handle(grouped)
    assert grouped["handle"]["from"].startswith("l1.") and grouped["handle"]["from"] != groups
    mapped = lab.ok("map", {"where": {"type": "site"}, "select": ["node_id", "attrs.kind"]})
    _handle(mapped)
    assert "kept" not in filtered and "kept" not in mapped


def test_a_raw_expand_answer_returns_a_handle(world) -> None:  # noqa: F811
    """MF 1 (raw): an uncut `expand(live=...)` without retain is kept."""
    world["adapter"].rows = [{"name": f"r{i}", "size": i} for i in range(3)]
    body = _ok(world["mcp"], "expand", {
        "input": {"node_id": world["hub"]}, "direction": "in",
        "live": {"relation": "lrs_rows", "args": {"q": "x"}},
    })
    reference = body["external_live"]["result_reference"]
    assert reference["retained"] is True and reference["id"].startswith("l1.")
    page = _ok(world["mcp"], "filter", {"input": reference["id"], "limit": 2})
    assert len(page["items"]) == 2 and len(world["adapter"].calls) == 1


# ============================================================ 2, 13, 14: strings


def test_a_string_handle_input_equals_the_ref_form(lab) -> None:
    """MF 2: filter, map and aggregate. Base: a string is not a handle
    (`filter`/`map` refuse it at validation; `aggregate` reads it as a node id)."""
    lab.example.set_sites(30)
    ref = _handle(lab.ok("filter", SITES))
    calls = lab.example.total()
    for tool, arguments in (
        ("filter", {"where": {"attrs.kind": "COLD"}, "limit": 3}),
        ("map", {"select": ["node_id", "attrs.kind"], "limit": 4}),
        ("aggregate", {"by": ["attrs.kind"], "metrics": ["count"]}),
    ):
        as_string = lab.ok(tool, {"input": ref, **arguments})
        as_object = lab.ok(tool, {"input": {"ref": ref}, **arguments})
        key = "groups" if tool == "aggregate" else "items"
        assert as_string[key] == as_object[key], tool
        assert as_string[key], tool
    assert lab.example.total() == calls, "a read of a handle made an upstream call"


@pytest.mark.parametrize("spelling", ["bare", "json_string", "json_ref"])
def test_a_string_handle_with_a_missing_field_grows_like_the_ref_form(lab, spelling) -> None:
    """MF 13 (bare) and 14 (JSON forms): `map(input="<A>", select=[<field A
    lacks>])` grows, one usage call per row, exactly as `{"ref": A}` does.
    Base: the string is refused (bare) or never grows (JSON)."""
    lab.example.set_sites(25)
    ref = _handle(lab.ok("filter", SITES))
    sent = {
        "bare": ref, "json_string": json.dumps(ref), "json_ref": json.dumps({"ref": ref}),
    }[spelling]
    usage = "example_get_site_usage_by_source"
    grown = lab.ok("map", {"input": sent, "select": ["node_id", "attrs.static_total"]})
    assert lab.example.made(usage) == 25
    assert {item["attrs_static_total"] for item in grown["items"]} == {100}
    again = lab.ok("map", {"input": {"ref": ref}, "select": ["node_id", "attrs.static_total"]})
    assert lab.example.made(usage) == 50, "the object form grows the same way"
    assert again["items"] == grown["items"]


# ============================================================ 3, 15: life


def test_a_live_handle_lives_ten_minutes_and_a_derived_one_gets_its_own(lab, clock) -> None:
    """MF 3: read at 301 s; a derived handle minted then lives its own 600 s.
    Base: expired at 300 s."""
    lab.example.set_sites(30)
    first = _handle(lab.ok("filter", SITES))
    clock.now += 301
    derived = lab.ok("filter", {"input": first, "where": {"attrs.kind": "COLD"}})
    derived_id = _handle(derived)
    assert derived["handle"]["from"] == first
    assert _handles().expires_of(derived_id) == _handles().expires_of(first) + 301
    clock.now += 300
    gone = lab.refused("filter", {"input": first})
    assert gone["data"]["reason"] == "expired"
    page = lab.ok("filter", {"input": derived_id})
    assert len(page["items"]) == 6 and lab.example.total() == 1


def test_a_grown_handles_parent_stays_readable_until_its_own_expiry(lab, clock) -> None:
    """MF 15 (N4): growing A into B leaves A readable with zero calls until
    A's own expiry. Base: A is discarded once B is committed."""
    lab.example.set_sites(25)
    a = _handle(lab.ok("filter", SITES))
    clock.now += 60
    grown = lab.ok("aggregate", {"input": a, "by": ["attrs.static_total"], "metrics": ["count"]})
    b = grown["handle"]["from"]
    assert b.startswith("l1.") and b != a and grown["grown_from"] == a
    calls = lab.example.total()
    page = lab.ok("map", {"input": a, "select": ["node_id"]})
    assert len(page["items"]) == 25 and lab.example.total() == calls
    clock.now += 541
    assert lab.refused("map", {"input": a})["data"]["reason"] == "expired"
    assert len(lab.ok("map", {"input": b, "select": ["node_id"]})["items"]) == 25


# ============================================================ 5: a full server


def test_a_full_server_evicts_the_largest_holders_oldest_and_keeps_a_cut_reply(lab) -> None:
    """MF 5: other sessions filled the server; a cut first read is kept and
    the largest holder's oldest is evicted. Base: refused `store_full`."""
    store = live_result_sets.live_store()
    held = []
    for i in range(live_result_sets.MAX_PER_SERVER):
        reader = "busy-reader" if i < 16 else f"reader-{i}"
        draft = store.mint(
            reader_key=reader, binding={}, members=(), header_claim=None, multi_user=False,
            payload=object(), payload_bytes=1, payload_rows=1, quota_key=reader,
        )
        held.append((reader, store.commit(draft).id))
    lab.example.set_sites(30)
    body = lab.ok("filter", {"where": {"type": "site"}, "limit": 5})
    assert body["truncated"] is True
    _handle(body)
    reader, oldest = held[0]
    with pytest.raises(live_result_sets._rr.ResultReferenceInvalid) as caught:
        store.lookup(oldest, reader_key=reader)
    assert caught.value.data["reason"] == "evicted"
    for reader, set_id in held[16:]:
        assert store.lookup(set_id, reader_key=reader).id == set_id


# ============================================================ 7, 18: decode bound


def test_decoded_rows_are_bounded_per_call_and_per_process(lab, monkeypatch) -> None:
    """MF 7: one call opening two handles past the per-call budget, and a
    read while the process budget is held, are refused at once by name with
    zero adapter calls. Base: no bound."""
    lab.example.set_sites(30)
    a = _handle(lab.ok("filter", SITES))
    b = _handle(lab.ok("filter", {"input": a, "where": {"attrs.kind": "COLD"}}))
    values = {"attrs.kind": {"op": "in", "values": {"ref": b, "field": "attrs.kind"}}}
    # Positive control: the value list reads B and narrows A to its kinds.
    joined = lab.ok("filter", {"input": a, "where": values})
    assert {unfence(item["attrs"]["kind"]) for item in joined["items"]} == {"COLD"}
    assert len(joined["items"]) == 6
    handles = _handles()
    store = live_result_sets.live_store()
    a_bytes = len(store._entries[a].payload.rows_json)  # noqa: SLF001
    calls = lab.example.total()
    monkeypatch.setattr(handles.decode_budget, "per_call_bytes", a_bytes + 10)
    error = lab.refused("filter", {"input": a, "where": values})
    assert error["code"] == "live_decode_capacity" and error["data"]["scope"] == "call"
    monkeypatch.setattr(handles.decode_budget, "per_call_bytes", handles.PER_CALL_BYTES)
    tokens = _fill_process_budget(handles)
    try:
        error = lab.refused("filter", {"input": a})
        assert error["code"] == "live_decode_capacity" and error["data"]["scope"] == "server"
    finally:
        for token in tokens:
            handles.decode_budget.release(token)
    assert lab.example.total() == calls
    assert handles.decode_budget.in_flight() == 0, "every read call gave its rows back"


def test_a_caller_who_lost_expand_hears_the_caller_refusal_not_capacity(lab) -> None:
    """MF 18 (N10): the decode budget full and expand revoked: the refusal
    is #4002's. Base: no decode budget (the module does not exist)."""
    lab.example.set_sites(30)
    a = _handle(lab.ok("filter", SITES))
    handles = _handles()
    tokens = _fill_process_budget(handles)
    try:
        with _gate(lab, _deny_expand_only):
            error = lab.refused("filter", {"input": a})
        assert error["code"] == "nomos_identity_denied", error
        error = lab.refused("filter", {"input": a})
        assert error["code"] == "live_decode_capacity"
    finally:
        for token in tokens:
            handles.decode_budget.release(token)


# ============================================================ 8: dead handles


def test_an_expired_handle_names_the_call_that_rebuilds_it(lab, clock) -> None:
    """MF 8 (lead decision Q2: one call; a chain names the read it began
    with): filter, then filter over its handle; both expire. `authority` is
    never echoed; another reader's expired id gets no call. Base: no hint."""
    authority = {"note": "mf8"}
    lab.example.set_sites(30)
    a = _handle(lab.ok("filter", {**SITES, "authority": authority}))
    b = _handle(lab.ok("filter", {"input": a, "where": {"attrs.kind": "COLD"},
                                  "authority": authority}))
    clock.now += 601
    calls = lab.example.total()
    error = lab.refused("filter", {"input": b, "authority": authority})
    assert error["data"]["reason"] == "expired"
    assert error["fix_hint"].startswith("run the read that made the handle again: it began with filter(")
    assert "then 1 more call on its handle" in error["fix_hint"]
    assert error["data"]["retry_call"]["tool"] == "filter"
    assert 'input={"kind":"nodes","subtype":"site"}' in error["fix_hint"]
    assert "authority" not in error["fix_hint"] and "mf8" not in error["fix_hint"]
    one = lab.refused("filter", {"input": a, "authority": authority})
    assert one["fix_hint"].startswith("retry: filter(") and "1 upstream call last time" in one["fix_hint"]
    other = lab.refused("filter", {"input": b, "authority": {"note": "someone-else"}})
    assert other["data"]["reason"] == "expired" and "retry_call" not in other["data"]
    assert "filter(" not in other["fix_hint"]
    assert lab.example.total() == calls


# ============================================================ 9, Q3: where handles go


def test_a_handle_in_a_compound_condition_is_refused_with_zero_work(lab) -> None:
    """MF 9 through the tools: no upstream call is made."""
    lab.example.set_sites(5)
    a = _handle(lab.ok("filter", SITES))
    calls = lab.example.total()
    error = lab.refused("filter", {"where": {"and": [{"type": "site"}, a]}})
    assert error["code"] == "invalid_operator_arguments"
    assert error["data"]["reason"] == "handle_as_condition"
    assert lab.example.total() == calls


def test_a_value_list_on_a_first_live_read_is_refused_with_zero_calls(lab) -> None:
    """Lead decision Q3: refused by name before any upstream call."""
    lab.example.set_sites(5)
    a = _handle(lab.ok("filter", SITES))
    calls = lab.example.total()
    error = lab.refused("filter", {"where": {"type": "site", "attrs.kind": {
        "op": "in", "values": {"ref": a, "field": "attrs.kind"}}}})
    assert error["data"]["reason"] == "values_from_handle_need_handle_input"
    assert lab.example.total() == calls


# ============================================================ 11, 12: the reply


def _next_lines(text: str) -> list[str]:
    return [part for line in text.splitlines() for part in line.split(" · ")
            if part.startswith("next: ")]


def test_one_handle_line_and_at_most_one_next_line(lab) -> None:
    """MF 11: a cut live filter page, a cut ranked live aggregate page and a
    cut kept map page each print the handle line and one `next:` in part 2's
    form; a kept map page's `next:` is a filter call; an uncut page prints
    none. Base: no handle line; a kept map page printed `offset=N`."""
    lab.example.set_sites(30)
    text = _text(lab, "filter", {"where": {"type": "site"}, "limit": 5})
    (line,) = [line for line in text.splitlines() if line.startswith("handle l1.")]
    handle = line.split(" ")[1]
    assert line == f"handle {handle} · 30 rows · 10 min"
    assert _next_lines(text) == [f'next: filter(input={{"ref":"{handle}"}}, limit=5, offset=5)']
    ranked = _text(lab, "aggregate", {"input": handle, "by": ["attrs.kind"],
                                       "metrics": ["count"], "order_by": "count desc",
                                       "limit": 1})
    (group_line,) = [line for line in ranked.splitlines() if line.startswith("handle l1.")]
    assert f"(groups) · from {handle} · 2 groups · 10 min" in group_line
    assert _next_lines(ranked) == [
        f'next: aggregate(input={{"ref": "{handle}"}}, by=["attrs.kind"], '
        'metrics=["count"], order_by="count desc", limit=1, offset=1)'
    ]
    mapped = _text(lab, "map", {"input": handle, "projection": {"id": "node_id"}, "limit": 5})
    assert _next_lines(mapped) == [
        f'next: map(input={{"ref":"{handle}"}}, projection={{"id":"node_id"}}, limit=5, offset=5)'
    ]
    listed = _text(lab, "map", {"input": handle, "select": ["node_id"], "limit": 5})
    assert _next_lines(listed) == [
        f'next: filter(input={{"ref":"{handle}"}}, select=["node_id"], limit=5, offset=5)'
    ]
    whole = _text(lab, "filter", {"input": handle, "limit": 50})
    assert _next_lines(whole) == [] and f"handle {handle} · 30 rows" in whole


def test_filter_over_a_groups_handle_orders_by_a_metric_label(lab) -> None:
    """MF 12: by fields, metric labels and `rank` in where, select and
    order_by. Base: no groups handle."""
    lab.example.set_sites(30)
    a = _handle(lab.ok("filter", SITES))
    grouped = lab.ok("aggregate", {"input": a, "by": ["attrs.kind"],
                                   "metrics": ["count", "sum(attrs.availability)"],
                                   "order_by": "count desc"})
    groups = _handle(grouped)
    assert grouped["handle"]["columns"] == [
        "attrs.kind", "count", "sum(attrs.availability)", "rank",
    ]
    calls = lab.example.total()
    ordered = lab.ok("filter", {"input": groups, "order_by": "count asc",
                                "select": ["attrs.kind", "count", "rank"]})
    rows = [(unfence(i["attrs.kind"]), i["count"], i["rank"]) for i in ordered["items"]]
    assert rows == [("COLD", 6, 2), ("HOT", 24, 1)]
    big = lab.ok("filter", {"input": groups, "where": {"sum(attrs.availability)":
                                                       {"op": "gt", "value": 100}}})
    assert [unfence(i["attrs.kind"]) for i in big["items"]] == ["HOT"]
    assert lab.example.total() == calls
    unknown = lab.refused("filter", {"input": groups, "where": {"attrs.tier": "2"}})
    assert unknown["data"]["reason"] == "groups_column_unknown"


# ============================================================ 16, Q1: sessions


def test_another_session_of_the_same_reader_cannot_read_a_raw_handle(world) -> None:  # noqa: F811
    """MF 16 (N5): `unknown`, zero adapter calls, and the owner's handle
    survives. Base: raw answers have no session check."""
    current = {"session": "s1"}
    world["mcp"]._okg_live_session = lambda context=None: (current["session"], False)
    world["adapter"].rows = [{"name": f"r{i}"} for i in range(3)]
    body = _ok(world["mcp"], "expand", {
        "input": {"node_id": world["hub"]}, "direction": "in",
        "live": {"relation": "lrs_rows", "args": {"q": "x"}},
    })
    set_id = body["external_live"]["result_reference"]["id"]
    calls = len(world["adapter"].calls)
    current["session"] = "s2"
    error = _refused(world["mcp"], "filter", {"input": {"ref": set_id}})
    assert error["data"]["reason"] == "unknown"
    assert len(world["adapter"].calls) == calls
    current["session"] = "s1"
    assert len(_ok(world["mcp"], "filter", {"input": {"ref": set_id}})["items"]) == 3


def test_another_session_hears_unknown_for_a_typed_handle_live_and_expired(lab, clock) -> None:
    """Lead decision Q1: no `binding_mismatch`, no discard, no sample row; a
    different reader answers as today."""
    lab.example.set_sites(5)
    with _session("s1"):
        a = _handle(lab.ok("filter", SITES))
    with _session("s2"):
        assert lab.refused("filter", {"input": a})["data"]["reason"] == "unknown"
    assert lab.refused("filter", {"input": a, "authority": {"note": "other"}})["data"]["reason"] == (
        "unknown"
    )
    with _session("s1"):
        assert len(lab.ok("filter", {"input": a})["items"]) == 5
    clock.now += 601
    with _session("s2"):
        assert lab.refused("filter", {"input": a})["data"]["reason"] == "unknown"
    with _session("s1"):
        assert lab.refused("filter", {"input": a})["data"]["reason"] == "expired"


# ============================================================ 17: the per-handle cap


def test_a_cut_reply_over_eight_mib_is_still_refused_kept_set_too_large(lab) -> None:
    """17 (regression of `req.read-live-handles.keep`): a cut read over the
    per-handle cap is refused; an uncut one is served with no handle for the
    set and says why."""
    lab.example.set_sites(999)
    for row in lab.example.sites:
        row["note"] = "n" * 9000
    error = lab.refused("filter", {"where": {"type": "site"}, "limit": 5})
    assert error["data"]["reason"] == "kept_set_too_large"
    body = lab.ok("aggregate", {"input": {"subtype": "site"}, "by": ["attrs.kind"],
                                "metrics": ["count"]})
    assert body["kept"] == {"kept": False, "reason": "too_large"}
    assert body["handle"]["from"] is None
    assert {unfence(g["attrs.kind"]): g["count"] for g in body["groups"]} == {
        "COLD": 200, "HOT": 799,
    }


# ============================================================ code review fixes


def test_value_list_rows_whose_field_was_not_read_are_counted_not_dropped(lab) -> None:
    """Review F4: one usage call fails; the value list from that handle says
    one of its 25 rows was not read, instead of dropping it silently."""
    lab.example.set_sites(25)
    lab.example.fail = {f"Z2_XX_S{24:04d}"}
    a = _handle(lab.ok("filter", SITES))
    grown = _handle(lab.ok("map", {"input": a, "select": ["node_id", "attrs.static_total"]}))
    body = lab.ok("filter", {"input": grown, "where": {"attrs.static_total": {
        "op": "in", "values": {"ref": grown, "field": "attrs.static_total"}}}})
    (note,) = body["value_lists_not_read"]
    assert note["ref"] == grown and note["not_read"] == 1 and note["rows"] == 25
    assert "1 of 25 rows" in note["note"]


def test_growth_decodes_its_handle_inside_the_decode_budget(lab) -> None:
    """Review F5: with the process budget held, a growth is refused at once
    by name, before any upstream call; released, it runs."""
    lab.example.set_sites(25)
    a = _handle(lab.ok("filter", SITES))
    handles = _handles()
    usage = "example_get_site_usage_by_source"
    tokens = _fill_process_budget(handles)
    try:
        error = lab.refused("map", {"input": a, "select": ["node_id", "attrs.static_total"]})
        assert error["code"] == "live_decode_capacity" and error["data"]["scope"] == "server"
        assert lab.example.made(usage) == 0
    finally:
        for token in tokens:
            handles.decode_budget.release(token)
    lab.ok("map", {"input": a, "select": ["node_id", "attrs.static_total"]})
    assert lab.example.made(usage) == 25 and handles.decode_budget.in_flight() == 0


def test_a_value_list_read_meets_the_caller_recheck(lab, monkeypatch) -> None:
    """Review F7: the value list's own handle asks #4002's caller check;
    a refusal there refuses the call with zero upstream calls."""
    from okg.substrate.mcp import live_resolver

    lab.example.set_sites(30)
    a = _handle(lab.ok("filter", SITES))
    b = _handle(lab.ok("filter", {"input": a, "where": {"attrs.kind": "COLD"}}))
    asked: list[str] = []
    real = live_resolver.check_caller_sync

    def second_denied(server: Any, name: str, subtype: str) -> None:
        asked.append(subtype)
        if len(asked) == 2:  # the value list's handle, read after the input
            _deny_expand_only("expand")
        real(server, name, subtype)

    monkeypatch.setattr(live_resolver, "check_caller_sync", second_denied)
    calls = lab.example.total()
    error = lab.refused("filter", {"input": a, "where": {"attrs.kind": {
        "op": "in", "values": {"ref": b, "field": "attrs.kind"}}}})
    assert error["code"] == "nomos_identity_denied" and len(asked) == 2
    assert error["data"].get("argument") == "where.values"
    assert lab.example.total() == calls


def test_a_groups_handle_read_meets_the_caller_recheck(lab) -> None:
    """Review F7: a groups handle is re-checked as the set it grouped."""
    lab.example.set_sites(30)
    groups = _handle(lab.ok("aggregate", {"input": _handle(lab.ok("filter", SITES)),
                                          "by": ["attrs.kind"], "metrics": ["count"]}))
    calls = lab.example.total()
    with _gate(lab, _deny_expand_only):
        error = lab.refused("filter", {"input": groups})
    assert error["code"] == "nomos_identity_denied"
    assert lab.example.total() == calls


def test_a_derived_handle_reports_the_original_call(lab) -> None:
    """Review F9: a narrowed handle's `original_call` is the call that made
    the set, and the reply says what it was derived from."""
    lab.example.set_sites(30)
    a = _handle(lab.ok("filter", SITES))
    b = _handle(lab.ok("filter", {"input": a, "where": {"attrs.kind": "COLD"}}))
    page = lab.ok("filter", {"input": b})
    assert page["original_call"]["rows"] == 30 and page["derived_from"] == a
    assert len(page["items"]) == 6


def test_a_value_list_from_another_sessions_handle_names_the_argument(lab) -> None:
    """Review F12: the refusal says the value list's handle failed."""
    lab.example.set_sites(5)
    with _session("s1"):
        a = _handle(lab.ok("filter", SITES))
    with _session("s2"):
        b = _handle(lab.ok("filter", SITES))
    with _session("s1"):
        error = lab.refused("filter", {"input": a, "where": {"attrs.kind": {
            "op": "in", "values": {"ref": b, "field": "attrs.kind"}}}})
    assert error["data"]["reason"] == "unknown" and error["data"]["argument"] == "where.values"


def test_a_growth_caller_who_lost_expand_hears_the_caller_refusal_not_capacity(lab) -> None:
    """Review R3-2: on the growth path too the caller check comes first."""
    lab.example.set_sites(25)
    a = _handle(lab.ok("filter", SITES))
    handles = _handles()
    tokens = _fill_process_budget(handles)
    try:
        with _gate(lab, _deny_expand_only):
            error = lab.refused("map", {"input": a, "select": ["node_id", "attrs.static_total"]})
        assert error["code"] == "nomos_identity_denied", error
    finally:
        for token in tokens:
            handles.decode_budget.release(token)
    assert lab.example.made("example_get_site_usage_by_source") == 0


def test_a_value_list_page_with_unread_rows_is_partial_in_text(lab) -> None:
    """Review R3-3: the page is not `complete`; the note is one sentence."""
    lab.example.set_sites(25)
    lab.example.fail = {f"Z2_XX_S{24:04d}"}
    a = _handle(lab.ok("filter", SITES))
    grown = _handle(lab.ok("map", {"input": a, "select": ["node_id", "attrs.static_total"]}))
    call = {"input": grown, "where": {"attrs.static_total": {
        "op": "in", "values": {"ref": grown, "field": "attrs.static_total"}}}}
    assert lab.ok("filter", call)["value_lists_not_read"][0]["not_read"] == 1
    text = _text(lab, "filter", call)
    head = text.splitlines()[0]
    assert "partial (value list rows not read)" in head and "complete" not in head
    (note,) = [line for line in text.splitlines() if line.startswith("! partial: ")]
    assert "1 of 25 rows of the value list's handle were not read" in note
    assert "{" not in note
