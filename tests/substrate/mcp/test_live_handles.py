"""Live handles without a database (PACT read-live-handles, PR 3a-i).

The store's caps per reader and session (the quota key), fair eviction when
the server is full, the 10-minute life, the session answer, the decode
budget, the one string-input rule, and where a handle id may go in a call.
The reads through the tools, with the recording fake system, are in
`test_live_handles_db.py`.

New names are read with `getattr` inside each test, so on a build without
them each test fails on its own, not the whole file on import.
"""
from __future__ import annotations

import json
from typing import Any

import pytest

from okg.substrate.mcp import live_result_sets, result_references
from okg.substrate.mcp.result_references import ResultReferenceInvalid

READER = "reader-a"
SESSION_A = f"{READER}\x00session-a"
SESSION_B = f"{READER}\x00session-b"


def _handles() -> Any:
    from okg.substrate.mcp import handles

    return handles


def _mint(store: Any, quota: str, *, reader: str = READER, nbytes: int = 2) -> Any:
    kwargs: dict[str, Any] = {"quota_key": quota}
    return store.mint(
        reader_key=reader, binding={}, members=(), header_claim=None, multi_user=False,
        payload=b"[]", payload_bytes=nbytes, payload_rows=0, **kwargs,
    )


def _commit(store: Any, quota: str, *, reader: str = READER, nbytes: int = 2) -> str:
    draft = _mint(store, quota, reader=reader, nbytes=nbytes)
    return store.commit(draft).id


def _reason(store: Any, set_id: str, reader: str = READER, quota: Any = None) -> str:
    with pytest.raises(ResultReferenceInvalid) as caught:
        store.lookup(set_id, reader_key=reader, quota_key=quota)
    return caught.value.data["reason"]


class Clock:
    def __init__(self) -> None:
        self.now = 2_000_000_000.0

    def __call__(self) -> float:
        return self.now


# ============================================================ caps and eviction


def test_caps_count_per_reader_and_session() -> None:
    """MF 4: two sessions of one reader keep 64 each (re-pinned by PACT
    read-multi-handle-input, Jason board d275; was 16); session A's 65th
    evicts A's oldest, never B's. Base: 4 per reader, sessions not counted."""
    store = live_result_sets.new_store()
    a = [_commit(store, SESSION_A) for _ in range(64)]
    b = [_commit(store, SESSION_B) for _ in range(64)]
    assert store.quota_count(SESSION_A) == 64 and store.quota_count(SESSION_B) == 64
    extra = _commit(store, SESSION_A)
    assert _reason(store, a[0], quota=SESSION_A) == "evicted"
    for set_id in [*a[1:], extra]:
        assert store.lookup(set_id, reader_key=READER, quota_key=SESSION_A).id == set_id
    for set_id in b:
        assert store.lookup(set_id, reader_key=READER, quota_key=SESSION_B).id == set_id


def test_a_quota_key_holds_at_most_32_mib_and_its_own_oldest_goes_first() -> None:
    store = live_result_sets.new_store()
    eight = live_result_sets.MAX_SET_BYTES
    first, *rest = [_commit(store, SESSION_A, nbytes=eight) for _ in range(5)]
    assert _reason(store, first, quota=SESSION_A) == "evicted"
    assert store.quota_count(SESSION_A) == 4


def test_a_full_server_evicts_the_largest_holders_oldest() -> None:
    """MF 5 (store half): the server is full; the next mint evicts the
    oldest handle of the quota key holding the most, never refuses. Base:
    `server_references` refused."""
    store = live_result_sets.new_store()
    # Re-pinned by PACT read-multi-handle-input: 64 per session, 1,024 per server.
    big = [_commit(store, "reader-big", reader="reader-big") for _ in range(64)]
    small = []
    for i in range(960):
        small.append(_commit(store, f"reader-{i}", reader=f"reader-{i}"))
    assert store.count() == live_result_sets.MAX_PER_SERVER == 1024
    late = _commit(store, "reader-late", reader="reader-late")
    assert store.lookup(late, reader_key="reader-late").id == late
    assert _reason(store, big[0], reader="reader-big") == "evicted"
    for i, set_id in enumerate(small):
        assert store.lookup(set_id, reader_key=f"reader-{i}").id == set_id


def test_an_evicted_handle_answers_evicted_to_its_owner_and_unknown_to_others() -> None:
    """6 (regression): evicted to its owner; unknown to another reader, and
    (lead decision Q1) to another session of the same reader."""
    store = live_result_sets.new_store()
    ids = [_commit(store, SESSION_A) for _ in range(live_result_sets.MAX_PER_READER + 1)]
    assert _reason(store, ids[0], quota=SESSION_A) == "evicted"
    assert _reason(store, ids[0], reader="reader-b") == "unknown"
    assert _reason(store, ids[0], quota=SESSION_B) == "unknown"


def test_another_session_hears_unknown_for_live_evicted_and_expired_and_the_owner_keeps_it() -> None:
    clock = Clock()
    store = live_result_sets.new_store(clock=clock)
    live = _commit(store, SESSION_A)
    assert _reason(store, live, quota=SESSION_B) == "unknown"
    # The probe left the owner's handle as it was.
    assert store.lookup(live, reader_key=READER, quota_key=SESSION_A).id == live
    clock.now += live_result_sets.LIFETIME_SECONDS + 1
    assert _reason(store, live, quota=SESSION_B) == "unknown"
    assert _reason(store, live, quota=SESSION_A) == "expired"
    # Still after the purge a mint makes.
    _commit(store, "reader-c", reader="reader-c")
    assert _reason(store, live, quota=SESSION_B) == "unknown"
    assert _reason(store, live, quota=SESSION_A) == "expired"
    # A different reader keeps today's answer for an expired id.
    assert _reason(store, live, reader="reader-b", quota="reader-b") == "expired"


def test_a_cut_reply_is_never_refused_because_other_sessions_filled_the_server() -> None:
    store = live_result_sets.new_store()
    for i in range(live_result_sets.MAX_PER_SERVER):
        _commit(store, f"{READER}\x00s{i % 4}")
    draft = _mint(store, SESSION_B)
    assert store.commit(draft).id == draft.id


def test_only_reservations_and_ancestors_block_a_mint() -> None:
    store = live_result_sets.new_store()
    pending = [_mint(store, f"r{i}", reader=f"r{i}") for i in range(live_result_sets.MAX_PER_SERVER)]
    with pytest.raises(result_references.ResultReferenceCapacity):
        _mint(store, "late", reader="late")
    store.release(pending[0].id)
    _mint(store, "late", reader="late")


# ============================================================ life


def test_a_live_handle_lives_ten_minutes_from_its_own_mint() -> None:
    """MF 3 (store half): readable at 301 s, expired at 600 s; a handle
    minted later has its own 600 s. Base: 300 s."""
    clock = Clock()
    store = live_result_sets.new_store(clock=clock)
    first = _commit(store, SESSION_A)
    clock.now += 301
    assert store.lookup(first, reader_key=READER, quota_key=SESSION_A).id == first
    derived = _commit(store, SESSION_A)
    clock.now += 600 - 301
    assert _reason(store, first, quota=SESSION_A) == "expired"
    assert store.lookup(derived, reader_key=READER, quota_key=SESSION_A).id == derived


def test_a_dead_handle_says_handle() -> None:
    clock = Clock()
    store = live_result_sets.new_store(clock=clock)
    set_id = _commit(store, SESSION_A)
    clock.now += 601
    with pytest.raises(ResultReferenceInvalid) as caught:
        store.lookup(set_id, reader_key=READER)
    assert caught.value.message == "the handle has expired"


# ============================================================ the decode budget


def test_the_decode_budget_refuses_at_once_per_call_and_per_process() -> None:
    handles = _handles()
    budget = handles.DecodeBudget(per_call_bytes=100, per_process_bytes=150)
    held = budget.reserve(90)
    with pytest.raises(handles.LiveDecodeCapacity) as caught:
        budget.reserve(20, call_bytes=90)
    assert caught.value.data["scope"] == "call"
    with pytest.raises(handles.LiveDecodeCapacity) as caught:
        budget.reserve(70)
    assert caught.value.data["scope"] == "server"
    budget.release(held)
    budget.release(budget.reserve(100))
    assert budget.in_flight() == 0


def test_a_read_calls_close_callbacks_run_on_every_exit() -> None:
    ran: list[str] = []
    with pytest.raises(RuntimeError):
        with result_references.draft_scope():
            result_references.on_close(lambda: ran.append("closed"))
            raise RuntimeError("the read failed")
    assert ran == ["closed"]
    with pytest.raises(result_references.ResultReferenceInternal):
        result_references.on_close(lambda: None)


def test_quota_key_is_the_reader_alone_on_stateless_http() -> None:
    handles = _handles()
    assert handles.quota_key("r", "s", False) == "r\x00s"
    assert handles.quota_key("r", "s", True) == "r"
    assert handles.quota_key("r", None, False) == "r"


# ============================================================ string input (N7)

LIVE_ID = "l1.AAAAAAAA.2000000600.BBBBBBBBBBBBBBBB.CCCCCCCCCCCCCCCCCCCCCC"
GRAPH_ID = "r1.AAAAAAAA.2000000600.BBBBBBBBBBBBBBBB.CCCCCCCCCCCCCCCCCCCCCC"


@pytest.mark.parametrize("text", [
    LIVE_ID, f"  {LIVE_ID} ", json.dumps(LIVE_ID), json.dumps({"ref": LIVE_ID}), GRAPH_ID,
])
def test_a_json_encoded_handle_and_a_json_ref_object_are_read_as_the_handle(text) -> None:
    """MF 14 (shape half): every spelling is `("handle", id)`, the grammar
    rewrites it to `{"ref": id}`, and #3989's decoder gives the same."""
    from okg.substrate.mcp.live_resolver import decode_json_arguments
    from okg.substrate.mcp.read_grammar import rewrite_call

    expected = text.strip().strip('"') if not text.strip().startswith("{") else LIVE_ID
    shape = getattr(result_references, "string_input_shape")(text)
    assert shape == ("handle", expected)
    for tool in ("filter", "map", "aggregate"):
        decoded = decode_json_arguments(tool, {"input": text})
        assert decoded["input"] == {"ref": expected}, (tool, decoded)
        arguments: dict[str, Any] = {"input": text}
        if tool == "aggregate":
            arguments.update(by=["attrs.kind"], metrics=["count"])
        rewritten = rewrite_call(tool, arguments).arguments
        assert rewritten["input"] == {"ref": expected}, (tool, rewritten)


@pytest.mark.parametrize("text,shape", [
    ("site:Z2_XX_S0001", "text"), ('{"kind": "nodes"}', "json"), ("[1, 2]", "json"),
    (json.dumps({"ref": LIVE_ID, "field": "x"}), "json"), ('"site:Z2"', "text"),
])
def test_other_strings_keep_their_shape(text, shape) -> None:
    assert result_references.string_input_shape(text)[0] == shape


def test_expand_never_reads_a_handle_string_as_a_reference() -> None:
    from okg.substrate.mcp.live_resolver import decode_json_arguments

    assert decode_json_arguments("expand", {"input": LIVE_ID}) == {"input": LIVE_ID}


def test_a_string_input_that_is_no_handle_is_refused_on_filter_and_map() -> None:
    from okg.substrate.mcp.read_grammar import GrammarRefusal, rewrite_call

    for tool in ("filter", "map"):
        with pytest.raises(GrammarRefusal) as caught:
            rewrite_call(tool, {"input": "site:Z2_XX_S0001"})
        assert "names a handle" in caught.value.message


# ============================================================ where a handle may go (3.3)


def _refusal(tool: str, arguments: dict[str, Any]) -> Any:
    from okg.substrate.mcp.read_grammar import GrammarRefusal, rewrite_call

    with pytest.raises(GrammarRefusal) as caught:
        rewrite_call(tool, arguments)
    return caught.value


@pytest.mark.parametrize("handle", [LIVE_ID, GRAPH_ID])
@pytest.mark.parametrize("where", [
    lambda h: {"and": [{"type": "doc"}, h]},
    lambda h: {"op": "and", "clauses": [{"type": "doc"}, {"ref": h}]},
    lambda h: {"or": [{"type": "doc"}, {"not": h}]},
])
def test_a_handle_in_a_compound_condition_is_refused_by_name(handle, where) -> None:
    """MF 9: stored and live ids alike; the reason is new."""
    for tool in ("filter", "map", "aggregate"):
        arguments: dict[str, Any] = {"where": where(handle)}
        if tool == "aggregate":
            arguments.update(by=["attrs.kind"], metrics=["count"])
        refusal = _refusal(tool, arguments)
        assert refusal.reason == "handle_as_condition", (tool, refusal.message)


def test_a_handle_as_a_scopes_filter_or_beside_an_input_is_refused() -> None:
    assert _refusal("filter", {"input": {"kind": "nodes", "filter": LIVE_ID}}).reason == (
        "handle_as_condition"
    )
    assert _refusal(
        "filter", {"input": {"ref": LIVE_ID}, "where": {"ref": LIVE_ID}},
    ).reason == "handle_as_condition"


@pytest.mark.parametrize("where", [{"ref": LIVE_ID}, LIVE_ID, {"ref": GRAPH_ID}])
def test_a_where_that_is_only_a_handle_is_read_as_input(where) -> None:
    """10 (N6): the aggregate form is today's rewrite (regression); filter,
    map and the bare string are new."""
    from okg.substrate.mcp.read_grammar import rewrite_call

    expected = where["ref"] if isinstance(where, dict) else where
    for tool in ("filter", "map", "aggregate"):
        arguments: dict[str, Any] = {"where": where}
        if tool == "aggregate":
            arguments.update(by=["attrs.kind"], metrics=["count"])
        rewritten = rewrite_call(tool, arguments)
        assert rewritten.arguments["input"] == {"ref": expected}, (tool, rewritten.arguments)
        assert any(
            note.get("rule") == "kept_answer_ref_as_input" for note in rewritten.adjusted
        ), rewritten.adjusted


def test_a_value_list_from_a_handle_is_read_only_over_a_live_handle() -> None:
    """Lead decision Q3: a first read and a stored condition refuse it by
    name; over a live handle it passes the grammar."""
    from okg.substrate.mcp.read_grammar import rewrite_call

    values = {"op": "in", "values": {"ref": LIVE_ID, "field": "attrs.kind"}}
    for arguments in (
        {"where": {"type": "site", "attrs.kind": values}},
        {"input": {"ref": GRAPH_ID}, "where": {"attrs.kind": values}},
    ):
        refusal = _refusal("filter", arguments)
        assert refusal.reason == "values_from_handle_need_handle_input", arguments
    passed = rewrite_call("filter", {"input": LIVE_ID, "where": {"attrs.kind": values}})
    assert passed.arguments["input"] == {"ref": LIVE_ID}


def test_a_groups_handles_columns_pass_the_grammar() -> None:
    from okg.substrate.mcp.read_grammar import rewrite_call

    rewritten = rewrite_call("filter", {
        "input": LIVE_ID, "where": {"count": {"op": "gt", "value": 2}},
        "order_by": "-sum(attrs.size)", "select": ["attrs.kind", "count", "rank"],
    })
    assert rewritten.arguments["predicate"]["field"] == "count"


# ============================================================ the reply


def test_the_handle_line_and_the_served_ids() -> None:
    from okg.substrate.mcp.result_text import _handle_line

    handles = _handles()
    block = handles.handle_block(
        set_id=LIVE_ID, rows=24, columns=["node_id"], order="input order",
        expires_at=2_000_000_600,
    )
    assert _handle_line({"operator_name": "filter", "handle": block}) == (
        f"handle {LIVE_ID} · 24 rows · 10 min"
    )
    groups = handles.handle_block(
        set_id=LIVE_ID, rows=11, columns=["count"], order="rank",
        expires_at=2_000_000_600, from_id=GRAPH_ID,
    )
    assert _handle_line({"operator_name": "aggregate", "groups": [], "handle": groups}) == (
        f"handle {LIVE_ID} (groups) · from {GRAPH_ID} · 11 groups · 10 min"
    )
    assert result_references._served_reference_ids({"handle": groups}) == {LIVE_ID, GRAPH_ID}
    many = handles.handle_block(
        set_id=LIVE_ID, rows=1, columns=[f"c{i}" for i in range(20)], order="input order",
        expires_at=2_000_000_600,
    )
    assert len(many["columns"]) == 12 and many["columns_more"] == 8


# ============================================================ code review fixes


def test_an_in_list_from_a_handle_is_hashed_20000_rows_by_20000_values() -> None:
    """Review F2: 4,000 x 4,000 took 5.9 s with a scan per row; the index is
    built once per call, so 20,000 x 20,000 is well under 2 s."""
    import time

    from okg.substrate.mcp import live_reader

    n = 20_000
    rows = [{"node_id": f"t:{i}", "subtype": "t", "attrs": {"x": f"v{i}"},
             "live": {"fields": {"x": "live"}}} for i in range(n)]
    misses = live_reader.HashedValues([f"w{i}" for i in range(n)])
    hits = live_reader.HashedValues([f"v{i}" for i in range(0, n, 2)])
    started = time.monotonic()
    none = live_reader.Matcher({"op": "in", "field": "attrs.x", "values": misses})
    assert not any(none.matches(row) for row in rows)
    half = live_reader.Matcher({"op": "in", "field": "attrs.x", "values": hits})
    assert sum(half.matches(row) for row in rows) == n // 2
    assert time.monotonic() - started < 2.0
    # The raw-row path tests membership with `in` on the list itself.
    assert "v4" in hits and "v5" not in hits


@pytest.mark.parametrize("value,values", [
    ("5", [5]), (5, ["5"]), ("true", [True]), (True, [1]), (1, [True]), (5.0, [5]),
    ("x", [None]), (None, [None, "a"]), ({"k": 1}, [{"k": 1}]), ("TAPE", ["TAPE", 2]),
    (3, ["a", "b"]), ("False", [False, "no"]), (2.5, ["2.5"]),
])
def test_the_hashed_in_list_answers_as_the_scan_did(value, values) -> None:
    """Review F2: the index gives the scan's answer and its not-comparable
    count for every kind the coercion rule names."""
    from okg.substrate.mcp import live_reader

    row = {"node_id": "t:1", "subtype": "t", "attrs": {"x": value},
           "live": {"fields": {"x": "live"}}}
    if value is None:
        row["attrs"] = {}
    matcher = live_reader.Matcher({"op": "in", "field": "attrs.x", "values": list(values)})
    got = matcher.matches(row)
    if value is None:
        expected, bad = any(v is None for v in values), False
    elif any(live_reader.coerce(value, v) is not None for v in values):
        expected = any(
            (pair := live_reader.coerce(value, v)) is not None and pair[0] == pair[1]
            for v in values
        )
        bad = False
    else:
        expected, bad = False, True
    assert got == expected and bool(matcher.not_comparable) == bad, (value, values)


def test_under_the_byte_cap_the_holder_of_the_most_bytes_is_evicted_first() -> None:
    """Review F3: four sessions hold 4 x 8 MiB each (the 128 MiB server
    cap); a session holding ten 1 KiB handles mints one more. It loses none;
    one 8 MiB handle goes. Base ranking by count: it lost six of its own."""
    clock = Clock()
    store = live_result_sets.new_store(clock=clock)
    mib = 1024 * 1024
    owner: dict[str, str] = {}
    for s in range(4):
        for _ in range(4):
            owner[_commit(store, f"{READER}\x00big{s}", nbytes=8 * mib - 1024)] = f"big{s}"
            clock.now += 1
    small = []
    for _ in range(10):
        small.append(_commit(store, SESSION_A, nbytes=1024))
        clock.now += 1
    _commit(store, SESSION_A, nbytes=64 * 1024)
    assert store.quota_count(SESSION_A) == 11
    gone = [i for i in owner if i not in store._entries]  # noqa: SLF001
    assert len(gone) == 1 and owner[gone[0]].startswith("big")
    for set_id in small:
        assert store.lookup(set_id, reader_key=READER, quota_key=SESSION_A).id == set_id


def test_one_call_decodes_at_most_half_the_process_budget() -> None:
    """Review F6: one call can never take the whole in-flight budget."""
    handles = _handles()
    assert handles.PER_CALL_BYTES * 2 <= handles.PER_PROCESS_BYTES
    assert handles.decode_budget.per_call_bytes == 16 * 1024 * 1024
