"""A cut live answer goes on by ONE `next` token (#3666, Live build 3).

When `expand(live=...)` cuts its answer to fit the reply (rows left out, or
fields or values shortened), the server keeps the whole answer and the reply
names one call, `expand(next="<token>")`. The page it serves comes from the
kept rows, each row with all its fields, and asks the live system nothing.
Fields are cut before rows, so after a field cut the token starts again at
row 1; after a row cut, at the first row not shown.

Hermetic: the gate, the adapter (counting its calls), the sampler and the
store are fakes or fresh instances; the reply budget is measured on the
served text, as the MCP server measures it. No database, no network. The
end-to-end path through the registered tools is in
`test_live_result_sets.py` (Postgres-marked).

New names are read with `getattr` inside each test, so on a build without
them each test fails on its own, not the whole file on import.
"""
from __future__ import annotations

import re
import time
from typing import Any

import pytest

from okg.substrate.external_backed.live_dispatch import (
    GateOutcome,
    dispatch_live_relation,
    live_reserve_envelope,
)
from okg.substrate.external_backed.live_relations import (
    ANNOTATION_KEY,
    bind_live_relations,
    compile_args_model,
    parse_live_bindings,
    parse_live_relations,
)
from okg.substrate.mcp import live_result_sets, result_references
from okg.substrate.mcp.response_bounds import (
    ResponseRoom,
    public_call_tool_result_bytes,
    served_text,
)
from okg.substrate.mcp.result_text import FENCE, result_format_scope

NODE = "dataset:/A/B/C"
RELATION = "list_items"
GRAPH = {"node_id": NODE, "direction": "out", "neighbors": []}
ROW_LINE = re.compile(r"^\[(\d+)\] ")
#: Every `tool(` a reply line names.
CALL = re.compile(r"\b([a-z_]+)\(")
READER = "reader-a"
BINDING = {
    "deployment": "default", "database": "db", "branch": "main",
    "catalog_version": 1, "authority": "a" * 64, "policy": None,
}


def _lrs(name: str) -> Any:
    value = getattr(live_result_sets, name, None)
    assert value is not None, f"live_result_sets.{name} is missing"
    return value


def _bound():
    declared = parse_live_relations(
        {ANNOTATION_KEY: {RELATION: {
            "action": "external.items.list",
            "sample_policy": "digest",
            "retention_class": "warm",
            "deadline_seconds": 5,
            "args": {"pattern": {"type": "string", "required": True}},
        }}},
        subtype="dataset",
    )
    bindings = parse_live_bindings(
        {RELATION: {"kind": "http", "config": {"url": "http://localhost"}}},
    )
    return bind_live_relations(declared, bindings)[RELATION]


def _allow(*_a: Any) -> GateOutcome:
    return GateOutcome(
        allowed=True, reason="allow", principal="service:okg-runtime",
        action="external.items.list",
    )


class CountingAdapter:
    def __init__(self, rows: Any):
        self.rows = rows
        self.calls = 0

    def __call__(self, **_kwargs: Any) -> Any:
        self.calls += 1
        return self.rows


def _served_bytes(envelope: dict[str, Any]) -> int:
    return public_call_tool_result_bytes({**GRAPH, "external_live": envelope}, tool="expand")


def _text(envelope: dict[str, Any]) -> str:
    return served_text({**GRAPH, "external_live": envelope}, tool="expand", result_format="text")


def _page_text(envelope: dict[str, Any]) -> str:
    payload = {"status": "ok", "operator_name": "expand", "result_kind": "live_page",
               "node_id": NODE, "external_live": envelope}
    return served_text(payload, tool="expand", result_format="text")


def _page_bytes(envelope: dict[str, Any]) -> int:
    payload = {"status": "ok", "operator_name": "expand", "result_kind": "live_page",
               "node_id": NODE, "external_live": envelope}
    return public_call_tool_result_bytes(payload, tool="expand")


def _row_numbers(text: str) -> list[int]:
    return [int(m.group(1)) for line in text.splitlines() if (m := ROW_LINE.match(line))]


def _bang(text: str) -> list[str]:
    return [line for line in text.splitlines() if line.startswith("! ")]


def _narrow_rows(count: int) -> list[dict[str, Any]]:
    """One identifying field per row: a cut leaves rows out, never fields."""
    return [{"name": f"/A/B/C{i:05d}-" + "x" * 40} for i in range(count)]


def _wide_rows(count: int) -> list[dict[str, Any]]:
    return [
        {
            "updated_at": "2026-01-02T03:04:05",
            "note": "n" * 30,
            "bytes": 1000 + i,
            "name": f"/A/B/C{i:04d}" + "x" * 60,
            "replicas": ["SITE_X", "SITE_Y", "SITE_Z"],
            "meta": {"a": 1, "b": 2},
            "scope": "group",
            "rse": "SITE_X",
            "did_type": "DATASET",
        }
        for i in range(count)
    ]


@pytest.fixture
def store() -> Any:
    fresh = live_result_sets.new_store()
    previous = live_result_sets.install_live_store(fresh)
    try:
        yield fresh
    finally:
        live_result_sets.install_live_store(previous)


def _keep_hook(store: Any, bound: Any):
    def keep(raw: Any, base: Any) -> Any:
        return live_result_sets.begin_retain(
            raw, base=base, store=store, reader_key=READER, header_claim=None,
            multi_user=False, binding=BINDING, relation=bound.relation,
            node_id=NODE, subject="service:okg-runtime",
            deployment_name="default", session_id="session-1",
        )
    return keep


def _cut(rows: Any, *, budget: int, store: Any, adapter: CountingAdapter | None = None,
         fmt: str = "text") -> tuple[dict[str, Any], CountingAdapter]:
    """One live call inside a read call's draft scope, finished as the
    wrapper finishes it: a kept set is committed only when the reply
    carries its id (here, inside the `next` token)."""
    bound = _bound()
    adapter = adapter or CountingAdapter(rows)
    with result_format_scope(fmt), result_references.draft_scope() as scope:
        envelope = dispatch_live_relation(
            bound=bound,
            args_model=compile_args_model(bound.relation),
            args={"pattern": "/A/B/*"},
            node_id=NODE,
            gate=_allow,
            adapter=adapter,
            write_sample=lambda **_k: object(),
            generation_id=7,
            room=ResponseRoom(budget_bytes=budget, served_bytes=_served_bytes),
            keep=_keep_hook(store, bound),
        )
        scope.finish({"external_live": envelope})
    return envelope, adapter


def _page(store: Any, token: str, *, budget: int) -> dict[str, Any]:
    """`expand(next=token)`'s page, built as the server builds it, from the
    kept set alone."""
    set_id, offset = _lrs("parse_next_token")(token)
    entry = store.lookup(set_id, reader_key=READER)
    rows = live_result_sets.decode_rows(entry.payload.rows_json)
    with result_format_scope("text"):
        return _lrs("page_envelope")(
            entry.payload, rows, offset=offset,
            fits=lambda envelope: _page_bytes(envelope) <= budget, set_id=set_id,
        )


def _unfenced(rows: list[Any]) -> list[dict[str, Any]]:
    def plain(value: Any) -> Any:
        if isinstance(value, str) and FENCE in value:
            return value.split("\n")[1]
        if isinstance(value, list):
            return [plain(v) for v in value]
        if isinstance(value, dict):
            return {plain(k): plain(v) for k, v in value.items()}
        return value
    return [plain(row) for row in rows]


# ---------------------------------------------------------------- the cut line


def test_a_row_cut_names_one_next_call_from_the_first_row_not_shown(store) -> None:
    budget = 16 * 1024
    envelope, adapter = _cut(_narrow_rows(3_000), budget=budget, store=store)

    assert envelope["truncated"] is True
    assert "text_shortened" not in envelope, "a one-field row cannot lose fields"
    kept = envelope["rows_returned"]
    token = envelope.get("next")
    assert isinstance(token, str), envelope.keys()
    assert envelope["next_from_row"] == kept + 1
    with result_format_scope("text"):
        assert _served_bytes(envelope) <= budget
    text = _text(envelope)
    assert _bang(text) == [
        f'! rows 1-{kept} of 3,000 shown; next: expand(next="{token}") '
        f"returns rows {kept + 1:,} onward with all fields"
    ], _bang(text)
    # ONE call, with one published argument; no rerun, no second read.
    assert [CALL.findall(line) for line in _bang(text)] == [["expand"]]
    assert "retain" not in text and "filter(" not in text and "offset=" not in text
    # The kept set is committed: the reply carried its id inside the token.
    assert store.count() == 1 and adapter.calls == 1


def test_a_field_cut_starts_again_at_row_one_with_all_fields(store) -> None:
    budget = 16 * 1024
    envelope, _ = _cut(_wide_rows(2_000), budget=budget, store=store)

    assert envelope["text_shortened"]["fields"] == 0
    assert envelope["next_from_row"] == 1
    kept = envelope["rows_returned"]
    text = _text(envelope)
    assert _bang(text) == [
        f"! rows 1-{kept} of 2,000 shown, fields cut to fit; "
        f'next: expand(next="{envelope["next"]}") returns rows 1 onward with all fields'
    ], _bang(text)


def test_values_cut_on_every_row_say_so_and_go_on_from_row_one(store) -> None:
    rows = [{**row, "note": "n" * 400} for row in _wide_rows(300)]
    envelope, _ = _cut(rows, budget=96 * 1024, store=store)

    assert "truncated" not in envelope and len(envelope["rows"]) == 300
    assert "fields" not in envelope["text_shortened"]
    assert envelope["next_from_row"] == 1
    assert _bang(_text(envelope)) == [
        f'! 300 rows shown, values cut to fit; next: expand(next="{envelope["next"]}") '
        "returns rows 1 onward with values not cut"
    ]


def test_an_answer_that_fits_gets_no_token_and_keeps_nothing(store) -> None:
    envelope, _ = _cut(_narrow_rows(5), budget=64 * 1024, store=store)
    assert "next" not in envelope and "next_unavailable" not in envelope
    assert not _bang(_text(envelope))
    assert store.count() == 0 and store.pending_count() == 0


def test_an_answer_that_cannot_be_kept_says_why_and_names_no_call(store) -> None:
    # One object, not a list of rows: it is cut, but it cannot be kept.
    answer = {"name": "/A/B/C", **{f"field_{i:02d}": "v" * 400 for i in range(60)}}
    envelope, _ = _cut(answer, budget=6 * 1024, store=store)
    assert envelope["next_unavailable"] == "rows_not_a_list"
    assert "next" not in envelope
    with result_format_scope("text"):
        assert _served_bytes(envelope) <= 6 * 1024
    assert _bang(_text(envelope)) == [
        "! 1 row shown, values cut to fit; the answer could not be kept to page "
        "(rows_not_a_list): narrow the args"
    ]
    assert store.count() == 0


# ---------------------------------------------------------------- the next page


def test_the_next_page_holds_the_following_rows_and_asks_the_adapter_nothing(store) -> None:
    upstream = _narrow_rows(3_000)
    budget = 16 * 1024
    envelope, adapter = _cut(upstream, budget=budget, store=store)
    kept = envelope["rows_returned"]

    page = _page(store, envelope["next"], budget=budget)

    assert adapter.calls == 1, "the next page is read from the kept answer"
    assert page["offset"] == kept
    count = page["rows_returned"]
    assert count > 0
    assert _unfenced(page["rows"]) == upstream[kept: kept + count]
    assert "text_shortened" not in page
    assert page["next_from_row"] == kept + count + 1
    assert "pinned_generation_id" not in page
    text = _page_text(page)
    assert text.splitlines()[0] == (
        f"expand ok · live {RELATION} · rows {kept + 1}-{kept + count} of 3,000 "
        "· from the kept answer"
    ), text.splitlines()[0]
    assert _row_numbers(text) == list(range(kept + 1, kept + count + 1))
    assert _bang(text) == [
        f"! rows {kept + 1}-{kept + count} of 3,000 shown; "
        f'next: expand(next="{page["next"]}") returns rows {kept + count + 1:,} onward '
        "with all fields"
    ]
    with result_format_scope("text"):
        assert _page_bytes(page) <= budget


def test_following_next_reaches_every_row_once_and_ends(store) -> None:
    upstream = _narrow_rows(3_000)
    budget = 16 * 1024
    envelope, adapter = _cut(upstream, budget=budget, store=store)
    seen = _unfenced(envelope["rows"])
    token = envelope["next"]
    pages = 0
    while token is not None:
        page = _page(store, token, budget=budget)
        seen += _unfenced(page["rows"])
        token = page.get("next")
        pages += 1
        assert pages < 100
    assert seen == upstream
    assert adapter.calls == 1
    last = _page_text(page)
    assert not _bang(last), "the last page names no further call"


def test_fields_cut_on_the_first_reply_come_back_whole_on_the_next(store) -> None:
    upstream = _wide_rows(2_000)
    budget = 16 * 1024
    envelope, adapter = _cut(upstream, budget=budget, store=store)
    assert envelope["text_shortened"]["fields"] == 0

    page = _page(store, envelope["next"], budget=budget)

    assert adapter.calls == 1
    assert page["offset"] == 0 and page["rows_returned"] > 0
    assert "text_shortened" not in page
    count = page["rows_returned"]
    assert _unfenced(page["rows"]) == upstream[:count]
    text = _page_text(page)
    rows = [line for line in text.splitlines() if ROW_LINE.match(line)]
    assert len(rows) == count
    for line in rows:
        # Every field the first reply dropped is printed again.
        for label in ("rse ", "did_type ", "bytes ", "updated_at ", "note ", "replicas ", "meta "):
            assert label in line, line


def test_a_row_too_wide_for_any_page_is_printed_cut_and_the_page_goes_on(store) -> None:
    # 200 fields of 100 chars: one row alone is wider than the reply.
    upstream = [
        {"name": f"r{i}", **{f"f{j:03d}": "v" * 100 for j in range(200)}}
        for i in range(3)
    ]
    budget = 8 * 1024
    envelope, _ = _cut(upstream, budget=budget, store=store)
    assert envelope["next_from_row"] == 1
    page = _page(store, envelope["next"], budget=budget)
    assert page["rows_returned"] == 1 and "text_shortened" in page
    # The page does not stall on the wide row: the next one starts after it.
    assert page["offset"] == 0 and page["next_from_row"] == 2


# ---------------------------------------------------------------- nested values
#
# A replica row's `states` is an object ({site: state}); printed as "1 field"
# the state never reached the reader. Object and list values now print their
# entries, bounded, with the rest counted and reachable by `next`.


def _replica(i: int, sites: int = 1) -> dict[str, Any]:
    names = [f"T1_XX_SITE{j:03d}_Tape" for j in range(sites)]
    return {
        "scope": "group", "name": f"/A/B/file{i}.root", "bytes": 100 + i,
        "states": {site: "AVAILABLE" for site in names},
        "rses": {site: [f"proto://host{j}/path/file{i}.root"] for j, site in enumerate(names)},
        "pfns": {f"proto://host0/path/file{i}.root": {"rse": names[0], "type": "DISK"}},
    }


def test_nested_object_and_list_values_print_their_entries(store) -> None:
    envelope, _ = _cut([_replica(i) for i in range(3)], budget=64 * 1024, store=store)
    text = _text(envelope)
    row = [line for line in text.splitlines() if line.startswith("[1] ")][0]
    assert "states T1_XX_SITE000_Tape: AVAILABLE" in row, row
    assert "rses T1_XX_SITE000_Tape: (proto://host0/path/file0.root)" in row, row
    # A key that is not a plain name is quoted, as every producer key is.
    assert 'pfns "proto://host0/path/file0.root": (rse: T1_XX_SITE000_Tape, type: DISK)' in row, row
    assert not re.search(r" \d+ (field|item)s?\b", text), text
    assert "next" not in envelope, "nothing was cut"


def test_nested_lists_print_their_items() -> None:
    rows = [{"logical_file_name": "/a/f.root", "lumi_section_num": [1, 2, 3],
             "event_count": [10, 20, 30]}]
    with result_format_scope("text"):
        envelope = dispatch_live_relation(
            bound=_bound(), args_model=compile_args_model(_bound().relation),
            args={"pattern": "/A/B/*"}, node_id=NODE, gate=_allow,
            adapter=lambda **_k: rows, write_sample=lambda **_k: object(),
            generation_id=7, room=None,
        )
    text = _text(envelope)
    assert "lumi_section_num: 1, 2, 3" in text and "event_count: 10, 20, 30" in text, text


def test_a_long_nested_value_counts_the_rest_and_next_prints_it_whole(store) -> None:
    upstream = [_replica(i, sites=120) for i in range(3)]
    budget = 64 * 1024
    envelope, adapter = _cut(upstream, budget=budget, store=store)
    text = _text(envelope)
    first = [line for line in text.splitlines() if line.startswith("[1] ")][0]
    assert re.search(r"states T1_XX_SITE000_Tape: AVAILABLE, .* \+\d+ more", first), first
    assert envelope.get("nested_cut") is True and envelope["next_from_row"] == 1
    assert _bang(text) == [
        f'! 3 rows shown, nested values cut; next: expand(next="{envelope["next"]}") '
        "returns rows 1 onward with nested values whole"
    ], _bang(text)

    page = _page(store, envelope["next"], budget=budget)
    assert adapter.calls == 1
    whole = _page_text(page)
    row = [line for line in whole.splitlines() if line.startswith("[1] ")][0]
    assert " more" not in row
    assert row.count(": AVAILABLE") == 120, row.count(": AVAILABLE")


# ---------------------------------------------------------------- the token


def test_a_tampered_or_malformed_token_is_refused_by_name(store) -> None:
    from okg.substrate.mcp.continuation import ContinuationTokenInvalid

    envelope, _ = _cut(_narrow_rows(3_000), budget=16 * 1024, store=store)
    token = envelope["next"]
    parts = token.split(".")
    parts[5] = str(int(parts[5]) + 1)
    with pytest.raises(ContinuationTokenInvalid) as forged:
        _lrs("parse_next_token")(".".join(parts))
    assert forged.value.data["reason"] == "signature_invalid"
    with pytest.raises(ContinuationTokenInvalid) as garbage:
        _lrs("parse_next_token")("l1.nonsense")
    assert garbage.value.data["reason"] == "malformed"
    assert _lrs("is_next_token")(token) and not _lrs("is_next_token")("eJx.abc")


def test_an_expired_or_unknown_kept_answer_is_said_plainly_with_its_redo_call() -> None:
    clock = [time.time()]
    store = live_result_sets.new_store(clock=lambda: clock[0])
    previous = live_result_sets.install_live_store(store)
    try:
        envelope, _ = _cut(_narrow_rows(3_000), budget=16 * 1024, store=store)
    finally:
        live_result_sets.install_live_store(previous)
    set_id, _offset = _lrs("parse_next_token")(envelope["next"])
    _lrs("remember_call")(set_id, READER, _lrs("redo_call")(
        NODE, RELATION, {"pattern": "/A/B/*"},
    ))
    clock[0] += live_result_sets.LIFETIME_SECONDS + 1
    with pytest.raises(result_references.ResultReferenceInvalid) as gone:
        store.lookup(set_id, reader_key=READER)
    reason = gone.value.data["reason"]
    assert reason == "expired"
    message = _lrs("NEXT_GONE_MESSAGES")[reason]
    assert "expired" in message and "600 seconds" in message  # re-pinned: PACT read-live-handles, 10 min
    call = _lrs("remembered_call")(set_id, READER)
    from okg.substrate.mcp.result_text import call_text

    assert call_text(call) == (
        f'expand(input="{NODE}", live={{"relation":"{RELATION}",'
        '"args":{"pattern":"/A/B/*"}})'
    )
    # Another reader is told nothing about the call.
    assert _lrs("remembered_call")(set_id, "reader-b") is None
    assert "unknown" in _lrs("NEXT_GONE_MESSAGES")
    assert "restarted" in _lrs("NEXT_GONE_MESSAGES")["store_lost"]


def test_the_reply_commits_the_kept_set_only_when_it_carries_the_token(store) -> None:
    bound = _bound()
    with result_references.draft_scope() as scope:
        handle = _keep_hook(store, bound)(_narrow_rows(10), {"query": {}})
        token = _lrs("next_token")(handle.draft.id, 4)
        scope.finish({"external_live": {"next": token}})
    assert store.count() == 1
    with result_references.draft_scope() as scope:
        _keep_hook(store, bound)(_narrow_rows(10), {"query": {}})
        scope.finish({"external_live": {"rows": []}})
    assert store.count() == 1 and store.pending_count() == 0


@pytest.mark.parametrize("fmt", ["text", "json"])
def test_the_reserve_holds_the_longest_next_line(store, fmt) -> None:
    """The graph step leaves room for the reserve; a rowless answer with
    the longest token any call can carry must fit in it."""
    bound = _bound()
    reserve = live_reserve_envelope(
        bound=bound, args_model=compile_args_model(bound.relation),
        args={"pattern": "/A/B/*"}, generation_id=7,
    )

    class NoRowsFit:
        budget_bytes = 10**9

        def fits(self, envelope: dict[str, Any]) -> bool:
            return "rows" not in envelope

    with result_format_scope(fmt), result_references.draft_scope() as scope:
        digest = dispatch_live_relation(
            bound=bound, args_model=compile_args_model(bound.relation),
            args={"pattern": "/A/B/*"}, node_id=NODE, gate=_allow,
            adapter=lambda **_k: _narrow_rows(5_000),
            write_sample=lambda **_k: object(), generation_id=7, room=NoRowsFit(),
            keep=_keep_hook(store, bound),
        )
        scope.finish({"external_live": digest})
        assert digest["policy"] == "digest" and digest["next_from_row"] == 1
        longest = {
            **digest, "next": _lrs("largest_next_token")(),
            "next_from_row": live_result_sets.MAX_ROWS,
            "summary": {**digest["summary"], "row_count": live_result_sets.MAX_ROWS},
        }
        assert _served_bytes(reserve) >= _served_bytes(longest) >= _served_bytes(digest)
