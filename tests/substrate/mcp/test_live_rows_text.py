"""expand(live=...) rows read like every other row (#3666, read-passage-rows).

A live relation's answer prints one line per row, `[n] key value · key
value`, identifying fields first; never a JSON dump of the envelope. When
the rows do not fit the reply budget, every row is kept and its values are
cut evenly; only when even the identifying fields of every row do not fit
is a prefix served, with the exact calls that read the rest.

Hermetic: the live gate, the adapter and the sample writer are fakes, and
the reply budget is measured on the served text, as the MCP server measures
it. No database, no network.
"""
from __future__ import annotations

import re
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
from okg.substrate.mcp.response_bounds import (
    ResponseRoom,
    public_call_tool_result_bytes,
    served_text,
)
from okg.substrate.mcp import result_text
from okg.substrate.mcp.result_text import (
    FENCE,
    FENCE_CLOSE,
    result_format_scope,
)

#: `result_text.LIVE_TEXT_SHORTENED_KEY`, spelled out so this file imports on
#: a build without it and fails on behaviour, not on the import.
LIVE_TEXT_SHORTENED_KEY = "text_shortened"
NODE = "dataset:/A/B/C"
RELATION = "list_items"
GRAPH = {"node_id": NODE, "direction": "out", "neighbors": []}
ROW_LINE = re.compile(r"^\[(\d+)\] ")


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


def _deny(*_a: Any) -> GateOutcome:
    return GateOutcome(
        allowed=False, reason="deny", principal="service:okg-runtime",
        action="external.items.list",
    )


def _served_bytes(envelope: dict[str, Any]) -> int:
    return public_call_tool_result_bytes({**GRAPH, "external_live": envelope}, tool="expand")


def _dispatch(rows: Any, *, budget: int | None, gate=_allow, fmt: str = "text") -> dict[str, Any]:
    bound = _bound()
    with result_format_scope(fmt):
        room = (
            None if budget is None
            else ResponseRoom(budget_bytes=budget, served_bytes=_served_bytes)
        )
        return dispatch_live_relation(
            bound=bound,
            args_model=compile_args_model(bound.relation),
            args={"pattern": "/A/B/*"},
            node_id=NODE,
            gate=gate,
            adapter=lambda **_k: rows,
            write_sample=lambda **_k: object(),
            generation_id=7,
            room=room,
        )


def _text(envelope: dict[str, Any]) -> str:
    return served_text({**GRAPH, "external_live": envelope}, tool="expand", result_format="text")


def _row_lines(text: str) -> list[str]:
    return [line for line in text.splitlines() if ROW_LINE.match(line)]


def _rows(count: int, *, note_chars: int = 0, name_pad: int = 0) -> list[dict[str, Any]]:
    return [
        {
            "updated_at": "2026-01-02T03:04:05",
            "note": ("n" * note_chars) or "ok",
            "bytes": 1000 + i,
            "name": f"/A/B/C{i:04d}" + "x" * name_pad,
            "replicas": ["SITE_X", "SITE_Y", "SITE_Z"],
            "meta": {"a": 1, "b": 2},
            "scope": "group",
            "rse": "SITE_X",
            "did_type": "DATASET",
        }
        for i in range(count)
    ]


def _assert_no_json_blob(text: str) -> None:
    assert "--- external_live" not in text
    assert '"rows"' not in text and '"schema"' not in text
    for line in _row_lines(text):
        assert "{" not in line and "[" not in line[line.index("]") + 1:], line


def test_the_text_fit_key_is_the_renderers() -> None:
    assert getattr(result_text, "LIVE_TEXT_SHORTENED_KEY", None) == LIVE_TEXT_SHORTENED_KEY


def test_a_list_relation_prints_one_named_line_per_row() -> None:
    envelope = _dispatch(_rows(300), budget=None)
    text = _text(envelope)

    status = text.splitlines()[0]
    assert status.startswith(f"expand ok · {NODE} out")
    assert status.endswith(f"live {RELATION} · 300 rows, all shown"), status
    lines = _row_lines(text)
    assert [int(ROW_LINE.match(line).group(1)) for line in lines] == list(range(1, 301))
    # Identifying fields first, then states and numbers, then the rest; a
    # nested value prints its entries (Live build 3), never a bare count.
    assert lines[0] == (
        "[1] scope:name group:/A/B/C0000 · rse SITE_X · did_type DATASET · "
        "bytes 1000 · updated_at 2026-01-02T03:04:05 · note ok · "
        "replicas SITE_X, SITE_Y, SITE_Z · meta a: 1, b: 2"
    ), lines[0]
    # One fence around the rows, no marker inside a row.
    assert text.count(f"{FENCE} source=external_live:{RELATION}") == 1
    assert text.count(FENCE_CLOSE) == 1
    _assert_no_json_blob(text)


def test_an_over_budget_list_keeps_every_row_and_cuts_values() -> None:
    budget = 64 * 1024
    rows = _rows(300, note_chars=400)
    envelope = _dispatch(rows, budget=budget)

    assert envelope["state"] == "ok"
    assert len(envelope["rows"]) == 300, "every row is kept"
    assert "truncated" not in envelope
    assert envelope[LIVE_TEXT_SHORTENED_KEY]["value_chars"] < 120
    with result_format_scope("text"):
        assert _served_bytes(envelope) <= budget
    text = _text(envelope)
    lines = _row_lines(text)
    assert len(lines) == 300
    for i, line in enumerate(lines):
        # The identifying value is whole; the long value is cut.
        assert f"scope:name group:/A/B/C{i:04d} " in line, line
        assert "n" * 200 not in line
    assert "to fit" in text.splitlines()[0]
    _assert_no_json_blob(text)


def test_rows_whose_keys_do_not_fit_serve_a_prefix_and_the_calls_for_the_rest() -> None:
    budget = 16 * 1024
    rows = _rows(2000, name_pad=60)
    envelope = _dispatch(rows, budget=budget)

    # The JSON payload keeps its contract: truncated, digest, refetch.
    assert envelope["truncated"] is True
    assert envelope["digest"].startswith("sha256:")
    assert envelope["refetch"]["relation"] == RELATION
    kept = envelope["rows_returned"]
    assert 0 < kept < 2000 and len(envelope["rows"]) == kept
    with result_format_scope("text"):
        assert _served_bytes(envelope) <= budget
    text = _text(envelope)
    assert text.splitlines()[0].endswith(
        f"live {RELATION} · rows 1-{kept} of 2,000 · identifying fields only, to fit"
    ), text.splitlines()[0]
    assert len(_row_lines(text)) == kept
    rest = [line for line in text.splitlines() if line.startswith("! rows ")]
    # Live build 3: no rerun-with-retain-then-filter hint. A dispatcher
    # called without a way to keep the answer (no `keep`) says only that
    # rows are left out; the MCP path keeps it and names `next`
    # (test_live_next_token.py).
    assert rest == [
        f"! rows 1-{kept} of 2,000 shown, fields cut to fit; narrow the args to see the rest"
    ], rest
    assert "retain" not in text and "filter(" not in text
    _assert_no_json_blob(text)


def test_a_retained_prefix_names_its_id_once_for_set_reads() -> None:
    envelope = _dispatch(_rows(2000, name_pad=60), budget=16 * 1024)
    envelope["result_reference"] = {"retained": True, "id": "l1.abc", "rows": 2000}
    text = _text(envelope)
    assert 'kept: filter/map/aggregate(input={"ref": "l1.abc"})' in text
    assert "offset=" not in text


def test_a_one_object_relation_prints_key_value_lines() -> None:
    answer = {"bytes": 123, "open": False, "name": "/A/B/C", "scope": "group",
              "meta": {"x": 1}, "account": "someone"}
    text = _text(_dispatch(answer, budget=None))

    assert text.splitlines()[0].endswith(f"live {RELATION} · 1 row, all shown")
    body = text.split(f"--- live {RELATION}\n", 1)[1].splitlines()
    assert body == [
        f"{FENCE} source=external_live:{RELATION}⟧",
        "scope:name: group:/A/B/C",
        "account: someone",
        "bytes: 123",
        "open: false",
        "meta: x: 1",
        FENCE_CLOSE,
    ], body
    _assert_no_json_blob(text)


def test_a_one_object_relation_over_budget_is_cut_to_fit() -> None:
    budget = 6 * 1024
    # Over the budget even with each value at its default cut.
    answer = {"name": "/A/B/C", **{f"field_{i:02d}": "v" * 400 for i in range(60)}}
    envelope = _dispatch(answer, budget=budget)

    assert envelope["state"] == "ok" and envelope["rows"]["name"]
    assert LIVE_TEXT_SHORTENED_KEY in envelope
    with result_format_scope("text"):
        assert _served_bytes(envelope) <= budget
    text = _text(envelope)
    assert "name: /A/B/C" in text
    assert "v" * 200 not in text


def test_the_json_form_is_unchanged_by_the_text_fit() -> None:
    """Under the JSON format the text block never helps, so the prefix form
    is the one it always was and carries no text block."""
    envelope = _dispatch(_rows(300, note_chars=400), budget=48 * 1024, fmt="json")
    assert envelope["truncated"] is True
    assert LIVE_TEXT_SHORTENED_KEY not in envelope
    assert envelope["rows"] == envelope["rows"][: envelope["rows_returned"]]
    with result_format_scope("json"):
        assert _served_bytes(envelope) <= 48 * 1024
        assert _served_bytes({**envelope, "rows": _dispatch(
            _rows(300, note_chars=400), budget=None, fmt="json",
        )["rows"][: envelope["rows_returned"] + 1]}) > 48 * 1024


@pytest.mark.parametrize("retain", [False, True])
def test_the_reserve_prints_at_least_as_large_as_any_rowless_form(retain: bool) -> None:
    """The graph step leaves room for the reserve, measured as text: a
    denied call and a digest-only answer must both fit in that room."""
    bound = _bound()
    reserve = live_reserve_envelope(
        bound=bound, args_model=compile_args_model(bound.relation),
        args={"pattern": "/A/B/*"}, generation_id=7, retain=retain,
    )
    denied = _dispatch(_rows(3), budget=None, gate=_deny)
    assert denied["state"] == "denied"

    class NoRowsFit:
        budget_bytes = 10**9

        def fits(self, envelope: dict[str, Any]) -> bool:
            return "rows" not in envelope

    digest = dispatch_live_relation(
        bound=bound, args_model=compile_args_model(bound.relation),
        args={"pattern": "/A/B/*"}, node_id=NODE, gate=_allow,
        adapter=lambda **_k: _rows(50), write_sample=lambda **_k: object(),
        generation_id=7, room=NoRowsFit(),
    )
    assert digest["policy"] == "digest"
    with result_format_scope("text"):
        assert _served_bytes(reserve) >= _served_bytes(denied)
        assert _served_bytes(reserve) >= _served_bytes(digest)
    with result_format_scope("json"):
        assert _served_bytes(reserve) >= _served_bytes(denied)
        assert _served_bytes(reserve) >= _served_bytes(digest)
    assert "! no rows shown of 50; narrow the args to see the rest" in _text(digest)
