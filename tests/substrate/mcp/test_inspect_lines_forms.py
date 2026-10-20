"""Common ways of writing an `inspect` text read's `lines` (issue #3666).

No database. `lines` written as one `[first, last]` pair, and `lines` sent
beside `target` instead of inside it, are read as the list-of-ranges form
inside the target, each with an `adjusted` note saying so. So are the shapes
of Format v7's refused calls (Format v10 B2): ranges out of order, numbers
written as text, line 0, a flat even-length list, and start/end objects. A
read the rewrite cannot settle is still refused, by name.
"""

from __future__ import annotations

import copy
from typing import Any

import anyio
import pytest

from okg.substrate.mcp import result_text
from okg.substrate.mcp.read_grammar import GrammarRefusal, rewrite_call
from okg.substrate.mcp.record_text import parse_line_target
from okg.substrate.mcp.response_bounds import (
    InvalidOperatorArguments,
    UnknownOperatorArgument,
)
from okg.substrate.mcp.server import build_server
from tests.substrate.mcp._result_payload import call_json, payload

DOC = "doc:src:tools/example.sh"
ONE_RANGE_NOTE = {
    "rule": "lines_one_range",
    "sent": "target.lines as one [first, last] pair",
    "served": "a list of that one range [[first, last]]",
}
MOVED_NOTE = {
    "rule": "argument_moved_into_target",
    "sent": "lines beside target",
    "served": "target.lines",
}

# A port nothing listens on: the argument checks run before any connection.
DEAD_DSN = "postgresql://postgres" + ":okg" + "@localhost:1/okg_probe"


def _rewritten(arguments: dict[str, Any]):
    before = copy.deepcopy(arguments)
    call = rewrite_call("inspect", arguments)
    assert arguments == before, "the caller's arguments are never changed in place"
    return call


@pytest.fixture
def mcp_server(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_RO_DSN", DEAD_DSN)
    server = build_server(dsn=DEAD_DSN)
    try:
        yield server
    finally:
        server._okg_pool.close()
        if server._okg_write_pool:
            server._okg_write_pool.close()


# --------------------------------------------------------------------------- #
# one flat [first, last] pair
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("pair,ranges", [
    ([1, 91], ((1, 91),)),
    ([40, None], ((40, -1),)),
])
def test_one_flat_pair_is_read_as_one_range_with_a_note(pair, ranges) -> None:
    call = _rewritten({
        "target": {"kind": "text", "node_id": DOC, "lines": pair}, "max_bytes": 16000,
    })
    assert call.arguments == {
        "target": {"kind": "text", "node_id": DOC, "lines": [pair]}, "max_bytes": 16000,
    }
    assert call.adjusted == (ONE_RANGE_NOTE,)
    target = call.arguments["target"]
    served = parse_line_target(target["lines"], target.get("at"))
    assert served is not None and len(served) == 1
    assert served[0][0] == ranges[0][0]


def test_the_list_form_is_served_as_sent_with_no_note() -> None:
    sent = {"target": {"kind": "text", "node_id": DOC, "lines": [[1, 91], [120, 130]]}}
    call = _rewritten(sent)
    assert call.arguments == sent
    assert call.adjusted == ()


def test_a_flat_pair_with_bad_bounds_is_still_refused_by_the_range_check() -> None:
    """The pair is wrapped, never repaired: first > last is refused as before."""
    call = _rewritten({"target": {"kind": "text", "node_id": DOC, "lines": [91, 1]}})
    target = call.arguments["target"]
    with pytest.raises(InvalidOperatorArguments) as refused:
        parse_line_target(target["lines"], None)
    assert "lines must be 1 to" in refused.value.message
    assert refused.value.data["invalid_fields"] == ["target.lines"]


@pytest.mark.parametrize("lines", [[1], [1, 2, 3], ["1", "x"], [True, 5], [[1, 2], 5]])
def test_a_value_that_is_no_flat_pair_is_left_for_the_range_check(lines) -> None:
    call = _rewritten({"target": {"kind": "text", "node_id": DOC, "lines": lines}})
    assert call.arguments["target"]["lines"] == lines
    assert call.adjusted == ()
    with pytest.raises(InvalidOperatorArguments):
        parse_line_target(lines, None)


def test_a_flat_pair_in_a_batch_item_is_read_as_one_range_with_a_note() -> None:
    call = _rewritten({"target": {"kind": "texts", "items": [
        {"key": "a", "node_id": DOC, "lines": [[1, 5]]},
        {"key": "b", "node_id": DOC, "lines": [10, 20]},
    ]}})
    assert call.arguments["target"]["items"] == [
        {"key": "a", "node_id": DOC, "lines": [[1, 5]]},
        {"key": "b", "node_id": DOC, "lines": [[10, 20]]},
    ]
    assert call.adjusted == ({
        **ONE_RANGE_NOTE, "sent": "target.items[1].lines as one [first, last] pair",
    },)
    for index, item in enumerate(call.arguments["target"]["items"]):
        parse_line_target(item["lines"], None, field=f"target.items[{index}]")


def test_a_flat_pair_on_another_kind_is_not_touched() -> None:
    sent = {"target": {"kind": "node", "node_id": DOC, "lines": [1, 5]}}
    assert _rewritten(sent).arguments == sent


# --------------------------------------------------------------------------- #
# lines beside target
# --------------------------------------------------------------------------- #


def test_lines_beside_a_text_target_is_moved_into_it_and_the_checks_accept_it(
        mcp_server) -> None:
    sent = {"target": {"kind": "text", "node_id": DOC}, "lines": [[174, 310]],
            "max_bytes": 20000}
    with pytest.raises(UnknownOperatorArgument):
        mcp_server._refuse_unknown_operator_arguments("inspect", sent)
    call = _rewritten(sent)
    assert call.arguments == {
        "target": {"kind": "text", "node_id": DOC, "lines": [[174, 310]]}, "max_bytes": 20000,
    }
    assert call.adjusted == (MOVED_NOTE,)
    mcp_server._refuse_unknown_operator_arguments("inspect", call.arguments)
    mcp_server._validate_operator_arguments("inspect", call.arguments)


def test_a_flat_pair_beside_target_gets_both_notes() -> None:
    call = _rewritten({"target": {"kind": "text", "node_id": DOC}, "lines": [3, 9]})
    assert call.arguments == {"target": {"kind": "text", "node_id": DOC, "lines": [[3, 9]]}}
    assert call.adjusted == (MOVED_NOTE, ONE_RANGE_NOTE)


def test_the_same_lines_beside_and_inside_target_are_read_once() -> None:
    call = _rewritten({
        "target": {"kind": "text", "node_id": DOC, "lines": [[3, 9]]}, "lines": [3, 9],
    })
    assert call.arguments == {"target": {"kind": "text", "node_id": DOC, "lines": [[3, 9]]}}
    assert call.adjusted == (MOVED_NOTE,)


def test_different_lines_beside_and_inside_target_are_refused_naming_both(
        mcp_server) -> None:
    sent = {"target": {"kind": "text", "node_id": DOC, "lines": [[3, 9]]},
            "lines": [[20, 30]]}
    with pytest.raises(GrammarRefusal) as refused:
        rewrite_call("inspect", sent)
    assert refused.value.fields == ("lines", "target.lines")

    async def run():
        return await call_json(mcp_server, "inspect", sent), await mcp_server.call_tool(
            "inspect", sent)

    as_json, as_text = anyio.run(run)
    assert as_json.isError is True and as_text.isError is True
    reply = payload(as_json)
    assert reply["status"] == "refused"
    assert reply["error"]["code"] == "invalid_operator_arguments"
    text = as_text.content[0].text
    assert "`lines` is given beside `target` and inside it" in text


def test_lines_beside_a_batch_target_is_still_refused_as_unknown(mcp_server) -> None:
    """A batch has one `lines` per item: a top-level one could mean any."""
    sent = {"target": {"kind": "texts", "items": [{"key": "a", "node_id": DOC}]},
            "lines": [[1, 2]]}
    call = _rewritten(sent)
    assert call.arguments == sent
    with pytest.raises(UnknownOperatorArgument):
        mcp_server._refuse_unknown_operator_arguments("inspect", call.arguments)


def test_the_notes_print_as_one_adjusted_line() -> None:
    call = _rewritten({"target": {"kind": "text", "node_id": DOC}, "lines": [3, 9]})
    line = result_text._adjusted_notice({"adjusted": list(call.adjusted)})
    assert line == (
        "! adjusted: lines beside target read as target.lines; "
        "target.lines as one [first, last] pair read as a list of that one range "
        "[[first, last]]"
    )


# --------------------------------------------------------------------------- #
# Format v7's refused shapes (Format v10 B2): each read with a note
# --------------------------------------------------------------------------- #


def _note(rule: str, sent: str, served: str, path: str) -> dict[str, str]:
    return {"rule": rule, "sent": f"{path} {sent}", "served": served}


def one_note(path: str = "target.lines") -> dict[str, str]:
    return _note("lines_one_range", "as one [first, last] pair",
                 "a list of that one range [[first, last]]", path)


def order_note(path: str = "target.lines") -> dict[str, str]:
    return _note("lines_in_order", "ranges out of order", "ascending order", path)


def merged_note(path: str = "target.lines") -> dict[str, str]:
    return _note("lines_in_order", "ranges out of order or overlapping",
                 "ascending order, overlaps merged", path)


def text_note(path: str = "target.lines") -> dict[str, str]:
    return _note("lines_numbers_as_text", "numbers written as text", "whole numbers", path)


def zero_note(path: str = "target.lines") -> dict[str, str]:
    return _note("lines_start_zero", "line 0", "line 1 (lines count from 1)", path)


def flat_note(path: str = "target.lines") -> dict[str, str]:
    return _note("lines_flat_list", "as one flat list of numbers",
                 "consecutive pairs [[first, last], ...]", path)


def objects_note(path: str = "target.lines") -> dict[str, str]:
    return _note("lines_range_objects", "ranges as start/end objects",
                 "[first, last] pairs", path)


NOTES = {
    "one": one_note, "order": order_note, "merged": merged_note, "text": text_note,
    "zero": zero_note, "flat": flat_note, "objects": objects_note,
}


@pytest.mark.parametrize("sent,served,notes", [
    # Ranges out of order: the measured calls, served ascending.
    ([[1798, 1856], [1, 100]], [[1, 100], [1798, 1856]], ("order",)),
    ([[1620, 1770], [570, 665]], [[570, 665], [1620, 1770]], ("order",)),
    ([[1800, 1900], [2560, 2725], [2400, 2540], [3525, 3710]],
     [[1800, 1900], [2400, 2540], [2560, 2725], [3525, 3710]], ("order",)),
    ([[480, 527], [370, 414]], [[370, 414], [480, 527]], ("order",)),
    # Overlapping ranges are merged; an open end reaches past later ranges.
    ([[1, 10], [5, 20], [30, 40]], [[1, 20], [30, 40]], ("merged",)),
    ([[50, None], [60, 70], [1, 5]], [[1, 5], [50, None]], ("merged",)),
    # Numbers written as text.
    ([["146", "245"], ["958", "974"]], [[146, 245], [958, 974]], ("text",)),
    ([["1", "150"]], [[1, 150]], ("text",)),
    # Line 0 is line 1.
    ([0, 20], [[1, 20]], ("one", "zero")),
    ([0, 0], [[1, 1]], ("one", "zero")),
    ([[0, 20], [40, 50]], [[1, 20], [40, 50]], ("zero",)),
    # A flat even-length list is consecutive pairs.
    ([450, 650, 805, 900], [[450, 650], [805, 900]], ("flat",)),
    (["1", "91"], [[1, 91]], ("one", "text")),
    # Start/end objects.
    ([{"start": 1, "end": 4}], [[1, 4]], ("objects",)),
    ([{"start": 10, "end": 12}, [1, 4]], [[1, 4], [10, 12]], ("objects", "order")),
    ([{"first": 1, "last": None}], [[1, None]], ("objects",)),
])
def test_a_refused_v7_shape_is_read_in_the_range_form_with_notes(
        sent, served, notes) -> None:
    with pytest.raises(InvalidOperatorArguments):
        parse_line_target(sent, None)
    call = _rewritten({"target": {"kind": "text", "node_id": DOC, "lines": sent}})
    assert call.arguments == {"target": {"kind": "text", "node_id": DOC, "lines": served}}
    assert call.adjusted == tuple(NOTES[name]() for name in notes)
    parse_line_target(call.arguments["target"]["lines"], None)


def test_a_rewritten_shape_passes_the_server_checks(mcp_server) -> None:
    sent = {"target": {"kind": "text", "node_id": DOC,
                       "lines": [["1798", "1856"], [0, 100]]}, "max_bytes": 16000}
    call = _rewritten(sent)
    assert call.arguments["target"]["lines"] == [[1, 100], [1798, 1856]]
    assert call.adjusted == (text_note(), zero_note(), order_note())
    mcp_server._refuse_unknown_operator_arguments("inspect", call.arguments)
    mcp_server._validate_operator_arguments("inspect", call.arguments)


@pytest.mark.parametrize("lines", [
    [[10, 5], [1, 2]],                      # first > last: never reordered
    [[5, 10], [-1, 2]],                     # a negative line: never reordered
    [[index * 10 + 1, index * 10 + 2] for index in range(21)][::-1],  # 21 ranges
    [1, 2, 3, 4, 5],                        # odd flat list
    [1, None, 3, 4],                        # an open end mid flat list
    [["1.5", "3"]],                         # no whole number
    [["abc", "2"]],
    [{"from": 1, "to": 4}],                 # unknown object keys
    [{"start": 1, "end": 4, "step": 1}],
    [{"start": 9, "end": 2}],               # first > last after reading
])
def test_a_shape_the_rewrite_cannot_settle_is_still_refused(lines) -> None:
    call = _rewritten({"target": {"kind": "text", "node_id": DOC, "lines": lines}})
    target = call.arguments["target"]
    if target["lines"] == lines:
        assert call.adjusted == ()
    with pytest.raises(InvalidOperatorArguments) as refused:
        parse_line_target(target["lines"], None)
    assert "lines must be 1 to" in refused.value.message


def test_a_batch_item_shape_is_read_with_the_item_path() -> None:
    call = _rewritten({"target": {"kind": "texts", "items": [
        {"key": "a", "node_id": DOC, "lines": [[1, 5]]},
        {"key": "b", "node_id": DOC, "lines": [["20", "30"], ["1", "4"]]},
        {"key": "c", "node_id": DOC, "lines": [{"start": 1, "end": 2}]},
    ]}})
    assert call.arguments["target"]["items"] == [
        {"key": "a", "node_id": DOC, "lines": [[1, 5]]},
        {"key": "b", "node_id": DOC, "lines": [[1, 4], [20, 30]]},
        {"key": "c", "node_id": DOC, "lines": [[1, 2]]},
    ]
    assert call.adjusted == (
        text_note("target.items[1].lines"), order_note("target.items[1].lines"),
        objects_note("target.items[2].lines"),
    )
    for index, item in enumerate(call.arguments["target"]["items"]):
        parse_line_target(item["lines"], None, field=f"target.items[{index}]")


def test_one_value_spelled_two_ways_beside_and_inside_target_is_read_once() -> None:
    call = _rewritten({
        "target": {"kind": "text", "node_id": DOC, "lines": [[3, 9]]}, "lines": ["3", "9"],
    })
    assert call.arguments == {"target": {"kind": "text", "node_id": DOC, "lines": [[3, 9]]}}
    assert call.adjusted == (MOVED_NOTE,)


def test_the_shape_notes_print_on_the_adjusted_line() -> None:
    call = _rewritten({"target": {"kind": "text", "node_id": DOC,
                                  "lines": [["480", "527"], ["370", "414"]]}})
    line = result_text._adjusted_notice({"adjusted": list(call.adjusted)})
    assert line == (
        "! adjusted: target.lines numbers written as text read as whole numbers; "
        "target.lines ranges out of order read as ascending order"
    )
