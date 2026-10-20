"""`external_live` in an expand `select` (issue #3666).

No database. A live call's rows always come back in `external_live`, beside
the graph rows, so on a call with `live` the name is read as "the live rows"
and taken out of the graph rows' `select`, with a note. Without `live` it
is still refused, and the refusal says what the name is.
"""

from __future__ import annotations

import copy
from typing import Any

import pytest

from okg.substrate.mcp import result_text
from okg.substrate.mcp.envelope import SelectUnknownField, validate_select
from okg.substrate.mcp.read_grammar import rewrite_call
from okg.substrate.mcp.tools import EXPAND_RECORD_FIELDS

LIVE = {"relation": "rel_example", "args": {"id": "x-1"}}
ANCHOR = {"node_id": "repo:example/one"}
SERVED = "live rows, which always return in `external_live` beside the graph rows"


def _rewritten(arguments: dict[str, Any]):
    before = copy.deepcopy(arguments)
    call = rewrite_call("expand", arguments)
    assert arguments == before, "the caller's arguments are never changed in place"
    return call


def _note(served: str) -> dict[str, str]:
    return {"rule": "live_rows_select", "sent": "select `external_live`", "served": served}


def test_external_live_beside_other_fields_is_taken_out_with_a_note():
    call = _rewritten({
        "input": ANCHOR, "live": LIVE, "select": ["node_id", "external_live", "attrs"],
        "limit": 5,
    })
    assert call.arguments == {
        "input": ANCHOR, "live": LIVE, "select": ["node_id", "attrs"], "limit": 5,
    }
    assert call.adjusted == (_note(SERVED),)
    validate_select(call.arguments["select"], EXPAND_RECORD_FIELDS, tool_name="expand")


def test_external_live_alone_serves_the_graph_rows_whole_and_says_so():
    call = _rewritten({"input": ANCHOR, "live": LIVE, "select": ["external_live"]})
    assert call.arguments == {"input": ANCHOR, "live": LIVE}
    assert call.adjusted == (_note(
        SERVED + "; nothing else was selected, so the graph rows are served whole",
    ),)


def test_the_note_prints_on_the_adjusted_line():
    call = _rewritten({"input": ANCHOR, "live": LIVE, "select": ["node_id", "external_live"]})
    line = result_text._adjusted_notice({"adjusted": list(call.adjusted)})
    assert line == "! adjusted: select `external_live` read as " + SERVED


@pytest.mark.parametrize("arguments", [
    {"input": ANCHOR, "select": ["node_id", "external_live"]},
    {"input": ANCHOR, "live": None, "select": ["node_id", "external_live"]},
])
def test_without_live_the_name_is_left_and_the_refusal_says_what_it_is(arguments):
    call = _rewritten(arguments)
    assert call.arguments == arguments
    assert all(record.get("rule") != "live_rows_select" for record in call.adjusted)
    with pytest.raises(SelectUnknownField) as refused:
        validate_select(arguments["select"], EXPAND_RECORD_FIELDS, tool_name="expand")
    said = str(refused.value) + " " + str(refused.value.fix_hint)
    assert "not a select field" in said
    assert "where a live call's rows come back" in said
    assert "live={relation, args}" in said


def test_a_live_call_without_external_live_is_untouched():
    sent = {"input": ANCHOR, "live": LIVE, "select": ["node_id", "attrs"]}
    call = _rewritten(sent)
    assert call.arguments == sent and call.adjusted == ()


@pytest.mark.parametrize("tool_name", [
    "search(method=lexical)", "search(method=attrs)", "filter", "map", "inspect(node)",
])
def test_external_live_hint_is_only_given_where_live_is_taken(tool_name):
    """Only expand takes `live`: elsewhere the name is a plain unknown field."""
    with pytest.raises(SelectUnknownField) as refused:
        validate_select(["node_id", "external_live"], ["node_id", "attrs"], tool_name=tool_name)
    said = str(refused.value) + " " + str(refused.value.fix_hint)
    assert "external_live" in said
    assert "live=" not in said and "live call" not in said
    with pytest.raises(SelectUnknownField) as on_expand:
        validate_select(["node_id", "external_live"], ["node_id", "attrs"], tool_name="expand")
    assert "pass live={relation, args}" in str(on_expand.value.fix_hint)
