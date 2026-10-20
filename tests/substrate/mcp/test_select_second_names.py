"""`select` naming `type` for `subtype` is served, with a note (issue #3666).

No database. On search, expand and an inspect record read, `type` is no
field of the tool and the rows carry the record's type as `subtype`: the
call is read as selecting `subtype` and says so. Where the rows have no
`subtype`, and for other names, the refusal is unchanged.
"""

from __future__ import annotations

import copy
from typing import Any

import pytest

from okg.substrate.mcp import result_text
from okg.substrate.mcp.envelope import SelectUnknownField, validate_select
from okg.substrate.mcp.read_grammar import rewrite_call
from okg.substrate.mcp.select_vocabulary import served_as_selected
from okg.substrate.mcp.tools import _GET_NODE_FIELDS

TYPE_NOTE = {
    "rule": "select_second_name",
    "sent": "select `type`",
    "served": "`subtype`, the record's type",
}


def _rewritten(tool: str, arguments: dict[str, Any]):
    before = copy.deepcopy(arguments)
    call = rewrite_call(tool, arguments)
    assert arguments == before, "the caller's arguments are never changed in place"
    assert rewrite_call(tool, arguments) == call
    return call


@pytest.mark.parametrize("tool,arguments,select", [
    ("search", {"query": "q", "select": ["node_id", "type", "excerpt"]},
     ["node_id", "subtype", "excerpt"]),
    ("search", {"query": "q", "method": "grep", "select": ["type", "node_id"]},
     ["subtype", "node_id"]),
    ("expand", {"input": "n:1", "select": ["node_id", "type", "attrs"]},
     ["node_id", "subtype", "attrs"]),
    ("expand", {"input": "n:1", "depth": 2, "edge_type": "e", "select": ["node_id", "type"]},
     ["node_id", "subtype"]),
    # Named twice: served once.
    ("expand", {"input": "n:1", "select": ["subtype", "type", "node_id"]},
     ["subtype", "node_id"]),
])
def test_type_in_a_select_is_served_as_subtype_with_a_note(tool, arguments, select):
    call = _rewritten(tool, arguments)
    assert call.arguments["select"] == select
    assert TYPE_NOTE in call.adjusted
    assert call.select_retry is None
    assert served_as_selected(tool, call.arguments)


@pytest.mark.parametrize("target,arguments", [
    ({"kind": "node", "node_id": "n:1"}, {"select": ["node_id", "type", "attrs"]}),
    ({"kind": "node", "node_id": "n:1", "select": ["node_id", "type", "attrs"]}, {}),
    ({"kind": "nodes", "node_ids": ["n:1", "n:2"]}, {"select": ["node_id", "type", "attrs"]}),
])
def test_type_in_an_inspect_record_select_is_served_as_subtype(target, arguments):
    sent = {"target": target, **arguments}
    # As sent, the record read's own check refuses the name.
    with pytest.raises(SelectUnknownField) as refused:
        validate_select(["node_id", "type", "attrs"], _GET_NODE_FIELDS, tool_name="inspect(node)")
    assert "(type: use 'subtype')" in str(refused.value)
    call = _rewritten("inspect", sent)
    assert call.arguments["select"] == ["node_id", "subtype", "attrs"]
    assert TYPE_NOTE in call.adjusted
    validate_select(call.arguments["select"], _GET_NODE_FIELDS, tool_name="inspect(node)")


def test_rows_without_subtype_keep_the_refusal():
    """An identifier mention is a span of the query text, not a record."""
    arguments = {"query": "x", "method": "identifier_mentions", "select": ["type"]}
    call = _rewritten("search", arguments)
    assert call.arguments == arguments
    assert TYPE_NOTE not in call.adjusted


def test_id_is_still_refused_with_a_corrected_call():
    arguments = {"input": "n:1", "select": ["id"]}
    call = _rewritten("expand", arguments)
    assert call.arguments == arguments and call.adjusted == ()
    assert call.select_retry == {"input": "n:1", "select": ["node_id"]}


def test_inspect_text_select_is_not_touched():
    sent = {"target": {"kind": "text", "node_id": "n:1"}, "select": ["type"]}
    assert _rewritten("inspect", sent).arguments == sent


def test_the_note_prints_on_the_adjusted_line():
    call = _rewritten("expand", {"input": "n:1", "select": ["node_id", "type"]})
    line = result_text._adjusted_notice({"adjusted": list(call.adjusted)})
    assert line == "! adjusted: select `type` read as `subtype`, the record's type"
