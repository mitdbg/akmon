"""The shared read grammar through the served tools (issue #3666, step 2a).

PostgreSQL tests on the published list corpus. Every case sends one call in
a new spelling and the call it stands for in today's spelling, through one
MCP client session, and compares the two replies: the rewrite is the only
thing between them.
"""
from __future__ import annotations

import ast
import json
import re
from typing import Any

import anyio
import psycopg
import pytest

from okg.substrate.external_backed.live_relations import ANNOTATION_KEY, parse_live_relations

from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes
from tests.substrate.mcp.test_page_next_db import CHUNK as TEXT_CHUNK
from tests.substrate.mcp.test_page_next_db import CHUNK_TEXTS
from tests.substrate.mcp.test_page_next_db import DOC as TEXT_DOC
from tests.substrate.mcp.test_search_list import (
    DOC,
    DOC_CHUNK,
    EXTERNAL,
    MAIN,
    READER,
    TERM,
    Served,
    _activate,
    _corpus,
    _edge,
    _node,
    _trust,
)

pytestmark = pytest.mark.postgres

#: Reply fields that carry the time of the call, not what was read.
_CLOCK_FIELDS = ("source_freshness", "publish_freshness")
#: Reply fields that name the call or its client session, at any depth.
_CALL_IDENTITY_FIELDS = (
    "logical_session_id", "error_id", "trace_id", "decision_id", "decision_ids",
    # The reply's own size (PACT read-reply-size-3666): it counts every other
    # field, the `adjusted` note of a corrected call included.
    "reply_size",
)


class Tools(Served):
    """`Served`, for any tool."""

    def run(self, *requests: tuple[str, dict, str]) -> list:
        """Each ``(tool, arguments, format)`` through one client session."""
        from mcp.shared.memory import create_connected_server_and_client_session

        out: list = []

        async def drive() -> None:
            async with create_connected_server_and_client_session(self.mcp) as client:
                for tool, arguments, result_format in requests:
                    out.append(await client.call_tool(
                        tool, arguments, meta={"okg/result_format": result_format},
                    ))

        anyio.run(drive)
        return out

    def pair(self, tool: str, sent: dict, today: dict) -> tuple[dict, dict]:
        """The JSON replies to a new spelling and to today's call."""
        one, two = self.run((tool, sent, "json"), (tool, today, "json"))
        return json.loads(one.content[0].text), json.loads(two.content[0].text)

    def text(self, tool: str, arguments: dict) -> str:  # type: ignore[override]
        (result,) = self.run((tool, arguments, "text"))
        return result.content[0].text


@pytest.fixture
def tools(clean_runtime, okg_mcp_can_log_in, tmp_path):
    server = Tools(clean_runtime, _corpus(tmp_path, clean_runtime))
    try:
        yield server
    finally:
        server.close()


def _same(reply: Any, *, without: tuple[str, ...] = ()) -> Any:
    """`reply` without the fields that say when, or as which call, it was
    made: the one comparison every equality check in this file uses."""
    if isinstance(reply, dict):
        return {
            key: _same(value) for key, value in reply.items()
            if key not in _CLOCK_FIELDS + _CALL_IDENTITY_FIELDS + without
        }
    if isinstance(reply, list):
        return [_same(item) for item in reply]
    return reply


def _served(reply: dict) -> dict:
    assert reply.get("status") != "refused", json.dumps(reply)[:800]
    return reply


def _ids(reply: dict, key: str = "items", field: str = "node_id") -> list[str]:
    return sorted(str(row[field]) for row in _served(reply)[key])


def _equal(tools: Tools, tool: str, sent: dict, today: dict) -> dict:
    """Assert the two calls reply alike with nothing adjusted; today's reply."""
    new, old = tools.pair(tool, sent, today)
    _served(old)
    assert "adjusted" not in new, new["adjusted"]
    assert _same(new) == _same(old), (tool, sent)
    return old


PATH_001 = {"op": "eq", "field": "attrs.path", "value": "docs/001.md"}
TWO_PATHS = ["docs/001.md", "docs/002.md"]


# --------------------------------------------------------------------------- #
# the short form, top-level `where` and `select`, the field aliases            #
# --------------------------------------------------------------------------- #


def test_search_short_form_equals_the_long_form(tools):
    one = _equal(
        tools, "search",
        {"query": TERM, "where": {"type": "ticket"}},
        {"query": TERM, "where": {"op": "eq", "field": "type", "value": "ticket"}},
    )
    assert _ids(one, "results") == ["ticket:1"]
    several = _equal(
        tools, "search",
        {"query": TERM, "limit": 5,
         "where": {"subtype": "doc", "id": ["doc:001", "doc:002", "ticket:1"]}},
        {"query": TERM, "limit": 5, "where": {"op": "and", "clauses": [
            {"op": "eq", "field": "subtype", "value": "doc"},
            {"op": "in", "field": "id", "values": ["doc:001", "doc:002", "ticket:1"]},
        ]}},
    )
    assert _ids(several, "results") == ["doc:001", "doc:002"]
    nested = _equal(
        tools, "search",
        {"query": TERM, "limit": 5, "where": {"op": "and", "clauses": [
            {"type": "doc"}, {"op": "not", "clause": {"id": "doc:001"}},
            {"id": {"op": "prefix", "value": "doc:00"}},
        ]}},
        {"query": TERM, "limit": 5, "where": {"op": "and", "clauses": [
            {"op": "eq", "field": "type", "value": "doc"},
            {"op": "not", "clause": {"op": "eq", "field": "id", "value": "doc:001"}},
            {"op": "prefix", "field": "id", "value": "doc:00"},
        ]}},
    )
    assert "doc:001" not in _ids(nested, "results")


def test_filter_where_equals_input_and_predicate(tools):
    # No `input`: the type in `where` is the set's type.
    listing = _equal(
        tools, "filter",
        {"where": {"type": "ticket"}},
        {"input": {"kind": "nodes", "subtype": "ticket"}, "predicate": {}},
    )
    assert _ids(listing) == ["ticket:1"]
    one = _equal(
        tools, "filter",
        {"where": {"type": "doc", "attrs.path": "docs/001.md"}},
        {"input": {"kind": "nodes", "subtype": "doc"}, "predicate": PATH_001},
    )
    assert _ids(one) == ["doc:001"]
    two = _equal(
        tools, "filter",
        {"where": {"subtype": "doc", "attrs.path": TWO_PATHS}, "select": ["node_id"]},
        {"input": {"kind": "nodes", "subtype": "doc"}, "select": ["node_id"],
         "predicate": {"op": "in", "field": "attrs.path", "values": TWO_PATHS}},
    )
    assert _ids(two) == ["doc:001", "doc:002"]
    # The old spellings take the short form and the aliases as well.
    old = _equal(
        tools, "filter",
        {"input": {"kind": "nodes", "filter": {"type": "doc"}},
         "predicate": {"attrs.path": {"op": "suffix", "value": "/001.md"}}},
        {"input": {"kind": "nodes",
                   "filter": {"op": "eq", "field": "subtype", "value": "doc"}},
         "predicate": {"op": "suffix", "field": "attrs.path", "value": "/001.md"}},
    )
    assert _ids(old) == ["doc:001"]


def test_the_id_alias_reads_the_node_id(tools):
    by_id = _equal(
        tools, "filter",
        {"where": {"id": "ticket:1"}},
        {"input": {"kind": "nodes"},
         "predicate": {"op": "eq", "field": "node_id", "value": "ticket:1"}},
    )
    assert _ids(by_id) == ["ticket:1"]
    # Failure case: on an edge set `id` could be the edge's or an end's, so
    # the key is refused in words that name the three fields, with no call.
    error = _refused(tools, "filter", {"input": {"kind": "edges"}, "where": {"id": "ticket:1"}})["error"]
    assert "`id` is not a field" in error["message"]
    assert "retry_call" not in error
    assert all(name in error["fix_hint"] for name in ("`edge_id`", "`src`", "`dst`"))


def test_a_long_form_clause_in_an_existing_argument_reads_the_second_names(tools):
    """Step 2c: `type` and `id` in a long-form clause of `predicate` read the
    record's columns, as in `where`, with a note there. (Before, the clause
    was passed on and refused: neither read path knew either name.)"""
    type_under_not = {"op": "and", "clauses": [
        PATH_001, {"op": "not", "clause": {"op": "eq", "field": "type", "value": "ticket"}},
    ]}
    by_subtype = {"op": "and", "clauses": [
        PATH_001,
        {"op": "not", "clause": {"op": "eq", "field": "subtype", "value": "ticket"}},
    ]}
    scope = {"kind": "nodes", "subtype": "doc"}
    renamed = _equal(
        tools, "filter",
        {"input": scope, "where": type_under_not},
        {"input": scope, "predicate": by_subtype},
    )
    assert _ids(renamed) == ["doc:001"]
    assert _ids(_noted(
        tools, "filter",
        {"input": scope, "predicate": type_under_not},
        {"input": scope, "predicate": by_subtype},
        ["second_name"],
    )) == ["doc:001"]


def _noted(tools: Tools, tool: str, sent: dict, meant: dict, rules: list[str]) -> dict:
    """Assert `sent` is served as `meant` with notes of `rules`; the reply."""
    noted, plain = tools.pair(tool, sent, meant)
    assert [record["rule"] for record in _served(noted)["adjusted"]] == rules
    assert "adjusted" not in _served(plain)
    assert _same(noted, without=("adjusted",)) == _same(plain), (tool, sent)
    return plain


def _dispatched(tools: Tools, reply: dict, sent: dict | None = None) -> dict:
    """Send the call a refusal offers, exactly as offered: it must be served,
    with no note. Every assertion that a `retry_call` exists goes through
    here, so no test accepts an offered call the server would refuse. What
    the offer lists under `repeat` is taken from the caller's own call."""
    retry = reply["error"]["retry_call"]
    assert retry["mode"] in {"corrected", "guess"}
    if sent is not None:
        # `reply` is the JSON text block the client session received. The
        # text reply an agent reads must carry the same call, and name it
        # on its first line.
        from tests.substrate.mcp.test_retry_call_reaches_the_caller_db import (
            received_retry_call,
        )

        (as_text,) = tools.run((reply["operator_name"], sent, "text"))
        assert received_retry_call(as_text) == retry, as_text.content[0].text[:1200]
        head = as_text.content[0].text.splitlines()[0]
        assert "see fix_hint" not in head and "retry: " in head, head
    repeated = {name: (sent or {})[name] for name in retry.get("repeat", [])}
    assert not set(repeated) & set(retry["arguments"])
    (result,) = tools.run((retry["tool"], {**retry["arguments"], **repeated}, "json"))
    served = json.loads(result.content[0].text)
    assert not result.isError and served.get("status") != "refused", (retry, served)
    assert "adjusted" not in served, (retry, served["adjusted"])
    return served


def _refused(tools: Tools, tool: str, arguments: dict) -> dict:
    (result,) = tools.run((tool, arguments, "json"))
    reply = json.loads(result.content[0].text)
    assert result.isError and reply["status"] == "refused", reply
    assert reply["error"]["code"] == "invalid_operator_arguments", reply["error"]
    assert "items" not in reply and "results" not in reply and "neighbors" not in reply
    return reply


def test_a_short_form_key_that_is_no_field_is_refused_not_answered_empty(tools):
    sent = {"where": {"type": "doc", "attrs.path": "docs/001.md", "summary": "Document 001"}}
    reply = _refused(tools, "filter", sent)
    assert "`summary` is not a field" in reply["error"]["message"]
    retry = reply["error"]["retry_call"]
    assert retry["tool"] == "filter" and retry["mode"] == "guess"
    assert retry["arguments"] == {"where": {
        "type": "doc", "attrs.path": "docs/001.md", "attrs.summary": "Document 001",
    }}
    # The call the refusal names is served, and finds the record.
    assert _ids(_dispatched(tools, reply)) == ["doc:001"]
    served = _equal(
        tools, "filter", retry["arguments"],
        {"input": {"kind": "nodes", "subtype": "doc"}, "predicate": {"op": "and", "clauses": [
            PATH_001, {"op": "eq", "field": "attrs.summary", "value": "Document 001"},
        ]}},
    )
    assert _ids(served) == ["doc:001"]
    # The same key refuses whether or not another key names an attribute.
    for where in ({"summary": "Document 001"}, {"type": "doc", "summary": "Document 001"}):
        alone = _refused(tools, "filter", {"where": where})
        assert "`summary` is not a field" in alone["error"]["message"]


#: A refused call, and the rows its offered call returns when it is sent.
_OFFERED = [
    ("filter", {"where": {"Type": "ticket"}}, "items", "node_id", ["ticket:1"]),
    ("filter", {"where": {"type": "doc", "attrs.path": {"suffix": "/001.md"}}},
     "items", "node_id", ["doc:001"]),
    ("filter", {"where": {"type": "ticket", "attrs.no_such_key": {"exists": False}}},
     "items", "node_id", ["ticket:1"]),
    ("filter", {"where": {"or": [{"id": "doc:001"}, {"id": "ticket:1"}]}},
     "items", "node_id", ["doc:001", "ticket:1"]),
    ("filter", {"where": {"not": {"Type": "doc"}, "attrs.summary": "Ticket one"}},
     "items", "node_id", ["ticket:1"]),
    ("filter", {"where": {"field": "type", "value": "ticket"}, "predicate": {}},
     "items", "node_id", None),
    # Step 2c: `predicate` reads the second names, so this is served too.
    ("filter", {"input": {"kind": "nodes"}, "predicate": {"field": "type", "value": "ticket"}},
     "items", "node_id", None),
    ("map", {"where": {"Type": "ticket"}}, "items", "node_id", ["ticket:1"]),
    # Step 2c: the repair leaves a long-form `type`, which `predicate` and
    # `input.filter` now read, so the repaired call is offered and served.
    ("filter", {"input": {"kind": "nodes"}, "predicate": {"op": "and", "clauses": [
        {"op": "eq", "field": "type", "value": "doc"}, {"attrs.path": {"gt": "docs/130.md"}}]}},
     "items", "node_id", ["doc:131", "doc:ext"]),
    ("map", {"input": {"kind": "nodes", "filter": {"op": "and", "clauses": [
        {"op": "eq", "field": "type", "value": "doc"}, {"attrs.path": {"gt": "docs/130.md"}}]}}},
     "items", "node_id", ["doc:131", "doc:ext"]),
    # Step 2c: filter compares a column by prefix (refused, and not offered,
    # before).
    ("filter", {"where": {"id": {"prefix": "doc:00"}}},
     "items", "node_id", [f"doc:00{i}" for i in range(10)]),
    # Step 2c: on a set of links `type` is the link's type.
    ("filter", {"input": {"kind": "edges"},
                "predicate": {"op": "and", "clauses": [
                    {"op": "eq", "field": "type", "value": "contains"},
                    {"op": "prefix", "field": "src", "value": "doc:001"}]}},
     "items", "dst", ["doc_chunk:001-0", "doc_chunk:001-1", "doc_chunk:001-2"]),
    ("expand", {"input": {"node_id": "doc:000"}, "edge_type": "contains",
                "where": {"Type": "doc_chunk", "attrs.text": {"prefix": "zirconium passage 1"}}},
     "neighbors", "node_id", ["doc_chunk:000-1"]),
    ("search", {"query": TERM, "where": {"Type": "ticket"}}, "results", "node_id", ["ticket:1"]),
    # D4: the shapes once checked only against the argument model.
    ("filter", {"input": {"kind": "edges"}, "where": {"type": "contains", "src": "doc:000"}},
     "items", "dst", ["doc_chunk:000-0", "doc_chunk:000-1", "doc_chunk:000-2"]),
    ("filter", {"where": {"type": "doc", "attrs.path": {"gt": "docs/130.md"}}},
     "items", "node_id", ["doc:131", "doc:ext"]),
    # #3666 step M7: `text` is a field now; with no op it is served as a
    # phrase match with a note, so nothing is offered.
    ("expand", {"input": {"node_id": "doc:000"}, "edge_type": "contains",
                "where": {"field": "text", "value": "zirconium passage 2 of document 0."}},
     "neighbors", "node_id", None),
    # An offer that names `authority` to repeat: sent again with the caller's own.
    ("filter", {"where": {"Type": "ticket"}, "authority": {}}, "items", "node_id", ["ticket:1"]),
    ("search", {"query": TERM, "limit": 5, "where": {"not": {"type": "doc"}, "Type": "ticket"}},
     "results", "node_id", ["ticket:1"]),
]


@pytest.mark.parametrize("case", range(len(_OFFERED)))
def test_every_offered_call_is_served_when_it_is_sent(tools, case):
    """M2: the proof that a `retry_call` works is the server serving it. The
    refusal's call is dispatched as written and must come back served, with
    no note, holding the rows the caller was after."""
    tool, sent, rows, field, expected = _OFFERED[case]
    (result,) = tools.run((tool, sent, "json"))
    reply = json.loads(result.content[0].text)
    if expected is None:
        # Served with a note today, so nothing is offered: nothing to send.
        assert not result.isError and reply["adjusted"]
        if tool == "expand":
            # #3666: the phrase is matched on the far records, the rows.
            assert [str(row[field]) for row in reply[rows]] == ["doc_chunk:000-2"]
        return
    assert result.isError and reply["status"] == "refused", reply
    assert ("repeat" in reply["error"]["retry_call"]) is ("authority" in sent)
    served = _dispatched(tools, reply, sent)
    assert sorted(str(row[field]) for row in served[rows]) == expected


#: Calls with one fix whose fixed form the tool refuses: no call is offered.
_NOT_OFFERED = [
    # #3666: `contains` with text on an attribute is served; a number is not.
    ("filter", {"where": {"Type": "doc", "attrs.summary": {"op": "contains", "value": 5}}},
     {"where": {"type": "doc", "attrs.summary": {"op": "contains", "value": 5}}}),
    ("filter", {"input": {"kind": "edges", "subtype": "doc"}, "where": {"type": "contains"}},
     {"input": {"kind": "edges", "subtype": "doc"}, "where": {"edge_type": "contains"}}),
    ("search", {"query": TERM, "where": {"Type": "doc", "date.bogus": "2025-01-01"}},
     {"query": TERM, "where": {"type": "doc", "date.bogus": "2025-01-01"}}),
    # D1: the repair fixes one mistake and a second one remains. (On filter
    # and map the long-form `type` is read since step 2c; see _OFFERED.)
    ("expand", {"input": {"node_id": "doc:000"}, "edge_type": "contains",
                "filter": {"op": "and", "clauses": [
                    {"op": "eq", "field": "type", "value": "doc_chunk"},
                    {"attrs.text": {"prefix": "zirconium"}}]}},
     {"input": {"node_id": "doc:000"}, "edge_type": "contains",
      "filter": {"op": "and", "clauses": [
          {"op": "eq", "field": "type", "value": "doc_chunk"},
          {"attrs.text": {"op": "prefix", "value": "zirconium"}}]}}),
    ("filter", {"where": {"Type": "doc", "attrs.path": {"op": "suffix", "value": ""}}},
     {"where": {"type": "doc", "attrs.path": {"op": "suffix", "value": ""}}}),
    ("filter", {"where": {"Type": "doc", "attrs.path": {"op": "suffix", "value": 5}}},
     {"where": {"type": "doc", "attrs.path": {"op": "suffix", "value": 5}}}),
    ("filter", {"where": {"op": "and", "clauses": [
        {"op": "gt", "field": "attrs", "value": 1}, {"Type": "doc"}]}},
     {"where": {"op": "and", "clauses": [
         {"op": "gt", "field": "attrs", "value": 1}, {"type": "doc"}]}}),
]


@pytest.mark.parametrize("case", range(len(_NOT_OFFERED)))
def test_a_fix_the_tool_would_refuse_is_not_offered(tools, case):
    """M2, the other side: the fixed call really is refused when sent, which
    is why the refusal gives words and no call."""
    tool, sent, fixed = _NOT_OFFERED[case]
    (result,) = tools.run((tool, sent, "json"))
    reply = json.loads(result.content[0].text)
    assert result.isError and "retry_call" not in reply["error"], reply["error"]
    assert reply["error"]["fix_hint"]
    (direct,) = tools.run((tool, fixed, "json"))
    assert direct.isError, json.loads(direct.content[0].text)


def test_an_empty_where_adds_nothing_and_alone_is_the_empty_condition(tools):
    scope = {"kind": "nodes", "subtype": "doc"}
    beside = _equal(
        tools, "filter",
        {"input": scope, "predicate": PATH_001, "where": {}},
        {"input": scope, "predicate": PATH_001},
    )
    assert _ids(beside) == ["doc:001"]
    alone = _equal(
        tools, "filter",
        {"input": {"kind": "nodes", "subtype": "ticket"}, "where": {}},
        {"input": {"kind": "nodes", "subtype": "ticket"}, "predicate": {}},
    )
    assert _ids(alone) == ["ticket:1"]


def test_a_test_for_absence_is_served_as_not_exists(tools):
    """`{"exists": false}` is refused; the call it offers lists the records
    that lack the attribute, and never the ones that have it."""
    reply = _refused(tools, "filter", {"where": {"type": "doc", "attrs.opened_at": {"exists": False}}})
    lacking = _ids(_dispatched(tools, reply))
    having = _ids(_equal(
        tools, "filter",
        {"where": {"type": "doc", "attrs.opened_at": {"op": "exists"}}, "limit": 200},
        {"input": {"kind": "nodes", "subtype": "doc"}, "limit": 200,
         "predicate": {"op": "exists", "field": "attrs.opened_at"}},
    ))
    assert lacking and having and not set(lacking) & set(having)
    assert "doc:no-date" in lacking and "doc:001" in having


def test_map_where_and_select_equal_input_and_projection(tools):
    listing = _equal(
        tools, "map",
        {"where": {"type": "ticket"}, "select": ["node_id", "subtype"]},
        {"input": {"kind": "nodes", "subtype": "ticket"},
         "projection": ["node_id", "subtype"]},
    )
    assert _ids(listing) == ["ticket:1"]
    assert listing["items"][0]["subtype"] == "ticket"
    two = _equal(
        tools, "map",
        {"where": {"type": "doc", "attrs.path": TWO_PATHS},
         "select": ["node_id", "attrs.path"]},
        {"input": {"kind": "nodes", "subtype": "doc",
                   "filter": {"op": "in", "field": "attrs.path", "values": TWO_PATHS}},
         "projection": ["node_id", "attrs.path"]},
    )
    assert _ids(two) == ["doc:001", "doc:002"]


def test_aggregate_where_equals_input_filter(tools):
    counted = _equal(
        tools, "aggregate",
        {"where": {"attrs.path": TWO_PATHS}, "by": ["subtype"], "metrics": ["count"]},
        {"input": {"kind": "nodes",
                   "filter": {"op": "in", "field": "attrs.path", "values": TWO_PATHS}},
         "by": ["subtype"], "metrics": ["count"]},
    )
    assert json.dumps(counted).count('"doc"') >= 1
    typed = _equal(
        tools, "aggregate",
        {"where": {"type": "doc", "attrs.path": TWO_PATHS},
         "by": ["attrs.path"], "metrics": ["count"]},
        {"input": {"kind": "nodes", "subtype": "doc",
                   "filter": {"op": "in", "field": "attrs.path", "values": TWO_PATHS}},
         "by": ["attrs.path"], "metrics": ["count"]},
    )
    assert all(path in json.dumps(typed) for path in TWO_PATHS)


def test_expand_where_equals_filter(tools):
    chunks = _equal(
        tools, "expand",
        {"input": {"node_id": "doc:000"}, "edge_type": "contains",
         "where": {"type": "doc_chunk"}},
        {"input": {"node_id": "doc:000"}, "edge_type": "contains",
         "filter": {"op": "eq", "field": "subtype", "value": "doc_chunk"}},
    )
    # Issue #3666: the rows are the linked records.
    assert _ids(chunks, "neighbors", "node_id") == [
        "doc_chunk:000-0", "doc_chunk:000-1", "doc_chunk:000-2",
    ]
    one = _equal(
        tools, "expand",
        {"input": {"node_id": "doc:000"}, "edge_type": "contains",
         "where": {"id": ["doc_chunk:000-1"]}},
        {"input": {"node_id": "doc:000"}, "edge_type": "contains",
         "filter": {"op": "in", "field": "node_id", "values": ["doc_chunk:000-1"]}},
    )
    assert _ids(one, "neighbors", "node_id") == ["doc_chunk:000-1"]
    # Failure case: a far-end type nothing has selects nothing.
    none = _equal(
        tools, "expand",
        {"input": {"node_id": "doc:000"}, "edge_type": "contains", "where": {"type": "ticket"}},
        {"input": {"node_id": "doc:000"}, "edge_type": "contains",
         "filter": {"op": "eq", "field": "subtype", "value": "ticket"}},
    )
    assert none["neighbors"] == []


def test_both_spellings_that_differ_are_refused_before_any_read(tools):
    new, old = tools.pair(
        "filter",
        {"where": {"type": "doc"}, "predicate": PATH_001},
        {"input": {"kind": "nodes", "subtype": "doc"}, "predicate": PATH_001},
    )
    assert new["status"] == "refused"
    assert new["error"]["code"] == "invalid_operator_arguments"
    assert "`where` and `predicate`" in new["error"]["message"]
    assert _ids(old) == ["doc:001"]


# --------------------------------------------------------------------------- #
# served with a note                                                           #
# --------------------------------------------------------------------------- #


def _adjusted(tools: Tools, tool: str, sent: dict, corrected: dict, rules: list[str]) -> dict:
    """Assert `sent` is served as `corrected` plus the note; the reply."""
    new, old = tools.pair(tool, sent, corrected)
    _served(old)
    assert "adjusted" not in old
    assert [record["rule"] for record in new["adjusted"]] == rules, new.get("adjusted")
    assert all(set(record) == {"rule", "sent", "served"} for record in new["adjusted"])
    assert _same(new, without=("adjusted",)) == _same(old), (tool, sent)
    line = next(
        (line for line in tools.text(tool, sent).splitlines() if line.startswith("! adjusted: ")),
        None,
    )
    assert line is not None, tools.text(tool, sent)
    for record in new["adjusted"]:
        assert f"{record['sent']} read as {record['served']}" in line
    assert "! adjusted" not in tools.text(tool, corrected)
    return new


def test_a_clause_without_op_is_served_as_eq_with_a_note(tools):
    filtered = _adjusted(
        tools, "filter",
        {"input": {"kind": "nodes", "subtype": "doc"},
         "predicate": {"field": "attrs.path", "value": "docs/001.md"}},
        {"input": {"kind": "nodes", "subtype": "doc"}, "predicate": PATH_001},
        ["clause_without_op"],
    )
    assert _ids(filtered) == ["doc:001"]
    assert filtered["adjusted"] == [{
        "rule": "clause_without_op",
        "sent": "predicate clause on `attrs.path` with no op",
        "served": "eq",
    }]
    searched = _adjusted(
        tools, "search",
        {"query": TERM, "where": {"field": "type", "value": "ticket"}},
        {"query": TERM, "where": {"op": "eq", "field": "type", "value": "ticket"}},
        ["clause_without_op"],
    )
    assert _ids(searched, "results") == ["ticket:1"]
    # `values` with no `op` has two readings and stays refused, with both.
    (refused,) = tools.run(("filter", {
        "input": {"kind": "nodes", "subtype": "doc"},
        "predicate": {"field": "attrs.path", "values": TWO_PATHS},
    }, "json"))
    error = json.loads(refused.content[0].text)["error"]
    assert refused.isError and error["code"] == "invalid_operator_arguments"
    assert '"op": "in"' in error["fix_hint"] and '"op": "between"' in error["fix_hint"]


def test_arguments_placed_one_level_too_deep_are_lifted_with_a_note(tools):
    node = _adjusted(
        tools, "inspect",
        {"target": {"kind": "node", "node_id": "doc:001", "select": ["attrs"],
                    "max_bytes": 8192}},
        {"target": {"kind": "node", "node_id": "doc:001"}, "select": ["attrs"],
         "max_bytes": 8192},
        ["argument_lifted", "argument_lifted"],
    )
    assert node["adjusted"][0]["sent"] == "select inside target"
    assert node["adjusted"][1]["sent"] == "max_bytes inside target"
    _adjusted(
        tools, "inspect",
        {"target": {"kind": "text", "node_id": "doc:000", "max_bytes": 8192}},
        {"target": {"kind": "text", "node_id": "doc:000"}, "max_bytes": 8192},
        ["argument_lifted"],
    )
    filtered = _adjusted(
        tools, "filter",
        {"input": {"kind": "nodes", "subtype": "doc", "select": ["node_id"],
                   "max_bytes": 8192}, "predicate": PATH_001},
        {"input": {"kind": "nodes", "subtype": "doc"}, "predicate": PATH_001,
         "select": ["node_id"], "max_bytes": 8192},
        ["argument_lifted", "argument_lifted"],
    )
    assert _ids(filtered) == ["doc:001"]
    _adjusted(
        tools, "map",
        {"input": {"kind": "nodes", "subtype": "ticket", "select": ["node_id"]}},
        {"input": {"kind": "nodes", "subtype": "ticket"}, "projection": ["node_id"]},
        ["argument_lifted"],
    )
    _adjusted(
        tools, "aggregate",
        {"input": {"kind": "nodes", "subtype": "ticket", "max_bytes": 8192},
         "by": ["subtype"], "metrics": ["count"]},
        {"input": {"kind": "nodes", "subtype": "ticket"}, "max_bytes": 8192,
         "by": ["subtype"], "metrics": ["count"]},
        ["argument_lifted"],
    )
    # Failure case: beside a different top-level value it is still refused.
    (refused,) = tools.run(("inspect", {
        "target": {"kind": "node", "node_id": "doc:001", "max_bytes": 6000},
        "max_bytes": 8192,
    }, "json"))
    reply = json.loads(refused.content[0].text)
    assert refused.isError and "adjusted" not in reply
    assert "move max_bytes out of target" in reply["error"]["fix_hint"]


def test_target_subtype_on_one_hop_is_refused_only_for_link_rows(tools):
    """Issue #3666: on record rows `target_subtype` is the type condition on
    the far record. On link rows (a `select` of only the link's own names)
    the refusal and its pointer are unchanged."""
    anchor = {"input": {"node_id": "doc:000"}, "edge_type": "contains"}
    typed, where = tools.pair(
        "expand", {**anchor, "target_subtype": "doc_chunk"},
        {**anchor, "where": {"type": "doc_chunk"}},
    )
    assert _same(typed) == _same(_served(where))
    assert len(typed["neighbors"]) == 3
    (refused,) = tools.run(("expand", {
        **anchor, "target_subtype": "doc_chunk", "select": ["edge_id", "dst"],
    }, "json"))
    reply = json.loads(refused.content[0].text)
    assert refused.isError and reply["error"]["code"] == "invalid_expand_shape"
    assert "adjusted" not in reply


def test_an_adjusted_call_pages_like_the_corrected_call_and_keeps_its_note(tools):
    """Page two of an adjusted call holds the rows of page two of the call
    it stands for, and says `adjusted` again: the note belongs to the call,
    not to its first page."""
    scope = {"kind": "nodes", "subtype": "doc"}
    sent = {"input": {**scope, "select": ["node_id"]}, "predicate": {}, "limit": 50}
    corrected = {"input": scope, "select": ["node_id"], "predicate": {}, "limit": 50}
    first, first_old = tools.pair("filter", sent, corrected)
    assert _same(first, without=("adjusted",)) == _same(first_old)
    offset = _served(first)["next_offset"]
    assert isinstance(offset, int) and offset > 0, first
    second, second_old = tools.pair(
        "filter", {**sent, "offset": offset}, {**corrected, "offset": offset},
    )
    assert _same(second, without=("adjusted",)) == _same(second_old)
    assert second["adjusted"] == first["adjusted"] == [{
        "rule": "argument_lifted", "sent": "select inside input", "served": "top-level select",
    }]
    assert "adjusted" not in second_old
    page_one, page_two = _ids(first), _ids(second)
    assert len(page_one) == 50 and page_two and not set(page_one) & set(page_two)


def _wire_bytes(result) -> int:
    return len(result.model_dump_json(by_alias=True, exclude_none=True).encode())


def test_a_page_with_a_note_fits_the_smallest_budget(tools):
    """A page the wrapper pages down, served with a note, in both formats:
    never over `max_bytes`, still holding rows, and never more rows than
    the same page without the note."""
    select = ["node_id", "attrs.summary"]
    sent = {"input": {"kind": "nodes", "select": select, "max_bytes": 4096},
            "predicate": {"field": "subtype", "value": "doc"}, "limit": 200}
    corrected = {"input": {"kind": "nodes"}, "select": select, "max_bytes": 4096,
                 "predicate": {"op": "eq", "field": "subtype", "value": "doc"}, "limit": 200}
    as_json, as_text, plain_json, plain_text = tools.run(
        ("filter", sent, "json"), ("filter", sent, "text"),
        ("filter", corrected, "json"), ("filter", corrected, "text"),
    )
    noted, plain = json.loads(as_json.content[0].text), json.loads(plain_json.content[0].text)
    assert [record["rule"] for record in _served(noted)["adjusted"]] == [
        "argument_lifted", "argument_lifted", "clause_without_op",
    ]
    assert "adjusted" not in _served(plain)
    # Precondition: the set is more than one 4,096-byte page.
    assert plain["truncated"] is True and plain["next_offset"] < 137
    for result in (as_json, as_text, plain_json, plain_text):
        assert not result.isError and _wire_bytes(result) <= 4096
    assert "! adjusted: " in as_text.content[0].text
    assert 1 <= len(noted["items"]) <= len(plain["items"])
    # The rows the note displaced are left for the next page, not lost: the
    # page is a prefix of the page without the note, and it has a cursor.
    rows, plain_rows = ([row["node_id"] for row in page["items"]] for page in (noted, plain))
    assert rows == plain_rows[:len(rows)]
    assert noted["truncated"] is True and isinstance(noted["next_offset"], int)


@pytest.fixture
def long_text_record(clean_runtime, okg_mcp_can_log_in, tmp_path):
    """`doc:big`, the page-next document: three passages of about 2,600
    characters, so a 4,096-byte text page holds about one."""
    from okg.substrate.library.sources.base import EdgeFact
    from tests.substrate.mcp.search_fixture import node
    from tests.substrate.mcp.test_page_next_db import CHUNK, CHUNK_TEXTS
    from tests.substrate.mcp.test_page_next_db import DOC as DOCUMENT

    name = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes = [node("doc:big", "document", "the big document", title="Big")]
    edges = []
    for index, text in enumerate(CHUNK_TEXTS):
        chunk_id = f"doc:big:chunk:{index}"
        nodes.append(node(chunk_id, "document_chunk", text, chunk_index=index,
                          chunk_ordinal=index, chunk_total=len(CHUNK_TEXTS)))
        edges.append(EdgeFact(
            src="doc:big", dst=chunk_id, edge_type="contains",
            source_record_id={"id": f"e{index}"}, source_revision={"v": 1},
        ))
    publish_nodes(
        clean_runtime, name, subtypes=[DOCUMENT, CHUNK],
        narrowings=[("document", "contains", "document_chunk")],
        nodes=nodes, edges=edges,
    )
    return name


def test_a_text_read_with_a_note_fits_the_smallest_budget(clean_runtime, long_text_record):
    """`inspect` text cuts its own page before the note is stamped: the
    note's bytes are reserved, so the served page is inside `max_bytes` and
    still carries text."""
    server = Tools(clean_runtime, long_text_record)
    try:
        sent = {"target": {"kind": "text", "node_id": "doc:big", "max_bytes": 4096}}
        corrected = {"target": {"kind": "text", "node_id": "doc:big"}, "max_bytes": 4096}
        noted_json, noted_text, plain_json = server.run(
            ("inspect", sent, "json"), ("inspect", sent, "text"), ("inspect", corrected, "json"),
        )
    finally:
        server.close()
    noted, plain = json.loads(noted_json.content[0].text), json.loads(plain_json.content[0].text)
    # Precondition: the record's text is more than one page, so the read cut.
    assert _served(plain)["items"] and plain["truncated"] is True
    assert _served(noted)["adjusted"] == [{
        "rule": "argument_lifted", "sent": "max_bytes inside target",
        "served": "top-level max_bytes",
    }]
    assert noted["truncated"] is True and noted.get("continuation_token")
    served_text = "".join(str(item.get("text") or "") for item in noted["items"])
    assert "word0" in served_text, noted
    for result in (noted_json, noted_text, plain_json):
        assert not result.isError and _wire_bytes(result) <= 4096
    assert "! adjusted: max_bytes inside target" in noted_text.content[0].text


# --------------------------------------------------------------------------- #
# access rules                                                                 #
# --------------------------------------------------------------------------- #

_DENY_EXTERNAL = [
    {"rule_id": "deny_external", "effect": "deny", "actions": ["graph.read.mcp"],
     "subjects": [READER], "match": {"source_id": EXTERNAL}, "priority": 200},
    {"rule_id": "allow_read", "effect": "allow",
     "actions": ["graph.read.mcp", "mcp.tool.call"], "subjects": [READER], "priority": 100},
]

_PATH_HIDDEN = {"op": "eq", "field": "attrs.path", "value": "x/y.md"}

#: Each new spelling of "the documents the word `external` finds" and of
#: "the document at path x/y.md", with today's call. `doc:ext` is the one
#: record that matches, and it comes from the source the reader is denied.
#: The last item is the reply key that holds the rows.
_HIDDEN_CASES: list[tuple[str, dict, dict, str]] = [
    ("search",
     {"query": "external", "where": {"type": "doc"}},
     {"query": "external", "where": {"op": "eq", "field": "type", "value": "doc"}},
     "results"),
    ("search",
     {"query": "external", "where": {"field": "type", "value": "doc"}},
     {"query": "external", "where": {"op": "eq", "field": "type", "value": "doc"}},
     "results"),
]

_HIDDEN_FILTER_CASES: list[tuple[str, dict, dict, str]] = [
    ("filter",
     {"where": {"type": "doc", "attrs.path": "x/y.md"}},
     {"input": {"kind": "nodes", "subtype": "doc"}, "predicate": _PATH_HIDDEN},
     "items"),
    ("filter",
     {"input": {"kind": "nodes", "subtype": "doc", "max_bytes": 8192},
      "predicate": {"field": "attrs.path", "value": "x/y.md"}},
     {"input": {"kind": "nodes", "subtype": "doc"}, "max_bytes": 8192,
      "predicate": _PATH_HIDDEN},
     "items"),
]

_HIDDEN_MAP_CASES: list[tuple[str, dict, dict, str]] = [
    ("map",
     {"where": {"type": "doc", "attrs.path": "x/y.md"}, "select": ["node_id", "attrs.path"]},
     {"input": {"kind": "nodes", "subtype": "doc", "filter": _PATH_HIDDEN},
      "projection": ["node_id", "attrs.path"]},
     "items"),
]

_HIDDEN_AGGREGATE = (
    "aggregate",
    {"where": {"type": "doc", "attrs.path": "x/y.md"}, "by": ["attrs.path"],
     "metrics": ["count"]},
    {"input": {"kind": "nodes", "subtype": "doc", "filter": _PATH_HIDDEN},
     "by": ["attrs.path"], "metrics": ["count"]},
)


def _under_enforcement(clean_runtime, monkeypatch, deployment, calls) -> tuple[list, list]:
    """Each `(tool, sent, today)` pair for the denied reader, then with
    enforcement off: `([(new, old), ...], [(new, old), ...])`."""
    _activate(clean_runtime, deployment, _DENY_EXTERNAL)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    server = Tools(clean_runtime, deployment)
    try:
        enforced = [server.pair(tool, sent, today) for tool, sent, today in calls]
        texts = [server.text(tool, sent) for tool, sent, _today in calls]
    finally:
        server.close()
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    monkeypatch.delenv("OKG_NOMOS_SUBJECT", raising=False)
    server = Tools(clean_runtime, deployment)
    try:
        opened = [server.pair(tool, sent, today) for tool, sent, today in calls]
    finally:
        server.close()
    for (tool, sent, _today), (new, old) in zip(calls * 2, enforced + opened):
        # The rewritten call is the call that is checked: this reader gets
        # from it exactly what today's call gives.
        assert _same(new, without=("adjusted",)) == _same(old), (tool, sent)
    return [*enforced, texts], opened


def _hidden_record_check(clean_runtime, tmp_path, monkeypatch, cases) -> None:
    deployment = _corpus(tmp_path, clean_runtime)
    enforced, opened = _under_enforcement(
        clean_runtime, monkeypatch, deployment,
        [(tool, sent, today) for tool, sent, today, _ in cases],
    )
    for (tool, _sent, _today, rows), (_new, old) in zip(cases, enforced):
        assert _served(old)[rows] == [], (tool, old[rows])
    served_bytes = json.dumps(enforced)
    assert "doc:ext" not in served_bytes and "Ignore previous" not in served_bytes
    # Failure case: with enforcement off the same calls return the record,
    # so its absence above comes from the policy.
    for (tool, sent, _today, rows), (new, _old) in zip(cases, opened):
        assert [row["node_id"] for row in _served(new)[rows]] == ["doc:ext"], (tool, sent)


def test_a_hidden_record_is_not_reached_through_a_new_search_spelling(
        clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """Under access enforcement a short-form and an adjusted `search` return
    exactly what today's call returns a reader who cannot see one source."""
    _hidden_record_check(clean_runtime, tmp_path, monkeypatch, _HIDDEN_CASES)


def test_a_hidden_record_is_not_reached_through_a_new_filter_spelling(
        clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """The same for `filter`: a top-level `where` with the type in it, and a
    call adjusted twice (a lifted `max_bytes`, a clause with no `op`)."""
    _hidden_record_check(clean_runtime, tmp_path, monkeypatch, _HIDDEN_FILTER_CASES)


def test_a_hidden_record_is_not_reached_through_map_where(
        clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """`map` with a top-level `where` and `select` serves the denied reader
    the rows `input.filter` and `projection` serve: none."""
    _hidden_record_check(clean_runtime, tmp_path, monkeypatch, _HIDDEN_MAP_CASES)


def test_a_hidden_record_is_not_counted_through_aggregate_where(
        clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """`aggregate` with a top-level `where` answers the denied reader as the
    `input.filter` call does (today a refusal under access rules, never a
    count), and counts the record once enforcement is off."""
    deployment = _corpus(tmp_path, clean_runtime)
    whole_type = {"input": {"kind": "nodes", "subtype": "doc"}, "by": ["attrs.path"],
                  "metrics": ["count"]}
    enforced, opened = _under_enforcement(
        clean_runtime, monkeypatch, deployment,
        [_HIDDEN_AGGREGATE, ("aggregate", whole_type, whole_type)],
    )
    (new, old), (unfiltered, _), _texts = enforced
    assert "doc:ext" not in json.dumps(enforced)
    counted = [row for row in (old.get("items") or old.get("groups") or [])
               if row.get("count")]
    assert counted == [], old
    # The `where` is part of the call that was checked: the reply is not the
    # reply to the same call without it.
    assert _same(new) != _same(unfiltered)
    (new, _old), (unfiltered, _) = opened
    assert "x/y.md" in json.dumps(_served(new)) and '"count": 1' in json.dumps(new)
    # Without the `where` the same count covers many paths, not this one.
    groups = _served(unfiltered).get("groups") or unfiltered.get("items") or []
    assert len(groups) > 1 and _same(new) != _same(unfiltered)


def test_a_hidden_far_end_is_not_reached_through_expand_where(
        clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """One passage has two records on the other end of its link: one the
    reader may see and one it may not. A `where` that admits only the hidden
    one serves nothing, and a `where` that admits only the visible one
    serves it: the condition is applied, and it never reaches the hidden
    record."""
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment, subtypes=[DOC, DOC_CHUNK],
        narrowings=[("doc", "contains", "doc_chunk")],
        nodes=[
            _node("doc:open", "doc", "an open record", kind="visible-kind"),
            _node("doc_chunk:shared-0", "doc_chunk", "a passage of both records"),
        ],
        edges=[_edge("doc:open", "doc_chunk:shared-0"),
               _edge("doc:secret", "doc_chunk:shared-0")],
        node_groups=[(EXTERNAL, [
            _node("doc:secret", "doc", "the hidden record itself", kind="hidden-kind"),
        ])],
    )
    _trust(clean_runtime, deployment, MAIN)

    def calls(kind: str) -> tuple[str, dict, dict]:
        anchor = {"input": {"node_id": "doc_chunk:shared-0"}, "edge_type": "contains",
                  "direction": "in"}
        return (
            "expand",
            {**anchor, "where": {"type": "doc", "attrs.kind": kind}},
            {**anchor, "filter": {"op": "and", "clauses": [
                {"op": "eq", "field": "subtype", "value": "doc"},
                {"op": "eq", "field": "attrs.kind", "value": kind},
            ]}},
        )

    enforced, opened = _under_enforcement(
        clean_runtime, monkeypatch, deployment,
        [calls("hidden-kind"), calls("visible-kind")],
    )
    (_hidden_new, hidden_old), (_open_new, open_old), _texts = enforced
    # Admits only the hidden far end: nothing, though a visible far end is
    # on the same link and would be served if the condition were dropped.
    assert _served(hidden_old)["neighbors"] == []
    assert "doc:secret" not in json.dumps(enforced)
    # Admits only the visible far end: exactly that one.
    assert [row["node_id"] for row in _served(open_old)["neighbors"]] == ["doc:open"]
    # Failure case: with enforcement off each condition admits its record.
    (hidden_new, _), (open_new, _) = opened
    assert [row["node_id"] for row in _served(hidden_new)["neighbors"]] == ["doc:secret"]
    assert [row["node_id"] for row in _served(open_new)["neighbors"]] == ["doc:open"]


# --------------------------------------------------------------------------- #
# step 3a: one `select` vocabulary per tool                                    #
# --------------------------------------------------------------------------- #

#: An attribute lookup needs a condition on an attribute: one on the type
#: alone is lifted into the type filter and the lookup refuses what is left.
DOC_ATTRS = {"op": "in", "field": "attrs.summary",
             "values": [f"Document {i:03d}" for i in range(8)]}
TYPE_DOC = {"op": "eq", "field": "type", "value": "doc"}

#: Shapes agents really sent and were refused (the replay behind this step,
#: with neutral values), one per (tool, method or view) class the pure count
#: in test_read_grammar.py calls served: the tool, the call, the same call
#: selecting only the fields its rows have, the reply key that holds the
#: rows, and the selected names that must be on no row. Dispatching them is
#: what shows the server serves them; the pure count is an upper bound.
_SELECT_SERVED = [
    ("search", {"query": TERM, "limit": 5,
                "select": ["node_id", "excerpt", "location", "match_windows"]},
     ["node_id", "excerpt", "location"], "results", ["match_windows"]),
    ("search", {"query": TERM, "method": "grep", "limit": 5,
                "select": ["node_id", "subtype", "handle", "excerpt", "score"]},
     ["node_id", "subtype", "handle", "excerpt"], "results", ["score"]),
    ("search", {"query": TERM, "method": "grep", "limit": 5,
                "select": ["node_id", "subtype", "handle", "excerpt", "source_lineage"]},
     ["node_id", "subtype", "handle", "excerpt"], "results", ["source_lineage"]),
    ("search", {"where": DOC_ATTRS, "limit": 5, "select": ["node_id", "handle", "excerpt"]},
     ["node_id", "handle"], "results", ["excerpt"]),
    ("search", {"where": DOC_ATTRS, "method": "attrs", "limit": 5,
                "select": ["node_id", "attrs", "source_lineage"]},
     ["node_id", "attrs"], "results", ["source_lineage"]),
    ("search", {"where": {"op": "eq", "field": "id", "value": "doc:000"}, "method": "attrs",
                "select": ["node_id", "attrs", "source_lineage"]},
     ["node_id", "attrs"], "results", ["source_lineage"]),
    ("search", {"query": TERM, "view": "list", "limit": 5,
                "select": ["node_id", "handle", "excerpt"]},
     ["node_id", "handle"], "results", ["excerpt"]),
]


@pytest.mark.parametrize("case", range(len(_SELECT_SERVED)))
def test_a_select_naming_another_shapes_field_is_served_with_the_fields_its_rows_have(
        tools, case):
    """Served, with the note, and row for row the reply of the same call
    selecting only the fields its rows have. A field left out is on no row
    and named in the note, in JSON and in text; a field the rows do have
    keeps whatever it held, empty or not, because the rows are the same."""
    tool, sent, kept, rows, left_out = _SELECT_SERVED[case]
    corrected = {**sent, "select": kept}
    reply = _adjusted(tools, tool, sent, corrected, ["select_not_returned"])
    (record,) = reply["adjusted"]
    assert record["sent"] == "select " + ", ".join(f"`{name}`" for name in left_out)
    assert record["served"].startswith("not returned, ")
    if sent.get("method") != "identifier_mentions":
        # (The corpus declares no identifier pattern, so that page is empty.)
        assert reply[rows], reply
    for row in reply[rows]:
        assert not set(left_out) & set(row), row
    text, plain = tools.text(tool, sent), tools.text(tool, corrected)
    noted = [line for line in text.splitlines() if line.startswith("! adjusted: ")]
    assert len(noted) == 1 and all(f"`{name}`" in noted[0] for name in left_out)
    # No placeholder for a field left out: the row lines are the corrected call's.
    def cards(body: str) -> list[str]:
        return [line for line in body.splitlines() if line.startswith("[")]

    assert cards(text) == cards(plain)


def _refused_as(tools: Tools, tool: str, arguments: dict, code: str) -> dict:
    (result,) = tools.run((tool, arguments, "json"))
    reply = json.loads(result.content[0].text)
    assert result.isError and reply["status"] == "refused", reply
    assert reply["error"]["code"] == code, reply["error"]
    assert "adjusted" not in reply
    return reply


@pytest.mark.parametrize("tool,arguments,code", [
    # Nothing selected is a field of this call's rows.
    ("search", {"query": TERM, "method": "grep", "select": ["score"]}, "select_unknown_field"),
    ("search", {"query": TERM, "method": "grep", "select": ["source_lineage"]},
     "source_lineage_select_unsupported"),
    ("search", {"where": DOC_ATTRS, "select": ["excerpt"]}, "select_unknown_field"),
    # No condition at all: the lookup's own refusal, untouched. (A type
    # alone lists that type since step 2b.)
    ("search", {"method": "attrs", "select": ["node_id", "handle"]},
     "invalid_operator_arguments"),
    ("expand", {"input": {"node_id": "doc:000"}, "depth": 2, "edge_type": "contains",
                "select": ["src"]}, "select_unknown_field"),
    # A name that is a field of no shape of the tool, beside ones that are
    # (on search the call without it is offered: the test below).
    ("expand", {"input": {"node_id": "doc:000"}, "select": ["label", "dst"]},
     "select_unknown_field"),
    # (A dotted attribute is a field of a linked record's row since lane RD:
    # the lane RD section of test_read_grammar_db.py serves it.)
])
def test_a_select_the_rows_cannot_answer_is_refused_as_before(tools, tool, arguments, code):
    error = _refused_as(tools, tool, arguments, code)["error"]
    assert "retry_call" not in error, error
    if code == "source_lineage_select_unsupported":
        assert "method='lexical' or 'semantic'" in error["message"]
        return
    if code == "invalid_operator_arguments":
        assert "this call has no condition" in error["message"]
        return
    # The words name what this call's rows do have, or where the field is.
    assert error["fix_hint"] and "select" in error["fix_hint"], error


def test_identifier_mentions_refusal_says_spans_and_names_the_record_methods(tools):
    reply = _refused_as(tools, "search", {
        "query": "see doc:000", "method": "identifier_mentions", "select": ["node_id", "subtype"],
    }, "select_unknown_field")
    error = reply["error"]
    # The fact once, in the message; the action, with the caller's own
    # query where a placeholder stood, in the hint.
    assert 'search(method="grep", query="see doc:000")' in error["fix_hint"]
    assert "spans of your query text" not in error["fix_hint"]
    assert "node_id, subtype: identifier_mentions returns spans" in error["message"]
    assert json.dumps(reply).count("spans of your query text") == 1
    assert all(f"'{name}'" in error["fix_hint"]
               for name in ("id_type", "value", "span_start", "span_end"))


@pytest.mark.parametrize("arguments,expected", [
    ({"input": {"node_id": "doc:000"}, "edge_type": "contains", "select": ["node_id", "subtype"]},
     ["doc_chunk:000-0", "doc_chunk:000-1", "doc_chunk:000-2"]),
    ({"input": {"node_id": "doc_chunk:000-1"}, "edge_type": "contains", "direction": "in",
      "select": ["node_id"]}, ["doc:000"]),
    # With an order, through the effective-attribute page.
    ({"input": {"node_id": "doc:000"}, "edge_type": "contains", "order_by": "node_id",
      "select": ["node_id"]},
     ["doc_chunk:000-0", "doc_chunk:000-1", "doc_chunk:000-2"]),
])
def test_one_hop_select_of_the_neighbour_is_served(tools, arguments, expected):
    """Issue #3666: these calls were refused with a call naming `dst` or
    `src`. A 1-hop row is now the linked record, so they are served."""
    (result,) = tools.run(("expand", arguments, "json"))
    reply = _served(json.loads(result.content[0].text))
    assert "adjusted" not in reply
    assert [row["node_id"] for row in reply["neighbors"]] == expected
    for row in reply["neighbors"]:
        assert set(row) == {"node_id", "subtype", "title", "link", "link_count"}


def test_a_second_name_in_select_is_refused_with_the_call_that_names_the_field(tools):
    sent = {"query": TERM, "limit": 3, "select": ["id", "excerpt"]}
    reply = _refused_as(tools, "search", sent, "select_unknown_field")
    assert reply["error"]["retry_call"] == {
        "tool": "search", "mode": "corrected",
        "arguments": {**sent, "select": ["node_id", "excerpt"]},
    }
    served = _dispatched(tools, reply, sent)
    assert len(served["results"]) == 3 and all(row["node_id"] for row in served["results"])
    # A name with no single field is dropped from the offered call, as the
    # refusal's words drop it (#3666).
    other = _refused_as(tools, "search", {**sent, "select": ["snippet", "excerpt"]},
                        "select_unknown_field")
    assert other["error"]["retry_call"]["arguments"]["select"] == ["excerpt"]
    _dispatched(tools, other, {**sent, "select": ["snippet", "excerpt"]})


#: The selects agents sent after M10 (X2 board e3mcp, comp-ops 42: title
#: 142, source 132, path 72 times), on this corpus's query, and the select
#: each is served as.
_WHERE_NAMES_SENT = [
    (["node_id", "subtype", "title", "excerpt", "source", "attrs"],
     ["node_id", "subtype", "handle", "excerpt", "provenance", "attrs"]),
    (["node_id", "subtype", "title", "path", "excerpt", "source"],
     ["node_id", "subtype", "handle", "excerpt", "provenance"]),
    (["node_id", "subtype", "source", "title", "path", "excerpt", "score"],
     ["node_id", "subtype", "provenance", "handle", "excerpt", "score"]),
]


@pytest.mark.parametrize("method", [None, "grep"])
@pytest.mark.parametrize("case", range(len(_WHERE_NAMES_SENT)))
def test_title_path_and_source_in_select_are_served_from_the_field_holding_them(
        tools, case, method):
    """Lenient yes: each name means one row field (`handle` holds a record's
    title, path and date; `provenance` its source), so the call is served
    as that call, with a note in JSON and in text."""
    sent_select, served_select = _WHERE_NAMES_SENT[case]
    base = {"query": TERM, "limit": 5, **({"method": method} if method else {})}
    if method == "grep":
        # grep rows have no score: that one is left out with its own note.
        served_select = [name for name in served_select if name != "score"]
    sent, corrected = {**base, "select": sent_select}, {**base, "select": served_select}
    rules = ["select_renamed"] * (2 if "source" in sent_select else 1)
    if method == "grep" and "score" in sent_select:
        rules.append("select_not_returned")
    reply = _adjusted(tools, "search", sent, corrected, rules)
    assert reply["results"], reply
    for row in reply["results"]:
        assert "title" not in row and "path" not in row
        assert "source" in row["provenance"] and "handle" in row, row
    noted = {record["served"] for record in reply["adjusted"]}
    assert "`handle`, which holds the title, path and date" in noted
    if "source" in sent_select:
        assert "`provenance`, which holds the source and trust" in noted


@pytest.mark.parametrize("sent_select,offered", [
    # A name no row field holds: refused, naming the fields, with the call.
    (["node_id", "subtype", "title", "excerpt", "created"],
     ["node_id", "subtype", "handle", "excerpt"]),
    (["node_id", "created"], ["node_id"]),
])
def test_a_name_no_row_field_holds_is_refused_with_an_exact_retry_call(
        tools, sent_select, offered):
    sent = {"query": TERM, "limit": 3, "select": sent_select}
    reply = _refused_as(tools, "search", sent, "select_unknown_field")
    error = reply["error"]
    assert "created" in error["message"] and "title" not in error["data"]["valid_fields"]
    assert "handle" in error["data"]["valid_fields"]
    assert error["retry_call"] == {
        "tool": "search", "mode": "corrected", "arguments": {**sent, "select": offered},
    }
    assert error["data"]["retry_select"] == offered
    served = _dispatched(tools, reply, sent)
    assert len(served["results"]) == 3


def test_a_where_name_on_a_row_without_its_field_is_refused_naming_the_fields(tools):
    """Failure case: a grep list row has no `provenance` field to select
    (PACT read-names-list-3666: it has a `handle` now, so `title` is served
    as it), so `source` means nothing there. Refused, in words that list
    the row's fields; a view call is never offered (the offer check cannot
    prove one)."""
    sent = {"query": TERM, "method": "grep", "view": "list", "limit": 3,
            "select": ["node_id", "source"]}
    error = _refused_as(tools, "search", sent, "select_unknown_field")["error"]
    assert "select from first_match, handle, matches, node_id, parent, subtype" in error["fix_hint"]
    assert "retry_call" not in error


def test_a_second_name_on_one_hop_gets_words_only_beside_an_order(tools):
    reply = _refused_as(tools, "expand", {
        "input": {"node_id": "doc:000"}, "order_by": "node_id", "select": ["id"],
    }, "select_unknown_field")
    # `order_by` is an argument only the read can judge: no call is offered.
    assert "retry_call" not in reply["error"]
    assert "node_id" in reply["error"]["fix_hint"]


def test_a_grep_list_takes_a_select_and_lists_the_same_records_in_the_same_order(tools):
    base = {"query": TERM, "method": "grep", "view": "list", "limit": 7}
    whole, cut, mixed = (
        json.loads(result.content[0].text) for result in tools.run(
            ("search", base, "json"),
            ("search", {**base, "select": ["node_id", "matches"]}, "json"),
            ("search", {**base, "select": ["node_id", "first_match", "excerpt"]}, "json"),
        )
    )
    ids = [row["node_id"] for row in _served(whole)["results"]]
    assert len(ids) == 7
    page_facts = ("total_count", "total_count_basis", "next_offset", "truncated",
                  "matches_basis", "view")
    for page in (cut, mixed):
        assert [row["node_id"] for row in _served(page)["results"]] == ids
        # Counts, bounds and the continuation are the unprojected page's.
        assert [page.get(key) for key in page_facts] == [whole.get(key) for key in page_facts]
    # Every selected field is a key of the fixed row: no note.
    assert "adjusted" not in cut
    kept = {"node_id", "subtype", "matches", "unit", "provenance", "read_provenance"}
    for full, row in zip(whole["results"], cut["results"]):
        assert "first_match" not in row and row["matches"] == full["matches"]
        assert row == {key: value for key, value in full.items() if key in kept}
    assert [record["sent"] for record in mixed["adjusted"]] == ["select `excerpt`"]
    for full, row in zip(whole["results"], mixed["results"]):
        assert "matches" not in row and "excerpt" not in row
        assert row.get("first_match") == full.get("first_match")
    text = tools.text("search", {**base, "select": ["node_id", "matches"]})
    listed = [line for line in text.splitlines() if line.startswith("[")]
    assert len(listed) == 7 and all(" match" in line for line in listed), text
    assert " \u00b7 first " not in text and "! adjusted" not in text
    # Failure case: nothing selected is a key of the row.
    error = _refused_as(
        tools, "search", {**base, "select": ["excerpt", "score"]}, "select_unknown_field",
    )["error"]
    assert "view list on method grep returns first_match, handle, matches" in error["message"]
    assert "select from first_match, handle, matches, node_id, parent, subtype" in error["fix_hint"]


def test_a_windows_page_takes_a_select_and_lists_the_same_records_in_the_same_order(tools):
    from okg.substrate.mcp.record_window_search import WINDOW_ROW_FIELDS

    base = {"query": TERM, "method": "grep", "view": "windows", "limit": 4}
    whole, cut, mixed = (
        json.loads(result.content[0].text) for result in tools.run(
            ("search", base, "json"),
            ("search", {**base, "select": ["node_id", "windows"]}, "json"),
            ("search", {**base, "select": ["node_id", "match_count", "handle"]}, "json"),
        )
    )
    rows = _served(whole)["results"]
    assert rows and all(set(row) <= set(WINDOW_ROW_FIELDS) for row in rows), rows
    ids = [row["node_id"] for row in rows]
    page_facts = ("total_count", "total_count_basis", "window_count", "match_count", "view")
    for page in (cut, mixed):
        assert [row["node_id"] for row in _served(page)["results"]] == ids
        assert [page.get(key) for key in page_facts] == [whole.get(key) for key in page_facts]
    assert "adjusted" not in cut
    # What says a record's windows may not be whole is kept with any select.
    always = {"node_id", "subtype", "decided", "refusal", "masked_occurrences",
              "continuity", "source_coverage", "passages"}
    for full, row in zip(rows, cut["results"]):
        assert row == {k: v for k, v in full.items() if k in always | {"windows"}}
        assert row["windows"]
    assert [record["sent"] for record in mixed["adjusted"]] == ["select `handle`"]
    for full, row in zip(rows, mixed["results"]):
        assert row == {k: v for k, v in full.items() if k in always | {"match_count"}}


def test_an_alias_page_serves_the_record_id_as_node_id_when_selected(tools):
    plain, named = (
        json.loads(result.content[0].text) for result in tools.run(
            ("search", {"query": "zr-alias", "method": "alias"}, "json"),
            ("search", {"query": "zr-alias", "method": "alias",
                        "select": ["node_id", "alias", "canonical_node_id"]}, "json"),
        )
    )
    # The default alias row is unchanged: it does not gain the key.
    assert _served(plain)["results"] and all("node_id" not in row for row in plain["results"])
    assert "adjusted" not in plain
    (row,) = _served(named)["results"]
    assert row["node_id"] == row["canonical_node_id"] == "doc:000"
    # Simply a field the row has: no note.
    assert "adjusted" not in named


def _page_rows(page: dict) -> list[dict]:
    return _served(page)["results" if "results" in page else "neighbors"]


def _continued(tool: str, sent: dict, page: dict) -> dict:
    """The call for the page after `page`, as the page itself says it."""
    token = page.get("next") or page.get("continuation_token")
    if tool == "expand" and page.get("next"):
        return {"next": token}
    if token:
        return {**sent, "continuation_token": token}
    assert isinstance(page.get("next_offset"), int), {
        key: page.get(key) for key in ("next", "continuation_token", "next_offset")}
    return {**sent, "offset": page["next_offset"]}


#: A paged shape, a call whose `select` names a field its rows lack, the
#: same call without that field, and the field.
_PAGED_SELECT = [
    ("search", {"query": TERM, "method": "grep", "limit": 3},
     ["node_id", "excerpt", "score"], ["node_id", "excerpt"], "score"),
    ("search", {"query": TERM, "method": "grep", "view": "list", "limit": 3},
     ["node_id", "matches", "excerpt"], ["node_id", "matches"], "excerpt"),
    ("search", {"query": TERM, "method": "grep", "view": "windows", "limit": 1,
                "scope": {"records": ["doc:000", "doc:001", "doc:002"]}},
     ["node_id", "windows", "handle"], ["node_id", "windows"], "handle"),
    ("search", {"query": TERM, "view": "list", "limit": 3},
     ["node_id", "handle", "excerpt"], ["node_id", "handle"], "excerpt"),
    ("expand", {"input": {"node_id": "doc:000"}, "path": ["contains:out"], "limit": 2},
     ["node_id", "subtype", "dst"], ["node_id", "subtype"], "dst"),
]


@pytest.mark.parametrize("case", range(len(_PAGED_SELECT)))
def test_page_two_of_a_served_select_has_the_keys_and_the_note_of_page_one(tools, case):
    """A continued page is planned as its first page was: the same fields,
    the same note, and the rows the narrower call's second page holds."""
    tool, base, mixed, narrow, left_out = _PAGED_SELECT[case]
    sent, plain = {**base, "select": mixed}, {**base, "select": narrow}
    first, first_plain = tools.pair(tool, sent, plain)
    assert [record["rule"] for record in _served(first)["adjusted"]] == ["select_not_returned"]
    assert "adjusted" not in _served(first_plain)
    assert _page_rows(first) == _page_rows(first_plain) and _page_rows(first)
    second, second_plain = tools.pair(
        tool, _continued(tool, sent, first), _continued(tool, plain, first_plain),
    )
    # Page two continues where page one stopped, exactly as the narrower call does.
    assert _page_rows(second) == _page_rows(second_plain) and _page_rows(second)
    one = [row["node_id"] for row in _page_rows(first)]
    two = [row["node_id"] for row in _page_rows(second)]
    assert not set(one) & set(two)
    assert second["adjusted"] == first["adjusted"]
    assert "adjusted" not in second_plain
    for row in _page_rows(first) + _page_rows(second):
        assert left_out not in row
    assert {key for row in _page_rows(second) for key in row} <= {
        key for row in _page_rows(first) for key in row} | set(narrow)


def test_a_select_note_is_counted_inside_the_smallest_budget(tools):
    sent = {"query": TERM, "method": "grep", "limit": 100, "max_bytes": 4096,
            "select": ["node_id", "subtype", "handle", "excerpt", "score"]}
    corrected = {**sent, "select": ["node_id", "subtype", "handle", "excerpt"]}
    as_json, as_text, plain_json = tools.run(
        ("search", sent, "json"), ("search", sent, "text"), ("search", corrected, "json"),
    )
    noted, plain = json.loads(as_json.content[0].text), json.loads(plain_json.content[0].text)
    assert [record["rule"] for record in _served(noted)["adjusted"]] == ["select_not_returned"]
    for result in (as_json, as_text, plain_json):
        assert not result.isError and _wire_bytes(result) <= 4096
    assert 1 <= len(noted["results"]) <= len(_served(plain)["results"])
    assert "! adjusted: select `score`" in as_text.content[0].text


def test_a_hidden_record_is_not_reached_through_a_served_select(
        clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """Under access enforcement a call served with fields left out returns a
    reader who cannot see one source exactly what the narrower call does."""
    where = {"op": "eq", "field": "type", "value": "doc"}
    _hidden_record_check(clean_runtime, tmp_path, monkeypatch, [
        ("search",
         {"query": "external", "where": where,
          "select": ["node_id", "excerpt", "match_windows"]},
         {"query": "external", "where": where, "select": ["node_id", "excerpt"]},
         "results"),
        ("search",
         {"query": "external", "method": "grep", "where": where,
          "select": ["node_id", "excerpt", "score", "source_lineage"]},
         {"query": "external", "method": "grep", "where": where,
          "select": ["node_id", "excerpt"]},
         "results"),
    ])


@pytest.mark.parametrize("tool,arguments", [
    ("expand", {"input": {"node_id": "doc:000"}, "depth": 2, "edge_type": "contains",
                "select": ["source_lineage", "edge_type"]}),
    ("search", {"query": TERM, "select": ["source_lineage", "id_type"]}),
    ("search", {"query": TERM, "method": "semantic", "select": ["source_lineage", "id_type"]}),
])
def test_a_select_that_would_leave_only_the_lineage_token_stays_refused(tools, tool, arguments):
    """Alone, the token means the default row plus the lineage: served, the
    call would return fields that were never selected."""
    (result,) = tools.run((tool, arguments, "json"))
    reply = json.loads(result.content[0].text)
    assert result.isError and reply["status"] == "refused", reply
    assert "adjusted" not in reply
    assert "results" not in reply and "neighbors" not in reply
    if arguments.get("method") != "semantic":  # no embedder here: refused earlier
        assert reply["error"]["code"] == "select_unknown_field", reply["error"]


@pytest.mark.parametrize("select", [["Attrs", "node_id"], ["SOURCE_LINEAGE", "node_id"]])
def test_a_case_variant_of_a_record_field_gets_a_retry_that_serves_the_record(tools, select):
    """Issue #3666: a 1-hop row is the linked record, so the corrected call
    is right. It is offered, and sent as offered it serves the far record:
    its node_id and its own attributes, never the link's."""
    sent = {"input": {"node_id": "doc:000"}, "edge_type": "contains", "select": select}
    reply = _refused_as(tools, "expand", sent, "select_unknown_field")
    offered = reply["error"]["retry_call"]
    assert offered["tool"] == "expand" and offered["mode"] == "corrected"
    assert offered["arguments"] == {**sent, "select": [name.lower() for name in select]}
    served = _dispatched(tools, reply, sent)
    rows = served["neighbors"]
    assert [row["node_id"] for row in rows] == [
        "doc_chunk:000-0", "doc_chunk:000-1", "doc_chunk:000-2",
    ]
    for row in rows:
        assert "edge_id" not in row and row["link"]["edge_type"] == "contains"
        if "attrs" in offered["arguments"]["select"]:
            # The passage's own text: a record attribute, not a link's.
            assert "passage" in json.dumps(row["attrs"])
        else:
            assert "source_lineage" in row and "attrs" not in row


def test_an_empty_select_on_fixed_rows_is_the_page_without_a_select(tools):
    for base in (
        {"query": TERM, "method": "grep", "view": "list", "limit": 3},
        {"query": TERM, "method": "grep", "view": "windows", "limit": 2},
    ):
        empty, plain = tools.pair("search", {**base, "select": []}, base)
        assert "adjusted" not in _served(empty)
        assert _same(empty) == _same(_served(plain)), base
    # Failure case: where the rows are projected by the tool it is refused.
    (result,) = tools.run(("search", {"query": TERM, "view": "list", "select": []}, "json"))
    assert result.isError


def test_a_refused_next_call_does_not_carry_the_first_pages_select_note(tools):
    sent = {"input": {"node_id": "doc:000"}, "path": ["contains:out"], "limit": 2,
            "select": ["node_id", "subtype", "dst"]}
    (result,) = tools.run(("expand", sent, "json"))
    first = _served(json.loads(result.content[0].text))
    assert first["adjusted"]
    token = first.get("next") or first.get("continuation_token")
    assert token, first
    refused, served = (
        json.loads(reply.content[0].text) for reply in tools.run(
            ("expand", {"next": token, "limit": 5}, "json"),
            ("expand", {"next": token}, "json"),
        )
    )
    # Refused for the argument beside `next`, not for the select.
    assert refused["status"] == "refused" and "adjusted" not in refused, refused
    assert _served(served)["adjusted"] == first["adjusted"]


@pytest.mark.parametrize("select,grep_fields", [
    (["node_id", "value", "id_type", "evidence_kind"], ["node_id", "evidence_kind"]),
    (["node_id", "subtype", "handle", "excerpt", "value", "id_type"],
     ["node_id", "subtype", "handle", "excerpt"]),
])
def test_records_asked_of_identifier_mentions_are_refused_with_the_grep_call(
        tools, select, grep_fields):
    """The replayed selects: span fields and record fields together. Served
    with the span fields the page would be empty and read as "no match"; it
    is refused, and the offered call finds the records holding the text."""
    sent = {"query": "doc:000", "method": "identifier_mentions", "limit": 5, "select": select}
    reply = _refused_as(tools, "search", sent, "select_unknown_field")
    error = reply["error"]
    assert "spans of your query text" in error["message"]
    assert error["retry_call"] == {
        "tool": "search", "mode": "guess",
        "arguments": {"query": "doc:000", "method": "grep", "limit": 5, "select": grep_fields},
    }
    # One answer: the refusal's own narrowed select is withdrawn.
    assert error["data"].get("retry_select") is None
    assert "retry with select" not in error["fix_hint"]
    served = _dispatched(tools, reply, sent)
    assert "results" in served
    for row in served["results"]:
        assert not {"value", "id_type"} & set(row)


#: One call of every shape the corpus serves, and fields only other shapes return.
_FOREIGN_ONLY_SERVED = [
    ("search", {"query": TERM}, ["match_windows", "id_type"]),
    ("search", {"query": TERM, "method": "grep"}, ["score", "distance"]),
    ("search", {"where": DOC_ATTRS}, ["excerpt", "score"]),
    ("search", {"query": "zr-alias", "method": "alias"}, ["score", "attrs"]),
    ("search", {"query": "doc:000", "method": "identifier_mentions"}, ["node_id", "excerpt"]),
    ("search", {"query": TERM, "view": "list"}, ["excerpt", "score"]),
    ("search", {"query": TERM, "method": "grep", "view": "list"}, ["excerpt", "score"]),
    ("search", {"query": TERM, "method": "grep", "view": "windows"}, ["excerpt", "handle"]),
    # No 1-hop expand row: its `select` picks its shape (#3666), so record
    # fields are served as the linked record's.
    ("expand", {"input": {"node_id": "doc:000"}, "depth": 2, "edge_type": "contains"},
     ["dst", "edge_type"]),
]


@pytest.mark.parametrize("case", range(len(_FOREIGN_ONLY_SERVED)))
def test_a_select_of_only_another_shapes_fields_is_refused_never_served_empty(tools, case):
    tool, base, foreign = _FOREIGN_ONLY_SERVED[case]
    (plain,) = tools.run((tool, base, "json"))
    assert not plain.isError, plain.content[0].text[:400]
    (result,) = tools.run((tool, {**base, "select": foreign}, "json"))
    reply = json.loads(result.content[0].text)
    assert result.isError and reply["status"] == "refused", reply
    assert not {"results", "neighbors", "mentions", "adjusted"} & set(reply), reply
    assert reply["error"]["code"] in ("select_unknown_field", "search_view_unsupported"), (
        reply["error"])


def test_a_refused_select_and_its_offered_call_say_the_same_select(tools):
    sent = {"query": TERM, "limit": 3, "select": ["SCORE", "node_id"]}
    error = _refused_as(tools, "search", sent, "select_unknown_field")["error"]
    assert error["retry_call"]["arguments"]["select"] == ["score", "node_id"]
    assert error["data"]["retry_select"] == ["score", "node_id"]
    assert "retry with select=['score', 'node_id']" in error["fix_hint"]
    # A 1-hop expand serves record fields (#3666); a letter-case variant
    # is still refused, and its two answers agree on the record fields.
    hop = _refused_as(tools, "expand", {
        "input": {"node_id": "doc:000"}, "edge_type": "contains", "select": ["Node_ID", "subtype"],
    }, "select_unknown_field")["error"]
    assert hop["retry_call"]["arguments"]["select"] == hop["data"]["retry_select"] == [
        "node_id", "subtype",
    ]


#: A fixed-row view paged with a select, and the keys that select keeps.
_FIXED_PAGED = [
    ({"query": TERM, "method": "grep", "view": "list", "limit": 3},
     ["node_id", "matches"], ["node_id", "first_match"], "matches", "first_match"),
    ({"query": TERM, "method": "grep", "view": "windows", "limit": 1,
      "scope": {"records": ["doc:000", "doc:001", "doc:002"]}},
     ["node_id", "windows"], ["node_id", "match_count"], "windows", "match_count"),
]


@pytest.mark.parametrize("case", range(len(_FIXED_PAGED)))
def test_page_two_of_a_fixed_row_view_is_cut_by_its_own_select(tools, case):
    """The continuation of a fixed-row view does not sign the `select` (it
    never reaches the read that issues the token). Page two sent with another
    select, or with none, is served: the same records as the plain page two,
    cut by the select of that call. Each page is consistent with its own call."""
    base, first_select, other_select, first_key, other_key = _FIXED_PAGED[case]
    (one, plain_one) = tools.pair("search", {**base, "select": first_select}, base)
    assert all(first_key in row and other_key not in row for row in _served(one)["results"])
    ids_one = [row["node_id"] for row in one["results"]]
    assert ids_one == [row["node_id"] for row in _served(plain_one)["results"]] and ids_one
    plain_next = _continued("search", base, plain_one)
    same, other, none, plain_two = (
        json.loads(result.content[0].text) for result in tools.run(
            ("search", _continued("search", {**base, "select": first_select}, one), "json"),
            ("search", _continued("search", {**base, "select": other_select}, one), "json"),
            ("search", _continued("search", base, one), "json"),
            ("search", plain_next, "json"),
        )
    )
    ids_two = [row["node_id"] for row in _served(plain_two)["results"]]
    assert ids_two and not set(ids_one) & set(ids_two)
    for page in (same, other, none):
        assert [row["node_id"] for row in _served(page)["results"]] == ids_two
        assert "adjusted" not in page
    assert all(first_key in row and other_key not in row for row in same["results"])
    assert all(other_key in row and first_key not in row for row in other["results"])
    assert none["results"] == plain_two["results"]


def test_the_windows_text_reply_shows_only_what_the_select_kept(tools):
    base = {"query": TERM, "method": "grep", "view": "windows", "limit": 2}
    whole = tools.text("search", base)
    with_windows = tools.text("search", {**base, "select": ["node_id", "windows"]})
    counts_only = tools.text("search", {**base, "select": ["node_id", "match_count"]})
    assert "--- window" in whole and "--- window" in with_windows
    assert "match_count" in whole and "match_count" not in "\n".join(
        line for line in with_windows.splitlines() if line.startswith("["))
    assert "--- window" not in counts_only and "match_count" in counts_only
    for text in (with_windows, counts_only):
        assert "! adjusted" not in text
        assert [line.split(" ")[1] for line in text.splitlines() if line.startswith("[")] == [
            line.split(" ")[1] for line in whole.splitlines() if line.startswith("[")]


def test_a_served_note_points_at_a_method_this_server_serves_and_at_no_other(tools):
    """Through the served tool: the note's pointer comes from the method list
    this server publishes. Grep is served here; semantic is not (no embedder)."""
    methods = set(tools.mcp._okg_capabilities.available_methods())
    assert "grep" in methods and "lexical" in methods and "semantic" not in methods
    (result,) = tools.run(
        ("search", {"query": TERM, "limit": 2, "select": ["node_id", "match_windows"]}, "json"))
    (record,) = _served(json.loads(result.content[0].text))["adjusted"]
    assert record["served"] == (
        "not returned, method lexical does not return it (lexical ranks by terms and "
        "returns no match windows, so call method grep for it)"
    )
    (result,) = tools.run(
        ("search", {"query": TERM, "limit": 2, "select": ["node_id", "distance"]}, "json"))
    (record,) = _served(json.loads(result.content[0].text))["adjusted"]
    assert "semantic" not in record["served"]
    assert record["served"].endswith("(not available on this server)")
    text = tools.text("search", {"query": TERM, "limit": 2, "select": ["node_id", "match_windows"]})
    assert "so call method grep for it" in text


@pytest.mark.parametrize("budget", [None, 4096])
@pytest.mark.parametrize("fmt", ["json", "text"])
def test_a_retry_call_is_never_the_part_a_fitted_refusal_gives_up(tools, fmt, budget):
    """At the default budget and at the smallest one, in both formats, a
    refusal that offers a call still carries it: `data`, then the tails of
    the hint and the message, are what a fitted refusal gives up first."""
    from tests.substrate.mcp.test_retry_call_reaches_the_caller_db import received_retry_call

    sent = {"query": TERM, "method": "identifier_mentions", "limit": 5,
            "select": ["node_id", "subtype", "handle", "excerpt", "citation", "parent",
                       "score", "location", "value", "id_type"]}
    if budget is not None:
        sent["max_bytes"] = budget
    (result,) = tools.run(("search", sent, fmt))
    assert result.isError
    if budget is not None:
        assert _wire_bytes(result) <= budget
    retry = received_retry_call(result)
    assert retry is not None and retry["mode"] == "guess", result.content[0].text[:1500]
    assert retry["arguments"]["method"] == "grep" and retry["arguments"]["query"] == TERM
    if fmt == "text":
        assert "see fix_hint" not in result.content[0].text.splitlines()[0]


# --------------------------------------------------------------------------- #
# the lean read surface (#3666, M10): what the descriptions show is served,   #
# and what the schemas no longer show is still served                         #
# --------------------------------------------------------------------------- #


def _lean_reply(tools: Tools, tool: str, arguments: dict) -> dict:
    (result,) = tools.run((tool, arguments, "json"))
    reply = json.loads(result.content[0].text)
    assert reply.get("status") != "refused", (tool, arguments, json.dumps(reply)[:800])
    return reply


def test_every_example_in_the_served_descriptions_is_served_as_written(tools):
    from tests.substrate.mcp.test_lean_read_surface import (
        READ_TOOLS,
        _runnable,
        example_calls,
        parse_call,
    )

    async def descriptions() -> dict[str, str]:
        return {t.name: t.description or "" for t in await tools.mcp.list_tools()}

    served = anyio.run(descriptions)
    sent = []
    for tool in READ_TOOLS:
        for call in example_calls(" ".join(served[tool].split())):
            if not _runnable(call):
                continue
            name, arguments = parse_call(call)
            reply = _lean_reply(tools, name, arguments)
            assert "adjusted" not in reply, (call, reply["adjusted"])
            sent.append(name)
    assert set(READ_TOOLS) <= set(sent)


#: Calls in spellings the lean surface does not publish, on this corpus.
UNPUBLISHED_FORMS: list[tuple[str, dict]] = [
    ("search", {"query": TERM, "view": "list", "limit": 5}),
    ("search", {"query": TERM, "method": "grep", "view": "windows", "limit": 2}),
    ("search", {"query": TERM, "rank_by": "self", "limit": 3}),
    ("search", {"query": TERM, "offset": 3, "limit": 3, "max_chars": 200}),
    ("search", {"query": TERM, "subtype_filter": "doc", "limit": 3}),
    ("search", {"query": TERM, "retain": True, "view": "metadata", "summary": ["subtype"]}),
    ("filter", {"input": {"kind": "nodes", "subtype": "doc"},
                "predicate": {"op": "suffix", "field": "attrs.path", "value": "/001.md"},
                "offset": 0}),
    ("map", {"input": {"kind": "nodes", "subtype": "doc"},
             "projection": ["node_id", "attrs.path"], "limit": 3}),
    ("aggregate", {"input": {"kind": "nodes", "subtype": "doc"},
                   "by": ["subtype"], "metrics": ["count"]}),
    ("expand", {"input": {"node_id": "doc:001"}, "edge_type": "contains",
                "filter": {"op": "eq", "field": "subtype", "value": "doc_chunk"}}),
    ("expand", {"input": "doc:001", "edge_type": "contains", "depth": 1, "offset": 0}),
    ("inspect", {"target": {"kind": "coverage"}}),
    ("inspect", {"target": {"kind": "ontology"}}),
]


@pytest.mark.parametrize("tool,arguments", UNPUBLISHED_FORMS, ids=[
    f"{tool}-{i}" for i, (tool, _) in enumerate(UNPUBLISHED_FORMS)
])
def test_an_unpublished_spelling_is_still_served(tools, tool, arguments):
    _lean_reply(tools, tool, arguments)


def test_a_misspelled_unpublished_argument_is_refused_by_name(tools):
    """Failure case: a name no argument model declares is refused, not
    ignored, now that its neighbours are no longer published."""
    (result,) = tools.run(("search", {"query": TERM, "rank": "self"}, "json"))
    reply = json.loads(result.content[0].text)
    assert reply["status"] == "refused"
    assert "rank" in json.dumps(reply["error"])


# ---- no set named: every record, with a note (#3666, M10) ---------------- #

_EVERY_FILTER = {"select": ["node_id"], "limit": 1000}
_EVERY_FILTER_TODAY = {"input": {"kind": "nodes"}, "predicate": {}, **_EVERY_FILTER}
_EVERY_AGGREGATE = {"by": ["subtype"], "metrics": ["count"]}
_EVERY_AGGREGATE_TODAY = {"input": {"kind": "nodes"}, **_EVERY_AGGREGATE}


def _defaulted(reply: dict) -> None:
    assert [r["rule"] for r in reply["adjusted"]] == ["scope_defaulted"], reply["adjusted"]


def test_no_set_named_reads_every_record_with_a_note(tools):
    """filter and aggregate with neither `where` nor `input`, as a client
    receives them: served over every record, the note in JSON and in text,
    and the same rows as the call that names the whole node set."""
    new, old = tools.pair("filter", {**_EVERY_FILTER}, _EVERY_FILTER_TODAY)
    _defaulted(new)
    assert _same(new, without=("adjusted",)) == _same(old)
    assert "doc:001" in _ids(new) and "ticket:1" in _ids(new)
    text = tools.text("filter", dict(_EVERY_FILTER))
    assert text.startswith("filter ok"), text[:300]
    assert "! adjusted: no `where` and no `input` read as every record" in text, text[:600]
    new, old = tools.pair("aggregate", dict(_EVERY_AGGREGATE), _EVERY_AGGREGATE_TODAY)
    _defaulted(new)
    assert _same(new, without=("adjusted",)) == _same(old)
    text = tools.text("aggregate", dict(_EVERY_AGGREGATE))
    assert text.startswith("aggregate ok") and "! adjusted: no `where`" in text, text[:600]


@pytest.mark.parametrize("regime", ["audit", "enforce"])
def test_no_set_named_follows_the_access_rules(
        clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch, regime):
    """Under access rules the defaulted filter returns exactly what the call
    naming every record returns this reader (under enforce, never the denied
    source's record), and aggregate stays refused, in plain words."""
    deployment = _corpus(tmp_path, clean_runtime)
    _activate(clean_runtime, deployment, _DENY_EXTERNAL)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1" if regime == "enforce" else "audit")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    server = Tools(clean_runtime, deployment)
    try:
        new, old = server.pair("filter", dict(_EVERY_FILTER), _EVERY_FILTER_TODAY)
        aggregate_new, aggregate_old = server.pair(
            "aggregate", dict(_EVERY_AGGREGATE), _EVERY_AGGREGATE_TODAY,
        )
        aggregate_text = server.text("aggregate", dict(_EVERY_AGGREGATE))
    finally:
        server.close()
    _defaulted(new)
    assert _same(new, without=("adjusted",)) == _same(old)
    if regime == "enforce":
        assert "doc:ext" not in _ids(new)
    for reply in (aggregate_new, aggregate_old):
        assert reply["status"] == "refused", json.dumps(reply)[:800]
        assert isinstance(reply["error"]["message"], str) and reply["error"]["message"]
    assert aggregate_new["error"]["code"] == aggregate_old["error"]["code"]
    assert aggregate_text.startswith("aggregate refused"), aggregate_text[:400]


def test_a_read_launch_lists_its_unshown_forms_as_a_client_reads_them(tools):
    text = tools.text("inspect", {"target": {"kind": "surface"}})
    assert text.startswith("inspect ok"), text[:300]
    for expected in ("read_only", "search(rank_by=...)",
                     "search(summary=...)", "expand(depth=...)", 'inspect target kind "view"',
                     "dates: a date field is date.<name>"):
        assert expected in text, expected
    # read-passage-rows-3666 (D1): search publishes `view` (the short rows,
    # "list"), so it is no longer listed as accepted-but-unshown.
    assert "search(view=...)" not in text
    # One form per plain line, not a quoted JSON list.
    assert "\nsearch(rank_by=...): " in text and '\\"search(' not in text
    assert "principal_readiness" not in text


#: The instruction and `query` examples, with values this corpus holds.
_INSTRUCTION_VALUES = {
    '"source_file"': '"doc"', '"src/app.py"': '"docs/001.md"',
    '"dir/file.py"': '"/001.md"', '"doc:<repo>:<path>"': '"doc:001"',
}


def test_every_instruction_example_is_served(tools):
    """The node-lookup, reading and `query` examples, run as written with
    this corpus's values: each is served, with no note, and the lookups find
    the record they promise."""
    import re

    from tests.substrate.mcp.test_lean_read_surface import parse_call

    async def served_text() -> str:
        tools_list = await tools.mcp.list_tools()
        query = next(t.description or "" for t in tools_list if t.name == "query")
        return (tools.mcp.instructions or "") + "\n" + query

    text = anyio.run(served_text)
    calls = re.findall(r"`((?:search|filter|inspect)\((?:[^`])*\))`", text)
    calls += re.findall(r"(inspect\(target=\{\"kind\":\"view\",[^)]*\))", text)
    assert len(calls) >= 4, calls
    for call in calls:
        for placeholder, value in _INSTRUCTION_VALUES.items():
            call = call.replace(placeholder, value)
        if "<" in call:
            continue
        name, arguments = parse_call(call)
        reply = _lean_reply(tools, name, arguments)
        assert "adjusted" not in reply, (call, reply["adjusted"])
        if name in ("search", "filter"):
            rows = reply.get("results") or reply.get("items") or []
            assert "doc:001" in [row.get("node_id") for row in rows], call


# --------------------------------------------------------------------------- #
# step 2c: one condition vocabulary on filter, map and aggregate               #
# --------------------------------------------------------------------------- #

#: The corpus's links: doc:000..004 and doc:passages-only each contain three
#: passages, 18 links in all.
LINKS = {"kind": "edges", "edge_type": "contains"}
ONE_DOC = ["doc_chunk:001-0", "doc_chunk:001-1", "doc_chunk:001-2"]


def _reply(tools: Tools, tool: str, arguments: dict) -> dict:
    (result,) = tools.run((tool, arguments, "json"))
    reply = json.loads(result.content[0].text)
    assert not result.isError, reply
    return _served(reply)


def _links(reply: dict, field: str = "dst") -> list[str]:
    return sorted(str(row[field]) for row in _served(reply)["items"])


def test_filter_compares_a_link_end_by_every_comparison_where_takes(tools):
    """The measured refusal: `prefix` on a link's `src` or `dst` reached the
    older translator, which has eq, in, exists and contains only."""
    by_eq = _reply(tools, "filter", {
        "input": LINKS, "predicate": {"op": "eq", "field": "src", "value": "doc:001"},
    })
    assert _links(by_eq) == ONE_DOC
    prefixed = _reply(tools, "filter", {
        "input": LINKS, "select": ["edge_id", "src", "dst", "edge_type"],
        "predicate": {"op": "and", "clauses": [
            {"op": "prefix", "field": "src", "value": "doc:001"},
            {"op": "prefix", "field": "dst", "value": "doc_chunk:"}]},
    })
    assert _links(prefixed) == ONE_DOC
    # The same link ids, as integers, whichever path served the page.
    assert sorted(row["edge_id"] for row in prefixed["items"]) == sorted(
        row["edge_id"] for row in by_eq["items"])
    assert all(isinstance(row["edge_id"], int) for row in prefixed["items"])
    cases = [
        ({"op": "suffix", "field": "dst", "value": "-2"}, 6),
        ({"op": "between", "field": "src", "values": ["doc:001", "doc:002"]}, 6),
        ({"op": "lt", "field": "dst", "value": "doc_chunk:001"}, 3),
        ({"op": "gt", "field": "src", "value": "doc:004"}, 3),
        ({"op": "not", "clause": {"op": "prefix", "field": "src", "value": "doc:00"}}, 3),
    ]
    for predicate, count in cases:
        rows = _reply(tools, "filter", {"input": LINKS, "predicate": predicate})["items"]
        assert len(rows) == count, (predicate, rows)
    # The scope's own condition takes the same comparisons (the measured
    # `input.filter` form), on filter, map and aggregate alike.
    scoped = {**LINKS, "filter": {"op": "prefix", "field": "src", "value": "doc:001"}}
    assert _links(_reply(tools, "filter", {
        "input": scoped, "predicate": {"op": "exists", "field": "dst"},
    })) == ONE_DOC
    assert _links(_reply(tools, "map", {"input": scoped, "projection": ["src", "dst"]})) == ONE_DOC
    counted = _reply(tools, "aggregate", {
        "input": {**LINKS, "filter": {"op": "prefix", "field": "dst", "value": "doc_chunk:po"}},
        "by": ["src"], "metrics": ["count"],
    })
    assert "doc:passages-only" in json.dumps(counted) and "doc:000" not in json.dumps(counted)


def test_an_integer_link_id_beside_a_compiled_comparison_is_compared_as_the_id(tools):
    """The relation stores a link's id as text: `eq` and `in` on an integer
    `edge_id` on the compiled path read it as that id, not as no link."""
    ids = sorted(row["edge_id"] for row in _reply(tools, "filter", {
        "input": LINKS, "predicate": {"op": "eq", "field": "src", "value": "doc:001"},
    })["items"])
    in_ids = _reply(tools, "filter", {"input": LINKS, "predicate": {"op": "and", "clauses": [
        {"op": "prefix", "field": "src", "value": "doc:00"},
        {"op": "in", "field": "edge_id", "values": ids}]}})
    assert sorted(row["edge_id"] for row in in_ids["items"]) == ids
    one = _reply(tools, "filter", {"input": LINKS, "predicate": {"op": "and", "clauses": [
        {"op": "prefix", "field": "src", "value": "doc:00"},
        {"op": "eq", "field": "edge_id", "value": ids[0]}]}})
    assert [row["edge_id"] for row in one["items"]] == [ids[0]]


def test_an_integer_range_on_a_link_id_compares_numbers(tools):
    """Review blocker: a range on `edge_id` compares the ids as numbers, not
    as text (where 9 > 10 and 5 > 1000000000000). Each answer is checked
    against the whole set of link ids compared in Python."""
    every = sorted(row["edge_id"] for row in _reply(tools, "filter", {
        "input": LINKS, "predicate": {}, "select": ["edge_id"]})["items"])
    assert len(every) == 18 and all(isinstance(i, int) for i in every)
    middle = every[9]
    cases = [
        ({"op": "gt", "field": "edge_id", "value": middle}, [i for i in every if i > middle]),
        ({"op": "lt", "field": "edge_id", "value": middle}, [i for i in every if i < middle]),
        ({"op": "gte", "field": "edge_id", "value": middle},
         [i for i in every if i >= middle]),
        ({"op": "between", "field": "edge_id", "values": [every[3], every[12]]},
         every[3:13]),
        # Bounds with fewer and more digits than any id (ids are below
        # 2**63 < 10**19): text order would put "9" above every id and
        # "10000000000000000000" below most of them.
        ({"op": "gt", "field": "edge_id", "value": 9}, every),
        ({"op": "lt", "field": "edge_id", "value": 10 ** 19}, every),
        ({"op": "gt", "field": "edge_id", "value": 10 ** 19}, []),
    ]
    for predicate, expected in cases:
        reply = _reply(tools, "filter", {"input": LINKS, "predicate": predicate,
                                         "select": ["edge_id"]})
        assert sorted(row["edge_id"] for row in reply["items"]) == expected, predicate
    # map's scope condition takes the same numbers.
    mapped = _reply(tools, "map", {
        "input": {**LINKS, "filter": {"op": "gt", "field": "edge_id", "value": middle}},
        "projection": ["edge_id"]})
    assert sorted(row["edge_id"] for row in mapped["items"]) == [i for i in every if i > middle]


def _received(tools: Tools, tool: str, arguments: dict) -> tuple[bool, dict]:
    (result,) = tools.run((tool, arguments, "json"))
    return bool(result.isError), json.loads(result.content[0].text)


def test_a_bound_of_the_wrong_type_is_never_an_empty_page(tools):
    """Re-review must-fix: a column range whose bound does not fit the
    column was compiled to "matches nothing" and served as an empty page
    marked complete. Each such call is now served with a note (a whole
    number written as a float), or refused with a call that is served."""
    every = sorted(row["edge_id"] for row in _reply(tools, "filter", {
        "input": LINKS, "predicate": {}, "select": ["edge_id"]})["items"])
    # Served with a note: 5.0 on an integer column is 5.
    for predicate, notes in (
        ({"op": "gt", "field": "edge_id", "value": 5.0}, ["integral_bound"]),
        ({"op": "gte", "field": "edge_id", "value": 5.0},
         ["inclusive_comparison", "integral_bound", "integral_bound"]),
    ):
        failed, reply = _received(tools, "filter", {"input": LINKS, "predicate": predicate,
                                                    "select": ["edge_id"]})
        assert not failed, reply
        assert [record["rule"] for record in reply["adjusted"]] == notes, reply["adjusted"]
        assert sorted(row["edge_id"] for row in reply["items"]) == every, predicate
    # Refused, with the one call that means it, which is served.
    for sent, offered, rows in (
        ({"input": LINKS, "predicate": {"op": "between", "field": "edge_id",
                                        "values": [1, "9"]}},
         {"op": "between", "field": "edge_id", "values": [1, 9]}, []),
        ({"input": LINKS, "predicate": {"op": "gt", "field": "edge_id", "value": "9"}},
         {"op": "gt", "field": "edge_id", "value": 9}, every),
        ({"input": LINKS, "predicate": {"op": "gt", "field": "edge_id", "value": 5.5}},
         {"op": "gt", "field": "edge_id", "value": 5}, every),
        ({"input": {"kind": "nodes", "subtype": "ticket"},
          "predicate": {"op": "gt", "field": "node_id", "value": 5}},
         {"op": "gt", "field": "node_id", "value": "5"}, None),
    ):
        failed, reply = _received(tools, "filter", sent)
        assert failed and reply["error"]["code"] == "invalid_operator_arguments", reply
        assert "items" not in reply
        retry = reply["error"]["retry_call"]
        assert retry["arguments"]["predicate"] == offered, retry
        served = _dispatched(tools, reply, sent)
        if rows is not None:
            assert sorted(row["edge_id"] for row in served["items"]) == rows
        else:
            assert _ids(served) == ["ticket:1"]
    # No single reading: refused in words, never an empty page.
    failed, reply = _received(tools, "filter", {"input": LINKS, "predicate": {
        "op": "lt", "field": "edge_id", "value": True}})
    assert failed and "retry_call" not in reply["error"], reply


def test_filter_compares_a_record_column_by_prefix(tools):
    prefixed = _reply(tools, "filter", {
        "input": {"kind": "nodes", "subtype": "doc"},
        "predicate": {"op": "prefix", "field": "node_id", "value": "doc:00"},
    })
    assert _ids(prefixed) == [f"doc:00{i}" for i in range(10)]
    # The second name reads the same column, with a note.
    assert _ids(_noted(
        tools, "filter",
        {"input": {"kind": "nodes"}, "predicate": {"op": "prefix", "field": "id",
                                                   "value": "ticket:"}},
        {"input": {"kind": "nodes"}, "predicate": {"op": "prefix", "field": "node_id",
                                                   "value": "ticket:"}},
        ["second_name"],
    )) == ["ticket:1"]


def test_aggregate_reads_the_second_names_in_its_scope_condition(tools):
    two = ["doc:001", "doc:002"]
    counted = _noted(
        tools, "aggregate",
        {"input": {"kind": "nodes", "filter": {"op": "in", "field": "id", "values": two}},
         "by": ["subtype"], "metrics": ["count"]},
        {"input": {"kind": "nodes", "filter": {"op": "in", "field": "node_id", "values": two}},
         "by": ["subtype"], "metrics": ["count"]},
        ["second_name"],
    )
    assert '"doc"' in json.dumps(counted)


@pytest.mark.parametrize("tool,arguments,named,hint", [
    # The measured forms: a field of search's `where` only, a field of no
    # set, and a field of the record at a link's end.
    ("filter", {"input": {"kind": "nodes", "subtype": "doc"},
                "predicate": {"op": "eq", "field": "source", "value": "docs.import"},
                "select": ["node_id", "attrs"]},
     "`source`", "search's `where` only"),
    ("filter", {"input": {"kind": "nodes", "subtype": "doc"},
                "predicate": {"op": "eq", "field": "provenance.source", "value": "docs.import"}},
     "`provenance.source`", "name one of the fields the message lists"),
    ("aggregate", {"input": {**LINKS, "filter": {"op": "and", "clauses": [
        {"op": "prefix", "field": "dst", "value": "doc_chunk:"},
        {"op": "eq", "field": "dst.attrs.is_merge", "value": False}]}},
        "by": ["src"], "metrics": ["count"]},
     "`dst.attrs.is_merge`", "name one of the fields the message lists"),
    ("map", {"input": {"kind": "nodes", "filter": {"op": "exists", "field": "title"}}},
     "`title`", "search's `where` only"),
])
def test_a_field_no_set_has_is_refused_by_name(tools, tool, arguments, named, hint):
    """Before step 2c each was `unsupported_operator_form` with words that
    named no field ("cannot be lowered exactly to SQL")."""
    error = _refused(tools, tool, arguments)["error"]
    assert f"{named} is not a field {tool} compares" in error["message"], error
    assert hint in error["fix_hint"], error
    # No single call means what was asked: none is offered.
    assert "retry_call" not in error


def test_a_near_miss_condition_is_served_with_a_note(tools):
    doc = {"kind": "nodes", "subtype": "doc"}
    on_day_one = ["2026-03-01", "2026-03-01T23:59:59Z"]
    noted, plain = tools.pair(
        "filter",
        {"input": doc, "predicate": {"op": "between", "field": "attrs.opened_at",
                                     "value": on_day_one}},
        {"input": doc, "predicate": {"op": "between", "field": "attrs.opened_at",
                                     "values": on_day_one}},
    )
    assert [record["rule"] for record in noted["adjusted"]] == ["values_in_value"]
    assert _same(noted, without=("adjusted",)) == _same(plain)
    assert _ids(plain) == ["doc:000", "doc:028", "doc:056", "doc:084", "doc:112"]
    counted, plain_count = tools.pair(
        "aggregate",
        {"input": {**doc, "filter": {"op": "between", "field": "attrs.opened_at",
                                     "value": on_day_one}},
         "by": ["subtype"], "metrics": ["count"]},
        {"input": {**doc, "filter": {"op": "between", "field": "attrs.opened_at",
                                     "values": on_day_one}},
         "by": ["subtype"], "metrics": ["count"]},
    )
    assert [record["rule"] for record in counted["adjusted"]] == ["values_in_value"]
    assert _same(counted, without=("adjusted",)) == _same(_served(plain_count))
    at_least, either = tools.pair(
        "filter",
        {"input": doc, "predicate": {"op": "gte", "field": "attrs.opened_at",
                                     "value": "2026-03-28"}},
        {"input": doc, "predicate": {"op": "or", "clauses": [
            {"op": "gt", "field": "attrs.opened_at", "value": "2026-03-28"},
            {"op": "eq", "field": "attrs.opened_at", "value": "2026-03-28"}]}},
    )
    assert [record["rule"] for record in at_least["adjusted"]] == ["inclusive_comparison"]
    assert _same(at_least, without=("adjusted",)) == _same(either)
    assert _ids(either) == ["doc:027", "doc:055", "doc:083", "doc:111"]
    listing, empty = tools.pair(
        "filter",
        {"input": {"kind": "nodes", "subtype": "ticket"}},
        {"input": {"kind": "nodes", "subtype": "ticket"}, "predicate": {}},
    )
    assert [record["rule"] for record in listing["adjusted"]] == ["predicate_missing"]
    assert _same(listing, without=("adjusted",)) == _same(empty)
    assert _ids(empty) == ["ticket:1"]
    # The note reaches the text reply an agent reads.
    text = tools.text("filter", {"input": {"kind": "nodes", "subtype": "ticket"}})
    assert ("! adjusted: no predicate read as every record in the input "
            "scope") in text, text


def test_a_comparison_the_set_does_not_have_is_still_refused(tools):
    """Failure case: an operator with no single meaning is not guessed."""
    (result,) = tools.run(("filter", {
        "input": {"kind": "nodes", "subtype": "doc"},
        "predicate": {"op": "ne", "field": "attrs.path", "value": "docs/001.md"},
    }, "json"))
    reply = json.loads(result.content[0].text)
    assert result.isError and reply["error"]["code"] == "invalid_operator_arguments", reply
    assert "adjusted" not in reply


def test_expand_orders_by_an_undeclared_attribute_as_text(tools):
    """#3666 (2c): the far end's `text` has no declared type; expand sorts
    its links by it as text, says so, and breaks ties by the link."""
    base = {"input": {"node_id": "doc:000"}, "edge_type": "contains"}
    up = _reply(tools, "expand", {**base, "order_by": "attrs.text"})
    down = _reply(tools, "expand", {**base, "order_by": "attrs.text desc"})
    chunks = [f"doc_chunk:000-{j}" for j in range(3)]
    assert [row["node_id"] for row in up["neighbors"]] == chunks
    assert [row["node_id"] for row in down["neighbors"]] == chunks[::-1]
    assert up["sorted_as_text"] == "attrs.text"
    text = tools.text("expand", {**base, "order_by": "attrs.text"})
    assert ("! sorted as text: attrs.text has no declared type; numbers sort as text "
            "too") in text, text
    # Stable across pages: one link per page gives the same order.
    paged = [
        _reply(tools, "expand", {**base, "order_by": "attrs.text", "limit": 1,
                                 "offset": offset})["neighbors"][0]["node_id"]
        for offset in range(3)
    ]
    assert paged == chunks


def test_an_order_on_a_named_date_names_the_stored_attribute(tools):
    """#3666 (2c): `date.created` maps to a different attribute per type and
    compares as an instant, so it is not ordered; with one type named the
    refusal names the attribute (`opened_at` for doc) and offers the call
    ordered by it (a guess: it has no declared type, so it sorts as text)."""
    pinned = _reply(tools, "filter", {"input": {"kind": "nodes", "subtype": "doc"},
                                      "predicate": {}, "limit": 1})["pinned_generation_id"]
    sent = {"input": {"kind": "nodes", "subtype": "doc"},
            "predicate": {"op": "prefix", "field": "node_id", "value": "doc:00"},
            "order_by": "date.created desc", "generation_id": pinned, "max_bytes": 60_000}
    reply = _refused_as(tools, "filter", sent, "unsupported_operator_form")
    error = reply["error"]
    assert "attrs.opened_at" in error["fix_hint"], error
    retry = error["retry_call"]
    assert retry["mode"] == "guess" and retry["tool"] == "filter"
    assert retry["arguments"]["order_by"] == "attrs.opened_at desc"
    # The offered call keeps the pinned generation and the byte budget.
    assert retry["arguments"]["generation_id"] == pinned, retry
    assert retry["arguments"]["max_bytes"] == 60_000, retry
    assert "repeat" not in retry
    # With authority: named in `repeat`, never echoed.
    # (No identity claim: this deployment has no identity provider.)
    authority = {"identity_claim": None, "access_context": None}
    claimed = _refused_as(tools, "filter", {**sent, "authority": authority},
                          "unsupported_operator_form")
    offered = claimed["error"]["retry_call"]
    assert "authority" not in offered["arguments"] and offered["repeat"] == ["authority"]
    assert offered["arguments"] == retry["arguments"], offered
    assert "identity_claim" not in json.dumps(offered), offered
    served = _dispatched(tools, reply, sent)
    assert served["sorted_as_text"] == "attrs.opened_at"
    # Failure case: no type named, so no attribute and no call.
    untyped = _refused_as(tools, "filter", {
        "input": {"kind": "nodes"}, "predicate": {}, "order_by": "date.created"},
        "unsupported_operator_form")
    assert "retry_call" not in untyped["error"]
    # A direction the order grammar does not take: the attribute is named,
    # and no call is offered (it would be refused again).
    upward = _refused_as(tools, "filter", {**sent, "order_by": "date.created upward"},
                         "unsupported_operator_form")
    assert "retry_call" not in upward["error"]
    assert "a direction is asc or desc" in upward["error"]["fix_hint"]


def test_contains_with_text_on_an_attribute_is_served_as_text_inside_it(tools):
    """#3666: `contains` with a string on one attribute matches the stored
    text holding it, ASCII case ignored, in filter, map, aggregate and
    search (with and without a query). It used to be refused, and agents
    recovered on the next call about two times in three."""
    contains = {"op": "contains", "field": "attrs.summary", "value": "UMENT 001"}
    sent = {"input": {"kind": "nodes", "subtype": "doc"}, "predicate": contains}
    assert _ids(_served(_reply(tools, "filter", sent))) == ["doc:001"]
    scoped = {"input": {"kind": "nodes", "subtype": "doc", "filter": contains},
              "projection": ["node_id"]}
    assert _ids(_served(_reply(tools, "map", scoped))) == ["doc:001"]
    ten = [f"doc:00{i}" for i in range(10)]
    # The shorthand the agents wrote, beside an exact match.
    short = {"where": {"type": "doc", "attrs.path": {"op": "contains", "value": "S/00"}},
             "limit": 20}
    assert _ids(_served(_reply(tools, "filter", short))) == ten
    counted = _served(_reply(tools, "aggregate", {
        "where": {"type": "doc", "attrs.path": {"op": "contains", "value": "S/00"}},
        "by": ["subtype"], "metrics": ["count"],
    }))
    assert [(group["subtype"], group["count"]) for group in counted["groups"]] == [("doc", 10)]
    # Under `not` a record without the attribute matches.
    absent = {"input": {"kind": "nodes", "subtype": "ticket"},
              "predicate": {"op": "not", "clause": {
                  "op": "contains", "field": "attrs.path", "value": "docs"}}}
    assert _ids(_served(_reply(tools, "filter", absent))) == ["ticket:1"]
    # search: ranked by the query, and the query-less lookup, on an
    # attribute and on the catalog path and title.
    ranked = _served(_reply(tools, "search", {"query": TERM, "where": {
        "type": "doc", "attrs.summary": {"op": "contains", "value": "ument 001"}}}))
    assert _ids(ranked, "results") == ["doc:001"]
    for where in (
        {"type": "doc", "attrs.path": {"op": "contains", "value": "S/00"}},
        {"type": "doc", "path": {"op": "contains", "value": "S/00"}},
        {"type": "doc", "title": {"op": "contains", "value": "UMENT 00"}},
    ):
        looked_up = _served(_reply(tools, "search", {"where": where, "limit": 20}))
        assert _ids(looked_up, "results") == ten, where
    # Text on a column is still refused; on a set of links no search is
    # offered.
    links = {"input": LINKS, "predicate": {"op": "contains", "field": "src", "value": "doc"}}
    assert "retry_call" not in _refused_as(
        tools, "filter", links, "unsupported_operator_form")["error"]


def test_search_matches_text_in_the_source_id_and_names_sources_on_an_empty_page(tools):
    """#3666: `contains` with text on `source` matches the record's source
    id (ASCII case ignored), ranked and by grep; an empty page under a
    `source` condition names the source ids holding its text, in the JSON
    and in the text reply (the text used to drop it)."""
    ranked = _reply(tools, "search", {"query": TERM, "limit": 5, "where": {
        "type": "ticket", "source": {"op": "contains", "value": "ANALYST_SEARCH"}}})
    assert _ids(ranked, "results") == ["ticket:1"]
    assert "source_hint" not in ranked  # a page with rows carries no hint
    grep = _reply(tools, "search", {"query": TERM, "method": "grep", "limit": 5, "where": {
        "type": "ticket", "source": {"op": "contains", "value": "analyst_search"}}})
    assert _ids(grep, "results") == ["ticket:1"]
    # The short name is no source id: no rows, and the reply names the id.
    short = _reply(tools, "search", {"query": TERM, "where": {
        "source": "analyst_search_fixture"}})
    assert short["results"] == []
    (hint,) = short["source_hint"]
    assert hint["source_ids"] == [MAIN]
    assert "no source id is exactly this text" in hint["note"]
    # The exact shape the lab replay sent, in the text an agent reads.
    text = tools.text("search", {"query": TERM, "where": {
        "source": "analyst_search_fixture"}, "limit": 5})
    assert ('! source "analyst_search_fixture": no source id is exactly this text; '
            f'write one of these, or use contains; source ids holding it: "{MAIN}"') in text
    nothing = _reply(tools, "search", {"query": TERM, "where": {
        "source": {"op": "contains", "value": "no-such-producer"}}})
    assert nothing["results"] == []
    assert nothing["source_hint"][0]["source_ids"] == []


def test_expand_serves_text_contains_on_the_far_records(tools):
    """#3666 review P2-4: expand filters the records it reaches with text
    `contains` on an attribute, ASCII case ignored, as filter does."""
    sent = {"input": {"node_id": "doc:000"}, "edge_type": "contains",
            "filter": {"op": "contains", "field": "attrs.text", "value": "PASSAGE 1 OF"}}
    reply = _reply(tools, "expand", sent)
    assert _ids(reply, "neighbors") == ["doc_chunk:000-1"]
    # The shorthand under `where` reads the same.
    short = _reply(tools, "expand", {
        "input": {"node_id": "doc:000"}, "edge_type": "contains",
        "where": {"attrs.text": {"op": "contains", "value": "passage 2 of"}},
    })
    assert _ids(short, "neighbors") == ["doc_chunk:000-2"]


def test_a_repository_name_is_read_as_its_source_ids(clean_runtime, okg_mcp_can_log_in,
                                                     tmp_path):
    """#3666: agents write the repository name where source ids are
    `<repo>.<family>`. The name is read as those ids, with a note; an exact
    id stays exact; a name that is neither still answers empty, with a hint."""
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment, subtypes=[DOC],
        nodes=[_node("doc:main", "doc", f"{TERM} main.", summary="Main")],
        node_groups=[
            ("repo-x.docs", [_node("doc:x1", "doc", f"{TERM} one.", summary="One")]),
            ("repo-x.git", [_node("doc:x2", "doc", f"{TERM} two.", summary="Two")]),
        ],
    )
    for producer in (MAIN, "repo-x.docs", "repo-x.git"):
        _trust(clean_runtime, deployment, producer)
    server = Tools(clean_runtime, deployment)
    try:
        sent = {"query": TERM, "where": {"source": "repo-x"}, "limit": 5}
        (result,) = server.run(("search", sent, "json"))
        reply = json.loads(result.content[0].text)
        assert not result.isError, reply
        assert _ids(reply, "results") == ["doc:x1", "doc:x2"]
        assert [note["rule"] for note in reply["adjusted"]] == ["source_repo_as_sources"]
        assert reply["adjusted"][0]["served"] == (
            "where source in [`repo-x.docs`, `repo-x.git`]"
        )
        assert "source_hint" not in reply
        text = server.text("search", sent)
        assert ("! adjusted: where source = `repo-x` read as where source in "
                "[`repo-x.docs`, `repo-x.git`]") in text
        exact = _reply(server, "search", {"query": TERM, "where": {"source": "repo-x.git"}})
        assert _ids(exact, "results") == ["doc:x2"] and "adjusted" not in exact
        other = _reply(server, "search", {"query": TERM, "where": {"source": "epo-x"}})
        assert other["results"] == []
        assert other["source_hint"][0]["source_ids"] == ["repo-x.docs", "repo-x.git"]
    finally:
        server.close()


# -- one rule over operator x field kind x value type (#3666, 2c) ------------

#: The value types every comparison is sent with: text (for the link id, text
#: that is not digits), digits, a whole number, a whole number written as a
#: float, a fractional float, a bool, null, an object, a list (`eq` only), and
#: one number beside text (`between` only).
_VTYPES = ("text", "digits", "int", "float_whole", "float_frac", "bool", "null", "object",
           "list", "mixed")
_OPS = ("eq", "in", "lt", "gt", "between", "prefix", "suffix", "contains", "exists")
#: What the client receives, one letter per value type in `_VTYPES` order:
#: r served, its rows checked; R served (a correct test, rows not checked);
#: E served with no rows (a correct test that nothing meets); N served with
#: the integral_bound note, rows checked; T refused with a retry_call that is
#: served; X refused, with no rows and no call; `.` not sent.
_TEXT_COLUMN = {
    "eq":       "rRTXXXEXT.",
    "in":       "rRTXXXEX..",
    "lt":       "rRTXXXXX..",
    "gt":       "rRTXXXXX..",
    "between":  "rRTXXXXX.T",
    "prefix":   "rRXXXXXX..",
    "suffix":   "rRXXXXXX..",
    "contains": "XXXXXXXX..",
    "exists":   "r.........",
}
#: search's `where` fields are text columns too, with the same answers.
_SEARCH_COLUMN = _TEXT_COLUMN
#: search's `source` is a text column too, and `contains` with text reads
#: the record's source id (#3666): served, as on an attribute.
_SEARCH_SOURCE = {**_TEXT_COLUMN, "contains": "rRXXXXXX.."}
_LINK_ID = {
    "eq":       "XrrNXXEXT.",
    "in":       "XrrNXXEX..",
    "lt":       "XTrNTXXX..",
    "gt":       "XTrNTXXX..",
    "between":  "XTrNTXXX.X",
    "prefix":   "XrXXXXXX..",
    "suffix":   "XrXXXXXX..",
    "contains": "XXXXXXXX..",
    "exists":   "r.........",
}
_NAMED_DATE = {
    "eq":       "rXXXXXXXX.",
    "in":       "rXXXXXXX..",
    "lt":       "rXXXXXXX..",
    "gt":       "rXXXXXXX..",
    "between":  "rXXXXXXX.X",
    "prefix":   "XXXXXXXX..",
    "suffix":   "XXXXXXXX..",
    # On a scope of one type, `contains` with text is offered as a search
    # for those words (a guess).
    "contains": "TTXXXXXX..",
    "exists":   "r.........",
}
#: One attribute holds any JSON value: `eq` and `in` compare it exactly, so a
#: number never equals the stored text (E); a range compares numbers or text.
#: `eq` with a list can mean `in` or a stored list: refused, `in` offered.
_ATTRIBUTE = {
    "eq":       "rEEEEERET.",
    "in":       "rEEEEERE..",
    "lt":       "rREEEXXX..",
    "gt":       "rREEEXXX..",
    "between":  "rREEEXXX.X",
    "prefix":   "rRXXXXXX..",
    "suffix":   "rRXXXXXX..",
    # #3666: text on one attribute is served (ASCII case ignored); any
    # other operand is refused.
    "contains": "rRXXXXXX..",
    "exists":   "r.........",
}
_WHOLE_ATTRS = {
    "eq":       "XXXXXXXEX.",
    "in":       "XXXXXXXE..",
    "lt":       "XXXXXXXX..",
    "gt":       "XXXXXXXX..",
    "between":  "XXXXXXXX.X",
    "prefix":   "XXXXXXXX..",
    "suffix":   "XXXXXXXX..",
    "contains": "TTXXXXXr..",
    "exists":   "r.........",
}
_SCALARS = {"digits": "5", "int": 5, "float_whole": 5.0, "float_frac": 5.5, "bool": True,
            "null": None, "object": {"a": 1}}
_PAIRS = {"digits": ["5", "6"], "int": [5, 6], "float_whole": [5.0, 6.0],
          "float_frac": [5.5, 6.5], "bool": [True, True], "null": [None, None],
          "object": [{"a": 1}, {"a": 1}], "mixed": [5, "u"]}
_FIVE_DOCS = ["doc:001", "doc:029", "doc:057", "doc:085", "doc:113"]


def _field_kinds(links: list[dict]) -> list[dict]:
    """Each kind of field a set read compares, with its operands and answers.

    `text` is the text operand of each operator, `expect` the rows its `r`
    and `N` cells must return: a list (exactly these), ("has", id) or
    ("any",). `links` is every link of the corpus, for the link answers.
    """
    every = sorted(row["edge_id"] for row in links)
    low, high = every[9], every[14]

    def by_src(test) -> list:
        return sorted(row["edge_id"] for row in links if test(row["src"]))

    nodes, docs = {"kind": "nodes"}, {"kind": "nodes", "subtype": "doc"}
    return [
        {"name": "text column", "field": "subtype", "scope": nodes, "key": "node_id",
         "table": _TEXT_COLUMN,
         "text": {"eq": "ticket", "in": "ticket", "lt": "e", "gt": "t",
                  "between": ["t", "u"], "prefix": "tick", "suffix": "cket",
                  "contains": "tick"},
         "expect": {"eq": ["ticket:1"], "in": ["ticket:1"], "lt": ("any",),
                    "gt": ["ticket:1"], "between": ["ticket:1"], "prefix": ["ticket:1"],
                    "suffix": ["ticket:1"], "exists": ("any",)}},
        # search's own `where` (third re-review): every field of it is text.
        {"name": "search type", "field": "type", "tool": "search", "key": "node_id",
         "table": _SEARCH_COLUMN,
         "text": {"eq": "ticket", "in": "ticket", "lt": "e", "gt": "t",
                  "between": ["t", "u"], "prefix": "tick", "suffix": "cket",
                  "contains": "tick"},
         "expect": {"eq": ["ticket:1"], "in": ["ticket:1"], "lt": ("any",),
                    "gt": ["ticket:1"], "between": ["ticket:1"], "prefix": ["ticket:1"],
                    "suffix": ["ticket:1"], "exists": ("any",)}},
        {"name": "search source", "field": "source", "tool": "search", "key": "node_id",
         "table": _SEARCH_SOURCE,
         "text": {"eq": "default", "in": "default", "lt": "e", "gt": "a",
                  "between": ["a", "z"], "prefix": "def", "suffix": "ault",
                  # Every fixture record's source is MAIN, which holds this
                  # text; "5" (digits) is in no source id, so 0 rows is true.
                  "contains": "ANALYST_SEARCH"},
         "expect": {"contains": ("any",)}},
        {"name": "id column", "field": "node_id", "scope": nodes, "key": "node_id",
         "table": _TEXT_COLUMN,
         "text": {"eq": "ticket:1", "in": "ticket:1", "lt": "doc:001", "gt": "ticket:0",
                  "between": ["ticket:0", "ticket:2"], "prefix": "ticket:",
                  "suffix": "et:1", "contains": "ticket"},
         "expect": {"eq": ["ticket:1"], "in": ["ticket:1"], "lt": ("has", "doc:000"),
                    "gt": ["ticket:1"], "between": ["ticket:1"], "prefix": ["ticket:1"],
                    "suffix": ["ticket:1"], "exists": ("any",)}},
        {"name": "link text column", "field": "src", "scope": LINKS, "key": "edge_id",
         "table": _TEXT_COLUMN,
         "text": {"eq": "doc:001", "in": "doc:001", "lt": "doc:001", "gt": "doc:004",
                  "between": ["doc:001", "doc:002"], "prefix": "doc:00", "suffix": "001",
                  "contains": "doc"},
         "expect": {"eq": by_src(lambda s: s == "doc:001"),
                    "in": by_src(lambda s: s == "doc:001"),
                    "lt": by_src(lambda s: s < "doc:001"),
                    "gt": by_src(lambda s: s > "doc:004"),
                    "between": by_src(lambda s: "doc:001" <= s <= "doc:002"),
                    "prefix": by_src(lambda s: s.startswith("doc:00")),
                    "suffix": by_src(lambda s: s.endswith("001")),
                    "exists": every}},
        {"name": "integer column", "field": "edge_id", "scope": LINKS, "key": "edge_id",
         "table": _LINK_ID,
         "text": {op: "abc" for op in _OPS} | {"between": ["abc", "abd"]},
         # A float is exact below 2**53 only, and link ids are larger: the
         # floats here are small (the ids past them are every id), and a
         # float the size of an id is checked on its own below.
         "scalars": {"digits": str(low), "int": low},
         "pairs": {"digits": [str(low), str(high)], "int": [low, high], "mixed": [low, "x"]},
         "list": [low],
         "expect": {"eq": [low], "in": [low], "lt": [i for i in every if i < low],
                    "gt": [i for i in every if i > low],
                    "between": [i for i in every if low <= i <= high],
                    "prefix": ("has", low), "suffix": ("has", low), "exists": every},
         "expect_float": {"eq": [], "in": [], "lt": [], "gt": every, "between": []}},
        {"name": "date", "field": "date.created", "scope": docs, "key": "node_id",
         "table": _NAMED_DATE,
         "text": {"eq": "2026-03-02", "in": "2026-03-02", "lt": "2026-03-02",
                  "gt": "2026-03-27", "between": ["2026-03-02", "2026-03-02"],
                  "prefix": "2026", "suffix": "02", "contains": "2026"},
         "list": ["2026-03-02"],
         "expect": {"eq": _FIVE_DOCS, "in": _FIVE_DOCS, "lt": ("has", "doc:000"),
                    "gt": ("has", "doc:027"), "between": _FIVE_DOCS, "exists": ("any",)}},
        {"name": "attribute", "field": "attrs.path", "scope": docs, "key": "node_id",
         "table": _ATTRIBUTE,
         "text": {"eq": "docs/001.md", "in": "docs/001.md", "lt": "docs/001.md",
                  "gt": "docs/130.md", "between": ["docs/001.md", "docs/002.md"],
                  "prefix": "docs/001", "suffix": "001.md", "contains": "S/001.MD"},
         "list": ["docs/001.md"],
         "expect": {"eq": ["doc:001"], "in": ["doc:001"], "lt": ("has", "doc:000"),
                    "gt": ("has", "doc:131"), "between": ["doc:001", "doc:002"],
                    "prefix": ["doc:001"], "suffix": ["doc:001"], "contains": ["doc:001"],
                    "exists": ("any",)}},
        {"name": "whole attrs", "field": "attrs", "scope": docs, "key": "node_id",
         "table": _WHOLE_ATTRS, "long_only": True,
         "text": {op: "docs" for op in _OPS} | {"between": ["a", "b"]},
         "scalars": {"object": {"path": "docs/001.md"}},
         "pairs": {"object": [{"path": "docs/001.md"}, {"path": "docs/002.md"}]},
         "list": [{"path": "docs/001.md"}],
         "expect": {"contains": ["doc:001"], "exists": ("any",)}},
    ]


def _operand(kind: dict, op: str, vtype: str) -> Any:
    if op == "exists":
        return None
    if vtype == "text":
        return kind["text"][op]
    if vtype == "list":
        return kind.get("list", [kind["text"]["eq"]])
    if op == "between":
        return kind.get("pairs", {}).get(vtype, _PAIRS[vtype])
    return kind.get("scalars", {}).get(vtype, _SCALARS[vtype])


def _test_of(op: str, operand: Any) -> dict:
    """The test without its field: `{"op": ..., <operand key>: ...}`."""
    if op == "exists":
        return {"op": "exists"}
    if op == "in":
        return {"op": "in", "values": [operand]}
    if op == "between":
        return {"op": "between", "values": operand}
    return {"op": op, "value": operand}


def _spellings(kind: dict, op: str, operand: Any) -> list[tuple[str, dict]]:
    """Every spelling of one test the client may send to filter."""
    field = kind["field"]
    test = _test_of(op, operand)
    long_form = {"field": field, **test}
    if kind.get("tool") == "search":
        # search's own `where`, in the long form and the short forms.
        def call(condition: dict) -> dict:
            return {"query": TERM, "where": condition}
        key = "where"
        out = [("long", call(long_form))]
    else:
        scope = kind["scope"]

        def call(condition: dict) -> dict:
            return {"input": scope, "predicate": condition}
        key = "predicate"
        out = [("long", call(long_form)), ("where", {"input": scope, "where": long_form})]
    if kind.get("long_only"):
        return out
    out.append(("short test", call({field: test})))
    if op == "eq" and not isinstance(operand, (list, dict)):
        out.append(("short", call({field: operand})))
    if op == "in":
        out.append(("short list", call({field: [operand]})))
    assert all(key in arguments or "where" in arguments for _, arguments in out)
    return out


def _rows_meet(ids: list, expect: Any) -> bool:
    if expect == ("any",):
        return bool(ids)
    if isinstance(expect, tuple) and expect[0] == "has":
        return expect[1] in ids
    return sorted(ids) == sorted(expect)


def _cell_failures(cell: str, kind: dict, expect: Any, result: Any) -> tuple[list[str], Any]:
    """What is wrong with one reply, and the call it offers (cell T)."""
    reply = json.loads(result.content[0].text)
    refused = bool(result.isError) or reply.get("status") == "refused"
    ids = [row[kind["key"]] for row in reply.get("items") or reply.get("results") or []]
    if cell in "rREN":
        if refused:
            return [f"refused: {json.dumps(reply.get('error'))[:240]}"], None
        notes = [record["rule"] for record in reply.get("adjusted", [])]
        wrong = []
        if cell == "N" and "integral_bound" not in notes:
            wrong.append(f"no integral_bound note: {notes}")
        if cell != "N" and notes:
            wrong.append(f"unexpected notes {notes}")
        if cell == "E" and ids:
            wrong.append(f"rows {ids[:8]}, expected none")
        # A kind with no expected rows (search's `source`, whose values the
        # corpus does not fix) is checked for served-or-refused only.
        if cell in "rN" and expect is not None and not _rows_meet(ids, expect):
            wrong.append(f"rows {ids[:8]} do not meet {expect}")
        return wrong, None
    if not refused:
        return [f"served {len(ids)} rows, expected a refusal"], None
    wrong = []
    if any(name in reply for name in ("items", "results", "groups")):
        wrong.append("a refusal carried rows")
    offered = reply.get("error", {}).get("retry_call")
    if cell == "X" and offered is not None:
        wrong.append(f"refused with a call {offered}")
    if cell == "T" and offered is None:
        wrong.append(f"refused with no call: {json.dumps(reply.get('error'))[:240]}")
    return wrong, offered if cell == "T" else None


def _compound_cases(links: list[dict]) -> list[tuple[str, str, dict, dict, Any]]:
    """A test of the wrong type inside a compound condition, and beside a
    `select`: the same answer as alone (third re-review)."""
    every = sorted(row["edge_id"] for row in links)
    low = every[9]
    on_links = {"key": "edge_id"}
    on_nodes = {"key": "node_id"}
    one_doc = sorted(row["edge_id"] for row in links if row["src"] == "doc:001")
    one_doc_past = [i for i in one_doc if i > low]
    prefix = {"op": "prefix", "field": "src", "value": "doc:001"}
    return [
        ("compound: a range that fits", "r", on_links,
         {"input": LINKS, "predicate": {"op": "and", "clauses": [
             prefix, {"op": "gt", "field": "edge_id", "value": low}]}}, one_doc_past),
        ("compound: a digit-string range on the link id", "T", on_links,
         {"input": LINKS, "predicate": {"op": "and", "clauses": [
             prefix, {"op": "gt", "field": "edge_id", "value": str(low)}]}}, None),
        ("compound: a bool range under not", "X", on_links,
         {"input": LINKS, "predicate": {"op": "not", "clause": {"op": "or", "clauses": [
             prefix, {"op": "lt", "field": "edge_id", "value": True}]}}}, None),
        ("compound: a number on a text column, short form", "T", on_links,
         {"input": LINKS, "predicate": {"src": "doc:001", "dst": 5}}, None),
        ("select: a number on a text column", "T", on_nodes,
         {"input": {"kind": "nodes"}, "predicate": {"subtype": 5}, "select": ["node_id"]}, None),
        ("select: a range that fits", "r", on_links,
         {"input": LINKS, "predicate": {"op": "gt", "field": "edge_id", "value": low},
          "select": ["edge_id", "src"]}, [i for i in every if i > low]),
        ("search select: a number on its type", "T", {"key": "node_id", "tool": "search"},
         {"query": TERM, "where": {"type": 5}, "select": ["node_id"]}, None),
        # The same rule in every other tool's condition (fourth review).
        ("map: a number on a text column in the scope", "T", {"key": "node_id", "tool": "map"},
         {"input": {"kind": "nodes", "filter": {"subtype": 5}}}, None),
        ("map: a bool range on the link id", "X", {"key": "edge_id", "tool": "map"},
         {"input": {**LINKS, "filter": {"op": "lt", "field": "edge_id", "value": True}}}, None),
        ("aggregate: a number on a text column", "T", {"key": "subtype", "tool": "aggregate"},
         {"input": {"kind": "nodes", "filter": {"subtype": 5}}, "by": ["subtype"],
          "metrics": ["count"]}, None),
        ("expand: a number on a text column of the far end", "T",
         {"key": "node_id", "tool": "expand"},
         {"input": "doc:001", "filter": {"subtype": 5}}, None),
        ("eq with a list on an attribute inside or", "T", on_nodes,
         {"input": {"kind": "nodes", "subtype": "doc"}, "predicate": {"op": "or", "clauses": [
             {"op": "eq", "field": "attrs.path", "value": ["docs/001.md"]},
             {"op": "eq", "field": "node_id", "value": "doc:002"}]}}, None),
    ]


def test_every_operator_on_every_field_kind_with_every_value_type_is_answered_or_refused(tools):
    """Second re-review blocker (#3666, 2c): a short-form test, and `eq` or
    `in` on a text column with a value of another type, compiled to FALSE: a
    confident empty page. Every operator on every kind of field (a text
    column, a record id, the link id, a named date, one attribute, the whole
    attribute object), with every value type, in every spelling, is answered
    as the client receives it: served with the right rows, served with a
    note, refused with a call that is served, or refused in words. A cell
    marked E is the one empty page served, and only where the test is a
    correct one that nothing in the corpus meets."""
    links = _reply(tools, "filter", {"input": LINKS, "predicate": {},
                                     "select": ["edge_id", "src", "dst"]})["items"]
    assert len(links) == 18
    cases: list[tuple[str, str, dict, dict, Any]] = []
    for kind in _field_kinds(links):
        for op in _OPS:
            for vtype, cell in zip(_VTYPES, kind["table"][op]):
                if cell == "." or (op == "exists" and vtype != "text"):
                    continue
                for form, arguments in _spellings(kind, op, _operand(kind, op, vtype)):
                    sent = arguments.get("predicate", arguments.get("where"))
                    label = f"{kind['name']} {op} {vtype} ({form}) [{cell}] {_spell(sent)}"
                    expect = kind["expect_float" if cell == "N" else "expect"].get(op)
                    cases.append((label, cell, kind, arguments, expect))
    cases += _compound_cases(links)
    results = tools.run(*[(kind.get("tool", "filter"), arguments, "json")
                          for _, _, kind, arguments, _ in cases])
    failures: list[str] = []
    retries: list[tuple[str, dict]] = []
    for (label, cell, kind, _arguments, expect), result in zip(cases, results):
        wrong, offered = _cell_failures(cell, kind, expect, result)
        failures += [f"{label}: {words}" for words in wrong]
        if offered is not None:
            retries.append((label, offered))
    served = tools.run(*[(offered["tool"], offered["arguments"], "json")
                         for _, offered in retries])
    for (label, offered), result in zip(retries, served):
        reply = json.loads(result.content[0].text)
        if result.isError or reply.get("status") == "refused" or "adjusted" in reply:
            failures.append(f"{label}: the offered call {_spell(offered)} is not served as sent")
    # A float the size of a link id is not that id (floats skip whole numbers
    # past 2**53): refused, never an empty page.
    huge = float(sorted(row["edge_id"] for row in links)[9])
    for test in ({"op": "eq", "value": huge}, {"op": "in", "values": [huge]},
                 {"op": "gt", "value": huge}, {"op": "between", "values": [1, huge]}):
        (result,) = tools.run(("filter", {"input": LINKS,
                                          "predicate": {"field": "edge_id", **test}}, "json"))
        wrong, _ = _cell_failures("X", {"key": "edge_id"}, None, result)
        failures += [f"edge_id {_spell(test)} as a float: {words}" for words in wrong]
    assert len(cases) > 600, len(cases)
    assert not failures, f"{len(failures)} of {len(cases)} cells:\n" + "\n".join(failures[:150])


def _spell(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)[:160]


# ==== every cut reply's continuation is published and runs ================ #
#
# PACT publish-paging-args-3666 (#3666). The lean read surface stopped
# publishing `search`'s `offset` and `max_chars` and every set tool's
# `offset`, while cut replies still said "next: offset=N" and "page with
# offset". Each case produces a cut page as a client receives it (the text
# reply), reads the continuation it prints (the `next:` fact and the cut
# notices), checks every argument it names is in the hinted tool's published
# schema, and sends the hinted call.


def _published(tools: Tools) -> dict[str, set[str]]:
    async def listed() -> dict[str, set[str]]:
        return {
            t.name: set((t.inputSchema or {}).get("properties") or {})
            for t in await tools.mcp.list_tools()
        }

    return anyio.run(listed)


def _json(tools: Tools, tool: str, arguments: dict) -> dict:
    (result,) = tools.run((tool, arguments, "json"))
    return json.loads(result.content[0].text)


#: The `next:` fact of a status line: up to the next fact or the line end.
_NEXT = re.compile(r"(?:^|· )next: (.+?)(?= · |$)", re.M)
#: `name=value` in a continuation; a value is JSON, a quoted string or a bare token.
_ASSIGN = re.compile(r'\b([a-z_]+)=("(?:[^"\\]|\\.)*"|[^\s,)]+)')
#: Argument names a cut notice tells the caller to use in words.
_WORDED = re.compile(r"\b(?:(?:page|rerun) (?:with|from)|pass) ([a-z_]+)\b")
#: The notices that say how to continue a cut page.
_CUT_NOTICES = ("! page cut", "! to read every", "! bound reached", "! order exact")


def _value(text: str) -> Any:
    try:
        return json.loads(text)
    except ValueError:
        return text


def _hint_call(text: str) -> tuple[str, dict[str, Any]]:
    """A whole `tool(name=value, ...)` hint, read as Python literals."""
    tool, body = text.split("(", 1)
    tree = ast.parse(f"f({body[:-1]})", mode="eval").body
    assert isinstance(tree, ast.Call), text
    return tool.strip(), {kw.arg: ast.literal_eval(kw.value) for kw in tree.keywords}


def continuation(text: str, tool: str, sent: dict) -> tuple[str, dict[str, Any], set[str]] | None:
    """The call the reply says continues the read, and every argument the
    cut notices name; None when the reply names no continuation.

    Forms (`result_text._next`): ``offset=N[ completeness="exact"]``,
    ``continuation_token=T``, ``search offset=N without retain``, a whole
    ``tool(...)`` call, or a bare token passed back as ``next``.
    """
    named: set[str] = set()
    for line in text.splitlines():
        if line.startswith(_CUT_NOTICES):
            named |= {m.group(1) for m in _ASSIGN.finditer(line)}
            named |= set(_WORDED.findall(line))
    found = _NEXT.findall(text)
    hints = [h.strip() for h in found if h.strip() not in ("none", "next_query below")]
    if not hints:
        return None
    assert len(set(hints)) == 1, hints
    (hint,) = set(hints)
    if re.fullmatch(r"[a-z_]+\(.*\)", hint):
        name, arguments = _hint_call(hint)
        return name, arguments, named | set(arguments)
    without_retain = hint.endswith(" without retain")
    if hint.startswith("search ") and without_retain:
        hint = hint[len("search "):-len(" without retain")]
    pairs = {m.group(1): _value(m.group(2)) for m in _ASSIGN.finditer(hint)}
    arguments = {k: v for k, v in sent.items() if not (without_retain and k == "retain")}
    if pairs:
        arguments.update(pairs)
        arguments.pop("next", None)
        return ("search" if without_retain else tool), arguments, named | set(pairs)
    # A token carries the read: it goes back alone (PACT read-model-s6-page-next).
    return tool, {"next": hint}, named | {"next"}


def test_the_continuation_reader_reads_every_form():
    sent = {"query": "x", "retain": True}
    line = "search ok · 5 rows · bound reached (limit 5) · next: offset=5"
    assert continuation(line, "search", sent) == (
        "search", {"query": "x", "retain": True, "offset": 5}, {"offset"},
    )
    line = 'x · next: offset=10 completeness="exact"'
    assert continuation(line, "search", {})[1] == {"offset": 10, "completeness": "exact"}
    line = "x · next: search offset=7 without retain"
    assert continuation(line, "search", sent) == ("search", {"query": "x", "offset": 7}, {"offset"})
    line = 'x · next: map(input={"ref": "r:1"}, offset=3)'
    assert continuation(line, "search", sent) == (
        "map", {"input": {"ref": "r:1"}, "offset": 3}, {"input", "offset"},
    )
    assert continuation("x · next: abc123", "filter", {"limit": 2}) == (
        "filter", {"next": "abc123"}, {"next"},
    )
    cut = "! page cut to fit max_bytes=2000; drop attrs from select, or page with offset\nx · next: none"
    assert continuation(cut, "search", {}) is None
    bound = ("! bound reached (id_rows 6): pass continuation_token for the next records, "
             "or narrow the query\nx · next: continuation_token=t.1")
    assert continuation(bound, "search", {"query": "q", "method": "grep"}) == (
        "search", {"query": "q", "method": "grep", "continuation_token": "t.1"},
        {"continuation_token"},
    )
    exact = ('! order exact through record 20; rerun with expand_concepts=false once per '
             'word of ["a", "b"] (separate rankings), or narrow the query, to see later '
             "records\nx · next: offset=20")
    assert continuation(exact, "search", {"query": "a"})[2] == {"offset", "expand_concepts"}
    assert continuation("x · complete", "search", {}) is None


#: Calls whose first page is cut on the `test_search_list` corpus.
CUT_CALLS: list[tuple[str, dict]] = [
    ("search", {"query": TERM, "limit": 5}),
    ("search", {"query": TERM, "select": ["node_id", "attrs"], "limit": 40, "max_bytes": 3000}),
    ("search", {"query": TERM, "method": "grep", "limit": 3}),
    ("search", {"query": TERM, "retain": True, "limit": 5}),
    ("filter", {"where": {"type": "doc"}, "select": ["node_id"], "limit": 5}),
    ("map", {"where": {"type": "doc"}, "select": ["node_id", "attrs.path"], "limit": 5}),
    ("aggregate", {"where": {"type": "doc"}, "by": ["attrs.path"], "metrics": ["count"],
                   "limit": 5}),
    ("expand", {"input": "doc:000", "edge_type": "contains", "limit": 1}),
]


def _follow(tools: Tools, published: dict[str, set[str]], tool: str, sent: dict) -> dict:
    """Send `sent` as text, follow its continuation, check both."""
    (result,) = tools.run((tool, sent, "text"))
    text = result.content[0].text
    assert text.startswith(f"{tool} ok"), text[:600]
    hinted = continuation(text, tool, sent)
    assert hinted is not None, ("the first page is not cut", text[:800])
    name, arguments, named = hinted
    unpublished = named - published[name]
    assert not unpublished, (
        f"the reply names {sorted(unpublished)}, which {name}'s published schema lacks",
        text[:800],
    )
    reply = _json(tools, name, arguments)
    assert reply.get("status") != "refused" and "error" not in reply, (
        name, arguments, json.dumps(reply)[:800],
    )
    return reply


@pytest.mark.parametrize("tool,sent", CUT_CALLS, ids=[
    f"{tool}-{i}" for i, (tool, _) in enumerate(CUT_CALLS)
])
def test_a_cut_reply_names_a_published_call_that_runs(tools, tool, sent):
    _follow(tools, _published(tools), tool, sent)


def test_search_offset_and_max_chars_do_what_they_say(tools):
    """`offset` and `max_chars` were accepted while hidden. Accepted is not
    enough: `offset` moves the page and `max_chars` shortens the excerpts;
    neither is ignored or refused."""
    first = _json(tools, "search", {"query": TERM, "limit": 3})
    later = _json(tools, "search", {"query": TERM, "limit": 3, "offset": 3})
    ids = [[row["node_id"] for row in page["results"]] for page in (first, later)]
    assert len(ids[1]) == 3 and not set(ids[0]) & set(ids[1]), ids
    short = _json(tools, "search", {"query": "Long record title", "max_chars": 64, "limit": 3})
    assert short.get("status") != "refused", json.dumps(short)[:600]
    excerpts = [row.get("excerpt") for row in short["results"] if row.get("excerpt")]
    assert excerpts and all(len(e) <= 64 + 3 for e in excerpts), excerpts
    for tool, arguments in [
        ("filter", {"where": {"type": "doc"}, "select": ["node_id"], "limit": 2, "offset": 2}),
        ("map", {"where": {"type": "doc"}, "select": ["node_id"], "limit": 2, "offset": 2}),
        ("aggregate", {"where": {"type": "doc"}, "by": ["attrs.path"], "metrics": ["count"],
                       "limit": 2, "offset": 2}),
        ("expand", {"input": "doc:000", "edge_type": "contains", "limit": 1, "offset": 1}),
    ]:
        reply = _json(tools, tool, arguments)
        assert reply.get("status") != "refused" and "error" not in reply, (
            tool, json.dumps(reply)[:600],
        )


def test_a_retained_page_cut_by_bytes_continues_by_a_published_map_input(tools):
    """A retained search's reference is read with the published
    `map(input={"ref": R}, offset=N)`, and the call is served with the
    reference's later members. Fails while `map.input` is hidden
    (a2e8cb9baa0).

    Under read-passage-rows-3666 (D4) a search page in text keeps every row
    and shortens passages to fit `max_bytes`, so the text page delivers
    every member and ends with search's own `next: offset=N`; its hint
    names only published arguments and runs. The JSON page is still cut by
    bytes: it says `page_reference` and the reference's `next_offset`, and
    `map(input={"ref": R}, offset=<that>)` is published and serves the
    members the JSON page did not deliver."""
    published = _published(tools)
    sent = {"query": TERM, "retain": True, "select": ["node_id", "attrs"],
            "limit": 40, "max_bytes": 3000}
    (result,) = tools.run(("search", sent, "text"))
    text = result.content[0].text
    hinted = continuation(text, "search", sent)
    assert hinted is not None, text[:900]
    name, arguments, named = hinted
    assert not named - published[name], (hinted, text[:900])
    served = _json(tools, name, arguments)
    assert served.get("status") != "refused" and "error" not in served, (
        arguments, json.dumps(served)[:800],
    )
    page = _json(tools, "search", sent)
    reference = page["result_reference"]
    # The JSON page is cut by bytes: the reference pages the rest.
    assert page.get("next_offset_reason") == "page_reference", page.get("next_offset_reason")
    delivered = reference["next_offset"]
    assert isinstance(delivered, int) and delivered > 0, reference
    first = [row["node_id"] for row in page["results"]]
    # map.input is published (read-live-handles also publishes filter.input
    # for handles, so filter is no longer asserted to lack it).
    assert "input" in published["map"]
    call = {"input": {"ref": reference["id"]}, "offset": delivered}
    assert set(call) <= published["map"]
    reply = _json(tools, "map", call)
    assert reply.get("status") != "refused" and "error" not in reply, (
        call, json.dumps(reply)[:800],
    )
    later = [row["node_id"] for row in reply["items"]]
    assert later and not set(first) & set(later), (first, later)


def test_the_link_count_hint_names_a_published_aggregate_input(tools):
    """aggregate by a link key is refused with the call that counts the
    links, `aggregate(input={"kind": "edges", "edge_type": E}, by=[...])`.
    `aggregate.input` is published, and that call is served."""
    assert "input" in _published(tools)["aggregate"]
    refused = _json(tools, "aggregate", {"where": {"node_id": ["doc:000", "doc:001"]},
                                         "by": ["link:contains:out"], "metrics": ["count"]})
    spelled = json.dumps(refused)
    assert "aggregate(input=" in spelled and "edges" in spelled, spelled[:800]
    reply = _json(tools, "aggregate", {"input": {"kind": "edges", "edge_type": "contains"},
                                       "by": ["dst"], "metrics": ["count"]})
    assert reply.get("status") != "refused" and "error" not in reply, json.dumps(reply)[:800]
    assert reply.get("groups") or reply.get("results"), json.dumps(reply)[:800]


def test_the_published_schema_carries_what_the_hints_name(tools):
    """The failure case: an argument a cut hint names and the schema lacks is
    caught by `_follow` before any call is sent."""
    published = _published(tools)
    assert {"offset", "max_chars"} <= published["search"]
    stripped = {name: set(fields) for name, fields in published.items()}
    stripped["search"].discard("offset")
    with pytest.raises(AssertionError, match="published schema lacks"):
        _follow(tools, stripped, "search", {"query": TERM, "limit": 5})


def test_a_grep_chain_stopped_at_its_scan_bound_continues_by_a_published_token(
        tools, monkeypatch):
    """A grep page stopped at its row bound, and every later page of that
    chain, ends `next: continuation_token=T` and says "send
    continuation_token with method="grep"" in its `! bound reached` notice
    (Format v10 G2 wording). Each hinted call is
    published and served, for three pages; the failure case strips the
    argument from the schema."""
    from okg.substrate.mcp import grep_search

    # A regex with no plain word is walked by node id; small bounds make the
    # walk stop at its row bound on this corpus (as test_search_grep_rank).
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", 3)
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 2)
    monkeypatch.setattr(grep_search, "PAGE_ID_ROWS", 6)
    published = _published(tools)
    assert "continuation_token" in published["search"]
    sent: dict[str, Any] = {"query": r"re:/zircon\w+/", "method": "grep", "limit": 6}
    seen: list[str] = []
    for _page in range(3):
        (result,) = tools.run(("search", sent, "text"))
        text = result.content[0].text
        assert text.startswith("search ok"), text[:600]
        assert ("! bound reached (id_rows 6): for the next records send "
                'continuation_token with method="grep"') in text, text[-900:]
        hinted = continuation(text, "search", sent)
        assert hinted is not None, text[:900]
        name, arguments, named = hinted
        # Format v10 names `method` beside the token; both must be published.
        assert named == {"continuation_token", "method"}, (named, text[:900])
        assert not named - published[name], (named, text[:900])
        reply = _json(tools, name, arguments)
        assert reply.get("status") != "refused" and "error" not in reply, (
            arguments, json.dumps(reply)[:800],
        )
        rows = [row["node_id"] for row in reply["results"]]
        assert rows and not set(rows) & set(seen), (rows, seen)
        seen += rows
        sent = arguments
    stripped = {name: set(fields) for name, fields in published.items()}
    stripped["search"].discard("continuation_token")
    with pytest.raises(AssertionError, match="published schema lacks"):
        _follow(tools, stripped, "search",
                {"query": r"re:/zircon\w+/", "method": "grep", "limit": 6})


_WORDS = (
    "transfer request rule dataset replica site queue workflow campaign "
    "deadline storage tape disk subscription block file stageout failure "
    "retry agent operator ticket review"
).split()


def _long_corpus(tmp_path, dsn: str, records: int = 60) -> str:
    """`records` documents of about 3,000 characters, each matching TERM,
    and one document held as chunks (`test_page_next_db`) for `inspect`."""
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    nodes = []
    for i in range(records):
        body = " ".join(_WORDS[(i + k) % len(_WORDS)] for k in range(400))
        nodes.append(_node(
            f"doc:{i:03d}", "doc", f"{TERM} " * (1 + i % 4) + body,
            summary=f"Long document {i:03d}", path=f"long/{i:03d}.md",
            notes=body[:1500],
        ))
    nodes.append(_node("doc:big", "document", "the big document", title="Big"))
    edges = []
    for i, text in enumerate(CHUNK_TEXTS * 2):
        chunk = f"doc:big:chunk:{i}"
        nodes.append(_node(chunk, "document_chunk", text, chunk_index=i, chunk_ordinal=i,
                           chunk_total=len(CHUNK_TEXTS) * 2))
        edges.append(_edge("doc:big", chunk))
    publish_nodes(dsn, deployment, subtypes=[DOC, DOC_CHUNK, TEXT_DOC, TEXT_CHUNK], nodes=nodes,
                  narrowings=[("doc", "contains", "doc_chunk"),
                              ("document", "contains", "document_chunk")], edges=edges)
    _trust(dsn, deployment, MAIN)
    return deployment


def test_on_long_records_short_excerpts_fit_far_more_rows(clean_runtime, okg_mcp_can_log_in,
                                                          tmp_path, capsys):
    """The call shapes the comp-ops runs chose, at their median `max_bytes`
    (12,000): whole attrs (e1, `max_chars` unpublished) against 1,200- and
    300-character excerpts (the run before the diet). Prints rows served and
    cut pages."""
    server = Tools(clean_runtime, _long_corpus(tmp_path, clean_runtime))
    try:
        published = _published(server)
        shapes = {
            "select attrs": {"select": ["node_id", "attrs"]},
            "max_chars 1200": {"max_chars": 1200},
            "max_chars 300": {"max_chars": 300},
        }
        rows: dict[str, list[int]] = {}
        cut: dict[str, int] = {}
        shortened: dict[str, int] = {}
        pages = [(limit, offset) for limit in (10, 20) for offset in (0, 20, 40)]
        for label, shape in shapes.items():
            rows[label], cut[label], shortened[label] = [], 0, 0
            for limit, offset in pages:
                reply = _json(server, "search", {
                    "query": TERM, "limit": limit, "offset": offset, "max_bytes": 12000,
                    **shape,
                })
                assert reply.get("status") != "refused", json.dumps(reply)[:600]
                rows[label].append(len(reply["results"]))
                cut[label] += bool(reply.get("truncation_reason")
                                   or "response_byte_budget" in reply)
                # A 20-row page of long records is over 12,000 bytes whole.
                shortened[label] += limit == 20 and "passages_shortened_to" in reply
        with capsys.disabled():
            print(f"\nlong-record pages, max_bytes 12000, (limit, offset) {pages}:")
            for label in shapes:
                print(f"  {label}: rows {rows[label]}, cut {cut[label]}, "
                      f"shortened {shortened[label]} of {len(pages)}")
        # read-passage-rows-3666 (D4, D6): a search page never drops a row
        # to fit, whole attrs selected or not. Every requested row is
        # served, its values shortened (down to head lines and the row's
        # floor fields), and the page says so. Before (f9634f77389): whole
        # attrs served 15 of 20 rows, and 1,200- or 300-character passages
        # 12 of 20, because 20 head-only rows with every JSON row field
        # measured over 12,000 bytes.
        for label in shapes:
            assert rows[label] == [limit for limit, _ in pages], rows
            assert cut[label] == 0, cut
            assert shortened[label] == 3, shortened
        # The text form says it on the status line.
        (result,) = server.run(("search", {
            "query": TERM, "limit": 20, "max_bytes": 12000, "select": ["node_id", "attrs"],
        }, "text"))
        status = result.content[0].text.split("\n", 1)[0]
        assert "rows 1-20" in status and "to fit" in status, status
        # The cut page continues with a published argument, which runs.
        _follow(server, published, "search", {
            "query": TERM, "limit": 20, "max_bytes": 12000, "select": ["node_id", "attrs"],
        })
        # Reading one long record's text past a byte cut continues too.
        sent = {"target": {"kind": "text", "node_id": "doc:big"}, "max_bytes": 4096}
        (result,) = server.run(("inspect", sent, "text"))
        text = result.content[0].text
        hinted = continuation(text, "inspect", sent)
        assert hinted is not None, text[-800:]
        name, arguments, named = hinted
        assert not named - published[name], (named, text[-800:])
        reply = _json(server, name, arguments)
        assert reply.get("status") != "refused" and "error" not in reply, (
            arguments, json.dumps(reply)[:600],
        )
    finally:
        server.close()


# --------------------------------------------------------------------------- #
# lane RD: select names read as what the record holds (issue #3666)           #
# --------------------------------------------------------------------------- #
# inspect(node) takes the page blocks `live_relations` and `provenance` in
# `select` and serves each exactly as the unselected page does; `attrs.<key>`
# narrows the attributes on inspect(node), inspect(nodes) and a 1-hop expand,
# and a key the record does not store is said. A bare name that is a stored
# attribute key of the rows is read as `attrs.<key>` with a note; one no row
# stores keeps the refusal.

#: One neutral live relation, as the catalog stores it (the whole compiled
#: declaration; `inspect` publishes the caller-facing subset).
_RELATION = {
    "deadline_seconds": 5,
    "action": "external.example.lookup",
    "sample_policy": "digest",
    "retention_class": "warm",
    "args": {"q": {"type": "string", "required": True}},
}


def _declare_live_relation(dsn: str, subtype: str) -> None:
    relations = parse_live_relations(
        {ANNOTATION_KEY: {"lookup": dict(_RELATION)}}, subtype=subtype,
    )
    stored = {name: relation.to_metadata() for name, relation in relations.items()}
    with psycopg.connect(dsn, autocommit=True) as conn:
        updated = conn.execute(
            "UPDATE okg.subtypes SET metadata = metadata || %s::jsonb WHERE name = %s",
            (json.dumps({"live_relations": stored}), subtype),
        ).rowcount
    assert updated >= 1


@pytest.fixture
def node_select_tools(clean_runtime, okg_mcp_can_log_in, tmp_path):
    deployment = _corpus(tmp_path, clean_runtime)
    _declare_live_relation(clean_runtime, "doc")
    server = Tools(clean_runtime, deployment)
    try:
        yield server
    finally:
        server.close()


def _rd_json(tools: Tools, tool: str, arguments: dict[str, Any]) -> tuple[bool, dict]:
    (result,) = tools.run((tool, arguments, "json"))
    return bool(result.isError), json.loads(result.content[0].text)


def _rd_node(node_id: str, **extra: Any) -> dict[str, Any]:
    return {"target": {"kind": "node", "node_id": node_id}, **extra}


def test_live_relations_alone_returns_the_listing_without_the_attributes(node_select_tools):
    _, whole = _rd_json(node_select_tools, "inspect", _rd_node("doc:000"))
    assert "lookup" in whole["live_relations"], whole
    failed, selected = _rd_json(node_select_tools, "inspect", _rd_node("doc:000", select=["live_relations"]))
    assert not failed, selected
    # The very block the page with no select serves, and no attributes.
    assert selected["live_relations"] == whole["live_relations"]
    assert "attrs" not in selected
    assert selected["node_id"] == "doc:000" and selected["subtype"] == "doc"
    # The text prints the listing as one `live_relations:` line, the same
    # line on the page with no select and on the selected page.
    text = node_select_tools.text("inspect", _rd_node("doc:000", select=["live_relations"]))
    whole_text = node_select_tools.text("inspect", _rd_node("doc:000"))

    def listing(page: str) -> list[str]:
        return [line for line in page.splitlines() if line.startswith("live_relations: ")]

    assert len(listing(text)) == 1 and "lookup" in listing(text)[0], text
    assert listing(text) == listing(whole_text), (text, whole_text)
    # No attribute lines: the record's `summary` is on the whole page only.
    assert any(line.startswith("summary: ") for line in whole_text.splitlines()), whole_text
    assert not any(line.startswith("summary: ") for line in text.splitlines()), text


def test_provenance_is_the_block_the_page_serves(node_select_tools):
    _, whole = _rd_json(node_select_tools, "inspect", _rd_node("doc:000"))
    failed, selected = _rd_json(node_select_tools, "inspect", _rd_node("doc:000", select=["provenance"]))
    assert not failed, selected
    assert selected["provenance"] == whole["provenance"]
    assert "attrs" not in selected and "live_relations" not in selected


def test_live_relations_on_a_type_that_declares_none_says_so(node_select_tools):
    failed, reply = _rd_json(node_select_tools, "inspect", _rd_node("ticket:1", select=["live_relations"]))
    assert not failed, reply
    assert "live_relations" not in reply
    assert {
        "rule": "select_page_block", "sent": "select `live_relations`",
        "served": "no listing: this record's type declares no live relations",
    } in reply["adjusted"]


def test_attrs_key_narrows_the_record_and_names_a_key_it_does_not_store(node_select_tools):
    failed, reply = _rd_json(
        node_select_tools, "inspect", _rd_node("doc:000", select=["attrs.summary", "attrs.missing_key"]),
    )
    assert not failed, reply
    assert set(reply["attrs"]) - {"nomos_redacted"} == {"summary"}
    assert "Document 000" in json.dumps(reply["attrs"]["summary"])
    assert reply["attrs_not_stored"] == ["attrs.missing_key"]
    assert "summary" in reply["attr_keys"] and "opened_at" in reply["attr_keys"]
    text = node_select_tools.text(
        "inspect", _rd_node("doc:000", select=["attrs.summary", "attrs.missing_key"]),
    )
    assert "! not stored on this record: attrs.missing_key; it has attrs" in text, text


def test_attrs_key_on_a_batch_narrows_each_record(node_select_tools):
    failed, reply = _rd_json(node_select_tools, "inspect", {
        "target": {"kind": "nodes", "node_ids": ["doc:000", "doc:001"]},
        "select": ["attrs.path"],
    })
    assert not failed, reply
    paths = {record["node_id"]: record["attrs"] for record in reply["records"]}
    assert {k: set(v) - {"nomos_redacted"} for k, v in paths.items()} == {
        "doc:000": {"path"}, "doc:001": {"path"},
    }


def test_attrs_key_on_one_hop_expand_narrows_the_linked_records(node_select_tools):
    failed, reply = _rd_json(node_select_tools, "expand", {
        "input": {"node_id": "doc:000"}, "edge_type": "contains",
        "select": ["node_id", "attrs.text"],
    })
    assert not failed, reply
    rows = reply["neighbors"]
    assert [row["node_id"] for row in rows] == [
        "doc_chunk:000-0", "doc_chunk:000-1", "doc_chunk:000-2",
    ]
    assert all(set(row["attrs"]) - {"nomos_redacted"} == {"text"} for row in rows), rows


def test_a_bare_stored_key_is_read_as_attrs_key_on_inspect_and_search(node_select_tools):
    failed, node = _rd_json(node_select_tools, "inspect", _rd_node("doc:000", select=["opened_at"]))
    assert not failed, node
    assert set(node["attrs"]) - {"nomos_redacted"} == {"opened_at"}
    assert any(record["rule"] == "select_attr_key" for record in node["adjusted"]), node
    failed, page = _rd_json(node_select_tools, "search", {
        "query": TERM, "limit": 3, "select": ["node_id", "opened_at"],
    })
    assert not failed, page
    assert any("attrs.opened_at" in (row.get("selected") or {}) for row in page["results"])
    assert any(record["rule"] == "select_attr_key" for record in page["adjusted"]), page


def test_a_bare_name_no_record_stores_keeps_the_refusal(node_select_tools):
    failed, reply = _rd_json(node_select_tools, "inspect", _rd_node("doc:000", select=["missing_key"]))
    assert failed and reply["error"]["code"] == "select_unknown_field", reply
    assert not any(
        record.get("rule") == "select_attr_key" for record in reply.get("adjusted") or ()
    )


def test_date_created_in_a_search_select_is_the_handle(node_select_tools):
    failed, page = _rd_json(node_select_tools, "search", {
        "query": TERM, "limit": 3, "select": ["node_id", "date.created"],
    })
    assert not failed, page
    assert all("handle" in row for row in page["results"]), page
    assert any(record["sent"] == "select `date.created`" for record in page["adjusted"])
