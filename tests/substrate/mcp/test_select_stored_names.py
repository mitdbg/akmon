"""`select` names read as what the rows hold (issue #3666, lane RD).

No database. Agents selected names the reply had shown them but the tools
refused: a key from a record's attributes (`created_at`), one attribute as
`attrs.<key>` on expand, a `where` date (`date.created`), and an
inspect(graph) block by a plain word (`types`). Each is now served with an
`! adjusted:` note, or keeps its refusal with words that name the attrs form.
A bare name no served row stores keeps the tool's own refusal: never an
empty page.

The record read's page blocks (`live_relations`, `provenance`) and its
`attrs.<key>` are proved on a real record in the lane RD section of test_read_grammar_db.py.
"""

from __future__ import annotations

import copy
from typing import Any

import anyio
import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from okg.substrate.mcp import result_text
from okg.substrate.mcp.envelope import SelectUnknownField, validate_select
from okg.substrate.mcp.read_grammar import rewrite_call
from okg.substrate.mcp.response_bounds import ByteBoundedFastMCP, bound_read_tool
from okg.substrate.mcp.select_vocabulary import served_as_selected
from okg.substrate.mcp.tools import expand_record_rows
from tests.substrate.mcp._result_payload import (
    JSON_META,
    payload as _result_payload,
    result_text as _result_text,
)


def _rewritten(tool: str, arguments: dict[str, Any]):
    before = copy.deepcopy(arguments)
    call = rewrite_call(tool, arguments)
    assert arguments == before, "the caller's arguments are never changed in place"
    assert rewrite_call(tool, arguments) == call
    return call


# -- expand: one attribute of the linked record ------------------------------ #

_RECORDS = [
    {"node_id": "n:2", "subtype": "doc",
     "attrs": {"status": "open", "summary": "long text", "nomos_redacted": True},
     "provenance": {"trust": "trusted"}},
    {"node_id": "n:3", "subtype": "doc", "attrs": {"summary": "other"},
     "provenance": {"trust": "trusted"}},
]
_LINKS = {
    node_id: [{"edge_type": "cites", "direction": "out", "edge_id": f"e:{node_id}",
               "src": "n:1", "dst": node_id}]
    for node_id in ("n:2", "n:3")
}


def test_a_one_hop_attrs_key_is_left_as_sent_and_served_by_its_rows():
    arguments = {"input": "n:1", "select": ["node_id", "attrs.status"]}
    call = _rewritten("expand", arguments)
    assert call.arguments == arguments and call.adjusted == ()
    assert served_as_selected("expand", arguments)
    rows = expand_record_rows(
        copy.deepcopy(_RECORDS), _LINKS, ["node_id", "attrs.status"],
        titles={}, link_attrs={},
    )
    # Narrowed to the named key; the redaction marker always travels.
    assert rows[0]["attrs"] == {"status": "open", "nomos_redacted": True}
    assert rows[1]["attrs"] == {}
    assert all(row["provenance"] == {"trust": "trusted"} for row in rows)


def test_attrs_beside_an_attrs_key_keeps_the_whole_map():
    rows = expand_record_rows(
        copy.deepcopy(_RECORDS), _LINKS, ["attrs", "attrs.status"],
        titles={}, link_attrs={},
    )
    assert rows[0]["attrs"] == _RECORDS[0]["attrs"]


def test_a_key_no_row_stores_is_said_on_the_expand_page():
    page = {
        "status": "ok", "node_id": "n:1", "direction": "out", "total_count": 2,
        "neighbors": expand_record_rows(
            copy.deepcopy(_RECORDS), _LINKS, ["node_id", "attrs.label"],
            titles={}, link_attrs={},
        ),
    }
    text = result_text.render(page, tool="expand", select=["node_id", "attrs.label"])
    assert "! no row on this page has attrs.label" in text, text


def test_a_link_field_select_still_returns_link_rows():
    """Only names that are all link fields ask for link rows: `attrs.<key>`
    is a record field, so the shape is the linked records."""
    from okg.substrate.mcp.select_vocabulary import call_shape

    assert call_shape("expand", {"input": "n:1", "select": ["edge_id", "dst"]}) == "edges"
    assert call_shape("expand", {"input": "n:1", "select": ["attrs.status"]}) == "records"


# -- search: `where` dates in a select -------------------------------------- #

def test_date_created_is_read_as_the_handle_with_a_note():
    call = _rewritten("search", {"query": "q", "select": ["node_id", "date.created"]})
    assert call.arguments["select"] == ["node_id", "handle"]
    assert {
        "rule": "select_renamed", "sent": "select `date.created`",
        "served": "`handle`, which holds the title, path and date",
    } in call.adjusted


def test_date_attrs_key_is_read_as_the_stored_attribute():
    call = _rewritten("search", {"query": "q", "select": ["node_id", "date.attrs.closed_at"]})
    assert call.arguments["select"] == ["node_id", "attrs.closed_at"]
    assert {
        "rule": "select_renamed", "sent": "select `date.attrs.closed_at`",
        "served": "`attrs.closed_at`, the stored attribute",
    } in call.adjusted
    line = result_text._adjusted_notice({"adjusted": list(call.adjusted)})
    assert line == (
        "! adjusted: select `date.attrs.closed_at` read as `attrs.closed_at`, "
        "the stored attribute"
    )


def test_another_where_date_is_refused_naming_the_attrs_form():
    with pytest.raises(SelectUnknownField) as refused:
        validate_select(
            ["node_id", "date.resolved"], ["node_id", "handle"],
            tool_name="search(method=lexical)",
        )
    said = str(refused.value) + " " + str(refused.value.fix_hint)
    assert "a where date field, not a row field" in said
    assert "attrs.<key>" in said
    assert refused.value.data["retry_select"] == ["node_id"]


# -- inspect(graph): blocks by a plain word --------------------------------- #

@pytest.mark.parametrize("sent,served", [
    (["types"], ["subtypes"]),
    (["relations", "embed_lag"], ["edge_types", "embed_lag"]),
    (["counts"], ["subtypes", "edge_types"]),
    (["subtypes", "counts"], ["subtypes", "edge_types"]),
    (["subtype_counts", "edge_type_counts"], ["subtypes", "edge_types"]),
    (["schema_digest"], ["subtypes", "edge_types"]),
])
def test_a_graph_block_named_by_a_plain_word_is_served_with_a_note(sent, served):
    call = _rewritten("inspect", {"target": {"kind": "graph"}, "select": sent})
    assert call.arguments["select"] == served
    assert any(
        record["rule"] == "select_second_name" for record in call.adjusted
    ), call.adjusted
    from okg.substrate.mcp.tools import INSPECT_GRAPH_BLOCKS

    validate_select(call.arguments["select"], INSPECT_GRAPH_BLOCKS, tool_name="inspect(graph)")


@pytest.mark.parametrize("sent", [["sources"], ["dates"], ["subtypes"]])
def test_a_word_with_no_single_block_is_left_for_the_refusal(sent):
    """`sources` and `dates` mean no one block: refused, with the blocks."""
    arguments = {"target": {"kind": "graph"}, "select": sent}
    assert _rewritten("inspect", arguments).arguments == arguments


# -- inspect(node): a key the record does not store -------------------------- #

def test_the_node_page_says_which_selected_keys_the_record_does_not_store():
    payload = {
        "status": "ok", "node_id": "n:1", "subtype": "doc",
        "attrs": {"status": "open"}, "provenance": {"trust": "trusted"},
        "attrs_not_stored": ["attrs.label"], "attr_keys": ["status", "summary"],
        "generation_id": "g:1", "pinned_generation_id": "g:1",
    }
    text = result_text.render(payload, tool="inspect", kind="node")
    assert "! not stored on this record: attrs.label; it has attrs status, summary" in text
    assert "other fields" not in text, text


def test_a_key_the_access_rules_may_have_withheld_is_never_said_not_stored():
    """Review P2-3: when the read mask dropped keys (free text, the
    redaction marker on `attrs`), a selected key the read lacks may be stored
    and withheld; the line says so instead of "not stored"."""
    from okg.substrate.mcp.tools import _narrow_node_attrs
    from okg.substrate.nomos.read_masking import REDACTION_MARKER

    assert result_text._REDACTION_MARKER == REDACTION_MARKER
    payload = {
        "status": "ok", "node_id": "n:1", "subtype": "doc",
        "attrs": {"status": "open", REDACTION_MARKER: True},
        "provenance": {"trust": "trusted"},
        "generation_id": "g:1", "pinned_generation_id": "g:1",
    }
    _narrow_node_attrs(payload, ["summary"], whole=False)
    assert payload["attrs_not_stored"] == ["attrs.summary"]
    assert payload["attr_keys"] == ["status"]
    text = result_text.render(payload, tool="inspect", kind="node")
    assert (
        "! not stored on this record, or withheld by access rules: attrs.summary; "
        "it has attrs status"
    ) in text, text
    # Failure case: the claim "not stored on this record:" is never made
    # for a masked read.
    assert "! not stored on this record: " not in text, text


# -- a bare name that is a stored attribute key ------------------------------ #

_ROW_FIELDS = ("node_id", "subtype", "attrs")
_STORED = {"n:2": {"created_at": "2025-01-02T03:04:05Z", "path": "a/b.txt"}}


def _server() -> tuple[ByteBoundedFastMCP, list[list[str]]]:
    """A server whose expand and inspect serve fixed rows, validating
    `select` as the record reads do: `attrs.<key>` narrows the attributes."""
    server = ByteBoundedFastMCP("select-stored-names-test")
    seen: list[list[str]] = []

    def narrowed(attrs: dict[str, Any], select: list[str]) -> dict[str, Any]:
        keys = [name[len("attrs."):] for name in select if name.startswith("attrs.")]
        return {key: attrs[key] for key in keys if key in attrs} if keys else attrs

    def check(select: Any, tool_name: str) -> None:
        keys = [name for name in select or () if name.startswith("attrs.")]
        validate_select(select, (*_ROW_FIELDS, *keys), tool_name=tool_name)

    def expand(input: Any, select: Any = None, max_bytes: Any = None) -> dict[str, Any]:
        del input, max_bytes
        seen.append(list(select or ()))
        check(select, "expand")
        return {
            "status": "ok", "node_id": "n:1", "direction": "out", "total_count": 1,
            "truncated": False,
            "neighbors": [{
                "node_id": "n:2", "subtype": "doc", "title": None,
                "attrs": narrowed(_STORED["n:2"], list(select or ())),
                "link": {"edge_type": "cites", "direction": "out", "edge_id": "e:1"},
                "link_count": 1,
            }],
        }

    def inspect(target: Any, select: Any = None, max_bytes: Any = None) -> dict[str, Any]:
        del max_bytes
        seen.append(list(select or ()))
        check(select, "inspect(node)")
        return {
            "status": "ok", "node_id": target["node_id"], "subtype": "doc",
            "attrs": narrowed(_STORED["n:2"], list(select or ())),
            "provenance": {"trust": "trusted"},
            "generation_id": "g:1", "pinned_generation_id": "g:1",
        }

    server.tool(name="expand", structured_output=False)(bound_read_tool("expand", expand))
    server.tool(name="inspect", structured_output=False)(bound_read_tool("inspect", inspect))
    return server, seen


def _calls(*calls: tuple[str, dict[str, Any], bool]) -> list[Any]:
    server, _ = _server()
    out: list[Any] = []

    async def drive() -> None:
        async with create_connected_server_and_client_session(server) as session:
            for tool, arguments, as_json in calls:
                out.append(await session.call_tool(
                    tool, arguments, **({"meta": JSON_META} if as_json else {}),
                ))

    anyio.run(drive)
    return out


def test_a_bare_stored_key_on_expand_is_read_as_attrs_key_with_a_note():
    call = {"input": {"node_id": "n:1"}, "select": ["node_id", "created_at"]}
    served, text = _calls(("expand", call, True), ("expand", call, False))
    assert served.isError is False, _result_text(served)
    reply = _result_payload(served)
    assert reply["neighbors"][0]["attrs"] == {"created_at": "2025-01-02T03:04:05Z"}
    assert {
        "rule": "select_attr_key", "sent": "select `created_at`",
        "served": "`attrs.created_at`, an attribute the rows store",
    } in reply["adjusted"]
    said = _result_text(text)
    assert (
        "! adjusted: select `created_at` read as `attrs.created_at`, "
        "an attribute the rows store"
    ) in said, said


def test_a_bare_stored_key_on_inspect_node_is_read_as_attrs_key():
    call = {"target": {"kind": "node", "node_id": "n:2"}, "select": ["path"]}
    (served,) = _calls(("inspect", call, True))
    assert served.isError is False, _result_text(served)
    reply = _result_payload(served)
    assert reply["attrs"] == {"path": "a/b.txt"}
    assert any(record["rule"] == "select_attr_key" for record in reply["adjusted"])


@pytest.mark.parametrize("tool,call", [
    # No row stores the key: the tool's own refusal, never an empty page.
    ("expand", {"input": {"node_id": "n:1"}, "select": ["node_id", "label"]}),
    ("inspect", {"target": {"kind": "node", "node_id": "n:2"}, "select": ["label"]}),
    # One name stored, one not: refused whole.
    ("expand", {"input": {"node_id": "n:1"}, "select": ["created_at", "label"]}),
    # A name the refusal corrects is never read as an attribute.
    ("expand", {"input": {"node_id": "n:1"}, "select": ["id"]}),
    # The text read is the answer for `text`.
    ("inspect", {"target": {"kind": "node", "node_id": "n:2"}, "select": ["text"]}),
])
def test_a_bare_name_no_row_stores_keeps_the_refusal(tool, call):
    (refused,) = _calls((tool, call, True))
    assert refused.isError is True, _result_text(refused)
    reply = _result_payload(refused)
    assert reply["error"]["code"] == "select_unknown_field"
    assert not any(
        record.get("rule") == "select_attr_key" for record in reply.get("adjusted") or ()
    ), reply


# -- the retry never reaches outside the graph, and reads its own call ------- #


def test_no_bare_key_retry_for_a_live_expand_or_a_hydrating_inspect():
    """Review P2-1: `live=` and `hydrate` call an external system (and write
    its cache); a retry that may be discarded never runs them again."""
    from okg.substrate.mcp.response_bounds import bare_names_as_attr_keys

    refusal = SelectUnknownField("no field", data={"unknown_fields": ["created_at"]})
    expand_call = {"input": {"node_id": "n:1"}, "select": ["node_id", "created_at"]}
    inspect_call = {"target": {"kind": "node", "node_id": "n:2"}, "select": ["created_at"]}
    assert bare_names_as_attr_keys("expand", expand_call, refusal) is not None
    assert bare_names_as_attr_keys("inspect", inspect_call, refusal) is not None
    # Failure cases: the same calls reaching an external system get no retry.
    live = {**expand_call, "live": {"relation": "tickets", "args": {}}}
    assert bare_names_as_attr_keys("expand", live, refusal) is None
    # #3666, lane RD: a caller that reads the keys on the graph rows first
    # (no external call) may ask for the live expand's retry.
    retried, keys = bare_names_as_attr_keys("expand", live, refusal, allow_live=True)
    assert retried["select"] == ["node_id", "attrs.created_at"] and keys == ["created_at"]
    assert retried["live"] == live["live"]
    for hydrate in (True, ["status"]):
        hydrating = {**inspect_call, "target": {**inspect_call["target"], "hydrate": hydrate}}
        assert bare_names_as_attr_keys("inspect", hydrating, refusal) is None


def _recording_server() -> tuple[ByteBoundedFastMCP, list[dict[str, Any]]]:
    """An expand that records each run: the select it was given, its `live`,
    and the call state the tool body reads (`call_arguments`)."""
    from okg.substrate.mcp.response_bounds import call_arguments, call_sent_select

    server = ByteBoundedFastMCP("select-stored-names-retry-test")
    runs: list[dict[str, Any]] = []

    def expand(
        input: Any, select: Any = None, live: Any = None, max_bytes: Any = None,
    ) -> dict[str, Any]:
        del input, max_bytes
        runs.append({
            "select": list(select or ()), "live": live,
            "call_select": (call_arguments() or {}).get("select"),
            "sent_select": call_sent_select(),
        })
        keys = [name for name in select or () if name.startswith("attrs.")]
        validate_select(select, (*_ROW_FIELDS, *keys), tool_name="expand")
        return {
            "status": "ok", "node_id": "n:1", "direction": "out", "total_count": 1,
            "truncated": False,
            "neighbors": [{
                "node_id": "n:2", "subtype": "doc", "title": None,
                "attrs": dict(_STORED["n:2"]),
                "link": {"edge_type": "cites", "direction": "out", "edge_id": "e:1"},
                "link_count": 1,
            }],
        }

    server.tool(name="expand", structured_output=False)(bound_read_tool("expand", expand))
    return server, runs


def _drive_one(server: ByteBoundedFastMCP, tool: str, arguments: dict[str, Any]) -> Any:
    out: list[Any] = []

    async def drive() -> None:
        async with create_connected_server_and_client_session(server) as session:
            out.append(await session.call_tool(tool, arguments, meta=JSON_META))

    anyio.run(drive)
    return out[0]


def test_the_bare_key_retry_reads_its_own_call_state_and_never_reruns_a_live_expand():
    """Review P2-1. Before: the retry ran under the first call's rewrite
    state, so its body read the first call's arguments (the bare names a
    path page's `next` token would sign), and a `live=` expand ran its live
    call a second time. After: the retry reads its own call; a live expand
    is not retried."""
    server, runs = _recording_server()
    call = {"input": {"node_id": "n:1"}, "select": ["node_id", "created_at"]}
    served = _drive_one(server, "expand", call)
    assert served.isError is False, _result_text(served)
    assert len(runs) == 2, runs
    first, retry = runs
    assert first["call_select"] == ["node_id", "created_at"]
    # The retry's body reads the new call, never the first call's bare names.
    assert retry["select"] == ["node_id", "attrs.created_at"]
    assert retry["call_select"] == ["node_id", "attrs.created_at"]
    assert retry["sent_select"] is None
    reply = _result_payload(served)
    assert any(record["rule"] == "select_attr_key" for record in reply["adjusted"])


def test_a_live_expand_reads_the_keys_on_the_graph_rows_before_calling_out():
    """#3666, lane RD: a live expand that selects bare stored keys is served
    (5 of 269 real comp-ops expand(live=...) calls carried bare names on a
    live expand). The keys are read on the graph rows WITHOUT `live`; only
    then is the call made with `live`, once, and served. Before: the live
    expand was refused `select_unknown_field`."""
    server, runs = _recording_server()
    live = {"relation": "t", "args": {}}
    call = {"input": {"node_id": "n:1"}, "select": ["node_id", "created_at"], "live": live}
    served = _drive_one(server, "expand", call)
    assert served.isError is False, _result_text(served)
    assert [run["live"] for run in runs] == [live, None, live], runs
    first, probe, read = runs
    assert probe["select"] == read["select"] == ["node_id", "attrs.created_at"]
    reply = _result_payload(served)
    assert any(record["rule"] == "select_attr_key" for record in reply["adjusted"])
    # One select_attr_key note: the probe's own notes never reach the reply.
    assert sum(record["rule"] == "select_attr_key" for record in reply["adjusted"]) == 1


def test_a_live_expand_whose_rows_lack_the_key_never_calls_out_again():
    """Failure case: the graph rows do not store the key, so the live call
    is never made again and the first refusal stands."""
    server, runs = _recording_server()
    live = {"relation": "t", "args": {}}
    refused = _drive_one(server, "expand", {
        "input": {"node_id": "n:1"}, "select": ["node_id", "no_such_key"], "live": live,
    })
    assert refused.isError is True, _result_text(refused)
    assert _result_payload(refused)["error"]["code"] == "select_unknown_field"
    assert [run["live"] for run in runs] == [live, None], runs


def test_external_live_in_a_live_expand_select_is_read_as_the_live_rows():
    """Regression guard (#3666, lane RD): select ["external_live"] (and with
    node_id) on a live expand is served, read as the live rows
    (rule live_rows_select), exactly as Format v10 served it."""
    for select, sent in ((["external_live"], []), (["node_id", "external_live"], ["node_id"])):
        server, runs = _recording_server()
        live = {"relation": "t", "args": {}}
        served = _drive_one(server, "expand", {
            "input": {"node_id": "n:1"}, "select": select, "live": live,
        })
        assert served.isError is False, _result_text(served)
        assert len(runs) == 1 and runs[0]["live"] == live, runs
        assert runs[0]["select"] == sent, runs
        reply = _result_payload(served)
        assert any(record["rule"] == "live_rows_select" for record in reply["adjusted"]), reply
