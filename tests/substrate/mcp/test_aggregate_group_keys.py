"""`aggregate`'s group keys, checked by the read grammar (issue #3666).

No database. A `by` key the set cannot be grouped by is refused by name with
the keys that set takes; a key naming the far end of a link gets the call
that counts those links where that call is exact; `each(attrs.<key>)` is a
key of its own, read before date periods. The served refusals are raised
before any connection is opened.
"""
from __future__ import annotations

from typing import Any

import anyio
import pytest

from okg.substrate.mcp.aggregate_each import (
    EACH_FORM,
    each_attribute,
    facts,
    parse_each_field,
)
from okg.substrate.mcp.aggregate_periods import parse_period_field
from okg.substrate.mcp.read_grammar import GrammarRefusal, link_key, rewrite_call
from okg.substrate.mcp.response_bounds import InvalidOperatorArguments

COUNT = {"metrics": ["count"]}


def _served(arguments: dict[str, Any]) -> dict[str, Any]:
    call = rewrite_call("aggregate", {**arguments, **COUNT})
    assert call.adjusted == ()
    return call.arguments


def _refused(arguments: dict[str, Any]) -> GrammarRefusal:
    with pytest.raises(GrammarRefusal) as refused:
        rewrite_call("aggregate", {**arguments, **COUNT})
    return refused.value


# --------------------------------------------------------------------------- #
# keys a set is grouped by                                                     #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("by", [
    ["subtype"], ["node_id"], ["text_hash"], ["attrs.status"], ["attrs.Size (MB)"],
    ["month(date.created)"], ["year(date.attrs.due)"], ["each(attrs.files)"],
    ["attrs.team", "each(attrs.files)"],
    # A malformed period is the period parser's to refuse, with its form.
    ["quarter(date.created)"],
])
def test_a_record_set_takes_its_columns_attributes_periods_and_each(by):
    assert _served({"where": {"type": "t"}, "by": by})["by"] == by


@pytest.mark.parametrize("by", [
    ["src"], ["dst"], ["edge_type"], ["edge_id"], ["attrs.weight"],
    ["month(date.attrs.at)"], ["each(attrs.tags)"],
])
def test_a_link_set_takes_its_columns_attributes_periods_and_each(by):
    assert _served({"input": {"kind": "edges"}, "by": by})["by"] == by


@pytest.mark.parametrize("arguments", [
    # One record's links keep their own two keys, judged by the tool.
    {"input": "n:1", "by": ["edge_type", "dst_subtype"]},
    {"input": {"node_id": "n:1"}, "by": ["edge_type", "dst_subtype"]},
    # The older count of every link by its ends' types.
    {"input": {"kind": "edges"}, "by": ["src_subtype", "edge_type", "dst_subtype"]},
    # A live result set has an outside system's columns.
    {"input": {"ref": "l1.abc"}, "by": ["anything"]},
])
def test_calls_that_are_not_a_grouped_graph_set_are_passed_on(arguments):
    assert _served(arguments)["by"] == arguments["by"]


def test_an_unknown_key_is_refused_by_name_with_the_keys_of_that_set():
    refusal = _refused({"where": {"type": "t"}, "by": ["attrs.status", "dst.project"]})
    assert refusal.message == (
        "aggregate: by `dst.project` is not a group key of a set of records; a "
        "set of records is grouped by node_id, subtype, text_hash, attrs.<key>, "
        "each(attrs.<key>), or <period>(date.<name>) with <period> one of day, "
        "week, month, year"
    )
    assert refusal.fields == ("by[1]",)
    assert "group the links by `src` or `dst`" in refusal.fix_hint
    assert refusal.retry_arguments is None
    links = _refused({"input": {"kind": "edges", "edge_type": "e"}, "by": ["src_subtype"]})
    assert "a set of links is grouped by dst, edge_id, edge_type, src, attrs.<key>" in (
        links.message)
    assert "<period>(date.attrs.<key>)" in links.message
    assert '["src_subtype", "edge_type", "dst_subtype"]' in links.fix_hint
    assert links.retry_arguments is None


def test_every_unknown_key_is_named_once():
    refusal = _refused({"where": {"type": "t"}, "by": ["kind", "date.created", "Subtype"]})
    assert "by `kind`, `date.created` and `Subtype` are not group keys" in refusal.message
    assert refusal.fields == ("by[0]", "by[1]", "by[2]")
    assert "`month(date.created)`" in refusal.fix_hint
    # The date has no single fix: no call is offered.
    assert refusal.retry_arguments is None


@pytest.mark.parametrize(("arguments", "fixed", "mode"), [
    ({"where": {"type": "t"}, "by": ["type"]}, ["subtype"], "corrected"),
    ({"where": {"type": "t"}, "by": ["id"]}, ["node_id"], "corrected"),
    ({"where": {"type": "t"}, "by": ["Subtype"]}, ["subtype"], "corrected"),
    ({"input": {"kind": "edges"}, "by": ["type"]}, ["edge_type"], "corrected"),
    ({"where": {"type": "t"}, "by": ["status"]}, ["attrs.status"], "guess"),
])
def test_a_key_with_one_fix_is_refused_with_the_call_that_has_it(arguments, fixed, mode):
    refusal = _refused(arguments)
    assert refusal.retry_arguments == {**arguments, **COUNT, "by": fixed}
    assert refusal.retry_mode == mode
    # The offered call is itself taken as written.
    assert _served(refusal.retry_arguments)["by"] == fixed


def test_a_fix_that_would_name_one_key_twice_is_not_offered():
    refusal = _refused({"where": {"type": "t"}, "by": ["subtype", "type"]})
    assert refusal.retry_arguments is None
    assert "`subtype` is already in by" in refusal.fix_hint


def test_each_naming_a_bare_word_gets_words_not_a_call():
    """Whether `files` holds a list is known only to the read, which refuses
    each() on a value that is not one: the fix is said, never offered."""
    refusal = _refused({"where": {"type": "t"}, "by": ["each(files)"]})
    assert refusal.retry_arguments is None
    assert "write `each(attrs.files)`" in refusal.fix_hint


def test_each_is_one_list_per_call_and_not_beside_a_period():
    two = _refused({"where": {"type": "t"}, "by": ["each(attrs.a)", "each(attrs.b)"]})
    assert "two each() keys" in two.message
    period = _refused({"where": {"type": "t"}, "by": ["each(attrs.a)", "month(date.created)"]})
    assert "each() beside a date period" in period.message
    bad = _refused({"where": {"type": "t"}, "by": ["each(date.attrs.a)"]})
    assert f"each() takes one attribute: {EACH_FORM}" in bad.fix_hint
    assert bad.retry_arguments is None


# --------------------------------------------------------------------------- #
# the far end of a link                                                        #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(("key", "parsed"), [
    ("link:assigned_to:in", ("assigned_to", "in")),
    ("link(assigned_to:out)", ("assigned_to", "out")),
    ("link:a:b:out", ("a:b", "out")),
    ("link:assigned_to", ("", "")),
    ("link(assigned_to:sideways)", ("", "")),
    ("link(assigned_to:in", ("", "")),
    ("attrs.link", None),
    ("linked", None),
])
def test_a_link_key_names_one_type_and_direction(key, parsed):
    assert link_key(key) == parsed


def test_a_link_key_on_all_records_gets_the_count_of_those_links():
    refusal = _refused({"input": {"kind": "nodes"}, "by": ["link(contains:in)"]})
    assert refusal.retry_arguments == {
        "input": {"kind": "edges", "edge_type": "contains"}, "by": ["src"], **COUNT,
    }
    # It counts links, and says what that means for records.
    assert refusal.retry_mode == "guess"
    assert "it counts links, not records" in refusal.fix_hint
    assert "a record with no contains link is in no group" in refusal.fix_hint
    assert _served(refusal.retry_arguments)["by"] == ["src"]


def test_conditions_on_record_ids_move_onto_the_near_end():
    refusal = _refused({
        "where": {"op": "or", "clauses": [
            {"id": "doc:1"}, {"id": {"op": "prefix", "value": "doc:9"}}]},
        "by": ["link:contains:out"], "limit": 5,
    })
    assert refusal.retry_arguments == {
        "input": {"kind": "edges", "edge_type": "contains", "filter": {
            "op": "or", "clauses": [
                {"op": "eq", "field": "src", "value": "doc:1"},
                {"op": "prefix", "field": "src", "value": "doc:9"},
            ]}},
        "by": ["dst"], "limit": 5, **COUNT,
    }


@pytest.mark.parametrize(("arguments", "cannot"), [
    ({"where": {"type": "git_commit"}, "by": ["link:authored:in"]}, "type"),
    ({"input": {"kind": "nodes", "subtype": "git_commit"}, "by": ["link:authored:in"]}, "type"),
    ({"where": {"attrs.repo": "r"}, "by": ["link:authored:in"]}, "conditions"),
    ({"input": {"kind": "nodes"}, "by": ["link:authored:in", "attrs.repo"]},
     "other group keys"),
    ({"input": {"ref": "r1.abc"}, "by": ["link:authored:in"]}, "result reference"),
])
def test_a_link_key_gets_words_where_the_count_of_links_is_not_exact(arguments, cannot):
    refusal = _refused(arguments)
    assert refusal.retry_arguments is None
    assert refusal.fix_hint.startswith(
        'aggregate(input={"kind": "edges", "edge_type": "authored"}, by=["src"])')
    assert f"so this call's {cannot} cannot be carried over and no call is offered" in (
        refusal.fix_hint)
    assert "groups records by the record at the other end of their authored links" in (
        refusal.message)


def test_a_link_key_naming_no_type_names_the_link_set_form():
    refusal = _refused({"where": {"type": "t"}, "by": ["link:assigned_to"]})
    assert "names a link" in refusal.message
    assert '"edge_type":"<link type>"' in refusal.fix_hint
    assert refusal.retry_arguments is None
    edges = _refused({"input": {"kind": "edges"}, "by": ["link:x:in"]})
    assert "grouped by its own ends: `src` or `dst`" in edges.fix_hint


# --------------------------------------------------------------------------- #
# each() is read before periods                                                #
# --------------------------------------------------------------------------- #


def test_each_is_never_read_as_a_date_period():
    assert parse_period_field("each(attrs.files)") is None
    assert parse_period_field("each(files)") is None
    assert each_attribute("each(attrs.files)") == "files"
    assert each_attribute("each(attrs.)") is None
    assert parse_each_field("attrs.files") is None
    assert parse_each_field("each(attrs.files)").attr == "files"
    with pytest.raises(InvalidOperatorArguments) as refused:
        parse_each_field("each(files)")
    assert refused.value.data == {"field": "each(files)", "expected": EACH_FORM}
    assert "'is not a period'" not in str(refused.value)


def test_the_reply_facts_have_fixed_names():
    assert facts(rows_counted=7, rows_with_none=3, rows_not_a_list=1) == {
        "rows_counted": 7, "rows_with_none": 3, "rows_not_a_list": 1,
        "groups_overlap": True,
    }


# --------------------------------------------------------------------------- #
# served refusals                                                              #
# --------------------------------------------------------------------------- #

# from pieces so the public-copy export does not read it as a credential.
DEAD_DSN = "postgresql://postgres" + ":okg" + "@localhost:1/okg_probe"


@pytest.fixture
def mcp_server(monkeypatch: pytest.MonkeyPatch):
    from okg.substrate.mcp.server import build_server

    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_RO_DSN", DEAD_DSN)
    server = build_server(dsn=DEAD_DSN)
    try:
        yield server
    finally:
        server._okg_pool.close()
        if server._okg_write_pool:
            server._okg_write_pool.close()


def _refusal(server: Any, arguments: dict[str, Any]) -> tuple[dict[str, Any], str]:
    from tests.substrate.mcp._result_payload import call_json, payload

    async def run() -> tuple[Any, Any]:
        return (
            await call_json(server, "aggregate", arguments),
            await server.call_tool("aggregate", arguments),
        )

    as_json, as_text = anyio.run(run)
    assert as_json.isError is True and as_text.isError is True
    reply = payload(as_json)
    assert reply["status"] == "refused"
    assert reply["error"]["code"] == "invalid_operator_arguments"
    return reply, as_text.content[0].text


def test_the_served_refusal_names_the_keys_and_carries_the_link_count(mcp_server):
    reply, text = _refusal(mcp_server, {"input": {"kind": "nodes"},
                                        "by": ["link(contains:in)"], **COUNT})
    assert reply["error"]["retry_call"] == {
        "tool": "aggregate", "mode": "guess", "arguments": {
            "input": {"kind": "edges", "edge_type": "contains"}, "by": ["src"], **COUNT,
        },
    }
    assert 'aggregate(input={"kind": "edges", "edge_type": "contains"}, by=["src"]' in text
    reply, text = _refusal(mcp_server, {"where": {"type": "t"}, "by": ["kind"], **COUNT})
    assert "a set of records is grouped by node_id, subtype, text_hash" in text
    assert reply["error"]["retry_call"]["arguments"]["by"] == ["attrs.kind"]


def test_a_count_needing_the_read_to_judge_its_keys_is_not_offered(mcp_server):
    reply, _ = _refusal(mcp_server, {"where": {"Type": "t"},
                                     "by": ["month(date.created)"], **COUNT})
    assert "retry_call" not in reply["error"], reply["error"]


def test_under_access_rules_no_count_is_offered(mcp_server, monkeypatch):
    """Every set count is refused under audit and enforce: the refusal gives
    words, and never a call that would be refused in turn."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    for by in (["link(contains:in)"], ["link:authored:in"], ["kind"]):
        reply, text = _refusal(mcp_server, {"input": {"kind": "nodes"}, "by": by, **COUNT})
        assert "retry_call" not in reply["error"], reply["error"]
        # No refused call is named: the words say counting is not available.
        assert reply["error"]["fix_hint"] == (
            "counting is not available on this deployment: access rules are on "
            "(audit or enforce), and aggregate refuses every count under them; "
            "filter or map read a bounded page"
        )
        assert "aggregate(" not in text, text


def test_when_the_access_rule_mode_cannot_be_read_nothing_is_claimed(mcp_server, monkeypatch):
    """A failed mode lookup offers no count and does not say access rules are
    on: the plain refusal is served."""
    from okg.substrate.nomos import runtime_enforcement

    def unreadable(deployment_name: str) -> bool:
        raise RuntimeError("mode unreadable")

    monkeypatch.setattr(runtime_enforcement, "deployment_requires_nomos_runtime", unreadable)
    reply, text = _refusal(mcp_server, {"input": {"kind": "nodes"},
                                        "by": ["link(contains:in)"], **COUNT})
    assert "retry_call" not in reply["error"], reply["error"]
    assert "access rules" not in text, text
    assert "it counts links, not records" in reply["error"]["fix_hint"]
