"""The shared read grammar as an argument rewrite (issue #3666, step 2a).

No database. The lowering function and the per-tool rewrite are pure; the
served refusals are raised before any connection is opened.
"""
from __future__ import annotations

import copy
import json
import sys
from typing import Any

import anyio
import pytest

from okg.substrate.mcp import live_result_sets, read_grammar, result_text
from okg.substrate.mcp.operator_sql import EFFECTIVE_VIEW_FIELD_COLUMNS
from okg.substrate.mcp.read_grammar import (
    DATE_CONDITION_TOOLS,
    EDGE_FIELDS,
    LIVE_FIELDS,
    MAX_ADJUSTED,
    MAX_ADJUSTED_BYTES,
    MORE_NOT_LISTED,
    NODE_FIELDS,
    SEARCH_FIELDS,
    GrammarRefusal,
    cap_adjusted,
    lower_condition,
    render_call,
    rewrite_call,
)
from okg.substrate.mcp.search_where import normalize_where

EQ_STATUS = {"op": "eq", "field": "attrs.status", "value": "Open"}


def _lowered(condition: Any, **kwargs: Any) -> Any:
    before = copy.deepcopy(condition)
    lowered, adjusted = lower_condition(condition, **kwargs)
    assert condition == before, "the caller's condition is never changed in place"
    assert adjusted == []
    return lowered


def _refused(condition: Any, **kwargs: Any) -> GrammarRefusal:
    with pytest.raises(GrammarRefusal) as refused:
        lower_condition(condition, **kwargs)
    return refused.value


# --------------------------------------------------------------------------- #
# the short form                                                               #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("value", ["Open", 3, 2.5, True, False, None])
def test_a_plain_value_means_equals(value):
    assert _lowered({"attrs.x": value}) == {"op": "eq", "field": "attrs.x", "value": value}


def test_a_list_means_any_of():
    assert _lowered({"attrs.status": ["Open", "In Progress"]}) == {
        "op": "in", "field": "attrs.status", "values": ["Open", "In Progress"],
    }


def test_a_test_object_applies_to_the_keys_field():
    assert _lowered(
        {"date.created": {"op": "between", "values": ["2025-01-01", "2025-12-31"]}},
        fields=SEARCH_FIELDS,
    ) == {"op": "between", "field": "date.created", "values": ["2025-01-01", "2025-12-31"]}
    assert _lowered({"attrs.resolution": {"op": "exists"}}) == {
        "op": "exists", "field": "attrs.resolution",
    }
    assert _lowered({"path": {"op": "suffix", "value": ".py"}}, fields=SEARCH_FIELDS) == {
        "op": "suffix", "field": "path", "value": ".py",
    }


def test_an_object_that_is_not_a_test_is_matched_exactly():
    # A one-key object with a bare-word key is refused (D1, loose-match-3666):
    # an object with several keys, or none, is a value.
    assert _lowered({"attrs.owner": {"name": "a", "id": 7}}) == {
        "op": "eq", "field": "attrs.owner", "value": {"name": "a", "id": 7},
    }
    assert _lowered({"attrs.owner": {}}) == {"op": "eq", "field": "attrs.owner", "value": {}}


def test_several_keys_all_hold_in_the_order_written():
    sent = {"type": "jira_issue", "attrs.status": "Open", "attrs.n": [1, 2]}
    assert _lowered(sent, fields=SEARCH_FIELDS) == {
        "op": "and",
        "clauses": [
            {"op": "eq", "field": "type", "value": "jira_issue"},
            EQ_STATUS,
            {"op": "in", "field": "attrs.n", "values": [1, 2]},
        ],
    }


def test_the_empty_condition_keeps_its_meaning():
    assert _lowered({}) == {}
    # Inside `clauses` it is left for the refusal it gets today.
    assert _lowered({"op": "and", "clauses": [{}, {"attrs.status": "Open"}]}) == {
        "op": "and", "clauses": [{}, EQ_STATUS],
    }


@pytest.mark.parametrize("value", [None, "$1", 3, True, ["attrs.x"], [{"attrs.x": 1}]])
def test_a_value_that_is_not_an_object_is_left_alone(value):
    assert _lowered(value) == value


LONG_FORM_TREES = [
    {"op": "and", "clauses": [
        {"op": "eq", "field": "subtype", "value": "git_commit"},
        {"op": "not", "clause": {"op": "in", "field": "attrs.k", "values": [1, 2]}},
        {"op": "or", "clauses": [
            {"op": "exists", "field": "attrs.a"},
            {"op": "contains", "field": "attrs", "value": {"type": "x", "op": "y"}},
        ]},
    ]},
    # `type` and `id` as long-form fields, and a name no set has: a tool's
    # existing argument carries them on untouched.
    {"op": "and", "clauses": [
        EQ_STATUS,
        {"op": "not", "clause": {"op": "eq", "field": "type", "value": "x"}},
        {"op": "or", "clauses": [
            {"op": "in", "field": "id", "values": ["a"]},
            {"op": "prefix", "field": "status", "value": "Op"},
        ]},
    ]},
    {"op": "eq", "field": "type", "value": "x"},
    {"op": "exists", "field": "id"},
]


@pytest.mark.parametrize("tree", LONG_FORM_TREES)
@pytest.mark.parametrize("fields", [NODE_FIELDS, EDGE_FIELDS, LIVE_FIELDS, SEARCH_FIELDS])
def test_a_long_form_tree_comes_back_exactly_as_sent(tree, fields):
    assert _lowered(tree, fields=fields, argument="predicate") == tree


def test_short_forms_nest_inside_and_or_not():
    assert _lowered({
        "op": "and",
        "clauses": [
            {"type": "git_commit"},
            {"op": "not", "clause": {"attrs.is_merge": True}},
            {"op": "or", "clauses": [{"attrs.a": 1, "attrs.b": 2}, {"attrs.c": [3]}]},
        ],
    }) == {
        "op": "and",
        "clauses": [
            {"op": "eq", "field": "subtype", "value": "git_commit"},
            {"op": "not", "clause": {"op": "eq", "field": "attrs.is_merge", "value": True}},
            {"op": "or", "clauses": [
                {"op": "and", "clauses": [
                    {"op": "eq", "field": "attrs.a", "value": 1},
                    {"op": "eq", "field": "attrs.b", "value": 2},
                ]},
                {"op": "in", "field": "attrs.c", "values": [3]},
            ]},
        ],
    }


def test_an_operand_is_data_and_is_never_read_as_a_condition():
    clause = {"op": "eq", "field": "attrs.x", "value": {"type": "y", "status": [1]}}
    assert _lowered(clause, new_spelling=True) == clause
    many = {"op": "in", "field": "attrs.x", "values": [{"type": "y"}, {"field": "a", "value": 1}]}
    assert _lowered(many, new_spelling=True) == many


def test_type_and_id_are_second_names_only_in_the_new_spelling():
    # Short-form keys on a node set.
    sent = {"type": "doc", "id": ["a", "b"], "subtype": "doc", "node_id": "a"}
    assert _lowered(sent, argument="predicate") == {
        "op": "and",
        "clauses": [
            {"op": "eq", "field": "subtype", "value": "doc"},
            {"op": "in", "field": "node_id", "values": ["a", "b"]},
            {"op": "eq", "field": "subtype", "value": "doc"},
            {"op": "eq", "field": "node_id", "value": "a"},
        ],
    }
    # A long-form clause: renamed in a `where`, never in an older argument.
    long_form = {"op": "prefix", "field": "id", "value": "doc:"}
    assert _lowered(long_form, new_spelling=True) == {
        "op": "prefix", "field": "node_id", "value": "doc:",
    }
    assert _lowered(long_form, argument="predicate") == long_form
    # A long-form clause beside a short-form one keeps its own name.
    mixed = {"op": "and", "clauses": [{"type": "doc"}, {"op": "eq", "field": "type", "value": "x"}]}
    assert _lowered(mixed, argument="filter")["clauses"] == [
        {"op": "eq", "field": "subtype", "value": "doc"},
        {"op": "eq", "field": "type", "value": "x"},
    ]
    # An attribute that happens to be called `type` is a different field.
    assert _lowered({"attrs.type": "x"}) == {"op": "eq", "field": "attrs.type", "value": "x"}
    # Search keeps both of its own names.
    assert _lowered({"type": "doc", "id": "a"}, fields=SEARCH_FIELDS, new_spelling=True) == {
        "op": "and", "clauses": [
            {"op": "eq", "field": "type", "value": "doc"},
            {"op": "eq", "field": "id", "value": "a"},
        ],
    }


# --------------------------------------------------------------------------- #
# a short-form key must be a field                                             #
# --------------------------------------------------------------------------- #


def test_the_field_lists_are_the_columns_the_code_reads():
    # `text` is read by the text condition (`text_match`), not a column.
    assert NODE_FIELDS.names == frozenset({*EFFECTIVE_VIEW_FIELD_COLUMNS["nodes"], "text"})
    assert EDGE_FIELDS.names == frozenset(EFFECTIVE_VIEW_FIELD_COLUMNS["edges"])
    assert set(NODE_FIELDS.aliases.values()) <= NODE_FIELDS.names
    assert set(EDGE_FIELDS.instead.values()) <= EDGE_FIELDS.names
    assert LIVE_FIELDS.names == frozenset()
    # Every name search's short form takes is a field search's `where` reads.
    for name in sorted(SEARCH_FIELDS.names):
        assert normalize_where({"op": "eq", "field": name, "value": "x"}) is not None, name


@pytest.mark.parametrize("fields", [NODE_FIELDS, EDGE_FIELDS, LIVE_FIELDS, SEARCH_FIELDS])
def test_any_attribute_name_is_a_field_and_no_bare_word_is(fields):
    assert fields.known("attrs.status")
    for name in ("attrs", "attrs.", "date", "date.", "status", "and", "or", "not", ""):
        assert not fields.known(name), name
    assert fields.known("date.created") is fields.dates
    assert fields.known("date.attrs.due") is fields.dates


def test_a_date_is_a_field_only_where_the_tool_compares_dates():
    """Item 1: one set says which tools compare dates. The dates step
    widened it to every read operator; a live result set has no date field."""
    assert DATE_CONDITION_TOOLS == {"search", "aggregate", "filter", "map", "expand"}
    assert SEARCH_FIELDS.dates
    assert not NODE_FIELDS.dates and not EDGE_FIELDS.dates and not LIVE_FIELDS.dates
    for tool in ("filter", "map", "expand", "aggregate", "search"):
        assert NODE_FIELDS.on(tool).dates is True, tool
        assert NODE_FIELDS.on(tool).tool == tool
    served = _rewritten("filter", {"where": {"date.created": "2025-01-01"}})
    assert served.arguments == {
        "input": {"kind": "nodes"},
        "predicate": {"op": "eq", "field": "date.created", "value": "2025-01-01"},
    }
    assert served.adjusted == ()
    for tool, arguments in (
        ("filter", {"where": {"type": "t", "date.created": {"op": "gt", "value": "2025"}}}),
        ("filter", {"where": {"op": "gt", "field": "date.created", "value": "2025"}}),
        ("filter", {"where": {"field": "date.created", "value": "2025-01-01"}}),
        ("filter", {"input": {"kind": "nodes"}, "predicate": {"date.created": "2025-01-01"}}),
        ("map", {"where": {"date.modified": "2025-01-01"}}),
        ("map", {"input": {"kind": "nodes", "filter": {"date.attrs.due": "2025-01-01"}}}),
        ("expand", {"input": "n:1", "where": {"date.created": "2025-01-01"}}),
        ("expand", {"input": "n:1", "filter": {"date.created": "2025-01-01"}}),
        ("filter", {"input": {"kind": "edges"}, "where": {"date.attrs.due": "2025-01-01"}}),
    ):
        lowered = json.dumps(_rewritten(tool, arguments).arguments)
        assert '"field": "date.' in lowered, (tool, arguments, lowered)
    # A live result set holds an outside system's rows: no date field, on
    # any tool, and the refusal says what to compare instead.
    for tool in ("filter", "map", "aggregate"):
        refusal = _call_refused(tool, {
            "input": {"ref": "l1.x"}, "where": {"date.created": "2025-01-01"},
            **({"by": ["attrs.a"], "metrics": ["count"]} if tool == "aggregate" else {}),
        })
        assert refusal.message == (
            "where: `date.created` is a date field, which reads graph records; "
            "this set holds an outside system's rows"
        ), tool
        assert refusal.fix_hint == (
            "compare the set's own column, written with the attrs. prefix "
            "(compared as text)"
        )
        assert refusal.retry_arguments is None
    # A named date on a set of links stays refused, on the widened tools too.
    for tool in ("filter", "map"):
        refusal = _call_refused(tool, {
            "input": {"kind": "edges"}, "where": {"date.created": "2025-01-01"},
        })
        assert "is a date declared per record type; this set has none" in refusal.message
    # The same key on aggregate and search, which compared dates before.
    between = {"op": "between", "values": ["2025-01-01", "2025-12-31"]}
    counted = _rewritten("aggregate", {
        "where": {"type": "t", "date.created": between},
        "by": ["month(date.created)"], "metrics": ["count"],
    })
    assert counted.arguments["input"] == {
        "kind": "nodes", "subtype": "t", "filter": {"field": "date.created", **between},
    }
    searched = _rewritten("search", {"query": "q", "where": {"date.created": between}})
    assert searched.arguments["where"] == {"field": "date.created", **between}
    # A long-form date clause in an argument a tool already had is untouched
    # on expand, and on filter (step 2c checks it: a date is a field there).
    today = {"input": "n:1", "filter": {"op": "gt", "field": "date.created", "value": "2025"}}
    assert _rewritten("expand", today).arguments == today
    in_predicate = {"input": {"kind": "nodes"},
                    "predicate": {"op": "gt", "field": "date.created", "value": "2025"}}
    assert _rewritten("filter", in_predicate).arguments == in_predicate
    # A named date in a long-form clause on a set of links is refused by name.
    refused = _call_refused("filter", {
        "input": {"kind": "edges"},
        "predicate": {"op": "gt", "field": "date.created", "value": "2025"}})
    assert "is a date declared per record type; this set has none" in refused.message


def test_a_key_that_is_not_a_field_is_refused_with_the_attribute_it_likely_meant():
    sent = {"type": "jira_issue", "attrs.priority": "High", "status": "Open"}
    refusal = _refused(sent)
    assert refusal.message == (
        "where: `status` is not a field this tool compares; a condition here "
        "reads id, node_id, subtype, text, text_hash, type, attrs.<name>"
    )
    # A guess, and the words say so.
    assert refusal.fix_hint == "if `status` is a stored attribute, write `attrs.status`"
    assert refusal.repairable
    # The outcome does not depend on the other keys.
    for other in ({"status": "Open"}, {"status": "Open", "type": "jira_issue"},
                  {"op": "and", "clauses": [{"attrs.a": 1}, {"status": ["Open"]}]},
                  {"op": "not", "clause": {"status": {"op": "prefix", "value": "O"}}}):
        assert "`status` is not a field" in _refused(other, argument="predicate").message


@pytest.mark.parametrize("fields,key,hint", [
    # The set itself says which field is meant.
    (EDGE_FIELDS, "type", "this set's field is `edge_type`"),
    (NODE_FIELDS, "Type", "this set's field is `type`"),
    (NODE_FIELDS, "NODE_ID", "this set's field is `node_id`"),
    (NODE_FIELDS, "SubType", "this set's field is `subtype`"),
    (SEARCH_FIELDS, "Path", "this set's field is `path`"),
    # Nothing better is known: a guess, said as one.
    (LIVE_FIELDS, "type", "if `type` is a stored attribute, write `attrs.type`"),
    (LIVE_FIELDS, "node_id", "if `node_id` is a stored attribute, write `attrs.node_id`"),
    (SEARCH_FIELDS, "status", "if `status` is a stored attribute, write `attrs.status`"),
])
def test_each_set_refuses_a_key_it_has_no_field_for(fields, key, hint):
    refusal = _refused({key: "x"}, fields=fields)
    assert f"`{key}` is not a field" in refusal.message
    assert refusal.fix_hint == hint and refusal.repairable


@pytest.mark.parametrize("fields,key,words", [
    (NODE_FIELDS, "source", "a field of search's `where` only"),
    (NODE_FIELDS, "title", "a field of search's `where` only"),
    (NODE_FIELDS, "path", "a field of search's `where` only"),
    (NODE_FIELDS, "source_family", "an argument of search"),
    (NODE_FIELDS, "edge_type", "a field of an edge set"),
    (NODE_FIELDS, "src", "a field of an edge set"),
    (EDGE_FIELDS, "subtype", "a field of a record"),
    (EDGE_FIELDS, "node_id", "a field of a record"),
    (EDGE_FIELDS, "title", "a field of search's `where` only"),
    # Item E: the edge's own id, or the record at either end.
    (EDGE_FIELDS, "id", "name `edge_id`, `src` or `dst`"),
])
def test_a_name_another_tool_or_argument_reads_is_pointed_there_in_words(fields, key, words):
    """Item 4: no `attrs.<name>` guess for a name that has a home."""
    refusal = _refused({key: "x"}, fields=fields)
    assert f"`{key}` is not a field" in refusal.message
    assert words in refusal.fix_hint and "attrs." + key not in refusal.fix_hint
    assert not refusal.repairable


def test_a_link_set_has_date_attributes_and_no_named_dates_on_aggregate():
    """m5: aggregate compares a link's date attribute, and says a named
    date is a record's, not that dates are still to come."""
    between = {"op": "between", "values": ["2025-01-01", "2025-12-31"]}
    served = _rewritten("aggregate", {
        "input": {"kind": "edges"}, "where": {"date.attrs.due": between},
        "by": ["month(date.attrs.due)"], "metrics": ["count"],
    })
    assert served.arguments["input"] == {
        "kind": "edges", "filter": {"field": "date.attrs.due", **between},
    }
    refusal = _call_refused("aggregate", {
        "input": {"kind": "edges"}, "where": {"date.created": "2025-01-01"},
        "by": ["edge_type"], "metrics": ["count"],
    })
    assert refusal.message == (
        "where: `date.created` is a date declared per record type; this set has none"
    )
    assert "date.attrs. prefix" in refusal.fix_hint and "dates step" not in refusal.fix_hint
    assert refusal.retry_arguments is None


@pytest.mark.parametrize("fields,key,words", [
    (EDGE_FIELDS, "ID", "name `edge_id`, `src` or `dst`"),
    (EDGE_FIELDS, "Id", "name `edge_id`, `src` or `dst`"),
    (EDGE_FIELDS, "SubType", "a field of a record"),
    (NODE_FIELDS, "Title", "a field of search's `where` only"),
    (NODE_FIELDS, "SOURCE", "a field of search's `where` only"),
    (NODE_FIELDS, "Edge_type", "a field of an edge set"),
    (NODE_FIELDS, "Source_Family", "an argument of search"),
])
def test_a_letter_case_variant_of_a_known_name_is_never_guessed(fields, key, words):
    """m4: `ID` on links and `Title` on records are the names they vary,
    pointed where those are pointed, not turned into `attrs.ID`."""
    refusal = _refused({key: "x"}, fields=fields)
    assert words in refusal.fix_hint and f"attrs.{key}" not in refusal.fix_hint
    assert not refusal.repairable
    tool_input = {"kind": "edges"} if fields is EDGE_FIELDS else {"kind": "nodes"}
    assert _call_refused("filter", {"input": tool_input, "where": {key: "x"}}).retry_arguments is None


def test_a_variant_of_a_field_is_corrected_to_the_field():
    assert _retry("filter", {"input": {"kind": "edges"}, "where": {"Edge_Type": "e"}}) == {
        "input": {"kind": "edges"}, "where": {"edge_type": "e"},
    }
    assert _retry("filter", {"input": {"kind": "edges"}, "where": {"TYPE": "e"}}) == {
        "input": {"kind": "edges"}, "where": {"edge_type": "e"},
    }
    assert _call_refused("filter", {"where": {"Node_Id": "n:1"}}).retry_mode == "corrected"


@pytest.mark.parametrize("arguments,name", [
    ({"where": {"Type": "a", "TYPE": "b"}}, "type"),
    ({"input": {"kind": "edges"}, "where": {"type": "a", "Edge_Type": "b"}}, "edge_type"),
    ({"where": {"status": "a", "Type": "t", "tYpe": "u"}}, "type"),
    ({"input": {"kind": "nodes"}, "predicate": {"NODE_ID": "a", "Node_Id": "b"}}, "node_id"),
])
def test_two_keys_that_come_to_one_name_get_no_retry(arguments, name):
    """M1: writing one over the other would drop a condition."""
    refusal = _call_refused("filter", arguments)
    assert refusal.retry_arguments is None
    assert f"two keys both name `{name}`" in refusal.fix_hint
    assert "cannot be told" in refusal.fix_hint


def test_every_cause_is_named_even_one_with_no_fix():
    """m3: a name with a home elsewhere beside a guess: both are said, in
    either order, and no call is offered."""
    for where in ({"source": "s", "status": "x"}, {"status": "x", "source": "s"}):
        refusal = _call_refused("filter", {"where": where})
        assert "a field of search's `where` only" in refusal.fix_hint
        assert "if `status` is a stored attribute, write `attrs.status`" in refusal.fix_hint
        assert refusal.retry_arguments is None
    bare = _call_refused("filter", {"where": {"attrs": {"a": 1}, "Type": "t"}})
    assert "cannot be told from the name" in bare.fix_hint
    assert "this set's field is `type`" in bare.fix_hint
    assert bare.retry_arguments is None


def test_a_test_object_carries_only_the_operand_its_operator_reads():
    """m2: the tools ignore `value` beside `exists`; the grammar does not."""
    refusal = _call_refused("filter", {"where": {"attrs.k": {"op": "exists", "value": False}}})
    assert refusal.message == (
        "where: the test on `attrs.k` carries `value`, which `exists` does not read"
    )
    assert '{"op": "not", "clause": {"attrs.k": {"op": "exists"}}}' in refusal.fix_hint
    assert refusal.retry_arguments is None
    for test in ({"op": "exists", "value": True}, {"op": "eq", "value": 1, "values": [1]},
                 {"op": "in", "value": 1, "values": [1]}, {"op": "between", "value": 3}):
        stray = _call_refused("filter", {"where": {"attrs.k": test}})
        assert "does not read" in stray.message and stray.retry_arguments is None
    for test in ({"op": "exists"}, {"op": "eq", "value": None}, {"op": "in", "values": [1]},
                 {"op": "between", "values": [1, 2]}, {"op": "prefix", "value": "a"}):
        assert _rewritten("filter", {"where": {"attrs.k": test}}).arguments["predicate"] == {
            "field": "attrs.k", **test,
        }
    # The long form in an argument a tool already had is passed on as it was.
    today = {"input": {"kind": "nodes"},
             "predicate": {"op": "exists", "field": "attrs.k", "value": False}}
    assert _rewritten("filter", today).arguments == today


def test_two_spellings_are_compared_as_json_not_as_python():
    """m1: `1` and `true` are different stored values."""
    scope = {"kind": "nodes"}
    for old, new in ((1, True), (0, False), (1, 1.0)):
        refusal = _call_refused("filter", {
            "input": scope, "predicate": {"op": "eq", "field": "attrs.n", "value": old},
            "where": {"attrs.n": new},
        })
        assert "both carry a condition and they differ" in refusal.message, (old, new)
    _call_refused("map", {"input": {"kind": "nodes", "filter": {"attrs.n": 1}},
                          "where": {"attrs.n": True}})
    _call_refused("expand", {"input": "n:1", "filter": {"attrs.n": [1]}, "where": {"attrs.n": [True]}})
    _call_refused("map", {"input": scope, "select": [1], "projection": [True]})
    same = _rewritten("filter", {
        "input": scope, "predicate": {"op": "eq", "field": "attrs.n", "value": True},
        "where": {"attrs.n": True},
    })
    assert same.arguments["predicate"]["value"] is True
    nested = {"target": {"kind": "node", "node_id": "n:1", "max_bytes": True}, "max_bytes": 1}
    assert _rewritten("inspect", nested).arguments == nested


def test_the_offered_call_names_authority_and_never_carries_it():
    """M3, in the grammar: the retry leaves `authority` for the caller."""
    authority = {"identity_claim": "CLAIM-MARK", "access_context": "CONTEXT-MARK"}
    refusal = _call_refused("filter", {"where": {"Type": "x"}, "authority": authority})
    assert refusal.retry_arguments == {"where": {"type": "x"}}
    assert refusal.retry_repeat == ("authority",)
    assert _call_refused("filter", {"where": {"Type": "x"}}).retry_repeat == ()
    renamed = _call_refused("map", {
        "input": {"kind": "nodes"}, "select": {"id": "node_id"}, "authority": authority,
    })
    assert renamed.retry_arguments == {"input": {"kind": "nodes"}, "projection": {"id": "node_id"}}
    assert renamed.retry_repeat == ("authority",)


def test_search_keeps_its_own_guidance_for_the_names_it_explains():
    """Item 4: `source_family` and a bare `date` get search's words."""
    family = _call_refused("search", {"query": "q", "where": {"source_family": "docs"}})
    assert "`where` field 'source_family' is not available on search here" in family.message
    assert "pass the `source_family` argument" in family.fix_hint
    assert family.retry_arguments is None
    bare = _call_refused("search", {"query": "q", "where": {"date": "2025-01-01"}})
    assert "date.created" in bare.fix_hint and bare.retry_arguments is None
    # Beside another mistake it is still not renamed into a retry.
    both = _call_refused("search", {"query": "q", "where": {"status": "x", "source_family": "d"}})
    assert both.retry_arguments is None


@pytest.mark.parametrize("sent", [
    {"attrs": {"status": "Open"}},
    {"date": "2025-01-01"},
    {"a b": 1},
    {"status": "Open", "attrs.status": "Closed"},
])
def test_a_key_with_no_likely_field_says_what_is_missing(sent):
    refusal = _refused(sent)
    assert "is not a field this tool compares" in refusal.message
    assert "cannot be told from the name" in refusal.fix_hint
    assert not refusal.repairable


@pytest.mark.parametrize("sent,word", [
    ({"or": [{"attrs.a": 1}, {"attrs.b": 2}]}, "or"),
    ({"and": [{"attrs.a": 1}]}, "and"),
    ({"not": {"attrs.a": 1}}, "not"),
    ({"type": "doc", "or": [{"attrs.a": 1}, {"attrs.b": 2}]}, "or"),
])
def test_a_logical_word_as_a_key_is_refused(sent, word):
    refusal = _refused(sent)
    assert f"`{word}` is not a field; it is written with `op`" in refusal.message
    assert refusal.repairable


@pytest.mark.parametrize("sent", [{"or": {"attrs.a": 1}}, {"and": []}, {"not": [{"attrs.a": 1}]}])
def test_a_logical_word_holding_the_wrong_shape_says_what_is_missing(sent):
    refusal = _refused(sent)
    assert "does not hold" in refusal.fix_hint and not refusal.repairable


# --------------------------------------------------------------------------- #
# reserved keys and the missing `op`                                           #
# --------------------------------------------------------------------------- #


def test_field_and_value_with_no_op_is_read_as_eq_and_recorded():
    lowered, adjusted = lower_condition(
        {"field": "type", "value": "jira_issue"}, fields=SEARCH_FIELDS,
    )
    assert lowered == {"op": "eq", "field": "type", "value": "jira_issue"}
    assert adjusted == [{
        "rule": "clause_without_op",
        "sent": "where clause on `type` with no op",
        "served": "eq",
    }]
    # In a `where` the second name is taken, and the note says that too.
    lowered, adjusted = lower_condition(
        {"op": "and", "clauses": [{"type": "doc"}, {"field": "id", "value": None}]},
        new_spelling=True,
    )
    assert lowered == {"op": "and", "clauses": [
        {"op": "eq", "field": "subtype", "value": "doc"},
        {"op": "eq", "field": "node_id", "value": None},
    ]}
    assert adjusted == [{
        "rule": "clause_without_op",
        "sent": "where.clauses[1] clause on `id` with no op",
        "served": "eq on `node_id`",
    }]


def test_a_clause_served_without_op_has_its_field_checked():
    """Item 2: the rewrite chooses to serve it, so it answers for the field."""
    bare = {"field": "status", "value": "Open"}
    for argument, new_spelling in (("predicate", False), ("where", True), ("filter", False)):
        refusal = _refused(bare, argument=argument, new_spelling=new_spelling)
        assert f"{argument}: `status` is not a field" in refusal.message
        assert refusal.fix_hint == "if `status` is a stored attribute, write `attrs.status`"
    # No second name inside an argument the tool already had, unless the
    # argument reads one vocabulary (`check_fields`: filter, map, aggregate).
    for field, column in (("type", "subtype"), ("id", "node_id")):
        refusal = _refused({"field": field, "value": "x"}, argument="predicate")
        assert refusal.message == f"predicate: `{field}` is written `{column}` in this argument"
        assert refusal.repairable
        lowered, _ = lower_condition(
            {"field": field, "value": "x"}, argument="predicate", check_fields=True,
        )
        assert lowered == {"op": "eq", "field": column, "value": "x"}
    assert _retry("filter", {"input": {"kind": "nodes"}, "predicate": bare}) == {
        "input": {"kind": "nodes"},
        "predicate": {"op": "eq", "field": "attrs.status", "value": "Open"},
    }
    # Step 2c: on filter the second name is read, and the note says so.
    noted = _rewritten("filter", {"input": {"kind": "nodes"},
                                  "predicate": {"field": "type", "value": "x"}})
    assert noted.arguments == {
        "input": {"kind": "nodes"}, "predicate": {"op": "eq", "field": "subtype", "value": "x"},
    }
    assert noted.adjusted[0]["served"] == "eq on `subtype`"
    assert _retry("expand", {"input": "n:1", "filter": {"field": "id", "value": "x"}}) == {
        "input": "n:1", "filter": {"op": "eq", "field": "node_id", "value": "x"},
    }
    # The same clause in `where` is served, with the whole change in the note.
    served = _rewritten("filter", {"where": {"field": "type", "value": "x"}})
    assert served.arguments == {"input": {"kind": "nodes", "subtype": "x"}, "predicate": {}}
    assert served.adjusted == ({
        "rule": "clause_without_op",
        "sent": "where clause on `type` with no op",
        "served": "eq on `subtype`",
    },)
    plain = _rewritten("filter", {"input": {"kind": "nodes"},
                                  "predicate": {"field": "subtype", "value": "x"}})
    assert plain.adjusted[0]["served"] == "eq"


def test_a_long_form_clause_in_where_has_its_field_checked():
    """Item 6: `where` is the new spelling, in either form."""
    clause = {"op": "eq", "field": "status", "value": "Open"}
    refusal = _call_refused("filter", {"where": clause})
    assert "where: `status` is not a field filter compares" in refusal.message
    assert refusal.retry_arguments == {
        "where": {"op": "eq", "field": "attrs.status", "value": "Open"},
    }
    assert refusal.retry_mode == "guess"
    for tool, arguments in (
        ("map", {"where": {"op": "and", "clauses": [{"type": "t"}, clause]}}),
        ("aggregate", {"where": {"op": "not", "clause": clause}, "by": ["subtype"],
                       "metrics": ["count"]}),
        ("expand", {"input": "n:1", "where": {"op": "in", "field": "status", "values": [1]}}),
        ("filter", {"where": {"op": "exists", "field": "title"}}),
    ):
        assert "is not a field" in _call_refused(tool, arguments).message, tool
    # Since step 2c `predicate` is read the same way on filter (it was passed
    # on and refused there); expand's `filter` stays as sent. The whole
    # attribute map is a field.
    in_predicate = _call_refused("filter", {"input": {"kind": "nodes"}, "predicate": clause})
    assert "predicate: `status` is not a field filter compares" in in_predicate.message
    today = {"input": "n:1", "filter": clause}
    assert _rewritten("expand", today).arguments == today
    contains = {"op": "contains", "field": "attrs", "value": {"status": "Open"}}
    assert _rewritten("filter", {"where": contains}).arguments["predicate"] == contains


def test_an_operator_written_as_a_key_is_refused_with_its_long_form():
    """Item 5: `{"attrs.n": {"gt": 3}}` is a test, not an object to match."""
    refusal = _refused({"attrs.n": {"gt": 3}})
    assert refusal.message == (
        "where: the value of `attrs.n` names `gt` as a key; a test names its "
        "operator in `op`"
    )
    assert refusal.fix_hint == 'write {"attrs.n": {"op": "gt", "value": 3}}'
    assert _retry("filter", {"where": {"attrs.n": {"gt": 3}}}) == {
        "where": {"attrs.n": {"op": "gt", "value": 3}},
    }
    assert _retry("filter", {"where": {"type": "t", "attrs.n": {"between": [1, 5]},
                                       "attrs.k": {"exists": True}}}) == {
        "where": {"type": "t", "attrs.n": {"op": "between", "values": [1, 5]},
                  "attrs.k": {"op": "exists"}},
    }
    # Several operators, or `in` without a list: refused, and no call offered.
    for value in ({"gt": 3, "lt": 9}, {"in": "a"}, {"and": [{"gt": 3}]}):
        several = _call_refused("filter", {"where": {"attrs.n": value}})
        assert "a test names its operator in `op`" in several.message
        assert several.retry_arguments is None
    # An object with several keys, or none, is still matched exactly (a
    # one-key object with a bare-word key is refused: D1, loose-match-3666).
    for value in ({"gt": 3, "unit": "s"}, {}):
        assert _lowered({"attrs.n": value}) == {"op": "eq", "field": "attrs.n", "value": value}


def test_an_unknown_operator_word_as_a_key_is_refused_not_matched_exactly():
    """#3666: `{"attrs.x": {"ilike": "..."}}` was served as an exact match on
    the object `{"ilike": "..."}`: 0 rows, and nothing said why."""
    refusal = _refused({"attrs.x": {"ilike": "%gpu%"}})
    assert "names `ilike`, which is not an operator" in refusal.message
    assert "eq, in, exists, contains, suffix, prefix, lt, gt, between" in refusal.message
    assert 'to match the object itself write {"op": "eq", "field": "attrs.x"' in (
        refusal.fix_hint
    )
    # Through the call, `ilike` has one reading (like): corrected.
    called = _call_refused("filter", {"where": {"attrs.x": {"ilike": "%gpu%"}}})
    assert called.retry_mode == "corrected"
    assert "write `ilike` as `op`: `like`" in called.fix_hint
    # loose-match-3666: `ilike` is like, `like` an operator; written as a
    # key each is refused with its `op` form, which is served as written.
    assert _retry("filter", {"where": {"attrs.x": {"like": "a"}}}) == {
        "where": {"attrs.x": {"op": "like", "value": "a"}},
    }
    assert _retry("filter", {"where": {"attrs.x": {"ilike": "%gpu%"}}}) == {
        "where": {"attrs.x": {"op": "like", "value": "gpu"}},
    }
    # A near spelling of one operator of four letters or more is offered as
    # a guess, and the words served before the call keep the object match.
    assert _retry("filter", {"where": {"attrs.n": {"betwen": [1, 5]}}}) == {
        "where": {"attrs.n": {"op": "between", "values": [1, 5]}},
    }
    near = _call_refused("filter", {"where": {"attrs.n": {"betwen": [1, 5]}}})
    assert near.retry_mode == "guess"
    assert (
        'to match the object itself write {"op": "eq", "field": "attrs.n", '
        '"value": {"betwen": [1, 5]}}'
    ) in near.fix_hint
    # A `$` form or a synonym has one meaning: corrected.
    for word in ("$prefix", "startswith"):
        sent = {"where": {"attrs.p": {word: "src/"}}}
        assert _retry("filter", sent) == {"where": {"attrs.p": {"op": "prefix", "value": "src/"}}}
        assert _call_refused("filter", sent).retry_mode == "corrected"
    equals = _call_refused("filter", {"where": {"attrs.n": {"equals": 3}}})
    assert equals.retry_mode == "corrected"
    assert equals.retry_arguments == {"where": {"attrs.n": {"op": "eq", "value": 3}}}
    # An operand that does not fit the operator gets no call.
    assert _call_refused("filter", {"where": {"attrs.n": {"betwen": 3}}}).retry_arguments is None


@pytest.mark.parametrize("value", [
    {"stauts": "open"}, {"name": "a"}, {"status": "open"}, {"region": "eu"},
    {"owner_id": 7}, {"x": {"y": 1}},
    # Short words one letter from eq, in, lt or gt are real keys: those
    # match only exactly, never as a near spelling (#3666 review).
    {"seq": 3}, {"min": 1}, {"get": 5}, {"int": 2}, {"alt": "x"}, {"req": 1},
    # Too far from any operator at the 0.85 cutoff.
    {"exit": 0}, {"contents": "x"}, {"between_ts": [1, 2]},
])
def test_a_one_key_object_with_a_bare_word_key_is_refused_with_the_exact_long_form(value):
    """D1 (loose-match-3666): `{"attrs.n": {"stauts": "open"}}` was served as
    an exact match on that object, so a typo read as an empty answer. Such
    an object is refused; the call offered is the long form, which matches
    the object exactly and is served as written."""
    sent = {"where": {"attrs.n": value}}
    refusal = _call_refused("filter", sent)
    assert "is not an operator" in refusal.message
    assert refusal.retry_mode == "corrected"
    assert _retry("filter", sent) == {
        "where": {"op": "eq", "field": "attrs.n", "value": value},
    }
    # Beside another key, the long form joins it.
    assert _retry("filter", {"where": {"type": "t", "attrs.n": value}}) == {
        "where": {"op": "and", "clauses": [
            {"type": "t"}, {"op": "eq", "field": "attrs.n", "value": value},
        ]},
    }


def test_a_real_object_value_still_compares_exactly():
    """An object with several keys, none, or a key that is not a bare word
    is a value, matched exactly as before."""
    for value in ({"ilike": "a", "unit": "s"}, {"gt": 3, "unit": "s"}, {"Meta Data": 1}, {}):
        assert _lowered({"attrs.n": value}) == {"op": "eq", "field": "attrs.n", "value": value}
    # The long form matches even an object whose one key is an operator word.
    long_form = {"op": "eq", "field": "attrs.x", "value": {"ilike": "a"}}
    assert _lowered(long_form) == long_form


def test_an_operand_is_never_dropped_or_reread_by_a_correction():
    """Item B: `{"exists": false}` means "does not exist", which is the
    negation of a test; every other operator keeps its operand as sent or
    gets no correction."""
    refusal = _refused({"attrs.k": {"exists": False}})
    assert refusal.fix_hint == (
        'write {"op": "not", "clause": {"attrs.k": {"op": "exists"}}}'
    )
    absent = {"op": "not", "clause": {"attrs.k": {"op": "exists"}}}
    assert _retry("filter", {"where": {"attrs.k": {"exists": False}}}) == {"where": absent}
    assert rewrite_call("filter", {"where": absent}).arguments["predicate"] == {
        "op": "not", "clause": {"op": "exists", "field": "attrs.k"},
    }
    assert _retry("filter", {"where": {"type": "t", "attrs.k": {"exists": False}}}) == {
        "where": {"op": "and", "clauses": [{"type": "t"}, absent]},
    }
    assert _retry("filter", {"where": {"attrs.k": {"exists": True}}}) == {
        "where": {"attrs.k": {"op": "exists"}},
    }
    for sent, kept in (
        ({"eq": None}, {"op": "eq", "value": None}),
        ({"lt": 0}, {"op": "lt", "value": 0}),
        ({"in": ["a"]}, {"op": "in", "values": ["a"]}),
        ({"between": [1, 5]}, {"op": "between", "values": [1, 5]}),
        ({"prefix": "ab"}, {"op": "prefix", "value": "ab"}),
        ({"suffix": ".py"}, {"op": "suffix", "value": ".py"}),
    ):
        assert _retry("filter", {"where": {"attrs.k": sent}}) == {"where": {"attrs.k": kept}}
    # An operand that does not fit its operator: words, never a call.
    for sent in ({"exists": "yes"}, {"exists": 0}, {"exists": None}, {"in": []},
                 {"in": "a"}, {"between": [1]}, {"between": [1, 2, 3]}, {"between": 5},
                 {"prefix": ""}, {"suffix": 3}, {"contains": "x"}, {"contains": {"a": 1}}):
        refusal = _call_refused("filter", {"where": {"attrs.k": sent}})
        assert "a test names its operator in `op`" in refusal.message, sent
        assert refusal.retry_arguments is None, sent
        assert "<" not in refusal.fix_hint


@pytest.mark.parametrize("where", [
    {"not": {}},
    {"and": [1]},
    {"or": [{}]},
    {"attrs.n": {"between": [1]}},
    {"subtype": {"contains": "x"}},
])
def test_no_retry_is_offered_that_the_tool_would_refuse(where):
    """Item A, in the grammar: each of these once carried a `corrected`
    call that the predicate model or the tool then refused."""
    refusal = _call_refused("filter", {"where": where})
    assert refusal.retry_arguments is None and not refusal.repairable
    assert refusal.fix_hint and "<" not in refusal.fix_hint


def test_every_cause_of_a_refusal_is_said():
    """Item C: two mistakes, one of them a guess. Both are named, and the
    call is offered as a guess."""
    refusal = _call_refused("filter", {"where": {"not": {"type": "a"}, "status": "Open"}})
    assert refusal.fix_hint == (
        "write `not` with `op`; if `status` is a stored attribute, write `attrs.status`"
    )
    assert refusal.retry_mode == "guess"
    assert refusal.retry_arguments == {"where": {"op": "and", "clauses": [
        {"attrs.status": "Open"}, {"op": "not", "clause": {"type": "a"}},
    ]}}
    # Every part determined by the input: corrected, each change still said.
    determined = _call_refused("filter", {"where": {
        "not": {"Type": "a"}, "attrs.n": {"gt": 3},
    }})
    assert determined.retry_mode == "corrected"
    assert determined.fix_hint == (
        "write `not` with `op`; this set's field is `type`; on `attrs.n` write "
        "the operator in `op`"
    )
    assert determined.retry_arguments == {"where": {"op": "and", "clauses": [
        {"attrs.n": {"op": "gt", "value": 3}}, {"op": "not", "clause": {"type": "a"}},
    ]}}
    # Two guesses are two sentences.
    two = _call_refused("filter", {"where": {"status": "Open", "owner": "me"}})
    assert two.fix_hint == (
        "if `status` is a stored attribute, write `attrs.status`; "
        "if `owner` is a stored attribute, write `attrs.owner`"
    )
    # When the repaired call is not served, the words still list each cause.
    unserved = _call_refused("filter", {"where": {"not": {"type": "a"}, "status": "Open",
                                                  "attrs": {"a": 1}}})
    assert unserved.retry_arguments is None
    assert unserved.fix_hint.startswith("write `not` with `op`; if `status`")


def test_an_empty_list_is_refused_not_served_as_nothing():
    """Item F: `in` with no values has no SQL form on one path."""
    for sent in ({"type": []}, {"attrs.k": []}, {"type": "t", "attrs.k": []},
                 {"op": "not", "clause": {"attrs.k": []}}):
        refusal = _call_refused("filter", {"where": sent})
        assert "a list means any of; an empty list matches nothing" in refusal.message
        assert refusal.retry_arguments is None and not refusal.repairable
    assert "an empty list matches nothing" in _call_refused(
        "search", {"query": "q", "where": {"type": []}}).message
    # The long form in an existing argument is passed on, as it always was.
    today = {"input": {"kind": "nodes"},
             "predicate": {"op": "in", "field": "attrs.k", "values": []}}
    assert _rewritten("filter", today).arguments == today


def test_the_depth_limit_is_the_new_spellings_only():
    """Item D: a long-form tree in an existing argument had no limit."""
    deep: Any = {"op": "exists", "field": "attrs.x"}
    for _ in range(read_grammar.MAX_CONDITION_DEPTH + 20):
        deep = {"op": "not", "clause": deep}
    for tool, arguments in (
        ("filter", {"input": {"kind": "nodes"}, "predicate": deep}),
        ("map", {"input": {"kind": "nodes", "filter": deep}}),
        ("expand", {"input": "n:1", "filter": deep}),
        ("search", {"query": "q", "where": deep}),
    ):
        call = _rewritten(tool, arguments)
        assert call.arguments == arguments and call.adjusted == ()
    # The same tree in a top-level `where`, and a short form at that depth
    # in any argument, are the new spelling: refused by name.
    assert "nests at most" in _call_refused("filter", {"where": deep}).message
    short: Any = {"attrs.x": 1}
    for _ in range(read_grammar.MAX_CONDITION_DEPTH + 1):
        short = {"op": "not", "clause": short}
    assert "nests at most" in _call_refused(
        "filter", {"input": {"kind": "nodes"}, "predicate": short}).message
    # Far deeper than any walk follows: handed on untouched, never refused here.
    abyss: Any = {"op": "exists", "field": "attrs.x"}
    for _ in range(sys.getrecursionlimit() * 2):
        abyss = {"op": "not", "clause": abyss}
    sent = {"input": {"kind": "nodes"}, "predicate": abyss}
    handed_on = rewrite_call("filter", sent)
    assert handed_on.arguments is sent and handed_on.adjusted == ()


def test_an_id_on_an_edge_set_is_refused_in_words():
    """Item E: the edge's id or an end's; a retry would serve empty."""
    for arguments in (
        {"input": {"kind": "edges"}, "where": {"id": "ticket:1"}},
        {"input": {"kind": "edges"}, "predicate": {"id": ["ticket:1"]}},
        {"input": {"kind": "edges"}, "where": {"op": "eq", "field": "id", "value": "ticket:1"}},
    ):
        refusal = _call_refused("filter", arguments)
        assert "`id` is not a field filter compares" in refusal.message
        assert refusal.fix_hint == (
            "`id` on an edge set can mean the edge or the record at either "
            "end: name `edge_id`, `src` or `dst`"
        )
        assert refusal.retry_arguments is None and not refusal.repairable
    # The names it gives are the edge set's own fields.
    assert {"edge_id", "src", "dst"} <= EDGE_FIELDS.names
    assert _retry("filter", {"input": {"kind": "edges"}, "where": {"type": "t"}}) == {
        "input": {"kind": "edges"}, "where": {"edge_type": "t"},
    }


def test_an_empty_where_is_no_condition():
    """Item G: beside an older argument it adds nothing and conflicts with
    nothing; alone it is the empty condition."""
    scope = {"kind": "nodes", "subtype": "t"}
    assert _rewritten("filter", {"input": scope, "predicate": EQ_STATUS, "where": {}}).arguments == {
        "input": scope, "predicate": EQ_STATUS,
    }
    assert _rewritten("filter", {"predicate": {"type": "t"}, "where": {}}).arguments == {
        "input": {"kind": "nodes"},
        "predicate": {"op": "eq", "field": "subtype", "value": "t"},
    }
    filtered = {"kind": "nodes", "filter": EQ_STATUS}
    assert _rewritten("map", {"input": filtered, "where": {}}).arguments == {"input": filtered}
    assert _rewritten("aggregate", {"input": {"subtype": "t", "filter": EQ_STATUS}, "where": {},
                                    "by": ["subtype"], "metrics": ["count"]}).arguments == {
        "input": {"subtype": "t", "filter": EQ_STATUS}, "by": ["subtype"], "metrics": ["count"],
    }
    assert _rewritten("expand", {"input": "n:1", "filter": EQ_STATUS, "where": {}}).arguments == {
        "input": "n:1", "filter": EQ_STATUS,
    }
    for call in (
        _rewritten("filter", {"input": scope, "predicate": EQ_STATUS, "where": {}}),
        _rewritten("map", {"input": filtered, "where": {}}),
    ):
        assert call.adjusted == ()
    # Alone, as before.
    assert _rewritten("filter", {"input": scope, "where": {}}).arguments == {
        "input": scope, "predicate": {},
    }
    assert _rewritten("expand", {"input": "n:1", "where": {}}).arguments == {
        "input": "n:1", "filter": {},
    }
    assert _rewritten("map", {"input": scope, "where": {}}).arguments == {"input": scope}
    # A condition on both sides that differs is still a conflict.
    _call_refused("filter", {"input": scope, "predicate": EQ_STATUS, "where": {"attrs.a": 1}})


def test_a_test_object_that_joins_conditions_is_refused():
    """Item 7: search would drop the `field` beside `and` without a word."""
    sent = {"query": "q", "where": {"type": {"op": "and", "clauses": [
        {"op": "eq", "value": "a"}, {"op": "eq", "value": "b"},
    ]}}}
    refusal = _call_refused("search", sent)
    assert refusal.message == (
        "where: the test on `type` uses `and`, which joins conditions; a test "
        "on a field is one comparison"
    )
    assert refusal.retry_arguments is None
    for op in ("or", "not"):
        assert "joins conditions" in _refused({"attrs.x": {"op": op, "clauses": []}}).message


def test_a_field_name_that_could_forge_a_line_is_not_echoed():
    _, adjusted = lower_condition({"field": "attrs.x\n! forged", "value": 1})
    assert adjusted[0]["sent"] == "where clause on a field with no op"


def test_field_and_values_with_no_op_is_refused_with_both_readings_of_the_callers_clause():
    refusal = _refused({"field": "date.created", "values": ["2025-01-01", "2025-12-31"]})
    assert "`in` or `between`" in refusal.message
    assert refusal.fix_hint == (
        'for any of the values write {"op": "in", "field": "date.created", '
        '"values": ["2025-01-01", "2025-12-31"]}; for a range write '
        '{"op": "between", "field": "date.created", "values": ["2025-01-01", "2025-12-31"]}'
    )
    assert refusal.retry_arguments is None and not refusal.repairable
    # Too long to spell: the same two changes, in words, with no placeholder.
    long = _refused({"field": "attrs.k", "values": ["v" * 300, "w"]})
    assert long.fix_hint.startswith('add "op": "in" to that clause')
    assert "<" not in long.fix_hint


@pytest.mark.parametrize("clause", [
    {"value": 1},
    {"field": "attrs.x"},
    {"clauses": [{"op": "exists", "field": "attrs.x"}]},
    {"clause": {"op": "exists", "field": "attrs.x"}},
    {"field": "attrs.x", "value": 1, "values": [1]},
    {"field": "attrs.x", "value": 1, "attrs.y": 2},
    {"field": "", "value": 1},
    {"field": 3, "value": 1},
])
def test_any_other_clause_without_op_is_left_for_todays_refusal(clause):
    lowered, adjusted = lower_condition(clause)
    assert lowered == clause and adjusted == []


@pytest.mark.parametrize("op_key", ["op", "field", "value", "values", "clause", "clauses"])
def test_a_reserved_key_is_never_a_field_name(op_key):
    assert op_key in read_grammar.RESERVED_KEYS
    # Beside a field name it is not a short form: the object is left as sent.
    sent = {op_key: "x", "attrs.y": 1} if op_key != "op" else {"op": "eq", "attrs.y": 1}
    lowered, adjusted = lower_condition(sent)
    assert lowered == sent and adjusted == []


def test_a_test_object_that_names_its_own_field_is_refused():
    other = _refused({"attrs.x": {"op": "eq", "field": "attrs.y", "value": 1}})
    assert "names a `field` of its own" in other.message
    assert "cannot be told" in other.fix_hint and not other.repairable
    same = _refused({"attrs.x": {"op": "eq", "field": "attrs.x", "value": 1}})
    assert same.fix_hint == "drop `field` from the test" and same.repairable


def test_a_test_object_without_op_is_refused_with_both_readings_of_the_callers_value():
    refusal = _refused({"attrs.x": {"value": 3}})
    assert refusal.fix_hint == (
        'for a test write {"attrs.x": {"op": "eq", "value": 3}}; to match the '
        'object itself write {"op": "eq", "field": "attrs.x", "value": {"value": 3}}'
    )
    for value in ({"values": [1, 2]}, {"clause": {}}):
        assert "has no `op`" in _refused({"attrs.x": value}).message


def test_a_key_that_is_not_a_string_is_refused():
    assert "a field name is a string" in _refused({1: "x"}).message
    assert "a field name is a string" in _refused({"op": "not", "clause": {2: "y"}}).message


def test_a_condition_nested_too_deep_is_refused_by_name():
    condition: Any = {"attrs.x": 1}
    for _ in range(read_grammar.MAX_CONDITION_DEPTH):
        condition = {"op": "not", "clause": condition}
    assert lower_condition(condition)[0]["op"] == "not"
    assert "nests at most" in _refused({"op": "not", "clause": condition}).message


# --------------------------------------------------------------------------- #
# the per-tool rewrite                                                         #
# --------------------------------------------------------------------------- #


def _rewritten(tool: str, arguments: dict[str, Any]) -> read_grammar.RewrittenCall:
    before = copy.deepcopy(arguments)
    call = rewrite_call(tool, arguments)
    assert arguments == before, "the caller's arguments are never changed in place"
    assert rewrite_call(tool, arguments) == call, "the rewrite is a function of the arguments"
    return call


def _call_refused(tool: str, arguments: dict[str, Any]) -> GrammarRefusal:
    before = copy.deepcopy(arguments)
    with pytest.raises(GrammarRefusal) as refused:
        rewrite_call(tool, arguments)
    assert arguments == before
    return refused.value


#: Long-form conditions that name `type` and `id`, nested under `and` and
#: `not`: passed on as sent to search, `expand` and a saved result. On
#: `filter`, `map` and `aggregate` over a set they read the record's
#: columns, as in `where` (step 2c; see the test after TODAY).
TYPE_UNDER_NOT = {"op": "and", "clauses": [
    EQ_STATUS,
    {"op": "not", "clause": {"op": "eq", "field": "type", "value": "x"}},
]}
ID_UNDER_AND = {"op": "and", "clauses": [
    {"op": "in", "field": "id", "values": ["a", "b"]},
    {"op": "not", "clause": {"op": "and", "clauses": [
        {"op": "exists", "field": "id"}, {"op": "prefix", "field": "type", "value": "j"},
    ]}},
]}

TODAY = [
    ("search", {"query": "q", "where": {"op": "eq", "field": "type", "value": "t"}}),
    ("search", {"query": "q", "where": ID_UNDER_AND}),
    ("search", {"query": "q", "filter": {"subtype": "t", "attr_filters": {"a": 1}}}),
    ("filter", {"input": {"kind": "nodes", "subtype": "t"}, "predicate": {}}),
    ("filter", {"input": {"kind": "nodes", "filter": EQ_STATUS}, "predicate": EQ_STATUS,
                "select": ["node_id"], "max_bytes": 8192}),
    ("filter", {"input": {"kind": "edges", "edge_type": "e"},
                "predicate": {"op": "eq", "field": "src", "value": "n:1"}}),
    ("filter", {"input": {"kind": "edges", "edge_type": "e"},
                "predicate": {"op": "prefix", "field": "src", "value": "commit:"}}),
    ("filter", {"input": {"ref": "r1.abc"}, "predicate": EQ_STATUS, "retain": True}),
    ("filter", {"input": {"ref": "r1.abc"}, "predicate": TYPE_UNDER_NOT}),
    ("filter", {"input": {"ref": "l1.abc"}, "predicate": ID_UNDER_AND}),
    ("map", {"input": {"kind": "nodes", "subtype": "t"}, "projection": {"id": "node_id"}}),
    ("aggregate", {"input": {"subtype": "t", "filter": EQ_STATUS}, "by": ["attrs.a"],
                   "metrics": ["count"]}),
    ("aggregate", {"input": "n:1", "by": ["edge_type", "dst_subtype"], "metrics": ["count"]}),
    ("expand", {"input": {"node_id": "n:1"}, "edge_type": "e", "filter": EQ_STATUS}),
    ("expand", {"input": {"node_id": "n:1"}, "filter": TYPE_UNDER_NOT}),
    ("expand", {"input": "n:1", "filter": ID_UNDER_AND, "order_by": "node_id"}),
    ("expand", {"input": "n:1", "path": ["e:out"], "target_subtype": "t"}),
    ("expand", {"input": "n:1", "edge_type": "e", "depth": 2, "target_subtype": "t"}),
    # Refused today, and still: this step does not serve it.
    ("expand", {"input": "n:1", "edge_type": "e", "target_subtype": "t"}),
    ("inspect", {"target": {"kind": "node", "node_id": "n:1"}, "select": ["attrs"],
                 "max_bytes": 8192}),
    ("query", {"sql": "select 1", "where": {"type": "t"}}),
    ("record", {"where": {"type": "t"}}),
]


@pytest.mark.parametrize("tool,arguments", TODAY)
def test_a_call_that_is_valid_today_is_not_changed(tool, arguments):
    call = _rewritten(tool, arguments)
    assert call.arguments == arguments
    assert call.adjusted == () and call.spellings == ()


# --------------------------------------------------------------------------- #
# step 2c: one condition vocabulary on filter, map and aggregate               #
# --------------------------------------------------------------------------- #


def _type_as_subtype(condition: Any) -> Any:
    """`condition` with each long-form `type`/`id` read as the record's column."""
    if isinstance(condition, dict):
        out = {key: _type_as_subtype(value) for key, value in condition.items()}
        if out.get("field") in ("type", "id"):
            out["field"] = {"type": "subtype", "id": "node_id"}[out["field"]]
        return out
    if isinstance(condition, list):
        return [_type_as_subtype(item) for item in condition]
    return condition


@pytest.mark.parametrize("tool,arguments,argument", [
    ("filter", {"input": {"kind": "nodes"}, "predicate": TYPE_UNDER_NOT}, "predicate"),
    ("filter", {"input": {"kind": "nodes", "filter": ID_UNDER_AND},
                "predicate": {"op": "eq", "field": "type", "value": "x"}}, "both"),
    ("map", {"input": {"kind": "nodes", "filter": TYPE_UNDER_NOT},
             "projection": ["node_id"]}, "input.filter"),
    ("aggregate", {"input": {"kind": "nodes", "filter": ID_UNDER_AND}, "by": ["subtype"],
                   "metrics": ["count"]}, "input.filter"),
])
def test_a_long_form_clause_on_a_set_reads_the_second_names(tool, arguments, argument):
    """Step 2c: `type` and `id` in a long-form clause of `predicate` or
    `input.filter` mean what they mean in `where`, served with a note (they
    are not this argument's spelling). Each such call was refused before:
    neither read path knew either name."""
    call = _rewritten(tool, arguments)
    expected = copy.deepcopy(arguments)
    if argument in ("predicate", "both"):
        expected["predicate"] = _type_as_subtype(arguments["predicate"])
    if argument in ("input.filter", "both"):
        expected["input"]["filter"] = _type_as_subtype(arguments["input"]["filter"])
    assert call.arguments == expected
    assert call.adjusted and {record["rule"] for record in call.adjusted} == {"second_name"}
    assert call.spellings == ()


def test_a_second_name_in_a_long_form_clause_is_noted():
    call = _rewritten("filter", {"input": {"kind": "nodes"}, "predicate": {
        "op": "not", "clause": {"op": "eq", "field": "type", "value": "x"}}})
    assert call.adjusted == ({
        "rule": "second_name",
        "sent": "predicate.clause clause on `type`",
        "served": "on `subtype`",
    },)
    # In a top-level `where` the second name is a spelling: no note.
    assert _rewritten("filter", {"where": {"op": "eq", "field": "id", "value": "a"}}).adjusted == ()


def test_a_long_form_clause_on_a_saved_result_or_a_search_is_left_as_sent():
    for tool, arguments in (
        ("filter", {"input": {"ref": "r1.abc"}, "predicate": TYPE_UNDER_NOT}),
        ("filter", {"input": {"ref": "l1.abc"}, "predicate": ID_UNDER_AND}),
        ("expand", {"input": {"node_id": "n:1"}, "filter": TYPE_UNDER_NOT}),
        ("search", {"query": "q", "where": ID_UNDER_AND}),
        ("filter", {"input": {"ref": "r1.abc"},
                    "predicate": {"op": "eq", "field": "source", "value": "x"}}),
    ):
        call = _rewritten(tool, arguments)
        assert call.arguments == arguments and call.adjusted == ()


#: The refused condition forms of two lab runs of the measured question set
#: (one on dev with step 3a, one on the base before it), with neutral values:
#: `(tool, arguments, outcome)`. `served` is passed to the tool unchanged
#: (the DB tests prove the tool serves it), `noted` is served with the
#: named note, and `named` is refused by the grammar naming the field.
MEASURED = [
    # prefix on a link end (base: 4 refusals, the older translator)
    ("filter", {"input": {"kind": "edges", "edge_type": "references"},
                "predicate": {"op": "prefix", "field": "src", "value": "twiki:Web:Page"},
                "limit": 1000, "select": ["edge_id", "src", "dst", "edge_type"]},
     ("served", None)),
    ("filter", {"input": {"kind": "edges", "edge_type": "references"},
                "predicate": {"op": "and", "clauses": [
                    {"op": "prefix", "field": "src", "value": "commit:repo-a:"},
                    {"op": "prefix", "field": "dst", "value": "jira:"}]},
                "limit": 50, "select": ["edge_id", "src", "dst", "edge_type"],
                "max_bytes": 8000},
     ("served", None)),
    ("filter", {"input": {"kind": "edges", "edge_type": "references",
                          "filter": {"op": "prefix", "field": "src", "value": "commit:repo-a:"}},
                "predicate": {"op": "exists", "field": "dst"}, "limit": 50,
                "select": ["edge_id", "src", "dst", "edge_type"], "max_bytes": 6000},
     ("served", None)),
    # a field of search's `where` only, or of no set (dev3a: 3 refusals)
    ("filter", {"input": {"kind": "nodes", "subtype": "document_chunk"},
                "predicate": {"op": "eq", "field": "source", "value": "site.status"},
                "limit": 1000, "select": ["node_id", "attrs"], "max_bytes": 20000},
     ("named", "`source` is a field of search's `where` only")),
    ("filter", {"input": {"kind": "nodes", "subtype": "git_author"},
                "predicate": {"op": "eq", "field": "provenance.source", "value": "repo.history"},
                "limit": 100, "select": ["node_id", "attrs"], "max_bytes": 15000},
     ("named", "`provenance.source` is not a field filter compares")),
    # a field of the record at a link's end (dev3a and base: 2 refusals)
    ("aggregate", {"input": {"kind": "edges", "edge_type": "assigned_to",
                             "filter": {"op": "and", "clauses": [
                                 {"op": "prefix", "field": "dst", "value": "commit:repo-a:"},
                                 {"op": "gt", "field": "attrs.observed_at",
                                  "value": "2025-01-01T00:00:00Z"},
                                 {"op": "eq", "field": "dst.attrs.is_merge", "value": False}]}},
                   "by": ["src"], "metrics": ["count"], "limit": 25, "max_bytes": 10000},
     ("named", "`dst.attrs.is_merge` is not a field aggregate compares")),
    # `between` with its pair in `value` (dev3a and base: 2 refusals)
    ("aggregate", {"input": {"kind": "nodes", "subtype": "git_commit",
                             "filter": {"op": "and", "clauses": [
                                 {"op": "between", "field": "attrs.committed_at",
                                  "value": ["2025-01-01", "2026-12-31T23:59:59"]},
                                 {"op": "eq", "field": "attrs.is_merge", "value": False}]}},
                   "by": ["attrs.repo"], "metrics": ["count"], "limit": 100,
                   "max_bytes": 10000},
     ("noted", "values_in_value")),
    # `gte`, an operator outside the set (base: 1 refusal)
    ("filter", {"input": {"kind": "nodes", "subtype": "git_commit"},
                "predicate": {"op": "and", "clauses": [
                    {"field": "attrs.repo", "op": "eq", "value": "repo-a"},
                    {"field": "attrs.authored_at", "op": "gte", "value": "2025-01-01"}]},
                "limit": 2, "select": ["node_id", "subtype", "attrs"]},
     ("noted", "inclusive_comparison")),
    # no `predicate` beside a scope with its own condition (base: 1 refusal)
    ("filter", {"input": {"kind": "nodes", "subtype": "release",
                          "filter": {"op": "and", "clauses": [
                              {"op": "eq", "field": "attrs.major", "value": 15},
                              {"op": "eq", "field": "attrs.minor", "value": 0}]}},
                "limit": 100, "select": ["node_id"], "max_bytes": 10000},
     ("noted", "predicate_missing")),
]


@pytest.mark.parametrize("case", range(len(MEASURED)))
def test_each_measured_refused_condition_is_served_noted_or_named(case):
    tool, arguments, (outcome, detail) = MEASURED[case]
    if outcome == "named":
        refusal = _call_refused(tool, arguments)
        assert detail in refusal.message + " " + refusal.fix_hint, (
            refusal.message, refusal.fix_hint,
        )
        # No single call can mean what was asked: none is offered.
        assert refusal.retry_arguments is None
        return
    call = _rewritten(tool, arguments)
    if outcome == "served":
        assert call.arguments == arguments and call.adjusted == ()
        return
    assert [record["rule"] for record in call.adjusted] == [detail]


def test_a_near_miss_clause_is_served_as_the_one_clause_it_means():
    fields = dict(argument="predicate", check_fields=True)
    lowered, adjusted = lower_condition(
        {"op": "between", "field": "attrs.n", "value": [1, 2]}, **fields,
    )
    assert lowered == {"op": "between", "field": "attrs.n", "values": [1, 2]}
    assert adjusted == [{
        "rule": "values_in_value",
        "sent": "predicate between on `attrs.n` with its list in `value`",
        "served": "between with that list in `values`",
    }]
    lowered, adjusted = lower_condition(
        {"op": "in", "field": "src", "value": ["a", "b"]}, fields=EDGE_FIELDS, **fields,
    )
    assert lowered == {"op": "in", "field": "src", "values": ["a", "b"]}
    for op, strict in (("gte", "gt"), ("ge", "gt"), ("lte", "lt"), ("le", "lt")):
        lowered, adjusted = lower_condition(
            {"op": "not", "clause": {"op": op, "field": "attrs.d", "value": "2025-01-01"}},
            **fields,
        )
        assert lowered == {"op": "not", "clause": {"op": "or", "clauses": [
            {"op": strict, "field": "attrs.d", "value": "2025-01-01"},
            {"op": "eq", "field": "attrs.d", "value": "2025-01-01"},
        ]}}
        assert adjusted == [{
            "rule": "inclusive_comparison",
            "sent": f"predicate.clause {op} on `attrs.d`",
            "served": f"{strict} or eq",
        }]
    # No predicate, or `predicate: null` (as `where: null`): every record in
    # the input scope, with the note.
    for sent in ({"input": {"kind": "nodes", "subtype": "t"}},
                 {"input": {"kind": "nodes", "subtype": "t"}, "predicate": None}):
        call = _rewritten("filter", sent)
        assert call.arguments.get("predicate") is None
        assert [record["rule"] for record in call.adjusted] == ["predicate_missing"]
    # The field of a near miss is checked like any other.
    refusal = _refused({"op": "gte", "field": "status", "value": 1}, **fields)
    assert "`status` is not a field" in refusal.message


@pytest.mark.parametrize("predicate,outcome", [
    # A whole number written as a float: served, with a note.
    ({"op": "gt", "field": "edge_id", "value": 5.0}, ("noted", {"op": "gt", "field": "edge_id",
                                                               "value": 5})),
    ({"op": "in", "field": "edge_id", "values": [5.0, 7]},
     ("noted", {"op": "in", "field": "edge_id", "values": [5, 7]})),
    # One corrected call: refused, with that call.
    ({"op": "gt", "field": "edge_id", "value": 5.5},
     ("corrected", {"op": "gt", "field": "edge_id", "value": 5})),
    ({"op": "lt", "field": "edge_id", "value": 5.5},
     ("corrected", {"op": "lt", "field": "edge_id", "value": 6})),
    ({"op": "between", "field": "edge_id", "values": [1.5, 9.5]},
     ("corrected", {"op": "between", "field": "edge_id", "values": [2, 9]})),
    ({"op": "between", "field": "edge_id", "values": [1, "9"]},
     ("guess", {"op": "between", "field": "edge_id", "values": [1, 9]})),
    ({"op": "gt", "field": "src", "value": 5}, ("guess", {"op": "gt", "field": "src",
                                                          "value": "5"})),
    # No single reading: refused in words.
    ({"op": "lt", "field": "edge_id", "value": True}, ("unfixable", None)),
    ({"op": "gt", "field": "dst", "value": None}, ("unfixable", None)),
    ({"op": "eq", "field": "edge_id", "value": 5.5}, ("unfixable", None)),
])
def test_a_bound_is_read_by_the_type_of_its_column(predicate, outcome):
    """Re-review must-fix: a bound that does not fit its column never
    compiles to "matches nothing"."""
    sent = {"input": {"kind": "edges"}, "predicate": predicate}
    mode, meant = outcome
    if mode == "noted":
        call = _rewritten("filter", sent)
        assert call.arguments["predicate"] == meant
        assert [r["rule"] for r in call.adjusted] == ["integral_bound"]
        return
    refusal = _call_refused("filter", sent)
    if mode == "unfixable":
        assert refusal.retry_arguments is None, refusal.retry_arguments
        return
    assert refusal.retry_mode == mode
    assert refusal.retry_arguments == {"input": {"kind": "edges"}, "predicate": meant}


@pytest.mark.parametrize("clause", [
    # More than one reading, or none: left for the tool's own refusal.
    {"op": "between", "field": "attrs.n", "value": [1, 2, 3]},
    {"op": "between", "field": "attrs.n", "value": 3},
    {"op": "in", "field": "attrs.n", "value": []},
    {"op": "in", "field": "attrs.n", "value": [1], "values": [2]},
    {"op": "gte", "field": "attrs.n", "value": [1]},
    {"op": "gte", "field": "attrs.n", "value": None},
    {"op": "gte", "field": "attrs.n", "values": [1]},
    {"op": "gte", "field": "date.attrs.due", "value": "2025-01-01"},
])
def test_a_clause_with_no_single_reading_is_left_as_sent(clause):
    lowered, adjusted = lower_condition(clause, argument="predicate", check_fields=True)
    assert lowered == clause and adjusted == []
    # And without `check_fields` nothing is read at all.
    near = {"op": "gte", "field": "attrs.n", "value": 1}
    assert lower_condition(near, argument="predicate") == (near, [])


def test_an_operator_word_over_a_saved_result_reads_as_on_the_set():
    """Issue #3790: a saved result's condition keeps its fields as sent, but
    an operator word in `op` gets the explicit set call's answer (before,
    the schema refused `regex` there with a different error)."""
    bad = {"op": "regex", "field": "attrs.status", "value": "op.*"}
    on_ref = _call_refused("filter", {"input": {"ref": "r1"}, "predicate": bad})
    on_set = _call_refused("filter", {"input": {"kind": "nodes"}, "predicate": bad})
    assert str(on_ref) == str(on_set) and on_ref.fix_hint == on_set.fix_hint
    assert on_ref.retry_arguments is None
    call = _rewritten("filter", {"input": {"ref": "r1"}, "predicate": {
        "op": "gte", "field": "attrs.n", "value": 1}})
    assert call.arguments["predicate"]["op"] == "or"
    assert [r["rule"] for r in call.adjusted] == ["inclusive_comparison"]


def test_an_unknown_field_in_a_long_form_clause_on_a_set_is_refused_by_name():
    """Before step 2c the clause reached the tool, which refused it with
    `unsupported_operator_form` and words that named no field."""
    refusal = _call_refused("filter", {
        "input": {"kind": "nodes"},
        "predicate": {"op": "and", "clauses": [
            EQ_STATUS, {"op": "eq", "field": "repo", "value": "a"}]},
    })
    assert refusal.message.startswith("predicate.clauses[1]: `repo` is not a field filter")
    assert refusal.retry_mode == "guess"
    assert refusal.retry_arguments == {"input": {"kind": "nodes"}, "predicate": {
        "op": "and", "clauses": [EQ_STATUS, {"op": "eq", "field": "attrs.repo", "value": "a"}],
    }}
    # On a set of links `type` is the link's type: one correction, offered.
    refusal = _call_refused("filter", {
        "input": {"kind": "edges"}, "predicate": {"op": "eq", "field": "type", "value": "e"},
    })
    assert refusal.retry_mode == "corrected"
    assert refusal.retry_arguments == {
        "input": {"kind": "edges"}, "predicate": {"op": "eq", "field": "edge_type", "value": "e"},
    }
    # `map` and `aggregate` read `input.filter` the same way.
    for tool, extra in (("map", {}), ("aggregate", {"by": ["subtype"], "metrics": ["count"]})):
        refusal = _call_refused(tool, {
            "input": {"kind": "nodes", "filter": {"op": "exists", "field": "title"}}, **extra,
        })
        assert "input.filter: `title` is not a field" in refusal.message
        assert "search's `where` only" in refusal.fix_hint


def test_arguments_that_are_not_an_object_are_handed_back():
    assert rewrite_call("filter", None).arguments is None
    assert rewrite_call("filter", ["x"]).arguments == ["x"]


def test_search_where_takes_the_short_form_and_keeps_its_own_field_names():
    call = _rewritten("search", {"query": "q", "where": {"type": "t", "id": ["a"]}})
    assert call.arguments == {"query": "q", "where": {"op": "and", "clauses": [
        {"op": "eq", "field": "type", "value": "t"},
        {"op": "in", "field": "id", "values": ["a"]},
    ]}}
    assert call.adjusted == () and call.spellings == ()


def test_filter_where_supplies_the_type_and_the_predicate():
    call = _rewritten("filter", {"where": {"type": "jira_issue", "attrs.status": "Open"}})
    assert call.arguments == {
        "input": {"kind": "nodes", "subtype": "jira_issue"}, "predicate": EQ_STATUS,
    }
    assert call.adjusted == () and call.spellings == (("where", "predicate"),)
    assert _rewritten("filter", {"where": {"type": "t"}, "limit": 5}).arguments == {
        "limit": 5, "input": {"kind": "nodes", "subtype": "t"}, "predicate": {},
    }
    assert _rewritten("filter", {"where": {}}).arguments == {
        "input": {"kind": "nodes"}, "predicate": {},
    }
    three = _rewritten("filter", {"where": {"attrs.a": 1, "subtype": "t", "attrs.b": 2}})
    assert three.arguments == {
        "input": {"kind": "nodes", "subtype": "t"},
        "predicate": {"op": "and", "clauses": [
            {"op": "eq", "field": "attrs.a", "value": 1},
            {"op": "eq", "field": "attrs.b", "value": 2},
        ]},
    }
    # In a `where` the long form has the second names too.
    long_form = _rewritten("filter", {"where": {"op": "and", "clauses": [
        {"op": "eq", "field": "type", "value": "t"},
        {"op": "not", "clause": {"op": "eq", "field": "id", "value": "n:1"}},
    ]}})
    assert long_form.arguments == {
        "input": {"kind": "nodes", "subtype": "t"},
        "predicate": {"op": "not", "clause": {"op": "eq", "field": "node_id", "value": "n:1"}},
    }


@pytest.mark.parametrize("where,predicate", [
    # Several types, a negated type, a type under `or`: not one type, so the
    # condition stays whole and the set is every node it selects.
    ({"type": ["a", "b"]}, {"op": "in", "field": "subtype", "values": ["a", "b"]}),
    ({"op": "not", "clause": {"type": "a"}},
     {"op": "not", "clause": {"op": "eq", "field": "subtype", "value": "a"}}),
    ({"op": "or", "clauses": [{"type": "a"}, {"attrs.x": 1}]},
     {"op": "or", "clauses": [{"op": "eq", "field": "subtype", "value": "a"},
                              {"op": "eq", "field": "attrs.x", "value": 1}]}),
    ({"type": {"op": "prefix", "value": "jira"}},
     {"op": "prefix", "field": "subtype", "value": "jira"}),
])
def test_only_one_plain_type_at_the_top_level_becomes_the_sets_type(where, predicate):
    assert _rewritten("filter", {"where": where}).arguments == {
        "input": {"kind": "nodes"}, "predicate": predicate,
    }


def test_a_type_condition_is_kept_when_the_input_already_names_a_type():
    call = _rewritten("filter", {
        "input": {"kind": "nodes", "subtype": "a"}, "where": {"type": "b", "attrs.x": 1},
    })
    assert call.arguments == {
        "input": {"kind": "nodes", "subtype": "a"},
        "predicate": {"op": "and", "clauses": [
            {"op": "eq", "field": "subtype", "value": "b"},
            {"op": "eq", "field": "attrs.x", "value": 1},
        ]},
    }


def test_filter_where_over_edges_a_reference_and_a_live_set():
    edges = _rewritten("filter", {
        "input": {"kind": "edges"}, "where": {"edge_type": "t", "src": "n"},
    })
    assert edges.arguments == {
        "input": {"kind": "edges"},
        "predicate": {"op": "and", "clauses": [
            {"op": "eq", "field": "edge_type", "value": "t"},
            {"op": "eq", "field": "src", "value": "n"},
        ]},
    }
    saved = _rewritten("filter", {"input": {"ref": "r1.abc"}, "where": {"type": "t"}})
    assert saved.arguments == {
        "input": {"ref": "r1.abc"},
        "predicate": {"op": "eq", "field": "subtype", "value": "t"},
    }
    live_id = live_result_sets.ID_PREFIX + "abc"
    assert live_result_sets.is_live_id(live_id) and live_id == "l1.abc"
    live = _rewritten("filter", {"input": {"ref": live_id}, "where": {"op": "and", "clauses": [
        {"attrs.kind": "t"}, {"op": "eq", "field": "attrs.type", "value": "x"},
    ]}})
    assert live.arguments["predicate"] == {"op": "and", "clauses": [
        {"op": "eq", "field": "attrs.kind", "value": "t"},
        {"op": "eq", "field": "attrs.type", "value": "x"},
    ]}
    # A live set's rows are an outside system's: it has no `type`, and the
    # name is not turned into a node's.
    refusal = _call_refused("filter", {"input": {"ref": live_id}, "where": {"type": "x"}})
    assert refusal.fix_hint == "if `type` is a stored attribute, write `attrs.type`"
    # `edge_type` on a saved result is not guessed into an attribute.
    edge = _call_refused("filter", {"input": {"ref": "r1.abc"}, "where": {"edge_type": "e"}})
    assert "a field of an edge set" in edge.fix_hint and edge.retry_arguments is None


def test_the_old_spellings_take_the_short_form_with_its_second_names():
    call = _rewritten("filter", {
        "input": {"kind": "nodes", "subtype": "t", "filter": {"id": ["a", "b"]}},
        "predicate": {"type": "t", "attrs.b": {"op": "gt", "value": 3}},
    })
    assert call.arguments == {
        "input": {"kind": "nodes", "subtype": "t",
                  "filter": {"op": "in", "field": "node_id", "values": ["a", "b"]}},
        "predicate": {"op": "and", "clauses": [
            {"op": "eq", "field": "subtype", "value": "t"},
            {"op": "gt", "field": "attrs.b", "value": 3},
        ]},
    }
    assert call.adjusted == () and call.spellings == ()


@pytest.mark.parametrize("tool,arguments,names", [
    ("filter", {"where": {"attrs.a": 1}, "predicate": {"op": "eq", "field": "attrs.a", "value": 2}},
     ("where", "predicate")),
    ("map", {"where": {"attrs.a": 1},
             "input": {"kind": "nodes", "filter": {"op": "eq", "field": "attrs.a", "value": 2}}},
     ("where", "input.filter")),
    ("aggregate", {"where": {"attrs.a": 1}, "by": ["subtype"], "metrics": ["count"],
                   "input": {"subtype": "t", "filter": {"attrs.a": 2}}},
     ("where", "input.filter")),
    ("expand", {"input": "n:1", "where": {"attrs.a": 1}, "filter": {"attrs.a": 2}},
     ("where", "filter")),
    ("map", {"input": {"kind": "nodes"}, "select": ["node_id"], "projection": ["subtype"]},
     ("select", "projection")),
])
def test_both_spellings_that_differ_are_refused_naming_both(tool, arguments, names):
    refusal = _call_refused(tool, arguments)
    for name in names:
        assert f"`{name}`" in refusal.message
    assert refusal.fields == names
    # Which of the two was meant is the caller's to say: no call is offered.
    assert refusal.retry_arguments is None
    assert "cannot be told from the call" in refusal.fix_hint


@pytest.mark.parametrize("tool,arguments,expected", [
    ("filter", {"input": {"kind": "nodes", "subtype": "t"}, "where": {"attrs.status": "Open"},
                "predicate": EQ_STATUS},
     {"input": {"kind": "nodes", "subtype": "t"}, "predicate": EQ_STATUS}),
    ("expand", {"input": "n:1", "where": {"attrs.status": "Open"}, "filter": EQ_STATUS},
     {"input": "n:1", "filter": EQ_STATUS}),
    ("map", {"input": {"kind": "nodes", "filter": EQ_STATUS}, "where": {"attrs.status": "Open"},
             "select": ["node_id"], "projection": ["node_id"]},
     {"input": {"kind": "nodes", "filter": EQ_STATUS}, "projection": ["node_id"]}),
    # A null or empty old spelling carries no condition.
    ("filter", {"where": {"attrs.status": "Open"}, "predicate": {}, "input": {"kind": "edges"}},
     {"input": {"kind": "edges"}, "predicate": EQ_STATUS}),
    ("expand", {"input": "n:1", "where": {"attrs.status": "Open"}, "filter": None},
     {"input": "n:1", "filter": EQ_STATUS}),
    ("filter", {"input": {"kind": "nodes"}, "predicate": EQ_STATUS, "where": None},
     {"input": {"kind": "nodes"}, "predicate": EQ_STATUS}),
])
def test_both_spellings_that_agree_are_one_call(tool, arguments, expected):
    assert _rewritten(tool, arguments).arguments == expected


def test_map_where_and_select():
    call = _rewritten("map", {
        "where": {"type": "git_tag"}, "select": ["node_id", "attrs.name"],
    })
    assert call.arguments == {
        "input": {"kind": "nodes", "subtype": "git_tag"},
        "projection": ["node_id", "attrs.name"],
    }
    assert call.adjusted == ()
    assert call.spellings == (("where", "input.filter"), ("select", "projection"))
    narrowed = _rewritten("map", {
        "input": {"kind": "nodes", "subtype": "t"}, "where": {"attrs.a": [1, 2]},
    })
    assert narrowed.arguments == {"input": {
        "kind": "nodes", "subtype": "t",
        "filter": {"op": "in", "field": "attrs.a", "values": [1, 2]},
    }}


def test_aggregate_where_with_and_without_an_input():
    call = _rewritten("aggregate", {
        "where": {"type": "jira_issue", "attrs.project": "X"},
        "by": ["attrs.status"], "metrics": ["count"],
    })
    assert call.arguments == {
        "by": ["attrs.status"], "metrics": ["count"],
        "input": {"kind": "nodes", "subtype": "jira_issue",
                  "filter": {"op": "eq", "field": "attrs.project", "value": "X"}},
    }
    legacy = _rewritten("aggregate", {
        "input": {"subtype": "t"}, "where": {"attrs.a": 1},
        "by": ["attrs.status"], "metrics": ["count"],
    })
    assert legacy.arguments["input"] == {
        "subtype": "t", "filter": {"op": "eq", "field": "attrs.a", "value": 1},
    }


@pytest.mark.parametrize("tool,arguments,needle,missing", [
    # Serving these would drop the condition: `map` and `aggregate` read a
    # saved result as its members, and a node anchor is one record.
    ("map", {"input": {"ref": "r1.abc"}, "where": {"attrs.a": 1}},
     "saved result", "does not exist until filter has run"),
    ("aggregate", {"input": {"ref": "r1.abc"}, "where": {"attrs.a": 1},
                   "by": ["subtype"], "metrics": ["count"]},
     "saved result", "does not exist until filter has run"),
    ("aggregate", {"input": "n:1", "where": {"attrs.a": 1},
                   "by": ["edge_type"], "metrics": ["count"]},
     "names one record", "cannot be told from the call"),
    ("aggregate", {"input": {"node_id": "n:1"}, "where": {"attrs.a": 1},
                   "by": ["edge_type"], "metrics": ["count"]},
     "names one record", "cannot be told from the call"),
])
def test_a_rewrite_that_would_drop_what_was_asked_is_refused(tool, arguments, needle, missing):
    refusal = _call_refused(tool, arguments)
    assert needle in refusal.message
    assert missing in refusal.fix_hint and refusal.retry_arguments is None


def test_expand_where_is_the_far_end_condition():
    call = _rewritten("expand", {
        "input": "n:1", "edge_type": "references", "where": {"type": "jira_issue"},
    })
    assert call.arguments == {
        "input": "n:1", "edge_type": "references",
        "filter": {"op": "eq", "field": "subtype", "value": "jira_issue"},
    }
    assert call.adjusted == () and call.spellings == (("where", "filter"),)


# --------------------------------------------------------------------------- #
# a refusal carries the corrected call when the input determines it            #
# --------------------------------------------------------------------------- #


def _retry(tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """The call a refusal carries; the rewrite must take it as written."""
    retry = _call_refused(tool, arguments).retry_arguments
    assert retry is not None
    again = rewrite_call(tool, retry)
    assert again.adjusted == (), "a retry is served with no note"
    return retry


@pytest.mark.parametrize("tool,arguments", [
    # Item 3: the condition can be repaired, and the call is still refused
    # for another reason, so no call is offered.
    ("map", {"input": {"ref": "r1.x"}, "where": {"status": "Open"}}),
    ("filter", {"input": {"kind": "nodes"},
                "predicate": {"op": "eq", "field": "attrs.a", "value": 1},
                "where": {"and": [{"attrs.a": 2}]}}),
    ("aggregate", {"input": {"node_id": "n:1"}, "where": {"status": "Open"},
                   "by": ["edge_type"], "metrics": ["count"]}),
    # The repaired call would still carry a note: not offered either.
    ("filter", {"input": {"kind": "nodes", "max_bytes": 8192}, "predicate": {"status": "Open"}}),
])
def test_a_retry_that_the_server_would_not_serve_as_written_is_not_offered(tool, arguments):
    refusal = _call_refused(tool, arguments)
    assert refusal.repairable and refusal.retry_arguments is None
    assert refusal.fix_hint and "<" not in refusal.fix_hint


def test_a_guess_is_marked_as_one_and_a_correction_is_not():
    guess = _call_refused("filter", {"where": {"status": "Open"}})
    assert guess.retry_mode == "guess" and guess.retry_arguments == {
        "where": {"attrs.status": "Open"},
    }
    for sent, corrected in (
        ({"Type": "doc"}, {"type": "doc"}),
        ({"or": [{"attrs.a": 1}]}, {"op": "or", "clauses": [{"attrs.a": 1}]}),
        ({"attrs.n": {"gt": 3}}, {"attrs.n": {"op": "gt", "value": 3}}),
    ):
        refusal = _call_refused("filter", {"where": sent})
        assert refusal.retry_mode == "corrected" and refusal.retry_arguments == {"where": corrected}
    # One guess among the fixes makes the whole call a guess.
    mixed = _call_refused("filter", {"where": {"Type": "doc", "status": "Open"}})
    assert mixed.retry_mode == "guess"
    assert mixed.retry_arguments == {"where": {"type": "doc", "attrs.status": "Open"}}


def test_an_unknown_key_is_refused_with_the_call_that_names_the_attribute():
    assert _retry("filter", {
        "where": {"type": "jira_issue", "attrs.priority": "High", "status": "Open"},
        "limit": 5,
    }) == {
        "where": {"type": "jira_issue", "attrs.priority": "High", "attrs.status": "Open"},
        "limit": 5,
    }
    assert _retry("filter", {"input": {"kind": "edges"}, "predicate": {"type": "t"}}) == {
        "input": {"kind": "edges"}, "predicate": {"edge_type": "t"},
    }
    assert _retry("map", {"input": {"kind": "nodes", "filter": {"status": ["a"]}}}) == {
        "input": {"kind": "nodes", "filter": {"attrs.status": ["a"]}},
    }
    assert _retry("search", {"query": "q", "where": {"type": "t", "status": "Open"}}) == {
        "query": "q", "where": {"type": "t", "attrs.status": "Open"},
    }
    assert _retry("expand", {"input": "n:1", "filter": {"status": "Open"}}) == {
        "input": "n:1", "filter": {"attrs.status": "Open"},
    }


def test_a_logical_word_as_a_key_is_refused_with_the_long_form_of_that_input():
    either = [{"attrs.a": 1}, {"status": "Open"}]
    assert _retry("filter", {"where": {"or": either}}) == {
        "where": {"op": "or", "clauses": [{"attrs.a": 1}, {"attrs.status": "Open"}]},
    }
    assert _retry("filter", {"where": {"type": "doc", "or": [{"attrs.a": 1}, {"attrs.b": 2}],
                                       "not": {"attrs.c": 3}}}) == {
        "where": {"op": "and", "clauses": [
            {"type": "doc"},
            {"op": "or", "clauses": [{"attrs.a": 1}, {"attrs.b": 2}]},
            {"op": "not", "clause": {"attrs.c": 3}},
        ]},
    }
    assert _retry("expand", {"input": "n:1", "where": {"op": "not", "clause": {
        "and": [{"attrs.a": 1}],
    }}}) == {
        "input": "n:1",
        "where": {"op": "not", "clause": {"op": "and", "clauses": [{"attrs.a": 1}]}},
    }


def test_other_single_fixes_carry_their_call():
    assert _retry("filter", {"where": {
        "attrs.x": {"op": "gt", "field": "attrs.x", "value": 3},
    }}) == {"where": {"attrs.x": {"op": "gt", "value": 3}}}
    assert _retry("map", {"input": {"kind": "nodes"}, "select": {"id": "node_id"}}) == {
        "input": {"kind": "nodes"}, "projection": {"id": "node_id"},
    }


@pytest.mark.parametrize("tool,arguments", [
    # A second mistake with no single fix remains after the first is fixed.
    ("filter", {"where": {"status": "Open", "attrs": {"a": 1}}}),
    ("filter", {"where": {"status": "Open", "attrs.k": {"values": [1]}}}),
    ("map", {"input": {"kind": "nodes"}, "select": "node_id"}),
    ("map", {"input": {"kind": "nodes"}, "select": {"id": "node_id"},
             "projection": ["node_id"]}),
])
def test_no_call_is_offered_when_the_input_does_not_determine_one(tool, arguments):
    assert _call_refused(tool, arguments).retry_arguments is None


def test_a_call_is_spelled_as_the_caller_would_send_it():
    assert render_call("filter", {"where": {"type": "t", "attrs.status": "Open"}, "limit": 5}) == (
        'filter(where={"type": "t", "attrs.status": "Open"}, limit=5)'
    )


# --------------------------------------------------------------------------- #
# served with a note                                                           #
# --------------------------------------------------------------------------- #


def test_a_clause_without_op_is_adjusted_in_every_spelling():
    bare = {"field": "attrs.status", "value": "Open"}
    cases = [
        ("search", {"query": "q", "where": bare}, "where"),
        ("filter", {"input": {"kind": "nodes"}, "predicate": bare}, "predicate"),
        ("filter", {"where": bare}, "where"),
        ("map", {"input": {"kind": "nodes", "filter": bare}}, "input.filter"),
        ("aggregate", {"input": {"subtype": "t", "filter": bare}, "by": ["subtype"],
                       "metrics": ["count"]}, "input.filter"),
        ("expand", {"input": "n:1", "filter": bare}, "filter"),
    ]
    for tool, arguments, argument in cases:
        call = _rewritten(tool, arguments)
        assert call.adjusted == ({
            "rule": "clause_without_op",
            "sent": f"{argument} clause on `attrs.status` with no op",
            "served": "eq",
        },), tool
    assert _rewritten("filter", {"where": bare}).arguments == {
        "input": {"kind": "nodes"}, "predicate": EQ_STATUS,
    }


def _lifted(name: str, outer: str) -> dict[str, str]:
    return {"rule": "argument_lifted", "sent": f"{name} inside {outer}",
            "served": f"top-level {name}"}


def test_select_and_max_bytes_inside_target_are_lifted_out():
    call = _rewritten("inspect", {"target": {
        "kind": "node", "node_id": "n:1", "select": ["attrs"], "max_bytes": 8192,
    }})
    assert call.arguments == {
        "target": {"kind": "node", "node_id": "n:1"}, "select": ["attrs"], "max_bytes": 8192,
    }
    assert call.adjusted == (_lifted("select", "target"), _lifted("max_bytes", "target"))
    # The same value at both levels is one value.
    same = _rewritten("inspect", {
        "target": {"kind": "text", "node_id": "n:1", "max_bytes": 8192}, "max_bytes": 8192,
    })
    assert same.arguments == {"target": {"kind": "text", "node_id": "n:1"}, "max_bytes": 8192}
    assert same.adjusted == (_lifted("max_bytes", "target"),)


def test_a_nested_argument_that_differs_from_the_top_level_is_left_refused():
    sent = {"target": {"kind": "node", "node_id": "n:1", "max_bytes": 6000}, "max_bytes": 8192}
    call = _rewritten("inspect", sent)
    assert call.arguments == sent and call.adjusted == ()
    scoped = {"input": {"kind": "nodes", "select": ["node_id"]}, "predicate": {},
              "select": ["subtype"]}
    assert _rewritten("filter", scoped).arguments == scoped


def test_select_and_max_bytes_inside_input_are_lifted_out():
    call = _rewritten("filter", {
        "input": {"kind": "nodes", "subtype": "t", "select": ["node_id"], "max_bytes": 8192},
        "predicate": {},
    })
    assert call.arguments == {
        "input": {"kind": "nodes", "subtype": "t"}, "predicate": {},
        "select": ["node_id"], "max_bytes": 8192,
    }
    assert call.adjusted == (_lifted("select", "input"), _lifted("max_bytes", "input"))
    mapped = _rewritten("map", {"input": {"kind": "nodes", "select": ["node_id"]}})
    assert mapped.arguments == {"input": {"kind": "nodes"}, "projection": ["node_id"]}
    assert mapped.adjusted == (_lifted("select", "input"),)
    counted = _rewritten("aggregate", {
        "input": {"subtype": "t", "max_bytes": 8192, "select": ["node_id"]},
        "by": ["subtype"], "metrics": ["count"],
    })
    # `aggregate` has no `select`: that key stays where it was, refused.
    assert counted.arguments == {
        "input": {"subtype": "t", "select": ["node_id"]},
        "by": ["subtype"], "metrics": ["count"], "max_bytes": 8192,
    }
    assert counted.adjusted == (_lifted("max_bytes", "input"),)


# --------------------------------------------------------------------------- #
# the note is bounded, and says what it left out                               #
# --------------------------------------------------------------------------- #


def _many(count: int) -> dict[str, Any]:
    return {"op": "or", "clauses": [
        {"field": f"attrs.key_{index}", "value": index} for index in range(count)
    ]}


def test_adjustments_past_the_cap_are_counted_not_dropped():
    call = _rewritten("filter", {"input": {"kind": "nodes"}, "predicate": _many(30)})
    assert len(call.arguments["predicate"]["clauses"]) == 30, "every clause is still served"
    listed, more = call.adjusted[:-1], call.adjusted[-1]
    assert 1 <= len(listed) <= MAX_ADJUSTED
    assert all(record["rule"] == "clause_without_op" for record in listed)
    assert more == {
        "rule": MORE_NOT_LISTED, "sent": f"{30 - len(listed)} more", "served": "not listed",
    }
    line = result_text.render(
        {"status": "ok", "items": [], "adjusted": list(call.adjusted)}, tool="filter",
    ).splitlines()[1]
    assert line.startswith("! adjusted: predicate.clauses[0] clause on `attrs.key_0`")
    assert line.endswith(f"; {30 - len(listed)} more not listed")
    # At the cap exactly, nothing is left out and nothing says so.
    exact = _rewritten("filter", {"input": {"kind": "nodes"}, "predicate": _many(2)})
    assert [record["rule"] for record in exact.adjusted] == ["clause_without_op"] * 2


def test_the_note_stays_small_however_many_and_however_deep():
    deep: Any = {"field": "attrs." + "k" * 90, "value": 1}
    for _ in range(read_grammar.MAX_CONDITION_DEPTH - 1):
        deep = {"op": "and", "clauses": [deep, {"field": "attrs." + "j" * 90, "value": 2}]}
    for predicate in (_many(200), deep):
        adjusted = list(rewrite_call(
            "filter", {"input": {"kind": "nodes"}, "predicate": predicate},
        ).adjusted)
        assert len(adjusted) <= MAX_ADJUSTED + 1
        assert adjusted[-1]["rule"] == MORE_NOT_LISTED
        listed = sum(len(read_grammar.json.dumps(r).encode()) for r in adjusted[:-1])
        assert listed <= MAX_ADJUSTED_BYTES
        # In either served form the note leaves most of the smallest budget.
        page = {"status": "ok", "items": [], "adjusted": adjusted}
        assert len(result_text.render(page, tool="filter").encode()) < 1536
        assert len(result_text.compact_json(page).encode()) < 1536


def test_cap_adjusted_keeps_a_short_list_whole():
    records = [_lifted("select", "input"), _lifted("max_bytes", "input")]
    assert cap_adjusted(records) == tuple(records)
    assert cap_adjusted([]) == ()


# --------------------------------------------------------------------------- #
# the text note                                                                #
# --------------------------------------------------------------------------- #


def test_the_text_reply_says_adjusted_on_one_line():
    page = {
        "status": "ok", "items": [{"node_id": "n:1", "subtype": "t"}], "total_count": 1,
        "adjusted": [
            {"rule": "clause_without_op",
             "sent": "predicate clause on `attrs.status` with no op", "served": "eq"},
            _lifted("max_bytes", "input"),
        ],
    }
    lines = result_text.render(page, tool="filter").splitlines()
    assert (
        "! adjusted: predicate clause on `attrs.status` with no op read as eq; "
        "max_bytes inside input read as top-level max_bytes"
    ) in lines
    assert not any("clause_without_op" in line for line in lines), lines
    plain = result_text.render({k: v for k, v in page.items() if k != "adjusted"}, tool="filter")
    assert "adjusted" not in plain


def test_a_refusal_of_a_rewritten_call_says_adjusted_too():
    refusal = {
        "status": "refused", "operator_name": "filter",
        "error": {"code": "unsupported_operator_form", "message": "m"},
        "adjusted": [_lifted("max_bytes", "input")],
    }
    lines = result_text.render(refusal, tool="filter").splitlines()
    assert lines[1] == "! adjusted: max_bytes inside input read as top-level max_bytes"
    assert not any(line.startswith("adjusted") for line in lines), lines


def test_an_adjusted_value_the_server_did_not_write_is_printed_as_data():
    page = {"status": "ok", "items": [], "adjusted": [{"sent": "a\n! forged", "served": "b"}]}
    text = result_text.render(page, tool="filter")
    assert "! adjusted:" not in text
    assert "! forged" not in text.splitlines()


# --------------------------------------------------------------------------- #
# the byte-bound wrapper: one note, inside the budget                          #
# --------------------------------------------------------------------------- #

pytest.importorskip("mcp")

from okg.substrate.mcp.response_bounds import (  # noqa: E402
    MIN_RESPONSE_BYTES,
    InvalidOperatorArguments,
    UnknownOperatorArgument,
    adjusted_reserve_bytes,
    adjusted_scope,
    bound_read_tool,
    call_adjusted,
    public_call_tool_result_bytes,
)
from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.substrate.mcp.server import build_server  # noqa: E402
from tests.substrate.mcp._result_payload import call_json, payload  # noqa: E402

NOTE = [_lifted("select", "input"), _lifted("max_bytes", "input")]


def _rows(limit: int = 50, offset: int = 0, max_bytes: Any = None,
          generation_id: Any = None) -> dict[str, Any]:
    """A pageable read: `limit` rows of about 200 bytes, and more after."""
    return {
        "status": "ok", "limit": limit, "offset": offset, "total_count": 500,
        "next_offset": offset + limit, "truncated": True,
        "items": [
            {"node_id": f"n:{index}", "subtype": "t", "attrs": {"text": "x" * 150}}
            for index in range(offset, offset + limit)
        ],
    }


def test_a_page_the_note_pushes_over_is_refused_saying_so_and_how_much():
    """Item 9: a read that is not paged (subtype paging at a budget just
    over the minimum) fits without the note and not with it. The refusal
    says the note is part of it, how big it is, and the size that fits."""
    def one_page(target: Any = None, max_bytes: Any = None) -> dict[str, Any]:
        return {"status": "ok", "summary": "s" * 4200}

    read = bound_read_tool("inspect", one_page)
    whole = read(max_bytes=8192)
    budget = public_call_tool_result_bytes(whole, tool="inspect") + 8
    assert budget >= MIN_RESPONSE_BYTES
    assert read(max_bytes=budget).get("status") == "ok"
    with adjusted_scope([_lifted("max_bytes", "target")]):
        note = adjusted_reserve_bytes()
        refused = read(max_bytes=budget)
    assert refused["status"] == "refused", refused
    error = refused["error"]
    assert error["code"] == "response_byte_budget_exceeded"
    assert error["adjusted_note_bytes"] == note > 8
    assert f"up to {note} of those bytes are the `adjusted` note" in error["message"]
    assert budget < error["needed_bytes"] <= budget + note
    assert refused["adjusted"] == [_lifted("max_bytes", "target")]
    # The size it names is enough.
    with adjusted_scope([_lifted("max_bytes", "target")]):
        assert read(max_bytes=error["needed_bytes"]).get("status") == "ok"


def test_a_page_with_the_note_fits_the_smallest_budget():
    read = bound_read_tool("filter", _rows)
    largest = list(rewrite_call(
        "filter", {"input": {"kind": "nodes"}, "predicate": _many(200)},
    ).adjusted)
    for note in (NOTE, largest):
        with adjusted_scope(note):
            assert adjusted_reserve_bytes() > 0
            page = read(max_bytes=MIN_RESPONSE_BYTES)
        assert page.get("status") != "refused", page
        assert page["adjusted"] == note
        assert page["items"], "a row fits beside the note"
        assert public_call_tool_result_bytes(page, tool="filter") <= MIN_RESPONSE_BYTES
    # Failure case: without the note the same budget holds more rows, so the
    # note's bytes were counted.
    plain = read(max_bytes=MIN_RESPONSE_BYTES)
    assert "adjusted" not in plain and len(plain["items"]) > len(page["items"])
    assert adjusted_reserve_bytes() == 0 and call_adjusted() == []


def test_a_read_tool_called_inside_another_does_not_nest_a_second_note():
    inner = bound_read_tool("filter", _rows)

    def outer(limit: int = 5, max_bytes: Any = None) -> dict[str, Any]:
        return {"status": "ok", "items": [inner(limit=limit)], "truncated": False}

    read = bound_read_tool("map", outer)
    with adjusted_scope(NOTE):
        page = read(limit=2)
        again = read(limit=2)
    assert page["adjusted"] == NOTE and again["adjusted"] == NOTE
    assert "adjusted" not in page["items"][0]
    assert len(page["items"][0]["items"]) == 2
    assert "adjusted" not in read(limit=2)


# --------------------------------------------------------------------------- #
# through the server, before any connection                                    #
# --------------------------------------------------------------------------- #

# A port nothing listens on: validation runs before any connection. Built
# from pieces so the public-copy export does not read it as a credential.
DEAD_DSN = "postgresql://postgres" + ":okg" + "@localhost:1/okg_probe"


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


NEW_SPELLINGS = [
    ("search", {"query": "q", "where": {"type": "t", "attrs.a": [1, 2]}}),
    ("search", {"query": "q", "where": {"field": "type", "value": "t"}}),
    ("filter", {"where": {"type": "jira_issue", "attrs.status": "Open"}}),
    ("filter", {"input": {"kind": "nodes"}, "predicate": {"attrs.status": "Open"}}),
    ("filter", {"input": {"kind": "nodes", "max_bytes": 8192}, "predicate": {}}),
    ("map", {"where": {"type": "git_tag"}, "select": ["node_id", "attrs.name"]}),
    ("aggregate", {"where": {"type": "t"}, "by": ["attrs.status"], "metrics": ["count"]}),
    ("expand", {"input": "n:1", "where": {"type": "t"}}),
    ("expand", {"input": {"node_id": "n:1"}, "filter": {"id": ["a"]}}),
    ("inspect", {"target": {"kind": "node", "node_id": "n:1", "select": ["attrs"]}}),
]


def _checks(server: Any, tool: str, arguments: dict[str, Any]) -> None:
    server._refuse_unknown_operator_arguments(tool, arguments)
    server._validate_operator_arguments(tool, arguments)


@pytest.mark.parametrize("tool,arguments", NEW_SPELLINGS)
def test_the_rewritten_call_is_the_one_the_argument_checks_accept(mcp_server, tool, arguments):
    # As sent, today's checks refuse it: the rewrite is what makes it a call.
    with pytest.raises((InvalidOperatorArguments, UnknownOperatorArgument)):
        _checks(mcp_server, tool, arguments)
    _checks(mcp_server, tool, rewrite_call(tool, arguments).arguments)


def _refusal(server: Any, tool: str, arguments: dict[str, Any]) -> tuple[dict[str, Any], str]:
    """A refused call's JSON reply and its text reply."""
    async def run() -> tuple[Any, Any]:
        return await call_json(server, tool, arguments), await server.call_tool(tool, arguments)

    as_json, as_text = anyio.run(run)
    assert as_json.isError is True and as_text.isError is True
    reply = payload(as_json)
    assert reply["status"] == "refused"
    assert reply["error"]["code"] == "invalid_operator_arguments"
    return reply, as_text.content[0].text


def test_both_spellings_are_refused_by_the_served_tool(mcp_server):
    reply, text = _refusal(mcp_server, "filter", {
        "where": {"attrs.a": 1}, "predicate": {"op": "eq", "field": "attrs.a", "value": 2},
    })
    assert "filter refused invalid_operator_arguments" in text
    assert "`where` and `predicate` both carry a condition and they differ" in text
    assert "retry_call" not in reply["error"]
    assert call_adjusted() == [], "nothing of the call is left behind"


def test_values_with_no_op_is_refused_by_the_served_tool_with_both_forms(mcp_server):
    reply, text = _refusal(mcp_server, "search", {
        "query": "q", "where": {"field": "date.created", "values": ["2025-01-01", "2025-12-31"]},
    })
    assert "search refused invalid_operator_arguments" in text
    assert "`in` or `between`" in reply["error"]["message"]
    assert '{"op": "between", "field": "date.created", "values": ["2025-01-01", "2025-12-31"]}' in (
        reply["error"]["fix_hint"]
    )
    assert "retry_call" not in reply["error"]


def test_an_unknown_key_is_refused_by_the_served_tool_with_the_corrected_call(mcp_server):
    sent = {"where": {"type": "jira_issue", "attrs.priority": "High", "status": "Open"}}
    corrected = {"where": {"type": "jira_issue", "attrs.priority": "High", "attrs.status": "Open"}}
    reply, text = _refusal(mcp_server, "filter", sent)
    assert "`status` is not a field filter compares" in reply["error"]["message"]
    # A guess is offered as one: the mode and the words both say so.
    assert reply["error"]["retry_call"] == {
        "tool": "filter", "mode": "guess", "arguments": corrected,
    }
    assert reply["error"]["fix_hint"] == (
        "if `status` is a stored attribute, write `attrs.status`: "
        + render_call("filter", corrected)
    )
    assert reply["error"]["fix_hint"].endswith(
        'filter(where={"type": "jira_issue", "attrs.priority": "High", "attrs.status": "Open"})'
    )
    assert "<" not in reply["error"]["fix_hint"]
    assert "filter refused invalid_operator_arguments" in text and "attrs.status" in text
    # The corrected call is a call: the argument checks accept its rewrite.
    _checks(mcp_server, "filter", rewrite_call("filter", corrected).arguments)


def test_a_logical_key_is_refused_by_the_served_tool_with_its_long_form(mcp_server):
    reply, _ = _refusal(mcp_server, "search", {
        "query": "q", "where": {"or": [{"type": "a"}, {"type": "b"}]},
    })
    assert reply["error"]["retry_call"] == {
        "tool": "search", "mode": "corrected", "arguments": {
            "query": "q", "where": {"op": "or", "clauses": [{"type": "a"}, {"type": "b"}]},
        },
    }
    assert reply["error"]["fix_hint"].startswith('search(query="q", where={"op": "or"')


def test_a_corrected_call_too_long_for_the_line_is_said_in_words(mcp_server):
    sent = {"where": {"status": "Open", **{f"attrs.key_{i}": "v" * 40 for i in range(40)}}}
    reply, _ = _refusal(mcp_server, "filter", sent)
    assert reply["error"]["fix_hint"] == (
        "if `status` is a stored attribute, write `attrs.status`"
    )
    assert "retry_call" not in reply["error"]


@pytest.mark.parametrize("tool,arguments,needle", [
    ("filter", {"input": {"ref": "l1.x"}, "where": {"date.created": "2025-01-01"}},
     "is a date field, which reads graph records"),
    ("filter", {"input": {"kind": "nodes"}, "predicate": {"field": "status", "value": "Open"}},
     "`status` is not a field filter compares"),
    ("filter", {"where": {"op": "eq", "field": "status", "value": "Open"}},
     "`status` is not a field filter compares"),
    ("map", {"input": {"ref": "r1.x"}, "where": {"status": "Open"}}, "`status` is not a field"),
    ("filter", {"where": {"Type": "doc"}}, "`Type` is not a field filter compares"),
    ("filter", {"where": {"title": "x"}}, "`title` is not a field filter compares"),
    ("search", {"query": "q", "where": {"source_family": "docs"}}, "source_family"),
    ("filter", {"where": {"attrs.n": {"gt": 3}}}, "names `gt` as a key"),
    ("search", {"query": "q", "where": {"type": {"op": "and", "clauses": [
        {"op": "eq", "value": "a"}]}}}, "joins conditions"),
])
def test_a_name_the_tool_cannot_evaluate_is_refused_by_the_served_tool(
        mcp_server, tool, arguments, needle):
    """Each reviewed input, through the server: refused before any read,
    and any `retry_call` it carries passes the argument checks as written."""
    reply, text = _refusal(mcp_server, tool, arguments)
    assert needle in reply["error"]["message"], reply["error"]
    assert f"{tool} refused invalid_operator_arguments" in text
    retry = reply["error"].get("retry_call")
    if retry is not None:
        assert retry["tool"] == tool and retry["mode"] in {"corrected", "guess"}
        again = rewrite_call(tool, retry["arguments"])
        assert again.adjusted == ()
        _checks(mcp_server, tool, again.arguments)
    else:
        assert reply["error"]["fix_hint"]


@pytest.mark.parametrize("where", [
    {"not": {}},
    {"and": [1]},
    {"attrs.n": {"between": [1]}},
    {"subtype": {"contains": "x"}},
    {"attrs.k": {"exists": "yes"}},
    {"type": []},
    # The grammar takes the repaired call; the tool's argument model does
    # not (no such operator), so it is not offered.
    {"attrs.n": {"op": "nearly", "value": 1}, "status": "x"},
])
def test_the_served_tool_offers_no_retry_its_own_argument_checks_refuse(mcp_server, where):
    """Item A: a retry is validated by the checks a dispatched call meets."""
    reply, _ = _refusal(mcp_server, "filter", {"where": where})
    assert "retry_call" not in reply["error"], reply["error"]
    assert reply["error"]["fix_hint"] and "filter(" not in reply["error"]["fix_hint"]


def test_every_retry_the_served_tool_offers_passes_its_argument_checks(mcp_server):
    """The argument checks only; that an offered call is SERVED is proven by
    dispatching it, in `test_read_grammar_db.py`."""
    offered = 0
    for tool, arguments in (
        ("filter", {"where": {"attrs.k": {"exists": False}}}),
        ("filter", {"where": {"not": {"type": "a"}, "status": "Open"}}),
        ("filter", {"where": {"Type": "doc", "attrs.n": {"gt": 3}}}),
        ("filter", {"input": {"kind": "edges"}, "where": {"type": "t"}}),
        ("map", {"where": {"or": [{"type": "a"}, {"type": "b"}]}}),
        ("expand", {"input": "n:1", "where": {"field": "status", "value": "x"}}),
        ("search", {"query": "q", "where": {"not": {"type": "a"}}}),
    ):
        reply, _ = _refusal(mcp_server, tool, arguments)
        retry = reply["error"]["retry_call"]
        _checks(mcp_server, tool, rewrite_call(tool, retry["arguments"]).arguments)
        offered += 1
    assert offered == 7
    reply, _ = _refusal(mcp_server, "filter", {"where": {"attrs.k": {"exists": False}}})
    assert reply["error"]["retry_call"] == {
        "tool": "filter", "mode": "corrected",
        "arguments": {"where": {"op": "not", "clause": {"attrs.k": {"op": "exists"}}}},
    }
    reply, _ = _refusal(mcp_server, "filter", {"where": {"not": {"type": "a"}, "status": "Open"}})
    assert reply["error"]["retry_call"]["mode"] == "guess"
    assert reply["error"]["fix_hint"].startswith(
        "write `not` with `op`; if `status` is a stored attribute, write "
        "`attrs.status`: filter(where="
    )


@pytest.mark.parametrize("tool,arguments", [
    # M2: each of these has one fix, and the fixed call is refused by a check
    # the tool body makes on its arguments. Words, not a call. (Since step
    # 2c filter compares a column by prefix, suffix and range, so a fixed
    # call with one is offered: see test_a_retry_may_compare_a_column.)
    ("filter", {"where": {"Type": "t", "type": {"op": "gt", "value": "a"}}}),
    # (#3666: `contains` with text on an attribute is served, so a number.)
    ("filter", {"where": {"Type": "t", "attrs.name": {"op": "contains", "value": 5}}}),
    ("filter", {"input": {"kind": "edges", "subtype": "x"}, "where": {"type": "e"}}),
    ("search", {"query": "q", "where": {"Type": "t", "date.bogus": "2025-01-01"}}),
    ("search", {"where": {"Type": "t"}}),
    ("filter", {"where": {"Type": "t", "text_hash": "abc"}}),
    # Classes of call whose other arguments only the read can judge: a count
    # by a date period (an attribute or a column is offered, #3666).
    ("aggregate", {"where": {"Type": "t"}, "by": ["month(date.created)"],
                   "metrics": ["count"]}),
    # (filter with a select of columns or attributes is offered since step
    # 2c's third review; the client-level table test dispatches such offers.)
    ("filter", {"where": {"Type": "t"}, "select": ["node_id", "text"]}),
    ("filter", {"where": {"Type": "t"}, "order_by": "attrs.created"}),
    ("filter", {"input": {"ref": "r1.abc"}, "where": {"Type": "t"}}),
    ("map", {"where": {"Type": "t"}, "select": ["node_id"]}),
    ("expand", {"input": "n:1", "path": ["contains:out"], "where": {"Type": "t"}}),
    ("expand", {"input": "n:1", "direction": "sideways", "where": {"Type": "t"}}),
    ("filter", {"where": {"Type": "t"}, "max_bytes": 100}),
    ("filter", {"where": {"Type": "t"}, "generation_id": "g1"}),
    # D1: a second mistake the repair leaves in place, in a clause the
    # grammar passes on unchecked or in an operand. (On filter, map and
    # aggregate a long-form clause is checked since step 2c, so the repair
    # covers it: see test_a_retry_may_compare_a_column.)
    ("expand", {"input": "n:1", "filter": {"op": "and", "clauses": [
        {"op": "eq", "field": "type", "value": "paper"}, {"attrs.year": {"gt": 2020}}]}}),
    ("filter", {"input": {"kind": "nodes"}, "predicate": {"op": "and", "clauses": [
        {"op": "exists", "field": "attrs."}, {"Type": "t"}]}}),
    ("filter", {"input": {"kind": "edges"}, "predicate": {"op": "and", "clauses": [
        {"op": "eq", "field": "subtype", "value": "x"}, {"type": "e"}]}}),
    ("filter", {"where": {"Type": "doc", "attrs.path": {"op": "suffix", "value": ""}}}),
    ("filter", {"where": {"Type": "doc", "attrs.path": {"op": "suffix", "value": 5}}}),
    ("filter", {"where": {"Type": "doc", "attrs.path": {"op": "prefix"}}}),
    ("filter", {"where": {"op": "and", "clauses": [
        {"op": "gt", "field": "attrs", "value": 1}, {"Type": "t"}]}}),
    ("expand", {"input": "n:1", "where": {"op": "and", "clauses": [
        {"op": "prefix", "field": "attrs", "value": "a"}, {"Type": "t"}]}}),
])
def test_no_retry_is_offered_that_the_tool_body_would_refuse(mcp_server, tool, arguments):
    reply, _ = _refusal(mcp_server, tool, arguments)
    assert "retry_call" not in reply["error"], reply["error"]
    assert reply["error"]["fix_hint"] and f"{tool}(" not in reply["error"]["fix_hint"]


@pytest.mark.parametrize("tool,arguments,offered", [
    # Step 2c: filter compares a column by prefix, suffix and range.
    ("filter", {"where": {"id": {"prefix": "doc:"}}},
     {"where": {"id": {"op": "prefix", "value": "doc:"}}}),
    ("filter", {"where": {"Type": "t", "id": {"op": "suffix", "value": ":1"}}},
     {"where": {"type": "t", "id": {"op": "suffix", "value": ":1"}}}),
    # #3666: `contains` with text on an attribute is served, so the repaired
    # call is offered.
    ("filter", {"where": {"Type": "t", "attrs.name": {"op": "contains", "value": "foo"}}},
     {"where": {"type": "t", "attrs.name": {"op": "contains", "value": "foo"}}}),
    # Step 2c: a long-form clause of predicate and input.filter is checked,
    # so the repair reaches it (writing a second name as its column, so the
    # offered call needs no note) and the repaired call is served.
    ("filter", {"input": {"kind": "nodes"}, "predicate": {"op": "and", "clauses": [
        {"op": "eq", "field": "type", "value": "paper"}, {"attrs.year": {"gt": 2020}}]}},
     {"input": {"kind": "nodes"}, "predicate": {"op": "and", "clauses": [
         {"op": "eq", "field": "subtype", "value": "paper"},
         {"attrs.year": {"op": "gt", "value": 2020}}]}}),
    ("map", {"input": {"kind": "nodes", "filter": {"op": "and", "clauses": [
        {"op": "eq", "field": "type", "value": "paper"}, {"attrs.year": {"gt": 2020}}]}}},
     {"input": {"kind": "nodes", "filter": {"op": "and", "clauses": [
         {"op": "eq", "field": "subtype", "value": "paper"},
         {"attrs.year": {"op": "gt", "value": 2020}}]}}}),
    ("filter", {"input": {"kind": "nodes"}, "predicate": {"op": "and", "clauses": [
        {"op": "exists", "field": "bogus"}, {"Type": "t"}]}},
     {"input": {"kind": "nodes"}, "predicate": {"op": "and", "clauses": [
         {"op": "exists", "field": "attrs.bogus"}, {"type": "t"}]}}),
])
def test_a_retry_may_compare_a_column(mcp_server, tool, arguments, offered):
    reply, _ = _refusal(mcp_server, tool, arguments)
    retry = reply["error"]["retry_call"]
    assert retry["arguments"] == offered, retry
    _checks(mcp_server, tool, rewrite_call(tool, retry["arguments"]).arguments)


def test_the_served_refusal_names_authority_and_never_echoes_it(mcp_server):
    """M3: the caller's claim and context are in no part of the refusal."""
    authority = {"identity_claim": "CLAIM-MARK", "access_context": "CONTEXT-MARK"}
    reply, text = _refusal(mcp_server, "filter", {"where": {"Type": "x"}, "authority": authority})
    assert reply["error"]["retry_call"] == {
        "tool": "filter", "mode": "corrected", "arguments": {"where": {"type": "x"}},
        "repeat": ["authority"],
    }
    assert reply["error"]["fix_hint"] == 'filter(where={"type": "x"}), with the same authority'
    for served in (json_text(reply), text):
        assert "CLAIM-MARK" not in served and "CONTEXT-MARK" not in served
    guessed, text = _refusal(
        mcp_server, "filter", {"where": {"status": "x"}, "authority": authority})
    assert guessed["error"]["retry_call"]["repeat"] == ["authority"]
    assert "authority" not in guessed["error"]["retry_call"]["arguments"]
    assert "CLAIM-MARK" not in json_text(guessed) + text
    plain, _ = _refusal(mcp_server, "filter", {"where": {"Type": "x"}})
    assert "repeat" not in plain["error"]["retry_call"]


def _denied(message: str) -> SubstrateError:
    """The gate's own denial: a `SubstrateError` instance carrying the
    registered code. A new subclass here would count as a product error
    code in the registry's parity check."""
    err = SubstrateError(message)
    err.code = "nomos_identity_denied"
    return err


class _OnlyInspect:
    """A session surface that shows `inspect` alone."""

    ceiling = frozenset({"inspect", "filter"})

    def visible_for(self, _identity: Any) -> frozenset[str]:
        return frozenset({"inspect"})

    def take_pending_notification(self, _identity: Any) -> bool:
        return False


#: Calls whose rewrite lifts an argument, refuses, or does nothing.
_PROBES = [
    {"input": {"kind": "nodes", "max_bytes": 8192}, "predicate": {}},
    {"where": {"status": "x"}},
    {"input": {"kind": "nodes"}, "predicate": {}},
]


def _replies(server: Any, tool: str) -> list[tuple[bool, str, str]]:
    async def run() -> list[tuple[bool, str, str]]:
        out = []
        for arguments in _PROBES:
            as_json = await call_json(server, tool, arguments)
            as_text = await server.call_tool(tool, arguments)
            out.append((as_text.isError, as_json.content[0].text, as_text.content[0].text))
        return out

    return anyio.run(run)


@pytest.mark.parametrize("barrier", ["gate", "not_visible"])
def test_a_refused_caller_cannot_tell_whether_the_tool_is_registered(
        monkeypatch: pytest.MonkeyPatch, barrier):
    """D2: a call stopped by the principal gate, or by the session's
    surface, gets byte for byte the same reply from a server that registers
    `filter` and from one that does not."""
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_RO_DSN", DEAD_DSN)
    servers = [build_server(dsn=DEAD_DSN), build_server(dsn=DEAD_DSN)]
    try:
        del servers[1]._tool_manager._tools["filter"]
        for server in servers:
            if barrier == "gate":
                def gate(_name: str) -> None:
                    raise _denied("this principal may not call tools")

                server._okg_dispatch_principal_gate = gate
            else:
                server._okg_session_tool_surface = _OnlyInspect()
        with_tool, without_tool = (_replies(server, "filter") for server in servers)
    finally:
        for server in servers:
            server._okg_pool.close()
            if server._okg_write_pool:
                server._okg_write_pool.close()
    assert with_tool == without_tool
    for is_error, as_json, as_text in with_tool:
        assert is_error
        assert "is not a field" not in as_json + as_text
        assert "invalid_operator_arguments" not in as_json + as_text
    expected = "nomos_identity_denied" if barrier == "gate" else "not in this session"
    assert expected in with_tool[0][2]


def test_a_tool_this_server_does_not_register_gets_no_grammar_refusal(mcp_server):
    """The rewrite runs only for a tool served to this session: a narrowed
    surface answers a missing tool as it always did."""
    del mcp_server._tool_manager._tools["map"]

    async def run() -> tuple[Any, Any]:
        return (
            await mcp_server.call_tool("map", {"where": {"status": "x"}}),
            await mcp_server.call_tool("map", {"input": {"kind": "nodes"}}),
        )

    new, old = anyio.run(run)
    for result in (new, old):
        text = result.content[0].text
        assert "is not a field" not in text and "invalid_operator_arguments" not in text
    # The same answer whichever spelling was sent.
    assert new.isError is old.isError
    assert new.content[0].text.split(" ")[:3] == old.content[0].text.split(" ")[:3]
    # A registered tool beside it is still rewritten.
    reply, _ = _refusal(mcp_server, "filter", {"where": {"status": "x"}})
    assert "`status` is not a field filter compares" in reply["error"]["message"]


def json_text(reply: dict[str, Any]) -> str:
    return read_grammar.json.dumps(reply)


def test_only_a_call_the_caller_input_determines_is_called_corrected(mcp_server):
    reply, _ = _refusal(mcp_server, "filter", {"where": {"Type": "doc"}})
    assert reply["error"]["retry_call"] == {
        "tool": "filter", "mode": "corrected", "arguments": {"where": {"type": "doc"}},
    }
    assert reply["error"]["fix_hint"] == 'filter(where={"type": "doc"})'
    for sent in ({"title": "x"}, {"source": "s"}, {"path": "p"}, {"edge_type": "e"}):
        reply, _ = _refusal(mcp_server, "filter", {"where": sent})
        assert "retry_call" not in reply["error"], sent


def test_a_refusal_of_a_new_spelling_names_the_argument_it_was_read_as(mcp_server):
    reply, text = _refusal(mcp_server, "filter", {
        "where": {"attrs.a": {"op": "nearly", "value": 1}},
    })
    assert "filter refused invalid_operator_arguments" in text
    assert "predicate" in reply["error"]["message"]
    assert "`where` is read as `predicate`" in reply["error"]["fix_hint"]


def test_a_nested_argument_that_differs_keeps_the_move_refusal(mcp_server):
    reply, text = _refusal(mcp_server, "inspect", {
        "target": {"kind": "node", "node_id": "n:1", "max_bytes": 6000}, "max_bytes": 8192,
    })
    assert "inspect refused invalid_operator_arguments" in text
    assert reply["error"]["fix_hint"] == "move max_bytes out of target to the top level of the call"
    assert "adjusted" not in reply


def test_one_hop_target_subtype_is_not_rewritten_by_this_step(mcp_server):
    sent = {"input": {"node_id": "n:1"}, "edge_type": "contains", "target_subtype": "document_chunk"}
    assert rewrite_call("expand", sent).arguments == sent
    _checks(mcp_server, "expand", sent)


# --------------------------------------------------------------------------- #
# step 3a: one `select` vocabulary per tool                                    #
# --------------------------------------------------------------------------- #

import collections  # noqa: E402
from pathlib import Path  # noqa: E402

from okg.substrate.mcp import select_vocabulary  # noqa: E402
from okg.substrate.mcp.select_vocabulary import (  # noqa: E402
    FIXED_ROW_ALWAYS,
    LINEAGE_TOKEN,
    SEARCH_METHODS,
    call_shape,
    path_select,
    plan_select,
    project_rows,
    served_as_selected,
    shape_fields,
    vocabulary,
)

NOT_RETURNED = "select_not_returned"


def _shape_constants() -> dict[str, dict[str, tuple[str, ...]]]:
    """Each shape's selectable fields, read from where the shape is served."""
    import importlib

    from okg.substrate.mcp.response_shape import LINEAGE_SELECT_TOKEN

    # By full module name: the package exports a `search` function too.
    grep_search, record_window_search, search, search_views, tools = (
        importlib.import_module(f"okg.substrate.mcp.{name}")
        for name in ("grep_search", "record_window_search", "search", "search_views", "tools")
    )
    assert all(
        module.__name__.startswith("okg.substrate.mcp.") and hasattr(module, "__file__")
        for module in (grep_search, record_window_search, search, search_views, tools)
    )

    return {
        "search": {
            "lexical": (*search._LEXICAL_FIELDS, LINEAGE_SELECT_TOKEN),
            "grep": grep_search.GREP_FIELDS,
            "attrs": search.ATTRS_LOOKUP_FIELDS,
            "semantic": (*search._SEMANTIC_FIELDS, LINEAGE_SELECT_TOKEN),
            "alias": (*search._ALIAS_FIELDS, "node_id"),
            "identifier_mentions": tools._FIND_MENTIONS_FIELDS,
            "list": search_views.LIST_FIELDS,
            "grep_list": (*search_views.GREP_LIST_FIELDS, "parent"),
            "windows": record_window_search.WINDOW_ROW_FIELDS,
        },
        "expand": {
            # #3666: a 1-hop row is the linked record; link rows are what a
            # `select` of only the link's old names asks for.
            "records": (*tools.EXPAND_RECORD_FIELDS, LINEAGE_SELECT_TOKEN),
            "edges": (*tools._LIST_NEIGHBORS_FIELDS, "link.attrs"),
            "path": (*tools._TRAVERSE_FIELDS, LINEAGE_SELECT_TOKEN),
        },
    }


@pytest.mark.parametrize("tool", ["search", "expand"])
def test_the_vocabulary_is_the_union_of_the_fields_each_shape_serves(tool):
    served = _shape_constants()[tool]
    assert {shape: set(fields) for shape, fields in shape_fields(tool).items()} == {
        shape: set(fields) for shape, fields in served.items()
    }
    words = vocabulary(tool)
    assert set(words) == {name for fields in served.values() for name in fields}
    for name, shapes in words.items():
        assert set(shapes) == {shape for shape, fields in served.items() if name in fields}
    assert set(select_vocabulary.SHAPE_WORDS) >= set(served)


def test_a_field_a_shape_gains_is_in_the_vocabulary(monkeypatch):
    import importlib

    grep_search = importlib.import_module("okg.substrate.mcp.grep_search")
    assert "brand_new_field" not in vocabulary("search")
    monkeypatch.setattr(
        grep_search, "GREP_FIELDS", (*grep_search.GREP_FIELDS, "brand_new_field"),
    )
    shape_fields.cache_clear()
    vocabulary.cache_clear()
    try:
        assert vocabulary("search")["brand_new_field"] == ("grep",)
    finally:
        monkeypatch.undo()
        shape_fields.cache_clear()
        vocabulary.cache_clear()


def test_the_lineage_token_and_the_attrs_fields_are_the_servers_own():
    from okg.substrate.mcp.response_shape import LINEAGE_SELECT_TOKEN
    from okg.substrate.mcp.search import _ALIAS_FIELDS, _ALIAS_SELECTABLE

    assert LINEAGE_TOKEN == LINEAGE_SELECT_TOKEN
    # The alias page's default row is unchanged: `node_id` is selected only.
    assert "node_id" not in _ALIAS_FIELDS and set(_ALIAS_SELECTABLE) - set(_ALIAS_FIELDS) == {"node_id"}
    assert vocabulary("filter") == {} and vocabulary("inspect") == {}
    # A view's shape name is not a method: only the six methods are.
    assert set(SEARCH_METHODS) == set(shape_fields("search")) - {"list", "grep_list", "windows"}


def test_a_grep_list_row_has_only_keys_a_select_can_name_or_always_keeps():
    from okg.substrate.mcp.search_views import (
        GREP_LIST_ALWAYS,
        GREP_LIST_SELECTABLE,
        grep_list_projection,
    )

    page = grep_list_projection({"result_unit": "passage", "results": [{
        "node_id": "c:1", "subtype": "doc_chunk", "matching_passages": 2, "own_match": True,
        "excerpt": "x", "parent": {"node_id": "d:1", "state": "resolved"},
        "match_windows": [{"char_start": 0, "line": 1, "excerpt": "x"}],
        "provenance": {"trust": "trusted"}, "read_provenance": {"k": "v"},
    }]})
    (row,) = page["results"]
    assert set(row) == set(GREP_LIST_SELECTABLE) | set(GREP_LIST_ALWAYS)
    assert FIXED_ROW_ALWAYS["grep_list"] == frozenset(GREP_LIST_ALWAYS)


def _select_note(call: read_grammar.RewrittenCall) -> dict[str, str]:
    (record,) = [r for r in call.adjusted if r["rule"] == NOT_RETURNED]
    assert set(record) == {"rule", "sent", "served"}
    return record


def test_a_field_another_method_returns_is_left_out_and_named():
    sent = {"query": "q", "select": ["node_id", "excerpt", "match_windows"]}
    call = _rewritten("search", sent)
    assert call.arguments == {"query": "q", "select": ["node_id", "excerpt"]}
    assert call.adjusted == ({
        "rule": NOT_RETURNED,
        "sent": "select `match_windows`",
        "served": (
            "not returned, method lexical does not return it (lexical ranks by terms "
            "and returns no match windows, so call method grep for it)"
        ),
    },)
    assert call.row_keys is None and call.select_retry is None
    # The same call with only the fields its rows have is not touched.
    plain = _rewritten("search", call.arguments)
    assert plain.arguments == call.arguments and plain.adjusted == ()


@pytest.mark.parametrize("arguments,kept,left_out", [
    ({"query": "q", "method": "grep", "select": ["node_id", "handle", "excerpt", "score"]},
     ["node_id", "handle", "excerpt"], ["score"]),
    ({"query": "q", "method": "grep", "select": ["node_id", "source_lineage", "attrs"]},
     ["node_id", "attrs"], ["source_lineage"]),
    ({"where": {"op": "eq", "field": "attrs.status", "value": "Open"},
      "select": ["node_id", "handle", "excerpt"]}, ["node_id", "handle"], ["excerpt"]),
    ({"where": {"type": "t", "attrs.status": "Open"}, "method": "attrs",
      "select": ["node_id", "attrs", "source_lineage"]}, ["node_id", "attrs"], ["source_lineage"]),
    ({"where": {"op": "eq", "field": "id", "value": "n:1"}, "method": "attrs",
      "select": ["node_id", "attrs", "source_lineage"]}, ["node_id", "attrs"], ["source_lineage"]),
    ({"query": "q", "view": "list", "select": ["node_id", "handle", "excerpt", "location"]},
     ["node_id", "handle"], ["excerpt", "location"]),
    ({"query": "q", "method": "semantic", "select": ["node_id", "score", "distance"]},
     ["node_id", "distance"], ["score"]),
    # The lineage token beside a field the rows have is still served.
    ({"query": "q", "select": ["source_lineage", "node_id", "match_windows"]},
     ["source_lineage", "node_id"], ["match_windows"]),
])
def test_each_search_shape_serves_the_fields_it_has(arguments, kept, left_out):
    call = _rewritten("search", arguments)
    assert call.arguments["select"] == kept
    record = _select_note(call)
    assert record["sent"] == "select " + ", ".join(f"`{name}`" for name in left_out)
    assert record["served"].startswith("not returned, ")
    # Never a field that was not selected.
    assert set(call.arguments["select"]) <= set(arguments["select"])


def test_a_record_field_asked_of_identifier_mentions_is_never_served_with_the_rest():
    """Its rows are spans of the query text. Served with the span fields, a
    select that also asked for records would return an empty page of another
    kind of row, which reads as "no such record"."""
    for select, grep_fields in [
        (["node_id", "value", "id_type", "evidence_kind"], ["node_id", "evidence_kind"]),
        (["node_id", "subtype", "handle", "excerpt", "value", "id_type"],
         ["node_id", "subtype", "handle", "excerpt"]),
        (["node_id"], ["node_id"]),
    ]:
        sent = {"query": "x-1 y", "method": "identifier_mentions", "select": select}
        call = _rewritten("search", sent)
        assert call.arguments == sent and call.adjusted == ()
        # Offered: the records whose text holds the identifier, from grep.
        assert call.select_retry == {"query": "x-1 y", "method": "grep", "select": grep_fields}
        assert call.select_retry_mode == "guess"
    spans = {"query": "x", "method": "identifier_mentions", "select": ["value", "id_type"]}
    assert _rewritten("search", spans).select_retry is None


#: One call of every shape, and a field only another shape returns.
_FOREIGN_ONLY = [
    ("search", {"query": "q"}, ["match_windows", "id_type"]),
    ("search", {"query": "q", "method": "grep"}, ["score", "distance"]),
    ("search", {"where": {"op": "eq", "field": "attrs.a", "value": 1}}, ["excerpt", "score"]),
    ("search", {"query": "q", "method": "semantic"}, ["score", "excerpt"]),
    ("search", {"query": "q", "method": "alias"}, ["score", "attrs"]),
    ("search", {"query": "q", "method": "identifier_mentions"}, ["node_id", "excerpt"]),
    ("search", {"query": "q", "view": "list"}, ["excerpt", "score"]),
    ("search", {"query": "q", "method": "grep", "view": "list"}, ["excerpt", "score"]),
    ("search", {"query": "q", "method": "grep", "view": "windows"}, ["excerpt", "handle"]),
    # No 1-hop expand row: its `select` picks its shape (#3666), see
    # test_no_select_of_a_one_hop_expand_names_only_foreign_fields.
    ("expand", {"input": "n:1", "depth": 2, "edge_type": "e"}, ["dst", "edge_type"]),
]


@pytest.mark.parametrize("tool,base,foreign", _FOREIGN_ONLY)
def test_a_select_of_only_another_shapes_fields_is_never_served(tool, base, foreign):
    """Decided from the select and the vocabulary, before any read: the call
    is refused here (fixed rows) or handed to the tool exactly as sent, whose
    own check refuses it. It is never narrowed to an empty select."""
    shape = call_shape(tool, base)
    assert shape is not None and not set(foreign) & shape_fields(tool)[shape]
    assert set(foreign) <= set(vocabulary(tool))
    sent = {**base, "select": foreign}
    if shape in FIXED_ROW_ALWAYS:
        assert _call_refused(tool, sent).code == "select_unknown_field"
        return
    call = _rewritten(tool, sent)
    assert call.arguments == sent and call.adjusted == () and call.row_keys is None


def test_every_shape_is_in_the_foreign_only_table():
    assert {call_shape(tool, base) for tool, base, _ in _FOREIGN_ONLY} == (
        set(shape_fields("search")) | set(shape_fields("expand"))
    ) - {"records", "edges"}


def test_no_select_of_a_one_hop_expand_names_only_foreign_fields():
    """Issue #3666: on a 1-hop expand the `select` picks the shape, so no
    select of fields this tool has is foreign to the rows it gets. Record
    fields are served as the linked record's; the one field only link rows
    have, alone, asks for link rows; a path field is a record field."""
    fields = shape_fields("expand")
    assert set(vocabulary("expand")) - fields["records"] == {"read_provenance"}
    assert fields["edges"] <= fields["records"] | {"read_provenance"}
    assert fields["path"] <= fields["records"] | {"source_lineage"}
    for select, shape in [
        (["node_id", "provenance"], "records"),
        (["read_provenance"], "edges"),
        (["dst", "edge_type"], "edges"),
    ]:
        sent = {"input": "n:1", "select": select}
        assert call_shape("expand", sent) == shape
        assert set(select) <= fields[shape]
        call = _rewritten("expand", sent)
        assert call.arguments == sent and call.adjusted == () and call.select_retry is None
    # Failure case: a name in no shape's vocabulary is still left refused.
    unknown = _rewritten("expand", {"input": "n:1", "select": ["label"]})
    assert unknown.arguments["select"] == ["label"] and unknown.select_retry is None


@pytest.mark.parametrize("tool,arguments", [
    # Nothing selected is a field of this call's rows: the tool refuses, as before.
    ("search", {"query": "q", "method": "grep", "select": ["score"]}),
    ("search", {"query": "q", "method": "grep", "select": ["source_lineage"]}),
    ("search", {"query": "x", "method": "identifier_mentions", "select": ["score"]}),
    ("search", {"where": {"op": "eq", "field": "type", "value": "t"}, "select": ["excerpt"]}),
    ("expand", {"input": "n:1", "depth": 2, "edge_type": "e", "select": ["src"]}),
    # A name in no shape's vocabulary: refused by the tool, never guessed
    # (on search the call without it is offered: the test below).
    ("expand", {"input": "n:1", "select": ["label", "dst"]}),
    ("expand", {"input": "n:1", "select": ["neighbor"]}),
    # A dotted attribute is a field of a linked record's row (#3666, lane
    # RD): left as sent, and the tool serves it.
    ("expand", {"input": "n:1", "select": ["node_id", "attrs.status"]}),
    # Only the lineage token would be left, and alone it means "the default
    # row plus the lineage": fields nobody selected. Left refused.
    ("expand", {"input": {"node_id": "n:1"}, "depth": 2, "edge_type": "e",
                "select": ["source_lineage", "edge_type"]}),
    ("search", {"query": "q", "select": ["source_lineage", "id_type"]}),
    ("search", {"query": "q", "method": "semantic", "select": ["source_lineage", "id_type"]}),
])
def test_a_select_the_rows_cannot_answer_is_left_for_the_tools_refusal(tool, arguments):
    call = _rewritten(tool, arguments)
    assert call.arguments == arguments and call.adjusted == ()
    assert call.select_retry is None and call.row_keys is None


@pytest.mark.parametrize("tool,arguments", [
    ("search", {"query": "q", "select": ["node_id", "excerpt"]}),
    ("search", {"query": "q", "select": ["source_lineage"]}),
    ("search", {"query": "q", "method": "grep", "select": ["node_id", "match_windows"]}),
    ("search", {"query": "q", "view": "list", "select": ["node_id", "parent"]}),
    ("search", {"query": "q", "method": "alias", "select": ["alias", "canonical_node_id"]}),
    ("search", {"query": "q", "select": []}),
    ("search", {"query": "q", "select": "node_id"}),
    ("search", {"query": "q", "select": ["node_id", 3]}),
    # The arguments do not settle the shape: left exactly as sent.
    ("search", {"query": "q", "method": "nope", "select": ["node_id", "match_windows"]}),
    ("search", {"query": "q", "view": "metadata", "select": ["node_id", "match_windows"]}),
    ("search", {"query": "q", "method": "windows", "select": ["node_id", "handle"]}),
    ("search", {"query": "q", "method": "grep_list", "select": ["node_id", "excerpt"]}),
    ("search", {"query": "q", "method": "list", "select": ["node_id", "excerpt"]}),
    # The attribute lookup takes no query: the tool refuses the call itself.
    ("search", {"query": "q", "method": "attrs", "select": ["node_id", "source_lineage"]}),
    # Nor a lookup with no condition: it refuses that too. (A condition on
    # the type alone lists that type since step 2b, so its select is read.)
    ("search", {"method": "attrs", "select": ["node_id", "excerpt"]}),
    ("search", {"select": ["node_id", "excerpt"]}),
    ("search", {"query": "q", "view": "windows", "select": ["node_id"]}),
    # #3666: a 1-hop row is the linked record, so its fields are served.
    ("expand", {"input": "n:1", "select": ["node_id", "subtype", "attrs", "provenance"]}),
    ("expand", {"input": "n:1", "direction": "in", "select": ["node_id", "attrs"]}),
    ("expand", {"input": "n:1", "select": ["node_id", "edge_type", "src", "dst", "attrs"]}),
    ("expand", {"input": "n:1", "direction": "both", "select": ["node_id", "dst"]}),
    ("expand", {"input": "n:1", "select": ["node_id", "source_lineage"]}),
    ("expand", {"input": "n:1", "select": ["link", "link.attrs", "link_count", "title"]}),
    # Only the link's own old names: link rows, as before.
    ("expand", {"input": "n:1", "select": ["edge_id", "src", "dst", "edge_type", "direction"]}),
    ("expand", {"input": "n:1", "select": ["edge_id", "link.attrs", "read_provenance"]}),
    ("expand", {"input": "n:1", "path": ["e:out"], "select": ["node_id", "provenance"]}),
    ("expand", {"next": "token", "select": ["node_id", "dst"]}),
    ("expand", {"input": "n:1", "depth": "2", "select": ["node_id", "dst"]}),
    ("filter", {"input": {"kind": "nodes"}, "predicate": {}, "select": ["node_id", "score"]}),
    ("inspect", {"target": {"kind": "node", "node_id": "n:1"}, "select": ["score"]}),
])
def test_a_select_that_was_valid_or_is_not_this_steps_is_passed_on_as_sent(tool, arguments):
    call = _rewritten(tool, arguments)
    assert call.arguments == arguments
    assert call.adjusted == () and call.row_keys is None and call.select_retry is None


@pytest.mark.parametrize("select", [
    ["dst", "attrs"], ["attrs", "edge_id", "read_provenance"],
    ["edge_id", "src", "dst", "edge_type", "direction", "attrs"],
])
def test_link_rows_with_attrs_are_served_as_sent_with_one_fixed_note(select):
    """A select of only link fields returns link rows, and `attrs` there is
    the link's own. The call is passed on unchanged; one note, in fixed
    words from a closed set, says whose `attrs` the rows carry."""
    from okg.substrate.mcp.select_vocabulary import (
        LINK_ROWS_ATTRS_NOTE,
        SELECT_NOTE_RULES,
    )

    assert set(SELECT_NOTE_RULES) == {"select_not_returned", "link_rows_attrs"}
    sent = {"input": "n:1", "select": select}
    call = _rewritten("expand", sent)
    assert call.arguments == sent and call.row_keys is None and call.select_retry is None
    assert list(call.adjusted) == [dict(LINK_ROWS_ATTRS_NOTE)]
    assert "each link's own attrs" in LINK_ROWS_ATTRS_NOTE["served"]
    assert "one row per link" in LINK_ROWS_ATTRS_NOTE["served"]
    # Failure cases: no note when `attrs` is the record's (a bare `attrs`, or
    # a record field beside it), when the link's are asked by their own
    # name, or on a path read.
    for other in (
        ["attrs"], ["node_id", "dst", "attrs"], ["edge_id", "link.attrs"],
    ):
        assert _rewritten("expand", {"input": "n:1", "select": other}).adjusted == (), other
    path = _rewritten("expand", {"input": "n:1", "path": ["e:out"], "select": ["attrs"]})
    assert path.adjusted == ()


def test_a_type_only_lookup_has_the_lookups_rows_so_its_select_is_read():
    """Step 2b: a `where` on the type alone lists that type, as cards or as
    a list, so a field its rows lack is left out and named."""
    where = {"op": "eq", "field": "type", "value": "t"}
    for extra in ({}, {"view": "list"}, {"method": "attrs"}):
        call = _rewritten("search", {"where": where, "select": ["node_id", "excerpt"], **extra})
        assert call.arguments["select"] == ["node_id"], extra
        assert [record["sent"] for record in call.adjusted] == ["select `excerpt`"]
    kept = _rewritten("search", {"where": where, "view": "list", "select": ["node_id", "attrs"]})
    assert kept.arguments["select"] == ["node_id", "attrs"] and kept.adjusted == ()


def test_a_path_page_leaves_out_a_link_field_and_says_where_it_is():
    path = _rewritten("expand", {"input": "n:1", "path": ["e:out"], "select": ["node_id", "dst"]})
    assert path.arguments["select"] == ["node_id"]
    assert path.sent_select == ("node_id", "dst")
    assert _select_note(path)["served"] == (
        "not returned, path rows do not return it (call a 1-hop expand for it)"
    )


def test_a_link_only_field_asked_of_a_record_row_is_left_out_and_named():
    """`read_provenance` is the link row's trust label. Beside a record
    field the rows are records, which carry it inside `link` with
    `link.attrs`: the name is left out and the note says where it is."""
    call = _rewritten("expand", {"input": "n:1", "select": ["node_id", "read_provenance"]})
    assert call.arguments["select"] == ["node_id"]
    assert _select_note(call) == {
        "rule": NOT_RETURNED,
        "sent": "select `read_provenance`",
        "served": (
            "not returned, 1-hop record rows do not return it "
            "(call a 1-hop expand that selects only link fields for it)"
        ),
    }


def test_a_continued_page_is_planned_as_its_first_page_was():
    """The caller repeats the call beside a `continuation_token`, so the
    shape, the fields and the note are the first page's."""
    for tool, first in [
        ("search", {"query": "q", "method": "grep", "select": ["node_id", "score"]}),
        ("search", {"query": "q", "method": "grep", "view": "list",
                    "select": ["node_id", "matches", "excerpt"]}),
        ("search", {"query": "q", "method": "grep", "view": "windows",
                    "select": ["node_id", "windows", "handle"]}),
        ("expand", {"input": "n:1", "path": ["e:out"], "select": ["node_id", "dst"]}),
    ]:
        one = _rewritten(tool, first)
        two = _rewritten(tool, {**first, "continuation_token": "token"})
        assert one.adjusted and two.adjusted == one.adjusted, (tool, first)
        assert two.row_keys == one.row_keys and two.sent_select == one.sent_select
        assert two.arguments == {**one.arguments, "continuation_token": "token"}
    # `expand(next=...)` carries no arguments: the token keeps the caller's
    # select (sorted), and the same fields and note are rebuilt from it.
    first = _rewritten("expand", {
        "input": "n:1", "depth": 2, "edge_type": "e", "select": ["subtype", "src", "node_id", "dst"],
    })
    select, notes = path_select(sorted(first.sent_select))
    assert set(select) == set(first.arguments["select"]) == {"node_id", "subtype"}
    assert notes == first.adjusted
    assert path_select(["node_id", "subtype"]) == (["node_id", "subtype"], ())
    assert path_select(None) == (None, ())
    untouched = _rewritten("expand", {"next": "token"})
    assert untouched.arguments == {"next": "token"} and untouched.adjusted == ()


@pytest.mark.parametrize("tool,arguments,select", [
    # Another letter case of a field, and the two second names.
    ("search", {"query": "q", "select": ["Node_ID", "excerpt"]}, ["node_id", "excerpt"]),
    ("search", {"query": "q", "select": ["id", "type", "node_id"]}, ["node_id", "subtype"]),
    # #3666: a 1-hop row is a record, so the second names are the record's.
    ("expand", {"input": "n:1", "select": ["id"]}, ["node_id"]),
    # (`type` beside a name no row has; alone it is served as `subtype`, see
    # test_select_second_names.py.)
    ("expand", {"input": "n:1", "direction": "in", "edge_type": "e", "select": ["type", "Attrs"]},
     ["subtype", "attrs"]),
    ("expand", {"input": "n:1", "select": ["Node_ID", "edge_type"]}, ["node_id", "edge_type"]),
    # The previous step refused these with words only, because a 1-hop row
    # was a link. The corrected call now serves the record, so it is offered.
    ("expand", {"input": {"node_id": "n:1"}, "select": ["Attrs", "node_id"]}, ["attrs", "node_id"]),
    ("expand", {"input": {"node_id": "n:1"}, "select": ["SOURCE_LINEAGE", "node_id"]},
     ["source_lineage", "node_id"]),
    ("expand", {"input": {"node_id": "n:1"}, "select": ["subtype", "ATTRS", "dst"]},
     ["subtype", "attrs", "dst"]),
])
def test_a_refused_select_the_input_determines_carries_the_corrected_call(tool, arguments, select):
    call = _rewritten(tool, arguments)
    assert call.arguments == arguments and call.adjusted == ()
    assert call.select_retry == {**arguments, "select": select}
    # The offered call is taken as written, and returns what it selects.
    again = _rewritten(tool, call.select_retry)
    assert again.arguments == call.select_retry and again.adjusted == ()
    assert again.select_retry is None
    assert served_as_selected(tool, call.select_retry)


@pytest.mark.parametrize("tool,arguments", [
    # Served as written (#3666), so there is nothing to correct.
    ("expand", {"input": "n:1", "direction": "both", "select": ["node_id"]}),
    ("expand", {"input": "n:1", "select": ["node_id", "provenance"]}),
    ("expand", {"input": "n:1", "select": ["node_id", "attrs"]}),
    ("expand", {"input": "n:1", "select": ["node_id", "source_lineage"]}),
    # A name with no single field: words only.
    ("expand", {"input": "n:1", "select": ["label"]}),
    ("expand", {"input": "n:1", "select": ["node_id", "attrs.status"]}),
    ("search", {"query": "q", "select": ["created"]}),
    ("search", {"query": "q", "method": "grep", "select": ["Score"]}),
    # No field of the select is one grep rows have: nothing to offer.
    ("search", {"query": "x", "method": "identifier_mentions", "select": ["score", "distance"]}),
])
def test_no_corrected_select_is_offered_when_the_input_does_not_determine_one(tool, arguments):
    call = _rewritten(tool, arguments)
    assert call.arguments == arguments and call.select_retry is None


@pytest.mark.parametrize("arguments,select", [
    # A name no search row has: the offered call drops it, or uses the field
    # the refusal's words name (`use 'excerpt'`), as the tool's hint does.
    # (`attrs.<key>` is served on search since D6: not here.)
    ({"query": "q", "select": ["node_id", "unknown_name"]}, ["node_id"]),
    ({"query": "q", "method": "grep", "select": ["node_id", "exerpt"]}, ["node_id", "excerpt"]),
    ({"query": "q", "select": ["snippet"]}, ["excerpt"]),
])
def test_a_refused_search_select_offers_the_call_without_the_unknown_name(arguments, select):
    call = _rewritten("search", arguments)
    assert call.arguments == arguments and call.adjusted == ()
    assert call.select_retry == {**arguments, "select": select}
    assert call.select_retry_mode == "corrected"
    assert served_as_selected("search", call.select_retry)


def test_a_where_name_in_a_search_select_is_read_as_its_row_field():
    call = _rewritten("search", {"query": "q", "select": ["node_id", "path"]})
    assert call.arguments == {"query": "q", "select": ["node_id", "handle"]}
    assert call.adjusted == ({
        "rule": "select_renamed", "sent": "select `path`",
        "served": "`handle`, which holds the title, path and date",
    },)
    assert call.select_retry is None


def test_the_corrected_select_never_carries_authority():
    call = _rewritten("expand", {
        "input": "n:1", "select": ["id"], "authority": {"trusted_only": True},
    })
    assert call.select_retry == {"input": "n:1", "select": ["node_id"]}
    assert call.select_retry_repeat == ("authority",)


def test_a_select_on_fixed_rows_is_taken_out_and_the_rows_are_cut():
    whole = _rewritten("search", {
        "query": "q", "method": "grep", "view": "list", "select": ["node_id", "matches"],
    })
    assert whole.arguments == {"query": "q", "method": "grep", "view": "list"}
    assert whole.adjusted == ()
    assert whole.row_keys == frozenset(
        {"node_id", "matches", "subtype", "unit", "provenance", "read_provenance"}
    )
    mixed = _rewritten("search", {
        "query": "q", "method": "grep", "view": "list",
        "select": ["node_id", "excerpt", "first_match"],
    })
    assert "select" not in mixed.arguments and "first_match" in mixed.row_keys
    assert _select_note(mixed)["sent"] == "select `excerpt`"
    windows = _rewritten("search", {
        "query": "q", "method": "grep", "view": "windows", "scope": {"record": "n:1"},
        "select": ["node_id", "windows", "handle"],
    })
    assert "select" not in windows.arguments
    # What says a record's windows may not be the whole answer is always kept.
    assert windows.row_keys == frozenset(
        {"node_id", "subtype", "windows", "decided", "refusal", "masked_occurrences",
         "continuity", "source_coverage", "passages"}
    )
    assert _select_note(windows)["sent"] == "select `handle`"


@pytest.mark.parametrize("select,words", [
    (["excerpt", "score"], "view list on method grep does not return"),
    (["node_id", "nope"], "is no field of this tool"),
])
def test_a_select_no_fixed_row_can_answer_is_refused_naming_the_rows_fields(select, words):
    refusal = _call_refused("search", {
        "query": "q", "method": "grep", "view": "list", "select": select,
    })
    assert words in refusal.message
    assert "returns first_match, matches, node_id, parent, subtype" in refusal.message
    assert refusal.retry_arguments is None and refusal.fields == ("select",)
    # The code and data every other `select` refusal has.
    assert refusal.code == "select_unknown_field"
    assert refusal.select_data["valid_fields"] == [
        "first_match", "matches", "node_id", "parent", "subtype"]
    assert set(refusal.select_data["unknown_fields"]) <= set(select)


def test_fixed_rows_are_cut_to_the_selected_keys_and_nothing_else_changes():
    page = {
        "view": "list", "total_count": 2, "next_offset": 2,
        "results": [
            {"node_id": "a", "subtype": "doc", "matches": 2, "first_match": {"line": 1},
             "provenance": {"trust": "trusted"}},
            {"node_id": "b", "subtype": "doc", "matches": 1},
        ],
    }
    before = copy.deepcopy(page)
    cut = project_rows(page, frozenset({"node_id", "subtype", "matches", "provenance"}))
    assert [row["node_id"] for row in cut["results"]] == ["a", "b"]
    assert cut["results"][0] == {
        "node_id": "a", "subtype": "doc", "matches": 2, "provenance": {"trust": "trusted"},
    }
    assert {k: v for k, v in cut.items() if k != "results"} == {
        k: v for k, v in before.items() if k != "results"
    }
    refusal = {"status": "refused", "error": {"code": "x"}, "results": [{"node_id": "a", "x": 1}]}
    assert project_rows(copy.deepcopy(refusal), frozenset({"node_id"})) == refusal


def test_node_id_is_a_field_of_an_alias_row():
    from okg.substrate.mcp.search import search_select_elsewhere

    sent = {"query": "q", "method": "alias", "select": ["node_id", "alias"]}
    call = _rewritten("search", sent)
    # Simply a field alias rows have: served as sent, with no note.
    assert call.arguments == sent and call.adjusted == ()
    assert "node_id" in shape_fields("search")["alias"]
    assert "node_id" not in search_select_elsewhere("alias")


def test_the_select_note_is_one_short_record_however_long_the_select():
    names = sorted(set(vocabulary("search")) - shape_fields("search")["grep"])
    call = _rewritten("search", {"query": "q", "method": "grep", "select": ["node_id", *names]})
    assert call.arguments["select"] == ["node_id"]
    record = _select_note(call)
    assert f"and {len(names) - 8} more" in record["sent"]
    assert len(json.dumps(record).encode()) <= MAX_ADJUSTED_BYTES
    assert len(call.adjusted) == 1


def test_the_text_reply_names_the_fields_left_out_and_prints_no_placeholder_for_them():
    call = _rewritten("search", {"query": "q", "select": ["node_id", "excerpt", "match_windows"]})
    page = {
        "status": "ok", "method": "lexical", "total_count": 1,
        "results": [{"node_id": "n:1", "subtype": "t", "excerpt": ""}],
        "adjusted": [dict(record) for record in call.adjusted],
    }
    lines = result_text.render(page, tool="search").splitlines()
    note = (
        "! adjusted: select `match_windows` read as not returned, method lexical does not "
        "return it (lexical ranks by terms and returns no match windows, so call method "
        "grep for it)"
    )
    assert note in lines
    assert not any("match_windows" in line for line in lines if line != note), lines
    assert "match_windows" not in json.dumps(page["results"])


def test_how_many_refused_selects_of_the_replay_the_rewrite_now_serves():
    """The select refusals of the replay behind this step (97 calls, values
    replaced by neutral ones), through the pure rewrite alone.

    `served_with_a_note` is an UPPER BOUND on what the server serves: the
    rewrite judges `select` only, and the tool may still refuse a call for
    another argument. One call of each (tool, method) class counted there is
    dispatched and must be served in
    test_read_grammar_db.py::test_a_select_naming_another_shapes_field_is_served_with_the_fields_its_rows_have.
    """
    from okg.substrate.mcp.response_bounds import _proven_servable

    data = json.loads(
        (Path(__file__).parent / "data" / "select_refusals_3666.json").read_text()
    )
    outcome: collections.Counter = collections.Counter()
    for row in data["rows"]:
        tool, arguments = row["tool"], row["arguments"]
        call = rewrite_call(tool, arguments)
        if any(record["rule"] == "select_renamed" for record in call.adjusted):
            # A `where` field (title, path, source) read as the row field
            # holding it (#3666).
            kind = "served_renamed"
        elif any(record["rule"] == NOT_RETURNED for record in call.adjusted):
            assert set(call.arguments["select"]) < set(arguments["select"])
            kind = "served_with_a_note"
        elif call_shape(tool, call.arguments) is None:
            # The arguments do not say which rows the call returns.
            assert call.arguments == arguments
            kind = "shape_not_settled"
        elif plan_select(tool, call.arguments) is None:
            # Every name is a field of the call's rows on this branch: the
            # refusal the agent met is already gone (grep handle, location).
            assert call.arguments == arguments
            kind = "valid_as_sent"
        elif call.select_retry is not None and _proven_servable(
            tool, call.select_retry, ceiling=None,
        ):
            kind = "refused_with_a_servable_retry"
        else:
            assert call.arguments == arguments and call.adjusted == ()
            kind = "refused_words_only"
        outcome[(tool, row["refusal_code"], kind)] += row["calls"]
    assert dict(outcome) == {
        # #3666: a 1-hop row is the linked record, so all 21 select fields
        # their rows have. test_expand_records_db.py dispatches each one.
        ("expand", "select_unknown_field", "valid_as_sent"): 21,
        ("search", "select_unknown_field", "served_with_a_note"): 18,
        # identifier_mentions asked for records: refused, with the grep call
        # for the same query (one more carries `max_chars`, which no offered
        # call may: words only).
        # ... and, since #3666's lenient yes, a select naming an unknown
        # field beside fields the rows have: the call without it.
        ("search", "select_unknown_field", "refused_with_a_servable_retry"): 20,
        # (+1, lane RD: `attrs.status` on the attribute lookup, which the
        # tool serves as `selected` since D6.)
        ("search", "select_unknown_field", "valid_as_sent"): 20,
        # `path` beside the rest: served as `handle` with a note.
        ("search", "select_unknown_field", "served_renamed"): 7,
        ("search", "select_unknown_field", "refused_words_only"): 1,
        ("search", "source_lineage_select_unsupported", "served_with_a_note"): 9,
        # method attrs with query text: the tool refuses the query.
        ("search", "source_lineage_select_unsupported", "shape_not_settled"): 1,
    }
    still = sorted({
        name for row in data["rows"] if row["tool"] == "search"
        for name in row["arguments"]["select"] if name not in vocabulary("search")
    })
    # The names of no search shape in the replay (`path` is now read as
    # `handle`; the rest are dropped from the offered call).
    assert still == ["attrs.status", "created", "date.created", "date.modified",
                     "date.resolved", "path"]


def test_a_refusal_carries_the_corrected_select_only_when_the_server_proves_it(mcp_server):
    from okg.substrate.mcp.response_bounds import _rewrite_operator_call

    def offered(tool: str, arguments: dict[str, Any]) -> Any:
        sent, state = _rewrite_operator_call(
            tool, arguments, accepts=mcp_server._okg_takes_as_written,
        )
        assert sent == arguments and state.refusal is None and state.adjusted == ()
        return state.select_retry

    # #3666: a 1-hop row is the linked record, so [node_id, subtype] is
    # served as sent; a second name is corrected to the record's field.
    assert offered("expand", {"input": {"node_id": "n:1"}, "select": ["node_id", "subtype"]}) is None
    assert offered("expand", {"input": {"node_id": "n:1"}, "select": ["id", "subtype"]}) == {
        "tool": "expand", "mode": "corrected",
        "arguments": {"input": {"node_id": "n:1"}, "select": ["node_id", "subtype"]},
    }
    assert offered("search", {"query": "q", "limit": 3, "select": ["id", "excerpt"]}) == {
        "tool": "search", "mode": "corrected",
        "arguments": {"query": "q", "limit": 3, "select": ["node_id", "excerpt"]},
    }
    assert offered("expand", {"input": "n:1", "select": ["id"], "authority": {}}) == {
        "tool": "expand", "mode": "corrected", "repeat": ["authority"],
        "arguments": {"input": "n:1", "select": ["node_id"]},
    }
    # An argument only the read can judge beside it: words, and no call.
    assert offered("expand", {
        "input": {"node_id": "n:1"}, "order_by": "node_id", "select": ["id"],
    }) is None
    assert offered("search", {"query": "q", "method": "lexical", "select": ["id"]}) is None
    # Nothing the input determines: no call either.
    assert offered("expand", {"input": "n:1", "select": ["node_id", "attrs.status"]}) is None
    # Records asked of identifier_mentions: the grep call for the same
    # query, marked a guess, with the fields of the select grep rows have.
    assert offered("search", {
        "query": "zirconium", "method": "identifier_mentions", "limit": 5,
        "select": ["node_id", "value", "id_type", "evidence_kind"],
    }) == {
        "tool": "search", "mode": "guess",
        "arguments": {"query": "zirconium", "method": "grep", "limit": 5,
                      "select": ["node_id", "evidence_kind"]},
    }
    assert offered("search", {
        "query": "zirconium", "method": "identifier_mentions", "max_chars": 500,
        "select": ["node_id", "value"],
    }) is None


def test_a_select_no_fixed_row_answers_is_refused_by_the_served_tool_as_a_select(mcp_server):
    async def run() -> Any:
        return await call_json(mcp_server, "search", {
            "query": "q", "method": "grep", "view": "list", "select": ["excerpt"],
        })

    result = anyio.run(run)
    reply = payload(result)
    assert result.isError is True and reply["status"] == "refused"
    assert reply["error"]["code"] == "select_unknown_field", reply["error"]
    assert "view list on method grep returns first_match, matches" in reply["error"]["message"]
    assert "retry_call" not in reply["error"]


def test_an_empty_select_on_fixed_rows_is_no_select():
    for view in ("list", "windows"):
        sent = {"query": "q", "method": "grep", "view": view, "select": []}
        call = _rewritten("search", sent)
        assert call.arguments == {"query": "q", "method": "grep", "view": view}
        assert call.adjusted == () and call.row_keys is None
    # A lexical list projects its rows itself and refuses an empty select.
    lexical = {"query": "q", "view": "list", "select": []}
    assert _rewritten("search", lexical).arguments == lexical


def test_a_case_variant_alone_on_a_link_row_is_still_corrected():
    call = _rewritten("expand", {"input": "n:1", "select": ["Attrs", "DST"]})
    assert call.select_retry == {"input": "n:1", "select": ["attrs", "dst"]}


def test_a_note_never_sends_the_caller_to_a_method_this_server_does_not_serve():
    sent = {"query": "q", "select": ["node_id", "distance", "match_windows"]}
    everywhere = _select_note(rewrite_call("search", sent))["served"]
    assert "distance: call method semantic" in everywhere
    assert "so call method grep" in everywhere
    lexical_only = rewrite_call("search", sent, search_methods=frozenset({"lexical"}))
    # The same fields are served either way: only the words differ.
    assert lexical_only.arguments == rewrite_call("search", sent).arguments
    served = _select_note(lexical_only)["served"]
    assert "semantic" not in served and "grep" not in served
    assert "distance: not available on this server" in served
    assert "match_windows: not available on this server" in served
    with_grep = rewrite_call("search", sent, search_methods=frozenset({"lexical", "grep"}))
    assert "so call method grep" in _select_note(with_grep)["served"]
    one = rewrite_call(
        "search", {"query": "q", "select": ["node_id", "distance"]},
        search_methods=frozenset({"lexical"}),
    )
    assert _select_note(one)["served"] == (
        "not returned, method lexical does not return it (not available on this server)"
    )
    # Nor is another method's call offered when that method is not served.
    mentions = {"query": "q", "method": "identifier_mentions", "select": ["node_id", "value"]}
    assert rewrite_call("search", mentions).select_retry is not None
    assert rewrite_call(
        "search", mentions, search_methods=frozenset({"lexical", "identifier_mentions"}),
    ).select_retry is None


def test_a_refused_select_gives_one_answer_not_two():
    from okg.substrate.mcp.envelope import SelectUnknownField, align_select_retry, validate_select
    from okg.substrate.mcp.search import _LEXICAL_FIELDS

    def refused() -> SelectUnknownField:
        with pytest.raises(SelectUnknownField) as caught:
            validate_select(["SCORE", "node_id"], _LEXICAL_FIELDS, tool_name="search")
        return caught.value

    error = refused()
    # On its own the refusal drops the refused name.
    assert error.data["retry_select"] == ["node_id"]
    align_select_retry(error, {
        "tool": "search", "mode": "corrected",
        "arguments": {"query": "q", "select": ["score", "node_id"]},
    })
    assert error.data["retry_select"] == ["score", "node_id"]
    assert "retry with select=['score', 'node_id']" in error.fix_hint
    assert "retry with select=['node_id']" not in error.fix_hint
    # Another method's call is a guess: the refusal's own list is withdrawn.
    guess = refused()
    align_select_retry(guess, {
        "tool": "search", "mode": "guess",
        "arguments": {"query": "q", "method": "grep", "select": ["node_id"]},
    })
    assert guess.data["retry_select"] is None
    assert "retry with select" not in guess.fix_hint and "drop select or choose from" in guess.fix_hint


def _big_refusal(**error: Any) -> dict[str, Any]:
    return {
        "status": "refused", "operator_name": "search",
        "error": {
            "code": "select_unknown_field",
            "message": "m " * 1500, "fix_hint": "h " * 1500,
            "data": {"valid_fields": [f"field_{i}" for i in range(60)]},
            "max_bytes": 4096, **error,
        },
    }


@pytest.mark.parametrize("fmt", ["json", "text"])
def test_a_fitted_refusal_gives_up_its_retry_call_last(fmt):
    """The order of sacrifice: data, the tails of hint and message, the hint
    and the message themselves, and only then the corrected call."""
    from okg.substrate.mcp.response_bounds import _fit_error_payload
    from okg.substrate.mcp.result_text import result_format_scope

    call = {"tool": "search", "mode": "guess", "repeat": ["authority"],
            "arguments": {"query": "zirconium", "method": "grep", "select": ["node_id"]}}
    with result_format_scope(fmt):
        fitted = _fit_error_payload(_big_refusal(retry_call=dict(call)), budget=4096)
        assert public_call_tool_result_bytes(fitted, is_error=True) <= 4096
        # Whole, with its mode and what the caller repeats.
        assert fitted["error"]["retry_call"] == call
        assert fitted["error"]["data"] == {"details_omitted": "response_byte_budget"}
        assert "clipped to fit" in fitted["error"]["message"]
        # The largest call a refusal may carry still fits beside the code.
        largest = {**call, "arguments": {"query": "q" * 1300, "method": "grep"}}
        assert len(json.dumps(largest)) <= 1536
        kept = _fit_error_payload(_big_refusal(retry_call=dict(largest)), budget=4096)
        assert public_call_tool_result_bytes(kept, is_error=True) <= 4096
        assert kept["error"]["retry_call"] == largest


def test_the_text_refusal_names_the_offered_call_on_its_first_line():
    def head(error: dict[str, Any]) -> str:
        page = {"status": "refused", "operator_name": "search", "error": error}
        return result_text.render(page, tool="search").splitlines()[0]

    call = {"tool": "search", "mode": "corrected",
            "arguments": {"query": "zirconium", "limit": 3, "select": ["node_id", "excerpt"]}}
    long_hint = "words " * 60
    # The hint is too long for the line: the call is rendered from retry_call.
    assert head({"code": "select_unknown_field", "fix_hint": long_hint, "retry_call": call}) == (
        'search refused select_unknown_field \u00b7 retry: search(query="zirconium", limit=3, '
        'select=["node_id","excerpt"])'
    )
    guess = {**call, "mode": "guess", "repeat": ["authority"]}
    assert head({"code": "c", "fix_hint": long_hint, "retry_call": guess}).endswith(
        'select=["node_id","excerpt"]), with the same authority (a guess)')
    # One renderer for every tool: when a call is offered the line is built
    # from `retry_call`, plain and unquoted, whatever the hint says (a
    # condition-grammar refusal's hint spells the call too).
    spelled = 'search(query="zirconium", where={"type": "ticket"})'
    plain = 'retry: search(query="zirconium", limit=3, select=["node_id","excerpt"])'
    assert head({"code": "c", "fix_hint": spelled, "retry_call": call}).endswith(plain)
    assert head({"code": "c", "retry_call": call}).endswith(plain)
    # Caller input stays one safe line: a line break or the line's own
    # separator in an argument is escaped inside its JSON, or the line
    # points at the field instead.
    hostile = {**call, "arguments": {"query": "a\nb \u00b7 retry: x"}}
    first = head({"code": "c", "retry_call": hostile})
    assert "\n" not in first and first.count("retry: ") == 1
    assert first.endswith("retry: see retry_call below") or '\\n' in first
    # A continuation's retry_call holds a token placeholder and a size
    # range, not a call to send: it keeps its hint.
    resume = {"tool": "inspect", "mode": "continue", "arguments": {"next": "<token>"},
              "max_bytes": {"min": 4096, "max": 8192}}
    assert head({
        "code": "c", "fix_hint": "continue with inspect(next=<this token>)", "retry_call": resume,
    }).endswith("retry: continue with inspect(next=<this token>)")
    # Too long for the line: it points at the call, never at the hint.
    wide = {**call, "arguments": {"query": "z" * 300}}
    assert head({"code": "c", "fix_hint": long_hint, "retry_call": wide}).endswith(
        "retry: see retry_call below")
    assert head({"code": "c", "fix_hint": "one line", "retry_call": wide}).endswith(
        "retry: see retry_call below")
    # No call offered: as before.
    assert head({"code": "c", "fix_hint": long_hint}).endswith("retry: see fix_hint")
    assert head({"code": "c", "fix_hint": "drop select"}).endswith("retry: drop select")
    # A continuation's retry_call is another shape and keeps its hint.
    token = {"next": "t", "max_bytes": {"min": 1, "max": 2}}
    assert head({"code": "c", "fix_hint": "raise max_bytes", "retry_call": token}).endswith(
        "retry: raise max_bytes")


def test_a_select_refusal_says_a_shared_hint_once():
    from okg.substrate.mcp.envelope import SelectUnknownField, validate_select
    from okg.substrate.mcp.search import search_select_elsewhere
    from okg.substrate.mcp.tools import _FIND_MENTIONS_FIELDS

    with pytest.raises(SelectUnknownField) as caught:
        validate_select(
            ["node_id", "subtype", "handle", "excerpt", "value"], _FIND_MENTIONS_FIELDS,
            tool_name="search(method=identifier_mentions)",
            elsewhere=search_select_elsewhere("identifier_mentions", query="CMS-433"),
        )
    error = caught.value
    # The fact once, in the message; the action once, in the hint; a fixed
    # word in `data`. One occurrence of the sentence in the whole refusal.
    assert error.message.count("spans of your query text") == 1
    assert "spans of your query text" not in error.fix_hint
    assert (
        "(node_id, subtype, handle, excerpt: identifier_mentions returns spans of your "
        "query text, not records)"
    ) in error.message
    assert error.fix_hint.startswith(
        'search(method="grep", query="CMS-433") finds the records whose text holds it')
    assert error.fix_hint.count('search(method="grep"') == 1
    assert "<value>" not in error.fix_hint and "<value>" not in error.message
    assert error.data["instead"] == {
        name: "rows_are_query_spans" for name in ("node_id", "subtype", "handle", "excerpt")}
    assert "spans of your query text" not in json.dumps(error.data)
    assert len(json.dumps(error.data)) < 1024
    # A long hint that is not in parts is still carried once in `data`.
    long_hint = "x" * 120
    with pytest.raises(SelectUnknownField) as shared:
        validate_select(["a", "b"], ["node_id"], tool_name="t",
                        elsewhere={"a": long_hint, "b": long_hint})
    assert shared.value.data["instead"] == {"a": long_hint, "b": "as 'a'"}
    # A query too long for one line is named in words, never as a placeholder.
    words = search_select_elsewhere("identifier_mentions", query="q" * 200)["node_id"]
    assert 'search(method="grep") with the same query' in words and "<value>" not in words
    # A short hint that names share stays whole for each of them.
    with pytest.raises(SelectUnknownField) as caught:
        validate_select(["node_id", "score", "recency"], ["node_id"], tool_name="t",
                        elsewhere={"score": "use lexical", "recency": "use lexical"})
    assert caught.value.data["instead"] == {"score": "use lexical", "recency": "use lexical"}
    assert "(score, recency: use lexical)" in caught.value.message


@pytest.mark.parametrize("tool", ["filter", "map"])
def test_a_keyset_next_call_gets_no_default_scope(tool):
    """M10's default scope (no `where`, no `input`) must not add `input`
    beside a 3b keyset token, which carries the whole read and refuses any
    read argument beside it."""
    call = _rewritten(tool, {"next": "tok", "limit": 5})
    assert "input" not in call.arguments and "where" not in call.arguments
    assert "predicate" not in call.arguments
    assert not any(record["rule"] == "scope_defaulted" for record in call.adjusted)
    # Failure case: without `next` the default still applies.
    assert any(
        record["rule"] == "scope_defaulted" for record in _rewritten(tool, {"limit": 5}).adjusted
    )


def test_filter_with_no_predicate_reads_the_whole_scope_with_a_note():
    """#3666 step 3b (agreed with M5's `predicate_missing`): `input` alone
    is every record in that scope, served with a note; a `next` call or a
    call with a condition carries no such note."""
    call = _rewritten("filter", {"input": {"kind": "nodes", "subtype": "t"}})
    assert call.arguments.get("predicate") is None
    assert {"rule": "predicate_missing", "sent": "no predicate",
            "served": "every record in the input scope"} in call.adjusted
    page = {"items": [], "adjusted": list(call.adjusted)}
    assert ("! adjusted: no predicate read as every record in the input scope"
            in result_text.render(page, tool="filter"))
    assert not any(
        record["rule"] == "predicate_missing"
        for record in _rewritten("filter", {"next": "tok"}).adjusted
    )
    assert not any(
        record["rule"] == "predicate_missing"
        for record in _rewritten(
            "filter", {"input": {"kind": "nodes"}, "predicate": EQ_STATUS},
        ).adjusted
    )


# --------------------------------------------------------------------------- #
# like and like_exact (PACT loose-match-3666)                                  #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("op", ["like", "like_exact"])
def test_like_short_and_long_forms_lower_to_one_clause(op):
    long_form = {"op": op, "field": "attrs.author_name", "value": "muller"}
    assert _lowered({"attrs.author_name": {"op": op, "value": "muller"}}) == long_form
    assert _lowered(long_form) == long_form
    assert _rewritten("filter", {"where": long_form}).arguments["predicate"] == long_form
    # The predicate model takes the operator the grammar passed on.
    from okg.substrate.mcp.operator_models import PREDICATE_OPS, Predicate

    assert op in PREDICATE_OPS
    assert Predicate.model_validate(long_form).op == op


@pytest.mark.parametrize("op", ["like", "like_exact"])
def test_like_written_as_a_key_is_refused_never_an_exact_object(op):
    """Guard: before this change `{"attrs.x": {"like": "a"}}` was refused with
    no call (and before #3775 served as eq on the object: 0 rows). Now an
    operator written as a key is refused with its `op` form."""
    sent = {"where": {"attrs.x": {op: "%muller%"}}}
    assert _call_refused("filter", sent).retry_mode == "corrected"
    assert _retry("filter", sent) == {"where": {"attrs.x": {"op": op, "value": "muller"}}}


@pytest.mark.parametrize("operand", ["%muller%", "%%muller", "muller%%%"])
def test_like_percent_at_the_ends_is_stripped_with_a_note(operand):
    for where in ({"attrs.author_name": {"op": "like", "value": operand}},
                  {"op": "like", "field": "attrs.author_name", "value": operand}):
        call = _rewritten("filter", {"where": where})
        assert call.arguments["predicate"] == {
            "op": "like", "field": "attrs.author_name", "value": "muller",
        }
        assert [record["rule"] for record in call.adjusted] == ["like_percent_stripped"]
    # In an argument the tool already had, too: a substring test means one
    # thing wherever it is written.
    expand = _rewritten("expand", {"input": "n:1", "filter": {
        "op": "like", "field": "attrs.a", "value": operand}})
    assert expand.arguments["filter"]["value"] == "muller"


def test_like_interior_percent_on_an_attribute_is_refused_with_no_call():
    """D2: two likes joined by and match more broadly (the order is lost),
    so no call is offered; the words say so."""
    refusal = _call_refused("filter", {"where": {"attrs.author_name": {
        "op": "like", "value": "mul%ler"}}})
    assert refusal.retry_arguments is None
    assert "% is not a wildcard" in refusal.message
    assert "`and`" in refusal.fix_hint and "order" in refusal.fix_hint


@pytest.mark.parametrize("sent,offered", [
    ("muller", "muller"),
    ("%reaper%", "reaper"),
    ("reaper pods", "re:/reaper pods/"),
    ("a.b%c", "re:/a\\.b.*c/"),
])
def test_like_on_text_is_refused_with_the_matches_call(sent, offered):
    """D2: like is not served on `text`; M7's matches is the call offered,
    a guess whose words say what matches reads differently."""
    refusal = _call_refused("filter", {"where": {"text": {"op": "like", "value": sent}}})
    assert refusal.retry_mode == "guess"
    assert "not accents" in refusal.fix_hint and "line feed" in refusal.fix_hint
    assert _retry("filter", {"where": {"text": {"op": "like", "value": sent}}}) == {
        "where": {"text": {"op": "matches", "value": offered}},
    }


def test_like_on_text_beside_a_type_is_refused_with_the_matches_call():
    """The matches call is offered when the condition also names a type."""
    sent = {"where": {"type": "doc", "text": {"op": "like", "value": "hub"}}}
    offered = {"where": {"type": "doc", "text": {"op": "matches", "value": "hub"}}}
    taken = rewrite_call("filter", offered)
    assert taken.adjusted == (), taken
    assert _retry("filter", sent) == offered


def test_a_text_clause_is_proven_servable_only_where_the_tool_serves_it():
    """The server offers a `matches` call on `text` only when the tool body
    would take it: on records, one text clause, access rules known off."""
    from okg.substrate.mcp.response_bounds import _proven_servable

    text = {"op": "matches", "field": "text", "value": "list"}
    typed = {"op": "and", "clauses": [{"op": "eq", "field": "subtype", "value": "doc"}, text]}
    nodes = {"input": {"kind": "nodes"}, "predicate": typed}
    assert _proven_servable("filter", nodes, ceiling=None, text_served=True) is True
    assert _proven_servable(
        "expand", {"input": {"node_id": "n:1"}, "filter": text}, ceiling=None, text_served=True,
    ) is True
    # Failure cases: access rules on or unknown, links, two text clauses,
    # a text clause under `or`, and a value grep's grammar refuses.
    assert _proven_servable("filter", nodes, ceiling=None) is False
    assert _proven_servable(
        "filter", {"input": {"kind": "edges"}, "predicate": text}, ceiling=None, text_served=True,
    ) is False
    assert _proven_servable(
        "filter", {"input": {"kind": "nodes", "filter": text}, "predicate": text},
        ceiling=None, text_served=True,
    ) is False
    # The tool body refuses these by name; so does the check (its caller
    # counts any refusal as not proven).
    from okg.substrate.mcp.text_match import TextConditionInvalid

    for predicate in (
        {"op": "or", "clauses": [{"op": "eq", "field": "subtype", "value": "doc"}, text]},
        {**text, "value": " "},
    ):
        with pytest.raises(TextConditionInvalid):
            _proven_servable(
                "filter", {"input": {"kind": "nodes"}, "predicate": predicate},
                ceiling=None, text_served=True,
            )


@pytest.mark.parametrize("operand", [
    "", "%", "%%", 3, None, ["a"], {"a": 1}, "a\x00b", "x" * 201, "́",
])
def test_like_operand_that_does_not_fit_is_refused(operand):
    refusal = _call_refused("filter", {"where": {"attrs.author_name": {
        "op": "like", "value": operand}}})
    assert refusal.retry_arguments is None


@pytest.mark.parametrize("field", ["attrs", "date.created", "text_hash"])
def test_like_on_the_whole_map_a_date_or_a_digest_is_refused_by_name(field):
    refusal = _call_refused("filter", {"where": {"op": "like", "field": field, "value": "a"}})
    assert "`like` on" in refusal.message
    assert refusal.retry_arguments is None


def test_ilike_is_served_as_like_with_a_note():
    """D3: ilike has one meaning here; the note says like also ignores accents."""
    for where in ({"attrs.x": {"op": "ilike", "value": "%gpu%"}},
                  {"op": "ilike", "field": "attrs.x", "value": "%gpu%"}):
        call = _rewritten("filter", {"where": where})
        assert call.arguments["predicate"] == {"op": "like", "field": "attrs.x", "value": "gpu"}
        assert [r["rule"] for r in call.adjusted] == ["ilike_as_like", "like_percent_stripped"]
        assert call.adjusted[0]["served"] == "like, which also ignores accents"


def test_search_where_refuses_like_with_its_own_contains_call():
    """search's `where` takes no like. On a field search's own `contains`
    reads text in, the refusal offers search's call with `contains` and the
    same text, so the ranking is kept (a guess: contains does not ignore
    accents); review of read stack PR 2, #3666. On any other field it still
    offers filter's call."""
    where = {"type": "ticket", "attrs.owner": {"op": "like", "value": "%muller%"}}
    refusal = _call_refused("search", {"query": "q", "where": where})
    assert refusal.retry_tool is None and refusal.retry_mode == "guess"
    assert refusal.retry_arguments == {"query": "q", "where": {
        "type": "ticket", "attrs.owner": {"op": "contains", "value": "muller"}}}
    assert "`contains` with a text value finds text inside" in refusal.message
    assert "filter" not in refusal.message
    assert rewrite_call("search", refusal.retry_arguments).adjusted == ()
    # Long form, inside `and`.
    long_form = {"op": "and", "clauses": [
        {"op": "eq", "field": "type", "value": "ticket"},
        {"op": "like_exact", "field": "title", "value": "Müller"},
    ]}
    refusal = _call_refused("search", {"query": "q", "where": long_form})
    assert refusal.retry_arguments["where"]["clauses"][1] == {
        "op": "contains", "field": "title", "value": "Müller"}
    # A field search's contains reads no text in: filter's call, as before.
    refusal = _call_refused("search", {"query": "q", "where": {
        "type": {"op": "like", "value": "tick"}}})
    assert refusal.retry_tool == "filter"
    # The predicate model's own check names search's contains too.
    with pytest.raises(Exception, match="`contains` with a text value finds text inside"):
        normalize_where({"op": "like", "field": "attrs.owner", "value": "x"})


def test_a_pattern_word_in_search_names_search_s_contains_not_like():
    """Review of read stack PR 2 (#3666): an operator word with no one
    meaning (`regex`) sent to search named `like`, which search's `where`
    refuses. Search's words name its own `contains`; filter's keep like."""
    where = {"attrs.title": {"op": "regex", "value": "foo"}}
    searched = _call_refused("search", {"query": "q", "where": where})
    assert "`contains` with a text value on attrs.<key>, path or title" in searched.fix_hint
    assert "`like`" not in searched.fix_hint
    filtered = _call_refused("filter", {"where": where})
    assert "for a substring of a field use `like`" in filtered.fix_hint


def test_a_live_set_refuses_like_by_name():
    for op in ("like", "like_exact"):
        with pytest.raises(
            live_result_sets.LiveResultPredicateUnsupported,
            match=f"does not support {op}: its rows are an outside system's",
        ):
            live_result_sets.check_predicate({"op": op, "field": "attrs.a", "value": "x"})


# --------------------------------------------------------------------------- #
# #3790: one answer per operator word                                          #
# --------------------------------------------------------------------------- #

#: An operand each operator word takes, by the word's meaning.
_WORD_OPERAND: dict[str, Any] = {
    "eq": 3, "lt": 3, "gt": 3, "in": [1, 2], "between": [1, 5], "exists": True,
    "prefix": "ab", "suffix": "ab", "like": "ab", "like_exact": "ab",
}
#: Words this test does not read: `contains` is containment on the whole
#: attribute map and `matches` tests `text`, so neither has a test on
#: `attrs.n` to compare.
_WORDS = [word for word in read_grammar.OPERATOR_WORDS if word not in ("contains", "matches")]
_CASE_VARIANTS = ["GTE", "$gte", "$prefix", "Equals", "ILIKE", "$NE"]


def _word_operand(word: str) -> Any:
    kind, meant = read_grammar.OPERATOR_WORDS.get(
        word.lstrip("$").lower(), ("none", None))
    return _WORD_OPERAND.get(meant or "", 3) if kind != "none" else True


def _outcome(where: dict) -> tuple[str, Any]:
    """("served", predicate) or ("refused", the offered call's predicate)."""
    try:
        call = rewrite_call("filter", {"where": where})
    except GrammarRefusal as refusal:
        retry = refusal.retry_arguments
        if retry is None:
            return "refused", None
        again = rewrite_call("filter", retry)
        assert again.adjusted == (), "a retry is served with no note"
        return "refused", again.arguments["predicate"]
    return "served", call.arguments["predicate"]


@pytest.mark.parametrize("word", _WORDS + _CASE_VARIANTS)
def test_each_operator_word_gives_one_answer_in_every_form(word):
    """#3790: the long form, the short form with `op` and the word written as
    a key read one table (`OPERATOR_WORDS`). The `op` forms give the same
    answer; written as a key every word is refused, and the call offered is
    served as the `op` forms are (or as their offer is)."""
    operand = _word_operand(word)
    key = "values" if isinstance(operand, list) else "value"
    plain = word.lstrip("$").lower()
    if plain == "exists":
        long_form: dict[str, Any] = {"op": word, "field": "attrs.n"}
        short_form: dict[str, Any] = {"attrs.n": {"op": word}}
    else:
        long_form = {"op": word, "field": "attrs.n", key: operand}
        short_form = {"attrs.n": {"op": word, key: operand}}
    as_long, as_short = _outcome(long_form), _outcome(short_form)
    assert as_long == as_short, (as_long, as_short)
    if as_long[0] == "served" and word not in read_grammar._LEAF_OPS:
        assert _rewritten("filter", {"where": long_form}).adjusted, "served with a note"
    as_key = _outcome({"attrs.n": {word: operand}})
    assert as_key[0] == "refused"
    assert as_key[1] == as_long[1], (as_key, as_long)


@pytest.mark.parametrize("word", ["gtee", "ltee", "likee", "prefx", "sufix", "betwen"])
def test_a_near_spelling_is_refused_as_a_guess_never_served_as_eq(word):
    """#3790: `{"attrs.n": {"gtee": 3}}` was served as eq on the object."""
    for where in ({"attrs.n": {word: 3}}, {"attrs.n": {"op": word, "value": 3}}):
        refusal = _call_refused("filter", {"where": where})
        assert refusal.retry_arguments is None or refusal.retry_mode == "guess"
        offered = refusal.retry_arguments
        assert offered != {"where": {"op": "eq", "field": "attrs.n", "value": {word: 3}}}
    gtee = _call_refused("filter", {"where": {"attrs.n": {"gtee": 3}}})
    assert gtee.retry_mode == "guess"
    assert rewrite_call("filter", gtee.retry_arguments).arguments["predicate"] == {
        "op": "or", "clauses": [
            {"op": "gt", "field": "attrs.n", "value": 3},
            {"op": "eq", "field": "attrs.n", "value": 3},
        ]}


def test_not_equal_is_offered_as_a_guess_that_says_what_it_keeps():
    refusal = _call_refused("filter", {"where": {"attrs.n": {"op": "ne", "value": 3}}})
    assert refusal.retry_mode == "guess"
    assert "keeps records without that field" in refusal.fix_hint
    assert refusal.retry_arguments == {"where": {"op": "not", "clause": {
        "attrs.n": {"op": "eq", "value": 3}}}}
