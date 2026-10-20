"""Text matching as a condition, without a database (issue #3666, step M7).

The condition's grammar (`read_grammar`), its split from the rest of a
condition (`text_match.split_text_condition`), the regex rewrite that lets
PostgreSQL return whole matches, the refusals, and the text notice. The
database half is in `test_text_lines_db.py`.
"""
from __future__ import annotations

import re

import pytest

from okg.substrate.mcp.grep_query import GrepQueryInvalid
from okg.substrate.mcp.match_count_hint import (
    count_each_match_call,
    has_regex_term,
    render_count_each_match,
)
from okg.substrate.mcp.operator_models import PREDICATE_OPS, Predicate
from okg.substrate.mcp.read_grammar import (
    EDGE_FIELDS,
    NODE_FIELDS,
    GrammarRefusal,
    lower_condition,
    rewrite_call,
)
from okg.substrate.mcp.result_text import render
from okg.substrate.mcp.text_match import (
    SCAN_LIMIT,
    TextConditionInvalid,
    TextMatchScanLimit,
    holds_text_clause,
    narrowing_query,
    refuse_scan_limit,
    refuse_under_access_rules,
    split_text_condition,
    type_scope,
    whole_match_regex,
)

KEYS = "re:/CMS[A-Z]+-[0-9]+/"
TEXT = {"op": "matches", "field": "text", "value": KEYS}
TYPE = {"op": "eq", "field": "subtype", "value": "git_commit"}
OPEN = {"op": "eq", "field": "attrs.status", "value": "Open"}


# -- the grammar --------------------------------------------------------------


def test_matches_is_one_of_the_predicate_operators_and_the_model_takes_it():
    # loose-match-3666 appended like and like_exact after matches.
    assert PREDICATE_OPS[-3:] == ("matches", "like", "like_exact")
    assert Predicate.model_validate(TEXT).op == "matches"


def test_the_short_and_long_forms_lower_to_one_clause():
    short, adjusted = lower_condition(
        {"type": "git_commit", "text": {"op": "matches", "value": KEYS}},
        new_spelling=True,
    )
    assert short == {"op": "and", "clauses": [TYPE, TEXT]}
    assert adjusted == []
    long, adjusted = lower_condition(TEXT, new_spelling=True)
    assert long == TEXT and adjusted == []
    # `{"matches": value}` under `text` is the operator written as a key: on
    # `text` it can mean only the text condition, so it is served with a note
    # (#3666, lenient yes).
    for keyed in ("matches", "contains"):
        served, adjusted = lower_condition({"text": {keyed: KEYS}}, new_spelling=True)
        assert served == TEXT
        assert [note["rule"] for note in adjusted] == ["text_as_matches"]
        assert f"`{keyed}` written as a key of `text`" in adjusted[0]["sent"]
    # Failure case: an operator written as a key on any other field is
    # refused with the accepted form, and the call offered is served as written.
    with pytest.raises(GrammarRefusal) as refused:
        rewrite_call("filter", {"where": {"attrs.status": {"prefix": "Op"}}})
    assert refused.value.repairable
    assert "write the operator in `op`" in refused.value.fix_hint
    retry = refused.value.retry_arguments
    assert retry is not None
    assert rewrite_call("filter", retry).adjusted == ()


@pytest.mark.parametrize(("sent", "served"), [
    ("re:CMSSW_[0-9]+", "re:/CMSSW_[0-9]+/"),
    ("re:CMS[A-Z]+-[0-9]+ -re:follow.up", "re:/CMS[A-Z]+-[0-9]+/ -re:/follow.up/"),
    # Beside a quoted phrase or a slashed regex the term is still found:
    # terms are delimited as grep delimits them.
    ('"build failed" re:CMSSW_.*', '"build failed" re:/CMSSW_.*/'),
    ("re:/a|b/ re:CMSSW_.*", "re:/a|b/ re:/CMSSW_.*/"),
    ('re:CMSSW_.* "re:CMSSW_.* in a phrase"', 're:/CMSSW_.*/ "re:CMSSW_.* in a phrase"'),
    ("re:https://host/x.y", "re:/https://host/x.y/"),
    ("re:a.+b", "re:/a.+b/"),
])
def test_a_regex_written_without_slashes_is_served_as_the_regex_with_a_note(sent, served):
    """#3666: `re:<pattern>` with a regex metacharacter, read as a literal
    word, would match only text holding that pattern as written; it can
    mean only the regex, so it is served with its slashes and a note."""
    for where in ({"text": {"op": "matches", "value": sent}}, {"text": {"matches": sent}}):
        lowered, adjusted = lower_condition(where, new_spelling=True)
        assert lowered == {"op": "matches", "field": "text", "value": served}
        assert "regex_without_slashes" in [note["rule"] for note in adjusted]
    searched = rewrite_call("search", {"query": sent, "method": "grep"})
    assert searched.arguments["query"] == served
    assert [note["rule"] for note in searched.adjusted] == ["regex_without_slashes"]
    # Failure case: a non-grep search is left as sent, with no note.
    lexical = rewrite_call("search", {"query": sent, "method": "lexical"})
    assert lexical.arguments["query"] == sent and not lexical.adjusted


@pytest.mark.parametrize("kept", [
    "re:/CMSSW_[0-9]+/",
    '"re:CMSSW_.*" closed',
    # A word with no metacharacter is a valid literal search ("RE:Jenkins").
    "re:jenkins",
    "re:src/okg",
    # Only + $ ^: words a person writes, never a regex that matches nothing.
    "re:C++",
    "re:$100",
    "re:a+b",
])
def test_a_value_with_no_unslashed_regex_is_left_as_sent(kept):
    lowered, adjusted = lower_condition(
        {"text": {"op": "matches", "value": kept}}, new_spelling=True,
    )
    assert lowered["value"] == kept and adjusted == []
    searched = rewrite_call("search", {"query": kept, "method": "grep"})
    assert searched.arguments["query"] == kept and not searched.adjusted


@pytest.mark.parametrize(("sent", "fixed"), [
    ("re:foo/", "re:/foo/"),
    ("re:foo/i", "re:/foo/"),
    ("closed -re:CMSSW_.*/i", "closed -re:/CMSSW_.*/"),
])
def test_a_regex_missing_its_opening_slash_is_refused_with_the_call_to_send(sent, fixed):
    """#3666: `re:foo/` and `re:foo/i` are a regex with a slash missing (and
    grep's regex takes no flags): refused, naming the term to write, with a
    call served as written."""
    for tool, arguments, retry_key in (
        ("filter", {"where": {"text": {"op": "matches", "value": sent}}}, "where"),
        ("filter", {"where": {"text": sent}}, "where"),
        ("search", {"query": sent, "method": "grep"}, "query"),
    ):
        with pytest.raises(GrammarRefusal) as refused:
            rewrite_call(tool, arguments)
        assert f"`{sent.split()[-1]}`" in refused.value.message
        assert f"write {fixed.split()[-1]}" in refused.value.fix_hint
        retry = refused.value.retry_arguments
        assert retry is not None, (tool, arguments)
        if retry_key == "query":
            assert retry["query"] == fixed
        else:
            assert retry["where"] == {"text": {"op": "matches", "value": fixed}}
        assert rewrite_call(tool, retry).adjusted == ()


@pytest.mark.parametrize("sent", [
    "re:CMSSW_.* NEAR/3 failed",
    "failed NEAR/3 re:CMSSW_.*",
    "failed NEAR/3 re:foo/",
])
def test_an_unslashed_regex_beside_near_is_refused_naming_only_what_was_sent(sent):
    """NEAR's operands are words or phrases: an unslashed regex beside NEAR
    is refused, quoting the caller's term, never a regex they did not write."""
    term = next(word for word in sent.split() if word.startswith("re:"))
    for tool, arguments in (
        ("filter", {"where": {"text": {"op": "matches", "value": sent}}}),
        ("search", {"query": sent, "method": "grep"}),
    ):
        with pytest.raises(GrammarRefusal) as refused:
            rewrite_call(tool, arguments)
        assert f"`{term}` beside NEAR" in refused.value.message
        assert "re:/" not in refused.value.message
        assert refused.value.retry_arguments is None


def test_the_regex_note_cuts_a_long_value_with_an_ellipsis():
    sent = "re:CMSSW_.* " + " ".join(["word"] * 40)
    _, adjusted = lower_condition({"text": {"op": "matches", "value": sent}}, new_spelling=True)
    note = next(n for n in adjusted if n["rule"] == "regex_without_slashes")
    assert note["sent"].endswith("...")
    assert note["served"].split(" (a regex")[0].endswith("...")
    # Failure case: a short value is quoted whole, with no ellipsis.
    _, short = lower_condition(
        {"text": {"op": "matches", "value": "re:CMSSW_.*"}}, new_spelling=True,
    )
    assert not short[0]["sent"].endswith("...")


@pytest.mark.parametrize("sent", [
    {"text": "reaper pods"},
    {"op": "contains", "field": "text", "value": "reaper pods"},
    {"field": "text", "value": "reaper pods"},
])
def test_every_near_miss_text_form_is_read_one_way_as_words_with_a_note(sent):
    lowered, adjusted = lower_condition(sent, new_spelling=True)
    # Words, as `matches` reads them: both must occur, in any order.
    assert lowered == {"op": "matches", "field": "text", "value": "reaper pods"}
    assert [note["rule"] for note in adjusted] == ["text_as_matches"]
    assert adjusted[0]["served"] == (
        'text matches "reaper pods" (grep grammar: every word must occur)'
    )
    # Failure case: a value that is not a string has no reading and is left
    # for the operator's refusal.
    if "op" in sent:
        left = {**sent, "value": 3}
        assert lower_condition(left, new_spelling=True) == (left, [])


def test_text_is_a_field_of_records_only():
    assert NODE_FIELDS.known("text")
    assert not EDGE_FIELDS.known("text")
    with pytest.raises(GrammarRefusal) as refused:
        rewrite_call("filter", {
            "input": {"kind": "edges"}, "where": {"text": {"op": "matches", "value": "x"}},
        })
    assert "set of records" in refused.value.fix_hint
    with pytest.raises(GrammarRefusal) as refused:
        rewrite_call("search", {"query": "q", "where": {"text": "x"}})
    assert "search's query already matches words" in refused.value.fix_hint


def test_where_on_each_set_tool_carries_the_clause_to_the_tool():
    where = {"type": "git_commit", "text": {"op": "matches", "value": KEYS}}
    filtered = rewrite_call("filter", {"where": where}).arguments
    assert filtered == {"input": {"kind": "nodes", "subtype": "git_commit"}, "predicate": TEXT}
    mapped = rewrite_call("map", {"where": where, "select": ["node_id", "match"]}).arguments
    assert mapped["input"] == {"kind": "nodes", "subtype": "git_commit", "filter": TEXT}
    assert mapped["projection"] == ["node_id", "match"]
    counted = rewrite_call("aggregate", {"where": where, "by": ["match"], "metrics": ["count"]})
    assert counted.arguments["input"]["filter"] == TEXT
    expanded = rewrite_call("expand", {"input": {"node_id": "n"}, "where": {"text": "x"}})
    assert expanded.arguments["filter"] == {"op": "matches", "field": "text", "value": "x"}


def test_match_is_a_group_key_and_text_is_corrected_to_it():
    where = {"type": "git_commit", "text": {"op": "matches", "value": KEYS}}
    rewrite_call("aggregate", {"where": where, "by": ["match"], "metrics": ["count"]})
    # Failure case: `text` is matched, not grouped; the offered call groups
    # by what it matched.
    with pytest.raises(GrammarRefusal) as refused:
        rewrite_call("aggregate", {"where": where, "by": ["text"], "metrics": ["count"]})
    assert refused.value.retry_arguments["by"] == ["match"]


# -- the split ------------------------------------------------------------------


def test_the_text_clause_is_taken_out_of_the_top_level_and_through_nested_ands():
    assert split_text_condition(TEXT, tool="filter")[0] is None
    rest, condition = split_text_condition(
        {"op": "and", "clauses": [TYPE, {"op": "and", "clauses": [TEXT, OPEN]}]},
        tool="filter",
    )
    assert rest == {"op": "and", "clauses": [TYPE, OPEN]}
    assert condition is not None and condition.value == KEYS
    assert condition.parsed.indexable
    assert type_scope(rest) == ["git_commit"]
    # No text clause: the condition comes back as it was.
    assert split_text_condition(OPEN, tool="filter") == (OPEN, None)
    assert not holds_text_clause({"op": "contains", "field": "attrs", "value": {"field": "text"}})


@pytest.mark.parametrize("condition,words", [
    ({"op": "or", "clauses": [TEXT, OPEN]}, "not under or"),
    ({"op": "not", "clause": TEXT}, "not under or"),
    ({"op": "and", "clauses": [TEXT, {**TEXT, "value": "other"}]}, "one text condition"),
    ({"op": "eq", "field": "text", "value": "x"}, "takes the operator `matches`"),
    ({"op": "matches", "field": "attrs.summary", "value": "x"}, "tests the field `text`"),
    ({"op": "matches", "field": "text", "value": "  "}, "non-empty string"),
])
def test_a_text_clause_the_tools_cannot_read_is_refused_by_name(condition, words):
    with pytest.raises(TextConditionInvalid) as refused:
        split_text_condition(condition, tool="filter")
    assert words in refused.value.message
    assert refused.value.code == "invalid_operator_arguments"
    # Not a ValueError: an operator's `except ValueError` cannot swallow it.
    assert not isinstance(refused.value, ValueError)


def test_the_value_is_grep_grammar_and_a_path_term_is_refused():
    with pytest.raises(GrepQueryInvalid):
        split_text_condition({**TEXT, "value": "re:/a(?=b)/"}, tool="filter")
    with pytest.raises(GrepQueryInvalid) as refused:
        split_text_condition({**TEXT, "value": "path:lib reaper"}, tool="filter")
    assert "attrs.path" in refused.value.fix_hint
    _rest, unindexable = split_text_condition({**TEXT, "value": "re:/[A-Z]+-[0-9]+/"}, tool="map")
    assert unindexable is not None and not unindexable.parsed.indexable


# -- whole matches ----------------------------------------------------------------


@pytest.mark.parametrize("pattern,expected", [
    ("CMS[A-Z]+-[0-9]+", "CMS[A-Z]+-[0-9]+"),
    ("CMS(TRANSF|DM)-([0-9]+)", "CMS(?:TRANSF|DM)-(?:[0-9]+)"),
    ("(?:a|b)c", "(?:a|b)c"),
    (r"\(lit\)x", r"\(lit\)x"),
    ("[(]x[^]()]", "[(]x[^]()]"),
    # A POSIX class's `]` does not end the brackets: `(` after it stays literal.
    ("x[[:alpha:]()]", "x[[:alpha:]()]"),
    ("x[[.a.][=e=]()](y)", "x[[.a.][=e=]()](?:y)"),
])
def test_capturing_groups_become_non_capturing_and_nothing_else_changes(pattern, expected):
    rewritten = whole_match_regex(pattern)
    assert rewritten == expected
    # The rewrite matches exactly what the pattern matches.
    for text in ("CMSTRANSF-1177", "CMSDM-42", "(lit)x", "ac", "(x)", "zz"):
        assert bool(re.search(pattern, text)) == bool(re.search(rewritten, text))


# -- refusals and the notice ---------------------------------------------------------


def test_the_scan_limit_refusal_names_the_limit_and_each_part_over_it():
    refused = refuse_scan_limit(
        "filter", indexable=False, text_over=False, rest_over=True, rest_given=True,
    )
    assert isinstance(refused, TextMatchScanLimit)
    assert refused.code == "text_match_scan_limit"
    assert f"{SCAN_LIMIT:,}" in refused.message
    assert "no run of 3 letters or digits" in refused.message
    assert "the rest of the condition holds more than" in refused.message
    assert "put 3 literal letters or digits" in refused.fix_hint
    over = refuse_scan_limit(
        "aggregate", indexable=True, text_over=True, rest_over=False, rest_given=False,
    )
    assert "records hold the value's indexed term" in over.message
    assert "no run" not in over.message and "literal letters" not in over.fix_hint
    assert over.data == {
        "scan_limit": SCAN_LIMIT, "indexable": True,
        "text_over_limit": True, "rest_over_limit": False,
    }


def test_under_access_rules_the_condition_is_refused_plainly():
    refused = refuse_under_access_rules("map")
    assert refused.code == "unsupported_operator_form"
    assert "not available under access rules on this deployment" in refused.message


def test_the_text_notice_says_what_was_read_and_the_cap_only_with_matches():
    page = {
        "items": [{"node_id": "c:1", "match": "CMSDM-42", "match_count": 2}],
        "total_count": 1, "total_count_basis": "exact", "retrieval_exhaustive": True,
        "text_match": {"value": KEYS, "matches_cap": 10, "records_scanned": 3, "scan": "index"},
    }
    text = render(page, tool="filter")
    assert "! text condition: 3 records' text read" in text
    assert "matches lists" not in text
    assert "text_match" not in text, "the notice says the block whole"
    page["items"][0]["matches"] = [{"line": 1, "text": "CMSDM-42"}]
    assert "matches lists the first 10 per record, match_count all" in render(page, tool="filter")
    groups = {
        "groups": [{"match": "CMSDM-42", "count": 1, "occurrences": 1}],
        "total_count": 1, "total_count_basis": "exact", "retrieval_exhaustive": True,
        "text_match": {"value": KEYS, "matches_cap": 10, "records_scanned": 3,
                       "scan": "index", "records_counted": 2},
    }
    counted = render(groups, tool="aggregate")
    assert "every match in 2 records counted" in counted
    assert "groups keep letter case though matching ignores it" in counted
    assert "a spot two terms match counts once per term" in counted
    assert "hold no matched text" not in counted
    groups["text_match"]["records_without_match"] = 1
    assert "1 of the records hold no matched text" in render(groups, tool="aggregate")


@pytest.mark.parametrize("value,narrows_with", [
    # A regex narrows on the literal run every match holds, never as written.
    ("re:/CMS[A-Z]+-[0-9]+/", "CMS"),
    ('"restarted after"', "restarted"),
    ("reaper pods", "reaper"),
    ("re:/judge$/", "judge"),
    ("re:/(release)-[0-9]+/", "release"),
    # Forms that can match a line feed narrow on their literal run only.
    (r"re:/reaper\u000apods/", "reaper"),
    (r"re:/reaper[\t-\r]pods/", "reaper"),
    ("re:/reaper.*pods/", "reaper"),
])
def test_the_index_narrows_only_with_a_term_one_text_holds_whole(value, narrows_with):
    _rest, condition = split_text_condition({**TEXT, "value": value}, tool="filter")
    narrowing = narrowing_query(condition.parsed)
    assert narrowing is not None and narrowing.terms[0].pg_regex == narrows_with


@pytest.mark.parametrize("value", [
    "re:/[A-Z]+-[0-9]+/", "re:/ab|cd|ef/", "re:/(rule|judge)s/",
    # POSIX bracket expressions read differently in Python and PostgreSQL.
    "re:/reaper[[:space:]]pods/", "re:/reaper[[:cntrl:]]pods/",
])
def test_a_regex_with_no_run_every_match_holds_does_not_narrow(value):
    # Failure case: no literal of 3 letters or digits is in every match,
    # so the index cannot find candidates by it.
    _rest, condition = split_text_condition({**TEXT, "value": value}, tool="filter")
    assert narrowing_query(condition.parsed) is None


# -- counting each distinct match (Format v10 item C) ---------------------------
#
# Agents asked "which key appears on the most pages" ran a grep with the key's
# regex, got a cut page of passages, and checked guessed keys one at a time.
# One aggregate call answers it. A cut grep page with a regex names that call,
# built from the caller's own condition; plain words never get it.

KEY_RE = "re:/ABC-[0-9]+/"
MEETING_WHERE = {"op": "and", "clauses": [
    {"op": "eq", "field": "type", "value": "Meeting"},
    {"op": "prefix", "field": "attrs.title", "value": "Weekly"},
]}


@pytest.mark.parametrize("query,expected", [
    (KEY_RE, True),
    (f"weekly {KEY_RE}", True),
    ("ABC", False),
    ("ABC-12", False),
    ('"weekly sync"', False),
    (f"-{KEY_RE} weekly", False),
    # A path term is refused by a text condition: no call is offered.
    (f"{KEY_RE} path:notes", False),
    ("", False),
    (None, False),
])
def test_only_a_query_with_a_positive_regex_gets_the_count_call(query, expected):
    assert has_regex_term(query) is expected


def test_the_count_call_is_built_from_the_callers_condition_and_is_served_as_written():
    call = count_each_match_call(KEY_RE, MEETING_WHERE, metadata={})
    assert call == {"tool": "aggregate", "arguments": {
        "where": {"type": "Meeting", "attrs.title": {"op": "prefix", "value": "Weekly"},
                  "text": {"op": "matches", "value": KEY_RE}},
        "by": ["match"], "metrics": ["count"],
    }}
    rewritten = rewrite_call("aggregate", call["arguments"])
    assert rewritten.adjusted == ()
    assert rewritten.arguments["input"]["subtype"] == "Meeting"
    assert render_count_each_match(call) == (
        'count each distinct match: aggregate(where={"type":"Meeting","attrs.title":'
        '{"op":"prefix","value":"Weekly"},"text":{"op":"matches","value":"re:/ABC-[0-9]+/"}}, '
        'by=["match"], metrics=["count"])'
    )


def test_search_only_fields_are_translated_through_the_catalog_or_named_as_left_out():
    where = {"op": "and", "clauses": [
        {"op": "in", "field": "type", "values": ["Meeting", "Note"]},
        {"op": "prefix", "field": "title", "value": "Weekly"},
        {"op": "eq", "field": "source", "value": "notes.repo"},
        {"op": "eq", "field": "id", "value": "m:1"},
    ]}
    declared = {"Meeting": {"title_from": "name"}, "Note": {"title_from": "name"}}
    call = count_each_match_call(KEY_RE, where, metadata=declared)
    assert call["arguments"]["where"] == {
        "type": ["Meeting", "Note"], "attrs.name": {"op": "prefix", "value": "Weekly"},
        "node_id": "m:1", "text": {"op": "matches", "value": KEY_RE},
    }
    assert call["read_as"] == {"title": "attrs.name"}
    assert call["dropped"] == ['{"source":"notes.repo"}']
    assert rewrite_call("aggregate", call["arguments"]).adjusted == ()
    assert render_count_each_match(call).endswith(
        "; title read as attrs.name; NOT the grep's set, it may count more records: "
        'aggregate cannot take {"source":"notes.repo"}'
    )
    # The types read title from different attributes: written per type,
    # never one attribute guessed for both.
    split = {"Meeting": {"title_from": "name"}, "Note": {"title_from": "heading"}}
    call = count_each_match_call(KEY_RE, where, metadata=split)
    assert call["read_as"] == {"title": "attrs.heading (Note), attrs.name (Meeting)"}
    clauses = call["arguments"]["where"]["clauses"]
    assert {"op": "or", "clauses": [
        {"op": "and", "clauses": [
            {"op": "eq", "field": "type", "value": "Note"},
            {"op": "prefix", "field": "attrs.heading", "value": "Weekly"}]},
        {"op": "and", "clauses": [
            {"op": "eq", "field": "type", "value": "Meeting"},
            {"op": "prefix", "field": "attrs.name", "value": "Weekly"}]},
    ]} in clauses
    assert rewrite_call("aggregate", call["arguments"]).adjusted == ()
    # Failure case: a caller's narrowing that carries over not at all gets no
    # call; a call over every record would count a different set.
    either = {"op": "or", "clauses": [
        {"op": "eq", "field": "source", "value": "a"},
        {"op": "eq", "field": "attrs.status", "value": "open"},
    ]}
    assert count_each_match_call(
        KEY_RE, either, metadata={}, path_prefix="notes/", record_scoped=True,
    ) is None
    # With no narrowing at all, the call counts the profile's record types.
    call = count_each_match_call(KEY_RE, None, metadata={}, scoped=["Meeting"])
    assert call["arguments"]["where"] == {
        "type": "Meeting", "text": {"op": "matches", "value": KEY_RE},
    }


#: The held-out shape (Format v10 item C, lead replay 2026-10-17): pages
#: whose text is in passages, a `source` the grep narrowed by, and a title
#: prefix; no type named, so the grep read the search profile's types.
PAGE_CATALOG = {
    "Page": {"title_from": "title",
             "passage_rollup": {"edge_type": "contains", "child_subtype": "PageChunk"}},
    "PageChunk": {"title_from": "heading_path"},
    "Ticket": {"title_from": "summary"},
    "Board": {},
}
SCOPED_WHERE = {"op": "and", "clauses": [
    {"op": "eq", "field": "source", "value": "docs.notes"},
    {"op": "prefix", "field": "title", "value": "Meeting"},
]}


def test_a_source_and_title_scoped_grep_keeps_its_scope_or_says_what_is_missing():
    """Before: the call was printed over every record, with source and title
    both left out, and its top group was another key. After: the passage type
    folds into the page type that holds it, title is read per type, and the
    one part aggregate cannot take (source) is named with which way the
    count moves."""
    call = count_each_match_call(
        KEY_RE, SCOPED_WHERE, metadata=PAGE_CATALOG,
        scoped=["Page", "PageChunk", "Ticket", "Board"],
    )
    assert call is not None
    where = call["arguments"]["where"]
    assert where["op"] == "and"
    clauses = where["clauses"]
    # The judged record types: the chunk counts through its page, never twice.
    assert clauses[0] == {"op": "in", "field": "type", "values": ["Board", "Page", "Ticket"]}
    # Title per declared type; Board declares none and is left out of it.
    assert clauses[1] == {"op": "or", "clauses": [
        {"op": "and", "clauses": [
            {"op": "eq", "field": "type", "value": "Ticket"},
            {"op": "prefix", "field": "attrs.summary", "value": "Meeting"}]},
        {"op": "and", "clauses": [
            {"op": "eq", "field": "type", "value": "Page"},
            {"op": "prefix", "field": "attrs.title", "value": "Meeting"}]},
    ]}
    assert clauses[2] == {"op": "matches", "field": "text", "value": KEY_RE}
    assert call["dropped"] == ['{"source":"docs.notes"}']
    assert call["types_left_out"] == {"title": ["Board"]}
    assert rewrite_call("aggregate", call["arguments"]).adjusted == ()
    line = render_count_each_match(call)
    assert "NOT the grep's set, it may count more records: aggregate cannot take " \
        '{"source":"docs.notes"}' in line
    assert "it may count fewer records: Board declare no title and are left out" in line
    # Failure case: the call never counts over every record with the scope
    # gone, which is what was printed before.
    assert where != {"text": {"op": "matches", "value": KEY_RE}}
    # One page type, as the replay's graph: the plain short form.
    call = count_each_match_call(
        KEY_RE, SCOPED_WHERE, metadata=PAGE_CATALOG, scoped=["PageChunk"],
    )
    assert call["arguments"]["where"] == {
        "type": "Page", "attrs.title": {"op": "prefix", "value": "Meeting"},
        "text": {"op": "matches", "value": KEY_RE},
    }
    assert call["read_as"] == {"title": "attrs.title"}
    # Only the source narrowed the grep: nothing of it carries, no call.
    source_only = {"op": "eq", "field": "source", "value": "docs.notes"}
    assert count_each_match_call(
        KEY_RE, source_only, metadata=PAGE_CATALOG, scoped=["PageChunk"],
    ) is None


def _grep_page(**extra):
    return {
        "operator_name": "search", "method": "grep", "status": "ok", "offset": 0, "query": KEY_RE,
        "results": [{"node_id": f"m:{i}", "subtype": "Meeting", "excerpt": "ABC-12"}
                    for i in range(3)],
        "count_each_match": count_each_match_call(KEY_RE, MEETING_WHERE, metadata={}),
        **extra,
    }


def test_a_cut_grep_page_prints_the_count_call_once():
    text = render(_grep_page(total_count=145, next_offset=3), tool="search")
    lines = [line for line in text.split("\n") if line.startswith("count each distinct match:")]
    assert len(lines) == 1 and 'by=["match"]' in lines[0], text
    # Failure cases: a page that is the whole answer, and a page of another
    # method, never print it.
    whole = render(_grep_page(total_count=3), tool="search")
    assert "count each distinct match" not in whole
    lexical = render(_grep_page(method="lexical", total_count=145, next_offset=3),
                     tool="search")
    assert "count each distinct match" not in lexical


def test_one_group_over_plain_words_says_a_regex_groups_each_match():
    groups = {
        "groups": [{"match": "ABC", "count": 4, "occurrences": 9}],
        "total_count": 1, "total_count_basis": "exact", "retrieval_exhaustive": True,
        "text_match": {"value": "ABC", "matches_cap": 10, "records_scanned": 4,
                       "scan": "index", "records_counted": 4,
                       "one_group_plain_words": True},
    }
    notice = "! plain words match only themselves, so one group; a regex (re:/<regex>/)"
    assert notice in render(groups, tool="aggregate")
    # Failure case: a regex count, or several groups, has no such notice.
    del groups["text_match"]["one_group_plain_words"]
    assert "plain words match only themselves" not in render(groups, tool="aggregate")


def _rules_server(monkeypatch, state):
    """A real server object whose access-rule mode reads as `state` (True:
    audit/enforce on; False: off; an exception: unreadable)."""
    from okg.substrate.mcp.response_bounds import ByteBoundedFastMCP
    from okg.substrate.nomos import runtime_enforcement

    def mode(deployment_name: str) -> bool:
        if isinstance(state, Exception):
            raise state
        return state

    monkeypatch.setattr(runtime_enforcement, "deployment_requires_nomos_runtime", mode)
    server = ByteBoundedFastMCP("count-each-match-probe")
    server._okg_read_deployment_name = "probe"  # type: ignore[attr-defined]
    server._okg_served_tools = ("aggregate", "inspect", "search")  # type: ignore[attr-defined]
    return server


def test_the_count_call_is_never_offered_where_aggregate_is_not_served(monkeypatch):
    """Review F1: a tool profile without aggregate (inspect+search) removes
    the tool; the cut grep page must not name a call the server lacks."""
    from okg.substrate.mcp.match_count_hint import count_call_offerable

    server = _rules_server(monkeypatch, False)
    assert count_call_offerable(server) is True
    # Failure cases: aggregate not served, and a server with no served list.
    server._okg_served_tools = ("inspect", "search")  # type: ignore[attr-defined]
    assert count_call_offerable(server) is False
    del server._okg_served_tools
    assert count_call_offerable(server) is False


def test_the_count_call_is_never_offered_where_access_rules_refuse_aggregate(monkeypatch):
    """Review P1-1: under audit/enforce aggregate refuses every count and the
    text condition is refused, so the cut grep page names no call there."""
    from okg.substrate.mcp.match_count_hint import count_call_offerable

    assert count_call_offerable(_rules_server(monkeypatch, False)) is True
    # Failure cases: rules on, a mode that cannot be read, and a server
    # without the check all offer nothing.
    assert count_call_offerable(_rules_server(monkeypatch, True)) is False
    assert count_call_offerable(_rules_server(monkeypatch, RuntimeError("unreadable"))) is False
    assert count_call_offerable(object()) is False


def test_a_regex_without_an_indexable_literal_says_the_count_may_be_refused_for_scan_size():
    """Review P1-1: a regex the trigram index cannot narrow is counted only
    over a set of at most SCAN_LIMIT records; the line says so."""
    bare = "re:/[A-Z]+-[0-9]+/"
    call = count_each_match_call(bare, MEETING_WHERE, metadata={})
    assert call["scan_bound"] is True
    assert render_count_each_match(call).endswith(
        "; it may be refused for scan size: the regex holds no run of 3 or more "
        "letters or digits for the index, so the records the rest of the condition "
        f"names must number at most {SCAN_LIMIT:,}"
    )
    # Failure case: a regex with a literal run carries no such warning.
    indexed = count_each_match_call(KEY_RE, MEETING_WHERE, metadata={})
    assert "scan_bound" not in indexed
    assert "scan size" not in render_count_each_match(indexed)
