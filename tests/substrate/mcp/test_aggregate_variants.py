"""aggregate's merged match keys without a database (PACT read-aggregate-variants-3666).

The keys and their SQL, the capture-group rule, the literals an index read
tries, the grammar and period parser letting the keys through, and the
reply's words. The database half is `test_aggregate_variants_db.py`.
"""
from __future__ import annotations

import pytest

from okg.substrate.mcp import match_keys as mk
from okg.substrate.mcp.aggregate_periods import parse_period_field
from okg.substrate.mcp.grep_query import parse_grep_query
from okg.substrate.mcp.read_grammar import GrammarRefusal, rewrite_call
from okg.substrate.mcp.result_text import render
from okg.substrate.mcp.text_match import (
    NARROWING_TERMS,
    TextConditionInvalid,
    capture_terms,
    capturing_groups,
    narrowing_queries,
    narrowing_query,
)

WHERE = {"type": "ticket", "text": {"op": "matches", "value": "re:/team: ([A-Za-z ]+)/"}}


@pytest.mark.parametrize("pattern,groups", [
    ("team: ([A-Za-z ]+)", 1),
    ("(?:Owner|Owning) team: ([A-Za-z ]+)", 1),
    ("(Owner|Owning) team: ([A-Za-z ]+)", 2),
    (r"\(lit\) [(]x[)]", 0),
    ("x[[:alpha:]()]", 0),
])
def test_capturing_groups_count_only_capturing_parentheses(pattern, groups):
    assert capturing_groups(pattern) == groups


def test_one_capture_group_is_counted_and_two_are_refused_by_name():
    assert capture_terms(parse_grep_query("postmortem re:/team: ([A-Za-z ]+)/")) == (1,)
    assert capture_terms(parse_grep_query("re:/team: [A-Za-z ]+/ postmortem")) is None
    # Failure case: two groups; which one is the value cannot be told.
    with pytest.raises(TextConditionInvalid) as refused:
        capture_terms(parse_grep_query("re:/(Owner|Owning) team: ([A-Za-z ]+)/"))
    assert refused.value.data["reason"] == "several_capture_groups"
    assert "(?:...)" in refused.value.fix_hint


def test_the_key_sql_and_an_unknown_key():
    assert mk.key_sql("match", "m.txt") == "m.txt"
    assert "lower(m.txt)" in mk.key_sql("norm(match)", "m.txt")
    assert "ORDER BY w" in mk.key_sql("words(match)", "m.txt")
    with pytest.raises(ValueError):
        mk.key_sql("lower(match)", "m.txt")


def test_variants_and_groups_that_may_be_one():
    assert mk.variants_text([("Eng Platform", 9), ("Platform  Eng\n", 2)]) == (
        "Eng Platform (9); Platform Eng (2)"
    )
    many = [(f"v{i}", 1) for i in range(6)]
    assert mk.variants_text(many).endswith("+2 more")
    assert mk.may_be_one(["eng platform", "platform", "sre", "eng sre"]) == [
        ("eng platform", "platform"), ("eng sre", "sre"),
    ]
    # Failure case: groups sharing a word but neither holding the other.
    assert mk.may_be_one(["eng platform", "platform sre"]) == []


def test_the_index_read_tries_the_longest_literals_first():
    parsed = parse_grep_query('"Owning team" Platform')
    tried = [q.positive[0].text for q in narrowing_queries(parsed)]
    assert tried[0] == narrowing_query(parsed).positive[0].text == "Platform"
    assert set(tried) == {"Platform", "Owning", "team"}
    assert len(narrowing_queries(parse_grep_query("alpha beta gamma delta"))) == NARROWING_TERMS
    assert narrowing_queries(parse_grep_query("re:/[A-Z]+-[0-9]+/")) == []


@pytest.mark.parametrize("key", ["match", "norm(match)", "words(match)"])
def test_the_grammar_and_the_period_parser_let_the_match_keys_through(key):
    assert parse_period_field(key) is None
    rewrite_call("aggregate", {"where": WHERE, "by": [key], "metrics": ["count"]})


def test_another_call_shaped_key_is_still_judged_as_a_period():
    with pytest.raises(Exception):
        parse_period_field("lower(match)")
    with pytest.raises(GrammarRefusal):
        rewrite_call("aggregate", {"where": WHERE, "by": ["text"], "metrics": ["count"]})


def _page(**facts):
    return {
        "groups": [
            {"match": "eng platform", "count": 2, "occurrences": 7,
             "variants": "Eng Platform (4); Platform Eng (3)"},
            {"match": "platform", "count": 1, "occurrences": 6,
             "variants": "Platform (6)"},
        ],
        "by": ["words(match)"], "metrics": ["count"], "total_count": 2,
        "total_count_basis": "exact", "retrieval_exhaustive": True,
        "truncated": False, "limit": 50, "offset": 0, "next_offset": None,
        "text_match": {
            "value": "re:/team: ([A-Za-z ]+)/", "matches_cap": 10,
            "records_scanned": 3, "scan": "index", "records_counted": 3,
            "records_without_match": 0, **facts,
        },
    }


def test_the_reply_says_what_was_merged_and_which_groups_may_be_one():
    text = render(_page(
        capture_group=True, merged=mk.merged_label("words(match)"),
        repeated_passages=2, may_be_one=[["eng platform", "platform"]],
    ), tool="aggregate")
    assert "the capture group's text in 3 records counted" in text
    assert "groups merge letter case, spacing, punctuation and word order" in text
    assert "only the capture group is counted, other terms choose records" in text
    assert "2 passages repeating their record's own text counted once" in text
    assert '! may be one value: "eng platform" holds "platform"' in text
    # One table row per group, the variants cell as written.
    assert "eng platform\t2\t7\tEng Platform (4); Platform Eng (3)" in text
    # Failure case: without those facts the reply says what it said before.
    plain = render(_page(), tool="aggregate")
    assert "every match in 3 records counted; groups keep letter case" in plain
    assert "may be one value" not in plain and "capture group" not in plain
