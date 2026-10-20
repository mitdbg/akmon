"""`search(method="grep")`'s grammar, regex subset and index rule (#3067).

PACT mcp-grep-3067. Pure Python: no database. The 27-pattern table is
`check_vs_pg_trgm` in the design's `evidence/bench_rx.json` (DESIGN.md
section 11): the static check's run for each pattern, and whether pg_trgm
narrowed it on the 201 MB corpus. The check must never accept a pattern
pg_trgm cannot narrow.
"""
from __future__ import annotations

import re

import pytest

from okg.substrate.mcp import grep_query as gq
from okg.substrate.mcp.grep_query import (
    MAX_REGEX_CHARS,
    REGEX_TERM_SYNTAX,
    GrepPatternTooShort,
    GrepQueryInvalid,
    GrepRegexUnindexable,
    assert_indexable_or_scoped,
    literal_run,
    parse_grep_query,
    read_regex_term,
)

#: (pattern, static run, pg_trgm narrowed it) from bench_rx.json.
PATTERN_TABLE = [
    (r"\d+", 0, False),
    (r".*", 0, False),
    (r"[a-z]{2}", 0, False),
    (r"abc", 3, True),
    (r"ab", 2, False),
    (r"a.c", 1, False),
    (r"foo|bar", 3, True),
    (r"foo|.*", 0, False),
    (r"(foo)+", 3, True),
    (r"(foo)?bar", 3, True),
    (r"(foo)*", 0, False),
    (r"generation\s+\d+", 10, True),
    (r"[Pp]ublish", 6, True),
    (r"publ[a-z]sh", 4, True),
    (r"okg\.mcp", 3, True),
    (r"pact_id", 4, True),
    (r"def\s+\w+", 3, True),
    (r"(?:select|insert)\s+into", 6, True),
    (r"[0-9]{14}", 0, False),
    (r"fo+o", 1, True),
    (r"x{3}", 1, True),
    (r"(ab|cd)ef", 2, True),
    (r"(abc|de)f", 2, True),
    (r"_[a-z]{3}_", 0, True),
    (r"\.py$", 2, True),
    (r"\w+_id", 2, True),
    (r"a{1,3}bc", 2, True),
]


def test_the_table_has_the_designs_27_patterns() -> None:
    assert len(PATTERN_TABLE) == 27


@pytest.mark.parametrize("pattern,run,narrowed", PATTERN_TABLE)
def test_the_static_run_matches_the_measured_table(pattern, run, narrowed) -> None:
    assert literal_run(pattern) == run


@pytest.mark.parametrize("pattern,run,narrowed", PATTERN_TABLE)
def test_an_accepted_pattern_is_one_pg_trgm_narrows(pattern, run, narrowed) -> None:
    """Failure case the rule exists for: accepting a full scan."""
    if literal_run(pattern) >= gq.MIN_INDEXABLE_RUN:
        assert narrowed


# ---------- the grammar ----------

def _kinds(query: str) -> list[tuple[str, str, bool]]:
    return [(t.kind, t.text, t.negated) for t in parse_grep_query(query).terms]


def test_words_phrases_near_exclusion_and_regex() -> None:
    assert _kinds('uuid "a  quoted\tphrase" -skip x NEAR/80 "y z" re:/user_id_\\d+/') == [
        ("word", "uuid", False),
        ("phrase", "a quoted phrase", False),
        ("word", "skip", True),
        ("near", "x NEAR/80 y z", False),
        ("regex", r"user_id_\d+", False),
    ]


def test_a_literal_is_escaped_and_matched_case_insensitively_as_a_substring() -> None:
    # The `.` between two letters is a separator and folds (#3666 R5); the
    # other metacharacters stay escaped literals.
    term = parse_grep_query("a.b(c)[d]").terms[0]
    assert term.pg_regex == r"a[-_. ]+b\(c\)\[d\]"
    assert re.search(term.pg_regex, "xA.B(C)[D]y", re.IGNORECASE)
    assert not re.search(term.pg_regex, "xAxB(C)[D]y", re.IGNORECASE)
    assert not parse_grep_query("(c)[d]").terms[0].folded


# ---------- #3666 R5: separators and case fold ----------

@pytest.mark.parametrize("text", [
    "copy keep_value now", "copy keep-value now", "copy keep value now",
    "copy KEEP.VALUE now", "copy Keep__Value now", "copy keep - value now",
])
def test_a_separator_inside_a_word_matches_any_separator_run(text) -> None:
    term = parse_grep_query("keep_value").terms[0]
    assert term.folded and term.pg_regex == "keep[-_. ]+value"
    assert re.search(term.pg_regex, text, re.IGNORECASE)


@pytest.mark.parametrize("query,text", [
    # Failure cases: folding never drops a separator, nor adds one at an edge.
    ("keep_value", "copy keepvalue now"),
    ("keep_value", "copy keep/value now"),
    ("_private", "a -private name"),
    ("trailing.", "trailing, then"),
])
def test_folding_needs_a_separator_and_leaves_word_edges_literal(query, text) -> None:
    term = parse_grep_query(query).terms[0]
    assert not re.search(term.pg_regex, text, re.IGNORECASE)


def test_edge_separators_stay_literal_and_unfolded_words_say_so() -> None:
    assert parse_grep_query("_private").terms[0].pg_regex == "_private"
    assert not parse_grep_query("_private").terms[0].folded
    assert parse_grep_query("trailing.").terms[0].pg_regex == r"trailing\."
    assert not parse_grep_query("plain").terms[0].folded


def test_a_phrase_folds_inside_its_words_and_keeps_its_whitespace_gap() -> None:
    term = parse_grep_query('"set keep_value now"').terms[0]
    assert term.folded
    assert re.search(term.pg_regex, "SET keep-value\n now", re.IGNORECASE)
    assert not re.search(term.pg_regex, "setkeep-value now", re.IGNORECASE)


def test_folding_keeps_the_literal_run_the_index_narrows_on() -> None:
    # `_` and `.` were never letters or digits, so the run is unchanged.
    assert parse_grep_query("keep_value").terms[0].run == 5
    assert parse_grep_query("ab_cd").terms[0].run == 2
    assert parse_grep_query("keep_value").indexable
    assert parse_grep_query("keep_value").rank_words == ("keep_value",)


def test_a_regex_and_a_path_term_are_never_folded() -> None:
    parsed = parse_grep_query("re:/keep_value/ path:a_b")
    assert parsed.terms[0].pg_regex == "keep_value" and not parsed.terms[0].folded
    assert parsed.path_terms[0].pg_regex == "a_b" and not parsed.path_terms[0].folded


def test_each_positive_term_says_how_it_was_spelled() -> None:
    from okg.substrate.mcp.grep_query import spelling_tried

    parsed = parse_grep_query("keep_value plain re:/x\\d+/ -skip")
    assert [spelling_tried(t) for t in parsed.positive] == [
        "keep_value: any case, and any run of _ - . or space where it has one "
        "between letters or digits",
        "plain: any case",
        r"re:/x\d+/ as written, any case",
    ]


def test_a_phrase_matches_any_whitespace_run_between_its_words() -> None:
    term = parse_grep_query('"foo bar"').terms[0]
    assert re.search(term.pg_regex, "foo \n\t bar")
    assert not re.search(term.pg_regex, "foobar")


def test_near_matches_both_orders_within_the_distance() -> None:
    pattern = parse_grep_query("alpha NEAR/5 omega").terms[0].pg_regex
    assert re.search(pattern, "alpha 123 omega")
    assert re.search(pattern, "omega..alpha")
    assert not re.search(pattern, "alpha 123456 omega")


def test_a_negated_regex_excludes() -> None:
    (word, regex) = parse_grep_query(r"thing -re:/\d{3}/").terms
    assert regex.negated and regex.kind == "regex"


def test_the_regex_syntax_lives_in_one_constant_and_one_reader() -> None:
    assert REGEX_TERM_SYNTAX == "re:/<pattern>/"
    assert read_regex_term("re:/a b/ c", 0) == ("a b", 8)
    assert read_regex_term("re:/a/b/ c", 0) == ("a/b", 8)
    assert read_regex_term(r"re:/a\/ b/", 0) == (r"a\/ b", 10)
    assert read_regex_term("word", 0) is None


def test_the_published_search_description_uses_the_constant() -> None:
    from okg.substrate.mcp import server

    source = server.__file__
    with open(source, encoding="utf-8") as handle:
        assert REGEX_TERM_SYNTAX in handle.read()


INVALID = [
    ('"open', "an unbalanced quote"),
    ('a"b', "a quote must start a phrase"),
    ("-only -exclusions", "exclusions only"),
    ("NEAR/5 b", "NEAR needs"),
    ("a NEAR/5", "NEAR needs"),
    ("a NEAR b", "NEAR needs"),
    ("a NEAR/300 b", "above 255"),
    ("a NEAR/5 re:/x+/", "words or quoted phrases"),
    ("-a NEAR/5 b", "cannot be negated"),
    ("a NEAR/5 b NEAR/5 c", "exactly two operands"),
    ("re:/abc", "unterminated"),
    ("re://", "empty regular expression"),
    (r"re:/(a)\1/", "back-references"),
    (r"re:/(?P<n>abc)(?P=n)/", "named groups"),
    ("re:/(?=abc)def/", "lookahead and lookbehind"),
    ("re:/(?<!abc)def/", "lookahead and lookbehind"),
    ("re:/(?i)abc/", "inline flags"),
    ("re:/(?i:abc)/", "inline flags"),
    (r"re:/\bword\b/", r"\b and \B"),
    (r"re:/\Bword/", r"\b and \B"),
    (r"re:/\mword/", "PostgreSQL-only"),
    (r"re:/\yword/", "PostgreSQL-only"),
    (r"re:/\x41bc/", r"\x escapes"),
    ("re:/(?>abc)/", "atomic groups"),
    ("re:/abc*+/", "possessive"),
    ("re:/a{300}bcd/", "above 255"),
    ("re:/abc(/", "invalid regular expression"),
    ("re:/" + "a" * (MAX_REGEX_CHARS + 1) + "/", "longer than 256"),
    ("x " * 17, "more than 16 terms"),
    ("", "empty"),
    # PACT mcp-grep-rank: the path term.
    ("abc path:", "needs a substring"),
    ('abc path:"open', "an unbalanced quote"),
    ("-path:tests", "exclusions only"),
    ("-abc -path:tests", "exclusions only"),
    ("path:src NEAR/5 b", "words or quoted phrases"),
]


@pytest.mark.parametrize("query,reason", INVALID)
def test_an_invalid_query_is_refused_with_the_reason(query, reason) -> None:
    with pytest.raises(GrepQueryInvalid) as raised:
        parse_grep_query(query)
    assert raised.value.code == "grep_query_invalid"
    assert reason in raised.value.data["reason"]
    assert REGEX_TERM_SYNTAX in raised.value.fix_hint


def test_a_256_character_pattern_is_accepted() -> None:
    assert parse_grep_query("re:/" + "a" * MAX_REGEX_CHARS + "/").terms[0].run == 256


# ---------- the index rule ----------

def test_an_unindexable_regex_is_refused_unscoped_and_runs_scoped() -> None:
    parsed = parse_grep_query(r"re:/\d{3}-\d{4}/")
    with pytest.raises(GrepRegexUnindexable) as raised:
        assert_indexable_or_scoped(parsed, scoped=False)
    assert raised.value.code == "grep_regex_unindexable"
    assert "JIRA-" in raised.value.fix_hint and "path_prefix" in raised.value.fix_hint
    assert_indexable_or_scoped(parsed, scoped=True)


def test_the_unindexable_hint_example_is_right_both_ways() -> None:
    """Issue #3666: the hint's example pattern is refused unscoped, and its
    rewrite is accepted. The old example (`user_\\d+`) holds the literal run
    `user`, so it was never refused."""
    with pytest.raises(GrepRegexUnindexable) as raised:
        assert_indexable_or_scoped(parse_grep_query(r"re:/\d{3}-\d{4}/"), scoped=False)
    example = re.search(r"for example (\S+) becomes (\S+)\)", raised.value.fix_hint)
    assert example is not None, raised.value.fix_hint
    before, after = example.groups()
    with pytest.raises(GrepRegexUnindexable):
        assert_indexable_or_scoped(parse_grep_query(f"re:/{before}/"), scoped=False)
    assert_indexable_or_scoped(parse_grep_query(f"re:/{after}/"), scoped=False)


def test_short_literals_alone_are_refused_unscoped() -> None:
    with pytest.raises(GrepPatternTooShort):
        assert_indexable_or_scoped(parse_grep_query("ab cd"), scoped=False)
    assert_indexable_or_scoped(parse_grep_query("ab cd"), scoped=True)


def test_one_indexable_positive_term_carries_the_query() -> None:
    assert_indexable_or_scoped(parse_grep_query(r"ab re:/\d+/ uuid"), scoped=False)


def test_a_negated_term_never_makes_a_query_indexable() -> None:
    with pytest.raises(GrepPatternTooShort):
        assert_indexable_or_scoped(parse_grep_query("ab -uuid"), scoped=False)


def test_near_is_indexable_when_either_operand_is() -> None:
    assert parse_grep_query("ab NEAR/9 uuid").indexable
    assert not parse_grep_query("ab NEAR/9 cd").indexable


# ---------- PACT grep-near-operands-3251: NEAR operands narrow the scan ----------

def _near_clause(query: str) -> tuple[str, dict]:
    from okg.substrate.mcp.grep_search import _term_sql

    params: dict = {}
    (clause,) = _term_sql(parse_grep_query(query), params, column="txt")
    return clause, params


def _sql_match(clause: str, params: dict, text: str) -> bool:
    """`clause` evaluated in Python: each `txt ~* %(k)s` as a case-folded search."""
    expression = re.sub(
        r"txt ~\* %\((\w+)\)s",
        lambda m: f"bool(re.search({params[m.group(1)]!r}, text, re.I))",
        clause,
    )
    return eval(expression.replace("AND", "and").replace("NOT", "not"))  # noqa: S307


def test_a_near_term_records_its_operand_regexes() -> None:
    term = parse_grep_query('alpha NEAR/80 "Beta  gamma"').terms[0]
    assert term.operands == ("alpha", r"Beta\s+gamma")
    # Exactly the operands inside the NEAR regex: same escaping, same phrase rule.
    for operand in term.operands:
        assert f"(?:{operand})" in term.pg_regex
    # `a.b` folds its separator (#3666 R5), in the operand as in the term.
    assert parse_grep_query("a.b NEAR/3 c(d)").terms[0].operands == (
        "a[-_. ]+b", r"c\(d\)",
    )
    assert parse_grep_query("a.b NEAR/3 c(d)").terms[0].folded
    # The same operand twice is ANDed once.
    assert parse_grep_query("kestrel NEAR/80 kestrel").terms[0].operands == ("kestrel",)


def test_only_near_terms_carry_operands() -> None:
    parsed = parse_grep_query('uuid "a phrase" -skip -"not this" re:/x\\d+/ path:src')
    assert all(t.operands == () for t in (*parsed.terms, *parsed.path_terms))


def test_the_near_clause_ands_both_operands_before_the_near_regex() -> None:
    clause, params = _near_clause("alpha NEAR/5 omega")
    assert clause == (
        "(txt ~* %(grep_t0o0)s AND txt ~* %(grep_t0o1)s AND txt ~* %(grep_t0)s)"
    )
    assert (params["grep_t0o0"], params["grep_t0o1"]) == ("alpha", "omega")


def test_a_negated_term_clause_is_unchanged() -> None:
    from okg.substrate.mcp.grep_search import _term_sql

    params: dict = {}
    clauses = _term_sql(parse_grep_query("alpha NEAR/5 omega -skip"), params, column="txt")
    assert clauses[1] == "NOT (txt ~* %(grep_t1)s)"
    assert sorted(params) == ["grep_t0", "grep_t0o0", "grep_t0o1", "grep_t1"]


@pytest.mark.parametrize("query,text,matches", [
    # Both operands present but farther apart than n: the operands pass,
    # the NEAR regex still refuses (failure case for a looser prefilter).
    ("alpha NEAR/5 omega", "alpha 123456 omega", False),
    ("alpha NEAR/5 omega", "alpha 123 omega", True),
    ("alpha NEAR/5 omega", "omega..alpha", True),
    ("alpha NEAR/5 omega", "alpha only", False),
    # Case folding is the NEAR regex's: case-insensitive on every part.
    ("Alpha NEAR/5 OMEGA", "aLPHA - Omega", True),
    # A phrase operand keeps its any-whitespace rule in the prefilter too.
    ('"beta gamma" NEAR/4 delta', "beta \n\t gamma, delta", True),
    ('"beta gamma" NEAR/4 delta', "betagamma, delta", False),
    ('"beta gamma" NEAR/4 delta', "beta gamma ...... delta", False),
    # Escaped metacharacters stay literal.
    ("a.b NEAR/2 c(d)", "a.b c(d)", True),
    ("a.b NEAR/2 c(d)", "axb c(d)", False),
])
def test_the_operand_prefilter_never_changes_a_near_result(query, text, matches) -> None:
    clause, params = _near_clause(query)
    near_only = bool(re.search(params["grep_t0"], text, re.I))
    assert near_only is matches
    assert _sql_match(clause, params, text) is matches


# ---------- PACT mcp-grep-rank: the path term and the rank words ----------

def test_a_path_term_is_parsed_apart_from_the_text_terms() -> None:
    parsed = parse_grep_query('uuid4 path:src/Pkg -path:"a b.py" re:/x\\d+/')
    assert [t.text for t in parsed.terms] == ["uuid4", r"x\d+"]
    assert [(t.kind, t.text, t.negated) for t in parsed.path_terms] == [
        ("path", "src/Pkg", False), ("path", "a b.py", True),
    ]
    # A path term is one escaped literal, matched case-insensitively.
    assert re.search(parsed.path_terms[1].pg_regex, "dir/A B.PY", re.I)
    assert not re.search(parsed.path_terms[1].pg_regex, "dir/aXb.py", re.I)
    assert [d["kind"] for d in parsed.disclosure()] == ["word", "regex", "path", "path"]
    # A quoted phrase "path:x" is text, not a path term.
    assert parse_grep_query('"path:x"').path_terms == ()


def test_a_path_term_is_a_scope_from_three_literal_characters() -> None:
    assert_indexable_or_scoped(parse_grep_query("ab path:pkg"), scoped=False)
    assert_indexable_or_scoped(parse_grep_query("path:pkg"), scoped=False)
    with pytest.raises(GrepPatternTooShort):
        assert_indexable_or_scoped(parse_grep_query("ab path:pk"), scoped=False)
    # A negated path term is never a scope.
    with pytest.raises(GrepPatternTooShort):
        assert_indexable_or_scoped(parse_grep_query("ab -path:pkg"), scoped=False)


def test_rank_words_are_the_positive_literal_words() -> None:
    parsed = parse_grep_query(
        'uuid4 "generation publish" alpha NEAR/9 "beta gamma" -skip '
        're:/regex\\d/ path:src uuid4'
    )
    assert parsed.rank_words == (
        "uuid4", "generation", "publish", "alpha", "beta", "gamma",
    )
    assert parse_grep_query("re:/abc\\d/ -word").rank_words == ()
    assert parse_grep_query("path:src").rank_words == ()


def test_an_alternation_of_plain_literals_ranks_on_its_words() -> None:
    """PACT mcp-grep-page: re:/a|b c/ orders by BM25 on its words."""
    from okg.substrate.mcp.grep_query import alternation_words

    assert alternation_words("rollback|roll back|revert") == ("rollback", "roll", "back", "revert")
    assert alternation_words("rollback") == ()
    assert alternation_words("roll.?back|revert") == ()
    assert alternation_words("(rollback|revert)") == ()
    assert alternation_words("rollback||revert") == ()
    assert parse_grep_query("re:/rollback|revert/ -foo").rank_words == ("rollback", "revert")
    assert parse_grep_query("-re:/rollback|revert/ word").rank_words == ("word",)


@pytest.mark.parametrize(("query", "regex", "quoted"), [
    ("rollback|revert", "re:/rollback|revert/", '"rollback|revert"'),
    ("deploy rollback|revert", "re:/rollback|revert/", '"rollback|revert"'),
    # An exclusion keeps its minus: dropping it would turn it into an include.
    ("x -rollback|revert", "-re:/rollback|revert/", '-"rollback|revert"'),
    ("a||b", "re:/a|b/", '"a||b"'),
    # Regex metacharacters are escaped, so the suggestion itself parses.
    ("c++|java", r"re:/c\+\+|java/", '"c++|java"'),
])
def test_a_bare_alternation_is_refused_with_both_forms(query, regex, quoted) -> None:
    """#3183: agents write `rollback|revert` meaning either word. Read as one
    plain word it matched only that literal text and came back empty with no
    hint; it is refused and names the regex and the quoted form."""
    with pytest.raises(GrepQueryInvalid) as refused:
        parse_grep_query(query)
    assert refused.value.data["reason"] == "bare_alternation"
    assert refused.value.fix_hint == (
        f"write {regex} to match any of its parts, or {quoted} to match the literal text"
    )
    for suggestion in (regex, quoted):
        parse_grep_query(f"keep {suggestion}")


@pytest.mark.parametrize(("query", "texts"), [
    ("re:/rollback|revert/", ["rollback|revert"]),
    ('"rollback|revert"', ["rollback|revert"]),
    ("cmd | grep", ["cmd", "|", "grep"]),
    ("a || b", ["a", "||", "b"]),
    ("x|", ["x|"]),
])
def test_a_pipe_that_is_not_a_bare_alternation_still_parses(query, texts) -> None:
    assert [term.text for term in parse_grep_query(query).positive] == texts
