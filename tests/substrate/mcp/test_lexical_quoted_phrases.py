"""Lexical search honors quoted phrases (PACT lexical-quoted-phrases-3064, #3064).

Two halves. The parser tests need no database: `quoted_spans` must agree
with grep's phrase rule (`parse_grep_query`) on every query grep accepts,
and read unbalanced, typographic, empty, nested and negated quotes without
refusing. The PostgreSQL tests run the real search on published fixture
graphs: an exact phrase leads, a passage match marks its record, masked text
never matches, the filtered fold promotes only inside its exact prefix,
`phrase_match` survives a narrowed `select`, paging stays consistent, an
unquoted search runs none of this code, and a quoted search with no exact
match says so with a grep call that runs.
"""
from __future__ import annotations

import importlib
import json
import re

import psycopg
import pytest

from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.grep_query import (
    parse_grep_query,
    phrase_pattern,
    quoted_spans,
    strip_quotes,
)
from okg.substrate.mcp.result_text import render
from tests.substrate.mcp.test_search_records import (
    MAIN,
    MASKED,
    OPEN,
    QUERY,
    READER,
    _activate,
    _allow,
    _basic_facts,
    _by_id,
    _edge,
    _ids,
    _node,
    _publish,
)



# ---------- the span parser (database-free) ----------

#: Queries grep accepts: the spans lexical search reads must be grep's
#: positive phrase terms, in order, without repeats.
_GREP_ACCEPTED = [
    'zirconium',
    '"zirconium cladding"',
    'weld "zirconium cladding" report',
    '"a  b\tc" "d e"',
    '"x y" -"z w" plain',
    '-"only excluded" kept',
    '"repeat me" "repeat me" other',
    '"one" two "three four five"',
    '"a"-"b"',
    'word -word2 "phrase here"',
]


@pytest.mark.parametrize("query", _GREP_ACCEPTED)
def test_spans_agree_with_grep_on_every_query_grep_accepts(query: str) -> None:
    parsed = parse_grep_query(query)
    grep_spans = tuple(dict.fromkeys(
        term.text for term in parsed.positive if term.kind == "phrase"
    ))
    assert quoted_spans(query) == grep_spans


_TEXTS = [
    "The zirconium cladding failed.",
    "zirconium\n  CLADDING sample",
    "cladding zirconium reversed",
    "zirconium-cladding hyphen",
    "a  b\tc and d e",
    "x.y literal dot",
    "xzy no dot",
]


@pytest.mark.parametrize("query", [
    '"zirconium cladding"', '"a b c"', '"d e"', '"x.y"', '"zirconium"',
])
def test_the_phrase_match_is_greps_match(query: str) -> None:
    (term,) = parse_grep_query(query).positive
    (span,) = quoted_spans(query)
    pattern = phrase_pattern(span.split())
    for text in _TEXTS:
        assert bool(pattern.search(text)) == bool(
            re.search(term.pg_regex, text, re.IGNORECASE)
        ), (query, text)


@pytest.mark.parametrize(("query", "spans"), [
    # Unbalanced: the rest is literal, never a refusal.
    ('"zirconium cladding', ()),
    ('"a b" "unclosed c', ("a b",)),
    # Typographic quotes read as straight ones.
    ("“zirconium   cladding” weld", ("zirconium cladding",)),
    ("“mixed close\" end", ("mixed close",)),
    ("„low open” end", ("low open",)),
    # An empty pair is skipped.
    ('"" "  " word', ()),
    # Nested: a quote closes the open span, so `a ` and ` c` are the spans.
    ('"a "b" c"', ("a", "c")),
    # A negated span is not a requirement; a `-` inside a word is not one.
    ('-"x y" "z"', ("z",)),
    ('pre-"x y"', ("x y",)),
    # Right after a closing quote the `-` starts a term, as grep reads it.
    ('"a"-"b"', ("a",)),
    # No quote at all.
    ("zirconium cladding", ()),
])
def test_spans_never_refuse_odd_quotes(query: str, spans: tuple[str, ...]) -> None:
    assert quoted_spans(query) == spans


def test_the_gap_between_words_is_ascii_whitespace_only() -> None:
    """Grep's SQL pattern reads `\\s` as ASCII whitespace; a no-break or an
    ideographic space joins words in neither."""
    pattern = phrase_pattern(["zirconium", "cladding"])
    assert pattern.search("zirconium \t\n cladding")
    assert not pattern.search("zirconium\u00a0cladding")
    assert not pattern.search("zirconium\u3000cladding")


def test_strip_quotes_replaces_every_quote_with_a_space() -> None:
    assert strip_quotes('a "b c" “d”') == "a  b c   d "
    assert '"' not in strip_quotes('"x" "y')


def test_an_empty_phrase_pattern_is_refused() -> None:
    """Failure case: the helper is never asked for an empty phrase; if it
    were, it must not compile a pattern that matches every text."""
    with pytest.raises(ValueError):
        phrase_pattern([])


# ---------- PostgreSQL: the real search ----------

PHRASE = f'"{QUERY}"'


def _session_search(dsn: str, deployment: str, query: str, **kwargs) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(
            conn, deployment_name=deployment,
            **({"nomos_subject": kwargs.pop("subject")} if "subject" in kwargs else {}),
        )
        return search(conn, session, query, **kwargs)


def _lexical(dsn: str, deployment: str, query: str = PHRASE, **kwargs) -> dict:
    return _session_search(dsn, deployment, query, method="lexical", **kwargs)


def _phrase_facts() -> list:
    """Scattered words outrank the exact phrase on BM25 alone."""
    return [
        *_basic_facts(),
        _node("note:scatter", "note", "cladding cladding zirconium zirconium."),
        _node("note:exact", "note",
              f"An inspection found the {QUERY} intact after a long service life "
              "in the reactor core, as the annual review recorded."),
    ]


@pytest.mark.postgres
@pytest.mark.parametrize("rank_by", ["self", "records"])
def test_an_exact_phrase_outranks_scattered_words(clean_runtime, tmp_path, rank_by):
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_phrase_facts())
    plain = _ids(_lexical(clean_runtime, deployment, QUERY, limit=50, rank_by=rank_by))
    # Precondition: BM25 alone puts the scattered note first.
    assert plain.index("note:scatter") < plain.index("note:exact")
    page = _lexical(clean_runtime, deployment, limit=50, rank_by=rank_by,
                    completeness="exact")
    ids = _ids(page)
    assert ids.index("note:exact") < ids.index("note:scatter")
    flags = [row["phrase_match"] for row in page["results"]]
    # Every match first, then every non-match: one boundary.
    assert flags == sorted(flags, reverse=True)
    assert _by_id(page)["note:scatter"]["phrase_match"] is False
    assert _by_id(page)["note:exact"]["phrase_match"] is True
    assert sorted(ids) == sorted(plain)
    assert page["phrase"]["status"] == "matched"
    assert page["phrase"]["spans"] == [QUERY]
    assert page["phrase"]["checked"] == len(ids)
    assert page["phrase"]["matched"] == sum(flags)


@pytest.mark.postgres
def test_a_passage_only_match_marks_its_record(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_basic_facts())
    rows = _by_id(_lexical(clean_runtime, deployment, limit=50))
    # ticket:quiet's own text lacks the phrase; its comment holds it.
    assert rows["ticket:quiet"]["rollup_best_chunk_id"] == "comment:quiet:0"
    assert rows["ticket:quiet"]["phrase_match"] is True
    # A near miss holds only one of the words.
    assert rows["ticket:near:0"]["phrase_match"] is False


@pytest.mark.postgres
def test_a_matching_passage_wins_its_record_over_a_better_scored_one(
    clean_runtime, tmp_path,
):
    facts = [
        _node("ticket:t", "ticket", "Plain ticket.", status="open"),
        _node("comment:t:scatter", "comment",
              "cladding cladding cladding zirconium zirconium zirconium."),
        _node("comment:t:exact", "comment",
              f"Long remark: the {QUERY} was replaced during the outage window."),
        _edge("ticket:t", "comment:t:scatter"),
        _edge("ticket:t", "comment:t:exact"),
    ]
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=facts)
    plain = _by_id(_lexical(clean_runtime, deployment, QUERY, limit=10))
    assert plain["ticket:t"]["rollup_best_chunk_id"] == "comment:t:scatter"
    quoted = _by_id(_lexical(clean_runtime, deployment, limit=10))
    assert quoted["ticket:t"]["rollup_best_chunk_id"] == "comment:t:exact"
    assert quoted["ticket:t"]["phrase_match"] is True


def _masked_deployment(tmp_path, dsn: str) -> str:
    groups = {
        OPEN: [
            _node("ticket:open", "ticket", "Open ticket.", status="open"),
            _node("comment:open", "comment", f"{QUERY} measured in the open."),
            _node("ticket:masked", "ticket", "Masked ticket.", status="open"),
        ],
        MASKED: [
            _node("comment:masked", "comment", f"{QUERY} {QUERY} masked words."),
        ],
    }
    main = [_edge("ticket:open", "comment:open"), _edge("ticket:masked", "comment:masked")]
    deployment = _publish(tmp_path, dsn, groups=groups, main=main)
    _activate(dsn, deployment, [_allow(OPEN), _allow(MAIN), {
        "rule_id": "redact_masked", "effect": "redact", "actions": ["graph.read.mcp"],
        "subjects": [READER], "match": {"source_id": MASKED}, "priority": 100,
        "obligations": [{"type": "redaction"}, {"type": "audit"}],
    }])
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"pii_classes\": {\"text\": \"free_text\"}}'::jsonb"
            " WHERE name = 'comment'"
        )
    return deployment


@pytest.mark.postgres
@pytest.mark.parametrize("filt", [None, {"subtype": "ticket"}])
def test_a_phrase_only_in_masked_text_does_not_match(
    clean_runtime, tmp_path, monkeypatch, filt,
):
    """#3377: a passage matches only on text the reader reads as stored, so
    a phrase, or a word, only the masked comment holds lifts nothing."""
    search_module = importlib.import_module("okg.substrate.mcp.search")
    deployment = _masked_deployment(tmp_path, clean_runtime)
    # Precondition: on stored text (Nomos off) the masked comment's ticket
    # ranks first.
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    stored = _ids(_lexical(clean_runtime, deployment, QUERY, limit=10, filter=filt))
    assert stored.index("ticket:masked") < stored.index("ticket:open")
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    kwargs = {"limit": 10, "subject": READER, "filter": filt}
    plain = _ids(_lexical(clean_runtime, deployment, QUERY, **kwargs))
    assert "ticket:masked" not in plain and "ticket:open" in plain
    # A spy on the filtered fold's class read of the prefix winners.
    class_read: list[str] = []
    real = search_module.filter_rows_by_read_class

    def spy(conn, session, rows, **spy_kwargs):
        rows = list(rows)
        class_read.extend(str(row["node_id"]) for row in rows)
        return real(conn, session, rows, **spy_kwargs)

    monkeypatch.setattr(search_module, "filter_rows_by_read_class", spy)
    page = _lexical(clean_runtime, deployment, **kwargs)
    rows = _by_id(page)
    assert "ticket:masked" not in rows and "comment:masked" not in rows
    assert "masked words" not in str(page)
    assert rows["ticket:open"]["phrase_match"] is True
    if filt is None:
        # The unfiltered fold's passages were read row by row already.
        assert class_read == []
    else:
        # The verdict came from the masked read of each prefix winner; the
        # masked comment never became one.
        assert sorted(class_read) == ["comment:open"]


def _prefix_facts() -> list:
    """Tickets reached only through their comments, the comments graded so
    the passage read's cap makes the last of four sit outside the prefix."""
    texts = {
        "a": "zirconium zirconium zirconium and cladding cladding cladding.",
        "b": f"The {QUERY} report.",
        "c": "zirconium zirconium then cladding notes.",
        "d": f"The {QUERY} report, filed later today.",
        "e": "zirconium appears alone in this long sentence with cladding far later on.",
        "f": "cladding only appears with zirconium in this final long and rambling note text.",
    }
    facts: list = []
    for key, text in texts.items():
        facts += [
            _node(f"ticket:{key}", "ticket", f"Ticket {key}.", status="open"),
            _node(f"comment:{key}", "comment", text),
            _edge(f"ticket:{key}", f"comment:{key}"),
        ]
    return facts


@pytest.mark.postgres
def test_the_filtered_fold_promotes_only_inside_its_exact_prefix(
    clean_runtime, tmp_path, monkeypatch,
):
    search_module = importlib.import_module("okg.substrate.mcp.search")

    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_prefix_facts())
    filt = {"subtype": "ticket"}
    full = _lexical(clean_runtime, deployment, QUERY, limit=50, filter=filt,
                    completeness="exact")
    order = _ids(full)
    monkeypatch.setattr(search_module, "_LEXICAL_RANK_CAP", 4)
    capped = _lexical(clean_runtime, deployment, QUERY, limit=50, filter=filt,
                      completeness="exact")
    prefix = capped["ordered_exactly_through"]
    holders = {"ticket:b", "ticket:d"}
    # Preconditions: the prefix is three records, today's first three; one
    # phrase holder is inside it (not first) and one sits just past it.
    assert prefix == 3 and _ids(capped) == order[:3]
    assert order[0] not in holders and holders & set(order[1:3])
    assert order[3] in holders
    page = _lexical(clean_runtime, deployment, limit=50, filter=filt,
                    completeness="exact")
    ids = _ids(page)
    assert page["ordered_exactly_through"] == prefix
    # Same records as today's prefix, the holder first; the holder past the
    # prefix is not lifted into it.
    assert sorted(ids) == sorted(order[:3])
    assert ids[0] in holders and order[3] not in ids
    assert [row["phrase_match"] for row in page["results"]] == [True, False, False]
    assert page["phrase"]["checked"] == prefix


@pytest.mark.postgres
def test_phrase_match_survives_a_narrowed_select(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_phrase_facts())
    page = _lexical(clean_runtime, deployment, limit=5, select=["node_id"])
    assert page["results"]
    for row in page["results"]:
        assert isinstance(row["phrase_match"], bool)
        assert "excerpt" not in row
    plain = _lexical(clean_runtime, deployment, QUERY, limit=5, select=["node_id"])
    assert all("phrase_match" not in row for row in plain["results"])


@pytest.mark.postgres
def test_paging_across_the_matched_boundary_is_consistent(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_phrase_facts())
    whole = _lexical(clean_runtime, deployment, limit=50, rank_by="self")
    paged: list[dict] = []
    for offset in range(len(whole["results"])):
        paged += _lexical(clean_runtime, deployment, limit=1, offset=offset,
                          rank_by="self")["results"]
    assert [r["node_id"] for r in paged] == _ids(whole)
    flags = [r["phrase_match"] for r in paged]
    assert True in flags and False in flags
    assert flags == sorted(flags, reverse=True)


@pytest.mark.postgres
@pytest.mark.parametrize("rank_by", ["self", "records", "passages"])
def test_an_unquoted_search_runs_no_phrase_code(clean_runtime, tmp_path, monkeypatch, rank_by):
    """No quote, no phrase path: with every phrase helper made to fail, the
    unquoted page is the same bytes as before, with no phrase field."""
    search_module = importlib.import_module("okg.substrate.mcp.search")

    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_phrase_facts())
    kwargs = {"limit": 20, "rank_by": rank_by}
    if rank_by == "passages":
        kwargs["filter"] = {"subtype": "ticket"}
    before = _lexical(clean_runtime, deployment, QUERY, **kwargs)

    def boom(*_args, **_kwargs):
        raise AssertionError("phrase code ran on an unquoted search")

    for name in ("strip_quotes", "_phrase_verdict", "_mark_phrase", "_promote_phrase_prefix"):
        monkeypatch.setattr(search_module, name, boom)
    after = _lexical(clean_runtime, deployment, QUERY, **kwargs)
    assert json.dumps(after, sort_keys=True, default=str) == json.dumps(
        before, sort_keys=True, default=str,
    )
    assert "phrase" not in after
    assert all("phrase_match" not in row for row in after["results"])


@pytest.mark.postgres
def test_passages_report_phrase_match_on_the_served_page(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_basic_facts())
    kwargs = {"limit": 50, "rank_by": "passages", "filter": {"subtype": "ticket"},
              "completeness": "exact"}
    plain = _ids(_lexical(clean_runtime, deployment, QUERY, **kwargs))
    page = _lexical(clean_runtime, deployment, **kwargs)
    # Reported, not reordered.
    assert _ids(page) == plain
    rows = _by_id(page)
    assert rows["ticket:quiet"]["phrase_match"] is True
    assert page["phrase"]["checked"] == len(page["results"])


@pytest.mark.postgres
def test_quoted_and_stripped_queries_score_alike(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_phrase_facts())
    quoted = _lexical(clean_runtime, deployment, limit=50, rank_by="self",
                      select=["node_id", "score"])
    plain = _lexical(clean_runtime, deployment, QUERY, limit=50, rank_by="self",
                     select=["node_id", "score"])
    assert {r["node_id"]: r["score"] for r in quoted["results"]} == {
        r["node_id"]: r["score"] for r in plain["results"]
    }
    # And BM25 itself reads the quotes as punctuation.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        from okg.substrate.bm25_node_search import resolve_node_text_bm25_index

        segment, index = resolve_node_text_bm25_index(conn)
        rows = conn.execute(
            f"SELECT (attrs->>'text') <@> to_bm25query(%s, %s), "
            f"(attrs->>'text') <@> to_bm25query(%s, %s) FROM {segment} "
            "WHERE attrs ? 'text'",
            (PHRASE, index, QUERY, index),
        ).fetchall()
    assert rows and all(a == b for a, b in rows)


@pytest.mark.postgres
@pytest.mark.parametrize("query", ['"cladding retained zirconium" weld', '"qqzx wwyv"'])
def test_no_exact_match_says_so_and_names_a_grep_call_that_runs(
    clean_runtime, tmp_path, query,
):
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_basic_facts())
    page = _lexical(clean_runtime, deployment, query, limit=10, completeness="exact")
    assert page["phrase"]["status"] == "no_exact_match"
    assert page["phrase"]["matched"] == 0
    assert page["phrase"]["unmatched_spans"] == list(quoted_spans(query))
    assert all(row["phrase_match"] is False for row in page["results"])
    text = render(page, tool="search")
    first, *rest = text.splitlines()
    assert "phrase=no_exact_match" in first.split(" · ")
    (notice,) = [line for line in rest if line.startswith("! no exact match")]
    assert f"among {page['phrase']['checked']} candidates" in notice
    call = notice.split("exact text: ", 1)[1]
    prefix = 'search(method="grep", query='
    assert call.startswith(prefix) and call.endswith(")")
    grep_query = json.loads(call[len(prefix):-1])
    grep = _session_search(clean_runtime, deployment, grep_query, method="grep")
    assert grep["results"] == []


@pytest.mark.postgres
def test_a_matched_quoted_search_has_no_notice(clean_runtime, tmp_path):
    """Failure-case control for the notice: a quoted search that matched
    prints neither the status fact nor the notice."""
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_basic_facts())
    text = render(_lexical(clean_runtime, deployment, limit=10), tool="search")
    assert "phrase=no_exact_match" not in text.splitlines()[0]
    assert "! no exact match" not in text


@pytest.mark.postgres
def test_a_withheld_count_withholds_the_checked_count(clean_runtime, tmp_path):
    """Failure case for a leak: the bounded default withholds `total_count`,
    so the phrase block and the notice must not say how many candidates were
    checked or matched; under `completeness="exact"` they do."""
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_basic_facts())
    query = '"cladding retained zirconium"'
    for filt in (None, {"subtype": "ticket"}):
        bounded = _lexical(clean_runtime, deployment, query, limit=10, filter=filt)
        assert bounded["total_count"] is None
        assert bounded["phrase"]["status"] == "no_exact_match"
        assert bounded["phrase"]["checked"] is None
        assert bounded["phrase"]["matched"] is None
        (notice,) = [line for line in render(bounded, tool="search").splitlines()
                     if line.startswith("! no exact match")]
        assert " among " not in notice
        exact = _lexical(clean_runtime, deployment, query, limit=10, filter=filt,
                         completeness="exact")
        assert isinstance(exact["phrase"]["checked"], int)
        assert exact["phrase"]["matched"] == 0


@pytest.mark.postgres
def test_passages_judge_the_overlaid_passage_text(clean_runtime, tmp_path):
    """Under `passages` the verdict reads the winning passage as the preview
    serves it, overlaid at the pin: an overlay that removes the phrase from
    the stored text makes `phrase_match` false."""
    from okg.substrate.attributes import (
        materialize_effective_attributes_for_generation,
        record_attribute_observation,
    )

    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_basic_facts())
    kwargs = {"limit": 50, "rank_by": "passages", "filter": {"subtype": "ticket"}}
    before = _by_id(_lexical(clean_runtime, deployment, **kwargs))
    assert before["ticket:quiet"]["phrase_match"] is True
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        generation = open_session(conn, deployment_name=deployment).generation_id
        record_attribute_observation(
            conn, target_kind="node", target_id="comment:quiet:0",
            generation_id=generation, attr_key="text",
            value="zirconium sample retained; cladding removed.",
            producer="test:lexical-quoted-phrases",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=generation)
    after = _by_id(_lexical(clean_runtime, deployment, **kwargs))
    row = after["ticket:quiet"]
    assert row["rollup_best_chunk_id"] == "comment:quiet:0"
    assert "cladding removed" in str(row.get("excerpt"))
    assert row["phrase_match"] is False
