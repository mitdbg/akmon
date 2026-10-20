"""`search(method="grep")` ranking and the `path:` term (PACT mcp-grep-rank).

PostgreSQL tests on a published fixture graph: the matched records sort by
the BM25 score of the query's literal words (the node-text BM25 index lexical
search uses), ties by node id; a query with no literal word keeps node-id
order and says so; the count never limits or reorders the page (PACT
mcp-grep-page builds it best-first; the tests after the marker below);
under a redacting policy a secret query and its control give identical
pages, order included. `path:` matches the record's path, own or lent, on the
values the reader reads.

PACT mcp-grep-page (the tests after the marker below): the page is the top
of every visible match, not of an id-ordered first slice; the candidates one
page decides are bounded by a row count and scale with the page; a query
with no literal word pages in node-id order under the same bound; the count
is one function with its own row bound, and paging never reads it; pages are
stable; a hidden row's content never changes a reader's page. Synthetic data
only.
"""
from __future__ import annotations

import psycopg
from psycopg.types.json import Jsonb
import pytest

from okg.substrate.mcp import grep_search
from okg.substrate.mcp.grep_query import GrepPatternTooShort
from tests.substrate.mcp.test_search_grep import (
    ATTR_PII,
    MAIN,
    MASKED,
    OPEN,
    READER,
    SECRET,
    _activate,
    _allow,
    _by_id,
    _corpus,
    _edge,
    _grep,
    _ids,
    _node,
    _publish,
    _same,
)

pytestmark = pytest.mark.postgres


#: A fixture graph is a few rows, so the planner would sort them rather than
#: walk the BM25 index; grep sets the window statement's plan itself, and
#: these connection options make every other statement plan as on a large
#: segment too.
INDEX_PLAN = "-c enable_seqscan=off -c enable_sort=off"


def _grep_indexed(dsn: str, deployment: str, query: str, **kwargs) -> dict:
    from okg.substrate.mcp import open_session, search

    with psycopg.connect(dsn, autocommit=True, options=INDEX_PLAN) as conn:
        session = open_session(conn, deployment_name=deployment)
        return search(conn, session, query, method="grep", **kwargs)


# ---------- ranking ----------

def _ranked_corpus() -> list:
    filler = " ".join(f"filler{i}" for i in range(60))
    return [
        # Highest: the word three times in a short text.
        _node("note:z", "note", "kestrel kestrel kestrel nesting"),
        # Lower: once, in a long text.
        _node("note:m", "note", f"one kestrel among {filler}"),
        # A substring inside another token: grep matches it, BM25 does not.
        _node("note:a", "note", "the xkestrelx token only"),
        # A record whose best member is its passage.
        _node("doc:b", "doc", f"doc text with kestrel once and {filler}", path="manual/b.md"),
        _node("chunk:b:0", "chunk", "kestrel kestrel kestrel kestrel"),
        _edge("doc:b", "chunk:b:0"),
        _node("note:x", "note", "no match here"),
    ]


def test_records_rank_by_bm25_on_the_literal_words(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, _ranked_corpus())
    page = _grep(clean_runtime, deployment, "kestrel")
    # doc:b ranks by its best member (the chunk); the token-internal match
    # scores 0 and comes last; id order would be doc:b, note:a, note:m, note:z.
    assert _ids(page) == ["doc:b", "note:z", "note:m", "note:a"]
    assert page["order_basis"] == "bm25"
    assert page["rank_words"] == ["kestrel"]
    assert "order_basis_reason" not in page
    # Four records (doc:b's own text and its chunk fold into it).
    assert page["total_count"] == 4 and page["total_count_basis"] == "exact"
    assert _by_id(page)["doc:b"]["matching_passages"] == 1
    # Paging walks the ranked order.
    second = _grep(clean_runtime, deployment, "kestrel", limit=2, offset=2)
    assert _ids(second) == ["note:m", "note:a"]


def test_the_index_scan_and_the_score_expression_give_the_same_order(
    clean_runtime, tmp_path, monkeypatch,
):
    deployment = _publish(tmp_path, clean_runtime, _ranked_corpus())
    expected = ["doc:b", "note:z", "note:m", "note:a"]
    # R1 first reads a small match set whole and scores it.
    first = _grep(clean_runtime, deployment, "kestrel")
    assert first["rank_scan"] == "match_set"
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", 0)
    # Each window runs as the index's own ordered scan (grep sets the plan
    # for the statement), which hands its score over, whatever plan the
    # connection would choose.
    planned = _grep(clean_runtime, deployment, "kestrel")
    assert "rank_scan" not in planned
    indexed = _grep_indexed(clean_runtime, deployment, "kestrel")
    assert "rank_scan" not in indexed
    assert _ids(first) == _ids(indexed) == _ids(planned) == expected
    assert first["total_count"] == indexed["total_count"] == planned["total_count"] == 4
    # A NEAR query reads the match set only after windows that did not fill
    # the page, then finishes from it.
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", 2_000)
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 1)
    monkeypatch.setattr(grep_search, "PAGE_PROBE_AFTER", 1)
    near = _grep(clean_runtime, deployment, "kestrel NEAR/80 kestrel")
    assert near["rank_scan"] == "match_set"
    assert _ids(near) == ["doc:b", "note:z"]


def test_the_index_score_is_not_trusted_off_the_index_scan(
    clean_runtime, tmp_path, monkeypatch,
):
    # Outside the index's ordered scan bm25_get_current_score() returns 0;
    # with index scans off for the connection the plan check sees no index
    # scan and scores every window with the expression: the same order.
    deployment = _publish(tmp_path, clean_runtime, _ranked_corpus())
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", 0)
    # Without grep's own plan settings the connection's settings decide.
    monkeypatch.setattr(grep_search, "_INDEX_PLAN", ())
    from okg.substrate.mcp import open_session, search

    with psycopg.connect(
        clean_runtime, autocommit=True,
        options="-c enable_indexscan=off -c enable_bitmapscan=off",
    ) as conn:
        session = open_session(conn, deployment_name=deployment)
        page = search(conn, session, "kestrel", method="grep")
    assert page["rank_scan"] == "expression"
    assert _ids(page) == ["doc:b", "note:z", "note:m", "note:a"]


@pytest.mark.parametrize(
    ("small_set", "first_window"),
    [(2_000, 1_000), (0, 1_000), (0, 1)],
    ids=["match-set", "windows", "one-row-windows"],
)
def test_a_real_match_bm25_scores_zero_is_kept_and_counted(
    clean_runtime, tmp_path, monkeypatch, small_set, first_window,
):
    """#3232 on grep: a word match the `<@>` expression scores 0 is not lost.

    pg_textsearch can score a real match 0 on the first read after a
    publish (issue #3232, fixed for lexical by #3234). Grep never uses the
    score as membership: the BM25 pass stops at the first row scoring 0 and
    the zero-score pass reads every remaining grep match in node-id order,
    so `note:m` (its "kestrel" scored 0 here) is still listed, after the
    scored matches, and the count stays exact. Simulated as #3234's tests
    do: a `<@>` earlier on the connection's search path scores 0 for it.
    """
    from okg.substrate.mcp import open_session, search
    from tests.substrate.test_bm25_node_search import _score_one_match_zero

    deployment = _publish(tmp_path, clean_runtime, _ranked_corpus())
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", small_set)
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", first_window)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        _score_one_match_zero(conn, "one kestrel among")
        page = search(conn, session, "kestrel", method="grep")
        first = search(conn, session, "kestrel", method="grep", limit=3)
        rest = search(conn, session, "kestrel", method="grep", limit=3, offset=3)
    # Unscored, note:m joins note:a (the token-internal match) in the
    # zero-score pass, in node-id order; no match is dropped.
    assert _ids(page) == ["doc:b", "note:z", "note:a", "note:m"]
    assert page["total_count"] == 4 and page["total_count_basis"] == "exact"
    assert page["page_basis"] == "top_of_all_visible_matches"
    assert page.get("next_offset") is None
    # Paging never ends before the zero-scored match.
    assert _ids(first) == ["doc:b", "note:z", "note:a"]
    assert first["next_offset"] == 3 and first["truncated"] is True
    assert _ids(rest) == ["note:m"] and rest.get("next_offset") is None


def test_ties_break_by_node_id_and_repeat_exactly(clean_runtime, tmp_path):
    facts = [
        _node(f"note:{i}", "note", "osprey osprey twin text") for i in (3, 1, 2)
    ] + [_node("note:0", "note", "osprey once in a much longer twin text body")]
    deployment = _publish(tmp_path, clean_runtime, facts)
    first = _grep(clean_runtime, deployment, "osprey")
    assert _ids(first) == ["note:1", "note:2", "note:3", "note:0"]
    for _ in range(3):
        assert _grep(clean_runtime, deployment, "osprey")["results"] == first["results"]


def test_a_regex_only_query_keeps_node_id_order_and_says_so(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, _ranked_corpus())
    page = _grep(clean_runtime, deployment, "re:/kestrel/")
    assert _ids(page) == ["doc:b", "note:a", "note:m", "note:z"]
    assert page["order_basis"] == "node_id"
    assert page["order_basis_reason"] == "no_literal_words"
    assert "rank_words" not in page
    # A negated word never ranks: the regex still decides the order.
    negated = _grep(clean_runtime, deployment, "re:/kestrel/ -nesting")
    assert negated["order_basis"] == "node_id"
    assert _ids(negated) == ["doc:b", "note:a", "note:m"]


def test_the_count_bound_never_limits_the_order(clean_runtime, tmp_path, monkeypatch):
    # The most relevant rows sort last by id; the count's bound is smaller
    # than the match set, and the page is still the top of every match.
    facts = [_node(f"note:{i:03d}", "note", "heron " + "pad " * (20 - i)) for i in range(10)]
    facts += [_node(f"note:{i:03d}", "note", "heron heron heron heron") for i in (10, 11)]
    deployment = _publish(tmp_path, clean_runtime, facts)
    exact = _grep(clean_runtime, deployment, "heron", limit=20)
    assert exact["total_count"] == 12 and exact["total_count_basis"] == "exact"
    assert _ids(exact)[:2] == ["note:010", "note:011"]
    monkeypatch.setattr(grep_search, "COUNT_SCAN_BOUND", 10)
    pages = [
        _grep(clean_runtime, deployment, "heron", limit=6, offset=offset)
        for offset in (0, 6)
    ]
    for capped in pages:
        # A lower bound is never below the records the page already found
        # (here all twelve, in one batch).
        assert capped["total_count"] == 12
        assert capped["total_count_basis"] == "lower_bound"
        assert capped["scan_cap"] == {"rows": 10}
        assert capped["order_basis"] == "bm25"
    assert _ids(pages[0]) + _ids(pages[1]) == _ids(exact)
    assert _ids(exact) == ["note:010", "note:011"] + [
        f"note:{i:03d}" for i in range(9, -1, -1)
    ]


# ---------- path: ----------

def test_path_term_matches_the_own_and_the_lending_path(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, _corpus())
    # Case-insensitive substring of doc:a's own path; its chunk folds in.
    page = _grep(clean_runtime, deployment, "uuid4 path:PKG/A")
    assert _ids(page) == ["doc:a"]
    assert page["results"][0]["matching_passages"] == 1
    assert {"kind": "path", "term": "PKG/A", "negated": False, "indexable": True} in (
        page["grep_terms"]
    )
    # "generation" is only in doc:b's chunk, which carries no path: it is
    # admitted through its record's path.
    assert _ids(_grep(clean_runtime, deployment, "generation path:guide")) == ["doc:b"]
    # A path term alone is a scope: every text unit under the path, in id order.
    alone = _grep(clean_runtime, deployment, "path:pkg/")
    assert _ids(alone) == ["doc:a", "doc:c"]
    assert alone["order_basis"] == "node_id"
    assert alone["order_basis_reason"] == "no_literal_words"
    # It ANDs with path_prefix and the text terms.
    assert sorted(_ids(_grep(clean_runtime, deployment, "zirconium path:pkg",
                             path_prefix="app/"))) == ["doc:a", "doc:c"]
    assert _grep(clean_runtime, deployment, "zirconium path:pkg",
                 path_prefix="manual/")["total_count"] == 0
    # Quoted, and literal: a regex metacharacter is not special.
    assert _ids(_grep(clean_runtime, deployment, 'uuid4 path:"a.py"')) == ["doc:a"]
    assert _grep(clean_runtime, deployment, "uuid4 path:a.p.")["total_count"] == 0


def test_a_path_term_counts_as_a_scope_only_from_three_characters(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, _corpus())
    with pytest.raises(GrepPatternTooShort):
        _grep(clean_runtime, deployment, "ab path:pk")
    # Three letters make the scope: a two-letter word now runs.
    page = _grep(clean_runtime, deployment, "on path:pkg")
    # "Second", "one" and "zirconium" hold "on"; doc:b is outside pkg/.
    assert sorted(_ids(page)) == ["doc:a", "doc:c"]


def test_exclusion_by_path(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, _corpus())
    # doc:a is under pkg/; note:1 has no path at all and stays.
    assert sorted(_ids(_grep(clean_runtime, deployment, "uuid4 -path:pkg"))) == [
        "doc:b", "note:1",
    ]
    # The chunk's lending record is excluded by its path.
    assert _grep(clean_runtime, deployment, "generation -path:guide")["total_count"] == 0
    assert _ids(_grep(clean_runtime, deployment, "generation -path:pkg")) == ["doc:b"]
    # With a positive path condition, the exclusion applies to the same path.
    assert _ids(_grep(clean_runtime, deployment, "uuid4 path:app -path:c.py")) == ["doc:a"]


def test_the_path_trigram_index_is_built_and_required(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, _corpus())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        defs = [
            row[0] for row in conn.execute(
                "SELECT indexdef FROM pg_indexes WHERE indexname ~ 'node_path_trgm_visible$'"
            )
        ]
        assert len(defs) == 1 and "gin_trgm_ops" in defs[0], defs
        name = conn.execute(
            "SELECT schemaname, indexname FROM pg_indexes "
            "WHERE indexname ~ 'node_path_trgm_visible$'"
        ).fetchone()
        conn.execute(f'DROP INDEX "{name[0]}"."{name[1]}"')
    from okg.substrate.bm25_node_search import SearchIndexUnavailable

    with pytest.raises(SearchIndexUnavailable):
        _grep(clean_runtime, deployment, "uuid4 path:pkg")
    # A query with no path term does not need it (three records).
    assert _grep(clean_runtime, deployment, "uuid4")["total_count"] == 3


# ---------- Nomos: no oracle, order included ----------

def _redacting_with_order(tmp_path, dsn: str, pii: dict[str, str]) -> str:
    """test_search_grep's masked-path fixture plus open rows whose BM25
    scores differ, so the order of a page is meaningful."""
    groups = {
        OPEN: [
            _node("note:open", "note", "an open launch memo", path="open/memo.md",
                  owner="carol"),
            _node("chunk:open:lent", "chunk", "launch checklist under a masked doc"),
            _node("note:open:2", "note", "launch launch launch day", path="open/a.md"),
            _node("note:open:3", "note", "launch " + "pad " * 40, path="open/b.md"),
        ],
        MASKED: [
            _node("note:masked", "note", "quasar7 launch notes", path="secret/alice/x.md",
                  owner="alice"),
            _node("doc:masked", "doc", "Masked doc.", path="secret/alice/doc.md",
                  owner="alice"),
        ],
    }
    main = [_edge("doc:masked", "chunk:open:lent")]
    rules = [_allow(OPEN), _allow(MAIN), {
        "rule_id": "redact_masked", "effect": "redact", "actions": ["graph.read.mcp"],
        "subjects": [READER], "match": {"source_id": MASKED}, "priority": 100,
        "obligations": [{"type": "redaction"}, {"type": "audit"}],
    }]
    deployment = _publish(tmp_path, dsn, main, groups)
    _activate(dsn, deployment, rules)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || jsonb_build_object('pii_classes', %s::jsonb)"
            " WHERE name IN ('note', 'doc')",
            (Jsonb(pii),),
        )
    return deployment


@pytest.mark.parametrize("secret,control", [
    ("launch -path:secret/al", "launch -path:secret/zz"),
    ("launch path:secret/al", "launch path:secret/zz"),
    ("launch -path:alice", "launch -path:zzzzz"),
])
def test_a_path_term_is_no_oracle_on_a_masked_path(
    clean_runtime, tmp_path, monkeypatch, secret, control,
):
    deployment = _redacting_with_order(tmp_path, clean_runtime, ATTR_PII)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    page = _grep(clean_runtime, deployment, secret, subject=READER)
    _same(page, _grep(clean_runtime, deployment, control, subject=READER))
    assert "note:masked" not in _ids(page)
    # A unit whose lending record's path is masked is never decided on it.
    assert "chunk:open:lent" not in str(page)
    if secret.startswith("launch -"):
        # Readable paths still decide, and the page is ranked.
        assert _ids(page) == ["note:open:2", "note:open", "note:open:3"]
        assert page["order_basis"] == "bm25"
    else:
        assert page["total_count"] == 0
    # A readable path still matches.
    assert _ids(_grep(clean_runtime, deployment, "launch path:MEMO",
                      subject=READER)) == ["note:open"]


@pytest.mark.parametrize("pii", [
    pytest.param({"text": "free_text"}, id="dropped-text"),
    pytest.param({"text": "quasi_identifier"}, id="masked-text"),
])
def test_ranking_is_no_oracle_on_redacted_text(clean_runtime, tmp_path, monkeypatch, pii):
    deployment = _redacting_with_order(tmp_path, clean_runtime, pii)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    for secret, control in (
        ("launch -quasar7", "launch -zzqqxx"),
        ("launch re:/quasar7|launch/", "launch re:/zzqqxx|launch/"),
    ):
        page = _grep(clean_runtime, deployment, secret, subject=READER)
        _same(page, _grep(clean_runtime, deployment, control, subject=READER))
        assert page["order_basis"] == "bm25"
        # Only rows whose text the reader reads unchanged are ranked; the
        # order is over them alone.
        assert "note:masked" not in _ids(page)
        ids = _ids(page)
        assert len(ids) >= 3
        assert ids.index("note:open:2") < ids.index("note:open:3")


# ========== PACT mcp-grep-page: the page is built best-first ==========

def _count_page_checks(monkeypatch) -> list[int]:
    """Record how many candidate rows each scan-site call decides, with the
    count replaced by a stub so every recorded row is the page's."""
    sizes: list[int] = []
    real = grep_search._scan_visible

    def scan_visible(conn, session, rows, **kwargs):
        sizes.append(len(rows))
        return real(conn, session, rows, **kwargs)

    monkeypatch.setattr(grep_search, "_scan_visible", scan_visible)
    monkeypatch.setattr(
        grep_search, "count_grep_matches",
        lambda *a, **k: grep_search.GrepCount(0, "exact", ()),
    )
    return sizes


# ---------- best first, past the old 10,000-row slice ----------

def test_the_best_match_past_the_first_ten_thousand_by_id_comes_first(
    clean_runtime, tmp_path, monkeypatch,
):
    # 10,050 matches; in node-id order the best one is the very last, past
    # the 10,000-row slice the previous design ranked.
    facts = [
        _node(f"note:{i:05d}", "note", f"heron {'pad ' * (8 + i % 5)}row {i}")
        for i in range(10_049)
    ]
    facts.append(_node("note:99999", "note", "heron heron heron"))
    deployment = _publish(tmp_path, clean_runtime, facts)

    page = _grep(clean_runtime, deployment, "heron", limit=5)
    assert _ids(page)[0] == "note:99999"
    assert page["order_basis"] == "bm25"
    assert page["page_basis"] == "top_of_all_visible_matches"
    assert page["next_offset"] == 5 and page["truncated"] is True
    # The count looks at its own bound of rows, unordered: a lower bound.
    assert page["total_count"] == grep_search.COUNT_SCAN_BOUND
    assert page["total_count_basis"] == "lower_bound"
    assert page["scan_cap"] == {"rows": grep_search.COUNT_SCAN_BOUND}
    assert page["count_basis_reasons"] == ["scan_cap"]

    # The page decided a small multiple of its own size, not the matches:
    # one batch per BM25 window it read (here the best row alone fills the
    # first window's settled part, then one batch fills the page).
    sizes = _count_page_checks(monkeypatch)
    again = _grep(clean_runtime, deployment, "heron", limit=5)
    assert _ids(again) == _ids(page)
    batch = max(grep_search.PAGE_BATCH_MIN, grep_search.PAGE_BATCH_MULTIPLE * (0 + 5 + 1))
    assert sizes == [1, batch]


def test_page_cost_is_bounded_by_a_row_count(clean_runtime, tmp_path, monkeypatch):
    facts = [_node(f"note:{i:03d}", "note", f"egret {'pad ' * i}") for i in range(40)]
    deployment = _publish(tmp_path, clean_runtime, facts)
    sizes = _count_page_checks(monkeypatch)
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 7)
    page = _grep(clean_runtime, deployment, "egret", limit=10)
    # R1: every row taken is charged and decided, never more than the bound;
    # the eighth row proves the bound was hit.
    assert sum(sizes) == 7
    assert _ids(page) == [f"note:{i:03d}" for i in range(7)]
    assert page["page_basis"] == "scan_bound_reached"
    assert page["page_scan_bound_hit"] == "candidate_rows"
    assert page["page_scan_bound"] == {
        "candidate_rows": 7, "index_rows": grep_search.PAGE_INDEX_ROWS,
        "id_rows": grep_search.PAGE_ID_ROWS,
    }
    assert page["next_offset"] is None
    assert page["next_offset_reason"] == "scan_bound_reached"
    # A page the bound allows fills, and costs a multiple of its size.
    sizes.clear()
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 2_000)
    small = _grep(clean_runtime, deployment, "egret", limit=3)
    assert small["page_basis"] == "top_of_all_visible_matches"
    assert small["next_offset"] == 3
    assert sum(sizes) <= max(grep_search.PAGE_BATCH_MIN, grep_search.PAGE_BATCH_MULTIPLE * 4)


def test_a_bm25_index_row_bound_stops_the_scan_and_says_so(
    clean_runtime, tmp_path, monkeypatch,
):
    # Thirty BM25 matches fail the regex; the one that passes ranks last.
    facts = [_node(f"note:{i:03d}", "note", f"plover kept {i}") for i in range(30)]
    facts.append(_node("note:999", "note", "plover kept 900 " + "pad " * 20))
    deployment = _publish(tmp_path, clean_runtime, facts)
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", 0)
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 4)
    monkeypatch.setattr(grep_search, "PAGE_INDEX_ROWS", 10)
    query = r"plover re:/kept 9\d\d/"
    page = _grep(clean_runtime, deployment, query)
    assert page["results"] == []
    assert page["page_basis"] == "scan_bound_reached"
    assert page["page_scan_bound_hit"] == "index_rows"
    assert page["page_scan_bound"]["index_rows"] == 10
    # Review P2-2: how far the index read went, so the empty page never
    # says a bare "no match".
    assert page["grep_searched"]["bound_read"] == {"bound": "index_rows", "rows": 10}
    assert page["next_offset"] is None
    # The count is its own read: it still sees the match.
    assert page["total_count"] == 1
    monkeypatch.setattr(grep_search, "PAGE_INDEX_ROWS", 100_000)
    unbounded = _grep(clean_runtime, deployment, query)
    assert _ids(unbounded) == ["note:999"]
    assert unbounded["page_basis"] == "top_of_all_visible_matches"


def test_a_tie_cut_by_the_index_row_bound_is_served_and_reported(
    clean_runtime, tmp_path, monkeypatch,
):
    # Ten equal scores; the windows end inside the tie.
    facts = [_node(f"note:{i:02d}", "note", "egret same text") for i in range(10)]
    deployment = _publish(tmp_path, clean_runtime, facts)
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", 0)
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 2)
    monkeypatch.setattr(grep_search, "PAGE_INDEX_ROWS", 8)
    page = _grep(clean_runtime, deployment, "egret", limit=3)
    ids = _ids(page)
    # The windows re-read the tie and every row they return counts against
    # the bound of eight: the last window allowed holds two tied rows, served
    # in node-id order, and the page says the bound was hit.
    assert len(ids) == 2 and ids == sorted(ids)
    assert page["page_basis"] == "scan_bound_reached"
    assert page["page_scan_bound_hit"] == "index_rows"
    assert page["next_offset"] is None
    # Without the bound the whole tie is read: the lowest ids come first.
    monkeypatch.setattr(grep_search, "PAGE_INDEX_ROWS", 100_000)
    whole = _grep(clean_runtime, deployment, "egret", limit=3)
    assert _ids(whole) == ["note:00", "note:01", "note:02"]
    assert whole["page_basis"] == "top_of_all_visible_matches"


@pytest.mark.parametrize("source", ["index", "expression"])
def test_windows_settle_on_one_score_source_for_buffered_rows(
    clean_runtime, tmp_path, monkeypatch, source,
):
    """Rows a fixture just published sit in the index's in-memory buffer,
    where the index and the `<@>` expression score lengths differently.
    With windows of one row, every row is a window's edge: the pages must
    still walk one order with no repeat or gap, from either source."""
    lengths = [3, 9, 3, 17, 5, 9, 30, 1, 12, 5, 7, 22, 3, 14, 9, 2]
    facts = [
        _node(f"note:{i:02d}", "note", "plover " + "pad " * k) for i, k in enumerate(lengths)
    ]
    deployment = _publish(tmp_path, clean_runtime, facts)
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", 0)
    if source == "expression":
        monkeypatch.setattr(grep_search, "_index_ordered", lambda *a, **k: False)
    whole = _grep(clean_runtime, deployment, "plover", limit=50)
    assert sorted(_ids(whole)) == sorted(f"note:{i:02d}" for i in range(len(lengths)))
    assert ("rank_scan" in whole) == (source == "expression")
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 1)
    pages = [
        _grep(clean_runtime, deployment, "plover", limit=3, offset=o)
        for o in range(0, len(lengths), 3)
    ]
    assert sum((_ids(p) for p in pages), []) == _ids(whole)
    if source == "expression":
        # The order is the expression's own, (score, node id).
        from okg.substrate.bm25_node_search import resolve_node_text_bm25_index

        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            _segment, index_name = resolve_node_text_bm25_index(conn)
            scored = conn.execute(
                "SELECT node_id, (attrs->>'text') <@> to_bm25query('plover', %s) "
                f"FROM {grep_search.resolve_trgm_segment(conn)} "
                "WHERE deleted = FALSE AND attrs ? 'text'",
                (index_name,),
            ).fetchall()
        expected = [nid for nid, sc in sorted(scored, key=lambda r: (r[1], r[0])) if sc < 0]
        assert _ids(whole) == expected


# ---------- no literal word: node-id order under the same bound ----------

def test_a_regex_only_query_pages_in_id_order_under_a_stated_bound(
    clean_runtime, tmp_path, monkeypatch,
):
    facts = [_node(f"note:{i:02d}", "note", f"ticket TCK-{1000 + i}") for i in range(9)]
    facts += [
        _node("doc:q", "doc", "a doc with no ticket"),
        _node("chunk:q:0", "chunk", "its chunk has TCK-7"),
        _edge("doc:q", "chunk:q:0"),
    ]
    deployment = _publish(tmp_path, clean_runtime, facts)
    page = _grep(clean_runtime, deployment, r"re:/TCK-\d+/", limit=4)
    # chunk:q:0 sorts first by id, so its record leads.
    assert _ids(page) == ["doc:q", "note:00", "note:01", "note:02"]
    assert page["order_basis"] == "node_id"
    assert page["order_basis_reason"] == "no_literal_words"
    assert page["page_scan_bound"] == {
        "candidate_rows": grep_search.PAGE_SCAN_BOUND, "id_rows": grep_search.PAGE_ID_ROWS,
    }
    assert page["page_basis"] == "top_of_all_visible_matches"
    assert page["next_offset"] == 4
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 3)
    bounded = _grep(clean_runtime, deployment, r"re:/TCK-\d+/", limit=4)
    assert _ids(bounded) == ["doc:q", "note:00", "note:01"]
    assert bounded["page_basis"] == "scan_bound_reached"
    assert bounded["page_scan_bound"] == {
        "candidate_rows": 3, "id_rows": grep_search.PAGE_ID_ROWS,
    }
    assert bounded["next_offset"] is None


def test_a_large_regex_only_match_set_is_walked_in_id_order_under_a_row_bound(
    clean_runtime, tmp_path, monkeypatch,
):
    # Matches spread through the id order between non-matching rows.
    facts = []
    for i in range(40):
        facts.append(_node(f"note:{i:03d}a", "note", f"ticket TCK-{1000 + i}"))
        facts.append(_node(f"note:{i:03d}b", "note", "no ticket here"))
    deployment = _publish(tmp_path, clean_runtime, facts)
    whole = _ids(_grep(clean_runtime, deployment, r"re:/TCK-\d+/", limit=6))
    assert whole == [f"note:{i:03d}a" for i in range(6)]
    # Past the small-set size the scope is walked by node id: same order.
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", 3)
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 2)
    walked = _grep(clean_runtime, deployment, r"re:/TCK-\d+/", limit=6)
    assert _ids(walked) == whole
    assert walked["page_basis"] == "top_of_all_visible_matches"
    assert walked["next_offset"] == 6
    # The walk stops at its row bound (rows of the scope, matching or not).
    monkeypatch.setattr(grep_search, "PAGE_ID_ROWS", 6)
    bounded = _grep(clean_runtime, deployment, r"re:/TCK-\d+/", limit=6)
    assert _ids(bounded) == ["note:000a", "note:001a", "note:002a"]
    assert bounded["page_basis"] == "scan_bound_reached"
    assert bounded["page_scan_bound_hit"] == "id_rows"
    assert bounded["page_scan_bound"]["id_rows"] == 6
    assert bounded["next_offset"] is None


def test_an_alternation_of_literals_ranks_by_bm25_on_its_words(clean_runtime, tmp_path):
    facts = [
        _node("note:a", "note", "a rollback " + "pad " * 30),
        _node("note:b", "note", "revert revert now"),
        _node("note:c", "note", "xrollbackx inside a token"),
        _node("note:d", "note", "nothing here"),
    ]
    deployment = _publish(tmp_path, clean_runtime, facts)
    page = _grep(clean_runtime, deployment, "re:/rollback|revert/")
    assert page["order_basis"] == "bm25"
    assert page["rank_words"] == ["rollback", "revert"]
    # BM25 matches first by score; the match inside a longer token (score 0)
    # still matches and follows.
    assert _ids(page) == ["note:b", "note:a", "note:c"]
    # Any other regex keeps node-id order.
    other = _grep(clean_runtime, deployment, "re:/roll.?back|revert/")
    assert other["order_basis"] == "node_id"
    assert _ids(other) == ["note:a", "note:b", "note:c"]


def test_a_path_scope_on_a_graph_without_paths_is_refused(clean_runtime, tmp_path):
    from okg.substrate.mcp.grep_search import GrepPathNotApplicable

    deployment = _publish(tmp_path, clean_runtime, [
        _node("note:1", "note", "uuid4 without any path"),
    ])
    for kwargs in ({"path_prefix": "app/"}, {}):
        query = "uuid4" if kwargs else "uuid4 path:app"
        with pytest.raises(GrepPathNotApplicable) as raised:
            _grep(clean_runtime, deployment, query, **kwargs)
        assert raised.value.code == "path_prefix_not_applicable"
    # An exclusion still runs: nothing carries the excluded path.
    assert _ids(_grep(clean_runtime, deployment, "uuid4 -path:app")) == ["note:1"]


# ---------- the count ----------

def test_the_count_is_exact_below_its_bound_and_a_lower_bound_past_it(
    clean_runtime, tmp_path, monkeypatch,
):
    facts = [_node(f"note:{i}", "note", f"curlew {i}") for i in range(5)]
    facts += [_node("doc:c", "doc", "a doc"), _node("chunk:c:0", "chunk", "curlew chunk"),
              _edge("doc:c", "chunk:c:0")]
    deployment = _publish(tmp_path, clean_runtime, facts)
    page = _grep(clean_runtime, deployment, "curlew", limit=2)
    # Six records (five notes, and doc:c through its chunk).
    assert page["total_count"] == 6 and page["total_count_basis"] == "exact"
    assert page["count_unit"] == "record"
    assert "scan_cap" not in page and "count_basis_reasons" not in page
    monkeypatch.setattr(grep_search, "COUNT_SCAN_BOUND", 6)
    assert _grep(clean_runtime, deployment, "curlew")["total_count_basis"] == "exact"
    monkeypatch.setattr(grep_search, "COUNT_SCAN_BOUND", 5)
    capped = _grep(clean_runtime, deployment, "curlew", limit=2)
    # Never below the records the page found (all six, in one batch).
    assert capped["total_count"] == 6
    assert capped["total_count_basis"] == "lower_bound"
    assert capped["scan_cap"] == {"rows": 5}
    assert capped["count_basis_reasons"] == ["scan_cap"]
    # Paging never reads the count.
    assert _ids(capped) == _ids(page) and capped["next_offset"] == 2
    monkeypatch.setattr(grep_search, "COUNT_SCAN_BOUND", 0)
    assert len(_ids(_grep(clean_runtime, deployment, "curlew", limit=10))) == 6


def test_the_count_seam_takes_match_sql_and_a_bound(clean_runtime, tmp_path, monkeypatch):
    deployment = _publish(tmp_path, clean_runtime, [
        _node(f"note:{i}", "note", f"godwit {i}") for i in range(3)
    ])
    seen: list[dict] = []
    real = grep_search.count_grep_matches

    def spy(conn, session, match_sql, params, **kwargs):
        from psycopg import sql as pgsql

        assert isinstance(match_sql, pgsql.Composable)
        text = match_sql.as_string(conn)
        assert "LIMIT" not in text and "n.subtype" in text and "n.source" in text
        seen.append(kwargs)
        return real(conn, session, match_sql, params, **kwargs)

    monkeypatch.setattr(grep_search, "count_grep_matches", spy)
    page = _grep(clean_runtime, deployment, "godwit")
    assert page["total_count"] == 3
    assert seen[0]["scan_bound"] == grep_search.COUNT_SCAN_BOUND
    assert grep_search.COUNT_BASIS == {
        "exact": "exact", "per_row": "exact", "lower_bound": "lower_bound",
    }


# ---------- paging ----------

def test_three_pages_walk_one_order_with_no_repeat_or_gap(
    clean_runtime, tmp_path, monkeypatch,
):
    facts = []
    for i in range(4):
        # Equal scores (ties) and records ranked by a passage.
        facts.append(_node(f"note:t{i}", "note", "dunlin dunlin tie"))
    for i in range(3):
        facts += [
            _node(f"doc:{i}", "doc", f"doc {i} says dunlin once among many other words"),
            _node(f"chunk:{i}:0", "chunk", "dunlin " * (3 - i) + "x"),
            _edge(f"doc:{i}", f"chunk:{i}:0"),
        ]
    facts.append(_node("note:sub", "note", "a xdunlinx substring"))
    deployment = _publish(tmp_path, clean_runtime, facts)
    whole = _ids(_grep(clean_runtime, deployment, "dunlin", limit=50))
    assert len(whole) == len(set(whole)) == 8
    assert whole[-1] == "note:sub"
    ties = [i for i in whole if i.startswith("note:t")]
    assert ties == sorted(ties)
    pages = [_grep(clean_runtime, deployment, "dunlin", limit=3, offset=o) for o in (0, 3, 6)]
    assert [p["next_offset"] for p in pages] == [3, 6, None]
    assert sum((_ids(p) for p in pages), []) == whole
    # Windows of one row put every tie on a window's edge (no match-set
    # read); then the same read whole first.
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 1)
    monkeypatch.setattr(grep_search, "PAGE_WINDOW_GROWTH", 2)
    for small in (0, 2_000):
        monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", small)
        for _ in range(2):
            pages = [
                _grep(clean_runtime, deployment, "dunlin", limit=3, offset=o)
                for o in (0, 3, 6)
            ]
            assert [p["next_offset"] for p in pages] == [3, 6, None]
            assert sum((_ids(p) for p in pages), []) == whole
        assert pages[-1].get("rank_scan") == ("match_set" if small else None)


# ---------- Nomos: hidden and redacted rows ----------

def _hidden_row_deployment(tmp_path, dsn: str, hidden_text: str) -> str:
    groups = {
        OPEN: [
            _node("note:o1", "note", "sanderling flock on the beach"),
            _node("note:o2", "note", "one sanderling"),
            _node("note:o3", "note", f"sanderling {'pad ' * 30}"),
        ],
        SECRET: [_node("note:hidden", "note", hidden_text)],
    }
    deployment = _publish(tmp_path, dsn, [], groups)
    _activate(dsn, deployment, [_allow(OPEN), _allow(MAIN)])
    return deployment


@pytest.mark.parametrize("bound", ["PAGE_SCAN_BOUND", "COUNT_SCOPE_ROWS"])
def test_a_hidden_rows_content_beyond_the_words_does_not_change_the_page(
    clean_runtime, tmp_path, monkeypatch, bound,
):
    """R2 (a hiding policy): a secret query and its control, differing only
    in a term a hidden row's content decides, give identical pages, counts
    and bounds at every offset, with the bound set small. What R2 does not
    hide is which rows the BM25 words match (hidden ones included) and
    their word statistics (the next test), as in lexical search."""
    deployment = _hidden_row_deployment(
        tmp_path, clean_runtime, "sanderling quasar7 hidden note",
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setattr(grep_search, bound, 2)
    for offset in (0, 1, 2):
        secret = _grep(clean_runtime, deployment, "sanderling -re:/quasar7/",
                       subject=READER, limit=1, offset=offset)
        control = _grep(clean_runtime, deployment, "sanderling -re:/zzqqxx/",
                        subject=READER, limit=1, offset=offset)
        _same(secret, control)
        assert "note:hidden" not in str(secret)


@pytest.mark.parametrize("hidden_text,expected", [
    pytest.param("nothing", ["note:a", "note:b"], id="short-hidden-row"),
    pytest.param("nothing " * 2000, ["note:b", "note:a"], id="long-hidden-row"),
])
def test_a_hidden_rows_length_can_change_visible_order_disclosed(
    clean_runtime, tmp_path, monkeypatch, hidden_text, expected,
):
    """Disclosed, not prevented (review of aabd67f653): BM25 normalizes by
    the average text length, which counts hidden rows, so a hidden row's
    length reorders visible rows of different lengths and term counts even
    for a one-word query, as in lexical search."""
    groups = {
        OPEN: [
            _node("note:a", "note", "sanderling x"),
            _node("note:b", "note", "sanderling sanderling " + "pad " * 8),
        ],
        SECRET: [_node("note:hidden", "note", hidden_text)],
    }
    deployment = _publish(tmp_path, clean_runtime, [], groups)
    _activate(clean_runtime, deployment, [_allow(OPEN), _allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    assert _ids(_grep(clean_runtime, deployment, "sanderling", subject=READER)) == expected


def _redacting_bound(tmp_path, dsn: str) -> str:
    groups = {
        OPEN: [_node(f"note:o{i}", "note", "dunlin xyz open") for i in range(2)],
        MASKED: [_node("note:m", "note", "dunlin SECRET42 masked")],
    }
    deployment = _publish(tmp_path, dsn, [], groups)
    _activate(dsn, deployment, [_allow(OPEN), _allow(MAIN), {
        "rule_id": "redact_masked", "effect": "redact", "actions": ["graph.read.mcp"],
        "subjects": [READER], "match": {"source_id": MASKED}, "priority": 100,
        "obligations": [{"type": "redaction"}, {"type": "audit"}],
    }])
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"pii_classes\": {\"text\": \"free_text\"}}'::jsonb WHERE name = 'note'"
        )
    return deployment


@pytest.mark.parametrize("bound", ["PAGE_SCAN_BOUND", "COUNT_SCOPE_ROWS", "PAGE_ID_ROWS"])
@pytest.mark.parametrize("small", [0, 2_000])
def test_the_row_bounds_are_no_oracle_on_redacted_text(
    clean_runtime, tmp_path, monkeypatch, bound, small,
):
    """Review of aabd67f653: the bounds were charged for candidates whose
    redacted text matched, so page_basis / scan_cap told whether it did.
    Under a redacting policy (R2) they charge BM25 rows or scope rows."""
    deployment = _redacting_bound(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", small)
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 2)
    for secret, control, value in (
        # The reviewer's repro at its bound of three.
        ("dunlin re:/xyz|SECRET42/", "dunlin re:/xyz|NOPE9999/", 3),
        ("dunlin re:/xyz.?|SECRET4./", "dunlin re:/xyz.?|NOPE999./", 2),
        ("re:/xyz.?|SECRET4./", "re:/xyz.?|NOPE999./", 2),
    ):
        monkeypatch.setattr(grep_search, bound, value)
        page = _grep(clean_runtime, deployment, secret, subject=READER)
        _same(page, _grep(clean_runtime, deployment, control, subject=READER))
        assert "note:m" not in _ids(page)
        assert "redaction" in page["count_basis_reasons"]


def _id_order_policy(tmp_path, dsn: str, kind: str, secret: bool) -> str:
    # Eight visible rows; the two matches sort last by node id.
    groups = {
        OPEN: [_node(f"note:a{i}", "note", f"row {i} nothing") for i in range(6)]
        + [_node("note:z1", "note", "ticket TCKX-1"), _node("note:z2", "note", "ticket TCKX-2")],
    }
    hidden_source = SECRET if kind == "hide" else MASKED
    groups[hidden_source] = [
        _node("note:b0", "note", "ticket TCKX-9 secret" if secret else "ticket none here"),
    ]
    deployment = _publish(tmp_path, dsn, [], groups)
    rules = [_allow(OPEN), _allow(MAIN)]
    if kind == "redact":
        rules.append({
            "rule_id": "redact_masked", "effect": "redact", "actions": ["graph.read.mcp"],
            "subjects": [READER], "match": {"source_id": MASKED}, "priority": 100,
            "obligations": [{"type": "redaction"}, {"type": "audit"}],
        })
    _activate(dsn, deployment, rules)
    if kind == "redact":
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(
                "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
                " || '{\"pii_classes\": {\"text\": \"free_text\"}}'::jsonb WHERE name = 'note'"
            )
    return deployment


@pytest.mark.parametrize("secret", [True, False], ids=["secret", "control"])
@pytest.mark.parametrize("kind", ["hide", "redact"])
def test_the_node_id_path_is_no_oracle_on_hidden_or_redacted_text(
    clean_runtime, tmp_path, monkeypatch, kind, secret,
):
    """Review of 46db226aa1 (F1): the node-id path chose between reading the
    match set whole and walking it by how many stored rows matched, hidden
    and redacted ones included. The secret case and its control differ only
    in a hidden or masked row's text, and must give the same page: the walk
    of four scope rows, which finds neither visible match and says so. On
    46db226aa1 the control read its two matches whole instead."""
    deployment = _id_order_policy(tmp_path, clean_runtime, kind, secret)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", 2)
    monkeypatch.setattr(grep_search, "PAGE_ID_ROWS_R2", 4)
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 2)
    page = _grep(clean_runtime, deployment, r"re:/TCKX-\d/", subject=READER)
    assert page["results"] == []
    assert page["page_basis"] == "scan_bound_reached"
    assert page["page_scan_bound_hit"] == "id_rows"
    assert page["next_offset"] is None


# ---------- review of 46db226aa1: bounded work, real bound hits ----------

def _statements_and_decisions(monkeypatch) -> dict[str, int]:
    seen = {"statements": 0, "decided": 0, "batches": 0}
    real_b, real_bw, real_scan = (
        grep_search._bounded, grep_search._bounded_with, grep_search._scan_visible,
    )

    def bounded(conn, sql, params):
        seen["statements"] += 1
        return real_bw(conn, sql, params, settings=())

    def bounded_with(conn, sql, params, **kwargs):
        seen["statements"] += 1
        return real_bw(conn, sql, params, **kwargs)

    def scan_visible(conn, session, rows, **kwargs):
        seen["decided"] += len(rows)
        seen["batches"] += 1
        return real_scan(conn, session, rows, **kwargs)

    del real_b
    monkeypatch.setattr(grep_search, "_bounded", bounded)
    monkeypatch.setattr(grep_search, "_bounded_with", bounded_with)
    monkeypatch.setattr(grep_search, "_scan_visible", scan_visible)
    return seen


@pytest.mark.parametrize("regime", ["R1", "R2"])
def test_a_rare_attr_filter_over_a_common_word_stays_bounded(
    clean_runtime, tmp_path, monkeypatch, regime,
):
    """F2: attr_filters were applied only at the scan site, so the count
    decided every stored match of the word. They are now in the SQL
    predicate; the work is a handful of statements and one decided row."""
    facts = [
        _node(f"note:{i:03d}", "note", f"common word {i}", owner="bob" if i == 7 else "alice")
        for i in range(300)
    ]
    if regime == "R1":
        deployment = _publish(tmp_path, clean_runtime, facts)
    else:
        deployment = _publish(tmp_path, clean_runtime, [], {OPEN: facts})
        _activate(clean_runtime, deployment, [_allow(OPEN), _allow(MAIN)])
        monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    seen = _statements_and_decisions(monkeypatch)
    kwargs = {"subject": READER} if regime == "R2" else {}
    for query in ("common", "re:/comm.n/"):
        seen.update(statements=0, decided=0)
        page = _grep(clean_runtime, deployment, query,
                     filter={"subtype": "note", "attr_filters": {"owner": "bob"}}, **kwargs)
        assert _ids(page) == ["note:007"]
        assert page["total_count"] == 1
        assert seen["decided"] <= 2, seen
        assert seen["statements"] <= 20, seen


def test_bounds_met_exactly_are_not_reported_as_hit(clean_runtime, tmp_path, monkeypatch):
    """F4: a bound is reported only when a further row exists past it."""
    facts = [_node(f"note:{i:02d}", "note", f"egret TCK-{100 + i}") for i in range(7)]
    deployment = _publish(tmp_path, clean_runtime, facts)
    # Seven candidates, a candidate bound of seven.
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 7)
    page = _grep(clean_runtime, deployment, "egret", limit=10)
    assert len(_ids(page)) == 7 and page["page_basis"] == "top_of_all_visible_matches"
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 2_000)
    # Seven BM25 rows, an index-row bound of seven.
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", 0)
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 7)
    monkeypatch.setattr(grep_search, "PAGE_INDEX_ROWS", 7)
    page = _grep(clean_runtime, deployment, "egret", limit=10)
    assert len(_ids(page)) == 7 and page["page_basis"] == "top_of_all_visible_matches"
    # A scope of seven rows, an id-row bound of seven (the walk).
    monkeypatch.setattr(grep_search, "PAGE_ID_ROWS", 7)
    page = _grep(clean_runtime, deployment, r"re:/TCK-\d+/", limit=10)
    assert len(_ids(page)) == 7 and page["page_basis"] == "top_of_all_visible_matches"
    # One row fewer allowed is a real hit: a further scope row exists.
    monkeypatch.setattr(grep_search, "PAGE_ID_ROWS", 6)
    page = _grep(clean_runtime, deployment, r"re:/TCK-\d+/", limit=10)
    assert page["page_basis"] == "scan_bound_reached"
    assert page["page_scan_bound_hit"] == "id_rows"


def test_the_count_counts_the_records_the_page_lists(clean_runtime, tmp_path):
    """Lane P on 46db226aa1: "0 of 13" over an empty page, and "1-1 of 2"
    for one record whose passage also matched. The count now uses the
    page's predicate and folds passages into records as the page does."""
    facts = [
        _node("doc:x", "doc", "type limit autoscaler in the record text"),
        _node("chunk:x:0", "chunk", "type limit autoscaler in its passage"),
        _edge("doc:x", "chunk:x:0"),
        # A passage with no declared parent: dropped by a subtype filter.
        _node("chunk:orphan", "chunk", "type limit autoscaler without a record"),
        _node("note:n", "note", "type and limit but not the third word"),
    ]
    deployment = _publish(tmp_path, clean_runtime, facts)
    query = '"type" "limit" "autoscaler"'
    page = _grep(clean_runtime, deployment, query)
    # doc:x (its own text and its passage) and the unfolded orphan passage.
    assert sorted(_ids(page)) == ["chunk:orphan", "doc:x"]
    assert page["total_count"] == 2 and page["count_unit"] == "record"
    scoped = _grep(clean_runtime, deployment, query, filter={"subtype": "doc"})
    assert _ids(scoped) == ["doc:x"]
    assert scoped["total_count"] == 1 and scoped["total_count_basis"] == "exact"
    none = _grep(clean_runtime, deployment, '"type" "limit" "autoscaler" "provider"',
                 filter={"subtype": "doc"})
    assert none["results"] == [] and none["total_count"] == 0


def test_the_index_row_bound_counts_global_positions(clean_runtime, tmp_path, monkeypatch):
    """pg_textsearch returns at most 100,000 results per index scan and any
    other condition is a filter after it, so the windows count GLOBAL index
    positions: out-of-scope matches ranking first use up the bound, and the
    page says so rather than claiming it saw every match."""
    facts = [_node(f"chunk:{i:03d}", "chunk", "heron") for i in range(30)]
    facts += [_node(f"note:{i:02d}", "note", "heron " + "pad " * (5 + i)) for i in range(5)]
    deployment = _publish(tmp_path, clean_runtime, facts)
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", 0)
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 4)
    monkeypatch.setattr(grep_search, "PAGE_INDEX_ROWS", 20)
    page = _grep(clean_runtime, deployment, "heron", filter={"subtype": "note"})
    # Twenty global positions, all chunks: no note was reached.
    assert page["results"] == []
    assert page["page_basis"] == "scan_bound_reached"
    assert page["page_scan_bound_hit"] == "index_rows"
    monkeypatch.setattr(grep_search, "PAGE_INDEX_ROWS", 50_000)
    whole = _grep(clean_runtime, deployment, "heron", filter={"subtype": "note"})
    assert _ids(whole) == [f"note:{i:02d}" for i in range(5)]
    assert whole["page_basis"] == "top_of_all_visible_matches"
    # Every window ends below pg_textsearch's own per-scan limit.
    assert grep_search.PAGE_INDEX_ROWS < grep_search.PG_TEXTSEARCH_SCAN_LIMIT == 100_000


# ---------- review of bdf798d229 ----------

def _activate_default(dsn, deployment, default, rules):
    import uuid

    from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph

    bundle = compile_policy_graph({
        "policy_bundle_id": f"nomos:test:rev3:{uuid.uuid4().hex[:8]}",
        "policy_version": "v1", "default_effect": default, "rules": rules,
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn, deployment_name=deployment, bundle=bundle,
            scope=f"deployment:{deployment}", activated_by="test:operator",
        )
        conn.commit()


@pytest.mark.parametrize("secret", [True, False], ids=["secret", "control"])
def test_no_active_bundle_under_enforce_is_r2(clean_runtime, tmp_path, monkeypatch, secret):
    """Finding 1: with no active bundle the runtime denies every row; the
    count must not count what the page cannot show (R1 used to)."""
    groups = {
        OPEN: [_node("note:o1", "note", "sanderling on the beach")],
        SECRET: [_node("note:s1", "note",
                       "sanderling quasar7 secret" if secret else "sanderling nothing")],
    }
    deployment = _publish(tmp_path, clean_runtime, [], groups)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    page = _grep(clean_runtime, deployment, "sanderling quasar7", subject=READER)
    assert page["results"] == []
    assert page["total_count"] == 0


@pytest.mark.parametrize("secret", [True, False], ids=["secret", "control"])
def test_an_invalid_identity_fact_under_an_allow_only_bundle_is_r2(
    clean_runtime, tmp_path, monkeypatch, secret,
):
    """Finding 1: a whitespace classification is an invalid identity fact,
    hidden whatever the bundle's rules say."""
    groups = {
        OPEN: [
            _node("note:o1", "note", "sanderling on the beach"),
            _node("note:s1", "note",
                  "sanderling quasar7 secret" if secret else "sanderling nothing",
                  classification="   "),
        ],
    }
    deployment = _publish(tmp_path, clean_runtime, [], groups)
    _activate_default(clean_runtime, deployment, "allow", [_allow(OPEN), _allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    page = _grep(clean_runtime, deployment, "sanderling quasar7", subject=READER)
    assert "note:s1" not in _ids(page)
    assert page["total_count"] == 0


@pytest.mark.parametrize("secret", [True, False], ids=["secret", "control"])
def test_hidden_rows_under_an_allow_only_bundle_never_fill_the_bound(
    clean_runtime, tmp_path, monkeypatch, secret,
):
    """Finding 1: two hidden rows (invalid identity fact) sort before the
    visible match; whether they match must not decide the page."""
    text = "ticket TCKX-9 secret" if secret else "ticket none"
    groups = {
        OPEN: [
            _node("note:a1", "note", text, classification="   "),
            _node("note:a2", "note", text, classification="   "),
            _node("note:z1", "note", "ticket TCKX-1 open"),
        ],
    }
    deployment = _publish(tmp_path, clean_runtime, [], groups)
    _activate_default(clean_runtime, deployment, "allow", [_allow(OPEN), _allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 2)
    page = _grep(clean_runtime, deployment, r"re:/TCKX-\d/", subject=READER)
    assert _ids(page) == ["note:z1"]
    assert page["page_basis"] == "top_of_all_visible_matches"
    assert page["total_count"] == 1


def test_a_passage_with_two_edges_to_one_record_counts_once(clean_runtime, tmp_path):
    groups = {
        OPEN: [
            _node("doc:p", "doc", "kestrel parent text"),
            _node("chunk:c", "chunk", "kestrel chunk text"),
        ],
    }
    e1 = _edge("doc:p", "chunk:c")
    e2 = type(e1)(src="doc:p", dst="chunk:c", edge_type="contains",
                  source_record_id={"id": "dup"}, source_revision={"v": 2})
    deployment = _publish(tmp_path, clean_runtime, [e1, e2], groups)
    page = _grep(clean_runtime, deployment, "kestrel")
    assert _ids(page) == ["doc:p"] and page["total_count"] == 1


def _falcon_corpus():
    return [
        _node("doc:a", "doc", "falcon one alpha", path="app/a.py", owner="alice"),
        _node("chunk:a0", "chunk", "falcon chunk zero", chunk_ordinal=0),
        _node("chunk:a1", "chunk", "falcon other text", chunk_ordinal=1),
        _edge("doc:a", "chunk:a0"), _edge("doc:a", "chunk:a1"),
        _node("doc:b", "doc", "nothing here", path="manual/b.md", owner="bob"),
        _node("chunk:b0", "chunk", "falcon bee chunk", chunk_ordinal=0),
        _edge("doc:b", "chunk:b0"),
        _node("chunk:o", "chunk", "falcon orphan chunk", chunk_ordinal=0),
        _node("chunk:m", "chunk", "falcon multi chunk", chunk_ordinal=0),
        _edge("doc:a", "chunk:m"), _edge("doc:b", "chunk:m"),
        _node("note:n", "note", "falcon note words", owner="alice"),
        _node("doc:c", "doc", "falcon near the beach", path="manual/c.md", owner="alice"),
    ]


_COUNT_CASES = [
    ("falcon", {}),
    ("falcon -other", {}),
    ('"falcon chunk"', {}),
    ("falcon path:app", {}),
    ("falcon -path:app", {}),
    ("falcon", {"filter": {"subtype": "doc"}}),
    ("falcon", {"filter": {"subtype": "chunk"}}),
    ("falcon", {"filter": {"attr_filters": {"owner": "alice"}}}),
    ("falcon", {"filter": {"attr_filters": {"owner": "bob"}}}),
    ("falcon", {"filter": {"subtype": "doc", "attr_filters": {"owner": "bob"}}}),
    ("falcon NEAR/2 chunk", {}),
    (r"re:/falcon (one|bee)/", {}),
]


def test_the_count_equals_the_records_every_page_lists(clean_runtime, tmp_path):
    """Review of bdf798d229: the count against the pages, one record a page."""
    deployment = _publish(tmp_path, clean_runtime, _falcon_corpus())
    bad = []
    for query, kwargs in _COUNT_CASES:
        ids, offset, first = [], 0, None
        while offset is not None:
            page = _grep(clean_runtime, deployment, query, limit=1, offset=offset, **kwargs)
            first = first or page
            ids += _ids(page)
            offset = page["next_offset"]
        whole = _grep(clean_runtime, deployment, query, limit=50, **kwargs)
        if (first["total_count"] != len(ids) or len(set(ids)) != len(ids)
                or sorted(ids) != sorted(_ids(whole))):
            bad.append((query, kwargs, first["total_count"], ids, _ids(whole)))
    assert not bad, bad


def test_attr_filters_drop_an_unfolded_passage_that_fails_them(clean_runtime, tmp_path):
    """Finding 2 (search-result-shape rule 4): a passage that does not fold
    is listed only if it passes the selections itself, as in lexical."""
    from okg.substrate.mcp import open_session, search

    deployment = _publish(tmp_path, clean_runtime, _falcon_corpus())
    page = _grep(clean_runtime, deployment, "falcon", filter={"attr_filters": {"owner": "bob"}})
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        lexical = search(conn, session, "falcon", method="lexical",
                         filter={"attr_filters": {"owner": "bob"}})
    assert _ids(page) == ["doc:b"] == [r["node_id"] for r in lexical["results"]]
    assert page["total_count"] == 1


@pytest.mark.parametrize("bounds", [
    {"PAGE_ID_ROWS_R2": 1, "PAGE_FIRST_WINDOW": 1},
    {"COUNT_SCOPE_ROWS": 1, "PAGE_FIRST_WINDOW": 1},
    {"PAGE_SCAN_BOUND": 1, "PAGE_FIRST_WINDOW": 1},
    {"PAGE_INDEX_ROWS": 2, "PAGE_FIRST_WINDOW": 1},
    {},
])
@pytest.mark.parametrize("query", ["launch", "re:/launch (notes|memo|checklist)/", "launch -zzz"])
@pytest.mark.parametrize("subtype", [None, "note"])
def test_a_masked_attr_filter_is_no_oracle_at_small_bounds(
    clean_runtime, tmp_path, monkeypatch, bounds, query, subtype,
):
    from tests.substrate.mcp.test_search_grep import _redacting_deployment

    deployment = _redacting_deployment(tmp_path, clean_runtime, ATTR_PII)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    for name, value in bounds.items():
        monkeypatch.setattr(grep_search, name, value)
    selected = {"subtype": subtype} if subtype else {}
    secret = _grep(clean_runtime, deployment, query, subject=READER,
                   filter={**selected, "attr_filters": {"owner": "alice"}})
    control = _grep(clean_runtime, deployment, query, subject=READER,
                    filter={**selected, "attr_filters": {"owner": "bob"}})
    _same(secret, control)


def test_r2_work_over_hidden_matches_is_bounded_by_scope_rows(
    clean_runtime, tmp_path, monkeypatch,
):
    """Finding 3: 10,000 hidden matches before the one visible. R2 charges
    every walked scope row against `PAGE_ID_ROWS_R2`, so the regex walk stops
    at that bound. The word query charges only admitted units against
    `PAGE_SCAN_BOUND` (#3384), so it reads on through the hidden matches,
    bounded by the index-row bound, and finds the visible one; its walk for
    matches no word reaches then stops at the scope-row bound. Each stored
    match is decided once, plus the count's scope walk, and batches that
    admit nothing double (`PAGE_BATCH_DRY_MAX`), so the 10,000 hidden rows
    take a few dozen scan-site round trips, not one per 32 rows. A work
    test: no clock."""
    groups = {
        SECRET: [_node(f"note:a{i:06d}", "note", f"ticket TCKX-{i} hidden") for i in range(10_000)],
        OPEN: [_node("note:z1", "note", "ticket TCKX-1 open")],
    }
    deployment = _publish(tmp_path, clean_runtime, [], groups)
    _activate(clean_runtime, deployment, [_allow(OPEN), _allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    seen = _statements_and_decisions(monkeypatch)
    limits = {
        r"re:/TCKX-\d+/": grep_search.PAGE_ID_ROWS_R2 + grep_search.COUNT_SCOPE_ROWS,
        "ticket": 10_001 + grep_search.COUNT_SCOPE_ROWS,
    }
    for query, limit in limits.items():
        seen.update(statements=0, decided=0, batches=0)
        page = _grep(clean_runtime, deployment, query, subject=READER,
                     filter={"subtype": "note"})
        assert "note:a000001" not in str(page)
        assert seen["decided"] <= limit, (query, seen)
        assert seen["statements"] <= 40, (query, seen)
        assert seen["batches"] <= 40, (query, seen)
        assert page["page_basis"] == "scan_bound_reached"
        if query == "ticket":
            # The hidden matches never fill the candidate bound; the visible
            # one is found, and the walk for matches no word reaches stops
            # at the scope-row bound.
            assert _ids(page) == ["note:z1"], page
            assert page["page_scan_bound_hit"] != "candidate_rows", page


@pytest.mark.parametrize("bounds,query", [
    ({"PAGE_ID_ROWS_R2": 3, "PAGE_FIRST_WINDOW": 1}, r"re:/TCKX-\d/"),
    ({"PAGE_SCAN_BOUND": 3, "PAGE_FIRST_WINDOW": 1}, "ticket"),
    ({"COUNT_SCOPE_ROWS": 3, "PAGE_FIRST_WINDOW": 1}, "ticket"),
    ({"COUNT_SCOPE_ROWS": 3, "PAGE_FIRST_WINDOW": 1}, r"re:/TCKX-\d/"),
])
def test_a_hidden_rows_stored_path_never_moves_an_r2_bound(
    clean_runtime, tmp_path, monkeypatch, bounds, query,
):
    """Profile path exclusions are in the predicate, not the R2 scope: hidden
    rows whose stored path the profile excludes, or not, give the same page,
    count and bounds (review of 3365125666, probe P1)."""
    from okg.substrate.mcp.search_profiles import ResolvedScope

    deployments = {}
    for excluded in (True, False):
        groups = {
            SECRET: [
                _node(f"note:a{i}", "note", f"ticket TCKX-{i} hidden",
                      path=("vault/x.md" if excluded else "public/x.md"))
                for i in range(4)
            ],
            OPEN: [_node("note:z1", "note", "ticket TCKX-1 open", path="public/z.md")],
        }
        deployments[excluded] = _publish(tmp_path / str(excluded), clean_runtime, [], groups)
        _activate(clean_runtime, deployments[excluded], [_allow(OPEN), _allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    for name, value in bounds.items():
        monkeypatch.setattr(grep_search, name, value)
    results = {
        excluded: _grep(
            clean_runtime, deployment, query, subject=READER,
            scope=ResolvedScope(excluded_path_globs=("vault/**",)),
        )
        for excluded, deployment in deployments.items()
    }
    _same(results[True], results[False])


# ---------- decision 260a: audit counts like Nomos off; enforce continues ----------

def _phrase_corpus() -> dict:
    """Records whose BM25 words match "provider rollback" while the phrase
    fails ("rollback provider"), interleaved with records the phrase
    matches; scores fall with each record's padding. doc:d folds two
    matching chunks, one near the top and one near the end."""
    nodes = []
    for i in range(12):
        pad = " filler" * i
        if i % 3 == 0:
            nodes.append(_node(f"note:m{i:02d}", "note", f"provider rollback{pad}"))
        else:
            nodes.append(_node(f"note:a{i:02d}", "note", f"rollback provider{pad}"))
    nodes += [
        _node("doc:d", "doc", "doc text" + " filler" * 20),
        _node("chunk:d:0", "chunk", "provider rollback filler"),
        _node("chunk:d:1", "chunk", "provider rollback" + " filler" * 10),
    ]
    return {OPEN: nodes}


def _phrase_deployment(tmp_path, dsn: str) -> str:
    deployment = _publish(
        tmp_path, dsn, [_edge("doc:d", "chunk:d:0"), _edge("doc:d", "chunk:d:1")],
        _phrase_corpus(),
    )
    _activate(dsn, deployment, [_allow(OPEN), _allow(MAIN)])
    return deployment


PHRASE = '"provider rollback"'
PHRASE_ORDER = ["note:m00", "doc:d", "note:m03", "note:m06", "note:m09"]


def _body(token: str) -> dict:
    import base64
    import json
    import zlib

    body = token.split(".", 1)[0]
    return json.loads(zlib.decompress(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4))))


def _cursor(token: str) -> dict:
    from okg.substrate.mcp.continuation import unseal_cursor

    return unseal_cursor(_body(token)["cursor"]["sealed"])


def _chain(dsn: str, deployment: str, query: str, **kwargs) -> tuple[list[str], list[dict]]:
    """Every record a first page and the pages after it list, following
    `next_offset` and then `continuation_token`, and the pages."""
    page = _grep(dsn, deployment, query, subject=READER, **kwargs)
    pages = [page]
    kwargs.pop("offset", None)
    while len(pages) < 50:
        if page.get("continuation_token"):
            follow = {"continuation_token": page["continuation_token"]}
        elif page.get("next_offset") is not None:
            follow = {"offset": page["next_offset"]}
        else:
            break
        page = _grep(dsn, deployment, query, subject=READER, **follow, **kwargs)
        pages.append(page)
    return [i for p in pages for i in _ids(p)], pages


@pytest.mark.parametrize("small_set", [2_000, 0], ids=["match_set", "windows"])
def test_audit_charges_only_rows_the_predicate_passes(
    clean_runtime, tmp_path, monkeypatch, small_set,
):
    """Decision 260a, #3384: under Nomos `audit` and `enforce` the page scan
    bound charges only the units the scan site admits, so BM25 rows the
    phrase fails never fill it, in either regime (before #3384 `enforce`
    charged every in-scope BM25 row and stopped this page at the bound)."""
    deployment = _phrase_deployment(tmp_path, clean_runtime)
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", small_set)
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 6)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    audit = _grep(clean_runtime, deployment, PHRASE, subject=READER)
    assert _ids(audit) == PHRASE_ORDER
    assert audit["page_basis"] == "top_of_all_visible_matches"
    assert audit["total_count"] == 5 and audit["total_count_basis"] == "exact"
    assert "continuation_token" not in audit
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    enforce = _grep(clean_runtime, deployment, PHRASE, subject=READER)
    assert _ids(enforce) == PHRASE_ORDER
    assert enforce["page_basis"] == "top_of_all_visible_matches"
    assert "continuation_token" not in enforce
    # Failure case: a bound below the admitted units still stops the page.
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 2)
    for mode in ("audit", "1"):
        monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
        bounded = _grep(clean_runtime, deployment, PHRASE, subject=READER)
        assert bounded["page_basis"] == "scan_bound_reached", mode
        assert bounded["page_scan_bound_hit"] == "candidate_rows", mode
        assert bounded["continuation_token"], mode


@pytest.mark.parametrize("window", [1, 1_000])
@pytest.mark.parametrize("limit", [1, 2, 10])
def test_enforce_continuations_list_every_record_once_in_order(
    clean_runtime, tmp_path, monkeypatch, window, limit,
):
    """Decision 260a: a page that stops at a bound under `enforce` gives a
    continuation token; following it lists the unbounded order, no repeat,
    no gap (doc:d's second chunk never lists it again), and no page reads
    as "0 of 0+"."""
    from okg.substrate.mcp.result_text import render

    deployment = _phrase_deployment(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", window)
    whole = _grep(clean_runtime, deployment, PHRASE, subject=READER)
    assert _ids(whole) == PHRASE_ORDER
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 3)
    ids, pages = _chain(clean_runtime, deployment, PHRASE, limit=limit)
    assert ids == PHRASE_ORDER
    assert len(pages) > 1
    seen = 0
    for number, page in enumerate(pages):
        status = render(page, tool="search").split("\n")
        assert " of 0+" not in status[0]
        if not page["results"]:
            # An empty page only says the bound was reached, or ends the chain.
            assert (
                "bound reached, no records on this page" in status[0]
                or number == len(pages) - 1
            )
        assert page["offset"] == seen
        seen += len(_ids(page))
        text = "\n".join(status)
        if page.get("page_basis") == "scan_bound_reached":
            assert "! bound reached (candidate_rows 3)" in text
        if page.get("continuation_token"):
            assert page["next_offset"] is None
    assert not pages[-1].get("continuation_token")


def test_an_offset_page_past_the_bound_continues_where_its_scan_stopped(
    clean_runtime, tmp_path, monkeypatch,
):
    from okg.substrate.mcp.result_text import render

    deployment = _phrase_deployment(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 3)
    deep = _grep(clean_runtime, deployment, PHRASE, subject=READER, limit=2, offset=4)
    assert deep["results"] == [] and deep["page_basis"] == "scan_bound_reached"
    text = render(deep, tool="search")
    assert "bound reached, no records on this page" in text.split("\n")[0]
    # `search` takes no `next`: the ending names the argument the token goes
    # back as, so following the status line works.
    assert (
        f"next: continuation_token={deep['continuation_token']}"
        in text.split("\n")[0]
    )
    found = _cursor(deep["continuation_token"])["listed"]
    ids, _pages = _chain(clean_runtime, deployment, PHRASE, limit=2, offset=4)
    # The continuation starts after the records the offset page's scan
    # reached, so nothing between them and the rest is skipped.
    assert found + ids == PHRASE_ORDER


def test_a_regex_only_walk_continues_under_enforce(clean_runtime, tmp_path, monkeypatch):
    groups = {OPEN: [
        _node(f"note:{i:02d}", "note", f"ticket TCKX-{i}" if i % 2 else "ticket none")
        for i in range(10)
    ]}
    deployment = _publish(tmp_path, clean_runtime, [], groups)
    _activate(clean_runtime, deployment, [_allow(OPEN), _allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 1)
    monkeypatch.setattr(grep_search, "PAGE_ID_ROWS_R2", 3)
    ids, pages = _chain(clean_runtime, deployment, r"re:/TCKX-\d/", limit=2)
    assert ids == [f"note:{i:02d}" for i in range(1, 10, 2)]
    assert any(p.get("page_scan_bound_hit") == "id_rows" for p in pages)


@pytest.mark.parametrize("query", [r"re:/TCKX-\d/", "ticket"])
def test_a_continuation_chain_is_the_same_whether_hidden_rows_match(
    clean_runtime, tmp_path, monkeypatch, query,
):
    """Under `enforce`, hidden rows that match the query or not (beyond the
    BM25 words, which every one holds) give the same pages along the whole
    chain; the token's cursor, whose resume point
    counts hidden rows (review of 13aeec6221), is sealed: the readable token
    body holds the binding and an opaque `sealed` value only."""
    chains = {}
    for secret in (True, False):
        monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
        groups = {
            SECRET: [
                _node(f"note:a{i}", "note", f"ticket TCKX-{i} hidden" if secret else "ticket none")
                for i in range(4)
            ],
            OPEN: [_node(f"note:z{i}", "note", f"ticket TCKX-{i} open") for i in range(3)],
        }
        deployment = _publish(tmp_path / str(secret), clean_runtime, [], groups)
        _activate(clean_runtime, deployment, [_allow(OPEN), _allow(MAIN)])
        monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
        monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 1)
        monkeypatch.setattr(grep_search, "PAGE_ID_ROWS_R2", 3)
        monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 3)
        ids, pages = _chain(clean_runtime, deployment, query, limit=1)
        assert ids == ["note:z0", "note:z1", "note:z2"]
        for page in pages:
            if page.get("continuation_token"):
                body = _body(page["continuation_token"])
                assert set(body["cursor"]) == {"sealed"}
                assert "note:z" not in body["cursor"]["sealed"]
        chains[secret] = [
            # parent_relation names each deployment's own catalog.
            {k: v for k, v in p.items() if k not in (
                "generation_id", "pinned_generation_id", "parent_relation",
                "continuation_token", "results",
            )}
            | {"ids": _ids(p)}
            for p in pages
        ]
    assert len(chains[True]) == len(chains[False])
    for secret_page, control_page in zip(chains[True], chains[False]):
        differ = {k for k in secret_page.keys() | control_page.keys()
                  if secret_page.get(k) != control_page.get(k)}
        assert not differ, (differ, secret_page, control_page)


def _sparse_notes(n: int, every: int, text: str, other: str) -> dict:
    """`n` notes, every `every`-th with `text`, the rest with `other`; each
    padded by its index so no two BM25 scores tie."""
    return {OPEN: [
        _node(f"note:{i:03d}", "note", (text if i % every == 0 else other) + " pad" * i)
        for i in range(n)
    ]}


@pytest.mark.parametrize("enforce", [False, True], ids=["off", "enforce"])
def test_a_chain_reads_on_past_an_index_row_stop(
    clean_runtime, tmp_path, monkeypatch, enforce,
):
    """Review of 13aeec6221, HIGH 1: after an `index_rows` stop the next call
    used to re-read the same rows and end with `no_progress`. The sequence
    now counts every row read."""
    groups = _sparse_notes(
        80, 10, "provider rollback note", "rollback provider note",
    )
    groups[OPEN] += [
        _node(f"doc:{i}", "doc", "provider rollback doc" + " pad" * i) for i in range(3)
    ]
    deployment = _publish(tmp_path, clean_runtime, [], groups)
    _activate(clean_runtime, deployment, [_allow(OPEN), _allow(MAIN)])
    if enforce:
        monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", 0)
    expected = _ids(_grep(clean_runtime, deployment, PHRASE, subject=READER,
                          filter={"subtype": "note"}, limit=50))
    assert expected == [f"note:{i:03d}" for i in range(0, 80, 10)]
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 5)
    monkeypatch.setattr(grep_search, "PAGE_INDEX_ROWS", 10)
    ids, pages = _chain(clean_runtime, deployment, PHRASE, filter={"subtype": "note"}, limit=3)
    assert ids == expected
    assert any(p.get("page_scan_bound_hit") == "index_rows" for p in pages)
    assert not any(p.get("continuation_withheld") for p in pages)


@pytest.mark.parametrize("mode", ["off", "audit"])
def test_an_r1_chain_reads_on_past_an_id_row_stop(clean_runtime, tmp_path, monkeypatch, mode):
    """Review of 13aeec6221, HIGH 1: an R1 walk feeds only matching rows; the
    sequence counts every walked row, so the next call starts past them."""
    deployment = _publish(tmp_path, clean_runtime, [], _sparse_notes(
        20, 6, "ticket TCKX-1", "ticket none",
    ))
    _activate(clean_runtime, deployment, [_allow(OPEN), _allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", 0)
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 1)
    monkeypatch.setattr(grep_search, "PAGE_ID_ROWS", 4)
    ids, pages = _chain(clean_runtime, deployment, r"re:/TCKX-\d+/", limit=2)
    assert ids == [f"note:{i:03d}" for i in range(0, 20, 6)]
    assert any(p.get("page_scan_bound_hit") == "id_rows" for p in pages)


def test_a_timed_out_whole_read_stays_timed_out_after_a_skipping_read(
    clean_runtime, tmp_path, monkeypatch,
):
    """Review of 13aeec6221, MEDIUM 3: the zero-score pass's own timed-out
    read (with units skipped) used to clear the flag the first read set."""
    groups = {OPEN: [_node(f"note:{i:02d}", "note", f"ticket TCKX-{i}") for i in range(4)]}
    deployment = _publish(tmp_path, clean_runtime, [], groups)
    real = grep_search._bounded

    def bounded(conn, sql, params):
        if params.get("grep_limit") == grep_search.PAGE_SMALL_SET + 1:
            raise grep_search._Cancelled()
        return real(conn, sql, params)

    def count(*args, **kwargs):
        raise AssertionError("the count ran")

    monkeypatch.setattr(grep_search, "_bounded", bounded)
    monkeypatch.setattr(grep_search, "count_grep_matches", count)
    # Words and a regex: the early whole read, then the BM25 windows, then
    # the zero-score pass's read for the regex matches no word reaches.
    page = _grep(clean_runtime, deployment, r"ticket re:/TCKX-\d/")
    assert page["total_count_basis"] == "timed_out"


def test_a_continuation_token_is_bound_to_its_query_and_refuses_an_offset(
    clean_runtime, tmp_path, monkeypatch,
):
    from okg.substrate.mcp.continuation import ContinuationTokenInvalid

    deployment = _phrase_deployment(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 3)
    token = _grep(clean_runtime, deployment, PHRASE, subject=READER)["continuation_token"]
    with pytest.raises(ContinuationTokenInvalid):
        _grep(clean_runtime, deployment, '"rollback provider"', subject=READER,
              continuation_token=token)
    with pytest.raises(ValueError, match="offset and continuation_token"):
        _grep(clean_runtime, deployment, PHRASE, subject=READER,
              continuation_token=token, offset=2)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        from okg.substrate.mcp import open_session, search

        session = open_session(conn, deployment_name=deployment)
        with pytest.raises(ValueError, match="continuation_token applies only"):
            search(conn, session, "provider", method="lexical", continuation_token=token)


def test_a_timed_out_match_set_read_is_not_paid_again_by_the_count(
    clean_runtime, tmp_path, monkeypatch,
):
    """Lane P (10-02): a regex-only R1 page whose unordered match-set read ran
    past the timeout used to pay it again in the count, which reads the same
    match set further. The count now says `timed_out` without running; the
    page still walks the scope."""
    groups = {OPEN: [_node(f"note:{i:02d}", "note", f"ticket TCKX-{i}") for i in range(4)]}
    deployment = _publish(tmp_path, clean_runtime, [], groups)
    real = grep_search._bounded
    calls = {"count": 0}

    def bounded(conn, sql, params):
        if params.get("grep_limit") == grep_search.PAGE_SMALL_SET + 1:
            raise grep_search._Cancelled()
        return real(conn, sql, params)

    def count(*args, **kwargs):
        calls["count"] += 1
        raise AssertionError("the count ran")

    monkeypatch.setattr(grep_search, "_bounded", bounded)
    monkeypatch.setattr(grep_search, "count_grep_matches", count)
    page = _grep(clean_runtime, deployment, r"re:/TCKX-\d/")
    assert _ids(page) == [f"note:{i:02d}" for i in range(4)]
    assert page["total_count"] is None and page["total_count_basis"] == "timed_out"
    assert calls["count"] == 0


def _cancel_whole_reads(monkeypatch, times: int) -> dict:
    """Make the first `times` whole-match-set reads time out."""
    real = grep_search._bounded
    state = {"left": times}

    def bounded(conn, sql, params):
        if params.get("grep_limit") == grep_search.PAGE_SMALL_SET + 1 and state["left"] > 0:
            state["left"] -= 1
            raise grep_search._Cancelled()
        return real(conn, sql, params)

    monkeypatch.setattr(grep_search, "_bounded", bounded)
    return state


def test_a_chain_keeps_its_walk_when_a_later_whole_read_would_succeed(
    clean_runtime, tmp_path, monkeypatch,
):
    """Review round 2 of decision 260a, HIGH: the first call's whole-match-set
    read timed out and it walked; a later call whose read succeeded fed the
    match set, numbered in other units, and skipped most records. The path
    is now pinned in the sealed cursor."""
    deployment = _publish(tmp_path, clean_runtime, [], _sparse_notes(
        20, 6, "ticket TCKX-1", "ticket none",
    ))
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 1)
    monkeypatch.setattr(grep_search, "PAGE_ID_ROWS", 4)
    _cancel_whole_reads(monkeypatch, 1)
    ids, pages = _chain(clean_runtime, deployment, r"re:/TCKX-\d+/", limit=2)
    assert ids == [f"note:{i:03d}" for i in range(0, 20, 6)]
    assert _cursor(pages[0]["continuation_token"])["path"]["id_set"] is False


def test_a_chain_keeps_its_windows_when_a_later_early_read_would_succeed(
    clean_runtime, tmp_path, monkeypatch,
):
    deployment = _publish(tmp_path, clean_runtime, [], _sparse_notes(
        80, 10, "provider rollback note", "rollback provider note",
    ))
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 5)
    monkeypatch.setattr(grep_search, "PAGE_INDEX_ROWS", 10)
    _cancel_whole_reads(monkeypatch, 1)
    counts = []
    real_count = grep_search.count_grep_matches
    monkeypatch.setattr(grep_search, "count_grep_matches",
                        lambda *a, **k: counts.append(1) or real_count(*a, **k))
    ids, pages = _chain(clean_runtime, deployment, PHRASE, limit=3)
    assert ids == [f"note:{i:03d}" for i in range(0, 80, 10)]
    assert _cursor(pages[0]["continuation_token"])["path"]["early"] is False
    # The first call's whole read timed out, so no page of the chain pays
    # the count's timeout again (review round 3, LOW 1).
    assert counts == []
    assert {p["total_count_basis"] for p in pages} == {"timed_out"}


def test_a_pinned_whole_read_that_times_out_on_replay_refuses(
    clean_runtime, tmp_path, monkeypatch,
):
    """A chain that took the whole match set cannot switch to the windows
    when that read times out later: the call refuses as timed out."""
    deployment = _publish(tmp_path, clean_runtime, [], _sparse_notes(
        12, 1, "provider rollback note", "unused",
    ))
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 3)
    first = _grep(clean_runtime, deployment, PHRASE, subject=READER, limit=5)
    assert first["page_scan_bound_hit"] == "candidate_rows"
    assert _cursor(first["continuation_token"])["path"]["early"] is True
    _cancel_whole_reads(monkeypatch, 1)
    with pytest.raises(grep_search.GrepTimeout):
        _grep(clean_runtime, deployment, PHRASE, subject=READER,
              continuation_token=first["continuation_token"])
