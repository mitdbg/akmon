"""pg_textsearch BM25 lexical node search (PACT restore-bm25-lexical-node-search).

Covers the requirements the console-composer / context-budget tests do not:
the maintained index is created on the chronos node segment (pin-agnostic, all
non-deleted), lexical search fails LOUD when the index is absent (no silent scan
fallback), and a no-match query returns nothing (BM25 does not pad with 0-score
non-matches)."""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.bm25_node_search import (
    BM25_TEXT_CONFIG,
    SearchIndexUnavailable,
    bm25_match_sql,
    branch_point,
    resolve_node_text_bm25_index,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.passage_rollup import ranked_chunk_sql
from okg.substrate.mcp.search import INDEX_SCAN_CAP_MARK
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


def _seed(conn: psycopg.Connection, *, count: int = 5) -> None:
    seed_catalog_version(conn)
    store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
    store.apply_facts(
        [
            NodeFact(
                node_id=f"n:bm25:{idx}",
                subtype="pact_change",
                attrs={"text": f"alpha bravo charlie item {idx}"},
                source_record_id={"id": f"n:bm25:{idx}"},
                source_revision={"v": 1},
            )
            for idx in range(count)
        ],
        source="default.bm25_test",
        strict_endpoints=True,
    )


def test_ensure_creates_pin_agnostic_bm25_index_on_segment(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn, ensure_schema=True)
        _segment, index_name = resolve_node_text_bm25_index(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT indexdef FROM pg_indexes WHERE indexname = %s", (index_name,)
            )
            indexdef = cur.fetchone()[0]
    assert "USING bm25" in indexdef
    assert "attrs ->> 'text'" in indexdef
    # All non-deleted text rows; visibility is applied at read time, so NO live_hi
    # sentinel is baked into the index (the open-interval value varies per database).
    assert "live_hi" not in indexdef
    assert "attrs ? 'text'" in indexdef


def test_lexical_search_fails_loud_without_index(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
        Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
        session = open_session(conn, deployment_name="default")
        _segment, index_name = resolve_node_text_bm25_index(conn)
        with conn.cursor() as cur:
            cur.execute(f'DROP INDEX "{index_name}"')
        # P0: no silent fall back to a full scan -- the absent index raises.
        with pytest.raises(SearchIndexUnavailable):
            search(conn, session, "alpha", method="lexical", limit=5)


def test_lexical_search_returns_matches_only(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
        Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
        session = open_session(conn, deployment_name="default")
        hits = search(conn, session, "alpha", method="lexical", completeness="exact", limit=20)
        miss = search(conn, session, "zzqxnotarealword", method="lexical", completeness="exact", limit=20)
    assert hits["total_count"] == 5
    assert {row["node_id"] for row in hits["results"]} == {
        f"n:bm25:{idx}" for idx in range(5)
    }
    # BM25 ORDER BY ... LIMIT pads with 0-score non-matches when matches < limit;
    # the composer filters <@> < 0, so a no-match query yields nothing.
    assert miss["total_count"] == 0
    assert miss["results"] == []


def test_ranked_query_keeps_the_match_test_out_of_the_scan(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The BM25 expression must not be a WHERE predicate of the ranked scan.

    This pins the SHAPE of the statement, because the cost it protects only
    appears at deployment scale and cannot be measured on a five-row fixture.
    Measured on okg-workspace (695,052 records) on 2026-09-22: with the match
    test `(attrs->>'text') <@> to_bm25query(...) < 0` in the ranked query's
    WHERE, the planner stopped choosing the BM25 index entirely and scanned the
    `live_hi` btree computing the score per row -- 697,178 rows removed, 10.2 s,
    and the same 10.2 s for a query with no match, because the cost was the scan
    and not the answer. Moving the test to an outer query over the capped ranked
    page restored the index scan at 9-62 ms (issue #1924).

    A plan assertion is deliberately NOT made here: on a fixture this small the
    planner would seq-scan whatever the SQL says, so an `EXPLAIN` check would
    pass for the wrong reason. `test_lexical_search_returns_matches_only`
    guards the behaviour the outer test preserves.
    """
    seen: list[str] = []
    original = psycopg.Cursor.execute

    def capture(self, query, params=None, **kwargs):
        seen.append(str(query))
        return original(self, query, params, **kwargs)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
        Publisher(
            clean_runtime,
            "default",
            catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
        ).cycle()
        session = open_session(conn, deployment_name="default")
        monkeypatch.setattr(psycopg.Cursor, "execute", capture)
        try:
            search(conn, session, "alpha", method="lexical", limit=20)
        finally:
            monkeypatch.undo()

    ranked = [
        sql for sql in seen
        if "to_bm25query" in sql and "ORDER BY" in sql
        # The index-cap guard's count counts index entries and never serves rows.
        and INDEX_SCAN_CAP_MARK not in sql
    ]
    assert ranked, f"no ranked BM25 statement was issued; saw {len(seen)} statements"
    for sql in ranked:
        flat = " ".join(sql.split())
        inner = flat[flat.index("WHERE"):flat.index("ORDER BY")]
        assert "<@>" not in inner, (
            "the BM25 match test is back in the ranked query's WHERE; at "
            "deployment scale this loses the BM25 index and becomes a "
            "full scan (issue #1924):\n" + flat
        )
        assert "ranked.score < 0" in flat, (
            "the ranked page no longer filters non-matches outside the scan, "
            "so 0-score padding rows can reach the caller:\n" + flat
        )


def test_lexical_honors_multi_generation_pin(clean_runtime: str) -> None:
    """Regression for the cp.ref-vs-branch_point bug.

    After several publishes, the latest pin's checkpoint segment_id is far BELOW the
    live rows' live_lo (segment ids are a small allocator; branch points are huge
    bisection coordinates). A visibility filter that used the segment id directly
    would keep only the gen-0 rows (live_lo = 0) and silently drop the rest -- an
    incomplete result reported as success. The branch-point filter must return all 15
    live nodes across the 3 generations."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        for gen in range(3):
            store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
            store.apply_facts(
                [
                    NodeFact(
                        node_id=f"n:g{gen}:{idx}",
                        subtype="pact_change",
                        attrs={"text": f"delta echo foxtrot gen{gen} item{idx}"},
                        source_record_id={"id": f"n:g{gen}:{idx}"},
                        source_revision={"v": 1},
                    )
                    for idx in range(5)
                ],
                source="default.bm25_multigen",
                strict_endpoints=True,
            )
            Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
        session = open_session(conn, deployment_name="default")
        hits = search(conn, session, "foxtrot", method="lexical", completeness="exact", limit=50)
    # 3 generations x 5 nodes, all live at the latest pin, all carry 'foxtrot'.
    assert hits["total_count"] == 15
    assert {row["node_id"] for row in hits["results"]} == {
        f"n:g{gen}:{idx}" for gen in range(3) for idx in range(5)
    }


def test_rollup_ranked_chunk_scan_keeps_the_match_test_out_of_the_scan() -> None:
    """The rollup's ranked chunk scan must not test `<@>` in its WHERE.

    The sibling test above pins the shape of `_search_lexical`'s statement.
    The passage rollup builds a SECOND ranked BM25 statement, and it carried
    the predicate form this branch removed from the first one; nothing failed
    when it did, because no test read that statement.

    This one needs no database and no rollup declaration, which is the point:
    okg-workspace declares exactly one passage rollup (`document` ->
    `document_chunk`) and the pinned generation holds zero rows of either
    subtype, so the composed path cannot be exercised against the deployment
    at all. The statement can be, and is.

    Measured on okg-workspace 2026-09-22 by running both forms verbatim
    against the 695,052-record `default` branch over the populated
    `code_symbol` subtype at the 2,001-row cap, three queries each: the inner
    predicate sorts on disk (external merge, 9,912 kB, 219,684 shared hits,
    2.82-2.87 s); the outer test sorts in memory (top-N heapsort, 3,883 kB,
    199,983 shared hits, 2.62-2.70 s). All three queries returned identical
    ordered node-id lists, at the saturated cap.
    """
    sql = ranked_chunk_sql(
        node_segment="seg_nodes",
        order_sql="(attrs->>'text') <@> to_bm25query(%(q)s, 'idx')",
        extra_child_where=("source LIKE %(fam)s",),
    )
    flat = " ".join(sql.split())
    inner = flat[flat.index("WHERE"):flat.index("ORDER BY")]
    assert "<@>" not in inner, (
        "the BM25 match test is a predicate of the rollup's ranked chunk "
        "scan; it belongs in an outer query over the capped page "
        "(issue #1924):\n" + flat
    )
    assert "ranked.score < 0" in flat, (
        "the rollup's ranked page no longer drops BM25's 0-score padding "
        "rows, so non-matching chunks can spend the chunk budget:\n" + flat
    )
    # The caller's own selection predicates stay on the chunks: they decide
    # which chunks may spend the budget, which the outer test does not.
    assert "source LIKE %(fam)s" in inner


def _score_one_match_zero(
    conn: psycopg.Connection, needle: str, *, on_search_path: bool = True,
) -> None:
    """Make `<@>` score every text containing `needle` 0 on this connection.

    This is the transient pg_textsearch 1.3.0 result issue #3232 reports: on
    the first read after a publish, while autovacuum's ANALYZE runs, the score
    expression read a document frequency of 0 for a term the document holds
    and scored that real match 0 (9 of 288 first reads). The race cannot be
    timed from a test, so a `<@>` earlier on this connection's search path
    returns 0 for those rows and pg_textsearch's own score for the rest.
    With `on_search_path=False` it is only created, as
    `OPERATOR(okg_test_zero_score.<@>)`.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT n.nspname FROM pg_proc p "
            "JOIN pg_namespace n ON n.oid = p.pronamespace "
            "WHERE p.proname = 'bm25_text_bm25query_score'"
        )
        extension_schema = cur.fetchone()[0]
        cur.execute("CREATE SCHEMA okg_test_zero_score")
        cur.execute(
            "CREATE FUNCTION okg_test_zero_score.score(doc text, q bm25query) "
            "RETURNS double precision LANGUAGE sql AS $$ "
            f"SELECT CASE WHEN strpos(doc, '{needle}') > 0 "
            "THEN 0.0::double precision "
            f"ELSE {extension_schema}.bm25_text_bm25query_score(doc, q) END $$"
        )
        cur.execute(
            "CREATE OPERATOR okg_test_zero_score.<@> ("
            "LEFTARG = text, RIGHTARG = bm25query, "
            "FUNCTION = okg_test_zero_score.score)"
        )
        if not on_search_path:
            return
        cur.execute("SELECT current_setting('search_path')")
        path = cur.fetchone()[0]
        cur.execute(
            "SELECT set_config('search_path', %s, false)",
            (f"okg_test_zero_score, {path}",),
        )


@pytest.mark.parametrize("completeness", ["exact", "bounded"])
def test_lexical_search_keeps_a_match_scored_zero(
    clean_runtime: str, completeness: str,
) -> None:
    """A real match the score expression scores 0 is still returned (#3232).

    Before the fix the ranked read kept only `score < 0`, so this returned 4
    of the 5 matches and said nothing about the fifth.

    Under `completeness="exact"` the short native read is read again by the
    exact scalar statement (PACT lexical-index-scan-cap-3162), whose `<@>`
    resolves to the same zero-scoring operator. That statement keeps and
    counts members, not negative scores, so the count is the exact 5; with
    the sign test it served 4 rows and called 4 exhaustive. Under the bounded
    default the native read serves the 5 rows and the count stays unknown.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
        Publisher(
            clean_runtime,
            "default",
            catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
        ).cycle()
        session = open_session(conn, deployment_name="default")
        _score_one_match_zero(conn, "item 3")
        hits = search(
            conn, session, "alpha", method="lexical", completeness=completeness, limit=20,
        )
        miss = search(
            conn, session, "zzqxnotarealword", method="lexical",
            completeness=completeness, limit=20,
        )
    assert {row["node_id"] for row in hits["results"]} == {
        f"n:bm25:{idx}" for idx in range(5)
    }
    if completeness == "exact":
        assert hits["total_count"] == 5 and hits["total_count_basis"] == "exact"
        assert hits["coverage"]["retrieval"] == {
            "status": "exhaustive", "basis": "exact_scalar",
        }
        assert miss["total_count"] == 0
    else:
        assert hits["total_count"] is None and hits["total_count_basis"] == "unknown"
    # The zero-scored match ranks after the scored ones; ranking stays BM25's.
    assert hits["results"][-1]["node_id"] == "n:bm25:3"
    # The word test admits matches only: 0-score padding still never returns.
    assert miss["results"] == []


@pytest.mark.parametrize("completeness", ["exact", "bounded"])
def test_record_scoped_lexical_search_keeps_a_match_scored_zero(
    clean_runtime: str, completeness: str,
) -> None:
    """The record-scope ranked read keeps a zero-scored match too (#3232).

    A `record_scope` search scores its materialized scoped rows in its own
    statement, not the unscoped ranked read; with `ranked.score < 0` there it
    returned 1 of the 2 scoped matches and said nothing about the other.
    Under `completeness="exact"` the short read is read again by the exact
    scalar statement, which counts members, so the count is the exact 2; under
    the bounded default (PACT lexical-index-scan-cap-3162) both rows are
    served and the count stays unknown.
    """
    scope = {"records": ["n:bm25:1", "n:bm25:3"]}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
        Publisher(
            clean_runtime,
            "default",
            catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
        ).cycle()
        session = open_session(conn, deployment_name="default")
        _score_one_match_zero(conn, "item 3")
        hits = search(
            conn, session, "alpha", method="lexical", limit=20, record_scope=scope,
            completeness=completeness,
        )
        miss = search(
            conn, session, "zzqxnotarealword", method="lexical", limit=20,
            record_scope=scope, completeness=completeness,
        )
    assert [row["node_id"] for row in hits["results"]] == ["n:bm25:1", "n:bm25:3"]
    if completeness == "exact":
        assert hits["total_count"] == 2 and hits["total_count_basis"] == "exact"
        assert miss["total_count"] == 0
    else:
        assert hits["total_count"] is None and hits["total_count_basis"] == "unknown"
    assert miss["results"] == []


def test_rollup_ranked_chunk_scan_keeps_a_chunk_scored_zero(clean_runtime: str) -> None:
    """The passage rollup's ranked chunk read keeps a zero-scored match too."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
        Publisher(
            clean_runtime,
            "default",
            catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
        ).cycle()
        session = open_session(conn, deployment_name="default")
        segment, index_name = resolve_node_text_bm25_index(conn)
        bp = branch_point(conn, session)
        _score_one_match_zero(conn, "item 3")
        sql = ranked_chunk_sql(
            node_segment=segment,
            order_sql=f"(attrs->>'text') <@> to_bm25query(%(q)s, '{index_name}')",
            extra_child_where=(),
        )
        params = {
            "bp": bp,
            "q": "alpha",
            "rollup_child_subtype": "pact_change",
            "rollup_raw_limit": 50,
        }
        with conn.cursor() as cur:
            cur.execute(sql, params)
            kept = [row[0] for row in cur.fetchall()]
            cur.execute(sql, {**params, "q": "zzqxnotarealword"})
            missed = cur.fetchall()
    assert sorted(kept) == [f"n:bm25:{idx}" for idx in range(5)]
    assert kept[-1] == "n:bm25:3"
    assert missed == []


@pytest.mark.parametrize(
    ("score", "text", "kept"),
    [
        (-1.5, "alpha bravo", True),  # scored: a match
        (0.0, "alpha bravo", True),  # a match scored 0 (#3232)
        (0.0, "Alphas bravo", True),  # matched as the index does: stemmed, case-folded
        (0.0, "bravo charlie", False),  # 0-score padding: not a match
        (0.0, "alphabet", False),  # a longer word is not the query word
        (0.0, "the a of", False),  # stopwords only
        (float("nan"), "alpha", True),
        (2.0, "bravo", False),
    ],
)
def test_bm25_match_sql_decides_membership_by_words(
    clean_runtime: str, score: float, text: str, kept: bool,
) -> None:
    sql = (
        "SELECT " + bm25_match_sql(score="%(score)s::double precision", text="%(text)s::text")
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"score": score, "text": text, "q": "alpha"})
            assert bool(cur.fetchone()[0]) is kept


def test_bm25_match_sql_skips_the_word_test_on_a_document_too_large_for_it(
    clean_runtime: str,
) -> None:
    """`to_tsvector` refuses lexemes over 1 MB; the test must not raise."""
    big = "alpha " + " ".join(f"w{idx}" for idx in range(200_000))
    sql = "SELECT " + bm25_match_sql(score="%(score)s::double precision", text="%(text)s::text")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"score": 0.0, "text": big, "q": "alpha"})
            assert cur.fetchone()[0] is False
            cur.execute(sql, {"score": -1.0, "text": big, "q": "alpha"})
            assert cur.fetchone()[0] is True


_EQUIVALENCE_TEXTS = [
    "Running the nightly jobs",
    "He runs; she ran. RUN!",
    "runner-up runway rerun",
    "e-mail the on-call engineer",
    "snake_case_identifier and CamelCaseName",
    "src/okg/substrate/mcp/search.py line 2420",
    "release v1.2.3 tagged 2026-10-03",
    "https://example.org/path?q=alpha&x=1",
    "café naïve résumé",
    "数据 检索 系统",
    "the of and a an to in",
    "alphabet soup",
    "Alpha, ALPHA... alpha's",
    "databases database's data",
    "x" * 3000 + " alpha",
]

_EQUIVALENCE_QUERIES = [
    "alpha", "run", "running", "RUNS", "rerun", "e-mail", "mail", "on-call",
    "snake_case_identifier", "camelcasename", "search.py", "src/okg/substrate/mcp/search.py",
    "v1.2.3", "2026-10-03", "example.org", "café", "cafe", "résumé", "数据", "检索",
    "the", "the of and", "!!!", "", "databases", "data", "alphabet", "alph",
    "alpha run café", "zzqxnotarealword",
]


def test_bm25_match_sql_admits_exactly_the_rows_bm25_scores_on_a_settled_index(
    clean_runtime: str,
) -> None:
    """With settled statistics the match test keeps exactly the scored rows.

    Negative test for #3232's fix: the word test may only restore a match the
    score lost, never admit a row BM25 excludes. Stemming, case, punctuation,
    compound words, paths, versions, URLs, accents, CJK, stopword-only and
    empty queries, near-miss words and a word too long to index all agree.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(
            [
                NodeFact(
                    node_id=f"n:eq:{idx}",
                    subtype="pact_change",
                    attrs={"text": text},
                    source_record_id={"id": f"n:eq:{idx}"},
                    source_revision={"v": 1},
                )
                for idx, text in enumerate(_EQUIVALENCE_TEXTS)
            ],
            source="default.bm25_test",
            strict_endpoints=True,
        )
        Publisher(
            clean_runtime,
            "default",
            catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
        ).cycle()
        segment, index_name = resolve_node_text_bm25_index(conn)
        with conn.cursor() as cur:
            cur.execute(f"ANALYZE {segment}")
            match = bm25_match_sql(score="s.score", text="s.txt")
            disagreements = {}
            scored_any = 0
            for query in _EQUIVALENCE_QUERIES:
                cur.execute(
                    f"SELECT s.node_id, s.score < 0 AS scored, {match} AS kept FROM ("
                    f"SELECT node_id, attrs->>'text' AS txt, "
                    f"(attrs->>'text') <@> to_bm25query(%(q)s, '{index_name}') AS score "
                    f"FROM {segment} WHERE deleted = FALSE AND attrs ? 'text' "
                    f"AND node_id LIKE 'n:eq:%%') s",
                    {"q": query},
                )
                rows = cur.fetchall()
                assert len(rows) == len(_EQUIVALENCE_TEXTS)
                scored_any += sum(1 for _node, scored, _kept in rows if scored)
                wrong = [
                    (node, scored, kept) for node, scored, kept in rows
                    if bool(scored) != bool(kept)
                ]
                if wrong:
                    disagreements[query] = wrong
    assert disagreements == {}
    # The corpus is not trivially empty of matches.
    assert scored_any > 20


def test_match_test_tokenizes_with_the_index_text_config(clean_runtime: str) -> None:
    """The word test's configuration is the one the index was built with."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn, ensure_schema=True)
        _segment, index_name = resolve_node_text_bm25_index(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT option_value FROM pg_class c, "
                "pg_options_to_table(c.reloptions) "
                "WHERE c.relname = %s AND option_name = 'text_config'",
                (index_name,),
            )
            assert cur.fetchone()[0] == BM25_TEXT_CONFIG


def test_filtered_passage_read_keeps_a_passage_rescored_zero(clean_runtime: str) -> None:
    """The windowed filtered passage read rescores its chosen passages with
    `<@>`; a real match rescored 0 is still returned (#3232).

    The windows run on the real BM25 index scan; only the rescoring statement
    uses the zero-scoring `<@>`. Before the fix this read kept `score < 0` and
    returned 4 of the 5 passages.
    """
    import importlib

    search_module = importlib.import_module("okg.substrate.mcp.search")

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS zs_nodes, zs_edges")
        conn.execute(
            "CREATE TABLE zs_nodes (node_id text PRIMARY KEY, subtype text, "
            "attrs jsonb, text_hash text, source text, source_record_id text, "
            "source_revision text, deleted bool DEFAULT false, "
            "live_lo bigint DEFAULT 0, live_hi bigint DEFAULT 9223372036854775807)"
        )
        conn.execute(
            "CREATE TABLE zs_edges (src text, dst text, edge_type text, "
            "deleted bool DEFAULT false, live_lo bigint DEFAULT 0, "
            "live_hi bigint DEFAULT 9223372036854775807)"
        )
        conn.execute(
            "INSERT INTO zs_nodes (node_id, subtype, attrs) SELECT 'c' || g, 'chunk', "
            "jsonb_build_object('text', 'alpha bravo chunk item ' || g) "
            "FROM generate_series(0, 4) g"
        )
        conn.execute(
            "INSERT INTO zs_nodes (node_id, subtype, attrs) SELECT 'x' || g, 'chunk', "
            "jsonb_build_object('text', 'unrelated words ' || g) "
            "FROM generate_series(0, 4) g"
        )
        conn.execute("INSERT INTO zs_nodes (node_id, subtype, attrs) VALUES ('P', 'doc', '{}')")
        conn.execute(
            "INSERT INTO zs_edges SELECT 'P', node_id, 'has_chunk' FROM zs_nodes "
            "WHERE subtype = 'chunk'"
        )
        conn.execute(
            "CREATE INDEX zs_bm25 ON zs_nodes USING bm25 ((attrs->>'text')) "
            "WITH (text_config = 'english') WHERE deleted = FALSE AND attrs ? 'text'"
        )
        conn.execute("ANALYZE zs_nodes")
        conn.execute("ANALYZE zs_edges")
        _score_one_match_zero(conn, "item 3", on_search_path=False)

        _index_sql, window_sql, score_sql = search_module._passage_windows_sql(
            segment="zs_nodes", edge_segment="zs_edges", index_literal="zs_bm25",
            keep=["n.subtype = %(child)s", "n.live_lo <= %(bp)s", "%(bp)s < n.live_hi"],
        )
        # The score is written twice: `steady_bm25_score_sql` keeps the
        # lower of two readings on a library older than 1.5.1 (#3666). Both
        # readings score zero here, so on any library the score is 0.
        real = "t.txt <@> to_bm25query("
        assert score_sql.count(real) == 2
        zero_scoring = score_sql.replace(
            real, "t.txt OPERATOR(okg_test_zero_score.<@>) to_bm25query(",
        )
        params = {"q": "alpha", "child": "chunk", "parent": "doc", "edge": "has_chunk", "bp": 1}
        with search_module._index_plan(conn) as cur:
            room = search_module._scan_room(cur, "zs_nodes")
            read = search_module._filtered_passage_read(
                cur, window_sql=window_sql, score_sql=zero_scoring, params=params,
                scan_room=room,
            )
    ids = [row["node_id"] for row in read.rows]
    assert sorted(ids) == [f"c{idx}" for idx in range(5)]
    assert ids[-1] == "c3"


def _passage_fixture_tables(conn: psycopg.Connection) -> None:
    """Tables `jit_nodes` / `jit_edges` with a BM25 index: five chunk passages
    matching "alpha" under one parent, five that do not."""
    conn.execute(
        "CREATE TABLE jit_nodes (node_id text PRIMARY KEY, subtype text, "
        "attrs jsonb, text_hash text, source text, source_record_id text, "
        "source_revision text, deleted bool DEFAULT false, "
        "live_lo bigint DEFAULT 0, live_hi bigint DEFAULT 9223372036854775807)"
    )
    conn.execute(
        "CREATE TABLE jit_edges (src text, dst text, edge_type text, "
        "deleted bool DEFAULT false, live_lo bigint DEFAULT 0, "
        "live_hi bigint DEFAULT 9223372036854775807)"
    )
    conn.execute(
        "INSERT INTO jit_nodes (node_id, subtype, attrs) SELECT 'c' || g, 'chunk', "
        "jsonb_build_object('text', 'alpha bravo chunk item ' || g) "
        "FROM generate_series(0, 4) g"
    )
    conn.execute(
        "INSERT INTO jit_nodes (node_id, subtype, attrs) SELECT 'x' || g, 'chunk', "
        "jsonb_build_object('text', 'unrelated words ' || g) "
        "FROM generate_series(0, 4) g"
    )
    conn.execute("INSERT INTO jit_nodes (node_id, subtype, attrs) VALUES ('P', 'doc', '{}')")
    conn.execute(
        "INSERT INTO jit_edges SELECT 'P', node_id, 'has_chunk' FROM jit_nodes "
        "WHERE subtype = 'chunk'"
    )
    conn.execute(
        "CREATE INDEX jit_bm25 ON jit_nodes USING bm25 ((attrs->>'text')) "
        "WITH (text_config = 'english') WHERE deleted = FALSE AND attrs ? 'text'"
    )
    conn.execute("ANALYZE jit_nodes")
    conn.execute("ANALYZE jit_edges")


def _explain_analyze(cur: psycopg.Cursor, sql: str, params: dict) -> dict:
    """The top-level object of `EXPLAIN (ANALYZE, FORMAT JSON)` of `sql`."""
    import json

    from okg.substrate.bm25_node_search import execute_with_custom_plan

    execute_with_custom_plan(cur, "EXPLAIN (ANALYZE, FORMAT JSON) " + sql, params)
    row = cur.fetchone()
    document = next(iter(row.values())) if isinstance(row, dict) else row[0]
    if isinstance(document, str):
        document = json.loads(document)
    return document[0]


def test_filtered_passage_read_statements_run_without_jit(clean_runtime: str) -> None:
    """The filtered passage read's window and scoring statements run with JIT
    off under `_index_plan` (PACT filtered-passage-rescore-cost, #3310).

    `_PASSAGE_INDEX_PLAN` sets `enable_sort=off`, whose cost penalty passed
    the JIT thresholds, so PostgreSQL compiled every statement (185-305 ms
    each on comp-ops). Here JIT is forced for the session (every threshold
    0): the same statements show a "JIT" section outside `_index_plan` and
    none inside it, and the session's `jit` is back to on afterwards.
    """
    import importlib

    search_module = importlib.import_module("okg.substrate.mcp.search")

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        if not conn.execute("SELECT pg_jit_available()").fetchone()[0]:
            pytest.skip(
                "PostgreSQL JIT (LLVM) is not available in the test database, "
                "so a statement can never show a JIT section and this test "
                "would prove nothing"
            )
        _passage_fixture_tables(conn)
        for setting in (
            "jit = on", "jit_above_cost = 0",
            "jit_inline_above_cost = 0", "jit_optimize_above_cost = 0",
        ):
            conn.execute(f"SET {setting}")

        _index_sql, window_sql, score_sql = search_module._passage_windows_sql(
            segment="jit_nodes", edge_segment="jit_edges", index_literal="jit_bm25",
            keep=["n.subtype = %(child)s", "n.live_lo <= %(bp)s", "%(bp)s < n.live_hi"],
        )
        params = {"q": "alpha", "child": "chunk", "parent": "doc", "edge": "has_chunk", "bp": 1}
        window_params = {**params, "passage_window": 1000, "passage_offset": 0}
        score_params = {
            **params,
            "passage_texts": [f"alpha bravo chunk item {idx}" for idx in range(5)],
        }
        statements = {"window": (window_sql, window_params), "score": (score_sql, score_params)}

        # Failure case: with the session's settings alone, each statement is
        # JIT-compiled.
        with conn.cursor() as cur:
            for name, (sql, values) in statements.items():
                plan = _explain_analyze(cur, sql, values)
                assert "JIT" in plan, f"{name} statement was not JIT-compiled outside _index_plan"

        with search_module._index_plan(conn) as cur:
            cur.execute("SELECT current_setting('jit') AS jit")
            assert cur.fetchone()["jit"] == "off"
            for name, (sql, values) in statements.items():
                plan = _explain_analyze(cur, sql, values)
                assert "JIT" not in plan, f"{name} statement was JIT-compiled under _index_plan"
            # The read itself still returns the five matching passages.
            room = search_module._scan_room(cur, "jit_nodes")
            read = search_module._filtered_passage_read(
                cur, window_sql=window_sql, score_sql=score_sql, params=params,
                scan_room=room,
            )
        assert sorted(row["node_id"] for row in read.rows) == [f"c{idx}" for idx in range(5)]

        assert conn.execute("SELECT current_setting('jit')").fetchone()[0] == "on"
        assert conn.execute("SELECT current_setting('jit_above_cost')").fetchone()[0] == "0"
