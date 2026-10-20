"""The filtered fold's passage read: its cost per call (PACT mcp-filtered-passage-cost).

PostgreSQL tests on published fixture graphs (the search-result-shape corpus
of `test_search_records`, and corpora built here).

* The byte-budget wrapper reruns the search at smaller limits; the BM25
  reads run once per call and the served page is byte-identical to the page
  served when every rerun reads again.
* The passage read walks the BM25 index in windows and returns the previous
  full statement's 200 passages, ties included, while the rows it scores
  follow the matches.
* A read that may have reached the index's result cap, or under which the
  index moved, runs the previous full statement and says so.
"""
from __future__ import annotations

import contextlib
import importlib
import json
from typing import Any, Optional

import psycopg
import pytest

from okg.substrate.mcp import open_session, response_bounds, search
from okg.substrate.mcp.response_bounds import bound_read_tool
from okg.substrate.mcp.result_text import render
from tests.substrate.mcp.test_search_records import (
    OPEN,
    QUERY,
    TERM,
    _bulk_facts,
    _edge,
    _node,
    _publish,
)

pytestmark = pytest.mark.postgres

# The module, not the package's re-exported `search` function.
search_module = importlib.import_module("okg.substrate.mcp.search")
CAP = search_module._LEXICAL_RANK_CAP
#: Text that marks a passage-read statement: the full statement's edge test,
#: the windows' and the scoring statement's probe, the plan check.
PASSAGE_MARKS = ("e.edge_type = %(edge)s", "statistics_probe", "bm25_get_current_score()")


class _Reads:
    """Counts the BM25-ordered statements and the passage reads of a call."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.passage_statements = 0
        self.ranked_statements = 0
        #: Every passage read's result, in call order.
        self.passage_reads: list[Any] = []
        original = search_module.execute_with_custom_plan
        original_cached = search_module.cached_read

        def counting(cur: Any, sql: Any, params: Any = None) -> Any:
            text = str(sql)
            if search_module.INDEX_SCAN_CAP_MARK in text:
                # The index-cap guard's count and complete read (PACT
                # lexical-index-scan-cap-3162) check a read; they are not one.
                pass
            elif any(mark in text for mark in PASSAGE_MARKS):
                self.passage_statements += 1
            else:
                self.ranked_statements += 1
            return original(cur, sql, params)

        def recording(key: Any, read: Any) -> Any:
            def run() -> Any:
                result = read()
                if isinstance(result, search_module._PassageRead):
                    self.passage_reads.append(result)
                return result

            return original_cached(key, run)

        monkeypatch.setattr(search_module, "execute_with_custom_plan", counting)
        monkeypatch.setattr(search_module, "cached_read", recording)

    def reset(self) -> None:
        self.passage_statements = self.ranked_statements = 0
        self.passage_reads = []


def _tool(dsn: str, deployment: str, runs: list[int]):
    """A `search` operator under the real byte-budget wrapper."""

    def search_tool(
        query: str,
        subtype_filter: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
        generation_id: Optional[str] = None,
        max_bytes: Optional[int] = None,
    ) -> dict[str, Any]:
        runs.append(limit)
        with psycopg.connect(dsn, autocommit=True) as conn:
            session = open_session(
                conn, deployment_name=deployment, requested_generation=generation_id,
            )
            return search(
                conn, session, query, method="lexical", limit=limit, offset=offset,
                filter={"subtype": subtype_filter} if subtype_filter else None,
            )

    return bound_read_tool("search", search_tool)


def _served(payload: dict[str, Any]) -> tuple[str, str]:
    return render(payload, tool="search"), json.dumps(payload, sort_keys=True, default=str)


def _search(dsn: str, deployment: str, query: str = QUERY, **kwargs: Any) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        return search(conn, session, query, method="lexical", **kwargs)


def _publish_still(tmp_path, dsn: str, **kwargs: Any) -> str:
    """Publish with autovacuum off on the node segment of this disposable
    database. pg_textsearch's BM25 statistics are global, not
    snapshot-consistent: a vacuum spills the in-memory buffer and drops
    dead entries, moving every score, even in the middle of a statement.
    Comparing two reads needs the statistics to hold still."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        segment, _index = search_module.resolve_node_text_bm25_index(conn)
        conn.execute(f"ALTER TABLE {segment} SET (autovacuum_enabled = off)")
    return _publish(tmp_path, dsn, **kwargs)


def _ids(page: dict) -> list[str]:
    return [row["node_id"] for row in page["results"]]


# ---------- one read per call ----------

def test_a_rerun_call_reads_the_passages_once_and_serves_the_same_page(
    clean_runtime, tmp_path, monkeypatch,
):
    groups, main = _bulk_facts(hidden_comments=False)
    deployment = _publish_still(tmp_path, clean_runtime, groups=groups, main=main)
    arguments = {"query": QUERY, "subtype_filter": "ticket", "limit": 60, "max_bytes": 6000}
    reads = _Reads(monkeypatch)

    # Before: no call cache, so every rerun reads again.
    runs_before: list[int] = []
    with monkeypatch.context() as patch:
        patch.setattr(response_bounds, "call_read_cache", contextlib.nullcontext)
        before = _tool(clean_runtime, deployment, runs_before)(**arguments)
    passage_before = reads.passage_statements
    ranked_before = reads.ranked_statements
    reads_before = len(reads.passage_reads)

    reads.reset()
    runs_after: list[int] = []
    after = _tool(clean_runtime, deployment, runs_after)(**arguments)

    # The wrapper did rerun (the case the cache exists for) ...
    assert len(runs_after) >= 2 and runs_after == runs_before
    assert after.get("status") != "refused" and after["results"]
    # ... every run read again without the cache, once with it.
    assert reads_before == len(runs_before) and ranked_before == len(runs_before)
    assert len(reads.passage_reads) == 1 and reads.ranked_statements == 1
    assert reads.passage_statements >= 1
    assert passage_before == reads.passage_statements * len(runs_before)
    # Same page, byte for byte, in both served forms.
    assert _served(after) == _served(before)


def test_without_a_call_cache_every_search_reads(clean_runtime, tmp_path, monkeypatch):
    """Failure case: a direct call (no wrapper, no cache) never reuses rows,
    so two calls cannot share one read."""
    groups, main = _bulk_facts(hidden_comments=False)
    deployment = _publish_still(tmp_path, clean_runtime, groups=groups, main=main)
    reads = _Reads(monkeypatch)
    for limit in (10, 5):
        _search(clean_runtime, deployment, limit=limit, filter={"subtype": "ticket"})
    assert len(reads.passage_reads) == 2 and reads.ranked_statements == 2


# ---------- the same passages, cost by matches ----------

STRONG = 140
TIED = 120
PAGE_STRONG = 50


def _tie_facts() -> tuple[dict[str, list], list]:
    """Ticket comments: STRONG with distinct scores, then TIED with one
    identical text, so the 200th passage sits inside the tie group. Page
    comments score better than all of them and never reach a ticket."""
    tickets = [
        _node(f"ticket:t:{i:03d}", "ticket", f"Plain ticket {i}.", status="open")
        for i in range((STRONG + TIED) // 2)
    ]
    comments = [
        _node(f"comment:strong:{i:03d}", "comment",
              f"{TERM} {TERM} cladding strong reading " + "pad " * (i // 7))
        for i in range(STRONG)
    ] + [
        # One query word in a long text: below every strong comment.
        _node(f"comment:tie:{i:03d}", "comment", f"{TERM} tie note " + "filler " * 60)
        for i in range(TIED)
    ]
    # Two comments per ticket, so records keep arriving deep in the scan.
    edges = [
        _edge(f"ticket:t:{n // 2:03d}", str(fact.node_id))
        for n, fact in enumerate(comments)
    ]
    pages = [_node(f"page:p:{i}", "page", f"Plain page {i}.", status="open") for i in range(5)]
    page_comments = [
        _node(f"comment:page:{i:03d}", "comment", f"{TERM} {TERM} {TERM} cladding page {i}")
        for i in range(PAGE_STRONG)
    ]
    edges += [
        _edge(f"page:p:{i % 5}", f"comment:page:{i:03d}") for i in range(PAGE_STRONG)
    ]
    return {OPEN: tickets + comments + pages + page_comments}, edges


def _statistics_probe(dsn: str, query: str) -> float:
    """The query text scored against the index: it moves whenever the
    index's BM25 statistics move (a vacuum or memtable spill)."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        _segment, index_name = search_module.resolve_node_text_bm25_index(conn)
        row = conn.execute(
            "SELECT %(q)s::text <@> to_bm25query(%(q)s, %(i)s)",
            {"q": query, "i": index_name},
        ).fetchone()
    return float(row[0])


def _both_reads(dsn: str, deployment: str, monkeypatch, **kwargs: Any):
    """The page and passage read through the windows, then through the
    previous full statement (forced by failing the plan check).

    pg_textsearch's statistics are global, not snapshot-consistent: an
    autovacuum after the publish changes every score. The two reads are
    compared only when the statistics held across both; otherwise the pair
    is read again."""
    query = kwargs.get("query", QUERY)
    for _attempt in range(5):
        before = _statistics_probe(dsn, query)
        reads = _Reads(monkeypatch)
        windowed = _search(dsn, deployment, **kwargs)
        windowed_reads = list(reads.passage_reads)
        reads.reset()
        with monkeypatch.context() as patch:
            patch.setattr(search_module, "_passage_index_ordered", lambda *a, **k: False)
            full = _search(dsn, deployment, **kwargs)
        if _statistics_probe(dsn, query) == before:
            break
    else:
        pytest.fail("the index statistics moved during every pair of reads")
    # The forced fallback discloses itself; the rest of the page must match.
    assert full.pop("passage_read_fallback") == "plan_not_index_scan"
    assert "passage_read_fallback" not in windowed
    return windowed, windowed_reads, full, list(reads.passage_reads)


def _rows(read: Any) -> list[tuple[str, float]]:
    return [(str(row["node_id"]), float(row["score"])) for row in read.rows]


def test_the_windowed_read_returns_the_full_statements_passages_ties_included(
    clean_runtime, tmp_path, monkeypatch,
):
    groups, main = _tie_facts()
    deployment = _publish_still(tmp_path, clean_runtime, groups=groups, main=main)
    # Small windows: many window edges, several inside the tie group.
    monkeypatch.setattr(search_module, "_PASSAGE_FIRST_WINDOW", 16)
    windowed, (new,), full, (old,) = _both_reads(
        clean_runtime, deployment, monkeypatch, limit=30, filter={"subtype": "ticket"},
    )
    assert new.scan == "index" and old.scan == "full"
    assert len(old.rows) == CAP
    # The cap falls inside the tie group, so which tied rows make it is
    # decided by node id; both reads agree row for row.
    scores = [score for _id, score in _rows(old)]
    assert scores[CAP - 1] == scores[CAP - 2]
    assert scores.count(scores[CAP - 1]) == CAP - STRONG
    assert _rows(new) == _rows(old)
    # Same page, byte for byte.
    assert json.dumps(windowed, sort_keys=True, default=str) == json.dumps(
        full, sort_keys=True, default=str,
    )
    # Page comments match better but were never scored as passages here.
    assert not [i for i, _s in _rows(new) if i.startswith("comment:page:")]
    assert new.rows_scored < STRONG + TIED + PAGE_STRONG


def test_rows_scored_follow_the_matches_not_the_passages(
    clean_runtime, tmp_path, monkeypatch,
):
    """Many passages under the filtered type, few matches: the read scores
    only the matches (the previous statement scored every passage)."""
    tickets = [
        _node(f"ticket:q:{i:02d}", "ticket", f"Quiet ticket {i}.", status="open")
        for i in range(20)
    ]
    quiet = [
        _node(f"comment:quiet:{i:03d}", "comment", f"Routine weld inspection {i}.")
        for i in range(400)
    ]
    hits = [
        _node(f"comment:hit:{i}", "comment", f"{TERM} cladding finding {i}.")
        for i in range(5)
    ]
    edges = [
        _edge(f"ticket:q:{n % 20:02d}", str(fact.node_id))
        for n, fact in enumerate(quiet + hits)
    ]
    deployment = _publish_still(
        tmp_path, clean_runtime, groups={OPEN: tickets + quiet + hits}, main=edges,
    )
    windowed, (new,), full, (old,) = _both_reads(
        clean_runtime, deployment, monkeypatch, limit=10, filter={"subtype": "ticket"},
    )
    assert new.scan == "index"
    assert _rows(new) == _rows(old) and len(new.rows) == 5
    assert new.rows_scored == 5 and new.index_rows == 5
    assert _ids(windowed) == _ids(full) and len(_ids(windowed)) == 5


# ---------- the index's result cap ----------

def test_a_read_that_reaches_the_scan_room_runs_the_full_statement_and_says_so(
    clean_runtime, tmp_path, monkeypatch,
):
    """With a cap of 128 global positions (50 page comments first), the read
    cannot prove it saw every match, so it answers with the exact statement."""
    groups, main = _tie_facts()
    deployment = _publish_still(tmp_path, clean_runtime, groups=groups, main=main)
    filt = {"subtype": "ticket"}
    exact = _search(clean_runtime, deployment, limit=100, filter=filt)
    monkeypatch.setattr(search_module, "_PASSAGE_FIRST_WINDOW", 16)
    monkeypatch.setattr(search_module, "_PASSAGE_INDEX_SCAN_CAP", 128)
    reads = _Reads(monkeypatch)
    capped = _search(clean_runtime, deployment, limit=100, filter=filt)
    (read,) = reads.passage_reads
    assert read.scan == "full" and read.fallback == "index_scan_cap"
    assert capped.pop("passage_read_fallback") == "index_scan_cap"
    assert "passage_read_fallback" not in exact
    assert capped == exact


def _cap_tables(conn: psycopg.Connection, *, others: int, filtered: int = 300) -> None:
    """Review repro (final round, finding 1), on plain tables: `others` short
    passages of another type outrank `filtered` long passages under one
    parent in the BM25 order."""
    conn.execute("DROP TABLE IF EXISTS cap_nodes, cap_edges")
    conn.execute(
        "CREATE TABLE cap_nodes (node_id text PRIMARY KEY, subtype text, attrs jsonb, "
        "text_hash text, source text, source_record_id text, source_revision text, "
        "deleted bool DEFAULT false, live_lo bigint DEFAULT 0, "
        "live_hi bigint DEFAULT 9223372036854775807)"
    )
    conn.execute(
        "CREATE TABLE cap_edges (src text, dst text, edge_type text, "
        "deleted bool DEFAULT false, live_lo bigint DEFAULT 0, "
        "live_hi bigint DEFAULT 9223372036854775807)"
    )
    conn.execute(
        "INSERT INTO cap_nodes (node_id, subtype, attrs) SELECT 'o' || g, 'other', "
        "jsonb_build_object('text', 'alpha beta') FROM generate_series(1, %s) g",
        (others,),
    )
    conn.execute(
        "INSERT INTO cap_nodes (node_id, subtype, attrs) SELECT 'L' || g, 'chunk', "
        "jsonb_build_object('text', 'alpha ' || repeat('filler words here ', 20 + g %% 7)) "
        "FROM generate_series(1, %s) g",
        (filtered,),
    )
    conn.execute("INSERT INTO cap_nodes (node_id, subtype, attrs) VALUES ('P', 'doc', '{}')")
    conn.execute(
        "INSERT INTO cap_edges SELECT 'P', 'L' || g, 'has_chunk' "
        "FROM generate_series(1, %s) g",
        (filtered,),
    )
    conn.execute("CREATE INDEX cap_edges_dst ON cap_edges (dst)")
    conn.execute(
        "CREATE INDEX cap_bm25 ON cap_nodes USING bm25 ((attrs->>'text')) "
        "WITH (text_config = 'english') WHERE deleted = FALSE AND attrs ? 'text'"
    )
    conn.execute("ANALYZE cap_nodes")
    conn.execute("ANALYZE cap_edges")


def _cap_read(conn: psycopg.Connection):
    keep = ["n.subtype = %(child)s", "n.live_lo <= %(bp)s", "%(bp)s < n.live_hi"]
    _index_sql, window_sql, score_sql = search_module._passage_windows_sql(
        segment="cap_nodes", edge_segment="cap_edges", index_literal="cap_bm25", keep=keep,
    )
    params = {"q": "alpha", "child": "chunk", "parent": "doc", "edge": "has_chunk", "bp": 1}
    with search_module._index_plan(conn) as cur:
        room = search_module._scan_room(cur, "cap_nodes")
        return search_module._filtered_passage_read(
            cur, window_sql=window_sql, score_sql=score_sql, params=params,
            scan_room=room,
        )


def _cap_exact(conn: psycopg.Connection) -> list[tuple[str, float]]:
    """The previous statement's answer, by a sequential scan (no index cap)."""
    with conn.transaction():
        conn.execute("SET LOCAL enable_indexscan = off")
        conn.execute("SET LOCAL enable_bitmapscan = off")
        rows = conn.execute(
            "SELECT node_id, score FROM (SELECT node_id, "
            "(attrs->>'text') <@> to_bm25query('alpha', 'cap_bm25') AS score "
            "FROM cap_nodes WHERE subtype = 'chunk') s WHERE score < 0 "
            "ORDER BY score, node_id LIMIT 200"
        ).fetchall()
    return [(str(node_id), float(score)) for node_id, score in rows]


def test_matches_past_the_index_result_cap_are_never_read_as_none(clean_runtime):
    """110,000 better-scoring matches of another type: the index stops at its
    100,000-result cap before the filtered passages. The read used to take
    that short window for the end of the matches and return 0 rows."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _cap_tables(conn, others=110_000)
        assert len(_cap_exact(conn)) == 200
        with pytest.raises(search_module._ScanCapReached):
            _cap_read(conn)


def test_matches_inside_the_index_result_cap_are_read_exactly(clean_runtime):
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _cap_tables(conn, others=20_000)
        read = _cap_read(conn)
        assert read.index_rows > 20_000
        assert _rows(read) == _cap_exact(conn)


# ---------- long passages still in the index's memory buffer ----------

def _length_facts(n: int = 400, paged: int = 0) -> tuple[dict[str, list], list]:
    """`n` comments of rising length (40 + i words; a longer text scores
    worse), the longer ones with the smaller node ids; the `paged` shortest
    hang under a page, the rest two per ticket. Published after the index
    exists, so the rows sit in the index's in-memory buffer (memtable),
    where the index scores with the exact length and `<@>` with the
    quantized one (up to 9/8 shorter)."""
    tickets = [
        _node(f"ticket:t:{i:03d}", "ticket", f"Plain ticket {i}.", status="open")
        for i in range(n // 2)
    ]
    comments = [
        _node(f"comment:len:{999 - i:03d}", "comment",
              f"{TERM} " + " ".join(f"filler{k}" for k in range(40 + i)))
        for i in range(n)
    ]
    edges = [
        _edge(f"page:p:{k % 5}" if k < paged else f"ticket:t:{k // 2:03d}",
              str(fact.node_id))
        for k, fact in enumerate(comments)
    ]
    pages = [_node(f"page:p:{i}", "page", f"Plain page {i}.", status="open") for i in range(5)]
    return {OPEN: tickets + comments + pages}, edges


def _memtable_gap(dsn: str) -> float:
    """The largest gap between the index scan's score and `<@>` over the
    first 400 comment matches: the index's own scores in one statement,
    `<@>` of the same texts in another (as the read itself does)."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        segment, index_name = search_module.resolve_node_text_bm25_index(conn)
        for name, value in search_module._PASSAGE_INDEX_PLAN:
            conn.execute(f"SET {name} = {value}")
        literal = index_name.replace("'", "''")
        query = f"to_bm25query(%(q)s, '{literal}')"
        rows = conn.execute(
            f"SELECT n.attrs->>'text' AS txt, bm25_get_current_score() AS idx "
            f"FROM {segment} n WHERE n.subtype = 'comment' AND n.deleted = FALSE "
            f"AND n.attrs ? 'text' ORDER BY (n.attrs->>'text') <@> {query} LIMIT 400",
            {"q": TERM},
        ).fetchall()
        scores = conn.execute(
            f"SELECT t.txt <@> {query} FROM unnest(%(texts)s::text[]) "
            f"WITH ORDINALITY AS t(txt, i) ORDER BY t.i",
            {"q": TERM, "texts": [row[0] for row in rows]},
        ).fetchall()
    return max(
        (abs(float(expr[0]) - float(row[1])) for row, expr in zip(rows, scores)),
        default=0.0,
    )


@pytest.mark.parametrize("paged", [0, 140])
def test_memtable_rows_of_varied_length_give_the_full_statements_passages(
    clean_runtime, tmp_path, monkeypatch, paged,
):
    """Review repro (round 1, finding 1): with a stop margin of float
    rounding only, the windowed read dropped the passages the full
    statement ranks 196-200 (paged=140, first window 25). The read must
    match row for row at every window size."""
    groups, main = _length_facts(paged=paged)
    deployment = _publish_still(tmp_path, clean_runtime, groups=groups, main=main)
    # Non-vacuity: some rows score differently in the index scan and in
    # `<@>` (memtable rows), the case a rounding-only margin got wrong.
    assert _memtable_gap(clean_runtime) > 1e-3
    for first_window in (13, 16, 25, 31, 64, 199, 256):
        monkeypatch.setattr(search_module, "_PASSAGE_FIRST_WINDOW", first_window)
        windowed, (new,), full, (old,) = _both_reads(
            clean_runtime, deployment, monkeypatch, query=TERM, limit=50,
            filter={"subtype": "ticket"},
        )
        assert new.scan == "index", first_window
        assert len(old.rows) == CAP
        assert _rows(new) == _rows(old), first_window
        assert json.dumps(windowed, sort_keys=True, default=str) == json.dumps(
            full, sort_keys=True, default=str,
        ), first_window


# ---------- the plan check and the fallback ----------

@pytest.mark.parametrize(
    "query",
    [f"Sorted {TERM}", f"{TERM} Sort", f"Limit Index Scan Order By {TERM}",
     f"Incremental Sort {TERM}"],
)
def test_plan_words_in_the_query_keep_the_windowed_read(
    clean_runtime, tmp_path, monkeypatch, query,
):
    """Review finding 2: the plan check read the EXPLAIN text, which holds
    the query, so a query word "Sort" forced the full statement."""
    groups, main = _length_facts(n=40)
    deployment = _publish_still(tmp_path, clean_runtime, groups=groups, main=main)
    reads = _Reads(monkeypatch)
    page = _search(clean_runtime, deployment, query=query, limit=5,
                   filter={"subtype": "ticket"})
    assert [read.scan for read in reads.passage_reads] == ["index"]
    assert "passage_read_fallback" not in page


def test_plan_words_in_a_source_filter_keep_the_windowed_read(
    clean_runtime, tmp_path, monkeypatch,
):
    groups, main = _length_facts(n=40)
    deployment = _publish_still(tmp_path, clean_runtime, groups=groups, main=main)
    reads = _Reads(monkeypatch)
    _search(clean_runtime, deployment, query=TERM, limit=5,
            filter={"subtype": "ticket", "source": "connector:Sort Index Scan"})
    assert [read.scan for read in reads.passage_reads] == ["index"]


def test_a_plan_that_is_not_the_index_scan_falls_back_and_says_so(
    clean_runtime, tmp_path, monkeypatch, caplog,
):
    groups, main = _length_facts(n=40)
    deployment = _publish_still(tmp_path, clean_runtime, groups=groups, main=main)
    reads = _Reads(monkeypatch)
    monkeypatch.setattr(search_module, "_passage_index_ordered", lambda *a, **k: False)
    with caplog.at_level("WARNING", logger="okg.substrate.mcp.search"):
        page = _search(clean_runtime, deployment, query=TERM, limit=5,
                       filter={"subtype": "ticket"})
    (read,) = reads.passage_reads
    assert read.scan == "full" and read.fallback == "plan_not_index_scan"
    assert page["passage_read_fallback"] == "plan_not_index_scan"
    # #3666 (read-passage-rows): the search text prints no other-fields
    # block; this read-plan fact is said by the JSON form and the log below.
    assert "passage_read_fallback" not in render(page, tool="search")
    assert any("plan_not_index_scan" in record.getMessage() for record in caplog.records)


def _move_statistics(monkeypatch, *, attempts_to_disturb: int) -> list[int]:
    """Make the statistics probe of each disturbed attempt's second window
    differ from its first, as a memtable spill or vacuum between two
    windows would."""
    original = search_module.execute_with_custom_plan
    windows = [0]
    attempts: list[int] = []

    def disturbed(cur: Any, sql: Any, params: Any = None) -> Any:
        text = str(sql)
        if "AS statistics_probe" in text:
            if params.get("passage_offset") == 0:
                attempts.append(1)
                windows[0] = 0
            windows[0] += 1
            if windows[0] == 2 and len(attempts) <= attempts_to_disturb:
                text = text.replace(
                    " AS statistics_probe", " - 1.0 AS statistics_probe", 1,
                )
        return original(cur, text, params)

    monkeypatch.setattr(search_module, "execute_with_custom_plan", disturbed)
    return attempts


def _disturbed_read(dsn: str, deployment: str, monkeypatch, *, disturb: int):
    """A reference page, then the same page with the statistics probe moved
    in the first `disturb` attempts, compared only when the real index
    statistics held across both."""
    monkeypatch.setattr(search_module, "_PASSAGE_FIRST_WINDOW", 16)
    for _attempt in range(5):
        before = _statistics_probe(dsn, TERM)
        reference = _search(dsn, deployment, query=TERM, limit=50,
                            filter={"subtype": "ticket"})
        with monkeypatch.context() as patch:
            attempts = _move_statistics(patch, attempts_to_disturb=disturb)
            reads = _Reads(patch)
            page = _search(dsn, deployment, query=TERM, limit=50,
                           filter={"subtype": "ticket"})
        if _statistics_probe(dsn, TERM) == before:
            (read,) = reads.passage_reads
            return reference, page, read, attempts
    pytest.fail("the index statistics moved during every pair of reads")


def test_statistics_that_move_between_windows_restart_the_read(
    clean_runtime, tmp_path, monkeypatch,
):
    """Review finding 3: pg_textsearch's statistics are not snapshot-
    consistent, so two windows' scores can come from different statistics.
    A moved probe restarts the read; the restarted read is exact."""
    groups, main = _length_facts()
    deployment = _publish_still(tmp_path, clean_runtime, groups=groups, main=main)
    reference, page, read, attempts = _disturbed_read(
        clean_runtime, deployment, monkeypatch, disturb=1,
    )
    assert len(attempts) == 2 and read.scan == "index"
    assert page == reference


def test_statistics_that_keep_moving_fall_back_and_say_so(
    clean_runtime, tmp_path, monkeypatch,
):
    groups, main = _length_facts()
    deployment = _publish_still(tmp_path, clean_runtime, groups=groups, main=main)
    reference, page, read, attempts = _disturbed_read(
        clean_runtime, deployment, monkeypatch, disturb=99,
    )
    assert len(attempts) == search_module._PASSAGE_READ_ATTEMPTS
    assert read.scan == "full" and read.fallback == "index_statistics_changed"
    assert page.pop("passage_read_fallback") == "index_statistics_changed"
    assert page == reference


# ---------- a memtable spill during a read ----------

def _spill_after_first_window(monkeypatch, dsn: str, index_name: str) -> list[int]:
    """Spill the index's memtable once, right after the first window of the
    first attempt: the spilled rows' index scores improve (quantized length)
    and they move earlier in the scan."""
    original = search_module.execute_with_custom_plan
    attempts: list[int] = []

    def spilling(cur: Any, sql: Any, params: Any = None) -> Any:
        result = original(cur, sql, params)
        text = str(sql)
        if "window_settled" in text and params.get("passage_offset") == 0:
            attempts.append(1)
            if len(attempts) == 1:
                with psycopg.connect(dsn, autocommit=True) as other:
                    other.execute("SELECT bm25_spill_index(%s)", (index_name,))
        return result

    monkeypatch.setattr(search_module, "execute_with_custom_plan", spilling)
    return attempts


def test_a_row_moving_before_the_offset_restarts_the_read(
    clean_runtime, tmp_path, monkeypatch,
):
    """Review final round, finding 2: a spill improves the spilled rows'
    index scores; one that moves from past `OFFSET` to before it pushes an
    already settled row into the next window. Simulated by starting the
    second window one row early: the order check must restart the read."""
    groups, main = _length_facts(paged=140)
    deployment = _publish_still(tmp_path, clean_runtime, groups=groups, main=main)
    monkeypatch.setattr(search_module, "_PASSAGE_FIRST_WINDOW", 16)
    original = search_module.execute_with_custom_plan
    windows: list[int] = []

    def shifted(cur: Any, sql: Any, params: Any = None) -> Any:
        if "window_settled" in str(sql):
            windows.append(params["passage_offset"])
            if len(windows) == 2:
                params = {**params, "passage_offset": params["passage_offset"] - 1}
        return original(cur, sql, params)

    raised: list[str] = []
    original_read = search_module._filtered_passage_read

    def watching(*args: Any, **kwargs: Any) -> Any:
        try:
            return original_read(*args, **kwargs)
        except search_module._StatisticsChanged as exc:
            raised.append(type(exc).__name__)
            raise

    monkeypatch.setattr(search_module, "_filtered_passage_read", watching)
    with monkeypatch.context() as patch:
        patch.setattr(search_module, "execute_with_custom_plan", shifted)
        reads = _Reads(patch)
        page = _search(clean_runtime, deployment, query=TERM, limit=50,
                       filter={"subtype": "ticket"})
    (read,) = reads.passage_reads
    assert raised == ["_OrderMoved"] and read.scan == "index"
    assert windows[2] == 0  # the restart began again at the top
    reference = _search(clean_runtime, deployment, query=TERM, limit=50,
                        filter={"subtype": "ticket"})
    assert page == reference


class _NoisyCursor:
    """A cursor that moves the second window's best score `shift` below the
    first window's edge, as float32 rounding between two statements does."""

    def __init__(self, cur: Any, state: dict[str, Any], shift: Any) -> None:
        self._cur, self._state, self._shift = cur, state, shift

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cur, name)

    def fetchall(self) -> list:
        rows = self._cur.fetchall()
        if not (self._state.pop("window", False) and rows):
            return rows
        self._state["windows"] += 1
        if self._state["windows"] == 1:
            self._state["edge"] = float(rows[0]["window_edge"])
        elif self._state["windows"] == 2:
            low = self._state["edge"] - self._shift(self._state["edge"])
            rows = [{**row, "window_low": low} for row in rows]
        return rows


@pytest.mark.parametrize(("shift", "moved"), [
    # The gap seen on the comp-ops copy: one float32 step at -15.5.
    pytest.param(lambda edge: 9.5e-7, False, id="observed-float32-gap"),
    pytest.param(lambda edge: 0.5 * search_module._index_score_noise(edge), False,
                 id="half-the-noise-margin"),
    # Just past the margin is a move: a wider margin would hide one.
    pytest.param(lambda edge: 2.0 * search_module._index_score_noise(edge), True,
                 id="just-past-the-margin-restarts"),
    pytest.param(lambda edge: 1.0, True, id="a-large-move-restarts"),
])
def test_float_rounding_at_the_window_edge_does_not_restart_the_read(
    clean_runtime, tmp_path, monkeypatch, shift, moved,
):
    """Issue #3327: the index returns float32 scores, so the same row can
    score about 1e-6 apart in two statements. Before, a second window whose
    best score was that much below the first window's edge raised
    `_OrderMoved` every time and the read fell back to the full statement,
    disclosed as `index_statistics_changed` although nothing changed. The
    check now allows the index's float32 noise (`_index_score_noise`, about
    16 float32 steps); anything past it still restarts, and the restarted
    read returns the same page."""
    groups, main = _length_facts(paged=140)
    deployment = _publish_still(tmp_path, clean_runtime, groups=groups, main=main)
    monkeypatch.setattr(search_module, "_PASSAGE_FIRST_WINDOW", 16)
    state: dict[str, Any] = {"windows": 0}
    original_execute = search_module.execute_with_custom_plan

    def marking(cur: Any, sql: Any, params: Any = None) -> Any:
        state["window"] = "window_settled" in str(sql)
        return original_execute(cur, sql, params)

    raised: list[str] = []
    original_read = search_module._filtered_passage_read

    def noisy(cur: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return original_read(_NoisyCursor(cur, state, shift), *args, **kwargs)
        except search_module._StatisticsChanged as exc:
            raised.append(type(exc).__name__)
            raise

    reference = _search(clean_runtime, deployment, query=TERM, limit=50,
                        filter={"subtype": "ticket"})
    with monkeypatch.context() as patch:
        patch.setattr(search_module, "execute_with_custom_plan", marking)
        patch.setattr(search_module, "_filtered_passage_read", noisy)
        page = _search(clean_runtime, deployment, query=TERM, limit=50,
                       filter={"subtype": "ticket"})
    assert state["windows"] >= 2, "the read must reach a second window"
    if moved:
        assert raised[:1] == ["_OrderMoved"]
    else:
        assert raised == []
        assert "passage_read_fallback" not in page
    assert page == reference


def test_a_real_spill_during_the_read_keeps_it_exact(
    clean_runtime, tmp_path, monkeypatch,
):
    """A real memtable spill (`bm25_spill_index`) right after the first
    window: whatever moved, the answer equals the previous statement's."""
    groups, main = _length_facts(paged=140)
    deployment = _publish_still(tmp_path, clean_runtime, groups=groups, main=main)
    assert _memtable_gap(clean_runtime) > 1e-3
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _segment, index_name = search_module.resolve_node_text_bm25_index(conn)
    monkeypatch.setattr(search_module, "_PASSAGE_FIRST_WINDOW", 16)
    with monkeypatch.context() as patch:
        attempts = _spill_after_first_window(patch, clean_runtime, index_name)
        reads = _Reads(patch)
        page = _search(clean_runtime, deployment, query=TERM, limit=50,
                       filter={"subtype": "ticket"})
    (read,) = reads.passage_reads
    assert attempts and _memtable_gap(clean_runtime) <= 1e-3  # it did spill
    windowed, (new,), full, (old,) = _both_reads(
        clean_runtime, deployment, monkeypatch, query=TERM, limit=50,
        filter={"subtype": "ticket"},
    )
    assert _rows(read) == _rows(old) == _rows(new)
    assert page["results"] == windowed["results"]


def test_a_long_passage_is_always_scored(clean_runtime, tmp_path, monkeypatch):
    """Review final round, finding 3b: past 65,535 tokens the index's
    single-term path wraps a document's length, so a passage that long has
    no index-score bound; it is always scored."""
    groups, main = _length_facts(n=40)
    long_text = f"{TERM} " + "filler " * 10_000  # 70,000+ characters
    groups[OPEN].append(_node("comment:long", "comment", long_text))
    main.append(_edge("ticket:t:000", "comment:long"))
    deployment = _publish_still(tmp_path, clean_runtime, groups=groups, main=main)
    windowed, (new,), full, (old,) = _both_reads(
        clean_runtime, deployment, monkeypatch, query=TERM, limit=50,
        filter={"subtype": "ticket"},
    )
    assert "comment:long" in [node_id for node_id, _score in _rows(new)]
    assert _rows(new) == _rows(old)
