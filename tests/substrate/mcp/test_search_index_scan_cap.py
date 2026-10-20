"""Lexical reads the BM25 index's result cap may cut (PACT lexical-index-scan-cap-3162).

pg_textsearch 1.3.0 through 1.4.x returns at most 100,000 results from one
index scan. A lexical read whose selections run as a Filter on the scan's
level starts its result list at the read's LIMIT, doubles it, and stops at
the cap without an error, so a query matching more rows than that returns
too few, silently. pg_textsearch 1.5.0 removed the cap (upstream PR #518);
ops/pg/Dockerfile ships 1.5.1. The guard stays while databases still run an
older library, so the tests below hold on both: where a test depends on the
cut, it asserts the cut before 1.5 and a complete read from 1.5 on.

* On a scratch table with 110,000 better-scoring matches of another type,
  the read as 38f049bcbc writes it returns short before 1.5 and equals the
  no-index read from 1.5 on; the complete read returns the rows of the same
  statement run without index scans, in the same order, on every one of
  several runs on one connection.
* A full read stands with no extra statement; every short read runs its
  complete form, also when it was not cut, and when the cut comes from index
  entries the statistics miss (uncommitted inserts, dead rows after a
  statistics reset).
* Through `search()`, the self, expanded self, passage and chunk reads run
  their complete form and say so. These exercise the wiring only: the
  extension's own cap is compiled in, so a small graph is never cut.
* A read that cannot be completed is refused, never served short.

Policy option A (Jason, 2026-10-03): all of the above runs only when the
caller asks for `completeness="exact"`. The default serves the native
candidates, runs no exact statement, and says the page may be incomplete:
null count with basis `unknown`, no absence claim, and the exact rerun as
recovery.
"""
from __future__ import annotations

import importlib
import json
from typing import Any

import psycopg
import pytest
from psycopg.rows import dict_row

from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.call_read_cache import call_read_cache
from okg.substrate.mcp.record_fold import (
    NEXT_OFFSET_REASON_BEYOND_EXACT_ORDER,
    RecordsOffsetBeyondExactOrder,
)
from okg.substrate.mcp.result_text import render
from tests.substrate.mcp.test_search_records import QUERY, _basic, _bulk_facts, _ids, _publish

pytestmark = pytest.mark.postgres

search_module = importlib.import_module("okg.substrate.mcp.search")
CAP = search_module._LEXICAL_RANK_CAP
TABLE = "scan_cap_nodes"
INDEX = "scan_cap_bm25"


# ---------- the scratch table ----------

def _table(conn: psycopg.Connection, *, decoys: int, chunks: int = 300,
           others: int = 0, distinct: bool = False) -> None:
    """`decoys` short matches of type `other` outrank `chunks` longer
    matches of type `chunk`; `others` rows do not match. Texts stay under 40
    tokens (lengths below 40 are stored exactly) and the index is spilled to
    a written segment, so the index's scores equal `<@>`'s."""
    conn.execute(f"DROP TABLE IF EXISTS {TABLE}")
    conn.execute(
        f"CREATE TABLE {TABLE} (node_id text PRIMARY KEY, subtype text, attrs jsonb, "
        "text_hash text, source text, source_record_id text, source_revision text, "
        "deleted bool DEFAULT false, live_lo bigint DEFAULT 0, "
        "live_hi bigint DEFAULT 9223372036854775807) WITH (autovacuum_enabled = off)"
    )
    conn.execute(
        f"INSERT INTO {TABLE} (node_id, subtype, attrs) SELECT 'o' || g, 'other', "
        "jsonb_build_object('text', 'alpha beta') FROM generate_series(1, %s) g",
        (decoys,),
    )
    conn.execute(
        f"INSERT INTO {TABLE} (node_id, subtype, attrs) SELECT 'x' || g, 'other', "
        "jsonb_build_object('text', 'gamma delta') FROM generate_series(1, %s) g",
        (others,),
    )
    # 5..34 filler words: 30 score levels, ties broken by node id; with
    # `distinct`, one length per row (all scores differ).
    fillers = "g" if distinct else "5 + g %% 30"
    conn.execute(
        f"INSERT INTO {TABLE} (node_id, subtype, attrs) SELECT "
        f"'c' || lpad(g::text, 4, '0'), 'chunk', "
        f"jsonb_build_object('text', 'alpha ' || repeat('filler ', {fillers})) "
        "FROM generate_series(1, %s) g",
        (chunks,),
    )
    conn.execute(
        f"CREATE INDEX {INDEX} ON {TABLE} USING bm25 ((attrs->>'text')) "
        "WITH (text_config = 'english') WHERE deleted = FALSE AND attrs ? 'text'"
    )
    conn.execute(f"ANALYZE {TABLE}")
    conn.execute("SELECT bm25_spill_index(%s)", (INDEX,))


WHERE = [
    "deleted = FALSE", "live_lo <= %(bp)s", "%(bp)s < live_hi", "attrs ? 'text'",
    "subtype = %(subtype)s",
]
PARAMS = {"q": "alpha", "bp": 1, "subtype": "chunk"}
ORDER = f"(attrs->>'text') <@> to_bm25query(%(q)s, '{INDEX}')"
#: The self read as 38f049bcbc writes it (`ranked_sql` in `_search_lexical`).
DEV_SQL = (
    f"SELECT * FROM (SELECT node_id, subtype, attrs, text_hash, source, "
    f"source_record_id, source_revision, {ORDER} AS score FROM {TABLE} "
    f"WHERE {' AND '.join(WHERE)} ORDER BY {ORDER}, node_id LIMIT {CAP}"
    f") AS ranked WHERE ranked.score < 0"
)


def _dev_read(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """The dev statement under the index plan, after asserting that plan is
    the BM25 index scan with the selections as its Filter."""
    with search_module._index_plan(conn) as cur:
        search_module.execute_with_custom_plan(cur, "EXPLAIN (FORMAT JSON) " + DEV_SQL, PARAMS)
        document = next(iter(cur.fetchone().values()))
        nodes = list(search_module._plan_nodes(document[0]["Plan"]))
        (scan,) = [node for node in nodes if node.get("Index Name") == INDEX]
        assert scan["Node Type"] == "Index Scan" and scan.get("Order By")
        assert "subtype" in scan.get("Filter", "")
        search_module.execute_with_custom_plan(cur, DEV_SQL, PARAMS)
        return cur.fetchall()


def _exact(conn: psycopg.Connection) -> list[tuple[str, float]]:
    """The dev statement with index scans disabled: no index, no cap."""
    with conn.transaction():
        conn.execute("SET LOCAL enable_indexscan = off")
        with conn.cursor(row_factory=dict_row) as cur:
            search_module.execute_with_custom_plan(cur, DEV_SQL, PARAMS)
            return _pairs(cur.fetchall())


def _pairs(rows: list[dict[str, Any]]) -> list[tuple[str, float]]:
    return [(str(row["node_id"]), float(row["score"])) for row in rows]


def _library_version(conn: psycopg.Connection) -> tuple[int, int]:
    """The loaded pg_textsearch library's major and minor version: the
    library, not `pg_extension.extversion`, runs the scan."""
    raw = conn.execute("SELECT current_setting('pg_textsearch.library_version')").fetchone()[0]
    major, minor = str(raw).split(".")[:2]
    return int(major), int(minor)


#: The first pg_textsearch release whose index scan has no result cap.
UNCAPPED = (1, 5)


class _Statements:
    """The statements `_IndexScanCap` runs: statistics reads, counts,
    complete reads."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.statistics = 0
        self.texts: list[str] = []
        original = search_module.execute_with_custom_plan

        def counting(cur: Any, sql: Any, params: Any = None) -> Any:
            self.texts.append(str(sql))
            return original(cur, sql, params)

        monkeypatch.setattr(search_module, "execute_with_custom_plan", counting)

    def count(self, mark: str) -> int:
        return sum(1 for text in self.texts if mark in text and "EXPLAIN" not in text)


COUNT_MARK = "input_size AS MATERIALIZED"
COMPLETE_MARK = "scored AS MATERIALIZED"


def _complete(conn: psycopg.Connection, rows: list[dict[str, Any]]):
    cap = search_module._IndexScanCap(conn, segment=TABLE, index_name=INDEX)
    return cap.read_complete(rows, where=WHERE, limit=CAP, params=PARAMS)


def test_matches_past_the_index_cap_are_read_completely_in_the_exact_order(
    clean_runtime, monkeypatch,
):
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _table(conn, decoys=110_000)
        exact = _exact(conn)
        assert len(exact) == CAP
        dev = _dev_read(conn)
        if _library_version(conn) < UNCAPPED:
            # Failure case: the read as 38f049bcbc writes it is cut by the cap.
            assert len(dev) < CAP
        else:
            # pg_textsearch 1.5+: the same read walks past 100,000 index
            # results and returns the no-index read's rows, in its order. A
            # full read stands with no statement, so the complete read below
            # starts from an empty one.
            assert _pairs(dev) == exact
            dev = []
        statements = _Statements(monkeypatch)
        # Six runs on one connection: no reused plan loses the scan's LIMIT.
        for run in range(6):
            rows, matches = _complete(conn, dev)
            assert matches.candidates == 300 and matches.matches == 300, run
            assert _pairs(rows) == exact, run
        assert statements.count(COMPLETE_MARK) == 6
        assert statements.count(COUNT_MARK) == 6


def test_the_final_order_is_the_score_order_with_distinct_scores(clean_runtime):
    """Every chunk has its own length, so every score differs: the rows come
    back in strictly rising score order, as the no-index statement orders
    them (an ORDER BY on an unselected `<@>` read a stale score)."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _table(conn, decoys=30, chunks=38, distinct=True)
        exact = _exact(conn)
        rows, matches = _complete(conn, [])
        assert matches.candidates == 38 and matches.matches == 38
        scores = [score for _id, score in _pairs(rows)]
        assert len(rows) == 38 and scores == sorted(scores) and len(set(scores)) == 38
        assert _pairs(rows) == exact


def test_a_short_read_under_the_cap_is_read_again_and_stays_exact(
    clean_runtime, monkeypatch,
):
    """20,150 matches: not cut, but no statistic may say so, so the read
    runs again; the rows equal the no-index statement's."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _table(conn, decoys=20_000, others=1_000, chunks=150)
        dev = _dev_read(conn)
        exact = _exact(conn)
        assert len(dev) == 150 and _pairs(dev) == exact
        statements = _Statements(monkeypatch)
        rows, matches = _complete(conn, dev)
        assert matches.candidates == 150 and matches.matches == 150 and _pairs(rows) == exact
        assert statements.count(COUNT_MARK) == 1 and statements.count(COMPLETE_MARK) == 1


def test_the_exact_read_keeps_and_counts_a_match_scored_zero(clean_runtime):
    """Membership, not the score's sign, decides the exact read (#3232).

    `<@>` can score a real match 0 (bm25-zero-score-membership-3232). Here a
    `<@>` earlier on the search path scores the chunk holding "item 3" 0, as
    the transient pg_textsearch read does. The exact statement keeps that
    chunk, ranks it last, and counts it: 5 matches of 8 scoped candidates.
    The three zero-score non-matches never enter the page or the count. With
    the sign test the same statement returned 4 rows and a count of 4, an
    exhaustive claim one match short."""
    from tests.substrate.test_bm25_node_search import _score_one_match_zero

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _table(conn, decoys=0, chunks=0)
        conn.execute(
            f"INSERT INTO {TABLE} (node_id, subtype, attrs) SELECT 'm' || g, 'chunk', "
            "jsonb_build_object('text', 'alpha bravo item ' || g) FROM generate_series(0, 4) g"
        )
        conn.execute(
            f"INSERT INTO {TABLE} (node_id, subtype, attrs) SELECT 'n' || g, 'chunk', "
            "jsonb_build_object('text', 'gamma delta ' || g) FROM generate_series(0, 2) g"
        )
        conn.execute(f"ANALYZE {TABLE}")
        _score_one_match_zero(conn, "item 3")
        signed = conn.execute(
            f"SELECT count(*) FILTER (WHERE (attrs->>'text') <@> to_bm25query('alpha', "
            f"'{INDEX}') < 0) FROM {TABLE} WHERE subtype = 'chunk'"
        ).fetchone()[0]
        assert signed == 4  # the zero-scoring operator is in force
        rows, matches = _complete(conn, [])
        assert matches.candidates == 8 and matches.matches == 5
        assert [row["node_id"] for row in rows][-1] == "m3"
        assert sorted(row["node_id"] for row in rows) == [f"m{g}" for g in range(5)]
        assert float(rows[-1]["score"]) == 0.0
        none, nothing = search_module._IndexScanCap(
            conn, segment=TABLE, index_name=INDEX,
        ).read_complete([], where=WHERE, limit=CAP, params={**PARAMS, "q": "zzqxnotarealword"})
        assert none == [] and nothing.candidates == 8 and nothing.matches == 0


def test_a_full_read_stands_with_no_statement(clean_runtime, monkeypatch):
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        statements = _Statements(monkeypatch)
        cap = search_module._IndexScanCap(conn, segment=TABLE, index_name=INDEX)
        full = [{"node_id": str(i)} for i in range(CAP)]
        assert cap.read_complete(full, where=WHERE, limit=CAP, params=PARAMS) == (full, None)
        assert statements.statistics == 0 and statements.texts == []


# ---------- entries the statistics miss (review round 1) ----------

def _visible_in_first(conn: psycopg.Connection, n: int) -> int:
    """Visible rows among the first `n` results of the unfiltered index scan
    (literal LIMIT, forced index plan)."""
    with search_module._index_plan(conn) as cur:
        search_module.execute_with_custom_plan(
            cur,
            f"SELECT count(*) AS visible FROM (SELECT 1 FROM {TABLE} "
            f"WHERE deleted = FALSE AND attrs ? 'text' ORDER BY {ORDER} LIMIT {int(n)}) s",
            PARAMS,
        )
        return int(cur.fetchone()["visible"])


def test_uncommitted_inserts_on_another_connection_never_shorten_the_read(clean_runtime):
    """95,300 visible matches, plus 10,000 a second connection is inserting
    and has not committed: their index entries take scan slots and the
    statistics do not see them. The served rows are the complete ones.

    The precondition never runs the old filtered statement under the writer
    (that statement crashed a server once, cause unconfirmed): an unfiltered
    scan with a literal LIMIT of 100,000 returns fewer visible rows than
    exist, so the uncommitted entries use up slots."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _table(conn, decoys=95_000)
        exact = _exact(conn)
        assert len(exact) == CAP
        assert _visible_in_first(conn, 100_000) == 95_300
        with psycopg.connect(clean_runtime) as writer:
            writer.execute(
                f"INSERT INTO {TABLE} (node_id, subtype, attrs) SELECT 'w' || g, 'other', "
                "jsonb_build_object('text', 'alpha beta') FROM generate_series(1, 10000) g"
            )
            if _library_version(conn) < UNCAPPED:
                assert _visible_in_first(conn, 100_000) < 95_300
            rows, matches = _complete(conn, [])
            # The index's statistics count the uncommitted rows, so scores
            # move; compare with the no-index statement under the same ones.
            exact_now = _exact(conn)
            writer.rollback()
        assert matches.candidates == 300 and matches.matches == 300 and _pairs(rows) == exact_now
        assert [node_id for node_id, _s in exact_now] == [node_id for node_id, _s in exact]


def test_dead_rows_after_a_statistics_reset_never_shorten_the_read(clean_runtime):
    """105,300 index entries, 10,000 of them deleted rows not yet vacuumed;
    after `pg_stat_reset()` the dead row estimate is 0, so matches + dead
    looks under the cap while the scan is cut. The served rows are the
    complete ones."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _table(conn, decoys=105_000)
        conn.execute(
            f"DELETE FROM {TABLE} WHERE node_id IN "
            "(SELECT 'o' || g FROM generate_series(1, 10000) g)"
        )
        # Flush this backend's pending counts first, so the reset clears the
        # DELETE's dead rows instead of their arriving after it.
        conn.execute("SELECT pg_stat_force_next_flush()")
        conn.execute("SELECT 1")
        try:
            conn.execute("SELECT pg_stat_reset()")
        except psycopg.errors.InsufficientPrivilege:
            pytest.skip("pg_stat_reset() needs a privileged role in this test database")
        exact = _exact(conn)
        assert len(exact) == CAP
        dev = _dev_read(conn)
        if _library_version(conn) >= UNCAPPED:
            # pg_textsearch 1.5+: dead entries no longer cut the scan.
            assert _pairs(dev) == exact
            return
        assert len(dev) < CAP
        cap = search_module._IndexScanCap(conn, segment=TABLE, index_name=INDEX)
        rows, matches = cap.read_complete(dev, where=WHERE, limit=CAP, params=PARAMS)
        assert matches.candidates == 300 and matches.matches == 300 and _pairs(rows) == exact


def test_a_short_windowed_passage_read_under_uncommitted_inserts_is_served_complete(
    clean_runtime, tmp_path,
):
    """#3161's windowed passage read took a window that came back short,
    before its scan room, for the end of the matches. 95,000 committed
    better-scoring rows (outside the pin) fill most of the room; a second
    connection's 10,000 uncommitted ones take the rest of the scan's slots,
    so the window covering the room comes back short and the read used to
    end there without one passage. The page served under the writer equals
    the page served without it, and says the passages were read completely.
    The precondition is an unfiltered count, never the old filtered
    statement under the writer."""
    from tests.substrate.mcp.test_search_filtered_passage_cost import _publish_still

    groups, main = _bulk_facts(hidden_comments=False)
    deployment = _publish_still(tmp_path, clean_runtime, groups=groups, main=main)
    filt = {"subtype": "ticket"}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        segment, index_name = search_module.resolve_node_text_bm25_index(conn)
        literal = index_name.replace("'", "''")
        decoys = (
            f"INSERT INTO {segment} (node_id, subtype, attrs, live_lo, live_hi, "
            "writer_segment_id) SELECT %s || g, 'decoy', "
            "jsonb_build_object('text', %s::text), 0, 1, 0 FROM generate_series(1, %s) g"
        )
        conn.execute(decoys, ("decoy:", QUERY, 95_000))
        conn.execute(f"ANALYZE {segment}")
        visible = (
            f"SELECT count(*) AS visible FROM (SELECT 1 FROM {segment} "
            f"WHERE deleted = FALSE AND attrs ? 'text' ORDER BY "
            f"(attrs->>'text') <@> to_bm25query(%(q)s, '{literal}') LIMIT 100000) s"
        )

        def slots() -> int:
            with search_module._index_plan(conn) as cur:
                search_module.execute_with_custom_plan(cur, visible, {"q": QUERY})
                return int(cur.fetchone()["visible"])

        before = slots()
        capped = _library_version(conn) < UNCAPPED
        session = open_session(conn, deployment_name=deployment)
        baseline = search(conn, session, QUERY, method="lexical", limit=60, filter=filt,
                          completeness="exact")
        with psycopg.connect(clean_runtime) as writer:
            writer.execute(decoys, ("pending:", QUERY, 10_000))
            if capped:
                assert slots() < before
            served = search(conn, session, QUERY, method="lexical", limit=60, filter=filt,
                            completeness="exact")
            writer.rollback()
    assert before > 95_000
    if capped:
        assert "passages" in served["index_scan_cap_reads"]
    assert "passages" not in (baseline.get("index_scan_cap_reads") or [])
    for page in (served, baseline):
        for key in ("index_scan_cap_reads", "lexical_exact_reads", "passage_read_fallback"):
            page.pop(key, None)
    # The index's statistics count the uncommitted rows, so scores move;
    # the records and their order do not.
    assert served["results"] and _ids(served) == _ids(baseline)
    assert served["total_count"] == baseline["total_count"]


# ---------- through search(): the wiring ----------

def _search(dsn: str, deployment: str, query: str = QUERY, **kwargs: Any) -> dict:
    """Exact unless the caller names a mode: these exercise the exact reads."""
    kwargs.setdefault("completeness", "exact")
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        return search(conn, session, query, method="lexical", **kwargs)


def _same_page(served: dict, unguarded: dict) -> dict:
    """`served` less its disclosure must equal `unguarded`; returns the disclosure."""
    disclosure = {
        key: served.pop(key) for key in ("index_scan_cap_reads", "lexical_exact_reads")
    }
    assert json.dumps(served, sort_keys=True, default=str) == json.dumps(
        unguarded, sort_keys=True, default=str,
    )
    return disclosure


@pytest.mark.parametrize(
    ("kwargs", "reads"),
    [
        ({"rank_by": "self"}, ["self"]),
        ({"filter": {"subtype": "ticket"}}, ["self", "passages"]),
        ({"filter": {"subtype": "ticket"}, "rank_by": "passages"}, ["chunks"]),
    ],
)
def test_short_reads_run_their_complete_form_and_say_so(
    clean_runtime, tmp_path, monkeypatch, kwargs, reads,
):
    """Wiring only (nothing is cut on a small graph): each short read runs
    its complete form, the page equals the page served without the guard,
    and the payload says which reads ran and how many rows match."""
    deployment = _basic(tmp_path, clean_runtime)
    # The passage read's windows fall back to the full statement only when
    # their own room runs out (PACT mcp-filtered-passage-cost).
    monkeypatch.setattr(search_module, "_PASSAGE_INDEX_SCAN_CAP", 3)
    with monkeypatch.context() as patch:
        # Every read stands as read: the code before this change.
        patch.setattr(
            search_module._IndexScanCap, "read_complete",
            lambda self, rows, **pieces: (rows, None),
        )
        unguarded = _search(clean_runtime, deployment, limit=20, **kwargs)
    assert "index_scan_cap_reads" not in unguarded
    statements = _Statements(monkeypatch)
    served = _search(clean_runtime, deployment, limit=20, **kwargs)
    disclosure = _same_page(served, unguarded)
    assert disclosure["index_scan_cap_reads"] == reads
    assert set(disclosure["lexical_exact_reads"]) == set(reads)
    for name, facts in disclosure["lexical_exact_reads"].items():
        assert facts["unit"] == "text_rows"
        assert facts["set"] == f"{name}_stored_scoped_input"
        assert facts["basis"] == "exact_scalar"
        assert facts["candidate_rows"] >= facts["matching_rows"] >= 0
    assert statements.statistics == 0
    assert statements.count(COMPLETE_MARK) == len(reads)


def test_an_expanded_query_runs_its_own_complete_read(clean_runtime, monkeypatch):
    """A declared concept group expands the query: the expanded self read
    is short too and runs its own count and complete form."""
    from tests.substrate.mcp.test_concept_expansion_ranking import SCOPE, _seed_and_publish

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_and_publish(clean_runtime, conn)
        session = open_session(conn, deployment_name="default")
        statements = _Statements(monkeypatch)
        page = search(conn, session, "throughput", method="lexical", limit=20, scope=SCOPE,
                      completeness="exact")
    assert page["concept_expansion"]["applied"] is True
    assert page["index_scan_cap_reads"] == ["self", "self_expanded"]
    assert {row["node_id"] for row in page["results"]} == {"n:ce:exact", "n:ce:variant"}
    assert statements.count(COUNT_MARK) == 2 and statements.count(COMPLETE_MARK) == 2


def test_a_full_self_read_runs_no_extra_statement(clean_runtime, tmp_path, monkeypatch):
    groups, main = _bulk_facts(hidden_comments=False)
    deployment = _publish(tmp_path, clean_runtime, groups=groups, main=main)
    statements = _Statements(monkeypatch)
    page = _search(clean_runtime, deployment, limit=10, rank_by="self")
    assert page["rank_capped"] is True
    assert "index_scan_cap_reads" not in page
    assert statements.statistics == 0 and statements.count(COUNT_MARK) == 0


def test_a_rerun_call_discloses_a_cached_complete_read(clean_runtime, tmp_path, monkeypatch):
    """The byte-budget wrapper's reruns reuse the cached read; the
    disclosure rides with the cached rows."""
    deployment = _basic(tmp_path, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        with call_read_cache():
            first = search(conn, session, QUERY, method="lexical", limit=20, rank_by="self",
                           completeness="exact")
            statements = _Statements(monkeypatch)
            second = search(conn, session, QUERY, method="lexical", limit=5, rank_by="self",
                            completeness="exact")
    assert first["index_scan_cap_reads"] == second["index_scan_cap_reads"] == ["self"]
    assert statements.count(COMPLETE_MARK) == 0


# ---------- the bounded default (policy option A) ----------

#: The bounded page's `coverage.retrieval` (PACT operator-page-counts-3190's
#: shape, pre-filled by lexical search; `with_read_coverage` keeps it). No
#: `bounds`: basis bm25_index under status bounded says the scan may have
#: stopped; `recovery` holds the arguments to rerun with, from offset 0.
BOUNDED = {
    "status": "bounded", "basis": "bm25_index", "recovery": {"completeness": "exact"},
}


@pytest.mark.parametrize(
    "kwargs",
    [{"rank_by": "self"}, {"filter": {"subtype": "ticket"}},
     {"filter": {"subtype": "ticket"}, "rank_by": "passages"}],
)
def test_the_default_runs_no_exact_read_and_says_it_is_bounded(
    clean_runtime, tmp_path, monkeypatch, kwargs,
):
    """Same rows as the exact page on a graph nothing cuts, but no exact
    statement, no count and the recovery named. Fails on fd88b77787, which
    ran the exact read on every short read."""
    deployment = _basic(tmp_path, clean_runtime)
    monkeypatch.setattr(search_module, "_PASSAGE_INDEX_SCAN_CAP", 3)
    exact = _search(clean_runtime, deployment, limit=20, **kwargs)
    statements = _Statements(monkeypatch)
    bounded = _search(clean_runtime, deployment, limit=20, completeness="bounded", **kwargs)
    default = _search(clean_runtime, deployment, limit=20, completeness=None, **kwargs)
    assert statements.count(COMPLETE_MARK) == 0 and statements.count(COUNT_MARK) == 0
    for page in (bounded, default):
        assert page["results"] and _ids(page) == _ids(exact)
        assert page["total_count"] is None and page["total_count_basis"] == "unknown"
        assert page["completeness"] == "bounded"
        assert page["coverage"]["retrieval"] == BOUNDED
        assert "index_scan_cap_reads" not in page and "lexical_exact_reads" not in page
    assert exact["completeness"] == "exact"
    assert exact["coverage"]["retrieval"] == {"status": "exhaustive", "basis": "exact_scalar"}
    assert isinstance(exact["total_count"], int)


@pytest.mark.parametrize(
    ("reason", "kept"),
    [("no_match", False), ("not_tokenized", False), ("no_rows", True), ("not_indexed", True)],
)
def test_a_bounded_zero_hit_makes_no_absence_claim(
    clean_runtime, tmp_path, monkeypatch, reason, kept,
):
    """`no_match` and `not_tokenized` start from "BM25 matched nothing",
    which a possibly cut read cannot establish; the row-population reasons
    stand. Exact keeps every reason."""
    deployment = _basic(tmp_path, clean_runtime)
    monkeypatch.setattr(
        search_module, "_diagnose_subtype_zero_hits",
        lambda *a, **k: (reason != "no_rows" or None, reason, "all"),
    )
    pages = {
        mode: _search(clean_runtime, deployment, "nonexistentcontrolnonce", limit=20,
                      filter={"subtype": "ticket"}, completeness=mode)
        for mode in ("bounded", "exact")
    }
    assert not pages["bounded"]["results"] and not pages["exact"]["results"]
    assert pages["exact"]["zero_hit_reason"] == reason
    assert pages["exact"]["total_count"] == 0
    assert pages["bounded"]["total_count"] is None
    assert ("zero_hit_reason" in pages["bounded"]) is kept


def test_the_call_cache_binds_the_mode(clean_runtime, tmp_path, monkeypatch):
    """One cache, both modes: a bounded read is never served as exact and
    an exact read never as bounded; a rerun in the same mode reuses it."""
    deployment = _basic(tmp_path, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        statements = _Statements(monkeypatch)
        with call_read_cache():
            pages = [
                search(conn, session, QUERY, method="lexical", limit=limit,
                       rank_by="self", completeness=mode)
                for mode, limit in (("bounded", 20), ("exact", 20), ("exact", 5),
                                    ("bounded", 5))
            ]
    assert statements.count(COMPLETE_MARK) == 1
    assert [page["completeness"] for page in pages] == [
        "bounded", "exact", "exact", "bounded",
    ]
    assert [("index_scan_cap_reads" in page) for page in pages] == [False, True, True, False]
    assert pages[0]["total_count"] is None and pages[3]["total_count"] is None
    assert pages[1]["total_count"] == pages[2]["total_count"]


@pytest.mark.parametrize("rank_by", ["self", "records"])
def test_exact_is_not_exhaustive_after_filters_cut_a_full_read(
    clean_runtime, tmp_path, rank_by,
):
    """Review H1. 300 equal-scoring matches fill the self read's 200-row
    candidate limit, so exact keeps the native read; `attr_filters` then keep
    the 20 of those 200 marked `keep`, although 30 exist. The page must not
    call 20 exact or retrieval exhaustive. Fails on e5e286c642 (77028d9eb9
    plus #3213), which counted the filtered list (20 < 200) as exact.

    The query has a second word no row holds. BM25 matches any query word,
    so every row matches; the effective-attribute read that also serves
    `attr_filters` requires every word (ILIKE), so it adds nothing and
    cannot hide the cut, as it would for a one-word query."""
    from tests.substrate.mcp.search_fixture import make_search_deployment, node, publish_nodes

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes = [
        node(f"hit:{i:04}", "selected", f"alpha beta {i}", kind="keep" if i % 10 == 0 else "drop")
        for i in range(300)
    ]
    publish_nodes(clean_runtime, deployment, subtypes=["selected"], nodes=nodes)
    page = _search(clean_runtime, deployment, "alpha zzqnone", rank_by=rank_by, limit=50,
                   filter={"subtype": "selected", "attr_filters": {"kind": "keep"}})
    assert len(page["results"]) == 20  # the keep rows among the first 200
    assert sum(1 for fact in nodes if fact.attrs["kind"] == "keep") == 30
    # The failure case first: the count of a filtered full read is a floor.
    assert page["total_count"] == 20 and page["total_count_basis"] == "lower_bound"
    assert page["rank_capped"] is True
    assert page["ranked_cap"] == {"rows": 200}
    # Issue #3666: the requested mode, and a listing that is not complete.
    assert page["completeness"] == "exact"
    assert page["listing"]["complete"] is False and page["listing"]["reason"] == "scan_bound"
    assert page["listing"]["ranked_cap"] == {"rows": 200}
    retrieval = page["coverage"]["retrieval"]
    assert retrieval["status"] != "exhaustive"
    assert retrieval == {"status": "bounded", "basis": "bm25_index", "bounds": ["candidate_cap"]}
    # The full read kept its native fast path: no exact statement for it.
    assert "self" not in (page.get("index_scan_cap_reads") or [])


# ---------- refusals ----------

@pytest.mark.parametrize("reason", ["scoped_work_limit", "plan_uses_bm25_index"])
def test_a_read_that_cannot_be_completed_is_refused(
    clean_runtime, tmp_path, monkeypatch, reason,
):
    deployment = _basic(tmp_path, clean_runtime)
    if reason == "scoped_work_limit":
        monkeypatch.setattr(search_module, "_EXACT_LEXICAL_WORK_ROWS", 3)
    else:
        monkeypatch.setattr(search_module, "_scalar_lexical_plan", lambda *a, **k: False)
    with pytest.raises(search_module.LexicalIndexScanCapRefused) as caught:
        _search(clean_runtime, deployment, limit=20, rank_by="self")
    error = caught.value
    assert error.code == "lexical_index_scan_cap"
    assert error.data["reason"] == reason
    assert "narrow the query" in error.fix_hint
    if reason == "scoped_work_limit":
        assert error.data["scoped_candidates"] == 4


# ---------- the MCP database role ----------

@pytest.mark.parametrize(
    ("kwargs", "reads"),
    [({"rank_by": "self"}, ["self"]), ({"filter": {"subtype": "ticket"}}, ["self", "passages"])],
)
def test_the_complete_read_runs_as_the_reviewed_mcp_database_role(
    clean_runtime, tmp_path, monkeypatch, kwargs, reads,
):
    """`okg_mcp` reads the table statistics, runs the forced-plan count and
    the complete read (the index's own summary function is superuser-only)."""
    deployment = _basic(tmp_path, clean_runtime)
    monkeypatch.setattr(search_module, "_PASSAGE_INDEX_SCAN_CAP", 3)
    statements = _Statements(monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        conn.execute("SET ROLE okg_mcp")
        try:
            page = search(conn, session, QUERY, method="lexical", limit=20,
                          completeness="exact", **kwargs)
        finally:
            conn.execute("RESET ROLE")
    assert page["index_scan_cap_reads"] == reads and page["results"]
    assert statements.statistics == 0 and statements.count(COUNT_MARK) == len(reads)
    assert statements.count(COMPLETE_MARK) == len(reads)


@pytest.mark.parametrize("preexisting_ms", [0, 250, 2000])
def test_exact_read_timeout_restores_outer_transaction_settings(clean_runtime, monkeypatch, preexisting_ms):
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _table(conn, decoys=10, chunks=5)
        original = search_module.execute_with_custom_plan
        observed = []

        def cancel(cur, sql, params=None):
            if str(sql).startswith(search_module.INDEX_SCAN_CAP_MARK):
                cur.execute("SELECT setting::integer AS milliseconds FROM pg_settings WHERE name = 'statement_timeout'")
                observed.append(cur.fetchone()["milliseconds"])
                cur.execute("SELECT pg_sleep(1.0)")
            return original(cur, sql, params)

        # Every statement in the bounded block, the setup read included, runs
        # under the stricter of the two timeouts. Millisecond timeouts cancel
        # that setup on a loaded host before the marked statement runs (#3261),
        # so both stay far above a plain statement's latency, and the sleep
        # stays far above both.
        monkeypatch.setattr(search_module, "_EXACT_LEXICAL_TIMEOUT_MS", 500)
        monkeypatch.setattr(search_module, "execute_with_custom_plan", cancel)
        # The plan check (tested on its own below) is an EXPLAIN under the
        # same caller timeout, and this test is about the marked statement.
        monkeypatch.setattr(search_module, "_scalar_lexical_plan", lambda *a, **k: True)
        with conn.transaction():
            conn.execute("SELECT set_config('statement_timeout', %s, true)", (str(preexisting_ms),))
            with pytest.raises(search_module.LexicalIndexScanCapRefused) as caught:
                _complete(conn, [])
            assert caught.value.data["reason"] == "exact_read_timeout"
            assert observed == [min(preexisting_ms, 500) if preexisting_ms else 500]
            assert caught.value.data["timeout_ms"] == observed[0]
            assert conn.execute("SELECT setting::integer FROM pg_settings WHERE name = 'statement_timeout'").fetchone()[0] == preexisting_ms
            assert conn.execute("SELECT 1").fetchone()[0] == 1


def test_work_limit_refusal_reports_the_bounded_scoped_input(clean_runtime, monkeypatch):
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _table(conn, decoys=10, chunks=5)
        monkeypatch.setattr(search_module, "_EXACT_LEXICAL_WORK_ROWS", 3)
        cap = search_module._IndexScanCap(conn, segment=TABLE, index_name=INDEX)
        with pytest.raises(search_module.LexicalIndexScanCapRefused) as caught:
            cap.read_complete([], where=WHERE, limit=CAP, params={**PARAMS, "q": "alpha"})
        assert caught.value.data["reason"] == "scoped_work_limit"
        assert caught.value.data["scoped_candidates"] == 4


def test_scalar_plan_rejects_capped_index_scan(clean_runtime):
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _table(conn, decoys=110_000)
        with search_module._index_plan(conn) as cur:
            assert not search_module._scalar_lexical_plan(cur, DEV_SQL, PARAMS, INDEX)


@pytest.mark.slow
def test_published_generation_recovers_late_filtered_matches(
    clean_runtime, tmp_path, monkeypatch,
):
    """Real fact admission/publication, rather than only a scratch SQL table."""
    from okg.substrate.library.sources.base import NodeFact
    from tests.pytest.catalog_fixture_builders import SubtypeSpec
    from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes = [
        NodeFact(
            node_id=f"decoy:{i:06}", subtype="decoy",
            attrs={"text": "alpha beta"}, source_record_id={"id": i},
            source_revision={"v": 1},
        ) for i in range(110_000)
    ] + [
        NodeFact(
            node_id=f"selected:{i:04}", subtype="selected",
            attrs={"text": "alpha " + "filler " * 20},
            source_record_id={"id": 110_000 + i}, source_revision={"v": 1},
        ) for i in range(300)
    ]
    _, generation = publish_nodes(
        clean_runtime, deployment,
        subtypes=[SubtypeSpec(name, metadata={"searchable_text_fields": ["text"]})
                  for name in ("decoy", "selected")],
        nodes=nodes,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        assert str(session.generation_id) == generation
        segment, index_name = search_module.resolve_node_text_bm25_index(conn)
        conn.execute(f"ANALYZE {segment}")
        conn.execute("SELECT bm25_spill_index(%s)", (index_name,))
        parameters = {"q": "alpha", "bp": search_module.branch_point(conn, session), "subtype": "selected"}
        score = f"(attrs->>'text') <@> to_bm25query(%(q)s, '{index_name}')"
        sql = (
            f"SELECT node_id, {score} AS score FROM {segment} "
            "WHERE deleted = FALSE AND attrs ? 'text' AND live_lo <= %(bp)s "
            "AND %(bp)s < live_hi AND subtype = %(subtype)s "
            f"ORDER BY {score}, node_id LIMIT 200"
        )
        with conn.transaction():
            conn.execute("SET LOCAL enable_indexscan = off")
            with conn.cursor(row_factory=dict_row) as cur:
                search_module.execute_with_custom_plan(cur, sql, parameters)
                expected = [row["node_id"] for row in cur.fetchall()]
        assert len(expected) == 200
        # Failure case: the native read under the index plan is cut short
        # before pg_textsearch 1.5; from 1.5 on it is the no-index read.
        with search_module._index_plan(conn) as cur:
            assert not search_module._scalar_lexical_plan(cur, sql, parameters, index_name)
            search_module.execute_with_custom_plan(cur, sql, parameters)
            native = [row["node_id"] for row in cur.fetchall()]
        if _library_version(conn) >= UNCAPPED:
            assert native == expected
            with search_module._index_plan(conn):
                bounded = search(conn, session, "alpha", method="lexical", rank_by="self",
                                 filter={"subtype": "selected"}, limit=50)
            assert _ids(bounded) == expected[:50]
            return
        assert len(native) < len(expected)
        # Force the affected planner choice through the actual search entry
        # point; an ordinary subtype-index plan already reads these rows.
        # The default (bounded) page first: the native read is cut, and the
        # page must say it may be incomplete instead of claiming a count or
        # an absence, without running the exact statement.
        statements = _Statements(monkeypatch)
        with search_module._index_plan(conn):
            bounded = search(conn, session, "alpha", method="lexical", rank_by="self",
                             filter={"subtype": "selected"}, limit=50)
        assert statements.count(COMPLETE_MARK) == 0
        assert len(bounded["results"]) < 50
        assert set(_ids(bounded)) < set(expected)
        # An empty or short bounded page claims no absence: neither "no
        # match" nor "not tokenized" (both inferred from the cut read).
        assert bounded["total_count"] is None
        assert bounded["total_count_basis"] == "unknown"
        assert bounded["completeness"] == "bounded"
        assert bounded["coverage"]["retrieval"] == BOUNDED
        assert "zero_hit_reason" not in bounded
        assert "index_scan_cap_reads" not in bounded
        # The recovery it names: the same query with completeness="exact".
        with search_module._index_plan(conn):
            page = search(conn, session, "alpha", method="lexical", rank_by="self",
                          filter={"subtype": "selected"}, limit=50,
                          completeness="exact")
        assert statements.count(COMPLETE_MARK) == 1
        assert _ids(page) == expected[:50]
        # 300 late matches fill the 200-row candidate limit: exact reads
        # them all in reference order, and the selection stays a floor.
        assert page["total_count"] == 200 and page["total_count_basis"] == "lower_bound"
        assert page["completeness"] == "exact"
        assert page["listing"]["complete"] is False
        # More than 200 matched: not exhaustive. Its rows came from the exact
        # scalar read, which selected the 200-row candidate limit.
        assert page["coverage"]["retrieval"] == {
            "status": "bounded", "basis": "exact_scalar", "bounds": ["candidate_cap"],
        }
        assert page["lexical_exact_reads"]["self"]["candidate_rows"] == 300
        assert page["lexical_exact_reads"]["self"]["matching_rows"] == 300
        assert "index_scan_matches" not in page


@pytest.mark.parametrize("hidden_query", ["secret", "masked"])
def test_hidden_or_redacted_queries_do_not_emit_raw_match_counts(
    clean_runtime, tmp_path, monkeypatch, hidden_query,
):
    from tests.substrate.mcp.test_search_records import (
        MAIN, MASKED, OPEN, READER, _allow, _nomos_deployment,
    )

    rules = [_allow(OPEN), _allow(MAIN), {
        "rule_id": "redact_masked", "effect": "redact", "actions": ["graph.read.mcp"],
        "subjects": [READER], "match": {"source_id": MASKED}, "priority": 100,
        "obligations": [{"type": "redaction"}, {"type": "audit"}],
    }]
    deployment = _nomos_deployment(tmp_path, clean_runtime, rules)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment, nomos_subject=READER)
        pages = [search(conn, session, query, method="lexical", rank_by="self", limit=20,
                        completeness="exact")
                 for query in (hidden_query, "nonexistentcontrolnonce")]
    # Under enforce the raw read facts are not emitted at all: which reads
    # were short, and their counts, are measured before Nomos (review 5).
    for page in pages:
        assert "lexical_exact_reads" not in page
        assert "index_scan_cap_reads" not in page
        assert "index_scan_matches" not in page
    # A query matching only hidden or redacted rows reads like one matching
    # nothing: both short, both exhaustive.
    assert pages[0]["coverage"]["retrieval"] == pages[1]["coverage"]["retrieval"] == {
        "status": "exhaustive", "basis": "exact_scalar",
    }


@pytest.mark.parametrize("rank_by", ["self", "records"])
def test_an_enforced_where_after_ranking_names_no_cap(
    clean_runtime, tmp_path, monkeypatch, rank_by,
):
    """Issue #3666 review: 220 comments the reader may not see fill the raw
    read. Under enforce a row filter checked after ranking says only that it
    was checked on the ranked rows, not shown whole: no cap, in JSON or
    text, whether or not hidden rows filled the read. Off and under audit
    (which hides no row) the same call names the 200-row cap."""
    from tests.substrate.mcp.test_search_records import MAIN, OPEN, READER, _activate, _allow

    groups, main = _bulk_facts(hidden_comments=True)
    deployment = _publish(tmp_path, clean_runtime, groups=groups, main=main)
    _activate(clean_runtime, deployment, [_allow(OPEN), _allow(MAIN)])
    where = {"op": "in", "field": "attrs.status", "values": ["open"]}

    def page() -> dict:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = open_session(conn, deployment_name=deployment, nomos_subject=READER)
            return search(conn, session, QUERY, method="lexical", rank_by=rank_by,
                          limit=50, where=where)

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    enforced = page()
    assert enforced["where_checked_on"] == {
        "ranked_rows_only": True,
        "every_record": "filter with the same where checks every record, without the text match",
    }
    first = render(enforced, tool="search").splitlines()[0]
    assert "where checked on the ranked rows only (not shown whole)" in first
    assert "top 200" not in first and "cap 200" not in first
    for mode in ("0", "audit"):
        monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
        shown = page()
        assert shown["where_checked_on"]["ranked_cap"]["rows"] == 200, (rank_by, mode)
        assert "ranked_rows_only" not in shown["where_checked_on"], (rank_by, mode)
        line = render(shown, tool="search").splitlines()[0]
        assert "where checked on the top 200 rows" in line, (rank_by, mode)


def test_enforced_output_never_says_hidden_rows_filled_a_read(
    clean_runtime, tmp_path, monkeypatch,
):
    """Review 5. 220 matching comments the reader may not see fill the raw
    200-row read before Nomos hides them; 5 visible tickets match. Under
    enforce neither mode may name `candidate_cap` or the raw reads: bounded
    keeps its plain bounded fact, exact says `unknown` with no reason. Off,
    the same graph names the cap: that is the signal enforce withholds."""
    from tests.substrate.mcp.test_search_records import (
        MAIN, OPEN, READER, _activate, _allow,
    )

    groups, main = _bulk_facts(hidden_comments=True)
    deployment = _publish(tmp_path, clean_runtime, groups=groups, main=main)
    _activate(clean_runtime, deployment, [_allow(OPEN), _allow(MAIN)])

    def pages() -> dict[str, dict]:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = open_session(conn, deployment_name=deployment, nomos_subject=READER)
            return {
                mode: search(conn, session, QUERY, method="lexical", rank_by="self",
                             limit=50, completeness=mode)
                for mode in ("bounded", "exact")
            }

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    enforced = pages()
    for page in enforced.values():
        assert {row["subtype"] for row in page["results"]} <= {"ticket"}
        assert len(page["results"]) == 5
        assert "lexical_exact_reads" not in page and "index_scan_cap_reads" not in page
        # Review 3, item 1: dev (6106362671) on this graph says total_count 5,
        # basis exact, rank_capped false, no ranked_cap: its plain path takes
        # the flag from the rows Nomos left. Neither mode may say more: no
        # floor, no cap, no "N+" in the text.
        assert page["rank_capped"] is False
        assert "ranked_cap" not in page
        assert page["total_count_basis"] != "lower_bound"
        first = render(page, tool="search").splitlines()[0]
        assert "+" not in first and "cap 200" not in first and "bounds" not in first
        assert "rank_capped" not in render(page, tool="search")
    assert enforced["bounded"]["coverage"]["retrieval"] == BOUNDED
    # Exact's proof needs the raw read, which hidden rows filled: retrieval
    # says `unknown` (the one bit the PR body discloses), and the count says
    # no more than that.
    assert enforced["exact"]["coverage"]["retrieval"] == {"status": "unknown"}
    for page in enforced.values():
        assert page["total_count"] is None and page["total_count_basis"] == "unknown"
    exact_text = render(enforced["exact"], tool="search").splitlines()[0]
    assert "retrieval unknown" in exact_text and "count unknown" in exact_text
    # Issue #3666: the exact page is a listing that is not complete, and
    # under enforce it names no cap, in JSON or in text.
    assert enforced["exact"]["completeness"] == "exact"
    assert enforced["exact"]["listing"] == {
        "complete": False, "reason": "scan_bound",
        "literal_text_match": {"method": "grep", "completeness": "exact"},
    }
    assert "listing not shown whole" in exact_text
    assert "rank cap" not in exact_text and " · complete" not in exact_text
    last = dict(enforced["exact"], next_offset=None, truncated=False)
    last_text = render(last, tool="search").splitlines()[0]
    assert "bound reached (not shown whole) · next: none" in last_text
    assert "rank cap" not in last_text
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "0")
    off = pages()
    assert off["bounded"]["coverage"]["retrieval"]["bounds"] == ["candidate_cap"]
    assert off["exact"]["coverage"]["retrieval"] == {
        "status": "bounded", "basis": "bm25_index", "bounds": ["candidate_cap"],
    }
    # Off, the raw floor is public: that is the signal enforce withholds.
    assert off["exact"]["rank_capped"] is True
    assert off["exact"]["total_count_basis"] == "lower_bound"
    assert "(cap 200 rows)" in render(off["exact"], tool="search").splitlines()[0]


# ---------- expanded reads on a filtered records page (review MEDIUM-1/2) ----------

def _expansion_corpus(tmp_path, dsn: str, *, expanded: int,
                      variant_status: str = "open", typed: int = 3) -> tuple[str, Any]:
    """`typed` tickets (3) carry the typed word `hafnium`; `expanded` tickets
    carry only its declared variant `zircaloy`; 210 open tickets match
    neither."""
    from okg.substrate.mcp.concept_expansion import ConceptGroup
    from okg.substrate.mcp.search_profiles import ResolvedScope
    from tests.substrate.mcp.test_search_records import _node

    facts = [
        _node(f"ticket:typed:{i}", "ticket", f"hafnium weld report {i}.", status="open")
        for i in range(typed)
    ] + [
        _node(f"ticket:variant:{i:03d}", "ticket", f"zircaloy weld report {i}.",
              status=variant_status)
        for i in range(expanded)
    ] + [
        _node(f"ticket:bulk:{i:03d}", "ticket", f"Bulk ticket {i}.", status="open")
        for i in range(210)
    ]
    deployment = _publish(tmp_path, dsn, groups={}, main=facts)
    scope = ResolvedScope(
        concept_groups=(ConceptGroup(canonical="hafnium", variants=("zircaloy",)),),
    )
    return deployment, scope


def test_exact_is_not_exhaustive_when_the_expanded_effective_read_fills(
    clean_runtime, tmp_path, monkeypatch,
):
    """Review MEDIUM-1. Exact, records on a filtered subtype, concept
    expansion and attr_filters. Every BM25 read is short and read exactly;
    the expanded effective-attribute read fills its 200-row limit. Its
    saturation stayed in a local flag the filtered fold replaced, so exact
    said exhaustive. Fails on 0698b174a3.

    The expanded effective read is served by the real read with a term the
    210 bulk tickets hold, so it returns 200 real open tickets: on a stored
    graph that read is a subset of the literal one, and filling only it
    needs overlaid attributes the fixture cannot publish."""
    deployment, scope = _expansion_corpus(tmp_path, clean_runtime, expanded=5)
    original = search_module._effective_view_lexical_candidates
    calls: list[tuple[str, int]] = []

    def effective(conn, session, *, query, **kwargs):
        rows = original(conn, session, query="Bulk" if "zircaloy" in query else query,
                        **kwargs)
        calls.append((query, len(rows)))
        return rows

    monkeypatch.setattr(search_module, "_effective_view_lexical_candidates", effective)
    page = _search(clean_runtime, deployment, "hafnium", rank_by="records", limit=10,
                   scope=scope, filter={"subtype": "ticket", "attr_filters": {"status": "open"}})
    assert page["concept_expansion"]["applied"] is True
    assert [count for query, count in calls if "zircaloy" in query] == [200]
    assert [count for query, count in calls if "zircaloy" not in query] == [0]
    # Every BM25 read was short and read exactly: only the effective read filled.
    assert set(page["index_scan_cap_reads"]) >= {"self", "self_expanded"}
    retrieval = page["coverage"]["retrieval"]
    assert retrieval["status"] != "exhaustive"
    assert retrieval == {"status": "bounded", "basis": "exact_scalar", "bounds": ["candidate_cap"]}
    assert page["total_count_basis"] == "lower_bound" and page["rank_capped"] is True
    # The exact prefix ends at the typed-word records: the effective read's
    # missing rows are expansion-only and rank after them.
    assert page["ordered_exactly_through"] == 3


def test_exact_paging_stops_where_a_filled_expanded_read_makes_order_unknown(
    clean_runtime, tmp_path,
):
    """Review MEDIUM-2. Exact, records on a filtered subtype, concept
    expansion: the literal self read is short (3 rows, read exactly), the
    expanded self read fills 200 with 230 variant matches. The count was a
    floor but `ordered_exactly_through` was the whole list and the last
    page said truncated false with no reason. Fails on 0698b174a3."""
    deployment, scope = _expansion_corpus(tmp_path, clean_runtime, expanded=230)
    filt = {"subtype": "ticket"}
    page = _search(clean_runtime, deployment, "hafnium", rank_by="records", limit=500,
                   scope=scope, filter=filt)
    assert page["concept_expansion"]["applied"] is True
    assert page["total_count_basis"] == "lower_bound"
    assert page["total_count"] > 3
    # Only the typed-word records are exactly ordered; the page stops there.
    assert page["ordered_exactly_through"] == 3
    assert sorted(_ids(page)) == [f"ticket:typed:{i}" for i in range(3)]
    assert page["truncated"] is True
    assert page["next_offset"] is None
    assert page["next_offset_reason"] == NEXT_OFFSET_REASON_BEYOND_EXACT_ORDER
    with pytest.raises(RecordsOffsetBeyondExactOrder):
        _search(clean_runtime, deployment, "hafnium", rank_by="records", limit=10,
                offset=3, scope=scope, filter=filt)
    # The bounded default keeps its paging over the retrieved candidates.
    bounded = _search(clean_runtime, deployment, "hafnium", rank_by="records", limit=500,
                      scope=scope, filter=filt, completeness="bounded")
    assert len(bounded["results"]) > 3


def test_a_filled_expanded_read_names_expand_concepts_false_and_it_reaches_the_rows(
    clean_runtime, tmp_path,
):
    """Review 3, item 2. 3 typed-word records and 198 variant-only records:
    each word alone matches fewer than 200, but the expanded query matches
    201 and fills its read, so exact ends the order at the 3 typed records.
    Narrowing cannot reach the variants; the cut and the refusal name
    `expand_concepts=false`, and following it reaches every record."""
    deployment, scope = _expansion_corpus(tmp_path, clean_runtime, expanded=198)
    filt = {"subtype": "ticket"}
    page = _search(clean_runtime, deployment, "hafnium", rank_by="records", limit=500,
                   scope=scope, filter=filt)
    assert page["concept_expansion"]["applied"] is True
    assert page["ordered_exactly_through"] == 3
    assert page["next_offset_reason"] == NEXT_OFFSET_REASON_BEYOND_EXACT_ORDER
    # The JSON is the whole remedy: the arguments and the words to rerun,
    # one per call (review 4, item 3), not only the text.
    remedy = {"expand_concepts": False, "rerun_each": ["hafnium", "zircaloy"]}
    assert page["next_offset_remedy"] == remedy
    text = render(page, tool="search")
    assert (
        "! order exact through record 3; rerun with expand_concepts=false"
        ' once per word of ["hafnium","zircaloy"] (separate rankings)'
    ) in text
    with pytest.raises(RecordsOffsetBeyondExactOrder) as refused:
        _search(clean_runtime, deployment, "hafnium", rank_by="records", limit=10,
                offset=3, scope=scope, filter=filt)
    assert refused.value.data["remedy"] == remedy
    assert "expand_concepts=false" in refused.value.fix_hint
    assert "rerun_each" in refused.value.fix_hint
    # The remedy: the same search without expansion, once per listed word.
    # Each word matches fewer than the 200-row limit, so each rerun is
    # exhaustive and together they hold every record.
    reached: set[str] = set()
    for word in page["next_offset_remedy"]["rerun_each"]:
        rerun = _search(clean_runtime, deployment, word, rank_by="records", limit=500,
                        scope=scope, filter=filt, expand_concepts=False)
        assert rerun["concept_expansion"]["applied"] is False
        assert rerun["coverage"]["retrieval"] == {
            "status": "exhaustive", "basis": "exact_scalar",
        }
        assert rerun["truncated"] is False and "next_offset_remedy" not in rerun
        reached |= set(_ids(rerun))
    assert reached == {f"ticket:typed:{i}" for i in range(3)} | {
        f"ticket:variant:{i:03d}" for i in range(198)
    }


def test_exact_truncates_when_every_expanded_record_was_filtered_out(
    clean_runtime, tmp_path,
):
    """Review 3, item 3. The expanded self read fills 200 with 230 closed
    variant tickets; `attr_filters` keeps only the 3 open typed records, so
    no retrieved record lies past the exact prefix. The count is a floor, so
    the page is truncated too (it said false on 0281cc8d28)."""
    deployment, scope = _expansion_corpus(
        tmp_path, clean_runtime, expanded=230, variant_status="closed",
    )
    filt = {"subtype": "ticket", "attr_filters": {"status": "open"}}
    page = _search(clean_runtime, deployment, "hafnium", rank_by="records", limit=10,
                   scope=scope, filter=filt)
    assert sorted(_ids(page)) == [f"ticket:typed:{i}" for i in range(3)]
    assert page["total_count"] == 3 and page["total_count_basis"] == "lower_bound"
    assert page["ordered_exactly_through"] == 3
    assert page["truncated"] is True
    assert page["next_offset"] is None
    assert page["next_offset_reason"] == NEXT_OFFSET_REASON_BEYOND_EXACT_ORDER
    assert page["next_offset_remedy"] == {
        "expand_concepts": False, "rerun_each": ["hafnium", "zircaloy"],
    }
    # The bounded default keeps its paging: nothing retrieved lies past.
    bounded = _search(clean_runtime, deployment, "hafnium", rank_by="records", limit=10,
                      scope=scope, filter=filt, completeness="bounded")
    assert bounded["truncated"] is False


def test_a_filled_typed_word_read_names_narrowing_not_expand_concepts(
    clean_runtime, tmp_path, monkeypatch,
):
    """Review 4, LOW. 230 records hold the typed word itself, so the typed
    read fills its 200-row limit. Rerunning without expansion fills it
    again, so `expand_concepts=false` would not reach the later records:
    the page, the refusal and the text name narrowing only, with counts
    public and under enforced Nomos. The enforced leg fails on 02ff8ec58a,
    which named the remedy whenever an expanded page had a floor."""
    deployment, scope = _expansion_corpus(tmp_path, clean_runtime, expanded=5, typed=230)
    filt = {"subtype": "ticket"}
    page = _search(clean_runtime, deployment, "hafnium", rank_by="records", limit=500,
                   scope=scope, filter=filt)
    assert page["concept_expansion"]["applied"] is True
    assert page["total_count_basis"] == "lower_bound"
    assert page["next_offset_reason"] == NEXT_OFFSET_REASON_BEYOND_EXACT_ORDER
    assert "next_offset_remedy" not in page
    text = render(page, tool="search")
    assert "narrow the query to see later records" in text
    assert "expand_concepts" not in text
    through = page["ordered_exactly_through"]
    with pytest.raises(RecordsOffsetBeyondExactOrder) as refused:
        _search(clean_runtime, deployment, "hafnium", rank_by="records", limit=10,
                offset=max(through, 1), scope=scope, filter=filt)
    assert "remedy" not in refused.value.data
    assert "expand_concepts" not in refused.value.fix_hint
    # Under enforced Nomos (every record readable) the same rule holds.
    from tests.substrate.mcp.test_search_records import MAIN, READER, _activate, _allow

    _activate(clean_runtime, deployment, [_allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    for mode in ("exact", "bounded"):
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = open_session(conn, deployment_name=deployment, nomos_subject=READER)
            enforced = search(conn, session, "hafnium", method="lexical", rank_by="records",
                              limit=500, scope=scope, filter=filt, completeness=mode)
        assert enforced["concept_expansion"]["applied"] is True
        assert enforced["next_offset_reason"] == NEXT_OFFSET_REASON_BEYOND_EXACT_ORDER
        assert "next_offset_remedy" not in enforced, mode
        assert "expand_concepts" not in render(enforced, tool="search")
