"""BM25 index readers drop repeated rows before they count them (issue #3335).

pg_textsearch 1.3.0 can return one table row more than once from a single
`ORDER BY <@> LIMIT n` index scan, and leave other matching rows out. It
happens when a condition above the scan rejects rows, so the scan reads past
its LIMIT and reruns its scoring with a doubled limit, and rows with equal
scores sit in more than one index segment: the rerun orders the tied rows
differently and the scan resumes by position. Upstream fixed it in 1.3.1
(pg_textsearch PR #417).

* The scratch table below reproduces it with the lexical self read's own
  statement shape on 1.3.0: 200 rows, fewer distinct.
* `_ranked_read` (the lexical self read) keeps each node once, counts the
  distinct rows, and still records that the read filled its LIMIT; under
  `completeness="exact"` the short distinct list is read again completely.
* grep's BM25 windows never number, charge or decide a node twice, so its
  candidate-row bound counts distinct rows.

The passage rollup's chunk scan and the filtered passage windows are later
work (phase 2 of issue #3335).
"""
from __future__ import annotations

import importlib
from types import SimpleNamespace
from typing import Any

import psycopg
import pytest
from psycopg.rows import dict_row

from okg.substrate.mcp import grep_search

search_module = importlib.import_module("okg.substrate.mcp.search")
CAP = search_module._LEXICAL_RANK_CAP
TABLE = "repeated_rows_nodes"
INDEX = "repeated_rows_bm25"
WHERE = [
    "deleted = FALSE", "live_lo <= %(bp)s", "%(bp)s < live_hi", "attrs ? 'text'",
    "subtype = %(subtype)s",
]
PARAMS = {"q": "alpha", "bp": 1, "subtype": "chunk"}
ORDER = f"(attrs->>'text') <@> to_bm25query(%(q)s, '{INDEX}')"
#: The lexical self read's statement shape (`ranked_sql` in `_search_lexical`).
SELF_SQL = (
    f"SELECT * FROM (SELECT node_id, subtype, attrs, text_hash, source, "
    f"source_record_id, source_revision, {ORDER} AS score FROM {TABLE} "
    f"WHERE {' AND '.join(WHERE)} ORDER BY {ORDER}, node_id LIMIT {CAP}"
    f") AS ranked WHERE ranked.score < 0"
)
INDEX_PLAN = "-c enable_seqscan=off -c enable_bitmapscan=off"
SESSION = SimpleNamespace(deployment_name="repeated-rows", generation_id="g1")


# ---------- the helper ----------

def test_drop_repeated_rows_keeps_each_nodes_first_row_in_order():
    from okg.substrate.bm25_node_search import drop_repeated_rows

    rows = [{"node_id": "b", "s": 1}, {"node_id": "a", "s": 2},
            {"node_id": "b", "s": 3}, {"node_id": "c", "s": 4}]
    assert drop_repeated_rows(rows) == [rows[0], rows[1], rows[3]]
    seen = {("a",)}
    assert drop_repeated_rows(rows, seen=seen) == [rows[0], rows[3]]
    assert seen == {("a",), ("b",), ("c",)}
    assert drop_repeated_rows([]) == []
    # Two versions of one node are two rows when the key names the version.
    versions = [{"node_id": "n", "lo": 1}, {"node_id": "n", "lo": 2},
                {"node_id": "n", "lo": 1}]
    assert drop_repeated_rows(versions, key=("node_id", "lo")) == versions[:2]
    assert drop_repeated_rows(versions) == versions[:1]


# ---------- a real reproduction on pg_textsearch 1.3.0 ----------

def _table(conn: psycopg.Connection) -> None:
    """1,200 rows with one text (one score), spread over three written
    segments and the in-memory buffer; half are `chunk`, so the subtype
    condition runs above the index scan and rejects rows (600 chunks)."""
    conn.execute(f"DROP TABLE IF EXISTS {TABLE}")
    conn.execute(
        f"CREATE TABLE {TABLE} (node_id text PRIMARY KEY, subtype text, attrs jsonb, "
        "text_hash text, source text, source_record_id text, source_revision text, "
        "deleted bool DEFAULT false, live_lo bigint DEFAULT 0, "
        "live_hi bigint DEFAULT 9223372036854775807) WITH (autovacuum_enabled = off)"
    )
    conn.execute(
        f"CREATE INDEX {INDEX} ON {TABLE} USING bm25 ((attrs->>'text')) "
        "WITH (text_config = 'english') WHERE deleted = FALSE AND attrs ? 'text'"
    )
    for part, spill in (("a", True), ("b", True), ("c", True), ("d", False)):
        conn.execute(
            f"INSERT INTO {TABLE} (node_id, subtype, attrs) SELECT "
            f"'{part}' || lpad(g::text, 4, '0'), "
            "CASE WHEN g % 2 = 0 THEN 'chunk' ELSE 'other' END, "
            "jsonb_build_object('text', 'alpha beta gamma') FROM generate_series(1, 300) g"
        )
        if spill:
            conn.execute("SELECT bm25_spill_index(%s)", (INDEX,))
    conn.execute(f"ANALYZE {TABLE}")


def _raw(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """The self read's statement as it runs, after asserting its plan is the
    BM25 index's ordered scan with the subtype condition as its Filter."""
    with conn.cursor(row_factory=dict_row) as cur:
        search_module.execute_with_custom_plan(cur, "EXPLAIN (FORMAT JSON) " + SELF_SQL, PARAMS)
        document = next(iter(cur.fetchone().values()))
        nodes = list(search_module._plan_nodes(document[0]["Plan"]))
        (scan,) = [node for node in nodes if node.get("Index Name") == INDEX]
        assert scan["Node Type"] == "Index Scan" and scan.get("Order By")
        assert "subtype" in scan.get("Filter", "")
        search_module.execute_with_custom_plan(cur, SELF_SQL, PARAMS)
        return cur.fetchall()


def _extension_version(conn: psycopg.Connection) -> str:
    return conn.execute(
        "SELECT extversion FROM pg_extension WHERE extname = 'pg_textsearch'"
    ).fetchone()[0]


def _cap(conn: psycopg.Connection, *, exact: bool) -> Any:
    return search_module._IndexScanCap(conn, segment=TABLE, index_name=INDEX, exact=exact)


def _ranked(conn: psycopg.Connection, cap: Any, sql: str = SELF_SQL) -> list[dict[str, Any]]:
    return search_module._ranked_read(
        conn, SESSION, sql, PARAMS, cap=cap, name="self", where=WHERE,
    )


@pytest.mark.postgres
def test_the_self_read_keeps_each_node_once_and_says_its_limit_filled(clean_runtime):
    # A scratch table this small would be read sequentially; these options
    # make PostgreSQL plan the read as on a large segment.
    with psycopg.connect(clean_runtime, autocommit=True, options=INDEX_PLAN) as conn:
        _table(conn)
        raw = _raw(conn)
        distinct = {str(row["node_id"]) for row in raw}
        assert len(raw) == CAP
        if _extension_version(conn) == "1.3.0":
            # The extension's own repeat, on a table no one writes to.
            assert len(distinct) < CAP
        cap = _cap(conn, exact=False)
        rows = _ranked(conn, cap)
        ids = [str(row["node_id"]) for row in rows]
        assert len(ids) == len(set(ids)) == len(distinct)
        assert set(ids) == distinct
        # The LIMIT was filled (by repeats or not): more matches lie past it,
        # and the page says so even though fewer than CAP distinct rows came.
        assert "self" in cap.filled and "self" in cap.saturated


@pytest.mark.postgres
def test_an_exact_self_read_reads_a_short_distinct_list_again_completely(clean_runtime):
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _table(conn)
        with conn.transaction():
            conn.execute("SET LOCAL enable_indexscan = off")
            with conn.cursor(row_factory=dict_row) as cur:
                search_module.execute_with_custom_plan(cur, SELF_SQL, PARAMS)
                exact = [str(row["node_id"]) for row in cur.fetchall()]
        assert len(exact) == len(set(exact)) == CAP
        cap = _cap(conn, exact=True)
        # The statement fills its LIMIT with one row twice: CAP rows, CAP - 1
        # distinct, so the distinct list is short and is read again.
        seeded = (
            f"SELECT * FROM ({SELF_SQL} LIMIT {CAP - 1}) most "
            f"UNION ALL SELECT * FROM ({SELF_SQL} LIMIT 1) one"
        )
        rows = _ranked(conn, cap, seeded)
        ids = [str(row["node_id"]) for row in rows]
        assert ids == exact
        assert cap.reads["self"].matches == 600 and "self" in cap.saturated


@pytest.mark.postgres
def test_a_seeded_repeat_never_reaches_the_bounded_self_read(clean_runtime):
    """The same check on any extension version: the statement returns its
    first row twice."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _table(conn)
        seeded = (
            f"SELECT * FROM (SELECT * FROM ({SELF_SQL}) s ORDER BY score, node_id LIMIT 5) five "
            f"UNION ALL SELECT * FROM (SELECT * FROM ({SELF_SQL}) s "
            f"ORDER BY score, node_id LIMIT 1) one"
        )
        cap = _cap(conn, exact=False)
        rows = _ranked(conn, cap, seeded)
        ids = [str(row["node_id"]) for row in rows]
        assert len(ids) == 5 and len(set(ids)) == 5
        assert "self" not in cap.filled and "self" not in cap.saturated


@pytest.mark.postgres
def test_an_exact_read_that_refuses_after_dropping_repeats_says_so(
    clean_runtime, monkeypatch,
):
    """Under exact, a LIMIT filled partly by repeats is short once they are
    dropped, so the scoped exact read runs; past its work limit it refuses,
    and the refusal says the index read held repeated rows. The same read
    without a repeat fills its LIMIT and stands with no exact statement."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _table(conn)
        monkeypatch.setattr(search_module, "_EXACT_LEXICAL_WORK_ROWS", 100)
        plain = f"SELECT * FROM ({SELF_SQL} LIMIT {CAP}) every"
        with conn.transaction():
            conn.execute("SET LOCAL enable_indexscan = off")
            rows = _ranked(conn, _cap(conn, exact=True), plain)
        assert len(rows) == CAP
        seeded = (
            f"SELECT * FROM ({SELF_SQL} LIMIT {CAP - 1}) most "
            f"UNION ALL SELECT * FROM ({SELF_SQL} LIMIT 1) one"
        )
        with pytest.raises(search_module.LexicalIndexScanCapRefused) as caught:
            _ranked(conn, _cap(conn, exact=True), seeded)
        assert caught.value.data["reason"] == "scoped_work_limit"
        assert caught.value.data["index_repeated_rows"] is True
        assert "repeated rows" in str(caught.value)


@pytest.mark.postgres
def test_a_self_read_filled_by_repeats_marks_the_filtered_fold_saturated(
    clean_runtime, tmp_path, monkeypatch,
):
    """The filtered record fold takes the self read's saturation from the
    rows the read returned (`self_saturated`). A self read whose LIMIT one
    row filled twice holds one row fewer, and is still saturated."""
    from tests.substrate.mcp.test_search_records import _basic, _search

    deployment = _basic(tmp_path, clean_runtime)
    # The query matches seven tickets' own text; a cap of eight leaves the
    # self read one row short of its LIMIT.
    monkeypatch.setattr(search_module, "_LEXICAL_RANK_CAP", 8)
    seen: list[bool] = []
    real_fold = search_module._fold_filtered

    def fold(*args, **kwargs):
        seen.append(kwargs["self_saturated"])
        return real_fold(*args, **kwargs)

    monkeypatch.setattr(search_module, "_fold_filtered", fold)
    page = _search(clean_runtime, deployment, limit=50, filter={"subtype": "ticket"})
    assert page["results"] and seen == [False]
    clean_ids = [row["node_id"] for row in page["results"]]

    real_execute = search_module.execute_with_custom_plan
    seeded: list[str] = []

    def execute(cur, sql, params=None):
        text = str(sql)
        if text.startswith("SELECT * FROM (SELECT node_id, subtype, attrs") and "AS ranked" in text:
            seeded.append(text)
            # Seven distinct rows and the first again: eight rows, the LIMIT.
            sql = (
                f"SELECT * FROM ({text} LIMIT 7) seven "
                f"UNION ALL SELECT * FROM ({text} LIMIT 1) again"
            )
        return real_execute(cur, sql, params)

    monkeypatch.setattr(search_module, "execute_with_custom_plan", execute)
    seen.clear()
    page = _search(clean_runtime, deployment, limit=50, filter={"subtype": "ticket"})
    assert seeded and seen == [True]
    # The same records, each once; only the saturation the fold sees differs.
    assert sorted(row["node_id"] for row in page["results"]) == sorted(clean_ids)


# ---------- grep's BM25 windows ----------

#: The real statement runner, so a fake installed twice never wraps a fake.
_REAL_BOUNDED_WITH = grep_search._bounded_with

#: (position copied, position the copy is inserted at) in the index's order.
REPEATS = ((1, 3), (6, 9))


def _repeating_index(
    monkeypatch: pytest.MonkeyPatch, repeats: tuple = REPEATS,
) -> list[int]:
    """Make the BM25 index repeat rows the way 1.3.0 does, in one order every
    statement shares: the row at position 1 comes back at position 3 and the
    row at 6 at 9, and every window is a LIMIT/OFFSET slice of that order.
    So a window that settles rows by their distinct count starts the next
    one on a repeat of a row it already settled. Returns the windows that
    held a repeat."""
    hit: list[int] = []
    real = _REAL_BOUNDED_WITH

    def bounded_with(conn, sql, params, *, settings, explain=False):
        window = params.get("grep_window") if not explain else None
        if not window:
            return real(conn, sql, params, settings=settings, explain=explain)
        offset = params["grep_offset"]
        whole = real(
            conn, sql, {**params, "grep_window": offset + window, "grep_offset": 0},
            settings=settings,
        )
        order = list(whole)
        for source, at in repeats:
            if source < len(whole) and at <= len(order):
                order.insert(at, dict(whole[source]))
        rows = order[offset: offset + window]
        if any(offset <= at < offset + window for _source, at in repeats):
            hit.append(offset)
        return rows

    monkeypatch.setattr(grep_search, "_bounded_with", bounded_with)
    return hit


def _egrets(tmp_path, dsn: str, n: int = 40) -> str:
    from tests.substrate.mcp.test_search_grep import _node, _publish

    facts = [_node(f"note:{i:03d}", "note", f"egret {'pad ' * i}") for i in range(n)]
    return _publish(tmp_path, dsn, facts)


@pytest.mark.postgres
def test_grep_charges_and_lists_each_node_once(clean_runtime, tmp_path, monkeypatch):
    from tests.substrate.mcp.test_search_grep import _grep, _ids
    from tests.substrate.mcp.test_search_grep_rank import _count_page_checks

    deployment = _egrets(tmp_path, clean_runtime)
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", 0)
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 4)
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 7)
    sizes = _count_page_checks(monkeypatch)
    hit = _repeating_index(monkeypatch)
    page = _grep(clean_runtime, deployment, "egret", limit=10)
    # The first window settles note:000 and note:001 (its edge is note:002);
    # the second starts at OFFSET 2 and reads note:002, then note:001 again.
    assert hit, "no window carried a repeated row"
    # Seven candidate rows charged and decided, seven distinct records: no
    # repeat, in its own window or the next one, took a slot of the bound.
    assert sum(sizes) == 7
    assert _ids(page) == [f"note:{i:03d}" for i in range(7)]
    assert page["page_scan_bound_hit"] == "candidate_rows"
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 2_000)
    whole = _grep(clean_runtime, deployment, "egret", limit=40)
    assert _ids(whole) == [f"note:{i:03d}" for i in range(40)]


@pytest.mark.postgres
def test_later_pages_over_repeats_never_skip_or_repeat_a_record(
    clean_runtime, tmp_path, monkeypatch,
):
    """A later page replays the windows before its resume point. Over an
    index that repeats rows before and after that point, the pages walk the
    same order with no record twice and none skipped. Under a tight
    index-row bound a page may stop earlier than without repeats (the
    repeats and the settled rows read again are rows read), but what it
    lists is a prefix of the page without them."""
    from tests.substrate.mcp.test_search_grep import _grep, _ids

    deployment = _egrets(tmp_path, clean_runtime)
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", 0)
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 4)

    def pages(bound: int) -> list[list[str]]:
        monkeypatch.setattr(grep_search, "PAGE_INDEX_ROWS", bound)
        return [
            _ids(_grep(clean_runtime, deployment, "egret", limit=5, offset=offset))
            for offset in (0, 5, 10)
        ]

    bounds = (8, 12, 16, 50_000)
    clean = {bound: pages(bound) for bound in bounds}
    hit = _repeating_index(monkeypatch, repeats=((0, 2), (1, 3), (6, 9)))
    repeating = {bound: pages(bound) for bound in bounds}
    assert hit
    assert repeating[50_000] == clean[50_000] == [
        [f"note:{i:03d}" for i in range(start, start + 5)] for start in (0, 5, 10)
    ]
    for bound in bounds:
        for got, want in zip(repeating[bound], clean[bound]):
            assert got == want[:len(got)], bound


# ---------- continuation chains: every call ends (review round 2) ----------

#: Window statements one call may run before the test calls it a loop.
STATEMENT_GUARD = 200
#: The reviewer's heavy pattern: rows 0 and 1 repeated nine times early.
HEAVY = ((0, 1), (0, 4), (1, 5), (0, 6), (1, 7), (0, 8), (1, 9), (0, 10), (1, 11))


class _Loop(Exception):
    """One call ran more window statements than `STATEMENT_GUARD`."""


def _logging_index(monkeypatch: pytest.MonkeyPatch, repeats: tuple, *,
                   threshold: int = 0) -> list[list[tuple[int, int, int]]]:
    """A repeating index (as `_repeating_index`) whose repeats appear only in
    statements whose LIMIT (offset + window) passes `threshold`, as 1.3.0's
    rerun fires only past its first scoring batch. Logs each call's window
    statements as (offset, window, rows returned) in `calls[-1]`, and raises
    `_Loop` past
    `STATEMENT_GUARD` statements in one call, so a loop fails the test
    instead of hanging it. Append `[]` to the log before each call."""
    calls: list[list[tuple[int, int]]] = [[]]
    real = _REAL_BOUNDED_WITH

    def bounded_with(conn, sql, params, *, settings, explain=False):
        window = params.get("grep_window") if not explain else None
        if not window:
            return real(conn, sql, params, settings=settings, explain=explain)
        offset = params["grep_offset"]
        if len(calls[-1]) >= STATEMENT_GUARD:
            raise _Loop(calls[-1][-6:])
        whole = real(
            conn, sql, {**params, "grep_window": offset + window, "grep_offset": 0},
            settings=settings,
        )
        order = list(whole)
        if offset + window > threshold:
            for source, at in repeats:
                if source < len(whole) and at <= len(order):
                    order.insert(at, dict(whole[source]))
        rows = order[offset: offset + window]
        calls[-1].append((offset, window, len(rows)))
        return rows

    monkeypatch.setattr(grep_search, "_bounded_with", bounded_with)
    return calls


def _chain(dsn: str, deployment: str, calls: list, *, limit: int) -> tuple[list[str], list]:
    """Follow one query's pages (continuation tokens, else offsets) to the
    end; returns the ids listed and each page's stop."""
    from tests.substrate.mcp.test_search_grep import _grep, _ids

    ids: list[str] = []
    stops: list = []
    follow: dict = {}
    for _page in range(60):
        calls.append([])
        page = _grep(dsn, deployment, "egret", limit=limit, **follow)
        ids.extend(_ids(page))
        stops.append((page.get("page_scan_bound_hit"), page.get("continuation_withheld")))
        if page.get("continuation_token"):
            follow = {"continuation_token": page["continuation_token"]}
        elif page.get("next_offset") is not None:
            follow = {"offset": page["next_offset"]}
        else:
            return ids, stops
    raise AssertionError("the chain did not end within 60 pages")


def _within_progress_bound(statements: list[tuple[int, int, int]]) -> bool:
    """The progress bound on one call's window queries: each pass numbers a
    row, counts one against `PAGE_INDEX_ROWS`, grows its window, or ends,
    and both a numbered and a counted row is a row some statement returned,
    so the statements run are at most (rows returned + 1) x (log2 of the
    per-scan limit + 1)."""
    import math

    steps = math.ceil(math.log2(grep_search.PG_TEXTSEARCH_SCAN_LIMIT)) + 1
    returned = sum(rows for _offset, _window, rows in statements)
    return len(statements) <= (returned + 1) * steps


@pytest.mark.postgres
@pytest.mark.parametrize("bound", [6, 8])
@pytest.mark.parametrize("limit", [3, 40])
def test_a_continuation_over_limit_dependent_repeats_always_ends(
    clean_runtime, tmp_path, monkeypatch, bound, limit,
):
    """The reviewer's fixture (round 2): a replay window that settled
    nothing was free, so the same statement ran forever. Every call now
    ends, under the statement guard, and every chain lists records once, in
    order (a prefix of the true order wherever it stops)."""
    deployment = _egrets(tmp_path, clean_runtime)
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", 0)
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 4)
    monkeypatch.setattr(grep_search, "PAGE_PROBE_AFTER", 10**9)
    monkeypatch.setattr(grep_search, "PAGE_INDEX_ROWS", bound)
    calls = _logging_index(monkeypatch, HEAVY, threshold=8)
    ids, _stops = _chain(clean_runtime, deployment, calls, limit=limit)
    assert ids == [f"note:{i:03d}" for i in range(len(ids))]
    assert all(_within_progress_bound(statements) for statements in calls[1:])


@pytest.mark.postgres
def test_window_queries_per_call_stay_within_the_progress_bound(
    clean_runtime, tmp_path, monkeypatch,
):
    """Every pass numbers a row, counts one against the index-row bound,
    grows its window, or ends (`_within_progress_bound`); checked over the
    heavy pattern at several bounds, with repeats from the first statement
    or only past a LIMIT of 8 or 12."""
    deployment = _egrets(tmp_path, clean_runtime)
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", 0)
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 4)
    monkeypatch.setattr(grep_search, "PAGE_PROBE_AFTER", 10**9)
    for bound in (6, 8, 12, 16, 50_000):
        monkeypatch.setattr(grep_search, "PAGE_INDEX_ROWS", bound)
        for threshold in (0, 8, 12):
            calls = _logging_index(monkeypatch, HEAVY, threshold=threshold)
            ids, _stops = _chain(clean_runtime, deployment, calls, limit=3)
            assert len(ids) == len(set(ids))
            assert all(_within_progress_bound(statements) for statements in calls[1:])


@pytest.mark.postgres
def test_a_chain_over_repeats_lists_every_record(clean_runtime, tmp_path, monkeypatch):
    """The PR's own repeat pattern, PAGE_INDEX_ROWS 8, three records a page,
    following continuation tokens: every record, once, in order. Freeing a
    replay window's dropped rows is what lets the chain past them (without
    it the chain stopped after 11 records, review round 2)."""
    deployment = _egrets(tmp_path, clean_runtime)
    monkeypatch.setattr(grep_search, "PAGE_SMALL_SET", 0)
    monkeypatch.setattr(grep_search, "PAGE_FIRST_WINDOW", 4)
    monkeypatch.setattr(grep_search, "PAGE_PROBE_AFTER", 10**9)
    monkeypatch.setattr(grep_search, "PAGE_INDEX_ROWS", 8)
    calls = _logging_index(monkeypatch, ((0, 2), (1, 3), (6, 9)))
    ids, _stops = _chain(clean_runtime, deployment, calls, limit=3)
    assert ids == [f"note:{i:03d}" for i in range(40)]


@pytest.mark.postgres
def test_no_repeat_note_on_a_refusal_under_withheld_counts(clean_runtime, monkeypatch):
    """Under enforced access rules (`withhold_counts`) the refusal keeps the
    generic reason and gains no note about repeated rows: the raw read held
    rows the caller may not read."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _table(conn)
        monkeypatch.setattr(search_module, "_EXACT_LEXICAL_WORK_ROWS", 100)
        seeded = (
            f"SELECT * FROM ({SELF_SQL} LIMIT {CAP - 1}) most "
            f"UNION ALL SELECT * FROM ({SELF_SQL} LIMIT 1) one"
        )
        cap = search_module._IndexScanCap(
            conn, segment=TABLE, index_name=INDEX, exact=True, withhold_counts=True,
        )
        with pytest.raises(search_module.LexicalIndexScanCapRefused) as caught:
            _ranked(conn, cap, seeded)
        assert caught.value.data["reason"] == "exact_read_bound"
        assert "index_repeated_rows" not in caught.value.data
        assert "repeated rows" not in str(caught.value)
