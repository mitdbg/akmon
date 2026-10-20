"""A real match keeps its rank while the first ANALYZE after a publish runs (issue #3666).

pg_textsearch 1.3.0 scores a row with `<@>` one row at a time. While the
first VACUUM or ANALYZE after a publish moves the index's in-memory rows to
disk, an occasional score in a statement reads the index statistics wrong:
0 for a row that contains the query word, or another wrong value. The
match test keeps the row (issue #3232), but its rank and page order change.
pg_textsearch 1.5.1 does not show it.

Each case publishes a fresh graph (autovacuum held off its node table) and
searches from several threads while another session runs that first
ANALYZE. Every reply must equal the reply the graph serves once the ANALYZE
is over: the same records, order and scores. Search scores each row twice on
a library older than 1.5.1 and keeps the better score
(`steady_bm25_score_sql`); without that, cases fail on 1.3.0.
"""
from __future__ import annotations

import importlib
import threading

import psycopg
import pytest

from okg.substrate.bm25_node_search import (
    STEADY_SCORE_LIBRARY,
    resolve_node_text_bm25_index,
    steady_bm25_score_sql,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import open_session, search
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes

pytestmark = pytest.mark.postgres

search_module = importlib.import_module("okg.substrate.mcp.search")

TERM = "zirconium"
#: The exact read's own time limit for these cases. The product limit (5 s)
#: is a latency guard; this test checks rank and score, not speed. On a
#: loaded CI runner, sharing its Postgres with the backup and restore tests,
#: six readers beside the first ANALYZE passed 5 s and were refused
#: `exact_read_timeout` (issue #3678), while a lab host read in 0.21 s at p99.
EXACT_READ_TIMEOUT_MS = 60_000
#: Cases, each with its own fresh database: the race needs a first spill.
ROUNDS = 8
READERS = 6
#: Non-matching records published beside the matches.
OTHERS = 3000

DOC = SubtypeSpec("doc", metadata={
    "searchable_text_fields": ["text"], "title_from": "summary",
    "passage_rollup": {"edge_type": "contains", "child_subtype": "doc_chunk"},
})
DOC_CHUNK = SubtypeSpec("doc_chunk", metadata={"searchable_text_fields": ["text"]})


def _node(node_id: str, subtype: str, text: str, **attrs) -> NodeFact:
    return NodeFact(
        node_id=node_id, subtype=subtype, attrs={"text": text, **attrs},
        source_record_id={"id": node_id}, source_revision={"v": 1},
    )


def _edge(src: str, dst: str) -> EdgeFact:
    return EdgeFact(
        src=src, dst=dst, edge_type="contains",
        source_record_id={"id": f"{src}|contains|{dst}"}, source_revision={"v": 1},
    )


def _publish(tmp_path, dsn: str) -> str:
    """Twelve equal matches, one weak match, passages and non-matching rows."""
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    nodes = [
        _node(f"doc:{i:03d}", "doc", f"{TERM} {TERM} body {i:03d}.", summary=f"Doc {i}")
        for i in range(12)
    ]
    nodes.append(_node(
        "doc:weak", "doc",
        f"{TERM} " + " ".join(f"filler{j}" for j in range(40)) + ".", summary="Weak",
    ))
    edges = []
    for i in range(4):
        chunk = f"doc_chunk:{i}"
        nodes.append(_node(chunk, "doc_chunk", f"{TERM} passage {i}."))
        edges.append(_edge(f"doc:{i:03d}", chunk))
    # Non-matching rows make the first spill long enough to overlap reads;
    # short, so the publish itself stays under the index's spill threshold.
    nodes += [
        _node(f"doc:other{i:04d}", "doc", f"unrelated record {i} about lions.", summary="x")
        for i in range(OTHERS)
    ]
    publish_nodes(dsn, deployment, subtypes=[DOC, DOC_CHUNK], edges=edges, nodes=nodes)
    return deployment


#: Three reads: the unfiltered exact read (13 matches under a limit of 50, so
#: it takes the complete read, which scores every matching row), the exact
#: read filtered to one type (the filtered fold), and a record scope (its
#: rows scored one by one).
READS = (
    {"completeness": "exact", "limit": 50},
    {"completeness": "exact", "limit": 50, "filter": {"subtype": "doc"}},
    {"record_scope": {"records": [f"doc:{i:03d}" for i in range(12)] + ["doc:weak"]}},
)


def _rows(page: dict) -> list[tuple[str, float]]:
    return [(row["node_id"], float(row["score"])) for row in page["results"]]


def _read(dsn: str, deployment: str, arguments: dict) -> list[tuple[str, float]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        return _rows(search(conn, session, TERM, method="lexical", **arguments))


@pytest.mark.parametrize("case", range(ROUNDS))
def test_a_match_keeps_its_rank_while_the_first_analyze_runs(
    clean_runtime, tmp_path, monkeypatch, case,
):
    monkeypatch.setattr(search_module, "_EXACT_LEXICAL_TIMEOUT_MS", EXACT_READ_TIMEOUT_MS)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        segment, _index_name = resolve_node_text_bm25_index(conn)
        # Autovacuum would run the first ANALYZE on its own, often before the
        # first read; the test runs it while the readers read.
        conn.execute(f"ALTER TABLE {segment} SET (autovacuum_enabled = false)")
    deployment = _publish(tmp_path, clean_runtime)
    replies: list[tuple[int, list[tuple[str, float]]]] = []
    errors: list[BaseException] = []
    stop = threading.Event()
    lock = threading.Lock()

    def reader() -> None:
        # One connection per reader, open before the spill, as a server pool's.
        try:
            with psycopg.connect(clean_runtime, autocommit=True) as conn:
                session = open_session(conn, deployment_name=deployment)
                while not stop.is_set():
                    for which, arguments in enumerate(READS):
                        page = search(conn, session, TERM, method="lexical", **arguments)
                        with lock:
                            replies.append((which, _rows(page)))
        except BaseException as exc:  # surfaced below, never swallowed
            errors.append(exc)

    threads = [threading.Thread(target=reader) for _ in range(READERS)]
    for thread in threads:
        thread.start()
    try:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            # Let every reader finish one reply first, then race the spill.
            while not errors:
                with lock:
                    if len(replies) >= READERS:
                        break
                stop.wait(0.01)
            conn.execute(f"ANALYZE {segment}")
            stop.wait(0.2)
    finally:
        stop.set()
        for thread in threads:
            thread.join()
    assert not errors, errors
    # The steady state: every read again, once the spill is over.
    expected = [_read(clean_runtime, deployment, dict(arguments)) for arguments in READS]
    assert all(len(rows) == 13 for rows in expected), expected
    wrong = [(READS[which], rows) for which, rows in replies if rows != expected[which]]
    assert not wrong, (
        f"{len(wrong)} of {len(replies)} replies served another order or score than the "
        f"steady state; first: {wrong[0][0]} {wrong[0][1]} expected {expected[READS.index(wrong[0][0])]}"
    )


def test_a_score_is_read_twice_only_on_a_library_older_than_1_5_1(clean_runtime):
    """The second reading runs on an old library and nowhere else."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        (version,) = conn.execute(
            "SELECT current_setting('pg_textsearch.library_version', true)"
        ).fetchone()
        assert version, "the server reports no pg_textsearch library version"
        old = tuple(int(part) for part in version.split(".")[:3]) < STEADY_SCORE_LIBRARY
        conn.execute("CREATE TEMP SEQUENCE zero_score_readings")
        # Each evaluation of the score expression takes one value.
        counter = "-nextval('zero_score_readings')::float8"
        (score,) = conn.execute(f"SELECT {steady_bm25_score_sql(counter)}").fetchone()
        (readings,) = conn.execute("SELECT currval('zero_score_readings')").fetchone()
    assert readings == (2 if old else 1), (version, readings)
    # The better (lower) reading is kept: the second, -2, when there are two.
    assert score == -readings, (version, score)
