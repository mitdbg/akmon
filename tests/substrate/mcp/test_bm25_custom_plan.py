"""BM25-ordered reads are never server-prepared (PACT mcp-passage-custom-plan).

psycopg prepares a statement after 5 runs on one connection and PostgreSQL may
then reuse a generic plan that cannot see the query text; on a copy of the
comp-ops graph, on an owner-login connection, passage scans with a median of
about 2.0 s for runs 1-10 took 28-68 s from the 11th run on
(pact/changes/mcp-passage-custom-plan/evidence/live-summary.json). These tests run the reads more times than that on ONE connection that
logs in as the deployment owner, so no role-level ``plan_cache_mode`` protects
them, and assert that no BM25 statement was ever server-prepared.
"""

from __future__ import annotations

import psycopg
import pytest

from okg.substrate.bm25_node_search import execute_with_custom_plan
from okg.substrate.mcp.search import search
from okg.substrate.mcp.session import open_session
from tests.substrate.mcp.erag_rollup_fixture import (
    CHUNK,
    NARROWINGS,
    SUBTYPES,
    TERM,
    build_facts,
)
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes

#: More runs than psycopg's prepare threshold (5) plus PostgreSQL's five
#: custom plans before it considers a generic one.
_RUNS = 12


class _RecordingCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[object, object, dict]] = []

    def execute(self, sql, params=None, **kwargs):
        self.calls.append((sql, params, kwargs))
        return self


def test_execute_with_custom_plan_disables_server_prepare() -> None:
    cur = _RecordingCursor()
    assert execute_with_custom_plan(cur, "SELECT %(q)s", {"q": "x"}) is cur
    assert cur.calls == [("SELECT %(q)s", {"q": "x"}, {"prepare": False})]


def _publish(dsn: str, tmp_path) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    facts = build_facts()
    publish_nodes(
        dsn,
        deployment,
        subtypes=SUBTYPES,
        narrowings=NARROWINGS,
        nodes=[fact for fact in facts if hasattr(fact, "node_id")],
        edges=[fact for fact in facts if not hasattr(fact, "node_id")],
    )
    return deployment


def _prepared_bm25_statements(conn: psycopg.Connection) -> int:
    row = conn.execute(
        "SELECT count(*) FROM pg_prepared_statements "
        "WHERE statement LIKE '%to_bm25query%'"
    ).fetchone()
    return int(row[0])


@pytest.mark.postgres
def test_bm25_reads_stay_unprepared_on_one_long_lived_connection(
    clean_runtime: str,
    tmp_path,
) -> None:
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # The failure this guards needs psycopg's default threshold; a
        # connection that never prepares would pass vacuously.
        assert conn.prepare_threshold == 5
        session = open_session(conn, deployment_name=deployment)
        for _ in range(_RUNS):
            passages = search(
                conn,
                session,
                TERM,
                method="lexical",
                filter={"subtype": "doc"},
                rank_by="passages",
                limit=10,
                select=["node_id", "score"],
            )
            own = search(
                conn,
                session,
                TERM,
                method="lexical",
                filter={"subtype": CHUNK.name},
                limit=10,
                select=["node_id", "score"],
            )
        assert passages["results"]
        assert own["results"]
        assert _prepared_bm25_statements(conn) == 0
