"""Coverage tests for `okg.substrate.mcp.run_sql.run_sql`.

The safety layer (`sql_safety.py` — validation regex + AST checks)
is separately covered by `tests/substrate/mcp/test_run_sql_safety.py`
(141 stmts, 88.9% covered). This file covers the **dispatcher**:
- happy path against `okg.v_nodes` returning rows
- row_cap truncation detection
- EXPLAIN plan-time error surfacing as `RunSqlInvalid`
- timeout surfacing as `RunSqlTimeout`
- session-pin enforcement (queries see the pinned generation)
- envelope shape: `rows`, `columns`, `returned_count`, `truncated`,
  `pinned_generation_id`, `query_plan_summary`, `elapsed_ms`
  (`total_count` was retired in issue #1440: it was computed after the
  caller's own OFFSET and saturated at `row_cap`, so it described the page,
  not the matching set)
"""
from __future__ import annotations

import psycopg
from psycopg.pq import TransactionStatus
import pytest

from okg.substrate.mcp import graph_sql_splice, open_session
from okg.substrate.mcp.run_sql import (
    RunSqlTimeout,
    _bound_query_payload,
    run_sql,
)
from okg.substrate.mcp.sql_safety import RunSqlInvalid
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import (
    ensure_published_generation,
    seed_catalog_version,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


# `_seed_catalog_version` hoisted to tests.substrate._db_helpers
# as `seed_catalog_version` (per Wave D-13).


def _insert_node_facts(conn, facts: list[tuple[str, str, str]]) -> None:
    """Insert (node_id, subtype, text) facts (mirror of
    `tests/substrate/test_mcp_tools.py::_insert_node_facts`)."""
    import json
    import uuid
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    ChronosGraphStore.from_connection(conn).apply_facts([
        NodeFact(
            node_id,
            subtype,
            {"text": text},
            {"id": node_id},
            {"v": 1},
        )
        for node_id, subtype, text in facts
    ])

    run_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        for node_id, subtype, text in facts:
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES (%s, %s, %s::jsonb, %s, %s::jsonb, %s::jsonb,
                        now(), %s, %s, 'I')
                """,
                (
                    node_id, subtype, json.dumps({"text": text}),
                    "test:src",
                    json.dumps({"id": node_id}),
                    json.dumps({"v": 1}),
                    f"{run_id}:{node_id}:{uuid.uuid4().hex[:8]}",
                    run_id,
                ),
            )


# Fake checkouts expose the two things run_sql's graph path uses: the read
# subqueries and branch point (`read_relations`), and `query`, which the test
# hook below routes the executed text to (issue #2899).
_FAKE_RELATIONS = {
    "okg.graph_nodes": (
        "SELECT node_id FROM okg.graph_nodes WHERE :_chronos_branch_point >= 0"
    ),
    "okg.graph_edges": (
        "SELECT edge_id, src, dst FROM okg.graph_edges "
        "WHERE :_chronos_branch_point >= 0"
    ),
}


class _FakeCheckout:
    def read_relations(self) -> tuple[dict[str, str], dict[str, int]]:
        return dict(_FAKE_RELATIONS), {"_chronos_branch_point": 7}


def _route_executed_sql_to(monkeypatch: pytest.MonkeyPatch, graph_cls: type) -> None:
    """Test hook: send the spliced text to a fake checkout's `query`.

    The fakes keep their state in the test's closure, so a fresh instance
    sees the same captures as the one `checkout_generation` returned.
    """
    monkeypatch.setattr(
        graph_sql_splice,
        "execute_spliced_sql",
        lambda conn, sql: graph_cls().query(sql),
    )


# ---------- happy path ----------


def test_query_payload_omits_heavy_columns_and_caps_bytes() -> None:
    payload = {
        "rows": [
            [f"n:{idx}", [0.1] * 2048, "x" * 20_000]
            for idx in range(100)
        ],
        "columns": ["node_id", "embedding", "text"],
        "total_count": 100,
        "truncated": False,
        "pinned_generation_id": "gen:test",
        "query_plan_summary": "fixture",
        "elapsed_ms": 1,
    }

    bounded = _bound_query_payload(payload)

    assert bounded["payload_bytes"] <= bounded["payload_byte_cap"]
    assert bounded["omitted_columns"] == ["embedding"]
    assert bounded["rows"][0][1] == "[omitted:heavy_column]"
    assert bounded["truncated_cells"] > 0
    assert bounded["payload_truncated"] is True
    assert bounded["payload_rows_omitted"] > 0
    assert bounded["truncated"] is True


def test_run_sql_returns_rows_envelope(clean_runtime: str) -> None:
    """A SELECT against okg.v_nodes returns the documented envelope."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _insert_node_facts(conn, [
            ("n:a", "page", "alpha"),
            ("n:b", "page", "beta"),
        ])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        out = run_sql(
            conn, s,
            "SELECT node_id, subtype FROM okg.v_nodes "
            " WHERE subtype = 'page' ORDER BY node_id",
        )
    assert out["columns"] == ["node_id", "subtype"]
    ids = [row[0] for row in out["rows"]]
    assert ids == ["n:a", "n:b"]
    assert out["returned_count"] == 2
    assert "total_count" not in out
    assert out["truncated"] is False
    assert out["pinned_generation_id"] == s.generation_id
    assert isinstance(out["query_plan_summary"], str)
    assert out["query_plan_summary"]
    assert isinstance(out["elapsed_ms"], int)
    assert out["elapsed_ms"] >= 0


def test_run_sql_allows_like_percent_wildcards_on_graph_views(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _insert_node_facts(conn, [
            ("n:a", "page", "alpha"),
            ("n:b", "page", "beta"),
            ("x:c", "page", "gamma"),
        ])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        out = run_sql(
            conn,
            s,
            "SELECT node_id FROM okg.v_nodes "
            "WHERE node_id LIKE 'n:%' ORDER BY node_id",
        )

    assert out["rows"] == [["n:a"], ["n:b"]]
    assert out["pinned_generation_id"] == s.generation_id


# ---------- row_cap truncation ----------


def test_run_sql_truncates_at_row_cap(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _insert_node_facts(conn, [
            (f"n:{i:03d}", "page", f"t{i}") for i in range(20)
        ])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        out = run_sql(
            conn, s,
            "SELECT node_id FROM okg.v_nodes WHERE subtype = 'page'",
            row_cap=5,
        )
    assert len(out["rows"]) == 5
    assert out["truncated"] is True


def test_graph_backed_query_pushes_row_cap_into_chronos_execution(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(conn, deployment_name="default")

    captured: dict[str, object] = {}

    class _CapturingGraph(_FakeCheckout):
        def query(self, sql: str, params: dict | None = None) -> list[dict]:
            captured["sql"] = sql
            captured["params"] = params
            return [{"node_id": f"n:{idx}"} for idx in range(6)]

    class _CapturingStore:
        def checkout_generation(self, generation_id: str, *, branch: str = "default"):
            captured["generation_id"] = generation_id
            captured["branch"] = branch
            return _CapturingGraph()

    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        monkeypatch.setattr(
            ChronosGraphStore,
            "from_connection",
            classmethod(lambda cls, conn, **kwargs: _CapturingStore()),
        )
        _route_executed_sql_to(monkeypatch, _CapturingGraph)
        out = run_sql(
            conn,
            s,
            "SELECT node_id FROM okg.v_nodes ORDER BY node_id",
            row_cap=5,
        )

    assert len(out["rows"]) == 5
    assert out["truncated"] is True
    assert captured["generation_id"] == s.generation_id
    assert captured["branch"] == "default"
    assert "okg.graph_nodes" in str(captured["sql"])
    assert "LIMIT 6" in str(captured["sql"])


@pytest.mark.parametrize(
    "sql, physical_view",
    [
        (
            'SELECT node_id FROM "okg"."v_nodes" ORDER BY node_id',
            "okg.graph_nodes",
        ),
        (
            'SELECT src, dst FROM "okg"."v_edges" ORDER BY edge_id',
            "okg.graph_edges",
        ),
        (
            'SELECT node_id FROM "OKG"."V_NODES" ORDER BY node_id',
            "okg.graph_nodes",
        ),
    ],
)
def test_quoted_graph_views_route_through_bounded_chronos_execution(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    sql: str,
    physical_view: str,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(conn, deployment_name="default")

    captured: dict[str, object] = {}

    class _CapturingGraph(_FakeCheckout):
        def query(self, sql: str, params: dict | None = None) -> list[dict]:
            captured["sql"] = sql
            return [
                {"node_id": f"n:{idx}", "src": "n:a", "dst": "n:b"}
                for idx in range(3)
            ]

    class _CapturingStore:
        def checkout_generation(self, generation_id: str, *, branch: str = "default"):
            captured["generation_id"] = generation_id
            return _CapturingGraph()

    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        monkeypatch.setattr(
            ChronosGraphStore,
            "from_connection",
            classmethod(lambda cls, conn, **kwargs: _CapturingStore()),
        )
        _route_executed_sql_to(monkeypatch, _CapturingGraph)
        out = run_sql(conn, s, sql, row_cap=2)

    assert len(out["rows"]) == 2
    assert out["truncated"] is True
    assert captured["generation_id"] == s.generation_id
    assert physical_view in str(captured["sql"])
    assert "LIMIT 3" in str(captured["sql"])


# ---------- composite deployment-view inlining ----------


def test_run_sql_inlines_composite_view_over_graph(clean_runtime: str) -> None:
    """A composite okg.v_* view built on okg.v_nodes is inlined and routed
    through the Chronos checkout.

    Post-cutover the base okg.graph_nodes is an empty shell -- state is
    reconstructed only by checking out the published checkpoint, and the
    checkout rewrites only literal registered-table references in the
    submitted SQL. A deployment view (e.g. okg.v_repo_cleanup_candidates)
    left intact would read the empty base (0 rows). The dispatcher expands
    such views via pg_get_viewdef so their v_nodes/v_edges references surface.
    pg_get_viewdef emits implicit-alias column references (`v_nodes.node_id`),
    so this also exercises the base-view alias-preservation path.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _insert_node_facts(conn, [
            ("n:a", "page", "alpha"),
            ("n:b", "page", "beta"),
            ("n:c", "note", "gamma"),
        ])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "CREATE OR REPLACE VIEW okg.v_test_pages AS "
                "SELECT node_id, subtype FROM okg.v_nodes "
                "WHERE subtype = 'page'"
            )
            cur.execute("GRANT SELECT ON okg.v_test_pages TO app_ro")

    try:
        with psycopg.connect(clean_runtime) as conn:
            s = open_session(conn, deployment_name="default")
            out = run_sql(
                conn, s,
                "SELECT node_id FROM okg.v_test_pages ORDER BY node_id",
            )

        # Without inlining this returns [] (empty base graph_nodes); the 'note'
        # node is filtered out by the composite view's WHERE.
        assert [row[0] for row in out["rows"]] == ["n:a", "n:b"]
        assert out["returned_count"] == 2
        # Confirms the query took the checkout path, not the app_ro EXPLAIN path.
        assert out["query_plan_summary"] == "Chronos checkout"
        assert out["pinned_generation_id"] == s.generation_id
    finally:
        # A fixture view left behind in the shared test database is an
        # unregistered `okg.v_*` view for every later test, which
        # okg.grant_registry_drift() reports by name (schema-grant-coherence).
        # clean_runtime truncates rows; it does not drop DDL.
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute("DROP VIEW IF EXISTS okg.v_test_pages")


@pytest.mark.parametrize(
    "view_sql",
    [
        'SELECT node_id FROM "okg"."v_test_pages" ORDER BY node_id',
        'SELECT node_id FROM U&"okg".U&"v_test_pages" ORDER BY node_id',
    ],
)
def test_composite_view_expansion_keeps_graph_bounds(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    view_sql: str,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(conn, deployment_name="default")
        with conn.cursor() as cur:
            cur.execute(
                "CREATE OR REPLACE VIEW okg.v_test_pages AS "
                "SELECT node_id, subtype FROM okg.v_nodes "
                "WHERE subtype = 'page'"
            )
            cur.execute("GRANT SELECT ON okg.v_test_pages TO app_ro")

    captured: dict[str, str] = {}

    class _CapturingGraph(_FakeCheckout):
        def query(self, sql: str, params: dict | None = None) -> list[dict]:
            captured["sql"] = sql
            return [{"node_id": f"n:{idx}"} for idx in range(4)]

    class _CapturingStore:
        def checkout_generation(self, generation_id: str, *, branch: str = "default"):
            return _CapturingGraph()

    try:
        with psycopg.connect(clean_runtime) as conn:
            s = open_session(conn, deployment_name="default")
            monkeypatch.setattr(
                ChronosGraphStore,
                "from_connection",
                classmethod(lambda cls, conn, **kwargs: _CapturingStore()),
            )
            _route_executed_sql_to(monkeypatch, _CapturingGraph)
            out = run_sql(
                conn,
                s,
                view_sql,
                row_cap=3,
            )

        assert len(out["rows"]) == 3
        assert out["truncated"] is True
        assert out["pinned_generation_id"] == s.generation_id
        assert "okg.graph_nodes" in captured["sql"]
        assert "LIMIT 4" in captured["sql"]
    finally:
        # See the sibling test: an unregistered `okg.v_*` view left in the
        # shared database is a drift finding for every later test.
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute("DROP VIEW IF EXISTS okg.v_test_pages")


# RETIRED (chronos-native cutover): test_run_sql_exposes_source_run_activity.
# It asserted okg.v_source_runs reports per-run append counts
# (nodes_appended == 2, ...) derived from nodes_live/edges_live
# node-fact-projection bookkeeping, which the cutover dropped.
# okg.v_source_runs now reads persisted `sync.run.<run_id>` /
# stage-timing progress payloads and fact logs, and projects zero rows
# for a source with no per-run evidence (PACT
# fix-v-source-runs-null-placeholder); it is covered by
# tests/substrate/mcp/test_v_source_runs_readability.py. The run_sql
# dispatcher's view-exposure contract remains covered by
# test_run_sql_returns_rows_envelope (okg.v_nodes) and
# test_run_sql_exposes_runtime_queue_health (okg.v_runtime_queue_health).


def test_run_sql_exposes_runtime_queue_health(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(conn, deployment_name="default")
    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        out = run_sql(
            conn, s,
            "SELECT queue_name, status, availability, active_count, dlq_count, "
            "workflow_name FROM okg.v_runtime_queue_health "
            "ORDER BY queue_name NULLS FIRST, workflow_name NULLS FIRST",
        )

    assert out["columns"] == [
        "queue_name",
        "status",
        "availability",
        "active_count",
        "dlq_count",
        "workflow_name",
    ]
    assert out["rows"]

    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        with pytest.raises(RunSqlInvalid, match="last_error"):
            run_sql(conn, s, "SELECT last_error FROM okg.v_runtime_queue_health")


def test_postgres_path_executes_exactly_the_checked_text(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """sql-safety-without-pglast: the database runs the statement the checks
    judged (U& decoded, outer comments and `;` trimmed), never the raw input."""
    import okg.substrate.mcp.run_sql as run_sql_module
    from okg.substrate.mcp.sql_safety import checked_run_sql

    raw = (
        "/* lead */ SELECT status FROM U&\"okg\".U&\"v_runtime_queue_h\\0065alth\" "
        "ORDER BY status NULLS FIRST ; -- tail"
    )
    checked = checked_run_sql(raw)
    assert checked == (
        'SELECT status FROM "okg"."v_runtime_queue_health" ORDER BY status NULLS FIRST'
    )
    executed: list[str] = []
    real = run_sql_module._execute_one_statement

    def record(conn, cur, text):
        executed.append(text)
        return real(conn, cur, text)

    monkeypatch.setattr(run_sql_module, "_execute_one_statement", record)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(conn, deployment_name="default")
    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        out = run_sql(conn, s, raw)
    assert out["columns"] == ["status"]
    assert executed == ["EXPLAIN (FORMAT JSON) " + checked, checked]


def test_postgres_path_cannot_send_a_second_statement(clean_runtime: str) -> None:
    """Even if the parser ever miscounted, the execution path uses the
    extended protocol, where PostgreSQL itself refuses a second command."""
    from okg.substrate.mcp.run_sql import _execute_one_statement

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            with pytest.raises(psycopg.errors.SyntaxError, match="multiple commands"):
                _execute_one_statement(conn, cur, "SELECT 1; SELECT 2")
        conn.rollback()
        with conn.cursor() as cur:
            _execute_one_statement(conn, cur, "SELECT 'a%b' LIKE 'a%'")
            assert cur.fetchall() == [(True,)]


def test_graph_table_string_literal_does_not_force_chronos_route(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(conn, deployment_name="default")

    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        out = run_sql(
            conn,
            s,
            "SELECT 'okg.graph_nodes' AS literal "
            "FROM okg.v_runtime_queue_health",
        )

    assert out["columns"] == ["literal"]
    assert out["query_plan_summary"] != "Chronos checkout"


def test_run_sql_clamps_row_cap_at_ceiling(clean_runtime: str) -> None:
    """Agent-supplied row_cap above the ceiling is clamped."""
    from okg.substrate.mcp import run_sql as run_sql_mod

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:a", "page", "x")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        # Pass an absurd row_cap; the function clamps internally.
        # We can't directly observe the clamp here, but the call
        # must succeed (no overflow / unbounded fetch).
        out = run_sql_mod.run_sql(
            conn, s,
            "SELECT node_id FROM okg.v_nodes",
            row_cap=10_000_000,
        )
    assert out["truncated"] is False  # we have 1 row


# ---------- safety layer surfaces structured errors ----------


def test_run_sql_rejects_non_select(clean_runtime: str) -> None:
    """A non-SELECT statement is refused by the safety layer
    BEFORE any DB call."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(conn, deployment_name="default")

    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        with pytest.raises(RunSqlInvalid):
            run_sql(conn, s, "INSERT INTO okg.v_nodes VALUES (1)")


def test_run_sql_rejects_disallowed_relation(clean_runtime: str) -> None:
    """A SELECT against a non-`v_*` relation is refused by safety."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(conn, deployment_name="default")

    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        with pytest.raises(RunSqlInvalid):
            run_sql(conn, s, "SELECT * FROM okg.nodes_live")


# ---------- plan-time error ----------


def test_run_sql_plan_time_error_for_unknown_column(
    clean_runtime: str,
) -> None:
    """A SELECT referencing an unknown column surfaces as
    `RunSqlInvalid` (the plan-time EXPLAIN catches it)."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(conn, deployment_name="default")

    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        with pytest.raises(RunSqlInvalid, match="plan-time"):
            run_sql(
                conn, s,
                "SELECT no_such_column FROM okg.v_nodes",
            )


# ---------- timeout ----------


def test_run_sql_timeout_surfaces_as_runsql_timeout(
    clean_runtime: str,
) -> None:
    """`pg_sleep` exceeds a tight timeout → `RunSqlTimeout`."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(conn, deployment_name="default")

    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        with pytest.raises((RunSqlTimeout, RunSqlInvalid)):
            # `pg_sleep` is a forbidden function per sql_safety,
            # so this may surface as RunSqlInvalid (forbidden) OR
            # RunSqlTimeout (if the safety layer doesn't catch it).
            # Either is correct; the test is for "the dispatcher
            # produces a structured error".
            run_sql(
                conn, s,
                "SELECT pg_sleep(2) FROM okg.v_nodes LIMIT 1",
                timeout_ms=200,
            )


def test_graph_backed_query_timeout_surfaces_as_runsql_timeout(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(conn, deployment_name="default")

    class _TimeoutGraph(_FakeCheckout):
        def query(self, sql: str, params: dict | None = None) -> list[dict]:
            raise psycopg.errors.QueryCanceled("synthetic graph timeout")

    class _TimeoutStore:
        def checkout_generation(self, generation_id: str, *, branch: str = "default"):
            return _TimeoutGraph()

    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        monkeypatch.setattr(
            ChronosGraphStore,
            "from_connection",
            classmethod(lambda cls, conn, **kwargs: _TimeoutStore()),
        )
        _route_executed_sql_to(monkeypatch, _TimeoutGraph)
        with pytest.raises(RunSqlTimeout) as exc_info:
            run_sql(
                conn,
                s,
                "SELECT node_id FROM okg.v_nodes",
                timeout_ms=200,
            )

    assert exc_info.value.code == "run_sql_timeout"
    assert exc_info.value.data["timeout_ms"] == 200


def test_graph_backed_query_statement_timeout_hits_real_chronos_query(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:slow", "page", "slow")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE OR REPLACE FUNCTION okg.run_sql_test_sleep(value text)
                RETURNS text
                LANGUAGE plpgsql
                AS $$
                BEGIN
                    PERFORM pg_sleep(1);
                    RETURN value;
                END;
                $$
                """
            )

    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        with pytest.raises(RunSqlTimeout) as exc_info:
            run_sql(
                conn,
                s,
                "SELECT okg.run_sql_test_sleep(node_id) AS node_id "
                "FROM okg.v_nodes",
                timeout_ms=100,
            )
        assert exc_info.value.code == "run_sql_timeout"
        assert exc_info.value.data["timeout_ms"] == 100
        assert conn.info.transaction_status != TransactionStatus.INERROR
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1


def test_graph_backed_query_preserves_caller_transaction(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:tx", "page", "tx")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TEMP TABLE run_sql_tx_keep "
                "(value integer) ON COMMIT DROP"
            )
            cur.execute("INSERT INTO run_sql_tx_keep VALUES (1)")

        out = run_sql(
            conn,
            s,
            "SELECT node_id FROM okg.v_nodes WHERE node_id = 'n:tx'",
        )

        assert out["rows"] == [["n:tx"]]
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM run_sql_tx_keep")
            assert cur.fetchone()[0] == 1


def test_graph_backed_query_restores_autocommit_connection(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:auto", "page", "auto")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        out = run_sql(
            conn,
            s,
            "SELECT node_id FROM okg.v_nodes WHERE node_id = 'n:auto'",
        )

        assert out["rows"] == [["n:auto"]]
        assert conn.autocommit is True
        assert conn.info.transaction_status == TransactionStatus.IDLE


# ---------- pin enforcement ----------


def test_run_sql_pins_to_session_generation(clean_runtime: str) -> None:
    """A query through run_sql sees the rows live at the session's
    pinned generation, not later ones."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:a", "page", "first")])
    r1 = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
    assert s.generation_id == r1["generation_id"]

    # Publish a second generation with a new node.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _insert_node_facts(conn, [("n:b", "page", "second")])
    r2 = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert r2["generation_id"] > r1["generation_id"]

    # The pinned session sees only the first node.
    with psycopg.connect(clean_runtime) as conn:
        out = run_sql(
            conn, s,
            "SELECT node_id FROM okg.v_nodes WHERE subtype = 'page' "
            "ORDER BY node_id",
        )
    ids = [row[0] for row in out["rows"]]
    assert ids == ["n:a"]
    assert out["pinned_generation_id"] == r1["generation_id"]


def test_run_sql_graph_join_uses_one_checkout_for_nodes_and_edges(
    clean_runtime: str,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog_version(conn)
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact("mix:a", "page", {"text": "a"}, {"id": "mix:a"}, {"v": 1}),
                NodeFact("mix:b", "page", {"text": "b"}, {"id": "mix:b"}, {"v": 1}),
                EdgeFact(
                    "mix:a",
                    "mix:b",
                    "links",
                    source_record_id={"id": "mix:e1"},
                    source_revision={"v": 1},
                ),
            ],
            strict_endpoints=True,
        )
        first = store.checkpoint_generation(
            "okg:default:branch:default:gen:mix-one",
            deployment_name="default",
            branch="default",
            catalog_version_id=cv,
        )

    with psycopg.connect(clean_runtime) as conn:
        pinned = open_session(conn, deployment_name="default")
    assert pinned.generation_id == first

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact("mix:c", "page", {"text": "c"}, {"id": "mix:c"}, {"v": 1}),
                EdgeFact(
                    "mix:b",
                    "mix:c",
                    "links",
                    source_record_id={"id": "mix:e2"},
                    source_revision={"v": 1},
                ),
            ],
            strict_endpoints=True,
        )
        second = store.checkpoint_generation(
            "okg:default:branch:default:gen:mix-two",
            deployment_name="default",
            branch="default",
            catalog_version_id=cv,
        )
    assert second != first

    sql = """
        SELECT n.node_id, e.dst
          FROM okg.v_nodes AS n
          JOIN okg.v_edges AS e ON e.src = n.node_id
         WHERE n.node_id LIKE 'mix:%'
         ORDER BY n.node_id, e.dst
    """
    with psycopg.connect(clean_runtime) as conn:
        stale = run_sql(conn, pinned, sql)
        latest_session = open_session(conn, deployment_name="default")
        latest = run_sql(conn, latest_session, sql)

    assert stale["query_plan_summary"] == "Chronos checkout"
    assert stale["pinned_generation_id"] == first
    assert stale["rows"] == [["mix:a", "mix:b"]]
    assert latest_session.generation_id == second
    assert latest["rows"] == [["mix:a", "mix:b"], ["mix:b", "mix:c"]]


# ---------- graph path runs the user query read-only (issues #2406, #2585) ----------
#
# The graph path runs as the MCP login role okg_mcp, not app_ro. These tests
# pin the part that is fixed: the user's rewritten query cannot write, and
# nothing about that leaks into a caller's transaction. They do NOT claim the
# graph path limits reads; the role that would is a pending decision.


def _publish_one_graph_node(dsn: str, node_id: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
        _insert_node_facts(conn, [(node_id, "page", "readonly")])
    Publisher(
        dsn, "default",
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()


def _install_write_probe(dsn: str) -> None:
    """A function that writes. The checker would refuse calling it; the
    tests bypass the checker to prove the database refuses the write too."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS okg.run_sql_readonly_sink (value text)"
        )
        cur.execute("TRUNCATE okg.run_sql_readonly_sink")
        cur.execute(
            """
            CREATE OR REPLACE FUNCTION okg.run_sql_readonly_probe(value text)
            RETURNS text
            LANGUAGE plpgsql
            AS $$
            BEGIN
                INSERT INTO okg.run_sql_readonly_sink VALUES (value);
                RETURN value;
            END;
            $$
            """
        )


def _sink_count(dsn: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.run_sql_readonly_sink")
        return int(cur.fetchone()[0])


def _caused_by_read_only(exc: BaseException) -> bool:
    seen: set[int] = set()
    stack: list[BaseException] = [exc]
    while stack:
        current = stack.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, psycopg.errors.ReadOnlySqlTransaction):
            return True
        stack.extend(
            e for e in (current.__cause__, current.__context__) if e is not None
        )
    return False


@pytest.mark.parametrize("caller_owned", [False, True])
def test_graph_path_write_below_the_checker_fails_read_only(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    caller_owned: bool,
) -> None:
    import okg.substrate.mcp.run_sql as run_sql_module

    _publish_one_graph_node(clean_runtime, "n:ro")
    _install_write_probe(clean_runtime)
    # Simulate a checker miss: the SQL reaches the graph path unvalidated.
    monkeypatch.setattr(run_sql_module, "checked_run_sql", lambda sql: sql)

    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        if caller_owned:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            assert conn.info.transaction_status == TransactionStatus.INTRANS
        else:
            conn.rollback()
        with pytest.raises(RunSqlInvalid) as exc_info:
            run_sql(
                conn,
                s,
                "SELECT okg.run_sql_readonly_probe(node_id) AS node_id "
                "FROM okg.v_nodes",
            )
        assert _caused_by_read_only(exc_info.value), exc_info.value
        if caller_owned:
            # The caller's transaction survives the refused write and can
            # still write: the read-only setting stayed inside the savepoint.
            assert conn.info.transaction_status == TransactionStatus.INTRANS
            with conn.cursor() as cur:
                cur.execute("SHOW transaction_read_only")
                assert cur.fetchone()[0] == "off"
                cur.execute(
                    "INSERT INTO okg.run_sql_readonly_sink VALUES ('caller')"
                )
            conn.commit()
    assert _sink_count(clean_runtime) == (1 if caller_owned else 0)


def test_graph_path_owned_transaction_returns_rows_and_leaves_no_setting(
    clean_runtime: str,
) -> None:
    _publish_one_graph_node(clean_runtime, "n:owned")
    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        conn.rollback()
        out = run_sql(
            conn, s, "SELECT node_id FROM okg.v_nodes WHERE node_id = 'n:owned'"
        )
        assert out["rows"] == [["n:owned"]]
        assert conn.info.transaction_status == TransactionStatus.IDLE
        with conn.cursor() as cur:
            cur.execute("SHOW transaction_read_only")
            assert cur.fetchone()[0] == "off"
            cur.execute("CREATE TEMP TABLE run_sql_after_owned (v int)")
            cur.execute("INSERT INTO run_sql_after_owned VALUES (1)")
        conn.commit()


def test_graph_path_caller_transaction_can_still_write_after_the_query(
    clean_runtime: str,
) -> None:
    _publish_one_graph_node(clean_runtime, "n:caller")
    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '45s'")
            cur.execute(
                "CREATE TEMP TABLE run_sql_caller_keep (v int) ON COMMIT DROP"
            )
            cur.execute("INSERT INTO run_sql_caller_keep VALUES (1)")

        out = run_sql(
            conn, s, "SELECT node_id FROM okg.v_nodes WHERE node_id = 'n:caller'"
        )

        assert out["rows"] == [["n:caller"]]
        assert conn.info.transaction_status == TransactionStatus.INTRANS
        with conn.cursor() as cur:
            cur.execute("SHOW transaction_read_only")
            assert cur.fetchone()[0] == "off"
            cur.execute("SHOW statement_timeout")
            assert cur.fetchone()[0] == "45s"
            cur.execute("INSERT INTO run_sql_caller_keep VALUES (2)")
            cur.execute("SELECT count(*) FROM run_sql_caller_keep")
            assert cur.fetchone()[0] == 2


def _pin_expiry(conn, pin_name: str):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT expires_at FROM okg.history_generation_pins "
            "WHERE pin_name = %s",
            (pin_name,),
        )
        row = cur.fetchone()
    assert row is not None, pin_name
    return row[0]


@pytest.mark.parametrize("caller_owned", [False, True])
def test_graph_path_renews_a_due_lease_before_going_read_only(
    clean_runtime: str,
    caller_owned: bool,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.history_read_guard import LeasedGenerationSession
    from okg.substrate.mcp.run_sql import (
        _read_spliced_graph_sql,
        _run_graph_query_in_caller_transaction,
        _run_graph_query_in_owned_transaction,
    )

    _publish_one_graph_node(clean_runtime, "n:lease")
    with psycopg.connect(clean_runtime) as conn:
        s = open_session(conn, deployment_name="default")
        conn.rollback()
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False, autocommit=False,
        ).checkout_generation(s.generation_id, branch=s.branch_name)
        assert isinstance(graph, LeasedGenerationSession)
        # Inside one transaction now() is fixed, so a renewal would write the
        # same expiry the acquisition did. Shorten the stored expiry first,
        # so a renewal is visible as a later expiry.
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.history_generation_pins "
                "SET expires_at = now() + interval '10 seconds' "
                "WHERE pin_name = %s",
                (graph._pin_name,),
            )
        before = _pin_expiry(conn, graph._pin_name)
        # Age the lease past its renewal point, so the query must renew it.
        # Without the renewal moved ahead of read-only, that renewal is a
        # write inside the read-only transaction and the query fails.
        graph._leased_at -= 200
        graph._leased_at_wall -= 200
        # The same read run_sql does: the checkout's relations, spliced into
        # the caller's text (issue #2899). Reading the relations through the
        # leased proxy would renew under read-only; the unified path reads
        # them from the inner session after the renewal.
        plan = graph_sql_splice.plan_graph_sql(
            "SELECT node_id FROM okg.v_nodes WHERE node_id = 'n:lease'", conn
        )
        executed: dict[str, str] = {}

        def read(reader):
            assert not isinstance(reader, LeasedGenerationSession)
            return _read_spliced_graph_sql(
                conn, reader, plan, row_cap=10, executed=executed
            )

        if caller_owned:
            rows = _run_graph_query_in_caller_transaction(
                conn, graph, read, timeout_ms=5000,
            )
            # The renewal is kept by the caller's transaction.
            assert _pin_expiry(conn, graph._pin_name) > before
        else:
            rows = _run_graph_query_in_owned_transaction(
                conn, graph, read, timeout_ms=5000, prior_autocommit=False,
            )
            # The owned transaction rolls back, and the renewal write with it
            # (as before this change); only the in-process lease clock resets.
            # What this proves is that the renewal ran before read-only: had
            # it not, the write would have been refused and no rows returned.
        assert [row["node_id"] for row in rows] == ["n:lease"]
        assert "WHERE node_id = 'n:lease'" in executed["sql"]
        assert graph._lease_age() < 60
