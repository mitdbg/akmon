"""Substrate-native MCP tool-call tracing (M6.7.4).

Every MCP tool invocation writes one row to the
``okg.mcp_trace_events`` TimescaleDB hypertable. This file
verifies the happy path, the error path, the kill switch, and
the two SQL views that ship alongside the hypertable.
"""
from __future__ import annotations

import os

import psycopg
import pytest

pytest.importorskip("mcp")

from okg.substrate.mcp.server import build_server
from okg.substrate.mcp.tracing import _process_session_id, trace_tool_call


pytestmark = pytest.mark.postgres


def test_process_session_id_prefers_agent_runtime_identity(monkeypatch) -> None:
    for name in (
        "OKG_MCP_SESSION_ID", "OKG_AGENT_SESSION_ID", "CODEX_THREAD_ID",
        "CODEX_SESSION_ID", "CLAUDE_SESSION_ID", "AGENT_SESSION_ID",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("CODEX_THREAD_ID", "codex-thread-42")
    assert _process_session_id() == "codex-thread-42"

    # Operator configuration remains authoritative when both are present.
    monkeypatch.setenv("OKG_MCP_SESSION_ID", "operator-session")
    assert _process_session_id() == "operator-session"


def _seed_published(dsn: str) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now()) RETURNING catalog_version_id"
            )
            (catalog_version_id,) = cur.fetchone()
        ChronosGraphStore.from_connection(conn).apply_facts([
            NodeFact(
                "docs:intro",
                "page",
                {"text": "hi"},
                {"id": 1},
                {"rev": "a"},
            )
        ])
    from okg.substrate.attributes import materialize_effective_attributes_for_generation

    with psycopg.connect(dsn, autocommit=True) as conn:
        generation = ChronosGraphStore.from_connection(conn).checkpoint_generation(
            deployment_name="default",
            branch="default",
            catalog_version_id=catalog_version_id,
            metadata={"correlation_id": "test-seed"},
        )
        # A publish writes the overlay marker; the graph read path refuses a
        # pin without one, so a bare checkpoint must write it too.
        materialize_effective_attributes_for_generation(conn, generation_id=generation)


def _count_trace_rows(conn: psycopg.Connection, **where: object) -> int:
    clauses = " AND ".join(f"{k} = %s" for k in where)
    sql = "SELECT count(*) FROM okg.mcp_trace_events"
    if clauses:
        sql += " WHERE " + clauses
    with conn.cursor() as cur:
        cur.execute(sql, tuple(where.values()))
        return cur.fetchone()[0]


def test_successful_tool_call_writes_one_row(clean_runtime: str) -> None:
    """`inspect(kind="node")` through the MCP server lands exactly one
    trace row with the request + response captured.

    Migrated from `get_node` per `shrink-mcp-to-operator-surface`.
    """
    _seed_published(clean_runtime)

    mcp = build_server(
        dsn=clean_runtime,
        deployment_name="default",
    )
    # Pull the registered fn off FastMCP and call it like MCP would.
    tool = mcp._tool_manager._tools["inspect"]  # type: ignore[attr-defined]
    result = tool.fn(target={"kind": "node", "node_id": "docs:intro"})
    assert result is not None
    assert result["node_id"] == "docs:intro"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _count_trace_rows(conn, tool_name="inspect") == 1
        with conn.cursor() as cur:
            cur.execute(
                "SELECT response_json->>'node_id', "
                "       error_text, duration_ms "
                "FROM okg.mcp_trace_events "
                "WHERE tool_name='inspect'"
            )
            resp_node, err, dur = cur.fetchone()
            assert resp_node == "docs:intro"
            assert err is None
            assert dur is not None and dur >= 0
            cur.execute(
                """
                SELECT deployment_name, operator_name, replacement_operator,
                       cost->>'model',
                       (cost->>'response_bytes')::int
                  FROM okg.v_mcp_calls
                 WHERE tool_name='inspect'
                """
            )
            deployment, operator, replacement, model, response_bytes = (
                cur.fetchone()
            )
            assert deployment == "default"
            assert operator == "inspect"
            assert replacement is None  # canonical, no replacement
            assert model == "mcp_tool_cost_v1"
            assert response_bytes > 0
            cur.execute(
                """
                SELECT span_name, span_kind, status_code, tool_name,
                       session_id, payload_summary->>'model'
                  FROM okg.telemetry_spans
                 WHERE tool_name = 'inspect'
                 ORDER BY event_time DESC
                 LIMIT 1
                """
            )
            span = cur.fetchone()
            assert span is not None
            assert span[0] == "mcp.inspect"
            assert span[1] == "server"
            assert span[2] == "ok"
            assert span[3] == "inspect"
            assert span[4]
            assert span[5] == "mcp_tool_cost_v1"


def test_error_tool_call_writes_row_with_error_text(
    clean_runtime: str,
) -> None:
    """A tool that raises produces exactly one row with
    `error_text` set and `response_json` NULL — and the
    exception still propagates.

    Migrated from `traverse_path` to `expand(path=...)` per
    `shrink-mcp-to-operator-surface`.
    """
    _seed_published(clean_runtime)

    mcp = build_server(
        dsn=clean_runtime,
        deployment_name="default",
    )

    # expand validates at arg parse; a 4-hop path fails fast.
    tool = mcp._tool_manager._tools["expand"]  # type: ignore[attr-defined]
    with pytest.raises(Exception):
        tool.fn(
            input={"node_id": "docs:intro"},
            path=["a:out", "b:out", "c:out", "d:out"],
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT error_text, response_json "
                "FROM okg.mcp_trace_events "
                "WHERE tool_name='expand' "
                "ORDER BY event_time DESC LIMIT 1"
            )
            err, resp = cur.fetchone()
            # Built from the product's own bound, not copied from it. The
            # assertion said "exceeds max depth" until ec672ed258 (2026-08-14)
            # reworded the message; nothing caught the drift because
            # tests/substrate runs only in the nightly substrate-db-full lane.
            # Hardcoding the new wording would re-create the same coupling one
            # constant change later.
            from okg.substrate.mcp.tools import MAX_TRAVERSE_HOPS

            expected = f"exceeds the maximum of {MAX_TRAVERSE_HOPS} hops"
            assert err is not None
            assert expected in err.lower(), err
            assert resp is None


def test_trace_kill_switch_short_circuits(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`OKG_MCP_TRACE_ENABLED=0` in the server env suppresses all
    tracing writes."""
    _seed_published(clean_runtime)

    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    mcp = build_server(
        dsn=clean_runtime,
        deployment_name="default",
    )

    # Migrated: graph_stats → inspect(kind="graph", legacy="graph_stats")
    tool = mcp._tool_manager._tools["inspect"]  # type: ignore[attr-defined]
    tool.fn(target={"kind": "graph", "legacy": "graph_stats"})
    tool.fn(target={"kind": "graph", "legacy": "graph_stats"})
    tool.fn(target={"kind": "graph", "legacy": "graph_stats"})

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _count_trace_rows(conn, tool_name="inspect") == 0


def test_trace_write_failure_does_not_break_tool_call(
    schema_destructive_runtime: str,
) -> None:
    """If the trace insert fails (e.g. table dropped), the tool
    call's result still returns normally."""
    clean_runtime = schema_destructive_runtime
    _seed_published(clean_runtime)

    mcp = build_server(
        dsn=clean_runtime,
        deployment_name="default",
    )

    # Drop the trace table to force the insert to fail.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE okg.mcp_trace_events CASCADE")

    # Migrated: graph_stats → inspect(kind="graph", legacy="graph_stats")
    tool = mcp._tool_manager._tools["inspect"]  # type: ignore[attr-defined]
    # Must not raise despite the underlying insert failing. The fixture drops
    # this test's whole disposable database afterward; no shared schema replay
    # can alter the next test's session database.
    result = tool.fn(target={"kind": "graph", "legacy": "graph_stats"})
    assert "node_count" in result


def test_trace_sessions_view(clean_runtime: str) -> None:
    """`okg.trace_sessions` aggregates per session_id with
    first/last/count/errors."""
    _seed_published(clean_runtime)

    os.environ["OKG_MCP_SESSION_ID"] = "test-session-alpha"
    try:
        mcp = build_server(
            dsn=clean_runtime,
            deployment_name="default",
        )
        # Migrated: graph_stats → inspect(kind="graph", legacy="graph_stats")
        _t = mcp._tool_manager._tools["inspect"]  # type: ignore[attr-defined]
        _t.fn(target={"kind": "graph", "legacy": "graph_stats"})
        _t.fn(target={"kind": "graph", "legacy": "graph_stats"})
    finally:
        del os.environ["OKG_MCP_SESSION_ID"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT tool_call_count, unique_tool_count, error_count "
                "FROM okg.trace_sessions "
                "WHERE session_id = %s",
                ("test-session-alpha",),
            )
            row = cur.fetchone()
            assert row is not None
            cc, uc, ec = row
            assert cc == 2
            assert uc == 1  # both calls are the same `inspect` tool
            assert ec == 0


# `start_trace` and `end_trace` (the labeled trace-bracket tools) were
# removed by `shrink-mcp-to-operator-surface`. The bracketing feature
# is no longer exposed via MCP; the underlying machinery
# (`clear_current_label`, `set_current_label`) remains for any
# internal substrate code that wants to inject trace labels directly.


def test_trace_tool_call_swallows_errors_when_pool_bad() -> None:
    """Direct call to the helper with a broken pool must not
    raise."""
    class _BadPool:
        def connection(self):
            raise RuntimeError("no pool")

    # No raise — must be swallowed.
    trace_tool_call(
        _BadPool(),  # type: ignore[arg-type]
        deployment_name="default",
        tool_name="x",
        request={"k": 1},
        response=None,
        error=None,
        duration_ms=0,
    )
