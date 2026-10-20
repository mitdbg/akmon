from __future__ import annotations

import json

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import open_session
from okg.substrate.mcp.run_sql import run_sql
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _seed_transcript_nodes(conn: psycopg.Connection) -> None:
    ChronosGraphStore.from_connection(conn).apply_facts([
        NodeFact(
            "session:codex:s1",
            "agent_session",
            {
                "session_id": "codex:s1",
                "model": "gpt-5",
                "cwd": "/repo",
                "repo": "okg",
                "started_at": "2026-06-12T04:00:00Z",
                "last_active_at": "2026-06-12T04:05:00Z",
                "message_count": 2,
                "tool_call_count": 3,
                "input_tokens": 1200,
                "output_tokens": 300,
                "total_tokens": 1500,
                "cost_usd_estimate": 0.12,
                "cost_confidence": "measured",
                "cost_basis": "codex_token_count_event",
            },
            {"id": "session"},
            {"v": 1},
        ),
        NodeFact(
            "message:codex:s1:0",
            "agent_message",
            {
                "session_id": "codex:s1",
                "chapter_index": 0,
                "role": "assistant",
                "message_kind": "assistant_response",
                "content_excerpt": "I will inspect the graph.",
                "content_hash": "a" * 64,
                "tool_call_count": 2,
                "timestamp": "2026-06-12T04:01:00Z",
            },
            {"id": "message"},
            {"v": 1},
        ),
        NodeFact(
            "tool_use:codex:s1:1:0",
            "tool_use_event",
            {
                "event_id": "tool_use:codex:s1:1:0",
                "session_id": "codex:s1",
                "turn_index": 1,
                "block_index": 0,
                "tool_name": "search",
                "mcp_server": "okg-workspace",
                "mcp_deployment": "okg-workspace",
                "mcp_operator": "search",
                "file_path": "src/okg/substrate/mcp/search.py",
                "command": "search(query='context budget')",
                "input_excerpt": json.dumps({"query": "context budget"}),
                "timestamp": "2026-06-12T04:02:00Z",
            },
            {"id": "tool"},
            {"v": 1},
        ),
    ])


def _seed_mcp_call_log(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.mcp_call_log
              (session_id, deployment_name, tool_name, operator_name,
               args, cost, pin, elapsed_ms, result_count, error_code)
            VALUES
              ('codex:s1', 'okg-workspace', 'search', 'search',
               '{}'::jsonb,
               '{"response_bytes": 3200, "args_bytes": 120,
                 "response_token_estimate": 800,
                 "args_token_estimate": 30, "truncated": true}'::jsonb,
               'gen:test', 80, 10, NULL),
              ('codex:s1', 'okg-workspace', 'search', 'search',
               '{}'::jsonb,
               '{"response_bytes": 2800, "args_bytes": 110,
                 "response_token_estimate": 700,
                 "args_token_estimate": 28, "truncated": false}'::jsonb,
               'gen:test', 120, 8, NULL)
            """
        )


def test_agent_transcript_views_return_compact_pages(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _seed_transcript_nodes(conn)
        _seed_mcp_call_log(conn)
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        sessions = run_sql(
            conn,
            session,
            """
            SELECT session_id, model, message_count, tool_call_count,
                   total_tokens, cost_usd_estimate, cost_confidence
              FROM okg.v_agent_sessions
             WHERE session_id = 'codex:s1'
             ORDER BY tool_call_count DESC NULLS LAST
             LIMIT 5
            """,
            row_cap=5,
        )
        messages = run_sql(
            conn,
            session,
            """
            SELECT session_id, turn_index, role, message_kind,
                   content_excerpt, content_hash, tool_call_count
              FROM okg.v_agent_messages
             WHERE session_id = 'codex:s1'
             ORDER BY turn_index
             LIMIT 5
            """,
            row_cap=5,
        )
        tools = run_sql(
            conn,
            session,
            """
            SELECT session_id, turn_index, block_index, tool_name,
                   mcp_operator, file_path, command_excerpt, input_excerpt
              FROM okg.v_tool_use_events
             WHERE session_id = 'codex:s1'
             ORDER BY turn_index, block_index
             LIMIT 5
            """,
            row_cap=5,
        )
        costs = run_sql(
            conn,
            session,
            """
            SELECT operator_name, calls, response_bytes, truncated_calls,
                   error_count, p50_ms, p90_ms, max_response_bytes
              FROM okg.v_mcp_operator_costs
             WHERE deployment_name = 'okg-workspace'
               AND operator_name = 'search'
            """,
            row_cap=5,
        )

    assert sessions["rows"] == [[
        "codex:s1", "gpt-5", 2, 3, 1500, 0.12, "measured",
    ]]
    assert messages["columns"] == [
        "session_id", "turn_index", "role", "message_kind",
        "content_excerpt", "content_hash", "tool_call_count",
    ]
    assert messages["rows"][0][4] == "I will inspect the graph."
    assert "attrs" not in messages["columns"]

    assert tools["rows"][0][:5] == ["codex:s1", 1, 0, "search", "search"]
    assert "attrs" not in tools["columns"]
    assert len(tools["rows"][0][6]) <= 500
    assert len(tools["rows"][0][7]) <= 500

    assert costs["rows"] == [[
        "search", 2, 6000, 1, 0, 80, 120, 3200,
    ]]
