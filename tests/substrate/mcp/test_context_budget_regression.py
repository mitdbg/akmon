from __future__ import annotations

import psycopg
import pytest

from okg.substrate.mcp.context_budget import (
    DEFAULT_CONTEXT_BUDGETS,
    ContextBudgetScenario,
    build_context_budget_report,
    summarize_mcp_operator_costs,
)


pytestmark = pytest.mark.postgres


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
               'gen:test', 120, 8, NULL),
              ('codex:s1', 'okg-workspace', 'query', 'query',
               '{}'::jsonb,
               '{"response_bytes": 1900, "args_bytes": 300,
                 "truncated": false}'::jsonb,
               'gen:test', 45, 4, 'run_sql_timeout'),
              ('codex:s1', 'okg-workspace', 'inspect', 'inspect',
               '{}'::jsonb,
               '{"response_bytes": 900, "args_bytes": 50,
                 "truncated": false}'::jsonb,
               'gen:test', 30, 1, NULL)
            """
        )


def test_context_budget_report_summarizes_mcp_ledger(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_mcp_call_log(conn)
        operator_costs = summarize_mcp_operator_costs(
            conn, deployment_name="okg-workspace",
        )

    search = next(
        row for row in operator_costs if row["operator_name"] == "search"
    )
    assert search["calls"] == 2
    assert search["response_bytes"] == 6000
    assert search["truncated_calls"] == 1
    assert search["error_count"] == 0
    assert search["p50_ms"] == 80
    assert search["p90_ms"] == 120
    assert search["max_response_bytes"] == 3200

    query = next(
        row for row in operator_costs if row["operator_name"] == "query"
    )
    assert query["error_count"] == 1

    report = build_context_budget_report(
        generation_id="gen:test",
        trust_level="stale_context",
        mcp_graph_read=True,
        stale_sources=["okg-workspace.agent-sessions"],
        operator_costs=operator_costs,
        scenarios=[
            ContextBudgetScenario(
                "search_default_page",
                response_bytes=6000,
                budget_bytes=DEFAULT_CONTEXT_BUDGETS["search_default_page"],
                source="fixture",
            ),
            ContextBudgetScenario(
                "transcript_page",
                response_bytes=4100,
                budget_bytes=DEFAULT_CONTEXT_BUDGETS["transcript_page"],
                source="fixture",
            ),
        ],
    )

    assert report["graph_authority"]["state"] == "stale_context"
    assert report["graph_authority"]["generation_id"] == "gen:test"
    assert report["budget_evaluation"]["ok"] is True
    assert report["budget_evaluation"]["failures"] == []


def test_context_budget_report_flags_bloated_scenarios() -> None:
    report = build_context_budget_report(
        generation_id=None,
        trust_level=None,
        mcp_graph_read=False,
        operator_costs=[],
        scenarios=[
            ContextBudgetScenario(
                "search_default_page",
                response_bytes=DEFAULT_CONTEXT_BUDGETS[
                    "search_default_page"
                ] + 1,
                budget_bytes=DEFAULT_CONTEXT_BUDGETS["search_default_page"],
                source="synthetic-bloat",
            ),
            ContextBudgetScenario(
                "transcript_page",
                response_bytes=DEFAULT_CONTEXT_BUDGETS[
                    "transcript_page"
                ] + 1,
                budget_bytes=DEFAULT_CONTEXT_BUDGETS["transcript_page"],
                source="synthetic-bloat",
            ),
        ],
    )

    assert report["graph_authority"]["state"] == "unavailable"
    assert report["budget_evaluation"]["ok"] is False
    assert {
        item["name"] for item in report["budget_evaluation"]["failures"]
    } == {"search_default_page", "transcript_page"}
