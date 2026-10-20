"""Context-budget helpers for MCP read-surface regression checks."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import psycopg


DEFAULT_CONTEXT_BUDGETS: dict[str, int] = {
    "search_default_page": 12_000,
    "search_selected_attrs": 35_000,
    "transcript_page": 16_000,
    "cli_json_page": 14_000,
    "budget_summary": 8_000,
}


@dataclass(frozen=True)
class ContextBudgetScenario:
    name: str
    response_bytes: int
    budget_bytes: int
    source: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source": self.source,
            "response_bytes": int(self.response_bytes),
            "budget_bytes": int(self.budget_bytes),
            "ok": int(self.response_bytes) <= int(self.budget_bytes),
        }


def classify_graph_authority(
    *,
    generation_id: str | int | None,
    trust_level: str | None,
    mcp_graph_read: bool | None,
    stale_sources: Iterable[str] = (),
) -> dict[str, Any]:
    """Normalize dogfood readiness into a budget-report authority state."""
    stale = list(stale_sources)
    if not generation_id or mcp_graph_read is False:
        state = "unavailable"
    elif trust_level == "stale_context" or stale:
        state = "stale_context"
    else:
        state = "available"
    return {
        "state": state,
        "generation_id": generation_id,
        "trust_level": trust_level,
        "mcp_graph_read": bool(mcp_graph_read),
        "stale_sources": stale,
    }


def summarize_mcp_operator_costs(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
) -> list[dict[str, Any]]:
    """Read typed operator-cost aggregates from the public view layer."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT operator_name, calls, response_bytes, args_bytes,
                   truncated_calls, error_count, p50_ms, p90_ms,
                   max_response_bytes, last_called_at
              FROM okg.v_mcp_operator_costs
             WHERE deployment_name = %s
             ORDER BY response_bytes DESC, operator_name
            """,
            (deployment_name,),
        )
        rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append({
            "operator_name": row[0],
            "calls": int(row[1] or 0),
            "response_bytes": int(row[2] or 0),
            "args_bytes": int(row[3] or 0),
            "truncated_calls": int(row[4] or 0),
            "error_count": int(row[5] or 0),
            "p50_ms": int(row[6]) if row[6] is not None else None,
            "p90_ms": int(row[7]) if row[7] is not None else None,
            "max_response_bytes": (
                int(row[8]) if row[8] is not None else None
            ),
            "last_called_at": row[9].isoformat() if row[9] else None,
        })
    return out


def evaluate_context_budgets(
    scenarios: Iterable[ContextBudgetScenario],
) -> dict[str, Any]:
    items = [scenario.as_dict() for scenario in scenarios]
    failures = [item for item in items if not item["ok"]]
    return {
        "ok": not failures,
        "scenarios": items,
        "failures": failures,
    }


def build_context_budget_report(
    *,
    generation_id: str | int | None,
    trust_level: str | None,
    mcp_graph_read: bool | None,
    operator_costs: list[dict[str, Any]],
    scenarios: Iterable[ContextBudgetScenario],
    stale_sources: Iterable[str] = (),
) -> dict[str, Any]:
    return {
        "graph_authority": classify_graph_authority(
            generation_id=generation_id,
            trust_level=trust_level,
            mcp_graph_read=mcp_graph_read,
            stale_sources=stale_sources,
        ),
        "operator_costs": operator_costs,
        "budget_evaluation": evaluate_context_budgets(scenarios),
    }
