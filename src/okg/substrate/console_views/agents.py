"""Agents composer for the console Teams/Agents view.

Surfaces the deployment's real agents: ``agent_session`` nodes from
``okg.graph_nodes`` through the pinned Chronos checkout. In okg-workspace the
"agents" are the coding sessions (claude / codex), not the retired order /
role_agent coordination layer the old summary.json board modeled. Each row
carries real activity counts from node attrs (tool calls, turns, denials,
tokens, cost) plus an age and an ``active`` flag (``ended_at`` IS NULL).

Request-time. Totals (count / active / distinct branches) are computed over
the full result; the session list is capped with the omitted count reported.
Counts are computed (0 when absent), never fabricated.
"""

from __future__ import annotations

from typing import Any

from okg.substrate.console_views._chronos import checkout
from okg.substrate.console_views.today_workspace import _humanize_age
from okg.substrate.mcp.session import Session


_SESSION_CAP = 50

# Chronos checkout read (okg.graph_nodes); the checkout pins the generation.
_AGENT_SQL = """
SELECT
  node_id AS node_id,
  COALESCE(NULLIF(attrs->>'session_id',''), node_id) AS session_id,
  NULLIF(attrs->>'title','')      AS title,
  NULLIF(attrs->>'git_branch','') AS branch,
  NULLIF(attrs->>'model','')      AS model,
  COALESCE(NULLIF(attrs->>'tool_call_count','')::int, 0)    AS tool_calls,
  COALESCE(NULLIF(attrs->>'turn_count','')::int, 0)         AS turns,
  COALESCE(NULLIF(attrs->>'denial_count','')::int, 0)       AS denials,
  COALESCE(NULLIF(attrs->>'total_tokens','')::bigint, 0)    AS total_tokens,
  COALESCE(NULLIF(attrs->>'cost_usd_estimate','')::float, 0) AS cost_usd,
  NULLIF(attrs->>'ended_at','')::timestamptz        AS ended_at,
  NULLIF(attrs->>'last_modified_at','')::timestamptz AS last_modified_at
FROM okg.graph_nodes
WHERE subtype = 'agent_session'
ORDER BY
  COALESCE(NULLIF(attrs->>'last_modified_at','')::timestamptz,
           NULLIF(attrs->>'ended_at','')::timestamptz) DESC NULLS LAST
"""


def compose_agents(
    conn: Any,
    *,
    deployment_name: str,
    session: Session,
) -> dict[str, Any]:
    """agent_session nodes for the pinned Chronos generation."""
    sessions: list[dict[str, Any]] = []
    branches: set[str] = set()
    active = 0
    total = 0
    generation_id = session.generation_id

    for r in checkout(conn, session).query(_AGENT_SQL):
        node_id, session_id, title, branch, model = (
            r["node_id"], r["session_id"], r["title"], r["branch"], r["model"])
        tool_calls, turns, denials = r["tool_calls"], r["turns"], r["denials"]
        total_tokens, cost_usd = r["total_tokens"], r["cost_usd"]
        ended_at, last_modified_at = r["ended_at"], r["last_modified_at"]
        total += 1
        is_active = ended_at is None
        if is_active:
            active += 1
        if branch:
            branches.add(str(branch))
        sessions.append({
            "node_id": str(node_id),
            "session_id": str(session_id),
            # Prefer a human title; fall back to branch, then a short id.
            "title": (
                str(title) if title
                else (str(branch) if branch else str(session_id)[:8])
            ),
            "branch": str(branch) if branch else None,
            "model": str(model) if model else None,
            "tool_calls": int(tool_calls or 0),
            "turns": int(turns or 0),
            "denials": int(denials or 0),
            "total_tokens": int(total_tokens or 0),
            "cost_usd": round(float(cost_usd or 0.0), 2),
            "active": is_active,
            "age": _humanize_age(ended_at or last_modified_at),
        })

    shown = sessions[:_SESSION_CAP]
    return {
        "deployment": deployment_name,
        "generation_id": generation_id,
        "total": total,
        "active": active,
        "branch_count": len(branches),
        "truncated": len(sessions) - len(shown),
        "sessions": shown,
    }
