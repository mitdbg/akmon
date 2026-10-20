"""Console status composer — powers the v2 topbar.

Returns the live generation + freshness-derived health + node/edge
counts. Operator identity is resolved at the viz layer (env / git
config) and passed in, since the substrate doesn't know who's
looking.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from okg.substrate.console_views._chronos import (
    GenerationCountsUnavailable,
    checkout,
    generation_counts,
    generation_published_at,
)
from okg.substrate.console_views.today_workspace import _humanize_age
from okg.substrate.mcp.session import Session


def compose_status(
    conn: Any,
    *,
    session: Session,
    deployment_name: str,
    operator_email: str | None,
    operator_display_name: str | None,
) -> dict[str, Any]:
    generation_id = session.generation_id
    published_at = generation_published_at(conn, session)
    # Totals come from the pinned generation's checkpoint metadata (O(1)) — not a
    # count(*) over the multi-GB effective view (the ~12s status cost). On a
    # checkpoint that lacks stamped counts (pre-stamp / compacted) we fall through
    # to a live count: slower, but never a wrong/default number (the P0).
    try:
        nodes, edges = generation_counts(conn, session)
    except GenerationCountsUnavailable:
        graph = checkout(conn, session)
        # derivation-read-lint: allow #444 unstamped status fallback only.
        nodes = int(graph.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"] or 0)
        # derivation-read-lint: allow #444 unstamped status fallback only.
        edges = int(graph.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"] or 0)

    # Freshness-derived health: published within 6h = live, 24h =
    # idle, older = stale. Honest about what we can actually tell
    # from generation cadence (no doctor wiring yet).
    health = "stale"
    if published_at is not None:
        pat = published_at
        if pat.tzinfo is None:
            pat = pat.replace(tzinfo=timezone.utc)
        hours = (datetime.now(timezone.utc) - pat).total_seconds() / 3600.0
        if hours <= 6:
            health = "live"
        elif hours <= 24:
            health = "idle"
        else:
            health = "stale"

    operator: dict[str, str] | None = None
    if operator_email or operator_display_name:
        operator = {
            "display_name": operator_display_name
            or (operator_email.split("@", 1)[0] if operator_email else "operator"),
            "email": operator_email or "",
        }

    return {
        "deployment": deployment_name,
        "generation_id": generation_id,
        "generation_age": _humanize_age(published_at) if published_at else None,
        "health": health,
        "nodes": nodes,
        "edges": edges,
        **({"operator": operator} if operator is not None else {}),
    }
