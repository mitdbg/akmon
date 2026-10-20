"""Chronos-backed composer for the `okg_workspace.decisions` view.

Backs the Decisions view of the supervisor console: a browseable
log of `design_decision` rows grouped two ways — newest first and
by parent project (OpenSpec change_id).

The composer reads `okg.graph_nodes` through the request Session's pinned
Chronos checkout, never the retired live tables. It keeps the existing v2
payload contract's numeric generation bridge id when the session provides one.
"""

from __future__ import annotations

from typing import Any

from okg.substrate.console_views._chronos import checkout
# Reuse the slug humanizer from today_workspace so acronyms render
# consistently across both views.
from okg.substrate.console_views.today_workspace import (
    _humanize_slug,
    _humanize_age,
)
from okg.substrate.mcp.session import Session


def _payload_generation_id(session: Session) -> int | str:
    return (
        session.compat_generation_id
        if session.compat_generation_id is not None
        else session.generation_id
    )


# Chronos checkout read (okg.graph_nodes); the checkout pins the generation,
# so no valid_to_gen predicate; recency from the node's own observed_at.
_DECISIONS_SQL = """
SELECT
  node_id,
  attrs->>'title' AS title,
  attrs->>'change_id' AS change_id,
  attrs->>'decision_number' AS decision_number,
  -- Rationale text (substrate stores it on both `text` and
  -- `rationale`; prefer rationale, fall back to text).
  COALESCE(attrs->>'rationale', attrs->>'text') AS rationale,
  observed_at AS observed_at
FROM okg.graph_nodes
WHERE subtype = 'design_decision'
  AND attrs->>'title' IS NOT NULL
  AND attrs->>'title' <> ''
ORDER BY observed_at DESC NULLS LAST, node_id
"""


def _preview(rationale: str | None, limit: int = 120) -> str | None:
    """Return a short one-line preview of the rationale for the
    collapsed row state. Strips newlines + collapses whitespace.
    Default limit is 120 chars — enough to be informative,
    short enough to fit cleanly on one line at common viewports."""
    if not rationale:
        return None
    flat = " ".join(rationale.split())
    if len(flat) <= limit:
        return flat
    return flat[: limit - 1].rstrip() + "…"


def _full_text(rationale: str | None) -> str | None:
    """Return the full rationale text, lightly normalized. Preserves
    paragraph breaks (blank lines) but trims trailing whitespace."""
    if not rationale:
        return None
    # Trim trailing whitespace per line; preserve paragraph breaks.
    lines = [ln.rstrip() for ln in rationale.split("\n")]
    return "\n".join(lines).strip() or None


def compose_decisions(
    conn: Any,
    *,
    session: Session,
    generated_at: str,
) -> dict[str, Any]:
    """Build the Decisions payload.

    Recent: top 20 decisions newest-first.
    By project: top 8 change_ids by decision_count, each with its
    decisions (capped at 6 per group so the page stays bounded).
    """
    del generated_at  # eyebrow/greeting unused on this view
    generation_id = _payload_generation_id(session)

    rows: list[dict[str, Any]] = []
    for r in checkout(conn, session).query(_DECISIONS_SQL):
        node_id, title, change_id = r["node_id"], r["title"], r["change_id"]
        dnum, rationale, observed_at = r["decision_number"], r["rationale"], r["observed_at"]
        if not title:
            continue
        rows.append({
            "id": str(node_id),
            "title": str(title),
            "change_id": str(change_id) if change_id else "(unknown)",
            "decision_number": str(dnum) if dnum else None,
            "rationale_preview": _preview(rationale),
            "rationale_full": _full_text(rationale),
            "age": _humanize_age(observed_at),
            "_observed_at": observed_at,
        })

    total_count = len(rows)

    # Recent: top 20 by observed_at desc (already sorted by SQL).
    recent_items = [
        _strip_internal(r) for r in rows[:20]
    ]

    # By project: group + count + select top 8 by decision_count.
    by_project: dict[str, dict[str, Any]] = {}
    for r in rows:
        pid = r["change_id"]
        group = by_project.setdefault(pid, {
            "project_id": pid,
            "project_name": _humanize_slug(pid),
            "items": [],
            "_latest": None,
        })
        group["items"].append(_strip_internal(r))
        observed = r.get("_observed_at")
        if observed and (group["_latest"] is None or observed > group["_latest"]):
            group["_latest"] = observed

    groups = list(by_project.values())
    # Sort: largest projects first.
    groups.sort(key=lambda g: len(g["items"]), reverse=True)
    top_groups = groups[:8]

    out_groups: list[dict[str, Any]] = []
    for g in top_groups:
        out_groups.append({
            "project_id": g["project_id"],
            "project_name": g["project_name"],
            "decision_count": len(g["items"]),
            "latest_age": _humanize_age(g["_latest"]),
            # Cap items per group at 6 so the page doesn't run
            # away when a single project has 12+ decisions.
            "items": g["items"][:6],
        })

    return {
        "generation_id": generation_id,
        "total_count": total_count,
        "recent": {
            "empty_marker": "No design_decision rows in the substrate.",
            "items": recent_items,
        },
        "by_project": {
            "empty_marker": "No project groupings — corpus is empty.",
            "groups": out_groups,
        },
    }


def _strip_internal(row: dict[str, Any]) -> dict[str, Any]:
    """Drop fields with `_` prefix before serializing to the wire."""
    return {k: v for k, v in row.items() if not k.startswith("_")}
