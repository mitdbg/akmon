"""Open-Work composer for the console Tasks view.

Surfaces the deployment's real work-in-flight: ``pact_change`` nodes from
the pinned Chronos checkout, grouped by lifecycle ``status`` (proposal /
implementation / verification / ...). Each item carries counts computed
directly from the node attrs:

  - ``blocked``        — ``blocked_task_count`` (int attr)
  - ``evidence_gaps``  — length of the ``evidence_gaps`` array attr
  - ``next_tasks``     — length of the ``next_task_ids`` array attr
  - ``children``       — length of the ``child_change_ids`` array attr

This replaces the console's dead summary.json coordination board (active_orders
/ role_agents) — okg-workspace tracks work as PACTs in the substrate, not as the
retired order/role_agent coordination layer. Direct-SQL, request-time, same
trade-off as the sibling console composers. Counts are computed (0 when an
array is empty/absent), never fabricated; capped groups report the omitted
count.
"""

from __future__ import annotations

from typing import Any

from okg.substrate.console_views._chronos import checkout
from okg.substrate.console_views.today_workspace import _humanize_age
from okg.substrate.mcp.session import Session


# Canonical lifecycle ordering for the board. Unknown statuses sort after
# these (alphabetically) so a new lifecycle value surfaces rather than hides.
_LIFECYCLE_ORDER = [
    "proposal",
    "intent_review",
    "design_review",
    "changes_requested",
    "implementation",
    "verification",
    "complete",
    "done",
    "archived",
]
_LIFECYCLE_LABELS = {
    "proposal": "Proposal",
    "intent_review": "Intent review",
    "design_review": "Design review",
    "changes_requested": "Changes requested",
    "implementation": "Implementation",
    "verification": "Verification",
    "complete": "Complete",
    "done": "Done",
    "archived": "Archived",
}

# Per-group item cap. Groups larger than this report `truncated` so the UI can
# say "+N more" rather than silently dropping rows.
_GROUP_CAP = 50

# Chronos checkout read (okg.graph_nodes); the checkout pins the generation.
# "Observed" recency comes from the node's own observed_at. jsonb ops run fine
# through the checkout.
_PACT_SQL = """
SELECT
  node_id               AS node_id,
  attrs->>'change_id'   AS change_id,
  NULLIF(attrs->>'title','')                       AS title,
  COALESCE(NULLIF(attrs->>'status',''), 'unknown') AS status,
  COALESCE(NULLIF(attrs->>'blocked_task_count','')::int, 0) AS blocked,
  CASE WHEN jsonb_typeof(attrs->'evidence_gaps') = 'array'
       THEN jsonb_array_length(attrs->'evidence_gaps') ELSE 0 END AS evidence_gaps,
  CASE WHEN jsonb_typeof(attrs->'next_task_ids') = 'array'
       THEN jsonb_array_length(attrs->'next_task_ids') ELSE 0 END AS next_tasks,
  CASE WHEN jsonb_typeof(attrs->'child_change_ids') = 'array'
       THEN jsonb_array_length(attrs->'child_change_ids') ELSE 0 END AS children,
  NULLIF(attrs->>'last_modified_at','')::timestamptz AS last_modified_at,
  observed_at AS observed_at
FROM okg.graph_nodes
WHERE subtype = 'pact_change'
ORDER BY
  COALESCE(NULLIF(attrs->>'last_modified_at','')::timestamptz,
           observed_at) DESC NULLS LAST
"""


def _lifecycle_rank(status: str) -> tuple[int, str]:
    try:
        return (_LIFECYCLE_ORDER.index(status), "")
    except ValueError:
        # Unknown status: sort after all known ones, then alphabetically.
        return (len(_LIFECYCLE_ORDER), status)


def compose_tasks(
    conn: Any,
    *,
    deployment_name: str,
    session: Session,
) -> dict[str, Any]:
    """pact_change nodes grouped by lifecycle for the published generation."""
    groups: dict[str, list[dict[str, Any]]] = {}
    total = 0
    blocked_total = 0
    generation_id = session.generation_id

    for r in checkout(conn, session).query(_PACT_SQL):
        change_id = r["change_id"]
        if not change_id:
            continue
        node_id, title, status = r["node_id"], r["title"], r["status"]
        blocked, evidence_gaps = r["blocked"], r["evidence_gaps"]
        next_tasks, children = r["next_tasks"], r["children"]
        last_modified_at, observed_at = r["last_modified_at"], r["observed_at"]
        total += 1
        blocked = int(blocked or 0)
        if blocked > 0:
            blocked_total += 1
        groups.setdefault(str(status), []).append({
            "node_id": str(node_id),
            "change_id": str(change_id),
            "title": str(title) if title else str(change_id),
            "status": str(status),
            "blocked": blocked,
            "evidence_gaps": int(evidence_gaps or 0),
            "next_tasks": int(next_tasks or 0),
            "children": int(children or 0),
            "age": _humanize_age(last_modified_at or observed_at),
        })

    lifecycles: list[dict[str, Any]] = []
    for status in sorted(groups, key=_lifecycle_rank):
        items = groups[status]
        # Within a group, blocked first, then by recency (rows already arrive
        # newest-first from SQL, so a stable sort by -blocked preserves that).
        items.sort(key=lambda it: -it["blocked"])
        count = len(items)
        shown = items[:_GROUP_CAP]
        lifecycles.append({
            "key": status,
            "label": _LIFECYCLE_LABELS.get(status, status.replace("_", " ").title()),
            "count": count,
            "truncated": count - len(shown),
            "items": shown,
        })

    return {
        "deployment": deployment_name,
        "generation_id": generation_id,
        "total": total,
        "blocked_total": blocked_total,
        "lifecycles": lifecycles,
    }
