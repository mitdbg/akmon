"""Decision-queue composer for the console Decisions view.

The operator's "what needs my verdict?" inbox: pact_change nodes awaiting a
decision — status in proposal / intent_review / changes_requested /
design_review — surfaced with the context to decide (why_text + impact_text),
not just a status chip. This is the substrate analog of the retired
coordination decide_queue.

Distinct from Open Work (Tasks), which is the full-lifecycle board over every
pact_change: Decisions is the narrow decision-needing subset, ordered by
urgency, and carries the rationale/impact so the operator can act. The
approve / request-changes verb is served by POST /viz/api/console/pact-review
(the Today view's control); this view is the read side that surfaces the queue.

Request-time via the pinned Chronos checkout. Counts/previews are computed
(empty when absent), never fabricated.
"""

from __future__ import annotations

from typing import Any

from okg.substrate.console_views._chronos import checkout
from okg.substrate.console_views.today_workspace import _humanize_age
from okg.substrate.mcp.session import Session


# Decision-needing lifecycle statuses, ordered most-actionable-first: a
# changes-requested PACT is waiting on a re-review; a proposal awaits the
# initial intent verdict.
_DECISION_STATUSES = [
    "changes_requested",
    "intent_review",
    "design_review",
    "proposal",
]
_STATUS_LABEL = {
    "changes_requested": "Changes requested",
    "intent_review": "Intent review",
    "design_review": "Design review",
    "proposal": "Proposal",
}
_QUEUE_CAP = 40

# Chronos checkout read (okg.graph_nodes); the checkout pins the generation, so
# no valid_from_gen / valid_to_gen predicate; recency from observed_at.
_DECISION_SQL = """
SELECT
  node_id AS node_id,
  attrs->>'change_id' AS change_id,
  NULLIF(attrs->>'title','') AS title,
  attrs->>'status' AS status,
  NULLIF(attrs->>'why_text','') AS why_text,
  NULLIF(attrs->>'impact_text','') AS impact_text,
  COALESCE(NULLIF(attrs->>'blocked_task_count','')::int, 0) AS blocked,
  CASE WHEN jsonb_typeof(attrs->'evidence_gaps') = 'array'
       THEN jsonb_array_length(attrs->'evidence_gaps') ELSE 0 END AS evidence_gaps,
  NULLIF(attrs->>'last_modified_at','')::timestamptz AS last_modified_at,
  observed_at AS observed_at
FROM okg.graph_nodes
WHERE subtype = 'pact_change'
  AND attrs->>'status' IN
    ('proposal', 'intent_review', 'design_review', 'changes_requested')
ORDER BY
  COALESCE(NULLIF(attrs->>'last_modified_at','')::timestamptz,
           observed_at) DESC NULLS LAST
"""


def _preview(text: str | None, limit: int = 200) -> str | None:
    if not text:
        return None
    flat = " ".join(text.split())
    if len(flat) <= limit:
        return flat
    return flat[: limit - 1].rstrip() + "…"


def _urgency(status: str) -> int:
    try:
        return _DECISION_STATUSES.index(status)
    except ValueError:
        return len(_DECISION_STATUSES)


def compose_decision_queue(
    conn: Any,
    *,
    deployment_name: str,
    session: Session,
) -> dict[str, Any]:
    """pact_change awaiting a decision from the pinned Chronos generation."""
    decisions: list[dict[str, Any]] = []
    by_status: dict[str, int] = {}
    generation_id = session.generation_id

    for r in checkout(conn, session).query(_DECISION_SQL):
        change_id = r["change_id"]
        if not change_id:
            continue
        node_id, title = r["node_id"], r["title"]
        why_text, impact_text = r["why_text"], r["impact_text"]
        blocked, evidence_gaps = r["blocked"], r["evidence_gaps"]
        last_modified_at, observed_at = r["last_modified_at"], r["observed_at"]
        status = str(r["status"])
        by_status[status] = by_status.get(status, 0) + 1
        blocked_n = int(blocked or 0)
        gaps_n = int(evidence_gaps or 0)
        decisions.append({
            "node_id": str(node_id),
            "change_id": str(change_id),
            "title": str(title) if title else str(change_id),
            "status": status,
            "status_label": _STATUS_LABEL.get(status, status.replace("_", " ").title()),
            "why": _preview(why_text),
            "impact": _preview(impact_text),
            "blocked": blocked_n,
            "evidence_gaps": gaps_n,
            # Decision-readiness: a PACT with no blockers and no evidence
            # gaps is decidable now; either one means it needs work first.
            "ready": blocked_n == 0 and gaps_n == 0,
            "age": _humanize_age(last_modified_at or observed_at),
        })

    # Most-actionable-first, then recency (rows arrive newest-first, so a
    # stable sort on urgency preserves that within a status).
    decisions.sort(key=lambda d: _urgency(d["status"]))
    total = len(decisions)
    ready_count = sum(1 for d in decisions if d["ready"])
    shown = decisions[:_QUEUE_CAP]

    return {
        "deployment": deployment_name,
        "generation_id": generation_id,
        "total": total,
        "ready_count": ready_count,
        "needs_work_count": total - ready_count,
        "by_status": by_status,
        "truncated": total - len(shown),
        "decisions": shown,
    }
