"""Node labelling and subtype listing for the console.

`compose_subtype_list` backs `/viz/api/console/subtype`, and `_label_for`
names a node for that listing.

The generic node-detail composer that used to live here was removed with the
Graph, Trace, Performance and Deployment views (issue #1924): `/console/node`
was a right-rail read those views opened into and nothing else called it.

Reads the graph through the chronos IR checkout (okg.graph_nodes /
okg.graph_edges pinned to the request's generation), per
`migrate-console-reads-to-chronos-ir`.
"""

from __future__ import annotations

from typing import Any

from okg.substrate.console_views._chronos import checkout
from okg.substrate.console_views.console_count_rollup_read import (
    console_count_rollup_enabled,
)
from okg.substrate.console_views.record_layout import LABEL_KEYS
from okg.substrate.mcp.session import Session


def _label_for(node_id: str, subtype: str, attrs: dict[str, Any]) -> str:
    # The same name keys the record reads use, so a person is titled by its
    # `display_name` in the inspector too, not by its hash.
    for key in LABEL_KEYS:
        v = attrs.get(key)
        if v:
            s = str(v).split("\n", 1)[0].strip()
            return s[:80]

    if subtype == "agent_message":
        for key in ("content_excerpt", "message_body", "content", "body", "text"):
            value = attrs.get(key)
            if value:
                excerpt = " ".join(str(value).split())
                if excerpt:
                    return excerpt[:77] + ("…" if len(excerpt) > 77 else "")

    if subtype == "tool_use_event":
        for key in ("tool_name", "operation", "action", "command"):
            value = attrs.get(key)
            if value:
                action = " ".join(str(value).split())
                return f"Tool · {action}"[:80]

    if subtype == "agent_session":
        harness = str(attrs.get("harness") or attrs.get("agent_nickname") or "Agent")
        context = attrs.get("git_branch") or attrs.get("repository") or attrs.get("cwd")
        if context:
            return f"{harness.title()} session · {' '.join(str(context).split())}"[:80]
        return f"{harness.title()} session"

    kind = subtype.replace("_", " ").strip().title() or "Node"
    suffix = node_id.rsplit(":", 1)[-1].strip()
    return f"{kind} · {suffix}"[:80] if suffix else kind[:80]


# Chronos checkout reads: okg.graph_nodes, `:name` params, inlined integer cap.
# The checkout pins the generation, so no valid_to_gen predicate. "Most recent"
# = newest observed_at (the chronos ingest timestamp).
_SUBTYPE_LIST_SQL = """
SELECT node_id, subtype, attrs
FROM okg.graph_nodes
WHERE subtype = :subtype
ORDER BY observed_at DESC NULLS LAST, node_id
LIMIT {cap}
"""

# derivation-read-lint: allow #444 flag-off subtype detail fallback remains.
_SUBTYPE_COUNT_SQL = """
SELECT count(*) AS c FROM okg.graph_nodes WHERE subtype = :subtype
"""


def compose_subtype_list(
    conn: Any, *, session: Session, subtype: str, cap: int = 40,
) -> dict[str, Any]:
    """List a sample of nodes of a given subtype (newest first).
    Backs the right-rail subtype panel: click a subtype bar in
    Overview → see the actual nodes, each clickable into node
    detail."""
    graph = checkout(conn, session)
    # console-count-rollup (#444 slice 2): when the flag is ON, read the maintained
    # by-subtype rollup O(1) (pick this subtype's bucket; absent bucket with a present
    # sentinel is a real 0) instead of the per-subtype count(*); flag-OFF keeps the
    # raw scan. A missing rollup (no sentinel) fails loud, never a silent 0.
    if console_count_rollup_enabled():
        from okg.substrate.console_views.console_count_rollup_read import (
            COUNT_KIND_SUBTYPE,
            read_console_count_rollup,
        )

        counts = read_console_count_rollup(
            conn, generation_id=session.generation_id, kinds={COUNT_KIND_SUBTYPE}
        )
        total = counts.get((COUNT_KIND_SUBTYPE, subtype), 0)
    else:
        total = int(graph.query(_SUBTYPE_COUNT_SQL, {"subtype": subtype})[0]["c"] or 0)
    items: list[dict[str, Any]] = []
    for r in graph.query(_SUBTYPE_LIST_SQL.format(cap=cap), {"subtype": subtype}):
        items.append({
            "node_id": str(r["node_id"]),
            "label": _label_for(str(r["node_id"]), str(r["subtype"]), r["attrs"] or {}),
        })
    return {
        "subtype": subtype,
        "total": total,
        "shown": len(items),
        "items": items,
    }
