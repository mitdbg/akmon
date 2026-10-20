"""Worktrees composer for the console Worktrees view.

The substrate analog of the retired coordination workspace_leases: the real
git worktrees where agents have run, derived from agent_session.cwd. Each
worktree groups its sessions and surfaces the branch(es) worked, session count,
total tool calls, and last activity — "where has work happened, and on what
branch?".

okg-workspace has no lease/dirty/holder coordination state (that was the
memex/supervisor model), so this view reports what the substrate actually
knows: the worktree's label, its branch(es), and the agent-session activity in
it. Direct-SQL, request-time; counts computed (0/empty when absent), never
fabricated.

The recorded ``cwd`` is grouped on but not served (#2130): it is an absolute
path that names the machine and the account. Each row carries ``label`` and
``cwd_unavailable_reason`` instead.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from okg.substrate.console_views._chronos import checkout
from okg.substrate.console_views.served_text import SERVER_PATH_NOT_SERVED
from okg.substrate.console_views.today_workspace import _humanize_age
from okg.substrate.mcp.session import Session


_WORKTREE_CAP = 50

# Chronos checkout read (okg.graph_nodes); the checkout pins the generation.
_WORKTREE_SQL = """
SELECT
  node_id                         AS node_id,
  NULLIF(attrs->>'cwd','')        AS cwd,
  NULLIF(attrs->>'git_branch','') AS branch,
  COALESCE(NULLIF(attrs->>'tool_call_count','')::int, 0) AS tool_calls,
  COALESCE(NULLIF(attrs->>'total_tokens','')::bigint, 0) AS total_tokens,
  NULLIF(attrs->>'ended_at','')::timestamptz        AS ended_at,
  NULLIF(attrs->>'last_modified_at','')::timestamptz AS last_modified_at
FROM okg.graph_nodes
WHERE subtype = 'agent_session'
  AND NULLIF(attrs->>'cwd','') IS NOT NULL
"""


def _label(cwd: str) -> str:
    """A short, identifying worktree label from the cwd.

    `.../worktrees/<name>/okg` -> `<name>/okg`; otherwise the LAST path
    segment only. The worktree name (codex hash or claude branch slug) is the
    identifying part. Two segments outside `worktrees/` would serve a parent
    directory, which on a checkout in a home directory is the account name.
    """
    normalised = cwd.replace("\\", "/")
    marker = "/worktrees/"
    if marker in normalised:
        tail = normalised.split(marker, 1)[1].strip("/")
        if tail:
            return tail
    parts = [p for p in normalised.split("/") if p]
    return parts[-1] if parts else "(unnamed)"


def _idle_hours(when: datetime | None, now: datetime) -> int | None:
    """Whole hours since the worktree's last activity (None if no timestamp).

    The read-side buckets on this: Recent (<=48h), Aging (<=168h / 7d),
    Stale (>168h). It is the same `_latest` that `_humanize_age` renders, so
    the bucket and the displayed age agree (relative under 7d, absolute date
    at/over 7d). Never fabricated: no stamp -> None.
    """
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    seconds = (now - when).total_seconds()
    return int(seconds // 3600) if seconds > 0 else 0


def compose_worktrees(
    conn: Any,
    *,
    deployment_name: str,
    session: Session,
) -> dict[str, Any]:
    """agent_session grouped by cwd into a worktree inventory."""
    by_cwd: dict[str, dict[str, Any]] = {}
    generation_id = session.generation_id

    rows = checkout(conn, session).query(_WORKTREE_SQL)
    for r in rows:
        node_id, cwd, branch = r["node_id"], r["cwd"], r["branch"]
        tool_calls, total_tokens = r["tool_calls"], r["total_tokens"]
        ended_at, last_modified_at = r["ended_at"], r["last_modified_at"]
        cwd = str(cwd)
        wt = by_cwd.setdefault(cwd, {
            "cwd": cwd,
            "label": _label(cwd),
            "_branches": set(),
            "session_count": 0,
            "tool_calls": 0,
            "total_tokens": 0,
            "active": False,
            "_latest": None,
            "_node_id": None,
        })
        if branch:
            wt["_branches"].add(str(branch))
        wt["session_count"] += 1
        wt["tool_calls"] += int(tool_calls or 0)
        wt["total_tokens"] += int(total_tokens or 0)
        if ended_at is None:
            wt["active"] = True
        # A worktree is not itself a node; anchor its selectable node to
        # the newest session (fallback: the first session seen) so the rail
        # opens its most-recent agent_session.
        if wt["_node_id"] is None:
            wt["_node_id"] = str(node_id)
        stamp = last_modified_at or ended_at
        if stamp and (wt["_latest"] is None or stamp > wt["_latest"]):
            wt["_latest"] = stamp
            wt["_node_id"] = str(node_id)

    now = datetime.now(timezone.utc)
    worktrees: list[dict[str, Any]] = []
    for wt in by_cwd.values():
        worktrees.append({
            "label": wt["label"],
            "cwd_unavailable_reason": SERVER_PATH_NOT_SERVED,
            "branches": sorted(wt["_branches"]),
            "session_count": wt["session_count"],
            "tool_calls": wt["tool_calls"],
            "total_tokens": wt["total_tokens"],
            "active": wt["active"],
            "node_id": wt["_node_id"],
            "age": _humanize_age(wt["_latest"]),
            "idle_hours": _idle_hours(wt["_latest"], now),
            "_latest": wt["_latest"],
        })

    # Most-recently-active worktree first; None timestamps sort last.
    # key=(has_stamp, stamp) with reverse=True: real stamps (True) sort
    # before None (False); within real stamps, newest first.
    worktrees.sort(
        key=lambda w: (w["_latest"] is not None, w["_latest"]),
        reverse=True,
    )
    for w in worktrees:
        del w["_latest"]

    total = len(worktrees)
    active = sum(1 for w in worktrees if w["active"])
    shown = worktrees[:_WORKTREE_CAP]

    return {
        "deployment": deployment_name,
        "generation_id": generation_id,
        "total": total,
        "active": active,
        "truncated": total - len(shown),
        "worktrees": shown,
    }
