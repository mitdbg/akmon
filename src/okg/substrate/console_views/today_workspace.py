"""Direct composer for the `okg_workspace.today` view (v2).

Backs `req.contract-views.today-renderer-passes-contract` of the
`rebuild-console-with-view-contracts` PACT.

v2 (2026-05-28): reshape from "what PACTs am I working on" to "a
PI's morning briefing." Sections:

  1. awaiting_you      — PACT decisions + (future) drafts/replies
  2. research_threads  — projects (grouped from PACT prefixes)
  3. worth_knowing     — design_decision + self_play_finding
  4. reading           — PACT design.md + recent documents
  5. yesterday         — commits + landed PACTs + ended sessions
  6. overnight         — agent sessions ended overnight
  7. calendar          — meetings/deadlines today (HONEST EMPTY)
  8. workspace_health  — fires (HONEST EMPTY until okg doctor
                          is wired)

Reads the graph through the chronos IR checkout (okg.graph_nodes /
okg.graph_edges pinned to the request's generation), per
`migrate-console-reads-to-chronos-ir`. Recency comes from each node's own
timestamps. Request-time; a follow-up converts this to an AKMON-materialized
view.

The output shape previously mirrored the TypeScript contract that was archived
under `archive/ui/legacy-viz-2026-06-17/deployments/okg-workspace/supervisor_console/`
at `src/screens/today/contract.ts`; that tree is now only in git (commit
3253beb069ba).
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from okg.substrate.console_views._chronos import checkout
from okg.substrate.mcp.session import Session


def compose_today_workspace(
    conn: Any,
    *,
    session: Session,
    generated_at: str,
    operator_email: str | None = None,
    operator_display_name: str | None = None,
) -> dict[str, Any]:
    """Build the v2 Today payload from the substrate."""
    eyebrow = _format_eyebrow(generated_at)
    greeting = _format_greeting(generated_at)
    generation_id = session.generation_id
    graph = checkout(conn, session)

    operator_block: dict[str, Any] | None = None
    if operator_email and _git_author_exists(graph, operator_email):
        operator_block = {
            "person_id": f"person:{operator_email.lower()}",
            "display_name": operator_display_name or operator_email.split("@", 1)[0],
            "email": operator_email,
        }
    op_scoped = operator_block is not None
    op_id = operator_block["person_id"] if operator_block else None

    desk = _fetch_desk(graph)

    awaiting_items = _fetch_awaiting_you(graph)
    research_threads = _fetch_research_threads(
        graph, operator_email=(operator_email if op_scoped else None),
    )
    worth_knowing = _fetch_worth_knowing(graph)
    reading = _fetch_reading(graph)
    yesterday = _fetch_yesterday(
        graph, operator_email=(operator_email if op_scoped else None),
    )
    overnight = _fetch_overnight(graph)
    calendar_items: list[dict[str, Any]] = []  # see note in section 7
    fires_items: list[dict[str, Any]] = []     # see note in section 8

    return {
        "generation_id": generation_id,
        "eyebrow": eyebrow,
        "greeting": greeting,
        **({"operator": operator_block} if operator_block is not None else {}),
        **({"desk": desk} if desk is not None else {}),

        "awaiting_you": _section(
            op_scoped=False,
            op_id=None,
            empty_marker=(
                "0 PACT decisions · 0 drafts owed review · 0 replies pending. "
                "Drafts + replies aren't yet tracked by the substrate."
            ),
            items=awaiting_items,
        ),

        "research_threads": _section(
            op_scoped=op_scoped,
            op_id=op_id,
            empty_marker=(
                f"No active projects for {operator_email}."
                if op_scoped and operator_email is not None
                else "No active research threads in the workspace."
            ),
            items=research_threads,
        ),

        "worth_knowing": _section(
            op_scoped=False,
            op_id=None,
            empty_marker=(
                "No design_decision or self_play_finding rows in the substrate."
            ),
            items=worth_knowing,
        ),

        "reading": _section(
            op_scoped=False,
            op_id=None,
            empty_marker=(
                "No PACT design docs or research documents in the substrate."
            ),
            items=reading,
        ),

        "yesterday": _section(
            op_scoped=op_scoped,
            op_id=op_id,
            empty_marker=(
                f"No commits or landings authored by {operator_email} "
                "in the last 48 hours."
                if op_scoped and operator_email is not None
                else "No commits or landings in the workspace yesterday."
            ),
            items=yesterday,
        ),

        "overnight": _section(
            op_scoped=False,
            op_id=None,
            empty_marker="No overnight session activity in the workspace.",
            items=overnight,
        ),

        "calendar": _section(
            op_scoped=False,
            op_id=None,
            empty_marker=(
                "Calendar wiring not yet implemented — no .ics ingestion "
                "source on this deployment. Substrate gap, not an empty day."
            ),
            items=calendar_items,
        ),

        "workspace_health": _section(
            op_scoped=False,
            op_id=None,
            empty_marker=(
                "Fires section not wired to `okg doctor` yet — substrate "
                "follow-up. Workspace may or may not be healthy."
            ),
            items=fires_items,
        ),
    }


def _section(
    *,
    op_scoped: bool,
    op_id: str | None,
    empty_marker: str,
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "operator_scoped": op_scoped,
        "empty_marker": empty_marker,
        "items": items,
    }
    if op_id is not None:
        out["operator_id"] = op_id
    return out


# ---------------------------------------------------------------
# Eyebrow + greeting
# ---------------------------------------------------------------


def _format_eyebrow(generated_at: str) -> str:
    dt = _parse_iso(generated_at)
    return f"{dt.strftime('%A')}, {dt.day} {dt.strftime('%B %Y')}"


def _format_greeting(generated_at: str) -> str:
    dt = _parse_iso(generated_at).astimezone()
    hour = dt.hour
    if hour < 5:
        return "Working late"
    if hour < 12:
        return "Good morning"
    if hour < 17:
        return "Good afternoon"
    if hour < 22:
        return "Good evening"
    return "Late night"


def _parse_iso(iso_str: str) -> datetime:
    s = iso_str.replace("Z", "+00:00") if iso_str.endswith("Z") else iso_str
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return datetime.now(timezone.utc)


# ---------------------------------------------------------------
# Operator existence check
# ---------------------------------------------------------------

_GIT_AUTHOR_EXISTS_SQL = """
SELECT 1 AS ok
FROM okg.graph_nodes
WHERE subtype = 'git_author'
  AND LOWER(attrs->>'email') = LOWER(:email)
LIMIT 1
"""


def _git_author_exists(graph: Any, email: str) -> bool:
    return bool(graph.query(_GIT_AUTHOR_EXISTS_SQL, {"email": email}))


# ---------------------------------------------------------------
# DESK
# ---------------------------------------------------------------

_DESK_SQL = """
SELECT
  COALESCE(attrs->>'session_id', node_id) AS session_id,
  NULLIF(attrs->>'git_branch', '')          AS branch,
  COALESCE(NULLIF(attrs->>'denial_count','')::int, 0) AS denial_count,
  NULLIF(attrs->>'last_modified_at','')::timestamptz AS last_modified_at
FROM okg.graph_nodes
WHERE subtype = 'agent_session'
  AND NULLIF(attrs->>'last_modified_at','') IS NOT NULL
ORDER BY NULLIF(attrs->>'last_modified_at','')::timestamptz DESC
LIMIT 1
"""


def _fetch_desk(graph: Any) -> dict[str, Any] | None:
    rows = graph.query(_DESK_SQL)
    if not rows:
        return None
    r = rows[0]
    session_id = r["session_id"]
    if not session_id:
        return None
    return {
        "last_session_age": _humanize_age(r["last_modified_at"]),
        "branch": r["branch"],
        "refusal_count": int(r["denial_count"]),
        "session_id": str(session_id),
    }


# ---------------------------------------------------------------
# Section 1: Awaiting you
# ---------------------------------------------------------------

_AWAITING_PACT_SQL = """
SELECT
  attrs->>'change_id'  AS change_id,
  attrs->>'status'     AS lifecycle,
  COALESCE(NULLIF(attrs->>'blocked_task_count','')::int, 0) AS blocked,
  NULLIF(attrs->>'last_modified_at','')::timestamptz AS last_modified_at,
  observed_at AS observed_at
FROM okg.graph_nodes
WHERE subtype = 'pact_change'
  AND (
    attrs->>'status' IN ('proposal', 'changes_requested', 'intent_review', 'design_review')
    OR COALESCE(NULLIF(attrs->>'blocked_task_count','')::int, 0) > 0
  )
ORDER BY
  COALESCE(NULLIF(attrs->>'last_modified_at','')::timestamptz,
           observed_at) DESC NULLS LAST
LIMIT 10
"""


def _fetch_awaiting_you(graph: Any) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for r in graph.query(_AWAITING_PACT_SQL):
        change_id = r["change_id"]
        if not change_id:
            continue
        items.append({
            "kind": "pact_decision",
            "change_id": str(change_id),
            "lifecycle": str(r["lifecycle"]) if r["lifecycle"] else "unknown",
            "age": _humanize_age(r["last_modified_at"] or r["observed_at"]),
            "tone": "bad" if int(r["blocked"] or 0) > 0 else "default",
        })
    return items


# ---------------------------------------------------------------
# Section 2: Research threads
#
# Project derivation: parse each pact_change.change_id to extract a
# project namespace. Heuristic: drop a leading verb (add / define /
# implement / harden / quiet / introduce / etc.) and take the next
# 2-3 words. Sessions on a worktree branch are joined to the project
# whose change_id prefix matches the worktree name.
# ---------------------------------------------------------------

# Tokens that should render in uppercase when humanizing a slug.
# Otherwise Python `str.capitalize` produces "Mcp" / "Okg" / "Iam"
# which read as misspellings rather than acronyms.
_UPPERCASE_TOKENS = {
    "mcp", "okg", "sql", "iam", "dsg", "ci", "cd", "api", "url",
    "uri", "json", "yaml", "xml", "ssh", "tls", "ics", "pdf",
    "pi", "ai", "ml", "io", "db", "ui", "ux", "rfc", "id",
    "kpi", "mit", "ssl", "dns", "rest", "rpc", "ipc", "tla",
    "ast", "rag", "llm", "v0", "v1", "v2", "v3", "akmon",
    "pact", "openspec", "fsm", "ttl", "qa",
}


def _humanize_slug(slug: str) -> str:
    """Convert a kebab/snake slug to a display string, preserving
    common acronyms in uppercase.

    Examples:
      mcp-tool-surface-friction → "MCP Tool Surface Friction"
      cold-start-ontology-discovery → "Cold Start Ontology Discovery"
      akmon-3-mockup-match → "AKMON 3 Mockup Match"
    """
    parts = re.split(r"[-_]", slug.strip())
    out: list[str] = []
    for p in parts:
        if not p:
            continue
        if p.lower() in _UPPERCASE_TOKENS:
            out.append(p.upper())
        elif p.isdigit():
            out.append(p)
        else:
            out.append(p.capitalize())
    return " ".join(out)


_LEADING_VERBS = {
    "add", "define", "implement", "harden", "quiet", "introduce",
    "cleanup", "close", "dogfood", "expose", "improve", "normalize",
    "operationalize", "refactor", "retire", "separate", "stabilize",
    "rebuild", "redesign", "anchor", "back", "adopt", "graph",
}


def _project_id_for_change_id(change_id: str) -> tuple[str, str]:
    """Return (project_id, project_name) for a change_id.

    Project_id is a stable slug; project_name is human-friendly
    (acronym-preserving)."""
    parts = re.split(r"[-_]", change_id.lower())
    if parts and parts[0] in _LEADING_VERBS:
        parts = parts[1:]
    if not parts:
        return ("other", "Other")
    slug_parts = parts[:3]
    project_id = "-".join(slug_parts)
    project_name = _humanize_slug("-".join(slug_parts))
    return (project_id, project_name)


_RESEARCH_PACT_SQL = """
SELECT
  attrs->>'change_id'  AS change_id,
  attrs->>'status'     AS lifecycle,
  -- Fall back to the node's observed_at when last_modified_at is
  -- absent. Without this, every thread shows latest_activity='unknown'
  -- because pact_change rows currently don't carry a last_modified_at.
  COALESCE(
    NULLIF(attrs->>'last_modified_at','')::timestamptz,
    observed_at
  ) AS observed_at
FROM okg.graph_nodes
WHERE subtype = 'pact_change'
  AND attrs->>'status' NOT IN ('done', 'archived')
"""


def _fetch_research_threads(
    graph: Any,
    *,
    operator_email: str | None,
) -> list[dict[str, Any]]:
    """Aggregate pact_change + (optionally operator-scoped) commits
    into per-project rows."""
    threads: dict[str, dict[str, Any]] = {}

    # Add PACT counts per project
    for r in graph.query(_RESEARCH_PACT_SQL):
        change_id = r["change_id"]
        if not change_id:
            continue
        lm = r["observed_at"]
        pid, pname = _project_id_for_change_id(str(change_id))
        t = threads.setdefault(pid, {
            "project_id": pid,
            "project_name": pname,
            "active_pact_count": 0,
            "commits_this_week": 0,
            "sessions_today": 0,
            "_latest": lm,
        })
        t["active_pact_count"] += 1
        if lm and (t["_latest"] is None or lm > t["_latest"]):
            t["_latest"] = lm

    # Commit-to-project attribution intentionally NOT done here.
    # Commit subjects are noisy and subject-based matching produces
    # a huge "Other commits" noise bucket. Substrate-pure path: an
    # `authored_for_project` edge from git_commit to pact_change
    # (or to a higher-level Project node). Until that lands,
    # commits_this_week stays at 0; the renderer surfaces this
    # honestly. Yesterday's commits are still rendered there.
    _ = operator_email  # currently unused; reserved for future use

    # Convert to list ordered by latest_activity desc.
    rows = list(threads.values())
    rows.sort(
        key=lambda t: t["_latest"] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    out: list[dict[str, Any]] = []
    for t in rows[:6]:  # top 6 threads
        out.append({
            "project_id": t["project_id"],
            "project_name": t["project_name"],
            "active_pact_count": t["active_pact_count"],
            "commits_this_week": t["commits_this_week"],
            "sessions_today": t["sessions_today"],
            "latest_activity": _humanize_age(t["_latest"]),
        })
    return out


# ---------------------------------------------------------------
# Section 3: Worth knowing
# ---------------------------------------------------------------

_WORTH_KNOWING_SQL = """
SELECT * FROM (
  SELECT
    node_id,
    'decision' AS kind,
    attrs->>'title' AS title_or_slug,
    attrs->>'source_ref' AS source_ref,
    FALSE AS humanize,
    COALESCE(
      NULLIF(attrs->>'last_modified_at','')::timestamptz,
      observed_at
    ) AS observed_at
  FROM okg.graph_nodes
  WHERE subtype = 'design_decision'
    AND attrs->>'title' IS NOT NULL
    AND attrs->>'title' <> ''

  UNION ALL

  SELECT
    node_id,
    'finding' AS kind,
    -- self_play_finding titles are empty in this substrate; emit
    -- the slug and let Python humanize_slug() preserve acronyms
    -- (MCP / OKG / SQL etc.) properly. SQL's INITCAP would render
    -- those as Mcp / Okg / Sql.
    split_part(node_id, ':', 3) AS title_or_slug,
    NULL::text AS source_ref,
    TRUE AS humanize,
    COALESCE(
      NULLIF(attrs->>'last_modified_at','')::timestamptz,
      observed_at
    ) AS observed_at
  FROM okg.graph_nodes
  WHERE subtype = 'self_play_finding'
) recent
ORDER BY observed_at DESC NULLS LAST
LIMIT 30
"""


def _fetch_worth_knowing(graph: Any) -> list[dict[str, Any]]:
    # Pull more than we need (LIMIT 30) so the dedup can keep the
    # most-recent row per title without dropping below the display
    # cap. After dedup we trim to 12 for the UI.
    items: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    for r in graph.query(_WORTH_KNOWING_SQL):
        title_or_slug = r["title_or_slug"]
        if not title_or_slug:
            continue
        title = (
            _humanize_slug(str(title_or_slug))
            if r["humanize"]
            else str(title_or_slug)
        )
        title_key = title.lower().strip()
        if title_key in seen_titles:
            continue
        seen_titles.add(title_key)
        entry: dict[str, Any] = {
            "id": str(r["node_id"]),
            "kind": str(r["kind"]),
            "title": title,
            "age": _humanize_age(r["observed_at"]),
        }
        if r["source_ref"]:
            entry["source_ref"] = str(r["source_ref"])
        items.append(entry)
        if len(items) >= 12:
            break
    return items


# ---------------------------------------------------------------
# Section 4: Reading
# ---------------------------------------------------------------

_READING_SQL = """
SELECT
  attrs->>'path' AS path,
  attrs->>'title' AS title_attr,
  CASE
    WHEN attrs->>'path' LIKE 'pact/changes/%/design.md'
      OR attrs->>'path' LIKE 'pact/changes/%/pact.md' THEN 'pact_design'
    ELSE 'document'
  END AS kind,
  NULLIF(attrs->>'parsed_at','')::timestamptz AS parsed_at
FROM okg.graph_nodes
WHERE subtype = 'document'
  AND (
    attrs->>'path' LIKE 'pact/changes/%/design.md'
    OR attrs->>'path' LIKE 'pact/changes/%/pact.md'
    OR attrs->>'path' LIKE 'docs/%.md'
  )
ORDER BY parsed_at DESC NULLS LAST
LIMIT 12
"""
# Frozen OpenSpec proposals are deliberately absent (decision D10,
# 2026-09-26, PACT layout-history-openspec): the tree is read-only history
# under history/openspec/, so it is not reading material for today. The
# proposals stay searchable in the graph.


_PACT_PATH_RE = re.compile(r"^pact/changes/([^/]+)/(?:design|pact)\.md$")


def _reading_display_title(path: str, kind: str, title_attr: str | None) -> str:
    """Pick the line a reader actually wants to see.

    For PACT design docs the change_id is the right identifier —
    `pact/changes/<change-id>/design.md` collapses to
    `<change-id> · design.md`. For other docs, the basename is more
    useful than the full path (the parent dir goes in the subline)."""
    if kind == "pact_design":
        m = _PACT_PATH_RE.match(path)
        if m:
            filename = path.rsplit("/", 1)[-1]
            return f"{m.group(1)} · {filename}"
    # If the title attr is set AND it isn't just the path repeated,
    # use it. Otherwise fall back to the basename — readers want the
    # filename, not the directory tree.
    if title_attr and title_attr != path:
        return title_attr
    return path.rsplit("/", 1)[-1] or path


def _reading_parent(path: str) -> str:
    """Return the parent directory of a path, or '' for top-level."""
    parts = path.rsplit("/", 1)
    return parts[0] + "/" if len(parts) == 2 else ""


def _fetch_reading(graph: Any) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for r in graph.query(_READING_SQL):
        path = r["path"]
        if not path:
            continue
        title = _reading_display_title(str(path), str(r["kind"]), r["title_attr"])
        items.append({
            "title": title,
            "path": str(path),
            "kind": str(r["kind"]),
            "age": _humanize_age(r["parsed_at"]),
        })
    return items


# ---------------------------------------------------------------
# Section 5: Yesterday — commits + landed PACTs + ended sessions
# ---------------------------------------------------------------

_YESTERDAY_COMMITS_OP_SQL = """
SELECT DISTINCT
  n.node_id,
  n.attrs->>'commit_sha' AS sha,
  COALESCE(n.attrs->>'message_subject', n.attrs->>'subject', n.attrs->>'label') AS subject,
  COALESCE(n.attrs->>'repo', 'okg') AS repo,
  NULLIF(n.attrs->>'authored_at','')::timestamptz AS authored_at
FROM okg.graph_nodes n
JOIN okg.graph_edges e
  ON e.dst = n.node_id AND e.edge_type = 'assigned_to'
JOIN okg.graph_nodes a
  ON a.node_id = e.src AND a.subtype = 'git_author'
 AND LOWER(a.attrs->>'email') = LOWER(:email)
WHERE n.subtype = 'git_commit'
  AND NULLIF(n.attrs->>'authored_at','')::timestamptz > now() - interval '48 hours'
ORDER BY authored_at DESC
LIMIT 20
"""

_YESTERDAY_COMMITS_WS_SQL = """
SELECT DISTINCT
  node_id,
  attrs->>'commit_sha' AS sha,
  COALESCE(attrs->>'message_subject', attrs->>'subject', attrs->>'label') AS subject,
  COALESCE(attrs->>'repo', 'okg') AS repo,
  NULLIF(attrs->>'authored_at','')::timestamptz AS authored_at
FROM okg.graph_nodes
WHERE subtype = 'git_commit'
  AND NULLIF(attrs->>'authored_at','')::timestamptz > now() - interval '48 hours'
ORDER BY authored_at DESC
LIMIT 20
"""


def _fetch_yesterday(
    graph: Any,
    *,
    operator_email: str | None,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    if operator_email:
        rows = graph.query(_YESTERDAY_COMMITS_OP_SQL, {"email": operator_email})
    else:
        rows = graph.query(_YESTERDAY_COMMITS_WS_SQL)
    for r in rows:
        sha = r["sha"]
        if not sha:
            continue
        items.append({
            "kind": "commit",
            "short_sha": str(sha)[:7],
            "subject": (r["subject"] or "(no subject)").split("\n", 1)[0],
            "repo": str(r["repo"]),
            "age": _humanize_age(r["authored_at"]),
        })
    return items


# ---------------------------------------------------------------
# Section 6: Overnight
# ---------------------------------------------------------------

_OVERNIGHT_SQL = """
SELECT
  attrs->>'git_branch'             AS branch,
  attrs->>'agent_nickname'         AS nickname,
  COALESCE(NULLIF(attrs->>'denial_count','')::int, 0) AS denial_count,
  NULLIF(attrs->>'ended_at','')::timestamptz   AS ended_at
FROM okg.graph_nodes
WHERE subtype = 'agent_session'
  AND NULLIF(attrs->>'ended_at','')::timestamptz > now() - interval '24 hours'
ORDER BY NULLIF(attrs->>'ended_at','')::timestamptz DESC
LIMIT 20
"""


def _fetch_overnight(graph: Any) -> list[dict[str, Any]]:
    by_branch: dict[str, dict[str, Any]] = {}
    for r in graph.query(_OVERNIGHT_SQL):
        branch, nickname = r["branch"], r["nickname"]
        denial_count, ended_at = r["denial_count"], r["ended_at"]
        # Skip rows where branch is empty/missing — those are
        # sessions that ran outside any git context (cluster
        # one-shots, detached HEAD work). They render as
        # "<no-branch>" which is ugly and uninformative.
        if not branch:
            continue
        key = str(branch)
        entry = by_branch.setdefault(key, {
            "branch": key,
            "nicknames": [],
            "session_count": 0,
            "denial_count": 0,
            "_latest_ended_at": ended_at,
        })
        # Skip the generic "agent" nickname — the agent-sessions
        # source emits it as a fallback when the JSONL doesn't
        # name the sub-agent. It conveys no information.
        if nickname and nickname != "agent" and nickname not in entry["nicknames"]:
            entry["nicknames"].append(str(nickname))
        entry["session_count"] += 1
        entry["denial_count"] += int(denial_count or 0)
        if ended_at and (
            entry["_latest_ended_at"] is None
            or ended_at > entry["_latest_ended_at"]
        ):
            entry["_latest_ended_at"] = ended_at

    items: list[dict[str, Any]] = []
    for entry in by_branch.values():
        items.append({
            "branch": entry["branch"],
            # If only the generic "agent" nickname was present, emit
            # an empty list — Today.tsx will simply say "N sessions"
            # without trailing nicknames.
            "nicknames": entry["nicknames"],
            "session_count": entry["session_count"],
            "denial_count": entry["denial_count"],
            "age": _humanize_age(entry["_latest_ended_at"]),
        })
    items.sort(key=lambda i: i["age"])
    return items


# ---------------------------------------------------------------
# Age formatting
# ---------------------------------------------------------------


def _humanize_age(when: datetime | None) -> str:
    if when is None:
        return "unknown"
    now = datetime.now(timezone.utc)
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    delta = now - when
    seconds = int(delta.total_seconds())
    if seconds < 0:
        return "just now"
    if seconds < 60:
        return f"{seconds}s ago"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h ago"
    days = hours // 24
    if days < 7:
        return f"{days}d ago"
    try:
        return when.strftime("%-d %b %Y")
    except Exception:  # noqa: BLE001
        return "long ago"
