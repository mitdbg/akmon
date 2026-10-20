"""Chronos-backed composer for the `okg_workspace.knowledge` view.

Three sections served from substrate state:

  1. Overview — total nodes/edges/generations + top-N subtype counts
  2. Map      — an arbitrary subgraph rendered around a focus node;
                composer computes a radial layout server-side and
                ships positions to the client
  3. Sources  — active source registry + last_run / enqueue / error
                state from okg.source_progress

The graph sections read okg.graph_nodes / okg.graph_edges through the pinned
Chronos checkout carried by the viz request Session. Source liveness still
comes from okg.source_progress; the configured sources come from the
deployment source registry through `console_views.producer_identity`, the one
rule the console uses to name a producer.
"""

from __future__ import annotations

import json
import logging
import math
from okg.substrate.searchable_text import without_producer_text
from datetime import datetime, timezone
from typing import Any

from okg.substrate.console_views._chronos import (
    GenerationCountsUnavailable,
    checkout,
    generation_counts,
    generation_published_at,
)
from okg.substrate.console_views.console_count_rollup_read import (
    COUNT_KIND_EDGE_SOURCE,
    COUNT_KIND_SOURCE,
    console_count_rollup_enabled,
    read_console_count_subtype_rows,
    require_console_count_rollup_enabled,
    rollup_reader,
)
from okg.substrate.console_views.producer_identity import (
    ProducerIdentity,
    is_unlisted_alias_key,
    registry_identities,
    runtime_key_index,
)
from okg.substrate.console_views.today_workspace import (
    _humanize_age,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.mcp.session import Session


logger = logging.getLogger(__name__)

# The overview reads a generation COUNT and no checkpoint rows, so it projects
# no metadata key. Declared so the projection guard fails the day it starts
# reading one (issue #2113).
OVERVIEW_METADATA_KEYS: tuple[str, ...] = ()


def _generation_total(store: Any, *, deployment_name: str, branch: str) -> int:
    """How many generation attempts the branch holds, counted exactly.

    It used to be `len(store.list_generations(status=None))`, which moved
    every checkpoint's full metadata -- 131 MB on okg-workspace -- to take a
    length (issue #2113). `count_generations` returns that same number in one
    statement. A backend that cannot count takes the NAMED full fetch below,
    logged, rather than a zero or a guess.
    """
    total = store.count_generations(deployment_name=deployment_name, branch=branch)
    if total is not None:
        return int(total)
    return _generation_total_full_scan(
        store, deployment_name=deployment_name, branch=branch,
    )


def _generation_total_full_scan(
    store: Any, *, deployment_name: str, branch: str,
) -> int:
    """Named fallback: count by the full-metadata fetch."""
    logger.warning(
        "overview_generation_total_full_scan: this backend cannot count "
        "generations in one statement; reading branch %r's whole history",
        branch,
    )
    return len(store.list_generations(
        deployment_name=deployment_name, branch=branch, status=None,
    ))


def _payload_generation_id(session: Session) -> int | str:
    """Generation id exposed inside the v2 view payload.

    The envelope uses the Chronos checkpoint id. Existing v2 frontend schemas
    still validate payload.generation_id as the publisher's numeric bridge id,
    so keep that value when the Session carries it.
    """
    return (
        session.compat_generation_id
        if session.compat_generation_id is not None
        else session.generation_id
    )


def compose_knowledge(
    conn: Any,
    *,
    session: Session,
    generated_at: str,
    deployment_name: str,
    focus_id: str | None = None,
) -> dict[str, Any]:
    """Three-section composer retained for callers that still want
    the historical aggregate knowledge payload."""
    graph = checkout(conn, session)
    overview = _fetch_overview(conn, graph, session)
    map_section = _fetch_map(graph, focus_id=focus_id)
    sources = _fetch_sources(
        conn, deployment_name, generation_id=session.generation_id,
    )

    return {
        "generation_id": _payload_generation_id(session),
        "generated_at": generated_at,
        "overview": overview,
        "map": map_section,
        "sources": sources,
    }


# ---------------------------------------------------------------
# Dedicated per-page composers — one section's data each
# ---------------------------------------------------------------


def compose_overview(
    conn: Any, *, session: Session, generated_at: str,
) -> dict[str, Any]:
    """Page payload for the Overview page (substrate stats +
    subtype distribution)."""
    graph = checkout(conn, session)
    return {
        "generation_id": _payload_generation_id(session),
        "generated_at": generated_at,
        "overview": _fetch_overview(conn, graph, session),
    }


def compose_map(
    conn: Any,
    *,
    session: Session,
    generated_at: str,
    focus_id: str | None = None,
) -> dict[str, Any]:
    """Page payload for the Map page (arbitrary subgraph centered
    on a focus node, server-side radial layout)."""
    graph = checkout(conn, session)
    return {
        "generation_id": _payload_generation_id(session),
        "generated_at": generated_at,
        "map": _fetch_map(graph, focus_id=focus_id),
    }


def compose_sources(
    conn: Any,
    *,
    session: Session,
    generated_at: str,
    deployment_name: str,
) -> dict[str, Any]:
    """Page payload for the Sources page (configured sources +
    last_run / enqueue / error state).

    Raises `ConsoleSourceRegistryUnreadable` when the deployment's source
    registry exists and cannot be read.
    """
    checkout(conn, session)
    return {
        "generation_id": _payload_generation_id(session),
        "generated_at": generated_at,
        "sources": _fetch_sources(
            conn, deployment_name, generation_id=session.generation_id,
        ),
    }


# ---------------------------------------------------------------
# Section 1: Overview
# ---------------------------------------------------------------

def _fetch_overview(conn: Any, graph: Any, session: Session) -> dict[str, Any]:
    # console-count-rollup (#444 slice 2): flag-ON, read the totals O(1) from the
    # pinned generation's stamped checkpoint metadata; flag-OFF (or an unstamped /
    # non-published gen) falls through to the raw count(*) -- never a default zero.
    total_nodes: int | None = None
    total_edges: int | None = None
    if console_count_rollup_enabled():
        try:
            total_nodes, total_edges = generation_counts(conn, session)
        except GenerationCountsUnavailable:
            total_nodes = total_edges = None
    if total_nodes is None or total_edges is None:
        total_nodes = int(
            # derivation-read-lint: allow #444 stamped-count fallback only.
            graph.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"] or 0
        )
        total_edges = int(
            # derivation-read-lint: allow #444 stamped-count fallback only.
            graph.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"] or 0
        )
    nodes_live = total_nodes
    edges_live = total_edges
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    gens_total = _generation_total(
        store,
        deployment_name=session.deployment_name,
        branch=session.branch_name,
    )
    published_at = generation_published_at(conn, session)
    latest_age = _humanize_age(published_at) if published_at else None
    # The by-subtype top-15. Flag-ON reads the maintained by-subtype rollup and
    # owns the sort+slice here (the read returns rows unordered/unlimited): the
    # secondary `subtype ASC` tiebreak is MANDATORY or ties at the 15/16 boundary
    # diverge from the SQL path. The flag-OFF SQL pins `subtype COLLATE "C"` so its
    # ORDER BY matches Python's codepoint sort on any deployment (byte-identical on
    # the C-collation deployments shipping today). A missing rollup fails LOUD
    # (ConsoleCountRollupUnavailable propagates -- this one panel, never silent []).
    if console_count_rollup_enabled():
        raw = read_console_count_subtype_rows(
            conn, generation_id=session.generation_id
        )
        raw.sort(key=lambda r: (-int(r["c"]), str(r["subtype"])))
        dist = [
            {"subtype": str(r["subtype"]), "count": int(r["c"])}
            for r in raw[:15]
        ]
    else:
        dist = [
            {"subtype": str(row["subtype"]), "count": int(row["n"])}
            for row in graph.query(
                # derivation-read-lint: allow #444 flag-off subtype rollup fallback.
                """
                SELECT subtype, count(*) AS n
                  FROM okg.graph_nodes
              GROUP BY subtype
              ORDER BY n DESC, subtype COLLATE "C"
                 LIMIT 15
                """
            )
        ]
    return {
        "total_nodes": nodes_live,
        "total_edges": edges_live,
        "total_generations": int(gens_total),
        "latest_generation_id": _payload_generation_id(session),
        **({"latest_generation_age": latest_age} if latest_age else {}),
        "subtype_distribution": dist,
    }


# ---------------------------------------------------------------
# Section 2: Map — arbitrary subgraph renderer
# ---------------------------------------------------------------

# Cap on rendered nodes; an SVG with more than ~25 nodes around a
# focus reads as visual noise. Higher-degree neighbors win.
_MAP_NODE_CAP = 24


def _resolve_focus(graph: Any, focus_id: str | None) -> str | None:
    """Pick a focus node id. If the caller passed one and it exists,
    use it; otherwise pick a default that yields a RICH subgraph —
    the highest-degree node among the 30 most-recent git_commits.
    A commit links to its author, parent commits, and the source
    files it touched, so it renders a varied, non-empty radial.
    Falls back to most-recent pact_change, then any node."""
    if focus_id:
        rows = graph.query(
            "SELECT 1 AS ok FROM okg.graph_nodes WHERE node_id = :focus LIMIT 1",
            {"focus": focus_id},
        )
        if rows:
            return focus_id

    # Highest-degree among recent commits — bounded candidate set so
    # the per-candidate degree count stays cheap.
    rows = graph.query(
        # derivation-read-lint: allow #444 bounded focus-pick degree helper.
        """
        WITH recent_commits AS (
          SELECT node_id
            FROM okg.graph_nodes
           WHERE subtype = 'git_commit'
        ORDER BY COALESCE(
                   attrs->>'committed_at',
                   attrs->>'author_date',
                   attrs->>'date',
                   attrs->>'observed_at',
                   node_id
                 ) DESC,
                 node_id
           LIMIT 30
        )
        SELECT rc.node_id, count(e.edge_id) AS deg
          FROM recent_commits rc
     LEFT JOIN okg.graph_edges e
            ON e.src = rc.node_id OR e.dst = rc.node_id
      GROUP BY rc.node_id
      ORDER BY deg DESC, rc.node_id
         LIMIT 1
        """
    )
    if rows and rows[0].get("node_id"):
        return str(rows[0]["node_id"])

    # Fallbacks.
    rows = graph.query(
        """
        SELECT node_id
          FROM okg.graph_nodes
         WHERE subtype = 'pact_change'
      ORDER BY COALESCE(
                 attrs->>'updated_at',
                 attrs->>'created_at',
                 attrs->>'change_id',
                 node_id
               ) DESC,
               node_id
         LIMIT 1
        """
    )
    if rows:
        return str(rows[0]["node_id"])

    rows = graph.query(
        """
        SELECT node_id
          FROM okg.graph_nodes
      ORDER BY node_id
         LIMIT 1
        """
    )
    if rows:
        return str(rows[0]["node_id"])
    return None


def _fetch_node(graph: Any, node_id: str) -> dict[str, Any] | None:
    rows = graph.query(
        """
        SELECT node_id, subtype, attrs
          FROM okg.graph_nodes
         WHERE node_id = :node_id
         LIMIT 1
        """,
        {"node_id": node_id},
    )
    if not rows:
        return None
    row = rows[0]
    nid = str(row["node_id"])
    subtype = str(row["subtype"])
    attrs = without_producer_text(row.get("attrs") or {})
    label = _label_for(nid, subtype, attrs or {})
    return {"id": nid, "subtype": subtype, "label": label, "attrs": attrs}


def _label_for(node_id: str, subtype: str, attrs: dict[str, Any]) -> str:
    """Pick a readable one-line label for a node."""
    for key in ("title", "change_id", "label", "message_subject", "name", "path"):
        v = attrs.get(key)
        if v:
            s = str(v).split("\n", 1)[0].strip()
            if len(s) > 60:
                s = s[:59] + "…"
            if s:
                return s
    # Fall back to a shortened node_id.
    if ":" in node_id:
        tail = node_id.split(":", 1)[1]
        return tail[:60]
    return node_id[:60]


def _fetch_neighbors(
    graph: Any, focus_id: str, cap: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return (neighbor_nodes, edges_between_focus_and_them).

    Neighbors are nodes 1-hop from the focus in either direction.
    Sorted by degree (sum of incoming + outgoing edges in
    graph_edges) so the most-connected nodes win when we exceed
    the cap."""
    rows = graph.query(
        # derivation-read-lint: allow #444 bounded neighbor ranking helper.
        """
        WITH out_edges AS (
          SELECT e.dst AS neighbor_id, e.edge_type, 'out' AS dir
            FROM okg.graph_edges e
           WHERE e.src = :focus
        ),
        in_edges AS (
          SELECT e.src AS neighbor_id, e.edge_type, 'in' AS dir
            FROM okg.graph_edges e
           WHERE e.dst = :focus
        ),
        combined AS (
          SELECT * FROM out_edges UNION ALL SELECT * FROM in_edges
        ),
        neighbor_ids AS (
          SELECT DISTINCT neighbor_id
            FROM combined
           WHERE neighbor_id <> :focus
        ),
        degrees AS (
          SELECT node_id, count(*) AS degree
            FROM (
              SELECT src AS node_id FROM okg.graph_edges
              UNION ALL
              SELECT dst AS node_id FROM okg.graph_edges
            ) d
        GROUP BY node_id
        ),
        ranked_neighbors AS (
          SELECT
            ni.neighbor_id,
            n.subtype,
            n.attrs,
            COALESCE(d.degree, 0) AS neighbor_degree
          FROM neighbor_ids ni
          JOIN okg.graph_nodes n
            ON n.node_id = ni.neighbor_id
     LEFT JOIN degrees d
            ON d.node_id = ni.neighbor_id
      ORDER BY COALESCE(d.degree, 0) DESC, ni.neighbor_id
         LIMIT :cap
        )
        SELECT
          c.neighbor_id, c.edge_type, c.dir,
          rn.subtype, rn.attrs, rn.neighbor_degree
        FROM combined c
        JOIN ranked_neighbors rn
          ON rn.neighbor_id = c.neighbor_id
        ORDER BY rn.neighbor_degree DESC, c.neighbor_id
        """,
        {"focus": focus_id, "cap": cap},
    )

    seen: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []
    for row in rows:
        neighbor_id = str(row["neighbor_id"])
        edge_type = str(row["edge_type"])
        direction = str(row["dir"])
        subtype = str(row["subtype"])
        attrs = row.get("attrs") or {}
        deg = row.get("neighbor_degree") or 0
        if neighbor_id == focus_id:
            continue
        if neighbor_id not in seen and len(seen) >= cap:
            continue
        if neighbor_id not in seen:
            seen[neighbor_id] = {
                "id": neighbor_id,
                "subtype": subtype,
                "label": _label_for(neighbor_id, subtype, attrs or {}),
                "degree": int(deg or 0),
            }
        # Edge orientation: out = focus → neighbor; in = neighbor → focus.
        if direction == "out":
            edges.append({
                "src_id": focus_id,
                "dst_id": neighbor_id,
                "edge_type": edge_type,
            })
        else:
            edges.append({
                "src_id": neighbor_id,
                "dst_id": focus_id,
                "edge_type": edge_type,
            })
    return list(seen.values()), edges


def _layout_radial(
    focus: dict[str, Any], neighbors: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Radial layout: focus at (50, 50), neighbors arranged on a
    circle of radius 38 around it. Coordinates normalized to a
    100×100 viewBox; the client's SVG scales them up."""
    positioned: list[dict[str, Any]] = []
    focus_degree = int(focus.get("degree") or 0)
    positioned.append({
        "id": focus["id"],
        "subtype": focus["subtype"],
        "label": focus["label"],
        "x": 50.0,
        "y": 50.0,
        "is_focus": True,
        "degree": focus_degree,
    })
    n = len(neighbors)
    if n == 0:
        return positioned
    radius = 38.0
    # Choose angles. For n >= 3, evenly distribute around the full
    # circle starting at the top. For n == 1 or 2, hand-place on an
    # upper arc so they never sit on a vertical diameter through the
    # focus (which read as an empty "line" in the old layout).
    if n == 1:
        angles = [-math.pi / 2]  # straight up
    elif n == 2:
        angles = [math.radians(-130), math.radians(-50)]  # upper arc
    else:
        angles = [
            -math.pi / 2 + (2 * math.pi * i / n) for i in range(n)
        ]
    for i, neighbor in enumerate(neighbors):
        angle = angles[i]
        x = 50.0 + radius * math.cos(angle)
        y = 50.0 + radius * math.sin(angle)
        positioned.append({
            "id": neighbor["id"],
            "subtype": neighbor["subtype"],
            "label": neighbor["label"],
            "x": round(x, 2),
            "y": round(y, 2),
            "is_focus": False,
            "degree": int(neighbor["degree"]),
        })
    return positioned


def _fetch_suggested_pivots(graph: Any) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    queries = [
        """
        SELECT node_id, subtype, attrs->>'title' AS title,
               attrs->>'change_id' AS change_id
          FROM okg.graph_nodes
         WHERE subtype = 'pact_change'
           AND attrs->>'status' IN ('proposal', 'implementation', 'verification')
      ORDER BY COALESCE(
                 attrs->>'updated_at',
                 attrs->>'created_at',
                 attrs->>'change_id',
                 node_id
               ) DESC,
               node_id
         LIMIT 3
        """,
        """
        SELECT node_id, subtype, attrs->>'title' AS title,
               NULL AS change_id
          FROM okg.graph_nodes
         WHERE subtype = 'design_decision'
           AND attrs->>'title' IS NOT NULL
      ORDER BY COALESCE(
                 attrs->>'updated_at',
                 attrs->>'created_at',
                 attrs->>'title',
                 node_id
               ) DESC,
               node_id
         LIMIT 2
        """,
        """
        SELECT node_id, subtype, NULLIF(attrs->>'message_subject', '') AS title,
               NULL AS change_id
          FROM okg.graph_nodes
         WHERE subtype = 'git_commit'
      ORDER BY COALESCE(
                 attrs->>'committed_at',
                 attrs->>'author_date',
                 attrs->>'date',
                 node_id
               ) DESC,
               node_id
         LIMIT 2
        """,
    ]
    for sql in queries:
        for row in graph.query(sql):
            node_id = str(row["node_id"])
            label = row.get("title") or row.get("change_id") or node_id.split(":", 1)[-1]
            items.append({
                "id": node_id,
                "subtype": str(row["subtype"]),
                "label": str(label)[:60],
            })
    return items


def _fetch_map(
    graph: Any, *, focus_id: str | None,
) -> dict[str, Any]:
    resolved_focus = _resolve_focus(graph, focus_id)
    if resolved_focus is None:
        # Empty substrate — return an empty map with a placeholder
        # focus rather than crash. Honest empty state.
        return {
            "focus_id": "",
            "focus_label": "(no nodes in substrate yet)",
            "focus_subtype": "none",
            "nodes": [],
            "edges": [],
            "suggested_pivots": [],
        }
    focus_node = _fetch_node(graph, resolved_focus)
    if focus_node is None:
        return {
            "focus_id": resolved_focus,
            "focus_label": "(node not found)",
            "focus_subtype": "unknown",
            "nodes": [],
            "edges": [],
            "suggested_pivots": _fetch_suggested_pivots(graph),
        }
    # Augment focus_node with degree count.
    rows = graph.query(
        # derivation-read-lint: allow #444 local focus-degree read.
        """
        SELECT count(*) AS n
          FROM okg.graph_edges
         WHERE src = :focus OR dst = :focus
        """,
        {"focus": resolved_focus},
    )
    focus_degree = int(rows[0]["n"] or 0) if rows else 0
    focus_node["degree"] = focus_degree

    neighbors, edges = _fetch_neighbors(graph, resolved_focus, _MAP_NODE_CAP)
    positioned = _layout_radial(focus_node, neighbors)
    pivots = _fetch_suggested_pivots(graph)
    return {
        "focus_id": resolved_focus,
        "focus_label": focus_node["label"],
        "focus_subtype": focus_node["subtype"],
        "nodes": positioned,
        "edges": edges,
        "suggested_pivots": pivots,
    }


# ---------------------------------------------------------------
# Section 3: Sources
# ---------------------------------------------------------------

_SOURCE_PROGRESS_SQL = """
SELECT source_name, key, value, updated_at
FROM okg.source_progress
WHERE deployment = %(d)s
ORDER BY source_name, key
"""


class ConsoleSourceRegistryUnreadable(SubstrateError):
    """The Sources section cannot name its configured sources.

    Raised when the deployment's configured sources cannot be named: its
    directory lookup or its ``source_registry.yaml`` fails to load, parse or
    admit (``stage: "load"``), or two sources claim the same name or
    ownership id (``stage: "identity_overlap"``). ``cause_type`` is the class
    name of the underlying failure, never its text. The section refuses
    rather than serving ``{"items": []}``: an empty list reads as a
    deployment that configures no sources (issue #2147).
    """

    code = "console_source_registry_unreadable"


#: `ConsoleSourceRegistryUnreadable.data["stage"]` values.
REGISTRY_STAGE_LOAD = "load"
REGISTRY_STAGE_IDENTITY_OVERLAP = "identity_overlap"

_REGISTRY_FIX_HINTS = {
    REGISTRY_STAGE_LOAD: (
        "check that the deployment directory resolves and that its "
        "source_registry.yaml parses and is admitted; cause_type names which failed"
    ),
    REGISTRY_STAGE_IDENTITY_OVERLAP: (
        "give each source in source_registry.yaml a name and ownership_id "
        "that no other source uses"
    ),
}


def _registry_refusal(
    deployment_name: str, stage: str, exc: Exception,
) -> ConsoleSourceRegistryUnreadable:
    return ConsoleSourceRegistryUnreadable(
        f"the configured sources of deployment {deployment_name!r} cannot be "
        f"named ({stage}: {type(exc).__name__})",
        data={
            "deployment": deployment_name,
            "stage": stage,
            "cause_type": type(exc).__name__,
        },
        fix_hint=_REGISTRY_FIX_HINTS[stage],
    )


def _source_identities(
    deployment_name: str,
) -> tuple[list[ProducerIdentity], dict[str, ProducerIdentity]]:
    """The configured producers, and every recorded key that names one.

    Resolved through `producer_identity`, the one identity rule the console
    uses, so this section and the Sources and Health reads agree on how many
    producers a deployment has. A registry that cannot be read is a named
    refusal, never an empty list.
    """
    try:
        identities = registry_identities(deployment_name)
    except Exception as exc:
        raise _registry_refusal(deployment_name, REGISTRY_STAGE_LOAD, exc) from exc
    try:
        return identities, runtime_key_index(identities)
    except ValueError as exc:
        raise _registry_refusal(
            deployment_name, REGISTRY_STAGE_IDENTITY_OVERLAP, exc,
        ) from exc


def _fetch_sources(
    conn: Any, deployment_name: str, *, generation_id: int | str,
) -> dict[str, Any]:
    # The configured producers under both of their names. The runtime records
    # progress against the declared ownership_id (older rows: the document
    # key), so rows join through the registry's identity rule, never through
    # the document key alone.
    identities, by_key = _source_identities(deployment_name)

    # Pull recent run state from source_progress.
    progress_by_source: dict[str, dict[str, Any]] = {}
    with conn.cursor() as cur:
        cur.execute(_SOURCE_PROGRESS_SQL, {"d": deployment_name})
        for source_name, key, value, updated_at in cur.fetchall():
            entry = progress_by_source.setdefault(source_name, {
                "last_run_at": None,
                "enqueue_count_24h": 0,
                "error_count_24h": 0,
            })
            if key == "sync.producer.poll":
                try:
                    payload = json.loads(value) if isinstance(value, str) else value
                except Exception:  # noqa: BLE001
                    payload = {}
                if isinstance(payload, dict):
                    last_run = payload.get("last_run_at")
                    if last_run:
                        try:
                            dt = datetime.fromisoformat(
                                str(last_run).replace("Z", "+00:00")
                            )
                            entry["last_run_at"] = dt
                        except Exception:  # noqa: BLE001
                            pass
                    entry["enqueue_count_24h"] = int(
                        payload.get("enqueue_count") or 0
                    )
                    entry["error_count_24h"] = int(
                        payload.get("error_count") or 0
                    )

    # Merge configured + progress. One configured producer is one row, named
    # by its registry key, whichever of its names the runtime recorded.
    # Configured-but-not-run sources render as "idle". Progress rows no
    # configured producer claims still render (legacy or external sources),
    # flagged ``in_registry: false`` under the key they were recorded with.
    rows: list[tuple[str, str, bool, dict[str, Any]]] = []
    for identity in identities:
        recorded = [
            progress_by_source[key]
            for key in (identity.ownership_id, identity.name)
            if key in progress_by_source
        ]
        rows.append(
            (identity.name, identity.ownership_id, True, _latest_progress(recorded))
        )
    # A legacy `<deployment>.<name>` row of a configured producer with no
    # records or edges of its own is omitted and named, by the same rule the
    # Sources read applies (`producer_identity.is_unlisted_alias_key`). The
    # counts are read only when such a key exists.
    live_counts: dict[str, int] | None = None

    def _has_counts(key: str) -> bool:
        nonlocal live_counts
        if live_counts is None:
            live_counts = _live_source_counts(conn, generation_id=generation_id)
        return live_counts.get(key, 0) != 0

    alias_keys: list[str] = []
    for key in sorted(progress_by_source):
        if key in by_key:
            continue
        if is_unlisted_alias_key(
            key,
            deployment_name=deployment_name,
            by_key=by_key,
            has_counts=lambda key=key: _has_counts(key),
        ):
            alias_keys.append(key)
            continue
        rows.append((key, key, False, progress_by_source[key]))

    items: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    for name, source_key, in_registry, prog in rows:
        last_run = prog.get("last_run_at")
        last_run_age = _humanize_age(last_run) if last_run else None
        enqueue_24h = int(prog.get("enqueue_count_24h") or 0)
        error_24h = int(prog.get("error_count_24h") or 0)
        # Derive status. Idle if no run in 24h, errored if error
        # count > 0 in the last 24h, otherwise healthy.
        if error_24h > 0:
            status = "errored"
        elif last_run is None:
            status = "idle"
        else:
            if last_run.tzinfo is None:
                last_run = last_run.replace(tzinfo=timezone.utc)
            delta_hours = (now - last_run).total_seconds() / 3600.0
            status = "idle" if delta_hours > 24 else "healthy"
        entry: dict[str, Any] = {
            "name": name,
            "source_key": source_key,
            "in_registry": in_registry,
            "enqueue_count_24h": enqueue_24h,
            "error_count_24h": error_24h,
            "status": status,
        }
        if last_run_age:
            entry["last_run_age"] = last_run_age
        items.append(entry)

    # Sort: healthy first, then idle, then errored (so errored
    # rows are visible at the top once we reverse). Actually the
    # operator probably wants errored first.
    status_order = {"errored": 0, "healthy": 1, "idle": 2}
    items.sort(key=lambda s: (status_order.get(s["status"], 9), s["name"]))
    return {
        "items": items,
        # Recorded keys that are only a prefixed spelling of a configured
        # producer's name and carry no records and no edges. Named, not
        # silently dropped -- the same field the Sources read serves.
        "alias_keys_not_listed": alias_keys,
    }


def _live_source_counts(conn: Any, *, generation_id: int | str) -> dict[str, int]:
    """Records plus edges per recorded source key, from the maintained rollup.

    The same counts the Sources read decides an alias key by. The rollup is
    required: the read refuses rather than guess that a key is empty.
    """
    require_console_count_rollup_enabled(generation_id=generation_id)
    rolled = rollup_reader(None, conn, generation_id=generation_id).read(
        {COUNT_KIND_SOURCE, COUNT_KIND_EDGE_SOURCE}
    )
    totals: dict[str, int] = {}
    for (_kind, name), count in rolled.items():
        totals[name] = totals.get(name, 0) + int(count)
    return totals


def _latest_progress(recorded: list[dict[str, Any]]) -> dict[str, Any]:
    """One producer's progress when it was recorded under both of its names.

    Older rows carry the document key and newer rows the ownership_id, so a
    producer can have two progress entries. The one with the latest run is its
    current state. Counts are not summed across them: ``enqueue_count`` and
    ``error_count`` are snapshots from each row's latest poll payload, not
    running totals, so the older row's numbers describe an older poll.
    """
    ran = [entry for entry in recorded if entry.get("last_run_at") is not None]
    if ran:
        return max(ran, key=lambda entry: _as_utc(entry["last_run_at"]))
    return recorded[0] if recorded else {}


def _as_utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
