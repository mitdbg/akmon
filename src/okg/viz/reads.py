"""Substrate-native read helpers for the viz server.

Every function takes a `(conn, session)` pair and runs a single
SQL query (or a small, tightly-scoped batch) pinned to the
session's `generation_id` / `catalog_version_id`. Results are
Python dicts matching the shape the viz UI expects — no
adapter class, no in-memory graph object, no snapshot pickle.

The substrate (the Chronos graph okg.graph_nodes / okg.graph_edges,
plus okg.subtypes / okg.edge_narrowings) IS the data layer; this
module is a thin translator to viz-shaped JSON.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Optional

import psycopg

from okg.substrate.mcp.session import Session, assert_session_matches
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.searchable_text import without_producer_text


@dataclass(frozen=True)
class NodeRow:
    """Viz node view. Flattened from okg.graph_nodes for template ease."""
    id: str
    type: str
    attrs: dict[str, Any]
    label: Optional[str] = None

    @classmethod
    def from_row(cls, row: tuple) -> "NodeRow":
        node_id, subtype, attrs = row
        # The searchable-text copy of `text` (issue #2991) is never returned.
        attrs = without_producer_text(attrs or {})
        label = (
            attrs.get("label")
            or attrs.get("title")
            or attrs.get("name")
            or node_id
        )
        return cls(id=node_id, type=subtype, attrs=attrs, label=label)


@dataclass(frozen=True)
class EdgeRow:
    """Viz edge view."""
    src: str
    dst: str
    edge_type: str
    attrs: dict[str, Any]


def _graph(conn: psycopg.Connection, session: Session):
    assert_session_matches(conn, session)
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    return store.checkout_generation(
        session.generation_id,
        branch=session.branch_name,
    )


def _node_from_mapping(row: dict[str, Any]) -> NodeRow:
    return NodeRow.from_row((row["node_id"], row["subtype"], row.get("attrs")))


def _edge_from_mapping(row: dict[str, Any]) -> EdgeRow:
    return EdgeRow(
        src=row["src"],
        dst=row["dst"],
        edge_type=row["edge_type"],
        attrs=row.get("attrs") or {},
    )


# ---------------------------------------------------------------------------
# Single-node reads
# ---------------------------------------------------------------------------

def fetch_node(
    conn: psycopg.Connection, session: Session, node_id: str,
) -> Optional[NodeRow]:
    """One node at the pinned generation, or None if not visible."""
    graph = _graph(conn, session)
    rows = graph.query(
        """
        SELECT node_id, subtype, attrs
          FROM okg.graph_nodes
         WHERE node_id = :node_id
        """,
        {"node_id": node_id},
    )
    return _node_from_mapping(rows[0]) if rows else None


def fetch_nodes(
    conn: psycopg.Connection, session: Session,
    node_ids: list[str], *, light: bool = False,
) -> dict[str, "NodeRow"]:
    """Bulk node fetch: one query for many ids. Returns id → NodeRow.
    Replaces N round-trip `fetch_node` loops in expand/khop paths.
    """
    if not node_ids:
        return {}
    if light:
        attrs_expr = (
            "(attrs - ARRAY['body_text','body_text_md','body','text',"
            "'pdf_url','local_pdf_path','pdf_bytes','raw','content','html',"
            "'okg_producer_text'])::jsonb"
        )
    else:
        attrs_expr = "attrs"
    graph = _graph(conn, session)
    rows = graph.query(
        f"""
        SELECT node_id, subtype, {attrs_expr} AS attrs
          FROM okg.graph_nodes
         WHERE node_id = ANY(:ids)
        """,
        {"ids": list(node_ids)},
    )
    return {r["node_id"]: _node_from_mapping(r) for r in rows}


def has_node(
    conn: psycopg.Connection, session: Session, node_id: str,
) -> bool:
    """Boolean existence check at the pin."""
    graph = _graph(conn, session)
    rows = graph.query(
        "SELECT 1 AS ok FROM okg.graph_nodes WHERE node_id = :node_id LIMIT 1",
        {"node_id": node_id},
    )
    return bool(rows)


def view_definition_registered(
    conn: psycopg.Connection, session: Session,
    view_def_id: str, deployment: str,
) -> bool:
    """True when the pinned generation carries a `view_definition` node
    registering `view_def_id` for `deployment`."""
    graph = _graph(conn, session)
    rows = graph.query(
        """
        SELECT 1 AS ok
          FROM okg.graph_nodes
         WHERE subtype = 'view_definition'
           AND attrs->>'view_def_id' = :view_def_id
           AND attrs->>'deployment' = :deployment
         LIMIT 1
        """,
        {"view_def_id": view_def_id, "deployment": deployment},
    )
    return bool(rows)


# ---------------------------------------------------------------------------
# Bulk reads
# ---------------------------------------------------------------------------

def iter_nodes(
    conn: psycopg.Connection, session: Session,
    *, subtype: Optional[str] = None, limit: Optional[int] = None,
    light: bool = False, order_by_id: bool = False,
) -> Iterator[NodeRow]:
    """Stream nodes at the pin, optionally filtered by subtype.

    `light=True` strips heavy attrs (body_text, pdf_url, …) from the
    returned `attrs` dict. Critical for callers that only need
    id/label/degree (e.g. the sample endpoint) — fetching full attrs
    on a 9380-row af_pubs publication scan otherwise pulls ~470 MB
    of body_text and dominates response time.
    """
    params: dict[str, Any] = {}
    where: list[str] = []
    if subtype is not None:
        where.append("subtype = :subtype")
        params["subtype"] = subtype
    if light:
        attrs_expr = (
            "(attrs - ARRAY['body_text','body_text_md','body','text',"
            "'pdf_url','local_pdf_path','pdf_bytes','raw','content','html',"
            "'okg_producer_text'])::jsonb"
        )
    else:
        attrs_expr = "attrs"
    sql = f"SELECT node_id, subtype, {attrs_expr} AS attrs FROM okg.graph_nodes"
    if where:
        sql += f" WHERE {' AND '.join(where)}"
    if order_by_id:
        sql += " ORDER BY node_id"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    graph = _graph(conn, session)
    for row in graph.query(sql, params):
        yield _node_from_mapping(row)


def iter_edges(
    conn: psycopg.Connection, session: Session,
    *, edge_type: Optional[str] = None, limit: Optional[int] = None,
) -> Iterator[EdgeRow]:
    """Stream all edges at the pin, optionally filtered by edge_type."""
    params: dict[str, Any] = {}
    where: list[str] = []
    if edge_type is not None:
        where.append("edge_type = :edge_type")
        params["edge_type"] = edge_type
    sql = "SELECT src, dst, edge_type, attrs FROM okg.graph_edges"
    if where:
        sql += f" WHERE {' AND '.join(where)}"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    graph = _graph(conn, session)
    for row in graph.query(sql, params):
        yield _edge_from_mapping(row)


def fetch_edges_within(
    conn: psycopg.Connection, session: Session, node_ids: list[str],
    *, limit: Optional[int] = None,
) -> list["EdgeRow"]:
    """Substrate edges where BOTH endpoints are in `node_ids` at the
    pin. Used by trace-mode hydration to surface the structural
    relationships between the agent's touched nodes — not just the
    edges returned by individual `list_neighbors` calls.
    """
    if not node_ids:
        return []
    sql = (
        "SELECT src, dst, edge_type, attrs FROM okg.graph_edges "
        "WHERE src = ANY(:ids) AND dst = ANY(:ids)"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    graph = _graph(conn, session)
    return [_edge_from_mapping(row) for row in graph.query(sql, {"ids": list(node_ids)})]


# ---------------------------------------------------------------------------
# Neighbor walks
# ---------------------------------------------------------------------------

def iter_out_edges(
    conn: psycopg.Connection, session: Session, src: str,
    *, edge_type: Optional[str] = None, limit: Optional[int] = None,
    offset: int = 0,
    excluded_subtypes: Optional[list[str]] = None,
    included_subtypes: Optional[list[str]] = None,
) -> list[EdgeRow]:
    """Outbound edges from `src` at the pin.

    `offset` enables sampled-expansion load-more (viz-expansion-and-
    supernodes §2): with ORDER BY (edge_type, dst), the same source
    returns deterministic pages so the placeholder click can fetch
    the next K without overlap.

    `excluded_subtypes` / `included_subtypes` push the sidebar
    type-filter into SQL via a JOIN on okg.graph_nodes. Without
    this, the endpoint had to over-fetch and post-filter, which
    silently shrank the returned set when a type was hidden
    (asking for 40 returned 5 if 35 were filtered).
    """
    params: dict[str, Any] = {
        "src": src,
    }
    where = ["e.src = :src"]
    join = ""
    if edge_type is not None:
        where.append("e.edge_type = :edge_type")
        params["edge_type"] = edge_type
    if excluded_subtypes or included_subtypes:
        join = (
            " JOIN okg.graph_nodes n ON n.node_id = e.dst"
        )
        if excluded_subtypes:
            where.append("n.subtype <> ALL(:excluded)")
            params["excluded"] = list(excluded_subtypes)
        if included_subtypes:
            where.append("n.subtype = ANY(:included)")
            params["included"] = list(included_subtypes)
    sql = (
        "SELECT e.src, e.dst, e.edge_type, e.attrs "
        f"FROM okg.graph_edges e{join} "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY e.edge_type, e.dst"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    if offset:
        sql += f" OFFSET {int(offset)}"
    graph = _graph(conn, session)
    return [_edge_from_mapping(row) for row in graph.query(sql, params)]


def iter_in_edges(
    conn: psycopg.Connection, session: Session, dst: str,
    *, edge_type: Optional[str] = None, limit: Optional[int] = None,
    offset: int = 0,
    excluded_subtypes: Optional[list[str]] = None,
    included_subtypes: Optional[list[str]] = None,
) -> list[EdgeRow]:
    """Inbound edges to `dst` at the pin. See iter_out_edges for
    the offset and subtype-filter semantics."""
    params: dict[str, Any] = {
        "dst": dst,
    }
    where = ["e.dst = :dst"]
    join = ""
    if edge_type is not None:
        where.append("e.edge_type = :edge_type")
        params["edge_type"] = edge_type
    if excluded_subtypes or included_subtypes:
        join = (
            " JOIN okg.graph_nodes n ON n.node_id = e.src"
        )
        if excluded_subtypes:
            where.append("n.subtype <> ALL(:excluded)")
            params["excluded"] = list(excluded_subtypes)
        if included_subtypes:
            where.append("n.subtype = ANY(:included)")
            params["included"] = list(included_subtypes)
    sql = (
        "SELECT e.src, e.dst, e.edge_type, e.attrs "
        f"FROM okg.graph_edges e{join} "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY e.edge_type, e.src"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    if offset:
        sql += f" OFFSET {int(offset)}"
    graph = _graph(conn, session)
    return [_edge_from_mapping(row) for row in graph.query(sql, params)]


def count_out_neighbors(
    conn: psycopg.Connection, session: Session, src: str,
    *, edge_type: Optional[str] = None,
    excluded_subtypes: Optional[list[str]] = None,
    included_subtypes: Optional[list[str]] = None,
) -> int:
    """Count unique outbound neighbors from `src` using the same filters
    as `iter_out_edges`.
    """
    return _count_directional_neighbors(
        conn, session, node_id=src, endpoint_col="src", neighbor_col="dst",
        edge_type=edge_type, excluded_subtypes=excluded_subtypes,
        included_subtypes=included_subtypes,
    )


def count_in_neighbors(
    conn: psycopg.Connection, session: Session, dst: str,
    *, edge_type: Optional[str] = None,
    excluded_subtypes: Optional[list[str]] = None,
    included_subtypes: Optional[list[str]] = None,
) -> int:
    """Count unique inbound neighbors to `dst` using the same filters
    as `iter_in_edges`.
    """
    return _count_directional_neighbors(
        conn, session, node_id=dst, endpoint_col="dst", neighbor_col="src",
        edge_type=edge_type, excluded_subtypes=excluded_subtypes,
        included_subtypes=included_subtypes,
    )


def _count_directional_neighbors(
    conn: psycopg.Connection, session: Session,
    *, node_id: str, endpoint_col: str, neighbor_col: str,
    edge_type: Optional[str] = None,
    excluded_subtypes: Optional[list[str]] = None,
    included_subtypes: Optional[list[str]] = None,
) -> int:
    if endpoint_col not in {"src", "dst"} or neighbor_col not in {"src", "dst"}:
        raise ValueError("endpoint_col and neighbor_col must be 'src' or 'dst'")
    params: dict[str, Any] = {"node_id": node_id}
    where = [f"e.{endpoint_col} = :node_id"]
    join = ""
    if edge_type is not None:
        where.append("e.edge_type = :edge_type")
        params["edge_type"] = edge_type
    if excluded_subtypes or included_subtypes:
        join = (
            f" JOIN okg.graph_nodes n ON n.node_id = e.{neighbor_col}"
        )
        if excluded_subtypes:
            where.append("n.subtype <> ALL(:excluded)")
            params["excluded"] = list(excluded_subtypes)
        if included_subtypes:
            where.append("n.subtype = ANY(:included)")
            params["included"] = list(included_subtypes)
    graph = _graph(conn, session)
    rows = graph.query(
        f"SELECT count(DISTINCT e.{neighbor_col})::int AS n "
        f"FROM okg.graph_edges e{join} "
        f"WHERE {' AND '.join(where)}",
        params,
    )
    return int(rows[0]["n"] or 0) if rows else 0


def degrees_for(
    conn: psycopg.Connection, session: Session,
    node_ids: list[str],
) -> dict[str, int]:
    """Return `{node_id: degree}` for every requested id in one
    round-trip. Degree counts both inbound and outbound edges.

    Migration regression fix (2026-04-25): every viz endpoint
    used to hardcode `degree: 0` after the substrate-native
    rewrite — the legacy implementation called `Backbone.degree(id)`
    which doesn't exist post-migration. Without real degrees the
    "MOST CONNECTED" panel sorts arbitrarily and any
    degree-aware layout reads zero everywhere.
    """
    if not node_ids:
        return {}
    graph = _graph(conn, session)
    rows = graph.query(
        """
        SELECT id, count(*)::int AS n FROM (
          SELECT src AS id FROM okg.graph_edges WHERE src = ANY(:ids)
          UNION ALL
          SELECT dst AS id FROM okg.graph_edges WHERE dst = ANY(:ids)
        ) joined
        GROUP BY id
        """,
        {"ids": list(node_ids)},
    )
    return {row["id"]: int(row["n"]) for row in rows}


# ---------------------------------------------------------------------------
# Ontology surface — reads from catalog tables at the pinned
# catalog_version_id. No OKGOntology reconstruction.
# ---------------------------------------------------------------------------

def fetch_subtypes(
    conn: psycopg.Connection, session: Session,
) -> list[dict[str, Any]]:
    """Subtypes at the pinned catalog_version_id."""
    assert_session_matches(conn, session)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name, metadata FROM okg.subtypes "
            "WHERE catalog_version_id = %s ORDER BY name",
            (session.catalog_version_id,),
        )
        return [
            {"name": r[0], "metadata": r[1] or {}}
            for r in cur.fetchall()
        ]


def fetch_edge_narrowings(
    conn: psycopg.Connection, session: Session,
) -> list[dict[str, Any]]:
    """Edge narrowings at the pinned catalog_version_id."""
    assert_session_matches(conn, session)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT parent_subtype, edge_type, child_subtype "
            "FROM okg.narrowings "
            "WHERE catalog_version_id = %s "
            "ORDER BY parent_subtype, edge_type, child_subtype",
            (session.catalog_version_id,),
        )
        return [
            {
                "src_subtype": r[0],
                "edge_type": r[1],
                "dst_subtype": r[2],
            }
            for r in cur.fetchall()
        ]


# ---------------------------------------------------------------------------
# Aggregates
# ---------------------------------------------------------------------------

def count_nodes(
    conn: psycopg.Connection, session: Session,
    *, subtype: Optional[str] = None,
) -> int:
    """Total node count at the pin, optionally filtered by subtype.
    Cheap SQL count — replaces the iterate-and-count anti-pattern."""
    params: dict[str, Any] = {}
    where: list[str] = []
    if subtype is not None:
        where.append("subtype = :subtype")
        params["subtype"] = subtype
    sql = "SELECT count(DISTINCT node_id) AS n FROM okg.graph_nodes"
    if where:
        sql += " WHERE " + " AND ".join(where)
    rows = _graph(conn, session).query(sql, params)
    return int(rows[0]["n"] or 0) if rows else 0


def count_edges(
    conn: psycopg.Connection, session: Session,
) -> int:
    rows = _graph(conn, session).query(
        "SELECT count(DISTINCT edge_id) AS n FROM okg.graph_edges"
    )
    return int(rows[0]["n"] or 0) if rows else 0


def count_by_subtype(
    conn: psycopg.Connection, session: Session,
) -> dict[str, int]:
    """Node count per subtype at the pin."""
    rows = _graph(conn, session).query(
        """
        SELECT subtype, count(*)::int AS n
          FROM okg.graph_nodes
      GROUP BY subtype
      ORDER BY subtype
        """
    )
    return {r["subtype"]: int(r["n"]) for r in rows}


def count_by_edge_type(
    conn: psycopg.Connection, session: Session,
) -> dict[str, int]:
    """Edge count per edge type at the pin."""
    rows = _graph(conn, session).query(
        """
        SELECT edge_type, count(DISTINCT edge_id)::int AS n
          FROM okg.graph_edges
      GROUP BY edge_type
      ORDER BY edge_type
        """
    )
    return {r["edge_type"]: int(r["n"]) for r in rows}


def dump_full_graph(
    conn: psycopg.Connection, session: Session,
    *, max_nodes: int, seed_node_id: Optional[str] = None,
) -> dict[str, Any]:
    """Dump the deployment graph in one payload for the bench harness.

    Returns ``{nodes, edges, meta}`` where ``meta`` reports
    ``total_nodes`` / ``total_edges`` (live counts), the
    ``sampled_nodes`` / ``sampled_edges`` actually returned, a
    ``truncated`` flag, and the ``seed`` (if any).

    Ordering is deterministic — ``(subtype, node_id)`` for nodes and
    ``(edge_type, src, dst)`` for edges — so two calls against the
    same generation return byte-identical payloads. Heavy attrs
    (body_text, pdf bytes, etc.) are stripped because the bench only
    cares about id/type/label and edges.

    BFS sampling: when ``seed_node_id`` is provided AND the live
    node count exceeds ``max_nodes``, a BFS expansion from the seed
    fills exactly ``max_nodes`` (or however many are reachable).
    Returned edges are filtered to those whose endpoints are both
    in the kept set. When the seed is absent or the live count
    fits, we return up to ``max_nodes`` nodes in deterministic order
    plus all edges whose endpoints are in the kept set.
    """
    graph = _graph(conn, session)
    light_attrs = (
        "(attrs - ARRAY['body_text','body_text_md','body','text',"
        "'pdf_url','local_pdf_path','pdf_bytes','raw','content','html',"
            "'okg_producer_text'])::jsonb"
    )
    total_nodes = int(
        graph.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"] or 0
    )
    total_edges = int(
        graph.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"] or 0
    )

    truncated = total_nodes > max_nodes
    kept_ids: set[str] = set()

    # When truncated and no explicit seed, default to BFS from
    # the largest-degree node — yields a connected sub-graph
    # that's actually useful for layout benching, instead of a
    # disconnected dust ordered by (subtype, id).
    if truncated and not seed_node_id:
        rows = graph.query(
            """
            SELECT node_id, count(*) AS deg
              FROM (
                SELECT src AS node_id FROM okg.graph_edges
                UNION ALL
                SELECT dst AS node_id FROM okg.graph_edges
              ) e
             GROUP BY node_id
             ORDER BY deg DESC, node_id
             LIMIT 1
            """
        )
        if rows:
            seed_node_id = rows[0]["node_id"]

    if seed_node_id and truncated:
        # BFS from seed across undirected adjacency until we hit
        # max_nodes. We pull the full edge list once (already
        # bounded by total_edges) and walk it in Python — simpler
        # than chained recursive CTEs and fast enough at the
        # scales we care about (≤25k nodes).
        edge_pairs = graph.query("SELECT src, dst FROM okg.graph_edges")
        adj: dict[str, list[str]] = {}
        for row in edge_pairs:
            s, d = row["src"], row["dst"]
            adj.setdefault(s, []).append(d)
            adj.setdefault(d, []).append(s)
        from collections import deque
        seen = {seed_node_id}
        order = [seed_node_id]
        q = deque([seed_node_id])
        while q and len(order) < max_nodes:
            cur_id = q.popleft()
            for nb in adj.get(cur_id, ()):
                if nb in seen:
                    continue
                seen.add(nb)
                order.append(nb)
                if len(order) >= max_nodes:
                    break
                q.append(nb)
        node_rows = graph.query(
            f"""
            SELECT node_id, subtype, {light_attrs} AS attrs
              FROM okg.graph_nodes
             WHERE node_id = ANY(:ids)
          ORDER BY subtype, node_id
            """,
            {"ids": list(order)},
        )
        kept_ids = {r["node_id"] for r in node_rows}
    else:
        node_rows = graph.query(
            f"""
            SELECT node_id, subtype, {light_attrs} AS attrs
              FROM okg.graph_nodes
          ORDER BY subtype, node_id
             LIMIT {int(max_nodes)}
            """
        )
        kept_ids = {r["node_id"] for r in node_rows}

    edge_rows = graph.query(
        """
        SELECT src, dst, edge_type, attrs
          FROM okg.graph_edges
         WHERE src = ANY(:ids) AND dst = ANY(:ids)
      ORDER BY edge_type, src, dst
        """,
        {"ids": list(kept_ids)},
    )

    nodes_out: list[dict[str, Any]] = []
    for row in node_rows:
        node_id = row["node_id"]
        attrs = row.get("attrs") or {}
        nodes_out.append({
            "id": node_id,
            "type": row["subtype"],
            "label": (
                attrs.get("label")
                or attrs.get("title")
                or attrs.get("name")
                or node_id
            ),
        })
    edges_out: list[dict[str, Any]] = []
    for row in edge_rows:
        edges_out.append({
            "src": row["src"],
            "dst": row["dst"],
            "edge_type": row["edge_type"],
        })
    return {
        "nodes": nodes_out,
        "edges": edges_out,
        "meta": {
            "generation_id": session.generation_id,
            "total_nodes": total_nodes,
            "total_edges": total_edges,
            "sampled_nodes": len(nodes_out),
            "sampled_edges": len(edges_out),
            "truncated": truncated,
            "seed": seed_node_id,
        },
    }


def graph_stats(
    conn: psycopg.Connection, session: Session,
) -> dict[str, Any]:
    """Aggregate stats — node_count, edge_count, subtype_count,
    edge_type_count. Matches the legacy /viz/api/stats shape."""
    graph = _graph(conn, session)
    node_row = graph.query(
        """
        SELECT count(*)::int AS n_nodes,
               count(DISTINCT subtype)::int AS n_subtypes
          FROM okg.graph_nodes
        """
    )[0]
    edge_row = graph.query(
        """
        SELECT count(*)::int AS n_edges,
               count(DISTINCT edge_type)::int AS n_edge_types
          FROM okg.graph_edges
        """
    )[0]
    return {
        "node_count": node_row["n_nodes"],
        "edge_count": edge_row["n_edges"],
        "subtype_count": node_row["n_subtypes"],
        "edge_type_count": edge_row["n_edge_types"],
        "generation_id": session.generation_id,
    }


# ---------------------------------------------------------------------------
# Quarantine — the substrate has no quarantine bit on graph nodes.
# Retractions remove the node from the generation, so a retracted
# node simply doesn't appear at the pin. Viz's "is this quarantined?"
# call sites always return False against substrate reads.
# ---------------------------------------------------------------------------

def is_quarantined(
    conn: psycopg.Connection, session: Session, node_id: str,
) -> bool:
    """Substrate has no quarantine concept; nodes visible at the
    pin are just visible. Retractions are absence, not flags."""
    return False
