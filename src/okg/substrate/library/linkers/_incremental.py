"""Incremental relevance guards for deterministic linkers."""
from __future__ import annotations

from okg.substrate.enrichers.base import IncrementalContext
from okg.substrate.library.linkers import _chronos


MAX_RELEVANCE_DIRTY_IDS = 10_000


def incremental_touches(
    cur,
    incremental: IncrementalContext | None,
    *,
    node_subtypes: tuple[str, ...],
    edge_endpoint_subtypes: tuple[str, ...] | None = None,
    edge_types: tuple[str, ...] | None = None,
) -> bool:
    """Return True when an incremental dirty set may affect a linker.

    Unknown is treated as relevant. This keeps the guard safe across deleted
    rows, Chronos read failures, and dirty sets large enough that a relevance
    probe costs as much as the linker scan it would avoid.
    """
    if incremental is None or incremental.is_first_run:
        return True
    if incremental.last_status != "success":
        return True
    if incremental.current_catalog_version_id is not None:
        if incremental.prev_catalog_version_id is None:
            return True
        if (
            incremental.prev_catalog_version_id
            != incremental.current_catalog_version_id
        ):
            return True
    relevant_nodes = frozenset(node_subtypes)
    relevant_edge_endpoints = frozenset(edge_endpoint_subtypes or node_subtypes)

    if incremental.dirty_node_ids:
        if len(incremental.dirty_node_ids) > MAX_RELEVANCE_DIRTY_IDS:
            return True
        dirty_subtypes = _dirty_node_subtypes(cur, incremental.dirty_node_ids)
        if dirty_subtypes is None:
            return True
        if dirty_subtypes.intersection(relevant_nodes):
            return True

    if incremental.dirty_edge_ids:
        if len(incremental.dirty_edge_ids) > MAX_RELEVANCE_DIRTY_IDS:
            return True
        endpoint_subtypes = _dirty_edge_endpoint_subtypes(
            cur,
            incremental.dirty_edge_ids,
            edge_types=edge_types,
        )
        if endpoint_subtypes is None:
            return True
        if endpoint_subtypes.intersection(relevant_edge_endpoints):
            return True

    return False


def _dirty_node_subtypes(
    cur,
    dirty_node_ids: frozenset[str],
) -> frozenset[str] | None:
    rows = _chronos.query(
        cur,
        """
        SELECT node_id, subtype
          FROM okg.graph_nodes
         WHERE node_id = ANY(:node_ids)
        """,
        {"node_ids": sorted(dirty_node_ids)},
    )
    if len(rows) != len(dirty_node_ids):
        return None
    return frozenset(str(row["subtype"]) for row in rows)


def _dirty_edge_endpoint_subtypes(
    cur,
    dirty_edge_ids: frozenset[int],
    *,
    edge_types: tuple[str, ...] | None = None,
) -> frozenset[str] | None:
    rows = _chronos.query(
        cur,
        """
        SELECT e.edge_id,
               e.edge_type,
               p.subtype AS src_subtype,
               c.subtype AS dst_subtype
          FROM okg.graph_edges e
          LEFT JOIN okg.graph_nodes p ON p.node_id = e.src
          LEFT JOIN okg.graph_nodes c ON c.node_id = e.dst
         WHERE e.edge_id = ANY(:edge_ids)
        """,
        {"edge_ids": sorted(dirty_edge_ids)},
    )
    if len(rows) != len(dirty_edge_ids):
        return None
    relevant_edge_types = frozenset(edge_types or ())
    out: set[str] = set()
    for row in rows:
        if relevant_edge_types and str(row.get("edge_type") or "") not in relevant_edge_types:
            continue
        src_subtype = row.get("src_subtype")
        dst_subtype = row.get("dst_subtype")
        if not src_subtype or not dst_subtype:
            return None
        out.add(str(src_subtype))
        out.add(str(dst_subtype))
    return frozenset(out)
