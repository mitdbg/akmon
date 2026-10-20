"""Graph write-sets that source runs report to the ingest service.

Each source run may report the identities it touched (``graph_write_set``)
and the source delta batch it appended (``source_delta_batch``). The ingest
service merges those per-layer reports into the publisher's dirty-scope and
enricher-invalidation hints, then shrinks oversized write-sets before the
operator report is written. These helpers are pure: they read and rewrite
result dictionaries and never touch the database.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from okg.substrate.enrichers.lineage import (
    invalidations_from_source_delta_batches,
)

# Sentinel hint: the publisher must derive enricher invalidations from chronos.
_ENRICHER_LINEAGE_FROM_CHRONOS = object()

_GRAPH_WRITE_SET_REPORT_ID_LIMIT = 10_000


def _compact_large_graph_write_sets(report: dict[str, Any]) -> None:
    """Summarize oversized dirty-scope ID lists in the operator report.

    ``graph_write_set`` is an in-process publisher optimization: the publish
    decision reads it to narrow its dirty scope. It is not operator output.
    A source that touched millions of identities makes the run's final JSON
    receipt cost gigabytes of memory to build and disk to store, purely to
    restate what the accompanying counts already say. Runs under the limit
    keep the exact IDs, which is what makes the payload useful for
    diagnosing a small delta.

    Called only after the publish decision consumed the write-sets.
    """
    results = report.get("results")
    if not isinstance(results, list):
        return
    for result in results:
        if isinstance(result, dict):
            _compact_large_graph_write_set_result(result)


def _compact_large_graph_write_set_result(result: dict[str, Any]) -> None:
    payload = result.get("graph_write_set")
    if not isinstance(payload, Mapping):
        return
    nodes = payload.get("nodes_by_subtype")
    edges = payload.get("edges_by_type")
    changes = payload.get("node_attr_changes_by_subtype")
    if not isinstance(nodes, Mapping) or not isinstance(edges, Mapping):
        return
    groups: list[Mapping[Any, Any]] = [nodes, edges]
    if isinstance(changes, Mapping):
        groups.append(changes)
    if any(
        not hasattr(value, "__len__")
        for group in groups
        for value in group.values()
    ):
        # An entry whose size cannot be read cannot be counted either, so a
        # summary built from the rest would silently understate the payload
        # it replaced. Leave the exact write-set in place instead.
        return
    node_counts = {
        str(subtype): len(node_ids) for subtype, node_ids in nodes.items()
    }
    edge_counts = {
        str(edge_type): len(edge_ids) for edge_type, edge_ids in edges.items()
    }
    change_counts = (
        {
            str(subtype): len(node_changes)
            for subtype, node_changes in changes.items()
        }
        if isinstance(changes, Mapping)
        else {}
    )
    materialized_ids = (
        sum(node_counts.values())
        + sum(edge_counts.values())
        + sum(change_counts.values())
    )
    if materialized_ids <= _GRAPH_WRITE_SET_REPORT_ID_LIMIT:
        return
    result.pop("graph_write_set", None)
    result["graph_write_set_summary"] = {
        "schema": "okg.graph_write_set.summary.v1",
        "nodes_by_subtype": node_counts,
        "edges_by_type": edge_counts,
        "node_attr_changes_by_subtype": change_counts,
        "nodes_total": sum(node_counts.values()),
        "edges_total": sum(edge_counts.values()),
        "node_attr_changes_total": sum(change_counts.values()),
        "edge_table_available": bool(payload.get("edge_table_available", True)),
        "materialized_ids": False,
    }


def _merge_layer_chronos_dirty_scope_hint(
    layer_results: list[dict[str, Any]],
) -> (
    tuple[
        dict[str, list[str]],
        dict[str, list[int]],
        bool,
        dict[str, dict[str, list[str]]] | None,
    ]
    | None
):
    """Merge source-run graph write-sets into a publisher dirty-scope hint."""
    if not layer_results:
        return None
    nodes_by_subtype: dict[str, list[str]] = {}
    node_attr_changes_by_subtype: dict[str, dict[str, list[str]]] = {}
    edges_by_type: dict[str, list[int]] = {}
    edge_table_available = True
    node_attr_changes_available = True
    hint_seen = False
    for result in layer_results:
        has_work = _source_result_has_work(result)
        payload = result.get("graph_write_set")
        if not isinstance(payload, Mapping):
            if has_work:
                return None
            continue
        if payload.get("schema") != "okg.graph_write_set.v1":
            if has_work:
                return None
            continue
        node_payload = payload.get("nodes_by_subtype")
        node_attr_payload = payload.get("node_attr_changes_by_subtype")
        edge_payload = payload.get("edges_by_type")
        if not isinstance(node_payload, Mapping) or not isinstance(
            edge_payload,
            Mapping,
        ):
            return None
        if node_attr_payload is None:
            node_attr_changes_available = False
        elif not isinstance(node_attr_payload, Mapping):
            return None
        hint_seen = True
        edge_table_available = edge_table_available and bool(
            payload.get("edge_table_available", True),
        )
        for subtype, node_ids in node_payload.items():
            if isinstance(node_ids, (str, bytes)):
                return None
            try:
                values = [str(node_id) for node_id in node_ids]
            except TypeError:
                return None
            nodes_by_subtype.setdefault(str(subtype), []).extend(values)
        if isinstance(node_attr_payload, Mapping):
            for subtype, node_changes in node_attr_payload.items():
                if not isinstance(node_changes, Mapping):
                    return None
                target = node_attr_changes_by_subtype.setdefault(
                    str(subtype),
                    {},
                )
                for node_id, fields in node_changes.items():
                    if isinstance(fields, (str, bytes)):
                        return None
                    try:
                        field_values = [str(field) for field in fields]
                    except TypeError:
                        return None
                    target.setdefault(str(node_id), []).extend(field_values)
        for edge_type, edge_ids in edge_payload.items():
            if isinstance(edge_ids, (str, bytes)):
                return None
            try:
                values = [int(edge_id) for edge_id in edge_ids]
            except (TypeError, ValueError):
                return None
            edges_by_type.setdefault(str(edge_type), []).extend(values)
    if not hint_seen:
        if not any(_source_result_has_work(result) for result in layer_results):
            return ({}, {}, True, {})
        return None
    return (
        {
            subtype: list(dict.fromkeys(node_ids))
            for subtype, node_ids in nodes_by_subtype.items()
        },
        {
            edge_type: list(dict.fromkeys(edge_ids))
            for edge_type, edge_ids in edges_by_type.items()
        },
        edge_table_available,
        (
            {
                subtype: {
                    node_id: list(dict.fromkeys(fields))
                    for node_id, fields in node_changes.items()
                }
                for subtype, node_changes in (node_attr_changes_by_subtype.items())
            }
            if node_attr_changes_available
            else None
        ),
    )


def _merge_layer_enricher_lineage_invalidations_hint(
    layer_results: list[dict[str, Any]],
) -> Any:
    """Merge source delta batches into publisher enricher invalidations."""

    if not layer_results:
        return ()
    batches: list[Mapping[str, Any]] = []
    for result in layer_results:
        batch = result.get("source_delta_batch")
        if isinstance(batch, Mapping):
            batches.append(batch)
        elif _source_result_has_work(result):
            return _ENRICHER_LINEAGE_FROM_CHRONOS
    invalidations = invalidations_from_source_delta_batches(tuple(batches))
    if invalidations is None:
        return _ENRICHER_LINEAGE_FROM_CHRONOS
    return invalidations


def _source_result_has_work(result: Mapping[str, Any]) -> bool:
    for key in ("nodes_appended", "edges_appended", "retract_n", "retract_e"):
        if _int_result_count(result, key) != 0:
            return True
    return False


def _int_result_count(result: Mapping[str, Any], key: str) -> int:
    try:
        return int(result.get(key) or 0)
    except (TypeError, ValueError):
        return 0
