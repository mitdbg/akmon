"""Operator-report compaction for oversized graph write-sets (#1479)."""
from __future__ import annotations

from okg.substrate.ingest.layer_write_sets import (
    _GRAPH_WRITE_SET_REPORT_ID_LIMIT,
    _compact_large_graph_write_sets,
)


def test_large_internal_write_set_is_summarized_for_operator_report() -> None:
    node_count = _GRAPH_WRITE_SET_REPORT_ID_LIMIT + 1
    report = {
        "results": [{
            "name": "large-catalog",
            "graph_write_set": {
                "schema": "okg.graph_write_set.v1",
                "nodes_by_subtype": {
                    "dataset": [f"dataset:{index}" for index in range(node_count)],
                },
                "node_attr_changes_by_subtype": {
                    "dataset": {"dataset:0": ["label"]},
                },
                "edges_by_type": {},
                "edge_table_available": True,
            },
        }],
    }

    _compact_large_graph_write_sets(report)

    result = report["results"][0]
    assert "graph_write_set" not in result
    assert result["graph_write_set_summary"] == {
        "schema": "okg.graph_write_set.summary.v1",
        "nodes_by_subtype": {"dataset": node_count},
        "edges_by_type": {},
        "node_attr_changes_by_subtype": {"dataset": 1},
        "nodes_total": node_count,
        "edges_total": 0,
        "node_attr_changes_total": 1,
        "edge_table_available": True,
        "materialized_ids": False,
    }


def test_write_set_at_the_limit_keeps_exact_ids_for_diagnostics() -> None:
    payload = {
        "schema": "okg.graph_write_set.v1",
        "nodes_by_subtype": {
            "site": [
                f"site:{index}"
                for index in range(_GRAPH_WRITE_SET_REPORT_ID_LIMIT)
            ],
        },
        "node_attr_changes_by_subtype": {},
        "edges_by_type": {},
        "edge_table_available": True,
    }
    report = {"results": [{"graph_write_set": payload}]}

    _compact_large_graph_write_sets(report)

    assert report["results"][0]["graph_write_set"] is payload
    assert "graph_write_set_summary" not in report["results"][0]


def test_edge_ids_count_toward_the_limit() -> None:
    edge_count = _GRAPH_WRITE_SET_REPORT_ID_LIMIT + 1
    report = {
        "results": [{
            "graph_write_set": {
                "schema": "okg.graph_write_set.v1",
                "nodes_by_subtype": {},
                "node_attr_changes_by_subtype": {},
                "edges_by_type": {"cites": list(range(edge_count))},
                "edge_table_available": False,
            },
        }],
    }

    _compact_large_graph_write_sets(report)

    summary = report["results"][0]["graph_write_set_summary"]
    assert summary["edges_total"] == edge_count
    assert summary["edge_table_available"] is False


def test_report_without_results_is_left_alone() -> None:
    report: dict = {"results": None}

    _compact_large_graph_write_sets(report)

    assert report == {"results": None}
