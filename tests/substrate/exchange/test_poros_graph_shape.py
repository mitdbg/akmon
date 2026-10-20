from __future__ import annotations

import pytest

from okg.substrate.exchange.graph_shape import (
    ROOT_PATTERN_VOCABULARY_VERSION,
    classify_graph_body,
    classify_graph_shape,
    edge_narrowings_for_body,
    root_subtypes_for_body,
)


@pytest.mark.parametrize(
    ("root_subtype", "expected_pattern", "expected_profile"),
    [
        ("claim", "claim", "claim"),
        ("hypothesis", "claim", "claim"),
        ("context_packet", "context", "context"),
        ("document", "context", "context"),
        ("trace", "context", "context"),
        ("dataset", "dataset_reference", "dataset_reference"),
        ("dataset_reference", "dataset_reference", "dataset_reference"),
        ("proposal", "proposal", "proposal"),
        ("open_spec_change", "proposal", "proposal"),
        ("domain_entity", "fact_bundle", "fact_bundle"),
    ],
)
def test_classify_graph_shape_uses_core_root_pattern_vocabulary(
    root_subtype: str,
    expected_pattern: str,
    expected_profile: str,
) -> None:
    shape = classify_graph_shape([root_subtype])

    assert shape.root_pattern == expected_pattern
    assert shape.graph_profile == expected_profile
    assert shape.node_subtypes == (root_subtype,)
    assert shape.root_subtypes == (root_subtype,)
    assert shape.vocabulary_version == ROOT_PATTERN_VOCABULARY_VERSION


def test_classify_graph_shape_marks_empty_roots_unknown() -> None:
    shape = classify_graph_shape([])

    assert shape.root_pattern == "unknown"
    assert shape.graph_profile == "unknown"
    assert shape.to_resource_fields()["root_pattern_vocabulary"] == (
        ROOT_PATTERN_VOCABULARY_VERSION
    )


def test_classify_graph_shape_allows_deployment_subtype_extensions() -> None:
    shape = classify_graph_shape(
        ["experiment_run_context"],
        extensions={"context": ["experiment_run_context"]},
    )

    assert shape.root_pattern == "context"
    assert shape.graph_profile == "context"


def test_classify_graph_shape_allows_deployment_root_pattern_extensions() -> None:
    shape = classify_graph_shape(
        ["instrument_calibration"],
        extensions={"calibration": ["instrument_calibration"]},
    )

    assert shape.root_pattern == "calibration"
    assert shape.graph_profile == "calibration"


def test_classify_graph_shape_proposal_root_dominates_mixed_graphs() -> None:
    shape = classify_graph_shape(["context_packet", "proposal"])

    assert shape.root_pattern == "proposal"
    assert shape.graph_profile == "proposal"
    assert shape.root_subtypes == ("context_packet", "proposal")


def test_classify_graph_body_extracts_roots_and_edge_narrowings() -> None:
    body = {
        "root_node_ids": ["root"],
        "nodes": [
            {"node_id": "root", "subtype": "context_packet"},
            {"node_id": "claim-1", "subtype": "claim"},
        ],
        "edges": [
            {
                "src": "root",
                "edge_type": "mentions",
                "dst": "claim-1",
            }
        ],
    }

    shape = classify_graph_body(body)

    assert root_subtypes_for_body(body) == ("context_packet",)
    assert edge_narrowings_for_body(body) == (
        {
            "src_subtype": "context_packet",
            "edge_type": "mentions",
            "dst_subtype": "claim",
        },
    )
    assert shape.to_resource_fields() == {
        "graph_profile": "context",
        "root_pattern": "context",
        "node_subtypes": ["claim", "context_packet"],
        "root_subtypes": ["context_packet"],
        "edge_narrowings": [
            {
                "src_subtype": "context_packet",
                "edge_type": "mentions",
                "dst_subtype": "claim",
            }
        ],
        "root_pattern_vocabulary": ROOT_PATTERN_VOCABULARY_VERSION,
    }


def test_root_subtypes_without_explicit_roots_use_whole_graph_shape() -> None:
    body = {
        "nodes": [
            {"node_id": "a", "subtype": "context_packet"},
            {"node_id": "b", "subtype": "dataset"},
        ],
        "edges": [],
    }

    assert root_subtypes_for_body(body) == ("context_packet", "dataset")
    assert classify_graph_body(body).root_pattern == "fact_bundle"
