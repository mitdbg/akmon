"""Change-frontier derivation for deltaflow-publish-projection.

These prove the pure mapping from real change inputs (SourceDeltaBatch
record ids, Chronos graph write-set, catalog component-hash diff) to the
`LineageInvalidation` list the derived-artifact scheduler consumes. No
DB: the scheduler's reverse-index lookup is what decides dirty/reuse;
this layer only names the changed dependency keys.
"""
from __future__ import annotations

import pytest

from okg.substrate.derived_artifacts.change_frontier import (
    ChangeFrontier,
    build_change_frontier,
    invalidations_from_delta_batch,
    invalidations_from_projection_deltas,
    lens_overlay_invalidations,
)
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.source_sync_contract import SourceDeltaAccumulator


def _node(record_id: dict[str, str], subtype: str = "document") -> NodeFact:
    return NodeFact(
        node_id="doc:a",
        subtype=subtype,
        attrs={},
        source_record_id=record_id,
        source_revision={"sha": "abc"},
    )


def test_delta_batch_maps_changed_records_to_source_record_keys() -> None:
    batch = {
        "contract": "valid",
        "source_id": "git-files",
        "changed_record_ids": ['{"path":"docs/a.md"}', '{"path":"docs/b.md"}'],
    }
    invalidations, skipped = invalidations_from_delta_batch(batch)
    refs = {(i.dependency_kind, i.dependency_ref) for i in invalidations}
    assert refs == {
        ("source_record", 'git-files:{"path":"docs/a.md"}'),
        ("source_record", 'git-files:{"path":"docs/b.md"}'),
    }
    assert skipped is None


def test_invalid_contract_skips_source_with_no_invalidations() -> None:
    # An unavailable/invalid contract has an unknown scope: it must force
    # the source's outputs full, not be treated as clean.
    for status in ("invalid", "unavailable"):
        batch = {"contract": status, "source_id": "git-files", "reason": "x"}
        invalidations, skipped = invalidations_from_delta_batch(batch)
        assert invalidations == []
        assert skipped == "git-files"


def test_typed_delta_batch_from_accumulator_is_accepted() -> None:
    # End-to-end with the task-(a) accumulator: a real SourceDeltaBatch
    # object (to_dict has no `contract` field, treated as valid).
    acc = SourceDeltaAccumulator()
    acc.observe(_node({"path": "docs/a.md"}))
    batch = acc.build(
        "git-files",
        next_frontier={"cursor": "2"},
        run_mode="scope_complete",
    )
    invalidations, skipped = invalidations_from_delta_batch(batch)
    assert [(i.dependency_kind, i.dependency_ref) for i in invalidations] == [
        ("source_record", 'git-files:{"path":"docs/a.md"}'),
    ]
    assert skipped is None


def test_build_change_frontier_unifies_and_counts() -> None:
    frontier = build_change_frontier(
        source_delta_batches=[
            {
                "contract": "valid",
                "source_id": "git-files",
                "changed_record_ids": ['{"path":"docs/a.md"}'],
            },
        ],
        graph_node_ids=["doc:a", "chunk:a:1"],
        graph_edge_ids=["edge:1"],
        changed_component_hashes=["sha256:viewhash"],
        source_content_hashes={"git-files": ["sha256:content"]},
    )
    assert isinstance(frontier, ChangeFrontier)
    assert not frontier.is_empty
    assert frontier.key_counts_by_kind == {
        "source_record": 1,
        "source_content_hash": 1,
        "graph_node": 2,
        "graph_edge": 1,
        "component_hash": 1,
    }
    assert frontier.to_dict()["invalidation_count"] == 6


def test_lens_overlay_invalidations_emit_decorated_graph_ids() -> None:
    invalidations = lens_overlay_invalidations([
        {"source_record_id": {"node_id": "doc:a"}},
        {"source_delta": {"source_record_id": {"edge_id": "edge:a"}}},
        {
            "source_record_id": {
                "node_id": "doc:a",
                "edge_id": "edge:a",
            }
        },
        {"source_record_id": {"row_key": "not-a-graph-id"}},
    ])

    assert [
        (item.dependency_kind, item.dependency_ref, item.dirty_reason)
        for item in invalidations
    ] == [
        ("graph_node", "doc:a", "lens_overlay_committed"),
        ("graph_edge", "edge:a", "lens_overlay_committed"),
    ]


def test_projection_delta_payload_rehydrates_invalidations() -> None:
    invalidations = invalidations_from_projection_deltas({
        "lineage_invalidations": [
            {
                "dependency_kind": "graph_node",
                "dependency_ref": "doc:a",
                "dirty_reason": "lens_overlay_committed",
            },
            {
                "dependency_kind": "decision",
                "dependency_ref": "decision:1",
                "dirty_reason": "decision_changed",
            },
        ]
    })

    assert [
        (item.dependency_kind, item.dependency_ref, item.dirty_reason)
        for item in invalidations
    ] == [
        ("graph_node", "doc:a", "lens_overlay_committed"),
        ("decision", "decision:1", "decision_changed"),
    ]


def test_projection_delta_payload_rejects_missing_ref() -> None:
    with pytest.raises(TypeError, match="dependency_kind and dependency_ref"):
        invalidations_from_projection_deltas({
            "lineage_invalidations": [
                {
                    "dependency_kind": "graph_node",
                    "dirty_reason": "lens_overlay_committed",
                }
            ]
        })


def test_build_change_frontier_accepts_lens_overlay_rows() -> None:
    frontier = build_change_frontier(
        graph_node_ids=["doc:a"],
        lens_overlay_rows=[
            {"source_record_id": {"node_id": "doc:a"}},
            {"source_record_id": {"edge_id": "edge:a"}},
        ],
    )

    assert [
        (item.dependency_kind, item.dependency_ref)
        for item in frontier.invalidations
    ] == [
        ("graph_node", "doc:a"),
        ("graph_edge", "edge:a"),
    ]
    assert frontier.key_counts_by_kind == {"graph_node": 1, "graph_edge": 1}


def test_build_change_frontier_deduplicates_across_inputs() -> None:
    # The same node id arriving from two batches collapses to one key.
    frontier = build_change_frontier(
        graph_node_ids=["doc:a", "doc:a", "doc:b"],
    )
    refs = [i.dependency_ref for i in frontier.invalidations]
    assert refs == ["doc:a", "doc:b"]
    assert frontier.key_counts_by_kind == {"graph_node": 2}


def test_empty_inputs_yield_empty_frontier() -> None:
    frontier = build_change_frontier()
    assert frontier.is_empty
    assert frontier.key_counts_by_kind == {}
    assert frontier.to_dict()["invalidation_count"] == 0


def test_skipped_sources_are_reported() -> None:
    frontier = build_change_frontier(
        source_delta_batches=[
            {"contract": "unavailable", "source_id": "pact", "reason": "x"},
            {
                "contract": "valid",
                "source_id": "git-files",
                "changed_record_ids": ['{"path":"a"}'],
            },
        ],
    )
    assert frontier.skipped_source_ids == ("pact",)
    assert frontier.key_counts_by_kind == {"source_record": 1}


def test_unindexed_kind_is_rejected() -> None:
    from okg.substrate.derived_artifacts.change_frontier import _invalidation

    with pytest.raises(ValueError, match="not reverse-indexed"):
        _invalidation("not_a_kind", "ref", "reason")
