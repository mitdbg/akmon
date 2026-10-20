"""Materialized lineage envelope contract tests."""
from __future__ import annotations

import pytest

from okg.substrate.derived_artifacts import (
    LINEAGE_ENVELOPE_SCHEMA,
    OUTPUT_KINDS,
    ComponentLineage,
    GraphLineage,
    LineageEnvelopeError,
    MaterializedLineageEnvelope,
    SourceRecordLineage,
)


def _hash(label: str) -> str:
    return f"sha256:{label}"


def _source_record(
    record_id: str = "commit:1",
    *,
    source_revision: dict[str, object] | None = None,
) -> SourceRecordLineage:
    return SourceRecordLineage(
        source_id="repo-sync",
        record_id=record_id,
        content_hash=_hash(record_id),
        source_revision=source_revision or {
            "commit": record_id,
            "parents": ["root"],
        },
    )


def _envelope(**overrides: object) -> MaterializedLineageEnvelope:
    values: dict[str, object] = {
        "output_id": "page:repo-summary",
        "output_kind": "generated_page",
        "source_records": [_source_record()],
        "graph": GraphLineage(
            graph_generation_id=42,
            graph_branch="main",
            node_ids=("node:b", "node:a", "node:a"),
            edge_ids=("edge:2", "edge:1"),
        ),
        "catalog_version_id": 7,
        "component": ComponentLineage(
            component_id="repo.summary.renderer",
            component_hash=_hash("component:v1"),
            component_kind="renderer",
            version="v1",
        ),
        "output_digest": _hash("output:v1"),
        "previous_output_digest": _hash("output:v0"),
        "invalidation_scope": "per-page",
        "proposal_ids": ("proposal:2", "proposal:1", "proposal:1"),
        "evidence_ids": ("evidence:1",),
        "decision_ids": ("decision:1",),
        "metadata": {"z": 2, "a": {"nested": True}},
    }
    values.update(overrides)
    return MaterializedLineageEnvelope(**values)  # type: ignore[arg-type]


def test_lineage_envelope_round_trips_normalized_schema() -> None:
    envelope = _envelope()

    encoded = envelope.to_dict()
    round_tripped = MaterializedLineageEnvelope.from_dict(encoded)

    assert encoded["schema"] == LINEAGE_ENVELOPE_SCHEMA
    assert encoded["catalog_version_id"] == "7"
    assert encoded["component"]["component_hash"] == _hash("component:v1")
    assert encoded["invalidation_scope"] == "per-page"
    assert round_tripped.to_dict() == encoded
    assert round_tripped.envelope_hash == envelope.envelope_hash
    assert envelope.envelope_hash.startswith("sha256:")


@pytest.mark.parametrize("output_kind", sorted(OUTPUT_KINDS))
def test_envelope_accepts_all_materialized_output_kinds(
    output_kind: str,
) -> None:
    envelope = _envelope(output_kind=output_kind)

    assert envelope.to_dict()["output_kind"] == output_kind


def test_envelope_rejects_source_record_without_content_hash() -> None:
    encoded = _envelope().to_dict()
    del encoded["source_records"][0]["content_hash"]

    with pytest.raises(LineageEnvelopeError, match="content_hash is required"):
        MaterializedLineageEnvelope.from_dict(encoded)


def test_envelope_rejects_component_without_hash() -> None:
    encoded = _envelope().to_dict()
    del encoded["component"]["component_hash"]

    with pytest.raises(LineageEnvelopeError, match="component_hash is required"):
        MaterializedLineageEnvelope.from_dict(encoded)


def test_envelope_rejects_graph_without_replay_anchor() -> None:
    encoded = _envelope().to_dict()
    encoded["graph"] = {
        "graph_generation_id": None,
        "graph_branch": None,
        "graph_checkpoint_id": None,
        "node_ids": [],
        "edge_ids": [],
    }

    with pytest.raises(LineageEnvelopeError, match="graph requires"):
        MaterializedLineageEnvelope.from_dict(encoded)


def test_envelope_rejects_unknown_invalidation_scope() -> None:
    with pytest.raises(
        LineageEnvelopeError,
        match="invalidation_scope must be one of",
    ):
        _envelope(invalidation_scope="eventually")


def test_dependency_keys_normalize_reverse_index_inputs() -> None:
    envelope = _envelope(
        source_records=[
            _source_record("commit:2"),
            _source_record("commit:1"),
            _source_record("commit:1"),
        ]
    )

    assert envelope.dependency_keys() == {
        "source_record": [
            "repo-sync:commit:1",
            "repo-sync:commit:2",
        ],
        "source_content_hash": [
            "repo-sync:sha256:commit:1",
            "repo-sync:sha256:commit:2",
        ],
        "graph_node": [
            "node:a",
            "node:b",
        ],
        "graph_edge": [
            "edge:1",
            "edge:2",
        ],
        "component": ["repo.summary.renderer"],
        "component_hash": [_hash("component:v1")],
        "proposal": [
            "proposal:1",
            "proposal:2",
        ],
        "evidence": ["evidence:1"],
        "decision": ["decision:1"],
    }


def test_envelope_hash_is_stable_for_nested_mapping_order() -> None:
    left = _envelope(
        source_records=[
            _source_record(
                source_revision={
                    "commit": "commit:1",
                    "metadata": {"b": 2, "a": 1},
                }
            )
        ],
        metadata={"z": {"b": 2, "a": 1}, "a": 1},
    )
    right = _envelope(
        source_records=[
            _source_record(
                source_revision={
                    "metadata": {"a": 1, "b": 2},
                    "commit": "commit:1",
                }
            )
        ],
        metadata={"a": 1, "z": {"a": 1, "b": 2}},
    )

    assert left.envelope_hash == right.envelope_hash
