"""Output-side lineage envelope construction for deltaflow-publish-projection.

These prove the pure assembly of `MaterializedLineageEnvelope`s from real
output provenance, the hash discipline the envelope requires, and — the
load-bearing property — that an envelope's reverse-index keys match the
change-frontier keys for the same record/node/component, so a change can
actually reach the output it dirties.
"""
from __future__ import annotations

import pytest

from okg.substrate.derived_artifacts.change_frontier import (
    build_change_frontier,
    component_hash_invalidations,
    decision_invalidations,
    evidence_invalidations,
    graph_node_invalidations,
    proposal_invalidations,
    source_record_invalidations,
)
from okg.substrate.derived_artifacts.lineage import LineageEnvelopeError
from okg.substrate.derived_artifacts.output_lineage import (
    as_hash,
    build_output_lineage_envelope,
    source_record_lineage,
    source_records_from_node_rows,
)
from okg.substrate.source_sync_contract import canonical_record_id


def _envelope(**overrides):
    kwargs = dict(
        output_id="page:docs/a.md",
        output_kind="native_view",
        source_records=[source_record_lineage("git-files", {"path": "docs/a.md"})],
        component_id="view:test",
        component_hash="viewhash",
        component_kind="view_component",
        output_digest="outdigest",
        catalog_version_id=1,
        graph_generation_id=10,
        node_ids=["doc:a"],
    )
    kwargs.update(overrides)
    return build_output_lineage_envelope(**kwargs)


def test_as_hash_prefixes_bare_digest_and_passes_prefixed() -> None:
    assert as_hash("abc123") == "sha256:abc123"
    assert as_hash("sha256:abc123") == "sha256:abc123"
    assert as_hash("dep:xyz") == "dep:xyz"
    with pytest.raises(ValueError):
        as_hash("")


def test_source_record_lineage_canonicalizes_and_derives_content_hash() -> None:
    record = source_record_lineage("git-files", {"z": "2", "a": "1"})
    assert record.record_id == '{"a":"1","z":"2"}'
    assert record.content_hash.startswith("sha256:")
    # Same identity is deterministic.
    again = source_record_lineage("git-files", {"a": "1", "z": "2"})
    assert again.content_hash == record.content_hash


def test_source_records_from_node_rows_dedupes_and_skips_incomplete() -> None:
    rows = [
        {"source": "git-files", "source_record_id": {"path": "a"},
         "source_revision": {"sha": "1"}},
        {"source": "git-files", "source_record_id": {"path": "a"},
         "source_revision": {"sha": "1"}},  # dup
        {"source": "git-files", "source_record_id": {"path": "b"}},
        {"source": None, "source_record_id": {"path": "c"}},  # skipped
        {"source": "git-files", "source_record_id": None},  # skipped
    ]
    records = source_records_from_node_rows(rows)
    assert [(r.source_id, r.record_id) for r in records] == [
        ("git-files", '{"path":"a"}'),
        ("git-files", '{"path":"b"}'),
    ]


def test_build_envelope_applies_hash_discipline() -> None:
    envelope = _envelope()
    assert envelope.component.component_hash == "sha256:viewhash"
    assert envelope.output_digest == "sha256:outdigest"
    assert envelope.previous_output_digest is None


def test_build_envelope_rejects_unknown_output_kind() -> None:
    with pytest.raises(LineageEnvelopeError, match="output_kind"):
        _envelope(output_kind="not_a_kind")


def test_build_envelope_requires_a_source_record() -> None:
    with pytest.raises(LineageEnvelopeError, match="source_records"):
        _envelope(source_records=[])


def test_envelope_dependency_keys_cover_record_node_and_component() -> None:
    keys = _envelope().dependency_keys()
    assert keys["source_record"] == ['git-files:{"path":"docs/a.md"}']
    assert keys["graph_node"] == ["doc:a"]
    assert keys["component_hash"] == ["sha256:viewhash"]
    assert keys["component"] == ["view:test"]


def test_frontier_and_envelope_keys_match_for_same_record() -> None:
    # THE correctness property: a source-record change on the frontier and
    # the envelope that declared that record must produce the SAME
    # reverse-index ref, or a real change could never dirty the output.
    raw_record_id = {"path": "docs/a.md"}
    # Frontier side: SourceDeltaBatch.changed_record_ids carry the
    # canonical record id; the frontier key is {source_id}:{canonical}.
    frontier_key = source_record_invalidations(
        "git-files", [canonical_record_id(raw_record_id)],
    )[0].dependency_ref
    # Envelope side: the envelope declares the same record.
    envelope = _envelope(
        source_records=[source_record_lineage("git-files", raw_record_id)],
    )
    envelope_key = envelope.dependency_keys()["source_record"][0]
    assert frontier_key == envelope_key == 'git-files:{"path":"docs/a.md"}'


def test_relation_envelope_keys_on_its_own_component_hash() -> None:
    # A derived-relation table records its own component (compute) hash, so
    # a change to that relation's definition finds only its outputs.
    envelope = _envelope(
        output_id="view-relation:v:relation",
        output_kind="derived_relation",
        component_id="view-relation:v:relation",
        component_hash="relationhash",
        component_kind="renderer",
        invalidation_scope="per-scope",
    )
    keys = envelope.dependency_keys()
    assert keys["component_hash"] == ["sha256:relationhash"]
    assert keys["graph_node"] == ["doc:a"]


def test_frontier_and_envelope_keys_match_for_node_and_component() -> None:
    envelope = _envelope(node_ids=["doc:a"], component_hash="viewhash")
    keys = envelope.dependency_keys()
    assert graph_node_invalidations(["doc:a"])[0].dependency_ref == (
        keys["graph_node"][0]
    )
    assert component_hash_invalidations(["sha256:viewhash"])[0].dependency_ref == (
        keys["component_hash"][0]
    )


def test_frontier_builds_decision_proposal_and_evidence_inputs() -> None:
    assert proposal_invalidations(["proposal:1"])[0].dependency_ref == "proposal:1"
    assert evidence_invalidations(["evidence:1"])[0].dependency_ref == "evidence:1"
    assert decision_invalidations(["decision:1"])[0].dependency_ref == "decision:1"

    frontier = build_change_frontier(
        graph_node_ids=["doc:a"],
        proposal_ids=["proposal:1", "proposal:1"],
        evidence_ids=["evidence:1"],
        decision_ids=["decision:1"],
    )

    assert [
        (item.dependency_kind, item.dependency_ref)
        for item in frontier.invalidations
    ] == [
        ("graph_node", "doc:a"),
        ("proposal", "proposal:1"),
        ("evidence", "evidence:1"),
        ("decision", "decision:1"),
    ]
    assert frontier.key_counts_by_kind == {
        "graph_node": 1,
        "proposal": 1,
        "evidence": 1,
        "decision": 1,
    }
