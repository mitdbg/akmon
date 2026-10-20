from __future__ import annotations

import pytest

from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)
from okg.substrate.source_sync_contract import (
    SourceDeltaBatch,
    SourceSyncContractError,
    extract_stable_record_ids,
    validate_source_delta_batch,
)


def _node(
    record_id: dict[str, str] | None = None,
    *,
    op: str = "I",
) -> NodeFact:
    return NodeFact(
        node_id="doc:a",
        subtype="document",
        attrs={"text": "A"},
        source_record_id=record_id or {"path": "docs/a.md"},
        source_revision={"sha": "abc"},
        op=op,
    )


def _edge(
    record_id: dict[str, str] | None = None,
    *,
    op: str = "I",
) -> EdgeFact:
    return EdgeFact(
        src="doc:a",
        edge_type="contains",
        dst="chunk:a:1",
        source_record_id=record_id or {
            "path": "docs/a.md",
            "edge": "contains:chunk:a:1",
        },
        source_revision={"sha": "abc"},
        op=op,
    )


def test_record_identity_is_stable() -> None:
    node = _node({"z": "last", "a": "first"})
    edge = _edge({"edge": "contains", "path": "docs/a.md"})
    marker = ProgressMarker("docs/a.md", "sha:abc")
    record_set = RecordSet(records={
        "docs/a.md": RecordEmission(
            primary=RecordNode("doc:a", "document"),
        ),
    })

    assert extract_stable_record_ids(node) == (
        '{"a":"first","z":"last"}',
    )
    assert extract_stable_record_ids(edge) == (
        '{"edge":"contains","path":"docs/a.md"}',
    )
    assert extract_stable_record_ids(marker) == ("docs/a.md",)
    assert extract_stable_record_ids(record_set) == ("docs/a.md",)


def test_source_run_normalizes_to_delta_batch() -> None:
    run = SourceRun(
        facts=[
            _node(),
            _edge(),
            ProgressMarker("docs/a.md", "sha:abc"),
        ],
        next_cursor={"cursor": "2"},
        run_mode="scope_incomplete",
        record_set=RecordSet(records={
            "docs/a.md": RecordEmission(
                primary=RecordNode("doc:a", "document"),
            ),
        }),
    )

    batch = SourceDeltaBatch.from_source_run(
        "doc-corpus",
        run,
        previous_frontier={"cursor": "1"},
        requested_scope={"scope_keys": ["docs/a.md"]},
        effective_scope={"scope_keys": ["docs/a.md"]},
        affected_scopes=("docs/a.md",),
    )

    validate_source_delta_batch(batch)
    assert batch.source_id == "doc-corpus"
    assert batch.next_frontier == {"cursor": "2"}
    assert "docs/a.md" in batch.record_ids
    assert batch.affected_scopes == ("docs/a.md",)
    assert batch.to_dict()["progress_marker_count"] == 1


def test_retractions_are_complete() -> None:
    run = SourceRun(
        facts=[
            _node({"path": "docs/a.md"}, op="R"),
            _edge({"path": "docs/a.md", "edge": "contains"}, op="R"),
        ],
        next_cursor={"cursor": "3"},
        run_mode="scope_complete",
    )

    batch = SourceDeltaBatch.from_source_run(
        "doc-corpus",
        run,
        previous_frontier={"cursor": "2"},
        requested_scope={"scope_keys": ["docs/a.md"]},
        effective_scope={"scope_keys": ["docs/a.md"]},
        affected_scopes=("docs/a.md",),
    )

    validate_source_delta_batch(batch)
    assert batch.retracted_record_ids == (
        '{"path":"docs/a.md"}',
        '{"edge":"contains","path":"docs/a.md"}',
    )


def test_validation_rejects_missing_record_identity() -> None:
    run = SourceRun(
        facts=[
            NodeFact(
                node_id="doc:a",
                subtype="document",
                attrs={},
                source_record_id={},
                source_revision={"sha": "abc"},
            ),
        ],
        next_cursor={"cursor": "2"},
        run_mode="scope_incomplete",
    )
    batch = SourceDeltaBatch.from_source_run(
        "doc-corpus",
        run,
        affected_scopes=("docs/a.md",),
    )

    with pytest.raises(SourceSyncContractError, match="source_record_id"):
        validate_source_delta_batch(batch)


def test_validation_requires_frontier_or_progress_state() -> None:
    batch = SourceDeltaBatch(
        source_id="doc-corpus",
        facts=(_node(),),
        changed_record_ids=('{"path":"docs/a.md"}',),
        affected_scopes=("docs/a.md",),
        requested_mode="scope_incomplete",
        effective_mode="scope_incomplete",
    )

    with pytest.raises(SourceSyncContractError, match="next_frontier"):
        validate_source_delta_batch(batch)
