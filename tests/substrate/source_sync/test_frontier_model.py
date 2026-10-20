from __future__ import annotations

import pytest

from okg.substrate.library.sources.base import NodeFact, SourceRun
from okg.substrate.source_sync import source_sync_desired_frontier_state
from okg.substrate.source_sync_contract import (
    SourceDeltaBatch,
    SourceFallbackEvidence,
    SourceSyncContractError,
    scope_or_mode_widened,
    validate_source_delta_batch,
)


def _node(record_id: str = "docs/a.md") -> NodeFact:
    return NodeFact(
        node_id=f"doc:{record_id}",
        subtype="document",
        attrs={"path": record_id},
        source_record_id={"path": record_id},
        source_revision={"sha": "abc"},
    )


def test_frontier_does_not_advance_past_unapplied_delta() -> None:
    frontier = source_sync_desired_frontier_state(
        source_name="doc-corpus",
        run_mode="scope_complete",
        workflow_id="wf-running",
        workflow_status="PENDING",
        started_age_seconds=7,
    )

    assert frontier["completed_scope_run_mode"] is True
    assert frontier["completed"] is False
    assert frontier["completion_state"] == "not_completed"
    assert frontier["completion_basis"] == (
        "dbos_wakeup_is_not_completion_evidence"
    )
    assert frontier["completion_requires"] == [
        "source_progress_or_source_watermark_completed_scope",
        "chronos_apply",
        "published_generation_visible_to_read_side",
    ]


def test_delta_replay_is_idempotent() -> None:
    run = SourceRun(
        facts=[_node()],
        next_cursor={"cursor": "2"},
        run_mode="scope_incomplete",
    )
    kwargs = {
        "previous_frontier": {"cursor": "1"},
        "requested_scope": {"scope_keys": ["docs/a.md"]},
        "effective_scope": {"scope_keys": ["docs/a.md"]},
        "affected_scopes": ("docs/a.md",),
    }

    first = validate_source_delta_batch(
        SourceDeltaBatch.from_source_run("doc-corpus", run, **kwargs)
    )
    replay = validate_source_delta_batch(
        SourceDeltaBatch.from_source_run("doc-corpus", run, **kwargs)
    )

    assert replay.to_dict() == first.to_dict()
    assert replay.record_ids == first.record_ids


def test_scope_widening_is_explicit() -> None:
    silently_widened = SourceDeltaBatch(
        source_id="doc-corpus",
        next_frontier={"cursor": "3"},
        facts=(_node(),),
        changed_record_ids=('{"path":"docs/a.md"}',),
        affected_scopes=("docs/a.md", "docs/b.md"),
        requested_scope={"scope_keys": ["docs/a.md"]},
        effective_scope={"scope_keys": ["docs/a.md", "docs/b.md"]},
        requested_mode="scope_incomplete",
        effective_mode="scope_complete",
    )

    assert scope_or_mode_widened(silently_widened) is True
    with pytest.raises(SourceSyncContractError, match="without fallback"):
        validate_source_delta_batch(silently_widened)

    fallback = SourceFallbackEvidence(
        reason="delete event requires complete scope proof",
        requested_mode="scope_incomplete",
        effective_mode="scope_complete",
        requested_scope={"scope_keys": ["docs/a.md"]},
        effective_scope={"scope_keys": ["docs/a.md", "docs/b.md"]},
        measured_input_size=2,
        affected_scopes=("docs/a.md", "docs/b.md"),
        readiness_impact="bounded widening for delete correctness",
        evidence_refs=("event:doc-corpus:delete",),
    )
    explicit_widening = SourceDeltaBatch(
        source_id="doc-corpus",
        next_frontier={"cursor": "3"},
        facts=(_node(),),
        changed_record_ids=('{"path":"docs/a.md"}',),
        affected_scopes=("docs/a.md", "docs/b.md"),
        requested_scope={"scope_keys": ["docs/a.md"]},
        effective_scope={"scope_keys": ["docs/a.md", "docs/b.md"]},
        requested_mode="scope_incomplete",
        effective_mode="scope_complete",
        fallback=fallback,
    )

    validate_source_delta_batch(explicit_widening)
