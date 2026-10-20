"""Which generation the interval backfill precondition is allowed to check.

``require_effective_interval_backfill_precondition`` equates a published
generation's checkpoint node/edge counts with the intervals open at that
generation's ordinal. That equality only holds for a generation that folded
itself into the interval table. An intermediate publish
(``cycle(native_view_refresh_final=False)``) DELIBERATELY defers its
effective-attribute snapshot, folds nothing, and so publishes counts that
legitimately run ahead of the interval rows -- which made the next publish
refuse a healthy deployment and hand the operator a repair command with nothing
to repair (issue #1927).

``resolve_interval_precondition_anchor`` steps back over exactly those
deliberately deferred generations. These cases pin that it moves for nothing
else, so every unproven state stays fail-closed.
"""
from __future__ import annotations

from typing import Any

import pytest

from okg.substrate.attributes import (
    DEFERRED_INTERVAL_SYNC_REASON,
    resolve_interval_precondition_anchor,
)


class _Checkpoint:
    """The two attributes the resolver reads off a Chronos checkpoint."""

    def __init__(self, checkpoint_id: str, metadata: dict[str, Any]) -> None:
        self.checkpoint_id = checkpoint_id
        self.metadata = metadata


def _synced(checkpoint_id: str, predecessor: str | None = None) -> _Checkpoint:
    return _Checkpoint(
        checkpoint_id,
        {
            "predecessor_generation_id": predecessor,
            "effective_interval_view_sync": {"ran": True, "reason": None},
        },
    )


def _deferred(checkpoint_id: str, predecessor: str | None) -> _Checkpoint:
    return _Checkpoint(
        checkpoint_id,
        {
            "predecessor_generation_id": predecessor,
            "effective_interval_view_sync": {
                "ran": False,
                "reason": DEFERRED_INTERVAL_SYNC_REASON,
            },
        },
    )


def _loader(*checkpoints: _Checkpoint):
    index = {c.checkpoint_id: c for c in checkpoints}
    seen: list[str] = []

    def load(generation_id: str) -> _Checkpoint | None:
        seen.append(generation_id)
        return index.get(generation_id)

    load.seen = seen  # type: ignore[attr-defined]
    return load


def test_generation_that_synced_is_its_own_anchor() -> None:
    gen = _synced("gen-b", predecessor="gen-a")
    load = _loader(_synced("gen-a"), gen)

    resolved = resolve_interval_precondition_anchor(gen, load_checkpoint=load)

    assert resolved["checkpoint"] is gen
    assert resolved["steps"] == 0
    assert resolved["reason"] == "interval_sync_ran"
    assert load.seen == []


def test_deferred_generation_anchors_on_the_last_one_that_synced() -> None:
    anchor = _synced("gen-a")
    load = _loader(
        anchor,
        _deferred("gen-b", predecessor="gen-a"),
        _deferred("gen-c", predecessor="gen-b"),
    )

    resolved = resolve_interval_precondition_anchor(
        _deferred("gen-c", predecessor="gen-b"), load_checkpoint=load
    )

    assert resolved["checkpoint"] is anchor
    assert resolved["steps"] == 2
    assert resolved["reason"] == "interval_sync_ran"


def test_unmarked_generation_does_not_move_the_anchor() -> None:
    """A checkpoint published before the marker existed stays fail-closed."""
    gen = _Checkpoint("gen-legacy", {"predecessor_generation_id": "gen-a"})
    load = _loader(_synced("gen-a"), gen)

    resolved = resolve_interval_precondition_anchor(gen, load_checkpoint=load)

    assert resolved["checkpoint"] is gen
    assert resolved["reason"] == "sync_state_unrecorded"
    assert load.seen == []


@pytest.mark.parametrize(
    "reason",
    ["blocked", "interval_representation_not_enabled", "something_new"],
)
def test_only_the_deliberate_deferral_reason_moves_the_anchor(reason: str) -> None:
    gen = _Checkpoint(
        "gen-b",
        {
            "predecessor_generation_id": "gen-a",
            "effective_interval_view_sync": {"ran": False, "reason": reason},
        },
    )
    load = _loader(_synced("gen-a"), gen)

    resolved = resolve_interval_precondition_anchor(gen, load_checkpoint=load)

    assert resolved["checkpoint"] is gen
    assert resolved["reason"] == reason
    assert load.seen == []


def test_deferred_generation_without_a_predecessor_stays_put() -> None:
    """The branch's FIRST publish was intermediate, so nothing ever synced.

    That is indistinguishable from the unbackfilled deployment issue #774's
    refusal exists to catch, so the caller keeps refusing -- and the refusal's
    own fix hint works here: the repair rebuilds a full snapshot from the
    published graph and reconciles the intervals from it.
    """
    gen = _deferred("gen-a", predecessor=None)
    load = _loader(gen)

    resolved = resolve_interval_precondition_anchor(gen, load_checkpoint=load)

    assert resolved["checkpoint"] is gen
    assert resolved["steps"] == 0
    assert resolved["reason"] == "deferred_without_recorded_predecessor"


def test_unreadable_predecessor_stays_put() -> None:
    gen = _deferred("gen-b", predecessor="gen-a")

    def load(generation_id: str) -> Any:
        raise RuntimeError("checkpoint compacted")

    resolved = resolve_interval_precondition_anchor(gen, load_checkpoint=load)

    assert resolved["checkpoint"] is gen
    assert resolved["reason"] == "deferred_predecessor_unavailable"


def test_missing_predecessor_stays_put() -> None:
    gen = _deferred("gen-b", predecessor="gen-a")
    load = _loader(gen)

    resolved = resolve_interval_precondition_anchor(gen, load_checkpoint=load)

    assert resolved["checkpoint"] is gen
    assert resolved["reason"] == "deferred_predecessor_unavailable"


def test_an_unbounded_deferred_chain_hands_back_the_callers_own_checkpoint() -> None:
    """Not the generation the walk stopped on, 3 steps back.

    An unbounded deferred chain is an unproven state; anchoring on a generation
    partway down it would check counts nothing in between has proven. The caller
    keeps its original comparison instead.
    """
    chain = [_deferred(f"gen-{i}", predecessor=f"gen-{i - 1}") for i in range(1, 12)]
    chain.insert(0, _deferred("gen-0", predecessor=None))
    load = _loader(*chain)
    start = chain[-1]

    resolved = resolve_interval_precondition_anchor(
        start, load_checkpoint=load, max_steps=3
    )

    assert resolved["checkpoint"] is start
    assert resolved["checkpoint"].checkpoint_id == "gen-11"
    assert resolved["steps"] == 3
    assert resolved["reason"] == "deferred_chain_longer_than_max_steps"


def test_compaction_and_rollover_drop_the_sync_marker_and_predecessor() -> None:
    """The real filter both `compact_branch_from_generation` and
    `rollover_branch_from_generation` build their new checkpoint's metadata
    through (generations.py lines 91 and 199).
    """
    from okg.substrate.graph_store_internal.generations import (
        ChronosGraphGenerationLifecycleMixin as _Lifecycle,
    )

    source = {
        "publisher": "chronos_native",
        "node_count": 2,
        "edge_count": 0,
        "predecessor_generation_id": "gen-a",
        "effective_interval_view_sync": {
            "ran": False,
            "reason": DEFERRED_INTERVAL_SYNC_REASON,
        },
        "invariant_summary": {"kept": True},
    }

    carried = _Lifecycle._compaction_source_metadata(_Lifecycle(), source)

    assert "effective_interval_view_sync" not in carried
    assert "predecessor_generation_id" not in carried
    # Unrelated metadata is still carried forward, so this is a targeted drop
    # rather than a blanket one.
    assert carried["invariant_summary"] == {"kept": True}


def test_a_compacted_checkpoint_carries_no_marker_to_step_over() -> None:
    """Compaction must not hand a new checkpoint another generation's answer.

    ``_COMPACTION_SOURCE_METADATA_IGNORED`` drops the sync marker and the
    predecessor pointer, so a compacted checkpoint built from a DEFERRED
    generation reads as unrecorded and the walk stops on it. Were the marker
    inherited, the walk would step past a checkpoint whose interval rows nothing
    has proven, straight onto pre-compaction history.
    """
    from okg.substrate.graph_store_internal.generations import (
        ChronosGraphGenerationLifecycleMixin,
    )

    ignored = ChronosGraphGenerationLifecycleMixin._COMPACTION_SOURCE_METADATA_IGNORED
    assert "effective_interval_view_sync" in ignored
    assert "predecessor_generation_id" in ignored

    deferred_source = _deferred("gen-b", predecessor="gen-a")
    carried = {
        key: value
        for key, value in deferred_source.metadata.items()
        if key not in ignored
    }
    compacted = _Checkpoint(
        "gen-compacted",
        {**carried, "publisher": "chronos_compaction",
         "compacted_from_generation": "gen-b"},
    )
    load = _loader(_synced("gen-a"), deferred_source, compacted)

    resolved = resolve_interval_precondition_anchor(compacted, load_checkpoint=load)

    assert resolved["checkpoint"] is compacted
    assert resolved["steps"] == 0
    assert resolved["reason"] == "sync_state_unrecorded"
    assert load.seen == []
