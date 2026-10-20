from __future__ import annotations

import uuid
from collections.abc import Iterator

import pytest

from okg.substrate.fact_batch import FactBatchHeader
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceHealth,
    SourceRun,
)
from okg.substrate.source_fact_batch import (
    SourceRunFactBatchLowerer,
    SourceRunLoweringError,
)
from okg.substrate.source_sync_contract import SourceDelta, SourceDeltaBatch


class SinglePassFacts:
    """Fact iterable that detects eager sizing/materialization and replay."""

    def __init__(self, items: list[object]) -> None:
        self.items = items
        self.iterations = 0

    def __len__(self) -> int:
        raise AssertionError("the fact iterator must not be sized")

    def __iter__(self) -> Iterator[object]:
        self.iterations += 1
        if self.iterations > 1:
            raise AssertionError("the fact iterator must be consumed once")
        yield from self.items


def _node(record: str, *, op: str = "I") -> NodeFact:
    return NodeFact(
        node_id=f"doc:{record}",
        subtype="document",
        attrs={"record": record},
        source_record_id={"record": record},
        source_revision={"sha": record},
        op=op,
    )


def test_source_delta_batch_is_only_a_compatibility_alias() -> None:
    assert SourceDeltaBatch is SourceDelta
    assert SourceDelta.__name__ == "SourceDelta"


def test_changed_run_streams_bounded_chunks_with_no_fact_or_state_drift() -> None:
    items: list[object] = [
        _node("a"),
        ProgressMarker("a", "sha:a"),
        _edge("a"),
        _node("b"),
        ProgressMarker("b", "sha:b"),
    ]
    stream = SinglePassFacts(items)
    record_set = _record_set("a", "b")
    health = SourceHealth(status="ok", record_count=2, reason="fixture clean")
    run = SourceRun(
        facts=stream,
        completed_scope=True,
        next_cursor={"cursor": "2", "probe": "changed"},
        run_mode="cursor",
        record_set=record_set,
        record_set_replacement_keys=("a", "b"),
        health=health,
    )
    lowerer = _lowerer(run, max_facts_per_chunk=2, max_markers_per_chunk=1)

    chunks = list(lowerer.iter_chunks())

    assert stream.iterations == 1
    assert [chunk.ordinal for chunk in chunks] == list(range(len(chunks)))
    assert all(len(chunk.facts) <= 2 for chunk in chunks)
    assert all(len(chunk.progress_markers) <= 1 for chunk in chunks)
    assert _flatten(chunks) == items
    assert lowerer.terminal.next_cursor == run.next_cursor
    assert lowerer.terminal.record_set is record_set
    assert lowerer.terminal.record_set_replacement_keys == ("a", "b")
    assert lowerer.terminal.health is health
    assert lowerer.terminal.health.reason == "fixture clean"
    assert lowerer.terminal.completion == "delta"
    assert lowerer.terminal.source_delta.changed_record_ids
    assert lowerer.terminal.state_advancement_preconditions_met is True


def test_progress_marker_stays_subordinate_to_its_preceding_fact_chunk() -> None:
    run = SourceRun(
        facts=SinglePassFacts(
            [
                _node("a"),
                ProgressMarker("a", "sha:a"),
            ]
        ),
        completed_scope=True,
        next_cursor={"cursor": "a"},
    )

    chunks = list(_lowerer(run).iter_chunks())

    assert len(chunks) == 1
    assert chunks[0].facts[0].source_record_id == {"record": "a"}
    assert chunks[0].progress_markers == (ProgressMarker("a", "sha:a"),)


@pytest.mark.parametrize(
    ("run", "completion", "zero_kind", "state_ready"),
    [
        (
            SourceRun(
                facts=SinglePassFacts([ProgressMarker("a", "sha:a")]),
                completed_scope=True,
                next_cursor={"probe": "unchanged"},
                run_mode="no_change",
                health=SourceHealth(status="ok"),
            ),
            "delta",
            "unchanged",
            True,
        ),
        (
            SourceRun(
                facts=SinglePassFacts([]),
                completed_scope=True,
                next_cursor={"snapshot": "empty"},
                run_mode="reconcile",
                record_set=RecordSet(),
                health=SourceHealth(status="ok", record_count=0),
            ),
            "snapshot",
            "authoritative_empty_snapshot",
            True,
        ),
        (
            SourceRun(
                facts=SinglePassFacts([_node("partial")]),
                completed_scope=False,
                next_cursor=None,
                run_mode="cursor",
                health=SourceHealth(status="ok"),
            ),
            "partial",
            None,
            False,
        ),
        (
            SourceRun(
                facts=SinglePassFacts([]),
                completed_scope=True,
                next_cursor=None,
                health=SourceHealth(
                    status="acquire_stale", reason="remote unavailable"
                ),
            ),
            "partial",
            "degraded",
            False,
        ),
        (
            SourceRun(
                facts=SinglePassFacts([]),
                completed_scope=True,
                next_cursor=None,
                health=SourceHealth(status="auth_failed", reason="token rejected"),
            ),
            "partial",
            "failed",
            False,
        ),
        (
            SourceRun(
                facts=SinglePassFacts([]),
                completed_scope=True,
                next_cursor=None,
                health=SourceHealth(
                    status="acquire_failed", reason="checkout unavailable"
                ),
            ),
            "partial",
            "failed",
            False,
        ),
        (
            SourceRun(
                facts=SinglePassFacts([]),
                completed_scope=False,
                next_cursor=None,
                health=SourceHealth(status="ok"),
            ),
            "partial",
            "partial",
            False,
        ),
    ],
)
def test_zero_output_and_incomplete_outcomes_remain_observed_evidence(
    run: SourceRun,
    completion: str,
    zero_kind: str | None,
    state_ready: bool,
) -> None:
    lowerer = _lowerer(run)

    chunks = list(lowerer.iter_chunks())
    assert bool(chunks) is bool(run.facts.items)
    assert lowerer.terminal.completion == completion
    assert lowerer.terminal.zero_output_kind == zero_kind
    assert lowerer.terminal.state_advancement_preconditions_met is state_ready
    if run.health is not None:
        assert lowerer.terminal.to_dict()["health"] == run.health.as_dict()


def test_reconcile_retractions_and_provenance_are_preserved_exactly() -> None:
    retract_node = _node("gone", op="R")
    retract_edge = _edge("gone", op="R", provenance="derived:test")
    duplicate = _node("duplicate")
    items = [retract_node, retract_edge, duplicate, duplicate]
    run = SourceRun(
        facts=SinglePassFacts(items),
        completed_scope=True,
        next_cursor={"reconciled": True},
        run_mode="reconcile",
        record_set=_record_set("duplicate"),
        health=SourceHealth(status="ok"),
    )
    lowerer = _lowerer(run)

    chunks = list(lowerer.iter_chunks())

    assert _flatten(chunks) == items
    assert chunks[0].facts[1].provenance == "derived:test"
    assert len(_flatten(chunks)) == 4
    assert lowerer.terminal.completion == "snapshot"
    assert len(lowerer.terminal.source_delta.retracted_record_ids) == 2


def test_terminal_is_unavailable_until_stream_exhaustion_and_run_is_single_use() -> (
    None
):
    lowerer = _lowerer(SourceRun(facts=SinglePassFacts([_node("a")])))
    with pytest.raises(SourceRunLoweringError, match="until chunks are exhausted"):
        _ = lowerer.terminal

    iterator = lowerer.iter_chunks()
    next(iterator)
    with pytest.raises(SourceRunLoweringError, match="until chunks are exhausted"):
        _ = lowerer.terminal
    with pytest.raises(StopIteration):
        next(iterator)
    assert lowerer.terminal.fact_count == 1
    with pytest.raises(SourceRunLoweringError, match="single-use"):
        list(lowerer.iter_chunks())


def _header() -> FactBatchHeader:
    return FactBatchHeader(
        schema_version=1,
        run_id=uuid.uuid4(),
        admission_id=uuid.uuid4(),
        producer_profile_fingerprint="sha256:" + "a" * 64,
        deployment="test",
        target_branch="candidate",
        catalog_version_id=1,
        intent="source_sync",
        idempotency_key="source-run-replay",
    )


def _lowerer(
    run: SourceRun,
    **kwargs: int,
) -> SourceRunFactBatchLowerer:
    return SourceRunFactBatchLowerer(
        header=_header(),
        source_id="fixture-source",
        source_run=run,
        **kwargs,
    )


def _edge(
    record: str,
    *,
    op: str = "I",
    provenance: str = "explicit",
) -> EdgeFact:
    return EdgeFact(
        src=f"doc:{record}",
        dst=f"chunk:{record}",
        edge_type="contains",
        provenance=provenance,
        source_record_id={"record": record, "edge": "contains"},
        source_revision={"sha": record},
        op=op,
    )


def _record_set(*records: str) -> RecordSet:
    return RecordSet(
        records={
            record: RecordEmission(primary=RecordNode(f"doc:{record}", "document"))
            for record in records
        }
    )


def _flatten(chunks: list[object]) -> list[object]:
    output: list[object] = []
    for chunk in chunks:
        output.extend(chunk.facts)
        output.extend(chunk.progress_markers)
    return output
