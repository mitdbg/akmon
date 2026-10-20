"""Compatibility lowering from the stateful source contract to FactBatch.

``SourceAdapter`` and ``SourceRun`` remain the source invocation API.  This
module consumes a run's fact iterator exactly once, emits bounded FactBatch
chunks, and exposes source-specific terminal evidence only after exhaustion.
It deliberately makes no admission or publication decision.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Mapping

from okg.substrate.fact_batch import (
    MAX_FACTS_PER_CHUNK,
    MAX_MARKERS_PER_CHUNK,
    FactBatchError,
    FactBatchHeader,
    FactChunk,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    RecordSet,
    SourceHealth,
    SourceRun,
)
from okg.substrate.source_sync_contract import (
    SourceDelta,
    SourceDeltaAccumulator,
    SourceSyncContractError,
)


_FAILED_HEALTH = frozenset(
    {
        "missing_credential",
        "auth_failed",
        "tls_failed",
        "endpoint_failed",
        "cache_missing",
        "acquire_failed",
    }
)
_HEALTHY_HEALTH = frozenset({"ok", "not_applicable", "skipped_optional"})
_SNAPSHOT_MODES = frozenset({"reconcile", "full_rebuild", "scope_complete"})
_UNCHANGED_MODES = frozenset({"no_change", "release_unchanged"})


class SourceRunLoweringError(FactBatchError):
    """Raised when SourceRun cannot be represented by the shared protocol."""


@dataclass(frozen=True)
class SourceRunTerminal:
    """Terminal source evidence preserved after the iterator is exhausted.

    ``state_advancement_preconditions_met`` is evidence for admission policy;
    it is never an acceptance decision.  Admission must still evaluate source
    policy, health, completeness, and the terminal FactBatch summary.
    """

    source_id: str
    next_cursor: Mapping[str, Any] | None
    completed_scope: bool
    run_mode: str
    record_set: RecordSet | None
    record_set_replacement_keys: tuple[str, ...]
    health: SourceHealth | None
    completion: str
    zero_output_kind: str | None
    state_advancement_preconditions_met: bool
    fact_count: int
    progress_marker_count: int
    chunk_count: int
    source_delta: SourceDelta

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "next_cursor": (
                dict(self.next_cursor) if self.next_cursor is not None else None
            ),
            "completed_scope": self.completed_scope,
            "run_mode": self.run_mode,
            "record_set": (
                self.record_set.to_cursor() if self.record_set is not None else None
            ),
            "record_set_replacement_keys": list(self.record_set_replacement_keys),
            "health": self.health.as_dict() if self.health is not None else None,
            "completion": self.completion,
            "zero_output_kind": self.zero_output_kind,
            "state_advancement_preconditions_met": (
                self.state_advancement_preconditions_met
            ),
            "fact_count": self.fact_count,
            "progress_marker_count": self.progress_marker_count,
            "chunk_count": self.chunk_count,
            "source_delta": self.source_delta.to_dict(),
        }


class SourceRunFactBatchLowerer:
    """Consume one SourceRun lazily and yield bounded ordered FactChunks."""

    def __init__(
        self,
        *,
        header: FactBatchHeader,
        source_id: str,
        source_run: SourceRun,
        max_facts_per_chunk: int = MAX_FACTS_PER_CHUNK,
        max_markers_per_chunk: int = MAX_MARKERS_PER_CHUNK,
        previous_frontier: Mapping[str, Any] | None = None,
    ) -> None:
        if not isinstance(source_id, str) or not source_id.strip():
            raise SourceRunLoweringError("source_id is required")
        if not isinstance(source_run, SourceRun):
            raise SourceRunLoweringError("source_run must be a SourceRun")
        self._require_bound(
            max_facts_per_chunk,
            MAX_FACTS_PER_CHUNK,
            "max_facts_per_chunk",
        )
        self._require_bound(
            max_markers_per_chunk,
            MAX_MARKERS_PER_CHUNK,
            "max_markers_per_chunk",
        )
        self.header = header
        self.source_id = source_id
        self.source_run = source_run
        self.max_facts_per_chunk = max_facts_per_chunk
        self.max_markers_per_chunk = max_markers_per_chunk
        self.previous_frontier = previous_frontier
        self._started = False
        self._terminal: SourceRunTerminal | None = None

    @property
    def terminal(self) -> SourceRunTerminal:
        if self._terminal is None:
            raise SourceRunLoweringError(
                "terminal evidence is unavailable until chunks are exhausted"
            )
        return self._terminal

    def iter_chunks(self) -> Iterator[FactChunk]:
        if self._started:
            raise SourceRunLoweringError("SourceRun fact iterator is single-use")
        self._started = True
        facts: list[NodeFact | EdgeFact] = []
        markers: list[ProgressMarker] = []
        ordinal = 0
        fact_count = 0
        marker_count = 0
        accumulator = SourceDeltaAccumulator()

        def flush() -> FactChunk:
            nonlocal facts, markers, ordinal
            chunk = FactChunk.create(
                run_id=self.header.run_id,
                ordinal=ordinal,
                facts=facts,
                progress_markers=markers,
            )
            ordinal += 1
            facts = []
            markers = []
            return chunk

        for item in self.source_run.facts:
            try:
                accumulator.observe(item)
            except SourceSyncContractError as exc:
                raise SourceRunLoweringError(str(exc)) from exc
            if isinstance(item, (NodeFact, EdgeFact)):
                facts.append(item)
                fact_count += 1
            elif isinstance(item, ProgressMarker):
                markers.append(item)
                marker_count += 1
            else:  # Defensive: accumulator currently rejects this first.
                raise SourceRunLoweringError(
                    "SourceRun facts must contain NodeFact, EdgeFact, or "
                    f"ProgressMarker; got {type(item).__name__}"
                )
            if (
                len(facts) >= self.max_facts_per_chunk
                or len(markers) >= self.max_markers_per_chunk
            ):
                yield flush()
        if facts or markers:
            yield flush()

        source_delta = accumulator.build(
            self.source_id,
            previous_frontier=self.previous_frontier,
            next_frontier=self.source_run.next_cursor,
            run_mode=self.source_run.run_mode,
            validate=(self.source_run.next_cursor is not None or marker_count > 0),
        )
        completion, zero_output_kind, state_ready = _classify_terminal(
            self.source_run,
            fact_count=fact_count,
            marker_count=marker_count,
        )
        replacement_keys = tuple(self.source_run.record_set_replacement_keys or ())
        self._terminal = SourceRunTerminal(
            source_id=self.source_id,
            next_cursor=self.source_run.next_cursor,
            completed_scope=self.source_run.completed_scope,
            run_mode=self.source_run.run_mode,
            record_set=self.source_run.record_set,
            record_set_replacement_keys=replacement_keys,
            health=self.source_run.health,
            completion=completion,
            zero_output_kind=zero_output_kind,
            state_advancement_preconditions_met=state_ready,
            fact_count=fact_count,
            progress_marker_count=marker_count,
            chunk_count=ordinal,
            source_delta=source_delta,
        )

    @staticmethod
    def _require_bound(value: int, maximum: int, name: str) -> None:
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 1
            or value > maximum
        ):
            raise SourceRunLoweringError(f"{name} must be between 1 and {maximum}")


def _classify_terminal(
    source_run: SourceRun,
    *,
    fact_count: int,
    marker_count: int,
) -> tuple[str, str | None, bool]:
    status = source_run.health.status if source_run.health is not None else "ok"
    failed = status in _FAILED_HEALTH
    degraded = status not in _HEALTHY_HEALTH and not failed
    complete = source_run.completed_scope and not failed and not degraded
    if not complete:
        completion = "partial"
    elif source_run.run_mode in _SNAPSHOT_MODES:
        completion = "snapshot"
    else:
        completion = "delta"

    zero_output_kind: str | None = None
    if fact_count == 0:
        if failed:
            zero_output_kind = "failed"
        elif degraded:
            zero_output_kind = "degraded"
        elif not source_run.completed_scope:
            zero_output_kind = "partial"
        elif (
            completion == "snapshot"
            and source_run.record_set is not None
            and not source_run.record_set.records
        ):
            zero_output_kind = "authoritative_empty_snapshot"
        elif source_run.run_mode in _UNCHANGED_MODES or marker_count > 0:
            zero_output_kind = "unchanged"
        else:
            zero_output_kind = "observed_empty"
    return completion, zero_output_kind, complete


__all__ = [
    "SourceRunFactBatchLowerer",
    "SourceRunLoweringError",
    "SourceRunTerminal",
]
