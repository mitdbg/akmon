"""Delta/frontier validation helpers for source sync runs.

This module is intentionally narrow: it does not change ``SourceRun`` or
registry loading. It gives future source migrations a small, SourceRun-shaped
contract surface that can validate stable record ids, frontier/progress state,
requested-vs-effective scope, and explicit fallback evidence.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    RecordSet,
    SourceRun,
)
from okg.substrate.source_sync import (
    SOURCE_SYNC_RUN_MODES,
    SourceSyncCapabilities,
)


SOURCE_CONTRACT_RUN_MODES = frozenset(
    {
        *SOURCE_SYNC_RUN_MODES,
        "full_rebuild",
        "no_change",
    }
)

_MODE_WIDTH = {
    "no_change": 0,
    "cursor": 1,
    "append": 1,
    "release_unchanged": 1,
    "release_new": 1,
    "scope_incomplete": 2,
    "scope_complete": 3,
    "reconcile": 4,
    "full_rebuild": 5,
}


class SourceSyncContractError(ValueError):
    """Raised when source delta evidence violates the contract."""


@dataclass(frozen=True)
class SourceFallbackEvidence:
    """Structured evidence for work widened beyond the requested delta."""

    reason: str
    requested_mode: str
    effective_mode: str
    measured_input_size: int
    affected_scopes: tuple[str, ...]
    readiness_impact: str
    requested_scope: Mapping[str, Any] | None = None
    effective_scope: Mapping[str, Any] | None = None
    evidence_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise SourceSyncContractError("fallback reason is required")
        _require_contract_mode(self.requested_mode, "fallback.requested_mode")
        _require_contract_mode(self.effective_mode, "fallback.effective_mode")
        if (
            isinstance(self.measured_input_size, bool)
            or not isinstance(self.measured_input_size, int)
            or self.measured_input_size < 0
        ):
            raise SourceSyncContractError(
                "fallback measured_input_size must be a non-negative integer"
            )
        scopes = _coerce_str_tuple(
            self.affected_scopes,
            "fallback.affected_scopes",
            require_non_empty=True,
        )
        object.__setattr__(self, "affected_scopes", scopes)
        if (
            not isinstance(self.readiness_impact, str)
            or not self.readiness_impact.strip()
        ):
            raise SourceSyncContractError("fallback readiness_impact is required")
        object.__setattr__(
            self,
            "evidence_refs",
            _coerce_str_tuple(
                self.evidence_refs,
                "fallback.evidence_refs",
                require_non_empty=False,
            ),
        )
        object.__setattr__(
            self,
            "requested_scope",
            _optional_scope_dict(self.requested_scope),
        )
        object.__setattr__(
            self,
            "effective_scope",
            _optional_scope_dict(self.effective_scope),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "reason": self.reason,
            "requested_mode": self.requested_mode,
            "effective_mode": self.effective_mode,
            "measured_input_size": self.measured_input_size,
            "affected_scopes": list(self.affected_scopes),
            "readiness_impact": self.readiness_impact,
            "requested_scope": (
                dict(self.requested_scope) if self.requested_scope is not None else None
            ),
            "effective_scope": (
                dict(self.effective_scope) if self.effective_scope is not None else None
            ),
            "evidence_refs": list(self.evidence_refs),
        }


@dataclass(frozen=True)
class SourceFallbackPolicy:
    """Fallback policy derived from existing registry sync fields."""

    scope_kinds: tuple[str, ...] = ()
    default_event_mode: str = "cursor"
    reconcile_mode: str | None = None
    unsupported_scope_fallback: str | None = None
    delete_fallback: str | None = None
    scoped_delete_mode: str | None = None

    @classmethod
    def from_sync(
        cls,
        sync: SourceSyncCapabilities | Mapping[str, Any] | None,
    ) -> "SourceFallbackPolicy":
        if sync is None:
            return cls()
        if isinstance(sync, Mapping):
            parsed = SourceSyncCapabilities.from_dict(sync)
            if parsed is None:
                return cls()
            sync = parsed
        if not isinstance(sync, SourceSyncCapabilities):
            raise SourceSyncContractError(
                "sync fallback policy requires SourceSyncCapabilities"
            )
        scope_kinds: tuple[str, ...] = ()
        unsupported_scope_fallback = None
        if sync.scope is not None:
            scope_kinds = tuple(sync.scope.accepts)
            unsupported_scope_fallback = sync.scope.unsupported_fallback
        delete_fallback = (
            sync.reconcile_mode or "reconcile"
            if sync.deletes_require_reconcile
            else None
        )
        return cls(
            scope_kinds=scope_kinds,
            default_event_mode=sync.default_event_mode,
            reconcile_mode=sync.reconcile_mode,
            unsupported_scope_fallback=unsupported_scope_fallback,
            delete_fallback=delete_fallback,
            scoped_delete_mode=sync.scoped_delete_mode,
        )

    @property
    def supports_scoped_change(self) -> bool:
        return bool(self.scope_kinds)

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope_kinds": list(self.scope_kinds),
            "default_event_mode": self.default_event_mode,
            "reconcile_mode": self.reconcile_mode,
            "unsupported_scope_fallback": self.unsupported_scope_fallback,
            "delete_fallback": self.delete_fallback,
            "scoped_delete_mode": self.scoped_delete_mode,
            "supports_scoped_change": self.supports_scoped_change,
        }


@dataclass(frozen=True)
class SourceDelta:
    """SourceRun-compatible delta/frontier evidence for one source run."""

    source_id: str
    previous_frontier: Mapping[str, Any] | None = None
    next_frontier: Mapping[str, Any] | None = None
    facts: tuple[NodeFact | EdgeFact, ...] = ()
    progress_markers: tuple[ProgressMarker, ...] = ()
    record_set: RecordSet | None = None
    inserted_record_ids: tuple[str, ...] = ()
    updated_record_ids: tuple[str, ...] = ()
    retracted_record_ids: tuple[str, ...] = ()
    changed_record_ids: tuple[str, ...] = ()
    affected_scopes: tuple[str, ...] = ()
    requested_scope: Mapping[str, Any] | None = None
    effective_scope: Mapping[str, Any] | None = None
    requested_mode: str = "cursor"
    effective_mode: str = "cursor"
    fallback: SourceFallbackEvidence | None = None
    evidence_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.source_id, str) or not self.source_id.strip():
            raise SourceSyncContractError("source_id is required")
        _require_contract_mode(self.requested_mode, "requested_mode")
        _require_contract_mode(self.effective_mode, "effective_mode")
        object.__setattr__(self, "facts", tuple(self.facts))
        object.__setattr__(
            self,
            "progress_markers",
            tuple(self.progress_markers),
        )
        if self.record_set is not None and not isinstance(
            self.record_set,
            RecordSet,
        ):
            raise SourceSyncContractError("record_set must be a RecordSet")
        object.__setattr__(
            self,
            "previous_frontier",
            _optional_scope_dict(self.previous_frontier),
        )
        object.__setattr__(
            self,
            "next_frontier",
            _optional_scope_dict(self.next_frontier),
        )
        object.__setattr__(
            self,
            "requested_scope",
            _optional_scope_dict(self.requested_scope),
        )
        object.__setattr__(
            self,
            "effective_scope",
            _optional_scope_dict(self.effective_scope),
        )
        for field_name in (
            "inserted_record_ids",
            "updated_record_ids",
            "retracted_record_ids",
            "changed_record_ids",
            "affected_scopes",
            "evidence_refs",
        ):
            object.__setattr__(
                self,
                field_name,
                _unique_str_tuple(getattr(self, field_name)),
            )
        if self.fallback is not None and not isinstance(
            self.fallback,
            SourceFallbackEvidence,
        ):
            raise SourceSyncContractError("fallback must be SourceFallbackEvidence")

    @classmethod
    def from_source_run(
        cls,
        source_id: str,
        source_run: SourceRun,
        *,
        previous_frontier: Mapping[str, Any] | None = None,
        next_frontier: Mapping[str, Any] | None = None,
        requested_scope: Mapping[str, Any] | None = None,
        effective_scope: Mapping[str, Any] | None = None,
        requested_mode: str | None = None,
        effective_mode: str | None = None,
        affected_scopes: Sequence[str] = (),
        fallback: SourceFallbackEvidence | None = None,
        evidence_refs: Sequence[str] = (),
    ) -> "SourceDelta":
        materialized = tuple(source_run.facts)
        facts: list[NodeFact | EdgeFact] = []
        progress: list[ProgressMarker] = []
        for item in materialized:
            if isinstance(item, (NodeFact, EdgeFact)):
                facts.append(item)
            elif isinstance(item, ProgressMarker):
                progress.append(item)
            else:
                raise SourceSyncContractError(
                    "SourceRun facts must contain NodeFact, EdgeFact, "
                    f"or ProgressMarker; got {type(item).__name__}"
                )
        inserted: list[str] = []
        updated: list[str] = []
        retracted: list[str] = []
        changed: list[str] = []
        for fact in facts:
            ids = extract_stable_record_ids(fact)
            changed.extend(ids)
            op = getattr(fact, "op", "I")
            if op == "R":
                retracted.extend(ids)
            elif op == "U":
                updated.extend(ids)
            else:
                inserted.extend(ids)
        for marker in progress:
            changed.extend(extract_stable_record_ids(marker))
        return cls(
            source_id=source_id,
            previous_frontier=previous_frontier,
            next_frontier=next_frontier or source_run.next_cursor,
            facts=tuple(facts),
            progress_markers=tuple(progress),
            record_set=source_run.record_set,
            inserted_record_ids=tuple(inserted),
            updated_record_ids=tuple(updated),
            retracted_record_ids=tuple(retracted),
            changed_record_ids=tuple(changed),
            affected_scopes=tuple(affected_scopes),
            requested_scope=requested_scope,
            effective_scope=effective_scope,
            requested_mode=requested_mode or source_run.run_mode,
            effective_mode=effective_mode or source_run.run_mode,
            fallback=fallback,
            evidence_refs=tuple(evidence_refs),
        )

    @property
    def record_ids(self) -> tuple[str, ...]:
        values: list[str] = []
        values.extend(self.inserted_record_ids)
        values.extend(self.updated_record_ids)
        values.extend(self.retracted_record_ids)
        values.extend(self.changed_record_ids)
        for fact in self.facts:
            values.extend(extract_stable_record_ids(fact))
        for marker in self.progress_markers:
            values.extend(extract_stable_record_ids(marker))
        if self.record_set is not None:
            values.extend(extract_stable_record_ids(self.record_set))
        return _unique_str_tuple(values)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "previous_frontier": (
                dict(self.previous_frontier)
                if self.previous_frontier is not None
                else None
            ),
            "next_frontier": (
                dict(self.next_frontier) if self.next_frontier is not None else None
            ),
            "record_ids": list(self.record_ids),
            "inserted_record_ids": list(self.inserted_record_ids),
            "updated_record_ids": list(self.updated_record_ids),
            "retracted_record_ids": list(self.retracted_record_ids),
            "changed_record_ids": list(self.changed_record_ids),
            "affected_scopes": list(self.affected_scopes),
            "requested_scope": (
                dict(self.requested_scope) if self.requested_scope is not None else None
            ),
            "effective_scope": (
                dict(self.effective_scope) if self.effective_scope is not None else None
            ),
            "requested_mode": self.requested_mode,
            "effective_mode": self.effective_mode,
            "fallback": self.fallback.to_dict() if self.fallback else None,
            "evidence_refs": list(self.evidence_refs),
            "fact_count": len(self.facts),
            "progress_marker_count": len(self.progress_markers),
            "record_set_count": (
                len(self.record_set.records) if self.record_set is not None else None
            ),
        }


# Compatibility names only. ``SourceDelta`` is evidence attached to a terminal
# FactBatch summary; it is not a second graph-output envelope.
SourceDeltaBatch = SourceDelta
DeltaBatch = SourceDelta


def extract_stable_record_ids(value: Any) -> tuple[str, ...]:
    """Extract deterministic record identity strings from source evidence."""
    if isinstance(value, (NodeFact, EdgeFact)):
        if not value.source_record_id:
            return ()
        return (_canonical_record_id(value.source_record_id),)
    if isinstance(value, ProgressMarker):
        return (_canonical_record_id(value.record_id),)
    if isinstance(value, RecordSet):
        return tuple(
            _canonical_record_id(record_id) for record_id in sorted(value.records)
        )
    if isinstance(value, SourceDelta):
        return value.record_ids
    raise SourceSyncContractError(
        "record identity can only be extracted from NodeFact, EdgeFact, "
        f"ProgressMarker, RecordSet, or SourceDeltaBatch; got "
        f"{type(value).__name__}"
    )


def scope_or_mode_widened(batch: SourceDelta) -> bool:
    return _mode_widened(
        batch.requested_mode,
        batch.effective_mode,
    ) or _scope_widened(
        batch.requested_scope,
        batch.effective_scope,
    )


def validate_source_delta_batch(
    batch: SourceDelta,
    *,
    fallback_policy: SourceFallbackPolicy | None = None,
) -> SourceDelta:
    """Validate source delta evidence and return the batch on success."""
    if not isinstance(batch, SourceDelta):
        raise SourceSyncContractError("batch must be SourceDelta evidence")
    _require_fact_record_identities(batch)
    if batch.next_frontier is None and not batch.progress_markers:
        raise SourceSyncContractError(
            "delta batch requires next_frontier or progress markers"
        )
    emitted_delta_ids = _unique_str_tuple(
        (
            *batch.inserted_record_ids,
            *batch.updated_record_ids,
            *batch.retracted_record_ids,
            *batch.changed_record_ids,
        )
    )
    if emitted_delta_ids and not batch.affected_scopes:
        raise SourceSyncContractError(
            "delta batch with changed records requires affected_scopes"
        )
    widened = scope_or_mode_widened(batch)
    if widened and batch.fallback is None:
        raise SourceSyncContractError(
            "source widened requested scope or mode without fallback evidence"
        )
    if batch.fallback is not None:
        _validate_fallback_matches_batch(batch)
    if fallback_policy is not None:
        _validate_against_policy(batch, fallback_policy)
    return batch


def _validate_against_policy(
    batch: SourceDelta,
    policy: SourceFallbackPolicy,
) -> None:
    if not isinstance(policy, SourceFallbackPolicy):
        raise SourceSyncContractError("fallback_policy must be a SourceFallbackPolicy")
    if (
        batch.requested_scope is not None
        and not policy.supports_scoped_change
        and batch.fallback is None
    ):
        raise SourceSyncContractError(
            "scoped request requires fallback evidence when source policy "
            "does not support scoped changes"
        )


def _validate_fallback_matches_batch(batch: SourceDelta) -> None:
    fallback = batch.fallback
    if fallback is None:
        return
    if fallback.requested_mode != batch.requested_mode:
        raise SourceSyncContractError(
            "fallback requested_mode must match batch requested_mode"
        )
    if fallback.effective_mode != batch.effective_mode:
        raise SourceSyncContractError(
            "fallback effective_mode must match batch effective_mode"
        )


def _require_fact_record_identities(batch: SourceDelta) -> None:
    for fact in batch.facts:
        if not isinstance(fact, (NodeFact, EdgeFact)):
            raise SourceSyncContractError(
                "facts must contain only NodeFact or EdgeFact values"
            )
        if not extract_stable_record_ids(fact):
            raise SourceSyncContractError(
                "NodeFact/EdgeFact is missing stable source_record_id"
            )
    for marker in batch.progress_markers:
        if not isinstance(marker, ProgressMarker):
            raise SourceSyncContractError(
                "progress_markers must contain only ProgressMarker values"
            )
        extract_stable_record_ids(marker)


def canonical_record_id(value: Any) -> str:
    """Canonicalize a source record identity to a deterministic string.

    Strings pass through; mappings become sorted compact JSON. This is the
    single canonicalization shared by the source delta contract and the
    materialized-output lineage envelopes, so a `source_record`
    invalidation key (`{source_id}:{canonical_record_id}`) on the change
    frontier matches the key an envelope declared for the same record.
    """
    if isinstance(value, str):
        if not value.strip():
            raise SourceSyncContractError("record identity must be non-empty")
        return value
    if isinstance(value, Mapping):
        if not value:
            raise SourceSyncContractError("record identity mapping is empty")
        return json.dumps(
            dict(value),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
    raise SourceSyncContractError(
        "record identity must be a non-empty string or mapping"
    )


# Back-compat private alias for existing call sites.
_canonical_record_id = canonical_record_id


def _require_contract_mode(value: Any, field_name: str) -> str:
    if value not in SOURCE_CONTRACT_RUN_MODES:
        allowed = ", ".join(sorted(SOURCE_CONTRACT_RUN_MODES))
        raise SourceSyncContractError(
            f"{field_name} must be one of {allowed}; got {value!r}"
        )
    return str(value)


def _mode_widened(requested_mode: str, effective_mode: str) -> bool:
    return _MODE_WIDTH[effective_mode] > _MODE_WIDTH[requested_mode]


def scope_widened(
    requested_scope: Mapping[str, Any] | None,
    effective_scope: Mapping[str, Any] | None,
) -> bool:
    """Whether the effective scope reaches outside the requested one.

    Public because a caller that widens a scope itself (the source runner,
    for issue #1454) must decide from the SAME rule whether the batch it is
    about to build needs fallback evidence.
    """
    requested_keys = _scope_keys(requested_scope)
    if not requested_keys:
        return False
    effective_keys = _scope_keys(effective_scope)
    if not effective_keys:
        return True
    return not set(effective_keys).issubset(set(requested_keys))


# Back-compat private alias for existing call sites.
_scope_widened = scope_widened


def scope_keys(scope: Mapping[str, Any] | None) -> tuple[str, ...]:
    """Return the scope keys the contract reads out of a scope mapping."""
    if scope is None:
        return ()
    for key in ("scope_keys", "keys", "affected_scopes"):
        if key in scope:
            return _unique_str_tuple(_coerce_sequence(scope[key], key))
    return ()


# Back-compat private alias for existing call sites.
_scope_keys = scope_keys


def _coerce_sequence(value: Any, field_name: str) -> tuple[Any, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Sequence) and not isinstance(value, bytes):
        return tuple(value)
    raise SourceSyncContractError(f"{field_name} must be a string or sequence")


def _coerce_str_tuple(
    values: Iterable[Any],
    field_name: str,
    *,
    require_non_empty: bool,
) -> tuple[str, ...]:
    result = _unique_str_tuple(values)
    if require_non_empty and not result:
        raise SourceSyncContractError(f"{field_name} must contain at least one value")
    return result


def _unique_str_tuple(values: Iterable[Any]) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise SourceSyncContractError(
                "contract tuple values must be non-empty strings"
            )
        if value not in seen:
            seen.add(value)
            out.append(value)
    return tuple(out)


def _optional_scope_dict(
    scope: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if scope is None:
        return None
    if not isinstance(scope, Mapping):
        raise SourceSyncContractError("scope/frontier must be a mapping")
    return dict(scope)


@dataclass(frozen=True)
class DeltaItemClassification:
    """How one streamed SourceRun item maps to delta evidence."""

    record_ids: tuple[str, ...]
    op: str  # "I" | "U" | "R" | "progress"
    scopes: tuple[str, ...]


def classify_delta_item(
    item: NodeFact | EdgeFact | ProgressMarker,
) -> DeltaItemClassification:
    """Classify one streamed SourceRun item into delta evidence.

    ``ProgressMarker`` is progress, NOT change: it carries a per-record
    fingerprint but no graph subtype/scope, so re-emitting a marker for
    an unchanged record must not mark that record or any scope dirty.
    This is the contract semantics for the no-change case and is the one
    place this module deliberately diverges from
    :meth:`SourceDeltaBatch.from_source_run`, which lumps marker ids into
    ``changed_record_ids``.

    ``NodeFact``/``EdgeFact`` carry a ``subtype``/``edge_type`` used as the
    affected scope. A fact without a stable ``source_record_id`` (e.g. an
    ``EdgeFact`` with an empty record id) contributes no record identity
    and therefore no scope.
    """
    if isinstance(item, ProgressMarker):
        return DeltaItemClassification(
            record_ids=extract_stable_record_ids(item),
            op="progress",
            scopes=(),
        )
    if isinstance(item, NodeFact):
        ids = extract_stable_record_ids(item)
        op = "R" if getattr(item, "op", "I") == "R" else "I"
        scopes = (item.subtype,) if ids and item.subtype else ()
        return DeltaItemClassification(ids, op, scopes)
    if isinstance(item, EdgeFact):
        ids = extract_stable_record_ids(item)
        op = "R" if getattr(item, "op", "I") == "R" else "I"
        scopes = (f"edge:{item.edge_type}",) if ids and item.edge_type else ()
        return DeltaItemClassification(ids, op, scopes)
    raise SourceSyncContractError(
        "delta classification requires NodeFact, EdgeFact, or "
        f"ProgressMarker; got {type(item).__name__}"
    )


class SourceDeltaAccumulator:
    """Build a :class:`SourceDeltaBatch` from a streamed fact sequence.

    ``SourceRunner`` consumes adapter facts as a stream (it never
    materializes ``SourceRun.facts``), so the delta contract is built by
    observing each item once as it flows through the runner's existing
    walk, then calling :meth:`build` after the final flush. The same
    accumulator backs both the runtime path and its tests, so the
    contract a readiness-critical source emits is exactly what the unit
    tests assert.
    """

    def __init__(self) -> None:
        self._inserted: list[str] = []
        self._updated: list[str] = []
        self._retracted: list[str] = []
        self._changed: list[str] = []
        self._scopes: set[str] = set()
        self.progress_marker_count = 0
        self.unidentified_fact_count = 0

    def observe(self, item: NodeFact | EdgeFact | ProgressMarker) -> None:
        classification = classify_delta_item(item)
        if classification.op == "progress":
            self.progress_marker_count += 1
            return
        if not classification.record_ids:
            # A fact that wrote graph state but carries no stable
            # source_record_id cannot be named in the delta contract.
            # Count it so the gap is loud rather than silently dropped.
            self.unidentified_fact_count += 1
            return
        self._changed.extend(classification.record_ids)
        if classification.op == "R":
            self._retracted.extend(classification.record_ids)
        elif classification.op == "U":
            self._updated.extend(classification.record_ids)
        else:
            self._inserted.extend(classification.record_ids)
        self._scopes.update(classification.scopes)

    def observe_all(
        self,
        items: Iterable[NodeFact | EdgeFact | ProgressMarker],
    ) -> "SourceDeltaAccumulator":
        for item in items:
            self.observe(item)
        return self

    @property
    def changed_record_count(self) -> int:
        return len(set(self._changed))

    def build(
        self,
        source_id: str,
        *,
        previous_frontier: Mapping[str, Any] | None = None,
        next_frontier: Mapping[str, Any] | None = None,
        run_mode: str = "cursor",
        requested_mode: str | None = None,
        requested_scope: Mapping[str, Any] | None = None,
        effective_scope: Mapping[str, Any] | None = None,
        fallback: SourceFallbackEvidence | None = None,
        evidence_refs: Sequence[str] = (),
        fallback_policy: SourceFallbackPolicy | None = None,
        validate: bool = True,
    ) -> SourceDelta:
        batch = SourceDelta(
            source_id=source_id,
            previous_frontier=previous_frontier,
            next_frontier=next_frontier,
            inserted_record_ids=tuple(self._inserted),
            updated_record_ids=tuple(self._updated),
            retracted_record_ids=tuple(self._retracted),
            changed_record_ids=tuple(self._changed),
            affected_scopes=tuple(sorted(self._scopes)),
            requested_scope=requested_scope,
            effective_scope=effective_scope,
            requested_mode=requested_mode or run_mode,
            effective_mode=run_mode,
            fallback=fallback,
            evidence_refs=tuple(evidence_refs),
        )
        if validate:
            validate_source_delta_batch(batch, fallback_policy=fallback_policy)
        return batch

    def summary(
        self,
        source_id: str,
        **build_kwargs: Any,
    ) -> dict[str, Any]:
        """Return the batch dict plus observed marker/unidentified counts.

        ``observed_progress_markers`` and ``unidentified_facts`` are
        carried alongside the batch (not inside it) so a downstream
        readiness gate can see when a run re-emitted progress for
        unchanged records (no-change evidence) or wrote facts that could
        not be named in the contract.
        """
        batch = self.build(source_id, **build_kwargs)
        return {
            **batch.to_dict(),
            "observed_progress_markers": self.progress_marker_count,
            "unidentified_facts": self.unidentified_fact_count,
        }


__all__ = [
    "DeltaBatch",
    "DeltaItemClassification",
    "SOURCE_CONTRACT_RUN_MODES",
    "SourceDeltaAccumulator",
    "SourceDelta",
    "SourceDeltaBatch",
    "SourceFallbackEvidence",
    "SourceFallbackPolicy",
    "SourceSyncContractError",
    "canonical_record_id",
    "classify_delta_item",
    "extract_stable_record_ids",
    "scope_keys",
    "scope_or_mode_widened",
    "scope_widened",
    "validate_source_delta_batch",
]
