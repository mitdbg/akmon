"""Derive a bounded reverse-index change frontier from real change inputs.

`deltaflow-publish-projection`: the publisher already holds, by the time
it reaches the derived-artifact and native-view stages, every input it
needs to bound downstream work — the `SourceDeltaBatch` a readiness
source emitted (`deltaflow-source-contracts`), the Chronos apply graph
write-set, and the catalog component-hash diff. This module turns that
evidence into the `LineageInvalidation` list the derived-artifact
scheduler consumes via `plan_lineage_invalidations`, so a one-record
source edit or a one-component view edit dirties only the dependent
materialized outputs instead of forcing a deployment-wide rebuild.

It is intentionally pure: no DB access, no publisher coupling. The
scheduler's reverse-index lookup is what decides dirty/reuse/force-full;
this module only names the changed dependency keys. Safety is preserved
by `lookup_dirty_outputs`: a change against a dependency kind that has no
envelopes yet is reported `unavailable`, which forces a full rebuild
rather than a false reuse, so wiring this frontier in can never skip work
that should have run.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from okg.substrate.derived_artifacts.dependency_index import (
    INDEXED_DEPENDENCY_KINDS,
)
from okg.substrate.derived_artifacts.scheduler import LineageInvalidation


@dataclass(frozen=True)
class ChangeFrontier:
    """A deduplicated set of reverse-index invalidations for one publish."""

    invalidations: tuple[LineageInvalidation, ...]
    key_counts_by_kind: Mapping[str, int]
    skipped_source_ids: tuple[str, ...] = ()

    @property
    def is_empty(self) -> bool:
        return not self.invalidations

    def to_dict(self) -> dict[str, Any]:
        return {
            "invalidation_count": len(self.invalidations),
            "key_counts_by_kind": dict(self.key_counts_by_kind),
            "skipped_source_ids": list(self.skipped_source_ids),
        }


def _invalidation(kind: str, ref: str, reason: str) -> LineageInvalidation:
    if kind not in INDEXED_DEPENDENCY_KINDS:
        raise ValueError(
            f"change frontier kind {kind!r} is not reverse-indexed; "
            f"expected one of {sorted(INDEXED_DEPENDENCY_KINDS)}"
        )
    if not isinstance(ref, str) or not ref.strip():
        raise ValueError(
            f"change frontier {kind} dependency_ref must be a non-empty string"
        )
    return LineageInvalidation(
        dependency_kind=kind,
        dependency_ref=ref,
        dirty_reason=reason,
    )


def source_record_invalidations(
    source_id: str,
    record_ids: Iterable[str],
    *,
    reason: str = "source_record_changed",
) -> list[LineageInvalidation]:
    """`source_record` keys are `{source_id}:{canonical_record_id}`.

    The record id must be the same canonical identity the lineage
    envelope used (the `SourceDeltaBatch` canonicalization), so the
    reverse-index ref matches.
    """
    return [
        _invalidation("source_record", f"{source_id}:{record_id}", reason)
        for record_id in record_ids
    ]


def source_content_hash_invalidations(
    source_id: str,
    content_hashes: Iterable[str],
    *,
    reason: str = "source_content_changed",
) -> list[LineageInvalidation]:
    return [
        _invalidation("source_content_hash", f"{source_id}:{value}", reason)
        for value in content_hashes
    ]


def graph_node_invalidations(
    node_ids: Iterable[str],
    *,
    reason: str = "graph_node_changed",
) -> list[LineageInvalidation]:
    return [_invalidation("graph_node", node_id, reason) for node_id in node_ids]


def graph_edge_invalidations(
    edge_ids: Iterable[str],
    *,
    reason: str = "graph_edge_changed",
) -> list[LineageInvalidation]:
    return [_invalidation("graph_edge", edge_id, reason) for edge_id in edge_ids]


def _string_ref(value: Any) -> str | None:
    if value is None or isinstance(value, (Mapping, list, tuple, set)):
        return None
    ref = str(value).strip()
    return ref or None


def _lens_overlay_source_record_id(
    overlay: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    source_record_id = overlay.get("source_record_id")
    if isinstance(source_record_id, Mapping):
        return source_record_id
    source_delta = overlay.get("source_delta")
    if isinstance(source_delta, Mapping):
        nested = source_delta.get("source_record_id")
        if isinstance(nested, Mapping):
            return nested
    return None


def lens_overlay_invalidations(
    overlays: Iterable[Mapping[str, Any]],
    *,
    reason: str = "lens_overlay_committed",
) -> list[LineageInvalidation]:
    """Map committed lens overlays to decorated graph-node/edge invalidations.

    Overlay commits are human/operator deltas over an existing judgment output.
    They do not touch Chronos graph rows, so the normal dirty scope cannot see
    them. The putback delta already carries the decorated source identity; when
    it names a graph node or edge, emit the same reverse-index key downstream
    deterministic rows already consume. Unknown source identities are skipped
    rather than guessed from row keys.
    """
    node_ids: list[str] = []
    edge_ids: list[str] = []
    for overlay in overlays:
        if not isinstance(overlay, Mapping):
            raise TypeError(
                "lens overlay invalidation input must be a mapping; got "
                f"{type(overlay).__name__}"
            )
        source_record_id = _lens_overlay_source_record_id(overlay)
        if source_record_id is None:
            continue
        node_id = _string_ref(source_record_id.get("node_id"))
        edge_id = _string_ref(source_record_id.get("edge_id"))
        if node_id is not None:
            node_ids.append(node_id)
        if edge_id is not None:
            edge_ids.append(edge_id)
    return [
        *graph_node_invalidations(dict.fromkeys(node_ids), reason=reason),
        *graph_edge_invalidations(dict.fromkeys(edge_ids), reason=reason),
    ]


def component_hash_invalidations(
    component_hashes: Iterable[str],
    *,
    reason: str = "component_definition_changed",
) -> list[LineageInvalidation]:
    return [
        _invalidation("component_hash", value, reason)
        for value in component_hashes
    ]


def proposal_invalidations(
    proposal_ids: Iterable[str],
    *,
    reason: str = "proposal_changed",
) -> list[LineageInvalidation]:
    return [_invalidation("proposal", value, reason) for value in proposal_ids]


def evidence_invalidations(
    evidence_ids: Iterable[str],
    *,
    reason: str = "evidence_changed",
) -> list[LineageInvalidation]:
    return [_invalidation("evidence", value, reason) for value in evidence_ids]


def decision_invalidations(
    decision_ids: Iterable[str],
    *,
    reason: str = "decision_changed",
) -> list[LineageInvalidation]:
    return [_invalidation("decision", value, reason) for value in decision_ids]


def invalidations_from_projection_deltas(
    projection_deltas: Mapping[str, Any],
) -> list[LineageInvalidation]:
    """Load persisted projection-delta invalidation payloads.

    Lens putback commits persist the exact reverse-index keys that downstream
    derivations consume. This parser intentionally revalidates the stored payload
    through the same indexed-kind checks as live frontier construction.
    """
    raw_items = projection_deltas.get("lineage_invalidations")
    if raw_items is None:
        return []
    if not isinstance(raw_items, Sequence) or isinstance(raw_items, (str, bytes)):
        raise TypeError("projection_deltas.lineage_invalidations must be a list")
    invalidations: list[LineageInvalidation] = []
    for raw in raw_items:
        if not isinstance(raw, Mapping):
            raise TypeError("projection delta invalidation must be a mapping")
        kind = raw.get("dependency_kind")
        ref = raw.get("dependency_ref")
        if not isinstance(kind, str) or not isinstance(ref, str):
            raise TypeError(
                "projection delta invalidation requires string "
                "dependency_kind and dependency_ref"
            )
        reason = raw.get("dirty_reason") or "projection_delta_changed"
        base = _invalidation(kind, ref, str(reason))
        dependency_hash = raw.get("dependency_hash")
        invalidations.append(
            LineageInvalidation(
                dependency_kind=base.dependency_kind,
                dependency_ref=base.dependency_ref,
                dirty_reason=base.dirty_reason,
                dependency_hash=(
                    str(dependency_hash)
                    if dependency_hash is not None else None
                ),
            )
        )
    return invalidations


def invalidations_from_delta_batch(
    batch: Any,
) -> tuple[list[LineageInvalidation], str | None]:
    """Map one `SourceDeltaBatch` (object or `to_dict()` form) to keys.

    Returns ``(invalidations, skipped_source_id)``. A batch whose runner
    ``contract`` status is not ``valid`` (``invalid`` / ``unavailable``)
    has an unknown change scope: it yields no invalidations and reports
    its ``source_id`` as skipped, so the caller treats that source as
    widened (force-full for its outputs) rather than silently clean.
    """
    data = batch.to_dict() if hasattr(batch, "to_dict") else batch
    if not isinstance(data, Mapping):
        raise TypeError(
            "delta batch must be a SourceDeltaBatch or mapping; got "
            f"{type(data).__name__}"
        )
    source_id = data.get("source_id")
    contract_status = data.get("contract")
    if contract_status not in (None, "valid"):
        return [], (str(source_id) if source_id else None)
    if not source_id:
        return [], None
    record_ids = data.get("changed_record_ids") or []
    return source_record_invalidations(str(source_id), record_ids), None


def build_change_frontier(
    *,
    source_delta_batches: Iterable[Any] = (),
    graph_node_ids: Iterable[str] = (),
    graph_edge_ids: Iterable[str] = (),
    changed_component_hashes: Iterable[str] = (),
    source_content_hashes: Mapping[str, Iterable[str]] | None = None,
    lens_overlay_rows: Iterable[Mapping[str, Any]] = (),
    proposal_ids: Iterable[str] = (),
    evidence_ids: Iterable[str] = (),
    decision_ids: Iterable[str] = (),
) -> ChangeFrontier:
    """Unify all change inputs into one deduplicated ChangeFrontier.

    Order is stable (source records, content hashes, graph nodes, graph
    edges, lens overlay decorated graph ids, component hashes, proposals,
    evidence, decisions) and duplicates across inputs collapse on
    ``(dependency_kind, dependency_ref)``.
    """
    seen: set[tuple[str, str]] = set()
    ordered: list[LineageInvalidation] = []
    skipped: list[str] = []

    def _add(items: Iterable[LineageInvalidation]) -> None:
        for invalidation in items:
            key = (invalidation.dependency_kind, invalidation.dependency_ref)
            if key not in seen:
                seen.add(key)
                ordered.append(invalidation)

    for batch in source_delta_batches:
        invalidations, skipped_source_id = invalidations_from_delta_batch(batch)
        _add(invalidations)
        if skipped_source_id is not None:
            skipped.append(skipped_source_id)

    if source_content_hashes:
        for source_id, hashes in source_content_hashes.items():
            _add(source_content_hash_invalidations(str(source_id), hashes))

    _add(graph_node_invalidations(graph_node_ids))
    _add(graph_edge_invalidations(graph_edge_ids))
    _add(lens_overlay_invalidations(lens_overlay_rows))
    _add(component_hash_invalidations(changed_component_hashes))
    _add(proposal_invalidations(proposal_ids))
    _add(evidence_invalidations(evidence_ids))
    _add(decision_invalidations(decision_ids))

    counts: dict[str, int] = {}
    for invalidation in ordered:
        counts[invalidation.dependency_kind] = (
            counts.get(invalidation.dependency_kind, 0) + 1
        )

    return ChangeFrontier(
        invalidations=tuple(ordered),
        key_counts_by_kind=counts,
        skipped_source_ids=tuple(dict.fromkeys(skipped)),
    )


__all__ = [
    "ChangeFrontier",
    "build_change_frontier",
    "component_hash_invalidations",
    "decision_invalidations",
    "evidence_invalidations",
    "graph_edge_invalidations",
    "graph_node_invalidations",
    "invalidations_from_delta_batch",
    "invalidations_from_projection_deltas",
    "lens_overlay_invalidations",
    "proposal_invalidations",
    "source_content_hash_invalidations",
    "source_record_invalidations",
]
