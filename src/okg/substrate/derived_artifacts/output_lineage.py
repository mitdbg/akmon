"""Build real lineage envelopes for materialized outputs.

`deltaflow-publish-projection`: the live publish path has never emitted
`MaterializedLineageEnvelope`s, so the reverse-dependency index stays
empty and the change frontier can never drive reuse. This module is the
output-side half of the contract (the input-side half is
`change_frontier`): given the real provenance a publish stage holds for
one output — the source records behind it, the graph node/edge ids it
derived from, and the component (view/relation/checker) that produced it
— it assembles a validated envelope ready for `publish_lineage_envelopes`.

It is pure (no DB, no publisher coupling) and centralizes the two pieces
of correctness that are easy to get wrong:

1. **Hash discipline.** The envelope requires explicit hash strings
   (`sha256:` / `hash:` / `dep:` prefixed); raw digests are rejected.
   `as_hash` enforces the prefix so callers cannot accidentally emit a
   bare hex digest.
2. **Record-id consistency.** A `source_record` reverse-index key is
   `{source_id}:{record_id}`. For a change frontier to ever match an
   envelope, both sides must canonicalize the record id identically, so
   this module derives `record_id` through the same
   `canonical_record_id` the source delta contract uses.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from okg.substrate.derived_artifacts.lineage import (
    ComponentLineage,
    GraphLineage,
    MaterializedLineageEnvelope,
    SourceRecordLineage,
)
from okg.substrate.derived_artifacts.relations import stable_hash
from okg.substrate.source_sync_contract import canonical_record_id

_HASH_PREFIXES = ("sha256:", "hash:", "dep:")


def as_hash(value: str) -> str:
    """Return ``value`` as an explicit hash string the envelope accepts.

    A value already carrying a known prefix passes through; a bare digest
    is wrapped as ``sha256:<digest>``.
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError("hash value must be a non-empty string")
    if value.startswith(_HASH_PREFIXES):
        return value
    return f"sha256:{value}"


def source_record_lineage(
    source_id: str,
    record_id: Any,
    *,
    content_hash: str | None = None,
    source_revision: Mapping[str, Any] | None = None,
) -> SourceRecordLineage:
    """Build one `SourceRecordLineage` with a shared-canonical record id.

    ``record_id`` may be a raw mapping (as carried on a fact's
    ``source_record_id``) or an already-canonical string; either way it is
    normalized through `canonical_record_id`. When ``content_hash`` is
    omitted it is derived deterministically from the source identity and
    revision, so an envelope always carries a stable content fingerprint.
    """
    canonical = canonical_record_id(record_id)
    revision = dict(source_revision or {})
    if content_hash is None:
        content_hash = stable_hash({
            "source_id": source_id,
            "record_id": canonical,
            "source_revision": revision,
        })
    return SourceRecordLineage(
        source_id=source_id,
        record_id=canonical,
        content_hash=as_hash(content_hash),
        source_revision=revision,
    )


def source_records_from_node_rows(
    rows: Iterable[Mapping[str, Any]],
) -> list[SourceRecordLineage]:
    """Assemble deduped source-record lineage from `okg.node_facts` rows.

    Each row is expected to carry ``source``, ``source_record_id`` and an
    optional ``source_revision`` (the columns a cited-node provenance
    query returns). Rows missing a source or record id are skipped — they
    cannot be named in the reverse index — and the caller decides whether
    the resulting (possibly empty) set is sufficient to emit an envelope.
    """
    seen: set[tuple[str, str]] = set()
    out: list[SourceRecordLineage] = []
    for row in rows:
        source_id = row.get("source")
        raw_record = row.get("source_record_id")
        if not source_id or not raw_record:
            continue
        record = source_record_lineage(
            str(source_id),
            raw_record,
            source_revision=row.get("source_revision") or {},
        )
        key = (record.source_id, record.record_id)
        if key in seen:
            continue
        seen.add(key)
        out.append(record)
    return out


def build_output_lineage_envelope(
    *,
    output_id: str,
    output_kind: str,
    source_records: Sequence[SourceRecordLineage],
    component_id: str,
    component_hash: str,
    component_kind: str,
    output_digest: str,
    catalog_version_id: str | int,
    graph_generation_id: str | int | None = None,
    graph_branch: str | None = "default",
    graph_checkpoint_id: str | int | None = None,
    node_ids: Iterable[str] = (),
    edge_ids: Iterable[str] = (),
    component_version: str | None = None,
    previous_output_digest: str | None = None,
    invalidation_scope: str = "per-output",
    proposal_ids: Iterable[str] = (),
    evidence_ids: Iterable[str] = (),
    decision_ids: Iterable[str] = (),
    metadata: Mapping[str, Any] | None = None,
) -> MaterializedLineageEnvelope:
    """Assemble one validated `MaterializedLineageEnvelope`.

    Hash-shaped fields (`component_hash`, `output_digest`,
    `previous_output_digest`) are normalized through `as_hash`. The
    `MaterializedLineageEnvelope` constructor performs the full validation
    (non-empty source records, known output/component kinds and
    invalidation scope, pinned graph context), so a malformed envelope
    raises here rather than on persistence.
    """
    return MaterializedLineageEnvelope(
        output_id=output_id,
        output_kind=output_kind,
        source_records=tuple(source_records),
        graph=GraphLineage(
            graph_generation_id=graph_generation_id,
            graph_branch=graph_branch,
            graph_checkpoint_id=graph_checkpoint_id,
            node_ids=tuple(node_ids),
            edge_ids=tuple(edge_ids),
        ),
        catalog_version_id=catalog_version_id,
        component=ComponentLineage(
            component_id=component_id,
            component_hash=as_hash(component_hash),
            component_kind=component_kind,
            version=component_version,
        ),
        output_digest=as_hash(output_digest),
        previous_output_digest=(
            as_hash(previous_output_digest)
            if previous_output_digest is not None
            else None
        ),
        invalidation_scope=invalidation_scope,
        proposal_ids=tuple(proposal_ids),
        evidence_ids=tuple(evidence_ids),
        decision_ids=tuple(decision_ids),
        metadata=dict(metadata or {}),
    )


__all__ = [
    "as_hash",
    "build_output_lineage_envelope",
    "source_record_lineage",
    "source_records_from_node_rows",
]
