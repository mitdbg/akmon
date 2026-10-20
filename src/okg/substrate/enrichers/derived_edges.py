"""Shared deterministic derived-edge emission helpers."""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from okg.substrate.ingest.sanitize import (
    NUL,
    identity_nul_fields_from,
    sanitize_value,
)
from okg.substrate.nomos.source_lineage import (
    LINEAGE_ATTR,
    attrs_with_lineage,
    combine_source_lineages,
    lineage_for_row,
    normalize_source_lineage,
    source_ids_from_lineage,
)


__all__ = [
    "DerivedEdgeCandidate",
    "DerivedEdgeInsertResult",
    "insert_deterministic_edges",
    "mint_edge_id",
    "stable_edge_dedupe_key",
]


DEDUP_KEY_LOOKUP_CHUNK = 100

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class DerivedEdgeCandidate:
    src: str
    edge_type: str
    dst: str
    attrs: dict[str, Any] | None = None
    provenance: str = "derived_deterministic"
    confidence: float = 1.0
    source_record_id: dict[str, Any] | None = None
    source_revision: dict[str, Any] | None = None
    dedupe_key: str | None = None


@dataclass(frozen=True)
class DerivedEdgeInsertResult:
    candidates: int
    inserted: int
    skipped_existing_dedupe: int = 0
    skipped_existing_live: int = 0
    skipped_duplicate_candidate: int = 0
    # PACT sanitize-fact-text-at-admission: NUL (U+0000) admission
    # applied at this insert path too. `sanitized_value_count` counts
    # NUL→U+FFFD replacements in attrs/provenance;
    # `rejected_identity_nul` counts candidates skipped because an
    # identity-bearing field carried NUL (never silently rewritten).
    sanitized_value_count: int = 0
    rejected_identity_nul: int = 0

    @property
    def skipped(self) -> int:
        return (
            self.skipped_existing_dedupe
            + self.skipped_existing_live
            + self.skipped_duplicate_candidate
        )


def insert_deterministic_edges(
    cur,
    *,
    source: str,
    candidates: Iterable[DerivedEdgeCandidate],
    observed_at: datetime | None = None,
    run_id: uuid.UUID | str | None = None,
) -> DerivedEdgeInsertResult:
    """Insert deterministic derived edge facts after bulk no-op filters."""
    observed_at = observed_at or datetime.now(timezone.utc)
    run_uuid = str(run_id or uuid.uuid4())
    candidates = list(candidates)
    candidate_count = len(candidates)
    duplicate_count = 0
    sanitized_count = 0
    rejected_nul = 0
    first_rejected_nul: str | None = None

    # Shared NUL admission contract (PACT
    # sanitize-fact-text-at-admission): identity-bearing fields are
    # never silently rewritten — a NUL there rejects the candidate
    # loudly; attr values scrub to U+FFFD with a count. provenance
    # gets the rejection rule too: it is a closed vocabulary
    # (edge_facts_provenance_check), so a scrubbed value could never
    # be valid. Pre-fix, one NUL aborted the whole executemany with
    # UntranslatableCharacter. The identity screen runs BEFORE the
    # endpoint-lineage lookup: a NUL-bearing src/dst in that bind
    # would abort the query (poisoning the enclosing transaction for
    # non-autocommit callers) and silently degrade lineage for every
    # clean candidate in the batch.
    clean_candidates: list[DerivedEdgeCandidate] = []
    for candidate in candidates:
        rejected_fields = identity_nul_fields_from(
            src=candidate.src,
            dst=candidate.dst,
            edge_type=candidate.edge_type,
            source_record_id=candidate.source_record_id,
            source_revision=candidate.source_revision,
            dedupe_key=candidate.dedupe_key,
            provenance=candidate.provenance,
        )
        if rejected_fields:
            rejected_nul += 1
            if first_rejected_nul is None:
                first_rejected_nul = (
                    f"{candidate.src}-[{candidate.edge_type}]->"
                    f"{candidate.dst} (fields: "
                    f"{', '.join(rejected_fields)})"
                ).replace(NUL, "\\x00")[:200]
            continue
        clean_candidates.append(candidate)

    endpoint_lineage = _endpoint_lineage(cur, clean_candidates)
    rows_by_dedupe: dict[str, tuple[Any, ...]] = {}
    for candidate in clean_candidates:
        attrs = dict(candidate.attrs or {})
        if LINEAGE_ATTR not in attrs:
            attrs = attrs_with_lineage(
                attrs,
                combine_source_lineages(
                    [
                        endpoint_lineage.get(candidate.src),
                        endpoint_lineage.get(candidate.dst),
                    ]
                ),
            )
        # Scrub BEFORE deriving the dedupe key so the key stays
        # consistent with the attrs actually stored. provenance is
        # NUL-free here by the screen above.
        attrs, n_attrs = sanitize_value(attrs)
        sanitized_count += n_attrs
        source_record_id = dict(candidate.source_record_id or {
            "src_node_id": candidate.src,
            "dst_node_id": candidate.dst,
        })
        source_revision = dict(candidate.source_revision or {})
        dedupe_key = candidate.dedupe_key or stable_edge_dedupe_key(
            source=source,
            src=candidate.src,
            edge_type=candidate.edge_type,
            dst=candidate.dst,
            attrs=attrs,
            provenance=candidate.provenance,
            confidence=float(candidate.confidence),
        )
        row = (
            mint_edge_id(candidate.src, candidate.edge_type, candidate.dst),
            candidate.src,
            candidate.dst,
            candidate.edge_type,
            attrs,
            candidate.provenance,
            float(candidate.confidence),
            source,
            source_record_id,
            source_revision,
            observed_at,
            dedupe_key,
            run_uuid,
        )
        if dedupe_key in rows_by_dedupe:
            duplicate_count += 1
            continue
        rows_by_dedupe[dedupe_key] = row

    if sanitized_count or rejected_nul:
        log.warning(
            "insert_deterministic_edges: NUL (U+0000) admission for "
            "source=%s: scrubbed %d attr occurrence(s) "
            "(each replaced with U+FFFD), rejected %d candidate(s) "
            "with NUL in identity/provenance field(s) (never "
            "silently rewritten; first=%r; see PACT "
            "sanitize-fact-text-at-admission)",
            source,
            sanitized_count,
            rejected_nul,
            first_rejected_nul,
        )

    rows = list(rows_by_dedupe.values())
    if not rows:
        return DerivedEdgeInsertResult(
            candidates=candidate_count,
            inserted=0,
            skipped_duplicate_candidate=duplicate_count,
            sanitized_value_count=sanitized_count,
            rejected_identity_nul=rejected_nul,
        )

    dedupe_keys = [row[11] for row in rows]
    existing_dedupe: set[str] = set()
    for start in range(0, len(dedupe_keys), DEDUP_KEY_LOOKUP_CHUNK):
        chunk = dedupe_keys[start:start + DEDUP_KEY_LOOKUP_CHUNK]
        cur.execute(
            "SELECT DISTINCT dedupe_key FROM okg.edge_facts "
            "WHERE dedupe_key = ANY(%s)",
            (chunk,),
        )
        existing_dedupe.update(str(row[0]) for row in cur.fetchall())

    edge_ids = list({int(row[0]) for row in rows})
    from okg.substrate.library.linkers import _chronos

    # strict: an unreadable branch must not read as "no edge is live".
    live_attr_hashes: dict[int, set[str]] = {}
    for live_row in _chronos.query_strict(
        cur,
        """
        SELECT edge_id, attrs
          FROM okg.graph_edges
         WHERE edge_id = ANY(:edge_ids)
        """,
        {"edge_ids": edge_ids},
    ):
        live_attr_hashes.setdefault(int(live_row["edge_id"]), set()).add(
            _dedupe_attrs_hash(dict(live_row["attrs"] or {}))
        )

    # Issue #1730: a key already in the fact log proves the edge was emitted
    # once, not that it is still in the graph. The key carries the endpoints'
    # lineage, so an endpoint that leaves the branch and returns unchanged
    # reproduces it byte for byte after the edge itself was removed. A known
    # key is re-derived only when all three hold: the edge is not live on
    # this branch, both endpoints are live on it, and this source's NEWEST
    # fact for the key is proven to have reached the graph
    # (`_keys_whose_newest_fact_reached_the_graph`). Otherwise it stays
    # skipped: a newest fact that admission rejected, failed or quarantined
    # would only be refused again, and one that is still pending, blocked or
    # not yet admitted is the key's current attempt, so writing another would
    # pile up one fact per call.
    rederive_candidates = sorted({
        row[11]
        for row in rows
        if row[11] in existing_dedupe
        and int(row[0]) not in live_attr_hashes
        and row[1] in endpoint_lineage
        and row[2] in endpoint_lineage
    })
    rederive_keys = _keys_whose_newest_fact_reached_the_graph(
        cur, source=source, dedupe_keys=rederive_candidates,
    )

    to_insert: list[tuple[Any, ...]] = []
    skipped_dedupe = 0
    skipped_live = 0
    for row in rows:
        if row[11] in existing_dedupe and row[11] not in rederive_keys:
            skipped_dedupe += 1
            continue
        if _dedupe_attrs_hash(row[4]) in live_attr_hashes.get(
            int(row[0]),
            set(),
        ):
            skipped_live += 1
            continue
        to_insert.append(row)

    if to_insert:
        cur.executemany(
            """
            INSERT INTO okg.edge_facts
              (edge_id, src, dst, edge_type, attrs,
               provenance, confidence, source,
               source_record_id, source_revision,
               observed_at, dedupe_key, run_id, op)
            VALUES (%s, %s, %s, %s, %s::jsonb,
                    %s, %s, %s,
                    %s::jsonb, %s::jsonb,
                    %s, %s, %s, 'I')
            """,
            [
                (
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    json.dumps(row[4], sort_keys=True, default=str),
                    row[5],
                    row[6],
                    row[7],
                    json.dumps(row[8], sort_keys=True, default=str),
                    json.dumps(row[9], sort_keys=True, default=str),
                    row[10],
                    row[11],
                    row[12],
                )
                for row in to_insert
            ],
        )

    return DerivedEdgeInsertResult(
        candidates=candidate_count,
        inserted=len(to_insert),
        skipped_existing_dedupe=skipped_dedupe,
        skipped_existing_live=skipped_live,
        skipped_duplicate_candidate=duplicate_count,
        sanitized_value_count=sanitized_count,
        rejected_identity_nul=rejected_nul,
    )


def _keys_whose_newest_fact_reached_the_graph(
    cur, *, source: str, dedupe_keys: list[str],
) -> set[str]:
    """Return the keys whose newest fact from ``source`` an admission record
    shows reached the graph.

    "Newest" is the highest ``fact_id`` (insertion order) for the key within
    this source. A derived fact reaches a branch when the publisher re-applies
    it: that application opens an admission envelope with intent
    ``publisher_derived:<stage>`` and status ``accepted`` and cites the fact
    range, and the generation sweep may later move it to ``published``. So
    the newest fact counts when an envelope citing it is ``published``, or
    ``accepted`` with a ``publisher_derived:`` intent. It does not count when
    its envelopes are only ``rejected``, ``failed`` or ``quarantined`` (the
    refusals in ``okg.substrate.admission.ADMISSION_STATUSES``; writing the
    key again would be refused again), only ``pending`` or ``blocked``
    (undecided: still waiting on review or on a failed publish), only an
    ``accepted`` emission envelope (``derived_edge_emission`` is decided
    before anything is applied), or when no envelope cites it yet. An older
    fact that once reached the graph no longer approves the key: the newest
    fact is the key's current attempt, and only its outcome decides.

    Why waiting on the newest fact strands nothing that could still load:
    the publisher's pre-view loader reads only facts past the fact watermark
    it takes at the start of the cycle (``pre_derived_edge_fact_id``) and,
    in enforce mode, only facts an ``accepted`` or ``published`` envelope
    cites. An enricher run's facts are admitted right after the run
    (``_admit_enricher_outputs`` over the run's own fact range) and loaded
    later in that same cycle. So a derived fact can reach the graph only in
    the cycle that wrote it, and a newest fact with no decided envelope is
    visible here in only two situations. (a) An earlier call in the same run
    wrote it: this cycle still admits and loads it, so skipping the key is
    what lets that one fact land. (b) It was written outside any publisher
    cycle, or its cycle stopped between the write and admission: it is
    already below every later cycle's watermark, so it will never load,
    whatever this function decides. Such a fact is stranded exactly like a
    first-time key written the same way; re-deriving it here would write one
    more unloadable fact per call, which is the pile-up this rule prevents.
    ``okg enricher backfill`` admits and applies its chunk in the same
    transaction as the write, or rolls the write back, so it leaves neither.

    One query per chunk; a failed read raises.
    """
    reached: set[str] = set()
    for start in range(0, len(dedupe_keys), DEDUP_KEY_LOOKUP_CHUNK):
        chunk = dedupe_keys[start:start + DEDUP_KEY_LOOKUP_CHUNK]
        cur.execute(
            """
            WITH newest AS (
              SELECT DISTINCT ON (ef.dedupe_key)
                     ef.dedupe_key, ef.fact_id, ef.source, ef.run_id
                FROM okg.edge_facts ef
               WHERE ef.dedupe_key = ANY(%s)
                 AND ef.source = %s
               ORDER BY ef.dedupe_key, ef.fact_id DESC
            )
            SELECT n.dedupe_key
              FROM newest n
             WHERE EXISTS (
                   SELECT 1
                     FROM okg.admission_record_refs r
                     JOIN okg.admission_envelopes e
                       ON e.admission_id = r.admission_id
                    WHERE r.ref_kind = 'edge_fact_range'
                      AND r.source = n.source
                      AND (r.run_id IS NULL OR r.run_id = n.run_id)
                      AND n.fact_id BETWEEN r.fact_id_min AND r.fact_id_max
                      AND (
                        e.status = 'published'
                        OR (
                          e.status = 'accepted'
                          AND e.intent LIKE 'publisher\\_derived:%%'
                        )
                      )
             )
            """,
            (chunk, source),
        )
        reached.update(str(row[0]) for row in cur.fetchall())
    return reached


def stable_edge_dedupe_key(
    *,
    source: str,
    src: str,
    edge_type: str,
    dst: str,
    attrs: dict[str, Any] | None = None,
    provenance: str = "derived_deterministic",
    confidence: float = 1.0,
) -> str:
    payload = "|".join((
        src,
        edge_type,
        dst,
        _canonical_json_hash(dict(attrs or {})),
        provenance,
        f"{float(confidence):.8g}",
    ))
    token = uuid.uuid5(uuid.NAMESPACE_URL, payload).hex
    return f"{source}:derived:{token}:I"


def mint_edge_id(src: str, edge_type: str, dst: str) -> int:
    h = uuid.uuid5(uuid.NAMESPACE_URL, f"{src}|{edge_type}|{dst}").int
    return h & ((1 << 63) - 1)


def _canonical_json_hash(value: dict[str, Any]) -> str:
    return hashlib.md5(
        json.dumps(value, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _dedupe_attrs_hash(value: dict[str, Any]) -> str:
    attrs = dict(value)
    lineage = normalize_source_lineage(attrs.pop(LINEAGE_ATTR, None))
    source_ids = source_ids_from_lineage(lineage)
    lineage_signature: dict[str, Any]
    if source_ids:
        fingerprints_by_source = {
            str(source.get("source_id")): list(
                source.get("policy_fingerprints") or []
            )
            for source in lineage.get("sources", [])
            if str(source.get("source_id")) in source_ids
        }
        lineage_signature = {
            "source_ids": list(source_ids),
            "policy_fingerprints": fingerprints_by_source,
        }
    else:
        lineage_signature = {"status": lineage.get("status")}
    attrs[LINEAGE_ATTR] = lineage_signature
    return _canonical_json_hash(attrs)


def _endpoint_lineage(
    cur,
    candidates: Iterable[DerivedEdgeCandidate],
) -> dict[str, dict[str, Any]]:
    node_ids = sorted({
        node_id
        for candidate in candidates
        for node_id in (candidate.src, candidate.dst)
        if node_id
    })
    if not node_ids:
        return {}

    from okg.substrate.library.linkers import _chronos

    # strict: an unreadable branch must not read as "no endpoint is live"
    # (issue #1730 uses these ids as the endpoint-liveness answer).
    rows = _chronos.query_strict(
        cur,
        """
        SELECT node_id, attrs, source, source_record_id, source_revision
          FROM okg.graph_nodes
         WHERE node_id = ANY(:node_ids)
        """,
        {"node_ids": node_ids},
    )
    return {
        str(row["node_id"]): lineage_for_row(
            dict(row.get("attrs") or {}),
            source_id=row.get("source"),
            source_record_id=row.get("source_record_id"),
            source_revision=row.get("source_revision"),
        )
        for row in rows
    }
