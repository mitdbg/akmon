"""Substrate-level INSERT helpers for enrichers.

Per substrate-enricher-direct-insert-pattern-broken finding: every
enricher that emits NodeFacts / EdgeFacts directly must ship the
projector pattern:

  - edge_facts: `edge_id` (deterministic uuid5 mint),
    `source = "_enricher:<name>"`, `run_id`
  - node_facts: `source = "_enricher:<name>"`

These module-level helpers encapsulate the pattern so a
non-LLM enricher (which doesn't inherit from `LLMExtractor`) can
use the same pattern. LLM-based extractors prefer the equivalent
methods on `LLMExtractor` (`_emit_node_fact` / `_emit_edge_fact`)
which dispatch to these.

Why module-level: not every enricher inherits from
`LLMExtractor`. `DefaultProjectMembershipEnricher`,
`LatexStructuralEnricher`, `PdfStructuralEnricher`, and future
substrate-shipped enrichers are standalone classes. Sharing the
helpers via module-level functions avoids forcing a base class on
unrelated enricher styles.

These helpers are also the enricher-side chokepoint for the shared
NUL admission normalizer (PACT `sanitize-fact-text-at-admission`,
issue #638): enricher-built attrs carry raw captured text (markdown
headings, pdf/latex spans) that bypasses the SourceRunner, and
Postgres text/jsonb cannot store U+0000. Attr values are scrubbed
(NUL → U+FFFD, counted); a NUL in an identity-bearing field
(node_id / src / dst / subtype / edge_type / source_record_id /
source_revision / dedupe_key / op) — or in edge `provenance`, a
closed vocabulary where a scrubbed value could never be valid —
rejects that fact loudly instead of inserting it. Each emit returns
a `FactEmitOutcome` naming what happened.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.derived_node_liveness import DerivedSupportPolicy
from okg.substrate.images.values import check_incoming
from okg.substrate.ingest.sanitize import (
    NUL,
    identity_nul_fields_from,
    sanitize_value,
)

log = logging.getLogger(__name__)


#: Accepted values for the ``dedupe_scope`` parameter of the emit helpers.
#: See :func:`emit_node_fact` for what each one means and when to pick it.
DEDUPE_SCOPES = ("key_and_observed_at", "key")


@dataclass(frozen=True)
class FactEmitOutcome:
    """Outcome of one emit_* call.

    Per PACT `sanitize-fact-text-at-admission` (issue #638): the
    enricher emit path bypasses the SourceRunner, so the shared NUL
    normalizer is applied here too. ``emitted=False`` ⇔ the fact was
    NOT ACCEPTED, for one of two reasons:

    - ``rejected_fields`` non-empty — an identity-bearing field carried
      NUL (U+0000), which is never silently rewritten; or
    - ``held=True`` — PACT `bind-at-rest-anonymization`: the caller
      supplied a not-store-raw ``pii_posture`` and this fact carries
      free text a detector must classify, but none is installed, so the
      fact is HELD (``held_reason`` names why) rather than stored raw.

    ``emitted`` therefore answers "was this fact acceptable", NOT "did a
    row appear". A fact can be accepted and still write nothing, because
    an identical one is already stored — under either dedupe scope. That
    case is ``emitted=True, inserted=False``, and it is the ONLY way the
    two disagree. Callers counting rejections should keep reading
    ``emitted`` (a benign duplicate is not a rejection); callers
    reporting how much they actually wrote want ``inserted``.

    ``inserted`` is taken from the cursor row count, so it reports what
    Postgres did rather than what the caller asked for. Before PACT
    `add-emit-once-dedupe-scope` (issue #901) there was no such field
    and ``emitted=True`` was returned unconditionally, which overstated
    every row the ON CONFLICT clause suppressed.

    ``sanitized_value_count`` counts NUL→U+FFFD replacements applied
    to attr values before the insert.
    """

    emitted: bool
    sanitized_value_count: int = 0
    rejected_fields: tuple[str, ...] = ()
    held: bool = False
    held_reason: str | None = None
    inserted: bool = False


def _check_dedupe_scope(dedupe_scope: str) -> None:
    """Reject an unrecognised dedupe scope before touching the database.

    Neither mode is a safe default to fall back to: guessing
    ``"key_and_observed_at"`` would silently reinstate the per-cycle
    ledger growth the caller asked to avoid, and guessing ``"key"``
    would silently suppress facts the caller expected to keep. So a
    typo raises.
    """
    if dedupe_scope not in DEDUPE_SCOPES:
        raise ValueError(
            f"dedupe_scope must be one of {DEDUPE_SCOPES!r}, "
            f"got {dedupe_scope!r}"
        )


def _log_preview(text: str) -> str:
    return text.replace(NUL, "\\x00")[:200]


def _apply_pii_posture_to_attrs(
    attrs: dict[str, Any],
    *,
    pii_posture: Any,
    emit_label: str,
    enricher_name: str,
) -> tuple[dict[str, Any] | None, str | None]:
    """At-rest PII transform for the enricher chokepoint — PACT
    `bind-at-rest-anonymization`.

    No-op (returns ``attrs`` unchanged) when ``pii_posture`` is absent or a
    store_raw posture — every enricher today. Under a not-store-raw posture the
    attrs are pseudonymized / masked via the SHARED attrs primitive
    (:func:`okg.substrate.ingest.pii_sanitize.sanitize_attrs_pii`); a fact
    carrying free text with no detector installed returns ``(None, reason)`` so
    the caller HOLDS it (``emitted=False``) rather than inserting raw.
    """
    if pii_posture is None or not getattr(pii_posture, "active", False):
        return attrs, None
    from okg.substrate.ingest.pii_sanitize import sanitize_attrs_pii

    transformed, held_reason = sanitize_attrs_pii(
        attrs,
        classifier=pii_posture.classifier,
        store_raw=False,
        pseudonym_service=pii_posture.pseudonym_service,
        detector=pii_posture.detector,
    )
    if transformed is None:
        log.warning(
            "%s: HOLDING fact from enricher=%s under a not-store-raw posture: "
            "%s (no raw PII stored; install the 'privacy' extra to enable "
            "free-text PII detection)",
            emit_label,
            enricher_name,
            held_reason,
        )
    return transformed, held_reason


#: Source prefix every fact these helpers write carries -- see the
#: ``source = f"_enricher:{{enricher_name}}"`` bindings below. It is what
#: makes a PARTIAL index viable for the emit-once probe.
# Four whole, literal statements rather than one assembled from parts.
#
# This module is watched by TWO independent tripwires, and an assembled
# statement defeats both. `tests/substrate/ingest/test_at_rest_binding.py`
# regexes source text for `INSERT INTO okg.node_facts` to enumerate every
# fact writer for at-rest PII review. `okg.substrate.write_inventory`
# (asserted by tests/substrate/fact_batch/test_write_inventory.py) walks
# the AST and reads the SQL out of each `cur.execute(...)` first argument,
# resolving only string literals and names bound to string literals -- a
# builder call returns None there, and the callsite silently drops out of
# the inventory of graph-mutation authority.
#
# So the SQL stays literal and the dedupe scope is chosen by picking a
# whole statement. The duplication is bounded (2 tables x 2 scopes) and
# each statement can be read and audited on its own.
#
# The emit-once variants differ from the historical ones ONLY by the
# `WHERE NOT EXISTS` probe. That probe keys on (source, dedupe_key, op);
# all three are load-bearing and none may be dropped:
#
#   - `source` -- dedupe_key uniqueness is only conventionally namespaced
#     (some producers prefix it, some do not), so a probe on the key
#     alone would let one producer permanently suppress another's fact
#     that happened to hash to the same string.
#   - `op` -- a retraction ('R') and the assertion it retracts ('I') may
#     legitimately share a dedupe key. A probe blind to `op` would find
#     the assertion, drop the retraction, and leave the edge live forever
#     with nothing logged.
#
# The `source LIKE` line is redundant against the equality above and is
# NOT decoration: the supporting index is PARTIAL on that same predicate,
# so the two hottest tables in the system do not carry a full-width btree
# for a probe only these helpers issue. A parameterised `source = $1`
# alone does not let the planner prove the index predicate holds, so the
# probe would fall back to scanning every chunk. Spelling the LIKE out as
# a constant makes the implication provable; `%` is doubled because
# psycopg reads a single one as a placeholder.
# test_probe_can_use_the_partial_index fails if this stops being true.
#
# `op` is cast `::text`, never `::char(1)`: an explicit cast to the
# column's own type TRUNCATES an out-of-vocabulary op instead of raising.

_NODE_FACT_INSERT = """
    INSERT INTO okg.node_facts
      (node_id, subtype, attrs, source, source_record_id,
       source_revision, observed_at, dedupe_key, run_id, op)
    VALUES (%(node_id)s::text, %(subtype)s::text, %(attrs)s::jsonb,
            %(source)s::text, %(srcrec)s::jsonb, %(srcrev)s::jsonb,
            %(observed_at)s::timestamptz, %(dk)s::text,
            %(run_id)s::uuid, %(op)s::text)
    ON CONFLICT (dedupe_key, observed_at) DO NOTHING
"""

_NODE_FACT_INSERT_EMIT_ONCE = """
    INSERT INTO okg.node_facts
      (node_id, subtype, attrs, source, source_record_id,
       source_revision, observed_at, dedupe_key, run_id, op)
    SELECT %(node_id)s::text, %(subtype)s::text, %(attrs)s::jsonb,
           %(source)s::text, %(srcrec)s::jsonb, %(srcrev)s::jsonb,
           %(observed_at)s::timestamptz, %(dk)s::text,
           %(run_id)s::uuid, %(op)s::text
     WHERE NOT EXISTS (
       SELECT 1 FROM okg.node_facts
        WHERE source = %(source)s
          AND source LIKE '\\_enricher:%%'
          AND dedupe_key = %(dk)s
          AND op = %(op)s
     )
    ON CONFLICT (dedupe_key, observed_at) DO NOTHING
"""

_EDGE_FACT_INSERT = """
    INSERT INTO okg.edge_facts
      (edge_id, src, dst, edge_type, attrs, provenance,
       confidence, source, source_record_id, source_revision,
       observed_at, dedupe_key, run_id, op)
    VALUES (%(edge_id)s::bigint, %(src)s::text, %(dst)s::text,
            %(edge_type)s::text, %(attrs)s::jsonb, %(prov)s::text,
            %(conf)s::real, %(source)s::text, %(srcrec)s::jsonb,
            %(srcrev)s::jsonb, %(observed_at)s::timestamptz,
            %(dk)s::text, %(run_id)s::uuid, %(op)s::text)
    ON CONFLICT (dedupe_key, observed_at) DO NOTHING
"""

_EDGE_FACT_INSERT_EMIT_ONCE = """
    INSERT INTO okg.edge_facts
      (edge_id, src, dst, edge_type, attrs, provenance,
       confidence, source, source_record_id, source_revision,
       observed_at, dedupe_key, run_id, op)
    SELECT %(edge_id)s::bigint, %(src)s::text, %(dst)s::text,
           %(edge_type)s::text, %(attrs)s::jsonb, %(prov)s::text,
           %(conf)s::real, %(source)s::text, %(srcrec)s::jsonb,
           %(srcrev)s::jsonb, %(observed_at)s::timestamptz,
           %(dk)s::text, %(run_id)s::uuid, %(op)s::text
     WHERE NOT EXISTS (
       SELECT 1 FROM okg.edge_facts
        WHERE source = %(source)s
          AND source LIKE '\\_enricher:%%'
          AND dedupe_key = %(dk)s
          AND op = %(op)s
     )
    ON CONFLICT (dedupe_key, observed_at) DO NOTHING
"""


_DEDUPE_SCOPE_DOC = """
    ``dedupe_scope`` selects the grain at which a re-emission is
    suppressed:

    - ``"key_and_observed_at"`` (default, and the only behaviour before
      issue #901): only two copies landing at the SAME instant collapse.
      A producer that re-derives the same fact every publish cycle
      supplies a new ``observed_at`` each time — as a real observation
      time must — so it appends a new fact-log row every cycle.
    - ``"key"``: emit-once. The fact is not inserted if a row with the
      same ``(source, dedupe_key, op)`` already exists, whatever its
      ``observed_at``. This is for DETERMINISTIC derived facts — an
      alias edge, a scaffold reference — that are true once and stay
      true. It exists so such a producer does not have to either
      fabricate a constant ``observed_at`` (which corrupts every
      freshness, ordering and retention path that reads it as a real
      observation time) or hand-roll its own INSERT and lose the
      envelope, NUL scrubbing and PII posture these helpers apply.

    Two limits of ``"key"`` are contract, not accident:

    - It is "once per RETENTION WINDOW", not "once, ever". The probe
      reads the fact table, which carries a 180-day retention policy, so
      a fact whose row has aged out is emittable again — and if its
      retraction aged out too, the publisher can re-project it. Probing
      a side ledger instead would make it "once, ever" at the cost of
      permanently suppressing facts whose evidence no longer exists,
      which is the worse failure.
    - It suppresses re-assertion after retraction. The sequence assert →
      retract → assert leaves the trailing assert unwritten, because the
      first assert is still in the log. A producer whose facts can
      become false and then true again must NOT use this scope.

    One asymmetry is worth knowing before it surprises someone: the
    emit-once probe is scoped by ``source``, but the retained
    same-instant ``UNIQUE (dedupe_key, observed_at)`` is NOT. Two
    producers that happen to share a dedupe key therefore already
    collapse into one row when they emit at the same instant — that is
    pre-existing behaviour, unchanged here — while under ``"key"`` they
    each keep their own row across instants.

    The probe is a ``WHERE NOT EXISTS`` guard rather than an ON CONFLICT
    against a unique index because no such index can exist:
    ``okg.node_facts`` and ``okg.edge_facts`` are Timescale hypertables
    partitioned on ``observed_at``, and Timescale rejects a unique index
    that omits the partitioning column. The guard is therefore NOT
    atomic against a concurrent writer inserting the same key at a
    different ``observed_at`` in an overlapping transaction. The cost of
    that race is bounded and one-directional: one extra fact-log row —
    exactly what every caller gets today with no emit-once mode at all.
    It cannot lose a fact, mint a different edge id, or corrupt the
    ledger. The same-instant ON CONFLICT clause is retained under both
    scopes and remains atomic.
"""


def emit_node_fact(
    conn: psycopg.Connection,
    *,
    enricher_name: str,
    node_id: str,
    subtype: str,
    attrs: dict[str, Any],
    dedupe_key: str,
    source_record_id: dict[str, Any],
    source_revision: dict[str, Any],
    run_id: str,
    observed_at: datetime,
    op: str = "I",
    pii_posture: Any = None,
    derived_from: Sequence[str] | None = None,
    support_policy: DerivedSupportPolicy,
    dedupe_scope: str = "key_and_observed_at",
) -> FactEmitOutcome:
    """INSERT one okg.node_facts row with the projector pattern.

    Always sets `source = "_enricher:<enricher_name>"`. ON CONFLICT
    (dedupe_key, observed_at) DO NOTHING.

    PACT `enforce-derived-node-liveness`: every live enricher node carries the
    exact versioned ``source_record_id.derived_support`` object. ``derived_from``
    names the graph
    node ids this node was produced from. Unlike an edge, a node has no
    structural endpoints the substrate can inherit provenance from, so
    only the emitting enricher knows this. A live assertion without an exact
    declaration is refused before insertion; no malformed derived node is
    allowed into the fact ledger. The ids are recorded under
    ``source_record_id["derived_from"]``; the publisher resolves them to
    their lineage and combines it through the existing meet at apply
    time, where a Chronos session exists to read them. Support is never
    guessed from graph adjacency, which would launder trust while looking
    principled.

    Postgres text/jsonb cannot store NUL (U+0000): attr values are
    scrubbed (NUL → U+FFFD, counted in the returned outcome); a NUL
    in an identity-bearing field rejects the fact loudly (warning
    log + ``emitted=False``, no insert) instead of aliasing the
    identity.

    PACT `bind-at-rest-anonymization`: when ``pii_posture`` is a
    not-store-raw posture (``active``), the attrs are pseudonymized /
    masked before the insert via the shared attrs primitive
    (:func:`okg.substrate.ingest.pii_sanitize.sanitize_attrs_pii`), and a
    fact carrying free text with no detector is HELD (``emitted=False``,
    ``held=True``) — never inserted raw. ``pii_posture`` None (the
    default, every enricher today) is a no-op.

    ``dedupe_scope`` is documented below, shared with
    :func:`emit_edge_fact`.

    Image values in ``attrs`` pass `okg.substrate.images.values.check_incoming`
    first: one carrying ``png`` or ``error`` raises ``ImageValueForged`` and a
    malformed one ``ImageRefInvalid``, before anything is written. Only the
    ingest image step completes an image value.
    """
    _check_dedupe_scope(dedupe_scope)
    check_incoming(attrs)
    rejected = identity_nul_fields_from(
        node_id=node_id,
        subtype=subtype,
        source_record_id=source_record_id,
        source_revision=source_revision,
        dedupe_key=dedupe_key,
        op=op,
    )
    if rejected:
        log.warning(
            "emit_node_fact: rejecting node fact from enricher=%s: "
            "NUL (U+0000) in identity field(s) %s (identity is never "
            "silently rewritten; fact skipped, node_id=%r; see PACT "
            "sanitize-fact-text-at-admission)",
            enricher_name,
            ", ".join(rejected),
            _log_preview(node_id),
        )
        return FactEmitOutcome(emitted=False, rejected_fields=rejected)
    if op.upper() == "I":
        from okg.substrate.derived_node_liveness import canonical_derived_support

        producer = f"_enricher:{enricher_name}"
        support, canonical_members = canonical_derived_support(
            producer=producer,
            members=derived_from,
            policy=support_policy,
        )
        supplied_support = dict(source_record_id).get("derived_support")
        if supplied_support is not None and supplied_support != support:
            raise ValueError(
                "source_record_id.derived_support conflicts with the canonical "
                "enricher support declaration"
            )
        source_record_id = {
            **dict(source_record_id),
            "derived_from": canonical_members,
            "derived_support": support,
        }
    attrs, sanitized_count = sanitize_value(attrs)
    if sanitized_count:
        log.warning(
            "emit_node_fact: scrubbed %d NUL (U+0000) occurrence(s) "
            "from attrs (each replaced with U+FFFD) for enricher=%s "
            "node_id=%s",
            sanitized_count,
            enricher_name,
            node_id,
        )
    attrs, held_reason = _apply_pii_posture_to_attrs(
        attrs,
        pii_posture=pii_posture,
        emit_label="emit_node_fact",
        enricher_name=enricher_name,
    )
    if attrs is None:
        return FactEmitOutcome(emitted=False, held=True, held_reason=held_reason)
    if pii_posture is not None and getattr(pii_posture, "active", False):
        from okg.substrate.ingest.pii_sanitize import sanitize_provenance_pii

        source_record_id = sanitize_provenance_pii(
            source_record_id,
            field_name="source_record_id",
            pseudonym_service=pii_posture.pseudonym_service,
        )
        source_revision = sanitize_provenance_pii(
            source_revision,
            field_name="source_revision",
            pseudonym_service=pii_posture.pseudonym_service,
        )
    with conn.cursor() as cur:
        # Two whole statements, each passed as a literal-bound name, so
        # both stay visible to the write inventory. See the statement
        # constants above.
        params = {
            "node_id": node_id,
            "subtype": subtype,
            "attrs": Jsonb(attrs),
            "source": f"_enricher:{enricher_name}",
            "srcrec": Jsonb(source_record_id),
            "srcrev": Jsonb(source_revision),
            "observed_at": observed_at,
            "dk": dedupe_key,
            "run_id": run_id,
            "op": op,
        }
        if dedupe_scope == "key":
            cur.execute(_NODE_FACT_INSERT_EMIT_ONCE, params)
        else:
            cur.execute(_NODE_FACT_INSERT, params)
        inserted = cur.rowcount == 1
    return FactEmitOutcome(
        emitted=True,
        sanitized_value_count=sanitized_count,
        inserted=inserted,
    )


def mint_edge_id(src: str, edge_type: str, dst: str) -> int:
    """Deterministic edge_id from uuid5(NAMESPACE_URL,
    f"{src}|{edge_type}|{dst}").

    Mirrors `projector._mint_edge_id`. Same (src, edge_type, dst)
    always produces the same int across runs, so an enricher's
    re-emission of the same edge naturally dedupes.
    """
    h = uuid.uuid5(uuid.NAMESPACE_URL, f"{src}|{edge_type}|{dst}").int
    return h & ((1 << 63) - 1)


def emit_edge_fact(
    conn: psycopg.Connection,
    *,
    enricher_name: str,
    src: str,
    dst: str,
    edge_type: str,
    attrs: dict[str, Any],
    provenance: str,
    confidence: float,
    dedupe_key: str,
    source_record_id: dict[str, Any],
    source_revision: dict[str, Any],
    run_id: str,
    observed_at: datetime,
    op: str = "I",
    pii_posture: Any = None,
    dedupe_scope: str = "key_and_observed_at",
) -> FactEmitOutcome:
    """INSERT one okg.edge_facts row with the projector pattern.

    Mints a deterministic `edge_id` from `mint_edge_id(src,
    edge_type, dst)`. Sets `source = "_enricher:<enricher_name>"`.
    ON CONFLICT (dedupe_key, observed_at) DO NOTHING.

    Postgres text/jsonb cannot store NUL (U+0000): attr values are
    scrubbed (NUL → U+FFFD, counted in the returned outcome); a NUL
    in an identity-bearing field — or in ``provenance``, a closed
    vocabulary (``edge_facts_provenance_check``) where a scrubbed
    value could never be valid — rejects the fact loudly (warning
    log + ``emitted=False``, no insert) instead of aliasing the
    identity.

    PACT `bind-at-rest-anonymization`: a not-store-raw ``pii_posture``
    pseudonymizes / masks the attrs before insert, and HOLDS
    (``emitted=False``, ``held=True``) a free-text fact with no detector
    rather than storing it raw. ``pii_posture`` None (the default) is a
    no-op.

    ``dedupe_scope`` is documented below, shared with
    :func:`emit_node_fact`.

    Image values in ``attrs`` are checked as in :func:`emit_node_fact`.
    """
    _check_dedupe_scope(dedupe_scope)
    check_incoming(attrs)
    rejected = identity_nul_fields_from(
        src=src,
        dst=dst,
        edge_type=edge_type,
        source_record_id=source_record_id,
        source_revision=source_revision,
        dedupe_key=dedupe_key,
        op=op,
        provenance=provenance,
    )
    if rejected:
        log.warning(
            "emit_edge_fact: rejecting edge fact from enricher=%s: "
            "NUL (U+0000) in identity field(s) %s (identity is never "
            "silently rewritten; fact skipped, edge=%r; see PACT "
            "sanitize-fact-text-at-admission)",
            enricher_name,
            ", ".join(rejected),
            _log_preview(f"{src}-[{edge_type}]->{dst}"),
        )
        return FactEmitOutcome(emitted=False, rejected_fields=rejected)
    attrs, sanitized_count = sanitize_value(attrs)
    if sanitized_count:
        log.warning(
            "emit_edge_fact: scrubbed %d NUL (U+0000) occurrence(s) "
            "from attrs (each replaced with U+FFFD) for "
            "enricher=%s edge=%s-[%s]->%s",
            sanitized_count,
            enricher_name,
            src,
            edge_type,
            dst,
        )
    attrs, held_reason = _apply_pii_posture_to_attrs(
        attrs,
        pii_posture=pii_posture,
        emit_label="emit_edge_fact",
        enricher_name=enricher_name,
    )
    if attrs is None:
        return FactEmitOutcome(emitted=False, held=True, held_reason=held_reason)
    if pii_posture is not None and getattr(pii_posture, "active", False):
        from okg.substrate.ingest.pii_sanitize import sanitize_provenance_pii

        source_record_id = sanitize_provenance_pii(
            source_record_id,
            field_name="source_record_id",
            pseudonym_service=pii_posture.pseudonym_service,
        )
        source_revision = sanitize_provenance_pii(
            source_revision,
            field_name="source_revision",
            pseudonym_service=pii_posture.pseudonym_service,
        )
    edge_id = mint_edge_id(src, edge_type, dst)
    with conn.cursor() as cur:
        # See emit_node_fact: whole literal statements, one per scope,
        # so the write inventory can still read the SQL off the callsite.
        params = {
            "edge_id": edge_id,
            "src": src,
            "dst": dst,
            "edge_type": edge_type,
            "attrs": Jsonb(attrs),
            "prov": provenance,
            "conf": float(confidence),
            "source": f"_enricher:{enricher_name}",
            "srcrec": Jsonb(source_record_id),
            "srcrev": Jsonb(source_revision),
            "observed_at": observed_at,
            "dk": dedupe_key,
            "run_id": run_id,
            "op": op,
        }
        if dedupe_scope == "key":
            cur.execute(_EDGE_FACT_INSERT_EMIT_ONCE, params)
        else:
            cur.execute(_EDGE_FACT_INSERT, params)
        inserted = cur.rowcount == 1
    return FactEmitOutcome(
        emitted=True,
        sanitized_value_count=sanitized_count,
        inserted=inserted,
    )


# The `dedupe_scope` contract is identical for both emitters and long
# enough that keeping two copies in sync by hand would not survive an
# edit. Appended at import so `help()` and the API docs show it on both.
# `python -OO` strips docstrings, hence the `or ""`.
for _emitter in (emit_node_fact, emit_edge_fact):
    _emitter.__doc__ = (_emitter.__doc__ or "") + _DEDUPE_SCOPE_DOC
del _emitter


__all__ = [
    "DEDUPE_SCOPES",
    "FactEmitOutcome",
    "emit_node_fact",
    "emit_edge_fact",
    "mint_edge_id",
]
