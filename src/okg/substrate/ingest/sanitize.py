"""NUL (U+0000) sanitization at fact admission.

Per PACT ``sanitize-fact-text-at-admission`` (GitHub issue #638):
Postgres ``text``/``jsonb`` cannot store the NUL character, so a
single adapter- or enricher-emitted string containing U+0000 aborted
the chronos splice COPY (``UntranslatableCharacter``) and took down
the ENTIRE source run and its publish layer. Admission therefore
guarantees every string reaching persistence for a fact is
Postgres-storable, with rules that differ by field class:

- **Attr values** (nested dicts / lists / tuples — string keys AND
  string values): every NUL occurrence is replaced with U+FFFD
  (REPLACEMENT CHARACTER) and counted. Non-string leaves pass
  through untouched.
- **Identity-bearing fields** (``node_id`` / ``src`` / ``dst`` /
  ``subtype`` / ``edge_type`` / ``source_record_id`` /
  ``source_revision`` / ``op``) are NEVER silently rewritten —
  replacement would alias distinct identities and mutate lineage
  (and rewriting ``op`` could flip insert/retract semantics). A NUL
  there rejects that fact loudly: the caller skips it, counts it,
  and surfaces the count in the per-source run report while the
  rest of the run continues. Edge ``provenance`` gets the same
  rejection rule for a different reason: it is a closed vocabulary
  (``edge_facts_provenance_check``), so a scrubbed value could
  never be valid — it would only trade the abort for a
  CheckViolation.

Source-declared record-set cursors mirror fact identities for incremental
retraction. They are part of the same persistence boundary: rejected fact
identities are removed from the record set and edge attrs are scrubbed before
the cursor can reach JSONB.

The SHARED normalizer is applied at BOTH emit paths — the
source-runner admission pass (``SourceRunner._sanitize_facts`` in
``okg.substrate.ingest.runner``) and the enricher fact emitters
(``okg.substrate.library.enrichers._fact_emitters``) — not
per-adapter.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Optional, Union

from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
)

__all__ = [
    "FactSanitization",
    "NUL",
    "REPLACEMENT_CHAR",
    "RecordSetSanitization",
    "fact_identity_preview",
    "identity_nul_fields",
    "identity_nul_fields_from",
    "sanitize_fact",
    "sanitize_record_set",
    "sanitize_value",
    "scrub_nul",
    "value_contains_nul",
]

NUL = "\x00"
#: The documented replacement for a NUL occurrence in an attr value:
#: U+FFFD REPLACEMENT CHARACTER.
REPLACEMENT_CHAR = "\ufffd"

#: Identity-bearing fields per fact shape. A NUL in any of these
#: rejects the fact (replacement would alias distinct identities).
_NODE_IDENTITY_FIELDS = (
    "node_id",
    "subtype",
    "source_record_id",
    "source_revision",
    "op",
)
_EDGE_IDENTITY_FIELDS = (
    "src",
    "dst",
    "edge_type",
    "source_record_id",
    "source_revision",
    "op",
    # Not identity, but the same rejection rule: edge provenance is a
    # CLOSED vocabulary enforced by edge_facts_provenance_check —
    # scrubbing NUL→U+FFFD can never produce a valid member, so a
    # scrub would just trade UntranslatableCharacter for a
    # CheckViolation abort at insert.
    "provenance",
)


def scrub_nul(text: str) -> tuple[str, int]:
    """Replace each NUL in ``text`` with U+FFFD.

    Returns ``(sanitized, replacement_count)``; the original object
    is returned unchanged when it contains no NUL.
    """
    count = text.count(NUL)
    if not count:
        return text, 0
    return text.replace(NUL, REPLACEMENT_CHAR), count


def sanitize_value(value: Any) -> tuple[Any, int]:
    """Recursively scrub NUL from every string inside ``value``.

    Handles nested dicts (string keys AND values), lists, and
    tuples. Non-string leaves (ints, floats, bools, None, ...) pass
    through untouched. Returns ``(sanitized, replacement_count)``;
    when the count is 0 the ORIGINAL object is returned (no copy).

    Pathological edge: two dict keys that differ only by NUL vs
    U+FFFD collide after scrubbing — the later entry wins, and every
    replacement is still counted, so the loss is visible in the
    count rather than silent.
    """
    if isinstance(value, str):
        return scrub_nul(value)
    if isinstance(value, dict):
        total = 0
        out: dict[Any, Any] = {}
        for key, item in value.items():
            if isinstance(key, str):
                new_key, n_key = scrub_nul(key)
            else:
                new_key, n_key = key, 0
            new_item, n_item = sanitize_value(item)
            out[new_key] = new_item
            total += n_key + n_item
        if not total:
            return value, 0
        return out, total
    if isinstance(value, (list, tuple)):
        total = 0
        items = []
        for item in value:
            new_item, n_item = sanitize_value(item)
            items.append(new_item)
            total += n_item
        if not total:
            return value, 0
        return (tuple(items) if isinstance(value, tuple) else items), total
    return value, 0


def value_contains_nul(value: Any) -> bool:
    """True when any string (dict key or value, nested) contains NUL."""
    if isinstance(value, str):
        return NUL in value
    if isinstance(value, dict):
        return any(
            (isinstance(key, str) and NUL in key) or value_contains_nul(item)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(value_contains_nul(item) for item in value)
    return False


def identity_nul_fields(fact: Union[NodeFact, EdgeFact]) -> tuple[str, ...]:
    """Names of identity-bearing fields on ``fact`` that contain NUL.

    Empty tuple ⇔ the fact's identity is Postgres-storable and the
    fact is eligible for attr-value scrubbing; non-empty ⇔ the fact
    MUST be rejected (identity is never silently rewritten).
    """
    fields = (
        _NODE_IDENTITY_FIELDS
        if isinstance(fact, NodeFact)
        else _EDGE_IDENTITY_FIELDS
    )
    return tuple(
        name for name in fields if value_contains_nul(getattr(fact, name))
    )


def identity_nul_fields_from(**fields: Any) -> tuple[str, ...]:
    """Names among the given identity-bearing ``name=value`` pairs
    whose value contains NUL (``None`` values are skipped).

    Keyword form of :func:`identity_nul_fields` for callers that
    hold identity components as loose arguments rather than a
    NodeFact/EdgeFact — e.g. the enricher fact emitters, which also
    pass their caller-supplied ``dedupe_key`` (rewriting it could
    alias two distinct dedupe identities, so it gets the identity
    rule, not the scrub rule).
    """
    return tuple(
        name
        for name, value in fields.items()
        if value is not None and value_contains_nul(value)
    )


@dataclass(frozen=True)
class FactSanitization:
    """Outcome of sanitizing one NodeFact / EdgeFact.

    ``fact is None`` ⇔ rejected: an identity-bearing field carried
    NUL (named in ``rejected_fields``); the fact must be skipped,
    counted, and reported. Otherwise ``fact`` is the storable fact —
    the ORIGINAL object when nothing needed scrubbing — and
    ``sanitized_value_count`` counts NUL→U+FFFD replacements applied
    to its attr values.
    """

    fact: Optional[Union[NodeFact, EdgeFact]]
    sanitized_value_count: int = 0
    rejected_fields: tuple[str, ...] = ()

    @property
    def rejected(self) -> bool:
        return self.fact is None


@dataclass(frozen=True)
class RecordSetSanitization:
    """Outcome of applying fact admission rules to a record-set cursor."""

    record_set: RecordSet
    dropped_record_count: int = 0
    dropped_node_count: int = 0
    dropped_edge_count: int = 0
    sanitized_value_count: int = 0

    @property
    def changed(self) -> bool:
        return bool(
            self.dropped_record_count
            or self.dropped_node_count
            or self.dropped_edge_count
            or self.sanitized_value_count
        )


def sanitize_record_set(record_set: RecordSet) -> RecordSetSanitization:
    """Make a source record set consistent with fact admission.

    A record set is durable retraction authority, so it must never claim an
    identity that admission rejected. Record keys and node/edge identity
    fields containing NUL are dropped; edge attrs use the same NUL→U+FFFD
    normalization as :func:`sanitize_fact`. Empty, clean record emissions are
    retained because they still prove that a source record exists but emits no
    graph facts.
    """
    records: dict[str, RecordEmission] = {}
    dropped_records = 0
    dropped_nodes = 0
    dropped_edges = 0
    sanitized_values = 0

    for record_id, emission in record_set.records.items():
        if value_contains_nul(record_id):
            dropped_records += 1
            dropped_nodes += len(emission.nodes_by_id())
            dropped_edges += len(emission.edges_by_identity())
            continue

        def clean_node(node: RecordNode | None) -> RecordNode | None:
            nonlocal dropped_nodes
            if node is None:
                return None
            bad = identity_nul_fields_from(
                node_id=node.node_id,
                subtype=node.subtype,
                source_record_id=node.source_record_id,
                source_revision=node.source_revision,
            )
            if bad:
                dropped_nodes += 1
                return None
            return node

        primary = clean_node(emission.primary)
        descendants = [
            clean
            for node in emission.descendants
            if (clean := clean_node(node)) is not None
        ]
        edges: list[EdgeKey] = []
        for edge in emission.edges:
            bad = identity_nul_fields_from(
                src=edge.src,
                dst=edge.dst,
                edge_type=edge.edge_type,
                provenance=edge.provenance,
                source_record_id=edge.source_record_id,
                source_revision=edge.source_revision,
            )
            if bad:
                dropped_edges += 1
                continue
            attrs, count = sanitize_value(edge.attrs)
            sanitized_values += count
            edges.append(replace(edge, attrs=attrs) if count else edge)
        records[record_id] = RecordEmission(
            primary=primary,
            descendants=descendants,
            edges=edges,
        )

    outcome = RecordSetSanitization(
        record_set=record_set,
        dropped_record_count=dropped_records,
        dropped_node_count=dropped_nodes,
        dropped_edge_count=dropped_edges,
        sanitized_value_count=sanitized_values,
    )
    if not outcome.changed:
        return outcome
    return replace(outcome, record_set=RecordSet(records=records))


def fact_identity_preview(fact: Union[NodeFact, EdgeFact]) -> str:
    """Short printable identity for log lines, NUL made visible."""
    if isinstance(fact, NodeFact):
        raw = f"node:{fact.subtype}:{fact.node_id}"
    else:
        raw = f"edge:{fact.edge_type}:{fact.src}->{fact.dst}"
    return raw.replace(NUL, "\\x00")[:200]


def sanitize_fact(
    fact: Union[NodeFact, EdgeFact],
) -> FactSanitization:
    """Apply the admission normalization contract to one fact.

    NUL in an identity-bearing field ⇒ rejection (``fact=None``,
    offending fields named). Edge ``provenance`` gets the SAME
    rejection rule — not because it is identity, but because it is a
    closed vocabulary (``edge_facts_provenance_check``): scrubbing
    could never produce a valid member, only a different abort.
    Otherwise the fact's ``attrs`` are scrubbed NUL→U+FFFD with a
    replacement count; a fact needing no change is returned by
    reference.
    """
    bad = identity_nul_fields(fact)
    if bad:
        return FactSanitization(fact=None, rejected_fields=bad)
    new_attrs, n_attrs = sanitize_value(fact.attrs)
    if not n_attrs:
        return FactSanitization(fact=fact)
    return FactSanitization(
        fact=replace(fact, attrs=new_attrs),
        sanitized_value_count=n_attrs,
    )
