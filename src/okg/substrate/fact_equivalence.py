"""When is a fact unchanged? The one rule.

A candidate fact is unchanged when every attribute it would store equals the
stored row's, compared as JSON, with two exclusions:

* the volatile fields its node subtype declares (``okg_volatile_fields`` in
  the ontology, for example ``Document.parsed_at``) -- per subtype, never a
  union across subtypes (issue #2334); edge types declare none;
* the source revision inside the lineage envelope
  (``nomos_source_lineage``): who asserted the fact, for which records, under
  which policy, and whether the record detail is complete still count, but a
  revision that moved with no content change does not (issues #2076, #1731).

Edges also compare their ``provenance`` column and their ``confidence``
column at the precision the ``REAL`` column stores.

The source runner's live-branch dedupe, its legacy fact-log dedupe and the
graph writer's no-op check all decide with this module (issues #2335,
#2336), so a fact the runner lets through for a revision-only change can no
longer be skipped by one writer and rewritten by another.
"""
from __future__ import annotations

import json
import struct
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any


#: The lineage attribute every writer stamps (``nomos/source_lineage.py``).
#: Kept literal so this module imports nothing from ``okg``; a test pins it.
LINEAGE_ATTR = "nomos_source_lineage"

#: Lineage keys that carry a source revision.
LINEAGE_REVISION_KEYS: frozenset[str] = frozenset({"source_revision", "revision"})

#: Marks an absent key, which differs from a key whose value is null.
MISSING: Any = object()


def canonical(value: Any) -> str:
    """JSON form used for equality.

    JSON equality, not Python equality: ``True == 1`` and ``1 == 1.0`` in
    Python but not in JSONB. An absent key (:data:`MISSING`) differs from a
    null one.
    """
    if value is MISSING:
        return "\x00absent"
    try:
        return json.dumps(value, sort_keys=True, default=str)
    except TypeError:
        # Keys of mixed type ({1: .., "b": ..}) cannot be sorted. JSON stores
        # every key as a string, so compare them that way.
        return json.dumps(_string_keys(value), sort_keys=True, default=str)


def _string_keys(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _string_keys(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_string_keys(item) for item in value]
    return value


def _without_revision(record: Any) -> Any:
    if not isinstance(record, Mapping):
        return record
    return {
        key: value for key, value in record.items()
        if key not in LINEAGE_REVISION_KEYS
    }


def lineage_for_content(lineage: Any) -> Any:
    """The lineage envelope with every source revision removed.

    The normalized envelope (``nomos/source_lineage.py``) carries:

    * top level: schema, status, source_ids, sources, and record_detail (the
      truncation marker) -- all kept;
    * per source: source_id, source_record_ids, policy_fingerprints,
      record_detail_truncated -- kept; source_revisions -- removed;
      source_records[] of {source_record_id, source_revision} -- the record
      id kept, the revision removed.

    Every revision-bearing key is removed at every level, including a
    top-level one a legacy envelope may carry.
    ``enrichers/derived_edges._dedupe_attrs_hash`` is deliberately different:
    it also drops record ids, the status and the truncation marker.
    """
    if not isinstance(lineage, Mapping):
        return lineage
    reduced = {
        key: value for key, value in lineage.items()
        if key not in LINEAGE_REVISION_KEYS and key != "source_revisions"
    }
    if "sources" not in lineage:
        return reduced
    sources = []
    for source in lineage.get("sources") or []:
        if not isinstance(source, Mapping):
            sources.append(source)
            continue
        entry = {
            key: value for key, value in source.items()
            if key not in LINEAGE_REVISION_KEYS and key != "source_revisions"
        }
        if isinstance(entry.get("source_records"), list):
            # The envelope sorts records by a fingerprint that includes the
            # revision, and one record id may appear once per revision;
            # re-sort and collapse once the revision is removed so neither the
            # order nor the count can move with the revision alone.
            projected = {
                canonical(record): record
                for record in (
                    _without_revision(r) for r in entry["source_records"]
                )
            }
            entry["source_records"] = [projected[key] for key in sorted(projected)]
        sources.append(entry)
    reduced["sources"] = sources
    return reduced


@dataclass(frozen=True)
class AttrsComparison:
    """How a candidate's attributes compare with one stored row's."""

    #: Equal under the rule.
    unchanged: bool
    #: Equal only because a lineage revision was ignored.
    revision_only: bool
    #: The top-level keys that differ; the lineage appears as
    #: ``nomos_source_lineage.source_revision`` when only its revision
    #: differs alongside another real change. Keys only, never values.
    differing: tuple[str, ...]


def compare_attrs(
    new_attrs: Mapping[str, Any],
    stored_attrs: Mapping[str, Any],
    *,
    ignore: Iterable[str] = (),
) -> AttrsComparison:
    """Compare a candidate's attributes with a stored row's under the rule.

    ``ignore`` names the volatile fields of the fact's own node subtype (see
    :func:`volatile_fields_for`); pass nothing for an edge.
    """
    skip = frozenset(ignore)
    differing = sorted(
        key for key in (set(new_attrs) | set(stored_attrs)) - skip
        if canonical(new_attrs.get(key, MISSING))
        != canonical(stored_attrs.get(key, MISSING))
    )
    if not differing:
        return AttrsComparison(True, False, ())
    if LINEAGE_ATTR in differing:
        revision_only_lineage = (
            canonical(lineage_for_content(new_attrs.get(LINEAGE_ATTR, MISSING)))
            == canonical(lineage_for_content(stored_attrs.get(LINEAGE_ATTR, MISSING)))
        )
        if revision_only_lineage and differing == [LINEAGE_ATTR]:
            return AttrsComparison(True, True, ())
        if revision_only_lineage:
            differing = [
                LINEAGE_ATTR + ".source_revision" if key == LINEAGE_ATTR else key
                for key in differing
            ]
    return AttrsComparison(False, False, tuple(differing))


def as_real(value: Any) -> float | None:
    """``value`` at the precision a Postgres ``REAL`` column stores.

    ``graph_edges.confidence`` is ``REAL``: 0.7 reads back as 0.69999999, so
    comparing at double precision would rewrite every such edge on every run.
    None stays None, so a missing confidence never equals 0.0.
    """
    if value is None:
        return None
    return struct.unpack("f", struct.pack("f", float(value)))[0]


def edge_columns_differing(
    *,
    new_provenance: Any,
    new_confidence: Any,
    stored_provenance: Any,
    stored_confidence: Any,
) -> tuple[str, ...]:
    """The edge columns (not attrs) whose value changed."""
    differing = []
    if new_provenance != stored_provenance:
        differing.append("edge.provenance")
    if as_real(new_confidence) != as_real(stored_confidence):
        differing.append("edge.confidence")
    return tuple(differing)


#: Volatile fields per node subtype: ``{subtype: frozenset(field, ...)}``.
VolatileFields = Mapping[str, frozenset[str]]


def volatile_fields_for(volatile: VolatileFields | None, subtype: Any) -> frozenset[str]:
    """The volatile fields ``subtype`` declares; empty when it declares none."""
    if not volatile or subtype is None:
        return frozenset()
    return volatile.get(str(subtype), frozenset())


__all__ = [
    "AttrsComparison",
    "LINEAGE_ATTR",
    "LINEAGE_REVISION_KEYS",
    "MISSING",
    "VolatileFields",
    "as_real",
    "canonical",
    "compare_attrs",
    "edge_columns_differing",
    "lineage_for_content",
    "volatile_fields_for",
]
