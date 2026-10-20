"""Baseline-pinned, provenance-preserving entity merge remediation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

import psycopg

from okg.substrate.errors import SubstrateError


#: Node-attribute merge dispositions, in the order a reader should think about
#: them: union both sides, take one side, or refuse.
#:
#: ``unresolved`` is the fail-closed default and is NOT selectable — it is what
#: an attribute gets when nobody said what to do with it, and it makes ``apply``
#: refuse. Only the three below can be declared in the catalog or passed on the
#: command line.
MERGE_DISPOSITIONS: tuple[str, ...] = ("union", "keep_survivor", "keep_retiree")

#: Edge-attribute keys that carry when-this-was-written rather than what it
#: means, and so are projected out of edge identity.
#:
#: A named set rather than an inline literal: a deployment whose extractor
#: stamps its own per-run key can add it here, instead of having every
#: cross-run pair block as an identity conflict with no way to say otherwise.
#: `nomos_source_lineage` is not simply dropped — the parts of it a read policy
#: acts on stay in identity; see `_lineage_identity`.
PROVENANCE_ATTR_KEYS: frozenset[str] = frozenset({"nomos_source_lineage"})

#: Versioned receipt shape for an assertion that is retired because a declared
#: deployment policy proved it equivalent to the already-live assertion.
EDGE_ASSERTION_VARIANCE_SCHEMA_VERSION = "okg.entity_merge.edge_assertion_variance.v1"
CANDIDATE_RECORD_ALIAS_SCHEMA_VERSION = "okg.entity_merge.candidate_record_alias.v1"
PROVENANCE_UNION_SCHEMA_VERSION = "okg.entity_merge.provenance_union.v1"

_EDGE_ASSERTION_FIELDS: tuple[str, ...] = (
    "attrs",
    "provenance",
    "confidence",
    "source",
    "source_record_id",
    "source_revision",
    "run_id",
    "observed_at",
)

#: Disposition assigned to a conflicting attribute nobody dispositioned.
DISPOSITION_UNRESOLVED = "unresolved"


@dataclass(frozen=True)
class EntityMergePlan:
    deployment_name: str
    branch_name: str
    survivor_node_id: str
    retired_node_id: str
    subtype: str
    pinned_checkpoint: str
    catalog_version_id: int
    branch_mutation_token: str
    footprint_digest: str
    edge_rehomes: tuple[dict[str, Any], ...]
    deduplicated_edges: tuple[dict[str, Any], ...]
    attribute_conflicts: tuple[dict[str, Any], ...]
    blocking_conflicts: tuple[dict[str, Any], ...]
    accept_survivor_attributes: bool
    survivor_snapshot: dict[str, Any]
    retired_snapshot: dict[str, Any]
    plan_digest: str
    #: Per-attribute dispositions the caller passed explicitly. Carried on the
    #: plan (and therefore into `plan_digest`) so the replans inside `apply`
    #: reproduce the dispositions the operator actually approved rather than
    #: silently falling back to the defaults.
    attribute_dispositions: dict[str, str]
    #: Full attribute map to write back to the survivor, or None when no
    #: disposition changes the survivor's value. None means the survivor node
    #: is not written at all, which is the pre-existing behaviour.
    survivor_attrs_merged: dict[str, Any] | None
    #: Refusals raised by the survivor write itself (as opposed to by an edge).
    #: Populated at plan time so a dry run shows them.
    survivor_write_refusals: tuple[dict[str, Any], ...]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class EntityMergeResult:
    pass_name: str
    applied: bool
    affected_count: int
    details: dict[str, Any]


def _fingerprint(value: Any) -> str:
    """Canonical JSON for a value, used as its identity when deduplicating.

    `json.dumps(sort_keys=True)` sorts dict KEYS only, so list order stays
    load-bearing. Anything that dedups or orders by this string is therefore
    reproducible across processes — which `plan_digest` depends on, since a
    dry-run digest has to match the digest recomputed inside `--apply`.
    """
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _digest(value: Any) -> str:
    return hashlib.sha256(_fingerprint(value).encode("utf-8")).hexdigest()


def _union_values(survivor: list[Any], retired: list[Any]) -> list[Any]:
    """Every entry from either side, deduplicated and ordered reproducibly.

    Ordering is by canonical fingerprint rather than by input order so the
    result — and therefore `plan_digest` — does not depend on which side was
    read first. This mirrors `normalize_source_lineage`, which sorts its own
    record lists the same way for the same reason.
    """
    by_fingerprint: dict[str, Any] = {}
    for value in [*survivor, *retired]:
        by_fingerprint.setdefault(_fingerprint(value), value)
    return [by_fingerprint[key] for key in sorted(by_fingerprint)]


def _scoped_catalog_version_id(
    conn: psycopg.Connection, deployment_name: str
) -> int | None:
    """Return the resolver-visible catalog pin for one deployment."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT catalog_version_id FROM okg.catalog_versions "
            "WHERE status = 'applied' "
            "AND (deployment_name = %s OR deployment_name IS NULL) "
            "ORDER BY (deployment_name = %s) DESC NULLS LAST, "
            "catalog_version_id DESC "
            "LIMIT 1",
            (deployment_name, deployment_name),
        )
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _declared_merge_semantics(
    conn: psycopg.Connection, subtype: str, catalog_version_id: int
) -> dict[str, str]:
    """Per-attribute merge dispositions declared in the catalog.

    Reads the same place `pii_classes` is read from — `okg.subtypes.metadata`,
    keyed by `(name, catalog_version_id)`. That table is relational, not part
    of the Chronos graph checkout, which is why this takes a connection rather
    than the pinned graph session. The catalog version is already drift-checked
    before `apply` replans (`_scoped_catalog_version_id`), so a declaration
    cannot change underneath an approved plan.

    An unknown disposition raises. Ignoring it would let a typo like
    `merge_semantics: unionn` read as "no declaration" and silently take the
    survivor's value — the exact citation loss this is here to prevent.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT metadata->'merge_semantics' FROM okg.subtypes "
            "WHERE name = %s AND catalog_version_id = %s",
            (subtype, catalog_version_id),
        )
        row = cur.fetchone()
    declared = row[0] if row and row[0] else {}
    if not isinstance(declared, dict):
        raise SubstrateError(
            f"subtype {subtype!r} declares a non-mapping merge_semantics",
            data={"declared": declared},
        )
    invalid = {
        key: value
        for key, value in declared.items()
        if str(value) not in MERGE_DISPOSITIONS
    }
    if invalid:
        raise SubstrateError(
            f"subtype {subtype!r} declares unknown merge_semantics",
            data={"invalid": invalid, "supported": list(MERGE_DISPOSITIONS)},
        )
    return {str(key): str(value) for key, value in declared.items()}


def _declared_edge_equivalence(
    conn: psycopg.Connection,
    *,
    src_subtype: str,
    edge_type: str,
    dst_subtype: str,
    catalog_version_id: int,
) -> dict[str, Any] | None:
    """Return the exact edge-shape equivalence rule at the catalog pin.

    Entity merge deliberately does not walk subtype ancestors here. Catalog
    composition expands a declaration into its concrete narrowing rows, so an
    absent exact row means the deployment did not authorize this assertion
    pair to collapse.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT attr_rules->'entity_merge_equivalence' "
            "FROM okg.narrowings WHERE parent_subtype = %s "
            "AND edge_type = %s AND child_subtype = %s "
            "AND catalog_version_id = %s",
            (src_subtype, edge_type, dst_subtype, catalog_version_id),
        )
        row = cur.fetchone()
    declared = row[0] if row else None
    if declared is None:
        return None
    if not isinstance(declared, dict):
        raise SubstrateError(
            "entity merge edge equivalence policy is not a mapping",
            data={
                "src_subtype": src_subtype,
                "edge_type": edge_type,
                "dst_subtype": dst_subtype,
                "catalog_version_id": catalog_version_id,
                "declared": declared,
            },
        )
    return dict(declared)


def _resolve_disposition(
    attribute: str,
    *,
    overrides: dict[str, str],
    declared: dict[str, str],
    accept_survivor_attributes: bool,
    both_are_lists: bool,
) -> str:
    """Which rule applies to one conflicting attribute.

    Precedence, highest first:

    1. an explicit per-attribute override from the caller;
    2. the catalog declaration;
    3. `accept_survivor_attributes`, which fills in `keep_survivor` ONLY where
       nothing above spoke, and never for a list;
    4. `unresolved`, which refuses the merge.

    Step 3 must not outrank steps 1 and 2. `--accept-survivor-attrs` is passed
    by essentially every existing caller, so letting it win would quietly
    reinstate survivor-takes-all for a citation list somebody deliberately
    declared as `union`.

    Step 3 also stops short of list-valued attributes entirely. A blanket
    "take the survivor's value" is a reasonable thing to say about a scalar,
    where one value has to win either way. Said about two lists it silently
    discards entries — which is the loss issue #1043 reports — and it would be
    said by default on any deployment whose declarations are missing, whose
    catalog version predates them, or whose operator simply forgot the flag.
    So a differing list needs someone to name a disposition for it. Note this
    is decided on the VALUES, not on a declaration, precisely so that a
    deployment with no declarations at all is still protected.
    """
    if attribute in overrides:
        return overrides[attribute]
    if attribute in declared:
        return declared[attribute]
    if accept_survivor_attributes and not both_are_lists:
        return "keep_survivor"
    return DISPOSITION_UNRESOLVED


def _merge_attributes(
    survivor_attrs: dict[str, Any],
    retired_attrs: dict[str, Any],
    *,
    overrides: dict[str, str],
    declared: dict[str, str],
    accept_survivor_attributes: bool,
) -> tuple[tuple[dict[str, Any], ...], dict[str, Any] | None]:
    """Disposition every conflicting attribute and build the survivor's value.

    Returns the conflict records and, when some disposition actually changes
    the survivor's attributes, the full attribute map to write back. Returning
    None for the second element means no survivor write happens at all — the
    pre-existing behaviour, which every `keep_survivor`-only plan keeps.
    """
    from okg.substrate.nomos.source_lineage import LINEAGE_ATTR

    # The Nomos lineage envelope is substrate-managed, not an operator-facing
    # attribute: the write path regenerates it from the batch source on every
    # node write. Dispositioning it would ask an operator to authorise a value
    # nobody gets to choose, and would make every union merge refuse until they
    # did. What the survivor's envelope may LOSE by being rewritten is a real
    # risk, and is checked separately by `_survivor_lineage_narrowing`.
    survivor_lineage = survivor_attrs.get(LINEAGE_ATTR)
    survivor_attrs = {
        key: value for key, value in survivor_attrs.items() if key != LINEAGE_ATTR
    }
    retired_attrs = {
        key: value for key, value in retired_attrs.items() if key != LINEAGE_ATTR
    }
    # The searchable-text copy (`okg_producer_text`, issue #2991) is system
    # state too: it is the producer's body, and the stored `text` is a rollup
    # stamped from it. Both sides are compared as their producer wrote them
    # (`text` = that body, no copy), so an operator chooses between bodies,
    # never between a copy and a rollup, and the survivor write stamps `text`
    # and the copy again from the chosen body.
    from okg.substrate.searchable_text import producer_view

    survivor_attrs = producer_view(survivor_attrs)
    retired_attrs = producer_view(retired_attrs)

    conflicts: list[dict[str, Any]] = []
    merged = dict(survivor_attrs)
    for key in sorted(set(survivor_attrs) | set(retired_attrs)):
        if survivor_attrs.get(key) == retired_attrs.get(key):
            continue
        disposition = _resolve_disposition(
            key,
            overrides=overrides,
            declared=declared,
            accept_survivor_attributes=accept_survivor_attributes,
            both_are_lists=isinstance(survivor_attrs.get(key), list)
            and isinstance(retired_attrs.get(key), list),
        )
        record: dict[str, Any] = {
            "attribute": key,
            "survivor": survivor_attrs.get(key),
            "retired": retired_attrs.get(key),
            "disposition": disposition,
        }
        if disposition == "union":
            survivor_value = survivor_attrs.get(key)
            retired_value = retired_attrs.get(key)
            if not isinstance(survivor_value, list) or not isinstance(
                retired_value, list
            ):
                # Refuse rather than pick some other rule. "Union what I can and
                # quietly do something else with the rest" is how a disposition
                # the operator authorised turns into one they did not.
                raise SubstrateError(
                    f"entity merge attribute {key!r} is dispositioned union but "
                    "is not a list on both sides",
                    data={
                        "attribute": key,
                        "survivor_type": type(survivor_value).__name__,
                        "retired_type": type(retired_value).__name__,
                    },
                    fix_hint="union applies to list-valued attributes; use "
                    "keep_survivor or keep_retiree for scalars",
                )
            merged[key] = _union_values(survivor_value, retired_value)
            record["merged"] = merged[key]
        elif disposition == "keep_retiree":
            if key in retired_attrs:
                merged[key] = retired_attrs[key]
            else:
                merged.pop(key, None)
        elif disposition not in (DISPOSITION_UNRESOLVED, "keep_survivor"):
            raise SubstrateError(
                f"entity merge attribute {key!r} has unknown disposition "
                f"{disposition!r}",
                data={"supported": list(MERGE_DISPOSITIONS)},
            )
        conflicts.append(record)
    if merged == survivor_attrs:
        return tuple(conflicts), None
    # Hand the survivor's own envelope back before the write. A source-scoped
    # write discards it and rebuilds one, so this changes nothing there. An
    # unscoped write (batch source None) PRESERVES what it is handed — and
    # would otherwise store a node with no lineage at all, because the strip
    # above removed it. That is a provenance loss the narrowing check cannot
    # see, since it reasons about a rebuilt envelope, not a deleted one.
    if survivor_lineage is not None:
        merged[LINEAGE_ATTR] = survivor_lineage
    return tuple(conflicts), merged


def _survivor_lineage_narrowing(
    survivor: dict[str, Any],
) -> dict[str, Any] | None:
    """Would rewriting the survivor node shrink its provenance?

    The write path regenerates a node's lineage envelope from the batch source
    plus the fact's own `source_record_id` / `source_revision`
    (`graph_store_internal/writes.py`), discarding whatever envelope the fact
    carried. For a survivor whose envelope accumulated several source records,
    rewriting its attributes would therefore also shrink its provenance — the
    merge would silently narrow what a read policy can see, as a side effect of
    merging citations.

    Returns a description of what would be lost, or None when nothing is.
    """
    from okg.substrate.nomos.source_lineage import (
        LINEAGE_ATTR,
        source_ids_from_lineage,
        source_record_refs_from_lineage,
        stamp_attrs_with_source_lineage,
    )

    source = survivor.get("source")
    if source is None:
        # With no batch source the write path preserves the envelope it is
        # handed, so there is nothing to lose.
        return None
    current = (survivor.get("attrs") or {}).get(LINEAGE_ATTR)
    regenerated = stamp_attrs_with_source_lineage(
        {},
        source_id=str(source),
        source_record_id=survivor.get("source_record_id"),
        source_revision=survivor.get("source_revision"),
    ).get(LINEAGE_ATTR)
    lost_sources = sorted(
        set(source_ids_from_lineage(current))
        - set(source_ids_from_lineage(regenerated))
    )
    lost_records = sorted(
        set(source_record_refs_from_lineage(current))
        - set(source_record_refs_from_lineage(regenerated))
    )
    if not lost_sources and not lost_records:
        return None
    return {
        "kind": "survivor_lineage_narrowing",
        "node_id": str(survivor["node_id"]),
        "lost_source_ids": lost_sources,
        "lost_source_record_refs": lost_records,
    }


def _nodes(graph: Any, node_ids: list[str]) -> dict[str, dict[str, Any]]:
    rows = graph.query(
        "SELECT node_id, subtype, attrs, source, source_record_id, "
        "source_revision, run_id, observed_at FROM okg.graph_nodes "
        "WHERE node_id = ANY(:node_ids) ORDER BY node_id",
        {"node_ids": node_ids},
    )
    return {str(row["node_id"]): dict(row) for row in rows}


def _incident_edges(graph: Any, node_ids: list[str]) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in graph.query(
            "SELECT edge_id, src, dst, edge_type, attrs, provenance, confidence, "
            "source, source_record_id, source_revision, run_id, observed_at "
            "FROM okg.graph_edges WHERE src = ANY(:node_ids) "
            "OR dst = ANY(:node_ids) ORDER BY edge_id",
            {"node_ids": node_ids},
        )
    ]


def _node_subtypes(graph: Any, node_ids: set[str]) -> dict[str, str]:
    if not node_ids:
        return {}
    rows = graph.query(
        "SELECT node_id, subtype FROM okg.graph_nodes "
        "WHERE node_id = ANY(:node_ids) ORDER BY node_id",
        {"node_ids": sorted(node_ids)},
    )
    return {str(row["node_id"]): str(row["subtype"]) for row in rows}


def _candidate_record_identity(
    record: Any,
    *,
    survivor_node_id: str,
    retired_node_id: str,
) -> Any:
    """Normalize only a shaped extraction record for this merge pair.

    A generic or nested ``node_id`` is not enough evidence that a record names
    the pre-resolution candidate.  The extraction boundary always supplies
    ``chunk_node_id`` and ``enricher`` alongside that candidate id; requiring
    all three fields keeps the projection shallow, pair-scoped, and injective
    over every other record component.
    """
    if not isinstance(record, Mapping):
        return record
    if not {"chunk_node_id", "enricher", "node_id"}.issubset(record):
        return record
    if not all(
        isinstance(record.get(key), str) and bool(str(record[key]).strip())
        for key in ("chunk_node_id", "enricher", "node_id")
    ):
        return record
    candidate_node_id = str(record["node_id"])
    if candidate_node_id not in {survivor_node_id, retired_node_id}:
        return record
    projected = dict(record)
    projected["node_id"] = survivor_node_id
    return projected


def _literal_json(value: Any) -> Any:
    """Keep any safe JSON value literal; only SQL NULL uses the empty default."""
    return {} if value is None else value


def _lineage_identity(
    lineage_attr: Any,
    *,
    survivor_node_id: str | None = None,
    retired_node_id: str | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Project a Nomos source-lineage envelope down to what it MEANS.

    Returns ``(signature, None)`` when the envelope can be compared, or
    ``(None, reason)`` when it cannot and the pair must block.

    The signature follows the convention already established by
    ``enrichers.derived_edges._dedupe_attrs_hash`` — source ids plus each
    source's policy fingerprints — extended with each source's source record
    ids, because record-level read policy acts on those and two edges claiming
    different records are not interchangeable.

    What is deliberately NOT in the signature: ``source_records`` and
    ``source_revisions``. Those carry the writing run's ``run_id``, which is
    when-this-was-written, not what-it-means. Including them is the whole
    defect this function exists to fix.

    Two envelopes that cannot be compared must never compare EQUAL. A malformed
    envelope makes ``source_ids_from_lineage`` return an empty tuple, so two
    unknown lineages would otherwise agree on emptiness and deduplicate — a
    missing signal read as agreement. A truncated envelope is worse: the
    retained record subset is capped and deterministic, so two envelopes can
    share every retained record and differ only in the discarded remainder.
    ``nomos.read_filter`` already fails closed on that marker; the merge must
    not be the one component that fails open on it.
    """
    from okg.substrate.nomos.source_lineage import (
        RECORD_DETAIL_KEY,
        RECORD_DETAIL_TRUNCATED,
        normalize_source_lineage,
        source_ids_from_lineage,
    )

    if lineage_attr is None:
        # No envelope at all on either side is a deployment that does not stamp
        # lineage, not an envelope that failed to parse. There is no read policy
        # to narrow, so this compares equal to another absent envelope and
        # differs from any present one.
        return {"lineage": "absent"}, None
    lineage = normalize_source_lineage(lineage_attr)
    if lineage.get("status") != "ok":
        return None, "lineage_malformed"
    if lineage.get(RECORD_DETAIL_KEY) == RECORD_DETAIL_TRUNCATED:
        return None, "lineage_truncated"
    source_ids = source_ids_from_lineage(lineage)
    if not source_ids:
        return None, "lineage_malformed"
    if (survivor_node_id is None) != (retired_node_id is None):
        raise ValueError("candidate record identity requires both merge node ids")

    def record_identity(value: Any) -> Any:
        if survivor_node_id is None or retired_node_id is None:
            return value
        return _candidate_record_identity(
            value,
            survivor_node_id=survivor_node_id,
            retired_node_id=retired_node_id,
        )

    known = set(source_ids)
    by_source: dict[str, dict[str, Any]] = {}
    for source in lineage.get("sources") or []:
        if not isinstance(source, dict):
            continue
        source_id = str(source.get("source_id"))
        if source_id not in known:
            continue
        by_source[source_id] = {
            "policy_fingerprints": sorted(
                _fingerprint(value)
                for value in (source.get("policy_fingerprints") or [])
            ),
            "source_record_ids": sorted(
                _fingerprint(record_identity(value))
                for value in (source.get("source_record_ids") or [])
            ),
        }
    return {"source_ids": sorted(source_ids), "sources": by_source}, None


def _edge_identity(
    row: dict[str, Any],
    *,
    survivor_node_id: str | None = None,
    retired_node_id: str | None = None,
) -> tuple[dict[str, Any], str | None]:
    """What the edge asserts, with per-run bookkeeping projected out.

    Returns ``(identity, reason)``. A non-None ``reason`` means the edge's
    lineage could not be compared, so the caller must block rather than treat
    the identity dicts as authoritative.
    """
    from okg.substrate.nomos.source_lineage import LINEAGE_ATTR

    raw_attrs = dict(row.get("attrs") or {})
    lineage_signature, reason = _lineage_identity(
        raw_attrs.get(LINEAGE_ATTR),
        survivor_node_id=survivor_node_id,
        retired_node_id=retired_node_id,
    )
    attrs = {
        key: value
        for key, value in raw_attrs.items()
        if key not in PROVENANCE_ATTR_KEYS
    }
    return (
        {
            "src": str(row["src"]),
            "dst": str(row["dst"]),
            "edge_type": str(row["edge_type"]),
            "attrs": attrs,
            "provenance": str(row.get("provenance") or ""),
            "confidence": float(row.get("confidence") or 0.0),
            "source": row.get("source"),
            "source_record_id": (
                _candidate_record_identity(
                    _literal_json(row.get("source_record_id")),
                    survivor_node_id=survivor_node_id,
                    retired_node_id=retired_node_id,
                )
                if survivor_node_id is not None and retired_node_id is not None
                else _literal_json(row.get("source_record_id"))
            ),
            "lineage": lineage_signature,
        },
        reason,
    )


def _edge_provenance(row: dict[str, Any]) -> dict[str, Any]:
    """When the edge was written, and by which run — never part of identity."""
    from okg.substrate.nomos.source_lineage import LINEAGE_ATTR

    raw = (row.get("attrs") or {}).get(LINEAGE_ATTR)
    lineage = dict(raw) if isinstance(raw, Mapping) else {}
    records: dict[str, Any] = {}
    for source in lineage.get("sources") or []:
        if not isinstance(source, dict):
            continue
        records[str(source.get("source_id"))] = {
            "source_records": source.get("source_records") or [],
            "source_revisions": source.get("source_revisions") or [],
        }
    return {
        "source_revision": _literal_json(row.get("source_revision")),
        "lineage_records": records,
    }


def _provenance_variance(
    surviving: dict[str, Any], retiring: dict[str, Any]
) -> dict[str, Any]:
    """Name the provenance the surviving edge does NOT pick up.

    The surviving edge is left exactly as it is: a source-scoped write
    regenerates its lineage envelope from the fact's own source / record /
    revision (``nomos.source_lineage.stamp_attrs_with_source_lineage``
    discards an incoming envelope whenever the batch source is set), so there
    is no write that could carry the retiree's envelope over. Recording the
    difference in the plan — which is stored verbatim in
    ``okg.entity_merge_receipts`` — is what is actually observable, and is
    better than dropping it unremarked.
    """
    kept = _edge_provenance(surviving)
    dropped = _edge_provenance(retiring)
    return {
        field: {"kept": kept[field], "not_carried_over": dropped[field]}
        for field in sorted(kept)
        if _fingerprint(kept[field]) != _fingerprint(dropped[field])
    }


def _edge_assertion_variance(
    existing: dict[str, Any], retired: dict[str, Any]
) -> dict[str, Any]:
    """Versioned, lossless record of the assertion fields that differ.

    Values are compared canonically because database JSON values can have
    equivalent mappings with different insertion order. The full raw attrs
    mapping is retained whenever attrs differ, so absent keys remain distinct
    from keys whose value is explicit null and the unprojected lineage envelope
    remains auditable.
    """
    differences: dict[str, dict[str, Any]] = {}
    for field in _EDGE_ASSERTION_FIELDS:
        existing_value = existing.get(field)
        retired_value = retired.get(field)
        if _fingerprint(existing_value) == _fingerprint(retired_value):
            continue
        differences[field] = {
            "existing": existing_value,
            "retired": retired_value,
        }
    return {
        "schema_version": EDGE_ASSERTION_VARIANCE_SCHEMA_VERSION,
        "differences": differences,
    }


def _edge_provenance_union(
    existing: dict[str, Any], retired: dict[str, Any]
) -> dict[str, Any]:
    """Losslessly union the literal provenance of both edge assertions.

    The live survivor assertion is deliberately not rewritten: source-scoped
    ingestion regenerates governed lineage and must not forge a composite read
    authority.  The merge receipt is the durable audit surface, so it carries
    both literal top-level records/revisions and both unprojected envelopes.
    """
    from okg.substrate.nomos.source_lineage import LINEAGE_ATTR

    lineages = [
        dict(row.get("attrs") or {}).get(LINEAGE_ATTR) for row in (existing, retired)
    ]
    return {
        "schema_version": PROVENANCE_UNION_SCHEMA_VERSION,
        "source_record_ids": _union_values(
            [_literal_json(existing.get("source_record_id"))],
            [_literal_json(retired.get("source_record_id"))],
        ),
        "source_revisions": _union_values(
            [_literal_json(existing.get("source_revision"))],
            [_literal_json(retired.get("source_revision"))],
        ),
        "lineage_envelopes": _union_values(lineages[:1], lineages[1:]),
        "sources": _union_values([existing.get("source")], [retired.get("source")]),
        "provenance_classes": _union_values(
            [existing.get("provenance")], [retired.get("provenance")]
        ),
    }


def _edge_tuple(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (str(row["src"]), str(row["edge_type"]), str(row["dst"]))


def _edge_uses_candidate_record_alias(
    row: Mapping[str, Any],
    *,
    survivor_node_id: str,
    retired_node_id: str,
) -> bool:
    """Return whether pair-scoped projection changes literal edge provenance."""
    source_record_id = _literal_json(row.get("source_record_id"))
    if _fingerprint(source_record_id) != _fingerprint(
        _candidate_record_identity(
            source_record_id,
            survivor_node_id=survivor_node_id,
            retired_node_id=retired_node_id,
        )
    ):
        return True
    lineage = dict(row.get("attrs") or {}).get("nomos_source_lineage")
    raw_lineage, raw_reason = _lineage_identity(lineage)
    projected_lineage, projected_reason = _lineage_identity(
        lineage,
        survivor_node_id=survivor_node_id,
        retired_node_id=retired_node_id,
    )
    return (
        raw_reason is None
        and projected_reason is None
        and raw_lineage != projected_lineage
    )


def _semantic_edge_attrs(
    row: Mapping[str, Any], *, audit_attrs: set[str]
) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in dict(row.get("attrs") or {}).items()
        if key not in PROVENANCE_ATTR_KEYS and key not in audit_attrs
    }


def _equivalence_policy_decision(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    existing: dict[str, Any],
    planned: dict[str, Any],
    endpoint_subtypes: Mapping[str, str],
    catalog_version_id: int,
    survivor_node_id: str,
    retired_node_id: str,
    candidate_record_alias: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Prove a declared cross-producer assertion equivalence.

    Returns digestable evidence on success, or one stable refusal reason. Every
    check is fail-closed: a hash-key collision, missing endpoint subtype,
    missing or nonmatching catalog rule, unknown policy vocabulary, malformed
    lineage, or any semantic/read-scope variance refuses the merge.
    """
    from okg.substrate.catalog.modules_compose import (
        ENTITY_MERGE_EQUIVALENCE_SCHEMA_VERSION,
        ENTITY_MERGE_RESERVED_AUDIT_ATTRS,
    )
    from okg.substrate.mcp.provenance import classify_source_evidence

    if _edge_tuple(existing) != _edge_tuple(planned):
        return None, "edge_id_hash_collision"
    src_subtype = endpoint_subtypes.get(str(planned["src"]))
    dst_subtype = endpoint_subtypes.get(str(planned["dst"]))
    if src_subtype is None or dst_subtype is None:
        return None, "endpoint_subtype_missing"
    rule = _declared_edge_equivalence(
        conn,
        src_subtype=src_subtype,
        edge_type=str(planned["edge_type"]),
        dst_subtype=dst_subtype,
        catalog_version_id=catalog_version_id,
    )
    if rule is None:
        return None, "equivalence_policy_missing"
    if set(rule) != {
        "schema_version",
        "survivor",
        "retiree",
        "audit_attrs",
        "confidence_rule",
    }:
        return None, "equivalence_policy_invalid"
    audit_attrs = rule.get("audit_attrs")
    survivor_rule = rule.get("survivor")
    retiree_rule = rule.get("retiree")
    if (
        rule.get("schema_version") != ENTITY_MERGE_EQUIVALENCE_SCHEMA_VERSION
        or rule.get("confidence_rule") != "survivor_gte"
        or not isinstance(audit_attrs, list)
        or not audit_attrs
        or not all(isinstance(key, str) and key for key in audit_attrs)
        or len(set(audit_attrs)) != len(audit_attrs)
        or bool(set(audit_attrs) & ENTITY_MERGE_RESERVED_AUDIT_ATTRS)
        or not isinstance(survivor_rule, dict)
        or not isinstance(retiree_rule, dict)
        or set(survivor_rule) != {"producer", "provenance"}
        or set(retiree_rule) != {"producer", "provenance"}
        or not all(
            isinstance(party.get(key), str) and party[key].strip()
            for party in (survivor_rule, retiree_rule)
            for key in ("producer", "provenance")
        )
        or survivor_rule == retiree_rule
    ):
        return None, "equivalence_policy_invalid"
    existing_party = {
        "producer": existing.get("source"),
        "provenance": str(existing.get("provenance") or ""),
    }
    planned_party = {
        "producer": planned.get("source"),
        "provenance": str(planned.get("provenance") or ""),
    }
    if existing_party != survivor_rule or planned_party != retiree_rule:
        return None, "equivalence_policy_party_mismatch"

    existing_lineage_raw = dict(existing.get("attrs") or {}).get("nomos_source_lineage")
    planned_lineage_raw = dict(planned.get("attrs") or {}).get("nomos_source_lineage")
    if existing_lineage_raw is None or planned_lineage_raw is None:
        return (
            None,
            "lineage_absent"
            if existing_lineage_raw is None and planned_lineage_raw is None
            else "lineage_differs",
        )
    existing_lineage, existing_reason = _lineage_identity(
        existing_lineage_raw,
        survivor_node_id=survivor_node_id,
        retired_node_id=retired_node_id,
    )
    planned_lineage, planned_reason = _lineage_identity(
        planned_lineage_raw,
        survivor_node_id=survivor_node_id,
        retired_node_id=retired_node_id,
    )
    if existing_reason is not None or planned_reason is not None:
        return None, existing_reason or planned_reason
    if existing_lineage != planned_lineage:
        return None, "lineage_differs"
    existing_record = _candidate_record_identity(
        _literal_json(existing.get("source_record_id")),
        survivor_node_id=survivor_node_id,
        retired_node_id=retired_node_id,
    )
    planned_record = _candidate_record_identity(
        _literal_json(planned.get("source_record_id")),
        survivor_node_id=survivor_node_id,
        retired_node_id=retired_node_id,
    )
    if existing_record != planned_record:
        return None, "source_record_differs"
    audit_attr_set = set(audit_attrs)
    if _semantic_edge_attrs(
        existing, audit_attrs=audit_attr_set
    ) != _semantic_edge_attrs(planned, audit_attrs=audit_attr_set):
        return None, "semantic_attrs_differ"

    trust_by_source = classify_source_evidence(
        conn,
        deployment_name=deployment_name,
        sources=(existing.get("source"), planned.get("source")),
    )
    existing_trust = trust_by_source[existing.get("source")]
    planned_trust = trust_by_source[planned.get("source")]
    if (
        existing_trust["effective_classification"]
        != planned_trust["effective_classification"]
    ):
        return None, "read_trust_differs"
    if float(existing.get("confidence") or 0.0) < float(
        planned.get("confidence") or 0.0
    ):
        return None, "survivor_confidence_lower"
    evidence = {
        "mode": "declared_catalog_equivalence",
        "edge_shape": {
            "src_subtype": src_subtype,
            "edge_type": str(planned["edge_type"]),
            "dst_subtype": dst_subtype,
            "catalog_version_id": catalog_version_id,
        },
        "catalog_rule": rule,
        "normalized_lineage": existing_lineage,
        "read_trust": {
            "existing": existing_trust,
            "retired": planned_trust,
        },
    }
    if candidate_record_alias is not None:
        evidence["candidate_record_alias"] = candidate_record_alias
    return evidence, None


def _existing_identity_evidence(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    existing: dict[str, Any],
    planned: dict[str, Any],
    candidate_record_alias: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Digest-bind trust evidence for the pre-existing identity-safe path."""
    from okg.substrate.mcp.provenance import classify_source_evidence

    trust_by_source = classify_source_evidence(
        conn,
        deployment_name=deployment_name,
        sources=(existing.get("source"), planned.get("source")),
    )
    evidence = {
        "mode": "existing_identity",
        "catalog_rule": None,
        "read_trust": {
            "existing": trust_by_source[existing.get("source")],
            "retired": trust_by_source[planned.get("source")],
        },
    }
    if candidate_record_alias is not None:
        evidence["candidate_record_alias"] = candidate_record_alias
    return evidence


def _plan_from_graph(
    conn: psycopg.Connection,
    graph: Any,
    *,
    deployment_name: str,
    branch_name: str,
    survivor_node_id: str,
    retired_node_id: str,
    pinned_checkpoint: str,
    catalog_version_id: int,
    branch_mutation_token: str,
    accept_survivor_attributes: bool,
    attribute_dispositions: dict[str, str] | None = None,
) -> EntityMergePlan:
    from okg.substrate.graph_store import _edge_id

    if survivor_node_id == retired_node_id:
        raise ValueError("entity merge requires distinct survivor and retired ids")
    nodes = _nodes(graph, [survivor_node_id, retired_node_id])
    missing = sorted({survivor_node_id, retired_node_id} - set(nodes))
    if missing:
        raise ValueError(f"entity merge nodes are not live: {', '.join(missing)}")
    survivor = nodes[survivor_node_id]
    retired = nodes[retired_node_id]
    if str(survivor["subtype"]) != str(retired["subtype"]):
        raise ValueError("entity merge survivor and retired node subtypes differ")
    subtype = str(survivor["subtype"])
    edges = _incident_edges(graph, [survivor_node_id, retired_node_id])
    endpoint_subtypes = _node_subtypes(
        graph,
        {str(endpoint) for edge in edges for endpoint in (edge["src"], edge["dst"])},
    )
    footprint_digest = _digest(
        {
            "nodes": [nodes[key] for key in sorted(nodes)],
            "incident_edges": edges,
            "endpoint_subtypes": endpoint_subtypes,
        }
    )

    from okg.substrate.nomos.runtime_enforcement import (
        deployment_nomos_enforcement_mode,
    )

    nomos_enforcement_mode = deployment_nomos_enforcement_mode(deployment_name)
    candidate_record_alias = {
        "schema_version": CANDIDATE_RECORD_ALIAS_SCHEMA_VERSION,
        "required_fields": ["chunk_node_id", "enricher", "node_id"],
        "candidate_field": "node_id",
        "survivor_node_id": survivor_node_id,
        "retired_node_id": retired_node_id,
        "normalized_node_id": survivor_node_id,
        "nomos_enforcement_mode": nomos_enforcement_mode,
    }

    overrides = dict(attribute_dispositions or {})
    unknown_overrides = {
        key: value
        for key, value in overrides.items()
        if value not in MERGE_DISPOSITIONS
    }
    if unknown_overrides:
        raise SubstrateError(
            "entity merge received unknown attribute dispositions",
            data={
                "invalid": unknown_overrides,
                "supported": list(MERGE_DISPOSITIONS),
            },
        )
    survivor_attrs = dict(survivor.get("attrs") or {})
    retired_attrs = dict(retired.get("attrs") or {})
    attribute_conflicts, survivor_attrs_merged = _merge_attributes(
        survivor_attrs,
        retired_attrs,
        overrides=overrides,
        declared=_declared_merge_semantics(conn, subtype, catalog_version_id),
        accept_survivor_attributes=accept_survivor_attributes,
    )
    survivor_write_refusals: tuple[dict[str, Any], ...] = ()
    if survivor_attrs_merged is not None:
        narrowing = _survivor_lineage_narrowing(survivor)
        if narrowing is not None:
            survivor_write_refusals = (narrowing,)

    all_by_id = {int(row["edge_id"]): row for row in edges}
    candidates: list[tuple[dict[str, Any], dict[str, Any]]] = []
    target_ids: list[int] = []
    blocking: list[dict[str, Any]] = []
    for old in edges:
        if retired_node_id not in {str(old["src"]), str(old["dst"])}:
            continue
        new = dict(old)
        new["src"] = (
            survivor_node_id if str(old["src"]) == retired_node_id else str(old["src"])
        )
        new["dst"] = (
            survivor_node_id if str(old["dst"]) == retired_node_id else str(old["dst"])
        )
        if new["src"] == new["dst"]:
            blocking.append(
                {
                    "kind": "self_loop_after_rehome",
                    "edge_id": int(old["edge_id"]),
                }
            )
            continue
        new["edge_id"] = _edge_id(
            str(new["src"]), str(new["edge_type"]), str(new["dst"])
        )
        target_ids.append(int(new["edge_id"]))
        candidates.append((old, new))

    missing_target_ids = sorted(set(target_ids) - set(all_by_id))
    if missing_target_ids:
        target_rows = graph.query(
            "SELECT edge_id, src, dst, edge_type, attrs, provenance, confidence, "
            "source, source_record_id, source_revision, run_id, observed_at "
            "FROM okg.graph_edges WHERE edge_id = ANY(:edge_ids)",
            {"edge_ids": missing_target_ids},
        )
        all_by_id.update({int(row["edge_id"]): dict(row) for row in target_rows})

    rehomes: list[dict[str, Any]] = []
    deduplicated: list[dict[str, Any]] = []
    planned_targets: dict[int, dict[str, Any]] = {}
    for old, new in candidates:
        target_id = int(new["edge_id"])
        comparison = planned_targets.get(target_id) or all_by_id.get(target_id)
        if comparison is not None:
            raw_existing_identity, raw_existing_reason = _edge_identity(comparison)
            raw_planned_identity, raw_planned_reason = _edge_identity(new)
            normalized_existing_identity, normalized_existing_reason = _edge_identity(
                comparison,
                survivor_node_id=survivor_node_id,
                retired_node_id=retired_node_id,
            )
            normalized_planned_identity, normalized_planned_reason = _edge_identity(
                new,
                survivor_node_id=survivor_node_id,
                retired_node_id=retired_node_id,
            )
            identities_match_after_alias = (
                raw_existing_reason is None
                and raw_planned_reason is None
                and normalized_existing_reason is None
                and normalized_planned_reason is None
                and raw_existing_identity != raw_planned_identity
                and normalized_existing_identity == normalized_planned_identity
            )
            alias_needed = identities_match_after_alias or any(
                _edge_uses_candidate_record_alias(
                    edge,
                    survivor_node_id=survivor_node_id,
                    retired_node_id=retired_node_id,
                )
                for edge in (comparison, new)
            )
            alias_allowed = nomos_enforcement_mode != "enforce"
            if alias_allowed:
                existing_identity = normalized_existing_identity
                planned_identity = normalized_planned_identity
                existing_reason = normalized_existing_reason
                planned_reason = normalized_planned_reason
            else:
                existing_identity = raw_existing_identity
                planned_identity = raw_planned_identity
                existing_reason = raw_existing_reason
                planned_reason = raw_planned_reason
            reason = existing_reason or planned_reason
            equivalence_evidence: dict[str, Any] | None = None
            if _edge_tuple(comparison) != _edge_tuple(new):
                reason = "edge_id_hash_collision"
            elif reason is None and alias_needed and not alias_allowed:
                reason = "candidate_record_alias_unproved"
            elif reason is None and existing_identity == planned_identity:
                equivalence_evidence = _existing_identity_evidence(
                    conn,
                    deployment_name=deployment_name,
                    existing=comparison,
                    planned=new,
                    candidate_record_alias=(
                        candidate_record_alias if alias_needed else None
                    ),
                )
            elif reason is None:
                existing_party = (
                    comparison.get("source"),
                    str(comparison.get("provenance") or ""),
                )
                planned_party = (
                    new.get("source"),
                    str(new.get("provenance") or ""),
                )
                if existing_party != planned_party:
                    equivalence_evidence, reason = _equivalence_policy_decision(
                        conn,
                        deployment_name=deployment_name,
                        existing=comparison,
                        planned=new,
                        endpoint_subtypes=endpoint_subtypes,
                        catalog_version_id=catalog_version_id,
                        survivor_node_id=survivor_node_id,
                        retired_node_id=retired_node_id,
                        candidate_record_alias=(
                            candidate_record_alias if alias_needed else None
                        ),
                    )
                else:
                    reason = "identity_differs"
            if equivalence_evidence is None:
                blocking.append(
                    {
                        "kind": "edge_identity_conflict",
                        "reason": reason or "identity_differs",
                        "old_edge_id": int(old["edge_id"]),
                        "target_edge_id": target_id,
                        "existing": existing_identity,
                        "planned": planned_identity,
                    }
                )
            else:
                deduplicated.append(
                    {
                        "old": old,
                        "existing": comparison,
                        "assertion_variance": _edge_assertion_variance(comparison, new),
                        "provenance_variance": _provenance_variance(comparison, new),
                        "provenance_union": _edge_provenance_union(comparison, new),
                        "equivalence_evidence": equivalence_evidence,
                    }
                )
            continue
        planned_targets[target_id] = new
        rehomes.append({"old": old, "new": new})

    payload = {
        "deployment_name": deployment_name,
        "branch_name": branch_name,
        "survivor_node_id": survivor_node_id,
        "retired_node_id": retired_node_id,
        "subtype": subtype,
        "pinned_checkpoint": pinned_checkpoint,
        "catalog_version_id": catalog_version_id,
        "branch_mutation_token": branch_mutation_token,
        "footprint_digest": footprint_digest,
        "edge_rehomes": rehomes,
        "deduplicated_edges": deduplicated,
        "attribute_conflicts": list(attribute_conflicts),
        "blocking_conflicts": blocking,
        "accept_survivor_attributes": accept_survivor_attributes,
        "survivor_snapshot": survivor,
        "retired_snapshot": retired,
        "attribute_dispositions": dict(sorted(overrides.items())),
        "survivor_attrs_merged": survivor_attrs_merged,
        "survivor_write_refusals": list(survivor_write_refusals),
    }
    return EntityMergePlan(
        **{
            **payload,
            "edge_rehomes": tuple(rehomes),
            "deduplicated_edges": tuple(deduplicated),
            "attribute_conflicts": attribute_conflicts,
            "blocking_conflicts": tuple(blocking),
            "survivor_write_refusals": survivor_write_refusals,
        },
        plan_digest=_digest(payload),
    )


def _plan_entity_merge_locked(
    conn: psycopg.Connection,
    *,
    survivor_node_id: str,
    retired_node_id: str,
    deployment_name: str = "default",
    branch_name: str = "default",
    accept_survivor_attributes: bool = False,
    attribute_dispositions: dict[str, str] | None = None,
) -> EntityMergePlan:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.publisher import Publisher

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    latest = store.latest_generation(
        deployment_name=deployment_name,
        branch=branch_name,
        status="published",
    )
    if latest is None:
        raise SubstrateError("entity merge requires a published checkpoint")
    catalog_version_id = int((latest.metadata or {}).get("catalog_version_id") or 0)
    if catalog_version_id <= 0:
        raise SubstrateError("published checkpoint has no catalog version pin")
    checkpoint = str(latest.checkpoint_id)
    branch_mutation_token = Publisher.chronos_graph_branch_mutation_token(
        conn, branch_name=branch_name
    )
    plan = _plan_from_graph(
        conn,
        store.checkout(branch_name),
        deployment_name=deployment_name,
        branch_name=branch_name,
        survivor_node_id=survivor_node_id,
        retired_node_id=retired_node_id,
        pinned_checkpoint=checkpoint,
        catalog_version_id=catalog_version_id,
        branch_mutation_token=branch_mutation_token,
        accept_survivor_attributes=accept_survivor_attributes,
        attribute_dispositions=attribute_dispositions,
    )
    # Advisory locks coordinate supported writers. The second token check also
    # catches a direct/bypassing mutation that ignored that lock while planning.
    observed_after = Publisher.chronos_graph_branch_mutation_token(
        conn, branch_name=branch_name
    )
    if observed_after != branch_mutation_token:
        raise SubstrateError(
            "entity merge branch changed while planning",
            data={
                "branch": branch_name,
                "planned_token": branch_mutation_token,
                "observed_token": observed_after,
            },
            fix_hint=(
                "wait for the writer to finish, then rerun the entity-merge dry-run"
            ),
        )
    return plan


def plan_entity_merge(
    conn: psycopg.Connection,
    *,
    survivor_node_id: str,
    retired_node_id: str,
    deployment_name: str = "default",
    branch_name: str = "default",
    accept_survivor_attributes: bool = False,
    attribute_dispositions: dict[str, str] | None = None,
) -> EntityMergePlan:
    """Freeze a branch view and its mutation token under one session lock."""
    from okg.substrate.graph_store import ChronosGraphStore

    # Chronos checkout may commit its metadata read when its context is in
    # autocommit mode. A session advisory lock deliberately survives that
    # commit, unlike the apply path's transaction lock, and fences supported
    # writers across checkout + token + plan construction.
    lock_store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    if not lock_store._lock_branch_write_session(branch_name):
        raise SubstrateError(
            "entity merge planning requires PostgreSQL branch fencing",
            data={"branch": branch_name},
        )
    try:
        return _plan_entity_merge_locked(
            conn,
            survivor_node_id=survivor_node_id,
            retired_node_id=retired_node_id,
            deployment_name=deployment_name,
            branch_name=branch_name,
            accept_survivor_attributes=accept_survivor_attributes,
            attribute_dispositions=attribute_dispositions,
        )
    finally:
        lock_store._unlock_branch_write_session(branch_name)


def _edge_fact(row: dict[str, Any], *, op: str):
    from okg.substrate.library.sources.base import EdgeFact

    return EdgeFact(
        src=str(row["src"]),
        dst=str(row["dst"]),
        edge_type=str(row["edge_type"]),
        attrs=dict(row.get("attrs") or {}),
        provenance=str(row.get("provenance") or "explicit"),
        confidence=float(row.get("confidence") or 0.0),
        source_record_id=_literal_json(row.get("source_record_id")),
        source_revision=_literal_json(row.get("source_revision")),
        op=op,
    )


def _apply_plan(
    conn: psycopg.Connection,
    plan: EntityMergePlan,
    *,
    actor: str,
    fault_after_graph: bool,
) -> dict[str, Any]:
    from okg.substrate.fact_application import apply_bounded_fact_batch
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.publisher import Publisher

    if plan.blocking_conflicts:
        raise SubstrateError(
            "entity merge plan has blocking edge conflicts",
            data={"conflicts": list(plan.blocking_conflicts)},
        )
    unresolved = [
        item
        for item in plan.attribute_conflicts
        if item.get("disposition") == DISPOSITION_UNRESOLVED
    ]
    if unresolved:
        raise SubstrateError(
            "entity merge has unresolved node attribute conflicts",
            data={"conflicts": unresolved},
            fix_hint="disposition each attribute (--merge-attr name=union|"
            "keep_survivor|keep_retiree) or replan with explicit "
            "survivor-attribute acceptance",
        )
    if plan.survivor_write_refusals:
        raise SubstrateError(
            "entity merge would narrow survivor node lineage",
            data={"refusals": list(plan.survivor_write_refusals)},
            fix_hint="the survivor's provenance covers records the rewrite "
            "cannot reproduce; merge attributes manually or retire the "
            "other node instead",
        )

    store = ChronosGraphStore.from_connection(
        conn, ensure_schema=False, autocommit=False
    )
    with conn.transaction():
        # The approved digest binds live catalog and producer-profile evidence,
        # not only Chronos branch rows. Hold shared table locks from before the
        # apply-time replan through receipt commit so a catalog/profile writer
        # cannot change that evidence in the gap between validation and edge
        # retraction. The fact-application path also writes producer profiles,
        # so take SHARE ROW EXCLUSIVE up front, before the branch lock: this
        # serializes concurrent profile writers and avoids two merges both
        # holding SHARE before deadlocking on a later ROW EXCLUSIVE upgrade.
        # Normal admission takes the same profile-table -> branch-lock order.
        # Entity merge is exceptional, so this short coarse lock is preferable
        # to a phantom-prone row lock (a new fingerprint could otherwise become
        # the latest profile row).
        with conn.cursor() as cur:
            cur.execute("LOCK TABLE okg.producer_profiles IN SHARE ROW EXCLUSIVE MODE")
            cur.execute(
                "LOCK TABLE okg.catalog_versions, okg.narrowings, "
                "okg.subtypes IN SHARE MODE"
            )
        store._lock_branch_write(plan.branch_name)
        current_branch_token = Publisher.chronos_graph_branch_mutation_token(
            conn, branch_name=plan.branch_name
        )
        if current_branch_token != plan.branch_mutation_token:
            raise SubstrateError(
                "entity merge branch has unpublished or concurrent writes; replan",
                data={
                    "branch": plan.branch_name,
                    "planned_token": plan.branch_mutation_token,
                    "current_token": current_branch_token,
                    "mutation_started": False,
                },
                fix_hint=(
                    "rerun the entity-merge dry-run against the current branch, "
                    "review the new plan digest, then apply that digest"
                ),
            )
        # Construct the checkout only after the branch lock is held. A
        # pre-lock checkout could miss a writer that commits while this merge
        # is waiting, defeating the incident-footprint drift check below.
        session = store.checkout(plan.branch_name)
        latest = store.latest_generation(
            deployment_name=plan.deployment_name,
            branch=plan.branch_name,
            status="published",
        )
        if latest is None or str(latest.checkpoint_id) != plan.pinned_checkpoint:
            raise SubstrateError(
                "entity merge published checkpoint drifted; replan before mutation",
                data={
                    "planned_checkpoint": plan.pinned_checkpoint,
                    "current_checkpoint": (
                        str(latest.checkpoint_id) if latest is not None else None
                    ),
                    "mutation_started": False,
                },
                fix_hint=(
                    "rerun the entity-merge dry-run, review the new plan, "
                    "then apply its digest"
                ),
            )
        if (
            current_catalog_version_id := _scoped_catalog_version_id(
                conn, plan.deployment_name
            )
        ) != plan.catalog_version_id:
            raise SubstrateError(
                "entity merge catalog version drifted; replan before mutation",
                data={
                    "planned_catalog_version_id": plan.catalog_version_id,
                    "current_catalog_version_id": current_catalog_version_id,
                    "mutation_started": False,
                },
                fix_hint=(
                    "rerun the entity-merge dry-run, review the new plan, "
                    "then apply its digest"
                ),
            )
        current = _plan_from_graph(
            conn,
            session,
            deployment_name=plan.deployment_name,
            branch_name=plan.branch_name,
            survivor_node_id=plan.survivor_node_id,
            retired_node_id=plan.retired_node_id,
            pinned_checkpoint=plan.pinned_checkpoint,
            catalog_version_id=plan.catalog_version_id,
            branch_mutation_token=current_branch_token,
            accept_survivor_attributes=plan.accept_survivor_attributes,
            # The dispositions the operator approved, not the defaults. Dropping
            # them here would either fail the digest check below or, worse, apply
            # a different disposition than the one that was reviewed.
            attribute_dispositions=plan.attribute_dispositions,
        )
        if current.footprint_digest != plan.footprint_digest:
            raise SubstrateError(
                "entity merge incident footprint drifted; replan before mutation",
                data={
                    "planned_footprint_digest": plan.footprint_digest,
                    "current_footprint_digest": current.footprint_digest,
                    "mutation_started": False,
                },
                fix_hint=(
                    "rerun the entity-merge dry-run, review the new plan, "
                    "then apply its digest"
                ),
            )
        if current.plan_digest != plan.plan_digest:
            raise SubstrateError(
                "entity merge plan digest drifted; replan before mutation",
                data={
                    "planned_plan_digest": plan.plan_digest,
                    "current_plan_digest": current.plan_digest,
                    "mutation_started": False,
                },
                fix_hint=(
                    "rerun the entity-merge dry-run, review the new plan, "
                    "then apply its digest"
                ),
            )
        observed_after_replan = Publisher.chronos_graph_branch_mutation_token(
            conn, branch_name=plan.branch_name
        )
        if observed_after_replan != current_branch_token:
            raise SubstrateError(
                "entity merge branch changed during apply validation; replan",
                data={
                    "branch": plan.branch_name,
                    "planned_token": current_branch_token,
                    "current_token": observed_after_replan,
                    "mutation_started": False,
                },
                fix_hint=(
                    "wait for the concurrent writer, then rerun the entity-merge "
                    "dry-run and apply its new digest"
                ),
            )

        by_source: dict[str | None, list[Any]] = {}
        for item in plan.edge_rehomes:
            old, new = dict(item["old"]), dict(item["new"])
            by_source.setdefault(old.get("source"), []).extend(
                [_edge_fact(old, op="R"), _edge_fact(new, op="I")]
            )
        for item in plan.deduplicated_edges:
            old = dict(item["old"])
            by_source.setdefault(old.get("source"), []).append(_edge_fact(old, op="R"))
        for source, facts in sorted(by_source.items(), key=lambda item: str(item[0])):
            source_token = str(source or "unscoped")
            apply_bounded_fact_batch(
                conn,
                store,
                facts,
                deployment_name=plan.deployment_name,
                producer_id="system:entity_merge",
                producer_kind="system",
                intent="entity_merge_edge_rehome",
                catalog_version_id=plan.catalog_version_id,
                authority_scope={
                    "pinned_checkpoint": plan.pinned_checkpoint,
                    "plan_digest": plan.plan_digest,
                    "source": source_token,
                },
                source=source,
                target_branch=plan.branch_name,
                idempotency_key=_digest([plan.plan_digest, "edges", source_token]),
                retractions_allowed=True,
                cross_source_retractions=source is None,
                session=session,
            )

        if plan.survivor_attrs_merged is not None:
            # Only written when a disposition actually changes the survivor's
            # value. A plan that is entirely keep_survivor touches no node, so
            # every pre-existing caller sees exactly the writes it saw before.
            survivor_source = plan.survivor_snapshot.get("source")
            apply_bounded_fact_batch(
                conn,
                store,
                [
                    NodeFact(
                        node_id=plan.survivor_node_id,
                        subtype=plan.subtype,
                        attrs=dict(plan.survivor_attrs_merged),
                        source_record_id=_literal_json(
                            plan.survivor_snapshot.get("source_record_id")
                        ),
                        source_revision=_literal_json(
                            plan.survivor_snapshot.get("source_revision")
                        ),
                        op="I",
                    )
                ],
                deployment_name=plan.deployment_name,
                producer_id="system:entity_merge",
                producer_kind="system",
                intent="entity_merge_survivor_attrs",
                catalog_version_id=plan.catalog_version_id,
                authority_scope={
                    "pinned_checkpoint": plan.pinned_checkpoint,
                    "plan_digest": plan.plan_digest,
                    "survivor_node_id": plan.survivor_node_id,
                },
                source=survivor_source,
                target_branch=plan.branch_name,
                idempotency_key=_digest([plan.plan_digest, "survivor_attrs"]),
                retractions_allowed=True,
                cross_source_retractions=survivor_source is None,
                session=session,
            )

        retired_source = plan.retired_snapshot.get("source")
        apply_bounded_fact_batch(
            conn,
            store,
            [
                NodeFact(
                    node_id=plan.retired_node_id,
                    subtype=plan.subtype,
                    attrs={},
                    source_record_id={"entity_merge_plan": plan.plan_digest},
                    source_revision={"pinned_checkpoint": plan.pinned_checkpoint},
                    op="R",
                )
            ],
            deployment_name=plan.deployment_name,
            producer_id="system:entity_merge",
            producer_kind="system",
            intent="entity_merge_retire_node",
            catalog_version_id=plan.catalog_version_id,
            authority_scope={
                "pinned_checkpoint": plan.pinned_checkpoint,
                "plan_digest": plan.plan_digest,
                "retired_node_id": plan.retired_node_id,
            },
            source=retired_source,
            target_branch=plan.branch_name,
            idempotency_key=_digest([plan.plan_digest, "retired_node"]),
            retractions_allowed=True,
            cross_source_retractions=retired_source is None,
            session=session,
            # The merged-away node is removed, not one source's claim on it:
            # it must not pass to another source that declares it, and every
            # recorded declaration of it goes with it (issue #1901).
            node_retraction="purge",
        )
        if fault_after_graph:
            raise RuntimeError("injected entity-merge fault after graph edits")

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.entity_identity_claims SET canonical_node_id = %s, "
                "resolution_method = CASE WHEN resolution_method = 'mint' "
                "THEN 'merged' ELSE resolution_method END "
                "WHERE deployment_name = %s AND branch_name = %s "
                "AND canonical_node_id = %s",
                (
                    plan.survivor_node_id,
                    plan.deployment_name,
                    plan.branch_name,
                    plan.retired_node_id,
                ),
            )
            cur.execute(
                """
                INSERT INTO okg.entity_merge_receipts
                  (deployment_name, branch_name, survivor_node_id,
                   retired_node_id, subtype, pinned_checkpoint,
                   catalog_version_id, footprint_digest, plan_digest, plan,
                   retired_snapshot, actor)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                        %s::jsonb, %s)
                RETURNING merge_id, status, applied_at
                """,
                (
                    plan.deployment_name,
                    plan.branch_name,
                    plan.survivor_node_id,
                    plan.retired_node_id,
                    plan.subtype,
                    plan.pinned_checkpoint,
                    plan.catalog_version_id,
                    plan.footprint_digest,
                    plan.plan_digest,
                    json.dumps(plan.as_dict(), default=str),
                    json.dumps(plan.retired_snapshot, default=str),
                    actor,
                ),
            )
            merge_id, status, applied_at = cur.fetchone()
    return {
        "merge_id": str(merge_id),
        "status": str(status),
        "applied_at": applied_at.isoformat(),
        "plan_digest": plan.plan_digest,
    }


def remediate_entity_merge(
    conn: psycopg.Connection,
    *,
    survivor_node_id: str,
    retired_node_id: str,
    deployment_name: str = "default",
    branch_name: str = "default",
    expected_plan_digest: str | None = None,
    accept_survivor_attributes: bool = False,
    attribute_dispositions: dict[str, str] | None = None,
    actor: str = "operator",
    apply: bool = False,
    _fault_after_graph: bool = False,
) -> EntityMergeResult:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT merge_id, status, plan_digest, repair_generation_id, "
            "survivor_node_id "
            "FROM okg.entity_merge_receipts WHERE deployment_name = %s "
            "AND branch_name = %s AND retired_node_id = %s",
            (deployment_name, branch_name, retired_node_id),
        )
        prior = cur.fetchone()
    if prior is not None:
        if str(prior[4]) != survivor_node_id:
            raise ValueError(
                "retired id was merged to a different survivor; refusing replay"
            )
        if apply and not expected_plan_digest:
            raise ValueError(
                "entity merge replay --apply requires --expected-plan-digest"
            )
        if expected_plan_digest and str(prior[2]) != expected_plan_digest:
            raise ValueError("retired id already has a different applied merge plan")
        return EntityMergeResult(
            "entity_merge",
            True,
            0,
            {
                "merge_id": str(prior[0]),
                "status": str(prior[1]),
                "plan_digest": str(prior[2]),
                "repair_generation_id": prior[3],
                "replayed": True,
            },
        )
    plan = plan_entity_merge(
        conn,
        survivor_node_id=survivor_node_id,
        retired_node_id=retired_node_id,
        deployment_name=deployment_name,
        branch_name=branch_name,
        accept_survivor_attributes=accept_survivor_attributes,
        attribute_dispositions=attribute_dispositions,
    )
    if not apply:
        return EntityMergeResult("entity_merge", False, 1, {"plan": plan.as_dict()})
    if not expected_plan_digest:
        raise ValueError("entity merge --apply requires --expected-plan-digest")
    if expected_plan_digest != plan.plan_digest:
        raise ValueError(
            "entity merge plan digest changed because the branch has unpublished "
            "or concurrent writes; replan by rerunning dry-run, review the new "
            "plan, and apply its digest"
        )
    receipt = _apply_plan(conn, plan, actor=actor, fault_after_graph=_fault_after_graph)
    return EntityMergeResult(
        "entity_merge",
        True,
        1,
        {"plan": plan.as_dict(), "receipt": receipt, "replayed": False},
    )


def mark_entity_merge_published(
    conn: psycopg.Connection,
    *,
    merge_id: str,
    repair_generation_id: str,
) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "SELECT deployment_name, branch_name, status, repair_generation_id, "
            "applied_at FROM okg.entity_merge_receipts WHERE merge_id = %s FOR UPDATE",
            (merge_id,),
        )
        receipt = cur.fetchone()
        if receipt is None:
            raise ValueError(f"unknown entity merge receipt {merge_id!r}")
        deployment_name, branch_name, status, prior_generation, applied_at = receipt
        if str(status) == "published":
            if str(prior_generation) == repair_generation_id:
                return
            raise ValueError(
                "published entity merge receipt is already generation-bound"
            )
        cur.execute(
            "SELECT metadata::jsonb->>'deployment_name', "
            "metadata::jsonb->>'branch', metadata::jsonb->>'status', "
            "created_at::timestamptz "
            "FROM _chronos_branch_interval_checkpoints WHERE checkpoint_id = %s",
            (repair_generation_id,),
        )
        generation = cur.fetchone()
        if generation is None or str(generation[2]) != "published":
            raise ValueError("entity merge repair generation is not published")
        if (str(generation[0]), str(generation[1])) != (
            str(deployment_name),
            str(branch_name),
        ):
            raise ValueError(
                "entity merge repair generation belongs to another deployment or branch"
            )
        if generation[3] < applied_at:
            raise ValueError("entity merge repair generation predates the merge apply")
        cur.execute(
            "UPDATE okg.entity_merge_receipts SET status = 'published', "
            "repair_generation_id = %s, published_at = now() "
            "WHERE merge_id = %s AND status = 'awaiting_publish'",
            (repair_generation_id, merge_id),
        )
        if cur.rowcount != 1:
            raise ValueError("entity merge receipt was not awaiting publication")


__all__ = [
    "DISPOSITION_UNRESOLVED",
    "MERGE_DISPOSITIONS",
    "PROVENANCE_ATTR_KEYS",
    "EntityMergePlan",
    "EntityMergeResult",
    "mark_entity_merge_published",
    "plan_entity_merge",
    "remediate_entity_merge",
]
