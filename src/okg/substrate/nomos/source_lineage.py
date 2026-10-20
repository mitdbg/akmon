"""Source-lineage helpers for Nomos read enforcement."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from copy import deepcopy
from typing import Any

LINEAGE_ATTR = "nomos_source_lineage"
LINEAGE_SCHEMA = "okg.nomos.source_lineage.v1"

#: Declared bound on per-source record detail carried in a COMBINED (derived)
#: lineage envelope — PACT ``bound-derived-lineage-envelope``.
#:
#: ``source_ids`` is never bounded: it is what source-level policy evaluation,
#: trust resolution, and readiness coverage all read, and it is small by
#: construction (one entry per distinct source). The growth is entirely in the
#: per-source ``source_records`` / ``source_record_ids`` / ``source_revisions``
#: lists, which union across every contributing input. A claim node lifted from
#: thousands of chunks would otherwise carry thousands of record ids in its
#: attrs JSON, on every such node — a storage regression on a host that has
#: already hit ENOSPC twice.
#:
#: Exceeding the bound is DECLARED, never silent: see
#: :data:`RECORD_DETAIL_TRUNCATED`.
MAX_LINEAGE_RECORDS_PER_SOURCE = 64

#: Envelope key recording whether per-source record detail is complete.
#:
#: ABSENCE MEANS COMPLETE. The key is emitted ONLY when truncated, so every
#: untruncated envelope — every source-ingest one and most derived ones —
#: keeps the bytes it had before this bound existed instead of churning the
#: whole graph to record a negative.
#:
#: So test for truncation, never for completeness:
#:
#:     if lineage.get(RECORD_DETAIL_KEY) == RECORD_DETAIL_TRUNCATED:  # right
#:     if lineage[RECORD_DETAIL_KEY] == RECORD_DETAIL_COMPLETE:       # WRONG
#:
#: the second raises KeyError on a complete envelope, and is never true.
#: ``RECORD_DETAIL_COMPLETE`` exists to name the default in docs and tests,
#: not to be compared against a stored envelope.
RECORD_DETAIL_KEY = "record_detail"
RECORD_DETAIL_COMPLETE = "complete"
RECORD_DETAIL_TRUNCATED = "truncated"


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value, sort_keys=True)
    except TypeError:
        return str(value)
    return value


def _fingerprint(value: Any) -> str:
    # One encode: equal to encoding ``_json_safe(value)``, which falls back to
    # ``str(value)`` exactly when this encode raises TypeError (issue #3002:
    # every read rebuilds every row's lineage, so the encodes add up).
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    except TypeError:
        return json.dumps(str(value), sort_keys=True, separators=(",", ":"))


def _canonical_ref_payload(value: Any) -> str:
    if isinstance(value, str):
        text = value.strip()
        return text or _fingerprint("")
    return _fingerprint(value)


def _normalise_source_id(source_id: Any) -> str | None:
    if source_id is None:
        return None
    text = str(source_id).strip()
    return text or None


def _append_unique(values: list[Any], value: Any) -> None:
    value = _json_safe(value)
    fingerprint = _fingerprint(value)
    if not any(_fingerprint(existing) == fingerprint for existing in values):
        values.append(value)


def _iterable_values(value: Any) -> list[Any]:
    if value is None:
        return []
    if (
        isinstance(value, Mapping)
        or isinstance(value, (str, bytes))
        or not isinstance(value, Iterable)
    ):
        return [value]
    return list(value)


def _entry(entries: dict[str, dict[str, Any]], source_id: str) -> dict[str, Any]:
    return entries.setdefault(source_id, {"source_id": source_id})


def _merge_source_record(
    target: dict[str, Any],
    source_record_id: Any,
    *,
    source_revision: Any | None = None,
) -> None:
    if source_record_id is None:
        return
    source_record_id = _json_safe(source_record_id)
    record: dict[str, Any] = {"source_record_id": source_record_id}
    if source_revision is not None:
        record["source_revision"] = _json_safe(source_revision)
    _append_unique(target.setdefault("source_records", []), record)
    _append_unique(target.setdefault("source_record_ids", []), source_record_id)
    if source_revision is not None:
        _append_unique(target.setdefault("source_revisions", []), source_revision)


def _merge_source_entry(
    entries: dict[str, dict[str, Any]],
    source_id: Any,
    *,
    source_record_id: Any | None = None,
    source_revision: Any | None = None,
    policy_fingerprint: str | None = None,
) -> None:
    normalized = _normalise_source_id(source_id)
    if normalized is None:
        return
    target = _entry(entries, normalized)
    if source_record_id is not None:
        _merge_source_record(
            target,
            source_record_id,
            source_revision=source_revision,
        )
    if source_revision is not None:
        revisions = target.setdefault("source_revisions", [])
        _append_unique(revisions, source_revision)
    if policy_fingerprint:
        fingerprints = target.setdefault("policy_fingerprints", [])
        _append_unique(fingerprints, policy_fingerprint)


def _merge_lineage_value(
    entries: dict[str, dict[str, Any]],
    value: Any,
) -> str:
    if value is None:
        return "missing"
    if isinstance(value, str):
        _merge_source_entry(entries, value)
        return "ok"
    if isinstance(value, Mapping):
        status = str(value.get("status") or "ok")
        sources = value.get("sources")
        if isinstance(sources, Iterable) and not isinstance(sources, (str, bytes)):
            for source in sources:
                if isinstance(source, Mapping):
                    source_id = source.get("source_id") or source.get("id")
                    if source_id is None:
                        status = "malformed"
                        continue
                    _merge_source_entry(entries, source_id)
                    normalized = _normalise_source_id(source_id)
                    if normalized is None:
                        status = "malformed"
                        continue
                    target = _entry(entries, normalized)
                    source_records = _iterable_values(source.get("source_records"))
                    for record in source_records:
                        if isinstance(record, Mapping):
                            record_id = (
                                record.get("source_record_id")
                                or record.get("record_id")
                                or record.get("id")
                            )
                            revision = record.get("source_revision")
                            if revision is None:
                                revision = record.get("revision")
                            _merge_source_record(
                                target,
                                record_id,
                                source_revision=revision,
                            )
                        else:
                            _merge_source_record(target, record)
                    records = _iterable_values(source.get("source_record_ids"))
                    revisions = _iterable_values(source.get("source_revisions"))
                    if not source_records:
                        if records and len(records) == len(revisions):
                            for record, revision in zip(records, revisions):
                                _merge_source_record(
                                    target,
                                    record,
                                    source_revision=revision,
                                )
                        else:
                            for record in records:
                                _merge_source_record(target, record)
                    for revision in revisions:
                        _append_unique(
                            target.setdefault("source_revisions", []), revision
                        )
                    for fingerprint in source.get("policy_fingerprints") or []:
                        _append_unique(
                            target.setdefault("policy_fingerprints", []), fingerprint
                        )
                    # PACT bound-derived-lineage-envelope: the truncation flag
                    # must LATCH through normalization. The read path does not
                    # read the stored envelope, it rebuilds it via
                    # lineage_for_row -> normalize_source_lineage, so a flag
                    # dropped here is a flag the fail-closed guard never sees
                    # — the envelope would read as complete on a subset.
                    if source.get("record_detail_truncated"):
                        target["record_detail_truncated"] = True
                else:
                    _merge_source_entry(entries, source)
        source_ids = value.get("source_ids")
        if isinstance(source_ids, Iterable) and not isinstance(
            source_ids, (str, bytes)
        ):
            for source_id in source_ids:
                _merge_source_entry(entries, source_id)
        elif isinstance(source_ids, str):
            _merge_source_entry(entries, source_ids)
        elif "source_id" in value:
            _merge_source_entry(entries, value.get("source_id"))
        return status if entries else "missing"
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        for source_id in value:
            _merge_source_entry(entries, source_id)
        return "ok" if entries else "missing"
    return "malformed"


def normalize_source_lineage(
    value: Any = None,
    *,
    source_id: str | None = None,
    source_record_id: Any | None = None,
    source_revision: Any | None = None,
    policy_fingerprint: str | None = None,
) -> dict[str, Any]:
    """Return a deterministic source-lineage envelope.

    The envelope is intentionally small and JSON-native so it can live in graph
    attrs across substrate stages. Missing or malformed lineage yields no source
    ids; Nomos read enforcement treats that as fail-closed.
    """

    entries: dict[str, dict[str, Any]] = {}
    status = _merge_lineage_value(entries, value)
    if entries:
        status = "ok" if status != "malformed" else status
    elif source_id:
        status = "ok"

    _merge_source_entry(
        entries,
        source_id,
        source_record_id=source_record_id,
        source_revision=source_revision,
        policy_fingerprint=policy_fingerprint,
    )
    if policy_fingerprint and source_id is None:
        for target in entries.values():
            fingerprints = target.setdefault("policy_fingerprints", [])
            _append_unique(fingerprints, policy_fingerprint)

    source_ids = sorted(entries)
    sources = [entries[source_id] for source_id in source_ids]
    for source in sources:
        for key in (
            "source_records",
            "source_record_ids",
            "source_revisions",
            "policy_fingerprints",
        ):
            if key in source:
                source[key] = sorted(source[key], key=_fingerprint)

    if not source_ids and status not in {"malformed", "missing"}:
        status = "missing"

    # Latch the truncation marker (PACT bound-derived-lineage-envelope).
    # `truncated` absorbs `complete`: once any contributing envelope or any
    # surviving source says its record detail was capped, the rebuilt
    # envelope must keep saying so. Re-normalizing a stored envelope is the
    # read path's ONLY view of it, so dropping the marker here reads as
    # "complete" over a capped subset — a silent fail-open on record-level
    # policy.
    # Emitted ONLY when truncated. Absence means complete, so every
    # untruncated envelope — which is every source-ingest envelope and most
    # derived ones — keeps exactly the bytes it had before, rather than
    # churning every row in the graph to record a negative.
    truncated = bool(
        (
            isinstance(value, Mapping)
            and value.get(RECORD_DETAIL_KEY) == RECORD_DETAIL_TRUNCATED
        )
        or any(source.get("record_detail_truncated") for source in sources)
    )

    envelope = {
        "schema": LINEAGE_SCHEMA,
        "status": status,
        "source_ids": source_ids,
        "sources": sources,
    }
    if truncated:
        envelope[RECORD_DETAIL_KEY] = RECORD_DETAIL_TRUNCATED
    return envelope


def combine_source_lineages(
    values: Iterable[Any],
    *,
    policy_fingerprint: str | None = None,
) -> dict[str, Any]:
    """Return the union of input lineage envelopes.

    A derived fact is only trustworthy when every contributing input has
    well-formed lineage. Preserve known source IDs for audit, but mark the
    combined envelope malformed when any input is missing or malformed so normal
    reads fail closed.
    """

    entries: dict[str, dict[str, Any]] = {}
    saw_input = False
    malformed = False
    inherited_truncation = False
    for value in values:
        saw_input = True
        lineage = normalize_source_lineage(value)
        if not source_ids_from_lineage(lineage):
            malformed = True
        # The per-input envelopes are discarded below (only `entries`
        # survives), so read the TOP-LEVEL marker here or it is lost. The
        # per-source flags latch through _merge_lineage_value, but relying
        # on those alone would make the fail-closed invariant depend on
        # every future writer stamping both — and an input carrying only
        # the top-level marker would launder to complete in one hop.
        if lineage.get(RECORD_DETAIL_KEY) == RECORD_DETAIL_TRUNCATED:
            inherited_truncation = True
        status = _merge_lineage_value(entries, lineage)
        if status != "ok":
            malformed = True
    envelope = normalize_source_lineage(
        {"sources": list(entries.values())} if entries else None,
        policy_fingerprint=policy_fingerprint,
    )
    if malformed or not saw_input:
        envelope["status"] = "malformed"
    if inherited_truncation:
        envelope[RECORD_DETAIL_KEY] = RECORD_DETAIL_TRUNCATED
    return _bound_record_detail(envelope)


def _bound_record_detail(envelope: dict[str, Any]) -> dict[str, Any]:
    """Cap per-source record detail and DECLARE it when capped.

    PACT ``bound-derived-lineage-envelope``. The record lists union across
    every contributing input, so a wide derivation grows them without limit;
    ``source_ids`` does not grow that way and is never touched here.

    Truncation is marked, never silent. A truncated envelope that still read
    as complete would let a record-level policy allow a read on evidence that
    was discarded — a silent fail-open, and the most dangerous thing this
    change could produce. Downstream, an envelope marked truncated is treated
    as unaddressable for fine-grained access and fails closed (see
    ``okg.substrate.nomos.read_filter``); source-level policy is unaffected
    because the source ids are complete.
    """
    truncated = False
    for source in envelope.get("sources") or []:
        if not isinstance(source, dict):
            continue
        for key in ("source_records", "source_record_ids", "source_revisions"):
            values = source.get(key)
            if isinstance(values, list) and len(values) > MAX_LINEAGE_RECORDS_PER_SOURCE:
                # Entries are already sorted by _fingerprint in
                # normalize_source_lineage, so the kept subset is stable
                # across runs rather than dependent on merge order.
                source[key] = values[:MAX_LINEAGE_RECORDS_PER_SOURCE]
                source["record_detail_truncated"] = True
                truncated = True
    # Upgrade only, never downgrade. A hop-2 node inheriting an already-capped
    # envelope carries fewer records than the bound, so nothing here
    # re-triggers truncation — overwriting with `complete` would let two hops
    # launder the marker and the read would fail open on a capped subset.
    if truncated or envelope.get(RECORD_DETAIL_KEY) == RECORD_DETAIL_TRUNCATED:
        envelope[RECORD_DETAIL_KEY] = RECORD_DETAIL_TRUNCATED
    return envelope


def stamp_attrs_with_source_lineage(
    attrs: Mapping[str, Any] | None,
    *,
    source_id: str | None,
    source_record_id: Any | None = None,
    source_revision: Any | None = None,
    policy_fingerprint: str | None = None,
    preserve_existing_lineage: bool = False,
) -> dict[str, Any]:
    """Return attrs with a normalized Nomos source-lineage envelope.

    ``preserve_existing_lineage`` marks a RE-APPLICATION of facts that
    were already attributed when they were emitted: the publisher writing
    an accepted derived fact-log row into the graph. Such a write must
    keep the lineage the row already carries — overwriting it with the
    re-applying producer's own id makes a derived node claim its emitter
    as its root source, which launders the emitter's declared trust label
    onto anything that reads the node (#1534).

    Before ``_publisher_derived`` stopped being the single re-application
    source id, that literal was the only way this branch was reached; it
    stays as a floor so callers that still pass it keep the behaviour.
    """

    stamped = dict(attrs or {})
    existing = stamped.get(LINEAGE_ATTR)
    if existing is None and source_id is None:
        return stamped
    if preserve_existing_lineage or source_id == "_publisher_derived":
        stamped[LINEAGE_ATTR] = normalize_source_lineage(
            existing,
            policy_fingerprint=policy_fingerprint,
        )
        return stamped
    if source_id is not None:
        existing = None
    stamped[LINEAGE_ATTR] = normalize_source_lineage(
        existing,
        source_id=None if existing is not None else source_id,
        source_record_id=source_record_id if existing is None else None,
        source_revision=source_revision if existing is None else None,
        policy_fingerprint=policy_fingerprint,
    )
    return stamped


def lineage_for_row(
    attrs: Mapping[str, Any] | None,
    *,
    source_id: str | None,
    source_record_id: Any | None = None,
    source_revision: Any | None = None,
) -> dict[str, Any]:
    """Normalize lineage for a persisted graph row.

    Legacy rows can lack the attrs envelope but still carry source columns, so
    read enforcement can bridge them while new writes converge on attrs.
    """

    attr_lineage = (attrs or {}).get(LINEAGE_ATTR)
    return normalize_source_lineage(
        attr_lineage,
        source_id=source_id if attr_lineage is None else None,
        source_record_id=source_record_id if attr_lineage is None else None,
        source_revision=source_revision if attr_lineage is None else None,
    )


def canonical_resource_ref(kind: str, value: Any) -> str | None:
    """Return a stable policy-addressable ref for a graph or source resource."""

    normalized_kind = _normalise_source_id(kind)
    if normalized_kind is None or value is None:
        return None
    return f"{normalized_kind}:{_canonical_ref_payload(_json_safe(value))}"


def graph_object_ref(kind: str, object_id: Any) -> str | None:
    """Return a stable policy ref for a graph node, edge, or other object id."""

    return canonical_resource_ref(kind, object_id)


def named_set_ref(set_id: Any) -> str | None:
    """Return a stable policy ref for a reusable access-control set."""

    return canonical_resource_ref("set", set_id)


def horizon_ref(as_of: Any) -> str | None:
    """Return a stable policy ref for an as-of access-control horizon."""

    return canonical_resource_ref("horizon", as_of)


def source_record_ref(source_id: Any, source_record_id: Any) -> str | None:
    """Return a stable source-agnostic ref for one source record."""

    normalized_source_id = _normalise_source_id(source_id)
    if normalized_source_id is None or source_record_id is None:
        return None
    return canonical_resource_ref(
        "source_record",
        {
            "source_id": normalized_source_id,
            "source_record_id": _json_safe(source_record_id),
        },
    )


def source_record_contributors_from_lineage(
    lineage: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], ...]:
    """Return stable source-record contributors from a normalized lineage envelope."""

    if not isinstance(lineage, Mapping) or lineage.get("status") != "ok":
        return ()
    sources = lineage.get("sources")
    if not isinstance(sources, Iterable) or isinstance(sources, (str, bytes)):
        return ()
    contributors: list[dict[str, Any]] = []
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        source_id = _normalise_source_id(source.get("source_id") or source.get("id"))
        if source_id is None:
            continue
        raw_source_records = _iterable_values(source.get("source_records"))
        records: list[tuple[Any, Any | None]] = []
        for raw_source_record in raw_source_records:
            if isinstance(raw_source_record, Mapping):
                record_id = (
                    raw_source_record.get("source_record_id")
                    or raw_source_record.get("record_id")
                    or raw_source_record.get("id")
                )
                revision = raw_source_record.get("source_revision")
                if revision is None:
                    revision = raw_source_record.get("revision")
                records.append((record_id, revision))
            else:
                records.append((raw_source_record, None))
        if not records:
            raw_record_ids = _iterable_values(source.get("source_record_ids"))
            raw_revisions = _iterable_values(source.get("source_revisions"))
            if raw_record_ids and len(raw_record_ids) == len(raw_revisions):
                records.extend(zip(raw_record_ids, raw_revisions))
            else:
                records.extend((record_id, None) for record_id in raw_record_ids)
        for raw_record_id, raw_revision in records:
            if raw_record_id is None:
                continue
            record_id = _json_safe(raw_record_id)
            ref = source_record_ref(source_id, record_id)
            if ref is None:
                continue
            contributor: dict[str, Any] = {
                "source_id": source_id,
                "source_record_id": record_id,
                "source_record_ref": ref,
            }
            if raw_revision is not None:
                contributor["source_revision"] = _json_safe(raw_revision)
            contributors.append(contributor)
    return tuple(sorted(contributors, key=lambda item: item["source_record_ref"]))


def source_record_refs_from_lineage(
    lineage: Mapping[str, Any] | None,
) -> tuple[str, ...]:
    """Return source-record refs only when lineage is well-formed."""

    return tuple(
        contributor["source_record_ref"]
        for contributor in source_record_contributors_from_lineage(lineage)
    )


def source_ids_from_lineage(lineage: Mapping[str, Any] | None) -> tuple[str, ...]:
    if not isinstance(lineage, Mapping) or lineage.get("status") != "ok":
        return ()
    source_ids = lineage.get("source_ids")
    if not isinstance(source_ids, Iterable) or isinstance(source_ids, (str, bytes)):
        return ()
    return tuple(
        source_id
        for source_id in (_normalise_source_id(source_id) for source_id in source_ids)
        if source_id is not None
    )


def attrs_with_lineage(
    attrs: Mapping[str, Any] | None,
    lineage: Mapping[str, Any],
) -> dict[str, Any]:
    stamped = dict(attrs or {})
    stamped[LINEAGE_ATTR] = _copy_json_tree(dict(lineage))
    return stamped


def _copy_json_tree(value: Any) -> Any:
    """``copy.deepcopy(value)`` for a JSON-shaped value, several times faster.

    A read stamps every returned row with its own copy of the row's lineage
    (issue #3002). Plain dicts, lists and tuples are rebuilt; str, int, float,
    bool and None are immutable and shared, as ``deepcopy`` shares them; any
    other value (a subclass included) goes to ``deepcopy`` itself.
    """
    kind = type(value)
    if kind is dict:
        return {key: _copy_json_tree(item) for key, item in value.items()}
    if kind is list:
        return [_copy_json_tree(item) for item in value]
    if kind is tuple:
        return tuple(_copy_json_tree(item) for item in value)
    if kind in _IMMUTABLE_JSON_TYPES:
        return value
    return deepcopy(value)


_IMMUTABLE_JSON_TYPES = frozenset({str, int, float, bool, type(None)})
