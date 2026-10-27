"""Facts for versioned documents: the helper for the `document_versions` module.

A source that reads documents which are issued again and again (a regulatory
report, a wiki page, a specification) hands this helper each version it has
read. The helper returns the node and edge facts the `document_versions`
ontology module declares, and it settles the things every such source would
otherwise get slightly different:

* **Fixed ids.** `docver:<deployment>:<entity>:<version>` for a version,
  `docblock:...:<position>` for a block, `docval:...:<ordinal>` for a value,
  `blockann:...:<kind>:<key>` for an annotation. A ":" or "%" inside a part is
  percent-escaped, so two different identities never share an id. Equal input
  gives equal facts, in the same order, whatever order the versions were added.
* **The latest flag.** One admitted version per (entity, family) is latest:
  the one with the greatest (issued_at, version). A version issued earlier than
  the current latest never takes the flag, however late it arrives. The flag
  is written on the version and copied onto every block, value and annotation,
  because a rule engine reads those as rows with no join.
* **Edges.** `contains` from a version to its blocks and values; `annotates`
  from an annotation to its version; `amends` to the version a version names;
  `supersedes` from each admitted version to the admitted version just before
  it in its (entity, family).
* **Refusals.** A version with no time is refused, and so is a time that is
  not a UTC instant to the second, an attribute that is not text, a duplicate
  id or key, and an `amends` that names a version never added. A version that
  was received but not admitted is the one case with no time or content: it
  is recorded with `admitted` "false" and the reason, and has no blocks.

Every attribute a surface reads is text. The one integer, `position`, is for
ordering and display. Nothing here names a particular kind of document.

The helper is complete-set: call `facts()` after adding every version the
source knows for the entities it covers. A version that stops being latest is
re-emitted with "false" on itself and on everything it contains, and the
runner sees the change as an ordinary attribute update.

A `supersedes` edge depends on the other versions: when a version arrives late
but was issued in the middle of a chain, the edge that skipped over it is no
longer true. Plain fact application only adds, so a source that must see such
an edge go away returns `record_set()` in its `SourceRun`: one record per
version, holding that version's nodes and the edges it owns, so the runner
retracts what a record stops emitting. A source that forgets it leaves two
`supersedes` edges on one version; the module declares that edge with the
cardinality `at_most_one_target`, so the consistency gate blocks the publish.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator, Mapping, Sequence, Union

from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
)


__all__ = [
    "BlockAnnotation",
    "DocumentBlock",
    "DocumentValue",
    "DocumentVersion",
    "DocumentVersionDuplicate",
    "DocumentVersionError",
    "DocumentVersionFacts",
    "DocumentVersionNotText",
    "DocumentVersionReferenceMissing",
    "DocumentVersionTimeInvalid",
    "DocumentVersionTimeMissing",
    "SUBTYPE_BLOCK",
    "SUBTYPE_VALUE",
    "SUBTYPE_VERSION",
    "SUBTYPE_ANNOTATION",
    "TEXT_ATTRIBUTES",
    "canonical_json",
]


SUBTYPE_VERSION = "document_version"
SUBTYPE_BLOCK = "document_block"
SUBTYPE_VALUE = "document_value"
SUBTYPE_ANNOTATION = "block_annotation"

#: Attributes of a version that a child row repeats. A row source cannot join.
_IDENTITY_ATTRS = (
    "entity",
    "family",
    "version",
    "issued_at",
    "covers",
    "doc_kind",
)

#: Every attribute the module declares with a text range, by subtype. The
#: module's schema and this table must agree; a test compares them.
TEXT_ATTRIBUTES: Mapping[str, tuple[str, ...]] = {
    SUBTYPE_VERSION: _IDENTITY_ATTRS + (
        "is_latest",
        "admitted",
        "refusal",
        "title",
        "raw_sha256",
        "reader_ref",
    ),
    SUBTYPE_BLOCK: _IDENTITY_ATTRS + (
        "is_latest",
        "block_key",
        "block_kind",
        "text",
        "payload",
    ),
    SUBTYPE_VALUE: _IDENTITY_ATTRS + (
        "is_latest",
        "value_key",
        "name",
        "unit",
        "context",
        "payload",
    ),
    SUBTYPE_ANNOTATION: _IDENTITY_ATTRS + (
        "is_latest",
        "annotation_key",
        "annotation_kind",
        "payload",
    ),
}

_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_TIME_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")


class DocumentVersionError(ValueError):
    """A version, block, value or annotation the helper refuses."""

    code = "document_version_invalid"


class DocumentVersionTimeMissing(DocumentVersionError):
    """An admitted version has no issue time, so it cannot be ordered."""

    code = "document_version_time_missing"


class DocumentVersionTimeInvalid(DocumentVersionError):
    """A time that is not a UTC instant to the second."""

    code = "document_version_time_invalid"


class DocumentVersionNotText(DocumentVersionError):
    """An attribute a surface reads was given something other than text."""

    code = "document_version_not_text"


class DocumentVersionDuplicate(DocumentVersionError):
    """Two versions, blocks, values or annotations would share an id or key."""

    code = "document_version_duplicate"


class DocumentVersionReferenceMissing(DocumentVersionError):
    """An `amends` names a version that was never added."""

    code = "document_version_reference_missing"


@dataclass(frozen=True)
class DocumentBlock:
    """One ordered piece of a version's content.

    `key` is the producer's key, unique within the version. `payload` is any
    JSON value; it is stored as canonical JSON text.
    """

    key: str
    kind: str
    text: str
    payload: Any = field(default_factory=dict)


@dataclass(frozen=True)
class DocumentValue:
    """One named value a version states."""

    key: str
    name: str
    unit: str = ""
    context: str = ""
    payload: Any = field(default_factory=dict)


@dataclass(frozen=True)
class BlockAnnotation:
    """A note of a stated kind (layout, history, ...) on a version's blocks."""

    kind: str
    key: str
    payload: Any = field(default_factory=dict)


@dataclass(frozen=True)
class DocumentVersion:
    """One issue of a document, with its content.

    `issued_at` is a UTC instant to the second, either the text
    `YYYY-MM-DDTHH:MM:SSZ` or a timezone-aware `datetime` with no fraction of
    a second. It may be None only when `admitted` is False. `amends` names, by
    `version`, another version of the same entity.
    """

    entity: str
    family: str
    version: str
    issued_at: Union[str, datetime, None]
    covers: str = ""
    doc_kind: str = ""
    title: str = ""
    raw_sha256: str = ""
    reader_ref: str = ""
    admitted: bool = True
    refusal: str = ""
    amends: str = ""
    blocks: Sequence[DocumentBlock] = ()
    values: Sequence[DocumentValue] = ()
    annotations: Sequence[BlockAnnotation] = ()


def canonical_json(value: Any, label: str = "payload") -> str:
    """Sorted keys, no spaces, non-ASCII kept; refuses NaN and non-JSON values."""
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise DocumentVersionNotText(
            f"{label} is not JSON-encodable: {exc}"
        ) from exc


def _text(label: str, value: Any, *, required: bool = False) -> str:
    if not isinstance(value, str):
        raise DocumentVersionNotText(
            f"{label} must be text, got {type(value).__name__}: a surface "
            "reads this attribute and the engine refuses non-text"
        )
    if "\x00" in value:
        raise DocumentVersionNotText(f"{label} holds a NUL character")
    if required and not value.strip():
        raise DocumentVersionError(f"{label} is required and was empty")
    return value


def _sequence(label: str, value: Any) -> tuple:
    if isinstance(value, (str, bytes, Mapping)) or not isinstance(
        value, (list, tuple)
    ):
        raise DocumentVersionError(
            f"{label} must be a list or tuple, got {type(value).__name__}"
        )
    return tuple(value)


def _escape(part: str) -> str:
    return part.replace("%", "%25").replace(":", "%3A")


def _issued_at(label: str, value: Any, *, admitted: bool) -> str:
    """The version's time as `YYYY-MM-DDTHH:MM:SSZ`, or "" if it may be absent."""
    if value is None or (isinstance(value, str) and not value.strip()):
        if admitted:
            raise DocumentVersionTimeMissing(
                f"{label} has no issue time. A version that cannot be ordered "
                "cannot be latest or superseded; refuse it, or record it "
                "with admitted=False and a refusal reason"
            )
        return ""
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise DocumentVersionTimeInvalid(
                f"{label} is a datetime with no timezone; give it one so the "
                "instant is not guessed"
            )
        if value.microsecond:
            raise DocumentVersionTimeInvalid(
                f"{label} has a fraction of a second; give the time to the "
                "second"
            )
        instant = value.astimezone(timezone.utc)
        # Written by hand: `strftime("%Y")` does not pad a year below 1000 on
        # every platform, and the text is what orders the versions.
        return (
            f"{instant.year:04d}-{instant.month:02d}-{instant.day:02d}"
            f"T{instant.hour:02d}:{instant.minute:02d}:{instant.second:02d}Z"
        )
    if not isinstance(value, str):
        raise DocumentVersionTimeInvalid(
            f"{label} must be text or a datetime, got {type(value).__name__}"
        )
    # The text itself orders the versions, so its shape must be exact:
    # `strptime` alone accepts "2026-9-01" and a space-padded day, and
    # "2026-9" sorts after "2026-10" as text.
    if not _TIME_RE.fullmatch(value):
        raise DocumentVersionTimeInvalid(
            f"{label} {value!r} is not a UTC instant to the second written "
            "YYYY-MM-DDTHH:MM:SSZ with every field zero-padded (an offset, a "
            "missing Z, a fraction of a second or an unpadded field is "
            "refused)"
        )
    try:
        datetime.strptime(value, _TIME_FORMAT)
    except ValueError as exc:
        raise DocumentVersionTimeInvalid(
            f"{label} {value!r} is not a real date and time"
        ) from exc
    return value


def _record_node(node: NodeFact) -> RecordNode:
    return RecordNode(
        node_id=node.node_id,
        subtype=node.subtype,
        source_record_id=node.source_record_id,
        source_revision=node.source_revision,
    )


@dataclass
class _Prepared:
    """A validated version plus the ids and attributes derived from it."""

    version: DocumentVersion
    node_id: str
    issued_at: str
    blocks: tuple[DocumentBlock, ...]
    values: tuple[DocumentValue, ...]
    annotations: tuple[BlockAnnotation, ...]


class DocumentVersionFacts:
    """Collects versions and returns the module's node and edge facts.

    Add every version with `add`, then read `facts()`. Adding order does not
    matter: ids, flags, edges and the order of facts depend only on the set.
    """

    def __init__(self, deployment: str) -> None:
        self._deployment = _text("deployment", deployment, required=True)
        self._prepared: dict[str, _Prepared] = {}

    @property
    def deployment(self) -> str:
        return self._deployment

    def version_id(self, entity: str, version: str) -> str:
        return "docver:" + ":".join(
            (_escape(self._deployment), _escape(entity), _escape(version))
        )

    def add(self, version: DocumentVersion) -> str:
        """Validate one version and keep it. Returns its node id."""
        if not isinstance(version, DocumentVersion):
            raise DocumentVersionError(
                f"add() takes a DocumentVersion, got {type(version).__name__}"
            )
        entity = _text("entity", version.entity, required=True)
        _text("family", version.family, required=True)
        name = _text("version", version.version, required=True)
        label = f"version {entity}/{name}"
        admitted = version.admitted
        if not isinstance(admitted, bool):
            raise DocumentVersionNotText(
                f"{label}: admitted must be a bool, got {type(admitted).__name__}"
            )
        issued_at = _issued_at(
            f"{label}: issued_at", version.issued_at, admitted=admitted
        )
        for attr in ("covers", "doc_kind", "title", "reader_ref", "amends"):
            _text(f"{label}: {attr}", getattr(version, attr))
        refusal = _text(f"{label}: refusal", version.refusal)
        raw_sha256 = _text(f"{label}: raw_sha256", version.raw_sha256)
        if raw_sha256 and not _SHA256_RE.fullmatch(raw_sha256):
            raise DocumentVersionError(
                f"{label}: raw_sha256 must be 64 lower-case hex digits or empty"
            )
        blocks = _sequence(f"{label}: blocks", version.blocks)
        values = _sequence(f"{label}: values", version.values)
        annotations = _sequence(f"{label}: annotations", version.annotations)
        if admitted and refusal:
            raise DocumentVersionError(
                f"{label} is admitted but carries a refusal reason"
            )
        if not admitted:
            if not refusal.strip():
                raise DocumentVersionError(
                    f"{label} is not admitted and gives no refusal reason"
                )
            if blocks or values or annotations:
                raise DocumentVersionError(
                    f"{label} is not admitted but carries content; a kept, "
                    "refused version has no blocks, values or annotations"
                )
        self._check_children(label, blocks, values, annotations)
        node_id = self.version_id(entity, name)
        if node_id in self._prepared:
            raise DocumentVersionDuplicate(
                f"{label} was added twice. A version name is unique within an "
                "entity, across families"
            )
        self._prepared[node_id] = _Prepared(
            version=version,
            node_id=node_id,
            issued_at=issued_at,
            blocks=blocks,
            values=values,
            annotations=annotations,
        )
        return node_id

    def _check_children(
        self,
        label: str,
        blocks: tuple,
        values: tuple,
        annotations: tuple,
    ) -> None:
        seen: set[Any] = set()
        for index, block in enumerate(blocks):
            where = f"{label}: block {index}"
            if not isinstance(block, DocumentBlock):
                raise DocumentVersionError(f"{where} is not a DocumentBlock")
            _text(f"{where}: key", block.key, required=True)
            _text(f"{where}: kind", block.kind)
            _text(f"{where}: text", block.text)
            canonical_json(block.payload, f"{where}: payload")
            if ("block", block.key) in seen:
                raise DocumentVersionDuplicate(
                    f"{where}: block key {block.key!r} is used twice"
                )
            seen.add(("block", block.key))
        for index, value in enumerate(values):
            where = f"{label}: value {index}"
            if not isinstance(value, DocumentValue):
                raise DocumentVersionError(f"{where} is not a DocumentValue")
            _text(f"{where}: key", value.key, required=True)
            _text(f"{where}: name", value.name, required=True)
            _text(f"{where}: unit", value.unit)
            _text(f"{where}: context", value.context)
            canonical_json(value.payload, f"{where}: payload")
            if ("value", value.key) in seen:
                raise DocumentVersionDuplicate(
                    f"{where}: value key {value.key!r} is used twice"
                )
            seen.add(("value", value.key))
        for index, annotation in enumerate(annotations):
            where = f"{label}: annotation {index}"
            if not isinstance(annotation, BlockAnnotation):
                raise DocumentVersionError(f"{where} is not a BlockAnnotation")
            _text(f"{where}: kind", annotation.kind, required=True)
            _text(f"{where}: key", annotation.key, required=True)
            canonical_json(annotation.payload, f"{where}: payload")
            token = ("annotation", annotation.kind, annotation.key)
            if token in seen:
                raise DocumentVersionDuplicate(
                    f"{where}: annotation ({annotation.kind!r}, "
                    f"{annotation.key!r}) is used twice"
                )
            seen.add(token)

    def _groups(self) -> dict[tuple[str, str], list[_Prepared]]:
        """Admitted versions per (entity, family), oldest first."""
        groups: dict[tuple[str, str], list[_Prepared]] = {}
        for prepared in self._prepared.values():
            if not prepared.version.admitted:
                continue
            key = (prepared.version.entity, prepared.version.family)
            groups.setdefault(key, []).append(prepared)
        for members in groups.values():
            members.sort(key=lambda p: (p.issued_at, p.version.version))
        return groups

    def latest(self) -> dict[tuple[str, str], str]:
        """The latest version's node id for each (entity, family)."""
        return {
            key: members[-1].node_id for key, members in self._groups().items()
        }

    def facts(self) -> Iterator[Union[NodeFact, EdgeFact]]:
        """Every node and edge fact for the versions added so far."""
        groups = self._groups()
        latest_ids = {members[-1].node_id for members in groups.values()}
        out: list[Union[NodeFact, EdgeFact]] = []
        for node_id in sorted(self._prepared):
            prepared = self._prepared[node_id]
            is_latest = "true" if node_id in latest_ids else "false"
            out.extend(self._version_facts(prepared, is_latest))
        for key in sorted(groups):
            members = groups[key]
            for older, newer in zip(members, members[1:]):
                out.append(
                    EdgeFact(
                        src=newer.node_id,
                        dst=older.node_id,
                        edge_type="supersedes",
                        source_record_id={
                            "document_version": newer.node_id,
                            "edge": "supersedes:" + older.node_id,
                        },
                    )
                )
        return iter(out)

    def record_set(self) -> RecordSet:
        """One record per version: its nodes and the edges it owns.

        The record key is the version's node id. A version owns the edges it
        starts (`contains`, `amends`, `supersedes`) and the `annotates` edges
        of its annotations. A source returns this beside `facts()` so the
        runner retracts a node or edge that a later run no longer emits.
        """
        grouped: dict[str, list[Union[NodeFact, EdgeFact]]] = {}
        for fact in self.facts():
            grouped.setdefault(fact.source_record_id["document_version"], []).append(fact)
        records: dict[str, RecordEmission] = {}
        for owner in sorted(grouped):
            nodes = [f for f in grouped[owner] if isinstance(f, NodeFact)]
            edges = [f for f in grouped[owner] if isinstance(f, EdgeFact)]
            records[owner] = RecordEmission(
                primary=next(
                    _record_node(n) for n in nodes if n.node_id == owner
                ),
                descendants=[_record_node(n) for n in nodes if n.node_id != owner],
                edges=[
                    EdgeKey(
                        src=e.src,
                        dst=e.dst,
                        edge_type=e.edge_type,
                        attrs=e.attrs,
                        provenance=e.provenance,
                        confidence=e.confidence,
                        source_record_id=e.source_record_id,
                        source_revision=e.source_revision,
                    )
                    for e in edges
                ],
            )
        return RecordSet(records=records)

    def _identity(self, prepared: _Prepared, is_latest: str) -> dict[str, str]:
        version = prepared.version
        return {
            "entity": version.entity,
            "family": version.family,
            "version": version.version,
            "issued_at": prepared.issued_at,
            "covers": version.covers,
            "doc_kind": version.doc_kind,
            "is_latest": is_latest,
        }

    def _node(
        self,
        node_id: str,
        subtype: str,
        attrs: dict[str, Any],
        owner: str,
    ) -> NodeFact:
        digest = hashlib.sha256(canonical_json(attrs).encode("utf-8")).hexdigest()
        record: dict[str, Any] = {"document_version": owner}
        if node_id != owner:
            record["part"] = node_id
        return NodeFact(
            node_id=node_id,
            subtype=subtype,
            attrs=attrs,
            source_record_id=record,
            source_revision={"attrs_sha256": digest},
        )

    def _version_facts(
        self, prepared: _Prepared, is_latest: str
    ) -> Iterator[Union[NodeFact, EdgeFact]]:
        version = prepared.version
        owner = prepared.node_id
        identity = self._identity(prepared, is_latest)
        head = dict(identity)
        head.update(
            {
                "admitted": "true" if version.admitted else "false",
                "refusal": version.refusal,
                "title": version.title,
                "raw_sha256": version.raw_sha256,
                "reader_ref": version.reader_ref,
            }
        )
        yield self._node(owner, SUBTYPE_VERSION, head, owner)
        prefix = ":".join(
            (_escape(self._deployment), _escape(version.entity),
             _escape(version.version))
        )
        for position, block in enumerate(prepared.blocks):
            attrs = dict(identity)
            attrs.update(
                {
                    "block_key": block.key,
                    "block_kind": block.kind,
                    "text": block.text,
                    "payload": canonical_json(block.payload),
                    "position": position,
                }
            )
            block_id = f"docblock:{prefix}:{position}"
            yield self._node(block_id, SUBTYPE_BLOCK, attrs, owner)
            yield EdgeFact(
                src=owner,
                dst=block_id,
                edge_type="contains",
                source_record_id={"document_version": owner, "part": block_id},
            )
        for ordinal, value in enumerate(prepared.values):
            attrs = dict(identity)
            attrs.update(
                {
                    "value_key": value.key,
                    "name": value.name,
                    "unit": value.unit,
                    "context": value.context,
                    "payload": canonical_json(value.payload),
                }
            )
            value_id = f"docval:{prefix}:{ordinal}"
            yield self._node(value_id, SUBTYPE_VALUE, attrs, owner)
            yield EdgeFact(
                src=owner,
                dst=value_id,
                edge_type="contains",
                source_record_id={"document_version": owner, "part": value_id},
            )
        for annotation in prepared.annotations:
            attrs = dict(identity)
            attrs.update(
                {
                    "annotation_key": annotation.key,
                    "annotation_kind": annotation.kind,
                    "payload": canonical_json(annotation.payload),
                }
            )
            annotation_id = "blockann:{}:{}:{}".format(
                prefix, _escape(annotation.kind), _escape(annotation.key)
            )
            yield self._node(annotation_id, SUBTYPE_ANNOTATION, attrs, owner)
            yield EdgeFact(
                src=annotation_id,
                dst=owner,
                edge_type="annotates",
                source_record_id={"document_version": owner, "part": annotation_id},
            )
        if version.amends:
            target = self.version_id(version.entity, version.amends)
            if target == owner:
                raise DocumentVersionReferenceMissing(
                    f"version {version.entity}/{version.version} amends itself"
                )
            if target not in self._prepared:
                raise DocumentVersionReferenceMissing(
                    f"version {version.entity}/{version.version} amends "
                    f"{version.amends!r}, which was never added; an edge to a "
                    "node that does not exist would be refused at publish"
                )
            yield EdgeFact(
                src=owner,
                dst=target,
                edge_type="amends",
                source_record_id={
                    "document_version": owner,
                    "edge": "amends:" + target,
                },
            )
