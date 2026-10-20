"""The document each node's vector embeds, and the hashes that key it.

PACT openrouter-embeddings-hnsw (design v3.2). A vector embeds a
*document*: one ``Label: value`` line per header field the node has, a blank
line, then the body (the node's ``text``), the whole cut at the set's
``max_chars``. The format is fixed and versioned
(:data:`~okg.substrate.embeddings.config.DOCUMENT_FORMAT_VERSION`).

Hashes:

* ``doc_hash`` -- sha256 of the built (cut) document. Vectors are keyed by
  ``(set_id, doc_hash)``, so identical documents embed once and an edit that
  leaves the document unchanged keeps its vector.
* ``template_hash`` -- one per (set, subtype): the format version, the
  subtype's template and the read class of each header field. A template
  change or a reclassification makes a new hash. What each hash was built
  from is stored with the links (``okg.embedding_templates``), so a read
  keeps serving a node's older link while its template is still safe under
  today's classification (:func:`record_still_safe`), and stops the moment a
  header field it shows becomes maskable.
* ``row_hash`` -- sha256 of ``subtype || attrs::text``, computed by
  PostgreSQL (``embeddings.store.row_hash_expression``, the one definition
  writer and reader share), so a link names node *content*, never a
  Chronos row position, which an in-place rewrite or a segment split moves.

Header safety (Jason, 2026-10-14, "headersafe A"): a header field is shown
only if today's read path shows it to every reader, i.e. the subtype's
classifier (declared ``pii_classes``, or the interim legacy classifier for an
unannotated subtype; ``nomos/read_masking.py``) resolves it to ``non_pii``.
A field that does not pass is left out of the document and reported; an
explicit per-type template that names one is refused (:func:`check_templates`).
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from okg.substrate.embeddings.config import (
    DOCUMENT_FORMAT_VERSION,
    SUBTYPE_FIELD,
    DocumentTemplate,
    VectorSetConfig,
    header_field_denied,
)
from okg.substrate.errors import SubstrateError


__all__ = [
    "NONBLANK_BODY_REGEX",
    "RenderedDocument",
    "TEXT_VALUE_COLUMN",
    "check_templates",
    "doc_hash",
    "field_label",
    "header_field_shown",
    "nonblank_body_sql",
    "record_hash",
    "record_still_safe",
    "render_document",
    "template_hash",
    "template_record",
    "text_value_sql",
]

_REVEALING_CLASS = "non_pii"

#: A PostgreSQL regular expression that matches a string with at least one
#: character Python's ``str.strip()`` keeps, so ``body ~ NONBLANK_BODY_REGEX``
#: is :func:`render_document`'s ``body.strip()`` test. Built from
#: ``str.isspace`` (every such character is in the Basic Multilingual Plane),
#: so the database and the renderer never disagree on what is blank. Each
#: character is written as a PostgreSQL ``\uXXXX`` escape, so the bound
#: pattern is pure ASCII and encodes under any client encoding (a raw
#: U+3000 raises UnicodeEncodeError on a LATIN1 or SQL_ASCII connection).
NONBLANK_BODY_REGEX = "[^" + "".join(
    f"\\u{code:04x}" for code in range(0x10000) if chr(code).isspace()
) + "]"


#: The column :func:`text_value_sql` outputs and :func:`nonblank_body_sql`
#: tests.
TEXT_VALUE_COLUMN = "okg_text"


def text_value_sql(alias: str) -> str:
    """Select-list item: node row ``alias``'s ``text`` value as jsonb, named
    :data:`TEXT_VALUE_COLUMN`. Put it in a subquery kept by ``OFFSET 0`` and
    apply :func:`nonblank_body_sql` one level up, so ``attrs`` is read for
    the text once per row; flattened, the planner repeats the extraction in
    each test (C4: twice the buffers of the count without the test)."""
    if not alias.isidentifier():
        raise ValueError(f"bad SQL alias {alias!r}")
    return f"{alias}.attrs -> 'text' AS {TEXT_VALUE_COLUMN}"


def nonblank_body_sql(alias: str) -> str:
    """SQL true when row ``alias`` (a subquery with :func:`text_value_sql`'s
    column) has a document body the renderer accepts: a JSON string
    ``text`` with a non-blank character. ``#>> '{}'`` of a JSON string is
    the string, as ``->> 'text'`` is. Binds the parameter
    ``%(nonblank_body)s`` (:data:`NONBLANK_BODY_REGEX`)."""
    if not alias.isidentifier():
        raise ValueError(f"bad SQL alias {alias!r}")
    value = f"{alias}.{TEXT_VALUE_COLUMN}"
    return (
        f"jsonb_typeof({value}) = 'string' "
        f"AND ({value} #>> '{{}}') ~ %(nonblank_body)s"
    )


def field_label(name: str) -> str:
    """``component_name`` -> ``Component name``; ``subtype`` -> ``Type``."""
    if name == SUBTYPE_FIELD:
        return "Type"
    words = name.replace("_", " ").strip()
    return words[:1].upper() + words[1:]


def _value_text(value: Any) -> str | None:
    """One line of text for a header value, or None when there is none."""
    if value is None:
        return None
    if isinstance(value, str):
        text = " ".join(value.split())
    elif isinstance(value, bool):
        text = "true" if value else "false"
    elif isinstance(value, (int, float)):
        text = json.dumps(value)
    elif isinstance(value, list) and all(
        isinstance(item, (str, int, float, bool)) and item is not None
        for item in value
    ):
        parts = [_value_text(item) for item in value]
        text = ", ".join(part for part in parts if part)
    else:
        text = json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False)
    return text or None


def _resolved_class(classification: Mapping[str, Any] | None, attr: str) -> str:
    """The class today's read resolves for ``attr``; the same composition
    as ``catalog/modules_compose.py:_read_pii_class``."""
    from okg.substrate.nomos.pii_classification import resolve_pii_class
    from okg.substrate.nomos.read_masking import subtype_attr_classifier

    declared = classification if isinstance(classification, Mapping) else None
    return resolve_pii_class(subtype_attr_classifier(declared)(attr))


def header_field_shown(
    classification: Mapping[str, Any] | None, attr: str,
) -> bool:
    """Whether every reader sees ``attr`` today (headersafe rule A).

    ``classification`` is the subtype's declared ``pii_classes`` map, or None
    for an unannotated subtype (the interim legacy classifier then decides).
    """
    if attr == SUBTYPE_FIELD:
        return True
    if header_field_denied(attr) is not None:
        return False
    return _resolved_class(classification, attr) == _REVEALING_CLASS


def _classification_marker(
    template: DocumentTemplate, classification: Mapping[str, Any] | None,
) -> dict[str, Any]:
    from okg.substrate.nomos.read_masking import LEGACY_REDACTION_KEYS

    marker: dict[str, Any] = {
        "mode": "legacy" if classification is None else "declared",
        "classes": {
            attr: _resolved_class(classification, attr)
            for attr in template.header
            if attr != SUBTYPE_FIELD
        },
    }
    if classification is None:
        # The interim classifier's own key list: a change there reclassifies.
        marker["legacy_keys"] = sorted(LEGACY_REDACTION_KEYS)
    return marker


def template_record(
    vector_set: VectorSetConfig,
    subtype: str,
    classification: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """What a template hash is the hash of: the format version, the set, the
    subtype, its template and the read class of each header field. Stored
    with the hash (``okg.embedding_templates``) so a reader can later decide
    whether documents built under it are still safe to serve."""
    template = vector_set.template_for(subtype)
    return {
        "document_format_version": DOCUMENT_FORMAT_VERSION,
        "set_id": vector_set.set_id,
        "subtype": subtype,
        "template": template.as_json(),
        "classification": _classification_marker(template, classification),
    }


def record_hash(record: Mapping[str, Any]) -> str:
    """The template hash of a :func:`template_record` (hex sha256)."""
    encoded = json.dumps(
        record, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def template_hash(
    vector_set: VectorSetConfig,
    subtype: str,
    classification: Mapping[str, Any] | None,
) -> str:
    """The template identity for one subtype of one set (hex sha256)."""
    return record_hash(template_record(vector_set, subtype, classification))


def record_still_safe(
    record: Mapping[str, Any],
    classifications: Iterable[Mapping[str, Any] | None],
) -> bool:
    """Whether documents built under a stored template record may still be
    served (headersafe rule A, applied again at read time).

    A document holds the header fields its template named *and* that were
    shown when it was built. Which fields those are is decided from the
    stored record alone (its classification marker), never from today's
    rules, so a field that today's deny list or classification hides still
    counts as in the document. It stays servable only while every reader
    still sees each of them (:func:`header_field_shown`, deny list included)
    under every classification given (the pinned catalog's and the latest
    one's). A field that has become maskable or denied since fails closed. A
    record of another document format version, or one that cannot be read,
    is never safe.
    """
    try:
        if record.get("document_format_version") != DOCUMENT_FORMAT_VERSION:
            return False
        header = [str(field) for field in record["template"]["header"]]
        classes = dict(record["classification"]["classes"])
    except (KeyError, TypeError, ValueError):
        return False
    included = [
        field for field in header
        if field == SUBTYPE_FIELD or classes.get(field) == _REVEALING_CLASS
    ]
    return all(
        header_field_shown(classification, field)
        for classification in classifications
        for field in included
    )


def doc_hash(document: str) -> str:
    return hashlib.sha256(document.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RenderedDocument:
    """A built document and what its build left out."""

    text: str
    doc_hash: str
    template_hash: str
    omitted_fields: tuple[str, ...]
    #: What ``template_hash`` hashes (:func:`template_record`); stored with
    #: the links so a later read can re-check the template's header safety.
    template_record: Mapping[str, Any] | None = None


def render_document(
    vector_set: VectorSetConfig,
    *,
    subtype: str,
    attrs: Mapping[str, Any],
    classification: Mapping[str, Any] | None,
) -> RenderedDocument | None:
    """Build the node's document, or None when the set does not cover it.

    A node is covered when the set takes its subtype and its body is a
    non-empty string. Header fields the node lacks are skipped; fields a
    reader could not see (:func:`header_field_shown`) are left out and named
    in ``omitted_fields``.
    """
    if not vector_set.covers(subtype):
        return None
    template = vector_set.template_for(subtype)
    body = attrs.get(template.body)
    if not isinstance(body, str) or not body.strip():
        return None
    lines: list[str] = []
    omitted: list[str] = []
    for field in template.header:
        if not header_field_shown(classification, field):
            omitted.append(field)
            continue
        value = subtype if field == SUBTYPE_FIELD else attrs.get(field)
        text = _value_text(value)
        if text is None:
            continue
        lines.append(f"{field_label(field)}: {text}")
    document = ("\n".join(lines) + "\n\n" + body) if lines else body
    document = document[: vector_set.max_chars]
    record = template_record(vector_set, subtype, classification)
    return RenderedDocument(
        text=document,
        doc_hash=doc_hash(document),
        template_hash=record_hash(record),
        omitted_fields=tuple(omitted),
        template_record=record,
    )


def check_templates(
    vector_set: VectorSetConfig,
    classifications: Mapping[str, Mapping[str, Any] | None],
    subtypes: Iterable[str],
) -> dict[str, list[str]]:
    """Check a set's templates against the catalog's read classification.

    ``classifications`` maps each annotated subtype to its declared
    ``pii_classes``; a subtype absent from it is unannotated. Refuses a
    per-type template that names a field some reader cannot see. Returns, per
    subtype, the default-template fields its documents will leave out (the
    default covers subtypes added later, so it is not refused).
    """
    refused: dict[str, list[str]] = {}
    for subtype, template in vector_set.type_templates:
        classification = classifications.get(subtype)
        hidden = [
            field for field in template.header
            if not header_field_shown(classification, field)
        ]
        if hidden:
            refused[subtype] = hidden
    if refused:
        err = SubstrateError(
            f"vector set {vector_set.name!r} has header fields that some "
            "readers cannot see",
            data={"set": vector_set.name, "fields_by_subtype": refused},
            fix_hint=(
                "remove the fields from the set's document.types header, or "
                "declare them non_pii in the subtype's pii_classes"
            ),
        )
        err.code = "embedding_header_field_masked"
        raise err
    omitted: dict[str, list[str]] = {}
    explicit = {name for name, _ in vector_set.type_templates}
    for subtype in sorted(set(subtypes)):
        if subtype in explicit or not vector_set.covers(subtype):
            continue
        classification = classifications.get(subtype)
        hidden = [
            field for field in vector_set.default_template.header
            if not header_field_shown(classification, field)
        ]
        if hidden:
            omitted[subtype] = hidden
    return omitted
