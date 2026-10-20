"""Classification-driven read masking.

Replaces the hardcoded ``_redacted_attrs`` key blocklist (which a newly-added PII
attribute silently survived) with a transform keyed on each attribute's
``pii_class`` (see :mod:`okg.substrate.nomos.pii_classification`):

* ``non_pii`` -> revealed;
* ``direct_identifier`` / ``quasi_identifier`` -> pseudonymized when a vault key
  is available (so the masked projection stays join-consistent for the reader),
  otherwise masked;
* every other class, and every UNCLASSIFIED attribute -> masked. The
  fail-safe is load-bearing: an attribute with no resolved class masks, it is
  never revealed.

:func:`mask_attrs` subsumes the legacy blocklist (see
:data:`LEGACY_REDACTION_KEYS`) and IS what the read filter's ``redact`` effect
applies live: the read filter classifies a node row by its subtype's declared
``pii_class`` (:func:`subtype_attr_classifier`, falling back to
:func:`legacy_attr_classifier` for a subtype the ontology has not annotated),
while the ``store`` export path still calls it with
:func:`legacy_attr_classifier` + :data:`READ_DROP_CLASSES` -- so the two
duplicated ``_redacted_attrs`` blocklists are retired into this one path.
:func:`safe_decision_payload` is what the read filter hands the decision sink --
attribute KEYS only, never raw values -- so raw PII does not reach decision
machinery. The at-rest transform (``ingest.pii_sanitize``) also composes this
function; binding it automatically into the source runner is the remaining live
step.
"""
from __future__ import annotations

from typing import Any, Callable, Mapping, Optional

from okg.substrate.nomos.pii_classification import FAILSAFE_PII_CLASS, resolve_pii_class
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR
from okg.substrate.searchable_text import PRODUCER_TEXT_ATTR

__all__ = [
    "MASK_TOKEN",
    "REDACTION_MARKER",
    "READ_DROP_CLASSES",
    "GOVERNANCE_KEYS",
    "LEGACY_REDACTION_KEYS",
    "REVEAL_CLASSES",
    "PSEUDONYMIZE_CLASSES",
    "AttrClassifier",
    "ReadRedactedAttrs",
    "is_governance_key",
    "mask_attrs",
    "legacy_attr_classifier",
    "subtype_attr_classifier",
    "safe_decision_payload",
]

#: Placeholder stamped in place of a masked value. Distinctive and non-empty so a
#: reader can see the attribute exists but its value is withheld.
MASK_TOKEN = "[nomos:masked]"

#: Structural / governance attribute keys that MUST NOT be masked. Masking these
#: would corrupt the metadata policy decisions key on (``classification``) or the
#: lineage the read filter routes on (a fail-open hazard), so they pass through
#: ahead of any pii_class resolution. This is an EXPLICIT allow-list, NOT a
#: ``nomos_*`` prefix: a broad prefix would let a source-authored attr like
#: ``nomos_email`` bypass masking, and once attributes carry a real pii_class it
#: would also override a declared identifier class. ``nomos_redacted`` is the
#: transform's own idempotency marker.
GOVERNANCE_KEYS: frozenset[str] = frozenset(
    {"classification", LINEAGE_ATTR, "nomos_redacted"}
)


def is_governance_key(key: str) -> bool:
    """Whether an attribute key is structural governance metadata (never masked)."""
    return key in GOVERNANCE_KEYS

#: The keys the retired ``_redacted_attrs`` blocklist used to drop. Retained here
#: so :func:`legacy_attr_classifier` reproduces (and :func:`mask_attrs` provably
#: subsumes) the old behavior while adding classification for everything else.
LEGACY_REDACTION_KEYS: frozenset[str] = frozenset({
    "body",
    "content",
    "evidence",
    # The searchable-text stamper's copy of a producer's `text` body (issue
    # #2991): the same content as `text`, so it is withheld whenever `text` is.
    PRODUCER_TEXT_ATTR,
    "private_body",
    "provenance",
    "raw",
    "raw_body",
    "text",
})

#: Classes revealed by default. Only ``non_pii``.
REVEAL_CLASSES: frozenset[str] = frozenset({"non_pii"})

#: Classes pseudonymized (rather than blanked) when a vault key is available, so
#: joins survive the masked projection.
PSEUDONYMIZE_CLASSES: frozenset[str] = frozenset(
    {"direct_identifier", "quasi_identifier"}
)

#: Marker stamped on a transformed attribute map. The canonical marker the Poros
#: outbox, operator surfaces, and existing tests consume for "privacy transform
#: applied to this row".
REDACTION_MARKER = "nomos_redacted"

#: Classes DROPPED (key removed) rather than mask-tokened, on the read/export
#: redact path -- so retiring the ``_redacted_attrs`` blocklist into ``mask_attrs``
#: preserves the prior drop-the-key behavior for free-text content while adding
#: pseudonymization for identifier classes.
READ_DROP_CLASSES: frozenset[str] = frozenset({"free_text"})

#: A classifier maps an attribute key to its declared ``pii_class`` (or ``None``
#: when the attribute is unclassified -- which fails safe to masked).
AttrClassifier = Callable[[str], Optional[str]]


class ReadRedactedAttrs(dict[str, Any]):
    """In-memory proof that the read filter, not ingest, produced this mask.

    The public ``nomos_redacted`` attribute is intentionally persisted by the
    at-rest sanitizer and can also arrive from a source. It therefore cannot
    serve as a trusted terminal-state signal for downstream overlay helpers.
    Instances of this type are created only after a read-time redact decision;
    JSON/JSONB round trips always return an ordinary dict.
    """


class _Pseudonymizer:  # pragma: no cover - structural typing only
    def pseudonym(self, *, canonical: str, entity_type: Optional[str] = None) -> str:
        ...


def legacy_attr_classifier(key: str) -> Optional[str]:
    """Behavior-preserving classifier for the interim, pre-ontology-population path.

    Classifies the legacy blocklist keys as ``free_text`` (so they mask, as
    before) and everything else as ``non_pii`` (so it is revealed, as before).
    That non_pii default is deliberately fail-OPEN -- it is NOT the fail-safe
    target -- and is retained ONLY so the read filter's live behavior does not
    change until the ontology declares ``pii_class`` per attribute and the
    two-principal live gate flips the cutover to a real classifier.
    """
    return "free_text" if key in LEGACY_REDACTION_KEYS else "non_pii"


def subtype_attr_classifier(
    subtype_pii_classes: Optional[Mapping[str, Any]],
) -> AttrClassifier:
    """A read classifier from a subtype's declared ``attr -> pii_class`` map
    (``okg.subtypes.metadata['pii_classes']``).

    A subtype with a PRESENT declaration (including an empty map) classifies each
    attribute by its declared class and fails SAFE for an attribute the map does
    not list (``None`` ->
    :func:`resolve_pii_class` -> ``sensitive_attribute`` -> mask), never revealing
    it. Only an UNANNOTATED subtype (``None``) falls back to
    :func:`legacy_attr_classifier`, preserving today's behavior for subtypes the
    ontology has not yet annotated -- so this is backward-compatible until a
    subtype opts in by declaring ``pii_classes``. An explicit empty mapping is
    a fail-safe opt-in and masks every attribute.
    """
    if subtype_pii_classes is None:
        return legacy_attr_classifier
    declared = {str(key): str(value) for key, value in subtype_pii_classes.items()}

    def classify(key: str) -> Optional[str]:
        # The stamper's copy of a `text` body (issue #2991) is `text`'s own
        # content, so unless the map lists it, it takes `text`'s class --
        # dropped where `text` is dropped, masked where `text` is unlisted.
        if key == PRODUCER_TEXT_ATTR and key not in declared:
            return declared.get("text")
        return declared.get(key)

    return classify


def mask_attrs(
    attrs: Mapping[str, Any],
    *,
    classifier: AttrClassifier,
    pseudonym_service: Optional[_Pseudonymizer] = None,
    reveal: frozenset[str] = REVEAL_CLASSES,
    drop_classes: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Transform an attribute map per ``pii_class``, failing safe.

    Reveals ``reveal`` classes, DROPS (removes) attributes whose class is in
    ``drop_classes``, pseudonymizes identifier classes when a vault is available
    (else mask-tokens them), and mask-tokens everything else -- including any
    attribute the ``classifier`` cannot classify (``None`` resolves to
    ``sensitive_attribute`` and masks). Governance keys are never touched. Stamps
    :data:`REDACTION_MARKER` when anything was withheld.
    """
    if FAILSAFE_PII_CLASS in reveal:
        # Revealing the fail-safe class would defeat the whole invariant:
        # unclassified data resolves to it, so it must never be a reveal class.
        raise ValueError(
            f"{FAILSAFE_PII_CLASS!r} must not be a reveal class; it is where "
            "unclassified attributes fail safe"
        )
    out: dict[str, Any] = {}
    withheld_any = False
    for key, value in dict(attrs).items():
        if is_governance_key(key):
            # Structural metadata (classification, lineage, nomos_* markers) is
            # never masked: policy decisions and the read filter key on it, so
            # masking it would fail open. It is not a PII value.
            out[key] = value
            continue
        pii_class = resolve_pii_class(classifier(key))
        if pii_class in reveal:
            out[key] = value
            continue
        withheld_any = True
        if pii_class in drop_classes:
            continue  # withhold by removing the key entirely
        if (
            pii_class in PSEUDONYMIZE_CLASSES
            and pseudonym_service is not None
            and isinstance(value, str)
            and value
        ):
            out[key] = pseudonym_service.pseudonym(
                canonical=value, entity_type=pii_class
            )
        else:
            out[key] = MASK_TOKEN
    if withheld_any:
        out[REDACTION_MARKER] = True
    return out


def safe_decision_payload(attrs: Mapping[str, Any]) -> dict[str, Any]:
    """A raw-free summary of an attribute map for the decision sink.

    The decision trace must record WHAT was evaluated without persisting raw
    values (which may be PII). Returns the attribute keys and their count only.
    """
    return {"attr_keys": sorted(str(key) for key in dict(attrs)), "attr_count": len(dict(attrs))}
