"""At-rest PII transformation for the shared admission chokepoint.

Extends the NUL-sanitize chokepoint (:mod:`okg.substrate.ingest.sanitize`) with a
classification-driven privacy transform for facts from *not-store-raw* sources.
Structured direct-identifier attributes are pseudonymized via the vault (so joins
survive), other sensitive attributes are masked, and free-text fields require the
detector: if a not-store-raw fact carries free text and the detector is ABSENT,
the fact is HELD (never stored raw) rather than silently skipping detection.

This is the tested transform + hold guard. Binding it into the live
source-runner (``SourceRunner._sanitize_facts``) and enricher fact emitters, the
residual-fact-writer sweep, and proof over a published generation from a
not-store-raw fixture source are staged behind the program's at-rest live gate
(they need a source-posture source, the concrete Presidio+GLiNER detector, and a
publish cycle on an isolated test DB -- never the live okg-workspace graph).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import Any, Mapping, Optional

from okg.substrate.nomos.pii_classification import free_text_detector, resolve_pii_class
from okg.substrate.nomos.read_masking import (
    MASK_TOKEN,
    REDACTION_MARKER,
    AttrClassifier,
    mask_attrs,
)

__all__ = [
    "FREE_TEXT_CLASS",
    "PiiSanitization",
    "attrs_have_free_text",
    "mask_text_spans",
    "sanitize_attrs_pii",
    "sanitize_fact_pii",
    "sanitize_provenance_pii",
]

FREE_TEXT_CLASS = "free_text"
#: Held-reason surfaced (by both chokepoints) when a not-store-raw payload
#: carries free text a detector must classify but no detector is installed.
DETECTOR_UNAVAILABLE = "pii_detector_unavailable"


def sanitize_provenance_pii(
    value: Any,
    *,
    field_name: str,
    pseudonym_service: Optional[Any],
) -> Any:
    """Replace provenance with a stable keyed token before either storage copy."""
    if value is None:
        return None
    if pseudonym_service is None:
        raise ValueError(
            "a not-store-raw fact with source provenance requires a pseudonym vault"
        )
    canonical = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return {
        "nomos_pseudonym": pseudonym_service.pseudonym(
            canonical=f"{field_name}:{canonical}",
            entity_type="direct_identifier",
        )
    }


def mask_text_spans(text: str, spans: Any) -> str:
    """Replace each detected PII span in ``text`` with :data:`MASK_TOKEN`.

    Detected spans are first clamped to the text and UNIONED into disjoint
    intervals, then each interval is replaced once (right-to-left, so offsets
    stay valid). The union is essential: a detector that emits overlapping,
    nested, or touching spans (e.g. GLiNER) would otherwise leave PII characters
    between mis-cut replacements. Generic over any detector that yields objects
    with ``start`` / ``end``.
    """
    intervals = sorted(
        (max(0, int(s.start)), min(len(text), int(s.end)))
        for s in spans
        if int(s.start) < int(s.end)
    )
    merged: list[list[int]] = []
    for start, end in intervals:
        if start >= end:
            continue
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    out = text
    for start, end in reversed(merged):
        out = out[:start] + MASK_TOKEN + out[end:]
    return out


@dataclass(frozen=True)
class PiiSanitization:
    """Outcome of the at-rest PII transform for one fact.

    ``fact is None`` ⇔ HELD: the fact carries free text a detector must classify,
    but no detector is installed -- so it is neither stored raw nor silently
    skipped; the caller holds the source watermark and surfaces ``held_reason``.
    Otherwise ``fact`` is the storable fact (pseudonymized/masked when
    ``transformed``).
    """

    fact: Optional[Any]
    transformed: bool = False
    held_reason: Optional[str] = None

    @property
    def held(self) -> bool:
        return self.fact is None


def attrs_have_free_text(attrs: Mapping[str, Any], classifier: AttrClassifier) -> bool:
    """True when any attribute resolves to the ``free_text`` class."""
    return any(resolve_pii_class(classifier(key)) == FREE_TEXT_CLASS for key in attrs)


def sanitize_attrs_pii(
    attrs: Mapping[str, Any],
    *,
    classifier: AttrClassifier,
    store_raw: bool,
    pseudonym_service: Optional[Any] = None,
    detector: Optional[Any] = None,
) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    """Apply the at-rest privacy transform to one attribute mapping.

    Returns ``(new_attrs, held_reason)``:

    * ``store_raw=True`` -> ``(dict(attrs), None)`` (an independent copy, so the
      caller may store it without aliasing the source object; unchanged content);
    * a not-store-raw payload carrying free text with NO detector installed ->
      ``(None, DETECTOR_UNAVAILABLE)`` (HELD -- the caller must never store it raw);
    * otherwise ``(transformed_attrs, None)`` -- structured (non-free-text)
      attributes go through :func:`mask_attrs` (direct/quasi identifiers
      pseudonymized via the vault so joins survive, other sensitive classes masked,
      non_pii kept), and each free-text attribute has its detected PII spans masked
      in place (:func:`mask_text_spans`), with the redaction marker stamped.

    This is the SHARED attrs-level primitive both write chokepoints call -- the
    source-runner admission pass (via :func:`sanitize_fact_pii`) and the enricher
    fact emitters -- mirroring how :func:`okg.substrate.ingest.sanitize.sanitize_value`
    is the shared NUL primitive both already call.
    """
    if store_raw:
        return dict(attrs), None
    active_detector = detector if detector is not None else free_text_detector()
    free_text_keys = {
        key for key in attrs if resolve_pii_class(classifier(key)) == FREE_TEXT_CLASS
    }
    if free_text_keys and active_detector is None:
        return None, DETECTOR_UNAVAILABLE
    structured = {key: value for key, value in attrs.items() if key not in free_text_keys}
    new_attrs = mask_attrs(
        structured, classifier=classifier, pseudonym_service=pseudonym_service
    )
    for key in free_text_keys:
        value = attrs[key]
        if isinstance(value, str):
            new_attrs[key] = mask_text_spans(value, active_detector.detect(value))
        else:
            new_attrs[key] = MASK_TOKEN
    if free_text_keys:
        new_attrs[REDACTION_MARKER] = True
    return new_attrs, None


def sanitize_fact_pii(
    fact: Any,
    *,
    classifier: AttrClassifier,
    store_raw: bool,
    pseudonym_service: Optional[Any] = None,
    detector: Optional[Any] = None,
) -> PiiSanitization:
    """Apply the at-rest privacy transform to one fact, or HOLD when a detector is needed but absent.

    Thin fact-level wrapper over :func:`sanitize_attrs_pii`: ``store_raw=True``
    returns the fact unchanged; a HELD attrs result (free text + no detector)
    becomes a held ``PiiSanitization`` (``fact=None``); otherwise the fact is
    rebuilt with the transformed attrs.
    """
    if store_raw:
        return PiiSanitization(fact=fact)
    attrs = getattr(fact, "attrs", None) or {}
    new_attrs, held_reason = sanitize_attrs_pii(
        attrs,
        classifier=classifier,
        store_raw=False,
        pseudonym_service=pseudonym_service,
        detector=detector,
    )
    if new_attrs is None:
        return PiiSanitization(fact=None, held_reason=held_reason)
    changes: dict[str, Any] = {"attrs": new_attrs}
    # Production NodeFact/EdgeFact values carry provenance, but small adapters
    # and test doubles are allowed to expose only attrs. Preserve each concrete
    # fact shape instead of adding constructor arguments it does not support.
    for field_name in ("source_record_id", "source_revision"):
        if hasattr(fact, field_name):
            changes[field_name] = sanitize_provenance_pii(
                getattr(fact, field_name),
                field_name=field_name,
                pseudonym_service=pseudonym_service,
            )
    return PiiSanitization(
        fact=replace(fact, **changes),
        transformed=True,
    )
