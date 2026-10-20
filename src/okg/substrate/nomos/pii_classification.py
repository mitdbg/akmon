"""Fail-safe privacy classification for graph attributes.

A graph attribute's ``pii_class`` (see :data:`okg.substrate.nomos.vocabulary.PII_CLASSES`)
decides how its value is transformed at rest and at read. Two rules are
load-bearing:

* **Fail-safe.** An attribute that carries a value but has no resolved
  ``pii_class`` -- unclassified, blank, or an unknown term -- resolves to
  :data:`FAILSAFE_PII_CLASS` (``sensitive_attribute``: masked at read,
  not-store-raw at ingest). It is NEVER treated as ``non_pii``. Defaulting
  unclassified data to ``non_pii`` (or to the Nomos ``"internal"``
  data-classification) is a fail-open bug.
* **Only ``non_pii`` reveals.** Every other class masks by default; the read and
  at-rest stages key their transforms off :func:`pii_class_masks`.

Free-text detection is pluggable: this module owns only the *interface*
(:class:`FreeTextPiiDetector`) and a process-global registry. The concrete
detector (Presidio analyzer + a GLiNER2-PII recognizer) is installed by the
at-rest slice as an optional extra, so this foundation carries no heavy NLP
dependency.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable

from okg.substrate.nomos.vocabulary import PII_CLASSES

#: The class an unclassified/unknown attribute fails safe to. A real member of
#: ``PII_CLASSES`` (not a sentinel), so downstream transforms need no special
#: case -- it simply masks like any other sensitive class.
FAILSAFE_PII_CLASS = "sensitive_attribute"

#: The only class that reveals by default.
REVEALING_PII_CLASS = "non_pii"


def resolve_pii_class(declared: Optional[str]) -> str:
    """Resolve an attribute's effective ``pii_class``, failing SAFE.

    ``declared`` is the class an ontology attribute (or a detector) asserts, or
    ``None`` when the attribute is unclassified. A valid declared class is
    returned unchanged; ``None``, blank, or an unknown term resolves to
    :data:`FAILSAFE_PII_CLASS` -- never ``non_pii``.
    """
    if isinstance(declared, str):
        candidate = declared.strip()
        if candidate in PII_CLASSES:
            return candidate
    # Unclassified, blank, or drift: fail safe. Never reveal.
    return FAILSAFE_PII_CLASS


def pii_class_masks(pii_class: Optional[str]) -> bool:
    """Whether an attribute of this class masks by default.

    Everything except ``non_pii`` masks. ``None``/unknown resolves through the
    fail-safe path first, so it masks too.
    """
    return resolve_pii_class(pii_class) != REVEALING_PII_CLASS


@dataclass(frozen=True)
class PiiSpan:
    """One detected personally-identifying span in free text."""

    start: int
    end: int
    pii_class: str
    detector: str
    score: float = 1.0


@runtime_checkable
class FreeTextPiiDetector(Protocol):
    """Pluggable detector for PII in unstructured text.

    Implemented by the at-rest slice with Presidio + GLiNER2-PII. The foundation
    ships only this interface plus a registry, so nothing here imports a heavy
    NLP stack.
    """

    def detect(self, text: str) -> list[PiiSpan]:
        ...


_DETECTOR: Optional[FreeTextPiiDetector] = None


def register_free_text_detector(detector: Optional[FreeTextPiiDetector]) -> None:
    """Install (or clear, with ``None``) the process-global free-text detector."""
    global _DETECTOR
    _DETECTOR = detector


def free_text_detector() -> Optional[FreeTextPiiDetector]:
    """Return the installed detector, or ``None`` when the extra is absent.

    A ``None`` return is the signal for the at-rest stage to HOLD (never skip)
    when a not-store-raw source has free text to classify.
    """
    return _DETECTOR
