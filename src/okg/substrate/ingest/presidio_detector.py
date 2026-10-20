"""Concrete free-text PII detector: Presidio analyzer -> pii_class spans.

Implements the :class:`~okg.substrate.nomos.pii_classification.FreeTextPiiDetector`
interface with Presidio (PyPI ``presidio-analyzer``; the project now lives under
the ``data-privacy-stack`` GitHub org). This is the
optional, heavy extra: Presidio + its NLP model are imported lazily inside the
adapter, so importing this module (and the whole ingest package) never pulls the
NLP stack. A not-store-raw source whose free text needs classification while the
extra is absent HOLDS (see :func:`okg.substrate.ingest.pii_sanitize.sanitize_fact_pii`);
it never silently skips detection.

The entity-type -> ``pii_class`` mapping (:func:`presidio_results_to_spans`) is a
pure function, unit-tested without the engine; the engine itself is built by
:func:`build_presidio_detector`, which raises a clear error when Presidio or its
model is unavailable rather than degrading silently.
"""
from __future__ import annotations

from typing import Any, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.nomos.pii_classification import PiiSpan

__all__ = [
    "PRESIDIO_ENTITY_PII_CLASS",
    "PresidioUnavailable",
    "PresidioFreeTextDetector",
    "build_presidio_detector",
    "maybe_build_presidio_detector",
    "presidio_results_to_spans",
]

#: Presidio entity type -> our pii_class. Anything Presidio detects but that is
#: not listed maps to ``sensitive_attribute`` (conservative: it still masks).
PRESIDIO_ENTITY_PII_CLASS: dict[str, str] = {
    "PERSON": "direct_identifier",
    "EMAIL_ADDRESS": "direct_identifier",
    "PHONE_NUMBER": "direct_identifier",
    "CREDIT_CARD": "direct_identifier",
    "US_SSN": "direct_identifier",
    "US_PASSPORT": "direct_identifier",
    "US_DRIVER_LICENSE": "direct_identifier",
    "US_BANK_NUMBER": "direct_identifier",
    "IBAN_CODE": "direct_identifier",
    "CRYPTO": "direct_identifier",
    "IP_ADDRESS": "direct_identifier",
    "MEDICAL_LICENSE": "direct_identifier",
    "LOCATION": "quasi_identifier",
    "DATE_TIME": "quasi_identifier",
    "NRP": "quasi_identifier",
    "URL": "quasi_identifier",
}

_UNKNOWN_ENTITY_PII_CLASS = "sensitive_attribute"


class PresidioUnavailable(SubstrateError):
    """Raised when Presidio or its NLP model cannot be loaded."""

    code = "presidio_unavailable"


def presidio_results_to_spans(results: Any) -> list[PiiSpan]:
    """Map Presidio ``RecognizerResult`` objects to :class:`PiiSpan`.

    Pure and engine-free: each result contributes a span at its ``start``/``end``
    with the mapped ``pii_class`` (unknown Presidio entities map to
    ``sensitive_attribute`` so they still mask). Zero-width or inverted spans are
    dropped.
    """
    spans: list[PiiSpan] = []
    for result in results:
        start = int(getattr(result, "start"))
        end = int(getattr(result, "end"))
        if start >= end:
            continue
        entity = str(getattr(result, "entity_type", "") or "")
        pii_class = PRESIDIO_ENTITY_PII_CLASS.get(entity, _UNKNOWN_ENTITY_PII_CLASS)
        score = float(getattr(result, "score", 1.0) or 0.0)
        spans.append(PiiSpan(start, end, pii_class, f"presidio:{entity}", score))
    return spans


class PresidioFreeTextDetector:
    """FreeTextPiiDetector backed by a Presidio ``AnalyzerEngine``."""

    def __init__(self, engine: Any, *, language: str = "en") -> None:
        self._engine = engine
        self._language = language

    def detect(self, text: str) -> list[PiiSpan]:
        if not text:
            return []
        results = self._engine.analyze(text=text, language=self._language)
        return presidio_results_to_spans(results)


def _offline_email_recognizer(recognizer_class, extractor_factory, *, language: str):
    """Retain Presidio email matching with the pinned bundled suffix list."""
    extractor = extractor_factory(cache_dir=None, suffix_list_urls=(), fallback_to_snapshot=True)

    class OfflineEmailRecognizer(recognizer_class):
        def validate_result(self, pattern_text: str):
            return extractor(pattern_text).fqdn != ""

    return OfflineEmailRecognizer(supported_language=language, name="EmailRecognizer")


def build_presidio_detector(*, language: str = "en") -> PresidioFreeTextDetector:
    """Build a Presidio-backed detector, or raise :class:`PresidioUnavailable`.

    Fails LOUD when the ``presidio-analyzer`` extra or its NLP model is not
    installed, so a caller can decide to HOLD rather than skip detection.
    """
    try:
        from presidio_analyzer import AnalyzerEngine
    except ImportError as exc:  # pragma: no cover - exercised only without extra
        raise PresidioUnavailable(
            "presidio-analyzer is not installed; install the privacy extra"
        ) from exc
    try:
        from presidio_analyzer.predefined_recognizers import EmailRecognizer
        from tldextract import TLDExtract
        engine = AnalyzerEngine()
        engine.registry.remove_recognizer("EmailRecognizer", language=language)
        engine.registry.add_recognizer(_offline_email_recognizer(
            EmailRecognizer, TLDExtract, language=language))
    except Exception as exc:  # noqa: BLE001 - model load surfaces many error types
        raise PresidioUnavailable(
            f"could not initialize the Presidio analyzer (NLP model missing?): {exc}"
        ) from exc
    return PresidioFreeTextDetector(engine, language=language)


def maybe_build_presidio_detector(
    *, language: str = "en"
) -> Optional[PresidioFreeTextDetector]:
    """Best-effort build; returns ``None`` when Presidio/model is unavailable."""
    try:
        return build_presidio_detector(language=language)
    except PresidioUnavailable:
        return None
