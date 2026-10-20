"""Tests for the Presidio free-text PII detector adapter.

The entity-type -> pii_class mapping is tested purely (no engine). The live
engine + its plumbing into the at-rest transform run only when Presidio and its
NLP model are actually loadable, and skip cleanly otherwise (the model download
is egress-gated).
"""
from __future__ import annotations

from dataclasses import dataclass

import pytest

from okg.substrate.ingest.pii_sanitize import sanitize_fact_pii
from okg.substrate.ingest.presidio_detector import (
    PRESIDIO_ENTITY_PII_CLASS,
    PresidioFreeTextDetector,
    maybe_build_presidio_detector,
    presidio_results_to_spans,
)
from okg.substrate.nomos.pii_classification import FreeTextPiiDetector


@dataclass
class _Result:
    entity_type: str
    start: int
    end: int
    score: float = 0.9


@dataclass
class _Fact:
    node_id: str
    attrs: dict


def test_mapping_covers_known_entities_and_defaults_unknown_to_sensitive() -> None:
    results = [
        _Result("EMAIL_ADDRESS", 0, 5),
        _Result("LOCATION", 6, 10),
        _Result("WEIRD_CUSTOM_ENTITY", 11, 15),
        _Result("ZERO_WIDTH", 20, 20),  # dropped
    ]
    spans = presidio_results_to_spans(results)
    assert [(s.start, s.end, s.pii_class) for s in spans] == [
        (0, 5, "direct_identifier"),
        (6, 10, "quasi_identifier"),
        (11, 15, "sensitive_attribute"),  # unknown entity still masks
    ]
    assert all(s.detector.startswith("presidio:") for s in spans)


def test_every_mapped_class_is_a_real_pii_class() -> None:
    from okg.substrate.nomos import PII_CLASSES

    assert set(PRESIDIO_ENTITY_PII_CLASS.values()) <= set(PII_CLASSES)


def test_detector_satisfies_the_protocol_with_a_fake_engine() -> None:
    class _FakeEngine:
        def analyze(self, text: str, language: str) -> list[_Result]:
            return [_Result("PERSON", 0, len(text))]

    detector = PresidioFreeTextDetector(_FakeEngine())
    assert isinstance(detector, FreeTextPiiDetector)
    spans = detector.detect("Alice")
    assert spans and spans[0].pii_class == "direct_identifier"
    assert detector.detect("") == []


# --- live engine (skips when the model isn't installed) ---
# Marked `slow`: building Presidio's NLP model (spaCy / GLiNER) costs minutes, so
# these are excluded from the budgeted `fast` lane and run in the slow lane when
# the `privacy` extra is installed. The engine-free tests above stay in `fast`.


@pytest.fixture(scope="module")
def live_detector():
    detector = maybe_build_presidio_detector()
    if detector is None:
        pytest.skip("Presidio NLP model not available (egress-gated)")
    return detector


@pytest.mark.slow
def test_live_presidio_detects_email_and_person(live_detector) -> None:
    spans = live_detector.detect("Contact Alice McCarthy at alice@example.com")
    classes = {s.pii_class for s in spans}
    assert "direct_identifier" in classes  # email and/or person detected


@pytest.mark.slow
def test_live_detector_masks_free_text_through_at_rest_transform(live_detector) -> None:
    fact = _Fact("n1", {"body": "Reach Alice McCarthy at alice@example.com"})
    out = sanitize_fact_pii(
        fact,
        classifier=lambda k: "free_text" if k == "body" else None,
        store_raw=False,
        detector=live_detector,
    )
    assert not out.held
    assert "alice@example.com" not in out.fact.attrs["body"]


def test_offline_email_recognizer_preserves_defaults_and_disables_fetch():
    from types import SimpleNamespace
    from okg.substrate.ingest.presidio_detector import _offline_email_recognizer
    received = {}
    class Recognizer:
        patterns = ("inherited-pattern",)
        context = ("email",)
        def __init__(self, **kwargs): self.options = kwargs
    def extractor_factory(**kwargs):
        received.update(kwargs)
        return lambda value: SimpleNamespace(fqdn="example.org" if value == "alice@example.org" else "")
    recognizer = _offline_email_recognizer(Recognizer, extractor_factory, language="en")
    assert received == {"cache_dir": None, "suffix_list_urls": (), "fallback_to_snapshot": True}
    assert recognizer.patterns == Recognizer.patterns and recognizer.context == Recognizer.context
    assert recognizer.options == {"supported_language": "en", "name": "EmailRecognizer"}
    assert recognizer.validate_result("alice@example.org") is True
    assert recognizer.validate_result("invalid") is False
