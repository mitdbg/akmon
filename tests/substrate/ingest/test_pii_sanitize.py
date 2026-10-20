"""Tests for the at-rest PII transform + detector-hold guard.

Proves a store-raw source is untouched, a not-store-raw source has its
direct-identifier attributes pseudonymized (raw absent) while non_pii is kept,
and a not-store-raw fact carrying free text is HELD when no detector is
installed (never stored raw) but transforms once a detector is present.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict

import pytest

from okg.substrate.alias.pseudonymize import PseudonymService
from okg.substrate.nomos.pii_classification import PiiSpan
from okg.substrate.ingest.pii_sanitize import mask_text_spans, sanitize_fact_pii

_VAULT = PseudonymService(key=b"unit-test-key-for-at-rest-sanitize", epoch="e1")


@dataclass(frozen=True)
class _Fact:
    node_id: str
    attrs: Dict[str, Any] = field(default_factory=dict)


def _classifier(mapping):
    return lambda key: mapping.get(key)


class _WholeTextDetector:
    def detect(self, text: str) -> list[PiiSpan]:
        return [PiiSpan(0, len(text), "direct_identifier", "fake")]


class _SubstringDetector:
    """Detects one known substring, so we can prove SPAN-level masking."""

    def __init__(self, needle: str) -> None:
        self._needle = needle

    def detect(self, text: str) -> list[PiiSpan]:
        start = text.find(self._needle)
        if start < 0:
            return []
        return [PiiSpan(start, start + len(self._needle), "direct_identifier", "fake")]


class _RaisingDetector:
    def detect(self, text: str) -> list[PiiSpan]:
        raise AssertionError("detect() must be invoked on the with-detector path")


def test_store_raw_source_is_untouched() -> None:
    fact = _Fact("n1", {"email": "alice@example.com", "name": "Alice"})
    out = sanitize_fact_pii(
        fact, classifier=_classifier({"email": "direct_identifier"}), store_raw=True
    )
    assert out.fact is fact
    assert not out.transformed
    assert not out.held


def test_not_store_raw_pseudonymizes_identifiers_and_keeps_non_pii() -> None:
    fact = _Fact("n1", {"email": "alice@example.com", "node_kind": "person"})
    out = sanitize_fact_pii(
        fact,
        classifier=_classifier({"email": "direct_identifier", "node_kind": "non_pii"}),
        store_raw=False,
        pseudonym_service=_VAULT,
    )
    assert out.transformed and not out.held
    assert "alice@example.com" not in json.dumps(out.fact.attrs)  # raw not stored
    assert out.fact.attrs["node_kind"] == "person"  # non_pii kept
    assert out.fact.attrs["nomos_redacted"] is True


def test_free_text_without_detector_is_held_not_stored_raw() -> None:
    fact = _Fact("n1", {"body": "Alice McCarthy lives at 12 Elm St"})
    out = sanitize_fact_pii(
        fact,
        classifier=_classifier({"body": "free_text"}),
        store_raw=False,
        pseudonym_service=_VAULT,
        detector=None,
    )
    assert out.held
    assert out.fact is None  # HELD: nothing raw is stored
    assert out.held_reason == "pii_detector_unavailable"


def test_free_text_with_detector_masks_whole_detected_span() -> None:
    fact = _Fact("n1", {"body": "Alice McCarthy lives at 12 Elm St"})
    out = sanitize_fact_pii(
        fact,
        classifier=_classifier({"body": "free_text"}),
        store_raw=False,
        pseudonym_service=_VAULT,
        detector=_WholeTextDetector(),
    )
    assert not out.held and out.transformed
    assert "Alice McCarthy" not in json.dumps(out.fact.attrs)  # detected span masked


def test_free_text_detector_masks_only_detected_span() -> None:
    # Proves the detector is actually invoked and masking is SPAN-level, not
    # whole-field: only the detected needle is withheld; the rest survives.
    fact = _Fact("n1", {"body": "Contact Alice McCarthy about the ticket"})
    out = sanitize_fact_pii(
        fact,
        classifier=_classifier({"body": "free_text"}),
        store_raw=False,
        detector=_SubstringDetector("Alice McCarthy"),
    )
    body = out.fact.attrs["body"]
    assert "Alice McCarthy" not in body
    assert body.startswith("Contact ") and body.endswith(" about the ticket")


def test_mask_text_spans_unions_overlapping_and_nested_spans() -> None:
    text = "0123456789"
    # nested: outer + inner -> whole range masked, no digit leaks
    assert mask_text_spans(text, [PiiSpan(0, 10, "x", "d"), PiiSpan(3, 6, "x", "d")]) == "[nomos:masked]"
    # overlapping: [0,5)+[3,8) union to [0,8); "89" survives, "01234567" gone
    out = mask_text_spans(text, [PiiSpan(0, 5, "x", "d"), PiiSpan(3, 8, "x", "d")])
    assert "89" in out and "01234567" not in out
    # touching: [0,5)+[5,10) union to whole
    assert mask_text_spans(text, [PiiSpan(0, 5, "x", "d"), PiiSpan(5, 10, "x", "d")]) == "[nomos:masked]"
    # disjoint: gap preserved
    out2 = mask_text_spans(text, [PiiSpan(0, 3, "x", "d"), PiiSpan(6, 10, "x", "d")])
    assert "345" in out2 and "012" not in out2 and "6789" not in out2


def test_detector_is_invoked_on_free_text() -> None:
    fact = _Fact("n1", {"body": "some text"})
    with pytest.raises(AssertionError):
        sanitize_fact_pii(
            fact,
            classifier=_classifier({"body": "free_text"}),
            store_raw=False,
            detector=_RaisingDetector(),
        )
