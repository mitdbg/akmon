"""Drift + fail-safe tests for the pii_class privacy taxonomy.

Proves the taxonomy agrees across the Python vocabulary and the Nomos ontology
enum (drift-denies), and that the resolver fails SAFE: unclassified or unknown
data resolves to ``sensitive_attribute`` and never to ``non_pii`` or the
``"internal"`` data-classification default.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

import okg.substrate as substrate
from okg.substrate.nomos import (
    FAILSAFE_PII_CLASS,
    PII_CLASSES,
    FreeTextPiiDetector,
    PiiSpan,
    free_text_detector,
    pii_class_masks,
    register_free_text_detector,
    resolve_pii_class,
    validate_term,
)
from okg.substrate.nomos.vocabulary import NomosVocabularyError

EXPECTED_PII_CLASSES = {
    "direct_identifier",
    "quasi_identifier",
    "sensitive_attribute",
    "free_text",
    "non_pii",
}

_SCHEMA_PATH = (
    Path(substrate.__file__).resolve().parent
    / "library"
    / "ontologies"
    / "nomos"
    / "schema.yaml"
)


def test_python_taxonomy_is_exactly_the_expected_five() -> None:
    assert set(PII_CLASSES) == EXPECTED_PII_CLASSES


def test_validate_term_accepts_taxonomy_and_denies_unknown() -> None:
    for value in PII_CLASSES:
        assert validate_term("pii_class", value) == value
    with pytest.raises(NomosVocabularyError):
        validate_term("pii_class", "directidentifier")


def test_ontology_enum_matches_python() -> None:
    schema = yaml.safe_load(_SCHEMA_PATH.read_text())
    enum = schema["enums"]["NomosPiiClass"]["permissible_values"]
    assert set(enum) == set(PII_CLASSES)


@pytest.mark.parametrize("declared", [None, "", "   ", "bogus", "internal", "PII"])
def test_unclassified_or_unknown_fails_safe_to_sensitive(declared) -> None:
    resolved = resolve_pii_class(declared)
    assert resolved == FAILSAFE_PII_CLASS == "sensitive_attribute"
    # The whole point: never silently reveal, never inherit the "internal" default.
    assert resolved != "non_pii"
    assert resolved != "internal"


@pytest.mark.parametrize("declared", sorted(EXPECTED_PII_CLASSES))
def test_declared_valid_class_is_honored(declared) -> None:
    assert resolve_pii_class(declared) == declared


def test_only_non_pii_reveals() -> None:
    assert pii_class_masks("non_pii") is False
    for value in PII_CLASSES - {"non_pii"}:
        assert pii_class_masks(value) is True
    # Unclassified masks (fail-safe), never reveals.
    assert pii_class_masks(None) is True
    assert pii_class_masks("bogus") is True


def test_free_text_detector_registry_defaults_absent() -> None:
    # The foundation ships no concrete detector; absent is the HOLD signal the
    # at-rest slice keys off (never skip detection silently).
    register_free_text_detector(None)
    assert free_text_detector() is None

    class _FakeDetector:
        def detect(self, text: str) -> list[PiiSpan]:
            return [PiiSpan(0, len(text), "direct_identifier", "fake")]

    detector = _FakeDetector()
    assert isinstance(detector, FreeTextPiiDetector)
    register_free_text_detector(detector)
    try:
        assert free_text_detector() is detector
        assert free_text_detector().detect("x")[0].pii_class == "direct_identifier"
    finally:
        register_free_text_detector(None)
