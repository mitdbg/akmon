"""Tests for the substrate concepts ontology module.

Schema-content tests (direct YAML inspection):
- Required attributes are exactly {name, description}
  (the four new attributes added by
  `extend-substrate-concepts-as-universal-hub` are all
  optional; legacy seed YAML continues to load).
- The four new attrs (`kind`, `definition`, `confidence`,
  `extracted_from_paper_ref`) are declared.
- `okg_alias_fields: name` annotation marks `name` as the
  canonical alias key for the eight cross-shared
  narrowings declared in EdgeNarrowings.

Catalog-composition tests for the EdgeNarrowings block
land in section 2 of tasks.md (test_concepts_narrowings.py).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml


SCHEMA_PATH = (
    Path(__file__).resolve().parents[4]
    / "src" / "okg" / "substrate" / "library"
    / "ontologies" / "concepts" / "schema.yaml"
)


@pytest.fixture(scope="module")
def schema_doc() -> dict[str, Any]:
    return yaml.safe_load(SCHEMA_PATH.read_text())


def _required_attrs(schema_doc: dict[str, Any], cls: str) -> set[str]:
    attrs = schema_doc["classes"][cls].get("attributes") or {}
    return {
        name for name, spec in attrs.items()
        if isinstance(spec, dict) and spec.get("required") is True
    }


def _attr(schema_doc: dict[str, Any], cls: str, name: str) -> dict[str, Any]:
    return schema_doc["classes"][cls]["attributes"][name]


def _annotation(schema_doc: dict[str, Any], cls: str, key: str) -> Any:
    return (
        schema_doc["classes"][cls].get("annotations") or {}
    ).get(key)


def test_concept_required_attributes_unchanged(
    schema_doc: dict[str, Any],
) -> None:
    """Required attrs are still exactly {name, description}.
    The four new attrs added by this child are optional so
    legacy seed YAML loads without modification."""
    assert _required_attrs(schema_doc, "Concept") == {
        "name", "description",
    }


def test_concept_new_attributes_declared(
    schema_doc: dict[str, Any],
) -> None:
    """The four new attrs land in the Concept class."""
    attrs = schema_doc["classes"]["Concept"]["attributes"]
    for name in (
        "kind", "definition", "confidence",
        "extracted_from_paper_ref",
    ):
        assert name in attrs, f"missing attribute: {name}"


def test_concept_new_attributes_are_optional(
    schema_doc: dict[str, Any],
) -> None:
    """All four new attrs are optional (no `required: true`)."""
    for name in (
        "kind", "definition", "confidence",
        "extracted_from_paper_ref",
    ):
        spec = _attr(schema_doc, "Concept", name)
        assert spec.get("required") is not True, (
            f"attribute {name} drifted to required; should be optional"
        )


def test_concept_confidence_is_float_typed(
    schema_doc: dict[str, Any],
) -> None:
    """`confidence` declares `range: float` per tasks.md 1.3."""
    spec = _attr(schema_doc, "Concept", "confidence")
    assert spec.get("range") == "float"


def test_concept_alias_field_annotation(
    schema_doc: dict[str, Any],
) -> None:
    """okg_alias_fields: name — Concept.name is the canonical
    alias key for the cross-shared narrowings."""
    assert _annotation(schema_doc, "Concept", "okg_alias_fields") == "name"


def test_concept_archetype_preserved(
    schema_doc: dict[str, Any],
) -> None:
    """archetype: artifact is preserved (existing
    convention)."""
    assert _annotation(schema_doc, "Concept", "archetype") == "artifact"


def test_concept_searchable_text_fields_preserved(
    schema_doc: dict[str, Any],
) -> None:
    """okg_searchable_text_fields: description is preserved
    (existing convention; the embedding is driven by
    `description`, not by `name` or the new `definition`)."""
    assert _annotation(
        schema_doc, "Concept", "okg_searchable_text_fields",
    ) == "description"
