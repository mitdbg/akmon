"""Tests for the substrate methodology ontology module.

Two test groups:

1. Catalog-composition tests verify the schema parses
   cleanly via the substrate's modules_compose pipeline,
   the four subtypes register in the active catalog, and
   the EdgeNarrowings declarations land both intra-module
   (member_of, contains) and outgoing Pattern B (embodies,
   violates).

2. Schema-content tests parse the YAML directly and verify
   required attributes, archetype annotations, and embed
   text field annotations — load-bearing for substrate
   retrieval per design.md D-meth-5 and the convention
   established in `add-substrate-semantic-concept-linking`.

Per `add-substrate-methodology-module`.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog


SCHEMA_PATH = (
    Path(__file__).resolve().parents[4]
    / "src" / "okg" / "substrate" / "library"
    / "ontologies" / "methodology" / "schema.yaml"
)


@pytest.fixture(scope="module")
def schema_doc() -> dict[str, Any]:
    """Parsed methodology/schema.yaml. Module-scoped so we
    parse once for the whole test file."""
    return yaml.safe_load(SCHEMA_PATH.read_text())


@pytest.fixture
def methodology_deployment(tmp_path: Path) -> Path:
    """Fixture deployment that composes ONLY the methodology
    module. Used to verify the schema parses + narrowings
    register in isolation (without the sibling reasoning
    module loaded — narrowings stay valid but produce no
    edges)."""
    dep = tmp_path / "test-methodology"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-methodology",
        "description": "methodology ontology smoke deployment",
        "modules": ["methodology"],
    }))
    (dep / "schemas").mkdir()
    return dep


# ---------------------------------------------------------------------------
# Catalog-composition tests
# ---------------------------------------------------------------------------


def test_compose_picks_up_methodology_subtypes(
    methodology_deployment: Path,
) -> None:
    """All four methodology subtypes land in the composed
    catalog."""
    composed = compose_catalog(methodology_deployment)
    expected = {"method_card", "method_pattern", "heuristic", "anti_pattern"}
    assert expected <= composed.subtypes, (
        f"missing subtypes: {expected - composed.subtypes}"
    )


def test_compose_emits_methodology_intra_narrowings(
    methodology_deployment: Path,
) -> None:
    """Intra-module narrowings register on catalog load."""
    composed = compose_catalog(methodology_deployment)
    triples = composed.narrowing_triples()
    expected = {
        ("method_card", "member_of", "method_pattern"),
        ("method_card", "contains", "heuristic"),
    }
    assert expected <= triples, (
        f"missing intra-module narrowings: {expected - triples}"
    )


def test_compose_does_not_declare_cross_module_narrowings(
    methodology_deployment: Path,
) -> None:
    """The methodology module SHOULD NOT declare cross-module
    narrowings to reasoning.Principle in its own schema — the
    substrate's `_validate_narrowing_subtypes` rejects
    narrowings whose destination subtype isn't in the composed
    catalog. Cross-module narrowings live in deployment
    bridges (Wave B's
    `memex-compose-personal-okg-deployment` lands them).

    This test regression-guards against accidentally adding
    them back to the substrate module."""
    composed = compose_catalog(methodology_deployment)
    triples = composed.narrowing_triples()
    forbidden = {
        ("method_card", "embodies", "principle"),
        ("heuristic", "embodies", "principle"),
        ("anti_pattern", "violates", "principle"),
    }
    overlap = forbidden & triples
    assert not overlap, (
        f"methodology module declared cross-module narrowings that should "
        f"live in deployment bridges: {overlap}"
    )


# ---------------------------------------------------------------------------
# Schema-content tests (direct YAML inspection)
# ---------------------------------------------------------------------------


def _required_attrs(schema_doc: dict[str, Any], cls: str) -> set[str]:
    attrs = schema_doc["classes"][cls].get("attributes") or {}
    return {
        name for name, spec in attrs.items()
        if isinstance(spec, dict) and spec.get("required") is True
    }


def _annotation(schema_doc: dict[str, Any], cls: str, key: str) -> Any:
    return (
        schema_doc["classes"][cls].get("annotations") or {}
    ).get(key)


def test_methodcard_required_attributes_are_minimal(
    schema_doc: dict[str, Any],
) -> None:
    """MethodCard required attrs are slug + name only.
    description / body_text / source_ref / embodies_principle
    are optional to permit Wave B migration symmetry."""
    assert _required_attrs(schema_doc, "MethodCard") == {"slug", "name"}


def test_methodpattern_required_attributes_are_minimal(
    schema_doc: dict[str, Any],
) -> None:
    """MethodPattern required attrs are slug + name only."""
    assert _required_attrs(schema_doc, "MethodPattern") == {"slug", "name"}


def test_heuristic_requires_statement(
    schema_doc: dict[str, Any],
) -> None:
    """Heuristic requires `statement` — a heuristic without
    its rule is not a heuristic."""
    assert _required_attrs(schema_doc, "Heuristic") == {
        "slug", "name", "statement",
    }


def test_antipattern_required_attributes_are_minimal(
    schema_doc: dict[str, Any],
) -> None:
    """AntiPattern required attrs are slug + name only."""
    assert _required_attrs(schema_doc, "AntiPattern") == {"slug", "name"}


def test_methodology_archetypes(schema_doc: dict[str, Any]) -> None:
    """MethodCard/MethodPattern are `artifact`;
    Heuristic/AntiPattern are `claim`."""
    assert _annotation(schema_doc, "MethodCard", "archetype") == "artifact"
    assert _annotation(schema_doc, "MethodPattern", "archetype") == "artifact"
    assert _annotation(schema_doc, "Heuristic", "archetype") == "claim"
    assert _annotation(schema_doc, "AntiPattern", "archetype") == "claim"


def test_methodology_has_no_dead_embed_text_selector(
    schema_doc: dict[str, Any],
) -> None:
    """No annotation claims to select text outside the real stamper."""
    for subtype in ("MethodCard", "MethodPattern", "Heuristic", "AntiPattern"):
        annotations = schema_doc["classes"][subtype].get("annotations", {})
        assert "okg_embed_text_field" not in annotations


def test_methodology_searchable_text_fields(
    schema_doc: dict[str, Any],
) -> None:
    """okg_searchable_text_fields names the attributes that
    contribute to the substrate's auto-stamped `text` for
    search and embedding fallback."""
    assert _annotation(
        schema_doc, "MethodCard", "okg_searchable_text_fields",
    ) == "name,description,body_text"
    assert _annotation(
        schema_doc, "MethodPattern", "okg_searchable_text_fields",
    ) == "name,summary,body_text"
    assert _annotation(
        schema_doc, "Heuristic", "okg_searchable_text_fields",
    ) == "name,statement,trigger"
    assert _annotation(
        schema_doc, "AntiPattern", "okg_searchable_text_fields",
    ) == "name,description,failure_mode,body_text"


def test_methodology_alias_fields(schema_doc: dict[str, Any]) -> None:
    """okg_alias_fields names the attributes that seed the
    substrate alias table for cross-source identity
    resolution."""
    for cls in ("MethodCard", "MethodPattern", "Heuristic", "AntiPattern"):
        assert _annotation(schema_doc, cls, "okg_alias_fields") == "slug,name", (
            f"{cls} alias fields drifted"
        )


def test_edge_narrowings_class_present(schema_doc: dict[str, Any]) -> None:
    """EdgeNarrowings LinkML class carries the
    okg_edge_narrowings: 'true' annotation, marking it for
    the substrate's narrowing-declaration parser."""
    cls = schema_doc["classes"]["EdgeNarrowings"]
    annotations = cls.get("annotations") or {}
    # The annotation value is either the string 'true' or
    # the boolean True depending on YAML quoting; both
    # parse the same in the substrate.
    assert annotations.get("okg_edge_narrowings") in {"true", True}


def test_edge_narrowings_declarations(schema_doc: dict[str, Any]) -> None:
    """Two intra-module narrowing declarations are present.
    Cross-module narrowings (methodology→reasoning) live in
    deployment bridges per the substrate's established
    pattern; the schema MUST NOT declare them here."""
    cls = schema_doc["classes"]["EdgeNarrowings"]
    attrs = cls.get("attributes") or {}
    expected_names = {
        "methodcard_member_of_methodpattern",
        "methodcard_contains_heuristic",
    }
    forbidden_names = {
        "methodcard_embodies_principle",
        "heuristic_embodies_principle",
        "antipattern_violates_principle",
    }
    assert expected_names <= set(attrs.keys()), (
        f"missing intra-module narrowing declarations: "
        f"{expected_names - set(attrs.keys())}"
    )
    assert not (forbidden_names & set(attrs.keys())), (
        f"cross-module narrowings must live in deployment bridges, "
        f"not in methodology/schema.yaml: "
        f"{forbidden_names & set(attrs.keys())}"
    )
