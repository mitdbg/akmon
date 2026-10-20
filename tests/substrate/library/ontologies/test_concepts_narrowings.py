"""Tests for the eight cross-shared narrowings declared by
`extend-substrate-concepts-as-universal-hub`.

Two test groups:

1. Compose-with-source-modules tests verify that when a
   deployment composes `concepts` alongside the source
   modules (`scholarly`, `methodology`, `reasoning`,
   `code_graph`, `experimental`) and their cross-module
   dependencies, all eight narrowing
   triples land in the active catalog.

2. Compose-only-concepts test verifies the relaxed behavior:
   the seven cross-module hub narrowings are
   `optional_when_subtypes_missing`, so composing `concepts`
   without its source modules now succeeds and silently skips
   them (superseding the old all-or-nothing R-A3-1 contract so
   linker-only consumers can compose `concepts` standalone),
   leaving only the self-referential `concept_refines_concept`.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog


@pytest.fixture
def concepts_hub_deployment(tmp_path: Path) -> Path:
    """Fixture deployment composing `concepts` + all source
    modules and cross-module dependencies. This is the only valid composition for
    the cross-shared narrowings to load."""
    dep = tmp_path / "test-concepts-hub"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-concepts-hub",
        "description": "concepts hub + source modules smoke",
        # Source modules + their cross-module deps.
        # scholarly references person via paper_authored_by_person;
        # other modules add their own deps as they're encountered.
        "modules": [
            "concepts",
            "scholarly",
            "extraction",
            "methodology",
            "reasoning",
            "code_graph",
            "experimental",
            "person",
            "dataset",
            "openspec",
            "agent_memory",
            "git_graph",  # supplies `source_file` for code_graph
            "agent_sessions",  # supplies `tool_use_event` for code_graph
            "ci",  # supplies `test_case` for code_graph
            "compute_env",  # supplies `remote_host` for agent_sessions
        ],
    }))
    (dep / "schemas").mkdir()
    return dep


@pytest.fixture
def concepts_only_deployment(tmp_path: Path) -> Path:
    """Fixture deployment composing ONLY `concepts`. Used to
    verify the cross-module hub narrowings are skipped
    (`optional_when_subtypes_missing`) when their source
    modules are absent, so a linker-only deployment composes
    cleanly."""
    dep = tmp_path / "test-concepts-only"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-concepts-only",
        "description": "concepts alone — linker-only; hub narrowings skipped",
        "modules": ["concepts"],
    }))
    (dep / "schemas").mkdir()
    return dep


def test_compose_with_source_modules_registers_eight_narrowings(
    concepts_hub_deployment: Path,
) -> None:
    """All eight Concept-hub narrowings register when the
    five source modules are composed alongside `concepts`."""
    composed = compose_catalog(concepts_hub_deployment)
    triples = composed.narrowing_triples()
    expected = {
        ("paper", "defines", "concept"),
        ("paper", "uses", "concept"),
        ("concept", "refines", "concept"),
        ("method_card", "embodies", "concept"),
        ("hypothesis", "predicts_about", "concept"),
        ("code_symbol", "implements_concept", "concept"),
        ("experiment", "concerns", "concept"),
        ("observation", "evidence_for", "concept"),
    }
    missing = expected - triples
    assert not missing, f"missing Concept-hub narrowings: {missing}"


def test_compose_only_concepts_skips_cross_module_hub_narrowings(
    concepts_only_deployment: Path,
) -> None:
    """Composing `concepts` WITHOUT its source modules now
    succeeds. The seven cross-module hub narrowings are
    `optional_when_subtypes_missing`, so they are silently
    skipped when their foreign source subtypes are absent
    (supersedes the all-or-nothing R-A3-1 contract — enables
    linker-only consumers like af_pubs that only want
    `<subtype> -mentions-> Concept`). Only the self-referential
    `concept_refines_concept` survives, since `Concept` is
    defined by the module itself."""
    composed = compose_catalog(concepts_only_deployment)
    triples = composed.narrowing_triples()
    # The seven cross-module hub narrowings reference subtypes
    # owned by other modules (Paper/MethodCard/Hypothesis/
    # Observation/CodeSymbol/Experiment) that are absent here.
    cross_module_hub = {
        ("paper", "defines", "concept"),
        ("paper", "uses", "concept"),
        ("method_card", "embodies", "concept"),
        ("hypothesis", "predicts_about", "concept"),
        ("code_symbol", "implements_concept", "concept"),
        ("experiment", "concerns", "concept"),
        ("observation", "evidence_for", "concept"),
    }
    leaked = cross_module_hub & triples
    assert not leaked, (
        f"cross-module hub narrowings must be skipped when their source "
        f"subtypes are absent; leaked: {leaked}"
    )
    # The intra-module self-referential narrowing still emits
    # (Concept is present), confirming the annotation is inert
    # when the subtypes ARE in the composed catalog.
    assert ("concept", "refines", "concept") in triples


def test_narrowings_concept_is_dst_in_seven_of_eight(
    concepts_hub_deployment: Path,
) -> None:
    """Concept is the destination subtype in seven of eight
    narrowings; only `concept_refines_concept` has Concept
    on both sides. Regression guard against the hub-pattern
    drifting."""
    composed = compose_catalog(concepts_hub_deployment)
    triples = composed.narrowing_triples()
    concept_hub_triples = {
        t for t in triples
        if t[2] == "concept"
        and t in {
            ("paper", "defines", "concept"),
            ("paper", "uses", "concept"),
            ("concept", "refines", "concept"),
            ("method_card", "embodies", "concept"),
            ("hypothesis", "predicts_about", "concept"),
            ("code_symbol", "implements_concept", "concept"),
            ("experiment", "concerns", "concept"),
            ("observation", "evidence_for", "concept"),
        }
    }
    assert len(concept_hub_triples) == 8


def test_edge_narrowings_class_present_with_annotation() -> None:
    """The EdgeNarrowings class exists and carries the
    okg_edge_narrowings annotation — load-bearing for the
    substrate's narrowing-declaration parser."""
    schema_path = (
        Path(__file__).resolve().parents[4]
        / "src" / "okg" / "substrate" / "library"
        / "ontologies" / "concepts" / "schema.yaml"
    )
    doc = yaml.safe_load(schema_path.read_text())
    cls = doc["classes"]["EdgeNarrowings"]
    annotations = cls.get("annotations") or {}
    assert annotations.get("okg_edge_narrowings") in {"true", True}
    attrs = cls.get("attributes") or {}
    expected_names = {
        "paper_defines_concept",
        "paper_uses_concept",
        "concept_refines_concept",
        "method_card_embodies_concept",
        "hypothesis_predicts_about_concept",
        "code_symbol_implements_concept",
        "experiment_concerns_concept",
        "observation_evidence_for_concept",
    }
    assert expected_names <= set(attrs.keys()), (
        f"missing narrowing entries: {expected_names - set(attrs.keys())}"
    )
