"""Tests for the scholarly paper-anatomy schema delta.

Covers spec scenarios:

  - S-A1-R1 / Scenario: Schema parses cleanly after the delta
    (compose_catalog returns without raising)
  - S-A1-R1 / Scenario: Catalog load registers anatomy subtypes
    (verified via compose_catalog → ComposedCatalog)
  - S-A1-R4 / Scenario: All new narrowings register on catalog load
  - S-A1-R6: provenance discipline is enforced at the substrate
    pipeline level; this test pins the subtype/narrowing surface.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]

_SCHOLARLY_TEST_MODULES = [
    "scholarly",
    "person",
    "dataset",
    "extraction",
    "reasoning",
    "openspec",
    "agent_memory",
    "git_graph",
    "agent_sessions",
    "ci",
    "compute_env",
    "code_graph",
]


@pytest.fixture(scope="module")
def composed_scholarly_catalog(tmp_path_factory: pytest.TempPathFactory):
    """Compose a minimal scholarly deployment catalog.

    DB-free; pure compose_catalog over temporary deployment files.
    """
    from okg.substrate.catalog.modules_compose import compose_catalog
    dep = tmp_path_factory.mktemp("scholarly-catalog")
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-scholarly",
        "description": "scholarly paper-anatomy schema test",
        "modules": _SCHOLARLY_TEST_MODULES,
    }))
    (dep / "schemas").mkdir()
    return compose_catalog(deployment_dir=dep)


def test_paper_anatomy_subtypes_registered(composed_scholarly_catalog) -> None:
    """S-A1-R1: all 9 anatomy subtypes resolve via compose."""
    expected = {
        "paper_section", "paragraph", "figure", "table",
        "algorithm", "equation", "theorem", "proof", "citation",
    }
    missing = expected - composed_scholarly_catalog.subtypes
    assert not missing, missing


def test_anatomy_subtypes_originate_in_scholarly(composed_scholarly_catalog) -> None:
    """S-A1-R1: substrate scholarly owns the anatomy subtypes
    (not memex deployment-local; not extraction; not concepts)."""
    expected = {
        "paper_section", "paragraph", "figure", "table",
        "algorithm", "equation", "theorem", "proof", "citation",
    }
    for sub in expected:
        origin = composed_scholarly_catalog.subtype_origin.get(sub)
        assert origin == "scholarly", (sub, origin)


def test_intra_anatomy_narrowings_registered(composed_scholarly_catalog) -> None:
    """S-A1-R4: the 14 new intra-anatomy narrowings appear in the
    composed catalog as scholarly-module narrowings."""
    expected_triples = {
        ("paper", "contains", "paper_section"),
        ("paper_section", "contains", "paper_section"),
        ("paper_section", "contains", "paragraph"),
        ("paper_section", "contains", "figure"),
        ("paper_section", "contains", "table"),
        ("paper_section", "contains", "algorithm"),
        ("paper_section", "contains", "equation"),
        ("paper_section", "contains", "theorem"),
        ("theorem", "contains", "proof"),
        ("paragraph", "contains", "citation"),
        ("theorem", "contains", "citation"),
        ("figure", "contains", "citation"),
        ("table", "contains", "citation"),
        ("citation", "cites", "paper"),
    }
    actual_triples = {
        (n.parent_subtype, n.edge_archetype, n.child_subtype)
        for n in composed_scholarly_catalog.narrowings
        if n.origin == "scholarly"
    }
    missing = expected_triples - actual_triples
    assert not missing, missing


def test_existing_scholarly_narrowings_preserved(
    composed_scholarly_catalog,
) -> None:
    """S-A1-R4: the schema delta SHALL NOT modify existing
    narrowings — `paper_cites_paper`, `paper_authored_by_person`,
    etc., remain present."""
    must_have_pre_existing = {
        ("paper", "cites", "paper"),
        ("paper", "authored_by", "person"),
        ("paper", "published_in", "conference_edition"),
        ("conference", "contains", "conference_edition"),
        ("conference", "published_by", "organization"),
        ("person", "affiliated_with", "organization"),
        ("paper", "mentions", "software"),
        ("paper", "mentions", "benchmark"),
        ("paper", "mentions", "dataset"),
    }
    actual_triples = {
        (n.parent_subtype, n.edge_archetype, n.child_subtype)
        for n in composed_scholarly_catalog.narrowings
        if n.origin == "scholarly"
    }
    missing = must_have_pre_existing - actual_triples
    assert not missing, missing


def test_no_duplicate_anatomy_narrowings(composed_scholarly_catalog) -> None:
    """S-A1-R4 Scenario: zero duplicate triples within scholarly."""
    triples = [
        (n.parent_subtype, n.edge_archetype, n.child_subtype)
        for n in composed_scholarly_catalog.narrowings
        if n.origin == "scholarly"
    ]
    assert len(triples) == len(set(triples))


def test_citation_cites_paper_cross_link_rule_present(
    composed_scholarly_catalog,
) -> None:
    """S-A1-R4 Scenario: `citation_cites_paper` resolves via
    DeclarativeLinker (so absent-target citations re-resolve on each
    publish cycle)."""
    triples = {
        (n.parent_subtype, n.edge_archetype, n.child_subtype)
        for n in composed_scholarly_catalog.narrowings
        if n.origin.startswith("cross_link:citation_cites_paper")
    }
    assert ("citation", "cites", "paper") in triples


def test_paper_anatomy_subtype_required_attrs() -> None:
    """S-A1-R1: minimal-viable required-attr set per subtype.

    Parses the LinkML schema to enforce the contract — required
    attrs match the spec table."""
    import yaml
    schema = yaml.safe_load(
        (REPO_ROOT
         / "src/okg/substrate/library/ontologies/scholarly/schema.yaml")
        .read_text("utf-8")
    )
    classes = schema["classes"]

    expected_required = {
        "PaperSection": {"paper_section_id", "paper_ref", "path", "ordinal"},
        "Paragraph": {"paragraph_id", "parent_section_ref", "ordinal", "text"},
        "Figure": {"figure_id", "parent_section_ref", "label", "ordinal"},
        "Table": {"table_id", "parent_section_ref", "label", "ordinal"},
        "Algorithm": {
            "algorithm_id", "parent_section_ref", "label", "ordinal",
        },
        "Equation": {"equation_id", "parent_section_ref", "label", "ordinal"},
        "Theorem": {
            "theorem_id", "parent_section_ref", "label", "kind", "statement",
        },
        "Proof": {"proof_id", "proves_theorem_ref", "text"},
        "Citation": {"citation_id", "citing_section_ref", "context_text"},
    }
    for cls_name, expected in expected_required.items():
        attrs = classes[cls_name].get("attributes", {})
        required = {
            name for name, defn in attrs.items()
            if isinstance(defn, dict) and defn.get("required")
        }
        missing = expected - required
        assert not missing, (cls_name, missing)


def test_text_bearing_subtypes_use_searchable_text_not_dead_embed_metadata() -> None:
    """Embedding input is canonical searchable text, not a dead selector."""
    import yaml
    schema = yaml.safe_load(
        (REPO_ROOT
         / "src/okg/substrate/library/ontologies/scholarly/schema.yaml")
        .read_text("utf-8")
    )
    classes = schema["classes"]
    expected_primary_fields = {
        "PaperSection": "text",
        "Paragraph": "text",
        "Figure": "caption",
        "Table": "caption",
        "Algorithm": "pseudocode",
        "Equation": "latex_source",
        "Theorem": "statement",
        "Proof": "text",
        "Citation": "context_text",
    }
    for cls_name, primary_field in expected_primary_fields.items():
        anns = classes[cls_name].get("annotations", {})
        assert "okg_embed_text_field" not in anns
        searchable = {
            value.strip()
            for value in anns["okg_searchable_text_fields"].split(",")
        }
        assert primary_field in searchable, (cls_name, searchable)
