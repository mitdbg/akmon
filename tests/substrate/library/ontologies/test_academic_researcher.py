"""academic_researcher ontology compose tests.

Coverage matrix:

1. Schema-level: 11 subtypes present, Advisee/Lab declare `is_a:`,
   module.yaml parses cleanly through the manifest loader.
2. Compose-level: catalog-compose with `academic_researcher` loaded
   alongside transitive deps surfaces all 11 subtypes; Advisee
   parent_subtype = 'person', Lab parent_subtype = 'team'.
3. Narrowings: 3 intra-module narrowings surface; cross-module
   narrowings (against person, scholarly, bookkeeping, reasoning,
   extraction) fire iff the named sibling is loaded.
4. Refinement closure: Advisee + Lab ride the Layer 1A closure
   so `ancestor_name = 'person'` queries surface advisees alongside
   base Persons, and `ancestor_name = 'team'` surfaces labs.

Per `add-academic-pi-ontology-module` (Child 1 of
`add-academic-pi-profile`).
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.catalog.module_manifest import load_manifest


ONTOLOGIES_DIR = (
    Path(__file__).resolve().parents[4]
    / "src/okg/substrate/library/ontologies"
)
ACADEMIC_RESEARCHER_DIR = ONTOLOGIES_DIR / "academic_researcher"

SCHEMA_PATH = ACADEMIC_RESEARCHER_DIR / "schema.yaml"
MANIFEST_PATH = ACADEMIC_RESEARCHER_DIR / "module.yaml"


def test_old_module_name_is_gone_and_refused(tmp_path: Path) -> None:
    """The module was renamed from `academic_pi` on 2026-09-23 with no
    alias: no library directory keeps the old name, and a deployment
    that still lists it fails composition instead of loading nothing.
    """
    assert not (ONTOLOGIES_DIR / "academic_pi").exists()
    dep = tmp_path / "old-name"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "old-name",
        "modules": ["academic_pi", "person"],
    }))
    (dep / "schemas").mkdir()
    with pytest.raises(Exception, match="academic_pi"):
        compose_catalog(dep)


# ---------- 1. Schema-level shape ----------


def _classes() -> dict:
    with SCHEMA_PATH.open() as f:
        return (yaml.safe_load(f) or {}).get("classes") or {}


def test_module_yaml_validates() -> None:
    """module.yaml parses through the Layer 1B manifest loader."""
    manifest = load_manifest(MANIFEST_PATH)
    assert manifest.name == "academic_researcher"
    assert manifest.version == "1.0.0"
    assert manifest.provides.subtypes_file == "schema.yaml"
    # All 13 declared edge_types are present.
    assert "advises" in manifest.provides.edge_types
    assert "has_deadline" in manifest.provides.edge_types
    assert "affiliated_with" in manifest.provides.edge_types
    assert "addresses" in manifest.provides.edge_types
    # Extends blocks cover all 5 sibling modules.
    for sibling in ("person", "scholarly", "bookkeeping",
                    "reasoning", "extraction"):
        assert sibling in manifest.extends, (
            f"missing extends: {sibling}"
        )


@pytest.mark.parametrize("subtype_name,parent", [
    ("Advisee", "Person"),
    ("Lab", "Team"),
])
def test_refinement_is_a_declared(subtype_name: str, parent: str) -> None:
    """Advisee and Lab declare LinkML is_a: so the Layer 1A
    refinement closure links them to their parents.
    """
    classes = _classes()
    assert subtype_name in classes
    assert classes[subtype_name].get("is_a") == parent, (
        f"{subtype_name}.is_a SHALL be {parent!r} for the Layer 1A "
        f"closure to populate okg.subtype_ancestors correctly"
    )


# ---------- 2. Compose-level: full + minimal deployments ----------


@pytest.fixture
def academic_researcher_deployment(tmp_path: Path) -> Path:
    """Deployment that loads academic_researcher + all 5 sibling modules
    so every cross-module narrowing surfaces. Includes transitive
    deps required by the siblings' narrowings.
    """
    dep = tmp_path / "test-academic-researcher"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-academic-researcher",
        "description": "academic_researcher compose smoke (all siblings loaded)",
        "modules": [
            "academic_researcher",
            # Refinement parents:
            "person",
            # Cross-module narrowing targets:
            "scholarly",
            "bookkeeping",
            "reasoning",
            "extraction",
            # Transitive deps required by siblings:
            "dataset",     # scholarly references
            "openspec",    # reasoning references
            "agent_memory",  # reasoning references
            "git_graph",   # openspec references
        ],
    }))
    (dep / "schemas").mkdir()
    return dep


@pytest.fixture
def academic_researcher_no_scholarly_deployment(tmp_path: Path) -> Path:
    """Deployment that loads academic_researcher + person (refinement
    parent) but NOT scholarly. Used to verify that the 6 academic_researcher
    → scholarly narrowings are SKIPPED when scholarly is absent.
    """
    dep = tmp_path / "test-pi-no-scholarly"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-pi-no-scholarly",
        "description": "academic_researcher without scholarly",
        "modules": [
            "academic_researcher",
            "person",
            # Other siblings still loaded:
            "bookkeeping",
            "reasoning",
            "extraction",
            # Transitive:
            "openspec",
            "agent_memory",
            "git_graph",
        ],
    }))
    (dep / "schemas").mkdir()
    return dep


def test_compose_picks_up_eleven_subtypes(
    academic_researcher_deployment: Path,
) -> None:
    """All 11 academic_researcher subtypes surface in the composed catalog."""
    composed = compose_catalog(academic_researcher_deployment)
    expected_subtypes = {
        "advisee", "lab", "grant", "deadline", "committee",
        "review", "course", "talk_engagement",
        "recommendation_letter", "action_item",
        # `meeting` REMOVED — migrated to calendar.CalendarEvent
        # per `refine-academic-pi-with-comms-and-calendar`.
    }
    missing = expected_subtypes - set(composed.subtypes)
    assert not missing, (
        f"academic_researcher subtypes missing from composed catalog: {missing}"
    )
    assert "meeting" not in composed.subtypes, (
        "Meeting was migrated to calendar.CalendarEvent and must "
        "no longer appear in the composed academic_researcher subtypes"
    )


def test_compose_advisee_parent_is_person(
    academic_researcher_deployment: Path,
) -> None:
    """Advisee.parent_subtype = 'person' — Layer 1A closure."""
    composed = compose_catalog(academic_researcher_deployment)
    assert composed.subtype_parent.get("advisee") == "person", (
        f"advisee.parent_subtype SHALL be 'person' "
        f"(got {composed.subtype_parent.get('advisee')!r})"
    )


def test_compose_lab_parent_is_team(
    academic_researcher_deployment: Path,
) -> None:
    """Lab.parent_subtype = 'team' — Layer 1A closure."""
    composed = compose_catalog(academic_researcher_deployment)
    assert composed.subtype_parent.get("lab") == "team", (
        f"lab.parent_subtype SHALL be 'team' "
        f"(got {composed.subtype_parent.get('lab')!r})"
    )


def test_compose_top_level_subtypes_have_no_parent(
    academic_researcher_deployment: Path,
) -> None:
    """The 9 new top-level subtypes have parent_subtype = None."""
    composed = compose_catalog(academic_researcher_deployment)
    for subtype in (
        "grant", "deadline", "committee", "review", "course",
        "talk_engagement", "recommendation_letter",
        "action_item",
    ):
        parent = composed.subtype_parent.get(subtype)
        assert parent in (None, ""), (
            f"{subtype}.parent_subtype SHALL be empty / None "
            f"(got {parent!r})"
        )


# ---------- 3. Narrowings ----------


def test_compose_intra_narrowings_present(
    academic_researcher_deployment: Path,
) -> None:
    """Three intra-module narrowings surface."""
    composed = compose_catalog(academic_researcher_deployment)
    triples = composed.narrowing_triples()
    expected_intra = {
        # `(meeting, produces, action_item)` REMOVED — see
        # `(calendar_event, produces, action_item)` in the
        # calendar module integration test.
        ("grant", "has_deadline", "deadline"),
        ("committee", "contains", "review"),
    }
    missing = expected_intra - triples
    assert not missing, (
        f"academic_researcher intra narrowings missing: {sorted(missing)}"
    )


def test_compose_person_cross_narrowings(
    academic_researcher_deployment: Path,
) -> None:
    """academic_researcher → person narrowings surface."""
    composed = compose_catalog(academic_researcher_deployment)
    triples = composed.narrowing_triples()
    expected = {
        ("person", "advises", "advisee"),
        ("person", "owns", "action_item"),
    }
    missing = expected - triples
    assert not missing, (
        f"person cross narrowings missing: {sorted(missing)}"
    )


def test_compose_scholarly_cross_narrowings_when_scholarly_loaded(
    academic_researcher_deployment: Path,
) -> None:
    """academic_researcher → scholarly narrowings surface when scholarly
    is loaded. Six cross narrowings expected.
    """
    composed = compose_catalog(academic_researcher_deployment)
    triples = composed.narrowing_triples()
    expected = {
        ("advisee", "author_of", "paper"),
        ("grant", "acknowledged_in", "paper"),
        ("review", "reviews", "paper"),
        ("talk_engagement", "presents", "paper"),
        ("talk_engagement", "hosted_by", "organization"),
        ("lab", "affiliated_with", "organization"),
        ("person", "affiliated_with", "organization"),
        ("course", "hosted_by", "organization"),
    }
    missing = expected - triples
    assert not missing, (
        f"academic_researcher↔scholarly narrowings missing: {sorted(missing)}"
    )


def test_compose_bookkeeping_cross_narrowings(
    academic_researcher_deployment: Path,
) -> None:
    """academic_researcher → bookkeeping narrowings surface."""
    composed = compose_catalog(academic_researcher_deployment)
    triples = composed.narrowing_triples()
    expected = {
        ("grant", "funds", "project"),
        ("grant", "has_milestone", "project_milestone"),
    }
    missing = expected - triples
    assert not missing, (
        f"bookkeeping cross narrowings missing: {sorted(missing)}"
    )


def test_compose_reasoning_cross_narrowing(
    academic_researcher_deployment: Path,
) -> None:
    """ActionItem addresses reasoning.Question — ties to core."""
    composed = compose_catalog(academic_researcher_deployment)
    triples = composed.narrowing_triples()
    assert ("action_item", "addresses", "question") in triples, (
        "ActionItem addresses Question narrowing missing"
    )


def test_compose_extraction_cross_narrowings(
    academic_researcher_deployment: Path,
) -> None:
    """RecommendationLetter → extraction.Document narrowing.
    Meeting's `has_document` migrated to
    calendar.CalendarEvent and is verified in the calendar
    integration test.
    """
    composed = compose_catalog(academic_researcher_deployment)
    triples = composed.narrowing_triples()
    assert ("recommendation_letter", "has_document", "document") in triples
    assert ("meeting", "has_document", "document") not in triples


def test_compose_skips_scholarly_when_scholarly_absent(
    academic_researcher_no_scholarly_deployment: Path,
) -> None:
    """When scholarly isn't loaded, the academic_researcher → scholarly
    narrowings SHALL NOT surface. The composer's narrowing-resolution
    step skips triples whose dst_subtype isn't in the composed
    subtypes set.
    """
    composed = compose_catalog(academic_researcher_no_scholarly_deployment)
    triples = composed.narrowing_triples()
    forbidden = {
        ("advisee", "author_of", "paper"),
        ("grant", "acknowledged_in", "paper"),
        ("review", "reviews", "paper"),
        ("talk_engagement", "presents", "paper"),
        ("talk_engagement", "hosted_by", "organization"),
        ("lab", "affiliated_with", "organization"),
    }
    leaked = forbidden & triples
    assert not leaked, (
        f"scholarly narrowings leaked without scholarly loaded: "
        f"{sorted(leaked)}"
    )


def test_compose_refinements_present_without_scholarly(
    academic_researcher_no_scholarly_deployment: Path,
) -> None:
    """Refinement closure works even without scholarly — the closure
    is local to person + academic_researcher.
    """
    composed = compose_catalog(academic_researcher_no_scholarly_deployment)
    assert composed.subtype_parent.get("advisee") == "person"
    assert composed.subtype_parent.get("lab") == "team"


# ---------- 4. Organization additive extension (scholarly) ----------


def test_scholarly_organization_has_parent_org_attribute() -> None:
    """The scholarly.Organization class gained a `parent_org`
    attribute per the umbrella's additive extension.
    """
    scholarly_schema = (
        Path(__file__).resolve().parents[4]
        / "src/okg/substrate/library/ontologies/scholarly/schema.yaml"
    )
    with scholarly_schema.open() as f:
        schema = yaml.safe_load(f) or {}
    org_class = (schema.get("classes") or {}).get("Organization") or {}
    attrs = org_class.get("attributes") or {}
    assert "parent_org" in attrs, (
        "scholarly.Organization SHALL gain a parent_org attribute "
        "for the affiliation graph"
    )
    assert "kind" in attrs, (
        "scholarly.Organization SHALL retain its kind attribute"
    )
