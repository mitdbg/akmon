"""Bookkeeping module composition tests.

Per `add-substrate-bookkeeping-module` (Wave A child of
`expand-memex-to-personal-okg`):

- Module composes standalone: only intra-module narrowings active.
- Cross-module narrowings (Pattern B → reasoning, cross-shared ↔
  openspec, Stakeholder → person) live in cross-link YAML files
  and are dormant when the dependent module is not composed.
- Cross-shared narrowings activate when bookkeeping + openspec are
  composed together.
- Stakeholder → Person narrowing activates when bookkeeping +
  person are composed together.

Module-level pattern matches sibling tests
(`test_ci_schema.py`, `test_quality_evidence_schema.py`).
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.library.linkers.rules import load_rules_file


# Per `add-module-manifests-and-composition` (2026-05-19): each
# extending module owns its cross-link YAMLs under
# `<module>/cross_links/`. Bookkeeping extends openspec / person /
# reasoning so all three files live under bookkeeping/.
CROSS_LINKS_ROOT = (
    Path(__file__).resolve().parents[4]
    / "src" / "okg" / "substrate" / "library"
    / "ontologies" / "bookkeeping" / "cross_links"
)


def _make_deployment(
    tmp_path: Path, name: str, modules: list[str],
) -> Path:
    dep = tmp_path / name
    dep.mkdir()
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": name,
            "description": f"test fixture for bookkeeping schema: {name}",
            "modules": modules,
        })
    )
    (dep / "schemas").mkdir()
    return dep


# ---------- rule file parsing ----------


@pytest.mark.parametrize(
    "rule_file,expected_count",
    [
        ("bookkeeping_to_reasoning.yaml", 3),
        ("bookkeeping_to_openspec.yaml", 4),
        ("bookkeeping_to_person.yaml", 1),
    ],
)
def test_cross_link_rule_file_parses(
    rule_file: str, expected_count: int,
) -> None:
    rules = load_rules_file(CROSS_LINKS_ROOT / rule_file)
    assert len(rules) == expected_count


@pytest.mark.parametrize(
    "rule_file,expected_ids",
    [
        ("bookkeeping_to_reasoning.yaml", {
            "project_investigates_hypothesis",
            "project_goal_guided_by_principle",
            "hypothesis_predicts_project_risk",
        }),
        ("bookkeeping_to_openspec.yaml", {
            "openspecchange_member_of_project",
            "openspec_goal_contributes_to_project_goal",
            "openspec_risk_contributes_to_project_risk",
            "openspec_stakeholder_identifies_as_project_stakeholder",
        }),
        ("bookkeeping_to_person.yaml", {
            "project_stakeholder_identifies_person",
        }),
    ],
)
def test_cross_link_rule_ids(
    rule_file: str, expected_ids: set[str],
) -> None:
    rules = load_rules_file(CROSS_LINKS_ROOT / rule_file)
    assert {r.id for r in rules} == expected_ids


# ---------- standalone composition ----------


def test_compose_bookkeeping_alone(tmp_path: Path) -> None:
    """The module loads standalone; only intra-module narrowings active."""
    dep = _make_deployment(tmp_path, "test-bk-solo", ["bookkeeping"])
    composed = compose_catalog(dep)

    assert {
        "project",
        "project_goal",
        "project_milestone",
        "project_deliverable",
        "project_risk",
        "project_stakeholder",
    } <= composed.subtypes


def test_compose_bookkeeping_alone_intra_narrowings(tmp_path: Path) -> None:
    dep = _make_deployment(tmp_path, "test-bk-solo-intra", ["bookkeeping"])
    composed = compose_catalog(dep)
    triples = composed.narrowing_triples()

    intra = {
        ("project", "contains", "project_milestone"),
        ("project", "contains", "project_goal"),
        ("project", "contains", "project_deliverable"),
        ("project", "contains", "project_risk"),
        ("project", "contains", "project_stakeholder"),
        ("project_milestone", "produces", "project_deliverable"),
    }
    assert intra <= triples


def test_compose_bookkeeping_alone_no_dormant_cross_module(
    tmp_path: Path,
) -> None:
    """Cross-module narrowings stay silent when their target modules
    are not composed."""
    dep = _make_deployment(tmp_path, "test-bk-solo-dormant", ["bookkeeping"])
    composed = compose_catalog(dep)
    triples = composed.narrowing_triples()

    new_archetypes = {
        "investigates", "guided_by", "predicts",
        "contributes_to", "identifies_as",
    }
    activated = {t for t in triples if t[1] in new_archetypes}
    assert activated == set(), (
        f"cross-module narrowings activated without dependent modules: "
        f"{activated}"
    )


# ---------- cross-shared activation ----------


def test_compose_with_openspec_activates_cross_shared(
    tmp_path: Path,
) -> None:
    """Bookkeeping + openspec → four cross-shared narrowings activate.

    `git_graph` is transitively required by openspec for its own
    `change_references_source_file` narrowing.
    """
    dep = _make_deployment(
        tmp_path, "test-bk-openspec",
        ["bookkeeping", "openspec", "git_graph"],
    )
    composed = compose_catalog(dep)
    triples = composed.narrowing_triples()

    cross_shared = {
        ("open_spec_change", "member_of", "project"),
        ("goal", "contributes_to", "project_goal"),
        ("risk", "contributes_to", "project_risk"),
        ("stakeholder", "identifies_as", "project_stakeholder"),
    }
    missing = cross_shared - triples
    assert not missing, f"missing cross-shared narrowings: {missing}"


def test_compose_with_person_activates_stakeholder_link(
    tmp_path: Path,
) -> None:
    dep = _make_deployment(
        tmp_path, "test-bk-person",
        ["bookkeeping", "person"],
    )
    composed = compose_catalog(dep)
    triples = composed.narrowing_triples()

    assert ("project_stakeholder", "identifies", "person") in triples


# ---------- subtype annotation discipline ----------


def test_subtypes_carry_required_annotations(tmp_path: Path) -> None:
    """Each bookkeeping subtype has archetype + alias + searchable
    annotations exposed via subtype_metadata."""
    dep = _make_deployment(tmp_path, "test-bk-annot", ["bookkeeping"])
    composed = compose_catalog(dep)
    md = composed.subtype_metadata

    for sub in (
        "project", "project_goal", "project_milestone",
        "project_deliverable", "project_risk", "project_stakeholder",
    ):
        assert sub in md, f"{sub} missing from subtype_metadata"
        meta = md[sub]
        assert "alias_fields" in meta, (
            f"{sub} missing alias_fields metadata"
        )
        assert "searchable_text_fields" in meta, (
            f"{sub} missing searchable_text_fields metadata"
        )
