from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.mcp.record import RECORD_KINDS, record_templates_payload


#: Reference field -> the target subtype its edges must be able to reach,
#: for the fields whose target set is a single agent-memory context subtype.
#: Expectations are DERIVED from the record templates rather than listed by
#: hand: issue #947 shipped because this file (and the cross-link rule it
#: checks) repeated a five-subtype list that omitted `study`, so the study
#: template's advertised fields had no route and nothing failed.
_CONTEXT_FIELD_TARGETS = {
    "study_ref": "study",
    "domain_refs": "domain_entity",
    "project_ref": "project",
}


def _expected_context_triples(*fields: str) -> set[tuple[str, str, str]]:
    """(src_subtype, 'references', target) for every template advertising `fields`."""
    templates = record_templates_payload()["templates"]
    expected: set[tuple[str, str, str]] = set()
    for row in templates:
        for field in fields:
            if field in row["reference_fields"]:
                expected.add((
                    row["target_subtype"],
                    "references",
                    _CONTEXT_FIELD_TARGETS[field],
                ))
    assert expected, f"no record template advertises {fields!r}"
    return expected


def _actual_context_triples(
    triples: set[tuple[str, str, str]],
    *fields: str,
) -> set[tuple[str, str, str]]:
    """The composed `references` triples that land on the `fields` targets.

    Paired with `_expected_context_triples` under `==`, this catches an omitted
    source subtype (the #947 defect) AND a spurious extra one. The two previous
    assertions here were `not expected - triples`, i.e. subset only, so any
    source subtype missing from the hand-written literal was invisible.
    """
    targets = {_CONTEXT_FIELD_TARGETS[field] for field in fields}
    record_subtypes = {
        row["target_subtype"]
        for row in record_templates_payload()["templates"]
    }
    return {
        triple for triple in triples
        if triple[1] == "references"
        and triple[2] in targets
        and triple[0] in record_subtypes
    }


@pytest.fixture
def agent_memory_deployment(tmp_path: Path) -> Path:
    dep = tmp_path / "test-agent-memory"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-agent-memory",
        "description": "agent memory ontology smoke deployment",
        "modules": [
            "git_graph",
            "openspec",
            "agent_sessions",
            "compute_env",
            "ci",
            "code_graph",
            "quality_evidence",
            "agent_memory",
        ],
    }))
    (dep / "schemas").mkdir()
    return dep


def test_compose_picks_up_agent_memory_subtypes(
    agent_memory_deployment: Path,
) -> None:
    composed = compose_catalog(agent_memory_deployment)
    agent_memory_targets = {
        row["target_subtype"]
        for row in record_templates_payload()["templates"]
        # `project` is owned by the bookkeeping module, not agent_memory.
        if row["target_subtype"] != "project"
    }
    # Bound from BELOW as well as above. `< len(RECORD_KINDS)` alone would hold
    # for an EMPTY derived set, and the loop under it would then assert nothing
    # -- a vacuous pass where the previous hand-written literal could not be.
    assert len(agent_memory_targets) >= 7, sorted(agent_memory_targets)
    assert len(agent_memory_targets) < len(RECORD_KINDS)
    assert "study" in agent_memory_targets
    for subtype in agent_memory_targets:
        assert subtype in composed.subtypes


def test_compose_emits_agent_memory_narrowings(
    agent_memory_deployment: Path,
) -> None:
    composed = compose_catalog(agent_memory_deployment)
    triples = composed.narrowing_triples()
    expected = {
        ("agent_claim", "supported_by", "agent_observation"),
        ("agent_claim", "supported_by", "verification_result"),
        ("agent_claim", "contradicted_by", "agent_observation"),
        ("agent_experiment", "produces", "agent_observation"),
        ("agent_experiment", "produces", "verification_result"),
        ("verification_result", "verifies", "agent_claim"),
        ("agent_claim", "supersedes", "agent_claim"),
        ("agent_decision", "references", "agent_claim"),
    }
    assert not expected - triples

    # Equality, not subset, over the record-source slice: every record subtype
    # whose template advertises study_ref / domain_refs must be a declared
    # source, and no others. Derived, so `study --references--> domain_entity`
    # is covered without naming it -- the study template advertises domain_refs
    # (#947).
    context_expected = _expected_context_triples("study_ref", "domain_refs")
    assert ("study", "references", "domain_entity") in context_expected
    assert _actual_context_triples(
        triples, "study_ref", "domain_refs",
    ) == context_expected


def test_compose_agent_memory_with_bookkeeping_emits_project_narrowings(
    tmp_path: Path,
) -> None:
    dep = tmp_path / "test-agent-memory-bookkeeping"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-agent-memory-bookkeeping",
        "description": "agent memory plus bookkeeping ontology fixture",
        "modules": [
            "git_graph",
            "openspec",
            "agent_sessions",
            "compute_env",
            "ci",
            "code_graph",
            "quality_evidence",
            "agent_memory",
            "bookkeeping",
        ],
    }))
    (dep / "schemas").mkdir()

    composed = compose_catalog(dep)
    triples = composed.narrowing_triples()

    # `project` is bookkeeping-owned, so this fixture -- not the agent_memory-only
    # one -- is where the project routes must hold.
    expected = _expected_context_triples("project_ref")
    # The study template advertises project_ref, so a study must be able to
    # link to its project (#947): the edge was dropped for missing_narrowing
    # while the study node was created anyway.
    assert ("study", "references", "project") in expected
    assert _actual_context_triples(triples, "project_ref") == expected
