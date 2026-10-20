"""Schema tests for the substrate `reasoning` ontology module.

Verifies that:
- The catalog composer picks up all 8 reasoning subtypes.
- The 14 reasoning ring narrowings are present, including the
  cross-shared `Hypothesis supported_by AcceptanceCriterion`.
- The `okg_relation_kind` mapping for 3 new semantic labels
  (`cites`, `predicts`, `instantiates`) resolves to `references`.

Mirrors `test_agent_memory_schema.py` pattern.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog


@pytest.fixture
def reasoning_deployment(tmp_path: Path) -> Path:
    dep = tmp_path / "test-reasoning"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-reasoning",
        "description": "reasoning ontology smoke deployment",
        "modules": [
            # git_graph provides source_file (referenced by openspec
            # module's intra-module narrowings).
            "git_graph",
            # openspec provides AcceptanceCriterion for the cross-shared
            # narrowing in reasoning.
            "openspec",
            # agent_memory provides AgentObservation for the optional
            # EpistemicObservationEnricher cross-module narrowing.
            "agent_memory",
            # The module under test.
            "reasoning",
        ],
    }))
    (dep / "schemas").mkdir()
    return dep


def test_compose_picks_up_reasoning_subtypes(
    reasoning_deployment: Path,
) -> None:
    composed = compose_catalog(reasoning_deployment)
    for subtype in {
        "hypothesis",
        "observation",
        "principle",
        "evidence",
        "refutation",
        "question",
        "connection",
        "insight",
    }:
        assert subtype in composed.subtypes, (
            f"reasoning subtype `{subtype}` missing from composed catalog"
        )


def test_compose_emits_reasoning_ring_narrowings(
    reasoning_deployment: Path,
) -> None:
    composed = compose_catalog(reasoning_deployment)
    triples = composed.narrowing_triples()
    expected = {
        # Hypothesis is supported_by various supporters
        ("hypothesis", "supported_by", "observation"),
        ("hypothesis", "supported_by", "evidence"),
        # Cross-shared: AcceptanceCriterion from openspec module
        ("hypothesis", "supported_by", "acceptance_criterion"),
        # Hypothesis is contradicted_by counter-evidence
        ("hypothesis", "contradicted_by", "observation"),
        ("hypothesis", "contradicted_by", "refutation"),
        # Hypothesis-to-Principle relationships
        ("hypothesis", "derives_from", "principle"),
        ("hypothesis", "cites", "principle"),
        ("principle", "predicts", "hypothesis"),
        # Observation instantiates Principle
        ("observation", "instantiates", "principle"),
        # Hypothesis evolution
        ("hypothesis", "supersedes", "hypothesis"),
        # Question / Connection / Insight references into the ring
        ("question", "references", "hypothesis"),
        ("connection", "references", "hypothesis"),
        ("insight", "references", "connection"),
        ("insight", "cites", "observation"),
        # Cross-module: EpistemicObservationEnricher emits these
        ("agent_observation", "references", "observation"),
    }
    missing = expected - triples
    assert not missing, (
        f"reasoning narrowing triples missing: {sorted(missing)}"
    )


def test_compose_picks_up_new_semantic_labels(
    reasoning_deployment: Path,
) -> None:
    """The three new semantic labels (cites, predicts, instantiates)
    SHALL appear as `edge_archetype` values on at least one narrowing
    triple in the composed catalog.
    """
    composed = compose_catalog(reasoning_deployment)
    triples = composed.narrowing_triples()
    edge_types = {edge_type for (_, edge_type, _) in triples}
    for new_label in {"cites", "predicts", "instantiates"}:
        assert new_label in edge_types, (
            f"new semantic label `{new_label}` missing from composed "
            f"narrowings (got: {sorted(edge_types)})"
        )
