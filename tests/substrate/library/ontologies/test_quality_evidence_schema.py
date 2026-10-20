from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog


@pytest.fixture
def quality_deployment(tmp_path: Path) -> Path:
    dep = tmp_path / "test-quality-evidence"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-quality-evidence",
        "description": "quality evidence ontology smoke deployment",
        "modules": [
            "git_graph",
            "openspec",
            "agent_sessions",
            "compute_env",
            "code_graph",
            "ci",
            "quality_evidence",
        ],
    }))
    (dep / "schemas").mkdir()
    return dep


def test_compose_picks_up_quality_evidence_subtypes(
    quality_deployment: Path,
) -> None:
    composed = compose_catalog(quality_deployment)
    for subtype in {
        "security_finding",
        "code_quality_finding",
        "secret_finding",
        "dependency_vulnerability",
        "sbom_artifact",
        "profile_run",
        "profile_stage",
        "profile_query",
        "profile_hotspot",
        "benchmark_measurement",
        "performance_budget",
    }:
        assert subtype in composed.subtypes


def test_compose_emits_quality_evidence_narrowings(
    quality_deployment: Path,
) -> None:
    composed = compose_catalog(quality_deployment)
    triples = composed.narrowing_triples()
    expected = {
        ("security_finding", "affects", "source_file"),
        ("security_finding", "affects", "code_symbol"),
        ("code_quality_finding", "affects", "source_file"),
        ("code_quality_finding", "affects", "code_symbol"),
        ("code_quality_finding", "affects", "git_commit"),
        ("dependency_vulnerability", "affects", "dependency_package"),
        ("profile_run", "contains", "profile_stage"),
        ("profile_run", "contains", "profile_query"),
        ("profile_run", "contains", "profile_hotspot"),
        ("profile_hotspot", "affects", "code_symbol"),
        ("benchmark_measurement", "references", "performance_budget"),
        ("benchmark_measurement", "references", "acceptance_criterion"),
        ("performance_budget", "budgeted_by", "acceptance_criterion"),
    }
    assert not expected - triples
