from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog


@pytest.fixture
def ci_coding_context_deployment(tmp_path: Path) -> Path:
    dep = tmp_path / "test-ci-coding-context"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-ci-coding-context",
        "description": "ci ontology with coding context bridges",
        "modules": [
            "git_graph",
            "forge",
            "openspec",
            "agent_sessions",
            "compute_env",
            "code_graph",
            "ci",
        ],
    }))
    (dep / "schemas").mkdir()
    return dep


def test_compose_picks_up_test_evidence_subtypes(
    ci_coding_context_deployment: Path,
) -> None:
    composed = compose_catalog(ci_coding_context_deployment)
    for subtype in {
        "test_report",
        "coverage_report",
        "runtime_event",
        "stack_frame",
        "ci_check_run",
        "ci_artifact",
        "ci_runner",
        "ci_runner_event",
    }:
        assert subtype in composed.subtypes


def test_compose_emits_test_evidence_narrowings(
    ci_coding_context_deployment: Path,
) -> None:
    composed = compose_catalog(ci_coding_context_deployment)
    triples = composed.narrowing_triples()
    expected = {
        ("test_report", "contains", "test_run"),
        ("test_report", "contains", "test_case"),
        ("test_report", "contains", "runtime_event"),
        ("runtime_event", "contains", "stack_frame"),
        ("coverage_report", "references", "source_file"),
        ("coverage_report", "references", "code_symbol"),
        ("runtime_event", "references", "test_case"),
        ("runtime_event", "references", "code_symbol"),
        ("stack_frame", "points_to", "source_file"),
        ("stack_frame", "points_to", "code_symbol"),
        ("acceptance_criterion", "verified_by", "test_case"),
        ("acceptance_criterion", "verified_by", "test_report"),
        ("repo", "contains", "ci_workflow"),
        ("repo", "contains", "ci_runner"),
        ("ci_workflow", "references", "pull_request"),
        ("ci_workflow", "references", "ci_check_run"),
        ("ci_job", "references", "ci_check_run"),
        ("ci_job", "references", "ci_runner"),
        ("ci_job", "produces", "ci_artifact"),
        ("ci_artifact", "references", "ci_workflow"),
        ("ci_artifact", "references", "ci_job"),
        ("test_report", "references", "ci_artifact"),
        ("coverage_report", "references", "ci_artifact"),
        ("ci_runner_event", "references", "ci_runner"),
        ("ci_runner_event", "references", "ci_job"),
    }
    assert not expected - triples
