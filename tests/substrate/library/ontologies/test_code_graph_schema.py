from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog


@pytest.fixture
def code_graph_deployment(tmp_path: Path) -> Path:
    dep = tmp_path / "test-code-graph"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-code-graph",
        "description": "code graph ontology smoke deployment",
        "modules": [
            "git_graph",
            "openspec",
            "agent_sessions",
            "compute_env",
            "ci",
            "code_graph",
        ],
    }))
    (dep / "schemas").mkdir()
    return dep


def test_compose_picks_up_code_graph_subtypes(
    code_graph_deployment: Path,
) -> None:
    composed = compose_catalog(code_graph_deployment)
    for subtype in {
        "code_symbol",
        "code_import",
        "code_reference",
        "interface_endpoint",
        "data_asset",
        "config_key",
        "data_flow_observation",
        "data_flow_step",
        "package_manifest",
        "dependency_package",
    }:
        assert subtype in composed.subtypes


def test_compose_emits_code_graph_data_flow_narrowings(
    code_graph_deployment: Path,
) -> None:
    composed = compose_catalog(code_graph_deployment)
    triples = composed.narrowing_triples()
    expected = {
        ("source_file", "contains", "code_symbol"),
        ("code_symbol", "calls", "code_symbol"),
        ("code_symbol", "references", "data_asset"),
        ("code_symbol", "references", "config_key"),
        ("data_flow_observation", "contains", "data_flow_step"),
        ("data_flow_observation", "references", "code_symbol"),
        ("data_flow_observation", "references", "data_asset"),
        ("data_flow_observation", "references", "config_key"),
        ("data_flow_step", "references", "code_symbol"),
        ("data_flow_step", "references", "data_asset"),
        ("data_flow_step", "references", "config_key"),
    }
    assert not expected - triples
