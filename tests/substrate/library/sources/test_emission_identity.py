from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.library.sources import change_probe, emission_identity
from okg.substrate.library.sources.change_probe import emit_version_for
from okg.substrate.library.sources.emission_identity import (
    EMISSION_IDENTITY_MODULES,
    emission_identity_targets,
)
from tests.mirror_scope import shipped


REPO_ROOT = Path(__file__).resolve().parents[4]

# Every source class that can grant the runner exact changed=false
# short-circuit authority must go through the reviewed closure.  Two files
# contain two independently runner-safe classes.
SAFE_SOURCE_CALLSITES = {
    "deployments/okg-workspace/coordination/config_snapshot.py": 1,
    "deployments/okg-workspace/coordination/native_state.py": 1,
    "src/okg/substrate/library/sources/agent_sessions.py": 1,
    "src/okg/substrate/library/sources/attack_stix.py": 1,
    "src/okg/substrate/library/sources/branch_dispositions.py": 1,
    "src/okg/substrate/library/sources/cwe_catalog.py": 1,
    "src/okg/substrate/library/sources/edgar_xbrl.py": 1,
    "src/okg/substrate/library/sources/nvd_feeds.py": 1,
    "src/okg/substrate/library/sources/code_graph_dataflow.py": 1,
    "src/okg/substrate/library/sources/code_graph_manifest.py": 1,
    "src/okg/substrate/library/sources/code_graph_structure.py": 1,
    "src/okg/substrate/library/sources/codex_sessions.py": 1,
    "src/okg/substrate/library/sources/compute_env_local.py": 1,
    "src/okg/substrate/library/sources/compute_env_remote_hosts.py": 1,
    "src/okg/substrate/library/sources/git_graph_files.py": 1,
    "src/okg/substrate/library/sources/git_graph_history.py": 1,
    "src/okg/substrate/library/sources/github.py": 1,
    "src/okg/substrate/library/sources/github_actions.py": 2,
    "src/okg/substrate/library/sources/code_graph_interface.py": 1,
    "src/okg/substrate/library/sources/opensearch_catalog.py": 1,
    "src/okg/substrate/library/sources/openspec.py": 1,
    "src/okg/substrate/library/sources/ops_telemetry.py": 1,
    "src/okg/substrate/library/sources/pact.py": 2,
    "src/okg/substrate/library/sources/paper_abstracts.py": 1,
    "src/okg/substrate/library/sources/phantom_wiki.py": 1,
    "src/okg/substrate/library/sources/security_analysis.py": 1,
    "src/okg/substrate/library/sources/stark_prime.py": 1,
    "src/okg/substrate/library/sources/statute_corpus.py": 1,
}

EXPECTED_RUNNER_SAFE_SOURCES = {
    "agent_sessions",
    "attack_stix",
    "branch_dispositions",
    "cwe_catalog",
    "nvd_feeds",
    "code_data_flow",
    "code_structure",
    "codex_sessions",
    "compute_env_local",
    "compute_env_remote_hosts",
    "coordination_config",
    "coordination_native",
    "git_graph_files",
    "git_graph_history",
    "github",
    "github_actions",
    "github_runner_inventory",
    "interfaces",
    "opensearch_catalog",
    "openspec",
    "package_manifests",
    "pact",
    "pact-coordination-events",
    "edgar_xbrl",
    "ops_telemetry",
    "paper_abstracts",
    "phantom_wiki",
    "security_analysis",
    "stark-prime",
    "statute_corpus",
}

# Reviewer-confirmed internal delegated fact code.  Extra conservative targets
# (for example immutable snapshot helpers) are allowed in the production
# matrix, but none of these may disappear.
REQUIRED_DELEGATED_MODULES = {
    "agent_sessions": {
        "okg.substrate.fastyaml",
        "okg.substrate.library.sources._agent_sessions_extract",
        "okg.substrate.library.sources._agent_sessions_cost",
        "okg.substrate.library.sources._claude_project_scope",
        "okg.substrate.library.sources._mcp_tool_telemetry",
        "okg.substrate.library.sources._transcript_redaction",
        "okg.substrate.library.sources._transcript_resume",
        "okg.substrate.library.sources._git_helpers",
        "okg.substrate.transcript_trust",
    },
    "code_data_flow": {
        "okg.substrate.library.sources._code_graph_git_snapshot",
        "okg.substrate.library.sources._code_graph_common",
        "okg.substrate.library.sources._file_walk",
        "okg.substrate.library.sources._git_helpers",
        "okg.substrate.library.sources.change_probe",
        "okg.substrate.library.sources.code_graph_structure",
        "okg.substrate.library.sources.git_scope_probe",
    },
    "code_structure": {
        "okg.substrate.library.extraction.code.dispatch",
        "okg.substrate.library.extraction.code.lsp_protocol",
        "okg.substrate.library.sources._code_graph_common",
        "okg.substrate.library.sources._git_helpers",
    },
    "codex_sessions": {
        "okg.substrate.library.sources._claude_project_scope",
        "okg.substrate.library.sources._mcp_tool_telemetry",
        "okg.substrate.library.sources._transcript_redaction",
        "okg.substrate.library.sources._transcript_resume",
        "okg.substrate.library.sources._git_helpers",
    },
    "compute_env_remote_hosts": {
        "okg.substrate.library.sources._claude_project_scope",
    },
    "git_graph_files": {
        "okg.substrate.library.sources._git_helpers",
    },
    "git_graph_history": {
        "okg.substrate.library.sources._git_helpers",
        "okg.substrate.support",
    },
    "github": {"okg.substrate.support"},
    "github_actions": {"okg.substrate.support"},
    "github_runner_inventory": {"okg.substrate.support"},
    "interfaces": {
        "okg.substrate.library.sources._code_graph_git_snapshot",
        "okg.substrate.library.sources._code_graph_common",
        "okg.substrate.library.sources._file_walk",
        "okg.substrate.library.sources._git_helpers",
        "okg.substrate.library.sources.change_probe",
        "okg.substrate.library.sources.code_graph_structure",
        "okg.substrate.library.sources.git_scope_probe",
    },
    "openspec": {
        "okg.substrate.fastyaml",
        "okg.substrate.library.sources._openspec_parse",
    },
    "package_manifests": {
        "okg.substrate.library.sources._code_graph_common",
    },
    "pact": {
        "okg.substrate.fastyaml",
        "okg.substrate.library.sources._git_helpers",
        "okg.substrate.pact.decisions",
        "okg.substrate.pact.evaluation_context",
        "okg.substrate.pact.evidence_kinds",
        "okg.substrate.pact.fsm",
        "okg.substrate.pact.models",
        "okg.substrate.pact.review",
        "okg.substrate.pact.scope_drift",
        "okg.substrate.pact.store",
        "okg.substrate.pact.validation",
        "okg.substrate.support",
        "okg.substrate.support.bug_report",
    },
    "branch_dispositions": {"okg.substrate.fastyaml"},
    "security_analysis": {"okg.substrate.fastyaml"},
    # CTI snapshot sources (okg-bench-ctibench): shared parse/emission
    # helpers whose changes must bust the probe token.
    "attack_stix": {
        "okg.substrate.library.sources._cti_snapshot",
    },
    # The OpenSearch catalog source delegates every parsing and bounding
    # decision -- the flattened field summary, the retention tri-state,
    # the scope exclusion -- to the client module. A change there changes
    # what is emitted, so it must bust the probe token.
    "opensearch_catalog": {
        "okg.substrate.library.sources._opensearch_client",
    },
    "cwe_catalog": {
        "okg.substrate.library.sources._cti_snapshot",
        "okg.substrate.library.sources._safe_xml",
    },
    "nvd_feeds": {
        "okg.substrate.library.sources._cti_snapshot",
    },
    # STaRK-Prime (okg-bench-graph-benchmarks): the torch-free tensor
    # reader decides what the edge endpoints ARE, so a change in it must
    # bust the probe token.
    "stark-prime": {
        "okg.substrate.library.sources._stark_pt",
    },
}


def test_every_runner_safe_source_uses_the_emission_identity_matrix() -> None:
    assert set(EMISSION_IDENTITY_MODULES) == EXPECTED_RUNNER_SAFE_SOURCES
    # The two okg-workspace coordination modules are private; the public copy
    # checks the shipped sources (tests/mirror_scope.py).
    for relative_path in shipped(SAFE_SOURCE_CALLSITES):
        safe_class_count = SAFE_SOURCE_CALLSITES[relative_path]
        source = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
        assert source.count("emission_identity_targets(") >= safe_class_count, (
            f"{relative_path} does not route every runner-safe source through "
            "the emission identity matrix"
        )


@pytest.mark.parametrize(
    ("source_name", "module_name"),
    [
        (source_name, module_name)
        for source_name, module_names in REQUIRED_DELEGATED_MODULES.items()
        for module_name in sorted(module_names)
    ],
)
def test_delegated_module_mutation_changes_emit_version(
    monkeypatch: pytest.MonkeyPatch,
    source_name: str,
    module_name: str,
) -> None:
    required = REQUIRED_DELEGATED_MODULES[source_name]
    assert required.issubset(EMISSION_IDENTITY_MODULES[source_name])

    helper_module = importlib.import_module(module_name)
    targets = emission_identity_targets(
        source_name,
        emission_identity,
    )
    baseline = emit_version_for(*targets)
    original_getsource = change_probe.inspect.getsource

    def _mutated_getsource(target: Any) -> str:
        source = original_getsource(target)
        if target is helper_module:
            return source + "\n# delegated-emitter-mutation\n"
        return source

    monkeypatch.setattr(change_probe.inspect, "getsource", _mutated_getsource)
    assert emit_version_for(*targets) != baseline


def test_unknown_runner_safe_source_fails_closed() -> None:
    with pytest.raises(ValueError, match="no emission identity matrix entry"):
        emission_identity_targets("unreviewed-safe-source", emission_identity)


def test_pact_coordination_free_helper_mutation_changes_probe_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources import pact

    source = pact.PACTCoordinationEventSource(repo="okg")
    baseline = source.change_probe.token_for_value("stable-ledger")
    original_getsource = change_probe.inspect.getsource

    def _mutated_getsource(target: Any) -> str:
        source_text = original_getsource(target)
        if target is pact:
            return source_text + "\n# pact-coordination-helper-mutation\n"
        return source_text

    monkeypatch.setattr(change_probe.inspect, "getsource", _mutated_getsource)
    assert source.change_probe.token_for_value("stable-ledger") != baseline
