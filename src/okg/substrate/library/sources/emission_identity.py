"""Auditable emission-code identity closure for runner-safe sources.

``emit_version_for`` can only fingerprint the targets it is given.  Source
adapters often delegate fact extraction, redaction, or graph construction to
helper modules, so targeting the wrapper module alone is not sufficient: a
helper-only code change could otherwise leave the change-probe token stable
and permit a false no-change short circuit.

This matrix is deliberately explicit.  It lists internal modules whose code
can change emitted facts without changing raw source bytes.  Adapters pass
their wrapper/domain-emitter targets to :func:`emission_identity_targets`;
this function adds the delegated closure and de-duplicates it.  Third-party
parsers and the shared fact value objects are outside this source-owned
closure.
"""
from __future__ import annotations

import importlib
from typing import Any


EMISSION_IDENTITY_MODULES: dict[str, tuple[str, ...]] = {
    "agent_sessions": (
        "okg.substrate.library.sources._agent_sessions_extract",
        "okg.substrate.library.sources._agent_sessions_cost",
        "okg.substrate.library.sources._claude_project_scope",
        "okg.substrate.library.sources._mcp_tool_telemetry",
        "okg.substrate.library.sources._transcript_redaction",
        "okg.substrate.library.sources._transcript_resume",
        "okg.substrate.library.sources._git_helpers",
        "okg.substrate.library.sources._content_snapshot",
        "okg.substrate.fastyaml",
        "okg.substrate.transcript_trust",
    ),
    "attack_stix": (
        "okg.substrate.library.sources._cti_snapshot",
    ),
    "branch_dispositions": (
        "okg.substrate.fastyaml",
    ),
    "code_data_flow": (
        "okg.substrate.library.sources._code_graph_git_snapshot",
        "okg.substrate.library.sources._code_graph_common",
        "okg.substrate.library.sources._file_walk",
        "okg.substrate.library.sources._git_helpers",
        "okg.substrate.library.sources.change_probe",
        "okg.substrate.library.sources.code_graph_structure",
        "okg.substrate.library.sources.git_scope_probe",
    ),
    "code_structure": (
        "okg.substrate.library.sources._code_graph_common",
        "okg.substrate.library.sources._git_helpers",
        "okg.substrate.library.sources._source_urls",
        "okg.substrate.library.extraction.code.dispatch",
        "okg.substrate.library.extraction.code.lsp_protocol",
    ),
    "codex_sessions": (
        "okg.substrate.library.sources._claude_project_scope",
        "okg.substrate.library.sources._mcp_tool_telemetry",
        "okg.substrate.library.sources._transcript_redaction",
        "okg.substrate.library.sources._transcript_resume",
        "okg.substrate.library.sources._git_helpers",
        "okg.substrate.library.sources._content_snapshot",
    ),
    "compute_env_local": (
        "okg.substrate.library.sources._compute_env_local_emit",
    ),
    "compute_env_remote_hosts": (
        "okg.substrate.library.sources._claude_project_scope",
        "okg.substrate.library.sources._content_snapshot",
    ),
    "coordination_config": (),
    "cwe_catalog": (
        "okg.substrate.library.sources._cti_snapshot",
        "okg.substrate.library.sources._safe_xml",
    ),
    "coordination_native": (
        "coordination.event_envelope",
    ),
    "edgar_xbrl": (
        "okg.substrate.library.sources._cti_snapshot",
        "okg.substrate.library.sources._safe_xml",
    ),
    "git_graph_files": (
        "okg.substrate.library.sources._git_helpers",
        "okg.substrate.library.sources._source_urls",
    ),
    "git_graph_history": (
        "okg.substrate.library.sources._git_helpers",
        "okg.substrate.support",
    ),
    "github": (
        "okg.substrate.support",
    ),
    "github_actions": (
        "okg.substrate.support",
    ),
    "github_runner_inventory": (
        "okg.substrate.support",
    ),
    "interfaces": (
        "okg.substrate.library.sources._code_graph_git_snapshot",
        "okg.substrate.library.sources._code_graph_common",
        "okg.substrate.library.sources._file_walk",
        "okg.substrate.library.sources._git_helpers",
        "okg.substrate.library.sources.change_probe",
        "okg.substrate.library.sources.code_graph_structure",
        "okg.substrate.library.sources.git_scope_probe",
    ),
    "nvd_feeds": (
        "okg.substrate.library.sources._cti_snapshot",
    ),
    "opensearch_catalog": (
        "okg.substrate.library.sources._opensearch_client",
    ),
    "openspec": (
        "okg.substrate.library.sources._openspec_parse",
        "okg.substrate.fastyaml",
    ),
    "ops_telemetry": (
        "okg.substrate.library.sources._cti_snapshot",
    ),
    "package_manifests": (
        "okg.substrate.library.sources._code_graph_common",
        "okg.substrate.library.sources._git_helpers",
    ),
    "pact": (
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
    ),
    "pact-coordination-events": (),
    "paper_abstracts": (),
    # phantom_wiki reads one generated instance's article files and its
    # Prolog KB with nothing but stdlib string handling, so its emission
    # identity closes over its own module alone.
    "phantom_wiki": (),
    "security_analysis": (
        "okg.substrate.fastyaml",
    ),
    # stark_prime reads the release's six processed files and maps them to
    # facts; the only module outside itself its emission closes over is the
    # torch-free tensor reader, which decides what the endpoint indices ARE.
    "stark-prime": (
        "okg.substrate.library.sources._stark_pt",
    ),
    "statute_corpus": (
        "okg.substrate.library.sources._cti_snapshot",
    ),
}


def emission_identity_targets(
    source_name: str,
    *primary_targets: Any,
) -> tuple[Any, ...]:
    """Return the complete source-owned emission identity target closure.

    Unknown source names fail closed so a newly runner-safe source cannot
    silently opt out of the reviewed matrix.
    """
    try:
        module_names = EMISSION_IDENTITY_MODULES[source_name]
    except KeyError as exc:
        raise ValueError(
            f"source {source_name!r} has no emission identity matrix entry"
        ) from exc

    targets = [*primary_targets]
    targets.extend(importlib.import_module(name) for name in module_names)

    unique: list[Any] = []
    seen: set[int] = set()
    for target in targets:
        identity = id(target)
        if identity in seen:
            continue
        seen.add(identity)
        unique.append(target)
    return tuple(unique)


__all__ = [
    "EMISSION_IDENTITY_MODULES",
    "emission_identity_targets",
]
