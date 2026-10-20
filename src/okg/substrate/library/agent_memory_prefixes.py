"""Catalog-backed, deployment-extensible prefix registry for agent memory.

Per ``substrate-operability``. Agent-memory node ids are prefixed (``claim:``,
``observation:``, ``symbol:``, ...) and each prefix maps to a catalog node
subtype. This module is the single source of truth for that mapping, shared by
the ``record`` MCP tool (write side) and the ``agent_memory`` library source
(ingest side) — replacing what used to be two divergent hardcoded tables.

``BUILTIN_RECORD_PREFIXES`` ships the substrate defaults. A deployment extends
the set WITHOUT editing substrate code by declaring ``record_prefixes`` in its
manifest (or, for the source, in its source config); those entries are merged
via :func:`normalize_prefixes`, where a deployment entry replaces a builtin
entry for the same prefix. Resolution prefers the longest matching prefix, so a
specific prefix (``agent-claim:``) wins over a generic one (``claim:``).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping


__all__ = [
    "RecordPrefix",
    "BUILTIN_RECORD_PREFIXES",
    "normalize_prefixes",
    "prefix_for_id",
    "subtype_for_id",
]


@dataclass(frozen=True)
class RecordPrefix:
    prefix: str
    target_subtype: str
    owner: str = "okg"
    source: str = "builtin"
    resolution_methods: tuple[str, ...] = ("exact_node_id", "alias")
    missing_policy: str = "warn_only"
    creation_surface: str = "forbidden"
    identity_fields: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        return {
            "prefix": self.prefix,
            "target_subtype": self.target_subtype,
            "owner": self.owner,
            "source": self.source,
            "resolution_methods": list(self.resolution_methods),
            "missing_policy": self.missing_policy,
            "creation_surface": self.creation_surface,
            "identity_fields": list(self.identity_fields),
        }


# Prefixes the `record` MCP tool may MINT a caller-supplied node_id under: the
# agent's own typed knowledge namespaces (claims, observations, decisions,
# experiments, verifications, projects, studies, agent-declared domain entities
# and artifacts). Every OTHER builtin prefix is `creation_surface="forbidden"`:
# those namespaces (doc:, commit:, file:, issue:, pr:, chunk:, mention:,
# session:, code-graph symbols, etc.) are populated by EXTERNAL ingest sources
# with externally-assigned ids, so an agent minting one would collide with or
# shadow curated nodes. `_record_id` honors a caller node_id only when its
# prefix is writable here (harden-mcp-write-boundaries; adversarial-review
# remediation). A deployment opts a custom prefix in by declaring
# `creation_surface: record_template` in its manifest entry.
_RECORD_WRITABLE_PREFIXES: frozenset[str] = frozenset({
    "claim:", "agent-claim:",
    "observation:", "agent-observation:",
    "experiment:", "agent-experiment:",
    "verification:", "verification-result:",
    "decision:", "agent-decision:",
    "proj:", "project:", "study:",
    "region:", "method:", "dataset:",
    "artifact:", "plot:",
})

BUILTIN_RECORD_PREFIXES: tuple[RecordPrefix, ...] = tuple(
    RecordPrefix(
        prefix,
        subtype,
        creation_surface=(
            "record_template" if prefix in _RECORD_WRITABLE_PREFIXES else "forbidden"
        ),
    )
    for prefix, subtype in (
        ("claim:", "agent_claim"),
        ("agent-claim:", "agent_claim"),
        ("observation:", "agent_observation"),
        ("agent-observation:", "agent_observation"),
        ("experiment:", "agent_experiment"),
        ("agent-experiment:", "agent_experiment"),
        ("verification:", "verification_result"),
        ("verification-result:", "verification_result"),
        ("decision:", "agent_decision"),
        ("agent-decision:", "agent_decision"),
        ("symbol:", "code_symbol"),
        ("code-ref:", "code_reference"),
        ("import:", "code_import"),
        ("endpoint:", "interface_endpoint"),
        ("interface:", "interface_endpoint"),
        ("asset:", "data_asset"),
        ("config:", "config_key"),
        ("manifest:", "package_manifest"),
        ("pkg:", "dependency_package"),
        ("change:", "open_spec_change"),
        ("task:", "proposal_task"),
        ("crit:", "acceptance_criterion"),
        ("scn:", "acceptance_scenario"),
        ("test-report:", "test_report"),
        ("coverage:", "coverage_report"),
        ("runtime:", "runtime_event"),
        ("frame:", "stack_frame"),
        ("test-run:", "test_run"),
        ("test-case:", "test_case"),
        ("security-finding:", "security_finding"),
        ("secret-finding:", "secret_finding"),
        ("vuln:", "dependency_vulnerability"),
        ("sbom:", "sbom_artifact"),
        ("profile-run:", "profile_run"),
        ("profile-stage:", "profile_stage"),
        ("profile-query:", "profile_query"),
        ("profile-hotspot:", "profile_hotspot"),
        ("benchmark:", "benchmark_measurement"),
        ("performance-budget:", "performance_budget"),
        ("flow:", "data_flow_observation"),
        ("flow-step:", "data_flow_step"),
        ("self-play-run:", "self_play_run"),
        ("self-play-scenario:", "self_play_scenario"),
        ("self-play-finding:", "self_play_finding"),
        ("message:", "agent_message"),
        ("tool_use:", "tool_use_event"),
        ("session:", "agent_session"),
        ("subsession:", "sub_agent_session"),
        ("commit:", "git_commit"),
        ("issue:", "issue"),
        ("pr:", "pull_request"),
        ("doc:", "document"),
        ("chunk:", "document_chunk"),
        ("mention:", "entity_mention"),
        ("file:", "source_file"),
        ("proj:", "project"),
        ("study:", "study"),
        # Actor namespaces for `actor_ref`. The id shapes here are the ones the
        # ingest sources actually MINT, not names derived from the subtype: git
        # authors are `author:<email>` (_git_graph_history_emit.py), agents are
        # `agent:<model>:<harness>` (_agent_sessions_emit.py), people are
        # `person:<pid|slug>` (scholarly sources). The longer `person:scraped:`
        # namespace is deliberately kept distinct: academic_researcher mints those ids
        # as `advisee`, so treating them as `person` would emit a mis-typed edge
        # that blocks publish. Longest-prefix resolution selects this guard first.
        # A prefix that resolved no real
        # id would change `actor_ref`'s diagnostic reason without making a single
        # link work. All three stay creation_surface="forbidden" (absent from
        # _RECORD_WRITABLE_PREFIXES): an agent may REFERENCE an actor node but
        # must not mint one, per harden-mcp-write-boundaries.
        ("agent:", "agent"),
        ("person:scraped:", "advisee"),
        ("person:", "person"),
        ("author:", "git_author"),
        ("project:", "project"),
        ("artifact:", "artifact"),
        ("plot:", "artifact"),
        ("dataset:", "data_asset"),
        ("region:", "domain_entity"),
        ("method:", "domain_entity"),
    )
)


def normalize_prefixes(
    prefixes: Iterable[RecordPrefix | Mapping[str, Any]] | None,
) -> tuple[RecordPrefix, ...]:
    """Merge builtin prefixes with deployment-supplied extensions.

    ``prefixes`` are deployment-supplied entries (``RecordPrefix`` instances or
    plain mappings from a manifest/source config). A supplied entry replaces a
    builtin with the same prefix. The result is sorted longest-prefix-first so
    the most specific prefix matches in :func:`prefix_for_id`.
    """
    entries = list(BUILTIN_RECORD_PREFIXES)
    for raw in prefixes or ():
        if isinstance(raw, RecordPrefix):
            entry = raw
        else:
            prefix = str(raw.get("prefix") or "")
            target_subtype = str(
                raw.get("target_subtype") or raw.get("subtype") or "",
            )
            if not prefix or not target_subtype:
                continue
            entry = RecordPrefix(
                prefix=prefix,
                target_subtype=target_subtype,
                owner=str(raw.get("owner") or "deployment"),
                source=str(raw.get("source") or "deployment"),
                resolution_methods=tuple(
                    str(v) for v in raw.get("resolution_methods")
                    or ("exact_node_id", "alias")
                ),
                missing_policy=str(raw.get("missing_policy") or "warn_only"),
                creation_surface=str(
                    raw.get("creation_surface") or "forbidden",
                ),
                identity_fields=tuple(
                    str(v) for v in raw.get("identity_fields") or ()
                ),
            )
        entries = [
            existing for existing in entries
            if existing.prefix != entry.prefix
        ]
        entries.append(entry)
    return tuple(sorted(entries, key=lambda item: len(item.prefix), reverse=True))


def prefix_for_id(
    node_id: str,
    prefixes: tuple[RecordPrefix, ...],
) -> RecordPrefix | None:
    """Return the registry entry whose prefix ``node_id`` starts with."""
    for entry in prefixes:
        if node_id.startswith(entry.prefix):
            return entry
    return None


def subtype_for_id(
    node_id: str,
    prefixes: tuple[RecordPrefix, ...],
) -> str | None:
    """Return the catalog subtype for ``node_id``, or ``None`` if unmatched."""
    entry = prefix_for_id(node_id, prefixes)
    return entry.target_subtype if entry is not None else None
