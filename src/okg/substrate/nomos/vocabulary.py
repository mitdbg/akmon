"""Canonical Nomos security, access, and privacy vocabulary."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping


#: Resource kinds of the read-boundary checks each operator read surface makes
#: before it reads rows. A policy rule scoped to one of these authorizes the
#: boundary only; it is not an unscoped graph-read grant, so source-lineage
#: audits, the Nomos doctor and repair planning skip it. The operator UI's
#: boundary joined the CLI and MCP ones in PACT operator-ui-search-view.
READ_BOUNDARY_RESOURCE_KINDS: frozenset[str] = frozenset({
    "cli_search_boundary",
    "mcp_graph_read_boundary",
    "local_operator_boundary",
})


class NomosVocabularyError(ValueError):
    """Raised when a Nomos vocabulary term is unknown or malformed."""


@dataclass(frozen=True)
class SurfaceDecisionMapping:
    """Canonical Nomos mapping for a runtime or manifest-facing surface."""

    surface: str
    action: str
    resource_kind: str
    principal_kind: str
    classification: str

    def to_dict(self) -> dict[str, str]:
        return {
            "surface": self.surface,
            "action": self.action,
            "resource_kind": self.resource_kind,
            "principal_kind": self.principal_kind,
            "classification": self.classification,
        }


EFFECTS: frozenset[str] = frozenset({
    "allow",
    "deny",
    "review",
    "quarantine",
    "redact",
    "declassify",
})

DEPLOYMENT_CLASSES: frozenset[str] = frozenset({
    "ci_authority",
    "dogfood_workspace",
    "personal_okg",
    "public_reference_demo",
    "synthetic_healthcare_demo",
    "governed_research_metadata",
    "org_operational_private",
    "private_product_engineering",
})

PRINCIPAL_KINDS: frozenset[str] = frozenset({
    "agent",
    "automation",
    "ci_runner",
    "external_identity",
    "human_operator",
    "mcp_client",
    "okg_service",
    "source_adapter",
    "system",
})

RESOURCE_KINDS: frozenset[str] = frozenset({
    "agent_run",
    "agent_runtime",
    "config_set",
    "container",
    "deployment",
    "edge",
    "external_action",
    "external_automation_run",
    "external_endpoint",
    "file",
    "generation",
    "graph",
    "graph_export",
    "mcp_tool",
    "network_destination",
    "node",
    "policy_bundle",
    "secret",
    "source",
    "ssh_identity",
    "transcript",
    "local_service",
})

ACTIONS: frozenset[str] = frozenset({
    "admin.override",
    "agent.bypass",
    "agent.launch",
    "config.activate",
    "declassify",
    "deployment.posture.audit",
    "docker.run",
    "external_action.attest_owner_unavailable",
    "external_action.cancel",
    "external_action.decide",
    "external_action.execute",
    "external_action.propose",
    "external_automation.register",
    "external_automation.read",
    "external_automation.link",
    "external_action.read",
    "external_action.read_payload",
    "external_action.reconcile",
    "external_action.report",
    "external_action.restore_reconcile",
    "file.read",
    "file.write",
    "generation.publish",
    "graph.export",
    "graph.read",
    "graph.read.audit",
    "graph.read.cli_search",
    "graph.read.export_like",
    "graph.read.local_operator",
    "graph.read.mcp",
    "graph.read.public_demo",
    "graph.read.sql_admin",
    "mcp.record",
    "mcp.tool.call",
    "network.egress",
    "policy.activate",
    "poros.graph.submit",
    "quarantine.release",
    "redact",
    "secret.use",
    "source.emit",
    "source.ingest",
    "source.live_call",
    "service.persist",
    "ssh.use",
    "transcript.persist",
})

SOURCE_SENSITIVITIES: frozenset[str] = frozenset({
    "credentialed_external",
    "internal",
    "local_private",
    "phi_synthetic",
    "private",
    "public",
    "regulated_metadata",
    "restricted",
    "secret",
})

DATA_CLASSIFICATIONS: frozenset[str] = frozenset({
    "confidential",
    "internal",
    "private",
    "public",
    "regulated_metadata",
    "restricted",
    "secret",
    "synthetic_phi",
})

# Privacy class of a single graph attribute. Orthogonal to
# ``DATA_CLASSIFICATIONS`` (which classifies a whole resource/row): ``pii_class``
# says what KIND of personal data one attribute value is, and therefore how it
# must be transformed at rest and at read. ``non_pii`` is the only class that
# reveals by default; every other class masks. Unclassified attributes must
# fail SAFE to ``sensitive_attribute`` (see ``pii_classification`` resolver) --
# never ``non_pii``.
PII_CLASSES: frozenset[str] = frozenset({
    "direct_identifier",
    "quasi_identifier",
    "sensitive_attribute",
    "free_text",
    "non_pii",
})

OBLIGATION_TYPES: frozenset[str] = frozenset({
    "audit",
    "break_glass_review",
    "declassification",
    "no_export",
    "quarantine",
    "redaction",
    "review",
    "secret_ref_required",
    "source_retention",
})

DECISION_TRACE_FIELDS: tuple[str, ...] = (
    "decision_id",
    "policy_bundle_id",
    "policy_version",
    "policy_fingerprint",
    "input_hash",
    "subject",
    "action",
    "resource",
    "classification",
    "context",
    "effect",
    "reason_code",
    "matched_rule_ids",
    "obligations",
    "enforcement_point",
)

VOCABULARY: Mapping[str, frozenset[str]] = {
    "action": ACTIONS,
    "data_classification": DATA_CLASSIFICATIONS,
    "deployment_class": DEPLOYMENT_CLASSES,
    "effect": EFFECTS,
    "obligation": OBLIGATION_TYPES,
    "pii_class": PII_CLASSES,
    "principal_kind": PRINCIPAL_KINDS,
    "resource_kind": RESOURCE_KINDS,
    "source_sensitivity": SOURCE_SENSITIVITIES,
}

SURFACE_DECISION_MAPPINGS: Mapping[str, SurfaceDecisionMapping] = {
    "agent.launch": SurfaceDecisionMapping(
        surface="agent.launch",
        action="agent.launch",
        resource_kind="agent_runtime",
        principal_kind="agent",
        classification="internal",
    ),
    "agent.bypass": SurfaceDecisionMapping(
        surface="agent.bypass",
        action="agent.bypass",
        resource_kind="agent_run",
        principal_kind="agent",
        classification="restricted",
    ),
    "deployment.manifest": SurfaceDecisionMapping(
        surface="deployment.manifest",
        action="deployment.posture.audit",
        resource_kind="deployment",
        principal_kind="system",
        classification="internal",
    ),
    "source.ingest": SurfaceDecisionMapping(
        surface="source.ingest",
        action="source.ingest",
        resource_kind="source",
        principal_kind="source_adapter",
        classification="internal",
    ),
    "source.emit": SurfaceDecisionMapping(
        surface="source.emit",
        action="source.emit",
        resource_kind="node",
        principal_kind="source_adapter",
        classification="internal",
    ),
    "source.live_call": SurfaceDecisionMapping(
        surface="source.live_call",
        action="source.live_call",
        resource_kind="external_endpoint",
        principal_kind="source_adapter",
        classification="restricted",
    ),
    "source.registry.metadata": SurfaceDecisionMapping(
        surface="source.registry.metadata",
        action="source.ingest",
        resource_kind="source",
        principal_kind="source_adapter",
        classification="internal",
    ),
    "mcp.tool.call": SurfaceDecisionMapping(
        surface="mcp.tool.call",
        action="mcp.tool.call",
        resource_kind="mcp_tool",
        principal_kind="mcp_client",
        classification="internal",
    ),
    "mcp.record": SurfaceDecisionMapping(
        surface="mcp.record",
        action="mcp.record",
        resource_kind="graph",
        principal_kind="mcp_client",
        classification="internal",
    ),
    "graph.read": SurfaceDecisionMapping(
        surface="graph.read",
        action="graph.read",
        resource_kind="graph",
        principal_kind="human_operator",
        classification="internal",
    ),
    "graph.read.audit": SurfaceDecisionMapping(
        surface="graph.read.audit",
        action="graph.read.audit",
        resource_kind="graph",
        principal_kind="system",
        classification="internal",
    ),
    "graph.read.cli_search": SurfaceDecisionMapping(
        surface="graph.read.cli_search",
        action="graph.read.cli_search",
        resource_kind="graph",
        principal_kind="human_operator",
        classification="internal",
    ),
    "graph.read.export_like": SurfaceDecisionMapping(
        surface="graph.read.export_like",
        action="graph.read.export_like",
        resource_kind="graph_export",
        principal_kind="human_operator",
        classification="restricted",
    ),
    "graph.read.local_operator": SurfaceDecisionMapping(
        surface="graph.read.local_operator",
        action="graph.read.local_operator",
        resource_kind="graph",
        principal_kind="human_operator",
        classification="internal",
    ),
    "graph.read.mcp": SurfaceDecisionMapping(
        surface="graph.read.mcp",
        action="graph.read.mcp",
        resource_kind="graph",
        principal_kind="mcp_client",
        classification="internal",
    ),
    "graph.read.public_demo": SurfaceDecisionMapping(
        surface="graph.read.public_demo",
        action="graph.read.public_demo",
        resource_kind="graph",
        principal_kind="external_identity",
        classification="public",
    ),
    "graph.read.sql_admin": SurfaceDecisionMapping(
        surface="graph.read.sql_admin",
        action="graph.read.sql_admin",
        resource_kind="graph",
        principal_kind="human_operator",
        classification="restricted",
    ),
    "graph.export": SurfaceDecisionMapping(
        surface="graph.export",
        action="graph.export",
        resource_kind="graph_export",
        principal_kind="human_operator",
        classification="restricted",
    ),
    "admin.override": SurfaceDecisionMapping(
        surface="admin.override",
        action="admin.override",
        resource_kind="policy_bundle",
        principal_kind="human_operator",
        classification="restricted",
    ),
    "generation.publish": SurfaceDecisionMapping(
        surface="generation.publish",
        action="generation.publish",
        resource_kind="generation",
        principal_kind="okg_service",
        classification="internal",
    ),
    "policy.activate": SurfaceDecisionMapping(
        surface="policy.activate",
        action="policy.activate",
        resource_kind="policy_bundle",
        principal_kind="human_operator",
        classification="internal",
    ),
    "config.activate": SurfaceDecisionMapping(
        surface="config.activate",
        action="config.activate",
        resource_kind="config_set",
        principal_kind="human_operator",
        classification="internal",
    ),
    "deployment.profile_defaults": SurfaceDecisionMapping(
        surface="deployment.profile_defaults",
        action="deployment.posture.audit",
        resource_kind="config_set",
        principal_kind="system",
        classification="internal",
    ),
    "secret.use": SurfaceDecisionMapping(
        surface="secret.use",
        action="secret.use",
        resource_kind="secret",
        principal_kind="okg_service",
        classification="secret",
    ),
    "ssh.use": SurfaceDecisionMapping(
        surface="ssh.use",
        action="ssh.use",
        resource_kind="ssh_identity",
        principal_kind="agent",
        classification="restricted",
    ),
    "docker.run": SurfaceDecisionMapping(
        surface="docker.run",
        action="docker.run",
        resource_kind="container",
        principal_kind="agent",
        classification="restricted",
    ),
    "network.egress": SurfaceDecisionMapping(
        surface="network.egress",
        action="network.egress",
        resource_kind="network_destination",
        principal_kind="agent",
        classification="restricted",
    ),
    "file.read": SurfaceDecisionMapping(
        surface="file.read",
        action="file.read",
        resource_kind="file",
        principal_kind="agent",
        classification="restricted",
    ),
    "file.write": SurfaceDecisionMapping(
        surface="file.write",
        action="file.write",
        resource_kind="file",
        principal_kind="agent",
        classification="restricted",
    ),
    "transcript.persist": SurfaceDecisionMapping(
        surface="transcript.persist",
        action="transcript.persist",
        resource_kind="transcript",
        principal_kind="agent",
        classification="restricted",
    ),
    "service.persist": SurfaceDecisionMapping(
        surface="service.persist",
        action="service.persist",
        resource_kind="local_service",
        principal_kind="agent",
        classification="restricted",
    ),
    "external_automation.register": SurfaceDecisionMapping(
        surface="external_automation.register", action="external_automation.register",
        resource_kind="external_automation_run", principal_kind="external_identity",
        classification="restricted",
    ),
    "external_automation.read": SurfaceDecisionMapping(
        surface="external_automation.read", action="external_automation.read",
        resource_kind="external_automation_run", principal_kind="external_identity",
        classification="restricted",
    ),
    "external_automation.link": SurfaceDecisionMapping(
        surface="external_automation.link", action="external_automation.link",
        resource_kind="external_automation_run", principal_kind="external_identity",
        classification="restricted",
    ),
    "external_action.propose": SurfaceDecisionMapping(
        surface="external_action.propose",
        action="external_action.propose",
        resource_kind="external_action",
        principal_kind="external_identity",
        classification="restricted",
    ),
    "external_action.read": SurfaceDecisionMapping(
        surface="external_action.read",
        action="external_action.read",
        resource_kind="external_action",
        principal_kind="external_identity",
        classification="restricted",
    ),
    "external_action.read_payload": SurfaceDecisionMapping(
        surface="external_action.read_payload",
        action="external_action.read_payload",
        resource_kind="external_action",
        principal_kind="external_identity",
        classification="restricted",
    ),
    "external_action.decide": SurfaceDecisionMapping(
        surface="external_action.decide",
        action="external_action.decide",
        resource_kind="external_action",
        principal_kind="external_identity",
        classification="restricted",
    ),
    "external_action.cancel": SurfaceDecisionMapping(
        surface="external_action.cancel",
        action="external_action.cancel",
        resource_kind="external_action",
        principal_kind="external_identity",
        classification="restricted",
    ),
    "external_action.execute": SurfaceDecisionMapping(
        surface="external_action.execute",
        action="external_action.execute",
        resource_kind="external_action",
        principal_kind="external_identity",
        classification="restricted",
    ),
    "external_action.report": SurfaceDecisionMapping(
        surface="external_action.report",
        action="external_action.report",
        resource_kind="external_action",
        principal_kind="external_identity",
        classification="restricted",
    ),
    "external_action.reconcile": SurfaceDecisionMapping(
        surface="external_action.reconcile",
        action="external_action.reconcile",
        resource_kind="external_action",
        principal_kind="external_identity",
        classification="restricted",
    ),
    "external_action.attest_owner_unavailable": SurfaceDecisionMapping(
        surface="external_action.attest_owner_unavailable",
        action="external_action.attest_owner_unavailable",
        resource_kind="external_action",
        principal_kind="external_identity",
        classification="restricted",
    ),
    "external_action.restore_reconcile": SurfaceDecisionMapping(
        surface="external_action.restore_reconcile",
        action="external_action.restore_reconcile",
        resource_kind="external_action",
        principal_kind="external_identity",
        classification="restricted",
    ),
    "privacy.redact": SurfaceDecisionMapping(
        surface="privacy.redact",
        action="redact",
        resource_kind="graph_export",
        principal_kind="okg_service",
        classification="restricted",
    ),
    "privacy.declassify": SurfaceDecisionMapping(
        surface="privacy.declassify",
        action="declassify",
        resource_kind="graph_export",
        principal_kind="human_operator",
        classification="restricted",
    ),
    "quarantine.release": SurfaceDecisionMapping(
        surface="quarantine.release",
        action="quarantine.release",
        resource_kind="graph",
        principal_kind="human_operator",
        classification="restricted",
    ),
    "poros.graph.submit": SurfaceDecisionMapping(
        surface="poros.graph.submit",
        action="poros.graph.submit",
        resource_kind="graph",
        principal_kind="external_identity",
        classification="internal",
    ),
}

REASON_CODE_PATTERN = r"^nomos(?:\.[a-z][a-z0-9_]*)+$"
REASON_CODE_RE = re.compile(REASON_CODE_PATTERN)


def canonical_terms(kind: str) -> frozenset[str]:
    """Return the canonical terms for a Nomos vocabulary kind."""
    try:
        return VOCABULARY[kind]
    except KeyError as exc:
        raise NomosVocabularyError(f"unknown Nomos vocabulary kind: {kind}") from exc


def validate_term(
    kind: str,
    value: str,
    *,
    field_name: str | None = None,
    allow_wildcard: bool = False,
) -> str:
    """Validate one canonical vocabulary term and return it unchanged."""
    if allow_wildcard and value == "*":
        return value
    terms = canonical_terms(kind)
    if value not in terms:
        name = field_name or kind
        raise NomosVocabularyError(
            f"unknown Nomos {name}: {value!r}; expected one of {sorted(terms)}"
        )
    return value


def validate_terms(
    kind: str,
    values: tuple[str, ...],
    *,
    field_name: str | None = None,
    allow_wildcard: bool = False,
) -> tuple[str, ...]:
    """Validate a tuple of canonical vocabulary terms."""
    return tuple(
        validate_term(
            kind,
            value,
            field_name=field_name,
            allow_wildcard=allow_wildcard,
        )
        for value in values
    )


def validate_reason_code(reason_code: str, *, field_name: str = "reason_code") -> str:
    """Validate a stable Nomos reason code."""
    if not REASON_CODE_RE.match(reason_code):
        raise NomosVocabularyError(
            f"{field_name} must be a dot-separated nomos.* code; "
            f"got {reason_code!r}"
        )
    return reason_code


def validate_obligation(obligation: Mapping[str, Any], *, field_name: str) -> None:
    """Validate one obligation object against the canonical obligation vocabulary."""
    obligation_type = obligation.get("type")
    if not isinstance(obligation_type, str) or not obligation_type.strip():
        raise NomosVocabularyError(f"{field_name}.type must be a non-empty string")
    validate_term(
        "obligation",
        obligation_type,
        field_name=f"{field_name}.type",
    )


def validate_policy_graph_vocabulary(policy_graph: Mapping[str, Any]) -> None:
    """Validate strict policy graph vocabulary terms.

    This is opt-in for now (`vocabulary_strict: true`) so older extension policies
    can keep loading while Nomos callers migrate onto canonical terms.
    """
    default_effect = policy_graph.get("default_effect")
    if isinstance(default_effect, str):
        validate_term("effect", default_effect, field_name="default_effect")
    raw_rules = policy_graph.get("rules") or []
    if not isinstance(raw_rules, list):
        raise NomosVocabularyError("rules must be a list before vocabulary checks")
    for index, raw_rule in enumerate(raw_rules):
        if not isinstance(raw_rule, Mapping):
            raise NomosVocabularyError(f"rules[{index}] must be an object")
        rule_id = str(raw_rule.get("rule_id") or f"rules[{index}]")
        effect = raw_rule.get("effect")
        if isinstance(effect, str):
            validate_term("effect", effect, field_name=f"{rule_id}.effect")
        actions = raw_rule.get("actions") or []
        if isinstance(actions, list):
            validate_terms(
                "action",
                tuple(str(action) for action in actions),
                field_name=f"{rule_id}.actions",
                allow_wildcard=True,
            )
        reason_code = raw_rule.get("reason_code")
        if isinstance(reason_code, str) and reason_code:
            validate_reason_code(reason_code, field_name=f"{rule_id}.reason_code")
        match = raw_rule.get("match") or {}
        if isinstance(match, Mapping):
            _validate_match_vocabulary(match, field_name=f"{rule_id}.match")
        obligations = raw_rule.get("obligations") or []
        if isinstance(obligations, list):
            for obligation_index, obligation in enumerate(obligations):
                if isinstance(obligation, Mapping):
                    validate_obligation(
                        obligation,
                        field_name=f"{rule_id}.obligations[{obligation_index}]",
                    )


def surface_mapping_for(surface: str) -> SurfaceDecisionMapping:
    """Return the canonical action/resource/classification mapping for a surface."""
    try:
        return SURFACE_DECISION_MAPPINGS[surface]
    except KeyError as exc:
        raise NomosVocabularyError(
            f"unknown Nomos decision surface: {surface!r}; expected one of "
            f"{sorted(SURFACE_DECISION_MAPPINGS)}"
        ) from exc


def _validate_match_vocabulary(match: Mapping[str, Any], *, field_name: str) -> None:
    _validate_match_term(
        match,
        key="classification",
        kind="data_classification",
        field_name=field_name,
    )
    _validate_match_term(
        match,
        key="resource_kind",
        kind="resource_kind",
        field_name=field_name,
    )
    _validate_match_term(
        match,
        key="source_sensitivity",
        kind="source_sensitivity",
        field_name=field_name,
    )


def _validate_match_term(
    match: Mapping[str, Any],
    *,
    key: str,
    kind: str,
    field_name: str,
) -> None:
    value = match.get(key)
    if isinstance(value, str):
        validate_term(kind, value, field_name=f"{field_name}.{key}")
    elif isinstance(value, list):
        validate_terms(
            kind,
            tuple(str(item) for item in value),
            field_name=f"{field_name}.{key}",
        )
