from __future__ import annotations

import pytest

from okg.substrate.nomos import (
    ACTIONS,
    DATA_CLASSIFICATIONS,
    DECISION_TRACE_FIELDS,
    DEPLOYMENT_CLASSES,
    OBLIGATION_TYPES,
    PRINCIPAL_KINDS,
    REASON_CODE_PATTERN,
    RESOURCE_KINDS,
    SOURCE_SENSITIVITIES,
    SURFACE_DECISION_MAPPINGS,
    VOCABULARY,
    DecisionInput,
    NomosPolicyError,
    compile_policy_graph,
    evaluate,
    surface_mapping_for,
    validate_reason_code,
    validate_term,
)


def test_vocabulary_exposes_expected_canonical_terms() -> None:
    assert DEPLOYMENT_CLASSES >= {
        "ci_authority",
        "dogfood_workspace",
        "personal_okg",
        "public_reference_demo",
        "synthetic_healthcare_demo",
        "governed_research_metadata",
        "org_operational_private",
        "private_product_engineering",
    }
    assert PRINCIPAL_KINDS >= {"human_operator", "mcp_client", "source_adapter"}
    assert RESOURCE_KINDS >= {"deployment", "source", "graph_export", "mcp_tool"}
    assert ACTIONS >= {
        "agent.launch",
        "agent.bypass",
        "deployment.posture.audit",
        "source.ingest",
        "source.emit",
        "mcp.tool.call",
        "graph.export",
        "ssh.use",
        "docker.run",
        "external_action.propose",
        "external_action.read",
        "external_action.read_payload",
        "external_action.decide",
        "external_action.cancel",
        "external_action.execute",
        "external_action.report",
        "external_action.reconcile",
        "external_action.attest_owner_unavailable",
        "external_action.restore_reconcile",
        "network.egress",
        "file.read",
        "file.write",
        "transcript.persist",
        "service.persist",
        "generation.publish",
        "policy.activate",
        "config.activate",
    }
    assert SOURCE_SENSITIVITIES >= {
        "public",
        "restricted",
        "local_private",
        "phi_synthetic",
        "regulated_metadata",
        "credentialed_external",
    }
    assert DATA_CLASSIFICATIONS >= {"public", "internal", "restricted", "private"}
    assert OBLIGATION_TYPES >= {"redaction", "declassification", "audit", "no_export"}
    assert VOCABULARY["action"] is ACTIONS


def test_validate_term_rejects_unknown_vocabulary() -> None:
    with pytest.raises(ValueError, match="unknown Nomos action"):
        validate_term("action", "source.fetch.whatever")

    with pytest.raises(ValueError, match="dot-separated nomos"):
        validate_reason_code("not_nomos_style")

    with pytest.raises(ValueError, match="dot-separated nomos"):
        validate_reason_code("nomos.1")

    assert REASON_CODE_PATTERN.startswith("^nomos")


def test_strict_policy_graph_accepts_canonical_vocabulary() -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:strict-vocabulary",
        "policy_version": "v1",
        "vocabulary_strict": True,
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_source_emit",
                "effect": "allow",
                "actions": ["source.emit"],
                "subjects": ["source:test"],
                "match": {
                    "classification": "public",
                    "resource_kind": "node",
                    "source_sensitivity": "public",
                },
                "reason_code": "nomos.test.source_emit_allowed",
                "obligations": [{"type": "audit"}],
            }
        ],
    })

    decision = evaluate(
        bundle,
        DecisionInput(
            subject="source:test",
            action="source.emit",
            resource={
                "resource_kind": "node",
                "source_sensitivity": "public",
            },
            classification="public",
        ),
    )

    assert decision.effect == "allow"
    assert decision.reason_code == "nomos.test.source_emit_allowed"


def test_strict_policy_graph_rejects_unknown_terms() -> None:
    policy = {
        "policy_bundle_id": "nomos:test:bad-vocabulary",
        "policy_version": "v1",
        "vocabulary_strict": True,
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "bad_action",
                "effect": "allow",
                "actions": ["source.fetch.whatever"],
                "subjects": ["source:test"],
                "reason_code": "nomos.test.bad_action",
            }
        ],
    }

    with pytest.raises(NomosPolicyError, match="unknown Nomos bad_action.actions"):
        compile_policy_graph(policy)

    policy["rules"][0]["actions"] = ["source.emit"]
    policy["rules"][0]["reason_code"] = "bad.reason"
    with pytest.raises(NomosPolicyError, match="bad_action.reason_code"):
        compile_policy_graph(policy)

    policy["rules"][0]["reason_code"] = "nomos.test.bad_obligation"
    policy["rules"][0]["obligations"] = [{"type": "notify_someone"}]
    with pytest.raises(NomosPolicyError, match="unknown Nomos .*obligations"):
        compile_policy_graph(policy)

    policy["rules"][0]["obligations"] = [{"type": "audit"}]
    policy["rules"][0]["match"] = {
        "resource_kind": ["node", "not_a_resource_kind"],
    }
    with pytest.raises(NomosPolicyError, match="unknown Nomos .*resource_kind"):
        compile_policy_graph(policy)

    policy["rules"][0]["match"] = {
        "source_sensitivity": ["public", "not_a_sensitivity"],
    }
    with pytest.raises(NomosPolicyError, match="unknown Nomos .*source_sensitivity"):
        compile_policy_graph(policy)


def test_surface_mappings_use_canonical_vocabulary() -> None:
    surfaces = {
        "agent.launch": ("agent.launch", "agent_runtime"),
        "agent.bypass": ("agent.bypass", "agent_run"),
        "deployment.manifest": ("deployment.posture.audit", "deployment"),
        "source.ingest": ("source.ingest", "source"),
        "source.emit": ("source.emit", "node"),
        "source.live_call": ("source.live_call", "external_endpoint"),
        "source.registry.metadata": ("source.ingest", "source"),
        "mcp.tool.call": ("mcp.tool.call", "mcp_tool"),
        "mcp.record": ("mcp.record", "graph"),
        "graph.read": ("graph.read", "graph"),
        "graph.read.audit": ("graph.read.audit", "graph"),
        "graph.read.cli_search": ("graph.read.cli_search", "graph"),
        "graph.read.export_like": ("graph.read.export_like", "graph_export"),
        "graph.read.local_operator": ("graph.read.local_operator", "graph"),
        "graph.read.mcp": ("graph.read.mcp", "graph"),
        "graph.read.public_demo": ("graph.read.public_demo", "graph"),
        "graph.read.sql_admin": ("graph.read.sql_admin", "graph"),
        "graph.export": ("graph.export", "graph_export"),
        "admin.override": ("admin.override", "policy_bundle"),
        "generation.publish": ("generation.publish", "generation"),
        "policy.activate": ("policy.activate", "policy_bundle"),
        "config.activate": ("config.activate", "config_set"),
        "deployment.profile_defaults": ("deployment.posture.audit", "config_set"),
        "secret.use": ("secret.use", "secret"),
        "ssh.use": ("ssh.use", "ssh_identity"),
        "docker.run": ("docker.run", "container"),
        "network.egress": ("network.egress", "network_destination"),
        "file.read": ("file.read", "file"),
        "file.write": ("file.write", "file"),
        "transcript.persist": ("transcript.persist", "transcript"),
        "service.persist": ("service.persist", "local_service"),
        "external_action.propose": ("external_action.propose", "external_action"),
        "external_automation.register": ("external_automation.register", "external_automation_run"),
        "external_automation.read": ("external_automation.read", "external_automation_run"),
        "external_automation.link": ("external_automation.link", "external_automation_run"),
        "external_action.read": ("external_action.read", "external_action"),
        "external_action.read_payload": ("external_action.read_payload", "external_action"),
        "external_action.decide": ("external_action.decide", "external_action"),
        "external_action.cancel": ("external_action.cancel", "external_action"),
        "external_action.execute": ("external_action.execute", "external_action"),
        "external_action.report": ("external_action.report", "external_action"),
        "external_action.reconcile": ("external_action.reconcile", "external_action"),
        "external_action.attest_owner_unavailable": (
            "external_action.attest_owner_unavailable",
            "external_action",
        ),
        "external_action.restore_reconcile": (
            "external_action.restore_reconcile",
            "external_action",
        ),
        "privacy.redact": ("redact", "graph_export"),
        "privacy.declassify": ("declassify", "graph_export"),
        "quarantine.release": ("quarantine.release", "graph"),
        "poros.graph.submit": ("poros.graph.submit", "graph"),
    }

    assert {mapping.action for mapping in SURFACE_DECISION_MAPPINGS.values()} == ACTIONS

    for surface, (action, resource_kind) in surfaces.items():
        mapping = surface_mapping_for(surface)
        assert mapping.action == action
        assert mapping.resource_kind == resource_kind
        assert mapping.action in ACTIONS
        assert mapping.resource_kind in RESOURCE_KINDS
        assert mapping.principal_kind in PRINCIPAL_KINDS
        assert mapping.classification in DATA_CLASSIFICATIONS


def test_decision_trace_fields_are_stable() -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:trace-fields",
        "policy_version": "v1",
        "vocabulary_strict": True,
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_export",
                "effect": "redact",
                "actions": ["graph.export"],
                "subjects": ["agent:test"],
                "match": {"classification": "restricted"},
                "reason_code": "nomos.test.export_redacted",
                "obligations": [{"type": "redaction"}],
            }
        ],
    })
    decision = evaluate(
        bundle,
        DecisionInput(
            subject="agent:test",
            action="graph.export",
            classification="restricted",
            resource={"resource_kind": "graph_export"},
        ),
    )

    assert set(DECISION_TRACE_FIELDS) <= set(decision.to_dict())
