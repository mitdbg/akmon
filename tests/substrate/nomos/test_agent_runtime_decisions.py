from __future__ import annotations

import json
from dataclasses import replace

from okg.substrate.nomos import (
    AgentRuntimeDecisionRequest,
    compile_policy_graph,
    evaluate_agent_runtime_decision,
)


def _policy(*actions: str):
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:agent-runtime-decisions",
        "policy_version": "v1",
        "vocabulary_strict": True,
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": f"allow_{action.replace('.', '_')}",
                "effect": "allow",
                "actions": [action],
                "subjects": ["subject:agent:1"],
                "priority": 100,
                "reason_code": "nomos.test.agent_runtime_allowed",
            }
            for action in actions
        ],
    })


def _request(**overrides):
    base = AgentRuntimeDecisionRequest(
        subject_binding_id="subject:agent:1",
        principal_kind="agent",
        action="docker.run",
        resource_kind="container",
        resource_id="container:image:okg-dev",
        data_classification="restricted",
        deployment_posture={
            "deployment_class": "dogfood_workspace",
            "workspace": "okg-workspace",
        },
        policy_generation_pin="policy:generation:42",
        config_generation_pin="config:generation:7",
        config_affects_decision=True,
        enforcement_point="docker.run",
        agent_run_id="agent-run-123",
        profile_id="dev",
        context={
            "command_class": "docker_run",
            "token": "sk-live-raw-agent-secret",
            "private_body": "private file payload",
        },
    )
    return replace(base, **overrides)


def test_agent_runtime_decision_shape_is_complete_and_redacted() -> None:
    bundle = _policy("docker.run")

    outcome = evaluate_agent_runtime_decision(
        bundle,
        _request(),
        active_policy_fingerprint=bundle.policy_fingerprint,
        active_config_fingerprint="config:fingerprint:7",
    )

    serialized = json.dumps(outcome.trace, sort_keys=True)
    assert outcome.allowed is True
    assert outcome.decision.action == "docker.run"
    assert outcome.decision.classification == "restricted"
    assert outcome.decision.resource["resource_kind"] == "container"
    assert outcome.decision.resource["resource_id"] == "container:image:okg-dev"
    assert outcome.decision.context["subject_binding_id"] == "subject:agent:1"
    assert outcome.decision.context["principal_kind"] == "agent"
    assert outcome.decision.context["policy_generation_pin"] == (
        "policy:generation:42"
    )
    assert outcome.decision.context["config_generation_pin"] == "config:generation:7"
    assert outcome.decision.enforcement_point == "docker.run"
    assert outcome.trace["effect"] == "allow"
    assert outcome.trace["reason_code"] == "nomos.test.agent_runtime_allowed"
    assert outcome.trace["matched_rule_ids"] == ["allow_docker_run"]
    assert outcome.trace["input_hash"].startswith("sha256:")
    assert outcome.trace["trace_ref"].startswith("nomos:trace:")
    assert outcome.trace["policy_generation_pin"] == "policy:generation:42"
    assert outcome.trace["config_generation_pin"] == "config:generation:7"
    assert outcome.trace["ordered_obligations"] == []
    assert outcome.trace["agent_runtime"]["agent_run_id"] == "agent-run-123"
    assert "sk-live-raw-agent-secret" not in serialized
    assert "private file payload" not in serialized


def test_agent_runtime_decision_fail_closed_shape_cases() -> None:
    bundle = _policy("docker.run")
    cases = [
        (
            _request(action="agent.teleport"),
            "nomos.runtime.unknown_action",
        ),
        (
            _request(principal_kind="ambient_user"),
            "nomos.runtime.unknown_principal_kind",
        ),
        (
            _request(resource_kind="home_directory"),
            "nomos.runtime.unknown_resource_kind",
        ),
        (
            _request(data_classification="super_secret"),
            "nomos.runtime.unknown_classification",
        ),
        (
            _request(subject_binding_id=""),
            "nomos.runtime.missing_subject_binding_id",
        ),
        (
            _request(policy_generation_pin=""),
            "nomos.runtime.missing_policy_generation_pin",
        ),
        (
            _request(enforcement_point="raw.shell"),
            "nomos.runtime.unknown_enforcement_point",
        ),
        (
            _request(enforcement_point="ssh.use"),
            "nomos.runtime.enforcement_point_mismatch",
        ),
        (
            _request(config_generation_pin=None),
            "nomos.runtime.missing_config_generation_pin",
        ),
        (
            _request(context={"blob": b"not-json"}),
            "nomos.runtime.malformed_request",
        ),
    ]

    for request, reason_code in cases:
        outcome = evaluate_agent_runtime_decision(bundle, request)

        assert outcome.allowed is False
        assert outcome.decision.effect == "deny"
        assert outcome.decision.reason_code == reason_code
        assert outcome.trace["allowed"] is False
        assert "not-json" not in json.dumps(outcome.trace, sort_keys=True)


def test_agent_runtime_decision_missing_policy_and_stale_pins_deny() -> None:
    bundle = _policy("docker.run")

    no_bundle = evaluate_agent_runtime_decision(None, _request())
    stale = evaluate_agent_runtime_decision(
        bundle,
        _request(),
        active_policy_fingerprint="sha256:stale",
    )

    assert no_bundle.allowed is False
    assert no_bundle.decision.reason_code == "nomos.default_deny.no_active_bundle"
    assert stale.allowed is False
    assert stale.decision.reason_code == "nomos.runtime.stale_active_state"


def test_break_glass_bypass_denies_without_active_grant() -> None:
    bundle = _policy("agent.bypass")
    request = _request(
        action="agent.bypass",
        resource_kind="agent_run",
        resource_id="agent-run-123",
        data_classification="restricted",
        enforcement_point="agent.bypass",
    )

    outcome = evaluate_agent_runtime_decision(
        bundle,
        request,
        active_policy_fingerprint=bundle.policy_fingerprint,
    )

    assert outcome.allowed is False
    assert outcome.decision.effect == "deny"
    assert outcome.decision.reason_code == "nomos.runtime.obligation_missing"
    assert outcome.missing_obligations == ("break_glass_review",)
    assert outcome.trace["break_glass"]["required"] is True
    assert outcome.trace["break_glass"]["active"] is False


def test_break_glass_bypass_allows_scoped_unexpired_grant_and_emits_finding():
    bundle = _policy("agent.bypass")
    request = _request(
        action="agent.bypass",
        resource_kind="agent_run",
        resource_id="agent-run-123",
        data_classification="restricted",
        enforcement_point="agent.bypass",
        break_glass_grant={
            "operator": "operator:riley",
            "subject": "subject:agent:1",
            "agent_run_id": "agent-run-123",
            "scope": "claude-code:bypass-permissions",
            "reason": "fixture approval",
            "decision_id": "nomos:decision:breakglass",
            "follow_up_review_state": "open",
            "expires_at": "2999-01-01T00:00:00Z",
        },
    )

    outcome = evaluate_agent_runtime_decision(
        bundle,
        request,
        active_policy_fingerprint=bundle.policy_fingerprint,
    )

    assert outcome.allowed is True
    assert outcome.required_obligations == ("break_glass_review",)
    assert outcome.enforced_obligations == ("break_glass_review",)
    assert outcome.trace["ordered_obligations"] == ["break_glass_review"]
    assert outcome.trace["break_glass"]["active"] is True
    assert outcome.trace["break_glass_finding"]["severity"] == "high"
    assert outcome.trace["break_glass_finding"]["status"] == "active"


def test_break_glass_bypass_rejects_expired_or_revoked_grants() -> None:
    bundle = _policy("agent.bypass")
    base = _request(
        action="agent.bypass",
        resource_kind="agent_run",
        resource_id="agent-run-123",
        data_classification="restricted",
        enforcement_point="agent.bypass",
        break_glass_grant={
            "operator": "operator:riley",
            "subject": "subject:agent:1",
            "agent_run_id": "agent-run-123",
            "scope": "codex:dangerous-mode",
            "reason": "fixture approval",
            "decision_id": "nomos:decision:breakglass",
            "follow_up_review_state": "open",
            "expires_at": "2000-01-01T00:00:00Z",
        },
    )
    expired = evaluate_agent_runtime_decision(bundle, base)
    revoked = evaluate_agent_runtime_decision(
        bundle,
        replace(
            base,
            break_glass_grant={
                **dict(base.break_glass_grant or {}),
                "expires_at": "2999-01-01T00:00:00Z",
                "follow_up_review_state": "revoked",
            },
        ),
    )

    assert expired.allowed is False
    assert expired.trace["break_glass"]["reason_code"] == (
        "nomos.runtime.break_glass_expired"
    )
    assert revoked.allowed is False
    assert revoked.trace["break_glass"]["reason_code"] == (
        "nomos.runtime.break_glass_closed"
    )


def test_agent_runtime_decision_keeps_the_audit_call_effect() -> None:
    # Issue #2279: the agent adapter rebuilds the outcome to enrich its trace;
    # it must carry the audit log-and-allow marker, or an allowed call reads
    # back as the policy's `deny`.
    bundle = _policy("file.read")

    enforce = evaluate_agent_runtime_decision(
        bundle,
        _request(),
        active_policy_fingerprint=bundle.policy_fingerprint,
        active_config_fingerprint="config:fingerprint:7",
    )
    audit = evaluate_agent_runtime_decision(
        bundle,
        _request(),
        active_policy_fingerprint=bundle.policy_fingerprint,
        active_config_fingerprint="config:fingerprint:7",
        enforcement_mode="audit",
    )

    assert enforce.allowed is False
    assert enforce.call_effect == "deny"
    assert audit.allowed is True
    assert audit.final_effect == "deny"
    assert audit.call_effect == "audit"
