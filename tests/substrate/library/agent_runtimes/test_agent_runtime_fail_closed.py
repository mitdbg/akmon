from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.substrate.library.agent_runtimes.docker_egress import (
    AgentDockerRunRequest,
    AgentEgressRequest,
    evaluate_agent_docker_run,
    evaluate_agent_egress,
)
from okg.substrate.library.agent_runtimes.launcher import (
    AgentLaunchRequest,
    AgentRuntimeBoundaryError,
    build_agent_launch_plan,
    require_profiled_agent_run,
)
from okg.substrate.library.agent_runtimes.secret_ssh import (
    AgentSecretGrant,
    AgentSecretUseRequest,
    AgentSshGrant,
    AgentSshUseRequest,
    evaluate_agent_secret_use,
    evaluate_agent_ssh_use,
)
from okg.substrate.nomos import compile_policy_graph
from tests.substrate._fake_credentials import fake_credential


SUBJECT = "subject:agent:fail-closed"
CANARY = fake_credential("sk-", "agent-runtime-fail-closed-DO-NOT-PERSIST")
SECRET_REF = "secret://okg/fail-closed/openai-token"
IDENTITY_REF = "secret://ssh/fail-closed-key"


def _policy(*actions: str):
    rules = []
    for action in actions:
        rule: dict[str, object] = {
            "rule_id": f"allow_{action.replace('.', '_')}",
            "effect": "allow",
            "actions": [action],
            "subjects": [SUBJECT],
            "priority": 100,
            "reason_code": f"nomos.test.{action.replace('.', '_')}_allowed",
        }
        if action == "secret.use":
            rule["obligations"] = [{"type": "secret_ref_required"}]
        rules.append(rule)
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:agent-runtime-fail-closed",
        "policy_version": "v1",
        "vocabulary_strict": True,
        "default_effect": "deny",
        "rules": rules,
    })


def _launch_request(tmp_path: Path, **overrides) -> AgentLaunchRequest:
    values = {
        "runtime": "codex",
        "profile_id": "dev",
        "command": ("codex", "--model", "gpt-5"),
        "workspace_root": tmp_path,
        "subject_binding_id": SUBJECT,
        "deployment": "okg-workspace",
        "operator_id": "operator:riley",
        "run_id": "agent-run-fail-closed",
        "ambient_env": {"PATH": "/usr/bin:/bin", "OPENAI_API_KEY": CANARY},
        "deployment_posture": {"deployment_class": "dogfood_workspace"},
        "policy_generation_pin": "policy:generation:fail-closed",
        "config_generation_pin": "config:generation:fail-closed",
    }
    values.update(overrides)
    return AgentLaunchRequest(**values)


def test_launcher_and_profile_checks_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(AgentRuntimeBoundaryError) as no_policy:
        build_agent_launch_plan(None, _launch_request(tmp_path))
    with pytest.raises(AgentRuntimeBoundaryError) as unknown_profile:
        build_agent_launch_plan(
            _policy("agent.launch"),
            _launch_request(tmp_path, profile_id="root-shell"),
        )
    with pytest.raises(AgentRuntimeBoundaryError) as unprofiled:
        require_profiled_agent_run({}, effect="docker.run")
    stale_bundle = _policy("agent.launch")
    with pytest.raises(AgentRuntimeBoundaryError) as stale:
        build_agent_launch_plan(
            stale_bundle,
            _launch_request(tmp_path),
            active_policy_fingerprint="sha256:stale",
        )
    plan = build_agent_launch_plan(
        stale_bundle,
        _launch_request(tmp_path),
        active_policy_fingerprint=stale_bundle.policy_fingerprint,
    )
    missing_audit_env = dict(plan.env)
    missing_audit_env.pop("OKG_AGENT_AUDIT_TRACE_REF")
    with pytest.raises(AgentRuntimeBoundaryError) as missing_audit:
        require_profiled_agent_run(missing_audit_env, effect="secret.use")
    for effect in ("secret.use", "ssh.use", "docker.run", "network.egress"):
        with pytest.raises(AgentRuntimeBoundaryError) as effect_unprofiled:
            require_profiled_agent_run({}, effect=effect)
        assert effect_unprofiled.value.reason_code == "agent_runtime.unprofiled_run"

    assert no_policy.value.reason_code == "nomos.default_deny.no_active_bundle"
    assert unknown_profile.value.reason_code == "agent_runtime.unknown_profile"
    assert unprofiled.value.reason_code == "agent_runtime.unprofiled_run"
    assert stale.value.reason_code == "nomos.runtime.stale_active_state"
    assert missing_audit.value.reason_code == "agent_runtime.unprofiled_run"
    assert missing_audit.value.trace["missing_env"] == ["OKG_AGENT_AUDIT_TRACE_REF"]

    serialized = json.dumps(
        {
            "no_policy": no_policy.value.trace,
            "unknown_profile": unknown_profile.value.trace,
            "unprofiled": unprofiled.value.trace,
            "stale": stale.value.trace,
            "missing_audit": missing_audit.value.trace,
        },
        sort_keys=True,
    )
    assert CANARY not in serialized


def test_secret_and_ssh_gates_fail_closed_without_valid_policy_or_grant(
    tmp_path: Path,
) -> None:
    secret_request = AgentSecretUseRequest(
        subject_binding_id=SUBJECT,
        agent_run_id="agent-run-fail-closed",
        profile_id="deploy",
        secret_ref=SECRET_REF,
        action="mcp.connect",
        resource_id="mcp:okg-workspace",
        agent_visible_config={"env": {"OPENAI_API_KEY": {"secret_ref": SECRET_REF}}},
    )
    secret_grant = AgentSecretGrant(
        grant_id="secret-grant-fail-closed",
        subject_binding_id=SUBJECT,
        agent_run_id="agent-run-fail-closed",
        profile_id="deploy",
        secret_ref=SECRET_REF,
        action="mcp.connect",
        resource_id="mcp:okg-workspace",
        delivery_mode="brokered_operation",
        expires_at="2999-01-01T00:00:00Z",
    )
    ssh_request = AgentSshUseRequest(
        subject_binding_id=SUBJECT,
        agent_run_id="agent-run-fail-closed",
        profile_id="deploy",
        target_host="vm.example",
        principal="ubuntu",
        command="hostname",
        workspace_root=tmp_path,
    )
    ssh_grant = AgentSshGrant(
        grant_id="ssh-grant-fail-closed",
        subject_binding_id=SUBJECT,
        agent_run_id="agent-run-fail-closed",
        profile_id="deploy",
        identity_ref=IDENTITY_REF,
        allowed_hosts=("vm.example",),
        allowed_principals=("ubuntu",),
        allowed_commands=("hostname",),
        socket_path=str(
            tmp_path
            / ".okg"
            / "agent-runtime"
            / "agent-run-fail-closed"
            / "ssh"
            / "agent.sock",
        ),
        expires_at="2999-01-01T00:00:00Z",
    )
    bundle = _policy("secret.use", "ssh.use")

    no_secret_policy = evaluate_agent_secret_use(None, secret_request, grant=secret_grant)
    missing_secret_grant = evaluate_agent_secret_use(
        bundle,
        secret_request,
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    literal_secret = evaluate_agent_secret_use(
        bundle,
        replace_secret_config(secret_request, {"env": {"OPENAI_API_KEY": CANARY}}),
        grant=secret_grant,
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    no_ssh_policy = evaluate_agent_ssh_use(None, ssh_request, grant=ssh_grant)
    missing_ssh_grant = evaluate_agent_ssh_use(
        bundle,
        ssh_request,
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    default_socket = evaluate_agent_ssh_use(
        bundle,
        AgentSshUseRequest(
            **{
                **ssh_request.__dict__,
                "ambient_env": {"SSH_AUTH_SOCK": "/private/tmp/ssh-agent.sock"},
            }
        ),
        grant=ssh_grant,
        active_policy_fingerprint=bundle.policy_fingerprint,
    )

    assert no_secret_policy.allowed is False
    assert no_secret_policy.reason_code == "nomos.default_deny.no_active_bundle"
    assert no_secret_policy.resolution == {}
    assert missing_secret_grant.allowed is False
    assert missing_secret_grant.reason_code == "nomos.runtime.secret_grant_missing"
    assert literal_secret.allowed is False
    assert literal_secret.reason_code == "nomos.runtime.secret_ref_required"
    assert no_ssh_policy.allowed is False
    assert no_ssh_policy.reason_code == "nomos.default_deny.no_active_bundle"
    assert no_ssh_policy.env == {}
    assert no_ssh_policy.resolution == {}
    assert missing_ssh_grant.allowed is False
    assert missing_ssh_grant.reason_code == "nomos.runtime.ssh_grant_missing"
    assert default_socket.allowed is False
    assert default_socket.reason_code == "nomos.runtime.ssh_default_socket_exposed"

    serialized = json.dumps(
        {
            "no_secret_policy": no_secret_policy.trace,
            "missing_secret_grant": missing_secret_grant.trace,
            "literal_secret": literal_secret.trace,
            "no_ssh_policy": no_ssh_policy.trace,
            "missing_ssh_grant": missing_ssh_grant.trace,
            "default_socket": default_socket.trace,
        },
        sort_keys=True,
    )
    assert CANARY not in serialized
    assert SECRET_REF not in serialized
    assert IDENTITY_REF not in serialized
    assert "/private/tmp/ssh-agent.sock" not in serialized


def test_docker_and_egress_gates_fail_closed_without_policy_or_review(
    tmp_path: Path,
) -> None:
    docker_request = AgentDockerRunRequest(
        subject_binding_id=SUBJECT,
        agent_run_id="agent-run-fail-closed",
        profile_id="dev",
        workspace_root=tmp_path,
        image="python:3.12-slim",
        command=("python", "-V"),
        network_mode="none",
    )
    egress_request = AgentEgressRequest(
        subject_binding_id=SUBJECT,
        agent_run_id="agent-run-fail-closed",
        profile_id="deploy",
        command=(
            "curl",
            "-X",
            "POST",
            "--upload-file",
            "dist/pkg.tar.gz",
            "https://example.com/artifacts",
        ),
        approved_destinations=("example.com",),
    )
    bundle = _policy("docker.run", "network.egress")

    no_docker_policy = evaluate_agent_docker_run(None, docker_request)
    stale_docker_policy = evaluate_agent_docker_run(
        bundle,
        docker_request,
        active_policy_fingerprint="sha256:stale",
    )
    secret_docker = evaluate_agent_docker_run(
        bundle,
        AgentDockerRunRequest(
            **{
                **docker_request.__dict__,
                "env": {"OPENAI_API_KEY": CANARY},
            }
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    no_egress_policy = evaluate_agent_egress(None, egress_request)
    missing_egress_grant = evaluate_agent_egress(
        bundle,
        egress_request,
        active_policy_fingerprint=bundle.policy_fingerprint,
    )

    assert no_docker_policy.allowed is False
    assert no_docker_policy.reason_code == "nomos.default_deny.no_active_bundle"
    assert no_docker_policy.command == ()
    assert no_docker_policy.resolution == {}
    assert stale_docker_policy.allowed is False
    assert stale_docker_policy.reason_code == "nomos.runtime.stale_active_state"
    assert secret_docker.allowed is False
    assert secret_docker.reason_code == "nomos.runtime.docker_secret_material_denied"
    assert no_egress_policy.allowed is False
    assert no_egress_policy.reason_code == "nomos.default_deny.no_active_bundle"
    assert no_egress_policy.command == ()
    assert no_egress_policy.resolution == {}
    assert missing_egress_grant.allowed is False
    assert missing_egress_grant.reason_code == "nomos.runtime.network_write_requires_review"

    serialized = json.dumps(
        {
            "no_docker_policy": no_docker_policy.trace,
            "stale_docker_policy": stale_docker_policy.trace,
            "secret_docker": secret_docker.trace,
            "no_egress_policy": no_egress_policy.trace,
            "missing_egress_grant": missing_egress_grant.trace,
        },
        sort_keys=True,
    )
    assert CANARY not in serialized
    assert "dist/pkg.tar.gz" not in serialized
    assert "example.com" not in serialized


def replace_secret_config(
    request: AgentSecretUseRequest,
    agent_visible_config: dict[str, object],
) -> AgentSecretUseRequest:
    return AgentSecretUseRequest(
        subject_binding_id=request.subject_binding_id,
        agent_run_id=request.agent_run_id,
        profile_id=request.profile_id,
        secret_ref=request.secret_ref,
        action=request.action,
        resource_id=request.resource_id,
        delivery_mode=request.delivery_mode,
        deployment=request.deployment,
        deployment_posture=request.deployment_posture,
        agent_visible_config=agent_visible_config,
        policy_generation_pin=request.policy_generation_pin,
        config_generation_pin=request.config_generation_pin,
    )
