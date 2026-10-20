from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from okg.substrate.library.agent_runtimes.secret_ssh import (
    AgentSshGrant,
    AgentSshUseRequest,
    evaluate_agent_ssh_use,
)
from okg.substrate.nomos import compile_policy_graph


SUBJECT = "subject:agent:test"
IDENTITY_REF = "secret://ssh/okg-deploy-key"


def _policy():
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:agent-ssh-gate",
        "policy_version": "v1",
        "vocabulary_strict": True,
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_agent_ssh_use",
                "effect": "allow",
                "actions": ["ssh.use"],
                "subjects": [SUBJECT],
                "priority": 100,
                "reason_code": "nomos.test.agent_ssh_allowed",
            },
        ],
    })


def _deny_policy():
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:agent-ssh-gate-deny",
        "policy_version": "v1",
        "vocabulary_strict": True,
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_different_subject_ssh_use",
                "effect": "allow",
                "actions": ["ssh.use"],
                "subjects": ["subject:agent:other"],
                "priority": 100,
                "reason_code": "nomos.test.other_agent_ssh_allowed",
            },
        ],
    })


def _request(tmp_path: Path, **overrides) -> AgentSshUseRequest:
    base = AgentSshUseRequest(
        subject_binding_id=SUBJECT,
        agent_run_id="agent-run-ssh",
        profile_id="deploy",
        target_host="vm.example",
        principal="ubuntu",
        command="hostname",
        workspace_root=tmp_path,
        deployment="okg-workspace",
        deployment_posture={"deployment_class": "dogfood_workspace"},
    )
    return replace(base, **overrides)


def _socket(tmp_path: Path) -> str:
    return str(tmp_path / ".okg" / "agent-runtime" / "agent-run-ssh" / "ssh" / "agent.sock")


def _grant(tmp_path: Path, **overrides) -> AgentSshGrant:
    base = AgentSshGrant(
        grant_id="ssh-grant-1",
        subject_binding_id=SUBJECT,
        agent_run_id="agent-run-ssh",
        profile_id="deploy",
        identity_ref=IDENTITY_REF,
        allowed_hosts=("vm.example",),
        allowed_principals=("ubuntu",),
        allowed_commands=("hostname",),
        socket_path=_socket(tmp_path),
        expires_at="2999-01-01T00:00:00Z",
    )
    return replace(base, **overrides)


def test_ssh_gate_denies_default_socket_and_ungranted_use(tmp_path: Path) -> None:
    bundle = _policy()
    default_socket = "/private/tmp/com.apple.launchd.agent/Listeners"
    default_socket_parent = str(Path(default_socket).parent)

    default_exposed = evaluate_agent_ssh_use(
        bundle,
        _request(
            tmp_path,
            ambient_env={"SSH_AUTH_SOCK": default_socket},
            discovered_socket_paths=(),
            docker_mounts=(
                f"{default_socket}:/agent/ssh.sock:ro",
                f"type=bind,source={default_socket},target=/agent/ssh.sock",
            ),
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    rediscovered_after_env_scrub = evaluate_agent_ssh_use(
        bundle,
        _request(
            tmp_path,
            ambient_env={},
            host_default_ssh_auth_sock=default_socket,
            discovered_socket_paths=(default_socket,),
            docker_mounts=(f"{default_socket_parent}:/host-agent:ro",),
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    untrusted_socket_without_baseline = evaluate_agent_ssh_use(
        bundle,
        _request(
            tmp_path,
            ambient_env={},
            discovered_socket_paths=("/tmp/ssh-agent.sock",),
            docker_mounts=("type=bind,src=/tmp/ssh-agent,target=/agent",),
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    launchd_socket_without_baseline = evaluate_agent_ssh_use(
        bundle,
        _request(
            tmp_path,
            ambient_env={},
            discovered_socket_paths=(default_socket,),
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    launchd_parent_mount_without_baseline = evaluate_agent_ssh_use(
        bundle,
        _request(
            tmp_path,
            ambient_env={},
            docker_mounts=(f"{default_socket_parent}:/host-agent:ro",),
        ),
        grant=_grant(tmp_path),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    missing_grant = evaluate_agent_ssh_use(
        bundle,
        _request(tmp_path),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )

    assert default_exposed.allowed is False
    assert default_exposed.reason_code == "nomos.runtime.ssh_default_socket_exposed"
    assert rediscovered_after_env_scrub.allowed is False
    assert rediscovered_after_env_scrub.reason_code == (
        "nomos.runtime.ssh_default_socket_exposed"
    )
    assert untrusted_socket_without_baseline.allowed is False
    assert untrusted_socket_without_baseline.reason_code == (
        "nomos.runtime.ssh_default_socket_exposed"
    )
    assert launchd_socket_without_baseline.allowed is False
    assert launchd_socket_without_baseline.reason_code == (
        "nomos.runtime.ssh_default_socket_exposed"
    )
    assert launchd_parent_mount_without_baseline.allowed is False
    assert launchd_parent_mount_without_baseline.reason_code == (
        "nomos.runtime.ssh_default_socket_exposed"
    )
    assert missing_grant.allowed is False
    assert missing_grant.reason_code == "nomos.runtime.ssh_grant_missing"
    serialized = json.dumps({
        "default": default_exposed.trace,
        "rediscovered": rediscovered_after_env_scrub.trace,
        "untrusted": untrusted_socket_without_baseline.trace,
        "launchd": launchd_socket_without_baseline.trace,
        "launchd_parent_mount": launchd_parent_mount_without_baseline.trace,
    }, sort_keys=True)
    assert default_socket not in serialized
    assert default_socket_parent not in serialized
    assert "path_hash" in serialized


def test_ssh_gate_allows_isolated_task_socket_and_scoped_constraints(
    tmp_path: Path,
) -> None:
    bundle = _policy()

    allowed = evaluate_agent_ssh_use(
        bundle,
        _request(tmp_path),
        grant=_grant(tmp_path),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    wrong_host = evaluate_agent_ssh_use(
        bundle,
        _request(tmp_path, target_host="other.example"),
        grant=_grant(tmp_path),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    expired = evaluate_agent_ssh_use(
        bundle,
        _request(tmp_path),
        grant=_grant(tmp_path, expires_at="2000-01-01T00:00:00Z"),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    revoked = evaluate_agent_ssh_use(
        bundle,
        _request(tmp_path),
        grant=_grant(tmp_path, status="revoked"),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )

    assert allowed.allowed is True
    assert allowed.env == {"OKG_AGENT_SSH_GRANT": "ssh-grant-1"}
    assert allowed.resolution["socket_scope"] == "agent_run"
    assert allowed.resolution["socket_delivery"] == "wrapper_owned_subprocess"
    assert allowed.resolution["wrapper_command"] == "okg agent-ssh-run"
    assert wrong_host.allowed is False
    assert wrong_host.reason_code == "nomos.runtime.ssh_grant_constraint_mismatch"
    assert expired.allowed is False
    assert expired.reason_code == "nomos.runtime.grant_expired"
    assert revoked.allowed is False
    assert revoked.reason_code == "nomos.runtime.grant_closed"
    serialized = json.dumps({
        "trace": allowed.trace,
        "resolution": allowed.resolution,
    }, sort_keys=True)
    assert IDENTITY_REF not in serialized
    assert _socket(tmp_path) not in serialized


def test_ssh_gate_rejects_unscoped_or_vanilla_grants(tmp_path: Path) -> None:
    bundle = _policy()

    host_unconstrained = evaluate_agent_ssh_use(
        bundle,
        _request(tmp_path),
        grant=_grant(tmp_path, allowed_hosts=()),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    vanilla_agent = evaluate_agent_ssh_use(
        bundle,
        _request(tmp_path),
        grant=_grant(tmp_path, mechanism="vanilla_ssh_agent"),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    global_socket = evaluate_agent_ssh_use(
        bundle,
        _request(tmp_path),
        grant=_grant(tmp_path, socket_path="/private/tmp/ssh-agent.sock"),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    invalid_identity_ref = evaluate_agent_ssh_use(
        bundle,
        _request(tmp_path),
        grant=_grant(tmp_path, identity_ref="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQ"),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )

    assert host_unconstrained.allowed is False
    assert host_unconstrained.reason_code == (
        "nomos.runtime.ssh_grant_constraint_mismatch"
    )
    assert vanilla_agent.allowed is False
    assert vanilla_agent.reason_code == "nomos.runtime.ssh_isolation_required"
    assert global_socket.allowed is False
    assert global_socket.reason_code == "nomos.runtime.ssh_socket_scope_invalid"
    assert invalid_identity_ref.allowed is False
    assert invalid_identity_ref.reason_code == "nomos.runtime.ssh_identity_ref_required"
    assert invalid_identity_ref.resolution == {}


def test_ssh_gate_suppresses_socket_resolution_when_policy_denies(
    tmp_path: Path,
) -> None:
    bundle = _deny_policy()

    result = evaluate_agent_ssh_use(
        bundle,
        _request(tmp_path),
        grant=_grant(tmp_path),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )

    assert result.allowed is False
    assert result.reason_code == "nomos.default_deny"
    assert result.env == {}
    assert result.resolution == {}
