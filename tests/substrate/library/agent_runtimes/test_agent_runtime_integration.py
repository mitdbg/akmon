from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from okg.substrate.library.agent_runtimes.docker_egress import (
    AgentDockerMount,
    AgentDockerRunRequest,
    AgentEgressGrant,
    AgentEgressRequest,
    evaluate_agent_docker_run,
    evaluate_agent_egress,
)
from okg.substrate.library.agent_runtimes.launcher import (
    AGENT_RUNTIME_PROFILES,
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
from okg.substrate.library.sources._transcript_redaction import (
    build_transcript_retention_queue,
)
from okg.substrate.library.sources.agent_runtime_security_monitor import (
    AgentRuntimeSecurityMonitorSource,
)
from okg.substrate.nomos import compile_policy_graph
from tests.mirror_scope import private_path
from tests.substrate._fake_credentials import fake_credential


SUBJECT = "subject:agent:integration"
CANARY = fake_credential("sk-", "agent-runtime-integration-DO-NOT-PERSIST")
SECRET_REF = "secret://okg/integration/openai-token"
IDENTITY_REF = "secret://ssh/okg-integration-key"


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
        "policy_bundle_id": "nomos:test:agent-runtime-integration",
        "policy_version": "v1",
        "vocabulary_strict": True,
        "default_effect": "deny",
        "rules": rules,
    })


def _launch_request(tmp_path: Path, *, profile_id: str) -> AgentLaunchRequest:
    return AgentLaunchRequest(
        runtime="codex",
        profile_id=profile_id,
        command=("codex", "--model", "gpt-5"),
        workspace_root=tmp_path,
        subject_binding_id=SUBJECT,
        deployment="okg-workspace",
        operator_id="operator:example",
        run_id=f"agent-run-{profile_id}",
        ambient_env={
            "PATH": "/usr/bin:/bin",
            "LANG": "C.UTF-8",
            "OPENAI_API_KEY": CANARY,
            "DATABASE_URL": f"postgres://okg:{CANARY}@localhost/okg",
            "SSH_AUTH_SOCK": "/private/tmp/com.apple.launchd.agent/Listeners",
            "CLAUDE_MCP_CONFIG": f'{{"env":{{"OKG_DSN":"postgres://{CANARY}"}}}}',
            "HOME": "/Users/example",
        },
        deployment_posture={"deployment_class": "dogfood_workspace"},
        policy_generation_pin="policy:generation:integration",
        config_generation_pin="config:generation:integration",
    )


def test_agent_runtime_children_compose_without_ambient_authority(
    tmp_path: Path,
) -> None:
    bundle = _policy(
        "agent.launch",
        "secret.use",
        "ssh.use",
        "docker.run",
        "network.egress",
    )
    plans = {
        profile_id: build_agent_launch_plan(
            bundle,
            _launch_request(tmp_path, profile_id=profile_id),
            active_policy_fingerprint=bundle.policy_fingerprint,
        )
        for profile_id in AGENT_RUNTIME_PROFILES
    }

    for profile_id, plan in plans.items():
        assert plan.launch_decision.allowed is True
        assert plan.env["OKG_AGENT_RUNTIME_PROFILE"] == profile_id
        assert plan.env["OKG_AGENT_SUBJECT"] == SUBJECT
        assert plan.env["HOME"].startswith(str(tmp_path / ".okg" / "agent-runtime"))
        assert "OPENAI_API_KEY" not in plan.env
        assert "DATABASE_URL" not in plan.env
        assert "SSH_AUTH_SOCK" not in plan.env
        assert "CLAUDE_MCP_CONFIG" not in plan.env
        assert {item["name"] for item in plan.redacted_env_report} >= {
            "OPENAI_API_KEY",
            "DATABASE_URL",
            "SSH_AUTH_SOCK",
            "CLAUDE_MCP_CONFIG",
            "HOME",
        }

    deploy = plans["deploy"]
    deploy_secret_run = require_profiled_agent_run(deploy.env, effect="secret.use")
    secret_allowed = evaluate_agent_secret_use(
        bundle,
        AgentSecretUseRequest(
            subject_binding_id=deploy_secret_run.subject_binding_id,
            agent_run_id=deploy_secret_run.agent_run_id,
            profile_id=deploy_secret_run.profile_id,
            secret_ref=SECRET_REF,
            action="mcp.connect",
            resource_id="mcp:okg-workspace",
            delivery_mode="brokered_operation",
            deployment="okg-workspace",
            deployment_posture={"deployment_class": "dogfood_workspace"},
            agent_visible_config={
                "env": {"OPENAI_API_KEY": {"secret_ref": SECRET_REF}},
                "argv": ["okg", "mcp-serve"],
            },
        ),
        grant=AgentSecretGrant(
            grant_id="secret-grant-integration",
            subject_binding_id=deploy_secret_run.subject_binding_id,
            agent_run_id=deploy_secret_run.agent_run_id,
            profile_id=deploy_secret_run.profile_id,
            secret_ref=SECRET_REF,
            action="mcp.connect",
            resource_id="mcp:okg-workspace",
            delivery_mode="brokered_operation",
            expires_at="2999-01-01T00:00:00Z",
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    assert secret_allowed.allowed is True
    assert secret_allowed.resolution["value_materialized"] is False

    deploy_ssh_run = require_profiled_agent_run(deploy.env, effect="ssh.use")
    ssh_allowed = evaluate_agent_ssh_use(
        bundle,
        AgentSshUseRequest(
            subject_binding_id=deploy_ssh_run.subject_binding_id,
            agent_run_id=deploy_ssh_run.agent_run_id,
            profile_id=deploy_ssh_run.profile_id,
            target_host="vm.example",
            principal="ubuntu",
            command="hostname",
            workspace_root=tmp_path,
            deployment="okg-workspace",
            deployment_posture={"deployment_class": "dogfood_workspace"},
        ),
        grant=AgentSshGrant(
            grant_id="ssh-grant-integration",
            subject_binding_id=deploy_ssh_run.subject_binding_id,
            agent_run_id=deploy_ssh_run.agent_run_id,
            profile_id=deploy_ssh_run.profile_id,
            identity_ref=IDENTITY_REF,
            allowed_hosts=("vm.example",),
            allowed_principals=("ubuntu",),
            allowed_commands=("hostname",),
            socket_path=str(
                tmp_path
                / ".okg"
                / "agent-runtime"
                / deploy_ssh_run.agent_run_id
                / "ssh"
                / "agent.sock",
            ),
            expires_at="2999-01-01T00:00:00Z",
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    assert ssh_allowed.allowed is True
    assert ssh_allowed.env == {"OKG_AGENT_SSH_GRANT": "ssh-grant-integration"}
    assert ssh_allowed.resolution["socket_scope"] == "agent_run"

    dev_docker_run = require_profiled_agent_run(plans["dev"].env, effect="docker.run")
    docker_allowed = evaluate_agent_docker_run(
        bundle,
        AgentDockerRunRequest(
            subject_binding_id=dev_docker_run.subject_binding_id,
            agent_run_id=dev_docker_run.agent_run_id,
            profile_id=dev_docker_run.profile_id,
            workspace_root=dev_docker_run.workspace_root,
            image="python:3.12-slim",
            command=("python", "-V"),
            env={"PUBLIC_FLAG": "1"},
            mounts=(
                AgentDockerMount(source=str(tmp_path), target="/workspace", mode="ro"),
            ),
            network_mode="none",
            deployment="okg-workspace",
            deployment_posture={"deployment_class": "dogfood_workspace"},
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    assert docker_allowed.allowed is True
    assert docker_allowed.resolution["precreation_decision"] is True
    assert docker_allowed.resolution["container_created"] is False

    dev_egress_run = require_profiled_agent_run(plans["dev"].env, effect="network.egress")
    routine_read = evaluate_agent_egress(
        bundle,
        AgentEgressRequest(
            subject_binding_id=dev_egress_run.subject_binding_id,
            agent_run_id=dev_egress_run.agent_run_id,
            profile_id=dev_egress_run.profile_id,
            command=("curl", "https://docs.python.org/3/"),
            deployment="okg-workspace",
            deployment_posture={"deployment_class": "dogfood_workspace"},
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    assert routine_read.allowed is True
    assert routine_read.resolution["payload_materialized"] is False

    deploy_egress_run = require_profiled_agent_run(deploy.env, effect="network.egress")
    approved_upload = evaluate_agent_egress(
        bundle,
        AgentEgressRequest(
            subject_binding_id=deploy_egress_run.subject_binding_id,
            agent_run_id=deploy_egress_run.agent_run_id,
            profile_id=deploy_egress_run.profile_id,
            command=(
                "curl",
                "-X",
                "POST",
                "--upload-file",
                "dist/pkg.tar.gz",
                "https://example.com/artifacts",
            ),
            approved_destinations=("example.com",),
            deployment="okg-workspace",
            deployment_posture={"deployment_class": "dogfood_workspace"},
        ),
        grant=AgentEgressGrant(
            grant_id="egress-grant-integration",
            subject_binding_id=deploy_egress_run.subject_binding_id,
            agent_run_id=deploy_egress_run.agent_run_id,
            profile_id=deploy_egress_run.profile_id,
            allowed_hosts=("example.com",),
            command_classes=("curl_upload",),
            expires_at="2999-01-01T00:00:00Z",
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    assert approved_upload.allowed is True
    assert approved_upload.resolution["egress_grant_id"] == "egress-grant-integration"

    transcript = tmp_path / "transcripts" / "session.jsonl"
    transcript.parent.mkdir()
    transcript.write_text(
        json.dumps({"cmd": f"API_KEY={CANARY}"}),
        encoding="utf-8",
    )
    retention_queue = build_transcript_retention_queue([transcript])
    assert retention_queue[0]["cleanup_mode"] == "report_only"
    assert retention_queue[0]["delete_without_operator_approval"] is False

    monitor_run = AgentRuntimeSecurityMonitorSource(
        repo="okg-workspace",
        snapshots=[
            {
                "observed_at": "2026-07-03T12:00:00Z",
                "processes": [
                    {
                        "pid": 100,
                        "argv": ["codex", "--api-key", CANARY],
                        "env": {"OPENAI_API_KEY": CANARY},
                    }
                ],
                "listeners": [
                    {
                        "service": "/Users/example/.codex/worktrees/old/server",
                        "bind_address": "0.0.0.0",
                        "port": 7331,
                        "approved": False,
                        "owner": "unknown",
                    }
                ],
            }
        ],
    ).run("integration", mode="scope_complete")
    monitor_facts = list(monitor_run.facts)
    findings = [fact.attrs for fact in monitor_facts]
    assert {
        "agent_runtime.secret_env_or_argv",
        "agent_runtime.public_listener",
    } <= {finding["rule_id"] for finding in findings}

    serialized = json.dumps(
        {
            "plans": [
                {
                    "trace": plan.trace,
                    "decision_trace": plan.launch_decision.trace,
                }
                for plan in plans.values()
            ],
            "secret": {"trace": secret_allowed.trace, "resolution": secret_allowed.resolution},
            "ssh": {"trace": ssh_allowed.trace, "resolution": ssh_allowed.resolution},
            "docker": {"trace": docker_allowed.trace, "resolution": docker_allowed.resolution},
            "egress": {
                "routine": routine_read.trace,
                "upload": approved_upload.trace,
                "upload_resolution": approved_upload.resolution,
            },
            "retention": retention_queue,
            "monitor": {
                "attrs": findings,
                "cursor": monitor_run.next_cursor,
                "record_set": monitor_run.record_set.to_cursor()
                if monitor_run.record_set else {},
                "revisions": [fact.source_revision for fact in monitor_facts],
            },
        },
        sort_keys=True,
        default=str,
    )
    assert CANARY not in serialized
    assert SECRET_REF not in serialized
    assert IDENTITY_REF not in serialized
    assert "postgres://okg:" not in serialized
    assert "dist/pkg.tar.gz" not in serialized
    assert "docs.python.org" not in serialized
    assert "example.com" not in serialized
    assert str(transcript) not in serialized
    assert "/Users/example/.codex/worktrees/old" not in serialized


def test_workspace_doctor_reports_agent_runtime_service_posture_without_leaks() -> None:
    """The okg-workspace doctor arm, split out of the test above.

    deployments/okg-workspace/doctor_checks.py is this repository's own
    deployment; the public copy skips this test (tests/mirror_scope.py).
    """
    service_posture = _doctor_checks()._okg_workspace_agent_runtime_service_posture_from_records(
        [
            {
                "kind": "launch_agent",
                "label": "/Users/example/.codex/worktrees/old/server",
                "working_directory": "/Users/example/.codex/worktrees/old",
                "program_arguments": [f"postgres://okg:{CANARY}@localhost/okg"],
                "age_days": 45,
                "stale": True,
            }
        ]
    )
    assert {item["code"] for item in service_posture["violations"]} >= {
        "stale_worktree_launchagent",
        "dsn_in_program_arguments",
        "unknown_owner",
    }
    doctor = _doctor_checks()
    failing_payload = doctor._okg_workspace_doctor_payload(
        {
            **_healthy_snapshot(),
            "agent_runtime_service_posture": service_posture,
        }
    )
    approved_loopback_payload = doctor._okg_workspace_doctor_payload(
        {
            **_healthy_snapshot(),
            "agent_runtime_service_posture": doctor._okg_workspace_agent_runtime_service_posture_from_records(
                [
                    {
                        "kind": "listener",
                        "service": "okg-workspace",
                        "bind_address": "127.0.0.1",
                        "port": 8765,
                        "approved": True,
                        "owner": "example",
                    }
                ]
            ),
        }
    )
    clean_payload = doctor._okg_workspace_doctor_payload(
        {
            **_healthy_snapshot(),
            "agent_runtime_service_posture": {
                "checked_services": 0,
                "coverage_status": "covered",
                "violations": [],
                "warnings": [],
            },
        }
    )
    assert failing_payload["status"] == "FAIL"
    assert approved_loopback_payload["status"] == "WARN"
    assert clean_payload["status"] == "OK"
    assert _check(approved_loopback_payload, "agent_runtime_service_posture")["status"] == "WARN"
    assert _check(clean_payload, "agent_runtime_service_posture")["status"] == "OK"

    serialized = json.dumps(
        {
            "service_posture": service_posture,
            "readiness": {
                "failing_check": _check(failing_payload, "agent_runtime_service_posture"),
                "approved_loopback_check": _check(
                    approved_loopback_payload,
                    "agent_runtime_service_posture",
                ),
                "clean_baseline_check": _check(
                    clean_payload,
                    "agent_runtime_service_posture",
                ),
            },
        },
        sort_keys=True,
        default=str,
    )
    assert CANARY not in serialized


def test_default_development_profile_denies_representative_ambient_effects(
    tmp_path: Path,
) -> None:
    bundle = _policy(
        "agent.launch",
        "agent.bypass",
        "secret.use",
        "ssh.use",
        "docker.run",
        "network.egress",
    )
    plan = build_agent_launch_plan(
        bundle,
        _launch_request(tmp_path, profile_id="dev"),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    with pytest.raises(AgentRuntimeBoundaryError) as bypass_denied:
        build_agent_launch_plan(
            bundle,
            AgentLaunchRequest(
                **{
                    **_launch_request(tmp_path, profile_id="dev").__dict__,
                    "command": (
                        "claude",
                        "--dangerously-skip-permissions",
                        f'--mcp-config={{"env":{{"OKG_DSN":"postgres://{CANARY}"}}}}',
                    ),
                    "run_id": "agent-run-bypass-denied",
                }
            ),
            active_policy_fingerprint=bundle.policy_fingerprint,
        )

    secret_denied = evaluate_agent_secret_use(
        bundle,
        AgentSecretUseRequest(
            subject_binding_id=SUBJECT,
            agent_run_id=plan.agent_run_id,
            profile_id=plan.profile.profile_id,
            secret_ref=f"secret://okg/default/{CANARY}",
            action="mcp.connect",
            resource_id="mcp:okg-workspace",
            agent_visible_config={"env": {"OPENAI_API_KEY": CANARY}},
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    ssh_denied = evaluate_agent_ssh_use(
        bundle,
        AgentSshUseRequest(
            subject_binding_id=SUBJECT,
            agent_run_id=plan.agent_run_id,
            profile_id=plan.profile.profile_id,
            target_host="vm.example",
            principal="ubuntu",
            command="hostname",
            workspace_root=tmp_path,
            ambient_env={"SSH_AUTH_SOCK": "/private/tmp/ssh-agent.sock"},
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    docker_denied = evaluate_agent_docker_run(
        bundle,
        AgentDockerRunRequest(
            subject_binding_id=SUBJECT,
            agent_run_id=plan.agent_run_id,
            profile_id=plan.profile.profile_id,
            workspace_root=tmp_path,
            image="python:3.12-slim",
            env={"OPENAI_API_KEY": CANARY},
            mounts=(AgentDockerMount(source=str(Path.home()), target="/host-home"),),
            network_mode="bridge",
            ambient_env={"SSH_AUTH_SOCK": "/private/tmp/ssh-agent.sock"},
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    upload_denied = evaluate_agent_egress(
        bundle,
        AgentEgressRequest(
            subject_binding_id=SUBJECT,
            agent_run_id=plan.agent_run_id,
            profile_id=plan.profile.profile_id,
            command=(
                "curl",
                "-X",
                "POST",
                "--data-binary",
                "artifact.tar.gz",
                "https://example.com/artifacts",
            ),
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )

    assert secret_denied.allowed is False
    assert secret_denied.reason_code == "nomos.runtime.secret_ref_required"
    assert bypass_denied.value.reason_code == "nomos.runtime.obligation_missing"
    assert bypass_denied.value.trace["break_glass"]["required"] is True
    assert bypass_denied.value.trace["break_glass"]["active"] is False
    assert ssh_denied.allowed is False
    assert ssh_denied.reason_code == "nomos.runtime.ssh_default_socket_exposed"
    assert docker_denied.allowed is False
    assert docker_denied.reason_code == "nomos.runtime.docker_secret_material_denied"
    assert upload_denied.allowed is False
    assert upload_denied.reason_code == "nomos.runtime.network_upload_denied"

    serialized = json.dumps(
        {
            "secret": secret_denied.trace,
            "bypass": bypass_denied.value.trace,
            "ssh": ssh_denied.trace,
            "docker": docker_denied.trace,
            "upload": upload_denied.trace,
        },
        sort_keys=True,
    )
    assert CANARY not in serialized
    assert str(Path.home()) not in serialized
    assert "example.com" not in serialized


def _doctor_checks():
    module_path = private_path("deployments/okg-workspace/doctor_checks.py")
    spec = importlib.util.spec_from_file_location(
        "okg_workspace_doctor_checks_agent_runtime_integration_test",
        module_path,
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _check(payload: dict, name: str) -> dict:
    for check in payload["checks"]:
        if check["name"] == name:
            return check
    raise AssertionError(f"missing check {name!r}")


def _healthy_snapshot() -> dict[str, object]:
    return {
        "deployment": "okg-workspace",
        "generation_id": 2,
        "summary": {
            "embed_lag": 0,
            "unresolved_mentions": {"rate_resolved": 0.95},
        },
        "subtype_counts": {
            "code_symbol": 10,
            "interface_endpoint": 2,
            "data_asset": 3,
            "config_key": 1,
        },
        "edge_pattern_counts": {
            "tool_use_event -> affects -> code_symbol": 3,
            "test_case -> references -> code_symbol": 2,
            "interface_endpoint -> routes_to -> code_symbol": 1,
            "security_finding -> affects -> code_symbol": 1,
            "profile_hotspot -> affects -> code_symbol": 1,
        },
        "language_symbol_counts": {
            "python": 2,
            "javascript": 2,
            "typescript": 2,
            "go": 2,
            "rust": 2,
            "cpp": 2,
        },
        "optional_source_status": {
            "semgrep-data-flow": "imported",
            "codeql-data-flow": "configured",
        },
        "evidence_status": {
            "test-reports": "imported",
            "acceptance-criteria": "linked",
            "security-analysis": "imported",
            "profile-measurements": "imported",
        },
        "agent_context_surface": {
            "runbook_chunk_count": 2,
            "workflow_gate_scenario_count": 5,
            "resolved_gate_finding_count": 5,
            "successful_agent_context_mcp_calls": 1,
        },
        "agent_memory_quality": {
            "supported_without_evidence_count": 0,
            "model_inferred_supported_without_deterministic_count": 0,
            "code_claim_without_code_subject_count": 0,
            "stale_supported_claim_count": 0,
            "packet_warning_count": 0,
        },
        "coordination": {
            "config_snapshot_count": 7,
            "merge_ready_without_review": 0,
            "completed_task_without_verification": 0,
            "handoffs_missing_structured_proof": 0,
            "unclaimed_orders": 0,
            "stale_assignments": 0,
            "stale_role_agent_heartbeats": 0,
            "role_order_mismatches": 0,
            "unauthorized_merge_decisions": 0,
            "merge_decisions_missing_authorization": 0,
            "merge_decisions_missing_review": 0,
            "merge_decisions_with_open_blocking_findings": 0,
            "merge_decisions_missing_final_verification": 0,
            "unauthorized_packet_kinds": 0,
            "invalid_correction_chains": 0,
            "invalid_transitions": 0,
            "invalid_packet_artifacts": 0,
            "missing_git_refs": 0,
            "multiple_active_writers": 0,
            "reader_mutations": 0,
            "merge_ready_dirty_workspaces": 0,
            "missing_fresh_observations": 0,
            "handoff_head_mismatches": 0,
        },
        "record_set_quality": {
            "stale_node_count": 0,
            "stale_edge_count": 0,
            "missing_node_count": 0,
            "missing_edge_count": 0,
            "sources_with_record_set": 1,
        },
        "queues": {
            "dead_letter": 0,
            "oldest_embed_queued_seconds": None,
        },
        "freshness": {},
        "source_runner_thresholds": {},
    }
