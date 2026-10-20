from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.substrate.cli import main as cli_main
from okg.substrate.library.agent_runtimes.launcher import (
    AGENT_RUNTIME_PROFILES,
    AgentLaunchRequest,
    AgentRuntimeBoundaryError,
    break_glass_scope_for_unsafe_request,
    build_agent_launch_plan,
    build_profiled_mcp_server_entry,
    detect_unsafe_agent_launch,
    require_profiled_agent_run,
)
from okg.substrate.nomos import compile_policy_graph


SUBJECT = "subject:agent:test"


def _policy(*actions: str):
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:agent-runtime-launcher",
        "policy_version": "v1",
        "vocabulary_strict": True,
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": f"allow_{action.replace('.', '_')}",
                "effect": "allow",
                "actions": [action],
                "subjects": [SUBJECT],
                "priority": 100,
                "reason_code": "nomos.test.agent_runtime_allowed",
            }
            for action in actions
        ],
    })


def _request(
    tmp_path: Path,
    *,
    profile_id: str = "dev",
    command: tuple[str, ...] = ("codex", "--model", "gpt-5"),
    run_id: str = "agent-run-test",
    ambient_env: dict[str, str] | None = None,
    extra_env: dict[str, str] | None = None,
    break_glass_grant: dict | None = None,
) -> AgentLaunchRequest:
    return AgentLaunchRequest(
        runtime="codex",
        profile_id=profile_id,
        command=command,
        workspace_root=tmp_path,
        subject_binding_id=SUBJECT,
        deployment="okg-workspace",
        operator_id="operator:example",
        run_id=run_id,
        ambient_env=ambient_env or {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
        extra_env=extra_env or {},
        policy_generation_pin="policy:generation:agent-runtime-test",
        config_generation_pin="config:generation:agent-runtime-test",
        deployment_posture={
            "deployment_class": "dogfood_workspace",
            "source": "test",
        },
        break_glass_grant=break_glass_grant,
    )


def test_launcher_profiles_emit_run_metadata_and_reject_unknown_profiles(
    tmp_path: Path,
) -> None:
    bundle = _policy("agent.launch")
    for profile_id in AGENT_RUNTIME_PROFILES:
        plan = build_agent_launch_plan(
            bundle,
            _request(tmp_path, profile_id=profile_id),
        )

        assert plan.agent_run_id == "agent-run-test"
        assert plan.env["OKG_AGENT_RUN_ID"] == "agent-run-test"
        assert plan.env["OKG_AGENT_RUNTIME_PROFILE"] == profile_id
        assert plan.env["OKG_AGENT_SUBJECT"] == SUBJECT
        assert plan.env["OKG_WORKSPACE_ROOT"] == str(tmp_path)
        assert plan.audit_trace_ref.startswith("agent-runtime:trace:")
        assert plan.launch_decision.allowed is True

    with pytest.raises(AgentRuntimeBoundaryError) as exc_info:
        build_agent_launch_plan(
            bundle,
            _request(tmp_path, profile_id="root-shell"),
        )
    assert exc_info.value.reason_code == "agent_runtime.unknown_profile"


def test_clean_environment_strips_ambient_credentials(tmp_path: Path) -> None:
    bundle = _policy("agent.launch")
    ambient = {
        "PATH": "/usr/bin:/bin",
        "OPENAI_API_KEY": "sk-live-raw-agent-secret",
        "DATABASE_URL": "postgres://postgres:secret@localhost/okg",
        "COOKIE_PATH": "/Users/example/Library/Cookies",
        "X509_USER_PROXY": "/tmp/x509-proxy",
        "SSH_AUTH_SOCK": "/private/tmp/ssh-agent.sock",
        "CLAUDE_MCP_CONFIG": '{"OKG_DSN":"postgres://raw-secret"}',
        "BROWSER_PROFILE_PATH": "/Users/example/Library/Application Support/Chrome",
        "HOME": "/Users/example",
    }

    plan = build_agent_launch_plan(
        bundle,
        _request(tmp_path, profile_id="dev", ambient_env=ambient),
    )

    for key in ambient:
        if key not in {"PATH", "HOME"}:
            assert key not in plan.env
    assert plan.env["PATH"] == "/usr/bin:/bin"
    assert plan.env["HOME"].startswith(str(tmp_path / ".okg" / "agent-runtime"))
    assert plan.env["HOME"] != "/Users/example"
    dropped = {item["name"]: item["class"] for item in plan.redacted_env_report}
    assert dropped["OPENAI_API_KEY"] == "token_like"
    assert dropped["DATABASE_URL"] == "dsn"
    assert dropped["SSH_AUTH_SOCK"] == "ssh_agent_socket"
    assert dropped["CLAUDE_MCP_CONFIG"] == "private_mcp_config"

    serialized = json.dumps({
        "trace": plan.trace,
        "decision": plan.launch_decision.trace,
    }, sort_keys=True)
    assert "sk-live-raw-agent-secret" not in serialized
    assert "postgres://raw-secret" not in serialized
    assert "/Users/example/Library/Cookies" not in serialized


def test_profile_scoped_role_dsns_are_explicit_extra_env(tmp_path: Path) -> None:
    bundle = _policy("agent.launch")
    ro_dsn = "postgres://okg_mcp:secret@managed.example/okg"
    rw_dsn = "postgres://app_rw:secret@managed.example/okg"

    read_only = build_agent_launch_plan(
        bundle,
        _request(
            tmp_path,
            profile_id="read-only",
            extra_env={
                "OKG_MCP_RO_DSN": ro_dsn,
                "OKG_MCP_RW_DSN": rw_dsn,
            },
        ),
    )
    assert read_only.env["OKG_MCP_RO_DSN"] == ro_dsn
    assert "OKG_MCP_RW_DSN" not in read_only.env
    dropped = {item["name"]: item["class"] for item in read_only.redacted_env_report}
    assert dropped["OKG_MCP_RW_DSN"] == "profile_capability"

    deploy = build_agent_launch_plan(
        bundle,
        _request(
            tmp_path,
            profile_id="deploy",
            extra_env={
                "OKG_MCP_RO_DSN": ro_dsn,
                "OKG_MCP_RW_DSN": rw_dsn,
            },
        ),
    )
    assert deploy.env["OKG_MCP_RO_DSN"] == ro_dsn
    assert deploy.env["OKG_MCP_RW_DSN"] == rw_dsn

    serialized = json.dumps({
        "read_only": read_only.trace,
        "deploy": deploy.trace,
    }, sort_keys=True)
    assert ro_dsn not in serialized
    assert rw_dsn not in serialized


def test_runtime_decision_context_shape(tmp_path: Path) -> None:
    bundle = _policy("agent.launch")
    plan = build_agent_launch_plan(
        bundle,
        _request(
            tmp_path,
            ambient_env={
                "PATH": "/usr/bin:/bin",
                "RAW_TOKEN": "token-raw-secret",
            },
        ),
    )

    decision = plan.launch_decision.decision
    context = decision.context
    caller_context = context["caller_context"]

    assert decision.action == "agent.launch"
    assert decision.resource["resource_kind"] == "agent_runtime"
    assert context["subject_binding_id"] == SUBJECT
    assert context["principal_kind"] == "agent"
    assert context["agent_run_id"] == "agent-run-test"
    assert context["profile_id"] == "dev"
    assert context["deployment_posture"]["deployment_class"] == "dogfood_workspace"
    assert context["policy_generation_pin"] == "policy:generation:agent-runtime-test"
    assert context["config_generation_pin"] == "config:generation:agent-runtime-test"
    assert context["caller_enforcement_point"] == "agent.launch"
    assert caller_context["network_class"] == "local_dev"
    assert caller_context["command"]["argv_hash"].startswith("sha256:")

    serialized = json.dumps(plan.trace, sort_keys=True)
    assert "token-raw-secret" not in serialized
    assert plan.trace["launch_decision_ref"].startswith("nomos:trace:")


def test_mcp_serve_child_command_is_not_inline_mcp_config() -> None:
    assert detect_unsafe_agent_launch(
        runtime="codex",
        command=("okg", "mcp-serve", "--deployment", "okg-workspace"),
    ) == ()


@pytest.mark.parametrize(
    "command,expected_flag",
    [
        (
            ("codex", "--dangerously-bypass-approvals-and-sandbox"),
            "--dangerously-bypass-approvals-and-sandbox",
        ),
        (
            ("codex", "--dangerously-bypass-hook-trust"),
            "--dangerously-bypass-hook-trust",
        ),
        (
            ("claude", "--allow-dangerously-skip-permissions"),
            "--allow-dangerously-skip-permissions",
        ),
        (
            ("claude", "--permission-mode", "bypassPermissions"),
            "--permission-mode",
        ),
        (
            ("claude", "--permission-mode=bypassPermissions"),
            "--permission-mode",
        ),
        (
            ("codex", "--sandbox", "danger-full-access"),
            "--sandbox",
        ),
        (
            ("codex", "--sandbox=danger-full-access"),
            "--sandbox",
        ),
        (
            ("codex", "-s", "danger-full-access"),
            "--sandbox",
        ),
        (
            ("codex", "--ask-for-approval", "never"),
            "--ask-for-approval",
        ),
        (
            ("codex", "--ask-for-approval=never"),
            "--ask-for-approval",
        ),
        (
            ("codex", "-a", "never"),
            "--ask-for-approval",
        ),
        (
            ("codex", "-c", 'sandbox="danger-full-access"'),
            "--config:sandbox",
        ),
        (
            ("codex", "--config", 'approval_policy="never"'),
            "--config:approval",
        ),
        (
            ("codex", '--config=sandbox="danger-full-access"'),
            "--config:sandbox",
        ),
    ],
)
def test_current_unsafe_flags_require_break_glass(
    tmp_path: Path,
    command: tuple[str, ...],
    expected_flag: str,
) -> None:
    findings = detect_unsafe_agent_launch(runtime="codex", command=command)
    assert any(
        finding["class"] == "dangerous_permission_flag"
        and finding["flag"] == expected_flag
        for finding in findings
    )

    with pytest.raises(AgentRuntimeBoundaryError) as denied:
        build_agent_launch_plan(
            _policy("agent.launch", "agent.bypass"),
            _request(tmp_path, command=command, run_id="agent-run-unsafe"),
        )
    assert denied.value.reason_code == "nomos.runtime.obligation_missing"


def test_bypass_requires_break_glass(tmp_path: Path) -> None:
    bundle = _policy("agent.launch", "agent.bypass")
    command = (
        "claude",
        "--dangerously-skip-permissions",
        '--mcp-config={"env":{"OKG_DSN":"postgres://secret"}}',
    )

    with pytest.raises(AgentRuntimeBoundaryError) as denied:
        build_agent_launch_plan(
            bundle,
            _request(tmp_path, command=command, run_id="agent-run-bypass"),
        )
    assert denied.value.reason_code == "nomos.runtime.obligation_missing"
    assert denied.value.trace["break_glass"]["required"] is True
    assert denied.value.trace["break_glass"]["active"] is False

    grant = {
        "operator": "operator:example",
        "subject": SUBJECT,
        "agent_run_id": "agent-run-bypass",
        "profile_id": "dev",
        "scope": break_glass_scope_for_unsafe_request(
            runtime="codex",
            profile_id="dev",
            unsafe_findings=detect_unsafe_agent_launch(
                runtime="codex",
                command=command,
            ),
        ),
        "reason": "fixture approval",
        "decision_id": "nomos:decision:breakglass",
        "follow_up_review_state": "open",
        "expires_at": "2999-01-01T00:00:00Z",
    }
    plan = build_agent_launch_plan(
        bundle,
        _request(
            tmp_path,
            command=command,
            run_id="agent-run-bypass",
            break_glass_grant=grant,
        ),
    )

    assert plan.env["OKG_AGENT_BREAK_GLASS"] == "1"
    assert plan.bypass_decision is not None
    assert plan.bypass_decision.allowed is True
    assert plan.break_glass_finding is not None
    assert plan.break_glass_finding["severity"] == "high"
    serialized = json.dumps({
        "trace": plan.trace,
        "bypass": plan.bypass_decision.trace,
    }, sort_keys=True)
    assert "postgres://secret" not in serialized
    assert "fixture approval" in serialized


def test_break_glass_grant_must_match_operator_profile_and_scope(
    tmp_path: Path,
) -> None:
    bundle = _policy("agent.launch", "agent.bypass")
    command = ("claude", "--dangerously-skip-permissions")
    grant = {
        "operator": "operator:someone-else",
        "subject": SUBJECT,
        "agent_run_id": "agent-run-bypass",
        "profile_id": "deploy",
        "scope": "agent-runtime:codex:dev:unsafe-mode:wrong",
        "reason": "fixture approval",
        "decision_id": "nomos:decision:breakglass",
        "follow_up_review_state": "open",
        "expires_at": "2999-01-01T00:00:00Z",
    }

    with pytest.raises(AgentRuntimeBoundaryError) as exc_info:
        build_agent_launch_plan(
            bundle,
            _request(
                tmp_path,
                command=command,
                run_id="agent-run-bypass",
                break_glass_grant=grant,
            ),
        )

    assert exc_info.value.reason_code == (
        "agent_runtime.break_glass_grant_mismatch"
    )
    assert set(exc_info.value.trace["mismatched_fields"]) == {
        "operator", "profile_id", "scope",
    }


def test_break_glass_grant_must_be_object(tmp_path: Path) -> None:
    bundle = _policy("agent.launch", "agent.bypass")

    with pytest.raises(AgentRuntimeBoundaryError) as exc_info:
        build_agent_launch_plan(
            bundle,
            _request(
                tmp_path,
                command=("claude", "--dangerously-skip-permissions"),
                break_glass_grant=["not", "an", "object"],  # type: ignore[arg-type]
            ),
        )

    assert exc_info.value.reason_code == "agent_runtime.break_glass_grant_invalid"


def test_unprofiled_sensitive_guard_denies_and_profiled_env_passes(
    tmp_path: Path,
) -> None:
    with pytest.raises(AgentRuntimeBoundaryError) as exc_info:
        require_profiled_agent_run({}, effect="docker.run")
    assert exc_info.value.reason_code == "agent_runtime.unprofiled_run"

    plan = build_agent_launch_plan(
        _policy("agent.launch"),
        _request(tmp_path),
    )
    profiled = require_profiled_agent_run(plan.env, effect="docker.run")

    assert profiled.agent_run_id == "agent-run-test"
    assert profiled.profile_id == "dev"
    assert profiled.subject_binding_id == SUBJECT
    assert profiled.effect == "docker.run"


def test_profiled_mcp_server_entry_carries_role_dsns_only(tmp_path: Path) -> None:
    ro_dsn = "postgres://okg_mcp:secret@managed.example/okg"
    rw_dsn = "postgres://app_rw:secret@managed.example/okg"

    read_only = build_profiled_mcp_server_entry(
        runtime="codex",
        deployment="okg-workspace",
        dsn="postgres://owner:secret@managed.example/okg",
        workspace_root=tmp_path,
        extra_env={
            "OKG_MCP_RO_DSN": ro_dsn,
            "OKG_MCP_RW_DSN": rw_dsn,
        },
    )
    assert read_only["env"]["OKG_AGENT_RUNTIME_PROFILE"] == "read-only"
    assert read_only["env"]["OKG_MCP_RO_DSN"] == ro_dsn
    assert "OKG_MCP_RW_DSN" not in read_only["env"]
    assert "OKG_DSN" not in read_only["env"]
    assert "mcp-serve" in read_only["args"][read_only["args"].index("--") + 1:]

    deploy = build_profiled_mcp_server_entry(
        runtime="codex",
        deployment="okg-workspace",
        dsn="postgres://owner:secret@managed.example/okg",
        workspace_root=tmp_path,
        extra_env={
            "OKG_MCP_ALLOW_WRITES": "1",
            "OKG_MCP_RO_DSN": ro_dsn,
            "OKG_MCP_RW_DSN": rw_dsn,
        },
    )
    assert deploy["env"]["OKG_AGENT_RUNTIME_PROFILE"] == "deploy"
    assert deploy["env"]["OKG_MCP_RO_DSN"] == ro_dsn
    assert deploy["env"]["OKG_MCP_RW_DSN"] == rw_dsn
    assert "OKG_DSN" not in deploy["env"]
    child = deploy["args"][deploy["args"].index("--") + 1:]
    assert "mcp-serve" in child
    assert "--allow-writes" in child


def test_agent_runtime_launch_cli_dry_run(tmp_path: Path, capsys) -> None:
    rc = cli_main.main([
        "--json",
        "agent-runtime-launch",
        "--runtime",
        "shell-agent",
        "--permissions",
        "dev",
        "--deployment",
        "okg-workspace",
        "--workspace-root",
        str(tmp_path),
        "--run-id",
        "agent-run-cli",
        "--dry-run",
        "--",
        "python",
        "-c",
        "print('ok')",
    ])

    assert rc == 0
    output = capsys.readouterr().out
    payload = json.loads(output)
    assert payload["schema"] == "okg.agent_runtime.launch_plan.v1"
    assert payload["agent_run_id"] == "agent-run-cli"
    assert payload["profile_id"] == "dev"
    assert payload["command"]["argv_hash"].startswith("sha256:")
    assert "print('ok')" not in output
    assert payload["trace"]["launch_decision_ref"].startswith("nomos:trace:")


def test_agent_runtime_launch_cli_preserves_profiled_role_dsns(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ro_dsn = "postgres://okg_mcp:secret@managed.example/okg"
    rw_dsn = "postgres://app_rw:secret@managed.example/okg"
    monkeypatch.setenv("OKG_MCP_RO_DSN", ro_dsn)
    monkeypatch.setenv("OKG_MCP_RW_DSN", rw_dsn)

    rc = cli_main.main([
        "--json",
        "agent-runtime-launch",
        "--runtime",
        "codex",
        "--permissions",
        "deploy",
        "--deployment",
        "okg-workspace",
        "--workspace-root",
        str(tmp_path),
        "--run-id",
        "agent-run-cli",
        "--dry-run",
        "--",
        "okg",
        "mcp-serve",
        "--deployment",
        "okg-workspace",
        "--allow-writes",
    ])

    assert rc == 0
    output = capsys.readouterr().out
    payload = json.loads(output)
    assert "OKG_MCP_RO_DSN" in payload["env_keys"]
    assert "OKG_MCP_RW_DSN" in payload["env_keys"]
    assert "mcp-serve" not in json.dumps(payload["trace"])
    assert ro_dsn not in output
    assert rw_dsn not in output


def test_agent_runtime_launch_cli_denies_inline_break_glass(
    tmp_path: Path,
    capsys,
) -> None:
    rc = cli_main.main([
        "--json",
        "agent-runtime-launch",
        "--runtime",
        "claude-code",
        "--permissions",
        "dev",
        "--deployment",
        "okg-workspace",
        "--workspace-root",
        str(tmp_path),
        "--run-id",
        "agent-run-cli",
        "--break-glass-grant-json",
        json.dumps({
            "operator": "operator:anyone",
            "subject": "subject:agent:okg-workspace:claude-code",
            "agent_run_id": "agent-run-cli",
            "profile_id": "dev",
            "scope": "arbitrary",
            "reason": "self asserted",
            "decision_id": "nomos:decision:self",
            "follow_up_review_state": "open",
            "expires_at": "2999-01-01T00:00:00Z",
        }),
        "--dry-run",
        "--",
        "claude",
        "--dangerously-skip-permissions",
    ])

    assert rc == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"]["code"] == "agent_runtime_inline_break_glass_denied"


def test_agent_runtime_launch_cli_dry_run_redacts_child_argv(
    tmp_path: Path,
    capsys,
) -> None:
    rc = cli_main.main([
        "--json",
        "agent-runtime-launch",
        "--runtime",
        "shell-agent",
        "--permissions",
        "dev",
        "--deployment",
        "okg-workspace",
        "--workspace-root",
        str(tmp_path),
        "--run-id",
        "agent-run-cli",
        "--dry-run",
        "--",
        "secret-tool",
        "--api-key=sk-live-raw-agent-secret",
    ])

    assert rc == 0
    output = capsys.readouterr().out
    payload = json.loads(output)
    assert payload["command"]["argv_hash"].startswith("sha256:")
    assert "sk-live-raw-agent-secret" not in output
    assert "--api-key" not in output


# One row per branch and class member of the launcher's secret-literal
# pattern (#2777). The env name carries no sensitive marker, so only the value
# pattern can classify the value. The key tail puts `_` and `-` within its
# first twelve characters, so dropping either member leaves too short a match.
# Values are assembled at runtime so the source holds no credential-shaped
# literal.
_LAUNCHER_SECRET_LITERALS = [
    ("dsn", "postgres" + "://db.example.test/okg"),
    ("dsn-ql", "postgresql" + "://db.example.test/okg"),
    ("openai-style", "sk" + "-" + "a_b-fake0sentinel0notakey"),
    ("token-colon", "token" + ": fake-value"),
    ("token-equals", "token" + "=fake-value"),
    ("api-underscore-equals", "api_key" + "=fake-value"),
    ("api-hyphen-colon", "api-key" + ": fake-value"),
    ("secret-colon", "secret" + ": fake-value"),
    ("secret-equals", "secret" + "=fake-value"),
    ("password-colon", "password" + ": fake-value"),
    ("password-equals", "password" + "=fake-value"),
]


@pytest.mark.parametrize(
    "value",
    [value for _, value in _LAUNCHER_SECRET_LITERALS],
    ids=[name for name, _ in _LAUNCHER_SECRET_LITERALS],
)
def test_env_value_shaped_like_a_secret_is_a_secret_literal(value: str) -> None:
    from okg.substrate.library.agent_runtimes.launcher import _sensitive_env_class

    assert _sensitive_env_class("OKG_AGENT_NOTE", value) == "secret_literal"


def test_plain_env_value_is_not_a_secret_literal() -> None:
    from okg.substrate.library.agent_runtimes.launcher import _sensitive_env_class

    assert _sensitive_env_class("OKG_AGENT_NOTE", "plain value") is None
