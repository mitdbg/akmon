"""Profiled agent runtime launch boundary.

The agent runtime registry writes profile-installed entries that call this
boundary before starting OKG-facing agent subprocesses. The boundary builds a
clean environment, binds a Nomos subject/run/profile, evaluates the canonical
``agent.launch`` or ``agent.bypass`` decisions, and only then returns a launch
plan that callers may exec.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from okg.substrate.nomos import (
    AgentRuntimeDecisionRequest,
    CompiledPolicyBundle,
    NomosRuntimeOutcome,
    compile_policy_graph,
    evaluate_agent_runtime_decision,
    sanitize_trace_mapping,
    sanitize_trace_value,
)

from . import resolve_okg_binary


_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_SAFE_ENV_NAMES = frozenset({
    "CLICOLOR",
    "CLICOLOR_FORCE",
    "COLORTERM",
    "LANG",
    "NO_COLOR",
    "PATH",
    "PYTHONIOENCODING",
    "PYTHONUTF8",
    "SSL_CERT_DIR",
    "SSL_CERT_FILE",
    "TERM",
    "TMP",
    "TMPDIR",
    "TZ",
})
_SAFE_ENV_PREFIXES = ("LC_",)
_SENSITIVE_ENV_EXACT_CLASSES: Mapping[str, str] = {
    "AWS_SHARED_CREDENTIALS_FILE": "home_derived_secret_path",
    "BROWSER_PROFILE_PATH": "browser_profile_path",
    "CHROME_USER_DATA_DIR": "browser_profile_path",
    "CLAUDE_CONFIG": "private_mcp_config",
    "CLAUDE_MCP_CONFIG": "private_mcp_config",
    "CODEX_MCP_CONFIG": "private_mcp_config",
    "COOKIE_PATH": "cookie_path",
    "DATABASE_URL": "dsn",
    "DOCKER_CONFIG": "home_derived_secret_path",
    "GOOGLE_APPLICATION_CREDENTIALS": "home_derived_secret_path",
    "GPG_AGENT_INFO": "home_derived_secret_path",
    "GNUPGHOME": "home_derived_secret_path",
    "HOME": "home_directory",
    "KUBECONFIG": "home_derived_secret_path",
    "MCP_CONFIG": "private_mcp_config",
    "NETRC": "home_derived_secret_path",
    "OKG_DSN": "dsn",
    "PGPASSWORD": "password",
    "SSH_AUTH_SOCK": "ssh_agent_socket",
    "SSLKEYLOGFILE": "secret_log_path",
    "USERPROFILE": "home_directory",
    "X509_USER_PROXY": "x509_proxy_path",
}
_SENSITIVE_ENV_MARKERS = (
    "ACCESS_KEY",
    "APIKEY",
    "API_KEY",
    "AUTH",
    "BEARER",
    "CONNECTION_STRING",
    "COOKIE",
    "CREDENTIAL",
    "DATABASE_URL",
    "DSN",
    "MCP",
    "OAUTH",
    "PASS",
    "PRIVATE",
    "SECRET",
    "SESSION",
    "TOKEN",
)
_MCP_ROLE_DSN_CAPABILITIES: Mapping[str, str] = {
    "OKG_MCP_RO_DSN": "mcp.read",
    "OKG_MCP_RW_DSN": "mcp.write",
}
_DANGEROUS_ARG_FLAGS = frozenset({
    "--allow-all",
    "--allow-dangerously-skip-permissions",
    "--bypass-permissions",
    "--dangerous-skip-permissions",
    "--dangerously-allow-all",
    "--dangerously-bypass-approvals-and-sandbox",
    "--dangerously-bypass-hook-trust",
    "--dangerously-skip-permissions",
    "--disable-sandbox",
    "--ignore-permissions",
    "--no-sandbox",
    "--skip-permissions",
    "--unsafe",
    "--yolo",
})
_DANGEROUS_ARG_VALUE_FLAGS: Mapping[str, tuple[str, frozenset[str]]] = {
    "--ask-for-approval": ("--ask-for-approval", frozenset({"never"})),
    "--sandbox": ("--sandbox", frozenset({"danger-full-access"})),
    "-a": ("--ask-for-approval", frozenset({"never"})),
    "-s": ("--sandbox", frozenset({"danger-full-access"})),
}
_SECRET_LITERAL_RE = re.compile(
    r"(?i)(postgres(?:ql)?://|sk-[a-z0-9_-]{12,}|"
    r"token\s*[:=]|api[_-]?key\s*[:=]|secret\s*[:=]|password\s*[:=])"
)
_INLINE_MCP_CONFIG_INDICATORS = (
    "mcp-config",
    "mcp_config",
    "mcpconfig",
    "mcpservers",
    "mcp_servers",
)
_INLINE_MCP_SECRET_INDICATORS = (
    "api_key",
    "apikey",
    "bearer",
    "connection_string",
    "database_url",
    "dsn",
    "okg_dsn",
    "password",
    "private",
    "secret",
    "token",
)


@dataclass(frozen=True)
class AgentRuntimeProfile:
    """Static capability envelope for a local agent run."""

    profile_id: str
    allowed_read_roots: tuple[str, ...]
    allowed_write_roots: tuple[str, ...]
    network_class: str
    capabilities: frozenset[str] = frozenset()


AGENT_RUNTIME_PROFILES: Mapping[str, AgentRuntimeProfile] = {
    "read-only": AgentRuntimeProfile(
        profile_id="read-only",
        allowed_read_roots=("$WORKSPACE_ROOT",),
        allowed_write_roots=(),
        network_class="none",
        capabilities=frozenset({"mcp.read"}),
    ),
    "dev": AgentRuntimeProfile(
        profile_id="dev",
        allowed_read_roots=("$WORKSPACE_ROOT",),
        allowed_write_roots=("$WORKSPACE_ROOT",),
        network_class="local_dev",
        capabilities=frozenset({"mcp.read", "workspace.write"}),
    ),
    "deploy": AgentRuntimeProfile(
        profile_id="deploy",
        allowed_read_roots=("$WORKSPACE_ROOT",),
        allowed_write_roots=("$WORKSPACE_ROOT",),
        network_class="deployment_admin",
        capabilities=frozenset({"mcp.read", "mcp.write", "workspace.write"}),
    ),
    "security-audit": AgentRuntimeProfile(
        profile_id="security-audit",
        allowed_read_roots=("$WORKSPACE_ROOT",),
        allowed_write_roots=("$WORKSPACE_ROOT/.okg/security-audit",),
        network_class="none",
        capabilities=frozenset({"mcp.read", "audit.write"}),
    ),
}


@dataclass(frozen=True)
class AgentLaunchRequest:
    """Inputs for one profiled agent subprocess."""

    runtime: str
    profile_id: str
    command: Sequence[str]
    workspace_root: str | os.PathLike[str]
    subject_binding_id: str
    deployment: str = "default"
    operator_id: str | None = None
    run_id: str | None = None
    ambient_env: Mapping[str, str] = field(default_factory=dict)
    extra_env: Mapping[str, str] = field(default_factory=dict)
    policy_generation_pin: str = "policy:agent-runtime:bootstrap"
    config_generation_pin: str = "config:agent-runtime:bootstrap"
    deployment_posture: Mapping[str, Any] = field(default_factory=dict)
    break_glass_grant: Mapping[str, Any] | None = None
    # PACT agent-connect-3412: explicit path bindings for the child. The
    # launcher replaces HOME and XDG_CONFIG_HOME and drops XDG_DATA_HOME, so
    # the child cannot find the user's deployments folder or role-password
    # file on its own. A registration names them here; each is validated
    # (absolute, existing, right kind, owned by this user) and exported to
    # the child as OKG_DEPLOYMENTS_DIR / OKG_LOCAL_ENV / OKG_ROLE_SECRETS_FILE.
    # Ambient copies of those variables stay scrubbed.
    deployments_dir: str | None = None
    local_env: str | None = None
    role_secrets_file: str | None = None
    # Lets the child use the public built-in role passwords
    # (OKG_ALLOW_BUILTIN_ROLE_PASSWORDS=1). Set only by a registration that
    # `okg agent connect` wrote, with a warning, for a fresh install whose
    # roles still have them.
    allow_builtin_role_passwords: bool = False


#: Launcher flag -> (request field, child variable, required kind).
PATH_BINDINGS: tuple[tuple[str, str, str, str], ...] = (
    ("--deployments-dir", "deployments_dir", "OKG_DEPLOYMENTS_DIR", "directory"),
    ("--local-env", "local_env", "OKG_LOCAL_ENV", "file"),
    ("--role-secrets-file", "role_secrets_file", "OKG_ROLE_SECRETS_FILE", "file"),
)


#: The deployment's credentials file, relative to the workspace root. A
#: write-enabled profile never gets it, even though its write root (the
#: workspace) contains it (decision `writesenv C`, 2026-10-06).
WORKSPACE_CREDENTIALS_FILE = (".okg", "local.env")


def credential_write_exclusions(
    request: "AgentLaunchRequest",
    *,
    workspace_root: Path,
    write_roots: Sequence[str],
) -> tuple[str, ...]:
    """Credential files carved out of the profile's write roots.

    Only two paths, and only when a write root contains them: the
    deployment's ``<workspace>/.okg/local.env`` and the bound role-password
    file. Nothing else under a write root is excluded.
    """
    if not write_roots:
        return ()
    candidates = [os.path.realpath(workspace_root.joinpath(*WORKSPACE_CREDENTIALS_FILE))]
    if request.role_secrets_file is not None and os.path.isabs(str(request.role_secrets_file)):
        candidates.append(os.path.realpath(str(request.role_secrets_file)))
    roots = [os.path.realpath(root) for root in write_roots]
    out: list[str] = []
    for path in candidates:
        if path not in out and any(_path_within(path, root) for root in roots):
            out.append(path)
    return tuple(out)


def _path_within(path: str, root: str) -> bool:
    return path == root or path.startswith(root.rstrip(os.sep) + os.sep)


def agent_may_write(
    path: str | os.PathLike[str],
    *,
    write_roots: Sequence[str],
    denied: Sequence[str] = (),
) -> bool:
    """True when ``path`` is inside a write root and not an excluded file."""
    resolved = os.path.realpath(str(path))
    if any(_path_within(resolved, os.path.realpath(d)) for d in denied):
        return False
    return any(_path_within(resolved, os.path.realpath(root)) for root in write_roots)


def check_agent_write(
    path: str | os.PathLike[str],
    env: Mapping[str, str] | None = None,
) -> None:
    """Refuse a write by a profiled agent run outside its write area.

    Reads the write roots and the excluded credential files the launcher
    exported (OKG_AGENT_ALLOWED_WRITE_ROOTS, OKG_AGENT_DENIED_WRITE_PATHS).
    Raises ``agent_runtime.write_denied``. Outside a profiled run it refuses
    with ``agent_runtime.unprofiled_run``.
    """
    env = os.environ if env is None else env
    run = require_profiled_agent_run(env, effect="file.write")
    roots = [r for r in str(env.get("OKG_AGENT_ALLOWED_WRITE_ROOTS") or "").split(os.pathsep) if r]
    denied = [d for d in str(env.get("OKG_AGENT_DENIED_WRITE_PATHS") or "").split(os.pathsep) if d]
    if agent_may_write(path, write_roots=roots, denied=denied):
        return
    resolved = os.path.realpath(str(path))
    excluded = any(_path_within(resolved, os.path.realpath(d)) for d in denied)
    raise AgentRuntimeBoundaryError(
        f"agent run {run.agent_run_id} may not write {resolved}: "
        + (
            "it is the deployment's credentials file, excluded from the "
            "profile's write roots"
            if excluded else "it is outside the profile's write roots"
        ),
        reason_code="agent_runtime.write_denied",
        trace={
            "path": resolved,
            "profile_id": run.profile_id,
            "excluded_from_write_roots": excluded,
        },
    )


def validate_path_bindings(
    request: "AgentLaunchRequest",
    *,
    profile: "AgentRuntimeProfile | None" = None,
    write_roots: Sequence[str] = (),
    denied_write_paths: Sequence[str] = (),
) -> dict[str, str]:
    """Check each explicit path binding and return the child's variables.

    A binding must be absolute and exist as the required kind (the final
    component not a symlink). The fully resolved path must belong to the user
    running the launcher, must not be writable by group or others, and must
    not be writable by the agent run: inside one of the profile's write roots
    and not one of the credential files ``credential_write_exclusions``
    carves out of them (an agent that may write there could swap the file).
    Anything else refuses with
    ``agent_runtime.path_binding_invalid``. The child receives the resolved
    path.

    ``allow_builtin_role_passwords`` is refused for a profile that can write
    through MCP (``mcp.write``): the public built-in passwords are tolerated
    for a read-only server only.
    """
    out: dict[str, str] = {}
    for flag, attribute, variable, kind in PATH_BINDINGS:
        raw = getattr(request, attribute, None)
        if raw is None:
            continue
        value = str(raw)
        problem, resolved = _path_binding_problem(value, kind)
        if problem is None and resolved is not None and agent_may_write(
            resolved, write_roots=write_roots, denied=denied_write_paths,
        ):
            problem = (
                "is inside the profile's write roots, where the agent run "
                "may replace it"
            )
        if problem is not None:
            raise AgentRuntimeBoundaryError(
                f"agent runtime {flag} {value!r} is not usable: {problem}",
                reason_code="agent_runtime.path_binding_invalid",
                trace={"flag": flag, "path": value, "problem": problem},
            )
        out[variable] = str(resolved)
    if request.allow_builtin_role_passwords:
        if profile is not None and "mcp.write" in profile.capabilities:
            raise AgentRuntimeBoundaryError(
                "--allow-builtin-role-passwords is refused for profile "
                f"{profile.profile_id!r}: the public built-in passwords are "
                "allowed for a read-only server only",
                reason_code="agent_runtime.builtin_passwords_with_write",
                trace={
                    "flag": "--allow-builtin-role-passwords",
                    "profile_id": profile.profile_id,
                },
            )
        out["OKG_ALLOW_BUILTIN_ROLE_PASSWORDS"] = "1"
    return out


def _path_binding_problem(value: str, kind: str) -> tuple[str | None, str | None]:
    """``(problem, resolved path)``; ``problem`` is None when usable."""
    import stat as _stat

    if not value or not os.path.isabs(value):
        return "not an absolute path", None
    try:
        info = os.lstat(value)
    except FileNotFoundError:
        return "does not exist", None
    except OSError as exc:
        return f"cannot be read ({type(exc).__name__})", None
    if _stat.S_ISLNK(info.st_mode):
        return "is a symbolic link", None
    resolved = os.path.realpath(value)
    try:
        info = os.stat(resolved)
    except OSError as exc:
        return f"cannot be read ({type(exc).__name__})", None
    if kind == "directory" and not _stat.S_ISDIR(info.st_mode):
        return "is not a directory", None
    if kind == "file" and not _stat.S_ISREG(info.st_mode):
        return "is not a regular file", None
    if info.st_uid != os.geteuid():
        return "belongs to another user", None
    if _stat.S_IMODE(info.st_mode) & 0o022:
        return (
            "is writable by group or others "
            f"(mode {_stat.S_IMODE(info.st_mode):04o})"
        ), None
    return None, resolved


@dataclass(frozen=True)
class AgentLaunchPlan:
    """A checked command/env/cwd tuple ready for ``execvpe``."""

    command: tuple[str, ...]
    cwd: Path
    env: Mapping[str, str]
    profile: AgentRuntimeProfile
    runtime: str
    agent_run_id: str
    subject_binding_id: str
    allowed_read_roots: tuple[str, ...]
    allowed_write_roots: tuple[str, ...]
    network_class: str
    audit_trace_ref: str
    launch_decision: NomosRuntimeOutcome
    bypass_decision: NomosRuntimeOutcome | None
    trace: Mapping[str, Any]
    redacted_env_report: tuple[Mapping[str, str], ...]
    break_glass_finding: Mapping[str, Any] | None = None
    #: Credential files excluded from ``allowed_write_roots``.
    denied_write_paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class ProfiledAgentRun:
    """Environment-derived proof that a downstream guard is profiled."""

    agent_run_id: str
    profile_id: str
    subject_binding_id: str
    workspace_root: str
    audit_trace_ref: str
    effect: str


class AgentRuntimeBoundaryError(RuntimeError):
    """Raised when the launcher refuses an unprofiled or denied run."""

    def __init__(
        self,
        message: str,
        *,
        reason_code: str,
        outcome: NomosRuntimeOutcome | None = None,
        trace: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.outcome = outcome
        self.trace = sanitize_trace_mapping(trace or {})


def build_agent_launch_plan(
    bundle: CompiledPolicyBundle | None,
    request: AgentLaunchRequest,
    *,
    active_policy_fingerprint: str | None = None,
    active_config_fingerprint: str | None = None,
) -> AgentLaunchPlan:
    """Return a checked launch plan or fail closed."""
    profile = AGENT_RUNTIME_PROFILES.get(request.profile_id)
    if profile is None:
        raise AgentRuntimeBoundaryError(
            f"unknown agent runtime profile: {request.profile_id}",
            reason_code="agent_runtime.unknown_profile",
            trace={"profile_id": request.profile_id},
        )
    command = _normalized_command(request.command)
    workspace_root = Path(request.workspace_root).expanduser().resolve(strict=False)
    agent_run_id = request.run_id or f"agent-run:{uuid.uuid4()}"
    audit_trace_ref = "agent-runtime:trace:" + _sha256_json({
        "agent_run_id": agent_run_id,
        "profile_id": profile.profile_id,
        "runtime": request.runtime,
        "workspace_root": str(workspace_root),
    })[:24]
    read_roots = _resolve_profile_roots(profile.allowed_read_roots, workspace_root)
    write_roots = _resolve_profile_roots(profile.allowed_write_roots, workspace_root)
    denied_write_paths = credential_write_exclusions(
        request, workspace_root=workspace_root, write_roots=write_roots,
    )
    path_env = validate_path_bindings(
        request, profile=profile, write_roots=write_roots,
        denied_write_paths=denied_write_paths,
    )
    clean_env, redacted_env_report = build_clean_agent_environment(
        profile=profile,
        request=request,
        agent_run_id=agent_run_id,
        workspace_root=workspace_root,
        allowed_read_roots=read_roots,
        allowed_write_roots=write_roots,
        audit_trace_ref=audit_trace_ref,
    )
    clean_env.update(path_env)
    clean_env["OKG_AGENT_DENIED_WRITE_PATHS"] = os.pathsep.join(denied_write_paths)
    if request.allow_builtin_role_passwords:
        # Recorded beside the scrubbed names: this variable reaches the child
        # only because the registration asked for it.
        redacted_env_report = (
            *redacted_env_report,
            {
                "name": "OKG_ALLOW_BUILTIN_ROLE_PASSWORDS",
                "class": "set_by_launcher_flag",
            },
        )
    deployment_posture = {
        "deployment": request.deployment,
        "runtime": request.runtime,
        "workspace": workspace_root.name,
        **dict(request.deployment_posture),
    }
    policy_fingerprint = active_policy_fingerprint or (
        bundle.policy_fingerprint if bundle is not None else None
    )
    config_fingerprint = active_config_fingerprint or request.config_generation_pin
    launch_outcome = evaluate_agent_runtime_decision(
        bundle,
        AgentRuntimeDecisionRequest(
            subject_binding_id=request.subject_binding_id,
            principal_kind="agent",
            action="agent.launch",
            resource_kind="agent_runtime",
            resource_id=f"agent-runtime:{request.runtime}",
            data_classification="internal",
            deployment_posture=deployment_posture,
            policy_generation_pin=request.policy_generation_pin,
            config_generation_pin=request.config_generation_pin,
            config_affects_decision=True,
            enforcement_point="agent.launch",
            agent_run_id=agent_run_id,
            profile_id=profile.profile_id,
            context={
                "runtime": request.runtime,
                "command": _command_shape(command),
                "allowed_read_roots": read_roots,
                "allowed_write_roots": write_roots,
                "network_class": profile.network_class,
                "audit_trace_ref": audit_trace_ref,
                "redacted_environment": redacted_env_report,
                "allow_builtin_role_passwords": bool(
                    request.allow_builtin_role_passwords
                ),
                "denied_write_paths": list(denied_write_paths),
                "path_bindings": sorted(path_env),
            },
        ),
        active_policy_fingerprint=policy_fingerprint,
        active_config_fingerprint=config_fingerprint,
    )
    if not launch_outcome.allowed:
        raise AgentRuntimeBoundaryError(
            "Nomos denied agent runtime launch",
            reason_code=launch_outcome.decision.reason_code,
            outcome=launch_outcome,
            trace=launch_outcome.trace,
        )

    unsafe_findings = detect_unsafe_agent_launch(
        runtime=request.runtime,
        command=command,
    )
    bypass_outcome: NomosRuntimeOutcome | None = None
    if unsafe_findings:
        break_glass_grant = _validated_break_glass_grant(
            request,
            profile=profile,
            agent_run_id=agent_run_id,
            unsafe_findings=unsafe_findings,
        )
        bypass_outcome = evaluate_agent_runtime_decision(
            bundle,
            AgentRuntimeDecisionRequest(
                subject_binding_id=request.subject_binding_id,
                principal_kind="agent",
                action="agent.bypass",
                resource_kind="agent_run",
                resource_id=agent_run_id,
                data_classification="restricted",
                deployment_posture=deployment_posture,
                policy_generation_pin=request.policy_generation_pin,
                config_generation_pin=request.config_generation_pin,
                config_affects_decision=True,
                enforcement_point="agent.bypass",
                agent_run_id=agent_run_id,
                profile_id=profile.profile_id,
                context={
                    "runtime": request.runtime,
                    "unsafe_request": unsafe_findings,
                    "command": _command_shape(command),
                    "operator": request.operator_id,
                },
                break_glass_grant=break_glass_grant,
            ),
            active_policy_fingerprint=policy_fingerprint,
            active_config_fingerprint=config_fingerprint,
        )
        if not bypass_outcome.allowed:
            raise AgentRuntimeBoundaryError(
                "Nomos denied unsafe agent runtime mode",
                reason_code=bypass_outcome.decision.reason_code,
                outcome=bypass_outcome,
                trace=bypass_outcome.trace,
            )
        clean_env = {**clean_env, "OKG_AGENT_BREAK_GLASS": "1"}

    break_glass_finding = (
        dict(bypass_outcome.trace["break_glass_finding"])
        if bypass_outcome is not None
        and isinstance(bypass_outcome.trace.get("break_glass_finding"), Mapping)
        else None
    )
    trace = sanitize_trace_mapping({
        "schema": "okg.agent_runtime.launch_trace.v1",
        "runtime": request.runtime,
        "profile_id": profile.profile_id,
        "agent_run_id": agent_run_id,
        "subject_binding_id": request.subject_binding_id,
        "workspace_root": str(workspace_root),
        "allowed_read_roots": read_roots,
        "allowed_write_roots": write_roots,
        "denied_write_paths": list(denied_write_paths),
        "network_class": profile.network_class,
        "audit_trace_ref": audit_trace_ref,
        "launch_decision_ref": launch_outcome.trace.get("trace_ref"),
        "command": _command_shape(command),
        "bypass_decision_ref": (
            bypass_outcome.trace.get("trace_ref")
            if bypass_outcome is not None else None
        ),
        "redacted_environment": redacted_env_report,
        "unsafe_request": unsafe_findings,
        "break_glass_finding": break_glass_finding,
    })
    return AgentLaunchPlan(
        command=command,
        cwd=workspace_root,
        env=clean_env,
        profile=profile,
        runtime=request.runtime,
        agent_run_id=agent_run_id,
        subject_binding_id=request.subject_binding_id,
        allowed_read_roots=read_roots,
        allowed_write_roots=write_roots,
        network_class=profile.network_class,
        audit_trace_ref=audit_trace_ref,
        launch_decision=launch_outcome,
        bypass_decision=bypass_outcome,
        trace=trace,
        redacted_env_report=redacted_env_report,
        break_glass_finding=break_glass_finding,
        denied_write_paths=denied_write_paths,
    )


def build_clean_agent_environment(
    *,
    profile: AgentRuntimeProfile,
    request: AgentLaunchRequest,
    agent_run_id: str,
    workspace_root: Path,
    allowed_read_roots: Sequence[str],
    allowed_write_roots: Sequence[str],
    audit_trace_ref: str,
) -> tuple[dict[str, str], tuple[Mapping[str, str], ...]]:
    """Build an allowlisted environment and a value-free redaction report."""
    env: dict[str, str] = {}
    dropped: dict[str, str] = {}
    for key, value in sorted((request.ambient_env or {}).items()):
        key_s = str(key)
        value_s = str(value)
        classification = _sensitive_env_class(key_s, value_s)
        if _safe_env_key(key_s) and classification is None:
            env[key_s] = value_s
        else:
            dropped[key_s] = classification or "not_allowlisted"

    for key, value in sorted((request.extra_env or {}).items()):
        key_s = str(key)
        value_s = str(value)
        role_dsn_capability = _MCP_ROLE_DSN_CAPABILITIES.get(key_s)
        if role_dsn_capability is not None:
            if not value_s.strip():
                dropped[key_s] = "empty"
            elif role_dsn_capability in profile.capabilities:
                env[key_s] = value_s
            else:
                dropped[key_s] = "profile_capability"
            continue
        classification = _sensitive_env_class(key_s, value_s)
        if classification is not None:
            dropped[key_s] = classification
            continue
        if key_s == "OKG_MCP_ALLOW_WRITES":
            if "mcp.write" in profile.capabilities and _truthy(value_s):
                env[key_s] = "1"
            else:
                dropped[key_s] = "profile_capability"
            continue
        if _safe_extra_env_key(key_s):
            env[key_s] = value_s
        else:
            dropped[key_s] = "not_allowlisted"

    runtime_home = workspace_root / ".okg" / "agent-runtime" / agent_run_id
    env.update({
        "HOME": str(runtime_home / "home"),
        "OKG_AGENT": "1",
        "OKG_AGENT_ALLOWED_READ_ROOTS": os.pathsep.join(allowed_read_roots),
        "OKG_AGENT_ALLOWED_WRITE_ROOTS": os.pathsep.join(allowed_write_roots),
        "OKG_AGENT_AUDIT_TRACE_REF": audit_trace_ref,
        "OKG_AGENT_NETWORK_CLASS": profile.network_class,
        "OKG_AGENT_RUN_ID": agent_run_id,
        "OKG_AGENT_RUNTIME_ID": request.runtime,
        "OKG_AGENT_RUNTIME_PROFILE": profile.profile_id,
        "OKG_AGENT_SUBJECT": request.subject_binding_id,
        "OKG_DEPLOYMENT": request.deployment,
        "OKG_NOMOS_CONFIG_PIN": request.config_generation_pin,
        "OKG_NOMOS_POLICY_PIN": request.policy_generation_pin,
        "OKG_WORKSPACE_ROOT": str(workspace_root),
        "XDG_CACHE_HOME": str(runtime_home / "cache"),
        "XDG_CONFIG_HOME": str(runtime_home / "config"),
        "XDG_STATE_HOME": str(runtime_home / "state"),
    })
    report = tuple(
        {"name": name, "class": dropped[name]}
        for name in sorted(dropped)
    )
    return env, report


def ensure_agent_runtime_directories(plan: AgentLaunchPlan) -> None:
    """Create launcher-private state dirs before exec."""
    for key in ("HOME", "XDG_CACHE_HOME", "XDG_CONFIG_HOME", "XDG_STATE_HOME"):
        value = plan.env.get(key)
        if value:
            Path(value).mkdir(parents=True, exist_ok=True)


def detect_unsafe_agent_launch(
    *,
    runtime: str,
    command: Sequence[str],
) -> tuple[Mapping[str, Any], ...]:
    """Return privacy-safe unsafe-mode findings for a command shape."""
    findings: list[Mapping[str, Any]] = []
    for index, raw in enumerate(command):
        token = str(raw)
        lower = token.lower()
        flag = lower.split("=", 1)[0]
        if flag in _DANGEROUS_ARG_FLAGS:
            findings.append({
                "class": "dangerous_permission_flag",
                "runtime": runtime,
                "arg_index": str(index),
                "flag": flag,
            })
        dangerous_value_flag = _dangerous_arg_value_flag(command, index, lower)
        if dangerous_value_flag is not None:
            findings.append({
                "class": "dangerous_permission_flag",
                "runtime": runtime,
                "arg_index": str(index),
                "flag": dangerous_value_flag,
            })
        if _dangerous_permission_mode(command, index, lower):
            findings.append({
                "class": "dangerous_permission_flag",
                "runtime": runtime,
                "arg_index": str(index),
                "flag": "--permission-mode",
            })
        if _inline_secret_mcp_config(lower):
            findings.append({
                "class": "inline_secret_bearing_mcp_config",
                "runtime": runtime,
                "arg_index": str(index),
                "token_hash": "sha256:" + _sha256_text(token),
            })
    return tuple(findings)


def require_profiled_agent_run(
    env: Mapping[str, str] | None = None,
    *,
    effect: str,
) -> ProfiledAgentRun:
    """Fail loudly when a sensitive guard runs outside the launcher."""
    env = env or os.environ
    required = {
        "OKG_AGENT_AUDIT_TRACE_REF": "audit_trace_ref",
        "OKG_AGENT_RUN_ID": "agent_run_id",
        "OKG_AGENT_RUNTIME_PROFILE": "profile_id",
        "OKG_AGENT_SUBJECT": "subject_binding_id",
        "OKG_WORKSPACE_ROOT": "workspace_root",
    }
    missing = [key for key in required if not str(env.get(key) or "").strip()]
    if missing:
        raise AgentRuntimeBoundaryError(
            "sensitive agent guard invoked outside a profiled runtime",
            reason_code="agent_runtime.unprofiled_run",
            trace={"effect": effect, "missing_env": missing},
        )
    return ProfiledAgentRun(
        agent_run_id=str(env["OKG_AGENT_RUN_ID"]),
        profile_id=str(env["OKG_AGENT_RUNTIME_PROFILE"]),
        subject_binding_id=str(env["OKG_AGENT_SUBJECT"]),
        workspace_root=str(env["OKG_WORKSPACE_ROOT"]),
        audit_trace_ref=str(env["OKG_AGENT_AUDIT_TRACE_REF"]),
        effect=effect,
    )


def build_profiled_mcp_server_entry(
    *,
    runtime: str,
    deployment: str,
    dsn: str,
    extra_env: Mapping[str, str] | None = None,
    profile: str | None = None,
    workspace_root: str | os.PathLike[str] | None = None,
    deployments_dir: str | os.PathLike[str] | None = None,
    local_env: str | os.PathLike[str] | None = None,
    role_secrets_file: str | os.PathLike[str] | None = None,
    allow_builtin_role_passwords: bool = False,
) -> dict[str, Any]:
    """Build an MCP config entry that starts through the runtime boundary.

    ``dsn`` is intentionally not written into the MCP config. The child
    ``mcp-serve`` command resolves deployment authority after the launcher has
    stripped ambient credentials. ``deployments_dir``, ``local_env`` and
    ``role_secrets_file`` become the launcher's explicit path flags (PACT
    agent-connect-3412): paths, never their contents.
    """
    del dsn
    command, prefix_args = resolve_okg_binary()
    safe_extra_env = extra_env or {}
    write_requested = _truthy(str(safe_extra_env.get("OKG_MCP_ALLOW_WRITES", "")))
    profile_id = profile or ("deploy" if write_requested else "read-only")
    root = Path(workspace_root) if workspace_root is not None else Path.cwd()
    subject = f"subject:agent:{deployment}:{runtime}"
    profile_caps = AGENT_RUNTIME_PROFILES.get(profile_id)
    env = {
        "OKG_AGENT_RUNTIME_PROFILE": profile_id,
        "OKG_DEPLOYMENT": deployment,
    }
    if profile_caps is not None:
        for key, capability in _MCP_ROLE_DSN_CAPABILITIES.items():
            value = str(safe_extra_env.get(key) or "").strip()
            if value and capability in profile_caps.capabilities:
                env[key] = value
    child_args = [
        command,
        *prefix_args,
        "mcp-serve",
        "--deployment",
        deployment,
    ]
    if write_requested:
        child_args.append("--allow-writes")
    launcher_args = [
        *prefix_args,
        "agent-runtime-launch",
        "--runtime",
        runtime,
        "--permissions",
        profile_id,
        "--deployment",
        deployment,
        "--subject",
        subject,
        "--workspace-root",
        str(root),
    ]
    for flag, value in (
        ("--deployments-dir", deployments_dir),
        ("--local-env", local_env),
        ("--role-secrets-file", role_secrets_file),
    ):
        if value is not None:
            launcher_args.extend([flag, str(value)])
    if allow_builtin_role_passwords:
        launcher_args.append("--allow-builtin-role-passwords")
    launcher_args.extend(["--", *child_args])
    return {
        "command": command,
        "args": launcher_args,
        "env": env,
    }


def bootstrap_agent_runtime_policy(
    *,
    subject_binding_id: str,
    deployment: str,
) -> CompiledPolicyBundle:
    """Narrow bootstrap policy for the CLI launcher entrypoint."""
    return compile_policy_graph({
        "policy_bundle_id": f"nomos:agent-runtime:bootstrap:{deployment}",
        "policy_version": "bootstrap-v0",
        "vocabulary_strict": True,
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "bootstrap_allow_profiled_agent_launch",
                "effect": "allow",
                "actions": ["agent.launch"],
                "subjects": [subject_binding_id],
                "priority": 100,
                "reason_code": "nomos.agent_runtime.launch_allowed",
            },
            {
                "rule_id": "bootstrap_allow_scoped_break_glass_bypass",
                "effect": "allow",
                "actions": ["agent.bypass"],
                "subjects": [subject_binding_id],
                "priority": 100,
                "reason_code": "nomos.agent_runtime.break_glass_allowed",
            },
        ],
    })


def break_glass_scope_for_unsafe_request(
    *,
    runtime: str,
    profile_id: str,
    unsafe_findings: Sequence[Mapping[str, Any]],
) -> str:
    """Return the exact break-glass scope for this unsafe launch shape."""
    return (
        f"agent-runtime:{runtime}:{profile_id}:unsafe-mode:"
        f"{_sha256_json(list(unsafe_findings))[:16]}"
    )


def _validated_break_glass_grant(
    request: AgentLaunchRequest,
    *,
    profile: AgentRuntimeProfile,
    agent_run_id: str,
    unsafe_findings: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    grant = request.break_glass_grant
    if grant is None:
        return None
    if not isinstance(grant, Mapping):
        raise AgentRuntimeBoundaryError(
            "break-glass grant must be an object",
            reason_code="agent_runtime.break_glass_grant_invalid",
            trace={"agent_run_id": agent_run_id, "profile_id": profile.profile_id},
        )
    operator = str(request.operator_id or "").strip()
    if not operator:
        raise AgentRuntimeBoundaryError(
            "break-glass grant requires an operator binding",
            reason_code="agent_runtime.break_glass_operator_required",
            trace={"agent_run_id": agent_run_id, "profile_id": profile.profile_id},
        )
    expected_scope = break_glass_scope_for_unsafe_request(
        runtime=request.runtime,
        profile_id=profile.profile_id,
        unsafe_findings=unsafe_findings,
    )
    expected = {
        "operator": operator,
        "subject": request.subject_binding_id,
        "agent_run_id": agent_run_id,
        "profile_id": profile.profile_id,
        "scope": expected_scope,
    }
    mismatched = [
        key
        for key, expected_value in expected.items()
        if str(grant.get(key) or "").strip() != expected_value
    ]
    if mismatched:
        raise AgentRuntimeBoundaryError(
            "break-glass grant does not match this launch scope",
            reason_code="agent_runtime.break_glass_grant_mismatch",
            trace={
                "mismatched_fields": mismatched,
                "expected_scope": expected_scope,
                "agent_run_id": agent_run_id,
                "profile_id": profile.profile_id,
            },
        )
    return dict(grant)


def _normalized_command(command: Sequence[str]) -> tuple[str, ...]:
    items = tuple(str(item) for item in command if str(item))
    if items and items[0] == "--":
        items = items[1:]
    if not items:
        raise AgentRuntimeBoundaryError(
            "agent runtime launch command is empty",
            reason_code="agent_runtime.empty_command",
        )
    return items


def _resolve_profile_roots(
    roots: Sequence[str],
    workspace_root: Path,
) -> tuple[str, ...]:
    resolved = []
    for raw in roots:
        value = raw.replace("$WORKSPACE_ROOT", str(workspace_root))
        resolved.append(str(Path(value).expanduser().resolve(strict=False)))
    return tuple(resolved)


def _safe_env_key(key: str) -> bool:
    return key in _SAFE_ENV_NAMES or key.startswith(_SAFE_ENV_PREFIXES)


def _safe_extra_env_key(key: str) -> bool:
    return key.startswith("OKG_AGENT_") or key in {"OKG_DEPLOYMENT"}


def _sensitive_env_class(key: str, value: str) -> str | None:
    upper = key.upper()
    if upper in _SENSITIVE_ENV_EXACT_CLASSES:
        return _SENSITIVE_ENV_EXACT_CLASSES[upper]
    if any(marker in upper for marker in _SENSITIVE_ENV_MARKERS):
        if "DSN" in upper or "DATABASE" in upper or "CONNECTION" in upper:
            return "dsn"
        if "MCP" in upper:
            return "private_mcp_config"
        if "COOKIE" in upper:
            return "cookie_path"
        return "token_like"
    if _SECRET_LITERAL_RE.search(value):
        return "secret_literal"
    return None


def _truthy(value: str) -> bool:
    return value.strip().lower() in _TRUE_VALUES


def _inline_secret_mcp_config(lower_token: str) -> bool:
    if "mcp" not in lower_token:
        return False
    if any(marker in lower_token for marker in _INLINE_MCP_CONFIG_INDICATORS):
        return True
    if any(marker in lower_token for marker in _INLINE_MCP_SECRET_INDICATORS):
        return True
    return _SECRET_LITERAL_RE.search(lower_token) is not None


def _dangerous_permission_mode(
    command: Sequence[str],
    index: int,
    lower_token: str,
) -> bool:
    flag, separator, value = lower_token.partition("=")
    if flag != "--permission-mode":
        return False
    if separator:
        return value == "bypasspermissions"
    if index + 1 >= len(command):
        return False
    return str(command[index + 1]).strip().lower() == "bypasspermissions"


def _dangerous_arg_value_flag(
    command: Sequence[str],
    index: int,
    lower_token: str,
) -> str | None:
    flag, separator, value = lower_token.partition("=")
    configured = _DANGEROUS_ARG_VALUE_FLAGS.get(flag)
    if configured is not None:
        canonical_flag, dangerous_values = configured
        raw_value = value if separator else _next_arg(command, index)
        if _normalized_arg_value(raw_value) in dangerous_values:
            return canonical_flag
    if flag == "--config" and separator:
        return _dangerous_codex_config_flag(value)
    if flag in {"--config", "-c"}:
        return _dangerous_codex_config_flag(_next_arg(command, index))
    return None


def _dangerous_codex_config_flag(raw_value: str) -> str | None:
    normalized = raw_value.strip().lower().replace("_", "-")
    if "danger-full-access" in normalized and "sandbox" in normalized:
        return "--config:sandbox"
    if "never" in normalized and "approval" in normalized:
        return "--config:approval"
    return None


def _next_arg(command: Sequence[str], index: int) -> str:
    if index + 1 >= len(command):
        return ""
    return str(command[index + 1])


def _normalized_arg_value(value: str) -> str:
    return value.strip().strip("'\"").lower()


def _command_shape(command: Sequence[str]) -> dict[str, Any]:
    executable = Path(command[0]).name if command else ""
    return sanitize_trace_mapping({
        "executable": executable,
        "argc": len(command),
        "argv_hash": "sha256:" + _sha256_json(list(command)),
        "contains_mcp_config": any(
            "mcp" in str(token).lower() and "config" in str(token).lower()
            for token in command
        ),
    })


def _sha256_json(value: Any) -> str:
    safe = sanitize_trace_value(value)
    encoded = json.dumps(
        safe,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
