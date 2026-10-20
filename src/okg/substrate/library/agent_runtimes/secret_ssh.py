"""Secret-reference and SSH grant gates for profiled agent runs."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit

from okg.substrate.nomos import (
    AgentRuntimeDecisionRequest,
    CompiledPolicyBundle,
    NomosPrimitiveCheck,
    NomosRuntimeOutcome,
    evaluate_agent_runtime_decision,
    sanitize_trace_mapping,
)


SECRET_REF_SCHEMES = frozenset({"secret", "vault", "keyring", "aws-sm"})
SECRET_REF_PREFIXES = tuple(f"{scheme}://" for scheme in sorted(SECRET_REF_SCHEMES))
SECRET_DELIVERY_MODES = frozenset({
    "brokered_operation",
    "wrapper_owned_subprocess",
    "reviewed_literal_break_glass",
})
_SECRET_REF_BODY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._~/-]*")
_SECRET_NAME_MARKERS = (
    "ACCESS_KEY",
    "APIKEY",
    "API_KEY",
    "AUTH",
    "BEARER",
    "COOKIE",
    "CREDENTIAL",
    "DATABASE_URL",
    "DSN",
    "KEYTAB",
    "OAUTH",
    "PASS",
    "PASSWORD",
    "PRIVATE_KEY",
    "SECRET",
    "SESSION",
    "TOKEN",
)
_SECRET_LITERAL_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("private_key", re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----")),
    ("dsn", re.compile(r"(?i)\b(?:postgres(?:ql)?|mysql|mongodb|redis)://")),
    ("api_key", re.compile(r"(?i)\bsk-[a-z0-9_-]{12,}\b")),
    ("bearer_token", re.compile(r"(?i)\bbearer\s+[a-z0-9._~+/-]{12,}")),
    ("aws_access_key", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("github_token", re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{30,}\b")),
    ("github_token", re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b")),
    ("slack_token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b")),
    (
        "assignment",
        re.compile(
            r"(?i)\b(?:api[_-]?key|token|secret|password|cookie|"
            r"access[_-]?key(?:[_-]?id)?|secret[_-]?access[_-]?key)"
            r"\s*[:=]\s*\S+",
        ),
    ),
    (
        # The user name may be empty: `https://:TOKEN@host` carries a token in
        # the password slot (#2812). A non-empty password is still required.
        "credential_url",
        re.compile(r"(?i)\b[a-z][a-z0-9+.-]*://[^/\s:@]*:[^@\s]+@"),
    ),
)


@dataclass(frozen=True)
class AgentSecretUseRequest:
    """One agent request to use a secret reference."""

    subject_binding_id: str
    agent_run_id: str
    profile_id: str
    secret_ref: str
    action: str
    resource_id: str
    delivery_mode: str = "brokered_operation"
    deployment: str = "default"
    deployment_posture: Mapping[str, Any] = field(default_factory=dict)
    agent_visible_config: Mapping[str, Any] = field(default_factory=dict)
    policy_generation_pin: str = "policy:agent-runtime:bootstrap"
    config_generation_pin: str = "config:agent-runtime:bootstrap"


@dataclass(frozen=True)
class AgentSecretGrant:
    """Scoped, time-bounded authorization to use one secret reference."""

    grant_id: str
    subject_binding_id: str
    agent_run_id: str
    profile_id: str
    secret_ref: str
    action: str
    resource_id: str
    delivery_mode: str
    expires_at: str
    status: str = "active"


@dataclass(frozen=True)
class AgentSshUseRequest:
    """One agent request to use SSH authority."""

    subject_binding_id: str
    agent_run_id: str
    profile_id: str
    target_host: str
    principal: str
    command: str
    workspace_root: str | Path
    deployment: str = "default"
    deployment_posture: Mapping[str, Any] = field(default_factory=dict)
    ambient_env: Mapping[str, str] = field(default_factory=dict)
    host_default_ssh_auth_sock: str | None = None
    discovered_socket_paths: Sequence[str] = ()
    docker_mounts: Sequence[str] = ()
    policy_generation_pin: str = "policy:agent-runtime:bootstrap"
    config_generation_pin: str = "config:agent-runtime:bootstrap"


@dataclass(frozen=True)
class AgentSshGrant:
    """Task-scoped isolated SSH socket grant."""

    grant_id: str
    subject_binding_id: str
    agent_run_id: str
    profile_id: str
    identity_ref: str
    allowed_hosts: tuple[str, ...]
    allowed_principals: tuple[str, ...]
    allowed_commands: tuple[str, ...]
    socket_path: str
    expires_at: str
    status: str = "active"
    mechanism: str = "isolated_ssh_agent"


@dataclass(frozen=True)
class AgentGateResult:
    """Value-free gate result returned to launcher-adjacent wrappers."""

    allowed: bool
    reason_code: str
    outcome: NomosRuntimeOutcome
    trace: Mapping[str, Any]
    resolution: Mapping[str, Any] = field(default_factory=dict)
    env: Mapping[str, str] = field(default_factory=dict)


def evaluate_agent_secret_use(
    bundle: CompiledPolicyBundle | None,
    request: AgentSecretUseRequest,
    *,
    grant: AgentSecretGrant | None = None,
    active_policy_fingerprint: str | None = None,
    active_config_fingerprint: str | None = None,
    now: datetime | None = None,
) -> AgentGateResult:
    """Evaluate a secret-reference use without exposing the secret value."""
    now = now or datetime.now(timezone.utc)
    literal_findings = detect_agent_secret_literals(
        request.agent_visible_config,
        source="agent_visible_config",
    )
    ref_valid = is_secret_ref(request.secret_ref)
    grant_status = _secret_grant_status(request, grant=grant, now=now)
    primitive = _secret_primitive_check(
        literal_findings=literal_findings,
        ref_valid=ref_valid,
        grant_status=grant_status,
    )
    secret_hash = (
        secret_ref_fingerprint(request.secret_ref)
        if ref_valid else "secret-ref:invalid"
    )
    candidate_resolution = (
        {
            "ref_hash": secret_hash,
            "grant_id": grant.grant_id,
            "delivery_mode": grant.delivery_mode,
            "expires_at": grant.expires_at,
            "value_materialized": False,
        }
        if primitive.allowed and grant is not None
        else {}
    )
    outcome = evaluate_agent_runtime_decision(
        bundle,
        AgentRuntimeDecisionRequest(
            subject_binding_id=request.subject_binding_id,
            principal_kind="okg_service",
            action="secret.use",
            resource_kind="secret",
            resource_id=secret_hash,
            data_classification="secret",
            deployment_posture={
                "deployment": request.deployment,
                **dict(request.deployment_posture),
            },
            policy_generation_pin=request.policy_generation_pin,
            config_generation_pin=request.config_generation_pin,
            config_affects_decision=True,
            enforcement_point="secret.use",
            agent_run_id=request.agent_run_id,
            profile_id=request.profile_id,
            context={
                "ref_hash": secret_hash,
                "requested_action": request.action,
                "requested_resource_id": request.resource_id,
                "delivery_mode": request.delivery_mode,
                "literal_findings": literal_findings,
                "grant": grant_status,
            },
        ),
        active_policy_fingerprint=active_policy_fingerprint,
        active_config_fingerprint=active_config_fingerprint,
        primitive_checks=(primitive,),
        performed_obligations=("secret_ref_required",) if ref_valid else (),
        payload=None,
    )
    resolution = candidate_resolution if outcome.allowed else {}
    trace = sanitize_trace_mapping({
        **dict(outcome.trace),
        "agent_material_gate": {
            "ref_hash": secret_hash,
            "literal_findings": literal_findings,
            "grant": grant_status,
            "delivery_mode": request.delivery_mode,
            "value_materialized": False,
        },
    })
    return AgentGateResult(
        allowed=outcome.allowed,
        reason_code=outcome.decision.reason_code,
        outcome=outcome,
        trace=trace,
        resolution=sanitize_trace_mapping(resolution),
    )


def evaluate_agent_ssh_use(
    bundle: CompiledPolicyBundle | None,
    request: AgentSshUseRequest,
    *,
    grant: AgentSshGrant | None = None,
    active_policy_fingerprint: str | None = None,
    active_config_fingerprint: str | None = None,
    now: datetime | None = None,
) -> AgentGateResult:
    """Evaluate SSH use and return only an isolated grant socket on success."""
    now = now or datetime.now(timezone.utc)
    default_socket_exposures = detect_default_ssh_socket_exposure(
        request.ambient_env,
        discovered_socket_paths=request.discovered_socket_paths,
        docker_mounts=request.docker_mounts,
        host_default_socket=request.host_default_ssh_auth_sock,
        allowed_socket_path=grant.socket_path if grant is not None else None,
    )
    grant_status = _ssh_grant_status(
        request,
        grant=grant,
        default_socket_exposures=default_socket_exposures,
        now=now,
    )
    primitive = _ssh_primitive_check(
        grant_status=grant_status,
        default_socket_exposures=default_socket_exposures,
    )
    identity_ref_valid = grant is not None and is_secret_ref(grant.identity_ref)
    identity_hash = (
        secret_ref_fingerprint(grant.identity_ref)
        if identity_ref_valid else "ssh-identity:invalid"
    )
    command_hash = "sha256:" + _sha256_text(request.command)
    candidate_resolution = (
        {
            "ssh_identity_hash": identity_hash,
            "grant_id": grant.grant_id,
            "mechanism": grant.mechanism,
            "socket_scope": "agent_run",
            "socket_delivery": "wrapper_owned_subprocess",
            "socket_path_hash": "sha256:" + _sha256_text(_safe_path(grant.socket_path)),
            "hosts": list(grant.allowed_hosts),
            "principals": list(grant.allowed_principals),
            "command_hash": command_hash,
            "wrapper_command": "okg agent-ssh-run",
            "wrapper_enforcement": "per_invocation_grant_recheck",
            "expires_at": grant.expires_at,
        }
        if primitive.allowed and grant is not None
        else {}
    )
    outcome = evaluate_agent_runtime_decision(
        bundle,
        AgentRuntimeDecisionRequest(
            subject_binding_id=request.subject_binding_id,
            principal_kind="agent",
            action="ssh.use",
            resource_kind="ssh_identity",
            resource_id=identity_hash,
            data_classification="restricted",
            deployment_posture={
                "deployment": request.deployment,
                **dict(request.deployment_posture),
            },
            policy_generation_pin=request.policy_generation_pin,
            config_generation_pin=request.config_generation_pin,
            config_affects_decision=True,
            enforcement_point="ssh.use",
            agent_run_id=request.agent_run_id,
            profile_id=request.profile_id,
            context={
                "ssh_identity_hash": identity_hash,
                "target_host": request.target_host,
                "principal": request.principal,
                "command_hash": command_hash,
                "grant": grant_status,
                "default_socket_exposures": default_socket_exposures,
            },
        ),
        active_policy_fingerprint=active_policy_fingerprint,
        active_config_fingerprint=active_config_fingerprint,
        primitive_checks=(primitive,),
        performed_obligations=("audit",),
        payload=None,
    )
    resolution = candidate_resolution if outcome.allowed else {}
    env = {"OKG_AGENT_SSH_GRANT": grant.grant_id} if outcome.allowed and grant else {}
    trace = sanitize_trace_mapping({
        **dict(outcome.trace),
        "agent_ssh_gate": {
            "ssh_identity_hash": identity_hash,
            "grant": grant_status,
            "target_host": request.target_host,
            "principal": request.principal,
            "command_hash": command_hash,
            "default_socket_exposures": default_socket_exposures,
            "socket_scope": "agent_run" if env else "none",
        },
    })
    return AgentGateResult(
        allowed=outcome.allowed,
        reason_code=outcome.decision.reason_code,
        outcome=outcome,
        trace=trace,
        resolution=sanitize_trace_mapping(resolution),
        env=env,
    )


def detect_agent_secret_literals(
    value: Any,
    *,
    source: str,
) -> tuple[Mapping[str, Any], ...]:
    """Return value-free findings for literal secrets in agent-visible data."""
    findings: list[Mapping[str, Any]] = []

    def visit(node: Any, path: tuple[str, ...]) -> None:
        if isinstance(node, Mapping):
            if set(node) == {"secret_ref"} and is_secret_ref(str(node["secret_ref"])):
                return
            for key, child in sorted(node.items(), key=lambda item: str(item[0])):
                key_s = str(key)
                child_path = (*path, key_s)
                if _secret_name_class(key_s) and not _value_is_secret_ref(child):
                    findings.append(_literal_finding(
                        source=source,
                        path=child_path,
                        literal_class=_secret_name_class(key_s) or "secret_named",
                    ))
                visit(child, child_path)
            return
        if isinstance(node, Sequence) and not isinstance(node, str | bytes | bytearray):
            for index, child in enumerate(node):
                visit(child, (*path, str(index)))
            return
        if isinstance(node, str) and not is_secret_ref(node):
            literal_class = _secret_literal_class(node)
            if literal_class is not None:
                findings.append(_literal_finding(
                    source=source,
                    path=path,
                    literal_class=literal_class,
                ))

    visit(value, ())
    deduped: dict[str, Mapping[str, Any]] = {}
    for finding in findings:
        key = json.dumps(finding, sort_keys=True, default=str)
        deduped[key] = finding
    return tuple(deduped.values())


def detect_default_ssh_socket_exposure(
    ambient_env: Mapping[str, str],
    *,
    discovered_socket_paths: Sequence[str] = (),
    docker_mounts: Sequence[str] = (),
    host_default_socket: str | None = None,
    allowed_socket_path: str | None = None,
) -> tuple[Mapping[str, Any], ...]:
    """Return value-free findings when the user's default SSH socket is reachable."""
    default_socket = str(
        host_default_socket or ambient_env.get("SSH_AUTH_SOCK") or ""
    ).strip()
    default_resolved = _safe_path(default_socket) if default_socket else ""
    if (
        default_socket
        and allowed_socket_path
        and default_resolved == _safe_path(allowed_socket_path)
    ):
        return ()
    findings: list[Mapping[str, Any]] = []
    if default_socket:
        sources_and_values = (
            ("env", (str(ambient_env.get("SSH_AUTH_SOCK") or ""),)),
            ("filesystem_probe", discovered_socket_paths),
            ("docker_mount", _docker_mount_source_paths(docker_mounts)),
        )
        for source, values in sources_and_values:
            for raw in values:
                raw_s = str(raw)
                if not raw_s:
                    continue
                resolved = _safe_path(raw_s)
                if resolved == default_resolved or (
                    source == "docker_mount" and _path_contains(resolved, default_resolved)
                ):
                    findings.append({
                        "class": "default_ssh_agent_socket",
                        "source": source,
                        "path_hash": "sha256:" + _sha256_text(default_resolved),
                    })
        return tuple(findings)

    sources_and_values = (
        ("env", (default_socket,)),
        ("filesystem_probe", discovered_socket_paths),
        ("docker_mount", _docker_mount_source_paths(docker_mounts)),
    )
    for source, values in sources_and_values:
        for raw in values:
            raw_s = str(raw)
            if not raw_s:
                continue
            resolved = _safe_path(raw_s)
            if _looks_like_ssh_agent_socket_path(resolved):
                findings.append({
                    "class": "untrusted_ssh_agent_socket",
                    "source": source,
                    "path_hash": "sha256:" + _sha256_text(resolved),
                })
    return tuple(findings)


def is_secret_ref(value: str) -> bool:
    """Return whether ``value`` is a supported secret reference."""
    text = str(value).strip()
    parsed = urlsplit(text)
    if parsed.scheme not in SECRET_REF_SCHEMES:
        return False
    if any(char.isspace() or ord(char) < 32 for char in text):
        return False
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        return False
    if "@" in parsed.netloc:
        return False
    body = f"{parsed.netloc}{parsed.path}"
    return _SECRET_REF_BODY_RE.fullmatch(body) is not None


def secret_ref_fingerprint(secret_ref: str) -> str:
    """Value-free stable id for secret refs and SSH identity refs."""
    return "secret-ref:sha256:" + _sha256_text(str(secret_ref).strip())


def _secret_primitive_check(
    *,
    literal_findings: Sequence[Mapping[str, Any]],
    ref_valid: bool,
    grant_status: Mapping[str, Any],
) -> NomosPrimitiveCheck:
    if literal_findings or not ref_valid:
        return NomosPrimitiveCheck(
            name="agent.secret_ref_required",
            allowed=False,
            reason_code="nomos.runtime.secret_ref_required",
            metadata={
                "literal_findings": list(literal_findings),
                "secret_ref_valid": ref_valid,
            },
        )
    if grant_status.get("active") is not True:
        return NomosPrimitiveCheck(
            name="agent.secret_grant_scope",
            allowed=False,
            reason_code=str(
                grant_status.get("reason_code") or "nomos.runtime.secret_grant_missing"
            ),
            metadata={"grant": grant_status},
        )
    return NomosPrimitiveCheck(
        name="agent.secret_grant_scope",
        allowed=True,
        reason_code="nomos.runtime.secret_grant_allowed",
        metadata={"grant": grant_status},
    )


def _ssh_primitive_check(
    *,
    grant_status: Mapping[str, Any],
    default_socket_exposures: Sequence[Mapping[str, Any]],
) -> NomosPrimitiveCheck:
    if default_socket_exposures:
        return NomosPrimitiveCheck(
            name="agent.ssh_default_socket",
            allowed=False,
            reason_code="nomos.runtime.ssh_default_socket_exposed",
            metadata={"default_socket_exposures": list(default_socket_exposures)},
        )
    if grant_status.get("active") is not True:
        return NomosPrimitiveCheck(
            name="agent.ssh_grant_scope",
            allowed=False,
            reason_code=str(
                grant_status.get("reason_code") or "nomos.runtime.ssh_grant_missing"
            ),
            metadata={"grant": grant_status},
        )
    return NomosPrimitiveCheck(
        name="agent.ssh_grant_scope",
        allowed=True,
        reason_code="nomos.runtime.ssh_grant_allowed",
        metadata={"grant": grant_status},
    )


def _secret_grant_status(
    request: AgentSecretUseRequest,
    *,
    grant: AgentSecretGrant | None,
    now: datetime,
) -> Mapping[str, Any]:
    if request.delivery_mode not in SECRET_DELIVERY_MODES:
        return {
            "active": False,
            "reason_code": "nomos.runtime.secret_delivery_unsupported",
            "delivery_mode": request.delivery_mode,
        }
    if request.delivery_mode == "reviewed_literal_break_glass":
        return {
            "active": False,
            "reason_code": "nomos.runtime.secret_break_glass_review_required",
            "delivery_mode": request.delivery_mode,
        }
    if grant is None:
        return {"active": False, "reason_code": "nomos.runtime.secret_grant_missing"}
    closed = _closed_or_expired(grant.status, grant.expires_at, now=now)
    if closed is not None:
        return closed
    expected = {
        "subject_binding_id": request.subject_binding_id,
        "agent_run_id": request.agent_run_id,
        "profile_id": request.profile_id,
        "secret_ref": request.secret_ref,
        "action": request.action,
        "resource_id": request.resource_id,
        "delivery_mode": request.delivery_mode,
    }
    mismatched = [
        field
        for field, expected_value in expected.items()
        if str(getattr(grant, field)).strip() != str(expected_value).strip()
    ]
    if mismatched:
        return {
            "active": False,
            "reason_code": "nomos.runtime.secret_grant_scope_mismatch",
            "mismatched_fields": mismatched,
            "grant_id": grant.grant_id,
        }
    return {
        "active": True,
        "grant_id": grant.grant_id,
        "delivery_mode": grant.delivery_mode,
        "expires_at": _expiry_iso(grant.expires_at),
    }


def _ssh_grant_status(
    request: AgentSshUseRequest,
    *,
    grant: AgentSshGrant | None,
    default_socket_exposures: Sequence[Mapping[str, Any]],
    now: datetime,
) -> Mapping[str, Any]:
    if default_socket_exposures:
        return {
            "active": False,
            "reason_code": "nomos.runtime.ssh_default_socket_exposed",
        }
    if grant is None:
        return {"active": False, "reason_code": "nomos.runtime.ssh_grant_missing"}
    closed = _closed_or_expired(grant.status, grant.expires_at, now=now)
    if closed is not None:
        return closed
    if not is_secret_ref(grant.identity_ref):
        return {
            "active": False,
            "reason_code": "nomos.runtime.ssh_identity_ref_required",
            "grant_id": grant.grant_id,
        }
    mismatched = [
        field
        for field, expected_value in {
            "subject_binding_id": request.subject_binding_id,
            "agent_run_id": request.agent_run_id,
            "profile_id": request.profile_id,
        }.items()
        if str(getattr(grant, field)).strip() != str(expected_value).strip()
    ]
    if mismatched:
        return {
            "active": False,
            "reason_code": "nomos.runtime.ssh_grant_scope_mismatch",
            "mismatched_fields": mismatched,
            "grant_id": grant.grant_id,
        }
    if not grant.allowed_hosts or request.target_host not in grant.allowed_hosts:
        return _ssh_constraint_denied(grant, "host")
    if not grant.allowed_principals or request.principal not in grant.allowed_principals:
        return _ssh_constraint_denied(grant, "principal")
    if not grant.allowed_commands or request.command not in grant.allowed_commands:
        return _ssh_constraint_denied(grant, "command")
    if grant.mechanism != "isolated_ssh_agent":
        return {
            "active": False,
            "reason_code": "nomos.runtime.ssh_isolation_required",
            "grant_id": grant.grant_id,
            "mechanism": grant.mechanism,
        }
    if not _socket_is_runtime_scoped(
        grant.socket_path,
        workspace_root=request.workspace_root,
        agent_run_id=request.agent_run_id,
    ):
        return {
            "active": False,
            "reason_code": "nomos.runtime.ssh_socket_scope_invalid",
            "grant_id": grant.grant_id,
        }
    return {
        "active": True,
        "grant_id": grant.grant_id,
        "mechanism": grant.mechanism,
        "delivery": "wrapper_owned_subprocess",
        "hosts": list(grant.allowed_hosts),
        "principals": list(grant.allowed_principals),
        "socket_scope": "agent_run",
        "expires_at": _expiry_iso(grant.expires_at),
    }


def _ssh_constraint_denied(grant: AgentSshGrant, constraint: str) -> Mapping[str, Any]:
    return {
        "active": False,
        "reason_code": "nomos.runtime.ssh_grant_constraint_mismatch",
        "constraint": constraint,
        "grant_id": grant.grant_id,
    }


def _closed_or_expired(
    status: str,
    expires_at: str,
    *,
    now: datetime,
) -> Mapping[str, Any] | None:
    status_l = str(status).strip().lower()
    if status_l != "active":
        return {
            "active": False,
            "reason_code": "nomos.runtime.grant_closed",
            "status": status_l or "missing",
        }
    expiry = _parse_expiry(expires_at)
    if expiry is None:
        return {"active": False, "reason_code": "nomos.runtime.grant_malformed"}
    if expiry <= now.astimezone(timezone.utc):
        return {
            "active": False,
            "reason_code": "nomos.runtime.grant_expired",
            "expires_at": expiry.isoformat().replace("+00:00", "Z"),
        }
    return None


def _socket_is_runtime_scoped(
    socket_path: str,
    *,
    workspace_root: str | Path,
    agent_run_id: str,
) -> bool:
    root = Path(workspace_root).expanduser().resolve(strict=False)
    expected_root = root / ".okg" / "agent-runtime" / agent_run_id / "ssh"
    socket = Path(socket_path).expanduser().resolve(strict=False)
    try:
        socket.relative_to(expected_root)
    except ValueError:
        return False
    return True


def _value_is_secret_ref(value: Any) -> bool:
    if isinstance(value, str):
        return is_secret_ref(value)
    if isinstance(value, Mapping):
        ref = value.get("secret_ref")
        return isinstance(ref, str) and is_secret_ref(ref)
    return False


def _secret_name_class(name: str) -> str | None:
    upper = name.upper()
    if any(marker in upper for marker in _SECRET_NAME_MARKERS):
        if "DSN" in upper or "DATABASE" in upper:
            return "dsn"
        if "COOKIE" in upper:
            return "cookie"
        if "APIKEY" in upper or "API_KEY" in upper or "ACCESS_KEY" in upper:
            return "token_like"
        if "PRIVATE_KEY" in upper or upper.endswith("_KEY"):
            return "private_key"
        return "token_like"
    return None


def _secret_literal_class(value: str) -> str | None:
    for literal_class, pattern in _SECRET_LITERAL_PATTERNS:
        if pattern.search(value):
            return literal_class
    return None


def _literal_finding(
    *,
    source: str,
    path: Sequence[str],
    literal_class: str,
) -> Mapping[str, Any]:
    return {
        "class": literal_class,
        "source": source,
        "path": ".".join(path) if path else "$",
        "name": str(path[-1]) if path else "$",
    }


def _safe_path(raw: str) -> str:
    return str(Path(raw).expanduser().resolve(strict=False))


def _path_contains(parent: str, child: str) -> bool:
    parent_path = Path(parent).expanduser().resolve(strict=False)
    child_path = Path(child).expanduser().resolve(strict=False)
    try:
        child_path.relative_to(parent_path)
    except ValueError:
        return False
    return True


def _looks_like_ssh_agent_socket_path(raw: str) -> bool:
    text = str(raw).lower()
    name = Path(text).name
    has_launchd_component = any(
        part.startswith("com.apple.launchd") for part in Path(text).parts
    )
    return (
        "ssh_auth_sock" in text
        or "ssh-agent" in text
        or "ssh_agent" in text
        or (has_launchd_component and (name == "listeners" or "launchd" in name))
        or (("ssh" in text or "agent" in text) and "sock" in name)
    )


def _docker_mount_source_paths(mounts: Sequence[str]) -> tuple[str, ...]:
    sources: list[str] = []
    for raw in mounts:
        text = str(raw).strip()
        if not text:
            continue
        source = _docker_key_value_mount_source(text)
        if source:
            sources.append(source)
            continue
        if ":" in text:
            sources.append(text.split(":", 1)[0])
            continue
        sources.append(text)
    return tuple(sources)


def _docker_key_value_mount_source(raw: str) -> str | None:
    for item in raw.split(","):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        if key.strip() in {"source", "src"}:
            return value.strip() or None
    return None


def _parse_expiry(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _expiry_iso(raw: str) -> str:
    parsed = _parse_expiry(raw)
    return "" if parsed is None else parsed.isoformat().replace("+00:00", "Z")


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
