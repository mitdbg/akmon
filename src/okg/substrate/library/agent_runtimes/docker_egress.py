"""Docker and outbound egress gates for profiled agent runs."""
from __future__ import annotations

import hashlib
import ipaddress
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse

from okg.substrate.library.sources._url_safety import (
    UnsafeFetchURL,
    assert_public_http_url,
)
from okg.substrate.nomos import (
    AgentRuntimeDecisionRequest,
    CompiledPolicyBundle,
    NomosPrimitiveCheck,
    NomosRuntimeOutcome,
    evaluate_agent_runtime_decision,
    sanitize_trace_mapping,
)

from .launcher import AGENT_RUNTIME_PROFILES
from .secret_ssh import (
    detect_agent_secret_literals,
    detect_default_ssh_socket_exposure,
    is_secret_ref,
)


DOCKER_ALLOWED_NO_NETWORK_MODES = frozenset({"", "none", "disabled"})
DOCKER_HOST_NETWORK_MODES = frozenset({"host", "container:host"})
EGRESS_ROUTINE_READ_HOSTS = frozenset({
    "docs.python.org",
    "files.pythonhosted.org",
    "github.com",
    "npmjs.com",
    "pypi.org",
    "registry.npmjs.org",
})
UPLOAD_HTTP_METHODS = frozenset({"POST", "PUT", "PATCH"})
NO_URL_NETWORK_CLIENTS = frozenset({"ftp", "sftp", "ssh", "telnet", "wget"})
_DOCKER_SOCKET_MARKERS = ("/var/run/docker.sock", "docker.sock")
_CREDENTIAL_PATH_MARKERS = (
    "/.aws",
    "/.config/gcloud",
    "/.docker",
    "/.gnupg",
    "/.kube",
    "/.netrc",
    "/.ssh",
    "/credentials",
    "/id_rsa",
)
_SECRET_PATH_MARKERS = (
    ".env",
    "credential",
    "id_rsa",
    "key.json",
    "netrc",
    "secret",
    "token",
)
_HTTP_URL_RE = re.compile(r"https?://[^\s'\"<>]+", re.IGNORECASE)


@dataclass(frozen=True)
class AgentDockerMount:
    """One requested Docker bind mount."""

    source: str
    target: str
    mode: str = "ro"


@dataclass(frozen=True)
class AgentDockerRunRequest:
    """One profiled agent request to create a Docker container."""

    subject_binding_id: str
    agent_run_id: str
    profile_id: str
    workspace_root: str | Path
    image: str
    command: Sequence[str] = ()
    env: Mapping[str, Any] = field(default_factory=dict)
    env_files: Sequence[str] = ()
    build_args: Mapping[str, Any] = field(default_factory=dict)
    labels: Mapping[str, Any] = field(default_factory=dict)
    mounts: Sequence[AgentDockerMount] = ()
    network_mode: str = "none"
    privileged: bool = False
    allowed_write_roots: Sequence[str] = ()
    compose_files: Sequence[str] = ()
    devcontainer_files: Sequence[str] = ()
    build_secret_paths: Sequence[str] = ()
    ambient_env: Mapping[str, str] = field(default_factory=dict)
    host_default_ssh_auth_sock: str | None = None
    deployment: str = "default"
    deployment_posture: Mapping[str, Any] = field(default_factory=dict)
    policy_generation_pin: str = "policy:agent-runtime:bootstrap"
    config_generation_pin: str = "config:agent-runtime:bootstrap"


@dataclass(frozen=True)
class AgentEgressRequest:
    """One profiled agent request for outbound network use."""

    subject_binding_id: str
    agent_run_id: str
    profile_id: str
    command: Sequence[str]
    approved_destinations: Sequence[str] = ()
    egress_grant_id: str | None = None
    network_sandbox_active: bool = False
    payload_class: str = "local_payload"
    deployment: str = "default"
    deployment_posture: Mapping[str, Any] = field(default_factory=dict)
    policy_generation_pin: str = "policy:agent-runtime:bootstrap"
    config_generation_pin: str = "config:agent-runtime:bootstrap"


@dataclass(frozen=True)
class AgentEgressGrant:
    """Scoped, time-bounded authorization for one upload-like egress effect."""

    grant_id: str
    subject_binding_id: str
    agent_run_id: str
    profile_id: str
    allowed_hosts: tuple[str, ...]
    command_classes: tuple[str, ...]
    expires_at: str
    payload_class: str = "local_payload"
    status: str = "active"
    reviewer_decision: str = "approved"


@dataclass(frozen=True)
class AgentGateResult:
    """Value-free gate result returned to wrapper code."""

    allowed: bool
    reason_code: str
    outcome: NomosRuntimeOutcome
    trace: Mapping[str, Any]
    resolution: Mapping[str, Any] = field(default_factory=dict)
    command: tuple[str, ...] = ()


@dataclass(frozen=True)
class _EgressCommandShape:
    command_class: str
    destination_url: str | None = None
    destination_host: str | None = None
    destination_class: str = "unknown"
    upload_like: bool = False
    routine_read: bool = False
    direct_bypass: bool = False
    known_network: bool = False


def evaluate_agent_docker_run(
    bundle: CompiledPolicyBundle | None,
    request: AgentDockerRunRequest,
    *,
    active_policy_fingerprint: str | None = None,
    active_config_fingerprint: str | None = None,
) -> AgentGateResult:
    """Evaluate a Docker request before any container can be created."""
    findings = detect_agent_docker_findings(request)
    primitive = _docker_primitive_check(findings)
    image_hash = "sha256:" + _sha256_text(request.image)
    image_class = _image_ref_class(request.image)
    profile_network_class = _profile_network_class(request.profile_id)
    mount_summary = tuple(_mount_summary(mount, request=request) for mount in request.mounts)
    env_summary = _env_summary(request.env)
    candidate_resolution = (
        {
            "gate_contract": "agent.docker_run.evaluate",
            "precreation_decision": True,
            "container_created": False,
            "image_hash": image_hash,
            "image_class": image_class,
            "network_mode": _network_mode(request.network_mode),
            "env": env_summary,
            "mounts": mount_summary,
            "argv": _docker_argv_shape(request),
        }
        if primitive.allowed else {}
    )
    outcome = evaluate_agent_runtime_decision(
        bundle,
        AgentRuntimeDecisionRequest(
            subject_binding_id=request.subject_binding_id,
            principal_kind="agent",
            action="docker.run",
            resource_kind="container",
            resource_id=f"container:image:{image_hash}",
            data_classification="restricted",
            deployment_posture={
                "deployment": request.deployment,
                **dict(request.deployment_posture),
            },
            policy_generation_pin=request.policy_generation_pin,
            config_generation_pin=request.config_generation_pin,
            config_affects_decision=True,
            enforcement_point="docker.run",
            agent_run_id=request.agent_run_id,
            profile_id=request.profile_id,
            context={
                "image_hash": image_hash,
                "image_class": image_class,
                "profile_network_class": profile_network_class,
                "network_mode": _network_mode(request.network_mode),
                "privileged": bool(request.privileged),
                "env": env_summary,
                "mounts": mount_summary,
                "command": _command_shape(request.command, default_executable="docker"),
                "findings": findings,
            },
        ),
        active_policy_fingerprint=active_policy_fingerprint,
        active_config_fingerprint=active_config_fingerprint,
        primitive_checks=(primitive,),
        performed_obligations=("audit",),
        payload=None,
    )
    resolution = candidate_resolution if outcome.allowed else {}
    trace = sanitize_trace_mapping({
        **dict(outcome.trace),
        "agent_docker_gate": {
            "image_hash": image_hash,
            "image_class": image_class,
            "profile_id": request.profile_id,
            "profile_network_class": profile_network_class,
            "network_mode": _network_mode(request.network_mode),
            "privileged": bool(request.privileged),
            "findings": findings,
            "container_created": False,
            "precreation_decision": True,
            "mounts": mount_summary,
            "env": env_summary,
        },
    })
    return AgentGateResult(
        allowed=outcome.allowed,
        reason_code=outcome.decision.reason_code,
        outcome=outcome,
        trace=trace,
        resolution=sanitize_trace_mapping(resolution),
        command=(),
    )


def evaluate_agent_egress(
    bundle: CompiledPolicyBundle | None,
    request: AgentEgressRequest,
    *,
    grant: AgentEgressGrant | None = None,
    active_policy_fingerprint: str | None = None,
    active_config_fingerprint: str | None = None,
    now: datetime | None = None,
) -> AgentGateResult:
    """Evaluate one outbound egress command without persisting payloads."""
    now = now or datetime.now(timezone.utc)
    shape = classify_agent_egress_command(request.command)
    grant_status = _egress_grant_status(request, shape=shape, grant=grant, now=now)
    findings = detect_agent_egress_findings(
        request,
        shape=shape,
        grant_status=grant_status,
    )
    primitive = _egress_primitive_check(findings)
    destination_hash = (
        "sha256:" + _sha256_text(shape.destination_host)
        if shape.destination_host else "network-destination:unknown"
    )
    candidate_resolution = (
        {
            "gate_contract": "agent.network_egress.evaluate",
            "command_class": shape.command_class,
            "destination_class": shape.destination_class,
            "destination_host_hash": destination_hash,
            "egress_grant_id": grant.grant_id if grant is not None else None,
            "egress_grant": grant_status,
            "payload_materialized": False,
        }
        if primitive.allowed and (not shape.upload_like or grant is not None) else {}
    )
    outcome = evaluate_agent_runtime_decision(
        bundle,
        AgentRuntimeDecisionRequest(
            subject_binding_id=request.subject_binding_id,
            principal_kind="agent",
            action="network.egress",
            resource_kind="network_destination",
            resource_id=f"network-destination:{destination_hash}",
            data_classification="restricted",
            deployment_posture={
                "deployment": request.deployment,
                **dict(request.deployment_posture),
            },
            policy_generation_pin=request.policy_generation_pin,
            config_generation_pin=request.config_generation_pin,
            config_affects_decision=True,
            enforcement_point="network.egress",
            agent_run_id=request.agent_run_id,
            profile_id=request.profile_id,
            context={
                "profile_network_class": _profile_network_class(request.profile_id),
                "command": _command_shape(request.command),
                "command_class": shape.command_class,
                "destination_class": shape.destination_class,
                "destination_host_hash": destination_hash,
                "upload_like": shape.upload_like,
                "routine_read": shape.routine_read,
                "direct_bypass": shape.direct_bypass,
                "network_sandbox_active": request.network_sandbox_active,
                "payload_class": request.payload_class,
                "egress_grant": grant_status,
                "findings": findings,
            },
        ),
        active_policy_fingerprint=active_policy_fingerprint,
        active_config_fingerprint=active_config_fingerprint,
        primitive_checks=(primitive,),
        performed_obligations=("audit",),
        payload=None,
    )
    resolution = candidate_resolution if outcome.allowed else {}
    trace = sanitize_trace_mapping({
        **dict(outcome.trace),
        "agent_egress_gate": {
            "profile_id": request.profile_id,
            "profile_network_class": _profile_network_class(request.profile_id),
            "command_class": shape.command_class,
            "destination_class": shape.destination_class,
            "destination_host_hash": destination_hash,
            "upload_like": shape.upload_like,
            "routine_read": shape.routine_read,
            "direct_bypass": shape.direct_bypass,
            "network_sandbox_active": request.network_sandbox_active,
            "payload_materialized": False,
            "egress_grant": grant_status,
            "findings": findings,
        },
    })
    return AgentGateResult(
        allowed=outcome.allowed,
        reason_code=outcome.decision.reason_code,
        outcome=outcome,
        trace=trace,
        resolution=sanitize_trace_mapping(resolution),
        command=(),
    )


def detect_agent_docker_findings(
    request: AgentDockerRunRequest,
) -> tuple[Mapping[str, Any], ...]:
    """Return value-free findings for unsafe Docker request features."""
    findings: list[Mapping[str, Any]] = []
    literal_findings = detect_agent_secret_literals(
        {
            "image": request.image,
            "env": request.env,
            "build_args": request.build_args,
            "labels": request.labels,
            "command": list(request.command),
        },
        source="docker_request",
    )
    for finding in literal_findings:
        findings.append({
            **dict(finding),
            "class": "literal_secret_material",
            "literal_class": str(finding.get("class") or "secret_literal"),
        })

    for source, paths in (
        ("env_file", request.env_files),
        ("compose_file", request.compose_files),
        ("devcontainer_file", request.devcontainer_files),
        ("build_secret", request.build_secret_paths),
    ):
        for path in paths:
            path_class = _secret_path_class(path)
            if path_class:
                findings.append(_path_finding(
                    finding_class=f"{source}_secret_path",
                    source=source,
                    path=path,
                    path_class=path_class,
                ))

    ssh_exposures = detect_default_ssh_socket_exposure(
        request.ambient_env,
        docker_mounts=tuple(_mount_string(mount) for mount in request.mounts),
        host_default_socket=request.host_default_ssh_auth_sock,
    )
    for exposure in ssh_exposures:
        findings.append({
            "class": "inherited_ssh_agent_socket",
            "source": exposure.get("source", "unknown"),
            "path_hash": exposure.get("path_hash", "sha256:unknown"),
        })

    if request.privileged:
        findings.append({"class": "privileged_container"})
    network_mode = _network_mode(request.network_mode)
    if network_mode in DOCKER_HOST_NETWORK_MODES:
        findings.append({"class": "host_network"})
    elif network_mode.startswith("container:"):
        findings.append({"class": "container_network_namespace"})
    elif _profile_network_class(request.profile_id) == "none" and (
        network_mode not in DOCKER_ALLOWED_NO_NETWORK_MODES
    ):
        findings.append({
            "class": "profile_disallowed_network",
            "profile_id": request.profile_id,
            "network_mode": network_mode,
        })

    for mount in request.mounts:
        source = _safe_path(mount.source)
        target = str(mount.target)
        if _looks_like_docker_socket(source) or _looks_like_docker_socket(target):
            findings.append(_path_finding(
                finding_class="docker_socket_mount",
                source="docker_mount",
                path=mount.source,
                path_class="docker_socket",
                target=target,
            ))
        source_secret_path_class = _secret_path_class(source)
        target_secret_path_class = _secret_path_class(target)
        if (
            _looks_like_credential_path(source)
            or _looks_like_credential_path(target)
            or source_secret_path_class
            or target_secret_path_class
        ):
            findings.append(_path_finding(
                finding_class="credential_mount",
                source="docker_mount",
                path=mount.source,
                path_class=source_secret_path_class or target_secret_path_class or "credential_path",
                target=target,
            ))
        if _is_broad_home_mount(source):
            findings.append(_path_finding(
                finding_class="broad_home_mount",
                source="docker_mount",
                path=mount.source,
                path_class="home_directory",
                target=target,
            ))
        if not _path_under_any(source, _profile_read_roots(request)):
            findings.append(_path_finding(
                finding_class="unapproved_read_mount",
                source="docker_mount",
                path=mount.source,
                path_class="read_outside_profile",
                target=target,
            ))
        if _mount_is_writable(mount) and not _path_under_any(source, _profile_write_roots(request)):
            findings.append(_path_finding(
                finding_class="unapproved_writable_mount",
                source="docker_mount",
                path=mount.source,
                path_class="writable_outside_workspace",
                target=target,
            ))
    return _dedupe_findings(findings)


def classify_agent_egress_command(command: Sequence[str]) -> _EgressCommandShape:
    """Classify command-wrapper-visible network behavior."""
    tokens = tuple(str(item) for item in command if str(item))
    if not tokens:
        return _EgressCommandShape(command_class="empty")
    exe = Path(tokens[0]).name.lower()
    text = " ".join(tokens)
    url = _first_url(tokens)
    host = _url_host(url)
    destination_class = _destination_class(url, fallback=exe)

    if exe == "curl":
        method = _curl_method(tokens)
        upload_like = method in UPLOAD_HTTP_METHODS or any(
            _curl_token_uploads(token) for token in tokens
        )
        return _EgressCommandShape(
            command_class="curl_upload" if upload_like else "curl_read",
            destination_url=url,
            destination_host=host,
            destination_class=destination_class,
            upload_like=upload_like,
            routine_read=not upload_like,
            known_network=True,
        )
    if exe in {"scp", "rsync"}:
        return _EgressCommandShape(
            command_class=f"{exe}_upload",
            destination_host=_remote_destination_host(tokens),
            destination_class="ssh_remote",
            upload_like=True,
            known_network=True,
        )
    if exe == "git" and len(tokens) > 1 and tokens[1] == "push":
        return _EgressCommandShape(
            command_class="git_push",
            destination_url=url,
            destination_host=host,
            destination_class="git_remote",
            upload_like=True,
            known_network=True,
        )
    if exe == "gh" and {"release", "upload"}.issubset(set(tokens[1:])):
        return _EgressCommandShape(
            command_class="gh_release_upload",
            destination_class="github_release",
            upload_like=True,
            known_network=True,
        )
    if exe == "gh" and "gist" in tokens[1:]:
        return _EgressCommandShape(
            command_class="gh_gist_upload",
            destination_class="paste_or_gist",
            upload_like=True,
            known_network=True,
        )
    if exe == "docker" and len(tokens) > 1 and tokens[1] == "push":
        return _EgressCommandShape(
            command_class="docker_push",
            destination_class="container_registry",
            upload_like=True,
            known_network=True,
        )
    if exe in {"npm", "pnpm", "yarn"} and "publish" in tokens[1:]:
        return _EgressCommandShape(
            command_class="package_publish",
            destination_class="package_registry",
            upload_like=True,
            known_network=True,
        )
    if exe == "twine" and "upload" in tokens[1:]:
        return _EgressCommandShape(
            command_class="package_publish",
            destination_class="package_registry",
            upload_like=True,
            known_network=True,
        )
    if exe in {"rclone", "aws", "gsutil"}:
        return _EgressCommandShape(
            command_class=f"{exe}_cloud_upload",
            destination_class="cloud_storage",
            upload_like=True,
            known_network=True,
        )
    if exe in {"nc", "netcat", "socat", "openssl"}:
        return _EgressCommandShape(
            command_class=f"{exe}_direct_socket",
            destination_class="raw_socket",
            direct_bypass=True,
            known_network=True,
        )
    if exe in NO_URL_NETWORK_CLIENTS:
        return _EgressCommandShape(
            command_class=f"{exe}_network_client",
            destination_class="network_client",
            direct_bypass=True,
            known_network=True,
        )
    if exe in {"sh", "bash", "zsh", "fish"} and _one_liner_uses_network(text):
        return _EgressCommandShape(
            command_class="shell_network_client",
            destination_url=url,
            destination_host=host,
            destination_class=destination_class,
            upload_like=_one_liner_uploads(text),
            direct_bypass=True,
            known_network=True,
        )
    if exe in {"python", "python3", "node"} and _one_liner_uses_network(text):
        upload_like = _one_liner_uploads(text)
        return _EgressCommandShape(
            command_class=f"{exe}_network_client",
            destination_url=url,
            destination_host=host,
            destination_class=destination_class,
            upload_like=upload_like,
            routine_read=not upload_like and url is not None,
            direct_bypass=not upload_like and url is None,
            known_network=True,
        )
    return _EgressCommandShape(
        command_class="unknown_network_client" if url else "unknown",
        destination_url=url,
        destination_host=host,
        destination_class=destination_class,
        direct_bypass=url is not None,
        known_network=url is not None,
    )


def detect_agent_egress_findings(
    request: AgentEgressRequest,
    *,
    shape: _EgressCommandShape | None = None,
    grant_status: Mapping[str, Any] | None = None,
) -> tuple[Mapping[str, Any], ...]:
    """Return value-free findings for unsafe or uncovered egress behavior."""
    shape = shape or classify_agent_egress_command(request.command)
    grant_status = grant_status or {"active": False, "reason_code": "nomos.runtime.network_grant_missing"}
    findings: list[Mapping[str, Any]] = []
    profile_network = _profile_network_class(request.profile_id)
    approved_hosts = _approved_hosts(request.approved_destinations)
    literal_findings = detect_agent_secret_literals(
        {"command": list(request.command)},
        source="egress_request",
    )
    for finding in literal_findings:
        findings.append({
            **dict(finding),
            "class": "literal_secret_material",
            "literal_class": str(finding.get("class") or "secret_literal"),
        })
    if _command_uses_inline_credentials(request.command):
        findings.append({
            "class": "literal_secret_material",
            "literal_class": "credentialed_egress_argv",
            "source": "egress_request",
        })
    if _command_overrides_network_destination(request.command):
        findings.append({
            "class": "unsafe_destination_override",
            "command_class": shape.command_class,
            "prevention": "requires_trusted_proxy_or_sandbox",
        })

    if shape.destination_url:
        try:
            assert_public_http_url(shape.destination_url)
        except UnsafeFetchURL:
            findings.append({
                "class": "unsafe_destination",
                "destination_class": shape.destination_class,
                "destination_host_hash": "sha256:" + _sha256_text(
                    shape.destination_host or ""
                ),
            })
    elif shape.destination_host and _host_is_private_destination(shape.destination_host):
        findings.append({
            "class": "unsafe_destination",
            "destination_class": shape.destination_class,
            "destination_host_hash": "sha256:" + _sha256_text(shape.destination_host),
        })

    if profile_network == "unknown":
        findings.append({"class": "unknown_network_profile", "profile_id": request.profile_id})
    if shape.known_network and profile_network == "none":
        findings.append({
            "class": "profile_network_denied",
            "profile_id": request.profile_id,
            "profile_network_class": profile_network,
        })
    if shape.direct_bypass:
        findings.append({
            "class": "direct_bypass_uncovered",
            "command_class": shape.command_class,
            "prevention": "requires_trusted_sandbox_attestation",
        })
    if (
        shape.routine_read
        and shape.known_network
        and profile_network == "local_dev"
        and not _routine_read_destination(shape, approved_hosts)
    ):
        findings.append({
            "class": "unapproved_read_destination",
            "command_class": shape.command_class,
            "destination_class": shape.destination_class,
        })
    if shape.upload_like and not _destination_approved(
        shape,
        approved_hosts,
    ):
        findings.append({
            "class": "unapproved_upload_destination",
            "command_class": shape.command_class,
            "destination_class": shape.destination_class,
        })
    grant_active = bool(grant_status.get("active"))
    if shape.upload_like and not grant_active:
        reason_code = str(grant_status.get("reason_code") or "")
        if reason_code != "nomos.runtime.network_grant_missing":
            findings.append({
                "class": _egress_grant_finding_class(reason_code),
                "reason_code": reason_code or "nomos.runtime.network_grant_invalid",
            })
    if shape.upload_like and request.profile_id == "deploy" and not grant_active:
        findings.append({
            "class": "deploy_write_requires_review",
            "command_class": shape.command_class,
        })
    if shape.upload_like and request.profile_id != "deploy" and not grant_active:
        findings.append({
            "class": "write_network_requires_task_grant",
            "profile_id": request.profile_id,
            "command_class": shape.command_class,
        })
    return _dedupe_findings(findings)


def _docker_primitive_check(
    findings: Sequence[Mapping[str, Any]],
) -> NomosPrimitiveCheck:
    if findings:
        return NomosPrimitiveCheck(
            name="agent.docker_policy",
            allowed=False,
            reason_code=_docker_reason_code(findings),
            metadata={"findings": list(findings)},
        )
    return NomosPrimitiveCheck(
        name="agent.docker_policy",
        allowed=True,
        reason_code="nomos.runtime.docker_policy_allowed",
        metadata={"precreation_decision": True},
    )


def _egress_primitive_check(
    findings: Sequence[Mapping[str, Any]],
) -> NomosPrimitiveCheck:
    if findings:
        return NomosPrimitiveCheck(
            name="agent.network_egress_policy",
            allowed=False,
            reason_code=_egress_reason_code(findings),
            metadata={"findings": list(findings)},
        )
    return NomosPrimitiveCheck(
        name="agent.network_egress_policy",
        allowed=True,
        reason_code="nomos.runtime.network_egress_allowed",
        metadata={"payload_materialized": False},
    )


def _docker_reason_code(findings: Sequence[Mapping[str, Any]]) -> str:
    classes = {str(finding.get("class") or "") for finding in findings}
    for class_name, reason in (
        ("literal_secret_material", "nomos.runtime.docker_secret_material_denied"),
        ("env_file_secret_path", "nomos.runtime.docker_secret_material_denied"),
        ("compose_file_secret_path", "nomos.runtime.docker_secret_material_denied"),
        ("devcontainer_file_secret_path", "nomos.runtime.docker_secret_material_denied"),
        ("build_secret_secret_path", "nomos.runtime.docker_secret_material_denied"),
        ("inherited_ssh_agent_socket", "nomos.runtime.docker_ssh_socket_denied"),
        ("docker_socket_mount", "nomos.runtime.docker_socket_mount_denied"),
        ("broad_home_mount", "nomos.runtime.docker_mount_denied"),
        ("credential_mount", "nomos.runtime.docker_mount_denied"),
        ("unapproved_read_mount", "nomos.runtime.docker_mount_denied"),
        ("unapproved_writable_mount", "nomos.runtime.docker_mount_denied"),
        ("privileged_container", "nomos.runtime.docker_privileged_denied"),
        ("host_network", "nomos.runtime.docker_host_network_denied"),
        ("container_network_namespace", "nomos.runtime.docker_host_network_denied"),
        ("profile_disallowed_network", "nomos.runtime.docker_network_denied"),
    ):
        if class_name in classes:
            return reason
    return "nomos.runtime.docker_policy_denied"


def _egress_reason_code(findings: Sequence[Mapping[str, Any]]) -> str:
    classes = {str(finding.get("class") or "") for finding in findings}
    for class_name, reason in (
        ("literal_secret_material", "nomos.runtime.network_secret_material_denied"),
        ("profile_network_denied", "nomos.runtime.network_profile_denied"),
        ("unsafe_destination_override", "nomos.runtime.network_destination_denied"),
        ("unsafe_destination", "nomos.runtime.network_destination_denied"),
        ("direct_bypass_uncovered", "nomos.runtime.network_direct_bypass_uncovered"),
        ("unapproved_read_destination", "nomos.runtime.network_destination_denied"),
        ("egress_grant_closed", "nomos.runtime.grant_closed"),
        ("egress_grant_expired", "nomos.runtime.grant_expired"),
        ("egress_grant_malformed", "nomos.runtime.grant_malformed"),
        ("egress_grant_unverified", "nomos.runtime.network_grant_unverified"),
        ("egress_grant_unapproved", "nomos.runtime.network_grant_unapproved"),
        ("egress_grant_scope_mismatch", "nomos.runtime.network_grant_scope_mismatch"),
        ("egress_grant_constraint_mismatch", "nomos.runtime.network_grant_constraint_mismatch"),
        ("deploy_write_requires_review", "nomos.runtime.network_write_requires_review"),
        ("unapproved_upload_destination", "nomos.runtime.network_upload_denied"),
        ("write_network_requires_task_grant", "nomos.runtime.network_write_requires_grant"),
        ("unknown_network_profile", "nomos.runtime.network_profile_unknown"),
    ):
        if class_name in classes:
            return reason
    return "nomos.runtime.network_egress_denied"


def _egress_grant_finding_class(reason_code: str) -> str:
    return {
        "nomos.runtime.grant_closed": "egress_grant_closed",
        "nomos.runtime.grant_expired": "egress_grant_expired",
        "nomos.runtime.grant_malformed": "egress_grant_malformed",
        "nomos.runtime.network_grant_unverified": "egress_grant_unverified",
        "nomos.runtime.network_grant_unapproved": "egress_grant_unapproved",
        "nomos.runtime.network_grant_scope_mismatch": "egress_grant_scope_mismatch",
        "nomos.runtime.network_grant_constraint_mismatch": "egress_grant_constraint_mismatch",
    }.get(reason_code, "egress_grant_constraint_mismatch")


def _env_summary(env: Mapping[str, Any]) -> tuple[Mapping[str, str], ...]:
    rows: list[Mapping[str, str]] = []
    for name, value in sorted(env.items(), key=lambda item: str(item[0])):
        name_s = str(name)
        rows.append({
            "name": name_s,
            "class": "secret_ref" if _value_is_secret_ref(value) else "literal",
        })
    return tuple(rows)


def _mount_summary(
    mount: AgentDockerMount,
    *,
    request: AgentDockerRunRequest,
) -> Mapping[str, str]:
    source = _safe_path(mount.source)
    if _looks_like_docker_socket(source) or _looks_like_docker_socket(mount.target):
        mount_class = "docker_socket"
    elif _looks_like_credential_path(source):
        mount_class = "credential_path"
    elif _secret_path_class(source):
        mount_class = "secret_path"
    elif _looks_like_credential_path(mount.target):
        mount_class = "credential_target"
    elif _secret_path_class(mount.target):
        mount_class = "secret_target"
    elif _is_broad_home_mount(source):
        mount_class = "home_directory"
    elif _path_under_any(source, (_safe_path(request.workspace_root),)):
        mount_class = "workspace"
    else:
        mount_class = "external_path"
    return {
        "source_hash": "sha256:" + _sha256_text(source),
        "target": str(mount.target),
        "mode": str(mount.mode or "ro"),
        "class": mount_class,
    }


def _docker_argv_shape(request: AgentDockerRunRequest) -> Mapping[str, Any]:
    return {
        "executable": "docker",
        "subcommand": "run",
        "argc": len(request.command),
        "command_hash": "sha256:" + _sha256_json(list(request.command)),
    }


def _command_shape(
    command: Sequence[str],
    *,
    default_executable: str = "",
) -> Mapping[str, Any]:
    executable = Path(str(command[0])).name if command else default_executable
    return {
        "executable": executable,
        "argc": len(command),
        "argv_hash": "sha256:" + _sha256_json(list(command)),
    }


def _value_is_secret_ref(value: Any) -> bool:
    if isinstance(value, str):
        return is_secret_ref(value)
    if isinstance(value, Mapping):
        ref = value.get("secret_ref")
        return isinstance(ref, str) and is_secret_ref(ref)
    return False


def _image_ref_class(image_ref: str) -> str:
    text = str(image_ref)
    if "/" not in text:
        return "named_image"
    first = text.split("/", 1)[0].lower()
    if "." in first or first == "localhost" or ":" in first:
        return "registry_image"
    return "named_image"


def _path_finding(
    *,
    finding_class: str,
    source: str,
    path: str,
    path_class: str,
    target: str | None = None,
) -> Mapping[str, str]:
    finding = {
        "class": finding_class,
        "source": source,
        "path_class": path_class,
        "path_hash": "sha256:" + _sha256_text(_safe_path(path)),
    }
    if target:
        finding["target"] = target
    return finding


def _secret_path_class(path: str) -> str | None:
    text = str(path).lower()
    name = Path(text).name
    if any(marker in text for marker in _SECRET_PATH_MARKERS):
        return "secret_path"
    if name in {"config.json", "credentials.json"}:
        return "credential_path"
    return None


def _looks_like_docker_socket(path: str) -> bool:
    text = str(path).lower()
    return any(marker in text for marker in _DOCKER_SOCKET_MARKERS)


def _looks_like_credential_path(path: str) -> bool:
    text = str(path).lower()
    return any(marker in text for marker in _CREDENTIAL_PATH_MARKERS)


def _is_broad_home_mount(path: str) -> bool:
    source = Path(path).expanduser().resolve(strict=False)
    home = Path.home().expanduser().resolve(strict=False)
    return source == home


def _mount_is_writable(mount: AgentDockerMount) -> bool:
    mode = str(mount.mode or "").lower()
    return mode in {"", "rw", "z", "rw,z"} or "rw" in mode


def _profile_read_roots(request: AgentDockerRunRequest) -> tuple[str, ...]:
    profile = AGENT_RUNTIME_PROFILES.get(str(request.profile_id))
    if profile is None:
        return ()
    return _resolve_profile_roots(profile.allowed_read_roots, request.workspace_root)


def _profile_write_roots(request: AgentDockerRunRequest) -> tuple[str, ...]:
    profile = AGENT_RUNTIME_PROFILES.get(str(request.profile_id))
    if profile is None:
        return ()
    return _resolve_profile_roots(profile.allowed_write_roots, request.workspace_root)


def _resolve_profile_roots(
    roots: Sequence[str],
    workspace_root: str | Path,
) -> tuple[str, ...]:
    workspace = Path(workspace_root).expanduser().resolve(strict=False)
    resolved: list[str] = []
    for raw in roots:
        value = str(raw).replace("$WORKSPACE_ROOT", str(workspace))
        resolved.append(_safe_path(value))
    return tuple(resolved)


def _path_under_any(path: str, roots: Sequence[str]) -> bool:
    source = Path(path).expanduser().resolve(strict=False)
    for root in roots:
        root_path = Path(root).expanduser().resolve(strict=False)
        try:
            source.relative_to(root_path)
            return True
        except ValueError:
            continue
    return False


def _mount_string(mount: AgentDockerMount) -> str:
    return f"{mount.source}:{mount.target}:{mount.mode}"


def _network_mode(value: str) -> str:
    return str(value or "none").strip().lower()


def _profile_network_class(profile_id: str) -> str:
    profile = AGENT_RUNTIME_PROFILES.get(str(profile_id))
    return profile.network_class if profile is not None else "unknown"


def _first_url(tokens: Sequence[str]) -> str | None:
    for token in tokens:
        match = _HTTP_URL_RE.search(str(token))
        if match:
            return match.group(0)
    return None


def _url_host(url: str | None) -> str | None:
    if not url:
        return None
    return urlparse(url).hostname


def _destination_class(url: str | None, *, fallback: str) -> str:
    host = _url_host(url)
    if not host:
        return "unknown" if fallback == "unknown" else fallback
    host_l = host.lower()
    if "hook" in host_l or "webhook" in host_l:
        return "webhook"
    if "paste" in host_l or "gist" in host_l:
        return "paste_or_gist"
    if host_l in EGRESS_ROUTINE_READ_HOSTS:
        return "routine_read"
    return "external_endpoint"


def _curl_method(tokens: Sequence[str]) -> str:
    method = "GET"
    for index, token in enumerate(tokens):
        token_s = str(token)
        if token_s in {"-X", "--request"} and index + 1 < len(tokens):
            method = str(tokens[index + 1]).upper()
        elif token_s.startswith("-X") and len(token_s) > 2:
            method = token_s[2:].upper()
        elif token_s.startswith("--request="):
            method = token_s.split("=", 1)[1].upper()
    return method


def _curl_token_uploads(token: str) -> bool:
    token_s = str(token)
    token_l = token_s.lower()
    if token_l in {
        "-d",
        "--data",
        "--data-raw",
        "--data-binary",
        "--form",
        "--json",
        "--upload-file",
    }:
        return True
    if token_s in {"-F", "-T"}:
        return True
    if token_l.startswith("--data") or token_l.startswith("--form"):
        return True
    if token_l.startswith("--json"):
        return True
    if token_l.startswith("--upload-file="):
        return True
    if token_s.startswith("-d") and token_l != "-doh-url":
        return True
    if token_s.startswith("-F") or token_s.startswith("-T"):
        return True
    return False


def _command_uses_inline_credentials(command: Sequence[str]) -> bool:
    tokens = tuple(str(item) for item in command if str(item))
    if not tokens or Path(tokens[0]).name.lower() != "curl":
        return False
    credential_flags = {
        "-b",
        "-u",
        "--cookie",
        "--oauth2-bearer",
        "--proxy-user",
        "--user",
    }
    long_header_flags = {"--header", "--proxy-header"}
    header_markers = ("authorization", "cookie", "x-api-key", "x-auth-token")
    for index, token in enumerate(tokens):
        token_s = str(token)
        token_l = token.lower()
        if token_l in credential_flags:
            return True
        if any(token_l.startswith(f"{flag}=") for flag in credential_flags if flag.startswith("--")):
            return True
        if token_s.startswith("-b"):
            return True
        if token_l.startswith("-u") and token_l != "-url":
            return True
        if (token == "-H" or token_l in long_header_flags) and index + 1 < len(tokens):
            header_value = tokens[index + 1].lower()
            if any(marker in header_value for marker in header_markers):
                return True
        if any(
            token_l.startswith(f"{flag}=") and any(marker in token_l for marker in header_markers)
            for flag in long_header_flags
        ):
            return True
        if token.startswith("-H") and any(marker in token_l for marker in header_markers):
            return True
    return False


def _command_overrides_network_destination(command: Sequence[str]) -> bool:
    tokens = tuple(str(item) for item in command if str(item))
    if not tokens or Path(tokens[0]).name.lower() != "curl":
        return False
    override_flags = {
        "--connect-to",
        "--dns-interface",
        "--dns-ipv4-addr",
        "--dns-ipv6-addr",
        "--dns-servers",
        "--doh-url",
        "--interface",
        "--preproxy",
        "--proxy",
        "--proxy1.0",
        "--resolve",
        "--socks4",
        "--socks4a",
        "--socks5",
        "--socks5-basic",
        "--socks5-gssapi",
        "--socks5-gssapi-nec",
        "--socks5-gssapi-service",
        "--socks5-hostname",
    }
    short_override_flags = {"--abstract-unix-socket", "--unix-socket"}
    for index, token in enumerate(tokens):
        token_l = token.lower()
        if token_l in override_flags or token_l in short_override_flags:
            return True
        if any(token_l.startswith(f"{flag}=") for flag in override_flags | short_override_flags):
            return True
        if token == "-x" and index + 1 < len(tokens):
            return True
        if token.startswith("-x") and len(token) > 2:
            return True
    return False


def _remote_destination_host(tokens: Sequence[str]) -> str | None:
    for token in reversed(tokens):
        text = str(token)
        if ":" not in text:
            continue
        left = text.split(":", 1)[0]
        if "@" in left:
            return left.rsplit("@", 1)[1] or None
        if "/" not in left:
            return left or None
    return None


def _one_liner_uses_network(text: str) -> bool:
    lower = text.lower()
    return any(
        marker in lower
        for marker in (
            "curl ",
            "fetch(",
            "http.client",
            "http ",
            "httpie ",
            "https://",
            "http://",
            "nc ",
            "netcat ",
            "requests.",
            "socket.",
            "urllib.request",
            "wget ",
        )
    )


def _one_liner_uploads(text: str) -> bool:
    lower = text.lower()
    return any(
        marker in lower
        for marker in (
            "fetch(",
            " -d ",
            " --data",
            " --form",
            " --post",
            " --upload-file",
            " -x post",
            " -x put",
            " -x patch",
            "post ",
            "method:'post'",
            'method:"post"',
            "requests.post",
            "requests.put",
            "requests.patch",
            "urlopen(",
        )
    )


def _approved_hosts(destinations: Sequence[str]) -> frozenset[str]:
    hosts: set[str] = set()
    for raw in destinations:
        text = str(raw).strip()
        if not text:
            continue
        host = urlparse(text).hostname if "://" in text else text
        if host:
            hosts.add(host.lower())
    return frozenset(hosts)


def _destination_approved(
    shape: _EgressCommandShape,
    approved_hosts: frozenset[str],
) -> bool:
    host = (shape.destination_host or "").lower()
    if host:
        return _host_in_patterns(host, approved_hosts)
    return False


def _routine_read_destination(
    shape: _EgressCommandShape,
    approved_hosts: frozenset[str],
) -> bool:
    host = (shape.destination_host or "").lower()
    if not host:
        return False
    if host in EGRESS_ROUTINE_READ_HOSTS:
        return True
    return _host_in_patterns(host, approved_hosts)


def _egress_grant_status(
    request: AgentEgressRequest,
    *,
    shape: _EgressCommandShape,
    grant: AgentEgressGrant | None,
    now: datetime,
) -> Mapping[str, Any]:
    if not shape.upload_like:
        return {"required": False, "active": True}
    if grant is None:
        return {
            "required": True,
            "active": False,
            "reason_code": (
                "nomos.runtime.network_grant_unverified"
                if request.egress_grant_id else "nomos.runtime.network_grant_missing"
            ),
        }
    closed = _closed_or_expired(grant.status, grant.expires_at, now=now)
    if closed is not None:
        return {"required": True, "grant_id": grant.grant_id, **closed}
    if str(grant.reviewer_decision).strip().lower() != "approved":
        return {
            "required": True,
            "active": False,
            "reason_code": "nomos.runtime.network_grant_unapproved",
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
    if request.egress_grant_id and request.egress_grant_id != grant.grant_id:
        mismatched.append("egress_grant_id")
    if mismatched:
        return {
            "required": True,
            "active": False,
            "reason_code": "nomos.runtime.network_grant_scope_mismatch",
            "mismatched_fields": mismatched,
            "grant_id": grant.grant_id,
        }
    if grant.payload_class not in {"*", request.payload_class}:
        return _egress_grant_constraint_denied(grant, "payload_class")
    if not grant.command_classes or shape.command_class not in grant.command_classes:
        return _egress_grant_constraint_denied(grant, "command_class")
    host = (shape.destination_host or "").lower()
    if not host or not _host_in_patterns(host, frozenset(grant.allowed_hosts)):
        return _egress_grant_constraint_denied(grant, "destination_host")
    return {
        "required": True,
        "active": True,
        "grant_id": grant.grant_id,
        "destination_host_hash": "sha256:" + _sha256_text(host),
        "command_classes": list(grant.command_classes),
        "payload_class": grant.payload_class,
        "expires_at": _expiry_iso(grant.expires_at),
    }


def _egress_grant_constraint_denied(
    grant: AgentEgressGrant,
    constraint: str,
) -> Mapping[str, Any]:
    return {
        "required": True,
        "active": False,
        "reason_code": "nomos.runtime.network_grant_constraint_mismatch",
        "constraint": constraint,
        "grant_id": grant.grant_id,
    }


def _host_in_patterns(host: str, patterns: frozenset[str]) -> bool:
    host_l = str(host).lower()
    return host_l in patterns or any(
        pattern.startswith(".") and (host_l == pattern[1:] or host_l.endswith(pattern))
        for pattern in patterns
    )


def _host_is_private_destination(host: str) -> bool:
    host_l = str(host or "").strip().strip("[]").lower()
    if not host_l:
        return False
    if host_l == "localhost" or host_l.endswith(".localhost"):
        return True
    try:
        ip = ipaddress.ip_address(host_l)
    except ValueError:
        return False
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    )


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


def _dedupe_findings(findings: Sequence[Mapping[str, Any]]) -> tuple[Mapping[str, Any], ...]:
    deduped: dict[str, Mapping[str, Any]] = {}
    for finding in findings:
        key = json.dumps(finding, sort_keys=True, default=str)
        deduped[key] = finding
    return tuple(deduped.values())


def _safe_path(path: str | Path) -> str:
    return str(Path(path).expanduser().resolve(strict=False))


def _sha256_json(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return _sha256_text(encoded)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
