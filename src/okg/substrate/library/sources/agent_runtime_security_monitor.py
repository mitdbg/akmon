"""Privacy-safe agent runtime security findings.

This source intentionally treats process, Docker, SSH-agent, egress, and
LaunchAgent snapshots as ephemeral input. It emits value-free
``security_finding`` nodes and does not carry raw snapshots in attrs, source
revisions, record sets, or cursors.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence
from urllib.parse import urlparse

from okg.substrate.library.sources.base import (
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)


__all__ = [
    "AgentRuntimeSecurityMonitorSource",
    "AGENT_RUNTIME_SECURITY_ALLOWED_ATTRS",
    "agent_runtime_security_findings",
]


TOOL_NAME = "okg-agent-runtime-monitor"
MONITOR_VERSION = "agent-runtime-security-monitor.v1"

AGENT_RUNTIME_SECURITY_ALLOWED_ATTRS = frozenset({
    "affected_processes",
    "age_days",
    "agent",
    "arg_keys",
    "bind_address",
    "bytes_out_class",
    "category",
    "confidence",
    "coverage_status",
    "decision",
    "dsn_schemes",
    "end_line",
    "env_classes",
    "env_names",
    "evidence_refs",
    "finding_class",
    "finding_id",
    "fingerprint",
    "first_seen",
    "key_path_classes",
    "key_refs",
    "last_seen",
    "message",
    "missing_sections",
    "observed_at",
    "owner",
    "path",
    "permission_mode",
    "port",
    "remediation",
    "repo",
    "rule_id",
    "sensitive_path_classes",
    "service",
    "severity",
    "start_line",
    "status",
    "text",
    "tool",
})

_EXPECTED_SECTIONS = (
    "processes",
    "ssh_agent",
    "egress_events",
    "launch_agents",
    "listeners",
    "sessions",
)
_TOKEN_NAME_RE = re.compile(
    r"(?i)(?:^|[_-])(api[_-]?key|auth|bearer|credential|github[_-]?token|"
    r"openai[_-]?api[_-]?key|anthropic[_-]?api[_-]?key|password|passwd|"
    r"secret|token)(?:$|[_-])"
)
_BENIGN_TOKEN_NAME_RE = re.compile(
    r"(?i)^(?:path|home|shell|term|user|logname|lang|pwd|tmpdir|ssh_auth_sock)$"
)
_TOKEN_VALUE_RE = re.compile(
    r"(?i)(?:sk-[A-Za-z0-9_-]{12,}|github_pat_[A-Za-z0-9_]{12,}|"
    r"gh[opsu]_[A-Za-z0-9_]{12,}|xox[baprs]-[A-Za-z0-9-]{12,}|"
    r"AKIA[0-9A-Z]{12,}|eyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,})"
)
_DSN_RE = re.compile(r"(?i)\b([a-z][a-z0-9+.-]{1,24})://[^\s]+")
_SENSITIVE_PATH_MARKERS = (
    "/.ssh/",
    "/.aws/credentials",
    "/.config/gcloud/",
    "/.docker/config.json",
    "/.netrc",
    "/.npmrc",
    "/.pypirc",
    "/.env",
)
_BYPASS_MODES = {
    "bypasspermissions",
    "bypass-permissions",
    "dangerously-skip-permissions",
    "skippermissions",
    "auto-approve",
    "auto_accept_edits",
    "danger-full-access",
    "full-access",
}
_PUBLIC_BINDS = {"0.0.0.0", "::", "[::]"}
_LOOPBACK_BINDS = {"127.0.0.1", "::1", "[::1]", "localhost"}
_HIGH_RISK_KEY_NAMES = {
    "id_dsa",
    "id_ecdsa",
    "id_ed25519",
    "id_rsa",
    "identity",
}


@dataclass
class AgentRuntimeSecurityMonitorSource:
    """Import sanitized agent runtime security findings from snapshots."""

    repo: str
    repo_root: Path | str = Path(".")
    snapshots: Sequence[Mapping[str, Any]] = ()
    snapshot_paths: Sequence[Path | str] = ()
    snapshot_globs: Sequence[str] = ()
    expected_sections: Sequence[str] = _EXPECTED_SECTIONS

    name: str = "agent_runtime_security_monitor"
    profile: str = "discovery_crawl"
    # The reader does not yet bind emitted facts to an exact captured probe
    # snapshot.  Conservatively force reads until that adapter contract exists.
    probe_short_circuit_safe = False
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        self.repo_root = Path(self.repo_root).expanduser().resolve()
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        return ContentHashProbe(
            content_items=self._probe_content_items,
            config={
                "repo": self.repo,
                "repo_root": str(self.repo_root),
                "snapshot_paths": [str(path) for path in self.snapshot_paths],
                "snapshot_globs": list(self.snapshot_globs),
                "expected_sections": list(self.expected_sections),
            },
            emit_targets=[sys.modules[__name__]],
        )

    def _probe_content_items(self) -> list[tuple[str, bytes | Path]]:
        items: list[tuple[str, bytes | Path]] = [
            (
                f"in_memory:{index}",
                json.dumps(
                    snapshot,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    default=str,
                ).encode("utf-8"),
            )
            for index, snapshot in enumerate(self.snapshots)
        ]
        items.extend(
            (f"file:{path}", path)
            for path in self._snapshot_files()
        )
        return items

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        del cursor, sync_scope
        findings = agent_runtime_security_findings(
            self.repo,
            [*self.snapshots, *self._read_snapshot_paths()],
            expected_sections=self.expected_sections,
        )
        revision = {
            "run_id": run_id,
            "monitor_version": MONITOR_VERSION,
        }
        completed_scope = mode in {"release_new", "scope_complete", "reconcile"}
        record_set = (
            _record_set_for_findings(self.repo, findings, revision=revision)
            if completed_scope
            else None
        )

        facts = [
            NodeFact(
                node_id=finding["finding_id"],
                subtype="security_finding",
                attrs=_allowlisted_attrs(finding),
                source_record_id={
                    "repo": self.repo,
                    "finding_id": finding["finding_id"],
                },
                source_revision=revision,
                op="I",
            )
            for finding in findings
        ]
        return SourceRun(
            facts=iter(facts),
            completed_scope=completed_scope,
            next_cursor={
                "monitor_version": MONITOR_VERSION,
                "finding_count": len(facts),
                "finding_fingerprints": [
                    fact.attrs["fingerprint"] for fact in facts
                ],
            },
            run_mode=mode,
            record_set=record_set,
        )

    def _read_snapshot_paths(self) -> list[Mapping[str, Any]]:
        snapshots: list[Mapping[str, Any]] = []
        for path in self._snapshot_files():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except OSError as exc:
                raise RuntimeError(
                    f"agent runtime security snapshot is unreadable: {path}"
                ) from exc
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"agent runtime security snapshot is malformed JSON: {path}"
                ) from exc
            if isinstance(payload, Mapping):
                snapshots.append(payload)
            elif isinstance(payload, list):
                for index, item in enumerate(payload):
                    if not isinstance(item, Mapping):
                        raise ValueError(
                            "agent runtime security snapshot list item "
                            f"{index} is not an object: {path}"
                        )
                    snapshots.append(item)
            else:
                raise ValueError(
                    "agent runtime security snapshot must contain an object "
                    f"or list of objects: {path}"
                )
        return snapshots

    def _snapshot_files(self) -> list[Path]:
        files: set[Path] = set()
        for raw_path in self.snapshot_paths:
            path = Path(raw_path).expanduser()
            files.add(path if path.is_absolute() else Path(self.repo_root) / path)
        for pattern in self.snapshot_globs:
            files.update(
                path
                for path in Path(self.repo_root).glob(str(pattern))
                if path.is_file()
            )
        return sorted(path.resolve() for path in files)


def agent_runtime_security_findings(
    repo: str,
    snapshots: Sequence[Mapping[str, Any]],
    *,
    expected_sections: Sequence[str] = _EXPECTED_SECTIONS,
) -> list[dict[str, Any]]:
    """Return redacted findings for one or more ephemeral snapshots."""

    grouped: dict[str, dict[str, Any]] = {}
    for index, snapshot in enumerate(snapshots):
        observed_at = _observed_at(snapshot)
        for finding in _findings_for_snapshot(
            repo,
            snapshot,
            snapshot_index=index,
            observed_at=observed_at,
            expected_sections=expected_sections,
        ):
            fingerprint = finding["fingerprint"]
            existing = grouped.get(fingerprint)
            if existing is None:
                grouped[fingerprint] = finding
                continue
            existing["first_seen"] = min(existing["first_seen"], finding["first_seen"])
            existing["last_seen"] = max(existing["last_seen"], finding["last_seen"])
            existing["observed_at"] = existing["last_seen"]
            existing["evidence_refs"] = sorted({
                *existing.get("evidence_refs", []),
                *finding.get("evidence_refs", []),
            })
    return sorted(grouped.values(), key=lambda item: (item["severity"], item["rule_id"], item["finding_id"]))


def _findings_for_snapshot(
    repo: str,
    snapshot: Mapping[str, Any],
    *,
    snapshot_index: int,
    observed_at: str,
    expected_sections: Sequence[str],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    findings.extend(_coverage_findings(repo, snapshot, snapshot_index, observed_at, expected_sections))
    findings.extend(_process_findings(repo, snapshot, snapshot_index, observed_at))
    findings.extend(_ssh_agent_findings(repo, snapshot, snapshot_index, observed_at))
    findings.extend(_egress_findings(repo, snapshot, snapshot_index, observed_at))
    findings.extend(_listener_findings(repo, snapshot, snapshot_index, observed_at))
    findings.extend(_launch_agent_findings(repo, snapshot, snapshot_index, observed_at))
    findings.extend(_session_findings(repo, snapshot, snapshot_index, observed_at))
    return findings


def _coverage_findings(
    repo: str,
    snapshot: Mapping[str, Any],
    snapshot_index: int,
    observed_at: str,
    expected_sections: Sequence[str],
) -> list[dict[str, Any]]:
    present = set()
    coverage = snapshot.get("coverage")
    if isinstance(coverage, Mapping):
        for key, value in coverage.items():
            if value:
                present.add(str(key))
    present.update(key for key in expected_sections if key in snapshot)
    missing = sorted(set(expected_sections) - present)
    if not missing:
        return []
    return [
        _finding(
            repo=repo,
            rule_id="agent_runtime.coverage_gap",
            severity="low",
            fingerprint_parts=["coverage_gap", *missing],
            observed_at=observed_at,
            evidence_refs=[f"snapshot:{snapshot_index}:coverage"],
            message="Agent runtime security monitor coverage is incomplete.",
            remediation="Enable the missing monitor collectors or mark them explicitly unsupported for this host.",
            finding_class="coverage_gap",
            coverage_status="missing",
            attrs={"missing_sections": missing},
        )
    ]


def _process_findings(
    repo: str,
    snapshot: Mapping[str, Any],
    snapshot_index: int,
    observed_at: str,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for process_index, process in enumerate(_records(snapshot.get("processes"))):
        process_ref = _process_ref(process, process_index)
        env_names = _env_names(process)
        token_env_names = sorted(
            name for name in env_names if _is_token_name(name)
        )
        argv = _argv(process)
        token_arg_keys = _secret_arg_keys(argv)
        token_value_seen = _argv_has_token_value(argv)
        dsn_schemes = _dsn_schemes(argv)
        evidence_ref = f"snapshot:{snapshot_index}:process:{process_ref}"

        if token_env_names or token_arg_keys or token_value_seen:
            attrs: dict[str, Any] = {
                "affected_processes": [process_ref],
                "env_names": token_env_names,
                "env_classes": _env_classes(token_env_names),
                "arg_keys": token_arg_keys,
            }
            if token_value_seen and not token_arg_keys:
                attrs["arg_keys"] = ["token-shaped-value"]
            findings.append(
                _finding(
                    repo=repo,
                    rule_id="agent_runtime.secret_env_or_argv",
                    severity="high",
                    fingerprint_parts=[
                        "secret_env_or_argv",
                        process_ref,
                        *token_env_names,
                        *token_arg_keys,
                    ],
                    observed_at=observed_at,
                    evidence_refs=[evidence_ref],
                    message="Agent process exposes secret-shaped environment or argv material.",
                    remediation="Move credentials to scoped grants or short-lived secret files and restart the agent without inherited secret argv/env.",
                    finding_class="secret_exposure",
                    coverage_status="covered",
                    attrs=attrs,
                )
            )

        if "SSH_AUTH_SOCK" in env_names:
            findings.append(
                _finding(
                    repo=repo,
                    rule_id="agent_runtime.inherited_ssh_auth_sock",
                    severity="medium",
                    fingerprint_parts=["ssh_auth_sock", process_ref],
                    observed_at=observed_at,
                    evidence_refs=[evidence_ref],
                    message="Agent process inherited SSH_AUTH_SOCK.",
                    remediation="Start the agent with an explicit SSH policy, or clear SSH_AUTH_SOCK unless the session needs agent-backed Git/SSH.",
                    finding_class="ssh_agent_exposure",
                    coverage_status="covered",
                    attrs={
                        "affected_processes": [process_ref],
                        "env_names": ["SSH_AUTH_SOCK"],
                        "env_classes": ["ssh_agent_socket"],
                    },
                )
            )

        if dsn_schemes:
            findings.append(
                _finding(
                    repo=repo,
                    rule_id="agent_runtime.dsn_in_args",
                    severity="high",
                    fingerprint_parts=["dsn_in_args", process_ref, *dsn_schemes],
                    observed_at=observed_at,
                    evidence_refs=[evidence_ref],
                    message="Process argv contains a DSN-like value.",
                    remediation="Pass DSNs through scoped config files or environment grants that redact values before graph persistence.",
                    finding_class="dsn_exposure",
                    coverage_status="covered",
                    attrs={
                        "affected_processes": [process_ref],
                        "dsn_schemes": dsn_schemes,
                    },
                )
            )

        sensitive_classes = sorted({
            _sensitive_path_class(path)
            for path in _file_reads(process)
            if _sensitive_path_class(path)
        })
        if sensitive_classes:
            findings.append(
                _finding(
                    repo=repo,
                    rule_id="agent_runtime.sensitive_file_read",
                    severity="medium",
                    fingerprint_parts=[
                        "sensitive_file_read",
                        process_ref,
                        *sensitive_classes,
                    ],
                    observed_at=observed_at,
                    evidence_refs=[f"{evidence_ref}:file_reads"],
                    message="Agent process read sensitive local file classes.",
                    remediation="Confirm the read was required; move credentials into scoped runtime grants and keep transcript/tool output redaction enabled.",
                    finding_class="sensitive_file_access",
                    coverage_status="covered",
                    attrs={
                        "affected_processes": [process_ref],
                        "sensitive_path_classes": sensitive_classes,
                    },
                )
            )

        docker_env_names = _docker_env_names(argv)
        risky_docker_env_names = sorted(name for name in docker_env_names if _is_token_name(name))
        if risky_docker_env_names:
            findings.append(
                _finding(
                    repo=repo,
                    rule_id="agent_runtime.docker_secret_env",
                    severity="high",
                    fingerprint_parts=[
                        "docker_secret_env",
                        process_ref,
                        *risky_docker_env_names,
                    ],
                    observed_at=observed_at,
                    evidence_refs=[f"{evidence_ref}:docker_env"],
                    message="Docker invocation forwards secret-shaped environment variables.",
                    remediation="Use Docker secrets, a one-shot scoped env file, or an approved egress grant instead of passing agent credentials through docker argv.",
                    finding_class="container_secret_exposure",
                    coverage_status="covered",
                    attrs={
                        "affected_processes": [process_ref],
                        "env_names": risky_docker_env_names,
                        "env_classes": _env_classes(risky_docker_env_names),
                    },
                )
            )
    return findings


def _ssh_agent_findings(
    repo: str,
    snapshot: Mapping[str, Any],
    snapshot_index: int,
    observed_at: str,
) -> list[dict[str, Any]]:
    ssh_agent = snapshot.get("ssh_agent")
    if not isinstance(ssh_agent, Mapping):
        return []
    findings: list[dict[str, Any]] = []
    for key_index, key in enumerate(_records(ssh_agent.get("keys"))):
        if not _is_high_risk_ssh_key(key):
            continue
        key_ref = _key_ref(key, key_index)
        findings.append(
            _finding(
                repo=repo,
                rule_id="agent_runtime.high_risk_ssh_key",
                severity="medium",
                fingerprint_parts=["high_risk_ssh_key", key_ref],
                observed_at=observed_at,
                evidence_refs=[f"snapshot:{snapshot_index}:ssh_agent:key:{key_ref}"],
                message="SSH agent has a loaded key without confirmation or lifetime constraints.",
                remediation="Unload broad local keys or reload them with confirmation and a short lifetime before starting agent sessions.",
                finding_class="ssh_agent_exposure",
                coverage_status="covered",
                attrs={
                    "key_refs": [key_ref],
                    "key_path_classes": _key_path_classes(key),
                },
            )
        )
    return findings


def _egress_findings(
    repo: str,
    snapshot: Mapping[str, Any],
    snapshot_index: int,
    observed_at: str,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for event_index, event in enumerate(_records(snapshot.get("egress_events"))):
        method = str(event.get("method") or "").upper()
        decision = str(event.get("decision") or event.get("outcome") or "unknown").lower()
        bytes_out = _int_or_none(event.get("bytes_out") or event.get("request_bytes"))
        upload_method = method in {"POST", "PUT", "PATCH"}
        upload_sized = bytes_out is not None and bytes_out >= 1_000_000
        if not upload_method and not upload_sized:
            continue
        host = _host_from_event(event)
        findings.append(
            _finding(
                repo=repo,
                rule_id="agent_runtime.upload_like_egress",
                severity="high" if decision in {"unknown", "denied", "blocked"} else "medium",
                fingerprint_parts=[
                    "upload_like_egress",
                    host,
                    method,
                    str(event.get("process_ref") or event_index),
                ],
                observed_at=observed_at,
                evidence_refs=[f"snapshot:{snapshot_index}:egress:{event_index}"],
                message="Agent runtime produced upload-like egress.",
                remediation="Require an explicit egress grant with destination, purpose, payload class, and expiry before allowing upload-like traffic.",
                finding_class="egress_exposure",
                coverage_status="covered",
                attrs={
                    "affected_processes": [_stable_ref(event.get("process_ref"), f"egress-{event_index}")],
                    "bytes_out_class": _bytes_class(bytes_out),
                    "decision": decision,
                    "service": host,
                },
            )
        )
    return findings


def _listener_findings(
    repo: str,
    snapshot: Mapping[str, Any],
    snapshot_index: int,
    observed_at: str,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for listener_index, listener in enumerate(_records(snapshot.get("listeners"))):
        raw_bind = listener.get("bind_address") or listener.get("host") or ""
        bind = _normalized_bind_address(raw_bind)
        service = _safe_label(listener.get("service") or listener.get("name") or "unknown")
        port = _int_or_none(listener.get("port")) or _port_from_bind_address(raw_bind)
        owner = _safe_label(listener.get("owner"))
        evidence_ref = f"snapshot:{snapshot_index}:listener:{listener_index}"
        service_attrs = {
            "bind_address": bind,
            "port": port,
            "service": service,
            "owner": owner,
        }

        if bind in _PUBLIC_BINDS and not bool(listener.get("approved")):
            findings.append(
                _finding(
                    repo=repo,
                    rule_id="agent_runtime.public_listener",
                    severity="high",
                    fingerprint_parts=["public_listener", service, bind, str(port)],
                    observed_at=observed_at,
                    evidence_refs=[evidence_ref],
                    message="Agent-affecting service listens on all interfaces without approval.",
                    remediation="Bind the service to loopback or add an explicit approved service record with owner and network exposure rationale.",
                    finding_class="service_exposure",
                    coverage_status="covered",
                    attrs=service_attrs,
                )
            )

        if _unknown_owner(owner):
            findings.append(
                _finding(
                    repo=repo,
                    rule_id="agent_runtime.unknown_service_owner",
                    severity="high",
                    fingerprint_parts=["unknown_owner", "listener", service, bind, str(port)],
                    observed_at=observed_at,
                    evidence_refs=[evidence_ref],
                    message="Agent-affecting service owner is unknown.",
                    remediation="Set an explicit owner before treating the service posture as ready.",
                    finding_class="service_inventory",
                    coverage_status="covered",
                    attrs=service_attrs,
                )
            )

        if bind in _LOOPBACK_BINDS and bool(listener.get("approved")):
            findings.append(
                _finding(
                    repo=repo,
                    rule_id="agent_runtime.approved_loopback_service",
                    severity="low",
                    fingerprint_parts=["approved_loopback_service", service, bind, str(port)],
                    observed_at=observed_at,
                    evidence_refs=[evidence_ref],
                    message="Approved loopback service is visible for readiness review.",
                    remediation="Keep owner metadata and redacted runtime configuration refs current.",
                    finding_class="service_inventory",
                    coverage_status="covered",
                    attrs=service_attrs,
                )
            )
    return findings


def _launch_agent_findings(
    repo: str,
    snapshot: Mapping[str, Any],
    snapshot_index: int,
    observed_at: str,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for agent_index, launch_agent in enumerate(_records(snapshot.get("launch_agents"))):
        argv = _argv(launch_agent, key="program_arguments")
        working_directory = str(launch_agent.get("working_directory") or "")
        label = _safe_label(launch_agent.get("label") or f"launch-agent-{agent_index}")
        age_days = _int_or_none(launch_agent.get("age_days"))
        owner = _safe_label(launch_agent.get("owner"))
        stale = bool(launch_agent.get("stale")) or (
            age_days is not None and age_days >= 14
        )
        worktree_class = _worktree_class(working_directory, argv)
        evidence_ref = f"snapshot:{snapshot_index}:launch_agent:{_short_hash(label)}"

        if stale and worktree_class in {"codex_worktree", "claude_worktree"}:
            findings.append(
                _finding(
                    repo=repo,
                    rule_id="agent_runtime.stale_launch_agent",
                    severity="high",
                    fingerprint_parts=["stale_launch_agent", label, worktree_class],
                    observed_at=observed_at,
                    evidence_refs=[evidence_ref],
                    message="LaunchAgent points at a stale agent worktree.",
                    remediation="Unload the LaunchAgent or repoint it to the approved okg-workspace deployment checkout.",
                    finding_class="service_exposure",
                    coverage_status="covered",
                    attrs={
                        "age_days": age_days,
                        "service": label,
                        "owner": owner,
                    },
                )
            )

        dsn_schemes = _dsn_schemes(argv)
        if dsn_schemes:
            findings.append(
                _finding(
                    repo=repo,
                    rule_id="agent_runtime.launch_agent_dsn_in_args",
                    severity="high",
                    fingerprint_parts=["launch_agent_dsn", label, *dsn_schemes],
                    observed_at=observed_at,
                    evidence_refs=[f"{evidence_ref}:program_arguments"],
                    message="LaunchAgent ProgramArguments contain a DSN-like value.",
                    remediation="Move DSNs into redacted config refs and keep ProgramArguments free of secret-bearing connection strings.",
                    finding_class="dsn_exposure",
                    coverage_status="covered",
                    attrs={
                        "service": label,
                        "dsn_schemes": dsn_schemes,
                        "owner": owner,
                    },
                )
            )
        secret_arg_keys = _secret_arg_keys(argv)
        if secret_arg_keys or _argv_has_token_value(argv):
            findings.append(
                _finding(
                    repo=repo,
                    rule_id="agent_runtime.launch_agent_secret_in_args",
                    severity="high",
                    fingerprint_parts=[
                        "launch_agent_secret_arg",
                        label,
                        *(secret_arg_keys or ["token-shaped-value"]),
                    ],
                    observed_at=observed_at,
                    evidence_refs=[f"{evidence_ref}:program_arguments"],
                    message="LaunchAgent ProgramArguments contain secret-like values or argument names.",
                    remediation="Move secret-bearing ProgramArguments into redacted config refs or scoped environment injection.",
                    finding_class="secret_exposure",
                    coverage_status="covered",
                    attrs={
                        "service": label,
                        "arg_keys": secret_arg_keys or ["token-shaped-value"],
                        "owner": owner,
                    },
                )
            )
        if _unknown_owner(owner):
            findings.append(
                _finding(
                    repo=repo,
                    rule_id="agent_runtime.unknown_service_owner",
                    severity="high",
                    fingerprint_parts=["unknown_owner", "launch_agent", label],
                    observed_at=observed_at,
                    evidence_refs=[evidence_ref],
                    message="Agent-affecting LaunchAgent owner is unknown.",
                    remediation="Set an explicit owner before treating the service posture as ready.",
                    finding_class="service_inventory",
                    coverage_status="covered",
                    attrs={
                        "age_days": age_days,
                        "service": label,
                        "owner": owner,
                    },
                )
            )
    return findings


def _session_findings(
    repo: str,
    snapshot: Mapping[str, Any],
    snapshot_index: int,
    observed_at: str,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for session_index, session in enumerate(_records(snapshot.get("sessions"))):
        mode = _normalized_mode(session.get("permission_mode") or session.get("mode"))
        if mode not in _BYPASS_MODES:
            continue
        agent = _safe_label(session.get("agent") or session.get("runtime") or "agent")
        session_ref = _stable_ref(session.get("session_id") or session.get("id"), f"session-{session_index}")
        findings.append(
            _finding(
                repo=repo,
                rule_id="agent_runtime.bypass_permissions",
                severity="high",
                fingerprint_parts=["bypass_permissions", agent, session_ref, mode],
                observed_at=observed_at,
                evidence_refs=[f"snapshot:{snapshot_index}:session:{session_ref}"],
                message="Agent session ran with bypass-style permissions.",
                remediation="Use deny-by-default prompts or scoped approvals for filesystem, network, Docker, and shell actions; reserve bypass mode for short break-glass windows.",
                finding_class="permission_bypass",
                coverage_status="covered",
                attrs={
                    "agent": agent,
                    "permission_mode": mode,
                },
            )
        )
    return findings


def _finding(
    *,
    repo: str,
    rule_id: str,
    severity: str,
    fingerprint_parts: Sequence[str],
    observed_at: str,
    evidence_refs: Sequence[str],
    message: str,
    remediation: str,
    finding_class: str,
    coverage_status: str,
    attrs: Mapping[str, Any],
) -> dict[str, Any]:
    fingerprint = _fingerprint([repo, rule_id, *fingerprint_parts])
    finding_id = f"agent-runtime-finding:{fingerprint}"
    base: dict[str, Any] = {
        "finding_id": finding_id,
        "repo": repo,
        "tool": TOOL_NAME,
        "rule_id": rule_id,
        "severity": severity,
        "confidence": "high",
        "path": "runtime://local-agent",
        "start_line": 0,
        "end_line": 0,
        "status": "open",
        "fingerprint": fingerprint,
        "observed_at": observed_at,
        "first_seen": observed_at,
        "last_seen": observed_at,
        "message": message,
        "text": f"{rule_id}: {message}",
        "category": "agent_runtime_security",
        "finding_class": finding_class,
        "coverage_status": coverage_status,
        "evidence_refs": sorted(str(ref) for ref in evidence_refs if ref),
        "remediation": remediation,
    }
    base.update({key: value for key, value in attrs.items() if value not in (None, [], {})})
    return _allowlisted_attrs(base)


def _allowlisted_attrs(attrs: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: _sanitize_attr_value(value)
        for key, value in attrs.items()
        if key in AGENT_RUNTIME_SECURITY_ALLOWED_ATTRS and value is not None
    }


def _sanitize_attr_value(value: Any) -> Any:
    if isinstance(value, str):
        return _safe_text(value)
    if isinstance(value, list):
        return [_sanitize_attr_value(item) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_attr_value(item) for item in value]
    if isinstance(value, dict):
        return {
            _safe_text(str(key)): _sanitize_attr_value(item)
            for key, item in value.items()
        }
    return value


def _record_set_for_findings(
    repo: str,
    findings: Sequence[Mapping[str, Any]],
    *,
    revision: Mapping[str, Any],
) -> RecordSet:
    records: dict[str, RecordEmission] = {}
    for finding in findings:
        finding_id = str(finding["finding_id"])
        records[finding_id] = RecordEmission(
            primary=RecordNode(
                node_id=finding_id,
                subtype="security_finding",
                source_record_id={"repo": repo, "finding_id": finding_id},
                source_revision=dict(revision),
            )
        )
    return RecordSet(records=records)


def _observed_at(snapshot: Mapping[str, Any]) -> str:
    raw = snapshot.get("observed_at") or snapshot.get("captured_at")
    if isinstance(raw, str) and raw:
        return raw
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _records(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, Mapping)]
    return []


def _env_names(process: Mapping[str, Any]) -> list[str]:
    names: set[str] = set()
    for key in ("env_names", "env_keys"):
        raw = process.get(key)
        if isinstance(raw, list):
            names.update(_safe_env_name(item) for item in raw)
    env = process.get("env")
    if isinstance(env, Mapping):
        names.update(_safe_env_name(key) for key in env)
    return sorted(name for name in names if name)


def _env_classes(names: Sequence[str]) -> list[str]:
    classes = set()
    for name in names:
        upper = name.upper()
        if "OPENAI" in upper:
            classes.add("openai_token")
        elif "ANTHROPIC" in upper:
            classes.add("anthropic_token")
        elif "GITHUB" in upper or upper in {"GH_TOKEN", "GITHUB_TOKEN"}:
            classes.add("github_token")
        elif "PASSWORD" in upper or "PASSWD" in upper:
            classes.add("password")
        elif "SECRET" in upper:
            classes.add("secret")
        elif "KEY" in upper:
            classes.add("api_key")
        elif "TOKEN" in upper:
            classes.add("token")
        else:
            classes.add("credential")
    return sorted(classes)


def _argv(record: Mapping[str, Any], *, key: str = "argv") -> list[str]:
    raw = record.get(key)
    if raw is None and key == "argv":
        raw = record.get("command")
    if isinstance(raw, str):
        return raw.split()
    if isinstance(raw, list):
        return [str(item) for item in raw]
    return []


def _secret_arg_keys(argv: Sequence[str]) -> list[str]:
    keys: set[str] = set()
    for index, token in enumerate(argv):
        key, _, value = token.partition("=")
        normalized_key = key.lstrip("-")
        if _is_token_name(normalized_key):
            keys.add(_safe_arg_key(normalized_key))
        if _is_token_name(token):
            keys.add(_safe_arg_key(token))
        if token.startswith("--") and index + 1 < len(argv) and _is_token_name(normalized_key):
            keys.add(_safe_arg_key(normalized_key))
        if value and _TOKEN_VALUE_RE.search(value):
            keys.add(_safe_arg_key(normalized_key or "token-shaped-value"))
    return sorted(key for key in keys if key)


def _argv_has_token_value(argv: Sequence[str]) -> bool:
    return any(_TOKEN_VALUE_RE.search(token) for token in argv)


def _dsn_schemes(argv: Sequence[str]) -> list[str]:
    schemes: set[str] = set()
    for token in argv:
        for match in _DSN_RE.finditer(token):
            scheme = match.group(1).lower()
            if scheme in {"http", "https", "file"}:
                continue
            schemes.add(scheme)
    return sorted(schemes)


def _docker_env_names(argv: Sequence[str]) -> list[str]:
    if not argv or "docker" not in Path(argv[0]).name.lower():
        return []
    names: set[str] = set()
    pending_env = False
    for token in argv[1:]:
        if pending_env:
            name = token.split("=", 1)[0]
            names.add(_safe_env_name(name))
            pending_env = False
            continue
        if token in {"-e", "--env", "--env-file"}:
            pending_env = token != "--env-file"
            continue
        if token.startswith("--env="):
            names.add(_safe_env_name(token.split("=", 1)[1].split("=", 1)[0]))
        elif token.startswith("-e") and len(token) > 2:
            names.add(_safe_env_name(token[2:].split("=", 1)[0]))
    return sorted(name for name in names if name)


def _file_reads(process: Mapping[str, Any]) -> list[str]:
    raw = process.get("file_reads") or process.get("read_paths")
    if isinstance(raw, list):
        return [str(item) for item in raw]
    return []


def _sensitive_path_class(path: str) -> str | None:
    lowered = path.lower()
    if "/.ssh/" in lowered:
        return "ssh_material"
    if "/.aws/credentials" in lowered:
        return "aws_credentials"
    if "/.docker/config.json" in lowered:
        return "docker_credentials"
    if lowered.endswith("/.netrc") or "/.netrc" in lowered:
        return "netrc"
    if lowered.endswith("/.npmrc") or "/.npmrc" in lowered:
        return "npm_credentials"
    if lowered.endswith("/.pypirc") or "/.pypirc" in lowered:
        return "python_package_credentials"
    if "/.config/gcloud/" in lowered:
        return "gcloud_credentials"
    if "/.env" in lowered or lowered.endswith(".env"):
        return "env_file"
    if any(marker in lowered for marker in _SENSITIVE_PATH_MARKERS):
        return "sensitive_file"
    return None


def _is_high_risk_ssh_key(key: Mapping[str, Any]) -> bool:
    confirm_required = bool(key.get("confirm_required") or key.get("requires_confirmation"))
    lifetime = _int_or_none(key.get("lifetime_seconds") or key.get("ttl_seconds"))
    if confirm_required and lifetime is not None and lifetime <= 3600:
        return False
    path_classes = _key_path_classes(key)
    return bool(path_classes) or not confirm_required or lifetime is None


def _key_ref(key: Mapping[str, Any], index: int) -> str:
    raw = (
        key.get("fingerprint")
        or key.get("sha256")
        or key.get("comment")
        or key.get("path")
        or f"key-{index}"
    )
    return f"ssh-key:{_short_hash(str(raw))}"


def _key_path_classes(key: Mapping[str, Any]) -> list[str]:
    raw = " ".join(
        str(key.get(field) or "")
        for field in ("path", "comment", "identity")
    ).lower()
    classes = set()
    for name in _HIGH_RISK_KEY_NAMES:
        if f"/{name}" in raw or raw.endswith(name):
            classes.add("default_private_key")
    if "/.ssh/" in raw:
        classes.add("ssh_material")
    return sorted(classes)


def _host_from_event(event: Mapping[str, Any]) -> str:
    host = event.get("host")
    if isinstance(host, str) and host:
        return _safe_label(host)
    url = event.get("url") or event.get("destination")
    if isinstance(url, str):
        parsed = urlparse(url)
        if parsed.hostname:
            return _safe_label(parsed.hostname)
    return "unknown-destination"


def _bytes_class(bytes_out: int | None) -> str:
    if bytes_out is None:
        return "unknown"
    if bytes_out >= 100_000_000:
        return "very_large"
    if bytes_out >= 1_000_000:
        return "large"
    if bytes_out > 0:
        return "small"
    return "none"


def _worktree_class(working_directory: str, argv: Sequence[str]) -> str:
    text = " ".join([working_directory, *argv]).lower()
    if "/.codex/" in text or "/codex-" in text:
        return "codex_worktree"
    if "/.claude/" in text or "/claude-" in text:
        return "claude_worktree"
    return "other"


def _normalized_mode(value: Any) -> str:
    return str(value or "").strip().lower().replace("_", "-")


def _normalized_bind_address(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text == "*":
        return "0.0.0.0"
    if text.startswith("[") and "]" in text:
        return text[1:text.index("]")]
    if text.count(":") == 1:
        host, _, port = text.rpartition(":")
        if port.isdigit():
            return "0.0.0.0" if host == "*" else host
    return text.strip("[]")


def _port_from_bind_address(value: Any) -> int | None:
    text = str(value or "").strip()
    if text.startswith("[") and "]:" in text:
        return _int_or_none(text.rsplit(":", 1)[-1])
    if text.count(":") == 1:
        _, _, port = text.rpartition(":")
        return _int_or_none(port)
    return None


def _process_ref(process: Mapping[str, Any], index: int) -> str:
    raw = process.get("process_ref") or process.get("pid") or process.get("id") or f"process-{index}"
    return _stable_ref(raw, f"process-{index}")


def _stable_ref(raw: Any, fallback: str) -> str:
    if raw is None or raw == "":
        return fallback
    text = str(raw)
    if text.isdigit() or re.fullmatch(r"[A-Za-z0-9_.:-]{1,64}", text):
        return text
    return _short_hash(text)


def _is_token_name(name: str) -> bool:
    if not name:
        return False
    if _BENIGN_TOKEN_NAME_RE.match(name):
        return False
    return bool(_TOKEN_NAME_RE.search(name))


def _safe_env_name(value: Any) -> str:
    text = str(value or "").strip()
    text = text.split("=", 1)[0]
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,80}", text):
        return text
    return f"env:{_short_hash(text)}" if text else ""


def _safe_arg_key(value: Any) -> str:
    text = str(value or "").strip().lstrip("-")
    text = text.split("=", 1)[0]
    text = text.replace("_", "-").lower()
    if re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,80}", text):
        return text
    return f"arg:{_short_hash(text)}" if text else ""


def _safe_label(value: Any) -> str:
    if value is None:
        return "unknown"
    text = str(value).strip()
    if not text:
        return "unknown"
    text = _safe_text(text)
    if "://" in text or "/" in text or "\\" in text or len(text) > 120:
        return f"label:{_short_hash(text)}"
    return text


def _unknown_owner(owner: Any) -> bool:
    return str(owner or "").strip().lower() in {"", "unknown", "none"}


def _safe_text(text: str) -> str:
    text = _DSN_RE.sub(lambda m: f"{m.group(1).lower()}://[REDACTED]", text)
    text = _TOKEN_VALUE_RE.sub("[REDACTED]", text)
    return text


def _fingerprint(parts: Sequence[str]) -> str:
    return hashlib.sha256(
        json.dumps([str(part) for part in parts], sort_keys=True).encode("utf-8")
    ).hexdigest()[:24]


def _short_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
