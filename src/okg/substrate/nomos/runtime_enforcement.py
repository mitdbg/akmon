"""Runtime enforcement boundary for Nomos decisions.

Nomos owns the policy decision, obligation, and privacy-safe trace layer. This
module keeps that boundary explicit: callers reduce a sensitive runtime effect to
one canonical request, Nomos evaluates it, and the returned outcome records
whether the effect may proceed before the caller performs it.
"""
from __future__ import annotations

import functools
import hashlib
import json
import logging
import math
import os
import re

import yaml
from collections.abc import Mapping as _AbcMapping, Sequence as _AbcSequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Iterable, Mapping, NoReturn, Sequence, TypeGuard

from okg.substrate.nomos.core import (
    CompiledPolicyBundle,
    DecisionInput,
    PolicyDecision,
    evaluate,
    looks_secret_key,
)
from okg.substrate.nomos.vocabulary import (
    ACTIONS,
    DATA_CLASSIFICATIONS,
    OBLIGATION_TYPES,
    SURFACE_DECISION_MAPPINGS,
    RESOURCE_KINDS,
    PRINCIPAL_KINDS,
    NomosVocabularyError,
    SurfaceDecisionMapping,
    surface_mapping_for,
)


REQUIRED_NOMOS_RUNTIME_BOUNDARIES: tuple[str, ...] = (
    "graph.read.public_demo",
    "graph.read.local_operator",
    "graph.read.mcp",
    "graph.read.cli_search",
    "graph.read.sql_admin",
    "graph.read.export_like",
    "graph.export",
    "mcp.tool.call",
    "mcp.record",
    "source.ingest",
    "source.emit",
    "source.live_call",
    "secret.use",
    "generation.publish",
    "policy.activate",
    "config.activate",
    "declassify",
    "redact",
    "quarantine.release",
    "admin.override",
)
_PRODUCTION_BRIDGE_INVENTORY: tuple[dict[str, str], ...] = (
    {
        "surface": "graph.read.public_demo",
        "status": "unsupported_no_core_surface",
        "bridge": "deployment-specific public view scripts",
        "verification": "checked_inventory",
    },
    {
        "surface": "graph.read.local_operator",
        "status": "wired",
        "bridge": "okg.substrate.operator_read.authorize_local_operator_read",
        "verification": "tests/substrate/viz/test_operator_record_reads.py",
    },
    {
        "surface": "graph.read.mcp",
        "status": "wired",
        "bridge": "okg.substrate.mcp.server._authorize_nomos_read_boundary",
        "verification": "tests/substrate/mcp/test_nomos_runtime_boundary.py",
    },
    {
        "surface": "graph.read.cli_search",
        "status": "wired",
        "bridge": "okg.substrate.cli._read_side._authorize_nomos_cli_read",
        "verification": "tests/substrate/test_dsn_authority_surfaces.py",
    },
    {
        "surface": "graph.read.sql_admin",
        "status": "wired",
        "bridge": "okg.substrate.mcp.server._authorize_nomos_read_boundary",
        "verification": "tests/substrate/mcp/test_nomos_runtime_boundary.py",
    },
    {
        "surface": "graph.read.export_like",
        "status": "wired",
        "bridge": "okg.substrate.exchange.outbox export and cli.poros proof",
        "verification": "tests/substrate/exchange/test_poros_outbox_nomos.py",
    },
    {
        "surface": "graph.export",
        "status": "wired",
        "bridge": "okg.substrate.exchange.outbox export and sender_delivery",
        "verification": "tests/substrate/exchange/test_poros_outbox_nomos.py",
    },
    {
        "surface": "mcp.tool.call",
        "status": "wired",
        "bridge": "okg.substrate.mcp.server._authorize_nomos_read_boundary",
        "verification": "tests/substrate/mcp/test_nomos_runtime_boundary.py",
    },
    {
        "surface": "mcp.record",
        "status": "wired",
        "bridge": "okg.substrate.mcp.server._authorize_record_with_nomos",
        "verification": "tests/substrate/mcp/test_nomos_runtime_boundary.py",
    },
    {
        "surface": "source.ingest",
        "status": "wired",
        "bridge": "okg.substrate.ingest.runner._authorize_nomos_source_boundary",
        "verification": "tests/substrate/test_source_runner.py",
    },
    {
        "surface": "source.emit",
        "status": "wired",
        "bridge": "okg.substrate.ingest.runner._authorize_nomos_source_boundary",
        "verification": "tests/substrate/test_source_runner.py",
    },
    {
        "surface": "source.live_call",
        "status": "wired",
        "bridge": "okg.substrate.ingest.runner._authorize_nomos_source_boundary",
        "verification": "tests/substrate/test_source_runner.py",
    },
    {
        "surface": "secret.use",
        "status": "wired",
        "bridge": "okg.substrate.exchange.outbox.authorize_poros_signing_key_use",
        "verification": "tests/substrate/exchange/test_poros_outbox_nomos.py",
    },
    {
        "surface": "generation.publish",
        "status": "wired",
        "bridge": "okg.substrate.publisher.publisher.Publisher._nomos_generation_publish_blocker",
        "verification": "tests/substrate/test_publisher.py",
    },
    {
        "surface": "policy.activate",
        "status": "wired",
        "bridge": "okg.substrate.nomos.store.activate_policy_bundle",
        "verification": "tests/substrate/nomos/test_nomos_active_store.py",
    },
    {
        "surface": "config.activate",
        "status": "wired",
        "bridge": "okg.substrate.nomos.store.activate_config_set",
        "verification": "tests/substrate/nomos/test_nomos_active_store.py",
    },
    {
        "surface": "declassify",
        "status": "wired_obligation_effect",
        "bridge": "okg.substrate.nomos.runtime_enforcement._obligation_result",
        "verification": "tests/substrate/nomos/test_nomos_runtime_enforcement.py",
    },
    {
        "surface": "redact",
        "status": "wired_obligation_effect",
        "bridge": "okg.substrate.nomos.runtime_enforcement._obligation_result",
        "verification": "tests/substrate/nomos/test_nomos_runtime_enforcement.py",
    },
    {
        "surface": "quarantine.release",
        "status": "wired",
        "bridge": "okg.substrate.runtime.admission_workflows._authorize_nomos_admission_decision",
        "verification": "tests/substrate/runtime/test_admission_workflows.py",
    },
    {
        "surface": "admin.override",
        "status": "wired",
        "bridge": "okg.substrate.runtime.admission_workflows._authorize_nomos_admission_decision",
        "verification": "tests/substrate/runtime/test_admission_workflows.py",
    },
)

ALLOW_EFFECTS: frozenset[str] = frozenset({"allow", "redact", "declassify"})
TERMINAL_DENY_EFFECTS: frozenset[str] = frozenset({
    "deny",
    "quarantine",
    "review",
})

_EFFECT_REQUIRED_OBLIGATION: Mapping[str, str] = {
    "declassify": "declassification",
    "quarantine": "quarantine",
    "redact": "redaction",
    "review": "review",
}
_ACTION_REQUIRED_OBLIGATIONS: Mapping[str, tuple[str, ...]] = {
    "agent.bypass": ("break_glass_review",),
}
AGENT_RUNTIME_LOCAL_EFFECT_SURFACES: tuple[str, ...] = (
    "agent.launch",
    "agent.bypass",
    "secret.use",
    "ssh.use",
    "docker.run",
    "network.egress",
    "file.read",
    "file.write",
    "transcript.persist",
    "service.persist",
)
_SECRET_REF_PREFIXES = ("secret://", "vault://", "keyring://", "aws-sm://")
_TRACE_PAYLOAD_KEYS = frozenset({
    "body",
    "okg_producer_text",
    "content",
    "file_content",
    "file_contents",
    "headers",
    "metadata",
    "params",
    "payload",
    "private_body",
    "raw",
    "raw_body",
    "request",
    "response",
    "source_metadata",
    "text",
})
_AUTHORIZATION_KEYS = frozenset({
    "authorization",
    "cookie",
    "credentials",
    "jwt",
    "raw_token",
    "session",
    "token",
})
_SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_DATE_OF_BIRTH_RE = re.compile(
    r"\b(?:19|20)\d{2}[-/](?:0[1-9]|1[0-2])[-/](?:0[1-9]|[12]\d|3[01])\b"
)
_LOCAL_PATH_RE = re.compile(r"^(?:/Users/|/home/|/private/|/var/folders/|[A-Za-z]:\\)")


@dataclass(frozen=True)
class NomosRuntimeRequest:
    """One canonical runtime effect request evaluated by Nomos."""

    surface: str
    subject: str
    resource: Mapping[str, Any] = field(default_factory=dict)
    classification: str | None = None
    context: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AgentRuntimeDecisionRequest:
    """Full Nomos Decision API wrapper for agent-local runtime effects."""

    subject_binding_id: str
    principal_kind: str
    action: str
    resource_kind: str
    resource_id: str
    data_classification: str
    deployment_posture: Mapping[str, Any]
    policy_generation_pin: str
    enforcement_point: str
    agent_run_id: str
    profile_id: str = ""
    context: Mapping[str, Any] = field(default_factory=dict)
    config_generation_pin: str | None = None
    config_affects_decision: bool = False
    break_glass_grant: Mapping[str, Any] | None = None
    profile_allows_unsafe_without_break_glass: bool = False


@dataclass(frozen=True)
class NomosPrimitiveCheck:
    """Result from an existing hardening primitive that Nomos composes with."""

    name: str
    allowed: bool
    reason_code: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_trace(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "allowed": self.allowed,
            "reason_code": self.reason_code or (
                "nomos.runtime.primitive_allowed"
                if self.allowed else "nomos.runtime.primitive_denied"
            ),
            "metadata": sanitize_trace_value(self.metadata),
        }


@dataclass(frozen=True)
class NomosRuntimeOutcome:
    """Decision plus enforcement outcome for a sensitive runtime boundary."""

    decision: PolicyDecision
    allowed: bool
    final_effect: str
    required_obligations: tuple[str, ...]
    enforced_obligations: tuple[str, ...]
    missing_obligations: tuple[str, ...]
    primitive_checks: tuple[NomosPrimitiveCheck, ...]
    trace: Mapping[str, Any]
    payload: Any | None = None
    # True only when audit mode let through an effect the policy did not
    # allow. ``final_effect`` and ``decision`` keep the policy's verdict (the
    # audit record); ``allowed`` says the call went ahead anyway.
    audit_log_and_allow: bool = False

    @property
    def call_effect(self) -> str:
        """The effect applied to THIS call, as an operator should read it.

        ``final_effect`` is the policy's verdict and stays ``deny`` when audit
        mode lets the call through, so reporting it next to returned rows reads
        as a refusal that did not happen (issue #2279). This names what
        happened: ``audit`` when audit mode allowed a call the policy would
        refuse, otherwise the policy's own effect.
        """
        if self.allowed and self.audit_log_and_allow:
            return "audit"
        return self.final_effect


def nomos_runtime_boundary_inventory() -> tuple[dict[str, str], ...]:
    """Return the checked sensitive boundary inventory for operator reports."""
    rows: list[dict[str, str]] = []
    for surface in REQUIRED_NOMOS_RUNTIME_BOUNDARIES:
        mapping = runtime_surface_mapping_for(surface)
        rows.append({
            "surface": surface,
            "action": mapping.action,
            "resource_kind": mapping.resource_kind,
            "principal_kind": mapping.principal_kind,
            "classification": mapping.classification,
        })
    return tuple(rows)


def nomos_runtime_production_bridge_inventory() -> tuple[dict[str, str], ...]:
    """Return concrete runtime bridges or explicit unsupported surfaces."""
    return tuple(dict(row) for row in _PRODUCTION_BRIDGE_INVENTORY)


def agent_runtime_local_effect_inventory() -> tuple[dict[str, str], ...]:
    """Return canonical Nomos mappings for agent-local runtime effects."""
    rows: list[dict[str, str]] = []
    for surface in AGENT_RUNTIME_LOCAL_EFFECT_SURFACES:
        mapping = runtime_surface_mapping_for(surface)
        rows.append({
            "surface": surface,
            "action": mapping.action,
            "resource_kind": mapping.resource_kind,
            "principal_kind": mapping.principal_kind,
            "classification": mapping.classification,
        })
    return tuple(rows)


#: Canonical tri-state Nomos runtime-enforcement modes. ``off`` skips the
#: boundary entirely (byte-identical to no nomos block); ``audit`` evaluates and
#: records the real decision but allows the effect (log-and-allow); ``enforce``
#: evaluates and blocks denied effects.
NOMOS_ENFORCEMENT_MODES: frozenset[str] = frozenset({"off", "audit", "enforce"})

_ENFORCE_MODE_TOKENS = frozenset({"1", "true", "yes", "on", "enforce"})
_OFF_MODE_TOKENS = frozenset({"0", "false", "no", "off", "disabled"})


class NomosEnforcementConfigError(ValueError):
    """A runtime-enforcement mode value is present but not recognized.

    Raised when the ``OKG_NOMOS_RUNTIME_ENFORCEMENT`` env token or a manifest's
    ``nomos.runtime_enforcement`` field carries a value that is not one of
    ``off``/``audit``/``enforce`` (or a boolean-like token). A misconfigured
    kill-switch or manifest must fail loud rather than silently normalizing to
    ``off`` -- that would be a fail-open on the enforcement authority surface.
    """


class NomosEnforcementResolutionError(RuntimeError):
    """A present deployment manifest could not be read/parsed for its mode.

    Raised when, after the deployment directory is resolved, the
    ``deployment.yaml`` is present but cannot be read or parsed (permission
    error, I/O error, decode error, YAML parse error). The enforcement mode is
    therefore unknown, so the boundary fails closed (deny) instead of
    defaulting to ``off``. Genuine absence -- no dir, no manifest file, or a
    parsed manifest with no ``nomos`` block -- is distinguished and resolves
    ``off`` without raising.
    """


def _enforcement_override() -> str | None:
    """Return the normalized ``OKG_NOMOS_RUNTIME_ENFORCEMENT`` override.

    ``None`` when the env var is unset. Raises
    :class:`NomosEnforcementConfigError` when the var is set to an
    unrecognized token -- an invalid kill-switch value is never silently
    ignored.
    """
    raw = os.getenv("OKG_NOMOS_RUNTIME_ENFORCEMENT")
    if raw is None:
        return None
    normalized = _normalize_enforcement_mode_value(raw)
    if normalized is None:
        raise NomosEnforcementConfigError(
            f"OKG_NOMOS_RUNTIME_ENFORCEMENT={raw!r} is not a recognized "
            f"enforcement token (expected one of "
            f"{sorted(NOMOS_ENFORCEMENT_MODES)} or a boolean-like value); "
            f"refusing to silently ignore the kill-switch"
        )
    return normalized


def _normalize_enforcement_mode_value(value: Any) -> str | None:
    """Map a raw manifest/env value to ``off``/``audit``/``enforce`` or None."""
    if isinstance(value, bool):
        return "enforce" if value else "off"
    if isinstance(value, str):
        token = value.strip().lower()
        if token == "audit":
            return "audit"
        if token in _ENFORCE_MODE_TOKENS:
            return "enforce"
        if token in _OFF_MODE_TOKENS:
            return "off"
    return None


def _raw_enforcement_mode_value(nomos: Mapping[str, Any]) -> Any | None:
    for key in ("runtime_enforcement", "runtime_enforcement_mode", "enforcement"):
        if key in nomos:
            return nomos[key]
    return None


# Per-process caches for the nomos enforcement mode. ``find_deployment_dir``
# does a multi-root filesystem walk and the manifest is re-parsed, yet this
# resolves on EVERY source-emit-boundary (~21x/commit, ~1.45s on a profiled
# critical path). The deployment dir is stable within a process; the mode is
# re-read only when ``deployment.yaml``'s mtime changes, so a re-provisioned
# deployment is still picked up (no silent staleness).
# Keyed by (OKG_DEPLOYMENTS_DIR, deployment_name) -> resolved dir /
# ((mtime, ctime, size), mode).
# The deployments ROOT is part of the key: find_deployment_dir resolves a name
# against the active root (chiefly OKG_DEPLOYMENTS_DIR), so a name-only key would
# serve a stale dir whenever the root changes within one process -- e.g. two
# deployments named the same under different roots, or two tests using different
# tmp dirs. The mode memo is additionally validated by deployment.yaml's mtime.
_NOMOS_DEPLOYMENT_DIR_CACHE: dict = {}
_NOMOS_MODE_CACHE: dict = {}


def _nomos_enforcement_cache_key(deployment_name: str) -> tuple[str | None, str]:
    """Cache key for the enforcement-mode memos: (active deployments root, name)."""
    return (os.environ.get("OKG_DEPLOYMENTS_DIR"), deployment_name)


def _clear_nomos_enforcement_mode_cache() -> None:
    """Drop the process-level enforcement-mode memos.

    For test harnesses that rebuild deployments of the same name under fresh
    paths within one process; production has stable deployment roots.
    """
    _NOMOS_DEPLOYMENT_DIR_CACHE.clear()
    _NOMOS_MODE_CACHE.clear()


def _raise_nomos_resolution_error(
    deployment_name: str, manifest_path: Any, cause: BaseException
) -> NoReturn:
    """Log and raise a fail-closed error for an unreadable present manifest."""
    logging.getLogger(__name__).error(
        "nomos runtime-enforcement mode for deployment %r is unresolved: "
        "manifest %s is present but could not be read/parsed (%s: %s); "
        "failing closed (deny) rather than defaulting to 'off'",
        deployment_name,
        manifest_path,
        type(cause).__name__,
        cause,
    )
    raise NomosEnforcementResolutionError(
        f"deployment {deployment_name!r} manifest {manifest_path} is present "
        f"but could not be read/parsed ({type(cause).__name__}: {cause}); "
        f"refusing to default runtime enforcement to 'off'"
    ) from cause


def _resolve_nomos_manifest_path(deployment_name: str, cache_key: tuple):
    """Resolve ``<deployment_dir>/deployment.yaml`` or ``None`` if absent.

    Returns ``None`` only on a *positive* absence signal: the deployment
    directory cannot be found. A cached directory that has since moved/vanished
    is dropped and re-resolved here (an ``is_dir`` probe), so a stale memo can
    never make a genuinely-present manifest look absent. The returned path is
    not guaranteed to exist -- the caller distinguishes ENOENT (genuine
    absence, ``off``) from other read errors (deny).
    """
    from okg.substrate.deployments import find_deployment_dir

    deployment_dir = _NOMOS_DEPLOYMENT_DIR_CACHE.get(cache_key)
    if deployment_dir is not None and not deployment_dir.is_dir():
        # Stale memo: the resolved dir moved/was removed. Drop it and let the
        # re-resolution below decide absence-vs-relocation.
        _NOMOS_DEPLOYMENT_DIR_CACHE.pop(cache_key, None)
        deployment_dir = None
    if deployment_dir is None:
        deployment_dir = find_deployment_dir(deployment_name)
        if deployment_dir is None:
            return None
        _NOMOS_DEPLOYMENT_DIR_CACHE[cache_key] = deployment_dir
    return deployment_dir / "deployment.yaml"


def deployment_nomos_enforcement_mode(deployment_name: str) -> str:
    """Return the canonical Nomos runtime-enforcement mode for a deployment.

    This is the single source of truth for the runtime-enforcement tri-state.
    The ``OKG_NOMOS_RUNTIME_ENFORCEMENT`` env override is applied here so every
    caller (readiness gate, runtime evaluation, duplicate manifest gates) reads
    one consistent answer.

    Fail-closed contract (design.md Section 2(b)): ``off`` is returned only on a
    *positive* absence signal -- the deployment dir is not found, the
    ``deployment.yaml`` is genuinely missing (ENOENT), or the parsed manifest
    has no ``nomos`` block. Every other failure after the dir is resolved
    (permission, I/O, decode, YAML parse) resolves to a fail-closed refusal via
    :class:`NomosEnforcementResolutionError` -- a present-but-unreadable
    manifest never silently degrades to ``off``. An invalid env override token
    or an unrecognized manifest value is a hard
    :class:`NomosEnforcementConfigError`.
    """
    override = _enforcement_override()
    if override is not None:
        return override

    cache_key = _nomos_enforcement_cache_key(deployment_name)

    # --- resolve phase: locate the deployment dir. Absence here is a positive
    #     signal that no manifest is configured -> off.
    manifest_path = _resolve_nomos_manifest_path(deployment_name, cache_key)
    if manifest_path is None:
        return "off"

    # --- read phase: a resolved dir means a manifest should be present. Only a
    #     genuinely-missing file (ENOENT) is treated as absence -> off; every
    #     other read/parse failure fails closed.
    try:
        manifest_stat = manifest_path.stat()
    except FileNotFoundError:
        # Dir exists but no deployment.yaml: a genuinely unconfigured
        # deployment (e.g. docs-site). Explicit opt-out.
        return "off"
    except OSError as exc:
        _raise_nomos_resolution_error(deployment_name, manifest_path, exc)

    manifest_fingerprint = (
        manifest_stat.st_mtime_ns,
        manifest_stat.st_ctime_ns,
        manifest_stat.st_size,
    )
    cached = _NOMOS_MODE_CACHE.get(cache_key)
    if cached is not None and cached[0] == manifest_fingerprint:
        return cached[1]

    from okg.substrate import fastyaml

    try:
        manifest_text = manifest_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        # Raced away between stat and read: treat as genuine absence.
        return "off"
    except (OSError, UnicodeDecodeError) as exc:
        _raise_nomos_resolution_error(deployment_name, manifest_path, exc)

    try:
        raw = fastyaml.safe_load(manifest_text) or {}
    except yaml.YAMLError as exc:
        # A present manifest whose YAML we cannot parse: fail closed.
        _raise_nomos_resolution_error(deployment_name, manifest_path, exc)

    mode = manifest_nomos_enforcement_mode(raw)
    _NOMOS_MODE_CACHE[cache_key] = (manifest_fingerprint, mode)
    return mode


def manifest_nomos_enforcement_mode(manifest: Any) -> str:
    """Resolve the tri-state enforcement mode from a parsed manifest mapping.

    The ``OKG_NOMOS_RUNTIME_ENFORCEMENT`` env override wins so the duplicate
    manifest-only gates honor the kill-switch exactly like the deployment-name
    gate does. A present-but-unrecognized enforcement value is a hard
    :class:`NomosEnforcementConfigError` rather than a silent ``off``.
    """
    override = _enforcement_override()
    if override is not None:
        return override
    if not isinstance(manifest, Mapping):
        return "off"
    nomos = manifest.get("nomos")
    if not isinstance(nomos, Mapping):
        return "off"
    raw_mode = _raw_enforcement_mode_value(nomos)
    if raw_mode is None:
        # No enforcement field configured -> genuine absence -> off.
        return "off"
    normalized = _normalize_enforcement_mode_value(raw_mode)
    if normalized is None:
        raise NomosEnforcementConfigError(
            f"manifest nomos.runtime_enforcement value {raw_mode!r} is not a "
            f"recognized enforcement mode (expected one of "
            f"{sorted(NOMOS_ENFORCEMENT_MODES)} or a boolean-like value)"
        )
    return normalized


def manifest_nomos_enforcement_mode_source(manifest: Any) -> str:
    """Return where the effective manifest enforcement mode came from.

    An invalid ``OKG_NOMOS_RUNTIME_ENFORCEMENT`` token raises
    :class:`NomosEnforcementConfigError` here too, keeping the source label
    consistent with the resolved mode instead of masking a bad token.
    """
    if _enforcement_override() is not None:
        return "environment"
    if isinstance(manifest, Mapping):
        nomos = manifest.get("nomos")
        if isinstance(nomos, Mapping):
            raw_mode = _raw_enforcement_mode_value(nomos)
            if _normalize_enforcement_mode_value(raw_mode) is not None:
                return "manifest"
    return "default"


def manifest_nomos_blocks_actions(manifest: Any) -> bool:
    """Return whether the manifest's Nomos mode blocks denied effects."""
    return manifest_nomos_enforcement_mode(manifest) == "enforce"


def _parse_audit_exception_expiry(raw: Any) -> datetime | None:
    if isinstance(raw, datetime):
        expires_at = raw
    elif isinstance(raw, date):
        expires_at = datetime(raw.year, raw.month, raw.day, tzinfo=timezone.utc)
    elif isinstance(raw, str) and raw.strip():
        text = raw.strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            expires_at = datetime.fromisoformat(text)
        except ValueError:
            return None
    else:
        return None
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return expires_at.astimezone(timezone.utc)


def manifest_nomos_audit_readiness_exception(
    manifest: Any,
    *,
    readiness_profile: str,
) -> dict[str, Any] | None:
    """Return an explicit audit-mode readiness exception for one profile.

    The accepted shape is intentionally narrow:

    ``nomos.rollout.audit_readiness_exception.allowed: true`` plus
    ``readiness_profiles`` containing the profile being checked, a reason, an
    owner, and a future ``expires_at`` timestamp. A bare audit mode is not
    enough to satisfy production readiness.
    """
    profile = str(readiness_profile or "").strip()
    if not profile or not isinstance(manifest, Mapping):
        return None
    if manifest_nomos_enforcement_mode(manifest) != "audit":
        return None
    nomos = manifest.get("nomos")
    if not isinstance(nomos, Mapping):
        return None
    rollout = nomos.get("rollout")
    if not isinstance(rollout, Mapping):
        return None
    exception = rollout.get("audit_readiness_exception")
    if not isinstance(exception, Mapping) or exception.get("allowed") is not True:
        return None
    raw_profiles = exception.get("readiness_profiles")
    if not isinstance(raw_profiles, (list, tuple)):
        return None
    profiles = tuple(
        str(value).strip()
        for value in raw_profiles
        if str(value).strip()
    )
    if profile not in profiles:
        return None
    reason = str(exception.get("reason") or "").strip()
    if not reason:
        return None
    owner = exception.get("owner")
    if isinstance(owner, str) and owner.strip():
        owner_payload: str | dict[str, str] | None = owner.strip()
    elif isinstance(owner, Mapping):
        owner_mapping = {
            str(key): str(value)
            for key, value in owner.items()
            if str(key).strip() and str(value).strip()
        }
        owner_payload = owner_mapping or None
    else:
        owner_payload = None
    if owner_payload is None:
        return None
    expires_at = _parse_audit_exception_expiry(exception.get("expires_at"))
    if expires_at is None or expires_at <= datetime.now(timezone.utc):
        return None
    return {
        "allowed": True,
        "readiness_profile": profile,
        "readiness_profiles": list(profiles),
        "reason": reason,
        "owner": owner_payload,
        "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
    }


def deployment_nomos_enforcement_mode_source(deployment_name: str) -> str:
    """Return where a deployment's effective enforcement mode came from."""
    if _enforcement_override() is not None:
        return "environment"
    try:
        from okg.substrate import fastyaml
        from okg.substrate.deployments import find_deployment_dir

        deployment_dir = find_deployment_dir(deployment_name)
        if deployment_dir is None:
            return "default"
        raw = fastyaml.safe_load(
            (deployment_dir / "deployment.yaml").read_text(encoding="utf-8"),
        ) or {}
    except FileNotFoundError:
        # Genuine absence: dir exists but no deployment.yaml. Match the resolver's
        # positive-absence contract -> defaulted source.
        return "default"
    # A present-but-unreadable/unparseable manifest (permission, decode, YAML)
    # surfaces here instead of being mislabeled "default", matching the
    # fail-closed contract of deployment_nomos_enforcement_mode.
    return manifest_nomos_enforcement_mode_source(raw)


def deployment_nomos_audit_readiness_exception(
    deployment_name: str,
    *,
    readiness_profile: str,
) -> dict[str, Any] | None:
    """Resolve a deployment manifest and return its audit readiness exception."""
    try:
        from okg.substrate import fastyaml
        from okg.substrate.deployments import find_deployment_dir

        deployment_dir = find_deployment_dir(deployment_name)
        if deployment_dir is None:
            return None
        raw = fastyaml.safe_load(
            (deployment_dir / "deployment.yaml").read_text(encoding="utf-8"),
        ) or {}
    except Exception:  # noqa: BLE001 - readiness reports missing exception.
        return None
    return manifest_nomos_audit_readiness_exception(
        raw,
        readiness_profile=readiness_profile,
    )


def deployment_requires_nomos_runtime(deployment_name: str) -> bool:
    """Return whether a deployment evaluates the production Nomos runtime gates.

    True for both ``audit`` and ``enforce`` so the boundary is EVALUATED (and the
    decision recorded) in audit mode; only ``off`` skips evaluation entirely.
    """
    return deployment_nomos_enforcement_mode(deployment_name) != "off"


def runtime_surface_mapping_for(surface: str) -> SurfaceDecisionMapping:
    """Resolve a runtime surface, including action-named privacy aliases."""
    if surface == "redact":
        return SurfaceDecisionMapping(
            surface="redact",
            action="redact",
            resource_kind="graph_export",
            principal_kind="okg_service",
            classification="restricted",
        )
    if surface == "declassify":
        return SurfaceDecisionMapping(
            surface="declassify",
            action="declassify",
            resource_kind="graph_export",
            principal_kind="human_operator",
            classification="restricted",
        )
    return surface_mapping_for(surface)


def surface_for_action(action: str, *, fallback_surface: str | None = None) -> str:
    """Choose a canonical runtime surface for an action."""
    if fallback_surface in SURFACE_DECISION_MAPPINGS:
        return str(fallback_surface)
    if action in REQUIRED_NOMOS_RUNTIME_BOUNDARIES:
        return action
    for surface, mapping in SURFACE_DECISION_MAPPINGS.items():
        if mapping.action == action:
            return surface
    return fallback_surface or action


def evaluate_runtime_boundary(
    bundle: CompiledPolicyBundle | None,
    request: NomosRuntimeRequest,
    *,
    active_policy_fingerprint: str | None = None,
    active_config_fingerprint: str | None = None,
    active_state_fresh: bool = True,
    primitive_checks: Iterable[NomosPrimitiveCheck] = (),
    performed_obligations: Iterable[str] | None = None,
    payload: Any | None = None,
    enforcement_mode: str = "enforce",
) -> NomosRuntimeOutcome:
    """Evaluate and enforce one sensitive runtime boundary.

    The policy decision is always built from privacy-safe input. Shape errors,
    missing identity, stale active state, unknown surfaces, primitive denials, and
    obligation mismatch all fail closed with a recorded deny/review/quarantine
    decision instead of silently allowing the effect.

    ``enforcement_mode`` selects the tri-state behavior. With ``"enforce"``
    (the default) the function is byte-identical to the strict evaluation: a
    denied/reviewed/quarantined boundary returns ``allowed=False``. With
    ``"audit"`` the SAME decision is still computed and recorded (the audit
    trail), but a non-allow effect is reported as ``allowed=True`` so callers do
    not block; the observed strict decision is preserved in the outcome trace
    under ``audit_observed`` (log-and-allow). ``"off"`` is not evaluated here —
    callers gate on it before reaching this boundary.
    """
    outcome = _evaluate_runtime_boundary_strict(
        bundle,
        request,
        active_policy_fingerprint=active_policy_fingerprint,
        active_config_fingerprint=active_config_fingerprint,
        active_state_fresh=active_state_fresh,
        primitive_checks=primitive_checks,
        performed_obligations=performed_obligations,
        payload=payload,
    )
    if enforcement_mode == "audit" and not outcome.allowed:
        return _audit_log_and_allow_outcome(outcome)
    return outcome


def evaluate_agent_runtime_decision(
    bundle: CompiledPolicyBundle | None,
    request: AgentRuntimeDecisionRequest,
    *,
    active_policy_fingerprint: str | None = None,
    active_config_fingerprint: str | None = None,
    active_state_fresh: bool = True,
    primitive_checks: Iterable[NomosPrimitiveCheck] = (),
    performed_obligations: Iterable[str] | None = None,
    payload: Any | None = None,
    enforcement_mode: str = "enforce",
) -> NomosRuntimeOutcome:
    """Evaluate one agent-local runtime effect using the full Nomos shape.

    This is a typed adapter over ``evaluate_runtime_boundary``. It keeps the
    canonical Decision API fields visible to policies and traces without letting
    caller-specific launch code invent local action names or persist raw payloads.
    Shape errors fail closed as Nomos decisions.
    """
    mapping, validation_error = _validate_agent_runtime_decision_request(request)
    break_glass_status = _break_glass_status(request)
    performed = tuple(performed_obligations or ())
    if break_glass_status.get("active") is True:
        performed = tuple(dict.fromkeys((*performed, "break_glass_review")))
    if validation_error is not None:
        decision_input = _agent_decision_input_for_request(request, mapping=mapping)
        decision = _fail_closed_decision(
            bundle,
            decision_input,
            reason_code=validation_error,
            enforcement_point=_agent_enforcement_point(request, mapping),
        )
        return _enrich_agent_runtime_trace(
            _outcome(
                decision,
                allowed=False,
                final_effect=decision.effect,
                primitive_checks=tuple(primitive_checks),
                active_config_fingerprint=active_config_fingerprint,
            ),
            request,
            break_glass_status=break_glass_status,
        )

    assert mapping is not None
    outcome = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface=mapping.surface,
            subject=request.subject_binding_id,
            resource={
                "resource_kind": request.resource_kind,
                "resource_id": request.resource_id,
                "agent_run_id": request.agent_run_id,
            },
            classification=request.data_classification,
            context=_agent_decision_context(request, mapping),
        ),
        active_policy_fingerprint=active_policy_fingerprint,
        active_config_fingerprint=active_config_fingerprint,
        active_state_fresh=active_state_fresh,
        primitive_checks=primitive_checks,
        performed_obligations=performed,
        payload=payload,
        enforcement_mode=enforcement_mode,
    )
    return _enrich_agent_runtime_trace(
        outcome,
        request,
        break_glass_status=break_glass_status,
    )


def _audit_log_and_allow_outcome(
    strict: NomosRuntimeOutcome,
) -> NomosRuntimeOutcome:
    """Wrap a strict deny/review/quarantine outcome as audit log-and-allow.

    The strict decision and final_effect are preserved verbatim in the recorded
    decision and under ``trace['audit_observed']`` (the audit trail), while the
    effect callers read is forced to allowed so an audit-mode deployment does not
    block. Only invoked when the strict outcome was NOT already allowed.
    """
    observed = {
        "enforcement_mode": "audit",
        "strict_allowed": strict.allowed,
        "strict_final_effect": strict.final_effect,
        "decision_effect": strict.decision.effect,
        "decision_reason_code": strict.decision.reason_code,
        "decision_id": strict.decision.decision_id,
        "missing_obligations": list(strict.missing_obligations),
    }
    trace = dict(strict.trace)
    trace["allowed"] = True
    trace["audit_observed"] = observed
    return NomosRuntimeOutcome(
        decision=strict.decision,
        allowed=True,
        final_effect=strict.final_effect,
        required_obligations=strict.required_obligations,
        enforced_obligations=strict.enforced_obligations,
        missing_obligations=strict.missing_obligations,
        primitive_checks=strict.primitive_checks,
        trace=trace,
        payload=strict.payload,
        audit_log_and_allow=True,
    )


def _evaluate_runtime_boundary_strict(
    bundle: CompiledPolicyBundle | None,
    request: NomosRuntimeRequest,
    *,
    active_policy_fingerprint: str | None = None,
    active_config_fingerprint: str | None = None,
    active_state_fresh: bool = True,
    primitive_checks: Iterable[NomosPrimitiveCheck] = (),
    performed_obligations: Iterable[str] | None = None,
    payload: Any | None = None,
) -> NomosRuntimeOutcome:
    """Strict (enforce) evaluation of one sensitive runtime boundary."""
    primitive_results = tuple(primitive_checks)
    mapping, validation_error = _validate_request_shape(request)
    if validation_error is not None:
        decision_input = _decision_input_for_failure(request, mapping=mapping)
        decision = _fail_closed_decision(
            bundle,
            decision_input,
            reason_code=validation_error,
            enforcement_point=request.surface or "nomos.runtime",
        )
        return _outcome(
            decision,
            allowed=False,
            final_effect=decision.effect,
            primitive_checks=primitive_results,
            active_config_fingerprint=active_config_fingerprint,
        )

    assert mapping is not None
    if not active_state_fresh or _active_bundle_mismatch(
        bundle,
        active_policy_fingerprint,
    ):
        decision_input = _decision_input_for_request(request, mapping)
        decision = _fail_closed_decision(
            bundle,
            decision_input,
            reason_code="nomos.runtime.stale_active_state",
            enforcement_point=mapping.surface,
        )
        return _outcome(
            decision,
            allowed=False,
            final_effect=decision.effect,
            primitive_checks=primitive_results,
            active_config_fingerprint=active_config_fingerprint,
        )

    decision_input = _decision_input_for_request(request, mapping)
    decision = _sanitize_evaluated_decision(
        evaluate(
            bundle,
            decision_input,
            enforcement_point=mapping.surface,
        ),
        decision_input,
    )
    if decision.effect in ALLOW_EFFECTS:
        denied_primitive = next(
            (check for check in primitive_results if not check.allowed),
            None,
        )
        if denied_primitive is not None:
            decision = _fail_closed_decision(
                bundle,
                decision_input,
                reason_code=denied_primitive.reason_code
                or "nomos.runtime.primitive_denied",
                enforcement_point=mapping.surface,
                context_extra={
                    "primitive": denied_primitive.name,
                    "primitive_metadata": denied_primitive.metadata,
                },
            )
            return _outcome(
                decision,
                allowed=False,
                final_effect=decision.effect,
                primitive_checks=primitive_results,
                active_config_fingerprint=active_config_fingerprint,
            )

    decision = _validate_obligation_shape(bundle, decision, decision_input, mapping)
    obligation_result = _obligation_result(
        decision,
        performed_obligations=performed_obligations,
        payload=payload,
    )
    final_effect = _final_effect(decision, obligation_result["missing"])
    enforcement_reason_code: str | None = None
    if final_effect != decision.effect:
        enforcement_reason_code = "nomos.runtime.obligation_missing"
    allowed = final_effect in ALLOW_EFFECTS and not obligation_result["blocks_effect"]
    if final_effect in TERMINAL_DENY_EFFECTS:
        allowed = False
    if obligation_result["blocks_effect"]:
        final_effect = "deny"
        enforcement_reason_code = "nomos.runtime.obligation_blocked"
        allowed = False

    return _outcome(
        decision,
        allowed=allowed,
        final_effect=final_effect,
        enforcement_reason_code=enforcement_reason_code,
        primitive_checks=primitive_results,
        active_config_fingerprint=active_config_fingerprint,
        required_obligations=obligation_result["required"],
        enforced_obligations=obligation_result["enforced"],
        missing_obligations=obligation_result["missing"],
        payload=obligation_result["payload"],
    )


# Issue #2784: a decision is sanitized exactly once. ``sanitize_policy_decision``
# stamps the PolicyDecision it builds with this private instance attribute (not
# a dataclass field, so equality, ``to_dict`` and ``dataclasses.replace`` never
# see or copy it). Every later boundary -- the outcome, its trace, the decision
# persist -- receives that same object and reuses it instead of walking the
# resource, context and obligations again. A decision built any other way,
# including ``dataclasses.replace`` of a stamped one, carries no stamp and is
# sanitized in full.
#
# The stamp is bound to content, not trust: it holds a fingerprint of the
# sanitized resource, context and obligations, and a decision is reused only
# while its current content still hashes to that fingerprint. A decision whose
# mappings were changed in place after sanitizing -- at any depth -- fails the
# comparison and is sanitized in full again, so the reuse never depends on
# callers leaving the mutable mappings alone.
_SANITIZED_DECISION_ATTR = "_nomos_sanitized_decision"


def _sanitized_content_fingerprint(decision: PolicyDecision) -> str | None:
    """Fingerprint of the fields ``sanitize_policy_decision`` rewrites.

    ``None`` when the content cannot be serialized canonically (for example
    keys of mixed types after an in-place edit); such a decision never
    matches a stamp and is sanitized in full.
    """
    try:
        encoded = json.dumps(
            [decision.resource, decision.context, decision.obligations],
            sort_keys=True,
            separators=(",", ":"),
            default=_fingerprint_default,
        )
    except (TypeError, ValueError):
        return None
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _fingerprint_default(value: Any) -> Any:
    # Distinguish a non-JSON object from the string it prints as, so replacing
    # a sanitized string with an object that renders the same is still a change.
    return {"__nomos_non_json__": type(value).__qualname__, "repr": repr(value)}


def _stamp_sanitized(decision: PolicyDecision) -> PolicyDecision:
    object.__setattr__(
        decision,
        _SANITIZED_DECISION_ATTR,
        _sanitized_content_fingerprint(decision),
    )
    return decision


def is_sanitized_policy_decision(decision: Any) -> TypeGuard[PolicyDecision]:
    """True when ``decision`` was built by the sanitizer and is unchanged since.

    The stamp must be present and the decision's resource, context and
    obligations must still hash to the fingerprint taken when it was built.
    """
    if type(decision) is not PolicyDecision:
        return False
    stamp = vars(decision).get(_SANITIZED_DECISION_ATTR)
    if not isinstance(stamp, str):
        return False
    return _sanitized_content_fingerprint(decision) == stamp


def sanitize_policy_decision(decision: PolicyDecision) -> PolicyDecision:
    """Return a decision whose persisted trace fields are privacy safe.

    The result is stamped as sanitized; passing it back in returns it
    unchanged, so the read path pays for one sanitize per decision however
    many boundaries (outcome, trace, persist) re-check it.
    """
    if is_sanitized_policy_decision(decision):
        return decision
    safe = PolicyDecision(
        decision_id=decision.decision_id,
        policy_bundle_id=decision.policy_bundle_id,
        policy_version=decision.policy_version,
        policy_fingerprint=decision.policy_fingerprint,
        input_hash=decision.input_hash,
        subject=decision.subject,
        action=decision.action,
        resource=sanitize_trace_mapping(decision.resource),
        classification=decision.classification,
        context=sanitize_trace_mapping(decision.context),
        effect=decision.effect,
        reason_code=decision.reason_code,
        matched_rule_ids=decision.matched_rule_ids,
        obligations=tuple(
            sanitize_trace_mapping(item)
            for item in decision.obligations
            if isinstance(item, Mapping)
        ),
        enforcement_point=decision.enforcement_point,
    )
    return _stamp_sanitized(safe)


def _sanitize_evaluated_decision(
    decision: PolicyDecision,
    decision_input: DecisionInput,
) -> PolicyDecision:
    """Sanitize a decision ``evaluate`` built from ``_decision_input_for_request``.

    That input's resource and context are sanitize outputs already, and
    ``evaluate`` copies them one level deep. When the decision still holds
    exactly those values (same keys, the very same value objects), walking
    them again would redact nothing new, so they are reused; only the
    policy's obligations -- the one part the input did not carry -- are
    sanitized. Any other shape is sanitized in full. Either way the result is
    stamped, so the outcome, its trace and the persist reuse it (issue #2784).
    """
    if not (
        _holds_same_values(decision.resource, decision_input.resource)
        and _holds_same_values(decision.context, decision_input.context)
    ):
        return sanitize_policy_decision(decision)
    safe = PolicyDecision(
        decision_id=decision.decision_id,
        policy_bundle_id=decision.policy_bundle_id,
        policy_version=decision.policy_version,
        policy_fingerprint=decision.policy_fingerprint,
        input_hash=decision.input_hash,
        subject=decision.subject,
        action=decision.action,
        resource=dict(decision.resource),
        classification=decision.classification,
        context=dict(decision.context),
        effect=decision.effect,
        reason_code=decision.reason_code,
        matched_rule_ids=decision.matched_rule_ids,
        obligations=tuple(
            sanitize_trace_mapping(item)
            for item in decision.obligations
            if isinstance(item, Mapping)
        ),
        enforcement_point=decision.enforcement_point,
    )
    return _stamp_sanitized(safe)


def _holds_same_values(copied: Any, source: Mapping[str, Any]) -> bool:
    """True when ``copied`` has ``source``'s keys, each bound to the same object."""
    if type(copied) is not dict or copied.keys() != source.keys():
        return False
    return all(copied[key] is value for key, value in source.items())


def sanitize_decision_trace_mapping(
    decision: PolicyDecision | Mapping[str, Any],
) -> dict[str, Any]:
    """Build the stable privacy-safe trace mapping persisted or exported."""
    if is_sanitized_policy_decision(decision):
        # Already sanitized once (issue #2784); ``to_dict`` copies every
        # container, so the trace never aliases the decision's mappings.
        return decision.to_dict()
    raw = decision.to_dict() if isinstance(decision, PolicyDecision) else dict(decision)
    raw["resource"] = sanitize_trace_value(raw.get("resource") or {})
    raw["context"] = sanitize_trace_value(raw.get("context") or {})
    raw["obligations"] = sanitize_trace_value(raw.get("obligations") or [])
    return dict(raw)


def sanitize_trace_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    safe = sanitize_trace_value(value)
    return safe if isinstance(safe, dict) else {}


@functools.lru_cache(maxsize=4096)
def _trace_key_redaction(key_l: str) -> str | None:
    """Redaction kind a (lowercased) trace key forces on its value, if any.

    A pure function of the key string, cached because a read pass sanitizes
    the same few dozen keys hundreds of thousands of times (issue #2784).
    """
    if key_l and (looks_secret_key(key_l) or key_l in _AUTHORIZATION_KEYS):
        return "secret"
    if key_l in _TRACE_PAYLOAD_KEYS:
        return "payload"
    return None


# Exact scalar types ``json.dumps(..., allow_nan=False)`` always accepts.
# ``int`` is deliberately absent: an int over Python's 4,300-digit conversion
# limit makes ``json.dumps`` raise, and must still become a non_json marker.
_JSON_SAFE_SCALAR_TYPES = (bool, type(None))


def sanitize_trace_value(value: Any, *, key: str | None = None, depth: int = 0) -> Any:
    """Redact secrets, raw payloads, local paths, and PHI-shaped values."""
    if depth > 8:
        return _redaction_marker("max_depth", value)
    if isinstance(value, bytes | bytearray | memoryview):
        return _redaction_marker("binary", value)
    key_redaction = _trace_key_redaction((key or "").lower())
    if key_redaction is not None:
        return _redaction_marker(key_redaction, value)
    # Exact-type fast paths (issue #2784). They only skip checks that cannot
    # match: an exact ``str`` is never a Mapping and is excluded from the
    # Sequence branch, and an exact bool/None always survives the JSON check
    # at the bottom unchanged.
    value_type = type(value)
    if value_type is str:
        return _sanitize_trace_string(value)
    if value_type in _JSON_SAFE_SCALAR_TYPES:
        return value
    if isinstance(value, _AbcMapping):
        return {
            str(k): sanitize_trace_value(v, key=str(k), depth=depth + 1)
            for k, v in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, _AbcSequence) and not isinstance(value, str | bytes | bytearray):
        return [
            sanitize_trace_value(item, key=key, depth=depth + 1)
            for item in value
        ]
    if isinstance(value, str):
        return _sanitize_trace_string(value)
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError):
        return _redaction_marker("non_json", type(value).__qualname__)
    return value


def _sanitize_trace_string(value: str) -> Any:
    """String leg of ``sanitize_trace_value``: secret refs, PHI, paths, credentials."""
    stripped = value.strip()
    if stripped.startswith(_SECRET_REF_PREFIXES):
        return _redaction_marker("secret_ref", stripped)
    if _SSN_RE.search(stripped) or (
        "patient" in stripped.lower()
        and _DATE_OF_BIRTH_RE.search(stripped)
    ):
        return _redaction_marker("phi", stripped)
    if _LOCAL_PATH_RE.search(stripped):
        return _redaction_marker("local_path", stripped)
    if _looks_like_credential_literal(stripped):
        return _redaction_marker("secret", stripped)
    if len(stripped) > 512:
        return {
            "redacted": "long_string",
            "sha256": "sha256:" + _sha256_hex(stripped),
            "length": len(stripped),
        }
    return value


def _validate_request_shape(
    request: NomosRuntimeRequest,
) -> tuple[SurfaceDecisionMapping | None, str | None]:
    if not isinstance(request.surface, str) or not request.surface.strip():
        return None, "nomos.runtime.malformed_request"
    try:
        mapping = runtime_surface_mapping_for(request.surface)
    except NomosVocabularyError:
        return None, "nomos.runtime.unknown_boundary"
    if mapping.action not in ACTIONS:
        return mapping, "nomos.runtime.unknown_action"
    if mapping.resource_kind not in RESOURCE_KINDS:
        return mapping, "nomos.runtime.unknown_resource_kind"
    if mapping.principal_kind not in PRINCIPAL_KINDS:
        return mapping, "nomos.runtime.unknown_principal_kind"
    if mapping.classification not in DATA_CLASSIFICATIONS:
        return mapping, "nomos.runtime.unknown_classification"
    if not isinstance(request.subject, str) or not request.subject.strip():
        return mapping, "nomos.runtime.missing_identity"
    if not isinstance(request.resource, Mapping):
        return mapping, "nomos.runtime.malformed_request"
    if not isinstance(request.context, Mapping):
        return mapping, "nomos.runtime.malformed_request"
    if not _is_json_compatible(request.resource):
        return mapping, "nomos.runtime.malformed_request"
    if not _is_json_compatible(request.context):
        return mapping, "nomos.runtime.malformed_request"
    return mapping, None


def _decision_input_for_request(
    request: NomosRuntimeRequest,
    mapping: SurfaceDecisionMapping,
) -> DecisionInput:
    # Each mapping is exactly one ``sanitize_trace_mapping`` output: the
    # vocabulary defaults sit under the request's own keys (a key the request
    # sets wins, as ``setdefault`` did) and are sanitized with it. Only key
    # order differs from sanitize-then-setdefault, and the input hash and rule
    # matching are order-free. ``_sanitize_evaluated_decision`` relies on this
    # to reuse these mappings instead of walking them again (issue #2784).
    resource = sanitize_trace_mapping({
        "resource_kind": mapping.resource_kind,
        "nomos_surface": mapping.surface,
        **request.resource,
    })
    context = sanitize_trace_mapping({
        "principal_kind": mapping.principal_kind,
        "nomos_boundary": mapping.surface,
        **request.context,
    })
    classification = request.classification or mapping.classification
    return DecisionInput(
        subject=request.subject.strip(),
        action=mapping.action,
        resource=resource,
        classification=classification,
        context=context,
    )


def _decision_input_for_failure(
    request: NomosRuntimeRequest,
    *,
    mapping: SurfaceDecisionMapping | None,
) -> DecisionInput:
    action = mapping.action if mapping is not None else str(request.surface or "")
    resource = (
        sanitize_trace_mapping(request.resource)
        if isinstance(request.resource, Mapping)
        else {}
    )
    if mapping is not None:
        resource.setdefault("resource_kind", mapping.resource_kind)
        resource.setdefault("nomos_surface", mapping.surface)
    context = (
        sanitize_trace_mapping(request.context)
        if isinstance(request.context, Mapping)
        else {}
    )
    return DecisionInput(
        subject=request.subject.strip() if isinstance(request.subject, str) else "",
        action=action,
        resource=resource,
        classification=(
            request.classification
            or (mapping.classification if mapping is not None else "internal")
        ),
        context=context,
    )


def _validate_agent_runtime_decision_request(
    request: AgentRuntimeDecisionRequest,
) -> tuple[SurfaceDecisionMapping | None, str | None]:
    if not isinstance(request.action, str) or not request.action.strip():
        return None, "nomos.runtime.malformed_request"
    if request.action not in ACTIONS:
        return None, "nomos.runtime.unknown_action"
    surface = surface_for_action(request.action, fallback_surface=request.action)
    try:
        mapping = runtime_surface_mapping_for(surface)
    except NomosVocabularyError:
        return None, "nomos.runtime.unknown_boundary"
    if mapping.action != request.action:
        return mapping, "nomos.runtime.unknown_action"
    if request.principal_kind not in PRINCIPAL_KINDS:
        return mapping, "nomos.runtime.unknown_principal_kind"
    if request.resource_kind not in RESOURCE_KINDS:
        return mapping, "nomos.runtime.unknown_resource_kind"
    if request.data_classification not in DATA_CLASSIFICATIONS:
        return mapping, "nomos.runtime.unknown_classification"
    for field_name in (
        "subject_binding_id",
        "resource_id",
        "policy_generation_pin",
        "enforcement_point",
        "agent_run_id",
    ):
        value = getattr(request, field_name)
        if not isinstance(value, str) or not value.strip():
            return mapping, f"nomos.runtime.missing_{field_name}"
    try:
        enforcement_mapping = runtime_surface_mapping_for(
            request.enforcement_point.strip()
        )
    except NomosVocabularyError:
        return mapping, "nomos.runtime.unknown_enforcement_point"
    if enforcement_mapping.action != request.action:
        return mapping, "nomos.runtime.enforcement_point_mismatch"
    if request.config_affects_decision and not (
        isinstance(request.config_generation_pin, str)
        and request.config_generation_pin.strip()
    ):
        return mapping, "nomos.runtime.missing_config_generation_pin"
    if not isinstance(request.deployment_posture, Mapping):
        return mapping, "nomos.runtime.malformed_request"
    if not isinstance(request.context, Mapping):
        return mapping, "nomos.runtime.malformed_request"
    if request.break_glass_grant is not None and not isinstance(
        request.break_glass_grant,
        Mapping,
    ):
        return mapping, "nomos.runtime.malformed_break_glass_grant"
    if not _is_json_compatible(request.deployment_posture):
        return mapping, "nomos.runtime.malformed_request"
    if not _is_json_compatible(request.context):
        return mapping, "nomos.runtime.malformed_request"
    return mapping, None


def _agent_decision_input_for_request(
    request: AgentRuntimeDecisionRequest,
    *,
    mapping: SurfaceDecisionMapping | None,
) -> DecisionInput:
    action = request.action if isinstance(request.action, str) else ""
    resource_kind = (
        request.resource_kind
        if isinstance(request.resource_kind, str) and request.resource_kind in RESOURCE_KINDS
        else (mapping.resource_kind if mapping is not None else "agent_runtime")
    )
    classification = (
        request.data_classification
        if (
            isinstance(request.data_classification, str)
            and request.data_classification in DATA_CLASSIFICATIONS
        )
        else (mapping.classification if mapping is not None else "internal")
    )
    context = _agent_decision_context(request, mapping) if mapping is not None else {
        "subject_binding_id": getattr(request, "subject_binding_id", ""),
        "principal_kind": getattr(request, "principal_kind", ""),
        "agent_run_id": getattr(request, "agent_run_id", ""),
        "profile_id": getattr(request, "profile_id", ""),
        "deployment_posture": getattr(request, "deployment_posture", {}),
        "policy_generation_pin": getattr(request, "policy_generation_pin", ""),
        "config_generation_pin": getattr(request, "config_generation_pin", None),
        "config_affects_decision": getattr(
            request,
            "config_affects_decision",
            False,
        ),
        "caller_enforcement_point": getattr(request, "enforcement_point", ""),
        "caller_context": getattr(request, "context", {}),
    }
    return DecisionInput(
        subject=(
            request.subject_binding_id.strip()
            if isinstance(request.subject_binding_id, str)
            else ""
        ),
        action=action,
        resource=sanitize_trace_mapping({
            "resource_kind": resource_kind,
            "resource_id": getattr(request, "resource_id", ""),
            "agent_run_id": getattr(request, "agent_run_id", ""),
            "nomos_surface": mapping.surface if mapping is not None else action,
        }),
        classification=classification,
        context=sanitize_trace_mapping(context),
    )


def _agent_decision_context(
    request: AgentRuntimeDecisionRequest,
    mapping: SurfaceDecisionMapping,
) -> dict[str, Any]:
    return sanitize_trace_mapping({
        "subject_binding_id": request.subject_binding_id,
        "principal_kind": request.principal_kind,
        "agent_run_id": request.agent_run_id,
        "profile_id": request.profile_id,
        "deployment_posture": request.deployment_posture,
        "policy_generation_pin": request.policy_generation_pin,
        "config_generation_pin": request.config_generation_pin,
        "config_affects_decision": request.config_affects_decision,
        "profile_allows_unsafe_without_break_glass": (
            request.profile_allows_unsafe_without_break_glass
        ),
        "caller_enforcement_point": request.enforcement_point,
        "nomos_boundary": mapping.surface,
        "caller_context": request.context,
    })


def _agent_enforcement_point(
    request: AgentRuntimeDecisionRequest,
    mapping: SurfaceDecisionMapping | None,
) -> str:
    if mapping is not None:
        return mapping.surface
    raw = getattr(request, "enforcement_point", "")
    return raw.strip() if isinstance(raw, str) and raw.strip() else "agent.runtime"


def _break_glass_status(
    request: AgentRuntimeDecisionRequest,
) -> dict[str, Any]:
    if request.action != "agent.bypass":
        return {"required": False, "active": False}
    if request.profile_allows_unsafe_without_break_glass:
        return {
            "required": False,
            "active": False,
            "profile_exception": True,
        }
    grant = request.break_glass_grant
    if not isinstance(grant, Mapping):
        return {
            "required": True,
            "active": False,
            "reason_code": "nomos.runtime.break_glass_missing",
        }
    required_fields = (
        "operator",
        "subject",
        "agent_run_id",
        "scope",
        "reason",
        "decision_id",
        "follow_up_review_state",
        "expires_at",
    )
    missing = [
        field
        for field in required_fields
        if not isinstance(grant.get(field), str) or not str(grant.get(field)).strip()
    ]
    if missing:
        return {
            "required": True,
            "active": False,
            "reason_code": "nomos.runtime.break_glass_malformed",
            "missing_fields": missing,
        }
    if str(grant["subject"]).strip() != request.subject_binding_id.strip():
        return {
            "required": True,
            "active": False,
            "reason_code": "nomos.runtime.break_glass_subject_mismatch",
        }
    if str(grant["agent_run_id"]).strip() != request.agent_run_id.strip():
        return {
            "required": True,
            "active": False,
            "reason_code": "nomos.runtime.break_glass_run_mismatch",
        }
    review_state = str(grant["follow_up_review_state"]).strip().lower()
    if review_state in {"closed", "expired", "revoked"}:
        return {
            "required": True,
            "active": False,
            "reason_code": "nomos.runtime.break_glass_closed",
            "follow_up_review_state": review_state,
        }
    expires_at = _parse_break_glass_expiry(grant.get("expires_at"))
    if expires_at is None:
        return {
            "required": True,
            "active": False,
            "reason_code": "nomos.runtime.break_glass_malformed",
            "field": "expires_at",
        }
    if expires_at <= datetime.now(timezone.utc):
        return {
            "required": True,
            "active": False,
            "reason_code": "nomos.runtime.break_glass_expired",
            "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
        }
    return sanitize_trace_mapping({
        "required": True,
        "active": True,
        "operator": grant["operator"],
        "subject": grant["subject"],
        "agent_run_id": grant["agent_run_id"],
        "scope": grant["scope"],
        "reason": grant["reason"],
        "decision_id": grant["decision_id"],
        "follow_up_review_state": review_state,
        "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
    })


def _parse_break_glass_expiry(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    text = raw.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        expires_at = datetime.fromisoformat(text)
    except ValueError:
        return None
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return expires_at.astimezone(timezone.utc)


def _enrich_agent_runtime_trace(
    outcome: NomosRuntimeOutcome,
    request: AgentRuntimeDecisionRequest,
    *,
    break_glass_status: Mapping[str, Any],
) -> NomosRuntimeOutcome:
    trace = dict(outcome.trace)
    trace["trace_ref"] = "nomos:trace:" + _sha256_hex({
        "decision_id": outcome.decision.decision_id,
        "input_hash": outcome.decision.input_hash,
        "agent_run_id": request.agent_run_id,
        "action": request.action,
    })[:24]
    trace["policy_generation_pin"] = request.policy_generation_pin
    trace["config_generation_pin"] = request.config_generation_pin
    trace["ordered_obligations"] = list(
        _ordered_obligations(
            outcome.decision.obligations,
            set(outcome.required_obligations),
        )
    )
    trace["agent_runtime"] = sanitize_trace_mapping({
        "subject_binding_id": request.subject_binding_id,
        "principal_kind": request.principal_kind,
        "action": request.action,
        "resource_kind": request.resource_kind,
        "resource_id": request.resource_id,
        "data_classification": request.data_classification,
        "deployment_posture": request.deployment_posture,
        "enforcement_point": request.enforcement_point,
        "agent_run_id": request.agent_run_id,
        "profile_id": request.profile_id,
    })
    trace["break_glass"] = sanitize_trace_value(break_glass_status)
    if break_glass_status.get("active") is True:
        trace["break_glass_finding"] = {
            "severity": "high",
            "status": "active",
            "agent_run_id": request.agent_run_id,
            "decision_id": outcome.decision.decision_id,
            "follow_up_review_state": break_glass_status.get(
                "follow_up_review_state",
            ),
        }
    return NomosRuntimeOutcome(
        decision=outcome.decision,
        allowed=outcome.allowed,
        final_effect=outcome.final_effect,
        required_obligations=outcome.required_obligations,
        enforced_obligations=outcome.enforced_obligations,
        missing_obligations=outcome.missing_obligations,
        primitive_checks=outcome.primitive_checks,
        trace=trace,
        payload=outcome.payload,
        audit_log_and_allow=outcome.audit_log_and_allow,
    )


def _fail_closed_decision(
    bundle: CompiledPolicyBundle | None,
    decision_input: DecisionInput,
    *,
    reason_code: str,
    enforcement_point: str,
    effect: str = "deny",
    context_extra: Mapping[str, Any] | None = None,
) -> PolicyDecision:
    safe_context = sanitize_trace_mapping({
        **dict(decision_input.context),
        **dict(context_extra or {}),
    })
    safe_resource = sanitize_trace_mapping(decision_input.resource)
    input_payload = {
        "subject": decision_input.subject,
        "action": decision_input.action,
        "resource": safe_resource,
        "classification": decision_input.classification,
        "context": safe_context,
    }
    input_hash = "sha256:" + _sha256_hex(input_payload)
    policy_fingerprint = bundle.policy_fingerprint if bundle is not None else None
    seed = {
        "policy_fingerprint": policy_fingerprint,
        "input_hash": input_hash,
        "effect": effect,
        "reason_code": reason_code,
    }
    return PolicyDecision(
        decision_id="nomos:decision:" + _sha256_hex(seed)[:24],
        policy_bundle_id=bundle.policy_bundle_id if bundle is not None else None,
        policy_version=bundle.policy_version if bundle is not None else None,
        policy_fingerprint=policy_fingerprint,
        input_hash=input_hash,
        subject=decision_input.subject,
        action=decision_input.action,
        resource=safe_resource,
        classification=decision_input.classification,
        context=safe_context,
        effect=effect,
        reason_code=reason_code,
        matched_rule_ids=(),
        obligations=(),
        enforcement_point=enforcement_point,
    )


def _validate_obligation_shape(
    bundle: CompiledPolicyBundle | None,
    decision: PolicyDecision,
    decision_input: DecisionInput,
    mapping: SurfaceDecisionMapping,
) -> PolicyDecision:
    for obligation in decision.obligations:
        if not isinstance(obligation, Mapping):
            return _malformed_obligation_decision(
                bundle,
                decision,
                decision_input,
                mapping,
                malformed_obligation="<non-object>",
            )
        obligation_type = obligation.get("type")
        if (
            not isinstance(obligation_type, str)
            or not obligation_type.strip()
            or obligation_type not in OBLIGATION_TYPES
        ):
            return _malformed_obligation_decision(
                bundle,
                decision,
                decision_input,
                mapping,
                malformed_obligation=str(obligation_type),
            )
    obligation_types = _obligation_types(decision.obligations)
    required_for_effect = _EFFECT_REQUIRED_OBLIGATION.get(decision.effect)
    if required_for_effect and required_for_effect not in obligation_types:
        return _fail_closed_decision(
            bundle,
            decision_input,
            reason_code="nomos.runtime.obligation_mismatch",
            enforcement_point=mapping.surface,
            context_extra={
                "policy_decision_id": decision.decision_id,
                "missing_obligation": required_for_effect,
            },
        )
    return decision


def _malformed_obligation_decision(
    bundle: CompiledPolicyBundle | None,
    decision: PolicyDecision,
    decision_input: DecisionInput,
    mapping: SurfaceDecisionMapping,
    *,
    malformed_obligation: str,
) -> PolicyDecision:
    return _fail_closed_decision(
        bundle,
        decision_input,
        reason_code="nomos.runtime.obligation_mismatch",
        enforcement_point=mapping.surface,
        context_extra={
            "policy_decision_id": decision.decision_id,
            "malformed_obligation": malformed_obligation,
        },
    )


def _obligation_result(
    decision: PolicyDecision,
    *,
    performed_obligations: Iterable[str] | None,
    payload: Any | None,
) -> dict[str, Any]:
    required = set(_obligation_types(decision.obligations))
    required.update(_required_obligations_for_action(decision))
    required_for_effect = _EFFECT_REQUIRED_OBLIGATION.get(decision.effect)
    if required_for_effect:
        required.add(required_for_effect)
    enforced = set()
    if performed_obligations is not None:
        enforced = {
            item for item in performed_obligations
            if item in OBLIGATION_TYPES
        }
    if "audit" in required:
        enforced.add("audit")

    out_payload = payload
    if "redaction" in required and payload is not None:
        out_payload = _redact_payload(payload)
        enforced.add("redaction")

    missing: set[str] = required - enforced

    blocks_effect = False
    if "no_export" in required and decision.action in {
        "graph.export",
        "graph.read.export_like",
    }:
        blocks_effect = True
        enforced.add("no_export")
        missing.discard("no_export")
    return {
        "required": _ordered_obligations(decision.obligations, required),
        "enforced": tuple(sorted(enforced)),
        "missing": tuple(sorted(missing)),
        "blocks_effect": blocks_effect,
        "payload": out_payload,
    }


def _required_obligations_for_action(decision: PolicyDecision) -> tuple[str, ...]:
    if (
        decision.action == "agent.bypass"
        and decision.context.get("profile_allows_unsafe_without_break_glass") is True
    ):
        return ()
    return _ACTION_REQUIRED_OBLIGATIONS.get(decision.action, ())


def _ordered_obligations(
    obligations: Iterable[Mapping[str, Any]],
    required: set[str],
) -> tuple[str, ...]:
    ordered: list[str] = []
    for obligation in obligations:
        obligation_type = (
            obligation.get("type") if isinstance(obligation, Mapping) else None
        )
        if isinstance(obligation_type, str) and obligation_type in required:
            ordered.append(obligation_type)
    ordered.extend(sorted(required - set(ordered)))
    return tuple(ordered)


def _outcome(
    decision: PolicyDecision,
    *,
    allowed: bool,
    final_effect: str,
    enforcement_reason_code: str | None = None,
    primitive_checks: tuple[NomosPrimitiveCheck, ...],
    active_config_fingerprint: str | None,
    required_obligations: tuple[str, ...] = (),
    enforced_obligations: tuple[str, ...] = (),
    missing_obligations: tuple[str, ...] = (),
    payload: Any | None = None,
) -> NomosRuntimeOutcome:
    if final_effect != decision.effect:
        decision = _enforced_decision(
            decision,
            final_effect=final_effect,
            reason_code=(
                enforcement_reason_code
                or f"nomos.runtime.enforced_{final_effect}"
            ),
        )
    safe_decision = sanitize_policy_decision(decision)
    trace = sanitize_decision_trace_mapping(safe_decision)
    trace["final_effect"] = final_effect
    trace["allowed"] = allowed
    trace["required_obligations"] = list(required_obligations)
    trace["enforced_obligations"] = list(enforced_obligations)
    trace["missing_obligations"] = list(missing_obligations)
    trace["active_config_fingerprint"] = active_config_fingerprint
    trace["primitive_checks"] = [check.to_trace() for check in primitive_checks]
    return NomosRuntimeOutcome(
        decision=safe_decision,
        allowed=allowed,
        final_effect=final_effect,
        required_obligations=required_obligations,
        enforced_obligations=enforced_obligations,
        missing_obligations=missing_obligations,
        primitive_checks=primitive_checks,
        trace=trace,
        payload=payload,
    )


def _active_bundle_mismatch(
    bundle: CompiledPolicyBundle | None,
    active_policy_fingerprint: str | None,
) -> bool:
    if active_policy_fingerprint is None or bundle is None:
        return False
    return bundle.policy_fingerprint != active_policy_fingerprint


def _final_effect(
    decision: PolicyDecision,
    missing_obligations: Sequence[str],
) -> str:
    if missing_obligations:
        return "deny"
    return decision.effect


def _obligation_types(obligations: Iterable[Mapping[str, Any]]) -> set[str]:
    types: set[str] = set()
    for obligation in obligations:
        obligation_type = obligation.get("type") if isinstance(obligation, Mapping) else None
        if isinstance(obligation_type, str) and obligation_type in OBLIGATION_TYPES:
            types.add(obligation_type)
    return types


def _redact_payload(payload: Any) -> Any:
    if isinstance(payload, Mapping):
        return {
            str(key): _redact_payload(value)
            for key, value in payload.items()
            if str(key) not in {
                "body",
                "content",
                "evidence",
                # Issue #2991: the stamper's copy of a `text` body.
                "okg_producer_text",
                "private_body",
                "raw",
                "raw_body",
                "text",
            }
        } | {"nomos_redacted": True}
    if isinstance(payload, list):
        return [_redact_payload(item) for item in payload]
    return payload


def _enforced_decision(
    decision: PolicyDecision,
    *,
    final_effect: str,
    reason_code: str,
) -> PolicyDecision:
    context = {
        **dict(decision.context),
        "policy_decision_id": decision.decision_id,
        "policy_effect": decision.effect,
        "policy_reason_code": decision.reason_code,
    }
    seed = {
        "policy_fingerprint": decision.policy_fingerprint,
        "input_hash": decision.input_hash,
        "effect": final_effect,
        "reason_code": reason_code,
        "matched_rule_ids": list(decision.matched_rule_ids),
    }
    return PolicyDecision(
        decision_id="nomos:decision:" + _sha256_hex(seed)[:24],
        policy_bundle_id=decision.policy_bundle_id,
        policy_version=decision.policy_version,
        policy_fingerprint=decision.policy_fingerprint,
        input_hash=decision.input_hash,
        subject=decision.subject,
        action=decision.action,
        resource=decision.resource,
        classification=decision.classification,
        context=context,
        effect=final_effect,
        reason_code=reason_code,
        matched_rule_ids=decision.matched_rule_ids,
        obligations=decision.obligations,
        enforcement_point=decision.enforcement_point,
    )


def _is_json_compatible(value: Any) -> bool:
    if value is None or isinstance(value, str | int | bool):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, bytes | bytearray | memoryview):
        return False
    if isinstance(value, Mapping):
        return all(
            isinstance(key, str) and _is_json_compatible(item)
            for key, item in value.items()
        )
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return all(_is_json_compatible(item) for item in value)
    return False


def _looks_like_credential_literal(value: str) -> bool:
    lowered = value.lower()
    if lowered.startswith(("bearer ", "basic ")):
        return True
    if lowered.startswith(("sk-", "ghp_", "github_pat_", "xoxb-", "akia")):
        return True
    return bool(re.search(r"(?:token|secret|password|api[_-]?key)\s*[:=]", lowered))


def _redaction_marker(kind: str, value: Any) -> dict[str, Any]:
    return {
        "redacted": kind,
        "sha256": "sha256:" + _redaction_hash(value),
    }


def _redaction_hash(value: Any) -> str:
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytes | bytearray):
        return hashlib.sha256(bytes(value)).hexdigest()
    try:
        return _sha256_hex(value)
    except TypeError:
        return hashlib.sha256(
            type(value).__qualname__.encode("utf-8")
        ).hexdigest()


def _sha256_hex(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            _json_safe(value),
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_json_safe(v) for v in value]
    return value
