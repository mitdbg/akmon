"""Reusable source preflight helpers.

These helpers only inspect references and lightweight probes. They do
not fetch full source payloads and they never return secret values.
"""
from __future__ import annotations

import importlib
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from okg.substrate.library.sources.base import SourcePreflightResult
from okg.substrate.sources.redaction import redact_endpoint, redact_text


def _checked_at() -> str:
    return datetime.now(timezone.utc).isoformat()


def credential_env_preflight(
    source_name: str,
    credential_ref: str,
    *,
    aliases: tuple[str, ...] = (),
    required: bool = False,
    mode: str = "live",
    env: Mapping[str, str] | None = None,
) -> SourcePreflightResult:
    """Check that a token-like env ref or one of its aliases exists."""
    env = os.environ if env is None else env
    if env.get(credential_ref):
        return SourcePreflightResult(
            source_name=source_name,
            status="ok",
            mode=mode,
            required=required,
            credential_refs=(credential_ref,),
            reason=f"{credential_ref} is set",
            checked_at=_checked_at(),
        )
    used_aliases = tuple(a for a in aliases if env.get(a))
    if used_aliases:
        return SourcePreflightResult(
            source_name=source_name,
            status="ok",
            mode=mode,
            required=required,
            credential_refs=(credential_ref,),
            alias_refs={credential_ref: used_aliases},
            reason="using_alias",
            checked_at=_checked_at(),
        )
    return SourcePreflightResult(
        source_name=source_name,
        status="missing_credential",
        mode=mode,
        required=required,
        credential_refs=(credential_ref,),
        alias_refs={credential_ref: tuple(aliases)} if aliases else {},
        reason=f"{credential_ref} is not set",
        checked_at=_checked_at(),
    )


def file_ref_preflight(
    source_name: str,
    credential_ref: str,
    *,
    aliases: tuple[str, ...] = (),
    required: bool = False,
    mode: str = "live",
    env: Mapping[str, str] | None = None,
) -> SourcePreflightResult:
    """Check that a file path env ref exists."""
    env = os.environ if env is None else env
    refs = (credential_ref,) + tuple(aliases)
    for ref in refs:
        value = env.get(ref)
        if value and Path(value).is_file():
            alias_refs = (
                {credential_ref: (ref,)}
                if ref != credential_ref else {}
            )
            return SourcePreflightResult(
                source_name=source_name,
                status="ok",
                mode=mode,
                required=required,
                credential_refs=(credential_ref,),
                alias_refs=alias_refs,
                reason="using_alias" if ref != credential_ref else "file exists",
                checked_at=_checked_at(),
            )
    return SourcePreflightResult(
        source_name=source_name,
        status="missing_credential",
        mode=mode,
        required=required,
        credential_refs=(credential_ref,),
        alias_refs={credential_ref: tuple(aliases)} if aliases else {},
        reason=f"{credential_ref} file is not set or does not exist",
        checked_at=_checked_at(),
    )


def dependency_preflight(
    source_name: str,
    module_name: str,
    *,
    required: bool = False,
    mode: str = "live",
) -> SourcePreflightResult:
    """Check that a runtime dependency can be imported."""
    try:
        importlib.import_module(module_name)
    except Exception as exc:  # noqa: BLE001
        return SourcePreflightResult(
            source_name=source_name,
            status="endpoint_failed",
            mode=mode,
            required=required,
            reason=redact_text(
                f"import {module_name!r} failed: {type(exc).__name__}: {exc}"
            ),
            checked_at=_checked_at(),
        )
    return SourcePreflightResult(
        source_name=source_name,
        status="ok",
        mode=mode,
        required=required,
        reason=f"import {module_name!r} succeeded",
        checked_at=_checked_at(),
    )


def http_probe_result(
    source_name: str,
    *,
    ok: bool,
    endpoint: str,
    required: bool = False,
    mode: str = "live",
    reason: str = "",
    login_page_detected: bool = False,
) -> SourcePreflightResult:
    """Convert a tiny HTTP probe outcome to a redacted result."""
    if ok and not login_page_detected:
        status = "ok"
    elif login_page_detected:
        status = "auth_failed"
    else:
        status = "endpoint_failed"
    return SourcePreflightResult(
        source_name=source_name,
        status=status,
        mode=mode,
        required=required,
        endpoint=redact_endpoint(endpoint),
        reason=redact_text(reason or status),
        checked_at=_checked_at(),
    )
