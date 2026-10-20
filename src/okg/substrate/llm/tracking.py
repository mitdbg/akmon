"""Resolve the database authority used by optional LLM-call tracking.

Tracking is deliberately opt-in.  A worker's database DSN is an authority
that can be inherited only when a caller explicitly selects ``optional`` or
``required`` tracking; merely having a DSN must never turn tracking on.
"""

from __future__ import annotations

import logging
import re

from psycopg.conninfo import conninfo_to_dict


__all__ = [
    "TRACKING_MODES",
    "TrackingConfigurationError",
    "resolve_tracking_dsn",
]

TRACKING_MODES = frozenset({"disabled", "optional", "required"})
_UNRESOLVED_DSN_RE = re.compile(r"\$\{[^}]+\}")
_WHOLE_ENV_REF_RE = re.compile(r"^\$[A-Za-z_][A-Za-z0-9_]*$")
_log = logging.getLogger(__name__)


class TrackingConfigurationError(ValueError):
    """A tracking configuration cannot be safely assembled."""

    code = "llm_tracking_configuration"

    def __init__(
        self,
        message: str,
        *,
        component: str | None = None,
        reason: str | None = None,
    ) -> None:
        super().__init__(message)
        self.data = {
            key: value
            for key, value in {
                "component": component,
                "reason": reason,
            }.items()
            if value is not None
        }


def _is_unresolved_dsn(value: str) -> bool:
    return bool(_UNRESOLVED_DSN_RE.search(value) or _WHOLE_ENV_REF_RE.fullmatch(value))


def resolve_tracking_dsn(
    *,
    tracking_mode: str | None,
    explicit_tracking_dsn: str | None,
    runtime_dsn: str | None,
    component: str,
) -> str | None:
    """Return the DSN for a mode-gated tracking lane.

    ``tracking_mode=None`` preserves the historical constructor contract:
    an explicit DSN enables optional tracking, while no DSN leaves tracking
    disabled.  A concrete mode is required to inherit ``runtime_dsn``.
    Values are never included in errors or log records.
    """

    if tracking_mode is None:
        mode = "optional" if explicit_tracking_dsn is not None else "disabled"
    else:
        mode = str(tracking_mode).strip().lower()
    if mode not in TRACKING_MODES:
        raise TrackingConfigurationError(
            f"{component} tracking_mode must be one of disabled, optional, "
            "or required",
            component=component,
            reason="invalid_mode",
        )

    explicit = (
        explicit_tracking_dsn.strip()
        if isinstance(explicit_tracking_dsn, str)
        else explicit_tracking_dsn
    )
    if explicit and _is_unresolved_dsn(explicit):
        raise TrackingConfigurationError(
            f"{component} tracking configuration contains an unresolved "
            "database placeholder; resolve the worker database authority "
            "before constructing the client",
            component=component,
            reason="unresolved_explicit_dsn",
        )

    if mode == "disabled":
        return None

    # Explicit authority wins.  In particular, an unused worker placeholder
    # must not poison an explicitly supplied, concrete tracking DSN.
    candidate = explicit or None
    if candidate is None:
        runtime = (
            runtime_dsn.strip() if isinstance(runtime_dsn, str) else runtime_dsn
        )
        if runtime and _is_unresolved_dsn(runtime):
            raise TrackingConfigurationError(
                f"{component} runtime database authority is unresolved; "
                "tracking cannot be constructed",
                component=component,
                reason="unresolved_runtime_dsn",
            )
        candidate = runtime
    if candidate:
        try:
            # Parse only; do not connect.  This catches malformed URI/key-value
            # syntax before a provider client is built while preserving valid
            # socket-only and keyword-form conninfo strings.
            conninfo_to_dict(candidate)
        except Exception as exc:
            raise TrackingConfigurationError(
                f"{component} tracking database authority is malformed; "
                "provide valid PostgreSQL connection information",
                component=component,
                reason="malformed_dsn",
            ) from exc
        return candidate
    if mode == "required":
        raise TrackingConfigurationError(
            f"{component} tracking_mode=required needs a usable runtime "
            "database authority",
            component=component,
            reason="required_authority_missing",
        )

    _log.warning(
        "LLM call tracking unavailable: no usable runtime database authority",
        extra={
            "event": "llm_tracking_unavailable",
            "component": component,
            "tracking_mode": mode,
        },
    )
    return None
