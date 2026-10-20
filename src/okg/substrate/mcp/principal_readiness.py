"""Safe principal diagnostics; configuration and observed HTTP facts stay separate."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from okg.substrate.mcp.principal_mapping import (
    PrincipalMapping,
    principal_mapping_from_manifest,
    validate_mapping_against_providers,
)


def load_readiness_providers(conn: Any, deployment: str) -> Sequence[Any]:
    """Isolate a failed read with a transaction/savepoint, preserving caller work."""
    from okg.substrate.nomos.identity_store import load_identity_providers

    with conn.transaction():
        return load_identity_providers(conn, deployment_name=deployment)


def _provider_status(
    mapping: PrincipalMapping | None,
    deployment: str,
    load_providers: Callable[[], Sequence[Any]],
) -> dict[str, str]:
    if mapping is None:
        return {"status": "unmeasured", "reason": "mapping_absent"}
    try:
        providers = load_providers()
    except Exception:  # noqa: BLE001 - never return provider inventories/errors.
        return {"status": "unmeasured", "reason": "provider_probe_unavailable"}
    try:
        validate_mapping_against_providers(
            mapping, providers, deployment_name=deployment
        )
    except Exception:  # noqa: BLE001 - validator data contains identity metadata.
        return {"status": "blocked", "reason": "provider_validation_failed"}
    return {"status": "validated"}


def _request_route_observation(server: Any) -> str:
    """Observe only the exact route on the app serving this SDK request.

    Constructed apps, an unrelated wrapped route and ambiguous mount shapes
    cannot establish this fact. This is not a negative-credential HTTP probe.
    """
    from mcp.server.auth.middleware.bearer_auth import RequireAuthMiddleware
    from starlette.routing import Route

    try:
        request = server._mcp_server.request_context.request
        scope = request.scope
        path = server.settings.streamable_http_path
        if scope.get("type") != "http" or scope.get("path") != path:
            return "unmeasured"
        app = scope.get("app")
        routes = [
            route
            for route in app.routes
            if isinstance(route, Route) and route.path == path
        ]
        if len(routes) == 1 and isinstance(routes[0].endpoint, RequireAuthMiddleware):
            return "auth_wrapper_observed"
    except (AttributeError, LookupError, TypeError):
        return "unmeasured"
    return "unmeasured"


def runtime_principal_readiness(
    server: Any,
    *,
    deployment: str,
    load_providers: Callable[[], Sequence[Any]],
) -> dict[str, Any]:
    """Read the process's retained mapping and freshly loaded provider rows."""
    from okg.substrate.mcp.http_auth import server_has_token_verifier

    mapping = getattr(server, "_okg_principal_mapping", None)
    return {
        "runtime_mode": "multi_user"
        if getattr(server, "_okg_multi_user_principals", False)
        else "single_operator",
        "credential_verifier": "configured"
        if server_has_token_verifier(server)
        else "absent",
        "mapping": {
            "status": "configured" if mapping is not None else "absent",
            "source": "startup_parsed",
        },
        "provider_validation": _provider_status(mapping, deployment, load_providers),
        "http_protection": _request_route_observation(server),
        "http_credential_probe": "unmeasured",
    }


def offline_principal_readiness(conn: Any, deployment: str) -> dict[str, Any]:
    """Inspect current manifest preconditions, without claiming a served mode."""
    from okg.substrate.deployments import resolve_deployment
    from okg.substrate.nomos.runtime_enforcement import manifest_nomos_enforcement_mode

    report: dict[str, Any] = {
        "runtime_mode": "unmeasured",
        "credential_verifier": "unmeasured",
        "mapping": {"status": "unmeasured", "source": "current_manifest"},
        "runtime_enforcement": "unmeasured",
        "provider_validation": {
            "status": "unmeasured",
            "reason": "mapping_unavailable",
        },
        "http_protection": "unmeasured",
        "http_credential_probe": "unmeasured",
    }
    try:
        manifest = resolve_deployment(deployment)
    except Exception:  # noqa: BLE001 - no paths or manifest error payloads.
        report["mapping"].update(reason="manifest_unavailable")
        return report
    try:
        mapping = principal_mapping_from_manifest(manifest)
    except Exception:  # noqa: BLE001 - parse/posture failures use a fixed code.
        report["mapping"].update(status="blocked", reason="mapping_invalid")
        return report
    report["mapping"]["status"] = "configured" if mapping is not None else "absent"
    try:
        mode = manifest_nomos_enforcement_mode({"nomos": manifest.nomos})
        report["runtime_enforcement"] = "disabled" if mode == "off" else "enabled"
    except Exception:  # noqa: BLE001 - invalid enforcement is a separate fact.
        report["runtime_enforcement"] = "invalid"
    report["provider_validation"] = _provider_status(
        mapping,
        deployment,
        lambda: load_readiness_providers(conn, deployment),
    )
    return report
