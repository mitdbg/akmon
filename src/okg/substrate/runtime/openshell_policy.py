"""Generate OpenShell worker-containment policy from deployment config."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

import yaml

from okg.substrate.deployments import (
    DeploymentManifest,
    require_postgres_dsn,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.sources.registry import SourceEntry, load_source_registry


_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

_URL_KEYS = frozenset({
    "api_url",
    "base_url",
    "database_url",
    "dsn",
    "endpoint",
    "graphql_url",
    "index_url",
    "rss_url",
    "url",
})
_URL_KEY_SUFFIXES = ("_endpoint", "_url")


@dataclass(frozen=True, order=True)
class EgressEndpoint:
    """One derived outbound network target."""

    host: str
    port: int
    source: str
    path: str
    scheme: str

    def key(self) -> tuple[str, int]:
        return (self.host, self.port)


class OpenShellPolicyError(SubstrateError):
    """Raised when the sandbox policy cannot be derived exactly."""


def generate_openshell_policy(
    manifest: DeploymentManifest,
    *,
    env: Mapping[str, str] | None = None,
    postgres_dsn: str | None = None,
) -> dict[str, Any]:
    """Derive a deterministic OpenShell policy from a deployment.

    The policy intentionally has a small OpenShell-native shape:
    filesystem access is constrained to the deployment directory and
    Python venv, and network policy has explicit host:port endpoints
    from the source registry and deployment Postgres DSN.
    """
    dsn = postgres_dsn or require_postgres_dsn(
        manifest,
        deployment=manifest.name,
    )
    effective_env = dict(env or os.environ)
    effective_env["OKG_DSN"] = dsn

    registry = load_source_registry(manifest.source_registry)
    endpoints = _collect_source_endpoints(registry, env=effective_env)

    postgres_endpoint = _parse_endpoint_string(
        dsn,
        source="deployment.postgres",
        path="deployment.postgres.dsn",
        env=effective_env,
    )
    endpoints.append(postgres_endpoint)

    egress = _dedupe_endpoints(endpoints)
    filesystem = _filesystem_policy(manifest.deployment_dir, env=effective_env)
    venv = _derive_venv_path(manifest.deployment_dir.resolve(), env=effective_env)

    return {
        "version": 1,
        "filesystem_policy": filesystem,
        "landlock": {
            "compatibility": "hard_requirement",
        },
        "process": {
            "run_as_user": "sandbox",
            "run_as_group": "sandbox",
        },
        "network_policies": {
            "okg_runtime_worker": {
                "name": f"{manifest.name}-runtime-worker",
                "endpoints": egress,
                "binaries": [
                    {"path": f"{venv}/bin/**"},
                ],
            },
        },
    }


def render_openshell_policy_yaml(policy: Mapping[str, Any]) -> str:
    """Render policy YAML with stable key order."""
    return yaml.safe_dump(policy, sort_keys=False)


def _filesystem_policy(
    deployment_dir: Path,
    *,
    env: Mapping[str, str],
) -> dict[str, list[str]]:
    deployment_root = deployment_dir.resolve()
    venv = _derive_venv_path(deployment_root, env=env)
    return {
        "read_only": [str(venv)],
        "read_write": [str(deployment_root)],
    }


def _derive_venv_path(
    deployment_dir: Path,
    *,
    env: Mapping[str, str],
) -> Path:
    candidates: list[Path] = []
    if env.get("VIRTUAL_ENV"):
        candidates.append(Path(env["VIRTUAL_ENV"]))
    if deployment_dir.parent.name == "deployments":
        candidates.append(deployment_dir.parent.parent / ".venv")
    candidates.append(Path.cwd() / ".venv")

    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if resolved.exists():
            return resolved
    raise OpenShellPolicyError(
        "cannot derive OpenShell filesystem policy: Python venv not found",
        fix_hint=(
            "set VIRTUAL_ENV or create a .venv next to the repository "
            "containing the deployment"
        ),
        data={"deployment_dir": str(deployment_dir)},
    )


def _collect_source_endpoints(
    registry: Mapping[str, SourceEntry],
    *,
    env: Mapping[str, str],
) -> list[EgressEndpoint]:
    endpoints: list[EgressEndpoint] = []
    for source_name in sorted(registry):
        entry = registry[source_name]
        # Derive sandbox egress ONLY from the runtime `params` block, not the
        # whole registry entry. Walking `entry.raw` previously minted any nested
        # `*_url` / `*_endpoint` metadata key (e.g. metadata.documentation_url)
        # into allowed egress, over-granting the worker's network. Runtime
        # endpoints a worker actually dials live under `params:` (the deployment
        # Postgres DSN is added separately). Per centralize-egress-url-validation.
        endpoints.extend(
            _walk_declared_endpoints(
                entry.raw.get("params"),
                source=source_name,
                path=f"sources.{source_name}.params",
                env=env,
            )
        )
    return endpoints


def _walk_declared_endpoints(
    node: Any,
    *,
    source: str,
    path: str,
    env: Mapping[str, str],
) -> list[EgressEndpoint]:
    endpoints: list[EgressEndpoint] = []
    if isinstance(node, dict):
        for key, value in node.items():
            key_str = str(key)
            child_path = f"{path}.{key_str}"
            key_l = key_str.lower()
            if key_l == "host":
                endpoints.append(
                    _parse_host_value(
                        value,
                        source=source,
                        path=child_path,
                        port=node.get("port"),
                        env=env,
                    )
                )
                continue
            if _is_endpoint_key(key_l):
                values = value if isinstance(value, list) else [value]
                for idx, item in enumerate(values):
                    item_path = (
                        f"{child_path}[{idx}]"
                        if isinstance(value, list) else child_path
                    )
                    endpoints.append(
                        _parse_endpoint_string(
                            item,
                            source=source,
                            path=item_path,
                            env=env,
                        )
                    )
                continue
            endpoints.extend(
                _walk_declared_endpoints(
                    value,
                    source=source,
                    path=child_path,
                    env=env,
                )
            )
    elif isinstance(node, list):
        for idx, item in enumerate(node):
            endpoints.extend(
                _walk_declared_endpoints(
                    item,
                    source=source,
                    path=f"{path}[{idx}]",
                    env=env,
                )
            )
    return endpoints


def _is_endpoint_key(key: str) -> bool:
    return key in _URL_KEYS or any(
        key.endswith(suffix) for suffix in _URL_KEY_SUFFIXES
    )


def _parse_endpoint_string(
    value: Any,
    *,
    source: str,
    path: str,
    env: Mapping[str, str],
) -> EgressEndpoint:
    if not isinstance(value, str) or not value.strip():
        raise _underivable_endpoint(
            source=source,
            path=path,
            value=value,
            reason="expected a non-empty string",
        )
    resolved = _expand_env(value.strip(), source=source, path=path, env=env)
    parsed = urlparse(resolved)
    scheme = parsed.scheme.lower()
    host = parsed.hostname
    if not scheme or not host:
        raise _underivable_endpoint(
            source=source,
            path=path,
            value=value,
            reason="expected an absolute URL or DSN with a host",
        )
    try:
        parsed_port = parsed.port
    except ValueError as exc:
        raise _underivable_endpoint(
            source=source,
            path=path,
            value=value,
            reason=str(exc),
        ) from exc
    port = parsed_port or _default_port_for_scheme(scheme)
    if port is None:
        raise _underivable_endpoint(
            source=source,
            path=path,
            value=value,
            reason=f"no default port for scheme {scheme!r}",
        )
    return EgressEndpoint(
        host=host.lower(),
        port=port,
        source=source,
        path=path,
        scheme=scheme,
    )


def _parse_host_value(
    value: Any,
    *,
    source: str,
    path: str,
    port: Any,
    env: Mapping[str, str],
) -> EgressEndpoint:
    if not isinstance(value, str) or not value.strip():
        raise _underivable_endpoint(
            source=source,
            path=path,
            value=value,
            reason="expected a non-empty host string",
        )
    resolved = _expand_env(value.strip(), source=source, path=path, env=env)
    parsed = urlparse(f"//{resolved}")
    host = parsed.hostname
    try:
        parsed_port = parsed.port
    except ValueError as exc:
        raise _underivable_endpoint(
            source=source,
            path=path,
            value=value,
            reason=str(exc),
        ) from exc
    port_value = parsed_port if parsed_port is not None else port
    if isinstance(port_value, str) and port_value.isdigit():
        port_value = int(port_value)
    if not host or not isinstance(port_value, int):
        raise _underivable_endpoint(
            source=source,
            path=path,
            value=value,
            reason="host entries must declare a port",
        )
    return EgressEndpoint(
        host=host.lower(),
        port=port_value,
        source=source,
        path=path,
        scheme="tcp",
    )


def _expand_env(
    value: str,
    *,
    source: str,
    path: str,
    env: Mapping[str, str],
) -> str:
    missing: list[str] = []

    def _replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in env:
            missing.append(name)
            return match.group(0)
        return env[name]

    expanded = _ENV_RE.sub(_replace, value)
    if missing:
        raise _underivable_endpoint(
            source=source,
            path=path,
            value=value,
            reason=f"unresolved env placeholder(s): {', '.join(missing)}",
        )
    return expanded


def _default_port_for_scheme(scheme: str) -> int | None:
    if scheme == "http":
        return 80
    if scheme == "https":
        return 443
    if scheme in {"postgres", "postgresql"}:
        return 5432
    return None


def _dedupe_endpoints(
    endpoints: list[EgressEndpoint],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int], list[EgressEndpoint]] = {}
    for endpoint in endpoints:
        grouped.setdefault(endpoint.key(), []).append(endpoint)

    out: list[dict[str, Any]] = []
    for host, port in sorted(grouped):
        out.append({
            "host": host,
            "port": port,
            "enforcement": "enforce",
        })
    return out


def _underivable_endpoint(
    *,
    source: str,
    path: str,
    value: Any,
    reason: str,
) -> OpenShellPolicyError:
    return OpenShellPolicyError(
        "cannot derive OpenShell egress policy from declared source "
        f"endpoint at {path}: {reason}",
        fix_hint=(
            "declare an absolute http(s)/postgres URL, a host with a "
            "port, or remove the endpoint field; wildcard egress is "
            "never generated"
        ),
        data={"source": source, "path": path, "value": value},
    )
