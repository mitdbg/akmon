"""Deployment-registered MCP tool loading.

The substrate owns only the registration contract here. Tool domain
semantics, credentials, query syntax, and result shaping stay in the
deployment that declares the tool.
"""

from __future__ import annotations

import functools
import importlib
import inspect
import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from okg.substrate.deployments import (
    DeploymentConfigError,
    DeploymentManifest,
    DeploymentResolutionError,
    resolve_deployment,
)


log = logging.getLogger(__name__)

VALID_BOUNDARIES = {"okg_graph", "external_read", "external_live"}
CATALOG_ONLY_KINDS = {
    "operator_macro",
    "query_recipe",
    "score_function",
    "resource",
}
EXTERNAL_LIVE_REQUIRED_KEYS = {
    "boundary",
    "source",
    "observed_at",
    "query",
    "time_window",
}


@dataclass(frozen=True)
class RegisteredDeploymentTool:
    name: str
    module: str
    function: str
    boundary: str


@dataclass(frozen=True)
class SkippedDeploymentTool:
    name: str
    reason: str
    boundary: str | None = None
    profile: str = "compat"
    generation_pinned: bool | None = None
    description: str | None = None
    kind: str | None = None
    operator: str | None = None
    resource: str | None = None
    sql: str | None = None


def deployment_tool_prefix(deployment_name: str) -> str:
    """Return the required MCP name prefix for deployment tools."""
    normalized = re.sub(r"[^0-9A-Za-z_]+", "_", deployment_name).strip("_")
    if not normalized:
        normalized = "deployment"
    return f"{normalized}_"


def catalog_deployment_tool_declarations(
    *,
    deployment_name: str,
    manifest: DeploymentManifest | None = None,
) -> list[SkippedDeploymentTool]:
    """Return deployment tool declarations without registering them.

    The operator surface excludes deployment ``agent_tools`` by default, but
    the catalog still needs to expose their boundary so agents can see that a
    live external tool exists only behind the compatibility profile. This
    static pass intentionally avoids importing modules or checking secrets.
    """
    if manifest is None:
        try:
            manifest = resolve_deployment(deployment_name)
        except DeploymentResolutionError:
            return []

    raw_tools = manifest.agent_tools if manifest.agent_tools is not None else {}
    if not isinstance(raw_tools, dict):
        raise DeploymentConfigError("deployment.yaml agent_tools must be a mapping")

    declarations: list[SkippedDeploymentTool] = []
    for name, raw_entry in raw_tools.items():
        try:
            if _is_catalog_only_entry(raw_entry):
                declarations.append(
                    _catalog_only_declaration(
                        deployment_name=deployment_name,
                        name=name,
                        raw_entry=raw_entry,
                    )
                )
                continue
            boundary = _static_tool_boundary(
                deployment_name=deployment_name,
                name=name,
                raw_entry=raw_entry,
            )
            description = (
                raw_entry.get("description")
                if isinstance(raw_entry, dict)
                and isinstance(raw_entry.get("description"), str)
                else None
            )
            declarations.append(
                SkippedDeploymentTool(
                    name=str(name),
                    reason="compatibility_only_excluded_from_operator_surface",
                    boundary=boundary,
                    profile="compat",
                    generation_pinned=(boundary != "external_live"),
                    description=description,
                )
            )
        except Exception as exc:
            optional = _entry_optional(raw_entry)
            if optional:
                declarations.append(
                    SkippedDeploymentTool(
                        name=str(name),
                        reason=f"{type(exc).__name__}: {exc}",
                    )
                )
                continue
            raise DeploymentConfigError(
                f"deployment tool {name!r} is invalid: {exc}"
            ) from exc
    return declarations


def register_deployment_tools(
    mcp: Any,
    *,
    deployment_name: str,
    tracer_factory: Callable[
        [str],
        Callable[[Callable[..., Any]], Callable[..., Any]],
    ],
    manifest: DeploymentManifest | None = None,
    env: Mapping[str, str] | None = None,
) -> list[RegisteredDeploymentTool]:
    """Register deployment-declared tools on an existing FastMCP server.

    Missing deployment manifests are treated as "no deployment tools" so unit
    tests and default local servers can still build without a deployment dir.
    A found manifest with malformed required tools fails closed.
    """
    env_map = env if env is not None else os.environ
    if manifest is None:
        try:
            manifest = resolve_deployment(deployment_name)
        except DeploymentResolutionError:
            mcp._okg_deployment_tools = []  # type: ignore[attr-defined]
            mcp._okg_skipped_deployment_tools = []  # type: ignore[attr-defined]
            return []

    raw_tools = manifest.agent_tools if manifest.agent_tools is not None else {}
    if not isinstance(raw_tools, dict):
        raise DeploymentConfigError("deployment.yaml agent_tools must be a mapping")
    if not raw_tools:
        mcp._okg_deployment_tools = []  # type: ignore[attr-defined]
        mcp._okg_skipped_deployment_tools = []  # type: ignore[attr-defined]
        return []

    registered: list[RegisteredDeploymentTool] = []
    skipped: list[SkippedDeploymentTool] = []
    for name, raw_entry in raw_tools.items():
        try:
            if _is_catalog_only_entry(raw_entry):
                skipped.append(
                    _catalog_only_declaration(
                        deployment_name=deployment_name,
                        name=name,
                        raw_entry=raw_entry,
                    )
                )
                continue
            config = _validate_entry(
                deployment_name=deployment_name,
                name=name,
                raw_entry=raw_entry,
                existing_tool_names=set(mcp._tool_manager._tools.keys()),
                env=env_map,
            )
            fn = _load_callable(config)
            wrapped = _wrap_tool(
                fn,
                tool_name=config.name,
                boundary=config.boundary,
                required_env=config.requires_env,
                env=env_map,
            )
            traced_fn = tracer_factory(config.name)(wrapped)
            # `structured_output=False` (issue #3059): no tool publishes an
            # `outputSchema`; `ByteBoundedFastMCP.call_tool` serves the
            # result once, as one text block in the call's format.
            mcp._tool_manager.add_tool(
                traced_fn,
                name=config.name,
                description=config.description,
                structured_output=False,
                meta={
                    "okg_deployment_tool": True,
                    "deployment_name": deployment_name,
                    "boundary": config.boundary,
                },
            )
            registered.append(
                RegisteredDeploymentTool(
                    name=config.name,
                    module=config.module,
                    function=config.function,
                    boundary=config.boundary,
                )
            )
        except Exception as exc:
            optional = _entry_optional(raw_entry)
            if optional:
                reason = f"{type(exc).__name__}: {exc}"
                log.warning(
                    "skipping optional deployment tool %s: %s",
                    name,
                    reason,
                )
                skipped.append(SkippedDeploymentTool(name=str(name), reason=reason))
                continue
            raise DeploymentConfigError(
                f"deployment tool {name!r} is invalid: {exc}"
            ) from exc

    mcp._okg_deployment_tools = registered  # type: ignore[attr-defined]
    mcp._okg_skipped_deployment_tools = skipped  # type: ignore[attr-defined]
    return registered


@dataclass(frozen=True)
class _ToolConfig:
    name: str
    module: str
    function: str
    boundary: str
    description: str
    requires_env: tuple[str, ...]


def _validate_entry(
    *,
    deployment_name: str,
    name: Any,
    raw_entry: Any,
    existing_tool_names: set[str],
    env: Mapping[str, str],
) -> _ToolConfig:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("tool name must be a non-empty string")
    prefix = deployment_tool_prefix(deployment_name)
    if not name.startswith(prefix):
        raise ValueError(
            f"deployment tool name must be prefixed with {prefix!r}"
        )
    if name in existing_tool_names:
        raise ValueError(f"tool name collides with existing MCP tool {name!r}")
    if not isinstance(raw_entry, dict):
        raise ValueError("tool entry must be a mapping")

    module = _required_str(raw_entry, "module")
    function = _required_str(raw_entry, "function")
    boundary = _required_str(raw_entry, "boundary")
    description = _required_str(raw_entry, "description")
    if boundary not in VALID_BOUNDARIES:
        raise ValueError(
            f"boundary must be one of {sorted(VALID_BOUNDARIES)}; got {boundary!r}"
        )
    if boundary == "external_live":
        lowered = description.lower()
        if "live" not in lowered or (
            "external" not in lowered and "not generation-pinned" not in lowered
        ):
            raise ValueError(
                "external_live tool descriptions must label live external "
                "evidence"
            )

    requires_env = raw_entry.get("requires_env") or []
    if not isinstance(requires_env, list) or not all(
        isinstance(v, str) and v for v in requires_env
    ):
        raise ValueError(
            "requires_env must be a list of environment variable names"
        )
    missing_env = [key for key in requires_env if not env.get(key)]
    if missing_env:
        raise ValueError(
            "missing required environment variable(s): "
            + ", ".join(sorted(missing_env))
        )

    return _ToolConfig(
        name=name,
        module=module,
        function=function,
        boundary=boundary,
        description=description,
        requires_env=tuple(requires_env),
    )


def _static_tool_boundary(
    *,
    deployment_name: str,
    name: Any,
    raw_entry: Any,
) -> str:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("tool name must be a non-empty string")
    _reject_builtin_name_collision(name)
    prefix = deployment_tool_prefix(deployment_name)
    if not name.startswith(prefix):
        raise ValueError(
            f"deployment tool name must be prefixed with {prefix!r}"
        )
    if not isinstance(raw_entry, dict):
        raise ValueError("tool entry must be a mapping")
    _required_str(raw_entry, "module")
    _required_str(raw_entry, "function")
    boundary = _required_str(raw_entry, "boundary")
    description = _required_str(raw_entry, "description")
    if boundary not in VALID_BOUNDARIES:
        raise ValueError(
            f"boundary must be one of {sorted(VALID_BOUNDARIES)}; got {boundary!r}"
        )
    if boundary == "external_live":
        lowered = description.lower()
        if "live" not in lowered or (
            "external" not in lowered and "not generation-pinned" not in lowered
        ):
            raise ValueError(
                "external_live tool descriptions must label live external "
                "evidence"
            )
    return boundary


def _is_catalog_only_entry(raw_entry: Any) -> bool:
    return (
        isinstance(raw_entry, dict)
        and raw_entry.get("kind") in CATALOG_ONLY_KINDS
    )


def _catalog_only_declaration(
    *,
    deployment_name: str,
    name: Any,
    raw_entry: Any,
) -> SkippedDeploymentTool:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("tool name must be a non-empty string")
    _reject_builtin_name_collision(name)
    prefix = deployment_tool_prefix(deployment_name)
    if not name.startswith(prefix):
        raise ValueError(
            f"deployment tool name must be prefixed with {prefix!r}"
        )
    if not isinstance(raw_entry, dict):
        raise ValueError("tool entry must be a mapping")
    kind = str(raw_entry.get("kind") or "")
    if kind not in CATALOG_ONLY_KINDS:
        raise ValueError(
            f"kind must be one of {sorted(CATALOG_ONLY_KINDS)}"
        )
    description = _required_str(raw_entry, "description")
    boundary = str(raw_entry.get("boundary") or "okg_graph")
    if boundary not in VALID_BOUNDARIES:
        raise ValueError(
            f"boundary must be one of {sorted(VALID_BOUNDARIES)}; got {boundary!r}"
        )
    operator = raw_entry.get("operator")
    sql = raw_entry.get("sql")
    resource = raw_entry.get("resource")
    if kind == "operator_macro":
        from okg.substrate.mcp.operators import OPERATOR_TOOLS

        if operator not in OPERATOR_TOOLS:
            raise ValueError(
                "operator_macro requires operator to be one of "
                f"{list(OPERATOR_TOOLS)}"
            )
    if kind == "query_recipe":
        if not isinstance(sql, str) and not isinstance(resource, str):
            raise ValueError("query_recipe requires sql or resource")
    if kind == "resource" and not isinstance(resource, str):
        raise ValueError("resource declaration requires resource")
    return SkippedDeploymentTool(
        name=name,
        reason=f"{kind}_catalog_entry_not_top_level_tool",
        boundary=boundary,
        profile="operator",
        generation_pinned=(boundary != "external_live"),
        description=description,
        kind=kind,
        operator=str(operator) if isinstance(operator, str) else None,
        resource=str(resource) if isinstance(resource, str) else None,
        sql=str(sql) if isinstance(sql, str) else None,
    )


def _reject_builtin_name_collision(name: str) -> None:
    from okg.substrate.mcp.operators import ALL_BUILTIN_TOOLS

    if name in ALL_BUILTIN_TOOLS:
        raise ValueError(f"tool name collides with existing MCP tool {name!r}")


def _required_str(entry: dict[str, Any], key: str) -> str:
    value = entry.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a non-empty string")
    return value


def _entry_optional(raw_entry: Any) -> bool:
    return isinstance(raw_entry, dict) and raw_entry.get("optional") is True


def _load_callable(config: _ToolConfig) -> Callable[..., Any]:
    module = importlib.import_module(config.module)
    fn = getattr(module, config.function)
    if not callable(fn):
        raise TypeError(f"{config.module}.{config.function} is not callable")
    try:
        inspect.signature(fn)
    except (TypeError, ValueError) as exc:
        raise TypeError(
            f"{config.module}.{config.function} has no inspectable signature"
        ) from exc
    return fn


def _wrap_tool(
    fn: Callable[..., Any],
    *,
    tool_name: str,
    boundary: str,
    required_env: tuple[str, ...],
    env: Mapping[str, str],
) -> Callable[..., Any]:
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        result = fn(*args, **kwargs)
        if boundary == "external_live":
            _validate_external_live_result(
                tool_name=tool_name,
                result=result,
                required_env=required_env,
                env=env,
            )
        return result

    return wrapper


def _validate_external_live_result(
    *,
    tool_name: str,
    result: Any,
    required_env: tuple[str, ...],
    env: Mapping[str, str],
) -> None:
    if not isinstance(result, dict):
        raise ValueError(f"{tool_name} external_live result must be a JSON object")
    missing = sorted(EXTERNAL_LIVE_REQUIRED_KEYS - set(result))
    if missing:
        raise ValueError(
            f"{tool_name} external_live result missing keys: {', '.join(missing)}"
        )
    if result.get("boundary") != "external_live":
        raise ValueError(
            f"{tool_name} external_live result must set boundary='external_live'"
        )
    if "results" not in result and "aggregation" not in result:
        raise ValueError(
            f"{tool_name} external_live result must include results or aggregation"
        )
    serialized = json.dumps(result, default=str, sort_keys=True)
    for key in required_env:
        secret = env.get(key)
        if secret and len(secret) >= 8 and secret in serialized:
            raise ValueError(
                f"{tool_name} external_live result includes secret value from {key}"
            )
