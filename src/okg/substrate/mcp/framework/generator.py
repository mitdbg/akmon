"""Pre-join MCP tool generator: registration at MCP server bootstrap.

Per S-A7-R1 + R3:
- The substrate MCP server loads each deployment's
  `mcp_pre_join_tools.yaml` after catalog load completes and
  before accepting client connections.
- Each tool's request schema is generated from the `params`
  block; the tool's body is a closure over the parsed spec + a
  ResolverRegistry + a callable that hands back the current
  (conn, session) per call.
- Schema-invalid tools are isolated (other tools in the same
  YAML still register).
- A tool whose `resolve:` references an unknown resolver slug
  is isolated separately (PreJoinResolverUnknown).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional

import psycopg

from okg.substrate.deployments import DeploymentManifest
from okg.substrate.mcp.framework.errors import (
    PreJoinSchemaError,
    PreJoinToolError,
)
from okg.substrate.mcp.framework.executor import execute_pre_join_tool
from okg.substrate.mcp.framework.resolvers import (
    ResolverRegistry,
    validate_resolver_refs,
)
from okg.substrate.mcp.framework.schema import (
    PreJoinToolSpec,
    PreJoinValidationFailure,
    load_pre_join_tools_from_path,
)
from okg.substrate.mcp.session import Session


__all__ = [
    "RegisteredPreJoinTool",
    "SkippedPreJoinTool",
    "build_pre_join_tools_for_manifest",
    "build_request_schema_for_tool",
    "make_pre_join_tool_callable",
]


@dataclass(frozen=True)
class RegisteredPreJoinTool:
    """One pre-join tool registered with FastMCP."""

    name: str
    description: str
    spec: PreJoinToolSpec
    request_schema: dict[str, Any]
    callable: Callable[..., Any]


@dataclass(frozen=True)
class SkippedPreJoinTool:
    """One pre-join tool omitted from registration.

    The substrate bootstrap logs the reason and registers the
    rest of the deployment's pre-join YAML (per S-A7-R1's
    isolation scenario).
    """

    name: Optional[str]
    code: str
    message: str
    data: dict[str, Any]


def build_pre_join_tools_for_manifest(
    manifest: DeploymentManifest,
    *,
    connect: Callable[[], psycopg.Connection],
    open_session: Callable[[psycopg.Connection], Session],
    resolvers: Optional[ResolverRegistry] = None,
) -> tuple[
    list[RegisteredPreJoinTool],
    list[SkippedPreJoinTool],
]:
    """Read the manifest's pre-join YAML + resolver block and
    return (registered, skipped). Caller (MCP server bootstrap)
    iterates `registered` and calls FastMCP's `add_tool` with
    each entry's callable + schema.

    `connect` is a zero-arg callable returning a psycopg
    connection for the deployment; `open_session` opens a fresh
    Session per call using the passed connection. The framework
    issues one connection-per-call (consistent with FastMCP
    tool-call pattern).
    """
    if resolvers is None:
        resolvers = ResolverRegistry.with_substrate_builtins()
    # Wire deployment-registered resolvers first so collisions
    # are reported at bootstrap.
    try:
        resolvers.register_from_manifest(manifest.pre_join_resolvers or {})
    except PreJoinSchemaError as e:
        return [], [
            SkippedPreJoinTool(
                name=None,
                code=e.code,
                message=str(e),
                data=dict(e.data or {}),
            )
        ]

    skipped: list[SkippedPreJoinTool] = []
    path = manifest.mcp_pre_join_tools
    if path is None:
        return [], skipped
    try:
        specs, schema_failures = load_pre_join_tools_from_path(path)
    except PreJoinSchemaError as e:
        return [], [
            SkippedPreJoinTool(
                name=None,
                code=e.code,
                message=str(e),
                data=dict(e.data or {}),
            )
        ]
    for f in schema_failures:
        skipped.append(_skipped_from_failure(f))

    # Filter out tools that reference unknown resolver slugs.
    specs, resolver_failures = validate_resolver_refs(specs, resolvers)
    for f in resolver_failures:
        skipped.append(_skipped_from_failure(f))

    registered: list[RegisteredPreJoinTool] = []
    for spec in specs:
        try:
            request_schema = build_request_schema_for_tool(spec)
            fn = make_pre_join_tool_callable(
                spec=spec,
                connect=connect,
                open_session=open_session,
                resolvers=resolvers,
            )
        except (PreJoinSchemaError, PreJoinToolError) as e:
            skipped.append(
                SkippedPreJoinTool(
                    name=spec.name,
                    code=e.code,
                    message=str(e),
                    data=dict(e.data or {}),
                )
            )
            continue
        registered.append(
            RegisteredPreJoinTool(
                name=spec.name,
                description=spec.description,
                spec=spec,
                request_schema=request_schema,
                callable=fn,
            )
        )
    return registered, skipped


def _skipped_from_failure(
    failure: PreJoinValidationFailure,
) -> SkippedPreJoinTool:
    err = failure.error
    return SkippedPreJoinTool(
        name=failure.tool_name,
        code=err.code,
        message=str(err),
        data=dict(err.data or {}),
    )


# ---- request schema generation ----


_KIND_TO_JSON_TYPE = {
    "text": "string",
    "node_id": "string",
    "alias": "string",
    "int": "integer",
    "bool": "boolean",
}


def build_request_schema_for_tool(spec: PreJoinToolSpec) -> dict[str, Any]:
    """Generate the FastMCP request schema for one pre-join tool.

    Returns a JSON-schema-shaped dict with `type=object`, a
    `properties` map keyed by param name, and a `required` list
    populated from each param's `required` flag (S-A7-R1).
    """
    properties: dict[str, Any] = {}
    required: list[str] = []
    for param in spec.params:
        json_type = _KIND_TO_JSON_TYPE.get(param.kind, "string")
        prop: dict[str, Any] = {"type": json_type}
        if param.description:
            prop["description"] = param.description
        properties[param.name] = prop
        if param.required:
            required.append(param.name)
    schema: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }
    if required:
        schema["required"] = required
    return schema


# ---- callable factory ----


def make_pre_join_tool_callable(
    *,
    spec: PreJoinToolSpec,
    connect: Callable[[], psycopg.Connection],
    open_session: Callable[[psycopg.Connection], Session],
    resolvers: ResolverRegistry,
) -> Callable[..., dict[str, Any]]:
    """Return a kwargs-only callable that FastMCP can register.

    The callable opens one connection + session per call, dispatches
    to the executor, and returns the envelope.
    """

    def _call(**kwargs: Any) -> dict[str, Any]:
        # Slice the kwargs down to declared params; anything else
        # is silently ignored. (FastMCP usually filters at the
        # JSON-schema layer; this is defensive.)
        declared = {p.name for p in spec.params}
        params = {k: v for k, v in kwargs.items() if k in declared}
        # Early validation of required params — keeps the cheap
        # missing-arg failure ahead of the connection-open cost
        # AND lets callers detect the failure without DB access.
        from okg.substrate.mcp.framework.executor import (
            _validate_required_params,
        )
        _validate_required_params(spec=spec, params=params)
        with connect() as conn:
            session = open_session(conn)
            return execute_pre_join_tool(
                spec,
                conn=conn,
                session=session,
                params=params,
                resolvers=resolvers,
            )

    _call.__name__ = spec.name
    _call.__doc__ = spec.description
    return _call


# ---- compatibility shim ----


def iter_skipped_log_entries(
    skipped: list[SkippedPreJoinTool],
) -> Iterator[tuple[str, str, str, dict[str, Any]]]:
    """Iterator over (level, code, message, data) tuples the
    substrate bootstrap path uses to log + telemetry-record each
    skipped tool. Kept here so the log shape stays consistent
    across deployments.
    """
    for entry in skipped:
        level = "error" if entry.name is None else "warning"
        yield level, entry.code, entry.message, dict(entry.data or {})
