"""Pre-join MCP tool framework.

Per `add-substrate-pre-join-mcp-framework`:

- `schema.py` — typed parser + validator for the
  `mcp_pre_join_tools.yaml` deployment-authored file.
- `resolvers.py` — substrate-shipped + deployment-registered
  resolver registry; turns text/alias params into node_ids.
- `executor.py` — runs one tool's `bundle:` ordered steps by
  calling existing substrate MCP read primitives.
- `generator.py` — at MCP server bootstrap, walks each
  deployment's pre-join YAML and registers one FastMCP-callable
  per validated tool.
- `errors.py` — typed errors used across the framework
  surfaces (parser → log; executor → tool response).
"""
from __future__ import annotations

from okg.substrate.mcp.framework.errors import (
    PreJoinResolverUnknown,
    PreJoinSchemaError,
    PreJoinStepKindUnknown,
    PreJoinSummaryTemplateError,
    PreJoinToolError,
    PreJoinTraverseHopsExceeded,
)
from okg.substrate.mcp.framework.executor import (
    execute_pre_join_tool,
    format_summary_template,
)
from okg.substrate.mcp.framework.generator import (
    RegisteredPreJoinTool,
    SkippedPreJoinTool,
    build_pre_join_tools_for_manifest,
    build_request_schema_for_tool,
    make_pre_join_tool_callable,
)
from okg.substrate.mcp.framework.resolvers import (
    Resolver,
    ResolverContext,
    ResolverRegistry,
    build_substrate_resolvers,
    validate_resolver_refs,
)
from okg.substrate.mcp.framework.schema import (
    PreJoinBundleStep,
    PreJoinParam,
    PreJoinToolSpec,
    SUPPORTED_PARAM_KINDS,
    SUPPORTED_STEP_KINDS,
    load_pre_join_tools_from_path,
    parse_pre_join_tools,
)


__all__ = [
    "PreJoinBundleStep",
    "PreJoinParam",
    "PreJoinResolverUnknown",
    "PreJoinSchemaError",
    "PreJoinStepKindUnknown",
    "PreJoinSummaryTemplateError",
    "PreJoinTraverseHopsExceeded",
    "PreJoinToolError",
    "PreJoinToolSpec",
    "RegisteredPreJoinTool",
    "SkippedPreJoinTool",
    "Resolver",
    "ResolverContext",
    "ResolverRegistry",
    "SUPPORTED_PARAM_KINDS",
    "SUPPORTED_STEP_KINDS",
    "build_pre_join_tools_for_manifest",
    "build_request_schema_for_tool",
    "build_substrate_resolvers",
    "execute_pre_join_tool",
    "format_summary_template",
    "load_pre_join_tools_from_path",
    "make_pre_join_tool_callable",
    "parse_pre_join_tools",
    "validate_resolver_refs",
]
