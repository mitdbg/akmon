"""Typed errors raised by the pre-join MCP tool framework.

All errors inherit from `SubstrateError` so they render in the
standard JSON error envelope. The codes appear in operator logs
when a deployment ships malformed YAML; the typed errors also
flow up the MCP layer when a tool fails at runtime.
"""
from __future__ import annotations

from okg.substrate.errors import SubstrateError


__all__ = [
    "PreJoinSchemaError",
    "PreJoinStepKindUnknown",
    "PreJoinTraverseHopsExceeded",
    "PreJoinResolverUnknown",
    "PreJoinSummaryTemplateError",
    "PreJoinToolError",
]


class PreJoinSchemaError(SubstrateError):
    """The pre-join YAML failed schema validation. Surfaced at
    bootstrap; the malformed tool is omitted from registration so
    other tools in the same file still register."""

    code = "pre_join_tool_schema_error"


class PreJoinStepKindUnknown(PreJoinSchemaError):
    """A bundle step names a `call:` that isn't in the supported
    whitelist of substrate read primitives.

    Inherits from PreJoinSchemaError so the schema-validation
    pipeline isolates step-kind failures alongside other schema
    bugs; the wire code stays specific so operators can grep for
    it."""

    code = "pre_join_tool_step_kind_unknown"


class PreJoinTraverseHopsExceeded(PreJoinSchemaError):
    """A `traverse_path` bundle step requests `max_hops > 3`.
    The existing substrate ceiling is preserved at framework
    boundary.

    Inherits from PreJoinSchemaError so the schema-validation
    pipeline isolates hop-ceiling failures alongside other schema
    bugs."""

    code = "pre_join_tool_traverse_hops_exceeded"


class PreJoinResolverUnknown(PreJoinSchemaError):
    """A bundle step references a resolver slug that isn't
    substrate-shipped and isn't registered by the deployment.
    The substrate refuses to register the tool; other tools in
    the same file still register.

    Inherits from PreJoinSchemaError so the registration pipeline
    isolates resolver-unknown failures alongside other schema bugs.
    """

    code = "pre_join_tool_resolver_unknown"


class PreJoinSummaryTemplateError(SubstrateError):
    """A `summary_template` failed to render at tool-call time.
    The tool still returns the bundle; the `summary` field on
    the response carries the literal template string + an
    operator-visible warning."""

    code = "pre_join_tool_summary_template_error"


class PreJoinToolError(SubstrateError):
    """Runtime error inside the pre-join framework that isn't a
    schema problem (e.g. resolver returned None when a step
    required a non-null start)."""

    code = "pre_join_tool_error"
