"""Served text as per-deployment, branch-able config (issue #4280).

A deployment's `agent_surface.surface_text` names one YAML file that changes
what the MCP server says to an agent, without code:

```yaml
tools:            # a tool's whole published description
  search: "..."
params:           # one argument's published description, as <tool>.<arg>
  search.method: "..."
fix_hints:        # a refusal's fix_hint, by its error code
  search_completeness_refused: "{default}; or drop completeness"
instructions: instructions-v2.md   # replaces agent_surface.instructions
branches:         # per-branch layers over the base, same keys minus branches
  a4-trial-1:
    fix_hints:
      node_not_found: "copy the id exactly as printed"
```

`okg mcp-serve --branch <name>` serves the base with that branch's layer on
top (the layer wins per id); the default branch serves the base alone. With no
file, every text is the code's own default.

A `fix_hints` template may use only the placeholder `{default}` (the code's
own hint for that refusal); an empty string drops the hint. Unknown keys,
unknown error codes, tools the server does not register, arguments a tool does
not publish, and other placeholders are refused, never ignored: a text that
cannot change anything must not load as if it did.

`surface_text_report` is what a client records: one digest over everything
the agent reads that is not data (the instructions, every published tool
description and input schema, the effective fix_hint map), plus which ids
were overridden and which branch layer applied.
"""
from __future__ import annotations

import hashlib
import json
import string
from collections.abc import Iterable, Mapping
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from okg.substrate.errors import SubstrateError

SCHEMA = "okg.mcp.surface_text.v1"
#: Longest served fix_hint text (the error payload cuts a hint at 1,024 too).
MAX_FIX_HINT_CHARS = 1_024
#: Longest tool or argument description.
MAX_DESCRIPTION_CHARS = 16_384

_LAYER_KEYS = frozenset({"tools", "params", "fix_hints", "instructions"})
_TOP_KEYS = _LAYER_KEYS | {"branches"}
_HINT_PLACEHOLDERS = frozenset({"default"})

#: Read refusals built as plain dictionaries, with no SubstrateError class
#: whose `code` the class walk in `known_error_codes` could find.
DICT_BUILT_CODES = frozenset({
    "invalid_response_byte_budget",     # response_bounds
    "response_byte_budget_exceeded",    # response_bounds
    "operator_execution_failed",        # response_bounds, an untyped raise
    "response_bound_failed",            # search
    "next_with_read_arguments",         # response_bounds
    "rows_are_query_spans",             # search
})


class SurfaceTextInvalid(SubstrateError):
    """The surface_text file cannot be served as written."""

    code = "surface_text_invalid"


@dataclass(frozen=True)
class SurfaceTextLayer:
    """One layer: the base, or one branch's overrides."""

    tools: Mapping[str, str] = field(default_factory=dict)
    params: Mapping[str, str] = field(default_factory=dict)
    fix_hints: Mapping[str, str] = field(default_factory=dict)
    instructions: str | None = None


@dataclass(frozen=True)
class SurfaceTextFile:
    """A parsed surface_text file: the base layer and its branch layers."""

    base: SurfaceTextLayer
    branches: Mapping[str, SurfaceTextLayer] = field(default_factory=dict)


@dataclass(frozen=True)
class EffectiveSurfaceText:
    """What one launch serves: the base merged with its branch's layer."""

    tools: Mapping[str, str] = field(default_factory=dict)
    params: Mapping[str, str] = field(default_factory=dict)
    fix_hints: Mapping[str, str] = field(default_factory=dict)
    #: Instructions file, relative to the deployment directory, or None.
    instructions: str | None = None
    #: The branch whose layer applied, or None (base only or no file).
    branch_layer: str | None = None
    #: False when the deployment declares no surface_text file.
    declared: bool = False

    def overridden(self) -> dict[str, list[str]]:
        return {
            "tools": sorted(self.tools),
            "params": sorted(self.params),
            "fix_hints": sorted(self.fix_hints),
            "instructions": [self.instructions] if self.instructions else [],
        }


NO_SURFACE_TEXT = EffectiveSurfaceText()


def _refuse(message: str, *, where: str, **data: Any) -> SurfaceTextInvalid:
    return SurfaceTextInvalid(
        f"{message} ({where})",
        data={"file": where, **data},
        fix_hint="fix the surface_text file; see okg.substrate.mcp.surface_text",
    )


def _text_map(raw: Any, *, label: str, where: str, limit: int) -> dict[str, str]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise _refuse(f"`{label}` must be a mapping of id to text", where=where, key=label)
    out: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not key:
            raise _refuse(f"`{label}` has a non-text key {key!r}", where=where, key=label)
        if not isinstance(value, str):
            raise _refuse(f"`{label}.{key}` must be text", where=where, key=f"{label}.{key}")
        if len(value) > limit:
            raise _refuse(
                f"`{label}.{key}` is {len(value)} characters; the limit is {limit}",
                where=where, key=f"{label}.{key}",
            )
        out[key] = value
    return out


def _check_hint_template(code: str, text: str, *, where: str) -> None:
    try:
        names = {name for _, name, _, _ in string.Formatter().parse(text) if name is not None}
    except ValueError as exc:
        raise _refuse(
            f"`fix_hints.{code}` is not a valid template: {exc}", where=where,
            key=f"fix_hints.{code}",
        ) from exc
    extra = sorted(names - _HINT_PLACEHOLDERS)
    if extra:
        raise _refuse(
            f"`fix_hints.{code}` uses placeholder(s) {extra}; only {{default}} is allowed",
            where=where, key=f"fix_hints.{code}", placeholders=extra,
        )


def _layer(raw: Any, *, label: str, where: str, allowed: frozenset[str]) -> SurfaceTextLayer:
    if not isinstance(raw, dict):
        raise _refuse(f"`{label}` must be a mapping", where=where, key=label)
    unknown = sorted(str(k) for k in raw if k not in allowed)
    if unknown:
        raise _refuse(
            f"`{label}` has unknown key(s) {unknown}; allowed: {sorted(allowed)}",
            where=where, key=label, unknown=unknown,
        )
    prefix = "" if label == "surface_text" else f"{label}."
    fix_hints = _text_map(
        raw.get("fix_hints"), label=f"{prefix}fix_hints", where=where, limit=MAX_FIX_HINT_CHARS,
    )
    for code, text in fix_hints.items():
        _check_hint_template(code, text, where=where)
    params = _text_map(
        raw.get("params"), label=f"{prefix}params", where=where, limit=MAX_DESCRIPTION_CHARS,
    )
    for key in params:
        tool, dot, arg = key.partition(".")
        if not dot or not tool or not arg:
            raise _refuse(
                f"`{prefix}params.{key}` must be named <tool>.<argument>", where=where,
                key=f"{prefix}params.{key}",
            )
    instructions = raw.get("instructions")
    if instructions is not None and (not isinstance(instructions, str) or not instructions):
        raise _refuse(f"`{prefix}instructions` must be a file path", where=where)
    return SurfaceTextLayer(
        tools=_text_map(
            raw.get("tools"), label=f"{prefix}tools", where=where, limit=MAX_DESCRIPTION_CHARS,
        ),
        params=params,
        fix_hints=fix_hints,
        instructions=instructions,
    )


def parse_surface_text(raw: Any, *, where: str) -> SurfaceTextFile:
    """Validate a loaded surface_text document (strict at every level)."""
    if raw is None:
        raw = {}
    base = _layer(raw, label="surface_text", where=where, allowed=_TOP_KEYS)
    raw_branches = raw.get("branches") or {}
    if not isinstance(raw_branches, dict):
        raise _refuse("`branches` must be a mapping of branch name to layer", where=where)
    branches: dict[str, SurfaceTextLayer] = {}
    for name, body in raw_branches.items():
        if not isinstance(name, str) or not name:
            raise _refuse(f"branch name {name!r} must be text", where=where)
        if name == "default":
            raise _refuse(
                "`branches.default` is not allowed: the base layer is the default branch",
                where=where,
            )
        branches[name] = _layer(body, label=f"branches.{name}", where=where, allowed=_LAYER_KEYS)
    return SurfaceTextFile(base=base, branches=branches)


def load_surface_text(path: Path) -> SurfaceTextFile:
    """Read and validate one surface_text file."""
    from okg.substrate import fastyaml

    where = str(path)
    try:
        raw = fastyaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 - re-raised as this file's own refusal.
        raise _refuse(f"cannot parse YAML: {type(exc).__name__}: {exc}", where=where) from exc
    return parse_surface_text(raw, where=where)


def effective_surface_text(parsed: SurfaceTextFile | None, branch: str) -> EffectiveSurfaceText:
    """The base merged with `branch`'s layer (the layer wins per id).

    A branch with no layer serves the base. That is not a fallback: a branch
    created from default serves default's text until a layer says otherwise,
    the same way it reads default's records until it publishes its own.
    """
    if parsed is None:
        return NO_SURFACE_TEXT
    layer = parsed.branches.get(branch) if branch and branch != "default" else None
    base = parsed.base
    if layer is None:
        return EffectiveSurfaceText(
            tools=dict(base.tools), params=dict(base.params),
            fix_hints=dict(base.fix_hints), instructions=base.instructions,
            branch_layer=None, declared=True,
        )
    return EffectiveSurfaceText(
        tools={**base.tools, **layer.tools},
        params={**base.params, **layer.params},
        fix_hints={**base.fix_hints, **layer.fix_hints},
        instructions=layer.instructions or base.instructions,
        branch_layer=branch,
        declared=True,
    )


def known_error_codes() -> frozenset[str]:
    """Every refusal code this server can serve that a fix_hint may name.

    The `code` of every loaded SubstrateError subclass, plus the read
    refusals built as plain dictionaries (`DICT_BUILT_CODES`), which have no
    class to find. Checked at the end of server build, after registration has
    imported the modules that declare them.
    """
    seen: set[str] = set(DICT_BUILT_CODES)
    stack: list[type] = [SubstrateError]
    visited: set[type] = set()
    while stack:
        cls = stack.pop()
        if cls in visited:
            continue
        visited.add(cls)
        code = cls.__dict__.get("code")
        if isinstance(code, str) and code:
            seen.add(code)
        stack.extend(cls.__subclasses__())
    return frozenset(seen)


def check_fix_hint_codes(
    effective: EffectiveSurfaceText, *, known: Iterable[str] | None = None,
) -> None:
    """Refuse a fix_hints key that no refusal of this server carries."""
    codes = frozenset(known) if known is not None else known_error_codes()
    unknown = sorted(code for code in effective.fix_hints if code not in codes)
    if unknown:
        raise SurfaceTextInvalid(
            f"surface_text fix_hints name error code(s) {unknown} that no refusal "
            "of this server carries",
            data={"unknown_codes": unknown},
            fix_hint="use an error code exactly as a refusal prints it",
        )


def check_tool_text(mcp: Any, effective: EffectiveSurfaceText) -> None:
    """Refuse tool or argument names this server does not publish."""
    tools = getattr(getattr(mcp, "_tool_manager", None), "_tools", None) or {}
    missing = sorted(name for name in effective.tools if name not in tools)
    for key in effective.params:
        tool_name, _, arg = key.partition(".")
        tool = tools.get(tool_name)
        props = (getattr(tool, "parameters", None) or {}).get("properties") if tool else None
        if not isinstance(props, dict) or not isinstance(props.get(arg), dict):
            missing.append(key)
    if missing:
        raise SurfaceTextInvalid(
            f"surface_text names tool(s) or argument(s) {sorted(missing)} this "
            f"server does not publish; it publishes {sorted(tools)}",
            data={"unpublished": sorted(missing), "published_tools": sorted(tools)},
            fix_hint="name a published tool, or <tool>.<argument> from its input schema",
        )


def apply_tool_text(mcp: Any, effective: EffectiveSurfaceText) -> None:
    """Replace published tool and argument descriptions; refuse unknown names.

    Runs last, after every pass that writes descriptions, on the published
    copy only: argument validation stays on the models.
    """
    check_tool_text(mcp, effective)
    tools = mcp._tool_manager._tools
    for name, text in effective.tools.items():
        tools[name].description = text
    for key, text in effective.params.items():
        tool_name, _, arg = key.partition(".")
        tools[tool_name].parameters["properties"][arg]["description"] = text


def all_layers(parsed: SurfaceTextFile | None) -> EffectiveSurfaceText:
    """Every id any layer names, for checking all branches at every launch."""
    if parsed is None:
        return NO_SURFACE_TEXT
    layers = [parsed.base, *parsed.branches.values()]
    return EffectiveSurfaceText(
        tools={k: v for layer in layers for k, v in layer.tools.items()},
        params={k: v for layer in layers for k, v in layer.params.items()},
        fix_hints={k: v for layer in layers for k, v in layer.fix_hints.items()},
        declared=True,
    )


def _inside(root: Path, value: str, *, what: str) -> Path:
    """`root / value`, refused unless it stays inside `root` and is a file."""
    base = root.resolve()
    path = (base / value).resolve()
    if not path.is_relative_to(base):
        raise SurfaceTextInvalid(
            f"{what} {value!r} resolves outside the deployment directory",
            data={"file": value},
            fix_hint="name a file inside the deployment directory",
        )
    if not path.is_file():
        raise SurfaceTextInvalid(
            f"{what} {value!r} does not exist in the deployment directory",
            data={"file": value},
            fix_hint="add the file, or remove the key",
        )
    return path


@dataclass(frozen=True)
class LaunchSurfaceText:
    """What `resolve_for_launch` read: the file, this branch's view, its instructions."""

    parsed: SurfaceTextFile | None
    effective: EffectiveSurfaceText
    #: The layer's instructions file text, or None when no layer names one.
    instructions_text: str | None = None


def resolve_for_launch(
    agent_surface: Any, deployment_dir: Path | str | None, branch: str,
) -> LaunchSurfaceText:
    """Load a deployment's surface_text for `branch` (server build and doctor).

    Paths resolve against the deployment directory, as
    `agent_surface.instructions` does, and must stay inside it. Every
    layer's instructions file must exist, not only the served branch's, so a
    mistake in another branch's layer refuses every launch.
    """
    name = getattr(agent_surface, "surface_text", None) if agent_surface is not None else None
    if not name or deployment_dir is None:
        return LaunchSurfaceText(None, NO_SURFACE_TEXT)
    root = Path(deployment_dir)
    parsed = load_surface_text(_inside(root, name, what="agent_surface.surface_text"))
    for label, layer in (("base", parsed.base), *(
        (f"branches.{b}", layer) for b, layer in parsed.branches.items()
    )):
        if layer.instructions:
            _inside(root, layer.instructions, what=f"surface_text {label} instructions")
    effective = effective_surface_text(parsed, branch)
    text = None
    if effective.instructions:
        text = _inside(root, effective.instructions, what="surface_text instructions").read_text(
            encoding="utf-8",
        )
    return LaunchSurfaceText(parsed, effective, text)


#: The surface text of the server serving the current call. Set for the
#: whole call by `ByteBoundedFastMCP`, so refusals built outside the server
#: object (`run`'s own) apply the same overrides.
CURRENT: ContextVar[EffectiveSurfaceText | None] = ContextVar("okg_surface_text", default=None)


def apply_current_fix_hint(payload: Any) -> Any:
    """`apply_fix_hint` with the current call's surface text."""
    return apply_fix_hint(payload, CURRENT.get())


def render_fix_hint(template: str, default: str | None) -> str:
    return template.format_map({"default": default or ""})


def apply_fix_hint(payload: Any, effective: EffectiveSurfaceText | None) -> Any:
    """The payload with its refusal's fix_hint replaced when one is declared.

    Returns a new dictionary; the caller's payload is not mutated. A payload
    with no error code, or a code with no override, is returned as is.
    """
    if effective is None or not effective.fix_hints or not isinstance(payload, dict):
        return payload
    error = payload.get("error")
    if not isinstance(error, dict):
        return payload
    code = error.get("code")
    if not isinstance(code, str) or code not in effective.fix_hints:
        return payload
    default = error.get("fix_hint") if isinstance(error.get("fix_hint"), str) else None
    text = render_fix_hint(effective.fix_hints[code], default)[:MAX_FIX_HINT_CHARS]
    new_error = dict(error)
    if text:
        new_error["fix_hint"] = text
    else:
        new_error.pop("fix_hint", None)
    return {**payload, "error": new_error}


def _published_tools(mcp: Any) -> list[dict[str, Any]]:
    tools = getattr(getattr(mcp, "_tool_manager", None), "_tools", None) or {}
    return [
        {
            "name": name,
            "description": getattr(tool, "description", None),
            "input_schema": getattr(tool, "parameters", None),
        }
        for name, tool in sorted(tools.items())
    ]


def surface_text_report(
    mcp: Any, *, instructions: str, effective: EffectiveSurfaceText,
    instructions_applied: bool = True,
) -> dict[str, Any]:
    """The digest and ids a client records for this launch's served text.

    `instructions_applied` is False when the caller passed its own
    instructions, which then win over a layer's file: the report says so
    rather than listing an override that was not served.
    """
    body = {
        "instructions": instructions,
        "tools": _published_tools(mcp),
        "fix_hints": dict(sorted(effective.fix_hints.items())),
    }
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    return {
        "schema": SCHEMA,
        "digest": "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        "declared": effective.declared,
        "branch_layer": effective.branch_layer,
        "overridden": {
            **effective.overridden(),
            "instructions": (
                effective.overridden()["instructions"] if instructions_applied else []
            ),
        },
        "instructions_applied": bool(effective.instructions) and instructions_applied,
    }


__all__ = [
    "CURRENT",
    "DICT_BUILT_CODES",
    "EffectiveSurfaceText",
    "LaunchSurfaceText",
    "MAX_DESCRIPTION_CHARS",
    "MAX_FIX_HINT_CHARS",
    "NO_SURFACE_TEXT",
    "SCHEMA",
    "SurfaceTextFile",
    "SurfaceTextInvalid",
    "SurfaceTextLayer",
    "all_layers",
    "apply_current_fix_hint",
    "apply_fix_hint",
    "apply_tool_text",
    "check_fix_hint_codes",
    "check_tool_text",
    "effective_surface_text",
    "known_error_codes",
    "load_surface_text",
    "parse_surface_text",
    "render_fix_hint",
    "resolve_for_launch",
    "surface_text_report",
]
