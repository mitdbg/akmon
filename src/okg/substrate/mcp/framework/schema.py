"""Typed parser + validator for `mcp_pre_join_tools.yaml`.

Per add-substrate-pre-join-mcp-framework / S-A7-R1:

YAML shape:

    tools:
      - name: <deployment_prefix>_<tool_name>
        description: <agent-visible doc string>
        params:
          - { name: <p>, kind: text|node_id|int|bool|alias, required: <bool> }
        bundle:
          - call: <substrate_primitive>
            args:
              start: { resolve: <slug>, input: <param_or_prior_emit> }
              path: [ "<edge_type>:<dir>", ... ]
              max_hops: <int 1..3>
              ...
            emit_as: <name>
            when: <optional_predicate>
        summary_template: |
          <Jinja-style template>

Step kinds (S-A7-R2 closed whitelist):

    traverse_path | get_node | list_neighbors | aggregate_neighbors
    | aggregate_by_type | search | find_identifier_mentions

Predicate language (S-A7-R3 task 1.4): only
    <param> != null
    <param> == "<string-literal>"
    <param> == <int-literal>
    <param> == true | false

are supported. Anything else is rejected at bootstrap.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml
from okg.substrate import fastyaml

from okg.substrate.mcp.framework.errors import (
    PreJoinSchemaError,
    PreJoinStepKindUnknown,
    PreJoinTraverseHopsExceeded,
)


__all__ = [
    "PreJoinBundleStep",
    "PreJoinParam",
    "PreJoinToolSpec",
    "PreJoinValidationFailure",
    "SUPPORTED_PARAM_KINDS",
    "SUPPORTED_STEP_KINDS",
    "MAX_TRAVERSE_HOPS",
    "load_pre_join_tools_from_path",
    "parse_pre_join_tools",
    "parse_predicate",
    "evaluate_predicate",
]


SUPPORTED_PARAM_KINDS = frozenset({"text", "node_id", "int", "bool", "alias"})

# Closed whitelist of substrate read primitives the framework wraps.
# Each entry maps the YAML `call:` value to its substrate
# primitive name; the executor uses the same key to dispatch.
#
# `aggregate_by_type` was retired by `retire-redundant-mcp-tools`
# before this child landed; the spec's earlier draft listed it in
# S-A7-R2 but the substrate no longer exports it. The whitelist
# below tracks the actual exported set. See finding
# `2026-05-18-aggregate-by-type-retired-upstream.md`.
SUPPORTED_STEP_KINDS = frozenset({
    "traverse_path",
    "get_node",
    "list_neighbors",
    "aggregate_neighbors",
    "search",
    "find_identifier_mentions",
})

MAX_TRAVERSE_HOPS = 3

_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_PREDICATE_NE_NULL = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*!=\s*null\s*$"
)
_PREDICATE_EQ_STR = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*==\s*\"([^\"]*)\"\s*$"
)
_PREDICATE_EQ_INT = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*==\s*(-?\d+)\s*$"
)
_PREDICATE_EQ_BOOL = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*==\s*(true|false)\s*$"
)


@dataclass(frozen=True)
class PreJoinParam:
    name: str
    kind: str
    required: bool = False
    description: str = ""


@dataclass(frozen=True)
class PreJoinBundleStep:
    call: str  # substrate primitive name (one of SUPPORTED_STEP_KINDS)
    args: dict[str, Any]
    emit_as: str
    when: Optional[str] = None  # predicate text (already validated)


@dataclass(frozen=True)
class PreJoinToolSpec:
    name: str
    description: str
    params: tuple[PreJoinParam, ...]
    bundle: tuple[PreJoinBundleStep, ...]
    summary_template: Optional[str] = None
    source_path: Optional[Path] = None  # where the spec was loaded from


@dataclass(frozen=True)
class PreJoinValidationFailure:
    """Captures one schema-validation failure when parsing a YAML
    file with multiple tools — lets the caller log + skip a
    malformed entry while still registering the rest."""

    tool_name: Optional[str]
    error: PreJoinSchemaError


# ---- predicate parser ----


@dataclass(frozen=True)
class _Predicate:
    """Internal AST for the simple when-predicate language."""

    kind: str  # "ne_null" | "eq_str" | "eq_int" | "eq_bool"
    param: str
    value: Optional[object] = None


def parse_predicate(text: str) -> _Predicate:
    """Validate a `when:` predicate. Raises PreJoinSchemaError on
    anything outside the supported grammar (task 1.4)."""
    s = (text or "").strip()
    if not s:
        raise PreJoinSchemaError("empty when: predicate")

    m = _PREDICATE_NE_NULL.match(s)
    if m:
        return _Predicate(kind="ne_null", param=m.group(1))
    m = _PREDICATE_EQ_STR.match(s)
    if m:
        return _Predicate(kind="eq_str", param=m.group(1), value=m.group(2))
    m = _PREDICATE_EQ_INT.match(s)
    if m:
        return _Predicate(
            kind="eq_int", param=m.group(1), value=int(m.group(2))
        )
    m = _PREDICATE_EQ_BOOL.match(s)
    if m:
        return _Predicate(
            kind="eq_bool", param=m.group(1), value=(m.group(2) == "true")
        )

    raise PreJoinSchemaError(
        f"unsupported when: predicate {text!r}; supported forms: "
        "`<param> != null`, `<param> == \"<literal>\"`, "
        "`<param> == <int>`, `<param> == true|false`",
        data={"predicate": text},
    )


def evaluate_predicate(text: str, params: dict[str, Any]) -> bool:
    """Evaluate a when-predicate against a call's parameter map.

    The substrate runs this at tool-invocation time to decide
    whether to execute the step. Unknown params evaluate to None
    (i.e. `<param> != null` is False).
    """
    pred = parse_predicate(text)
    actual = params.get(pred.param)
    if pred.kind == "ne_null":
        return actual is not None
    return actual == pred.value


# ---- top-level parser ----


def load_pre_join_tools_from_path(
    path: Path,
) -> tuple[list[PreJoinToolSpec], list[PreJoinValidationFailure]]:
    """Load + parse one deployment's `mcp_pre_join_tools.yaml`.

    Returns (validated_tool_specs, failures). The caller (the
    bootstrap MCP server) logs failures; validated specs proceed
    to registration. A whole-file load error (file unreadable,
    not a YAML mapping) raises PreJoinSchemaError directly.
    """
    if not path.is_file():
        return [], []
    try:
        raw = fastyaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as e:
        raise PreJoinSchemaError(
            f"could not parse pre-join YAML at {path}: {e}",
            data={"path": str(path)},
        ) from e
    if not isinstance(raw, dict):
        raise PreJoinSchemaError(
            f"pre-join YAML at {path} must be a top-level mapping",
            data={"path": str(path)},
        )
    return parse_pre_join_tools(raw, source_path=path)


def parse_pre_join_tools(
    raw: dict[str, Any],
    *,
    source_path: Optional[Path] = None,
) -> tuple[list[PreJoinToolSpec], list[PreJoinValidationFailure]]:
    """Parse + validate a pre-join YAML body.

    Schema-invalid tools are isolated: the caller can log them
    and continue registering the validated ones (S-A7-R1 second
    scenario).
    """
    specs: list[PreJoinToolSpec] = []
    failures: list[PreJoinValidationFailure] = []
    tools_raw = raw.get("tools")
    if tools_raw is None:
        return specs, failures
    if not isinstance(tools_raw, list):
        raise PreJoinSchemaError(
            "pre-join YAML top-level `tools:` must be a list",
            data={"observed": type(tools_raw).__name__},
        )
    seen_names: set[str] = set()
    for i, entry in enumerate(tools_raw):
        name = entry.get("name") if isinstance(entry, dict) else None
        try:
            spec = _parse_one_tool(
                entry, index=i, source_path=source_path,
            )
            if spec.name in seen_names:
                raise PreJoinSchemaError(
                    f"duplicate tool name {spec.name!r} in pre-join YAML",
                    data={"name": spec.name},
                )
            seen_names.add(spec.name)
            specs.append(spec)
        except PreJoinSchemaError as e:
            failures.append(
                PreJoinValidationFailure(
                    tool_name=(
                        str(name) if isinstance(name, str) else None
                    ),
                    error=e,
                )
            )
    return specs, failures


# ---- per-tool parser ----


def _parse_one_tool(
    raw: Any, *, index: int, source_path: Optional[Path]
) -> PreJoinToolSpec:
    if not isinstance(raw, dict):
        raise PreJoinSchemaError(
            f"tools[{index}] must be a mapping; got "
            f"{type(raw).__name__}",
            data={"index": index},
        )
    name = raw.get("name")
    if not isinstance(name, str) or not _NAME_RE.match(name):
        raise PreJoinSchemaError(
            f"tools[{index}]: invalid `name`; must match "
            f"[A-Za-z_][A-Za-z0-9_]*",
            data={"index": index, "name": name},
        )
    description = raw.get("description")
    if not isinstance(description, str) or not description.strip():
        raise PreJoinSchemaError(
            f"tool {name!r}: missing or empty `description`",
            data={"name": name},
        )

    params = _parse_params(raw.get("params") or [], tool_name=name)
    declared_param_names = {p.name for p in params}

    bundle_raw = raw.get("bundle") or []
    if not isinstance(bundle_raw, list) or not bundle_raw:
        raise PreJoinSchemaError(
            f"tool {name!r}: `bundle:` must be a non-empty list",
            data={"name": name},
        )
    bundle: list[PreJoinBundleStep] = []
    seen_emit_as: set[str] = set()
    for j, step_raw in enumerate(bundle_raw):
        step = _parse_one_step(
            step_raw,
            tool_name=name,
            index=j,
            params=declared_param_names,
            seen_emit_as=set(seen_emit_as),
        )
        if step.emit_as in seen_emit_as:
            raise PreJoinSchemaError(
                f"tool {name!r} bundle[{j}]: duplicate emit_as "
                f"{step.emit_as!r}",
                data={"name": name, "emit_as": step.emit_as},
            )
        seen_emit_as.add(step.emit_as)
        bundle.append(step)

    summary_template = raw.get("summary_template")
    if summary_template is not None and not isinstance(summary_template, str):
        raise PreJoinSchemaError(
            f"tool {name!r}: `summary_template` must be a string",
            data={"name": name},
        )

    return PreJoinToolSpec(
        name=name,
        description=description.strip(),
        params=tuple(params),
        bundle=tuple(bundle),
        summary_template=summary_template,
        source_path=source_path,
    )


def _parse_params(
    raw_params: Any, *, tool_name: str
) -> list[PreJoinParam]:
    if not isinstance(raw_params, list):
        raise PreJoinSchemaError(
            f"tool {tool_name!r}: `params:` must be a list",
            data={"name": tool_name},
        )
    out: list[PreJoinParam] = []
    seen: set[str] = set()
    for i, p in enumerate(raw_params):
        if not isinstance(p, dict):
            raise PreJoinSchemaError(
                f"tool {tool_name!r} params[{i}]: must be a mapping",
                data={"name": tool_name, "index": i},
            )
        name = p.get("name")
        if not isinstance(name, str) or not _NAME_RE.match(name):
            raise PreJoinSchemaError(
                f"tool {tool_name!r} params[{i}]: invalid `name`",
                data={"name": tool_name, "index": i, "param_name": name},
            )
        if name in seen:
            raise PreJoinSchemaError(
                f"tool {tool_name!r}: duplicate param {name!r}",
                data={"name": tool_name, "param_name": name},
            )
        seen.add(name)
        kind = p.get("kind")
        if kind not in SUPPORTED_PARAM_KINDS:
            raise PreJoinSchemaError(
                f"tool {tool_name!r} param {name!r}: unsupported "
                f"kind {kind!r}; allowed: {sorted(SUPPORTED_PARAM_KINDS)}",
                data={
                    "name": tool_name,
                    "param_name": name,
                    "kind": kind,
                },
            )
        required = bool(p.get("required", False))
        description = str(p.get("description") or "")
        out.append(
            PreJoinParam(
                name=name,
                kind=str(kind),
                required=required,
                description=description,
            )
        )
    return out


def _parse_one_step(
    raw: Any,
    *,
    tool_name: str,
    index: int,
    params: set[str],
    seen_emit_as: set[str],
) -> PreJoinBundleStep:
    if not isinstance(raw, dict):
        raise PreJoinSchemaError(
            f"tool {tool_name!r} bundle[{index}]: must be a mapping",
            data={"name": tool_name, "index": index},
        )
    call = raw.get("call")
    if call not in SUPPORTED_STEP_KINDS:
        # Distinct typed error for step-kind unknown so the
        # bootstrap log can spot configuration drift.
        raise PreJoinStepKindUnknown(
            f"tool {tool_name!r} bundle[{index}]: unknown step kind "
            f"{call!r}; allowed: {sorted(SUPPORTED_STEP_KINDS)}",
            data={
                "name": tool_name,
                "index": index,
                "call": call,
            },
        )
    args = raw.get("args") or {}
    if not isinstance(args, dict):
        raise PreJoinSchemaError(
            f"tool {tool_name!r} bundle[{index}]: `args` must be a "
            "mapping",
            data={"name": tool_name, "index": index},
        )

    # Per-call sanity checks for the most common shapes.
    _check_args_shape(
        call=str(call),
        args=args,
        tool_name=tool_name,
        index=index,
        params=params,
        seen_emit_as=seen_emit_as,
    )

    emit_as = raw.get("emit_as")
    if not isinstance(emit_as, str) or not _NAME_RE.match(emit_as):
        raise PreJoinSchemaError(
            f"tool {tool_name!r} bundle[{index}]: invalid `emit_as`",
            data={"name": tool_name, "index": index, "emit_as": emit_as},
        )

    when = raw.get("when")
    if when is not None:
        if not isinstance(when, str):
            raise PreJoinSchemaError(
                f"tool {tool_name!r} bundle[{index}]: `when` must be a "
                "string",
                data={"name": tool_name, "index": index},
            )
        # Parse to confirm grammar; the actual evaluation happens
        # at tool-call time.
        pred = parse_predicate(when)
        if pred.param not in params:
            raise PreJoinSchemaError(
                f"tool {tool_name!r} bundle[{index}]: `when` references "
                f"unknown param {pred.param!r}",
                data={
                    "name": tool_name,
                    "index": index,
                    "param": pred.param,
                },
            )

    return PreJoinBundleStep(
        call=str(call),
        args=args,
        emit_as=emit_as,
        when=when,
    )


def _check_args_shape(
    *,
    call: str,
    args: dict[str, Any],
    tool_name: str,
    index: int,
    params: set[str],
    seen_emit_as: set[str],
) -> None:
    """Spot-check the most common args shape, especially the
    `{ resolve | from }` resolver-ref rule (S-A7-R3) and the
    traverse_path hop ceiling. We don't try to fully constrain
    every primitive's args here — that surface lives on the
    substrate primitives themselves; this is the YAML-side guard
    against typos that would otherwise blow up at runtime.

    Both `start: { resolve | from }` (for traverse_path / etc.)
    and `node_id: { resolve | from }` (for get_node /
    list_neighbors) use the same shape, so we validate every
    nested mapping that carries either key.
    """
    for arg_key, value in args.items():
        if isinstance(value, dict) and (
            "resolve" in value or "from" in value
        ):
            _validate_resolver_ref(
                ref=value,
                arg_key=str(arg_key),
                tool_name=tool_name,
                index=index,
                params=params,
                seen_emit_as=seen_emit_as,
            )

    if call == "traverse_path":
        _validate_traverse_path_args(
            args=args, tool_name=tool_name, index=index,
        )


def _validate_traverse_path_args(
    *, args: dict[str, Any], tool_name: str, index: int,
) -> None:
    """Enforce the substrate's ≤3-hop ceiling at framework
    boundary (task 3.3)."""
    max_hops = args.get("max_hops", 1)
    try:
        mh = int(max_hops)
    except (TypeError, ValueError) as e:
        raise PreJoinSchemaError(
            f"tool {tool_name!r} bundle[{index}]: `max_hops` must "
            "be an integer",
            data={"name": tool_name, "index": index},
        ) from e
    if mh < 1 or mh > MAX_TRAVERSE_HOPS:
        raise PreJoinTraverseHopsExceeded(
            f"tool {tool_name!r} bundle[{index}]: `max_hops`={mh} "
            f"out of range [1, {MAX_TRAVERSE_HOPS}]",
            data={
                "name": tool_name, "index": index, "max_hops": mh,
            },
        )
    path = args.get("path")
    if not isinstance(path, list) or not path:
        raise PreJoinSchemaError(
            f"tool {tool_name!r} bundle[{index}]: traverse_path "
            "requires a non-empty `path` list",
            data={"name": tool_name, "index": index},
        )
    if len(path) > MAX_TRAVERSE_HOPS:
        raise PreJoinTraverseHopsExceeded(
            f"tool {tool_name!r} bundle[{index}]: `path` length "
            f"{len(path)} exceeds {MAX_TRAVERSE_HOPS}",
            data={
                "name": tool_name,
                "index": index,
                "path_len": len(path),
            },
        )
    for k, hop in enumerate(path):
        if not isinstance(hop, str) or ":" not in hop:
            raise PreJoinSchemaError(
                f"tool {tool_name!r} bundle[{index}] path[{k}]: "
                f"each hop must be `<edge_type>:<dir>`; got "
                f"{hop!r}",
                data={
                    "name": tool_name,
                    "index": index,
                    "hop": hop,
                },
            )


def _validate_resolver_ref(
    *,
    ref: dict[str, Any],
    arg_key: str,
    tool_name: str,
    index: int,
    params: set[str],
    seen_emit_as: set[str],
) -> None:
    """Validate a `{ resolve, input }` or `{ from }` reference.
    Two mutually-exclusive shapes (S-A7-R3)."""
    has_resolve = "resolve" in ref
    has_from = "from" in ref
    if has_resolve and has_from:
        raise PreJoinSchemaError(
            f"tool {tool_name!r} bundle[{index}]: `{arg_key}` may "
            "have at most one of `resolve` / `from`",
            data={"name": tool_name, "index": index, "arg": arg_key},
        )
    if has_resolve:
        slug = ref.get("resolve")
        if not isinstance(slug, str) or not slug:
            raise PreJoinSchemaError(
                f"tool {tool_name!r} bundle[{index}]: "
                f"`{arg_key}.resolve` must be a non-empty string",
                data={"name": tool_name, "index": index, "arg": arg_key},
            )
        inp = ref.get("input")
        if not isinstance(inp, str) or not inp:
            raise PreJoinSchemaError(
                f"tool {tool_name!r} bundle[{index}]: "
                f"`{arg_key}.input` must be a non-empty string",
                data={"name": tool_name, "index": index, "arg": arg_key},
            )
        if inp not in params and inp not in seen_emit_as:
            raise PreJoinSchemaError(
                f"tool {tool_name!r} bundle[{index}]: "
                f"`{arg_key}.input` {inp!r} is neither a declared "
                "param nor a prior emit_as",
                data={
                    "name": tool_name,
                    "index": index,
                    "arg": arg_key,
                    "input": inp,
                },
            )
    if has_from:
        target = ref.get("from")
        if not isinstance(target, str) or not target:
            raise PreJoinSchemaError(
                f"tool {tool_name!r} bundle[{index}]: "
                f"`{arg_key}.from` must be a non-empty string",
                data={"name": tool_name, "index": index, "arg": arg_key},
            )
        if target not in seen_emit_as:
            raise PreJoinSchemaError(
                f"tool {tool_name!r} bundle[{index}]: "
                f"`{arg_key}.from` {target!r} is not a prior emit_as",
                data={
                    "name": tool_name,
                    "index": index,
                    "arg": arg_key,
                    "from": target,
                },
            )
