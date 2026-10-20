"""Pre-join MCP framework — runtime executor.

Per S-A7-R1 / R2 / R3: takes a validated `PreJoinToolSpec` +
caller-supplied params, dispatches each bundle step to its
substrate read primitive, and returns the framework's standard
response envelope:

    {
        "summary": <str>,
        "generation_id": <int>,
        "bundles": { <emit_as>: <array_or_obj>, ... },
    }

The executor doesn't talk to FastMCP directly — `generator.py`
wraps it in a callable that FastMCP registers. Keeping the
executor I/O-shaped (conn + session + params -> dict) makes it
testable without an MCP server.
"""
from __future__ import annotations

import logging
import string
from typing import Any, Optional

import psycopg

from okg.substrate.mcp.framework.errors import (
    PreJoinSummaryTemplateError,
    PreJoinToolError,
)
from okg.substrate.mcp.framework.resolvers import (
    ResolverContext,
    ResolverRegistry,
)
from okg.substrate.mcp.framework.schema import (
    PreJoinBundleStep,
    PreJoinToolSpec,
    evaluate_predicate,
)
from okg.substrate.mcp.session import Session


logger = logging.getLogger(__name__)


__all__ = [
    "execute_pre_join_tool",
    "format_summary_template",
]


def execute_pre_join_tool(
    spec: PreJoinToolSpec,
    *,
    conn: psycopg.Connection,
    session: Session,
    params: dict[str, Any],
    resolvers: ResolverRegistry,
) -> dict[str, Any]:
    """Run one declared tool's bundle and return the envelope.

    `params` is the caller's input keyed by declared param name.
    Required-param validation happens here (the schema parser
    only validates names + kinds at registration time).
    """
    _validate_required_params(spec=spec, params=params)
    bundles: dict[str, Any] = {}
    ctx = ResolverContext(
        conn=conn,
        session=session,
        catalog_version_id=session.catalog_version_id,
    )

    for i, step in enumerate(spec.bundle):
        if step.when is not None and not evaluate_predicate(step.when, params):
            # Predicate gates the step out; emit_as not populated.
            continue
        result = _execute_step(
            step=step,
            tool_name=spec.name,
            index=i,
            conn=conn,
            session=session,
            params=params,
            bundles=bundles,
            ctx=ctx,
            resolvers=resolvers,
        )
        bundles[step.emit_as] = result

    summary = format_summary_template(
        template=spec.summary_template, params=params, bundles=bundles,
    )
    return {
        "summary": summary,
        "generation_id": session.generation_id,
        "bundles": bundles,
    }


def _validate_required_params(
    *, spec: PreJoinToolSpec, params: dict[str, Any]
) -> None:
    missing = [
        p.name for p in spec.params
        if p.required and params.get(p.name) in (None, "")
    ]
    if missing:
        raise PreJoinToolError(
            f"tool {spec.name!r} missing required param(s): "
            f"{', '.join(missing)}",
            data={"name": spec.name, "missing": missing},
        )


def _execute_step(
    *,
    step: PreJoinBundleStep,
    tool_name: str,
    index: int,
    conn: psycopg.Connection,
    session: Session,
    params: dict[str, Any],
    bundles: dict[str, Any],
    ctx: ResolverContext,
    resolvers: ResolverRegistry,
) -> Any:
    """Dispatch a single bundle step to its named substrate primitive."""
    call = step.call
    args = _resolve_args(
        args=step.args,
        tool_name=tool_name,
        index=index,
        params=params,
        bundles=bundles,
        ctx=ctx,
        resolvers=resolvers,
    )
    # Lazy imports keep the framework module light + avoid cycles.
    if call == "get_node":
        from okg.substrate.mcp.tools import inspect_node as _get_node
        node_id = args.get("node_id")
        if not isinstance(node_id, str):
            raise PreJoinToolError(
                f"tool {tool_name!r} bundle[{index}]: get_node "
                "resolved `node_id` is not a string",
                data={"name": tool_name, "index": index},
            )
        select = args.get("select")
        return _get_node(conn, session, node_id, select=select)
    if call == "list_neighbors":
        from okg.substrate.mcp.tools import expand_neighbors as _ln
        start_node = args.get("start") or args.get("node_id")
        if not isinstance(start_node, str):
            raise PreJoinToolError(
                f"tool {tool_name!r} bundle[{index}]: list_neighbors "
                "needs `start` or `node_id` resolved to a string",
                data={"name": tool_name, "index": index},
            )
        return _ln(
            conn,
            session,
            start_node,
            edge_type=args.get("edge_type"),
            direction=args.get("direction", "out"),
            limit=args.get("limit", 100),
        )
    if call == "aggregate_neighbors":
        from okg.substrate.mcp.tools import (
            aggregate_node as _agg,
        )
        start_node = args.get("start") or args.get("node_id")
        if not isinstance(start_node, str):
            raise PreJoinToolError(
                f"tool {tool_name!r} bundle[{index}]: "
                "aggregate_neighbors needs `start` or `node_id` "
                "resolved to a string",
                data={"name": tool_name, "index": index},
            )
        return _agg(
            conn,
            session,
            start_node,
            select=args.get("select"),
        )
    if call == "traverse_path":
        from okg.substrate.mcp.tools import expand_path as _tp, parse_path_hops
        start_node = args.get("start")
        if not isinstance(start_node, str):
            raise PreJoinToolError(
                f"tool {tool_name!r} bundle[{index}]: traverse_path "
                "needs `start` resolved to a string",
                data={"name": tool_name, "index": index},
            )
        path = args.get("path") or []
        return _tp(
            conn,
            session,
            start_node,
            hops=parse_path_hops(path),
            target_subtype=args.get("target_subtype"),
            limit=args.get("limit", 100),
        )
    if call == "search":
        from okg.substrate.mcp.search import search as _search
        subtype = args.get("subtype")
        filters = args.get("filter") or {}
        if subtype is not None:
            filters = {**filters, "subtype": subtype}
        page = _search(
            conn,
            session,
            method=args.get("method", "lexical"),
            query=args.get("query") or args.get("q"),
            filter=filters,
            limit=args.get("limit", 50),
            select=args.get("select"),
            # Pinned: pre-join bundles were written against row-level pages,
            # before lexical search defaulted to `records` (PACT
            # search-result-shape; lead call).
            rank_by="self",
        )
        # A bundle step has no `completeness` argument, so its caller cannot
        # take the bounded page's "rerun with completeness=exact" advice: the
        # page stays qualified as bounded but names no recovery it cannot
        # honour (PACT lexical-index-scan-cap-3162).
        retrieval = (page.get("coverage") or {}).get("retrieval")
        if isinstance(retrieval, dict):
            retrieval.pop("recovery", None)
        return page
    if call == "find_identifier_mentions":
        from okg.substrate.mcp.tools import (
            find_identifier_mentions as _fim,
        )
        return _fim(
            conn,
            session,
            text=args.get("text") or args.get("query"),
            select=args.get("select"),
        )
    raise PreJoinToolError(
        f"tool {tool_name!r} bundle[{index}]: executor has no "
        f"dispatch for call {call!r}",
        data={"name": tool_name, "index": index, "call": call},
    )


def _resolve_args(
    *,
    args: dict[str, Any],
    tool_name: str,
    index: int,
    params: dict[str, Any],
    bundles: dict[str, Any],
    ctx: ResolverContext,
    resolvers: ResolverRegistry,
) -> dict[str, Any]:
    """Walk the step's args; substitute any
    `{resolve, input}` or `{from}` reference with a concrete
    value via the resolver registry (S-A7-R3) or prior emit.
    Returns a fresh dict with concrete values; nested
    non-resolver mappings pass through unchanged.
    """
    out: dict[str, Any] = {}
    for key, value in args.items():
        if isinstance(value, dict) and (
            "resolve" in value or "from" in value
        ):
            out[key] = _resolve_reference(
                ref=value,
                arg_key=str(key),
                tool_name=tool_name,
                index=index,
                params=params,
                bundles=bundles,
                ctx=ctx,
                resolvers=resolvers,
            )
        else:
            out[key] = value
    return out


def _resolve_reference(
    *,
    ref: dict[str, Any],
    arg_key: str,
    tool_name: str,
    index: int,
    params: dict[str, Any],
    bundles: dict[str, Any],
    ctx: ResolverContext,
    resolvers: ResolverRegistry,
) -> Optional[str]:
    if "resolve" in ref:
        slug = str(ref["resolve"])
        input_key = str(ref["input"])
        # Prefer prior-emit-derived node_id when the input name
        # matches a prior emit_as; falls back to param value.
        candidate = _candidate_from_input_key(
            input_key=input_key, params=params, bundles=bundles,
        )
        if candidate in (None, ""):
            return None
        resolver = resolvers.get(slug)
        if resolver is None:
            # Defensive: a resolver-unknown spec should have been
            # rejected at registration. Surface a clear runtime
            # error if we got here anyway.
            raise PreJoinToolError(
                f"tool {tool_name!r} bundle[{index}]: resolver "
                f"{slug!r} is not in the registry at call time",
                data={"name": tool_name, "index": index, "slug": slug},
            )
        return resolver(candidate, ctx)
    if "from" in ref:
        target = str(ref["from"])
        emit = bundles.get(target)
        return _first_node_id(emit)
    return None


def _candidate_from_input_key(
    *,
    input_key: str,
    params: dict[str, Any],
    bundles: dict[str, Any],
) -> Optional[str]:
    """`input:` may reference a declared param or a prior emit.
    Params resolve to their string value; prior emits resolve to
    the first item's node_id (mirrors `from:` semantics).
    """
    if input_key in params:
        value = params[input_key]
        return value if isinstance(value, str) else None
    if input_key in bundles:
        return _first_node_id(bundles[input_key])
    return None


def _first_node_id(emit: Any) -> Optional[str]:
    """Extract the first item's node_id from a prior emit."""
    if isinstance(emit, dict):
        nid = emit.get("node_id")
        return nid if isinstance(nid, str) else None
    if isinstance(emit, list) and emit:
        head = emit[0]
        if isinstance(head, dict):
            nid = head.get("node_id")
            return nid if isinstance(nid, str) else None
    return None


# ---- summary template formatter ----


class _DotAccessFormatter(string.Formatter):
    """Jinja-style dotted-access formatter for the summary
    template. Supports `{key}`, `{section.attr}`, `{emit.count}`,
    and `{emit.0.node_id}` (first item's field) — enough to render
    the brief's example template without dragging in Jinja2.
    """

    _COUNT_KEY = "count"

    def get_field(self, field_name: str, args, kwargs):
        first, *rest = field_name.split(".")
        if first not in kwargs:
            raise KeyError(first)
        value: Any = kwargs[first]
        used = first
        for part in rest:
            value, used = self._step(value, part, used)
        return value, used

    def _step(self, value: Any, part: str, used: str) -> tuple[Any, str]:
        new_used = f"{used}.{part}"
        if part == self._COUNT_KEY:
            if isinstance(value, (list, tuple)):
                return len(value), new_used
            if isinstance(value, dict):
                return 1, new_used
            return 0, new_used
        if isinstance(value, list):
            try:
                idx = int(part)
            except ValueError as e:
                raise KeyError(new_used) from e
            try:
                return value[idx], new_used
            except IndexError as e:
                raise KeyError(new_used) from e
        if isinstance(value, dict):
            if part in value:
                return value[part], new_used
            # Allow nested attr access through the conventional
            # `attrs.<key>` shape returned by the substrate's
            # node-shaped primitives.
            attrs = value.get("attrs")
            if isinstance(attrs, dict) and part in attrs:
                return attrs[part], new_used
            raise KeyError(new_used)
        raise KeyError(new_used)


def format_summary_template(
    *,
    template: Optional[str],
    params: dict[str, Any],
    bundles: dict[str, Any],
) -> str:
    """Render the spec's `summary_template` against the call's
    params + bundle results. On template-time errors, log a
    warning and return the literal template string (per
    S-A7-R1 task 3.4)."""
    if not template:
        return ""
    formatter = _DotAccessFormatter()
    namespace: dict[str, Any] = {**params}
    # Bundles live in their own namespace key + at the top level
    # so templates can write either {papers.count} or
    # {bundles.papers.count}; we prefer the shorter form.
    for emit_as, value in bundles.items():
        namespace.setdefault(emit_as, value)
    namespace.setdefault("bundles", bundles)
    try:
        return formatter.vformat(template, (), namespace)
    except (KeyError, IndexError, ValueError) as e:
        logger.warning(
            "pre_join_tool_summary_template_error: %s; returning "
            "literal template",
            e,
        )
        # We don't raise — S-A7-R1 task 3.4 says the tool still
        # returns the bundle; the `summary` field carries the
        # literal template so the caller sees the intended shape.
        _ = PreJoinSummaryTemplateError  # keep typed-error symbol referenced
        return template
