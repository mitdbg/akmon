"""Read-only tool palette callable through the browser-facing dispatcher.

Each entry is ``(InputModel, handler)``. Handlers take a
``VizContext`` (not the legacy ``OKGTools``) and the validated
input. Every call goes through SQL on the substrate, pinned to
the context's session.

Policy:
- read-only: no write, propose, quarantine, shell, or
  schema-modifying tools ever appear here
- narrow: tools with an existing UI affordance
  (``search_nodes``, ``list_neighbors``, ``get_node``) stay in
  those widgets; they are deliberately not in the palette

The HTTP dispatcher in ``okg.viz.app`` is intentionally thin:
look up the name, validate the body, call the handler, return
the result. Unknown names return 404.
"""

from __future__ import annotations

from typing import Any, Callable

from pydantic import BaseModel, Field

from okg.viz import reads
from okg.viz.context import VizContext


# ----------------------------- input schemas ----------------------


class ResolveAliasInput(BaseModel):
    name: str = Field(min_length=1, description="alias to resolve")
    limit: int = Field(default=5, ge=1, le=50)


class TraversePathInput(BaseModel):
    start_node: str = Field(min_length=1)
    path: list[str] = Field(
        min_length=1,
        description='list of "edge_type:direction" hops',
    )
    target_type: str | None = None
    page_size: int = Field(default=50, ge=1, le=500)


class AggregateNeighborsInput(BaseModel):
    node_id: str = Field(min_length=1)
    edge_type: str | None = None
    direction: str = Field(default="out")


class SearchByTypeInput(BaseModel):
    subtype: str = Field(min_length=1)
    page_size: int = Field(default=25, ge=1, le=200)
    offset: int = Field(default=0, ge=0)


# ----------------------------- handlers ---------------------------


def _resolve_alias(ctx: VizContext, inp: ResolveAliasInput) -> dict[str, Any]:
    """Substrate alias-search via mcp.search."""
    from okg.substrate.mcp.search import search as mcp_search
    with ctx.conn() as conn:
        return mcp_search(
            conn, ctx.session, inp.name,
            method="alias", filter={}, limit=inp.limit,
            embedder=None,
        )


def _traverse_path(ctx: VizContext, inp: TraversePathInput) -> dict[str, Any]:
    """Substrate `traverse_path` MCP tool."""
    from okg.substrate.mcp.tools import expand_path as _ep, parse_path_hops
    with ctx.conn() as conn:
        out = _ep(
            conn, ctx.session, inp.start_node,
            hops=parse_path_hops(inp.path),
            target_subtype=inp.target_type,
            limit=inp.page_size,
        )
    return out


def _aggregate_neighbors(
    ctx: VizContext, inp: AggregateNeighborsInput,
) -> dict[str, Any]:
    """Substrate `aggregate_neighbors` MCP tool."""
    from okg.substrate.mcp.tools import aggregate_node as _agg
    with ctx.conn() as conn:
        return _agg(conn, ctx.session, inp.node_id)


def _search_by_type(
    ctx: VizContext, inp: SearchByTypeInput,
) -> dict[str, Any]:
    """Page through nodes of a given subtype."""
    with ctx.conn() as conn:
        # Materialize one page (offset + limit pattern). The
        # iter_nodes helper streams; for paging we walk to offset
        # then collect page_size. For modest datasets this is
        # fine; substrate-native paging via SQL OFFSET/LIMIT is
        # the obvious follow-up.
        rows = list(
            reads.iter_nodes(
                conn, ctx.session, subtype=inp.subtype,
                limit=inp.offset + inp.page_size,
            )
        )
    page = rows[inp.offset:inp.offset + inp.page_size]
    return {
        "items": [
            {"node_id": n.id, "subtype": n.type, "label": n.label}
            for n in page
        ],
        "total": None,  # cheap count not implemented in this surface
        "page_size": inp.page_size,
        "offset": inp.offset,
    }


# ----------------------------- allowlist --------------------------


ALLOWLIST: dict[
    str, tuple[type[BaseModel], Callable[[VizContext, Any], dict[str, Any]]]
] = {
    "resolve_alias": (ResolveAliasInput, _resolve_alias),
    "traverse_path": (TraversePathInput, _traverse_path),
    "aggregate_neighbors": (AggregateNeighborsInput, _aggregate_neighbors),
    "search_by_type": (SearchByTypeInput, _search_by_type),
}


PROHIBITED_TOOL_NAMES = frozenset({
    "apply_proposal", "propose_change", "quarantine_node",
    "rehabilitate_node", "upsert_node", "delete_node",
    "shell", "exec", "run_command", "system",
    "apply_schema", "extend_ontology",
})


def describe_allowlist() -> dict[str, dict[str, Any]]:
    """Machine-readable description of the palette."""
    descriptions: dict[str, dict[str, Any]] = {}
    for name, (model, _handler) in ALLOWLIST.items():
        descriptions[name] = {
            "input_schema": model.model_json_schema(),
        }
    return descriptions
