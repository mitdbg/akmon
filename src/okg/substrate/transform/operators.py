"""AKMON read operators expressed as transform-IR programs (PACT migrate-reads).

The `expand` operator (list_neighbors) executes by lowering a TransformProgram over
the registered ``edges_live`` logical surface rather than hand-built SQL against a
base table. The relation it reads is therefore a registered surface (the
seal-graph-reads precondition); over the Chronos registry that surface resolves to
okg.graph_edges and the pin is injected by the checkout, with no WHERE-param.

Scope: the default (single) branch. `search` (BM25/pgvector) needs a SEARCH-op
lowering that does not yet exist and is a tracked follow-on.
"""

from __future__ import annotations

import re
from typing import Any, Optional

import psycopg

from .expr import (
    and_,
    cast,
    col,
    eq,
    eq_any,
    func,
    in_,
    is_null,
    json_contains,
    json_exists,
    json_get,
    lit,
    not_,
    or_,
    param,
)
from .ir import Binding, Block, Op, Terminator, TransformProgram
from .lower import lower_query
from .registry import (
    Effect,
    FunctionRegistry,
    OpKind,
    Registries,
    TerminatorKind,
)
from .surfaces import chronos_surface_registry, substrate_surface_registry

_NAMED_PARAM_RE = re.compile(r"%\((\w+)\)s")


def to_chronos_sql(sql: str) -> str:
    """Translate psycopg ``%(name)s`` placeholders (what lower_query emits) to the
    ``:name`` bind style a Chronos checkout session's ``query`` expects."""
    return _NAMED_PARAM_RE.sub(r":\1", sql)

# v_edges projection the expand operator returns (direction is computed caller-side).
_NEIGHBOR_PROJECT = {
    "edge_id": col("e.edge_id"),
    "src": col("e.src"),
    "dst": col("e.dst"),
    "edge_type": col("e.edge_type"),
    "attrs": col("e.attrs"),
    "source": col("e.source"),
}


# v_nodes projection the inspect(node) operator returns. `source` (the
# node's producer_id) is projected for the read-side provenance/fencing
# layer (PACT fence-untrusted-node-text); it is consumed by the MCP
# tools layer and replaced with a structured `provenance` block before
# the row reaches the agent.
_NODE_PROJECT = {
    "node_id": col("n.node_id"),
    "subtype": col("n.subtype"),
    "attrs": col("n.attrs"),
    "text_hash": col("n.text_hash"),
    "source": col("n.source"),
}


def node_by_id_program() -> TransformProgram:
    """The inspect(node) read (get_node) as a TransformProgram over nodes_live."""
    return TransformProgram(
        program_id="akmon.inspect.get_node",
        effect=Effect.READ_ONLY,
        outputs=("node",),
        entry_block="entry",
        attrs={"operator": "inspect", "target": "node"},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}), output="n"),
                    Binding(Op(OpKind.WHERE, {"expr": eq(col("n.node_id"), param("focal"))}), output="w", inputs=("n",)),
                    Binding(Op(OpKind.SELECT, {"project": _NODE_PROJECT}), output="selected", inputs=("w",)),
                    Binding(Op(OpKind.LIMIT, {"n": 1}), output="limited", inputs=("selected",)),
                    Binding(Op(OpKind.EMIT, {"target": "limited"}), inputs=("limited",)),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["limited"]}),
            ),
        ),
    )


def neighbors_program(
    *, direction: str, edge_type: Optional[str], limit: int, offset: int = 0
) -> TransformProgram:
    """The list_neighbors read as a TransformProgram over the edges_live surface.

    `offset` is a paging parameter, not a routing signal (PACT
    fix-expand-pagination-contract): every page of one neighborhood is served
    by this program, so page 2 cannot differ in shape from page 1. It rides the
    LIMIT op's `offset` arg, which both the SQL lowering and the interpreter
    already honor, and the ORDER on `e.edge_id` above makes the paging stable.
    """
    if direction == "out":
        dir_pred = eq(col("e.src"), param("focal"))
    elif direction == "in":
        dir_pred = eq(col("e.dst"), param("focal"))
    else:  # both
        dir_pred = or_(
            eq(col("e.src"), param("focal")), eq(col("e.dst"), param("focal"))
        )
    where = (
        dir_pred
        if edge_type is None
        else and_(dir_pred, eq(col("e.edge_type"), param("etype")))
    )
    return TransformProgram(
        program_id="akmon.expand.list_neighbors",
        effect=Effect.READ_ONLY,
        outputs=("neighbors",),
        entry_block="entry",
        attrs={"operator": "expand", "direction": direction, "offset": int(offset)},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "edges_live", "alias": "e"}), output="e"),
                    Binding(Op(OpKind.WHERE, {"expr": where}), output="w", inputs=("e",)),
                    Binding(Op(OpKind.SELECT, {"project": _NEIGHBOR_PROJECT}), output="neighbors", inputs=("w",)),
                    Binding(Op(OpKind.ORDER, {"expr": col("e.edge_id")}), output="ordered", inputs=("neighbors",)),
                    Binding(
                        Op(OpKind.LIMIT, {"n": limit, "offset": int(offset)}),
                        output="limited",
                        inputs=("ordered",),
                    ),
                    Binding(Op(OpKind.EMIT, {"target": "limited"}), inputs=("limited",)),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["limited"]}),
            ),
        ),
    )


def aggregate_neighbors_program() -> TransformProgram:
    """The aggregate(node) read (aggregate_neighbors) as a TransformProgram.

    Counts a node's outbound edges grouped by (edge_type, dst subtype) at the pin:
    a LEFT JOIN to nodes_live resolves each destination's subtype (``<unresolved>``
    when the endpoint is absent), GROUP counts per group, ORDER by count desc then
    the two keys. The ``focal`` param binds the source node. Lowers to the canonical
    GROUP+LEFT JOIN shape (the aggregate shape proven in test_lower_read_primitives).
    """
    return TransformProgram(
        program_id="akmon.aggregate.neighbors",
        effect=Effect.READ_ONLY,
        outputs=("counts",),
        entry_block="entry",
        attrs={"operator": "aggregate", "target": "node"},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "edges_live", "alias": "e"}), output="e"),
                    Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "ds"}), output="ds"),
                    Binding(
                        Op(OpKind.JOIN, {"how": "left", "on": eq(col("ds.node_id"), col("e.dst"))}),
                        output="j", inputs=("e", "ds"),
                    ),
                    Binding(
                        Op(OpKind.WHERE, {"expr": eq(col("e.src"), param("focal"))}),
                        output="w", inputs=("j",),
                    ),
                    Binding(
                        Op(OpKind.GROUP, {
                            "key": {
                                "edge_type": col("e.edge_type"),
                                "dst_subtype": func("coalesce", col("ds.subtype"), lit("<unresolved>")),
                            },
                            "agg": {"cnt": {"fn": "count"}},
                        }),
                        output="g", inputs=("w",),
                    ),
                    Binding(
                        Op(OpKind.ORDER, {"keys": [
                            {"expr": col("cnt"), "desc": True},
                            {"expr": col("edge_type")},
                            {"expr": col("dst_subtype")},
                        ]}),
                        output="counts", inputs=("g",),
                    ),
                    Binding(Op(OpKind.EMIT, {"target": "counts"}), inputs=("counts",)),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["counts"]}),
            ),
        ),
    )


def aggregate_edge_routes_program(
    *,
    src_subtype: Optional[str] = None,
    edge_type: Optional[str] = None,
    dst_subtype: Optional[str] = None,
) -> TransformProgram:
    """The aggregate(edge) read (aggregate_edge_routes) as a TransformProgram.

    Counts live edges grouped by (src subtype, edge type, dst subtype) at the pin:
    two INNER JOINs to nodes_live resolve both endpoints' subtypes (so only edges
    whose endpoints are both present are counted), GROUP counts per route, ORDER by
    count desc then the three keys. Optional src_subtype/edge_type/dst_subtype filters
    bind via params and are AND-ed into a WHERE. There is no SQL LIMIT/window: the
    caller fetches the full (small) group set, pages it, and takes the total from its
    length -- the hand-built count(*) OVER() is avoidable (PACT root_cause), and
    count(*) equals the hand-built count(DISTINCT edge_id) because the PK joins never
    fan out.
    """
    preds = []
    if src_subtype is not None:
        preds.append(eq(col("ns.subtype"), param("src_subtype")))
    if edge_type is not None:
        preds.append(eq(col("e.edge_type"), param("edge_type")))
    if dst_subtype is not None:
        preds.append(eq(col("nd.subtype"), param("dst_subtype")))

    bindings = [
        Binding(Op(OpKind.FROM, {"surface": "edges_live", "alias": "e"}), output="e"),
        Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "ns"}), output="ns"),
        Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "nd"}), output="nd"),
        Binding(
            Op(OpKind.JOIN, {"how": "inner", "on": eq(col("ns.node_id"), col("e.src"))}),
            output="j1", inputs=("e", "ns"),
        ),
        Binding(
            Op(OpKind.JOIN, {"how": "inner", "on": eq(col("nd.node_id"), col("e.dst"))}),
            output="j2", inputs=("j1", "nd"),
        ),
    ]
    cur = "j2"
    if preds:
        where = preds[0] if len(preds) == 1 else and_(*preds)
        bindings.append(
            Binding(Op(OpKind.WHERE, {"expr": where}), output="w", inputs=(cur,))
        )
        cur = "w"
    bindings.append(
        Binding(
            Op(OpKind.GROUP, {
                "key": {
                    "src_subtype": col("ns.subtype"),
                    "edge_type": col("e.edge_type"),
                    "dst_subtype": col("nd.subtype"),
                },
                "agg": {"count": {"fn": "count"}},
            }),
            output="g", inputs=(cur,),
        )
    )
    bindings.append(
        Binding(
            Op(OpKind.ORDER, {"keys": [
                {"expr": col("count"), "desc": True},
                {"expr": col("src_subtype")},
                {"expr": col("edge_type")},
                {"expr": col("dst_subtype")},
            ]}),
            output="routes", inputs=("g",),
        )
    )
    bindings.append(Binding(Op(OpKind.EMIT, {"target": "routes"}), inputs=("routes",)))
    return TransformProgram(
        program_id="akmon.aggregate.edge_routes",
        effect=Effect.READ_ONLY,
        outputs=("routes",),
        entry_block="entry",
        attrs={"operator": "aggregate", "target": "edge"},
        blocks=(
            Block(
                block_id="entry",
                bindings=tuple(bindings),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["routes"]}),
            ),
        ),
    )


# --------------------------------------------------------------------------- #
# filter / map: the v1 predicate + projection DSL lowered into the expr DSL.
#
# These translate the AKMON filter/map surface vocabulary (eq / in / exists /
# contains / and / or / not predicates; field-list or {alias: field} projections)
# into expr-DSL nodes over a single live surface, so filter/map lower a
# TransformProgram instead of hand-built SQL. The Chronos graph tables carry no
# `valid_from_gen` column (the pin is the checkout, not a row column), so it is
# NOT in the routed field vocabulary -- the audit records it as a dropped
# legacy-view field. Unsupported ops/fields raise ValueError, which the operator
# surface turns into the `unsupported_operator_form` refusal (fallback: query).
# --------------------------------------------------------------------------- #

_FILTER_SCALAR_FIELDS = {
    "nodes": {"node_id", "subtype", "text_hash"},
    "edges": {"edge_id", "src", "dst", "edge_type"},
}
_FILTER_DEFAULT_PROJECTION = {
    "nodes": ("node_id", "subtype", "attrs", "text_hash"),
    "edges": ("edge_id", "src", "dst", "edge_type", "attrs"),
}

#: Issue #3067, #3666: `contains` has two forms, text inside one attribute
#: and JSON containment on attrs. Every place that refuses another
#: `contains` shape says this one sentence, so no refusal points a caller at
#: a tool it cannot use. Since loose-match-3666 it also names `like`, the
#: substring test on one field that ignores accents and case.
CONTAINS_FORMS_MESSAGE = (
    "contains takes text on one attribute (field 'attrs.<key>': the stored "
    "text holds it, ASCII case ignored) or an object on field 'attrs' (JSON "
    "containment, {key: exact value}); like on one field matches a "
    "substring, ignoring accents and case. Use search(query=...) to find "
    "words."
)


def _scope_surface(kind: str) -> str:
    if kind == "nodes":
        return "nodes_live"
    if kind == "edges":
        return "edges_live"
    raise ValueError(f"unsupported scope kind {kind!r}")


def _field_to_expr(kind: str, field: str) -> dict:
    if field in _FILTER_SCALAR_FIELDS[kind]:
        return col(field)
    if field == "attrs":
        return col("attrs")
    if field.startswith("attrs."):
        key = field.split(".", 1)[1]
        if not key:
            raise ValueError("attrs field path must name a key")
        return json_get(col("attrs"), key)
    raise ValueError(
        f"unsupported {kind} field {field!r}; use stable graph fields or attrs.<key>"
    )


def _predicate_to_expr(kind: str, predicate: Any) -> Optional[dict]:
    """Translate a v1 predicate tree into an expr-DSL node (None = no WHERE)."""
    if predicate in (None, True) or predicate == {}:
        return None
    if not isinstance(predicate, dict):
        raise ValueError("predicate must be an object")
    op = predicate.get("op")
    if op in {"and", "or"}:
        clauses = predicate.get("clauses")
        if not isinstance(clauses, list) or not clauses:
            raise ValueError(f"{op} predicate requires non-empty clauses")
        subs = [_predicate_to_expr(kind, c) for c in clauses]
        subs = [s if s is not None else lit(True) for s in subs]
        return and_(*subs) if op == "and" else or_(*subs)
    if op == "not":
        sub = _predicate_to_expr(kind, predicate.get("clause"))
        return not_(sub if sub is not None else lit(True))
    if op == "eq":
        field = str(predicate.get("field") or "")
        value = predicate.get("value")
        if field.startswith("attrs."):
            key = field.split(".", 1)[1]
            return json_contains(col("attrs"), {key: value})
        return eq(_field_to_expr(kind, field), lit(value))
    if op == "in":
        field = str(predicate.get("field") or "")
        values = predicate.get("values")
        if not isinstance(values, list) or not values:
            raise ValueError("in predicate requires non-empty values")
        return in_(_field_to_expr(kind, field), list(values))
    if op == "exists":
        field = str(predicate.get("field") or "")
        if field.startswith("attrs."):
            key = field.split(".", 1)[1]
            return json_exists(col("attrs"), key)
        return not_(is_null(_field_to_expr(kind, field)))
    if op == "contains":
        field = str(predicate.get("field") or "")
        value = predicate.get("value")
        if field != "attrs" or not isinstance(value, dict):
            # Text on `attrs.<key>` reads attributes, so it is read through
            # the effective-attribute relation and never lowered here.
            raise ValueError(CONTAINS_FORMS_MESSAGE)
        return json_contains(col("attrs"), value)
    raise ValueError(
        f"unsupported predicate op {op!r}; v1 supports and, or, not, eq, "
        "in, exists, contains"
    )


def _projection_to_map(kind: str, projection: Any) -> tuple[dict, list[str]]:
    """Translate a v1 projection into an ordered SELECT map + its column order."""
    if projection is None:
        fields = _FILTER_DEFAULT_PROJECTION[kind]
        return {f: _field_to_expr(kind, f) for f in fields}, list(fields)
    if isinstance(projection, list):
        proj: dict = {}
        columns: list[str] = []
        for field in projection:
            if not isinstance(field, str) or not field:
                raise ValueError("projection list entries must be field names")
            alias = field.replace(".", "_")
            proj[alias] = _field_to_expr(kind, field)
            columns.append(alias)
        return proj, columns
    if isinstance(projection, dict):
        proj = {}
        columns = []
        for alias, field in projection.items():
            if (
                not isinstance(alias, str)
                or not alias.replace("_", "").isalnum()
                or not isinstance(field, str)
            ):
                raise ValueError("projection mapping must be {safe_alias: field_name}")
            proj[alias] = _field_to_expr(kind, field)
            columns.append(alias)
        return proj, columns
    raise ValueError("projection must be a list or mapping")


def _scan_program(
    *, operator: str, kind: str, predicate: Any, projection: Any, limit: int, offset: int
) -> tuple[TransformProgram, list[str]]:
    """Shared core for filter (predicate + projection) and map (projection only).

    FROM live surface [WHERE predicate] SELECT projection ORDER BY first column
    LIMIT n OFFSET m -- the canonical scan shape lower_query already compiles.
    Returns ``(program, columns)``; ``columns`` is the ordered output column list
    the caller uses to shape the row envelope.
    """
    surface = _scope_surface(kind)
    proj, columns = _projection_to_map(kind, projection)
    where = _predicate_to_expr(kind, predicate)
    bindings = [
        Binding(Op(OpKind.FROM, {"surface": surface, "alias": "r"}), output="r"),
    ]
    cur = "r"
    if where is not None:
        bindings.append(
            Binding(Op(OpKind.WHERE, {"expr": where}), output="w", inputs=(cur,))
        )
        cur = "w"
    bindings.append(
        Binding(Op(OpKind.SELECT, {"project": proj}), output="s", inputs=(cur,))
    )
    bindings.append(
        Binding(Op(OpKind.ORDER, {"expr": col(columns[0])}), output="o", inputs=("s",))
    )
    bindings.append(
        Binding(Op(OpKind.LIMIT, {"n": limit, "offset": offset}), output="lim", inputs=("o",))
    )
    bindings.append(Binding(Op(OpKind.EMIT, {"target": "lim"}), inputs=("lim",)))
    program = TransformProgram(
        program_id=f"akmon.{operator}.{kind}",
        effect=Effect.READ_ONLY,
        outputs=("lim",),
        entry_block="entry",
        attrs={"operator": operator, "scope": kind},
        blocks=(
            Block(
                block_id="entry",
                bindings=tuple(bindings),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["lim"]}),
            ),
        ),
    )
    return program, columns


def filter_program(
    *, kind: str, predicate: Any, projection: Any, limit: int, offset: int
) -> tuple[TransformProgram, list[str]]:
    """The filter operator as a TransformProgram (akmon.filter.<scope>)."""
    return _scan_program(
        operator="filter", kind=kind, predicate=predicate,
        projection=projection, limit=limit, offset=offset,
    )


def map_program(
    *, kind: str, projection: Any, limit: int, offset: int
) -> tuple[TransformProgram, list[str]]:
    """The map operator as a TransformProgram (akmon.map.<scope>) -- no WHERE."""
    return _scan_program(
        operator="map", kind=kind, predicate=None,
        projection=projection, limit=limit, offset=offset,
    )


# --------------------------------------------------------------------------- #
# inspect(graph) / inspect(subtype): the composite describe_* reads as a set of
# small single-purpose programs over the live surfaces. node/edge totals are
# derived from the per-subtype / per-edge-type distributions (sum of the groups),
# so no keyless-aggregate primitive is needed; embed_lag groups on the boolean
# key `embedding IS NULL` (a non-keyless GROUP) so it counts within a group.
# --------------------------------------------------------------------------- #


def node_subtype_counts_program() -> TransformProgram:
    """Live node count grouped by subtype (inspect(graph).subtype_counts)."""
    return TransformProgram(
        program_id="akmon.inspect.node_subtype_counts",
        effect=Effect.READ_ONLY,
        outputs=("counts",),
        entry_block="entry",
        attrs={"operator": "inspect", "target": "graph"},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}), output="n"),
                    Binding(
                        Op(OpKind.GROUP, {
                            "key": {"subtype": col("n.subtype")},
                            "agg": {"n": {"fn": "count"}},
                        }),
                        output="g", inputs=("n",),
                    ),
                    Binding(Op(OpKind.ORDER, {"expr": col("subtype")}), output="counts", inputs=("g",)),
                    Binding(Op(OpKind.EMIT, {"target": "counts"}), inputs=("counts",)),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["counts"]}),
            ),
        ),
    )


def edge_type_counts_program() -> TransformProgram:
    """Live edge count grouped by edge_type (inspect(graph).edge_type_counts)."""
    return TransformProgram(
        program_id="akmon.inspect.edge_type_counts",
        effect=Effect.READ_ONLY,
        outputs=("counts",),
        entry_block="entry",
        attrs={"operator": "inspect", "target": "graph"},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "edges_live", "alias": "e"}), output="e"),
                    Binding(
                        Op(OpKind.GROUP, {
                            "key": {"edge_type": col("e.edge_type")},
                            "agg": {"n": {"fn": "count"}},
                        }),
                        output="g", inputs=("e",),
                    ),
                    Binding(Op(OpKind.ORDER, {"expr": col("edge_type")}), output="counts", inputs=("g",)),
                    Binding(Op(OpKind.EMIT, {"target": "counts"}), inputs=("counts",)),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["counts"]}),
            ),
        ),
    )


def embed_lag_program() -> TransformProgram:
    """Count of live text nodes whose embedding is absent, as a GROUP on the
    boolean key (embedding IS NULL): the caller reads the count for the True
    group (inspect(graph).embed_lag)."""
    return TransformProgram(
        program_id="akmon.inspect.embed_lag",
        effect=Effect.READ_ONLY,
        outputs=("rows",),
        entry_block="entry",
        attrs={"operator": "inspect", "target": "graph"},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}), output="n"),
                    Binding(
                        Op(OpKind.WHERE, {"expr": json_exists(col("n.attrs"), "text")}),
                        output="w", inputs=("n",),
                    ),
                    Binding(
                        Op(OpKind.GROUP, {
                            "key": {"missing": is_null(col("n.embedding"))},
                            "agg": {"n": {"fn": "count"}},
                        }),
                        output="rows", inputs=("w",),
                    ),
                    Binding(Op(OpKind.EMIT, {"target": "rows"}), inputs=("rows",)),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["rows"]}),
            ),
        ),
    )


def entity_mentions_program() -> TransformProgram:
    """Live entity_mention nodes (node_id, attrs) for the mention-resolution
    block (inspect(graph).unresolved_mentions)."""
    return TransformProgram(
        program_id="akmon.inspect.entity_mentions",
        effect=Effect.READ_ONLY,
        outputs=("mentions",),
        entry_block="entry",
        attrs={"operator": "inspect", "target": "graph"},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}), output="n"),
                    Binding(
                        Op(OpKind.WHERE, {"expr": eq(col("n.subtype"), lit("entity_mention"))}),
                        output="w", inputs=("n",),
                    ),
                    Binding(
                        Op(OpKind.SELECT, {"project": {"node_id": col("n.node_id"), "attrs": col("n.attrs")}}),
                        output="mentions", inputs=("w",),
                    ),
                    Binding(Op(OpKind.EMIT, {"target": "mentions"}), inputs=("mentions",)),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["mentions"]}),
            ),
        ),
    )


def resolved_mention_sources_program() -> TransformProgram:
    """Distinct src node_ids carrying a mentions/references edge -- the resolved
    set for inspect(graph).unresolved_mentions."""
    return TransformProgram(
        program_id="akmon.inspect.resolved_mention_sources",
        effect=Effect.READ_ONLY,
        outputs=("resolved",),
        entry_block="entry",
        attrs={"operator": "inspect", "target": "graph"},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "edges_live", "alias": "e"}), output="e"),
                    Binding(
                        Op(OpKind.WHERE, {"expr": in_(col("e.edge_type"), ["mentions", "references"])}),
                        output="w", inputs=("e",),
                    ),
                    Binding(
                        Op(OpKind.SELECT, {"project": {"node_id": col("e.src")}, "distinct": True}),
                        output="resolved", inputs=("w",),
                    ),
                    Binding(Op(OpKind.EMIT, {"target": "resolved"}), inputs=("resolved",)),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["resolved"]}),
            ),
        ),
    )


def subtype_nodes_program() -> TransformProgram:
    """Live nodes (node_id, attrs) of one subtype, ordered by node_id -- the
    inspect(subtype) attr-key/sample/count source. `subtype` binds via param."""
    return TransformProgram(
        program_id="akmon.inspect.subtype_nodes",
        effect=Effect.READ_ONLY,
        outputs=("nodes",),
        entry_block="entry",
        attrs={"operator": "inspect", "target": "subtype"},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}), output="n"),
                    Binding(
                        Op(OpKind.WHERE, {"expr": eq(col("n.subtype"), param("subtype"))}),
                        output="w", inputs=("n",),
                    ),
                    Binding(
                        Op(OpKind.SELECT, {"project": {
                            "node_id": col("n.node_id"),
                            "attrs": col("n.attrs"),
                            "source": col("n.source"),
                        }}),
                        output="s", inputs=("w",),
                    ),
                    Binding(Op(OpKind.ORDER, {"expr": col("node_id")}), output="nodes", inputs=("s",)),
                    Binding(Op(OpKind.EMIT, {"target": "nodes"}), inputs=("nodes",)),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["nodes"]}),
            ),
        ),
    )


def _subtype_routes_program(*, program_id: str, anchor: str, other: str, other_alias: str) -> TransformProgram:
    """Shared shape for inspect(subtype) edges_out/edges_in: DISTINCT (edge_type,
    <other endpoint subtype>) for edges whose <anchor endpoint> has the param
    subtype. INNER JOIN resolves the anchor endpoint; LEFT JOIN + coalesce
    resolves the other endpoint (``<unresolved>`` when absent)."""
    other_col = "src_subtype" if other_alias == "ns" else "dst_subtype"
    return TransformProgram(
        program_id=program_id,
        effect=Effect.READ_ONLY,
        outputs=("routes",),
        entry_block="entry",
        attrs={"operator": "inspect", "target": "subtype"},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "edges_live", "alias": "e"}), output="e"),
                    Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "na"}), output="na"),
                    Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": other_alias}), output="no"),
                    Binding(
                        Op(OpKind.JOIN, {"how": "inner", "on": eq(col("na.node_id"), col(f"e.{anchor}"))}),
                        output="j1", inputs=("e", "na"),
                    ),
                    Binding(
                        Op(OpKind.JOIN, {"how": "left", "on": eq(col(f"{other_alias}.node_id"), col(f"e.{other}"))}),
                        output="j2", inputs=("j1", "no"),
                    ),
                    Binding(
                        Op(OpKind.WHERE, {"expr": eq(col("na.subtype"), param("subtype"))}),
                        output="w", inputs=("j2",),
                    ),
                    Binding(
                        Op(OpKind.SELECT, {
                            "project": {
                                "edge_type": col("e.edge_type"),
                                other_col: func("coalesce", col(f"{other_alias}.subtype"), lit("<unresolved>")),
                            },
                            "distinct": True,
                        }),
                        output="s", inputs=("w",),
                    ),
                    Binding(
                        Op(OpKind.ORDER, {"keys": [
                            {"expr": col("edge_type")},
                            {"expr": col(other_col)},
                        ]}),
                        output="routes", inputs=("s",),
                    ),
                    Binding(Op(OpKind.EMIT, {"target": "routes"}), inputs=("routes",)),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["routes"]}),
            ),
        ),
    )


def subtype_edges_out_program() -> TransformProgram:
    """inspect(subtype).edges_out: DISTINCT (edge_type, dst_subtype) for edges
    whose src has the param subtype."""
    return _subtype_routes_program(
        program_id="akmon.inspect.subtype_edges_out",
        anchor="src", other="dst", other_alias="nd",
    )


def subtype_edges_in_program() -> TransformProgram:
    """inspect(subtype).edges_in: DISTINCT (edge_type, src_subtype) for edges
    whose dst has the param subtype."""
    return _subtype_routes_program(
        program_id="akmon.inspect.subtype_edges_in",
        anchor="dst", other="src", other_alias="ns",
    )


# --------------------------------------------------------------------------- #
# expand(path): the typed-path / uniform-depth traversal as a bounded, unrolled
# JOIN chain. The traversal depth is a compile-time constant (MAX 3 hops), so the
# bounded path lowers by UNROLLING -- one edges_live JOIN per hop -- which is the
# sound lowering of a compile-time-bounded loop and reproduces the pre-migration
# traverse_path terminal set. (A WITH RECURSIVE form is only required for a
# runtime-unbounded depth, which this operator does not have; the heterogeneous
# typed path could not be a uniform recursion in any case.) `start` / `etype_i` /
# `target_subtype` bind via params.
# --------------------------------------------------------------------------- #


def _hop_cols(direction: str) -> tuple[str, str]:
    """(match_col, next_col): a hop matches the frontier on match_col and steps to
    next_col. 'out' follows src->dst; 'in' follows dst->src."""
    if direction == "out":
        return "src", "dst"
    if direction == "in":
        return "dst", "src"
    raise ValueError(f"hop direction must be 'out' or 'in'; got {direction!r}")


def expand_path_program(
    *, hops, target_subtype: Optional[str] = None, limit: int
) -> TransformProgram:
    """expand(path) as an unrolled JOIN-chain TransformProgram (akmon.expand.path).

    ``hops`` is a non-empty list of ``(edge_type, direction)`` with direction in
    {"out","in"}; ``edge_type=None`` means "any edge type" for that hop (the
    uniform-depth case may omit it). The program seeds the frontier at the start
    node (the first hop's anchor column = :start), chains one edges_live JOIN per
    hop, and joins nodes_live to resolve the terminal node's subtype/attrs.
    ``SELECT DISTINCT`` collapses multi-path duplicates to the reachable terminal
    set; a degenerate length-1 path is the 1-hop neighbor lookup.
    """
    if not hops:
        raise ValueError("expand_path_program requires at least one hop")

    bindings: list[Binding] = []
    where_terms: list[dict] = []

    etype0, dir0 = hops[0]
    match0, next0 = _hop_cols(dir0)
    bindings.append(Binding(Op(OpKind.FROM, {"surface": "edges_live", "alias": "e1"}), output="e1"))
    where_terms.append(eq(col(f"e1.{match0}"), param("start")))
    if etype0 is not None:
        where_terms.append(eq(col("e1.edge_type"), param("etype_1")))
    prev_alias, prev_next, cur_sym = "e1", next0, "e1"

    for i, (etype, direction) in enumerate(hops[1:], start=2):
        match_i, next_i = _hop_cols(direction)
        alias = f"e{i}"
        bindings.append(Binding(Op(OpKind.FROM, {"surface": "edges_live", "alias": alias}), output=alias))
        on = eq(col(f"{alias}.{match_i}"), col(f"{prev_alias}.{prev_next}"))
        if etype is not None:
            on = and_(on, eq(col(f"{alias}.edge_type"), param(f"etype_{i}")))
        join_out = f"j{i}"
        bindings.append(Binding(Op(OpKind.JOIN, {"how": "inner", "on": on}), output=join_out, inputs=(cur_sym, alias)))
        prev_alias, prev_next, cur_sym = alias, next_i, join_out

    bindings.append(Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "nl"}), output="nl"))
    bindings.append(
        Binding(
            Op(OpKind.JOIN, {"how": "inner", "on": eq(col("nl.node_id"), col(f"{prev_alias}.{prev_next}"))}),
            output="jn", inputs=(cur_sym, "nl"),
        )
    )
    if target_subtype is not None:
        where_terms.append(eq(col("nl.subtype"), param("target_subtype")))
    where_expr = where_terms[0] if len(where_terms) == 1 else and_(*where_terms)
    bindings.append(Binding(Op(OpKind.WHERE, {"expr": where_expr}), output="w", inputs=("jn",)))
    bindings.append(
        Binding(
            Op(OpKind.SELECT, {
                    "project": {
                        "node_id": col("nl.node_id"),
                        "subtype": col("nl.subtype"),
                        "attrs": col("nl.attrs"),
                        "source": col("nl.source"),
                    },
                "distinct": True,
            }),
            output="s", inputs=("w",),
        )
    )
    bindings.append(Binding(Op(OpKind.ORDER, {"expr": col("node_id")}), output="o", inputs=("s",)))
    bindings.append(Binding(Op(OpKind.LIMIT, {"n": limit}), output="lim", inputs=("o",)))
    bindings.append(Binding(Op(OpKind.EMIT, {"target": "lim"}), inputs=("lim",)))
    return TransformProgram(
        program_id="akmon.expand.path",
        effect=Effect.READ_ONLY,
        outputs=("lim",),
        entry_block="entry",
        attrs={"operator": "expand", "mode": "path", "hops": len(hops)},
        blocks=(
            Block(
                block_id="entry",
                bindings=tuple(bindings),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["lim"]}),
            ),
        ),
    )


# --------------------------------------------------------------------------- #
# search: lexical (relational scan; the custom term-count scorer stays client-
# side, so routing preserves the exact pre-migration ranking) and semantic
# (pgvector cosine_distance over the registered surface, ordered + limited in
# SQL). The alias method reads CATALOG tables and is out of scope (residual).
# --------------------------------------------------------------------------- #


def lexical_search_program() -> TransformProgram:
    """The search(method=lexical) scan as a TransformProgram (akmon.search.lexical).

    Selects every live node that carries an ``attrs.text`` key; the caller applies
    the (subtype/attr) filter and the term-count scorer client-side, so the routed
    scan reproduces the pre-migration ranking exactly while naming only the
    registered nodes_live surface.
    """
    return TransformProgram(
        program_id="akmon.search.lexical",
        effect=Effect.READ_ONLY,
        outputs=("rows",),
        entry_block="entry",
        attrs={"operator": "search", "method": "lexical"},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}), output="n"),
                    Binding(
                        Op(OpKind.WHERE, {"expr": json_exists(col("n.attrs"), "text")}),
                        output="w", inputs=("n",),
                    ),
                    Binding(
                        Op(OpKind.SELECT, {"project": {
                            "node_id": col("n.node_id"),
                            "subtype": col("n.subtype"),
                            "attrs": col("n.attrs"),
                            "text_hash": col("n.text_hash"),
                            "source": col("n.source"),
                        }}),
                        output="s", inputs=("w",),
                    ),
                    Binding(Op(OpKind.ORDER, {"expr": col("node_id")}), output="rows", inputs=("s",)),
                    Binding(Op(OpKind.EMIT, {"target": "rows"}), inputs=("rows",)),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["rows"]}),
            ),
        ),
    )


def semantic_search_program(
    *, limit: int, scoped_subtypes: bool = False,
    scoped_source_ids: bool = False,
) -> TransformProgram:
    """The search(method=semantic) ranking as a TransformProgram (akmon.search.semantic).

    pgvector cosine distance from the query vector (bound as the ``qvec`` param,
    cast to ``vector``) to each live node's embedding, ordered ascending then by
    node_id, limited in SQL. The caller applies the (subtype/attr) filter and final
    truncation client-side (matching the pre-migration shape). SQL-only: the
    distance is not row-wise Python-evaluable, so this program is verified by
    result-parity, not the interpreter.

    ``scoped_subtypes=True`` adds ``n.subtype = ANY(%(scoped_subtypes)s)`` to the
    WHERE, so a deployment search profile narrows the ranking IN SQL rather than
    the returned page. This matters: the SQL ``LIMIT`` is ``offset+limit+1``, so
    filtering subtypes client-side over that window would under-fill or empty a
    scoped page while in-scope matches sat deeper in the ranking — the same
    eviction the lexical scope predicate exists to prevent
    (`analyst-search-profiles`, issue #797). The caller binds the resolved
    subtype list as the ``scoped_subtypes`` param.

    ``scoped_source_ids=True`` adds ``n.source = ANY(%(scoped_source_ids)s)`` on
    exactly the same argument, for the deployment-declared source-family filter
    (`mcp-authority-diversity-budgets`, issue #1444). The caller binds the
    resolved producer ids as the ``scoped_source_ids`` param.
    """
    distance = func("cosine_distance", col("n.embedding"), cast(param("qvec"), "vector"))
    where_expr = not_(is_null(col("n.embedding")))
    if scoped_subtypes:
        where_expr = and_(
            where_expr, eq_any(col("n.subtype"), "scoped_subtypes"),
        )
    if scoped_source_ids:
        where_expr = and_(
            where_expr, eq_any(col("n.source"), "scoped_source_ids"),
        )
    return TransformProgram(
        program_id="akmon.search.semantic",
        effect=Effect.READ_ONLY,
        outputs=("ranked",),
        entry_block="entry",
        attrs={"operator": "search", "method": "semantic"},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}), output="n"),
                    Binding(
                        Op(OpKind.WHERE, {"expr": where_expr}),
                        output="w", inputs=("n",),
                    ),
                    Binding(
                        Op(OpKind.SELECT, {"project": {
                            "node_id": col("n.node_id"),
                            "subtype": col("n.subtype"),
                            "attrs": col("n.attrs"),
                            "text_hash": col("n.text_hash"),
                            "source": col("n.source"),
                            # The source RECORD this row was derived from, so a
                            # semantic candidate can be cited back to the
                            # producer record rather than only to its source
                            # name (issue #1444).
                            "source_record_id": col("n.source_record_id"),
                            "source_revision": col("n.source_revision"),
                            # Which model wrote this row's vector. Projected so
                            # a row admitted under
                            # --allow-unverified-embedding-provenance can be
                            # LABELLED rather than blended silently with
                            # verified rows (issue #797).
                            "embedding_model": col("n.embedding_model"),
                            "distance": distance,
                        }}),
                        output="s", inputs=("w",),
                    ),
                    Binding(
                        Op(OpKind.ORDER, {"keys": [
                            {"expr": col("distance")},
                            {"expr": col("node_id")},
                        ]}),
                        output="ordered", inputs=("s",),
                    ),
                    Binding(Op(OpKind.LIMIT, {"n": limit}), output="ranked", inputs=("ordered",)),
                    Binding(Op(OpKind.EMIT, {"target": "ranked"}), inputs=("ranked",)),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["ranked"]}),
            ),
        ),
    )


def lower_neighbors_sql(
    *, direction: str, edge_type: Optional[str], limit: int, offset: int = 0
) -> tuple[str, dict]:
    """Lower the list_neighbors program to (sql, params) over registered surfaces."""
    prog = neighbors_program(
        direction=direction, edge_type=edge_type, limit=limit, offset=offset
    )
    reg = Registries(surfaces=substrate_surface_registry(), functions=FunctionRegistry())
    return lower_query(prog, reg)


def execute_list_neighbors(
    conn: psycopg.Connection[Any],
    *,
    generation_id: int,
    node_id: str,
    edge_type: Optional[str],
    direction: str,
    limit: int,
) -> tuple[str, list[tuple]]:
    """Run the lowered expand program over okg.v_edges pinned to ``generation_id``.

    Returns ``(sql, rows)`` where each row is ``(edge_id, src, dst, edge_type,
    attrs)``. The caller labels direction and shapes the envelope. The pin GUC is
    restored afterward so the operator does not leak its pin onto the connection.
    """
    sql, params = lower_neighbors_sql(
        direction=direction, edge_type=edge_type, limit=limit
    )
    params = {**params, "focal": node_id}
    if edge_type is not None:
        params["etype"] = edge_type
    with conn.cursor() as cur:
        cur.execute("SELECT current_setting('okg.pin_generation', true)")
        prior = cur.fetchone()[0]
        cur.execute(
            "SELECT set_config('okg.pin_generation', %s, false)", (str(generation_id),)
        )
        try:
            cur.execute(sql, params)
            rows = cur.fetchall()
        finally:
            cur.execute("SELECT set_config('okg.pin_generation', %s, false)", (prior,))
    return sql, rows


def lower_neighbors_chronos(
    *, direction: str, edge_type: Optional[str], limit: int, offset: int = 0
) -> tuple[str, dict]:
    """Lower the list_neighbors program over the Chronos surfaces (okg.graph_edges),
    in the ``:name`` bind style for the Chronos session."""
    prog = neighbors_program(
        direction=direction, edge_type=edge_type, limit=limit, offset=offset
    )
    reg = Registries(surfaces=chronos_surface_registry(), functions=FunctionRegistry())
    sql, params = lower_query(prog, reg)
    return to_chronos_sql(sql), params


def execute_list_neighbors_chronos(
    graph_session: Any,
    *,
    node_id: str,
    edge_type: Optional[str],
    direction: str,
    limit: int,
    offset: int = 0,
) -> list[dict]:
    """Run the lowered expand program over okg.graph_edges via a pinned Chronos
    checkout ``graph_session`` (e.g. store.checkout_generation(gen)).

    Returns the session's row dicts (edge_id, src, dst, edge_type, attrs, source);
    the caller labels direction and shapes the envelope. The session injects the
    checkpoint pin, so the lowered SQL carries no pin predicate.
    """
    sql, params = lower_neighbors_chronos(
        direction=direction, edge_type=edge_type, limit=limit, offset=offset
    )
    params = {**params, "focal": node_id}
    if edge_type is not None:
        params["etype"] = edge_type
    return graph_session.query(sql, params)


def lower_program_over_chronos(
    program: TransformProgram,
    conn: Any,
    *,
    generation_id: Any = None,
    branch: str = "default",
    catalog_version_id: Optional[int] = None,
    extra_params: Optional[dict] = None,
) -> list[dict]:
    """Run a read-only TransformProgram through the IR over the Chronos store.

    This is the canonical "graph read through the IR, over Chronos" seam (PACT
    chronos-native-graph-cutover, "survivors go through the IR"): lower the program
    over chronos_surface_registry() (live surfaces bound to okg.graph_nodes/
    graph_edges), translate to the Chronos `:name` bind style, and execute via a
    pinned checkout (`generation_id` set) or the current default-branch state
    (`generation_id is None`). Returns the session's dict rows.

    `catalog_version_id` registers the narrowing_allowed closure surface (for the
    narrowing-conformance program). `extra_params` supplies runtime binds the program
    references via param() (e.g. {"focal": node_id}).
    """
    from okg.substrate.graph_store import ChronosGraphStore

    reg = chronos_surface_registry()
    if catalog_version_id is not None:
        from .consistency import narrowing_allowed_binding
        from .registry import Surface, SurfaceKind

        reg.register(
            Surface(
                "narrowing_allowed",
                SurfaceKind.CATALOG_POLICY,
                narrowing_allowed_binding(catalog_version_id),
                pinned=False,
            )
        )
    sql, params = lower_query(
        program, Registries(surfaces=reg, functions=FunctionRegistry())
    )
    if extra_params:
        params = {**params, **extra_params}
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    session = (
        store.checkout(branch)
        if generation_id is None
        else store.checkout_generation(generation_id, branch=branch)
    )
    return session.query(to_chronos_sql(sql), params)


__all__ = [
    "node_by_id_program",
    "neighbors_program",
    "aggregate_neighbors_program",
    "aggregate_edge_routes_program",
    "filter_program",
    "map_program",
    "node_subtype_counts_program",
    "edge_type_counts_program",
    "embed_lag_program",
    "entity_mentions_program",
    "resolved_mention_sources_program",
    "subtype_nodes_program",
    "subtype_edges_out_program",
    "subtype_edges_in_program",
    "expand_path_program",
    "lexical_search_program",
    "semantic_search_program",
    "lower_neighbors_sql",
    "execute_list_neighbors",
    "to_chronos_sql",
    "lower_neighbors_chronos",
    "execute_list_neighbors_chronos",
    "lower_program_over_chronos",
]
