"""Sugar normalization (task.gtir.normalize-and-lower, normalization half).

Surface syntax (MATCH, EXPAND, TOPK, UPSERT/DELETE/PATCH, legacy WALK) is not part
of the core algebra; it normalizes into core ``Binding``s *before* costing,
verification, or ledgering (design 01). There is no primitive traversal operator:
EXPAND is a one-hop graph pattern and WALK is a bounded ITERATE.

Normalization is deterministic -- expanding the same sugar twice yields identical
core bindings (byte-stable normal form), which is what makes content-addressing
of the normalized program meaningful.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .ir import Binding, Block, Op, Terminator
from .registry import DeltaOp, OpKind, TerminatorKind


class SugarKind(str, Enum):
    MATCH = "match"
    EXPAND = "expand"
    TOPK = "topk"
    UPSERT = "upsert"
    DELETE = "delete"
    PATCH = "patch"
    WALK = "walk"


@dataclass
class Sugar:
    """A surface-syntax node that normalizes into core bindings."""

    kind: SugarKind
    output: str
    args: dict[str, Any] = field(default_factory=dict)
    inputs: tuple[str, ...] = ()


_DELTA_OP = {
    SugarKind.UPSERT: DeltaOp.UPSERT,
    SugarKind.DELETE: DeltaOp.RETRACT,
    SugarKind.PATCH: DeltaOp.PATCH,
}


def expand(sugar: Sugar) -> list[Binding]:
    """Expand one within-block sugar node into core bindings (deterministic)."""
    out = sugar.output
    a = sugar.args
    k = sugar.kind

    if k is SugarKind.TOPK:
        ordered = f"{out}__ord"
        return [
            Binding(Op(OpKind.ORDER, {"key": a["key"], "desc": a.get("desc", True)}), output=ordered, inputs=sugar.inputs),
            Binding(Op(OpKind.LIMIT, {"n": a["n"]}), output=out, inputs=(ordered,)),
        ]

    if k in _DELTA_OP:
        # UPSERT/DELETE/PATCH are DELTA rows with an explicit op value.
        return [
            Binding(
                Op(OpKind.DELTA, {"op": _DELTA_OP[k].value, "target_surface": a["target_surface"]}),
                output=out,
                inputs=sugar.inputs,
            )
        ]

    if k is SugarKind.EXPAND:
        # One-hop pattern: frontier -JOIN edges -[WHERE type]- JOIN nodes -SELECT.
        (frontier,) = sugar.inputs
        edges, nodes = f"{out}__edges", f"{out}__nodes"
        je, jn = f"{out}__je", f"{out}__jn"
        bindings = [
            Binding(Op(OpKind.FROM, {"surface": a["edge_surface"]}), output=edges),
            Binding(Op(OpKind.JOIN, {"how": "inner", "condition": a["frontier_edge_cond"]}), output=je, inputs=(frontier, edges)),
        ]
        prev = je
        if "edge_type_pred" in a:
            wt = f"{out}__wt"
            bindings.append(Binding(Op(OpKind.WHERE, {"predicate": a["edge_type_pred"]}), output=wt, inputs=(prev,)))
            prev = wt
        bindings += [
            Binding(Op(OpKind.FROM, {"surface": a["node_surface"]}), output=nodes),
            Binding(Op(OpKind.JOIN, {"how": "inner", "condition": a["edge_node_cond"]}), output=jn, inputs=(prev, nodes)),
            Binding(Op(OpKind.SELECT, {"projection": a["projection"]}), output=out, inputs=(jn,)),
        ]
        return bindings

    if k is SugarKind.MATCH:
        src, edges, dst = f"{out}__src", f"{out}__edges", f"{out}__dst"
        je, jd = f"{out}__je", f"{out}__jd"
        bindings = [
            Binding(Op(OpKind.FROM, {"surface": a["src_surface"]}), output=src),
            Binding(Op(OpKind.FROM, {"surface": a["edge_surface"]}), output=edges),
            Binding(Op(OpKind.JOIN, {"how": "inner", "condition": a["src_edge_cond"]}), output=je, inputs=(src, edges)),
            Binding(Op(OpKind.FROM, {"surface": a["dst_surface"]}), output=dst),
            Binding(Op(OpKind.JOIN, {"how": "inner", "condition": a["edge_dst_cond"]}), output=jd, inputs=(je, dst)),
        ]
        prev = jd
        if "where" in a:
            w = f"{out}__w"
            bindings.append(Binding(Op(OpKind.WHERE, {"predicate": a["where"]}), output=w, inputs=(prev,)))
            prev = w
        bindings.append(Binding(Op(OpKind.SELECT, {"projection": a["projection"]}), output=out, inputs=(prev,)))
        return bindings

    raise ValueError(f"{k} is not a within-block sugar; WALK lowers via walk_blocks()")


def normalize_block(block_id: str, items: list, returns: tuple[str, ...]) -> Block:
    """Expand a mixed list of core Binding | Sugar into an all-core Block."""
    bindings: list[Binding] = []
    for item in items:
        if isinstance(item, Sugar):
            bindings.extend(expand(item))
        else:
            bindings.append(item)
    return Block(
        block_id=block_id,
        bindings=tuple(bindings),
        terminator=Terminator(TerminatorKind.RETURN, {"values": list(returns)}),
    )


def walk_blocks(walk: Sugar, *, entry_id: str = "entry", step_id: str = "walk_step") -> tuple[Block, Block]:
    """Lower a legacy WALK into a bounded ITERATE (entry + step blocks).

    WALK(seed_surface, edge_surface, node_surface, max_steps, ...) becomes: seed a
    frontier, then ITERATE a one-hop EXPAND step up to ``max_steps``.
    """
    a = walk.args
    seed = "frontier0"
    entry = Block(
        block_id=entry_id,
        bindings=(Binding(Op(OpKind.FROM, {"surface": a["seed_surface"]}), output=seed),),
        terminator=Terminator(
            TerminatorKind.ITERATE,
            {
                "state0": seed,
                "state_symbol": "frontier",
                "step_block": step_id,
                "max_steps": a["max_steps"],
                "result": walk.output,
            },
        ),
    )
    step_expand = Sugar(
        SugarKind.EXPAND,
        output="frontier_next",
        args={
            "edge_surface": a["edge_surface"],
            "node_surface": a["node_surface"],
            "frontier_edge_cond": a["frontier_edge_cond"],
            "edge_node_cond": a["edge_node_cond"],
            "projection": a["projection"],
        },
        inputs=("frontier",),
    )
    step = Block(
        block_id=step_id,
        bindings=tuple(expand(step_expand)),
        terminator=Terminator(TerminatorKind.RETURN, {"values": ["frontier_next"]}),
    )
    return entry, step


__all__ = ["SugarKind", "Sugar", "expand", "normalize_block", "walk_blocks"]
