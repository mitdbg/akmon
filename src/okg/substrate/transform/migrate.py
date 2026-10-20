"""Seven-op IR -> transform IR lowering (task.gtir.ledger-and-migration).

Migrates the existing seven-op optimizer IR (okg.optimizer) into the
delta-centric transform IR per the documented migration table:

    READ          -> FROM
    WALK          -> JOIN (graph-pattern)
    APPLY         -> CALL  (or SEARCH when the function is akmon.search)
    EDIT          -> DELTA
    ATOMIC_EDIT   -> DELTA (substrate commit of an admitted DeltaSet)
    EMIT_EXTERNAL -> CALL (external effect)
    RANDOM        -> CALL (stochastic effect)

Both representations are content-addressed and co-resident in the shared
okg.ir_programs ledger (the transform side under program_kind='transform').
This proves structural op-kind equivalence; runtime read-result equality on a
live fixture is the deeper follow-on.
"""

from __future__ import annotations

from typing import Any

from .ir import Binding, Block, Op, Terminator, TransformProgram
from .registry import Effect, OpKind, TerminatorKind

_KIND_MAP = {
    "read": OpKind.FROM,
    "walk": OpKind.JOIN,
    "apply": OpKind.CALL,
    "edit": OpKind.DELTA,
    "atomic_edit": OpKind.DELTA,
    "emit_external": OpKind.CALL,
    "random": OpKind.CALL,
}
_SURFACE_FOR_TARGET = {"index": "search_index", "node": "nodes_live", "neighbors": "edges_live"}


def _sym(symbol: Any) -> str | None:
    if symbol is None:
        return None
    return getattr(symbol, "name", None) or str(symbol)


def _transform_op(seven_op: Any) -> Op:
    kind = seven_op.kind.name.lower()  # member name is stable (READ/APPLY/...)
    args = dict(seven_op.args)
    if kind == "apply" and args.get("function") == "akmon.search":
        return Op(OpKind.SEARCH, {
            "surface": "search_index", "method": "lexical",
            "query": "$q", "scorer": "akmon.search",
        })
    tkind = _KIND_MAP.get(kind, OpKind.CALL)
    if tkind is OpKind.FROM:
        return Op(OpKind.FROM, {"surface": _SURFACE_FOR_TARGET.get(seven_op.target, "nodes_live")})
    if tkind is OpKind.CALL:
        return Op(OpKind.CALL, {"function_id": args.get("function") or seven_op.target or "kernel"})
    if tkind is OpKind.DELTA:
        return Op(OpKind.DELTA, {"op": "upsert", "target_surface": seven_op.target or "node_facts"})
    if tkind is OpKind.JOIN:
        return Op(OpKind.JOIN, {"how": "inner", "condition": "edge_join"})
    return Op(tkind, {})


def lower_seven_op_to_transform(ir_program: Any) -> TransformProgram:
    """Lower a seven-op optimizer IRProgram into a transform TransformProgram."""
    bindings = [
        Binding(_transform_op(b.op), output=_sym(b.output), inputs=tuple(b.inputs))
        for b in ir_program.ops
    ]
    has_delta = any(b.op.kind is OpKind.DELTA for b in bindings)
    return TransformProgram(
        program_id=f"{ir_program.program_id}.transform",
        effect=Effect.DELTA if has_delta else Effect.READ_ONLY,
        outputs=tuple(ir_program.outputs),
        entry_block="entry",
        attrs={"lowered_from_seven_op": ir_program.program_id},
        blocks=(
            Block(
                "entry",
                tuple(bindings),
                Terminator(TerminatorKind.RETURN, {"values": list(ir_program.outputs)}),
            ),
        ),
    )


def seven_op_kind_correspondence(ir_program: Any, transform: TransformProgram) -> list[tuple[str, str]]:
    """The per-op (seven-op kind -> transform kind) correspondence, in order."""
    return [
        (b.op.kind.name.lower(), t.op.kind.value)
        for b, t in zip(ir_program.ops, transform.blocks[0].bindings)
    ]


__all__ = ["lower_seven_op_to_transform", "seven_op_kind_correspondence"]
