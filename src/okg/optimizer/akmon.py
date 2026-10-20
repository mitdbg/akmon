"""Lower AKMON operator calls into the stack IR."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any

from .ir import (
    IRBinding,
    IRBlock,
    IRExecutionEnv,
    IRForEachOutputPolicy,
    IREffect,
    IROp,
    IROpKind,
    IRProgram,
    IRSymbol,
    IRTerminator,
    IRType,
    IRValueKind,
)


class AKMONOperator(str, Enum):
    SEARCH = "Search"
    INSPECT = "Inspect"
    EXPAND = "Expand"
    FILTER = "Filter"
    MAP = "Map"
    AGGREGATE = "Aggregate"
    PROPOSE = "Propose"


@dataclass(frozen=True)
class AKMONCall:
    operator: AKMONOperator
    args: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    call_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "operator", AKMONOperator(self.operator))
        object.__setattr__(self, "args", MappingProxyType(dict(self.args)))


NODE = IRType(IRValueKind.NODE_REF.value)
EDGE_SET = IRType(IRValueKind.COLLECTION.value, {"element": IRValueKind.EDGE_REF.value})
NODE_SET = IRType(IRValueKind.COLLECTION.value, {"element": IRValueKind.NODE_REF.value})
RECORD = IRType(IRValueKind.RECORD.value)
SCALAR = IRType(IRValueKind.SCALAR.value)
PROPOSAL = IRType(IRValueKind.RECORD.value, {"subtype": "proposal"})


def lower_akmon_call(
    call: AKMONCall,
    *,
    env: IRExecutionEnv | None = None,
    program_id: str | None = None,
) -> IRProgram:
    """Lower one AKMON operator call into a typed IR program."""

    op = call.operator
    pid = program_id or f"akmon:{op.value}:{call.call_id or 'call'}"
    if op is AKMONOperator.SEARCH:
        return _search(call, env=env, program_id=pid)
    if op is AKMONOperator.INSPECT:
        return _inspect(call, env=env, program_id=pid)
    if op is AKMONOperator.EXPAND:
        return _expand(call, env=env, program_id=pid)
    if op is AKMONOperator.FILTER:
        return _filter(call, env=env, program_id=pid)
    if op is AKMONOperator.MAP:
        return _map(call, env=env, program_id=pid)
    if op is AKMONOperator.AGGREGATE:
        return _aggregate(call, env=env, program_id=pid)
    if op is AKMONOperator.PROPOSE:
        return _propose(call, env=env, program_id=pid)
    raise ValueError(f"unsupported AKMON operator: {op!r}")


def lower_akmon_calls(
    calls: Sequence[AKMONCall],
    *,
    env: IRExecutionEnv | None = None,
    program_id: str = "akmon:session",
) -> tuple[IRProgram, ...]:
    return tuple(
        lower_akmon_call(call, env=env, program_id=f"{program_id}:{index}")
        for index, call in enumerate(calls)
    )


def _search(call: AKMONCall, *, env: IRExecutionEnv | None, program_id: str) -> IRProgram:
    return IRProgram(
        program_id=program_id,
        env=env,
        ops=(
            IRBinding(
                output=IRSymbol("index", RECORD),
                op=IROp(
                    IROpKind.READ,
                    target="index",
                    args={
                        "kind": "index",
                        "query": call.args.get("q") or call.args.get("query"),
                        "tau": call.args.get("tau"),
                        "k": call.args.get("k"),
                    },
                ),
            ),
            IRBinding(
                output=IRSymbol("results", NODE_SET),
                op=IROp(
                    IROpKind.APPLY,
                    args={
                        "function": "akmon.search",
                        "query": call.args.get("q") or call.args.get("query"),
                        "tau": call.args.get("tau"),
                        "k": call.args.get("k"),
                    },
                ),
                inputs=("index",),
            ),
        ),
        outputs=("results",),
    )


def _inspect(call: AKMONCall, *, env: IRExecutionEnv | None, program_id: str) -> IRProgram:
    return IRProgram(
        program_id=program_id,
        env=env,
        ops=(
            IRBinding(
                output=IRSymbol("node", NODE),
                op=IROp(
                    IROpKind.READ,
                    target="node",
                    args={"node_id": call.args.get("n") or call.args.get("node_id")},
                ),
            ),
        ),
        outputs=("node",),
    )


def _expand(call: AKMONCall, *, env: IRExecutionEnv | None, program_id: str) -> IRProgram:
    return IRProgram(
        program_id=program_id,
        env=env,
        inputs=(IRSymbol("node", NODE),),
        ops=(
            IRBinding(
                output=IRSymbol("edges", EDGE_SET),
                op=IROp(
                    IROpKind.WALK,
                    args={
                        "edge_type": call.args.get("edge_type"),
                        "edge_types": call.args.get("edge_types"),
                        "direction": call.args.get("direction", "out"),
                        "predicate": call.args.get("predicate"),
                    },
                ),
                inputs=("node",),
            ),
        ),
        outputs=("edges",),
    )


def _filter(call: AKMONCall, *, env: IRExecutionEnv | None, program_id: str) -> IRProgram:
    return IRProgram(
        program_id=program_id,
        env=env,
        inputs=(IRSymbol("items", NODE_SET),),
        ops=(
            IRBinding(
                output=IRSymbol("filtered", NODE_SET),
                op=IROp(
                    IROpKind.APPLY,
                    args={
                        "function": call.args.get("predicate") or "akmon.filter",
                        "predicate_args": dict(call.args),
                    },
                ),
                inputs=("items",),
            ),
        ),
        outputs=("filtered",),
    )


def _map(call: AKMONCall, *, env: IRExecutionEnv | None, program_id: str) -> IRProgram:
    return IRProgram(
        program_id=program_id,
        env=env,
        inputs=(IRSymbol("items", NODE_SET),),
        blocks=(
            IRBlock(
                "entry",
                terminator=IRTerminator.for_each(
                    collection="items",
                    item_symbol=IRSymbol("item", NODE),
                    body_block="map_item",
                    known_cardinality=int(call.args["known_cardinality"])
                    if "known_cardinality" in call.args
                    else None,
                    max_items=int(call.args["max_items"])
                    if "max_items" in call.args
                    else None,
                    output_policy=IRForEachOutputPolicy.COLLECT,
                ),
            ),
            IRBlock(
                "map_item",
                inputs=(IRSymbol("item", NODE),),
                bindings=(
                    IRBinding(
                        output=IRSymbol("mapped", RECORD),
                        op=IROp(
                            IROpKind.APPLY,
                            args={
                                "function": call.args.get("function") or "akmon.map",
                            },
                        ),
                        inputs=("item",),
                    ),
                ),
                terminator=IRTerminator.return_(("mapped",)),
            ),
        ),
    )


def _aggregate(call: AKMONCall, *, env: IRExecutionEnv | None, program_id: str) -> IRProgram:
    return IRProgram(
        program_id=program_id,
        env=env,
        inputs=(IRSymbol("items", NODE_SET),),
        ops=(
            IRBinding(
                output=IRSymbol("aggregate", SCALAR),
                op=IROp(
                    IROpKind.APPLY,
                    args={
                        "function": call.args.get("aggregate") or "akmon.aggregate",
                        "aggregate_args": dict(call.args),
                    },
                ),
                inputs=("items",),
            ),
        ),
        outputs=("aggregate",),
    )


def _propose(call: AKMONCall, *, env: IRExecutionEnv | None, program_id: str) -> IRProgram:
    return IRProgram(
        program_id=program_id,
        env=env,
        blocks=(
            IRBlock(
                "entry",
                bindings=(
                    IRBinding(
                        output=IRSymbol("proposal", PROPOSAL),
                        op=IROp(
                            IROpKind.EDIT,
                            args={
                                "kind": "proposal_envelope",
                                "actor": call.args.get("actor"),
                                "intent": call.args.get("intent"),
                                "target": call.args.get("target"),
                                "payload": call.args.get("payload"),
                                "spec": call.args.get("spec"),
                                "radius": call.args.get("radius"),
                            },
                        ),
                    ),
                    IRBinding(
                        output=IRSymbol("admission_ok", IRType("bool")),
                        op=IROp(
                            IROpKind.APPLY,
                            args={"function": "akmon.admission_predicate"},
                        ),
                        inputs=("proposal",),
                    ),
                ),
                terminator=IRTerminator.if_(
                    condition="admission_ok",
                    then_block="admit",
                    else_block="reject",
                ),
            ),
            IRBlock(
                "admit",
                bindings=(
                    IRBinding(
                        output=IRSymbol("admit_verdict", RECORD),
                        op=IROp(
                            IROpKind.ATOMIC_EDIT,
                            args={"kind": "admitted_fact", "proposal": "$proposal"},
                        ),
                        inputs=("proposal",),
                    ),
                ),
                terminator=IRTerminator.return_(("admit_verdict",)),
            ),
            IRBlock(
                "reject",
                bindings=(
                    IRBinding(
                        output=IRSymbol("reject_verdict", RECORD),
                        op=IROp(
                            IROpKind.EDIT,
                            args={"kind": "rejected", "proposal": "$proposal"},
                        ),
                        inputs=("proposal",),
                    ),
                ),
                terminator=IRTerminator.return_(("reject_verdict",)),
            ),
        ),
        effects=(IREffect.GRAPH_DELTA,),
    )
