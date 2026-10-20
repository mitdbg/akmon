"""Executable reference models for optimizer formal checks.

These helpers are deliberately small and pure. They are not a second
optimizer implementation; they encode the proof-facing semantics used by
property tests and substrate refinement checks.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from .ir import (
    IRBinding,
    IRForEachOutputPolicy,
    IRFunctionRegistry,
    IRProgram,
    IRTerminatorKind,
    IROpKind,
)


class IRReferenceError(ValueError):
    """Raised when a reference-model program violates the proof boundary."""


@dataclass(frozen=True)
class IRReferenceResult:
    values: Mapping[str, Any]
    graph_delta_count: int = 0
    external_count: int = 0
    random_count: int = 0
    truncation_count: int = 0
    steps: int = 0
    unsupported: tuple[str, ...] = ()


@dataclass
class _IRReferenceState:
    values: dict[str, Any] = field(default_factory=dict)
    graph_delta_count: int = 0
    external_count: int = 0
    random_count: int = 0
    truncation_count: int = 0
    steps: int = 0
    unsupported: list[str] = field(default_factory=list)


def evaluate_ir_reference(
    program: IRProgram,
    *,
    registry: IRFunctionRegistry | None = None,
    replay_read_only: bool = False,
    input_values: Mapping[str, Any] | None = None,
) -> IRReferenceResult:
    """Evaluate the formal subset of optimizer IR.

    The result is intentionally abstract. `READ`, `WALK`, and deterministic
    `APPLY` produce stable records rather than touching a real graph. The
    purpose is to prove effect discipline and bounded control flow, not to
    duplicate substrate replay.
    """

    functions = None if registry is None else registry.specs()
    program.validate(functions=functions)
    state = _IRReferenceState(values=dict(input_values or {}))
    for symbol in (*program.params, *program.inputs):
        state.values.setdefault(symbol.name, [])
    if program.ops:
        _eval_bindings(
            program.ops,
            state=state,
            registry=registry,
            replay_read_only=replay_read_only,
        )
    elif program.blocks:
        block_by_id = {block.block_id: block for block in program.blocks}
        entry = program.entry_block or program.blocks[0].block_id
        _eval_block(
            entry,
            block_by_id=block_by_id,
            state=state,
            registry=registry,
            replay_read_only=replay_read_only,
        )
    return IRReferenceResult(
        values={
            output: state.values.get(output)
            for output in program.outputs
        },
        graph_delta_count=state.graph_delta_count,
        external_count=state.external_count,
        random_count=state.random_count,
        truncation_count=state.truncation_count,
        steps=state.steps,
        unsupported=tuple(state.unsupported),
    )


def _eval_bindings(
    bindings: tuple[IRBinding, ...],
    *,
    state: _IRReferenceState,
    registry: IRFunctionRegistry | None,
    replay_read_only: bool,
) -> None:
    for binding in bindings:
        value = _eval_binding(
            binding,
            state=state,
            registry=registry,
            replay_read_only=replay_read_only,
        )
        if binding.output is not None:
            state.values[binding.output.name] = value


def _eval_block(
    block_id: str,
    *,
    block_by_id: Mapping[str, Any],
    state: _IRReferenceState,
    registry: IRFunctionRegistry | None,
    replay_read_only: bool,
) -> Mapping[str, Any]:
    block = block_by_id[block_id]
    _eval_bindings(
        block.bindings,
        state=state,
        registry=registry,
        replay_read_only=replay_read_only,
    )
    terminator = block.terminator
    if terminator.kind is IRTerminatorKind.RETURN:
        return {
            str(name): state.values.get(str(name))
            for name in terminator.args.get("values", ())
        }
    if terminator.kind is IRTerminatorKind.IF:
        condition = bool(state.values.get(str(terminator.args["condition"])))
        next_block = str(
            terminator.args["then_block" if condition else "else_block"]
        )
        return _eval_block(
            next_block,
            block_by_id=block_by_id,
            state=state,
            registry=registry,
            replay_read_only=replay_read_only,
        )
    if terminator.kind is IRTerminatorKind.FOR_EACH:
        collection = list(state.values.get(str(terminator.args["collection"]), ()) or ())
        max_items = terminator.args.get("max_items")
        known_cardinality = terminator.args.get("known_cardinality")
        if max_items is None and known_cardinality is None:
            raise IRReferenceError("FOR_EACH requires max_items or known_cardinality")
        if max_items is not None and len(collection) > int(max_items):
            state.truncation_count += 1
            collection = collection[: int(max_items)]
        policy = IRForEachOutputPolicy(terminator.args["output_policy"])
        body_block = str(terminator.args["body_block"])
        item_symbol = terminator.args["item_symbol"]
        collected: list[Any] = []
        for item in collection:
            state.values[item_symbol.name] = item
            result = _eval_block(
                body_block,
                block_by_id=block_by_id,
                state=state,
                registry=registry,
                replay_read_only=replay_read_only,
            )
            if policy is IRForEachOutputPolicy.COLLECT:
                collected.append(result)
        return {"items": collected} if policy is IRForEachOutputPolicy.COLLECT else {}
    raise IRReferenceError(f"unsupported terminator: {terminator.kind.value}")


def _eval_binding(
    binding: IRBinding,
    *,
    state: _IRReferenceState,
    registry: IRFunctionRegistry | None,
    replay_read_only: bool,
) -> Any:
    state.steps += 1
    op = binding.op
    if replay_read_only and op.kind in {IROpKind.EDIT, IROpKind.ATOMIC_EDIT}:
        raise IRReferenceError(
            f"read-only replay cannot execute {op.kind.value}"
        )
    if op.kind is IROpKind.READ:
        return {
            "op": "READ",
            "target": op.target,
            "args": dict(op.args),
        }
    if op.kind is IROpKind.WALK:
        return [
            {
                "op": "WALK",
                "input": name,
                "value": state.values.get(name),
                "args": dict(op.args),
            }
            for name in binding.inputs
        ]
    if op.kind is IROpKind.APPLY:
        function_id = str(op.args.get("function") or "")
        if registry is not None and function_id not in registry.functions:
            raise IRReferenceError(f"unregistered function: {function_id}")
        return {
            "op": "APPLY",
            "function": function_id,
            "inputs": [state.values.get(name) for name in binding.inputs],
            "args": {
                key: value for key, value in op.args.items() if key != "function"
            },
        }
    if op.kind in {IROpKind.EDIT, IROpKind.ATOMIC_EDIT}:
        state.graph_delta_count += 1
        return {
            "op": op.kind.value,
            "args": dict(op.args),
        }
    if op.kind is IROpKind.EMIT_EXTERNAL:
        state.external_count += 1
        return {
            "op": "EMIT_EXTERNAL",
            "target": op.target,
            "args": dict(op.args),
        }
    if op.kind is IROpKind.RANDOM:
        if "observed" not in op.args:
            raise IRReferenceError("RANDOM requires observed value in reference model")
        state.random_count += 1
        return op.args["observed"]
    state.unsupported.append(op.kind.value)
    raise IRReferenceError(f"unsupported op: {op.kind.value}")


@dataclass(frozen=True)
class BranchFact:
    fact_id: str
    owner_branch: str
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BranchVisibilityModel:
    """Pure branch visibility model for optimizer proof tests."""

    parent_by_branch: Mapping[str, str | None]
    default_branch: str = "default"

    def lineage(self, branch_name: str) -> tuple[str, ...]:
        if branch_name not in self.parent_by_branch:
            raise KeyError(branch_name)
        lineage: list[str] = []
        seen: set[str] = set()
        current: str | None = branch_name
        while current is not None and current not in seen:
            seen.add(current)
            lineage.append(current)
            parent = self.parent_by_branch.get(current)
            if parent == current:
                break
            current = parent
        if self.default_branch not in lineage:
            lineage.append(self.default_branch)
        return tuple(lineage)

    def visible_fact_ids(
        self,
        branch_name: str,
        facts: tuple[BranchFact, ...],
    ) -> frozenset[str]:
        visible_branches = set(self.lineage(branch_name))
        return frozenset(
            fact.fact_id
            for fact in facts
            if fact.owner_branch in visible_branches
        )

    def promote_fact(
        self,
        fact: BranchFact,
        *,
        promoted_fact_id: str,
    ) -> BranchFact:
        """Create a separate default effect for a promoted branch fact."""

        return BranchFact(
            fact_id=promoted_fact_id,
            owner_branch=self.default_branch,
            payload={
                **dict(fact.payload),
                "promoted_from_fact_id": fact.fact_id,
                "promoted_from_branch": fact.owner_branch,
            },
        )
