"""General optimizer IR operations and typed graph programs.

The seven operations mirror the research framework's graph IR. Domain
adapters can lower richer actions into these operations without expanding
the optimizer core.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any

from .graph import OptimizerEdge, OptimizerGraph, OptimizerNode


class IRValidationError(ValueError):
    """Raised when an IR program is not a closed, typed graph program."""


class IROpKind(str, Enum):
    READ = "READ"
    EDIT = "EDIT"
    WALK = "WALK"
    APPLY = "APPLY"
    ATOMIC_EDIT = "ATOMIC_EDIT"
    EMIT_EXTERNAL = "EMIT_EXTERNAL"
    RANDOM = "RANDOM"


class IREffect(str, Enum):
    READ_ONLY = "read_only"
    GRAPH_DELTA = "graph_delta"
    EXTERNAL = "external"
    RANDOM = "random"


class IRDeterminism(str, Enum):
    DETERMINISTIC = "deterministic"
    STOCHASTIC = "stochastic"
    EXTERNAL_OBSERVED = "external_observed"


class IRValueKind(str, Enum):
    NODE_REF = "node_ref"
    EDGE_REF = "edge_ref"
    FACT_REF = "fact_ref"
    ARTIFACT_REF = "artifact_ref"
    SCALAR = "scalar"
    RECORD = "record"
    COLLECTION = "collection"
    GRAPH_DELTA = "graph_delta"
    OBSERVATION = "observation"
    EXTERNAL_RESULT = "external_result"
    ERROR_VALUE = "error_value"


class IRTerminatorKind(str, Enum):
    RETURN = "RETURN"
    IF = "IF"
    FOR_EACH = "FOR_EACH"


class IRForEachOutputPolicy(str, Enum):
    COLLECT = "collect"
    REDUCE = "reduce"
    EMIT_DELTA = "emit_delta"
    NONE = "none"


@dataclass(frozen=True)
class IRType:
    """Small structural type used by optimizer IR programs."""

    name: str
    params: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        if not self.name:
            raise IRValidationError("IRType.name must be non-empty")
        object.__setattr__(self, "params", _frozen_mapping(self.params))

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "params": _canonical_value(self.params)}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "IRType":
        return cls(
            name=str(raw["name"]),
            params=_mapping_arg(raw.get("params")),
        )


@dataclass(frozen=True)
class IRExecutionEnv:
    """Pinned execution context for replay, scoring, and evidence."""

    generation_id: str | int | None = None
    branch_id: str | None = None
    catalog_version_id: str | int | None = None
    actor_id: str | None = None
    deployment_id: str | None = None
    trace_id: str | None = None
    replay_id: str | None = None
    budget: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        object.__setattr__(self, "budget", _frozen_mapping(self.budget))

    def to_dict(self) -> dict[str, Any]:
        return {
            "generation_id": self.generation_id,
            "branch_id": self.branch_id,
            "catalog_version_id": self.catalog_version_id,
            "actor_id": self.actor_id,
            "deployment_id": self.deployment_id,
            "trace_id": self.trace_id,
            "replay_id": self.replay_id,
            "budget": _canonical_value(self.budget),
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any] | None) -> "IRExecutionEnv | None":
        if raw is None:
            return None
        return cls(
            generation_id=raw.get("generation_id"),
            branch_id=_optional_str(raw.get("branch_id")),
            catalog_version_id=raw.get("catalog_version_id"),
            actor_id=_optional_str(raw.get("actor_id")),
            deployment_id=_optional_str(raw.get("deployment_id")),
            trace_id=_optional_str(raw.get("trace_id")),
            replay_id=_optional_str(raw.get("replay_id")),
            budget=_mapping_arg(raw.get("budget")),
        )


@dataclass(frozen=True)
class IRSymbol:
    """Named SSA value, program input, program output, or parameter."""

    name: str
    type: IRType

    def __post_init__(self) -> None:
        if not self.name:
            raise IRValidationError("IRSymbol.name must be non-empty")

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "type": self.type.to_dict()}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "IRSymbol":
        return cls(name=str(raw["name"]), type=IRType.from_dict(_mapping_arg(raw["type"])))


@dataclass(frozen=True)
class IROp:
    kind: IROpKind
    target: str | None = None
    args: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", IROpKind(self.kind))
        object.__setattr__(self, "args", _frozen_mapping(self.args))

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind.value,
            "target": self.target,
            "args": _canonical_value(self.args),
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "IROp":
        return cls(
            kind=IROpKind(str(raw["kind"])),
            target=_optional_str(raw.get("target")),
            args=_mapping_arg(raw.get("args")),
        )


@dataclass(frozen=True)
class IRFunctionSpec:
    """Registered function contract for APPLY operations.

    Function specs are the safety boundary for IR universality: adapters
    can express arbitrary graph functions by lowering them to registered
    functions, but optimizer programs cannot carry opaque executable
    payloads.
    """

    function_id: str
    input_schema: Mapping[str, IRType]
    output_schema: IRType
    effect: IREffect
    determinism: IRDeterminism
    cardinality_estimator: str
    cost_estimator: str
    quality_contract: str
    attrs: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        if not self.function_id:
            raise IRValidationError("IRFunctionSpec.function_id must be non-empty")
        for name, schema in self.input_schema.items():
            if not name:
                raise IRValidationError("IRFunctionSpec input names must be non-empty")
            if not isinstance(schema, IRType):
                raise IRValidationError(
                    f"IRFunctionSpec {self.function_id!r} has non-IRType input"
                )
        if not isinstance(self.output_schema, IRType):
            raise IRValidationError(
                f"IRFunctionSpec {self.function_id!r} must declare output_schema"
            )
        object.__setattr__(
            self,
            "input_schema",
            MappingProxyType(dict(sorted(self.input_schema.items()))),
        )
        object.__setattr__(self, "effect", IREffect(self.effect))
        object.__setattr__(self, "determinism", IRDeterminism(self.determinism))
        object.__setattr__(self, "attrs", _frozen_mapping(self.attrs))
        for field_name in (
            "cardinality_estimator",
            "cost_estimator",
            "quality_contract",
        ):
            if not getattr(self, field_name):
                raise IRValidationError(
                    f"IRFunctionSpec {self.function_id!r} must declare {field_name}"
                )

    def to_dict(self) -> dict[str, Any]:
        return {
            "function_id": self.function_id,
            "input_schema": {
                name: schema.to_dict() for name, schema in self.input_schema.items()
            },
            "output_schema": self.output_schema.to_dict(),
            "effect": self.effect.value,
            "determinism": self.determinism.value,
            "cardinality_estimator": self.cardinality_estimator,
            "cost_estimator": self.cost_estimator,
            "quality_contract": self.quality_contract,
            "attrs": _canonical_value(self.attrs),
        }

    def checksum(self) -> str:
        return _stable_checksum(self.to_dict())

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "IRFunctionSpec":
        input_schema = {
            str(name): IRType.from_dict(_mapping_arg(schema))
            for name, schema in _mapping_arg(raw.get("input_schema")).items()
        }
        return cls(
            function_id=str(raw["function_id"]),
            input_schema=input_schema,
            output_schema=IRType.from_dict(_mapping_arg(raw["output_schema"])),
            effect=IREffect(str(raw["effect"])),
            determinism=IRDeterminism(str(raw["determinism"])),
            cardinality_estimator=str(raw["cardinality_estimator"]),
            cost_estimator=str(raw["cost_estimator"]),
            quality_contract=str(raw["quality_contract"]),
            attrs=_mapping_arg(raw.get("attrs")),
        )


@dataclass(frozen=True)
class IRFunction:
    spec: IRFunctionSpec
    implementation: Callable[..., Any] | None = None


@dataclass(frozen=True)
class IRFunctionRegistry:
    functions: Mapping[str, IRFunction] = field(
        default_factory=lambda: MappingProxyType({})
    )
    registry_id: str = "default"
    version: str = "v0"

    def __post_init__(self) -> None:
        normalized: dict[str, IRFunction] = {}
        for function_id, function in self.functions.items():
            if function_id != function.spec.function_id:
                raise IRValidationError(
                    f"registry key {function_id!r} does not match function spec "
                    f"{function.spec.function_id!r}"
                )
            normalized[function_id] = function
        object.__setattr__(self, "functions", MappingProxyType(normalized))

    @classmethod
    def from_specs(
        cls,
        specs: tuple[IRFunctionSpec, ...],
        *,
        registry_id: str = "default",
        version: str = "v0",
    ) -> "IRFunctionRegistry":
        return cls(
            {
                spec.function_id: IRFunction(spec=spec)
                for spec in specs
            },
            registry_id=registry_id,
            version=version,
        )

    def specs(self) -> Mapping[str, IRFunctionSpec]:
        return MappingProxyType(
            {function_id: fn.spec for function_id, fn in self.functions.items()}
        )

    def register(
        self,
        spec: IRFunctionSpec,
        implementation: Callable[..., Any] | None = None,
    ) -> "IRFunctionRegistry":
        functions = dict(self.functions)
        functions[spec.function_id] = IRFunction(spec=spec, implementation=implementation)
        return IRFunctionRegistry(
            functions,
            registry_id=self.registry_id,
            version=self.version,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "registry_id": self.registry_id,
            "version": self.version,
            "functions": {
                function_id: function.spec.to_dict()
                for function_id, function in sorted(self.functions.items())
            },
        }

    def checksum(self) -> str:
        return _stable_checksum(self.to_dict())


@dataclass(frozen=True)
class IRBinding:
    """One SSA-style operation binding inside an IR program."""

    output: IRSymbol | None
    op: IROp
    inputs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "inputs", tuple(self.inputs))
        if not isinstance(self.op, IROp):
            raise IRValidationError("IRBinding.op must be an IROp")
        if self.output is not None and not isinstance(self.output, IRSymbol):
            raise IRValidationError("IRBinding.output must be an IRSymbol or None")
        for input_name in self.inputs:
            if not input_name:
                raise IRValidationError("IRBinding inputs must be non-empty names")

    def to_dict(self) -> dict[str, Any]:
        return {
            "output": None if self.output is None else self.output.to_dict(),
            "op": self.op.to_dict(),
            "inputs": list(self.inputs),
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "IRBinding":
        output = raw.get("output")
        return cls(
            output=(
                None
                if output is None
                else IRSymbol.from_dict(_mapping_arg(output))
            ),
            op=IROp.from_dict(_mapping_arg(raw["op"])),
            inputs=tuple(str(item) for item in raw.get("inputs", ())),
        )


@dataclass(frozen=True)
class IRTerminator:
    """Structured v1 control-flow terminator."""

    kind: IRTerminatorKind
    args: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", IRTerminatorKind(self.kind))
        object.__setattr__(self, "args", _frozen_mapping(self.args))
        _canonical_value(self.args)

    @classmethod
    def return_(cls, values: tuple[str, ...] = ()) -> "IRTerminator":
        return cls(IRTerminatorKind.RETURN, {"values": values})

    @classmethod
    def if_(
        cls,
        *,
        condition: str,
        then_block: str,
        else_block: str,
    ) -> "IRTerminator":
        return cls(
            IRTerminatorKind.IF,
            {
                "condition": condition,
                "then_block": then_block,
                "else_block": else_block,
            },
        )

    @classmethod
    def for_each(
        cls,
        *,
        collection: str,
        item_symbol: IRSymbol,
        body_block: str,
        output_policy: IRForEachOutputPolicy,
        max_items: int | None = None,
        known_cardinality: int | None = None,
        order_policy: str = "stable",
        accumulator: str | None = None,
    ) -> "IRTerminator":
        args: dict[str, Any] = {
            "collection": collection,
            "item_symbol": item_symbol,
            "body_block": body_block,
            "output_policy": output_policy,
            "order_policy": order_policy,
        }
        if max_items is not None:
            args["max_items"] = max_items
        if known_cardinality is not None:
            args["known_cardinality"] = known_cardinality
        if accumulator is not None:
            args["accumulator"] = accumulator
        return cls(IRTerminatorKind.FOR_EACH, args)

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind.value, "args": _canonical_value(self.args)}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "IRTerminator":
        args = dict(_mapping_arg(raw.get("args")))
        if "item_symbol" in args:
            args["item_symbol"] = IRSymbol.from_dict(_mapping_arg(args["item_symbol"]))
        if "output_policy" in args:
            args["output_policy"] = IRForEachOutputPolicy(str(args["output_policy"]))
        return cls(kind=IRTerminatorKind(str(raw["kind"])), args=args)


@dataclass(frozen=True)
class IRBlock:
    """Structured block containing SSA bindings and one terminator."""

    block_id: str
    bindings: tuple[IRBinding, ...] = ()
    terminator: IRTerminator = field(default_factory=IRTerminator.return_)
    inputs: tuple[IRSymbol, ...] = ()

    def __post_init__(self) -> None:
        if not self.block_id:
            raise IRValidationError("IRBlock.block_id must be non-empty")
        object.__setattr__(self, "bindings", tuple(self.bindings))
        object.__setattr__(self, "inputs", tuple(self.inputs))
        if not isinstance(self.terminator, IRTerminator):
            raise IRValidationError("IRBlock.terminator must be an IRTerminator")
        for binding in self.bindings:
            if not isinstance(binding, IRBinding):
                raise IRValidationError("IRBlock.bindings must contain IRBinding values")
        _require_unique_symbols(self.inputs, label=f"block {self.block_id} inputs")

    def to_dict(self) -> dict[str, Any]:
        return {
            "block_id": self.block_id,
            "inputs": [symbol.to_dict() for symbol in self.inputs],
            "bindings": [binding.to_dict() for binding in self.bindings],
            "terminator": self.terminator.to_dict(),
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "IRBlock":
        return cls(
            block_id=str(raw["block_id"]),
            inputs=tuple(
                IRSymbol.from_dict(_mapping_arg(symbol))
                for symbol in raw.get("inputs", ())
            ),
            bindings=tuple(
                IRBinding.from_dict(_mapping_arg(binding))
                for binding in raw.get("bindings", ())
            ),
            terminator=IRTerminator.from_dict(
                _mapping_arg(
                    raw.get(
                        "terminator",
                        IRTerminator.return_().to_dict(),
                    )
                )
            ),
        )


@dataclass(frozen=True)
class IRProgram:
    """Typed, serializable graph program used by optimizer rewrites."""

    program_id: str
    params: tuple[IRSymbol, ...] = ()
    inputs: tuple[IRSymbol, ...] = ()
    ops: tuple[IRBinding, ...] = ()
    blocks: tuple[IRBlock, ...] = ()
    entry_block: str | None = None
    outputs: tuple[str, ...] = ()
    env: IRExecutionEnv | None = None
    effects: tuple[IREffect, ...] = (IREffect.READ_ONLY,)
    determinism: IRDeterminism = IRDeterminism.DETERMINISTIC
    provenance_policy: str = "preserve"
    attrs: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        if not self.program_id:
            raise IRValidationError("IRProgram.program_id must be non-empty")
        object.__setattr__(self, "params", tuple(self.params))
        object.__setattr__(self, "inputs", tuple(self.inputs))
        object.__setattr__(self, "ops", tuple(self.ops))
        object.__setattr__(self, "blocks", tuple(self.blocks))
        object.__setattr__(self, "outputs", tuple(self.outputs))
        object.__setattr__(
            self,
            "effects",
            tuple(dict.fromkeys(IREffect(effect) for effect in self.effects)),
        )
        object.__setattr__(self, "determinism", IRDeterminism(self.determinism))
        if self.env is not None and not isinstance(self.env, IRExecutionEnv):
            raise IRValidationError("IRProgram.env must be an IRExecutionEnv")
        object.__setattr__(self, "attrs", _frozen_mapping(self.attrs))
        self.validate()

    def validate(
        self,
        functions: Mapping[str, IRFunctionSpec] | IRFunctionRegistry | None = None,
    ) -> None:
        function_specs = _function_specs(functions)
        _require_unique_symbols(self.params, label="params")
        _require_unique_symbols(self.inputs, label="inputs")
        defined = {symbol.name for symbol in (*self.params, *self.inputs)}
        if len(defined) != len(self.params) + len(self.inputs):
            raise IRValidationError("IRProgram params and inputs must not overlap")

        effects = set(self.effects)
        if not effects:
            raise IRValidationError("IRProgram.effects must be non-empty")
        if IREffect.RANDOM in effects and self.determinism is IRDeterminism.DETERMINISTIC:
            raise IRValidationError("random programs cannot be deterministic")

        if self.ops and self.blocks:
            raise IRValidationError("IRProgram cannot mix legacy ops and blocks")

        symbol_types = {symbol.name: symbol.type for symbol in (*self.params, *self.inputs)}
        if self.ops:
            self._validate_bindings(
                self.ops,
                defined=defined,
                symbol_types=symbol_types,
                functions=function_specs,
                effects=effects,
            )

        if self.blocks:
            self._validate_blocks(
                defined=defined,
                symbol_types=symbol_types,
                functions=function_specs,
                effects=effects,
            )

        missing_outputs = set(self.outputs) - defined
        if missing_outputs:
            raise IRValidationError(
                "IRProgram outputs reference undefined symbols: "
                + ", ".join(sorted(missing_outputs))
            )
        _canonical_value(self.attrs)
        for binding in self.ops:
            binding.to_dict()
        for block in self.blocks:
            block.to_dict()

    def to_dict(self) -> dict[str, Any]:
        return {
            "program_id": self.program_id,
            "params": [symbol.to_dict() for symbol in self.params],
            "inputs": [symbol.to_dict() for symbol in self.inputs],
            "ops": [binding.to_dict() for binding in self.ops],
            "blocks": [block.to_dict() for block in self.blocks],
            "entry_block": self.entry_block,
            "outputs": list(self.outputs),
            "env": None if self.env is None else self.env.to_dict(),
            "effects": [effect.value for effect in self.effects],
            "determinism": self.determinism.value,
            "provenance_policy": self.provenance_policy,
            "attrs": _canonical_value(self.attrs),
        }

    def to_json(self) -> str:
        return _stable_json(self.to_dict())

    def checksum(self) -> str:
        return _stable_checksum(self.to_dict())

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "IRProgram":
        return cls(
            program_id=str(raw["program_id"]),
            params=tuple(
                IRSymbol.from_dict(_mapping_arg(symbol))
                for symbol in raw.get("params", ())
            ),
            inputs=tuple(
                IRSymbol.from_dict(_mapping_arg(symbol))
                for symbol in raw.get("inputs", ())
            ),
            ops=tuple(
                IRBinding.from_dict(_mapping_arg(binding))
                for binding in raw.get("ops", ())
            ),
            blocks=tuple(
                IRBlock.from_dict(_mapping_arg(block))
                for block in raw.get("blocks", ())
            ),
            entry_block=_optional_str(raw.get("entry_block")),
            outputs=tuple(str(output) for output in raw.get("outputs", ())),
            env=IRExecutionEnv.from_dict(raw.get("env")),  # type: ignore[arg-type]
            effects=tuple(
                IREffect(str(effect))
                for effect in raw.get("effects", (IREffect.READ_ONLY.value,))
            ),
            determinism=IRDeterminism(
                str(raw.get("determinism", IRDeterminism.DETERMINISTIC.value))
            ),
            provenance_policy=str(raw.get("provenance_policy", "preserve")),
            attrs=_mapping_arg(raw.get("attrs")),
        )

    def _validate_bindings(
        self,
        bindings: tuple[IRBinding, ...],
        *,
        defined: set[str],
        symbol_types: dict[str, IRType],
        functions: Mapping[str, IRFunctionSpec] | None,
        effects: set[IREffect],
    ) -> None:
        for binding in bindings:
            if not isinstance(binding, IRBinding):
                raise IRValidationError("IRProgram bindings must contain IRBinding values")
            missing_inputs = set(binding.inputs) - defined
            if missing_inputs:
                raise IRValidationError(
                    "IRBinding references undefined inputs: "
                    + ", ".join(sorted(missing_inputs))
                )
            _validate_op_effect(binding.op, effects)
            self._validate_apply_function(binding.op, functions, effects)
            if binding.output is not None:
                if binding.output.name in defined:
                    raise IRValidationError(
                        f"IRProgram redefines symbol {binding.output.name!r}"
                    )
                defined.add(binding.output.name)
                symbol_types[binding.output.name] = binding.output.type

    def _validate_blocks(
        self,
        *,
        defined: set[str],
        symbol_types: dict[str, IRType],
        functions: Mapping[str, IRFunctionSpec] | None,
        effects: set[IREffect],
    ) -> None:
        block_by_id = {block.block_id: block for block in self.blocks}
        if len(block_by_id) != len(self.blocks):
            raise IRValidationError("IRProgram block ids must be unique")
        entry = self.entry_block or self.blocks[0].block_id
        if entry not in block_by_id:
            raise IRValidationError(f"IRProgram entry_block {entry!r} is missing")

        for block in self.blocks:
            block_defined = set(defined)
            for symbol in block.inputs:
                if symbol.name in block_defined:
                    raise IRValidationError(
                        f"IRBlock {block.block_id!r} input {symbol.name!r} "
                        "redefines an existing symbol"
                    )
                block_defined.add(symbol.name)
                symbol_types[symbol.name] = symbol.type
            self._validate_bindings(
                block.bindings,
                defined=block_defined,
                symbol_types=symbol_types,
                functions=functions,
                effects=effects,
            )
            defined.update(block_defined - defined)

        for block in self.blocks:
            self._validate_terminator(
                block.terminator,
                block_by_id=block_by_id,
                symbol_types=symbol_types,
            )

    def _validate_apply_function(
        self,
        op: IROp,
        functions: Mapping[str, IRFunctionSpec] | None,
        effects: set[IREffect],
    ) -> None:
        if op.kind is not IROpKind.APPLY:
            return
        function_id = op.args.get("function")
        if not isinstance(function_id, str) or not function_id:
            raise IRValidationError("APPLY op must declare args['function']")
        if functions is None:
            return
        if function_id not in functions:
            raise IRValidationError(f"APPLY references unregistered function {function_id!r}")
        function = functions[function_id]
        if not isinstance(function, IRFunctionSpec):
            raise IRValidationError(f"registered function {function_id!r} is invalid")
        if function.effect is not IREffect.READ_ONLY and function.effect not in effects:
            raise IRValidationError(
                f"APPLY function {function_id!r} requires program effect "
                f"{function.effect.value!r}"
            )
        if (
            function.determinism is IRDeterminism.STOCHASTIC
            and self.determinism is IRDeterminism.DETERMINISTIC
        ):
            raise IRValidationError(
                f"APPLY function {function_id!r} is stochastic but program is "
                "deterministic"
            )

    def _validate_terminator(
        self,
        terminator: IRTerminator,
        *,
        block_by_id: Mapping[str, IRBlock],
        symbol_types: Mapping[str, IRType],
    ) -> None:
        if terminator.kind is IRTerminatorKind.RETURN:
            for value in _terminator_values(terminator):
                if value not in symbol_types:
                    raise IRValidationError(
                        f"RETURN references undefined symbol {value!r}"
                    )
            return

        if terminator.kind is IRTerminatorKind.IF:
            condition = terminator.args.get("condition")
            then_block = terminator.args.get("then_block")
            else_block = terminator.args.get("else_block")
            if condition not in symbol_types:
                raise IRValidationError(
                    f"IF references undefined condition {condition!r}"
                )
            if then_block not in block_by_id:
                raise IRValidationError(f"IF references missing then_block {then_block!r}")
            if else_block not in block_by_id:
                raise IRValidationError(f"IF references missing else_block {else_block!r}")
            then_types = _return_types(block_by_id[str(then_block)], symbol_types)
            else_types = _return_types(block_by_id[str(else_block)], symbol_types)
            if then_types is not None and else_types is not None and then_types != else_types:
                raise IRValidationError("IF branch return types must match")
            return

        if terminator.kind is IRTerminatorKind.FOR_EACH:
            collection = terminator.args.get("collection")
            body_block = terminator.args.get("body_block")
            item_symbol = terminator.args.get("item_symbol")
            output_policy = terminator.args.get("output_policy")
            max_items = terminator.args.get("max_items")
            known_cardinality = terminator.args.get("known_cardinality")
            if collection not in symbol_types:
                raise IRValidationError(
                    f"FOR_EACH references undefined collection {collection!r}"
                )
            if body_block not in block_by_id:
                raise IRValidationError(
                    f"FOR_EACH references missing body_block {body_block!r}"
                )
            if not isinstance(item_symbol, IRSymbol):
                raise IRValidationError("FOR_EACH must declare an item_symbol")
            body = block_by_id[str(body_block)]
            body_inputs = {symbol.name: symbol.type for symbol in body.inputs}
            if body_inputs.get(item_symbol.name) != item_symbol.type:
                raise IRValidationError(
                    "FOR_EACH item_symbol must match a body_block input"
                )
            if output_policy is None:
                raise IRValidationError("FOR_EACH must declare output_policy")
            policy = IRForEachOutputPolicy(output_policy)
            if max_items is None and known_cardinality is None:
                raise IRValidationError(
                    "FOR_EACH must declare max_items or known_cardinality"
                )
            if max_items is not None and (not isinstance(max_items, int) or max_items <= 0):
                raise IRValidationError("FOR_EACH max_items must be a positive int")
            if known_cardinality is not None and (
                not isinstance(known_cardinality, int) or known_cardinality < 0
            ):
                raise IRValidationError(
                    "FOR_EACH known_cardinality must be a non-negative int"
                )
            accumulator = terminator.args.get("accumulator")
            if policy is IRForEachOutputPolicy.REDUCE and accumulator not in symbol_types:
                raise IRValidationError(
                    "FOR_EACH reduce output_policy requires a typed accumulator"
                )
            if policy is IRForEachOutputPolicy.EMIT_DELTA and IREffect.GRAPH_DELTA not in set(
                self.effects
            ):
                raise IRValidationError(
                    "FOR_EACH emit_delta output_policy requires graph_delta effect"
                )
            return

        raise IRValidationError(f"unsupported terminator {terminator.kind.value!r}")


@dataclass(frozen=True)
class IRPlan:
    ops: tuple[IROp, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "ops", tuple(self.ops))

    def append(self, op: IROp) -> "IRPlan":
        return IRPlan((*self.ops, op))

    def to_program(
        self,
        *,
        program_id: str,
        effects: tuple[IREffect, ...] = (IREffect.READ_ONLY,),
        determinism: IRDeterminism = IRDeterminism.DETERMINISTIC,
    ) -> IRProgram:
        """Wrap legacy linear ops in a typed program with `any` outputs."""

        any_type = IRType("any")
        bindings = tuple(
            IRBinding(
                output=IRSymbol(f"op_{index}", any_type),
                op=op,
                inputs=() if index == 0 else (f"op_{index - 1}",),
            )
            for index, op in enumerate(self.ops)
        )
        outputs = () if not bindings else (f"op_{len(bindings) - 1}",)
        return IRProgram(
            program_id=program_id,
            ops=bindings,
            outputs=outputs,
            effects=effects,
            determinism=determinism,
        )


@dataclass(frozen=True)
class IRProgramResult:
    values: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    graph: OptimizerGraph | None = None
    graph_delta: tuple[OptimizerNode | OptimizerEdge, ...] = ()
    observations: tuple[Mapping[str, Any], ...] = ()
    cost_components: Mapping[str, float] = field(
        default_factory=lambda: MappingProxyType({})
    )
    quality_components: Mapping[str, float] = field(
        default_factory=lambda: MappingProxyType({})
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "values", MappingProxyType(dict(self.values)))
        object.__setattr__(self, "graph_delta", tuple(self.graph_delta))
        object.__setattr__(
            self,
            "observations",
            tuple(MappingProxyType(dict(obs)) for obs in self.observations),
        )
        object.__setattr__(
            self,
            "cost_components",
            MappingProxyType(dict(self.cost_components)),
        )
        object.__setattr__(
            self,
            "quality_components",
            MappingProxyType(dict(self.quality_components)),
        )


@dataclass(frozen=True)
class IRCostStaticAnalysis:
    op_counts: Mapping[str, int]
    function_counts: Mapping[str, int]
    max_iterations: int
    effects: tuple[IREffect, ...]
    observations: tuple[Mapping[str, Any], ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "op_counts", MappingProxyType(dict(self.op_counts)))
        object.__setattr__(
            self,
            "function_counts",
            MappingProxyType(dict(self.function_counts)),
        )
        object.__setattr__(self, "effects", tuple(self.effects))
        object.__setattr__(
            self,
            "observations",
            tuple(MappingProxyType(dict(obs)) for obs in self.observations),
        )


class IRInterpreter:
    """Small deterministic interpreter/static analyzer for optimizer IR."""

    def __init__(self, registry: IRFunctionRegistry | None = None) -> None:
        self.registry = registry or IRFunctionRegistry()

    def analyze(self, program: IRProgram) -> IRCostStaticAnalysis:
        program.validate(self.registry)
        op_counts: dict[str, int] = {}
        function_counts: dict[str, int] = {}
        observations: list[Mapping[str, Any]] = []
        max_iterations = 1
        for binding in program.ops:
            _count_binding(binding, op_counts, function_counts)
        for block in program.blocks:
            for binding in block.bindings:
                _count_binding(binding, op_counts, function_counts)
            if block.terminator.kind is IRTerminatorKind.FOR_EACH:
                max_items = block.terminator.args.get("max_items")
                known = block.terminator.args.get("known_cardinality")
                bound = int(max_items if max_items is not None else known)
                max_iterations = max(max_iterations, bound)
                if max_items is not None and known is not None and max_items < known:
                    observations.append(
                        {
                            "kind": "truncation_possible",
                            "block_id": block.block_id,
                            "max_items": max_items,
                            "known_cardinality": known,
                        }
                    )
        return IRCostStaticAnalysis(
            op_counts=op_counts,
            function_counts=function_counts,
            max_iterations=max_iterations,
            effects=program.effects,
            observations=tuple(observations),
        )

    def evaluate(
        self,
        program: IRProgram,
        graph: OptimizerGraph,
        env: Mapping[str, Any] | None = None,
    ) -> IRProgramResult:
        program.validate(self.registry)
        values: dict[str, Any] = dict(env or {})
        current_graph = graph
        delta: list[OptimizerNode | OptimizerEdge] = []
        observations: list[Mapping[str, Any]] = []
        costs: dict[str, float] = {}

        for symbol in (*program.params, *program.inputs):
            values.setdefault(symbol.name, None)
        if program.ops:
            self._eval_bindings(
                program.ops,
                values=values,
                graph=current_graph,
                delta=delta,
                costs=costs,
            )
            current_graph = _apply_delta(current_graph, delta)
            return IRProgramResult(
                values={name: values.get(name) for name in program.outputs},
                graph=current_graph,
                graph_delta=tuple(delta),
                observations=tuple(observations),
                cost_components=costs,
            )

        if program.blocks:
            entry = program.entry_block or program.blocks[0].block_id
            block_by_id = {block.block_id: block for block in program.blocks}
            returned = self._eval_block(
                entry,
                block_by_id=block_by_id,
                values=values,
                graph=current_graph,
                delta=delta,
                observations=observations,
                costs=costs,
            )
            current_graph = _apply_delta(current_graph, delta)
            return IRProgramResult(
                values=returned,
                graph=current_graph,
                graph_delta=tuple(delta),
                observations=tuple(observations),
                cost_components=costs,
            )

        return IRProgramResult(
            values={name: values.get(name) for name in program.outputs},
            graph=current_graph,
            graph_delta=(),
            observations=tuple(observations),
            cost_components=costs,
        )

    def _eval_block(
        self,
        block_id: str,
        *,
        block_by_id: Mapping[str, IRBlock],
        values: dict[str, Any],
        graph: OptimizerGraph,
        delta: list[OptimizerNode | OptimizerEdge],
        observations: list[Mapping[str, Any]],
        costs: dict[str, float],
    ) -> dict[str, Any]:
        block = block_by_id[block_id]
        self._eval_bindings(
            block.bindings,
            values=values,
            graph=graph,
            delta=delta,
            costs=costs,
        )
        terminator = block.terminator
        if terminator.kind is IRTerminatorKind.RETURN:
            return {name: values.get(name) for name in _terminator_values(terminator)}
        if terminator.kind is IRTerminatorKind.IF:
            condition = bool(values.get(str(terminator.args["condition"])))
            next_block = str(
                terminator.args["then_block" if condition else "else_block"]
            )
            return self._eval_block(
                next_block,
                block_by_id=block_by_id,
                values=dict(values),
                graph=graph,
                delta=delta,
                observations=observations,
                costs=costs,
            )
        if terminator.kind is IRTerminatorKind.FOR_EACH:
            collection = values.get(str(terminator.args["collection"]), ())
            if collection is None:
                collection = ()
            items = list(collection)
            max_items = terminator.args.get("max_items")
            known = terminator.args.get("known_cardinality")
            if max_items is not None and len(items) > int(max_items):
                observations.append(
                    {
                        "kind": "truncated_iteration",
                        "block_id": block_id,
                        "max_items": max_items,
                        "input_count": len(items),
                    }
                )
                items = items[: int(max_items)]
            elif max_items is not None and known is not None and int(max_items) < int(known):
                observations.append(
                    {
                        "kind": "truncation_possible",
                        "block_id": block_id,
                        "max_items": max_items,
                        "known_cardinality": known,
                    }
                )
            policy = IRForEachOutputPolicy(terminator.args["output_policy"])
            item_symbol = terminator.args["item_symbol"]
            assert isinstance(item_symbol, IRSymbol)
            body_block = str(terminator.args["body_block"])
            outputs: list[Any] = []
            for item in items:
                body_values = dict(values)
                body_values[item_symbol.name] = item
                result = self._eval_block(
                    body_block,
                    block_by_id=block_by_id,
                    values=body_values,
                    graph=graph,
                    delta=delta,
                    observations=observations,
                    costs=costs,
                )
                if policy is IRForEachOutputPolicy.COLLECT:
                    outputs.append(result)
            if policy is IRForEachOutputPolicy.COLLECT:
                return {"items": outputs}
            return {}
        raise IRValidationError(f"unsupported terminator {terminator.kind.value!r}")

    def _eval_bindings(
        self,
        bindings: tuple[IRBinding, ...],
        *,
        values: dict[str, Any],
        graph: OptimizerGraph,
        delta: list[OptimizerNode | OptimizerEdge],
        costs: dict[str, float],
    ) -> None:
        for binding in bindings:
            result = self._eval_op(binding, values=values, graph=graph, delta=delta)
            _add_cost(costs, f"op.{binding.op.kind.value}", 1.0)
            if binding.output is not None:
                values[binding.output.name] = result

    def _eval_op(
        self,
        binding: IRBinding,
        *,
        values: Mapping[str, Any],
        graph: OptimizerGraph,
        delta: list[OptimizerNode | OptimizerEdge],
    ) -> Any:
        op = binding.op
        if op.kind is IROpKind.READ:
            return _eval_read(op, values=values, graph=graph)
        if op.kind is IROpKind.WALK:
            return _eval_walk(op, binding=binding, values=values, graph=graph)
        if op.kind is IROpKind.APPLY:
            return self._eval_apply(op, binding=binding, values=values)
        if op.kind in {IROpKind.EDIT, IROpKind.ATOMIC_EDIT}:
            edit = _eval_edit(op, values=values)
            if isinstance(edit, OptimizerNode | OptimizerEdge):
                delta.append(edit)
            return edit
        if op.kind is IROpKind.EMIT_EXTERNAL:
            return {
                "status": "observed",
                "channel": op.args.get("channel"),
                "payload": op.args.get("payload"),
            }
        if op.kind is IROpKind.RANDOM:
            if "observed" in op.args:
                return op.args["observed"]
            raise IRValidationError("RANDOM evaluation requires observed value")
        raise IRValidationError(f"unsupported op {op.kind.value!r}")

    def _eval_apply(
        self,
        op: IROp,
        *,
        binding: IRBinding,
        values: Mapping[str, Any],
    ) -> Any:
        function_id = str(op.args["function"])
        function = self.registry.functions.get(function_id)
        if function is None:
            raise IRValidationError(f"APPLY references unregistered function {function_id!r}")
        args = {name: values.get(name) for name in binding.inputs}
        args.update(
            {
                key: value
                for key, value in op.args.items()
                if key != "function"
            }
        )
        if function.implementation is None:
            return {
                "function": function_id,
                "inputs": args,
            }
        return function.implementation(**args)


def _frozen_mapping(values: Mapping[str, Any] | None) -> Mapping[str, Any]:
    return MappingProxyType(dict(values or {}))


def _mapping_arg(value: Any) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise IRValidationError(
            f"expected IR mapping payload, got {type(value).__name__}"
        )
    return value


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _require_unique_symbols(symbols: tuple[IRSymbol, ...], *, label: str) -> None:
    names = [symbol.name for symbol in symbols]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise IRValidationError(
            f"IRProgram {label} contain duplicate symbols: "
            + ", ".join(duplicates)
        )


def _validate_op_effect(op: IROp, effects: set[IREffect]) -> None:
    required: IREffect | None = None
    if op.kind in {IROpKind.EDIT, IROpKind.ATOMIC_EDIT}:
        required = IREffect.GRAPH_DELTA
    elif op.kind is IROpKind.EMIT_EXTERNAL:
        required = IREffect.EXTERNAL
    elif op.kind is IROpKind.RANDOM:
        required = IREffect.RANDOM
    if required is not None and required not in effects:
        raise IRValidationError(
            f"{op.kind.value} op requires program effect {required.value!r}"
        )


def _terminator_values(terminator: IRTerminator) -> tuple[str, ...]:
    values = terminator.args.get("values", ())
    if not isinstance(values, tuple | list):
        raise IRValidationError("RETURN values must be a list or tuple")
    for value in values:
        if not isinstance(value, str) or not value:
            raise IRValidationError("RETURN values must be symbol names")
    return tuple(values)


def _return_types(
    block: IRBlock,
    symbol_types: Mapping[str, IRType],
) -> tuple[dict[str, Any], ...] | None:
    if block.terminator.kind is not IRTerminatorKind.RETURN:
        return None
    return tuple(symbol_types[value].to_dict() for value in _terminator_values(block.terminator))


def _function_specs(
    functions: Mapping[str, IRFunctionSpec] | IRFunctionRegistry | None,
) -> Mapping[str, IRFunctionSpec] | None:
    if functions is None:
        return None
    if isinstance(functions, IRFunctionRegistry):
        return functions.specs()
    return functions


def _count_binding(
    binding: IRBinding,
    op_counts: dict[str, int],
    function_counts: dict[str, int],
) -> None:
    op_counts[binding.op.kind.value] = op_counts.get(binding.op.kind.value, 0) + 1
    if binding.op.kind is IROpKind.APPLY:
        function_id = binding.op.args.get("function")
        if isinstance(function_id, str):
            function_counts[function_id] = function_counts.get(function_id, 0) + 1


def _add_cost(costs: dict[str, float], key: str, value: float) -> None:
    costs[key] = costs.get(key, 0.0) + value


def _apply_delta(
    graph: OptimizerGraph,
    delta: list[OptimizerNode | OptimizerEdge],
) -> OptimizerGraph:
    result = graph
    for item in delta:
        if isinstance(item, OptimizerNode):
            result = result.add_node(item)
        elif isinstance(item, OptimizerEdge):
            result = result.add_edge(item)
    return result


def _eval_read(
    op: IROp,
    *,
    values: Mapping[str, Any],
    graph: OptimizerGraph,
) -> Any:
    target = op.target or str(op.args.get("target", ""))
    node_id = _resolve_arg(op.args.get("node_id") or target, values)
    edge_id = _resolve_arg(op.args.get("edge_id") or target, values)
    if op.args.get("kind") == "nodes" or target == "nodes":
        return tuple(graph.nodes.values())
    if op.args.get("kind") == "edges" or target == "edges":
        return tuple(graph.edges.values())
    if node_id in graph.nodes:
        return graph.nodes[str(node_id)]
    if edge_id in graph.edges:
        return graph.edges[str(edge_id)]
    return None


def _eval_walk(
    op: IROp,
    *,
    binding: IRBinding,
    values: Mapping[str, Any],
    graph: OptimizerGraph,
) -> tuple[OptimizerEdge, ...]:
    start_value = values.get(binding.inputs[0]) if binding.inputs else None
    start_id = _node_id(start_value)
    edge_type = _resolve_arg(op.args.get("edge_type"), values)
    direction = str(op.args.get("direction", "out"))
    if direction not in {"out", "in", "both"}:
        raise IRValidationError("WALK direction must be out, in, or both")
    edges: list[OptimizerEdge] = []
    for edge in graph.edges.values():
        if edge_type is not None and edge.edge_type != edge_type:
            continue
        if start_id is None:
            edges.append(edge)
        elif direction == "out" and edge.src == start_id:
            edges.append(edge)
        elif direction == "in" and edge.dst == start_id:
            edges.append(edge)
        elif direction == "both" and (edge.src == start_id or edge.dst == start_id):
            edges.append(edge)
    return tuple(edges)


def _eval_edit(op: IROp, *, values: Mapping[str, Any]) -> Any:
    action = str(op.args.get("action") or op.args.get("kind") or "")
    if action in {"add_node", "node"}:
        node_id = str(_resolve_arg(op.args.get("node_id"), values))
        subtype = str(_resolve_arg(op.args.get("subtype"), values))
        attrs = _resolve_mapping(op.args.get("attrs", {}), values)
        return OptimizerNode(node_id=node_id, subtype=subtype, attrs=attrs)
    if action in {"add_edge", "edge"}:
        edge_id = str(
            _resolve_arg(
                op.args.get("edge_id")
                or f"{op.args.get('src')}:{op.args.get('edge_type')}:{op.args.get('dst')}",
                values,
            )
        )
        src = str(_resolve_arg(op.args.get("src"), values))
        dst = str(_resolve_arg(op.args.get("dst"), values))
        edge_type = str(_resolve_arg(op.args.get("edge_type"), values))
        attrs = _resolve_mapping(op.args.get("attrs", {}), values)
        return OptimizerEdge(edge_id=edge_id, src=src, dst=dst, edge_type=edge_type, attrs=attrs)
    return {
        key: _resolve_arg(value, values)
        for key, value in op.args.items()
    }


def _resolve_mapping(value: Any, values: Mapping[str, Any]) -> Mapping[str, Any]:
    resolved = _resolve_arg(value, values)
    if not isinstance(resolved, Mapping):
        raise IRValidationError("expected mapping value")
    return dict(resolved)


def _resolve_arg(value: Any, values: Mapping[str, Any]) -> Any:
    if isinstance(value, str) and value.startswith("$"):
        return values.get(value[1:])
    if isinstance(value, Mapping):
        return {
            str(key): _resolve_arg(item, values)
            for key, item in value.items()
        }
    if isinstance(value, tuple | list):
        return [_resolve_arg(item, values) for item in value]
    return value


def _node_id(value: Any) -> str | None:
    if isinstance(value, OptimizerNode):
        return value.node_id
    if isinstance(value, str):
        return value
    if isinstance(value, Mapping) and "node_id" in value:
        return str(value["node_id"])
    return None


def _canonical_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, IRType):
        return value.to_dict()
    if isinstance(value, IRSymbol):
        return value.to_dict()
    if isinstance(value, IRExecutionEnv):
        return value.to_dict()
    if isinstance(value, IRFunctionSpec):
        return value.to_dict()
    if isinstance(value, IRBinding):
        return value.to_dict()
    if isinstance(value, IRTerminator):
        return value.to_dict()
    if isinstance(value, IRBlock):
        return value.to_dict()
    if isinstance(value, Mapping):
        return {
            str(key): _canonical_value(value[key])
            for key in sorted(value, key=str)
        }
    if isinstance(value, tuple | list):
        return [_canonical_value(item) for item in value]
    if value is None or isinstance(value, str | int | float | bool):
        return value
    raise IRValidationError(
        f"IR values must be JSON-serializable; got {type(value).__name__}"
    )


def _stable_json(value: Any) -> str:
    return json.dumps(_canonical_value(value), sort_keys=True, separators=(",", ":"))


def _stable_checksum(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()
