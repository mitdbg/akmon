"""Cost and quality model protocols for optimizer search."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Protocol

from .graph import OptimizerGraph
from .ir import IRFunctionRegistry, IRInterpreter, IRProgram


@dataclass(frozen=True)
class CostEstimate:
    total: float
    components: Mapping[str, float] = field(
        default_factory=lambda: MappingProxyType({})
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "components", MappingProxyType(dict(self.components)))


@dataclass(frozen=True)
class QualityEstimate:
    score: float
    components: Mapping[str, float] = field(
        default_factory=lambda: MappingProxyType({})
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "components", MappingProxyType(dict(self.components)))


class CostModel(Protocol):
    def estimate(self, graph: OptimizerGraph) -> CostEstimate:
        """Return the estimated cost for a graph state."""


class QualityModel(Protocol):
    def estimate(self, graph: OptimizerGraph) -> QualityEstimate:
        """Return the estimated quality for a graph state."""


@dataclass(frozen=True)
class IRCostModel:
    """Static IR cost model with decomposed op/function components."""

    registry: IRFunctionRegistry
    op_costs: Mapping[str, float] = field(default_factory=lambda: MappingProxyType({}))
    function_costs: Mapping[str, float] = field(
        default_factory=lambda: MappingProxyType({})
    )
    iteration_cost: float = 0.0
    cost_hint_weight: float = 1.0

    def __post_init__(self) -> None:
        _validate_registry(self.registry)
        if not 0.0 <= self.cost_hint_weight <= 1.0:
            raise ValueError("cost_hint_weight must be in [0, 1]")
        object.__setattr__(self, "op_costs", MappingProxyType(dict(self.op_costs)))
        object.__setattr__(
            self,
            "function_costs",
            MappingProxyType(dict(self.function_costs)),
        )

    @property
    def function_registry_version(self) -> str:
        return self.registry.version

    @property
    def function_registry_checksum(self) -> str:
        return self.registry.checksum()

    def registry_metadata(self) -> Mapping[str, str]:
        return MappingProxyType(
            {
                "function_registry_id": self.registry.registry_id,
                "function_registry_version": self.registry.version,
                "function_registry_checksum": self.registry.checksum(),
            }
        )

    def checksum(self) -> str:
        return _stable_checksum(
            {
                "model": "IRCostModel",
                "registry_id": self.registry.registry_id,
                "registry_version": self.registry.version,
                "registry_checksum": self.registry.checksum(),
                "op_costs": dict(self.op_costs),
                "function_costs": dict(self.function_costs),
                "iteration_cost": self.iteration_cost,
                "cost_hint_weight": self.cost_hint_weight,
            }
        )

    def estimate_program(self, program: IRProgram) -> CostEstimate:
        analysis = IRInterpreter(self.registry).analyze(program)
        components: dict[str, float] = {}
        total = 0.0
        for binding in _program_bindings(program):
            hint = _cost_hint(binding.op.args)
            op_name = binding.op.kind.value
            model_value = self._model_binding_cost(binding)
            op_value = self.op_costs.get(op_name, 1.0)
            if hint is not None:
                value = (
                    self.cost_hint_weight * hint
                    + (1.0 - self.cost_hint_weight) * model_value
                )
                components[f"op.{op_name}"] = components.get(f"op.{op_name}", 0.0) + value
                components[f"cost_hint.{op_name}"] = (
                    components.get(f"cost_hint.{op_name}", 0.0) + hint
                )
                if self.cost_hint_weight < 1.0:
                    components[f"model_blend.{op_name}"] = (
                        components.get(f"model_blend.{op_name}", 0.0) + model_value
                    )
                total += value
                continue
            components[f"op.{op_name}"] = components.get(f"op.{op_name}", 0.0) + op_value
            total += model_value
            self._add_function_components(binding, components)
        if analysis.max_iterations > 1 and self.iteration_cost:
            value = self.iteration_cost * analysis.max_iterations
            components["control.max_iterations"] = value
            total += value
        for observation in analysis.observations:
            if observation.get("kind") == "truncation_possible":
                components["quality.truncation_possible"] = 1.0
        return CostEstimate(total=total, components=components)

    def _model_binding_cost(self, binding: Any) -> float:
        op_name = binding.op.kind.value
        value = self.op_costs.get(op_name, 1.0)
        function_id = binding.op.args.get("function")
        if isinstance(function_id, str):
            value += self.function_costs.get(function_id, 1.0)
        return value

    def _add_function_components(
        self,
        binding: Any,
        components: dict[str, float],
    ) -> None:
        function_id = binding.op.args.get("function")
        if isinstance(function_id, str):
            function_value = self.function_costs.get(function_id, 1.0)
            components[f"function.{function_id}"] = (
                components.get(f"function.{function_id}", 0.0) + function_value
            )

    def estimate_delta(
        self,
        *,
        before: IRProgram,
        after: IRProgram,
        install: IRProgram | None = None,
    ) -> CostEstimate:
        before_estimate = self.estimate_program(before)
        after_estimate = self.estimate_program(after)
        install_estimate = (
            self.estimate_program(install)
            if install is not None
            else CostEstimate(total=0.0)
        )
        total = after_estimate.total + install_estimate.total - before_estimate.total
        components = {
            "before.total": before_estimate.total,
            "after.total": after_estimate.total,
            "install.total": install_estimate.total,
            "delta.total": total,
        }
        return CostEstimate(total=total, components=components)


@dataclass(frozen=True)
class IRQualityModel:
    """Contract-based IR quality model for v1 rewrite checks."""

    registry: IRFunctionRegistry
    required_contracts: tuple[str, ...] = ()
    max_truncation_penalty: float = 1.0

    def __post_init__(self) -> None:
        _validate_registry(self.registry)
        object.__setattr__(self, "required_contracts", tuple(self.required_contracts))

    @property
    def function_registry_version(self) -> str:
        return self.registry.version

    @property
    def function_registry_checksum(self) -> str:
        return self.registry.checksum()

    def registry_metadata(self) -> Mapping[str, str]:
        return MappingProxyType(
            {
                "function_registry_id": self.registry.registry_id,
                "function_registry_version": self.registry.version,
                "function_registry_checksum": self.registry.checksum(),
            }
        )

    def checksum(self) -> str:
        return _stable_checksum(
            {
                "model": "IRQualityModel",
                "registry_id": self.registry.registry_id,
                "registry_version": self.registry.version,
                "registry_checksum": self.registry.checksum(),
                "required_contracts": self.required_contracts,
                "max_truncation_penalty": self.max_truncation_penalty,
            }
        )

    def estimate_program(self, program: IRProgram) -> QualityEstimate:
        analysis = IRInterpreter(self.registry).analyze(program)
        score = 1.0
        components: dict[str, float] = {}
        if any(obs.get("kind") == "truncation_possible" for obs in analysis.observations):
            score -= self.max_truncation_penalty
            components["truncation_possible"] = self.max_truncation_penalty
        for contract in self.required_contracts:
            if contract not in str(program.attrs.get("quality_contracts", ())):
                score = min(score, 0.0)
                components[f"missing_contract.{contract}"] = 1.0
        return QualityEstimate(score=max(0.0, score), components=components)

    def estimate_delta(
        self,
        *,
        before: IRProgram,
        after: IRProgram,
    ) -> QualityEstimate:
        before_estimate = self.estimate_program(before)
        after_estimate = self.estimate_program(after)
        return QualityEstimate(
            score=after_estimate.score - before_estimate.score,
            components={
                "before.score": before_estimate.score,
                "after.score": after_estimate.score,
                "delta.score": after_estimate.score - before_estimate.score,
            },
        )


def _validate_registry(registry: IRFunctionRegistry) -> None:
    if not isinstance(registry, IRFunctionRegistry):
        raise TypeError("IR scoring models require an explicit IRFunctionRegistry")
    if not registry.registry_id:
        raise ValueError("IRFunctionRegistry.registry_id must be non-empty")
    if not registry.version:
        raise ValueError("IRFunctionRegistry.version must be non-empty")


def _program_bindings(program: IRProgram) -> tuple[Any, ...]:
    bindings = list(program.ops)
    for block in program.blocks:
        bindings.extend(block.bindings)
    return tuple(bindings)


def _cost_hint(args: Mapping[str, Any]) -> float | None:
    value = args.get("estimated_cost")
    if isinstance(value, int | float):
        if value < 0:
            raise ValueError("IR op estimated_cost must be non-negative")
        return float(value)
    return None


def _stable_checksum(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            _json_ready(value),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _json_ready(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _json_ready(item)
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, tuple | list):
        return [_json_ready(item) for item in value]
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return str(value)
