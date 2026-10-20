"""IR-backed candidate scoring for optimizer rewrites."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from .bounds import AdoptionAssumption, BenefitBounds
from .ir import IRFunctionRegistry, IRInterpreter, IRProgram, IRValidationError
from .models import CostEstimate, IRCostModel, IRQualityModel, QualityEstimate
from .rewrite import RewriteCandidate


@dataclass(frozen=True)
class IRCandidateScore:
    """Auditable IR score for one rewrite candidate.

    Negative ``expected_cost_delta`` means the candidate is expected to reduce
    traversal cost. Quality remains a unit score in [0, 1], while
    ``quality_risk_upper_bound`` carries the candidate's explicit error bound.
    """

    candidate_id: str
    template_id: str
    admissible: bool
    rejection_reason: str | None
    function_registry_id: str
    function_registry_version: str
    function_registry_checksum: str
    cost_model_checksum: str
    quality_model_checksum: str
    before_ir_checksum: str | None
    install_ir_checksum: str | None
    after_ir_checksum: str | None
    cost_before: float
    cost_after: float
    cost_install: float
    cost_maintenance: float
    expected_cost_delta: float
    quality_before: float
    quality_after: float
    expected_quality_delta: float
    quality_risk_upper_bound: float
    adoption_assumption: str | None
    benefit_upper_bound: float | None
    benefit_lower_bound: float | None
    scalar_estimated_cost_delta: float
    scalar_ir_delta_gap: float
    components: Mapping[str, float] = field(
        default_factory=lambda: MappingProxyType({})
    )
    diagnostics: Mapping[str, Any] = field(
        default_factory=lambda: MappingProxyType({})
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "components",
            MappingProxyType({key: float(value) for key, value in self.components.items()}),
        )
        object.__setattr__(self, "diagnostics", MappingProxyType(dict(self.diagnostics)))

    @property
    def program_checksums(self) -> Mapping[str, str]:
        checksums: dict[str, str] = {}
        if self.before_ir_checksum is not None:
            checksums["before_ir"] = self.before_ir_checksum
        if self.install_ir_checksum is not None:
            checksums["install_ir"] = self.install_ir_checksum
        if self.after_ir_checksum is not None:
            checksums["after_ir"] = self.after_ir_checksum
        return MappingProxyType(checksums)

    def as_metadata(self) -> Mapping[str, Any]:
        """Return a JSON-ready metadata payload for substrate persistence."""

        return MappingProxyType(
            {
                "ir_score": {
                    "candidate_id": self.candidate_id,
                    "template_id": self.template_id,
                    "admissible": self.admissible,
                    "rejection_reason": self.rejection_reason,
                    "cost_before": self.cost_before,
                    "cost_after": self.cost_after,
                    "cost_install": self.cost_install,
                    "cost_maintenance": self.cost_maintenance,
                    "expected_cost_delta": self.expected_cost_delta,
                    "quality_before": self.quality_before,
                    "quality_after": self.quality_after,
                    "expected_quality_delta": self.expected_quality_delta,
                    "quality_risk_upper_bound": self.quality_risk_upper_bound,
                    "adoption_assumption": self.adoption_assumption,
                    "benefit_upper_bound": self.benefit_upper_bound,
                    "benefit_lower_bound": self.benefit_lower_bound,
                    "scalar_estimated_cost_delta": self.scalar_estimated_cost_delta,
                    "scalar_ir_delta_gap": self.scalar_ir_delta_gap,
                    "components": dict(self.components),
                    "diagnostics": dict(self.diagnostics),
                },
                "function_registry_id": self.function_registry_id,
                "function_registry_version": self.function_registry_version,
                "function_registry_checksum": self.function_registry_checksum,
                "cost_model_checksum": self.cost_model_checksum,
                "quality_model_checksum": self.quality_model_checksum,
                "program_checksums": dict(self.program_checksums),
            }
        )


@dataclass(frozen=True)
class IRCandidateScorer:
    """Score candidates by analyzing their declared IR programs."""

    cost_model: IRCostModel
    quality_model: IRQualityModel
    maintenance_cost: float = 0.0
    cost_model_source: str = "static_ir_counts_v1"
    reference_cost_model: IRCostModel | None = None
    reference_cost_model_source: str = "static_ir_counts_v1"

    def __post_init__(self) -> None:
        if self.cost_model.registry.checksum() != self.quality_model.registry.checksum():
            raise ValueError("IR cost and quality models must use the same registry")
        if self.maintenance_cost < 0:
            raise ValueError("maintenance_cost must be non-negative")
        if (
            self.reference_cost_model is not None
            and self.reference_cost_model.registry.checksum()
            != self.cost_model.registry.checksum()
        ):
            raise ValueError("reference cost model must use the same registry")

    @property
    def registry(self) -> IRFunctionRegistry:
        return self.cost_model.registry

    def score(self, candidate: RewriteCandidate) -> IRCandidateScore:
        if not candidate.quality_contract:
            return self._rejected(
                candidate,
                reason="missing_quality_contract",
                diagnostics={"detail": "candidate must declare a quality contract"},
            )
        if (
            candidate.benefit_bounds is not None
            and candidate.benefit_bounds.adoption_assumption
            is AdoptionAssumption.BEHAVIORAL_AGENT_INTERPRETATION
        ):
            return self._rejected(
                candidate,
                reason="adoption_dependent_rewrite",
                diagnostics={
                    "detail": "Tier-One IR scoring requires mechanical adoption bounds"
                },
            )
        programs = _candidate_programs(candidate)
        if programs is None:
            return self._rejected(
                candidate,
                reason="missing_candidate_ir",
                diagnostics={"detail": "candidate must declare before/install/after IR"},
            )
        before, install, after = programs
        try:
            before_cost = self.cost_model.estimate_program(before)
            install_cost = self.cost_model.estimate_program(install)
            after_cost = self.cost_model.estimate_program(after)
            before_quality = self.quality_model.estimate_program(before)
            after_quality = self.quality_model.estimate_program(after)
            before_analysis = IRInterpreter(self.registry).analyze(before)
            install_analysis = IRInterpreter(self.registry).analyze(install)
            after_analysis = IRInterpreter(self.registry).analyze(after)
            reference_costs = self._reference_costs(before, install, after)
        except IRValidationError as exc:
            return self._rejected(
                candidate,
                reason=_classify_ir_validation_error(exc),
                diagnostics={"detail": str(exc)},
            )

        expected_cost_delta = (
            after_cost.total
            + install_cost.total
            + self.maintenance_cost
            - before_cost.total
        )
        expected_quality_delta = after_quality.score - before_quality.score
        benefit_bounds = candidate.benefit_bounds
        components = {
            "cost.before.total": before_cost.total,
            "cost.after.total": after_cost.total,
            "cost.install.total": install_cost.total,
            "cost.maintenance.total": self.maintenance_cost,
            "cost.delta.expected": expected_cost_delta,
            "quality.before.score": before_quality.score,
            "quality.after.score": after_quality.score,
            "quality.delta.expected": expected_quality_delta,
            "quality.risk_upper_bound": _quality_risk(candidate, benefit_bounds),
            "scalar.estimated_cost_delta": candidate.estimated_cost_delta,
            "scalar.ir_delta_gap": expected_cost_delta - candidate.estimated_cost_delta,
            **_prefixed_components("before", before_cost),
            **_prefixed_components("install", install_cost),
            **_prefixed_components("after", after_cost),
            **_prefixed_quality_components("before", before_quality),
            **_prefixed_quality_components("after", after_quality),
            **_analysis_components("before", before_analysis),
            **_analysis_components("install", install_analysis),
            **_analysis_components("after", after_analysis),
            **_benefit_components(benefit_bounds),
        }
        components["storage.install.graph_delta_ops"] = (
            install_analysis.op_counts.get("ATOMIC_EDIT", 0)
            + install_analysis.op_counts.get("EDIT", 0)
        )
        diagnostics = {
            "scalar_ir_delta_gap": expected_cost_delta - candidate.estimated_cost_delta,
            "equivalence_contract": candidate.equivalence_contract,
            "quality_contract": candidate.quality_contract,
            "cost_model_source": self.cost_model_source,
            "reference_cost_model_source": self.reference_cost_model_source,
            "cardinality_model": "static_ir_counts_v1",
            "storage_model": "install_delta_op_count_v1",
        }
        if reference_costs is not None:
            reference_before, reference_install, reference_after = reference_costs
            reference_delta = (
                reference_after.total
                + reference_install.total
                + self.maintenance_cost
                - reference_before.total
            )
            components.update(
                {
                    "reference.cost.before.total": reference_before.total,
                    "reference.cost.after.total": reference_after.total,
                    "reference.cost.install.total": reference_install.total,
                    "reference.cost.delta.expected": reference_delta,
                    "reference.cost.delta_disagreement": (
                        expected_cost_delta - reference_delta
                    ),
                }
            )
            diagnostics.update(
                {
                    "active_cost_model_source": self.cost_model_source,
                    "reference_cost_model_checksum": (
                        self.reference_cost_model.checksum()
                        if self.reference_cost_model is not None
                        else None
                    ),
                    "reference_expected_cost_delta": reference_delta,
                    "model_delta_disagreement": expected_cost_delta - reference_delta,
                }
            )
        return IRCandidateScore(
            candidate_id=candidate.candidate_id,
            template_id=candidate.template_id,
            admissible=True,
            rejection_reason=None,
            function_registry_id=self.registry.registry_id,
            function_registry_version=self.registry.version,
            function_registry_checksum=self.registry.checksum(),
            cost_model_checksum=self.cost_model.checksum(),
            quality_model_checksum=self.quality_model.checksum(),
            before_ir_checksum=before.checksum(),
            install_ir_checksum=install.checksum(),
            after_ir_checksum=after.checksum(),
            cost_before=before_cost.total,
            cost_after=after_cost.total,
            cost_install=install_cost.total,
            cost_maintenance=self.maintenance_cost,
            expected_cost_delta=expected_cost_delta,
            quality_before=before_quality.score,
            quality_after=after_quality.score,
            expected_quality_delta=expected_quality_delta,
            quality_risk_upper_bound=_quality_risk(candidate, benefit_bounds),
            adoption_assumption=_adoption_assumption(benefit_bounds),
            benefit_upper_bound=(
                None if benefit_bounds is None else benefit_bounds.benefit_upper_bound
            ),
            benefit_lower_bound=(
                None if benefit_bounds is None else benefit_bounds.benefit_lower_bound
            ),
            scalar_estimated_cost_delta=candidate.estimated_cost_delta,
            scalar_ir_delta_gap=expected_cost_delta - candidate.estimated_cost_delta,
            components=components,
            diagnostics=diagnostics,
        )

    def _reference_costs(
        self,
        before: IRProgram,
        install: IRProgram,
        after: IRProgram,
    ) -> tuple[Any, Any, Any] | None:
        if self.reference_cost_model is None:
            return None
        return (
            self.reference_cost_model.estimate_program(before),
            self.reference_cost_model.estimate_program(install),
            self.reference_cost_model.estimate_program(after),
        )

    def _rejected(
        self,
        candidate: RewriteCandidate,
        *,
        reason: str,
        diagnostics: Mapping[str, Any],
    ) -> IRCandidateScore:
        return IRCandidateScore(
            candidate_id=candidate.candidate_id,
            template_id=candidate.template_id,
            admissible=False,
            rejection_reason=reason,
            function_registry_id=self.registry.registry_id,
            function_registry_version=self.registry.version,
            function_registry_checksum=self.registry.checksum(),
            cost_model_checksum=self.cost_model.checksum(),
            quality_model_checksum=self.quality_model.checksum(),
            before_ir_checksum=(
                None if candidate.before_ir is None else candidate.before_ir.checksum()
            ),
            install_ir_checksum=(
                None if candidate.install_ir is None else candidate.install_ir.checksum()
            ),
            after_ir_checksum=(
                None if candidate.after_ir is None else candidate.after_ir.checksum()
            ),
            cost_before=0.0,
            cost_after=0.0,
            cost_install=0.0,
            cost_maintenance=self.maintenance_cost,
            expected_cost_delta=0.0,
            quality_before=0.0,
            quality_after=0.0,
            expected_quality_delta=0.0,
            quality_risk_upper_bound=_quality_risk(candidate, candidate.benefit_bounds),
            adoption_assumption=_adoption_assumption(candidate.benefit_bounds),
            benefit_upper_bound=(
                None
                if candidate.benefit_bounds is None
                else candidate.benefit_bounds.benefit_upper_bound
            ),
            benefit_lower_bound=(
                None
                if candidate.benefit_bounds is None
                else candidate.benefit_bounds.benefit_lower_bound
            ),
            scalar_estimated_cost_delta=candidate.estimated_cost_delta,
            scalar_ir_delta_gap=0.0 - candidate.estimated_cost_delta,
            components={
                "cost.maintenance.total": self.maintenance_cost,
                "quality.risk_upper_bound": _quality_risk(
                    candidate,
                    candidate.benefit_bounds,
                ),
                "scalar.estimated_cost_delta": candidate.estimated_cost_delta,
                "scalar.ir_delta_gap": 0.0 - candidate.estimated_cost_delta,
            },
            diagnostics={
                "cost_model_source": self.cost_model_source,
                "reference_cost_model_source": self.reference_cost_model_source,
                **dict(diagnostics),
            },
        )


def _candidate_programs(
    candidate: RewriteCandidate,
) -> tuple[IRProgram, IRProgram, IRProgram] | None:
    if (
        candidate.before_ir is None
        or candidate.install_ir is None
        or candidate.after_ir is None
    ):
        return None
    return candidate.before_ir, candidate.install_ir, candidate.after_ir


def _classify_ir_validation_error(exc: IRValidationError) -> str:
    message = str(exc)
    if "unregistered function" in message:
        return "unregistered_ir_function"
    if "FOR_EACH must declare max_items or known_cardinality" in message:
        return "unbounded_cardinality"
    if "quality_contract" in message or "quality_contracts" in message:
        return "missing_quality_contract"
    return "invalid_ir_program"


def _quality_risk(
    candidate: RewriteCandidate,
    benefit_bounds: BenefitBounds | None,
) -> float:
    if benefit_bounds is not None:
        return float(benefit_bounds.quality_risk_upper_bound)
    return float(candidate.quality_regression_upper_bound)


def _adoption_assumption(bounds: BenefitBounds | None) -> str | None:
    if bounds is None:
        return None
    return bounds.adoption_assumption.value


def _prefixed_components(prefix: str, estimate: CostEstimate) -> dict[str, float]:
    return {
        f"cost.{prefix}.{key}": float(value)
        for key, value in estimate.components.items()
    }


def _prefixed_quality_components(
    prefix: str,
    estimate: QualityEstimate,
) -> dict[str, float]:
    return {
        f"quality.{prefix}.{key}": float(value)
        for key, value in estimate.components.items()
    }


def _analysis_components(prefix: str, analysis: Any) -> dict[str, float]:
    components: dict[str, float] = {
        f"cardinality.{prefix}.max_iterations": float(analysis.max_iterations)
    }
    for op_name, count in analysis.op_counts.items():
        components[f"cardinality.{prefix}.op_count.{op_name}"] = float(count)
    for function_id, count in analysis.function_counts.items():
        components[f"cardinality.{prefix}.function_count.{function_id}"] = float(count)
    return components


def _benefit_components(bounds: BenefitBounds | None) -> dict[str, float]:
    if bounds is None:
        return {}
    return {
        "adoption.benefit_upper_bound": float(bounds.benefit_upper_bound),
        "adoption.benefit_lower_bound": float(bounds.benefit_lower_bound),
        "adoption.quality_risk_upper_bound": float(bounds.quality_risk_upper_bound),
    }
