"""Rewrite-space declarations and safety validation."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any

from .bounds import BenefitBounds, RewriteTier
from .graph import OptimizerGraph
from .ir import IRProgram


class RewriteValidationError(ValueError):
    """Raised when a rewrite template widens the allowed search space."""


class RewriteSurface(str, Enum):
    TOPOLOGY = "topology"
    SCENT = "scent"
    ROUTE_POLICY = "route_policy"
    MATERIALIZATION = "materialization"
    PAYLOAD = "payload"


_FORBIDDEN_NAME_PARTS = (
    "qdtree",
    "qd_tree",
    "pando",
    "quake",
    "dostoevsky",
    "faiss",
    "rocksdb",
)


@dataclass(frozen=True)
class ParameterSchema:
    """Closed candidate parameter schema.

    The schema is intentionally small. Adapters that need JSON Schema can
    translate into this closed form before entering the core.
    """

    fields: Mapping[str, str]
    allow_extra: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "fields", MappingProxyType(dict(self.fields)))

    def validate_values(self, values: Mapping[str, Any]) -> None:
        extra = set(values) - set(self.fields)
        missing = set(self.fields) - set(values)
        if missing:
            raise RewriteValidationError(
                "candidate parameters missing required keys: "
                + ", ".join(sorted(missing))
            )
        if extra and not self.allow_extra:
            raise RewriteValidationError(
                "candidate parameters include undeclared keys: "
                + ", ".join(sorted(extra))
            )


@dataclass(frozen=True)
class BoundedTargetGenerator:
    """Declaration for the candidate generator's search boundary."""

    name: str
    max_candidates: int
    evidence_sources: tuple[str, ...]
    scans_all_pairs: bool = False

    def validate(self) -> None:
        if self.max_candidates <= 0:
            raise RewriteValidationError("target generator must be bounded")
        if not self.evidence_sources:
            raise RewriteValidationError(
                "target generator must declare at least one evidence source"
            )
        if self.scans_all_pairs:
            raise RewriteValidationError("all-pairs target generators are forbidden")


@dataclass(frozen=True)
class MaterializationPlan:
    """Plan for turning a candidate into substrate-visible evidence."""

    kind: str
    surface: RewriteSurface
    payload_schema: ParameterSchema
    mutates_route_manifest: bool = False
    arbitrary_sql: bool = False
    source_truth_mutation: bool = False

    def validate(self) -> None:
        if self.arbitrary_sql:
            raise RewriteValidationError("arbitrary SQL materialization is forbidden")
        if self.mutates_route_manifest:
            raise RewriteValidationError(
                "route manifest edits are forbidden during planning"
            )
        if self.source_truth_mutation:
            raise RewriteValidationError(
                "source-truth delete/retype rewrites are forbidden"
            )
        if self.payload_schema.allow_extra:
            raise RewriteValidationError("materialization payload schema must be closed")


@dataclass(frozen=True)
class RollbackPolicy:
    mechanism: str
    eviction_signal: str

    def validate(self) -> None:
        if not self.mechanism:
            raise RewriteValidationError("rollback policy must declare a mechanism")
        if not self.eviction_signal:
            raise RewriteValidationError(
                "rollback policy must declare an eviction signal"
            )


@dataclass(frozen=True)
class RewriteCandidate:
    candidate_id: str
    template_id: str
    params: Mapping[str, Any]
    apply: Callable[[OptimizerGraph], OptimizerGraph]
    estimated_cost_delta: float
    estimated_quality_delta: float = 0.0
    quality_regression_upper_bound: float = 0.0
    evidence_refs: tuple[str, ...] = ()
    benefit_bounds: BenefitBounds | None = None
    before_ir: IRProgram | None = None
    install_ir: IRProgram | None = None
    after_ir: IRProgram | None = None
    equivalence_contract: str | None = None
    quality_contract: str | None = None
    conflict_key: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "params", MappingProxyType(dict(self.params)))
        if self.conflict_key is not None and not str(self.conflict_key).strip():
            raise RewriteValidationError("candidate conflict_key cannot be empty")

    @property
    def program_checksums(self) -> Mapping[str, str]:
        checksums: dict[str, str] = {}
        if self.before_ir is not None:
            checksums["before_ir"] = self.before_ir.checksum()
        if self.install_ir is not None:
            checksums["install_ir"] = self.install_ir.checksum()
        if self.after_ir is not None:
            checksums["after_ir"] = self.after_ir.checksum()
        return MappingProxyType(checksums)


@dataclass(frozen=True)
class RewriteTemplate:
    rewrite_id: str
    surface: RewriteSurface
    parameter_schema: ParameterSchema
    target_generator: BoundedTargetGenerator
    preconditions: tuple[str, ...]
    dependencies: tuple[str, ...]
    materialization: MaterializationPlan
    cost_estimator: str
    error_estimator: str
    conflict_group: str
    rollback: RollbackPolicy
    generate: Callable[[OptimizerGraph], Iterable[RewriteCandidate]]
    quality_label_policy: str = "required"
    tier: RewriteTier = RewriteTier.TIER_1

    def validate(self) -> None:
        lowered = self.rewrite_id.lower()
        if any(part in lowered for part in _FORBIDDEN_NAME_PARTS):
            raise RewriteValidationError(
                f"rewrite {self.rewrite_id!r} wraps a specialized algorithm"
            )
        if self.parameter_schema.allow_extra:
            raise RewriteValidationError("rewrite parameter schema must be closed")
        if not self.preconditions:
            raise RewriteValidationError("rewrite must declare preconditions")
        if not self.dependencies:
            raise RewriteValidationError("rewrite must declare dependencies")
        if not self.cost_estimator:
            raise RewriteValidationError("rewrite must declare a cost estimator")
        if not self.error_estimator:
            raise RewriteValidationError("rewrite must declare an error estimator")
        if not self.conflict_group:
            raise RewriteValidationError("rewrite must declare a conflict group")
        if self.quality_label_policy not in {"required", "report_only"}:
            raise RewriteValidationError(
                "quality-label policy must be 'required' or 'report_only'"
            )
        if not isinstance(self.tier, RewriteTier):
            raise RewriteValidationError("rewrite must declare a valid tier")
        self.target_generator.validate()
        self.materialization.validate()
        self.rollback.validate()

    def candidates(self, graph: OptimizerGraph) -> tuple[RewriteCandidate, ...]:
        self.validate()
        candidates = tuple(self.generate(graph))
        if len(candidates) > self.target_generator.max_candidates:
            raise RewriteValidationError(
                f"rewrite {self.rewrite_id!r} generated {len(candidates)} "
                f"candidates, exceeding bound {self.target_generator.max_candidates}"
            )
        for candidate in candidates:
            if candidate.template_id != self.rewrite_id:
                raise RewriteValidationError(
                    f"candidate {candidate.candidate_id!r} has template_id "
                    f"{candidate.template_id!r}, expected {self.rewrite_id!r}"
                )
            self.parameter_schema.validate_values(candidate.params)
            if not candidate.evidence_refs:
                raise RewriteValidationError(
                    f"candidate {candidate.candidate_id!r} lacks evidence refs"
                )
            if self.tier is RewriteTier.TIER_1:
                if (
                    candidate.before_ir is None
                    or candidate.install_ir is None
                    or candidate.after_ir is None
                ):
                    raise RewriteValidationError(
                        f"Tier-1 candidate {candidate.candidate_id!r} must "
                        "declare before_ir, install_ir, and after_ir"
                    )
                if not candidate.equivalence_contract:
                    raise RewriteValidationError(
                        f"Tier-1 candidate {candidate.candidate_id!r} must "
                        "declare an equivalence contract"
                    )
                if not candidate.quality_contract:
                    raise RewriteValidationError(
                        f"Tier-1 candidate {candidate.candidate_id!r} must "
                        "declare a quality contract"
                    )
                if candidate.benefit_bounds is None:
                    raise RewriteValidationError(
                        f"Tier-1 candidate {candidate.candidate_id!r} "
                        "must declare benefit bounds"
                    )
                try:
                    candidate.benefit_bounds.validate(tier=self.tier)
                except ValueError as exc:
                    raise RewriteValidationError(str(exc)) from exc
                if (
                    candidate.quality_regression_upper_bound
                    < candidate.benefit_bounds.quality_risk_upper_bound
                ):
                    raise RewriteValidationError(
                        "candidate quality_regression_upper_bound must cover "
                        "benefit_bounds.quality_risk_upper_bound"
                    )
        return candidates


def validate_rewrite_space(
    templates: Iterable[RewriteTemplate],
    *,
    allowed_tiers: tuple[RewriteTier, ...] | None = None,
) -> None:
    seen: set[str] = set()
    for template in templates:
        if template.rewrite_id in seen:
            raise RewriteValidationError(
                f"duplicate rewrite id: {template.rewrite_id!r}"
            )
        seen.add(template.rewrite_id)
        template.validate()
        if allowed_tiers is not None and template.tier not in allowed_tiers:
            raise RewriteValidationError(
                f"rewrite {template.rewrite_id!r} has disallowed tier "
                f"{template.tier.value!r}"
            )
