"""Bounds-based candidate evaluation.

Tier-1 optimizer rewrites should be testable without modeling an LLM's
interpretation of hints. The core therefore represents candidate benefit
as explicit upper/lower bounds plus a quality-risk bound.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class AdoptionAssumption(str, Enum):
    ALWAYS_TAKEN_WHEN_RELEVANT = "always_taken_when_relevant"
    DETERMINISTIC_TOOL_SURFACE = "deterministic_tool_surface"
    IGNORED_CLUTTER_BOUND = "ignored_clutter_bound"
    BEHAVIORAL_AGENT_INTERPRETATION = "behavioral_agent_interpretation"


class RewriteTier(str, Enum):
    TIER_1 = "tier_1_mechanical_bounds"
    TIER_2 = "tier_2_behavioral"
    TIER_3 = "tier_3_research"


@dataclass(frozen=True)
class BenefitBounds:
    """Benefit and quality-risk bounds for one candidate.

    Benefit is positive when a rewrite saves cost. ``benefit_upper_bound``
    is the optimistic "agent always uses it when relevant" or deterministic
    tool-surface benefit. ``benefit_lower_bound`` is the ignored/adverse
    bound, often negative due to added clutter.
    """

    benefit_upper_bound: float
    benefit_lower_bound: float
    quality_risk_upper_bound: float
    adoption_assumption: AdoptionAssumption
    quality_assumption: str

    def validate(self, *, tier: RewriteTier) -> None:
        if self.benefit_upper_bound < 0:
            raise ValueError("benefit_upper_bound must be non-negative")
        if self.benefit_lower_bound > self.benefit_upper_bound:
            raise ValueError("benefit_lower_bound cannot exceed upper bound")
        if not 0 <= self.quality_risk_upper_bound <= 1:
            raise ValueError("quality_risk_upper_bound must be in [0, 1]")
        if not self.quality_assumption:
            raise ValueError("quality_assumption must be explicit")
        if (
            tier is RewriteTier.TIER_1
            and self.adoption_assumption
            is AdoptionAssumption.BEHAVIORAL_AGENT_INTERPRETATION
        ):
            raise ValueError("Tier-1 rewrites cannot depend on agent interpretation")


@dataclass(frozen=True)
class BoundedDecision:
    admissible: bool
    reason: str
    benefit_upper_bound: float
    benefit_lower_bound: float
    quality_risk_upper_bound: float


def evaluate_benefit_bounds(
    bounds: BenefitBounds,
    *,
    tier: RewriteTier,
    min_benefit_upper_bound: float = 0.0,
    max_lower_bound_loss: float | None = None,
    max_quality_risk: float | None = None,
) -> BoundedDecision:
    bounds.validate(tier=tier)
    if bounds.benefit_upper_bound < min_benefit_upper_bound:
        return _decision(False, "benefit_upper_bound_too_small", bounds)
    if (
        max_lower_bound_loss is not None
        and bounds.benefit_lower_bound < -max_lower_bound_loss
    ):
        return _decision(False, "lower_bound_loss_too_large", bounds)
    if (
        max_quality_risk is not None
        and bounds.quality_risk_upper_bound > max_quality_risk
    ):
        return _decision(False, "quality_risk_bound_too_large", bounds)
    return _decision(True, "bounds_admissible", bounds)


def _decision(
    admissible: bool,
    reason: str,
    bounds: BenefitBounds,
) -> BoundedDecision:
    return BoundedDecision(
        admissible=admissible,
        reason=reason,
        benefit_upper_bound=bounds.benefit_upper_bound,
        benefit_lower_bound=bounds.benefit_lower_bound,
        quality_risk_upper_bound=bounds.quality_risk_upper_bound,
    )
