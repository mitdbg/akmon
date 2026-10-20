"""Instantiation contract for domain-specific optimizer adapters."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass

from .bounds import RewriteTier
from .graph import OptimizerGraph
from .models import CostModel, QualityModel
from .rewrite import RewriteTemplate, validate_rewrite_space


@dataclass(frozen=True)
class OptimizerInstantiation:
    """A concrete optimizer workload over the pure optimizer core.

    ``build_graph`` and ``rewrite_space`` are adapter hooks. They may close
    over deployment state in the adapter layer, but the instantiated object
    itself should remain usable by the pure search engine without importing
    substrate packages.
    """

    name: str
    workload_id: str
    build_graph: Callable[[], OptimizerGraph]
    rewrite_space: Callable[[str], Iterable[RewriteTemplate]]
    cost_model: CostModel
    quality_model: QualityModel
    quality_target: float
    allowed_rewrite_tiers: tuple[RewriteTier, ...] = (RewriteTier.TIER_1,)

    def templates(self) -> tuple[RewriteTemplate, ...]:
        templates = tuple(self.rewrite_space(self.workload_id))
        validate_rewrite_space(templates, allowed_tiers=self.allowed_rewrite_tiers)
        return templates

    def initial_graph(self) -> OptimizerGraph:
        return self.build_graph()
