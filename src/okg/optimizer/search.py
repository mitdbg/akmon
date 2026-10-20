"""Beam-search optimizer over declared rewrite candidates."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from .bounds import evaluate_benefit_bounds
from .graph import OptimizerGraph
from .models import CostModel, QualityModel
from .rewrite import (
    RewriteCandidate,
    RewriteTemplate,
    RewriteValidationError,
    validate_rewrite_space,
)
from .scoring import IRCandidateScore, IRCandidateScorer


@dataclass(frozen=True)
class OptimizationEvent:
    depth: int
    candidate_id: str
    template_id: str
    decision: str
    reason: str
    cost: float
    quality: float
    metadata: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True)
class _SearchState:
    graph: OptimizerGraph
    cost: float
    quality: float
    applied: tuple[str, ...]
    conflicts: frozenset[str] = frozenset()


@dataclass(frozen=True)
class OptimizationResult:
    graph: OptimizerGraph
    initial_cost: float
    final_cost: float
    initial_quality: float
    final_quality: float
    applied_candidates: tuple[str, ...]
    trace: tuple[OptimizationEvent, ...]


class BeamSearchOptimizer:
    """Bounded optimizer that searches over explicit rewrite candidates."""

    def __init__(
        self,
        *,
        cost_model: CostModel,
        quality_model: QualityModel,
        quality_target: float,
        beam_width: int = 4,
        max_depth: int = 4,
        min_benefit_upper_bound: float = 0.0,
        max_lower_bound_loss: float | None = None,
        max_quality_risk: float | None = None,
        ir_candidate_scorer: IRCandidateScorer | None = None,
    ) -> None:
        if beam_width <= 0:
            raise ValueError("beam_width must be positive")
        if max_depth < 0:
            raise ValueError("max_depth cannot be negative")
        self.cost_model = cost_model
        self.quality_model = quality_model
        self.quality_target = quality_target
        self.beam_width = beam_width
        self.max_depth = max_depth
        self.min_benefit_upper_bound = min_benefit_upper_bound
        self.max_lower_bound_loss = max_lower_bound_loss
        self.max_quality_risk = max_quality_risk
        self.ir_candidate_scorer = ir_candidate_scorer

    def optimize(
        self,
        graph: OptimizerGraph,
        templates: tuple[RewriteTemplate, ...],
    ) -> OptimizationResult:
        validate_rewrite_space(templates)
        initial_cost = self.cost_model.estimate(graph).total
        initial_quality = self.quality_model.estimate(graph).score
        start = _SearchState(
            graph=graph,
            cost=initial_cost,
            quality=initial_quality,
            applied=(),
        )
        beam = (start,)
        best = start
        trace: list[OptimizationEvent] = []

        for depth in range(self.max_depth):
            next_states: list[_SearchState] = []
            for state in beam:
                for template in templates:
                    for candidate in template.candidates(state.graph):
                        if candidate.candidate_id in state.applied:
                            continue
                        conflict_token = _candidate_conflict_token(template, candidate)
                        if conflict_token is not None and conflict_token in state.conflicts:
                            trace.append(
                                self._event(
                                    depth=depth,
                                    candidate_id=candidate.candidate_id,
                                    template_id=candidate.template_id,
                                    decision="suppressed",
                                    reason="conflict_already_applied",
                                    cost=state.cost,
                                    quality=state.quality,
                                    metadata={"conflict_token": conflict_token},
                                )
                            )
                            continue
                        if candidate.benefit_bounds is not None:
                            bounds_decision = evaluate_benefit_bounds(
                                candidate.benefit_bounds,
                                tier=template.tier,
                                min_benefit_upper_bound=(
                                    self.min_benefit_upper_bound
                                ),
                                max_lower_bound_loss=self.max_lower_bound_loss,
                                max_quality_risk=self.max_quality_risk,
                            )
                            if not bounds_decision.admissible:
                                trace.append(
                                    self._event(
                                        depth=depth,
                                        candidate_id=candidate.candidate_id,
                                        template_id=candidate.template_id,
                                        decision="suppressed",
                                        reason=bounds_decision.reason,
                                        cost=state.cost,
                                        quality=state.quality,
                                    )
                                )
                                continue
                        score = self._score_candidate(candidate)
                        if score is not None and not score.admissible:
                            trace.append(
                                self._event(
                                    depth=depth,
                                    candidate_id=candidate.candidate_id,
                                    template_id=candidate.template_id,
                                    decision="suppressed",
                                    reason=score.rejection_reason or "ir_score_rejected",
                                    cost=state.cost,
                                    quality=state.quality,
                                    score=score,
                                )
                            )
                            continue
                        if score is None and candidate.estimated_cost_delta >= 0:
                            trace.append(
                                self._event(
                                    depth=depth,
                                    candidate_id=candidate.candidate_id,
                                    template_id=candidate.template_id,
                                    decision="suppressed",
                                    reason="scalar_not_beneficial",
                                    cost=state.cost + candidate.estimated_cost_delta,
                                    quality=state.quality,
                                )
                            )
                            continue
                        if score is not None and score.expected_cost_delta >= 0:
                            trace.append(
                                self._event(
                                    depth=depth,
                                    candidate_id=candidate.candidate_id,
                                    template_id=candidate.template_id,
                                    decision="suppressed",
                                    reason=_not_beneficial_reason(score),
                                    cost=state.cost + score.expected_cost_delta,
                                    quality=state.quality,
                                    score=score,
                                )
                            )
                            continue
                        new_state = self._apply_candidate(
                            state,
                            candidate,
                            score=score,
                            conflict_token=conflict_token,
                        )
                        if new_state.quality < self.quality_target:
                            trace.append(
                                self._event(
                                    depth=depth,
                                    candidate_id=candidate.candidate_id,
                                    template_id=candidate.template_id,
                                    decision="suppressed",
                                    reason="quality_below_target",
                                    cost=new_state.cost,
                                    quality=new_state.quality,
                                    score=score,
                                )
                            )
                            continue
                        if (
                            candidate.quality_regression_upper_bound
                            > max(0.0, 1.0 - self.quality_target)
                        ):
                            trace.append(
                                self._event(
                                    depth=depth,
                                    candidate_id=candidate.candidate_id,
                                    template_id=candidate.template_id,
                                    decision="suppressed",
                                    reason="quality_regression_bound",
                                    cost=new_state.cost,
                                    quality=new_state.quality,
                                    score=score,
                                )
                            )
                            continue
                        trace.append(
                            self._event(
                                depth=depth,
                                candidate_id=candidate.candidate_id,
                                template_id=candidate.template_id,
                                decision="accepted",
                                reason=(
                                    "improves_objective_ir_score"
                                    if score is not None
                                    else "improves_objective"
                                ),
                                cost=new_state.cost,
                                quality=new_state.quality,
                                score=score,
                            )
                        )
                        next_states.append(new_state)

            if not next_states:
                break
            next_states.sort(key=lambda state: (state.cost, -state.quality))
            beam = tuple(next_states[: self.beam_width])
            if beam[0].cost < best.cost:
                best = beam[0]

        return OptimizationResult(
            graph=best.graph,
            initial_cost=initial_cost,
            final_cost=best.cost,
            initial_quality=initial_quality,
            final_quality=best.quality,
            applied_candidates=best.applied,
            trace=tuple(trace),
        )

    def _apply_candidate(
        self,
        state: _SearchState,
        candidate: RewriteCandidate,
        *,
        score: IRCandidateScore | None = None,
        conflict_token: str | None = None,
    ) -> _SearchState:
        graph = candidate.apply(state.graph)
        _validate_additive_graph_rewrite(
            before=state.graph,
            after=graph,
            candidate_id=candidate.candidate_id,
        )
        cost_delta = (
            candidate.estimated_cost_delta
            if score is None
            else score.expected_cost_delta
        )
        quality_delta = (
            candidate.estimated_quality_delta
            if score is None
            else min(score.expected_quality_delta, -score.quality_risk_upper_bound)
        )
        return _SearchState(
            graph=graph,
            cost=max(0.0, state.cost + cost_delta),
            quality=state.quality + quality_delta,
            applied=(*state.applied, candidate.candidate_id),
            conflicts=(
                state.conflicts
                if conflict_token is None
                else frozenset((*state.conflicts, conflict_token))
            ),
        )

    def _score_candidate(
        self,
        candidate: RewriteCandidate,
    ) -> IRCandidateScore | None:
        if self.ir_candidate_scorer is None:
            return None
        return self.ir_candidate_scorer.score(candidate)

    def _event(
        self,
        *,
        depth: int,
        candidate_id: str,
        template_id: str,
        decision: str,
        reason: str,
        cost: float,
        quality: float,
        score: IRCandidateScore | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> OptimizationEvent:
        event_metadata: Mapping[str, Any]
        if score is None:
            event_metadata = {} if metadata is None else metadata
        elif metadata is None:
            event_metadata = _score_event_metadata(score)
        else:
            event_metadata = {**dict(_score_event_metadata(score)), **dict(metadata)}
        return OptimizationEvent(
            depth=depth,
            candidate_id=candidate_id,
            template_id=template_id,
            decision=decision,
            reason=reason,
            cost=cost,
            quality=quality,
            metadata=event_metadata,
        )


def _not_beneficial_reason(score: IRCandidateScore) -> str:
    if score.scalar_estimated_cost_delta < 0 <= score.expected_cost_delta:
        return "ir_score_not_beneficial_scalar_ir_disagreement"
    return "ir_score_not_beneficial"


def _candidate_conflict_token(
    template: RewriteTemplate,
    candidate: RewriteCandidate,
) -> str | None:
    if candidate.conflict_key is None:
        return None
    return f"{template.conflict_group}:{candidate.conflict_key}"


def _validate_additive_graph_rewrite(
    *,
    before: OptimizerGraph,
    after: OptimizerGraph,
    candidate_id: str,
) -> None:
    missing_nodes = sorted(set(before.nodes) - set(after.nodes))
    if missing_nodes:
        raise RewriteValidationError(
            f"candidate {candidate_id!r} removed existing nodes: "
            + ", ".join(missing_nodes)
        )
    for node_id, before_node in before.nodes.items():
        after_node = after.nodes[node_id]
        if after_node.subtype != before_node.subtype:
            raise RewriteValidationError(
                f"candidate {candidate_id!r} changed subtype for node {node_id!r}"
            )
        if dict(after_node.attrs) != dict(before_node.attrs):
            raise RewriteValidationError(
                f"candidate {candidate_id!r} changed attrs for node {node_id!r}"
            )

    missing_edges = sorted(set(before.edges) - set(after.edges))
    if missing_edges:
        raise RewriteValidationError(
            f"candidate {candidate_id!r} removed existing edges: "
            + ", ".join(missing_edges)
        )
    for edge_id, before_edge in before.edges.items():
        after_edge = after.edges[edge_id]
        if (
            after_edge.src != before_edge.src
            or after_edge.dst != before_edge.dst
            or after_edge.edge_type != before_edge.edge_type
        ):
            raise RewriteValidationError(
                f"candidate {candidate_id!r} changed route for edge {edge_id!r}"
            )
        if dict(after_edge.attrs) != dict(before_edge.attrs):
            raise RewriteValidationError(
                f"candidate {candidate_id!r} changed attrs for edge {edge_id!r}"
            )


def _score_event_metadata(score: IRCandidateScore) -> Mapping[str, Any]:
    return MappingProxyType(
        {
            "ir_score": {
                "admissible": score.admissible,
                "rejection_reason": score.rejection_reason,
                "expected_cost_delta": score.expected_cost_delta,
                "expected_quality_delta": score.expected_quality_delta,
                "quality_risk_upper_bound": score.quality_risk_upper_bound,
                "scalar_estimated_cost_delta": score.scalar_estimated_cost_delta,
                "scalar_ir_delta_gap": score.scalar_ir_delta_gap,
            },
            "function_registry_version": score.function_registry_version,
            "function_registry_checksum": score.function_registry_checksum,
            "program_checksums": dict(score.program_checksums),
        }
    )
