"""Executable consistency checks for the Althea optimizer safety model."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from .graph import OptimizerGraph
from .rewrite import RewriteCandidate
from .search import OptimizationResult


SEARCH_THEOREM_TO_INVARIANT: Mapping[str, str] = MappingProxyType(
    {
        "optimizer.search.nonnegative_cost": "SearchCostNonNegative",
        "optimizer.search.selection_admitted": "SelectedCandidatesAreAdmitted",
        "optimizer.search.eligible_selection": "SelectedCandidatesAreEligible",
        "optimizer.search.conflict_exclusion": "SearchConflictExclusion",
        "optimizer.search.no_admission_bypass": "NoAdmissionBypass",
        "optimizer.materialization.selection_required": (
            "MaterializationRequiresSearchSelection"
        ),
        "optimizer.rewrite.additive_overlay": "AdditiveOverlayPreservesBase",
    }
)


class SearchFormalConsistencyError(AssertionError):
    """Raised when a concrete search result violates a formal search theorem."""

    def __init__(self, theorem_id: str, message: str) -> None:
        super().__init__(f"{theorem_id}: {message}")
        self.theorem_id = theorem_id


@dataclass(frozen=True)
class SearchFormalConsistencyReport:
    ok: bool
    theorem_results: Mapping[str, bool]
    selected_candidate_ids: tuple[str, ...]
    materialized_candidate_ids: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "theorem_results",
            MappingProxyType(dict(self.theorem_results)),
        )
        object.__setattr__(
            self,
            "selected_candidate_ids",
            tuple(self.selected_candidate_ids),
        )
        object.__setattr__(
            self,
            "materialized_candidate_ids",
            tuple(self.materialized_candidate_ids),
        )
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


def check_search_result_against_formal_model(
    *,
    result: OptimizationResult,
    candidate_universe: Iterable[RewriteCandidate],
    initial_graph: OptimizerGraph,
    materialized_candidate_ids: Iterable[str] = (),
    conflict_tokens: Mapping[str, str] | None = None,
    max_quality_risk: float | None = None,
) -> SearchFormalConsistencyReport:
    """Check concrete search output against `AltheaOptimizerSafety.tla`.

    This is not an optimality checker. It is an executable refinement map for
    the search-level safety invariants in the formal model.
    """

    candidates = {candidate.candidate_id: candidate for candidate in candidate_universe}
    selected = tuple(result.applied_candidates)
    materialized = tuple(materialized_candidate_ids)
    theorem_results = {theorem_id: True for theorem_id in SEARCH_THEOREM_TO_INVARIANT}

    _require(
        result.final_cost >= 0.0,
        "optimizer.search.nonnegative_cost",
        f"final_cost={result.final_cost}",
    )
    _require(
        set(selected).issubset(candidates),
        "optimizer.search.selection_admitted",
        "selected candidate was not in the admitted candidate universe",
    )
    _require(
        set(selected).issubset(candidates),
        "optimizer.search.no_admission_bypass",
        "selected candidate bypassed the admitted candidate universe",
    )
    _require(
        set(materialized).issubset(set(selected)),
        "optimizer.materialization.selection_required",
        "materialized candidate was not selected by search",
    )
    _require(
        set(materialized).issubset(candidates),
        "optimizer.search.no_admission_bypass",
        "materialized candidate bypassed the admitted candidate universe",
    )
    _check_selected_candidates_are_eligible(
        selected=selected,
        candidates=candidates,
        result=result,
        max_quality_risk=max_quality_risk,
    )
    _check_conflicts(
        selected=selected,
        candidates=candidates,
        conflict_tokens=conflict_tokens,
    )
    _check_additive_overlay(
        before=initial_graph,
        after=result.graph,
    )
    return SearchFormalConsistencyReport(
        ok=True,
        theorem_results=theorem_results,
        selected_candidate_ids=selected,
        materialized_candidate_ids=materialized,
        metadata={
            "formal_model": "formal/althea/AltheaOptimizerSafety.tla",
            "property_id": "AltheaOptimizerSafetyKernel",
            "invariants": dict(SEARCH_THEOREM_TO_INVARIANT),
        },
    )


def _check_selected_candidates_are_eligible(
    *,
    selected: tuple[str, ...],
    candidates: Mapping[str, RewriteCandidate],
    result: OptimizationResult,
    max_quality_risk: float | None,
) -> None:
    accepted_delta_by_id = _accepted_cost_delta_by_candidate(result)
    accepted_score_by_id = _accepted_ir_score_by_candidate(result)
    for candidate_id in selected:
        candidate = candidates[candidate_id]
        score = accepted_score_by_id.get(candidate_id)
        if score is not None:
            _require(
                score.get("admissible") is not False,
                "optimizer.search.eligible_selection",
                f"selected candidate {candidate_id!r} had inadmissible IR score",
            )
            _require(
                not score.get("rejection_reason"),
                "optimizer.search.eligible_selection",
                (
                    f"selected candidate {candidate_id!r} carried IR rejection "
                    f"reason {score.get('rejection_reason')!r}"
                ),
            )
        cost_delta = accepted_delta_by_id.get(
            candidate_id,
            candidate.estimated_cost_delta,
        )
        _require(
            cost_delta < 0.0,
            "optimizer.search.eligible_selection",
            f"selected candidate {candidate_id!r} was not beneficial",
        )
        if max_quality_risk is not None:
            _require(
                candidate.quality_regression_upper_bound <= max_quality_risk,
                "optimizer.search.eligible_selection",
                (
                    f"selected candidate {candidate_id!r} quality risk "
                    f"{candidate.quality_regression_upper_bound} exceeded "
                    f"{max_quality_risk}"
                ),
            )


def _accepted_cost_delta_by_candidate(result: OptimizationResult) -> dict[str, float]:
    deltas: dict[str, float] = {}
    for event in result.trace:
        if event.decision != "accepted":
            continue
        raw_score = event.metadata.get("ir_score")
        if isinstance(raw_score, Mapping) and "expected_cost_delta" in raw_score:
            deltas[event.candidate_id] = float(raw_score["expected_cost_delta"])
        else:
            # Scalar candidates are checked against their candidate record.
            continue
    return deltas


def _accepted_ir_score_by_candidate(
    result: OptimizationResult,
) -> dict[str, Mapping[str, Any]]:
    scores: dict[str, Mapping[str, Any]] = {}
    for event in result.trace:
        if event.decision != "accepted":
            continue
        raw_score = event.metadata.get("ir_score")
        if isinstance(raw_score, Mapping):
            scores[event.candidate_id] = raw_score
    return scores


def _check_conflicts(
    *,
    selected: tuple[str, ...],
    candidates: Mapping[str, RewriteCandidate],
    conflict_tokens: Mapping[str, str] | None,
) -> None:
    seen: set[str] = set()
    for candidate_id in selected:
        candidate = candidates[candidate_id]
        token = (
            conflict_tokens.get(candidate_id)
            if conflict_tokens is not None
            else _fallback_conflict_token(candidate)
        )
        if token is None:
            continue
        _require(
            token not in seen,
            "optimizer.search.conflict_exclusion",
            f"duplicate selected conflict token {token!r}",
        )
        seen.add(token)


def _fallback_conflict_token(candidate: RewriteCandidate) -> str | None:
    if candidate.conflict_key is None:
        return None
    return f"{candidate.template_id}:{candidate.conflict_key}"


def _check_additive_overlay(
    *,
    before: OptimizerGraph,
    after: OptimizerGraph,
) -> None:
    missing_nodes = sorted(set(before.nodes) - set(after.nodes))
    _require(
        not missing_nodes,
        "optimizer.rewrite.additive_overlay",
        "missing base nodes: " + ", ".join(missing_nodes),
    )
    for node_id, before_node in before.nodes.items():
        _require(
            after.nodes[node_id].subtype == before_node.subtype,
            "optimizer.rewrite.additive_overlay",
            f"base node {node_id!r} changed subtype",
        )
        _require(
            dict(after.nodes[node_id].attrs) == dict(before_node.attrs),
            "optimizer.rewrite.additive_overlay",
            f"base node {node_id!r} changed attrs",
        )

    missing_edges = sorted(set(before.edges) - set(after.edges))
    _require(
        not missing_edges,
        "optimizer.rewrite.additive_overlay",
        "missing base edges: " + ", ".join(missing_edges),
    )
    for edge_id, before_edge in before.edges.items():
        after_edge = after.edges[edge_id]
        _require(
            (
                after_edge.src == before_edge.src
                and after_edge.dst == before_edge.dst
                and after_edge.edge_type == before_edge.edge_type
            ),
            "optimizer.rewrite.additive_overlay",
            f"base edge {edge_id!r} changed route",
        )
        _require(
            dict(after_edge.attrs) == dict(before_edge.attrs),
            "optimizer.rewrite.additive_overlay",
            f"base edge {edge_id!r} changed attrs",
        )


def _require(condition: bool, theorem_id: str, message: str) -> None:
    if not condition:
        raise SearchFormalConsistencyError(theorem_id, message)
