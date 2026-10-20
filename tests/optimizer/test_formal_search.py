from __future__ import annotations

from collections.abc import Iterable

import pytest

from okg.optimizer import (
    BeamSearchOptimizer,
    CostEstimate,
    MaterializationPlan,
    OptimizerEdge,
    OptimizerGraph,
    OptimizerNode,
    OptimizationEvent,
    OptimizationResult,
    ParameterSchema,
    PayloadSummarySpec,
    QualityEstimate,
    RewriteCandidate,
    RewriteSurface,
    RewriteTemplate,
    RewriteTier,
    RewriteValidationError,
    RollbackPolicy,
    BoundedTargetGenerator,
    SearchFormalConsistencyError,
    check_search_result_against_formal_model,
    summarize_payload_template,
)


class _StaticCostModel:
    def estimate(self, graph: OptimizerGraph) -> CostEstimate:
        return CostEstimate(total=float(graph.attrs.get("cost", 1.0)))


class _StaticQualityModel:
    def estimate(self, graph: OptimizerGraph) -> QualityEstimate:
        return QualityEstimate(score=float(graph.attrs.get("quality", 1.0)))


def test_formal_checker_accepts_valid_search_result() -> None:
    graph = _base_graph()
    candidate = _candidate("candidate:1")
    result = _result(
        graph=candidate.apply(graph),
        applied=("candidate:1",),
        trace=(_accepted_event("candidate:1"),),
    )

    report = check_search_result_against_formal_model(
        result=result,
        candidate_universe=(candidate,),
        initial_graph=graph,
        materialized_candidate_ids=("candidate:1",),
    )

    assert report.ok is True
    assert report.selected_candidate_ids == ("candidate:1",)
    assert report.metadata["formal_model"] == "formal/althea/AltheaOptimizerSafety.tla"
    assert report.metadata["property_id"] == "AltheaOptimizerSafetyKernel"


def test_formal_checker_rejects_selected_candidate_outside_admitted_universe() -> None:
    graph = _base_graph()
    result = _result(
        graph=graph,
        applied=("candidate:missing",),
        trace=(_accepted_event("candidate:missing"),),
    )

    with pytest.raises(SearchFormalConsistencyError) as excinfo:
        check_search_result_against_formal_model(
            result=result,
            candidate_universe=(_candidate("candidate:1"),),
            initial_graph=graph,
        )

    assert excinfo.value.theorem_id == "optimizer.search.selection_admitted"


def test_formal_checker_rejects_materialization_without_selection() -> None:
    graph = _base_graph()
    candidate = _candidate("candidate:1")
    result = _result(graph=graph, applied=())

    with pytest.raises(SearchFormalConsistencyError) as excinfo:
        check_search_result_against_formal_model(
            result=result,
            candidate_universe=(candidate,),
            initial_graph=graph,
            materialized_candidate_ids=("candidate:1",),
        )

    assert excinfo.value.theorem_id == "optimizer.materialization.selection_required"


def test_formal_checker_rejects_non_beneficial_candidate() -> None:
    graph = _base_graph()
    candidate = _candidate("candidate:1", estimated_cost_delta=0.0)
    result = _result(
        graph=graph,
        applied=("candidate:1",),
        trace=(_accepted_event("candidate:1", cost_delta=0.0),),
    )

    with pytest.raises(SearchFormalConsistencyError) as excinfo:
        check_search_result_against_formal_model(
            result=result,
            candidate_universe=(candidate,),
            initial_graph=graph,
        )

    assert excinfo.value.theorem_id == "optimizer.search.eligible_selection"


def test_formal_checker_rejects_inadmissible_ir_score() -> None:
    graph = _base_graph()
    candidate = _candidate("candidate:1")
    result = _result(
        graph=candidate.apply(graph),
        applied=("candidate:1",),
        trace=(
            _accepted_event(
                "candidate:1",
                admissible=False,
                rejection_reason="registry_mismatch",
            ),
        ),
    )

    with pytest.raises(SearchFormalConsistencyError) as excinfo:
        check_search_result_against_formal_model(
            result=result,
            candidate_universe=(candidate,),
            initial_graph=graph,
        )

    assert excinfo.value.theorem_id == "optimizer.search.eligible_selection"


def test_formal_checker_rejects_conflicting_candidates() -> None:
    graph = _base_graph()
    candidates = (
        _candidate("candidate:1", conflict_key="same-target"),
        _candidate("candidate:2", conflict_key="same-target"),
    )
    result = _result(
        graph=graph,
        applied=("candidate:1", "candidate:2"),
        trace=(
            _accepted_event("candidate:1"),
            _accepted_event("candidate:2"),
        ),
    )

    with pytest.raises(SearchFormalConsistencyError) as excinfo:
        check_search_result_against_formal_model(
            result=result,
            candidate_universe=candidates,
            initial_graph=graph,
        )

    assert excinfo.value.theorem_id == "optimizer.search.conflict_exclusion"


def test_formal_checker_rejects_base_graph_attr_mutation() -> None:
    graph = _base_graph()
    candidate = _candidate(
        "candidate:1",
        apply=lambda g: g.with_node_attrs("node:base", {"payload": "mutated"}),
    )
    result = _result(
        graph=candidate.apply(graph),
        applied=("candidate:1",),
        trace=(_accepted_event("candidate:1"),),
    )

    with pytest.raises(SearchFormalConsistencyError) as excinfo:
        check_search_result_against_formal_model(
            result=result,
            candidate_universe=(candidate,),
            initial_graph=graph,
        )

    assert excinfo.value.theorem_id == "optimizer.rewrite.additive_overlay"


def test_optimizer_rejects_base_graph_mutation() -> None:
    graph = _base_graph()
    template = _template([
        _candidate(
            "candidate:mutates-base",
            apply=lambda g: g.with_node_attrs("node:base", {"payload": "mutated"}),
        )
    ])
    optimizer = BeamSearchOptimizer(
        cost_model=_StaticCostModel(),
        quality_model=_StaticQualityModel(),
        quality_target=0.0,
        max_depth=1,
    )

    with pytest.raises(RewriteValidationError, match="changed attrs"):
        optimizer.optimize(graph, (template,))


def test_payload_summary_template_adds_overlay_without_mutating_base_node() -> None:
    graph = _base_graph()
    template = summarize_payload_template(
        (
            PayloadSummarySpec(
                candidate_id="candidate:summary",
                node_id="node:base",
                summary_id="summary:1",
                summary_kind="short",
                original_payload_cost=10.0,
                summarized_payload_cost=2.0,
                ignored_clutter_cost=0.0,
                quality_risk_upper_bound=0.0,
                retained_fields=("title",),
                required_fields=("title",),
                evidence_refs=("trace:1",),
                quality_assumption="retained fields cover required fields",
            ),
        )
    )
    optimizer = BeamSearchOptimizer(
        cost_model=_StaticCostModel(),
        quality_model=_StaticQualityModel(),
        quality_target=0.0,
        max_depth=1,
    )

    result = optimizer.optimize(graph, (template,))

    assert result.applied_candidates == ("candidate:summary",)
    assert dict(result.graph.nodes["node:base"].attrs) == {"payload": "original"}
    assert "optimizer_payload_summary:summary:1" in result.graph.nodes
    assert "optimizer_payload_summary_edge:summary:1" in result.graph.edges


def _base_graph() -> OptimizerGraph:
    graph = OptimizerGraph(attrs={"cost": 1.0, "quality": 1.0})
    return graph.add_node(
        OptimizerNode(
            "node:base",
            "source_document",
            attrs={"payload": "original"},
        )
    )


def _candidate(
    candidate_id: str,
    *,
    estimated_cost_delta: float = -1.0,
    conflict_key: str | None = None,
    apply=None,
) -> RewriteCandidate:
    def add_overlay(graph: OptimizerGraph) -> OptimizerGraph:
        overlay_node_id = f"node:overlay:{candidate_id}"
        overlay_edge_id = f"edge:overlay:{candidate_id}"
        return graph.add_node(
            OptimizerNode(
                overlay_node_id,
                "optimizer_overlay",
                attrs={"candidate_id": candidate_id},
            )
        ).add_edge(
            OptimizerEdge(
                overlay_edge_id,
                src="node:base",
                dst=overlay_node_id,
                edge_type="has_optimizer_overlay",
            )
        )

    return RewriteCandidate(
        candidate_id=candidate_id,
        template_id="test_rewrite",
        params={"candidate_id": candidate_id},
        apply=add_overlay if apply is None else apply,
        estimated_cost_delta=estimated_cost_delta,
        estimated_quality_delta=0.0,
        quality_regression_upper_bound=0.0,
        evidence_refs=("trace:1",),
        conflict_key=conflict_key,
    )


def _result(
    *,
    graph: OptimizerGraph,
    applied: tuple[str, ...],
    final_cost: float = 0.0,
    trace: tuple[OptimizationEvent, ...] = (),
) -> OptimizationResult:
    return OptimizationResult(
        graph=graph,
        initial_cost=1.0,
        final_cost=final_cost,
        initial_quality=1.0,
        final_quality=1.0,
        applied_candidates=applied,
        trace=trace,
    )


def _accepted_event(
    candidate_id: str,
    *,
    cost_delta: float = -1.0,
    admissible: bool = True,
    rejection_reason: str | None = None,
) -> OptimizationEvent:
    return OptimizationEvent(
        depth=0,
        candidate_id=candidate_id,
        template_id="test_rewrite",
        decision="accepted",
        reason="test",
        cost=0.0,
        quality=1.0,
        metadata={
            "ir_score": {
                "admissible": admissible,
                "rejection_reason": rejection_reason,
                "expected_cost_delta": cost_delta,
            }
        },
    )


def _template(candidates: Iterable[RewriteCandidate]) -> RewriteTemplate:
    candidate_tuple = tuple(candidates)

    return RewriteTemplate(
        rewrite_id="test_rewrite",
        surface=RewriteSurface.TOPOLOGY,
        parameter_schema=ParameterSchema({"candidate_id": "str"}),
        target_generator=BoundedTargetGenerator(
            name="test_generator",
            max_candidates=max(1, len(candidate_tuple)),
            evidence_sources=("test",),
        ),
        preconditions=("test precondition",),
        dependencies=("test dependency",),
        materialization=MaterializationPlan(
            kind="test_overlay",
            surface=RewriteSurface.TOPOLOGY,
            payload_schema=ParameterSchema({"candidate_id": "str"}),
        ),
        cost_estimator="test_cost",
        error_estimator="test_quality",
        conflict_group="test_conflict",
        rollback=RollbackPolicy(
            mechanism="discard_test_overlay",
            eviction_signal="test_regression",
        ),
        generate=lambda _graph: candidate_tuple,
        tier=RewriteTier.TIER_2,
    )
