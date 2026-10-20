"""Tier-1 mechanically bounded rewrite families."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from .bounds import AdoptionAssumption, BenefitBounds, RewriteTier
from .graph import OptimizerEdge, OptimizerGraph, OptimizerNode
from .ir import IREffect, IRBinding, IROp, IROpKind, IRProgram, IRSymbol, IRType
from .rewrite import (
    BoundedTargetGenerator,
    MaterializationPlan,
    ParameterSchema,
    RewriteCandidate,
    RewriteSurface,
    RewriteTemplate,
    RewriteValidationError,
    RollbackPolicy,
)


NODE = IRType("node_ref")
EDGE_SET = IRType("collection", {"element": "edge_ref"})
NODE_SET = IRType("collection", {"element": "node_ref"})
GRAPH_DELTA = IRType("graph_delta")
RECORD = IRType("record")
SCALAR = IRType("scalar")
BOOL = IRType("bool")


@dataclass(frozen=True)
class OverlayEdgeSpec:
    candidate_id: str
    edge_id: str
    src: str
    dst: str
    edge_type: str
    baseline_path_cost: float
    shortcut_path_cost: float
    ignored_clutter_cost: float
    quality_risk_upper_bound: float
    evidence_refs: tuple[str, ...]
    quality_assumption: str
    agentic_decision: bool = False
    anchor_resolution_cost: float = 0.0
    evidence_packet_cost: float = 0.0
    policy_decision_cost: float = 0.0
    accept_path_cost: float = 0.0
    fallback_path_cost: float = 0.0
    accept_probability: float = 0.0
    branch_overlay_cost: float = 0.0
    conflict_key: str | None = None


@dataclass(frozen=True)
class MaterializedAggregateSpec:
    candidate_id: str
    aggregate_id: str
    anchor_node_id: str
    aggregate_kind: str
    baseline_expand_cost: float
    aggregate_cost: float
    ignored_clutter_cost: float
    quality_risk_upper_bound: float
    retained_fields: tuple[str, ...]
    required_fields: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    quality_assumption: str


@dataclass(frozen=True)
class RouteMaterializationSpec:
    candidate_id: str
    materialization_id: str
    anchor_node_id: str
    route_signature: tuple[str, ...]
    output_count: int
    set_checksum: str
    baseline_route_cost: float
    materialized_read_cost: float
    install_write_cost: float
    storage_overhead_bytes: int
    ignored_clutter_cost: float
    quality_risk_upper_bound: float
    evidence_refs: tuple[str, ...]
    quality_assumption: str


@dataclass(frozen=True)
class MaterializedIntersectionSpec:
    candidate_id: str
    intersection_id: str
    anchor_node_id: str
    operand_route_signatures: tuple[tuple[str, ...], ...]
    operand_set_checksums: tuple[str, ...]
    output_count: int
    set_checksum: str
    baseline_operand_cost: float
    intersection_compute_cost: float
    materialized_read_cost: float
    install_write_cost: float
    storage_overhead_bytes: int
    ignored_clutter_cost: float
    quality_risk_upper_bound: float
    evidence_refs: tuple[str, ...]
    quality_assumption: str


@dataclass(frozen=True)
class AnchorDisambiguationSpec:
    candidate_id: str
    disambiguation_id: str
    anchor_node_id: str
    anchor_query: str
    allowed_anchor_subtypes: tuple[str, ...]
    baseline_resolution_cost: float
    disambiguated_resolution_cost: float
    inspect_cost: float
    install_write_cost: float
    storage_overhead_bytes: int
    ambiguity_count: int
    precision: float
    coverage: float
    ignored_clutter_cost: float
    quality_risk_upper_bound: float
    evidence_refs: tuple[str, ...]
    quality_assumption: str


@dataclass(frozen=True)
class RoutePolicyReplacementSpec:
    candidate_id: str
    route_id: str
    replacement_operator: str
    baseline_route_cost: float
    replacement_route_cost: float
    ignored_clutter_cost: float
    quality_risk_upper_bound: float
    evidence_refs: tuple[str, ...]
    quality_assumption: str


@dataclass(frozen=True)
class PayloadSummarySpec:
    candidate_id: str
    node_id: str
    summary_id: str
    summary_kind: str
    original_payload_cost: float
    summarized_payload_cost: float
    ignored_clutter_cost: float
    quality_risk_upper_bound: float
    retained_fields: tuple[str, ...]
    required_fields: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    quality_assumption: str


def add_overlay_edge_template(
    specs: Iterable[OverlayEdgeSpec],
    *,
    max_candidates: int | None = None,
) -> RewriteTemplate:
    specs_tuple = tuple(specs)

    def generate(graph: OptimizerGraph) -> tuple[RewriteCandidate, ...]:
        return tuple(_overlay_edge_candidate(graph, spec) for spec in specs_tuple)

    return RewriteTemplate(
        rewrite_id="add_overlay_edge",
        surface=RewriteSurface.TOPOLOGY,
        parameter_schema=ParameterSchema(
            {"edge_id": "str", "src": "str", "dst": "str", "edge_type": "str"}
        ),
        target_generator=BoundedTargetGenerator(
            name="witnessed_evidence_paths",
            max_candidates=max_candidates or max(1, len(specs_tuple)),
            evidence_sources=("trace_evidence_paths", "quality_labels"),
        ),
        preconditions=("endpoints exist", "catalog edge route exists"),
        dependencies=("quality_label", "catalog_narrowing"),
        materialization=MaterializationPlan(
            kind="optimizer_overlay_edge",
            surface=RewriteSurface.TOPOLOGY,
            payload_schema=ParameterSchema(
                {"edge_id": "str", "src": "str", "dst": "str", "edge_type": "str"}
            ),
        ),
        cost_estimator="shortcut_path_cost_bound_v0",
        error_estimator="typed_evidence_path_quality_bound_v0",
        conflict_group="topology.overlay_edge",
        rollback=RollbackPolicy(
            mechanism="branch_abandon_or_overlay_supersede",
            eviction_signal="heldout_quality_or_usage_regression",
        ),
        generate=generate,
        tier=RewriteTier.TIER_1,
    )


def materialize_aggregate_template(
    specs: Iterable[MaterializedAggregateSpec],
    *,
    max_candidates: int | None = None,
) -> RewriteTemplate:
    specs_tuple = tuple(specs)

    def generate(graph: OptimizerGraph) -> tuple[RewriteCandidate, ...]:
        return tuple(_aggregate_candidate(graph, spec) for spec in specs_tuple)

    return RewriteTemplate(
        rewrite_id="materialize_aggregate",
        surface=RewriteSurface.MATERIALIZATION,
        parameter_schema=ParameterSchema(
            {
                "aggregate_id": "str",
                "anchor_node_id": "str",
                "aggregate_kind": "str",
            }
        ),
        target_generator=BoundedTargetGenerator(
            name="high_cost_hub_aggregates",
            max_candidates=max_candidates or max(1, len(specs_tuple)),
            evidence_sources=("mcp_trace_events", "quality_labels"),
        ),
        preconditions=("anchor node exists", "required fields retained"),
        dependencies=("quality_label", "derived_artifact_policy"),
        materialization=MaterializationPlan(
            kind="optimizer_materialized_aggregate",
            surface=RewriteSurface.MATERIALIZATION,
            payload_schema=ParameterSchema(
                {
                    "aggregate_id": "str",
                    "anchor_node_id": "str",
                    "aggregate_kind": "str",
                }
            ),
        ),
        cost_estimator="aggregate_response_cost_bound_v0",
        error_estimator="retained_fields_quality_bound_v0",
        conflict_group="materialization.aggregate",
        rollback=RollbackPolicy(
            mechanism="derived_artifact_retire",
            eviction_signal="stale_trace_window_or_quality_regression",
        ),
        generate=generate,
        tier=RewriteTier.TIER_1,
    )


def materialize_route_template(
    specs: Iterable[RouteMaterializationSpec],
    *,
    max_candidates: int | None = None,
) -> RewriteTemplate:
    specs_tuple = tuple(specs)

    def generate(graph: OptimizerGraph) -> tuple[RewriteCandidate, ...]:
        return tuple(_route_materialization_candidate(graph, spec) for spec in specs_tuple)

    return RewriteTemplate(
        rewrite_id="materialize_route",
        surface=RewriteSurface.MATERIALIZATION,
        parameter_schema=ParameterSchema(
            {
                "materialization_id": "str",
                "anchor_node_id": "str",
                "route_signature": "tuple[str]",
            }
        ),
        target_generator=BoundedTargetGenerator(
            name="repeated_path_witness_route_sets",
            max_candidates=max_candidates or max(1, len(specs_tuple)),
            evidence_sources=("route_slate_trace_events", "path_witnesses"),
        ),
        preconditions=("anchor node exists", "route signature is stable"),
        dependencies=("path_witness", "set_checksum", "quality_contract"),
        materialization=MaterializationPlan(
            kind="optimizer_route_materialization",
            surface=RewriteSurface.MATERIALIZATION,
            payload_schema=ParameterSchema(
                {
                    "materialization_id": "str",
                    "anchor_node_id": "str",
                    "route_signature": "tuple[str]",
                }
            ),
        ),
        cost_estimator="route_materialization_cost_bound_v0",
        error_estimator="route_set_preservation_bound_v0",
        conflict_group="materialization.route",
        rollback=RollbackPolicy(
            mechanism="branch_abandon_or_route_materialization_supersede",
            eviction_signal="heldout_set_quality_or_storage_regression",
        ),
        generate=generate,
        tier=RewriteTier.TIER_1,
    )


def materialize_intersection_template(
    specs: Iterable[MaterializedIntersectionSpec],
    *,
    max_candidates: int | None = None,
) -> RewriteTemplate:
    specs_tuple = tuple(specs)

    def generate(graph: OptimizerGraph) -> tuple[RewriteCandidate, ...]:
        return tuple(_intersection_candidate(graph, spec) for spec in specs_tuple)

    return RewriteTemplate(
        rewrite_id="materialize_intersection",
        surface=RewriteSurface.MATERIALIZATION,
        parameter_schema=ParameterSchema(
            {
                "intersection_id": "str",
                "anchor_node_id": "str",
                "operand_count": "int",
            }
        ),
        target_generator=BoundedTargetGenerator(
            name="repeated_conjunctive_route_sets",
            max_candidates=max_candidates or max(1, len(specs_tuple)),
            evidence_sources=("route_slate_trace_events", "set_checksums"),
        ),
        preconditions=("anchor node exists", "operand checksums are stable"),
        dependencies=("operand_route_signatures", "set_checksum", "quality_contract"),
        materialization=MaterializationPlan(
            kind="optimizer_materialized_intersection",
            surface=RewriteSurface.MATERIALIZATION,
            payload_schema=ParameterSchema(
                {
                    "intersection_id": "str",
                    "anchor_node_id": "str",
                    "operand_count": "int",
                }
            ),
        ),
        cost_estimator="intersection_materialization_cost_bound_v0",
        error_estimator="intersection_set_preservation_bound_v0",
        conflict_group="materialization.intersection",
        rollback=RollbackPolicy(
            mechanism="branch_abandon_or_intersection_supersede",
            eviction_signal="heldout_set_quality_or_storage_regression",
        ),
        generate=generate,
        tier=RewriteTier.TIER_1,
    )


def add_anchor_disambiguation_template(
    specs: Iterable[AnchorDisambiguationSpec],
    *,
    max_candidates: int | None = None,
) -> RewriteTemplate:
    specs_tuple = tuple(specs)

    def generate(graph: OptimizerGraph) -> tuple[RewriteCandidate, ...]:
        return tuple(_anchor_candidate(graph, spec) for spec in specs_tuple)

    return RewriteTemplate(
        rewrite_id="add_anchor_disambiguation",
        surface=RewriteSurface.SCENT,
        parameter_schema=ParameterSchema(
            {
                "disambiguation_id": "str",
                "anchor_node_id": "str",
                "anchor_query": "str",
            }
        ),
        target_generator=BoundedTargetGenerator(
            name="ambiguous_answer_blind_anchors",
            max_candidates=max_candidates or max(1, len(specs_tuple)),
            evidence_sources=("route_slate_trace_events", "anchor_resolution_traces"),
        ),
        preconditions=("anchor node exists", "anchor evidence is answer-blind"),
        dependencies=("anchor_precision", "anchor_coverage", "quality_contract"),
        materialization=MaterializationPlan(
            kind="optimizer_anchor_disambiguation",
            surface=RewriteSurface.SCENT,
            payload_schema=ParameterSchema(
                {
                    "disambiguation_id": "str",
                    "anchor_node_id": "str",
                    "anchor_query": "str",
                }
            ),
        ),
        cost_estimator="anchor_disambiguation_cost_bound_v0",
        error_estimator="anchor_precision_coverage_bound_v0",
        conflict_group="anchor.disambiguation",
        rollback=RollbackPolicy(
            mechanism="branch_abandon_or_anchor_policy_supersede",
            eviction_signal="heldout_anchor_precision_or_coverage_regression",
        ),
        generate=generate,
        tier=RewriteTier.TIER_1,
    )


def replace_route_policy_template(
    specs: Iterable[RoutePolicyReplacementSpec],
    *,
    max_candidates: int | None = None,
) -> RewriteTemplate:
    specs_tuple = tuple(specs)

    def generate(graph: OptimizerGraph) -> tuple[RewriteCandidate, ...]:
        return tuple(_route_candidate(graph, spec) for spec in specs_tuple)

    return RewriteTemplate(
        rewrite_id="replace_route_policy",
        surface=RewriteSurface.ROUTE_POLICY,
        parameter_schema=ParameterSchema(
            {"route_id": "str", "replacement_operator": "str"}
        ),
        target_generator=BoundedTargetGenerator(
            name="repeated_route_outcomes",
            max_candidates=max_candidates or max(1, len(specs_tuple)),
            evidence_sources=("route_trace_events", "heldout_route_labels"),
        ),
        preconditions=("route observed", "replacement operator available"),
        dependencies=("route_manifest_checksum", "quality_label"),
        materialization=MaterializationPlan(
            kind="optimizer_route_policy_replacement",
            surface=RewriteSurface.ROUTE_POLICY,
            payload_schema=ParameterSchema(
                {"route_id": "str", "replacement_operator": "str"}
            ),
            mutates_route_manifest=False,
        ),
        cost_estimator="route_replacement_cost_bound_v0",
        error_estimator="route_quality_bound_v0",
        conflict_group="route_policy.replacement",
        rollback=RollbackPolicy(
            mechanism="route_policy_proposal_supersede",
            eviction_signal="route_follow_regression",
        ),
        generate=generate,
        tier=RewriteTier.TIER_1,
    )


def summarize_payload_template(
    specs: Iterable[PayloadSummarySpec],
    *,
    max_candidates: int | None = None,
) -> RewriteTemplate:
    specs_tuple = tuple(specs)

    def generate(graph: OptimizerGraph) -> tuple[RewriteCandidate, ...]:
        return tuple(_payload_candidate(graph, spec) for spec in specs_tuple)

    return RewriteTemplate(
        rewrite_id="summarize_payload",
        surface=RewriteSurface.PAYLOAD,
        parameter_schema=ParameterSchema(
            {"node_id": "str", "summary_id": "str", "summary_kind": "str"}
        ),
        target_generator=BoundedTargetGenerator(
            name="payload_bloat_with_labels",
            max_candidates=max_candidates or max(1, len(specs_tuple)),
            evidence_sources=("mcp_trace_events", "heldout_quality_labels"),
        ),
        preconditions=("node exists", "required fields retained"),
        dependencies=("quality_label", "payload_summary_policy"),
        materialization=MaterializationPlan(
            kind="optimizer_payload_summary",
            surface=RewriteSurface.PAYLOAD,
            payload_schema=ParameterSchema(
                {"node_id": "str", "summary_id": "str", "summary_kind": "str"}
            ),
        ),
        cost_estimator="payload_summary_cost_bound_v0",
        error_estimator="retained_fields_quality_bound_v0",
        conflict_group="payload.summary",
        rollback=RollbackPolicy(
            mechanism="payload_summary_supersede",
            eviction_signal="quality_regression_or_stale_summary",
        ),
        generate=generate,
        tier=RewriteTier.TIER_1,
    )


def _overlay_edge_candidate(
    graph: OptimizerGraph,
    spec: OverlayEdgeSpec,
) -> RewriteCandidate:
    _require_node(graph, spec.src)
    _require_node(graph, spec.dst)
    _require_edge_route(graph, spec.src, spec.edge_type, spec.dst)
    bounds = _bounds(
        before=spec.baseline_path_cost,
        after=spec.shortcut_path_cost,
        ignored_clutter_cost=spec.ignored_clutter_cost,
        quality_risk_upper_bound=spec.quality_risk_upper_bound,
        adoption_assumption=(
            AdoptionAssumption.DETERMINISTIC_TOOL_SURFACE
            if spec.agentic_decision
            else AdoptionAssumption.ALWAYS_TAKEN_WHEN_RELEVANT
        ),
        quality_assumption=spec.quality_assumption,
    )
    return RewriteCandidate(
        candidate_id=spec.candidate_id,
        template_id="add_overlay_edge",
        params={
            "edge_id": spec.edge_id,
            "src": spec.src,
            "dst": spec.dst,
            "edge_type": spec.edge_type,
        },
        apply=lambda g: g.add_edge(
            OptimizerEdge(
                edge_id=spec.edge_id,
                src=spec.src,
                dst=spec.dst,
                edge_type=spec.edge_type,
                attrs={"optimizer_tier": RewriteTier.TIER_1.value},
            )
        ),
        estimated_cost_delta=-bounds.benefit_upper_bound,
        estimated_quality_delta=-bounds.quality_risk_upper_bound,
        quality_regression_upper_bound=bounds.quality_risk_upper_bound,
        evidence_refs=spec.evidence_refs,
        benefit_bounds=bounds,
        before_ir=_overlay_before_ir(spec),
        install_ir=_overlay_install_ir(spec),
        after_ir=_overlay_after_ir(spec),
        equivalence_contract="typed_path_witness_preserves_target_identity",
        quality_contract=spec.quality_assumption,
        conflict_key=spec.conflict_key,
    )


def _aggregate_candidate(
    graph: OptimizerGraph,
    spec: MaterializedAggregateSpec,
) -> RewriteCandidate:
    _require_node(graph, spec.anchor_node_id)
    _require_fields(spec.retained_fields, spec.required_fields)
    node_id = f"optimizer_aggregate:{spec.aggregate_id}"
    edge_id = f"optimizer_aggregate_edge:{spec.aggregate_id}"
    bounds = _bounds(
        before=spec.baseline_expand_cost,
        after=spec.aggregate_cost,
        ignored_clutter_cost=spec.ignored_clutter_cost,
        quality_risk_upper_bound=spec.quality_risk_upper_bound,
        adoption_assumption=AdoptionAssumption.DETERMINISTIC_TOOL_SURFACE,
        quality_assumption=spec.quality_assumption,
    )

    def apply(graph: OptimizerGraph) -> OptimizerGraph:
        return graph.add_node(
            OptimizerNode(
                node_id=node_id,
                subtype="optimizer_materialized_aggregate",
                attrs={
                    "aggregate_id": spec.aggregate_id,
                    "aggregate_kind": spec.aggregate_kind,
                    "retained_fields": spec.retained_fields,
                },
            )
        ).add_edge(
            OptimizerEdge(
                edge_id=edge_id,
                src=spec.anchor_node_id,
                dst=node_id,
                edge_type="has_optimizer_aggregate",
            )
        )

    return RewriteCandidate(
        candidate_id=spec.candidate_id,
        template_id="materialize_aggregate",
        params={
            "aggregate_id": spec.aggregate_id,
            "anchor_node_id": spec.anchor_node_id,
            "aggregate_kind": spec.aggregate_kind,
        },
        apply=apply,
        estimated_cost_delta=-bounds.benefit_upper_bound,
        estimated_quality_delta=-bounds.quality_risk_upper_bound,
        quality_regression_upper_bound=bounds.quality_risk_upper_bound,
        evidence_refs=spec.evidence_refs,
        benefit_bounds=bounds,
        before_ir=_aggregate_before_ir(spec),
        install_ir=_aggregate_install_ir(spec, node_id=node_id, edge_id=edge_id),
        after_ir=_aggregate_after_ir(spec),
        equivalence_contract="deterministic_aggregate_over_same_anchor",
        quality_contract=spec.quality_assumption,
    )


def _route_candidate(
    graph: OptimizerGraph,
    spec: RoutePolicyReplacementSpec,
) -> RewriteCandidate:
    _require_route(graph, spec.route_id)
    _require_operator(graph, spec.replacement_operator)
    bounds = _bounds(
        before=spec.baseline_route_cost,
        after=spec.replacement_route_cost,
        ignored_clutter_cost=spec.ignored_clutter_cost,
        quality_risk_upper_bound=spec.quality_risk_upper_bound,
        adoption_assumption=AdoptionAssumption.DETERMINISTIC_TOOL_SURFACE,
        quality_assumption=spec.quality_assumption,
    )

    def apply(graph: OptimizerGraph) -> OptimizerGraph:
        existing = tuple(graph.attrs.get("route_policy_replacements", ()))
        return graph.with_attrs(
            {
                "route_policy_replacements": (
                    *existing,
                    {
                        "route_id": spec.route_id,
                        "replacement_operator": spec.replacement_operator,
                    },
                )
            }
        )

    return RewriteCandidate(
        candidate_id=spec.candidate_id,
        template_id="replace_route_policy",
        params={
            "route_id": spec.route_id,
            "replacement_operator": spec.replacement_operator,
        },
        apply=apply,
        estimated_cost_delta=-bounds.benefit_upper_bound,
        estimated_quality_delta=-bounds.quality_risk_upper_bound,
        quality_regression_upper_bound=bounds.quality_risk_upper_bound,
        evidence_refs=spec.evidence_refs,
        benefit_bounds=bounds,
        before_ir=_route_before_ir(spec),
        install_ir=_route_install_ir(spec),
        after_ir=_route_after_ir(spec),
        equivalence_contract="route_replacement_preserves_declared_output",
        quality_contract=spec.quality_assumption,
    )


def _route_materialization_candidate(
    graph: OptimizerGraph,
    spec: RouteMaterializationSpec,
) -> RewriteCandidate:
    _require_node(graph, spec.anchor_node_id)
    _require_non_empty_tuple(spec.route_signature, "route_signature")
    _require_positive_int(spec.output_count, "output_count")
    node_id = f"optimizer_route_materialization:{spec.materialization_id}"
    edge_id = f"optimizer_route_materialization_edge:{spec.materialization_id}"
    bounds = _bounds(
        before=spec.baseline_route_cost,
        after=spec.materialized_read_cost + spec.install_write_cost,
        ignored_clutter_cost=spec.ignored_clutter_cost,
        quality_risk_upper_bound=spec.quality_risk_upper_bound,
        adoption_assumption=AdoptionAssumption.DETERMINISTIC_TOOL_SURFACE,
        quality_assumption=spec.quality_assumption,
    )

    def apply(graph: OptimizerGraph) -> OptimizerGraph:
        return graph.add_node(
            OptimizerNode(
                node_id,
                subtype="optimizer_route_materialization",
                attrs={
                    "materialization_id": spec.materialization_id,
                    "route_signature": spec.route_signature,
                    "output_count": spec.output_count,
                    "set_checksum": spec.set_checksum,
                    "storage_overhead_bytes": spec.storage_overhead_bytes,
                },
            )
        ).add_edge(
            OptimizerEdge(
                edge_id,
                src=spec.anchor_node_id,
                dst=node_id,
                edge_type="has_optimizer_route_materialization",
            )
        )

    return RewriteCandidate(
        candidate_id=spec.candidate_id,
        template_id="materialize_route",
        params={
            "materialization_id": spec.materialization_id,
            "anchor_node_id": spec.anchor_node_id,
            "route_signature": spec.route_signature,
        },
        apply=apply,
        estimated_cost_delta=-bounds.benefit_upper_bound,
        estimated_quality_delta=-bounds.quality_risk_upper_bound,
        quality_regression_upper_bound=bounds.quality_risk_upper_bound,
        evidence_refs=spec.evidence_refs,
        benefit_bounds=bounds,
        before_ir=_route_materialization_before_ir(spec),
        install_ir=_route_materialization_install_ir(
            spec,
            node_id=node_id,
            edge_id=edge_id,
        ),
        after_ir=_route_materialization_after_ir(spec),
        equivalence_contract="route_materialization_preserves_candidate_set_checksum",
        quality_contract=spec.quality_assumption,
        conflict_key=f"route_materialization:{spec.anchor_node_id}:{spec.route_signature}",
    )


def _intersection_candidate(
    graph: OptimizerGraph,
    spec: MaterializedIntersectionSpec,
) -> RewriteCandidate:
    _require_node(graph, spec.anchor_node_id)
    if len(spec.operand_route_signatures) < 2:
        raise RewriteValidationError("intersection requires at least two operands")
    if len(spec.operand_set_checksums) != len(spec.operand_route_signatures):
        raise RewriteValidationError("intersection operand checksum count mismatch")
    _require_positive_int(spec.output_count, "output_count")
    node_id = f"optimizer_intersection:{spec.intersection_id}"
    edge_id = f"optimizer_intersection_edge:{spec.intersection_id}"
    before = spec.baseline_operand_cost + spec.intersection_compute_cost
    after = spec.materialized_read_cost + spec.install_write_cost
    bounds = _bounds(
        before=before,
        after=after,
        ignored_clutter_cost=spec.ignored_clutter_cost,
        quality_risk_upper_bound=spec.quality_risk_upper_bound,
        adoption_assumption=AdoptionAssumption.DETERMINISTIC_TOOL_SURFACE,
        quality_assumption=spec.quality_assumption,
    )

    def apply(graph: OptimizerGraph) -> OptimizerGraph:
        return graph.add_node(
            OptimizerNode(
                node_id,
                subtype="optimizer_materialized_intersection",
                attrs={
                    "intersection_id": spec.intersection_id,
                    "operand_route_signatures": spec.operand_route_signatures,
                    "operand_set_checksums": spec.operand_set_checksums,
                    "output_count": spec.output_count,
                    "set_checksum": spec.set_checksum,
                    "storage_overhead_bytes": spec.storage_overhead_bytes,
                },
            )
        ).add_edge(
            OptimizerEdge(
                edge_id,
                src=spec.anchor_node_id,
                dst=node_id,
                edge_type="has_optimizer_intersection",
            )
        )

    return RewriteCandidate(
        candidate_id=spec.candidate_id,
        template_id="materialize_intersection",
        params={
            "intersection_id": spec.intersection_id,
            "anchor_node_id": spec.anchor_node_id,
            "operand_count": len(spec.operand_route_signatures),
        },
        apply=apply,
        estimated_cost_delta=-bounds.benefit_upper_bound,
        estimated_quality_delta=-bounds.quality_risk_upper_bound,
        quality_regression_upper_bound=bounds.quality_risk_upper_bound,
        evidence_refs=spec.evidence_refs,
        benefit_bounds=bounds,
        before_ir=_intersection_before_ir(spec),
        install_ir=_intersection_install_ir(spec, node_id=node_id, edge_id=edge_id),
        after_ir=_intersection_after_ir(spec),
        equivalence_contract="materialized_intersection_preserves_operand_set_intersection",
        quality_contract=spec.quality_assumption,
        conflict_key=f"intersection:{spec.anchor_node_id}:{spec.operand_set_checksums}",
    )


def _anchor_candidate(
    graph: OptimizerGraph,
    spec: AnchorDisambiguationSpec,
) -> RewriteCandidate:
    _require_node(graph, spec.anchor_node_id)
    if not 0.0 <= spec.precision <= 1.0:
        raise RewriteValidationError("anchor precision must be in [0, 1]")
    if not 0.0 <= spec.coverage <= 1.0:
        raise RewriteValidationError("anchor coverage must be in [0, 1]")
    if spec.ambiguity_count <= 1:
        raise RewriteValidationError("anchor disambiguation requires ambiguity")
    node_id = f"optimizer_anchor_disambiguation:{spec.disambiguation_id}"
    edge_id = f"optimizer_anchor_disambiguation_edge:{spec.disambiguation_id}"
    after = spec.disambiguated_resolution_cost + spec.inspect_cost + spec.install_write_cost
    bounds = _bounds(
        before=spec.baseline_resolution_cost,
        after=after,
        ignored_clutter_cost=spec.ignored_clutter_cost,
        quality_risk_upper_bound=spec.quality_risk_upper_bound,
        adoption_assumption=AdoptionAssumption.DETERMINISTIC_TOOL_SURFACE,
        quality_assumption=spec.quality_assumption,
    )

    def apply(graph: OptimizerGraph) -> OptimizerGraph:
        return graph.add_node(
            OptimizerNode(
                node_id,
                subtype="optimizer_anchor_disambiguation",
                attrs={
                    "disambiguation_id": spec.disambiguation_id,
                    "anchor_query": spec.anchor_query,
                    "allowed_anchor_subtypes": spec.allowed_anchor_subtypes,
                    "ambiguity_count": spec.ambiguity_count,
                    "precision": spec.precision,
                    "coverage": spec.coverage,
                    "storage_overhead_bytes": spec.storage_overhead_bytes,
                },
            )
        ).add_edge(
            OptimizerEdge(
                edge_id,
                src=spec.anchor_node_id,
                dst=node_id,
                edge_type="has_optimizer_anchor_disambiguation",
            )
        )

    return RewriteCandidate(
        candidate_id=spec.candidate_id,
        template_id="add_anchor_disambiguation",
        params={
            "disambiguation_id": spec.disambiguation_id,
            "anchor_node_id": spec.anchor_node_id,
            "anchor_query": spec.anchor_query,
        },
        apply=apply,
        estimated_cost_delta=-bounds.benefit_upper_bound,
        estimated_quality_delta=-bounds.quality_risk_upper_bound,
        quality_regression_upper_bound=bounds.quality_risk_upper_bound,
        evidence_refs=spec.evidence_refs,
        benefit_bounds=bounds,
        before_ir=_anchor_before_ir(spec),
        install_ir=_anchor_install_ir(spec, node_id=node_id, edge_id=edge_id),
        after_ir=_anchor_after_ir(spec),
        equivalence_contract="anchor_disambiguation_preserves_answer_blind_anchor_contract",
        quality_contract=spec.quality_assumption,
        conflict_key=f"anchor_disambiguation:{spec.anchor_query}",
    )


def _payload_candidate(
    graph: OptimizerGraph,
    spec: PayloadSummarySpec,
) -> RewriteCandidate:
    _require_node(graph, spec.node_id)
    _require_fields(spec.retained_fields, spec.required_fields)
    summary_node_id = f"optimizer_payload_summary:{spec.summary_id}"
    summary_edge_id = f"optimizer_payload_summary_edge:{spec.summary_id}"
    bounds = _bounds(
        before=spec.original_payload_cost,
        after=spec.summarized_payload_cost,
        ignored_clutter_cost=spec.ignored_clutter_cost,
        quality_risk_upper_bound=spec.quality_risk_upper_bound,
        adoption_assumption=AdoptionAssumption.DETERMINISTIC_TOOL_SURFACE,
        quality_assumption=spec.quality_assumption,
    )
    return RewriteCandidate(
        candidate_id=spec.candidate_id,
        template_id="summarize_payload",
        params={
            "node_id": spec.node_id,
            "summary_id": spec.summary_id,
            "summary_kind": spec.summary_kind,
        },
        apply=lambda g: g.add_node(
            OptimizerNode(
                summary_node_id,
                subtype="optimizer_payload_summary",
                attrs={
                    "summary_id": spec.summary_id,
                    "summary_kind": spec.summary_kind,
                    "retained_fields": spec.retained_fields,
                },
            )
        ).add_edge(
            OptimizerEdge(
                summary_edge_id,
                src=spec.node_id,
                dst=summary_node_id,
                edge_type="has_optimizer_payload_summary",
            )
        ),
        estimated_cost_delta=-bounds.benefit_upper_bound,
        estimated_quality_delta=-bounds.quality_risk_upper_bound,
        quality_regression_upper_bound=bounds.quality_risk_upper_bound,
        evidence_refs=spec.evidence_refs,
        benefit_bounds=bounds,
        before_ir=_payload_before_ir(spec),
        install_ir=_payload_install_ir(
            spec,
            node_id=summary_node_id,
            edge_id=summary_edge_id,
        ),
        after_ir=_payload_after_ir(spec),
        equivalence_contract="retained_fields_cover_required_fields",
        quality_contract=spec.quality_assumption,
    )


def _overlay_before_ir(spec: OverlayEdgeSpec) -> IRProgram:
    if spec.agentic_decision:
        return _agentic_overlay_before_ir(spec)
    return IRProgram(
        program_id=f"{spec.candidate_id}:before",
        ops=(
            IRBinding(
                output=IRSymbol("src", NODE),
                op=IROp(IROpKind.READ, args={"node_id": spec.src}),
            ),
            IRBinding(
                output=IRSymbol("witness_edges", EDGE_SET),
                op=IROp(
                    IROpKind.WALK,
                    args={
                        "edge_type": spec.edge_type,
                        "direction": "out",
                        "estimated_cost": spec.baseline_path_cost,
                    },
                ),
                inputs=("src",),
            ),
        ),
        outputs=("witness_edges",),
        attrs={"quality_contracts": ("typed_path_witness_preserves_target_identity",)},
    )


def _overlay_install_ir(spec: OverlayEdgeSpec) -> IRProgram:
    return IRProgram(
        program_id=f"{spec.candidate_id}:install",
        ops=(
            IRBinding(
                output=IRSymbol("delta", GRAPH_DELTA),
                op=IROp(
                    IROpKind.ATOMIC_EDIT,
                    args={
                        "action": "add_edge",
                        "edge_id": spec.edge_id,
                        "src": spec.src,
                        "dst": spec.dst,
                        "edge_type": spec.edge_type,
                        "attrs": {
                            "optimizer_tier": RewriteTier.TIER_1.value,
                            "evidence_refs": spec.evidence_refs,
                        },
                    },
                ),
            ),
        ),
        outputs=("delta",),
        effects=(IREffect.GRAPH_DELTA,),
    )


def _overlay_after_ir(spec: OverlayEdgeSpec) -> IRProgram:
    if spec.agentic_decision:
        return _agentic_overlay_after_ir(spec)
    return IRProgram(
        program_id=f"{spec.candidate_id}:after",
        ops=(
            IRBinding(
                output=IRSymbol("src", NODE),
                op=IROp(IROpKind.READ, args={"node_id": spec.src}),
            ),
            IRBinding(
                output=IRSymbol("shortcut_edges", EDGE_SET),
                op=IROp(
                    IROpKind.WALK,
                    args={
                        "edge_type": spec.edge_type,
                        "direction": "out",
                        "estimated_cost": spec.shortcut_path_cost,
                    },
                ),
                inputs=("src",),
            ),
        ),
        outputs=("shortcut_edges",),
    )


def _agentic_overlay_before_ir(spec: OverlayEdgeSpec) -> IRProgram:
    return IRProgram(
        program_id=f"{spec.candidate_id}:before",
        ops=(
            IRBinding(
                output=IRSymbol("query", RECORD),
                op=IROp(IROpKind.READ, args={"target": "query", "estimated_cost": 0.0}),
            ),
            IRBinding(
                output=IRSymbol("anchors", NODE_SET),
                op=IROp(
                    IROpKind.APPLY,
                    args={
                        "function": "optimizer.resolve_reusable_anchors",
                        "estimated_cost": spec.anchor_resolution_cost,
                    },
                ),
                inputs=("query",),
            ),
            IRBinding(
                output=IRSymbol("base_candidates", NODE_SET),
                op=IROp(
                    IROpKind.WALK,
                    args={
                        "route": "base_traversal",
                        "estimated_cost": spec.baseline_path_cost,
                    },
                ),
                inputs=("anchors",),
            ),
        ),
        outputs=("base_candidates",),
        attrs={
            "quality_contracts": (
                "answer_blind_anchor_resolution",
                "fallback_preserves_base_traversal_semantics",
            ),
            "shortcut_mode": "base_only",
        },
    )


def _agentic_overlay_after_ir(spec: OverlayEdgeSpec) -> IRProgram:
    accept_cost = max(0.0, spec.accept_probability) * spec.accept_path_cost
    fallback_cost = max(0.0, 1.0 - spec.accept_probability) * spec.fallback_path_cost
    shortcut_read_cost = spec.shortcut_path_cost - (
        spec.anchor_resolution_cost
        + spec.evidence_packet_cost
        + spec.policy_decision_cost
        + accept_cost
        + fallback_cost
        + spec.branch_overlay_cost
    )
    shortcut_read_cost = max(0.0, shortcut_read_cost)
    return IRProgram(
        program_id=f"{spec.candidate_id}:after",
        ops=(
            IRBinding(
                output=IRSymbol("query", RECORD),
                op=IROp(IROpKind.READ, args={"target": "query", "estimated_cost": 0.0}),
            ),
            IRBinding(
                output=IRSymbol("anchors", NODE_SET),
                op=IROp(
                    IROpKind.APPLY,
                    args={
                        "function": "optimizer.resolve_reusable_anchors",
                        "estimated_cost": spec.anchor_resolution_cost,
                    },
                ),
                inputs=("query",),
            ),
            IRBinding(
                output=IRSymbol("shortcut_edges", EDGE_SET),
                op=IROp(
                    IROpKind.WALK,
                    args={
                        "edge_type": spec.edge_type,
                        "direction": "out",
                        "estimated_cost": shortcut_read_cost + spec.branch_overlay_cost,
                    },
                ),
                inputs=("anchors",),
            ),
            IRBinding(
                output=IRSymbol("shortcut_cards", RECORD),
                op=IROp(
                    IROpKind.APPLY,
                    args={
                        "function": "optimizer.build_shortcut_evidence_cards",
                        "estimated_cost": spec.evidence_packet_cost,
                    },
                ),
                inputs=("shortcut_edges",),
            ),
            IRBinding(
                output=IRSymbol("decision", BOOL),
                op=IROp(
                    IROpKind.APPLY,
                    args={
                        "function": "optimizer.shortcut_decision_model",
                        "estimated_cost": spec.policy_decision_cost,
                        "accept_probability": spec.accept_probability,
                    },
                ),
                inputs=("shortcut_cards",),
            ),
            IRBinding(
                output=IRSymbol("accepted_candidates", NODE_SET),
                op=IROp(
                    IROpKind.APPLY,
                    args={
                        "function": "optimizer.shortcut_accept_path",
                        "estimated_cost": accept_cost,
                    },
                ),
                inputs=("shortcut_cards",),
            ),
            IRBinding(
                output=IRSymbol("fallback_candidates", NODE_SET),
                op=IROp(
                    IROpKind.APPLY,
                    args={
                        "function": "optimizer.shortcut_fallback_path",
                        "estimated_cost": fallback_cost,
                    },
                ),
                inputs=("query",),
            ),
            IRBinding(
                output=IRSymbol("answer_candidates", NODE_SET),
                op=IROp(
                    IROpKind.APPLY,
                    args={
                        "function": "optimizer.shortcut_decision_merge",
                        "estimated_cost": 0.0,
                    },
                ),
                inputs=("decision", "accepted_candidates", "fallback_candidates"),
            ),
        ),
        outputs=("answer_candidates",),
        attrs={
            "quality_contracts": (
                "answer_blind_anchor_resolution",
                "shortcut_cards_are_answer_blind",
                "recorded_or_calibrated_shortcut_decision",
                "fallback_preserves_base_traversal_semantics",
            ),
            "shortcut_mode": "calibrated_agent_accept",
            "decision_boundary": "accept_or_fallback",
        },
    )


def _aggregate_before_ir(spec: MaterializedAggregateSpec) -> IRProgram:
    return IRProgram(
        program_id=f"{spec.candidate_id}:before",
        ops=(
            IRBinding(
                output=IRSymbol("anchor", NODE),
                op=IROp(IROpKind.READ, args={"node_id": spec.anchor_node_id}),
            ),
            IRBinding(
                output=IRSymbol("neighbors", EDGE_SET),
                op=IROp(
                    IROpKind.WALK,
                    args={
                        "direction": "out",
                        "estimated_cost": spec.baseline_expand_cost,
                    },
                ),
                inputs=("anchor",),
            ),
            IRBinding(
                output=IRSymbol("aggregate", SCALAR),
                op=IROp(
                    IROpKind.APPLY,
                    args={
                        "function": spec.aggregate_kind,
                        "retained_fields": spec.retained_fields,
                        "estimated_cost": 0.0,
                    },
                ),
                inputs=("neighbors",),
            ),
        ),
        outputs=("aggregate",),
        attrs={"quality_contracts": ("retained_fields_cover_required_fields",)},
    )


def _aggregate_install_ir(
    spec: MaterializedAggregateSpec,
    *,
    node_id: str,
    edge_id: str,
) -> IRProgram:
    return IRProgram(
        program_id=f"{spec.candidate_id}:install",
        ops=(
            IRBinding(
                output=IRSymbol("node_delta", GRAPH_DELTA),
                op=IROp(
                    IROpKind.ATOMIC_EDIT,
                    args={
                        "action": "add_node",
                        "node_id": node_id,
                        "subtype": "optimizer_materialized_aggregate",
                        "attrs": {
                            "aggregate_id": spec.aggregate_id,
                            "aggregate_kind": spec.aggregate_kind,
                            "retained_fields": spec.retained_fields,
                        },
                    },
                ),
            ),
            IRBinding(
                output=IRSymbol("edge_delta", GRAPH_DELTA),
                op=IROp(
                    IROpKind.ATOMIC_EDIT,
                    args={
                        "action": "add_edge",
                        "edge_id": edge_id,
                        "src": spec.anchor_node_id,
                        "dst": node_id,
                        "edge_type": "has_optimizer_aggregate",
                    },
                ),
            ),
        ),
        outputs=("node_delta", "edge_delta"),
        effects=(IREffect.GRAPH_DELTA,),
    )


def _aggregate_after_ir(spec: MaterializedAggregateSpec) -> IRProgram:
    return IRProgram(
        program_id=f"{spec.candidate_id}:after",
        ops=(
            IRBinding(
                output=IRSymbol("anchor", NODE),
                op=IROp(IROpKind.READ, args={"node_id": spec.anchor_node_id}),
            ),
            IRBinding(
                output=IRSymbol("aggregate_edges", EDGE_SET),
                op=IROp(
                    IROpKind.WALK,
                    args={
                        "edge_type": "has_optimizer_aggregate",
                        "estimated_cost": spec.aggregate_cost,
                    },
                ),
                inputs=("anchor",),
            ),
        ),
        outputs=("aggregate_edges",),
    )


def _route_materialization_before_ir(spec: RouteMaterializationSpec) -> IRProgram:
    return IRProgram(
        program_id=f"{spec.candidate_id}:before",
        ops=(
            IRBinding(
                output=IRSymbol("anchor", NODE),
                op=IROp(IROpKind.READ, args={"node_id": spec.anchor_node_id}),
            ),
            IRBinding(
                output=IRSymbol("route_candidates", NODE_SET),
                op=IROp(
                    IROpKind.WALK,
                    args={
                        "route_signature": spec.route_signature,
                        "set_checksum": spec.set_checksum,
                        "estimated_cost": spec.baseline_route_cost,
                    },
                ),
                inputs=("anchor",),
            ),
        ),
        outputs=("route_candidates",),
        attrs={
            "quality_contracts": (
                "route_materialization_preserves_candidate_set_checksum",
            )
        },
    )


def _route_materialization_install_ir(
    spec: RouteMaterializationSpec,
    *,
    node_id: str,
    edge_id: str,
) -> IRProgram:
    return IRProgram(
        program_id=f"{spec.candidate_id}:install",
        ops=(
            IRBinding(
                output=IRSymbol("node_delta", GRAPH_DELTA),
                op=IROp(
                    IROpKind.ATOMIC_EDIT,
                    args={
                        "action": "add_node",
                        "node_id": node_id,
                        "subtype": "optimizer_route_materialization",
                        "estimated_cost": spec.install_write_cost,
                        "attrs": {
                            "materialization_id": spec.materialization_id,
                            "route_signature": spec.route_signature,
                            "output_count": spec.output_count,
                            "set_checksum": spec.set_checksum,
                            "storage_overhead_bytes": spec.storage_overhead_bytes,
                        },
                    },
                ),
            ),
            IRBinding(
                output=IRSymbol("edge_delta", GRAPH_DELTA),
                op=IROp(
                    IROpKind.ATOMIC_EDIT,
                    args={
                        "action": "add_edge",
                        "edge_id": edge_id,
                        "src": spec.anchor_node_id,
                        "dst": node_id,
                        "edge_type": "has_optimizer_route_materialization",
                    },
                ),
            ),
        ),
        outputs=("node_delta", "edge_delta"),
        effects=(IREffect.GRAPH_DELTA,),
    )


def _route_materialization_after_ir(spec: RouteMaterializationSpec) -> IRProgram:
    return IRProgram(
        program_id=f"{spec.candidate_id}:after",
        ops=(
            IRBinding(
                output=IRSymbol("anchor", NODE),
                op=IROp(IROpKind.READ, args={"node_id": spec.anchor_node_id}),
            ),
            IRBinding(
                output=IRSymbol("materialized_route", EDGE_SET),
                op=IROp(
                    IROpKind.WALK,
                    args={
                        "edge_type": "has_optimizer_route_materialization",
                        "set_checksum": spec.set_checksum,
                        "estimated_cost": spec.materialized_read_cost,
                    },
                ),
                inputs=("anchor",),
            ),
        ),
        outputs=("materialized_route",),
        attrs={
            "quality_contracts": (
                "route_materialization_preserves_candidate_set_checksum",
            )
        },
    )


def _intersection_before_ir(spec: MaterializedIntersectionSpec) -> IRProgram:
    operand_bindings: list[IRBinding] = [
        IRBinding(
            output=IRSymbol("anchor", NODE),
            op=IROp(IROpKind.READ, args={"node_id": spec.anchor_node_id}),
        )
    ]
    per_operand_cost = spec.baseline_operand_cost / len(spec.operand_route_signatures)
    for index, signature in enumerate(spec.operand_route_signatures):
        operand_bindings.append(
            IRBinding(
                output=IRSymbol(f"operand_{index}", NODE_SET),
                op=IROp(
                    IROpKind.WALK,
                    args={
                        "route_signature": signature,
                        "set_checksum": spec.operand_set_checksums[index],
                        "estimated_cost": per_operand_cost,
                    },
                ),
                inputs=("anchor",),
            )
        )
    operand_names = tuple(f"operand_{index}" for index in range(len(spec.operand_route_signatures)))
    operand_bindings.append(
        IRBinding(
            output=IRSymbol("intersection", NODE_SET),
            op=IROp(
                IROpKind.APPLY,
                args={
                    "function": "optimizer.intersect_candidate_sets",
                    "set_checksum": spec.set_checksum,
                    "estimated_cost": spec.intersection_compute_cost,
                },
            ),
            inputs=operand_names,
        )
    )
    return IRProgram(
        program_id=f"{spec.candidate_id}:before",
        ops=tuple(operand_bindings),
        outputs=("intersection",),
        attrs={
            "quality_contracts": (
                "materialized_intersection_preserves_operand_set_intersection",
            )
        },
    )


def _intersection_install_ir(
    spec: MaterializedIntersectionSpec,
    *,
    node_id: str,
    edge_id: str,
) -> IRProgram:
    return IRProgram(
        program_id=f"{spec.candidate_id}:install",
        ops=(
            IRBinding(
                output=IRSymbol("node_delta", GRAPH_DELTA),
                op=IROp(
                    IROpKind.ATOMIC_EDIT,
                    args={
                        "action": "add_node",
                        "node_id": node_id,
                        "subtype": "optimizer_materialized_intersection",
                        "estimated_cost": spec.install_write_cost,
                        "attrs": {
                            "intersection_id": spec.intersection_id,
                            "operand_route_signatures": spec.operand_route_signatures,
                            "operand_set_checksums": spec.operand_set_checksums,
                            "output_count": spec.output_count,
                            "set_checksum": spec.set_checksum,
                            "storage_overhead_bytes": spec.storage_overhead_bytes,
                        },
                    },
                ),
            ),
            IRBinding(
                output=IRSymbol("edge_delta", GRAPH_DELTA),
                op=IROp(
                    IROpKind.ATOMIC_EDIT,
                    args={
                        "action": "add_edge",
                        "edge_id": edge_id,
                        "src": spec.anchor_node_id,
                        "dst": node_id,
                        "edge_type": "has_optimizer_intersection",
                    },
                ),
            ),
        ),
        outputs=("node_delta", "edge_delta"),
        effects=(IREffect.GRAPH_DELTA,),
    )


def _intersection_after_ir(spec: MaterializedIntersectionSpec) -> IRProgram:
    return IRProgram(
        program_id=f"{spec.candidate_id}:after",
        ops=(
            IRBinding(
                output=IRSymbol("anchor", NODE),
                op=IROp(IROpKind.READ, args={"node_id": spec.anchor_node_id}),
            ),
            IRBinding(
                output=IRSymbol("materialized_intersection", EDGE_SET),
                op=IROp(
                    IROpKind.WALK,
                    args={
                        "edge_type": "has_optimizer_intersection",
                        "set_checksum": spec.set_checksum,
                        "estimated_cost": spec.materialized_read_cost,
                    },
                ),
                inputs=("anchor",),
            ),
        ),
        outputs=("materialized_intersection",),
        attrs={
            "quality_contracts": (
                "materialized_intersection_preserves_operand_set_intersection",
            )
        },
    )


def _anchor_before_ir(spec: AnchorDisambiguationSpec) -> IRProgram:
    return IRProgram(
        program_id=f"{spec.candidate_id}:before",
        ops=(
            IRBinding(
                output=IRSymbol("query", RECORD),
                op=IROp(
                    IROpKind.READ,
                    args={
                        "target": "anchor_query",
                        "anchor_query": spec.anchor_query,
                    },
                ),
            ),
            IRBinding(
                output=IRSymbol("anchors", NODE_SET),
                op=IROp(
                    IROpKind.APPLY,
                    args={
                        "function": "optimizer.resolve_reusable_anchors",
                        "ambiguity_count": spec.ambiguity_count,
                        "estimated_cost": spec.baseline_resolution_cost,
                    },
                ),
                inputs=("query",),
            ),
        ),
        outputs=("anchors",),
        attrs={"quality_contracts": ("answer_blind_anchor_resolution",)},
    )


def _anchor_install_ir(
    spec: AnchorDisambiguationSpec,
    *,
    node_id: str,
    edge_id: str,
) -> IRProgram:
    return IRProgram(
        program_id=f"{spec.candidate_id}:install",
        ops=(
            IRBinding(
                output=IRSymbol("node_delta", GRAPH_DELTA),
                op=IROp(
                    IROpKind.ATOMIC_EDIT,
                    args={
                        "action": "add_node",
                        "node_id": node_id,
                        "subtype": "optimizer_anchor_disambiguation",
                        "estimated_cost": spec.install_write_cost,
                        "attrs": {
                            "disambiguation_id": spec.disambiguation_id,
                            "anchor_query": spec.anchor_query,
                            "allowed_anchor_subtypes": spec.allowed_anchor_subtypes,
                            "ambiguity_count": spec.ambiguity_count,
                            "precision": spec.precision,
                            "coverage": spec.coverage,
                            "storage_overhead_bytes": spec.storage_overhead_bytes,
                        },
                    },
                ),
            ),
            IRBinding(
                output=IRSymbol("edge_delta", GRAPH_DELTA),
                op=IROp(
                    IROpKind.ATOMIC_EDIT,
                    args={
                        "action": "add_edge",
                        "edge_id": edge_id,
                        "src": spec.anchor_node_id,
                        "dst": node_id,
                        "edge_type": "has_optimizer_anchor_disambiguation",
                    },
                ),
            ),
        ),
        outputs=("node_delta", "edge_delta"),
        effects=(IREffect.GRAPH_DELTA,),
    )


def _anchor_after_ir(spec: AnchorDisambiguationSpec) -> IRProgram:
    return IRProgram(
        program_id=f"{spec.candidate_id}:after",
        ops=(
            IRBinding(
                output=IRSymbol("anchor", NODE),
                op=IROp(IROpKind.READ, args={"node_id": spec.anchor_node_id}),
            ),
            IRBinding(
                output=IRSymbol("disambiguation", EDGE_SET),
                op=IROp(
                    IROpKind.WALK,
                    args={
                        "edge_type": "has_optimizer_anchor_disambiguation",
                        "estimated_cost": spec.disambiguated_resolution_cost,
                    },
                ),
                inputs=("anchor",),
            ),
            IRBinding(
                output=IRSymbol("anchors", NODE_SET),
                op=IROp(
                    IROpKind.APPLY,
                    args={
                        "function": "optimizer.apply_anchor_disambiguation",
                        "allowed_anchor_subtypes": spec.allowed_anchor_subtypes,
                        "precision": spec.precision,
                        "coverage": spec.coverage,
                        "estimated_cost": spec.inspect_cost,
                    },
                ),
                inputs=("disambiguation",),
            ),
        ),
        outputs=("anchors",),
        attrs={
            "quality_contracts": (
                "anchor_disambiguation_preserves_answer_blind_anchor_contract",
            )
        },
    )


def _route_before_ir(spec: RoutePolicyReplacementSpec) -> IRProgram:
    return IRProgram(
        program_id=f"{spec.candidate_id}:before",
        ops=(
            IRBinding(
                output=IRSymbol("route_result", SCALAR),
                op=IROp(
                    IROpKind.APPLY,
                    args={
                        "function": "route.execute",
                        "route_id": spec.route_id,
                        "estimated_cost": spec.baseline_route_cost,
                    },
                ),
            ),
        ),
        outputs=("route_result",),
    )


def _route_install_ir(spec: RoutePolicyReplacementSpec) -> IRProgram:
    return IRProgram(
        program_id=f"{spec.candidate_id}:install",
        ops=(
            IRBinding(
                output=IRSymbol("route_delta", GRAPH_DELTA),
                op=IROp(
                    IROpKind.EDIT,
                    args={
                        "kind": "route_policy_replacement",
                        "route_id": spec.route_id,
                        "replacement_operator": spec.replacement_operator,
                    },
                ),
            ),
        ),
        outputs=("route_delta",),
        effects=(IREffect.GRAPH_DELTA,),
    )


def _route_after_ir(spec: RoutePolicyReplacementSpec) -> IRProgram:
    return IRProgram(
        program_id=f"{spec.candidate_id}:after",
        ops=(
            IRBinding(
                output=IRSymbol("replacement_result", SCALAR),
                op=IROp(
                    IROpKind.APPLY,
                    args={
                        "function": spec.replacement_operator,
                        "route_id": spec.route_id,
                        "estimated_cost": spec.replacement_route_cost,
                    },
                ),
            ),
        ),
        outputs=("replacement_result",),
    )


def _payload_before_ir(spec: PayloadSummarySpec) -> IRProgram:
    return IRProgram(
        program_id=f"{spec.candidate_id}:before",
        ops=(
            IRBinding(
                output=IRSymbol("node", NODE),
                op=IROp(
                    IROpKind.READ,
                    args={
                        "node_id": spec.node_id,
                        "estimated_cost": spec.original_payload_cost,
                    },
                ),
            ),
        ),
        outputs=("node",),
        attrs={"quality_contracts": ("retained_fields_cover_required_fields",)},
    )


def _payload_install_ir(
    spec: PayloadSummarySpec,
    *,
    node_id: str,
    edge_id: str,
) -> IRProgram:
    return IRProgram(
        program_id=f"{spec.candidate_id}:install",
        ops=(
            IRBinding(
                output=IRSymbol("summary_node_delta", GRAPH_DELTA),
                op=IROp(
                    IROpKind.ATOMIC_EDIT,
                    args={
                        "action": "add_node",
                        "node_id": node_id,
                        "subtype": "optimizer_payload_summary",
                        "attrs": {
                            "summary_id": spec.summary_id,
                            "summary_kind": spec.summary_kind,
                            "retained_fields": spec.retained_fields,
                        },
                    },
                ),
            ),
            IRBinding(
                output=IRSymbol("summary_edge_delta", GRAPH_DELTA),
                op=IROp(
                    IROpKind.ATOMIC_EDIT,
                    args={
                        "action": "add_edge",
                        "edge_id": edge_id,
                        "src": spec.node_id,
                        "dst": node_id,
                        "edge_type": "has_optimizer_payload_summary",
                        "summary_id": spec.summary_id,
                    },
                ),
            ),
        ),
        outputs=("summary_node_delta", "summary_edge_delta"),
        effects=(IREffect.GRAPH_DELTA,),
    )


def _payload_after_ir(spec: PayloadSummarySpec) -> IRProgram:
    return IRProgram(
        program_id=f"{spec.candidate_id}:after",
        ops=(
            IRBinding(
                output=IRSymbol("summary", SCALAR),
                op=IROp(
                    IROpKind.APPLY,
                    args={
                        "function": "payload.read_summary",
                        "summary_id": spec.summary_id,
                        "estimated_cost": spec.summarized_payload_cost,
                    },
                ),
            ),
        ),
        outputs=("summary",),
    )


def _bounds(
    *,
    before: float,
    after: float,
    ignored_clutter_cost: float,
    quality_risk_upper_bound: float,
    adoption_assumption: AdoptionAssumption,
    quality_assumption: str,
) -> BenefitBounds:
    if before < 0 or after < 0 or ignored_clutter_cost < 0:
        raise RewriteValidationError("cost inputs must be non-negative")
    return BenefitBounds(
        benefit_upper_bound=max(0.0, before - after),
        benefit_lower_bound=-ignored_clutter_cost,
        quality_risk_upper_bound=quality_risk_upper_bound,
        adoption_assumption=adoption_assumption,
        quality_assumption=quality_assumption,
    )


def _require_node(graph: OptimizerGraph, node_id: str) -> None:
    if node_id not in graph.nodes:
        raise RewriteValidationError(f"candidate references missing node {node_id!r}")


def _require_edge_route(
    graph: OptimizerGraph,
    src: str,
    edge_type: str,
    dst: str,
) -> None:
    allowed = graph.attrs.get("allowed_edge_routes")
    if allowed is None:
        raise RewriteValidationError("graph must declare allowed_edge_routes")
    route = (graph.nodes[src].subtype, edge_type, graph.nodes[dst].subtype)
    if route not in {tuple(item) for item in allowed}:
        raise RewriteValidationError(f"invalid target edge route: {route!r}")


def _require_route(graph: OptimizerGraph, route_id: str) -> None:
    if route_id not in set(graph.attrs.get("available_routes", ())):
        raise RewriteValidationError(f"candidate references unknown route {route_id!r}")


def _require_operator(graph: OptimizerGraph, operator: str) -> None:
    if operator not in set(graph.attrs.get("available_operators", ())):
        raise RewriteValidationError(
            f"candidate references unavailable operator {operator!r}"
        )


def _require_fields(retained: tuple[str, ...], required: tuple[str, ...]) -> None:
    missing = set(required) - set(retained)
    if missing:
        raise RewriteValidationError(
            "candidate omits required quality fields: " + ", ".join(sorted(missing))
        )


def _require_non_empty_tuple(value: tuple[str, ...], name: str) -> None:
    if not value:
        raise RewriteValidationError(f"candidate {name} must be non-empty")


def _require_positive_int(value: int, name: str) -> None:
    if int(value) <= 0:
        raise RewriteValidationError(f"candidate {name} must be positive")
