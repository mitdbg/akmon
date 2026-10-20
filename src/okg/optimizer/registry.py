"""Built-in IR function registry profiles for optimizer programs."""

from __future__ import annotations

from collections.abc import Iterable

from .ir import (
    IRDeterminism,
    IREffect,
    IRFunctionRegistry,
    IRFunctionSpec,
    IRType,
    IRValueKind,
)


BUILTIN_IR_REGISTRY_ID = "okg.optimizer.builtin"
BUILTIN_IR_REGISTRY_VERSION = "v1"

NODE = IRType(IRValueKind.NODE_REF.value)
EDGE = IRType(IRValueKind.EDGE_REF.value)
EDGE_SET = IRType(IRValueKind.COLLECTION.value, {"element": IRValueKind.EDGE_REF.value})
NODE_SET = IRType(IRValueKind.COLLECTION.value, {"element": IRValueKind.NODE_REF.value})
RECORD = IRType(IRValueKind.RECORD.value)
PROPOSAL = IRType(IRValueKind.RECORD.value, {"subtype": "proposal"})
SCALAR = IRType(IRValueKind.SCALAR.value)
BOOL = IRType("bool")


def builtin_ir_function_specs() -> tuple[IRFunctionSpec, ...]:
    """Return the built-in function contracts used by shipped optimizer IR."""

    return (
        _read_only(
            "akmon.search",
            {"index": RECORD},
            NODE_SET,
            cardinality="search_k_or_selectivity_v1",
            cost="akmon_search_cost_v1",
            quality="search_results_preserve_query_relevance",
        ),
        _read_only(
            "akmon.filter",
            {"items": NODE_SET},
            NODE_SET,
            cardinality="filter_selectivity_v1",
            cost="akmon_filter_cost_v1",
            quality="filter_predicate_preserves_required_fields",
        ),
        _read_only(
            "akmon.map",
            {"item": NODE},
            RECORD,
            cardinality="one_output_per_input_v1",
            cost="akmon_map_cost_v1",
            quality="map_preserves_declared_projection",
        ),
        _read_only(
            "akmon.aggregate",
            {"items": NODE_SET},
            SCALAR,
            cardinality="single_aggregate_v1",
            cost="akmon_aggregate_cost_v1",
            quality="aggregate_retains_declared_fields",
        ),
        _read_only(
            "akmon.admission_predicate",
            {"proposal": PROPOSAL},
            BOOL,
            cardinality="single_boolean_v1",
            cost="admission_predicate_cost_v1",
            quality="admission_predicate_enforces_policy",
        ),
        _read_only(
            "route.execute",
            {},
            SCALAR,
            cardinality="single_route_result_v1",
            cost="route_execute_cost_v1",
            quality="route_result_preserves_declared_output",
        ),
        _read_only(
            "payload.read_summary",
            {},
            SCALAR,
            cardinality="single_summary_v1",
            cost="payload_summary_read_cost_v1",
            quality="summary_preserves_required_fields",
        ),
        _read_only(
            "optimizer.intersect_candidate_sets",
            {"left": NODE_SET, "right": NODE_SET},
            NODE_SET,
            cardinality="bounded_candidate_set_intersection_v1",
            cost="candidate_set_intersection_cost_v1",
            quality="materialized_intersection_preserves_operand_set_intersection",
        ),
        _read_only(
            "optimizer.apply_anchor_disambiguation",
            {"disambiguation": EDGE_SET},
            NODE_SET,
            cardinality="bounded_disambiguated_anchor_set_v1",
            cost="anchor_disambiguation_apply_cost_v1",
            quality="anchor_disambiguation_preserves_answer_blind_anchor_contract",
        ),
        _read_only(
            "optimizer.read_route_materialization",
            {"anchor": NODE},
            NODE_SET,
            cardinality="materialized_route_output_count_v1",
            cost="route_materialization_read_cost_v1",
            quality="route_materialization_preserves_candidate_set_checksum",
        ),
        _read_only(
            "neighbor_status_counts",
            {"neighbors": EDGE_SET},
            SCALAR,
            cardinality="single_aggregate_v1",
            cost="neighbor_status_counts_cost_v1",
            quality="aggregate_retains_heldout_answer_fields",
        ),
        _read_only(
            "status_counts",
            {"neighbors": EDGE_SET},
            SCALAR,
            cardinality="single_aggregate_v1",
            cost="status_counts_cost_v1",
            quality="aggregate_retains_heldout_answer_fields",
        ),
        _read_only(
            "aggregate",
            {},
            SCALAR,
            cardinality="single_route_result_v1",
            cost="aggregate_route_replacement_cost_v1",
            quality="replacement_operator_preserves_route_answer",
        ),
        _read_only(
            "expand",
            {},
            EDGE_SET,
            cardinality="observed_expand_fanout_v1",
            cost="expand_route_replacement_cost_v1",
            quality="replacement_operator_preserves_route_answer",
        ),
        _read_only(
            "optimizer.retained_fields_cover_required_fields",
            {"record": RECORD},
            BOOL,
            cardinality="single_boolean_v1",
            cost="retained_field_check_cost_v1",
            quality="retained_fields_cover_required_fields",
        ),
        _read_only(
            "optimizer.typed_path_witness_preserves_target_identity",
            {"edge": EDGE},
            BOOL,
            cardinality="single_boolean_v1",
            cost="typed_path_witness_check_cost_v1",
            quality="typed_path_witness_preserves_target_identity",
        ),
        _read_only(
            "optimizer.exact_equivalence",
            {"before": RECORD, "after": RECORD},
            BOOL,
            cardinality="single_boolean_v1",
            cost="exact_equivalence_check_cost_v1",
            quality="exact_equivalence",
        ),
        _read_only(
            "optimizer.citation_coverage",
            {"record": RECORD},
            BOOL,
            cardinality="single_boolean_v1",
            cost="citation_coverage_check_cost_v1",
            quality="citation_coverage",
        ),
        _read_only(
            "optimizer.freshness_dependency_coverage",
            {"record": RECORD},
            BOOL,
            cardinality="single_boolean_v1",
            cost="freshness_dependency_check_cost_v1",
            quality="freshness_dependency_coverage",
        ),
        _read_only(
            "extract_required_fields",
            {"record": RECORD},
            RECORD,
            cardinality="single_record_v1",
            cost="extract_required_fields_cost_v1",
            quality="required_fields_extracted_from_source_record",
        ),
        _read_only(
            "optimizer.resolve_reusable_anchors",
            {"query": RECORD},
            NODE_SET,
            cardinality="bounded_anchor_resolution_v1",
            cost="shortcut_anchor_resolution_cost_v1",
            quality="answer_blind_anchor_resolution",
        ),
        _read_only(
            "optimizer.build_shortcut_evidence_cards",
            {"shortcut_edges": EDGE_SET},
            RECORD,
            cardinality="one_card_per_shortcut_edge_v1",
            cost="shortcut_evidence_packet_cost_v1",
            quality="shortcut_cards_are_answer_blind",
        ),
        _read_only(
            "optimizer.shortcut_decision_model",
            {"shortcut_cards": RECORD},
            BOOL,
            cardinality="single_decision_v1",
            cost="shortcut_policy_decision_cost_v1",
            quality="recorded_or_calibrated_shortcut_decision",
        ),
        _read_only(
            "optimizer.shortcut_accept_path",
            {"shortcut_cards": RECORD},
            NODE_SET,
            cardinality="accepted_shortcut_candidates_v1",
            cost="shortcut_accept_path_cost_v1",
            quality="accepted_shortcut_candidates_preserve_policy_decision",
        ),
        _read_only(
            "optimizer.shortcut_fallback_path",
            {"query": RECORD},
            NODE_SET,
            cardinality="fallback_base_candidates_v1",
            cost="shortcut_fallback_path_cost_v1",
            quality="fallback_preserves_base_traversal_semantics",
        ),
        _read_only(
            "optimizer.shortcut_decision_merge",
            {"decision": BOOL, "accepted": NODE_SET, "fallback": NODE_SET},
            NODE_SET,
            cardinality="decision_selected_candidates_v1",
            cost="shortcut_decision_merge_cost_v1",
            quality="shortcut_decision_selects_accepted_or_fallback",
        ),
    )


def builtin_ir_function_registry(
    extra_specs: Iterable[IRFunctionSpec] = (),
) -> IRFunctionRegistry:
    """Build the standard registry profile, optionally extended by a deployment."""

    specs = (*builtin_ir_function_specs(), *tuple(extra_specs))
    return IRFunctionRegistry.from_specs(
        specs,
        registry_id=BUILTIN_IR_REGISTRY_ID,
        version=BUILTIN_IR_REGISTRY_VERSION,
    )


def _read_only(
    function_id: str,
    input_schema: dict[str, IRType],
    output_schema: IRType,
    *,
    cardinality: str,
    cost: str,
    quality: str,
) -> IRFunctionSpec:
    return IRFunctionSpec(
        function_id=function_id,
        input_schema=input_schema,
        output_schema=output_schema,
        effect=IREffect.READ_ONLY,
        determinism=IRDeterminism.DETERMINISTIC,
        cardinality_estimator=cardinality,
        cost_estimator=cost,
        quality_contract=quality,
    )
