"""General optimizer instantiation helpers for agent-visible shortcuts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from .graph import OptimizerGraph
from .models import IRCostModel, IRQualityModel
from .registry import builtin_ir_function_registry
from .rewrite import RewriteTemplate
from .scoring import IRCandidateScorer
from .tier1 import OverlayEdgeSpec, add_overlay_edge_template


OPTIMIZER_SHORTCUT_EDGE_TYPE = "optimizer_shortcut"


@dataclass(frozen=True)
class AgenticShortcutCostComponents:
    baseline_path_cost: float
    anchor_resolution_cost: float = 0.0
    shortcut_read_cost: float = 0.0
    evidence_packet_cost: float = 0.0
    policy_decision_cost: float = 0.0
    accept_path_cost: float = 0.0
    fallback_path_cost: float = 0.0
    branch_overlay_cost: float = 0.0
    accept_probability: float = 0.0
    ignored_clutter_cost: float = 0.0
    expected_after_cost_override: float | None = None

    def __post_init__(self) -> None:
        for key, value in self.to_components().items():
            if value is not None and value < 0:
                raise ValueError(f"{key} must be non-negative")
        if not 0.0 <= self.accept_probability <= 1.0:
            raise ValueError("accept_probability must be in [0, 1]")

    @property
    def expected_after_cost(self) -> float:
        if self.expected_after_cost_override is not None:
            return self.expected_after_cost_override
        return (
            self.anchor_resolution_cost
            + self.shortcut_read_cost
            + self.evidence_packet_cost
            + self.policy_decision_cost
            + self.branch_overlay_cost
            + self.accept_probability * self.accept_path_cost
            + (1.0 - self.accept_probability) * self.fallback_path_cost
        )

    def to_components(self) -> dict[str, float]:
        return {
            "baseline_path_cost": self.baseline_path_cost,
            "anchor_resolution_cost": self.anchor_resolution_cost,
            "shortcut_read_cost": self.shortcut_read_cost,
            "evidence_packet_cost": self.evidence_packet_cost,
            "policy_decision_cost": self.policy_decision_cost,
            "accept_path_cost": self.accept_path_cost,
            "fallback_path_cost": self.fallback_path_cost,
            "branch_overlay_cost": self.branch_overlay_cost,
            "accept_probability": self.accept_probability,
            "ignored_clutter_cost": self.ignored_clutter_cost,
            "expected_after_cost_override": self.expected_after_cost_override,
            "expected_after_cost": self.expected_after_cost,
        }


@dataclass(frozen=True)
class AgenticShortcutEvidence:
    candidate_id: str
    edge_id: str
    src: str
    dst: str
    src_subtype: str
    dst_subtype: str
    cost: AgenticShortcutCostComponents
    quality_risk_upper_bound: float
    evidence_refs: tuple[str, ...]
    edge_type: str = OPTIMIZER_SHORTCUT_EDGE_TYPE
    quality_assumption: str = "recorded or calibrated agent shortcut decision"
    support_features: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    conflict_key: str | None = None

    def __post_init__(self) -> None:
        for field_name in ("candidate_id", "edge_id", "src", "dst", "src_subtype", "dst_subtype"):
            if not getattr(self, field_name):
                raise ValueError(f"{field_name} must be non-empty")
        if not 0.0 <= self.quality_risk_upper_bound <= 1.0:
            raise ValueError("quality_risk_upper_bound must be in [0, 1]")
        if not self.evidence_refs:
            raise ValueError("evidence_refs must be non-empty")
        object.__setattr__(self, "evidence_refs", tuple(str(ref) for ref in self.evidence_refs))
        object.__setattr__(self, "support_features", MappingProxyType(dict(self.support_features)))
        if self.conflict_key is not None and not str(self.conflict_key).strip():
            raise ValueError("conflict_key cannot be empty")

    def to_overlay_edge_spec(self) -> OverlayEdgeSpec:
        return OverlayEdgeSpec(
            candidate_id=self.candidate_id,
            edge_id=self.edge_id,
            src=self.src,
            dst=self.dst,
            edge_type=self.edge_type,
            baseline_path_cost=self.cost.baseline_path_cost,
            shortcut_path_cost=self.cost.expected_after_cost,
            ignored_clutter_cost=self.cost.ignored_clutter_cost,
            quality_risk_upper_bound=self.quality_risk_upper_bound,
            evidence_refs=self.evidence_refs,
            quality_assumption=self.quality_assumption,
            agentic_decision=True,
            anchor_resolution_cost=self.cost.anchor_resolution_cost,
            evidence_packet_cost=self.cost.evidence_packet_cost,
            policy_decision_cost=self.cost.policy_decision_cost,
            accept_path_cost=self.cost.accept_path_cost,
            fallback_path_cost=self.cost.fallback_path_cost,
            accept_probability=self.cost.accept_probability,
            branch_overlay_cost=self.cost.branch_overlay_cost,
            conflict_key=self.conflict_key,
        )

    def checksum(self) -> str:
        return _stable_checksum(
            {
                "candidate_id": self.candidate_id,
                "edge_id": self.edge_id,
                "src": self.src,
                "dst": self.dst,
                "src_subtype": self.src_subtype,
                "dst_subtype": self.dst_subtype,
                "edge_type": self.edge_type,
                "cost": self.cost.to_components(),
                "quality_risk_upper_bound": self.quality_risk_upper_bound,
                "evidence_refs": self.evidence_refs,
                "quality_assumption": self.quality_assumption,
                "support_features": dict(self.support_features),
                "conflict_key": self.conflict_key,
            }
        )


def agentic_shortcut_overlay_template(
    evidence: Iterable[AgenticShortcutEvidence],
    *,
    max_candidates: int | None = None,
) -> RewriteTemplate:
    """Build the general Tier-One overlay template for agent-visible shortcuts."""

    specs = tuple(item.to_overlay_edge_spec() for item in evidence)
    return add_overlay_edge_template(specs, max_candidates=max_candidates)


def default_agentic_shortcut_ir_scorer() -> IRCandidateScorer:
    """Return the explicit-registry scorer used by shortcut harnesses."""

    registry = builtin_ir_function_registry()
    return IRCandidateScorer(
        cost_model=IRCostModel(
            registry=registry,
            op_costs={
                "READ": 0.0,
                "WALK": 1.0,
                "APPLY": 1.0,
                "ATOMIC_EDIT": 1.0,
            },
            function_costs={
                "optimizer.resolve_reusable_anchors": 0.0,
                "optimizer.build_shortcut_evidence_cards": 0.0,
                "optimizer.shortcut_decision_model": 0.0,
                "optimizer.shortcut_accept_path": 0.0,
                "optimizer.shortcut_fallback_path": 0.0,
                "optimizer.shortcut_decision_merge": 0.0,
            },
        ),
        quality_model=IRQualityModel(registry=registry),
        cost_model_source="agentic_shortcut_component_hints_v1",
    )


def shortcut_feature_schema_checksum() -> str:
    return _stable_checksum(
        {
            "schema": "agentic_shortcut_features_v1",
            "fields": (
                "src_subtype",
                "dst_subtype",
                "anchor_resolution_cost",
                "shortcut_read_cost",
                "evidence_packet_cost",
                "policy_decision_cost",
                "accept_probability",
                "quality_risk_upper_bound",
                "support_features",
            ),
        }
    )


def optimizer_shortcut_graph(
    evidence: Iterable[AgenticShortcutEvidence],
    *,
    base_cost: float = 1.0,
    base_quality: float = 1.0,
) -> OptimizerGraph:
    from .graph import OptimizerNode

    evidence_tuple = tuple(evidence)
    graph = OptimizerGraph(
        attrs={
            "cost": float(base_cost),
            "quality": float(base_quality),
            "allowed_edge_routes": tuple(
                sorted(
                    {
                        (item.src_subtype, item.edge_type, item.dst_subtype)
                        for item in evidence_tuple
                    }
                )
            ),
        }
    )
    for item in evidence_tuple:
        if item.src not in graph.nodes:
            graph = graph.add_node(OptimizerNode(item.src, item.src_subtype))
        if item.dst not in graph.nodes:
            graph = graph.add_node(OptimizerNode(item.dst, item.dst_subtype))
    return graph


def _stable_checksum(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()
