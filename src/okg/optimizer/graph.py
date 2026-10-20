"""Small immutable graph value types for optimizer planning."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any


def _frozen_attrs(attrs: Mapping[str, Any] | None) -> Mapping[str, Any]:
    return MappingProxyType(dict(attrs or {}))


@dataclass(frozen=True)
class OptimizerNode:
    node_id: str
    subtype: str
    attrs: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        object.__setattr__(self, "attrs", _frozen_attrs(self.attrs))


@dataclass(frozen=True)
class OptimizerEdge:
    edge_id: str
    src: str
    dst: str
    edge_type: str
    attrs: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        object.__setattr__(self, "attrs", _frozen_attrs(self.attrs))


@dataclass(frozen=True)
class OptimizerGraph:
    """Typed directed multigraph snapshot used by the optimizer."""

    nodes: Mapping[str, OptimizerNode] = field(
        default_factory=lambda: MappingProxyType({})
    )
    edges: Mapping[str, OptimizerEdge] = field(
        default_factory=lambda: MappingProxyType({})
    )
    attrs: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        object.__setattr__(self, "nodes", MappingProxyType(dict(self.nodes)))
        object.__setattr__(self, "edges", MappingProxyType(dict(self.edges)))
        object.__setattr__(self, "attrs", _frozen_attrs(self.attrs))

    def add_node(self, node: OptimizerNode) -> "OptimizerGraph":
        nodes = dict(self.nodes)
        nodes[node.node_id] = node
        return replace(self, nodes=nodes)

    def add_edge(self, edge: OptimizerEdge) -> "OptimizerGraph":
        if edge.src not in self.nodes:
            raise ValueError(f"edge {edge.edge_id!r} has missing src {edge.src!r}")
        if edge.dst not in self.nodes:
            raise ValueError(f"edge {edge.edge_id!r} has missing dst {edge.dst!r}")
        edges = dict(self.edges)
        edges[edge.edge_id] = edge
        return replace(self, edges=edges)

    def with_node_attrs(self, node_id: str, attrs: Mapping[str, Any]) -> "OptimizerGraph":
        if node_id not in self.nodes:
            raise KeyError(node_id)
        nodes = dict(self.nodes)
        current = nodes[node_id]
        merged = dict(current.attrs)
        merged.update(attrs)
        nodes[node_id] = replace(current, attrs=merged)
        return replace(self, nodes=nodes)

    def with_attrs(self, attrs: Mapping[str, Any]) -> "OptimizerGraph":
        merged = dict(self.attrs)
        merged.update(attrs)
        return replace(self, attrs=merged)
