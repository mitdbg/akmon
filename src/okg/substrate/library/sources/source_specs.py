"""Internal source-domain NodeSpec / EdgeSpec descriptors.

These value objects are used by source-domain emitters before public
SourceAdapter wrappers translate output to substrate-native NodeFact/EdgeFact
rows.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from okg.substrate.library.sources.base import NODE_OWNERSHIP_MODES


__all__ = [
    "EdgeSpec",
    "NodeSpec",
    "VALID_PROVENANCE_CLASSES",
]


@dataclass
class NodeSpec:
    """Lightweight descriptor for a node to be ingested."""

    node_id: str
    subtype: str
    label: str
    attrs: dict[str, Any]
    source: str  # provenance tag, e.g. "cric"
    ownership_mode: str = "authoritative"

    def __post_init__(self) -> None:
        if self.ownership_mode not in NODE_OWNERSHIP_MODES:
            raise ValueError(
                "node ownership_mode must be one of "
                f"{sorted(NODE_OWNERSHIP_MODES)}; got {self.ownership_mode!r}"
            )


VALID_PROVENANCE_CLASSES = frozenset(
    {
        "explicit",
        "derived_deterministic",
        "derived_semantic",
        "linked_heuristic",
        "extracted_model",
    }
)


@dataclass
class EdgeSpec:
    """Lightweight descriptor for an edge to be ingested."""

    src: str
    dst: str
    edge_type: str
    attrs: dict[str, Any] = field(default_factory=dict)
    provenance: str = "explicit"
    confidence: float | None = None
