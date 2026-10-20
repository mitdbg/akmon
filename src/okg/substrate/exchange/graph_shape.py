"""Graph-shape classification for Poros named-subgraph messages."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


ROOT_PATTERN_VOCABULARY_VERSION = "poros-root-patterns-v1"

ROOT_PATTERN_VOCABULARY: dict[str, tuple[str, ...]] = {
    "claim": ("claim", "hypothesis"),
    "context": (
        "context_packet",
        "document",
        "document_chunk",
        "doc_chunk",
        "paragraph",
        "trace",
        "execution_trace",
        "agent_session",
    ),
    "dataset_reference": (
        "dataset",
        "dataset_reference",
        "data_reference",
        "data_asset",
    ),
    "proposal": ("proposal", "open_spec_change"),
}

GRAPH_PROFILE_BY_ROOT_PATTERN: dict[str, str] = {
    "claim": "claim",
    "context": "context",
    "dataset_reference": "dataset_reference",
    "proposal": "proposal",
    "fact_bundle": "fact_bundle",
    "unknown": "unknown",
}


@dataclass(frozen=True)
class PorosGraphShape:
    node_subtypes: tuple[str, ...]
    root_subtypes: tuple[str, ...]
    root_pattern: str
    graph_profile: str
    edge_narrowings: tuple[dict[str, str], ...] = ()
    vocabulary_version: str = ROOT_PATTERN_VOCABULARY_VERSION

    def to_resource_fields(self) -> dict[str, Any]:
        return {
            "graph_profile": self.graph_profile,
            "root_pattern": self.root_pattern,
            "node_subtypes": list(self.node_subtypes),
            "root_subtypes": list(self.root_subtypes),
            "edge_narrowings": [
                dict(narrowing) for narrowing in self.edge_narrowings
            ],
            "root_pattern_vocabulary": self.vocabulary_version,
        }


def classify_graph_body(
    body: Mapping[str, Any],
    *,
    extensions: Mapping[str, Sequence[str]] | None = None,
) -> PorosGraphShape:
    """Classify a Poros graph body and expose policy-relevant shape fields."""
    return classify_graph_shape(
        root_subtypes_for_body(body),
        node_subtypes=node_subtypes_for_body(body),
        edge_narrowings=edge_narrowings_for_body(body),
        extensions=extensions,
    )


def node_subtypes_for_body(body: Mapping[str, Any]) -> tuple[str, ...]:
    nodes = body.get("nodes") or body.get("node_facts") or []
    if not isinstance(nodes, list):
        return ()
    return tuple(
        sorted({
            subtype
            for node in nodes
            if isinstance(node, dict)
            and (subtype := _clean_str(node.get("subtype")))
        })
    )


def root_subtypes_for_body(body: Mapping[str, Any]) -> tuple[str, ...]:
    """Return sorted root subtypes from a Poros graph body.

    If `root_node_ids` is omitted, every node subtype is treated as a
    root-candidate subtype. This keeps legacy/draft messages
    conservative: policy sees the whole submitted shape.
    """
    nodes = body.get("nodes") or body.get("node_facts") or []
    if not isinstance(nodes, list):
        return ()
    raw_root_node_ids = body.get("root_node_ids") or []
    root_node_ids = (
        {str(item) for item in raw_root_node_ids}
        if isinstance(raw_root_node_ids, list)
        else set()
    )
    if root_node_ids:
        subtypes = [
            _clean_str(node.get("subtype"))
            for node in nodes
            if isinstance(node, dict)
            and str(node.get("node_id")) in root_node_ids
            and node.get("subtype")
        ]
    else:
        subtypes = [
            _clean_str(node.get("subtype"))
            for node in nodes
            if isinstance(node, dict) and node.get("subtype")
        ]
    return tuple(sorted({subtype for subtype in subtypes if subtype}))


def edge_narrowings_for_body(body: Mapping[str, Any]) -> tuple[dict[str, str], ...]:
    """Return sorted `(src_subtype, edge_type, dst_subtype)` signatures.

    Poros policy needs these signatures before admission so a sender trusted
    for one graph shape cannot submit an otherwise valid graph with disallowed
    edge narrowings and rely on downstream projection to catch it.
    """
    nodes = body.get("nodes") or body.get("node_facts") or []
    edges = body.get("edges") or body.get("edge_facts") or []
    if not isinstance(nodes, list) or not isinstance(edges, list):
        return ()

    node_subtypes = {
        str(node.get("node_id")): _clean_str(node.get("subtype"))
        for node in nodes
        if isinstance(node, dict) and node.get("node_id") and node.get("subtype")
    }
    narrowings: set[tuple[str, str, str]] = set()
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        edge_type = _clean_str(edge.get("edge_type"))
        if not edge_type:
            continue
        src_id = str(edge.get("src") or "")
        dst_id = str(edge.get("dst") or "")
        src_subtype = _clean_str(edge.get("src_subtype")) or node_subtypes.get(src_id)
        dst_subtype = _clean_str(edge.get("dst_subtype")) or node_subtypes.get(dst_id)
        narrowings.add((
            src_subtype or "<unknown>",
            edge_type,
            dst_subtype or "<unknown>",
        ))
    return tuple(
        {
            "src_subtype": src_subtype,
            "edge_type": edge_type,
            "dst_subtype": dst_subtype,
        }
        for src_subtype, edge_type, dst_subtype in sorted(narrowings)
    )


def classify_graph_shape(
    root_subtypes: Sequence[str],
    *,
    node_subtypes: Sequence[str] | None = None,
    edge_narrowings: Sequence[Mapping[str, str]] | None = None,
    extensions: Mapping[str, Sequence[str]] | None = None,
) -> PorosGraphShape:
    """Classify Poros root subtypes into the receiver's root vocabulary."""
    unique = tuple(
        sorted({cleaned for item in root_subtypes if (cleaned := _clean_str(item))})
    )
    if not unique:
        return PorosGraphShape(
            _normalized_subtypes(node_subtypes),
            (),
            "unknown",
            "unknown",
            _normalized_edge_narrowings(edge_narrowings),
        )

    vocabulary = _merged_vocabulary(extensions)
    patterns = {
        pattern
        for subtype in unique
        for pattern, subtypes in vocabulary.items()
        if subtype in subtypes
    }
    if "proposal" in patterns:
        root_pattern = "proposal"
    elif len(patterns) == 1:
        root_pattern = next(iter(patterns))
    elif patterns:
        root_pattern = "fact_bundle"
    else:
        root_pattern = "fact_bundle"
    return PorosGraphShape(
        node_subtypes=_normalized_subtypes(node_subtypes) or unique,
        root_subtypes=unique,
        root_pattern=root_pattern,
        graph_profile=GRAPH_PROFILE_BY_ROOT_PATTERN.get(
            root_pattern,
            root_pattern,
        ),
        edge_narrowings=_normalized_edge_narrowings(edge_narrowings),
    )


def infer_graph_profile_from_roots(root_subtypes: Sequence[str]) -> str:
    return classify_graph_shape(root_subtypes).graph_profile


def _normalized_subtypes(subtypes: Sequence[str] | None) -> tuple[str, ...]:
    if not subtypes:
        return ()
    return tuple(
        sorted({cleaned for item in subtypes if (cleaned := _clean_str(item))})
    )


def _merged_vocabulary(
    extensions: Mapping[str, Sequence[str]] | None,
) -> dict[str, set[str]]:
    vocabulary = {
        pattern: set(subtypes)
        for pattern, subtypes in ROOT_PATTERN_VOCABULARY.items()
    }
    for pattern, subtypes in (extensions or {}).items():
        normalized_pattern = _clean_str(pattern)
        if not normalized_pattern:
            continue
        if normalized_pattern not in vocabulary:
            vocabulary[normalized_pattern] = set()
        vocabulary[normalized_pattern].update(
            cleaned
            for item in subtypes
            if (cleaned := _clean_str(item))
        )
    return vocabulary


def _normalized_edge_narrowings(
    edge_narrowings: Sequence[Mapping[str, str]] | None,
) -> tuple[dict[str, str], ...]:
    if not edge_narrowings:
        return ()
    normalized: set[tuple[str, str, str]] = set()
    for narrowing in edge_narrowings:
        src_subtype = _clean_str(narrowing.get("src_subtype"))
        edge_type = _clean_str(narrowing.get("edge_type"))
        dst_subtype = _clean_str(narrowing.get("dst_subtype"))
        if src_subtype and edge_type and dst_subtype:
            normalized.add((src_subtype, edge_type, dst_subtype))
    return tuple(
        {
            "src_subtype": src_subtype,
            "edge_type": edge_type,
            "dst_subtype": dst_subtype,
        }
        for src_subtype, edge_type, dst_subtype in sorted(normalized)
    )


def _clean_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()
