"""GraphSnapshot + shadow_diff — the cutover harness (M6.7.3).

The shadow-diff compares two graph states node-for-node and
edge-for-edge. It is the gate for every per-deployment
cutover: parallel-run both pipelines, capture snapshots, diff.
When the diff is empty (within any author-declared legitimate
diffs) for the soak period, the deployment cuts over.

Scope contracts:

  - A snapshot is a PURE value — two snapshots can be captured
    at different points in time and compared without touching
    the originating DB.
  - The diff is deterministic: sort-and-walk both sides; no
    randomness.
  - Samples of divergent rows are capped (≤ `sample_cap`) so a
    10^6-row difference doesn't blow up memory.
  - A legitimate-diff budget lets the author encode "the new
    pipeline emits retractions the old one didn't" (a class of
    expected differences per proposal.md shadow-diff protocol).

Intentionally SMALL surface. The harness runs node-id-level
comparison; deeper attr-level diffing can layer on top when a
deployment needs it.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class NodeSnapshot:
    node_id: str
    subtype: str
    text_hash: Optional[str]


@dataclass(frozen=True)
class EdgeSnapshot:
    # edge_id is omitted from the identity tuple on purpose:
    # legacy and substrate mint edge_ids differently
    # (counter vs uuid5-hash). The edge IDENTITY for diff
    # purposes is (src, edge_type, dst).
    src: str
    edge_type: str
    dst: str


@dataclass
class GraphSnapshot:
    """A comparable snapshot of one graph state.

    `origin` identifies which pipeline produced it
    ('substrate' | 'legacy' | '<deployment>:<timestamp>').
    `generation_id` / `catalog_version_id` are substrate-side
    only (None for legacy snapshots).
    """

    origin: str
    nodes: frozenset[NodeSnapshot]
    edges: frozenset[EdgeSnapshot]
    generation_id: Optional[int | str] = None
    catalog_version_id: Optional[int] = None

    @property
    def node_count(self) -> int:
        return len(self.nodes)

    @property
    def edge_count(self) -> int:
        return len(self.edges)

    @property
    def node_ids(self) -> frozenset[str]:
        return frozenset(n.node_id for n in self.nodes)

    @property
    def edge_tuples(self) -> frozenset[tuple[str, str, str]]:
        return frozenset((e.src, e.edge_type, e.dst) for e in self.edges)


@dataclass
class DiffBudget:
    """Author-declared expected differences.

    `allow_nodes_only_in_b` / `allow_nodes_only_in_a`: sets of
    node_ids the author expects to appear only on one side.
    Useful when the substrate emits richer subtype resolution
    than the legacy extractor did, producing nodes the legacy
    doesn't have (or vice versa).

    `allow_edges_only_in_b` / `allow_edges_only_in_a`: same for
    (src, edge_type, dst) tuples.

    `expected_subtype_remap`: {legacy_subtype: substrate_subtype}
    — applied when comparing node subtypes. Useful when M5.4
    reconcile renamed a subtype but legacy kept the old name.
    """

    allow_nodes_only_in_a: frozenset[str] = field(default_factory=frozenset)
    allow_nodes_only_in_b: frozenset[str] = field(default_factory=frozenset)
    allow_edges_only_in_a: frozenset[tuple[str, str, str]] = \
        field(default_factory=frozenset)
    allow_edges_only_in_b: frozenset[tuple[str, str, str]] = \
        field(default_factory=frozenset)
    expected_subtype_remap: dict[str, str] = field(default_factory=dict)


@dataclass
class ShadowDiffResult:
    """Outcome of a `shadow_diff(a, b)` call.

    `is_clean` is True iff no divergence survives the budget.
    """

    a_origin: str
    b_origin: str
    a_node_count: int
    b_node_count: int
    a_edge_count: int
    b_edge_count: int
    nodes_only_in_a: list[str]
    nodes_only_in_b: list[str]
    edges_only_in_a: list[tuple[str, str, str]]
    edges_only_in_b: list[tuple[str, str, str]]
    subtype_mismatches: list[dict[str, Any]]
    text_hash_mismatches: list[dict[str, Any]]
    sample_cap: int
    # Counts BEFORE budget — useful for observability even when
    # the budget silences.
    raw_counts: dict[str, int] = field(default_factory=dict)

    @property
    def is_clean(self) -> bool:
        return (
            not self.nodes_only_in_a
            and not self.nodes_only_in_b
            and not self.edges_only_in_a
            and not self.edges_only_in_b
            and not self.subtype_mismatches
            and not self.text_hash_mismatches
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "a_origin": self.a_origin,
            "b_origin": self.b_origin,
            "a_node_count": self.a_node_count,
            "b_node_count": self.b_node_count,
            "a_edge_count": self.a_edge_count,
            "b_edge_count": self.b_edge_count,
            "is_clean": self.is_clean,
            "nodes_only_in_a": self.nodes_only_in_a,
            "nodes_only_in_b": self.nodes_only_in_b,
            "edges_only_in_a": [list(t) for t in self.edges_only_in_a],
            "edges_only_in_b": [list(t) for t in self.edges_only_in_b],
            "subtype_mismatches": self.subtype_mismatches,
            "text_hash_mismatches": self.text_hash_mismatches,
            "raw_counts": self.raw_counts,
            "sample_cap": self.sample_cap,
        }


def capture_substrate_snapshot(
    dsn: str,
    *,
    deployment_name: str = "default",
    generation_id: Optional[int | str] = None,
) -> GraphSnapshot:
    """Read the substrate graph at a Chronos generation checkpoint.

    `generation_id=None` pins to the latest published.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        if generation_id is None:
            checkpoint = store.latest_generation(deployment_name=deployment_name)
            if checkpoint is None:
                raise RuntimeError(
                    f"no published generation for {deployment_name!r}"
                )
            generation_id = checkpoint.checkpoint_id
        else:
            checkpoint = store.get_generation(generation_id)
            metadata = checkpoint.metadata or {}
            if (
                metadata.get("deployment_name") != deployment_name
                or metadata.get("status") != "published"
            ):
                raise RuntimeError(f"generation {generation_id} not published")
        catalog_version_id = (checkpoint.metadata or {}).get("catalog_version_id")
        graph = store.checkout_generation(generation_id)

        nodes = frozenset(
            NodeSnapshot(
                node_id=str(r["node_id"]),
                subtype=str(r["subtype"]),
                text_hash=r["text_hash"],
            )
            for r in graph.query(
                """
                SELECT node_id, subtype, text_hash
                  FROM okg.graph_nodes
                 ORDER BY node_id
                """
            )
        )

        edges = frozenset(
            EdgeSnapshot(
                src=str(r["src"]),
                edge_type=str(r["edge_type"]),
                dst=str(r["dst"]),
            )
            for r in graph.query(
                """
                SELECT src, edge_type, dst
                  FROM okg.graph_edges
                 ORDER BY src, edge_type, dst
                """
            )
        )
    finally:
        store.close()

    return GraphSnapshot(
        origin=f"substrate:{deployment_name}:g{generation_id}",
        nodes=nodes,
        edges=edges,
        generation_id=generation_id,
        catalog_version_id=(
            int(catalog_version_id)
            if catalog_version_id is not None
            else None
        ),
    )


def shadow_diff(
    a: GraphSnapshot,
    b: GraphSnapshot,
    *,
    budget: Optional[DiffBudget] = None,
    sample_cap: int = 100,
) -> ShadowDiffResult:
    """Structural diff: A vs B.

    Deterministic: walk both sides in sorted order; samples are
    the first `sample_cap` items of each divergent set.

    Budget removes author-declared legitimate differences from
    the result. Counts under `raw_counts` reflect pre-budget
    reality for observability.
    """
    budget = budget or DiffBudget()

    # Remap subtypes on A per the budget's subtype_remap so
    # "legacy says 'Issue', substrate says 'Ticket'" doesn't
    # false-positive.
    def _remap_node(n: NodeSnapshot) -> NodeSnapshot:
        if n.subtype in budget.expected_subtype_remap:
            return NodeSnapshot(
                node_id=n.node_id,
                subtype=budget.expected_subtype_remap[n.subtype],
                text_hash=n.text_hash,
            )
        return n

    a_nodes = {n.node_id: _remap_node(n) for n in a.nodes}
    b_nodes = {n.node_id: n for n in b.nodes}

    raw_only_a = sorted(set(a_nodes) - set(b_nodes))
    raw_only_b = sorted(set(b_nodes) - set(a_nodes))

    nodes_only_in_a = [
        nid for nid in raw_only_a
        if nid not in budget.allow_nodes_only_in_a
    ]
    nodes_only_in_b = [
        nid for nid in raw_only_b
        if nid not in budget.allow_nodes_only_in_b
    ]

    subtype_mismatches: list[dict[str, Any]] = []
    text_hash_mismatches: list[dict[str, Any]] = []
    for nid in sorted(set(a_nodes) & set(b_nodes)):
        an, bn = a_nodes[nid], b_nodes[nid]
        if an.subtype != bn.subtype:
            subtype_mismatches.append({
                "node_id": nid,
                "a_subtype": an.subtype,
                "b_subtype": bn.subtype,
            })
        if (
            an.text_hash is not None
            and bn.text_hash is not None
            and an.text_hash != bn.text_hash
        ):
            text_hash_mismatches.append({
                "node_id": nid,
                "a_text_hash": an.text_hash,
                "b_text_hash": bn.text_hash,
            })

    a_edges = a.edge_tuples
    b_edges = b.edge_tuples
    raw_edges_a = sorted(a_edges - b_edges)
    raw_edges_b = sorted(b_edges - a_edges)
    edges_only_in_a = [
        t for t in raw_edges_a if t not in budget.allow_edges_only_in_a
    ]
    edges_only_in_b = [
        t for t in raw_edges_b if t not in budget.allow_edges_only_in_b
    ]

    return ShadowDiffResult(
        a_origin=a.origin,
        b_origin=b.origin,
        a_node_count=a.node_count,
        b_node_count=b.node_count,
        a_edge_count=a.edge_count,
        b_edge_count=b.edge_count,
        nodes_only_in_a=nodes_only_in_a[:sample_cap],
        nodes_only_in_b=nodes_only_in_b[:sample_cap],
        edges_only_in_a=edges_only_in_a[:sample_cap],
        edges_only_in_b=edges_only_in_b[:sample_cap],
        subtype_mismatches=subtype_mismatches[:sample_cap],
        text_hash_mismatches=text_hash_mismatches[:sample_cap],
        sample_cap=sample_cap,
        raw_counts={
            "raw_nodes_only_in_a": len(raw_only_a),
            "raw_nodes_only_in_b": len(raw_only_b),
            "raw_edges_only_in_a": len(raw_edges_a),
            "raw_edges_only_in_b": len(raw_edges_b),
            "raw_subtype_mismatches": len(subtype_mismatches),
            "raw_text_hash_mismatches": len(text_hash_mismatches),
        },
    )
