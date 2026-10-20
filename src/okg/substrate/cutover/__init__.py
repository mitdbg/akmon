"""Cutover tooling (M6).

Enables per-deployment migration from the legacy
`okg.ingest.pipeline` (4-stage: ingest / link / resolve / enrich)
to the substrate. Every cutover is a parallel-run gated by a
shadow-diff: both pipelines consume the same input, write to
separate database state, and the harness compares the resulting
graph snapshots. Cutover proceeds only when the structural diff
is empty (or within an author-declared legitimate-diff budget)
for the operator-set soak period.

Shipped at M6:
  - `GraphSnapshot`: normalized, comparable representation of
    a graph state (nodes + edges + generation metadata)
  - `capture_substrate_snapshot(dsn, deployment, generation)`:
    reads the substrate's nodes_live / edges_live at a pin
  - `capture_legacy_snapshot(dsn)`: reads the legacy
    graph store's node/edge tables (for deployments still on
    legacy)
  - `shadow_diff(a, b)`: structural comparison with counts,
    symmetric differences, and a bounded sample of divergences
  - `DiffBudget`: author-declared legitimate diffs (expected
    shape changes between the two pipelines)

Deferred (per-deployment work):
  - Legacy-path driver (needs legacy infra per deployment)
  - Continuous "query diff" replay of MCP query sets
  - Telemetry emission during soak
"""
from okg.substrate.cutover.snapshot import (
    DiffBudget,
    GraphSnapshot,
    NodeSnapshot,
    EdgeSnapshot,
    capture_substrate_snapshot,
    shadow_diff,
)


__all__ = [
    "DiffBudget",
    "GraphSnapshot",
    "NodeSnapshot",
    "EdgeSnapshot",
    "capture_substrate_snapshot",
    "shadow_diff",
]
