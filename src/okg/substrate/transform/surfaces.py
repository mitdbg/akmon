"""The substrate surface registry (task.gtir.surface-registry).

The transform IR grammar contains zero hard-coded surface names; a deployment
*registers* surfaces. This module seeds the registry for the OKG substrate,
binding each logical surface to a real table/view and declaring its pin
semantics. Bindings were verified against ``src/okg/substrate/db/schema.sql`` and
``views.sql`` -- no invented names.

Pin semantics (``pinned=True`` means reads are stable under a generation pin):
- fact logs and live/derived surfaces are generation-pinned;
- catalog/policy surfaces are version-pinned (modelled as pinned);
- the IR ledger, the trace log, and pre-publish admission state are NOT
  generation-pinned (content-addressed / append-only / pre-commit).
"""

from __future__ import annotations

from .registry import Surface, SurfaceKind, SurfaceRegistry

# (name, kind, binding, pinned) -- each binding is a real okg.* object.
SUBSTRATE_SURFACES: tuple[Surface, ...] = (
    # Fact logs (append-only, bitemporal).
    Surface("node_facts", SurfaceKind.FACT_LOG, "okg.node_facts", pinned=True),
    Surface("edge_facts", SurfaceKind.FACT_LOG, "okg.edge_facts", pinned=True),
    # Live / derived (generation-pinned read views).
    Surface("nodes_live", SurfaceKind.LIVE, "okg.v_nodes", pinned=True),
    Surface("edges_live", SurfaceKind.LIVE, "okg.v_edges", pinned=True),
    # Catalog / policy (version-pinned).
    Surface("catalog", SurfaceKind.CATALOG_POLICY, "okg.catalog_versions", pinned=True),
    Surface("narrowing_catalog", SurfaceKind.CATALOG_POLICY, "okg.narrowings", pinned=True),
    # Indexes.
    Surface("alias_index", SurfaceKind.INDEX, "okg.v_aliases", pinned=True),
    Surface("search_index", SurfaceKind.INDEX, "okg.v_nodes", pinned=True),
    # IR ledger (content-addressed; not generation-pinned).
    Surface("ir_programs", SurfaceKind.LEDGER, "okg.ir_programs", pinned=False),
    Surface("ir_invocations", SurfaceKind.LEDGER, "okg.ir_invocations", pinned=False),
    # Trace log (append-only; not pinned).
    Surface("trace", SurfaceKind.TRACE, "okg.mcp_trace_events", pinned=False),
    # Pre-publish admission state (not generation-pinned).
    Surface("admission_envelope", SurfaceKind.FACT_LOG, "okg.v_admission_envelopes", pinned=False),
)


def substrate_surface_registry() -> SurfaceRegistry:
    """A fresh registry seeded with the OKG substrate surfaces."""
    return SurfaceRegistry(SUBSTRATE_SURFACES)


# Chronos-native bindings (PACT chronos-native-graph-cutover): the live graph
# surfaces resolve to the Chronos-versioned base tables okg.graph_nodes /
# okg.graph_edges instead of the legacy okg.v_* pinned views. A lowered program
# over these surfaces is executed through a pinned Chronos checkout session
# (store.checkout_generation(gen).query(...)), which injects branch/checkpoint
# visibility -- so no okg.pin_generation GUC / SQL pin predicate is used. The
# catalog/policy surfaces (narrowings, catalog_versions) are NOT Chronos-versioned
# (plain catalog tables) and pass through the session unchanged.
CHRONOS_SURFACES: tuple[Surface, ...] = tuple(
    Surface("nodes_live", SurfaceKind.LIVE, "okg.graph_nodes", pinned=True)
    if s.name == "nodes_live"
    else Surface("edges_live", SurfaceKind.LIVE, "okg.graph_edges", pinned=True)
    if s.name == "edges_live"
    else Surface("search_index", SurfaceKind.INDEX, "okg.graph_nodes", pinned=True)
    if s.name == "search_index"
    else s
    for s in SUBSTRATE_SURFACES
)


def chronos_surface_registry() -> SurfaceRegistry:
    """A registry whose live surfaces bind to the Chronos graph tables.

    Lower a read program over this registry and execute it via a Chronos checkout
    session; the session applies the generation pin (a checkpoint), so the lowered
    SQL carries no pin predicate.
    """
    return SurfaceRegistry(CHRONOS_SURFACES)


__all__ = [
    "SUBSTRATE_SURFACES",
    "CHRONOS_SURFACES",
    "substrate_surface_registry",
    "chronos_surface_registry",
]
