"""Substrate post-projection enrichers.

After source facts are applied to the Chronos graph and before the
publisher checkpoints the generation, it invokes registered
enrichers. Each enricher reads the current graph checkout and emits
derived `edge_facts` (with `provenance='derived_deterministic'` or
`extracted_model`). Those facts flow through the same derived-fact
apply path as source-runner-emitted edges, so derived + explicit
edges land at the same published graph boundary.

Per `add-substrate-enricher-hook` (2026-04-25). Replaces the
legacy 4-stage pipeline's enricher hook (retired in M6.7.5
without a substrate-side replacement).
"""
from okg.substrate.enrichers.base import Enricher, EnrichResult
from okg.substrate.enrichers.lineage import (
    EnricherDependencyDeclaration,
    EnricherLineageInvalidation,
    EnricherLineagePlan,
    EnricherRunLineageEnvelope,
    build_enricher_run_lineage_envelope,
    declare_enricher_dependencies,
    invalidations_from_chronos_dirty_scope,
    plan_enricher_lineage,
)


__all__ = [
    "Enricher",
    "EnrichResult",
    "EnricherDependencyDeclaration",
    "EnricherLineageInvalidation",
    "EnricherLineagePlan",
    "EnricherRunLineageEnvelope",
    "build_enricher_run_lineage_envelope",
    "declare_enricher_dependencies",
    "invalidations_from_chronos_dirty_scope",
    "plan_enricher_lineage",
]
