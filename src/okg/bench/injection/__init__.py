"""Synthetic inconsistency injection framework (PACT consistency-injection-benchmark).

Deterministic, provenance-exact mutation of a frozen document corpus,
targeted by the pinned clean generation's claim state, per
pact/changes/consistency-injection-benchmark/designs/design.md and
internal issue 749. The core guarantees:

- byte-deterministic artifacts for pinned (corpus digest, seed, operator
  versions, config digest); seed-disjoint mutation ids by construction;
- gold integrity gated reject-with-reason (ambiguous localization, byte
  pre-image mismatch, identity-unstable insertion, b0 overlap, span
  overlap) — never a silent drop;
- sealed ``benchmark-manifest.json`` (no run outcomes, no wall clock) and
  schema-validated ``mutations.jsonl`` / ``rejected_sites.jsonl``.

PUBLIC SURFACE. ``__all__`` below is the whole of it: the names a caller
outside this package is expected to use. Two layers are re-exported here —
the mutation library (targeting, operators, PRF, manifest, snapshots, types)
and the ring-0 end-to-end runner in ``benchmark``, which is what the
``okg bench inject`` CLI verb and the harness call. Everything else under
``okg.bench.injection.*`` is package-internal; importing a submodule directly
is not part of the contract.

DIRECTION OF DEPENDENCY. This package depends on the substrate; the substrate
does NOT depend on it, and must not — benchmark code may read the engine, the
engine may not read the corrupter. Every substrate name this package reaches
for is public API, and the whole set is pinned by
``tests/bench/injection/test_injection_surface.py``:

* the mutation library needs exactly one — ``okg.substrate.library.sources.base``
  (``NodeFact`` / ``SourceRun``, the types ``claim_corpus_source`` emits as a
  corpus Source);
* the ring-0 runner in ``benchmark`` additionally needs ``graph_store`` and
  ``publisher.factory``, both by deferred import, because it drives real
  publish cycles through the authority-bearing factory.

``tests/bench/injection/test_injection_boundary.py`` enforces the direction.
"""

from okg.bench.injection.independent import (
    INDEPENDENT_FACET_KINDS,
    IndependentSite,
    IndependentTargeter,
)
from okg.bench.injection.injector import (
    InjectionResult,
    apply_inverses,
    run_injection,
)
from okg.bench.injection.manifest import (
    canonical_json_bytes,
    compute_config_digest,
    compute_family_id,
    compute_mutation_id,
)
from okg.bench.injection.operator_base import MutationOperator
from okg.bench.injection.prf import KeyedPRF
from okg.bench.injection.scope_index import MarkdownScopeIndex
from okg.bench.injection.snapshot import ClaimSnapshot, CorpusSnapshot
from okg.bench.injection.types import (
    HARDNESS_TIERS,
    REJECTION_REASONS,
    ClaimRow,
    ExpectedOutcome,
    Facets,
    InjectionError,
    MutationPlan,
    MutationRecord,
    RejectedSite,
    RunConfig,
    Site,
    Span,
)

# Imported last: ``benchmark`` imports this package's other submodules, so it
# must run after they are bound.
from okg.bench.injection.benchmark import (  # noqa: E402
    RING0_CHECK_ID,
    RING0_DEPLOYMENT,
    RING0_POLICIES,
    RUN_BINDING_FILENAME,
    SCORECARD_ANNEX_FILENAME,
    SCORECARD_CORE_FILENAME,
    ingest_and_publish,
    load_pipeline_state,
    pipeline_state_at,
    render_scorecard,
    ring0_operators,
    ring0_stability_probe,
    routed_buckets,
    run_injection_benchmark,
    run_ring0_injection,
    score_ring0_run,
    stage_ledger_entry,
    sync_corpus,
    write_run_binding,
)

__all__ = [
    "HARDNESS_TIERS",
    "INDEPENDENT_FACET_KINDS",
    "REJECTION_REASONS",
    "ClaimRow",
    "ClaimSnapshot",
    "CorpusSnapshot",
    "ExpectedOutcome",
    "Facets",
    "IndependentSite",
    "IndependentTargeter",
    "InjectionError",
    "InjectionResult",
    "KeyedPRF",
    "MarkdownScopeIndex",
    "MutationOperator",
    "MutationPlan",
    "MutationRecord",
    "RING0_CHECK_ID",
    "RING0_DEPLOYMENT",
    "RING0_POLICIES",
    "RUN_BINDING_FILENAME",
    "RejectedSite",
    "RunConfig",
    "SCORECARD_ANNEX_FILENAME",
    "SCORECARD_CORE_FILENAME",
    "Site",
    "Span",
    "apply_inverses",
    "canonical_json_bytes",
    "compute_config_digest",
    "compute_family_id",
    "compute_mutation_id",
    "ingest_and_publish",
    "load_pipeline_state",
    "pipeline_state_at",
    "render_scorecard",
    "ring0_operators",
    "ring0_stability_probe",
    "routed_buckets",
    "run_injection",
    "run_injection_benchmark",
    "run_ring0_injection",
    "score_ring0_run",
    "stage_ledger_entry",
    "sync_corpus",
    "write_run_binding",
]
