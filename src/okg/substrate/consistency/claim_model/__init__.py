"""The (C, Σ, ≻) claim-consistency core — pure, deployment-parametric.

Normative reference: ``docs/develop/architecture/consistency.md`` (versioned
with this package; a semantic change here without a matching spec edit is a
review defect). API reference — the frozen public surface (this module's
``__all__``) plus the API-freeze / versioning contract deployments adopt
against: ``docs/reference/consistency-claim-model-api.md`` (adoption proof:
``tests/substrate/consistency/test_claim_model_adoption_contract.py``). Hoisted
from the cern-twiki reference deployment under PACT
``consistency-claim-model-core``; cern-twiki's v1-currency-spec remains the
reference deployment profile.

Consistency checking over a claim graph is three correctable objects:

- **C** — effective claims: :mod:`.effective` merges derived claims with
  operator source-corrections (retract / override / assert).
- **Σ** — admitted constraints: deployments supply predicate schemas and
  incompatibility tables as :class:`.conflicts.ConflictRules`; the generic
  engine in :mod:`.conflicts` evaluates them.
- **≻** — scoped precedence: :mod:`.precedence` resolves a conflict bucket to
  canonical or contested under the fixed lexicographic signal order plus
  operator precedents.

Core invariant: every operator decision updates exactly one object
(source correction → C, rule correction → Σ, governance precedent → ≻).

Every module is pure (DB-free, no LLM, no wall-clock, deterministic sorted
output). Persistence, decision consumption, and the canonical/contested
projection layer on top (see the PACT's later tasks).
"""
from okg.substrate.consistency.claim_model.applicability import (
    OMEGA,
    Applicability,
    ScopeBounds,
    TimeBounds,
    applicability_relation,
)
from okg.substrate.consistency.claim_model.conflicts import (
    CONSTRAINT_AMBIGUOUS,
    LOCALITY_CROSS_DOC,
    LOCALITY_INTERNAL,
    OUTCOME_DEFINITE,
    OUTCOME_CONFLICT,
    OUTCOME_ALIGNMENT_UNCERTAIN,
    OUTCOME_APPLICABILITY_UNCERTAIN,
    OUTCOME_NO_CONFLICT,
    OUTCOME_CATEGORIES,
    OUTCOME_NOT_COMPARABLE,
    OUTCOME_POSSIBLE,
    APPLICABILITY_DIMENSIONS,
    ALIGNMENT_DIMENSIONS,
    LEGACY_POSSIBLE_MARKER,
    ConflictFinding,
    ConflictRules,
    CrossPredicatePolicy,
    TypedOutcome,
    classify_outcome,
    derive_conflict_findings,
    legacy_outcome,
)
from okg.substrate.consistency.claim_model.effective import (
    ACTION_ASSERT,
    ACTION_OVERRIDE,
    ACTION_RETRACT,
    PROV_ASSERTED,
    PROV_CORRECTED,
    PROV_DERIVED,
    Correction,
    EffectiveClaimError,
    MergeResult,
    merge_effective_claims,
)
from okg.substrate.consistency.claim_model.identity import (
    claim_key_slug,
    claim_logical_id,
)
from okg.substrate.consistency.claim_model.selection import (
    DEFAULT_GENERATORS,
    EMPTY_REPAIR_BUCKET_KEY,
    EMPTY_REPAIR_GENERATOR_ID,
    GENERATOR_VERSION_V1,
    OVERRIDE_SURVIVING_VALUE_GENERATOR,
    OVERRIDE_SURVIVING_VALUE_GENERATOR_ID,
    PRECEDENCE_RETRACTION_GENERATOR,
    PRECEDENCE_RETRACTION_GENERATOR_ID,
    SELECTION_SCHEMA_VERSION,
    SELECTION_SCHEMA_VERSION_LEGACY_V1,
    Candidate,
    CandidateSet,
    ConflictBucket,
    RepairCorrection,
    RepairGenerator,
    RepairSelectionError,
    SelectedCandidate,
    SelectedRepair,
    UnresolvedConflict,
    enumerate_candidates,
    select_repairs,
)
from okg.substrate.consistency.claim_model.precedence import (
    OUTCOME_CANONICAL,
    OUTCOME_CONTESTED,
    SIGNAL_DOCUMENT_RECENCY,
    SIGNAL_EFFECTIVE_DATE,
    SIGNAL_INDEPENDENT_SUPPORT,
    SIGNAL_MAINTENANCE,
    SIGNAL_PRECEDENT,
    SIGNAL_SOURCE_AUTHORITY,
    SIGNAL_SUPERSESSION,
    ClaimSignals,
    Precedent,
    Resolution,
    resolve_bucket,
)
from okg.substrate.consistency.claim_model.views import (
    ELEMENT_KIND_EDGE,
    ELEMENT_KIND_NODE,
    REL_DISJOINT,
    REL_OVERLAP,
    REL_UNKNOWN,
    SCOPE_KNOWN,
    SCOPE_UNKNOWN,
    ClaimSemantics,
    ClaimView,
    ElementRef,
    scope_relation,
)

__all__ = [
    "ACTION_ASSERT",
    "ACTION_OVERRIDE",
    "ACTION_RETRACT",
    "CONSTRAINT_AMBIGUOUS",
    "ClaimSemantics",
    "ClaimSignals",
    "ClaimView",
    "ConflictFinding",
    "ConflictRules",
    "Correction",
    "CrossPredicatePolicy",
    "ELEMENT_KIND_EDGE",
    "ELEMENT_KIND_NODE",
    "EffectiveClaimError",
    "ElementRef",
    "LOCALITY_CROSS_DOC",
    "LOCALITY_INTERNAL",
    "MergeResult",
    "OMEGA",
    "OUTCOME_CANONICAL",
    "OUTCOME_CONTESTED",
    "OUTCOME_DEFINITE",
    "OUTCOME_CONFLICT",
    "OUTCOME_ALIGNMENT_UNCERTAIN",
    "OUTCOME_APPLICABILITY_UNCERTAIN",
    "OUTCOME_NO_CONFLICT",
    "OUTCOME_CATEGORIES",
    "OUTCOME_NOT_COMPARABLE",
    "OUTCOME_POSSIBLE",
    "ALIGNMENT_DIMENSIONS",
    "APPLICABILITY_DIMENSIONS",
    "LEGACY_POSSIBLE_MARKER",
    "PROV_ASSERTED",
    "PROV_CORRECTED",
    "PROV_DERIVED",
    "Precedent",
    "REL_DISJOINT",
    "REL_OVERLAP",
    "REL_UNKNOWN",
    "Resolution",
    "SCOPE_KNOWN",
    "SCOPE_UNKNOWN",
    "ScopeBounds",
    "SIGNAL_DOCUMENT_RECENCY",
    "SIGNAL_EFFECTIVE_DATE",
    "SIGNAL_INDEPENDENT_SUPPORT",
    "SIGNAL_MAINTENANCE",
    "SIGNAL_PRECEDENT",
    "SIGNAL_SOURCE_AUTHORITY",
    "SIGNAL_SUPERSESSION",
    "TimeBounds",
    "Applicability",
    "applicability_relation",
    "claim_key_slug",
    "claim_logical_id",
    "derive_conflict_findings",
    "TypedOutcome",
    "classify_outcome",
    "legacy_outcome",
    "merge_effective_claims",
    "resolve_bucket",
    "scope_relation",
    # Repair selection (consistency-repair-objective) — additive.
    "Candidate",
    "CandidateSet",
    "ConflictBucket",
    "DEFAULT_GENERATORS",
    "EMPTY_REPAIR_BUCKET_KEY",
    "EMPTY_REPAIR_GENERATOR_ID",
    "GENERATOR_VERSION_V1",
    "OVERRIDE_SURVIVING_VALUE_GENERATOR",
    "OVERRIDE_SURVIVING_VALUE_GENERATOR_ID",
    "PRECEDENCE_RETRACTION_GENERATOR",
    "PRECEDENCE_RETRACTION_GENERATOR_ID",
    "RepairCorrection",
    "RepairGenerator",
    "RepairSelectionError",
    "SELECTION_SCHEMA_VERSION",
    "SELECTION_SCHEMA_VERSION_LEGACY_V1",
    "SelectedCandidate",
    "SelectedRepair",
    "UnresolvedConflict",
    "enumerate_candidates",
    "select_repairs",
]
