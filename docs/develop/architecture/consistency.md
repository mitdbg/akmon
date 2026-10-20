---
audience: developer
type: reference
canonical_id: consistency-model
section: development
visibility: primary
status: active
owner: architecture
---
# Claim consistency

Claim consistency represents competing source claims, applies ordered
corrections, records conflicts, and evaluates declared precedence rules. It
also checks whether an unchanged claim result has enough dependency evidence
to be reused. It does not make a source fact true or publish a graph version.

Runtime support is limited to the opt-in `consistency-fixture`, which evaluates
one unchanged-state claim case after publication. Relationship builders,
rollups, views, and embeddings are not included. The check does not retry, and
its failure does not invalidate the graph version that already published.
Normal deployments use the complete publication path.

## Inputs, outputs, and authority

The consistency component consumes the accepted claim set, operator
corrections, deployment rules and precedence, the previous resolved result,
and the recorded change/evidence needed to assess reuse. It produces effective
claims, conflict findings, canonical-or-contested resolutions, repair
selections, equivalence evidence, and a check result that tells publication
whether the candidate may proceed.

| Question | Consistency contract |
|---|---|
| What may it decide? | Which claims are effective after ordered corrections; which claims conflict or cannot be compared; whether a declared precedence has a decisive winner; which derived steps are in the affected frontier; whether prior work is safe to reuse; and whether its checks are complete enough for publication policy. |
| What may it not decide? | It cannot assert that a source is factually true, delete the evidence for a losing claim, broaden a deployment's constraints, or move the reader-visible generation. The publisher owns that final pointer. |
| What is the safe optimization? | Reuse is allowed only behind recorded dependency evidence: a valid reuse certificate, a provably unchanged dependency set, or a conservative possible-effects/dominance proof. Extra recomputation is acceptable; stale reuse is not. |
| What happens when scope or evidence is uncertain? | The incremental pass widens to a complete computation with a recorded reason, or refuses the skip. Unknown effects, missing dependency facts, malformed rules, and unavailable checks never become an empty or healthy result. |
| What happens to disagreement? | Conflicting assertions and provenance remain available. A decisive policy can mark one canonical and the others superseded; a tie or ranking-only lead remains contested. A passing check means only that configured checks passed, not that the surviving claim is true. |

The consistency engine is therefore fail-closed: missing evidence removes the
permission to skip, not the requirement to compute. An operator or storage
failure leaves the prior state intact and records a visible failure.

## 1. Internal model: C, Σ, and ≻

The model has three inputs:

- **C — claims:** what imported sources and operator corrections assert.
- **Σ — constraints:** which combinations of claims are incompatible.
- **≻ — precedence:** which claim should win when a conflict is resolvable.

Each operator decision changes one of these inputs: correct an extracted claim
in C, change a rule in Σ, or change a governance preference in ≻. Keeping these
decisions separate makes later results explainable.

The pure implementation is in
`src/okg/substrate/consistency/claim_model/`. Deployment configuration supplies
predicate schemas, incompatibility rules, allowed scope values, and check
programs.

## 2. Claims and corrections

A `ClaimView` contains a predicate, logical key, value, source, scope,
provenance, and optional typed meaning. `claim_logical_id` excludes the value,
so correcting the value does not invent a new logical assertion.

An optional `ElementRef` binds a claim to one graph record or relationship.
Failure to resolve a binding remains explicit; AKMON does not guess from
similar text.

Corrections are ordered and have one of three actions:

| Action | Meaning |
|---|---|
| `retract` | Remove the named claim from the effective set. |
| `override` | Replace named fields on an existing claim. |
| `assert` | Add an operator-authored claim. |

At the same sequence number, the deterministic action order is retract,
override, assert. Expired corrections remain observable instead of silently
disappearing.

## 3. Constraints (Σ)

Constraints are versioned deployment data. They describe incompatibility, not
confidence. A check receives a complete declared rule; a missing or malformed
rule fails validation. Constraints do not choose a winner.

## 4. Conflict findings

A finding identifies the conflicting claim set, the rule that found it, its
scope, and evidence. Finding identity is stable across reruns of the same
logical conflict. A clean evaluation can still find conflicts; evaluation
health and publication policy are separate fields.

## 5. Precedence and repair (≻)

Resolution compares claims only inside the same conflict bucket. Governance
signals may include operator correction, source class, policy priority, and
recency when the deployment declares them. Ranking breaks ties only where the
declared policy makes one claim decisively preferable.

The result for each claim is:

- `canonical` when one claim is decisively preferred;
- `superseded` for claims displaced by that winner; or
- `contested` when no unique winner exists.

Source claims are preserved. A repair selection records which claims were
distrusted or overridden; it does not delete their provenance.

## 6. Checks and publication policy

Checks are deterministic programs with declared read sets. The engine derives
the affected frontier from a change and evaluates only the required region.
Coverage metadata records whether that scoped evaluation is sufficient.

Publication policy uses these current keys:

| Key | Meaning |
|---|---|
| `block_on: any_open` | Block while any matching open finding exists. |
| `block_on: new_findings` | Block only on findings opened by this candidate. |
| `block_on: never` | Report findings but do not block publication. |
| `block_on_min_severity` | Apply the blocking rule at or above this severity. |

The result field reports evaluation health such as `evaluated`. Finding counts
are separate fields including `newly_opened_count` and `resolved_count`.
`blocks_publish` records the policy decision.

## 7. Deliberate limits

- Confidence is evidence metadata, not a hidden precedence score.
- Incompatibility is boolean; there are no probabilistic conflict levels.
- Deployments do not share constraints or precedence automatically.
- A scoped check cannot certify claims outside its declared coverage.
- An unavailable dependency fails explicitly; it is not treated as an empty
  or healthy result.

## 8. Core and deployment responsibilities

| AKMON core | Deployment configuration |
|---|---|
| Claim identity and correction merge | Predicate schemas and incompatibility tables |
| Conflict derivation and precedence evaluation | Closed scope vocabulary |
| Canonical, superseded, and contested statuses | Extraction and document-intent rules |
| Check execution, coverage, and blocking policy | Check programs and severity policy |
| Incremental reuse and equivalence evidence | Source-specific governance priorities |

## 9. Incremental maintenance

Incremental execution must produce the same resolved state as a complete run
over the same source snapshot and recorded model outcomes. Skipping work is
allowed only when recorded evidence proves the skipped work cannot change
the result. A source correction first marks the affected dependency frontier;
the engine recomputes dirty buckets in dependency order and carries forward
only unaffected results whose evidence still matches. A missing tier-1
dependency table, malformed correction, missing reuse evidence, changed policy,
changed scope, or changed schema forces a complete computation, or a loud
refusal before any skip. It never silently downgrades to
unsafe reuse.

The reusable result is a checked maintenance decision, not an assertion that a
claim is true. The live v1 hook currently evaluates only unchanged fixture
input; changed-input execution remains a component contract rather than a
generally deployed runtime capability.

### 9.1 Same-result contract

`run_equivalence_series` compares incremental execution and complete execution
with one differ. Verdicts are `equal`, `mismatch`, or `excluded`. A mismatch is
release-blocking. An exclusion names missing replay evidence and is never
counted as agreement.

### 9.2 Recorded model outcomes

Each model-backed call has a stable key over step identity, operator version,
model, prompt content, and typed input. Record mode reuses the first stored
outcome for that key. Replay mode refuses a missing key instead of sampling.
Changing any key component requests a distinct outcome.

### 9.3 Producer write envelopes

Every producer declares the record types, relationship shapes, claim fields,
and closed value sets it may emit, or explicitly declares a dimension
unbounded. Output outside a finite envelope records a violation and prevents
unsafe reuse. Observed history never widens authority.

### 9.4 Possible-effects summaries

For a changed step, AKMON over-approximates every finding, candidate, and
selection that the producer could affect. An unknown effect forces a complete
run with a recorded cause.

### 9.5 Reuse certificates

Every skipped step inside the affected dependency cone records a certificate.
The certificate binds the incumbent result, comparison, dependencies,
versions, policies, catalogs, constraints, and producer envelopes used to
justify the skip. Any changed dependency invalidates it before the next reuse
decision.

### 9.6 Resolution-aware scheduler

The scheduler may skip work only when the possible-effects bound cannot beat
or invalidate the current resolved choice. An undecidable comparison runs the
step. A missing certificate or dependency is a loud refusal, never permission
to skip.

### 9.7 Complete reference execution

The reference path recomputes from the same source snapshot and consumes the
same recorded nondeterministic outcomes. It shares the result differ but not
the incremental skip decision. Certified differences are reported explicitly
when a valid certificate licenses a representation difference.

### 9.8 Differential repair selection

Incremental selection recomputes dirty conflict buckets and copies untouched
bucket results forward only when their selection schema version matches.
`selection_digest` is stable and order-independent. A schema mismatch forces a
complete selection pass for that generation.

### 9.9 Work accounting

The work ledger records measured units for effect analysis, certificate
validation, deterministic execution, model calls, propagation, and repair
solving. Uninstrumented work is `measured = false` with a null count, never a
silent zero. Reference runs are marked as validation work so savings reports do
not count counterfactual work as production cost.

## Implementation and verification

| Area | Code | Primary tests |
|---|---|---|
| Claim identity, corrections, conflicts, precedence | `src/okg/substrate/consistency/claim_model/` | `tests/substrate/consistency/test_claim_model_properties.py` |
| Check programs and publication policy | `src/okg/substrate/consistency/` | `tests/substrate/test_consistency_gates.py` |
| Recorded outcomes | `src/okg/substrate/llm/outcomes.py` | `tests/substrate/consistency/test_outcome_replay.py` |
| Envelopes and effect summaries | `src/okg/substrate/producer_authority.py`, `src/okg/substrate/consistency/` | `tests/substrate/test_producer_envelopes.py`, `tests/substrate/consistency/test_possible_effects.py` |
| Reuse certificates and scheduling | `src/okg/substrate/consistency/` | `tests/substrate/consistency/test_reuse_certificates.py`, `tests/substrate/consistency/test_reuse_scheduler.py` |
| Reference comparison and work ledger | `src/okg/substrate/qa/`, `src/okg/substrate/telemetry/` | `tests/substrate/consistency/test_equivalence_harness.py`, `tests/substrate/telemetry/test_work_ledger.py` |

Exact public Python types are listed in
[Claim-model API](../extensions/consistency-api.md). Persisted tables
and columns are listed in [Database schema](../../reference/database-schema.md).
