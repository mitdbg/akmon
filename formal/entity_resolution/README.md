# Entity Resolution Formal Model

## System

`EntityMerge.tla` is a finite two-retiree refinement model for the entity-merge
remediation transaction. It models a whole-branch mutation token, the latest
published checkpoint/catalog pins, sequential replanning after the first
commit, and one follow-up publish. The graph contains a survivor, two distinct
retirees, and one neighboring node, with inbound and outbound edges that
collapse when each retiree is rehomed.

The model also makes the identity boundary explicit. A candidate record is
normalizable only when it contains the required `chunk`, `enricher`, and
top-level `node` fields, and that node is the retiree in the current
survivor/retiree pair. Extra fields are preserved and remain identity-bearing;
missing fields, nested-only lookalikes, and another pair's node do not
normalize. Candidate normalization is unavailable when the Nomos runtime mode
is `enforce`; receipt audit data is not treated as an alias proof.

## Design Contract

- Safety property: accepted merges atomically fold the intended retiree into
  the survivor, preserve a complete provenance audit, never commit against a
  changed branch token or policy baseline, and never accept an unproved
  candidate alias or semantic/confidence divergence.
- Authority boundary: `src/okg/substrate/entity_merge.py` owns planning,
  identity comparison, drift refusal, staging, and receipt content;
  `src/okg/substrate/publisher/publisher.py` owns the whole-branch mutation
  token over current graph rows.
- Valid runtime paths: a single merge, two sequential same-survivor merges
  before publish, pair-scoped candidate alias deduplication, catalog-authorized
  cross-producer deduplication, pre-mutation concurrent-write refusal, rollback,
  receipt replay, and final publication.
- Out of scope: arbitrary batch cardinality, SQL/Chronos implementation
  details, catalog composition, and queue/DLQ health are runtime evidence
  obligations rather than finite-state claims.
- Preconditions/refinement: the model assumes branch locks serialize writers,
  the mutation token covers every current graph row, and persistence follows
  the modeled atomic boundary. Those assumptions are exercised by
  `tests/substrate/entity_resolution/test_merge_rehome.py`,
  `tests/substrate/entity_resolution/test_merge_identity_projection.py`, and
  `tests/substrate/entity_resolution/test_merge_dedup_provenance.py`.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `EntityMerge.tla` | `EntityMerge.cfg` | Sequential two-retiree folding, branch-token validation, pair-scoped candidate projection, semantic/confidence refusal, provenance coupling, rollback, and publication. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `EntityMerge.tla` | Plan from the locked current branch while pinning the published/catalog baseline; validate the whole-branch token and approved digest before atomic graph/receipt writes; project only the current pair's shaped candidate record for comparison. | `src/okg/substrate/entity_merge.py`, `src/okg/substrate/publisher/publisher.py` | `tests/substrate/entity_resolution/test_merge_rehome.py`, `tests/substrate/entity_resolution/test_merge_identity_projection.py`, `tests/substrate/entity_resolution/test_merge_dedup_provenance.py` |

## Invariant Map

| Invariant | What it means | Code/test signal |
| --- | --- | --- |
| `TypeOK` | Finite domains and record/function shapes remain well typed. | Strict report-shape coverage in `tests/pact/test_formal_report_producers.py`. |
| `NoDangling` | Every live edge endpoint is a live node. | Exact graph assertions in `tests/substrate/entity_resolution/test_merge_rehome.py`. |
| `BatchFold` | Live nodes, edges, and survivor attributes equal the sequential fold over committed retirees. | Same-survivor batch test in `tests/substrate/entity_resolution/test_merge_rehome.py`. |
| `PrefixPreserved` | Once the first retiree is approved, it remains retired across later planning/refusal. | Batch plus concurrent-writer assertions in `tests/substrate/entity_resolution/test_merge_rehome.py`. |
| `ReceiptCoupled` | Every receipt has the complete survivor/retiree provenance union and distinct logical write keys. | Receipt assertions in `tests/substrate/entity_resolution/test_merge_dedup_provenance.py` and `tests/substrate/entity_resolution/test_merge_rehome.py`. |
| `PlanIdentityInjective` | Plan digests retain survivor, retiree, branch token, and approved content; distinct instances cannot collide. | Distinct digest/write-key assertions in `tests/substrate/entity_resolution/test_merge_rehome.py`. |
| `WholeBranchTokenPinned` | A staged plan cannot proceed with a changed whole-branch token. | Unrelated-writer refusal in `tests/substrate/entity_resolution/test_merge_rehome.py`. |
| `StagingIsPrivate` | Staging exposes no graph, ledger, attribute, audit, or receipt mutation. | Fault and refusal no-write assertions in `tests/substrate/entity_resolution/test_merge_rehome.py`. |
| `RefusalPreservesPrefix` | Stale, unsafe, or faulted plans refuse without changing the committed prefix. | Exact writer-only state comparison in `tests/substrate/entity_resolution/test_merge_rehome.py`. |
| `CandidateProjectionInvariant` | A shaped record normalizes only for its pair; extra fields are preserved and nested-only lookalikes stay unchanged. | Projection cases in `tests/substrate/entity_resolution/test_merge_identity_projection.py`. |
| `CandidatePairSafety` | A committed merge used a valid pair-scoped candidate record. | Candidate integration tests in `tests/substrate/entity_resolution/test_merge_dedup_provenance.py`. |
| `SemanticConfidenceSafety` | Committed merges never accept semantic or confidence divergence. | Divergence tests in `tests/substrate/entity_resolution/test_merge_dedup_provenance.py`. |
| `NomosEnforceNoAlias` | Enforce mode never commits a candidate alias without persisted proof. | Enforce refusal test in `tests/substrate/entity_resolution/test_merge_dedup_provenance.py`. |
| `PublishedCoupling` | Published state contains both committed merges and binds the current branch token. | Real publisher cycle in `tests/substrate/entity_resolution/test_merge_rehome.py`. |

## Verification Process

Run the bounded model:

```bash
uv run --extra dev python scripts/check_formal_entity_resolution.py \
  --out .scratch/formal/entity-resolution
```

Run the runtime refinement:

```bash
uv run --extra dev pytest -q --no-cov \
  tests/substrate/entity_resolution/test_merge_rehome.py \
  tests/substrate/entity_resolution/test_merge_identity_projection.py \
  tests/substrate/entity_resolution/test_merge_dedup_provenance.py
```

## Model Boundary

The TLA+ model is intentionally finite: it proves the two-merge batch fold and
the safety/refusal boundaries. PostgreSQL/Chronos storage behavior, arbitrary
batch sizes, catalog composition, and queue/DLQ readiness remain runtime
evidence obligations covered by the implementation tests and disposable
publish proof above.
