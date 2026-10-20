# Source retirement and derived-node liveness

## System

This model covers source-retirement planning, graph retraction, crash recovery,
publication, registry disablement, and the derived-support cleanup required by
issues #1230 and #1231. The production failure modes are stale or ambiguous
plans mutating the graph, partial retractions becoming visible, unsupported
derived nodes surviving a source retirement, and historical snapshots changing.

## Design Contract

- Safety property: A source retirement changes only the digest-bound footprint,
  never publishes dangling or unsupported graph content, retains the pinned
  historical snapshot, and does not disable registry authority before a complete
  terminal receipt exists.
- Authority boundary: The retirement plan and receipt own mutation authority for
  the selected deployment, branch, registry identity, source footprint, catalog,
  and policy version; the publisher alone advances the visible generation.
- Derived-row identity boundary: one current node row may be updated by the same
  declared producer, while a different producer attempting the same node id is
  refused before any graph effect. The finite `EmptySource`/collision state is
  the model representative for that PR2 admission path.
- Valid runtime paths: retirement may apply an empty derived-support frontier;
  the publisher evaluates explicit `any_member` and `all_members` support,
  retracts the unsupported fixed point, passes the cleaned graph through the
  remaining gates, and publishes. Interrupted applications resume through
  durable chunk and terminal receipts.
- Out of scope: Cryptographic hash collision resistance, database scheduling,
  worker fairness, unbounded graph size, performance, and operational queue/DLQ
  health are outside the finite state model.
- Preconditions/refinement: `tests/substrate/test_source_retirement.py` checks
  canonical digest binding, exact registry/source identity, chunk replay,
  rollback and continuation behavior, publication fencing, and historical reads.
  Disposable publish evidence must additionally verify exact node/edge deltas,
  queue/DLQ bounds, and doctor health.

## Model Inventory

| Model/configuration | Purpose |
| --- | --- |
| `SourceRetirementLiveness.tla` | Finite state machine for PR1 refusal/application and PR2 derived-support cleanup, crash recovery, publication, and history. |
| `SourceRetirementLiveness.cfg` | Concrete two-source graph, derived dependency shapes, failure injections, invariants, and temporal closure property used by TLC. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `SourceRetirementLiveness.tla` | Resolve and freeze a source footprint, apply replay-safe node/edge retractions, preserve a durable receipt across interruption, compute the derived-support fixed point before the remaining publish gates, bind publication to the exact graph effect, and only then allow registry removal. | `src/okg/substrate/source_retirement.py`; `src/okg/substrate/derived_node_liveness.py`; `src/okg/substrate/graph_store_internal/writes.py`; `src/okg/substrate/consistency/transform_gate.py`; `src/okg/substrate/publisher/publisher.py`; `src/okg/substrate/mcp/source_lifecycle.py` | `tests/substrate/test_source_retirement.py`; `tests/substrate/test_derived_node_liveness.py`; `tests/substrate/test_derived_node_liveness_e2e.py` |

## Invariant Map

| Invariant/property | What it means | Code/test signal |
| --- | --- | --- |
| `TypeOK` | Every state value stays in its declared finite domain. | State-machine/checker failure; runtime enum and receipt-shape tests. |
| `UnrelatedContentPreserved` | Other sources and curated nodes survive retirement. | Exact-footprint and disposable node-delta assertions. |
| `PR1FrontierRefused` | PR1 cannot apply when the target reaches derived content. | Conservative-frontier refusal tests. |
| `StalePlanHasNoEffect` | Registry, branch, footprint, or policy drift refuses before mutation. | Stale-digest and continuation-plan tests. |
| `IdentityRefusalHasNoEffect` | Ambiguous identity, wrong deployment, shared ownership, and unsupported frontiers fail closed. | Negative identity/ownership/deployment tests. |
| `ProducerCollisionHasNoEffect` | A different producer cannot replace the current derived-node row or create a partial graph effect. | Fenced producer/node-id collision refinement test. |
| `ChunkReplayIdempotent` | Replaying a completed chunk cannot duplicate graph effects. | Chunk receipt and retry assertions. |
| `RepeatedApplyIdempotent` | Reapplying a published retirement preserves the same result. | Terminal receipt replay tests. |
| `TerminalReceiptComplete` | A terminal receipt exists only after every graph chunk landed. | Crash-before-terminal and resume tests. |
| `ReceiptBeforeRegistryDisable` | Registry authority remains active until the terminal receipt exists. | Registry removal fence tests. |
| `BlockedPublishInvisible` | A failed candidate never advances the visible generation. | Publisher binding and injected-failure tests. |
| `PublishedSupportSound` | Published derived content has valid live support. | Derived fixed-point unit and disposable publish tests. |
| `NoDanglingPublishedEdges` | Every published edge has two live endpoints. | Publication binding and disposable integrity checks. |
| `PublishedPointerAtomic` | A visible-generation advance implies a completed publish. | Publisher transaction tests. |
| `HistoricalSnapshotRetained` | The baseline generation is immutable. | Chronos pinned-history assertions. |
| `PinnedHistoryPreserved` | Retired content disappears from the new generation but remains in the pinned old generation. | Disposable before/after generation reads. |
| `AnyAndAllSemantics` | One-live-member support is distinct from all-members support. | Derived-support policy unit and disposable publish tests. |
| `MissingPolicyNeverDefaults` | Legacy derived provenance without an explicit policy fails closed. | Missing/malformed provenance refusal tests. |
| `RecoveryStateRemainsExplicit` | Every temporal step remains in an explicit recovery, refusal, active, or terminal phase. | TLC `PROPERTIES` check plus runtime interruption/resume tests. |

## Verification Process

Run the dedicated checker:

```sh
uv run --extra dev python scripts/check_formal_source_retirement_liveness.py \
  --out .scratch/formal/source-retirement-liveness
```

Run it through the aggregate gate-facing registry:

```sh
uv run --extra dev python scripts/run_formal_suite.py \
  --only check_formal_source_retirement_liveness
```

Both commands must report a fresh TLC pass and preserve module/config/checker
digests in `formal-report.yaml`.

## Model Boundary

The finite graph represents empty-frontier retirement plus representative
single-source, shared-source, chain, cycle, `any_member`, `all_members`, legacy,
and injected-orphan shapes. `Grounded` computes a bounded least support fixed
point. Canonical SHA-256 binding is modeled as exact record equality; collision
resistance is assumed. TLC proves safety and temporal state-space closure, not
eventual completion under real worker scheduling. PostgreSQL transactionality,
large-batch behavior, live publication deltas, Chronos retention, queue/DLQ
health, and doctor output remain runtime and disposable-deployment obligations.
