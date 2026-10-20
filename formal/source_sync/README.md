# Source Sync Formal Models

## System

The source-sync models cover the delta-first ingestion pipeline: source
frontiers, source delta identity, tombstones, dirty projection scopes, view
refresh scopes, execution lanes, readiness convergence, observability, and
dogfood cutover rollback.

## Design Contract

- Safety property: source-sync readiness and read visibility require complete
  frontier progress, faithful delta projection, safe execution lanes, measured
  observability, and rollback-safe dogfood cutover.
- Authority boundary: source-sync contracts and runtime workers own source
  progress; publisher/Chronos state owns read visibility.
- Valid runtime paths: explicit deltas, complete-scope record replacement,
  synthesized tombstones, dirty-scope projection, readiness recovery, lane
  serialization, measurement gaps, and cutover rollback.
- Out of scope: adapter-specific extraction correctness, DBOS internals, and
  live publish health remain runtime evidence.
- Preconditions/refinement: lane serialization is covered by
  `SourceExecutionLanes.tla`, report-property behavior is covered by
  `SourceSyncObservability.tla`, and complete-scope tombstone synthesis is
  bound to SourceRunner refinement tests. Any future lane or report-property
  missing formal coverage must be added to `formal/gap-inventory.toml` before
  it can be treated as an active gap.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `DeltaFrontier.tla` | `DeltaFrontier.cfg`, `DeltaFrontierStaleReadinessWitness.cfg`, `DeltaFrontierResumeSkipWitness.cfg` | Frontier, publish, and read-visibility safety over a MULTI-RUN machine: new deltas arrive at any point (resetting the per-run flags), crash-resume re-enters with durable flags intact, and two buggy-toggle witnesses refute the stale-readiness and resume-skip invariants by name. |
| `SourceDeltaContract.tla` | `SourceDeltaContract.cfg`, `SourceDeltaOmittedTombstoneWitness.cfg`, `SourceDeltaRawTombstoneWitness.cfg`, `SourceDeltaChurnOrderWitness.cfg` | The runner's emit pipeline as a state machine (raw-id diff, canonicalizing flush, retractions-before-inserts ordering, acceptance); three buggy-toggle witnesses refute tombstone completeness, canonical tombstone landing, and churn ordering by name. |
| `StableReemission.tla` | `StableReemission.cfg`, `StableReemissionWorktreeMembershipWitness.cfg`, `StableReemissionRunRevisionWitness.cfg`, `StableReemissionClobberWitness.cfg`, `StableReemissionNoBackendGuardWitness.cfg` | Stable committed-history output, explicit substitute ownership, and exact interval no-op safety; isolated witnesses refute worktree-dependent membership, execution-id revisions, substitute clobbering, and a missing backend guard. |
| `DeltaProjection.tla` | `DeltaProjection.cfg` | Dirty-scope projection and retraction soundness. |
| `ReadinessConvergence.tla` | `ReadinessConvergence.cfg` | Source-sync readiness stage ordering. |
| `ReadinessConvergenceLiveness.tla` | `ReadinessConvergenceLiveness.cfg`, `ReadinessConvergenceLivenessNoFairness.cfg` | Readiness LIVENESS over the REAL convergence machine (replacing the retired 5-state ReadinessLiveness toy): budgeted disruptions, WF on the completing run, SF on publish/exposure, proving eventual Ready-or-parked; the no-fairness config is refuted by TLC (tests/formal/test_readiness_liveness_witness.py). |
| `ReadinessConvergenceCounterexample.tla` | `ReadinessConvergenceReadyCounterexample.cfg` | Expected-failing ready witness. |
| `ReadinessConvergenceCounterexample.tla` | `ReadinessConvergenceStuckCounterexample.cfg` | Expected-failing stuck witness. |
| `ReadinessConvergenceCounterexample.tla` | `ReadinessConvergenceMcpBlockedCounterexample.cfg` | Expected-failing MCP-blocked witness. |
| `SourceExecutionLanes.tla` | `SourceExecutionLanes.cfg` | Lane isolation and serialized mutation safety. |
| `SourceSyncObservability.tla` | `SourceSyncObservability.cfg` | Measurement and severity semantics. |
| `DogfoodCutover.tla` | `DogfoodCutover.cfg` | Rollback and cutover readiness safety. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `DeltaFrontier.tla` | Source progress becomes a Chronos-applied frontier, then a published and read-visible frontier. | `src/okg/substrate/source_sync.py`, `src/okg/substrate/source_sync_contract.py`, `src/okg/substrate/publisher/publisher.py` | `tests/substrate/source_sync/test_frontier_model.py`, `tests/substrate/source_sync/test_frontier_resume.py` |
| `SourceDeltaContract.tla` | SourceRunner diffs RAW record-set ids (`_record_set_retractions`), synthesizes retraction facts, and the per-chunk flush canonicalizes ALL fact ids (`_canonicalize_facts`, op='R' included) with retractions flushed before the run's inserts — the mechanism pair that makes raw-id diffing safe. | `src/okg/substrate/ingest/runner.py`, `src/okg/substrate/source_sync_contract.py` | `tests/substrate/test_record_set_canonical_retractions.py`, `tests/substrate/source_sync/test_delta_batch_contract.py`, `tests/substrate/test_source_runner_delta_contract.py` |
| `StableReemission.tla` | Git-history derives facts from committed refs and explicit configuration; substitute nodes fill absence without taking foreign ownership; the graph and interval layers suppress exact replays after branch serialization. | `src/okg/substrate/library/sources/git_graph_history.py`, `src/okg/substrate/graph_store_internal/writes.py`, `src/chronos_core/branching/_interval_backend.py` | `tests/substrate/library/sources/test_git_graph_history.py`, `tests/substrate/test_chronos_graph_store.py`, `tests/substrate/test_git_history_reemission_e2e.py` |
| `DeltaProjection.tla` | Source deltas become projected outputs, retractions, lookup fallback decisions, and dirty downstream scopes. | `src/okg/substrate/source_sync_contract.py`, `src/okg/substrate/publisher/native_view_stage.py`, `src/okg/substrate/derived_artifacts/dirty_rules.py` | `tests/substrate/source_sync/test_delta_projection.py`, `tests/substrate/source_sync/test_dirty_scope_refresh.py` |
| `ReadinessConvergence.tla` | Completed work becomes fresh only after source freshness, published generation, and MCP visibility agree. | `src/okg/substrate/source_sync_convergence.py`, `src/okg/substrate/source_sync_producers.py`, `src/okg/substrate/source_sync_recovery.py` | `tests/substrate/test_source_sync_convergence_state.py`, `tests/substrate/test_source_sync_convergence_tracking.py`, `tests/substrate/source_sync/test_frontier_resume.py` |
| `ReadinessConvergenceCounterexample.tla` | Ready, stuck, and MCP-blocked witness states are intentionally reachable in negative configs. | `src/okg/substrate/source_sync_convergence.py`, `src/okg/substrate/source_sync_recovery.py` | `tests/substrate/test_source_sync_convergence_state.py`, `tests/substrate/test_source_sync_convergence_tracking.py`, `tests/substrate/source_sync/test_frontier_resume.py` |
| `ReadinessConvergenceLiveness.tla` | Under bounded disruption (StartIncompleteScope / stale recovery / manual / unsupported all reset or park readiness) and a fair worker (WF on the completing run, SF on publish and read-side exposure), a readiness-required source eventually reaches Ready or an operator-visible parked state — it cannot hover in converging/stuck/blocked forever. | `src/okg/substrate/source_sync_convergence.py`, `src/okg/substrate/source_sync_producers.py`, `src/okg/substrate/source_sync_recovery.py` | `tests/substrate/test_source_sync_liveness.py`, `tests/formal/test_readiness_liveness_witness.py` |
| `SourceExecutionLanes.tla` | Per-source adapter work runs in source lanes while admission/publish mutation boundaries stay serialized. | `src/okg/substrate/source_sync.py`, `src/okg/substrate/source_sync_enqueue.py`, `src/okg/substrate/runtime/worker.py` | `tests/substrate/runtime/test_source_sync_execution_lanes.py`, `tests/substrate/runtime/test_source_sync_workflow.py` |
| `SourceSyncObservability.tla` | Expected deltas, observed deltas, work amplification, queue state, and read visibility become readiness severity. | `src/okg/substrate/source_sync_producers.py`, `src/okg/substrate/source_sync_microbench.py`, `src/okg/substrate/source_sync_diagnostics.py` | `tests/substrate/source_sync/test_source_sync_measurements.py`, `tests/substrate/test_source_sync_liveness.py`, `tests/substrate/test_source_sync_diagnostics_telemetry.py` |
| `DogfoodCutover.tla` | A source-by-source dogfood cutover can advance, roll back, or fail while preserving readiness safety. | `src/okg/substrate/source_sync_recovery.py`, `src/okg/substrate/source_sync_producers.py`, `src/okg/substrate/cli/main.py` | `test_dogfood_readiness.py` in the development repository (not in this copy), `tests/substrate/test_dogfood_runtime_workflow_observability.py` |

## Invariant Map

| Invariants | What it means | Code/test signal |
| --- | --- | --- |
| `TypeOK` | Each source-sync model keeps its source ids, frontiers, deltas, dirty scopes, lanes, and readiness states well formed. | Model fixtures in the source-sync tests listed below. |
| `ReadinessRequiresPublishedChronos`, `PublishedRequiresChronos`, `WatermarkRequiresProgress`, `NoSilentWidening`, `NoReadyWithPendingUnappliedDelta`, `ReadVisibilityRequiresFullFrontier`, `PendingUnappliedDeltaNotReadVisible`, `ChronosNeverClaimsUnappliedDelta`, `DeltaFrontierContract` | A source cannot be ready or read-visible until progress is applied to Chronos, published, and no unapplied delta remains. | `tests/substrate/source_sync/test_frontier_model.py`, `tests/substrate/source_sync/test_frontier_resume.py`. |
| `TombstonesLandCanonical`, `RetractionsAreComplete`, `ChurnKeepsRecordLive`, `SourceDeltaContract` | Synthesized retractions land under canonical ids (a raw-id tombstone is a silent no-op — the audited hazard, now witnessed); every semantically-removed record has a tombstone by acceptance (carried by the full-prior-set diff, not an acceptance-time check); alias churn within a surviving record leaves the canonical node live (retractions flush before inserts). Each has a named buggy-toggle witness. | `src/okg/substrate/source_sync_contract.py`, `src/okg/substrate/ingest/runner.py`, `tests/substrate/source_sync/test_delta_batch_contract.py`, `tests/substrate/test_source_runner_delta_contract.py`, `tests/substrate/test_output_lineage.py`. |
| `StableSourceEmission`, `SubstitutePreservesAuthority`, `HistoryRestoresMissingSubstitute`, `DuplicateStableWakeupsDoNotVersion`, `StableReemission` | Fixed refs/config emit stable facts; explicit substitutes never take a foreign authoritative row and can restore absence; exact graph or direct interval replays do not create versions. Each guard category has an expected-failing witness. | `tests/substrate/library/sources/test_git_graph_history.py`, `tests/substrate/test_chronos_graph_store.py`, `tests/substrate/test_git_history_reemission_e2e.py`. |
| `ProjectionRetractionsAreFaithful`, `RetractionDoesNotLeaveLiveProvenance`, `LookupPreservesCoveredDirtyOutputs`, `DirtyScopesCoverChangedDependents`, `NoChangedDependentReused`, `UnavailableLookupForcesFullFallback`, `MixedUnavailablePreservesCoveredDirtyScopes`, `DirtyPlanAccountsForUniverse`, `DeltaProjectionContract` | Projected outputs reflect changed/retracted source ids, dirty scopes cover downstream dependents, and missing lookup coverage fails closed to full fallback. | `tests/substrate/source_sync/test_delta_projection.py`, `tests/substrate/source_sync/test_dirty_scope_refresh.py`. |
| `NoFalseReadyFromIncompleteWork`, `EventualReadyRequiresCompletedFreshnessAndPublishedGeneration`, `FrontierAdvanceResumeIsSafe`, `FreshWatermarkRequiresPublishAndMcpVisibility`, `McpVisibilityRequiresPublishedGeneration`, `NoHiddenStuckRecoveryWork`, `SourceSyncRecoveryBounded` | Source-sync readiness cannot hide incomplete work; fresh watermarks require publish and MCP visibility; recovery work remains visible and bounded until complete. | `tests/substrate/test_source_sync_convergence_state.py`, `tests/substrate/test_source_sync_convergence_tracking.py`, `tests/substrate/source_sync/test_frontier_resume.py`. |
| `ReadyWitnessNeverOccurs`, `StuckWitnessNeverOccurs`, `McpBlockedWitnessNeverOccurs` | Negative configs must find ready, stuck, and MCP-blocked witness states so convergence checks are not vacuous. | `scripts/check_formal_source_sync_convergence.py` and convergence witness tests. |
| `TypeOK` (in `ReadinessConvergenceLiveness.cfg`); temporal PROPERTY `EventuallyReadyOrParked` | The readiness LIVENESS theorem over the real convergence machine: under a bounded-disruption budget and a fair worker (WF on the completing run, SF on publish/exposure — their enablement is intermittent because a new run resets published), every trace eventually reaches Ready or an operator-visible parked state. The no-fairness spec is REFUTED by TLC, so the fairness conjunction is load-bearing. | `tests/substrate/test_source_sync_liveness.py`, `tests/substrate/test_source_sync_convergence_state.py`. |
| `LaneOwnerMatchesSource`, `SerializedMutationExclusive`, `SourceWritesStayInLane`, `PublishRequiresAdmission`, `SupersedeDoesNotPublishUnadmittedState`, `SourceExecutionLaneIsolation` | Source work stays in its source lane, serialized mutation boundaries remain exclusive, and superseded work cannot publish unadmitted state. | `tests/substrate/runtime/test_source_sync_execution_lanes.py`, `tests/substrate/runtime/test_source_sync_workflow.py`. |
| `AmplificationUsesMeasuredTerms`, `MeasurementGapsSurfaceAsWarningsOrErrors`, `SelfConfirmingExpectedDeltaNotReady`, `OverAmplificationWarnsOrBlocks`, `SourceSyncObservabilityContract` | Readiness severity must be based on measured terms; expected deltas must come from a pre-run explicit or harness-declared source; missing/self-confirming deltas and over-amplification warn or block. | `tests/substrate/source_sync/test_source_sync_measurements.py`, `tests/substrate/test_source_sync_liveness.py`, `tests/substrate/test_source_sync_diagnostics_telemetry.py`. |
| `RollbackPreservesFrontierAndProvenance`, `PartialRollbackNotReady`, `FailedRollbackNotReady`, `DogfoodReadinessReflectsCutoverState`, `DogfoodCutoverContract` | Rollback keeps frontier/provenance state intact, partial or failed rollback is not ready, and dogfood readiness reflects cutover state. | `test_dogfood_readiness.py` in the development repository (not in this copy), `tests/substrate/test_dogfood_runtime_workflow_observability.py`. |

## Verification Process

Use the dedicated wrappers:

```sh
uv run --extra dev python scripts/check_formal_delta_frontier.py
uv run --extra dev python scripts/check_formal_source_delta_contract.py
uv run --extra dev python scripts/check_formal_stable_reemission.py
uv run --extra dev python scripts/check_formal_delta_projection.py
uv run --extra dev python scripts/check_formal_source_sync_convergence.py
uv run --extra dev python scripts/check_formal_source_execution_lanes.py
uv run --extra dev python scripts/check_formal_source_sync_observability.py
uv run --extra dev python scripts/check_formal_dogfood_cutover.py
```

Each wrapper writes TLC logs and `formal-report.yaml` under `.scratch/formal/*`.
The convergence checker also runs expected-failing witness configs so the
readiness model cannot pass vacuously.

The readiness LIVENESS model is checked directly with TLC (its config
declares temporal `PROPERTIES`, not just safety `INVARIANTS`):

```sh
cd formal/source_sync && java -cp ../../.tools/tla2tools.jar tlc2.TLC \
  -config ReadinessConvergenceLiveness.cfg -deadlock -workers auto ReadinessConvergenceLiveness.tla
```

TLC reports "Implied-temporal checking" and then "Model checking completed.
No error has been found." The property is non-vacuous: dropping the
`WF_vars(Step)` fairness conjunct makes TLC refute `EventuallyReady` with a
behavior that stutters short of Ready, so the eventual-convergence claim
genuinely depends on fairness.

## Model Boundary

Implementation tests must exercise real source-sync delta extraction, frontier
resume, dirty-scope planning, view refresh metrics, source lane supersedure,
observability artifact validation, and dogfood rollback behavior. These models
do not prove the live dogfood cutover itself; final cutover still requires
published generation evidence, source health, dogfood readiness, doctor output,
queue/DLQ bounds, and MCP graph-read visibility.
