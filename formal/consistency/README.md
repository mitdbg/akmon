# Consistency Formal Models

## System

The consistency models cover two arms of the consistency publish path.

The **dirty-scope** models (`TransformGateDirtyScope`,
`TransformGateNodeSubtypeScope`) cover the incremental TransformConsistencyGate.
Their concern is whether dirty-node and dirty-edge scopes are sufficient to
validate a candidate generation without missing invalid post-state edges or
dirty nodes whose subtype is not in the active catalog.

The **qualification** model (`QualificationGate`) covers the finding-blocking
arm of `evaluate_consistency_publish_gate`. Its concern is whether the
declarative blocking policy (`never` / `new_findings` [+ severity floor] /
`any_open`) plus a deployment `blocks_publish` override honors Concord's
standing qualification predicate across consecutive publish attempts -- and it
exhibits the one production hazard that survived witnesses: under `new_findings`
a mandatory finding blocks exactly once and then publishes as backlog.

`ReusePass.tla` covers a second incremental machine in this folder: the
resolution-aware reuse pass (the differential core). Its concern is whether the
certificate-justified skips the reuse worklist takes can ever make the committed
resolved state diverge from a full recompute of the same delta — in particular
the least-witnessed section-9 assumption that a skip whose rerun would
dynamically create a new consumer step (a new check candidate) is refused.

`DbosCommitResume.tla` covers the third machine in this folder: the publish
commit/resume protocol under the DBOS workflow (the formal-strengthening
umbrella's coordination-dbos wave). Its concern is whether a crash at any
step boundary, followed by a workflow resume that honors durable
step-completion markers, can ever make a partial generation observable,
commit the same generation twice with different content, or strand durable
certificates referencing a generation whose checkpoint never committed.

`WatermarkRetry.tla` carries the folder's liveness arm: the per-item extractor
record-and-hold contract (a failed LLM call marks the run incomplete and the
publisher holds the enricher watermark) composed with the publisher's two skip
paths — the per-enricher skip-clean rung and the generation-level no-op-reuse
fast path. Its concern is whether a held item is ever stranded: the headline
temporal property `EveryItemEventuallyProcessed` says every failed-and-held
item is eventually retried and processed under weak fairness on publish
cycles, and the named counterexample removes the fast path's retry-state
consultation to reproduce the stranded-watermark lasso mechanically.

`EnricherBacklogDrain.tla` covers the distinct successful-watermark liveness
gap: an upstream stage emits a node fact after the publisher's phase-start
snapshot, so a successful downstream stage can remain one frontier behind even
after source traffic becomes quiescent. It checks that both generation-reuse
sites consult that lag until the finite acyclic chain drains.

## Design Contract

- Safety property: an incremental transform gate must reject any candidate that
  a full post-state validation would reject, or fall back to full validation.
- Authority boundary: `src/okg/substrate/consistency/transform_gate.py` owns the
  gate decision. `src/okg/substrate/publisher/publisher.py` owns the Chronos
  segment-delta query that produces dirty-scope evidence for that gate.
- Valid runtime paths: additive edges, dirty edges, dirty nodes, retractions,
  topology changes, narrowing violations, node-subtype violations, and
  full-fallback cases.
- Out of scope: live publish success and generation publication are not proven
  by this model.
- Preconditions/refinement: the model starts from dirty nodes/edges. The Chronos
  extraction step that produces those dirty scopes is checked by
  `tests/substrate/test_chronos_narrowing_gate.py::test_chronos_dirty_scope_extraction_covers_node_edge_changes`.
  If extraction is unavailable, the runtime must use full validation/full
  rebuild; that fallback is checked by
  `tests/substrate/test_chronos_narrowing_gate.py::test_chronos_dirty_scope_unavailable_forces_full_work`.
  Node-subtype validation uses the same dirty-node input: incremental mode checks
  every dirty node, while full fallback checks every live node.
- Gap inventory: no open consistency gap remains for dirty-scope extraction or
  node-subtype validation in `formal/gap-inventory.toml`.

The **qualification gate** (`QualificationGate.tla`) adds a finding-blocking
safety contract in this folder:

- Safety property: under `block_on=any_open` no generation publishes while a
  mandatory finding is open (Concord's standing qualification predicate, spec
  section 9.10). Only `any_open` implements it: under `new_findings` a mandatory
  finding blocks exactly once and then publishes as backlog (the one-shot
  ratchet), because a still-open unchanged finding emits no re-observed event.
- Authority boundary: `src/okg/substrate/consistency/gates.py` owns the
  `evaluate_consistency_publish_gate` blocking decision and the
  `finding_is_mandatory` section-9.10 facet; the model reasons over the same
  inputs (open findings, newly-opened findings, severity floor, override).
- Valid runtime paths: a finding absent, opened (a new observed event), still
  open without a re-observed event (bounded-event-growth), resolved, or
  reopened; crossed with `never` / `new_findings` [+ severity floor] /
  `any_open` and a deployment `blocks_publish` override (force / advisory /
  unset), across consecutive publish attempts where a blocked attempt retries.
- Out of scope: operational blocks (timeout / error / incomplete coverage),
  which the engine always treats as blocking regardless of policy, are not
  modeled -- this is the finding-blocking arm only.
- Preconditions/refinement: the bounded-event-growth fact (a still-open
  unchanged finding emits no re-observed event on a later run) is
  `_newly_opened_this_run` in `gates.py`, exercised by
  `tests/substrate/test_consistency_gates.py::test_backlog_has_zero_newly_opened_and_resolution_is_counted`.

The **reuse pass** (`ReusePass.tla`) adds a second safety contract in this
folder:

- Safety property: after the reuse worklist drains to a fixed point and the
  generation commits, the incrementally maintained resolved state equals the
  full-recompute resolved state for the same delta (section 9.7).
- Authority boundary: `src/okg/substrate/consistency/reuse_scheduler.py` owns
  the skip ladder and worklist; `src/okg/substrate/consistency/reuse_certificates.py`
  owns certificate validity, evaluated per dependency class at decision time.
- Valid runtime paths: valid-prior-certificate skip, degenerate unchanged-inputs
  skip, bounded-dominance skip, run, and dynamic worklist growth when a run
  discovers a new consumer.
- Out of scope: the bounded-dominance authority arithmetic and the
  discard/abort fallbacks are not proven by this model (see Model Boundary).
- Preconditions/refinement: the model starts from a delta and abstracts the ten
  `DEPENDENCY_CLASSES` to three representative ones; the ladder and commit gate
  are exercised by the scheduler tests in
  `tests/substrate/consistency/test_reuse_scheduler.py`.

The **commit/resume protocol** (`DbosCommitResume.tla`) adds a crash-atomicity
contract in this folder:

- Safety property: across every crash/resume interleaving, the visible
  generation pointer only ever reaches a fully-committed generation; no
  generation is committed twice with different content; and no durable
  certificate references a generation whose checkpoint never committed.
- Authority boundary: `src/okg/substrate/runtime/reuse_pass_workflow.py` owns
  the commit protocol (`commit_maintenance_state`: certificates atomic with
  the generation checkpoint in one transaction; the pass registered as a DBOS
  step); `src/okg/substrate/publisher/publisher.py` owns the chronos-native
  cycle that makes a generation visible only at its checkpoint.
- Valid runtime paths: a clean run to the checkpoint; a crash at any step
  boundary with the open transaction lost; resume re-executing incomplete
  steps and replaying marker-complete ones; an idempotent replay of an
  already-committed cycle; and a second publish cycle as a fresh workflow
  instance.
- Out of scope: the reuse-pass fallback ladder (discard / abort), the
  blocked-publish path, and the gate decision itself (the sibling models and
  the runtime tests own those); concurrent writers (the publisher is
  single-writer by lease).
- Preconditions/refinement: the pass is deterministic from its inputs
  (byte-equal `ReusePassResult` digest on re-execution), exercised by
  `tests/substrate/consistency/test_reuse_scheduler.py::test_crash_mid_update_resumes_or_discards`;
  the live-DBOS singleton wiring that must satisfy the
  marker-no-earlier-than-effects constraint is `task.reuse.live-proof` scope.

The **watermark retry** (`WatermarkRetry.tla`) adds the folder's liveness
contract:

- Safety property: a held (failed) status always stands for concrete
  unprocessed inputs (`HeldWatermarkImpliesPendingInput`); the headline
  obligation is the liveness (temporal) property
  `EveryItemEventuallyProcessed` — under weak fairness on publish cycles,
  with the generation-level no-op-reuse fast path consulting enricher
  `last_status` and LLM failure transient, every item whose call failed and
  whose watermark was held is eventually retried and processed.
- Authority boundary: `src/okg/substrate/library/enrichers/extraction/base.py`
  owns the per-item record-and-hold contract (`_record_input_failure` /
  `_complete_tracked` / `_keyed_complete`; a non-empty failure list marks the
  run incomplete). `src/okg/substrate/publisher/publisher.py` owns both skip
  decisions: `_should_skip_clean_enricher` (refuses a clean skip whenever
  `last_status != 'success'`) and the pre-enrichment no-op-reuse probe whose
  `_enricher_retry_pending_name` arm invalidates generation reuse while any
  wired enricher's `last_status != 'success'`.
- Valid runtime paths: per-item LLM success and failure with record-and-hold;
  watermark hold and advance; skip-clean refusal on a non-success status;
  the generation-level no-op-reuse skip on source-quiescent cycles (guarded
  and, in the counterexample, unguarded); source churn and permanent
  quiescence.
- Out of scope: the reuse closure's other invalidation arms (catalog version,
  lenses, nomos pin, gate inputs, native views, extractor signature),
  permanently failing (poison) items, and replay-mode exclusions (see Model
  Boundary).
- Preconditions/refinement: the record-and-hold contract is exercised by
  `tests/substrate/test_enricher_input_failure_holds_watermark.py` and
  `tests/substrate/extraction/test_extractor_llm_failure_watermark.py`; the
  generation-level guard by
  `tests/substrate/runtime/test_publish_commit_protocol.py::test_p6_held_enricher_retry_invalidates_reuse`.

The **successful-watermark backlog** (`EnricherBacklogDrain.tla`) adds the
complementary liveness contract for enrichers whose last run succeeded:

- Safety property: neither generation-reuse site may return while a successful
  enricher has an eligible node fact above its recorded fact watermark.
- Liveness property: with finite input, an acyclic bounded enricher chain, and
  weakly fair publish cycles, every eligible input is eventually processed.
- Authority boundary: `src/okg/substrate/publisher/publisher.py` owns the
  phase-start frontier, progress advancement, legacy node-fact dirty selector,
  and both generation-reuse decisions.
- Valid runtime paths: upstream same-cycle node emission, downstream
  next-cycle consumption, pre-enrichment reuse, post-enrichment reuse,
  matching retractions, subtype filtering, and retention-stale progress.
- Out of scope: edge-only prior-segment backlog, because the current
  incremental context cannot reconstruct historical edge facts.
- Preconditions/refinement:
  `tests/substrate/test_publisher.py::test_pending_enricher_backlog_prevents_noop_reuse_until_dag_drains`
  and
  `tests/substrate/test_publisher.py::test_pending_enricher_backlog_prevents_post_enrichment_noop_reuse`
  exercise the two runtime guard sites.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `TypedOutcomeContract.tla` | `TypedOutcomeContract.cfg` | Exhaustive finite refinement of the five typed consistency outcomes: semantic alignment uncertainty, applicability uncertainty, unresolved comparability, actionable conflict, and no conflict. The model proves unknown semantic dimensions never become actionable, applicability uncertainty stays distinct, unresolved keys are not comparable, and the legacy definite/possible adapter is explicit. |
| `TypedApplicability.tla` | `TypedApplicability.cfg` | Exhaustive finite model of the single typed applicability outcome table and v1 heading adapter: unknown never becomes definite, disjoint upper bounds never become findings, partial overlap stays possible, and explicit unboundedness remains definite-capable. |
| `TransformGateDirtyScope.tla` | `TransformGateDirtyScope.cfg` | Dirty-scope soundness for incremental transform gate evaluation over the full existing universe, with one endpoint-invalid edge independently ranging source-only, destination-only, or both missing. |
| `TransformGateDirtyScope.tla` | `TransformGateDirtyScopeSmoke.cfg` | CI/operator smoke profile with bounded dirty-scope cardinalities and fallback reasons. |
| `TransformGateDirtyScope.tla` | `TransformGateDirtyScopeCounterexample.cfg` | Buggy-toggle witness: `BuggyTrustsUnsoundDelta = TRUE` relaxes `DeltaEvidenceSound` so an invalid post-edge outside the dirty scope publishes without blocking, refuting `NoInvalidPostEdgePublishes` by name. |
| `TransformGateDirtyScope.tla` | `TransformGateMissingSourceCounterexample.cfg` | Issue #1151 witness: `BuggyChecksOnlyDestination = TRUE` sees a source-only missing endpoint as clean and refutes `MissingSourceEndpointCannotPublish` by name. |
| `TransformGateNodeSubtypeScope.tla` | `TransformGateNodeSubtypeScope.cfg` | Dirty-node subtype scope soundness for incremental transform gate evaluation. |
| `TransformGateNodeSubtypeScope.tla` | `TransformGateNodeSubtypeScopeCounterexample.cfg` | Buggy-toggle witness: `BuggyTrustsUnsoundDelta = TRUE` relaxes `DeltaEvidenceSound` so an invalid-subtype node outside the dirty scope publishes without blocking, refuting `NoInvalidPostNodePublishes` by name. |
| `QualificationGate.tla` | `QualificationGateSmoke.cfg` | Sanity profile (`block_on=never`, override ranging): the never policy never blocks on findings, force always blocks, advisory always publishes. |
| `QualificationGate.tla` | `QualificationGateStanding.cfg` | Standing-qualification profile (`block_on=any_open`): `NoPublishWhileMandatoryOpen` holds exhaustively over the finding lifecycle x override x mixed-severity scope. |
| `QualificationGate.tla` | `QualificationGateOneShotExhibit.cfg` | Exhibit (checks-as-violation): under `block_on=new_findings` a mandatory finding blocks once and then publishes over itself as backlog, refuting `NoPublishWhileMandatoryOpen` by name -- the one-shot-mandatory ratchet. |
| `QualificationGate.tla` | `QualificationGateStandingCounterexample.cfg` | Buggy-toggle witness: `BuggyConsultsNewlyOpenedForStanding = TRUE` makes an `any_open` gate consult the newly-opened count instead of the open count, degrading it into the one-shot ratchet, refuting `NoPublishWhileMandatoryOpen` by name. |
| `ReusePass.tla` | `ReusePass.cfg` | Exhaustive small-scope positive model of the reuse pass (850 distinct states): the committed resolved state equals the full recompute for every delta. This IS the smoke model — the state space is small enough for full TLC exhaustion. |
| `ReusePass.tla` | `ReusePassDynamicConsumerCounterexample.cfg` | Buggy-toggle witness: `GuardDynamicConsumer = FALSE` lets a step whose run would spawn a new consumer be skipped, so the consumer is omitted, refuting `NoOmittedConsumer` by name. |
| `ReusePass.tla` | `ReusePassStaleCertCounterexample.cfg` | Buggy-toggle witness: `CertIgnoresStaleClass = TRUE` accepts a certificate while one recorded dependency class has changed, carrying a stale output forward, refuting `SkippedOutputsUnchanged` by name. |
| `DbosCommitResume.tla` | `DbosCommitResume.cfg` | Exhaustive small-scope positive model of the publish commit/resume protocol (2944 distinct states): two publish cycles, per-cycle delta inputs ranged, a crash at every step boundary, resume honoring durable step markers. This IS the smoke model. |
| `DbosCommitResume.tla` | `DbosCommitResumeMarkerFirstCounterexample.cfg` | Buggy-toggle witness: `MarkerBeforeEffects = TRUE` writes the reuse step's durability marker before its effects are durable, so resume skips the step and the commit checkpoints a generation without its certificates, refuting `NoPartialGenerationObservable` by name. |
| `DbosCommitResume.tla` | `DbosCommitResumeSplitCommitCounterexample.cfg` | Buggy-toggle witness: `SplitCommitTxn = TRUE` lands the commit set in two transactions (certificates first, checkpoint second); a crash between them strands durable certificates for an uncommitted generation, refuting `NoCertWithoutCheckpoint` by name. |
| `DbosCommitResume.tla` | `DbosCommitResumeNonIdempotentCounterexample.cfg` | Buggy-toggle witness: `NonIdempotentEffects = TRUE` makes the reuse step write durably outside the commit transaction with attempt-derived content; crash and resume re-execute it, committing the same generation twice with different content, refuting `NoDoubleCommit` by name. |
| `WatermarkRetry.tla` | `WatermarkRetry.cfg` | Positive liveness model (19 distinct states, complete state space): with the generation-level guard on (`GuardConsultsRetryState = TRUE`, the P6 engine), TLC checks the temporal property `EveryItemEventuallyProcessed` under weak fairness on publish cycles. This IS the smoke model. |
| `WatermarkRetry.tla` | `WatermarkRetryStrandedCounterexample.cfg` | Buggy-toggle witness: `GuardConsultsRetryState = FALSE` (the pre-P6 defect) makes the no-op-reuse fast path skip the enrichers stage on quiescence alone; TLC refutes `EveryItemEventuallyProcessed` under the same fair spec with the stranded-watermark lasso (a temporal violation, checked in-process by the checker). |
| `WatermarkQuarantine.tla` | `WatermarkQuarantine.cfg` | Positive bounded-retry model: item/signature failures consume at most the authored retry limit, terminal quarantine retains prior output, and record/snapshot/progress release is atomic. TLC also checks `EveryProcessableItemEventuallyTerminal` under fair publish cycles. |
| `WatermarkQuarantine.tla` | `WatermarkQuarantineCrash.cfg` | Crash-enabled positive model: failures before completion or terminal commit roll the working claim and terminal preparation back together. |
| `WatermarkQuarantine.tla` | `WatermarkQuarantineSystemicHeld.cfg` | Positive systemic-failure model: an outage releases its working reservation, spends no item attempts, creates no quarantine, and keeps `failed:%` progress held. |
| `WatermarkQuarantine.tla` | `WatermarkQuarantineSystemicFailure.cfg` | Named unsafe-classification witness: treating a systemic outage as an item failure eventually quarantines corpus input and refutes `SystemicFailureMisclassificationWitness`. |
| `WatermarkQuarantine.tla` | `WatermarkQuarantineSnapshotLoss.cfg` | Named unsafe-snapshot witness: omitting terminally quarantined prior output retracts it and refutes `SnapshotOmissionRetractionWitness`. |
| `EnricherBacklogDrain.tla` | `EnricherBacklogDrain.cfg` | Positive finite two-stage liveness model: both reuse sites consult successful fact-watermark lag, so eligible backlog drains. |
| `EnricherBacklogDrain.tla` | `EnricherBacklogDrainPreReuseStrandedCounterexample.cfg` | Pre-reuse guard-off temporal witness: source quiescence strands the downstream stage forever. |
| `EnricherBacklogDrain.tla` | `EnricherBacklogDrainPostReuseUnsafeCounterexample.cfg` | Post-reuse guard-off safety witness: graph-idempotent upstream output creates lag after the phase snapshot and unsafe final reuse violates `ReuseOnlyWhenNoEligibleBacklog`. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `TypedApplicability.tla` | Effective claims normalize v1 or v2 applicability once; candidate conflict pairs are classified by the same conjunctive bounds/heading/time/discrete-axis operator before findings and component-scoped resolution. | `src/okg/substrate/consistency/claim_model/applicability.py`, `src/okg/substrate/consistency/claim_model/conflicts.py`, `src/okg/substrate/derived_artifacts/claim_projection_derivation.py` | `tests/substrate/consistency/test_typed_applicability.py`, `tests/substrate/test_claim_projection_derivation.py::test_typed_publish_exercises_active_time_activity_and_conflict_path` |
| `TypedOutcomeContract.tla` | A candidate pair is refined into an explicit typed category and structured established/unknown dimension evidence before it is projected. The compatibility adapter is applied only at the persistence boundary. | `src/okg/substrate/consistency/claim_model/conflicts.py`, `src/okg/substrate/derived_artifacts/claim_projection_derivation.py` | `tests/substrate/consistency/test_consistency_outcome_contract.py`, `tests/substrate/test_claim_projection_derivation.py` |
| `TransformGateDirtyScope.tla` | The publisher reads the Chronos segment delta and returns dirty nodes/edges. The gate then chooses scoped validation or full validation. | `src/okg/substrate/publisher/publisher.py`, `src/okg/substrate/consistency/transform_gate.py` | `tests/substrate/test_chronos_narrowing_gate.py::test_chronos_dirty_scope_extraction_covers_node_edge_changes`, `tests/substrate/test_chronos_narrowing_gate.py::test_chronos_dirty_scope_unavailable_forces_full_work` |
| `TransformGateDirtyScope.tla` | Once runtime scope evidence is sound, candidate edges are checked for both missing source and destination endpoints, catalog narrowing, retractions, and topology changes before publish can pass. Runtime database tests separately prove that a clean v3 full basis and exact previous-live/current-absent deletion-marker set are required before scoped evaluation. | `src/okg/substrate/consistency/gates.py`, `src/okg/substrate/consistency/transform_gate.py`, `src/okg/substrate/transform/consistency.py` | `tests/substrate/test_chronos_narrowing_gate.py::test_chunk_retraction_cannot_publish_dangling_enricher_citation`, `tests/substrate/transform/test_chronos_ir_seam.py`, `tests/pact/test_formal_report_producers.py` |
| `TransformGateDirtyScope.tla` | The smoke profile constrains dirty-scope cardinalities while retaining dirty edge, incident topology, additive node, retraction, and fallback cases for CI/operator freshness accounting. | `src/okg/substrate/consistency/transform_gate.py` | `tests/pact/test_formal_report_producers.py::test_expensive_tlc_smoke_profiles_emit_profile_metadata` |
| `TransformGateNodeSubtypeScope.tla` | Dirty nodes are checked against the active catalog subtype set. Incremental mode checks all dirty nodes; full fallback checks all live nodes. | `src/okg/substrate/consistency/transform_gate.py`, `src/okg/substrate/transform/consistency.py` | `tests/substrate/test_chronos_narrowing_gate.py::test_chronos_gate_blocks_node_with_unknown_subtype`, `tests/substrate/test_chronos_narrowing_gate.py::test_chronos_gate_passes_known_node_subtype`, `tests/substrate/test_fork_publish.py::test_fork_first_publish_blocks_unknown_node_subtype`, `tests/substrate/test_fork_publish.py::test_fork_first_publish_blocks_retyped_inherited_node_to_unknown_subtype` |
| `QualificationGate.tla` | The gate derives the finding-blocking decision from the open findings, the findings this run opened, the severity floor, and the deployment override; a blocked attempt retries on the next cycle. | `src/okg/substrate/consistency/gates.py` | `tests/substrate/test_consistency_gates.py::test_block_on_any_open_reproduces_fail_and_block_byte_for_byte`, `tests/substrate/test_consistency_gates.py::test_block_on_new_findings_passes_pre_existing_backlog`, `tests/substrate/test_consistency_gates.py::test_block_on_never_does_not_block_open_findings` |
| `QualificationGate.tla` | The section-9.10 mandatory facet is derived from the same inputs; a still-open unchanged finding emits no re-observed event, so under `new_findings` it stops being mandatory (the one-shot ratchet), while `any_open` keeps it mandatory. | `src/okg/substrate/consistency/gates.py` | `tests/substrate/consistency/test_qualification_mapping.py::test_finding_is_mandatory_truth_table`, `tests/substrate/consistency/test_qualification_mapping.py::test_backlog_under_new_findings_is_not_mandatory_and_does_not_block` |
| `ReusePass.tla` | The reuse worklist processes each reached step through the skip ladder to a fixed point; a run extends the worklist with discovered dependents; commit happens only at the fixed point. | `src/okg/substrate/consistency/reuse_scheduler.py`, `src/okg/substrate/consistency/reuse_certificates.py` | `tests/substrate/consistency/test_reuse_scheduler.py::test_scheduler_governed_publish_equals_complete_reference`, `tests/substrate/consistency/test_reuse_scheduler.py::test_mixed_run_skip_fixed_point_every_outcome_cause_recorded`, `tests/substrate/consistency/test_reuse_scheduler.py::test_commit_refused_before_fixed_point` |
| `ReusePass.tla` | A skip is admitted only behind a certificate whose recorded dependency classes all match current facts; a step whose run would create a new consumer must run. | `src/okg/substrate/consistency/reuse_certificates.py`, `src/okg/substrate/consistency/reuse_scheduler.py` | `tests/substrate/consistency/test_reuse_certificates.py::test_each_dependency_class_invalidates`, `tests/substrate/consistency/test_reuse_scheduler.py::test_no_cone_step_skips_without_certificate`, `tests/substrate/consistency/test_reuse_scheduler.py::test_valid_prior_certificate_skip` |
| `DbosCommitResume.tla` | The reuse pass runs as a DBOS step recording tentative certificates into the open transaction; `commit_maintenance_state` lands certificates and the generation checkpoint with a single `conn.commit()`, and a crash before it leaves neither. | `src/okg/substrate/runtime/reuse_pass_workflow.py` | `tests/substrate/consistency/test_reuse_scheduler.py::test_crash_mid_update_resumes_or_discards`, `tests/substrate/consistency/test_reuse_scheduler.py::test_reuse_pass_registers_as_a_dbos_step` |
| `DbosCommitResume.tla` | The chronos-native publish cycle makes a generation visible only at its checkpoint; an aborted or discarded pass rolls back and leaves no certificate rows for the generation. | `src/okg/substrate/publisher/publisher.py`, `src/okg/substrate/runtime/reuse_pass_workflow.py` | `tests/substrate/consistency/test_reuse_scheduler.py::test_operator_failure_aborts_leaving_prior_state_intact`, `tests/substrate/consistency/test_reuse_scheduler.py::test_discarded_pass_writes_zero_certificates`, `tests/substrate/consistency/test_reuse_scheduler.py::test_commit_refused_before_fixed_point` |
| `WatermarkRetry.tla` | A failed per-item LLM call records an input failure, the run comes back incomplete, the publisher stamps `last_status = 'failed:...'` and holds the enricher watermark; the failed items stay pending and a later run's success advances the watermark. | `src/okg/substrate/library/enrichers/extraction/base.py`, `src/okg/substrate/publisher/publisher.py` | `tests/substrate/test_enricher_input_failure_holds_watermark.py::test_incomplete_result_holds_watermark_but_admits_good_outputs`, `tests/substrate/test_enricher_input_failure_holds_watermark.py::test_complete_result_advances_watermark`, `tests/substrate/extraction/test_extractor_llm_failure_watermark.py::test_llm_failure_marks_run_incomplete_but_keeps_good_outputs` |
| `WatermarkRetry.tla` | On a source-quiescent cycle the publisher's pre-enrichment no-op-reuse probe may return the previous generation with `enrichers` in `skipped_stages`; the guarded probe first invalidates reuse (`enricher_retry_pending`) while any wired enricher's `last_status != 'success'`, and the per-enricher skip-clean rung refuses a clean skip on a non-success status so the held items get a full re-read. | `src/okg/substrate/publisher/publisher.py` | `tests/substrate/runtime/test_publish_commit_protocol.py::test_p6_held_enricher_retry_invalidates_reuse`, `tests/substrate/runtime/test_publish_commit_protocol.py::test_p6_never_run_enricher_is_pending`, `tests/substrate/test_enricher_input_failure_holds_watermark.py::test_incomplete_status_forces_rerun_success_status_skips` |
| `WatermarkQuarantine.tla` | A model-backed extractor claims each item/signature inside the publisher transaction, counts only trusted deterministic item failures, activates quarantine at the pinned limit, carries prior tagged output into the complete terminal snapshot, and advances progress in the same commit. Systemic failures release the working claim without spending the item budget. | `src/okg/substrate/enrichers/input_quarantine.py`, `src/okg/substrate/library/enrichers/extraction/base.py`, `src/okg/substrate/publisher/publisher.py` | `tests/substrate/extraction/test_enricher_input_quarantine.py`, `tests/substrate/test_enricher_input_failure_holds_watermark.py` |
| `EnricherBacklogDrain.tla` | The publisher captures one node-fact frontier at phase start; upstream output after that snapshot is eligible for a downstream stage only on a later publish cycle, and both generation-reuse sites consult successful watermark lag until it is consumed. | `src/okg/substrate/publisher/publisher.py` | `tests/substrate/test_publisher.py::test_pending_enricher_backlog_prevents_noop_reuse_until_dag_drains`, `tests/substrate/test_publisher.py::test_pending_enricher_backlog_prevents_post_enrichment_noop_reuse` |

## Invariant Map

| Invariants | What it means | Code/test signal |
| --- | --- | --- |
| `TypeOK`, `UnknownNeverDefinite`, `UpperDisjointNeverFinds`, `LowerDisjointUpperOverlapIsPossible`, `UnboundedKnownIsDefiniteCapable`, `ConjunctiveAcrossAxes`, `V1AdapterEquivalent`, `UnboundedWitnessExists`, `TypedApplicabilitySafety` | The runtime outcome cannot overstate uncertain applicability, cannot emit a finding across provably disjoint upper bounds, preserves every legacy heading case through the adapter, and includes a concrete explicit-unbounded definite witness. | `scripts/check_formal_typed_applicability.py` and `tests/substrate/consistency/test_typed_applicability.py::test_exhaustive_finite_model_truth_table_and_symmetry`. |
| `TypeOK`, `UnknownSemanticNeverConflict`, `UnknownApplicabilityDistinct`, `UnresolvedKeyNotComparable`, `ConflictRequiresProof`, `AdapterExplicit`, `TypedOutcomeSafety` | The five-category contract is exhaustive and actionable only for proven conflicts; semantic uncertainty, applicability uncertainty, and non-comparability remain typed and carry dimension evidence. | `scripts/check_formal_typed_outcome.py` and `tests/substrate/consistency/test_consistency_outcome_contract.py`. |
| `TypeOK` | Graph deltas, dirty scopes, candidates, and fallback states stay inside the modeled shape. | Gate setup in `tests/substrate/test_chronos_narrowing_gate.py`. |
| `RetractionsDoNotSurvive` | Retracted edges are removed from the post-state. | Retraction cases in `tests/substrate/test_chronos_narrowing_gate.py`. |
| `CandidateSelectionMatchesDirtyScope`, `TopologyDirtyIncidentEdgesInScope`, `AdditiveNodesDoNotExpandIncidentEdges` | Scoped validation checks exactly the edges affected by dirty nodes/edges and does not widen because a new node exists. | Dirty-scope extraction and scoped-gate tests in `tests/substrate/test_chronos_narrowing_gate.py`. |
| `PreviousCleanDeltaCoversInvalidEdges`, `InvalidDirtyEdgeCannotPublish`, `NoInvalidPostEdgePublishes` | Invalid post-state edges are either in scope and blocked or force full validation; they cannot publish silently. | Narrowing violation tests in `tests/substrate/transform/test_chronos_ir_seam.py`. |
| `MissingSourceEndpointCannotPublish`, `MissingDestinationEndpointCannotPublish` | A candidate edge with either absent endpoint cannot be reported clean; the negative witness reproduces the historical destination-only evaluator. | `tests/substrate/test_chronos_narrowing_gate.py::test_chunk_retraction_cannot_publish_dangling_enricher_citation`. |
| `CandidateNodeSelectionMatchesDirtyScope`, `PreviousCleanDeltaCoversInvalidNodes` | A node with an invalid subtype is in the checked node set: dirty nodes for incremental mode, all nodes for fallback mode. | Node-subtype full and scoped tests in `tests/substrate/test_chronos_narrowing_gate.py` and `tests/substrate/test_fork_publish.py`. |
| `InvalidDirtyNodeCannotPublish`, `NoInvalidPostNodePublishes` | A dirty node with a subtype missing from the active catalog blocks publish. | `tests/substrate/test_chronos_narrowing_gate.py::test_chronos_gate_blocks_node_with_unknown_subtype` and fork publish node-subtype tests. |
| `UnsafeDirtyScopeFallsBackFull` | Missing or unsafe dirty-scope data must choose full validation, not a partial pass. | Fallback cases in `tests/substrate/test_chronos_narrowing_gate.py`. |
| `CleanPostEdgesPass` | A post-state whose checked edges all satisfy narrowing passes the dirty-edge part of the gate. | Known-good narrowing tests in `tests/substrate/transform/test_chronos_ir_seam.py` and dirty-scope setup in `tests/substrate/test_chronos_narrowing_gate.py`. |
| `CleanPostNodesPass` | A post-state with no invalid node subtypes passes the node-subtype part of the gate. | Known-subtype tests in `tests/substrate/test_chronos_narrowing_gate.py`. |
| `TransformGateDirtyScopeSoundness` | The incremental gate is sound with respect to the full post-state check. | Formal report producer coverage in `tests/pact/test_formal_report_producers.py`. |
| `TransformGateNodeSubtypeScopeSoundness` | The incremental node-subtype check is sound with respect to the full node-subtype check. | Formal report producer coverage in `tests/pact/test_formal_report_producers.py`. |
| `NoPublishWhileMandatoryOpen` | No generation publishes while a mandatory (floored-open) finding stands. Holds under `any_open` (the standing predicate); the `QualificationGateOneShotExhibit` and `QualificationGateStandingCounterexample` configs refute it under `new_findings` and under the wrong-count bug. | `tests/substrate/consistency/test_qualification_mapping.py::test_open_finding_on_blocking_check_is_mandatory_and_blocks`, `tests/substrate/test_consistency_gates.py::test_block_on_new_findings_passes_pre_existing_backlog`. |
| `NeverPolicyNeverBlocksOnFindings` | Under `never` (no override) the finding arm never blocks; only operational results would, which are out of scope. | `tests/substrate/test_consistency_gates.py::test_block_on_never_does_not_block_open_findings`. |
| `OverrideForceBlocksWhenDecided`, `OverrideAdvisoryPublishesWhenDecided` | A deployment `blocks_publish` override wins both ways: force blocks every decided attempt, advisory publishes every decided attempt. | `tests/substrate/consistency/test_qualification_mapping.py::test_finding_is_mandatory_truth_table`. |
| `ResolvedStateEquality` | After the reuse worklist drains and the generation commits, the incrementally maintained resolved state equals the full-recompute resolved state for the same delta. | `tests/substrate/consistency/test_reuse_scheduler.py::test_scheduler_governed_publish_equals_complete_reference`. |
| `NoOmittedConsumer` | A step whose run would dynamically create a new consumer cannot be skipped, so the new consumer's contribution is never omitted (the `reuse-pass-dynamic-consumer` witness in `scripts/run_formal_suite.py` refutes it when the guard is off). | `tests/substrate/consistency/test_reuse_scheduler.py::test_mixed_run_skip_fixed_point_every_outcome_cause_recorded`. |
| `SkippedOutputsUnchanged` | A skipped step's carried-forward output equals its fresh output — a certificate is accepted only when every recorded dependency class matches current facts (the `reuse-pass-stale-cert` witness refutes it when one class is dropped). | `tests/substrate/consistency/test_reuse_certificates.py::test_each_dependency_class_invalidates`, `tests/substrate/consistency/test_reuse_scheduler.py::test_policy_and_precedence_fingerprint_changes_force_the_step_to_run`. |
| `ReusePassSoundness` | The conjunction: the reuse pass is sound with respect to the full recompute of the same delta. | Formal report producer coverage in `tests/pact/test_formal_report_producers.py`. |
| `NoPartialGenerationObservable` | The visible generation pointer only ever reaches a generation whose full commit set (projection, certificates, claims, checkpoint) is durable (the `dbos-commit-marker-first` witness in `scripts/run_formal_suite.py` refutes it when a step's marker is written before its effects are durable). | `tests/substrate/consistency/test_reuse_scheduler.py::test_crash_mid_update_resumes_or_discards`. |
| `NoDoubleCommit` | Resume never commits the same generation twice with different content: effects are transactional (a lost attempt leaves nothing durable) and input-deterministic (re-execution reproduces the same bytes) — the `dbos-commit-non-idempotent` witness refutes it when a step writes attempt-derived content outside the transaction. | `tests/substrate/consistency/test_reuse_scheduler.py::test_crash_mid_update_resumes_or_discards`. |
| `NoCertWithoutCheckpoint` | Durable certificate rows exist only for generations whose checkpoint committed — the one-transaction commit boundary (the `dbos-commit-split-txn` witness refutes it when the commit set lands in two transactions). | `tests/substrate/consistency/test_reuse_scheduler.py::test_discarded_pass_writes_zero_certificates`, `tests/substrate/consistency/test_reuse_scheduler.py::test_operator_failure_aborts_leaving_prior_state_intact`. |
| `DbosCommitResumeSoundness` | The conjunction: the publish commit/resume protocol is crash-atomic under DBOS resume semantics. | Formal report producer coverage in `tests/pact/test_formal_report_producers.py`. |
| `HeldWatermarkImpliesPendingInput` | Record-and-hold is honest: a `failed` status always stands for concrete unprocessed inputs — the watermark is never held over nothing. | `tests/substrate/test_enricher_input_failure_holds_watermark.py::test_run_is_complete_predicate`, `tests/substrate/extraction/test_extractor_llm_failure_watermark.py::test_every_item_failing_yields_zero_edges_and_incomplete`. |
| `EveryItemEventuallyProcessed` (temporal property, not an invariant) | Eventual retry: under weak fairness on publish cycles and transient failure, every failed-and-held item is eventually processed. Holds with the generation-level guard on; the `WatermarkRetryStrandedCounterexample` config refutes it with the guard off (the stranded-watermark lasso — the pre-P6 dev defect class). | `tests/substrate/runtime/test_publish_commit_protocol.py::test_p6_held_enricher_retry_invalidates_reuse`, `tests/substrate/test_enricher_input_failure_holds_watermark.py::test_incomplete_status_forces_rerun_success_status_skips`. |
| `DispatchBudgetBound`, `NoPrematureQuarantine`, `QuarantineHasDurableRecord`, `ActiveRecordBackedByHistory` | Each item/signature consumes at most its pinned limit, cannot become active early, and has durable active/history state when terminal. | `tests/substrate/extraction/test_enricher_input_quarantine.py::test_real_publish_quarantines_poison_retains_output_and_advances`, `tests/substrate/extraction/test_enricher_input_quarantine.py::test_duplicate_item_signature_dispatches_and_consumes_once`. |
| `ChangedSignatureKeepsHistory`, `OldFindingActiveDuringChangedRetry`, `SuccessfulChangedSignatureSupersedes` | A changed signature receives a fresh episode while the old finding stays visible until the replacement succeeds and supersedes it. | `tests/substrate/extraction/test_enricher_input_quarantine.py::test_real_publish_quarantines_poison_retains_output_and_advances`. |
| `QuarantinedOutputRetained`, `SuccessfulSiblingReplaced` | Terminal snapshot completion preserves the poison item's prior output exactly while independently replacing a healthy sibling. | `tests/substrate/extraction/test_enricher_input_quarantine.py::test_real_publish_quarantines_poison_retains_output_and_advances`. |
| `TerminalRecordAtomicWithRelease`, `WatermarkReleaseIsTerminal`, `ClosedCycleHasNoWorkingState` | A terminal record and watermark release commit together, and no committed cycle retains an in-flight claim. | `tests/substrate/extraction/test_enricher_input_quarantine.py::test_publisher_transaction_fault_cuts_do_not_split_state`. |
| `HeldStatusUsesFailedNamespace`, `NoSystemicQuarantine`, `SystemicCommittedAttemptsStayZero`, `SystemicReservationReleased` | Held progress remains in the required `failed:%` namespace; shared failures release their reservation without spending attempts or activating quarantine. | `tests/substrate/extraction/test_enricher_input_quarantine.py::test_repeated_systemic_provider_failure_never_consumes_item_budget`. |
| `EveryProcessableItemEventuallyTerminal` (temporal property, not an invariant) | Under fair publish cycles, every processable item eventually succeeds or reaches its explicit terminal quarantine bound. | `tests/substrate/extraction/test_enricher_input_quarantine.py::test_real_publish_quarantines_poison_retains_output_and_advances`. |
| `SystemicFailureMisclassificationWitness`, `SnapshotOmissionRetractionWitness` | Named falsifiability checks respectively exhibit corpus quarantine from systemic misclassification and prior-output deletion from an incomplete terminal snapshot. | `tests/pact/test_formal_report_producers.py`, `tests/substrate/extraction/test_enricher_input_quarantine.py`. |
| `WatermarkWithinFrontier`, `ReuseOnlyWhenNoEligibleBacklog` | Successful progress never advances past the phase-start frontier, and neither pre- nor post-enrichment generation reuse is admitted while an eligible node fact remains above that progress watermark. | `tests/substrate/test_publisher.py::test_pending_enricher_backlog_prevents_noop_reuse_until_dag_drains`, `tests/substrate/test_publisher.py::test_pending_enricher_backlog_prevents_post_enrichment_noop_reuse`. |
| `EveryEligibleInputEventuallyProcessed` (temporal property, not an invariant) | Under finite source input, an acyclic bounded chain, and fair publish cycles, every node fact eligible for a downstream enricher is eventually processed. | `tests/substrate/test_publisher.py::test_pending_enricher_backlog_prevents_noop_reuse_until_dag_drains`. |

## Execution Profiles

| Profile | Config | Budget | Purpose | Command |
| --- | --- | --- | --- | --- |
| `smoke` | `TransformGateDirtyScopeSmoke.cfg` | 30s | CI/operator freshness check for dirty-scope soundness; not release evidence. | `uv run --extra dev python scripts/check_formal_transform_gate_dirty_scope.py --profile smoke --out .scratch/formal/transform-gate-dirty-scope-smoke --timeout-seconds 30` |
| `full` | `TransformGateDirtyScope.cfg` | 900s | Full-strength release/PACT evidence for dirty-scope soundness. | `uv run --extra dev python scripts/check_formal_transform_gate_dirty_scope.py --profile full --out .scratch/formal/transform-gate-dirty-scope --timeout-seconds 900` |

Use `uv run --extra dev python
scripts/check_formal_transform_gate_dirty_scope.py --list-profiles --json` to
inspect the profile metadata. A timeout row is `timeout/not freshly proven`, not
an invariant violation. `TransformGateNodeSubtypeScope.tla` remains on the
standard single-profile checker because it is not one of the expensive TLC
models in this sprint.

## Verification Process

Run:

```sh
uv run --extra dev python scripts/check_formal_transform_gate_dirty_scope.py \
  --profile full --out .scratch/formal/transform-gate-dirty-scope \
  --timeout-seconds 900

uv run --extra dev python scripts/check_formal_transform_gate_node_subtype.py \
  --out .scratch/formal/transform-gate-node-subtype
```

The dirty-scope checker emits one row for
`TransformGateDirtyScopeSoundness`, with implementation tests for Chronos
segment-delta extraction, scoped edge checks, and unavailable fallback. The
node-subtype checker emits one row for
`TransformGateNodeSubtypeScopeSoundness`, with implementation tests for unknown
node subtype, known node subtype, and fork-scoped dirty-node subtype cases.

Run the qualification checker (all four configs in one invocation; exits 0 only
when both positive configs prove clean AND both counterexample configs produce
their named violation, printing distinct-state count and wall time per config):

```sh
uv run --extra dev python scripts/check_formal_qualification_gate.py \
  --out .scratch/formal/qualification-gate
```

It emits four rows -- the smoke sanity proof, the `any_open` standing proof
(`NoPublishWhileMandatoryOpen`), and the two counterexample rows (the
`new_findings` one-shot ratchet and the wrong-count witness).

Run the commit/resume checker (positive exhaustion by default; add
`--counterexamples` to also drive the three named counterexample configs and
assert each refutes exactly its named invariant, printing distinct-state
count and wall time per config):

```sh
uv run --extra dev python scripts/check_formal_dbos_commit.py \
  --out .scratch/formal/dbos-commit --counterexamples
```

Run the watermark-retry checker (both configs in one invocation; exits 0 only
when the positive config proves the liveness property clean AND the guard-off
config produces the temporal violation, printing distinct-state count and
wall time per config):

```sh
uv run --extra dev python scripts/check_formal_watermark_retry.py \
  --out .scratch/formal/watermark-retry
```

It emits two rows -- the `WatermarkRetryLiveness` proof (temporal property
`EveryItemEventuallyProcessed` checked under weak fairness) and the
`WatermarkRetryStrandedCounterexample` row, whose pass condition is TLC
reporting `Temporal properties were violated` for the same property under the
same fair spec with `GuardConsultsRetryState = FALSE`. The temporal
refutation is asserted in-process (the suite's named-invariant witness
registry cannot express it), the same idiom as
`tests/formal/test_readiness_liveness_witness.py`.

Run the successful-watermark backlog checker (the positive model and both
reuse-site counterexamples run in one invocation):

```sh
uv run --extra dev python scripts/check_formal_enricher_backlog_drain.py \
  --out .scratch/formal/enricher-backlog-drain
```

`EnricherBacklogDrain.tla` models a finite acyclic two-stage enricher chain.
The publisher snapshots the node-fact frontier once at phase start, so an
upstream fact emitted during that phase becomes eligible for the downstream
stage only on a later publish cycle. `EveryEligibleInputEventuallyProcessed`
proves that finite backlog drains under weak fairness when both the
pre-enrichment and post-enrichment generation-reuse decisions consult
successful progress watermark lag. The pre-reuse guard-off config produces a
temporal stranded-backlog lasso; the post-reuse guard-off config independently
violates `ReuseOnlyWhenNoEligibleBacklog`.

| Model transition | Runtime binding | Refinement evidence |
| --- | --- | --- |
| Phase-start fact frontier snapshot | `src/okg/substrate/publisher/publisher.py::_run_enrichers` | `tests/substrate/test_publisher.py::test_pending_enricher_backlog_prevents_noop_reuse_until_dag_drains` |
| Pre-enrichment reuse consults successful watermark lag | `src/okg/substrate/publisher/publisher.py::_has_pending_enricher_backlog` | `tests/substrate/test_publisher.py::test_pending_enricher_backlog_prevents_noop_reuse_until_dag_drains` |
| Post-enrichment reuse independently rechecks lag | `src/okg/substrate/publisher/publisher.py::run` | `tests/substrate/test_publisher.py::test_pending_enricher_backlog_prevents_post_enrichment_noop_reuse` |

## Model Boundary

Implementation tests must cover Chronos dirty-scope extraction, additive edge
violations, dirty node subtype changes, additive node non-expansion,
node-subtype violations, retractions, and every incremental fallback reason.
The models do not replace live publish-cycle proof that a generation was
blocked or published. They prove the scope-selection rules used by that gate,
while runtime tests bind the publisher extraction boundary and node-subtype SQL
checks to those rules.

`QualificationGate` models the finding-blocking decision only. Operational
blocks (timeout / error / incomplete coverage), the observation-ledger SQL, the
backpressure probing breaker, and the three-level severity lattice (abstracted
to a per-config qualifying/non-qualifying partition at the declared floor) stay
runtime-test evidence. It does not replace live publish-cycle proof; it proves
that only `any_open` honors the standing qualification predicate and that
`new_findings` is a one-shot ratchet, while
`tests/substrate/test_consistency_gates.py` and
`tests/substrate/consistency/test_qualification_mapping.py` bind the real gate
to those rules.

`EnricherBacklogDrain.tla` deliberately bounds the chain to one upstream fact
and one downstream stage, assumes finite source input, an acyclic dependency
graph, and weakly fair publish cycles, and abstracts graph payload contents.
Its watermark comparison models the legacy node-fact incremental selector,
including retractions. Edge-only prior-segment backlog is outside this model:
the current incremental context cannot reconstruct historical edge facts.
Runtime regressions retain authority for selector filtering, retention-stale
progress, retractions, and graph-visible generation output.

`ReusePass.tla` deliberately abstracts the following, each of which stays covered
by runtime tests or the claim-core model rather than by this model:

- **Dependency classes 10 -> 3.** The ten `DEPENDENCY_CLASSES` of
  `reuse_certificates.py` are abstracted to three representative ones
  (`input`, `envelope`, `precedence`). Certificate validity in the model is
  "every consulted class matches the prior/clean baseline"; the real per-class
  byte comparison is `validate_certificate` /
  `dependency_classes_differ`, exercised by
  `tests/substrate/consistency/test_reuse_certificates.py::test_each_dependency_class_invalidates`.
- **Output = identity on the recorded facts.** A step's resolved output is
  modeled as its recorded-fact record, so ANY recorded-class change is an output
  change (the faithful over-approximation). The model therefore does NOT
  represent a genuine changed-input-but-unchanged-output skip; that is exactly
  the bounded-dominance rung, whose authority arithmetic
  (`dominance_is_justifiable`, `derive_competitor_bound`) is OUT of scope here
  and is proven by the claim-core precedence model and
  `test_dominance_rung_approves_only_a_provable_loss` /
  `test_dominance_refusal_falls_through_to_run`. Abstracting dominance to a
  class-match WEAKENS the model: it exercises the ladder/worklist structure and
  the two hazards below, not the number-theoretic dominance bound.
- **New-consumer trigger is independent of the step's recorded classes.** The
  `spawnActive` flag models a new check candidate arising from cross-claim
  interaction (a second operand appearing elsewhere this cycle), NOT from the
  spawner's own recorded dependencies — the faithful representation of the
  section-9 hole. The guard the live engine relies on (possible-effects reach
  over-approximating consumer creation, `compile_possible_effects` /
  `skip_is_justifiable`) is modeled as a single `GuardDynamicConsumer` refusal;
  the model catches its omission but does not prove the reach computation itself.
- **Single spawner + single new consumer; discard/abort out of scope.** One
  Spawner and one Consumer suffice to exhibit an omitted-consumer divergence.
  The discard (unknown dependency / envelope violation -> complete recompute)
  and abort (operator failure -> prior state intact) fallbacks are not modeled
  as transitions: neither can violate resolved-state equality (discard runs the
  full path; abort does not commit), and they are covered by
  `test_unknown_dependency_discards_into_complete_execution` and
  `test_operator_failure_aborts_leaving_prior_state_intact`.
- **Crash/resume atomicity is `DbosCommitResume.tla`.** The DBOS commit/resume
  interleavings that make the pass byte-equal under a crash are that sibling
  model's scope (below), not this equality invariant.

`DbosCommitResume.tla` deliberately abstracts the following, each of which
stays covered by runtime tests or the sibling models rather than by this
model:

- **The commit set is four stores.** Claims, selections, certificates,
  projection, and checkpoint are abstracted to `projection` / `certs` /
  `claims` / `checkpoint`; content is one value per store per cycle (the
  small-scope stand-in for "the pass's bytes are a function of the cycle's
  inputs"). WHAT the correct bytes are is the equality invariant's territory
  (`ReusePass.tla` plus the equivalence harness); this model checks WHEN they
  may become durable and visible.
- **Steps are atomic actions at boundaries.** derive / reuse / admit / commit
  are single atomic actions; a crash hits any boundary and kills the open
  transaction. Mid-step crashes collapse to boundary crashes because every
  within-step durable write is one atomic action and volatile state dies at
  every crash point — the real mid-step hazard (marker vs effects
  decoupling) is modeled explicitly by the `MarkerBeforeEffects` toggle
  rather than by sub-step program counters.
- **Observable-state invariants.** Durable-store invariants are asserted at
  crash boundaries and termination only: between them the engine holds an
  open transaction whose intermediate durable states are unobservable under
  transaction isolation. The split-commit hazard is precisely a protocol
  that makes such a state observable — a crash strands it.
- **Discard / abort / blocked publishes out of scope.** The fallback ladder
  rolls back the transaction (no checkpoint, no certificate rows —
  `test_operator_failure_aborts_leaving_prior_state_intact`,
  `test_discarded_pass_writes_zero_certificates` in
  `tests/substrate/consistency/test_reuse_scheduler.py`), so neither can
  violate the commit-set invariants; blocked publishes checkpoint without
  the maintenance state and stay gate-model territory.
- **Single writer.** The publisher is single-writer (lease-held); concurrent
  workflow instances for the SAME generation are unrepresentable here.
  Cross-cycle interaction is limited to successive generations as fresh
  workflow instances.
- **Live-DBOS wiring is a refinement precondition.** The v1 witnesses run
  the step under the mock-DBOS harness, which re-executes the deterministic
  step body on resume; under a live DBOS singleton a marker-complete step is
  replayed, not re-executed. The model pins the constraint that wiring must
  satisfy — a step's completion marker becomes durable no earlier than its
  effects (markers ride the commit transaction, the DBOS same-database
  boundary) — and `DbosCommitResumeMarkerFirstCounterexample.cfg` is the
  mechanical pin of what happens otherwise. The live-singleton wiring itself
  is `task.reuse.live-proof` scope.

`WatermarkRetry.tla` names its fairness assumptions and abstractions
explicitly; each weakens the liveness theorem and stays covered by runtime
tests rather than by this model:

- **Failure is transient by construction.** A bounded global budget
  (`MaxTransientFailures`) caps failing runs, so at budget zero every attempt
  succeeds — the model form of "an item that keeps being attempted eventually
  succeeds". A permanently failing (poison) item is OUT of scope: it would
  legitimately never process, the theorem does not claim otherwise, and the
  engine's answer to it (held watermark surfaced as
  `failed:incomplete_inputs`, operator-visible) is runtime behavior, not a
  temporal property. The budget is global, not per-item — per-item failure
  independence is not modeled.
- **Fairness is on the engine only.** Weak fairness on publish cycles models
  ingest repeatedly invoking publish; the source has NO fairness and may stay
  quiescent forever — deliberately, because permanent quiescence is the
  environment of the stranded-watermark hazard. The guard-off counterexample
  runs under the SAME fair spec, so the guard, not the fairness, is the
  load-bearing difference.
- **The reuse closure is collapsed to its enricher-retry arm.** The
  generation-level no-op-reuse predicate's other invalidation arms (catalog
  version, lenses, nomos policy pin, gate inputs, native views, extractor
  input signature) are abstracted into the quiescence test; only the
  `last_status` consultation is toggled. Those arms are exercised by
  `tests/substrate/runtime/test_publish_commit_protocol.py` and the publisher
  reuse tests.
- **One enricher, no fact-id arithmetic.** The watermark is abstracted to
  `last_status` plus the pending set; watermark advance/hold byte mechanics
  are `tests/substrate/test_enricher_input_failure_holds_watermark.py`
  territory. The per-enricher skip-clean rung appears as its REFUSAL (in
  every reachable stage-run state either inputs are dirty or the status is
  non-success, so the stage attempts all pending items) — the rung's positive
  skip case coincides with the generation-level skip in this scope. Replay
  exclusions (`ReplayMissError`) are different bookkeeping, not data loss,
  and are not modeled.
- **Engine guard status (the falsification's target).** The stranded-watermark
  defect is NOT open on this tree: the generation-level guard landed via P6 of
  `specify-publish-commit-protocol` (`_enricher_retry_pending_name` in
  `src/okg/substrate/publisher/publisher.py` invalidates reuse as
  `enricher_retry_pending`; commit `0880349b9`, present on `dev`), pinned by
  `tests/substrate/runtime/test_publish_commit_protocol.py::test_p6_held_enricher_retry_invalidates_reuse`.
  `GuardConsultsRetryState = FALSE` therefore pins the PRE-P6 defect class —
  the no-op-reuse stranding that did ship on dev before 2026-07-14 — as a
  named mechanical falsification, so any regression of the guard re-becomes
  this model's counterexample.
