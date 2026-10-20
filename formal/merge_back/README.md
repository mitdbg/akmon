# Merge-Back Formal Models

## System

The merge-back models cover the worktree fork merge-back pipeline implemented
by `src/okg/substrate/merge_back.py`. The production path stages a filtered fork
delta on a temporary branch, gates that candidate with the publisher, replays the
accepted source-only rows onto `default` after a human decision, and preserves
quarantined rows for review.

These models focus on the safety properties that were previously tracked as
open merge-back gaps in `formal/gap-inventory.toml`: replay equivalence,
anti-laundering restamp, quarantine atomicity, and blocked-LAND rollback.

## Design Contract

- Safety property: merge-back must only land the source rows that were staged
  and gated, must not launder trusted or derived provenance from a fork onto
  `default`, must never silently lose durable preview rows, and must restore the
  `default` live read set when the LAND publish blocks.
- Authority boundary: `src/okg/substrate/merge_back.py` owns STAGE -> GATE ->
  LAND orchestration and rollback; `src/okg/substrate/merge_restamp.py` owns
  provenance restamp/refusal; `src/okg/substrate/merge_partition.py` owns the
  accepted/quarantined split.
- Valid runtime paths: clean merge-back, gate-blocked staging, target advance
  before LAND, LAND publish block with rollback, reserved/null source refusal,
  legal row acceptance, illegal/conflicting row quarantine, and injected
  staging/quarantine write faults.
- Out of scope: the models do not prove the full Chronos branch interval
  allocator, the full publisher consistency gate, or human decision identity.
  Those remain covered by runtime tests and existing merge-guard checks.
- Preconditions/refinement: the models assume Chronos merge previews identify
  durable changes and conflicts, publisher gates correctly report pass/block,
  and branch locks exclude concurrent default writes during LAND. These
  assumptions are pinned by
  `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_holds_default_write_lock_through_land_publish`,
  `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_gate_violation_blocks_default_unchanged`,
  `tests/substrate/test_merge_partition.py::test_partition_over_real_fork_preview`,
  and the focused restamp/partition tests named below.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `MergeBackReplay.tla` | `MergeBackReplay.cfg` | STAGE isolation, GATE/LAND replay equivalence, target-advance blocking, gate-derived row exclusion, and no phantom retraction after LAND. |
| `MergeBackRestamp.tla` | `MergeBackRestamp.cfg`, `MergeBackRestampRecordIdWitness.cfg`, `MergeBackRestampRevisionWitness.cfg`, `MergeBackRestampForkIdentityWitness.cfg`, `MergeBackRestampPromoteMarkerWitness.cfg`, `MergeBackRestampTrustedDefaultWitness.cfg` | Trust restamp anti-laundering with PER-FIELD provenance (source, source_record_id, source_revision) and buggy-toggle witness configs that refute each pass-through invariant by name. |
| `MergeBackPartitionAtomicity.tla` | `MergeBackPartitionAtomicity.cfg` | Accepted/quarantined partition totality and atomic staging/quarantine commit behavior. |
| `MergeBackLandRollback.tla` | `MergeBackLandRollback.cfg` | Blocked LAND rollback of default live node/edge rows, not just generation pointer restoration. |
| `MergeBackPartitionAtomicityCounterexample.tla` | `MergeBackPartitionAtomicityCounterexample.cfg`, `MergeBackPartitionMidTxnWitness.cfg` | Non-vacuity witnesses, one invariant per config: the committed/aborted phases are reachable, and the MID-TRANSACTION partial state (staging buffered, quarantine not yet, nothing durable) is reachable — the interleaving the atomicity invariants guard. |
| `MergeBackLandRollbackCounterexample.tla` | `MergeBackLandRollbackCounterexample.cfg`, `MergeBackLandRollbackIncompleteWitness.cfg` | Non-vacuity witnesses, one invariant per config: the rolled-back phase is reachable, and the delete-failure (rollback-incomplete) branch is reachable, so the no-silent-fallback invariant guards a live hazard state. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `MergeBackReplay.tla` | `merge_back` creates staging from `default`, composes a source-only accepted delta, gates a clone, excludes gate-derived rows from LAND, blocks if target advanced, and replays the gated source delta onto `default`. | `src/okg/substrate/merge_back.py` | `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_clean_fork_lands_on_default`, `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_replays_source_only_and_derives_once_on_land`, `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_holds_default_write_lock_through_land_publish`, `tests/substrate/test_merge_back_orchestrator.py::test_gate_side_effect_cleanup_keeps_foreign_derived_facts` |
| `MergeBackRestamp.tla` | `_compose_and_stage` restamps durable fork rows through `restamp_diff`, refuses null/system/derived sources, and lands only merge-producer provenance. | `src/okg/substrate/merge_restamp.py`, `src/okg/substrate/merge_back.py` | `tests/substrate/test_merge_restamp.py`, `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_blocks_whole_merge_on_unmergeable_source` |
| `MergeBackPartitionAtomicity.tla` | `_compose_and_stage` partitions durable preview rows into accepted and quarantined sets, creates `quarantine:<fork>` for quarantined rows, and writes accepted staging rows plus quarantine rows in one transaction (modeled as buffered writes with a crash interleaving between them; only COMMIT makes either partition durable). | `src/okg/substrate/merge_partition.py`, `src/okg/substrate/merge_back.py` | `tests/substrate/test_merge_partition.py`, `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_composes_promote_scope_restamp_partition`, `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_quarantines_conflicting_durable_rows`, `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_stage_quarantine_fault_rolls_back_both_writes` |
| `MergeBackLandRollback.tla` | After LAND applies to `default`, a blocked default publish runs the TWO-STEP rollback: delete the blocked generation (best-effort — a failure is LOUD, marking the result rollback-incomplete with the stranded generation id), then unconditionally restore `default` to the pre-LAND live node/edge read set. | `src/okg/substrate/merge_back.py` | `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_rolls_default_back_when_land_publish_blocks`, `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_rolls_back_modified_node_and_edge_live_rows` |
| `MergeBackPartitionAtomicityCounterexample.tla` | The committed and aborted partition phases are intentionally asserted unreachable, so TLC must exhibit a clean atomic commit and a fault-during-commit abort, proving the atomicity antecedents are live. | `src/okg/substrate/merge_partition.py`, `src/okg/substrate/merge_back.py` | `tests/substrate/test_merge_partition.py`, `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_stage_quarantine_fault_rolls_back_both_writes` |
| `MergeBackLandRollbackCounterexample.tla` | The rolled-back LAND phase is intentionally asserted unreachable, so TLC must exhibit a blocked LAND that recovers the pre-LAND live set, proving the rollback antecedent is live. | `src/okg/substrate/merge_back.py` | `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_rolls_default_back_when_land_publish_blocks`, `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_rolls_back_modified_node_and_edge_live_rows` |

## Invariant Map

| Invariants | What it means | Code/test signal |
| --- | --- | --- |
| `TypeOK` | Each model state stays inside its finite merge-back state shape. | Formal report producer coverage in `tests/pact/test_formal_report_producers.py`. |
| `DefaultIsolatedUntilGatePass` | `default` is not mutated before a gate-passed LAND replay. | `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_gate_violation_blocks_default_unchanged`. |
| `GateValidatesStagedAcceptedDelta`, `LandReplayEqualsGatedSourceDelta` | GATE validates the accepted staging delta and LAND replays exactly that source delta. | `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_clean_fork_lands_on_default` and `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_composes_promote_scope_restamp_partition`. |
| `GateBlockedLeavesDefaultUnchanged`, `TargetAdvanceBlocksLand` | Gate failure or target advance blocks LAND and leaves `default` unchanged. | `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_gate_violation_blocks_default_unchanged` and `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_holds_default_write_lock_through_land_publish`. |
| `GateDerivedRowsAreNotReplayed` | Derived rows emitted while gating the clone are cleaned up and not replayed onto `default`. | `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_replays_source_only_and_derives_once_on_land` and `tests/substrate/test_merge_back_orchestrator.py::test_gate_side_effect_cleanup_keeps_foreign_derived_facts`. |
| `NoPhantomRetractionAfterLand` | A successful LAND remains live on the next default publish. | `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_clean_fork_lands_on_default`. |
| `ReservedOrNullSourcesBlockBeforeStage` | Null, trusted/system, and derived fork sources do not reach default. | `tests/substrate/test_merge_restamp.py::test_null_source_refused`, `tests/substrate/test_merge_restamp.py::test_reserved_system_source_refused`, and `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_blocks_whole_merge_on_unmergeable_source`. |
| `MergeProducerClassifiesUntrusted` | Merge producer ids read back as untrusted, not trusted/system. | `tests/substrate/test_merge_restamp.py::test_merge_producer_id_reads_back_untrusted`. |
| `SourceFieldRestamped`, `SourceRecordIdRestamped`, `SourceRevisionRestamped`, `ForkProvenanceIdentityDoesNotLand`, `PromoteMarkerDoesNotLand` | `source`, `source_record_id`, `source_revision`, and the server-only promote marker cannot preserve fork identity on default. | `tests/substrate/test_merge_restamp.py::test_restamp_row_normalizes_all_provenance_and_strips_promote_intent`. |
| `NoForkInputYieldsTrustedDefaultSource` | No mergeable fork input can produce a trusted default source label. | `tests/substrate/test_merge_restamp.py::test_reserved_is_superset_of_system_prefixes`, `tests/substrate/test_merge_restamp.py::test_system_prefixes_read_trusted_and_null_too`, and `tests/substrate/test_merge_restamp.py::test_derived_prefixes_read_untrusted_but_still_refused`. |
| `AcceptedQuarantinedDisjoint`, `AcceptedQuarantinedCoverDurablePreview` | Accepted and quarantined rows are disjoint and cover every durable preview row. | `tests/substrate/test_merge_partition.py::test_total_coverage_no_silent_loss`. |
| `ConflictsAlwaysQuarantined`, `IllegalRowsQuarantined` | Conflicting rows and illegal-subtype rows go to quarantine, not default. | `tests/substrate/test_merge_partition.py::test_pact_accept_conflict_illegal_bucketing`, `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_quarantines_conflicting_durable_rows`, and `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_composes_promote_scope_restamp_partition`. |
| `UnclassifiableRowsBlock` | A node row without subtype is refused rather than silently accepted. | `tests/substrate/test_merge_partition.py::test_node_with_no_subtype_is_unclassifiable_not_silently_accepted`. |
| `StageAndQuarantineCommitAtomically`, `NoDurableWritesBeforeCommit`, `CrashLeavesNeitherPartialWrite` | Accepted staging and quarantine writes commit together; an injected fault leaves no partial quarantine branch or accepted default row. | `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_stage_quarantine_fault_rolls_back_both_writes`. |
| `PreLandLiveSetSnapshotCaptured`, `BlockedLandRestoresDefaultLiveSet` | LAND rollback uses the pre-LAND live read set and restores it. | `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_rolls_default_back_when_land_publish_blocks` and `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_rolls_back_modified_node_and_edge_live_rows`. |
| `RollbackIncompleteNeverSilent`, `CleanRollbackDeletesBlockedGeneration`, `PublishedPointerRestorationIsNotSufficient` | A rollback that ends with the blocked generation checkpoint still live MUST have reported itself incomplete (the no-silent-fallback contract); the clean path really deletes the checkpoint; pointer restoration alone is not enough. | `tests/substrate/test_merge_back_rollback_loud.py` (delete failure is loud, restore still runs) and `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_rolls_back_modified_node_and_edge_live_rows`. |
| `MergeBackReplaySafety`, `MergeBackRestampSafety`, `MergeBackPartitionAtomicitySafety`, `LandBlockRollbackSafety` | Aggregate property for each gap-level model. | Strict report rows emitted by `scripts/check_formal_merge_back_safety.py` and tested in `tests/pact/test_formal_report_producers.py`. |
| `BrokenPartitionNeverCommits` | Non-vacuity witness in `MergeBackPartitionAtomicityCounterexample.cfg`: asserts the partition never reaches a committed-or-aborted phase. TLC refutes it, proving `StageAndQuarantineCommitAtomically`/`CrashLeavesNeitherPartialWrite` guard reachable terminal phases. | `MergeBackPartitionAtomicityCounterexample.cfg` must report the broken invariant violated; positive atomicity behavior is exercised by `tests/substrate/test_merge_partition.py` and `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_stage_quarantine_fault_rolls_back_both_writes`. |
| `BrokenLandNeverRollsBack` | Non-vacuity witness in `MergeBackLandRollbackCounterexample.cfg`: asserts a blocked LAND never reaches the rolled-back phase. TLC refutes it, proving `BlockedLandRestoresDefaultLiveSet` and the rollback-reporting invariants guard a reachable rollback. | `MergeBackLandRollbackCounterexample.cfg` must report the broken invariant violated; positive rollback behavior is exercised by `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_rolls_default_back_when_land_publish_blocks` and `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_rolls_back_modified_node_and_edge_live_rows`. |
| `BrokenRollbackNeverIncomplete` | Non-vacuity witness in `MergeBackLandRollbackIncompleteWitness.cfg`: asserts the rollback-incomplete report never occurs. TLC refutes it, proving the delete-failure branch behind `RollbackIncompleteNeverSilent` is reachable. | `run_formal_suite` verifies the named violation; the code path is exercised by `tests/substrate/test_merge_back_rollback_loud.py`. |
| `BrokenNoMidTransactionState` | Non-vacuity witness in `MergeBackPartitionMidTxnWitness.cfg`: asserts the staging transaction never holds buffered rows before COMMIT. TLC refutes it, proving the mid-transaction interleaving behind `NoDurableWritesBeforeCommit` and `CrashLeavesNeitherPartialWrite` is reachable. | `run_formal_suite` verifies the named violation; injected-fault behavior is exercised by `tests/substrate/test_merge_back_orchestrator.py::test_merge_back_stage_quarantine_fault_rolls_back_both_writes`. |
| `BuggySkipRecordIdRestamp`, `BuggySkipRevisionRestamp`, `BuggyForkIdentityLands`, `BuggyPromoteMarkerLands`, `BuggyTrustedDefault` (constants) | Buggy toggles, FALSE in the positive config: each per-invariant witness config flips one toggle so TLC refutes the REAL invariant by name (`SourceRecordIdRestamped`, `SourceRevisionRestamped`, `ForkProvenanceIdentityDoesNotLand`, `PromoteMarkerDoesNotLand`, `NoForkInputYieldsTrustedDefaultSource`), proving each constrains reachable state rather than reading dead variables. | The five `MergeBackRestamp*Witness.cfg` configs in `run_formal_suite`; positive behavior is exercised by `tests/substrate/test_merge_restamp.py`. |

## Verification Process

Run:

```sh
uv run --extra dev python scripts/check_formal_merge_back_safety.py \
  --out .scratch/formal/merge-back-safety
```

The checker runs all four modules and writes one top-level
`.scratch/formal/merge-back-safety/formal-report.yaml` containing strict rows
for:

- `formal.merge-back.stage-land-replay-equivalence`
- `formal.merge-back.trust-restamp-antilaundering`
- `formal.merge-back.quarantine-partition-atomicity`
- `formal.merge-back.land-block-rollback`

Each row includes the model/config digests, the executed TLC command, the
checker command, and the runtime refinement tests listed in this README.

## Model Boundary

The models deliberately abstract Chronos storage internals to finite row sets
and state-machine phases. They do not prove SQL query planning, catalog
loading, publisher lock implementation, or the full transform gate. Runtime
tests must keep proving real Chronos previews, publisher gate pass/block
signals, branch locks, source restamp, partition behavior, injected fault
rollback, and live node/edge reads. The formal models prove the safety contract
that those runtime boundaries are meant to implement.
