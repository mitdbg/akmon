---- MODULE MergeBackLandRollback ----

\* Blocked-LAND rollback, mirroring the FIXED production sequence
\* (merge_back.py, strengthen-formal-models wave 3): the rollback is TWO
\* sequential steps — delete the blocked generation checkpoint, then restore
\* the target branch to the pre-land generation — and the delete can fail.
\* The delete failure is best-effort by design (the restore is the
\* correctness-critical step and always runs), but it must be LOUD: the
\* result carries a rollback-incomplete marker. The no-silent-fallback
\* invariant below is the model form of that contract; before this wave the
\* whole rollback was one atomic action, so the invariants were tautologies
\* over a transition relation with no partial state.

LiveRows == {"node:v1", "node:v2", "edge:v1", "edge:v2", "new-node"}
PreLive == {"node:v1", "edge:v1"}
LandedLive == {"node:v2", "edge:v2", "new-node"}
Phases == {
  "start", "land_committed", "generation_deleted", "delete_failed",
  "rolled_back"
}

VARIABLES
  phase,
  preLandLiveSet,
  defaultLiveSet,
  blockedGenerationLive,
  publishedPointerRestored,
  rollbackReportedIncomplete

vars == <<
  phase,
  preLandLiveSet,
  defaultLiveSet,
  blockedGenerationLive,
  publishedPointerRestored,
  rollbackReportedIncomplete
>>

Init ==
  /\ phase = "start"
  /\ preLandLiveSet = PreLive
  /\ defaultLiveSet = PreLive
  /\ blockedGenerationLive = FALSE
  /\ publishedPointerRestored = FALSE
  /\ rollbackReportedIncomplete = FALSE

LandCommitBeforePublish ==
  /\ phase = "start"
  /\ defaultLiveSet' = LandedLive
  /\ blockedGenerationLive' = TRUE
  /\ phase' = "land_committed"
  /\ UNCHANGED <<
       preLandLiveSet, publishedPointerRestored, rollbackReportedIncomplete
     >>

\* Step 1a: delete_generation succeeds — the blocked checkpoint is gone.
DeleteBlockedGeneration ==
  /\ phase = "land_committed"
  /\ blockedGenerationLive' = FALSE
  /\ phase' = "generation_deleted"
  /\ UNCHANGED <<
       preLandLiveSet, defaultLiveSet, publishedPointerRestored,
       rollbackReportedIncomplete
     >>

\* Step 1b: delete_generation FAILS — the checkpoint stays stranded and the
\* failure is reported loudly (the fixed code marks the result
\* rollback-incomplete and logs). Deleting the reporting conjunct here — or
\* the equivalent marker in production — trips RollbackIncompleteNeverSilent.
DeleteBlockedGenerationFails ==
  /\ phase = "land_committed"
  /\ rollbackReportedIncomplete' = TRUE
  /\ phase' = "delete_failed"
  /\ UNCHANGED <<
       preLandLiveSet, defaultLiveSet, blockedGenerationLive,
       publishedPointerRestored
     >>

\* Step 2: restore_branch_to_generation — unconditional, runs after either
\* delete outcome. One production call restores both the branch pointer and
\* the live row set, hence the two assignments in one action.
RestoreBranch ==
  /\ phase \in {"generation_deleted", "delete_failed"}
  /\ defaultLiveSet' = preLandLiveSet
  /\ publishedPointerRestored' = TRUE
  /\ phase' = "rolled_back"
  /\ UNCHANGED <<
       preLandLiveSet, blockedGenerationLive, rollbackReportedIncomplete
     >>

Next ==
  \/ LandCommitBeforePublish
  \/ DeleteBlockedGeneration
  \/ DeleteBlockedGenerationFails
  \/ RestoreBranch

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ phase \in Phases
  /\ preLandLiveSet \subseteq LiveRows
  /\ defaultLiveSet \subseteq LiveRows
  /\ blockedGenerationLive \in BOOLEAN
  /\ publishedPointerRestored \in BOOLEAN
  /\ rollbackReportedIncomplete \in BOOLEAN

PreLandLiveSetSnapshotCaptured ==
  preLandLiveSet = PreLive

BlockedLandRestoresDefaultLiveSet ==
  phase = "rolled_back" => defaultLiveSet = preLandLiveSet

\* The no-silent-fallback contract (repo P0 policy): a rollback that ends
\* with the blocked generation checkpoint still live MUST have reported
\* itself incomplete. Reachably falsifiable: DeleteBlockedGenerationFails
\* minus its reporting conjunct reaches rolled_back with a live checkpoint
\* and no report.
RollbackIncompleteNeverSilent ==
  (phase = "rolled_back" /\ blockedGenerationLive) =>
    rollbackReportedIncomplete

\* The clean path really deletes the checkpoint: an unreported rollback end
\* state has no stranded generation.
CleanRollbackDeletesBlockedGeneration ==
  (phase = "rolled_back" /\ ~rollbackReportedIncomplete) =>
    ~blockedGenerationLive

\* Restoring the published pointer is not sufficient evidence of rollback:
\* the live row set must match the pre-land snapshot too. Holds because one
\* production call restores both together.
PublishedPointerRestorationIsNotSufficient ==
  publishedPointerRestored => defaultLiveSet = preLandLiveSet

LandBlockRollbackSafety ==
  /\ TypeOK
  /\ PreLandLiveSetSnapshotCaptured
  /\ BlockedLandRestoresDefaultLiveSet
  /\ RollbackIncompleteNeverSilent
  /\ CleanRollbackDeletesBlockedGeneration
  /\ PublishedPointerRestorationIsNotSufficient

====
