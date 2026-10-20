---- MODULE DogfoodCutover ----
EXTENDS Naturals

\* A small source-by-source cutover model for dogfood source-sync.
\* Rollback snapshots the source frontier and provenance state before a source
\* enters delta mode, and completion is only valid after both are restored.

Sources == {"agent_sessions", "pact"}
Modes == {"legacy", "delta", "rollback"}
Frontiers == 0..2
MaxFrontier == 2

VARIABLES
  mode,
  publishedFrontier,
  visibleFrontier,
  deltaFrontier,
  snapshotFrontier,
  provenance,
  snapshotProvenance,
  deltaCommitted,
  rollbackFrontierRestored,
  rollbackProvenanceRestored,
  rollbackFailed,
  rollbackCompleted

vars == <<
  mode,
  publishedFrontier,
  visibleFrontier,
  deltaFrontier,
  snapshotFrontier,
  provenance,
  snapshotProvenance,
  deltaCommitted,
  rollbackFrontierRestored,
  rollbackProvenanceRestored,
  rollbackFailed,
  rollbackCompleted
>>

BoolFn == [Sources -> BOOLEAN]
FrontierFn == [Sources -> Frontiers]
ModeFn == [Sources -> Modes]

Init ==
  /\ mode = [s \in Sources |-> "legacy"]
  /\ publishedFrontier = [s \in Sources |-> 0]
  /\ visibleFrontier = [s \in Sources |-> 0]
  /\ deltaFrontier = [s \in Sources |-> 0]
  /\ snapshotFrontier = [s \in Sources |-> 0]
  /\ provenance = [s \in Sources |-> TRUE]
  /\ snapshotProvenance = [s \in Sources |-> TRUE]
  /\ deltaCommitted = [s \in Sources |-> FALSE]
  /\ rollbackFrontierRestored = [s \in Sources |-> FALSE]
  /\ rollbackProvenanceRestored = [s \in Sources |-> FALSE]
  /\ rollbackFailed = [s \in Sources |-> FALSE]
  /\ rollbackCompleted = [s \in Sources |-> FALSE]

StartCutover(s) ==
  /\ mode[s] = "legacy"
  /\ ~rollbackCompleted[s]
  /\ mode' = [mode EXCEPT ![s] = "delta"]
  /\ snapshotFrontier' = [snapshotFrontier EXCEPT ![s] = publishedFrontier[s]]
  /\ snapshotProvenance' = [snapshotProvenance EXCEPT ![s] = provenance[s]]
  /\ deltaFrontier' = [deltaFrontier EXCEPT ![s] = publishedFrontier[s]]
  /\ deltaCommitted' = [deltaCommitted EXCEPT ![s] = FALSE]
  /\ rollbackFrontierRestored' = [rollbackFrontierRestored EXCEPT ![s] = FALSE]
  /\ rollbackProvenanceRestored' = [rollbackProvenanceRestored EXCEPT ![s] = FALSE]
  /\ rollbackFailed' = [rollbackFailed EXCEPT ![s] = FALSE]
  /\ UNCHANGED <<
       publishedFrontier,
       visibleFrontier,
       provenance,
       rollbackCompleted
     >>

AdvanceDelta(s) ==
  /\ mode[s] = "delta"
  /\ deltaFrontier[s] < MaxFrontier
  /\ deltaFrontier' = [deltaFrontier EXCEPT ![s] = @ + 1]
  /\ provenance' = [provenance EXCEPT ![s] = TRUE]
  /\ UNCHANGED <<
       mode,
       publishedFrontier,
       visibleFrontier,
       snapshotFrontier,
       snapshotProvenance,
       deltaCommitted,
       rollbackFrontierRestored,
       rollbackProvenanceRestored,
       rollbackFailed,
       rollbackCompleted
     >>

PublishDelta(s) ==
  /\ mode[s] = "delta"
  /\ ~deltaCommitted[s]
  /\ provenance[s]
  /\ publishedFrontier' = [publishedFrontier EXCEPT ![s] = deltaFrontier[s]]
  /\ visibleFrontier' = [visibleFrontier EXCEPT ![s] = deltaFrontier[s]]
  /\ deltaCommitted' = [deltaCommitted EXCEPT ![s] = TRUE]
  /\ UNCHANGED <<
       mode,
       deltaFrontier,
       snapshotFrontier,
       provenance,
       snapshotProvenance,
       rollbackFrontierRestored,
       rollbackProvenanceRestored,
       rollbackFailed,
       rollbackCompleted
     >>

BeginRollback(s) ==
  /\ mode[s] = "delta"
  /\ mode' = [mode EXCEPT ![s] = "rollback"]
  /\ rollbackFrontierRestored' = [rollbackFrontierRestored EXCEPT ![s] = FALSE]
  /\ rollbackProvenanceRestored' = [rollbackProvenanceRestored EXCEPT ![s] = FALSE]
  /\ rollbackFailed' = [rollbackFailed EXCEPT ![s] = FALSE]
  /\ UNCHANGED <<
       publishedFrontier,
       visibleFrontier,
       deltaFrontier,
       snapshotFrontier,
       provenance,
       snapshotProvenance,
       deltaCommitted,
       rollbackCompleted
     >>

RestoreRollbackFrontier(s) ==
  /\ mode[s] = "rollback"
  /\ ~rollbackFailed[s]
  /\ ~rollbackFrontierRestored[s]
  /\ publishedFrontier' = [publishedFrontier EXCEPT ![s] = snapshotFrontier[s]]
  /\ visibleFrontier' = [visibleFrontier EXCEPT ![s] = snapshotFrontier[s]]
  /\ deltaFrontier' = [deltaFrontier EXCEPT ![s] = snapshotFrontier[s]]
  /\ rollbackFrontierRestored' = [rollbackFrontierRestored EXCEPT ![s] = TRUE]
  /\ UNCHANGED <<
       mode,
       snapshotFrontier,
       provenance,
       snapshotProvenance,
       deltaCommitted,
       rollbackProvenanceRestored,
       rollbackFailed,
       rollbackCompleted
     >>

RestoreRollbackProvenance(s) ==
  /\ mode[s] = "rollback"
  /\ ~rollbackFailed[s]
  /\ ~rollbackProvenanceRestored[s]
  /\ provenance' = [provenance EXCEPT ![s] = snapshotProvenance[s]]
  /\ rollbackProvenanceRestored' = [rollbackProvenanceRestored EXCEPT ![s] = TRUE]
  /\ UNCHANGED <<
       mode,
       publishedFrontier,
       visibleFrontier,
       deltaFrontier,
       snapshotFrontier,
       snapshotProvenance,
       deltaCommitted,
       rollbackFrontierRestored,
       rollbackFailed,
       rollbackCompleted
     >>

FailRollback(s) ==
  /\ mode[s] = "rollback"
  /\ ~(rollbackFrontierRestored[s] /\ rollbackProvenanceRestored[s])
  /\ rollbackFailed' = [rollbackFailed EXCEPT ![s] = TRUE]
  /\ UNCHANGED <<
       mode,
       publishedFrontier,
       visibleFrontier,
       deltaFrontier,
       snapshotFrontier,
       provenance,
       snapshotProvenance,
       deltaCommitted,
       rollbackFrontierRestored,
       rollbackProvenanceRestored,
       rollbackCompleted
     >>

CompleteRollback(s) ==
  /\ mode[s] = "rollback"
  /\ rollbackFrontierRestored[s]
  /\ rollbackProvenanceRestored[s]
  /\ ~rollbackFailed[s]
  /\ mode' = [mode EXCEPT ![s] = "legacy"]
  /\ deltaCommitted' = [deltaCommitted EXCEPT ![s] = FALSE]
  /\ rollbackCompleted' = [rollbackCompleted EXCEPT ![s] = TRUE]
  /\ UNCHANGED <<
       publishedFrontier,
       visibleFrontier,
       deltaFrontier,
       snapshotFrontier,
       provenance,
       snapshotProvenance,
       rollbackFrontierRestored,
       rollbackProvenanceRestored,
       rollbackFailed
     >>

Stutter ==
  UNCHANGED vars

Next ==
  \/ \E s \in Sources : StartCutover(s)
  \/ \E s \in Sources : AdvanceDelta(s)
  \/ \E s \in Sources : PublishDelta(s)
  \/ \E s \in Sources : BeginRollback(s)
  \/ \E s \in Sources : RestoreRollbackFrontier(s)
  \/ \E s \in Sources : RestoreRollbackProvenance(s)
  \/ \E s \in Sources : FailRollback(s)
  \/ \E s \in Sources : CompleteRollback(s)
  \/ Stutter

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ mode \in ModeFn
  /\ publishedFrontier \in FrontierFn
  /\ visibleFrontier \in FrontierFn
  /\ deltaFrontier \in FrontierFn
  /\ snapshotFrontier \in FrontierFn
  /\ provenance \in BoolFn
  /\ snapshotProvenance \in BoolFn
  /\ deltaCommitted \in BoolFn
  /\ rollbackFrontierRestored \in BoolFn
  /\ rollbackProvenanceRestored \in BoolFn
  /\ rollbackFailed \in BoolFn
  /\ rollbackCompleted \in BoolFn

DogfoodReady ==
  \A s \in Sources:
    \/ /\ mode[s] = "legacy"
       /\ visibleFrontier[s] = publishedFrontier[s]
       /\ provenance[s]
    \/ /\ mode[s] = "delta"
       /\ deltaCommitted[s]
       /\ visibleFrontier[s] = deltaFrontier[s]
       /\ publishedFrontier[s] = deltaFrontier[s]
       /\ provenance[s]

RollbackPreservesFrontierAndProvenance ==
  \A s \in Sources:
    rollbackCompleted[s] =>
      /\ mode[s] = "legacy"
      /\ ~deltaCommitted[s]
      /\ ~rollbackFailed[s]
      /\ publishedFrontier[s] = snapshotFrontier[s]
      /\ visibleFrontier[s] = snapshotFrontier[s]
      /\ deltaFrontier[s] = snapshotFrontier[s]
      /\ provenance[s] = snapshotProvenance[s]

PartialRollbackNotReady ==
  DogfoodReady => \A s \in Sources : mode[s] # "rollback"

FailedRollbackNotReady ==
  DogfoodReady => \A s \in Sources : ~rollbackFailed[s]

DogfoodReadinessReflectsCutoverState ==
  DogfoodReady =>
    \A s \in Sources:
      \/ mode[s] = "legacy"
      \/ /\ mode[s] = "delta"
         /\ deltaCommitted[s]
         /\ publishedFrontier[s] = deltaFrontier[s]
         /\ visibleFrontier[s] = deltaFrontier[s]

DogfoodCutoverContract ==
  /\ TypeOK
  /\ RollbackPreservesFrontierAndProvenance
  /\ PartialRollbackNotReady
  /\ FailedRollbackNotReady
  /\ DogfoodReadinessReflectsCutoverState

====
