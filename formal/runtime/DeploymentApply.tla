---- MODULE DeploymentApply ----
EXTENDS Naturals, Sequences

CONSTANT BuggyStalePlan, BuggySkippedStage, BuggyDuplicateAction, BuggyFalseWorkerAck

Stages == <<"ownership", "catalog", "nomos", "sources", "worker">>

VARIABLES phase, desired, planned, completed, receiptDigest, actionCounts,
          workerAckDigest, failed, ready, lockHeld, crashed

vars == <<phase, desired, planned, completed, receiptDigest, actionCounts,
          workerAckDigest, failed, ready, lockHeld, crashed>>

Init ==
  /\ phase = "idle"
  /\ desired = 1
  /\ planned = 0
  /\ completed = 0
  /\ receiptDigest = 0
  /\ actionCounts = [s \in 1..Len(Stages) |-> 0]
  /\ workerAckDigest = 0
  /\ failed = FALSE
  /\ ready = FALSE
  /\ lockHeld = FALSE
  /\ crashed = FALSE

Plan ==
  /\ phase = "idle"
  /\ planned' = desired
  /\ phase' = "planned"
  /\ UNCHANGED <<completed, receiptDigest, actionCounts, workerAckDigest,
                  failed, ready, lockHeld, crashed, desired>>

Drift ==
  /\ phase = "planned"
  /\ desired = 1
  /\ desired' = 2
  /\ UNCHANGED <<phase, planned, completed, receiptDigest, actionCounts,
                  workerAckDigest, failed, ready, lockHeld, crashed>>

Begin ==
  /\ phase \in {"planned", "crashed"}
  /\ ~lockHeld
  /\ (planned = desired \/ BuggyStalePlan)
  /\ phase' = "applying"
  /\ lockHeld' = TRUE
  /\ crashed' = FALSE
  /\ UNCHANGED <<desired, planned, completed, receiptDigest, actionCounts,
                  workerAckDigest, failed, ready>>

CompleteNext ==
  /\ phase = "applying"
  /\ completed < Len(Stages)
  /\ LET next == IF BuggySkippedStage /\ completed = 0 THEN 2 ELSE completed + 1 IN
       /\ completed' = next
       /\ receiptDigest' = planned
       /\ actionCounts' = [actionCounts EXCEPT ![next] = @ + 1]
  /\ UNCHANGED <<phase, desired, planned, workerAckDigest, failed,
                  ready, lockHeld, crashed>>

RepeatSource ==
  /\ phase = "applying"
  /\ completed >= 4
  /\ BuggyDuplicateAction
  /\ actionCounts' = [actionCounts EXCEPT ![4] = @ + 1]
  /\ UNCHANGED <<phase, desired, planned, completed, receiptDigest,
                  workerAckDigest, failed, ready, lockHeld, crashed>>

AckWorker ==
  /\ phase = "applying"
  /\ completed = Len(Stages)
  /\ workerAckDigest' = IF BuggyFalseWorkerAck THEN desired + 1 ELSE desired
  /\ ready' = IF BuggyFalseWorkerAck THEN TRUE ELSE planned = desired
  /\ phase' = "complete"
  /\ lockHeld' = FALSE
  /\ UNCHANGED <<desired, planned, completed, receiptDigest, actionCounts,
                  failed, crashed>>

Crash ==
  /\ phase = "applying"
  /\ phase' = "crashed"
  /\ crashed' = TRUE
  /\ lockHeld' = FALSE
  /\ UNCHANGED <<desired, planned, completed, receiptDigest, actionCounts,
                  workerAckDigest, failed, ready>>

Fail ==
  /\ phase = "applying"
  /\ phase' = "failed"
  /\ failed' = TRUE
  /\ ready' = FALSE
  /\ lockHeld' = FALSE
  /\ UNCHANGED <<desired, planned, completed, receiptDigest, actionCounts,
                  workerAckDigest, crashed>>

Next == Plan \/ Drift \/ Begin \/ CompleteNext \/ RepeatSource \/ AckWorker \/ Crash \/ Fail
Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ phase \in {"idle", "planned", "applying", "crashed", "failed", "complete"}
  /\ desired \in 1..2 /\ planned \in 0..2
  /\ completed \in 0..Len(Stages)
  /\ receiptDigest \in 0..2 /\ workerAckDigest \in 0..3

OrderedStages == completed = 0 \/ \A i \in 1..completed : actionCounts[i] = 1
NoStaleReceipt == receiptDigest = 0 \/ receiptDigest = planned
NoStaleMutation == phase \notin {"applying", "crashed", "complete"} \/ planned = desired
NoDuplicateSourceRegistration == actionCounts[4] <= 1
ExactWorkerAck == ready => workerAckDigest = desired
FailureNeverReady == failed => ~ready
NoOpMeansConverged == ready => completed = Len(Stages) /\ planned = desired

DeploymentApplySafety ==
  TypeOK /\ OrderedStages /\ NoStaleReceipt /\ NoStaleMutation /\
  NoDuplicateSourceRegistration /\ ExactWorkerAck /\ FailureNeverReady /\
  NoOpMeansConverged
====
