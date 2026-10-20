---- MODULE TargetedEnricherBackfill ----
EXTENDS Naturals, FiniteSets

\* Small state-machine model for issue 214 targeted enricher backfill.
\* It models one command invocation. Failed chunks remain in the missing
\* input set, so a later command can retry them. Normal publish progress
\* advances only after this command drains the selected worklist without
\* failed inputs.

Enrichers == {"target", "other"}
Inputs == {"i1", "i2"}
LateInputs == {"late"}
Surfaces == {"source", "view", "lens", "consistency"}
NameStatuses == {"unique", "missing", "ambiguous"}
BranchNames == {"default", "experiment"}
ChunkSafety == {"safe", "unsafe"}
OutputScopes == {"in_scope", "out_of_scope"}
Phases == {"ready", "running", "failed", "done"}
ProgressFrontiers == {"none", "pinned"}

VARIABLES
  nameStatus,
  branchName,
  chunkSafety,
  outputScope,
  invokedEnrichers,
  invokedSurfaces,
  lockHeld,
  lockAcquired,
  requestedLimit,
  chunkSize,
  remainingInputs,
  concurrentInputs,
  completedInputs,
  failedInputs,
  processedInputsThisRun,
  lastChunkInputs,
  phase,
  globalProgressAdvanced,
  progressFrontier,
  graphWrites

vars == <<
  nameStatus,
  branchName,
  chunkSafety,
  outputScope,
  invokedEnrichers,
  invokedSurfaces,
  lockHeld,
  lockAcquired,
  requestedLimit,
  chunkSize,
  remainingInputs,
  concurrentInputs,
  completedInputs,
  failedInputs,
  processedInputsThisRun,
  lastChunkInputs,
  phase,
  globalProgressAdvanced,
  progressFrontier,
  graphWrites
>>

Init ==
  /\ nameStatus \in NameStatuses
  /\ branchName \in BranchNames
  /\ chunkSafety \in ChunkSafety
  /\ outputScope \in OutputScopes
  /\ invokedEnrichers = {}
  /\ invokedSurfaces = {}
  /\ lockHeld = FALSE
  /\ lockAcquired = FALSE
  /\ requestedLimit \in 0..2
  /\ chunkSize \in 0..2
  /\ remainingInputs = Inputs
  /\ concurrentInputs = {}
  /\ completedInputs = {}
  /\ failedInputs = {}
  /\ processedInputsThisRun = {}
  /\ lastChunkInputs = {}
  /\ phase = "ready"
  /\ globalProgressAdvanced = FALSE
  /\ progressFrontier = "none"
  /\ graphWrites = FALSE

BadSelectionStop ==
  /\ nameStatus # "unique"
  /\ phase = "ready"
  /\ phase' = "done"
  /\ UNCHANGED <<
       nameStatus,
       branchName,
       chunkSafety,
       outputScope,
       invokedEnrichers,
       invokedSurfaces,
       lockHeld,
       lockAcquired,
       requestedLimit,
       chunkSize,
       remainingInputs,
       concurrentInputs,
       completedInputs,
       failedInputs,
       processedInputsThisRun,
       lastChunkInputs,
       globalProgressAdvanced,
       progressFrontier,
       graphWrites
	 >>

UnsupportedBranchStop ==
  /\ nameStatus = "unique"
  /\ branchName # "default"
  /\ phase = "ready"
  /\ phase' = "done"
  /\ UNCHANGED <<
       nameStatus,
       branchName,
       chunkSafety,
       outputScope,
       invokedEnrichers,
       invokedSurfaces,
       lockHeld,
       lockAcquired,
       requestedLimit,
       chunkSize,
       remainingInputs,
       concurrentInputs,
       completedInputs,
       failedInputs,
       processedInputsThisRun,
       lastChunkInputs,
       globalProgressAdvanced,
       progressFrontier,
       graphWrites
     >>

UnsafeCapabilityStop ==
  /\ nameStatus = "unique"
  /\ branchName = "default"
  /\ chunkSafety = "unsafe"
  /\ phase = "ready"
  /\ phase' = "done"
  /\ UNCHANGED <<
       nameStatus,
       branchName,
       chunkSafety,
       outputScope,
       invokedEnrichers,
       invokedSurfaces,
       lockHeld,
       lockAcquired,
       requestedLimit,
       chunkSize,
       remainingInputs,
       concurrentInputs,
       completedInputs,
       failedInputs,
       processedInputsThisRun,
       lastChunkInputs,
       globalProgressAdvanced,
       progressFrontier,
       graphWrites
     >>

AcquireLock ==
  /\ nameStatus = "unique"
  /\ branchName = "default"
  /\ chunkSafety = "safe"
  /\ phase = "ready"
  /\ ~lockHeld
  /\ lockHeld' = TRUE
  /\ lockAcquired' = TRUE
  /\ UNCHANGED <<
       nameStatus,
       branchName,
       chunkSafety,
       outputScope,
       invokedEnrichers,
       invokedSurfaces,
       requestedLimit,
       chunkSize,
       remainingInputs,
       concurrentInputs,
       completedInputs,
       failedInputs,
       processedInputsThisRun,
       lastChunkInputs,
       phase,
       globalProgressAdvanced,
       progressFrontier,
       graphWrites
     >>

AcceptedChunk ==
  /\ nameStatus = "unique"
  /\ branchName = "default"
  /\ chunkSafety = "safe"
  /\ outputScope = "in_scope"
  /\ lockHeld
  /\ phase \in {"ready", "running"}
  /\ \E chunk \in SUBSET remainingInputs:
       /\ chunk # {}
       /\ Cardinality(processedInputsThisRun \cup chunk) <= requestedLimit
       /\ Cardinality(chunk) <= chunkSize
       /\ completedInputs' = completedInputs \cup chunk
       /\ remainingInputs' = remainingInputs \ chunk
       /\ failedInputs' = failedInputs
       /\ processedInputsThisRun' = processedInputsThisRun \cup chunk
       /\ lastChunkInputs' = chunk
       /\ invokedEnrichers' = invokedEnrichers \cup {"target"}
       /\ invokedSurfaces' = invokedSurfaces
       /\ graphWrites' = TRUE
       /\ phase' = "running"
       /\ globalProgressAdvanced' = globalProgressAdvanced
       /\ UNCHANGED <<
            nameStatus,
            branchName,
            chunkSafety,
            outputScope,
            lockHeld,
            lockAcquired,
            requestedLimit,
            chunkSize,
            concurrentInputs,
            progressFrontier
          >>

RejectedChunk ==
  /\ nameStatus = "unique"
  /\ branchName = "default"
  /\ chunkSafety = "safe"
  /\ lockHeld
  /\ phase \in {"ready", "running"}
  /\ \E chunk \in SUBSET remainingInputs:
       /\ chunk # {}
       /\ Cardinality(processedInputsThisRun \cup chunk) <= requestedLimit
       /\ Cardinality(chunk) <= chunkSize
       /\ completedInputs' = completedInputs
       /\ remainingInputs' = remainingInputs
       /\ failedInputs' = failedInputs \cup chunk
       /\ processedInputsThisRun' = processedInputsThisRun \cup chunk
       /\ lastChunkInputs' = chunk
       /\ invokedEnrichers' = invokedEnrichers \cup {"target"}
       /\ invokedSurfaces' = invokedSurfaces
       /\ graphWrites' = graphWrites
       /\ phase' = "failed"
       /\ globalProgressAdvanced' = FALSE
       /\ lockHeld' = FALSE
       /\ UNCHANGED <<
            nameStatus,
            branchName,
            chunkSafety,
            outputScope,
            lockAcquired,
            requestedLimit,
            chunkSize,
            concurrentInputs,
            progressFrontier
          >>

ConcurrentInputArrives ==
  /\ concurrentInputs = {}
  /\ concurrentInputs' = LateInputs
  /\ UNCHANGED <<
       nameStatus,
       branchName,
       chunkSafety,
       outputScope,
       invokedEnrichers,
       invokedSurfaces,
       lockHeld,
       lockAcquired,
       requestedLimit,
       chunkSize,
       remainingInputs,
       completedInputs,
       failedInputs,
       processedInputsThisRun,
       lastChunkInputs,
       phase,
       globalProgressAdvanced,
       progressFrontier,
       graphWrites
     >>

AdvanceGlobalProgress ==
  /\ nameStatus = "unique"
  /\ branchName = "default"
  /\ chunkSafety = "safe"
  /\ lockHeld
  /\ phase \in {"ready", "running"}
  /\ remainingInputs = {}
  /\ failedInputs = {}
  /\ globalProgressAdvanced' = TRUE
  /\ progressFrontier' = "pinned"
  /\ phase' = "done"
  /\ UNCHANGED <<
       nameStatus,
       branchName,
       chunkSafety,
       outputScope,
       invokedEnrichers,
       invokedSurfaces,
       lockAcquired,
       requestedLimit,
       chunkSize,
       remainingInputs,
       concurrentInputs,
       completedInputs,
       failedInputs,
       processedInputsThisRun,
       lastChunkInputs,
       graphWrites
     >>
  /\ lockHeld' = FALSE

Next ==
  \/ BadSelectionStop
  \/ UnsupportedBranchStop
  \/ UnsafeCapabilityStop
  \/ AcquireLock
  \/ AcceptedChunk
  \/ RejectedChunk
  \/ ConcurrentInputArrives
  \/ AdvanceGlobalProgress

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ nameStatus \in NameStatuses
  /\ branchName \in BranchNames
  /\ chunkSafety \in ChunkSafety
  /\ outputScope \in OutputScopes
  /\ invokedEnrichers \subseteq Enrichers
  /\ invokedSurfaces \subseteq Surfaces
  /\ lockHeld \in BOOLEAN
  /\ lockAcquired \in BOOLEAN
  /\ requestedLimit \in 0..2
  /\ chunkSize \in 0..2
  /\ remainingInputs \subseteq Inputs
  /\ concurrentInputs \subseteq LateInputs
  /\ completedInputs \subseteq Inputs
  /\ failedInputs \subseteq Inputs
  /\ processedInputsThisRun \subseteq Inputs
  /\ lastChunkInputs \subseteq Inputs
  /\ phase \in Phases
  /\ globalProgressAdvanced \in BOOLEAN
  /\ progressFrontier \in ProgressFrontiers
  /\ graphWrites \in BOOLEAN

BadSelectionWritesNothing ==
  nameStatus # "unique" =>
    /\ invokedEnrichers = {}
    /\ invokedSurfaces = {}
    /\ ~graphWrites
    /\ ~globalProgressAdvanced

NonDefaultBranchWritesNothing ==
  branchName # "default" =>
    /\ invokedEnrichers = {}
    /\ invokedSurfaces = {}
    /\ ~graphWrites
    /\ ~globalProgressAdvanced

UnsafeCapabilityWritesNothing ==
  chunkSafety = "unsafe" =>
    /\ invokedEnrichers = {}
    /\ invokedSurfaces = {}
    /\ ~graphWrites
    /\ ~globalProgressAdvanced

UniqueSelectionOnlyTargetsEnricher ==
  nameStatus = "unique" =>
    /\ invokedEnrichers \subseteq {"target"}
    /\ invokedSurfaces = {}

TargetedBackfillIsolation ==
  /\ BadSelectionWritesNothing
  /\ NonDefaultBranchWritesNothing
  /\ UnsafeCapabilityWritesNothing
  /\ UniqueSelectionOnlyTargetsEnricher

ChunkBounded ==
  /\ Cardinality(lastChunkInputs) <= requestedLimit
  /\ Cardinality(lastChunkInputs) <= chunkSize

ProcessedWithinCommandLimit ==
  Cardinality(processedInputsThisRun) <= requestedLimit

CompletedOnlyFromKnownInputs == completedInputs \subseteq Inputs
FailedInputsRemainRetryable == failedInputs \subseteq remainingInputs
CompletedInputsWereProcessed == completedInputs \subseteq processedInputsThisRun
FailedInputsWereProcessed == failedInputs \subseteq processedInputsThisRun
NoInputBothCompleteAndFailed == completedInputs \cap failedInputs = {}
FailedChunkBlocksGlobalProgress == failedInputs # {} => ~globalProgressAdvanced
GlobalProgressOnlyWhenComplete ==
  globalProgressAdvanced =>
    /\ branchName = "default"
    /\ remainingInputs = {}
    /\ failedInputs = {}
    /\ completedInputs = Inputs
    /\ progressFrontier = "pinned"
ConcurrentInputsRemainOutsideCommandWork ==
  concurrentInputs \cap (
    remainingInputs \cup completedInputs \cup failedInputs \cup processedInputsThisRun
  ) = {}
PinnedFrontierSafety ==
  globalProgressAdvanced => progressFrontier = "pinned"
GraphWritesRequireAcceptedWork ==
  graphWrites =>
    /\ nameStatus = "unique"
    /\ branchName = "default"
    /\ chunkSafety = "safe"
    /\ outputScope = "in_scope"
    /\ completedInputs # {}
OutOfScopeOutputsNeverComplete ==
  outputScope = "out_of_scope" =>
    /\ completedInputs = {}
    /\ ~graphWrites
    /\ ~globalProgressAdvanced
LockHeldOnlyAfterAcquire == lockHeld => lockAcquired
MutationsRequireAcquiredLock ==
  (completedInputs # {}
   \/ failedInputs # {}
   \/ graphWrites
   \/ globalProgressAdvanced) => lockAcquired

ChunkProgressRetrySafety ==
  /\ ChunkBounded
  /\ ProcessedWithinCommandLimit
  /\ CompletedOnlyFromKnownInputs
  /\ CompletedInputsWereProcessed
  /\ FailedInputsWereProcessed
  /\ FailedInputsRemainRetryable
  /\ NoInputBothCompleteAndFailed
  /\ FailedChunkBlocksGlobalProgress
  /\ GlobalProgressOnlyWhenComplete
  /\ ConcurrentInputsRemainOutsideCommandWork
  /\ PinnedFrontierSafety
  /\ GraphWritesRequireAcceptedWork
  /\ OutOfScopeOutputsNeverComplete
  /\ LockHeldOnlyAfterAcquire
  /\ MutationsRequireAcquiredLock

TargetedEnricherBackfillContract ==
  /\ TypeOK
  /\ TargetedBackfillIsolation
  /\ ChunkProgressRetrySafety

====
