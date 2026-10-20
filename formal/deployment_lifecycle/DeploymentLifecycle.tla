-------------------- MODULE DeploymentLifecycle --------------------
EXTENDS FiniteSets, Naturals, TLC

WorkPhases == {
    "preflight", "backup", "quiesce", "migration",
    "catalog", "smoke", "readiness"
}
Phases == WorkPhases \cup {"none", "cleanup"}
MutationPhases == {"migration", "catalog"}
EffectPhases == {
    "backup", "quiesce", "migration", "catalog", "smoke", "readiness"
}
InputKinds == {
    "preflight_input", "backup_input", "migration_input",
    "catalog_input", "smoke_input", "readiness_input"
}
Resources == {"child", "disposable_database", "temporary_archive", "service"}
Outcomes == {"running", "recovering", "blocked", "succeeded", "failed"}
CleanOutcomes == {"succeeded", "failed"}

Prerequisites(p) ==
    CASE p = "preflight" -> {}
      [] p = "backup" -> {"preflight"}
      [] p = "quiesce" -> {"preflight", "backup"}
      [] p = "migration" -> {"preflight", "backup", "quiesce"}
      [] p = "catalog" -> {"preflight", "backup", "quiesce", "migration"}
      [] p = "smoke" -> {
            "preflight", "backup", "quiesce", "migration", "catalog"
         }
      [] p = "readiness" -> WorkPhases \ {"readiness"}

Invalidates(inputKind) ==
    CASE inputKind = "preflight_input" -> WorkPhases
      [] inputKind = "backup_input" -> WorkPhases \ {"preflight"}
      [] inputKind = "migration_input" -> {
            "migration", "catalog", "smoke", "readiness"
         }
      [] inputKind = "catalog_input" -> {"catalog", "smoke", "readiness"}
      [] inputKind = "smoke_input" -> {"smoke", "readiness"}
      [] inputKind = "readiness_input" -> {"readiness"}

VARIABLES
    active,
    validCompleted,
    everCompleted,
    invalidatedEver,
    allowedInvalidations,
    replayedCompleted,
    priorServiceRunning,
    serviceRunning,
    childRunning,
    disposableDatabase,
    temporaryArchive,
    cleanupObligations,
    receiptObserved,
    receiptPhase,
    uncertainPhase,
    recoveryNeeded,
    unsafeDisruption,
    unsafeMutation,
    outcome

vars == <<
    active, validCompleted, everCompleted, invalidatedEver,
    allowedInvalidations, replayedCompleted, priorServiceRunning,
    serviceRunning, childRunning, disposableDatabase, temporaryArchive,
    cleanupObligations, receiptObserved, receiptPhase, uncertainPhase, recoveryNeeded,
    unsafeDisruption, unsafeMutation, outcome
>>

DirtyResources ==
    \/ serviceRunning # priorServiceRunning
    \/ childRunning
    \/ disposableDatabase
    \/ temporaryArchive
    \/ cleanupObligations # {}

Init ==
    /\ active = "none"
    /\ validCompleted = {}
    /\ everCompleted = {}
    /\ invalidatedEver = {}
    /\ allowedInvalidations = {}
    /\ replayedCompleted = {}
    /\ priorServiceRunning \in BOOLEAN
    /\ serviceRunning = priorServiceRunning
    /\ childRunning = FALSE
    /\ disposableDatabase = FALSE
    /\ temporaryArchive = FALSE
    /\ cleanupObligations = {}
    /\ receiptObserved = FALSE
    /\ receiptPhase = "none"
    /\ uncertainPhase = "none"
    /\ recoveryNeeded = FALSE
    /\ unsafeDisruption = FALSE
    /\ unsafeMutation = FALSE
    /\ outcome = "running"

StartPhase(p) ==
    /\ outcome = "running"
    /\ active = "none"
    /\ uncertainPhase = "none"
    /\ p \in WorkPhases
    /\ p \notin validCompleted
    /\ Prerequisites(p) \subseteq validCompleted
    /\ active' = p
    /\ receiptObserved' = FALSE
    /\ receiptPhase' = "none"
    /\ replayedCompleted' =
        IF p \in everCompleted
        THEN replayedCompleted \cup {p}
        ELSE replayedCompleted
    /\ childRunning' = IF p = "backup" THEN TRUE ELSE childRunning
    /\ disposableDatabase' =
        IF p = "backup" THEN TRUE ELSE disposableDatabase
    /\ temporaryArchive' =
        IF p = "backup" THEN TRUE ELSE temporaryArchive
    /\ serviceRunning' = IF p = "quiesce" THEN FALSE ELSE serviceRunning
    /\ cleanupObligations' =
        IF p = "backup"
        THEN cleanupObligations \cup {
            "child", "disposable_database", "temporary_archive"
        }
        ELSE IF p = "quiesce" /\ priorServiceRunning
             THEN cleanupObligations \cup {"service"}
             ELSE cleanupObligations
    /\ unsafeDisruption' =
        unsafeDisruption \/ (p \in EffectPhases /\ "preflight" \notin validCompleted)
    /\ UNCHANGED <<
        validCompleted, everCompleted, invalidatedEver,
        allowedInvalidations, priorServiceRunning, uncertainPhase,
        recoveryNeeded, unsafeMutation, outcome>>

FinishPhase(p) ==
    /\ outcome = "running"
    /\ active = p
    /\ p \in WorkPhases
    /\ active' = "none"
    /\ validCompleted' = validCompleted \cup {p}
    /\ everCompleted' = everCompleted \cup {p}
    /\ receiptObserved' = TRUE
    /\ receiptPhase' = p
    /\ unsafeMutation' =
        unsafeMutation \/ (p \in MutationPhases /\ "preflight" \notin validCompleted)
    /\ childRunning' = IF p = "backup" THEN FALSE ELSE childRunning
    /\ disposableDatabase' =
        IF p = "backup" THEN FALSE ELSE disposableDatabase
    /\ temporaryArchive' =
        IF p = "backup" THEN FALSE ELSE temporaryArchive
    /\ cleanupObligations' =
        IF p = "backup"
        THEN cleanupObligations \ {"child", "disposable_database", "temporary_archive"}
        ELSE cleanupObligations
    /\ UNCHANGED <<
        invalidatedEver, allowedInvalidations, replayedCompleted,
        priorServiceRunning, serviceRunning, uncertainPhase, recoveryNeeded,
        unsafeDisruption, outcome>>

InjectFailure ==
    /\ outcome = "running"
    /\ active \in WorkPhases
    /\ uncertainPhase' = active
    /\ active' = "none"
    /\ recoveryNeeded' = TRUE
    /\ outcome' = "recovering"
    /\ UNCHANGED <<
        validCompleted, everCompleted, invalidatedEver,
        allowedInvalidations, replayedCompleted, priorServiceRunning,
        serviceRunning, childRunning, disposableDatabase, temporaryArchive,
        cleanupObligations, receiptObserved, receiptPhase,
        unsafeDisruption, unsafeMutation>>

ObserveUncertainIncomplete ==
    /\ outcome = "recovering"
    /\ uncertainPhase \in WorkPhases
    /\ uncertainPhase' = "none"
    /\ UNCHANGED <<
        active, validCompleted, everCompleted, invalidatedEver,
        allowedInvalidations, replayedCompleted, priorServiceRunning,
        serviceRunning, childRunning, disposableDatabase, temporaryArchive,
        cleanupObligations, receiptObserved, receiptPhase, recoveryNeeded, unsafeDisruption,
        unsafeMutation, outcome>>

ObserveUncertainComplete ==
    /\ outcome = "recovering"
    /\ uncertainPhase \in WorkPhases
    /\ receiptObserved
    /\ receiptPhase = uncertainPhase
    /\ validCompleted' = validCompleted \cup {uncertainPhase}
    /\ everCompleted' = everCompleted \cup {uncertainPhase}
    /\ uncertainPhase' = "none"
    /\ UNCHANGED <<
        active, invalidatedEver, allowedInvalidations, replayedCompleted,
        priorServiceRunning, serviceRunning, childRunning,
        disposableDatabase, temporaryArchive, cleanupObligations,
        receiptObserved, receiptPhase,
        recoveryNeeded, unsafeDisruption, unsafeMutation, outcome>>

\* A restart may inspect the durable effect/target and write a receipt while
\* the phase is still uncertain.  Completion is permitted only after this
\* matching observation; it is not inferred from the interruption itself.
RecordUncertainReceipt ==
    /\ outcome = "recovering"
    /\ uncertainPhase \in WorkPhases
    /\ receiptObserved' = TRUE
    /\ receiptPhase' = uncertainPhase
    /\ UNCHANGED <<
        active, validCompleted, everCompleted, invalidatedEver,
        allowedInvalidations, replayedCompleted, priorServiceRunning,
        serviceRunning, childRunning, disposableDatabase, temporaryArchive,
        cleanupObligations, uncertainPhase, recoveryNeeded,
        unsafeDisruption, unsafeMutation, outcome>>

ChangeInput(inputKind) ==
    /\ inputKind \in InputKinds
    /\ active = "none"
    /\ uncertainPhase = "none"
    /\ outcome \in {"running", "failed"}
    /\ validCompleted' = validCompleted \ Invalidates(inputKind)
    /\ invalidatedEver' = invalidatedEver \cup Invalidates(inputKind)
    /\ allowedInvalidations' = allowedInvalidations \cup Invalidates(inputKind)
    /\ UNCHANGED <<
        active, everCompleted, replayedCompleted, priorServiceRunning,
        serviceRunning, childRunning, disposableDatabase, temporaryArchive,
        cleanupObligations, receiptObserved, receiptPhase, uncertainPhase, recoveryNeeded,
        unsafeDisruption, unsafeMutation, outcome>>

Cancel ==
    /\ outcome = "running"
    /\ active \in WorkPhases
    /\ uncertainPhase' = active
    /\ active' = "none"
    /\ recoveryNeeded' = TRUE
    /\ outcome' = "recovering"
    /\ UNCHANGED <<
        validCompleted, everCompleted, invalidatedEver,
        allowedInvalidations, replayedCompleted, priorServiceRunning,
        serviceRunning, childRunning, disposableDatabase, temporaryArchive,
        cleanupObligations, receiptObserved, receiptPhase,
        unsafeDisruption, unsafeMutation>>

BeginCleanup ==
    /\ active = "none"
    /\ uncertainPhase = "none"
    /\ outcome \in {"running", "recovering"}
    /\ recoveryNeeded \/ "readiness" \in validCompleted
    /\ active' = "cleanup"
    /\ UNCHANGED <<
        validCompleted, everCompleted, invalidatedEver,
        allowedInvalidations, replayedCompleted, priorServiceRunning,
        serviceRunning, childRunning, disposableDatabase, temporaryArchive,
        cleanupObligations, receiptObserved, receiptPhase, uncertainPhase, recoveryNeeded,
        unsafeDisruption, unsafeMutation, outcome>>

FinishCleanup ==
    /\ active = "cleanup"
    /\ active' = "none"
    /\ serviceRunning' = priorServiceRunning
    /\ childRunning' = FALSE
    /\ disposableDatabase' = FALSE
    /\ temporaryArchive' = FALSE
    /\ cleanupObligations' = {}
    /\ recoveryNeeded' = FALSE
    /\ outcome' =
        IF "readiness" \in validCompleted /\ ~recoveryNeeded
        THEN "succeeded"
        ELSE "failed"
    /\ UNCHANGED <<
        validCompleted, everCompleted, invalidatedEver,
        allowedInvalidations, replayedCompleted, priorServiceRunning,
        uncertainPhase, receiptObserved, receiptPhase, unsafeDisruption, unsafeMutation>>

BlockCleanup ==
    /\ active = "cleanup"
    /\ DirtyResources
    /\ active' = "none"
    /\ outcome' = "blocked"
    /\ UNCHANGED <<
        validCompleted, everCompleted, invalidatedEver,
        allowedInvalidations, replayedCompleted, priorServiceRunning,
        serviceRunning, childRunning, disposableDatabase, temporaryArchive,
        cleanupObligations, receiptObserved, receiptPhase, uncertainPhase, recoveryNeeded,
        unsafeDisruption, unsafeMutation>>

RetryBlockedCleanup ==
    /\ outcome = "blocked"
    /\ active = "none"
    /\ active' = "cleanup"
    /\ recoveryNeeded' = TRUE
    /\ outcome' = "recovering"
    /\ UNCHANGED <<
        validCompleted, everCompleted, invalidatedEver,
        allowedInvalidations, replayedCompleted, priorServiceRunning,
        serviceRunning, childRunning, disposableDatabase, temporaryArchive,
        cleanupObligations, receiptObserved, receiptPhase, uncertainPhase,
        unsafeDisruption, unsafeMutation>>

ResumeFailed ==
    /\ outcome = "failed"
    /\ active = "none"
    /\ validCompleted' = validCompleted \ {"preflight"}
    /\ invalidatedEver' = invalidatedEver \cup {"preflight"}
    /\ allowedInvalidations' = allowedInvalidations \cup {"preflight"}
    /\ outcome' = "running"
    /\ UNCHANGED <<
        active, everCompleted, replayedCompleted, priorServiceRunning,
        serviceRunning, childRunning, disposableDatabase, temporaryArchive,
        cleanupObligations, receiptObserved, receiptPhase, uncertainPhase, recoveryNeeded,
        unsafeDisruption, unsafeMutation>>

TerminalStutter ==
    /\ outcome = "succeeded"
    /\ UNCHANGED vars

Next ==
    \/ \E p \in WorkPhases : StartPhase(p)
    \/ \E p \in WorkPhases : FinishPhase(p)
    \/ InjectFailure
    \/ Cancel
    \/ ObserveUncertainIncomplete
    \/ ObserveUncertainComplete
    \/ RecordUncertainReceipt
    \/ \E inputKind \in InputKinds : ChangeInput(inputKind)
    \/ BeginCleanup
    \/ FinishCleanup
    \/ BlockCleanup
    \/ RetryBlockedCleanup
    \/ ResumeFailed
    \/ TerminalStutter

TypeInvariant ==
    /\ active \in Phases
    /\ validCompleted \subseteq WorkPhases
    /\ everCompleted \subseteq WorkPhases
    /\ invalidatedEver \subseteq WorkPhases
    /\ allowedInvalidations \subseteq WorkPhases
    /\ replayedCompleted \subseteq WorkPhases
    /\ priorServiceRunning \in BOOLEAN
    /\ serviceRunning \in BOOLEAN
    /\ childRunning \in BOOLEAN
    /\ disposableDatabase \in BOOLEAN
    /\ temporaryArchive \in BOOLEAN
    /\ cleanupObligations \subseteq Resources
    /\ receiptObserved \in BOOLEAN
    /\ receiptPhase \in WorkPhases \cup {"none"}
    /\ uncertainPhase \in WorkPhases \cup {"none"}
    /\ recoveryNeeded \in BOOLEAN
    /\ unsafeDisruption \in BOOLEAN
    /\ unsafeMutation \in BOOLEAN
    /\ outcome \in Outcomes
    /\ validCompleted \subseteq everCompleted

NoDisruptiveEffectBeforePreflight == ~unsafeDisruption
NoMutationBeforePreflight == ~unsafeMutation
CompletedPhaseReplayRequiresInvalidation ==
    replayedCompleted \subseteq invalidatedEver
InputChangeInvalidatesOnlyDependents ==
    invalidatedEver \subseteq allowedInvalidations
CleanTerminalRestoresPriorServiceState ==
    outcome \in CleanOutcomes => serviceRunning = priorServiceRunning
CleanTerminalOwnsNoDisposableResources ==
    outcome \in CleanOutcomes =>
        /\ ~childRunning
        /\ ~disposableDatabase
        /\ ~temporaryArchive
        /\ cleanupObligations = {}
UnknownEffectCannotBeClean ==
    uncertainPhase # "none" => outcome \notin CleanOutcomes
UncertainCompletionRequiresReceipt ==
    uncertainPhase # "none" => ~receiptObserved \/ receiptPhase = uncertainPhase
CleanupFailureCannotBeClean ==
    outcome = "blocked" => /\ outcome \notin CleanOutcomes /\ DirtyResources

CancellationRequiresCompensation ==
    outcome = "recovering" => recoveryNeeded

Spec == Init /\ [][Next]_vars

====================================================================
