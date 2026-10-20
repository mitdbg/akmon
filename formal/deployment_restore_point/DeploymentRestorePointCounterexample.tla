-------------------- MODULE DeploymentRestorePointCounterexample --------------------
EXTENDS DeploymentRestorePoint

UnsafeMutateWithoutRestore ==
    /\ outcome = "running"
    /\ active = "none"
    /\ mutated' = TRUE
    /\ UNCHANGED <<
        active, completed, terminalReceipts, uncertainPhase, outcome,
        targetSubstituted, safetyPlanExact, targetConfirmed,
        externalEffect, externalConfirmed, skipBackup, restoreReceiptExact,
        restoreVerified, providerEffectMayHaveHappened,
        providerEffectReversible, providerReplay, disclosureVisible,
        phaseStatus, attemptGuarantees, exactObservationPhase,
        exactObservationKind>>

UnsafeMutateSubstitutedTarget ==
    /\ outcome = "planning"
    /\ targetSubstituted
    /\ targetConfirmed = FALSE
    /\ restoreVerified' = TRUE
    /\ mutated' = TRUE
    /\ outcome' = "running"
    /\ UNCHANGED <<
        active, completed, terminalReceipts, uncertainPhase,
        targetSubstituted, safetyPlanExact, targetConfirmed,
        externalEffect, externalConfirmed, skipBackup, restoreReceiptExact,
        providerEffectMayHaveHappened, providerEffectReversible,
        providerReplay, disclosureVisible, phaseStatus, attemptGuarantees,
        exactObservationPhase, exactObservationKind>>

UnsafeReplayExternalSmoke ==
    /\ outcome = "recovering"
    /\ uncertainPhase = "smoke_publish"
    /\ externalEffect
    /\ providerEffectMayHaveHappened
    /\ providerReplay' = TRUE
    /\ UNCHANGED <<
        active, completed, terminalReceipts, uncertainPhase, outcome,
        targetSubstituted, safetyPlanExact, targetConfirmed,
        externalEffect, externalConfirmed, skipBackup, restoreReceiptExact,
        restoreVerified, mutated, providerEffectMayHaveHappened,
        providerEffectReversible, disclosureVisible, phaseStatus,
        attemptGuarantees, exactObservationPhase, exactObservationKind>>

UnsafeSkipBackupAuthority ==
    /\ outcome = "running"
    /\ skipBackup
    /\ ~restoreReceiptExact
    /\ mutated' = TRUE
    /\ UNCHANGED <<
        active, completed, terminalReceipts, uncertainPhase, outcome,
        targetSubstituted, safetyPlanExact, targetConfirmed,
        externalEffect, externalConfirmed, skipBackup, restoreReceiptExact,
        restoreVerified, providerEffectMayHaveHappened,
        providerEffectReversible, providerReplay, disclosureVisible,
        phaseStatus, attemptGuarantees, exactObservationPhase,
        exactObservationKind>>

UnsafeHideExternalEffect ==
    /\ outcome = "planning"
    /\ externalEffect
    /\ disclosureVisible' = FALSE
    /\ providerEffectReversible' = TRUE
    /\ UNCHANGED <<
        active, completed, terminalReceipts, uncertainPhase, outcome,
        targetSubstituted, safetyPlanExact, targetConfirmed,
        externalEffect, externalConfirmed, skipBackup, restoreReceiptExact,
        restoreVerified, mutated, providerEffectMayHaveHappened,
        providerReplay, phaseStatus, attemptGuarantees,
        exactObservationPhase, exactObservationKind>>

MissingRestoreSpec == Init /\ [][Next \/ UnsafeMutateWithoutRestore]_vars
TargetSubstitutionSpec == Init /\ [][Next \/ UnsafeMutateSubstitutedTarget]_vars
ExternalReplaySpec == Init /\ [][Next \/ UnsafeReplayExternalSmoke]_vars
SkipBackupSpec == Init /\ [][Next \/ UnsafeSkipBackupAuthority]_vars
ExternalDisclosureSpec == Init /\ [][Next \/ UnsafeHideExternalEffect]_vars

IndependentHandoffUnreachable ==
    ~(skipBackup /\ restoreReceiptExact /\ mutated)

====================================================================
