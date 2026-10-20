---------------- MODULE DeploymentPromotionCounterexample ----------------
EXTENDS DeploymentPromotion

\* Removed backup-verification guard: production can activate directly after
\* drain with no verified backup. TLC must refute the real backup invariant.
UnsafeActivateWithoutVerifiedBackup ==
    /\ phase = "drained"
    /\ phase' = "mutating"
    /\ productionMutated' = TRUE
    /\ mutationBoundary' = "release"
    /\ activeArtifact' = "candidate"
    /\ previousReady' = FALSE
    /\ UNCHANGED <<
        stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupCreated, backupVerified, backupRejected, recoveryFailure, databaseVersion,
        writersOpen
        >>

\* Removed readiness guard: rollback reopens writers immediately after the old
\* release is active, before previous-version readiness has been proved.
UnsafeReopenBeforePreviousReady ==
    /\ phase = "previous_release_active"
    /\ ~previousReady
    /\ phase' = "rolled_back"
    /\ lockHeld' = FALSE
    /\ lockOwners' = {}
    /\ writersFenced' = FALSE
    /\ writersOpen' = TRUE
    /\ UNCHANGED <<
        stagingReady, queuesDrained, backupCreated, backupVerified,
        backupRejected, recoveryFailure, productionMutated, mutationBoundary, activeArtifact,
        databaseVersion, previousReady
        >>

\* Removed lock-acquisition guard: a second promotion joins the owner set while
\* the first promotion still owns the production transition.
UnsafeConcurrentAcquire ==
    /\ phase \in {"locked", "fenced", "drained", "backup_created"}
    /\ Cardinality(lockOwners) = 1
    /\ \E promotion \in PromotionIds \ lockOwners :
        lockOwners' = lockOwners \cup {promotion}
    /\ UNCHANGED <<
        phase, stagingReady, lockHeld, writersFenced, queuesDrained,
        backupCreated, backupVerified, backupRejected, recoveryFailure,
        productionMutated, mutationBoundary, activeArtifact, databaseVersion,
        previousReady, writersOpen
        >>

\* Removed restore-order guard: the old release can be activated while the
\* candidate database is still live. TLC must refute compatibility safety.
UnsafeReactivateBeforeDatabaseRestore ==
    /\ phase = "rollback_required"
    /\ databaseVersion = "candidate"
    /\ phase' = "previous_release_active"
    /\ activeArtifact' = "old"
    /\ UNCHANGED <<
        stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupCreated, backupVerified, backupRejected, recoveryFailure, productionMutated,
        mutationBoundary, databaseVersion, previousReady, writersOpen
        >>

BackupGuardNext == Next \/ UnsafeActivateWithoutVerifiedBackup
WriterReadyGuardNext == Next \/ UnsafeReopenBeforePreviousReady
RestoreOrderGuardNext == Next \/ UnsafeReactivateBeforeDatabaseRestore
LockGuardNext == Next \/ UnsafeConcurrentAcquire

BackupGuardSpec == Init /\ [][BackupGuardNext]_vars
WriterReadyGuardSpec == Init /\ [][WriterReadyGuardNext]_vars
RestoreOrderGuardSpec == Init /\ [][RestoreOrderGuardNext]_vars
LockGuardSpec == Init /\ [][LockGuardNext]_vars

=============================================================================
