---------------------- MODULE DeploymentPromotion ----------------------
EXTENDS Naturals, FiniteSets, TLC

Phases == {
    "candidate",
    "staging_ready",
    "failed_staging",
    "approved",
    "locked",
    "fenced",
    "drained",
    "backup_created",
    "backup_invalid",
    "backup_verified",
    "mutating",
    "production_ready",
    "rollback_required",
    "database_restored",
    "previous_release_active",
    "previous_ready",
    "product_admitted",
    "completed",
    "rolled_back",
    "recovery_blocked"
}

Artifacts == {"old", "candidate"}
DatabaseVersions == {"old", "candidate"}
PromotionIds == {"promotion_a", "promotion_b"}
LockOwnerSets == SUBSET PromotionIds
RecoveryFailures == {"none", "restore", "reactivate", "readiness"}
MutationBoundaries == {
    "none",
    "release",
    "database",
    "policy",
    "services",
    "publish",
    "readiness"
}
RecoveryPhases == {
    "backup_invalid",
    "rollback_required",
    "database_restored",
    "previous_release_active",
    "previous_ready"
}
TerminalPhases == {
    "failed_staging",
    "product_admitted",
    "completed",
    "rolled_back",
    "recovery_blocked"
}
WriterOpenPhases == {
    "candidate", "staging_ready", "failed_staging", "approved", "locked",
    "product_admitted", "completed", "rolled_back"
}
ActiveMutationPhases == {
    "mutating", "production_ready", "rollback_required", "database_restored",
    "previous_release_active", "previous_ready", "recovery_blocked"
}

VARIABLES
    phase,
    stagingReady,
    lockHeld,
    lockOwners,
    writersFenced,
    queuesDrained,
    backupCreated,
    backupVerified,
    backupRejected,
    recoveryFailure,
    productionMutated,
    mutationBoundary,
    activeArtifact,
    databaseVersion,
    previousReady,
    writersOpen

vars == <<
    phase,
    stagingReady,
    lockHeld,
    lockOwners,
    writersFenced,
    queuesDrained,
    backupCreated,
    backupVerified,
    backupRejected,
    recoveryFailure,
    productionMutated,
    mutationBoundary,
    activeArtifact,
    databaseVersion,
    previousReady,
    writersOpen
>>

Init ==
    /\ phase = "candidate"
    /\ stagingReady = FALSE
    /\ lockHeld = FALSE
    /\ lockOwners = {}
    /\ writersFenced = FALSE
    /\ queuesDrained = FALSE
    /\ backupCreated = FALSE
    /\ backupVerified = FALSE
    /\ backupRejected = FALSE
    /\ recoveryFailure \in RecoveryFailures
    /\ productionMutated = FALSE
    /\ mutationBoundary = "none"
    /\ activeArtifact = "old"
    /\ databaseVersion = "old"
    /\ previousReady = TRUE
    /\ writersOpen = TRUE

StagePass ==
    /\ phase = "candidate"
    /\ phase' = "staging_ready"
    /\ stagingReady' = TRUE
    /\ UNCHANGED <<
        lockHeld, lockOwners, writersFenced, queuesDrained, backupCreated,
        backupVerified, backupRejected, recoveryFailure, productionMutated, mutationBoundary,
        activeArtifact, databaseVersion, previousReady, writersOpen
        >>

StageFail ==
    /\ phase = "candidate"
    /\ phase' = "failed_staging"
    /\ UNCHANGED <<
        stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupCreated, backupVerified, backupRejected, recoveryFailure, productionMutated,
        mutationBoundary, activeArtifact, databaseVersion, previousReady,
        writersOpen
        >>

Approve ==
    /\ phase = "staging_ready"
    /\ stagingReady
    /\ phase' = "approved"
    /\ UNCHANGED <<
        stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupCreated, backupVerified, backupRejected, recoveryFailure, productionMutated,
        mutationBoundary, activeArtifact, databaseVersion, previousReady,
        writersOpen
        >>

AcquireLock ==
    /\ phase = "approved"
    /\ ~lockHeld
    /\ lockOwners = {}
    /\ \E promotion \in PromotionIds : lockOwners' = {promotion}
    /\ phase' = "locked"
    /\ lockHeld' = TRUE
    /\ UNCHANGED <<
        stagingReady, writersFenced, queuesDrained, backupCreated,
        backupVerified, backupRejected, recoveryFailure, productionMutated, mutationBoundary,
        activeArtifact, databaseVersion, previousReady, writersOpen
        >>

FenceWriters ==
    /\ phase = "locked"
    /\ lockHeld /\ Cardinality(lockOwners) = 1
    /\ phase' = "fenced"
    /\ writersFenced' = TRUE
    /\ writersOpen' = FALSE
    /\ UNCHANGED <<
        stagingReady, lockHeld, lockOwners, queuesDrained, backupCreated,
        backupVerified, backupRejected, recoveryFailure, productionMutated, mutationBoundary,
        activeArtifact, databaseVersion, previousReady
        >>

DrainQueues ==
    /\ phase = "fenced"
    /\ writersFenced
    /\ phase' = "drained"
    /\ queuesDrained' = TRUE
    /\ UNCHANGED <<
        stagingReady, lockHeld, lockOwners, writersFenced, backupCreated,
        backupVerified, backupRejected, recoveryFailure, productionMutated, mutationBoundary,
        activeArtifact, databaseVersion, previousReady, writersOpen
        >>

CreateBackup ==
    /\ phase = "drained"
    /\ lockHeld /\ writersFenced /\ queuesDrained
    /\ phase' = "backup_created"
    /\ backupCreated' = TRUE
    /\ UNCHANGED <<
        stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupVerified, backupRejected, recoveryFailure, productionMutated, mutationBoundary,
        activeArtifact, databaseVersion, previousReady, writersOpen
        >>

VerifyBackup ==
    /\ phase = "backup_created"
    /\ backupCreated
    /\ phase' = "backup_verified"
    /\ backupVerified' = TRUE
    /\ UNCHANGED <<
        stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupCreated, backupRejected, recoveryFailure, productionMutated, mutationBoundary,
        activeArtifact, databaseVersion, previousReady, writersOpen
        >>

RejectBackup ==
    /\ phase = "backup_created"
    /\ backupCreated
    /\ phase' = "backup_invalid"
    /\ backupRejected' = TRUE
    /\ previousReady' = FALSE
    /\ UNCHANGED <<
        stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupCreated, backupVerified, recoveryFailure, productionMutated, mutationBoundary,
        activeArtifact, databaseVersion, writersOpen
        >>

ActivateCandidate ==
    /\ phase = "backup_verified"
    /\ stagingReady /\ lockHeld /\ Cardinality(lockOwners) = 1
    /\ writersFenced /\ queuesDrained /\ backupVerified
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

ApplyDatabaseMutation ==
    /\ phase = "mutating"
    /\ mutationBoundary = "release"
    /\ activeArtifact = "candidate"
    /\ databaseVersion' = "candidate"
    /\ mutationBoundary' = "database"
    /\ UNCHANGED <<
        phase, stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupCreated, backupVerified, backupRejected, recoveryFailure, productionMutated,
        activeArtifact, previousReady, writersOpen
        >>

ActivatePolicy ==
    /\ phase = "mutating"
    /\ mutationBoundary = "database"
    /\ mutationBoundary' = "policy"
    /\ UNCHANGED <<
        phase, stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupCreated, backupVerified, backupRejected, recoveryFailure, productionMutated,
        activeArtifact, databaseVersion, previousReady, writersOpen
        >>

RestartServices ==
    /\ phase = "mutating"
    /\ mutationBoundary = "policy"
    /\ mutationBoundary' = "services"
    /\ UNCHANGED <<
        phase, stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupCreated, backupVerified, backupRejected, recoveryFailure, productionMutated,
        activeArtifact, databaseVersion, previousReady, writersOpen
        >>

PublishGeneration ==
    /\ phase = "mutating"
    /\ mutationBoundary = "services"
    /\ mutationBoundary' = "publish"
    /\ UNCHANGED <<
        phase, stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupCreated, backupVerified, backupRejected, recoveryFailure, productionMutated,
        activeArtifact, databaseVersion, previousReady, writersOpen
        >>

CheckProductionReadiness ==
    /\ phase = "mutating"
    /\ mutationBoundary = "publish"
    /\ mutationBoundary' = "readiness"
    /\ UNCHANGED <<
        phase, stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupCreated, backupVerified, backupRejected, recoveryFailure, productionMutated,
        activeArtifact, databaseVersion, previousReady, writersOpen
        >>

ProductionReady ==
    /\ phase = "mutating"
    /\ mutationBoundary = "readiness"
    /\ activeArtifact = "candidate"
    /\ databaseVersion = "candidate"
    /\ phase' = "production_ready"
    /\ UNCHANGED <<
        stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupCreated, backupVerified, backupRejected, recoveryFailure, productionMutated,
        mutationBoundary, activeArtifact, databaseVersion, previousReady,
        writersOpen
        >>

Complete ==
    /\ phase = "production_ready"
    /\ activeArtifact = "candidate"
    /\ databaseVersion = "candidate"
    /\ phase' = "completed"
    /\ lockHeld' = FALSE
    /\ lockOwners' = {}
    /\ writersFenced' = FALSE
    /\ writersOpen' = TRUE
    /\ UNCHANGED <<
        stagingReady, queuesDrained, backupCreated, backupVerified,
        backupRejected, recoveryFailure, productionMutated, mutationBoundary, activeArtifact,
        databaseVersion, previousReady
        >>

RequireRollback ==
    /\ phase \in {"mutating", "production_ready"}
    /\ productionMutated
    /\ mutationBoundary \in MutationBoundaries \ {"none"}
    /\ phase' = "rollback_required"
    /\ UNCHANGED <<
        stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupCreated, backupVerified, backupRejected, recoveryFailure, productionMutated,
        mutationBoundary, activeArtifact, databaseVersion, previousReady,
        writersOpen
        >>

RestoreDatabase ==
    /\ phase = "rollback_required"
    /\ recoveryFailure # "restore"
    /\ backupVerified /\ writersFenced /\ ~writersOpen
    /\ phase' = "database_restored"
    /\ databaseVersion' = "old"
    /\ UNCHANGED <<
        stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupCreated, backupVerified, backupRejected, recoveryFailure, productionMutated,
        mutationBoundary, activeArtifact, previousReady, writersOpen
        >>

ReactivatePrevious ==
    /\ phase = "database_restored"
    /\ recoveryFailure # "reactivate"
    /\ databaseVersion = "old"
    /\ phase' = "previous_release_active"
    /\ activeArtifact' = "old"
    /\ UNCHANGED <<
        stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupCreated, backupVerified, backupRejected, recoveryFailure, productionMutated,
        mutationBoundary, databaseVersion, previousReady, writersOpen
        >>

ProvePreviousReady ==
    /\ phase = "previous_release_active"
    /\ recoveryFailure # "readiness"
    /\ activeArtifact = "old" /\ databaseVersion = "old"
    /\ phase' = "previous_ready"
    /\ previousReady' = TRUE
    /\ UNCHANGED <<
        stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupCreated, backupVerified, backupRejected, recoveryFailure, productionMutated,
        mutationBoundary, activeArtifact, databaseVersion, writersOpen
        >>

ProveUnmutatedPreviousReady ==
    /\ phase = "backup_invalid"
    /\ recoveryFailure # "readiness"
    /\ ~productionMutated
    /\ activeArtifact = "old" /\ databaseVersion = "old"
    /\ phase' = "previous_ready"
    /\ previousReady' = TRUE
    /\ UNCHANGED <<
        stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupCreated, backupVerified, backupRejected, recoveryFailure, productionMutated,
        mutationBoundary, activeArtifact, databaseVersion, writersOpen
        >>

CompleteRollback ==
    /\ phase = "previous_ready"
    /\ previousReady
    /\ activeArtifact = "old" /\ databaseVersion = "old"
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

BlockRecovery ==
    /\ \/ phase = "rollback_required" /\ recoveryFailure = "restore"
       \/ phase = "database_restored" /\ recoveryFailure = "reactivate"
       \/ phase \in {"backup_invalid", "previous_release_active"}
          /\ recoveryFailure = "readiness"
    /\ writersFenced /\ ~writersOpen
    /\ phase' = "recovery_blocked"
    /\ UNCHANGED <<
        stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupCreated, backupVerified, backupRejected, recoveryFailure, productionMutated,
        mutationBoundary, activeArtifact, databaseVersion, previousReady,
        writersOpen
        >>

AbortBeforeMutation ==
    /\ phase \in {"locked", "fenced", "drained", "backup_created", "backup_verified"}
    /\ ~productionMutated
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

TerminalStutter ==
    /\ phase \in TerminalPhases
    /\ UNCHANGED vars

RecoveryAdvance ==
    RestoreDatabase
    \/ ReactivatePrevious
    \/ ProvePreviousReady
    \/ ProveUnmutatedPreviousReady
    \/ CompleteRollback
    \/ BlockRecovery

LegacyNext ==
    \/ StagePass
    \/ StageFail
    \/ Approve
    \/ AcquireLock
    \/ FenceWriters
    \/ DrainQueues
    \/ CreateBackup
    \/ VerifyBackup
    \/ RejectBackup
    \/ ActivateCandidate
    \/ ApplyDatabaseMutation
    \/ ActivatePolicy
    \/ RestartServices
    \/ PublishGeneration
    \/ CheckProductionReadiness
    \/ ProductionReady
    \/ Complete
    \/ RequireRollback
    \/ RecoveryAdvance
    \/ AbortBeforeMutation
    \/ TerminalStutter

AdmitProductFirstInstall ==
    /\ phase = "candidate"
    /\ phase' = "product_admitted"
    /\ UNCHANGED <<
        stagingReady, lockHeld, lockOwners, writersFenced, queuesDrained,
        backupCreated, backupVerified, backupRejected, recoveryFailure,
        productionMutated, mutationBoundary, activeArtifact, databaseVersion,
        previousReady, writersOpen
        >>

Next ==
    \/ LegacyNext
    \/ AdmitProductFirstInstall

TypeInvariant ==
    /\ phase \in Phases
    /\ stagingReady \in BOOLEAN
    /\ lockHeld \in BOOLEAN
    /\ lockOwners \in LockOwnerSets
    /\ writersFenced \in BOOLEAN
    /\ queuesDrained \in BOOLEAN
    /\ backupCreated \in BOOLEAN
    /\ backupVerified \in BOOLEAN
    /\ backupRejected \in BOOLEAN
    /\ recoveryFailure \in RecoveryFailures
    /\ productionMutated \in BOOLEAN
    /\ mutationBoundary \in MutationBoundaries
    /\ activeArtifact \in Artifacts
    /\ databaseVersion \in DatabaseVersions
    /\ previousReady \in BOOLEAN
    /\ writersOpen \in BOOLEAN

NoProductionMutationBeforeStagingReady ==
    productionMutated => stagingReady

OneActivePromotion ==
    /\ Cardinality(lockOwners) <= 1
    /\ (lockHeld \equiv Cardinality(lockOwners) = 1)

MutationRequiresVerifiedPostFenceBackup ==
    productionMutated =>
        backupVerified /\ backupCreated /\ queuesDrained

MutationBoundaryRequiresLockAndFence ==
    /\ phase \in ActiveMutationPhases
    /\ productionMutated
    => lockHeld /\ writersFenced /\ backupVerified

WritersOpenOnlyInStableState ==
    writersOpen =>
        /\ ~writersFenced
        /\ phase \in WriterOpenPhases

CompletedUsesApprovedArtifact ==
    phase = "completed" =>
        activeArtifact = "candidate"
        /\ databaseVersion = "candidate"
        /\ mutationBoundary = "readiness"

RollbackReopensOnlyPreviousState ==
    phase = "rolled_back" =>
        activeArtifact = "old" /\ databaseVersion = "old" /\ previousReady

PreviousArtifactRequiresRestoredDatabase ==
    phase \in {"previous_release_active", "previous_ready", "rolled_back"} =>
        activeArtifact = "old" /\ databaseVersion = "old"

InvalidBackupNeverMutates ==
    backupRejected =>
        ~productionMutated /\ activeArtifact = "old" /\ databaseVersion = "old"

InvalidBackupRetainsFenceUntilReady ==
    backupRejected /\ phase # "rolled_back" =>
        lockHeld /\ Cardinality(lockOwners) = 1 /\ writersFenced /\ ~writersOpen

RecoveryBlockedKeepsWritersFenced ==
    phase = "recovery_blocked" =>
        lockHeld /\ Cardinality(lockOwners) = 1 /\ writersFenced /\ ~writersOpen

ProductAdmissionNeverMutates ==
    phase = "product_admitted" =>
        /\ ~lockHeld
        /\ lockOwners = {}
        /\ ~writersFenced
        /\ writersOpen
        /\ ~queuesDrained
        /\ ~backupCreated
        /\ ~backupVerified
        /\ ~productionMutated
        /\ mutationBoundary = "none"
        /\ activeArtifact = "old"
        /\ databaseVersion = "old"

RecoveryConverges ==
    phase \in RecoveryPhases ~>
        phase \in {"rolled_back", "recovery_blocked"}

RecoveryWithoutInjectedFailureSucceeds ==
    phase \in RecoveryPhases /\ recoveryFailure = "none" ~>
        phase = "rolled_back"

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(RecoveryAdvance)

=============================================================================
