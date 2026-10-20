------------------- MODULE DeploymentBackupPolicy -------------------
EXTENDS Naturals, TLC

Phases == {
    "preflight", "refused", "archive_ready", "full_verifying",
    "full_verified", "routine_admitted", "mutating", "complete"
}

EffectFailures == {
    "none",
    "target_revision_unpinned",
    "planner_untrusted",
    "plan_unreadable",
    "pending_migration",
    "extension_change",
    "catalog_changed",
    "nomos_changed",
    "smoke_source_unversioned",
    "read_model_not_rebuildable",
    "external_effect"
}

\* Effects whose undo is impossible or incomplete, so the run must hold a
\* current verified full restore before it mutates.  "external_effect" is one
\* of these, not a refusal: a smoke publish that calls an external provider
\* cannot roll those calls back, but it can still roll back its database.
KnownPhysicalEffects == {
    "pending_migration", "extension_change", "catalog_changed",
    "nomos_changed", "external_effect"
}

\* Claims about whether the target plan can be trusted, read, and rebuilt.
\* A failure here means no archive can make the mutation safe.
RefusedEffects == {
    "target_revision_unpinned", "planner_untrusted", "plan_unreadable",
    "smoke_source_unversioned", "read_model_not_rebuildable"
}

RecoveryFailures == {
    "none",
    "typed_schema",
    "deployment_scoped_path",
    "private_mode",
    "current_owner",
    "archive_digest_bound",
    "same_archive_restored",
    "archive_fresh",
    "restore_proof_fresh",
    "deployment_equal",
    "host_equal",
    "port_equal",
    "database_equal",
    "database_owner_equal",
    "cluster_system_identifier_equal",
    "server_version_equal",
    "dump_tool_major_equal",
    "restore_tool_major_equal",
    "backup_tool_executable_identity_equal",
    "extension_versions_equal",
    "timescale_restore_proven_or_risk_admitted",
    "schema_lineage_equal",
    "recovery_point_objective_declared"
}

NoEffectFailure == "none"
NoRecoveryFailure == "none"

VARIABLES
    phase,
    policyModeRiskAdmitted,
    releaseChannelDev,
    readinessProfileDogfood,
    deploymentClassDogfood,
    destructiveConfirmed,
    effectFailure,
    recoveryFailure,
    planStable,
    archiveValid,
    fullRestoreVerified,
    routineAdmitted,
    mutated

vars == <<
    phase, policyModeRiskAdmitted, releaseChannelDev,
    readinessProfileDogfood, deploymentClassDogfood,
    destructiveConfirmed, effectFailure, recoveryFailure, planStable,
    archiveValid, fullRestoreVerified, routineAdmitted, mutated
>>

DogfoodPosture ==
    /\ policyModeRiskAdmitted
    /\ releaseChannelDev
    /\ readinessProfileDogfood
    /\ deploymentClassDogfood
    /\ ~destructiveConfirmed

ClosedRoutineEffects == effectFailure = NoEffectFailure
RecoveryCompatible == recoveryFailure = NoRecoveryFailure
TrustedReadablePlan == effectFailure \notin RefusedEffects

RoutineEligible ==
    /\ DogfoodPosture
    /\ ClosedRoutineEffects
    /\ planStable

CurrentPhysicalBackupRequired ==
    \/ ~DogfoodPosture
    \/ effectFailure \in KnownPhysicalEffects

Init ==
    /\ phase = "preflight"
    /\ policyModeRiskAdmitted \in BOOLEAN
    /\ releaseChannelDev \in BOOLEAN
    /\ readinessProfileDogfood \in BOOLEAN
    /\ deploymentClassDogfood \in BOOLEAN
    /\ destructiveConfirmed \in BOOLEAN
    /\ effectFailure \in EffectFailures
    /\ recoveryFailure \in RecoveryFailures
    /\ planStable \in BOOLEAN
    /\ archiveValid = FALSE
    /\ fullRestoreVerified = FALSE
    /\ routineAdmitted = FALSE
    /\ mutated = FALSE

RefuseUntrustedOrDriftedPlan ==
    /\ phase = "preflight"
    /\ (~TrustedReadablePlan \/ ~planStable)
    /\ phase' = "refused"
    /\ UNCHANGED <<policyModeRiskAdmitted, releaseChannelDev,
        readinessProfileDogfood, deploymentClassDogfood,
        destructiveConfirmed, effectFailure, recoveryFailure, planStable,
        archiveValid, fullRestoreVerified, routineAdmitted, mutated>>

RefuseRoutineRecoveryGap ==
    /\ phase = "preflight"
    /\ RoutineEligible
    /\ ~RecoveryCompatible
    /\ phase' = "refused"
    /\ UNCHANGED <<policyModeRiskAdmitted, releaseChannelDev,
        readinessProfileDogfood, deploymentClassDogfood,
        destructiveConfirmed, effectFailure, recoveryFailure, planStable,
        archiveValid, fullRestoreVerified, routineAdmitted, mutated>>

AdmitRoutineNoPhysicalRollback ==
    /\ phase = "preflight"
    /\ RoutineEligible
    /\ RecoveryCompatible
    /\ phase' = "routine_admitted"
    /\ routineAdmitted' = TRUE
    /\ UNCHANGED <<policyModeRiskAdmitted, releaseChannelDev,
        readinessProfileDogfood, deploymentClassDogfood,
        destructiveConfirmed, effectFailure, recoveryFailure, planStable,
        archiveValid, fullRestoreVerified, mutated>>

CreateCurrentArchive ==
    /\ phase = "preflight"
    /\ TrustedReadablePlan
    /\ planStable
    /\ CurrentPhysicalBackupRequired
    /\ phase' = "archive_ready"
    /\ archiveValid' \in BOOLEAN
    /\ UNCHANGED <<policyModeRiskAdmitted, releaseChannelDev,
        readinessProfileDogfood, deploymentClassDogfood,
        destructiveConfirmed, effectFailure, recoveryFailure, planStable,
        fullRestoreVerified, routineAdmitted, mutated>>

RefuseInvalidArchive ==
    /\ phase = "archive_ready"
    /\ ~archiveValid
    /\ phase' = "refused"
    /\ UNCHANGED <<policyModeRiskAdmitted, releaseChannelDev,
        readinessProfileDogfood, deploymentClassDogfood,
        destructiveConfirmed, effectFailure, recoveryFailure, planStable,
        archiveValid, fullRestoreVerified, routineAdmitted, mutated>>

SelectCurrentFullRestore ==
    /\ phase = "archive_ready"
    /\ archiveValid
    /\ phase' = "full_verifying"
    /\ UNCHANGED <<policyModeRiskAdmitted, releaseChannelDev,
        readinessProfileDogfood, deploymentClassDogfood,
        destructiveConfirmed, effectFailure, recoveryFailure, planStable,
        archiveValid, fullRestoreVerified, routineAdmitted, mutated>>

VerifyCurrentFullRestore ==
    /\ phase = "full_verifying"
    /\ archiveValid
    /\ phase' = "full_verified"
    /\ fullRestoreVerified' = TRUE
    /\ UNCHANGED <<policyModeRiskAdmitted, releaseChannelDev,
        readinessProfileDogfood, deploymentClassDogfood,
        destructiveConfirmed, effectFailure, recoveryFailure, planStable,
        archiveValid, routineAdmitted, mutated>>

RejectCurrentFullRestore ==
    /\ phase = "full_verifying"
    /\ phase' = "refused"
    /\ UNCHANGED <<policyModeRiskAdmitted, releaseChannelDev,
        readinessProfileDogfood, deploymentClassDogfood,
        destructiveConfirmed, effectFailure, recoveryFailure, planStable,
        archiveValid, fullRestoreVerified, routineAdmitted, mutated>>

IntroduceTargetPlanDrift ==
    /\ phase \in {"routine_admitted", "full_verified"}
    /\ planStable
    /\ planStable' = FALSE
    /\ routineAdmitted' = FALSE
    /\ UNCHANGED <<phase, policyModeRiskAdmitted, releaseChannelDev,
        readinessProfileDogfood, deploymentClassDogfood,
        destructiveConfirmed, effectFailure, recoveryFailure,
        archiveValid, fullRestoreVerified, mutated>>

RefuseTargetPlanDrift ==
    /\ phase \in {"routine_admitted", "full_verified"}
    /\ ~planStable
    /\ phase' = "refused"
    /\ UNCHANGED <<policyModeRiskAdmitted, releaseChannelDev,
        readinessProfileDogfood, deploymentClassDogfood,
        destructiveConfirmed, effectFailure, recoveryFailure, planStable,
        archiveValid, fullRestoreVerified, routineAdmitted, mutated>>

BeginMutation ==
    /\ phase \in {"full_verified", "routine_admitted"}
    /\ planStable
    /\ IF phase = "full_verified"
          THEN /\ archiveValid /\ fullRestoreVerified
          ELSE /\ routineAdmitted
               /\ RoutineEligible
               /\ RecoveryCompatible
    /\ phase' = "mutating"
    /\ mutated' = TRUE
    /\ UNCHANGED <<policyModeRiskAdmitted, releaseChannelDev,
        readinessProfileDogfood, deploymentClassDogfood,
        destructiveConfirmed, effectFailure, recoveryFailure, planStable,
        archiveValid, fullRestoreVerified, routineAdmitted>>

Complete ==
    /\ phase = "mutating"
    /\ phase' = "complete"
    /\ UNCHANGED <<policyModeRiskAdmitted, releaseChannelDev,
        readinessProfileDogfood, deploymentClassDogfood,
        destructiveConfirmed, effectFailure, recoveryFailure, planStable,
        archiveValid, fullRestoreVerified, routineAdmitted, mutated>>

TerminalStutter == /\ phase \in {"refused", "complete"} /\ UNCHANGED vars

Next ==
    \/ RefuseUntrustedOrDriftedPlan
    \/ RefuseRoutineRecoveryGap
    \/ AdmitRoutineNoPhysicalRollback
    \/ CreateCurrentArchive
    \/ RefuseInvalidArchive
    \/ SelectCurrentFullRestore
    \/ VerifyCurrentFullRestore
    \/ RejectCurrentFullRestore
    \/ IntroduceTargetPlanDrift
    \/ RefuseTargetPlanDrift
    \/ BeginMutation
    \/ Complete
    \/ TerminalStutter

TypeInvariant ==
    /\ phase \in Phases
    /\ policyModeRiskAdmitted \in BOOLEAN
    /\ releaseChannelDev \in BOOLEAN
    /\ readinessProfileDogfood \in BOOLEAN
    /\ deploymentClassDogfood \in BOOLEAN
    /\ destructiveConfirmed \in BOOLEAN
    /\ effectFailure \in EffectFailures
    /\ recoveryFailure \in RecoveryFailures
    /\ planStable \in BOOLEAN
    /\ archiveValid \in BOOLEAN
    /\ fullRestoreVerified \in BOOLEAN
    /\ routineAdmitted \in BOOLEAN
    /\ mutated \in BOOLEAN

MutationRequiresRecoveryEvidence ==
    mutated => (fullRestoreVerified \/ (routineAdmitted /\ RecoveryCompatible))
CurrentRestoreRequiresCurrentArchive == fullRestoreVerified => archiveValid
PhysicalPlanMutationRequiresCurrentRestore ==
    (mutated /\ CurrentPhysicalBackupRequired) => fullRestoreVerified
RoutineAdmissionRequiresClosedEffects ==
    routineAdmitted => (RoutineEligible /\ RecoveryCompatible)
\* Every effect must be classified exactly once.  An effect in NEITHER set is
\* silently ignored: no action refuses it and no action forces an archive, so
\* the two invariants below hold VACUOUSLY and TLC still passes.  That is the
\* shape a future "tidy up the classification" edit produces, and it is the
\* only thing here that catches it - the mirror of
\* test_effect_classification_partitions_every_field on the Python side.
EffectClassificationPartitionsFailures ==
    /\ KnownPhysicalEffects \cap RefusedEffects = {}
    /\ KnownPhysicalEffects \cup RefusedEffects = EffectFailures \ {NoEffectFailure}
UntrustedPlanNeverMutates ==
    (effectFailure \in RefusedEffects) => ~mutated
\* The external-effect path is open, but only through a current verified full
\* restore.  Stated separately from PhysicalPlanMutationRequiresCurrentRestore
\* so the guarantee that replaced the old refusal is named, and so a future
\* edit that reclassifies "external_effect" cannot quietly weaken it.
ExternalEffectMutationRequiresCurrentRestore ==
    (mutated /\ effectFailure = "external_effect") => fullRestoreVerified
ExternalEffectNeverTakesRoutineAdmission ==
    (effectFailure = "external_effect") => ~routineAdmitted
TargetPlanDriftNeverMutates == ~planStable => ~mutated
MissingRoutineRecoveryNeverMutates ==
    (RoutineEligible /\ ~RecoveryCompatible) => ~mutated
NoPhysicalRollbackSkipsCurrentArchive == routineAdmitted => ~archiveValid

Spec == Init /\ [][Next]_vars

=====================================================================
