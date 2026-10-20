-------------------- MODULE DeploymentRestorePoint --------------------
EXTENDS FiniteSets, Naturals, TLC

RuntimePhases == {
    "preflight", "backup", "stop_services", "sync_code",
    "dependency_sync", "migrate", "catalog_apply", "nomos_baseline",
    "generation_recovery", "attribute_read_model", "restart_services",
    "source_convergence", "smoke_publish", "readiness"
}
MutationPhases == {
    "migrate", "catalog_apply", "nomos_baseline", "generation_recovery",
    "attribute_read_model", "smoke_publish"
}
ProviderMutationPhases == {"smoke_publish"}
EffectfulPhases == RuntimePhases \ {"preflight", "readiness"}
RetryableAfterObservation == RuntimePhases \ ProviderMutationPhases
Outcomes == {"planning", "running", "recovering", "blocked", "succeeded"}
AttemptStatuses == {
    "not_started", "active", "uncertain", "succeeded", "failed",
    "skipped", "superseded"
}
SkippablePhases == {"stop_services", "sync_code", "dependency_sync", "migrate", "restart_services"}

Prerequisites(p) ==
    CASE p = "preflight" -> {}
      [] p = "backup" -> {"preflight"}
      [] p = "stop_services" -> {"preflight", "backup"}
      [] p = "sync_code" -> {"preflight", "backup", "stop_services"}
      [] p = "dependency_sync" -> {
            "preflight", "backup", "stop_services", "sync_code"
         }
      [] p = "migrate" -> {
            "preflight", "backup", "stop_services", "sync_code",
            "dependency_sync"
         }
      [] p = "catalog_apply" -> {
            "preflight", "backup", "stop_services", "sync_code",
            "dependency_sync", "migrate"
         }
      [] p = "nomos_baseline" -> {
            "preflight", "backup", "stop_services", "sync_code",
            "dependency_sync", "migrate", "catalog_apply"
         }
      [] p = "generation_recovery" -> {
            "preflight", "backup", "stop_services", "sync_code",
            "dependency_sync", "migrate", "catalog_apply", "nomos_baseline"
         }
      [] p = "attribute_read_model" -> {
            "preflight", "backup", "stop_services", "sync_code",
            "dependency_sync", "migrate", "catalog_apply", "nomos_baseline",
            "generation_recovery"
         }
      [] p = "restart_services" -> {
            "preflight", "backup", "stop_services", "sync_code",
            "dependency_sync", "migrate", "catalog_apply", "nomos_baseline",
            "generation_recovery", "attribute_read_model"
         }
      [] p = "source_convergence" -> {
            "preflight", "backup", "stop_services", "sync_code",
            "dependency_sync", "migrate", "catalog_apply", "nomos_baseline",
            "generation_recovery", "attribute_read_model", "restart_services"
         }
      [] p = "smoke_publish" -> RuntimePhases \ {"smoke_publish", "readiness"}
      [] p = "readiness" -> RuntimePhases \ {"readiness"}

VARIABLES
    active,
    completed,
    terminalReceipts,
    uncertainPhase,
    outcome,
    targetSubstituted,
    safetyPlanExact,
    targetConfirmed,
    externalEffect,
    externalConfirmed,
    skipBackup,
    restoreReceiptExact,
    restoreVerified,
    mutated,
    providerEffectMayHaveHappened,
    providerEffectReversible,
    providerReplay,
    disclosureVisible,
    phaseStatus,
    attemptGuarantees,
    exactObservationPhase,
    exactObservationKind

vars == <<
    active, completed, terminalReceipts, uncertainPhase, outcome,
    targetSubstituted, safetyPlanExact, targetConfirmed,
    externalEffect, externalConfirmed, skipBackup, restoreReceiptExact,
    restoreVerified, mutated, providerEffectMayHaveHappened,
    providerEffectReversible, providerReplay, disclosureVisible,
    phaseStatus, attemptGuarantees, exactObservationPhase,
    exactObservationKind
>>

TargetAuthorized == ~targetSubstituted \/ (safetyPlanExact /\ targetConfirmed)
ExternalAuthorized == ~externalEffect \/ (safetyPlanExact /\ externalConfirmed)
ConfirmationRequired == targetSubstituted \/ externalEffect
SafetyConfirmed == ~ConfirmationRequired \/ (TargetAuthorized /\ ExternalAuthorized)
MutationAuthorized == TargetAuthorized /\ ExternalAuthorized /\ restoreVerified
EffectMayHaveHappened(p) ==
    p \in EffectfulPhases /\ phaseStatus[p] \in {"active", "failed", "uncertain"}

Init ==
    /\ active = "none"
    /\ completed = {}
    /\ terminalReceipts = {}
    /\ uncertainPhase = "none"
    /\ outcome = "planning"
    /\ targetSubstituted \in BOOLEAN
    /\ safetyPlanExact \in BOOLEAN
    /\ targetConfirmed = FALSE
    /\ externalEffect \in BOOLEAN
    /\ externalConfirmed = FALSE
    /\ skipBackup \in BOOLEAN
    /\ restoreReceiptExact \in BOOLEAN
    /\ restoreVerified = FALSE
    /\ mutated = FALSE
    /\ providerEffectMayHaveHappened = FALSE
    /\ providerEffectReversible = FALSE
    /\ providerReplay = FALSE
    /\ disclosureVisible = externalEffect
    /\ phaseStatus = [p \in RuntimePhases |-> "not_started"]
    /\ attemptGuarantees = {}
    /\ exactObservationPhase = "none"
    /\ exactObservationKind = "none"

ConfirmSafetyPlan ==
    /\ outcome = "planning"
    /\ safetyPlanExact
    /\ targetConfirmed' = targetSubstituted
    /\ externalConfirmed' = externalEffect
    /\ UNCHANGED <<
        active, completed, terminalReceipts, uncertainPhase, outcome,
        targetSubstituted, safetyPlanExact, externalEffect, skipBackup,
        restoreReceiptExact, restoreVerified, mutated,
        providerEffectMayHaveHappened, providerEffectReversible,
        providerReplay, disclosureVisible, phaseStatus, attemptGuarantees,
        exactObservationPhase, exactObservationKind>>

BeginRun ==
    /\ outcome = "planning"
    /\ SafetyConfirmed
    /\ outcome' = "running"
    /\ UNCHANGED <<
        active, completed, terminalReceipts, uncertainPhase,
        targetSubstituted, safetyPlanExact, targetConfirmed,
        externalEffect, externalConfirmed, skipBackup, restoreReceiptExact,
        restoreVerified, mutated, providerEffectMayHaveHappened,
        providerEffectReversible, providerReplay, disclosureVisible,
        phaseStatus, attemptGuarantees, exactObservationPhase,
        exactObservationKind>>

AdmitIndependentRestore ==
    /\ outcome \in {"planning", "running"}
    /\ active = "none"
    /\ uncertainPhase = "none"
    /\ restoreReceiptExact
    /\ restoreVerified' = TRUE
    /\ completed' = completed \cup {"preflight", "backup"}
    /\ terminalReceipts' = terminalReceipts \cup {"preflight", "backup"}
    /\ phaseStatus' = [phaseStatus EXCEPT
        !["preflight"] = "succeeded",
        !["backup"] = "succeeded"]
    /\ attemptGuarantees' = attemptGuarantees \cup {"preflight", "backup"}
    /\ UNCHANGED <<
        active, uncertainPhase, outcome,
        targetSubstituted, safetyPlanExact, targetConfirmed,
        externalEffect, externalConfirmed, skipBackup, restoreReceiptExact,
        mutated, providerEffectMayHaveHappened, providerEffectReversible,
        providerReplay, disclosureVisible, exactObservationPhase,
        exactObservationKind>>

StartPhase(p) ==
    /\ outcome = "running"
    /\ active = "none"
    /\ uncertainPhase = "none"
    /\ p \in RuntimePhases
    /\ p \notin completed
    /\ Prerequisites(p) \subseteq completed
    /\ (p = "backup" => ~skipBackup)
    /\ (p = "backup" => SafetyConfirmed)
    /\ (p \in MutationPhases => MutationAuthorized)
    /\ ~(p = "smoke_publish" /\ externalEffect /\ providerEffectMayHaveHappened)
    /\ active' = p
    /\ phaseStatus' = [phaseStatus EXCEPT ![p] = "active"]
    /\ attemptGuarantees' = attemptGuarantees \cup {p}
    /\ providerEffectMayHaveHappened' =
        IF p = "smoke_publish" /\ externalEffect
        THEN TRUE
        ELSE providerEffectMayHaveHappened
    /\ providerReplay' =
        IF p = "smoke_publish" /\ externalEffect /\ providerEffectMayHaveHappened
        THEN TRUE
        ELSE providerReplay
    /\ UNCHANGED <<
        completed, terminalReceipts, uncertainPhase, outcome,
        targetSubstituted, safetyPlanExact, targetConfirmed,
        externalEffect, externalConfirmed, skipBackup, restoreReceiptExact,
        restoreVerified, mutated, providerEffectReversible, disclosureVisible,
        exactObservationPhase, exactObservationKind>>

FinishPhase(p) ==
    /\ outcome = "running"
    /\ active = p
    /\ p \in RuntimePhases
    /\ (p = "backup" => restoreReceiptExact)
    /\ active' = "none"
    /\ completed' = completed \cup {p}
    /\ terminalReceipts' = terminalReceipts \cup {p}
    /\ phaseStatus' = [phaseStatus EXCEPT ![p] = "succeeded"]
    /\ restoreVerified' = IF p = "backup" THEN TRUE ELSE restoreVerified
    /\ mutated' = IF p \in MutationPhases THEN TRUE ELSE mutated
    /\ outcome' = IF p = "readiness" THEN "succeeded" ELSE outcome
    /\ UNCHANGED <<
        uncertainPhase, targetSubstituted, safetyPlanExact, targetConfirmed,
        externalEffect, externalConfirmed, skipBackup, restoreReceiptExact,
        providerEffectMayHaveHappened, providerEffectReversible,
        providerReplay, disclosureVisible, attemptGuarantees,
        exactObservationPhase, exactObservationKind>>

SkipPhase(p) ==
    /\ outcome = "running"
    /\ active = "none"
    /\ uncertainPhase = "none"
    /\ p \in SkippablePhases
    /\ p \notin completed
    /\ Prerequisites(p) \subseteq completed
    /\ completed' = completed \cup {p}
    /\ terminalReceipts' = terminalReceipts \cup {p}
    /\ phaseStatus' = [phaseStatus EXCEPT ![p] = "skipped"]
    /\ attemptGuarantees' = attemptGuarantees \cup {p}
    /\ UNCHANGED <<
        active, uncertainPhase, outcome, targetSubstituted, safetyPlanExact,
        targetConfirmed, externalEffect, externalConfirmed, skipBackup,
        restoreReceiptExact, restoreVerified, mutated,
        providerEffectMayHaveHappened, providerEffectReversible,
        providerReplay, disclosureVisible, exactObservationPhase,
        exactObservationKind>>

RecordTerminalFailure ==
    /\ outcome = "running"
    /\ active \in RuntimePhases
    /\ phaseStatus' = [phaseStatus EXCEPT ![active] = "failed"]
    /\ active' = "none"
    /\ outcome' = "blocked"
    /\ UNCHANGED <<
        completed, terminalReceipts, uncertainPhase, targetSubstituted, safetyPlanExact,
        targetConfirmed, externalEffect, externalConfirmed, skipBackup,
        restoreReceiptExact, restoreVerified, mutated,
        providerEffectMayHaveHappened, providerEffectReversible,
        providerReplay, disclosureVisible, attemptGuarantees,
        exactObservationPhase, exactObservationKind>>

Interrupt ==
    /\ outcome = "running"
    /\ active \in RuntimePhases
    /\ uncertainPhase' = active
    /\ phaseStatus' = [phaseStatus EXCEPT ![active] = "uncertain"]
    /\ active' = "none"
    /\ outcome' = "recovering"
    /\ UNCHANGED <<
        completed, terminalReceipts, targetSubstituted, safetyPlanExact,
        targetConfirmed, externalEffect, externalConfirmed, skipBackup,
        restoreReceiptExact, restoreVerified, mutated,
        providerEffectMayHaveHappened, providerEffectReversible,
        providerReplay, disclosureVisible, attemptGuarantees,
        exactObservationPhase, exactObservationKind>>

RecordExactTerminalObservation ==
    /\ outcome = "recovering"
    /\ uncertainPhase \in RuntimePhases
    /\ uncertainPhase \in attemptGuarantees
    /\ safetyPlanExact
    /\ (uncertainPhase = "backup" => restoreReceiptExact)
    /\ (uncertainPhase \in MutationPhases => restoreVerified)
    /\ exactObservationPhase' = uncertainPhase
    /\ exactObservationKind' = "matching_terminal_receipt"
    /\ UNCHANGED <<
        active, completed, terminalReceipts, uncertainPhase, outcome,
        targetSubstituted, safetyPlanExact, targetConfirmed,
        externalEffect, externalConfirmed, skipBackup, restoreReceiptExact,
        restoreVerified, mutated, providerEffectMayHaveHappened,
        providerEffectReversible, providerReplay, disclosureVisible,
        phaseStatus, attemptGuarantees>>

RecordExactNoEffectObservation ==
    /\ outcome = "recovering"
    /\ uncertainPhase \in RetryableAfterObservation
    /\ uncertainPhase \in attemptGuarantees
    /\ safetyPlanExact
    /\ (uncertainPhase \in MutationPhases => restoreVerified)
    /\ exactObservationPhase' = uncertainPhase
    /\ exactObservationKind' = "matching_no_effect_proof"
    /\ UNCHANGED <<
        active, completed, terminalReceipts, uncertainPhase, outcome,
        targetSubstituted, safetyPlanExact, targetConfirmed,
        externalEffect, externalConfirmed, skipBackup, restoreReceiptExact,
        restoreVerified, mutated, providerEffectMayHaveHappened,
        providerEffectReversible, providerReplay, disclosureVisible,
        phaseStatus, attemptGuarantees>>

ObserveExactTerminalReceipt ==
    /\ outcome = "recovering"
    /\ uncertainPhase \in RuntimePhases
    /\ exactObservationPhase = uncertainPhase
    /\ exactObservationKind = "matching_terminal_receipt"
    /\ (uncertainPhase = "backup" => restoreReceiptExact)
    /\ terminalReceipts' = terminalReceipts \cup {uncertainPhase}
    /\ completed' = completed \cup {uncertainPhase}
    /\ phaseStatus' = [phaseStatus EXCEPT ![uncertainPhase] = "succeeded"]
    /\ restoreVerified' =
        IF uncertainPhase = "backup" THEN TRUE ELSE restoreVerified
    /\ mutated' =
        IF uncertainPhase \in MutationPhases THEN TRUE ELSE mutated
    /\ uncertainPhase' = "none"
    /\ exactObservationPhase' = "none"
    /\ exactObservationKind' = "none"
    /\ outcome' = "running"
    /\ UNCHANGED <<
        active, targetSubstituted, safetyPlanExact, targetConfirmed,
        externalEffect, externalConfirmed, skipBackup, restoreReceiptExact,
        providerEffectMayHaveHappened, providerEffectReversible,
        providerReplay, disclosureVisible, attemptGuarantees>>

ObserveNoEffect ==
    /\ outcome = "recovering"
    /\ uncertainPhase \in RetryableAfterObservation
    /\ exactObservationPhase = uncertainPhase
    /\ exactObservationKind = "matching_no_effect_proof"
    /\ uncertainPhase' = "none"
    /\ exactObservationPhase' = "none"
    /\ exactObservationKind' = "none"
    /\ phaseStatus' = [phaseStatus EXCEPT ![uncertainPhase] = "superseded"]
    /\ outcome' = "running"
    /\ UNCHANGED <<
        active, completed, terminalReceipts, targetSubstituted,
        safetyPlanExact, targetConfirmed, externalEffect, externalConfirmed,
        skipBackup, restoreReceiptExact, restoreVerified, mutated,
        providerEffectMayHaveHappened, providerEffectReversible,
        providerReplay, disclosureVisible, attemptGuarantees>>

BlockUncertainExternal ==
    /\ outcome = "recovering"
    /\ uncertainPhase = "smoke_publish"
    /\ externalEffect
    /\ outcome' = "blocked"
    /\ uncertainPhase' = "none"
    /\ phaseStatus' = [phaseStatus EXCEPT !["smoke_publish"] = "failed"]
    /\ UNCHANGED <<
        active, completed, terminalReceipts,
        targetSubstituted, safetyPlanExact, targetConfirmed,
        externalEffect, externalConfirmed, skipBackup, restoreReceiptExact,
        restoreVerified, mutated, providerEffectMayHaveHappened,
        providerEffectReversible, providerReplay, disclosureVisible,
        attemptGuarantees, exactObservationPhase, exactObservationKind>>

DriftSafetyPlan ==
    /\ active = "none"
    /\ uncertainPhase = "none"
    /\ outcome \in {"planning", "running"}
    /\ ~mutated
    /\ safetyPlanExact' = FALSE
    /\ targetConfirmed' = FALSE
    /\ externalConfirmed' = FALSE
    /\ restoreVerified' = FALSE
    /\ completed' = {}
    /\ terminalReceipts' = {}
    /\ phaseStatus' = [p \in RuntimePhases |-> "not_started"]
    /\ attemptGuarantees' = {}
    /\ outcome' = "planning"
    /\ UNCHANGED <<
        active, uncertainPhase, targetSubstituted, externalEffect, skipBackup,
        restoreReceiptExact, mutated, providerEffectMayHaveHappened,
        providerEffectReversible, providerReplay, disclosureVisible,
        exactObservationPhase, exactObservationKind>>

DetectPostMutationDrift ==
    /\ active = "none"
    /\ uncertainPhase = "none"
    /\ outcome = "running"
    /\ mutated
    /\ outcome' = "blocked"
    /\ UNCHANGED <<
        active, completed, terminalReceipts, uncertainPhase,
        targetSubstituted, safetyPlanExact, targetConfirmed,
        externalEffect, externalConfirmed, skipBackup, restoreReceiptExact,
        restoreVerified, mutated, providerEffectMayHaveHappened,
        providerEffectReversible, providerReplay, disclosureVisible,
        phaseStatus, attemptGuarantees, exactObservationPhase,
        exactObservationKind>>

TerminalStutter ==
    /\ outcome \in {"blocked", "succeeded"}
    /\ UNCHANGED vars

Next ==
    \/ ConfirmSafetyPlan
    \/ BeginRun
    \/ AdmitIndependentRestore
    \/ \E p \in RuntimePhases : StartPhase(p)
    \/ \E p \in RuntimePhases : FinishPhase(p)
    \/ \E p \in SkippablePhases : SkipPhase(p)
    \/ RecordTerminalFailure
    \/ Interrupt
    \/ RecordExactTerminalObservation
    \/ RecordExactNoEffectObservation
    \/ ObserveExactTerminalReceipt
    \/ ObserveNoEffect
    \/ BlockUncertainExternal
    \/ DriftSafetyPlan
    \/ DetectPostMutationDrift
    \/ TerminalStutter

TypeInvariant ==
    /\ active \in RuntimePhases \cup {"none"}
    /\ completed \subseteq RuntimePhases
    /\ terminalReceipts \subseteq RuntimePhases
    /\ uncertainPhase \in RuntimePhases \cup {"none"}
    /\ outcome \in Outcomes
    /\ targetSubstituted \in BOOLEAN
    /\ safetyPlanExact \in BOOLEAN
    /\ targetConfirmed \in BOOLEAN
    /\ externalEffect \in BOOLEAN
    /\ externalConfirmed \in BOOLEAN
    /\ skipBackup \in BOOLEAN
    /\ restoreReceiptExact \in BOOLEAN
    /\ restoreVerified \in BOOLEAN
    /\ (restoreVerified => restoreReceiptExact)
    /\ mutated \in BOOLEAN
    /\ providerEffectMayHaveHappened \in BOOLEAN
    /\ providerEffectReversible \in BOOLEAN
    /\ providerReplay \in BOOLEAN
    /\ disclosureVisible \in BOOLEAN
    /\ phaseStatus \in [RuntimePhases -> AttemptStatuses]
    /\ attemptGuarantees \subseteq RuntimePhases
    /\ exactObservationPhase \in RuntimePhases \cup {"none"}
    /\ exactObservationKind \in {
        "none", "matching_terminal_receipt", "matching_no_effect_proof"
       }
    /\ (exactObservationPhase = "none" <=> exactObservationKind = "none")
    /\ completed \subseteq terminalReceipts
    /\ {p \in RuntimePhases : phaseStatus[p] # "not_started"}
       \subseteq attemptGuarantees
    /\ (active # "none" => phaseStatus[active] = "active")
    /\ (uncertainPhase # "none" => phaseStatus[uncertainPhase] = "uncertain")
    /\ \A p \in completed : phaseStatus[p] \in {"succeeded", "skipped"}

MutationRequiresExactTargetAndVerifiedRestore ==
    mutated =>
        TargetAuthorized /\ ExternalAuthorized
        /\ restoreVerified /\ restoreReceiptExact
TargetSubstitutionRequiresExplicitAcknowledgement ==
    mutated /\ targetSubstituted => safetyPlanExact /\ targetConfirmed
ExternalEffectNeverClaimsReversible ==
    externalEffect => disclosureVisible /\ ~providerEffectReversible
UncertainExternalEffectIsNeverReplayed == ~providerReplay
SkipBackupNeverAuthorizesMutation == skipBackup /\ ~restoreReceiptExact => ~mutated
UncertainPhaseCannotSucceed == uncertainPhase # "none" => outcome # "succeeded"
AttemptGuaranteesRecorded ==
    {p \in RuntimePhases : phaseStatus[p] # "not_started"}
    \subseteq attemptGuarantees
EffectUncertaintyIsRecorded ==
    \A p \in EffectfulPhases :
        phaseStatus[p] \in {"failed", "uncertain"} => EffectMayHaveHappened(p)

Spec == Init /\ [][Next]_vars

====================================================================
