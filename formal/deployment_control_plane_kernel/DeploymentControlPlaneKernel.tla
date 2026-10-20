------------------ MODULE DeploymentControlPlaneKernel ------------------
EXTENDS FiniteSets, Naturals, Sequences, TLC

CONSTANTS Environment, Staging, Production, FailureMode, ApprovalMode,
          EffectClass, None

AttemptId == "attempt-1"
WorkflowId == "workflow-attempt-1"
Artifact == "sealed-artifact"
Definition == "definition-v1"
Policy == "policy-v1"
Database == IF Environment = Staging THEN "staging-db" ELSE "production-db"
Context == IF Environment = Staging THEN "staging-context-1" ELSE "production-context-1"
SourceContext == "staging-context-1"
SourceGeneration == 1
TargetGeneration == 2
PredecessorGeneration == 0
ActivationReceipt == "activation-receipt-1"
RollbackReceipt == "activation-receipt-rollback-2"
PredecessorArtifact == "sealed-predecessor"
StagedDigest == "staged-digest-1"
CandidateLifecycleEpoch == 1
CandidateDatabaseWriterEpoch == 2
RollbackLifecycleEpoch == 2
RollbackDatabaseWriterEpoch == 3

LifecycleStates == {"absent", "planned", "admitted", "applying", "verifying",
                    "ready", "promoted", "recovering", "rolled_back", "blocked"}
TerminalStates == {"promoted", "rolled_back", "blocked"}
EffectPhases == {"absent", "planned", "intended", "applied", "observed", "committed", "failed"}
CompensationPhases == {"none", "intended", "applied", "observed", "compensated", "failed"}
CompensationPlans == {"absent", "exact", "partial", "conflicting"}
CompensationOrders == {"none", "reverse", "forward"}
PossiblyAppliedPhases == {"intended", "applied", "observed", "committed"}
OutboxPhases == {"absent", "intended", "delivered", "refused"}
ApprovalStates == {"absent", "issued", "consumed", "revoked", "expired"}
EffectClasses == {"reversible", "irreversible"}
FailureModes == {"success", "admission_refusal", "reversible_failure",
                 "irreversible_ambiguity", "irreversible_postcommit_failure",
                 "compensation_failure", "recover_admitted", "recover_applying",
                 "recover_verifying", "recover_ready"}
ApprovalModes == {"none", "valid", "stale", "expired", "revoked", "consumed",
                  "unhealthy", "incomplete", "source_not_ready"}
ObservationValues == {"none", "exact", "conflicting"}
IdentityValues == {"reserved", "consumed"}
MutationResults == {"canonical_accepted", "legacy_accepted", "legacy_rejected",
                    "stale_epoch_rejected", "stopped_rejected", "stopped_accepted"}

LegalEdges == {
    <<"planned", "admitted">>, <<"planned", "recovering">>,
    <<"admitted", "applying">>, <<"admitted", "recovering">>,
    <<"applying", "verifying">>, <<"applying", "recovering">>,
    <<"verifying", "ready">>, <<"verifying", "recovering">>,
    <<"ready", "promoted">>, <<"ready", "recovering">>,
    <<"recovering", "rolled_back">>, <<"recovering", "blocked">>
}

VARIABLES environmentState, attemptIdentity, attemptState, outbox,
          effectState, compensationState, observations, identityClaims,
          healthResults, approvals, eventHistory, projectionReads,
          mutationHistory, logicalTime

vars == <<environmentState, attemptIdentity, attemptState, outbox,
          effectState, compensationState, observations, identityClaims,
          healthResults, approvals, eventHistory, projectionReads,
          mutationHistory, logicalTime>>

InitialApprovalState ==
    CASE ApprovalMode \in {"valid", "unhealthy", "incomplete", "source_not_ready"} -> "issued"
      [] ApprovalMode = "stale" -> "issued"
      [] ApprovalMode = "expired" -> "issued"
      [] ApprovalMode = "revoked" -> "revoked"
      [] ApprovalMode = "consumed" -> "consumed"
      [] OTHER -> "absent"

Init ==
    /\ Environment \in {Staging, Production}
    /\ Staging # Production
    /\ FailureMode \in FailureModes
    /\ ApprovalMode \in ApprovalModes
    /\ EffectClass \in EffectClasses
    /\ environmentState = [currentAttempt |-> None,
                           activeAttempts |-> {},
                           lifecycleEpoch |-> 0,
                           databaseWriterEpoch |-> 1,
                           ledgerAuthority |-> "external-controller-ledger",
                           currentContext |-> Context,
                           currentGeneration |-> 0,
                           stagedCapability |-> "absent",
                           stagedWrites |-> FALSE,
                           publishIntent |-> FALSE,
                           publishOutcome |-> FALSE,
                           readerVisible |-> FALSE,
                           visibilitySwitches |-> 0,
                           producersFrozen |-> FALSE,
                           activationReceipt |-> None,
                           activationEpoch |-> 0,
                           activationDatabaseWriterEpoch |-> 0,
                           activationCandidate |-> None,
                           activationStagedDigest |-> None,
                           activationPredecessor |-> 0,
                           targetDatabaseReceipt |-> None,
                           producerCapabilities |-> "absent",
                           finalObservation |-> FALSE,
                           authorizationRecovered |-> FALSE,
                           publishedUnobservedRecovered |-> FALSE,
                           invalidatedReceipts |-> {},
                           authorizationOnlyRollback |-> FALSE,
                           rollbackPhase |-> "none",
                           recoveryMode |-> "none",
                           stagingContext |-> SourceContext,
                           stagingGeneration |-> SourceGeneration,
                           stagingHealthSequence |-> 1,
                           stagingHealthy |-> ApprovalMode # "unhealthy",
                           stagingComplete |-> ApprovalMode # "incomplete",
                           stagingState |-> IF ApprovalMode = "source_not_ready" THEN "applying" ELSE "ready"]
    /\ attemptIdentity = [id |-> None, environment |-> Environment,
                          artifact |-> None, definition |-> None,
                          policy |-> None, database |-> None,
                          admissionContext |-> None, targetContext |-> None,
                          lifecycleEpoch |-> 0,
                          databaseWriterEpoch |-> 0]
    /\ attemptState = [lifecycle |-> "absent", version |-> 0,
                       workflowId |-> None, recoveryReason |-> None,
                       running |-> TRUE, crashUsed |-> FALSE,
                       crashLifecycle |-> None, crashEffectPhase |-> None,
                       crashCompensationPhase |-> None, crashVersion |-> 0,
                       crashActivationReceipt |-> None,
                       crashPublishOutcome |-> FALSE,
                       crashFinalObservation |-> FALSE]
    /\ outbox = [phase |-> "absent", workflowId |-> None]
    /\ effectState = [phase |-> "absent", kind |-> EffectClass,
                      observation |-> "none", predecessorsCommitted |-> FALSE]
    /\ compensationState = [phase |-> "none", observation |-> "none",
                              plan |-> "absent", required |-> FALSE,
                              forwardPhase |-> "absent", boundReceipt |-> None,
                              order |-> "none"]
    /\ observations = {}
    /\ identityClaims = {}
    /\ healthResults = [context |-> None, generation |-> 0,
                         complete |-> FALSE, ready |-> FALSE, sequence |-> 0]
    /\ approvals = [state |-> InitialApprovalState,
                     sourceContext |-> IF ApprovalMode = "stale" THEN "stale-context" ELSE SourceContext,
                     sourceGeneration |-> SourceGeneration,
                     sourceHealthSequence |-> 1,
                     sourceHealthy |-> ApprovalMode # "unhealthy",
                     sourceComplete |-> ApprovalMode # "incomplete",
                     sourceStagingState |-> IF ApprovalMode = "source_not_ready" THEN "applying" ELSE "ready",
                     targetContext |-> Context,
                     consumedBy |-> IF ApprovalMode = "consumed" THEN "older-attempt" ELSE None,
                     expiresAt |-> IF ApprovalMode = "expired" THEN 0 ELSE 2]
    /\ eventHistory = {}
    /\ projectionReads = {}
    /\ mutationHistory = {}
    /\ logicalTime = IF ApprovalMode = "expired" THEN 1 ELSE 0

ApprovalEligible ==
    /\ Environment = Production
    /\ approvals.state = "issued"
    /\ approvals.sourceContext = SourceContext
    /\ approvals.sourceGeneration = SourceGeneration
    /\ approvals.sourceHealthSequence = 1
    /\ approvals.sourceContext = environmentState.stagingContext
    /\ approvals.sourceGeneration = environmentState.stagingGeneration
    /\ approvals.sourceHealthSequence = environmentState.stagingHealthSequence
    /\ approvals.sourceHealthy = environmentState.stagingHealthy
    /\ approvals.sourceComplete = environmentState.stagingComplete
    /\ approvals.sourceStagingState = environmentState.stagingState
    /\ environmentState.stagingHealthy
    /\ environmentState.stagingComplete
    /\ environmentState.stagingState = "ready"
    /\ approvals.targetContext = Context
    /\ approvals.consumedBy = None
    /\ logicalTime < approvals.expiresAt

PlanAttempt ==
    /\ attemptState.lifecycle = "absent"
    /\ environmentState.currentAttempt = None
    /\ environmentState' = [environmentState EXCEPT
          !.currentAttempt = AttemptId,
          !.activeAttempts = {AttemptId}]
    /\ attemptIdentity' = [id |-> AttemptId, environment |-> Environment,
                            artifact |-> Artifact, definition |-> Definition,
                            policy |-> Policy, database |-> Database,
                            admissionContext |-> Context, targetContext |-> Context,
                            lifecycleEpoch |-> CandidateLifecycleEpoch,
                            databaseWriterEpoch |-> CandidateDatabaseWriterEpoch]
    /\ attemptState' = [attemptState EXCEPT !.lifecycle = "planned",
                                             !.version = 1,
                                             !.workflowId = WorkflowId]
    /\ outbox' = [phase |-> "intended", workflowId |-> WorkflowId]
    /\ effectState' = [effectState EXCEPT !.phase = "planned"]
    /\ UNCHANGED <<compensationState, observations, identityClaims,
                    healthResults, approvals, eventHistory, projectionReads,
                    mutationHistory, logicalTime>>

DeliverWorkflowStart ==
    /\ attemptState.lifecycle = "planned"
    /\ attemptState.running
    /\ outbox = [phase |-> "intended", workflowId |-> WorkflowId]
    /\ FailureMode # "admission_refusal"
    /\ Environment = Staging \/ ApprovalEligible
    /\ attemptState' = [attemptState EXCEPT !.lifecycle = "admitted", !.version = @ + 1]
    /\ outbox' = [outbox EXCEPT !.phase = "delivered"]
    /\ approvals' = IF Environment = Production
                     THEN [approvals EXCEPT !.state = "consumed", !.consumedBy = AttemptId]
                     ELSE approvals
    /\ eventHistory' = eventHistory \cup {<<"planned", "admitted">>}
    /\ UNCHANGED <<environmentState, attemptIdentity, effectState,
                    compensationState, observations, identityClaims, healthResults,
                    projectionReads, mutationHistory, logicalTime>>

RefuseWorkflowStart ==
    /\ attemptState.lifecycle = "planned"
    /\ attemptState.running
    /\ outbox.phase = "intended"
    /\ FailureMode = "admission_refusal" \/ (Environment = Production /\ ~ApprovalEligible)
    /\ attemptState' = [attemptState EXCEPT !.lifecycle = "recovering",
          !.version = @ + 1,
          !.recoveryReason = IF FailureMode = "admission_refusal"
                            THEN "admission_refusal" ELSE "approval_refusal"]
    /\ outbox' = [outbox EXCEPT !.phase = "refused"]
    /\ eventHistory' = eventHistory \cup {<<"planned", "recovering">>}
    /\ UNCHANGED <<environmentState, attemptIdentity, effectState,
                    compensationState, observations, identityClaims, healthResults,
                    approvals, projectionReads, mutationHistory, logicalTime>>

BeginApply ==
    /\ attemptState.lifecycle = "admitted"
    /\ attemptState.running
    /\ attemptState' = [attemptState EXCEPT !.lifecycle = "applying", !.version = @ + 1]
    /\ eventHistory' = eventHistory \cup {<<"admitted", "applying">>}
    /\ UNCHANGED <<environmentState, attemptIdentity, outbox, effectState,
                    compensationState, observations, identityClaims, healthResults,
                    approvals, projectionReads, mutationHistory, logicalTime>>

CommitPredecessors ==
    /\ attemptState.lifecycle = "applying"
    /\ attemptState.running
    /\ effectState.phase = "planned"
    /\ effectState' = [effectState EXCEPT !.predecessorsCommitted = TRUE]
    /\ UNCHANGED <<environmentState, attemptIdentity, attemptState, outbox,
                    compensationState, observations, identityClaims, healthResults,
                    approvals, eventHistory, projectionReads, mutationHistory,
                    logicalTime>>

IntentEffect ==
    /\ attemptState.lifecycle = "applying"
    /\ attemptState.running
    /\ effectState.phase = "planned"
    /\ effectState.kind = "reversible" \/ effectState.predecessorsCommitted
    /\ effectState' = [effectState EXCEPT !.phase = "intended"]
    /\ identityClaims' = identityClaims \cup {"reserved"}
    /\ UNCHANGED <<environmentState, attemptIdentity, attemptState, outbox,
                    compensationState, observations, healthResults, approvals,
                    eventHistory, projectionReads, mutationHistory, logicalTime>>

ApplyEffect ==
    /\ attemptState.lifecycle = "applying"
    /\ effectState.phase = "intended"
    /\ attemptState.running
    /\ effectState' = [effectState EXCEPT !.phase = "applied"]
    /\ UNCHANGED <<environmentState, attemptIdentity, attemptState, outbox,
                    compensationState, observations, identityClaims, healthResults,
                    approvals, eventHistory, projectionReads, mutationHistory,
                    logicalTime>>

ObserveEffect ==
    /\ attemptState.lifecycle = "applying"
    /\ attemptState.running
    /\ effectState.phase = "applied"
    /\ FailureMode # "irreversible_ambiguity"
    /\ effectState' = [effectState EXCEPT !.phase = "observed", !.observation = "exact"]
    /\ observations' = observations \cup {"effect-exact"}
    /\ UNCHANGED <<environmentState, attemptIdentity, attemptState, outbox,
                    compensationState, identityClaims, healthResults, approvals,
                    eventHistory, projectionReads, mutationHistory, logicalTime>>

ObserveConflict ==
    /\ attemptState.lifecycle = "applying"
    /\ attemptState.running
    /\ effectState.phase = "applied"
    /\ FailureMode = "irreversible_ambiguity"
    /\ effectState' = [effectState EXCEPT !.observation = "conflicting"]
    /\ observations' = observations \cup {"effect-conflicting"}
    /\ UNCHANGED <<environmentState, attemptIdentity, attemptState, outbox,
                    compensationState, identityClaims, healthResults, approvals,
                    eventHistory, projectionReads, mutationHistory, logicalTime>>

CommitEffect ==
    /\ attemptState.lifecycle = "applying"
    /\ attemptState.running
    /\ effectState.phase = "observed"
    /\ effectState.observation = "exact"
    /\ "reserved" \in identityClaims
    /\ attemptIdentity.databaseWriterEpoch = environmentState.databaseWriterEpoch + 1
    /\ effectState' = [effectState EXCEPT !.phase = "committed"]
    /\ identityClaims' = identityClaims \cup {"consumed"}
    /\ attemptState' = [attemptState EXCEPT !.lifecycle = "verifying", !.version = @ + 1]
    /\ eventHistory' = eventHistory \cup {<<"applying", "verifying">>}
    /\ UNCHANGED <<environmentState, attemptIdentity, outbox, compensationState,
                    observations, healthResults, approvals, projectionReads,
                    mutationHistory, logicalTime>>

GrantStagedAttemptCapability ==
    /\ attemptState.lifecycle = "verifying"
    /\ attemptState.running
    /\ effectState.phase = "committed"
    /\ environmentState.stagedCapability = "absent"
    /\ environmentState' = [environmentState EXCEPT !.stagedCapability = "active"]
    /\ UNCHANGED <<attemptIdentity, attemptState, outbox, effectState,
                    compensationState, observations, identityClaims, healthResults,
                    approvals, eventHistory, projectionReads, mutationHistory,
                    logicalTime>>

StageVerificationWrites ==
    /\ attemptState.lifecycle = "verifying"
    /\ attemptState.running
    /\ environmentState.stagedCapability = "active"
    /\ ~environmentState.stagedWrites
    /\ environmentState' = [environmentState EXCEPT !.stagedWrites = TRUE]
    /\ UNCHANGED <<attemptIdentity, attemptState, outbox, effectState,
                    compensationState, observations, identityClaims, healthResults,
                    approvals, eventHistory, projectionReads, mutationHistory,
                    logicalTime>>

RecordPublishIntent ==
    /\ attemptState.lifecycle = "verifying"
    /\ attemptState.running
    /\ environmentState.stagedWrites
    /\ ~environmentState.publishIntent
    /\ environmentState' = [environmentState EXCEPT !.publishIntent = TRUE]
    /\ UNCHANGED <<attemptIdentity, attemptState, outbox, effectState,
                    compensationState, observations, identityClaims, healthResults,
                    approvals, eventHistory, projectionReads, mutationHistory,
                    logicalTime>>

FreezeProducersForActivation ==
    /\ attemptState.lifecycle = "verifying"
    /\ attemptState.running
    /\ environmentState.publishIntent
    /\ ~environmentState.producersFrozen
    /\ environmentState' = [environmentState EXCEPT !.producersFrozen = TRUE]
    /\ UNCHANGED <<attemptIdentity, attemptState, outbox, effectState,
                    compensationState, observations, identityClaims, healthResults,
                    approvals, eventHistory, projectionReads, mutationHistory,
                    logicalTime>>

AuthorizeActivation ==
    /\ attemptState.lifecycle = "verifying"
    /\ attemptState.running
    /\ effectState.phase = "committed"
    /\ environmentState.stagedWrites
    /\ environmentState.publishIntent
    /\ environmentState.producersFrozen
    /\ environmentState.activationReceipt = None
    /\ environmentState' = [environmentState EXCEPT
          !.activationReceipt = ActivationReceipt,
          !.lifecycleEpoch = attemptIdentity.lifecycleEpoch,
          !.activationEpoch = attemptIdentity.lifecycleEpoch,
          !.activationDatabaseWriterEpoch = attemptIdentity.databaseWriterEpoch,
          !.activationCandidate = Artifact,
          !.activationStagedDigest = StagedDigest,
          !.activationPredecessor = PredecessorGeneration,
          !.stagedCapability = "authorized",
          !.recoveryMode = "read_only_stale"]
    /\ UNCHANGED <<attemptIdentity, attemptState, outbox, effectState,
                    compensationState, observations, identityClaims, healthResults,
                    approvals, eventHistory, projectionReads, mutationHistory,
                    logicalTime>>

PublishAuthorizedGeneration ==
    /\ attemptState.lifecycle = "verifying"
    /\ attemptState.running
    /\ FailureMode # "irreversible_postcommit_failure"
    /\ effectState.phase = "committed"
    /\ environmentState.activationReceipt = ActivationReceipt
    /\ environmentState.activationEpoch = environmentState.lifecycleEpoch
    /\ environmentState.activationDatabaseWriterEpoch = attemptIdentity.databaseWriterEpoch
    /\ attemptIdentity.databaseWriterEpoch = environmentState.databaseWriterEpoch + 1
    /\ environmentState.activationCandidate = Artifact
    /\ environmentState.activationStagedDigest = StagedDigest
    /\ environmentState.activationPredecessor = PredecessorGeneration
    /\ environmentState.currentGeneration = PredecessorGeneration
    /\ environmentState.targetDatabaseReceipt = None
    /\ environmentState' = [environmentState EXCEPT
          !.currentGeneration = TargetGeneration,
          !.databaseWriterEpoch = attemptIdentity.databaseWriterEpoch,
          !.targetDatabaseReceipt = ActivationReceipt,
          !.stagedCapability = "consumed",
          !.publishOutcome = TRUE,
          !.readerVisible = TRUE,
          !.visibilitySwitches = @ + 1,
          !.producerCapabilities = "derived",
          !.authorizationRecovered =
              attemptState.crashUsed
              /\ attemptState.crashActivationReceipt = ActivationReceipt
              /\ ~attemptState.crashPublishOutcome]
    /\ UNCHANGED <<attemptIdentity, attemptState, outbox, effectState,
                    compensationState, observations, identityClaims, healthResults,
                    approvals, eventHistory, projectionReads, mutationHistory,
                    logicalTime>>

ObserveAndFinalizeReady ==
    /\ attemptState.lifecycle = "verifying"
    /\ attemptState.running
    /\ environmentState.publishOutcome
    /\ environmentState.readerVisible
    /\ environmentState.targetDatabaseReceipt = ActivationReceipt
    /\ healthResults' = [context |-> Context, generation |-> TargetGeneration,
                          complete |-> TRUE, ready |-> TRUE, sequence |-> 2]
    /\ attemptState' = [attemptState EXCEPT !.lifecycle = "ready", !.version = @ + 1]
    /\ environmentState' = [environmentState EXCEPT
          !.finalObservation = TRUE,
          !.producerCapabilities = "active",
          !.publishedUnobservedRecovered =
              attemptState.crashUsed
              /\ attemptState.crashPublishOutcome
              /\ ~attemptState.crashFinalObservation,
          !.recoveryMode = "none"]
    /\ eventHistory' = eventHistory \cup {<<"verifying", "ready">>}
    /\ UNCHANGED <<attemptIdentity, outbox, effectState, compensationState,
                    observations, identityClaims, approvals, projectionReads,
                    mutationHistory, logicalTime>>

PublishFreshGeneration == PublishAuthorizedGeneration

Promote ==
    /\ attemptState.lifecycle = "ready"
    /\ attemptState.running
    /\ healthResults.context = environmentState.currentContext
    /\ healthResults.generation = environmentState.currentGeneration
    /\ healthResults.complete /\ healthResults.ready
    /\ attemptState' = [attemptState EXCEPT !.lifecycle = "promoted", !.version = @ + 1]
    /\ environmentState' = [environmentState EXCEPT !.activeAttempts = {}]
    /\ eventHistory' = eventHistory \cup {<<"ready", "promoted">>}
    /\ UNCHANGED <<attemptIdentity, outbox, effectState, compensationState,
                    observations, identityClaims, healthResults, approvals,
                    projectionReads, mutationHistory, logicalTime>>

RequestRecovery ==
    /\ attemptState.lifecycle \in {"admitted", "applying", "verifying", "ready"}
    /\ attemptState.running
    /\ ((FailureMode = "recover_admitted" /\ attemptState.lifecycle = "admitted")
            \/ (FailureMode = "recover_applying" /\ attemptState.lifecycle = "applying")
            \/ (FailureMode = "recover_verifying" /\ attemptState.lifecycle = "verifying")
            \/ (FailureMode = "recover_ready" /\ attemptState.lifecycle = "ready")
            \/ (FailureMode \in {"reversible_failure", "compensation_failure"}
                /\ effectState.phase \in PossiblyAppliedPhases)
            \/ (effectState.kind = "irreversible"
                /\ (effectState.observation = "conflicting"
                    \/ effectState.phase = "committed")))
    /\ attemptState' = [attemptState EXCEPT !.lifecycle = "recovering",
          !.version = @ + 1,
          !.recoveryReason = IF effectState.kind = "irreversible"
                            THEN IF effectState.phase = "committed"
                                 THEN "irreversible_failure" ELSE "irreversible_ambiguity"
                            ELSE "reversible_failure"]
    /\ eventHistory' = eventHistory \cup {<<attemptState.lifecycle, "recovering">>}
    /\ UNCHANGED <<environmentState, attemptIdentity, outbox, effectState,
                    compensationState, observations, identityClaims, healthResults,
                    approvals, projectionReads, mutationHistory, logicalTime>>

CompensationPlanReady ==
    /\ compensationState.plan = "exact"
    /\ compensationState.boundReceipt = RollbackReceipt
    /\ compensationState.order = "reverse"
    /\ compensationState.forwardPhase = effectState.phase
    /\ compensationState.required = (effectState.phase \in PossiblyAppliedPhases)

CompensationComplete ==
    \/ (~compensationState.required /\ compensationState.phase = "none")
    \/ (compensationState.required /\ compensationState.phase = "compensated")

AuthorizeRollback ==
    /\ attemptState.lifecycle = "recovering"
    /\ attemptState.running
    /\ attemptState.recoveryReason \notin {"admission_refusal", "approval_refusal"}
    /\ environmentState.activationReceipt \in {None, ActivationReceipt}
    /\ environmentState.rollbackPhase = "none"
    /\ compensationState.plan = "absent"
    /\ environmentState' = [environmentState EXCEPT
          !.lifecycleEpoch = RollbackLifecycleEpoch,
          !.activationReceipt = RollbackReceipt,
          !.activationEpoch = RollbackLifecycleEpoch,
          !.activationDatabaseWriterEpoch = RollbackDatabaseWriterEpoch,
          !.activationCandidate = PredecessorArtifact,
          !.activationStagedDigest = None,
          !.activationPredecessor = environmentState.currentGeneration,
          !.stagedCapability = "absent",
          !.stagedWrites = FALSE,
          !.publishIntent = FALSE,
          !.producersFrozen = FALSE,
          !.producerCapabilities = IF environmentState.readerVisible THEN "derived" ELSE "absent",
          !.finalObservation = FALSE,
          !.invalidatedReceipts = @ \cup {ActivationReceipt},
          !.authorizationOnlyRollback = ~environmentState.publishOutcome,
          !.rollbackPhase = "authorized",
          !.recoveryMode = "read_only_stale"]
    /\ compensationState' = [compensationState EXCEPT
          !.plan = "exact",
          !.required = effectState.phase \in PossiblyAppliedPhases,
          !.forwardPhase = effectState.phase,
          !.boundReceipt = RollbackReceipt,
          !.order = "reverse"]
    /\ UNCHANGED <<attemptIdentity, attemptState, outbox, effectState,
                    observations, identityClaims, healthResults, approvals,
                    eventHistory, projectionReads, mutationHistory, logicalTime>>

CompensationIntent ==
    /\ attemptState.lifecycle = "recovering"
    /\ attemptState.running
    /\ environmentState.rollbackPhase = "authorized"
    /\ CompensationPlanReady
    /\ compensationState.required
    /\ compensationState.phase = "none"
    /\ compensationState' = [compensationState EXCEPT !.phase = "intended"]
    /\ UNCHANGED <<environmentState, attemptIdentity, attemptState, outbox,
                    effectState, observations, identityClaims, healthResults,
                    approvals, eventHistory, projectionReads, mutationHistory,
                    logicalTime>>

CompensationApply ==
    /\ attemptState.lifecycle = "recovering"
    /\ compensationState.phase = "intended"
    /\ attemptState.running
    /\ compensationState' = [compensationState EXCEPT !.phase = "applied"]
    /\ UNCHANGED <<environmentState, attemptIdentity, attemptState, outbox,
                    effectState, observations, identityClaims, healthResults,
                    approvals, eventHistory, projectionReads, mutationHistory,
                    logicalTime>>

CompensationObserve ==
    /\ attemptState.lifecycle = "recovering"
    /\ attemptState.running
    /\ compensationState.phase = "applied"
    /\ FailureMode # "compensation_failure"
    /\ compensationState' = [compensationState EXCEPT !.phase = "observed", !.observation = "exact"]
    /\ observations' = observations \cup {"compensation-exact"}
    /\ UNCHANGED <<environmentState, attemptIdentity, attemptState, outbox,
                    effectState, identityClaims, healthResults, approvals,
                    eventHistory, projectionReads, mutationHistory, logicalTime>>

CompensationCommit ==
    /\ attemptState.lifecycle = "recovering"
    /\ attemptState.running
    /\ compensationState.phase = "observed"
    /\ compensationState.observation = "exact"
    /\ compensationState' = [compensationState EXCEPT !.phase = "compensated"]
    /\ UNCHANGED <<environmentState, attemptIdentity, attemptState, outbox,
                    effectState, observations, identityClaims, healthResults,
                    approvals, eventHistory, projectionReads, mutationHistory,
                    logicalTime>>

RestorePredecessorUnderNewEpoch ==
    /\ attemptState.lifecycle = "recovering"
    /\ attemptState.running
    /\ environmentState.activationReceipt = RollbackReceipt
    /\ environmentState.activationCandidate = PredecessorArtifact
    /\ environmentState.currentGeneration \in {PredecessorGeneration, TargetGeneration}
    /\ environmentState.rollbackPhase = "authorized"
    /\ CompensationPlanReady
    /\ CompensationComplete
    /\ environmentState' = [environmentState EXCEPT
          !.databaseWriterEpoch = RollbackDatabaseWriterEpoch,
          !.targetDatabaseReceipt = RollbackReceipt,
          !.currentGeneration = PredecessorGeneration,
          !.stagedCapability = "absent",
          !.stagedWrites = FALSE,
          !.publishIntent = FALSE,
          !.publishOutcome = TRUE,
          !.readerVisible = TRUE,
          !.producerCapabilities = "derived",
          !.finalObservation = FALSE,
          !.visibilitySwitches = @ + 1,
          !.producersFrozen = FALSE,
          !.rollbackPhase = "restored",
          !.recoveryMode = "read_only_stale"]
    /\ UNCHANGED <<attemptIdentity, attemptState, outbox, effectState,
                    compensationState, observations, identityClaims, healthResults,
                    approvals, eventHistory, projectionReads, mutationHistory,
                    logicalTime>>

ObserveAndFinalizeRollback ==
    /\ attemptState.lifecycle = "recovering"
    /\ attemptState.running
    /\ environmentState.rollbackPhase = "restored"
    /\ environmentState.activationReceipt = RollbackReceipt
    /\ environmentState.activationEpoch = RollbackLifecycleEpoch
    /\ environmentState.activationDatabaseWriterEpoch = RollbackDatabaseWriterEpoch
    /\ environmentState.targetDatabaseReceipt = RollbackReceipt
    /\ environmentState.currentGeneration = PredecessorGeneration
    /\ ActivationReceipt \in environmentState.invalidatedReceipts
    /\ CompensationPlanReady
    /\ CompensationComplete
    /\ healthResults' = [context |-> Context, generation |-> PredecessorGeneration,
                          complete |-> TRUE, ready |-> TRUE, sequence |-> 2]
    /\ attemptState' = [attemptState EXCEPT
          !.lifecycle = "rolled_back", !.version = @ + 1]
    /\ environmentState' = [environmentState EXCEPT
          !.activeAttempts = {},
          !.finalObservation = TRUE,
          !.producerCapabilities = "active",
          !.rollbackPhase = "observed",
          !.recoveryMode = "none"]
    /\ eventHistory' = eventHistory \cup {<<"recovering", "rolled_back">>}
    /\ UNCHANGED <<attemptIdentity, outbox, effectState, compensationState,
                    observations, identityClaims, approvals, projectionReads,
                    mutationHistory, logicalTime>>

SettleBlocked ==
    /\ attemptState.lifecycle = "recovering"
    /\ attemptState.running
    /\ attemptState.recoveryReason \in {"admission_refusal", "approval_refusal"}
        \/ (FailureMode = "compensation_failure" /\ compensationState.phase = "applied")
    /\ attemptState' = [attemptState EXCEPT !.lifecycle = "blocked", !.version = @ + 1]
    /\ environmentState' = [environmentState EXCEPT !.activeAttempts = {}]
    /\ compensationState' = IF FailureMode = "compensation_failure" /\ compensationState.phase = "applied"
                            THEN [compensationState EXCEPT !.phase = "failed"]
                            ELSE compensationState
    /\ eventHistory' = eventHistory \cup {<<"recovering", "blocked">>}
    /\ UNCHANGED <<attemptIdentity, outbox, effectState, observations,
                    identityClaims, healthResults, approvals, projectionReads,
                    mutationHistory, logicalTime>>

Crash ==
    /\ attemptState.lifecycle \notin TerminalStates \cup {"absent"}
    /\ attemptState.running
    /\ ~attemptState.crashUsed
    /\ attemptState' = [attemptState EXCEPT
          !.running = FALSE,
          !.crashUsed = TRUE,
          !.crashLifecycle = attemptState.lifecycle,
          !.crashEffectPhase = effectState.phase,
          !.crashCompensationPhase = compensationState.phase,
          !.crashVersion = attemptState.version,
          !.crashActivationReceipt = environmentState.activationReceipt,
          !.crashPublishOutcome = environmentState.publishOutcome,
          !.crashFinalObservation = environmentState.finalObservation]
    /\ UNCHANGED <<environmentState, attemptIdentity, outbox, effectState,
                    compensationState, observations, identityClaims, healthResults,
                    approvals, eventHistory, projectionReads, mutationHistory,
                    logicalTime>>

Restart ==
    /\ ~attemptState.running
    /\ attemptState' = [attemptState EXCEPT !.running = TRUE]
    /\ UNCHANGED <<environmentState, attemptIdentity, outbox, effectState,
                    compensationState, observations, identityClaims, healthResults,
                    approvals, eventHistory, projectionReads, mutationHistory,
                    logicalTime>>

InvalidateStagingSource ==
    /\ Environment = Production
    /\ attemptState.lifecycle = "planned"
    /\ ApprovalEligible
    /\ environmentState' = [environmentState EXCEPT
          !.stagingContext = "staging-context-2",
          !.stagingHealthSequence = 2,
          !.stagingHealthy = FALSE,
          !.stagingState = "applying"]
    /\ UNCHANGED <<attemptIdentity, attemptState, outbox, effectState,
                    compensationState, observations, identityClaims, healthResults,
                    approvals, eventHistory, projectionReads, mutationHistory,
                    logicalTime>>

ReadProjection ==
    /\ FailureMode = "success"
    /\ attemptState.lifecycle # "absent"
    /\ projectionReads' = projectionReads \cup
          {<<attemptIdentity.id, attemptState.version, attemptState.lifecycle,
             environmentState.currentContext>>}
    /\ UNCHANGED <<environmentState, attemptIdentity, attemptState, outbox,
                    effectState, compensationState, observations, identityClaims,
                    healthResults, approvals, eventHistory, mutationHistory,
                    logicalTime>>

RequestMutation(capability, lifecycleEpoch, databaseWriterEpoch) ==
    /\ FailureMode = "success"
    /\ attemptState.lifecycle \notin TerminalStates \cup {"absent"}
    /\ mutationHistory' = mutationHistory \cup
          {IF capability = environmentState.activationReceipt
              /\ capability \notin environmentState.invalidatedReceipts
              /\ lifecycleEpoch = environmentState.lifecycleEpoch
              /\ databaseWriterEpoch = environmentState.databaseWriterEpoch
              /\ environmentState.finalObservation
              /\ environmentState.producerCapabilities = "active"
           THEN IF attemptState.running THEN "canonical_accepted" ELSE "stopped_rejected"
           ELSE IF capability = "legacy" THEN "legacy_rejected"
           ELSE "stale_epoch_rejected"}
    /\ UNCHANGED <<environmentState, attemptIdentity, attemptState, outbox,
                    effectState, compensationState, observations, identityClaims,
                    healthResults, approvals, eventHistory, projectionReads,
                    logicalTime>>

Tick ==
    /\ logicalTime < 2
    /\ logicalTime' = logicalTime + 1
    /\ approvals' = IF approvals.state = "issued" /\ logicalTime' >= approvals.expiresAt
                    THEN [approvals EXCEPT !.state = "expired"] ELSE approvals
    /\ UNCHANGED <<environmentState, attemptIdentity, attemptState, outbox,
                    effectState, compensationState, observations, identityClaims,
                    healthResults, eventHistory, projectionReads, mutationHistory>>

TerminalNoOp ==
    /\ attemptState.lifecycle \in TerminalStates
    /\ UNCHANGED vars

RecoveryStep == AuthorizeRollback \/ CompensationIntent \/ CompensationApply \/ CompensationObserve
                \/ CompensationCommit \/ RestorePredecessorUnderNewEpoch
                \/ ObserveAndFinalizeRollback \/ SettleBlocked

Next == PlanAttempt \/ DeliverWorkflowStart \/ RefuseWorkflowStart \/ BeginApply
        \/ CommitPredecessors \/ IntentEffect \/ ApplyEffect \/ ObserveEffect
        \/ ObserveConflict \/ CommitEffect \/ GrantStagedAttemptCapability
        \/ StageVerificationWrites \/ RecordPublishIntent
        \/ FreezeProducersForActivation \/ AuthorizeActivation
        \/ PublishAuthorizedGeneration \/ ObserveAndFinalizeReady \/ Promote
        \/ RequestRecovery \/ RecoveryStep \/ Crash \/ Restart \/ ReadProjection
        \/ InvalidateStagingSource
        \/ RequestMutation(ActivationReceipt, CandidateLifecycleEpoch,
                           CandidateDatabaseWriterEpoch)
        \/ RequestMutation("legacy", 0, 0)
        \/ RequestMutation(ActivationReceipt, 0, CandidateDatabaseWriterEpoch)
        \/ RequestMutation(ActivationReceipt, CandidateLifecycleEpoch, 1)
        \/ Tick \/ TerminalNoOp

TypeScopeExact ==
    /\ DOMAIN environmentState = {"currentAttempt", "activeAttempts", "lifecycleEpoch",
          "databaseWriterEpoch",
          "ledgerAuthority", "currentContext", "currentGeneration",
          "stagedCapability", "stagedWrites", "publishIntent", "publishOutcome",
          "readerVisible", "visibilitySwitches", "producersFrozen",
          "activationReceipt", "activationEpoch", "activationCandidate",
          "activationDatabaseWriterEpoch",
          "activationStagedDigest", "activationPredecessor", "targetDatabaseReceipt",
          "producerCapabilities", "finalObservation", "authorizationRecovered",
          "publishedUnobservedRecovered", "invalidatedReceipts",
          "authorizationOnlyRollback", "rollbackPhase", "recoveryMode",
          "stagingContext", "stagingGeneration",
          "stagingHealthSequence", "stagingHealthy", "stagingComplete", "stagingState"}
    /\ DOMAIN attemptIdentity = {"id", "environment", "artifact", "definition", "policy",
          "database", "admissionContext", "targetContext", "lifecycleEpoch",
          "databaseWriterEpoch"}
    /\ DOMAIN attemptState = {"lifecycle", "version", "workflowId", "recoveryReason",
          "running", "crashUsed", "crashLifecycle", "crashEffectPhase",
          "crashCompensationPhase", "crashVersion", "crashActivationReceipt",
          "crashPublishOutcome", "crashFinalObservation"}
    /\ DOMAIN outbox = {"phase", "workflowId"}
    /\ DOMAIN effectState = {"phase", "kind", "observation", "predecessorsCommitted"}
    /\ DOMAIN compensationState = {"phase", "observation", "plan", "required",
                                     "forwardPhase", "boundReceipt", "order"}
    /\ DOMAIN healthResults = {"context", "generation", "complete", "ready", "sequence"}
    /\ DOMAIN approvals = {"state", "sourceContext", "sourceGeneration",
          "sourceHealthSequence", "sourceHealthy", "sourceComplete", "sourceStagingState",
          "targetContext", "consumedBy", "expiresAt"}
    /\ environmentState.currentAttempt \in {None, AttemptId}
    /\ environmentState.activeAttempts \subseteq {AttemptId}
    /\ environmentState.lifecycleEpoch \in 0..RollbackLifecycleEpoch
    /\ environmentState.databaseWriterEpoch \in 1..RollbackDatabaseWriterEpoch
    /\ environmentState.ledgerAuthority = "external-controller-ledger"
    /\ environmentState.currentContext = Context
    /\ environmentState.currentGeneration \in 0..TargetGeneration
    /\ environmentState.stagedCapability \in {"absent", "active", "authorized", "consumed"}
    /\ environmentState.stagedWrites \in BOOLEAN
    /\ environmentState.publishIntent \in BOOLEAN
    /\ environmentState.publishOutcome \in BOOLEAN
    /\ environmentState.readerVisible \in BOOLEAN
    /\ environmentState.visibilitySwitches \in 0..2
    /\ environmentState.producersFrozen \in BOOLEAN
    /\ environmentState.activationReceipt \in {None, ActivationReceipt, RollbackReceipt}
    /\ environmentState.activationEpoch \in 0..RollbackLifecycleEpoch
    /\ environmentState.activationDatabaseWriterEpoch \in 0..RollbackDatabaseWriterEpoch
    /\ environmentState.activationCandidate \in {None, Artifact, PredecessorArtifact}
    /\ environmentState.activationStagedDigest \in {None, StagedDigest}
    /\ environmentState.activationPredecessor \in 0..TargetGeneration
    /\ environmentState.targetDatabaseReceipt \in {None, ActivationReceipt, RollbackReceipt}
    /\ environmentState.producerCapabilities \in {"absent", "derived", "active"}
    /\ environmentState.finalObservation \in BOOLEAN
    /\ environmentState.authorizationRecovered \in BOOLEAN
    /\ environmentState.publishedUnobservedRecovered \in BOOLEAN
    /\ environmentState.invalidatedReceipts \subseteq {ActivationReceipt, RollbackReceipt}
    /\ environmentState.authorizationOnlyRollback \in BOOLEAN
    /\ environmentState.rollbackPhase \in {"none", "authorized", "restored", "observed"}
    /\ environmentState.recoveryMode \in {"none", "read_only_stale"}
    /\ environmentState.stagingContext \in {SourceContext, "staging-context-2"}
    /\ environmentState.stagingGeneration = SourceGeneration
    /\ environmentState.stagingHealthSequence \in 1..2
    /\ environmentState.stagingHealthy \in BOOLEAN
    /\ environmentState.stagingComplete \in BOOLEAN
    /\ environmentState.stagingState \in {"applying", "ready"}
    /\ attemptIdentity.id \in {None, AttemptId}
    /\ attemptIdentity.environment = Environment
    /\ attemptIdentity.artifact \in {None, Artifact}
    /\ attemptIdentity.definition \in {None, Definition}
    /\ attemptIdentity.policy \in {None, Policy}
    /\ attemptIdentity.database \in {None, Database}
    /\ attemptIdentity.admissionContext \in {None, Context}
    /\ attemptIdentity.targetContext \in {None, Context}
    /\ attemptIdentity.lifecycleEpoch \in 0..CandidateLifecycleEpoch
    /\ attemptIdentity.databaseWriterEpoch \in 0..CandidateDatabaseWriterEpoch
    /\ attemptState.lifecycle \in LifecycleStates
    /\ attemptState.version \in 0..8
    /\ attemptState.workflowId \in {None, WorkflowId}
    /\ attemptState.recoveryReason \in {None, "admission_refusal", "approval_refusal",
                                         "reversible_failure", "irreversible_ambiguity",
                                         "irreversible_failure"}
    /\ attemptState.running \in BOOLEAN
    /\ attemptState.crashUsed \in BOOLEAN
    /\ attemptState.crashLifecycle \in {None} \cup LifecycleStates
    /\ attemptState.crashEffectPhase \in {None} \cup EffectPhases
    /\ attemptState.crashCompensationPhase \in {None} \cup CompensationPhases
    /\ attemptState.crashVersion \in 0..8
    /\ attemptState.crashActivationReceipt \in {None, ActivationReceipt, RollbackReceipt}
    /\ attemptState.crashPublishOutcome \in BOOLEAN
    /\ attemptState.crashFinalObservation \in BOOLEAN
    /\ outbox.phase \in OutboxPhases /\ outbox.workflowId \in {None, WorkflowId}
    /\ effectState.phase \in EffectPhases /\ effectState.kind \in EffectClasses
    /\ effectState.observation \in ObservationValues
    /\ effectState.predecessorsCommitted \in BOOLEAN
    /\ compensationState.phase \in CompensationPhases
    /\ compensationState.observation \in ObservationValues
    /\ compensationState.plan \in CompensationPlans
    /\ compensationState.required \in BOOLEAN
    /\ compensationState.forwardPhase \in EffectPhases
    /\ compensationState.boundReceipt \in {None, ActivationReceipt, RollbackReceipt}
    /\ compensationState.order \in CompensationOrders
    /\ observations \subseteq {"effect-exact", "effect-conflicting", "compensation-exact"}
    /\ identityClaims \subseteq IdentityValues
    /\ healthResults.context \in {None, Context}
    /\ healthResults.generation \in 0..TargetGeneration
    /\ healthResults.complete \in BOOLEAN /\ healthResults.ready \in BOOLEAN
    /\ healthResults.sequence \in 0..2
    /\ approvals.state \in ApprovalStates
    /\ approvals.sourceContext \in {SourceContext, "stale-context"}
    /\ approvals.sourceGeneration = SourceGeneration
    /\ approvals.sourceHealthSequence = 1
    /\ approvals.sourceHealthy \in BOOLEAN
    /\ approvals.sourceComplete \in BOOLEAN
    /\ approvals.sourceStagingState \in {"applying", "ready"}
    /\ approvals.targetContext = Context
    /\ approvals.consumedBy \in {None, AttemptId, "older-attempt"}
    /\ approvals.expiresAt \in 0..2
    /\ eventHistory \subseteq LegalEdges
    /\ projectionReads \subseteq
          ({AttemptId} \X (0..8) \X LifecycleStates \X {Context})
    /\ mutationHistory \subseteq MutationResults
    /\ logicalTime \in 0..2

SingleActiveAttempt == Cardinality(environmentState.activeAttempts) <= 1

SingleWriterEpoch ==
    "canonical_accepted" \in mutationHistory =>
      /\ attemptIdentity.id = environmentState.currentAttempt
      /\ attemptIdentity.lifecycleEpoch = environmentState.lifecycleEpoch
      /\ attemptIdentity.databaseWriterEpoch = environmentState.databaseWriterEpoch

LegalLifecycle ==
    /\ eventHistory \subseteq LegalEdges
    /\ attemptState.lifecycle \in TerminalStates => environmentState.activeAttempts = {}
    /\ attemptState.lifecycle \in {"admitted", "applying", "verifying", "ready", "promoted"}
         => outbox.phase = "delivered"

AttemptContextImmutable ==
    attemptIdentity.id = AttemptId =>
      /\ attemptIdentity.environment = Environment
      /\ attemptIdentity.artifact = Artifact
      /\ attemptIdentity.definition = Definition
      /\ attemptIdentity.policy = Policy
      /\ attemptIdentity.database = Database
      /\ attemptIdentity.admissionContext = Context
      /\ attemptIdentity.targetContext = Context
      /\ attemptIdentity.lifecycleEpoch = CandidateLifecycleEpoch
      /\ attemptIdentity.databaseWriterEpoch = CandidateDatabaseWriterEpoch

ObservedCommit ==
    effectState.phase = "committed" =>
      /\ effectState.observation = "exact"
      /\ "effect-exact" \in observations
      /\ {"reserved", "consumed"} \subseteq identityClaims

IdentityHistoryMonotonic == "consumed" \in identityClaims => "reserved" \in identityClaims

RecoveryEquivalence ==
    /\ ~attemptState.running =>
         /\ attemptState.lifecycle = attemptState.crashLifecycle
         /\ attemptState.version = attemptState.crashVersion
         /\ effectState.phase = attemptState.crashEffectPhase
         /\ compensationState.phase = attemptState.crashCompensationPhase
         /\ environmentState.activationReceipt = attemptState.crashActivationReceipt
         /\ environmentState.publishOutcome = attemptState.crashPublishOutcome
         /\ environmentState.finalObservation = attemptState.crashFinalObservation
    /\ compensationState.phase # "none" =>
         /\ compensationState.required
         /\ effectState.phase \in PossiblyAppliedPhases
         /\ CompensationPlanReady
    /\ FailureMode = "irreversible_postcommit_failure"
        /\ effectState.phase = "committed" =>
         attemptState.lifecycle \notin {"ready", "promoted"}
    /\ attemptState.lifecycle = "rolled_back" =>
         /\ environmentState.rollbackPhase = "observed"
         /\ CompensationPlanReady
         /\ CompensationComplete
    /\ attemptState.lifecycle = "blocked" =>
         attemptState.recoveryReason \in {"admission_refusal", "approval_refusal"}
         \/ compensationState.phase = "failed"

ExactCompensationPlan ==
    compensationState.plan # "absent" =>
      /\ CompensationPlanReady
      /\ (effectState.phase = "planned" => ~compensationState.required)

RollbackReceiptBindsCompensationPlan ==
    environmentState.rollbackPhase # "none" =>
      /\ compensationState.plan = "exact"
      /\ compensationState.boundReceipt = environmentState.activationReceipt

CompensationOrderIsReverse ==
    compensationState.required => compensationState.order = "reverse"

RollbackPublicationRequiresCompensation ==
    environmentState.rollbackPhase \in {"restored", "observed"} =>
      /\ CompensationPlanReady
      /\ CompensationComplete

RollbackFinalizationRequiresCompensation ==
    attemptState.lifecycle = "rolled_back" =>
      /\ CompensationPlanReady
      /\ CompensationComplete

ExactContextHealth ==
    attemptState.lifecycle \in {"ready", "promoted"} =>
      /\ healthResults.context = environmentState.currentContext
      /\ healthResults.generation = environmentState.currentGeneration
      /\ healthResults.complete /\ healthResults.ready

ExactSingleUseApproval ==
    Environment = Production /\ attemptState.lifecycle \notin {"absent", "planned", "recovering", "blocked"} =>
      /\ approvals.state = "consumed"
      /\ approvals.sourceContext = SourceContext
      /\ approvals.sourceGeneration = SourceGeneration
      /\ approvals.sourceHealthSequence = 1
      /\ approvals.sourceContext = environmentState.stagingContext
      /\ approvals.sourceGeneration = environmentState.stagingGeneration
      /\ approvals.sourceHealthSequence = environmentState.stagingHealthSequence
      /\ approvals.sourceHealthy = environmentState.stagingHealthy
      /\ approvals.sourceComplete = environmentState.stagingComplete
      /\ approvals.sourceStagingState = environmentState.stagingState
      /\ environmentState.stagingHealthy
      /\ environmentState.stagingComplete
      /\ environmentState.stagingState = "ready"
      /\ approvals.targetContext = Context
      /\ approvals.consumedBy = AttemptId

ProjectionConsistency ==
    \A read \in projectionReads :
      /\ read[1] = AttemptId
      /\ read[4] = Context
      /\ <<read[3], read[3]>> \notin eventHistory

CanonicalPathOnly ==
    /\ "legacy_accepted" \notin mutationHistory
    /\ "stopped_accepted" \notin mutationHistory
    /\ "canonical_accepted" \in mutationHistory =>
         /\ environmentState.finalObservation
         /\ environmentState.producerCapabilities = "active"

FreshPublication ==
    attemptState.lifecycle = "promoted" =>
      /\ environmentState.currentGeneration = TargetGeneration
      /\ healthResults.generation = TargetGeneration
      /\ healthResults.sequence > SourceGeneration

OrderedIrreversibility ==
    effectState.kind = "irreversible" /\ effectState.phase \in {"intended", "applied", "observed", "committed"}
      => effectState.predecessorsCommitted

SoleLedgerAuthority ==
    /\ environmentState.ledgerAuthority = "external-controller-ledger"
    /\ (environmentState.activationReceipt = None =>
         /\ environmentState.activationEpoch = 0
         /\ environmentState.activationDatabaseWriterEpoch = 0
         /\ environmentState.activationCandidate = None
         /\ environmentState.activationStagedDigest = None
         /\ ~environmentState.authorizationOnlyRollback)
    /\ (environmentState.activationReceipt = ActivationReceipt =>
         /\ environmentState.activationEpoch = CandidateLifecycleEpoch
         /\ environmentState.activationDatabaseWriterEpoch = CandidateDatabaseWriterEpoch
         /\ environmentState.lifecycleEpoch = CandidateLifecycleEpoch
         /\ environmentState.databaseWriterEpoch =
              IF environmentState.targetDatabaseReceipt = ActivationReceipt
              THEN CandidateDatabaseWriterEpoch ELSE 1
         /\ environmentState.activationCandidate = Artifact
         /\ environmentState.activationStagedDigest = StagedDigest
         /\ environmentState.activationPredecessor = PredecessorGeneration
         /\ ~environmentState.authorizationOnlyRollback)
    /\ (environmentState.activationReceipt = RollbackReceipt =>
         /\ environmentState.activationEpoch = RollbackLifecycleEpoch
         /\ environmentState.activationDatabaseWriterEpoch = RollbackDatabaseWriterEpoch
         /\ environmentState.lifecycleEpoch = RollbackLifecycleEpoch
         /\ environmentState.activationCandidate = PredecessorArtifact
         /\ environmentState.activationStagedDigest = None
         /\ environmentState.activationPredecessor =
              IF environmentState.authorizationOnlyRollback
              THEN PredecessorGeneration ELSE TargetGeneration
         /\ (environmentState.rollbackPhase = "authorized" =>
              environmentState.databaseWriterEpoch # RollbackDatabaseWriterEpoch)
         /\ (environmentState.rollbackPhase \in {"restored", "observed"} =>
              environmentState.databaseWriterEpoch = RollbackDatabaseWriterEpoch))

StagedAttemptIsolation ==
    /\ environmentState.stagedWrites =>
         environmentState.stagedCapability \in {"active", "authorized", "consumed"}
    /\ environmentState.activationReceipt = None =>
         /\ ~environmentState.readerVisible
         /\ environmentState.currentGeneration # TargetGeneration
    /\ environmentState.stagedCapability = "active" => ~environmentState.readerVisible

PublicationVisibilityOrdering ==
    /\ environmentState.publishOutcome =>
         /\ ((environmentState.rollbackPhase = "none"
               /\ environmentState.publishIntent
               /\ environmentState.stagedWrites
               /\ environmentState.activationReceipt = ActivationReceipt
               /\ environmentState.targetDatabaseReceipt = ActivationReceipt)
             \/ (environmentState.rollbackPhase = "authorized"
               /\ environmentState.activationReceipt = RollbackReceipt
               /\ environmentState.currentGeneration = TargetGeneration
               /\ environmentState.targetDatabaseReceipt = ActivationReceipt)
             \/ (environmentState.rollbackPhase \in {"restored", "observed"}
               /\ ~environmentState.publishIntent
               /\ ~environmentState.stagedWrites
               /\ environmentState.activationReceipt = RollbackReceipt
               /\ environmentState.targetDatabaseReceipt = RollbackReceipt
               /\ ActivationReceipt \in environmentState.invalidatedReceipts))
    /\ environmentState.readerVisible =>
         /\ environmentState.publishOutcome
         /\ ((environmentState.rollbackPhase = "none"
               /\ environmentState.stagedCapability = "consumed"
               /\ environmentState.currentGeneration = TargetGeneration
               /\ environmentState.visibilitySwitches = 1)
             \/ (environmentState.rollbackPhase = "authorized"
               /\ environmentState.currentGeneration = TargetGeneration
               /\ environmentState.visibilitySwitches = 1)
             \/ (environmentState.rollbackPhase \in {"restored", "observed"}
               /\ environmentState.stagedCapability = "absent"
               /\ environmentState.currentGeneration = PredecessorGeneration
               /\ environmentState.visibilitySwitches \in 1..2))
         /\ environmentState.producerCapabilities \in {"derived", "active"}
    /\ environmentState.finalObservation =>
         /\ environmentState.readerVisible
         /\ environmentState.producerCapabilities = "active"
         /\ healthResults.ready
    /\ environmentState.recoveryMode = "read_only_stale" =>
         environmentState.producerCapabilities # "active"

LifecycleAndDatabaseEpochsAreDistinct ==
    /\ CandidateLifecycleEpoch # CandidateDatabaseWriterEpoch
    /\ RollbackLifecycleEpoch # RollbackDatabaseWriterEpoch
    /\ environmentState.activationReceipt # None =>
         environmentState.activationEpoch # environmentState.activationDatabaseWriterEpoch

BroadMutationRequiresObservedReceiptCapability ==
    "canonical_accepted" \in mutationHistory =>
      /\ environmentState.finalObservation
      /\ environmentState.producerCapabilities = "active"
      /\ environmentState.activationReceipt \notin environmentState.invalidatedReceipts

RecoveryGapsAreExplicit ==
    /\ environmentState.authorizationRecovered =>
         /\ environmentState.publishOutcome
         /\ (environmentState.activationReceipt = ActivationReceipt
             \/ ActivationReceipt \in environmentState.invalidatedReceipts)
    /\ environmentState.publishedUnobservedRecovered =>
         /\ (environmentState.finalObservation
             \/ environmentState.rollbackPhase \in {"authorized", "restored"})
         /\ environmentState.readerVisible

RollbackRestoresPredecessorUnderNewEpoch ==
    environmentState.rollbackPhase \in {"restored", "observed"} =>
      /\ environmentState.currentGeneration = PredecessorGeneration
      /\ environmentState.activationReceipt = RollbackReceipt
      /\ environmentState.activationEpoch = RollbackLifecycleEpoch
      /\ environmentState.lifecycleEpoch = RollbackLifecycleEpoch
      /\ environmentState.databaseWriterEpoch = RollbackDatabaseWriterEpoch
      /\ RollbackLifecycleEpoch > CandidateLifecycleEpoch
      /\ RollbackDatabaseWriterEpoch > CandidateDatabaseWriterEpoch
      /\ ActivationReceipt \in environmentState.invalidatedReceipts
      /\ environmentState.targetDatabaseReceipt = RollbackReceipt

RollbackWriterEpochRotationBoundary ==
    /\ (environmentState.rollbackPhase = "authorized" =>
         /\ environmentState.databaseWriterEpoch # RollbackDatabaseWriterEpoch
         /\ environmentState.targetDatabaseReceipt # RollbackReceipt)
    /\ (environmentState.rollbackPhase \in {"restored", "observed"} =>
         /\ environmentState.databaseWriterEpoch = RollbackDatabaseWriterEpoch
         /\ environmentState.targetDatabaseReceipt = RollbackReceipt
         /\ environmentState.currentGeneration = PredecessorGeneration)

AuthorizationOnlyRollbackSeparatesAuthorizationFromPublication ==
    environmentState.authorizationOnlyRollback =>
      /\ environmentState.rollbackPhase \in {"authorized", "restored", "observed"}
      /\ environmentState.activationReceipt = RollbackReceipt
      /\ environmentState.activationPredecessor = PredecessorGeneration
      /\ environmentState.lifecycleEpoch = RollbackLifecycleEpoch
      /\ RollbackLifecycleEpoch > CandidateLifecycleEpoch
      /\ RollbackDatabaseWriterEpoch > CandidateDatabaseWriterEpoch
      /\ ActivationReceipt \in environmentState.invalidatedReceipts
      /\ environmentState.stagedCapability = "absent"
      /\ (environmentState.rollbackPhase = "authorized" =>
           /\ environmentState.databaseWriterEpoch # RollbackDatabaseWriterEpoch
           /\ environmentState.targetDatabaseReceipt # RollbackReceipt)
      /\ (environmentState.rollbackPhase \in {"restored", "observed"} =>
           /\ environmentState.databaseWriterEpoch = RollbackDatabaseWriterEpoch
           /\ environmentState.targetDatabaseReceipt = RollbackReceipt
           /\ environmentState.currentGeneration = PredecessorGeneration
           /\ ~environmentState.stagedWrites
           /\ ~environmentState.publishIntent
           /\ ~environmentState.producersFrozen
           /\ environmentState.readerVisible)
      /\ (environmentState.rollbackPhase = "observed" =>
           /\ environmentState.finalObservation
           /\ environmentState.producerCapabilities = "active"
           /\ healthResults.generation = PredecessorGeneration
           /\ healthResults.complete
           /\ healthResults.ready)

TerminalRollbackHasNoCandidateAuthority ==
    attemptState.lifecycle = "rolled_back" =>
      /\ environmentState.activationReceipt # ActivationReceipt
      /\ environmentState.stagedCapability \notin {"authorized", "consumed"}

InvalidatedReceiptCapabilityCannotMutate ==
    ActivationReceipt \in environmentState.invalidatedReceipts =>
      /\ environmentState.activationReceipt = RollbackReceipt
      /\ (environmentState.producerCapabilities = "active" =>
           /\ environmentState.finalObservation
           /\ environmentState.rollbackPhase = "observed")

RecoveryEventuallySettles ==
    [](attemptState.lifecycle = "recovering" => <>(attemptState.lifecycle \in {"rolled_back", "blocked"}))

NeverReachPlanned == attemptState.lifecycle # "planned"
NeverReachAdmitted == attemptState.lifecycle # "admitted"
NeverReachApplying == attemptState.lifecycle # "applying"
NeverReachVerifying == attemptState.lifecycle # "verifying"
NeverReachReady == attemptState.lifecycle # "ready"
NeverReachPromoted == attemptState.lifecycle # "promoted"
NeverReachRecovering == attemptState.lifecycle # "recovering"
NeverReachRolledBack == attemptState.lifecycle # "rolled_back"
NeverReachBlocked == attemptState.lifecycle # "blocked"
NeverReachIntended == effectState.phase # "intended"
NeverReachApplied == effectState.phase # "applied"
NeverReachObserved == effectState.phase # "observed"
NeverReachCommitted == effectState.phase # "committed"
NeverReachCompensationIntended == compensationState.phase # "intended"
NeverReachCompensationApplied == compensationState.phase # "applied"
NeverReachCompensationObserved == compensationState.phase # "observed"
NeverReachCompensated == compensationState.phase # "compensated"
NeverConsumeApproval == approvals.consumedBy # AttemptId
NeverRejectLegacyMutation == "legacy_rejected" \notin mutationHistory
NeverPublishFreshGeneration == environmentState.currentGeneration # TargetGeneration
NeverGrantStagedCapability == environmentState.stagedCapability # "active"
NeverStageVerificationWrites == ~environmentState.stagedWrites
NeverRecordPublishIntent == ~environmentState.publishIntent
NeverAuthorizeActivation == environmentState.activationReceipt # ActivationReceipt
NeverPublishAuthorizedGeneration == ~environmentState.publishOutcome
NeverFinalizeReady == ~environmentState.finalObservation
NeverRecoverAuthorizationOnly == ~environmentState.authorizationRecovered
NeverRecoverPublishedUnobserved == ~environmentState.publishedUnobservedRecovered
NeverAuthorizeRollback == environmentState.rollbackPhase # "authorized"
NeverRestorePredecessorUnderNewEpoch == environmentState.rollbackPhase # "restored"
NeverReachRollbackAuthorizationBeforeWriterEpochRotation ==
    ~(/\ environmentState.rollbackPhase = "authorized"
      /\ environmentState.databaseWriterEpoch # RollbackDatabaseWriterEpoch
      /\ environmentState.targetDatabaseReceipt # RollbackReceipt)
NeverReachAtomicRollbackWriterEpochPublication ==
    ~(/\ environmentState.rollbackPhase = "restored"
      /\ environmentState.databaseWriterEpoch = RollbackDatabaseWriterEpoch
      /\ environmentState.targetDatabaseReceipt = RollbackReceipt
      /\ environmentState.currentGeneration = PredecessorGeneration)
NeverFinalizeRollback == environmentState.rollbackPhase # "observed"
NeverPlanCompensationForIntended ==
    compensationState.plan # "exact" \/ compensationState.forwardPhase # "intended"
NeverPlanCompensationForApplied ==
    compensationState.plan # "exact" \/ compensationState.forwardPhase # "applied"
NeverPlanCompensationForObserved ==
    compensationState.plan # "exact" \/ compensationState.forwardPhase # "observed"
NeverPlanCompensationForCommitted ==
    compensationState.plan # "exact" \/ compensationState.forwardPhase # "committed"
NeverExcludePlannedFromCompensation ==
    compensationState.plan # "exact" \/ compensationState.forwardPhase # "planned"
    \/ compensationState.required
NeverCompensateIrreversibleEffect ==
    effectState.kind # "irreversible" \/ compensationState.phase # "compensated"
NeverFinalizeAuthorizationOnlyRollback ==
    ~(/\ environmentState.authorizationOnlyRollback
      /\ environmentState.rollbackPhase = "observed"
      /\ attemptState.lifecycle = "rolled_back")
NeverCrashBeforeAuthorization ==
    attemptState.running \/ attemptState.lifecycle # "verifying"
    \/ environmentState.activationReceipt # None
NeverCrashAuthorizedBeforePublish ==
    attemptState.running \/ environmentState.activationReceipt # ActivationReceipt
    \/ environmentState.readerVisible
NeverCrashPublishedBeforeFinalObservation ==
    attemptState.running \/ ~environmentState.readerVisible
    \/ environmentState.finalObservation
NeverRecoverFromAdmitted == <<"admitted", "recovering">> \notin eventHistory
NeverRecoverFromApplying == <<"applying", "recovering">> \notin eventHistory
NeverRecoverFromVerifying == <<"verifying", "recovering">> \notin eventHistory
NeverRecoverFromReady == <<"ready", "recovering">> \notin eventHistory
NeverCrashPlanned == attemptState.lifecycle # "planned" \/ attemptState.running
NeverCrashAdmitted == attemptState.lifecycle # "admitted" \/ attemptState.running
NeverCrashEffectIntended == effectState.phase # "intended" \/ attemptState.running
NeverCrashEffectApplied == effectState.phase # "applied" \/ attemptState.running
NeverCrashEffectObserved == effectState.phase # "observed" \/ attemptState.running
NeverCrashEffectCommitted == effectState.phase # "committed" \/ attemptState.running
NeverCrashCompensationIntended == compensationState.phase # "intended" \/ attemptState.running
NeverCrashCompensationApplied == compensationState.phase # "applied" \/ attemptState.running
NeverCrashCompensationObserved == compensationState.phase # "observed" \/ attemptState.running
NeverCrashCompensated == compensationState.phase # "compensated" \/ attemptState.running
NeverRejectChangedStaging ==
    attemptState.lifecycle # "blocked" \/ environmentState.stagingContext # "staging-context-2"
NeverRejectStoppedMutation == "stopped_rejected" \notin mutationHistory

Spec == Init /\ [][Next]_vars
       /\ WF_vars(Restart)
       /\ WF_vars(RequestRecovery)
       /\ WF_vars(RecoveryStep)

=============================================================================
