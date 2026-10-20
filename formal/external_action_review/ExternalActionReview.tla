---- MODULE ExternalActionReview ----
EXTENDS Naturals

(***************************************************************************
 Design-wave lifecycle model for okg#1182. It models durable review state,
 stale prepared mutations, stable separation subjects, the single external
 claim/outcome, replay disclosure, and the explicit no-provider-I/O boundary.
 The production wave must retain these properties and add runtime refinement.
*)

Subjects == {"proposer", "approver", "worker1", "worker2", "reconciler", "attester"}
ReviewStates == {"none", "pending", "edited", "approved", "rejected", "expired", "cancelled"}
ExecutionStates == {"unavailable", "ready", "claimed", "succeeded", "failed", "indeterminate"}
Outcomes == {"none", "succeeded", "failed", "indeterminate"}
Hashes == {"none", "h1", "h2"}
DecisionIds == {"none", "d1"}
NoSubject == "none"
ReviewDeadline == 2
ExecuteBefore == 2
MaxLifecycle == 8
EffectDigest == "effect-v1"
ReconcileDelay == 1

VARIABLES
  reviewState, executionState, lifecycleVersion, payloadVersion,
  currentPayloadHash, originalPayloadHash, proposerSubject,
  decisionId, decisionLifecycle, decisionPayloadVersion,
  decisionPayloadHash, decisionEffectDigest, approverSubject,
  claimOwner, claimAt, claimEffectDigest, outcome, outcomeReporter,
  databaseTime, preparedApprovalLifecycle, preparedApprovalPayloadVersion,
  preparedApprovalPayloadHash, preparedApprover,
  preparedCancelLifecycle, preparedCancelDecisionId,
  replayDisclosedTo, ownerRevoked, attestationValid, providerCalls

vars == <<
  reviewState, executionState, lifecycleVersion, payloadVersion,
  currentPayloadHash, originalPayloadHash, proposerSubject,
  decisionId, decisionLifecycle, decisionPayloadVersion,
  decisionPayloadHash, decisionEffectDigest, approverSubject,
  claimOwner, claimAt, claimEffectDigest, outcome, outcomeReporter,
  databaseTime, preparedApprovalLifecycle, preparedApprovalPayloadVersion,
  preparedApprovalPayloadHash, preparedApprover,
  preparedCancelLifecycle, preparedCancelDecisionId,
  replayDisclosedTo, ownerRevoked, attestationValid, providerCalls
>>

Init ==
  /\ reviewState = "none"
  /\ executionState = "unavailable"
  /\ lifecycleVersion = 0
  /\ payloadVersion = 0
  /\ currentPayloadHash = "none"
  /\ originalPayloadHash = "none"
  /\ proposerSubject = NoSubject
  /\ decisionId = "none"
  /\ decisionLifecycle = 0
  /\ decisionPayloadVersion = 0
  /\ decisionPayloadHash = "none"
  /\ decisionEffectDigest = "none"
  /\ approverSubject = NoSubject
  /\ claimOwner = NoSubject
  /\ claimAt = 0
  /\ claimEffectDigest = "none"
  /\ outcome = "none"
  /\ outcomeReporter = NoSubject
  /\ databaseTime = 0
  /\ preparedApprovalLifecycle = 0
  /\ preparedApprovalPayloadVersion = 0
  /\ preparedApprovalPayloadHash = "none"
  /\ preparedApprover = NoSubject
  /\ preparedCancelLifecycle = 0
  /\ preparedCancelDecisionId = "none"
  /\ replayDisclosedTo = NoSubject
  /\ ownerRevoked = FALSE
  /\ attestationValid = FALSE
  /\ providerCalls = 0

Submit ==
  /\ reviewState = "none"
  /\ reviewState' = "pending"
  /\ lifecycleVersion' = 1
  /\ payloadVersion' = 1
  /\ currentPayloadHash' = "h1"
  /\ originalPayloadHash' = "h1"
  /\ proposerSubject' = "proposer"
  /\ UNCHANGED <<executionState, decisionId, decisionLifecycle,
       decisionPayloadVersion, decisionPayloadHash, decisionEffectDigest,
       approverSubject, claimOwner, claimAt, claimEffectDigest, outcome,
       outcomeReporter, databaseTime, preparedApprovalLifecycle,
       preparedApprovalPayloadVersion, preparedApprovalPayloadHash,
       preparedApprover, preparedCancelLifecycle, preparedCancelDecisionId,
       replayDisclosedTo, ownerRevoked, attestationValid, providerCalls>>

Edit ==
  /\ reviewState \in {"pending", "edited"}
  /\ payloadVersion = 1
  /\ lifecycleVersion < MaxLifecycle
  /\ databaseTime < ReviewDeadline
  /\ reviewState' = "edited"
  /\ lifecycleVersion' = lifecycleVersion + 1
  /\ payloadVersion' = 2
  /\ currentPayloadHash' = "h2"
  /\ UNCHANGED <<executionState, originalPayloadHash, proposerSubject,
       decisionId, decisionLifecycle, decisionPayloadVersion,
       decisionPayloadHash, decisionEffectDigest, approverSubject, claimOwner,
       claimAt, claimEffectDigest, outcome, outcomeReporter, databaseTime,
       preparedApprovalLifecycle, preparedApprovalPayloadVersion,
       preparedApprovalPayloadHash, preparedApprover,
       preparedCancelLifecycle, preparedCancelDecisionId,
       replayDisclosedTo, ownerRevoked, attestationValid, providerCalls>>

Defer ==
  /\ reviewState \in {"pending", "edited"}
  /\ lifecycleVersion < MaxLifecycle
  /\ databaseTime < ReviewDeadline
  /\ lifecycleVersion' = lifecycleVersion + 1
  /\ UNCHANGED <<reviewState, executionState, payloadVersion,
       currentPayloadHash, originalPayloadHash, proposerSubject, decisionId,
       decisionLifecycle, decisionPayloadVersion, decisionPayloadHash,
       decisionEffectDigest, approverSubject, claimOwner, claimAt,
       claimEffectDigest, outcome, outcomeReporter, databaseTime,
       preparedApprovalLifecycle, preparedApprovalPayloadVersion,
       preparedApprovalPayloadHash, preparedApprover,
       preparedCancelLifecycle, preparedCancelDecisionId,
       replayDisclosedTo, ownerRevoked, attestationValid, providerCalls>>

PrepareApproval(actor) ==
  /\ actor \in Subjects
  /\ reviewState \in {"pending", "edited"}
  /\ preparedApprovalLifecycle' = lifecycleVersion
  /\ preparedApprovalPayloadVersion' = payloadVersion
  /\ preparedApprovalPayloadHash' = currentPayloadHash
  /\ preparedApprover' = actor
  /\ UNCHANGED <<reviewState, executionState, lifecycleVersion,
       payloadVersion, currentPayloadHash, originalPayloadHash,
       proposerSubject, decisionId, decisionLifecycle, decisionPayloadVersion,
       decisionPayloadHash, decisionEffectDigest, approverSubject, claimOwner,
       claimAt, claimEffectDigest, outcome, outcomeReporter, databaseTime,
       preparedCancelLifecycle, preparedCancelDecisionId,
       replayDisclosedTo, ownerRevoked, attestationValid, providerCalls>>

ApplyPreparedApproval ==
  /\ reviewState \in {"pending", "edited"}
  /\ databaseTime < ReviewDeadline
  /\ preparedApprover \in Subjects
  /\ preparedApprover # proposerSubject
  /\ preparedApprovalLifecycle = lifecycleVersion
  /\ preparedApprovalPayloadVersion = payloadVersion
  /\ preparedApprovalPayloadHash = currentPayloadHash
  /\ lifecycleVersion < MaxLifecycle
  /\ reviewState' = "approved"
  /\ executionState' = "ready"
  /\ lifecycleVersion' = lifecycleVersion + 1
  /\ decisionId' = "d1"
  /\ decisionLifecycle' = lifecycleVersion + 1
  /\ decisionPayloadVersion' = payloadVersion
  /\ decisionPayloadHash' = currentPayloadHash
  /\ decisionEffectDigest' = EffectDigest
  /\ approverSubject' = preparedApprover
  /\ UNCHANGED <<payloadVersion, currentPayloadHash, originalPayloadHash,
       proposerSubject, claimOwner, claimAt, claimEffectDigest, outcome,
       outcomeReporter, databaseTime, preparedApprovalLifecycle,
       preparedApprovalPayloadVersion, preparedApprovalPayloadHash,
       preparedApprover, preparedCancelLifecycle, preparedCancelDecisionId,
       replayDisclosedTo, ownerRevoked, attestationValid, providerCalls>>

Reject ==
  /\ reviewState \in {"pending", "edited"}
  /\ databaseTime < ReviewDeadline
  /\ lifecycleVersion < MaxLifecycle
  /\ reviewState' = "rejected"
  /\ lifecycleVersion' = lifecycleVersion + 1
  /\ UNCHANGED <<executionState, payloadVersion, currentPayloadHash,
       originalPayloadHash, proposerSubject, decisionId, decisionLifecycle,
       decisionPayloadVersion, decisionPayloadHash, decisionEffectDigest,
       approverSubject, claimOwner, claimAt, claimEffectDigest, outcome,
       outcomeReporter, databaseTime, preparedApprovalLifecycle,
       preparedApprovalPayloadVersion, preparedApprovalPayloadHash,
       preparedApprover, preparedCancelLifecycle, preparedCancelDecisionId,
       replayDisclosedTo, ownerRevoked, attestationValid, providerCalls>>

PrepareCancel ==
  /\ reviewState \in {"pending", "edited", "approved"}
  /\ preparedCancelLifecycle' = lifecycleVersion
  /\ preparedCancelDecisionId' = decisionId
  /\ UNCHANGED <<reviewState, executionState, lifecycleVersion,
       payloadVersion, currentPayloadHash, originalPayloadHash,
       proposerSubject, decisionId, decisionLifecycle, decisionPayloadVersion,
       decisionPayloadHash, decisionEffectDigest, approverSubject, claimOwner,
       claimAt, claimEffectDigest, outcome, outcomeReporter, databaseTime,
       preparedApprovalLifecycle, preparedApprovalPayloadVersion,
       preparedApprovalPayloadHash, preparedApprover,
       replayDisclosedTo, ownerRevoked, attestationValid, providerCalls>>

ApplyPreparedCancel ==
  /\ reviewState \in {"pending", "edited", "approved"}
  /\ executionState # "claimed"
  /\ preparedCancelLifecycle = lifecycleVersion
  /\ (reviewState = "approved" => preparedCancelDecisionId = decisionId)
  /\ lifecycleVersion < MaxLifecycle
  /\ reviewState' = "cancelled"
  /\ executionState' = "unavailable"
  /\ lifecycleVersion' = lifecycleVersion + 1
  /\ UNCHANGED <<payloadVersion, currentPayloadHash, originalPayloadHash,
       proposerSubject, decisionId, decisionLifecycle, decisionPayloadVersion,
       decisionPayloadHash, decisionEffectDigest, approverSubject, claimOwner,
       claimAt, claimEffectDigest, outcome, outcomeReporter, databaseTime,
       preparedApprovalLifecycle, preparedApprovalPayloadVersion,
       preparedApprovalPayloadHash, preparedApprover,
       preparedCancelLifecycle, preparedCancelDecisionId,
       replayDisclosedTo, ownerRevoked, attestationValid, providerCalls>>

AdvanceTime ==
  /\ databaseTime < 2
  /\ databaseTime' = databaseTime + 1
  /\ UNCHANGED <<reviewState, executionState, lifecycleVersion,
       payloadVersion, currentPayloadHash, originalPayloadHash,
       proposerSubject, decisionId, decisionLifecycle, decisionPayloadVersion,
       decisionPayloadHash, decisionEffectDigest, approverSubject, claimOwner,
       claimAt, claimEffectDigest, outcome, outcomeReporter,
       preparedApprovalLifecycle, preparedApprovalPayloadVersion,
       preparedApprovalPayloadHash, preparedApprover,
       preparedCancelLifecycle, preparedCancelDecisionId,
       replayDisclosedTo, ownerRevoked, attestationValid, providerCalls>>

MaterializeExpiry ==
  /\ lifecycleVersion < MaxLifecycle
  /\ \/ /\ reviewState \in {"pending", "edited"}
        /\ databaseTime >= ReviewDeadline
     \/ /\ reviewState = "approved"
        /\ executionState = "ready"
        /\ databaseTime >= ExecuteBefore
  /\ reviewState' = "expired"
  /\ executionState' = "unavailable"
  /\ lifecycleVersion' = lifecycleVersion + 1
  /\ UNCHANGED <<payloadVersion, currentPayloadHash, originalPayloadHash,
       proposerSubject, decisionId, decisionLifecycle, decisionPayloadVersion,
       decisionPayloadHash, decisionEffectDigest, approverSubject, claimOwner,
       claimAt, claimEffectDigest, outcome, outcomeReporter, databaseTime,
       preparedApprovalLifecycle, preparedApprovalPayloadVersion,
       preparedApprovalPayloadHash, preparedApprover,
       preparedCancelLifecycle, preparedCancelDecisionId,
       replayDisclosedTo, ownerRevoked, attestationValid, providerCalls>>

Claim(actor) ==
  /\ actor \in {"worker1", "worker2"}
  /\ reviewState = "approved"
  /\ executionState = "ready"
  /\ databaseTime < ExecuteBefore
  /\ decisionPayloadVersion = payloadVersion
  /\ decisionPayloadHash = currentPayloadHash
  /\ decisionEffectDigest = EffectDigest
  /\ lifecycleVersion < MaxLifecycle
  /\ executionState' = "claimed"
  /\ lifecycleVersion' = lifecycleVersion + 1
  /\ claimOwner' = actor
  /\ claimAt' = databaseTime
  /\ claimEffectDigest' = decisionEffectDigest
  /\ UNCHANGED <<reviewState, payloadVersion, currentPayloadHash,
       originalPayloadHash, proposerSubject, decisionId, decisionLifecycle,
       decisionPayloadVersion, decisionPayloadHash, decisionEffectDigest,
       approverSubject, outcome, outcomeReporter, databaseTime,
       preparedApprovalLifecycle, preparedApprovalPayloadVersion,
       preparedApprovalPayloadHash, preparedApprover,
       preparedCancelLifecycle, preparedCancelDecisionId,
       replayDisclosedTo, ownerRevoked, attestationValid, providerCalls>>

Report(actor, result) ==
  /\ actor = claimOwner
  /\ ~ownerRevoked
  /\ executionState = "claimed"
  /\ result \in {"succeeded", "failed", "indeterminate"}
  /\ lifecycleVersion < MaxLifecycle
  /\ executionState' = result
  /\ outcome' = result
  /\ outcomeReporter' = actor
  /\ lifecycleVersion' = lifecycleVersion + 1
  /\ UNCHANGED <<reviewState, payloadVersion, currentPayloadHash,
       originalPayloadHash, proposerSubject, decisionId, decisionLifecycle,
       decisionPayloadVersion, decisionPayloadHash, decisionEffectDigest,
       approverSubject, claimOwner, claimAt, claimEffectDigest, databaseTime,
       preparedApprovalLifecycle, preparedApprovalPayloadVersion,
       preparedApprovalPayloadHash, preparedApprover,
       preparedCancelLifecycle, preparedCancelDecisionId,
       replayDisclosedTo, ownerRevoked, attestationValid, providerCalls>>

ReconcileIndeterminate ==
  /\ executionState = "claimed"
  /\ ownerRevoked
  /\ attestationValid
  /\ databaseTime >= claimAt + ReconcileDelay
  /\ lifecycleVersion < MaxLifecycle
  /\ executionState' = "indeterminate"
  /\ outcome' = "indeterminate"
  /\ outcomeReporter' = "reconciler"
  /\ lifecycleVersion' = lifecycleVersion + 1
  /\ UNCHANGED <<reviewState, payloadVersion, currentPayloadHash,
       originalPayloadHash, proposerSubject, decisionId, decisionLifecycle,
       decisionPayloadVersion, decisionPayloadHash, decisionEffectDigest,
       approverSubject, claimOwner, claimAt, claimEffectDigest, databaseTime,
       preparedApprovalLifecycle, preparedApprovalPayloadVersion,
       preparedApprovalPayloadHash, preparedApprover,
       preparedCancelLifecycle, preparedCancelDecisionId,
       replayDisclosedTo, ownerRevoked, attestationValid, providerCalls>>

RevokeClaimOwner ==
  /\ executionState = "claimed"
  /\ ~ownerRevoked
  /\ ownerRevoked' = TRUE
  /\ UNCHANGED <<reviewState, executionState, lifecycleVersion,
       payloadVersion, currentPayloadHash, originalPayloadHash,
       proposerSubject, decisionId, decisionLifecycle, decisionPayloadVersion,
       decisionPayloadHash, decisionEffectDigest, approverSubject, claimOwner,
       claimAt, claimEffectDigest, outcome, outcomeReporter, databaseTime,
       preparedApprovalLifecycle, preparedApprovalPayloadVersion,
       preparedApprovalPayloadHash, preparedApprover,
       preparedCancelLifecycle, preparedCancelDecisionId,
       replayDisclosedTo, attestationValid, providerCalls>>

RecordOwnerUnavailabilityAttestation ==
  /\ executionState = "claimed"
  /\ ownerRevoked
  /\ ~attestationValid
  /\ databaseTime >= claimAt + ReconcileDelay
  /\ attestationValid' = TRUE
  /\ UNCHANGED <<reviewState, executionState, lifecycleVersion,
       payloadVersion, currentPayloadHash, originalPayloadHash,
       proposerSubject, decisionId, decisionLifecycle, decisionPayloadVersion,
       decisionPayloadHash, decisionEffectDigest, approverSubject, claimOwner,
       claimAt, claimEffectDigest, outcome, outcomeReporter, databaseTime,
       preparedApprovalLifecycle, preparedApprovalPayloadVersion,
       preparedApprovalPayloadHash, preparedApprover,
       preparedCancelLifecycle, preparedCancelDecisionId,
       replayDisclosedTo, ownerRevoked, providerCalls>>

ReplayOutcome(actor) ==
  /\ outcome # "none"
  /\ actor = outcomeReporter
  /\ (actor = claimOwner => ~ownerRevoked)
  /\ replayDisclosedTo' = actor
  /\ UNCHANGED <<reviewState, executionState, lifecycleVersion,
       payloadVersion, currentPayloadHash, originalPayloadHash,
       proposerSubject, decisionId, decisionLifecycle, decisionPayloadVersion,
       decisionPayloadHash, decisionEffectDigest, approverSubject, claimOwner,
       claimAt, claimEffectDigest, outcome, outcomeReporter, databaseTime,
       preparedApprovalLifecycle, preparedApprovalPayloadVersion,
       preparedApprovalPayloadHash, preparedApprover,
       preparedCancelLifecycle, preparedCancelDecisionId,
       ownerRevoked, attestationValid, providerCalls>>

Next ==
  \/ Submit
  \/ Edit
  \/ Defer
  \/ \E actor \in Subjects: PrepareApproval(actor)
  \/ ApplyPreparedApproval
  \/ Reject
  \/ PrepareCancel
  \/ ApplyPreparedCancel
  \/ AdvanceTime
  \/ MaterializeExpiry
  \/ \E actor \in {"worker1", "worker2"}: Claim(actor)
  \/ \E actor \in {"worker1", "worker2"}, result \in {"succeeded", "failed", "indeterminate"}: Report(actor, result)
  \/ RevokeClaimOwner
  \/ RecordOwnerUnavailabilityAttestation
  \/ ReconcileIndeterminate
  \/ \E actor \in Subjects: ReplayOutcome(actor)

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ reviewState \in ReviewStates
  /\ executionState \in ExecutionStates
  /\ lifecycleVersion \in 0..MaxLifecycle
  /\ payloadVersion \in 0..2
  /\ currentPayloadHash \in Hashes
  /\ originalPayloadHash \in Hashes
  /\ proposerSubject \in Subjects \cup {NoSubject}
  /\ decisionId \in DecisionIds
  /\ decisionLifecycle \in 0..MaxLifecycle
  /\ decisionPayloadVersion \in 0..2
  /\ decisionPayloadHash \in Hashes
  /\ approverSubject \in Subjects \cup {NoSubject}
  /\ claimOwner \in Subjects \cup {NoSubject}
  /\ claimAt \in 0..2
  /\ outcome \in Outcomes
  /\ outcomeReporter \in Subjects \cup {NoSubject}
  /\ databaseTime \in 0..2
  /\ replayDisclosedTo \in Subjects \cup {NoSubject}
  /\ ownerRevoked \in BOOLEAN
  /\ attestationValid \in BOOLEAN
  /\ providerCalls = 0

ImmutableOriginal == reviewState # "none" => originalPayloadHash = "h1"

SeparatedAuthority == decisionId # "none" => approverSubject # proposerSubject

DecisionBindsCurrent ==
  decisionId # "none" =>
    /\ decisionLifecycle > 0
    /\ decisionPayloadVersion = payloadVersion
    /\ decisionPayloadHash = currentPayloadHash
    /\ decisionEffectDigest = EffectDigest

StalePreparedCancelCannotRevoke ==
  decisionId # "none" /\ preparedCancelLifecycle > 0 /\
  preparedCancelLifecycle < decisionLifecycle => reviewState # "cancelled"

ClaimBindsApproval ==
  claimOwner # NoSubject =>
    /\ decisionId = "d1"
    /\ claimEffectDigest = decisionEffectDigest
    /\ claimAt < ExecuteBefore

OutcomeHasAuthorizedReporter ==
  outcome # "none" =>
    \/ outcomeReporter = claimOwner
    \/ /\ outcomeReporter = "reconciler"
       /\ outcome = "indeterminate"
       /\ ownerRevoked
       /\ attestationValid
       /\ databaseTime >= claimAt + ReconcileDelay

ReplayPreservesOwner ==
  replayDisclosedTo # NoSubject => replayDisclosedTo = outcomeReporter

ExternalActionReviewSafety ==
  /\ TypeOK
  /\ ImmutableOriginal
  /\ SeparatedAuthority
  /\ DecisionBindsCurrent
  /\ StalePreparedCancelCannotRevoke

ExternalActionExecutionSafety ==
  /\ ClaimBindsApproval
  /\ OutcomeHasAuthorizedReporter
  /\ ReplayPreservesOwner
  /\ providerCalls = 0

====
