-------------------------- MODULE ReplaceableCutover --------------------------
EXTENDS Naturals, TLC

(***************************************************************************
 * Bounded application-version cutover model. The gate row has an exact     *
 * workflow identity by construction; exactQueue, oldVersion, status, and   *
 * started capture the remaining SQL classification inputs. A started       *
 * PENDING row is never eligible because issue 1254 does not own live-       *
 * executor cancellation.                                                    *
 ****************************************************************************)

VARIABLES phase, sourceSyncState, gateState, customState, currentGateState,
          gateStatus, exactQueue, oldVersion, started, ready, launchSucceeds

vars == <<phase, sourceSyncState, gateState, customState, currentGateState,
          gateStatus, exactQueue, oldVersion, started, ready, launchSucceeds>>

ActiveStatuses == {"ENQUEUED", "DELAYED", "PENDING"}

GateEligible ==
    /\ exactQueue
    /\ oldVersion
    /\ gateStatus \in ActiveStatuses
    /\ (gateStatus # "PENDING" \/ ~started)

Init ==
    /\ phase = "Start"
    /\ sourceSyncState = "Active"
    /\ gateState = "Active"
    /\ customState \in {"Active", "Absent"}
    /\ currentGateState = "Active"
    /\ gateStatus \in ActiveStatuses
    /\ exactQueue \in BOOLEAN
    /\ oldVersion \in BOOLEAN
    /\ started \in BOOLEAN
    /\ gateStatus # "PENDING" => ~started
    /\ ready = FALSE
    /\ launchSucceeds \in BOOLEAN

BlockUnsafe ==
    /\ phase = "Start"
    /\ (customState = "Active" \/ ~GateEligible)
    /\ phase' = "Blocked"
    /\ ready' = FALSE
    /\ UNCHANGED <<sourceSyncState, gateState, customState, currentGateState,
                    gateStatus, exactQueue, oldVersion, started, launchSucceeds>>

Cleanup ==
    /\ phase = "Start"
    /\ customState = "Absent"
    /\ GateEligible
    /\ phase' = "Cleaned"
    /\ sourceSyncState' = "Cancelled"
    /\ gateState' = "Cancelled"
    /\ UNCHANGED <<customState, currentGateState, gateStatus, exactQueue,
                    oldVersion, started, ready, launchSucceeds>>

CommitReady ==
    /\ phase = "Cleaned"
    /\ launchSucceeds
    /\ phase' = "Committed"
    /\ ready' = TRUE
    /\ UNCHANGED <<sourceSyncState, gateState, customState, currentGateState,
                    gateStatus, exactQueue, oldVersion, started, launchSucceeds>>

Rollback ==
    /\ phase = "Cleaned"
    /\ ~launchSucceeds
    /\ phase' = "RolledBack"
    /\ sourceSyncState' = "Active"
    /\ gateState' = "Active"
    /\ ready' = FALSE
    /\ UNCHANGED <<customState, currentGateState, gateStatus, exactQueue,
                    oldVersion, started, launchSucceeds>>

Next == BlockUnsafe \/ Cleanup \/ CommitReady \/ Rollback

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

TypeOK ==
    /\ phase \in {"Start", "Cleaned", "Blocked", "Committed", "RolledBack"}
    /\ sourceSyncState \in {"Active", "Cancelled"}
    /\ gateState \in {"Active", "Cancelled"}
    /\ customState \in {"Active", "Absent"}
    /\ currentGateState = "Active"
    /\ gateStatus \in ActiveStatuses
    /\ exactQueue \in BOOLEAN
    /\ oldVersion \in BOOLEAN
    /\ started \in BOOLEAN
    /\ ready \in BOOLEAN
    /\ launchSucceeds \in BOOLEAN

CurrentVersionUnchanged == currentGateState = "Active"

BlockedCutoverCommitsNoMutation ==
    phase = "Blocked" =>
        /\ sourceSyncState = "Active"
        /\ gateState = "Active"
        /\ ~ready

ReadinessRequiresCommittedCleanup ==
    ready =>
        /\ phase = "Committed"
        /\ sourceSyncState = "Cancelled"
        /\ gateState = "Cancelled"
        /\ customState = "Absent"
        /\ GateEligible

UnknownOrUnsafeWorkflowBlocksReadiness ==
    (customState = "Active" \/ ~GateEligible) => ~ready

StartedPendingNeverCancelled ==
    (gateStatus = "PENDING" /\ started) => gateState = "Active"

RollbackRestoresReplaceableRows ==
    phase = "RolledBack" =>
        /\ sourceSyncState = "Active"
        /\ gateState = "Active"
        /\ ~ready

ReplaceableCutoverSafety ==
    /\ TypeOK
    /\ CurrentVersionUnchanged
    /\ BlockedCutoverCommitsNoMutation
    /\ ReadinessRequiresCommittedCleanup
    /\ UnknownOrUnsafeWorkflowBlocksReadiness
    /\ StartedPendingNeverCancelled
    /\ RollbackRestoresReplaceableRows

EligibleSuccessfulCutover ==
    /\ phase = "Start"
    /\ customState = "Absent"
    /\ GateEligible
    /\ launchSucceeds

EligibleSuccessfulCutoverLeadsToReady == EligibleSuccessfulCutover ~> ready

=============================================================================
