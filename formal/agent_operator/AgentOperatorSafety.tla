---------------------- MODULE AgentOperatorSafety ----------------------
EXTENDS TLC

CONSTANTS EnableUnsafeApproval,
          EnableDuplicatePoll,
          EnableBackstopSuppression,
          EnableStartedReap

ApprovalCases == {
    "eligible", "malformed", "ambiguous", "over_budget", "retraction"
}

VARIABLES approvalCase, admission,
          watch, backstopDue, pollEvaluated, pollOutcome, baselineAdvanced,
          suppressionReceipt, watchAppliedObservedToken,
          workflow, reaped

vars == <<approvalCase, admission,
          watch, backstopDue, pollEvaluated, pollOutcome, baselineAdvanced,
          suppressionReceipt, watchAppliedObservedToken,
          workflow, reaped>>

Init ==
    /\ approvalCase \in ApprovalCases
    /\ admission = "pending"
    /\ watch = "idle"
    /\ backstopDue \in BOOLEAN
    /\ pollEvaluated = FALSE
    /\ pollOutcome = "none"
    /\ baselineAdvanced = FALSE
    /\ suppressionReceipt = FALSE
    /\ watchAppliedObservedToken \in BOOLEAN
    /\ workflow = "never_started"
    /\ reaped = FALSE

AutoApprove ==
    /\ admission = "pending"
    /\ approvalCase = "eligible"
    /\ admission' = "accepted"
    /\ UNCHANGED <<approvalCase, watch, backstopDue, pollEvaluated,
                    pollOutcome, baselineAdvanced, suppressionReceipt,
                    watchAppliedObservedToken, workflow, reaped>>

UnsafeAutoApprove ==
    /\ EnableUnsafeApproval
    /\ admission = "pending"
    /\ approvalCase # "eligible"
    /\ admission' = "accepted"
    /\ UNCHANGED <<approvalCase, watch, backstopDue, pollEvaluated,
                    pollOutcome, baselineAdvanced, suppressionReceipt,
                    watchAppliedObservedToken, workflow, reaped>>

StartWatch ==
    /\ watch = "idle"
    /\ watch' = "active"
    /\ UNCHANGED <<approvalCase, admission, backstopDue, pollEvaluated,
                    pollOutcome, baselineAdvanced, suppressionReceipt,
                    watchAppliedObservedToken, workflow, reaped>>

PollDuringActiveWatch ==
    /\ watch = "active"
    /\ ~backstopDue
    /\ ~pollEvaluated
    /\ pollEvaluated' = TRUE
    /\ pollOutcome' = "suppressed"
    /\ baselineAdvanced' = FALSE
    /\ suppressionReceipt' = TRUE
    /\ UNCHANGED <<approvalCase, admission, watch, backstopDue,
                    watchAppliedObservedToken, workflow, reaped>>

DuplicatePollDuringActiveWatch ==
    /\ EnableDuplicatePoll
    /\ watch = "active"
    /\ ~backstopDue
    /\ ~pollEvaluated
    /\ pollEvaluated' = TRUE
    /\ pollOutcome' = "event_enqueued"
    /\ baselineAdvanced' = TRUE
    /\ suppressionReceipt' = FALSE
    /\ UNCHANGED <<approvalCase, admission, watch, backstopDue,
                    watchAppliedObservedToken, workflow, reaped>>

CompleteWatch ==
    /\ watch = "active"
    /\ watch' = "complete"
    /\ UNCHANGED <<approvalCase, admission, backstopDue, pollEvaluated,
                    pollOutcome, baselineAdvanced, suppressionReceipt,
                    watchAppliedObservedToken, workflow, reaped>>

FailWatch ==
    /\ watch = "active"
    /\ watch' = "failed"
    /\ UNCHANGED <<approvalCase, admission, backstopDue, pollEvaluated,
                    pollOutcome, baselineAdvanced, suppressionReceipt,
                    watchAppliedObservedToken, workflow, reaped>>

PollReadyAfterWatch ==
    ~pollEvaluated \/ pollOutcome = "suppressed"

PollAfterComplete ==
    /\ watch = "complete"
    /\ ~backstopDue
    /\ PollReadyAfterWatch
    /\ suppressionReceipt
    /\ watchAppliedObservedToken
    /\ pollEvaluated' = TRUE
    /\ pollOutcome' = "baseline_advanced"
    /\ baselineAdvanced' = TRUE
    /\ UNCHANGED <<approvalCase, admission, watch, backstopDue,
                    suppressionReceipt, watchAppliedObservedToken,
                    workflow, reaped>>

PollAfterCompleteUncovered ==
    /\ watch = "complete"
    /\ ~backstopDue
    /\ PollReadyAfterWatch
    /\ ~(suppressionReceipt /\ watchAppliedObservedToken)
    /\ pollEvaluated' = TRUE
    /\ pollOutcome' = "event_enqueued"
    /\ baselineAdvanced' = FALSE
    /\ suppressionReceipt' = FALSE
    /\ UNCHANGED <<approvalCase, admission, watch, backstopDue,
                    watchAppliedObservedToken, workflow, reaped>>

PollAfterFailure ==
    /\ watch = "failed"
    /\ ~backstopDue
    /\ PollReadyAfterWatch
    /\ pollEvaluated' = TRUE
    /\ pollOutcome' = "event_enqueued"
    /\ baselineAdvanced' = FALSE
    /\ suppressionReceipt' = FALSE
    /\ UNCHANGED <<approvalCase, admission, watch, backstopDue,
                    watchAppliedObservedToken, workflow, reaped>>

PollBackstop ==
    /\ backstopDue
    /\ ~pollEvaluated
    /\ pollEvaluated' = TRUE
    /\ pollOutcome' = "backstop_enqueued"
    /\ baselineAdvanced' = FALSE
    /\ suppressionReceipt' = FALSE
    /\ UNCHANGED <<approvalCase, admission, watch, backstopDue,
                    watchAppliedObservedToken, workflow, reaped>>

SuppressBackstop ==
    /\ EnableBackstopSuppression
    /\ backstopDue
    /\ ~pollEvaluated
    /\ pollEvaluated' = TRUE
    /\ pollOutcome' = "backstop_suppressed"
    /\ baselineAdvanced' = FALSE
    /\ suppressionReceipt' = FALSE
    /\ UNCHANGED <<approvalCase, admission, watch, backstopDue,
                    watchAppliedObservedToken, workflow, reaped>>

StartWorkflow ==
    /\ workflow = "never_started"
    /\ ~reaped
    /\ workflow' = "started"
    /\ UNCHANGED <<approvalCase, admission, watch, backstopDue,
                    pollEvaluated, pollOutcome, baselineAdvanced,
                    suppressionReceipt, watchAppliedObservedToken, reaped>>

ReapNeverStarted ==
    /\ workflow = "never_started"
    /\ reaped' = TRUE
    /\ UNCHANGED <<approvalCase, admission, watch, backstopDue,
                    pollEvaluated, pollOutcome, baselineAdvanced,
                    suppressionReceipt, watchAppliedObservedToken, workflow>>

ReapStarted ==
    /\ EnableStartedReap
    /\ workflow = "started"
    /\ reaped' = TRUE
    /\ UNCHANGED <<approvalCase, admission, watch, backstopDue,
                    pollEvaluated, pollOutcome, baselineAdvanced,
                    suppressionReceipt, watchAppliedObservedToken, workflow>>

Idle == UNCHANGED vars

Next ==
    \/ AutoApprove
    \/ UnsafeAutoApprove
    \/ StartWatch
    \/ PollDuringActiveWatch
    \/ DuplicatePollDuringActiveWatch
    \/ CompleteWatch
    \/ FailWatch
    \/ PollAfterComplete
    \/ PollAfterCompleteUncovered
    \/ PollAfterFailure
    \/ PollBackstop
    \/ SuppressBackstop
    \/ StartWorkflow
    \/ ReapNeverStarted
    \/ ReapStarted
    \/ Idle

TypeOK ==
    /\ approvalCase \in ApprovalCases
    /\ admission \in {"pending", "accepted"}
    /\ watch \in {"idle", "active", "complete", "failed"}
    /\ backstopDue \in BOOLEAN
    /\ pollEvaluated \in BOOLEAN
    /\ pollOutcome \in {
        "none", "suppressed", "baseline_advanced", "event_enqueued",
        "backstop_enqueued", "backstop_suppressed"
       }
    /\ baselineAdvanced \in BOOLEAN
    /\ suppressionReceipt \in BOOLEAN
    /\ watchAppliedObservedToken \in BOOLEAN
    /\ workflow \in {"never_started", "started"}
    /\ reaped \in BOOLEAN

ApprovalIsBounded ==
    admission = "accepted" => approvalCase = "eligible"

ActiveWatchSuppressesOrdinaryPoll ==
    watch = "active" /\ ~backstopDue /\ pollEvaluated
      => pollOutcome = "suppressed" /\ ~baselineAdvanced

BackstopRetainsAuthority ==
    backstopDue /\ pollEvaluated => pollOutcome = "backstop_enqueued"

BaselineAdvanceRequiresAppliedDelegation ==
    baselineAdvanced =>
      watch = "complete" /\ suppressionReceipt /\ watchAppliedObservedToken

StartedWorkflowIsNotReaped ==
    workflow = "started" => ~reaped

AgentOperatorRuntimeSafety ==
    /\ TypeOK
    /\ ApprovalIsBounded
    /\ ActiveWatchSuppressesOrdinaryPoll
    /\ BackstopRetainsAuthority
    /\ BaselineAdvanceRequiresAppliedDelegation
    /\ StartedWorkflowIsNotReaped

Spec == Init /\ [][Next]_vars

=============================================================================
