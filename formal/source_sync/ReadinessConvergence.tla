---- MODULE ReadinessConvergence ----
EXTENDS Naturals

FreshnessStates == {"fresh", "stale", "missing"}
RunModes == {"none", "scope_incomplete", "scope_complete", "reconcile"}
WorkAges == {"fresh", "stale"}
ActiveWorkKinds == {"none", "started_pending", "protected_enqueued", "replaceable_enqueued"}
SourceStates == {"fresh", "converging", "blocked", "stuck", "manual_required", "unsupported"}
CompletedRunModes == {"scope_complete", "reconcile"}
RecoveryActionKinds == {
  "no_op",
  "observe_active_completed_scope_recovery",
  "completed_scope_reconcile",
  "restart_started_workflow",
  "requeue_protected_workflow",
  "cancel_replaceable_workflow",
  "manual_or_credential_action",
  "unsupported"
}

VARIABLES
  freshness,
  activeRunMode,
  activeWorkAge,
  activeWorkKind,
  manualRequired,
  supported,
  readinessRequired,
  published,
  mcpVisible

vars == <<
  freshness,
  activeRunMode,
  activeWorkAge,
  activeWorkKind,
  manualRequired,
  supported,
  readinessRequired,
  published,
  mcpVisible
>>

CompletedFreshRun ==
  /\ activeRunMode \in CompletedRunModes
  /\ activeWorkAge = "fresh"

SourceState ==
  IF ~readinessRequired THEN "fresh"
  ELSE IF freshness = "fresh" /\ ~published THEN "blocked"
  ELSE IF freshness = "fresh" /\ ~mcpVisible THEN "blocked"
  ELSE IF freshness = "fresh" THEN "fresh"
  ELSE IF manualRequired THEN "manual_required"
  ELSE IF ~supported THEN "unsupported"
  ELSE IF activeRunMode \in CompletedRunModes /\ activeWorkAge = "fresh" THEN "converging"
  ELSE IF activeRunMode \in CompletedRunModes /\ activeWorkAge = "stale" THEN "stuck"
  ELSE "blocked"

RecoveryActionKind ==
  IF ~readinessRequired \/ SourceState = "fresh" THEN "no_op"
  ELSE IF SourceState = "manual_required" THEN "manual_or_credential_action"
  ELSE IF SourceState = "unsupported" THEN "unsupported"
  ELSE IF activeRunMode \in CompletedRunModes /\ activeWorkAge = "fresh"
    THEN "observe_active_completed_scope_recovery"
  ELSE IF activeRunMode \in CompletedRunModes /\ activeWorkAge = "stale" /\ activeWorkKind = "started_pending"
    THEN "restart_started_workflow"
  ELSE IF activeRunMode \in CompletedRunModes /\ activeWorkAge = "stale" /\ activeWorkKind = "protected_enqueued"
    THEN "requeue_protected_workflow"
  ELSE IF activeRunMode \in CompletedRunModes /\ activeWorkAge = "stale" /\ activeWorkKind = "replaceable_enqueued"
    THEN "cancel_replaceable_workflow"
  ELSE "completed_scope_reconcile"

Ready ==
  /\ readinessRequired
  /\ SourceState = "fresh"
  /\ freshness = "fresh"
  /\ published
  /\ mcpVisible

Init ==
  /\ freshness = "missing"
  /\ activeRunMode = "none"
  /\ activeWorkAge = "fresh"
  /\ activeWorkKind = "none"
  /\ manualRequired = FALSE
  /\ supported = TRUE
  /\ readinessRequired = TRUE
  /\ published = FALSE
  /\ mcpVisible = FALSE

StartIncompleteScope ==
  /\ supported
  /\ ~manualRequired
  /\ freshness' = "stale"
  /\ activeRunMode' = "scope_incomplete"
  /\ activeWorkAge' = "fresh"
  /\ activeWorkKind' = "replaceable_enqueued"
  /\ published' = FALSE
  /\ mcpVisible' = FALSE
  /\ UNCHANGED << manualRequired, supported, readinessRequired >>

CompleteFreshRun ==
  /\ supported
  /\ ~manualRequired
  /\ activeRunMode' \in CompletedRunModes
  /\ activeWorkAge' = "fresh"
  /\ activeWorkKind' = "started_pending"
  /\ freshness' = "fresh"
  /\ published' = FALSE
  /\ mcpVisible' = FALSE
  /\ UNCHANGED << manualRequired, supported, readinessRequired >>

PublishGeneration ==
  /\ freshness = "fresh"
  /\ CompletedFreshRun
  /\ ~published
  /\ published' = TRUE
  /\ mcpVisible' = FALSE
  /\ UNCHANGED << freshness, activeRunMode, activeWorkAge, activeWorkKind, manualRequired, supported, readinessRequired >>

ExposeReadSide ==
  /\ freshness = "fresh"
  /\ CompletedFreshRun
  /\ published
  /\ mcpVisible' = TRUE
  /\ UNCHANGED << freshness, activeRunMode, activeWorkAge, activeWorkKind, manualRequired, supported, readinessRequired, published >>

StaleCompletedRecovery ==
  /\ supported
  /\ ~manualRequired
  /\ activeRunMode' \in CompletedRunModes
  /\ activeWorkAge' = "stale"
  /\ activeWorkKind' \in {"started_pending", "protected_enqueued", "replaceable_enqueued"}
  /\ freshness' \in {"stale", "missing"}
  /\ published' = FALSE
  /\ mcpVisible' = FALSE
  /\ UNCHANGED << manualRequired, supported, readinessRequired >>

RequireManualAction ==
  /\ manualRequired' = TRUE
  /\ published' = FALSE
  /\ mcpVisible' = FALSE
  /\ UNCHANGED << freshness, activeRunMode, activeWorkAge, activeWorkKind, supported, readinessRequired >>

MarkUnsupported ==
  /\ supported' = FALSE
  /\ published' = FALSE
  /\ mcpVisible' = FALSE
  /\ UNCHANGED << freshness, activeRunMode, activeWorkAge, activeWorkKind, manualRequired, readinessRequired >>

MarkNotReadinessRequired ==
  /\ readinessRequired' = FALSE
  /\ UNCHANGED << freshness, activeRunMode, activeWorkAge, activeWorkKind, manualRequired, supported, published, mcpVisible >>

NoChange ==
  UNCHANGED vars

Step ==
  \/ StartIncompleteScope
  \/ CompleteFreshRun
  \/ PublishGeneration
  \/ ExposeReadSide
  \/ StaleCompletedRecovery
  \/ RequireManualAction
  \/ MarkUnsupported
  \/ MarkNotReadinessRequired
  \/ NoChange

Spec == Init /\ [][Step]_vars

TypeOK ==
  /\ freshness \in FreshnessStates
  /\ activeRunMode \in RunModes
  /\ activeWorkAge \in WorkAges
  /\ activeWorkKind \in ActiveWorkKinds
  /\ manualRequired \in BOOLEAN
  /\ supported \in BOOLEAN
  /\ readinessRequired \in BOOLEAN
  /\ published \in BOOLEAN
  /\ mcpVisible \in BOOLEAN
  /\ SourceState \in SourceStates
  /\ RecoveryActionKind \in RecoveryActionKinds

NoFalseReadyFromIncompleteWork ==
  ~(Ready /\ activeRunMode = "scope_incomplete" /\ freshness # "fresh")

EventualReadyRequiresCompletedFreshnessAndPublishedGeneration ==
  \* This is a safety precondition for Ready, not a liveness claim that
  \* every source eventually becomes ready.
  Ready => /\ CompletedFreshRun
           /\ freshness = "fresh"
           /\ published
           /\ mcpVisible

FrontierAdvanceResumeIsSafe ==
  (readinessRequired /\ SourceState = "fresh")
    => /\ freshness = "fresh"
       /\ published
       /\ mcpVisible

FreshWatermarkRequiresPublishAndMcpVisibility ==
  (readinessRequired /\ freshness = "fresh" /\ SourceState = "fresh")
    => /\ published
       /\ mcpVisible

McpVisibilityRequiresPublishedGeneration ==
  mcpVisible => published

NoHiddenStuckRecoveryWork ==
  (readinessRequired /\ activeRunMode \in CompletedRunModes /\ activeWorkAge = "stale" /\ freshness # "fresh" /\ ~manualRequired /\ supported)
    => SourceState = "stuck"

SourceSyncRecoveryBounded ==
  (readinessRequired /\ activeRunMode \in CompletedRunModes /\ activeWorkAge = "stale" /\ freshness # "fresh" /\ ~manualRequired /\ supported)
    => /\ SourceState = "stuck"
       /\ RecoveryActionKind \in {
            "restart_started_workflow",
            "requeue_protected_workflow",
            "cancel_replaceable_workflow"
          }

ReadyWitness ==
  /\ readinessRequired
  /\ supported
  /\ ~manualRequired
  /\ CompletedFreshRun
  /\ freshness = "fresh"
  /\ published
  /\ mcpVisible
  /\ Ready

StuckWitness ==
  /\ readinessRequired
  /\ supported
  /\ ~manualRequired
  /\ activeRunMode \in CompletedRunModes
  /\ activeWorkAge = "stale"
  /\ activeWorkKind \in {"started_pending", "protected_enqueued", "replaceable_enqueued"}
  /\ freshness # "fresh"
  /\ SourceState = "stuck"
  /\ RecoveryActionKind # "observe_active_completed_scope_recovery"

McpBlockedWitness ==
  /\ readinessRequired
  /\ supported
  /\ ~manualRequired
  /\ CompletedFreshRun
  /\ freshness = "fresh"
  /\ published
  /\ ~mcpVisible
  /\ SourceState = "blocked"
  /\ ~Ready

====
