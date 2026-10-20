---- MODULE ReadinessConvergenceLiveness ----
EXTENDS Naturals

\* Liveness over the REAL readiness machine (strengthen-formal-models wave
\* 5). The retired ReadinessLiveness.tla proved <>Ready on a 5-state
\* deterministic chain with no failure or interference branch — a fairness
\* template demo whose ReadyIsStable claim its own sibling safety model
\* (ReadinessConvergence) contradicts. This module carries the sibling's
\* transition structure, including the disruptions that make its liveness
\* genuinely hard: StartIncompleteScope and StaleCompletedRecovery reset
\* published/mcpVisible (the livelock the audit named), and
\* RequireManualAction / MarkUnsupported park the source.
\*
\* The theorem is honest about its assumptions:
\*   - Disruptions are BOUNDED (MaxDisruptions budget) — the model form of
\*     "the environment eventually quiesces"; unbounded disruption trivially
\*     livelocks any scheduler and no worker can promise readiness under it.
\*   - The worker is fair: WF on the completing run, SF on publish and
\*     read-side exposure (their enablement is intermittent — a new run
\*     resets published — so weak fairness is insufficient; strong fairness
\*     mirrors the DBOS scheduler retrying enabled work).
\* Under those assumptions: the source eventually becomes Ready OR parks in
\* an operator-visible terminal state (manual_required / unsupported /
\* readiness not required). Dropping the fairness conjunction makes TLC
\* refute the property (pinned by tests/formal/test_readiness_liveness_witness.py),
\* so the theorem is not vacuously satisfiable.

CONSTANTS MaxDisruptions

FreshnessStates == {"fresh", "stale", "missing"}
RunModes == {"none", "scope_incomplete", "scope_complete", "reconcile"}
WorkAges == {"fresh", "stale"}
ActiveWorkKinds == {
  "none", "started_pending", "protected_enqueued", "replaceable_enqueued"
}
CompletedRunModes == {"scope_complete", "reconcile"}

VARIABLES
  freshness,
  activeRunMode,
  activeWorkAge,
  activeWorkKind,
  manualRequired,
  supported,
  readinessRequired,
  published,
  mcpVisible,
  disruptions

vars == <<
  freshness, activeRunMode, activeWorkAge, activeWorkKind, manualRequired,
  supported, readinessRequired, published, mcpVisible, disruptions
>>

Ready ==
  /\ readinessRequired
  /\ freshness = "fresh"
  /\ published
  /\ mcpVisible

Parked ==
  \/ manualRequired
  \/ ~supported
  \/ ~readinessRequired

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
  /\ disruptions = 0

\* --- Disruptions (budgeted) -------------------------------------------
StartIncompleteScope ==
  /\ disruptions < MaxDisruptions
  /\ supported /\ ~manualRequired
  /\ freshness' = "stale"
  /\ activeRunMode' = "scope_incomplete"
  /\ activeWorkAge' = "fresh"
  /\ activeWorkKind' = "replaceable_enqueued"
  /\ published' = FALSE
  /\ mcpVisible' = FALSE
  /\ disruptions' = disruptions + 1
  /\ UNCHANGED << manualRequired, supported, readinessRequired >>

StaleCompletedRecovery ==
  /\ disruptions < MaxDisruptions
  /\ supported /\ ~manualRequired
  /\ activeRunMode' \in CompletedRunModes
  /\ activeWorkAge' = "stale"
  /\ activeWorkKind' \in {
       "started_pending", "protected_enqueued", "replaceable_enqueued"
     }
  /\ freshness' \in {"stale", "missing"}
  /\ published' = FALSE
  /\ mcpVisible' = FALSE
  /\ disruptions' = disruptions + 1
  /\ UNCHANGED << manualRequired, supported, readinessRequired >>

RequireManualAction ==
  /\ disruptions < MaxDisruptions
  /\ manualRequired' = TRUE
  /\ published' = FALSE
  /\ mcpVisible' = FALSE
  /\ disruptions' = disruptions + 1
  /\ UNCHANGED <<
       freshness, activeRunMode, activeWorkAge, activeWorkKind, supported,
       readinessRequired
     >>

MarkUnsupported ==
  /\ disruptions < MaxDisruptions
  /\ supported' = FALSE
  /\ published' = FALSE
  /\ mcpVisible' = FALSE
  /\ disruptions' = disruptions + 1
  /\ UNCHANGED <<
       freshness, activeRunMode, activeWorkAge, activeWorkKind,
       manualRequired, readinessRequired
     >>

\* --- Worker progress (fair) --------------------------------------------
CompleteFreshRun ==
  /\ supported /\ ~manualRequired
  /\ ~(freshness = "fresh")
  /\ activeRunMode' \in CompletedRunModes
  /\ activeWorkAge' = "fresh"
  /\ activeWorkKind' = "started_pending"
  /\ freshness' = "fresh"
  /\ published' = FALSE
  /\ mcpVisible' = FALSE
  /\ disruptions' = disruptions
  /\ UNCHANGED << manualRequired, supported, readinessRequired >>

PublishGeneration ==
  /\ freshness = "fresh"
  /\ activeRunMode \in CompletedRunModes
  /\ activeWorkAge = "fresh"
  /\ ~published
  /\ published' = TRUE
  /\ mcpVisible' = FALSE
  /\ disruptions' = disruptions
  /\ UNCHANGED <<
       freshness, activeRunMode, activeWorkAge, activeWorkKind,
       manualRequired, supported, readinessRequired
     >>

ExposeReadSide ==
  /\ freshness = "fresh"
  /\ activeRunMode \in CompletedRunModes
  /\ activeWorkAge = "fresh"
  /\ published
  /\ ~mcpVisible
  /\ mcpVisible' = TRUE
  /\ disruptions' = disruptions
  /\ UNCHANGED <<
       freshness, activeRunMode, activeWorkAge, activeWorkKind,
       manualRequired, supported, readinessRequired, published
     >>

Step ==
  \/ StartIncompleteScope
  \/ StaleCompletedRecovery
  \/ RequireManualAction
  \/ MarkUnsupported
  \/ CompleteFreshRun
  \/ PublishGeneration
  \/ ExposeReadSide

\* Fair worker: the completing run is retried while enabled (WF); publish
\* and exposure fire despite intermittent enablement (SF — a fresh run
\* resets published, so their enablement flickers).
Spec ==
  /\ Init
  /\ [][Step]_vars
  /\ WF_vars(CompleteFreshRun)
  /\ SF_vars(PublishGeneration)
  /\ SF_vars(ExposeReadSide)

\* Same machine, no fairness: TLC refutes EventuallyReadyOrParked here (a
\* lazy worker may stutter forever) — the non-triviality witness.
SpecNoFairness == Init /\ [][Step]_vars

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
  /\ disruptions \in 0..MaxDisruptions

\* THE liveness theorem: under bounded disruption and a fair worker, every
\* trace eventually reaches Ready or an operator-visible parked state — a
\* source cannot hover in converging/stuck/blocked forever.
EventuallyReadyOrParked == <>(Ready \/ Parked)

====
