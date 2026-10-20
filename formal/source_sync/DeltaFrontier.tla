---- MODULE DeltaFrontier ----
EXTENDS Naturals

\* Wave 5 (strengthen-formal-models): the previous revision was
\* one-shot — StartRun could fire once and nothing ever reset, so a
\* delta could never arrive after chronos applied and the two
\* pending-delta invariants had unsatisfiable antecedents. The frontier
\* is now multi-run: NewDeltaArrives resets the per-run flags (a newer
\* delta means chronos is behind and the frontier's readiness is not
\* yet visible), and ResumeRun re-enters the pipeline after a crash
\* with the durable flags intact. The buggy toggles break each
\* mechanism so TLC can refute the corresponding invariant by name.

CONSTANTS
  BuggyNewDeltaKeepsReadiness,
  BuggyResumeSkipsChronos

FrontierStates == {
  "requested",
  "source_progressed",
  "source_watermarked",
  "chronos_applied",
  "published_visible",
  "read_side_visible"
}

RunModes == {
  "cursor",
  "release_unchanged",
  "release_new",
  "scope_incomplete",
  "scope_complete",
  "reconcile"
}

VARIABLES
  state,
  runMode,
  deltaPending,
  progressCommitted,
  watermarkCommitted,
  chronosApplied,
  published,
  readVisible,
  widened,
  wideningReason

vars == <<
  state,
  runMode,
  deltaPending,
  progressCommitted,
  watermarkCommitted,
  chronosApplied,
  published,
  readVisible,
  widened,
  wideningReason
>>

CompletedModes == {
  "release_unchanged",
  "release_new",
  "scope_complete",
  "reconcile"
}

Init ==
  /\ state = "requested"
  /\ runMode = "cursor"
  /\ deltaPending = FALSE
  /\ progressCommitted = FALSE
  /\ watermarkCommitted = FALSE
  /\ chronosApplied = FALSE
  /\ published = FALSE
  /\ readVisible = FALSE
  /\ widened = FALSE
  /\ wideningReason = FALSE

StartRun ==
  /\ state = "requested"
  /\ runMode' \in RunModes
  /\ widened' \in BOOLEAN
  /\ wideningReason' \in BOOLEAN
  /\ widened' => wideningReason'
  /\ UNCHANGED <<
       state,
       deltaPending,
       progressCommitted,
       watermarkCommitted,
       chronosApplied,
       published,
       readVisible
     >>

CommitProgress ==
  /\ ~progressCommitted
  /\ progressCommitted' = TRUE
  /\ watermarkCommitted' = watermarkCommitted
  /\ chronosApplied' = chronosApplied
  /\ published' = published
  /\ readVisible' = readVisible
  /\ state' = "source_progressed"

CommitWatermark ==
  /\ progressCommitted
  /\ ~watermarkCommitted
  /\ watermarkCommitted' = TRUE
  /\ progressCommitted' = progressCommitted
  /\ chronosApplied' = chronosApplied
  /\ published' = published
  /\ readVisible' = readVisible
  /\ state' = "source_watermarked"

ApplyChronos ==
  /\ progressCommitted
  /\ watermarkCommitted
  /\ ~chronosApplied
  /\ chronosApplied' = TRUE
  /\ deltaPending' = FALSE
  /\ progressCommitted' = progressCommitted
  /\ watermarkCommitted' = watermarkCommitted
  /\ published' = published
  /\ readVisible' = readVisible
  /\ state' = "chronos_applied"

Publish ==
  /\ chronosApplied
  /\ ~published
  /\ published' = TRUE
  /\ progressCommitted' = progressCommitted
  /\ watermarkCommitted' = watermarkCommitted
  /\ chronosApplied' = chronosApplied
  /\ readVisible' = readVisible
  /\ state' = "published_visible"

ExposeReadSide ==
  /\ published
  /\ ~readVisible
  /\ readVisible' = TRUE
  /\ progressCommitted' = progressCommitted
  /\ watermarkCommitted' = watermarkCommitted
  /\ chronosApplied' = chronosApplied
  /\ published' = published
  /\ state' = "read_side_visible"

\* A delta newer than what chronos holds arrives at ANY point: the
\* frontier advances, so the per-run flags reset — chronos is behind and
\* this frontier's readiness is not yet visible. The buggy toggle keeps
\* readiness visible across the arrival, the exact stale-read hazard
\* NoReadyWithPendingUnappliedDelta exists to catch.
NewDeltaArrives ==
  /\ deltaPending' = TRUE
  /\ progressCommitted' = FALSE
  /\ watermarkCommitted' = FALSE
  /\ chronosApplied' = FALSE
  /\ published' = FALSE
  /\ readVisible' =
       IF BuggyNewDeltaKeepsReadiness THEN readVisible ELSE FALSE
  /\ state' = "requested"

\* Crash-resume: the pipeline re-enters with the DURABLE flags intact —
\* resume never skips unapplied work. The buggy toggle marks chronos
\* applied without applying (watermark says done, the graph never got the
\* delta — the frontier-skip hazard), tripping
\* ChronosNeverClaimsUnappliedDelta.
ResumeRun ==
  /\ state # "requested"
  /\ chronosApplied' =
       IF BuggyResumeSkipsChronos THEN TRUE ELSE chronosApplied
  /\ progressCommitted' = progressCommitted
  /\ watermarkCommitted' = watermarkCommitted
  /\ published' = published
  /\ readVisible' = readVisible
  /\ state' = "requested"

Stutter ==
  /\ progressCommitted' = progressCommitted
  /\ watermarkCommitted' = watermarkCommitted
  /\ chronosApplied' = chronosApplied
  /\ published' = published
  /\ readVisible' = readVisible
  /\ state' = state

StageStep ==
  \/ CommitProgress
  \/ CommitWatermark
  \/ Publish
  \/ ExposeReadSide
  \/ Stutter

Next ==
  \/ StartRun
  \/ /\ NewDeltaArrives
     /\ runMode' = runMode
     /\ widened' = widened
     /\ wideningReason' = wideningReason
  \/ /\ ResumeRun
     /\ runMode' = runMode
     /\ deltaPending' = deltaPending
     /\ widened' = widened
     /\ wideningReason' = wideningReason
  \/ /\ ApplyChronos
     /\ runMode' = runMode
     /\ widened' = widened
     /\ wideningReason' = wideningReason
  \/ /\ StageStep
     /\ runMode' = runMode
     /\ deltaPending' = deltaPending
     /\ widened' = widened
     /\ wideningReason' = wideningReason

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ state \in FrontierStates
  /\ runMode \in RunModes
  /\ deltaPending \in BOOLEAN
  /\ progressCommitted \in BOOLEAN
  /\ watermarkCommitted \in BOOLEAN
  /\ chronosApplied \in BOOLEAN
  /\ published \in BOOLEAN
  /\ readVisible \in BOOLEAN
  /\ widened \in BOOLEAN
  /\ wideningReason \in BOOLEAN

ReadinessVisible == readVisible

ReadinessRequiresPublishedChronos ==
  ReadinessVisible => /\ chronosApplied
                      /\ published

PublishedRequiresChronos ==
  published => chronosApplied

WatermarkRequiresProgress ==
  watermarkCommitted => progressCommitted

NoSilentWidening ==
  widened => wideningReason

NoReadyWithPendingUnappliedDelta ==
  ~(ReadinessVisible /\ deltaPending /\ ~chronosApplied)

ReadVisibilityRequiresFullFrontier ==
  ReadinessVisible => /\ progressCommitted
                      /\ watermarkCommitted
                      /\ chronosApplied
                      /\ published

PendingUnappliedDeltaNotReadVisible ==
  deltaPending /\ ~chronosApplied => ~ReadinessVisible

\* Chronos never CLAIMS a delta it did not apply: applied-with-pending is
\* unreachable in the honest model (ApplyChronos consumes the delta; a new
\* delta resets chronosApplied), and reachable exactly when a buggy resume
\* marks the apply done without doing it.
ChronosNeverClaimsUnappliedDelta ==
  ~(chronosApplied /\ deltaPending)

DeltaFrontierContract ==
  /\ TypeOK
  /\ ReadinessRequiresPublishedChronos
  /\ PublishedRequiresChronos
  /\ WatermarkRequiresProgress
  /\ NoSilentWidening
  /\ NoReadyWithPendingUnappliedDelta
  /\ ReadVisibilityRequiresFullFrontier
  /\ PendingUnappliedDeltaNotReadVisible
  /\ ChronosNeverClaimsUnappliedDelta

====
