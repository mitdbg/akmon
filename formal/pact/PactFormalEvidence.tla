---- MODULE PactFormalEvidence ----
EXTENDS FiniteSets

\* Formal evidence gating, rewritten for honesty (strengthen-formal-models
\* wave 4). The previous model excluded the post-verification staleness
\* hazard by guarding AttachStaleFormal with lifecycle # "verification" —
\* proving "verification implies fresh forever", a property the system does
\* not have (model rewrites staled 67 evidence rows in closed changes before
\* wave 1). The real contract has two halves, both modeled now:
\*   1. ENTRY freshness: verification advances only while the formal
\*      evidence is fresh at that moment (the validation gate). The
\*      BuggyAdvanceIgnoresFreshness toggle makes the guard's load-bearing
\*      role witnessable by name.
\*   2. DETECTION after entry: evidence CAN go stale after verification
\*      (StaleAfterVerification below is unguarded); the corpus freshness
\*      sweep must then flag it — a sweep that runs and stays silent about a
\*      stale row violates NoSilentStaleAfterVerification.

CONSTANTS
  NoFormal,
  StaleFormal,
  FreshFormal,
  BroadAcceptance,
  SpecificAcceptance,
  BuggyAdvanceIgnoresFreshness,
  BuggySweepMissesStale

FormalStates == {NoFormal, StaleFormal, FreshFormal}
AcceptanceStates == {BroadAcceptance, SpecificAcceptance}
LifecycleStates == {"proposal", "implementation", "verification"}

VARIABLES
  lifecycle,
  implementationApproved,
  lifecycleBypass,
  formalEvidence,
  acceptanceEvidence,
  taskDone,
  enteredVerificationStale,
  sweepRan,
  staleDetected

vars == <<
  lifecycle,
  implementationApproved,
  lifecycleBypass,
  formalEvidence,
  acceptanceEvidence,
  taskDone,
  enteredVerificationStale,
  sweepRan,
  staleDetected
>>

Init ==
  /\ lifecycle = "proposal"
  /\ implementationApproved = FALSE
  /\ lifecycleBypass = FALSE
  /\ formalEvidence = NoFormal
  /\ acceptanceEvidence = BroadAcceptance
  /\ taskDone = FALSE
  /\ enteredVerificationStale = FALSE
  /\ sweepRan = FALSE
  /\ staleDetected = FALSE

ApproveImplementation ==
  /\ lifecycle' = "implementation"
  /\ implementationApproved' = TRUE
  /\ UNCHANGED <<
       lifecycleBypass, formalEvidence, acceptanceEvidence, taskDone,
       enteredVerificationStale, sweepRan, staleDetected
     >>

DeclareLifecycleBypass ==
  /\ lifecycleBypass' = TRUE
  /\ UNCHANGED <<
       lifecycle, implementationApproved, formalEvidence,
       acceptanceEvidence, taskDone, enteredVerificationStale, sweepRan,
       staleDetected
     >>

\* Evidence can go stale at ANY point — before or after verification. A
\* model rewrite does not consult the change's lifecycle.
AttachStaleFormal ==
  /\ formalEvidence' = StaleFormal
  /\ sweepRan' = FALSE
  /\ staleDetected' = FALSE
  /\ UNCHANGED <<
       lifecycle, implementationApproved, lifecycleBypass,
       acceptanceEvidence, taskDone, enteredVerificationStale
     >>

AttachFreshFormal ==
  /\ formalEvidence' = FreshFormal
  /\ sweepRan' = FALSE
  /\ staleDetected' = FALSE
  /\ UNCHANGED <<
       lifecycle, implementationApproved, lifecycleBypass,
       acceptanceEvidence, taskDone, enteredVerificationStale
     >>

MapSpecificAcceptance ==
  /\ acceptanceEvidence' = SpecificAcceptance
  /\ UNCHANGED <<
       lifecycle, implementationApproved, lifecycleBypass, formalEvidence,
       taskDone, enteredVerificationStale, sweepRan, staleDetected
     >>

CompleteTask ==
  /\ taskDone' = TRUE
  /\ UNCHANGED <<
       lifecycle, implementationApproved, lifecycleBypass, formalEvidence,
       acceptanceEvidence, enteredVerificationStale, sweepRan, staleDetected
     >>

\* The validation gate: verification advances only with fresh formal
\* evidence at entry (plus semantic acceptance, a completed task, and
\* implementation approval or an explicit bypass). The buggy toggle drops
\* the freshness conjunct so the VerificationEnteredFresh invariant is
\* witnessable by name.
AdvanceVerification ==
  /\ taskDone
  /\ formalEvidence = FreshFormal \/ BuggyAdvanceIgnoresFreshness
  /\ acceptanceEvidence = SpecificAcceptance
  /\ implementationApproved \/ lifecycleBypass
  /\ lifecycle' = "verification"
  /\ enteredVerificationStale' = (formalEvidence # FreshFormal)
  /\ UNCHANGED <<
       implementationApproved, lifecycleBypass, formalEvidence,
       acceptanceEvidence, taskDone, sweepRan, staleDetected
     >>

\* The corpus freshness sweep (wave 1): flags stale fresh-claiming evidence
\* wherever the lifecycle stands. The buggy toggle models a sweep that runs
\* and stays silent — the no-silent-staleness invariant catches it.
SweepCorpus ==
  /\ sweepRan' = TRUE
  /\ staleDetected' =
       IF formalEvidence = StaleFormal /\ ~BuggySweepMissesStale
       THEN TRUE
       ELSE staleDetected
  /\ UNCHANGED <<
       lifecycle, implementationApproved, lifecycleBypass, formalEvidence,
       acceptanceEvidence, taskDone, enteredVerificationStale
     >>

Stutter ==
  UNCHANGED vars

Next ==
  \/ ApproveImplementation
  \/ DeclareLifecycleBypass
  \/ AttachStaleFormal
  \/ AttachFreshFormal
  \/ MapSpecificAcceptance
  \/ CompleteTask
  \/ AdvanceVerification
  \/ SweepCorpus
  \/ Stutter

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ lifecycle \in LifecycleStates
  /\ implementationApproved \in BOOLEAN
  /\ lifecycleBypass \in BOOLEAN
  /\ formalEvidence \in FormalStates
  /\ acceptanceEvidence \in AcceptanceStates
  /\ taskDone \in BOOLEAN
  /\ enteredVerificationStale \in BOOLEAN
  /\ sweepRan \in BOOLEAN
  /\ staleDetected \in BOOLEAN

VerificationRequiresImplementationOrBypass ==
  lifecycle = "verification" => implementationApproved \/ lifecycleBypass

\* ENTRY freshness (history form): no verification advance ever happened
\* while the formal evidence was stale. Witnessable: the buggy-advance
\* toggle makes TLC exhibit a stale-entry trace. NOTE: this trusts
\* AdvanceVerification's enteredVerificationStale assignment — any future
\* action that enters verification must set the flag from the evidence
\* state, never unconditionally FALSE, or this invariant is blinded.
VerificationEnteredFresh ==
  ~enteredVerificationStale

VerificationRequiresSemanticAcceptance ==
  lifecycle = "verification" => acceptanceEvidence = SpecificAcceptance

VerificationRequiresCompletedTask ==
  lifecycle = "verification" => taskDone

\* DETECTION: a sweep that has run while a verified change's formal
\* evidence is stale must have flagged it. The buggy-sweep toggle makes the
\* silent-miss trace exhibitable by name.
NoSilentStaleAfterVerification ==
  (/\ lifecycle = "verification"
   /\ formalEvidence = StaleFormal
   /\ sweepRan)
  => staleDetected

PactFormalEvidenceFresh ==
  /\ TypeOK
  /\ VerificationRequiresImplementationOrBypass
  /\ VerificationEnteredFresh
  /\ VerificationRequiresSemanticAcceptance
  /\ VerificationRequiresCompletedTask
  /\ NoSilentStaleAfterVerification

====
