---- MODULE ReusePass ----
\* Resolution-aware reuse pass: resolved-state equality under certificate skips.
\*
\* System under test: src/okg/substrate/consistency/reuse_scheduler.py (the
\* worklist state machine + the skip ladder) composed with
\* src/okg/substrate/consistency/reuse_certificates.py (validity EVALUATED at
\* decision time). Normative semantics: docs/develop/architecture/consistency.md
\* sections 9.4-9.6 (possible-effects, certificates, update protocol).
\*
\* The claim this model checks (section 9.7): after the reuse worklist drains to a
\* fixed point and the generation commits, the incrementally maintained resolved
\* state equals the state a FULL recompute would produce for the SAME delta, on
\* EVERY behavior. Two guards keep that true; disabling either is a named
\* counterexample (see the two *Counterexample.cfg files):
\*   (a) GuardDynamicConsumer -- a step whose RUN would dynamically create a NEW
\*       consumer step (a new check candidate arising from cross-claim
\*       interaction, NOT from the step's own recorded dependencies) must RUN, not
\*       skip. This is the least-witnessed section-9 assumption.
\*   (b) CertIgnoresStaleClass=FALSE -- a certificate is valid only when EVERY
\*       recorded dependency class matches the current facts; dropping a class
\*       admits a skip whose carried-forward output has actually changed.
\*
\* Model boundary (documented in formal/consistency/README.md): the ten
\* DEPENDENCY_CLASSES are abstracted to three representative ones; a step's
\* output is the identity on its recorded-fact record (so any recorded-class
\* change is an output change -- the faithful over-approximation); the
\* bounded-dominance rung's authority arithmetic and the discard/abort fallbacks
\* are OUT of scope for this equality invariant (covered by the claim-core model
\* and the scheduler's runtime tests).

EXTENDS FiniteSets, Naturals

CONSTANTS
  BaseSteps,              \* seed steps present in the prior generation (carry prior facts)
  Spawner,                \* the base step whose RUN dynamically creates Consumer
  Consumer,               \* the new consumer step, absent from the prior generation
  DepClasses,             \* representative dependency classes (abstraction of the 10)
  StaleClass,             \* the dependency class the buggy certificate check drops
  GuardDynamicConsumer,   \* TRUE = sound: refuse to skip a step whose RUN would spawn a new consumer
  CertIgnoresStaleClass   \* FALSE = sound: a certificate is valid only if EVERY class matches

AllSteps == BaseSteps \cup {Consumer}

\* The dependency classes a (possibly buggy) certificate check consults. Sound is
\* every class; the stale-certificate witness drops StaleClass, so a certificate
\* can validate while that class has actually changed.
CheckedClasses ==
  IF CertIgnoresStaleClass THEN DepClasses \ {StaleClass} ELSE DepClasses

VARIABLES
  currentDirty,   \* [BaseSteps -> [DepClasses -> BOOLEAN]] : this cycle's delta vs the clean prior baseline
  spawnActive,    \* BOOLEAN : this cycle a new-consumer condition arose for Spawner (a new bucket / check candidate)
  reached,        \* SUBSET AllSteps : steps added to the worklist (seed + dynamically discovered dependents)
  decided,        \* [AllSteps -> {"none","skip","run"}] : the ladder outcome per reached step
  committed       \* BOOLEAN : the worklist drained to a fixed point and the generation committed

vars == <<currentDirty, spawnActive, reached, decided, committed>>

CleanFacts == [k \in DepClasses |-> FALSE]   \* the prior generation's baseline (WLOG all-clean)
ConsumerVal == "consumer_output"             \* the new consumer's contribution to the resolved state
Absent == "absent"                           \* a step that contributes nothing (never reached / run)

HasPrior(s) == s \in BaseSteps               \* Consumer is brand-new: no prior facts, no prior certificate
WouldSpawn(s) == (s = Spawner) /\ spawnActive  \* running s dynamically creates Consumer this cycle

\* A step's RUN reproduces its fresh output; a SKIP carries the prior (clean)
\* output forward. The output is the recorded-fact record, so it differs from the
\* baseline iff ANY recorded class changed -- the faithful over-approximation
\* (the resolved output depends on every recorded dependency).
FreshOutput(s) == currentDirty[s]
PriorOutput(s) == CleanFacts

\* CertMatch: every class the check consults matches the recorded (prior/clean)
\* facts. A prior certificate recorded the clean baseline, so validity reduces to
\* "no consulted class changed" -- the single legality test standing in for all
\* sound skip rungs (valid prior certificate / degenerate unchanged-inputs /
\* bounded dominance); see the Model Boundary in the folder README.
CertMatch(s) == \A k \in CheckedClasses : currentDirty[s][k] = FALSE

\* SkipLegal: the ladder admits a skip only behind a matching certificate AND
\* (soundly) only when the RUN would not spawn a new consumer. Disabling either
\* conjunct is a named counterexample config.
SkipLegal(s) ==
  /\ HasPrior(s)
  /\ CertMatch(s)
  /\ (GuardDynamicConsumer => ~WouldSpawn(s))

Processable(s) ==
  /\ s \in reached
  /\ decided[s] = "none"

Init ==
  /\ currentDirty \in [BaseSteps -> [DepClasses -> BOOLEAN]]
  /\ spawnActive \in BOOLEAN
  /\ reached = BaseSteps
  /\ decided = [s \in AllSteps |-> "none"]
  /\ committed = FALSE

\* RUN a reached step: its observed dependents extend the worklist. The only
\* dynamically discovered dependent modeled is Consumer, reached iff the Spawner
\* runs while the new-consumer condition is active (section 9.6 worklist growth).
RunStep(s) ==
  /\ ~committed
  /\ Processable(s)
  /\ decided' = [decided EXCEPT ![s] = "run"]
  /\ reached' = IF WouldSpawn(s) THEN reached \cup {Consumer} ELSE reached
  /\ UNCHANGED <<currentDirty, spawnActive, committed>>

\* SKIP a reached step behind a certificate (the only skip mechanism, section 9.5).
SkipStep(s) ==
  /\ ~committed
  /\ Processable(s)
  /\ SkipLegal(s)
  /\ decided' = [decided EXCEPT ![s] = "skip"]
  /\ UNCHANGED <<currentDirty, spawnActive, reached, committed>>

\* COMMIT only at the worklist's fixed point: every reached step decided
\* (section 9.6 -- commit_gate refuses before the fixed point).
Commit ==
  /\ ~committed
  /\ \A s \in reached : decided[s] # "none"
  /\ committed' = TRUE
  /\ UNCHANGED <<currentDirty, spawnActive, reached, decided>>

Done ==
  /\ committed
  /\ UNCHANGED vars

Next ==
  \/ \E s \in AllSteps : RunStep(s) \/ SkipStep(s)
  \/ Commit
  \/ Done

Spec == Init /\ [][Next]_vars

\* --------------------------------------------------------------------------- \*
\* The full-recompute reference and the incremental result.                     \*
\* --------------------------------------------------------------------------- \*
\* Full recompute: every reachable step RUNS. Consumer is reachable iff the delta
\* activated the new-consumer condition.
FullState ==
  [ s \in AllSteps |->
      CASE s \in BaseSteps -> FreshOutput(s)
        [] s = Consumer    -> IF spawnActive THEN ConsumerVal ELSE Absent ]

\* Incremental result the reuse pass produced: a SKIP carries the prior output, a
\* RUN reproduces the fresh output, a step never reached contributes nothing.
IncrState ==
  [ s \in AllSteps |->
      CASE s \in BaseSteps -> IF decided[s] = "skip" THEN PriorOutput(s) ELSE FreshOutput(s)
        [] s = Consumer    -> IF (Consumer \in reached /\ decided[Consumer] = "run")
                              THEN ConsumerVal ELSE Absent ]

\* --------------------------------------------------------------------------- \*
\* Invariants.                                                                   \*
\* --------------------------------------------------------------------------- \*
TypeOK ==
  /\ currentDirty \in [BaseSteps -> [DepClasses -> BOOLEAN]]
  /\ spawnActive \in BOOLEAN
  /\ reached \in SUBSET AllSteps
  /\ BaseSteps \subseteq reached
  /\ decided \in [AllSteps -> {"none", "skip", "run"}]
  /\ committed \in BOOLEAN

\* Headline (section 9.7): once the worklist drains and the generation commits,
\* the incrementally maintained resolved state equals the full-recompute resolved
\* state for the same delta, on EVERY behavior.
ResolvedStateEquality ==
  committed => (IncrState = FullState)

\* Decomposition A -- the dynamic-consumer guard. A skip whose RUN would have
\* spawned a new consumer omits that consumer's contribution. Named target of
\* ReusePassDynamicConsumerCounterexample.cfg (GuardDynamicConsumer = FALSE).
NoOmittedConsumer ==
  committed => (spawnActive => (Consumer \in reached))

\* Decomposition B -- certificate completeness. A skipped step's carried-forward
\* output must equal its fresh output, i.e. a skip is admitted only when NO
\* recorded dependency class changed. Named target of
\* ReusePassStaleCertCounterexample.cfg (CertIgnoresStaleClass = TRUE), where a
\* certificate validates while StaleClass has actually moved.
SkippedOutputsUnchanged ==
  committed =>
    \A s \in BaseSteps :
      (decided[s] = "skip") => (FreshOutput(s) = PriorOutput(s))

ReusePassSoundness ==
  /\ TypeOK
  /\ NoOmittedConsumer
  /\ SkippedOutputsUnchanged
  /\ ResolvedStateEquality

====
