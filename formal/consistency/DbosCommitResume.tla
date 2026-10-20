---- MODULE DbosCommitResume ----
\* Publish commit/resume protocol under the DBOS workflow (crash atomicity).
\*
\* System under test: src/okg/substrate/runtime/reuse_pass_workflow.py --
\* commit_maintenance_state commits the maintenance state (certificates +
\* generation checkpoint, standing in for the full claims / selections /
\* certificates / projection / checkpoint commit set of
\* consistency-resolution-aware-reuse req.reuse.single-mechanism) in ONE
\* transaction: the pass records tentative rows into the open transaction and a
\* single conn.commit() lands them with the checkpoint. The publisher's
\* chronos-native cycle (src/okg/substrate/publisher/publisher.py,
\* _cycle_chronos_native) makes the generation VISIBLE only at that checkpoint.
\* Normative semantics: docs/develop/architecture/consistency.md section 9.6.
\*
\* The DBOS workflow semantics modeled (the formal-strengthening umbrella's
\* open coordination-dbos wave): a publish workflow of ordered steps
\* (derive -> reuse -> admit -> commit-checkpoint), each step with a durable
\* completion marker; a crash may occur at any step boundary (volatile
\* transaction-buffered state is lost, durable state survives); resume restarts
\* the workflow from the top and a marker-complete step is NOT re-executed --
\* DBOS replays its recorded result WITHOUT re-creating its transactional
\* effects. The sound protocol therefore requires a step's marker to become
\* durable NO EARLIER than its effects: here markers ride the commit
\* transaction (the DBOS same-database boundary the workflow module names).
\* Each publish cycle is a fresh workflow instance over its own generation;
\* the per-cycle delta (what each step writes) is a rigid input ranged by TLC,
\* which is exactly the engine's determinism claim -- the pass is
\* deterministic FROM ITS INPUTS, so re-execution after a crash reproduces the
\* same bytes for the same delta. The existing crash-mid-update pytest witness
\* (tests/substrate/consistency/test_reuse_scheduler.py::
\* test_crash_mid_update_resumes_or_discards) is ONE trace of the sound
\* configuration -- crash between step and checkpoint, resume re-executes to a
\* byte-equal digest, commit lands both-or-neither; this model checks EVERY
\* crash/resume interleaving in the scope, which that single witness cannot.
\*
\* Three falsification levers, each a named counterexample config:
\*   (a) MarkerBeforeEffects -- the reuse step's durability marker is written
\*       BEFORE its effects are durable (the effects sit in the transaction
\*       buffer). A crash then strands the marker; resume SKIPS the step, its
\*       certificates are never re-created, and the commit checkpoints a
\*       generation without them: NoPartialGenerationObservable is refuted.
\*   (b) SplitCommitTxn -- the commit set lands in TWO transactions
\*       (certificates first, checkpoint second) instead of one. A crash
\*       between them strands durable certificates referencing a generation
\*       whose checkpoint never committed: NoCertWithoutCheckpoint is refuted.
\*   (c) NonIdempotentEffects -- the non-idempotent hazard: the reuse step
\*       writes its certificates durably OUTSIDE the transactional commit, with
\*       content derived from the attempt counter (a timestamp-like escape from
\*       input-determinism). Crash before the marker, resume re-executes, and
\*       the same generation is committed twice with different content:
\*       NoDoubleCommit is refuted.
\*
\* Model boundary (full statement in formal/consistency/README.md): NumGens
\* successive publish cycles, per-cycle delta inputs ranged over a two-value
\* alphabet per store, the reuse-pass fallback ladder (discard / abort) and
\* the blocked-publish path out of scope, and durable-store invariants
\* asserted at OBSERVABLE states (crash boundaries and termination) because
\* transaction isolation makes mid-commit durable states unobservable in the
\* engine -- the split-commit hazard makes them real exactly when a crash
\* strands them.

EXTENDS Integers, FiniteSets

CONSTANTS
  NumGens,              \* successive publish cycles (workflow instances)
  MaxAttempts,          \* per-cycle bound on executions (1 initial + resumes)
  MarkerBeforeEffects,  \* FALSE = sound: no marker before effects are durable
  SplitCommitTxn,       \* FALSE = sound: the commit set lands in ONE transaction
  NonIdempotentEffects  \* FALSE = sound: effects are transactional + input-deterministic

NoContent == -1   \* store member absent
NoGen == 0        \* the visible pointer before any generation commits

Generations == 1..NumGens

\* The atomic commit set: what derive / reuse / admit buffer into the open
\* transaction, plus the generation checkpoint written by the commit step.
PreStores == {"projection", "certs", "claims"}
Stores == PreStores \cup {"checkpoint"}
Steps == {"derive", "reuse", "admit", "commit"}
Phases == {"derive", "reuse", "admit", "commit", "commit_tail",
           "crashed", "done"}

\* Per-cycle delta alphabet: each pre-commit step's output for a generation is
\* one of two values -- the small-scope stand-in for "the pass's bytes are a
\* function of the cycle's inputs". Distinct from the hazard contents
\* (attempts run 1..MaxAttempts; delta values sit above them) so a
\* delta-derived write can never masquerade as an attempt-derived one.
DeltaValues == (MaxAttempts + 1)..(MaxAttempts + 2)

VARIABLES
  delta,     \* rigid input: [Generations -> [PreStores -> DeltaValues]]
  curGen,    \* the generation this workflow instance is publishing
  phase,     \* program counter of the workflow execution
  attempt,   \* which execution attempt of this cycle (bumped by every resume)
  buf,       \* [PreStores -> content]: the OPEN TRANSACTION buffer (volatile)
  dur,       \* [Generations -> [Stores -> content]]: durable stores
  visible,   \* the visible generation pointer (moves only at checkpoint)
  marker,    \* [Steps -> BOOLEAN]: durable DBOS step markers, per instance
  firstDur   \* ghost: first content ever durably committed per (gen, store)

vars == <<delta, curGen, phase, attempt, buf, dur, visible, marker, firstDur>>

Contents == {NoContent} \cup 0..(MaxAttempts + 2) \cup Generations

\* A sound step reproduces the same content on every attempt of the same cycle
\* (the engine's determinism claim: byte-equal ReusePassResult digest on
\* resume, because the bytes are a function of the cycle's delta). The
\* non-idempotent hazard derives content from the attempt counter instead.
StepContent(s) == IF NonIdempotentEffects THEN attempt ELSE delta[curGen][s]

\* The generation checkpoint's content is the generation id itself.
CheckpointContent == curGen

\* Ghost update: pin the first durable content each store of curGen ever
\* receives. A commit that carries NoContent (an omitted member) pins nothing.
PinFirst(ghost, newRow) ==
  [ghost EXCEPT ![curGen] =
     [s \in Stores |-> IF ghost[curGen][s] = NoContent
                       THEN newRow[s] ELSE ghost[curGen][s]]]

Init ==
  /\ delta \in [Generations -> [PreStores -> DeltaValues]]
  /\ curGen = 1
  /\ phase = "derive"
  /\ attempt = 1
  /\ buf = [s \in PreStores |-> NoContent]
  /\ dur = [g \in Generations |-> [s \in Stores |-> NoContent]]
  /\ visible = NoGen
  /\ marker = [s \in Steps |-> FALSE]
  /\ firstDur = [g \in Generations |-> [s \in Stores |-> NoContent]]

\* --------------------------------------------------------------------------- \*
\* Workflow steps. Each step: if its marker is durable, DBOS does NOT          \*
\* re-execute it (the recorded result is replayed; transactional effects are   \*
\* NOT re-created). Otherwise the step buffers its effects into the open       \*
\* transaction -- except under the named hazards.                              \*
\* --------------------------------------------------------------------------- \*
Derive ==
  /\ phase = "derive"
  /\ phase' = "reuse"
  /\ buf' = IF marker["derive"] THEN buf
            ELSE [buf EXCEPT !["projection"] = StepContent("projection")]
  /\ UNCHANGED <<delta, curGen, attempt, dur, visible, marker, firstDur>>

Reuse ==
  /\ phase = "reuse"
  /\ phase' = "admit"
  \* Sound: certificates enter the open transaction. Hazard (c): they land
  \* durably OUTSIDE it, immediately.
  /\ buf' = IF marker["reuse"] \/ NonIdempotentEffects THEN buf
            ELSE [buf EXCEPT !["certs"] = StepContent("certs")]
  /\ dur' = IF ~marker["reuse"] /\ NonIdempotentEffects
            THEN [dur EXCEPT ![curGen]["certs"] = StepContent("certs")]
            ELSE dur
  /\ firstDur' = IF ~marker["reuse"] /\ NonIdempotentEffects
                 THEN PinFirst(firstDur, dur'[curGen])
                 ELSE firstDur
  \* Sound: the step's marker rides the commit transaction below. Hazard (a):
  \* it is written durably NOW, before the effects are durable.
  /\ marker' = IF ~marker["reuse"] /\ MarkerBeforeEffects
               THEN [marker EXCEPT !["reuse"] = TRUE]
               ELSE marker
  /\ UNCHANGED <<delta, curGen, attempt, visible>>

Admit ==
  /\ phase = "admit"
  /\ phase' = "commit"
  /\ buf' = IF marker["admit"] THEN buf
            ELSE [buf EXCEPT !["claims"] = StepContent("claims")]
  /\ UNCHANGED <<delta, curGen, attempt, dur, visible, marker, firstDur>>

\* The commit step, already completed on a prior attempt: DBOS replays it.
CommitReplaySkip ==
  /\ phase = "commit"
  /\ marker["commit"]
  /\ phase' = "done"
  /\ UNCHANGED <<delta, curGen, attempt, buf, dur, visible, marker, firstDur>>

\* What the commit transaction writes per store of curGen: the transaction
\* buffer, except that a store which escaped the transaction (hazard (c))
\* keeps its durable content rather than being clobbered by an empty slot.
CommitRow ==
  [s \in Stores |->
     IF s = "checkpoint" THEN CheckpointContent
     ELSE IF s = "certs" /\ NonIdempotentEffects THEN dur[curGen]["certs"]
     ELSE buf[s]]

\* Sound commit: ONE transaction lands the buffered commit set, the generation
\* checkpoint, the visibility flip, and every step marker together
\* (commit_maintenance_state: single conn.commit(); markers ride the same
\* database -- the DBOS same-database boundary).
CommitAtomic ==
  /\ phase = "commit"
  /\ ~marker["commit"]
  /\ ~SplitCommitTxn
  /\ phase' = "done"
  /\ dur' = [dur EXCEPT ![curGen] = CommitRow]
  /\ visible' = curGen
  /\ marker' = [s \in Steps |-> TRUE]
  /\ firstDur' = PinFirst(firstDur, CommitRow)
  /\ UNCHANGED <<delta, curGen, attempt, buf>>

\* Hazard (b): the commit set lands in TWO transactions -- the certificate /
\* claim / projection stores first, the checkpoint + visibility + markers
\* second. The state between them is durable, and a crash strands it.
CommitSplitHead ==
  /\ phase = "commit"
  /\ ~marker["commit"]
  /\ SplitCommitTxn
  /\ phase' = "commit_tail"
  /\ dur' = [dur EXCEPT ![curGen] =
               [s \in Stores |->
                  IF s \in PreStores
                  THEN (IF s = "certs" /\ NonIdempotentEffects
                        THEN dur[curGen]["certs"] ELSE buf[s])
                  ELSE dur[curGen][s]]]
  /\ firstDur' = PinFirst(firstDur, dur'[curGen])
  /\ UNCHANGED <<delta, curGen, attempt, buf, visible, marker>>

CommitSplitTail ==
  /\ phase = "commit_tail"
  /\ phase' = "done"
  /\ dur' = [dur EXCEPT ![curGen]["checkpoint"] = CheckpointContent]
  /\ visible' = curGen
  /\ marker' = [s \in Steps |-> TRUE]
  /\ firstDur' = PinFirst(firstDur, dur'[curGen])
  /\ UNCHANGED <<delta, curGen, attempt, buf>>

\* --------------------------------------------------------------------------- \*
\* Crash and resume. A crash may hit ANY step boundary: the open transaction   \*
\* is lost (buf resets), durable stores and markers survive. Resume restarts   \*
\* the workflow from the top; the per-step marker checks above implement the   \*
\* DBOS completed-steps-are-not-re-executed semantics. Mid-step crashes are    \*
\* boundary crashes here because every within-step durable write is a single   \*
\* atomic action and volatile state dies at every crash point; the real        \*
\* mid-step hazard -- marker vs effects decoupling -- is what the toggles      \*
\* model explicitly.                                                            \*
\* --------------------------------------------------------------------------- \*
Crash ==
  /\ phase \in {"derive", "reuse", "admit", "commit", "commit_tail"}
  /\ attempt < MaxAttempts
  /\ phase' = "crashed"
  /\ buf' = [s \in PreStores |-> NoContent]
  /\ UNCHANGED <<delta, curGen, attempt, dur, visible, marker, firstDur>>

Resume ==
  /\ phase = "crashed"
  /\ phase' = "derive"
  /\ attempt' = attempt + 1
  /\ UNCHANGED <<delta, curGen, buf, dur, visible, marker, firstDur>>

\* The next publish cycle: a FRESH workflow instance (its own DBOS markers,
\* its own attempt budget, an empty transaction) over the next generation.
StartNextCycle ==
  /\ phase = "done"
  /\ curGen < NumGens
  /\ curGen' = curGen + 1
  /\ phase' = "derive"
  /\ attempt' = 1
  /\ buf' = [s \in PreStores |-> NoContent]
  /\ marker' = [s \in Steps |-> FALSE]
  /\ UNCHANGED <<delta, dur, visible, firstDur>>

Done ==
  /\ phase = "done"
  /\ curGen = NumGens
  /\ UNCHANGED vars

Next ==
  \/ Derive \/ Reuse \/ Admit
  \/ CommitReplaySkip \/ CommitAtomic \/ CommitSplitHead \/ CommitSplitTail
  \/ Crash \/ Resume \/ StartNextCycle \/ Done

Spec == Init /\ [][Next]_vars

\* --------------------------------------------------------------------------- \*
\* Invariants. Durable-store invariants are asserted at OBSERVABLE states:     \*
\* crash boundaries and termination. Between them the engine holds an open     \*
\* transaction whose intermediate durable states are unobservable; the         \*
\* split-commit hazard is precisely a protocol that makes them observable.     \*
\* --------------------------------------------------------------------------- \*
Quiescent == phase \in {"crashed", "done"}

TypeOK ==
  /\ delta \in [Generations -> [PreStores -> DeltaValues]]
  /\ curGen \in Generations
  /\ phase \in Phases
  /\ attempt \in 1..MaxAttempts
  /\ buf \in [PreStores -> Contents]
  /\ dur \in [Generations -> [Stores -> Contents]]
  /\ visible \in {NoGen} \cup Generations
  /\ marker \in [Steps -> BOOLEAN]
  /\ firstDur \in [Generations -> [Stores -> Contents]]

\* (1) No partial generation is ever observable: the visible generation
\* pointer only ever reaches a generation whose FULL commit set is durable --
\* and every generation at or below the pointer stays fully committed.
\* Named target of DbosCommitResumeMarkerFirstCounterexample.cfg.
NoPartialGenerationObservable ==
  Quiescent =>
    \A g \in Generations :
      g <= visible => \A s \in Stores : dur[g][s] # NoContent

\* (2) No double-commit: resume never commits the same generation twice with
\* different content -- every durable store member equals the FIRST content
\* ever committed for it (the ghost). The sound protocol earns this two ways
\* at once: effects are transactional (a lost attempt leaves nothing) and the
\* pass is deterministic from its inputs (a re-execution reproduces the same
\* bytes). Named target of DbosCommitResumeNonIdempotentCounterexample.cfg.
NoDoubleCommit ==
  Quiescent =>
    \A g \in Generations : \A s \in Stores :
      dur[g][s] # NoContent => dur[g][s] = firstDur[g][s]

\* (3) No certificate references an uncommitted generation: durable
\* certificate rows exist only for generations whose checkpoint committed.
\* Named target of DbosCommitResumeSplitCommitCounterexample.cfg.
NoCertWithoutCheckpoint ==
  Quiescent =>
    \A g \in Generations :
      dur[g]["certs"] # NoContent => dur[g]["checkpoint"] # NoContent

DbosCommitResumeSoundness ==
  /\ TypeOK
  /\ NoPartialGenerationObservable
  /\ NoDoubleCommit
  /\ NoCertWithoutCheckpoint

====
