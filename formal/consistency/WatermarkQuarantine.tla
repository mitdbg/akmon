---- MODULE WatermarkQuarantine ----
\* Bounded, item-scoped retry/quarantine composed with publisher progress.
\*
\* One publisher transaction owns retry accounting, terminal quarantine,
\* snapshot replacement, and enricher_progress. The deployment advisory lock
\* excludes concurrent publisher cycles. Claim(i) is the main-thread
\* pre-dispatch reservation inside that transaction; duplicate item keys are
\* refused within an invocation. CrashCycle rolls the transaction back to the
\* prior committed attempt counters. This deliberately bounds COMMITTED
\* deterministic failures, not external calls across a process crash: without
\* provider idempotency a call may have happened before the rollback.
\*
\* CompleteItemFailure only prepares terminal state. FinishCycle atomically
\* commits the quarantine record, retained snapshot output, successful
\* progress status, and watermark release. No split durable terminal state is
\* reachable. Systemic failures undo their reservation and never consume the
\* committed item budget.

EXTENDS Naturals, FiniteSets

CONSTANTS
  Items,
  RetryLimit,
  InitialDeterministicItems,
  ChangedDeterministicItems,
  SystemicItems,
  ClassifySystemicAsItem,
  RetainQuarantinedOutput,
  AllowCrash

VARIABLES
  state,                 \* committed item state
  workingState,          \* current publisher transaction's item state
  committedAttempts,     \* committed deterministic failure count
  workingAttempts,       \* current publisher transaction's reservations
  signature,             \* item -> 0 | 1 (behavior-affecting key change)
  activeQuarantine,      \* active doctor findings (may name prior signature)
  quarantineHistory,     \* item -> signatures durably quarantined
  publishedOutput,       \* item-owned output present in published snapshot
  outputVersion,         \* changed on successful sibling replacement
  inFlight,              \* currently dispatched item keys
  claimedThisCycle,      \* duplicate refs cannot dispatch twice/invocation
  terminalReady,         \* final failures prepared, not yet committed
  cycleOpen,
  crashUsed,             \* at most one modeled process death per behavior
  failureSeen,           \* none | deterministic | systemic
  committedIncomplete,   \* last committed run held progress
  lastStatus,            \* never | success | failed:<class>
  watermarkAdvanced

vars == <<
  state, workingState, committedAttempts, workingAttempts, signature,
  activeQuarantine, quarantineHistory, publishedOutput, outputVersion,
  inFlight, claimedThisCycle, terminalReady, cycleOpen, failureSeen,
  crashUsed, committedIncomplete, lastStatus, watermarkAdvanced
>>

Pending == {i \in Items : workingState[i] = "pending"}
Terminal == {i \in Items : state[i] \in {"processed", "quarantined"}}
WorkingTerminal == {
  i \in Items : workingState[i] \in {"processed", "quarantined"}
}
WillBeTerminal == WorkingTerminal \cup terminalReady = Items

IsDeterministic(i) ==
  \/ (signature[i] = 0 /\ i \in InitialDeterministicItems)
  \/ (signature[i] = 1 /\ i \in ChangedDeterministicItems)
  \/ (i \in SystemicItems /\ ClassifySystemicAsItem)

IsSystemic(i) == i \in SystemicItems /\ ~ClassifySystemicAsItem
IsHealthy(i) == ~IsDeterministic(i) /\ ~IsSystemic(i)

Init ==
  /\ state = [i \in Items |-> "pending"]
  /\ workingState = [i \in Items |-> "pending"]
  /\ committedAttempts = [i \in Items |-> 0]
  /\ workingAttempts = [i \in Items |-> 0]
  /\ signature = [i \in Items |-> 0]
  /\ activeQuarantine = {}
  /\ quarantineHistory = [i \in Items |-> {}]
  /\ publishedOutput = Items
  /\ outputVersion = [i \in Items |-> 0]
  /\ inFlight = {}
  /\ claimedThisCycle = {}
  /\ terminalReady = {}
  /\ cycleOpen = FALSE
  /\ crashUsed = FALSE
  /\ failureSeen = "none"
  /\ committedIncomplete = FALSE
  /\ lastStatus = "never"
  /\ watermarkAdvanced = FALSE

\* Atomic, main-thread pre-dispatch reservation. The publisher lock excludes
\* a concurrent cycle; claimedThisCycle removes duplicate refs before fanout.
Claim(i) ==
  /\ i \in Pending
  /\ i \notin terminalReady
  /\ i \notin inFlight
  /\ i \notin claimedThisCycle
  /\ workingAttempts[i] < RetryLimit
  /\ inFlight' = inFlight \cup {i}
  /\ claimedThisCycle' = claimedThisCycle \cup {i}
  /\ workingAttempts' = [workingAttempts EXCEPT ![i] = @ + 1]
  /\ cycleOpen' = TRUE
  /\ UNCHANGED <<
       state, workingState, committedAttempts, signature, activeQuarantine,
       quarantineHistory, publishedOutput, outputVersion, terminalReady,
       crashUsed, failureSeen, committedIncomplete, lastStatus,
       watermarkAdvanced
     >>

CompleteSuccess(i) ==
  /\ i \in inFlight
  /\ IsHealthy(i)
  /\ workingState' = [workingState EXCEPT ![i] = "processed"]
  /\ workingAttempts' = [workingAttempts EXCEPT ![i] = 0]
  /\ inFlight' = inFlight \ {i}
  /\ UNCHANGED <<
       state, committedAttempts, signature, activeQuarantine,
       quarantineHistory, publishedOutput,
       outputVersion, claimedThisCycle, terminalReady, cycleOpen,
       crashUsed, failureSeen, committedIncomplete, lastStatus,
       watermarkAdvanced
     >>

\* Eligible deterministic failure. At the limit it prepares terminal state,
\* but does not durably write a quarantine or release progress yet.
CompleteItemFailure(i) ==
  /\ i \in inFlight
  /\ IsDeterministic(i)
  /\ inFlight' = inFlight \ {i}
  /\ failureSeen' = "deterministic"
  /\ terminalReady' =
       IF workingAttempts[i] = RetryLimit
       THEN terminalReady \cup {i}
       ELSE terminalReady
  /\ UNCHANGED <<
       state, workingState, committedAttempts, workingAttempts, signature,
       activeQuarantine, quarantineHistory, publishedOutput, outputVersion,
       claimedThisCycle, cycleOpen, crashUsed, committedIncomplete,
       lastStatus, watermarkAdvanced
     >>

\* Systemic failure does not consume the item budget. The reservation is
\* undone before FinishCycle; repeated outages keep committedAttempts at zero.
CompleteSystemicFailure(i) ==
  /\ i \in inFlight
  /\ IsSystemic(i)
  /\ inFlight' = inFlight \ {i}
  /\ workingAttempts' = [workingAttempts EXCEPT ![i] = @ - 1]
  /\ failureSeen' = "systemic"
  /\ UNCHANGED <<
       state, workingState, committedAttempts, signature, activeQuarantine,
       quarantineHistory, publishedOutput, outputVersion,
       claimedThisCycle, terminalReady, cycleOpen,
       crashUsed, committedIncomplete, lastStatus, watermarkAdvanced
     >>

Complete(i) ==
  CompleteSuccess(i)
  \/ CompleteItemFailure(i)
  \/ CompleteSystemicFailure(i)

\* One commit transition for failure ledger, complete-snapshot decision,
\* progress status, and watermark. With terminal poison, prior item-owned
\* output is retained; successful siblings replace independently (their
\* outputVersion advances). RetainQuarantinedOutput=FALSE is the intentionally
\* unsafe configuration used by the non-vacuity witness.
FinishCycle ==
  /\ cycleOpen
  /\ inFlight = {}
  /\ state' = [
       i \in Items |->
         IF i \in terminalReady THEN "quarantined" ELSE workingState[i]
     ]
  /\ workingState' = state'
  /\ committedAttempts' = workingAttempts
  \* Changed-signature success supersedes the OLD active finding only at the
  \* transaction commit. A crash before this transition therefore leaves
  \* doctor red and the item pending for redispatch.
  /\ activeQuarantine' =
       (activeQuarantine \ {i \in Items : workingState[i] = "processed"})
       \cup terminalReady
  /\ quarantineHistory' = [
       i \in Items |->
         IF i \in terminalReady
         THEN quarantineHistory[i] \cup {signature[i]}
         ELSE quarantineHistory[i]
     ]
  /\ publishedOutput' =
       IF WillBeTerminal
       THEN {i \in Items : workingState[i] = "processed"}
            \cup IF RetainQuarantinedOutput
                 THEN publishedOutput
                      \cap ({i \in Items : workingState[i] = "quarantined"}
                            \cup terminalReady)
                 ELSE {}
       ELSE publishedOutput
  /\ outputVersion' = [
       i \in Items |->
         IF WillBeTerminal /\ workingState[i] = "processed"
         THEN signature[i] + 1
         ELSE outputVersion[i]
     ]
  /\ lastStatus' =
       IF WillBeTerminal
       THEN "success"
       ELSE IF failureSeen = "systemic"
            THEN "failed:systemic"
            ELSE "failed:deterministic"
  /\ watermarkAdvanced' = WillBeTerminal
  /\ claimedThisCycle' = {}
  /\ terminalReady' = {}
  /\ cycleOpen' = FALSE
  /\ failureSeen' = "none"
  /\ committedIncomplete' = ~WillBeTerminal
  /\ workingAttempts' = workingAttempts
  /\ UNCHANGED <<signature, inFlight, crashUsed>>

\* Process crash before commit: the publisher transaction rolls back retry
\* reservations and terminal preparation together. A provider call may be
\* repeated after restart; exactly-once external calls are out of scope and
\* would require provider idempotency. No split durable OKG state survives.
CrashCycle ==
  /\ AllowCrash
  /\ ~crashUsed
  /\ cycleOpen
  /\ workingAttempts' = committedAttempts
  /\ workingState' = state
  /\ inFlight' = {}
  /\ claimedThisCycle' = {}
  /\ terminalReady' = {}
  /\ cycleOpen' = FALSE
  /\ crashUsed' = TRUE
  /\ failureSeen' = "none"
  /\ UNCHANGED <<
       state, committedAttempts, signature, activeQuarantine,
       quarantineHistory, publishedOutput, outputVersion,
       committedIncomplete, lastStatus, watermarkAdvanced
     >>

\* A changed OutcomeKey gets a fresh budget. The old active finding remains
\* red through retries and is superseded only by CompleteSuccess.
ChangeSignature(i) ==
  /\ state[i] = "quarantined"
  /\ signature[i] = 0
  /\ ~cycleOpen
  /\ state' = [state EXCEPT ![i] = "pending"]
  /\ workingState' = [workingState EXCEPT ![i] = "pending"]
  /\ committedAttempts' = [committedAttempts EXCEPT ![i] = 0]
  /\ workingAttempts' = [workingAttempts EXCEPT ![i] = 0]
  /\ signature' = [signature EXCEPT ![i] = 1]
  /\ watermarkAdvanced' = FALSE
  /\ UNCHANGED <<
       activeQuarantine, quarantineHistory, publishedOutput, outputVersion,
       inFlight, claimedThisCycle, terminalReady, cycleOpen,
       crashUsed, failureSeen, committedIncomplete, lastStatus
     >>

Next ==
  (\E i \in Items : Claim(i))
  \/ (\E i \in Items : Complete(i))
  \/ FinishCycle
  \/ CrashCycle
  \/ (\E i \in Items : ChangeSignature(i))

Fairness ==
  /\ (\A i \in Items : WF_vars(Claim(i)))
  /\ (\A i \in Items : WF_vars(Complete(i)))
  /\ WF_vars(FinishCycle)

Spec == Init /\ [][Next]_vars /\ Fairness

TypeOK ==
  /\ state \in [Items -> {"pending", "processed", "quarantined"}]
  /\ workingState \in [Items -> {"pending", "processed", "quarantined"}]
  /\ committedAttempts \in [Items -> 0..RetryLimit]
  /\ workingAttempts \in [Items -> 0..RetryLimit]
  /\ signature \in [Items -> 0..1]
  /\ activeQuarantine \in SUBSET Items
  /\ quarantineHistory \in [Items -> SUBSET (0..1)]
  /\ publishedOutput \in SUBSET Items
  /\ outputVersion \in [Items -> 0..2]
  /\ inFlight \in SUBSET Items
  /\ claimedThisCycle \in SUBSET Items
  /\ terminalReady \in SUBSET Items
  /\ cycleOpen \in BOOLEAN
  /\ crashUsed \in BOOLEAN
  /\ failureSeen \in {"none", "deterministic", "systemic"}
  /\ committedIncomplete \in BOOLEAN
  /\ lastStatus \in {
       "never", "success", "failed:deterministic", "failed:systemic"
     }
  /\ watermarkAdvanced \in BOOLEAN

DispatchBudgetBound ==
  \A i \in Items :
    /\ committedAttempts[i] <= RetryLimit
    /\ workingAttempts[i] <= RetryLimit
NoPrematureQuarantine ==
  \A i \in Items :
    state[i] = "quarantined" => committedAttempts[i] = RetryLimit
QuarantineHasDurableRecord ==
  \A i \in Items :
    state[i] = "quarantined" =>
      /\ i \in activeQuarantine
      /\ signature[i] \in quarantineHistory[i]
ActiveRecordBackedByHistory ==
  \A i \in activeQuarantine : quarantineHistory[i] /= {}
ChangedSignatureKeepsHistory ==
  \A i \in Items : signature[i] = 1 => 0 \in quarantineHistory[i]
OldFindingActiveDuringChangedRetry ==
  \A i \in Items :
    signature[i] = 1 /\ state[i] = "pending" => i \in activeQuarantine
SuccessfulChangedSignatureSupersedes ==
  \A i \in Items :
    signature[i] = 1 /\ state[i] = "processed" =>
      /\ i \notin activeQuarantine
      /\ 0 \in quarantineHistory[i]
QuarantinedOutputRetained ==
  \A i \in Items : state[i] = "quarantined" => i \in publishedOutput
SuccessfulSiblingReplaced ==
  \A i \in Items :
    watermarkAdvanced /\ state[i] = "processed" =>
      outputVersion[i] = signature[i] + 1
TerminalRecordAtomicWithRelease ==
  watermarkAdvanced =>
    \A i \in Items : state[i] = "quarantined" =>
      /\ i \in activeQuarantine
      /\ signature[i] \in quarantineHistory[i]
WatermarkReleaseIsTerminal == watermarkAdvanced => Pending = {}
HeldStatusUsesFailedNamespace ==
  committedIncomplete =>
    /\ lastStatus \in {"failed:deterministic", "failed:systemic"}
    /\ ~watermarkAdvanced
NoSystemicQuarantine ==
  \A i \in SystemicItems : state[i] /= "quarantined"
SystemicCommittedAttemptsStayZero ==
  \A i \in SystemicItems : committedAttempts[i] = 0
SystemicReservationReleased ==
  \A i \in SystemicItems :
    i \notin inFlight => workingAttempts[i] = committedAttempts[i]
ClosedCycleHasNoWorkingState == ~cycleOpen => workingState = state

EveryProcessableItemEventuallyTerminal ==
  \A i \in (Items \ SystemicItems) : []<>(i \in Terminal)

\* Named aggregate properties / counterexample targets.
WatermarkQuarantineSafety ==
  /\ DispatchBudgetBound
  /\ NoPrematureQuarantine
  /\ QuarantineHasDurableRecord
  /\ QuarantinedOutputRetained
  /\ TerminalRecordAtomicWithRelease
  /\ WatermarkReleaseIsTerminal
SystemicFailureHeld ==
  /\ NoSystemicQuarantine
  /\ SystemicCommittedAttemptsStayZero
  /\ SystemicReservationReleased
SystemicFailureMisclassificationWitness == NoSystemicQuarantine
SnapshotOmissionRetractionWitness == QuarantinedOutputRetained

====
