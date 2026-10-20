---- MODULE OfflineReplay ----
(***************************************************************************
  Console offline & failure state machine.

  The Console framework's offline cross-cutting contract makes three
  commitments:

    - Capture, triage, reading, drafts, and recall work fully offline
      against the local mirror; the Console never blocks indefinitely
      on the substrate.

    - When the substrate is reachable again, queued writes replay in
      commit order. Writes never skip the queue or reorder.

    - On reconnect, conflicts (writes that touch state another client
      changed while we were offline) route to Triage as a new kind of
      consistency call. Conflicts never disappear silently.

  This model encodes the local queue as an explicit sequence, models
  the substrate as a "committed" set, models Triage as a "triaged" set,
  and treats the substrate's accept/conflict decision on replay as a
  nondeterministic choice (the model checker explores both branches).

  Out of scope: which writes specifically conflict, the substrate's own
  consistency rules, retry/backoff timing, network errors mid-replay
  (modelled as offline). These are deliberately abstracted so the
  contract-level properties are testable.
 ***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
  Writes,        \* Finite universe of write ids.
  MaxQueueLen    \* Bound on queue length (for finite checking).

ASSUME MaxQueueLen \in Nat \ {0}

VARIABLES
  online,        \* BOOLEAN; whether the substrate is currently reachable.
  queue,         \* Seq(Writes); pending writes in FIFO order.
  committed,     \* SUBSET Writes; writes accepted into the substrate.
  triaged,       \* SUBSET Writes; writes routed to Triage as conflicts.
  degraded,      \* BOOLEAN; the Console's degradation banner is visible.
  submitted,     \* Seq(Writes); writes in operator submission order.
  replayed       \* Seq(Writes); writes in the order replay consumed them.

vars == << online, queue, committed, triaged, degraded, submitted, replayed >>

(***************************************************************************
  Helpers.
 ***************************************************************************)
QueueSet == { queue[i] : i \in 1..Len(queue) }
SubmittedSet == { submitted[i] : i \in 1..Len(submitted) }
ReplayedSet == { replayed[i] : i \in 1..Len(replayed) }
SeenWrites == SubmittedSet
QueueEmpty == queue = << >>
HeadIs(w) == IF QueueEmpty THEN FALSE ELSE Head(queue) = w

Init ==
  /\ online    = TRUE
  /\ queue     = << >>
  /\ committed = {}
  /\ triaged   = {}
  /\ degraded  = FALSE
  /\ submitted = << >>
  /\ replayed  = << >>

(***************************************************************************
  WriteLocal(w): the operator submits a new write. This works regardless
  of online state. The write joins the local queue. (In practice the
  Console also writes it to a local mirror so the operator sees it;
  this model abstracts that into "queued".)

  Each write id is unique; once seen it cannot be submitted again. The
  finite Writes universe and the queue-length bound keep TLC's state
  space bounded.
 ***************************************************************************)
WriteLocal(w) ==
  /\ w \in Writes
  /\ w \notin SeenWrites
  /\ Len(queue) < MaxQueueLen
  /\ queue' = Append(queue, w)
  /\ submitted' = Append(submitted, w)
  /\ degraded' = TRUE  \* queue is now nonempty, so banner is on
  /\ UNCHANGED << online, committed, triaged, replayed >>

(***************************************************************************
  GoOffline / GoOnline: model the network state. Going offline makes
  the substrate unreachable; the degradation banner appears. Going
  online lets replay proceed; the banner stays until the queue is
  drained.
 ***************************************************************************)
GoOffline ==
  /\ online
  /\ online' = FALSE
  /\ degraded' = TRUE
  /\ UNCHANGED << queue, committed, triaged, submitted, replayed >>

GoOnline ==
  /\ ~online
  /\ online' = TRUE
  /\ degraded' = ~QueueEmpty
  /\ UNCHANGED << queue, committed, triaged, submitted, replayed >>

(***************************************************************************
  ReplayAccept: while online, take the head of the queue and accept it
  into the substrate. FIFO is preserved by the sequence-head semantics.
 ***************************************************************************)
ReplayAccept ==
  /\ online
  /\ ~QueueEmpty
  /\ committed' = committed \cup { Head(queue) }
  /\ queue' = Tail(queue)
  /\ replayed' = Append(replayed, Head(queue))
  /\ triaged' = triaged
  /\ degraded' = ~(Tail(queue) = << >> /\ online)
  /\ UNCHANGED << online, submitted >>

(***************************************************************************
  ReplayConflict: alternative outcome at the substrate. The head of
  the queue conflicts with state another client wrote while we were
  offline. The write does NOT silently drop; it routes to Triage.
 ***************************************************************************)
ReplayConflict ==
  /\ online
  /\ ~QueueEmpty
  /\ triaged' = triaged \cup { Head(queue) }
  /\ queue' = Tail(queue)
  /\ replayed' = Append(replayed, Head(queue))
  /\ committed' = committed
  /\ degraded' = ~(Tail(queue) = << >> /\ online)
  /\ UNCHANGED << online, submitted >>

ReplayStep ==
  \/ ReplayAccept
  \/ ReplayConflict

Next ==
  \/ \E w \in Writes: WriteLocal(w)
  \/ GoOffline
  \/ GoOnline
  \/ ReplayStep

Spec == Init /\ [][Next]_vars

(***************************************************************************
  ProgressSpec

      Safety checks use the same reachable states as Spec, but replay
      progress needs a fairness assumption. Weak fairness on ReplayStep
      says that if the console remains online with a non-empty queue,
      replay cannot stay enabled forever without consuming the head
      write. The model still permits disconnects; those are visible
      degraded states rather than hidden stalled replay.
 ***************************************************************************)
ProgressSpec == Init /\ [][Next]_vars /\ WF_vars(ReplayStep)

(***************************************************************************
  TypeOK
 ***************************************************************************)
TypeOK ==
  /\ online    \in BOOLEAN
  /\ queue     \in Seq(Writes)
  /\ Len(queue) <= MaxQueueLen
  /\ committed \subseteq Writes
  /\ triaged   \subseteq Writes
  /\ degraded  \in BOOLEAN
  /\ submitted \in Seq(Writes)
  /\ replayed  \in Seq(Writes)
  /\ Len(submitted) <= Cardinality(Writes)
  /\ Len(replayed) <= Len(submitted)
  /\ Cardinality(SubmittedSet) = Len(submitted)
  /\ Cardinality(ReplayedSet) = Len(replayed)

(***************************************************************************
  WritesAreUniquelyClassified

      No write ever appears in two of the three states (queue,
      committed, triaged) at the same time. Combined with the
      WriteLocal precondition that w is not already in SeenWrites,
      this captures "writes never replay twice and never end up in
      two places".
 ***************************************************************************)
WritesAreUniquelyClassified ==
  /\ QueueSet  \cap committed = {}
  /\ QueueSet  \cap triaged   = {}
  /\ committed \cap triaged   = {}

(***************************************************************************
  WritesReplayInCommitOrder

      Non-tautological FIFO. The model records every local submission
      in submitted and every replay disposition in replayed. At every
      state, submitted must equal exactly the already replayed prefix
      followed by the still-pending queue. A transition that consumes
      anything except Head(queue), reorders queue, or skips a write
      violates this invariant.
 ***************************************************************************)
WritesReplayInCommitOrder ==
  submitted = replayed \o queue

(***************************************************************************
  ConflictsRouteToTriage

      Every submitted write is still pending, committed, or triaged.
      Every replayed write has exactly one terminal disposition:
      committed on accept or triaged on conflict. There is no third
      disposition and no silent drop after a queue pop.
 ***************************************************************************)
ConflictsRouteToTriage ==
  /\ SubmittedSet = QueueSet \cup committed \cup triaged
  /\ ReplayedSet = committed \cup triaged

(***************************************************************************
  DegradedBannerReflectsState

      The Console shows the degradation banner whenever the substrate
      is offline OR there is at least one queued write awaiting
      replay. It shows no banner exactly when the substrate is online
      AND the queue is empty. This captures "the Console never
      silently blocks on the substrate".
 ***************************************************************************)
DegradedBannerReflectsState ==
  degraded = (~online \/ ~QueueEmpty)

(***************************************************************************
  ConsoleNeverBlocksIndefinitely

      At every reachable state with the substrate unreachable, the
      operator sees the degradation banner. The Console does not show
      a "loading" full-screen with nothing visible — when offline,
      either the local data renders or the banner names what's
      reduced.
 ***************************************************************************)
ConsoleNeverBlocksIndefinitely ==
  ~online => degraded

(***************************************************************************
  OnlineHeadWriteEventuallyLeavesQueue

      Progress for the online replay path. If a write is at the head
      of the queue while the console is online, then under ProgressSpec
      it eventually leaves the queue, unless the console goes offline.

      This is intentionally narrower than "the queue always drains":
      the runtime cannot replay while disconnected, and the correct
      offline behavior is visible degraded state.
 ***************************************************************************)
OnlineHeadWriteEventuallyLeavesQueue ==
  \A w \in Writes:
    [](online /\ HeadIs(w) => <>(~online \/ w \notin QueueSet))
====
