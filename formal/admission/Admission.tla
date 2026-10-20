---- MODULE Admission ----
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS
  Admissions,
  Runs,
  DerivedFacts,
  Generations,
  IdempotencyKeys,
  NoRun,
  NoAdmission,
  NoGeneration

Modes == {"direct", "fast_track", "review", "quarantine"}
Statuses == {
  "absent", "pending", "accepted", "rejected",
  "published", "blocked", "failed", "quarantined"
}
EventTypes == {
  "created", "duplicate_coalesced", "submitted",
  "accepted", "rejected", "quarantined",
  "published", "blocked", "failed", "refs_attached"
}
WorkflowNames == {
  "okg_source_sync",
  "okg_publisher_cycle",
  "okg_admission_submit",
  "okg_admission_decide"
}

VARIABLES envelopes, events, workflowAttempts, derivedFacts

DefaultEnvelope == [
  status |-> "absent",
  mode |-> "direct",
  key |-> CHOOSE k \in IdempotencyKeys : TRUE,
  run |-> NoRun,
  factsCommitted |-> FALSE,
  refsAttached |-> FALSE,
  zeroOutput |-> FALSE,
  hasCheckSummary |-> FALSE,
  targetGeneration |-> NoGeneration
]

Event(a, t) == [admission |-> a, type |-> t]
Attempt(name, a) == [name |-> name, admission |-> a]
DefaultDerivedFact == [
  emitted |-> FALSE,
  projectable |-> FALSE,
  visible |-> FALSE,
  admission |-> NoAdmission,
  workflowEventWritten |-> FALSE
]

Init ==
  /\ envelopes = [a \in Admissions |-> DefaultEnvelope]
  /\ events = {}
  /\ workflowAttempts = {}
  /\ derivedFacts = [f \in DerivedFacts |-> DefaultDerivedFact]

Known(a) == envelopes[a].status # "absent"
Terminal(s) == s \in {
  "published", "blocked", "rejected", "failed", "quarantined"
}

NoEnvelopeWithKey(k) ==
  \A a \in Admissions:
    ~Known(a) \/ envelopes[a].key # k

NoDerivedFactsForAdmission(a) ==
  \A f \in DerivedFacts:
    derivedFacts[f].admission # a

SubmitSourceRun(a, k, r, m) ==
  /\ ~Known(a)
  /\ NoEnvelopeWithKey(k)
  /\ r \in Runs
  /\ m \in Modes
  /\ envelopes' = [
       envelopes EXCEPT ![a] = [
         status |-> "pending",
         mode |-> m,
         key |-> k,
         run |-> r,
         factsCommitted |-> FALSE,
         refsAttached |-> FALSE,
         zeroOutput |-> FALSE,
         hasCheckSummary |-> TRUE,
         targetGeneration |-> NoGeneration
       ]
     ]
  /\ events' = events \cup {Event(a, "created")}
  /\ workflowAttempts' =
       workflowAttempts \cup {Attempt("okg_source_sync", a)}
  /\ derivedFacts' = derivedFacts

\* refsAttached is FALSE at submit time (refs attach after the run emits
\* output — for ANY mode, see CommitFacts/RecoverRefs). What keeps a pending
\* review envelope out of the publish sweep is the sweep's MODE guard
\* (admission.py _mark_generation_published: review/quarantine require
\* status accepted), modeled in PublishFeedback/BlockFeedback below.
SubmitReview(a, k) ==
  /\ ~Known(a)
  /\ NoEnvelopeWithKey(k)
  /\ envelopes' = [
       envelopes EXCEPT ![a] = [
         status |-> "pending",
         mode |-> "review",
         key |-> k,
         run |-> NoRun,
         factsCommitted |-> FALSE,
         refsAttached |-> FALSE,
         zeroOutput |-> FALSE,
         hasCheckSummary |-> FALSE,
         targetGeneration |-> NoGeneration
       ]
     ]
  /\ events' = events \cup {Event(a, "submitted")}
  /\ workflowAttempts' =
       workflowAttempts \cup {Attempt("okg_admission_submit", a)}
  /\ derivedFacts' = derivedFacts

OpenDerivedAdmission(a, k, r) ==
  /\ ~Known(a)
  /\ NoEnvelopeWithKey(k)
  /\ r \in Runs
  /\ envelopes' = [
       envelopes EXCEPT ![a] = [
         status |-> "pending",
         mode |-> "fast_track",
         key |-> k,
         run |-> r,
         factsCommitted |-> TRUE,
         refsAttached |-> FALSE,
         zeroOutput |-> FALSE,
         hasCheckSummary |-> TRUE,
         targetGeneration |-> NoGeneration
       ]
     ]
  /\ events' = events \cup {Event(a, "created")}
  /\ workflowAttempts' =
       workflowAttempts \cup {Attempt("okg_publisher_cycle", a)}
  /\ derivedFacts' = derivedFacts

DuplicateCoalesced(a) ==
  /\ Known(a)
  /\ envelopes' = envelopes
  /\ events' = events \cup {Event(a, "duplicate_coalesced")}
  /\ workflowAttempts' = workflowAttempts
  /\ derivedFacts' = derivedFacts

\* Production attaches fact-range refs for ANY admission mode with output
\* (ingest/service.py _finalize_source_admission), not only fast_track —
\* direct-mode source runs are the everyday path (audit F2).
CommitFacts(a) ==
  /\ Known(a)
  /\ ~envelopes[a].factsCommitted
  /\ ~envelopes[a].zeroOutput
  /\ envelopes' = [
       envelopes EXCEPT ![a].factsCommitted = TRUE
     ]
  /\ events' = events
  /\ workflowAttempts' = workflowAttempts
  /\ derivedFacts' = derivedFacts

RecoverRefs(a) ==
  /\ Known(a)
  /\ envelopes[a].factsCommitted
  /\ ~envelopes[a].refsAttached
  /\ envelopes' = [
       envelopes EXCEPT ![a].refsAttached = TRUE
     ]
  /\ events' = events \cup {Event(a, "refs_attached")}
  /\ workflowAttempts' = workflowAttempts
  /\ derivedFacts' = derivedFacts

\* Atomic decide-approve for review/quarantine envelopes: production emits
\* the held facts, attaches their range refs, and accepts inside ONE
\* transaction (admission_workflows decide path), so no intermediate
\* pending-with-qualifying-refs state exists for the publish sweep to see.
ApproveReview(a) ==
  /\ envelopes[a].status = "pending"
  /\ envelopes[a].mode \in {"review", "quarantine"}
  /\ envelopes' = [
       envelopes EXCEPT
         ![a].status = "accepted",
         ![a].factsCommitted = TRUE,
         ![a].refsAttached = TRUE
     ]
  /\ events' = events \cup {Event(a, "refs_attached"), Event(a, "accepted")}
  /\ workflowAttempts' =
       workflowAttempts \cup {Attempt("okg_admission_decide", a)}
  /\ derivedFacts' = derivedFacts

Accept(a) ==
  /\ envelopes[a].status = "pending"
  /\ envelopes[a].refsAttached
  /\ envelopes' = [
       envelopes EXCEPT ![a].status = "accepted"
     ]
  /\ events' = events \cup {Event(a, "accepted")}
  /\ workflowAttempts' =
       workflowAttempts \cup {Attempt("okg_admission_decide", a)}
  /\ derivedFacts' = derivedFacts

AcceptZeroOutput(a) ==
  /\ envelopes[a].status = "pending"
  /\ ~envelopes[a].factsCommitted
  /\ ~envelopes[a].refsAttached
  /\ envelopes[a].hasCheckSummary
  /\ NoDerivedFactsForAdmission(a)
  /\ envelopes' = [
       envelopes EXCEPT
         ![a].status = "accepted",
         ![a].zeroOutput = TRUE
     ]
  /\ events' = events \cup {Event(a, "accepted")}
  /\ workflowAttempts' =
       workflowAttempts \cup {Attempt("okg_source_sync", a)}
  /\ derivedFacts' = derivedFacts

Reject(a) ==
  /\ envelopes[a].status = "pending"
  /\ envelopes' = [
       envelopes EXCEPT ![a].status = "rejected"
     ]
  /\ events' = events \cup {Event(a, "rejected")}
  /\ workflowAttempts' =
       workflowAttempts \cup {Attempt("okg_admission_decide", a)}
  /\ derivedFacts' = derivedFacts

Quarantine(a) ==
  /\ envelopes[a].status = "pending"
  /\ envelopes' = [
       envelopes EXCEPT ![a].status = "quarantined"
     ]
  /\ events' = events \cup {Event(a, "quarantined")}
  /\ workflowAttempts' =
       workflowAttempts \cup {Attempt("okg_admission_decide", a)}
  /\ derivedFacts' = derivedFacts

\* Publish decisiveness (add-admission-envelope design): the publish sweep
\* resolves PENDING-or-accepted envelopes whose refs are under the watermark
\* (admission.py _mark_generation_published). Direct-mode envelopes have no
\* review loop and publish from pending; review/quarantine envelopes require
\* an explicit accepted decision (the sweep's mode guard — a pending review
\* envelope awaits `okg admission decide` and must not be consumed).
PublishFeedback(a, g) ==
  /\ envelopes[a].status \in {"pending", "accepted"}
  /\ \/ envelopes[a].mode \notin {"review", "quarantine"}
     \/ envelopes[a].status = "accepted"
  /\ envelopes[a].refsAttached
  /\ g \in Generations
  /\ envelopes' = [
       envelopes EXCEPT
         ![a].status = "published",
         ![a].targetGeneration = g
     ]
  /\ events' = events \cup {Event(a, "published")}
  /\ workflowAttempts' =
       workflowAttempts \cup {Attempt("okg_publisher_cycle", a)}
  /\ derivedFacts' = derivedFacts

BlockFeedback(a, g) ==
  /\ envelopes[a].status \in {"pending", "accepted"}
  /\ \/ envelopes[a].mode \notin {"review", "quarantine"}
     \/ envelopes[a].status = "accepted"
  /\ envelopes[a].refsAttached
  /\ g \in Generations
  /\ envelopes' = [
       envelopes EXCEPT
         ![a].status = "blocked",
         ![a].targetGeneration = g
     ]
  /\ events' = events \cup {Event(a, "blocked")}
  /\ workflowAttempts' =
       workflowAttempts \cup {Attempt("okg_publisher_cycle", a)}
  /\ derivedFacts' = derivedFacts

Fail(a) ==
  /\ Known(a)
  /\ ~Terminal(envelopes[a].status)
  /\ envelopes' = [
       envelopes EXCEPT ![a].status = "failed"
     ]
  /\ events' = events \cup {Event(a, "failed")}
  /\ workflowAttempts' = workflowAttempts
  /\ derivedFacts' = derivedFacts

\* Derived facts are loadable by default once emitted (deny-list semantics,
\* mirroring the derived-fact loader after the wave-2 fix): rejection/failure
\* of the producer's admission revokes projectability; acceptance is not a
\* projection precondition, because a producer whose policy evaluation never
\* completed (envelope pending) is still swept to published and its facts
\* load. Only rejected/failed/quarantined runs are excluded.
EmitDerivedFacts(f, a) ==
  /\ f \in DerivedFacts
  /\ Known(a)
  /\ ~envelopes[a].zeroOutput
  /\ ~derivedFacts[f].emitted
  /\ derivedFacts' = [
       derivedFacts EXCEPT ![f] = [
         emitted |-> TRUE,
         projectable |-> TRUE,
         visible |-> FALSE,
         admission |-> a,
         workflowEventWritten |-> FALSE
       ]
     ]
  /\ envelopes' = envelopes
  /\ events' = events
  /\ workflowAttempts' = workflowAttempts

EvaluateDerivedPolicy(f) ==
  LET a == derivedFacts[f].admission IN
  /\ f \in DerivedFacts
  /\ a \in Admissions
  /\ derivedFacts[f].emitted
  /\ envelopes[a].status = "pending"
  /\ envelopes' = [
       envelopes EXCEPT
         ![a].status = "accepted",
         ![a].refsAttached = TRUE
     ]
  /\ derivedFacts' = [
       derivedFacts EXCEPT
         ![f].projectable = TRUE,
         ![f].workflowEventWritten = TRUE
     ]
  /\ events' = events \cup {Event(a, "refs_attached"), Event(a, "accepted")}
  /\ workflowAttempts' =
       workflowAttempts \cup {Attempt("okg_publisher_cycle", a)}

RejectDerivedPolicy(f) ==
  LET a == derivedFacts[f].admission IN
  /\ f \in DerivedFacts
  /\ a \in Admissions
  /\ derivedFacts[f].emitted
  /\ envelopes[a].status = "pending"
  /\ envelopes' = [
       envelopes EXCEPT
         ![a].status = "rejected",
         ![a].refsAttached = TRUE
     ]
  /\ derivedFacts' = [
       derivedFacts EXCEPT
         ![f].projectable = FALSE,
         ![f].workflowEventWritten = TRUE
     ]
  /\ events' = events \cup {Event(a, "refs_attached"), Event(a, "rejected")}
  /\ workflowAttempts' =
       workflowAttempts \cup {Attempt("okg_publisher_cycle", a)}

FailDerivedProducer(f) ==
  LET a == derivedFacts[f].admission IN
  /\ f \in DerivedFacts
  /\ a \in Admissions
  /\ derivedFacts[f].emitted
  /\ ~Terminal(envelopes[a].status)
  /\ envelopes' = [
       envelopes EXCEPT
         ![a].status = "failed",
         ![a].refsAttached = TRUE
     ]
  /\ derivedFacts' = [
       derivedFacts EXCEPT
         ![f].projectable = FALSE,
         ![f].workflowEventWritten = TRUE
     ]
  /\ events' = events \cup {Event(a, "refs_attached"), Event(a, "failed")}
  /\ workflowAttempts' =
       workflowAttempts \cup {Attempt("okg_publisher_cycle", a)}

ProjectDerivedFacts(f) ==
  LET a == derivedFacts[f].admission IN
  /\ f \in DerivedFacts
  /\ a \in Admissions
  /\ derivedFacts[f].emitted
  /\ derivedFacts[f].projectable
  /\ envelopes[a].status = "published"
  /\ derivedFacts' = [
       derivedFacts EXCEPT ![f].visible = TRUE
     ]
  /\ envelopes' = envelopes
  /\ events' = events
  /\ workflowAttempts' = workflowAttempts

DBOSPublisherReplay(a) ==
  /\ Known(a)
  /\ workflowAttempts' =
       workflowAttempts \cup {Attempt("okg_publisher_cycle", a)}
  /\ envelopes' = envelopes
  /\ events' = events
  /\ derivedFacts' = derivedFacts

WriteDerivedWorkflowEvent(f) ==
  /\ f \in DerivedFacts
  /\ derivedFacts[f].emitted
  /\ derivedFacts' = [
       derivedFacts EXCEPT ![f].workflowEventWritten = TRUE
     ]
  /\ envelopes' = envelopes
  /\ events' = events
  /\ workflowAttempts' = workflowAttempts

Next ==
  \/ \E a \in Admissions, k \in IdempotencyKeys, r \in Runs, m \in Modes:
       SubmitSourceRun(a, k, r, m)
  \/ \E a \in Admissions, k \in IdempotencyKeys:
       SubmitReview(a, k)
  \/ \E a \in Admissions, k \in IdempotencyKeys, r \in Runs:
       OpenDerivedAdmission(a, k, r)
  \/ \E a \in Admissions: DuplicateCoalesced(a)
  \/ \E a \in Admissions: CommitFacts(a)
  \/ \E a \in Admissions: RecoverRefs(a)
  \/ \E a \in Admissions: ApproveReview(a)
  \/ \E a \in Admissions: Accept(a)
  \/ \E a \in Admissions: AcceptZeroOutput(a)
  \/ \E a \in Admissions: Reject(a)
  \/ \E a \in Admissions: Quarantine(a)
  \/ \E a \in Admissions, g \in Generations: PublishFeedback(a, g)
  \/ \E a \in Admissions, g \in Generations: BlockFeedback(a, g)
  \/ \E a \in Admissions: Fail(a)
  \/ \E f \in DerivedFacts, a \in Admissions: EmitDerivedFacts(f, a)
  \/ \E f \in DerivedFacts: EvaluateDerivedPolicy(f)
  \/ \E f \in DerivedFacts: RejectDerivedPolicy(f)
  \/ \E f \in DerivedFacts: FailDerivedProducer(f)
  \/ \E f \in DerivedFacts: ProjectDerivedFacts(f)
  \/ \E a \in Admissions: DBOSPublisherReplay(a)
  \/ \E f \in DerivedFacts: WriteDerivedWorkflowEvent(f)

Spec ==
  Init /\
  [][Next]_<<envelopes, events, workflowAttempts, derivedFacts>>

TypeOK ==
  /\ envelopes \in [
       Admissions -> [
         status: Statuses,
         mode: Modes,
         key: IdempotencyKeys,
         run: Runs \cup {NoRun},
         factsCommitted: BOOLEAN,
         refsAttached: BOOLEAN,
         zeroOutput: BOOLEAN,
         hasCheckSummary: BOOLEAN,
         targetGeneration: Generations \cup {NoGeneration}
       ]
     ]
  /\ events \subseteq [
       admission: Admissions,
       type: EventTypes
     ]
  /\ workflowAttempts \subseteq [
       name: WorkflowNames,
       admission: Admissions
     ]
  /\ derivedFacts \in [
       DerivedFacts -> [
         emitted: BOOLEAN,
         projectable: BOOLEAN,
         visible: BOOLEAN,
         admission: Admissions \cup {NoAdmission},
         workflowEventWritten: BOOLEAN
       ]
     ]

NoDuplicateIdempotency ==
  \A a, b \in Admissions:
    /\ a # b
    /\ Known(a)
    /\ Known(b)
    => envelopes[a].key # envelopes[b].key

FastTrackHasChecks ==
  \A a \in Admissions:
    /\ Known(a)
    /\ envelopes[a].mode = "fast_track"
    => envelopes[a].hasCheckSummary

RefsBeforePublish ==
  \A a \in Admissions:
    envelopes[a].status = "published" => envelopes[a].refsAttached

ZeroOutputAcceptedHasNoRefs ==
  \A a \in Admissions:
    envelopes[a].zeroOutput =>
      /\ ~envelopes[a].factsCommitted
      /\ ~envelopes[a].refsAttached
      /\ envelopes[a].targetGeneration = NoGeneration

ZeroOutputAdmissionCannotPublishWithoutRefs ==
  \A a \in Admissions:
    /\ envelopes[a].zeroOutput
    /\ ~envelopes[a].refsAttached
    => envelopes[a].status # "published"

PublishedHaveGeneration ==
  \A a \in Admissions:
    envelopes[a].status = "published" =>
      envelopes[a].targetGeneration # NoGeneration

RejectedNeverPublished ==
  \A a \in Admissions:
    envelopes[a].status = "rejected" =>
      envelopes[a].targetGeneration = NoGeneration

\* History form of rejection safety (the design's headline clause): once an
\* envelope has a rejected event, no published event can ever follow. This
\* holds through the interplay of three guards — Reject only fires from
\* pending, PublishFeedback excludes rejected, and rejected is terminal —
\* so deleting any resurrection guard trips it.
RejectedNeverLaterPublished ==
  \A a \in Admissions:
    Event(a, "rejected") \in events =>
      Event(a, "published") \notin events

\* Publish decisiveness must NOT bypass review: a review/quarantine envelope
\* can only reach published through an accepted decision, because its raw
\* payload ref never qualifies for the publish sweep and qualifying refs
\* attach only inside the atomic approve. Setting refsAttached at review
\* submit (or letting RecoverRefs touch review envelopes) trips this.
ReviewPublishRequiresDecision ==
  \A a \in Admissions:
    /\ envelopes[a].mode \in {"review", "quarantine"}
    /\ envelopes[a].status = "published"
    => Event(a, "accepted") \in events

EventsReferenceKnownEnvelope ==
  \A e \in events: Known(e.admission)

WorkflowAttemptsAreNamed ==
  \A w \in workflowAttempts: w.name \in WorkflowNames

\* Renamed from VisibleDerivedImpliesAcceptedPolicy: under publish
\* decisiveness a producer whose policy evaluation never completed is still
\* swept pending-to-published and its facts load, so visibility implies the
\* envelope PUBLISHED — not that the policy was affirmatively accepted.
VisibleDerivedImpliesPublishedEnvelope ==
  \A f \in DerivedFacts:
    derivedFacts[f].visible =>
      /\ derivedFacts[f].admission \in Admissions
      /\ envelopes[derivedFacts[f].admission].status = "published"

RejectedDerivedNeverVisible ==
  \A f \in DerivedFacts:
    /\ derivedFacts[f].admission \in Admissions
    /\ envelopes[derivedFacts[f].admission].status \in {
         "rejected", "failed", "quarantined"
       }
    => ~derivedFacts[f].visible

ExtractorCannotBypassProjectability ==
  \A f \in DerivedFacts:
    derivedFacts[f].visible => derivedFacts[f].projectable

WorkflowEventNotAuthority ==
  \A f \in DerivedFacts:
    /\ derivedFacts[f].workflowEventWritten
    /\ derivedFacts[f].admission \in Admissions
    /\ envelopes[derivedFacts[f].admission].status \notin {
         "accepted", "published"
       }
    => ~derivedFacts[f].visible

====
