---- MODULE AdmissionCounterexample ----
EXTENDS Admission

\* Negative-check module for the proof gate. It intentionally asserts
\* that no admission can ever publish, while Admission permits publish
\* feedback after refs attach. TLC must produce a counterexample.
BrokenNoPublish == \A a \in Admissions:
  envelopes[a].status # "published"

\* Non-vacuity witness for FastTrackHasChecks. That invariant only bites
\* when a known fast_track admission exists; OpenDerivedAdmission reaches
\* exactly that state. Asserting it never happens must be refuted, proving
\* the FastTrackHasChecks antecedent is reachable (and that the witnessed
\* fast_track admission really does carry hasCheckSummary = TRUE).
BrokenNoFastTrack == \A a \in Admissions:
  ~(Known(a) /\ envelopes[a].mode = "fast_track")

\* Non-vacuity witness for ExtractorCannotBypassProjectability. That
\* invariant only bites when a visible derived fact exists; Emit ->
\* EvaluateDerivedPolicy -> PublishFeedback -> ProjectDerivedFacts reaches
\* a visible fact. Asserting no derived fact is ever visible must be
\* refuted, proving the visible-derived antecedent is reachable.
BrokenNoVisibleDerived == \A f \in DerivedFacts:
  ~derivedFacts[f].visible

\* Non-vacuity witness for VisibleDerivedImpliesAcceptedPolicy. The
\* interesting consequent branch is a derived fact that is both visible and
\* backed by a published admission. ProjectDerivedFacts requires exactly a
\* published producer, so this state is reachable; asserting it never
\* happens must be refuted.
BrokenNoPublishedVisibleDerived == \A f \in DerivedFacts:
  ~(/\ derivedFacts[f].visible
    /\ derivedFacts[f].admission \in Admissions
    /\ envelopes[derivedFacts[f].admission].status = "published")

\* Non-vacuity witness for the rejection-safety pair (RejectedNeverPublished,
\* RejectedNeverLaterPublished, RejectedDerivedNeverVisible): the hazard
\* state those invariants guard — a REJECTED admission with committed facts,
\* exactly what a derived producer reaches when its policy evaluation rejects
\* after its facts landed (audit F3, the loader deny-list fix) — must be
\* reachable, or the whole rejection-safety claim is proven over a world
\* where rejection with output never happens. Asserting the state never
\* occurs must be refuted.
BrokenNoRejectedWithCommittedFacts == \A a \in Admissions:
  ~(/\ envelopes[a].status = "rejected"
    /\ envelopes[a].factsCommitted)

\* Non-vacuity witness for the publish-decisiveness sweep (audit F1): a
\* PENDING envelope with refs under the watermark is swept to published
\* without an accepted decision — the everyday direct-mode path. Asserting
\* an envelope can only publish with a prior accepted event must be refuted,
\* proving the pending-to-published transition is genuinely exercised.
BrokenPublishRequiresAcceptedEvent == \A a \in Admissions:
  envelopes[a].status = "published" => Event(a, "accepted") \in events

\* Non-vacuity witness for WorkflowEventNotAuthority. That invariant only
\* bites when a workflow event has been written for a derived fact whose
\* producing admission is in a non-{accepted, published} status.
\* RejectDerivedPolicy / FailDerivedProducer write the workflow event while
\* moving the admission to rejected / failed, reaching exactly that state.
\* Asserting it never happens must be refuted.
BrokenNoWorkflowEventOnNonAccepted == \A f \in DerivedFacts:
  ~(/\ derivedFacts[f].workflowEventWritten
    /\ derivedFacts[f].admission \in Admissions
    /\ envelopes[derivedFacts[f].admission].status \notin {
         "accepted", "published"
       })

====
