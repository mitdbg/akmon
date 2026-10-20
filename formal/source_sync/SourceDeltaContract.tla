---- MODULE SourceDeltaContract ----
EXTENDS Naturals, FiniteSets

\* Source-delta contract, rewritten as the runner's actual emit pipeline
\* (strengthen-formal-models wave 5). The previous revision had
\* Next == UNCHANGED vars and conjoined retraction completeness into the
\* acceptance predicate, making every invariant a P /\ Q => Q tautology.
\* The pipeline modeled now, faithful to SourceRunner:
\*   1. DiffAndEmit — the runner diffs RAW record-set ids (the audit's
\*      critique: the diff itself does not canonicalize) and synthesizes
\*      retraction facts carrying raw prior ids.
\*   2. FlushRetractions — the per-chunk flush canonicalizes ALL fact ids
\*      (op='R' included) before COPY; retractions are flushed BEFORE the
\*      run's source facts.
\*   3. FlushInserts — the current records' inserts land after, so alias
\*      churn (same canonical node behind a rotated raw id) ends with the
\*      insert as the latest fact.
\*   4. Accept — frontier/progress and affected-scope evidence gate
\*      acceptance; retraction completeness is NOT an acceptance check in
\*      production — it holds because the diff consumes the full prior
\*      record set. The buggy toggles break each mechanism so TLC can
\*      refute the corresponding REAL invariant by name.

CONSTANTS
  BuggyOmitTombstone,
  BuggySkipRetractionCanonicalize,
  BuggyRetractionsAfterInserts

StableIds == {"doc:a", "edge:a"}
AliasIds == {"doc:a:alias", "edge:a:alias"}
RawIds == StableIds \cup AliasIds
Ops == {"none", "I", "R"}
Phases == {"start", "emitted", "retractions_flushed", "inserts_flushed",
           "accepted"}

CanonicalId(raw) ==
  CASE raw = "doc:a:alias" -> "doc:a"
    [] raw = "edge:a:alias" -> "edge:a"
    [] OTHER -> raw

Canonicalized(ids) == {CanonicalId(raw) : raw \in ids}

SmallRawSets == {ids \in SUBSET RawIds : Cardinality(ids) <= 1}

VARIABLES
  phase,
  previousRawIds,
  currentRawIds,
  explicitRetractedRawIds,
  emittedRetractionRawIds,
  flushedRetractionIds,
  latestOp,
  nextFrontierPresent,
  progressMarkerPresent,
  affectedScopesPresent,
  completeScope

vars == <<
  phase,
  previousRawIds,
  currentRawIds,
  explicitRetractedRawIds,
  emittedRetractionRawIds,
  flushedRetractionIds,
  latestOp,
  nextFrontierPresent,
  progressMarkerPresent,
  affectedScopesPresent,
  completeScope
>>

Init ==
  /\ phase = "start"
  /\ previousRawIds \in SmallRawSets
  /\ currentRawIds \in SmallRawSets
  /\ explicitRetractedRawIds \in SmallRawSets
  /\ emittedRetractionRawIds = {}
  /\ flushedRetractionIds = {}
  /\ latestOp = [c \in StableIds |-> "none"]
  /\ nextFrontierPresent \in BOOLEAN
  /\ progressMarkerPresent \in BOOLEAN
  /\ affectedScopesPresent \in BOOLEAN
  /\ completeScope \in BOOLEAN

\* The raw-set diff: disappeared prior raw ids plus explicit adapter
\* retractions. BuggyOmitTombstone drops the diff-derived tombstones — the
\* incomplete-batch hazard RetractionsAreComplete exists to catch.
RawRemoved ==
  IF completeScope THEN previousRawIds \ currentRawIds ELSE {}

DiffAndEmit ==
  /\ phase = "start"
  /\ emittedRetractionRawIds' =
       explicitRetractedRawIds \cup
         (IF BuggyOmitTombstone THEN {} ELSE RawRemoved)
  /\ phase' = "emitted"
  /\ UNCHANGED <<
       previousRawIds, currentRawIds, explicitRetractedRawIds,
       flushedRetractionIds, latestOp, nextFrontierPresent,
       progressMarkerPresent, affectedScopesPresent, completeScope
     >>

\* The flush canonicalizes every retraction id before COPY. The buggy
\* toggle lands raw ids — the audit's silent no-op tombstone, permanently
\* guarded by TombstonesLandCanonical.
FlushRetractions ==
  /\ phase = "emitted"
  /\ flushedRetractionIds' =
       IF BuggySkipRetractionCanonicalize
       THEN emittedRetractionRawIds
       ELSE Canonicalized(emittedRetractionRawIds)
  /\ latestOp' =
       [c \in StableIds |->
         IF c \in (IF BuggySkipRetractionCanonicalize
                   THEN emittedRetractionRawIds
                   ELSE Canonicalized(emittedRetractionRawIds))
         THEN "R" ELSE latestOp[c]]
  /\ phase' = "retractions_flushed"
  /\ UNCHANGED <<
       previousRawIds, currentRawIds, explicitRetractedRawIds,
       emittedRetractionRawIds, nextFrontierPresent,
       progressMarkerPresent, affectedScopesPresent, completeScope
     >>

\* The run's source facts flush after the synthesized retractions, so a
\* canonical node re-emitted under a rotated alias ends live. The buggy
\* toggle replays the retractions AFTER the inserts — the ordering hazard
\* ChurnKeepsRecordLive exists to catch.
FlushInserts ==
  /\ phase = "retractions_flushed"
  /\ latestOp' =
       IF BuggyRetractionsAfterInserts
       THEN [c \in StableIds |->
              IF c \in Canonicalized(flushedRetractionIds)
              THEN "R"
              ELSE IF c \in Canonicalized(currentRawIds)
                   THEN "I" ELSE latestOp[c]]
       ELSE [c \in StableIds |->
              IF c \in Canonicalized(currentRawIds)
              THEN "I" ELSE latestOp[c]]
  /\ phase' = "inserts_flushed"
  /\ UNCHANGED <<
       previousRawIds, currentRawIds, explicitRetractedRawIds,
       emittedRetractionRawIds, flushedRetractionIds, nextFrontierPresent,
       progressMarkerPresent, affectedScopesPresent, completeScope
     >>

FrontierOrProgressPresent == nextFrontierPresent \/ progressMarkerPresent

ChangedIds ==
  Canonicalized(currentRawIds) \cup Canonicalized(flushedRetractionIds)

AffectedScopesValid == ChangedIds = {} \/ affectedScopesPresent

Accept ==
  /\ phase = "inserts_flushed"
  /\ FrontierOrProgressPresent
  /\ AffectedScopesValid
  /\ phase' = "accepted"
  /\ UNCHANGED <<
       previousRawIds, currentRawIds, explicitRetractedRawIds,
       emittedRetractionRawIds, flushedRetractionIds, latestOp,
       nextFrontierPresent, progressMarkerPresent, affectedScopesPresent,
       completeScope
     >>

Next == DiffAndEmit \/ FlushRetractions \/ FlushInserts \/ Accept

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ phase \in Phases
  /\ previousRawIds \subseteq RawIds
  /\ currentRawIds \subseteq RawIds
  /\ explicitRetractedRawIds \subseteq RawIds
  /\ emittedRetractionRawIds \subseteq RawIds
  /\ flushedRetractionIds \subseteq RawIds
  /\ latestOp \in [StableIds -> Ops]
  /\ nextFrontierPresent \in BOOLEAN
  /\ progressMarkerPresent \in BOOLEAN
  /\ affectedScopesPresent \in BOOLEAN
  /\ completeScope \in BOOLEAN

\* Semantic truth: which canonical records SHOULD be retracted. Note the
\* canonical diff — alias churn (prior alias, current stable form of the
\* same record) removes NOTHING semantically even though the raw diff
\* synthesizes a tombstone (healed by ordering, see ChurnKeepsRecordLive).
ExpectedRetractedCanonical ==
  Canonicalized(explicitRetractedRawIds) \cup
    (IF completeScope
     THEN Canonicalized(previousRawIds) \ Canonicalized(currentRawIds)
     ELSE {})

\* Every semantically-removed record has a canonical tombstone by the time
\* the run is accepted. Trips when the diff drops tombstones
\* (BuggyOmitTombstone) — the mechanism is the full-prior-set diff, not an
\* acceptance-time check.
RetractionsAreComplete ==
  phase = "accepted" =>
    ExpectedRetractedCanonical \subseteq Canonicalized(flushedRetractionIds)

\* Tombstones land under canonical ids only — a raw-id tombstone targets a
\* node the graph does not hold (the audit's silent no-op hazard). Trips
\* when the flush skips canonicalizing retractions.
TombstonesLandCanonical ==
  phase \in {"retractions_flushed", "inserts_flushed", "accepted"} =>
    flushedRetractionIds \subseteq StableIds

\* Alias churn keeps the record live: a canonical record present in the
\* current set ends the run with an insert as its latest fact, never a
\* trailing tombstone. Trips when retractions replay after inserts.
ChurnKeepsRecordLive ==
  phase \in {"inserts_flushed", "accepted"} =>
    \A c \in Canonicalized(currentRawIds) : latestOp[c] = "I"

SourceDeltaContract ==
  /\ TypeOK
  /\ RetractionsAreComplete
  /\ TombstonesLandCanonical
  /\ ChurnKeepsRecordLive

====
