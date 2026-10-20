---- MODULE DeltaProjection ----
EXTENDS FiniteSets

Records == {"rec_a", "rec_b", "rec_c"}

Outputs == {"out_a", "out_b", "summary"}

Phases == {"collected", "projected", "lookup_ready", "planned"}

DependencyKinds == {"source_record", "graph_node", "component_hash", "decision"}

Dependencies == [
  out_a   |-> {"source:repo-sync:rec_a", "graph:node:rec_a"},
  out_b   |-> {"source:repo-sync:rec_b", "graph:node:rec_b"},
  summary |-> {"source:repo-sync:rec_a",
               "source:repo-sync:rec_b",
               "component:summary:v1"}
]

AllDependencyKeys == UNION {Dependencies[output] : output \in Outputs}

MissingOnlyKeys == {"decision:missing"}

LookupDependencyKeys == AllDependencyKeys \cup MissingOnlyKeys

KindOf(key) ==
  CASE key \in {"source:repo-sync:rec_a", "source:repo-sync:rec_b"}
       -> "source_record"
    [] key \in {"graph:node:rec_a", "graph:node:rec_b"}
       -> "graph_node"
    [] key = "component:summary:v1"
       -> "component_hash"
    [] key = "decision:missing"
       -> "decision"

VARIABLES
  phase,
  previousLiveRecords,
  insertedRecords,
  retractedRecords,
  emittedRetractions,
  liveProvenance,
  changedDependencyKeys,
  coveredDependencyKinds,
  missingDependencyKinds,
  hasReusablePrevious,
  lookupDirtyOutputs,
  plannedDirtyOutputs,
  reusableOutputs,
  fullOutputs,
  fallbackOutputs

vars == <<
  phase,
  previousLiveRecords,
  insertedRecords,
  retractedRecords,
  emittedRetractions,
  liveProvenance,
  changedDependencyKeys,
  coveredDependencyKinds,
  missingDependencyKinds,
  hasReusablePrevious,
  lookupDirtyOutputs,
  plannedDirtyOutputs,
  reusableOutputs,
  fullOutputs,
  fallbackOutputs
>>

ExpectedDirtyOutputs ==
  {output \in Outputs : Dependencies[output] \cap changedDependencyKeys # {}}

CoveredChangedDependencyKeys ==
  {key \in changedDependencyKeys : KindOf(key) \in coveredDependencyKinds}

ExpectedCoveredDirtyOutputs ==
  {output \in Outputs : Dependencies[output] \cap CoveredChangedDependencyKeys # {}}

RequestedDependencyKinds == {KindOf(key) : key \in changedDependencyKeys}

MissingDependencyKindsAfterLookup ==
  RequestedDependencyKinds \ coveredDependencyKinds

ReusableAfterLookup(dirty) ==
  {output \in Outputs \ dirty : hasReusablePrevious[output]}

FullAfterLookup(dirty) == Outputs \ (dirty \cup ReusableAfterLookup(dirty))

Init ==
  /\ phase = "collected"
  /\ previousLiveRecords \in SUBSET Records
  /\ insertedRecords \in SUBSET Records
  /\ retractedRecords \in SUBSET previousLiveRecords
  /\ emittedRetractions = {}
  /\ liveProvenance = previousLiveRecords
  /\ changedDependencyKeys \in SUBSET LookupDependencyKeys
  /\ coveredDependencyKinds \in SUBSET DependencyKinds
  /\ missingDependencyKinds = {}
  /\ hasReusablePrevious \in [Outputs -> BOOLEAN]
  /\ lookupDirtyOutputs = {}
  /\ plannedDirtyOutputs = {}
  /\ reusableOutputs = {}
  /\ fullOutputs = {}
  /\ fallbackOutputs = {}

Project ==
  /\ phase = "collected"
  /\ phase' = "projected"
  /\ emittedRetractions' = retractedRecords
  /\ liveProvenance' = (previousLiveRecords \cup insertedRecords) \ retractedRecords
  /\ UNCHANGED <<
       previousLiveRecords,
       insertedRecords,
       retractedRecords,
       changedDependencyKeys,
       coveredDependencyKinds,
       missingDependencyKinds,
       hasReusablePrevious,
       lookupDirtyOutputs,
       plannedDirtyOutputs,
       reusableOutputs,
       fullOutputs,
       fallbackOutputs
     >>

Lookup ==
  /\ phase = "projected"
  /\ phase' = "lookup_ready"
  /\ missingDependencyKinds' = MissingDependencyKindsAfterLookup
  /\ lookupDirtyOutputs' = ExpectedCoveredDirtyOutputs
  /\ UNCHANGED <<
       previousLiveRecords,
       insertedRecords,
       retractedRecords,
       emittedRetractions,
       liveProvenance,
       changedDependencyKeys,
       coveredDependencyKinds,
       hasReusablePrevious,
       plannedDirtyOutputs,
       reusableOutputs,
       fullOutputs,
       fallbackOutputs
     >>

Plan ==
  /\ phase = "lookup_ready"
  /\ phase' = "planned"
  /\ plannedDirtyOutputs' = lookupDirtyOutputs
  /\ IF missingDependencyKinds # {}
     THEN /\ reusableOutputs' = {}
          /\ fullOutputs' = {}
          /\ fallbackOutputs' = Outputs
     ELSE /\ reusableOutputs' = ReusableAfterLookup(lookupDirtyOutputs)
          /\ fullOutputs' = FullAfterLookup(lookupDirtyOutputs)
          /\ fallbackOutputs' = {}
  /\ UNCHANGED <<
       previousLiveRecords,
       insertedRecords,
       retractedRecords,
       emittedRetractions,
       liveProvenance,
       changedDependencyKeys,
       coveredDependencyKinds,
       missingDependencyKinds,
       hasReusablePrevious,
       lookupDirtyOutputs
     >>

Stutter == /\ phase = "planned"
           /\ UNCHANGED vars

Next == Project \/ Lookup \/ Plan \/ Stutter

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ phase \in Phases
  /\ previousLiveRecords \subseteq Records
  /\ insertedRecords \subseteq Records
  /\ retractedRecords \subseteq previousLiveRecords
  /\ emittedRetractions \subseteq Records
  /\ liveProvenance \subseteq Records
  /\ changedDependencyKeys \subseteq LookupDependencyKeys
  /\ coveredDependencyKinds \subseteq DependencyKinds
  /\ missingDependencyKinds \subseteq DependencyKinds
  /\ hasReusablePrevious \in [Outputs -> BOOLEAN]
  /\ lookupDirtyOutputs \subseteq Outputs
  /\ plannedDirtyOutputs \subseteq Outputs
  /\ reusableOutputs \subseteq Outputs
  /\ fullOutputs \subseteq Outputs
  /\ fallbackOutputs \subseteq Outputs

ProjectionRetractionsAreFaithful ==
  phase \in {"projected", "lookup_ready", "planned"} =>
    /\ emittedRetractions = retractedRecords
    /\ liveProvenance = (previousLiveRecords \cup insertedRecords) \ retractedRecords

RetractionDoesNotLeaveLiveProvenance ==
  phase \in {"projected", "lookup_ready", "planned"} =>
    retractedRecords \cap liveProvenance = {}

LookupPreservesCoveredDirtyOutputs ==
  phase \in {"lookup_ready", "planned"} =>
    lookupDirtyOutputs = ExpectedCoveredDirtyOutputs

DirtyScopesCoverChangedDependents ==
  phase = "planned" =>
    /\ lookupDirtyOutputs \subseteq plannedDirtyOutputs
    /\ ExpectedDirtyOutputs \subseteq (plannedDirtyOutputs \cup fallbackOutputs)

NoChangedDependentReused ==
  phase = "planned" => ExpectedDirtyOutputs \cap reusableOutputs = {}

UnavailableLookupForcesFullFallback ==
  /\ phase = "planned"
  /\ missingDependencyKinds # {}
  => /\ fallbackOutputs = Outputs
     /\ reusableOutputs = {}

MixedUnavailablePreservesCoveredDirtyScopes ==
  /\ phase = "planned"
  /\ missingDependencyKinds # {}
  => lookupDirtyOutputs \subseteq plannedDirtyOutputs

DirtyPlanAccountsForUniverse ==
  phase = "planned" =>
    plannedDirtyOutputs \cup reusableOutputs \cup fullOutputs \cup fallbackOutputs
      = Outputs

DeltaProjectionContract ==
  /\ TypeOK
  /\ ProjectionRetractionsAreFaithful
  /\ RetractionDoesNotLeaveLiveProvenance
  /\ LookupPreservesCoveredDirtyOutputs
  /\ DirtyScopesCoverChangedDependents
  /\ NoChangedDependentReused
  /\ UnavailableLookupForcesFullFallback
  /\ MixedUnavailablePreservesCoveredDirtyScopes
  /\ DirtyPlanAccountsForUniverse

====
