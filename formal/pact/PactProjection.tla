---- MODULE PactProjection ----
EXTENDS FiniteSets

CONSTANTS
  InitialState,
  DependencyState,
  VerifiedState,
  NoState,
  Dep1,
  Dep2,
  Proof1,
  Proof2

States == {InitialState, DependencyState, VerifiedState}
DependencyEdges == {Dep1, Dep2}
ProofEdges == {Proof1, Proof2}

AccessStates == {"available", "unavailable"}
ProjectionStatuses == {"fresh", "stale", "missing", "unavailable"}
LifecycleStatuses == {"proposal", "implementation", "verification"}
ActionNames == {"init", "edit", "publish", "graph_read", "set_access"}

DepsOf(s) ==
  IF s = InitialState
  THEN {}
  ELSE IF s = DependencyState
  THEN {Dep1}
  ELSE {Dep1, Dep2}

ProofsOf(s) ==
  IF s = InitialState
  THEN {}
  ELSE IF s = DependencyState
  THEN {Proof1}
  ELSE {Proof1, Proof2}

LifecycleOf(s) ==
  IF s = InitialState
  THEN "proposal"
  ELSE IF s = DependencyState
  THEN "implementation"
  ELSE "verification"

VARIABLES
  fileState,
  graphState,
  graphDeps,
  graphProofs,
  graphLifecycle,
  access,
  lastAction,
  beforeFile

vars == <<
  fileState,
  graphState,
  graphDeps,
  graphProofs,
  graphLifecycle,
  access,
  lastAction,
  beforeFile
>>

ObservableStatus ==
  IF access = "unavailable"
  THEN "unavailable"
  ELSE IF graphState = NoState
  THEN "missing"
  ELSE IF graphState = fileState
  THEN "fresh"
  ELSE "stale"

Init ==
  /\ fileState = InitialState
  /\ graphState = NoState
  /\ graphDeps = {}
  /\ graphProofs = {}
  /\ graphLifecycle = "none"
  /\ access = "available"
  /\ lastAction = "init"
  /\ beforeFile = InitialState

EditFile(s) ==
  /\ s \in States
  /\ beforeFile' = fileState
  /\ fileState' = s
  /\ lastAction' = "edit"
  /\ UNCHANGED <<
       graphState,
       graphDeps,
       graphProofs,
       graphLifecycle,
       access
     >>

Publish ==
  /\ beforeFile' = fileState
  /\ graphState' = fileState
  /\ graphDeps' = DepsOf(fileState)
  /\ graphProofs' = ProofsOf(fileState)
  /\ graphLifecycle' = LifecycleOf(fileState)
  /\ lastAction' = "publish"
  /\ UNCHANGED << fileState, access >>

GraphRead ==
  /\ beforeFile' = fileState
  /\ lastAction' = "graph_read"
  /\ UNCHANGED <<
       fileState,
       graphState,
       graphDeps,
       graphProofs,
       graphLifecycle,
       access
     >>

SetAccess(a) ==
  /\ a \in AccessStates
  /\ beforeFile' = fileState
  /\ access' = a
  /\ lastAction' = "set_access"
  /\ UNCHANGED <<
       fileState,
       graphState,
       graphDeps,
       graphProofs,
       graphLifecycle
     >>

Next ==
  \/ \E s \in States: EditFile(s)
  \/ Publish
  \/ GraphRead
  \/ \E a \in AccessStates: SetAccess(a)

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ InitialState \in States
  /\ DependencyState \in States
  /\ VerifiedState \in States
  /\ Cardinality(States) = 3
  /\ NoState \notin States
  /\ Dep1 \in DependencyEdges
  /\ Dep2 \in DependencyEdges
  /\ Cardinality(DependencyEdges) = 2
  /\ Proof1 \in ProofEdges
  /\ Proof2 \in ProofEdges
  /\ Cardinality(ProofEdges) = 2
  /\ fileState \in States
  /\ graphState \in States \cup {NoState}
  /\ graphDeps \subseteq DependencyEdges
  /\ graphProofs \subseteq ProofEdges
  /\ graphLifecycle \in LifecycleStatuses \cup {"none"}
  /\ access \in AccessStates
  /\ lastAction \in ActionNames
  /\ beforeFile \in States

GraphOperationsDoNotMutateFile ==
  lastAction \in {"publish", "graph_read", "set_access"} => fileState = beforeFile

\* DEFINITIONAL SANITY-CHECK (not an independent safety proof). This invariant
\* re-derives each branch of the ObservableStatus operator (unavailable /
\* missing / fresh / stale) from its inputs, so it holds by construction. It
\* documents the status-classification contract; the load-bearing projection
\* safety lives in PublishedProjectionIsStateSnapshot and the FreshGraph*
\* invariants below.
FreshnessStatusReflectsProjection ==
  /\ access = "unavailable" => ObservableStatus = "unavailable"
  /\ access # "unavailable" /\ graphState = NoState =>
       ObservableStatus = "missing"
  /\ access # "unavailable" /\ graphState # NoState /\ graphState = fileState =>
       ObservableStatus = "fresh"
  /\ access # "unavailable" /\ graphState # NoState /\ graphState # fileState =>
       ObservableStatus = "stale"
  /\ ObservableStatus \in ProjectionStatuses

PublishedProjectionIsStateSnapshot ==
  /\ graphState = NoState =>
       /\ graphDeps = {}
       /\ graphProofs = {}
       /\ graphLifecycle = "none"
  /\ graphState # NoState =>
       /\ graphDeps = DepsOf(graphState)
       /\ graphProofs = ProofsOf(graphState)
       /\ graphLifecycle = LifecycleOf(graphState)

FreshGraphCarriesProjectedDependencies ==
  ObservableStatus = "fresh" => graphDeps = DepsOf(fileState)

FreshGraphCarriesProjectedProofs ==
  ObservableStatus = "fresh" => graphProofs = ProofsOf(fileState)

FreshGraphCarriesLifecycle ==
  ObservableStatus = "fresh" => graphLifecycle = LifecycleOf(fileState)

PactProjectionContract ==
  /\ GraphOperationsDoNotMutateFile
  /\ FreshnessStatusReflectsProjection
  /\ PublishedProjectionIsStateSnapshot
  /\ FreshGraphCarriesProjectedDependencies
  /\ FreshGraphCarriesProjectedProofs
  /\ FreshGraphCarriesLifecycle

====
