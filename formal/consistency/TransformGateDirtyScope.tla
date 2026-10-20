---- MODULE TransformGateDirtyScope ----
EXTENDS FiniteSets, Naturals

CONSTANTS Edges, Nodes, FallbackReasons, BuggyTrustsUnsoundDelta,
          BuggyChecksOnlyDestination

Triples == {"allowed", "rejected"}

AllowedTriples == {"allowed"}

RejectedTriples == Triples \ AllowedTriples

Phases == {"collected", "selected", "evaluated"}

IncidentEdges(node) ==
  CASE node = "node_topology" -> {"edge_incident"}
    [] node = "node_additive" -> {"edge_clean"}
    [] node = "node_clean" -> {}

IncidentEdgesFor(nodes) == UNION {IncidentEdges(node) : node \in nodes}

VARIABLES
  phase,
  previousLiveEdges,
  upsertedEdges,
  retractedEdges,
  rejectedTripleEdges,
  missingSrcEdges,
  missingDstEdges,
  dirtyEdges,
  dirtyNodes,
  topologyDirtyNodes,
  fallbackReason,
  candidateEdges,
  scopeMode,
  blocksPublish

vars == <<
  phase,
  previousLiveEdges,
  upsertedEdges,
  retractedEdges,
  rejectedTripleEdges,
  missingSrcEdges,
  missingDstEdges,
  dirtyEdges,
  dirtyNodes,
  topologyDirtyNodes,
  fallbackReason,
  candidateEdges,
  scopeMode,
  blocksPublish
>>

SmokeDirtyScopeConstraint ==
  /\ fallbackReason \in {"none", "dirty_scope_unavailable"}
  /\ Cardinality(previousLiveEdges) <= 2
  /\ Cardinality(upsertedEdges) <= 1
  /\ Cardinality(retractedEdges) <= 1
  /\ Cardinality(rejectedTripleEdges) <= 1
  /\ Cardinality(missingSrcEdges) <= 1
  /\ Cardinality(missingDstEdges) <= 1
  /\ Cardinality(dirtyEdges) <= 1
  /\ Cardinality(dirtyNodes) <= 2
  /\ Cardinality(topologyDirtyNodes) <= 1

\* The release profile ranges the full existing dirty-scope universe while
\* bounding endpoint absence to one offending edge. That edge independently
\* ranges source-only, destination-only, and both-endpoints-missing states.
FullDirtyScopeConstraint ==
  Cardinality(missingSrcEdges \cup missingDstEdges) <= 1

MissingSourceWitnessInit ==
  /\ phase = "collected"
  /\ previousLiveEdges = {"edge_incident"}
  /\ upsertedEdges = {}
  /\ retractedEdges = {}
  /\ rejectedTripleEdges = {}
  /\ missingSrcEdges = {"edge_incident"}
  /\ missingDstEdges = {}
  /\ dirtyEdges = {}
  /\ dirtyNodes = {"node_topology"}
  /\ topologyDirtyNodes = {"node_topology"}
  /\ fallbackReason = "none"
  /\ candidateEdges = {}
  /\ scopeMode = "unselected"
  /\ blocksPublish = FALSE

PostLiveEdges == (previousLiveEdges \ retractedEdges) \cup upsertedEdges

InvalidPostEdges ==
  PostLiveEdges \cap (rejectedTripleEdges \cup missingSrcEdges \cup missingDstEdges)

ObservedMissingEndpointEdges ==
  IF BuggyChecksOnlyDestination
  THEN missingDstEdges
  ELSE missingSrcEdges \cup missingDstEdges

ObservedInvalidPostEdges ==
  PostLiveEdges \cap (rejectedTripleEdges \cup ObservedMissingEndpointEdges)

ScopedCandidates ==
  PostLiveEdges \cap (dirtyEdges \cup IncidentEdgesFor(topologyDirtyNodes))

PreviousGenerationClean == TRUE

UnchangedSurvivingEdges ==
  (previousLiveEdges \ retractedEdges)
    \ (dirtyEdges \cup IncidentEdgesFor(topologyDirtyNodes))

DeltaEvidenceSound ==
  /\ retractedEdges \subseteq previousLiveEdges
  /\ upsertedEdges \cap retractedEdges = {}
  /\ upsertedEdges \subseteq dirtyEdges
  /\ retractedEdges \subseteq dirtyEdges
  /\ topologyDirtyNodes \subseteq dirtyNodes
  /\ (rejectedTripleEdges \cup missingSrcEdges \cup missingDstEdges)
     \cap UnchangedSurvivingEdges = {}

Init ==
  /\ phase = "collected"
  /\ previousLiveEdges \in SUBSET Edges
  /\ upsertedEdges \in SUBSET Edges
  /\ retractedEdges \in SUBSET Edges
  /\ rejectedTripleEdges \in SUBSET Edges
  /\ missingSrcEdges \in SUBSET Edges
  /\ missingDstEdges \in SUBSET Edges
  /\ dirtyEdges \in SUBSET Edges
  /\ dirtyNodes \in SUBSET Nodes
  /\ topologyDirtyNodes \in SUBSET dirtyNodes
  /\ fallbackReason \in FallbackReasons
  /\ PreviousGenerationClean
  \* DeltaEvidenceSound is the load-bearing precondition: the scoped dirty-edge
  \* selection is only safe because the collected delta faithfully covers every
  \* changed/invalid edge. BuggyTrustsUnsoundDelta (FALSE on the positive config)
  \* relaxes it, so the model may start from an unsound delta where an invalid
  \* post-edge sits outside the dirty scope, escapes candidate selection, and is
  \* published without blocking -- refuting NoInvalidPostEdgePublishes by name.
  /\ (BuggyTrustsUnsoundDelta \/ DeltaEvidenceSound)
  /\ candidateEdges = {}
  /\ scopeMode = "unselected"
  /\ blocksPublish = FALSE

\* Deterministic witness for the load-bearing delta-evidence guard.  The
\* concrete state is explicitly unsound and exists only with the buggy toggle,
\* avoiding an exhaustive search through every unsound Init combination.
UnsoundDeltaEdgeWitnessInit ==
  /\ BuggyTrustsUnsoundDelta
  /\ phase = "collected"
  /\ previousLiveEdges = {"edge_clean"}
  /\ upsertedEdges = {}
  /\ retractedEdges = {}
  /\ rejectedTripleEdges = {"edge_clean"}
  /\ missingSrcEdges = {}
  /\ missingDstEdges = {}
  /\ dirtyEdges = {}
  /\ dirtyNodes = {}
  /\ topologyDirtyNodes = {}
  /\ fallbackReason = "none"
  /\ candidateEdges = {}
  /\ scopeMode = "unselected"
  /\ blocksPublish = FALSE
  /\ ~DeltaEvidenceSound

SelectCandidates ==
  /\ phase = "collected"
  /\ phase' = "selected"
  /\ IF fallbackReason = "none"
     THEN /\ candidateEdges' = ScopedCandidates
          /\ scopeMode' = "dirty_edges+incident_dirty_nodes"
     ELSE /\ candidateEdges' = PostLiveEdges
          /\ scopeMode' = "full"
  /\ UNCHANGED <<
       previousLiveEdges,
       upsertedEdges,
       retractedEdges,
       rejectedTripleEdges,
       missingSrcEdges,
       missingDstEdges,
       dirtyEdges,
       dirtyNodes,
       topologyDirtyNodes,
       fallbackReason,
       blocksPublish
     >>

EvaluateCandidates ==
  /\ phase = "selected"
  /\ phase' = "evaluated"
  /\ blocksPublish' = (candidateEdges \cap ObservedInvalidPostEdges # {})
  /\ UNCHANGED <<
       previousLiveEdges,
       upsertedEdges,
       retractedEdges,
       rejectedTripleEdges,
       missingSrcEdges,
       missingDstEdges,
       dirtyEdges,
       dirtyNodes,
       topologyDirtyNodes,
       fallbackReason,
       candidateEdges,
       scopeMode
     >>

Stutter == /\ phase = "evaluated"
           /\ UNCHANGED vars

Next == SelectCandidates \/ EvaluateCandidates \/ Stutter

Spec == Init /\ [][Next]_vars

MissingSourceWitnessSpec == MissingSourceWitnessInit /\ [][Next]_vars

UnsoundDeltaEdgeWitnessSpec == UnsoundDeltaEdgeWitnessInit /\ [][Next]_vars

TypeOK ==
  /\ phase \in Phases
  /\ previousLiveEdges \subseteq Edges
  /\ upsertedEdges \subseteq Edges
  /\ retractedEdges \subseteq Edges
  /\ rejectedTripleEdges \subseteq Edges
  /\ missingSrcEdges \subseteq Edges
  /\ missingDstEdges \subseteq Edges
  /\ dirtyEdges \subseteq Edges
  /\ dirtyNodes \subseteq Nodes
  /\ topologyDirtyNodes \subseteq dirtyNodes
  /\ fallbackReason \in FallbackReasons
  /\ candidateEdges \subseteq Edges
  /\ scopeMode \in {"unselected", "dirty_edges+incident_dirty_nodes", "full"}
  /\ blocksPublish \in BOOLEAN

RetractionsDoNotSurvive ==
  retractedEdges \cap PostLiveEdges = {}

CandidateSelectionMatchesDirtyScope ==
  phase \in {"selected", "evaluated"} =>
    IF fallbackReason = "none"
    THEN /\ candidateEdges = ScopedCandidates
         /\ scopeMode = "dirty_edges+incident_dirty_nodes"
    ELSE /\ candidateEdges = PostLiveEdges
         /\ scopeMode = "full"

PreviousCleanDeltaCoversInvalidEdges ==
  /\ phase \in {"selected", "evaluated"}
  /\ fallbackReason = "none"
  => InvalidPostEdges \subseteq candidateEdges

InvalidDirtyEdgeCannotPublish ==
  /\ phase = "evaluated"
  /\ fallbackReason = "none"
  /\ dirtyEdges \cap InvalidPostEdges # {}
  => blocksPublish

TopologyDirtyIncidentEdgesInScope ==
  /\ phase \in {"selected", "evaluated"}
  /\ fallbackReason = "none"
  => (IncidentEdgesFor(topologyDirtyNodes) \cap PostLiveEdges) \subseteq candidateEdges

AdditiveNodesDoNotExpandIncidentEdges ==
  /\ phase \in {"selected", "evaluated"}
  /\ fallbackReason = "none"
  => (IncidentEdgesFor(dirtyNodes \ topologyDirtyNodes) \ (dirtyEdges \cup retractedEdges))
     \cap candidateEdges = {}

UnsafeDirtyScopeFallsBackFull ==
  /\ phase \in {"selected", "evaluated"}
  /\ fallbackReason # "none"
  => /\ candidateEdges = PostLiveEdges
     /\ scopeMode = "full"

NoInvalidPostEdgePublishes ==
  /\ phase = "evaluated"
  /\ InvalidPostEdges # {}
  => blocksPublish

MissingSourceEndpointCannotPublish ==
  /\ phase = "evaluated"
  /\ candidateEdges \cap missingSrcEdges # {}
  => blocksPublish

MissingDestinationEndpointCannotPublish ==
  /\ phase = "evaluated"
  /\ candidateEdges \cap missingDstEdges # {}
  => blocksPublish

CleanPostEdgesPass ==
  /\ phase = "evaluated"
  /\ InvalidPostEdges = {}
  => ~blocksPublish

TransformGateDirtyScopeSoundness ==
  /\ TypeOK
  /\ RetractionsDoNotSurvive
  /\ CandidateSelectionMatchesDirtyScope
  /\ PreviousCleanDeltaCoversInvalidEdges
  /\ InvalidDirtyEdgeCannotPublish
  /\ TopologyDirtyIncidentEdgesInScope
  /\ AdditiveNodesDoNotExpandIncidentEdges
  /\ UnsafeDirtyScopeFallsBackFull
  /\ NoInvalidPostEdgePublishes
  /\ MissingSourceEndpointCannotPublish
  /\ MissingDestinationEndpointCannotPublish
  /\ CleanPostEdgesPass

====
