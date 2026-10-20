---- MODULE TransformGateNodeSubtypeScope ----
EXTENDS FiniteSets

CONSTANT BuggyTrustsUnsoundDelta

Nodes == {"dirty_new", "dirty_retyped", "clean"}

FallbackReasons == {
  "none",
  "no_previous_generation",
  "catalog_version_changed",
  "delta_counts_unavailable",
  "dirty_scope_unavailable",
  "dirty_edge_scope_unavailable",
  "dirty_node_scope_mismatch",
  "dirty_edge_scope_mismatch"
}

Phases == {"collected", "selected", "evaluated"}

VARIABLES
  phase,
  dirtyNodes,
  invalidSubtypeNodes,
  fallbackReason,
  candidateNodes,
  blocksPublish

vars == <<
  phase,
  dirtyNodes,
  invalidSubtypeNodes,
  fallbackReason,
  candidateNodes,
  blocksPublish
>>

CandidateNodesFor(reason, dirty) ==
  IF reason = "none" THEN dirty ELSE Nodes

PreviousGenerationClean == TRUE

DeltaEvidenceSound ==
  fallbackReason = "none" => invalidSubtypeNodes \subseteq dirtyNodes

Init ==
  /\ phase = "collected"
  /\ dirtyNodes \in SUBSET Nodes
  /\ invalidSubtypeNodes \in SUBSET Nodes
  /\ fallbackReason \in FallbackReasons
  /\ PreviousGenerationClean
  \* BuggyTrustsUnsoundDelta (FALSE on the positive config) relaxes the
  \* DeltaEvidenceSound precondition so an invalid-subtype node may sit outside
  \* the dirty scope, escape candidate selection, and publish without blocking --
  \* refuting NoInvalidPostNodePublishes by name and proving the guard is
  \* load-bearing.
  /\ (BuggyTrustsUnsoundDelta \/ DeltaEvidenceSound)
  /\ candidateNodes = {}
  /\ blocksPublish = FALSE

SelectCandidates ==
  /\ phase = "collected"
  /\ phase' = "selected"
  /\ candidateNodes' = CandidateNodesFor(fallbackReason, dirtyNodes)
  /\ UNCHANGED << dirtyNodes, invalidSubtypeNodes, fallbackReason, blocksPublish >>

EvaluateCandidates ==
  /\ phase = "selected"
  /\ phase' = "evaluated"
  /\ blocksPublish' = (candidateNodes \cap invalidSubtypeNodes # {})
  /\ UNCHANGED << dirtyNodes, invalidSubtypeNodes, fallbackReason, candidateNodes >>

Stutter == /\ phase = "evaluated"
           /\ UNCHANGED vars

Next == SelectCandidates \/ EvaluateCandidates \/ Stutter

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ phase \in Phases
  /\ dirtyNodes \subseteq Nodes
  /\ invalidSubtypeNodes \subseteq Nodes
  /\ fallbackReason \in FallbackReasons
  /\ candidateNodes \subseteq Nodes
  /\ blocksPublish \in BOOLEAN

CandidateNodeSelectionMatchesDirtyScope ==
  phase \in {"selected", "evaluated"} =>
    candidateNodes = CandidateNodesFor(fallbackReason, dirtyNodes)

PreviousCleanDeltaCoversInvalidNodes ==
  /\ phase \in {"selected", "evaluated"}
  /\ fallbackReason = "none"
  => invalidSubtypeNodes \subseteq candidateNodes

InvalidDirtyNodeCannotPublish ==
  /\ phase = "evaluated"
  /\ fallbackReason = "none"
  /\ dirtyNodes \cap invalidSubtypeNodes # {}
  => blocksPublish

UnsafeDirtyScopeFallsBackFull ==
  /\ phase \in {"selected", "evaluated"}
  /\ fallbackReason # "none"
  => candidateNodes = Nodes

NoInvalidPostNodePublishes ==
  /\ phase = "evaluated"
  /\ invalidSubtypeNodes # {}
  => blocksPublish

CleanPostNodesPass ==
  /\ phase = "evaluated"
  /\ invalidSubtypeNodes = {}
  => ~blocksPublish

TransformGateNodeSubtypeScopeSoundness ==
  /\ TypeOK
  /\ CandidateNodeSelectionMatchesDirtyScope
  /\ PreviousCleanDeltaCoversInvalidNodes
  /\ InvalidDirtyNodeCannotPublish
  /\ UnsafeDirtyScopeFallsBackFull
  /\ NoInvalidPostNodePublishes
  /\ CleanPostNodesPass

====
