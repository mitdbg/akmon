---- MODULE AltheaOptimizerSafety ----
EXTENDS FiniteSets, Naturals

Candidates == {
  "c_good",
  "c_other",
  "c_conflict_a",
  "c_conflict_b",
  "c_bad"
}

EligibleCandidates == {"c_good", "c_other", "c_conflict_a", "c_conflict_b"}

BaseNodes == {"n_base"}
BaseEdges == {"e_base"}
OverlayNodes == {"n_overlay"}
OverlayEdges == {"e_overlay"}
AttrValues == {"original", "mutated"}
CostValues == 0..3
Phases == {"declared", "selected", "materialized"}

\* When TRUE, model an optimizer that MUTATES a base-graph attribute while
\* materializing its overlay -- the destructive-rewrite hazard the additive-overlay
\* contract forbids. The main config pins this FALSE (Althea's search is additive:
\* it only adds overlay nodes/edges), so BaseNodeAttrsPreserved / BaseEdgeAttrsPreserved
\* hold. The mutate-base counterexample config sets it TRUE so a materialization
\* rewrites the base attr and TLC refutes BaseNodeAttrsPreserved BY NAME -- proving
\* the preservation invariants are load-bearing, not guard-restatements (no
\* transition otherwise touches baseNodeAttrs). The real load-bearing proof is the
\* executable checker src/okg/optimizer/formal_search.py, which diffs real
\* before/after graph attrs; this TLC witness makes the kernel non-vacuous.
CONSTANT BuggyOptimizerMutatesBase

ConflictToken(candidate) ==
  CASE candidate = "c_conflict_a" -> "shared_conflict"
    [] candidate = "c_conflict_b" -> "shared_conflict"
    [] OTHER -> candidate

PairwiseConflictFree(candidate_set) ==
  \A left \in candidate_set:
    \A right \in candidate_set:
      left # right => ConflictToken(left) # ConflictToken(right)

VARIABLES
  phase,
  admitted,
  selected,
  materialized,
  graphNodes,
  graphEdges,
  baseNodeAttrs,
  baseEdgeAttrs,
  finalCost

vars == <<
  phase,
  admitted,
  selected,
  materialized,
  graphNodes,
  graphEdges,
  baseNodeAttrs,
  baseEdgeAttrs,
  finalCost
>>

Init ==
  /\ phase = "declared"
  /\ admitted \in SUBSET Candidates
  /\ selected = {}
  /\ materialized = {}
  /\ graphNodes = BaseNodes
  /\ graphEdges = BaseEdges
  /\ baseNodeAttrs = [node \in BaseNodes |-> "original"]
  /\ baseEdgeAttrs = [edge \in BaseEdges |-> "original"]
  /\ finalCost \in CostValues

SelectCandidates ==
  /\ phase = "declared"
  /\ phase' = "selected"
  /\ selected' \in SUBSET admitted
  /\ selected' \subseteq EligibleCandidates
  /\ PairwiseConflictFree(selected')
  /\ UNCHANGED <<
       admitted,
       materialized,
       graphNodes,
       graphEdges,
       baseNodeAttrs,
       baseEdgeAttrs,
       finalCost
     >>

MaterializeSelectedCandidates ==
  /\ phase = "selected"
  /\ phase' = "materialized"
  /\ materialized' \in SUBSET selected
  /\ graphNodes' = graphNodes \cup (IF materialized' = {} THEN {} ELSE OverlayNodes)
  /\ graphEdges' = graphEdges \cup (IF materialized' = {} THEN {} ELSE OverlayEdges)
  \* Additive by contract: base attrs are untouched. Under the buggy toggle a
  \* non-empty materialization rewrites the base attr, violating preservation.
  /\ baseNodeAttrs' =
       IF BuggyOptimizerMutatesBase /\ materialized' # {}
       THEN [node \in BaseNodes |-> "mutated"]
       ELSE baseNodeAttrs
  /\ baseEdgeAttrs' =
       IF BuggyOptimizerMutatesBase /\ materialized' # {}
       THEN [edge \in BaseEdges |-> "mutated"]
       ELSE baseEdgeAttrs
  /\ UNCHANGED <<
       admitted,
       selected,
       finalCost
     >>

Stutter ==
  /\ phase = "materialized"
  /\ UNCHANGED vars

Next == SelectCandidates \/ MaterializeSelectedCandidates \/ Stutter

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ phase \in Phases
  /\ admitted \subseteq Candidates
  /\ selected \subseteq Candidates
  /\ materialized \subseteq Candidates
  /\ graphNodes \subseteq (BaseNodes \cup OverlayNodes)
  /\ graphEdges \subseteq (BaseEdges \cup OverlayEdges)
  /\ baseNodeAttrs \in [BaseNodes -> AttrValues]
  /\ baseEdgeAttrs \in [BaseEdges -> AttrValues]
  /\ finalCost \in CostValues

SearchCostNonNegative ==
  finalCost \in Nat

SelectedCandidatesAreAdmitted ==
  selected \subseteq admitted

SelectedCandidatesAreEligible ==
  selected \subseteq EligibleCandidates

SearchConflictExclusion ==
  PairwiseConflictFree(selected)

NoAdmissionBypass ==
  (selected \cup materialized) \subseteq admitted

MaterializationRequiresSearchSelection ==
  materialized \subseteq selected

BaseNodeAttrsPreserved ==
  \A node \in BaseNodes:
    baseNodeAttrs[node] = "original"

BaseEdgeAttrsPreserved ==
  \A edge \in BaseEdges:
    baseEdgeAttrs[edge] = "original"

AdditiveOverlayPreservesBase ==
  /\ BaseNodes \subseteq graphNodes
  /\ BaseEdges \subseteq graphEdges
  /\ BaseNodeAttrsPreserved
  /\ BaseEdgeAttrsPreserved

AltheaOptimizerSafetyKernel ==
  /\ TypeOK
  /\ SearchCostNonNegative
  /\ SelectedCandidatesAreAdmitted
  /\ SelectedCandidatesAreEligible
  /\ SearchConflictExclusion
  /\ NoAdmissionBypass
  /\ MaterializationRequiresSearchSelection
  /\ BaseNodeAttrsPreserved
  /\ BaseEdgeAttrsPreserved
  /\ AdditiveOverlayPreservesBase

====
