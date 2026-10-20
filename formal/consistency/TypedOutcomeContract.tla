---------------------- MODULE TypedOutcomeContract ----------------------
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS SemanticStates, KeyStates, ApplicabilityStates, ValueStates
SEMANTIC == SemanticStates
KEY_STATE == KeyStates
APPLICABILITY == ApplicabilityStates
VALUES == ValueStates
ALIGNMENT_UNKNOWN == {"unknown_subject", "unknown_activity", "unknown_value"}
APPLICABILITY_UNKNOWN == {"unknown_heading", "unknown_time", "unknown_scope"}
CATEGORIES == {"conflict", "alignment_uncertain",
                "applicability_uncertain", "not_comparable", "no_conflict"}

Outcome(semantic_state, key_state, applicability_state, value_state) ==
  IF key_state = "not_comparable" THEN "not_comparable"
  ELSE IF semantic_state # "established" THEN "alignment_uncertain"
  ELSE IF (value_state = "compatible") \/ (applicability_state = "disjoint")
       THEN "no_conflict"
  ELSE IF applicability_state \in APPLICABILITY_UNKNOWN
       THEN "applicability_uncertain"
  ELSE "conflict"

Actionable(category) == category = "conflict"

LegacyAdapter(category) ==
  CASE category = "conflict" -> "definite"
    [] category = "not_comparable" -> "not_comparable"
    [] OTHER -> "possible"

VARIABLES semantic, key, applicability, values
vars == <<semantic, key, applicability, values>>

Init ==
  /\ semantic \in SEMANTIC
  /\ key \in KEY_STATE
  /\ applicability \in APPLICABILITY
  /\ values \in VALUES

Next == UNCHANGED vars
Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ Outcome(semantic, key, applicability, values) \in CATEGORIES
  /\ Actionable(Outcome(semantic, key, applicability, values))
       = (Outcome(semantic, key, applicability, values) = "conflict")

UnknownSemanticNeverConflict ==
  semantic \in ALIGNMENT_UNKNOWN
    => Outcome(semantic, key, applicability, values) # "conflict"

UnknownApplicabilityDistinct ==
  /\ semantic = "established"
  /\ key = "comparable"
  /\ values = "incompatible"
  /\ applicability \in APPLICABILITY_UNKNOWN
  => Outcome(semantic, key, applicability, values) = "applicability_uncertain"

UnresolvedKeyNotComparable ==
  key = "not_comparable"
    => Outcome(semantic, key, applicability, values) = "not_comparable"

ConflictRequiresProof ==
  Outcome(semantic, key, applicability, values) = "conflict"
    => /\ semantic = "established"
       /\ key = "comparable"
       /\ applicability = "overlap"
       /\ values = "incompatible"

AdapterExplicit ==
  LegacyAdapter("conflict") = "definite"
  /\ LegacyAdapter("alignment_uncertain") = "possible"
  /\ LegacyAdapter("applicability_uncertain") = "possible"
  /\ LegacyAdapter("not_comparable") = "not_comparable"
  /\ LegacyAdapter("no_conflict") = "possible"

TypedOutcomeSafety ==
  TypeOK /\ UnknownSemanticNeverConflict /\ UnknownApplicabilityDistinct
    /\ UnresolvedKeyNotComparable /\ ConflictRequiresProof /\ AdapterExplicit

=======================================================================
SPECIFICATION Spec

INVARIANTS
  TypeOK
  UnknownSemanticNeverConflict
  UnknownApplicabilityDistinct
  UnresolvedKeyNotComparable
  ConflictRequiresProof
  AdapterExplicit
  TypedOutcomeSafety
