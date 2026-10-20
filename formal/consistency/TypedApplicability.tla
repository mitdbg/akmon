---------------------- MODULE TypedApplicability ----------------------
EXTENDS Naturals, FiniteSets, Sequences, TLC

(* Finite abstraction of the lower/upper-bound algebra used by the runtime.
   X/Y stand for deployment-owned discrete values or half-open time buckets. *)
VALUES == {"x", "y"}
SHAPES == {"unknown", "unbounded", "known_x", "known_y",
           "partial_x", "partial_y"}
HEADINGS == {"absent", "unknown", "empty", "root", "child_a", "child_b"}
V1HEADINGS == HEADINGS \ {"absent"}

Lo(s) ==
  CASE s = "unknown"   -> {}
    [] s = "unbounded" -> VALUES
    [] s = "known_x"   -> {"x"}
    [] s = "known_y"   -> {"y"}
    [] s = "partial_x" -> {"x"}
    [] OTHER            -> {"y"}

Hi(s) ==
  CASE s = "unknown"   -> VALUES
    [] s = "unbounded" -> VALUES
    [] s = "known_x"   -> {"x"}
    [] s = "known_y"   -> {"y"}
    [] s = "partial_x" -> VALUES
    [] OTHER            -> VALUES

Intersects(a, b) == a \cap b # {}

(* The hierarchy-aware heading axis exactly matches the legacy relation:
   known-empty overlaps only known-empty; root and descendants overlap;
   siblings are disjoint; unknown remains possible-only. *)
HeadingHiOverlap(a, b) ==
  \/ /\ a = "absent" /\ b = "absent"
  \/ a = "unknown"
  \/ b = "unknown"
  \/ a = "absent"
  \/ b = "absent"
  \/ a = b
  \/ /\ a = "root" /\ b \in {"child_a", "child_b"}
  \/ /\ b = "root" /\ a \in {"child_a", "child_b"}

HeadingLoOverlap(a, b) ==
  /\ \/ /\ a = "absent" /\ b = "absent"
     \/ /\ a # "absent" /\ b # "absent"
  /\ a # "unknown"
  /\ b # "unknown"
  /\ \/ a = b
     \/ /\ a = "root" /\ b \in {"child_a", "child_b"}
     \/ /\ b = "root" /\ a \in {"child_a", "child_b"}

LegacyHeadingRelation(a, b) ==
  IF a = "unknown" \/ b = "unknown" THEN "unknown"
  ELSE IF HeadingLoOverlap(a, b) THEN "overlap"
  ELSE "disjoint"

AdapterHeadingRelation(a, b) ==
  IF ~HeadingHiOverlap(a, b) THEN "disjoint"
  ELSE IF HeadingLoOverlap(a, b) THEN "overlap"
  ELSE "unknown"

VARIABLES a1, b1, a2, b2, headingA, headingB
vars == <<a1, b1, a2, b2, headingA, headingB>>

Init ==
  /\ a1 \in SHAPES
  /\ b1 \in SHAPES
  /\ a2 \in SHAPES
  /\ b2 \in SHAPES
  /\ headingA \in HEADINGS
  /\ headingB \in HEADINGS

Next == UNCHANGED vars
Spec == Init /\ [][Next]_vars

LoOverlap ==
  /\ Intersects(Lo(a1), Lo(b1))
  /\ Intersects(Lo(a2), Lo(b2))
  /\ HeadingLoOverlap(headingA, headingB)
HiOverlap ==
  /\ Intersects(Hi(a1), Hi(b1))
  /\ Intersects(Hi(a2), Hi(b2))
  /\ HeadingHiOverlap(headingA, headingB)
Outcome == IF ~HiOverlap THEN "none" ELSE IF LoOverlap THEN "definite" ELSE "possible"

TypeOK ==
  /\ a1 \in SHAPES /\ b1 \in SHAPES
  /\ a2 \in SHAPES /\ b2 \in SHAPES
  /\ headingA \in HEADINGS /\ headingB \in HEADINGS
  /\ Outcome \in {"none", "definite", "possible"}

UnknownNeverDefinite ==
  (a1 = "unknown" \/ b1 = "unknown" \/
   a2 = "unknown" \/ b2 = "unknown" \/
   headingA = "unknown" \/ headingB = "unknown") => Outcome # "definite"

UpperDisjointNeverFinds == ~HiOverlap => Outcome = "none"

LowerDisjointUpperOverlapIsPossible ==
  (~LoOverlap /\ HiOverlap) => Outcome = "possible"

UnboundedKnownIsDefiniteCapable ==
  /\ a1 = "unbounded" /\ b1 = "known_x"
  /\ a2 = "unbounded" /\ b2 = "known_x"
  /\ headingA = "absent" /\ headingB = "absent"
  => Outcome = "definite"

ConjunctiveAcrossAxes ==
  (~Intersects(Hi(a1), Hi(b1)) \/ ~Intersects(Hi(a2), Hi(b2)) \/
   ~HeadingHiOverlap(headingA, headingB))
  => Outcome = "none"

V1AdapterEquivalent ==
  (headingA \in V1HEADINGS /\ headingB \in V1HEADINGS) =>
    AdapterHeadingRelation(headingA, headingB) =
      LegacyHeadingRelation(headingA, headingB)

UnboundedWitnessExists ==
  /\ "unbounded" \in SHAPES
  /\ "known_x" \in SHAPES
  /\ "absent" \in HEADINGS
  /\ Intersects(Lo("unbounded"), Lo("known_x"))
  /\ Intersects(Hi("unbounded"), Hi("known_x"))

TypedApplicabilitySafety ==
  /\ TypeOK
  /\ UnknownNeverDefinite
  /\ UpperDisjointNeverFinds
  /\ LowerDisjointUpperOverlapIsPossible
  /\ UnboundedKnownIsDefiniteCapable
  /\ ConjunctiveAcrossAxes
  /\ V1AdapterEquivalent
  /\ UnboundedWitnessExists

=======================================================================
