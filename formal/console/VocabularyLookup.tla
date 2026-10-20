---- MODULE VocabularyLookup ----
(***************************************************************************
  Two-tier vocabulary lookup for the Console framework.

  Substrate operator terms (envelope, producer, branch, gate, narrowing,
  promote, quarantine, fsm_state) must never appear in any Console chrome
  string. Each Console manifest declares a per-deployment vocabulary map.
  Each substrate ontology profile declares a default vocabulary map. The
  lookup chain at any chrome render is:

      deployment override  ->  profile default  ->  ERROR

  This model proves:

    LookupIsDeterministic
        For any (profileVocab, deploymentVocab, term), the lookup result
        is uniquely determined by the lookup function. (Determinism
        invariant; trivially true if the function is total, but stated
        explicitly so that a future implementation that adds non-
        determinism would fail refinement.)

    DeploymentOverridesProfile
        If both maps define a term, the deployment override wins.

    NoSilentFallbackToSubstrateTerm
        If neither map defines a term, the result is ERROR, not the
        substrate term itself. There is no implicit identity fallback.

  The model deliberately ignores the actual operator string content
  (which is editorial), and represents resolved values as an opaque
  ResolvedValues set plus an explicit ERROR sentinel. The substrate term
  itself is NEVER a member of ResolvedValues; this captures the no-silent-
  fallback invariant by construction at the type level, and the property
  catches an implementation that would route a substrate term into the
  chrome through a code path that bypasses the lookup function.
 ***************************************************************************)
EXTENDS FiniteSets

CONSTANTS
  Terms,           \* The set of substrate term names (e.g. {envelope, producer, ...}).
  ResolvedValues,  \* The set of operator-facing strings the chrome may render.
  ERROR            \* The sentinel returned when neither map defines a term.

(***************************************************************************
  The substrate term itself is never a legal resolved value. This is a
  modelling discipline: a term cannot "resolve to itself" in the chrome.
 ***************************************************************************)
ASSUME ResolvedValues \cap Terms = {}
ASSUME ERROR \notin ResolvedValues
ASSUME ERROR \notin Terms

(***************************************************************************
  A vocabulary map associates a subset of Terms with values from
  ResolvedValues. We model a partial function as a set of {term, value}
  records, then index by term.
 ***************************************************************************)
VocabMap == [Terms -> ResolvedValues \cup {ERROR}]

(***************************************************************************
  Lookup function: deployment override beats profile default; ERROR if
  neither is defined.
 ***************************************************************************)
Lookup(profileVocab, deploymentVocab, t) ==
  IF deploymentVocab[t] # ERROR
    THEN deploymentVocab[t]
    ELSE IF profileVocab[t] # ERROR
      THEN profileVocab[t]
      ELSE ERROR

(***************************************************************************
  State: one resolution at a time. The model is small and finite; TLC
  enumerates all (profileVocab, deploymentVocab, requestedTerm) tuples.
 ***************************************************************************)
VARIABLES
  profileVocab,
  deploymentVocab,
  requestedTerm,
  result

vars == << profileVocab, deploymentVocab, requestedTerm, result >>

Init ==
  /\ profileVocab    \in [Terms -> ResolvedValues \cup {ERROR}]
  /\ deploymentVocab \in [Terms -> ResolvedValues \cup {ERROR}]
  /\ requestedTerm   \in Terms
  /\ result          = Lookup(profileVocab, deploymentVocab, requestedTerm)

(***************************************************************************
  The lookup is the only transition. We change which term is requested
  and recompute. Vocab maps are inputs (held fixed across a render pass)
  but TLC explores all configurations because Init picks them
  nondeterministically.
 ***************************************************************************)
RequestAnotherTerm ==
  /\ \E t \in Terms:
       /\ requestedTerm' = t
       /\ result' = Lookup(profileVocab, deploymentVocab, t)
  /\ UNCHANGED << profileVocab, deploymentVocab >>

Next == RequestAnotherTerm

Spec == Init /\ [][Next]_vars

(***************************************************************************
  Type invariant.
 ***************************************************************************)
TypeOK ==
  /\ profileVocab    \in [Terms -> ResolvedValues \cup {ERROR}]
  /\ deploymentVocab \in [Terms -> ResolvedValues \cup {ERROR}]
  /\ requestedTerm   \in Terms
  /\ result          \in ResolvedValues \cup {ERROR}

(***************************************************************************
  LookupIsDeterministic
      At every reachable state, the recorded result equals Lookup applied
      to the current inputs. A non-deterministic implementation that
      occasionally returned a different value for the same inputs would
      violate this invariant.
 ***************************************************************************)
LookupIsDeterministic ==
  result = Lookup(profileVocab, deploymentVocab, requestedTerm)

(***************************************************************************
  DeploymentOverridesProfile
      Whenever the deployment map defines a value for the current term,
      that value is the lookup result regardless of what the profile map
      says. A regression where the profile beat the deployment override
      would fail this invariant.
 ***************************************************************************)
DeploymentOverridesProfile ==
  deploymentVocab[requestedTerm] # ERROR =>
    result = deploymentVocab[requestedTerm]

(***************************************************************************
  NoSilentFallbackToSubstrateTerm
      If neither map defines the term, the result is ERROR, never the
      substrate term itself, and never any value from ResolvedValues that
      happens to coincide with the term. The ASSUME above plus this
      invariant pin the no-silent-fallback property at both the type
      level and the runtime level.
 ***************************************************************************)
NoSilentFallbackToSubstrateTerm ==
  /\ (deploymentVocab[requestedTerm] = ERROR /\ profileVocab[requestedTerm] = ERROR) =>
       result = ERROR
  /\ result \notin Terms
====
