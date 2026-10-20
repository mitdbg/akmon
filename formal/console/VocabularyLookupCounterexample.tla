---- MODULE VocabularyLookupCounterexample ----
EXTENDS VocabularyLookup

(***************************************************************************
  Negative-check module for the proof gate. It intentionally asserts
  that the profile default always wins — which is FALSE under the
  two-tier lookup contract (deployment override beats profile default).
  TLC must produce a counterexample, confirming the model is not
  vacuously satisfied and that the lookup chain is actually being
  checked.

  Mirrors formal/admission/AdmissionCounterexample.tla.
 ***************************************************************************)

BrokenProfileAlwaysWins ==
  /\ profileVocab[requestedTerm] # ERROR
  => result = profileVocab[requestedTerm]

====
