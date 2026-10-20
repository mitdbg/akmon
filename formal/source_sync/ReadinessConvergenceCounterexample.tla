---- MODULE ReadinessConvergenceCounterexample ----
(***************************************************************************
  Intentional negative coverage for ReadinessConvergence.

  These deliberately false invariants prove the positive readiness model has
  reachable ready and stuck witness states. Each cfg checks one invariant so
  TLC must find both counterexamples before the checker emits proof.
 ***************************************************************************)
EXTENDS ReadinessConvergence

ReadyWitnessNeverOccurs ==
  ~ReadyWitness

StuckWitnessNeverOccurs ==
  ~StuckWitness

McpBlockedWitnessNeverOccurs ==
  ~McpBlockedWitness

====
