---- MODULE MutationSafetyCounterexample ----
EXTENDS MutationSafety

\* Negative-check module for the proof gate. It witnesses that the states the
\* MutationSafety invariants guard are NON-VACUOUSLY reachable.
\*
\* BrokenPlanNeverApplies asserts a plan effect never lands (dependencyCount
\* stays 0). MutationSafety DOES let a plan writer apply the effect exactly
\* once, driving dependencyCount to 1, so TLC must produce a counterexample.
\* The violation proves PlanEffectAtMostOnce (dependencyCount <= 1) guards a
\* state where the count is actually positive -- the bound is exercised, not
\* vacuously satisfied by a count that is always 0.
BrokenPlanNeverApplies == dependencyCount = 0

\* BrokenNoMutationCompletes asserts no mutation ever completes (completed
\* stays empty). MutationSafety DOES let a mutation writer save and complete,
\* so TLC must refute this. The violation proves NoLostCompletedMutation
\* (completed \subseteq file) guards a reachable state with a non-empty
\* completed set -- the durability claim has real content.
BrokenNoMutationCompletes == completed = {}

====
