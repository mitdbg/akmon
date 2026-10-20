---- MODULE WorkflowAuthorityCounterexample ----
EXTENDS WorkflowAuthority

\* Negative-check module for the proof gate. It witnesses that the headline
\* CurrentAuthority-vs-RuntimeBlockers states are NON-VACUOUSLY reachable:
\*
\*   BrokenAuthorityNeverAccepted asserts no state ever clears every blocker
\*   (accepted = TRUE / RuntimeBlockers = {}). WorkflowAuthority DOES admit a
\*   fully-fresh, fully-authorized surface, so TLC must produce a
\*   counterexample. The violation is the PROOF that the antecedents of
\*   FreshAuthorityCanAuthorize and CurrentAuthority are reachable -- i.e. the
\*   acceptance side of the contract is not vacuously unsatisfiable.
BrokenAuthorityNeverAccepted == ~accepted

\* Companion witness for the blocked side: assert that a real runtime blocker
\* can never arise. WorkflowAuthority DOES admit stale/unpinned surfaces that
\* populate RuntimeBlockers, so TLC must refute this too. The violation proves
\* the No*Authority guards have a reachable, non-empty blocker antecedent.
BrokenBlockersNeverRaised == blockers = {}

====
