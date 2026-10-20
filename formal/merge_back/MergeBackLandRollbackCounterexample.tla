---- MODULE MergeBackLandRollbackCounterexample ----
EXTENDS MergeBackLandRollback

\* Non-vacuity witness for the blocked-LAND rollback safety invariants.
\*
\* BlockedLandRestoresDefaultLiveSet and BlockedGenerationNotLive are
\* implications guarded by phase = "rolled_back". They would hold vacuously if
\* a blocked LAND could never actually roll back -- if the system could commit
\* a LAND ahead of publish but never recover when that publish is blocked,
\* leaving the blocked generation's rows live. This module asserts, as an
\* invariant, that the LAND NEVER reaches the rolled_back phase. TLC must
\* refute it, exhibiting a blocked LAND that does roll back (defaultLiveSet
\* restored to preLandLiveSet, blockedGenerationLive cleared), proving the
\* rollback antecedent is live, not vacuous.
BrokenLandNeverRollsBack ==
  phase # "rolled_back"

\* Non-vacuity witness for RollbackIncompleteNeverSilent: the delete-failure
\* branch must be genuinely reachable — a rollback that ends with the blocked
\* generation still live and the incompleteness reported. Asserting the
\* incomplete report never occurs must be refuted by TLC, proving the
\* no-silent-fallback invariant guards a live hazard state rather than an
\* unreachable one.
BrokenRollbackNeverIncomplete ==
  ~rollbackReportedIncomplete

====
