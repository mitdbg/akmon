---- MODULE MergeBackPartitionAtomicityCounterexample ----
EXTENDS MergeBackPartitionAtomicity

\* Non-vacuity witness for the partition-atomicity safety invariants.
\*
\* StageAndQuarantineCommitAtomically and CrashLeavesNeitherPartialWrite are
\* implications guarded by phase = "committed" / phase = "aborted". They would
\* hold vacuously if neither terminal commit phase were ever reachable -- i.e.
\* if the merge-back partition never actually committed atomically and never
\* actually aborted on a fault during commit. This module asserts, as an
\* invariant, that the partition NEVER reaches a committed-or-aborted phase.
\* TLC must refute it, exhibiting:
\*   - a clean atomic commit (phase = "committed"), and
\*   - a fault-during-commit abort (phase = "aborted"),
\* proving the atomicity antecedents are live, not vacuous.
BrokenPartitionNeverCommits ==
  phase \notin {"committed", "aborted"}

\* Non-vacuity witness for NoDurableWritesBeforeCommit and the crash
\* interleaving: a mid-transaction state (staging written into the txn
\* buffer, quarantine not yet, nothing durable) must be genuinely reachable
\* — otherwise the atomicity invariants are proven over a model with no
\* partial state, which is exactly the disease the wave-3 rewrite removed.
\* Asserting the txn buffer never holds rows before COMMIT must be refuted.
BrokenNoMidTransactionState ==
  ~(phase = "staging_written" /\ txnStagingRows # {})

====
