---- MODULE OfflineReplayCounterexample ----
(***************************************************************************
  Intentional negative coverage for OfflineReplay.

  The base OfflineReplay model must admit a behavior where a queued write
  replays. This deliberately false invariant proves the replay portion of
  the state space is reachable; if TLC ever stops finding this
  counterexample, the positive OfflineReplay invariants are likely vacuous.
 ***************************************************************************)
EXTENDS OfflineReplay

BrokenReplayNeverHappens ==
  replayed = << >>

====
