---- MODULE WatermarkRetry ----
\* Watermark-hold retry liveness: every held input item is eventually processed.
\*
\* System under test: the per-item extractor record-and-hold contract
\* (src/okg/substrate/library/enrichers/extraction/base.py --
\* _record_input_failure / _complete_tracked / _keyed_complete: a failed
\* per-item LLM call is recorded as an input failure, the run comes back
\* incomplete, the publisher records last_status = 'failed:...' and holds the
\* enricher watermark, and the failed items stay pending for a full re-read
\* next cycle) COMPOSED WITH the publisher's two skip paths
\* (src/okg/substrate/publisher/publisher.py):
\*
\*   (a) the per-enricher skip-clean rung (_should_skip_clean_enricher):
\*       refuses a clean skip whenever last_status != 'success', so once the
\*       enrichers STAGE runs, a held retry forces a full re-read (modeled
\*       below as RunEnrichersStage always attempting every pending item --
\*       see SkipCleanRungRefuses).
\*   (b) the generation-level no-op-reuse fast path (the pre-enrichment
\*       probe in _cycle_chronos_native): on a source-quiescent (zero graph
\*       delta) cycle the publisher may return the previous generation with
\*       'enrichers' in skipped_stages -- the enrichers stage never runs.
\*
\* The hazard this model pins: if the fast path does NOT consult enricher
\* retry state (GuardConsultsRetryState = FALSE), an item can fail, the
\* source can go permanently quiescent, and the fast path then skips the
\* enrichers stage on every later cycle -- the held watermark is stranded
\* and the item is never retried. That is the dev defect class that shipped
\* before the P6 fix (the publisher no-op fast path bypassing the retry
\* guarantee), refuted here as a named temporal falsification:
\* WatermarkRetryStrandedCounterexample.cfg violates
\* EveryItemEventuallyProcessed under the SAME fair spec, so the guard --
\* not the fairness -- is the load-bearing difference.
\*
\* Engine guard status (per req.formal.watermark-liveness's "document what
\* the live engine lacks if the defect is still open"): the defect is NOT
\* still open. The generation-level guard landed via P6 of
\* specify-publish-commit-protocol (_enricher_retry_pending_name invalidates
\* reuse with reason 'enricher_retry_pending' whenever a wired enricher's
\* last_status != 'success'; commit 0880349b9, on dev), pinned by
\* tests/substrate/runtime/test_publish_commit_protocol.py::
\* test_p6_held_enricher_retry_invalidates_reuse and
\* ::test_p6_never_run_enricher_is_pending. GuardConsultsRetryState = TRUE
\* models exactly that guard; FALSE models the pre-P6 engine.
\*
\* THE liveness theorem (WatermarkRetry.cfg): under weak fairness on publish
\* cycles, with LLM failure transient (a bounded failure budget: an item
\* that keeps being attempted eventually succeeds), every item is eventually
\* processed -- EveryItemEventuallyProcessed. The fairness and abstraction
\* choices that weaken the theorem are named in the folder README's Model
\* Boundary section.

EXTENDS Naturals, FiniteSets

CONSTANTS
  Items,                   \* input items awaiting enrichment (small scope: 2)
  MaxTransientFailures,    \* bound on failing runs -- failure is TRANSIENT
  GuardConsultsRetryState  \* TRUE = fast path consults last_status (P6 engine)
                           \* FALSE = pre-P6 defect (named counterexample)

VARIABLES
  processed,     \* SUBSET Items : items whose typed outputs have landed
  lastStatus,    \* "never" | "success" | "failed" : enricher_progress.last_status
  pendingDelta,  \* BOOLEAN : un-consumed source writes (graph delta present)
  failuresLeft   \* 0..MaxTransientFailures : remaining transient-failure budget

vars == <<processed, lastStatus, pendingDelta, failuresLeft>>

\* Held/unprocessed inputs the held watermark stands for. Items are all
\* delivered at Init (their delivery IS the initial delta); the watermark is
\* abstracted to (lastStatus, Pending) -- no fact-id arithmetic.
Pending == Items \ processed

Init ==
  /\ processed = {}
  /\ lastStatus = "never"
  /\ pendingDelta = TRUE          \* the items' initial delivery is the delta
  /\ failuresLeft = MaxTransientFailures

\* ------------------------------------------------------------------------
\* Environment: the source is UNFAIR -- it may write again or go quiescent
\* forever. Permanent quiescence is exactly the environment of the
\* stranded-watermark hazard, so no fairness is assumed on SourceWrite.
\* ------------------------------------------------------------------------
SourceWrite ==
  /\ pendingDelta = FALSE
  /\ pendingDelta' = TRUE
  /\ UNCHANGED <<processed, lastStatus, failuresLeft>>

\* ------------------------------------------------------------------------
\* Engine: one atomic publish cycle. When the generation-level no-op-reuse
\* condition holds the fast path fires (the engine RETURNS the reuse result;
\* it is not a scheduler choice), else the enrichers stage runs.
\* ------------------------------------------------------------------------

\* The generation-level no-op-reuse condition. Source-quiescent means zero
\* graph delta since the previous generation. The guarded variant refuses
\* reuse while last_status != 'success' (P6 'enricher_retry_pending'
\* invalidation, which also covers never-run enrichers); the unguarded
\* variant skips on quiescence alone -- the defect. The reuse closure's
\* OTHER invalidation arms (catalog version, lenses, nomos pin, gate
\* inputs, native views, extractor signature) are abstracted away: this
\* model isolates the enricher-retry arm.
FastPathClean ==
  /\ pendingDelta = FALSE
  /\ (GuardConsultsRetryState => lastStatus = "success")

\* Fast path taken: 'enrichers' lands in skipped_stages; nothing changes.
\* Deliberately a self-loop (stutter): a skipped cycle moves no state, so
\* under GuardConsultsRetryState = FALSE the stranded state admits a fair
\* behavior that skips forever -- the lasso TLC reports.
SkipEnrichersStage ==
  /\ FastPathClean
  /\ UNCHANGED vars

\* The per-enricher skip-clean rung, stated for documentation: a clean skip
\* needs last_status = 'success' AND no dirty inputs. In every reachable
\* state where the enrichers stage runs, this rung REFUSES (either
\* pendingDelta is TRUE -- dirty inputs -- or lastStatus != "success", which
\* forces is_first_run and a full re-read of the held items), so
\* RunEnrichersStage always attempts every pending item. That refusal is
\* the faithful model of _should_skip_clean_enricher consulting last_status.
SkipCleanRungRefuses == pendingDelta \/ lastStatus /= "success"

\* The enrichers stage runs: every pending item's LLM call succeeds or
\* fails, chosen nondeterministically per item (failset). Failure =>
\* recorded input failure, run incomplete, watermark holds: failed items
\* stay pending and last_status = 'failed'. Success on every attempted
\* item => last_status = 'success' and the watermark advances (pending
\* empties). A failing run consumes one unit of the transient-failure
\* budget; at budget 0 every attempt succeeds -- the model form of
\* "failure is transient; a retried item eventually succeeds".
RunEnrichersStage ==
  /\ ~FastPathClean
  /\ \E failset \in SUBSET Pending :
       /\ (failset /= {}) => (failuresLeft > 0)
       /\ processed' = processed \cup (Pending \ failset)
       /\ lastStatus' = IF failset = {} THEN "success" ELSE "failed"
       /\ failuresLeft' = IF failset = {}
                          THEN failuresLeft
                          ELSE failuresLeft - 1
       /\ pendingDelta' = FALSE   \* the cycle consumed the delta

PublishCycle == SkipEnrichersStage \/ RunEnrichersStage

Next == PublishCycle \/ SourceWrite

\* Weak fairness on publish cycles ONLY: the publisher keeps cycling
\* (ingest invokes publish repeatedly), but the source may stay quiescent
\* forever and transient failures are budgeted. Under the guarded variant
\* a changing publish step stays enabled while anything is pending, so WF
\* forces the retry; under the unguarded variant the stranded state has NO
\* changing publish step (the skip is a stutter), WF is vacuous there, and
\* the eventuality fails -- the counterexample.
Spec == Init /\ [][Next]_vars /\ WF_vars(PublishCycle)

\* ------------------------------------------------------------------------
\* Obligations.
\* ------------------------------------------------------------------------
TypeOK ==
  /\ processed \in SUBSET Items
  /\ lastStatus \in {"never", "success", "failed"}
  /\ pendingDelta \in BOOLEAN
  /\ failuresLeft \in 0..MaxTransientFailures

\* Record-and-hold safety: a held (failed) status always stands for
\* concrete unprocessed inputs -- the watermark is never held over nothing.
HeldWatermarkImpliesPendingInput ==
  (lastStatus = "failed") => (Pending /= {})

\* THE liveness property: every (failed-and-held included) item is
\* eventually processed. HOLDS under GuardConsultsRetryState = TRUE
\* (WatermarkRetry.cfg); VIOLATED under FALSE by the stranded-watermark
\* lasso (WatermarkRetryStrandedCounterexample.cfg).
EveryItemEventuallyProcessed ==
  \A i \in Items : <>(i \in processed)

====
