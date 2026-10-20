---- MODULE EnricherBacklogDrain ----
\* Successful-watermark backlog liveness across the publisher's two no-op
\* generation-reuse decisions.
\*
\* The concrete publisher snapshots max(node_facts.fact_id) once at the start
\* of the enricher phase. Every successful enricher advances only to that
\* phase-start frontier. Therefore a node fact emitted by an earlier enricher
\* in the same phase is intentionally invisible to a downstream enricher until
\* a later publish cycle. This is safe only if both generation-reuse decisions
\* consult the resulting watermark lag:
\*
\*   * the pre-enrichment fast path must not skip the whole phase while a
\*     successful downstream enricher has matching facts past its watermark;
\*   * the post-enrichment fast path must not reuse after a same-cycle,
\*     graph-idempotent upstream emission creates that lag.
\*
\* The finite scope is an acyclic two-stage derivation: one upstream stage can
\* emit exactly one node fact, and one downstream stage consumes it and makes
\* its graph output visible. Source input is finite and permanently quiescent
\* after the initial cycle. Weak fairness is assumed only for publish cycles.
\* These are the load-bearing limits of the liveness theorem; cyclic or
\* unbounded derivation is deliberately outside the claim.

EXTENDS Naturals

CONSTANTS
  PreReuseConsultsWatermarkLag,
  PostReuseConsultsWatermarkLag

VARIABLES
  factFrontier,             \* max node fact id; bounded to 0..1
  downstreamWatermark,      \* successful downstream phase-start snapshot
  upstreamPending,          \* one finite upstream input remains to emit
  sourceDelta,              \* current segment/source delta forces the first run
  downstreamOutputVisible,  \* downstream-derived graph output was checkpointed
  lastAction                \* init | checkpoint | pre_reuse | post_reuse

vars ==
  <<factFrontier, downstreamWatermark, upstreamPending, sourceDelta,
    downstreamOutputVisible, lastAction>>

EligibleBacklog == downstreamWatermark < factFrontier

Init ==
  /\ factFrontier = 0
  /\ downstreamWatermark = 0
  /\ upstreamPending = TRUE
  /\ sourceDelta = TRUE
  /\ downstreamOutputVisible = FALSE
  /\ lastAction = "init"

\* The pre-enrichment reuse predicate. With the guard enabled, source
\* quiescence is insufficient: the successful downstream watermark must also
\* have caught up to every eligible node fact.
PreReuseClean ==
  /\ ~sourceDelta
  /\ (PreReuseConsultsWatermarkLag => ~EligibleBacklog)

PreReuse ==
  /\ PreReuseClean
  /\ lastAction' = "pre_reuse"
  /\ UNCHANGED
       <<factFrontier, downstreamWatermark, upstreamPending, sourceDelta,
         downstreamOutputVisible>>

\* One enricher phase. `phaseStart` is the concrete phase-wide fact watermark
\* captured before ANY enricher runs. The downstream stage can process facts
\* only through that frontier. The upstream stage may then append one fact,
\* leaving downstreamWatermark' behind factFrontier'.
\*
\* `upstreamChangesGraph` represents the two relevant runtime outcomes:
\* TRUE  -> the upstream node changes the graph and a checkpoint is mandatory;
\* FALSE -> the emitted fact is graph-idempotent, so only the FINAL backlog
\*          guard prevents post-enrichment generation reuse.
RunEnricherPhase(upstreamChangesGraph) ==
  /\ ~PreReuseClean
  /\ LET phaseStart == factFrontier
         upstreamEmits == upstreamPending
         downstreamProcesses == downstreamWatermark < phaseStart
         nextFactFrontier ==
           IF upstreamEmits THEN factFrontier + 1 ELSE factFrontier
         nextWatermark == phaseStart
         postBacklog == nextWatermark < nextFactFrontier
         graphChanged ==
           downstreamProcesses \/ (upstreamEmits /\ upstreamChangesGraph)
         postReuse ==
           ~graphChanged
           /\ (~PostReuseConsultsWatermarkLag \/ ~postBacklog)
     IN
       /\ factFrontier' = nextFactFrontier
       /\ downstreamWatermark' = nextWatermark
       /\ upstreamPending' = FALSE
       /\ sourceDelta' = FALSE
       /\ downstreamOutputVisible' =
            (downstreamOutputVisible \/ downstreamProcesses)
       /\ lastAction' = IF postReuse THEN "post_reuse" ELSE "checkpoint"

PublishCycle ==
  PreReuse
  \/ \E upstreamChangesGraph \in BOOLEAN :
       RunEnricherPhase(upstreamChangesGraph)

Next == PublishCycle

\* The publisher keeps cycling. There is intentionally no source-write action:
\* permanent source quiescence is the environment in which the defect strands
\* the downstream stage.
Spec == Init /\ [][Next]_vars /\ WF_vars(PublishCycle)

TypeOK ==
  /\ factFrontier \in 0..1
  /\ downstreamWatermark \in 0..1
  /\ upstreamPending \in BOOLEAN
  /\ sourceDelta \in BOOLEAN
  /\ downstreamOutputVisible \in BOOLEAN
  /\ lastAction \in {"init", "checkpoint", "pre_reuse", "post_reuse"}

WatermarkWithinFrontier == downstreamWatermark <= factFrontier

\* Both reuse sites are sound: whenever either one returns, no eligible
\* successful-watermark backlog remains.
ReuseOnlyWhenNoEligibleBacklog ==
  (lastAction \in {"pre_reuse", "post_reuse"}) => ~EligibleBacklog

\* Under finite acyclic input and fair publish cycles, the downstream stage
\* eventually consumes the upstream fact and publishes its graph output.
EveryEligibleInputEventuallyProcessed ==
  <>(
    ~upstreamPending
    /\ downstreamWatermark = factFrontier
    /\ downstreamOutputVisible
  )

====
