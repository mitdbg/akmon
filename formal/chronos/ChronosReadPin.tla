---- MODULE ChronosReadPin ----
EXTENDS FiniteSets, Naturals

Generations == {"g1", "g2"}

NoGeneration == "none"

Phases == {
  "start",
  "session_open",
  "published_next",
  "graph_read",
  "operational_sql"
}

CheckpointKinds == {"okg_generation", "other"}

Deployments == {"default", "other"}

Statuses == {"unpublished", "published", "blocked"}

OperationalSqlOutcomes == {
  "unrun",
  "uses_compat",
  "rejected",
  "uses_default_zero"
}

\* When TRUE, model the int(0)-fallback authority bug that production
\* src/okg/substrate/mcp/run_sql.py deliberately guards against (it raises
\* RunSqlInvalid rather than defaulting the generation to zero). The main
\* config pins this FALSE -- the real, correct behavior -- so
\* OperationalSqlNeverDefaultsGeneration holds. The counterexample config sets
\* it TRUE so the forbidden uses_default_zero outcome becomes reachable and TLC
\* genuinely violates the invariant, proving it is load-bearing, not vacuous.
CONSTANT BuggyDefaultZeroFallback

\* When TRUE, model a session whose graph read follows the LATEST generation
\* instead of its frozen checkout pin -- the pin-freeze regression the headline
\* invariants guard against. The main config pins this FALSE (the correct
\* behavior: a session stays on sessionPin across a publish), so
\* GraphReadUsesPinnedGeneration and NoMixedGenerationObservation hold. The
\* follows-latest counterexample config sets it TRUE so a read after a publish
\* observes latest # sessionPin, and TLC refutes GraphReadUsesPinnedGeneration by
\* name -- proving the pin-freeze headline is load-bearing, not structurally true.
CONSTANT BuggySessionFollowsLatest

VARIABLES
  phase,
  published,
  latest,
  sessionPin,
  latestAtOpen,
  graphReadGeneration,
  observedGenerations,
  checkpointCompatAvailable,
  sessionCompatAvailable,
  operationalSqlOutcome,
  checkpointKind,
  checkpointDeployment,
  checkpointStatus

vars == <<
  phase,
  published,
  latest,
  sessionPin,
  latestAtOpen,
  graphReadGeneration,
  observedGenerations,
  checkpointCompatAvailable,
  sessionCompatAvailable,
  operationalSqlOutcome,
  checkpointKind,
  checkpointDeployment,
  checkpointStatus
>>

Init ==
  /\ phase = "start"
  /\ published = {"g1"}
  /\ latest = "g1"
  /\ sessionPin = NoGeneration
  /\ latestAtOpen = NoGeneration
  /\ graphReadGeneration = NoGeneration
  /\ observedGenerations = {}
  /\ checkpointCompatAvailable \in [Generations -> BOOLEAN]
  /\ sessionCompatAvailable = FALSE
  /\ operationalSqlOutcome = "unrun"
  /\ checkpointKind \in [Generations -> CheckpointKinds]
  /\ checkpointKind["g1"] = "okg_generation"
  /\ checkpointDeployment \in [Generations -> Deployments]
  /\ checkpointDeployment["g1"] = "default"
  /\ checkpointStatus \in [Generations -> Statuses]
  /\ checkpointStatus["g1"] = "published"
  /\ checkpointStatus["g2"] # "published"

PublishNext ==
  /\ "g2" \notin published
  /\ checkpointKind["g2"] = "okg_generation"
  /\ checkpointDeployment["g2"] = "default"
  /\ checkpointStatus["g2"] # "blocked"
  /\ phase \in {"start", "session_open"}
  /\ published' = published \cup {"g2"}
  /\ latest' = "g2"
  /\ checkpointStatus' = [checkpointStatus EXCEPT !["g2"] = "published"]
  /\ phase' = "published_next"
  /\ UNCHANGED <<
       sessionPin,
       latestAtOpen,
       graphReadGeneration,
       observedGenerations,
       checkpointCompatAvailable,
       sessionCompatAvailable,
       operationalSqlOutcome,
       checkpointKind,
       checkpointDeployment
     >>

OpenSession ==
  /\ sessionPin = NoGeneration
  /\ latest \in published
  /\ checkpointKind[latest] = "okg_generation"
  /\ checkpointDeployment[latest] = "default"
  /\ checkpointStatus[latest] = "published"
  /\ sessionPin' = latest
  /\ sessionCompatAvailable' = checkpointCompatAvailable[latest]
  /\ latestAtOpen' = latest
  /\ phase' = "session_open"
  /\ UNCHANGED <<
       published,
       latest,
       graphReadGeneration,
       observedGenerations,
       checkpointCompatAvailable,
       operationalSqlOutcome,
       checkpointKind,
       checkpointDeployment,
       checkpointStatus
     >>

GraphRead ==
  /\ sessionPin # NoGeneration
  /\ phase \in {"session_open", "published_next", "operational_sql"}
  /\ LET readGen == IF BuggySessionFollowsLatest THEN latest ELSE sessionPin IN
       /\ graphReadGeneration' = readGen
       /\ observedGenerations' = observedGenerations \cup {readGen}
  /\ phase' = "graph_read"
  /\ UNCHANGED <<
       published,
       latest,
       sessionPin,
       latestAtOpen,
       checkpointCompatAvailable,
       sessionCompatAvailable,
       operationalSqlOutcome,
       checkpointKind,
       checkpointDeployment,
       checkpointStatus
     >>

OperationalSql ==
  /\ sessionPin # NoGeneration
  /\ phase \in {"session_open", "published_next", "graph_read"}
  /\ operationalSqlOutcome' =
       IF sessionCompatAvailable THEN "uses_compat"
       ELSE IF BuggyDefaultZeroFallback THEN "uses_default_zero"
       ELSE "rejected"
  /\ phase' = "operational_sql"
  /\ UNCHANGED <<
       published,
       latest,
       sessionPin,
       latestAtOpen,
       graphReadGeneration,
       observedGenerations,
       checkpointCompatAvailable,
       sessionCompatAvailable,
       checkpointKind,
       checkpointDeployment,
       checkpointStatus
     >>

Next == PublishNext \/ OpenSession \/ GraphRead \/ OperationalSql

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ phase \in Phases
  /\ published \subseteq Generations
  /\ latest \in published
  /\ sessionPin \in Generations \cup {NoGeneration}
  /\ latestAtOpen \in Generations \cup {NoGeneration}
  /\ graphReadGeneration \in Generations \cup {NoGeneration}
  /\ observedGenerations \subseteq Generations
  /\ checkpointCompatAvailable \in [Generations -> BOOLEAN]
  /\ sessionCompatAvailable \in BOOLEAN
  /\ operationalSqlOutcome \in OperationalSqlOutcomes
  /\ checkpointKind \in [Generations -> CheckpointKinds]
  /\ checkpointDeployment \in [Generations -> Deployments]
  /\ checkpointStatus \in [Generations -> Statuses]

PinnedOnlyPublishedOkgGeneration ==
  sessionPin # NoGeneration =>
    /\ sessionPin \in published
    /\ checkpointKind[sessionPin] = "okg_generation"
    /\ checkpointDeployment[sessionPin] = "default"
    /\ checkpointStatus[sessionPin] = "published"

SessionPinFrozenAcrossPublish ==
  latestAtOpen # NoGeneration => sessionPin = latestAtOpen

LatestAdvanceDoesNotMutateOpenSession ==
  /\ latestAtOpen # NoGeneration
  /\ latest # latestAtOpen
  => sessionPin = latestAtOpen

GraphReadUsesPinnedGeneration ==
  graphReadGeneration # NoGeneration =>
    /\ sessionPin # NoGeneration
    /\ graphReadGeneration = sessionPin

NoMixedGenerationObservation ==
  Cardinality(observedGenerations) <= 1

SessionPreservesCheckpointCompat ==
  sessionPin # NoGeneration =>
    sessionCompatAvailable = checkpointCompatAvailable[sessionPin]

OperationalSqlNeverDefaultsGeneration ==
  operationalSqlOutcome # "uses_default_zero"

OperationalSqlRequiresSessionCompatOrRejects ==
  operationalSqlOutcome # "unrun" =>
    IF sessionCompatAvailable
    THEN operationalSqlOutcome = "uses_compat"
    ELSE operationalSqlOutcome = "rejected"

OperationalSqlUsesOnlyPreservedCompat ==
  operationalSqlOutcome = "uses_compat" =>
    /\ sessionPin # NoGeneration
    /\ sessionCompatAvailable
    /\ checkpointCompatAvailable[sessionPin]

OperationalSqlRejectsMissingSessionCompat ==
  operationalSqlOutcome = "rejected" => ~sessionCompatAvailable

NoUnpinnedRead ==
  \/ /\ graphReadGeneration = NoGeneration
     /\ operationalSqlOutcome = "unrun"
  \/ sessionPin # NoGeneration

ChronosReadPinSafety ==
  /\ TypeOK
  /\ PinnedOnlyPublishedOkgGeneration
  /\ SessionPinFrozenAcrossPublish
  /\ LatestAdvanceDoesNotMutateOpenSession
  /\ GraphReadUsesPinnedGeneration
  /\ NoMixedGenerationObservation
  /\ SessionPreservesCheckpointCompat
  /\ OperationalSqlNeverDefaultsGeneration
  /\ OperationalSqlRequiresSessionCompatOrRejects
  /\ OperationalSqlUsesOnlyPreservedCompat
  /\ OperationalSqlRejectsMissingSessionCompat
  /\ NoUnpinnedRead

====
