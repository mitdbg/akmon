---- MODULE MutationSafety ----
EXTENDS Naturals, FiniteSets

CONSTANTS
  MutationWriters,
  PlanWriters,
  NoOwner

MutationPhases == {"idle", "locked", "read", "mutated", "saved", "done"}
PlanPhases == {"idle", "locked", "read", "saved", "done"}
Owners == MutationWriters \cup PlanWriters \cup {NoOwner}

VARIABLES
  owner,
  file,
  completed,
  mutationPhase,
  mutationSnapshot,
  mutationLocal,
  planApplied,
  dependencyCount,
  planPhase,
  planSawApplied

vars == <<
  owner,
  file,
  completed,
  mutationPhase,
  mutationSnapshot,
  mutationLocal,
  planApplied,
  dependencyCount,
  planPhase,
  planSawApplied
>>

Init ==
  /\ owner = NoOwner
  /\ file = {}
  /\ completed = {}
  /\ mutationPhase = [w \in MutationWriters |-> "idle"]
  /\ mutationSnapshot = [w \in MutationWriters |-> {}]
  /\ mutationLocal = [w \in MutationWriters |-> {}]
  /\ planApplied = FALSE
  /\ dependencyCount = 0
  /\ planPhase = [w \in PlanWriters |-> "idle"]
  /\ planSawApplied = [w \in PlanWriters |-> FALSE]

AcquireMutation(w) ==
  /\ w \in MutationWriters
  /\ owner = NoOwner
  /\ mutationPhase[w] = "idle"
  /\ owner' = w
  /\ mutationPhase' = [mutationPhase EXCEPT ![w] = "locked"]
  /\ UNCHANGED <<
       file,
       completed,
       mutationSnapshot,
       mutationLocal,
       planApplied,
       dependencyCount,
       planPhase,
       planSawApplied
     >>

ReadMutation(w) ==
  /\ w \in MutationWriters
  /\ owner = w
  /\ mutationPhase[w] = "locked"
  /\ mutationSnapshot' = [mutationSnapshot EXCEPT ![w] = file]
  /\ mutationPhase' = [mutationPhase EXCEPT ![w] = "read"]
  /\ UNCHANGED <<
       owner,
       file,
       completed,
       mutationLocal,
       planApplied,
       dependencyCount,
       planPhase,
       planSawApplied
     >>

MutateManifest(w) ==
  /\ w \in MutationWriters
  /\ owner = w
  /\ mutationPhase[w] = "read"
  /\ mutationLocal' = [
       mutationLocal EXCEPT ![w] = mutationSnapshot[w] \cup {w}
     ]
  /\ mutationPhase' = [mutationPhase EXCEPT ![w] = "mutated"]
  /\ UNCHANGED <<
       owner,
       file,
       completed,
       mutationSnapshot,
       planApplied,
       dependencyCount,
       planPhase,
       planSawApplied
     >>

SaveMutation(w) ==
  /\ w \in MutationWriters
  /\ owner = w
  /\ mutationPhase[w] = "mutated"
  /\ file' = mutationLocal[w]
  /\ completed' = completed \cup {w}
  /\ mutationPhase' = [mutationPhase EXCEPT ![w] = "saved"]
  /\ UNCHANGED <<
       owner,
       mutationSnapshot,
       mutationLocal,
       planApplied,
       dependencyCount,
       planPhase,
       planSawApplied
     >>

ReleaseMutation(w) ==
  /\ w \in MutationWriters
  /\ owner = w
  /\ mutationPhase[w] = "saved"
  /\ owner' = NoOwner
  /\ mutationPhase' = [mutationPhase EXCEPT ![w] = "done"]
  /\ UNCHANGED <<
       file,
       completed,
       mutationSnapshot,
       mutationLocal,
       planApplied,
       dependencyCount,
       planPhase,
       planSawApplied
     >>

AcquirePlan(w) ==
  /\ w \in PlanWriters
  /\ owner = NoOwner
  /\ planPhase[w] = "idle"
  /\ owner' = w
  /\ planPhase' = [planPhase EXCEPT ![w] = "locked"]
  /\ UNCHANGED <<
       file,
       completed,
       mutationPhase,
       mutationSnapshot,
       mutationLocal,
       planApplied,
       dependencyCount,
       planSawApplied
     >>

ReadPlan(w) ==
  /\ w \in PlanWriters
  /\ owner = w
  /\ planPhase[w] = "locked"
  /\ planSawApplied' = [planSawApplied EXCEPT ![w] = planApplied]
  /\ planPhase' = [planPhase EXCEPT ![w] = "read"]
  /\ UNCHANGED <<
       owner,
       file,
       completed,
       mutationPhase,
       mutationSnapshot,
       mutationLocal,
       planApplied,
       dependencyCount
     >>

SavePlan(w) ==
  /\ w \in PlanWriters
  /\ owner = w
  /\ planPhase[w] = "read"
  /\ IF planSawApplied[w]
     THEN
       /\ planApplied' = planApplied
       /\ dependencyCount' = dependencyCount
     ELSE
       /\ planApplied' = TRUE
       /\ dependencyCount' = dependencyCount + 1
  /\ planPhase' = [planPhase EXCEPT ![w] = "saved"]
  /\ UNCHANGED <<
       owner,
       file,
       completed,
       mutationPhase,
       mutationSnapshot,
       mutationLocal,
       planSawApplied
     >>

ReleasePlan(w) ==
  /\ w \in PlanWriters
  /\ owner = w
  /\ planPhase[w] = "saved"
  /\ owner' = NoOwner
  /\ planPhase' = [planPhase EXCEPT ![w] = "done"]
  /\ UNCHANGED <<
       file,
       completed,
       mutationPhase,
       mutationSnapshot,
       mutationLocal,
       planApplied,
       dependencyCount,
       planSawApplied
     >>

Next ==
  \/ \E w \in MutationWriters:
       AcquireMutation(w)
       \/ ReadMutation(w)
       \/ MutateManifest(w)
       \/ SaveMutation(w)
       \/ ReleaseMutation(w)
  \/ \E w \in PlanWriters:
       AcquirePlan(w)
       \/ ReadPlan(w)
       \/ SavePlan(w)
       \/ ReleasePlan(w)

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ owner \in Owners
  /\ file \subseteq MutationWriters
  /\ completed \subseteq MutationWriters
  /\ mutationPhase \in [MutationWriters -> MutationPhases]
  /\ mutationSnapshot \in [MutationWriters -> SUBSET MutationWriters]
  /\ mutationLocal \in [MutationWriters -> SUBSET MutationWriters]
  /\ planApplied \in BOOLEAN
  /\ dependencyCount \in 0..Cardinality(PlanWriters)
  /\ planPhase \in [PlanWriters -> PlanPhases]
  /\ planSawApplied \in [PlanWriters -> BOOLEAN]

NoLostCompletedMutation == completed \subseteq file

PlanEffectAtMostOnce == dependencyCount <= 1

MutationCriticalLock ==
  \A w \in MutationWriters:
    mutationPhase[w] \in {"locked", "read", "mutated", "saved"} => owner = w

PlanCriticalLock ==
  \A w \in PlanWriters:
    planPhase[w] \in {"locked", "read", "saved"} => owner = w

====
