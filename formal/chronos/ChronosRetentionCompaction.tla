---- MODULE ChronosRetentionCompaction ----
EXTENDS FiniteSets, Naturals

Generations == {"g0", "g1", "g2", "g3", "g4"}
Tables == {"nodes", "edges", "effective", "tombstones"}
DataTables == {"nodes", "edges", "effective"}
Phases == {"idle", "planned", "pruning", "crashed", "complete"}
LockOwners == {"none", "compactor"}

CONSTANT BuggyIgnoreProtection
CONSTANT BuggyAllowPublishOverlap
CONSTANT BuggyAllowStalePlan
CONSTANT BuggyAllowDataBeforeTombstone
CONSTANT BuggyAllowUnleasedReader

VARIABLES
  phase,
  retained,
  namedPins,
  leases,
  activeReaders,
  forkBases,
  head,
  headEpoch,
  activePolicy,
  activeCatalog,
  activeTableMap,
  protectionEpoch,
  planHeadEpoch,
  planPolicy,
  planCatalog,
  planTableMap,
  planProtectionEpoch,
  planProtected,
  lockOwner,
  committed,
  tombstoned,
  refused,
  overlap,
  staleMutation,
  completionSnapshot,
  replayed

vars == <<
  phase, retained, namedPins, leases, activeReaders, forkBases, head, headEpoch,
  activePolicy, activeCatalog, activeTableMap, protectionEpoch,
  planHeadEpoch, planPolicy, planCatalog, planTableMap,
  planProtectionEpoch, planProtected, lockOwner, committed,
  tombstoned, refused, overlap, staleMutation, completionSnapshot,
  replayed
>>

Protected == {head} \cup namedPins \cup leases \cup forkBases
Expired == IF BuggyIgnoreProtection THEN {"g0", "g2"} ELSE {"g0"} \ Protected

BindingsMatch ==
  /\ planHeadEpoch = headEpoch
  /\ planPolicy = activePolicy
  /\ planCatalog = activeCatalog
  /\ planTableMap = activeTableMap
  /\ planProtectionEpoch = protectionEpoch
  /\ planProtected = Protected

Init ==
  /\ phase = "idle"
  /\ retained = Generations
  /\ namedPins = {"g1"}
  /\ leases = {"g2"}
  /\ activeReaders = {"g2"}
  /\ forkBases = {"g3"}
  /\ head = "g4"
  /\ headEpoch = 1
  /\ activePolicy = 1
  /\ activeCatalog = 1
  /\ activeTableMap = 1
  /\ protectionEpoch = 1
  /\ planHeadEpoch = 0
  /\ planPolicy = 0
  /\ planCatalog = 0
  /\ planTableMap = 0
  /\ planProtectionEpoch = 0
  /\ planProtected = {}
  /\ lockOwner = "none"
  /\ committed = {}
  /\ tombstoned = {}
  /\ refused = {}
  /\ overlap = FALSE
  /\ staleMutation = FALSE
  /\ completionSnapshot = Generations
  /\ replayed = FALSE

CreatePlan ==
  /\ phase = "idle"
  /\ phase' = "planned"
  /\ planHeadEpoch' = headEpoch
  /\ planPolicy' = activePolicy
  /\ planCatalog' = activeCatalog
  /\ planTableMap' = activeTableMap
  /\ planProtectionEpoch' = protectionEpoch
  /\ planProtected' = Protected
  /\ UNCHANGED <<retained, namedPins, leases, activeReaders, forkBases, head, headEpoch,
                  activePolicy, activeCatalog, activeTableMap, protectionEpoch,
                  lockOwner, committed, tombstoned, refused, overlap,
                  staleMutation, completionSnapshot, replayed>>

AcquirePin ==
  /\ phase \in {"planned", "crashed"}
  /\ protectionEpoch = 1
  /\ "g0" \in retained
  /\ namedPins' = namedPins \cup {"g0"}
  /\ protectionEpoch' = 2
  /\ UNCHANGED <<phase, retained, leases, activeReaders, forkBases, head, headEpoch,
                  activePolicy, activeCatalog, activeTableMap, planHeadEpoch,
                  planPolicy, planCatalog, planTableMap, planProtectionEpoch,
                  planProtected, lockOwner, committed, tombstoned, refused,
                  overlap, staleMutation, completionSnapshot, replayed>>

OpenHistoricalReader ==
  /\ phase \in {"idle", "planned", "crashed"}
  /\ "g0" \in retained
  /\ "g0" \notin activeReaders
  /\ activeReaders' = activeReaders \cup {"g0"}
  /\ IF BuggyAllowUnleasedReader
        THEN /\ leases' = leases
             /\ protectionEpoch' = protectionEpoch
        ELSE /\ leases' = leases \cup {"g0"}
             /\ protectionEpoch' = 2
  /\ UNCHANGED <<phase, retained, namedPins, forkBases, head, headEpoch,
                  activePolicy, activeCatalog, activeTableMap, planHeadEpoch,
                  planPolicy, planCatalog, planTableMap, planProtectionEpoch,
                  planProtected, lockOwner, committed, tombstoned, refused,
                  overlap, staleMutation, completionSnapshot, replayed>>

DriftPolicy ==
  /\ phase \in {"planned", "crashed"}
  /\ activePolicy = 1
  /\ activePolicy' = 2
  /\ UNCHANGED <<phase, retained, namedPins, leases, activeReaders, forkBases, head,
                  headEpoch, activeCatalog, activeTableMap, protectionEpoch,
                  planHeadEpoch, planPolicy, planCatalog, planTableMap,
                  planProtectionEpoch, planProtected, lockOwner, committed,
                  tombstoned, refused, overlap, staleMutation,
                  completionSnapshot, replayed>>

DriftCatalog ==
  /\ phase \in {"planned", "crashed"}
  /\ activeCatalog = 1
  /\ activeCatalog' = 2
  /\ UNCHANGED <<phase, retained, namedPins, leases, activeReaders, forkBases, head,
                  headEpoch, activePolicy, activeTableMap, protectionEpoch,
                  planHeadEpoch, planPolicy, planCatalog, planTableMap,
                  planProtectionEpoch, planProtected, lockOwner, committed,
                  tombstoned, refused, overlap, staleMutation,
                  completionSnapshot, replayed>>

DriftTableMap ==
  /\ phase \in {"planned", "crashed"}
  /\ activeTableMap = 1
  /\ activeTableMap' = 2
  /\ UNCHANGED <<phase, retained, namedPins, leases, activeReaders, forkBases, head,
                  headEpoch, activePolicy, activeCatalog, protectionEpoch,
                  planHeadEpoch, planPolicy, planCatalog, planTableMap,
                  planProtectionEpoch, planProtected, lockOwner, committed,
                  tombstoned, refused, overlap, staleMutation,
                  completionSnapshot, replayed>>

StartCompaction ==
  /\ phase = "planned"
  /\ lockOwner = "none"
  /\ BindingsMatch \/ BuggyAllowStalePlan
  /\ phase' = "pruning"
  /\ lockOwner' = "compactor"
  /\ staleMutation' = ~BindingsMatch
  /\ UNCHANGED <<retained, namedPins, leases, activeReaders, forkBases, head, headEpoch,
                  activePolicy, activeCatalog, activeTableMap, protectionEpoch,
                  planHeadEpoch, planPolicy, planCatalog, planTableMap,
                  planProtectionEpoch, planProtected, committed, tombstoned,
                  refused, overlap, completionSnapshot, replayed>>

Publish ==
  /\ headEpoch = 1
  /\ \/ phase \in {"idle", "planned", "crashed", "complete"}
     \/ /\ phase = "pruning"
        /\ BuggyAllowPublishOverlap
  /\ headEpoch' = 2
  /\ overlap' = (overlap \/ (phase = "pruning"))
  /\ UNCHANGED <<phase, retained, namedPins, leases, activeReaders, forkBases, head,
                  activePolicy, activeCatalog, activeTableMap, protectionEpoch,
                  planHeadEpoch, planPolicy, planCatalog, planTableMap,
                  planProtectionEpoch, planProtected, lockOwner, committed,
                  tombstoned, refused, staleMutation, completionSnapshot,
                  replayed>>

PruneBatch(table) ==
  /\ phase = "pruning"
  /\ lockOwner = "compactor"
  /\ table \in Tables \ committed
  /\ table \in DataTables =>
       "tombstones" \in committed \/ BuggyAllowDataBeforeTombstone
  /\ committed' = committed \cup {table}
  /\ retained' = IF table = "tombstones" THEN retained \ Expired ELSE retained
  /\ tombstoned' = IF table = "tombstones" THEN tombstoned \cup Expired ELSE tombstoned
  /\ refused' = IF table = "tombstones" THEN refused \cup Expired ELSE refused
  /\ UNCHANGED <<phase, namedPins, leases, activeReaders, forkBases, head, headEpoch,
                  activePolicy, activeCatalog, activeTableMap, protectionEpoch,
                  planHeadEpoch, planPolicy, planCatalog, planTableMap,
                  planProtectionEpoch, planProtected, lockOwner, overlap,
                  staleMutation, completionSnapshot, replayed>>

Crash ==
  /\ phase = "pruning"
  /\ phase' = "crashed"
  /\ lockOwner' = "none"
  /\ UNCHANGED <<retained, namedPins, leases, activeReaders, forkBases, head, headEpoch,
                  activePolicy, activeCatalog, activeTableMap, protectionEpoch,
                  planHeadEpoch, planPolicy, planCatalog, planTableMap,
                  planProtectionEpoch, planProtected, committed, tombstoned,
                  refused, overlap, staleMutation, completionSnapshot,
                  replayed>>

Resume ==
  /\ phase = "crashed"
  /\ lockOwner = "none"
  /\ BindingsMatch \/ BuggyAllowStalePlan
  /\ phase' = "pruning"
  /\ lockOwner' = "compactor"
  /\ staleMutation' = staleMutation \/ ~BindingsMatch
  /\ UNCHANGED <<retained, namedPins, leases, activeReaders, forkBases, head, headEpoch,
                  activePolicy, activeCatalog, activeTableMap, protectionEpoch,
                  planHeadEpoch, planPolicy, planCatalog, planTableMap,
                  planProtectionEpoch, planProtected, committed, tombstoned,
                  refused, overlap, completionSnapshot, replayed>>

Finish ==
  /\ phase = "pruning"
  /\ lockOwner = "compactor"
  /\ committed = Tables
  /\ BindingsMatch
  /\ phase' = "complete"
  /\ lockOwner' = "none"
  /\ completionSnapshot' = retained
  /\ UNCHANGED <<retained, namedPins, leases, activeReaders, forkBases, head, headEpoch,
                  activePolicy, activeCatalog, activeTableMap, protectionEpoch,
                  planHeadEpoch, planPolicy, planCatalog, planTableMap,
                  planProtectionEpoch, planProtected, committed, tombstoned,
                  refused, overlap, staleMutation, replayed>>

Replay ==
  /\ phase = "complete"
  /\ phase' = "planned"
  /\ planHeadEpoch' = headEpoch
  /\ planPolicy' = activePolicy
  /\ planCatalog' = activeCatalog
  /\ planTableMap' = activeTableMap
  /\ planProtectionEpoch' = protectionEpoch
  /\ planProtected' = Protected
  /\ committed' = {}
  /\ staleMutation' = FALSE
  /\ replayed' = TRUE
  /\ UNCHANGED <<retained, namedPins, leases, activeReaders, forkBases, head, headEpoch,
                  activePolicy, activeCatalog, activeTableMap, protectionEpoch,
                  lockOwner, tombstoned, refused, overlap, completionSnapshot>>

Next ==
  CreatePlan \/ AcquirePin \/ OpenHistoricalReader \/ DriftPolicy \/ DriftCatalog \/ DriftTableMap \/
  StartCompaction \/ Publish \/
  (\E table \in Tables: PruneBatch(table)) \/ Crash \/ Resume \/ Finish \/ Replay

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ phase \in Phases
  /\ retained \subseteq Generations
  /\ namedPins \subseteq Generations
  /\ leases \subseteq Generations
  /\ activeReaders \subseteq Generations
  /\ forkBases \subseteq Generations
  /\ head \in Generations
  /\ headEpoch \in 1..2
  /\ activePolicy \in 1..2
  /\ activeCatalog \in 1..2
  /\ activeTableMap \in 1..2
  /\ protectionEpoch \in 1..2
  /\ planHeadEpoch \in 0..2
  /\ planPolicy \in 0..2
  /\ planCatalog \in 0..2
  /\ planTableMap \in 0..2
  /\ planProtectionEpoch \in 0..2
  /\ planProtected \subseteq Generations
  /\ lockOwner \in LockOwners
  /\ committed \subseteq Tables
  /\ tombstoned \subseteq Generations
  /\ refused \subseteq Generations
  /\ overlap \in BOOLEAN
  /\ staleMutation \in BOOLEAN
  /\ completionSnapshot \subseteq Generations
  /\ replayed \in BOOLEAN

CurrentAndProtectedReadsPreserved == Protected \subseteq retained

FloorNeverCrossesProtection ==
  phase = "pruning" => Expired \cap Protected = {}

PublishAndCompactionMutuallyExclusive == ~overlap

PlanBindingsFreshWhileMutating ==
  phase = "pruning" => BindingsMatch /\ ~staleMutation

TableBatchesAreAtomicAndOrdered ==
  /\ committed \subseteq Tables
  /\ committed \cap DataTables # {} => "tombstones" \in committed

HistoricalReadsAreCompleteOrRefused ==
  committed \cap DataTables # {} =>
    /\ Expired \subseteq tombstoned
    /\ Expired \subseteq refused
    /\ activeReaders \cap tombstoned = {}

EveryActiveReaderHasLease == activeReaders \subseteq leases

CrashStateIsValidOrResumable ==
  phase = "crashed" =>
    /\ lockOwner = "none"
    /\ committed \subseteq Tables
    /\ head \in retained
    /\ Protected \subseteq retained

RetiredGenerationRefusalIsStable ==
  /\ tombstoned \cap retained = {}
  /\ tombstoned \subseteq refused

ReplayIsIdempotent ==
  replayed =>
    /\ retained = completionSnapshot
    /\ tombstoned = refused

RetentionCompactionSafety ==
  /\ TypeOK
  /\ CurrentAndProtectedReadsPreserved
  /\ FloorNeverCrossesProtection
  /\ PublishAndCompactionMutuallyExclusive
  /\ PlanBindingsFreshWhileMutating
  /\ TableBatchesAreAtomicAndOrdered
  /\ EveryActiveReaderHasLease
  /\ HistoricalReadsAreCompleteOrRefused
  /\ CrashStateIsValidOrResumable
  /\ RetiredGenerationRefusalIsStable
  /\ ReplayIsIdempotent

====
