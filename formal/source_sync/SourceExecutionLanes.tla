---- MODULE SourceExecutionLanes ----
EXTENDS Naturals

\* Abstract source-sync lane model.
\*
\* Adapter work is per-source and may be considered independently lane-owned,
\* but admission/publish mutation remains serialized. Each write records the
\* source it is allowed to affect; the safety contract forbids cross-source
\* frontier, record-set, admission, or publish corruption.

Sources == {"docs", "pact"}
None == "none"
Values == 0..2
OwnerFor == [s \in Sources |-> s]

VARIABLES
  laneOwner,
  frontier,
  recordSet,
  admission,
  published,
  adapterDone,
  mutationOwner,
  superseded,
  lastWriter

vars == <<
  laneOwner,
  frontier,
  recordSet,
  admission,
  published,
  adapterDone,
  mutationOwner,
  superseded,
  lastWriter
>>

Init ==
  /\ laneOwner = [s \in Sources |-> None]
  /\ frontier = [s \in Sources |-> 0]
  /\ recordSet = [s \in Sources |-> 0]
  /\ admission = [s \in Sources |-> 0]
  /\ published = [s \in Sources |-> 0]
  /\ adapterDone = [s \in Sources |-> FALSE]
  /\ mutationOwner = None
  /\ superseded = [s \in Sources |-> FALSE]
  /\ lastWriter = [s \in Sources |-> None]

AcquireLane(s) ==
  /\ laneOwner[s] = None
  /\ laneOwner' = [laneOwner EXCEPT ![s] = OwnerFor[s]]
  /\ superseded' = [superseded EXCEPT ![s] = FALSE]
  /\ UNCHANGED <<
       frontier,
       recordSet,
       admission,
       published,
       adapterDone,
       mutationOwner,
       lastWriter
     >>

AdapterWrite(s) ==
  /\ laneOwner[s] = OwnerFor[s]
  /\ adapterDone[s] = FALSE
  /\ frontier[s] < 2
  /\ frontier' = [frontier EXCEPT ![s] = @ + 1]
  /\ recordSet' = [recordSet EXCEPT ![s] = @ + 1]
  /\ adapterDone' = [adapterDone EXCEPT ![s] = TRUE]
  /\ lastWriter' = [lastWriter EXCEPT ![s] = s]
  /\ UNCHANGED <<
       laneOwner,
       admission,
       published,
       mutationOwner,
       superseded
     >>

BeginSerializedMutation(s) ==
  /\ laneOwner[s] = OwnerFor[s]
  /\ adapterDone[s]
  /\ mutationOwner = None
  /\ mutationOwner' = s
  /\ UNCHANGED <<
       laneOwner,
       frontier,
       recordSet,
       admission,
       published,
       adapterDone,
       superseded,
       lastWriter
     >>

CommitAdmission(s) ==
  /\ mutationOwner = s
  /\ admission[s] < frontier[s]
  /\ admission' = [admission EXCEPT ![s] = frontier[s]]
  /\ lastWriter' = [lastWriter EXCEPT ![s] = s]
  /\ UNCHANGED <<
       laneOwner,
       frontier,
       recordSet,
       published,
       adapterDone,
       mutationOwner,
       superseded
     >>

Publish(s) ==
  /\ mutationOwner = s
  /\ admission[s] = frontier[s]
  /\ published' = [published EXCEPT ![s] = admission[s]]
  /\ laneOwner' = [laneOwner EXCEPT ![s] = None]
  /\ adapterDone' = [adapterDone EXCEPT ![s] = FALSE]
  /\ mutationOwner' = None
  /\ lastWriter' = [lastWriter EXCEPT ![s] = s]
  /\ UNCHANGED <<
       frontier,
       recordSet,
       admission,
       superseded
     >>

SupersedeBeforeMutation(s) ==
  /\ laneOwner[s] = OwnerFor[s]
  /\ mutationOwner # s
  /\ laneOwner' = [laneOwner EXCEPT ![s] = None]
  /\ adapterDone' = [adapterDone EXCEPT ![s] = FALSE]
  /\ superseded' = [superseded EXCEPT ![s] = TRUE]
  /\ lastWriter' = [lastWriter EXCEPT ![s] = s]
  /\ UNCHANGED <<
       frontier,
       recordSet,
       admission,
       published,
       mutationOwner
     >>

Stutter == UNCHANGED vars

Next ==
  \/ \E s \in Sources :
       \/ AcquireLane(s)
       \/ AdapterWrite(s)
       \/ BeginSerializedMutation(s)
       \/ CommitAdmission(s)
       \/ Publish(s)
       \/ SupersedeBeforeMutation(s)
  \/ Stutter

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ laneOwner \in [Sources -> (Sources \cup {None})]
  /\ frontier \in [Sources -> Values]
  /\ recordSet \in [Sources -> Values]
  /\ admission \in [Sources -> Values]
  /\ published \in [Sources -> Values]
  /\ adapterDone \in [Sources -> BOOLEAN]
  /\ mutationOwner \in Sources \cup {None}
  /\ superseded \in [Sources -> BOOLEAN]
  /\ lastWriter \in [Sources -> (Sources \cup {None})]

LaneOwnerMatchesSource ==
  \A s \in Sources : laneOwner[s] \in {None, OwnerFor[s]}

SerializedMutationExclusive ==
  mutationOwner = None \/ laneOwner[mutationOwner] = OwnerFor[mutationOwner]

SourceWritesStayInLane ==
  \A s \in Sources :
    /\ lastWriter[s] \in {None, s}
    /\ recordSet[s] = frontier[s]

PublishRequiresAdmission ==
  \A s \in Sources :
    /\ published[s] <= admission[s]
    /\ admission[s] <= frontier[s]

SupersedeDoesNotPublishUnadmittedState ==
  \A s \in Sources :
    superseded[s] => /\ laneOwner[s] = None
                     /\ published[s] <= admission[s]

SourceExecutionLaneIsolation ==
  /\ TypeOK
  /\ LaneOwnerMatchesSource
  /\ SerializedMutationExclusive
  /\ SourceWritesStayInLane
  /\ PublishRequiresAdmission
  /\ SupersedeDoesNotPublishUnadmittedState

====
