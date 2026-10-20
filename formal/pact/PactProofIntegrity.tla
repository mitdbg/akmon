-------------------------- MODULE PactProofIntegrity --------------------------
EXTENDS FiniteSets

ArmIds == {"pytest-arm", "live-arm"}
Witnesses == {"pytest", "transcript"}
NoWitness == "none"

VARIABLES armWitness, integrationOwnerProven, namedTestExecuted, completed

vars == <<armWitness, integrationOwnerProven, namedTestExecuted, completed>>

TypeOK ==
    /\ armWitness \in [ArmIds -> (Witnesses \cup {NoWitness})]
    /\ integrationOwnerProven \in BOOLEAN
    /\ namedTestExecuted \in BOOLEAN
    /\ completed \in BOOLEAN

ArmsBound ==
    /\ \A arm \in ArmIds : armWitness[arm] # NoWitness
    /\ \A left, right \in ArmIds :
          left # right => armWitness[left] # armWitness[right]

Init ==
    /\ armWitness = [arm \in ArmIds |-> NoWitness]
    /\ integrationOwnerProven = FALSE
    /\ namedTestExecuted = FALSE
    /\ completed = FALSE

AttachArmWitness(arm, witness) ==
    /\ ~completed
    /\ arm \in ArmIds
    /\ witness \in Witnesses
    /\ armWitness' = [armWitness EXCEPT ![arm] = witness]
    /\ UNCHANGED <<integrationOwnerProven, namedTestExecuted, completed>>

ProveIntegrationOwner ==
    /\ ~completed
    /\ integrationOwnerProven' = TRUE
    /\ UNCHANGED <<armWitness, namedTestExecuted, completed>>

ExecuteNamedTest ==
    /\ ~completed
    /\ namedTestExecuted' = TRUE
    /\ UNCHANGED <<armWitness, integrationOwnerProven, completed>>

Complete ==
    /\ ~completed
    /\ ArmsBound
    /\ integrationOwnerProven
    /\ namedTestExecuted
    /\ completed' = TRUE
    /\ UNCHANGED <<armWitness, integrationOwnerProven, namedTestExecuted>>

Next ==
    \/ \E arm \in ArmIds, witness \in Witnesses : AttachArmWitness(arm, witness)
    \/ ProveIntegrationOwner
    \/ ExecuteNamedTest
    \/ Complete

Spec == Init /\ [][Next]_vars

CompletionRequiresBoundArms == completed => ArmsBound
CompletionRequiresIntegrationOwner == completed => integrationOwnerProven
CompletionRequiresNamedTestExecution == completed => namedTestExecuted
ProofIntegrity ==
    /\ CompletionRequiresBoundArms
    /\ CompletionRequiresIntegrationOwner
    /\ CompletionRequiresNamedTestExecution

=============================================================================
