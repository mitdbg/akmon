---------------------- MODULE DbosEpochSupersession ----------------------
EXTENDS Naturals, TLC

CurrentApplication == "application-v2"
CurrentEpoch == 2
CurrentReceipt == "activation-v2"
Applications == {"application-v1", CurrentApplication}
Epochs == {1, CurrentEpoch}
Receipts == {"activation-v1", CurrentReceipt}
CurrentDatabaseWriterEpoch == 3
CurrentWorktreeCapabilityEpoch == 2
CurrentBranchBindingEpoch == 2
OperationId == "source-sync-alpha-v2"
OperationDigest == "operation-digest-v2"
EquivalenceClass == "source-sync:alpha"
ExpectedParent == 1
CurrentPublishedGeneration == 1
CapsuleDigest == "capsule-digest-v2"

VARIABLES workApplication, workEpoch, workReceipt, rowState, bodyState,
          producerState, leaseOwner, mutationCount, refusedMutations,
          recoveryOwner, recoveryCount, outcomeState, databaseEpoch,
          databaseReceipt, transactionState, transactionEpoch,
          transactionReceipt

vars == <<workApplication, workEpoch, workReceipt, rowState, bodyState,
          producerState, leaseOwner, mutationCount, refusedMutations,
          recoveryOwner, recoveryCount, outcomeState, databaseEpoch,
          databaseReceipt, transactionState, transactionEpoch,
          transactionReceipt>>

CurrentBinding ==
    /\ workApplication = CurrentApplication
    /\ workEpoch = CurrentEpoch
    /\ workReceipt = CurrentReceipt

SharedFenceHeld == bodyState = "running"
ExclusiveFenceHeld == producerState = "frozen" /\ ~SharedFenceHeld

Init ==
    /\ workApplication \in Applications
    /\ workEpoch = IF workApplication = CurrentApplication THEN CurrentEpoch ELSE 1
    /\ workReceipt = IF workApplication = CurrentApplication THEN CurrentReceipt ELSE "activation-v1"
    /\ rowState = "pending"
    /\ bodyState = "idle"
    /\ producerState = "running"
    /\ leaseOwner = "none"
    /\ mutationCount = 0
    /\ refusedMutations = 0
    /\ recoveryOwner = "none"
    /\ recoveryCount = 0
    /\ outcomeState = "none"
    /\ databaseEpoch = workEpoch
    /\ databaseReceipt = workReceipt
    /\ transactionState = "none"
    /\ transactionEpoch = 0
    /\ transactionReceipt = "none"

StartBody ==
    /\ rowState = "pending"
    /\ bodyState = "idle"
    /\ producerState = "running"
    /\ ~ExclusiveFenceHeld
    /\ rowState' = "running"
    /\ bodyState' = "running"
    /\ leaseOwner' = workApplication
    /\ UNCHANGED <<workApplication, workEpoch, workReceipt, producerState,
                    mutationCount, refusedMutations, recoveryOwner,
                    recoveryCount, outcomeState, databaseEpoch,
                    databaseReceipt, transactionState, transactionEpoch,
                    transactionReceipt>>

FreezeProducer ==
    /\ producerState = "running"
    /\ producerState' = "frozen"
    /\ UNCHANGED <<workApplication, workEpoch, workReceipt, rowState,
                    bodyState, leaseOwner, mutationCount, refusedMutations,
                    recoveryOwner, recoveryCount, outcomeState, databaseEpoch,
                    databaseReceipt, transactionState, transactionEpoch,
                    transactionReceipt>>

FenceOldBody ==
    /\ ~CurrentBinding
    /\ producerState = "frozen"
    /\ bodyState = "running"
    /\ bodyState' = "fenced"
    /\ leaseOwner' = "none"
    /\ UNCHANGED <<workApplication, workEpoch, workReceipt, rowState,
                    producerState, mutationCount, refusedMutations,
                    recoveryOwner, recoveryCount, outcomeState, databaseEpoch,
                    databaseReceipt, transactionState, transactionEpoch,
                    transactionReceipt>>

SupersedeOldWork ==
    /\ ~CurrentBinding
    /\ rowState \in {"pending", "running"}
    /\ ExclusiveFenceHeld
    /\ rowState' = "superseded"
    /\ bodyState' = "fenced"
    /\ producerState' = "superseded"
    /\ leaseOwner' = "none"
    /\ UNCHANGED <<workApplication, workEpoch, workReceipt, mutationCount,
                    refusedMutations, recoveryOwner, recoveryCount,
                    outcomeState, databaseEpoch, databaseReceipt,
                    transactionState, transactionEpoch, transactionReceipt>>

RefuseStaleMutation ==
    /\ ~CurrentBinding
    /\ bodyState = "running"
    /\ refusedMutations = 0
    /\ refusedMutations' = 1
    /\ UNCHANGED <<workApplication, workEpoch, workReceipt, rowState,
                    bodyState, producerState, leaseOwner, mutationCount,
                    recoveryOwner, recoveryCount, outcomeState, databaseEpoch,
                    databaseReceipt, transactionState, transactionEpoch,
                    transactionReceipt>>

MutateCurrent ==
    /\ CurrentBinding
    /\ rowState = "running"
    /\ bodyState = "running"
    /\ leaseOwner = CurrentApplication
    /\ SharedFenceHeld
    /\ mutationCount = 0
    /\ mutationCount' = 1
    /\ UNCHANGED <<workApplication, workEpoch, workReceipt, rowState,
                    bodyState, producerState, leaseOwner, refusedMutations,
                    recoveryOwner, recoveryCount, outcomeState, databaseEpoch,
                    databaseReceipt, transactionState, transactionEpoch,
                    transactionReceipt>>

AcquireCurrentRecovery ==
    /\ ~CurrentBinding
    /\ rowState = "superseded"
    /\ recoveryOwner = "none"
    /\ recoveryOwner' = CurrentApplication
    /\ recoveryCount' = 1
    /\ UNCHANGED <<workApplication, workEpoch, workReceipt, rowState,
                    bodyState, producerState, leaseOwner, mutationCount,
                    refusedMutations, outcomeState, databaseEpoch,
                    databaseReceipt, transactionState, transactionEpoch,
                    transactionReceipt>>

QuarantineOldOutcome ==
    /\ ~CurrentBinding
    /\ rowState = "superseded"
    /\ outcomeState = "none"
    /\ outcomeState' = "quarantined"
    /\ UNCHANGED <<workApplication, workEpoch, workReceipt, rowState,
                    bodyState, producerState, leaseOwner, mutationCount,
                    refusedMutations, recoveryOwner, recoveryCount,
                    databaseEpoch, databaseReceipt, transactionState,
                    transactionEpoch, transactionReceipt>>

PublishCurrentOutcome ==
    /\ CurrentBinding
    /\ mutationCount = 1
    /\ rowState = "running"
    /\ outcomeState = "none"
    /\ outcomeState' = "published"
    /\ rowState' = "succeeded"
    /\ bodyState' = "finished"
    /\ leaseOwner' = "none"
    /\ UNCHANGED <<workApplication, workEpoch, workReceipt, producerState,
                    mutationCount, refusedMutations, recoveryOwner,
                    recoveryCount, databaseEpoch, databaseReceipt,
                    transactionState, transactionEpoch, transactionReceipt>>

BeginPostgresTransaction ==
    /\ rowState = "running"
    /\ bodyState = "running"
    /\ transactionState = "none"
    /\ SharedFenceHeld
    /\ transactionState' = "open"
    /\ transactionEpoch' = databaseEpoch
    /\ transactionReceipt' = databaseReceipt
    /\ UNCHANGED <<workApplication, workEpoch, workReceipt, rowState,
                    bodyState, producerState, leaseOwner, mutationCount,
                    refusedMutations, recoveryOwner, recoveryCount,
                    outcomeState, databaseEpoch, databaseReceipt>>

AdvanceActivationEpoch ==
    /\ databaseEpoch = 1
    /\ transactionState # "committed"
    /\ ExclusiveFenceHeld
    /\ databaseEpoch' = CurrentEpoch
    /\ databaseReceipt' = CurrentReceipt
    /\ UNCHANGED <<workApplication, workEpoch, workReceipt, rowState,
                    bodyState, producerState, leaseOwner, mutationCount,
                    refusedMutations, recoveryOwner, recoveryCount,
                    outcomeState, transactionState, transactionEpoch,
                    transactionReceipt>>

CommitPostgresTransaction ==
    /\ transactionState = "open"
    /\ CurrentBinding
    /\ transactionEpoch = databaseEpoch
    /\ transactionReceipt = databaseReceipt
    /\ SharedFenceHeld
    /\ transactionState' = "committed"
    /\ UNCHANGED <<workApplication, workEpoch, workReceipt, rowState,
                    bodyState, producerState, leaseOwner, mutationCount,
                    refusedMutations, recoveryOwner, recoveryCount,
                    outcomeState, databaseEpoch, databaseReceipt,
                    transactionEpoch, transactionReceipt>>

RefuseStaleTransactionCommit ==
    /\ transactionState = "open"
    /\ (~CurrentBinding \/ transactionEpoch # databaseEpoch
        \/ transactionReceipt # databaseReceipt)
    /\ transactionState' = "refused"
    /\ UNCHANGED <<workApplication, workEpoch, workReceipt, rowState,
                    bodyState, producerState, leaseOwner, mutationCount,
                    refusedMutations, recoveryOwner, recoveryCount,
                    outcomeState, databaseEpoch, databaseReceipt,
                    transactionEpoch, transactionReceipt>>

Noop == UNCHANGED vars

Next == StartBody \/ FreezeProducer \/ FenceOldBody
        \/ SupersedeOldWork \/ RefuseStaleMutation
        \/ MutateCurrent \/ AcquireCurrentRecovery \/ QuarantineOldOutcome
        \/ PublishCurrentOutcome \/ BeginPostgresTransaction
        \/ AdvanceActivationEpoch \/ CommitPostgresTransaction
        \/ RefuseStaleTransactionCommit \/ Noop

TypeInvariant ==
    /\ workApplication \in Applications
    /\ workEpoch \in Epochs
    /\ workReceipt \in Receipts
    /\ rowState \in {"pending", "running", "superseded", "succeeded"}
    /\ bodyState \in {"idle", "running", "fenced", "finished"}
    /\ producerState \in {"running", "frozen", "superseded"}
    /\ leaseOwner \in {"none"} \cup Applications
    /\ mutationCount \in 0..1
    /\ refusedMutations \in 0..1
    /\ recoveryOwner \in {"none", CurrentApplication}
    /\ recoveryCount \in 0..1
    /\ outcomeState \in {"none", "quarantined", "published"}
    /\ databaseEpoch \in Epochs
    /\ databaseReceipt \in Receipts
    /\ transactionState \in {"none", "open", "committed", "refused"}
    /\ transactionEpoch \in {0} \cup Epochs
    /\ transactionReceipt \in {"none"} \cup Receipts

EpochBindingComplete ==
    (workApplication = CurrentApplication) <=>
      (workEpoch = CurrentEpoch /\ workReceipt = CurrentReceipt)

SupersededBodyCannotMutate ==
    rowState = "superseded" => bodyState = "fenced" /\ mutationCount = 0

StaleBindingNeverMutates == ~CurrentBinding => mutationCount = 0

SingleCurrentRecovery ==
    /\ recoveryCount <= 1
    /\ recoveryCount = 1 => recoveryOwner = CurrentApplication

LeaseFencedOnSupersession ==
    rowState = "superseded" => leaseOwner = "none"

SupersededProducerCannotRecreate ==
    producerState = "superseded" => rowState = "superseded"

OutcomeOwnedByCurrentEpoch ==
    outcomeState = "published" => CurrentBinding /\ mutationCount = 1

NoStaleTransactionCommitAcrossEpochAdvance ==
    transactionState = "committed" =>
      /\ CurrentBinding
      /\ transactionEpoch = databaseEpoch
      /\ transactionReceipt = databaseReceipt

SharedAndExclusiveFencesNeverOverlap ==
    /\ ~(SharedFenceHeld /\ ExclusiveFenceHeld)
    /\ SharedFenceHeld => producerState \in {"running", "frozen"}
    /\ ExclusiveFenceHeld => bodyState # "running"

MutationBoundaryCarriesCompleteIdentity ==
    mutationCount = 1 =>
      /\ workEpoch = CurrentEpoch
      /\ workReceipt = CurrentReceipt
      /\ databaseEpoch = CurrentEpoch
      /\ CurrentDatabaseWriterEpoch > CurrentEpoch
      /\ CurrentApplication # ""
      /\ CurrentWorktreeCapabilityEpoch = CurrentEpoch
      /\ CurrentBranchBindingEpoch = CurrentEpoch
      /\ OperationId # ""
      /\ OperationDigest # ""
      /\ EquivalenceClass # ""
      /\ ExpectedParent = CurrentPublishedGeneration
      /\ CapsuleDigest # ""

RecoveryIsUniquePerEquivalenceClass ==
    /\ recoveryCount <= 1
    /\ recoveryCount = 1 =>
         /\ recoveryOwner = CurrentApplication
         /\ EquivalenceClass = "source-sync:alpha"

Spec == Init /\ [][Next]_vars

=============================================================================
