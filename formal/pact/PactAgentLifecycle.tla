---- MODULE PactAgentLifecycle ----
(***************************************************************************
  Integrated PACT v4 lifecycle used by the agent-experience program.

  Intent is file canonical. Proposal and final decisions are ledger events
  bound to the current intent digest. Task proof is an input to review state.
 ************************************************************************ **)

EXTENDS Naturals, FiniteSets

Tiers == {0, 1, 2, 3}
Digests == {"d0", "d1"}
NoDigest == "none"
States == {"draft", "proposed", "in_progress", "awaiting_review", "retired",
            "rejected", "superseded"}
Locations == {"active", "archived"}

VARIABLES
  tier,
  state,
  currentDigest,
  proposalDigest,
  finalDigest,
  reviewEpoch,
  finalEpoch,
  tasksProven,
  ledgerReachable,
  fileLocation

vars == <<
  tier,
  state,
  currentDigest,
  proposalDigest,
  finalDigest,
  reviewEpoch,
  finalEpoch,
  tasksProven,
  ledgerReachable,
  fileLocation
>>

Init ==
  /\ tier = 1
  /\ state = "draft"
  /\ currentDigest = "d0"
  /\ proposalDigest = NoDigest
  /\ finalDigest = NoDigest
  /\ reviewEpoch = 0
  /\ finalEpoch = 0
  /\ tasksProven = FALSE
  /\ ledgerReachable = TRUE
  /\ fileLocation = "active"

ChooseTier(value) ==
  /\ state = "draft"
  /\ value \in Tiers
  /\ tier' = value
  /\ UNCHANGED << state, currentDigest, proposalDigest, finalDigest,
                  reviewEpoch, finalEpoch,
                  tasksProven, ledgerReachable, fileLocation >>

PostEdit ==
  /\ state = "draft"
  /\ state' = IF tier \in {0, 1} THEN "in_progress" ELSE "proposed"
  /\ UNCHANGED << tier, currentDigest, proposalDigest, finalDigest,
                  reviewEpoch, finalEpoch,
                  tasksProven, ledgerReachable, fileLocation >>

ApproveProposal ==
  /\ tier \in {2, 3}
  /\ state = "proposed"
  /\ ledgerReachable
  /\ proposalDigest' = currentDigest
  /\ state' = "in_progress"
  /\ UNCHANGED << tier, currentDigest, finalDigest, reviewEpoch, finalEpoch, tasksProven,
                  ledgerReachable, fileLocation >>

CompleteTasks ==
  /\ state = "in_progress"
  /\ (tier \in {0, 1} \/ proposalDigest = currentDigest)
  /\ tasksProven' = TRUE
  /\ state' = "awaiting_review"
  /\ UNCHANGED << tier, currentDigest, proposalDigest, finalDigest,
                  reviewEpoch, finalEpoch,
                  ledgerReachable, fileLocation >>

ApproveFinal ==
  /\ state = "awaiting_review"
  /\ tasksProven
  /\ ledgerReachable
  /\ finalDigest' = currentDigest
  /\ finalEpoch' = reviewEpoch
  /\ state' = "retired"
  /\ fileLocation' = "archived"
  /\ UNCHANGED << tier, currentDigest, proposalDigest, reviewEpoch, tasksProven,
                  ledgerReachable >>

Revise ==
  /\ state \in {"awaiting_review", "retired"}
  /\ reviewEpoch < 2
  /\ state' = "in_progress"
  /\ reviewEpoch' = reviewEpoch + 1
  /\ tasksProven' = FALSE
  /\ fileLocation' = "active"
  /\ UNCHANGED << tier, currentDigest, proposalDigest, finalDigest, finalEpoch,
                  ledgerReachable >>

StaleProof ==
  /\ tasksProven
  /\ state \in {"awaiting_review", "retired"}
  /\ reviewEpoch < 2
  /\ tasksProven' = FALSE
  /\ state' = "in_progress"
  /\ reviewEpoch' = reviewEpoch + 1
  /\ fileLocation' = "active"
  /\ UNCHANGED << tier, currentDigest, proposalDigest, finalDigest, finalEpoch,
                  ledgerReachable >>

EditIntent(digest) ==
  /\ digest \in Digests
  /\ digest # currentDigest
  /\ reviewEpoch < 2
  /\ currentDigest' = digest
  /\ tasksProven' = FALSE
  /\ state' = IF tier \in {2, 3} THEN "proposed" ELSE "in_progress"
  /\ fileLocation' = "active"
  /\ reviewEpoch' = reviewEpoch + 1
  /\ UNCHANGED << tier, proposalDigest, finalDigest, finalEpoch, ledgerReachable >>

Reopen ==
  /\ state = "retired"
  /\ reviewEpoch < 2
  /\ state' = "awaiting_review"
  /\ reviewEpoch' = reviewEpoch + 1
  /\ fileLocation' = "active"
  /\ UNCHANGED << tier, currentDigest, proposalDigest, finalDigest, finalEpoch,
                  tasksProven, ledgerReachable >>

Reject ==
  /\ state \notin {"retired", "rejected", "superseded"}
  /\ ledgerReachable
  /\ state' = "rejected"
  /\ fileLocation' = "archived"
  /\ UNCHANGED << tier, currentDigest, proposalDigest, finalDigest,
                  reviewEpoch, finalEpoch, tasksProven, ledgerReachable >>

Supersede ==
  /\ state \notin {"retired", "rejected", "superseded"}
  /\ ledgerReachable
  /\ state' = "superseded"
  /\ fileLocation' = "archived"
  /\ UNCHANGED << tier, currentDigest, proposalDigest, finalDigest,
                  reviewEpoch, finalEpoch, tasksProven, ledgerReachable >>

SetLedgerReachable(value) ==
  /\ value \in BOOLEAN
  /\ ledgerReachable' = value
  /\ UNCHANGED << tier, state, currentDigest, proposalDigest, finalDigest,
                  reviewEpoch, finalEpoch,
                  tasksProven, fileLocation >>

Next ==
  \/ \E value \in Tiers: ChooseTier(value)
  \/ PostEdit
  \/ ApproveProposal
  \/ CompleteTasks
  \/ ApproveFinal
  \/ Revise
  \/ StaleProof
  \/ \E digest \in Digests: EditIntent(digest)
  \/ Reopen
  \/ Reject
  \/ Supersede
  \/ \E value \in BOOLEAN: SetLedgerReachable(value)

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ tier \in Tiers
  /\ state \in States
  /\ currentDigest \in Digests
  /\ proposalDigest \in Digests \cup {NoDigest}
  /\ finalDigest \in Digests \cup {NoDigest}
  /\ reviewEpoch \in 0..2
  /\ finalEpoch \in 0..2
  /\ tasksProven \in BOOLEAN
  /\ ledgerReachable \in BOOLEAN
  /\ fileLocation \in Locations

TierTwoPlusImplementationRequiresProposal ==
  tier \in {2, 3} /\ state \in {"in_progress", "awaiting_review", "retired"}
    => proposalDigest = currentDigest

ReviewRequiresProvenTasks ==
  state \in {"awaiting_review", "retired"} => tasksProven

RetirementRequiresFinalApproval ==
  state = "retired"
    => finalDigest = currentDigest /\ finalEpoch = reviewEpoch
       /\ fileLocation = "archived"

IntentDriftRevokesProgress ==
  proposalDigest \in Digests /\ proposalDigest # currentDigest
    => state \notin {"in_progress", "awaiting_review", "retired"}

UnprovenWorkCannotRemainInReview ==
  ~tasksProven => state \notin {"awaiting_review", "retired"}

TerminalDecisionsAreArchived ==
  state \in {"rejected", "superseded"} => fileLocation = "archived"

PactAgentLifecycleStateContract ==
  /\ TypeOK
  /\ TierTwoPlusImplementationRequiresProposal
  /\ ReviewRequiresProvenTasks
  /\ UnprovenWorkCannotRemainInReview
  /\ TerminalDecisionsAreArchived

PactAgentLifecycleApprovalContract ==
  /\ RetirementRequiresFinalApproval
  /\ IntentDriftRevokesProgress

PactAgentLifecycleContract ==
  /\ PactAgentLifecycleStateContract
  /\ PactAgentLifecycleApprovalContract

BrokenRetirementNeverOccurs == state # "retired"

====
