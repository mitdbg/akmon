---- MODULE PactV4DecisionLedger ----
(***************************************************************************
  PACT v4 decision-ledger pre-merge safety.

  The model covers the merge boundary after a human/operator decision has been
  recorded in the shared decision ledger. A merge attempt may succeed only when
  the ledger is reachable, the latest ledger decision is a terminal review
  approval for the current intent digest, and the file being merged is the
  archived terminal manifest. Reopens, stale approvals, missing approvals, and
  stale active file copies must refuse merge.
 ***************************************************************************)

EXTENDS FiniteSets

Digests == {"d0", "d1"}
NoDigest == "none"
Verdicts == {"none", "approve", "reopen", "revise", "reject", "supersede", "defer"}
ToStates == {"none", "retired", "awaiting_review", "in_progress", "rejected", "superseded", "legacy"}
FileLocations == {"active", "archived"}
Statuses == {
  "not_attempted",
  "approved",
  "ledger_unreachable",
  "no_approval_recorded",
  "approval_stale_intent_changed",
  "approval_superseded_by_later_decision",
  "approval_not_terminal_archive"
}
BlockerNames == {
  "ledger_unreachable",
  "missing_current_approval",
  "stale_approval_digest",
  "approval_superseded",
  "file_not_archived"
}

VARIABLES
  ledgerReachable,
  currentDigest,
  approvalDigest,
  latestVerdict,
  latestDigest,
  latestToState,
  fileLocation,
  mergeAttempted

vars == <<
  ledgerReachable,
  currentDigest,
  approvalDigest,
  latestVerdict,
  latestDigest,
  latestToState,
  fileLocation,
  mergeAttempted
>>

TerminalReviewApprovalForCurrent ==
  /\ latestVerdict = "approve"
  /\ latestDigest = currentDigest
  /\ latestToState \in {"retired", "legacy"}

CurrentApprovalExists ==
  approvalDigest = currentDigest

MergeStatus ==
  IF ~mergeAttempted
  THEN "not_attempted"
  ELSE IF ~ledgerReachable
  THEN "ledger_unreachable"
  ELSE IF approvalDigest \in Digests /\ ~CurrentApprovalExists
  THEN "approval_stale_intent_changed"
  ELSE IF ~CurrentApprovalExists
  THEN "no_approval_recorded"
  ELSE IF ~TerminalReviewApprovalForCurrent
  THEN "approval_superseded_by_later_decision"
  ELSE IF fileLocation # "archived"
  THEN "approval_not_terminal_archive"
  ELSE "approved"

RuntimeBlockers ==
  IF ~mergeAttempted
  THEN {}
  ELSE
    (IF ledgerReachable THEN {} ELSE {"ledger_unreachable"})
      \cup (IF approvalDigest = NoDigest THEN {"missing_current_approval"} ELSE {})
      \cup (IF approvalDigest \in Digests /\ approvalDigest # currentDigest THEN {"stale_approval_digest"} ELSE {})
      \cup (IF CurrentApprovalExists /\ ~TerminalReviewApprovalForCurrent THEN {"approval_superseded"} ELSE {})
      \cup (IF CurrentApprovalExists /\ TerminalReviewApprovalForCurrent /\ fileLocation # "archived" THEN {"file_not_archived"} ELSE {})

RuntimeAccepted == MergeStatus = "approved"

Init ==
  /\ ledgerReachable = TRUE
  /\ currentDigest = "d0"
  /\ approvalDigest = NoDigest
  /\ latestVerdict = "none"
  /\ latestDigest = NoDigest
  /\ latestToState = "none"
  /\ fileLocation = "active"
  /\ mergeAttempted = FALSE

SetLedgerReachable(value) ==
  /\ value \in BOOLEAN
  /\ ledgerReachable' = value
  /\ UNCHANGED <<
       currentDigest,
       approvalDigest,
       latestVerdict,
       latestDigest,
       latestToState,
       fileLocation,
       mergeAttempted
     >>

EditIntent(digest) ==
  /\ digest \in Digests
  /\ currentDigest' = digest
  /\ fileLocation' = "active"
  /\ UNCHANGED <<
       ledgerReachable,
       approvalDigest,
       latestVerdict,
       latestDigest,
       latestToState,
       mergeAttempted
     >>

ApproveForReview ==
  /\ ledgerReachable
  /\ fileLocation = "active"
  /\ approvalDigest' = currentDigest
  /\ latestVerdict' = "approve"
  /\ latestDigest' = currentDigest
  /\ latestToState' = "retired"
  /\ fileLocation' = "archived"
  /\ UNCHANGED << ledgerReachable, currentDigest, mergeAttempted >>

LegacyApproveForReview ==
  /\ ledgerReachable
  /\ fileLocation = "active"
  /\ approvalDigest' = currentDigest
  /\ latestVerdict' = "approve"
  /\ latestDigest' = currentDigest
  /\ latestToState' = "legacy"
  /\ fileLocation' = "archived"
  /\ UNCHANGED << ledgerReachable, currentDigest, mergeAttempted >>

Reopen ==
  /\ ledgerReachable
  /\ fileLocation = "archived"
  /\ latestVerdict' = "reopen"
  /\ latestDigest' = currentDigest
  /\ latestToState' = "awaiting_review"
  /\ fileLocation' = "active"
  /\ UNCHANGED << ledgerReachable, currentDigest, approvalDigest, mergeAttempted >>

RestoreActiveStaleFile ==
  /\ fileLocation = "archived"
  /\ fileLocation' = "active"
  /\ UNCHANGED <<
       ledgerReachable,
       currentDigest,
       approvalDigest,
       latestVerdict,
       latestDigest,
       latestToState,
       mergeAttempted
     >>

MergeAttempt ==
  /\ mergeAttempted' = TRUE
  /\ UNCHANGED <<
       ledgerReachable,
       currentDigest,
       approvalDigest,
       latestVerdict,
       latestDigest,
       latestToState,
       fileLocation
     >>

Next ==
  \/ \E value \in BOOLEAN: SetLedgerReachable(value)
  \/ \E digest \in Digests: EditIntent(digest)
  \/ ApproveForReview
  \/ LegacyApproveForReview
  \/ Reopen
  \/ RestoreActiveStaleFile
  \/ MergeAttempt

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ ledgerReachable \in BOOLEAN
  /\ currentDigest \in Digests
  /\ approvalDigest \in Digests \cup {NoDigest}
  /\ latestVerdict \in Verdicts
  /\ latestDigest \in Digests \cup {NoDigest}
  /\ latestToState \in ToStates
  /\ fileLocation \in FileLocations
  /\ mergeAttempted \in BOOLEAN
  /\ MergeStatus \in Statuses
  /\ RuntimeBlockers \subseteq BlockerNames

LedgerRowsWellFormed ==
  /\ latestVerdict = "none" => latestDigest = NoDigest /\ latestToState = "none"
  /\ latestVerdict # "none" => latestDigest \in Digests /\ latestToState # "none"
  /\ latestVerdict = "approve" /\ latestToState \in {"retired", "legacy"} =>
       approvalDigest = latestDigest

\* DEFINITIONAL SANITY-CHECK (not an independent safety proof). RuntimeAccepted
\* is DEFINED as (MergeStatus = "approved"), so this invariant is X <=> X and
\* holds by construction. It documents that acceptance is exactly the
\* "approved" merge status; the real safety content lives in the
\* NoMergeWithout* / NoMergeWith* invariants below.
RuntimeDecisionMatchesStatus ==
  RuntimeAccepted <=> MergeStatus = "approved"

BlockersReflectDecisionRows ==
  /\ "ledger_unreachable" \in RuntimeBlockers <=> mergeAttempted /\ ~ledgerReachable
  /\ "missing_current_approval" \in RuntimeBlockers <=> mergeAttempted /\ approvalDigest = NoDigest
  /\ "stale_approval_digest" \in RuntimeBlockers <=> mergeAttempted /\ approvalDigest \in Digests /\ approvalDigest # currentDigest
  /\ "approval_superseded" \in RuntimeBlockers <=> mergeAttempted /\ CurrentApprovalExists /\ ~TerminalReviewApprovalForCurrent
  /\ "file_not_archived" \in RuntimeBlockers <=> mergeAttempted /\ CurrentApprovalExists /\ TerminalReviewApprovalForCurrent /\ fileLocation # "archived"

NoMergeWithoutReachableLedger ==
  mergeAttempted /\ ~ledgerReachable => ~RuntimeAccepted

NoMergeWithoutCurrentApproval ==
  mergeAttempted /\ ~CurrentApprovalExists => ~RuntimeAccepted

NoMergeWithStaleDigest ==
  mergeAttempted /\ approvalDigest \in Digests /\ approvalDigest # currentDigest => ~RuntimeAccepted

NoMergeAfterReopen ==
  mergeAttempted /\ latestVerdict = "reopen" => ~RuntimeAccepted

NoMergeWithActiveFileState ==
  mergeAttempted /\ fileLocation = "active" => ~RuntimeAccepted

OnlyLatestTerminalApprovalCanMerge ==
  RuntimeAccepted => TerminalReviewApprovalForCurrent /\ fileLocation = "archived"

ArchivedCurrentApprovalCanMerge ==
  /\ mergeAttempted
  /\ ledgerReachable
  /\ CurrentApprovalExists
  /\ TerminalReviewApprovalForCurrent
  /\ fileLocation = "archived"
  => RuntimeAccepted

PactV4DecisionLedgerContract ==
  /\ LedgerRowsWellFormed
  /\ RuntimeDecisionMatchesStatus
  /\ BlockersReflectDecisionRows
  /\ NoMergeWithoutReachableLedger
  /\ NoMergeWithoutCurrentApproval
  /\ NoMergeWithStaleDigest
  /\ NoMergeAfterReopen
  /\ NoMergeWithActiveFileState
  /\ OnlyLatestTerminalApprovalCanMerge
  /\ ArchivedCurrentApprovalCanMerge

====
