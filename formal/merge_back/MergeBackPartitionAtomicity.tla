---- MODULE MergeBackPartitionAtomicity ----
EXTENDS FiniteSets

\* Staging/quarantine partition commit, modeled at the transaction level
\* (strengthen-formal-models wave 3). Production writes the accepted rows to
\* the staging branch and the quarantined rows to the quarantine branch
\* inside ONE staging-session transaction (merge_back.py _compose_and_stage);
\* the writes become durable only at COMMIT, and a crash mid-transaction
\* discards both. Before this wave the whole commit was a single atomic
\* action, so the atomicity invariants were tautologies over a transition
\* relation with no mid-transaction state. Now the two writes are separate
\* buffered actions with a crash interleaving between them: a model (or
\* production) edit that writes either partition durably before COMMIT trips
\* CrashLeavesNeitherPartialWrite.

Rows == {"clean", "illegal", "conflict"}
DurablePreview == Rows
Phases == {
  "start", "blocked", "partitioned", "staging_written",
  "quarantine_written", "committed", "aborted"
}

VARIABLES
  phase,
  accepted,
  quarantined,
  txnStagingRows,
  txnQuarantineRows,
  stagingRows,
  quarantineRows,
  unclassifiable,
  faultDuringCommit

vars == <<
  phase,
  accepted,
  quarantined,
  txnStagingRows,
  txnQuarantineRows,
  stagingRows,
  quarantineRows,
  unclassifiable,
  faultDuringCommit
>>

Init ==
  /\ phase = "start"
  /\ accepted = {}
  /\ quarantined = {}
  /\ txnStagingRows = {}
  /\ txnQuarantineRows = {}
  /\ stagingRows = {}
  /\ quarantineRows = {}
  /\ unclassifiable \in BOOLEAN
  /\ faultDuringCommit \in BOOLEAN

BlockUnclassifiable ==
  /\ phase = "start"
  /\ unclassifiable
  /\ phase' = "blocked"
  /\ UNCHANGED <<
       accepted, quarantined, txnStagingRows, txnQuarantineRows,
       stagingRows, quarantineRows, unclassifiable, faultDuringCommit
     >>

Partition ==
  /\ phase = "start"
  /\ ~unclassifiable
  /\ accepted' = {"clean"}
  /\ quarantined' = {"illegal", "conflict"}
  /\ phase' = "partitioned"
  /\ UNCHANGED <<
       txnStagingRows, txnQuarantineRows, stagingRows, quarantineRows,
       unclassifiable, faultDuringCommit
     >>

\* Write 1: accepted rows into the staging branch — INSIDE the open
\* transaction. Durable rows are untouched; only the txn buffer fills.
WriteStagingInTxn ==
  /\ phase = "partitioned"
  /\ txnStagingRows' = accepted
  /\ phase' = "staging_written"
  /\ UNCHANGED <<
       accepted, quarantined, txnQuarantineRows, stagingRows,
       quarantineRows, unclassifiable, faultDuringCommit
     >>

\* Write 2: quarantined rows into the quarantine branch — same transaction.
WriteQuarantineInTxn ==
  /\ phase = "staging_written"
  /\ txnQuarantineRows' = quarantined
  /\ phase' = "quarantine_written"
  /\ UNCHANGED <<
       accepted, quarantined, txnStagingRows, stagingRows, quarantineRows,
       unclassifiable, faultDuringCommit
     >>

\* COMMIT publishes both partitions together.
TxnCommit ==
  /\ phase = "quarantine_written"
  /\ ~faultDuringCommit
  /\ stagingRows' = txnStagingRows
  /\ quarantineRows' = txnQuarantineRows
  /\ phase' = "committed"
  /\ UNCHANGED <<
       accepted, quarantined, txnStagingRows, txnQuarantineRows,
       unclassifiable, faultDuringCommit
     >>

\* A crash at ANY point inside the open transaction (after either write,
\* or at the failed COMMIT) discards the buffers; durable rows never moved.
TxnCrash ==
  /\ \/ phase \in {"staging_written", "quarantine_written"} /\ faultDuringCommit
     \/ phase = "partitioned" /\ faultDuringCommit
  /\ txnStagingRows' = {}
  /\ txnQuarantineRows' = {}
  /\ phase' = "aborted"
  /\ UNCHANGED <<
       accepted, quarantined, stagingRows, quarantineRows,
       unclassifiable, faultDuringCommit
     >>

Next ==
  \/ BlockUnclassifiable
  \/ Partition
  \/ WriteStagingInTxn
  \/ WriteQuarantineInTxn
  \/ TxnCommit
  \/ TxnCrash

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ phase \in Phases
  /\ accepted \subseteq Rows
  /\ quarantined \subseteq Rows
  /\ txnStagingRows \subseteq Rows
  /\ txnQuarantineRows \subseteq Rows
  /\ stagingRows \subseteq Rows
  /\ quarantineRows \subseteq Rows
  /\ unclassifiable \in BOOLEAN
  /\ faultDuringCommit \in BOOLEAN

AcceptedQuarantinedDisjoint ==
  accepted \cap quarantined = {}

AcceptedQuarantinedCoverDurablePreview ==
  phase \in {
    "partitioned", "staging_written", "quarantine_written",
    "committed", "aborted"
  } => accepted \cup quarantined = DurablePreview

ConflictsAlwaysQuarantined ==
  phase \in {
    "partitioned", "staging_written", "quarantine_written",
    "committed", "aborted"
  } => "conflict" \in quarantined

IllegalRowsQuarantined ==
  phase \in {
    "partitioned", "staging_written", "quarantine_written",
    "committed", "aborted"
  } => "illegal" \in quarantined

UnclassifiableRowsBlock ==
  unclassifiable => phase \notin {
    "partitioned", "staging_written", "quarantine_written", "committed"
  }

StageAndQuarantineCommitAtomically ==
  phase = "committed" =>
    /\ stagingRows = accepted
    /\ quarantineRows = quarantined

\* Mid-transaction writes are never durable: at every state before COMMIT,
\* including between the two writes, the durable partitions are empty. This
\* is the invariant a direct-write edit (bypassing the txn buffer) trips via
\* the crash interleaving. Positive enumeration so a future post-commit
\* phase does not trip it spuriously.
NoDurableWritesBeforeCommit ==
  phase \in {
    "start", "blocked", "partitioned", "staging_written",
    "quarantine_written", "aborted"
  } => stagingRows = {} /\ quarantineRows = {}

CrashLeavesNeitherPartialWrite ==
  phase = "aborted" =>
    /\ stagingRows = {}
    /\ quarantineRows = {}

MergeBackPartitionAtomicitySafety ==
  /\ TypeOK
  /\ AcceptedQuarantinedDisjoint
  /\ AcceptedQuarantinedCoverDurablePreview
  /\ ConflictsAlwaysQuarantined
  /\ IllegalRowsQuarantined
  /\ UnclassifiableRowsBlock
  /\ StageAndQuarantineCommitAtomically
  /\ NoDurableWritesBeforeCommit
  /\ CrashLeavesNeitherPartialWrite

====
