---- MODULE StableReemission ----
EXTENDS Naturals

\* Issue #795: an unchanged source may be awakened repeatedly, including by a
\* caller that bypasses OKG's graph-store no-op filter.  Correctness therefore
\* needs both stable source identity and an exact no-op guard at the interval
\* boundary.  Wake is the linearized mutation after the implementation's
\* per-branch database lock; concurrent compare-and-splice behavior refines to
\* one Wake at a time and is checked by a PostgreSQL concurrency test.
\* Git-history source_file nodes explicitly declare substitute ownership: when a
\* richer git-files owner already exists, a substitute must not reattribute it.

CONSTANTS
  BuggyWorktreeMembership,
  BuggyRunRevision,
  GraphNoopGuard,
  BackendNoopGuard,
  NonClobberSubstitute

Callers == {"graph", "direct"}
Memberships == {"unrun", "absent", "present"}
Revisions == {"unrun", "stable", "run0", "run1", "git-files", "none"}
Owners == {"none", "git_history", "git_files"}
Actions == {"init", "dirty", "history", "files_upsert", "files_retract"}

VARIABLES
  dirty,
  runBit,
  emittedMembership,
  emittedRevision,
  visibleMembership,
  visibleRevision,
  fileAuthorityExpected,
  owner,
  duplicateVersioned,
  lastAction

vars == <<
  dirty,
  runBit,
  emittedMembership,
  emittedRevision,
  visibleMembership,
  visibleRevision,
  fileAuthorityExpected,
  owner,
  duplicateVersioned,
  lastAction
>>

Init ==
  /\ dirty \in BOOLEAN
  /\ runBit = 0
  /\ emittedMembership = "unrun"
  /\ emittedRevision = "unrun"
  /\ fileAuthorityExpected \in BOOLEAN
  /\ visibleMembership = IF fileAuthorityExpected THEN "present" ELSE "absent"
  /\ visibleRevision = IF fileAuthorityExpected THEN "git-files" ELSE "none"
  /\ owner = IF fileAuthorityExpected THEN "git_files" ELSE "none"
  /\ duplicateVersioned = FALSE
  /\ lastAction = "init"

ToggleDirty ==
  /\ dirty' = ~dirty
  /\ lastAction' = "dirty"
  /\ UNCHANGED <<
       runBit, emittedMembership, emittedRevision, visibleMembership,
       visibleRevision, fileAuthorityExpected, owner, duplicateVersioned
     >>

CandidateMembership ==
  IF BuggyWorktreeMembership
  THEN IF dirty THEN "absent" ELSE "present"
  ELSE "present"

NextRunBit == 1 - runBit

CandidateRevision ==
  IF BuggyRunRevision
  THEN IF NextRunBit = 0 THEN "run0" ELSE "run1"
  ELSE "stable"

ExactVisibleMatch ==
  /\ CandidateMembership = visibleMembership
  /\ CandidateRevision = visibleRevision

SubstituteBlocked == NonClobberSubstitute /\ owner = "git_files"

Wake(caller) ==
  /\ caller \in Callers
  /\ runBit' = NextRunBit
  /\ emittedMembership' = CandidateMembership
  /\ emittedRevision' = CandidateRevision
  /\ lastAction' = "history"
  /\ IF SubstituteBlocked
        \/ (caller = "graph" /\ GraphNoopGuard /\ ExactVisibleMatch)
        \/ (BackendNoopGuard /\ ExactVisibleMatch)
     THEN
       /\ visibleMembership' = visibleMembership
       /\ visibleRevision' = visibleRevision
       /\ owner' = owner
       /\ duplicateVersioned' = duplicateVersioned
     ELSE
       /\ visibleMembership' = CandidateMembership
       /\ visibleRevision' = CandidateRevision
       /\ owner' = IF CandidateMembership = "present" THEN "git_history" ELSE "none"
       /\ duplicateVersioned' = (duplicateVersioned \/ ExactVisibleMatch)
  /\ UNCHANGED <<dirty, fileAuthorityExpected>>

FilesUpsert ==
  /\ fileAuthorityExpected' = TRUE
  /\ visibleMembership' = "present"
  /\ visibleRevision' = "git-files"
  /\ owner' = "git_files"
  /\ lastAction' = "files_upsert"
  /\ UNCHANGED <<
       dirty, runBit, emittedMembership, emittedRevision, duplicateVersioned
     >>

FilesRetract ==
  /\ fileAuthorityExpected
  /\ fileAuthorityExpected' = FALSE
  /\ visibleMembership' = "absent"
  /\ visibleRevision' = "none"
  /\ owner' = "none"
  /\ lastAction' = "files_retract"
  /\ UNCHANGED <<
       dirty, runBit, emittedMembership, emittedRevision, duplicateVersioned
     >>

Next ==
  ToggleDirty \/ FilesUpsert \/ FilesRetract \/
    (\E caller \in Callers : Wake(caller))

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ dirty \in BOOLEAN
  /\ runBit \in 0..1
  /\ emittedMembership \in Memberships
  /\ emittedRevision \in Revisions
  /\ visibleMembership \in {"absent", "present"}
  /\ visibleRevision \in Revisions
  /\ fileAuthorityExpected \in BOOLEAN
  /\ owner \in Owners
  /\ duplicateVersioned \in BOOLEAN
  /\ lastAction \in Actions

\* With fixed committed history, neither a dirty-worktree transition nor a new
\* execution id may change the source's semantic membership or revision.
StableSourceEmission ==
  emittedMembership = "unrun" \/
    /\ emittedMembership = "present"
    /\ emittedRevision = "stable"

\* A substitute endpoint fills an absence but never takes ownership from the
\* richer git-files source.
SubstitutePreservesAuthority ==
  fileAuthorityExpected => owner = "git_files"

\* After an authoritative row is retracted, the next history run restores its
\* substitute endpoint. This covers history -> files -> history and subsequent
\* files retraction -> history recovery, not only an initial-state choice.
HistoryRestoresMissingSubstitute ==
  lastAction = "history" => owner # "none"

\* The first semantic write is allowed. This flag records only a write whose
\* full row already matched the visible row, including graph-bypassing callers.
DuplicateStableWakeupsDoNotVersion == ~duplicateVersioned

StableReemission ==
  /\ TypeOK
  /\ StableSourceEmission
  /\ SubstitutePreservesAuthority
  /\ HistoryRestoresMissingSubstitute
  /\ DuplicateStableWakeupsDoNotVersion

====
