---- MODULE WorkflowAuthority ----
(***************************************************************************
  Workflow authority across file, graph, ledger, generation pin, evidence, and
  git surfaces.

  The model treats each surface as an explicit input. Review, merge, and task
  completion share the same safety rule: a stale or unpinned derived surface
  cannot authorize workflow progress. Live git evidence is required when the
  report makes code claims.
 ***************************************************************************)

Actions == {"review", "merge", "task_done"}

VARIABLES
  fileFresh,
  graphRequired,
  graphReady,
  graphFresh,
  projectionAuthority,
  eventPlaneAuthorityAllowed,
  intentEventFresh,
  intentDigestMatches,
  worktreeHeadMatches,
  worktreeClean,
  ledgerFresh,
  ledgerApproval,
  generationPinMatches,
  evidenceArtifactsFresh,
  pactCoordinationAgree,
  codeClaims,
  gitEvidence,
  action,
  blockers,
  requiredLiveChecks,
  accepted

vars == <<
  fileFresh,
  graphRequired,
  graphReady,
  graphFresh,
  projectionAuthority,
  eventPlaneAuthorityAllowed,
  intentEventFresh,
  intentDigestMatches,
  worktreeHeadMatches,
  worktreeClean,
  ledgerFresh,
  ledgerApproval,
  generationPinMatches,
  evidenceArtifactsFresh,
  pactCoordinationAgree,
  codeClaims,
  gitEvidence,
  action,
  blockers,
  requiredLiveChecks,
  accepted
>>

BlockerNames == {
  "file_not_fresh",
  "graph_authority_unknown",
  "event_plane_not_allowed",
  "graph_not_ready",
  "graph_not_fresh",
  "intent_event_not_fresh",
  "intent_digest_mismatch",
  "worktree_head_mismatch",
  "worktree_not_clean",
  "ledger_not_fresh",
  "no_ledger_approval",
  "approval_not_archived",
  "generation_pin_mismatch",
  "evidence_artifacts_stale",
  "pact_coordination_disagreement",
  "missing_git_evidence"
}

CheckNames == {
  "reload pact.yaml",
  "git status",
  "focused tests",
  "select graph authority surface",
  "use sync-root graph projection",
  "run graph readiness check",
  "refresh graph projection",
  "refresh pact intent event",
  "refresh pact intent digest",
  "refresh pact intent head",
  "clean worktree",
  "refresh decision ledger",
  "record an operator decision with okg pact decide",
  "archive the approved change",
  "repin graph generation",
  "verify exact published generation",
  "reconcile published generation pin",
  "rerun evidence checks",
  "reconcile pact and coordination state"
}

LedgerApprovals == {
  "not_judged",
  "approved",
  "absent",
  "not_archived"
}

ProjectionAuthorities == {
  "default_graph",
  "worktree_branch_graph",
  "published_generation_evidence",
  "event_plane_intent",
  "not_required"
}

GraphBackedAuthority ==
  graphRequired /\ projectionAuthority \in {
    "default_graph", "worktree_branch_graph", "published_generation_evidence"
  }

EventPlaneAuthority ==
  graphRequired /\ projectionAuthority = "event_plane_intent" /\
    eventPlaneAuthorityAllowed

EventPlaneRequested ==
  graphRequired /\ projectionAuthority = "event_plane_intent"

KnownAuthority ==
  ~graphRequired \/ GraphBackedAuthority \/ EventPlaneAuthority \/
    projectionAuthority = "not_required"

RuntimeBlockers ==
  (IF fileFresh THEN {} ELSE {"file_not_fresh"})
    \cup (IF KnownAuthority THEN {} ELSE {"graph_authority_unknown"})
    \cup (IF EventPlaneRequested /\ ~eventPlaneAuthorityAllowed THEN {"event_plane_not_allowed"} ELSE {})
    \cup (IF GraphBackedAuthority /\ ~graphReady THEN {"graph_not_ready"} ELSE {})
    \cup (IF GraphBackedAuthority /\ ~graphFresh THEN {"graph_not_fresh"} ELSE {})
    \cup (IF EventPlaneAuthority /\ ~intentEventFresh THEN {"intent_event_not_fresh"} ELSE {})
    \cup (IF EventPlaneAuthority /\ ~intentDigestMatches THEN {"intent_digest_mismatch"} ELSE {})
    \cup (IF EventPlaneAuthority /\ ~worktreeHeadMatches THEN {"worktree_head_mismatch"} ELSE {})
    \cup (IF EventPlaneAuthority /\ ~worktreeClean THEN {"worktree_not_clean"} ELSE {})
    \cup (IF ledgerFresh THEN {} ELSE {"ledger_not_fresh"})
    \cup (IF ledgerFresh /\ ledgerApproval = "absent" THEN
      {"no_ledger_approval"} ELSE {})
    \cup (IF ledgerFresh /\ ledgerApproval = "not_archived" THEN
      {"approval_not_archived"} ELSE {})
    \cup (IF GraphBackedAuthority /\ ~generationPinMatches THEN {"generation_pin_mismatch"} ELSE {})
    \cup (IF evidenceArtifactsFresh THEN {} ELSE {"evidence_artifacts_stale"})
    \cup (IF pactCoordinationAgree THEN {} ELSE {"pact_coordination_disagreement"})
    \cup (IF codeClaims /\ ~gitEvidence THEN {"missing_git_evidence"} ELSE {})

RuntimeLiveChecks ==
  (IF fileFresh THEN {} ELSE {"reload pact.yaml"})
    \cup (IF codeClaims THEN {"git status", "focused tests"} ELSE {})
    \cup (IF KnownAuthority THEN {} ELSE {"select graph authority surface"})
    \cup (IF EventPlaneRequested /\ ~eventPlaneAuthorityAllowed THEN {"use sync-root graph projection"} ELSE {})
    \cup (IF GraphBackedAuthority /\ ~graphReady THEN
      {IF projectionAuthority = "published_generation_evidence"
       THEN "verify exact published generation" ELSE "run graph readiness check"}
      ELSE {})
    \cup (IF GraphBackedAuthority /\ ~graphFresh THEN
      {IF projectionAuthority = "published_generation_evidence"
       THEN "verify exact published generation" ELSE "refresh graph projection"}
      ELSE {})
    \cup (IF EventPlaneAuthority /\ ~intentEventFresh THEN {"refresh pact intent event"} ELSE {})
    \cup (IF EventPlaneAuthority /\ ~intentDigestMatches THEN {"refresh pact intent digest"} ELSE {})
    \cup (IF EventPlaneAuthority /\ ~worktreeHeadMatches THEN {"refresh pact intent head"} ELSE {})
    \cup (IF EventPlaneAuthority /\ ~worktreeClean THEN {"clean worktree"} ELSE {})
    \cup (IF ledgerFresh THEN {} ELSE {"refresh decision ledger"})
    \cup (IF ledgerFresh /\ ledgerApproval = "absent" THEN
      {"record an operator decision with okg pact decide"} ELSE {})
    \cup (IF ledgerFresh /\ ledgerApproval = "not_archived" THEN
      {"archive the approved change"} ELSE {})
    \cup (IF GraphBackedAuthority /\ ~generationPinMatches THEN
      {IF projectionAuthority = "published_generation_evidence"
       THEN "reconcile published generation pin" ELSE "repin graph generation"}
      ELSE {})
    \cup (IF evidenceArtifactsFresh THEN {} ELSE {"rerun evidence checks"})
    \cup (IF pactCoordinationAgree THEN {} ELSE {"reconcile pact and coordination state"})

CurrentAuthority ==
  /\ fileFresh
  /\ KnownAuthority
  /\ (~GraphBackedAuthority \/ (graphReady /\ graphFresh /\ generationPinMatches))
  /\ (~EventPlaneAuthority \/ (
       intentEventFresh /\ intentDigestMatches /\ worktreeHeadMatches /\ worktreeClean
     ))
  /\ ledgerFresh
  /\ ledgerApproval \in {"approved", "not_judged"}
  /\ evidenceArtifactsFresh
  /\ pactCoordinationAgree
  /\ (~codeClaims \/ gitEvidence)

RuntimeAccepted == RuntimeBlockers = {}

AuthorityAccepted == accepted

Init ==
  /\ fileFresh \in BOOLEAN
  /\ graphRequired \in BOOLEAN
  /\ graphReady \in BOOLEAN
  /\ graphFresh \in BOOLEAN
  /\ projectionAuthority \in ProjectionAuthorities
  /\ eventPlaneAuthorityAllowed \in BOOLEAN
  /\ intentEventFresh \in BOOLEAN
  /\ intentDigestMatches \in BOOLEAN
  /\ worktreeHeadMatches \in BOOLEAN
  /\ worktreeClean \in BOOLEAN
  /\ ledgerFresh \in BOOLEAN
  /\ ledgerApproval \in LedgerApprovals
  /\ generationPinMatches \in BOOLEAN
  /\ evidenceArtifactsFresh \in BOOLEAN
  /\ pactCoordinationAgree \in BOOLEAN
  /\ codeClaims \in BOOLEAN
  /\ gitEvidence \in BOOLEAN
  /\ action \in Actions
  /\ blockers = RuntimeBlockers
  /\ requiredLiveChecks = RuntimeLiveChecks
  /\ accepted = RuntimeAccepted

Next == UNCHANGED vars

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ fileFresh \in BOOLEAN
  /\ graphRequired \in BOOLEAN
  /\ graphReady \in BOOLEAN
  /\ graphFresh \in BOOLEAN
  /\ projectionAuthority \in ProjectionAuthorities
  /\ eventPlaneAuthorityAllowed \in BOOLEAN
  /\ intentEventFresh \in BOOLEAN
  /\ intentDigestMatches \in BOOLEAN
  /\ worktreeHeadMatches \in BOOLEAN
  /\ worktreeClean \in BOOLEAN
  /\ ledgerFresh \in BOOLEAN
  /\ ledgerApproval \in LedgerApprovals
  /\ generationPinMatches \in BOOLEAN
  /\ evidenceArtifactsFresh \in BOOLEAN
  /\ pactCoordinationAgree \in BOOLEAN
  /\ codeClaims \in BOOLEAN
  /\ gitEvidence \in BOOLEAN
  /\ action \in Actions
  /\ blockers \subseteq BlockerNames
  /\ requiredLiveChecks \subseteq CheckNames
  /\ accepted \in BOOLEAN

\* DEFINITIONAL SANITY-CHECK (not an independent safety proof). Init sets
\* accepted = RuntimeAccepted and RuntimeAccepted == (RuntimeBlockers = {}),
\* with blockers = RuntimeBlockers and Next == UNCHANGED. This invariant just
\* restates that initialization, so it can never fail by construction. It
\* documents the accepted/blockers correspondence; the real safety content
\* lives in the No*Authority and *RequireCurrentAuthority invariants below.
RuntimeDecisionMatchesBlockers ==
  accepted <=> blockers = {}

\* DEFINITIONAL SANITY-CHECK (not an independent safety proof). Restates the
\* Init binding blockers = RuntimeBlockers under Next == UNCHANGED; true by
\* construction. Documents that the recorded blocker set equals the operator,
\* it does not constrain runtime behavior.
BlockersReflectStaleSurfaces ==
  blockers = RuntimeBlockers

\* DEFINITIONAL SANITY-CHECK (not an independent safety proof). Restates the
\* Init binding requiredLiveChecks = RuntimeLiveChecks under Next == UNCHANGED;
\* true by construction. Documents the live-check derivation, it does not
\* constrain runtime behavior.
LiveChecksReflectStaleSurfaces ==
  requiredLiveChecks = RuntimeLiveChecks

FreshAuthorityCanAuthorize ==
  CurrentAuthority => AuthorityAccepted

NoStaleFileAuthority ==
  ~fileFresh => ~AuthorityAccepted

NoStaleGraphAuthority ==
  GraphBackedAuthority /\ ~(graphReady /\ graphFresh) => ~AuthorityAccepted

NoUnauthorizedEventPlaneAuthority ==
  EventPlaneRequested /\ ~eventPlaneAuthorityAllowed => ~AuthorityAccepted

NoStaleWorktreeIntentAuthority ==
  EventPlaneAuthority /\ ~(
    intentEventFresh /\ intentDigestMatches /\ worktreeHeadMatches /\ worktreeClean
  ) => ~AuthorityAccepted

NoStaleLedgerAuthority ==
  ~ledgerFresh => ~AuthorityAccepted

\* A ledger that ANSWERED and reported that no approval binds the current
\* intent digest cannot authorize the merge. Separate from
\* NoStaleLedgerAuthority: that one covers a ledger the gate could not read,
\* this one covers a ledger the gate read whose answer was no.
NoUnapprovedLedgerAuthority ==
  ledgerFresh /\ ledgerApproval = "absent" => ~AuthorityAccepted

\* An approval binding the current digest still cannot authorize the merge
\* while the change is unarchived. Distinct from NoUnapprovedLedgerAuthority
\* because the repair is the archive step, not a new decision.
NoUnarchivedApprovalAuthority ==
  ledgerFresh /\ ledgerApproval = "not_archived" => ~AuthorityAccepted

NoMismatchedGenerationAuthority ==
  GraphBackedAuthority /\ ~generationPinMatches => ~AuthorityAccepted

NoStaleEvidenceAuthority ==
  ~evidenceArtifactsFresh => ~AuthorityAccepted

NoCoordinationDisagreementAuthority ==
  ~pactCoordinationAgree => ~AuthorityAccepted

CodeClaimsRequireGitEvidence ==
  codeClaims /\ ~gitEvidence => ~AuthorityAccepted

ReviewMergeTaskDoneRequireCurrentAuthority ==
  action \in Actions /\ AuthorityAccepted => CurrentAuthority

WorkflowAuthorityContract ==
  /\ RuntimeDecisionMatchesBlockers
  /\ BlockersReflectStaleSurfaces
  /\ LiveChecksReflectStaleSurfaces
  /\ FreshAuthorityCanAuthorize
  /\ NoStaleFileAuthority
  /\ NoStaleGraphAuthority
  /\ NoUnauthorizedEventPlaneAuthority
  /\ NoStaleWorktreeIntentAuthority
  /\ NoStaleLedgerAuthority
  /\ NoUnapprovedLedgerAuthority
  /\ NoUnarchivedApprovalAuthority
  /\ NoMismatchedGenerationAuthority
  /\ NoStaleEvidenceAuthority
  /\ NoCoordinationDisagreementAuthority
  /\ CodeClaimsRequireGitEvidence
  /\ ReviewMergeTaskDoneRequireCurrentAuthority

====
