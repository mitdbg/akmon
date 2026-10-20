---------------------- MODULE WorktreeBranchSafety ----------------------
EXTENDS Naturals, Integers, FiniteSets, TLC

\* Each guard is TRUE in the proof configuration.  The retained witness
\* configurations turn off exactly one guard and must expose its named hazard.
CONSTANTS
    GuardDefaultIsolation,
    GuardContributionOverlayIsolation,
    GuardTransactionRecovery,
    GuardCommittedOutboxRecovery,
    GuardProjectionClosure,
    GuardLifecycleFencing,
    GuardCapabilityFencing

PreviewBranches == {"branch_a", "branch_b"}
AllBranches == PreviewBranches \cup {"default"}
TransactionStates == {
    "idle", "reserved", "staged", "gated", "committed",
    "effect_released", "finalized", "blocked", "aborted", "superseded"
}
PublishedStates == {"committed", "effect_released", "finalized"}
RecoverableStates == TransactionStates \ {"idle"}
CrashPoints == (TransactionStates \ {"idle"}) \cup {"none"}
Facts == {"base", "preview_a", "preview_b", "trunk", "event_0"}
BaseFacts == {"base", "event_0"}
CleanTrunkFacts == {"base", "trunk", "event_0"}
PreviewOnlyFacts == {"preview_a", "preview_b"}
NoLease == -1
LeaseValues == {NoLease, 0, 1}
ContributionTokens == {
    "base:source_s:record_x", "base:source_t:record_x", "event:ledger:0",
    "branch_a:source_s:preview_a", "branch_b:source_s:preview_b"
}
BaseContributionTokens == {
    "base:source_s:record_x", "base:source_t:record_x", "event:ledger:0"
}
SourceClasses == {
    "branch_projectable", "canonical_append_ledger", "external_snapshot"
}
AllowedBranchSourceClasses == {"branch_projectable"}

Other(branch) == IF branch = "branch_a" THEN "branch_b" ELSE "branch_a"
ExpectedOverlay(branch) ==
    IF branch = "branch_a" THEN {"preview_a"} ELSE {"preview_b"}
ExpectedContribution(branch) ==
    IF branch = "branch_a"
    THEN {"branch_a:source_s:preview_a"}
    ELSE {"branch_b:source_s:preview_b"}
ExpectedTombstone(branch) ==
    IF branch = "branch_a" THEN {"base:source_s:record_x"} ELSE {}
OperationDigest(branch) ==
    IF branch = "branch_a" THEN "capsule_digest_a" ELSE "capsule_digest_b"
OperationId(branch) ==
    IF branch = "branch_a" THEN "operation_a" ELSE "operation_b"
EffectId(branch) ==
    IF branch = "branch_a" THEN "effect_a" ELSE "effect_b"

\* This is the complete mutation capsule shared by enqueue, body, stage,
\* promotion, effects, refresh, and GC.  DOMAIN equality makes field removal a
\* refusal instead of a default-value fallback.
CapsuleFields == {
    "deployment_uuid",
    "lifecycle_activation_epoch",
    "activation_receipt_digest",
    "database_writer_epoch",
    "dbos_application_version",
    "worktree_write_capability_epoch",
    "branch_incarnation_uuid",
    "binding_epoch",
    "base_generation_id",
    "worktree_incarnation",
    "git_repository_id",
    "commit_oid",
    "content_manifest_digest",
    "catalog_digest",
    "source_config_digest",
    "producer_build_digests",
    "determinism_artifact_digests",
    "external_snapshot_tokens",
    "canonical_event_ledger_offset",
    "expected_parent_checkpoint",
    "operation_id",
    "capsule_digest",
    "release_manifest_digest",
    "evidence_digest",
    "dependency_lock_digest",
    "operation_class",
    "capability_mode",
    "canary_issuer",
    "canary_issued_at",
    "canary_expiry",
    "change_control_identity"
}

Capsule(branch) == [
    deployment_uuid |-> "deployment_1",
    lifecycle_activation_epoch |-> 0,
    activation_receipt_digest |-> "receipt_0",
    database_writer_epoch |-> 0,
    dbos_application_version |-> "dbos_0",
    worktree_write_capability_epoch |-> 0,
    branch_incarnation_uuid |-> branch,
    binding_epoch |-> 0,
    base_generation_id |-> 0,
    worktree_incarnation |->
        IF branch = "branch_a" THEN "worktree_a_1" ELSE "worktree_b_1",
    git_repository_id |-> "repository_1",
    commit_oid |-> IF branch = "branch_a" THEN "commit_a" ELSE "commit_b",
    content_manifest_digest |-> "clean_digest",
    catalog_digest |-> "catalog_1",
    source_config_digest |-> "sources_1",
    producer_build_digests |-> "build_1",
    determinism_artifact_digests |-> "determinism_1",
    external_snapshot_tokens |-> "snapshot_0",
    canonical_event_ledger_offset |-> 0,
    expected_parent_checkpoint |-> 0,
    operation_id |-> IF branch = "branch_a" THEN "operation_a" ELSE "operation_b",
    capsule_digest |-> OperationDigest(branch),
    release_manifest_digest |-> "release_1",
    evidence_digest |-> "evidence_1",
    dependency_lock_digest |-> "lock_1",
    operation_class |-> "publish",
    capability_mode |-> "scoped-canary",
    canary_issuer |-> "controller",
    canary_issued_at |-> 0,
    canary_expiry |-> 2,
    change_control_identity |-> "change_993"
]

CompleteCapsule(capsule) == DOMAIN capsule = CapsuleFields
\* OperationDigest abstracts the deterministic hash of every field in
\* CapsulePayloadFields; the digest itself is transported in the capsule.
CapsulePayloadFields == CapsuleFields \ {"capsule_digest"}
CapsuleDigestValid(capsule, branch) ==
    /\ DOMAIN capsule = CapsulePayloadFields \cup {"capsule_digest"}
    /\ capsule.capsule_digest = OperationDigest(branch)

VARIABLES
    authority,
    capsules,
    transactions,
    sharedLifecycleHolders,
    exclusiveLifecycleFence,
    stagedFacts,
    overlays,
    branchHeads,
    effectsReleased,
    publishCounts,
    defaultFacts,
    defaultHead,
    trunk,
    branchLifecycle,
    dbNow,
    capabilityRevoked,
    processLaunchAttestations,
    priorAuthority,
    lastControllerAction,
    hazards

vars == <<
    authority,
    capsules,
    transactions,
    sharedLifecycleHolders,
    exclusiveLifecycleFence,
    stagedFacts,
    overlays,
    branchHeads,
    effectsReleased,
    publishCounts,
    defaultFacts,
    defaultHead,
    trunk,
    branchLifecycle,
    dbNow,
    capabilityRevoked,
    processLaunchAttestations,
    priorAuthority,
    lastControllerAction,
    hazards
>>

InitialTransaction(branch) == [
    state |-> "idle",
    receipt_state |-> "idle",
    expected_parent |-> 0,
    published |-> FALSE,
    finalized |-> FALSE,
    commit_authorized |-> FALSE,
    effect_authorized |-> FALSE,
    effect_count |-> 0,
    outbox_state |-> "none",
    effect_id |-> EffectId(branch),
    delivery_attempts |-> 0,
    sink_dedup_recorded |-> FALSE,
    committed_capsule_digest |-> "none",
    committed_lifecycle_epoch |-> NoLease,
    committed_receipt_digest |-> "none",
    committed_writer_epoch |-> NoLease,
    committed_dbos_version |-> "none",
    committed_capability_epoch |-> NoLease,
    terminal_result |-> "none",
    reservation_released |-> FALSE,
    operation_id |-> OperationId(branch),
    capsule_digest |-> OperationDigest(branch),
    presented_digest |-> OperationDigest(branch),
    logical_idempotency_key |-> "logical_batch_1",
    reuse_probed |-> FALSE,
    reuse_refused |-> FALSE,
    worker_up |-> TRUE,
    recovery_attempts |-> 0,
    crash_point |-> "none",
    cursor |-> 0,
    watermark |-> 0,
    progress |-> 0,
    idempotency_claimed |-> FALSE,
    contributions |-> {},
    tombstones |-> {},
    source_classes |-> {},
    head_owner |-> "none",
    publish_parent |-> NoLease,
    expected_parent_refused |-> FALSE
]

InitialLifecycle == [
    epoch |-> 0,
    live |-> TRUE,
    retired |-> FALSE,
    refresh_lease |-> NoLease,
    gc_lease |-> NoLease,
    refresh_ready |-> FALSE,
    durable_jobs |-> 0,
    read_leases |-> 0,
    collected |-> FALSE,
    path_reuse_refused |-> FALSE,
    scenario |-> "none"
]

InitialHazards == [
    default_isolation |-> FALSE,
    contribution_overlay |-> FALSE,
    transaction_recovery |-> FALSE,
    projection_closure |-> FALSE,
    lifecycle_fencing |-> FALSE,
    capability_fencing |-> FALSE,
    branch_blind_cursor |-> FALSE,
    cross_branch_idempotency |-> FALSE,
    inherited_delete |-> FALSE,
    cross_source_tombstone |-> FALSE,
    source_class_isolation |-> FALSE,
    crash_duplicate |-> FALSE,
    out_of_order_publish |-> FALSE,
    graph_union_landing |-> FALSE,
    refresh_gc_race |-> FALSE
]

Init ==
    /\ authority = [
        deployment_uuid |-> "deployment_1",
        lifecycle_epoch |-> 0,
        receipt_digest |-> "receipt_0",
        writer_epoch |-> 0,
        dbos_version |-> "dbos_0",
        capability_epoch |-> 0,
        mode |-> "scoped-canary",
        release_manifest_digest |-> "release_1",
        evidence_digest |-> "evidence_1",
        dependency_lock_digest |-> "lock_1"
        ]
    /\ capsules = [branch \in PreviewBranches |-> Capsule(branch)]
    /\ transactions = [branch \in PreviewBranches |-> InitialTransaction(branch)]
    /\ sharedLifecycleHolders = {}
    /\ exclusiveLifecycleFence = FALSE
    /\ stagedFacts = [branch \in PreviewBranches |-> {}]
    /\ overlays = [branch \in PreviewBranches |-> {}]
    /\ branchHeads = [branch \in PreviewBranches |-> 0]
    /\ effectsReleased = [branch \in PreviewBranches |-> FALSE]
    /\ publishCounts = [branch \in PreviewBranches |-> 0]
    /\ defaultFacts = BaseFacts
    /\ defaultHead = 0
    /\ trunk = [
        merged |-> FALSE,
        merge_order |-> "none",
        projected |-> FALSE,
        capsule_complete |-> TRUE,
        sources_classified |-> TRUE,
        expected_parent |-> 0,
        sync_root |-> "sync_root",
        catalog_closed |-> TRUE,
        event_offset_pinned |-> TRUE,
        external_snapshot_pinned |-> TRUE,
        git_snapshot_compatible |-> TRUE,
        projected_from |-> "none"
        ]
    /\ branchLifecycle = [branch \in PreviewBranches |-> InitialLifecycle]
    /\ dbNow = 0
    /\ capabilityRevoked = FALSE
    /\ processLaunchAttestations = 0
    /\ priorAuthority = authority
    /\ lastControllerAction = "none"
    /\ hazards = InitialHazards

CapsuleMatchesAuthority(branch) ==
    LET capsule == capsules[branch] IN
    /\ CompleteCapsule(capsule)
    /\ CapsuleDigestValid(capsule, branch)
    /\ capsule.deployment_uuid = authority.deployment_uuid
    /\ capsule.lifecycle_activation_epoch = authority.lifecycle_epoch
    /\ capsule.activation_receipt_digest = authority.receipt_digest
    /\ capsule.database_writer_epoch = authority.writer_epoch
    /\ capsule.dbos_application_version = authority.dbos_version
    /\ capsule.worktree_write_capability_epoch = authority.capability_epoch
    /\ capsule.branch_incarnation_uuid = branch
    /\ capsule.binding_epoch = branchLifecycle[branch].epoch
    /\ capsule.base_generation_id = 0
    /\ capsule.worktree_incarnation =
        IF branch = "branch_a" THEN "worktree_a_1" ELSE "worktree_b_1"
    /\ capsule.git_repository_id = "repository_1"
    /\ capsule.commit_oid =
        IF branch = "branch_a" THEN "commit_a" ELSE "commit_b"
    /\ capsule.content_manifest_digest = "clean_digest"
    /\ capsule.catalog_digest = "catalog_1"
    /\ capsule.source_config_digest = "sources_1"
    /\ capsule.producer_build_digests = "build_1"
    /\ capsule.determinism_artifact_digests = "determinism_1"
    /\ capsule.external_snapshot_tokens = "snapshot_0"
    /\ capsule.canonical_event_ledger_offset = 0
    /\ capsule.expected_parent_checkpoint = branchHeads[branch]
    /\ capsule.operation_id =
        IF branch = "branch_a" THEN "operation_a" ELSE "operation_b"
    /\ capsule.capsule_digest = OperationDigest(branch)
    /\ capsule.release_manifest_digest = authority.release_manifest_digest
    /\ capsule.evidence_digest = authority.evidence_digest
    /\ capsule.dependency_lock_digest = authority.dependency_lock_digest

CapabilityValid(branch) ==
    LET capsule == capsules[branch] IN
    /\ authority.mode = "scoped-canary"
    /\ capsule.capability_mode = authority.mode
    /\ ~capabilityRevoked
    /\ branch \in PreviewBranches
    /\ capsule.operation_class = "publish"
    /\ capsule.canary_issuer = "controller"
    /\ capsule.canary_issued_at <= dbNow
    /\ dbNow < capsule.canary_expiry
    /\ capsule.change_control_identity = "change_993"

CompletePublicationCAS(branch) ==
    /\ transactions[branch].state = "gated"
    /\ transactions[branch].receipt_state = "gated"
    /\ transactions[branch].expected_parent = branchHeads[branch]
    /\ capsules[branch].expected_parent_checkpoint = branchHeads[branch]
    /\ transactions[branch].operation_id = capsules[branch].operation_id
    /\ transactions[branch].capsule_digest = capsules[branch].capsule_digest
    /\ transactions[branch].presented_digest = capsules[branch].capsule_digest
    /\ branch \in sharedLifecycleHolders
    /\ ~exclusiveLifecycleFence
    /\ CapsuleMatchesAuthority(branch)
    /\ stagedFacts[branch] = ExpectedOverlay(branch)
    /\ branchLifecycle[branch].live
    /\ ~branchLifecycle[branch].retired
    /\ branchLifecycle[branch].durable_jobs = 1

\* Once publish atomically releases the outbox, its immutable commit receipt --
\* not mutable current lifecycle/capability authority -- authorizes recovery.
CommittedOutboxAuthorized(branch) ==
    /\ transactions[branch].published
    /\ transactions[branch].commit_authorized
    /\ transactions[branch].operation_id = capsules[branch].operation_id
    /\ transactions[branch].effect_id = EffectId(branch)
    /\ transactions[branch].committed_capsule_digest =
        transactions[branch].capsule_digest
    /\ transactions[branch].committed_capsule_digest = OperationDigest(branch)
    /\ transactions[branch].committed_lifecycle_epoch =
        capsules[branch].lifecycle_activation_epoch
    /\ transactions[branch].committed_receipt_digest =
        capsules[branch].activation_receipt_digest
    /\ transactions[branch].committed_writer_epoch =
        capsules[branch].database_writer_epoch
    /\ transactions[branch].committed_dbos_version =
        capsules[branch].dbos_application_version
    /\ transactions[branch].committed_capability_epoch =
        capsules[branch].worktree_write_capability_epoch

DeliveryAuthorization(branch) ==
    IF GuardCommittedOutboxRecovery
    THEN CommittedOutboxAuthorized(branch)
    ELSE /\ CapsuleMatchesAuthority(branch)
         /\ CapabilityValid(branch)

Reserve(branch) ==
    /\ branch = "branch_a"
    /\ transactions[branch].state = "idle"
    /\ transactions[branch].worker_up
    /\ transactions[branch].operation_id = capsules[branch].operation_id
    /\ transactions[branch].capsule_digest = capsules[branch].capsule_digest
    /\ transactions[branch].presented_digest = capsules[branch].capsule_digest
    /\ branchLifecycle[branch].durable_jobs = 0
    /\ branchLifecycle[branch].scenario = "none"
    /\ branchLifecycle[branch].live
    /\ ~branchLifecycle[branch].retired
    /\ ~branchLifecycle[branch].collected
    /\ ~trunk.merged
    /\ lastControllerAction = "none"
    /\ ~exclusiveLifecycleFence
    /\ CapsuleMatchesAuthority(branch)
    /\ GuardCapabilityFencing => CapabilityValid(branch)
    /\ transactions' = [transactions EXCEPT
        ![branch].state = "reserved",
        ![branch].receipt_state = "reserved"
        ]
    /\ sharedLifecycleHolders' = sharedLifecycleHolders \cup {branch}
    /\ branchLifecycle' = [branchLifecycle EXCEPT ![branch].durable_jobs = 1]
    /\ UNCHANGED <<
        authority, capsules, exclusiveLifecycleFence, stagedFacts, overlays,
        branchHeads, effectsReleased, publishCounts, defaultFacts, defaultHead,
        trunk, dbNow, capabilityRevoked,
        processLaunchAttestations, priorAuthority, lastControllerAction, hazards
        >>

Stage(branch) ==
    /\ branch \in PreviewBranches
    /\ transactions[branch].state = "reserved"
    /\ transactions[branch].worker_up
    /\ branch \in sharedLifecycleHolders
    /\ transactions' = [transactions EXCEPT
        ![branch].state = "staged",
        ![branch].receipt_state = "staged",
        ![branch].cursor = IF GuardTransactionRecovery THEN @ ELSE 1,
        ![branch].watermark = IF GuardTransactionRecovery THEN @ ELSE 1,
        ![branch].progress = IF GuardTransactionRecovery THEN @ ELSE 1,
        ![branch].idempotency_claimed =
            IF GuardTransactionRecovery THEN @ ELSE TRUE,
        ![branch].outbox_state = "quarantined"
        ]
    /\ stagedFacts' = [stagedFacts EXCEPT ![branch] = ExpectedOverlay(branch)]
    /\ overlays' = IF GuardTransactionRecovery
        THEN overlays
        ELSE [overlays EXCEPT ![branch] = ExpectedOverlay(branch)]
    /\ hazards' = [hazards EXCEPT
        !.transaction_recovery = @ \/ ~GuardTransactionRecovery,
        !.branch_blind_cursor = @ \/ ~GuardTransactionRecovery,
        !.cross_branch_idempotency = @ \/ ~GuardTransactionRecovery
        ]
    /\ UNCHANGED <<
        authority, capsules, sharedLifecycleHolders, exclusiveLifecycleFence,
        branchHeads, effectsReleased, publishCounts, defaultFacts, defaultHead,
        trunk, branchLifecycle, dbNow, capabilityRevoked,
        processLaunchAttestations, priorAuthority, lastControllerAction
        >>

Gate(branch) ==
    /\ branch \in PreviewBranches
    /\ transactions[branch].state = "staged"
    /\ transactions[branch].worker_up
    /\ transactions' = [transactions EXCEPT
        ![branch].state = "gated",
        ![branch].receipt_state = "gated"
        ]
    /\ UNCHANGED <<
        authority, capsules, sharedLifecycleHolders, exclusiveLifecycleFence,
        stagedFacts, overlays, branchHeads, effectsReleased, publishCounts,
        defaultFacts, defaultHead, trunk, branchLifecycle, dbNow,
        capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction, hazards
        >>

Commit(branch) ==
    /\ branch \in PreviewBranches
    /\ CompletePublicationCAS(branch)
    /\ transactions[branch].worker_up
    /\ GuardCapabilityFencing => CapabilityValid(branch)
    /\ transactions' = [transactions EXCEPT
        ![branch].state = "committed",
        ![branch].receipt_state = "committed",
        ![branch].published = TRUE,
        ![branch].commit_authorized = CapabilityValid(branch),
        ![branch].cursor = 1,
        ![branch].watermark = 1,
        ![branch].progress = 1,
        ![branch].idempotency_claimed = TRUE,
        ![branch].committed_capsule_digest = capsules[branch].capsule_digest,
        ![branch].committed_lifecycle_epoch = authority.lifecycle_epoch,
        ![branch].committed_receipt_digest = authority.receipt_digest,
        ![branch].committed_writer_epoch = authority.writer_epoch,
        ![branch].committed_dbos_version = authority.dbos_version,
        ![branch].committed_capability_epoch = authority.capability_epoch,
        ![branch].contributions = ExpectedContribution(branch),
        ![branch].tombstones = IF GuardContributionOverlayIsolation
            THEN ExpectedTombstone(branch) ELSE BaseContributionTokens,
        ![branch].source_classes = IF GuardContributionOverlayIsolation
            THEN AllowedBranchSourceClasses ELSE SourceClasses,
        ![branch].head_owner = "operation",
        ![branch].publish_parent = branchHeads[branch],
        ![branch].outbox_state = "released",
        ![Other(branch)].contributions = IF GuardContributionOverlayIsolation
            THEN @ ELSE @ \cup ExpectedContribution(branch),
        ![Other(branch)].idempotency_claimed = IF GuardTransactionRecovery
            THEN @ ELSE TRUE
        ]
    /\ overlays' = [overlays EXCEPT
        ![branch] = stagedFacts[branch],
        ![Other(branch)] = IF GuardContributionOverlayIsolation
            THEN @ ELSE @ \cup stagedFacts[branch]
        ]
    /\ branchHeads' = [branchHeads EXCEPT ![branch] = @ + 1]
    /\ publishCounts' = [publishCounts EXCEPT ![branch] = @ + 1]
    /\ defaultFacts' = IF GuardDefaultIsolation
        THEN defaultFacts ELSE defaultFacts \cup stagedFacts[branch]
    /\ hazards' = [hazards EXCEPT
        !.default_isolation = @ \/ ~GuardDefaultIsolation,
        !.contribution_overlay = @ \/ ~GuardContributionOverlayIsolation,
        !.inherited_delete = @ \/ ~GuardContributionOverlayIsolation,
        !.cross_source_tombstone = @ \/ ~GuardContributionOverlayIsolation,
        !.source_class_isolation = @ \/ ~GuardContributionOverlayIsolation,
        !.cross_branch_idempotency = @ \/ ~GuardTransactionRecovery,
        !.capability_fencing = @ \/ ~CapabilityValid(branch)
        ]
    /\ UNCHANGED <<
        authority, capsules, sharedLifecycleHolders, exclusiveLifecycleFence,
        stagedFacts, effectsReleased, defaultHead, trunk, branchLifecycle,
        dbNow, capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction
        >>

ClaimOutbox(branch) ==
    /\ branch \in PreviewBranches
    /\ transactions[branch].state = "committed"
    /\ transactions[branch].outbox_state = "released"
    /\ transactions[branch].worker_up
    /\ CommittedOutboxAuthorized(branch)
    /\ transactions' = [transactions EXCEPT
        ![branch].outbox_state = "claimed",
        ![branch].delivery_attempts = @ + 1
        ]
    /\ UNCHANGED <<
        authority, capsules, sharedLifecycleHolders, exclusiveLifecycleFence,
        stagedFacts, overlays, branchHeads, effectsReleased, publishCounts,
        defaultFacts, defaultHead, trunk, branchLifecycle, dbNow,
        capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction, hazards
        >>

RetryDeliveryClaim(branch) ==
    /\ branch = "branch_a"
    /\ transactions[branch].state = "committed"
    /\ transactions[branch].outbox_state = "claimed"
    /\ transactions[branch].worker_up
    /\ transactions[branch].recovery_attempts > 0
    /\ transactions[branch].delivery_attempts < 2
    /\ transactions' = [transactions EXCEPT
        ![branch].delivery_attempts = @ + 1
        ]
    /\ UNCHANGED <<
        authority, capsules, sharedLifecycleHolders, exclusiveLifecycleFence,
        stagedFacts, overlays, branchHeads, effectsReleased, publishCounts,
        defaultFacts, defaultHead, trunk, branchLifecycle, dbNow,
        capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction, hazards
        >>

DeliverOutbox(branch) ==
    /\ branch \in PreviewBranches
    /\ transactions[branch].state = "committed"
    /\ transactions[branch].outbox_state = "claimed"
    /\ transactions[branch].worker_up
    /\ DeliveryAuthorization(branch)
    /\ transactions' = [transactions EXCEPT
        ![branch].state = "effect_released",
        ![branch].receipt_state = "effect_released",
        ![branch].effect_count = IF @ = 0 THEN 1 ELSE @,
        ![branch].effect_authorized = CommittedOutboxAuthorized(branch),
        ![branch].sink_dedup_recorded = TRUE,
        ![branch].outbox_state = "delivered"
        ]
    /\ effectsReleased' = [effectsReleased EXCEPT ![branch] = TRUE]
    /\ UNCHANGED <<
        authority, capsules, sharedLifecycleHolders, exclusiveLifecycleFence,
        stagedFacts, overlays, branchHeads, publishCounts, defaultFacts,
        defaultHead, trunk, branchLifecycle, dbNow, capabilityRevoked,
        processLaunchAttestations, priorAuthority, lastControllerAction, hazards
        >>

RetryDeliveredEffect(branch) ==
    /\ branch = "branch_a"
    /\ transactions[branch].state = "effect_released"
    /\ transactions[branch].outbox_state = "delivered"
    /\ transactions[branch].worker_up
    /\ transactions[branch].recovery_attempts > 0
    /\ transactions[branch].delivery_attempts < 2
    /\ CommittedOutboxAuthorized(branch)
    /\ transactions' = [transactions EXCEPT
        ![branch].delivery_attempts = @ + 1,
        ![branch].effect_count = IF @ = 0 THEN 1 ELSE @,
        ![branch].sink_dedup_recorded = TRUE
        ]
    /\ UNCHANGED <<
        authority, capsules, sharedLifecycleHolders, exclusiveLifecycleFence,
        stagedFacts, overlays, branchHeads, effectsReleased, publishCounts,
        defaultFacts, defaultHead, trunk, branchLifecycle, dbNow,
        capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction, hazards
        >>

AcknowledgeDelivery(branch) ==
    /\ branch \in PreviewBranches
    /\ transactions[branch].state = "effect_released"
    /\ transactions[branch].outbox_state = "delivered"
    /\ transactions[branch].worker_up
    /\ CommittedOutboxAuthorized(branch)
    /\ transactions' = [transactions EXCEPT ![branch].outbox_state = "acked"]
    /\ UNCHANGED <<
        authority, capsules, sharedLifecycleHolders, exclusiveLifecycleFence,
        stagedFacts, overlays, branchHeads, effectsReleased, publishCounts,
        defaultFacts, defaultHead, trunk, branchLifecycle, dbNow,
        capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction, hazards
        >>

Finalize(branch) ==
    /\ branch \in PreviewBranches
    /\ transactions[branch].state = "effect_released"
    /\ transactions[branch].outbox_state = "acked"
    /\ transactions[branch].worker_up
    /\ CommittedOutboxAuthorized(branch)
    /\ transactions' = [transactions EXCEPT
        ![branch].state = "finalized",
        ![branch].receipt_state = "finalized",
        ![branch].finalized = TRUE,
        ![branch].terminal_result = "published",
        ![branch].reservation_released = TRUE
        ]
    /\ sharedLifecycleHolders' = sharedLifecycleHolders \ {branch}
    /\ branchLifecycle' = [branchLifecycle EXCEPT ![branch].durable_jobs = 0]
    /\ UNCHANGED <<
        authority, capsules, exclusiveLifecycleFence, stagedFacts, overlays,
        branchHeads, effectsReleased, publishCounts, defaultFacts, defaultHead,
        trunk, dbNow, capabilityRevoked,
        processLaunchAttestations, priorAuthority, lastControllerAction, hazards
        >>

RecoverAbort(branch) ==
    /\ branch \in PreviewBranches
    /\ transactions[branch].state \in {"reserved", "staged", "gated"}
    /\ transactions[branch].worker_up
    /\ (~CompletePublicationCAS(branch) \/ ~CapabilityValid(branch))
    /\ transactions' = [transactions EXCEPT
        ![branch].state = "aborted",
        ![branch].receipt_state = "aborted",
        ![branch].outbox_state = "discarded",
        ![branch].terminal_result = "aborted",
        ![branch].reservation_released = TRUE,
        ![branch].expected_parent_refused =
            transactions[branch].expected_parent # branchHeads[branch]
        ]
    /\ stagedFacts' = [stagedFacts EXCEPT ![branch] = {}]
    /\ sharedLifecycleHolders' = sharedLifecycleHolders \ {branch}
    /\ branchLifecycle' = [branchLifecycle EXCEPT ![branch].durable_jobs = 0]
    /\ UNCHANGED <<
        authority, capsules, exclusiveLifecycleFence, overlays, branchHeads,
        effectsReleased, publishCounts, defaultFacts, defaultHead, trunk,
        dbNow, capabilityRevoked, processLaunchAttestations,
        priorAuthority, lastControllerAction, hazards
        >>

BlockStaged(branch) ==
    /\ branch = "branch_a"
    /\ transactions[branch].worker_up
    /\ transactions[branch].state = "staged"
    /\ transactions' = [transactions EXCEPT
        ![branch].state = "blocked",
        ![branch].receipt_state = "blocked",
        ![branch].outbox_state = "discarded",
        ![branch].terminal_result = "blocked",
        ![branch].reservation_released = TRUE
        ]
    /\ stagedFacts' = [stagedFacts EXCEPT ![branch] = {}]
    /\ sharedLifecycleHolders' = sharedLifecycleHolders \ {branch}
    /\ branchLifecycle' = [branchLifecycle EXCEPT ![branch].durable_jobs = 0]
    /\ UNCHANGED <<
        authority, capsules, exclusiveLifecycleFence, overlays, branchHeads,
        effectsReleased, publishCounts, defaultFacts, defaultHead, trunk, dbNow,
        capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction, hazards
        >>

SupersedeOutOfOrder(branch) ==
    /\ branch = "branch_a"
    /\ transactions[branch].worker_up
    /\ transactions[branch].state \in {"reserved", "staged", "gated"}
    /\ transactions[branch].expected_parent # branchHeads[branch]
    /\ transactions' = [transactions EXCEPT
        ![branch].state = "superseded",
        ![branch].receipt_state = "superseded",
        ![branch].outbox_state = "discarded",
        ![branch].terminal_result = "superseded",
        ![branch].reservation_released = TRUE,
        ![branch].expected_parent_refused = TRUE
        ]
    /\ stagedFacts' = [stagedFacts EXCEPT ![branch] = {}]
    /\ sharedLifecycleHolders' = sharedLifecycleHolders \ {branch}
    /\ branchLifecycle' = [branchLifecycle EXCEPT ![branch].durable_jobs = 0]
    /\ UNCHANGED <<
        authority, capsules, exclusiveLifecycleFence, overlays, branchHeads,
        effectsReleased, publishCounts, defaultFacts, defaultHead, trunk, dbNow,
        capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction, hazards
        >>

\* A process crash drops only the ephemeral shared-fence holder. Durable
\* transaction, staging, graph, cursor, receipt, and outbox state is replayed.
CrashWorker(branch) ==
    /\ branch = "branch_a"
    /\ transactions[branch].worker_up
    /\ transactions[branch].state \in RecoverableStates
    /\ transactions[branch].recovery_attempts < 2
    /\ transactions' = [transactions EXCEPT
        ![branch].worker_up = FALSE,
        ![branch].crash_point = transactions[branch].state
        ]
    /\ sharedLifecycleHolders' = sharedLifecycleHolders \ {branch}
    /\ UNCHANGED <<
        authority, capsules, exclusiveLifecycleFence, stagedFacts, overlays,
        branchHeads, effectsReleased, publishCounts, defaultFacts, defaultHead,
        trunk, branchLifecycle, dbNow, capabilityRevoked,
        processLaunchAttestations, priorAuthority, lastControllerAction, hazards
        >>

RestartWorker(branch) ==
    /\ branch = "branch_a"
    /\ ~transactions[branch].worker_up
    /\ transactions' = [transactions EXCEPT
        ![branch].worker_up = TRUE,
        ![branch].recovery_attempts = @ + 1
        ]
    /\ UNCHANGED <<
        authority, capsules, sharedLifecycleHolders, exclusiveLifecycleFence,
        stagedFacts, overlays, branchHeads, effectsReleased, publishCounts,
        defaultFacts, defaultHead, trunk, branchLifecycle, dbNow,
        capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction, hazards
        >>

RecoverSharedLifecycleHolder(branch) ==
    /\ branch = "branch_a"
    /\ transactions[branch].worker_up
    /\ transactions[branch].recovery_attempts > 0
    /\ transactions[branch].state \in
        {"reserved", "staged", "gated", "committed", "effect_released"}
    /\ branch \notin sharedLifecycleHolders
    /\ ~exclusiveLifecycleFence
    /\ CapsuleMatchesAuthority(branch)
    /\ CapabilityValid(branch)
    /\ sharedLifecycleHolders' = sharedLifecycleHolders \cup {branch}
    /\ UNCHANGED <<
        authority, capsules, transactions, exclusiveLifecycleFence, stagedFacts,
        overlays, branchHeads, effectsReleased, publishCounts, defaultFacts,
        defaultHead, trunk, branchLifecycle, dbNow, capabilityRevoked,
        processLaunchAttestations, priorAuthority, lastControllerAction, hazards
        >>

ProbeOperationIdReuse(branch) ==
    /\ branch = "branch_a"
    /\ ~transactions[branch].reuse_probed
    /\ transactions' = [transactions EXCEPT
        ![branch].reuse_probed = TRUE,
        ![branch].presented_digest = "forged_capsule_digest",
        ![branch].reuse_refused = GuardTransactionRecovery,
        ![branch].capsule_digest = IF GuardTransactionRecovery
            THEN @ ELSE "forged_capsule_digest"
        ]
    /\ hazards' = [hazards EXCEPT
        !.transaction_recovery = @ \/ ~GuardTransactionRecovery,
        !.cross_branch_idempotency = @ \/ ~GuardTransactionRecovery
        ]
    /\ UNCHANGED <<
        authority, capsules, sharedLifecycleHolders, exclusiveLifecycleFence,
        stagedFacts, overlays, branchHeads, effectsReleased, publishCounts,
        defaultFacts, defaultHead, trunk, branchLifecycle, dbNow,
        capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction
        >>

AdvanceExpectedParent(branch) ==
    /\ branch = "branch_a"
    /\ transactions[branch].state \in {"reserved", "staged", "gated"}
    /\ branchHeads[branch] = 0
    /\ branchHeads' = [branchHeads EXCEPT ![branch] = 1]
    /\ transactions' = [transactions EXCEPT ![branch].head_owner = "newer_operation"]
    /\ UNCHANGED <<
        authority, capsules, sharedLifecycleHolders, exclusiveLifecycleFence,
        stagedFacts, overlays, effectsReleased, publishCounts, defaultFacts,
        defaultHead, trunk, branchLifecycle, dbNow, capabilityRevoked,
        processLaunchAttestations, priorAuthority, lastControllerAction, hazards
        >>

\* This transition exists only in the guard-removal model. It makes the
\* at-most-once graph/delta/effect obligation genuinely load-bearing.
ReplayCompletedWithoutDedup(branch) ==
    /\ ~GuardTransactionRecovery
    /\ branch = "branch_a"
    /\ transactions[branch].state = "finalized"
    /\ transactions[branch].worker_up
    /\ transactions' = [transactions EXCEPT ![branch].effect_count = 2]
    /\ publishCounts' = [publishCounts EXCEPT ![branch] = 2]
    /\ hazards' = [hazards EXCEPT
        !.transaction_recovery = TRUE,
        !.crash_duplicate = TRUE,
        !.out_of_order_publish = TRUE
        ]
    /\ UNCHANGED <<
        authority, capsules, sharedLifecycleHolders, exclusiveLifecycleFence,
        stagedFacts, overlays, branchHeads, effectsReleased, defaultFacts,
        defaultHead, trunk, branchLifecycle, dbNow, capabilityRevoked,
        processLaunchAttestations, priorAuthority, lastControllerAction
        >>

ReplayTerminalResult(branch) ==
    /\ branch = "branch_a"
    /\ transactions[branch].worker_up
    /\ transactions[branch].state \in
        {"finalized", "blocked", "aborted", "superseded"}
    /\ transactions[branch].recovery_attempts < 2
    /\ transactions' = [transactions EXCEPT
        ![branch].recovery_attempts = @ + 1,
        ![branch].crash_point = transactions[branch].state
        ]
    /\ UNCHANGED <<
        authority, capsules, sharedLifecycleHolders, exclusiveLifecycleFence,
        stagedFacts, overlays, branchHeads, effectsReleased, publishCounts,
        defaultFacts, defaultHead, trunk, branchLifecycle, dbNow,
        capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction, hazards
        >>

TickDatabaseTime ==
    /\ dbNow < 2
    /\ \E branch \in PreviewBranches : transactions[branch].state # "idle"
    /\ dbNow' = dbNow + 1
    /\ UNCHANGED <<
        authority, capsules, transactions, sharedLifecycleHolders,
        exclusiveLifecycleFence, stagedFacts, overlays, branchHeads,
        effectsReleased, publishCounts, defaultFacts, defaultHead, trunk,
        branchLifecycle, capabilityRevoked, processLaunchAttestations,
        priorAuthority, lastControllerAction, hazards
        >>

RevokeCapability ==
    /\ ~capabilityRevoked
    /\ \E branch \in PreviewBranches : transactions[branch].state # "idle"
    /\ capabilityRevoked' = TRUE
    /\ authority' = [authority EXCEPT
        !.capability_epoch = 1,
        !.mode = "disabled"
        ]
    /\ UNCHANGED <<
        capsules, transactions, sharedLifecycleHolders, exclusiveLifecycleFence,
        stagedFacts, overlays, branchHeads, effectsReleased, publishCounts,
        defaultFacts, defaultHead, trunk, branchLifecycle, dbNow,
        processLaunchAttestations, priorAuthority, lastControllerAction, hazards
        >>

AcquireExclusiveLifecycleFence ==
    /\ ~exclusiveLifecycleFence
    /\ ~trunk.merged
    /\ lastControllerAction = "none"
    /\ GuardLifecycleFencing => sharedLifecycleHolders = {}
    /\ exclusiveLifecycleFence' = TRUE
    /\ hazards' = [hazards EXCEPT
        !.lifecycle_fencing = @ \/ (sharedLifecycleHolders # {})
        ]
    /\ UNCHANGED <<
        authority, capsules, transactions, sharedLifecycleHolders, stagedFacts,
        overlays, branchHeads, effectsReleased, publishCounts, defaultFacts,
        defaultHead, trunk, branchLifecycle, dbNow, capabilityRevoked,
        processLaunchAttestations, priorAuthority, lastControllerAction
        >>

ControllerRollback ==
    /\ exclusiveLifecycleFence
    /\ authority.lifecycle_epoch = 0
    /\ priorAuthority' = authority
    /\ authority' = [authority EXCEPT
        !.lifecycle_epoch = 1,
        !.receipt_digest = "receipt_1",
        !.writer_epoch = 1,
        !.dbos_version = "dbos_1"
        ]
    /\ lastControllerAction' = "controller_rollback"
    /\ hazards' = [hazards EXCEPT
        !.lifecycle_fencing = @ \/ ~GuardLifecycleFencing
        ]
    /\ UNCHANGED <<
        capsules, transactions, sharedLifecycleHolders, exclusiveLifecycleFence,
        stagedFacts, overlays, branchHeads, effectsReleased, publishCounts,
        defaultFacts, defaultHead, trunk, branchLifecycle, dbNow,
        capabilityRevoked, processLaunchAttestations
        >>

ReleaseExclusiveLifecycleFence ==
    /\ exclusiveLifecycleFence
    /\ exclusiveLifecycleFence' = FALSE
    /\ UNCHANGED <<
        authority, capsules, transactions, sharedLifecycleHolders, stagedFacts,
        overlays, branchHeads, effectsReleased, publishCounts, defaultFacts,
        defaultHead, trunk, branchLifecycle, dbNow, capabilityRevoked,
        processLaunchAttestations, priorAuthority, lastControllerAction, hazards
        >>

SameReceiptServiceRestart ==
    /\ processLaunchAttestations = 0
    /\ \A branch \in PreviewBranches : transactions[branch].state = "idle"
    /\ branchLifecycle["branch_a"].scenario = "none"
    /\ ~trunk.merged
    /\ priorAuthority' = authority
    /\ processLaunchAttestations' = 1
    /\ lastControllerAction' = "same_receipt_restart"
    /\ UNCHANGED <<
        authority, capsules, transactions, sharedLifecycleHolders,
        exclusiveLifecycleFence, stagedFacts, overlays, branchHeads,
        effectsReleased, publishCounts, defaultFacts, defaultHead, trunk,
        branchLifecycle, dbNow, capabilityRevoked, hazards
        >>

AcquireRefreshLease(branch) ==
    /\ branch = "branch_a"
    /\ branchLifecycle[branch].live
    /\ branchLifecycle[branch].scenario = "none"
    /\ ~trunk.merged
    /\ lastControllerAction = "none"
    /\ branchLifecycle[branch].refresh_lease = NoLease
    /\ branchLifecycle' = [branchLifecycle EXCEPT
        ![branch].refresh_lease = branchLifecycle[branch].epoch,
        ![branch].scenario = "refresh"
        ]
    /\ UNCHANGED <<
        authority, capsules, transactions, sharedLifecycleHolders,
        exclusiveLifecycleFence, stagedFacts, overlays, branchHeads,
        effectsReleased, publishCounts, defaultFacts, defaultHead, trunk, dbNow,
        capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction, hazards
        >>

BuildRefreshReplacement(branch) ==
    /\ branch = "branch_a"
    /\ branchLifecycle[branch].refresh_lease = branchLifecycle[branch].epoch
    /\ ~branchLifecycle[branch].refresh_ready
    /\ branchLifecycle' = [branchLifecycle EXCEPT
        ![branch].refresh_ready = TRUE
        ]
    /\ UNCHANGED <<
        authority, capsules, transactions, sharedLifecycleHolders,
        exclusiveLifecycleFence, stagedFacts, overlays, branchHeads,
        effectsReleased, publishCounts, defaultFacts, defaultHead, trunk, dbNow,
        capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction, hazards
        >>

RefreshCutover(branch) ==
    /\ branch = "branch_a"
    /\ branchLifecycle[branch].epoch = 0
    /\ branchLifecycle[branch].refresh_ready
    /\ GuardLifecycleFencing =>
        /\ exclusiveLifecycleFence
        /\ sharedLifecycleHolders = {}
        /\ branchLifecycle[branch].refresh_lease = branchLifecycle[branch].epoch
    /\ branchLifecycle' = [branchLifecycle EXCEPT
        ![branch].epoch = 1,
        ![branch].refresh_lease = NoLease,
        ![branch].refresh_ready = FALSE
        ]
    /\ hazards' = [hazards EXCEPT
        !.lifecycle_fencing = @ \/
            (branchLifecycle[branch].refresh_lease # branchLifecycle[branch].epoch),
        !.refresh_gc_race = @ \/ ~GuardLifecycleFencing
        ]
    /\ UNCHANGED <<
        authority, capsules, transactions, sharedLifecycleHolders,
        exclusiveLifecycleFence, stagedFacts, overlays, branchHeads,
        effectsReleased, publishCounts, defaultFacts, defaultHead, trunk, dbNow,
        capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction
        >>

AcquireGCLease(branch) ==
    /\ branch = "branch_a"
    /\ branchLifecycle[branch].live
    /\ branchLifecycle[branch].scenario \in {"none", "gc"}
    /\ ~trunk.merged
    /\ lastControllerAction = "none"
    /\ branchLifecycle[branch].gc_lease = NoLease
    /\ branchLifecycle' = [branchLifecycle EXCEPT
        ![branch].gc_lease = branchLifecycle[branch].epoch,
        ![branch].scenario = "gc"
        ]
    /\ UNCHANGED <<
        authority, capsules, transactions, sharedLifecycleHolders,
        exclusiveLifecycleFence, stagedFacts, overlays, branchHeads,
        effectsReleased, publishCounts, defaultFacts, defaultHead, trunk, dbNow,
        capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction, hazards
        >>

AcquireReadLease(branch) ==
    /\ branch = "branch_a"
    /\ branchLifecycle[branch].live
    /\ ~branchLifecycle[branch].collected
    /\ branchLifecycle[branch].read_leases = 0
    /\ branchLifecycle[branch].scenario \in {"none", "gc"}
    /\ ~trunk.merged
    /\ lastControllerAction = "none"
    /\ branchLifecycle' = [branchLifecycle EXCEPT
        ![branch].read_leases = 1,
        ![branch].scenario = "gc"
        ]
    /\ UNCHANGED <<
        authority, capsules, transactions, sharedLifecycleHolders,
        exclusiveLifecycleFence, stagedFacts, overlays, branchHeads,
        effectsReleased, publishCounts, defaultFacts, defaultHead, trunk, dbNow,
        capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction, hazards
        >>

ReleaseReadLease(branch) ==
    /\ branch = "branch_a"
    /\ branchLifecycle[branch].read_leases = 1
    /\ branchLifecycle' = [branchLifecycle EXCEPT ![branch].read_leases = 0]
    /\ UNCHANGED <<
        authority, capsules, transactions, sharedLifecycleHolders,
        exclusiveLifecycleFence, stagedFacts, overlays, branchHeads,
        effectsReleased, publishCounts, defaultFacts, defaultHead, trunk, dbNow,
        capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction, hazards
        >>

RetireBranch(branch) ==
    /\ branch = "branch_a"
    /\ exclusiveLifecycleFence
    /\ sharedLifecycleHolders = {}
    /\ branchLifecycle[branch].live
    /\ GuardLifecycleFencing =>
        /\ branchLifecycle[branch].durable_jobs = 0
        /\ branchLifecycle[branch].read_leases = 0
    /\ branchLifecycle' = [branchLifecycle EXCEPT ![branch].retired = TRUE]
    /\ hazards' = [hazards EXCEPT
        !.lifecycle_fencing = @ \/
            branchLifecycle[branch].durable_jobs # 0 \/
            branchLifecycle[branch].read_leases # 0,
        !.refresh_gc_race = @ \/ ~GuardLifecycleFencing
        ]
    /\ UNCHANGED <<
        authority, capsules, transactions, sharedLifecycleHolders,
        exclusiveLifecycleFence, stagedFacts, overlays, branchHeads,
        effectsReleased, publishCounts, defaultFacts, defaultHead, trunk, dbNow,
        capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction
        >>

GarbageCollectBranch(branch) ==
    /\ branch = "branch_a"
    /\ branchLifecycle[branch].gc_lease # NoLease
    /\ GuardLifecycleFencing =>
        /\ exclusiveLifecycleFence
        /\ sharedLifecycleHolders = {}
        /\ branchLifecycle[branch].retired
        /\ branchLifecycle[branch].gc_lease = branchLifecycle[branch].epoch
        /\ branchLifecycle[branch].durable_jobs = 0
        /\ branchLifecycle[branch].read_leases = 0
    /\ branchLifecycle' = [branchLifecycle EXCEPT
        ![branch].live = FALSE,
        ![branch].collected = TRUE
        ]
    /\ hazards' = [hazards EXCEPT
        !.lifecycle_fencing = @ \/
            ~branchLifecycle[branch].retired \/
            (branchLifecycle[branch].gc_lease # branchLifecycle[branch].epoch) \/
            branchLifecycle[branch].durable_jobs # 0 \/
            branchLifecycle[branch].read_leases # 0,
        !.refresh_gc_race = @ \/ ~GuardLifecycleFencing
        ]
    /\ UNCHANGED <<
        authority, capsules, transactions, sharedLifecycleHolders,
        exclusiveLifecycleFence, stagedFacts, overlays, branchHeads,
        effectsReleased, publishCounts, defaultFacts, defaultHead, trunk, dbNow,
        capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction
        >>

MergeGit(order) ==
    /\ order \in {"a_then_b", "b_then_a"}
    /\ ~trunk.merged
    /\ \A branch \in PreviewBranches : transactions[branch].state = "idle"
    /\ branchLifecycle["branch_a"].scenario = "none"
    /\ lastControllerAction = "none"
    /\ dbNow = 0
    /\ ~capabilityRevoked
    /\ processLaunchAttestations = 0
    /\ trunk' = [trunk EXCEPT
        !.merged = TRUE,
        !.merge_order = order
        ]
    /\ UNCHANGED <<
        authority, capsules, transactions, sharedLifecycleHolders,
        exclusiveLifecycleFence, stagedFacts, overlays, branchHeads,
        effectsReleased, publishCounts, defaultFacts, defaultHead,
        branchLifecycle, dbNow, capabilityRevoked, processLaunchAttestations,
        priorAuthority, lastControllerAction, hazards
        >>

ProjectCleanTrunk ==
    /\ trunk.merged
    /\ ~trunk.projected
    /\ trunk.capsule_complete
    /\ trunk.sources_classified
    /\ trunk.catalog_closed
    /\ trunk.event_offset_pinned
    /\ trunk.external_snapshot_pinned
    /\ trunk.git_snapshot_compatible
    /\ trunk.sync_root = "sync_root"
    /\ trunk.expected_parent = defaultHead
    /\ defaultFacts' = IF GuardProjectionClosure
        THEN CleanTrunkFacts
        ELSE CleanTrunkFacts \cup {"preview_a"}
    /\ defaultHead' = defaultHead + 1
    /\ trunk' = [trunk EXCEPT
        !.projected = TRUE,
        !.projected_from = IF GuardProjectionClosure
            THEN "clean_projection" ELSE "preview_graph_union"
        ]
    /\ hazards' = [hazards EXCEPT
        !.projection_closure = @ \/ ~GuardProjectionClosure,
        !.graph_union_landing = @ \/ ~GuardProjectionClosure
        ]
    /\ UNCHANGED <<
        authority, capsules, transactions, sharedLifecycleHolders,
        exclusiveLifecycleFence, stagedFacts, overlays, branchHeads,
        effectsReleased, publishCounts, branchLifecycle, dbNow,
        capabilityRevoked, processLaunchAttestations, priorAuthority,
        lastControllerAction
        >>

Next ==
    \/ \E branch \in PreviewBranches : Reserve(branch)
    \/ \E branch \in PreviewBranches : Stage(branch)
    \/ \E branch \in PreviewBranches : Gate(branch)
    \/ \E branch \in PreviewBranches : Commit(branch)
    \/ \E branch \in PreviewBranches : ClaimOutbox(branch)
    \/ \E branch \in PreviewBranches : RetryDeliveryClaim(branch)
    \/ \E branch \in PreviewBranches : DeliverOutbox(branch)
    \/ \E branch \in PreviewBranches : RetryDeliveredEffect(branch)
    \/ \E branch \in PreviewBranches : AcknowledgeDelivery(branch)
    \/ \E branch \in PreviewBranches : Finalize(branch)
    \/ \E branch \in PreviewBranches : RecoverAbort(branch)
    \/ \E branch \in PreviewBranches : BlockStaged(branch)
    \/ \E branch \in PreviewBranches : SupersedeOutOfOrder(branch)
    \/ \E branch \in PreviewBranches : CrashWorker(branch)
    \/ \E branch \in PreviewBranches : RestartWorker(branch)
    \/ \E branch \in PreviewBranches : RecoverSharedLifecycleHolder(branch)
    \/ \E branch \in PreviewBranches : ProbeOperationIdReuse(branch)
    \/ \E branch \in PreviewBranches : AdvanceExpectedParent(branch)
    \/ \E branch \in PreviewBranches : ReplayCompletedWithoutDedup(branch)
    \/ \E branch \in PreviewBranches : ReplayTerminalResult(branch)
    \/ TickDatabaseTime
    \/ RevokeCapability
    \/ AcquireExclusiveLifecycleFence
    \/ ControllerRollback
    \/ ReleaseExclusiveLifecycleFence
    \/ SameReceiptServiceRestart
    \/ \E branch \in PreviewBranches : AcquireRefreshLease(branch)
    \/ \E branch \in PreviewBranches : BuildRefreshReplacement(branch)
    \/ \E branch \in PreviewBranches : RefreshCutover(branch)
    \/ \E branch \in PreviewBranches : AcquireGCLease(branch)
    \/ \E branch \in PreviewBranches : AcquireReadLease(branch)
    \/ \E branch \in PreviewBranches : ReleaseReadLease(branch)
    \/ \E branch \in PreviewBranches : RetireBranch(branch)
    \/ \E branch \in PreviewBranches : GarbageCollectBranch(branch)
    \/ \E order \in {"a_then_b", "b_then_a"} : MergeGit(order)
    \/ ProjectCleanTrunk

Spec == Init /\ [][Next]_vars

PostCommitRecoveryStep(branch) ==
    \/ /\ transactions[branch].state \in {"committed", "effect_released"}
       /\ RestartWorker(branch)
    \/ ClaimOutbox(branch)
    \/ RetryDeliveryClaim(branch)
    \/ DeliverOutbox(branch)
    \/ RetryDeliveredEffect(branch)
    \/ AcknowledgeDelivery(branch)
    \/ Finalize(branch)

FairSpec ==
    /\ Spec
    /\ \A branch \in PreviewBranches : WF_vars(PostCommitRecoveryStep(branch))

TypeInvariant ==
    /\ authority.lifecycle_epoch \in {0, 1}
    /\ authority.receipt_digest \in {"receipt_0", "receipt_1"}
    /\ authority.writer_epoch \in {0, 1}
    /\ authority.dbos_version \in {"dbos_0", "dbos_1"}
    /\ authority.capability_epoch \in {0, 1}
    /\ authority.mode \in {"disabled", "scoped-canary", "enabled"}
    /\ DOMAIN capsules = PreviewBranches
    /\ \A branch \in PreviewBranches : CompleteCapsule(capsules[branch])
    /\ transactions \in [PreviewBranches -> [
        state: TransactionStates,
        receipt_state: TransactionStates,
        expected_parent: {0, 1},
        published: BOOLEAN,
        finalized: BOOLEAN,
        commit_authorized: BOOLEAN,
        effect_authorized: BOOLEAN,
        effect_count: 0..2,
        outbox_state: {
            "none", "quarantined", "released", "claimed", "delivered", "acked",
            "discarded"},
        effect_id: {"effect_a", "effect_b"},
        delivery_attempts: 0..2,
        sink_dedup_recorded: BOOLEAN,
        committed_capsule_digest: {
            "none", "capsule_digest_a", "capsule_digest_b"},
        committed_lifecycle_epoch: {NoLease, 0},
        committed_receipt_digest: {"none", "receipt_0"},
        committed_writer_epoch: {NoLease, 0},
        committed_dbos_version: {"none", "dbos_0"},
        committed_capability_epoch: {NoLease, 0},
        terminal_result: {
            "none", "published", "blocked", "aborted", "superseded"},
        reservation_released: BOOLEAN,
        operation_id: {"operation_a", "operation_b"},
        capsule_digest: {
            "capsule_digest_a", "capsule_digest_b", "forged_capsule_digest"},
        presented_digest: {
            "capsule_digest_a", "capsule_digest_b", "forged_capsule_digest"},
        logical_idempotency_key: {"logical_batch_1"},
        reuse_probed: BOOLEAN,
        reuse_refused: BOOLEAN,
        worker_up: BOOLEAN,
        recovery_attempts: 0..2,
        crash_point: CrashPoints,
        cursor: {0, 1},
        watermark: {0, 1},
        progress: {0, 1},
        idempotency_claimed: BOOLEAN,
        contributions: SUBSET ContributionTokens,
        tombstones: SUBSET BaseContributionTokens,
        source_classes: SUBSET SourceClasses,
        head_owner: {"none", "operation", "newer_operation"},
        publish_parent: {NoLease, 0},
        expected_parent_refused: BOOLEAN
        ]]
    /\ sharedLifecycleHolders \subseteq PreviewBranches
    /\ exclusiveLifecycleFence \in BOOLEAN
    /\ stagedFacts \in [PreviewBranches -> SUBSET Facts]
    /\ overlays \in [PreviewBranches -> SUBSET Facts]
    /\ branchHeads \in [PreviewBranches -> {0, 1}]
    /\ effectsReleased \in [PreviewBranches -> BOOLEAN]
    /\ publishCounts \in [PreviewBranches -> {0, 1, 2}]
    /\ defaultFacts \subseteq Facts
    /\ defaultHead \in {0, 1}
    /\ dbNow \in 0..2
    /\ capabilityRevoked \in BOOLEAN
    /\ processLaunchAttestations \in {0, 1}
    /\ priorAuthority.lifecycle_epoch \in {0, 1}
    /\ priorAuthority.receipt_digest \in {"receipt_0", "receipt_1"}
    /\ priorAuthority.writer_epoch \in {0, 1}
    /\ priorAuthority.dbos_version \in {"dbos_0", "dbos_1"}
    /\ \A branch \in PreviewBranches :
        /\ branchLifecycle[branch].epoch \in {0, 1}
        /\ branchLifecycle[branch].durable_jobs \in {0, 1}
        /\ branchLifecycle[branch].read_leases \in {0, 1}
        /\ branchLifecycle[branch].collected \in BOOLEAN
        /\ branchLifecycle[branch].scenario \in {"none", "refresh", "gc"}
    /\ trunk.projected_from \in
        {"none", "clean_projection", "preview_graph_union"}

\* A preview transaction may never add to default; no missing capsule field is
\* interpreted as the default branch.
DefaultIsolation ==
    /\ defaultFacts \cap PreviewOnlyFacts = {}
    /\ ~hazards.default_isolation

\* Base identity is immutable, branch overlays contain only contributions
\* owned by that branch incarnation, and another branch never observes them.
ContributionOverlayIsolation ==
    /\ \A branch \in PreviewBranches : overlays[branch] \subseteq ExpectedOverlay(branch)
    /\ overlays["branch_a"] \cap overlays["branch_b"] = {}
    /\ \A branch \in PreviewBranches :
        /\ transactions[branch].contributions \subseteq ExpectedContribution(branch)
        /\ transactions[branch].tombstones \subseteq ExpectedTombstone(branch)
        /\ transactions[branch].source_classes \subseteq AllowedBranchSourceClasses
        /\ "base:source_t:record_x" \notin transactions[branch].tombstones
        /\ "event:ledger:0" \notin transactions[branch].tombstones
    /\ transactions["branch_a"].contributions \cap
        transactions["branch_b"].contributions = {}
    /\ ~hazards.contribution_overlay
    /\ ~hazards.inherited_delete
    /\ ~hazards.cross_source_tombstone
    /\ ~hazards.source_class_isolation

\* Staging is non-authoritative.  The durable receipt, visibility, effect, and
\* finalization states advance together and complete publication is at most once.
TransactionRecovery ==
    /\ transactions["branch_a"].logical_idempotency_key =
        transactions["branch_b"].logical_idempotency_key
    /\ transactions["branch_a"].operation_id #
        transactions["branch_b"].operation_id
    /\ transactions["branch_a"].capsule_digest #
        transactions["branch_b"].capsule_digest
    /\ \A branch \in PreviewBranches :
        /\ transactions[branch].state = transactions[branch].receipt_state
        /\ (transactions[branch].published <=>
            transactions[branch].state \in PublishedStates)
        /\ (overlays[branch] # {} =>
            transactions[branch].state \in PublishedStates)
        /\ (effectsReleased[branch] =>
            transactions[branch].state \in {"effect_released", "finalized"})
        /\ (transactions[branch].finalized =>
            transactions[branch].state = "finalized")
        /\ (transactions[branch].published =>
            /\ transactions[branch].cursor = 1
            /\ transactions[branch].watermark = 1
            /\ transactions[branch].progress = 1
            /\ transactions[branch].idempotency_claimed)
        /\ (~transactions[branch].published =>
            /\ transactions[branch].cursor = 0
            /\ transactions[branch].watermark = 0
            /\ transactions[branch].progress = 0
            /\ ~transactions[branch].idempotency_claimed)
        /\ (effectsReleased[branch] <=> transactions[branch].effect_count = 1)
        /\ (transactions[branch].sink_dedup_recorded <=>
            transactions[branch].effect_count = 1)
        /\ (transactions[branch].state = "reserved" =>
            transactions[branch].outbox_state = "none")
        /\ (transactions[branch].state \in {"staged", "gated"} =>
            transactions[branch].outbox_state = "quarantined")
        /\ (transactions[branch].state = "committed" =>
            transactions[branch].outbox_state \in {"released", "claimed"})
        /\ (transactions[branch].state = "effect_released" =>
            transactions[branch].outbox_state \in {"delivered", "acked"})
        /\ (transactions[branch].state = "finalized" =>
            transactions[branch].outbox_state = "acked")
        /\ (transactions[branch].state \in {"blocked", "aborted", "superseded"} =>
            transactions[branch].outbox_state = "discarded")
        /\ (transactions[branch].effect_count > 0 =>
            transactions[branch].effect_authorized)
        /\ (transactions[branch].published =>
            CommittedOutboxAuthorized(branch))
        /\ publishCounts[branch] =
            IF transactions[branch].published THEN 1 ELSE 0
        /\ (transactions[branch].published =>
            /\ transactions[branch].head_owner = "operation"
            /\ branchHeads[branch] = transactions[branch].publish_parent + 1)
        /\ (transactions[branch].expected_parent_refused =>
            ~transactions[branch].published)
        /\ (transactions[branch].state = "finalized" =>
            /\ transactions[branch].terminal_result = "published"
            /\ transactions[branch].reservation_released
            /\ branchLifecycle[branch].durable_jobs = 0)
        /\ (transactions[branch].state \in {"blocked", "aborted", "superseded"} =>
            /\ transactions[branch].terminal_result = transactions[branch].state
            /\ transactions[branch].reservation_released
            /\ branchLifecycle[branch].durable_jobs = 0)
        /\ (transactions[branch].reuse_probed =>
            /\ transactions[branch].reuse_refused
            /\ transactions[branch].presented_digest #
                transactions[branch].capsule_digest
            /\ transactions[branch].capsule_digest = OperationDigest(branch))
    /\ ~hazards.transaction_recovery
    /\ ~hazards.branch_blind_cursor
    /\ ~hazards.cross_branch_idempotency
    /\ ~hazards.crash_duplicate
    /\ ~hazards.out_of_order_publish

\* Every durable post-commit state has an enabled recovery step, including after
\* controller rollback invalidates the mutable authority used before commit.
PostCommitRecoveryEnabled ==
    \A branch \in PreviewBranches :
        transactions[branch].state \in {"committed", "effect_released"} =>
            ENABLED PostCommitRecoveryStep(branch)

RollbackPostCommitRecoveryEnabled ==
    lastControllerAction = "controller_rollback" =>
        \A branch \in PreviewBranches :
            transactions[branch].state \in {"committed", "effect_released"} =>
                ENABLED PostCommitRecoveryStep(branch)

\* Under weak fairness for the recovery driver, every committed transaction
\* eventually records its terminal result and releases its durable job.
CommittedEventuallyFinalized ==
    \A branch \in PreviewBranches :
        (transactions[branch].state = "committed") ~>
            /\ transactions[branch].state = "finalized"
            /\ transactions[branch].terminal_result = "published"
            /\ branchLifecycle[branch].durable_jobs = 0

\* Default is either the immutable base or the exact clean-trunk projection;
\* preview rows, provenance, and merge order are outside the projection result.
ProjectionClosure ==
    /\ (trunk.projected => defaultFacts = CleanTrunkFacts)
    /\ (~trunk.projected => defaultFacts = BaseFacts)
    /\ defaultFacts \cap PreviewOnlyFacts = {}
    /\ (trunk.projected => trunk.projected_from = "clean_projection")
    /\ trunk.catalog_closed
    /\ trunk.event_offset_pinned
    /\ trunk.external_snapshot_pinned
    /\ trunk.git_snapshot_compatible
    /\ ~hazards.projection_closure
    /\ ~hazards.graph_union_landing

\* Lifecycle transitions are exclusive against in-flight mutation.  Refresh
\* and GC leases are epoch fenced.  Controller rollback mints a new receipt;
\* a same-receipt service restart changes only its launch attestation.
LifecycleFencing ==
    /\ (exclusiveLifecycleFence => sharedLifecycleHolders = {})
    /\ \A branch \in PreviewBranches :
        branchLifecycle[branch].live \/ branchLifecycle[branch].retired
    /\ \A branch \in PreviewBranches :
        branchLifecycle[branch].collected =>
            /\ branchLifecycle[branch].retired
            /\ branchLifecycle[branch].durable_jobs = 0
            /\ branchLifecycle[branch].read_leases = 0
    /\ (lastControllerAction = "controller_rollback" =>
        /\ authority.lifecycle_epoch # priorAuthority.lifecycle_epoch
        /\ authority.receipt_digest # priorAuthority.receipt_digest
        /\ authority.writer_epoch # priorAuthority.writer_epoch
        /\ authority.dbos_version # priorAuthority.dbos_version
        /\ authority.lifecycle_epoch = 1
        /\ authority.receipt_digest = "receipt_1"
        /\ authority.writer_epoch = 1
        /\ authority.dbos_version = "dbos_1"
        /\ \A branch \in PreviewBranches : ~CapsuleMatchesAuthority(branch))
    /\ (lastControllerAction = "same_receipt_restart" =>
        /\ authority.lifecycle_epoch = priorAuthority.lifecycle_epoch
        /\ authority.receipt_digest = priorAuthority.receipt_digest
        /\ authority.writer_epoch = priorAuthority.writer_epoch
        /\ authority.dbos_version = priorAuthority.dbos_version)
    /\ ~hazards.lifecycle_fencing
    /\ ~hazards.refresh_gc_race

\* Authorization is checked against database time and every digest/epoch while
\* the transaction holds the shared lifecycle fence immediately before commit.
CapabilityFencing ==
    /\ \A branch \in PreviewBranches :
        transactions[branch].published => transactions[branch].commit_authorized
    /\ \A branch \in PreviewBranches :
        transactions[branch].effect_count > 0 =>
            transactions[branch].effect_authorized
    /\ ~hazards.capability_fencing

=============================================================================
