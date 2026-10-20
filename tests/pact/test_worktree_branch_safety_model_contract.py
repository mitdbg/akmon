"""Structural refinement checks for the shared worktree-branch TLA+ model.

These tests bind authored design obligations to concrete model operators and
load-bearing witness configurations.  Product/runtime behavior remains covered
by the separate ``tests/substrate`` obligations in each owning PACT.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
FORMAL_ROOT = REPO_ROOT / "formal"
MODEL_DIR = REPO_ROOT / "formal" / "worktree_branches"
MODEL_TEXT = (MODEL_DIR / "WorktreeBranchSafety.tla").read_text(encoding="utf-8")
POSITIVE_CONFIG = (MODEL_DIR / "WorktreeBranchSafety.cfg").read_text(encoding="utf-8")
BINDINGS = yaml.safe_load(
    (MODEL_DIR / "pact_bindings.yaml").read_text(encoding="utf-8")
)["bindings"]

GUARDS = {
    "DefaultIsolation": "GuardDefaultIsolation",
    "ContributionOverlayIsolation": "GuardContributionOverlayIsolation",
    "TransactionRecovery": "GuardTransactionRecovery",
    "ProjectionClosure": "GuardProjectionClosure",
    "LifecycleFencing": "GuardLifecycleFencing",
    "CapabilityFencing": "GuardCapabilityFencing",
}

WITNESS_INVARIANTS = {
    "WorktreeBranchSafetyDefaultIsolationWitness.cfg": "DefaultIsolation",
    "WorktreeBranchSafetyContributionOverlayIsolationWitness.cfg": (
        "ContributionOverlayIsolation"
    ),
    "WorktreeBranchSafetyTransactionRecoveryWitness.cfg": "TransactionRecovery",
    "WorktreeBranchSafetyPostCommitRecoveryWitness.cfg": (
        "RollbackPostCommitRecoveryEnabled"
    ),
    "WorktreeBranchSafetyProjectionClosureWitness.cfg": "ProjectionClosure",
    "WorktreeBranchSafetyLifecycleFencingWitness.cfg": "LifecycleFencing",
    "WorktreeBranchSafetyCapabilityFencingWitness.cfg": "CapabilityFencing",
}
EXPECTED_CAPSULE_FIELDS = {
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
    "change_control_identity",
}


def _operator_body(name: str, next_name: str) -> str:
    match = re.search(
        rf"^{re.escape(name)}(?:\([^\n]*\))? ==\n(?P<body>.*?)"
        rf"(?=^{re.escape(next_name)}(?:\([^\n]*\))? ==)",
        MODEL_TEXT,
        flags=re.MULTILINE | re.DOTALL,
    )
    assert match is not None, f"missing or reordered model operator {name}"
    return match.group("body")


def _cfg_invariants(path: Path) -> list[str]:
    """Parse TLC INVARIANT(S) names without collapsing duplicates."""
    names: list[str] = []
    in_block = False
    directives = {
        "CHECK_DEADLOCK",
        "CONSTANT",
        "CONSTRAINT",
        "INVARIANT",
        "INVARIANTS",
        "PROPERTY",
        "PROPERTIES",
        "SPECIFICATION",
        "VIEW",
    }
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("\\*", 1)[0].strip()
        if not line:
            continue
        parts = line.split()
        directive = parts[0]
        if directive in {"INVARIANT", "INVARIANTS"}:
            in_block = True
            names.extend(part.rstrip(",") for part in parts[1:])
            continue
        if directive in directives:
            in_block = False
            continue
        if in_block:
            names.append(directive.rstrip(","))
    return names
def _assert_property_and_isolated_witness(property_id: str) -> None:
    guard = GUARDS[property_id]
    assert f"{property_id} ==" in MODEL_TEXT
    assert f"  {property_id}" in POSITIVE_CONFIG
    assert f"  {guard} = TRUE" in POSITIVE_CONFIG
    assert "  GuardCommittedOutboxRecovery = TRUE" in POSITIVE_CONFIG

    witness = (MODEL_DIR / f"WorktreeBranchSafety{property_id}Witness.cfg").read_text(
        encoding="utf-8"
    )
    assert f"  {guard} = FALSE" in witness
    assert f"INVARIANT {property_id}" in witness
    assert "  GuardCommittedOutboxRecovery = TRUE" in witness
    for other_guard in set(GUARDS.values()) - {guard}:
        assert f"  {other_guard} = TRUE" in witness

    matching_bindings = [
        row for row in BINDINGS if row.get("property_id") == property_id
    ]
    assert len(matching_bindings) == 1
    assert matching_bindings[0]["intent_digest"].startswith("sha256:")


def test_worktree_witness_configs_isolate_exactly_one_invariant() -> None:
    positive = set(_cfg_invariants(MODEL_DIR / "WorktreeBranchSafety.cfg"))
    assert len(positive) > 1
    for filename, expected in WITNESS_INVARIANTS.items():
        config = MODEL_DIR / filename
        invariants = _cfg_invariants(config)
        assert invariants == [expected], (
            f"{filename} must declare exactly [{expected}], got {invariants}"
        )
        assert set(invariants) != positive


def test_worktree_model_is_registered_at_all_four_formal_surfaces() -> None:
    """Keep the model visible in index, folder, aggregate, and report inventory."""
    root_readme = (FORMAL_ROOT / "README.md").read_text(encoding="utf-8")
    folder_readme = (MODEL_DIR / "README.md").read_text(encoding="utf-8")
    suite = (REPO_ROOT / "scripts" / "run_formal_suite.py").read_text(
        encoding="utf-8"
    )
    report_inventory = (REPO_ROOT / "tests/pact/test_formal_report_producers.py").read_text(
        encoding="utf-8"
    )

    assert "| `worktree_branches` |" in root_readme
    assert "WorktreeBranchSafety.tla" in folder_readme
    assert "check_formal_worktree_branch_safety.py" in suite
    assert "check_formal_worktree_branch_safety.py" in report_inventory
def test_default_isolation_refines_complete_capsule_and_default_guard() -> None:
    fields_match = re.search(
        r"^CapsuleFields == \{(?P<fields>.*?)^\}",
        MODEL_TEXT,
        flags=re.MULTILINE | re.DOTALL,
    )
    assert fields_match is not None
    assert set(re.findall(r'"([^"]+)"', fields_match.group("fields"))) == (
        EXPECTED_CAPSULE_FIELDS
    )
    assert "CompleteCapsule(capsule) == DOMAIN capsule = CapsuleFields" in MODEL_TEXT
    assert 'CapsulePayloadFields == CapsuleFields \\ {"capsule_digest"}' in MODEL_TEXT
    assert "CapsuleDigestValid(capsule, branch)" in MODEL_TEXT
    reserve = _operator_body("Reserve", "Stage")
    assert "CapsuleMatchesAuthority(branch)" in reserve
    assert "GuardCapabilityFencing => CapabilityValid(branch)" in reserve
    isolation = _operator_body("DefaultIsolation", "ContributionOverlayIsolation")
    assert "defaultFacts \\cap PreviewOnlyFacts = {}" in isolation
    assert "~hazards.default_isolation" in isolation
    _assert_property_and_isolated_witness("DefaultIsolation")


def test_contribution_overlay_refines_branch_qualified_state() -> None:
    commit = _operator_body("Commit", "ClaimOutbox")
    assert "![branch] = stagedFacts[branch]" in commit
    assert "![Other(branch)] = IF GuardContributionOverlayIsolation" in commit
    assert "THEN @ ELSE @ \\cup stagedFacts[branch]" in commit
    for state_update in (
        "![branch].contributions = ExpectedContribution(branch)",
        "![branch].tombstones = IF GuardContributionOverlayIsolation",
        "![branch].source_classes = IF GuardContributionOverlayIsolation",
    ):
        assert state_update in commit
    isolation = _operator_body("ContributionOverlayIsolation", "TransactionRecovery")
    assert "overlays[branch] \\subseteq ExpectedOverlay(branch)" in isolation
    assert 'overlays["branch_a"] \\cap overlays["branch_b"] = {}' in isolation
    assert (
        "transactions[branch].contributions \\subseteq ExpectedContribution(branch)"
        in isolation
    )
    assert (
        "transactions[branch].tombstones \\subseteq ExpectedTombstone(branch)"
        in isolation
    )
    assert (
        "transactions[branch].source_classes \\subseteq AllowedBranchSourceClasses"
        in isolation
    )
    assert "~hazards.inherited_delete" in isolation
    assert "~hazards.cross_source_tombstone" in isolation
    assert "~hazards.source_class_isolation" in isolation
    _assert_property_and_isolated_witness("ContributionOverlayIsolation")


def test_transaction_recovery_refines_atomic_publication_and_effects() -> None:
    publication_cas = _operator_body(
        "CompletePublicationCAS", "CommittedOutboxAuthorized"
    )
    for clause in (
        'transactions[branch].state = "gated"',
        "transactions[branch].expected_parent = branchHeads[branch]",
        "capsules[branch].expected_parent_checkpoint = branchHeads[branch]",
        "transactions[branch].operation_id = capsules[branch].operation_id",
        "transactions[branch].capsule_digest = capsules[branch].capsule_digest",
        "transactions[branch].presented_digest = capsules[branch].capsule_digest",
        "branch \\in sharedLifecycleHolders",
        "~exclusiveLifecycleFence",
        "CapsuleMatchesAuthority(branch)",
        "stagedFacts[branch] = ExpectedOverlay(branch)",
    ):
        assert clause in publication_cas
    stage = _operator_body("Stage", "Gate")
    assert '![branch].outbox_state = "quarantined"' in stage
    committed_authority = _operator_body(
        "CommittedOutboxAuthorized", "DeliveryAuthorization"
    )
    for clause in (
        "transactions[branch].published",
        "transactions[branch].commit_authorized",
        "transactions[branch].operation_id = capsules[branch].operation_id",
        "transactions[branch].effect_id = EffectId(branch)",
        "transactions[branch].committed_capsule_digest =",
        "transactions[branch].committed_receipt_digest =",
        "transactions[branch].committed_writer_epoch =",
        "transactions[branch].committed_dbos_version =",
        "transactions[branch].committed_capability_epoch =",
    ):
        assert clause in committed_authority
    delivery_authority = _operator_body("DeliveryAuthorization", "Reserve")
    assert "IF GuardCommittedOutboxRecovery" in delivery_authority
    assert "THEN CommittedOutboxAuthorized(branch)" in delivery_authority
    assert "ELSE /\\ CapsuleMatchesAuthority(branch)" in delivery_authority
    commit = _operator_body("Commit", "ClaimOutbox")
    for clause in (
        '![branch].state = "committed"',
        '![branch].outbox_state = "released"',
        "![branch].cursor = 1",
        "![branch].watermark = 1",
        "![branch].progress = 1",
        "![branch].idempotency_claimed = TRUE",
        "![branch].committed_capsule_digest = capsules[branch].capsule_digest",
        "![branch].committed_lifecycle_epoch = authority.lifecycle_epoch",
        "![branch].committed_receipt_digest = authority.receipt_digest",
        "![branch].committed_writer_epoch = authority.writer_epoch",
        "![branch].committed_dbos_version = authority.dbos_version",
        "![branch].committed_capability_epoch = authority.capability_epoch",
        "![branch].contributions = ExpectedContribution(branch)",
        "![branch].publish_parent = branchHeads[branch]",
    ):
        assert clause in commit
    claim = _operator_body("ClaimOutbox", "RetryDeliveryClaim")
    for clause in (
        'transactions[branch].outbox_state = "released"',
        "CommittedOutboxAuthorized(branch)",
        '![branch].outbox_state = "claimed"',
        "![branch].delivery_attempts = @ + 1",
    ):
        assert clause in claim
    claim_retry = _operator_body("RetryDeliveryClaim", "DeliverOutbox")
    assert "transactions[branch].recovery_attempts > 0" in claim_retry
    assert "transactions[branch].delivery_attempts < 2" in claim_retry
    deliver = _operator_body("DeliverOutbox", "RetryDeliveredEffect")
    for clause in (
        'transactions[branch].outbox_state = "claimed"',
        "DeliveryAuthorization(branch)",
        "![branch].effect_count = IF @ = 0 THEN 1 ELSE @",
        "![branch].effect_authorized = CommittedOutboxAuthorized(branch)",
        "![branch].sink_dedup_recorded = TRUE",
        '![branch].outbox_state = "delivered"',
    ):
        assert clause in deliver
    delivery_retry = _operator_body("RetryDeliveredEffect", "AcknowledgeDelivery")
    assert "transactions[branch].recovery_attempts > 0" in delivery_retry
    assert "![branch].effect_count = IF @ = 0 THEN 1 ELSE @" in delivery_retry
    assert "![branch].sink_dedup_recorded = TRUE" in delivery_retry
    acknowledge = _operator_body("AcknowledgeDelivery", "Finalize")
    assert 'transactions[branch].outbox_state = "delivered"' in acknowledge
    assert '![branch].outbox_state = "acked"' in acknowledge
    finalize = _operator_body("Finalize", "RecoverAbort")
    assert 'transactions[branch].outbox_state = "acked"' in finalize
    assert '![branch].terminal_result = "published"' in finalize
    assert "![branch].reservation_released = TRUE" in finalize
    assert "![branch].durable_jobs = 0" in finalize
    for terminal, next_operator, terminal_state in (
        ("RecoverAbort", "BlockStaged", "aborted"),
        ("BlockStaged", "SupersedeOutOfOrder", "blocked"),
        ("SupersedeOutOfOrder", "CrashWorker", "superseded"),
    ):
        body = _operator_body(terminal, next_operator)
        assert f'![branch].state = "{terminal_state}"' in body
        assert '![branch].outbox_state = "discarded"' in body
        assert f'![branch].terminal_result = "{terminal_state}"' in body
        assert "![branch].reservation_released = TRUE" in body
    crash = _operator_body("CrashWorker", "RestartWorker")
    assert "transactions[branch].state \\in RecoverableStates" in crash
    assert "![branch].worker_up = FALSE" in crash
    assert "![branch].crash_point = transactions[branch].state" in crash
    restart = _operator_body("RestartWorker", "RecoverSharedLifecycleHolder")
    assert "![branch].worker_up = TRUE" in restart
    assert "![branch].recovery_attempts = @ + 1" in restart
    reacquire = _operator_body("RecoverSharedLifecycleHolder", "ProbeOperationIdReuse")
    assert (
        "sharedLifecycleHolders' = sharedLifecycleHolders \\cup {branch}" in reacquire
    )
    assert "CapsuleMatchesAuthority(branch)" in reacquire
    assert "CapabilityValid(branch)" in reacquire
    reuse = _operator_body("ProbeOperationIdReuse", "AdvanceExpectedParent")
    assert '![branch].presented_digest = "forged_capsule_digest"' in reuse
    assert "![branch].reuse_refused = GuardTransactionRecovery" in reuse
    assert 'THEN @ ELSE "forged_capsule_digest"' in reuse
    supersede = _operator_body("AdvanceExpectedParent", "ReplayCompletedWithoutDedup")
    assert "branchHeads' = [branchHeads EXCEPT ![branch] = 1]" in supersede
    replay = _operator_body("ReplayCompletedWithoutDedup", "ReplayTerminalResult")
    assert "~GuardTransactionRecovery" in replay
    assert "![branch].effect_count = 2" in replay
    assert "publishCounts' = [publishCounts EXCEPT ![branch] = 2]" in replay
    terminal_replay = _operator_body("ReplayTerminalResult", "TickDatabaseTime")
    assert '{"finalized", "blocked", "aborted", "superseded"}' in terminal_replay
    recovery = _operator_body("TransactionRecovery", "PostCommitRecoveryEnabled")
    for clause in (
        "state = transactions[branch].receipt_state",
        "transactions[branch].published <=>",
        "effectsReleased[branch] =>",
        "publishCounts[branch] =",
        "transactions[branch].effect_count = 1",
        'transactions[branch].outbox_state = "quarantined"',
        'transactions[branch].outbox_state \\in {"released", "claimed"}',
        'transactions[branch].outbox_state = "acked"',
        'transactions[branch].outbox_state = "discarded"',
        "transactions[branch].presented_digest #",
        "~hazards.transaction_recovery",
        "~hazards.branch_blind_cursor",
        "~hazards.cross_branch_idempotency",
        "~hazards.crash_duplicate",
        "~hazards.out_of_order_publish",
    ):
        assert clause in recovery
    enabled = _operator_body(
        "PostCommitRecoveryEnabled", "RollbackPostCommitRecoveryEnabled"
    )
    assert '{"committed", "effect_released"}' in enabled
    assert "ENABLED PostCommitRecoveryStep(branch)" in enabled
    rollback_enabled = _operator_body(
        "RollbackPostCommitRecoveryEnabled", "CommittedEventuallyFinalized"
    )
    assert 'lastControllerAction = "controller_rollback"' in rollback_enabled
    assert "ENABLED PostCommitRecoveryStep(branch)" in rollback_enabled
    liveness = _operator_body("CommittedEventuallyFinalized", "ProjectionClosure")
    assert '(transactions[branch].state = "committed") ~>' in liveness
    assert 'transactions[branch].state = "finalized"' in liveness
    assert "branchLifecycle[branch].durable_jobs = 0" in liveness
    assert "SPECIFICATION FairSpec" in POSITIVE_CONFIG
    assert "PROPERTY CommittedEventuallyFinalized" in POSITIVE_CONFIG
    post_commit_witness = (
        MODEL_DIR / "WorktreeBranchSafetyPostCommitRecoveryWitness.cfg"
    ).read_text(encoding="utf-8")
    assert "  GuardCommittedOutboxRecovery = FALSE" in post_commit_witness
    assert "INVARIANT RollbackPostCommitRecoveryEnabled" in post_commit_witness
    for family_guard in GUARDS.values():
        assert f"  {family_guard} = TRUE" in post_commit_witness
    _assert_property_and_isolated_witness("TransactionRecovery")


def test_projection_closure_refines_clean_trunk_without_preview_copy() -> None:
    merge = _operator_body("MergeGit", "ProjectCleanTrunk")
    assert '{"a_then_b", "b_then_a"}' in merge
    projection = _operator_body("ProjectCleanTrunk", "Next")
    assert "trunk.capsule_complete" in projection
    assert "trunk.sources_classified" in projection
    assert 'trunk.sync_root = "sync_root"' in projection
    assert "trunk.expected_parent = defaultHead" in projection
    for input_guard in (
        "trunk.catalog_closed",
        "trunk.event_offset_pinned",
        "trunk.external_snapshot_pinned",
        "trunk.git_snapshot_compatible",
    ):
        assert input_guard in projection
    assert "IF GuardProjectionClosure" in projection
    assert "THEN CleanTrunkFacts" in projection
    closure = _operator_body("ProjectionClosure", "LifecycleFencing")
    assert "trunk.projected => defaultFacts = CleanTrunkFacts" in closure
    assert "defaultFacts \\cap PreviewOnlyFacts = {}" in closure
    assert 'trunk.projected_from = "clean_projection"' in closure
    assert "~hazards.graph_union_landing" in closure
    _assert_property_and_isolated_witness("ProjectionClosure")


def test_lifecycle_fencing_refines_epoch_leases_and_receipt_restart() -> None:
    acquire = _operator_body("AcquireExclusiveLifecycleFence", "ControllerRollback")
    assert "GuardLifecycleFencing => sharedLifecycleHolders = {}" in acquire
    refresh = _operator_body("RefreshCutover", "AcquireGCLease")
    assert "exclusiveLifecycleFence" in refresh
    assert "sharedLifecycleHolders = {}" in refresh
    assert "refresh_lease = branchLifecycle[branch].epoch" in refresh
    gc = _operator_body("GarbageCollectBranch", "MergeGit")
    assert "gc_lease = branchLifecycle[branch].epoch" in gc
    assert "branchLifecycle[branch].retired" in gc
    assert "branchLifecycle[branch].durable_jobs = 0" in gc
    assert "branchLifecycle[branch].read_leases = 0" in gc
    rollback = _operator_body("ControllerRollback", "ReleaseExclusiveLifecycleFence")
    for update in (
        "!.lifecycle_epoch = 1",
        '!.receipt_digest = "receipt_1"',
        "!.writer_epoch = 1",
        '!.dbos_version = "dbos_1"',
    ):
        assert update in rollback
    restart = _operator_body("SameReceiptServiceRestart", "AcquireRefreshLease")
    assert "priorAuthority' = authority" in restart
    assert "authority" in restart.split("UNCHANGED <<", maxsplit=1)[1]
    lifecycle = _operator_body("LifecycleFencing", "CapabilityFencing")
    for field in ("lifecycle_epoch", "receipt_digest", "writer_epoch", "dbos_version"):
        assert f"authority.{field} # priorAuthority.{field}" in lifecycle
        assert f"authority.{field} = priorAuthority.{field}" in lifecycle
    assert "~CapsuleMatchesAuthority(branch)" in lifecycle
    assert "~hazards.refresh_gc_race" in lifecycle
    _assert_property_and_isolated_witness("LifecycleFencing")


def test_capability_fencing_refines_db_time_and_receipt_authority() -> None:
    capability = _operator_body("CapabilityValid", "CompletePublicationCAS")
    for clause in (
        'authority.mode = "scoped-canary"',
        "~capabilityRevoked",
        'capsule.operation_class = "publish"',
        "capsule.canary_issued_at <= dbNow",
        "dbNow < capsule.canary_expiry",
        'capsule.change_control_identity = "change_993"',
    ):
        assert clause in capability
    authority = _operator_body("CapsuleMatchesAuthority", "CapabilityValid")
    for clause in (
        "capsule.lifecycle_activation_epoch = authority.lifecycle_epoch",
        "capsule.activation_receipt_digest = authority.receipt_digest",
        "capsule.database_writer_epoch = authority.writer_epoch",
        "capsule.dbos_application_version = authority.dbos_version",
        "capsule.worktree_write_capability_epoch = authority.capability_epoch",
        "capsule.release_manifest_digest = authority.release_manifest_digest",
        "capsule.evidence_digest = authority.evidence_digest",
        "capsule.dependency_lock_digest = authority.dependency_lock_digest",
    ):
        assert clause in authority
    assert MODEL_TEXT.count("GuardCapabilityFencing => CapabilityValid(branch)") == 2
    fencing = MODEL_TEXT.split("CapabilityFencing ==", maxsplit=1)[1]
    assert (
        "transactions[branch].published => transactions[branch].commit_authorized"
        in fencing
    )
    _assert_property_and_isolated_witness("CapabilityFencing")


def test_parent_hazard_matrix_is_compositionally_covered() -> None:
    expected_hazards = {
        "default_isolation": "DefaultIsolation",
        "contribution_overlay": "ContributionOverlayIsolation",
        "inherited_delete": "ContributionOverlayIsolation",
        "cross_source_tombstone": "ContributionOverlayIsolation",
        "source_class_isolation": "ContributionOverlayIsolation",
        "transaction_recovery": "TransactionRecovery",
        "branch_blind_cursor": "TransactionRecovery",
        "cross_branch_idempotency": "TransactionRecovery",
        "crash_duplicate": "TransactionRecovery",
        "out_of_order_publish": "TransactionRecovery",
        "projection_closure": "ProjectionClosure",
        "graph_union_landing": "ProjectionClosure",
        "lifecycle_fencing": "LifecycleFencing",
        "refresh_gc_race": "LifecycleFencing",
        "capability_fencing": "CapabilityFencing",
    }
    for hazard, property_id in expected_hazards.items():
        property_body = MODEL_TEXT.split(f"{property_id} ==", maxsplit=1)[1]
        if property_id != "CapabilityFencing":
            next_property = list(GUARDS).index(property_id) + 1
            if next_property < len(GUARDS):
                property_body = property_body.split(
                    f"{list(GUARDS)[next_property]} ==", maxsplit=1
                )[0]
        assert f"~hazards.{hazard}" in property_body


def test_checker_reports_absolute_witness_artifacts_outside_repo(
    tmp_path: Path,
) -> None:
    from scripts.check_formal_worktree_branch_safety import _report_path

    artifact = tmp_path / "guard-removal-witnesses" / "TransactionRecovery.log"
    assert _report_path(artifact) == str(artifact.resolve())
