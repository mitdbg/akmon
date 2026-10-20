#!/usr/bin/env python3
"""Prove and digest-bind the compact deployment control-plane kernel."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

import yaml

from okg.substrate.formal_tlc import tlc_jvm_args, tlc_workers
from okg.substrate.pact.store import intent_digest, load_manifest_file


ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = ROOT / "formal" / "deployment_control_plane_kernel"
MODULE = MODEL_DIR / "DeploymentControlPlaneKernel.tla"
CONFIG = MODEL_DIR / "DeploymentControlPlaneKernel.cfg"
REFINEMENTS = MODEL_DIR / "refinement-owners.yaml"
README = MODEL_DIR / "README.md"
DESIGN = (
    ROOT
    / "pact"
    / "changes"
    / "externalize-local-okg-lifecycle-controller"
    / "pact.yaml"
)
JAR = ROOT / ".tools" / "tla2tools.jar"
DEFAULT_OUT = ROOT / ".scratch" / "formal" / "deployment-control-plane-kernel"
CHECKER_COMMAND = (
    "uv run python scripts/check_formal_deployment_control_plane_kernel.py "
    "--all --out .scratch/formal/deployment-control-plane-kernel"
)
SMOKE_CHECKER_COMMAND = (
    "uv run python scripts/check_formal_deployment_control_plane_kernel.py "
    "--profile smoke --out .scratch/formal/deployment-control-plane-kernel-smoke"
)
CONTROLLER_REFINEMENT_COMMAND = (
    "uv run python scripts/check_formal_deployment_control_plane_kernel.py "
    "--refinement controller --out .scratch/formal/deployment-control-plane-kernel"
)
PROPERTY_ID = "DeploymentControlPlaneKernelAggregate"
INVARIANTS = (
    "TypeScopeExact",
    "SingleActiveAttempt",
    "SingleWriterEpoch",
    "LegalLifecycle",
    "AttemptContextImmutable",
    "ObservedCommit",
    "IdentityHistoryMonotonic",
    "RecoveryEquivalence",
    "ExactContextHealth",
    "ExactSingleUseApproval",
    "ProjectionConsistency",
    "CanonicalPathOnly",
    "FreshPublication",
    "OrderedIrreversibility",
    "SoleLedgerAuthority",
    "StagedAttemptIsolation",
    "PublicationVisibilityOrdering",
    "LifecycleAndDatabaseEpochsAreDistinct",
    "BroadMutationRequiresObservedReceiptCapability",
    "RecoveryGapsAreExplicit",
    "RollbackRestoresPredecessorUnderNewEpoch",
    "RollbackWriterEpochRotationBoundary",
    "AuthorizationOnlyRollbackSeparatesAuthorizationFromPublication",
    "TerminalRollbackHasNoCandidateAuthority",
    "InvalidatedReceiptCapabilityCannotMutate",
    "ExactCompensationPlan",
    "RollbackReceiptBindsCompensationPlan",
    "CompensationOrderIsReverse",
    "RollbackPublicationRequiresCompensation",
    "RollbackFinalizationRequiresCompensation",
)
TEMPORAL_PROPERTIES = ("RecoveryEventuallySettles",)
REFINEMENT_TESTS = (
    "tests/pact/test_deployment_control_plane_kernel_contract.py::test_typed_scope_is_exact",
    "tests/pact/test_deployment_control_plane_kernel_contract.py::test_production_reachability_registry_is_complete",
    "tests/pact/test_deployment_control_plane_kernel_contract.py::test_mutation_registry_covers_every_kernel_invariant",
    "tests/pact/test_deployment_control_plane_kernel_contract.py::test_refinement_owners_bind_concrete_controller_mapping",
    "tests/pact/test_deployment_control_plane_kernel_contract.py::test_aggregate_digest_binds_every_registry",
    "tests/pact/test_deployment_control_plane_kernel_contract.py::test_only_ledger_actions_commit_lifecycle_associations",
    "tests/pact/test_deployment_control_plane_kernel_contract.py::test_authorization_only_recovery_publishes_writer_epoch_atomically",
    "tests/pact/test_deployment_control_plane_kernel_contract.py::test_controller_refinement_predicates_are_concrete_and_mutation_sensitive",
)
CONTROLLER_REFINEMENT_TESTS = (
    "tests/substrate/test_local_fleet_controller.py::test_boundary_irreversible_failure_recovers_with_prepublication_rollback",
    "tests/substrate/test_local_fleet_controller.py::test_boundary_preauthorization_compensation_crash_retry_is_exact",
    "tests/substrate/test_local_fleet_controller.py::test_boundary_preauthorization_compensation_plan_refuses_corruption",
    "tests/substrate/test_local_fleet_controller.py::test_boundary_rollback_compensates_every_possibly_applied_phase",
    "tests/substrate/test_local_fleet_controller.py::test_boundary_rollback_retry_and_retirement_are_exact_and_resumable",
    "tests/substrate/test_local_fleet_controller.py::test_boundary_strict_newer_rollback_rotates_receipt_and_capability",
    "tests/substrate/test_local_fleet_controller.py::test_boundary_parallel_resumes_serialize_external_effect_invocation",
    "tests/substrate/test_local_fleet_controller.py::test_boundary_execution_lock_serializes_independent_processes",
)
CONTROLLER_RUNTIME_FILES = (
    ROOT / "controller/okg_local_controller/ledger.py",
    ROOT / "controller/okg_local_controller/effects.py",
    ROOT / "controller/okg_local_controller/controller.py",
    ROOT / "controller/okg_local_controller/reference_target.py",
    ROOT / "tests/substrate/test_local_fleet_controller.py",
    ROOT / "controller/okg_local_controller/protocol.py",
)
CHILD_CHANGES = (
    "externalize-local-okg-lifecycle-controller",
    "unify-deployment-control-plane-authority",
)
LEDGER_ASSOCIATION_FIELDS = (
    "lifecycleEpoch",
    "activationReceipt",
    "activationEpoch",
    "activationDatabaseWriterEpoch",
    "activationCandidate",
    "activationStagedDigest",
    "activationPredecessor",
)


@dataclass(frozen=True)
class Scenario:
    name: str
    environment: str
    failure: str
    approval: str
    effect_class: str


SCENARIOS = (
    Scenario("staging_success", "staging", "success", "none", "reversible"),
    Scenario("production_success", "production", "success", "valid", "reversible"),
    Scenario("admission_refusal", "staging", "admission_refusal", "none", "reversible"),
    Scenario(
        "reversible_failure", "staging", "reversible_failure", "none", "reversible"
    ),
    Scenario(
        "irreversible_ambiguity",
        "production",
        "irreversible_ambiguity",
        "valid",
        "irreversible",
    ),
    Scenario(
        "irreversible_postcommit_failure",
        "production",
        "irreversible_postcommit_failure",
        "valid",
        "irreversible",
    ),
    Scenario(
        "compensation_failure", "staging", "compensation_failure", "none", "reversible"
    ),
    Scenario("recover_admitted", "staging", "recover_admitted", "none", "reversible"),
    Scenario("recover_applying", "staging", "recover_applying", "none", "reversible"),
    Scenario("recover_verifying", "staging", "recover_verifying", "none", "reversible"),
    Scenario("recover_ready", "staging", "recover_ready", "none", "reversible"),
    Scenario("approval_stale", "production", "success", "stale", "reversible"),
    Scenario("approval_expired", "production", "success", "expired", "reversible"),
    Scenario("approval_revoked", "production", "success", "revoked", "reversible"),
    Scenario("approval_consumed", "production", "success", "consumed", "reversible"),
    Scenario("approval_unhealthy", "production", "success", "unhealthy", "reversible"),
    Scenario(
        "approval_incomplete", "production", "success", "incomplete", "reversible"
    ),
    Scenario(
        "approval_source_not_ready",
        "production",
        "success",
        "source_not_ready",
        "reversible",
    ),
)
SCENARIO_BY_NAME = {scenario.name: scenario for scenario in SCENARIOS}

# The per-PR profile is intentionally representative rather than exhaustive.
# Keep every selection named so a registry edit cannot silently change its
# coverage or make the smoke artifact look like the full PACT proof.
SMOKE_SCENARIO_NAMES = (
    "staging_success",
    "production_success",
    "reversible_failure",
    "recover_verifying",
    "approval_stale",
)


@dataclass(frozen=True)
class Reachability:
    name: str
    scenario: str
    invariant: str


REACHABILITY = (
    Reachability("lifecycle_planned", "staging_success", "NeverReachPlanned"),
    Reachability("lifecycle_admitted", "staging_success", "NeverReachAdmitted"),
    Reachability("lifecycle_applying", "staging_success", "NeverReachApplying"),
    Reachability("lifecycle_verifying", "staging_success", "NeverReachVerifying"),
    Reachability("lifecycle_ready", "staging_success", "NeverReachReady"),
    Reachability("lifecycle_promoted", "production_success", "NeverReachPromoted"),
    Reachability("admission_recovery", "admission_refusal", "NeverReachRecovering"),
    Reachability("reversible_rollback", "reversible_failure", "NeverReachRolledBack"),
    Reachability(
        "irreversible_ambiguity_rollback",
        "irreversible_ambiguity",
        "NeverReachRolledBack",
    ),
    Reachability(
        "irreversible_postcommit_rollback",
        "irreversible_postcommit_failure",
        "NeverReachRolledBack",
    ),
    Reachability(
        "compensation_failure_block", "compensation_failure", "NeverReachBlocked"
    ),
    Reachability("effect_intended", "staging_success", "NeverReachIntended"),
    Reachability("effect_applied", "staging_success", "NeverReachApplied"),
    Reachability("effect_observed", "staging_success", "NeverReachObserved"),
    Reachability("effect_committed", "staging_success", "NeverReachCommitted"),
    Reachability(
        "compensation_intended", "reversible_failure", "NeverReachCompensationIntended"
    ),
    Reachability(
        "compensation_applied", "reversible_failure", "NeverReachCompensationApplied"
    ),
    Reachability(
        "compensation_observed", "reversible_failure", "NeverReachCompensationObserved"
    ),
    Reachability(
        "compensation_committed", "reversible_failure", "NeverReachCompensated"
    ),
    Reachability("approval_consumed", "production_success", "NeverConsumeApproval"),
    Reachability("legacy_rejected", "staging_success", "NeverRejectLegacyMutation"),
    Reachability(
        "fresh_publication", "production_success", "NeverPublishFreshGeneration"
    ),
    Reachability("staged_capability", "staging_success", "NeverGrantStagedCapability"),
    Reachability(
        "staged_verification_writes", "staging_success", "NeverStageVerificationWrites"
    ),
    Reachability("publish_intent", "staging_success", "NeverRecordPublishIntent"),
    Reachability(
        "activation_authorized", "staging_success", "NeverAuthorizeActivation"
    ),
    Reachability(
        "authorized_publication", "staging_success", "NeverPublishAuthorizedGeneration"
    ),
    Reachability("final_readiness_observed", "staging_success", "NeverFinalizeReady"),
    Reachability(
        "authorization_only_recovered",
        "staging_success",
        "NeverRecoverAuthorizationOnly",
    ),
    Reachability(
        "published_unobserved_recovered",
        "staging_success",
        "NeverRecoverPublishedUnobserved",
    ),
    Reachability(
        "rollback_authorized",
        "recover_verifying",
        "NeverAuthorizeRollback",
    ),
    Reachability(
        "rollback_authorized_before_writer_epoch_rotation",
        "recover_verifying",
        "NeverReachRollbackAuthorizationBeforeWriterEpochRotation",
    ),
    Reachability(
        "predecessor_restored_under_new_epoch",
        "recover_verifying",
        "NeverRestorePredecessorUnderNewEpoch",
    ),
    Reachability(
        "atomic_rollback_writer_epoch_publication",
        "recover_verifying",
        "NeverReachAtomicRollbackWriterEpochPublication",
    ),
    Reachability(
        "rollback_finalized",
        "recover_verifying",
        "NeverFinalizeRollback",
    ),
    Reachability(
        "authorization_only_rollback_finalized",
        "recover_verifying",
        "NeverFinalizeAuthorizationOnlyRollback",
    ),
    Reachability(
        "compensation_plan_intended",
        "reversible_failure",
        "NeverPlanCompensationForIntended",
    ),
    Reachability(
        "compensation_plan_applied",
        "reversible_failure",
        "NeverPlanCompensationForApplied",
    ),
    Reachability(
        "compensation_plan_observed",
        "reversible_failure",
        "NeverPlanCompensationForObserved",
    ),
    Reachability(
        "compensation_plan_committed",
        "reversible_failure",
        "NeverPlanCompensationForCommitted",
    ),
    Reachability(
        "planned_effect_excluded_from_compensation",
        "recover_admitted",
        "NeverExcludePlannedFromCompensation",
    ),
    Reachability(
        "irreversible_effect_compensated",
        "irreversible_ambiguity",
        "NeverCompensateIrreversibleEffect",
    ),
    Reachability(
        "crash_before_authorization", "staging_success", "NeverCrashBeforeAuthorization"
    ),
    Reachability(
        "crash_authorized_before_publish",
        "staging_success",
        "NeverCrashAuthorizedBeforePublish",
    ),
    Reachability(
        "crash_published_before_final_observation",
        "staging_success",
        "NeverCrashPublishedBeforeFinalObservation",
    ),
    Reachability("stale_approval_rejected", "approval_stale", "NeverReachBlocked"),
    Reachability("expired_approval_rejected", "approval_expired", "NeverReachBlocked"),
    Reachability("revoked_approval_rejected", "approval_revoked", "NeverReachBlocked"),
    Reachability(
        "consumed_approval_rejected", "approval_consumed", "NeverReachBlocked"
    ),
    Reachability(
        "unhealthy_approval_rejected", "approval_unhealthy", "NeverReachBlocked"
    ),
    Reachability(
        "incomplete_approval_rejected", "approval_incomplete", "NeverReachBlocked"
    ),
    Reachability(
        "source_not_ready_approval_rejected",
        "approval_source_not_ready",
        "NeverReachBlocked",
    ),
    Reachability(
        "recovery_from_admitted", "recover_admitted", "NeverRecoverFromAdmitted"
    ),
    Reachability(
        "recovery_from_applying", "recover_applying", "NeverRecoverFromApplying"
    ),
    Reachability(
        "recovery_from_verifying", "recover_verifying", "NeverRecoverFromVerifying"
    ),
    Reachability("recovery_from_ready", "recover_ready", "NeverRecoverFromReady"),
    Reachability("crash_at_planned", "staging_success", "NeverCrashPlanned"),
    Reachability("crash_at_admitted", "staging_success", "NeverCrashAdmitted"),
    Reachability(
        "crash_at_effect_intended", "staging_success", "NeverCrashEffectIntended"
    ),
    Reachability(
        "crash_at_effect_applied", "staging_success", "NeverCrashEffectApplied"
    ),
    Reachability(
        "crash_at_effect_observed", "staging_success", "NeverCrashEffectObserved"
    ),
    Reachability(
        "crash_at_effect_committed", "staging_success", "NeverCrashEffectCommitted"
    ),
    Reachability(
        "crash_at_compensation_intended",
        "reversible_failure",
        "NeverCrashCompensationIntended",
    ),
    Reachability(
        "crash_at_compensation_applied",
        "reversible_failure",
        "NeverCrashCompensationApplied",
    ),
    Reachability(
        "crash_at_compensation_observed",
        "reversible_failure",
        "NeverCrashCompensationObserved",
    ),
    Reachability("crash_at_compensated", "reversible_failure", "NeverCrashCompensated"),
    Reachability(
        "changed_staging_rejected", "production_success", "NeverRejectChangedStaging"
    ),
    Reachability(
        "stopped_mutation_rejected", "staging_success", "NeverRejectStoppedMutation"
    ),
)
REACHABILITY_BY_NAME = {witness.name: witness for witness in REACHABILITY}
SMOKE_REACHABILITY_NAMES = (
    "lifecycle_ready",
    "lifecycle_promoted",
    "reversible_rollback",
    "authorization_only_rollback_finalized",
    "stale_approval_rejected",
)


@dataclass(frozen=True)
class Replacement:
    old: str
    new: str


@dataclass(frozen=True)
class Mutation:
    name: str
    scenario: str
    invariant: str
    replacements: tuple[Replacement, ...]
    kind: str = "INVARIANT"


MUTATIONS = (
    Mutation(
        "k0_invalid_version",
        "staging_success",
        "TypeScopeExact",
        (
            Replacement(
                "!.version = 1,\n                                             !.workflowId",
                "!.version = 99,\n                                             !.workflowId",
            ),
        ),
    ),
    Mutation(
        "k1_second_active_attempt",
        "staging_success",
        "SingleActiveAttempt",
        (
            Replacement(
                "!.activeAttempts = {AttemptId}]",
                '!.activeAttempts = {AttemptId, "attempt-2"}]',
            ),
        ),
    ),
    Mutation(
        "k2_stale_writer_epoch",
        "staging_success",
        "SingleWriterEpoch",
        (
            Replacement(
                "!.databaseWriterEpoch = attemptIdentity.databaseWriterEpoch,",
                "!.databaseWriterEpoch = 1,",
            ),
        ),
    ),
    Mutation(
        "k3_illegal_terminal_edge",
        "staging_success",
        "LegalLifecycle",
        (
            Replacement(
                'eventHistory \\cup {<<"ready", "promoted">>}',
                'eventHistory \\cup {<<"applying", "blocked">>}',
            ),
        ),
    ),
    Mutation(
        "k4_context_overwrite",
        "staging_success",
        "AttemptContextImmutable",
        (
            Replacement(
                "admissionContext |-> Context, targetContext |-> Context,",
                'admissionContext |-> Context, targetContext |-> "foreign-context",',
            ),
        ),
    ),
    Mutation(
        "k5_commit_conflicting_observation",
        "irreversible_ambiguity",
        "ObservedCommit",
        (
            Replacement(
                'effectState\' = [effectState EXCEPT !.observation = "conflicting"]',
                'effectState\' = [effectState EXCEPT !.phase = "observed", !.observation = "conflicting"]',
            ),
            Replacement(
                '/\\ effectState.observation = "exact"\n    /\\ "reserved"',
                '/\\ TRUE\n    /\\ "reserved"',
            ),
        ),
    ),
    Mutation(
        "k6_identity_history_reuse",
        "staging_success",
        "IdentityHistoryMonotonic",
        (
            Replacement(
                'identityClaims\' = identityClaims \\cup {"consumed"}',
                'identityClaims\' = {"consumed"}',
            ),
        ),
    ),
    Mutation(
        "k7_unsafe_rollback",
        "reversible_failure",
        "RecoveryEquivalence",
        (
            Replacement(
                '/\\ environmentState.rollbackPhase = "restored"\n'
                "    /\\ environmentState.activationReceipt = RollbackReceipt\n"
                "    /\\ environmentState.activationEpoch = RollbackLifecycleEpoch\n"
                "    /\\ environmentState.activationDatabaseWriterEpoch = RollbackDatabaseWriterEpoch\n"
                "    /\\ environmentState.targetDatabaseReceipt = RollbackReceipt\n"
                "    /\\ environmentState.currentGeneration = PredecessorGeneration\n"
                "    /\\ ActivationReceipt \\in environmentState.invalidatedReceipts\n"
                "    /\\ CompensationPlanReady\n"
                "    /\\ CompensationComplete",
                '/\\ environmentState.rollbackPhase = "authorized"\n'
                "    /\\ CompensationPlanReady",
            ),
        ),
    ),
    Mutation(
        "k8_stale_health",
        "staging_success",
        "ExactContextHealth",
        (
            Replacement(
                "healthResults' = [context |-> Context, generation |-> TargetGeneration,",
                'healthResults\' = [context |-> "stale-context", generation |-> TargetGeneration,',
            ),
        ),
    ),
    Mutation(
        "k9_approval_guard_removed",
        "approval_stale",
        "ExactSingleUseApproval",
        (Replacement("/\\ Environment = Staging \\/ ApprovalEligible", "/\\ TRUE"),),
    ),
    Mutation(
        "k10_fabricated_projection",
        "staging_success",
        "ProjectionConsistency",
        (Replacement("environmentState.currentContext>>}", '"fabricated-context">>}'),),
    ),
    Mutation(
        "k11_legacy_capability_bypass",
        "staging_success",
        "CanonicalPathOnly",
        (
            Replacement(
                'ELSE IF capability = "legacy" THEN "legacy_rejected"',
                'ELSE IF capability = "legacy" THEN "legacy_accepted"',
            ),
        ),
    ),
    Mutation(
        "k12_generation_reuse",
        "staging_success",
        "FreshPublication",
        (
            Replacement(
                "!.currentGeneration = TargetGeneration",
                "!.currentGeneration = SourceGeneration",
            ),
            Replacement(
                "healthResults' = [context |-> Context, generation |-> TargetGeneration,",
                "healthResults' = [context |-> Context, generation |-> SourceGeneration,",
            ),
        ),
    ),
    Mutation(
        "k13_irreversible_before_predecessors",
        "irreversible_ambiguity",
        "OrderedIrreversibility",
        (
            Replacement(
                '/\\ effectState.kind = "reversible" \\/ effectState.predecessorsCommitted',
                "/\\ TRUE",
            ),
        ),
    ),
    Mutation(
        "sole_ledger_authority_changed",
        "staging_success",
        "SoleLedgerAuthority",
        (
            Replacement(
                'ledgerAuthority |-> "external-controller-ledger",',
                'ledgerAuthority |-> "candidate-owned-ledger",',
            ),
        ),
    ),
    Mutation(
        "staged_capability_bypass",
        "staging_success",
        "StagedAttemptIsolation",
        (
            Replacement(
                "!.stagedWrites = TRUE]",
                '!.stagedWrites = TRUE, !.stagedCapability = "absent"]',
            ),
        ),
    ),
    Mutation(
        "publication_visibility_before_outcome",
        "staging_success",
        "PublicationVisibilityOrdering",
        (
            Replacement(
                "/\\ environmentState.activationReceipt = ActivationReceipt\n    /\\ environmentState.activationEpoch = environmentState.lifecycleEpoch\n    /\\ environmentState.activationDatabaseWriterEpoch = attemptIdentity.databaseWriterEpoch\n    /\\ attemptIdentity.databaseWriterEpoch = environmentState.databaseWriterEpoch + 1\n    /\\ environmentState.activationCandidate = Artifact\n    /\\ environmentState.activationStagedDigest = StagedDigest\n    /\\ environmentState.activationPredecessor = PredecessorGeneration",
                "/\\ TRUE",
            ),
        ),
    ),
    Mutation(
        "outbox_dual_commit",
        "staging_success",
        "LegalLifecycle",
        (
            Replacement(
                'outbox\' = [outbox EXCEPT !.phase = "delivered"]', "outbox' = outbox"
            ),
        ),
    ),
    Mutation(
        "compensation_overwrites_forward_journal",
        "reversible_failure",
        "RecoveryEquivalence",
        (
            Replacement(
                '/\\ compensationState\' = [compensationState EXCEPT !.phase = "intended"]\n    /\\ UNCHANGED <<environmentState, attemptIdentity, attemptState, outbox,\n                    effectState, observations',
                '/\\ compensationState\' = [compensationState EXCEPT !.phase = "intended"]\n    /\\ effectState\' = [effectState EXCEPT !.phase = "planned"]\n    /\\ UNCHANGED <<environmentState, attemptIdentity, attemptState, outbox,\n                    observations',
            ),
        ),
    ),
    Mutation(
        "crashed_commit_before_restart",
        "staging_success",
        "RecoveryEquivalence",
        (
            Replacement(
                'CommitEffect ==\n    /\\ attemptState.lifecycle = "applying"\n    /\\ attemptState.running',
                'CommitEffect ==\n    /\\ attemptState.lifecycle = "applying"',
            ),
        ),
    ),
    Mutation(
        "stopped_canonical_mutation_bypass",
        "staging_success",
        "CanonicalPathOnly",
        (
            Replacement(
                'THEN IF attemptState.running THEN "canonical_accepted" ELSE "stopped_rejected"',
                'THEN IF attemptState.running THEN "canonical_accepted" ELSE "stopped_accepted"',
            ),
        ),
    ),
    Mutation(
        "recovery_fairness_removed",
        "reversible_failure",
        "RecoveryEventuallySettles",
        (
            Replacement(
                "       /\\ WF_vars(RequestRecovery)\n       /\\ WF_vars(RecoveryStep)",
                "       /\\ WF_vars(RequestRecovery)",
            ),
        ),
        "PROPERTY",
    ),
    Mutation(
        "irreversible_failure_can_promote",
        "irreversible_postcommit_failure",
        "RecoveryEquivalence",
        (
            Replacement(
                '    /\\ FailureMode # "irreversible_postcommit_failure"\n    /\\ effectState.phase = "committed"',
                '    /\\ effectState.phase = "committed"',
            ),
        ),
    ),
    Mutation(
        "lifecycle_database_epoch_alias",
        "staging_success",
        "LifecycleAndDatabaseEpochsAreDistinct",
        (
            Replacement(
                "RollbackDatabaseWriterEpoch == 3", "RollbackDatabaseWriterEpoch == 2"
            ),
        ),
    ),
    Mutation(
        "broad_mutation_before_final_observation",
        "staging_success",
        "BroadMutationRequiresObservedReceiptCapability",
        (
            Replacement(
                '/\\ environmentState.finalObservation\n              /\\ environmentState.producerCapabilities = "active"',
                "/\\ TRUE",
            ),
        ),
    ),
    Mutation(
        "authorization_recovery_claimed_without_recovery",
        "staging_success",
        "RecoveryGapsAreExplicit",
        (
            Replacement(
                '!.stagedCapability = "authorized",\n          !.recoveryMode = "read_only_stale"]',
                '!.stagedCapability = "authorized",\n          !.recoveryMode = "read_only_stale",\n          !.authorizationRecovered = TRUE]',
            ),
        ),
    ),
    Mutation(
        "rollback_reuses_lifecycle_epoch",
        "recover_verifying",
        "RollbackRestoresPredecessorUnderNewEpoch",
        (
            Replacement(
                "!.lifecycleEpoch = RollbackLifecycleEpoch,",
                "!.lifecycleEpoch = CandidateLifecycleEpoch,",
            ),
        ),
    ),
    Mutation(
        "rollback_writer_epoch_rotates_during_authorization",
        "recover_verifying",
        "RollbackWriterEpochRotationBoundary",
        (
            Replacement(
                "!.lifecycleEpoch = RollbackLifecycleEpoch,\n"
                "          !.activationReceipt = RollbackReceipt,",
                "!.lifecycleEpoch = RollbackLifecycleEpoch,\n"
                "          !.databaseWriterEpoch = RollbackDatabaseWriterEpoch,\n"
                "          !.activationReceipt = RollbackReceipt,",
            ),
        ),
    ),
    Mutation(
        "rollback_writer_epoch_not_rotated_at_publication",
        "recover_verifying",
        "RollbackWriterEpochRotationBoundary",
        (
            Replacement(
                "!.databaseWriterEpoch = RollbackDatabaseWriterEpoch,\n"
                "          !.targetDatabaseReceipt = RollbackReceipt,",
                "!.databaseWriterEpoch = environmentState.databaseWriterEpoch,\n"
                "          !.targetDatabaseReceipt = RollbackReceipt,",
            ),
        ),
    ),
    Mutation(
        "old_receipt_remains_current_after_rollback",
        "recover_verifying",
        "InvalidatedReceiptCapabilityCannotMutate",
        (
            Replacement(
                "!.activationReceipt = RollbackReceipt,",
                "!.activationReceipt = ActivationReceipt,",
            ),
        ),
    ),
    Mutation(
        "authorization_only_rollback_keeps_candidate_receipt",
        "recover_verifying",
        "AuthorizationOnlyRollbackSeparatesAuthorizationFromPublication",
        (
            Replacement(
                "!.activationReceipt = RollbackReceipt,",
                "!.activationReceipt = ActivationReceipt,",
            ),
        ),
    ),
    Mutation(
        "authorization_only_settles_without_rotating_authority",
        "recover_verifying",
        "TerminalRollbackHasNoCandidateAuthority",
        (
            Replacement(
                '!.producerCapabilities = "active",\n          !.rollbackPhase = "observed",',
                '!.producerCapabilities = "active",\n          !.activationReceipt = ActivationReceipt,\n          !.rollbackPhase = "observed",',
            ),
        ),
    ),
    Mutation(
        "compensation_phase_selection_only_committed",
        "reversible_failure",
        "ExactCompensationPlan",
        (
            Replacement(
                "!.required = effectState.phase \\in PossiblyAppliedPhases,",
                '!.required = effectState.phase = "committed",',
            ),
        ),
    ),
    Mutation(
        "planned_effect_included_in_compensation",
        "recover_admitted",
        "ExactCompensationPlan",
        (
            Replacement(
                "!.required = effectState.phase \\in PossiblyAppliedPhases,",
                '!.required = effectState.phase \\in PossiblyAppliedPhases \\cup {"planned"},',
            ),
        ),
    ),
    Mutation(
        "compensation_plan_not_exact",
        "reversible_failure",
        "ExactCompensationPlan",
        (Replacement('!.plan = "exact",', '!.plan = "partial",'),),
    ),
    Mutation(
        "compensation_plan_bound_to_candidate_receipt",
        "reversible_failure",
        "RollbackReceiptBindsCompensationPlan",
        (
            Replacement(
                "!.boundReceipt = RollbackReceipt,",
                "!.boundReceipt = ActivationReceipt,",
            ),
        ),
    ),
    Mutation(
        "compensation_order_forward",
        "reversible_failure",
        "CompensationOrderIsReverse",
        (Replacement('!.order = "reverse"]', '!.order = "forward"]'),),
    ),
    Mutation(
        "rollback_publication_before_compensation",
        "reversible_failure",
        "RollbackPublicationRequiresCompensation",
        (
            Replacement(
                '/\\ environmentState.rollbackPhase = "authorized"\n'
                "    /\\ CompensationPlanReady\n"
                "    /\\ CompensationComplete\n"
                "    /\\ environmentState' = [environmentState EXCEPT",
                '/\\ environmentState.rollbackPhase = "authorized"\n'
                "    /\\ CompensationPlanReady\n"
                "    /\\ environmentState' = [environmentState EXCEPT",
            ),
        ),
    ),
    Mutation(
        "rollback_finalization_before_compensation",
        "reversible_failure",
        "RollbackFinalizationRequiresCompensation",
        (
            Replacement(
                '/\\ environmentState.rollbackPhase = "restored"\n'
                "    /\\ environmentState.activationReceipt = RollbackReceipt\n"
                "    /\\ environmentState.activationEpoch = RollbackLifecycleEpoch\n"
                "    /\\ environmentState.activationDatabaseWriterEpoch = RollbackDatabaseWriterEpoch\n"
                "    /\\ environmentState.targetDatabaseReceipt = RollbackReceipt\n"
                "    /\\ environmentState.currentGeneration = PredecessorGeneration\n"
                "    /\\ ActivationReceipt \\in environmentState.invalidatedReceipts\n"
                "    /\\ CompensationPlanReady\n"
                "    /\\ CompensationComplete",
                '/\\ environmentState.rollbackPhase = "authorized"\n'
                "    /\\ CompensationPlanReady",
            ),
        ),
    ),
)
MUTATION_BY_NAME = {mutation.name: mutation for mutation in MUTATIONS}
SMOKE_MUTATION_NAMES = (
    "k7_unsafe_rollback",
    "k9_approval_guard_removed",
    "crashed_commit_before_restart",
    "authorization_only_rollback_keeps_candidate_receipt",
)


def _ast_member_source(path: Path, *, class_name: str, method_name: str) -> str:
    text = path.read_text(encoding="utf-8")
    tree = ast.parse(text, filename=str(path))
    classes = [
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == class_name
    ]
    if len(classes) != 1:
        raise RuntimeError(
            f"controller refinement expected one class {class_name} in {path}"
        )
    methods = [
        node
        for node in classes[0].body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == method_name
    ]
    if len(methods) != 1:
        raise RuntimeError(
            f"controller refinement expected one {class_name}.{method_name}"
        )
    source = ast.get_source_segment(text, methods[0])
    if source is None:
        raise RuntimeError(
            f"controller refinement could not recover {class_name}.{method_name}"
        )
    return source


def _tla_operator_source(text: str, name: str, next_name: str) -> str:
    start = text.index(f"{name} ==")
    end = text.index(f"\n{next_name} ==", start)
    return text[start:end]


def _require_source_predicate(
    results: dict[str, dict[str, object]],
    predicate_id: str,
    *,
    source: str,
    condition: bool,
    detail: str,
) -> None:
    if not condition:
        raise RuntimeError(
            f"controller refinement source predicate failed: {predicate_id}: {detail}"
        )
    results[predicate_id] = {
        "status": "pass",
        "detail": detail,
        "source_digest": "sha256:" + hashlib.sha256(source.encode()).hexdigest(),
    }


def _controller_source_predicates() -> dict[str, dict[str, object]]:
    ledger = CONTROLLER_RUNTIME_FILES[0]
    effects = CONTROLLER_RUNTIME_FILES[1]
    controller = CONTROLLER_RUNTIME_FILES[2]
    target = CONTROLLER_RUNTIME_FILES[3]
    tests = CONTROLLER_RUNTIME_FILES[4]
    protocol = CONTROLLER_RUNTIME_FILES[5]
    ledger_text = ledger.read_text(encoding="utf-8")
    controller_text = controller.read_text(encoding="utf-8")
    protocol_text = protocol.read_text(encoding="utf-8")
    tests_text = tests.read_text(encoding="utf-8")
    expected = _ast_member_source(
        ledger,
        class_name="ControllerLedger",
        method_name="_expected_compensation_plan",
    )
    validate = _ast_member_source(
        ledger,
        class_name="ControllerLedger",
        method_name="_validate_compensation_plan",
    )
    materialize = _ast_member_source(
        ledger,
        class_name="ControllerLedger",
        method_name="_materialize_compensation_plan",
    )
    authorize = _ast_member_source(
        ledger, class_name="ControllerLedger", method_name="authorize_rollback"
    )
    publish = _ast_member_source(
        ledger, class_name="ControllerLedger", method_name="record_publication"
    )
    finalize = _ast_member_source(
        ledger, class_name="ControllerLedger", method_name="finalize_observation"
    )
    runner = _ast_member_source(
        effects, class_name="CompensationRunner", method_name="run_all"
    )
    rollback = _ast_member_source(
        controller, class_name="LocalLifecycleController", method_name="rollback"
    )
    resume = _ast_member_source(
        controller, class_name="LocalLifecycleController", method_name="resume"
    )
    target_publish = _ast_member_source(
        target,
        class_name="SQLiteReferenceTarget",
        method_name="publish_authorized_generation",
    )
    model = MODULE.read_text(encoding="utf-8")
    model_authorize_rollback = _tla_operator_source(
        model, "AuthorizeRollback", "CompensationIntent"
    )
    model_restore_predecessor = _tla_operator_source(
        model, "RestorePredecessorUnderNewEpoch", "ObserveAndFinalizeRollback"
    )
    results: dict[str, dict[str, object]] = {}
    selected = {
        phase
        for phase in ("INTENDED", "APPLIED", "OBSERVED", "COMMITTED", "PLANNED")
        if f"EffectPhase.{phase}" in expected
    }
    _require_source_predicate(
        results,
        "ledger.possibly-applied-phase-selection",
        source=expected,
        condition=selected == {"INTENDED", "APPLIED", "OBSERVED", "COMMITTED"},
        detail="exactly INTENDED/APPLIED/OBSERVED/COMMITTED are selected; PLANNED is excluded",
    )
    _require_source_predicate(
        results,
        "ledger.reverse-exact-receipt-bound-plan",
        source=expected + validate + materialize,
        condition=(
            "ORDER BY ordinal DESC" in expected
            and '"rollback_receipt_digest": rollback_receipt.digest' in expected
            and "compensation_predecessors = (compensation_key,)" in expected
            and "len(existing) != len(expected)" in validate
            and "zip(existing, expected, strict=True)" in validate
            and '!.plan = "partial"' not in materialize
            and "self._validate_compensation_plan" in materialize
        ),
        detail="plan is reverse ordered, receipt bound, exact-length, immutable, and retry validated",
    )
    _require_source_predicate(
        results,
        "ledger.rollback-authorization-materializes-plan",
        source=authorize,
        condition=(
            authorize.count("self._materialize_compensation_plan(") == 2
            and "if existing is not None:" in authorize
            and "if failed is None" in authorize
        ),
        detail="both new authorization and retry materialize/validate the plan, including preauthorization failure",
    )
    _require_source_predicate(
        results,
        "ledger.rollback-generation-is-derived",
        source=authorize + protocol_text,
        condition=(
            "staged_predecessor_digest" not in rollback
            and "rollback_generation_digest(" in authorize
            and '"okg.local-controller.rollback-generation/v1"' in protocol_text
            and '"predecessor_digest": predecessor_digest' in protocol_text
            and '"expected_predecessor": expected_predecessor' in protocol_text
        ),
        detail="rollback staged identity is derived from durable predecessor bytes and generation; callers cannot supply it",
    )
    _require_source_predicate(
        results,
        "ledger.rollback-publication-compensation-guard",
        source=publish,
        condition=(
            'if kind == "rollback":' in publish
            and "self._assert_rollback_compensated(" in publish
            and publish.index("self._assert_rollback_compensated(")
            < publish.index('if row["published"]:')
        ),
        detail="rollback publication checks exact committed compensation before idempotent publication",
    )
    _require_source_predicate(
        results,
        "ledger.rollback-finalization-compensation-guard",
        source=finalize,
        condition=(
            'if kind == "rollback":' in finalize
            and "self._assert_rollback_compensated(" in finalize
            and finalize.index("self._assert_rollback_compensated(")
            < finalize.index('if row["observed"]:')
        ),
        detail="rollback final observation checks exact committed compensation before idempotent finalization",
    )
    _require_source_predicate(
        results,
        "runner.observe-before-apply-and-resume",
        source=runner,
        condition=(
            runner.index("prior = adapter.observe_compensation(context)")
            < runner.index("applied = adapter.compensate(context)")
            and all(
                f"EffectPhase.{phase}" in runner
                for phase in ("PLANNED", "INTENDED", "APPLIED", "OBSERVED", "COMMITTED")
            )
            and 'set(item["predecessors"]) <= committed' in runner
        ),
        detail="runner resumes every phase, observes before retrying apply, and honors the durable predecessor chain",
    )
    _require_source_predicate(
        results,
        "controller.compensate-before-publish-before-finalize",
        source=rollback,
        condition=(
            rollback.index("CompensationRunner(")
            < rollback.index("target.publish_authorized_generation(receipt)")
            < rollback.index("self.ledger.record_publication(")
            < rollback.index("self.ledger.finalize_observation(")
        ),
        detail="controller completes compensation before target publication and ledger finalization",
    )
    _require_source_predicate(
        results,
        "model-runtime.rollback-writer-epoch-publication-boundary",
        source=(
            model_authorize_rollback
            + model_restore_predecessor
            + authorize
            + rollback
            + target_publish
        ),
        condition=(
            "!.databaseWriterEpoch = RollbackDatabaseWriterEpoch"
            not in model_authorize_rollback
            and "!.activationDatabaseWriterEpoch = RollbackDatabaseWriterEpoch"
            in model_authorize_rollback
            and "!.databaseWriterEpoch = RollbackDatabaseWriterEpoch"
            in model_restore_predecessor
            and model_restore_predecessor.index("CompensationComplete")
            < model_restore_predecessor.index(
                "!.databaseWriterEpoch = RollbackDatabaseWriterEpoch"
            )
            and "advance_database_writer_epoch" not in authorize
            and "UPDATE target_authority" not in authorize
            and rollback.index("CompensationRunner(")
            < rollback.index("target.publish_authorized_generation(receipt)")
            and target_publish.index('conn.execute("BEGIN IMMEDIATE")')
            < target_publish.index("UPDATE target_authority SET current_generation=?")
            < target_publish.index("conn.commit()")
            and "activation_receipt_digest=?, lifecycle_activation_epoch=?"
            in target_publish
            and "database_writer_epoch=?" in target_publish
            and "receipt.database_writer_epoch" in target_publish
            and "receipt.expected_database_writer_epoch" in target_publish
        ),
        detail=(
            "model and runtime keep the target writer epoch unchanged at rollback "
            "authorization and rotate it atomically with predecessor publication"
        ),
    )
    _require_source_predicate(
        results,
        "target.predecessor-receipt-epoch-cas",
        source=target_publish,
        condition=all(
            token in target_publish
            for token in (
                'authority["current_generation"] != receipt.expected_predecessor',
                'authority["activation_receipt_digest"]',
                "receipt.expected_activation_receipt_digest",
                'authority["database_writer_epoch"]',
                "receipt.expected_database_writer_epoch",
                'conn.execute("BEGIN IMMEDIATE")',
            )
        ),
        detail="target publication CAS binds predecessor generation, predecessor receipt, and database-writer epoch",
    )
    _require_source_predicate(
        results,
        "controller.postpublication-unfreeze",
        source=resume,
        condition=(
            "runner.run_prepublication(transition_id)" in resume
            and resume.index("self.ledger.record_publication(")
            < resume.index('runner.run_effect_key(transition_id, "unfreeze")')
            < resume.index("self.ledger.finalize_observation(")
            and resume.count("target.observe_authorized_generation(receipt)") >= 2
            and resume.index('runner.run_effect_key(transition_id, "unfreeze")')
            < resume.index(
                "target.observe_authorized_generation(receipt),",
                resume.index('runner.run_effect_key(transition_id, "unfreeze")'),
            )
        ),
        detail="unfreeze stays planned through authorization/publication and finalization uses a fresh post-unfreeze observation",
    )
    _require_source_predicate(
        results,
        "controller.serialized-external-invocation",
        source=ledger_text + controller_text + tests_text,
        condition=(
            "def execution_lock(" in ledger_text
            and "fcntl.flock(descriptor, fcntl.LOCK_EX)" in ledger_text
            and "with self.ledger.execution_lock():" in controller_text
            and controller_text.count("@_serialized_execution") == 3
            and "test_boundary_parallel_resumes_serialize_external_effect_invocation"
            in tests_text
            and "assert all(count == 1 for count in racing.apply_counts.values())"
            in tests_text
            and "test_boundary_execution_lock_serializes_independent_processes"
            in tests_text
            and 'multiprocessing.get_context("spawn")' in tests_text
            and 'assert len({event["pid"] for event in events}) == 2' in tests_text
        ),
        detail="resume, rollback, and retirement serialize external invocation; thread and independent-process races prove single apply and cross-process exclusion",
    )
    return results


def _run_controller_refinement(out: Path, timeout: int) -> Path:
    missing = [
        str(path.relative_to(ROOT))
        for path in CONTROLLER_RUNTIME_FILES
        if not path.is_file()
    ]
    if missing:
        raise RuntimeError(
            f"controller refinement runtime artifacts are missing: {missing}"
        )
    predicates = _controller_source_predicates()
    command = [
        "uv",
        "run",
        "--extra",
        "dev",
        "python",
        "-m",
        "pytest",
        "-q",
        *CONTROLLER_REFINEMENT_TESTS,
    ]
    completed = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
        timeout=timeout,
    )
    output = completed.stdout + completed.stderr
    if completed.returncode != 0:
        raise RuntimeError(
            "controller refinement executable tests failed:\n" + output[-4000:]
        )
    obligations = {
        "AuthorizeRollback": {
            "source_predicates": [
                "ledger.possibly-applied-phase-selection",
                "ledger.rollback-authorization-materializes-plan",
                "ledger.rollback-generation-is-derived",
                "ledger.reverse-exact-receipt-bound-plan",
            ],
            "tests": list(CONTROLLER_REFINEMENT_TESTS[0:4]),
        },
        "CompensationIntentApplyObserveCommit": {
            "source_predicates": [
                "runner.observe-before-apply-and-resume",
                "controller.serialized-external-invocation",
            ],
            "tests": list(CONTROLLER_REFINEMENT_TESTS[1:5]),
        },
        "ExactCompensationPlan": {
            "source_predicates": [
                "ledger.possibly-applied-phase-selection",
                "ledger.reverse-exact-receipt-bound-plan",
            ],
            "tests": list(CONTROLLER_REFINEMENT_TESTS[0:5]),
        },
        "RollbackReceiptBindsCompensationPlan": {
            "source_predicates": [
                "ledger.reverse-exact-receipt-bound-plan",
                "target.predecessor-receipt-epoch-cas",
            ],
            "tests": list(CONTROLLER_REFINEMENT_TESTS[1:6]),
        },
        "CompensationOrderIsReverse": {
            "source_predicates": [
                "ledger.reverse-exact-receipt-bound-plan",
                "runner.observe-before-apply-and-resume",
            ],
            "tests": list(CONTROLLER_REFINEMENT_TESTS[3:5]),
        },
        "RollbackPublicationRequiresCompensation": {
            "source_predicates": [
                "ledger.rollback-publication-compensation-guard",
                "controller.compensate-before-publish-before-finalize",
                "model-runtime.rollback-writer-epoch-publication-boundary",
                "target.predecessor-receipt-epoch-cas",
            ],
            "tests": list(CONTROLLER_REFINEMENT_TESTS[0:6]),
        },
        "RollbackWriterEpochRotationBoundary": {
            "source_predicates": [
                "model-runtime.rollback-writer-epoch-publication-boundary",
                "target.predecessor-receipt-epoch-cas",
            ],
            "tests": list(CONTROLLER_REFINEMENT_TESTS[0:6]),
        },
        "RollbackFinalizationRequiresCompensation": {
            "source_predicates": [
                "ledger.rollback-finalization-compensation-guard",
                "controller.compensate-before-publish-before-finalize",
            ],
            "tests": list(CONTROLLER_REFINEMENT_TESTS[0:5]),
        },
    }
    artifact = out / "controller-refinement.json"
    payload = {
        "property_id": "DeploymentControlPlaneControllerRefinement",
        "status": "pass",
        "proof_kind": "concrete_ast_predicates_plus_executable_tests",
        "model_digest": _sha256(MODULE),
        "checker_digest": _sha256(Path(__file__)),
        "runtime_artifacts": {
            str(path.relative_to(ROOT)): _sha256(path)
            for path in CONTROLLER_RUNTIME_FILES
        },
        "source_predicates": predicates,
        "obligations": obligations,
        "test_command": " ".join(command),
        "test_output_digest": "sha256:" + hashlib.sha256(output.encode()).hexdigest(),
        "test_output": output.strip(),
    }
    artifact.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return artifact


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _registry_digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _scenario_config(
    scenario: Scenario, *, check: str | None = None, kind: str = "INVARIANT"
) -> str:
    text = CONFIG.read_text(encoding="utf-8")
    replacements = {
        "  Environment = staging\n": f"  Environment = {scenario.environment}\n",
        '  FailureMode = "success"\n': f'  FailureMode = "{scenario.failure}"\n',
        '  ApprovalMode = "none"\n': f'  ApprovalMode = "{scenario.approval}"\n',
        '  EffectClass = "reversible"\n': f'  EffectClass = "{scenario.effect_class}"\n',
    }
    for old, new in replacements.items():
        if text.count(old) != 1:
            raise RuntimeError(f"scenario config anchor is not unique: {old!r}")
        text = text.replace(old, new, 1)
    if check is not None:
        prefix, marker, _ = text.partition("\nINVARIANTS\n")
        if not marker:
            raise RuntimeError("base config has no INVARIANTS section")
        text = f"{prefix}\n\n{kind}\n  {check}\n"
    return text


def _mutated_module(mutation: Mutation) -> str:
    text = MODULE.read_text(encoding="utf-8")
    for replacement in mutation.replacements:
        count = text.count(replacement.old)
        if count != 1:
            raise RuntimeError(
                f"mutation {mutation.name} anchor occurs {count} times: {replacement.old!r}"
            )
        text = text.replace(replacement.old, replacement.new, 1)
    if text == MODULE.read_text(encoding="utf-8"):
        raise RuntimeError(f"mutation {mutation.name} did not change the module")
    return text


def _final_distinct_states(output: str) -> int | None:
    """Return TLC's completed-state count, ignoring progress snapshots."""
    matches = re.findall(r"([0-9,]+) distinct states found", output)
    return int(matches[-1].replace(",", "")) if matches else None


def _run_tlc(
    module: Path, config: Path, *, out_dir: Path, timeout: int
) -> dict[str, object]:
    java = shutil.which("java")
    if java is None or not JAR.is_file():
        raise RuntimeError(
            "Java and the pinned .tools/tla2tools.jar are required; run "
            "`uv run python scripts/bootstrap_formal_admission.py`"
        )
    meta = out_dir / f"tlc-meta-{config.stem}"
    workers = tlc_workers()
    command = [
        java,
        *tlc_jvm_args(workers),
        "-cp",
        str(JAR),
        "tlc2.TLC",
        "-workers",
        workers,
        "-metadir",
        str(meta),
        "-config",
        str(config),
        str(module),
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = (
            exc.stdout.decode(errors="replace")
            if isinstance(exc.stdout, bytes)
            else (exc.stdout or "")
        )
        stderr = (
            exc.stderr.decode(errors="replace")
            if isinstance(exc.stderr, bytes)
            else (exc.stderr or "")
        )
        return {
            "command": command,
            "returncode": 124,
            "output": stdout + stderr,
            "distinct_states": None,
            "timed_out": True,
        }
    output = completed.stdout + completed.stderr
    return {
        "command": command,
        "returncode": completed.returncode,
        "output": output,
        "distinct_states": _final_distinct_states(output),
        "timed_out": False,
    }


def _write_log(path: Path, result: dict[str, object]) -> None:
    path.write_text(str(result["output"]), encoding="utf-8")


def _validate_contract() -> dict[str, object]:
    text = MODULE.read_text(encoding="utf-8")
    config = CONFIG.read_text(encoding="utf-8")
    missing = [
        name for name in (*INVARIANTS, *TEMPORAL_PROPERTIES) if f"{name} ==" not in text
    ]
    if missing:
        raise RuntimeError(f"model omits obligations: {missing}")
    configured = tuple(re.findall(r"^  ([A-Za-z][A-Za-z0-9_]*)$", config, re.MULTILINE))
    expected = (*INVARIANTS, *TEMPORAL_PROPERTIES)
    if configured != expected:
        raise RuntimeError(
            f"config obligations differ: expected={expected}, actual={configured}"
        )
    if "Mutant" in text or "EnabledMutant" in text:
        raise RuntimeError("safe module contains a mutant selector")
    refinement = yaml.safe_load(REFINEMENTS.read_text(encoding="utf-8"))
    if (
        refinement.get("claim") != "concrete_controller_mapping"
        or refinement.get("runtime_refinement_execution")
        != "bounded_recovery_trace_and_concrete_controller"
    ):
        raise RuntimeError(
            "refinement registry must bind both the bounded recovery trace and concrete controller"
        )
    obligations = refinement.get("obligations")
    expected_owner_ids = {
        "refinement.external-controller-protocol",
        "refinement.sole-ledger-authority",
    }
    actual_owner_ids = (
        {item.get("id") for item in obligations if isinstance(item, dict)}
        if isinstance(obligations, list)
        else set()
    )
    if actual_owner_ids != expected_owner_ids:
        raise RuntimeError(
            "refinement registry does not exactly cover the named owner categories: "
            f"missing={sorted(expected_owner_ids - actual_owner_ids)}, "
            f"extra={sorted(actual_owner_ids - expected_owner_ids)}"
        )
    _smoke_selection()
    return refinement


def _smoke_selection() -> tuple[
    tuple[Scenario, ...], tuple[Reachability, ...], tuple[Mutation, ...]
]:
    try:
        scenarios = tuple(SCENARIO_BY_NAME[name] for name in SMOKE_SCENARIO_NAMES)
        reachability = tuple(
            REACHABILITY_BY_NAME[name] for name in SMOKE_REACHABILITY_NAMES
        )
        mutations = tuple(MUTATION_BY_NAME[name] for name in SMOKE_MUTATION_NAMES)
    except KeyError as exc:
        raise RuntimeError(f"smoke profile names an absent registry row: {exc.args[0]}")
    for label, names in (
        ("scenario", SMOKE_SCENARIO_NAMES),
        ("reachability", SMOKE_REACHABILITY_NAMES),
        ("mutation", SMOKE_MUTATION_NAMES),
    ):
        if len(names) != len(set(names)):
            raise RuntimeError(f"smoke profile repeats a {label} row")
    return scenarios, reachability, mutations


def _run_safety(
    out: Path, timeout: int, scenarios: tuple[Scenario, ...] = SCENARIOS
) -> list[dict[str, object]]:
    results = []
    for scenario in scenarios:
        directory = out / "scenarios" / scenario.name
        directory.mkdir(parents=True, exist_ok=True)
        module = directory / MODULE.name
        shutil.copyfile(MODULE, module)
        config = directory / "DeploymentControlPlaneKernel.cfg"
        config.write_text(_scenario_config(scenario), encoding="utf-8")
        result = _run_tlc(module, config, out_dir=directory, timeout=timeout)
        _write_log(directory / "tlc.log", result)
        if result[
            "returncode"
        ] != 0 or "Model checking completed. No error has been found." not in str(
            result["output"]
        ):
            raise RuntimeError(
                f"safe scenario {scenario.name} failed; see {directory / 'tlc.log'}"
            )
        if (
            not isinstance(result["distinct_states"], int)
            or result["distinct_states"] < 10
        ):
            raise RuntimeError(
                f"safe scenario {scenario.name} has a vacuous state space"
            )
        results.append(
            {
                "name": scenario.name,
                "config_digest": _sha256(config),
                "distinct_states": result["distinct_states"],
                "status": "pass",
            }
        )
        print(
            f"kernel safety: {scenario.name} passed ({result['distinct_states']} distinct states)"
        )
    return results


def _run_reachability(
    out: Path, timeout: int, witnesses: tuple[Reachability, ...] = REACHABILITY
) -> list[dict[str, object]]:
    results = []
    for witness in witnesses:
        directory = out / "reachability" / witness.name
        directory.mkdir(parents=True, exist_ok=True)
        module = directory / MODULE.name
        shutil.copyfile(MODULE, module)
        config = directory / "DeploymentControlPlaneKernel.cfg"
        config.write_text(
            _scenario_config(
                SCENARIO_BY_NAME[witness.scenario], check=witness.invariant
            ),
            encoding="utf-8",
        )
        result = _run_tlc(module, config, out_dir=directory, timeout=timeout)
        _write_log(directory / "tlc.log", result)
        marker = f"Invariant {witness.invariant} is violated"
        if result["returncode"] != 12 or marker not in str(result["output"]):
            raise RuntimeError(
                f"reachability {witness.name} was not witnessed exactly; see {directory / 'tlc.log'}"
            )
        results.append(
            {
                "name": witness.name,
                "scenario": witness.scenario,
                "invariant": witness.invariant,
                "status": "witnessed",
            }
        )
        print(f"kernel reachability: {witness.name} witnessed")
    return results


def _run_mutations(
    out: Path, timeout: int, mutations: tuple[Mutation, ...] = MUTATIONS
) -> list[dict[str, object]]:
    results = []
    for mutation in mutations:
        directory = out / "mutations" / mutation.name
        directory.mkdir(parents=True, exist_ok=True)
        module = directory / MODULE.name
        module.write_text(_mutated_module(mutation), encoding="utf-8")
        config = directory / "DeploymentControlPlaneKernel.cfg"
        config.write_text(
            _scenario_config(
                SCENARIO_BY_NAME[mutation.scenario],
                check=mutation.invariant,
                kind=mutation.kind,
            ),
            encoding="utf-8",
        )
        result = _run_tlc(module, config, out_dir=directory, timeout=timeout)
        _write_log(directory / "tlc.log", result)
        markers = (
            (f"Invariant {mutation.invariant} is violated",)
            if mutation.kind == "INVARIANT"
            else ("Temporal properties were violated.",)
        )
        expected_exit = 12 if mutation.kind == "INVARIANT" else 13
        if result["returncode"] != expected_exit or not any(
            marker in str(result["output"]) for marker in markers
        ):
            raise RuntimeError(
                f"semantic mutation {mutation.name} was not witnessed exactly; see {directory / 'tlc.log'}"
            )
        results.append(
            {
                "name": mutation.name,
                "scenario": mutation.scenario,
                "invariant": mutation.invariant,
                "module_digest": _sha256(module),
                "status": "witnessed",
            }
        )
        print(f"kernel mutation: {mutation.name} witnessed {mutation.invariant}")
    return results


def _write_smoke_report(
    out: Path,
    safety: list[dict[str, object]],
    reachability: list[dict[str, object]],
    mutations: list[dict[str, object]],
) -> Path:
    """Emit bounded smoke evidence without impersonating the full PACT proof."""
    document = {
        "property_id": f"{PROPERTY_ID}Smoke",
        "status": "pass",
        "profile": "smoke",
        "proof_kind": "bounded_tlc_smoke",
        "full_pact_evidence": False,
        "checker_command": SMOKE_CHECKER_COMMAND,
        "model_digest": _sha256(MODULE),
        "config_digest": _sha256(CONFIG),
        "checker_digest": _sha256(Path(__file__)),
        "coverage": {
            "safety": safety,
            "reachability": reachability,
            "mutations": mutations,
        },
        "selected_registry_rows": {
            "safety": list(SMOKE_SCENARIO_NAMES),
            "reachability": list(SMOKE_REACHABILITY_NAMES),
            "mutations": list(SMOKE_MUTATION_NAMES),
        },
        "full_profile_command": CHECKER_COMMAND,
    }
    path = out / "kernel-smoke-aggregate.json"
    path.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return path


def _atomic_yaml(path: Path, data: object) -> None:
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        yaml.safe_dump(data, handle, sort_keys=False)
        temp = Path(handle.name)
    temp.replace(path)


def _aggregate(
    out: Path,
    refinement: dict[str, object],
    safety: list[dict[str, object]],
    reachability: list[dict[str, object]],
    mutations: list[dict[str, object]],
    controller_refinement: Path,
) -> Path:
    child_digests = {}
    for change in CHILD_CHANGES:
        manifest_path = ROOT / "pact" / "changes" / change / "pact.yaml"
        if not manifest_path.is_file():
            raise RuntimeError(
                f"refinement owner manifest is unavailable: {change}; a checkout "
                "without this repository's PACT change records, such as the "
                "public copy, can run the model checks without the aggregate "
                "report with --without-pact-records "
                "(scripts/run_formal_suite.py --without-pact-records forwards it)"
            )
        manifest = load_manifest_file(manifest_path)
        child_digests[change] = intent_digest(manifest)
    scenario_registry = [scenario.__dict__ for scenario in SCENARIOS]
    reachability_registry = [witness.__dict__ for witness in REACHABILITY]
    mutation_registry = [
        {
            "name": mutation.name,
            "scenario": mutation.scenario,
            "invariant": mutation.invariant,
            "kind": mutation.kind,
            "replacements": [
                replacement.__dict__ for replacement in mutation.replacements
            ],
        }
        for mutation in MUTATIONS
    ]
    aggregate = {
        "property_id": PROPERTY_ID,
        "status": "pass",
        "proof_kind": "tlc_fixpoint",
        "model_digest": _sha256(MODULE),
        "config_digest": _sha256(CONFIG),
        "checker_digest": _sha256(Path(__file__)),
        "design_intent_digest": intent_digest(load_manifest_file(DESIGN)),
        "readme_digest": _sha256(README),
        "refinement_registry_digest": _sha256(REFINEMENTS),
        "scenario_registry_digest": _registry_digest(scenario_registry),
        "reachability_registry_digest": _registry_digest(reachability_registry),
        "mutation_registry_digest": _registry_digest(mutation_registry),
        "child_intent_digests": child_digests,
        "runtime_refinement_execution": refinement["runtime_refinement_execution"],
        "refinement_claim": refinement["claim"],
        "safety": safety,
        "reachability": reachability,
        "mutations": mutations,
        "controller_refinement_artifact": str(controller_refinement.resolve()),
        "controller_refinement_digest": _sha256(controller_refinement),
        "tla2tools_digest": _sha256(JAR),
    }
    path = out / "kernel-proof-aggregate.json"
    path.write_text(
        json.dumps(aggregate, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return path


def _checker_invocation(argv: list[str]) -> str:
    return shlex.join(
        (
            "uv",
            "run",
            "python",
            "scripts/check_formal_deployment_control_plane_kernel.py",
            *argv,
        )
    )


def _write_report(
    out: Path,
    aggregate: Path,
    *,
    minimum_states: int,
    checker_command: str,
) -> None:
    row = {
        "property_id": PROPERTY_ID,
        "kind": "formal_check",
        "status": "pass",
        "fresh": True,
        "proof_status": "freshly_proven",
        "proof_kind": "tlc_fixpoint",
        "checker_command": checker_command,
        "command": checker_command,
        "artifact": str(aggregate.resolve()),
        "module_digest": _sha256(MODULE),
        "config_digest": _sha256(CONFIG),
        "refinement_tests": list(REFINEMENT_TESTS),
        "controller_refinement_tests": list(CONTROLLER_REFINEMENT_TESTS),
        "summary": "TLC proved K0-K13 and the strengthened rollback protocol across every scenario; exact reachability, semantic guard-removal witnesses, concrete controller source predicates, and executable rollback refinements passed.",
        "distinct_states": minimum_states,
    }
    _atomic_yaml(out / "formal-report.yaml", {"results": [row]})


def _run_recovery_refinement(out: Path, timeout: int) -> None:
    runtime_files = (
        ROOT / "src/okg/substrate/deployment_control_plane/workflow.py",
        ROOT
        / "src/okg/substrate/deployment_control_plane/migrations/0002_workflow_ownership.sql",
        ROOT / "tests/substrate/deployment_control_plane/test_workflow_recovery.py",
    )
    missing = [
        str(path.relative_to(ROOT)) for path in runtime_files if not path.is_file()
    ]
    if missing:
        raise RuntimeError(
            f"recovery refinement runtime artifacts are missing: {missing}"
        )
    workflow_text = runtime_files[0].read_text(encoding="utf-8")
    required_boundaries = (
        "before_admission",
        "after_admission",
        "before_forward_apply",
        "after_admission_before_dbos_checkpoint",
        "after_forward_apply",
        "before_forward_observe",
        "after_forward_observe",
        "before_forward_commit",
        "after_forward_commit",
        "before_compensation_intent",
        "after_compensation_intent",
        "before_compensation_apply",
        "after_compensation_apply",
        "before_compensation_observe",
        "after_compensation_observe",
        "before_compensation_commit",
        "after_compensation_commit",
        "before_terminal_rolled_back",
        "after_terminal_rolled_back",
        "before_terminal_blocked",
        "after_terminal_blocked",
        "before_terminal_ready",
        "after_terminal_ready",
        "before_terminal_promoted",
        "after_terminal_promoted",
        "after_undeliverable_recovering",
    )
    omitted = [
        boundary for boundary in required_boundaries if boundary not in workflow_text
    ]
    if omitted:
        raise RuntimeError(f"recovery refinement omits fault boundaries: {omitted}")
    history_path = out / "runtime-history.json"
    if not history_path.is_file():
        raise RuntimeError(
            "persisted runtime history is required; run the workflow recovery pytest "
            "selectors before the formal refinement"
        )
    history = json.loads(history_path.read_text(encoding="utf-8"))
    event_actions = {
        ("begun", None, "planned"): "PlanAttempt",
        ("lifecycle", "planned", "admitted"): "DeliverWorkflowStart",
        ("lifecycle", "admitted", "applying"): "BeginApply",
        ("lifecycle", "verifying", "ready"): "PublishFreshGeneration",
        ("lifecycle", "ready", "promoted"): "Promote",
        ("lifecycle", "admitted", "recovering"): "RequestRecovery",
        ("lifecycle", "applying", "recovering"): "RequestRecovery",
        ("lifecycle", "verifying", "recovering"): "RequestRecovery",
        ("lifecycle", "ready", "recovering"): "RequestRecovery",
        ("lifecycle", "recovering", "rolled_back"): "SettleRollback",
        ("lifecycle", "recovering", "blocked"): "SettleBlocked",
    }
    effect_actions = {
        "intent": "IntentEffect",
        "applied": "ApplyEffect",
        "observed": "ObserveEffect",
        "committed": "CommitEffect",
    }
    compensation_actions = {
        "intent": "CompensationIntent",
        "applied": "CompensationApply",
        "observed": "CompensationObserve",
        "committed": "CompensationCommit",
    }
    collections = {
        "events": history.get("events", []),
        "effect_phases": history.get("effect_phases", []),
        "compensation_phases": history.get("compensation_phases", []),
    }
    for collection, rows in collections.items():
        positions: dict[int, list[int]] = {}
        for row in rows:
            positions.setdefault(int(row["attempt_id"]), []).append(
                int(row["sequence"])
            )
        if any(
            values != sorted(values) or len(values) != len(set(values))
            for values in positions.values()
        ):
            raise RuntimeError(
                f"persisted {collection} is not strictly append-only by attempt/sequence"
            )

    def phase_groups(rows, key_name: str, phases: tuple[str, ...]):
        groups: dict[tuple[int, str], list[dict[str, object]]] = {}
        for row in rows:
            key = (int(row["attempt_id"]), str(row[key_name]))
            groups.setdefault(key, []).append(row)
        for key, group in groups.items():
            actual = tuple(str(row["phase"]) for row in group)
            if actual != phases[: len(actual)]:
                raise RuntimeError(
                    f"persisted {key_name} {key} has illegal phase trace {actual}"
                )
            if (
                key_name == "effect_key"
                and len({str(row["compensation_class"]) for row in group}) != 1
            ):
                raise RuntimeError(
                    f"persisted {key_name} {key} changes compensation class"
                )
        return groups

    effect_groups = phase_groups(
        collections["effect_phases"],
        "effect_key",
        ("intent", "applied", "observed", "committed"),
    )
    compensation_groups = phase_groups(
        collections["compensation_phases"],
        "compensation_key",
        ("intent", "applied", "observed", "committed"),
    )
    events_by_attempt: dict[int, list[dict[str, object]]] = {}
    for event in collections["events"]:
        events_by_attempt.setdefault(int(event["attempt_id"]), []).append(event)
    runtime_trace: list[dict[str, object]] = []
    legal_edges = {
        ("planned", "admitted"),
        ("planned", "recovering"),
        ("admitted", "applying"),
        ("admitted", "recovering"),
        ("applying", "verifying"),
        ("applying", "recovering"),
        ("verifying", "ready"),
        ("verifying", "recovering"),
        ("ready", "promoted"),
        ("ready", "recovering"),
        ("recovering", "rolled_back"),
        ("recovering", "blocked"),
    }
    for attempt_id, events in sorted(events_by_attempt.items()):
        if not events or (
            events[0]["event_type"],
            events[0]["from_state"],
            events[0]["to_state"],
        ) != ("begun", None, "planned"):
            raise RuntimeError(f"attempt {attempt_id} does not begin at planned")
        state = "planned"
        committed_effects = 0
        effects_emitted = False
        compensations_emitted = False

        def emit_effects() -> None:
            nonlocal effects_emitted
            if effects_emitted:
                return
            for (owner, _), group in sorted(effect_groups.items()):
                if owner == attempt_id:
                    runtime_trace.extend(
                        {
                            "model": effect_actions[str(row["phase"])],
                            "source": "effect_phase",
                            **row,
                        }
                        for row in group
                    )
            effects_emitted = True

        def emit_compensations() -> None:
            nonlocal compensations_emitted
            if compensations_emitted:
                return
            for (owner, _), group in sorted(compensation_groups.items()):
                if owner == attempt_id:
                    runtime_trace.extend(
                        {
                            "model": compensation_actions[str(row["phase"])],
                            "source": "compensation_phase",
                            **row,
                        }
                        for row in group
                    )
            compensations_emitted = True

        for event in events:
            event_type = str(event["event_type"])
            if event_type == "effect_committed":
                if state not in {"applying", "recovering"}:
                    raise RuntimeError(
                        f"attempt {attempt_id} committed an effect from {state}"
                    )
                committed_effects += 1
                continue
            if event_type == "compensation_committed":
                if state != "recovering":
                    raise RuntimeError(f"attempt {attempt_id} compensated from {state}")
                continue
            action = event_actions.get(
                (event["event_type"], event["from_state"], event["to_state"])
            )
            if event_type != "lifecycle" and action is None:
                continue
            if action == "PlanAttempt":
                runtime_trace.append(
                    {"model": action, "source": "attempt_event", **event}
                )
                continue
            edge = (str(event["from_state"]), str(event["to_state"]))
            if edge not in legal_edges or edge[0] != state:
                raise RuntimeError(
                    f"attempt {attempt_id} has illegal lifecycle edge {edge} from {state}"
                )
            if edge == ("applying", "verifying") and committed_effects == 0:
                raise RuntimeError(
                    f"attempt {attempt_id} verifies before a committed effect"
                )
            if edge == ("recovering", "rolled_back"):
                committed_effect_groups = {
                    effect_key: group
                    for (owner, effect_key), group in effect_groups.items()
                    if owner == attempt_id and group[-1]["phase"] == "committed"
                }
                if any(
                    group[-1]["compensation_class"] == "irreversible"
                    for group in committed_effect_groups.values()
                ):
                    raise RuntimeError(
                        f"attempt {attempt_id} rolled back an irreversible effect"
                    )
                expected_compensations = {
                    f"compensate:{effect_key}"
                    for effect_key, group in committed_effect_groups.items()
                    if group[-1]["compensation_class"] == "reversible"
                }
                actual_compensations = {
                    compensation_key
                    for (owner, compensation_key) in compensation_groups
                    if owner == attempt_id
                }
                if actual_compensations != expected_compensations or any(
                    compensation_groups[(attempt_id, key)][-1]["phase"] != "committed"
                    for key in actual_compensations
                ):
                    raise RuntimeError(
                        f"attempt {attempt_id} rolled back before exact compensation"
                    )
            if edge == ("applying", "verifying"):
                emit_effects()
            elif edge[1] == "recovering":
                emit_effects()
            elif edge[0] == "recovering":
                emit_compensations()
            state = edge[1]
            if action is not None:
                runtime_trace.append(
                    {"model": action, "source": "attempt_event", **event}
                )
        if state not in {"ready", "promoted", "rolled_back", "blocked"}:
            raise RuntimeError(
                f"attempt {attempt_id} has unsettled trace state {state}"
            )
        emit_effects()
        emit_compensations()
    model_text = MODULE.read_text(encoding="utf-8")
    required_actions = (
        set(event_actions.values())
        | set(effect_actions.values())
        | set(compensation_actions.values())
    )
    actual_actions = {str(row["model"]) for row in runtime_trace}
    if missing_actions := sorted(required_actions - actual_actions):
        raise RuntimeError(
            f"persisted runtime histories omit model actions: {missing_actions}"
        )
    for row in runtime_trace:
        if f"{row['model']} ==" not in model_text:
            raise RuntimeError(
                f"persisted runtime row maps to absent model action {row['model']}"
            )
    scenario_names = {
        "reversible_failure",
        "irreversible_ambiguity",
        "irreversible_postcommit_failure",
        "compensation_failure",
        "recover_admitted",
        "recover_applying",
        "recover_verifying",
        "recover_ready",
    }
    witness_names = {
        "reversible_rollback",
        "irreversible_ambiguity_rollback",
        "irreversible_postcommit_rollback",
        "compensation_failure_block",
        "recovery_from_admitted",
        "recovery_from_applying",
        "recovery_from_verifying",
        "recovery_from_ready",
        "crash_at_planned",
        "crash_at_admitted",
        "crash_at_effect_intended",
        "crash_at_effect_applied",
        "crash_at_effect_observed",
        "crash_at_effect_committed",
        "crash_at_compensation_intended",
        "crash_at_compensation_applied",
        "crash_at_compensation_observed",
        "crash_at_compensated",
    }
    safety = _run_safety(
        out, timeout, tuple(s for s in SCENARIOS if s.name in scenario_names)
    )
    reachability = _run_reachability(
        out, timeout, tuple(w for w in REACHABILITY if w.name in witness_names)
    )
    evidence = {
        "property_id": "DeploymentControlPlaneRecoveryRefinement",
        "status": "pass",
        "proof_kind": "bounded_model_plus_runtime_boundary_binding",
        "model_digest": _sha256(MODULE),
        "checker_digest": _sha256(Path(__file__)),
        "runtime_artifacts": {
            str(path.relative_to(ROOT)): _sha256(path) for path in runtime_files
        },
        "fault_boundaries": list(required_boundaries),
        "runtime_transition_trace": list(runtime_trace),
        "runtime_transition_trace_digest": _registry_digest(runtime_trace),
        "persisted_runtime_history_digest": _sha256(history_path),
        "safety": safety,
        "reachability": reachability,
        "runtime_refinement_execution": "bounded_recovery_trace",
    }
    artifact = out / "recovery-refinement.json"
    artifact.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    _atomic_yaml(
        out / "formal-report.yaml",
        {
            "results": [
                {
                    "property_id": evidence["property_id"],
                    "kind": "formal_check",
                    "status": "pass",
                    "fresh": True,
                    "proof_kind": evidence["proof_kind"],
                    "artifact": str(artifact.resolve()),
                    "command": (
                        "uv run python scripts/check_formal_deployment_control_plane_kernel.py "
                        "--refinement recovery --out " + str(out)
                    ),
                    "summary": (
                        "Recovery safety and reachability passed for every modeled crash, "
                        "forward, compensation, and irreversible settlement boundary."
                    ),
                }
            ]
        },
    )
    print(f"recovery refinement: proof report at {out / 'formal-report.yaml'}")


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser()
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--all", action="store_true")
    modes.add_argument("--safety-only", action="store_true")
    modes.add_argument("--reachability-only", action="store_true")
    modes.add_argument("--mutations-only", action="store_true")
    modes.add_argument("--contract-only", action="store_true")
    modes.add_argument("--refinement", choices=("recovery", "controller"))
    modes.add_argument("--profile", choices=("smoke", "full"))
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument(
        "--without-pact-records",
        action="store_true",
        help=(
            "run every model check but do not write the aggregate proof report, "
            "which binds this repository's PACT change records, for a checkout "
            "that does not carry them"
        ),
    )
    args = parser.parse_args(raw_argv)
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    (out / "formal-report.yaml").unlink(missing_ok=True)
    (out / "kernel-proof-aggregate.json").unlink(missing_ok=True)
    (out / "kernel-smoke-aggregate.json").unlink(missing_ok=True)
    try:
        refinement = _validate_contract()
        if args.contract_only:
            print("kernel contract: registries and refinement boundary are valid")
            return 0
        if args.refinement == "recovery":
            _run_recovery_refinement(out, args.timeout_seconds)
            return 0
        if args.refinement == "controller":
            artifact = _run_controller_refinement(out, args.timeout_seconds)
            _atomic_yaml(
                out / "formal-report.yaml",
                {
                    "results": [
                        {
                            "property_id": "DeploymentControlPlaneControllerRefinement",
                            "kind": "formal_check",
                            "status": "pass",
                            "fresh": True,
                            "proof_status": "freshly_proven",
                            "proof_kind": "concrete_ast_predicates_plus_executable_tests",
                            "artifact": str(artifact.resolve()),
                            "command": CONTROLLER_REFINEMENT_COMMAND,
                            "summary": (
                                "Concrete controller guards and executable rollback tests "
                                "refine the strengthened compensation protocol."
                            ),
                        }
                    ]
                },
            )
            print(
                f"controller refinement: proof report at {out / 'formal-report.yaml'}"
            )
            return 0
        if args.profile == "smoke":
            scenarios, witnesses, mutations = _smoke_selection()
            safety = _run_safety(out, args.timeout_seconds, scenarios)
            reachability = _run_reachability(out, args.timeout_seconds, witnesses)
            mutation_results = _run_mutations(out, args.timeout_seconds, mutations)
            artifact = _write_smoke_report(out, safety, reachability, mutation_results)
            print(f"kernel smoke: bounded profile at {artifact}")
            return 0
        run_all = (
            args.profile == "full"
            or args.all
            or not any((args.safety_only, args.reachability_only, args.mutations_only))
        )
        safety = (
            _run_safety(out, args.timeout_seconds)
            if run_all or args.safety_only
            else []
        )
        reachability = (
            _run_reachability(out, args.timeout_seconds)
            if run_all or args.reachability_only
            else []
        )
        mutations = (
            _run_mutations(out, args.timeout_seconds)
            if run_all or args.mutations_only
            else []
        )
        if run_all:
            controller_refinement = _run_controller_refinement(
                out, args.timeout_seconds
            )
            if args.without_pact_records:
                # The aggregate binds the design and child PACT records by
                # intent digest; without them there is no honest report to
                # write, so none is written and the run says so.
                print(
                    "kernel aggregate: every model check ran; the aggregate "
                    "proof report was NOT written (--without-pact-records)"
                )
                return 0
            aggregate = _aggregate(
                out,
                refinement,
                safety,
                reachability,
                mutations,
                controller_refinement,
            )
            _write_report(
                out,
                aggregate,
                minimum_states=min(int(row["distinct_states"]) for row in safety),
                checker_command=_checker_invocation(raw_argv),
            )
            print(f"kernel aggregate: proof report at {out / 'formal-report.yaml'}")
    except (RuntimeError, OSError, ValueError, yaml.YAMLError) as exc:
        print(f"deployment-control-plane-kernel: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
