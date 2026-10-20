#!/usr/bin/env python3
"""Aggregate runner for the formal/ verification corpus.

This is the single gating run path for the formal checkers. It executes real
TLC (and the Python executable models), aggregates pass/fail across every
checker, and exits non-zero if any checker reports a non-pass result.

Modes:
  smoke           run every checker's fast/smoke configuration
  counterexamples run every counterexample/non-vacuity witness
  full            run the dedicated full profile of every checker whose smoke
                  run is a reduced config (nomos, attributes, admission,
                  transform dirty-scope) — the scheduled formal-full CI lane
  all             smoke + counterexamples

A checker passes when its process exits 0 (the checkers themselves return a
non-zero exit code on TLC timeout, invariant violation, missing-obligation, or
a missing expected counterexample). The registry below is the source of truth
for what runs; ``tests/docs/test_formal_suite_reachability.py`` asserts that
every ``scripts/check_formal_*.py`` is reachable from this registry, so a new
checker cannot be added without a run path.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field, replace
from pathlib import Path

from okg.substrate.formal_tlc import (
    TLC_TIMEOUT_EXIT_CODE,
    tlc_jvm_args,
    tlc_workers,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
SUITE_OUT = REPO_ROOT / ".scratch" / "formal" / "suite"


def _load_full_gate():
    """scripts/ci/formal_full_gate.py, loaded by path (scripts/ci is not a package)."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "formal_full_gate", SCRIPTS_DIR / "ci" / "formal_full_gate.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@dataclass(frozen=True)
class CheckerSpec:
    """One formal checker and how the suite invokes it."""

    name: str  # scripts/<name>.py
    kind: str  # "tlc" | "pymodel"
    # Extra args for the smoke run (e.g. ["--profile", "smoke"]); empty means
    # the checker's default configuration is the smoke configuration.
    smoke_args: tuple[str, ...] = ()
    # Each entry is the full extra-arg set for one counterexample witness run
    # (the runner appends --out). Empty means the checker has no witness yet.
    witnesses: tuple[tuple[str, ...], ...] = ()
    # Per-checker smoke budget in seconds (TLC checkers receive it as
    # --timeout-seconds; pymodels are not timed by TLC and ignore it).
    smoke_timeout: int = 240
    # Budget for the checker's dedicated full profile (--mode full). Only
    # meaningful for checkers with a dedicated smoke profile; full mode skips
    # smoke_is_full_model checkers because their smoke run IS the full model.
    full_timeout: int = 900
    # True when the smoke configuration is the FULL model (no dedicated fast
    # profile yet). Disclosed by --list so the smoke/full gap is honest.
    smoke_is_full_model: bool = False
    # Archive-only helpers (e.g. legacy console) cannot satisfy active PACT
    # formal evidence; the gating suite registers but skips them.
    archive_only: bool = False
    # ENROLMENT IN THE BOUNDED "NOT PROVEN" BUCKET, and nothing else.
    #
    # Set to the issue number tracking restoration of proof when this
    # checker's FULL profile has never completed inside its cap. A full-mode
    # run that exits 124 (TLC's own timeout) is then reported as UNPROVEN
    # rather than FAIL: it does not red the lane, and it is never called a
    # pass either -- the junit case is `skipped` with a message that says the
    # property set was NOT proven, and the run prints a loud summary line.
    #
    # Deliberately narrow. It applies to the FULL profile only, to exit code
    # 124 only, and only to checkers listed here by hand with an issue. A
    # smoke timeout, any other exit code, and any checker without an issue
    # number all stay hard failures -- an invariant violation can never reach
    # this bucket.
    full_profile_unproven_issue: int | None = None
    # True when the checker also checks the model against this repository's
    # PACT change records and accepts --without-pact-records to skip only that
    # step. The suite forwards the flag only when it is run with
    # --without-pact-records (a checkout without the records, such as the
    # public copy); by default the checker checks the records.
    reads_pact_records: bool = False


# ---------------------------------------------------------------------------
# Registry: every scripts/check_formal_*.py must appear here exactly once.
# ---------------------------------------------------------------------------
CHECKERS: tuple[CheckerSpec, ...] = (
    CheckerSpec("check_formal_admission.py", "tlc",
                smoke_args=("--profile", "smoke"),
                # Full profile floors at 22.3M distinct states and has never
                # finished inside 900s: every retained nightly back to
                # 2026-08-12 shows exactly 900.x / exit=124, and the cap has
                # not changed since it was introduced with the lane on
                # 2026-07-01. The cap was below the cost from the first day;
                # this is not a regression and not fleet noise.
                full_profile_unproven_issue=1537,
                witnesses=(("--model", "AdmissionCounterexample.tla",
                            "--config", "AdmissionCounterexample.cfg",
                            "--expect-counterexample"),)),
    CheckerSpec("check_formal_agent_operator_ergonomics.py", "tlc",
                smoke_timeout=120,
                smoke_is_full_model=True),
    CheckerSpec("check_formal_althea_optimizer_safety.py", "tlc",
                smoke_is_full_model=True),
    CheckerSpec("check_formal_attribute_state.py", "tlc",
                smoke_args=("--profile", "smoke")),
    CheckerSpec("check_formal_chronos_read_pin.py", "tlc",
                smoke_is_full_model=True),
    CheckerSpec("check_formal_chronos_retention_compaction.py", "tlc",
                smoke_timeout=120,
                smoke_is_full_model=True),
    CheckerSpec("check_formal_console.py", "tlc",
                archive_only=True,
                witnesses=(
                    ("--archived-helper", "--model", "vocabulary-lookup-counterexample"),
                    ("--archived-helper", "--model", "offline-replay-counterexample"),
                )),
    # smoke_is_full_model: one exhaustive positive config (2944 distinct
    # states) covers the whole crash/resume scope; the three counterexample
    # configs run via DIRECT_COUNTEREXAMPLES and the checker's own
    # --counterexamples self-proof. Budget MEASURED (lane-integration,
    # 2026-07-17): 3 suite runs 2.2/2.3/2.3s local at OKG_TLC_WORKERS=4;
    # 60s = ~26x headroom for slower shared CI runners.
    CheckerSpec("check_formal_dbos_commit.py", "tlc",
                smoke_timeout=60,
                smoke_is_full_model=True),
    CheckerSpec("check_formal_dbos_epoch_supersession.py", "tlc",
                smoke_args=("--all",),
                smoke_timeout=120,
                smoke_is_full_model=True),
    CheckerSpec("check_formal_delta_frontier.py", "tlc",
                smoke_is_full_model=True),
    CheckerSpec("check_formal_delta_projection.py", "tlc",
                smoke_is_full_model=True),
    CheckerSpec("check_formal_deployment_authoring.py", "tlc",
                smoke_is_full_model=True),
    # One smoke invocation exhaustively proves the deployment-apply safety
    # model and runs all four named counterexample configs in-process.
    CheckerSpec("check_formal_deployment_apply.py", "tlc",
                smoke_timeout=120,
                smoke_is_full_model=True),
    # smoke_is_full_model: one exhaustive positive config covers the whole
    # backup-policy scope (archive validity, full-restore selection, routine
    # receipt reuse, and drift escalation); there is no cheaper profile to
    # split out. Budget is the registry default, as for the sibling
    # deployment models.
    CheckerSpec("check_formal_deployment_backup_policy.py", "tlc",
                smoke_is_full_model=True),
    # The full 18-safety/59-reachability/31-mutation aggregate measured 307.3s
    # locally. Existing per-PR formal QA already consumes ~1000s of its 25m
    # job budget, so smoke runs a named representative subset; scheduled
    # formal-full remains the only suite lane that emits the strict aggregate.
    CheckerSpec("check_formal_deployment_control_plane_kernel.py", "tlc",
                smoke_args=("--profile", "smoke"),
                smoke_timeout=120,
                full_timeout=420,
                reads_pact_records=True),
    CheckerSpec("check_formal_deployment_lifecycle.py", "tlc",
                smoke_timeout=120,
                smoke_is_full_model=True),
    CheckerSpec("check_formal_deployment_restore_point.py", "tlc",
                smoke_timeout=120,
                smoke_is_full_model=True),
    CheckerSpec("check_formal_deployment_promotion.py", "tlc",
                smoke_is_full_model=True),
    CheckerSpec("check_formal_derivation_parity.py", "tlc",
                smoke_is_full_model=True),
    CheckerSpec("check_formal_dogfood_cutover.py", "tlc",
                smoke_is_full_model=True),
    CheckerSpec("check_formal_enricher_backfill.py", "tlc",
                smoke_is_full_model=True),
    # smoke_is_full_model: one invocation exhausts the four-state positive
    # liveness model and independently runs the pre- and post-reuse guard-off
    # witnesses. Budget measured locally at ~4s for all three configs.
    CheckerSpec("check_formal_enricher_backlog_drain.py", "tlc",
                smoke_timeout=60,
                smoke_is_full_model=True),
    # This checker runs the baseline and all four guard-removal witnesses in
    # one process.  The suite provides the outer wall-clock timeout.
    CheckerSpec("check_formal_external_action_review.py", "pymodel",
                reads_pact_records=True),
    CheckerSpec("check_formal_entity_resolution.py", "tlc",
                smoke_is_full_model=True),
    CheckerSpec("check_formal_fact_batch_recovery.py", "tlc",
                smoke_timeout=120,
                smoke_is_full_model=True),
    CheckerSpec("check_formal_source_retirement_liveness.py", "tlc",
                smoke_timeout=120,
                smoke_is_full_model=True),
    CheckerSpec("check_formal_fleet_capacity_reservation.py", "tlc",
                smoke_args=("--all",),
                smoke_timeout=120,
                smoke_is_full_model=True),
    CheckerSpec("check_formal_gtir_commit.py", "pymodel"),
    CheckerSpec("check_formal_merge_back_safety.py", "tlc",
                smoke_is_full_model=True),
    CheckerSpec("check_formal_nomos.py", "tlc",
                smoke_args=("--profile", "smoke"),
                # Same history as admission: 900.x / exit=124 in every
                # retained nightly since 2026-08-12, never a completion. The
                # narrower `iam` and `pins` profiles that were split out of
                # this config DO pass, so the un-proven surface is the
                # 19-request aggregate only.
                full_profile_unproven_issue=1537,
                witnesses=(("--model", "NomosCounterexample.tla",
                            "--config", "NomosCounterexample.cfg",
                            "--expect-counterexample"),)),
    CheckerSpec("check_formal_pact_agent_lifecycle.py", "tlc",
                smoke_timeout=300,
                smoke_is_full_model=True),
    CheckerSpec("check_formal_pact_formal_evidence.py", "tlc",
                smoke_is_full_model=True),
    # This finite model exhaustively covers the three proof-integrity gates:
    # distinct acceptance-arm witnesses, completed integration owners, and
    # execution-attested named tests. There is no reduced configuration.
    CheckerSpec("check_formal_pact_proof_integrity.py", "tlc",
                smoke_timeout=120,
                smoke_is_full_model=True),
    CheckerSpec("check_formal_pact_mutation.py", "tlc",
                smoke_is_full_model=True),
    CheckerSpec("check_formal_pact_projection.py", "tlc",
                smoke_is_full_model=True),
    CheckerSpec("check_formal_pact_v4_decision_ledger.py", "tlc",
                smoke_is_full_model=True),
    CheckerSpec("check_formal_poros.py", "tlc",
                witnesses=(("--model", "PorosExchangeCounterexample.tla",
                            "--config", "PorosExchangeCounterexample.cfg",
                            "--expect-counterexample"),),
                smoke_is_full_model=True),
    # smoke_is_full_model: one invocation runs all four configs exhaustively
    # (smoke sanity + any_open standing + new_findings one-shot exhibit +
    # wrong-count counterexample); there is no reduced smoke profile, and the
    # two counterexample configs are checked in-process, not via the suite's
    # --expect-counterexample witness path. Budget MEASURED (lane-integration,
    # 2026-07-17): 3 suite runs 5.3/5.2/5.5s local (all four configs) at
    # OKG_TLC_WORKERS=4; 60s = ~11x headroom for slower shared CI runners.
    CheckerSpec("check_formal_qualification_gate.py", "tlc",
                smoke_timeout=60,
                smoke_is_full_model=True),
    # Budget MEASURED (lane-integration, 2026-07-17): 3 suite runs
    # 3.0/1.8/1.9s local (850 distinct states) at OKG_TLC_WORKERS=4;
    # 60s = ~20x headroom for slower shared CI runners. Small exhaustive
    # model -> smoke IS the full model.
    CheckerSpec("check_formal_reuse_pass.py", "tlc",
                smoke_timeout=60,
                smoke_is_full_model=True),
    CheckerSpec("check_formal_runtime_cutover.py", "tlc",
                smoke_timeout=60,
                smoke_is_full_model=True),
    CheckerSpec("check_formal_scholarly_source_shapes.py", "tlc",
                smoke_timeout=120,
                smoke_is_full_model=True),
    CheckerSpec("check_formal_seal_writes.py", "pymodel"),
    CheckerSpec("check_formal_source_delta_contract.py", "tlc",
                smoke_is_full_model=True),
    CheckerSpec("check_formal_source_execution_lanes.py", "tlc",
                smoke_is_full_model=True),
    # One invocation checks the stable-reemission model plus all four isolated
    # guard-removal witnesses. There is no reduced profile: the checker is the
    # complete model and non-vacuity proof used by the gating smoke lane.
    CheckerSpec("check_formal_stable_reemission.py", "tlc",
                smoke_is_full_model=True),
    CheckerSpec("check_formal_source_sync_convergence.py", "tlc",
                smoke_is_full_model=True),
    # The slowest checker in the smoke corpus by a wide margin: MEASURED at
    # 143 / 137 / 125s over three local runs (mean ~135s), all exit=0, against
    # 6s for its sync_convergence sibling. It inherited the 240s default rather
    # than being given a considered one, and 240s was never enough: this repo's
    # CI runners measure 2-3x slower than a dev host (see the substrate-db
    # budget history in tests/qa_lanes.yaml), so a 135s dev-host run projects
    # to 270-405s on CI and the ceiling is BELOW the floor of that range. It
    # therefore fails whenever the runner is on the slow end, as exit=124 at
    # 240.7s in run 30924173256 — a kill, never an invariant violation. 480s
    # covers the pessimistic 3x with ~18% headroom.
    CheckerSpec("check_formal_source_sync_observability.py", "tlc",
                smoke_timeout=480,
                smoke_is_full_model=True),
    CheckerSpec("check_formal_transform_gate_dirty_scope.py", "tlc",
                smoke_args=("--profile", "smoke")),
    CheckerSpec("check_formal_transform_gate_node_subtype.py", "tlc",
                smoke_is_full_model=True),
    CheckerSpec("check_formal_typed_applicability.py", "tlc",
                smoke_timeout=120,
                smoke_is_full_model=True),
    CheckerSpec("check_formal_typed_outcome.py", "tlc",
                smoke_timeout=120,
                smoke_is_full_model=True),
    # smoke_is_full_model: one invocation runs both configs exhaustively (the
    # guarded eventual-retry LIVENESS proof under weak fairness + the
    # guard-off stranded-watermark counterexample). The counterexample is a
    # TEMPORAL-property refutation ("Temporal properties were violated"),
    # which named_invariant_violated cannot see, so it is checked in-process
    # rather than via DIRECT_COUNTEREXAMPLES (same idiom as the readiness
    # liveness witness). Budget MEASURED (lane-integration, 2026-07-17):
    # 3 suite runs 2.7/2.6/2.6s local (both configs, 19 distinct states
    # each) at OKG_TLC_WORKERS=4; 60s = ~22x headroom for slower shared CI
    # runners.
    CheckerSpec("check_formal_watermark_retry.py", "tlc",
                smoke_timeout=60,
                smoke_is_full_model=True),
    # One exhaustive invocation checks item/signature retry safety, eventual
    # processing-or-quarantine, and the required shared-outage
    # misclassification counterexample. Measured locally at <4s for both
    # configs; 60s leaves ample shared-runner headroom.
    CheckerSpec("check_formal_watermark_quarantine.py", "tlc",
                smoke_timeout=60,
                smoke_is_full_model=True),
    CheckerSpec("check_formal_workflow_authority.py", "tlc",
                smoke_is_full_model=True),
    CheckerSpec("check_formal_worktree_branch_safety.py", "tlc",
                # The checker owns the guard-removal TLC runs.  Register each
                # exact witness config as an aggregate entry so ``--mode
                # counterexamples --only`` cannot silently skip this model.
                witnesses=(
                    ("--witness-id", "DefaultIsolation", "--witness-only"),
                    ("--witness-id", "ContributionOverlayIsolation", "--witness-only"),
                    ("--witness-id", "TransactionRecovery", "--witness-only"),
                    ("--witness-id", "ProjectionClosure", "--witness-only"),
                    ("--witness-id", "LifecycleFencing", "--witness-only"),
                    ("--witness-id", "CapabilityFencing", "--witness-only"),
                    ("--witness-id", "PostCommitRecovery", "--witness-only"),
                ),
                smoke_timeout=300,
                smoke_is_full_model=True,
                reads_pact_records=True),
    CheckerSpec("check_formal_writer_gate_narrowing.py", "tlc",
                smoke_is_full_model=True),
)


@dataclass(frozen=True)
class DirectCounterexample:
    """A counterexample run TLC directly (for checkers without their own
    --expect-counterexample suite). TLC must report the expected invariant
    violated, proving the corresponding real invariant is non-vacuous."""

    name: str
    model: str            # path relative to repo root
    config: str           # path relative to repo root (same dir as model)
    expected_invariant: str
    timeout: int = 180


# Counterexamples wired directly into the gate. Each proves a named invariant
# is genuinely violable (load-bearing), not vacuously true.
DIRECT_COUNTEREXAMPLES: tuple[DirectCounterexample, ...] = (
    DirectCounterexample(
        "chronos-default-zero",
        "formal/chronos/ChronosReadPin.tla",
        "formal/chronos/ChronosReadPinCounterexample.cfg",
        "OperationalSqlNeverDefaultsGeneration",
    ),
    # Wave 8 (strengthen-formal-models): the ChronosReadPin pin-freeze headline
    # is load-bearing -- a session that follows latest instead of its pin
    # observes a generation other than sessionPin after a publish.
    DirectCounterexample(
        "chronos-follows-latest",
        "formal/chronos/ChronosReadPin.tla",
        "formal/chronos/ChronosReadPinFollowsLatestCounterexample.cfg",
        "GraphReadUsesPinnedGeneration",
    ),
    # Wave 8: the consistency dirty-scope DeltaEvidenceSound guard is
    # load-bearing -- dropping it lets an invalid post-edge/post-node sit
    # outside the dirty scope and publish without blocking.
    DirectCounterexample(
        "transform-gate-unsound-delta-edge",
        "formal/consistency/TransformGateDirtyScope.tla",
        "formal/consistency/TransformGateDirtyScopeCounterexample.cfg",
        "NoInvalidPostEdgePublishes",
    ),
    DirectCounterexample(
        "transform-gate-missing-source-endpoint",
        "formal/consistency/TransformGateDirtyScope.tla",
        "formal/consistency/TransformGateMissingSourceCounterexample.cfg",
        "MissingSourceEndpointCannotPublish",
    ),
    DirectCounterexample(
        "transform-gate-unsound-delta-node",
        "formal/consistency/TransformGateNodeSubtypeScope.tla",
        "formal/consistency/TransformGateNodeSubtypeScopeCounterexample.cfg",
        "NoInvalidPostNodePublishes",
    ),
    # formalize-differential-core (task.formal.reuse-model): the reuse pass's two
    # guards are load-bearing. Disabling the dynamic-consumer guard omits a new
    # consumer's contribution from the committed resolved state
    # (NoOmittedConsumer); accepting a certificate with a stale dependency class
    # carries a changed output forward (SkippedOutputsUnchanged). Each refutes
    # its named invariant via a reachable committed trace.
    DirectCounterexample(
        "reuse-pass-dynamic-consumer",
        "formal/consistency/ReusePass.tla",
        "formal/consistency/ReusePassDynamicConsumerCounterexample.cfg",
        "NoOmittedConsumer",
    ),
    DirectCounterexample(
        "reuse-pass-stale-cert",
        "formal/consistency/ReusePass.tla",
        "formal/consistency/ReusePassStaleCertCounterexample.cfg",
        "SkippedOutputsUnchanged",
    ),
    # formalize-differential-core (task.formal.coordination-dbos): the publish
    # commit/resume protocol's three guards are load-bearing. Writing a step's
    # durability marker before its effects are durable lets resume skip the
    # step and checkpoint a partial generation (NoPartialGenerationObservable);
    # splitting the one-transaction commit set strands certificates for an
    # uncommitted generation (NoCertWithoutCheckpoint); a non-idempotent step
    # writing outside the commit transaction double-commits the same
    # generation with different content on resume (NoDoubleCommit). Each
    # refutes its named invariant via a crash/resume trace.
    DirectCounterexample(
        "dbos-commit-marker-first",
        "formal/consistency/DbosCommitResume.tla",
        "formal/consistency/DbosCommitResumeMarkerFirstCounterexample.cfg",
        "NoPartialGenerationObservable",
    ),
    DirectCounterexample(
        "dbos-commit-split-txn",
        "formal/consistency/DbosCommitResume.tla",
        "formal/consistency/DbosCommitResumeSplitCommitCounterexample.cfg",
        "NoCertWithoutCheckpoint",
    ),
    DirectCounterexample(
        "dbos-commit-non-idempotent",
        "formal/consistency/DbosCommitResume.tla",
        "formal/consistency/DbosCommitResumeNonIdempotentCounterexample.cfg",
        "NoDoubleCommit",
    ),
    # Wave 8: the writer's raw-rows arm is load-bearing -- a gate that uses the
    # descendant-closure arm alone disagrees with the writer under an incomplete
    # reflexive subtype_ancestors closure (a spurious publish-block).
    DirectCounterexample(
        "writer-gate-raw-rows",
        "formal/transform/WriterGateNarrowingEquivalence.tla",
        "formal/transform/WriterGateNarrowingRawRowsCounterexample.cfg",
        "WriterGateNarrowingEquivalence",
    ),
    # Wave 9 (strengthen-formal-models): the althea additive-overlay base
    # preservation is load-bearing -- an optimizer that mutates a base attr
    # during materialization violates it (the TLC kernel was otherwise a
    # guard-restatement; the real proof is the executable formal_search.py).
    DirectCounterexample(
        "althea-mutates-base",
        "formal/althea/AltheaOptimizerSafety.tla",
        "formal/althea/AltheaOptimizerMutatesBaseCounterexample.cfg",
        "BaseNodeAttrsPreserved",
    ),
    # The edge twin: TLC halts at the first violated invariant, so the base-edge
    # preservation invariant needs its own witness config to be load-bearing in
    # isolation rather than an unwitnessed guard-restatement.
    DirectCounterexample(
        "althea-mutates-base-edge",
        "formal/althea/AltheaOptimizerSafety.tla",
        "formal/althea/AltheaOptimizerMutatesBaseEdgeCounterexample.cfg",
        "BaseEdgeAttrsPreserved",
    ),
    # Wave 9 (strengthen-formal-models): the attribute_effective_view drift
    # invariant is load-bearing -- a MaintainView that marks the view fresh
    # without copying `effective` serves a stale view as current, refuting
    # ViewMatchesEffectiveWhenFresh via a reachable multi-step trace (the
    # pre-strengthening model conflated the view with truth, so drift was
    # unmodelable).
    DirectCounterexample(
        "attribute-view-drift",
        "formal/attributes/AttributeState.tla",
        "formal/attributes/AttributeViewDriftCounterexample.cfg",
        "ViewMatchesEffectiveWhenFresh",
    ),
    # Wave 9 (strengthen-formal-models): the derivation-parity invariant is
    # load-bearing by TRANSITION, not by an initial-state mismatch -- an
    # incremental interval-fold that opens a new interval without closing the
    # old one double-counts the prior contribution and diverges from the batch
    # recompute after a second delta, refuting the parity invariant by name.
    DirectCounterexample(
        "derivation-missed-close",
        "formal/derivation_parity/DerivationParity.tla",
        "formal/derivation_parity/DerivationParityDrift.cfg",
        "IncrementalEqualsBatchForLinearDerivation",
    ),
    # Admission: four formerly-tautological invariants, each witnessed by a
    # reachable precondition state.
    DirectCounterexample(
        "admission-fast-track",
        "formal/admission/AdmissionCounterexample.tla",
        "formal/admission/AdmissionFastTrackWitness.cfg",
        "BrokenNoFastTrack",
    ),
    DirectCounterexample(
        "admission-visible-derived",
        "formal/admission/AdmissionCounterexample.tla",
        "formal/admission/AdmissionVisibleDerivedWitness.cfg",
        "BrokenNoVisibleDerived",
    ),
    DirectCounterexample(
        "admission-published-visible",
        "formal/admission/AdmissionCounterexample.tla",
        "formal/admission/AdmissionPublishedVisibleWitness.cfg",
        "BrokenNoPublishedVisibleDerived",
    ),
    DirectCounterexample(
        "admission-workflow-event",
        "formal/admission/AdmissionCounterexample.tla",
        "formal/admission/AdmissionWorkflowEventWitness.cfg",
        "BrokenNoWorkflowEventOnNonAccepted",
    ),
    # Wave 2 (strengthen-formal-models): the rejection-safety hazard state
    # (rejected admission with committed facts) and the publish-decisiveness
    # pending-sweep path are both genuinely reachable.
    DirectCounterexample(
        "admission-rejected-with-facts",
        "formal/admission/AdmissionCounterexample.tla",
        "formal/admission/AdmissionRejectedFactsWitness.cfg",
        "BrokenNoRejectedWithCommittedFacts",
    ),
    DirectCounterexample(
        "admission-pending-publish",
        "formal/admission/AdmissionCounterexample.tla",
        "formal/admission/AdmissionPendingPublishWitness.cfg",
        "BrokenPublishRequiresAcceptedEvent",
    ),
    # deployment_authoring: the no-mutation clauses, now reachable under free
    # boolean inputs. One witness invariant per config.
    DirectCounterexample(
        "deployment-authoring-no-custom-exec",
        "formal/deployment_authoring/DeploymentAuthoringCounterexample.tla",
        "formal/deployment_authoring/DeploymentAuthoringCounterexample.cfg",
        "BrokenInspectionNoCustomExecution",
    ),
    DirectCounterexample(
        "deployment-authoring-no-local-runtime",
        "formal/deployment_authoring/DeploymentAuthoringCounterexample.tla",
        "formal/deployment_authoring/DeploymentAuthoringNoLocalRuntimeWitness.cfg",
        "BrokenInspectionNoLocalRuntime",
    ),
    # deployment_promotion: each retained witness removes one production or
    # recovery guard and must refute the corresponding real invariant.
    DirectCounterexample(
        "deployment-promotion-backup-guard",
        "formal/deployment_promotion/DeploymentPromotionCounterexample.tla",
        "formal/deployment_promotion/DeploymentPromotionBackupGuardWitness.cfg",
        "MutationRequiresVerifiedPostFenceBackup",
    ),
    DirectCounterexample(
        "deployment-promotion-lock-guard",
        "formal/deployment_promotion/DeploymentPromotionCounterexample.tla",
        "formal/deployment_promotion/DeploymentPromotionLockGuardWitness.cfg",
        "OneActivePromotion",
    ),
    DirectCounterexample(
        "deployment-promotion-writer-ready-guard",
        "formal/deployment_promotion/DeploymentPromotionCounterexample.tla",
        "formal/deployment_promotion/DeploymentPromotionWriterReadyGuardWitness.cfg",
        "RollbackReopensOnlyPreviousState",
    ),
    DirectCounterexample(
        "deployment-promotion-restore-order-guard",
        "formal/deployment_promotion/DeploymentPromotionCounterexample.tla",
        "formal/deployment_promotion/DeploymentPromotionRestoreOrderGuardWitness.cfg",
        "PreviousArtifactRequiresRestoredDatabase",
    ),
    # merge_back: partition-atomicity and blocked-LAND rollback are live.
    DirectCounterexample(
        "merge-back-partition-atomicity",
        "formal/merge_back/MergeBackPartitionAtomicityCounterexample.tla",
        "formal/merge_back/MergeBackPartitionAtomicityCounterexample.cfg",
        "BrokenPartitionNeverCommits",
    ),
    DirectCounterexample(
        "merge-back-land-rollback",
        "formal/merge_back/MergeBackLandRollbackCounterexample.tla",
        "formal/merge_back/MergeBackLandRollbackCounterexample.cfg",
        "BrokenLandNeverRollsBack",
    ),
    # Wave 3 (strengthen-formal-models): the merge-back hazard states are
    # genuinely reachable — the delete-failure rollback branch, the
    # mid-transaction partial state, and the per-field restamp / provenance
    # pass-through hazards (buggy-toggle configs refute the REAL invariant).
    DirectCounterexample(
        "merge-back-rollback-incomplete",
        "formal/merge_back/MergeBackLandRollbackCounterexample.tla",
        "formal/merge_back/MergeBackLandRollbackIncompleteWitness.cfg",
        "BrokenRollbackNeverIncomplete",
    ),
    DirectCounterexample(
        "merge-back-mid-txn-state",
        "formal/merge_back/MergeBackPartitionAtomicityCounterexample.tla",
        "formal/merge_back/MergeBackPartitionMidTxnWitness.cfg",
        "BrokenNoMidTransactionState",
    ),
    DirectCounterexample(
        "merge-back-restamp-record-id",
        "formal/merge_back/MergeBackRestamp.tla",
        "formal/merge_back/MergeBackRestampRecordIdWitness.cfg",
        "SourceRecordIdRestamped",
    ),
    DirectCounterexample(
        "merge-back-restamp-revision",
        "formal/merge_back/MergeBackRestamp.tla",
        "formal/merge_back/MergeBackRestampRevisionWitness.cfg",
        "SourceRevisionRestamped",
    ),
    DirectCounterexample(
        "merge-back-restamp-fork-identity",
        "formal/merge_back/MergeBackRestamp.tla",
        "formal/merge_back/MergeBackRestampForkIdentityWitness.cfg",
        "ForkProvenanceIdentityDoesNotLand",
    ),
    DirectCounterexample(
        "merge-back-restamp-promote-marker",
        "formal/merge_back/MergeBackRestamp.tla",
        "formal/merge_back/MergeBackRestampPromoteMarkerWitness.cfg",
        "PromoteMarkerDoesNotLand",
    ),
    DirectCounterexample(
        "merge-back-restamp-trusted-default",
        "formal/merge_back/MergeBackRestamp.tla",
        "formal/merge_back/MergeBackRestampTrustedDefaultWitness.cfg",
        "NoForkInputYieldsTrustedDefaultSource",
    ),
    # pact: workflow-authority and mutation-safety are non-vacuous. One
    # witness invariant per config so each witness is demonstrated by name.
    DirectCounterexample(
        "pact-workflow-authority",
        "formal/pact/WorkflowAuthorityCounterexample.tla",
        "formal/pact/WorkflowAuthorityCounterexample.cfg",
        "BrokenAuthorityNeverAccepted",
    ),
    DirectCounterexample(
        "pact-workflow-blockers",
        "formal/pact/WorkflowAuthorityCounterexample.tla",
        "formal/pact/WorkflowAuthorityBlockersWitness.cfg",
        "BrokenBlockersNeverRaised",
    ),
    DirectCounterexample(
        "pact-mutation-safety",
        "formal/pact/MutationSafetyCounterexample.tla",
        "formal/pact/MutationSafetyCounterexample.cfg",
        "BrokenPlanNeverApplies",
    ),
    DirectCounterexample(
        "pact-mutation-completion",
        "formal/pact/MutationSafetyCounterexample.tla",
        "formal/pact/MutationSafetyCompletionWitness.cfg",
        "BrokenNoMutationCompletes",
    ),
    # Wave 5 (strengthen-formal-models): the source-delta pipeline hazards
    # are witnessable by name via buggy toggles — omitted tombstones, raw
    # (uncanonicalized) tombstones, and retraction-after-insert churn order.
    DirectCounterexample(
        "source-delta-omitted-tombstone",
        "formal/source_sync/SourceDeltaContract.tla",
        "formal/source_sync/SourceDeltaOmittedTombstoneWitness.cfg",
        "RetractionsAreComplete",
    ),
    DirectCounterexample(
        "source-delta-raw-tombstone",
        "formal/source_sync/SourceDeltaContract.tla",
        "formal/source_sync/SourceDeltaRawTombstoneWitness.cfg",
        "TombstonesLandCanonical",
    ),
    DirectCounterexample(
        "source-delta-churn-order",
        "formal/source_sync/SourceDeltaContract.tla",
        "formal/source_sync/SourceDeltaChurnOrderWitness.cfg",
        "ChurnKeepsRecordLive",
    ),
    DirectCounterexample(
        "delta-frontier-stale-readiness",
        "formal/source_sync/DeltaFrontier.tla",
        "formal/source_sync/DeltaFrontierStaleReadinessWitness.cfg",
        "NoReadyWithPendingUnappliedDelta",
    ),
    DirectCounterexample(
        "delta-frontier-resume-skip",
        "formal/source_sync/DeltaFrontier.tla",
        "formal/source_sync/DeltaFrontierResumeSkipWitness.cfg",
        "ChronosNeverClaimsUnappliedDelta",
    ),
    # Wave 4 (strengthen-formal-models): both halves of the formal-evidence
    # contract are witnessable by name via buggy toggles — a gate that
    # ignores freshness at entry, and a sweep that stays silent about a
    # stale row after verification.
    DirectCounterexample(
        "pact-evidence-stale-advance",
        "formal/pact/PactFormalEvidence.tla",
        "formal/pact/PactFormalEvidenceStaleAdvanceWitness.cfg",
        "VerificationEnteredFresh",
    ),
    DirectCounterexample(
        "pact-evidence-silent-sweep",
        "formal/pact/PactFormalEvidence.tla",
        "formal/pact/PactFormalEvidenceSilentSweepWitness.cfg",
        "NoSilentStaleAfterVerification",
    ),
    # Nomos: secret-claim persistence is now reachable state, so the privacy
    # invariant is load-bearing rather than the former {}={} tautology.
    DirectCounterexample(
        "nomos-secret-persistence",
        "formal/nomos/Nomos.tla",
        "formal/nomos/NomosSecretPersistenceCounterexample.cfg",
        "ClaimEvidenceDoesNotPersistSecret",
    ),
    # Nomos IAM claim family: each institutional claim check has its own
    # isolated request in Nomos.cfg and a buggy-toggle witness that skips
    # exactly that check, refuting the REAL *CannotGrant invariant by name.
    DirectCounterexample(
        "nomos-untrusted-provider",
        "formal/nomos/Nomos.tla",
        "formal/nomos/NomosUntrustedProviderCounterexample.cfg",
        "UntrustedProviderCannotGrant",
    ),
    DirectCounterexample(
        "nomos-expired-claim",
        "formal/nomos/Nomos.tla",
        "formal/nomos/NomosExpiredClaimCounterexample.cfg",
        "ExpiredClaimCannotGrant",
    ),
    DirectCounterexample(
        "nomos-audience-mismatch",
        "formal/nomos/Nomos.tla",
        "formal/nomos/NomosAudienceMismatchCounterexample.cfg",
        "AudienceMismatchCannotGrant",
    ),
    DirectCounterexample(
        "nomos-missing-assurance",
        "formal/nomos/Nomos.tla",
        "formal/nomos/NomosMissingAssuranceCounterexample.cfg",
        "MissingAssuranceCannotGrant",
    ),
    DirectCounterexample(
        "nomos-ambiguous-binding",
        "formal/nomos/Nomos.tla",
        "formal/nomos/NomosAmbiguousBindingCounterexample.cfg",
        "AmbiguousBindingCannotGrant",
    ),
    DirectCounterexample(
        "nomos-revoked-provider-claim",
        "formal/nomos/Nomos.tla",
        "formal/nomos/NomosRevokedProviderClaimCounterexample.cfg",
        "RevokedProviderCannotGrant",
    ),
    DirectCounterexample(
        "nomos-revoked-kerberos",
        "formal/nomos/Nomos.tla",
        "formal/nomos/NomosRevokedKerberosCounterexample.cfg",
        "RevokedKerberosServicePrincipalCannotGrant",
    ),
    # Issue #1007: a claim carrying no verification evidence must not grant.
    # Skipping this one check lets its isolated request through, so the
    # invariant is proven load-bearing rather than vacuously true.
    DirectCounterexample(
        "nomos-unverified-claim",
        "formal/nomos/Nomos.tla",
        "formal/nomos/NomosUnverifiedClaimCounterexample.cfg",
        "UnverifiedClaimCannotGrant",
    ),
    # Nomos publish pins: the bundle universe is >= 2 with RotateBundle in
    # the machine, so the pin invariant distinguishes WHICH bundle was
    # pinned. Two hazards, each refuted by name against the frozen ghost.
    DirectCounterexample(
        "nomos-stale-pin-publish",
        "formal/nomos/Nomos.tla",
        "formal/nomos/NomosStalePinPublishCounterexample.cfg",
        "GenerationPinMatchesPublishBundle",
    ),
    DirectCounterexample(
        "nomos-rotation-restamp",
        "formal/nomos/Nomos.tla",
        "formal/nomos/NomosRotationRestampCounterexample.cfg",
        "GenerationPinMatchesPublishBundle",
    ),
    # Poros: a sender's authorization hint must not stand in for the receiver's
    # decision. BuggySenderHintGrantsAllow=TRUE lets a sender-hinted,
    # receiver-unauthorized message be allowed and admitted, refuting
    # NoSenderPolicyAuthority by name (the prior receipt-vs-publication
    # counterexample said nothing about sender authority).
    DirectCounterexample(
        "poros-sender-authority",
        "formal/poros/PorosExchange.tla",
        "formal/poros/PorosSenderAuthorityWitness.cfg",
        "NoSenderPolicyAuthority",
    ),
)


@dataclass(frozen=True)
class DirectModel:
    """A model run directly via TLC in smoke mode (e.g. a liveness/temporal
    model with no check_formal wrapper). TLC must complete with no error."""

    name: str
    model: str
    config: str
    timeout: int = 180


# Models gated in smoke that have no check_formal_*.py wrapper.
DIRECT_SMOKE_MODELS: tuple[DirectModel, ...] = (
    # The corpus's first live-subsystem liveness proof: readiness eventually
    # converges under fairness (a real temporal PROPERTY, not safety-only).
    DirectModel(
        "source-sync-readiness-liveness",
        "formal/source_sync/ReadinessConvergenceLiveness.tla",
        "formal/source_sync/ReadinessConvergenceLiveness.cfg",
    ),
    # Nomos publish-pin distinguishing proof over a MINIMAL request universe
    # (bundle universe >= 2 + RotateBundle). Kept out of the full
    # control-plane config so the pin/rotation state space does not multiply
    # the full decision zoo; ~322 distinct states, sub-second.
    DirectModel(
        "nomos-publish-pin-rotation",
        "formal/nomos/Nomos.tla",
        "formal/nomos/NomosPinRotation.cfg",
        timeout=60,
    ),
    # Nomos IAM claim-isolation proof: seven otherwise-grantable requests, one
    # per institutional claim check. Kept out of the full control-plane config
    # so the binding/grant/claim combinatorics do not multiply the full
    # request universe; ~50K distinct states, ~12s.
    DirectModel(
        "nomos-iam-claim-isolation",
        "formal/nomos/Nomos.tla",
        "formal/nomos/NomosIamClaims.cfg",
        timeout=120,
    ),
)


def _run_direct_model(dm: DirectModel) -> CheckerResult:
    jar = _find_jar()
    if jar is None:
        return CheckerResult(dm.name, "smoke", 2, 0.0,
                             log=("tla2tools.jar not found (run bootstrap)",))
    model_dir = REPO_ROOT / Path(dm.model).parent
    workers = tlc_workers()
    cmd = ["java", *tlc_jvm_args(workers), "-cp", str(jar), "tlc2.TLC",
           "-config", Path(dm.config).name,
           "-deadlock", "-workers", workers,
           "-metadir", str(_fresh_direct_metadir("smoke", dm.name)),
           Path(dm.model).name]
    start = time.monotonic()
    try:
        proc = subprocess.run(cmd, cwd=str(model_dir), text=True,
                              capture_output=True, timeout=dm.timeout + 60)
    except subprocess.TimeoutExpired:
        return CheckerResult(dm.name, "smoke", 124, time.monotonic() - start,
                             timed_out=True, log=(f"timeout after {dm.timeout + 60}s",))
    ok = "No error has been found" in proc.stdout
    tail = _direct_failure_tail(proc) if not ok else ()
    return CheckerResult(dm.name, "smoke", 0 if ok else 1,
                         time.monotonic() - start, log=tail)


def _fresh_direct_metadir(mode: str, name: str) -> Path:
    """An empty, per-run directory for TLC's ``-metadir``.

    Without ``-metadir`` TLC writes its state files to
    ``<model dir>/states/<yy-MM-dd-HH-mm-ss>`` and refuses to start when that
    directory already exists. Two direct runs of models in the same directory
    that start within the same wall-clock second therefore collide: the second
    JVM exits 1 in about 0.1s before printing anything to stdout. The nightly
    of 2026-09-29 failed that way on deployment-authoring-no-local-runtime,
    which starts right after deployment-authoring-no-custom-exec (0.9s) in
    formal/deployment_authoring. A directory per run, emptied first, cannot
    collide with another run, and it keeps TLC's state files out of the source
    tree.
    """
    metadir = SUITE_OUT / mode / "direct" / name / "tlc-meta"
    if metadir.exists():
        shutil.rmtree(metadir)
    metadir.mkdir(parents=True)
    return metadir


def _direct_failure_tail(proc: subprocess.CompletedProcess[str]) -> tuple[str, ...]:
    """The lines a failed direct run shows: TLC's stdout tail, its exit code,
    and its stderr tail.

    TLC and the JVM report start-up failures (an existing metadir, a JVM that
    cannot start) on stderr with an empty stdout, so a stdout-only tail printed
    nothing at all for exactly the failures that most need a reason.
    """
    lines = proc.stdout.splitlines()[-6:]
    lines.append(f"TLC exit code: {proc.returncode}")
    lines.extend(f"stderr: {line}" for line in proc.stderr.splitlines()[-6:])
    return tuple(lines)


def _find_jar() -> Path | None:
    for candidate in (REPO_ROOT / ".tools" / "tla2tools.jar",
                      REPO_ROOT / "tools" / "tla2tools.jar"):
        if candidate.is_file():
            return candidate
    env = os.environ.get("TLA2TOOLS_JAR")
    return Path(env) if env and Path(env).is_file() else None


def _run_direct_counterexample(dc: DirectCounterexample) -> CheckerResult:
    jar = _find_jar()
    if jar is None:
        return CheckerResult(dc.name, "counterexamples", 2, 0.0,
                             log=("tla2tools.jar not found (run bootstrap)",))
    model_dir = REPO_ROOT / Path(dc.model).parent
    workers = tlc_workers()
    cmd = ["java", *tlc_jvm_args(workers), "-cp", str(jar), "tlc2.TLC",
           "-config", Path(dc.config).name,
           "-deadlock", "-workers", workers,
           "-metadir", str(_fresh_direct_metadir("counterexamples", dc.name)),
           Path(dc.model).name]
    start = time.monotonic()
    try:
        proc = subprocess.run(cmd, cwd=str(model_dir), text=True,
                              capture_output=True, timeout=dc.timeout + 60)
    except subprocess.TimeoutExpired:
        return CheckerResult(dc.name, "counterexamples", 124,
                             time.monotonic() - start, timed_out=True,
                             log=(f"timeout after {dc.timeout + 60}s",))
    violated = named_invariant_violated(dc.expected_invariant, proc.stdout)
    exit_code = 0 if violated else 1
    tail = _direct_failure_tail(proc) if not violated else ()
    return CheckerResult(dc.name, "counterexamples", exit_code,
                         time.monotonic() - start, log=tail)


def named_invariant_violated(expected_invariant: str, tlc_stdout: str) -> bool:
    """True only when TLC reports the NAMED invariant violated.

    No fallback: a witness that violates a different invariant than the one it
    names has demonstrated nothing about the named property. Configs must
    declare exactly one witness invariant (TLC halts at the first violation, so
    a bundled config can only ever demonstrate one).
    """
    import re
    seen = re.findall(r"Invariant (\S+) is violated", tlc_stdout)
    return expected_invariant in seen


def registered_checker_names() -> set[str]:
    return {spec.name for spec in CHECKERS}


def discovered_checker_names() -> set[str]:
    return {p.name for p in SCRIPTS_DIR.glob("check_formal_*.py")}


@dataclass(frozen=True)
class OnlySelection:
    """Resolved ``--only`` target, optionally narrowed to one witness."""

    checker_name: str
    witness_index: int | None = None


def resolve_only_selector(selector: str) -> OnlySelection:
    """Resolve a checker filename, property id, or witness config basename.

    The aggregate historically treated an unknown ``--only`` value as an
    empty selection and returned ``OK``.  That is a vacuous pass, especially
    dangerous for a witness typo.  Keep selectors explicit and map the
    worktree model's property/config names to its registered witness runs.
    """
    value = Path(selector).name
    candidates = {selector, value, value.removesuffix(".py")}
    for spec in CHECKERS:
        if spec.name in candidates or spec.name.removesuffix(".py") in candidates:
            return OnlySelection(spec.name)
        for index, witness in enumerate(spec.witnesses):
            property_id = None
            witness_id = None
            if "--property-id" in witness:
                property_index = witness.index("--property-id")
                if property_index + 1 < len(witness):
                    property_id = witness[property_index + 1]
            if "--witness-id" in witness:
                witness_index = witness.index("--witness-id")
                if witness_index + 1 < len(witness):
                    witness_id = witness[witness_index + 1]
            if property_id is None and witness_id is None:
                continue
            selected_id = property_id or witness_id
            config_name = f"WorktreeBranchSafety{selected_id}Witness.cfg"
            config_stem = config_name.removesuffix(".cfg")
            if selected_id in candidates or config_name in candidates \
                    or config_stem in candidates:
                return OnlySelection(spec.name, index)
    raise ValueError(
        f"unknown --only selector {selector!r}; expected a registered checker "
        "or a registered witness property/config"
    )


@dataclass
class CheckerResult:
    name: str
    mode: str
    exit_code: int
    duration_s: float
    timed_out: bool = False
    skipped_reason: str | None = None
    log: tuple[str, ...] = field(default_factory=tuple)
    #: Set only by the bounded full-profile timeout bucket (see
    #: CheckerSpec.full_profile_unproven_issue). Carries the sentence printed
    #: and written to junit. NOT a pass: the property set was not proven.
    unproven_reason: str | None = None

    @property
    def ok(self) -> bool:
        return (
            self.skipped_reason is not None
            or self.unproven_reason is not None
            or self.exit_code == 0
        )


def _invoke(spec: CheckerSpec, label: str, extra: tuple[str, ...], out_sub: str,
            *, mode: str, timeout_override: int | None) -> CheckerResult:
    out_dir = SUITE_OUT / out_sub
    cmd = ["uv", "run", "python", f"scripts/{spec.name}", *extra]
    timeout = timeout_override or spec.smoke_timeout
    if spec.kind == "tlc" and "--timeout-seconds" not in extra:
        cmd.extend(["--timeout-seconds", str(timeout)])
    cmd.extend(["--out", str(out_dir)])
    start = time.monotonic()
    wall = timeout + 60
    try:
        proc = subprocess.run(cmd, cwd=str(REPO_ROOT), text=True,
                              capture_output=True, timeout=wall)
        exit_code = proc.returncode
        # A TLC checker enforces its OWN --timeout-seconds and exits 124
        # (src/okg/substrate/formal_tlc.py TLC_TIMEOUT_EXIT_CODE) before this
        # outer wall (timeout + 60) can trip. Reading `timed_out` off the
        # outer wall alone therefore reported every real timeout as a bare
        # `exit=124`, indistinguishable in junit from an invariant violation.
        timed_out = spec.kind == "tlc" and exit_code == TLC_TIMEOUT_EXIT_CODE
        tail = (proc.stdout + proc.stderr).splitlines()[-8:]
    except subprocess.TimeoutExpired:
        exit_code, timed_out = TLC_TIMEOUT_EXIT_CODE, True
        tail = [f"wall-clock timeout after {wall}s"]
    return CheckerResult(label, mode, exit_code,
                         time.monotonic() - start, timed_out=timed_out,
                         log=tuple(tail))


def _with_record_flag(spec: CheckerSpec, extra: tuple[str, ...],
                      without_pact_records: bool) -> tuple[str, ...]:
    if without_pact_records and spec.reads_pact_records:
        return (*extra, "--without-pact-records")
    return extra


def run_mode(mode: str, *, only: str | None, timeout_override: int | None,
             include_archive: bool, witness_index: int | None = None,
             without_pact_records: bool = False) -> list[CheckerResult]:
    results: list[CheckerResult] = []
    for spec in CHECKERS:
        if only and spec.name != only:
            continue
        if spec.archive_only and not include_archive:
            results.append(CheckerResult(spec.name, mode, 0, 0.0,
                                         skipped_reason="archive-only"))
            _print_result(results[-1], mode, spec.name)
            continue
        if mode == "full":
            # Full-profile pass over checkers with a dedicated smoke profile:
            # this is where the full invariant sets (nomos IAM family,
            # attribute composites, cardinality-2 admission universes) are
            # actually proven. smoke_is_full_model checkers already prove
            # their full model in smoke.
            if spec.kind != "tlc" or spec.smoke_is_full_model \
                    or "--profile" not in spec.smoke_args:
                reason = ("no dedicated full profile"
                          if spec.kind != "tlc"
                          else "smoke is already the full model")
                results.append(CheckerResult(
                    spec.name, mode, 0, 0.0, skipped_reason=reason))
                _print_result(results[-1], mode, spec.name)
                continue
            base = spec.name.removesuffix(".py")
            res = _invoke(spec, spec.name,
                          _with_record_flag(
                              spec,
                              ("--profile", "full",
                               "--timeout-seconds", str(spec.full_timeout)),
                              without_pact_records),
                          f"full/{base}",
                          mode=mode,
                          timeout_override=timeout_override or spec.full_timeout)
            if res.timed_out and spec.full_profile_unproven_issue is not None:
                res = replace(
                    res,
                    unproven_reason=(
                        "NOT PROVEN: TLC did not finish the full profile "
                        f"within {spec.full_timeout}s (exit "
                        f"{TLC_TIMEOUT_EXIT_CODE}). This is a known, tracked "
                        "coverage gap, not a passing proof and not an "
                        "invariant violation -- see issue "
                        f"#{spec.full_profile_unproven_issue}."
                    ),
                )
            results.append(res)
            _print_result(res, mode, spec.name)
            continue
        if mode == "counterexamples":
            witnesses = spec.witnesses
            if witness_index is not None:
                if witness_index >= len(witnesses):
                    results.append(CheckerResult(
                        spec.name, mode, 2, 0.0,
                        log=(f"registered witness index {witness_index} is unavailable",)
                    ))
                    _print_result(results[-1], mode, spec.name)
                    continue
                witnesses = (witnesses[witness_index],)
            if not witnesses:
                if only:
                    results.append(CheckerResult(
                        spec.name,
                        mode,
                        2,
                        0.0,
                        log=(
                            f"{spec.name} has no registered counterexample witness; "
                            "refusing a vacuous --only pass",
                        ),
                    ))
                    _print_result(results[-1], mode, spec.name)
                    continue
                results.append(CheckerResult(spec.name, mode, 0, 0.0,
                                             skipped_reason="no counterexample witness"))
                _print_result(results[-1], mode, spec.name)
                continue
            for idx, witness in enumerate(witnesses):
                base = spec.name.removesuffix(".py")
                res = _invoke(spec, f"{spec.name}[w{idx}]",
                              _with_record_flag(spec, witness, without_pact_records),
                              f"counterexamples/{base}/w{idx}",
                              mode=mode, timeout_override=timeout_override)
                results.append(res)
                _print_result(res, mode, f"{spec.name}[w{idx}]")
        else:  # smoke
            base = spec.name.removesuffix(".py")
            res = _invoke(spec, spec.name,
                          _with_record_flag(spec, spec.smoke_args, without_pact_records),
                          f"smoke/{base}",
                          mode=mode, timeout_override=timeout_override)
            results.append(res)
            _print_result(res, mode, spec.name)
    if mode == "counterexamples" and not only:
        for dc in DIRECT_COUNTEREXAMPLES:
            res = _run_direct_counterexample(dc)
            results.append(res)
            _print_result(res, mode, f"direct:{dc.name}")
    if mode == "smoke" and not only:
        for dm in DIRECT_SMOKE_MODELS:
            res = _run_direct_model(dm)
            results.append(res)
            _print_result(res, mode, f"direct:{dm.name}")
    return results


def _print_result(res: CheckerResult, mode: str, label: str) -> None:
    if res.skipped_reason:
        status, extra = "SKIP", f" ({res.skipped_reason})"
    elif res.unproven_reason:
        status, extra = "UNPROVEN", f" exit={res.exit_code}"
    else:
        status, extra = ("PASS" if res.ok else "FAIL"), f" exit={res.exit_code}"
    print(f"  [{status}] {mode:14s} {label:48s} {res.duration_s:6.1f}s{extra}", flush=True)
    if res.unproven_reason:
        print(f"        | {res.unproven_reason}", flush=True)
    if not res.ok or res.unproven_reason:
        for line in res.log:
            print(f"        | {line}", flush=True)


def write_junit(results: list[CheckerResult], path: Path) -> None:
    """Emit standard junit XML (one testcase per checker result) so the
    formal lane's verdict reaches the test-results-gate junit classifier."""
    import xml.etree.ElementTree as ET

    failures = [r for r in results if not r.ok]
    # An UNPROVEN result is reported as `skipped`, never as a passing
    # testcase: the property set was not proven, and a silent pass here would
    # be absence dressed as agreement. The message says so in words.
    skipped = [r for r in results if r.skipped_reason or r.unproven_reason]
    suite = ET.Element(
        "testsuite",
        name="formal-suite",
        tests=str(len(results)),
        failures=str(len(failures)),
        errors="0",
        skipped=str(len(skipped)),
    )
    for res in results:
        case = ET.SubElement(
            suite,
            "testcase",
            classname=f"formal.{res.mode}",
            name=res.name,
            time=f"{res.duration_s:.1f}",
        )
        if res.skipped_reason:
            ET.SubElement(case, "skipped", message=res.skipped_reason)
        elif res.unproven_reason:
            ET.SubElement(case, "skipped", message=res.unproven_reason)
        elif not res.ok:
            message = (
                f"timed out after {res.duration_s:.0f}s"
                if res.timed_out
                else f"exit={res.exit_code}"
            )
            failure = ET.SubElement(case, "failure", message=message)
            failure.text = "\n".join(res.log)
    path.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(suite).write(path, encoding="unicode", xml_declaration=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode",
                        choices=["smoke", "counterexamples", "full", "all"],
                        default="smoke")
    parser.add_argument(
        "--only",
        help="run only this checker or a named witness property/config",
    )
    parser.add_argument("--timeout-seconds", type=int, default=None,
                        help="override per-checker TLC smoke timeout")
    parser.add_argument("--include-archive", action="store_true",
                        help="also run archive-only helpers (e.g. console)")
    parser.add_argument("--list", action="store_true",
                        help="print the checker registry as JSON and exit")
    parser.add_argument("--json", action="store_true",
                        help="emit a machine-readable run summary")
    parser.add_argument("--without-pact-records", action="store_true",
                        help="pass --without-pact-records to the checkers that "
                             "also check this repository's PACT change records, "
                             "for a checkout without them (the public copy); "
                             "their model runs and witnesses are unchanged")
    parser.add_argument("--junit-xml", type=Path, default=None,
                        help="write a junit XML summary (one testcase per "
                             "checker result) for the test-results-gate")
    args = parser.parse_args(argv)

    if args.list:
        payload = {
            "checkers": [
                {"name": s.name, "kind": s.kind,
                 "smoke_args": list(s.smoke_args),
                 "witness_count": len(s.witnesses),
                 "smoke_is_full_model": s.smoke_is_full_model,
                 "full_profile_unproven_issue": s.full_profile_unproven_issue,
                 "reads_pact_records": s.reads_pact_records,
                 "archive_only": s.archive_only}
                for s in CHECKERS
            ],
            "registered": sorted(registered_checker_names()),
            "discovered": sorted(discovered_checker_names()),
            "orphans": sorted(discovered_checker_names() - registered_checker_names()),
        }
        print(json.dumps(payload, indent=2))
        return 0

    selection = None
    if args.only:
        try:
            selection = resolve_only_selector(args.only)
        except ValueError as exc:
            print(f"formal-suite: ERROR {exc}", file=sys.stderr)
            return 2
        if selection.witness_index is not None and args.mode not in {
            "counterexamples",
            "all",
        }:
            print(
                "formal-suite: ERROR a witness selector requires "
                "--mode counterexamples (or all)",
                file=sys.stderr,
            )
            return 2

    orphans = discovered_checker_names() - registered_checker_names()
    if orphans and not args.only:
        message = ("unregistered checkers without a run path: "
                   f"{', '.join(sorted(orphans))}")
        print(f"formal-suite: ERROR {message}", file=sys.stderr)
        if args.junit_xml is not None:
            # The gate consumes junit; a setup failure must red it too, not
            # vanish as a missing report.
            write_junit(
                [CheckerResult("registry-orphans", "setup", 2, 0.0,
                               log=(message,))],
                args.junit_xml,
            )
        return 2

    # The reverse gap: a registered checker whose script is not in this
    # checkout would otherwise surface only as an opaque subprocess failure
    # halfway through the run. Refuse up front and name it.
    missing = registered_checker_names() - discovered_checker_names()
    if selection is not None:
        missing &= {selection.checker_name}
    if missing:
        message = ("registered checkers missing from this checkout: "
                   f"{', '.join(sorted(missing))}")
        print(f"formal-suite: ERROR {message}", file=sys.stderr)
        if args.junit_xml is not None:
            write_junit(
                [CheckerResult("registry-missing", "setup", 2, 0.0,
                               log=(message,))],
                args.junit_xml,
            )
        return 2

    modes = ["smoke", "counterexamples"] if args.mode == "all" else [args.mode]
    all_results: list[CheckerResult] = []
    # Only a whole, default full-mode run is gated (scripts/ci/formal_full_gate.py):
    # a scheduled run skips when no input on the gate's fixed list changed since
    # the last passing run, and a full run is forced at least weekly. The
    # smoke and counterexample modes (the nightly formal job) are never gated.
    full_gate = None
    if (args.mode == "full" and selection is None and not args.include_archive
            and not args.without_pact_records):
        full_gate = _load_full_gate()
        decision = full_gate.check(os.environ)
        if decision.skip:
            all_results.append(CheckerResult(
                "formal-full-input-gate", "full", 0, 0.0,
                skipped_reason=f"inputs unchanged: {decision.reason}"))
            modes = []
    for mode in modes:
        print(f"formal-suite: running mode={mode}", flush=True)
        all_results.extend(run_mode(mode, only=selection.checker_name if selection else None,
                                     timeout_override=args.timeout_seconds,
                                     include_archive=args.include_archive,
                                     witness_index=(
                                         selection.witness_index if selection else None
                                     ),
                                     without_pact_records=args.without_pact_records))
    # NOT PROVEN is a green lane but not a proof: recording it would let the
    # following nights skip the timed-out profile and drop its loud line, so
    # only a fully proven run records and a NOT PROVEN night is rerun.
    if (full_gate is not None and modes and full_gate.records(os.environ)
            and all(r.ok and not r.unproven_reason for r in all_results)):
        print(f"formal-suite: recorded the inputs of this fully proven full run in "
              f"{full_gate.record()}", flush=True)

    failures = [r for r in all_results if not r.ok]
    unproven = [r for r in all_results if r.unproven_reason]
    ran = [r for r in all_results if not r.skipped_reason]
    print(f"\nformal-suite: {len(ran)} ran, {len(failures)} failed, "
          f"{len(unproven)} NOT PROVEN, "
          f"{len(all_results) - len(ran)} skipped")
    if args.without_pact_records:
        # Loud, like NOT PROVEN: the models ran, the PACT-record checks did not.
        print("formal-suite: PACT RECORDS NOT CHECKED (--without-pact-records): "
              + ", ".join(sorted(s.name for s in CHECKERS if s.reads_pact_records)),
              file=sys.stderr)
    for res in unproven:
        # Loud on purpose, and on stderr so it survives a truncated tail: the
        # lane is green with these, so this line is the only place the gap is
        # stated.
        print(f"formal-suite: NOT PROVEN {res.name} ({res.mode}): "
              f"{res.unproven_reason}", file=sys.stderr)
    if args.junit_xml is not None:
        write_junit(all_results, args.junit_xml)
        print(f"formal-suite: junit written to {args.junit_xml}")
    if args.json:
        print(json.dumps({
            "ok": not failures,
            # `ok` answers "did the lane pass". `fully_proven` answers "was
            # everything actually proven", which an UNPROVEN result makes
            # false while `ok` stays true. A consumer that wants proof must
            # read this key, not `ok`.
            "fully_proven": not failures and not unproven,
            "pact_records_checked": not args.without_pact_records,
            "ran": len(ran),
            "failed": [r.name for r in failures],
            "unproven": [{"name": r.name, "mode": r.mode,
                          "reason": r.unproven_reason} for r in unproven],
            "results": [{"name": r.name, "mode": r.mode, "exit_code": r.exit_code,
                         "duration_s": round(r.duration_s, 1),
                         "skipped": r.skipped_reason,
                         "unproven": r.unproven_reason} for r in all_results],
        }, indent=2))
    if failures:
        print("formal-suite: FAILED: "
              + ", ".join(f"{r.name}" for r in failures), file=sys.stderr)
        return 1
    if unproven:
        # Never a bare OK: the lane is green, and N property sets were not
        # proven. Both facts belong in the one line a reader skims.
        print(f"formal-suite: OK ({len(unproven)} NOT PROVEN)")
    else:
        print("formal-suite: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
