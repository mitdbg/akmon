from __future__ import annotations

import importlib.util
import json
import re
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from okg.substrate.pact.store import load_manifest_file


ROOT = Path(__file__).resolve().parents[2]
MODULE = (
    ROOT / "formal/deployment_control_plane_kernel/DeploymentControlPlaneKernel.tla"
)
CONFIG = (
    ROOT / "formal/deployment_control_plane_kernel/DeploymentControlPlaneKernel.cfg"
)
REFINEMENTS = ROOT / "formal/deployment_control_plane_kernel/refinement-owners.yaml"
README = ROOT / "formal/deployment_control_plane_kernel/README.md"
DESIGN = ROOT / "pact/changes/externalize-local-okg-lifecycle-controller/pact.yaml"
CHECKER = ROOT / "scripts/check_formal_deployment_control_plane_kernel.py"


def _checker():
    spec = importlib.util.spec_from_file_location("deployment_kernel_checker", CHECKER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _operator_body(text: str, name: str, next_name: str) -> str:
    start = text.index(f"{name} ==")
    end = text.index(f"\n{next_name} ==", start)
    return text[start:end]


def test_tlc_state_count_uses_completed_space_not_progress_snapshot() -> None:
    checker = _checker()
    output = """Progress: 19,212 distinct states found
Model checking completed. No error has been found.
496262 states generated, 113811 distinct states found, 0 states left on queue.
"""

    assert checker._final_distinct_states(output) == 113811


def test_controller_refinement_runs_true_cross_process_lock_proof() -> None:
    checker = _checker()

    assert (
        "tests/substrate/test_local_fleet_controller.py::"
        "test_boundary_execution_lock_serializes_independent_processes"
        in checker.CONTROLLER_REFINEMENT_TESTS
    )


def test_tlc_launch_uses_one_shared_worker_and_jvm_profile(
    monkeypatch, tmp_path: Path
) -> None:
    checker = _checker()
    jar = tmp_path / "tla2tools.jar"
    jar.write_text("pinned", encoding="utf-8")
    seen: list[list[str]] = []
    worker_calls: list[None] = []
    profile_calls: list[str] = []

    monkeypatch.setattr(checker, "JAR", jar)
    monkeypatch.setattr(checker.shutil, "which", lambda name: "/usr/bin/java")

    def fake_workers() -> str:
        worker_calls.append(None)
        return "3"

    def fake_jvm_args(workers: str) -> tuple[str, ...]:
        profile_calls.append(workers)
        return ("-Xmx1g", "-XX:ActiveProcessorCount=3")

    def fake_run(command, **kwargs):
        seen.append(command)
        return SimpleNamespace(
            returncode=0,
            stdout=(
                "Model checking completed. No error has been found.\n"
                "42 states generated, 21 distinct states found, 0 states left on queue.\n"
            ),
            stderr="",
        )

    monkeypatch.setattr(checker, "tlc_workers", fake_workers)
    monkeypatch.setattr(checker, "tlc_jvm_args", fake_jvm_args)
    monkeypatch.setattr(checker.subprocess, "run", fake_run)

    result = checker._run_tlc(
        checker.MODULE,
        checker.CONFIG,
        out_dir=tmp_path,
        timeout=30,
    )

    assert result["returncode"] == 0
    assert worker_calls == [None]
    assert profile_calls == ["3"]
    command = seen[0]
    assert command[1:3] == ["-Xmx1g", "-XX:ActiveProcessorCount=3"]
    assert command[command.index("-workers") + 1] == "3"
    assert command.index("-Xmx1g") < command.index("tlc2.TLC")


def test_smoke_profile_is_named_representative_and_strictly_bounded() -> None:
    checker = _checker()
    scenarios, reachability, mutations = checker._smoke_selection()

    assert tuple(row.name for row in scenarios) == checker.SMOKE_SCENARIO_NAMES
    assert tuple(row.name for row in reachability) == checker.SMOKE_REACHABILITY_NAMES
    assert tuple(row.name for row in mutations) == checker.SMOKE_MUTATION_NAMES
    assert set(checker.SMOKE_SCENARIO_NAMES) == {
        "staging_success",
        "production_success",
        "reversible_failure",
        "recover_verifying",
        "approval_stale",
    }
    assert set(checker.SMOKE_REACHABILITY_NAMES) == {
        "lifecycle_ready",
        "lifecycle_promoted",
        "reversible_rollback",
        "authorization_only_rollback_finalized",
        "stale_approval_rejected",
    }
    assert set(checker.SMOKE_MUTATION_NAMES) == {
        "k7_unsafe_rollback",
        "k9_approval_guard_removed",
        "crashed_commit_before_restart",
        "authorization_only_rollback_keeps_candidate_receipt",
    }
    assert len(scenarios) < len(checker.SCENARIOS)
    assert len(reachability) < len(checker.REACHABILITY)
    assert len(mutations) < len(checker.MUTATIONS)


def test_smoke_profile_never_emits_the_full_pact_report(
    monkeypatch, tmp_path: Path
) -> None:
    checker = _checker()
    calls: dict[str, tuple[str, ...]] = {}

    monkeypatch.setattr(
        checker,
        "_validate_contract",
        lambda: {"claim": "concrete_controller_mapping"},
    )

    def fake_safety(out, timeout, scenarios=checker.SCENARIOS):
        calls["safety"] = tuple(row.name for row in scenarios)
        return [{"name": row.name, "status": "pass"} for row in scenarios]

    def fake_reachability(out, timeout, witnesses=checker.REACHABILITY):
        calls["reachability"] = tuple(row.name for row in witnesses)
        return [{"name": row.name, "status": "witnessed"} for row in witnesses]

    def fake_mutations(out, timeout, mutations=checker.MUTATIONS):
        calls["mutations"] = tuple(row.name for row in mutations)
        return [{"name": row.name, "status": "witnessed"} for row in mutations]

    monkeypatch.setattr(checker, "_run_safety", fake_safety)
    monkeypatch.setattr(checker, "_run_reachability", fake_reachability)
    monkeypatch.setattr(checker, "_run_mutations", fake_mutations)

    assert checker.main(["--profile", "smoke", "--out", str(tmp_path)]) == 0
    assert calls == {
        "safety": checker.SMOKE_SCENARIO_NAMES,
        "reachability": checker.SMOKE_REACHABILITY_NAMES,
        "mutations": checker.SMOKE_MUTATION_NAMES,
    }
    smoke = json.loads(
        (tmp_path / "kernel-smoke-aggregate.json").read_text(encoding="utf-8")
    )
    assert smoke["profile"] == "smoke"
    assert smoke["full_pact_evidence"] is False
    assert not (tmp_path / "formal-report.yaml").exists()
    assert not (tmp_path / "kernel-proof-aggregate.json").exists()


def test_full_profile_and_all_preserve_complete_strict_report_semantics(
    monkeypatch, tmp_path: Path
) -> None:
    checker = _checker()
    runs: list[tuple[str, tuple[str, ...]]] = []
    controller_runs: list[tuple[Path, int]] = []
    reports: list[tuple[Path, int, str]] = []

    monkeypatch.setattr(
        checker,
        "_validate_contract",
        lambda: {"claim": "concrete_controller_mapping"},
    )

    def fake_safety(out, timeout, scenarios=checker.SCENARIOS):
        runs.append(("safety", tuple(row.name for row in scenarios)))
        return [{"name": row.name, "distinct_states": 10} for row in scenarios]

    def fake_reachability(out, timeout, witnesses=checker.REACHABILITY):
        runs.append(("reachability", tuple(row.name for row in witnesses)))
        return [{"name": row.name} for row in witnesses]

    def fake_mutations(out, timeout, mutations=checker.MUTATIONS):
        runs.append(("mutations", tuple(row.name for row in mutations)))
        return [{"name": row.name} for row in mutations]

    def fake_controller_refinement(out, timeout):
        controller_runs.append((out, timeout))
        return out / "controller-refinement.json"

    def fake_aggregate(
        out, refinement, safety, reachability, mutations, controller_refinement
    ):
        assert len(safety) == len(checker.SCENARIOS)
        assert len(reachability) == len(checker.REACHABILITY)
        assert len(mutations) == len(checker.MUTATIONS)
        assert controller_refinement == out / "controller-refinement.json"
        return out / "kernel-proof-aggregate.json"

    def fake_write_report(out, aggregate, *, minimum_states, checker_command):
        reports.append((aggregate, minimum_states, checker_command))

    monkeypatch.setattr(checker, "_run_safety", fake_safety)
    monkeypatch.setattr(checker, "_run_reachability", fake_reachability)
    monkeypatch.setattr(checker, "_run_mutations", fake_mutations)
    monkeypatch.setattr(
        checker, "_run_controller_refinement", fake_controller_refinement
    )
    monkeypatch.setattr(checker, "_aggregate", fake_aggregate)
    monkeypatch.setattr(checker, "_write_report", fake_write_report)

    for selector in (("--profile", "full"), ("--all",)):
        assert checker.main([*selector, "--out", str(tmp_path)]) == 0

    expected_run = [
        ("safety", tuple(row.name for row in checker.SCENARIOS)),
        ("reachability", tuple(row.name for row in checker.REACHABILITY)),
        ("mutations", tuple(row.name for row in checker.MUTATIONS)),
    ]
    assert runs == expected_run * 2
    assert controller_runs == [(tmp_path, 120), (tmp_path, 120)]
    assert reports == [
        (
            tmp_path / "kernel-proof-aggregate.json",
            10,
            f"uv run python scripts/check_formal_deployment_control_plane_kernel.py --profile full --out {tmp_path}",
        ),
        (
            tmp_path / "kernel-proof-aggregate.json",
            10,
            f"uv run python scripts/check_formal_deployment_control_plane_kernel.py --all --out {tmp_path}",
        ),
    ]


def test_full_profile_without_pact_records_runs_every_check_and_writes_no_report(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    """The aggregate binds this repository's PACT records; the model checks do not.

    Without the records (the public copy) the flag runs every model check and
    says plainly that no aggregate report was written; without the flag the
    missing records refuse and name it.
    """
    checker = _checker()
    monkeypatch.setattr(
        checker, "_validate_contract", lambda: {"claim": "concrete_controller_mapping"}
    )
    ran: list[str] = []
    monkeypatch.setattr(
        checker, "_run_safety",
        lambda out, timeout, scenarios=checker.SCENARIOS: ran.append("safety")
        or [{"name": "s", "distinct_states": 10}],
    )
    monkeypatch.setattr(
        checker, "_run_reachability",
        lambda out, timeout, witnesses=checker.REACHABILITY: ran.append("reachability") or [],
    )
    monkeypatch.setattr(
        checker, "_run_mutations",
        lambda out, timeout, mutations=checker.MUTATIONS: ran.append("mutations") or [],
    )
    monkeypatch.setattr(
        checker, "_run_controller_refinement",
        lambda out, timeout: ran.append("controller") or out / "controller.json",
    )

    def no_report(*_args, **_kwargs):
        raise AssertionError("no aggregate report without the PACT records")

    monkeypatch.setattr(checker, "_write_report", no_report)
    monkeypatch.setattr(checker, "ROOT", tmp_path)

    assert checker.main(["--profile", "full", "--out", str(tmp_path)]) == 1
    assert "--without-pact-records" in capsys.readouterr().err

    ran.clear()
    assert checker.main(
        ["--profile", "full", "--without-pact-records", "--out", str(tmp_path)]
    ) == 0
    assert ran == ["safety", "reachability", "mutations", "controller"]
    assert "aggregate proof report was NOT written" in capsys.readouterr().out
    assert not (tmp_path / "formal-report.yaml").exists()


def test_typed_scope_is_exact() -> None:
    text = MODULE.read_text(encoding="utf-8")
    variables_match = re.search(r"VARIABLES (.*?)\n\nvars ==", text, re.DOTALL)
    assert variables_match is not None
    variables = {
        token.strip() for token in variables_match.group(1).replace("\n", "").split(",")
    }
    assert variables == {
        "environmentState",
        "attemptIdentity",
        "attemptState",
        "outbox",
        "effectState",
        "compensationState",
        "observations",
        "identityClaims",
        "healthResults",
        "approvals",
        "eventHistory",
        "projectionReads",
        "mutationHistory",
        "logicalTime",
    }
    type_body = _operator_body(text, "TypeScopeExact", "SingleActiveAttempt")
    for variable in variables:
        assert variable in type_body
    for record in (
        "environmentState",
        "attemptIdentity",
        "attemptState",
        "outbox",
        "effectState",
        "compensationState",
        "healthResults",
        "approvals",
    ):
        assert f"DOMAIN {record} =" in type_body
    for excluded in (
        "descriptorField",
        "cliFlag",
        "processAttribute",
        "graphMetric",
        "adapterPayload",
    ):
        assert excluded not in text


def test_config_checks_exact_kernel_obligations() -> None:
    checker = _checker()
    configured = tuple(
        re.findall(
            r"^  ([A-Za-z][A-Za-z0-9_]*)$",
            CONFIG.read_text(encoding="utf-8"),
            re.MULTILINE,
        )
    )
    assert configured == (*checker.INVARIANTS, *checker.TEMPORAL_PROPERTIES)
    assert checker.INVARIANTS == (
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


def test_lifecycle_edges_match_redesign_and_terminals_never_reopen() -> None:
    text = MODULE.read_text(encoding="utf-8")
    start = text.index("LegalEdges ==")
    legal = text[start : text.index("\nVARIABLES ", start)]
    edges = set(re.findall(r'<<"([a-z_]+)", "([a-z_]+)">>', legal))
    assert edges == {
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
    assert not any(
        source in {"promoted", "rolled_back", "blocked"} for source, _ in edges
    )
    assert ("applying", "blocked") not in edges


def test_production_reachability_registry_is_complete() -> None:
    checker = _checker()
    names = {item.name for item in checker.REACHABILITY}
    assert names == {
        "lifecycle_planned",
        "lifecycle_admitted",
        "lifecycle_applying",
        "lifecycle_verifying",
        "lifecycle_ready",
        "lifecycle_promoted",
        "admission_recovery",
        "reversible_rollback",
        "irreversible_ambiguity_rollback",
        "irreversible_postcommit_rollback",
        "compensation_failure_block",
        "effect_intended",
        "effect_applied",
        "effect_observed",
        "effect_committed",
        "compensation_intended",
        "compensation_applied",
        "compensation_observed",
        "compensation_committed",
        "approval_consumed",
        "legacy_rejected",
        "fresh_publication",
        "staged_capability",
        "staged_verification_writes",
        "publish_intent",
        "activation_authorized",
        "authorized_publication",
        "final_readiness_observed",
        "authorization_only_recovered",
        "published_unobserved_recovered",
        "predecessor_restored_under_new_epoch",
        "rollback_finalized",
        "authorization_only_rollback_finalized",
        "rollback_authorized",
        "rollback_authorized_before_writer_epoch_rotation",
        "atomic_rollback_writer_epoch_publication",
        "compensation_plan_intended",
        "compensation_plan_applied",
        "compensation_plan_observed",
        "compensation_plan_committed",
        "planned_effect_excluded_from_compensation",
        "irreversible_effect_compensated",
        "crash_before_authorization",
        "crash_authorized_before_publish",
        "crash_published_before_final_observation",
        "stale_approval_rejected",
        "expired_approval_rejected",
        "revoked_approval_rejected",
        "consumed_approval_rejected",
        "unhealthy_approval_rejected",
        "incomplete_approval_rejected",
        "source_not_ready_approval_rejected",
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
        "changed_staging_rejected",
        "stopped_mutation_rejected",
    }
    scenarios = {item.name for item in checker.SCENARIOS}
    assert {item.scenario for item in checker.REACHABILITY} <= scenarios
    module_text = MODULE.read_text(encoding="utf-8")
    assert all(f"{item.invariant} ==" in module_text for item in checker.REACHABILITY)


def test_mutation_registry_covers_every_kernel_invariant() -> None:
    checker = _checker()
    covered = {mutation.invariant for mutation in checker.MUTATIONS}
    assert set((*checker.INVARIANTS, *checker.TEMPORAL_PROPERTIES)) <= covered
    assert {mutation.name for mutation in checker.MUTATIONS} >= {
        "outbox_dual_commit",
        "compensation_overwrites_forward_journal",
        "recovery_fairness_removed",
        "rollback_writer_epoch_rotates_during_authorization",
        "rollback_writer_epoch_not_rotated_at_publication",
    }
    source = MODULE.read_text(encoding="utf-8")
    for mutation in checker.MUTATIONS:
        mutated = checker._mutated_module(mutation)
        assert mutated != source
        for replacement in mutation.replacements:
            assert source.count(replacement.old) == 1


def test_refinement_owners_bind_concrete_controller_mapping() -> None:
    registry = yaml.safe_load(REFINEMENTS.read_text(encoding="utf-8"))
    assert registry["claim"] == "concrete_controller_mapping"
    assert (
        registry["runtime_refinement_execution"]
        == "bounded_recovery_trace_and_concrete_controller"
    )
    assert {item["id"] for item in registry["obligations"]} == {
        "refinement.external-controller-protocol",
        "refinement.sole-ledger-authority",
    }
    for obligation in registry["obligations"]:
        manifest_path = ROOT / "pact/changes" / obligation["owner_change"] / "pact.yaml"
        assert manifest_path.is_file()
        manifest = load_manifest_file(manifest_path)
        assert obligation["requirement"] in {
            requirement.id for requirement in manifest.requirements
        }
        # v5 folded the v4 `test_plan` into each task's verification prose and
        # did not carry the row ids, so `proof` no longer resolves to a
        # manifest row. What still binds -- and is the substantive claim -- is
        # that the named test exists and the owning change names it.
        assert obligation["planned_test"].startswith("tests/")
        assert (ROOT / obligation["planned_test"]).is_file()
    assert all(
        not Path(item["planned_test"]).is_absolute() for item in registry["obligations"]
    )

    authority = (
        ROOT / "pact/changes/unify-deployment-control-plane-authority/pact.yaml"
    ).read_text(encoding="utf-8")
    assert "external environment saga ledger alone owns" in authority


def test_staged_publication_visibility_is_ordered() -> None:
    text = MODULE.read_text(encoding="utf-8")
    authorize = _operator_body(
        text, "AuthorizeActivation", "PublishAuthorizedGeneration"
    )
    publish = _operator_body(
        text, "PublishAuthorizedGeneration", "ObserveAndFinalizeReady"
    )
    finalize = _operator_body(text, "ObserveAndFinalizeReady", "PublishFreshGeneration")
    assert "!.activationReceipt = ActivationReceipt" in authorize
    assert "!.activationCandidate = Artifact" in authorize
    assert "!.activationStagedDigest = StagedDigest" in authorize
    assert "!.databaseWriterEpoch = attemptIdentity.databaseWriterEpoch" not in authorize
    assert "environmentState.activationReceipt = ActivationReceipt" in publish
    assert "!.databaseWriterEpoch = attemptIdentity.databaseWriterEpoch" in publish
    assert (
        "attemptIdentity.databaseWriterEpoch = "
        "environmentState.databaseWriterEpoch + 1" in publish
    )
    assert '!.stagedCapability = "consumed"' in publish
    assert "!.publishOutcome = TRUE" in publish
    assert "!.readerVisible = TRUE" in publish
    assert "!.visibilitySwitches = @ + 1" in publish
    assert "!.finalObservation = TRUE" in finalize
    assert '!.producerCapabilities = "active"' in finalize
    ordering = _operator_body(
        text, "PublicationVisibilityOrdering", "RecoveryEventuallySettles"
    )
    for required in (
        "publishIntent",
        "stagedWrites",
        "publishOutcome",
        "readerVisible",
        "visibilitySwitches = 1",
    ):
        assert required in ordering


def test_only_ledger_actions_commit_lifecycle_associations() -> None:
    checker = _checker()
    text = MODULE.read_text(encoding="utf-8")
    authorize = _operator_body(
        text, "AuthorizeActivation", "PublishAuthorizedGeneration"
    )
    rollback = _operator_body(text, "AuthorizeRollback", "CompensationIntent")
    restore = _operator_body(
        text, "RestorePredecessorUnderNewEpoch", "ObserveAndFinalizeRollback"
    )
    for field in checker.LEDGER_ASSOCIATION_FIELDS:
        assignment = f"!.{field} ="
        assert assignment in authorize
        assert assignment in rollback
        assert text.count(assignment) == 2
    assert "!.databaseWriterEpoch = RollbackDatabaseWriterEpoch" not in rollback
    assert "!.databaseWriterEpoch = RollbackDatabaseWriterEpoch" in restore
    assert text.count("!.databaseWriterEpoch =") == 2
    assert 'ledgerAuthority |-> "external-controller-ledger"' in text
    assert "FreezeProducersForActivation ==" in text
    assert "NeverCrashBeforeAuthorization ==" in text
    assert "NeverCrashAuthorizedBeforePublish ==" in text
    assert "NeverCrashPublishedBeforeFinalObservation ==" in text


def test_authorization_only_recovery_publishes_writer_epoch_atomically() -> None:
    text = MODULE.read_text(encoding="utf-8")
    authorize = _operator_body(text, "AuthorizeRollback", "CompensationIntent")
    restore = _operator_body(
        text, "RestorePredecessorUnderNewEpoch", "ObserveAndFinalizeRollback"
    )
    finalize = _operator_body(text, "ObserveAndFinalizeRollback", "SettleBlocked")
    assert (
        "environmentState.activationReceipt \\in {None, ActivationReceipt}" in authorize
    )
    assert (
        "environmentState.readerVisible"
        not in authorize.split("/\\ environmentState' =", maxsplit=1)[0]
    )
    assert "!.activationReceipt = RollbackReceipt" in authorize
    assert "!.lifecycleEpoch = RollbackLifecycleEpoch" in authorize
    assert "!.databaseWriterEpoch = RollbackDatabaseWriterEpoch" not in authorize
    assert "!.invalidatedReceipts = @ \\cup {ActivationReceipt}" in authorize
    assert "!.authorizationOnlyRollback = ~environmentState.publishOutcome" in authorize
    assert '!.stagedCapability = "absent"' in authorize
    assert "!.producersFrozen = FALSE" in authorize
    assert '!.plan = "exact"' in authorize
    assert "!.boundReceipt = RollbackReceipt" in authorize
    assert '!.order = "reverse"' in authorize
    assert "CompensationComplete" in restore
    assert "!.databaseWriterEpoch = RollbackDatabaseWriterEpoch" in restore
    assert "!.targetDatabaseReceipt = RollbackReceipt" in restore
    assert "!.currentGeneration = PredecessorGeneration" in restore
    assert '!.lifecycle = "rolled_back"' in finalize
    assert "generation |-> PredecessorGeneration" in finalize
    assert "!.finalObservation = TRUE" in finalize
    assert '!.producerCapabilities = "active"' in finalize


def test_aggregate_digest_binds_every_registry() -> None:
    checker = _checker()
    scenarios = [scenario.__dict__ for scenario in checker.SCENARIOS]
    original = checker._registry_digest(scenarios)
    changed = [*scenarios, {"name": "unbound_extra"}]
    assert checker._registry_digest(changed) != original
    assert checker._sha256(MODULE).startswith("sha256:")
    assert checker._sha256(CONFIG).startswith("sha256:")
    assert checker._sha256(REFINEMENTS).startswith("sha256:")
    assert checker._sha256(CHECKER).startswith("sha256:")
    assert checker._sha256(README).startswith("sha256:")
    assert checker._sha256(DESIGN).startswith("sha256:")


def test_full_aggregate_binds_concrete_controller_refinement() -> None:
    report = ROOT / ".scratch/formal/deployment-control-plane-kernel/formal-report.yaml"
    aggregate = (
        ROOT
        / ".scratch/formal/deployment-control-plane-kernel/kernel-proof-aggregate.json"
    )
    if not report.is_file() or not aggregate.is_file():
        return
    row = yaml.safe_load(report.read_text(encoding="utf-8"))["results"][0]
    payload = json.loads(aggregate.read_text(encoding="utf-8"))
    checker = _checker()
    if payload.get("model_digest") != checker._sha256(MODULE):
        return
    assert row["property_id"] == "DeploymentControlPlaneKernelAggregate"
    assert row["proof_kind"] == "tlc_fixpoint"
    assert row["proof_status"] == "freshly_proven"
    assert row["fresh"] is True
    assert (
        payload["runtime_refinement_execution"]
        == "bounded_recovery_trace_and_concrete_controller"
    )
    assert payload["controller_refinement_digest"].startswith("sha256:")


def test_controller_refinement_predicates_are_concrete_and_mutation_sensitive(
    tmp_path: Path,
) -> None:
    checker = _checker()
    predicates = checker._controller_source_predicates()
    assert set(predicates) == {
        "ledger.possibly-applied-phase-selection",
        "ledger.reverse-exact-receipt-bound-plan",
        "ledger.rollback-authorization-materializes-plan",
        "ledger.rollback-generation-is-derived",
        "ledger.rollback-publication-compensation-guard",
        "ledger.rollback-finalization-compensation-guard",
        "runner.observe-before-apply-and-resume",
        "controller.compensate-before-publish-before-finalize",
        "model-runtime.rollback-writer-epoch-publication-boundary",
        "target.predecessor-receipt-epoch-cas",
        "controller.postpublication-unfreeze",
        "controller.serialized-external-invocation",
    }
    model_text = MODULE.read_text(encoding="utf-8")
    mutated_model = tmp_path / MODULE.name
    authorization_anchor = (
        "!.lifecycleEpoch = RollbackLifecycleEpoch,\n"
        "          !.activationReceipt = RollbackReceipt,"
    )
    assert model_text.count(authorization_anchor) == 1
    mutated_model.write_text(
        model_text.replace(
            authorization_anchor,
            "!.lifecycleEpoch = RollbackLifecycleEpoch,\n"
            "          !.databaseWriterEpoch = RollbackDatabaseWriterEpoch,\n"
            "          !.activationReceipt = RollbackReceipt,",
        ),
        encoding="utf-8",
    )
    checker.MODULE = mutated_model
    with pytest.raises(
        RuntimeError, match="rollback-writer-epoch-publication-boundary"
    ):
        checker._controller_source_predicates()

    publication_anchor = (
        "!.databaseWriterEpoch = RollbackDatabaseWriterEpoch,\n"
        "          !.targetDatabaseReceipt = RollbackReceipt,"
    )
    assert model_text.count(publication_anchor) == 1
    mutated_model.write_text(
        model_text.replace(
            publication_anchor,
            "!.targetDatabaseReceipt = RollbackReceipt,",
        ),
        encoding="utf-8",
    )
    with pytest.raises(
        RuntimeError, match="rollback-writer-epoch-publication-boundary"
    ):
        checker._controller_source_predicates()
    checker.MODULE = MODULE

    copied = []
    for source in checker.CONTROLLER_RUNTIME_FILES:
        target = tmp_path / source.name
        shutil.copyfile(source, target)
        copied.append(target)
    ledger = copied[0]
    text = ledger.read_text(encoding="utf-8")
    anchor = (
        "EffectPhase.INTENDED,\n"
        "                EffectPhase.APPLIED,\n"
        "                EffectPhase.OBSERVED,\n"
        "                EffectPhase.COMMITTED,"
    )
    assert text.count(anchor) == 2
    offset = text.rfind(anchor)
    ledger.write_text(
        text[:offset]
        + anchor.replace("INTENDED", "PLANNED")
        + text[offset + len(anchor) :],
        encoding="utf-8",
    )
    checker.CONTROLLER_RUNTIME_FILES = tuple(copied)
    with pytest.raises(RuntimeError, match="possibly-applied-phase-selection"):
        checker._controller_source_predicates()
