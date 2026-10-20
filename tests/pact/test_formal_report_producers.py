"""REPORT-SCHEMA tests for the formal report producers.

These tests monkeypatch ``subprocess.run`` to feed the checkers canned TLC
output. They validate the SHAPE of the emitted formal-report.yaml rows
(status/fresh/proof_status/digests/refinement bindings) -- they do NOT run TLC
and therefore do NOT prove any model property. Real model proof comes from the
gating aggregate runner (scripts/run_formal_suite.py) executing real TLC in CI.
"""

from __future__ import annotations

import importlib
import importlib.util
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import yaml

from okg.substrate import formal_tlc
from tests.mirror_scope import is_public_copy, private_path

FORMAL_REPORT_SCHEMA_TEST = True


REPO_ROOT = Path(__file__).resolve().parents[2]
COVERED_FORMAL_REPORT_PRODUCERS = {
    "check_formal_admission.py",
    "check_formal_agent_operator_ergonomics.py",
    "check_formal_althea_optimizer_safety.py",
    "check_formal_attribute_state.py",
    "check_formal_chronos_read_pin.py",
    "check_formal_chronos_retention_compaction.py",
    "check_formal_coordination_protocols.py",
    "check_formal_dbos_commit.py",
    "check_formal_dbos_epoch_supersession.py",
    "check_formal_delta_frontier.py",
    "check_formal_delta_projection.py",
    "check_formal_deployment_apply.py",
    "check_formal_deployment_authoring.py",
    "check_formal_deployment_backup_policy.py",
    "check_formal_deployment_control_plane_kernel.py",
    "check_formal_deployment_lifecycle.py",
    "check_formal_deployment_restore_point.py",
    "check_formal_deployment_promotion.py",
    "check_formal_derivation_parity.py",
    "check_formal_dogfood_cutover.py",
    "check_formal_enricher_backfill.py",
    "check_formal_enricher_backlog_drain.py",
    "check_formal_entity_resolution.py",
    "check_formal_external_action_review.py",
    "check_formal_fact_batch_recovery.py",
    "check_formal_fleet_capacity_reservation.py",
    "check_formal_gtir_commit.py",
    "check_formal_merge_back_safety.py",
    "check_formal_native_coordination.py",
    "check_formal_nomos.py",
    "check_formal_okg_ci_recovery_lifecycle.py",
    "check_formal_pact_agent_lifecycle.py",
    "check_formal_pact_formal_evidence.py",
    "check_formal_pact_proof_integrity.py",
    "check_formal_pact_mutation.py",
    "check_formal_pact_projection.py",
    "check_formal_pact_v4_decision_ledger.py",
    "check_formal_poros.py",
    "check_formal_qualification_gate.py",
    "check_formal_reuse_pass.py",
    "check_formal_runtime_cutover.py",
    "check_formal_scholarly_source_shapes.py",
    "check_formal_seal_writes.py",
    "check_formal_source_delta_contract.py",
    "check_formal_source_execution_lanes.py",
    "check_formal_source_retirement_liveness.py",
    "check_formal_stable_reemission.py",
    "check_formal_source_sync_convergence.py",
    "check_formal_source_sync_observability.py",
    "check_formal_transform_gate_dirty_scope.py",
    "check_formal_transform_gate_node_subtype.py",
    "check_formal_typed_outcome.py",
    "check_formal_typed_applicability.py",
    "check_formal_watermark_retry.py",
    "check_formal_watermark_quarantine.py",
    "check_formal_worktree_branch_safety.py",
    "check_formal_workflow_authority.py",
    "check_formal_writer_gate_narrowing.py",
}
NON_PACT_FORMAL_HELPERS: set[str] = {
    "check_formal_console.py",
}


def _load_script(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load_suite():
    path = REPO_ROOT / "scripts" / "run_formal_suite.py"
    spec = importlib.util.spec_from_file_location("run_formal_suite", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _fake_tlc(module, monkeypatch) -> None:
    monkeypatch.setattr(
        formal_tlc,
        "find_tla2tools_jar",
        lambda _repo_root: REPO_ROOT / ".tools" / "tla2tools.jar",
    )
    monkeypatch.setattr(formal_tlc.shutil, "which", lambda _name: "/usr/bin/java")
    monkeypatch.setattr(
        formal_tlc.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout="Model checking completed. No error has been found.\n"
            "200000000 states generated, 99000000 distinct states found, 0 states left on queue.\n",
            stderr="",
        ),
    )


def _fake_tlc_counterexample(
    module,
    monkeypatch,
    *,
    invariant: str = "BrokenReplayNeverHappens",
) -> None:
    monkeypatch.setattr(
        formal_tlc,
        "find_tla2tools_jar",
        lambda _repo_root: REPO_ROOT / ".tools" / "tla2tools.jar",
    )
    monkeypatch.setattr(formal_tlc.shutil, "which", lambda _name: "/usr/bin/java")
    monkeypatch.setattr(
        formal_tlc.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=1,
            stdout=(f"Error: Invariant {invariant} is violated.\n"),
            stderr="",
        ),
    )


def _fake_agent_operator_tlc(module, monkeypatch) -> None:
    _fake_tlc(module, monkeypatch)
    witness_invariants = {
        "AgentOperatorUnsafeApproval.cfg": "ApprovalIsBounded",
        "AgentOperatorDuplicatePoll.cfg": "ActiveWatchSuppressesOrdinaryPoll",
        "AgentOperatorBackstopSuppression.cfg": "BackstopRetainsAuthority",
        "AgentOperatorStartedReap.cfg": "StartedWorkflowIsNotReaped",
    }

    def run(cmd, **_kwargs):
        command_text = " ".join(str(part) for part in cmd)
        for config, invariant in witness_invariants.items():
            if config in command_text:
                return SimpleNamespace(
                    returncode=1,
                    stdout=f"Error: Invariant {invariant} is violated.\n",
                    stderr="",
                )
        return SimpleNamespace(
            returncode=0,
            stdout=(
                "Model checking completed. No error has been found.\n"
                "40 states generated, 20 distinct states found, "
                "0 states left on queue.\n"
            ),
            stderr="",
        )

    monkeypatch.setattr(formal_tlc.subprocess, "run", run)


def _fake_nomos_counterexamples(module, monkeypatch) -> None:
    monkeypatch.setattr(
        formal_tlc,
        "find_tla2tools_jar",
        lambda _repo_root: REPO_ROOT / ".tools" / "tla2tools.jar",
    )
    monkeypatch.setattr(formal_tlc.shutil, "which", lambda _name: "/usr/bin/java")
    expected = {
        "NomosMissingPolicyCounterexample.cfg": "BrokenMissingPolicyCanAllow",
        "NomosWeakDeploymentPostureCounterexample.cfg": (
            "BrokenWeakDeploymentPostureCanAllow"
        ),
        "NomosUnscopedIdentityCounterexample.cfg": (
            "BrokenUnscopedIdentityClaimCanGrant"
        ),
        "NomosUnknownActionCounterexample.cfg": "BrokenUnknownActionCanAllow",
        "NomosPrivateExportCounterexample.cfg": (
            "BrokenPrivateExportWithoutScopedClaimCanPublish"
        ),
        "NomosSourceDeploymentMismatchCounterexample.cfg": (
            "BrokenSourceDeploymentMismatchCanAllow"
        ),
    }

    def run(cmd, **_kwargs):
        command_text = " ".join(str(part) for part in cmd)
        invariant = next(
            value for config, value in expected.items() if config in command_text
        )
        return SimpleNamespace(
            returncode=1,
            stdout=f"Error: Invariant {invariant} is violated.\n",
            stderr="",
        )

    monkeypatch.setattr(formal_tlc.subprocess, "run", run)


def _fake_source_sync_tlc(module, monkeypatch) -> None:
    monkeypatch.setattr(
        formal_tlc,
        "find_tla2tools_jar",
        lambda _repo_root: REPO_ROOT / ".tools" / "tla2tools.jar",
    )
    monkeypatch.setattr(formal_tlc.shutil, "which", lambda _name: "/usr/bin/java")

    def run(cmd, **_kwargs):
        command_text = " ".join(str(part) for part in cmd)
        if "ReadinessConvergenceReadyCounterexample.cfg" in command_text:
            return SimpleNamespace(
                returncode=1,
                stdout="Error: Invariant ReadyWitnessNeverOccurs is violated.\n",
                stderr="",
            )
        if "ReadinessConvergenceStuckCounterexample.cfg" in command_text:
            return SimpleNamespace(
                returncode=1,
                stdout="Error: Invariant StuckWitnessNeverOccurs is violated.\n",
                stderr="",
            )
        if "ReadinessConvergenceMcpBlockedCounterexample.cfg" in command_text:
            return SimpleNamespace(
                returncode=1,
                stdout="Error: Invariant McpBlockedWitnessNeverOccurs is violated.\n",
                stderr="",
            )
        return SimpleNamespace(
            returncode=0,
            stdout="Model checking completed. No error has been found.\n"
            "200000000 states generated, 99000000 distinct states found, 0 states left on queue.\n",
            stderr="",
        )

    monkeypatch.setattr(formal_tlc.subprocess, "run", run)


def _fake_deployment_apply_tlc(module, monkeypatch) -> None:
    monkeypatch.setattr(
        formal_tlc,
        "find_tla2tools_jar",
        lambda _repo_root: REPO_ROOT / ".tools" / "tla2tools.jar",
    )
    monkeypatch.setattr(formal_tlc.shutil, "which", lambda _name: "/usr/bin/java")
    expected = {
        "DeploymentApplyStalePlanCounterexample.cfg": "NoStaleMutation",
        "DeploymentApplySkippedStageCounterexample.cfg": "OrderedStages",
        "DeploymentApplyDuplicateActionCounterexample.cfg": (
            "NoDuplicateSourceRegistration"
        ),
        "DeploymentApplyFalseWorkerAckCounterexample.cfg": "ExactWorkerAck",
    }

    def run(cmd, **_kwargs):
        command_text = " ".join(str(part) for part in cmd)
        for config, invariant in expected.items():
            if config in command_text:
                return SimpleNamespace(
                    returncode=1,
                    stdout=f"Error: Invariant {invariant} is violated.\n",
                    stderr="",
                )
        return SimpleNamespace(
            returncode=0,
            stdout="Model checking completed. No error has been found.\n"
            "40 states generated, 20 distinct states found, 0 states left on queue.\n",
            stderr="",
        )

    monkeypatch.setattr(formal_tlc.subprocess, "run", run)


def _fake_stable_reemission_tlc(module, monkeypatch) -> None:
    monkeypatch.setattr(
        formal_tlc,
        "find_tla2tools_jar",
        lambda _repo_root: REPO_ROOT / ".tools" / "tla2tools.jar",
    )
    monkeypatch.setattr(formal_tlc.shutil, "which", lambda _name: "/usr/bin/java")
    witness_invariants = {
        "StableReemissionNoBackendGuardWitness.cfg": (
            "DuplicateStableWakeupsDoNotVersion"
        ),
        "StableReemissionWorktreeMembershipWitness.cfg": "StableSourceEmission",
        "StableReemissionRunRevisionWitness.cfg": "StableSourceEmission",
        "StableReemissionClobberWitness.cfg": "SubstitutePreservesAuthority",
    }

    def run(cmd, **_kwargs):
        command_text = " ".join(str(part) for part in cmd)
        for config, invariant in witness_invariants.items():
            if config in command_text:
                return SimpleNamespace(
                    returncode=1,
                    stdout=f"Error: Invariant {invariant} is violated.\n",
                    stderr="",
                )
        return SimpleNamespace(
            returncode=0,
            stdout="Model checking completed. No error has been found.\n"
            "200000000 states generated, 99000000 distinct states found, 0 states left on queue.\n",
            stderr="",
        )

    monkeypatch.setattr(formal_tlc.subprocess, "run", run)


def _fake_derivation_parity_tlc(module, monkeypatch) -> None:
    monkeypatch.setattr(
        formal_tlc,
        "find_tla2tools_jar",
        lambda _repo_root: REPO_ROOT / ".tools" / "tla2tools.jar",
    )
    monkeypatch.setattr(formal_tlc.shutil, "which", lambda _name: "/usr/bin/java")

    def run(cmd, **_kwargs):
        command_text = " ".join(str(part) for part in cmd)
        if "DerivationParityDrift.cfg" in command_text:
            return SimpleNamespace(
                returncode=1,
                stdout=(
                    "Error: Invariant IncrementalEqualsBatchForLinearDerivation "
                    "is violated.\n"
                ),
                stderr="",
            )
        return SimpleNamespace(
            returncode=0,
            stdout="Model checking completed. No error has been found.\n"
            "200000000 states generated, 99000000 distinct states found, 0 states left on queue.\n",
            stderr="",
        )

    monkeypatch.setattr(formal_tlc.subprocess, "run", run)


def _assert_strict_rows(report_path: Path) -> list[dict]:
    report = yaml.safe_load(report_path.read_text(encoding="utf-8"))
    rows = report["results"]
    assert rows
    for row in rows:
        assert row["kind"] == "formal_check"
        assert row["status"] == "pass"
        assert row["fresh"] is True
        assert row["checker_command"]
        assert row["command"]
        assert row["refinement_tests"]
        assert row["module_digest"].startswith("sha256:")
        assert row["config_digest"].startswith("sha256:")
        artifact = Path(row["artifact"])
        assert artifact.is_file()
        artifact.relative_to(report_path.parent)
    return rows


def _assert_payload_witnesses(
    rows: list[dict],
    expected_property_ids: set[str],
    expected_witnesses: dict[str, bool],
) -> dict:
    payload = json.loads(Path(rows[0]["artifact"]).read_text(encoding="utf-8"))
    assert payload["ok"] is True
    assert set(payload["property_ids"]) == expected_property_ids
    # The passing checker echoes the exact named positive/negative witness map:
    # every expected key present, every bool matching the hand-written outcome.
    assert payload["witnesses"] == expected_witnesses
    for key, want in expected_witnesses.items():
        assert type(payload["witnesses"][key]) is bool
        assert payload["witnesses"][key] is want
    return payload


def _degenerate_witness_cases(module) -> tuple[dict[str, bool], ...]:
    """Witness maps a passing checker must reject, one per failure mode:

    empty, a flipped outcome, a missing key, and an unexpected extra key.
    """
    correct = dict(module.EXPECTED_WITNESSES)
    first_key = next(iter(correct))
    flipped = dict(correct)
    flipped[first_key] = not flipped[first_key]
    missing = dict(correct)
    missing.pop(first_key)
    extra = dict(correct)
    extra["bogus.unexpected_witness"] = True
    return ({}, flipped, missing, extra)


def test_formal_report_producer_inventory_is_explicit() -> None:
    scripts = {path.name for path in (REPO_ROOT / "scripts").glob("check_formal_*.py")}
    classified = COVERED_FORMAL_REPORT_PRODUCERS | NON_PACT_FORMAL_HELPERS

    assert scripts <= classified
    # The okg-ci recovery checker and the two native-coordination checkers
    # (their models refine private deployment code) are this repository's own
    # and are not in the public copy; there, every other covered producer must
    # still be present.
    expected = COVERED_FORMAL_REPORT_PRODUCERS
    if is_public_copy():
        expected = expected - {
            "check_formal_okg_ci_recovery_lifecycle.py",
            "check_formal_coordination_protocols.py",
            "check_formal_native_coordination.py",
        }
    assert expected <= scripts
    assert NON_PACT_FORMAL_HELPERS <= scripts
    assert "check_formal_console.py" not in COVERED_FORMAL_REPORT_PRODUCERS
    assert "check_formal_console.py" in NON_PACT_FORMAL_HELPERS


def test_pact_lifecycle_formal_report_sanitizes_discovered_parent_jar(
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_pact_agent_lifecycle.py"
    )
    monkeypatch.delenv("TLA2TOOLS_JAR", raising=False)
    command = (
        "java -cp /Users/Jane Doe/parent-worktree/.tools/tla2tools.jar "
        "tlc2.TLC -config PactAgentLifecycle.cfg"
    )

    portable = module._portable_value({"command": command})

    assert portable == {
        "command": ("java -cp $TLA2TOOLS_JAR tlc2.TLC -config PactAgentLifecycle.cfg")
    }


def test_worktree_checker_registers_named_counterexample_witnesses() -> None:
    suite = _load_suite()
    spec = next(
        checker
        for checker in suite.CHECKERS
        if checker.name == "check_formal_worktree_branch_safety.py"
    )
    assert len(spec.witnesses) == 7
    assert all(
        "--witness-id" in witness and "--witness-only" in witness
        for witness in spec.witnesses
    )


def test_only_witness_selection_runs_one_registered_witness(monkeypatch) -> None:
    suite = _load_suite()
    selection = suite.resolve_only_selector(
        "WorktreeBranchSafetyDefaultIsolationWitness.cfg"
    )
    calls = []

    def fake_invoke(spec, label, extra, out_sub, **kwargs):
        calls.append((spec.name, label, extra, out_sub))
        return suite.CheckerResult(label, kwargs["mode"], 0, 0.0)

    monkeypatch.setattr(suite, "_invoke", fake_invoke)
    results = suite.run_mode(
        "counterexamples",
        only=selection.checker_name,
        witness_index=selection.witness_index,
        timeout_override=1,
        include_archive=False,
    )
    assert len(results) == 1
    assert results[0].ok
    assert calls[0][2] == (
        "--witness-id",
        "DefaultIsolation",
        "--witness-only",
    )


def test_only_post_commit_witness_selects_its_own_config(monkeypatch) -> None:
    suite = _load_suite()
    selection = suite.resolve_only_selector(
        "WorktreeBranchSafetyPostCommitRecoveryWitness.cfg"
    )
    calls = []

    def fake_invoke(spec, label, extra, out_sub, **kwargs):
        calls.append((spec.name, label, extra, out_sub))
        return suite.CheckerResult(label, kwargs["mode"], 0, 0.0)

    monkeypatch.setattr(suite, "_invoke", fake_invoke)
    results = suite.run_mode(
        "counterexamples",
        only=selection.checker_name,
        witness_index=selection.witness_index,
        timeout_override=1,
        include_archive=False,
    )
    assert len(results) == 1
    assert results[0].ok
    assert calls[0][2] == (
        "--witness-id",
        "PostCommitRecovery",
        "--witness-only",
    )


def test_worktree_checker_accepts_named_witness_id(monkeypatch, tmp_path: Path) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_worktree_branch_safety.py"
    )
    calls = []

    def fake_witnesses(**kwargs):
        calls.append(kwargs)
        return 0

    monkeypatch.setattr(module, "_run_witnesses", fake_witnesses)
    # The owner-binding check reads this repository's PACT records; it has its
    # own tests below, so this one exercises only the witness selection.
    assert (
        module.main(
            [
                "--witness-id",
                "PostCommitRecovery",
                "--witness-only",
                "--without-pact-records",
                "--out",
                str(tmp_path),
            ]
        )
        == 0
    )
    assert calls[0]["witness_ids"] == ("PostCommitRecovery",)


def test_worktree_checker_owner_bindings_match_the_pact_records() -> None:
    private_path("pact/changes/worktree-branch-execution-context/pact.yaml")
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_worktree_branch_safety.py"
    )
    module._validate_bindings()


def test_worktree_checker_without_pact_records_refuses_and_names_the_flag(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_worktree_branch_safety.py"
    )
    # A checkout with the model but none of the owner change records, as in
    # the public copy.
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)

    def no_witnesses(**_kwargs):
        raise AssertionError("no TLC run may start after a refused binding check")

    monkeypatch.setattr(module, "_run_witnesses", no_witnesses)

    assert module.main(["--witness-only", "--out", str(tmp_path / "out")]) == 2
    error = capsys.readouterr().err
    assert "owner manifest is missing" in error
    assert "--without-pact-records" in error


def test_external_action_checker_without_pact_records_refuses_and_names_the_flag(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_external_action_review.py"
    )
    jar = tmp_path / "tla2tools.jar"
    jar.write_bytes(b"")
    # A checkout with the model but not the approved copy kept in the PACT
    # change record, as in the public copy.
    monkeypatch.setattr(module, "ROOT", tmp_path)
    monkeypatch.setattr(module, "TLA_TOOLS", jar)

    assert module.main(["--out", str(tmp_path / "out")]) == 1
    error = json.loads(capsys.readouterr().out)["error"]
    assert "approved PACT model directory is missing" in error
    assert "--without-pact-records" in error


def test_external_action_checker_flag_skips_only_the_pact_comparison(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_external_action_review.py"
    )
    jar = tmp_path / "tla2tools.jar"
    jar.write_bytes(b"")
    monkeypatch.setattr(module, "TLA_TOOLS", jar)

    def no_pact_records():
        raise AssertionError("the flag must not read the PACT model directory")

    monkeypatch.setattr(module, "_pact_model_dir", no_pact_records)
    # The next step, the binding checker, still runs; stop it there.
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *_a, **_k: SimpleNamespace(returncode=1, stdout="stopped here\n"),
    )

    assert (
        module.main(["--out", str(tmp_path / "out"), "--without-pact-records"]) == 1
    )
    assert "PACT binding checker failed" in json.loads(capsys.readouterr().out)["error"]


def test_only_checker_without_witness_fails_loudly() -> None:
    suite = _load_suite()
    results = suite.run_mode(
        "counterexamples",
        only="check_formal_gtir_commit.py",
        timeout_override=1,
        include_archive=False,
    )
    assert len(results) == 1
    assert not results[0].ok
    assert isinstance(results[0].log, tuple)
    assert "vacuous" in "\n".join(results[0].log)


def test_scholarly_source_shapes_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_scholarly_source_shapes.py"
    )
    monkeypatch.setattr(
        formal_tlc,
        "find_tla2tools_jar",
        lambda _repo_root: REPO_ROOT / ".tools" / "tla2tools.jar",
    )
    monkeypatch.setattr(formal_tlc.shutil, "which", lambda _name: "/usr/bin/java")

    def _run_tlc(command, **_kwargs):
        if any(
            "ScholarlySourceShapesAmbiguousAlias.cfg" in str(part) for part in command
        ):
            return SimpleNamespace(
                returncode=1,
                stdout="Error: Invariant AliasConfigurationSafe is violated.\n",
                stderr="",
            )
        return SimpleNamespace(
            returncode=0,
            stdout=(
                "Model checking completed. No error has been found.\n"
                "10 states generated, 8 distinct states found, "
                "0 states left on queue.\n"
            ),
            stderr="",
        )

    monkeypatch.setattr(formal_tlc.subprocess, "run", _run_tlc)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")
    assert {row["property_id"] for row in rows} == {
        "ScholarlySourceShapeSafety",
        "ScholarlySourceShapeAmbiguousAliasWitness",
    }


def test_typed_applicability_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_typed_applicability.py")
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {"TypedApplicabilitySafety"}


def test_fact_batch_recovery_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_fact_batch_recovery.py")
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {"formal.producer-batch.recovery"}
    assert rows[0]["refinement_tests"] == [
        "tests/substrate/fact_batch/test_recovery.py"
    ]


def test_entity_resolution_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_entity_resolution.py")
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {"EntityMergeAtomicity"}
    assert rows[0]["refinement_tests"] == [
        "tests/substrate/entity_resolution/test_merge_rehome.py",
        "tests/substrate/entity_resolution/test_merge_identity_projection.py",
        "tests/substrate/entity_resolution/test_merge_dedup_provenance.py",
    ]


def test_pact_projection_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_pact_projection.py")
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {"PactProjectionContract"}


def test_pact_formal_evidence_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_pact_formal_evidence.py"
    )
    _fake_tlc(module, monkeypatch)

    default_out = tmp_path / "default"
    assert module.main(["--out", str(default_out)]) == 0
    rows = _assert_strict_rows(default_out / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {"PactFormalEvidenceFresh"}
    assert rows[0]["checker_command"] == (
        "uv run --extra dev python "
        f"scripts/check_formal_pact_formal_evidence.py --out {default_out}"
    )
    assert set(rows[0]["refinement_tests"]) >= {
        "tests/pact/test_pact_formal_evidence_formal.py::"
        "test_pact_formal_evidence_formal_binding_covers_contract",
        "tests/pact/test_v3_formal_flow.py::"
        "test_formal_binding_requires_checker_refinement_and_freshness",
        "tests/pact/test_v3_formal_flow.py::"
        "test_stale_formal_evidence_no_longer_satisfies_requirements_tasks_or_gates",
    }

    selected_out = tmp_path / "selected"
    assert (
        module.main(
            [
                "--out",
                str(selected_out),
                "--property-id",
                "TypeOK",
                "--property-id",
                "VerificationEnteredFresh",
                "--property-id",
                "NoSilentStaleAfterVerification",
            ]
        )
        == 0
    )
    selected_rows = _assert_strict_rows(selected_out / "formal-report.yaml")

    assert {row["property_id"] for row in selected_rows} == {
        "TypeOK",
        "VerificationEnteredFresh",
        "NoSilentStaleAfterVerification",
    }


def test_workflow_authority_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_workflow_authority.py")
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {"WorkflowAuthorityContract"}


def test_nomos_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_nomos.py")
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {"NomosControlPlaneContract"}


def test_nomos_counterexample_report_producer_requires_expected_invariant(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_nomos.py")
    _fake_nomos_counterexamples(module, monkeypatch)

    assert (
        module.main(
            [
                "--model",
                "NomosCounterexample.tla",
                "--config",
                "NomosCounterexample.cfg",
                "--out",
                str(tmp_path),
                "--expect-counterexample",
            ]
        )
        == 0
    )
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "NomosNonVacuityMissingPolicy",
        "NomosNonVacuityWeakDeploymentPosture",
        "NomosNonVacuityUnscopedIdentityClaim",
        "NomosNonVacuityUnknownAction",
        "NomosNonVacuityPrivateExportWithoutScopedClaim",
        "NomosNonVacuitySourceDeploymentMismatch",
    }


def test_poros_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_poros.py")
    _fake_tlc(module, monkeypatch)

    positive_out = tmp_path / "positive"
    assert (
        module.main(
            [
                "--out",
                str(positive_out),
                "--checker-command",
                module.DEFAULT_CHECKER_COMMAND,
            ]
        )
        == 0
    )
    rows = _assert_strict_rows(positive_out / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {"formal.poros.exchange-safety"}
    positive_row = rows[0]
    assert positive_row["checker_command"] == module.DEFAULT_CHECKER_COMMAND
    assert {
        "scripts/check_poros_refinement_fixture.py",
        "scripts/check_poros_refinement_from_db.py",
        "tests/substrate/exchange/test_poros_http.py::"
        "test_http_receiver_service_exported_outbox_message_refines_published_flow",
        "tests/substrate/exchange/test_poros_inbox_and_authority.py",
        "tests/substrate/exchange/test_poros_admission.py",
        "tests/substrate/exchange/test_poros_graph_shape.py",
    } <= set(positive_row["refinement_tests"])
    assert set(module.POROS_POSITIVE_INVARIANTS) == formal_tlc.config_invariants(
        REPO_ROOT / "formal" / "poros" / "PorosExchange.cfg"
    )

    _fake_tlc_counterexample(
        module,
        monkeypatch,
        invariant="BrokenReceiptImpliesPublication",
    )
    counterexample_out = tmp_path / "counterexample"
    assert (
        module.main(
            [
                "--model",
                "PorosExchangeCounterexample.tla",
                "--config",
                "PorosExchangeCounterexample.cfg",
                "--out",
                str(counterexample_out),
                "--expect-counterexample",
                "--checker-command",
                module.DEFAULT_COUNTEREXAMPLE_CHECKER_COMMAND,
            ]
        )
        == 0
    )
    counterexample_rows = _assert_strict_rows(counterexample_out / "formal-report.yaml")

    assert {row["property_id"] for row in counterexample_rows} == {
        "formal.poros.receipt-publication-nonvacuity",
    }
    counterexample_row = counterexample_rows[0]
    assert (
        counterexample_row["checker_command"]
        == module.DEFAULT_COUNTEREXAMPLE_CHECKER_COMMAND
    )
    assert {
        "tests/substrate/test_formal_poros.py",
        "tests/substrate/exchange/test_poros_inbox_and_authority.py",
        "tests/substrate/exchange/test_poros_admission.py",
    } <= set(counterexample_row["refinement_tests"])
    assert "BrokenReceiptImpliesPublication" in Path(
        counterexample_row["artifact"]
    ).read_text(encoding="utf-8")
    assert "BrokenReceiptImpliesPublication" in (
        counterexample_out / "counterexample.txt"
    ).read_text(encoding="utf-8")


def test_pact_v4_decision_ledger_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_pact_v4_decision_ledger.py"
    )
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {"PactV4DecisionLedgerContract"}


def test_native_coordination_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(private_path("scripts/check_formal_native_coordination.py"))
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} >= {
        "coord-native.native-pact-task-targets",
        "coord-native.target-edge-filtering",
        "coord-native.expected-state-prevents-conflicts",
        "coord-native.lifecycle-and-pact-gates-separated",
        "coord-native.closeout-required-for-task-done",
    }
    assert "coord-native.legacy-import-not-authority" not in {
        row["property_id"] for row in rows
    }
    assert all(
        prop.property_id != "coord-native.legacy-import-not-authority"
        for prop in module.PROPERTIES
    )
    model_text = (
        REPO_ROOT / "formal" / "coordination" / "NativeCoordination.tla"
    ).read_text(encoding="utf-8")
    assert "ImportLegacyHistory" not in model_text
    assert "LegacyHistoryNeverCurrentAuthority" not in model_text
    assert '"legacy"' not in model_text
    separated = next(
        row
        for row in rows
        if row["property_id"] == "coord-native.lifecycle-and-pact-gates-separated"
    )
    assert separated["refinement_tests"] == [
        (
            "tests/pact/test_coordination_gate_evidence.py::"
            "test_task_done_explains_missing_and_satisfied_coordination_gates"
        ),
        (
            "tests/pact/test_coordination_gate_evidence.py::"
            "test_task_done_rejects_stale_coordination_gate_generation"
        ),
        (
            "tests/pact/test_coordination_gate_evidence.py::"
            "test_v4_task_done_refuses_stale_graph_authority"
        ),
        (
            "tests/okg-workspace/test_coord_native_write_state.py::"
            "test_closeout_before_finalize_marks_task_done_on_merge"
        ),
    ]


def test_coordination_protocols_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(private_path("scripts/check_formal_coordination_protocols.py"))
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")
    by_property = {row["property_id"]: row for row in rows}

    assert set(by_property) == {
        "ClaimUniqueness",
        "TakeoverRoutingCapacitySafety",
    }
    assert tuple(by_property["ClaimUniqueness"]["refinement_tests"]) == (
        module.CLAIM_REFINEMENT_TESTS
    )
    assert (
        tuple(by_property["TakeoverRoutingCapacitySafety"]["refinement_tests"])
        == module.TAKEOVER_ROUTING_CAPACITY_REFINEMENT_TESTS
    )
    assert all(row["checker_command"] == module.DEFAULT_CHECKER_COMMAND for row in rows)
    expected_module_digest = (
        "sha256:" + hashlib.sha256(module.DEFAULT_MODEL.read_bytes()).hexdigest()
    )
    expected_config_digest = (
        "sha256:" + hashlib.sha256(module.DEFAULT_CONFIG.read_bytes()).hexdigest()
    )
    assert {row["module_digest"] for row in rows} == {expected_module_digest}
    assert {row["config_digest"] for row in rows} == {expected_config_digest}


def test_coordination_protocols_witnesses_are_registered_and_expected(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(private_path("scripts/check_formal_coordination_protocols.py"))
    suite = _load_suite()
    checker = next(
        spec
        for spec in suite.CHECKERS
        if spec.name == "check_formal_coordination_protocols.py"
    )
    expected = {
        "CoordinationProtocolsStaleAckCounterexample.cfg": (
            "StaleAcknowledgementCounterexample",
            "ExactRouteAtMessageTransition",
        ),
        "CoordinationProtocolsDuplicateChangeCounterexample.cfg": (
            "DuplicateChangeCounterexample",
            "ChangeUniqueness",
        ),
        "CoordinationProtocolsDuplicateOwnerPairCounterexample.cfg": (
            "DuplicateOwnerPairCounterexample",
            "OwnerPairUniqueness",
        ),
        "CoordinationProtocolsHelperMaxCounterexample.cfg": (
            "HelperMaxCounterexample",
            "HelperBudgetSafety",
        ),
    }
    assert checker.witnesses == tuple(
        ("--config", config_name, "--expect-counterexample") for config_name in expected
    )

    for index, (config_name, (property_id, invariant)) in enumerate(expected.items()):
        _fake_tlc_counterexample(module, monkeypatch, invariant=invariant)
        out_dir = tmp_path / f"witness-{index}"
        assert (
            module.main(
                [
                    "--config",
                    config_name,
                    "--out",
                    str(out_dir),
                    "--expect-counterexample",
                ]
            )
            == 0
        )
        rows = _assert_strict_rows(out_dir / "formal-report.yaml")
        assert {row["property_id"] for row in rows} == {property_id}
        assert rows[0]["module_digest"] == (
            "sha256:" + hashlib.sha256(module.DEFAULT_MODEL.read_bytes()).hexdigest()
        )
        assert rows[0]["config_digest"] == (
            "sha256:"
            + hashlib.sha256((module.MODEL_DIR / config_name).read_bytes()).hexdigest()
        )
        assert invariant in Path(rows[0]["artifact"]).read_text(encoding="utf-8")


def test_source_sync_report_producer_uses_bound_invariant_ids(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_source_sync_convergence.py"
    )
    _fake_source_sync_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "FrontierAdvanceResumeIsSafe",
    }
    artifact = Path(rows[0]["artifact"])
    assert "Non-vacuity witnesses:" in artifact.read_text(encoding="utf-8")


def test_source_sync_report_producer_filters_requested_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_source_sync_convergence.py"
    )
    _fake_source_sync_tlc(module, monkeypatch)

    assert (
        module.main(
            [
                "--out",
                str(tmp_path),
                "--property-id",
                "NoFalseReadyFromIncompleteWork",
                "--property-id",
                "NoHiddenStuckRecoveryWork",
                "--property-id",
                "FreshWatermarkRequiresPublishAndMcpVisibility",
            ]
        )
        == 0
    )
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "NoFalseReadyFromIncompleteWork",
        "NoHiddenStuckRecoveryWork",
        "FreshWatermarkRequiresPublishAndMcpVisibility",
    }


def test_source_sync_nonvacuity_counterexamples_are_expected(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_source_sync_convergence.py"
    )
    _fake_source_sync_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    ready_rows = _assert_strict_rows(
        tmp_path / "nonvacuity-ReadyWitness" / "formal-report.yaml"
    )
    stuck_rows = _assert_strict_rows(
        tmp_path / "nonvacuity-StuckWitness" / "formal-report.yaml"
    )
    mcp_blocked_rows = _assert_strict_rows(
        tmp_path / "nonvacuity-McpBlockedWitness" / "formal-report.yaml"
    )

    assert ready_rows[0]["property_id"] == "source-sync.ready-witness-reachable"
    assert stuck_rows[0]["property_id"] == "source-sync.stuck-witness-reachable"
    assert (
        mcp_blocked_rows[0]["property_id"]
        == "source-sync.mcp-blocked-witness-reachable"
    )
    assert "ReadyWitnessNeverOccurs" in Path(ready_rows[0]["artifact"]).read_text(
        encoding="utf-8"
    )
    assert "StuckWitnessNeverOccurs" in Path(stuck_rows[0]["artifact"]).read_text(
        encoding="utf-8"
    )
    assert "McpBlockedWitnessNeverOccurs" in Path(
        mcp_blocked_rows[0]["artifact"]
    ).read_text(encoding="utf-8")


def test_admission_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_admission.py")
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "ZeroOutputAdmissionCannotPublishWithoutRefs",
    }
    assert (
        "tests/substrate/test_ingest_service.py::"
        "test_zero_output_source_sync_uses_bounded_admission_path"
    ) in rows[0]["refinement_tests"]
    assert (
        "tests/substrate/test_ingest_service.py::"
        "test_zero_output_source_sync_accepts_non_fast_track_modes"
    ) in rows[0]["refinement_tests"]
    # Repo paths are stamped RELATIVE so the recorded command stays portable
    # across checkouts; the tmp out dir is outside the repo and stays absolute.
    assert rows[0]["checker_command"] == (
        "uv run --extra dev python scripts/check_formal_admission.py "
        f"--profile full --config Admission.cfg "
        f"--out {tmp_path} "
        "--property-id ZeroOutputAdmissionCannotPublishWithoutRefs "
        "--timeout-seconds 900"
    )
    assert rows[0]["execution_profile"] == "full"
    assert rows[0]["profile_timeout_seconds"] == 900
    assert rows[0]["proof_status"] == "freshly_proven"
    assert rows[0]["timed_out"] is False


def test_agent_operator_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_agent_operator_ergonomics.py"
    )
    _fake_agent_operator_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "AgentOperatorApprovalSafety",
        "AgentOperatorTriggerSafety",
        "AgentOperatorReaperSafety",
        "AgentOperatorRuntimeSafety",
        "AgentOperatorUnsafeApprovalWitness",
        "AgentOperatorDuplicatePollWitness",
        "AgentOperatorBackstopSuppressionWitness",
        "AgentOperatorStartedReapWitness",
    }
    refinement_tests = {test for row in rows for test in row["refinement_tests"]}
    assert {
        "tests/substrate/runtime/test_admission_workflows.py",
        "tests/substrate/test_source_sync_poll_bounded.py",
        "tests/substrate/runtime/test_worker_schedule_modes.py",
    } <= refinement_tests


def test_deployment_authoring_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_deployment_authoring.py"
    )
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        prop.property_id for prop in module.PROPERTIES
    }
    assert all(row["checker_command"] == module.DEFAULT_CHECKER_COMMAND for row in rows)
    by_id = {row["property_id"]: row for row in rows}
    assert (
        "tests/substrate/cli/test_deployment_new.py"
        in by_id["Issue252ScaffoldSafety"]["refinement_tests"]
    )


def test_deployment_apply_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_deployment_apply.py")
    _fake_deployment_apply_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "DeploymentApplySafety",
        "DeploymentApplyStalePlanWitness",
        "DeploymentApplySkippedStageWitness",
        "DeploymentApplyDuplicateActionWitness",
        "DeploymentApplyFalseWorkerAckWitness",
    }
    assert all(
        row["refinement_tests"] == ["tests/substrate/cli/test_deployment_apply.py"]
        for row in rows
    )


def test_runtime_cutover_report_producer_emits_strict_row(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_runtime_cutover.py")
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert [row["property_id"] for row in rows] == ["ReplaceableCutoverSafety"]
    assert rows[0]["checker_command"] == module.DEFAULT_CHECKER_COMMAND
    assert set(rows[0]["refinement_tests"]) == set(module.REFINEMENT_TESTS)


def test_deployment_promotion_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_deployment_promotion.py"
    )
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {"DeploymentPromotionSafety"}
    assert rows[0]["checker_command"] == module.DEFAULT_CHECKER_COMMAND
    assert set(rows[0]["refinement_tests"]) == set(module.REFINEMENT_TESTS)


def test_derivation_parity_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_derivation_parity.py")
    _fake_derivation_parity_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "IncrementalEqualsBatchForLinearDerivation",
        "DerivationParityDriftCounterexample",
    }
    by_id = {row["property_id"]: row for row in rows}
    assert by_id["IncrementalEqualsBatchForLinearDerivation"]["proof_status"] == (
        "freshly_proven"
    )
    assert by_id["DerivationParityDriftCounterexample"]["summary"] == (
        "Counter-example produced as expected."
    )
    assert all(row["checker_command"] == module.DEFAULT_CHECKER_COMMAND for row in rows)
    assert all(
        "tests/substrate/test_maintained_derivation_interval_storage.py"
        in row["refinement_tests"]
        for row in rows
    )


def test_enricher_backfill_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_enricher_backfill.py")
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "TargetedBackfillIsolation",
        "ChunkProgressRetrySafety",
        "PinnedFrontierSafety",
    }
    assert all(row["checker_command"] == module.DEFAULT_CHECKER_COMMAND for row in rows)
    assert "TargetedEnricherBackfillContract" not in {
        row["property_id"] for row in rows
    }


def test_enricher_backlog_drain_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_enricher_backlog_drain.py"
    )

    def fake_jar(_repo_root):
        return REPO_ROOT / ".tools" / "tla2tools.jar"

    monkeypatch.setattr(
        module,
        "find_tla2tools_jar",
        fake_jar,
    )
    monkeypatch.setattr(formal_tlc, "find_tla2tools_jar", fake_jar)
    monkeypatch.setattr(module.shutil, "which", lambda _name: "/usr/bin/java")

    def _run_tlc(command, **_kwargs):
        config = str(command[command.index("-config") + 1])
        common = "Finished in 00s\n"
        if config.endswith("EnricherBacklogDrainPreReuseStrandedCounterexample.cfg"):
            return SimpleNamespace(
                returncode=13,
                stdout=(
                    "Temporal properties were violated.\n"
                    "The following behavior constitutes a counter-example:\n"
                    "5 states generated, 3 distinct states found, "
                    "0 states left on queue.\n" + common
                ),
                stderr="",
            )
        if config.endswith("EnricherBacklogDrainPostReuseUnsafeCounterexample.cfg"):
            return SimpleNamespace(
                returncode=12,
                stdout=(
                    "Invariant ReuseOnlyWhenNoEligibleBacklog is violated.\n"
                    "2 states generated, 2 distinct states found, "
                    "0 states left on queue.\n" + common
                ),
                stderr="",
            )
        return SimpleNamespace(
            returncode=0,
            stdout=(
                "Model checking completed. No error has been found.\n"
                "6 states generated, 4 distinct states found, "
                "0 states left on queue.\n" + common
            ),
            stderr="",
        )

    monkeypatch.setattr(module.subprocess, "run", _run_tlc)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "EveryEligibleInputEventuallyProcessed",
        "EnricherBacklogPreReuseStrandedCounterexample",
        "EnricherBacklogPostReuseUnsafeCounterexample",
    }
    assert all(row["checker_command"] == module.DEFAULT_CHECKER_COMMAND for row in rows)
    assert all(
        set(row["refinement_tests"]) == set(module.REFINEMENT_TESTS) for row in rows
    )


def test_watermark_quarantine_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_watermark_quarantine.py"
    )

    def fake_jar(_repo_root):
        return REPO_ROOT / ".tools" / "tla2tools.jar"

    monkeypatch.setattr(module, "find_tla2tools_jar", fake_jar)
    monkeypatch.setattr(formal_tlc, "find_tla2tools_jar", fake_jar)
    monkeypatch.setattr(module.shutil, "which", lambda _name: "/usr/bin/java")

    def _run_tlc(command, **_kwargs):
        config = str(command[command.index("-config") + 1])
        witness_by_config = {
            "WatermarkQuarantineSystemicFailure.cfg": (
                "SystemicFailureMisclassificationWitness"
            ),
            "WatermarkQuarantineSnapshotLoss.cfg": (
                "SnapshotOmissionRetractionWitness"
            ),
        }
        witness_id = next(
            (
                property_id
                for suffix, property_id in witness_by_config.items()
                if config.endswith(suffix)
            ),
            None,
        )
        if witness_id is not None:
            return SimpleNamespace(
                returncode=12,
                stdout=(
                    f"Invariant {witness_id} is violated.\n"
                    "The behavior up to this point is:\n"
                    "6 states generated, 6 distinct states found, "
                    "0 states left on queue.\nFinished in 00s\n"
                ),
                stderr="",
            )
        return SimpleNamespace(
            returncode=0,
            stdout=(
                "Model checking completed. No error has been found.\n"
                "100 states generated, 63 distinct states found, "
                "0 states left on queue.\nFinished in 00s\n"
            ),
            stderr="",
        )

    monkeypatch.setattr(module.subprocess, "run", _run_tlc)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")
    assert {row["property_id"] for row in rows} == {
        "WatermarkQuarantineSafety",
        "SystemicFailureHeld",
        "CrashRollbackSafety",
        "SystemicFailureMisclassificationWitness",
        "SnapshotOmissionRetractionWitness",
    }
    assert all(row["checker_command"] == module.DEFAULT_CHECKER_COMMAND for row in rows)
    assert all(
        set(row["refinement_tests"]) == set(module.REFINEMENT_TESTS) for row in rows
    )


def test_enricher_backlog_drain_counterexample_validation_fails_closed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_enricher_backlog_drain.py"
    )
    monkeypatch.setattr(
        module,
        "find_tla2tools_jar",
        lambda _repo_root: REPO_ROOT / ".tools" / "tla2tools.jar",
    )
    monkeypatch.setattr(module.shutil, "which", lambda _name: "/usr/bin/java")
    args = SimpleNamespace(
        model=module.DEFAULT_MODEL,
        timeout_seconds=1,
        checker_command=module.DEFAULT_CHECKER_COMMAND,
    )

    malformed_results = (
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=99,
            stdout="Temporal properties were violated.\n",
            stderr="fatal TLC launcher error\n",
        ),
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=13,
            stdout=(
                "Parsing or semantic analysis failed.\n"
                "Temporal properties were violated.\n"
                "The following behavior constitutes a counter-example:\n"
                "3 states generated, 3 distinct states found, "
                "0 states left on queue.\nFinished in 00s\n"
            ),
            stderr="",
        ),
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=12,
            stdout=(
                "Invariant ReuseOnlyWhenNoEligibleBacklog is violated.\n"
                "3 states generated, 3 distinct states found, "
                "0 states left on queue.\nFinished in 00s\n"
            ),
            stderr="",
        ),
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            subprocess.TimeoutExpired(cmd="tlc2.TLC", timeout=1)
        ),
    )

    for index, runner in enumerate(malformed_results):
        monkeypatch.setattr(module.subprocess, "run", runner)
        rows, rc = module._run_counterexample(
            args,
            tmp_path / f"failure-{index}",
            label="pre_reuse_stranded",
            config=module.PRE_REUSE_COUNTEREXAMPLE_CONFIG,
            properties=module.PRE_REUSE_COUNTEREXAMPLE_PROPERTIES,
            expected_exit_code=13,
            expected_violation=module._TEMPORAL_VIOLATION,
            temporal=True,
            min_distinct_states=module.MIN_PRE_REUSE_WITNESS_STATES,
        )
        assert rc != 0
        assert rows == []


def test_gtir_commit_report_producer_emits_strict_rows(
    tmp_path: Path,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_gtir_commit.py")

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "gtir.commit_witness",
        "gtir.guard_dominance",
    }
    assert all(row["checker_command"] == module.DEFAULT_CHECKER_COMMAND for row in rows)
    payload = json.loads(Path(rows[0]["artifact"]).read_text(encoding="utf-8"))
    by_id = {row["property_id"]: row for row in payload["results"]}
    assert by_id["gtir.commit_witness"]["states_checked"] >= 1000
    assert by_id["gtir.guard_dominance"]["counterexamples"] == ["unguarded_mutation"]


def test_gtir_commit_report_producer_rejects_degenerate_results(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_gtir_commit.py")
    low_counts = [
        {
            "property_id": "gtir.commit_witness",
            "status": "pass",
            "states_checked": 1,
            "counterexamples": ["bypass"],
        },
        {
            "property_id": "gtir.guard_dominance",
            "status": "pass",
            "states_checked": 1,
            "counterexamples": ["unguarded_mutation"],
        },
    ]
    duplicate_rows = [
        {
            "property_id": "gtir.commit_witness",
            "status": "pass",
            "states_checked": module.MIN_STATES_CHECKED,
            "counterexamples": ["bypass"],
        },
        {
            "property_id": "gtir.commit_witness",
            "status": "pass",
            "states_checked": module.MIN_STATES_CHECKED,
            "counterexamples": ["unwitnessed"],
        },
    ]

    for index, rows in enumerate((low_counts, duplicate_rows)):
        out_dir = tmp_path / f"case-{index}"
        monkeypatch.setattr(module, "run_report", lambda rows=rows: rows)
        assert module.main(["--out", str(out_dir)]) == 1
        assert not (out_dir / "formal-report.yaml").exists()


def test_seal_writes_report_producer_emits_strict_rows(
    tmp_path: Path,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_seal_writes.py")

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "sealw.runtime_write_witnessed",
    }
    assert rows[0]["checker_command"] == module.DEFAULT_CHECKER_COMMAND
    payload = json.loads(Path(rows[0]["artifact"]).read_text(encoding="utf-8"))
    result = payload["results"][0]
    assert result["states_checked"] >= module.MIN_STATES_CHECKED
    assert result["counterexamples"] == ["seal_disabled_runtime_unwitnessed_reachable"]


def test_seal_writes_report_producer_rejects_degenerate_results(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_seal_writes.py")
    low_counts = [
        {
            "property_id": "sealw.runtime_write_witnessed",
            "status": "pass",
            "states_checked": 1,
            "counterexamples": ["seal_disabled_runtime_unwitnessed_reachable"],
        }
    ]
    duplicate_rows = [
        {
            "property_id": "sealw.runtime_write_witnessed",
            "status": "pass",
            "states_checked": module.MIN_STATES_CHECKED,
            "counterexamples": ["seal_disabled_runtime_unwitnessed_reachable"],
        },
        {
            "property_id": "sealw.runtime_write_witnessed",
            "status": "pass",
            "states_checked": module.MIN_STATES_CHECKED,
            "counterexamples": ["seal_disabled_runtime_unwitnessed_reachable"],
        },
    ]

    for index, rows in enumerate((low_counts, duplicate_rows)):
        out_dir = tmp_path / f"case-{index}"
        monkeypatch.setattr(module, "run_seal_report", lambda rows=rows: rows)
        assert module.main(["--out", str(out_dir)]) == 1
        assert not (out_dir / "formal-report.yaml").exists()


def test_delta_frontier_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_delta_frontier.py")
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {"DeltaFrontierContract"}


def test_delta_projection_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_delta_projection.py")
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "RetractionDoesNotLeaveLiveProvenance",
        "DirtyScopesCoverChangedDependents",
    }


def test_transform_gate_dirty_scope_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_transform_gate_dirty_scope.py"
    )
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "TransformGateDirtyScopeSoundness",
    }
    soundness_row = rows[0]
    assert (
        "tests/substrate/test_chronos_narrowing_gate.py::"
        "test_chronos_dirty_scope_extraction_covers_node_edge_changes"
    ) in soundness_row["refinement_tests"]
    assert (
        "tests/substrate/test_chronos_narrowing_gate.py::"
        "test_chronos_dirty_scope_unavailable_forces_full_work"
    ) in soundness_row["refinement_tests"]
    # Repo paths are stamped RELATIVE so the recorded command stays portable.
    assert soundness_row["checker_command"] == (
        "uv run --extra dev python "
        "scripts/check_formal_transform_gate_dirty_scope.py "
        "--profile full --config TransformGateDirtyScope.cfg "
        f"--out {tmp_path} "
        "--property-id TransformGateDirtyScopeSoundness --timeout-seconds 900"
    )
    assert soundness_row["execution_profile"] == "full"
    assert soundness_row["profile_timeout_seconds"] == 900


def test_transform_gate_node_subtype_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_transform_gate_node_subtype.py"
    )
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "TransformGateNodeSubtypeScopeSoundness",
    }
    soundness_row = rows[0]
    assert (
        "tests/substrate/test_chronos_narrowing_gate.py::"
        "test_chronos_gate_blocks_node_with_unknown_subtype"
    ) in soundness_row["refinement_tests"]
    assert (
        "tests/substrate/test_fork_publish.py::"
        "test_fork_first_publish_blocks_retyped_inherited_node_to_unknown_subtype"
    ) in soundness_row["refinement_tests"]


def test_althea_optimizer_safety_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_althea_optimizer_safety.py"
    )
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "AltheaOptimizerSafetyKernel",
    }


def test_dogfood_cutover_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_dogfood_cutover.py")
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "RollbackPreservesFrontierAndProvenance",
    }


def test_chronos_read_pin_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_chronos_read_pin.py")
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {"ChronosReadPinSafety"}


def test_merge_back_safety_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_merge_back_safety.py")
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "formal.merge-back.stage-land-replay-equivalence",
        "formal.merge-back.trust-restamp-antilaundering",
        "formal.merge-back.quarantine-partition-atomicity",
        "formal.merge-back.land-block-rollback",
    }
    by_id = {row["property_id"]: row for row in rows}
    assert (
        "tests/substrate/test_merge_back_orchestrator.py::"
        "test_merge_back_holds_default_write_lock_through_land_publish"
    ) in by_id["formal.merge-back.stage-land-replay-equivalence"]["refinement_tests"]
    assert (
        "tests/substrate/test_merge_restamp.py::"
        "test_merge_producer_id_reads_back_untrusted"
    ) in by_id["formal.merge-back.trust-restamp-antilaundering"]["refinement_tests"]
    assert (
        "tests/substrate/test_merge_back_orchestrator.py::"
        "test_merge_back_stage_quarantine_fault_rolls_back_both_writes"
    ) in by_id["formal.merge-back.quarantine-partition-atomicity"]["refinement_tests"]
    assert (
        "tests/substrate/test_merge_back_orchestrator.py::"
        "test_merge_back_rolls_back_modified_node_and_edge_live_rows"
    ) in by_id["formal.merge-back.land-block-rollback"]["refinement_tests"]

    selected_out = tmp_path / "selected"
    assert (
        module.main(
            [
                "--out",
                str(selected_out),
                "--property-id",
                "formal.merge-back.land-block-rollback",
            ]
        )
        == 0
    )
    selected_rows = _assert_strict_rows(selected_out / "formal-report.yaml")
    assert {row["property_id"] for row in selected_rows} == {
        "formal.merge-back.land-block-rollback"
    }


def test_attribute_state_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_attribute_state.py")
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "AttributeStateSafety",
        "AttributeReadSurfaceSafety",
        "AttributeRetentionSafety",
        "AttributePromotionLifecycleSafety",
        "AttributeConflictSafety",
    }
    assert {row["execution_profile"] for row in rows} == {"full"}
    assert {row["profile_timeout_seconds"] for row in rows} == {600}
    assert {row["proof_status"] for row in rows} == {"freshly_proven"}


def test_expensive_tlc_profile_catalogs_are_machine_readable(
    capsys,
) -> None:
    for script_name in (
        "check_formal_admission.py",
        "check_formal_transform_gate_dirty_scope.py",
        "check_formal_attribute_state.py",
        "check_formal_nomos.py",
    ):
        module = _load_script(REPO_ROOT / "scripts" / script_name)

        assert module.main(["--list-profiles", "--json"]) == 0
        payload = json.loads(capsys.readouterr().out)

        assert payload["default_profile"] == "full"
        profiles = {profile["name"]: profile for profile in payload["profiles"]}
        # Every expensive-TLC checker exposes at least the ci smoke and the
        # full-strength release profiles. A checker MAY additionally expose
        # focused profiles that decompose an orthogonal feature dimension out
        # of the full config (e.g. nomos `iam` / `pins`); those keep the full
        # config's state space additive rather than multiplicative.
        assert {"full", "smoke"} <= set(profiles)
        for name, profile in profiles.items():
            if name in {"full", "smoke"}:
                continue
            assert profile["property_ids"], (
                f"{script_name} focused profile {name} must bind a property"
            )
            assert profile["full_strength"] is False
            assert profile["ci_smoke"] is False
        assert profiles["smoke"]["ci_smoke"] is True
        assert profiles["smoke"]["full_strength"] is False
        assert profiles["smoke"]["timeout_seconds"] <= 30
        assert profiles["smoke"]["property_ids"]
        assert profiles["full"]["ci_smoke"] is False
        assert profiles["full"]["full_strength"] is True
        assert profiles["full"]["timeout_seconds"] >= 600
        assert profiles["full"]["property_ids"]


def test_expensive_tlc_smoke_profiles_emit_profile_metadata(
    tmp_path: Path,
    monkeypatch,
) -> None:
    cases = (
        (
            "check_formal_admission.py",
            {"ZeroOutputAdmissionCannotPublishWithoutRefs"},
            30,
        ),
        (
            "check_formal_transform_gate_dirty_scope.py",
            {"TransformGateDirtyScopeSoundness"},
            30,
        ),
        (
            "check_formal_attribute_state.py",
            {"AttributeReadSurfaceSafety", "AttributeRetentionSafety"},
            30,
        ),
        (
            "check_formal_nomos.py",
            {"formal.nomos-source-lineage.visibility"},
            30,
        ),
    )
    for script_name, property_ids, timeout_seconds in cases:
        module = _load_script(REPO_ROOT / "scripts" / script_name)
        _fake_tlc(module, monkeypatch)
        out_dir = tmp_path / script_name.removesuffix(".py")

        assert module.main(["--profile", "smoke", "--out", str(out_dir)]) == 0
        rows = _assert_strict_rows(out_dir / "formal-report.yaml")

        assert {row["property_id"] for row in rows} == property_ids
        assert {row["execution_profile"] for row in rows} == {"smoke"}
        assert {row["profile_timeout_seconds"] for row in rows} == {timeout_seconds}
        assert {row["proof_status"] for row in rows} == {"freshly_proven"}
        assert {row["timed_out"] for row in rows} == {False}
        assert all("--profile smoke" in row["checker_command"] for row in rows)
        assert all("--config " in row["checker_command"] for row in rows)
        assert all("--timeout-seconds 30" in row["checker_command"] for row in rows)


def test_source_sync_observability_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_source_sync_observability.py"
    )
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "AmplificationUsesMeasuredTerms",
    }


def test_stable_reemission_report_producer_emits_strict_row(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_stable_reemission.py")
    _fake_stable_reemission_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {"StableReemission"}
    assert {row["proof_status"] for row in rows} == {"freshly_proven"}


def test_source_delta_contract_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_source_delta_contract.py"
    )
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "TombstonesLandCanonical",
        "ChurnKeepsRecordLive",
        "RetractionsAreComplete",
    }
    retractions_row = next(
        row for row in rows if row["property_id"] == "RetractionsAreComplete"
    )
    assert (
        "tests/substrate/test_source_runner_delta_contract.py::"
        "test_readiness_source_emits_delta_contract"
    ) in retractions_row["refinement_tests"]


def test_source_execution_lanes_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_source_execution_lanes.py"
    )
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "SourceExecutionLaneIsolation",
    }


def test_writer_gate_narrowing_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(
        REPO_ROOT / "scripts" / "check_formal_writer_gate_narrowing.py"
    )
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} == {
        "WriterGateNarrowingEquivalence",
    }


def test_console_helper_requires_explicit_archived_mode(tmp_path: Path) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_console.py")
    stale_report = tmp_path / "formal-report.yaml"
    stale_report.write_text("stale\n", encoding="utf-8")

    assert module.main(["--model", "vocabulary-lookup", "--out", str(tmp_path)]) == 2
    assert not stale_report.exists()
    assert not (tmp_path / "tlc-artifacts" / "formal-report.yaml").exists()
    assert not (tmp_path / "archived-formal-report.yaml").exists()


def test_console_archived_helper_removes_stale_report_on_setup_error(
    tmp_path: Path,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_console.py")
    stale_report = tmp_path / "formal-report.yaml"
    stale_report.write_text("stale\n", encoding="utf-8")

    assert (
        module.main(
            [
                "--archived-helper",
                "--model",
                "vocabulary-lookup",
                "--property-id",
                "DoesNotExist",
                "--out",
                str(tmp_path),
            ]
        )
        == 2
    )

    assert not stale_report.exists()
    assert not (tmp_path / "tlc-artifacts" / "formal-report.yaml").exists()
    assert not (tmp_path / "archived-formal-report.yaml").exists()


def test_console_archived_helper_does_not_emit_pact_formal_report(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_console.py")
    _fake_tlc(module, monkeypatch)

    assert (
        module.main(
            [
                "--archived-helper",
                "--model",
                "vocabulary-lookup",
                "--out",
                str(tmp_path),
            ]
        )
        == 0
    )
    report = yaml.safe_load(
        (tmp_path / "archived-formal-report.yaml").read_text(encoding="utf-8")
    )

    assert not (tmp_path / "formal-report.yaml").exists()
    assert not (tmp_path / "tlc-artifacts" / "formal-report.yaml").exists()
    assert report["schema"] == "okg.formal.archived-helper.v1"
    assert report["active_pact_evidence"] is False
    assert report["status"] == "pass"
    assert "results" not in report
    assert {row["property_id"] for row in report["properties"]} >= {
        "LookupIsDeterministic",
    }


def test_console_archived_helper_disables_shared_formal_report_write(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_console.py")
    called: dict[str, bool] = {}

    def fake_run_tlc_formal_report(**kwargs):
        called["write_report"] = kwargs["write_report"]
        report_path = kwargs["out_dir"] / "formal-report.yaml"
        return SimpleNamespace(
            report_path=report_path,
            stdout_path=kwargs["out_dir"] / "tlc.stdout.log",
            stderr_path=kwargs["out_dir"] / "tlc.stderr.log",
            rows=(
                {
                    "property_id": "LookupIsDeterministic",
                    "summary": "archived helper row",
                },
            ),
            exit_code=0,
            run_failed=False,
            timed_out=False,
            invariant_violated=False,
        )

    monkeypatch.setattr(module, "run_tlc_formal_report", fake_run_tlc_formal_report)

    assert (
        module.main(
            [
                "--archived-helper",
                "--model",
                "vocabulary-lookup",
                "--out",
                str(tmp_path),
            ]
        )
        == 0
    )

    assert called["write_report"] is False
    assert not (tmp_path / "formal-report.yaml").exists()
    assert not (tmp_path / "tlc-artifacts" / "formal-report.yaml").exists()
    assert (tmp_path / "archived-formal-report.yaml").exists()


def test_console_archived_helper_keeps_progress_property_non_ingestible(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_console.py")
    _fake_tlc(module, monkeypatch)

    assert (
        module.main(
            [
                "--model",
                "offline-replay",
                "--property-id",
                "OnlineHeadWriteEventuallyLeavesQueue",
                "--archived-helper",
                "--out",
                str(tmp_path),
            ]
        )
        == 0
    )
    report = yaml.safe_load(
        (tmp_path / "archived-formal-report.yaml").read_text(encoding="utf-8")
    )

    assert not (tmp_path / "formal-report.yaml").exists()
    assert not (tmp_path / "tlc-artifacts" / "formal-report.yaml").exists()
    assert "results" not in report
    assert {row["property_id"] for row in report["properties"]} == {
        "OnlineHeadWriteEventuallyLeavesQueue",
    }


def test_console_archived_counterexample_is_expected_but_non_ingestible(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_console.py")
    _fake_tlc_counterexample(module, monkeypatch)

    out_dir = tmp_path / "offline-replay-counterexample"
    assert (
        module.main(
            [
                "--model",
                "offline-replay-counterexample",
                "--archived-helper",
                "--out",
                str(out_dir),
            ]
        )
        == 0
    )
    report = yaml.safe_load(
        (out_dir / "archived-formal-report.yaml").read_text(encoding="utf-8")
    )
    assert not (out_dir / "formal-report.yaml").exists()
    assert not (out_dir / "tlc-artifacts" / "formal-report.yaml").exists()
    assert "results" not in report
    assert {row["property_id"] for row in report["properties"]} == {
        "offline-replay-counterexample.replay-never-happens",
    }


def test_pact_mutation_report_producer_emits_strict_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_script(REPO_ROOT / "scripts" / "check_formal_pact_mutation.py")
    _fake_tlc(module, monkeypatch)

    assert module.main(["--out", str(tmp_path)]) == 0
    rows = _assert_strict_rows(tmp_path / "formal-report.yaml")

    assert {row["property_id"] for row in rows} >= {
        "mutation-safety.no-lost-completed-mutation",
        "mutation-safety.plan-effect-at-most-once",
    }
