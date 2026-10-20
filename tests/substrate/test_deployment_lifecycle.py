"""Failure-resilient deployment-up refinement tests.

These tests use a temporary receipt and a command seam only.  They never point
destructive backup or restore operations at the declared okg-workspace target.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from okg.substrate.deployment_upgrade import (
    CommandResult,
    UpgradeContext,
    UpgradeOptions,
    _load_or_initialize_state,
    _run_phase,
    run_deployment_upgrade,
)
from okg.substrate.deployment_lifecycle import (
    UP_PHASE_ORDER,
    integrated_deployment_gate,
    invalidated_up_phases,
    phase_fingerprints,
)
from okg.substrate.deployment_lifecycle_contract import CANONICAL_CONTRACT
from okg.substrate.errors import SubstrateError


def _options(tmp_path: Path, **kwargs: object) -> UpgradeOptions:
    return UpgradeOptions(
        deployment="disposable-fixture",
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path / "backups",
        **kwargs,
    )


def test_phase_fingerprints_keep_smoke_options_local(tmp_path: Path) -> None:
    base = _options(tmp_path, readiness_profile="dogfood")
    changed_source = _options(
        tmp_path, include=("fixture-source",), readiness_profile="dogfood"
    )
    target = {
        "deployment": "disposable-fixture",
        "database": "fixture_db",
        "identity": "isolated",
        "code_revision": "a" * 40,
    }
    before = phase_fingerprints(options=base, target_key=target)
    after = phase_fingerprints(options=changed_source, target_key=target)

    assert invalidated_up_phases(before, after) == frozenset(
        {"smoke_publish", "readiness"}
    )
    assert before["backup"] == after["backup"]
    assert before["migrate"] == after["migrate"]


def test_readiness_option_does_not_replay_smoke_or_backup(tmp_path: Path) -> None:
    target = {"deployment": "disposable-fixture", "database": "fixture_db"}
    before = phase_fingerprints(
        options=_options(tmp_path, readiness_profile="dogfood"), target_key=target
    )
    after = phase_fingerprints(
        options=_options(tmp_path, readiness_profile="ci"), target_key=target
    )
    assert invalidated_up_phases(before, after) == frozenset({"readiness"})


def test_target_revision_does_not_replay_backup_or_migration(tmp_path: Path) -> None:
    target = {
        "deployment": "disposable-fixture",
        "database": "fixture_db",
        "code_revision": "a" * 40,
        "manifest_digest": "sha256:manifest-a",
    }
    changed = {**target, "code_revision": "b" * 40}
    before = phase_fingerprints(options=_options(tmp_path), target_key=target)
    after = phase_fingerprints(options=_options(tmp_path), target_key=changed)
    invalidated = invalidated_up_phases(before, after)
    assert "backup" not in invalidated
    assert "migrate" not in invalidated
    assert "smoke_publish" in invalidated
    assert "readiness" in invalidated


def test_manifest_drift_invalidates_catalog_dependents_only(tmp_path: Path) -> None:
    target = {
        "deployment": "disposable-fixture",
        "database": "fixture_db",
        "manifest_digest": "sha256:manifest-a",
    }
    changed = {**target, "manifest_digest": "sha256:manifest-b"}
    before = phase_fingerprints(options=_options(tmp_path), target_key=target)
    after = phase_fingerprints(options=_options(tmp_path), target_key=changed)
    invalidated = invalidated_up_phases(before, after)
    assert "backup" not in invalidated
    assert "migrate" not in invalidated
    assert "catalog_apply" in invalidated


def test_backup_policy_drift_invalidates_backup_and_dependents(tmp_path: Path) -> None:
    before = phase_fingerprints(
        options=_options(tmp_path, backup_keep=1),
        target_key={"deployment": "disposable-fixture", "database": "fixture_db"},
    )
    after = phase_fingerprints(
        options=_options(tmp_path, backup_keep=2),
        target_key={"deployment": "disposable-fixture", "database": "fixture_db"},
    )
    invalidated = invalidated_up_phases(before, after)
    assert "backup" in invalidated
    assert "preflight" not in invalidated


def test_backup_policy_digest_is_load_bearing_for_backup(tmp_path: Path) -> None:
    target = {
        "deployment": "disposable-fixture",
        "database": "fixture_db",
        "backup_policy_digest": "sha256:policy-a",
    }
    changed = {**target, "backup_policy_digest": "sha256:policy-b"}
    before = phase_fingerprints(options=_options(tmp_path), target_key=target)
    after = phase_fingerprints(options=_options(tmp_path), target_key=changed)
    invalidated = invalidated_up_phases(before, after)
    assert "backup" in invalidated


def test_resume_receipt_records_phase_fingerprints_and_invalidates_dependents(
    tmp_path: Path,
    monkeypatch,
) -> None:
    target = {"deployment": "disposable-fixture", "database": "fixture_db"}
    original = _options(tmp_path, readiness_profile="dogfood")
    original_fp = phase_fingerprints(options=original, target_key=target)
    state_file = original.state_file
    state_file.write_text(
        json.dumps(
            {
                "deployment": original.deployment,
                "profile": original.profile,
                "target_key": target,
                "plan_only": False,
                "lifecycle_contract_digest": CANONICAL_CONTRACT.digest,
                "lifecycle_contract_version": CANONICAL_CONTRACT.contract_version,
                "completed": False,
                "completed_phases": list(UP_PHASE_ORDER),
                "phase_results": [],
                "phase_fingerprints": original_fp,
            }
        )
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._target_state_key",
        lambda _ctx: target,
    )
    changed = _options(
        tmp_path, include=("fixture-source",), readiness_profile="dogfood"
    )
    ctx = SimpleNamespace(
        options=changed,
        state_file=state_file,
        data={},
    )

    resumed = _load_or_initialize_state(ctx)

    assert resumed["invalidated_phases"] == ["smoke_publish", "readiness"]
    assert "backup" in resumed["completed_phases"]
    assert "migrate" in resumed["completed_phases"]
    assert "smoke_publish" not in resumed["completed_phases"]
    assert "readiness" not in resumed["completed_phases"]
    assert resumed["phase_fingerprints"] != original_fp


def test_policy_option_drift_reuses_receipts_and_invalidates_dependents(
    tmp_path: Path,
    monkeypatch,
) -> None:
    target = {
        "deployment": "disposable-fixture",
        "database": "fixture_db",
        "identity": "isolated",
    }
    original = _options(tmp_path, apply_services=True, sync_code=True)
    original_fp = phase_fingerprints(options=original, target_key=target)
    original.state_file.write_text(
        json.dumps({
            "deployment": original.deployment,
            "profile": original.profile,
            "target_key": {
                **target,
                "lifecycle_policy": {
                    "apply_services": True,
                    "sync_code": True,
                    "sync_deps": True,
                },
            },
            "plan_only": False,
            "lifecycle_contract_digest": CANONICAL_CONTRACT.digest,
            "lifecycle_contract_version": CANONICAL_CONTRACT.contract_version,
            "completed": False,
            "completed_phases": list(UP_PHASE_ORDER),
            "phase_results": [],
            "phase_fingerprints": original_fp,
        })
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._target_state_key",
        lambda _ctx: {
            **target,
            "lifecycle_policy": {
                "apply_services": False,
                "sync_code": True,
                "sync_deps": True,
            },
        },
    )
    changed = _options(tmp_path, apply_services=False, sync_code=True)
    ctx = SimpleNamespace(options=changed, state_file=original.state_file, data={})
    resumed = _load_or_initialize_state(ctx)
    assert resumed["completed_phases"]
    assert "preflight" in resumed["completed_phases"]
    assert "stop_services" not in resumed["completed_phases"]
    assert "restart_services" not in resumed["completed_phases"]


def test_phase_receipt_carries_the_semantic_fingerprint(
    tmp_path: Path,
    monkeypatch,
) -> None:
    options = _options(tmp_path)
    ctx = UpgradeContext(
        options=options,
        dsn="postgresql://fixture/fixture_db",
        resolution={"target": {"database": "fixture_db", "identity": "isolated"}},
        env={},
        state_file=options.state_file,
        backup_dir=options.backup_dir,
        command_runner=lambda _argv, _env: CommandResult(0, "{}", ""),
        data={"phase_fingerprints": {"preflight": "sha256:fixture"}},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_preflight",
        lambda _ctx: {"ok": True},
    )

    result = _run_phase(ctx, "preflight")

    assert result["ok"] is True
    assert result["semantic_fingerprint"] == "sha256:fixture"


def test_integrated_gate_keeps_backup_and_source_readiness_evidence_separate() -> None:
    gate = integrated_deployment_gate(
        [
            {
                "phase": "backup",
                "ok": True,
                "duration_seconds": 12.5,
                "dump_duration_seconds": 8.0,
                "budget_checks": {"within_budget": True},
                "verification_mode": "current_full_restore",
            },
            {"phase": "smoke_publish", "ok": True, "smoke_generation_id": 41},
            {"phase": "readiness", "ok": True, "readiness_generation_id": 41},
        ]
    )

    assert gate["schema"] == "okg.deployment_up.integrated_gate.v1"
    assert gate["ok"] is True
    assert gate["backup_performance"]["dump_duration_seconds"] == 8.0
    assert gate["source_readiness"]["smoke_generation_id"] == 41
    assert gate["source_readiness"]["readiness_generation_id"] == 41


@pytest.mark.parametrize(
    "backup_row",
    [
        {"phase": "backup", "ok": True, "skipped": "--plan"},
        {"phase": "backup", "ok": True, "planned": "pg_dump"},
    ],
)
def test_integrated_gate_rejects_unproven_backup_or_generation_mismatch(
    backup_row: dict[str, object],
) -> None:
    rows = [
        backup_row,
        {"phase": "smoke_publish", "ok": True, "smoke_generation_id": 41},
        {"phase": "readiness", "ok": True, "readiness_generation_id": 42},
    ]
    gate = integrated_deployment_gate(rows)
    assert gate["ok"] is False
    assert gate["backup_performance"]["phase_ok"] is False
    assert gate["source_readiness"]["generation_match"] is False


@pytest.mark.parametrize("failure_phase", UP_PHASE_ORDER)
def test_restore_contract_interruption_matrix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_phase: str,
) -> None:
    """Inject one failure per boundary using only a disposable receipt seam."""

    options = _options(tmp_path)
    ctx = UpgradeContext(
        options=options,
        dsn="postgresql://fixture/fixture_db",
        resolution={"target": {"database": "fixture_db", "identity": "isolated"}},
        env={},
        state_file=options.state_file,
        backup_dir=options.backup_dir,
        command_runner=lambda _argv, _env: CommandResult(0, "{}", ""),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._build_context",
        lambda *args, **kwargs: ctx,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._load_or_initialize_state",
        lambda _ctx: {
            "completed_phases": [],
            "phase_results": [],
            "completed": False,
            "cleanup_obligations": {},
        },
    )

    calls: list[str] = []

    def phase(_ctx: UpgradeContext, name: str) -> dict[str, object]:
        calls.append(name)
        if name == "backup":
            _ctx.data["cleanup_obligations"] = {
                "backup_artifacts": {"database": "fixture_db", "archive": "fixture.tgz"}
            }
        if name == "stop_services":
            _ctx.data["services_stopped"] = True
            _ctx.data["cleanup_obligations"]["service"] = {"action": "restart_services"}
        if name == "restart_services":
            _ctx.data["services_stopped"] = False
        if name == failure_phase:
            return {"phase": name, "ok": False, "reason": "injected boundary failure"}
        return {"phase": name, "ok": True}

    monkeypatch.setattr("okg.substrate.deployment_upgrade._run_phase", phase)

    cleanup_calls: list[str] = []

    def cleanup(_ctx: UpgradeContext, *, failed_phase: str) -> dict[str, object]:
        cleanup_calls.append(failed_phase)
        if _ctx.data.get("services_stopped"):
            _ctx.data["services_stopped"] = False
        _ctx.data["backup_obligation_discharged"] = True
        _ctx.data["service_obligation_discharged"] = True
        return {
            "phase": "failure_cleanup",
            "ok": True,
            "failed_phase": failed_phase,
        }

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._failure_service_cleanup",
        cleanup,
    )

    result = run_deployment_upgrade(options)

    boundary = UP_PHASE_ORDER.index(failure_phase)
    assert result["failed_phase"] == failure_phase
    assert calls == list(UP_PHASE_ORDER[: boundary + 1])
    assert cleanup_calls == [failure_phase]
    assert result["phases"][-1]["failure_cleanup"]["ok"] is True
    state = json.loads(options.state_file.read_text(encoding="utf-8"))
    assert state["cleanup_obligations"] == {}
    assert state["phase_statuses"][failure_phase] == "failed"
    failed_attempt = next(
        attempt for attempt in state["phase_attempts"].values()
        if attempt["phase"] == failure_phase and attempt["status"] == "failed"
    )
    assert failed_attempt["terminal_receipt"]["ok"] is False
    assert failed_attempt["effect_may_have_happened"] is (
        failure_phase not in {"preflight", "readiness"}
    )
    assert failed_attempt["reconcile"]["decision"] in {
        "terminal_receipt_observed",
        "terminal_failure_requires_postcondition_observation",
    }


@pytest.mark.parametrize("interrupted_phase", UP_PHASE_ORDER)
@pytest.mark.parametrize("receipt_window", ("before_terminal", "after_terminal"))
def test_restore_contract_process_interruption_and_resume_matrix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    interrupted_phase: str,
    receipt_window: str,
) -> None:
    """A real BaseException is blocked before, and promoted after, a receipt."""

    class InjectedInterruption(BaseException):
        pass

    options = _options(tmp_path, stop_after=interrupted_phase)
    ctx = UpgradeContext(
        options=options,
        dsn="postgresql://fixture/fixture_db",
        resolution={"target": {"database": "fixture_db", "identity": "isolated"}},
        env={},
        state_file=options.state_file,
        backup_dir=options.backup_dir,
        command_runner=lambda _argv, _env: CommandResult(0, "{}", ""),
    )
    fingerprints = {
        phase: f"sha256:{phase}-semantic-fingerprint" for phase in UP_PHASE_ORDER
    }
    initial = {
        "completed_phases": [],
        "phase_results": [],
        "completed": False,
        "cleanup_obligations": {},
        "safety_contract": {
            "plan": {"confirmation": {"digest": "sha256:test-plan"}}
        },
    }

    def load_state(_ctx: UpgradeContext) -> dict[str, object]:
        _ctx.data["phase_fingerprints"] = fingerprints
        if options.state_file.exists():
            _ctx.data["resumed_existing_state"] = True
            return json.loads(options.state_file.read_text(encoding="utf-8"))
        return dict(initial)

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._build_context",
        lambda *args, **kwargs: ctx,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._load_or_initialize_state",
        load_state,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._revalidate_resumed_safety_contract",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._revalidate_resume_postconditions",
        lambda _ctx, *, phase_results, completed: (
            phase_results,
            completed,
            {
                "schema": "okg.deployment_up.resume_revalidation.v1",
                "ok": True,
                "checks": {phase: True for phase in completed},
                "evidence": {},
                "superseded_phase_results": [],
            },
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._failure_service_cleanup",
        lambda *_args, **_kwargs: None,
    )
    calls: list[str] = []

    def phase(_ctx: UpgradeContext, name: str) -> dict[str, object]:
        calls.append(name)
        if name != interrupted_phase:
            return {"phase": name, "ok": True}
        if receipt_window == "after_terminal":
            state = json.loads(options.state_file.read_text(encoding="utf-8"))
            attempt_id = str(state["phase_attempt_id"])
            state.setdefault("phase_terminal_receipts", {})[attempt_id] = {
                "phase": name,
                "phase_attempt_id": attempt_id,
                "semantic_fingerprint": fingerprints[name],
                "ok": True,
                "completed_at": "2026-08-13T00:00:00+00:00",
            }
            from okg.substrate import deployment_upgrade as du

            du._write_state(options.state_file, state)
        raise InjectedInterruption(f"injected {receipt_window}")

    monkeypatch.setattr("okg.substrate.deployment_upgrade._run_phase", phase)

    with pytest.raises(InjectedInterruption, match=receipt_window):
        run_deployment_upgrade(options)
    first_calls = list(calls)
    interrupted = json.loads(options.state_file.read_text(encoding="utf-8"))
    attempt = interrupted["phase_attempts"][interrupted["phase_attempt_id"]]
    assert attempt["status"] == "uncertain"
    assert interrupted["phase_statuses"][interrupted_phase] == "uncertain"
    if receipt_window == "after_terminal":
        assert interrupted["phase_terminal_receipts"][
            interrupted["phase_attempt_id"]
        ]["semantic_fingerprint"] == fingerprints[interrupted_phase]

    if receipt_window == "before_terminal":
        with pytest.raises(
            SubstrateError,
            match="no matching terminal receipt",
        ):
            run_deployment_upgrade(options)
        assert calls == first_calls
        blocked = json.loads(options.state_file.read_text(encoding="utf-8"))
        assert blocked["status"] == "blocked"
        assert blocked["recovery_required"] is True
        assert attempt["terminal_receipt"] is None
    else:
        resumed = run_deployment_upgrade(options)
        assert resumed["stopped_after"] == interrupted_phase
        assert resumed["resumed_stop_boundary"] is True
        assert calls == first_calls
        promoted = json.loads(options.state_file.read_text(encoding="utf-8"))
        assert promoted["phase_statuses"][interrupted_phase] == "succeeded"
        promoted_attempt = promoted["phase_attempts"][
            next(
                attempt_id
                for attempt_id, row in promoted["phase_attempts"].items()
                if row["phase"] == interrupted_phase
                and row["status"] == "succeeded"
            )
        ]
        assert promoted_attempt["reconcile"]["decision"] == (
            "exact_terminal_receipt_promoted"
        )
