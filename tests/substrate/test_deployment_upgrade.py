from __future__ import annotations

import copy
import dataclasses

from contextlib import contextmanager
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.deployment_lifecycle_contract import (
    CANONICAL_CONTRACT,
    contract_digest,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.deployment_upgrade import (
    CommandResult,
    FleetUpgradeOptions,
    FleetDisposition,
    UpgradeContext,
    UpgradeOptions,
    load_fleet_dispositions,
    run_deployment_fleet_upgrade,
    run_deployment_upgrade,
    _phase_attribute_read_model,
    _phase_source_convergence,
    _phase_nomos_baseline as _actual_phase_nomos_baseline,
    _phase_stop_services,
    _backup_compatibility_snapshot as _actual_backup_compatibility_snapshot,
    _backup_headroom_preflight as _actual_backup_headroom_preflight,
    _backup_tool_executable_identity as _actual_backup_tool_executable_identity,
    _backup_post_dump_headroom_preflight as _actual_backup_post_dump_headroom_preflight,
    _collect_effect_inventory as _actual_collect_effect_inventory,
    _restore_headroom_preflight as _actual_restore_headroom_preflight,
    _verify_backup_restore as _actual_verify_backup_restore,
)


def _ok(payload: dict | None = None) -> CommandResult:
    return CommandResult(
        0,
        json.dumps(payload or {"ok": True, **_no_published_recovery()}),
        "",
    )


# Issue #1291: the uniform publish gate now also requires the receipt's
# measured advisory-lock hold to clear the configured timeout headroom
# (default 2x, i.e. at most half the 900s hard statement timeout). Hermetic
# payloads that stand for a HEALTHY publish carry a comfortably bounded hold;
# a payload with no lock receipt is unproven and fails the gate.
def _healthy_lock_timing_details() -> dict:
    return {
        "schema": "okg.publisher.timing_details.v1",
        "lock": {
            "coordination_mode": "dbos",
            "acquire_ms": 4.0,
            "wait_ms": 4.0,
            "hold_ms": 12_000.0,
            "hard_timeout_ms": 900_000,
            "slowest_phase": {
                "name": "stage.chronos_commit",
                "elapsed_ms": 8_000.0,
            },
        },
    }


def _published(generation_id: str, **extra) -> dict:
    """A smoke-publish payload that PASSES the uniform real-publish gate.

    The honest-up gate (publish_proof.assert_publish_proof) requires a published
    (not blocked) generation with a positive node/edge row delta; hermetic
    upgrade tests that drive a successful smoke publish must return such a
    payload from the ingest command.
    """
    return {
        "status": "published",
        "generation_id": generation_id,
        "nodes_added": 3,
        "edges_added": 2,
        "timing_details": _healthy_lock_timing_details(),
        **extra,
    }


def _no_published_recovery() -> dict:
    payload = {
        "schema": "okg.effective_attribute_recovery_classification.v1",
        "classification": "no_published_generation",
        "deployment": "dep",
        "branch": "default",
    }
    payload["binding_digest"] = "sha256:" + hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return payload


def test_ci_final_proof_accepts_resume_compatible_authoritative_shapes(
    tmp_path: Path,
) -> None:
    """A validated no-op smoke remains bound across a readiness resume."""
    from okg.substrate import deployment_upgrade as du

    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({
        "phase_results": [
            {"phase": "catalog_apply", "ok": True},
            {"phase": "nomos_baseline", "ok": True},
        ]
    }))
    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="okg-ci", readiness_profile="ci"),
        dsn="postgres://unused",
        resolution={"target": {}},
        env={},
        state_file=state_file,
        backup_dir=tmp_path,
        command_runner=lambda argv, env: _ok({
            "current_count": 0,
            "historical_count": 1,
        }),
        data={
            "smoke_publish": {
                "ordered": ["pact"],
                "results": [{"name": "pact", "status": "OK"}],
                "publishes": [{
                    "status": "skipped",
                    "publish_skipped": True,
                    "reason": "no_source_work",
                }],
            },
        },
    )
    release = {
        "ok": True,
        "current": {"code_revision": "resume-revision"},
    }
    readiness = {
        "database": {
            "required_relations": ["okg.nodes_live"],
            "present_relations": ["okg.nodes_live"],
            "missing_relations": [],
        },
        "generation": {
            "status": "published",
            "generation_id": "gen-2",
            "count_integrity": {
                "ok": True,
                "checkpoint": {"node_count": 2, "edge_count": 1},
                "pinned": {"node_count": 2, "edge_count": 1},
            },
        },
        "queues": {"active_queue_depth": 0},
        "source_freshness": {"ok": True, "missing": [], "stale": []},
        "source_sync_liveness": {
            # The direct producer evidence is healthy. The broader `ok` bit
            # can be false because its embedded convergence view is built
            # without the generation/freshness inputs supplied below.
            "ok": False,
            "producer_liveness_ok": True,
            "missing_producers": [],
            "stale_producers": [],
        },
        "source_sync_status": {
            "convergence": {"ok": True, "state": "ready"},
        },
        "nomos_audit": {"ok": True},
        "nomos_runtime": {"ok": True},
        "doctor": {"ok": True},
        "mcp": {"ok": True, "generation_id": "gen-2"},
    }

    proof = du._collect_ci_deployment_final_proof(
        ctx,
        release=release,
        readiness=readiness,
    )

    assert proof["ok"] is True, proof
    assert proof["smoke_generation_id"] == "gen-2"
    assert all(proof["checks"].values())


def _applied_sync_code(ctx) -> dict:
    """`_phase_sync_code` stub that reports a genuine APPLY (not a skip).

    A skip would (correctly) downgrade the run to status:"partial"; these tests
    characterize a fully-applied default upgrade, so the stub returns an applied
    revision with no ``skipped``/``planned`` marker.
    """
    return {"ok": True, "code_revision": "deadbeefcafe"}


def _stub_dependency_sync_checkout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give a faked deployment a resolvable dependency-sync target.

    `dependency_sync` resolves its target through the same repository
    authority `sync_code` uses, so a deployment faked at a tmp path -- which is
    inside no repository -- has no target and the phase refuses. These tests
    already stub `_phase_sync_code` for the same reason; this is the other half
    of that stub, and it names THIS checkout, which is what an in-repo
    deployment resolves to. Tests that exercise the resolution itself build a
    real repository instead and never call this.
    """
    from okg.substrate import deployment_upgrade as du

    monkeypatch.setattr(
        du,
        "_dependency_sync_repo_root",
        lambda *_args, **_kwargs: _planner_project_root(),
    )


def _stub_recovery_phases(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep single-phase ordering tests focused on their stated authority."""
    _stub_dependency_sync_checkout(monkeypatch)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_generation_recovery",
        lambda ctx: {"ok": True, "recovered": False},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_source_convergence",
        lambda ctx: {"ok": True, "state": "ready"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_attribute_read_model",
        lambda ctx: {"ok": True, "status": "unit"},
    )


def _blocked_convergence(source: str, run_mode: str = "scope_complete") -> dict:
    return {
        "ok": False,
        "source_sync_status": {
            "convergence": {
                "ok": False,
                "state": "blocked",
                "recovery_plan": {
                    "actions": [{
                        "action_kind": "completed_scope_reconcile",
                        "source_name": source,
                        "run_mode": run_mode,
                    }]
                },
                "next_action": {"blocking_stage": "planned_action"},
            }
        },
    }


def test_source_convergence_executes_readiness_recovery_plan_to_terminal_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"completed": False, "phase_results": []}))
    observed_sources: list[str] = []
    ready_payloads = [
        _blocked_convergence("git", "scope_complete"),
        _blocked_convergence("pact", "reconcile"),
        {"ok": True, "source_sync_status": {"convergence": {"ok": True, "state": "ready"}}},
    ]

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if "ready" in argv:
            return CommandResult(1 if not ready_payloads[0]["ok"] else 0, json.dumps(ready_payloads.pop(0)), "")
        if "current-runtime-dlq" in argv:
            return _ok({"current_count": 0, "current_failures": []})
        if "find-source-run" in argv:
            return _ok({"dbos_workflow_id": None})
        if "enqueue-source" in argv:
            source = argv[argv.index("--source") + 1]
            assert argv[argv.index("--promotion-id") + 1] == "deploy-owner"
            observed_sources.append(source)
            return _ok({"dbos_workflow_id": f"wf-{source}"})
        if "debug-workflow" in argv:
            workflow_id = argv[-1]
            persisted = json.loads(state_file.read_text())["source_recovery_workflows"]
            assert any(row["workflow_id"] == workflow_id for row in persisted)
            return _ok({"workflow_id": workflow_id, "status": "SUCCESS", "output": {"any_failed": False}})
        raise AssertionError(argv)

    dep_dir = tmp_path / "deployment"
    dep_dir.mkdir()
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(deployment_dir=dep_dir),
    )
    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="dep", skip_backup=True),
        dsn="postgres://test",
        resolution={"target": {"database": "test", "identity": "isolated"}},
        env={"OKG_DSN": "postgres://test", "OKG_DEPLOYMENT_SOURCE_POLL_SECONDS": "0.001"},
        state_file=state_file,
        backup_dir=tmp_path,
        command_runner=runner,
        data={"runtime_fence_owner": "deploy-owner"},
    )
    result = _phase_source_convergence(ctx)
    assert result["ok"] is True, result
    assert observed_sources == ["git", "pact"]
    assert [row["terminal_status"] for row in result["workflows"]] == ["SUCCESS", "SUCCESS"]


def test_source_convergence_refuses_manual_and_nonmoving_work(
    tmp_path: Path,
) -> None:
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"completed": False, "phase_results": []}))
    payloads = [{
        "ok": False,
        "source_sync_status": {"convergence": {
            "ok": False,
            "recovery_plan": {"actions": [{
                "action_kind": "manual_or_credential_action",
                "source_name": "private",
            }]},
        }},
    }]

    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="dep", skip_backup=True),
        dsn="postgres://test",
        resolution={"target": {}},
        env={"OKG_DSN": "postgres://test"},
        state_file=state_file,
        backup_dir=tmp_path,
        command_runner=lambda argv, env: CommandResult(1, json.dumps(payloads[0]), ""),
    )
    result = _phase_source_convergence(ctx)
    assert result["ok"] is False
    assert result["source_name"] == "private"
    assert result["blocking_stage"] == "planned_action"


@pytest.mark.parametrize("outcome", ["ERROR", "PENDING", "NONMOVING"])
def test_source_convergence_fails_loudly_on_bad_terminal_outcomes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
) -> None:
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"completed": False, "phase_results": []}))
    ready_calls = 0

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        nonlocal ready_calls
        del env
        if "ready" in argv:
            ready_calls += 1
            return CommandResult(1, json.dumps(_blocked_convergence("git")), "")
        if "current-runtime-dlq" in argv:
            return _ok({"current_count": 0, "current_failures": []})
        if "find-source-run" in argv:
            return _ok({"dbos_workflow_id": None})
        if "enqueue-source" in argv:
            return _ok({"dbos_workflow_id": "wf-git"})
        if "debug-workflow" in argv:
            return _ok({"workflow_id": "wf-git", "status": ("SUCCESS" if outcome == "NONMOVING" else outcome)})
        raise AssertionError(argv)

    dep_dir = tmp_path / "deployment"
    dep_dir.mkdir()
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(deployment_dir=dep_dir),
    )
    # Only the PENDING arm may run against a deadline that can actually
    # elapse — its assertion IS the deadline message. The ERROR and
    # NONMOVING arms need their terminal/watermark checks to win the
    # race against the clock, and 0.05s lost that race on a loaded CI
    # runner: the durable-deadline branch fired before the mocked
    # workflow's status was ever read ('source recovery exceeded its
    # durable deadline' instead of 'watermark did not move'; flaked on
    # PR #925's fast lane, passed on rerun).
    action_timeout = "0.05" if outcome == "PENDING" else "30"
    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="dep", skip_backup=True),
        dsn="postgres://test",
        resolution={"target": {"identity": "isolated"}},
        env={
            "OKG_DSN": "postgres://test",
            "OKG_DEPLOYMENT_SOURCE_ACTION_TIMEOUT_SECONDS": action_timeout,
            "OKG_DEPLOYMENT_SOURCE_POLL_SECONDS": "0.001",
        },
        state_file=state_file,
        backup_dir=tmp_path,
        command_runner=runner,
    )
    result = _phase_source_convergence(ctx)
    assert result["ok"] is False
    if outcome == "ERROR":
        assert result["terminal_status"] == "ERROR"
    elif outcome == "PENDING":
        assert "deadline" in result["reason"]
    else:
        assert "watermark did not move" in result["reason"]
        assert ready_calls == 2


def test_source_convergence_recovers_enqueued_work_after_checkpoint_crash(
    tmp_path: Path,
) -> None:
    state_file = tmp_path / "state.json"
    run_key = "deploy-recovery:target:git"
    state_file.write_text(json.dumps({
        "completed": False,
        "phase_results": [],
        "source_recovery_workflows": [{
            "source_name": "git",
            "action_kind": "completed_scope_reconcile",
            "run_mode": "scope_complete",
            "run_key": run_key,
            "workflow_id": None,
            "supersedes_workflow_id": "failed-git",
            "enqueued_at": None,
            "terminal_status": None,
        }],
    }))
    ready_payloads = [
        _blocked_convergence("git"),
        {"ok": True, "source_sync_status": {"convergence": {
            "ok": True, "state": "ready",
        }}},
    ]
    enqueues = 0

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        nonlocal enqueues
        del env
        if "ready" in argv:
            payload = ready_payloads.pop(0)
            return CommandResult(0 if payload["ok"] else 1, json.dumps(payload), "")
        if "find-source-run" in argv:
            assert argv[argv.index("--run-key") + 1] == run_key
            return _ok({"dbos_workflow_id": "wf-git"})
        if "enqueue-source" in argv:
            enqueues += 1
            return _ok({"dbos_workflow_id": "duplicate"})
        if "debug-workflow" in argv:
            assert argv[-1] == "wf-git"
            return _ok({"workflow_id": "wf-git", "status": "SUCCESS"})
        raise AssertionError(argv)

    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="dep", skip_backup=True),
        dsn="postgres://test",
        resolution={"target": {"identity": "isolated"}},
        env={"OKG_DSN": "postgres://test"},
        state_file=state_file,
        backup_dir=tmp_path,
        command_runner=runner,
        data={"source_recovery_workflows": json.loads(state_file.read_text())[
            "source_recovery_workflows"
        ]},
    )

    result = _phase_source_convergence(ctx)
    assert result["ok"] is True
    assert enqueues == 0
    assert result["workflows"][0]["workflow_id"] == "wf-git"
    assert result["workflows"][0]["terminal_status"] == "SUCCESS"


def test_source_convergence_accepts_clean_general_readiness(
    tmp_path: Path,
) -> None:
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({
        "completed": False,
        "phase_results": [],
    }))

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        assert "ready" in argv
        return _ok({"ok": True, "status": "clean", "blockers": []})

    result = _phase_source_convergence(UpgradeContext(
        options=UpgradeOptions(
            deployment="dep",
            readiness_profile="general",
            skip_backup=True,
        ),
        dsn="postgres://test",
        resolution={"target": {"identity": "isolated"}},
        env={"OKG_DSN": "postgres://test"},
        state_file=state_file,
        backup_dir=tmp_path,
        command_runner=runner,
    ))

    assert result["ok"] is True
    assert result["state"] == "ready"
    assert result["observations"] == [{
        "ok": True,
        "state": "ready",
        "authority": "clean_deployment_readiness",
        "recovery_plan": {"actions": []},
    }]


def test_source_convergence_observes_readiness_owned_active_work(
    tmp_path: Path,
) -> None:
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"completed": False, "phase_results": []}))
    ready = [
        {"ok": False, "source_sync_status": {"convergence": {
            "ok": False,
            "state": "blocked",
            "recovery_plan": {"actions": [{
                "action_kind": "observe_active_completed_scope_recovery",
                "source_name": "git",
                "run_mode": "scope_complete",
                "workflow_id": "wf-active",
            }]},
        }}},
        {"ok": True, "source_sync_status": {"convergence": {
            "ok": True, "state": "ready",
        }}},
    ]

    def runner(argv, env):
        del env
        if "ready" in argv:
            payload = ready.pop(0)
            return CommandResult(0 if payload["ok"] else 1, json.dumps(payload), "")
        if "debug-workflow" in argv:
            assert argv[-1] == "wf-active"
            return _ok({"workflow_id": "wf-active", "status": "SUCCESS"})
        if "enqueue-source" in argv:
            raise AssertionError("active work must not be enqueued again")
        raise AssertionError(argv)

    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="dep"),
        dsn="postgres://test",
        resolution={"target": {"identity": "isolated"}},
        env={"OKG_DSN": "postgres://test"},
        state_file=state_file,
        backup_dir=tmp_path,
        command_runner=runner,
    )
    result = _phase_source_convergence(ctx)
    assert result["ok"] is True
    assert result["workflows"][0]["workflow_id"] == "wf-active"


def test_source_convergence_global_deadline_survives_resume(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({
        "completed": False,
        "phase_results": [],
        "source_convergence_deadline_epoch": 1.0,
    }))
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        commands.append(argv)
        assert "ready" in argv
        return CommandResult(
            1,
            json.dumps(_blocked_convergence("git")),
            "",
        )

    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="dep"),
        dsn="postgres://test",
        resolution={"target": {"identity": "isolated"}},
        env={"OKG_DSN": "postgres://test"},
        state_file=state_file,
        backup_dir=tmp_path,
        command_runner=runner,
        data={"source_convergence_deadline_epoch": 1.0},
    )
    result = _phase_source_convergence(ctx)
    assert result["ok"] is False
    assert "global deployment deadline" in result["reason"]
    assert result["observations"][0]["state"] == "blocked"
    assert len(commands) == 1


def test_source_convergence_accepts_live_ready_after_resume_deadline(
    tmp_path: Path,
) -> None:
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({
        "completed": False,
        "phase_results": [],
        "source_convergence_deadline_epoch": 1.0,
    }))
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        commands.append(argv)
        assert "ready" in argv
        return _ok({
            "ok": True,
            "source_sync_status": {
                "convergence": {"ok": True, "state": "ready"}
            },
        })

    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="dep"),
        dsn="postgres://test",
        resolution={"target": {"identity": "isolated"}},
        env={"OKG_DSN": "postgres://test"},
        state_file=state_file,
        backup_dir=tmp_path,
        command_runner=runner,
        data={"source_convergence_deadline_epoch": 1.0},
    )

    result = _phase_source_convergence(ctx)

    assert result["ok"] is True
    assert result["state"] == "ready"
    assert len(commands) == 1
    assert json.loads(state_file.read_text())[
        "source_convergence_deadline_epoch"
    ] == 1.0


def test_attribute_read_model_phase_refuses_interval_opt_out() -> None:
    result = _phase_attribute_read_model(
        SimpleNamespace(
            options=SimpleNamespace(plan_only=False),
            env={"OKG_IVM_INTERVAL_VIEW": "0"},
        )
    )
    assert result["ok"] is False
    assert result["reason"] == "interval-backed attribute view is disabled"


def test_attribute_read_model_phase_rejects_malformed_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._run_okg",
        lambda ctx, argv: CommandResult(0, "{}", ""),
    )
    result = _phase_attribute_read_model(
        SimpleNamespace(
            options=SimpleNamespace(plan_only=False, deployment="dep"),
            env={"OKG_IVM_INTERVAL_VIEW": "1"},
        )
    )
    assert result["ok"] is False
    assert result["reason"] == "attribute read-model repair did not report success"


def test_stop_services_invokes_applied_down_and_propagates_failure() -> None:
    calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        calls.append(argv)
        assert env == {"OKG_DSN": "postgres://okg/test"}
        return CommandResult(1, '{"ok": false}', "systemctl stop failed")

    result = _phase_stop_services(SimpleNamespace(
        options=SimpleNamespace(deployment="okg-ci", apply_services=True),
        env={"OKG_DSN": "postgres://okg/test"},
        command_runner=runner,
    ))

    assert calls == [[
        *running_okg_argv(),
        "--json",
        "dogfood",
        "down",
        "--deployment",
        "okg-ci",
        "--apply",
    ]]
    assert result["ok"] is False
    assert result["reason"] == "stop services failed"
    assert result["returncode"] == 1


def test_stop_services_phase_accepts_dogfood_already_absent_result() -> None:
    calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        calls.append(argv)
        return _ok({
            "ok": True,
            "action": "down",
            "final_state": "already_absent",
            "process_audit": {"process_count": 0, "status": "not_observed"},
        })

    result = _phase_stop_services(SimpleNamespace(
        options=SimpleNamespace(deployment="cooper", apply_services=True),
        env={},
        command_runner=runner,
    ))

    assert result["ok"] is True, json.dumps(result, indent=2)
    assert result["result"]["final_state"] == "already_absent"
    assert calls[0][-5:] == [
        "dogfood", "down", "--deployment", "cooper", "--apply",
    ]


def test_runtime_quiescence_fences_and_drains_before_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime.admin import DrainResult

    events: list[str] = []
    state_file = tmp_path / "state.json"
    state_file.write_text("{}")
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._runtime_quiescence_required",
        lambda ctx: True,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._runtime_fence_metadata",
        lambda ctx: {"candidate_revision": "candidate"},
    )

    def acquire(**kwargs):
        assert kwargs["promotion_id"] == "deploy-owner"
        events.append("fence")
        return {
            "status": "fenced",
            "promotion_id": "deploy-owner",
            "schedule_snapshot_digest": "sha256:snapshot",
        }

    def drain(**kwargs):
        events.append("drain")
        return DrainResult(
            deployment=kwargs["deployment"],
            drained=True,
            in_flight_at_timeout=0,
            elapsed_seconds=0.01,
            pending_workflows=[],
        )

    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.acquire_promotion_fence",
        acquire,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_drain",
        drain,
    )

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del argv, env
        events.append("stop")
        return _ok({"ok": True, "final_state": "stopped"})

    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="okg-ci"),
        dsn="postgres://test",
        resolution={"target": {"identity": "ok"}},
        env={"OKG_DSN": "postgres://test"},
        state_file=state_file,
        backup_dir=tmp_path,
        command_runner=runner,
        data={"runtime_fence_owner": "deploy-owner"},
    )
    result = _phase_stop_services(ctx)

    assert result["ok"] is True
    assert events == ["fence", "drain", "stop"]
    assert result["runtime_quiescence"]["drain"]["drained"] is True
    persisted = json.loads(state_file.read_text())
    assert persisted["runtime_fence_owner"] == "deploy-owner"
    assert persisted["runtime_fence_snapshot_digest"] == "sha256:snapshot"


def test_runtime_quiescence_drain_failure_reopens_without_stopping(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime.admin import DrainResult

    state_file = tmp_path / "state.json"
    state_file.write_text("{}")
    stops: list[list[str]] = []
    releases: list[dict[str, object]] = []
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._runtime_quiescence_required",
        lambda ctx: True,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._runtime_fence_metadata",
        lambda ctx: {},
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.acquire_promotion_fence",
        lambda **kwargs: {
            "status": "fenced",
            "promotion_id": kwargs["promotion_id"],
            "schedule_snapshot_digest": "sha256:snapshot",
        },
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_drain",
        lambda **kwargs: DrainResult(
            deployment=kwargs["deployment"],
            drained=False,
            in_flight_at_timeout=1,
            elapsed_seconds=10.0,
            pending_workflows=[{"workflow_id": "still-running"}],
        ),
    )

    def release(**kwargs):
        releases.append(kwargs)
        return {
            "status": "released",
            "released": True,
            "schedule_disposition": kwargs["schedule_disposition"],
        }

    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.release_promotion_fence",
        release,
    )
    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="okg-ci"),
        dsn="postgres://test",
        resolution={"target": {"identity": "ok"}},
        env={"OKG_DSN": "postgres://test"},
        state_file=state_file,
        backup_dir=tmp_path,
        command_runner=lambda argv, env: (
            stops.append(argv) or _ok({"ok": True})
        ),
        data={"runtime_fence_owner": "deploy-owner"},
    )
    result = _phase_stop_services(ctx)

    assert result["ok"] is False
    assert "did not drain" in result["reason"]
    assert stops == []
    assert releases[0]["schedule_disposition"] == "restore"


def test_runtime_reopen_keeps_candidate_schedules_after_bound_readiness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    releases: list[dict[str, object]] = []
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._runtime_quiescence_required",
        lambda ctx: True,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.promotion_fence_status",
        lambda **kwargs: {
            "status": "fenced",
            "promotion_id": "deploy-owner",
            "fenced_at": datetime.now(timezone.utc) - timedelta(minutes=20),
        },
    )

    def release(**kwargs):
        releases.append(kwargs)
        return {
            "status": "released",
            "released": True,
            "durable_release_receipt": True,
            "schedule_disposition": kwargs["schedule_disposition"],
            "candidate_schedule_snapshot_digest": "sha256:candidate-schedules",
            "receipt_digest": "sha256:release",
        }

    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.release_promotion_fence",
        release,
    )

    ready_commands: list[list[str]] = []
    ready_envs: list[dict[str, str]] = []
    state_file = tmp_path / "operation.json"
    state_file.write_text("{}", encoding="utf-8")

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        if "enqueue-fenced-recovery" in argv:
            assert argv[argv.index("--promotion-id") + 1] == "deploy-owner"
            assert argv[argv.index("--deduplication-id") + 1].startswith(
                "deployment-readiness-doctor:"
            )
            return _ok({"workflow_id": "wf-doctor"})
        if "debug-workflow" in argv:
            assert argv[-1] == "wf-doctor"
            return _ok({"workflow_id": "wf-doctor", "status": "SUCCESS"})
        if "release" in argv:
            return _ok({"ok": True})
        ready_commands.append(argv)
        ready_envs.append(env)
        return _ok({
            "ok": True,
            "generation": {"generation_id": "smoke-generation"},
        })

    monkeypatch.setattr(
        du,
        "_collect_ci_deployment_final_proof",
        lambda *args, **kwargs: {"ok": True, "checks": {"doctor": True}},
    )
    result = du._phase_readiness(SimpleNamespace(
        options=UpgradeOptions(
            deployment="dep",
            readiness_profile="ci",
        ),
        dsn="postgres://test",
        env={},
        state_file=state_file,
        command_runner=runner,
        data={
            "smoke_publish": _published("smoke-generation"),
            "runtime_fence_owner": "deploy-owner",
        },
    ))

    assert result["ok"] is True
    assert result["runtime_witness"]["doctor_workflow_id"] == "wf-doctor"
    assert result["runtime_witness"]["max_producer_age_seconds"] >= 30 * 60
    assert len(ready_commands) == 1
    # renamed-flag compat (decision 310): the child runs the checked-out
    # release pin, which may predate `--checks`, so it gets the spelling old
    # and new code both accept, with the rename warning silenced.
    assert ready_commands[0][-7:] == [
        "deployment",
        "ready",
        "dep",
        "--profile",
        "ci",
        "--max-producer-age-seconds",
        str(result["runtime_witness"]["max_producer_age_seconds"]),
    ]
    assert ready_envs[0]["OKG_RENAMED_FLAG_WARNINGS"] == "0"
    persisted = json.loads(state_file.read_text())
    assert persisted["runtime_readiness_witness"]["status"] == "success"
    assert persisted["runtime_readiness_witness"]["workflow_id"] == "wf-doctor"
    assert result["runtime_reopen"]["receipt_digest"] == "sha256:release"
    assert releases == [{
        "deployment": "dep",
        "dsn": "postgres://test",
        "promotion_id": "deploy-owner",
        "schedule_disposition": "keep",
    }]


def test_runtime_readiness_witness_reuses_pending_workflow_on_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.promotion_fence_status",
        lambda **kwargs: {
            "status": "fenced",
            "promotion_id": "deploy-owner",
            "fenced_at": datetime.now(timezone.utc) - timedelta(minutes=20),
        },
    )
    waits = iter([
        {
            "ok": False,
            "reason": "source recovery workflow exceeded its action deadline",
            "workflow_id": "wf-doctor",
        },
        {
            "ok": True,
            "workflow_id": "wf-doctor",
            "report": {"workflow_id": "wf-doctor", "status": "SUCCESS"},
        },
    ])
    monkeypatch.setattr(
        du,
        "_wait_for_source_recovery_workflow",
        lambda *args, **kwargs: next(waits),
    )
    enqueues: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        enqueues.append(argv)
        return _ok({"workflow_id": "wf-doctor"})

    state_file = tmp_path / "operation.json"
    state_file.write_text("{}", encoding="utf-8")
    ctx = SimpleNamespace(
        options=UpgradeOptions(deployment="dep"),
        dsn="postgres://test",
        env={},
        state_file=state_file,
        command_runner=runner,
        data={"runtime_fence_owner": "deploy-owner"},
    )

    first = du._prepare_runtime_readiness_witness(ctx)
    second = du._prepare_runtime_readiness_witness(ctx)

    assert first["ok"] is False
    assert second["ok"] is True
    assert second["doctor_workflow_id"] == "wf-doctor"
    assert len(enqueues) == 1


def test_runtime_readiness_witness_refuses_persisted_owner_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.promotion_fence_status",
        lambda **kwargs: {
            "status": "fenced",
            "promotion_id": "deploy-owner",
            "fenced_at": datetime.now(timezone.utc),
        },
    )
    state_file = tmp_path / "operation.json"
    state_file.write_text("{}", encoding="utf-8")
    ctx = SimpleNamespace(
        options=UpgradeOptions(deployment="dep"),
        dsn="postgres://test",
        env={},
        state_file=state_file,
        command_runner=lambda argv, env: pytest.fail(
            "mismatched persisted authority must not enqueue"
        ),
        data={
            "runtime_fence_owner": "deploy-owner",
            "runtime_readiness_witness": {
                "promotion_id": "different-owner",
                "deduplication_id": "wrong-deduplication-id",
            },
        },
    )

    observed = du._prepare_runtime_readiness_witness(ctx)

    assert observed["ok"] is False
    assert "different authority" in observed["reason"]


def test_resume_accepts_pending_doctor_from_persisted_witness_intent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.runtime.admin import DrainResult

    snapshot = [{"schedule_id": "poll"}]
    expected_metadata = {
        "schema": "okg.deployment_runtime_quiescence.v1",
        "deployment": "okg-ci",
        "candidate_revision": "a" * 40,
        "target_key_digest": "sha256:target",
        "lifecycle_contract_digest": "sha256:contract",
        "lifecycle_plan_digest": "sha256:plan",
        "bootstrap_quiescence": False,
    }
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._runtime_fence_metadata",
        lambda ctx: expected_metadata,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.promotion_fence_status",
        lambda **kwargs: {
            "status": "fenced",
            "promotion_id": "deploy-owner",
            "schedule_snapshot": snapshot,
            "metadata": expected_metadata,
        },
    )
    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="okg-ci"),
        dsn="postgres://test",
        resolution={"target": {"identity": "ok"}},
        env={},
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path,
        command_runner=lambda argv, env: _ok(),
        data={
            "runtime_fence_owner": "deploy-owner",
            "runtime_fence_snapshot_digest": (
                du._runtime_schedule_snapshot_digest(snapshot)
            ),
            "source_recovery_workflows": [],
        },
    )
    deduplication_id = du._runtime_readiness_witness_deduplication_id(
        ctx,
        owner="deploy-owner",
    )
    ctx.data["runtime_readiness_witness"] = {
        "promotion_id": "deploy-owner",
        "deduplication_id": deduplication_id,
        "status": "intent",
    }
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_drain",
        lambda **kwargs: DrainResult(
            deployment=kwargs["deployment"],
            drained=False,
            in_flight_at_timeout=1,
            elapsed_seconds=0.01,
            pending_workflows=[{
                "workflow_id": "wf-doctor",
                "name": "doctor_workflow",
                "deduplication_id": deduplication_id,
            }],
        ),
    )

    observed = du._revalidate_runtime_fence_resume(
        ctx,
        completed={"stop_services", "smoke_publish"},
    )

    assert observed["ok"] is True
    assert observed["checks"]["queue_drained_or_owner_bound"] is True

    ctx.data["runtime_readiness_witness"] = {
        **ctx.data["runtime_readiness_witness"],
        "promotion_id": "different-owner",
    }
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.mark_promotion_recovery_blocked",
        lambda **kwargs: {"status": "recovery_blocked"},
    )
    with pytest.raises(SubstrateError, match="recovery is blocked"):
        du._revalidate_runtime_fence_resume(
            ctx,
            completed={"stop_services", "smoke_publish"},
        )


def test_nomos_policy_inventory_includes_catalog_restamp_lineage() -> None:
    from okg.substrate import deployment_upgrade as du

    source_ids = du._nomos_policy_source_ids(
        {"sources": {}},
        deployment="dep",
    )

    assert "_catalog:restamp" in source_ids


def test_target_nomos_inventory_uses_deployment_managed_baseline_spec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du
    import okg.substrate.nomos as nomos
    import okg.substrate.nomos.baseline as baseline
    import psycopg

    observed: dict[str, object] = {}

    def desired(*, deployment, deployment_dir):
        observed.update({
            "deployment": deployment,
            "deployment_dir": deployment_dir,
        })
        return {
            "policy_fingerprint": "sha256:deployment-managed-policy",
            "config_fingerprint": "sha256:deployment-managed-config",
        }

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            del args

    monkeypatch.setattr(baseline, "deployment_baseline_spec", desired)
    monkeypatch.setattr(psycopg, "connect", lambda *args, **kwargs: Connection())
    monkeypatch.setattr(
        nomos,
        "load_active_policy_bundle",
        lambda *args, **kwargs: SimpleNamespace(
            policy_fingerprint="sha256:deployment-managed-policy"
        ),
    )
    monkeypatch.setattr(
        nomos,
        "load_active_config_set",
        lambda *args, **kwargs: SimpleNamespace(
            config_fingerprint="sha256:deployment-managed-config"
        ),
    )
    ctx = SimpleNamespace(
        options=UpgradeOptions(deployment="source-bound"),
        dsn="postgres://unused",
    )

    result = du._target_nomos_inventory(
        ctx,
        raw_manifest={"nomos": {"runtime_enforcement": "audit"}},
        raw_registry={"sources": {}},
        deployment_dir=tmp_path,
    )

    assert observed == {
        "deployment": "source-bound",
        "deployment_dir": tmp_path,
    }
    assert result["unchanged"] is True
    assert result["expected_policy_fingerprint"] == (
        "sha256:deployment-managed-policy"
    )


def test_candidate_adopts_only_exact_bootstrap_quiescence_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    expected = {
        "schema": "okg.deployment_runtime_quiescence.v1",
        "deployment": "okg-ci",
        "candidate_revision": "a" * 40,
        "target_key_digest": "target",
        "lifecycle_contract_digest": "sha256:contract",
        "lifecycle_plan_digest": "sha256:plan",
        "bootstrap_quiescence": False,
    }
    status = {
        "status": "fenced",
        "promotion_id": "bootstrap-owner",
        "metadata": {
            **expected,
            "bootstrap_quiescence": True,
        },
    }
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._runtime_fence_metadata",
        lambda ctx: expected,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.promotion_fence_status",
        lambda **kwargs: status,
    )
    ctx = SimpleNamespace(
        options=UpgradeOptions(deployment="okg-ci"),
        dsn="postgres://test",
        data={},
    )

    owner = du._initialize_runtime_fence_owner(ctx, {})

    assert owner == "bootstrap-owner"
    assert ctx.data["adopted_bootstrap_quiescence"] is True
    status["metadata"]["candidate_revision"] = "b" * 40
    with pytest.raises(SubstrateError, match="exact candidate-bound bootstrap"):
        du._initialize_runtime_fence_owner(ctx, {})


@pytest.mark.parametrize(
    "option_overrides",
    [
        {},
        {"plan_only": True},
        {"apply_services": False},
        {"independent_backup_verify": True},
    ],
)
def test_reset_state_refuses_active_runtime_fence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    option_overrides: dict[str, object],
) -> None:
    from okg.substrate import deployment_upgrade as du

    state_file = tmp_path / "state.json"
    state_file.write_text('{"completed": false}\n')
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._target_state_key",
        lambda ctx: {"code_revision": "candidate"},
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.promotion_fence_status",
        lambda **kwargs: {
            "status": "fenced",
            "promotion_id": "active-owner",
        },
    )
    ctx = UpgradeContext(
        options=UpgradeOptions(
            deployment="okg-ci",
            reset_state=True,
            **option_overrides,
        ),
        dsn="postgres://test",
        resolution={"target": {}},
        env={},
        state_file=state_file,
        backup_dir=tmp_path,
        command_runner=lambda argv, env: _ok(),
        data={"runtime_enabled": True},
    )

    with pytest.raises(SubstrateError, match="cannot reset deployment state"):
        du._load_or_initialize_state(ctx)

    assert state_file.exists()


def test_resume_revalidates_live_owner_snapshot_binding_and_queue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.runtime.admin import DrainResult

    snapshot = [{"schedule_id": "predecessor"}]
    snapshot_digest = du._runtime_schedule_snapshot_digest(snapshot)
    expected_metadata = {
        "schema": "okg.deployment_runtime_quiescence.v1",
        "deployment": "okg-ci",
        "candidate_revision": "a" * 40,
        "target_key_digest": "sha256:target",
        "lifecycle_contract_digest": "sha256:contract",
        "lifecycle_plan_digest": "sha256:plan",
        "bootstrap_quiescence": False,
    }
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._runtime_fence_metadata",
        lambda ctx: expected_metadata,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.promotion_fence_status",
        lambda **kwargs: {
            "status": "fenced",
            "promotion_id": "deploy-owner",
            "schedule_snapshot": snapshot,
            "metadata": expected_metadata,
        },
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_drain",
        lambda **kwargs: DrainResult(
            deployment=kwargs["deployment"],
            drained=True,
            in_flight_at_timeout=0,
            elapsed_seconds=0.01,
            pending_workflows=[],
        ),
    )
    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="okg-ci"),
        dsn="postgres://test",
        resolution={"target": {"identity": "ok"}},
        env={},
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path,
        command_runner=lambda argv, env: _ok(),
        data={
            "runtime_fence_owner": "deploy-owner",
            "runtime_fence_snapshot_digest": snapshot_digest,
            "source_recovery_workflows": [],
        },
    )

    observed = du._revalidate_runtime_fence_resume(
        ctx,
        completed={"stop_services", "migrate"},
    )

    assert observed["ok"] is True
    assert observed["checks"]["snapshot_digest"] is True
    assert observed["queue"]["drained"] is True


def test_resume_accepts_durable_release_before_readiness_phase_persisted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    snapshot_digest = "sha256:predecessor"
    expected_metadata = {
        "schema": "okg.deployment_runtime_quiescence.v1",
        "deployment": "okg-ci",
        "candidate_revision": "a" * 40,
        "target_key_digest": "sha256:target",
        "lifecycle_contract_digest": "sha256:contract",
        "lifecycle_plan_digest": "sha256:plan",
        "bootstrap_quiescence": False,
        "release_receipt": {
            "durable_release_receipt": True,
            "promotion_id": "deploy-owner",
            "schedule_snapshot_digest": snapshot_digest,
        },
    }
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._runtime_fence_metadata",
        lambda ctx: expected_metadata,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.promotion_fence_status",
        lambda **kwargs: {
            "status": "released",
            "promotion_id": "deploy-owner",
            "schedule_snapshot": [{"schedule_id": "predecessor"}],
            "metadata": expected_metadata,
        },
    )
    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="okg-ci"),
        dsn="postgres://test",
        resolution={"target": {"identity": "ok"}},
        env={},
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path,
        command_runner=lambda argv, env: _ok(),
        data={
            "runtime_fence_owner": "deploy-owner",
            "runtime_fence_snapshot_digest": snapshot_digest,
        },
    )

    observed = du._revalidate_runtime_fence_resume(
        ctx,
        completed={"stop_services", "smoke_publish"},
    )

    assert observed["ok"] is True
    assert observed["released_before_phase_persistence"] is True


def test_resume_blocks_missing_fence_after_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.runtime.admin import DrainResult

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._runtime_fence_metadata",
        lambda ctx: {
            "schema": "okg.deployment_runtime_quiescence.v1",
            "deployment": "okg-ci",
            "candidate_revision": "a" * 40,
            "lifecycle_contract_digest": "sha256:contract",
            "lifecycle_plan_digest": "sha256:plan",
        },
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.promotion_fence_status",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_drain",
        lambda **kwargs: DrainResult(
            deployment=kwargs["deployment"],
            drained=True,
            in_flight_at_timeout=0,
            elapsed_seconds=0.01,
            pending_workflows=[],
        ),
    )
    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="okg-ci"),
        dsn="postgres://test",
        resolution={"target": {"identity": "ok"}},
        env={},
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path,
        command_runner=lambda argv, env: _ok(),
        data={
            "runtime_fence_owner": "deploy-owner",
            "runtime_fence_snapshot_digest": "sha256:missing",
        },
    )

    with pytest.raises(SubstrateError, match="recovery is blocked"):
        du._revalidate_runtime_fence_resume(
            ctx,
            completed={"stop_services", "migrate"},
        )


def test_source_sync_compaction_requires_owner_promotion_permit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.source_sync import SourceSyncCompaction, SourceSyncJob
    from okg.substrate.source_sync_enqueue import (
        enqueue_compacted_source_sync_via_dbos,
    )

    events: list[tuple[str, str | None]] = []

    @contextmanager
    def permit(**kwargs):
        events.append(("permit", kwargs.get("promotion_id")))
        yield

    def compact(**kwargs):
        del kwargs
        events.append(("compact", None))
        return "wf-compacted"

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.promotion_enqueue_permit",
        permit,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue._compact_dbos_queued_source_sync",
        compact,
    )
    workflow_id = enqueue_compacted_source_sync_via_dbos(
        deployment="dep",
        dsn="postgres://test",
        job=SourceSyncJob(
            deployment="dep",
            sources=("git",),
            run_mode="scope_complete",
        ),
        deduplication_id="watch:git:1",
        compaction=SourceSyncCompaction(window_seconds=30),
        promotion_id="deploy-owner",
    )

    assert workflow_id == "wf-compacted"
    assert events == [
        ("permit", "deploy-owner"),
        ("compact", None),
    ]


def test_readiness_phase_refuses_generation_other_than_smoke() -> None:
    from okg.substrate import deployment_upgrade as du

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if "release" in argv:
            return _ok({"ok": True})
        return _ok({
            "ok": True,
            "generation": {"generation_id": "different-generation"},
        })

    result = du._phase_readiness(SimpleNamespace(
        options=SimpleNamespace(
            plan_only=False,
            deployment="dep",
            readiness_profile="dogfood",
        ),
        env={},
        command_runner=runner,
        data={"smoke_publish": _published("smoke-generation")},
    ))

    assert result["ok"] is False
    assert result["reason"] == (
        "deployment readiness does not match smoke generation"
    )
    assert result["smoke_generation_id"] == "smoke-generation"
    assert result["readiness_generation_id"] == "different-generation"


def test_readiness_phase_binds_caught_up_noop_to_head_generation() -> None:
    # A caught-up smoke whose commit publish SKIPPED (no_source_work, no generation
    # minted) must not fail readiness on smoke_generation=None: it proved the head
    # is current, so readiness binds the proof to the head generation.
    from okg.substrate import deployment_upgrade as du

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if "release" in argv:
            return _ok({"ok": True})
        return _ok({
            "ok": True,
            "generation": {"generation_id": "head-generation"},
        })

    result = du._phase_readiness(SimpleNamespace(
        options=SimpleNamespace(
            plan_only=False,
            deployment="dep",
            readiness_profile="dogfood",
        ),
        env={},
        command_runner=runner,
        data={"smoke_publish": _no_source_work_skip_payload()},
    ))

    assert result["ok"] is True, result
    assert result["smoke_generation_id"] == "head-generation"
    assert result["readiness_generation_id"] == "head-generation"


def _planned_sync_code(ctx) -> dict:
    """`_phase_sync_code` stub that reports a PLAN-only preview (skip)."""
    return {
        "ok": True,
        "planned": "git fetch --all --prune && git checkout <pin>",
        "skipped": "code sync skipped (--plan / --no-sync-code)",
    }


def _stub_verified_backup_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep tests of later phases focused without exercising real backup I/O."""
    _stub_dependency_sync_checkout(monkeypatch)

    def backup(ctx) -> dict:
        ctx.data["safety_plan"] = {
            "schema": "okg.deployment_safety_plan.v1",
            "external_effects": {"present": False},
            "restore_point": {
                "source": "unit_verified_boundary",
                "verified": True,
            },
            "confirmation": {
                "required": False,
                "digest": "sha256:unit-verified-boundary",
                "provided": False,
                "exact": False,
            },
        }
        result = {
            "ok": True,
            "verification_mode": "current_full_restore",
            "archive_revalidated": True,
        }
        ctx.data["backup_result"] = result
        return result

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_backup",
        backup,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._revalidate_backup_before_mutation",
        lambda ctx: {
            "ok": True,
            "verification_mode": "current_full_restore",
            "archive_revalidated": True,
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._revalidate_effect_authority",
        lambda ctx, backup: {
            "ok": True,
            "target_revision_stable": True,
            "effect_inventory": {
                "smoke": {"selected_source_digest": "sha256:test-smoke"}
            },
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._revalidate_resumed_safety_contract",
        lambda *_args, **_kwargs: None,
    )


@pytest.fixture(autouse=True)
def _stub_extension_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    # These orchestration tests stub database mutations. The real guarded
    # reclaim and one-line catalog-edit regression live in test_up_catalog_reclaim.
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._reclaim_changed_catalog",
        lambda ctx, deployment_dir: {"reclaimed": False},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._ensure_upgrade_extensions",
        lambda ctx: {"created": [], "already_present": []},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_nomos_baseline",
        lambda ctx: {"ok": True, "skipped": "unit"},
    )
    # #1791: a refused backup admission reads the deployment's publish state.
    # Hermetic tests carry fake DSNs; keep them off the network. The tests
    # that prove the guidance override this stub with their own plan.
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._publish_recovery_plan",
        lambda ctx: {
            "schema": "okg.publish_recovery_plan.v1",
            "ok": False,
            "state": "unknown",
            "okg_up_applies": False,
            "recovery_command": None,
            "error": {"type": "HermeticStub", "message": "no database in unit tests"},
        },
    )
    # The smoke-publish phase now applies the uniform real-publish gate, which
    # queries DBOS-backed runtime health for the DLQ. Hermetic upgrade tests
    # stub the command runner (no real DB), so stub the probe to "clean".
    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count",
        lambda dsn, **_kwargs: (0, {}),
    )
    # Executable identity hashes the real pg_dump/pg_restore binaries on
    # disk; hermetic tests must not depend on the host having them (CI
    # fast lanes do not). The values mirror the stubbed compatibility
    # snapshot so receipt-bound revalidation stays self-consistent.
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._backup_tool_executable_identity",
        lambda tool: {"sha256": "dump" if tool == "pg_dump" else "restore"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._verify_backup_restore",
        lambda ctx, path, **_: {
            "ok": True,
            "restore_verified": True,
            "schema_ok": True,
            "ownership_ok": True,
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._backup_headroom_preflight",
        lambda ctx, **kwargs: {"ok": True, "refuse": False, "status": "ok"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._backup_post_dump_headroom_preflight",
        lambda ctx: {"ok": True, "refuse": False, "status": "ok"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._restore_headroom_preflight",
        lambda ctx, policy, **kwargs: {
            "ok": True,
            "refuse": False,
            "status": "ok",
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._backup_compatibility_snapshot",
        lambda ctx, toolchain: {
            "target": {
                "deployment": ctx.options.deployment,
                "host": "host",
                "port": 5432,
                "database": "db",
            },
            "database_owner": {
                "deployment": ctx.options.deployment,
                "uuid": "test-owner",
                "schema_fingerprint": "test-schema",
            },
            "cluster_system_identifier": "test-cluster",
            "server_version": "170010",
            "extension_versions": [],
            "schema_migrations_digest": "sha256:test-schema-migrations",
            "schema_catalog_digest": "sha256:test-schema-catalog",
            "candidate_plan_digest": "sha256:test-plan",
            "archive_format": "postgresql-custom",
            "timescaledb": {
                "extension_version": None,
                "hypertables": [],
                "continuous_aggregates": [],
                "hypertable_chunk_counts": [],
            },
            "dump_tool_major": 17,
            "restore_tool_major": 17,
            "backup_tool_executable_identity": {
                "pg_dump": {"sha256": "dump"},
                "pg_restore": {"sha256": "restore"},
            },
        },
    )
    if request.node.get_closest_marker("postgres") is None:
        def closed_inventory(ctx, expected_digest=None):
            from okg.substrate import deployment_upgrade as du

            checks = _closed_effect_checks(
                effect_digest_stable=(
                    expected_digest in (None, "", "sha256:test-effect")
                )
            )
            return checks, (), {
                "schema": "okg.deployment_effect_inventory.v1",
                "target_revision": du._upgrade_state_revision(ctx) or "test-revision",
                "effect_digest": "sha256:test-effect",
                "execution_checkout_exact": True,
                "smoke": {"selected_source_digest": "sha256:test-smoke"},
                "checks": checks.to_dict(),
            }

        monkeypatch.setattr(
            "okg.substrate.deployment_upgrade._collect_effect_inventory",
            closed_inventory,
        )


def _backup_command(argv: list[str]) -> CommandResult | None:
    """Serve a valid PostgreSQL-17 backup toolchain in hermetic tests."""
    if argv[:2] in (["pg_dump", "--version"], ["pg_restore", "--version"]):
        return CommandResult(0, f"{argv[0]} (PostgreSQL) 17.10\n", "")
    if argv[:2] == ["pg_restore", "--list"]:
        return CommandResult(0, "; archive listing\n", "")
    if argv and argv[0] == "pg_dump":
        path = Path(argv[argv.index("--file") + 1])
        path.write_bytes(b"valid custom archive")
        return CommandResult(0, "", "")
    return None


def _routine_manifest(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        deployment_dir=tmp_path / "dep-root",
        release={"channel": "dev"},
        nomos={
            "deployment_posture": {
                "deployment_class": "dogfood_workspace",
            }
        },
        deployment_up={
            "backup": {
                "verification_policy": "restore_drill",
                "restore_drill_max_age_seconds": 3600,
                "restore_volume": {"kind": "docker_data_root"},
                "restore_peak_factor": 1.25,
                "max_parallel_restore_jobs": 4,
                "progress_interval_seconds": 0.01,
                "budgets_seconds": {
                    "dump": 1200,
                    "backup": 1800,
                    "total": 2400,
                },
            }
        },
    )


def _full_restore_manifest(deployment_dir: Path) -> SimpleNamespace:
    return SimpleNamespace(
        deployment_dir=deployment_dir,
        release={"channel": "prod"},
        nomos={"deployment_posture": {"deployment_class": "production"}},
        deployment_up={
            "backup": {
                "verification_policy": "current_full_restore",
                "restore_volume": {"kind": "docker_data_root"},
                "restore_peak_factor": 1.25,
            }
        },
    )


def _risk_admitted_manifest(
    deployment_dir: Path,
    *,
    release_channel: str = "dev",
    deployment_class: str = "dogfood_workspace",
) -> SimpleNamespace:
    return SimpleNamespace(
        deployment_dir=deployment_dir,
        release={"channel": release_channel},
        nomos={"deployment_posture": {"deployment_class": deployment_class}},
        deployment_up={
            "backup": {
                "verification_policy": "risk_admitted",
                "disaster_recovery_archive_max_age_seconds": 86400,
                "disaster_recovery_restore_max_age_seconds": 604800,
                "recovery_point_objective_seconds": 86400,
                "restore_volume": {"kind": "docker_data_root"},
                "restore_peak_factor": 1.25,
                "max_parallel_restore_jobs": 4,
                "progress_interval_seconds": 0.01,
                "budgets_seconds": {
                    "dump": 1200,
                    "backup": 1800,
                    "total": 2400,
                },
            }
        },
    )


def _closed_effect_checks(**overrides):
    from okg.substrate.deployment_backup_policy import EffectInventoryChecks

    values = {
        "target_revision_pinned": True,
        "planner_trusted": True,
        "plan_readable": True,
        "no_pending_migrations": True,
        "no_extension_changes": True,
        "catalog_unchanged": True,
        "nomos_unchanged": True,
        "smoke_source_versioned": True,
        "attribute_read_model_rebuildable": True,
        "no_external_effects": True,
        "effect_digest_stable": True,
    }
    values.update(overrides)
    return EffectInventoryChecks(**values)


def _fresh_recovery_checks(**overrides):
    from okg.substrate.deployment_backup_policy import DisasterRecoveryChecks

    values = {
        "typed_schema": True,
        "deployment_scoped_path": True,
        "private_mode": True,
        "current_owner": True,
        "archive_digest_bound": True,
        "same_archive_restored": True,
        "archive_fresh": True,
        "restore_proof_fresh": True,
        "deployment_equal": True,
        "host_equal": True,
        "port_equal": True,
        "database_equal": True,
        "database_owner_equal": True,
        "cluster_system_identifier_equal": True,
        "server_version_equal": True,
        "dump_tool_major_equal": True,
        "restore_tool_major_equal": True,
        "backup_tool_executable_identity_equal": True,
        "extension_versions_equal": True,
        "timescale_restore_proven_or_risk_admitted": True,
        "schema_lineage_equal": True,
        "recovery_point_objective_declared": True,
    }
    values.update(overrides)
    return DisasterRecoveryChecks(**values)


def _patch_risk_admission(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    manifest: SimpleNamespace | None = None,
    effects=None,
    recovery=None,
) -> SimpleNamespace:
    from okg.substrate import deployment_upgrade as du

    selected = manifest or _risk_admitted_manifest(tmp_path / "dep-root")
    _patch_routine_backup(
        monkeypatch,
        tmp_path,
        manifest=selected,
    )
    effect_checks = effects if effects is not None else _closed_effect_checks()
    recovery_checks = (
        recovery if recovery is not None else _fresh_recovery_checks()
    )
    monkeypatch.setattr(
        du,
        "_collect_effect_inventory",
        lambda ctx, expected_digest=None: (
            effect_checks,
            (),
            {
                "schema": "okg.deployment_effect_inventory.v1",
                "target_revision": "test-revision",
                "effect_digest": "sha256:effect",
                "execution_checkout_exact": True,
                "checks": effect_checks.to_dict(),
            },
        ),
    )
    monkeypatch.setattr(
        du,
        "_receipt_bound_backup_toolchain",
        lambda ctx: {
            "ok": True,
            "validation": "unit-receipt-bound-without-child",
            "tools": {
                "pg_dump": {"major": 17},
                "pg_restore": {"major": 17},
            },
        },
    )
    monkeypatch.setattr(
        du,
        "_disaster_recovery_receipt_evidence",
        lambda ctx, snapshot, policy: (
            recovery_checks,
            (),
            {"status": "compatible", "checks": recovery_checks.to_dict()},
            70 * 1024**3,
        ),
    )
    return selected


def test_risk_admitted_routine_backup_starts_no_dump_or_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_risk_admission(monkeypatch, tmp_path)
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        commands.append(argv)
        if "migrations" in argv:
            return _ok({"pending": [], "destructive_pending": []})
        if "inspect-effective-attribute-recovery" in argv:
            return _ok()
        raise AssertionError(f"unexpected command: {argv}")

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "state.json",
            backup_dir=tmp_path / "backups",
            readiness_profile="dogfood",
            stop_after="backup",
        ),
        command_runner=runner,
    )

    backup = result["phases"][-1]
    assert backup["ok"] is True
    assert backup["verification_mode"] == "no_physical_rollback_required"
    assert backup["physical_backup_started"] is False
    assert backup["restore_started"] is False
    assert backup["backup_tool_child_processes_started"] == 0
    assert backup["backup_tool_validation"] == "unit-receipt-bound-without-child"
    assert backup["source_database_size_bytes"] == 68 * 1024**3
    assert backup["budgets_seconds"] == {
        "dump": 1200.0,
        "backup": 1800.0,
        "total": 2400.0,
    }
    assert not any(
        Path(command[0]).name in {"pg_dump", "pg_restore"}
        for command in commands
    )


def test_dirty_planner_checkout_refuses_risk_admitted_backup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An uncommitted edit costs the whole mutation, not just the free pass.

    This is the other side of
    ``test_risk_admission_uses_real_target_and_postgres_without_dump``, which
    can only assert the admitted path when the suite's own checkout is
    pristine.  Unlike a stale-live-state check such as ``no_pending_migrations``
    -- which merely downgrades the plan to ``current_full_restore`` -- the two
    authority checks below leave the planner with nothing it can trust to plan
    from, so the phase refuses before any physical backup rather than paying
    for one.  Both checks are read out of the real planner against a real,
    deliberately dirtied repository instead of being written by hand, so this
    test also fails if either dirty-tree predicate stops flipping.
    """
    from okg.substrate import deployment_upgrade as du

    repo = tmp_path / "planner-repo"
    deployment_dir = repo / "deployments/dep"
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        "name: dep\nrelease:\n  channel: dev\n"
    )
    planner_module = repo / _PLANNER_MODULE_RELPATH
    planner_module.parent.mkdir(parents=True, exist_ok=True)
    planner_module.write_text("# planner\n")
    _init_git_repo(repo)
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "installed")
    revision = _git(repo, "rev-parse", "HEAD")

    manifest = SimpleNamespace(
        deployment_dir=deployment_dir,
        release={"channel": "dev"},
    )
    probe_ctx = SimpleNamespace(options=UpgradeOptions(deployment="dep"))

    with monkeypatch.context() as probe:
        probe.setattr(du, "__file__", str(planner_module))
        probe.setattr(du, "_resolve_upgrade_deployment", lambda _ctx: manifest)

        assert du._planner_integrity_inventory(repo, revision)["trusted"] is True
        assert du._git_target_revision(
            probe_ctx, manifest
        ).target_revision_pinned is True

        planner_module.write_text("# planner\n# uncommitted local edit\n")

        integrity = du._planner_integrity_inventory(repo, revision)
        authority = du._git_target_revision(probe_ctx, manifest)

    assert integrity["trusted"] is False
    assert integrity["checks"]["tracked_worktree_pristine"] is False
    assert integrity["dirty_paths"] == [f" M {_PLANNER_MODULE_RELPATH}"]
    assert authority.target_revision_pinned is False

    _patch_risk_admission(
        monkeypatch,
        tmp_path,
        effects=_closed_effect_checks(
            planner_trusted=integrity["trusted"],
            target_revision_pinned=authority.target_revision_pinned,
        ),
    )
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        commands.append(argv)
        if backup := _backup_command(argv):
            return backup
        if "migrations" in argv:
            return _ok({"pending": [], "destructive_pending": []})
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "state.json",
            backup_dir=tmp_path / "backups",
            readiness_profile="dogfood",
            stop_after="backup",
        ),
        command_runner=runner,
    )

    backup = result["phases"][-1]
    assert backup["ok"] is False, json.dumps(backup, sort_keys=True, default=str)
    assert backup["reason"] == "physical rollback admission refused mutation"
    assert backup["backup_policy_decision"]["mode"] == "refuse"
    assert backup["backup_policy_decision"]["reasons"] == [
        "effect.target_revision_pinned",
        "effect.planner_trusted",
    ]
    assert backup["failed_checks"] == ["target_revision_pinned", "planner_trusted"]
    assert not any(command and command[0] == "pg_dump" for command in commands)


def test_risk_admitted_missing_recovery_refuses_before_dump(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    _patch_risk_admission(monkeypatch, tmp_path)
    monkeypatch.setattr(
        du,
        "_disaster_recovery_receipt_evidence",
        lambda ctx, snapshot, policy: (
            None,
            (),
            {"status": "missing"},
            None,
        ),
    )
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        commands.append(argv)
        if "migrations" in argv:
            return _ok({"pending": [], "destructive_pending": []})
        if "inspect-effective-attribute-recovery" in argv:
            return _ok()
        if argv[:2] in (["pg_dump", "--version"], ["pg_restore", "--version"]):
            return CommandResult(0, f"{argv[0]} (PostgreSQL) 17.10\n", "")
        raise AssertionError(argv)

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "state.json",
            backup_dir=tmp_path / "backups",
            readiness_profile="dogfood",
        ),
        command_runner=runner,
    )

    assert result["failed_phase"] == "backup"
    assert "backup verify dep" in result["phases"][-1]["fix_hint"]
    assert not any("--file" in command for command in commands)


@pytest.mark.parametrize(
    ("manifest", "effects"),
    [
        (None, _closed_effect_checks(no_pending_migrations=False)),
        (
            _risk_admitted_manifest(
                Path("unused"),
                release_channel="prod",
                deployment_class="production",
            ),
            _closed_effect_checks(),
        ),
    ],
)
def test_risk_admitted_known_physical_or_production_forces_current_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    manifest: SimpleNamespace | None,
    effects,
) -> None:
    selected = manifest
    if selected is not None:
        selected.deployment_dir = tmp_path / "dep-root"
    _patch_risk_admission(
        monkeypatch,
        tmp_path,
        manifest=selected,
        effects=effects,
    )
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        commands.append(argv)
        if backup := _backup_command(argv):
            return backup
        if "migrations" in argv:
            return _ok({"pending": [], "destructive_pending": []})
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "state.json",
            backup_dir=tmp_path / "backups",
            readiness_profile="dogfood",
            stop_after="backup",
        ),
        command_runner=runner,
    )

    backup = result["phases"][-1]
    assert backup["ok"] is True
    assert backup["verification_mode"] == "current_full_restore"
    assert any(command and command[0] == "pg_dump" for command in commands)


def test_risk_admitted_effect_digest_drift_refuses_after_worker_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    _patch_risk_admission(monkeypatch, tmp_path)
    collection_count = 0

    def collect(ctx, expected_digest=None):
        nonlocal collection_count
        collection_count += 1
        checks = _closed_effect_checks(
            effect_digest_stable=collection_count == 1,
        )
        return checks, (), {
            "target_revision": "test-revision",
            "effect_digest": (
                "sha256:effect" if collection_count == 1 else "sha256:drift"
            ),
            "execution_checkout_exact": True,
            "checks": checks.to_dict(),
        }

    monkeypatch.setattr(du, "_collect_effect_inventory", collect)

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if "migrations" in argv:
            return _ok({"pending": [], "destructive_pending": []})
        if "inspect-effective-attribute-recovery" in argv:
            return _ok()
        if argv[:2] in (["pg_dump", "--version"], ["pg_restore", "--version"]):
            return CommandResult(0, f"{argv[0]} (PostgreSQL) 17.10\n", "")
        if "dogfood" in argv and "down" in argv:
            return _ok({"final_state": "stopped"})
        raise AssertionError(argv)

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "state.json",
            backup_dir=tmp_path / "backups",
            readiness_profile="dogfood",
            stop_after="stop_services",
        ),
        command_runner=runner,
    )

    assert result["failed_phase"] == "stop_services"
    assert "drifted" in result["reason"]


def test_checkout_exactness_is_deferred_until_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    _patch_risk_admission(monkeypatch, tmp_path)
    checks = _closed_effect_checks()

    def collect(ctx, expected_digest=None):
        del ctx, expected_digest
        return checks, (), {
            "target_revision": "test-revision",
            "effect_digest": "sha256:effect",
            "execution_checkout_exact": False,
            "checks": checks.to_dict(),
        }

    monkeypatch.setattr(du, "_collect_effect_inventory", collect)
    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "state.json",
            backup_dir=tmp_path / "backups",
            readiness_profile="dogfood",
            stop_after="backup",
        ),
        command_runner=_routine_backup_runner,
    )

    assert result["ok"] is False
    assert result["reason"] == "stopped after requested phase"
    assert result["phases"][-1]["ok"] is True
    assert result["phases"][-1]["verification_mode"] == (
        "no_physical_rollback_required"
    )
    mutation = du._revalidate_effect_authority(
        SimpleNamespace(data={}),
        {
            "effect_inventory": {
                "target_revision": "test-revision",
                "effect_digest": "sha256:effect",
            },
        },
    )
    assert mutation["ok"] is False
    assert mutation["failed_checks"] == ["execution_checkout_exact"]


def test_independent_recovery_receipt_binds_restored_archive_identity(
    tmp_path: Path,
) -> None:
    from okg.substrate import deployment_upgrade as du

    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    archive_path = backup_dir / "dep-current.dump"
    archive_path.write_bytes(b"verified archive")
    archive_path.chmod(0o600)
    digest = "sha256:" + hashlib.sha256(archive_path.read_bytes()).hexdigest()
    snapshot = {
        "target": {
            "deployment": "dep",
            "host": "host",
            "port": 5432,
            "database": "db",
        },
        "database_owner": {
            "deployment": "dep",
            "uuid": "owner",
            "schema_fingerprint": "schema",
        },
        "cluster_system_identifier": "cluster",
        "server_version": "170010",
        "extension_versions": [["vector", "0.8"]],
        "schema_migrations_digest": "sha256:schema",
        "schema_catalog_digest": "sha256:schema-catalog",
        "candidate_plan_digest": "sha256:plan",
        "archive_format": "postgresql-custom",
        "timescaledb": {
            "extension_version": None,
            "hypertables": [],
            "continuous_aggregates": [],
            "hypertable_chunk_counts": [],
        },
        "dump_tool_major": 17,
        "restore_tool_major": 17,
        # Real snapshots always carry both tool identities; the evidence
        # checks refuse degenerate/absent identity maps outright.
        "backup_tool_executable_identity": {
            "pg_dump": {"resolved_path": "/usr/bin/pg_dump", "sha256": "d"},
            "pg_restore": {
                "resolved_path": "/usr/bin/pg_restore",
                "sha256": "r",
            },
        },
    }
    policy = {
        "disaster_recovery_archive_max_age_seconds": 86400,
        "disaster_recovery_restore_max_age_seconds": 604800,
        "recovery_point_objective_seconds": 86400.0,
    }
    ctx = SimpleNamespace(
        options=SimpleNamespace(
            deployment="dep",
            independent_backup_verify=True,
        ),
        backup_dir=backup_dir,
    )
    receipt = du._write_disaster_recovery_receipt(
        ctx,
        backup_path=archive_path,
        archive={
            "backup_digest": digest,
            "backup_size_bytes": archive_path.stat().st_size,
        },
        snapshot=snapshot,
        restore={"observed_restored_size_bytes": 1234},
        policy=policy,
    )
    checks, errors, report, prior_size = (
        du._disaster_recovery_receipt_evidence(ctx, snapshot, policy)
    )

    assert receipt["archive_digest"] == digest
    assert errors == ()
    assert checks is not None and checks.compatible
    assert report["status"] == "compatible"
    assert prior_size == 1234

    original_stat = archive_path.stat()
    archive_path.write_bytes(b"tampered archive")
    archive_path.chmod(0o600)
    os.utime(
        archive_path,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
    )
    tampered, _, tampered_report, _ = (
        du._disaster_recovery_receipt_evidence(ctx, snapshot, policy)
    )
    assert tampered is not None and tampered.compatible is False
    assert tampered_report["checks"]["archive_digest_bound"] is False


def test_independent_backup_verify_resumes_retained_archive_after_restore_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    manifest = _patch_routine_backup(
        monkeypatch,
        tmp_path,
        manifest=_risk_admitted_manifest(tmp_path / "dep-root"),
    )
    checkout = {"revision": "revision-a", "manifest": "sha256:manifest-a"}
    monkeypatch.setattr(
        du, "_upgrade_state_revision", lambda ctx: checkout["revision"]
    )
    monkeypatch.setattr(
        du, "_upgrade_manifest_digest", lambda ctx: checkout["manifest"]
    )
    restore_calls = 0

    def restore(ctx, path: Path, **kwargs) -> dict:
        nonlocal restore_calls
        del ctx, path, kwargs
        restore_calls += 1
        if restore_calls == 1:
            return {"ok": False, "reason": "injected late restore failure"}
        return {
            "ok": True,
            "restore_verified": True,
            "schema_ok": True,
            "ownership_ok": True,
            "observed_restored_size_bytes": 70 * 1024**3,
        }

    monkeypatch.setattr(du, "_verify_backup_restore", restore)
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        commands.append(argv)
        return _routine_backup_runner(argv, env)

    options = UpgradeOptions(
        deployment="dep",
        state_file=tmp_path / "verify-state.json",
        backup_dir=tmp_path / "backups",
        readiness_profile="dogfood",
    )
    first = du.run_deployment_backup_verify(options, command_runner=runner)
    checkout.update({
        "revision": "revision-b",
        "manifest": "sha256:manifest-b",
    })
    manifest.deployment_up["backup"]["budgets_seconds"] = {
        "routine": {
            "backup": 61,
            "attribute_read_model": 31,
            "total": 601,
        },
        "forced": {"dump": 1201, "backup": 1801, "total": 2401},
    }
    second = du.run_deployment_backup_verify(options, command_runner=runner)

    assert first["ok"] is False
    assert second["ok"] is True, json.dumps(second, indent=2)
    backup = next(
        row for row in reversed(second["phases"])
        if row.get("phase") == "backup" and row.get("ok") is True
    )
    assert backup["resumed_archive"] is True
    assert restore_calls == 2
    assert sum(
        command[0] == "pg_dump" and "--file" in command
        for command in commands
    ) == 1
    assert "deployment up dep" in second["next_command"]
    assert "--verified-backup-state" in second["next_command"]
    assert "verify-state.json" in second["next_command"]
    assert "--skip-backup" in second["next_command"]
    assert "--plan" in second["next_command"]


def test_verified_restore_carries_external_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An exact independent receipt, not --skip-backup, authorizes mutation."""
    from okg.substrate import deployment_upgrade as du

    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    archive = backup_dir / "dep.dump"
    archive.write_bytes(b"verified archive")
    archive.chmod(0o600)
    digest = "sha256:" + hashlib.sha256(archive.read_bytes()).hexdigest()
    target_key = {
        "deployment": "dep",
        "authority_mode": "declared",
        "host": "localhost",
        "port": 5432,
        "database": "dep",
        "expected_database": "dep",
        "identity": "ok",
    }
    receipt = {
        "operation": "deployment_backup_verify",
        "completed": True,
        "deployment": "dep",
        "profile": "prod",
        "target_key": target_key,
        "phase_results": [
            {"phase": "preflight", "ok": True},
            {
                "phase": "backup",
                "ok": True,
                "backup_path": str(archive),
                "backup_digest": digest,
                "backup_size_bytes": archive.stat().st_size,
                "restore_verified": True,
                "verification_mode": "current_full_restore",
            },
        ],
    }
    receipt_path = tmp_path / "verified.json"
    receipt_path.write_text(json.dumps(receipt))
    receipt_path.chmod(0o600)
    report = {
        "effect_digest": "sha256:external",
        "smoke": {
            "entries": [{
                "name": "llm",
                "external": True,
                "adapter_contract_digest": "sha256:adapter",
            }]
        },
    }
    checks = _closed_effect_checks(no_external_effects=False)
    monkeypatch.setattr(
        du,
        "_validate_backup_archive",
        lambda ctx, path: {
            "ok": True,
            "backup_digest": digest,
            "backup_size_bytes": archive.stat().st_size,
            "archive_checks": {"private_mode": True},
        },
    )
    monkeypatch.setattr(
        du,
        "_revalidate_independent_backup_completion",
        lambda ctx, payload, backup: {"ok": True},
    )
    monkeypatch.setattr(
        du,
        "_collect_effect_inventory",
        lambda ctx, expected_digest=None: (checks, (), report),
    )
    resolution = {"target": target_key}
    plan_options = UpgradeOptions(
        deployment="dep",
        state_file=tmp_path / "up-state.json",
        backup_dir=backup_dir,
        verified_backup_state=receipt_path,
        skip_backup=True,
        plan_only=True,
    )
    ctx = UpgradeContext(
        options=plan_options,
        dsn="postgres://user@localhost/dep",
        resolution=resolution,
        env={},
        state_file=plan_options.state_file,
        backup_dir=backup_dir,
        command_runner=lambda argv, env: _ok(),
    )
    with pytest.raises(
        SubstrateError,
        match="independent verified backup receipt failed admission",
    ) as forged:
        du._state_with_independent_verified_backup(
            ctx,
            target_key=target_key,
            receipt_path=receipt_path,
            receipt=receipt,
            receipt_stat=receipt_path.stat(),
        )
    assert forged.value.data["checks"]["canonical_safety_contract"] is False

    restore_evidence = {
        "receipt_path": str(tmp_path / "restore-receipt.json"),
        "receipt_digest": "sha256:restore-receipt",
        "archive_digest": digest,
        "fresh": True,
        "owner_current": True,
        "restore_checks": {"restore_verified": True},
    }
    du._deployment_safety_plan(
        ctx,
        effect_report=report,
        restore_point_source="independent_backup_verify",
        restore_point_verified=True,
        restore_point_evidence=restore_evidence,
    )
    ctx.data["backup_result"] = receipt["phase_results"][1]
    receipt["safety_contract"] = du._safety_contract_snapshot(
        ctx,
        restore_point_verified=True,
        phase="backup",
    )
    receipt_path.write_text(json.dumps(receipt))
    receipt_path.chmod(0o600)
    planned = du._state_with_independent_verified_backup(
        ctx,
        target_key=target_key,
        receipt_path=receipt_path,
        receipt=receipt,
        receipt_stat=receipt_path.stat(),
    )
    confirmation = ctx.data["safety_plan"]["confirmation"]["digest"]
    apply_options = dataclasses.replace(
        plan_options,
        plan_only=False,
        confirm_safety_plan=confirmation,
        stop_after="migrate",
    )
    ctx.options = apply_options
    ctx.state_file = apply_options.state_file
    admitted = du._state_with_independent_verified_backup(
        ctx,
        target_key=target_key,
        receipt_path=receipt_path,
        receipt=receipt,
        receipt_stat=receipt_path.stat(),
    )
    admitted["plan_only"] = False
    ran: list[str] = []
    monkeypatch.setattr(du, "_build_context", lambda *args, **kwargs: ctx)
    monkeypatch.setattr(du, "_load_or_initialize_state", lambda _ctx: admitted)
    monkeypatch.setattr(
        du,
        "_revalidate_backup_before_mutation",
        lambda _ctx: {"ok": True, "archive_revalidated": True},
    )
    monkeypatch.setattr(
        du,
        "_revalidate_effect_authority",
        lambda _ctx, backup: {"ok": True, "effect_digest_stable": True},
    )

    def phase(_ctx: UpgradeContext, name: str) -> dict:
        ran.append(name)
        return {"phase": name, "ok": True}

    monkeypatch.setattr(du, "_run_phase", phase)
    result = du.run_deployment_upgrade(apply_options)

    assert planned["completed_phases"] == ["preflight", "backup"]
    assert result["stopped_after"] == "migrate"
    assert "backup" not in ran
    assert "migrate" in ran
    assert result["safety_contract"]["guarantees"][
        "database_restore_point_verified"
    ] is True
    assert result["safety_contract"]["guarantees"][
        "provider_effects_reversible"
    ] is False


def test_independent_backup_verify_resumes_archive_after_restore_interrupt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    _patch_routine_backup(
        monkeypatch,
        tmp_path,
        manifest=_risk_admitted_manifest(tmp_path / "dep-root"),
    )
    restore_calls = 0

    def restore(ctx, path: Path, **kwargs) -> dict:
        nonlocal restore_calls
        del ctx, path, kwargs
        restore_calls += 1
        if restore_calls == 1:
            raise KeyboardInterrupt
        return {
            "ok": True,
            "restore_verified": True,
            "schema_ok": True,
            "ownership_ok": True,
            "observed_restored_size_bytes": 70 * 1024**3,
        }

    monkeypatch.setattr(du, "_verify_backup_restore", restore)
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        commands.append(argv)
        return _routine_backup_runner(argv, env)

    options = UpgradeOptions(
        deployment="dep",
        state_file=tmp_path / "verify-state.json",
        backup_dir=tmp_path / "backups",
        readiness_profile="dogfood",
    )
    with pytest.raises(KeyboardInterrupt):
        du.run_deployment_backup_verify(options, command_runner=runner)
    resumed = du.run_deployment_backup_verify(options, command_runner=runner)

    assert resumed["ok"] is True, resumed
    assert restore_calls == 2
    assert sum(
        command[0] == "pg_dump" and "--file" in command
        for command in commands
    ) == 1


def test_backup_verify_crash_window_revalidates_archive_before_terminalizing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    _patch_routine_backup(
        monkeypatch,
        tmp_path,
        manifest=_risk_admitted_manifest(tmp_path / "dep-root"),
    )
    monkeypatch.setattr(
        du,
        "_verify_backup_restore",
        lambda ctx, path, **kwargs: {
            "ok": True,
            "restore_verified": True,
            "schema_ok": True,
            "ownership_ok": True,
            "observed_restored_size_bytes": 70 * 1024**3,
        },
    )
    options = UpgradeOptions(
        deployment="dep",
        state_file=tmp_path / "verify-state.json",
        backup_dir=tmp_path / "backups",
        readiness_profile="dogfood",
    )
    real_write_state = du._write_state
    terminal_write_failed = False

    def crash_terminal_write(path: Path, state: dict) -> None:
        nonlocal terminal_write_failed
        if (
            not terminal_write_failed
            and state.get("operation") == "deployment_backup_verify"
            and state.get("completed") is True
        ):
            terminal_write_failed = True
            raise OSError("injected terminal-state crash")
        real_write_state(path, state)

    monkeypatch.setattr(du, "_write_state", crash_terminal_write)
    with pytest.raises(OSError, match="terminal-state crash"):
        du.run_deployment_backup_verify(
            options,
            command_runner=_routine_backup_runner,
        )

    state = json.loads(options.state_file.read_text())
    backup = next(
        row for row in reversed(state["phase_results"])
        if row.get("phase") == "backup" and row.get("ok") is True
    )
    archive = Path(backup["backup_path"])
    original_stat = archive.stat()
    original_size = original_stat.st_size
    archive.write_bytes(b"X" * original_size)
    os.utime(
        archive,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
    )

    resumed = du.run_deployment_backup_verify(
        options,
        command_runner=_routine_backup_runner,
    )

    assert resumed["ok"] is False
    assert resumed["completion_revalidation"]["ok"] is False
    assert "--reset-state" in resumed["next_command"]
    assert resumed.get("failed_phase") is None


def test_qualified_smoke_override_is_closed_but_external_override_refuses() -> None:
    from okg.substrate import deployment_upgrade as du

    manifest = {"runtime": {"smoke_sources": ["pact"]}}
    registry = {
        "sources": {
            "pact": {
                "module": "okg.substrate.library.sources.pact",
                "class": "PACTSource",
                "source_class": "discovery_crawl",
                "ownership_id": "dep.pact",
                "params": {"pact_root": "${repo_root}/pact", "repo": "okg"},
                "source_revision_kind": "content_hash",
                "publication_mode": "published_generation",
                "admission_policy": {
                    "producer_kind": "source",
                    "trust_label": "repo-local-trusted",
                },
            },
            "openspec": {
                "module": "okg.substrate.library.sources.openspec",
                "class": "OpenSpecSource",
                "source_class": "discovery_crawl",
                "ownership_id": "dep.openspec",
                "params": {"openspec_root": "${repo_root}/openspec", "repo": "okg"},
                "source_revision_kind": "git_commit",
                "publication_mode": "published_generation",
                "admission_policy": {
                    "producer_kind": "source",
                    "trust_label": "repo-local-trusted",
                },
            },
            "remote": {
                "module": "okg.substrate.library.sources.remote",
                "class": "RemoteSource",
                "ownership_id": "dep.remote",
                "source_revision_kind": "content_hash",
                "publication_mode": "published_generation",
                "source_class": "mutable_api",
                "params": {"url": "https://example.test/feed"},
                "admission_policy": {
                    "producer_kind": "source",
                    "trust_label": "repo-local-trusted",
                },
            },
            "nested-remote": {
                "module": "okg.substrate.library.sources.openspec",
                "class": "OpenSpecSource",
                "source_class": "discovery_crawl",
                "ownership_id": "dep.nested-remote",
                "source_revision_kind": "content_hash",
                "publication_mode": "published_generation",
                "params": {
                    "seeds": [{"url": "https://example.test/nested"}],
                },
                "admission_policy": {
                    "producer_kind": "source",
                    "trust_label": "repo-local-trusted",
                },
            },
            "unknown-adapter": {
                "module": "deployment.sources.unknown",
                "class": "UnknownSource",
                "source_class": "discovery_crawl",
                "ownership_id": "dep.unknown-adapter",
                "params": {"openspec_root": "${repo_root}/openspec", "repo": "okg"},
                "source_revision_kind": "content_hash",
                "publication_mode": "published_generation",
                "admission_policy": {
                    "producer_kind": "source",
                    "trust_label": "repo-local-trusted",
                },
            },
        }
    }
    base = SimpleNamespace(options=UpgradeOptions(deployment="dep"))
    override = SimpleNamespace(
        options=UpgradeOptions(deployment="dep", include=("openspec",))
    )
    remote = SimpleNamespace(
        options=UpgradeOptions(deployment="dep", include=("remote",))
    )
    nested_remote = SimpleNamespace(
        options=UpgradeOptions(deployment="dep", include=("nested-remote",))
    )
    unknown_adapter = SimpleNamespace(
        options=UpgradeOptions(deployment="dep", include=("unknown-adapter",))
    )

    configured = du._target_smoke_inventory(
        base, raw_manifest=manifest, raw_registry=registry
    )
    selected = du._target_smoke_inventory(
        override, raw_manifest=manifest, raw_registry=registry
    )
    external = du._target_smoke_inventory(
        remote, raw_manifest=manifest, raw_registry=registry
    )
    nested_external = du._target_smoke_inventory(
        nested_remote, raw_manifest=manifest, raw_registry=registry
    )
    unknown = du._target_smoke_inventory(
        unknown_adapter, raw_manifest=manifest, raw_registry=registry
    )

    assert configured["versioned_and_deployment_owned"] is True
    assert selected["versioned_and_deployment_owned"] is True
    assert selected["selected_sources"] == ["openspec"]
    assert selected["selected_source_digest"] != configured["selected_source_digest"]
    assert selected["effect_capabilities"] != configured["effect_capabilities"]
    assert external["no_external_effects"] is False
    assert nested_external["no_external_effects"] is False
    assert unknown["versioned_and_deployment_owned"] is False
    assert unknown["entries"][0]["adapter_trusted"] is False


def test_smoke_inventory_separates_source_authority_from_external_reach() -> None:
    """Authority and external reach must be independently reportable.

    Both booleans used to be the same expression: ``closed`` already required
    ``not external``, so ``versioned_and_deployment_owned`` and
    ``no_external_effects`` were always equal.  Every refusal therefore named
    a claim the operator could not satisfy without also satisfying the other,
    and issue 1013's proposed fix (drop one from the refusal set) would have
    changed nothing.
    """
    from okg.substrate import deployment_upgrade as du

    # An ALLOWLISTED adapter carrying an external param is the only shape that
    # separates the two claims: full source authority, external reach.
    openspec = {
        "module": "okg.substrate.library.sources.openspec",
        "class": "OpenSpecSource",
        "source_class": "discovery_crawl",
        "source_revision_kind": "git_commit",
        "publication_mode": "published_generation",
        "admission_policy": {
            "producer_kind": "source",
            "trust_label": "repo-local-trusted",
        },
    }
    registry = {
        "sources": {
            "local": {
                **openspec,
                "ownership_id": "dep.local",
                "params": {"openspec_root": "${repo_root}/openspec", "repo": "okg"},
            },
            "external": {
                **openspec,
                "ownership_id": "dep.external",
                "params": {
                    "openspec_root": "https://example.test/openspec",
                    "repo": "okg",
                },
            },
            "untrusted-adapter": {
                **openspec,
                "module": "deployment.sources.unknown",
                "class": "UnknownSource",
                "ownership_id": "dep.untrusted-adapter",
                "params": {"openspec_root": "${repo_root}/openspec", "repo": "okg"},
            },
        }
    }

    def inventory(name: str) -> dict:
        return du._target_smoke_inventory(
            SimpleNamespace(options=UpgradeOptions(deployment="dep")),
            raw_manifest={"runtime": {"smoke_sources": [name]}},
            raw_registry=registry,
        )

    local = inventory("local")
    external = inventory("external")
    untrusted = inventory("untrusted-adapter")

    assert local["versioned_and_deployment_owned"] is True
    assert local["no_external_effects"] is True

    # The load-bearing case: authority holds, reach does not.
    assert external["versioned_and_deployment_owned"] is True
    assert external["no_external_effects"] is False
    assert external["entries"][0]["adapter_trusted"] is True
    assert external["entries"][0]["external"] is True

    # Losing adapter authority still fails the authority claim only.
    assert untrusted["versioned_and_deployment_owned"] is False
    assert untrusted["no_external_effects"] is True

    # Absence is not proof: an empty selection fails BOTH claims rather than
    # reading as "nothing external here".
    empty = du._target_smoke_inventory(
        SimpleNamespace(options=UpgradeOptions(deployment="dep")),
        raw_manifest={"runtime": {"smoke_sources": []}},
        raw_registry=registry,
    )
    assert empty["versioned_and_deployment_owned"] is False
    assert empty["no_external_effects"] is False


def test_okg_ci_production_smoke_and_attribute_authority_are_closed() -> None:
    from okg.substrate import deployment_upgrade as du
    from okg.substrate import fastyaml
    from okg.substrate.library.sources.pact import PACT_EMISSION_CONTRACT

    root = Path(__file__).resolve().parents[2]
    deployment_dir = root / "deployments/okg-ci"
    manifest = fastyaml.safe_load((deployment_dir / "deployment.yaml").read_text())
    registry = fastyaml.safe_load((deployment_dir / "source_registry.yaml").read_text())
    normalized = du._normalized_source_registry_snapshot(
        deployment_dir / "source_registry.yaml",
        repo_root=root,
    )
    ctx = SimpleNamespace(
        options=UpgradeOptions(deployment="okg-ci"),
        env={"OKG_IVM_INTERVAL_VIEW": "1"},
    )

    smoke = du._target_smoke_inventory(
        ctx,
        raw_manifest=manifest,
        raw_registry=registry,
        normalized_registry=normalized,
        pact_emission_contract=PACT_EMISSION_CONTRACT,
    )

    assert smoke["versioned_and_deployment_owned"] is True
    assert smoke["no_external_effects"] is True
    pact = smoke["entries"][0]
    assert pact["name"] == "pact"
    assert pact["admission_contract_valid"] is True
    assert all(pact["admission_contract_checks"].values())
    assert (
        manifest["native_views"]["attribute_effective_view_empty_overlay_fast_path"]
        is True
    )
    missing_view_declaration = copy.deepcopy(manifest)
    del missing_view_declaration["native_views"][
        "attribute_effective_view_empty_overlay_fast_path"
    ]
    assert (
        du._target_attribute_read_model_rebuildable(
            ctx,
            missing_view_declaration,
        )
        is False
    )


def test_normalized_strict_registry_snapshot_is_recursively_json_shaped(
    tmp_path: Path,
) -> None:
    from okg.substrate import deployment_upgrade as du

    registry_path = tmp_path / "source_registry.yaml"
    registry_path.write_text(yaml.safe_dump({
        "admission_contract": "strict_v1",
        "sources": {
            "proof": {
                "module": "tests.proof",
                "class": "ProofSource",
                "ownership_id": "proof.source",
                "source_class": "reference_catalog",
                "record_identity_kind": "domain_key",
                "record_identity_fields": ["id"],
                "source_revision_kind": "content_hash",
                "deletion_semantics": "missing_from_completed_scope",
                "publication_mode": "published_generation",
                "admission_policy": {
                    "trust_label": "review-trusted",
                    "authority_scope": {
                        "path_globs": ["docs/**/*"],
                    },
                    "output_signature": {
                        "nodes": [
                            {
                                "subtype": "document",
                                "attrs": {"source": "proof"},
                            }
                        ]
                    },
                },
            }
        },
    }, sort_keys=False))

    snapshot = du._normalized_source_registry_snapshot(
        registry_path,
        repo_root=Path(__file__).resolve().parents[2],
    )

    json.dumps(snapshot)
    policy = snapshot["proof"]["admission_policy"]
    raw = snapshot["proof"]["raw_admission_policy"]
    assert policy["output_signature"]["nodes"][0]["attrs"] == {
        "source": "proof"
    }
    assert raw["authority_scope"]["path_globs"] == ["docs/**/*"]


def _synthetic_okg_ci_pact_smoke_inputs() -> tuple[dict, dict]:
    """A manifest and registry whose pact smoke source meets the strict okg-ci contract.

    Built from the product's own expectation (``_expected_pact_smoke_policy``
    over the shipped PACT emission contract), not read from
    deployments/okg-ci, so the public copy runs the refusal cases below too.
    ``test_okg_ci_pact_smoke_policy_synthetic_baseline_is_accepted`` is the
    control; the real okg-ci registry is checked by the test above.
    """
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.library.sources.pact import PACT_EMISSION_CONTRACT

    producer_id = "okg-ci.pact"
    expected = du._expected_pact_smoke_policy(producer_id, PACT_EMISSION_CONTRACT)
    signature = expected["output_signature"]
    admission = {
        "producer_id": producer_id,
        "producer_kind": "source",
        "trust_label": "repo-local-trusted",
        "admission_mode": "fast_track",
        "authority_scope": {
            "source_family": "okg_ci_repo",
            "path_globs": ["pact/**/*"],
        },
        "allowed_record_kinds": ["node", "edge"],
        "output_signature": signature,
        "policy_clauses": expected["policy_clauses"],
        "output_scope_summary": {
            "scope": "PACT manifests under pact/",
            "nodes": [node["subtype"] for node in signature["nodes"]],
            "edges": [
                f"{edge['src_subtype']} {edge['edge_type']} {edge['dst_subtype']}"
                for edge in signature["edges"]
            ],
        },
    }
    registry = {
        "sources": {
            "pact": {
                "module": "okg.substrate.library.sources.pact",
                "class": "PACTSource",
                "ownership_id": producer_id,
                "params": {"repo": "okg", "pact_root": "${repo_root}/pact"},
                "source_class": "discovery_crawl",
                "record_identity_kind": "scoped_locator",
                "record_identity_fields": ["repo", "change_id"],
                "source_revision_kind": "content_hash",
                "deletion_semantics": "missing_from_completed_scope",
                "publication_mode": "published_generation",
                "admission_policy": admission,
            }
        }
    }
    manifest = {"runtime": {"smoke_sources": ["pact"]}}
    return manifest, copy.deepcopy(registry)


def _strict_pact_smoke_for_okg_ci(monkeypatch: pytest.MonkeyPatch) -> None:
    """Which deployment gets the strict PACT smoke check is repository
    configuration (#2823); these cases configure it themselves, so they run
    the same with or without this repository's .okg/coordination.yaml."""
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.deployment_default import DeploymentFleet

    monkeypatch.setattr(
        du,
        "_code_checkout_fleet",
        lambda: DeploymentFleet(
            source="test", strict_pact_smoke={"okg-ci": "okg_ci_repo"}
        ),
    )


def test_okg_ci_pact_smoke_policy_synthetic_baseline_is_accepted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The control for the refusal cases below: unmutated, the inputs pass."""
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.library.sources.pact import PACT_EMISSION_CONTRACT

    _strict_pact_smoke_for_okg_ci(monkeypatch)

    manifest, registry = _synthetic_okg_ci_pact_smoke_inputs()
    registry_path = tmp_path / "source_registry.yaml"
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False))
    normalized = du._normalized_source_registry_snapshot(
        registry_path, repo_root=Path(__file__).resolve().parents[2]
    )
    smoke = du._target_smoke_inventory(
        SimpleNamespace(options=UpgradeOptions(deployment="okg-ci")),
        raw_manifest=manifest,
        raw_registry=registry,
        normalized_registry=normalized,
        pact_emission_contract=PACT_EMISSION_CONTRACT,
    )
    entry = smoke["entries"][0]
    assert all(entry["admission_contract_checks"].values()), entry["admission_contract_checks"]
    assert entry["admission_contract_valid"] is True
    assert smoke["versioned_and_deployment_owned"] is True


def test_strict_pact_smoke_applies_only_to_configured_deployments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The strict check follows configuration, not a built-in name."""
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.deployment_default import DeploymentFleet
    from okg.substrate.library.sources.pact import PACT_EMISSION_CONTRACT

    manifest, registry = _synthetic_okg_ci_pact_smoke_inputs()
    registry["sources"]["pact"]["admission_policy"]["admission_mode"] = "review"
    registry_path = tmp_path / "source_registry.yaml"
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False))
    normalized = du._normalized_source_registry_snapshot(
        registry_path, repo_root=Path(__file__).resolve().parents[2]
    )

    def entry_for(fleet: DeploymentFleet) -> dict:
        monkeypatch.setattr(du, "_code_checkout_fleet", lambda: fleet)
        smoke = du._target_smoke_inventory(
            SimpleNamespace(options=UpgradeOptions(deployment="okg-ci")),
            raw_manifest=manifest,
            raw_registry=registry,
            normalized_registry=normalized,
            pact_emission_contract=PACT_EMISSION_CONTRACT,
        )
        return smoke["entries"][0]

    strict = entry_for(
        DeploymentFleet(source="t", strict_pact_smoke={"okg-ci": "okg_ci_repo"})
    )
    assert strict["admission_contract_checks"]["admission_mode"] is False
    assert strict["admission_contract_valid"] is False

    unconfigured = entry_for(DeploymentFleet())
    assert "admission_mode" not in unconfigured["admission_contract_checks"]

    # The inventory says where the configuration came from, so a run without
    # it is visible rather than silent.
    monkeypatch.setattr(du, "_code_checkout_fleet", lambda: DeploymentFleet())
    smoke = du._target_smoke_inventory(
        SimpleNamespace(options=UpgradeOptions(deployment="okg-ci")),
        raw_manifest=manifest,
        raw_registry=registry,
        normalized_registry=normalized,
        pact_emission_contract=PACT_EMISSION_CONTRACT,
    )
    assert smoke["strict_pact_smoke"] == {
        "config": "none configured",
        "applies_to_this_deployment": False,
    }


@pytest.mark.parametrize(
    "mutation",
    [
        "producer_id",
        "admission_mode",
        "authority_scope",
        "allowed_record_kinds",
        "output_node",
        "output_edge",
        "policy_clause",
        "source_revision_kind",
        "publication_mode",
        "write_envelope",
        "candidate_emitter",
        "pact_root",
        "repo",
        "missing_pact_root",
    ],
)
def test_okg_ci_pact_smoke_policy_drift_refuses(
    tmp_path: Path,
    mutation: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.library.sources.pact import PACT_EMISSION_CONTRACT

    _strict_pact_smoke_for_okg_ci(monkeypatch)

    root = Path(__file__).resolve().parents[2]
    manifest, registry = _synthetic_okg_ci_pact_smoke_inputs()
    pact = registry["sources"]["pact"]
    policy = pact["admission_policy"]
    emission_contract = copy.deepcopy(PACT_EMISSION_CONTRACT)
    if mutation == "producer_id":
        policy.pop("producer_id")
    elif mutation == "admission_mode":
        policy["admission_mode"] = "review"
    elif mutation == "authority_scope":
        policy["authority_scope"]["path_globs"] = ["docs/**/*"]
    elif mutation == "allowed_record_kinds":
        policy["allowed_record_kinds"] = ["node"]
    elif mutation == "output_node":
        policy["output_signature"]["nodes"].pop()
    elif mutation == "output_edge":
        policy["output_signature"]["edges"].pop()
    elif mutation == "policy_clause":
        policy["policy_clauses"].pop()
    elif mutation == "source_revision_kind":
        pact["source_revision_kind"] = "version_token"
    elif mutation == "publication_mode":
        pact["publication_mode"] = "cursor_only"
    elif mutation == "write_envelope":
        policy["write_envelope"] = {"unbounded": ["nodes"]}
    elif mutation == "candidate_emitter":
        emission_contract["nodes"] = emission_contract["nodes"][:-1]
    elif mutation == "pact_root":
        pact["params"]["pact_root"] = "/tmp/pact"
    elif mutation == "repo":
        pact["params"]["repo"] = "other"
    elif mutation == "missing_pact_root":
        pact["params"].pop("pact_root")
    else:
        raise AssertionError(mutation)
    registry_path = tmp_path / "source_registry.yaml"
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False))
    try:
        normalized = du._normalized_source_registry_snapshot(
            registry_path,
            repo_root=root,
        )
    except ValueError:
        assert mutation == "publication_mode"
        return
    smoke = du._target_smoke_inventory(
        SimpleNamespace(options=UpgradeOptions(deployment="okg-ci")),
        raw_manifest=manifest,
        raw_registry=registry,
        normalized_registry=normalized,
        pact_emission_contract=emission_contract,
    )

    assert smoke["versioned_and_deployment_owned"] is False
    if mutation in {"pact_root", "repo", "missing_pact_root"}:
        entry = smoke["entries"][0]
        assert entry["adapter_trusted"] is False
        assert entry["admission_contract_checks"]["repo_local_params"] is False


def test_planner_integrity_requires_revision_bound_pristine_substrate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    expected = tmp_path / "src/okg/substrate/deployment_upgrade.py"
    expected.parent.mkdir(parents=True)
    expected.write_text("# trusted planner\n")
    monkeypatch.setattr(du, "__file__", str(expected))

    tracked_status = ""
    untracked_status = ""

    def git_command(repo_root, args, **kwargs):
        del repo_root, kwargs
        if args == ["rev-parse", "HEAD"]:
            return "a" * 40 + "\n"
        if args == ["status", "--porcelain=v1", "--untracked-files=no"]:
            return tracked_status
        if args == ["ls-files", "--others", "--exclude-standard"]:
            return untracked_status
        raise AssertionError(args)

    monkeypatch.setattr(du, "_git_command", git_command)

    trusted = du._planner_integrity_inventory(tmp_path, "a" * 40)
    assert trusted["trusted"] is True

    tracked_status = " M src/okg/substrate/deployment_upgrade.py\n"
    dirty = du._planner_integrity_inventory(tmp_path, "a" * 40)
    assert dirty["trusted"] is False
    assert dirty["checks"]["tracked_worktree_pristine"] is False

    tracked_status = " M deployments/okg-ci/deployment.yaml\n"
    dirty_gate = du._planner_integrity_inventory(tmp_path, "a" * 40)
    assert dirty_gate["trusted"] is False
    assert dirty_gate["dirty_paths"] == [" M deployments/okg-ci/deployment.yaml"]

    tracked_status = ""
    untracked_status = "deployments/okg-ci/local_override.py\n"
    untracked = du._planner_integrity_inventory(tmp_path, "a" * 40)
    assert untracked["trusted"] is False
    assert untracked["dirty_paths"] == [
        "?? deployments/okg-ci/local_override.py",
    ]

    untracked_status = ""
    wrong_revision = du._planner_integrity_inventory(tmp_path, "b" * 40)
    assert wrong_revision["trusted"] is False
    assert wrong_revision["checks"]["planner_revision_is_head"] is False


def test_planner_integrity_allows_controller_state_but_rejects_unrelated_dirt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    repo = tmp_path / "repo"
    planner = repo / "src/okg/substrate/deployment_upgrade.py"
    planner.parent.mkdir(parents=True)
    planner.write_text("# trusted planner\n")
    (repo / ".gitignore").write_text(
        ".okg/autoupdate/\n.okg/up/\n.okg/backups/\n"
    )

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", *args],
            cwd=repo,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    git("init")
    git("config", "user.email", "test@example.com")
    git("config", "user.name", "Test")
    git("add", ".")
    git("commit", "-m", "trusted planner")
    revision = git("rev-parse", "HEAD")
    monkeypatch.setattr(du, "__file__", str(planner))

    controller_state = {
        ".okg/autoupdate/active-baseline.json": "{}",
        ".okg/autoupdate/manual.plan.json": "{}",
        ".okg/up/okg-ci-prod.json": "{}",
        ".okg/backups/okg-ci/verified.dump": "backup",
    }
    for relative, contents in controller_state.items():
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)

    trusted = du._planner_integrity_inventory(repo, revision)
    assert trusted["trusted"] is True
    assert trusted["dirty_paths"] == []

    unrelated = repo / "deployments/okg-ci/local_override.py"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_text("raise RuntimeError('untrusted')\n")
    refused = du._planner_integrity_inventory(repo, revision)
    assert refused["trusted"] is False
    assert refused["dirty_paths"] == [
        "?? deployments/okg-ci/local_override.py",
    ]


def test_precheckout_target_revision_binds_candidate_without_executing_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    repo = tmp_path / "repo"
    deployment_dir = repo / "deployments/dep"
    deployment_dir.mkdir(parents=True)

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", *args],
            cwd=repo,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    git("init")
    git("config", "user.email", "test@example.com")
    git("config", "user.name", "Test")
    (deployment_dir / "deployment.yaml").write_text(
        "name: dep\nrelease:\n  channel: dev\n"
    )
    planner_module = repo / _PLANNER_MODULE_RELPATH
    planner_module.parent.mkdir(parents=True, exist_ok=True)
    planner_module.write_text("# planner\n")
    monkeypatch.setattr(du, "__file__", str(planner_module))
    git("add", ".")
    git("commit", "-m", "installed")
    installed = git("rev-parse", "HEAD")
    sentinel = tmp_path / "candidate-imported"
    candidate_module = repo / "src/okg/substrate/candidate_only.py"
    candidate_module.parent.mkdir(parents=True, exist_ok=True)
    candidate_module.write_text(
        f"from pathlib import Path\nPath({str(sentinel)!r}).touch()\n"
    )
    candidate_pact = repo / "src/okg/substrate/library/sources/pact.py"
    candidate_pact.parent.mkdir(parents=True)
    candidate_pact.write_text(
        "PACT_EMISSION_CONTRACT = {\n"
        "    'nodes': (('pact_change', 'pact-change:', 'change'),),\n"
        "    'edges': (('pact_change', 'contains', 'pact_change', 'contains'),),\n"
        "}\n"
        "class PACTSource:\n"
        "    emission_contract = PACT_EMISSION_CONTRACT\n"
        f"from pathlib import Path\nPath({str(sentinel)!r}).touch()\n"
    )
    (deployment_dir / "deployment.yaml").write_text(
        "name: dep\nrelease:\n  channel: dev\ndescription: candidate\n"
    )
    git("add", ".")
    git("commit", "-m", "candidate")
    candidate = git("rev-parse", "HEAD")
    git("checkout", "--detach", installed)

    manifest = SimpleNamespace(
        deployment_dir=deployment_dir,
        release={"channel": "dev"},
    )
    ctx = SimpleNamespace(
        options=UpgradeOptions(
            deployment="dep",
            precheckout_target_revision=candidate,
        )
    )
    monkeypatch.setattr(du, "_resolve_upgrade_deployment", lambda _ctx: manifest)

    authority = du._git_target_revision(ctx, manifest)
    repo_root = authority.code_repo_root
    target = authority.code_revision
    planner = authority.planner_revision
    exact = authority.execution_checkout_exact
    pinned = authority.target_revision_pinned
    digest = du._upgrade_manifest_digest(ctx)
    emission_contract = du._target_pact_emission_contract(repo, candidate)
    expected_manifest = subprocess.run(
        ["git", "show", f"{candidate}:deployments/dep/deployment.yaml"],
        cwd=repo,
        check=True,
        capture_output=True,
    ).stdout

    assert repo_root == repo
    assert authority.deployment_repo_root == repo
    assert authority.vendored is False
    assert authority.deployment_revision == candidate
    assert target == candidate
    assert planner == installed
    assert exact is False
    assert pinned is True
    assert digest == "sha256:" + hashlib.sha256(expected_manifest).hexdigest()
    assert emission_contract["nodes"] == (
        ("pact_change", "pact-change:", "change"),
    )
    assert not sentinel.exists()


def test_precheckout_manifest_failure_preserves_authority_diagnostics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    manifest = SimpleNamespace(deployment_dir=tmp_path / "deployments/dep")
    authority = SimpleNamespace(
        deployment_relative=Path("deployments/dep"),
        deployment_repo_root=tmp_path,
        deployment_revision="d" * 40,
    )
    ctx = SimpleNamespace(
        options=SimpleNamespace(
            deployment="dep",
            precheckout_target_revision="c" * 40,
        ),
    )
    monkeypatch.setattr(du, "_resolve_upgrade_deployment", lambda _ctx: manifest)
    monkeypatch.setattr(du, "_git_target_revision", lambda _ctx, _manifest: authority)
    monkeypatch.setattr(
        du,
        "_git_command",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            OSError("candidate manifest read failed")
        ),
    )

    with pytest.raises(SubstrateError) as raised:
        du._upgrade_manifest_digest(ctx)

    error = raised.value.data["authority_error"]
    assert set(error) == {
        "authority_role",
        "repository",
        "requested_revision",
        "resolved_revision",
        "operation",
        "reason",
    }
    assert error["authority_role"] == "deployment"
    assert error["repository"] == str(tmp_path)
    assert error["resolved_revision"] == "d" * 40


def test_verified_backup_source_failure_preserves_authority_diagnostics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    receipt_path = tmp_path / "verified-backup.json"
    receipt_path.write_text("{}")
    manifest = SimpleNamespace(
        deployment_dir=tmp_path / "deployments/dep",
        release={},
    )
    options = UpgradeOptions(
        deployment="dep",
        verified_backup_state=receipt_path,
        verified_backup_source_revision="s" * 40,
    )
    ctx = SimpleNamespace(options=options)
    monkeypatch.setattr(du, "_resolve_upgrade_deployment", lambda _ctx: manifest)
    monkeypatch.setattr(
        du,
        "_resolve_repo_authorities",
        lambda *_args, **_kwargs: (tmp_path, tmp_path),
    )
    monkeypatch.setattr(
        du,
        "_git_command",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            OSError("source revision read failed")
        ),
    )

    with pytest.raises(SubstrateError) as raised:
        du._state_with_admitted_verified_backup(ctx, target_key={})

    error = raised.value.data["authority_error"]
    assert set(error) == {
        "authority_role",
        "repository",
        "requested_revision",
        "resolved_revision",
        "operation",
        "reason",
    }
    assert error["authority_role"] == "framework"
    assert error["repository"] == str(tmp_path)
    assert error["requested_revision"] == "s" * 40


_PLANNER_MODULE_RELPATH = "src/okg/substrate/deployment_upgrade.py"


def _planner_project_root() -> Path:
    """The checkout `dependency_sync` names: the one providing the planner."""
    from okg.substrate import deployment_upgrade as du

    return Path(du.__file__).resolve().parents[3]


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _init_git_repo(repo: Path) -> None:
    repo.mkdir(parents=True, exist_ok=True)
    _git(repo, "init")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test")


def _write_vendored_okg_repo(okg_repo: Path) -> None:
    """A miniature okg source checkout: exactly the trees the planner reads."""
    db = okg_repo / "src/okg/substrate/db"
    (db / "migrations").mkdir(parents=True)
    (db / "schema.sql").write_text("CREATE SCHEMA okg;\n")
    (db / "views.sql").write_text("-- views\n")
    (db / "migrations/0001_add_thing.sql").write_text("SELECT 1;\n")
    (db / "migrate.py").write_text(
        'REQUIRED_EXTENSIONS = {"vector": "0.7.0"}\n'
    )
    (db / "management.py").write_text(
        "_ORDERED_MIGRATION_CHECKSUM_COMPATIBILITIES = {}\n"
    )
    pact_source = okg_repo / "src/okg/substrate/library/sources/pact.py"
    pact_source.parent.mkdir(parents=True)
    pact_source.write_text(
        "PACT_EMISSION_CONTRACT = {\n"
        "    'nodes': (('pact_change', 'pact-change:', 'change'),),\n"
        "    'edges': (('pact_change', 'contains', 'pact_change', 'contains'),),\n"
        "}\n"
        "class PACTSource:\n"
        "    emission_contract = PACT_EMISSION_CONTRACT\n"
    )
    planner = okg_repo / "src/okg/substrate/deployment_upgrade.py"
    planner.write_text("# vendored planner\n")
    ontologies = okg_repo / "src/okg/substrate/library/ontologies"
    ontologies.mkdir(parents=True)
    real_module = (
        Path(__file__).resolve().parents[2]
        / "src/okg/substrate/library/ontologies/document_starter"
    )
    shutil.copytree(real_module, ontologies / "document_starter")


def _vendored_layout(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Build a deployment repo that vendors okg as its own inner repository."""
    outer = tmp_path / "deployment-repo"
    deployment_dir = outer / "deployments/dep"
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        "name: dep\n"
        "release:\n  channel: dev\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules:\n  - document_starter\n"
    )
    (deployment_dir / "source_registry.yaml").write_text("sources: {}\n")
    (deployment_dir / "schemas").mkdir()
    (deployment_dir / "schemas/.gitkeep").write_text("")
    _init_git_repo(outer)
    _git(outer, "add", ".")
    _git(outer, "commit", "-m", "deployment")

    okg_repo = outer / "vendor/okg"
    okg_repo.mkdir(parents=True)
    _init_git_repo(okg_repo)
    _write_vendored_okg_repo(okg_repo)
    _git(okg_repo, "add", ".")
    _git(okg_repo, "commit", "-m", "vendored okg")
    return outer, okg_repo, deployment_dir


def _vendored_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path, Path, SimpleNamespace, SimpleNamespace]:
    from okg.substrate import deployment_upgrade as du

    outer, okg_repo, deployment_dir = _vendored_layout(tmp_path)
    monkeypatch.setattr(
        du,
        "__file__",
        str(okg_repo / _PLANNER_MODULE_RELPATH),
    )
    manifest = SimpleNamespace(
        deployment_dir=deployment_dir,
        release={"channel": "dev"},
    )
    ctx = SimpleNamespace(
        options=UpgradeOptions(deployment="dep"),
        env={"OKG_IVM_INTERVAL_VIEW": "1"},
        dsn="postgres://user@host/db",
        data={},
    )
    monkeypatch.setattr(du, "_resolve_upgrade_deployment", lambda _ctx: manifest)
    return outer, okg_repo, deployment_dir, ctx, manifest


def _explicit_split_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path, Path, SimpleNamespace, SimpleNamespace]:
    """Build separate framework/deployment repos with an explicit framework pin."""
    from okg.substrate import deployment_upgrade as du

    outer = tmp_path / "deployment-repo"
    deployment_dir = outer / "deployments/dep"
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        "name: dep\n"
        "release:\n  channel: prod\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules:\n  - document_starter\n"
    )
    (deployment_dir / "source_registry.yaml").write_text("sources: {}\n")
    (deployment_dir / "schemas").mkdir()
    (deployment_dir / "schemas/.gitkeep").write_text("")
    _init_git_repo(outer)
    _git(outer, "add", ".")
    _git(outer, "commit", "-m", "deployment")

    framework = tmp_path / "framework-repo"
    _init_git_repo(framework)
    _write_vendored_okg_repo(framework)
    _git(framework, "add", ".")
    _git(framework, "commit", "-m", "framework")
    framework_revision = _git(framework, "rev-parse", "HEAD")
    manifest = SimpleNamespace(
        deployment_dir=deployment_dir,
        release={
            "channel": "prod",
            "pin": {
                "code_repository": "../framework-repo",
                "code_revision": framework_revision,
            },
        },
    )
    ctx = SimpleNamespace(
        options=UpgradeOptions(deployment="dep"),
        env={"OKG_IVM_INTERVAL_VIEW": "1"},
        dsn="postgres://user@host/db",
        data={},
    )
    monkeypatch.setattr(du, "_resolve_upgrade_deployment", lambda _ctx: manifest)
    return outer, framework, deployment_dir, ctx, manifest


def test_git_target_revision_resolves_vendored_code_repo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    outer, okg_repo, _deployment_dir, ctx, manifest = _vendored_authority(
        tmp_path, monkeypatch
    )

    assert du.CODE_REPO_MODULE_PATH == _PLANNER_MODULE_RELPATH
    authority = du._git_target_revision(ctx, manifest)

    assert authority.code_repo_root == okg_repo.resolve()
    assert authority.deployment_repo_root == outer.resolve()
    assert authority.vendored is True
    assert authority.code_revision == _git(okg_repo, "rev-parse", "HEAD")
    assert authority.planner_revision == authority.code_revision
    assert authority.deployment_revision == _git(outer, "rev-parse", "HEAD")
    assert authority.deployment_relative == Path("deployments/dep")
    assert authority.execution_checkout_exact is True
    assert authority.target_revision_pinned is True
    # The read that used to fail: okg source addressed to the okg repository.
    assert du._target_required_extensions(
        authority.code_repo_root,
        authority.code_revision,
    ) == {"vector": "0.7.0"}


def test_git_target_revision_preserves_in_repo_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    repo = tmp_path / "repo"
    deployment_dir = repo / "deployments/dep"
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        "name: dep\nrelease:\n  channel: dev\n"
    )
    planner = repo / _PLANNER_MODULE_RELPATH
    planner.parent.mkdir(parents=True)
    planner.write_text("# planner\n")
    _init_git_repo(repo)
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "in-repo deployment")
    monkeypatch.setattr(du, "__file__", str(planner))

    manifest = SimpleNamespace(
        deployment_dir=deployment_dir,
        release={"channel": "dev"},
    )
    ctx = SimpleNamespace(options=UpgradeOptions(deployment="dep"))
    authority = du._git_target_revision(ctx, manifest)

    head = _git(repo, "rev-parse", "HEAD")
    assert authority.code_repo_root == repo.resolve()
    assert authority.deployment_repo_root == repo.resolve()
    assert authority.vendored is False
    assert authority.code_revision == head
    assert authority.deployment_revision == head
    assert authority.execution_checkout_exact is True
    assert authority.target_revision_pinned is True


def test_git_target_revision_resolves_explicit_framework_repo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    outer, framework, _deployment_dir, ctx, manifest = _explicit_split_authority(
        tmp_path, monkeypatch
    )

    authority = du._git_target_revision(ctx, manifest)

    assert authority.code_repo_root == framework.resolve()
    assert authority.deployment_repo_root == outer.resolve()
    assert authority.code_revision == _git(framework, "rev-parse", "HEAD")
    assert authority.planner_revision == authority.code_revision
    assert authority.deployment_revision == _git(outer, "rev-parse", "HEAD")
    assert authority.deployment_relative == Path("deployments/dep")
    assert authority.vendored is True
    assert du._target_required_extensions(
        authority.code_repo_root, authority.code_revision
    ) == {"vector": "0.7.0"}


def test_target_and_precheckout_identity_carry_both_authorities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    outer, framework, _deployment_dir, ctx, _manifest = _explicit_split_authority(
        tmp_path, monkeypatch
    )
    ctx.resolution = {
        "target": {
            "deployment": "dep",
            "host": "host",
            "port": 5432,
            "database": "db",
            "expected_database": "db",
            "identity": "ok",
        }
    }
    authority = du._git_target_revision(ctx, du._resolve_upgrade_deployment(ctx))
    target_key = du._target_state_key(ctx)
    seed = du._precheckout_authority_seed(ctx, target_key=target_key)

    assert target_key["framework_repository"] == str(framework.resolve())
    assert target_key["deployment_repository"] == str(outer.resolve())
    assert target_key["deployment_revision"] == authority.deployment_revision
    assert seed["framework_repository"] == str(framework.resolve())
    assert seed["deployment_repository"] == str(outer.resolve())
    assert seed["deployment_revision"] == authority.deployment_revision


def test_target_state_key_preserves_authority_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    _outer, _framework, _deployment_dir, ctx, manifest = _explicit_split_authority(
        tmp_path, monkeypatch
    )
    ctx.resolution = {"target": {"deployment": "dep", "identity": "ok"}}
    manifest.release["pin"]["code_repository"] = "../missing-framework"

    target_key = du._target_state_key(ctx)

    assert target_key["code_revision"] is None
    assert target_key["authority_error"]["authority_role"] == "framework"
    assert target_key["authority_error"]["requested_revision"] == (
        manifest.release["pin"]["code_revision"]
    )


def test_planner_integrity_accepts_matching_installed_framework_package(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    _outer, framework, _deployment_dir, _ctx, _manifest = _explicit_split_authority(
        tmp_path, monkeypatch
    )
    installed_root = tmp_path / "site-packages/okg"
    shutil.copytree(framework / "src/okg", installed_root)
    installed_module = installed_root / "substrate/deployment_upgrade.py"
    monkeypatch.setattr(du, "__file__", str(installed_module))

    revision = _git(framework, "rev-parse", "HEAD")
    report = du._planner_integrity_inventory(framework, revision)

    assert report["checks"]["canonical_module_path"] is False
    assert report["checks"]["planner_source_matches_revision"] is True
    assert report["trusted"] is True


def test_planner_integrity_surfaces_unevaluable_framework_provenance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    _outer, framework, _deployment_dir, _ctx, _manifest = _explicit_split_authority(
        tmp_path, monkeypatch
    )
    installed_root = tmp_path / "site-packages/okg"
    shutil.copytree(framework / "src/okg", installed_root)
    monkeypatch.setattr(
        du,
        "__file__",
        str(installed_root / "substrate/deployment_upgrade.py"),
    )
    monkeypatch.setattr(
        du,
        "_installed_framework_matches_revision",
        lambda *_args, **_kwargs: du.FrameworkProvenanceResult(
            status="unevaluable",
            reason="git_command_failed",
            operation="ls_tree",
            error_type="ValueError",
        ),
    )

    revision = _git(framework, "rev-parse", "HEAD")
    report = du._planner_integrity_inventory(framework, revision)

    assert report["framework_provenance"]["status"] == "unevaluable"
    assert report["framework_provenance"]["reason"] == "git_command_failed"
    assert report["checks"]["planner_source_matches_revision"] is False
    assert report["trusted"] is False


def test_target_catalog_inventory_assembles_explicit_framework_and_deployment_repos(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    outer, framework, _deployment_dir, ctx, manifest = _explicit_split_authority(
        tmp_path, monkeypatch
    )
    authority = du._git_target_revision(ctx, manifest)

    target = du._target_catalog_and_nomos_inventory(
        ctx,
        repo_root=authority.code_repo_root,
        revision=authority.code_revision,
        deployment_repo_root=authority.deployment_repo_root,
        deployment_revision=authority.deployment_revision,
        deployment_relative=authority.deployment_relative,
    )

    assert target["raw_manifest"]["modules"] == ["document_starter"]
    assert target["catalog_fingerprint"]
    assert target["manifest_fingerprint"]
    assert authority.code_repo_root == framework.resolve()
    assert authority.deployment_repo_root == outer.resolve()


def test_git_target_revision_refuses_missing_framework_repo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    _outer, _framework, _deployment_dir, ctx, manifest = _explicit_split_authority(
        tmp_path, monkeypatch
    )
    manifest.release["pin"]["code_repository"] = "../missing-framework"

    with pytest.raises(du.RevisionAuthorityError) as raised:
        du._git_target_revision(ctx, manifest)

    error = raised.value.data
    assert set(error) == {
        "authority_role",
        "repository",
        "requested_revision",
        "resolved_revision",
        "operation",
        "reason",
    }
    assert error["authority_role"] == "framework"
    assert error["requested_revision"] == manifest.release["pin"]["code_revision"]
    assert error["resolved_revision"] is None


def test_git_target_revision_refuses_unresolved_framework_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    _outer, framework, _deployment_dir, ctx, manifest = _explicit_split_authority(
        tmp_path, monkeypatch
    )
    manifest.release["pin"]["code_revision"] = "missing-framework-revision"

    with pytest.raises(du.RevisionAuthorityError) as raised:
        du._git_target_revision(ctx, manifest)

    error = raised.value.data
    assert error["authority_role"] == "framework"
    assert error["repository"] == str(framework.resolve())
    assert error["requested_revision"] == "missing-framework-revision"
    assert error["resolved_revision"] is None


def test_git_target_revision_refuses_unresolved_deployment_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    _outer, _framework, _deployment_dir, ctx, manifest = _explicit_split_authority(
        tmp_path, monkeypatch
    )
    manifest.release["pin"]["deployment_revision"] = "missing-deployment-revision"

    with pytest.raises(du.RevisionAuthorityError) as raised:
        du._git_target_revision(ctx, manifest)

    error = raised.value.data
    assert error["authority_role"] == "deployment"
    assert error["requested_revision"] == "missing-deployment-revision"
    assert error["resolved_revision"] is None


def test_collect_effect_inventory_reads_explicit_framework_repo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    outer, framework, _deployment_dir, ctx, _manifest = _explicit_split_authority(
        tmp_path, monkeypatch
    )
    monkeypatch.setattr(
        du,
        "_planner_integrity_inventory",
        lambda *args, **kwargs: {"trusted": True},
    )
    monkeypatch.setattr(
        du,
        "_live_migration_and_extension_inventory",
        lambda *args, **kwargs: {
            "ledger_exists": True,
            "target_migrations": [],
            "pending": [],
            "checksum_mismatches": [],
            "checksum_compatibilities": [],
            "required_extensions": {},
            "installed_extensions": {},
            "extension_work": [],
        },
    )
    monkeypatch.setattr(
        du,
        "_live_catalog_ownership",
        lambda _ctx: {
            "catalog_fingerprint": None,
            "owner_catalog_fingerprint": None,
            "manifest_fingerprint": None,
        },
    )
    monkeypatch.setattr(
        du,
        "_target_attribute_read_model_rebuildable",
        lambda _ctx, _raw_manifest: True,
    )

    _effects, errors, report = _actual_collect_effect_inventory(ctx)

    assert errors == ()
    assert report["effect_plan"]["framework_repository"] == str(framework.resolve())
    assert report["effect_plan"]["deployment_repository"] == str(outer.resolve())
    assert report["target_revision"] == _git(framework, "rev-parse", "HEAD")
    assert report["deployment_revision"] == _git(outer, "rev-parse", "HEAD")


def test_deployment_revision_change_invalidates_effect_inventory_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    outer, _framework, deployment_dir, ctx, manifest = _explicit_split_authority(
        tmp_path, monkeypatch
    )
    monkeypatch.setattr(
        du,
        "_planner_integrity_inventory",
        lambda *args, **kwargs: {"trusted": True},
    )
    monkeypatch.setattr(
        du,
        "_live_migration_and_extension_inventory",
        lambda *args, **kwargs: {
            "ledger_exists": True,
            "target_migrations": [],
            "pending": [],
            "checksum_mismatches": [],
            "checksum_compatibilities": [],
            "required_extensions": {},
            "installed_extensions": {},
            "extension_work": [],
        },
    )
    monkeypatch.setattr(
        du,
        "_live_catalog_ownership",
        lambda _ctx: {
            "catalog_fingerprint": None,
            "owner_catalog_fingerprint": None,
            "manifest_fingerprint": None,
        },
    )
    monkeypatch.setattr(
        du,
        "_target_attribute_read_model_rebuildable",
        lambda _ctx, _raw_manifest: True,
    )
    calls = 0
    real_target = du._target_catalog_and_nomos_inventory

    def target(*args, **kwargs):
        nonlocal calls
        calls += 1
        return real_target(*args, **kwargs)

    monkeypatch.setattr(du, "_target_catalog_and_nomos_inventory", target)
    _actual_collect_effect_inventory(ctx)
    old_deployment_revision = _git(outer, "rev-parse", "HEAD")
    (deployment_dir / "deployment.yaml").write_text(
        (deployment_dir / "deployment.yaml").read_text() + "description: changed\n"
    )
    _git(outer, "add", ".")
    _git(outer, "commit", "-m", "change deployment revision")
    new_deployment_revision = _git(outer, "rev-parse", "HEAD")
    manifest.release["pin"]["deployment_revision"] = new_deployment_revision

    _effects, errors, report = _actual_collect_effect_inventory(ctx)

    assert errors == ()
    assert calls == 2
    assert old_deployment_revision != new_deployment_revision
    assert report["deployment_revision"] == new_deployment_revision


def test_backup_admission_refuses_before_mutation_for_missing_framework_repo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    _outer, _framework, _deployment_dir, ctx, manifest = _explicit_split_authority(
        tmp_path, monkeypatch
    )
    manifest.release["pin"]["code_repository"] = "../missing-framework"
    monkeypatch.setattr(
        du,
        "_collect_effect_inventory",
        _actual_collect_effect_inventory,
    )
    commands: list[list[str]] = []
    ctx.backup_dir = tmp_path / "backups"
    ctx.backup_dir.mkdir()
    ctx.options = UpgradeOptions(deployment="dep")
    ctx.command_runner = lambda argv, env: commands.append(argv) or (_ for _ in ()).throw(
        AssertionError("backup command started before authority refusal")
    )
    ctx.dsn = "postgres://user@host/db"
    monkeypatch.setattr(
        du,
        "_backup_policy_config",
        lambda _ctx: {
            "verification_policy": "current_full_restore",
            "dump_strategy": {
                "format": "custom",
                "compression": "gzip",
                "compression_level": 6,
                "jobs": 1,
                "lock_wait_timeout_seconds": 30,
                },
            "restore_volume": {"kind": "host_path", "path": str(tmp_path)},
            "restore_peak_factor": 1.25,
            "max_parallel_restore_jobs": 1,
            "progress_interval_seconds": 30.0,
            "budgets_seconds": None,
        },
    )

    effects, errors, report = du._collect_effect_inventory(ctx)
    assert effects is None, report
    assert errors == ("effect_inventory_unreadable:RevisionAuthorityError",)

    result = du._phase_backup(ctx)

    assert result["ok"] is False
    assert result["reason"] == "physical rollback admission refused mutation"
    assert result["effect_inventory"]["error"]["authority_role"] == "framework"
    assert result["effect_inventory"]["error"]["requested_revision"] == (
        manifest.release["pin"]["code_revision"]
    )
    assert commands == []
    assert not any(ctx.backup_dir.iterdir())


def test_plan_only_discloses_effect_authority_refusal_without_authorizing_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A preview stays usable while preserving the execution refusal."""
    _patch_risk_admission(
        monkeypatch,
        tmp_path,
        effects=_closed_effect_checks(planner_trusted=False),
    )
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        commands.append(argv)
        if "migrations" in argv:
            return _ok({"pending": [], "destructive_pending": []})
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "state.json",
            backup_dir=tmp_path / "backups",
            readiness_profile="dogfood",
            plan_only=True,
        ),
        command_runner=runner,
    )

    backup = next(row for row in result["phases"] if row["phase"] == "backup")
    assert result["ok"] is True, json.dumps(result, indent=2, default=str)
    assert backup["ok"] is True
    assert backup["mutation_authorized"] is False
    assert backup["planned"] == "backup admission remains refused"
    assert backup["backup_policy_decision"]["mode"] == "refuse"
    assert backup["backup_policy_decision"]["reasons"] == [
        "effect.planner_trusted"
    ]
    assert backup["safety_plan"]["restore_point"][
        "authorizes_database_mutation"
    ] is False
    assert not any(command and command[0] == "pg_dump" for command in commands)


def test_git_target_revision_dirty_deployment_repo_is_not_exact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    outer, _okg_repo, deployment_dir, ctx, manifest = _vendored_authority(
        tmp_path, monkeypatch
    )
    assert du._git_target_revision(ctx, manifest).execution_checkout_exact is True

    (deployment_dir / "deployment.yaml").write_text(
        "name: dep\n"
        "release:\n  channel: dev\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules:\n  - document_starter\n"
        "description: uncommitted\n"
    )
    assert _git(outer, "status", "--porcelain=v1", "--untracked-files=no")

    dirty = du._git_target_revision(ctx, manifest)
    assert dirty.execution_checkout_exact is False
    assert dirty.target_revision_pinned is False


def test_git_target_revision_ignores_vendored_submodule_gitlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real submodule bump is the upgrade, not deployment-repo dirt."""
    from okg.substrate import deployment_upgrade as du

    okg_repo = tmp_path / "okg-source"
    _init_git_repo(okg_repo)
    _write_vendored_okg_repo(okg_repo)
    _git(okg_repo, "add", ".")
    _git(okg_repo, "commit", "-m", "vendored okg")

    outer = tmp_path / "deployment-repo"
    deployment_dir = outer / "deployments/dep"
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        "name: dep\nrelease:\n  channel: dev\n"
    )
    _init_git_repo(outer)
    _git(
        outer,
        "-c",
        "protocol.file.allow=always",
        "submodule",
        "add",
        str(okg_repo),
        "vendor/okg",
    )
    _git(outer, "add", ".")
    _git(outer, "commit", "-m", "deployment with vendored okg")

    # `git submodule add` clones a fresh working copy; it inherits no identity
    # from the source repo, and CI runners have no global git identity.
    submodule = outer / "vendor/okg"
    _git(submodule, "config", "user.email", "test@example.com")
    _git(submodule, "config", "user.name", "Test")
    monkeypatch.setattr(
        du,
        "__file__",
        str(submodule / _PLANNER_MODULE_RELPATH),
    )
    manifest = SimpleNamespace(
        deployment_dir=deployment_dir,
        release={"channel": "dev"},
    )
    ctx = SimpleNamespace(options=UpgradeOptions(deployment="dep"))

    # Bump the submodule the way an operator adopting a new okg revision would:
    # commit inside vendor/okg without committing the gitlink in the outer repo.
    (submodule / "src/okg/substrate/db/schema.sql").write_text(
        "CREATE SCHEMA okg;\n-- bumped\n"
    )
    _git(submodule, "add", ".")
    _git(submodule, "commit", "-m", "bump okg")
    assert [
        line.strip()
        for line in _git(
            outer, "status", "--porcelain=v1", "--untracked-files=no"
        ).splitlines()
    ] == ["M vendor/okg"]

    authority = du._git_target_revision(ctx, manifest)
    assert authority.vendored is True
    assert authority.code_repo_root == submodule.resolve()
    assert authority.code_revision == _git(submodule, "rev-parse", "HEAD")
    assert authority.execution_checkout_exact is True
    assert authority.target_revision_pinned is True
    planner = du._planner_integrity_inventory(
        authority.code_repo_root,
        authority.planner_revision,
        authority.deployment_repo_root,
    )
    assert planner["trusted"] is True, planner["dirty_paths"]


def test_planner_integrity_refuses_untracked_deployment_repo_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Vendored admission is no laxer than in-repo about uncommitted catalog files."""
    from okg.substrate import deployment_upgrade as du

    _outer, _okg_repo, deployment_dir, ctx, manifest = _vendored_authority(
        tmp_path, monkeypatch
    )
    authority = du._git_target_revision(ctx, manifest)
    assert du._planner_integrity_inventory(
        authority.code_repo_root,
        authority.planner_revision,
        authority.deployment_repo_root,
    )["trusted"] is True

    (deployment_dir / "schemas/uncommitted.yaml").write_text("classes: {}\n")
    refused = du._planner_integrity_inventory(
        authority.code_repo_root,
        authority.planner_revision,
        authority.deployment_repo_root,
    )
    assert refused["trusted"] is False
    assert refused["checks"]["deployment_untracked_worktree_pristine"] is False
    assert (
        "?? deployments/dep/schemas/uncommitted.yaml" in refused["dirty_paths"]
    )


def test_sibling_worktree_is_not_treated_as_a_vendored_code_repo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two worktrees of one repository are not a vendored install."""
    from okg.substrate import deployment_upgrade as du

    repo = tmp_path / "repo"
    deployment_dir = repo / "deployments/dep"
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        "name: dep\nrelease:\n  channel: dev\n"
    )
    planner = repo / _PLANNER_MODULE_RELPATH
    planner.parent.mkdir(parents=True)
    planner.write_text("# planner\n")
    _init_git_repo(repo)
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "in-repo deployment")
    lane = tmp_path / "lane"
    _git(repo, "worktree", "add", str(lane), "-b", "lane")
    monkeypatch.setattr(du, "__file__", str(lane / _PLANNER_MODULE_RELPATH))

    manifest = SimpleNamespace(
        deployment_dir=deployment_dir,
        release={"channel": "dev"},
    )
    ctx = SimpleNamespace(options=UpgradeOptions(deployment="dep"))
    authority = du._git_target_revision(ctx, manifest)

    assert authority.vendored is False
    assert authority.code_repo_root == repo.resolve()
    # The lane's module is not the deployment repo's canonical module, so the
    # planner refuses rather than silently planning from the wrong checkout.
    planner_report = du._planner_integrity_inventory(
        authority.code_repo_root,
        authority.planner_revision,
        authority.deployment_repo_root,
    )
    assert planner_report["checks"]["canonical_module_path"] is False
    assert planner_report["trusted"] is False


def test_planner_integrity_trusts_vendored_checkout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    _outer, okg_repo, _deployment_dir, ctx, manifest = _vendored_authority(
        tmp_path, monkeypatch
    )
    authority = du._git_target_revision(ctx, manifest)

    trusted = du._planner_integrity_inventory(
        authority.code_repo_root,
        authority.planner_revision,
    )
    assert trusted["trusted"] is True
    assert trusted["checks"]["canonical_module_path"] is True

    (okg_repo / "src/okg/substrate/db/schema.sql").write_text("DROP SCHEMA okg;\n")
    refused = du._planner_integrity_inventory(
        authority.code_repo_root,
        authority.planner_revision,
    )
    assert refused["trusted"] is False


def test_target_catalog_inventory_assembles_two_repositories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    _outer, _okg_repo, _deployment_dir, ctx, manifest = _vendored_authority(
        tmp_path, monkeypatch
    )
    authority = du._git_target_revision(ctx, manifest)

    target = du._target_catalog_and_nomos_inventory(
        ctx,
        repo_root=authority.code_repo_root,
        revision=authority.code_revision,
        deployment_repo_root=authority.deployment_repo_root,
        deployment_revision=authority.deployment_revision,
        deployment_relative=authority.deployment_relative,
    )

    assert target["raw_manifest"]["modules"] == ["document_starter"]
    assert target["catalog_fingerprint"]
    assert target["manifest_fingerprint"]
    assert target["pact_emission_contract"]["nodes"] == (
        ("pact_change", "pact-change:", "change"),
    )

    # A deployment directory that overlaps the ontology tree would let the
    # second extraction overwrite proven code-repo content; refuse it.
    with pytest.raises(ValueError, match="overlaps the okg ontology tree"):
        du._target_catalog_and_nomos_inventory(
            ctx,
            repo_root=authority.code_repo_root,
            revision=authority.code_revision,
            deployment_repo_root=authority.deployment_repo_root,
            deployment_revision=authority.deployment_revision,
            deployment_relative=Path(
                "src/okg/substrate/library/ontologies/document_starter"
            ),
        )
    with pytest.raises(ValueError, match="overlaps the okg ontology tree"):
        du._target_catalog_and_nomos_inventory(
            ctx,
            repo_root=authority.code_repo_root,
            revision=authority.code_revision,
            deployment_repo_root=authority.deployment_repo_root,
            deployment_revision=authority.deployment_revision,
            deployment_relative=Path("."),
        )


def test_sync_code_scopes_git_commands_to_framework_repository(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.deployment_release import ReleaseArtifact

    framework = tmp_path / "framework"
    framework.mkdir()
    commands: list[list[str]] = []
    authority = SimpleNamespace(
        code_repo_root=framework,
        code_revision="target-revision",
    )
    manifest = SimpleNamespace(
        release={
            "channel": "prod",
            "pin": {"code_revision": "target-revision"},
        },
    )
    ctx = SimpleNamespace(
        options=SimpleNamespace(
            deployment="dep",
            sync_code=True,
        ),
        env={},
        command_runner=lambda argv, env: (
            commands.append(argv) or CommandResult(0)
        ),
    )
    monkeypatch.setattr(du, "_resolve_upgrade_deployment", lambda _ctx: manifest)
    monkeypatch.setattr(du, "_git_target_revision", lambda _ctx, _manifest: authority)
    monkeypatch.setattr(
        "okg.substrate.deployment_release.current_release_artifact",
        lambda _manifest: ReleaseArtifact(
            code_revision="old-revision",
            schema_checksum="schema",
            catalog_fingerprint="catalog",
            skill_bundle_hash="skills",
        ),
    )

    result = du._phase_sync_code(ctx)

    assert result == {"ok": True, "code_revision": "target-revision"}
    assert commands == [
        ["git", "-C", str(framework), "fetch", "--all", "--prune"],
        ["git", "-C", str(framework), "checkout", "target-revision"],
    ]


def test_dev_sync_code_skips_release_provenance_for_unversioned_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    manifest = SimpleNamespace(
        deployment_dir=tmp_path / "staged-deployment",
        release={"channel": "dev"},
    )
    ctx = SimpleNamespace(
        options=SimpleNamespace(deployment="dep", sync_code=False),
        env={},
        command_runner=lambda _argv, _env: CommandResult(0),
    )
    monkeypatch.setattr(du, "_resolve_upgrade_deployment", lambda _ctx: manifest)
    monkeypatch.setattr(
        "okg.substrate.deployment_release.current_release_artifact",
        lambda _manifest: (_ for _ in ()).throw(
            AssertionError("dev fixtures must not require release provenance")
        ),
    )

    assert du._phase_sync_code(ctx) == {
        "ok": True,
        "skipped": "dev channel tracks the current checkout",
        "code_revision": None,
    }


# A deployment-private schema kept in one place and reached from the
# deployment's schema dir -- the shape issue #1012 reports from cooper.
_SHARED_SCHEMA = """\
id: https://palimpzest.mit.edu/okg/modules/physics
name: physics
default_range: string
classes:
  PhysicsRun:
    description: A deployment-private subtype reached through a tracked symlink.
    attributes:
      run_id:
        required: true
    annotations:
      archetype: artifact
"""


def _shared_schema_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    mode: str,
) -> tuple[Path, Path, Path, SimpleNamespace, SimpleNamespace]:
    """Vendored layout whose deployment reaches a schema kept outside it.

    ``mode`` picks how the deployment names that shared file: ``symlink`` is
    the tracked link issue #1012 reports, ``copy`` is the operator workaround
    that dereferences it by hand, and ``absent`` leaves the schema out of the
    deployment entirely. The shared file always lives outside the archived
    pathspec, so a link to it is not satisfied by the deployment archive alone.
    """
    outer, okg_repo, deployment_dir, ctx, manifest = _vendored_authority(
        tmp_path, monkeypatch
    )
    shared = outer / "ontology/physics_v5.yaml"
    shared.parent.mkdir(parents=True)
    shared.write_text(_SHARED_SCHEMA)
    seen = deployment_dir / "schemas/physics.yaml"
    if mode == "symlink":
        seen.symlink_to(
            os.path.relpath(shared, seen.parent)
        )
    elif mode == "copy":
        seen.write_text(_SHARED_SCHEMA)
    elif mode != "absent":
        raise AssertionError(f"unknown fixture mode: {mode}")
    _git(outer, "add", "ontology", "deployments")
    _git(outer, "commit", "-m", f"shared ontology ({mode})")
    return outer, okg_repo, deployment_dir, ctx, manifest


def _target_catalog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    mode: str,
) -> dict[str, object]:
    from okg.substrate import deployment_upgrade as du

    _outer, _okg_repo, _deployment_dir, ctx, manifest = _shared_schema_authority(
        tmp_path, monkeypatch, mode=mode
    )
    authority = du._git_target_revision(ctx, manifest)
    return du._target_catalog_and_nomos_inventory(
        ctx,
        repo_root=authority.code_repo_root,
        revision=authority.code_revision,
        deployment_repo_root=authority.deployment_repo_root,
        deployment_revision=authority.deployment_revision,
        deployment_relative=authority.deployment_relative,
    )


def _linked_repo(root: Path, link: str, target: str) -> Path:
    """A one-deployment repository whose schema dir holds one tracked link."""
    deployment_dir = root / "deployments/dep"
    (deployment_dir / "schemas").mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        "name: dep\nrelease:\n  channel: dev\n"
    )
    (deployment_dir / link).symlink_to(target)
    _init_git_repo(root)
    _git(root, "add", "-A")
    _git(root, "commit", "-m", "tracked symlink")
    return root


def test_target_catalog_inventory_resolves_tracked_schema_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#1012: a tracked link no longer makes the candidate tree unreadable."""
    from okg.substrate import deployment_upgrade as du

    outer, _okg_repo, _deployment_dir, _ctx, _manifest = _shared_schema_authority(
        tmp_path, monkeypatch, mode="symlink"
    )
    destination = tmp_path / "candidate"
    destination.mkdir()

    du._extract_candidate_tree(
        repo_root=outer,
        revision=_git(outer, "rev-parse", "HEAD"),
        paths=["deployments/dep"],
        destination=destination,
        owners={},
    )

    landed = destination / "deployments/dep/schemas/physics.yaml"
    # The link's target lives outside the archived pathspec, so admitting the
    # link without resolving it would leave a dangling entry that the catalog
    # readers swallow -- the candidate tree must carry the content itself.
    assert landed.is_file()
    assert not landed.is_symlink()
    assert landed.read_text() == _SHARED_SCHEMA


def test_symlinked_schema_fingerprints_match_a_real_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The link contributes exactly what a real file at that path would."""
    linked = _target_catalog(tmp_path / "linked", monkeypatch, mode="symlink")
    copied = _target_catalog(tmp_path / "copied", monkeypatch, mode="copy")
    absent = _target_catalog(tmp_path / "absent", monkeypatch, mode="absent")

    assert linked["catalog_fingerprint"] == copied["catalog_fingerprint"]
    # Not a vacuous equality: dropping the schema really does move the
    # fingerprint, so matching the copy means the subtype was composed in.
    assert linked["catalog_fingerprint"] != absent["catalog_fingerprint"]


def test_effect_inventory_is_readable_with_a_tracked_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The reported symptom: effect_inventory came back `unreadable`."""
    from okg.substrate import deployment_upgrade as du

    _outer, _okg_repo, _deployment_dir, ctx, _manifest = _shared_schema_authority(
        tmp_path, monkeypatch, mode="symlink"
    )
    monkeypatch.setattr(
        du,
        "_live_migration_and_extension_inventory",
        lambda *args, **kwargs: {
            "ledger_exists": True,
            "target_migrations": [],
            "pending": [],
            "checksum_mismatches": [],
            "checksum_compatibilities": [],
            "required_extensions": {},
            "installed_extensions": {},
            "extension_work": [],
        },
    )
    monkeypatch.setattr(
        du,
        "_live_catalog_ownership",
        lambda _ctx: {
            "catalog_fingerprint": None,
            "owner_catalog_fingerprint": None,
            "manifest_fingerprint": None,
        },
    )
    monkeypatch.setattr(
        du,
        "_target_attribute_read_model_rebuildable",
        lambda _ctx, _raw_manifest: True,
    )

    _effects, errors, report = _actual_collect_effect_inventory(ctx)

    assert errors == ()
    assert report.get("status") != "unreadable"


# A refusal that does not tell the operator what to do is the defect #1012 was
# filed about, so every refusal has to end in one of these.
_REMEDY_MARKERS = (
    "replace it",
    "replace the symlink",
    "replace the first link",
    "repoint it",
    "repoint the link",
    "commit the target",
    "shorten the chain",
    "rename",
    "remove it",
    "keep the two repositories",
    "file it with",
)


def _refusal_message(
    repo: Path,
    destination: Path,
    *,
    paths: list[str] | None = None,
) -> str:
    from okg.substrate import deployment_upgrade as du

    with pytest.raises(ValueError) as excinfo:
        du._extract_candidate_tree(
            repo_root=repo,
            revision=_git(repo, "rev-parse", "HEAD"),
            paths=paths or ["deployments/dep"],
            destination=destination,
            owners={},
        )
    return str(excinfo.value)


def _assert_actionable(message: str, *, member: str, refusal: str) -> None:
    """Every refusal names the offending member and a remedy, per the PACT."""
    assert refusal in message, message
    assert member in message, f"refusal does not name {member}: {message}"
    assert any(marker in message for marker in _REMEDY_MARKERS), (
        f"refusal offers no remedy: {message}"
    )


def test_candidate_tree_refuses_symlink_escaping_the_repository(
    tmp_path: Path,
) -> None:
    repo = _linked_repo(
        tmp_path / "repo",
        "schemas/escape.yaml",
        "../../../../../etc/passwd",
    )
    destination = tmp_path / "candidate"
    destination.mkdir()

    _assert_actionable(
        _refusal_message(repo, destination),
        member="deployments/dep/schemas/escape.yaml",
        refusal="pointing outside the repository",
    )


def test_candidate_tree_refuses_untracked_symlink_target(
    tmp_path: Path,
) -> None:
    repo = _linked_repo(
        tmp_path / "repo",
        "schemas/missing.yaml",
        "../../../ontology/never_committed.yaml",
    )
    destination = tmp_path / "candidate"
    destination.mkdir()

    _assert_actionable(
        _refusal_message(repo, destination),
        member="deployments/dep/schemas/missing.yaml",
        refusal="not tracked at the planned revision",
    )


def test_candidate_tree_refuses_symlink_into_its_own_parent(
    tmp_path: Path,
) -> None:
    """A link at `a/b` pointing at `a` would copy its container into itself."""
    repo = _linked_repo(tmp_path / "repo", "schemas/self", ".")
    destination = tmp_path / "candidate"
    destination.mkdir()

    _assert_actionable(
        _refusal_message(repo, destination),
        member="deployments/dep/schemas/self",
        refusal="pointing at itself or its own parent",
    )


def test_candidate_tree_resolves_symlink_to_a_directory(
    tmp_path: Path,
) -> None:
    """A linked directory materializes as a real directory, contents and all."""
    from okg.substrate import deployment_upgrade as du

    root = tmp_path / "repo"
    shared = root / "ontology/physics"
    shared.mkdir(parents=True)
    (shared / "core.yaml").write_text("classes: {}\n")
    (shared / "nested/inner.yaml").parent.mkdir()
    (shared / "nested/inner.yaml").write_text("classes: {inner: {}}\n")
    repo = _linked_repo(root, "schemas/physics", "../../../ontology/physics")
    destination = tmp_path / "candidate"
    destination.mkdir()

    du._extract_candidate_tree(
        repo_root=repo,
        revision=_git(repo, "rev-parse", "HEAD"),
        paths=["deployments/dep"],
        destination=destination,
        owners={},
    )

    landed = destination / "deployments/dep/schemas/physics"
    assert landed.is_dir() and not landed.is_symlink()
    assert (landed / "core.yaml").read_text() == "classes: {}\n"
    assert (landed / "nested/inner.yaml").read_text() == "classes: {inner: {}}\n"


def test_candidate_tree_refuses_absolute_symlink(tmp_path: Path) -> None:
    """An absolute link target never resolves inside the repository."""
    repo = _linked_repo(tmp_path / "repo", "schemas/abs.yaml", "/etc/passwd")
    destination = tmp_path / "candidate"
    destination.mkdir()

    _assert_actionable(
        _refusal_message(repo, destination),
        member="deployments/dep/schemas/abs.yaml",
        refusal="pointing outside the repository",
    )


def test_candidate_tree_refuses_symlink_cycle(tmp_path: Path) -> None:
    """Mutual links close the closure loop but must not survive materializing.

    `Path.resolve(strict=True)` reports a cycle as RuntimeError, not OSError,
    on the supported Python floor -- catching only OSError would surface a bare
    `effect_inventory_unreadable:RuntimeError` naming neither link nor remedy.
    """
    root = tmp_path / "repo"
    ring = root / "ring"
    ring.mkdir(parents=True)
    (ring / "a").symlink_to("b")
    (ring / "b").symlink_to("a")
    repo = _linked_repo(root, "schemas/head.yaml", "../../../ring/a")
    destination = tmp_path / "candidate"
    destination.mkdir()

    _assert_actionable(
        _refusal_message(repo, destination),
        member="deployments/dep/schemas/head.yaml",
        refusal="does not resolve to a tracked path",
    )


def test_candidate_tree_archives_an_ancestor_target_whole(tmp_path: Path) -> None:
    """A link to a directory already present as a tar PREFIX is not truncated.

    `git archive -- a/b/c` creates `a/` and `a/b/` as bare prefixes. A later
    link pointing at `a` would find it on disk and, if presence were taken for
    coverage, be materialized from that empty shell -- a candidate tree missing
    real files, which the catalog readers swallow rather than report.
    """
    root = tmp_path / "repo"
    lib = root / "src/okg/substrate/library"
    (lib / "ontologies/mod").mkdir(parents=True)
    (lib / "ontologies/mod/schema.yaml").write_text("classes: {}\n")
    (lib / "sources").mkdir()
    (lib / "sources/pact.py").write_text("SENTINEL = 1\n")
    repo = _linked_repo(
        root,
        "schemas/lib",
        "../../../src/okg/substrate/library",
    )
    destination = tmp_path / "candidate"
    destination.mkdir()

    from okg.substrate import deployment_upgrade as du

    du._extract_candidate_tree(
        repo_root=repo,
        revision=_git(repo, "rev-parse", "HEAD"),
        # The ontologies pathspec creates the prefix; the deployment carries a
        # link to its grandparent. This is the production shape.
        paths=["deployments/dep", "src/okg/substrate/library/ontologies"],
        destination=destination,
        owners={},
    )

    landed = destination / "deployments/dep/schemas/lib"
    assert (landed / "ontologies/mod/schema.yaml").is_file()
    assert (landed / "sources/pact.py").read_text() == "SENTINEL = 1\n"


def test_candidate_tree_refuses_a_link_over_an_existing_path(
    tmp_path: Path,
) -> None:
    """Two repositories, one candidate tree: a link may not land on a directory.

    The first repository materializes `shared/mod` as a real directory; the
    second carries a symlink at the same name. Writing the link would either
    fail obscurely or shadow proven content, so it refuses by name.
    """
    from okg.substrate import deployment_upgrade as du

    first = tmp_path / "first"
    (first / "shared/mod").mkdir(parents=True)
    (first / "shared/mod/x.yaml").write_text("classes: {}\n")
    _init_git_repo(first)
    _git(first, "add", "-A")
    _git(first, "commit", "-m", "real directory")

    second = tmp_path / "second"
    (second / "shared").mkdir(parents=True)
    (second / "other").mkdir()
    (second / "other/real.yaml").write_text("classes: {}\n")
    (second / "shared/mod").symlink_to("../other/real.yaml")
    _init_git_repo(second)
    _git(second, "add", "-A")
    _git(second, "commit", "-m", "link at the same name")

    destination = tmp_path / "candidate"
    destination.mkdir()
    owners: dict[str, Path] = {}
    du._extract_candidate_tree(
        repo_root=first,
        revision=_git(first, "rev-parse", "HEAD"),
        paths=["shared"],
        destination=destination,
        owners=owners,
    )

    with pytest.raises(ValueError) as excinfo:
        du._extract_candidate_tree(
            repo_root=second,
            revision=_git(second, "rev-parse", "HEAD"),
            paths=["shared"],
            destination=destination,
            owners=owners,
        )
    _assert_actionable(
        str(excinfo.value),
        member="shared/mod",
        refusal="lands on a path the candidate tree already holds",
    )


def test_materialize_refuses_a_link_to_a_special_file(tmp_path: Path) -> None:
    """Defense in depth behind the archive scanner.

    `_extract_candidate_tree` writes only files and directories, so a link
    resolving to something else cannot arise from `git archive` today. Proving
    the arm still refuses keeps it from rotting into a silent copy attempt.
    """
    from okg.substrate import deployment_upgrade as du

    destination = tmp_path / "candidate"
    (destination / "dep").mkdir(parents=True)
    fifo = destination / "dep/pipe"
    os.mkfifo(fifo)
    link = destination / "dep/link"
    link.symlink_to("pipe")

    with pytest.raises(ValueError) as excinfo:
        du._materialize_candidate_link(
            link,
            "dep/link",
            "dep/pipe",
            destination.resolve(),
        )
    _assert_actionable(
        str(excinfo.value),
        member="dep/link",
        refusal="which is neither a file nor a directory",
    )


def test_candidate_member_ledger_refuses_a_second_repository(tmp_path: Path) -> None:
    """Defense in depth behind `_require_link_target_authority`.

    Reaching this through `_target_catalog_and_nomos_inventory` is not possible
    today -- the link-target check fires first, and the deployment/ontology
    overlap check covers the base pathspecs -- so the ledger is exercised
    directly rather than left unproven.
    """
    from okg.substrate import deployment_upgrade as du

    first = tmp_path / "first"
    second = tmp_path / "second"
    owners: dict[Path, Path] = {}

    assert du._claim_candidate_member("shared/schema.yaml", first, owners) is True
    # Same repository, same name: already materialized, nothing to redo.
    assert du._claim_candidate_member("shared/schema.yaml", first, owners) is False
    with pytest.raises(ValueError, match="crosses repository authority"):
        du._claim_candidate_member("shared/schema.yaml", second, owners)


@pytest.mark.parametrize(
    ("kind", "name", "expected"),
    [
        ("absolute", "/etc/passwd", "unsafe archive member"),
        ("parent", "../escape.yaml", "unsafe archive member"),
        ("fifo", "deployments/dep/pipe", "neither a file, a directory, nor a"),
    ],
    # Stable ids: the generated ones embed the expected message, so the PACT
    # test_plan could never name a case that survives a wording change.
    ids=["absolute", "parent", "fifo"],
)
def test_candidate_tree_refuses_unsafe_archive_members(
    kind: str,
    name: str,
    expected: str,
) -> None:
    """Tar-slip and special-file refusals survive admitting symlinks."""
    import io
    import tarfile

    from okg.substrate import deployment_upgrade as du

    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as writer:
        member = tarfile.TarInfo(name)
        if kind == "fifo":
            member.type = tarfile.FIFOTYPE
            writer.addfile(member)
        else:
            member.size = 0
            writer.addfile(member, io.BytesIO(b""))
    buffer.seek(0)

    with tarfile.open(fileobj=buffer, mode="r:") as archive:
        with pytest.raises(ValueError, match=expected):
            du._scan_candidate_archive_members(archive)


def test_candidate_tree_refuses_unclosed_symlink_chain(
    tmp_path: Path,
) -> None:
    """A short chain resolves; one longer than the bound refuses loudly."""
    from okg.substrate import deployment_upgrade as du

    def build(root: Path, links: int) -> Path:
        """A repo whose deployment schema reaches a file through `links` hops.

        Hop 1 is the schema-dir link itself; hops 2..n live in `chain/` and the
        last one lands on a regular file.
        """
        chain = root / "chain"
        chain.mkdir(parents=True)
        (chain / "real.yaml").write_text("classes: {}\n")
        for index in range(1, links):
            target = "real.yaml" if index == links - 1 else f"l{index + 1}"
            (chain / f"l{index}").symlink_to(target)
        return _linked_repo(root, "schemas/head.yaml", "../../../chain/l1")

    # Exactly at the bound. Round 0 is spent archiving the pathspec, and the
    # final round fetches the regular file the last link lands on, so the
    # longest chain that resolves is CANDIDATE_TREE_LINK_ROUNDS - 1 links --
    # the number the refusal message and the runbook both quote.
    bound = du.CANDIDATE_TREE_LINK_ROUNDS - 1
    at_bound = build(tmp_path / "at-bound", bound)
    destination = tmp_path / "at-bound-candidate"
    destination.mkdir()
    du._extract_candidate_tree(
        repo_root=at_bound,
        revision=_git(at_bound, "rev-parse", "HEAD"),
        paths=["deployments/dep"],
        destination=destination,
        owners={},
    )
    landed = destination / "deployments/dep/schemas/head.yaml"
    assert landed.is_file() and not landed.is_symlink()

    # One hop further refuses, naming a LINK in the chain rather than the
    # innocent regular file the chain ends at.
    long_chain = build(tmp_path / "long", bound + 1)
    long_destination = tmp_path / "long-candidate"
    long_destination.mkdir()
    _assert_actionable(
        _refusal_message(long_chain, long_destination),
        member=f"chain/l{bound}",
        refusal="symlink chain longer than",
    )


def test_vendored_layout_resolves_links_per_repository(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each link resolves against the repository that carries it.

    Both repositories carry an indirection at once: the deployment's schema is
    linked out of the outer repository, and the module schema is linked out of
    the vendored okg repository. The proof is fingerprint equality against the
    same pair of repositories with neither link -- a truthiness check would pass
    on a truncated or wrong-repository read.
    """
    from okg.substrate import deployment_upgrade as du

    def build(where: Path, *, linked: bool) -> dict[str, object]:
        outer, okg_repo, _dir, ctx, manifest = _shared_schema_authority(
            where, monkeypatch, mode="symlink" if linked else "copy"
        )
        if linked:
            module_schema = (
                okg_repo
                / "src/okg/substrate/library/ontologies/document_starter/schema.yaml"
            )
            shared = okg_repo / "shared/document_starter_schema.yaml"
            shared.parent.mkdir(parents=True)
            shutil.move(str(module_schema), str(shared))
            module_schema.symlink_to(os.path.relpath(shared, module_schema.parent))
            _git(okg_repo, "add", "-A")
            _git(okg_repo, "commit", "-m", "link the module schema")
        assert outer.exists()
        authority = du._git_target_revision(ctx, manifest)
        return du._target_catalog_and_nomos_inventory(
            ctx,
            repo_root=authority.code_repo_root,
            revision=authority.code_revision,
            deployment_repo_root=authority.deployment_repo_root,
            deployment_revision=authority.deployment_revision,
            deployment_relative=authority.deployment_relative,
        )

    linked = build(tmp_path / "linked", linked=True)
    plain = build(tmp_path / "plain", linked=False)

    assert linked["raw_manifest"]["modules"] == ["document_starter"]
    # Two links, two repositories, one catalog -- identical to the tree that
    # has no links at all.
    assert linked["catalog_fingerprint"] == plain["catalog_fingerprint"]


def test_vendored_layout_refuses_cross_repository_link_collision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One repository's link may not overwrite the other's materialized path."""
    from okg.substrate import deployment_upgrade as du

    outer, _okg_repo, deployment_dir, ctx, manifest = _vendored_authority(
        tmp_path, monkeypatch
    )
    # The outer repository tracks its own file at the ontology tree's path, and
    # links the deployment schema dir at it. Both land on one candidate-tree
    # name; the vendored okg repository already owns that name.
    shadow = outer / "src/okg/substrate/library/ontologies/document_starter/schema.yaml"
    shadow.parent.mkdir(parents=True)
    shadow.write_text("classes: {}\n")
    link = deployment_dir / "schemas/shadow.yaml"
    link.symlink_to(os.path.relpath(shadow, link.parent))
    _git(outer, "add", "src", "deployments")
    _git(outer, "commit", "-m", "shadow the ontology tree")

    authority = du._git_target_revision(ctx, manifest)
    with pytest.raises(ValueError, match="crosses repository authority"):
        du._target_catalog_and_nomos_inventory(
            ctx,
            repo_root=authority.code_repo_root,
            revision=authority.code_revision,
            deployment_repo_root=authority.deployment_repo_root,
            deployment_revision=authority.deployment_revision,
            deployment_relative=authority.deployment_relative,
        )


def test_collect_effect_inventory_reads_vendored_layout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    _outer, _okg_repo, _deployment_dir, ctx, _manifest = _vendored_authority(
        tmp_path, monkeypatch
    )
    _ = _outer
    monkeypatch.setattr(
        du,
        "_live_migration_and_extension_inventory",
        lambda *args, **kwargs: {
            "ledger_exists": True,
            "target_migrations": [],
            "pending": [],
            "checksum_mismatches": [],
            "checksum_compatibilities": [],
            "required_extensions": {},
            "installed_extensions": {},
            "extension_work": [],
        },
    )
    monkeypatch.setattr(
        du,
        "_live_catalog_ownership",
        lambda _ctx: {
            "catalog_fingerprint": None,
            "owner_catalog_fingerprint": None,
            "manifest_fingerprint": None,
        },
    )
    monkeypatch.setattr(
        du,
        "_target_attribute_read_model_rebuildable",
        lambda _ctx, _raw_manifest: True,
    )

    _effects, errors, report = _actual_collect_effect_inventory(ctx)

    assert errors == ()
    assert report.get("status") != "unreadable"
    assert report["vendored_code_repo"] is True
    assert report["planner"]["trusted"] is True
    assert report["target_revision"] == _git(_okg_repo, "rev-parse", "HEAD")
    assert report["deployment_revision"] == _git(_outer, "rev-parse", "HEAD")


@pytest.mark.parametrize("mutation", ["duplicate_contract", "class_rebind"])
def test_target_pact_emission_contract_refuses_ambiguous_bindings(
    tmp_path: Path,
    mutation: str,
) -> None:
    from okg.substrate import deployment_upgrade as du

    repo = tmp_path / "repo"
    pact_path = repo / "src/okg/substrate/library/sources/pact.py"
    pact_path.parent.mkdir(parents=True)
    contract = (
        "PACT_EMISSION_CONTRACT = {\n"
        "    'nodes': (('pact_change', 'pact-change:', 'change'),),\n"
        "    'edges': (('pact_change', 'contains', 'pact_change', 'contains'),),\n"
        "}\n"
    )
    source = (
        contract
        + (
            contract
            if mutation == "duplicate_contract"
            else ""
        )
        + "class PACTSource:\n"
        + "    emission_contract = PACT_EMISSION_CONTRACT\n"
        + (
            "PACTSource.emission_contract = {'nodes': (), 'edges': ()}\n"
            if mutation == "class_rebind"
            else ""
        )
    )
    pact_path.write_text(source)
    subprocess.run(["git", "init"], cwd=repo, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"],
        cwd=repo,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"],
        cwd=repo,
        check=True,
    )
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-m", mutation], cwd=repo, check=True)
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    with pytest.raises(ValueError, match="binding"):
        du._target_pact_emission_contract(repo, revision)


@pytest.mark.parametrize(
    "mutation",
    [
        "abbreviated_revision",
        "dev_profile",
        "missing_plan",
        "wrong_stop",
        "apply_services",
        "sync_code",
        "sync_deps",
        "skip_backup",
        "plan_only",
        "reuse_state",
        "verified_backup",
        "verified_backup_source_revision",
        "include_override",
    ],
)
def test_precheckout_target_revision_refuses_mutation_capable_shapes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    from okg.substrate import deployment_upgrade as du

    values: dict[str, object] = {
        "deployment": "dep",
        "profile": "prod",
        "precheckout_target_revision": "a" * 40,
        "lifecycle_plan": tmp_path / "plan.json",
        "stop_after": "backup",
        "apply_services": False,
        "sync_code": False,
        "sync_deps": False,
        "skip_backup": False,
        "plan_only": False,
        "reset_state": True,
    }
    updates: dict[str, dict[str, object]] = {
        "abbreviated_revision": {"precheckout_target_revision": "a" * 12},
        "dev_profile": {"profile": "dev"},
        "missing_plan": {"lifecycle_plan": None},
        "wrong_stop": {"stop_after": "preflight"},
        "apply_services": {"apply_services": True},
        "sync_code": {"sync_code": True},
        "sync_deps": {"sync_deps": True},
        "skip_backup": {"skip_backup": True},
        "plan_only": {"plan_only": True},
        "reuse_state": {"reset_state": False},
        "verified_backup": {
            "verified_backup_state": tmp_path / "backup.json",
        },
        "verified_backup_source_revision": {
            "verified_backup_source_revision": "old-revision",
        },
        "include_override": {"include": ("openspec",)},
    }
    values.update(updates[mutation])
    context_built = False

    def forbidden_context(*args, **kwargs):
        nonlocal context_built
        context_built = True
        raise AssertionError((args, kwargs))

    monkeypatch.setattr(du, "_build_context", forbidden_context)
    with pytest.raises(SubstrateError, match="exact read-only backup shape"):
        run_deployment_upgrade(UpgradeOptions(**values))
    assert context_built is False


@pytest.mark.parametrize("missing", ["lifecycle_plan", "source_revision"])
def test_verified_backup_admission_requires_plan_and_exact_source_before_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    missing: str,
) -> None:
    from okg.substrate import deployment_upgrade as du

    values: dict[str, object] = {
        "deployment": "dep",
        "verified_backup_state": tmp_path / "backup.json",
        "verified_backup_source_revision": "a" * 40,
        "lifecycle_plan": tmp_path / "plan.json",
    }
    if missing == "lifecycle_plan":
        values["lifecycle_plan"] = None
    else:
        values["verified_backup_source_revision"] = "old-revision"
    context_built = False

    def forbidden_context(*args, **kwargs):
        nonlocal context_built
        context_built = True
        raise AssertionError((args, kwargs))

    monkeypatch.setattr(du, "_build_context", forbidden_context)
    with pytest.raises(
        SubstrateError,
        match="exact source commit and lifecycle plan",
    ):
        run_deployment_upgrade(UpgradeOptions(**values))
    assert context_built is False


@pytest.mark.parametrize(
    ("mutation", "value"),
    [
        ("source_registry", "/tmp/outside.yaml"),
        ("source_registry", "../outside.yaml"),
        ("schema_dir", "/tmp/outside"),
        ("schema_dir", "../outside"),
        ("module", "../outside"),
        ("module_rule", "../../outside.yaml"),
    ],
)
def test_candidate_catalog_paths_cannot_escape_git_tree(
    tmp_path: Path,
    mutation: str,
    value: str,
) -> None:
    from okg.substrate import deployment_upgrade as du

    candidate_root = tmp_path / "candidate"
    deployment_dir = candidate_root / "deployments/dep"
    modules_root = candidate_root / "src/okg/substrate/library/ontologies"
    module_dir = modules_root / "safe"
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "source_registry.yaml").write_text("sources: {}\n")
    (deployment_dir / "schemas").mkdir()
    module_dir.mkdir(parents=True)
    (module_dir / "schema.yaml").write_text("classes: {}\n")
    raw_manifest: dict[str, object] = {
        "source_registry": "source_registry.yaml",
        "schema_dir": "schemas",
        "modules": ["safe"],
    }
    if mutation in {"source_registry", "schema_dir"}:
        raw_manifest[mutation] = value
    elif mutation == "module":
        raw_manifest["modules"] = [value]
    else:
        (module_dir / "module.yaml").write_text(
            "name: safe\n"
            "version: 1.0.0\n"
            "provides:\n"
            "  subtypes_file: schema.yaml\n"
            "  intra_cross_link_rules:\n"
            f"    - {value}\n"
        )

    with pytest.raises(ValueError, match="escapes candidate Git tree"):
        du._validate_candidate_catalog_paths(
            candidate_root=candidate_root,
            deployment_dir=deployment_dir,
            modules_root=modules_root,
            raw_manifest=raw_manifest,
        )


def test_effect_classifier_caches_target_build_but_refreshes_live_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    deployment_dir = tmp_path / "deployments/dep"
    deployment_dir.mkdir(parents=True)
    ctx = SimpleNamespace(
        options=UpgradeOptions(deployment="dep"),
        env={"OKG_IVM_INTERVAL_VIEW": "1"},
        dsn="postgres://user@host/db",
        data={},
    )
    manifest = SimpleNamespace(deployment_dir=deployment_dir)
    counters = {"target": 0, "migration": 0, "catalog": 0, "nomos": 0}
    monkeypatch.setattr(du, "_resolve_upgrade_deployment", lambda _ctx: manifest)
    monkeypatch.setattr(
        du,
        "_git_target_revision",
        lambda _ctx, _manifest: du.RevisionAuthority(
            code_repo_root=tmp_path,
            code_revision="target-revision",
            planner_revision="planner-revision",
            deployment_repo_root=tmp_path,
            deployment_revision="target-revision",
            deployment_relative=Path("deployments/dep"),
            execution_checkout_exact=True,
            target_revision_pinned=True,
        ),
    )
    monkeypatch.setattr(
        du,
        "_planner_integrity_inventory",
        lambda repo_root, revision, deployment_repo_root=None: {"trusted": True},
    )
    monkeypatch.setattr(
        du,
        "_target_known_migrations",
        lambda repo_root, revision: [{
            "migration_id": "0000_schema_snapshot",
            "checksum": "sha256:schema",
            "destructive": False,
        }],
    )
    monkeypatch.setattr(
        du,
        "_target_migration_checksum_compatibilities",
        lambda repo_root, revision: {},
    )
    monkeypatch.setattr(
        du,
        "_target_required_extensions",
        lambda repo_root, revision: {},
    )

    def target(*args, **kwargs):
        del args, kwargs
        counters["target"] += 1
        return {
            "raw_manifest": {
                "runtime": {"smoke_sources": ["pact"]},
                "native_views": {
                    "attribute_effective_view_empty_overlay_fast_path": True,
                },
            },
            "raw_registry": {
                "sources": {
                    "pact": {
                        "ownership_id": "dep.pact",
                        "source_revision_kind": "content_hash",
                        "publication_mode": "published_generation",
                        "admission_policy": {
                            "producer_kind": "source",
                            "trust_label": "repo-local-trusted",
                        },
                    },
                },
            },
            "catalog_fingerprint": "catalog-fingerprint",
            "manifest_fingerprint": "manifest-fingerprint",
            "nomos": {"required": False, "unchanged": True},
            "pact_emission_contract": None,
        }

    def refresh_nomos(_ctx, cached):
        counters["nomos"] += 1
        return dict(cached)

    def live_migrations(*args, **kwargs):
        del args, kwargs
        counters["migration"] += 1
        return {
            "ledger_exists": True,
            "target_migrations": [],
            "pending": [],
            "checksum_mismatches": [],
            "checksum_compatibilities": [],
            "required_extensions": {},
            "installed_extensions": {},
            "extension_work": [],
        }

    def live_catalog(_ctx):
        counters["catalog"] += 1
        return {
            "catalog_fingerprint": "catalog-fingerprint",
            "owner_catalog_fingerprint": "catalog-fingerprint",
            "manifest_fingerprint": "manifest-fingerprint",
        }

    monkeypatch.setattr(du, "_target_catalog_and_nomos_inventory", target)
    monkeypatch.setattr(du, "_refresh_target_nomos_inventory", refresh_nomos)
    monkeypatch.setattr(du, "_live_migration_and_extension_inventory", live_migrations)
    monkeypatch.setattr(du, "_live_catalog_ownership", live_catalog)

    first = _actual_collect_effect_inventory(ctx)
    second = _actual_collect_effect_inventory(ctx)

    assert not first[1] and not second[1]
    assert first[2]["effect_digest"] == second[2]["effect_digest"]
    assert counters == {"target": 1, "migration": 2, "catalog": 2, "nomos": 1}


def _patch_catalog_effect_inventory_inputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> SimpleNamespace:
    """Keep target planning real enough while live ownership stays PostgreSQL-backed."""
    from okg.substrate import deployment_upgrade as du

    deployment_dir = tmp_path / "deployments/legacy-preflight"
    deployment_dir.mkdir(parents=True)
    ctx = SimpleNamespace(
        options=UpgradeOptions(deployment="legacy-preflight"),
        env={"OKG_IVM_INTERVAL_VIEW": "1"},
        dsn="",
        data={},
    )
    manifest = SimpleNamespace(deployment_dir=deployment_dir)
    monkeypatch.setattr(du, "_resolve_upgrade_deployment", lambda _ctx: manifest)
    monkeypatch.setattr(
        du,
        "_git_target_revision",
        lambda _ctx, _manifest: du.RevisionAuthority(
            code_repo_root=tmp_path,
            code_revision="target-revision",
            planner_revision="planner-revision",
            deployment_repo_root=tmp_path,
            deployment_revision="target-revision",
            deployment_relative=Path("deployments/dep"),
            execution_checkout_exact=True,
            target_revision_pinned=True,
        ),
    )
    monkeypatch.setattr(
        du,
        "_planner_integrity_inventory",
        lambda repo_root, revision, deployment_repo_root=None: {"trusted": True},
    )
    monkeypatch.setattr(
        du,
        "_target_known_migrations",
        lambda repo_root, revision: [{
            "migration_id": "0000_schema_snapshot",
            "checksum": "sha256:schema",
            "destructive": False,
        }],
    )
    monkeypatch.setattr(
        du,
        "_target_migration_checksum_compatibilities",
        lambda repo_root, revision: {},
    )
    monkeypatch.setattr(
        du,
        "_target_required_extensions",
        lambda repo_root, revision: {},
    )
    monkeypatch.setattr(
        du,
        "_target_catalog_and_nomos_inventory",
        lambda *args, **kwargs: {
            "raw_manifest": {
                "runtime": {"smoke_sources": ["pact"]},
                "native_views": {
                    "attribute_effective_view_empty_overlay_fast_path": True,
                },
            },
            "raw_registry": {
                "sources": {
                    "pact": {
                        "module": "okg.substrate.library.sources.pact",
                        "class": "PACTSource",
                        "source_class": "discovery_crawl",
                        "ownership_id": "legacy-preflight.pact",
                        "source_revision_kind": "content_hash",
                        "publication_mode": "published_generation",
                        "params": {},
                        "admission_policy": {
                            "producer_kind": "source",
                            "trust_label": "repo-local-trusted",
                        },
                    },
                },
            },
            "catalog_fingerprint": "catalog-fingerprint",
            "manifest_fingerprint": "manifest-fingerprint",
            "nomos": {"required": False, "unchanged": True},
            "pact_emission_contract": None,
        },
    )
    monkeypatch.setattr(
        du,
        "_refresh_target_nomos_inventory",
        lambda _ctx, cached: dict(cached),
    )
    monkeypatch.setattr(
        du,
        "_live_migration_and_extension_inventory",
        lambda *args, **kwargs: {
            "ledger_exists": True,
            "target_migrations": [],
            "pending": [],
            "checksum_mismatches": [],
            "checksum_compatibilities": [],
            "required_extensions": {},
            "installed_extensions": {},
            "extension_work": [],
        },
    )
    return ctx


def _catalog_effect_rollback_decision(effects, errors):
    from okg.substrate.deployment_backup_policy import (
        PhysicalRollbackPolicyInput,
        select_physical_rollback_policy,
    )

    return select_physical_rollback_policy(
        PhysicalRollbackPolicyInput(
            policy_mode="current_full_restore",
            release_channel="prod",
            readiness_profile="ci",
            deployment_class="production",
            destructive_confirmed=False,
            effects=effects,
            recovery=None,
            effect_errors=errors,
            recovery_errors=(),
        )
    )


@pytest.mark.postgres
def test_legacy_catalog_ownership_preflight_selects_full_restore(
    tmp_path: Path,
    okg_workspace_isolated_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    from okg.substrate import deployment_upgrade as du
    from okg.substrate.catalog.ownership import ensure_catalog_ownership_schema
    from okg.substrate.deployment_backup_policy import (
        FULL_RESTORE,
        REFUSE,
    )

    dsn = okg_workspace_isolated_runtime
    deployment = "legacy-preflight"
    ctx = _patch_catalog_effect_inventory_inputs(monkeypatch, tmp_path)
    ctx.dsn = dsn

    def collect():
        effects, errors, report = _actual_collect_effect_inventory(ctx)
        decision = _catalog_effect_rollback_decision(effects, errors)
        return effects, errors, report, decision

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM okg.deployment_write_ownership "
            "WHERE deployment_name = %s",
            (deployment,),
        )
        cur.execute(
            "DELETE FROM okg.catalog_versions WHERE deployment_name = %s",
            (deployment,),
        )
        cur.execute(
            """
            INSERT INTO okg.catalog_versions
              (status, applied_at, deployment_name, catalog_fingerprint)
            VALUES ('applied', now(), %s, 'catalog-fingerprint')
            """,
            (deployment,),
        )
        cur.execute(
            """
            INSERT INTO okg.deployment_write_ownership
              (deployment_name, owner_fingerprint, schema_fingerprint,
               catalog_fingerprint, manifest_fingerprint)
            VALUES (%s, 'owner-fingerprint', 'schema-fingerprint',
                    'catalog-fingerprint', 'manifest-fingerprint')
            """,
            (deployment,),
        )

    try:
        current = du._live_catalog_ownership(ctx)
        effects, errors, report, decision = collect()
        assert current["ownership_schema"] == "current"
        assert current["manifest_fingerprint"] == "manifest-fingerprint"
        assert errors == ()
        assert effects is not None and effects.catalog_unchanged is True
        assert "effect.catalog_unchanged" not in decision.reasons

        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.deployment_write_ownership "
                "SET manifest_fingerprint = NULL WHERE deployment_name = %s",
                (deployment,),
            )
        current_null = du._live_catalog_ownership(ctx)
        effects, errors, report, decision = collect()
        assert current_null["ownership_schema"] == "current"
        assert current_null["manifest_fingerprint"] is None
        assert errors == ()
        assert effects is not None and effects.catalog_unchanged is False
        assert decision.mode == FULL_RESTORE, decision
        assert "effect.catalog_unchanged" in decision.reasons

        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE okg.deployment_write_ownership "
                "DROP COLUMN manifest_fingerprint"
            )
        legacy = du._live_catalog_ownership(ctx)
        effects, errors, report, decision = collect()
        assert legacy == {
            "ownership_schema": "pre_709",
            "catalog_fingerprint": "catalog-fingerprint",
            "owner_catalog_fingerprint": "catalog-fingerprint",
            "manifest_fingerprint": None,
        }
        assert errors == ()
        assert effects is not None and effects.catalog_unchanged is False
        assert report["checks"]["catalog_unchanged"] is False
        assert decision.mode == FULL_RESTORE, decision
        assert "effect.catalog_unchanged" in decision.reasons

        with psycopg.connect(dsn) as conn:
            ensure_catalog_ownership_schema(conn)
        migrated = du._live_catalog_ownership(ctx)
        assert migrated["ownership_schema"] == "current"
        assert migrated["manifest_fingerprint"] is None

        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM okg.deployment_write_ownership "
                "WHERE deployment_name = %s",
                (deployment,),
            )
        no_owner = du._live_catalog_ownership(ctx)
        effects, errors, report, decision = collect()
        assert no_owner["ownership_schema"] == "current"
        assert no_owner["owner_catalog_fingerprint"] is None
        assert no_owner["manifest_fingerprint"] is None
        assert errors == ()
        assert effects is not None and effects.catalog_unchanged is False
        assert decision.mode == FULL_RESTORE, decision

        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE okg.deployment_write_ownership "
                "DROP CONSTRAINT deployment_write_ownership_pkey"
            )
            cur.execute(
                """
                INSERT INTO okg.deployment_write_ownership
                  (deployment_name, owner_fingerprint, schema_fingerprint,
                   catalog_fingerprint, manifest_fingerprint)
                VALUES
                  (%s, 'owner-a', 'schema-a', 'catalog-fingerprint', NULL),
                  (%s, 'owner-b', 'schema-b', 'catalog-fingerprint', NULL)
                """,
                (deployment, deployment),
            )
        try:
            effects, errors, report, decision = collect()
            assert effects is None
            assert errors == ("effect_inventory_unreadable:CardinalityViolation",)
            assert report["status"] == "unreadable"
            assert decision.mode == REFUSE
        finally:
            with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM okg.deployment_write_ownership "
                    "WHERE deployment_name = %s",
                    (deployment,),
                )
                cur.execute(
                    "ALTER TABLE okg.deployment_write_ownership "
                    "ADD CONSTRAINT deployment_write_ownership_pkey "
                    "PRIMARY KEY (deployment_name)"
                )

        shape_mutations = (
            (
                "ALTER TABLE okg.deployment_write_ownership "
                "DROP COLUMN claimed_by_command",
                "ALTER TABLE okg.deployment_write_ownership "
                "ADD COLUMN claimed_by_command TEXT",
            ),
            (
                "ALTER TABLE okg.deployment_write_ownership "
                "ADD COLUMN unexpected_identity TEXT",
                "ALTER TABLE okg.deployment_write_ownership "
                "DROP COLUMN unexpected_identity",
            ),
            (
                "ALTER TABLE okg.deployment_write_ownership "
                "ALTER COLUMN claimed_by_command TYPE VARCHAR",
                "ALTER TABLE okg.deployment_write_ownership "
                "ALTER COLUMN claimed_by_command TYPE TEXT",
            ),
            (
                "ALTER TABLE okg.deployment_write_ownership "
                "ALTER COLUMN claimed_by_command SET NOT NULL",
                "ALTER TABLE okg.deployment_write_ownership "
                "ALTER COLUMN claimed_by_command DROP NOT NULL",
            ),
        )
        for break_shape, restore_shape in shape_mutations:
            with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
                cur.execute(break_shape)
            try:
                effects, errors, report, decision = collect()
                assert effects is None
                assert errors == ("effect_inventory_unreadable:RuntimeError",)
                assert report["status"] == "unreadable"
                assert decision.mode == REFUSE
            finally:
                with psycopg.connect(
                    dsn, autocommit=True
                ) as conn, conn.cursor() as cur:
                    cur.execute(restore_shape)

        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE okg.deployment_write_ownership "
                "RENAME TO deployment_write_ownership_missing"
            )
        try:
            effects, errors, report, decision = collect()
            assert effects is None
            assert errors == ("effect_inventory_unreadable:UndefinedTable",)
            assert report["status"] == "unreadable"
            assert decision.mode == REFUSE
        finally:
            with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
                cur.execute(
                    "ALTER TABLE okg.deployment_write_ownership_missing "
                    "RENAME TO deployment_write_ownership"
                )

        ctx.dsn = "postgresql://127.0.0.1:1/okg_unreachable"
        effects, errors, report, decision = collect()
        assert effects is None
        assert errors
        assert errors[0].startswith("effect_inventory_unreadable:OperationalError")
        assert report["status"] == "unreadable"
        assert decision.mode == REFUSE
    finally:
        ctx.dsn = dsn
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("DROP TABLE okg.deployment_write_ownership")
            cur.execute(
                "DELETE FROM okg.catalog_versions WHERE deployment_name = %s",
                (deployment,),
            )
        with psycopg.connect(dsn) as conn:
            ensure_catalog_ownership_schema(conn)


@pytest.mark.postgres
def test_legacy_catalog_ownership_permission_failure_is_unreadable(
    tmp_path: Path,
    okg_workspace_isolated_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg
    from psycopg import sql

    dsn = okg_workspace_isolated_runtime
    role = f"okg_legacy_reader_{os.getpid()}_{time.time_ns() % 1_000_000}"
    password = "legacy-preflight-test-password"
    info = psycopg.conninfo.conninfo_to_dict(dsn)
    database = str(info["dbname"])
    restricted_info = dict(info)
    restricted_info.update({"user": role, "password": password})
    restricted_dsn = psycopg.conninfo.make_conninfo(**restricted_info)
    ctx = _patch_catalog_effect_inventory_inputs(monkeypatch, tmp_path)
    ctx.dsn = restricted_dsn

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(role),
                sql.Literal(password),
            )
        )
        cur.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(database),
                sql.Identifier(role),
            )
        )
        cur.execute(
            sql.SQL("GRANT USAGE ON SCHEMA okg TO {}").format(
                sql.Identifier(role)
            )
        )
        cur.execute(
            sql.SQL("GRANT SELECT ON okg.catalog_versions TO {}").format(
                sql.Identifier(role)
            )
        )
        cur.execute(
            sql.SQL(
                "REVOKE ALL PRIVILEGES ON "
                "okg.deployment_write_ownership FROM {}"
            ).format(sql.Identifier(role))
        )

    try:
        effects, errors, report = _actual_collect_effect_inventory(ctx)
        decision = _catalog_effect_rollback_decision(effects, errors)
        assert effects is None
        assert errors == ("effect_inventory_unreadable:InsufficientPrivilege",)
        assert report["status"] == "unreadable"
        assert "permission denied" in report["error"]["message"]
        assert decision.mode == "refuse"
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
            cur.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


@pytest.mark.postgres
def test_live_catalog_ownership_unrelated_query_failure_is_unreadable(
    tmp_path: Path,
    okg_workspace_isolated_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    ctx = _patch_catalog_effect_inventory_inputs(monkeypatch, tmp_path)
    ctx.dsn = okg_workspace_isolated_runtime
    real_connect = psycopg.connect

    class FailingCursor:
        def __init__(self, cursor):
            self._cursor = cursor

        def __enter__(self):
            self._cursor.__enter__()
            return self

        def __exit__(self, *args):
            return self._cursor.__exit__(*args)

        def execute(self, query, params=None):
            if "WITH ownership AS" in str(query):
                raise psycopg.errors.DivisionByZero(
                    "injected unrelated ownership query failure"
                )
            return self._cursor.execute(query, params)

        def fetchall(self):
            return self._cursor.fetchall()

    class FailingConnection:
        def __init__(self, connection):
            self._connection = connection

        def __enter__(self):
            self._connection.__enter__()
            return self

        def __exit__(self, *args):
            return self._connection.__exit__(*args)

        def cursor(self):
            return FailingCursor(self._connection.cursor())

    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *args, **kwargs: FailingConnection(
            real_connect(*args, **kwargs)
        ),
    )
    effects, errors, report = _actual_collect_effect_inventory(ctx)
    decision = _catalog_effect_rollback_decision(effects, errors)

    assert effects is None
    assert errors == ("effect_inventory_unreadable:DivisionByZero",)
    assert report["status"] == "unreadable"
    assert report["error"]["message"] == (
        "injected unrelated ownership query failure"
    )
    assert decision.mode == "refuse"


@pytest.mark.postgres
def test_live_catalog_ownership_lock_serializes_concurrent_ddl(
    okg_workspace_isolated_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import threading

    import psycopg

    from okg.substrate import deployment_upgrade as du

    dsn = okg_workspace_isolated_runtime
    ctx = SimpleNamespace(
        options=UpgradeOptions(deployment="legacy-preflight"),
        dsn=dsn,
    )
    real_connect = psycopg.connect
    ddl_conn = real_connect(dsn, autocommit=True)
    lock_acquired = threading.Event()
    ddl_started = threading.Event()
    ddl_finished = threading.Event()
    ddl_outcome: list[str] = []
    observed_queries: list[str] = []

    class ObservedCursor:
        def __init__(self, cursor):
            self._cursor = cursor

        def __enter__(self):
            self._cursor.__enter__()
            return self

        def __exit__(self, *args):
            return self._cursor.__exit__(*args)

        def execute(self, query, params=None):
            rendered = str(query)
            observed_queries.append(rendered)
            result = self._cursor.execute(query, params)
            if rendered.startswith("LOCK TABLE"):
                lock_acquired.set()
                assert ddl_started.wait(timeout=10)
                assert ddl_finished.wait(timeout=10)
            return result

        def fetchall(self):
            return self._cursor.fetchall()

        def fetchone(self):
            return self._cursor.fetchone()

    class ObservedConnection:
        def __init__(self, connection):
            self._connection = connection

        def __enter__(self):
            self._connection.__enter__()
            return self

        def __exit__(self, *args):
            return self._connection.__exit__(*args)

        def cursor(self):
            return ObservedCursor(self._connection.cursor())

    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *args, **kwargs: ObservedConnection(
            real_connect(*args, **kwargs)
        ),
    )

    def attempt_ddl() -> None:
        assert lock_acquired.wait(timeout=10)
        try:
            with ddl_conn.cursor() as cur:
                cur.execute("SET lock_timeout = '1s'")
                ddl_started.set()
                cur.execute(
                    "ALTER TABLE okg.deployment_write_ownership "
                    "ADD COLUMN ddl_race_column TEXT"
                )
        except psycopg.errors.LockNotAvailable:
            ddl_outcome.append("serialized")
        else:
            ddl_outcome.append("altered")
        finally:
            ddl_finished.set()

    thread = threading.Thread(target=attempt_ddl)
    thread.start()
    try:
        result = du._live_catalog_ownership(ctx)
        thread.join(timeout=10)
        assert not thread.is_alive()
        assert result["ownership_schema"] == "current"
        assert ddl_outcome == ["serialized"]
        assert observed_queries[0].startswith("LOCK TABLE")
        assert "pg_catalog.pg_attribute" in observed_queries[1]
        assert "WITH ownership AS" in observed_queries[2]
    finally:
        thread.join(timeout=10)
        ddl_conn.close()
        with real_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE okg.deployment_write_ownership "
                "DROP COLUMN IF EXISTS ddl_race_column"
            )


def test_full_restore_refuses_unknown_effects_before_pg_dump(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    _patch_routine_backup(
        monkeypatch,
        tmp_path,
        manifest=_full_restore_manifest(tmp_path / "dep-root"),
    )
    checks = _closed_effect_checks(
        smoke_source_versioned=False,
        no_external_effects=False,
    )
    monkeypatch.setattr(
        du,
        "_collect_effect_inventory",
        lambda ctx, expected_digest=None: (
            checks,
            (),
            {
                "checks": checks.to_dict(),
                "target_revision": "test-revision",
                "execution_checkout_exact": True,
            },
        ),
    )
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        commands.append(argv)
        return _routine_backup_runner(argv, env)

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "state.json",
            backup_dir=tmp_path / "backups",
            stop_after="backup",
        ),
        command_runner=runner,
    )

    assert result["failed_phase"] == "backup"
    assert "admission refused" in result["reason"]
    assert not any(command and command[0] == "pg_dump" for command in commands)


@pytest.mark.parametrize(
    "failing_claim",
    [
        "target_revision_pinned",
        "planner_trusted",
        "plan_readable",
        "smoke_source_versioned",
        "attribute_read_model_rebuildable",
    ],
)
def test_each_authority_claim_alone_refuses_before_pg_dump(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failing_claim: str,
) -> None:
    """Every authority claim must refuse on its own, not only in combination.

    The pre-existing coverage flipped two claims at once, so it stayed green
    whether or not either one was still load-bearing.
    """
    from okg.substrate import deployment_upgrade as du

    _patch_routine_backup(
        monkeypatch,
        tmp_path,
        manifest=_full_restore_manifest(tmp_path / "dep-root"),
    )
    checks = _closed_effect_checks(**{failing_claim: False})
    monkeypatch.setattr(
        du,
        "_collect_effect_inventory",
        lambda ctx, expected_digest=None: (
            checks,
            (),
            {"checks": checks.to_dict(), "target_revision": "test-revision"},
        ),
    )
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        commands.append(argv)
        return _routine_backup_runner(argv, env)

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "state.json",
            backup_dir=tmp_path / "backups",
            stop_after="backup",
        ),
        command_runner=runner,
    )

    assert result["failed_phase"] == "backup"
    assert "admission refused" in result["reason"]
    assert failing_claim in result["phases"][-1]["failed_checks"]
    assert not any(command and command[0] == "pg_dump" for command in commands)


def test_external_effect_plan_dumps_instead_of_refusing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An LLM-bearing smoke publish must get a backup, not a dead end.

    Before this, `okg up` refused at the backup phase for any deployment whose
    smoke publish reaches an external service, so migrate, catalog apply,
    nomos baseline and the publish never ran and the lifecycle state file was
    never written (issue 1013).

    The effect inventory is DERIVED from a real source registry rather than
    hand-built, so this also proves a registry can actually produce the
    (authority holds, reach does not) shape the fix depends on. Asserting the
    policy wiring against a synthetic inventory would pass even if no registry
    could ever reach it.
    """
    from okg.substrate import deployment_upgrade as du

    registry = {
        "sources": {
            "external-openspec": {
                "module": "okg.substrate.library.sources.openspec",
                "class": "OpenSpecSource",
                "source_class": "discovery_crawl",
                "ownership_id": "dep.external-openspec",
                "source_revision_kind": "git_commit",
                "publication_mode": "published_generation",
                "params": {
                    "openspec_root": "https://example.test/openspec",
                    "repo": "okg",
                },
                "admission_policy": {
                    "producer_kind": "source",
                    "trust_label": "repo-local-trusted",
                },
            },
        }
    }
    smoke = du._target_smoke_inventory(
        SimpleNamespace(options=UpgradeOptions(deployment="dep")),
        raw_manifest={"runtime": {"smoke_sources": ["external-openspec"]}},
        raw_registry=registry,
    )
    assert smoke["versioned_and_deployment_owned"] is True
    assert smoke["no_external_effects"] is False

    _patch_risk_admission(
        monkeypatch,
        tmp_path,
        effects=_closed_effect_checks(
            smoke_source_versioned=smoke["versioned_and_deployment_owned"],
            no_external_effects=smoke["no_external_effects"],
        ),
    )
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        commands.append(argv)
        if backup := _backup_command(argv):
            return backup
        if "migrations" in argv:
            return _ok({"pending": [], "destructive_pending": []})
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "state.json",
            backup_dir=tmp_path / "backups",
            readiness_profile="dogfood",
            stop_after="backup",
        ),
        command_runner=runner,
    )

    backup = result["phases"][-1]
    assert backup["ok"] is True
    assert "admission refused" not in str(result.get("reason") or "")
    # It buys a real restore point rather than skipping the archive.
    assert backup["verification_mode"] == "current_full_restore"
    assert any(command and command[0] == "pg_dump" for command in commands)
    assert (
        "effect.no_external_effects"
        in backup["backup_policy_decision"]["reasons"]
    )


@pytest.mark.parametrize(
    ("identity", "expected_database", "authority_mode", "substituted"),
    [
        # The operated database IS the declared one - nothing was substituted.
        ("ok", "cooper", "dev_isolated", False),
        ("ok", "cooper", "declared", False),
        ("isolated", "cooper", "dev_isolated", True),
        ("mismatch", "cooper", "declared", True),
        # A deployment declaring no `postgres.database` still resolves to a
        # worktree-isolated database under --profile dev.  Identity alone reads
        # `undeclared` there, so authority_mode has to carry the notice.
        ("undeclared", None, "dev_isolated", True),
        ("unresolved", None, "dev_isolated", True),
        # Outside dev isolation these say "we could not tell", not "we swapped
        # your database", and resolved_target already reports them.
        ("undeclared", None, "declared", False),
        ("unresolved", None, "declared", False),
        # An identity this build does not recognize is unproven, not proven
        # equal: it must speak up rather than default to silence.
        ("some_future_classification", "cooper", "declared", True),
    ],
)
def test_dev_isolated_target_substitution_is_reported(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    identity: str,
    expected_database: str | None,
    authority_mode: str,
    substituted: bool,
) -> None:
    """A substituted database must be named at the top of the result.

    `--profile dev` resolves a worktree-isolated database by design and then
    reports the same shape of success as a run against the real deployment.
    An operator who reached for it because the declared target refused would
    otherwise read that success as the deployment being repaired (issue 1013).
    """
    from okg.substrate import deployment_upgrade as du

    _patch_routine_backup(monkeypatch, tmp_path)
    target: dict[str, object] = {
        "deployment": "cooper",
        "authority_mode": authority_mode,
        "host": "localhost",
        "port": 5432,
        "database": "cooper_1358cf6279",
        "identity": identity,
    }
    if expected_database is not None:
        target["expected_database"] = expected_database
    monkeypatch.setattr(
        du,
        "_resolve_target",
        lambda options: {"dsn": "postgres://user@host/db", "target": target},
    )

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="cooper",
            profile="dev",
            state_file=tmp_path / "state.json",
            backup_dir=tmp_path / "backups",
            stop_after="preflight",
        ),
        command_runner=_routine_backup_runner,
    )

    assert result["requested_target"] == {
        "deployment": "cooper",
        "profile": "dev",
        "declared_database": expected_database,
    }
    assert result["resolved_target"]["database"] == "cooper_1358cf6279"
    if not substituted:
        assert "target_substitution" not in result
        return
    notice = result["target_substitution"]
    assert notice["substituted"] is True
    assert notice["identity"] == identity
    assert notice["expected_database"] == expected_database
    assert notice["operated_database"] == "cooper_1358cf6279"
    assert notice["authority_mode"] == authority_mode
    assert "database" in notice["explanation"]
    # An operator scanning the head of the output must reach the notice before
    # any per-phase success. `--json` sorts keys, so this is the text order.
    keys = list(result)
    assert keys.index("target_substitution") < keys.index("phases")


def test_unreadable_resolved_target_still_declares_a_substitution() -> None:
    """A target we cannot read is unproven, never silently `ok`."""
    from okg.substrate import deployment_upgrade as du

    notice = du._target_substitution_notice(None)

    assert notice is not None
    assert notice["substituted"] is True
    assert notice["identity"] is None
    assert "could not be read" in notice["explanation"]


def test_command_payload_in_an_allowed_param_still_fails_adapter_trust() -> None:
    """A subprocess payload under an allowed param name is an AUTHORITY failure.

    ``params_closed`` checks param NAMES only. Splitting reach out of
    ``adapter_trusted`` must not also drop the value-level half: params that
    can execute something make the adapter's behaviour unknowable, and no
    backup compensates for that, so it refuses rather than forcing one.
    """
    from okg.substrate import deployment_upgrade as du

    openspec = {
        "module": "okg.substrate.library.sources.openspec",
        "class": "OpenSpecSource",
        "source_class": "discovery_crawl",
        "source_revision_kind": "git_commit",
        "publication_mode": "published_generation",
        "ownership_id": "dep.src",
        "admission_policy": {
            "producer_kind": "source",
            "trust_label": "repo-local-trusted",
        },
    }

    def inventory(params: dict) -> dict:
        return du._target_smoke_inventory(
            SimpleNamespace(options=UpgradeOptions(deployment="dep")),
            raw_manifest={"runtime": {"smoke_sources": ["src"]}},
            raw_registry={"sources": {"src": {**openspec, "params": params}}},
        )

    remote = inventory(
        {"openspec_root": "https://example.test/openspec", "repo": "okg"}
    )
    payload = inventory(
        {
            "openspec_root": {"command": ["/bin/sh", "-c", "echo hi"]},
            "repo": "okg",
        }
    )

    # A remote endpoint is reach, not untrust: authority holds, backup forced.
    assert remote["entries"][0]["adapter_trusted"] is True
    assert remote["versioned_and_deployment_owned"] is True
    assert remote["no_external_effects"] is False

    # A command payload is untrust: it refuses, and it is external too.
    assert payload["entries"][0]["adapter_trusted"] is False
    assert payload["versioned_and_deployment_owned"] is False
    assert payload["no_external_effects"] is False


@pytest.mark.parametrize(
    ("effects", "observed", "receipt", "expected"),
    [
        # The plan has not moved: the carried-in proof is usable.
        (_closed_effect_checks(), "sha256:a", "sha256:a", []),
        # External reach alone no longer refuses here either — but only
        # because the receipt proves a current full restore already exists.
        (
            _closed_effect_checks(no_external_effects=False),
            "sha256:a",
            "sha256:a",
            [],
        ),
        # The plan moved after the backup was verified.
        (
            _closed_effect_checks(effect_digest_stable=False),
            "sha256:b",
            "sha256:a",
            ["effect_digest_stable", "effect_digest_reproduced"],
        ),
        # Digests differ while the inventory still calls itself stable.
        (
            _closed_effect_checks(),
            "sha256:b",
            "sha256:a",
            ["effect_digest_reproduced"],
        ),
        # Absence is not agreement: no receipt digest to compare against.
        (_closed_effect_checks(), "sha256:a", None, ["effect_digest_reproduced"]),
        (_closed_effect_checks(), "sha256:a", "", ["effect_digest_reproduced"]),
        # Authority claims still refuse on this path.
        (
            _closed_effect_checks(planner_trusted=False),
            "sha256:a",
            "sha256:a",
            ["planner_trusted"],
        ),
    ],
)
def test_verified_backup_state_refuses_a_plan_it_was_not_verified_against(
    effects,
    observed: str,
    receipt: str | None,
    expected: list[str],
) -> None:
    """The carried-in proof is only usable while the plan has not moved.

    This admission path never reaches ``select_physical_rollback_policy``, so
    the forced-current-backup class cannot compensate for anything here. The
    receipt digest must reproduce exactly, and a drifted digest must refuse
    rather than be recorded and ignored.
    """
    from okg.substrate import deployment_upgrade as du

    failures, reproduced = du._verified_backup_effect_failures(
        effects=effects,
        effect_errors=(),
        observed_digest=observed,
        receipt_digest=receipt,
    )

    assert failures == expected
    assert reproduced is (observed == receipt and bool(receipt))


def test_verified_backup_state_refuses_an_unreadable_inventory() -> None:
    from okg.substrate import deployment_upgrade as du

    failures, reproduced = du._verified_backup_effect_failures(
        effects=None,
        effect_errors=("effect_inventory_unreadable:ValueError",),
        observed_digest=None,
        receipt_digest="sha256:a",
    )

    assert reproduced is False
    assert "effect_inventory_unreadable:ValueError" in failures
    # Every claim is unproven when the inventory cannot be read.
    for name in (*du._REFUSAL_EFFECT_AUTHORITY_CHECKS, "effect_digest_stable"):
        assert name in failures


def test_external_reach_stays_inside_the_effect_digest() -> None:
    """The digest guards the forced path against turning external mid-run.

    ``no_external_effects`` is no longer in
    ``_REFUSAL_EFFECT_AUTHORITY_CHECKS``, so on the forced path a plan that
    becomes external between admission and the mutation boundary is caught by
    the effect digest: every capability row carries ``external`` as its own
    key AND folds it into the ``effect_mode`` label, and those rows feed
    ``effect_plan`` and therefore ``effect_digest``.  Pin both, so that
    dropping either one — say, a cleanup of the two-valued ``effect_mode``
    label — cannot quietly narrow the guard.
    """
    from okg.substrate import deployment_upgrade as du

    openspec = {
        "module": "okg.substrate.library.sources.openspec",
        "class": "OpenSpecSource",
        "source_class": "discovery_crawl",
        "source_revision_kind": "git_commit",
        "publication_mode": "published_generation",
        "ownership_id": "dep.src",
        "admission_policy": {
            "producer_kind": "source",
            "trust_label": "repo-local-trusted",
        },
    }

    def capabilities(openspec_root: str) -> list[dict]:
        return du._target_smoke_inventory(
            SimpleNamespace(options=UpgradeOptions(deployment="dep")),
            raw_manifest={"runtime": {"smoke_sources": ["src"]}},
            raw_registry={
                "sources": {
                    "src": {
                        **openspec,
                        "params": {"openspec_root": openspec_root, "repo": "okg"},
                    }
                }
            },
        )["effect_capabilities"]

    local = capabilities("${repo_root}/openspec")
    external = capabilities("https://example.test/openspec")

    assert local[0]["external"] is False
    assert external[0]["external"] is True
    assert local[0]["effect_mode"] == "deployment_owned_versioned_publish"
    assert external[0]["effect_mode"] == "unclosed_or_external"
    # Different capabilities => different effect_plan => different digest, so
    # `_revalidate_effect_authority` refuses if reach changes mid-run.
    assert local != external
    assert du._canonical_digest(local) != du._canonical_digest(external)


def test_restore_point_absence_and_skip_backup_refuse_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The flag skips work; it never manufactures restore authority."""
    _patch_risk_admission(monkeypatch, tmp_path)
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        commands.append(argv)
        return _routine_backup_runner(argv, {})

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "state.json",
            backup_dir=tmp_path / "backups",
            skip_backup=True,
        ),
        command_runner=runner,
    )

    assert result["ok"] is False
    assert result["failed_phase"] == "backup"
    assert result["phases"][-1]["mutation_authorized"] is False
    assert "cannot authorize database mutation" in result["reason"]
    assert not any("migrate" in command for command in commands)
    assert not any(command and command[0] == "pg_dump" for command in commands)


def test_target_substitution_requires_exact_acknowledgement() -> None:
    from okg.substrate import deployment_upgrade as du

    target = {
        "deployment": "dep",
        "authority_mode": "dev_isolated",
        "host": "localhost",
        "port": 5432,
        "database": "dep_isolated",
        "expected_database": "dep",
        "identity": "isolated",
    }
    options = UpgradeOptions(deployment="dep", profile="dev")
    ctx = SimpleNamespace(options=options, resolution={"target": target}, data={})
    plan = du._deployment_safety_plan(
        ctx,
        effect_report={"effect_digest": "sha256:effects", "smoke": {"entries": []}},
        restore_point_source="current_run",
        restore_point_verified=True,
    )

    assert plan["target_substitution"]["substituted"] is True
    assert plan["resolved_target"]["database"] == "dep_isolated"
    assert du._safety_plan_refusal(ctx, plan) is not None
    exact = plan["confirmation"]["digest"]
    ctx.options = dataclasses.replace(options, confirm_safety_plan=exact)
    confirmed = du._deployment_safety_plan(
        ctx,
        effect_report={"effect_digest": "sha256:effects", "smoke": {"entries": []}},
        restore_point_source="current_run",
        restore_point_verified=True,
    )
    assert confirmed["confirmation"]["exact"] is True
    assert du._safety_plan_refusal(ctx, confirmed) is None


def test_safety_plan_digest_binds_code_and_manifest() -> None:
    from okg.substrate import deployment_upgrade as du

    options = UpgradeOptions(deployment="dep")
    target = {"database": "dep", "identity": "ok"}
    ctx = SimpleNamespace(
        options=options,
        resolution={"target": target},
        data={
            "target_key": {
                **target,
                "code_revision": "revision-a",
                "manifest_digest": "sha256:manifest-a",
            }
        },
    )
    report = {"effect_digest": "sha256:effects", "smoke": {"entries": []}}
    first = du._deployment_safety_plan(
        ctx,
        effect_report=report,
        restore_point_source="current_run_verified_backup",
        restore_point_verified=False,
    )
    ctx.data["target_key"] = {
        **target,
        "code_revision": "revision-b",
        "manifest_digest": "sha256:manifest-b",
    }
    second = du._deployment_safety_plan(
        ctx,
        effect_report=report,
        restore_point_source="current_run_verified_backup",
        restore_point_verified=False,
    )

    assert first["code_revision"] == "revision-a"
    assert first["manifest_digest"] == "sha256:manifest-a"
    assert first["confirmation"]["digest"] != second["confirmation"]["digest"]


def test_resume_rechecks_exact_confirmation_and_effect_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    target = {"database": "dep", "identity": "ok"}
    target_key = {
        **target,
        "code_revision": "revision-a",
        "manifest_digest": "sha256:manifest-a",
    }
    report = {
        "effect_digest": "sha256:external-plan",
        "smoke": {
            "entries": [{
                "name": "llm-smoke",
                "external": True,
                "adapter_contract_digest": "sha256:adapter",
            }]
        },
    }
    checks = {name: True for name in du._REFUSAL_EFFECT_AUTHORITY_CHECKS}
    effects = SimpleNamespace(to_dict=lambda: checks)
    monkeypatch.setattr(
        du,
        "_collect_effect_inventory",
        lambda _ctx: (effects, (), report),
    )
    options = UpgradeOptions(deployment="dep")
    ctx = SimpleNamespace(
        options=options,
        resolution={"target": target},
        data={"target_key": target_key},
    )
    preview = du._deployment_safety_plan(
        ctx,
        effect_report=report,
        restore_point_source="independent_backup_verify",
        restore_point_verified=True,
        restore_point_evidence={"archive_digest": "sha256:archive"},
    )
    digest = preview["confirmation"]["digest"]
    ctx.options = dataclasses.replace(options, confirm_safety_plan=digest)
    persisted = du._deployment_safety_plan(
        ctx,
        effect_report=report,
        restore_point_source="independent_backup_verify",
        restore_point_verified=True,
        restore_point_evidence={"archive_digest": "sha256:archive"},
    )

    ctx.options = options
    with pytest.raises(SubstrateError, match="exact safety-plan confirmation"):
        du._revalidate_resumed_safety_contract(ctx, persisted)

    ctx.options = dataclasses.replace(options, confirm_safety_plan=digest)
    du._revalidate_resumed_safety_contract(ctx, persisted)

    drifted_report = {**report, "effect_digest": "sha256:changed-plan"}
    monkeypatch.setattr(
        du,
        "_collect_effect_inventory",
        lambda _ctx: (effects, (), drifted_report),
    )
    with pytest.raises(SubstrateError, match="safety plan drifted"):
        du._revalidate_resumed_safety_contract(ctx, persisted)


def test_external_effect_disclosure_survives_failure_and_resume() -> None:
    from okg.substrate import deployment_upgrade as du

    endpoint = "https://secret-provider.invalid/v1"
    report = {
        "effect_digest": "sha256:external-plan",
        "smoke": {
            "entries": [{
                "name": "llm-smoke",
                "external": True,
                "adapter_contract_digest": "sha256:adapter",
            }]
        },
    }
    options = UpgradeOptions(deployment="dep")
    ctx = SimpleNamespace(
        options=options,
        resolution={"target": {"database": "dep", "identity": "ok"}},
        data={},
    )
    plan = du._deployment_safety_plan(
        ctx,
        effect_report=report,
        restore_point_source="independent_backup_verify",
        restore_point_verified=True,
        restore_point_evidence={"archive_digest": "sha256:archive", "fresh": True},
    )
    ctx.options = dataclasses.replace(
        options, confirm_safety_plan=plan["confirmation"]["digest"]
    )
    plan = du._deployment_safety_plan(
        ctx,
        effect_report=report,
        restore_point_source="independent_backup_verify",
        restore_point_verified=True,
        restore_point_evidence={"archive_digest": "sha256:archive", "fresh": True},
    )
    ctx.data["backup_result"] = {
        "ok": True,
        "mutation_authorized": True,
        "backup_path": "/redacted/backup.dump",
        "backup_digest": "sha256:archive",
    }
    contract = du._safety_contract_snapshot(
        ctx, restore_point_verified=True, phase="migrate"
    )
    actions = du._recovery_actions(ctx)

    assert contract is not None
    assert contract["plan"]["effect_digest"] == "sha256:external-plan"
    assert contract["guarantees"]["provider_effects_reversible"] is False
    assert contract["plan"]["network_effects"]["classes"] == [
        "read_only_network_fetch"
    ]
    provider = next(row for row in actions if row["kind"] == "provider_manual_reconciliation")
    assert provider["automatic_replay_authorized"] is False
    snapshot = json.dumps({"plan": plan, "contract": contract, "actions": actions})
    assert endpoint not in snapshot
    assert "llm-smoke" in snapshot


def test_verified_restore_observation_appends_without_rewriting_confirmation(
    tmp_path: Path,
) -> None:
    from okg.substrate import deployment_upgrade as du

    archive = tmp_path / "verified.dump"
    archive.write_bytes(b"archive")
    archive.chmod(0o600)
    options = UpgradeOptions(deployment="dep")
    ctx = SimpleNamespace(
        options=options,
        resolution={"target": {"database": "dep", "identity": "ok"}},
        data={
            "target_key": {
                "database": "dep",
                "identity": "ok",
                "code_revision": "revision-a",
                "manifest_digest": "sha256:manifest-a",
            }
        },
    )
    plan = du._deployment_safety_plan(
        ctx,
        effect_report={"effect_digest": "sha256:effects", "smoke": {"entries": []}},
        restore_point_source="current_run_verified_backup",
        restore_point_verified=False,
    )
    confirmed_digest = plan["confirmation"]["digest"]
    ctx.data["backup_result"] = {
        "ok": True,
        "backup_path": str(archive),
        "backup_digest": "sha256:archive",
        "restore_verified": True,
        "schema_ok": True,
        "ownership_ok": True,
        "identity_checks": {"database_owner_equal": True},
    }

    contract = du._safety_contract_snapshot(
        ctx,
        restore_point_verified=True,
        phase="backup",
    )

    assert contract is not None
    assert contract["confirmed_plan_digest"] == confirmed_digest
    assert contract["plan"]["confirmation"]["digest"] == confirmed_digest
    assert contract["plan"]["restore_point"]["verified"] is False
    observed = contract["plan"]["observed_restore_point"]
    assert observed == contract["observed_restore_point"]
    assert observed["verified"] is True
    assert observed["archive_digest"] == "sha256:archive"
    assert observed["restore_checks"]["restore_verified"] is True
    assert observed["restore_checks"]["schema_ok"] is True
    assert observed["restore_checks"]["ownership_ok"] is True
    assert observed["authorizes_database_mutation"] is True
    assert contract["guarantees"]["database_mutation_authorized"] is True


def test_declared_external_smoke_adapter_contract_is_closed() -> None:
    from okg.substrate import deployment_upgrade as du

    contract = {
        "module": "vendor.llm",
        "class": "LLMSource",
        "source_class": "mutable_api",
        "allowed_params": ["endpoint", "prompt"],
        "publishable_revision_kinds": ["content_hash"],
        "publication_mode": "published_generation",
        "emits_on_demand": True,
        "external_effects": True,
    }
    manifest = {
        "runtime": {"smoke_sources": ["llm"]},
        "deployment_up": {"smoke_adapter_contracts": {"llm": contract}},
    }
    base = {
        "module": "vendor.llm",
        "class": "LLMSource",
        "source_class": "mutable_api",
        "ownership_id": "dep.llm",
        "source_revision_kind": "content_hash",
        "publication_mode": "published_generation",
        "admission_policy": {
            "producer_kind": "source",
            "trust_label": "repo-local-trusted",
        },
    }

    def inventory(params: object) -> dict:
        return du._target_smoke_inventory(
            SimpleNamespace(options=UpgradeOptions(deployment="dep")),
            raw_manifest=manifest,
            raw_registry={"sources": {"llm": {**base, "params": params}}},
        )

    admitted = inventory({"endpoint": "https://provider.invalid", "prompt": "ping"})
    malformed = inventory("endpoint=https://provider.invalid")
    non_closed = inventory({"endpoint": "https://provider.invalid", "extra": True})
    assert admitted["versioned_and_deployment_owned"] is True
    assert admitted["no_external_effects"] is False
    assert admitted["entries"][0]["adapter_contract_source"] == "deployment_manifest"
    assert malformed["entries"][0]["params_shape_valid"] is False
    assert malformed["versioned_and_deployment_owned"] is False
    assert non_closed["versioned_and_deployment_owned"] is False


def test_full_restore_effect_digest_drift_refuses_at_mutation_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    checks = _closed_effect_checks()
    monkeypatch.setattr(
        du,
        "_collect_effect_inventory",
        lambda ctx, expected_digest=None: (
            checks,
            (),
            {
                "target_revision": "target-revision",
                "effect_digest": "sha256:changed",
                "execution_checkout_exact": True,
                "smoke": {"selected_source_digest": "sha256:smoke-b"},
                "checks": checks.to_dict(),
            },
        ),
    )
    result = du._revalidate_effect_authority(
        SimpleNamespace(data={}),
        {
            "verification_mode": "current_full_restore",
            "effect_inventory": {
                "target_revision": "target-revision",
                "effect_digest": "sha256:original",
            },
        },
    )

    assert result["ok"] is False
    assert result["target_revision_stable"] is True
    assert result["effect_digest_stable"] is False
    assert result["expected_effect_digest"] == "sha256:original"


def _live_checkout_planner_dirt(deployment_dir: Path) -> list[str]:
    """Dirty paths the production planner sees in the checkout under test.

    Asks the production predicate rather than re-deriving it from ``git
    status``: the planner resolves its own code/deployment repository pair,
    counts tracked and untracked dirt separately, and excludes a vendored
    checkout.  A gate written independently of it could disagree with the very
    refusal it exists to anticipate.
    """
    from okg.substrate import deployment_upgrade as du

    repo_root, deployment_repo_root = du._resolve_repo_authorities(deployment_dir)
    head = str(du._git_command(repo_root, ["rev-parse", "HEAD"])).strip()
    planner = du._planner_integrity_inventory(
        repo_root,
        head,
        deployment_repo_root,
    )
    return list(planner["dirty_paths"])


@pytest.mark.postgres
@pytest.mark.skip(
    reason=(
        "quarantined pending #1533 (fails on dev nightly substrate-db-full; "
        "not owned by any per-PR lane before PR #1524)"
    ),
)
def test_risk_admission_uses_real_target_and_postgres_without_dump(
    tmp_path: Path,
    okg_workspace_isolated_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """Exercise the immutable planner and both backup passes without mocks."""
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.cli.main import main

    dsn = okg_workspace_isolated_runtime
    monkeypatch.setattr(
        du,
        "_phase_nomos_baseline",
        _actual_phase_nomos_baseline,
    )
    monkeypatch.setattr(
        du,
        "_backup_compatibility_snapshot",
        _actual_backup_compatibility_snapshot,
    )
    monkeypatch.setattr(
        du,
        "_backup_headroom_preflight",
        _actual_backup_headroom_preflight,
    )
    monkeypatch.setattr(
        du,
        "_backup_post_dump_headroom_preflight",
        _actual_backup_post_dump_headroom_preflight,
    )
    monkeypatch.setattr(
        du,
        "_restore_headroom_preflight",
        _actual_restore_headroom_preflight,
    )
    monkeypatch.setattr(
        du,
        "_verify_backup_restore",
        _actual_verify_backup_restore,
    )
    monkeypatch.setattr(
        du,
        "_backup_tool_executable_identity",
        _actual_backup_tool_executable_identity,
    )
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    deployment_dir = Path("deployments/okg-workspace").resolve()
    assert main([
        "--json",
        "migrate",
        "--deployment",
        "okg-workspace",
        "--dsn",
        dsn,
        "--apply",
    ]) == 0
    capsys.readouterr()
    assert main([
        "--json",
        "catalog",
        "ownership",
        "claim",
        "--deployment",
        str(deployment_dir),
        "--dsn",
        dsn,
        "--force",
    ]) == 0
    capsys.readouterr()
    assert main([
        "--json",
        "catalog",
        "load",
        "--deployment",
        str(deployment_dir),
        "--dsn",
        dsn,
        "--apply",
    ]) == 0
    capsys.readouterr()

    backup_dir = tmp_path / "backups"
    verify_options = UpgradeOptions(
        deployment="okg-workspace",
        profile="dev",
        dsn=dsn,
        readiness_profile="dogfood",
        state_file=tmp_path / "verify-state.json",
        backup_dir=backup_dir,
        independent_backup_verify=True,
    )
    verify_ctx = du._build_context(verify_options, command_runner=None)
    preflight = du._phase_preflight(verify_ctx)
    assert preflight["ok"] is True, preflight
    nomos = du._phase_nomos_baseline(verify_ctx)
    assert nomos["ok"] is True, nomos
    verified = du._phase_backup(verify_ctx)
    # This pass takes a real dump, so it needs the host free space the
    # deployment's own manifest demands -- another condition of the machine
    # rather than of this test.  A storage refusal here is the preflight doing
    # its job, so report it as an unmet precondition instead of a failure.  Only
    # that one refusal is tolerated; every other way the phase can fail still
    # reds.  Host storage refusals are covered by
    # tests/substrate/runtime/test_host_headroom.py.
    headroom = verified.get("headroom") or {}
    if verified["ok"] is False and headroom.get("code") == "host_headroom_blocker":
        pytest.skip(
            "backup verification needs the host free space okg-workspace's "
            f"manifest requires: {headroom.get('available_pct')}% available on "
            f"{headroom.get('target_path')}, below the "
            f"{headroom.get('thresholds', {}).get('blocker_available_pct')}% "
            f"blocker floor (short {headroom.get('shortfall_bytes')} bytes). "
            "Reclaim host storage to exercise this test."
        )
    assert verified["ok"] is True, json.dumps(verified, sort_keys=True)
    assert verified["verification_mode"] == "current_full_restore"
    assert verified["disaster_recovery_receipt"]["schema"] == (
        "okg.deployment_disaster_recovery.v1"
    )

    # Everything above is independent of how the checkout happens to look.  The
    # risk-admission pass below is not: the planner refuses to skip the physical
    # backup unless its own source tree is committed, and this test drives that
    # planner against the real repository it is running from rather than a
    # fixture it controls.  Refusing there is correct production behavior, so
    # assert the admitted path only when the checkout is genuinely pristine --
    # otherwise every developer with an unsaved edit sees a red that is really
    # the planner working.  The refusal itself is covered by
    # ``test_dirty_planner_checkout_refuses_risk_admitted_backup``.
    dirt = _live_checkout_planner_dirt(deployment_dir)
    if dirt:
        shown = ", ".join(dirt[:5])
        if len(dirt) > 5:
            shown += f", ... (+{len(dirt) - 5} more)"
        pytest.skip(
            "risk admission requires a pristine checkout, and this working "
            f"tree has {len(dirt)} dirty path(s) the planner refuses on: "
            f"{shown}. Commit or stash them to exercise the admitted path; "
            "the refusal is covered by "
            "test_dirty_planner_checkout_refuses_risk_admitted_backup."
        )

    commands: list[list[str]] = []

    def observing_runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        commands.append(argv)
        return du._default_command_runner(argv, env)

    routine_options = UpgradeOptions(
        deployment="okg-workspace",
        profile="dev",
        dsn=dsn,
        readiness_profile="dogfood",
        state_file=tmp_path / "routine-state.json",
        backup_dir=backup_dir,
    )
    routine_ctx = du._build_context(
        routine_options,
        command_runner=observing_runner,
    )
    routine_preflight = du._phase_preflight(routine_ctx)
    assert routine_preflight["ok"] is True, routine_preflight
    admitted = du._phase_backup(routine_ctx)

    assert admitted["ok"] is True, admitted
    assert admitted["verification_mode"] == "no_physical_rollback_required"
    assert admitted["effect_inventory"]["checks"] == {
        name: True for name in _closed_effect_checks().to_dict()
    }
    assert not any("--file" in command for command in commands)
    assert not any(
        command[:2] == ["pg_restore", "--list"] for command in commands
    )


@pytest.mark.parametrize("peak_factor", [None, True, float("nan"), float("inf"), 1.24])
def test_backup_policy_requires_explicit_conservative_restore_peak_factor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    peak_factor: object,
) -> None:
    from okg.substrate import deployment_upgrade as du

    backup = {
        "verification_policy": "current_full_restore",
        "restore_volume": {"kind": "docker_data_root"},
    }
    if peak_factor is not None:
        backup["restore_peak_factor"] = peak_factor
    monkeypatch.setattr(
        du,
        "_resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(
            deployment_dir=tmp_path,
            deployment_up={"backup": backup},
        ),
    )

    with pytest.raises(du.SubstrateError):
        du._backup_policy_config(
            SimpleNamespace(options=SimpleNamespace(deployment="dep"))
        )


def test_backup_policy_requires_explicit_restore_volume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    monkeypatch.setattr(
        du,
        "_resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(
            deployment_dir=tmp_path,
            deployment_up={
                "backup": {
                    "verification_policy": "current_full_restore",
                    "restore_peak_factor": 1.25,
                }
            },
        ),
    )

    with pytest.raises(du.SubstrateError):
        du._backup_policy_config(
            SimpleNamespace(options=SimpleNamespace(deployment="dep"))
        )


def _patch_routine_backup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    manifest: SimpleNamespace | None = None,
) -> SimpleNamespace:
    _stub_dependency_sync_checkout(monkeypatch)
    selected = manifest or _routine_manifest(tmp_path)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": "postgres://user@host/db",
            "target": {
                "deployment": "dep",
                "host": "host",
                "port": 5432,
                "database": "db",
                "identity": "ok",
            },
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: selected,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._estimate_source_db_bytes",
        lambda ctx: 68 * 1024**3,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._backup_headroom_preflight",
        lambda ctx, **kwargs: {"ok": True, "refuse": False, "status": "ok"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._collect_effect_inventory",
        lambda ctx, expected_digest=None: (
            _closed_effect_checks(
                effect_digest_stable=(
                    expected_digest in (None, "", "sha256:test-effect")
                )
            ),
            (),
            {
                "target_revision": "test-revision",
                "effect_digest": "sha256:test-effect",
                "execution_checkout_exact": True,
                "smoke": {"selected_source_digest": "sha256:test-smoke"},
                "checks": _closed_effect_checks().to_dict(),
            },
        ),
    )
    return selected


def _routine_backup_runner(argv: list[str], env: dict[str, str]) -> CommandResult:
    del env
    if backup := _backup_command(argv):
        return backup
    if "migrations" in argv:
        return _ok({"pending": [], "destructive_pending": []})
    return _ok()


def test_routine_backup_automatically_reuses_exact_restore_drill_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_routine_backup(monkeypatch, tmp_path)
    restore_calls: list[Path] = []

    def restore(ctx, path: Path, **_) -> dict:
        del ctx
        restore_calls.append(path)
        return {
            "ok": True,
            "restore_verified": True,
            "schema_ok": True,
            "ownership_ok": True,
            "observed_restored_size_bytes": 70 * 1024**3,
        }

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._verify_backup_restore",
        restore,
    )
    backup_dir = tmp_path / "backups"
    first = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "first.json",
            backup_dir=backup_dir,
            readiness_profile="dogfood",
            stop_after="backup",
        ),
        command_runner=_routine_backup_runner,
    )
    second = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "second.json",
            backup_dir=backup_dir,
            readiness_profile="dogfood",
            stop_after="backup",
        ),
        command_runner=_routine_backup_runner,
    )

    first_backup = first["phases"][-1]
    second_backup = second["phases"][-1]
    receipt = backup_dir / "dep-latest-restore-drill.json"
    assert first_backup["verification_mode"] == "current_full_restore"
    assert second_backup["verification_mode"] == "routine_restore_drill_reused"
    assert second_backup["receipt_reused"] is True
    assert len(restore_calls) == 1
    assert receipt.stat().st_mode & 0o777 == 0o600
    assert json.loads(receipt.read_text())["schema"] == (
        "okg.deployment_restore_drill.v1"
    )
    assert all(second_backup["archive_checks"].values())


@pytest.mark.parametrize(
    ("receipt_mutation", "expected_reason"),
    [
        (lambda receipt: receipt.write_text("[]"), "receipt_not_object"),
        (
            lambda receipt: _rewrite_receipt_timestamp(
                receipt, "2000-01-01T00:00:00+00:00"
            ),
            "receipt.fresh",
        ),
        (
            lambda receipt: _rewrite_receipt_observed_size(receipt, missing=True),
            "receipt.observed_restored_size_valid",
        ),
        (
            lambda receipt: _rewrite_receipt_observed_size(receipt, value=True),
            "receipt.observed_restored_size_valid",
        ),
        (
            lambda receipt: _rewrite_receipt_observed_size(receipt, value="70"),
            "receipt.observed_restored_size_valid",
        ),
        (
            lambda receipt: _rewrite_receipt_observed_size(receipt, value=0),
            "receipt.observed_restored_size_valid",
        ),
        (
            lambda receipt: _rewrite_receipt_observed_size(receipt, value=-1),
            "receipt.observed_restored_size_valid",
        ),
    ],
)
def test_malformed_or_stale_restore_drill_receipt_forces_full_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    receipt_mutation,
    expected_reason: str,
) -> None:
    _patch_routine_backup(monkeypatch, tmp_path)
    restore_calls = 0

    def restore(ctx, path: Path, **_) -> dict:
        nonlocal restore_calls
        del ctx, path
        restore_calls += 1
        return {
            "ok": True,
            "restore_verified": True,
            "schema_ok": True,
            "ownership_ok": True,
            "observed_restored_size_bytes": 70 * 1024**3,
        }

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._verify_backup_restore",
        restore,
    )
    backup_dir = tmp_path / "backups"
    options = dict(
        deployment="dep",
        backup_dir=backup_dir,
        readiness_profile="dogfood",
        stop_after="backup",
    )
    run_deployment_upgrade(
        UpgradeOptions(state_file=tmp_path / "seed.json", **options),
        command_runner=_routine_backup_runner,
    )
    receipt = backup_dir / "dep-latest-restore-drill.json"
    receipt_mutation(receipt)
    receipt.chmod(0o600)
    result = run_deployment_upgrade(
        UpgradeOptions(state_file=tmp_path / "rerun.json", **options),
        command_runner=_routine_backup_runner,
    )

    backup = result["phases"][-1]
    assert backup["verification_mode"] == "current_full_restore"
    assert expected_reason in backup["backup_policy_decision"]["reasons"]
    assert restore_calls == 2


def test_production_release_channel_never_reuses_dogfood_drill_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _patch_routine_backup(monkeypatch, tmp_path)
    restore_calls = 0

    def restore(ctx, path: Path, **_) -> dict:
        nonlocal restore_calls
        del ctx, path
        restore_calls += 1
        return {
            "ok": True,
            "restore_verified": True,
            "schema_ok": True,
            "ownership_ok": True,
            "observed_restored_size_bytes": 70 * 1024**3,
        }

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._verify_backup_restore",
        restore,
    )
    backup_dir = tmp_path / "backups"
    common = dict(
        deployment="dep",
        backup_dir=backup_dir,
        readiness_profile="dogfood",
        stop_after="backup",
    )
    run_deployment_upgrade(
        UpgradeOptions(state_file=tmp_path / "seed.json", **common),
        command_runner=_routine_backup_runner,
    )
    manifest.release = {"channel": "prod"}
    result = run_deployment_upgrade(
        UpgradeOptions(state_file=tmp_path / "prod.json", **common),
        command_runner=_routine_backup_runner,
    )

    backup = result["phases"][-1]
    assert backup["verification_mode"] == "current_full_restore"
    assert "release_channel_dev" in backup["backup_policy_decision"]["reasons"]
    assert restore_calls == 2


def test_candidate_plan_drift_revalidates_and_restores_before_migration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    _patch_routine_backup(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    restore_calls = 0

    def restore(ctx, path: Path, **_) -> dict:
        nonlocal restore_calls
        del ctx, path
        restore_calls += 1
        return {
            "ok": True,
            "restore_verified": True,
            "schema_ok": True,
            "ownership_ok": True,
            "observed_restored_size_bytes": 70 * 1024**3,
        }

    def snapshot(ctx, toolchain) -> dict:
        del toolchain
        return {
            "target": {
                "deployment": "dep",
                "host": "host",
                "port": 5432,
                "database": "db",
            },
            "database_owner": {
                "deployment": "dep",
                "uuid": "owner",
                "schema_fingerprint": "schema",
            },
            "cluster_system_identifier": "cluster",
            "server_version": "170010",
            "extension_versions": [],
            "schema_migrations_digest": "sha256:schema",
            "schema_catalog_digest": "sha256:schema-catalog",
            "candidate_plan_digest": du._migration_plan_digest(
                ctx.data["migrations_before"]
            ),
            "archive_format": "postgresql-custom",
            "timescaledb": {
                "extension_version": None,
                "hypertables": [],
                "continuous_aggregates": [],
                "hypertable_chunk_counts": [],
            },
            "dump_tool_major": 17,
            "restore_tool_major": 17,
        }

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._verify_backup_restore",
        restore,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._backup_compatibility_snapshot",
        snapshot,
    )
    restore_headroom_inputs: list[dict[str, object]] = []

    def restore_headroom(ctx, policy, **kwargs) -> dict:
        del ctx, policy
        restore_headroom_inputs.append(kwargs)
        return {"ok": True, "refuse": False, "status": "ok"}

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._restore_headroom_preflight",
        restore_headroom,
    )
    migration_reads = 0

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        nonlocal migration_reads
        del env
        if backup := _backup_command(argv):
            return backup
        if "migrations" in argv:
            migration_reads += 1
            plan = "a" if migration_reads <= 2 else "b"
            return _ok({
                "pending": [{"migration_id": plan}],
                "destructive_pending": [],
            })
        return _ok()

    backup_dir = tmp_path / "backups"
    run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "seed.json",
            backup_dir=backup_dir,
            readiness_profile="dogfood",
            stop_after="backup",
        ),
        command_runner=runner,
    )
    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "drift.json",
            backup_dir=backup_dir,
            readiness_profile="dogfood",
            stop_after="migrate",
        ),
        command_runner=runner,
    )

    backup = next(row for row in result["phases"] if row["phase"] == "backup")
    migrate = result["phases"][-1]
    assert backup["verification_mode"] == "routine_restore_drill_reused"
    assert migrate["phase"] == "migrate"
    assert migrate["backup_revalidation"]["candidate_plan_stable"] is False
    assert migrate["backup_revalidation"]["verification_mode"] == (
        "current_full_restore"
    )
    assert restore_calls == 2
    assert restore_headroom_inputs[-1]["prior_restored_size"] == 70 * 1024**3


def test_skip_backup_refuses_a_mutating_upgrade(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_routine_backup(monkeypatch, tmp_path)
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        commands.append(argv)
        if "migrations" in argv:
            return _ok({"pending": [], "destructive_pending": []})
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "state.json",
            skip_backup=True,
        ),
        command_runner=runner,
    )

    assert result["ok"] is False
    assert result["failed_phase"] == "backup"
    assert result["reason"] == "--skip-backup cannot authorize database mutation"
    assert not any("migrate" in argv for argv in commands)


def test_archive_digest_is_revalidated_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_routine_backup(monkeypatch, tmp_path)
    backup_dir = tmp_path / "backups"
    run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "seed.json",
            backup_dir=backup_dir,
            readiness_profile="dogfood",
            stop_after="backup",
        ),
        command_runner=_routine_backup_runner,
    )

    def tamper_after_backup(ctx) -> dict:
        Path(ctx.data["backup_result"]["backup_path"]).write_bytes(b"tampered")
        return _applied_sync_code(ctx)

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        tamper_after_backup,
    )
    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "mutate.json",
            backup_dir=backup_dir,
            readiness_profile="dogfood",
            stop_after="migrate",
        ),
        command_runner=_routine_backup_runner,
    )

    assert result["ok"] is False
    assert result["failed_phase"] == "migrate"
    assert result["reason"] == "current backup archive changed before mutation"


def test_skip_migrate_revalidates_backup_before_catalog_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_routine_backup(monkeypatch, tmp_path)

    def tamper_after_backup(ctx) -> dict:
        Path(ctx.data["backup_result"]["backup_path"]).write_bytes(b"tampered")
        return _applied_sync_code(ctx)

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        tamper_after_backup,
    )
    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "state.json",
            backup_dir=tmp_path / "backups",
            readiness_profile="dogfood",
            skip_migrate=True,
            stop_after="catalog_apply",
        ),
        command_runner=_routine_backup_runner,
    )

    assert result["ok"] is False
    assert result["failed_phase"] == "catalog_apply"
    assert result["reason"] == "current backup archive changed before mutation"


def test_policy_tightening_after_sync_forces_current_full_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    manifest = _patch_routine_backup(monkeypatch, tmp_path)
    restore_calls = 0

    def restore(ctx, path: Path, **_) -> dict:
        nonlocal restore_calls
        del ctx, path
        restore_calls += 1
        return {
            "ok": True,
            "restore_verified": True,
            "schema_ok": True,
            "ownership_ok": True,
            "observed_restored_size_bytes": 70 * 1024**3,
        }

    def snapshot(ctx, toolchain) -> dict:
        del toolchain
        value = {
            "target": {
                "deployment": "dep",
                "host": "host",
                "port": 5432,
                "database": "db",
            },
            "database_owner": {
                "deployment": "dep",
                "uuid": "owner",
                "schema_fingerprint": "schema",
            },
            "cluster_system_identifier": "cluster",
            "server_version": "170010",
            "extension_versions": [],
            "schema_migrations_digest": "sha256:schema",
            "schema_catalog_digest": "sha256:schema-catalog",
            "candidate_plan_digest": du._migration_plan_digest(
                ctx.data["migrations_before"]
            ),
            "archive_format": "postgresql-custom",
            "timescaledb": {
                "extension_version": None,
                "hypertables": [],
                "continuous_aggregates": [],
                "hypertable_chunk_counts": [],
            },
            "dump_tool_major": 17,
            "restore_tool_major": 17,
        }
        return value

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._verify_backup_restore",
        restore,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._backup_compatibility_snapshot",
        snapshot,
    )
    backup_dir = tmp_path / "backups"
    run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "seed.json",
            backup_dir=backup_dir,
            readiness_profile="dogfood",
            stop_after="backup",
        ),
        command_runner=_routine_backup_runner,
    )

    def tighten_policy(ctx) -> dict:
        manifest.deployment_up["backup"]["verification_policy"] = (
            "current_full_restore"
        )
        return _applied_sync_code(ctx)

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        tighten_policy,
    )
    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "tightened.json",
            backup_dir=backup_dir,
            readiness_profile="dogfood",
            stop_after="migrate",
        ),
        command_runner=_routine_backup_runner,
    )

    backup = next(row for row in result["phases"] if row["phase"] == "backup")
    revalidation = result["phases"][-1]["backup_revalidation"]
    assert backup["verification_mode"] == "routine_restore_drill_reused"
    assert revalidation["verification_mode"] == "current_full_restore"
    assert revalidation["policy_stable"] is False
    assert restore_calls == 2


def test_full_restore_plan_drift_restores_again_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    manifest = _routine_manifest(tmp_path)
    manifest.release = {"channel": "prod"}
    _patch_routine_backup(monkeypatch, tmp_path, manifest=manifest)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    restore_calls = 0
    migration_reads = 0

    def restore(ctx, path: Path, **_) -> dict:
        nonlocal restore_calls
        del ctx, path
        restore_calls += 1
        return {
            "ok": True,
            "restore_verified": True,
            "schema_ok": True,
            "ownership_ok": True,
            "observed_restored_size_bytes": 70 * 1024**3,
        }

    def snapshot(ctx, toolchain) -> dict:
        del toolchain
        return {
            "target": {
                "deployment": "dep",
                "host": "host",
                "port": 5432,
                "database": "db",
            },
            "database_owner": {
                "deployment": "dep",
                "uuid": "owner",
                "schema_fingerprint": "schema",
            },
            "cluster_system_identifier": "cluster",
            "server_version": "170010",
            "extension_versions": [],
            "schema_migrations_digest": "sha256:schema",
            "schema_catalog_digest": "sha256:schema-catalog",
            "candidate_plan_digest": du._migration_plan_digest(
                ctx.data["migrations_before"]
            ),
            "archive_format": "postgresql-custom",
            "timescaledb": {
                "extension_version": None,
                "hypertables": [],
                "continuous_aggregates": [],
                "hypertable_chunk_counts": [],
            },
            "dump_tool_major": 17,
            "restore_tool_major": 17,
        }

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        nonlocal migration_reads
        del env
        if backup := _backup_command(argv):
            return backup
        if "migrations" in argv:
            migration_reads += 1
            return _ok({
                "pending": [{"migration_id": f"plan-{migration_reads}"}],
                "destructive_pending": [],
            })
        return _ok()

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._verify_backup_restore",
        restore,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._backup_compatibility_snapshot",
        snapshot,
    )
    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "state.json",
            backup_dir=tmp_path / "backups",
            readiness_profile="dogfood",
            stop_after="migrate",
        ),
        command_runner=runner,
    )

    backup = next(row for row in result["phases"] if row["phase"] == "backup")
    revalidation = result["phases"][-1]["backup_revalidation"]
    assert backup["verification_mode"] == "current_full_restore"
    assert revalidation["candidate_plan_stable"] is False
    assert revalidation["verification_mode"] == "current_full_restore"
    assert restore_calls == 2


def _rewrite_receipt_timestamp(path: Path, timestamp: str) -> None:
    payload = json.loads(path.read_text())
    payload["verified_at"] = timestamp
    path.write_text(json.dumps(payload))


def _rewrite_receipt_observed_size(
    path: Path,
    *,
    value: object = None,
    missing: bool = False,
) -> None:
    payload = json.loads(path.read_text())
    if missing:
        payload.pop("observed_restored_size_bytes", None)
    else:
        payload["observed_restored_size_bytes"] = value
    path.write_text(json.dumps(payload))


def test_upgrade_runs_ordered_phases_and_persists_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_dependency_sync_checkout(monkeypatch)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": "postgres://user@host/db",
            "target": {"database": "db", "identity": "ok"},
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_generation_recovery",
        lambda ctx: {"ok": True, "classification": "not_required"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_source_convergence",
        lambda ctx: {"ok": True, "state": "ready"},
    )
    scratch_deployment = tmp_path / "scratch-dep"
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: _full_restore_manifest(scratch_deployment),
    )
    calls: list[list[str]] = []
    smoke_env: dict[str, str] = {}

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        calls.append(argv)
        assert env["OKG_DSN"] == "postgres://user@host/db"
        if backup := _backup_command(argv):
            return backup
        if argv[-2:] == ["--deployment", "dep"] and "migrations" in argv:
            return _ok({"pending": [], "destructive_pending": []})
        if "inspect-effective-attribute-recovery" in argv:
            return _ok(_no_published_recovery())
        if "ingest" in argv:
            smoke_env.update(env)
            return _ok({"publishes": [_published("gen-smoke")]})
        if "ready" in argv:
            return _ok({
                "generation": {"generation_id": "gen-smoke"},
                "doctor": {"ok": True},
            })
        if argv[-4:-2] == ["release", "dep"]:
            return _ok({"ok": True, "channel": "dev"})
        return _ok()

    state_file = tmp_path / "up-state.json"
    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=state_file,
            backup_dir=tmp_path / "backups",
            skip_backup=False,
        ),
        command_runner=runner,
    )

    assert result["ok"] is True
    assert [row["phase"] for row in result["phases"]] == [
        "preflight",
        "backup",
        "stop_services",
        "sync_code",
        "dependency_sync",
        "migrate",
        "catalog_apply",
        "nomos_baseline",
        "generation_recovery",
        "attribute_read_model",
        "restart_services",
        "source_convergence",
        "smoke_publish",
        "readiness",
    ]
    assert "OKG_FORCE_COUNT_STAMP_RECONCILE" not in smoke_env
    assert "OKG_COUNT_STAMP_RECOVERY_AUTHORIZATION" not in smoke_env
    assert result["smoke_generation"] == "gen-smoke"
    assert result["readiness_generation"] == "gen-smoke"
    assert result["integrated_gate"]["ok"] is True
    assert result["integrated_gate"]["backup_performance"]["phase_ok"] is True
    assert result["integrated_gate"]["source_readiness"] == {
        "status": "passed",
        "smoke_ok": True,
        "readiness_ok": True,
        "smoke_generation_id": "gen-smoke",
        "readiness_generation_id": "gen-smoke",
        "generation_match": True,
    }
    assert result["total_duration_seconds"] >= 0
    assert all(
        row["started_at"]
        and row["completed_at"]
        and row["duration_seconds"] >= 0
        for row in result["phases"]
    )
    state = json.loads(state_file.read_text())
    assert state["completed"] is True
    assert state["smoke_generation_id"] == "gen-smoke"
    assert state["readiness_generation_id"] == "gen-smoke"
    assert state["completed_phases"][-1] == "readiness"
    assert any(call[0] == "pg_dump" for call in calls)
    catalog_calls = [call for call in calls if "catalog" in call]
    assert len(catalog_calls) == 1
    assert catalog_calls[0][-3:] == ["--deployment", str(scratch_deployment), "--apply"]


def test_late_phase_failure_restarts_stopped_services_and_persists_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    options = UpgradeOptions(
        deployment="dep",
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path / "backups",
    )
    ctx = du.UpgradeContext(
        options=options,
        dsn="postgres://user@host/db",
        resolution={"target": {"database": "db"}},
        env={},
        state_file=options.state_file,
        backup_dir=options.backup_dir,
        command_runner=lambda argv, env: _ok(),
    )
    monkeypatch.setattr(du, "_build_context", lambda *args, **kwargs: ctx)
    monkeypatch.setattr(
        du,
        "_load_or_initialize_state",
        lambda _ctx: {
            "completed_phases": [],
            "phase_results": [],
            "completed": False,
        },
    )
    calls: list[str] = []

    def phase(_ctx, name: str):
        calls.append(name)
        if name == "backup":
            _ctx.data["backup_result"] = {
                "budgets_seconds": {"backup": 60.0, "total": 600.0}
            }
        if name == "stop_services":
            _ctx.data["services_stopped"] = True
        if name == "migrate":
            return {"phase": name, "ok": False, "reason": "migration failed"}
        if name == "restart_services":
            _ctx.data["services_stopped"] = False
        return {"phase": name, "ok": True}

    monkeypatch.setattr(du, "_run_phase", phase)

    result = du.run_deployment_upgrade(options)

    assert result["failed_phase"] == "migrate"
    assert calls[-1] == "restart_services"
    failed = result["phases"][-1]
    cleanup = failed["failure_cleanup"]
    assert cleanup == {
        "phase": "failure_cleanup",
        "ok": True,
        "action": "restart_services",
        "failed_phase": "migrate",
    }
    state = json.loads(options.state_file.read_text())
    assert state["phase_results"][-1]["failure_cleanup"] == cleanup
    assert "failure_cleanup" not in state["completed_phases"]


def test_backup_and_total_budgets_fail_the_selected_runtime_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    ctx = SimpleNamespace(
        options=SimpleNamespace(plan_only=False),
        data={},
    )
    monkeypatch.setattr(
        du,
        "_phase_backup",
        lambda _ctx: {
            "ok": True,
            "budgets_seconds": {"backup": 60.0, "total": 600.0},
        },
    )
    ticks = iter((100.0, 161.0))
    monkeypatch.setattr(du.time, "monotonic", lambda: next(ticks))

    backup = du._run_phase(ctx, "backup")

    assert backup["ok"] is False
    assert backup["budget_exceeded"] is True
    assert backup["budget_seconds"] == 60.0

    total = {"ok": True}
    ctx.data["backup_result"] = {
        "budgets_seconds": {"backup": 60.0, "total": 600.0}
    }
    du._enforce_total_runtime_budget(
        ctx,
        total,
        total_duration_seconds=601.0,
    )
    assert total["ok"] is False
    assert total["total_budget_seconds"] == 600.0


def _budget_failed_verified_backup(
    *,
    backup_path: Path,
    policy: dict[str, object],
    toolchain: dict[str, object],
    snapshot: dict[str, object],
) -> dict[str, object]:
    return {
        "phase": "backup",
        "ok": False,
        "reason": "backup phase budget exceeded",
        "budget_exceeded": True,
        "budget_exceeded_names": ["backup", "dump"],
        "duration_seconds": 8545.0,
        "dump_duration_seconds": 1389.0,
        "budgets_seconds": {
            "dump": 1012.0,
            "backup": 4200.0,
            "total": 7200.0,
        },
        "backup_path": str(backup_path),
        "backup_digest": "sha256:verified",
        "backup_size_bytes": 4817,
        "backup_policy": {
            **policy,
            "budgets_seconds": {
                **(
                    copy.deepcopy(policy["budgets_seconds"])
                    if isinstance(policy.get("budgets_seconds"), dict)
                    else {}
                ),
                "forced": {
                    "dump": 1012.0,
                    "backup": 4200.0,
                    "total": 7200.0,
                }
            },
        },
        "toolchain": toolchain,
        "compatibility_snapshot": snapshot,
        "candidate_plan_digest": snapshot["candidate_plan_digest"],
        "source_database_size_bytes": 75902,
        "verification_mode": "current_full_restore",
        "restore_current_archive": True,
        "receipt_reused": False,
        "restore_verified": True,
        "schema_ok": True,
        "ownership_ok": True,
        "identity_checks": {
            "database_owner_equal": True,
            "extension_versions_equal": True,
            "schema_catalog_equal": True,
            "schema_migrations_equal": True,
        },
        "archive_checks": {
            "current_owner": True,
            "digest_ok": True,
            "manifest_ok": True,
            "private_mode": True,
            "size_ok": True,
            "symlink_safe": True,
            "toc_readable": True,
        },
        "cleanup": {
            "disposable_database_dropped": True,
            "restore_toc_removed": True,
        },
        "progress": {
            "pg_dump": {"completed_at": "2026-07-27T11:02:38+00:00"},
            "pg_restore": {"completed_at": "2026-07-27T13:01:42+00:00"},
        },
    }


@pytest.mark.parametrize(
    ("failure_reason", "exceeded_names"),
    [
        ("backup phase budget exceeded", ["backup", "dump"]),
        ("dump phase budget exceeded", ["dump"]),
        ("deployment total runtime budget exceeded", ["total"]),
    ],
)
def test_backup_budget_resume_revalidates_verified_restore_without_repeating_io(
    tmp_path: Path,
    record_property,
    failure_reason: str,
    exceeded_names: list[str],
) -> None:
    from okg.substrate import deployment_upgrade as du

    record_property(
        "requirements",
        "req.runtime-authority.backup-admission "
        "test.runtime-authority.backup-admission "
        "test.runtime-authority.backup-budget-resume",
    )
    backup_dir = tmp_path / "backups"
    backup_path = backup_dir / "dep-verified.dumpdir"
    backup_dir.mkdir(parents=True)
    archive_bytes = b"verified archive"
    backup_path.write_bytes(archive_bytes)
    policy: dict[str, object] = {
        "verification_policy": "risk_admitted",
        "dump_strategy": {
            "format": "directory",
            "compression": "gzip",
            "compression_level": 1,
            "jobs": 4,
        },
        "budgets_seconds": {
            "forced": {
                "dump": 1800.0,
                "backup": 10800.0,
                "total": 21600.0,
            }
        },
    }
    toolchain = {"ok": True, "tools": {"pg_restore": {"major": 18}}}
    snapshot = {
        "candidate_plan_digest": "sha256:plan",
        "schema_catalog_digest": "sha256:schema",
    }
    prior = _budget_failed_verified_backup(
        backup_path=backup_path,
        policy=policy,
        toolchain=toolchain,
        snapshot=snapshot,
    )
    prior["reason"] = failure_reason
    prior["budget_exceeded_names"] = exceeded_names
    if failure_reason == "deployment total runtime budget exceeded":
        # _enforce_total_runtime_budget stamps these on the phase row.
        prior["total_budget_seconds"] = 7200.0
        prior["total_duration_seconds"] = 12147.0
    prior["backup_digest"] = "sha256:" + hashlib.sha256(
        archive_bytes
    ).hexdigest()
    prior["backup_size_bytes"] = len(archive_bytes)
    prior["archive_metadata_fingerprint"] = du._canonical_fingerprint(
        du._backup_archive_metadata_fingerprint(backup_path)
    )
    commands: list[list[str]] = []

    def runner(argv: list[str], _env: dict[str, str]) -> CommandResult:
        commands.append(argv)
        if argv[:2] == ["pg_restore", "--list"]:
            return CommandResult(0, "; archive listing\n", "")
        return CommandResult(1, "", "unexpected command")

    ctx = SimpleNamespace(
        backup_dir=backup_dir,
        data={
            "prior_backup_attempt": prior,
            "prior_total_duration_seconds": 12147.0,
        },
        command_runner=runner,
        env={},
    )

    resumed = du._resumable_budget_verified_backup(
        ctx,
        policy=policy,
        toolchain=toolchain,
        snapshot=snapshot,
        source_database_size=75902,
        effect_inventory={"target_revision": "current"},
    )

    assert resumed is not None
    assert resumed["ok"] is True
    assert resumed["resumed_budget_verified_backup"] is True
    assert resumed["physical_backup_started"] is False
    assert resumed["restore_started"] is False
    assert resumed["backup_tool_child_processes_started"] == 0
    assert resumed["budgets_seconds"] == {
        "dump": 1800.0,
        "backup": 10800.0,
        "total": 21600.0,
    }
    assert "duration_seconds" not in resumed
    assert "total_budget_seconds" not in resumed
    assert "total_duration_seconds" not in resumed
    assert resumed["archive_content_rehash_skipped"] is True
    assert resumed["backup_policy"] == policy
    assert resumed["budgets_seconds"] == policy["budgets_seconds"]["forced"]
    assert resumed.get("budget_exceeded") is None
    assert resumed.get("reason") is None
    assert commands == [["pg_restore", "--list", str(backup_path)]]
    assert ctx.data["backup_result"] is resumed

    # ctime may use a coarse per-tick clock on fast Linux filesystems. A
    # same-size rewrite in the same tick is intentionally invisible to the
    # cheap metadata drift key, so wait until the kernel exposes the mutation
    # before exercising the full-rehash refusal path.
    cached_fingerprint = du._canonical_fingerprint(
        prior["archive_metadata_fingerprint"]
    )
    deadline = time.monotonic() + 5.0
    while True:
        backup_path.write_bytes(b"tampered archive")
        observed_fingerprint = du._canonical_fingerprint(
            du._backup_archive_metadata_fingerprint(backup_path)
        )
        if observed_fingerprint != cached_fingerprint:
            break
        assert time.monotonic() < deadline, (
            "archive ctime never advanced past the cached fingerprint"
        )
        time.sleep(0.005)
    drift_ctx = SimpleNamespace(
        backup_dir=backup_dir,
        data={
            "prior_backup_attempt": prior,
            "prior_total_duration_seconds": 12147.0,
        },
        command_runner=runner,
        env={},
    )
    refused = du._resumable_budget_verified_backup(
        drift_ctx,
        policy=policy,
        toolchain=toolchain,
        snapshot=snapshot,
        source_database_size=75902,
        effect_inventory={"target_revision": "current"},
    )
    assert refused["ok"] is False
    assert refused["reason"] == (
        "verified backup budget-resume archive no longer matches its proof"
    )
    assert refused["mutation_authorized"] is False


def test_phase_backup_returns_budget_resume_before_starting_dump(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record_property,
) -> None:
    """Pin the production wiring: _phase_backup itself must reuse the proof.

    Every other budget-resume test drives _resumable_budget_verified_backup
    directly or stubs _phase_backup; severing the call inside _phase_backup
    would silently restart the full dump while those tests stay green.
    """
    from okg.substrate import deployment_upgrade as du

    record_property(
        "requirements",
        "req.runtime-authority.backup-admission "
        "test.runtime-authority.backup-budget-resume",
    )
    manifest = _routine_manifest(tmp_path)
    # restore_drill posture forces the physical path while still parsing
    # budgets_seconds; the flat shape applies to both routine and forced.
    manifest.deployment_up["backup"]["budgets_seconds"] = {
        "dump": 1800.0,
        "backup": 10800.0,
        "total": 21600.0,
    }
    _patch_routine_backup(monkeypatch, tmp_path, manifest=manifest)
    monkeypatch.setattr(
        du,
        "_disaster_recovery_receipt_evidence",
        lambda ctx, snapshot, policy: (
            _fresh_recovery_checks(),
            (),
            {"status": "compatible"},
            70 * 1024**3,
        ),
    )
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        commands.append(argv)
        return _routine_backup_runner(argv, env)

    backup_dir = tmp_path / "backups"
    backup_dir.mkdir(parents=True)
    options = UpgradeOptions(
        deployment="dep",
        state_file=tmp_path / "state.json",
        backup_dir=backup_dir,
    )
    ctx = du._build_context(options, command_runner=runner)
    policy = du._backup_policy_config(ctx)
    toolchain = du._backup_tool_preflight(ctx)
    assert toolchain["ok"] is True, toolchain
    snapshot = du._backup_compatibility_snapshot(ctx, toolchain)
    archive_bytes = b"verified archive"
    backup_path = backup_dir / "dep-verified.dumpdir"
    backup_path.write_bytes(archive_bytes)
    prior = _budget_failed_verified_backup(
        backup_path=backup_path,
        policy=policy,
        toolchain=toolchain,
        snapshot=snapshot,
    )
    prior["source_database_size_bytes"] = 68 * 1024**3
    prior["backup_digest"] = "sha256:" + hashlib.sha256(
        archive_bytes
    ).hexdigest()
    prior["backup_size_bytes"] = len(archive_bytes)
    prior["archive_metadata_fingerprint"] = du._canonical_fingerprint(
        du._backup_archive_metadata_fingerprint(backup_path)
    )
    ctx.data["prior_backup_attempt"] = prior
    ctx.data["prior_total_duration_seconds"] = 12147.0

    result = du._phase_backup(ctx)

    assert result["ok"] is True, result
    assert result["resumed_budget_verified_backup"] is True
    assert result["physical_backup_started"] is False
    assert result["restore_started"] is False
    assert result["backup_tool_child_processes_started"] == 0
    assert ctx.data["backup_result"] is result
    # Version probes are allowed; an actual dump would carry --file.
    assert not any("--file" in command for command in commands)
    assert all(
        command[1] == "--version"
        for command in commands
        if command and Path(command[0]).name == "pg_dump"
    )
    assert any(
        command[:2] == ["pg_restore", "--list"] for command in commands
    )


@pytest.mark.parametrize(
    ("drift_kind", "expected_reason"),
    [
        (
            "source_size",
            "verified backup budget-resume proof no longer matches current authority",
        ),
        (
            "budget",
            "verified backup remains outside current forced budgets",
        ),
        (
            "total_budget",
            "verified backup remains outside current forced budgets",
        ),
        (
            "failure_names",
            "verified backup budget-failure evidence is incomplete",
        ),
        (
            "policy",
            "verified backup budget-resume proof no longer matches current authority",
        ),
        (
            "toolchain",
            "verified backup budget-resume proof no longer matches current authority",
        ),
        (
            "compatibility",
            "verified backup budget-resume proof no longer matches current authority",
        ),
        (
            "identity",
            "verified backup budget-resume proof no longer matches current authority",
        ),
    ],
)
def test_backup_budget_resume_refuses_drift_or_still_insufficient_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record_property,
    drift_kind: str,
    expected_reason: str,
) -> None:
    from okg.substrate import deployment_upgrade as du

    record_property(
        "requirements",
        "req.runtime-authority.backup-admission "
        "test.runtime-authority.backup-budget-resume",
    )
    backup_dir = tmp_path / "backups"
    backup_path = backup_dir / "dep-verified.dumpdir"
    backup_path.mkdir(parents=True)
    policy: dict[str, object] = {
        "verification_policy": "risk_admitted",
        "dump_strategy": {"format": "directory", "jobs": 4},
        "budgets_seconds": {
            "routine": {
                "backup": 60.0,
                "attribute_read_model": 30.0,
                "total": 600.0,
            },
            "forced": {
                "dump": 1800.0,
                "backup": 10800.0,
                "total": 21600.0,
            }
        },
    }
    toolchain = {"ok": True, "tools": {"pg_restore": {"major": 18}}}
    snapshot = {"candidate_plan_digest": "sha256:plan"}
    prior = _budget_failed_verified_backup(
        backup_path=backup_path,
        policy=policy,
        toolchain=toolchain,
        snapshot=snapshot,
    )
    source_database_size = 75902
    current_toolchain = toolchain
    current_snapshot = snapshot
    if drift_kind == "source_size":
        source_database_size += 1
    elif drift_kind == "budget":
        policy["budgets_seconds"]["forced"]["backup"] = 8000.0
    elif drift_kind == "total_budget":
        policy["budgets_seconds"]["forced"]["total"] = 15000.0
    elif drift_kind == "failure_names":
        prior["budget_exceeded_names"] = []
    elif drift_kind == "policy":
        prior["backup_policy"]["budgets_seconds"]["routine"]["backup"] = 61.0
    elif drift_kind == "toolchain":
        current_toolchain = {
            "ok": True,
            "tools": {"pg_restore": {"major": 19}},
        }
    elif drift_kind == "compatibility":
        current_snapshot = {"candidate_plan_digest": "sha256:changed"}
    elif drift_kind == "identity":
        prior["identity_checks"] = {"database_owner_equal": True}
    monkeypatch.setattr(
        du,
        "_validate_backup_archive_for_run",
        lambda *_args, **_kwargs: pytest.fail(
            "archive validation must not start before authority and budget checks pass"
        ),
    )
    ctx = SimpleNamespace(
        backup_dir=backup_dir,
        data={
            "prior_backup_attempt": prior,
            "prior_total_duration_seconds": 12147.0,
        },
    )

    refused = du._resumable_budget_verified_backup(
        ctx,
        policy=policy,
        toolchain=current_toolchain,
        snapshot=current_snapshot,
        source_database_size=source_database_size,
        effect_inventory=None,
    )

    assert refused is not None
    assert refused["ok"] is False
    assert refused["reason"] == expected_reason
    assert refused["mutation_authorized"] is False
    assert refused["physical_backup_started"] is False
    assert refused["restore_started"] is False
    assert refused["backup_tool_child_processes_started"] == 0
    if drift_kind == "total_budget":
        total_check = refused["cumulative_total_budget_check"]
        assert total_check["observed_seconds"] == 12147.0
        assert total_check["required_seconds"] == 15183.75
        assert total_check["budget_seconds"] == 15000.0
        assert total_check["ok"] is False
    assert refused["budget_resume_refused"] is True
    retained = du._prior_backup_attempt_for_resume([
        prior,
        {"phase": "backup", **refused},
    ])
    assert retained is prior
    with pytest.raises(
        SubstrateError,
        match="lost its retained verified proof",
    ):
        du._prior_backup_attempt_for_resume([
            {"phase": "backup", **refused},
        ])


def test_budget_only_target_transition_resumes_after_no_io_total_refusal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record_property,
) -> None:
    from okg.substrate import deployment_upgrade as du

    record_property(
        "requirements",
        "req.runtime-authority.backup-admission "
        "test.runtime-authority.backup-budget-resume",
    )
    state_file = tmp_path / "state.json"
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    backup_path = backup_dir / "dep-verified.dump"
    archive_bytes = b"one production-sized backup"
    backup_path.write_bytes(archive_bytes)
    routine_budgets = {
        "backup": 60.0,
        "attribute_read_model": 30.0,
        "total": 600.0,
    }
    old_policy: dict[str, object] = {
        "verification_policy": "risk_admitted",
        "dump_strategy": {"format": "directory", "jobs": 4},
        "budgets_seconds": {
            "routine": routine_budgets,
            "forced": {
                "dump": 1012.0,
                "backup": 4200.0,
                "total": 7200.0,
            },
        },
    }
    new_policy = copy.deepcopy(old_policy)
    new_policy["budgets_seconds"]["forced"] = {
        "dump": 1800.0,
        "backup": 10800.0,
        "total": 21600.0,
    }
    intermediate_policy = copy.deepcopy(new_policy)
    intermediate_policy["budgets_seconds"]["forced"]["total"] = 8000.0
    toolchain = {"ok": True, "tools": {"pg_restore": {"major": 18}}}
    snapshot = {
        "candidate_plan_digest": "sha256:plan",
        "schema_catalog_digest": "sha256:schema",
    }
    verified = _budget_failed_verified_backup(
        backup_path=backup_path,
        policy=old_policy,
        toolchain=toolchain,
        snapshot=snapshot,
    )
    for key in (
        "phase",
        "ok",
        "reason",
        "duration_seconds",
        "budget_exceeded",
        "budget_exceeded_names",
    ):
        verified.pop(key, None)
    verified["backup_digest"] = "sha256:" + hashlib.sha256(
        archive_bytes
    ).hexdigest()
    verified["backup_size_bytes"] = len(archive_bytes)
    verified["archive_metadata_fingerprint"] = du._canonical_fingerprint(
        du._backup_archive_metadata_fingerprint(backup_path)
    )
    authority = {
        "revision": "revision-old",
        "manifest": "sha256:" + "1" * 64,
        "policy": old_policy,
    }
    clock = SimpleNamespace(now=0.0)
    calls = {"pg_dump": 0, "full_restore": 0, "toc": 0}

    def target_key(_ctx) -> dict[str, object]:
        return {
            "database": "dep",
            "identity": "ok",
            "code_revision": authority["revision"],
            "manifest_digest": authority["manifest"],
            "lifecycle_contract_digest": du.CANONICAL_CONTRACT.digest,
            "lifecycle_plan_digest": None,
            "lifecycle_policy": {
                "apply_services": False,
                "sync_code": True,
                "sync_deps": True,
                "skip_backup": False,
                "skip_migrate": False,
                "skip_destructive_migrations": False,
                "confirm_destructive_migrations": False,
                "plan_only": False,
                "independent_backup_verify": False,
            },
        }

    def runner(argv: list[str], _env: dict[str, str]) -> CommandResult:
        if argv[:2] == ["pg_restore", "--list"]:
            calls["toc"] += 1
            return CommandResult(0, "; archive listing\n", "")
        return CommandResult(1, "", f"unexpected command: {argv}")

    def build_context(options, command_runner=None) -> UpgradeContext:
        del command_runner
        return UpgradeContext(
            options=options,
            dsn="postgres://user@host/dep",
            resolution={"target": {"database": "dep", "identity": "ok"}},
            env={},
            state_file=state_file,
            backup_dir=backup_dir,
            command_runner=runner,
        )

    backup_invocations = 0

    def backup_phase(ctx: UpgradeContext) -> dict[str, object]:
        nonlocal backup_invocations
        backup_invocations += 1
        if backup_invocations == 1:
            calls["pg_dump"] += 1
            calls["full_restore"] += 1
            clock.now += 8545.0
            return dict(verified)
        return du._resumable_budget_verified_backup(
            ctx,
            policy=authority["policy"],
            toolchain=toolchain,
            snapshot=snapshot,
            source_database_size=75902,
            effect_inventory={"target_revision": authority["revision"]},
        )

    monkeypatch.setattr(du, "_build_context", build_context)
    monkeypatch.setattr(du, "_target_state_key", target_key)
    monkeypatch.setattr(
        du,
        "_backup_policy_config",
        lambda _ctx: authority["policy"],
    )
    monkeypatch.setattr(
        du,
        "_phase_preflight",
        lambda _ctx: {
            "ok": True,
            "migration_status": {"pending": [], "destructive_pending": []},
        },
    )
    monkeypatch.setattr(du, "_phase_backup", backup_phase)
    monkeypatch.setattr(du.time, "monotonic", lambda: clock.now)
    options = UpgradeOptions(
        deployment="dep",
        state_file=state_file,
        backup_dir=backup_dir,
        apply_services=False,
        stop_after="backup",
    )

    first = du.run_deployment_upgrade(options)

    assert first["ok"] is False
    assert first["failed_phase"] == "backup"
    assert first["reason"] == "backup phase budget exceeded"
    assert calls == {"pg_dump": 1, "full_restore": 1, "toc": 0}

    authority.update({
        "revision": "revision-intermediate",
        "manifest": "sha256:" + "2" * 64,
        "policy": intermediate_policy,
    })
    refused = du.run_deployment_upgrade(options)
    refused_backup = next(
        row for row in reversed(refused["phases"])
        if row.get("phase") == "backup"
    )

    assert refused["failed_phase"] == "backup"
    assert refused_backup["ok"] is False
    assert refused_backup["budget_resume_refused"] is True
    assert refused_backup["physical_backup_started"] is False
    assert refused_backup["restore_started"] is False
    assert calls == {"pg_dump": 1, "full_restore": 1, "toc": 0}

    authority.update({
        "revision": "revision-final",
        "manifest": "sha256:" + "3" * 64,
        "policy": new_policy,
    })
    resumed = du.run_deployment_upgrade(options)
    resumed_backup = next(
        row for row in reversed(resumed["phases"])
        if row.get("phase") == "backup"
    )

    assert resumed_backup["ok"] is True
    assert resumed_backup["resumed_budget_verified_backup"] is True
    assert resumed_backup["physical_backup_started"] is False
    assert resumed_backup["restore_started"] is False
    assert resumed_backup["budgets_seconds"] == {
        "dump": 1800.0,
        "backup": 10800.0,
        "total": 21600.0,
    }
    assert resumed["stopped_after"] == "backup"
    assert resumed["total_duration_seconds"] == pytest.approx(8545.0)
    assert calls == {"pg_dump": 1, "full_restore": 1, "toc": 1}
    persisted = json.loads(state_file.read_text())
    assert persisted["budget_resume_target_transition"]["ok"] is True
    assert persisted["target_key"]["code_revision"] == "revision-final"


@pytest.mark.parametrize("drift_kind", ["routine_policy", "headroom", "failure_names"])
def test_budget_only_target_transition_refuses_unqualified_or_unrelated_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record_property,
    drift_kind: str,
) -> None:
    from okg.substrate import deployment_upgrade as du

    record_property(
        "requirements",
        "req.runtime-authority.backup-admission "
        "test.runtime-authority.backup-budget-resume",
    )
    old_policy: dict[str, object] = {
        "verification_policy": "risk_admitted",
        "dump_strategy": {"format": "directory", "jobs": 4},
        "budgets_seconds": {
            "routine": {"backup": 60.0, "total": 600.0},
            "forced": {
                "dump": 1012.0,
                "backup": 4200.0,
                "total": 7200.0,
            },
        },
    }
    current_policy = copy.deepcopy(old_policy)
    current_policy["budgets_seconds"]["forced"] = {
        "dump": 1800.0,
        "backup": 10800.0,
        "total": 21600.0,
    }
    prior = _budget_failed_verified_backup(
        backup_path=tmp_path / "backups" / "verified.dump",
        policy=old_policy,
        toolchain={"ok": True},
        snapshot={"candidate_plan_digest": "sha256:plan"},
    )
    if drift_kind == "routine_policy":
        current_policy["budgets_seconds"]["routine"]["backup"] = 61.0
    elif drift_kind == "headroom":
        current_policy["budgets_seconds"]["forced"]["backup"] = 10000.0
    elif drift_kind == "failure_names":
        prior["budget_exceeded_names"] = []
    stable = {
        "database": "dep",
        "identity": "ok",
        "lifecycle_contract_digest": du.CANONICAL_CONTRACT.digest,
        "lifecycle_plan_digest": None,
        "lifecycle_policy": {"apply_services": True},
    }
    state = {
        "target_key": {
            **stable,
            "code_revision": "old",
            "manifest_digest": "sha256:" + "1" * 64,
        },
        "phase_results": [prior],
    }
    target_key = {
        **stable,
        "code_revision": "new",
        "manifest_digest": "sha256:" + "2" * 64,
    }
    monkeypatch.setattr(
        du,
        "_backup_policy_config",
        lambda _ctx: current_policy,
    )

    with pytest.raises(
        SubstrateError,
        match="budget-only resume target transition refused",
    ):
        du._budget_only_resume_target_transition(
            SimpleNamespace(),
            state=state,
            target_key=target_key,
        )


_MISSING_TOTAL = object()


@pytest.mark.parametrize(
    "corrupt_total",
    [_MISSING_TOTAL, "invalid", -1.0, float("inf")],
)
def test_resumed_upgrade_refuses_missing_or_corrupt_cumulative_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record_property,
    corrupt_total: object,
) -> None:
    from okg.substrate import deployment_upgrade as du

    record_property(
        "requirements",
        "req.runtime-authority.backup-admission "
        "test.runtime-authority.backup-budget-resume",
    )
    state_file = tmp_path / "state.json"
    target_key = {"database": "dep", "identity": "ok"}
    state = {
        "deployment": "dep",
        "profile": "prod",
        "target_key": target_key,
        "plan_only": False,
        "lifecycle_contract_digest": du.CANONICAL_CONTRACT.digest,
        "lifecycle_contract_version": du.CANONICAL_CONTRACT.contract_version,
        "completed": False,
        "completed_phases": ["preflight"],
        "phase_results": [{"phase": "preflight", "ok": True}],
        "phase_inputs": {
            "smoke_publish": {"include": []},
            "readiness": {"profile": "general"},
        },
    }
    if corrupt_total is not _MISSING_TOTAL:
        state["total_duration_seconds"] = corrupt_total
    state_file.write_text(json.dumps(state))
    options = UpgradeOptions(
        deployment="dep",
        state_file=state_file,
        backup_dir=tmp_path / "backups",
    )
    ctx = UpgradeContext(
        options=options,
        dsn="postgres://user@host/dep",
        resolution={"target": target_key},
        env={},
        state_file=state_file,
        backup_dir=tmp_path / "backups",
        command_runner=lambda _argv, _env: _ok(),
    )
    monkeypatch.setattr(du, "_build_context", lambda *_args, **_kwargs: ctx)
    monkeypatch.setattr(du, "_target_state_key", lambda _ctx: target_key)

    with pytest.raises(
        SubstrateError,
        match="cumulative runtime evidence is missing or invalid",
    ):
        du.run_deployment_upgrade(options)


def test_backup_budget_resume_total_overrun_retains_no_io_refusal_authority(
    record_property,
) -> None:
    from okg.substrate import deployment_upgrade as du

    record_property(
        "requirements",
        "req.runtime-authority.backup-admission "
        "test.runtime-authority.backup-budget-resume",
    )
    ctx = SimpleNamespace(
        options=SimpleNamespace(plan_only=False),
        data={"backup_result": {"budgets_seconds": {"total": 21600.0}}},
    )
    result = {
        "ok": True,
        "resumed_budget_verified_backup": True,
        "mutation_authorized": True,
    }

    du._enforce_total_runtime_budget(
        ctx,
        result,
        total_duration_seconds=21601.0,
    )

    assert result["ok"] is False
    assert result["budget_resume_refused"] is True
    assert result["mutation_authorized"] is False
    assert result["physical_backup_started"] is False
    assert result["restore_started"] is False
    assert result["backup_tool_child_processes_started"] == 0
    assert result["budget_exceeded_names"] == ["total"]
    # The refusal row itself must never become a resume candidate: it lost
    # its measured phase durations, so admitting it would shadow the
    # retained verified proof.
    assert du._budget_resume_failure_candidate(
        {"phase": "backup", **result}
    ) is False


def test_total_budget_failed_verified_backup_is_resume_candidate(
    record_property,
) -> None:
    from okg.substrate import deployment_upgrade as du

    record_property(
        "requirements",
        "req.runtime-authority.backup-admission "
        "test.runtime-authority.backup-budget-resume",
    )
    total_failed = {
        "phase": "backup",
        "ok": False,
        "budget_exceeded": True,
        "budget_exceeded_names": ["total"],
        "reason": "deployment total runtime budget exceeded",
        "total_budget_seconds": 7200.0,
        "total_duration_seconds": 12147.0,
    }
    assert du._budget_resume_failure_candidate(total_failed) is True
    assert du._budget_resume_failure_names(total_failed) == frozenset(
        {"total"}
    )


def test_resumed_runtime_budget_includes_prior_attempt_duration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    options = UpgradeOptions(
        deployment="dep",
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path / "backups",
        apply_services=False,
    )
    ctx = du.UpgradeContext(
        options=options,
        dsn="postgres://user@host/db",
        resolution={"target": {"database": "db"}},
        env={},
        state_file=options.state_file,
        backup_dir=options.backup_dir,
        command_runner=lambda argv, env: _ok(),
    )
    completed = [phase for phase in du.PHASES if phase != "readiness"]
    monkeypatch.setattr(du, "_build_context", lambda *args, **kwargs: ctx)
    monkeypatch.setattr(
        du,
        "_load_or_initialize_state",
        lambda _ctx: {
            "completed_phases": completed,
            "phase_results": [
                {
                    "phase": "backup",
                    "ok": True,
                    "budgets_seconds": {"total": 600.0},
                },
                {
                    "phase": "smoke_publish",
                    "ok": True,
                    "result": _published("generation-1"),
                },
            ],
            "total_duration_seconds": 590.0,
            "completed": False,
        },
    )
    monkeypatch.setattr(
        du,
        "_run_phase",
        lambda _ctx, phase: {
            "phase": phase,
            "ok": True,
            "readiness": {"generation_id": "generation-1"},
        },
    )
    ticks = iter((100.0, 111.0, 111.0, 111.0))
    monkeypatch.setattr(du.time, "monotonic", lambda: next(ticks))

    result = du.run_deployment_upgrade(options)

    assert result["ok"] is False
    assert result["failed_phase"] == "readiness"
    assert result["reason"] == "deployment total runtime budget exceeded"
    assert result["total_duration_seconds"] == pytest.approx(601.0)


def test_interrupt_cleanup_restarts_services_when_state_persistence_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    options = UpgradeOptions(
        deployment="dep",
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path / "backups",
    )
    ctx = du.UpgradeContext(
        options=options,
        dsn="postgres://user@host/db",
        resolution={"target": {"database": "db"}},
        env={},
        state_file=options.state_file,
        backup_dir=options.backup_dir,
        command_runner=lambda argv, env: _ok(),
    )
    monkeypatch.setattr(du, "_build_context", lambda *args, **kwargs: ctx)
    monkeypatch.setattr(
        du,
        "_load_or_initialize_state",
        lambda _ctx: {
            "completed_phases": [],
            "phase_results": [],
            "completed": False,
        },
    )
    calls: list[str] = []

    def phase(_ctx, name: str):
        calls.append(name)
        if name == "backup":
            _ctx.data["backup_result"] = {}
        if name == "stop_services":
            _ctx.data["services_stopped"] = True
        if name == "migrate":
            raise KeyboardInterrupt
        if name == "restart_services":
            _ctx.data["services_stopped"] = False
        return {"phase": name, "ok": True}

    def write_state(path: Path, state: dict) -> None:
        del path
        if state.get("failed_phase") == "migrate":
            raise OSError("injected full disk")

    monkeypatch.setattr(du, "_run_phase", phase)
    monkeypatch.setattr(du, "_write_state", write_state)

    with pytest.raises(KeyboardInterrupt):
        du.run_deployment_upgrade(options)

    assert calls[-1] == "restart_services"
    assert ctx.data["services_stopped"] is False


def test_interrupted_readiness_witness_survives_outer_state_write_and_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.runtime.admin import DrainResult

    options = UpgradeOptions(
        deployment="dep",
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path / "backups",
    )
    snapshot = [{"schedule_id": "poll"}]
    snapshot_digest = du._runtime_schedule_snapshot_digest(snapshot)
    ctx = du.UpgradeContext(
        options=options,
        dsn="postgres://user@host/db",
        resolution={"target": {"database": "db"}},
        env={},
        state_file=options.state_file,
        backup_dir=options.backup_dir,
        command_runner=lambda argv, env: _ok(),
    )
    monkeypatch.setattr(du, "_build_context", lambda *args, **kwargs: ctx)
    monkeypatch.setattr(du, "_runtime_quiescence_required", lambda _ctx: False)
    monkeypatch.setattr(
        du,
        "_load_or_initialize_state",
        lambda _ctx: {
            "completed_phases": [],
            "phase_results": [],
            "completed": False,
            "runtime_fence_owner": "deploy-owner",
            "runtime_fence_snapshot_digest": snapshot_digest,
        },
    )

    def phase(_ctx: UpgradeContext, name: str) -> dict[str, object]:
        if name == "backup":
            _ctx.data["backup_result"] = {
                "budgets_seconds": {"backup": 60.0, "total": 600.0},
            }
        if name == "readiness":
            deduplication_id = (
                du._runtime_readiness_witness_deduplication_id(
                    _ctx,
                    owner="deploy-owner",
                )
            )
            du._persist_runtime_readiness_witness(_ctx, {
                "schema": "okg.deployment_runtime_readiness_witness.v1",
                "promotion_id": "deploy-owner",
                "deduplication_id": deduplication_id,
                "status": "intent",
            })
            raise KeyboardInterrupt
        return {"phase": name, "ok": True}

    monkeypatch.setattr(du, "_run_phase", phase)

    with pytest.raises(KeyboardInterrupt):
        du.run_deployment_upgrade(options)

    persisted = json.loads(options.state_file.read_text())
    witness = persisted["runtime_readiness_witness"]
    assert witness["status"] == "intent"

    expected_metadata = {
        "schema": "okg.deployment_runtime_quiescence.v1",
        "deployment": "dep",
        "candidate_revision": "a" * 40,
        "target_key_digest": "sha256:target",
        "lifecycle_contract_digest": "sha256:contract",
        "lifecycle_plan_digest": "sha256:plan",
        "bootstrap_quiescence": False,
    }
    monkeypatch.setattr(
        du,
        "_runtime_fence_metadata",
        lambda _ctx: expected_metadata,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.promotion_fence_status",
        lambda **kwargs: {
            "status": "fenced",
            "promotion_id": "deploy-owner",
            "schedule_snapshot": snapshot,
            "metadata": expected_metadata,
        },
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_drain",
        lambda **kwargs: DrainResult(
            deployment=kwargs["deployment"],
            drained=False,
            in_flight_at_timeout=1,
            elapsed_seconds=0.01,
            pending_workflows=[{
                "workflow_id": "wf-doctor",
                "name": "doctor_workflow",
                "deduplication_id": witness["deduplication_id"],
            }],
        ),
    )
    resumed_ctx = du.UpgradeContext(
        options=options,
        dsn=ctx.dsn,
        resolution=ctx.resolution,
        env={},
        state_file=options.state_file,
        backup_dir=options.backup_dir,
        command_runner=lambda argv, env: _ok(),
        data={
            "runtime_fence_owner": "deploy-owner",
            "runtime_fence_snapshot_digest": snapshot_digest,
            "source_recovery_workflows": [],
            "runtime_readiness_witness": witness,
        },
    )

    observed = du._revalidate_runtime_fence_resume(
        resumed_ctx,
        completed={"stop_services", "smoke_publish"},
    )

    assert observed["ok"] is True
    assert observed["checks"]["queue_drained_or_owner_bound"] is True


def test_unexpected_phase_exception_is_timed_and_typed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    monkeypatch.setattr(
        du,
        "_phase_backup",
        lambda _ctx: (_ for _ in ()).throw(RuntimeError("induced")),
    )
    result = du._run_phase(
        SimpleNamespace(options=SimpleNamespace(plan_only=False), data={}),
        "backup",
    )

    assert result["ok"] is False
    assert result["reason"] == "backup raised an unexpected error"
    assert result["error"] == {"type": "RuntimeError", "message": "induced"}
    assert result["duration_seconds"] >= 0
    assert result["started_at"] and result["completed_at"]


def test_resume_after_persisted_readiness_rehydrates_exact_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_recovery_phases(monkeypatch)
    _patch_routine_backup(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        calls.append(argv)
        if backup := _backup_command(argv):
            return backup
        if "migrations" in argv:
            return _ok({"pending": [], "destructive_pending": []})
        if "ingest" in argv:
            return _ok(_published("gen-crash-window"))
        if "release" in argv:
            return _ok({"ok": True})
        if "ready" in argv:
            return _ok({
                "ok": True,
                "generation": {
                    "generation_id": "gen-crash-window",
                    "status": "published",
                    "count_integrity": {"ok": True},
                },
                "nomos_audit": {"ok": True},
                "nomos_runtime": {"ok": True},
                "source_sync_liveness": {"ok": True},
                "source_sync_status": {
                    "convergence": {"ok": True, "state": "ready"},
                },
            })
        return _ok()

    state_file = tmp_path / "state.json"
    options = UpgradeOptions(
        deployment="dep",
        state_file=state_file,
        backup_dir=tmp_path / "backups",
        readiness_profile="dogfood",
    )
    first = run_deployment_upgrade(options, command_runner=runner)
    assert first["ok"] is True

    state = json.loads(state_file.read_text())
    state["completed"] = False
    state_file.write_text(json.dumps(state))
    calls.clear()

    resumed = run_deployment_upgrade(options, command_runner=runner)

    assert resumed["ok"] is True
    assert resumed["smoke_generation"] == "gen-crash-window"
    assert resumed["readiness_generation"] == "gen-crash-window"
    assert any("migrations" in call for call in calls)
    assert any("ready" in call for call in calls)
    assert not any("ingest" in call for call in calls)


def test_upgrade_reports_missing_pg_dump_as_backup_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": "postgres://user@host/db",
            "target": {"database": "db", "identity": "ok"},
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_generation_recovery",
        lambda ctx: {"ok": True, "recovered": False},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_source_convergence",
        lambda ctx: {"ok": True, "state": "ready"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: _full_restore_manifest(tmp_path / "dep-root"),
    )
    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if argv[0] == "pg_dump":
            raise FileNotFoundError("pg_dump")
        if argv[-2:] == ["--deployment", "dep"] and "migrations" in argv:
            return _ok({"pending": [], "destructive_pending": []})
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "up-state.json",
            backup_dir=tmp_path / "backups",
            skip_backup=False,
        ),
        command_runner=runner,
    )

    assert result["ok"] is False
    assert result["failed_phase"] == "backup"
    backup = result["phases"][-1]
    assert backup["phase"] == "backup"
    assert backup["ok"] is False
    assert backup["reason"] == "backup tool missing"
    assert backup["error"]["type"] == "FileNotFoundError"
    assert "pg_dump" in backup["command"]
    assert "cannot skip" in backup["fix_hint"]


def test_upgrade_redacts_backup_dsn_from_report_and_persisted_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "not-for-logs"
    dsn = f"postgres://user:{secret}@host/db"
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": dsn,
            "target": {"database": "db", "identity": "ok"},
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: _full_restore_manifest(tmp_path / "dep-root"),
    )

    for backup_returncode in (0, 1):
        def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
            assert env.get("PGPASSWORD") in (None, secret)
            assert secret not in " ".join(argv)
            if argv[:2] in (["pg_dump", "--version"], ["pg_restore", "--version"]):
                return CommandResult(0, f"{argv[0]} (PostgreSQL) 17.10\n", "")
            if argv[:2] == ["pg_restore", "--list"]:
                return CommandResult(0, "; archive listing\n", "")
            if argv[0] == "pg_dump":
                if backup_returncode == 0:
                    Path(argv[argv.index("--file") + 1]).write_bytes(b"archive")
                return CommandResult(
                    backup_returncode,
                    "",
                    f"backup result for {dsn}",
                )
            return _ok()

        state_file = tmp_path / f"up-state-{backup_returncode}.json"
        result = run_deployment_upgrade(
            UpgradeOptions(
                deployment="dep",
                state_file=state_file,
                backup_dir=tmp_path / "backups",
                stop_after="backup",
            ),
            command_runner=runner,
        )

        serialized_report = json.dumps(result)
        serialized_state = state_file.read_text()
        assert secret not in serialized_report
        assert secret not in serialized_state
        assert "--dbname db" in serialized_report
        assert "--dbname db" in serialized_state


def test_upgrade_classifies_incompatible_pg_dump_as_backup_prerequisite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": "postgres://user@host/db",
            "target": {"database": "db", "identity": "ok"},
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: _full_restore_manifest(tmp_path / "dep-root"),
    )

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if argv[:2] == ["pg_dump", "--version"]:
            return CommandResult(0, "pg_dump (PostgreSQL) 16.4\n", "")
        if argv[:2] == ["pg_restore", "--version"]:
            return CommandResult(0, "pg_restore (PostgreSQL) 16.4\n", "")
        if argv[0] == "pg_dump":
            return CommandResult(
                1,
                "",
                "pg_dump: error: server version: 17.10; pg_dump version: 16.4",
            )
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "up-state.json",
            backup_dir=tmp_path / "backups",
        ),
        command_runner=runner,
    )

    backup = result["phases"][-1]
    assert result["failed_phase"] == "backup"
    assert backup["reason"] == "backup tool incompatible with server"
    assert "same major version" in backup["fix_hint"]
    assert "--skip-backup" not in backup["fix_hint"]


def test_backup_archive_is_nonempty_private_digest_bound_and_readable(
    tmp_path: Path,
) -> None:
    from okg.substrate import deployment_upgrade as du

    path = tmp_path / "backup.dump"
    path.write_bytes(b"custom archive proof")
    path.chmod(0o644)
    calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        calls.append(argv)
        return CommandResult(0, "; archive listing\n", "")

    proof = du._validate_backup_archive(
        SimpleNamespace(command_runner=runner, env={}),
        path,
    )

    assert proof["ok"] is True
    assert proof["backup_size_bytes"] == len(b"custom archive proof")
    assert proof["backup_digest"].startswith("sha256:")
    assert proof["backup_mode"] == "0600"
    assert path.stat().st_mode & 0o777 == 0o600
    assert calls == [["pg_restore", "--list", str(path)]]


def test_backup_archive_toc_check_rejects_silent_empty_listing(
    tmp_path: Path,
) -> None:
    """``pg_restore --list`` can exit 0 while emitting nothing for a damaged
    archive. ``toc_readable`` must be able to fail on that, not report a
    constant pass."""
    from okg.substrate import deployment_upgrade as du

    path = tmp_path / "backup.dump"
    path.write_bytes(b"custom archive proof")

    proof = du._validate_backup_archive(
        SimpleNamespace(
            command_runner=lambda argv, env: CommandResult(0, "   \n", ""),
            env={},
        ),
        path,
    )

    assert proof["ok"] is False
    assert proof["archive_checks"]["toc_readable"] is False
    assert proof["reason"] == "backup archive privacy or integrity checks failed"


def test_directory_backup_format_is_private_manifest_bound_and_atomic(
    tmp_path: Path,
) -> None:
    from okg.substrate import deployment_upgrade as du

    temporary = tmp_path / ".dep-archive.partial"
    final = tmp_path / "dep-20260720T120000Z.dumpdir"
    temporary.mkdir()
    (temporary / "toc.dat").write_bytes(b"toc")
    (temporary / "123.dat.gz").write_bytes(b"table payload")

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        assert argv[:2] == ["pg_restore", "--list"]
        return CommandResult(0, "; archive listing\n", "")

    ctx = SimpleNamespace(command_runner=runner, env={})
    finalized = du._finalize_backup_artifact(
        ctx,
        temporary_path=temporary,
        final_path=final,
        strategy={
            "format": "directory",
            "compression": "gzip",
            "compression_level": 1,
        },
    )

    assert finalized["ok"] is True, finalized
    assert finalized["atomic_finalized"] is True
    assert finalized["archive_kind"] == "directory"
    assert final.is_dir() and not temporary.exists()
    assert final.stat().st_mode & 0o777 == 0o700
    manifest_path = final / ".okg-archive-manifest.json"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["schema"] == "okg.backup_archive_manifest.v1"
    assert manifest["root_digest"] == finalized["backup_digest"]
    assert [row["path"] for row in manifest["members"]] == [
        "123.dat.gz",
        "toc.dat",
    ]
    assert all(
        child.stat().st_mode & 0o777 == 0o600
        for child in final.iterdir()
        if child.is_file()
    )
    revalidated = du._validate_backup_archive(ctx, final)
    assert revalidated["ok"] is True, revalidated


def test_directory_backup_manifest_detects_tamper_and_symlink(
    tmp_path: Path,
) -> None:
    from okg.substrate import deployment_upgrade as du

    temporary = tmp_path / ".partial"
    final = tmp_path / "dep-20260720T120000Z.dumpdir"
    temporary.mkdir()
    (temporary / "toc.dat").write_bytes(b"toc")
    ctx = SimpleNamespace(
        command_runner=lambda argv, env: CommandResult(
            0, "; archive listing\n", ""
        ),
        env={},
    )
    assert du._finalize_backup_artifact(
        ctx,
        temporary_path=temporary,
        final_path=final,
        strategy={
            "format": "directory",
            "compression": "gzip",
            "compression_level": 1,
        },
    )["ok"] is True

    (final / "toc.dat").write_bytes(b"tampered")
    assert du._validate_backup_archive(ctx, final)["ok"] is False
    (final / "escape").symlink_to(tmp_path / "outside")
    unsafe = du._validate_backup_archive(ctx, final)
    assert unsafe["ok"] is False
    assert "symlink" in unsafe["error"]["message"]


def test_run_local_archive_validation_rehashes_only_after_metadata_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    archive = tmp_path / "backup.dump"
    archive.write_bytes(b"original archive")
    archive.chmod(0o600)
    ctx = SimpleNamespace(
        data={},
        env={},
        command_runner=lambda argv, env: CommandResult(
            0, "; archive listing\n", ""
        ),
    )
    original_validate = du._validate_backup_archive
    validations = 0

    def validate(_ctx, path: Path, **kwargs):
        nonlocal validations
        validations += 1
        return original_validate(_ctx, path, **kwargs)

    monkeypatch.setattr(du, "_validate_backup_archive", validate)
    first = du._validate_backup_archive_for_run(ctx, archive)
    second = du._validate_backup_archive_for_run(ctx, archive)
    original_stat = archive.stat()
    cached_fingerprint = du._backup_archive_metadata_fingerprint(archive)
    # ctime comes from the kernel's coarse per-tick clock: a tamper that
    # lands in the same tick as the cached fingerprint is (by design)
    # invisible to the cheap drift key. Re-touch until the tick advances
    # so the test exercises the drift path deterministically on fast
    # runners.
    deadline = time.monotonic() + 5.0
    while True:
        archive.write_bytes(b"tampered archive")
        os.utime(
            archive,
            ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
        )
        if du._backup_archive_metadata_fingerprint(archive) != (
            cached_fingerprint
        ):
            break
        assert time.monotonic() < deadline, (
            "archive ctime never advanced past the cached fingerprint"
        )
        time.sleep(0.005)
    assert archive.stat().st_size == original_stat.st_size
    third = du._validate_backup_archive_for_run(ctx, archive)

    assert first["ok"] is True and second["ok"] is True and third["ok"] is True
    assert first["backup_digest"] == second["backup_digest"]
    assert third["backup_digest"] != first["backup_digest"]
    assert validations == 2


def _bank_archive_for_resume(tmp_path: Path):
    """Bank a file archive and return (banked_ctx, archive_path).

    Mirrors what ``_phase_backup`` persists: a ``backup_result`` carrying the
    content digest and the metadata fingerprint.  The fingerprint is the only
    carrier rehydrated on resume."""
    from okg.substrate import deployment_upgrade as du

    archive = tmp_path / "backup.dump"
    archive.write_bytes(b"valid custom archive")
    archive.chmod(0o600)
    ctx = SimpleNamespace(
        data={},
        env={},
        command_runner=lambda argv, env: CommandResult(
            0, "; archive listing\n", ""
        ),
    )
    validation = du._validate_backup_archive(ctx, archive)
    assert validation["ok"] is True
    backup_result = {**validation, "backup_path": str(archive)}
    ctx.data["backup_result"] = backup_result
    fingerprint = du._backup_archive_metadata_fingerprint(archive)
    du._bank_archive_validation(
        ctx, str(archive.resolve()), fingerprint, backup_result
    )
    assert "archive_metadata_fingerprint" in backup_result
    return ctx, archive


def _resume_ctx(banked_ctx):
    """Simulate a resume: JSON round-trip and rehydrate only ``backup_result``.

    Resume rehydration copies specific ctx.data keys (backup_result, ...) and
    NOT the in-run validation cache, so the fresh ctx deliberately omits it."""
    resumed = json.loads(json.dumps({"backup_result": banked_ctx.data["backup_result"]}))
    return SimpleNamespace(
        data=resumed,
        env={},
        command_runner=lambda argv, env: CommandResult(
            0, "; archive listing\n", ""
        ),
    )


def _counting_validate(monkeypatch):
    from okg.substrate import deployment_upgrade as du

    original = du._validate_backup_archive
    calls = {"n": 0}

    def counting(_ctx, path, **kwargs):
        calls["n"] += 1
        return original(_ctx, path, **kwargs)

    monkeypatch.setattr(du, "_validate_backup_archive", counting)
    return calls


def test_resume_reuses_persisted_archive_fingerprint_without_rehash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    banked_ctx, archive = _bank_archive_for_resume(tmp_path)
    banked_digest = banked_ctx.data["backup_result"]["backup_digest"]

    # Record commands so we can prove the cheap TOC re-list still runs while the
    # expensive content re-hash does not.
    commands: list[list[str]] = []

    resumed = _resume_ctx(banked_ctx)

    def recording_runner(argv, env):
        commands.append(argv)
        return CommandResult(0, "; archive listing\n", "")

    resumed.command_runner = recording_runner

    calls = _counting_validate(monkeypatch)
    result = du._validate_backup_archive_for_run(resumed, archive)

    # Clean resume: the persisted fingerprint matches on-disk metadata, so the
    # banked digest is reused. Decouple contract: the TOC is re-listed (cheap
    # restorability re-check) but NO full content re-digest runs.
    assert calls["n"] == 0
    assert result["ok"] is True
    assert result["backup_digest"] == banked_digest
    assert result.get("archive_content_rehash_skipped") is True
    assert ["pg_restore", "--list", str(archive)] in commands


def test_resume_chmod_drift_rehashes_once_and_rebanks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    banked_ctx, archive = _bank_archive_for_resume(tmp_path)
    banked_digest = banked_ctx.data["backup_result"]["backup_digest"]
    banked_fingerprint = banked_ctx.data["backup_result"][
        "archive_metadata_fingerprint"
    ]
    resumed = _resume_ctx(banked_ctx)

    # Drift the archive metadata while keeping the bytes identical: the chmod
    # pass validation itself performs (which moves ctime), plus an mtime move.
    # Waiting on ctime alone is a clock race -- its granularity is coarse enough
    # on some filesystems that a chmod lands in the same tick as banking and no
    # drift is observable. The mtime move is deterministic on every platform and
    # validation does not converge it away, so the re-banked key is provably
    # different from the banked one below.
    original = archive.stat()
    archive.chmod(0o600)
    os.utime(
        archive,
        ns=(original.st_atime_ns, original.st_mtime_ns - 1_000_000_000),
    )
    assert archive.stat().st_size == original.st_size
    # Compare canonically on BOTH sides: the raw fingerprint is nested tuples
    # while the banked one round-tripped through JSON into nested lists, and a
    # tuple never equals a list, so a raw compare is vacuously "drifted".
    assert du._canonical_fingerprint(
        du._backup_archive_metadata_fingerprint(archive)
    ) != du._canonical_fingerprint(banked_fingerprint), "drift was not staged"

    calls = _counting_validate(monkeypatch)
    result = du._validate_backup_archive_for_run(resumed, archive)

    # Drift forces exactly one full content re-digest (never the cheap reuse);
    # the digest still matches what banking proved, so the run proceeds and the
    # key is re-banked.
    assert calls["n"] == 1
    assert result.get("archive_content_rehash_skipped") is None
    assert result["ok"] is True
    assert result["backup_digest"] == banked_digest
    rebanked = resumed.data["backup_result"]["archive_metadata_fingerprint"]
    assert rebanked == du._canonical_fingerprint(
        du._backup_archive_metadata_fingerprint(archive)
    )
    assert rebanked != du._canonical_fingerprint(banked_fingerprint)

    # A subsequent boundary in the same run reuses the re-banked key: no rehash.
    second = du._validate_backup_archive_for_run(resumed, archive)
    assert calls["n"] == 1
    assert second["backup_digest"] == banked_digest


def test_resume_content_tamper_still_refuses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    banked_ctx, archive = _bank_archive_for_resume(tmp_path)
    banked_digest = banked_ctx.data["backup_result"]["backup_digest"]
    banked_fingerprint = banked_ctx.data["backup_result"][
        "archive_metadata_fingerprint"
    ]
    resumed = _resume_ctx(banked_ctx)

    archive.write_bytes(b"tampered custom archive")
    archive.chmod(0o600)

    calls = _counting_validate(monkeypatch)
    result = du._validate_backup_archive_for_run(resumed, archive)

    # Tamper: the re-digest disagrees with the banked digest -> the
    # mutation-boundary refusal condition holds and the persisted key is NOT
    # advanced past the tampered metadata.
    assert calls["n"] == 1
    refuses = (
        result.get("ok") is not True
        or result.get("backup_digest") != banked_digest
    )
    assert refuses
    assert result["backup_digest"] != banked_digest
    assert resumed.data["backup_result"]["archive_metadata_fingerprint"] == (
        du._canonical_fingerprint(banked_fingerprint)
    )


def test_directory_backup_interruption_cleanup_is_idempotent(
    tmp_path: Path,
) -> None:
    from okg.substrate import deployment_upgrade as du

    partial = tmp_path / ".dep.dumpdir.partial"
    partial.mkdir()
    (partial / "toc.dat").write_bytes(b"partial")

    first = du._remove_partial_backup_artifact(partial)
    second = du._remove_partial_backup_artifact(partial)

    assert first["removed"] is True
    assert second == {"removed": False, "path": str(partial), "reason": "absent"}


def test_parallel_dump_format_reserves_leader_connection() -> None:
    from okg.substrate import deployment_upgrade as du

    command = du._backup_dump_command(
        database="db",
        artifact_path=Path("archive.dumpdir"),
        strategy={
            "format": "directory",
            "compression": "gzip",
            "compression_level": 1,
            "jobs": 4,
            "lock_wait_timeout_seconds": 30,
        },
    )
    assert "--format=directory" in command
    assert command[command.index("--jobs") + 1] == "4"
    assert "--compress=gzip:1" in command
    assert "--lock-wait-timeout" in command


def test_okg_workspace_backup_selects_qualified_parallel_dump_format_strategy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.deployments import resolve_deployment

    manifest = resolve_deployment("okg-workspace")
    monkeypatch.setattr(du, "_resolve_upgrade_deployment", lambda ctx: manifest)

    policy = du._backup_policy_config(SimpleNamespace())

    assert policy["dump_strategy"] == {
        "format": "directory",
        "compression": "gzip",
        "compression_level": 1,
        "jobs": 4,
        "connection_safety_margin": 4,
        "lock_wait_timeout_seconds": 5.0,
    }
    assert policy["budgets_seconds"] == {
        "routine": {
            "backup": 60.0,
            "attribute_read_model": 30.0,
            "total": 600.0,
        },
        "forced": {
            "dump": 1800.0,
            "backup": 10800.0,
            "total": 21600.0,
        },
    }


def test_restore_headroom_uses_larger_observed_size_and_declared_factor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import host_headroom

    docker_root = tmp_path / "docker"
    docker_root.mkdir()
    observed: dict[str, object] = {}

    def preflight(**kwargs) -> dict:
        observed.update(kwargs)
        return {"ok": True, "refuse": False, "status": "ok"}

    monkeypatch.setattr(host_headroom, "resolve_docker_data_root", lambda: docker_root)
    monkeypatch.setattr(host_headroom, "big_writer_preflight", preflight)
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda deployment: tmp_path,
    )
    report = _actual_restore_headroom_preflight(
        SimpleNamespace(options=SimpleNamespace(deployment="dep")),
        {
            "restore_volume": {"kind": "docker_data_root"},
            "restore_peak_factor": 1.25,
        },
        source_database_size=60 * 1024**3,
        prior_restored_size=70 * 1024**3,
    )

    assert report["refuse"] is False
    assert observed["target_path"] == docker_root
    assert observed["needed_bytes"] == int(70 * 1024**3 * 1.25)
    assert report["estimated_peak_bytes"] == observed["needed_bytes"]


def test_restore_capacity_blocker_attaches_cold_history_advisory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.db import cold_history
    from okg.substrate.runtime import host_headroom

    restore_root = tmp_path / "restore"
    restore_root.mkdir()
    thresholds = host_headroom.HostStorageThresholds(
        warn_available_pct=15.0,
        blocker_available_pct=8.0,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda deployment: tmp_path,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._headroom_threshold_authority",
        lambda ctx, deployment_dir: (
            thresholds,
            [],
            {"kind": "test_manifest"},
        ),
    )
    monkeypatch.setattr(
        host_headroom,
        "big_writer_preflight",
        lambda **kwargs: {
            "ok": False,
            "refuse": True,
            "status": "blocker",
            "code": "host_headroom_blocker",
            "message": "measured restore shortfall",
            "available_bytes": 200,
            "needed_bytes": kwargs["needed_bytes"],
            "shortfall_bytes": 50,
        },
    )
    expected = cold_history.summarize_cold_history_reclaim(
        {
            "floor": {"floor": 123, "headroom_generations": 100},
            "indexes": [{
                "logical_table": "okg.graph_nodes",
                "physical_table": "nodes_physical",
                "suffix": "node_id_visible",
                "rotatable": True,
                "full_index": {
                    "index_name": "node_id_full",
                    "bytes": 75,
                    "indisvalid": True,
                    "indisready": True,
                },
            }],
        },
        deployment="dep",
    )
    calls: list[tuple[str, str]] = []

    def advisory(dsn: str, *, deployment: str) -> dict:
        calls.append((dsn, deployment))
        return expected

    monkeypatch.setattr(
        cold_history,
        "cold_history_reclaim_advisory",
        advisory,
    )

    report = _actual_restore_headroom_preflight(
        SimpleNamespace(
            options=SimpleNamespace(deployment="dep"),
            dsn="postgres://deployment",
        ),
        {
            "restore_volume": {
                "kind": "host_path",
                "path": str(restore_root),
            },
            "restore_peak_factor": 1.25,
        },
        source_database_size=100,
        prior_restored_size=None,
    )

    assert calls == [("postgres://deployment", "dep")]
    assert report["cold_history_reclaim"] == expected
    assert report["available_bytes"] == 200
    assert report["shortfall_bytes"] == 50
    assert "75 gross bytes across 1 rotatable" in report["message"]
    assert expected["inspect_command"] in report["message"]
    assert expected["activate_command"] in report["message"]
    assert "drops the listed full-history B-tree predecessors" in report["message"]


def test_restore_non_capacity_refusals_skip_cold_history_advisory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.db import cold_history
    from okg.substrate.runtime import host_headroom

    calls: list[object] = []
    monkeypatch.setattr(
        cold_history,
        "cold_history_reclaim_advisory",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda deployment: tmp_path,
    )
    thresholds = host_headroom.HostStorageThresholds(
        warn_available_pct=15.0,
        blocker_available_pct=8.0,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._headroom_threshold_authority",
        lambda ctx, deployment_dir: (
            thresholds,
            [],
            {"kind": "test_manifest"},
        ),
    )
    ctx = SimpleNamespace(
        options=SimpleNamespace(deployment="dep"),
        dsn="postgres://deployment",
    )

    monkeypatch.setattr(host_headroom, "resolve_docker_data_root", lambda: None)
    unresolved = _actual_restore_headroom_preflight(
        ctx,
        {
            "restore_volume": {"kind": "docker_data_root"},
            "restore_peak_factor": 1.25,
        },
        source_database_size=100,
        prior_restored_size=None,
    )
    missing = _actual_restore_headroom_preflight(
        ctx,
        {
            "restore_volume": {
                "kind": "host_path",
                "path": str(tmp_path / "missing"),
            },
            "restore_peak_factor": 1.25,
        },
        source_database_size=100,
        prior_restored_size=None,
    )
    unreadable = _actual_restore_headroom_preflight(
        ctx,
        {
            "restore_volume": {
                "kind": "host_path",
                "path": str(tmp_path),
            },
            "restore_peak_factor": 1.25,
        },
        source_database_size=None,
        prior_restored_size=None,
    )
    reports = [unresolved, missing, unreadable]
    for status, code in (
        ("error", "host_headroom_probe_failed"),
        ("misconfigured", "host_headroom_thresholds_undeclared"),
    ):
        monkeypatch.setattr(
            host_headroom,
            "big_writer_preflight",
            lambda status=status, code=code, **kwargs: {
                "ok": False,
                "refuse": True,
                "status": status,
                "code": code,
                "message": "non-capacity refusal",
            },
        )
        reports.append(
            _actual_restore_headroom_preflight(
                ctx,
                {
                    "restore_volume": {
                        "kind": "host_path",
                        "path": str(tmp_path),
                    },
                    "restore_peak_factor": 1.25,
                },
                source_database_size=100,
                prior_restored_size=None,
            )
        )

    assert calls == []
    assert [report["status"] for report in reports] == [
        "unresolved",
        "restore_volume_missing",
        "unreadable_source_size",
        "error",
        "misconfigured",
    ]
    assert all("cold_history_reclaim" not in report for report in reports)


def test_restore_capacity_blocker_survives_cold_history_planner_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.db import cold_history
    from okg.substrate.runtime import host_headroom

    restore_root = tmp_path / "restore"
    restore_root.mkdir()
    thresholds = host_headroom.HostStorageThresholds(
        warn_available_pct=15.0,
        blocker_available_pct=8.0,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda deployment: tmp_path,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._headroom_threshold_authority",
        lambda ctx, deployment_dir: (
            thresholds,
            [],
            {"kind": "test_manifest"},
        ),
    )
    monkeypatch.setattr(
        host_headroom,
        "big_writer_preflight",
        lambda **kwargs: {
            "ok": False,
            "refuse": True,
            "status": "blocker",
            "code": "host_headroom_blocker",
            "message": "original measured refusal",
            "available_bytes": 200,
            "needed_bytes": kwargs["needed_bytes"],
            "shortfall_bytes": 50,
        },
    )

    def fail_catalog(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("catalog unavailable")

    monkeypatch.setattr(cold_history, "cold_history_report", fail_catalog)

    report = _actual_restore_headroom_preflight(
        SimpleNamespace(
            options=SimpleNamespace(deployment="dep"),
            dsn="postgres://deployment",
        ),
        {
            "restore_volume": {
                "kind": "host_path",
                "path": str(restore_root),
            },
            "restore_peak_factor": 1.25,
        },
        source_database_size=100,
        prior_restored_size=None,
    )

    assert report["refuse"] is True
    assert report["status"] == "blocker"
    assert report["code"] == "host_headroom_blocker"
    assert report["available_bytes"] == 200
    assert report["needed_bytes"] == 125
    assert report["shortfall_bytes"] == 50
    assert report["cold_history_reclaim"]["status"] == "unavailable"
    assert report["cold_history_reclaim"]["rotatable_index_count"] is None
    assert report["cold_history_reclaim"]["error"] == {
        "type": "RuntimeError",
        "message": "catalog unavailable",
    }
    assert report["message"].startswith("original measured refusal;")
    assert "could not be measured" in report["message"]


def test_restore_headroom_refuses_unresolved_docker_volume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import host_headroom

    monkeypatch.setattr(host_headroom, "resolve_docker_data_root", lambda: None)
    report = _actual_restore_headroom_preflight(
        SimpleNamespace(options=SimpleNamespace(deployment="dep")),
        {
            "restore_volume": {"kind": "docker_data_root"},
            "restore_peak_factor": 1.25,
        },
        source_database_size=60 * 1024**3,
        prior_restored_size=None,
    )

    assert report["refuse"] is True
    assert report["status"] == "unresolved"


def test_restore_headroom_refuses_missing_declared_host_path(
    tmp_path: Path,
) -> None:
    report = _actual_restore_headroom_preflight(
        SimpleNamespace(options=SimpleNamespace(deployment="dep")),
        {
            "restore_volume": {
                "kind": "host_path",
                "path": str(tmp_path / "missing-volume"),
            },
            "restore_peak_factor": 1.25,
        },
        source_database_size=60 * 1024**3,
        prior_restored_size=None,
    )

    assert report["refuse"] is True
    assert report["status"] == "restore_volume_missing"


def test_archive_headroom_refuses_unreadable_source_size(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import host_headroom

    thresholds = host_headroom.HostStorageThresholds(
        warn_available_pct=15.0,
        blocker_available_pct=8.0,
    )
    monkeypatch.setattr(
        host_headroom,
        "load_host_storage_thresholds",
        lambda deployment_dir: (thresholds, []),
    )
    monkeypatch.setattr(
        host_headroom,
        "big_writer_preflight",
        lambda **kwargs: {"ok": True, "refuse": False, "status": "ok"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda deployment: tmp_path,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._estimate_source_db_bytes",
        lambda ctx: None,
    )
    report = _actual_backup_headroom_preflight(
        SimpleNamespace(
            options=SimpleNamespace(deployment="dep"),
            backup_dir=tmp_path,
        )
    )

    assert report["refuse"] is True
    assert report["status"] == "unreadable_source_size"


def test_backup_admission_reports_exact_shortfall_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record_property,
) -> None:
    from okg.substrate.runtime import host_headroom

    record_property(
        "requirements",
        "req.runtime-authority.backup-admission "
        "test.runtime-authority.backup-admission",
    )
    thresholds = host_headroom.HostStorageThresholds(
        warn_available_pct=15.0,
        blocker_available_pct=10.0,
    )
    monkeypatch.setattr(
        host_headroom,
        "load_host_storage_thresholds",
        lambda _deployment_dir: (thresholds, []),
    )
    samples = iter(((1_000, 200), (1_000, 400)))
    monkeypatch.setattr(
        host_headroom,
        "probe_filesystem",
        lambda _path: next(samples),
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda _deployment: tmp_path,
    )
    ctx = SimpleNamespace(
        options=SimpleNamespace(
            deployment="dep",
            precheckout_target_revision=None,
        ),
        backup_dir=tmp_path / "backups",
    )

    report = _actual_backup_headroom_preflight(ctx, needed_bytes=150)

    assert report["refuse"] is True
    assert report["status"] == "blocker"
    assert report["available_bytes"] == 200
    assert report["needed_bytes"] == 150
    assert report["blocker_floor_bytes"] == 100
    assert report["projected_available_bytes"] == 50
    assert report["shortfall_bytes"] == 50

    resumed_report = _actual_backup_headroom_preflight(
        ctx,
        needed_bytes=150,
    )
    assert resumed_report["refuse"] is False
    assert resumed_report["available_bytes"] == 400
    assert resumed_report["projected_available_bytes"] == 250


def test_backup_admission_refusal_starts_no_writer_and_emits_resume_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record_property,
) -> None:
    from okg.substrate import deployment_upgrade as du

    record_property(
        "requirements",
        "req.runtime-authority.backup-admission "
        "test.runtime-authority.backup-admission",
    )
    options = UpgradeOptions(
        deployment="dep",
        profile="prod",
        dsn="postgresql://postgres:test@127.0.0.1:5432/dep",
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path / "backups",
        independent_backup_verify=True,
        resume=True,
        reset_state=True,
    )
    ctx = UpgradeContext(
        options=options,
        dsn=options.dsn or "",
        resolution={"target": {"database": "dep", "identity": "isolated"}},
        env={},
        state_file=options.state_file or tmp_path / "state.json",
        backup_dir=options.backup_dir or tmp_path / "backups",
        command_runner=lambda _argv, _env: pytest.fail(
            "backup writer must not start"
        ),
    )
    policy = {
        "dump_strategy": {
            "format": "directory",
            "compression": "gzip",
            "compression_level": 1,
            "jobs": 4,
            "lock_wait_timeout_seconds": 5,
        },
        "progress_interval_seconds": 1,
        "verification_policy": "current_full_restore",
    }
    monkeypatch.setattr(du, "_backup_policy_config", lambda _ctx: policy)
    monkeypatch.setattr(
        du,
        "_backup_tool_preflight",
        lambda _ctx: {"ok": True, "validation": {}},
    )
    monkeypatch.setattr(
        du,
        "_dump_strategy_preflight",
        lambda *_args, **_kwargs: {"ok": True},
    )
    monkeypatch.setattr(du, "_estimate_source_db_bytes", lambda _ctx: 150)
    monkeypatch.setattr(
        du,
        "_directory_inode_headroom",
        lambda *_args, **_kwargs: {"ok": True},
    )
    monkeypatch.setattr(
        du,
        "_backup_headroom_preflight",
        lambda *_args, **_kwargs: {
            "ok": False,
            "refuse": True,
            "status": "blocker",
            "runbook": "docs/runbooks/troubleshooting/storage.md",
        },
    )
    monkeypatch.setattr(
        du,
        "_run_long_command",
        lambda *_args, **_kwargs: pytest.fail("pg_dump must not start"),
    )

    result = du._phase_backup(ctx)

    assert result["ok"] is False
    assert result["mutation_authorized"] is False
    assert result["physical_backup_started"] is False
    assert "--resume" in result["next_command"]
    assert "--reset-state" not in result["next_command"]
    assert not ctx.backup_dir.exists()


def test_candidate_threshold_authority_covers_every_backup_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.runtime import host_headroom

    deployment_dir = tmp_path / "deployment"
    deployment_dir.mkdir()
    (deployment_dir / "deployment.yaml").write_text("name: dep\n")
    restore_volume = tmp_path / "restore-volume"
    restore_volume.mkdir()
    revision = "a" * 40
    ctx = SimpleNamespace(
        options=SimpleNamespace(
            deployment="dep",
            precheckout_target_revision=revision,
        ),
        data={
            "effect_inventory_static": {
                "target_revision": revision,
                "raw_manifest": {
                    "host_storage": {
                        "warn_available_pct": 15,
                        "blocker_available_pct": 6,
                    },
                },
            },
        },
        backup_dir=tmp_path / "backups",
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda deployment: deployment_dir,
    )
    monkeypatch.setattr(
        du,
        "_upgrade_manifest_digest",
        lambda _ctx: "sha256:candidate-manifest",
    )
    monkeypatch.setattr(
        host_headroom,
        "probe_filesystem",
        lambda _path: (100 * 1024**3, 80 * 1024**3),
    )

    reports = [
        _actual_backup_headroom_preflight(ctx, needed_bytes=10 * 1024**3),
        _actual_backup_post_dump_headroom_preflight(ctx),
        _actual_restore_headroom_preflight(
            ctx,
            {
                "restore_volume": {
                    "kind": "host_path",
                    "path": str(restore_volume),
                },
                "restore_peak_factor": 1.25,
            },
            source_database_size=10 * 1024**3,
            prior_restored_size=None,
        ),
    ]

    assert all(report["refuse"] is False for report in reports)
    assert all(
        report["thresholds"]
        == {"warn_available_pct": 15.0, "blocker_available_pct": 6.0}
        for report in reports
    )
    assert all(
        report["threshold_authority"]["target_manifest_digest"]
        == "sha256:candidate-manifest"
        for report in reports
    )


def test_candidate_threshold_revision_mismatch_never_falls_back_to_installed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.runtime import host_headroom

    deployment_dir = tmp_path / "deployment"
    deployment_dir.mkdir()
    (deployment_dir / "deployment.yaml").write_text(
        "name: dep\nhost_storage:\n"
        "  warn_available_pct: 15\n"
        "  blocker_available_pct: 6\n"
    )
    restore_volume = tmp_path / "restore-volume"
    restore_volume.mkdir()
    revision = "b" * 40
    ctx = SimpleNamespace(
        options=SimpleNamespace(
            deployment="dep",
            precheckout_target_revision=revision,
        ),
        data={
            "effect_inventory_static": {
                "target_revision": "c" * 40,
                "raw_manifest": {
                    "host_storage": {
                        "warn_available_pct": 15,
                        "blocker_available_pct": 6,
                    },
                },
            },
        },
        backup_dir=tmp_path / "backups",
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda deployment: deployment_dir,
    )
    monkeypatch.setattr(
        du,
        "_upgrade_manifest_digest",
        lambda _ctx: "sha256:candidate-manifest",
    )
    probe_calls: list[Path] = []

    def probe(path: Path) -> tuple[int, int]:
        probe_calls.append(path)
        return (100 * 1024**3, 80 * 1024**3)

    monkeypatch.setattr(host_headroom, "probe_filesystem", probe)
    reports = [
        _actual_backup_headroom_preflight(ctx, needed_bytes=10 * 1024**3),
        _actual_backup_post_dump_headroom_preflight(ctx),
        _actual_restore_headroom_preflight(
            ctx,
            {
                "restore_volume": {
                    "kind": "host_path",
                    "path": str(restore_volume),
                },
                "restore_peak_factor": 1.25,
            },
            source_database_size=10 * 1024**3,
            prior_restored_size=None,
        ),
    ]

    assert all(report["refuse"] is True for report in reports)
    assert all(report["status"] == "misconfigured" for report in reports)
    assert all(
        "refusing to fall back" in " ".join(report["problems"])
        for report in reports
    )
    assert probe_calls == []


def test_long_backup_command_emits_heartbeat_before_runner_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    finished = {"value": False}
    writes: list[tuple[str, bool]] = []

    class Sink:
        def write(self, value: str) -> int:
            if value.strip():
                writes.append((value, finished["value"]))
            return len(value)

        def flush(self) -> None:
            return None

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del argv, env
        __import__("time").sleep(0.04)
        finished["value"] = True
        return CommandResult(0, "", "")

    monkeypatch.setattr(du.sys, "stderr", Sink())
    result, progress = du._run_long_command(
        SimpleNamespace(command_runner=runner),
        ["pg_dump", "--dbname", "db"],
        {"PGPASSWORD": "must-not-appear"},
        operation="pg_dump",
        progress_interval_seconds=0.01,
    )

    assert result.ok is True
    assert progress["heartbeat_count"] >= 1
    assert progress["heartbeat_before_completion"] is True
    assert any(finished_at_write is False for _, finished_at_write in writes)
    assert all("must-not-appear" not in text for text, _ in writes)


def test_interrupted_real_backup_terminates_the_child_process_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    launched: dict[str, object] = {}
    signals: list[tuple[int, int]] = []

    class Process:
        pid = 43210
        returncode = None
        interrupted = False

        def communicate(self, timeout=None):
            del timeout
            if not self.interrupted:
                self.interrupted = True
                raise KeyboardInterrupt
            self.returncode = -15
            return "", ""

        def poll(self):
            return self.returncode

        def terminate(self) -> None:
            raise AssertionError("process-group termination should be used")

        def kill(self) -> None:
            raise AssertionError("SIGTERM should complete in this test")

    def popen(argv, **kwargs):
        launched.update({"argv": argv, **kwargs})
        return Process()

    monkeypatch.setattr(du.subprocess, "Popen", popen)
    monkeypatch.setattr(
        du.os,
        "killpg",
        lambda pid, sig: signals.append((pid, sig)),
    )

    with pytest.raises(KeyboardInterrupt):
        du._run_long_command(
            SimpleNamespace(command_runner=du._default_command_runner),
            ["pg_dump", "--dbname", "db"],
            {},
            operation="pg_dump",
            progress_interval_seconds=30,
        )

    assert launched["start_new_session"] is True
    assert signals == [(43210, du.signal.SIGTERM)]


class _BackupVerifyCursor:
    def __init__(
        self,
        *,
        owner: str = "dep",
        timescaledb_version: str | None = None,
        hypertables: list[tuple[str, str, int]] | None = None,
        queries: list[str] | None = None,
    ) -> None:
        self.owner = owner
        self.timescaledb_version = timescaledb_version
        self.hypertables = hypertables or []
        self.queries = queries
        self.last_query = ""

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        return None

    def execute(self, query, params=None) -> None:
        del params
        self.last_query = str(query)
        if self.queries is not None:
            self.queries.append(self.last_query)

    def fetchone(self):
        # This cursor intentionally models several SQL result schemas. Each
        # branch returns the row shape of the query most recently executed.
        if "extname = 'timescaledb'" in self.last_query:
            return (
                [self.timescaledb_version]
                if self.timescaledb_version is not None
                else None
            )
        if "owner_deployment_name" in self.last_query:
            return [self.owner, "owner-uuid", "schema-fingerprint"]
        if "pg_database_size" in self.last_query:
            return [1024]
        return [True]

    def fetchall(self):
        if "extname, extversion" in self.last_query:
            return (
                [("timescaledb", self.timescaledb_version)]
                if self.timescaledb_version is not None
                else []
            )
        if "timescaledb_information.hypertables" in self.last_query:
            return list(self.hypertables)
        if "timescaledb_information.continuous_aggregates" in self.last_query:
            return []
        if "migration_id, checksum" in self.last_query:
            return [("0000_schema_snapshot", "sha256:test", False)]
        if "FROM pg_class c" in self.last_query and "pg_attribute" in self.last_query:
            return [("database_owner", "r", 1, "singleton", "boolean", True, "")]
        if "FROM pg_constraint" in self.last_query:
            return []
        if "FROM pg_index" in self.last_query:
            return []
        if "FROM pg_trigger" in self.last_query:
            return []
        if "FROM pg_proc" in self.last_query:
            return []
        return []


class _BackupVerifyConnection:
    def __init__(
        self,
        *,
        owner: str = "dep",
        timescaledb_version: str | None = None,
        hypertables: list[tuple[str, str, int]] | None = None,
        queries: list[str] | None = None,
    ) -> None:
        self.owner = owner
        self.timescaledb_version = timescaledb_version
        self.hypertables = hypertables
        self.queries = queries

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        return None

    def cursor(self) -> _BackupVerifyCursor:
        return _BackupVerifyCursor(
            owner=self.owner,
            timescaledb_version=self.timescaledb_version,
            hypertables=self.hypertables,
            queries=self.queries,
        )


def test_backup_disposable_restore_uses_env_credentials_and_checks_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *args, **kwargs: _BackupVerifyConnection(owner="dep"),
    )
    commands: list[tuple[list[str], dict[str, str]]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        commands.append((argv, env))
        return CommandResult(0, "; archive listing\n", "")

    result = _actual_verify_backup_restore(
        SimpleNamespace(
            dsn="postgresql://user:secret@host/db",
            env={"PGOPTIONS": "-c statement_timeout=0"},
            options=SimpleNamespace(deployment="dep"),
            command_runner=runner,
            data={
                "backup_policy_config": {
                    "max_parallel_restore_jobs": 4,
                    "progress_interval_seconds": 30,
                }
            },
        ),
        tmp_path / "backup.dump",
    )

    assert result["ok"] is True
    assert result["restore_verified"] is True
    assert result["schema_ok"] is True
    assert result["ownership_ok"] is True
    assert result["restore_parallel_jobs"] == 4
    assert result["progress"]["pg_restore"]["operation"] == "pg_restore"
    # First command reads the archive TOC; the restore itself follows.
    assert commands[0][0][:2] == ["pg_restore", "--list"]
    argv, env = commands[1]
    assert "--use-list" in argv
    assert argv[argv.index("--jobs") + 1] == "4"
    assert "secret" not in " ".join(argv)
    assert env["PGPASSWORD"] == "secret"
    assert env["PGOPTIONS"] == (
        "-c statement_timeout=0 "
        "-c default_transaction_read_only=off "
        "-c max_parallel_maintenance_workers=0"
    )
    assert argv[argv.index("--dbname") + 1].startswith("okg_upgrade_verify_")


def test_backup_restore_uses_caller_policy_when_backup_phase_did_not_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mutation-time revalidation resumes from the state file, so the
    in-memory ``backup_policy_config`` is absent. The caller's resolved
    policy must still drive parallelism rather than silently degrading to
    a serial restore."""
    import psycopg

    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *args, **kwargs: _BackupVerifyConnection(owner="dep"),
    )
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        commands.append(argv)
        return CommandResult(0, "; archive listing\n", "")

    result = _actual_verify_backup_restore(
        SimpleNamespace(
            dsn="postgresql://user@host/db",
            env={},
            options=SimpleNamespace(deployment="dep"),
            command_runner=runner,
            data={},
        ),
        tmp_path / "backup.dump",
        policy={
            "max_parallel_restore_jobs": 4,
            "progress_interval_seconds": 30,
        },
    )

    restore = next(command for command in commands if "--dbname" in command)
    assert result["ok"] is True
    assert result["restore_parallel_jobs"] == 4
    assert restore[restore.index("--jobs") + 1] == "4"


def test_backup_disposable_restore_keeps_timescaledb_serial(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *args, **kwargs: _BackupVerifyConnection(
            owner="dep",
            timescaledb_version="2.20.3",
        ),
    )
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        commands.append(argv)
        return CommandResult(0, "; archive listing\n", "")

    result = _actual_verify_backup_restore(
        SimpleNamespace(
            dsn="postgresql://user@host/db",
            env={},
            options=SimpleNamespace(deployment="dep"),
            command_runner=runner,
            data={
                "backup_policy_config": {
                    "max_parallel_restore_jobs": 4,
                    "progress_interval_seconds": 30,
                }
            },
        ),
        tmp_path / "backup.dump",
    )

    restore = next(command for command in commands if "--dbname" in command)
    assert result["ok"] is True
    assert result["restore_parallel_jobs"] == 1
    assert "TimescaleDB" in result["restore_parallel_reason"]
    assert "--jobs" not in restore


@pytest.mark.parametrize(
    ("restore_ok", "owner", "expected_reason"),
    [
        (False, "dep", "backup disposable restore failed"),
        (True, "another-deployment", "backup disposable restore identity check failed"),
    ],
)
def test_backup_disposable_restore_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    restore_ok: bool,
    owner: str,
    expected_reason: str,
) -> None:
    import psycopg

    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *args, **kwargs: _BackupVerifyConnection(owner=owner),
    )
    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if argv[:2] == ["pg_restore", "--list"]:
            return CommandResult(0, "; archive listing\n", "")
        return CommandResult(
            0 if restore_ok else 1,
            "",
            "induced restore failure" if not restore_ok else "",
        )

    result = _actual_verify_backup_restore(
        SimpleNamespace(
            dsn="postgresql://user:secret@host/db",
            env={},
            options=SimpleNamespace(deployment="dep"),
            command_runner=runner,
        ),
        tmp_path / "backup.dump",
    )

    assert result["ok"] is False
    assert result["reason"] == expected_reason


def test_backup_disposable_restore_rejects_schema_catalog_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg
    from okg.substrate import deployment_upgrade as du

    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *args, **kwargs: _BackupVerifyConnection(owner="dep"),
    )
    expected = du._database_logical_identity(_BackupVerifyConnection(owner="dep"))
    expected["schema_catalog_digest"] = "sha256:different-catalog"

    result = _actual_verify_backup_restore(
        SimpleNamespace(
            dsn="postgresql://user@host/db",
            env={},
            options=SimpleNamespace(deployment="dep"),
            command_runner=lambda argv, env: CommandResult(
                0, "; archive listing\n", ""
            ),
        ),
        tmp_path / "backup.dump",
        expected_snapshot=expected,
    )

    assert result["ok"] is False
    assert result["reason"] == "backup disposable restore identity check failed"
    assert result["schema_ok"] is False
    assert result["identity_checks"]["schema_catalog_equal"] is False


def test_backup_snapshot_records_timescaledb_state() -> None:
    """Requirement: detect TimescaleDB by probing, never assuming, and
    record the protected tables; a plain target records an explicit absent
    marker; an unreadable probe raises instead of reading as 'no
    TimescaleDB'."""
    from okg.substrate import deployment_upgrade as du

    timescale = du._database_logical_identity(
        _BackupVerifyConnection(
            owner="dep",
            timescaledb_version="2.28.2",
            hypertables=[("okg", "edge_facts", 12), ("okg", "node_facts", 7)],
        )
    )["timescaledb"]
    assert timescale == {
        "extension_version": "2.28.2",
        "hypertables": [["okg", "edge_facts"], ["okg", "node_facts"]],
        "continuous_aggregates": [],
        "hypertable_chunk_counts": [
            ["okg", "edge_facts", 12],
            ["okg", "node_facts", 7],
        ],
    }

    plain = du._database_logical_identity(
        _BackupVerifyConnection(owner="dep")
    )["timescaledb"]
    assert plain == {
        "extension_version": None,
        "hypertables": [],
        "continuous_aggregates": [],
        "hypertable_chunk_counts": [],
    }

    class UnreadableCursor(_BackupVerifyCursor):
        def execute(self, query, params=None) -> None:
            if "timescaledb_information.hypertables" in str(query):
                raise RuntimeError("hypertable probe failed")
            super().execute(query, params)

    class UnreadableConnection(_BackupVerifyConnection):
        def cursor(self):
            return UnreadableCursor(
                owner=self.owner,
                timescaledb_version=self.timescaledb_version,
            )

    with pytest.raises(RuntimeError, match="hypertable probe failed"):
        du._database_logical_identity(
            UnreadableConnection(owner="dep", timescaledb_version="2.28.2")
        )


def test_verified_restore_records_protected_hypertables(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The timescale_restore block is stamped from the executed restore's
    success branch and names the hypertables the procedure protected."""
    import psycopg

    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *args, **kwargs: _BackupVerifyConnection(
            owner="dep",
            timescaledb_version="2.28.2",
            hypertables=[("okg", "edge_facts", 12)],
        ),
    )

    result = _actual_verify_backup_restore(
        SimpleNamespace(
            dsn="postgresql://user@host/db",
            env={},
            options=SimpleNamespace(deployment="dep"),
            command_runner=lambda argv, env: CommandResult(
                0, "; archive listing\n", ""
            ),
        ),
        tmp_path / "backup.dump",
    )

    assert result["ok"] is True
    assert result["identity_checks"]["timescale_state_equal"] is True
    assert result["timescale_restore"] == {
        "status": "proven",
        "extension_version": "2.28.2",
        "pre_post_restore_used": True,
        "hypertables_protected": [["okg", "edge_facts"]],
        "continuous_aggregates": [],
        "source_hypertable_chunk_counts": [["okg", "edge_facts", 12]],
        "restored_hypertable_chunk_counts": [["okg", "edge_facts", 12]],
    }


def test_restore_losing_hypertables_fails_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A restored database whose hypertable catalog came back hollow must
    fail verification even though the okg-namespace identity and the
    prestaged extension version both match."""
    import psycopg

    def connect(dsn, *args, **kwargs):
        restored = "okg_upgrade_verify_" in str(dsn)
        return _BackupVerifyConnection(
            owner="dep",
            timescaledb_version="2.28.2",
            # The disposable database restored the extension (prestaged)
            # but lost hypertable-ness: names survive nowhere.
            hypertables=[] if restored else [("okg", "edge_facts", 12)],
        )

    monkeypatch.setattr(psycopg, "connect", connect)

    result = _actual_verify_backup_restore(
        SimpleNamespace(
            dsn="postgresql://user@host/db",
            env={},
            options=SimpleNamespace(deployment="dep"),
            command_runner=lambda argv, env: CommandResult(
                0, "; archive listing\n", ""
            ),
        ),
        tmp_path / "backup.dump",
    )

    assert result["ok"] is False
    assert result["reason"] == "backup disposable restore identity check failed"
    assert result["schema_ok"] is False
    assert result["identity_checks"]["timescale_state_equal"] is False
    # extension equality alone would have passed: the prestage pins the
    # version, which is exactly why it cannot stand in for this check.
    assert result["identity_checks"]["extension_versions_equal"] is True
    assert "timescale_restore" not in result


_TIMESCALE_SNAPSHOT = {
    "timescaledb": {
        "extension_version": "2.28.2",
        "hypertables": [["okg", "edge_facts"], ["okg", "node_facts"]],
        "continuous_aggregates": [],
        "hypertable_chunk_counts": [["okg", "edge_facts", 12]],
    },
}


def _proven_timescale_block(**overrides):
    block = {
        "status": "proven",
        "extension_version": "2.28.2",
        "pre_post_restore_used": True,
        "hypertables_protected": [["okg", "edge_facts"], ["okg", "node_facts"]],
        "continuous_aggregates": [],
        "source_hypertable_chunk_counts": [["okg", "edge_facts", 12]],
        "restored_hypertable_chunk_counts": [["okg", "edge_facts", 12]],
    }
    block.update(overrides)
    return block


def test_timescale_receipt_check_fails_closed_without_proof() -> None:
    """A receipt with no TimescaleDB block (every receipt written before
    this change) stops covering a hypertable-bearing target, and the kernel
    names the failed check when it selects the safer mode."""
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.deployment_backup_policy import (
        FULL_RESTORE,
        REFUSE,
        BackupPolicyInput,
        PhysicalRollbackPolicyInput,
        RestoreDrillChecks,
        select_backup_verification,
        select_physical_rollback_policy,
    )

    ok, status = du._timescale_receipt_check(None, _TIMESCALE_SNAPSHOT, {})
    assert (ok, status) == (False, "unproven")

    # Matching proof from an executed restore covers the target.
    ok, status = du._timescale_receipt_check(
        _proven_timescale_block(), _TIMESCALE_SNAPSHOT, {}
    )
    assert (ok, status) == (True, "proven")

    # A plain-PostgreSQL target needs no procedure — real equality, not a
    # vacuous pass: the probed extension version is recorded as None.
    ok, status = du._timescale_receipt_check(
        None,
        {
            "timescaledb": {
                "extension_version": None,
                "hypertables": [],
                "continuous_aggregates": [],
                "hypertable_chunk_counts": [],
            }
        },
        {},
    )
    assert (ok, status) == (True, "absent")

    # A snapshot without the probed state never passes silently.
    ok, status = du._timescale_receipt_check(
        _proven_timescale_block(), {}, {}
    )
    assert (ok, status) == (False, "target_state_unreadable")

    # Kernel: the failed drill field selects the current full restore and
    # names itself.
    drill_values = {
        item.name: True for item in dataclasses.fields(RestoreDrillChecks)
    }
    drill_values["timescale_restore_proven_or_risk_admitted"] = False
    decision = select_backup_verification(BackupPolicyInput(
        policy_mode="restore_drill",
        release_channel="dev",
        readiness_profile="dogfood",
        deployment_class="dogfood_workspace",
        preflight_readable=True,
        current_archive_valid=True,
        destructive_confirmed=False,
        candidate_plan_stable=True,
        policy_stable=True,
        receipt=RestoreDrillChecks(**drill_values),
    ))
    assert decision.mode == FULL_RESTORE
    assert "receipt.timescale_restore_proven_or_risk_admitted" in (
        decision.reasons
    )

    # Kernel: the failed disaster-recovery field refuses the
    # no-physical-rollback admission and names itself.
    recovery = _fresh_recovery_checks(
        timescale_restore_proven_or_risk_admitted=False
    )
    physical = select_physical_rollback_policy(
        PhysicalRollbackPolicyInput(
            policy_mode="risk_admitted",
            release_channel="dev",
            readiness_profile="dogfood",
            deployment_class="dogfood_workspace",
            destructive_confirmed=False,
            effects=_closed_effect_checks(),
            recovery=recovery,
        )
    )
    assert physical.mode == REFUSE
    assert (
        "disaster_recovery.timescale_restore_proven_or_risk_admitted"
        in physical.reasons
    )


def test_hypertable_added_after_drill_invalidates_receipt() -> None:
    """Reuse equality is over stable identity: a hypertable created after
    the drill invalidates the proof, while drifting chunk counts (normal
    ingest) never do."""
    from okg.substrate import deployment_upgrade as du

    grown = {
        "timescaledb": {
            **_TIMESCALE_SNAPSHOT["timescaledb"],
            "hypertables": [
                ["okg", "edge_facts"],
                ["okg", "node_facts"],
                ["okg", "telemetry_events"],
            ],
        },
    }
    ok, status = du._timescale_receipt_check(
        _proven_timescale_block(), grown, {}
    )
    assert (ok, status) == (False, "unproven")

    # Chunk counts drift with every ingest cycle; they are recorded
    # evidence, never part of the reuse predicate.
    ingested = {
        "timescaledb": {
            **_TIMESCALE_SNAPSHOT["timescaledb"],
            "hypertable_chunk_counts": [["okg", "edge_facts", 9000]],
        },
    }
    ok, status = du._timescale_receipt_check(
        _proven_timescale_block(), ingested, {}
    )
    assert (ok, status) == (True, "proven")


def test_receipt_without_timescale_proof_stops_authorizing_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Full pipeline: a drill receipt whose TimescaleDB block was earned by
    a verified restore authorizes reuse on a hypertable-bearing target;
    stripping the block (the shape of every receipt written before this
    change) forces the next run back to a current full restore with the
    failed check and the unproven status named in the decision."""
    _patch_routine_backup(monkeypatch, tmp_path)
    timescale_state = {
        "extension_version": "2.28.2",
        "hypertables": [["okg", "edge_facts"]],
        "continuous_aggregates": [],
        "hypertable_chunk_counts": [["okg", "edge_facts", 12]],
    }
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._backup_compatibility_snapshot",
        lambda ctx, toolchain: {
            "target": {
                "deployment": ctx.options.deployment,
                "host": "host",
                "port": 5432,
                "database": "db",
            },
            "database_owner": {
                "deployment": ctx.options.deployment,
                "uuid": "test-owner",
                "schema_fingerprint": "test-schema",
            },
            "cluster_system_identifier": "test-cluster",
            "server_version": "170010",
            "extension_versions": [["timescaledb", "2.28.2"]],
            "timescaledb": timescale_state,
            "schema_migrations_digest": "sha256:test-schema-migrations",
            "schema_catalog_digest": "sha256:test-schema-catalog",
            "candidate_plan_digest": "sha256:test-plan",
            "archive_format": "postgresql-custom",
            "dump_tool_major": 17,
            "restore_tool_major": 17,
            "backup_tool_executable_identity": {
                "pg_dump": {"sha256": "dump"},
                "pg_restore": {"sha256": "restore"},
            },
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._verify_backup_restore",
        lambda ctx, path, **_: {
            "ok": True,
            "restore_verified": True,
            "schema_ok": True,
            "ownership_ok": True,
            "observed_restored_size_bytes": 70 * 1024**3,
            "timescale_restore": {
                "status": "proven",
                "extension_version": "2.28.2",
                "pre_post_restore_used": True,
                "hypertables_protected": [["okg", "edge_facts"]],
                "continuous_aggregates": [],
                "source_hypertable_chunk_counts": [["okg", "edge_facts", 12]],
                "restored_hypertable_chunk_counts": [
                    ["okg", "edge_facts", 12]
                ],
            },
        },
    )
    backup_dir = tmp_path / "backups"

    def run(name: str) -> dict:
        report = run_deployment_upgrade(
            UpgradeOptions(
                deployment="dep",
                state_file=tmp_path / f"{name}.json",
                backup_dir=backup_dir,
                readiness_profile="dogfood",
                stop_after="backup",
            ),
            command_runner=_routine_backup_runner,
        )
        return report["phases"][-1]

    first = run("first")
    assert first["verification_mode"] == "current_full_restore"

    second = run("second")
    assert second["verification_mode"] == "routine_restore_drill_reused"
    assert second["backup_policy_decision"]["timescale_restore_status"] == (
        "proven"
    )

    receipt_path = backup_dir / "dep-latest-restore-drill.json"
    receipt = json.loads(receipt_path.read_text())
    assert receipt["timescale_restore"]["hypertables_protected"] == [
        ["okg", "edge_facts"]
    ]
    del receipt["timescale_restore"]
    receipt_path.write_text(json.dumps(receipt))
    receipt_path.chmod(0o600)

    third = run("third")
    assert third["verification_mode"] == "current_full_restore"
    assert third["backup_policy_decision"]["timescale_restore_status"] == (
        "unproven"
    )
    assert (
        "receipt.timescale_restore_proven_or_risk_admitted"
        in third["backup_policy_decision"]["reasons"]
    )


def test_timescale_risk_admission_is_recorded_not_silent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """timescale_restore_risk_admitted=true admits an unproven receipt but
    reports risk_admitted (a provable proof still reports proven); the knob
    must be an exact boolean; and the admission never rewrites history — an
    unreadable target state still fails."""
    from okg.substrate import deployment_upgrade as du

    admitted = {"timescale_restore_risk_admitted": True}
    ok, status = du._timescale_receipt_check(None, _TIMESCALE_SNAPSHOT, admitted)
    assert (ok, status) == (True, "risk_admitted")

    ok, status = du._timescale_receipt_check(
        _proven_timescale_block(), _TIMESCALE_SNAPSHOT, admitted
    )
    assert (ok, status) == (True, "proven")

    ok, status = du._timescale_receipt_check(None, {}, admitted)
    assert (ok, status) == (False, "target_state_unreadable")

    # The declaration is validated as an exact boolean; "true" the string
    # must refuse, not coerce.
    monkeypatch.setattr(
        du,
        "_resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(
            deployment_dir=tmp_path,
            deployment_up={
                "backup": {
                    "verification_policy": "current_full_restore",
                    "restore_peak_factor": 1.25,
                    "restore_volume": {"kind": "docker_data_root"},
                    "timescale_restore_risk_admitted": "true",
                }
            },
        ),
    )
    with pytest.raises(du.SubstrateError, match="timescale_restore_risk_admitted"):
        du._backup_policy_config(
            SimpleNamespace(options=SimpleNamespace(deployment="dep"))
        )

    # The declared knob lands in the parsed policy so evidence collection
    # and reports see the same authority.
    monkeypatch.setattr(
        du,
        "_resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(
            deployment_dir=tmp_path,
            deployment_up={
                "backup": {
                    "verification_policy": "current_full_restore",
                    "restore_peak_factor": 1.25,
                    "restore_volume": {"kind": "docker_data_root"},
                    "timescale_restore_risk_admitted": True,
                }
            },
        ),
    )
    policy = du._backup_policy_config(
        SimpleNamespace(options=SimpleNamespace(deployment="dep"))
    )
    assert policy["timescale_restore_risk_admitted"] is True


@pytest.mark.postgres
def test_live_timescale_restore_roundtrip(
    tmp_path: Path,
    okg_workspace_isolated_migrated: str,
) -> None:
    """Real dump-and-restore round trip against a real TimescaleDB database.

    Positive: the production disposable-restore verification (which wraps the
    restore in timescaledb_pre_restore()/timescaledb_post_restore()) reports
    verified, records the protected hypertables, and a manual restore using
    the same procedure preserves row counts and the hypertable catalog.
    Negative: the same archive restored naively (no prestage, no pre/post)
    must not end in a state the verification would call verified — either
    pg_restore itself fails, or the restored TimescaleDB state does not
    match the source and timescale_state_equal is False.
    """
    import psycopg
    from psycopg import sql as psql
    from okg.substrate import deployment_upgrade as du

    dsn = okg_workspace_isolated_migrated
    info = psycopg.conninfo.conninfo_to_dict(dsn)
    maintenance = dict(info)
    maintenance["dbname"] = "postgres"
    maintenance_dsn = psycopg.conninfo.make_conninfo(**maintenance)
    positive_db = "okg_test_ts_roundtrip_pos"
    negative_db = "okg_test_ts_roundtrip_neg"
    archive = tmp_path / "roundtrip.dump"

    def scratch_dsn(dbname: str) -> str:
        scratch = dict(info)
        scratch["dbname"] = dbname
        return psycopg.conninfo.make_conninfo(**scratch)

    def drop_scratch(cur, dbname: str) -> None:
        cur.execute(
            psql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                psql.Identifier(dbname)
            )
        )

    try:
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            # The identity probe requires the deployment-owner singleton the
            # migrate fixture does not stamp.
            from okg.substrate.db.deployment_owner import stamp_db_owner

            stamp_db_owner(cur, "okg-workspace", command="test-roundtrip")
            cur.execute(
                "CREATE TABLE public.ts_roundtrip ("
                "ts timestamptz NOT NULL, val double precision NOT NULL)"
            )
            cur.execute(
                "SELECT create_hypertable("
                "'public.ts_roundtrip', 'ts', "
                "chunk_time_interval => interval '1 day')"
            )
            cur.execute(
                "INSERT INTO public.ts_roundtrip "
                "SELECT g, random() FROM generate_series("
                "now() - interval '9 days', now(), "
                "interval '30 minutes') AS g"
            )
            cur.execute("SELECT count(*) FROM public.ts_roundtrip")
            source_rows = cur.fetchone()[0]
            cur.execute(
                "SELECT num_chunks FROM timescaledb_information.hypertables "
                "WHERE hypertable_schema = 'public' "
                "AND hypertable_name = 'ts_roundtrip'"
            )
            source_chunks = cur.fetchone()[0]
            cur.execute(
                "SELECT extversion FROM pg_extension "
                "WHERE extname = 'timescaledb'"
            )
            timescale_version = cur.fetchone()[0]
        assert source_rows > 0
        assert source_chunks >= 9
        with psycopg.connect(dsn) as conn:
            source_identity = du._database_logical_identity(conn)
        assert ["public", "ts_roundtrip"] in (
            source_identity["timescaledb"]["hypertables"]
        )

        database, dump_env = du._libpq_command_target(dsn, dict(os.environ))
        dump = subprocess.run(
            [
                "pg_dump",
                "--format=custom",
                "--compress=1",
                "--file",
                str(archive),
                "--dbname",
                database,
            ],
            env=dump_env,
            capture_output=True,
            text=True,
            timeout=600,
        )
        assert dump.returncode == 0, dump.stderr

        # Positive, production path: the disposable-restore verification
        # executes the vendor procedure and must earn "proven". (The
        # hermetic autouse fixture stubs du._verify_backup_restore, so this
        # must call the real implementation.)
        result = _actual_verify_backup_restore(
            SimpleNamespace(
                dsn=dsn,
                env=dict(os.environ),
                options=SimpleNamespace(deployment="okg-workspace"),
                command_runner=du._default_command_runner,
                data={
                    "backup_policy_config": {
                        "max_parallel_restore_jobs": 1,
                        "progress_interval_seconds": 30,
                    }
                },
            ),
            archive,
        )
        assert result["ok"] is True, result
        assert result["restore_verified"] is True
        assert result["identity_checks"]["timescale_state_equal"] is True
        block = result["timescale_restore"]
        assert block["status"] == "proven"
        assert block["pre_post_restore_used"] is True
        assert ["public", "ts_roundtrip"] in block["hypertables_protected"]
        assert ["public", "ts_roundtrip", source_chunks] in (
            block["restored_hypertable_chunk_counts"]
        )

        # Positive, row-count proof: a manual restore with the same
        # procedure preserves every row and the hypertable catalog.
        listing = subprocess.run(
            ["pg_restore", "--list", str(archive)],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert listing.returncode == 0, listing.stderr
        toc = du._restore_toc_without_server_pinned_extensions(
            listing.stdout, timescaledb_prestaged=True
        )
        toc_path = tmp_path / "roundtrip.toc"
        toc_path.write_text(toc, encoding="utf-8")
        with psycopg.connect(
            maintenance_dsn, autocommit=True
        ) as conn, conn.cursor() as cur:
            drop_scratch(cur, positive_db)
            cur.execute(
                psql.SQL("CREATE DATABASE {}").format(
                    psql.Identifier(positive_db)
                )
            )
        with psycopg.connect(
            scratch_dsn(positive_db), autocommit=True
        ) as conn, conn.cursor() as cur:
            cur.execute(
                psql.SQL(
                    "CREATE EXTENSION IF NOT EXISTS timescaledb VERSION {}"
                ).format(psql.Literal(timescale_version))
            )
            cur.execute(
                "SELECT extversion FROM pg_extension "
                "WHERE extname = 'timescaledb'"
            )
            assert cur.fetchone()[0] == timescale_version
            cur.execute("SELECT timescaledb_pre_restore()")
        restore_database, restore_env = du._libpq_command_target(
            scratch_dsn(positive_db), dict(os.environ)
        )
        restored = subprocess.run(
            [
                "pg_restore",
                "--exit-on-error",
                "--use-list",
                str(toc_path),
                "--dbname",
                restore_database,
                str(archive),
            ],
            env=restore_env,
            capture_output=True,
            text=True,
            timeout=600,
        )
        assert restored.returncode == 0, restored.stderr
        with psycopg.connect(
            scratch_dsn(positive_db), autocommit=True
        ) as conn, conn.cursor() as cur:
            cur.execute("SELECT timescaledb_post_restore()")
            cur.execute("SELECT count(*) FROM public.ts_roundtrip")
            assert cur.fetchone()[0] == source_rows
            cur.execute(
                "SELECT num_chunks FROM timescaledb_information.hypertables "
                "WHERE hypertable_schema = 'public' "
                "AND hypertable_name = 'ts_roundtrip'"
            )
            assert cur.fetchone()[0] == source_chunks

        # Negative: a restore without the vendor procedure whose extension
        # catalog payload did not apply — the documented TimescaleDB failure
        # shape (the July incident's COPY misalignment, and any non-superuser
        # restore whose catalog writes are rejected). Note the caveat this
        # test does NOT rely on: a naive same-version restore run AS
        # SUPERUSER can coincidentally succeed, because superuser bypasses
        # the catalog-write protection that timescaledb_pre_restore() exists
        # to lift. The enforced invariant is outcome-based: when the
        # hypertable catalog does not survive, the restored parent tables
        # silently lose every row, the okg-namespace identity digests still
        # match, and only timescale_state_equal refuses the verification.
        hollow_toc = "\n".join(
            line
            for line in listing.stdout.splitlines()
            if " EXTENSION - timescaledb" not in line
            and " COMMENT - EXTENSION timescaledb" not in line
            and " TABLE DATA _timescaledb_catalog " not in line
        ) + "\n"
        hollow_toc_path = tmp_path / "hollow.toc"
        hollow_toc_path.write_text(hollow_toc, encoding="utf-8")
        with psycopg.connect(
            maintenance_dsn, autocommit=True
        ) as conn, conn.cursor() as cur:
            drop_scratch(cur, negative_db)
            cur.execute(
                psql.SQL("CREATE DATABASE {}").format(
                    psql.Identifier(negative_db)
                )
            )
        naive_database, naive_env = du._libpq_command_target(
            scratch_dsn(negative_db), dict(os.environ)
        )
        naive = subprocess.run(
            [
                "pg_restore",
                "--use-list",
                str(hollow_toc_path),
                "--dbname",
                naive_database,
                str(archive),
            ],
            env=naive_env,
            capture_output=True,
            text=True,
            timeout=600,
        )
        del naive  # a hollow restore may or may not report errors; the
        # verification must refuse on the observed state either way.
        with psycopg.connect(scratch_dsn(negative_db)) as conn:
            naive_identity = du._database_logical_identity(conn)
        with psycopg.connect(
            scratch_dsn(negative_db)
        ) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('public.ts_roundtrip') IS NOT NULL"
            )
            table_survived = cur.fetchone()[0]
        # The silent-loss shape: the parent table exists and may even still
        # read historical rows through plain-PostgreSQL inheritance, but
        # hypertable-ness is gone — chunk management, compression,
        # retention, and insert routing are all lost, and nothing in the
        # okg-namespace identity digests can see it. (In this constructed
        # shape extension equality also fails; the hermetic
        # test_restore_losing_hypertables_fails_verification is the proof
        # that timescale_state_equal catches the loss even when extension
        # equality passes.)
        assert table_survived is True
        assert ["public", "ts_roundtrip"] not in (
            naive_identity["timescaledb"]["hypertables"]
        )
        # And the new check is the one that refuses it.
        assert (
            naive_identity["timescaledb"]["hypertables"]
            != source_identity["timescaledb"]["hypertables"]
        )
    finally:
        with psycopg.connect(
            maintenance_dsn, autocommit=True
        ) as conn, conn.cursor() as cur:
            drop_scratch(cur, positive_db)
            drop_scratch(cur, negative_db)
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS public.ts_roundtrip")


def test_schema_catalog_digest_covers_row_security_policy() -> None:
    from okg.substrate import deployment_upgrade as du

    class PolicyCursor(_BackupVerifyCursor):
        def __init__(self, policy_expression: str) -> None:
            super().__init__()
            self.policy_expression = policy_expression

        def fetchall(self):
            if "FROM pg_policy p" in self.last_query:
                return [[
                    "graph_nodes",
                    "tenant_policy",
                    "r",
                    True,
                    self.policy_expression,
                    None,
                    ["app_rw"],
                ]]
            return super().fetchall()

    class PolicyConnection:
        def __init__(self, policy_expression: str) -> None:
            self.policy_expression = policy_expression

        def cursor(self):
            return PolicyCursor(self.policy_expression)

    first = du._database_logical_identity(PolicyConnection("tenant_id = 1"))
    second = du._database_logical_identity(PolicyConnection("tenant_id = 2"))

    assert first["schema_catalog_digest"] != second["schema_catalog_digest"]


@pytest.mark.postgres
def test_database_function_identity_normalizes_defaults_and_rejects_acl_drift(
    okg_workspace_isolated_runtime: str,
) -> None:
    """Exercise NULL, explicit-default, and restricted ACLs in PostgreSQL."""
    import psycopg
    from psycopg import sql
    from uuid import uuid4
    from okg.substrate import deployment_upgrade as du

    suffix = uuid4().hex[:12]
    function_name = f"_backup_restore_acl_{suffix}"
    owner_name = f"okg_backup_acl_{suffix}"

    def target_row(cur) -> list[object]:
        return next(
            row
            for row in du._database_function_identity(cur)
            if row[0] == function_name
        )

    with psycopg.connect(
        okg_workspace_isolated_runtime,
        autocommit=True,
    ) as conn, conn.cursor() as cur:
        identifier = sql.Identifier(function_name)
        owner = sql.Identifier(owner_name)
        cur.execute(sql.SQL("CREATE ROLE {}").format(owner))
        try:
            cur.execute(
                sql.SQL("GRANT USAGE, CREATE ON SCHEMA okg TO {}").format(owner)
            )
            cur.execute(sql.SQL("SET ROLE {}").format(owner))
            cur.execute(
                sql.SQL(
                    "CREATE FUNCTION okg.{}() RETURNS integer LANGUAGE sql AS "
                    "'SELECT 1'"
                ).format(identifier)
            )
            cur.execute("RESET ROLE")
            cur.execute(
                "SELECT p.proacl IS NULL FROM pg_proc p "
                "JOIN pg_namespace n ON n.oid = p.pronamespace "
                "WHERE n.nspname = 'okg' AND p.proname = %s",
                (function_name,),
            )
            assert cur.fetchone() == (True,)
            implicit_default = target_row(cur)

            cur.execute(
                sql.SQL("REVOKE EXECUTE ON FUNCTION okg.{}() FROM PUBLIC").format(
                    identifier
                )
            )
            cur.execute(
                sql.SQL("GRANT EXECUTE ON FUNCTION okg.{}() TO PUBLIC").format(
                    identifier
                )
            )
            cur.execute(
                "SELECT p.proacl IS NOT NULL FROM pg_proc p "
                "JOIN pg_namespace n ON n.oid = p.pronamespace "
                "WHERE n.nspname = 'okg' AND p.proname = %s",
                (function_name,),
            )
            assert cur.fetchone() == (True,)
            explicit_default = target_row(cur)
            assert explicit_default == implicit_default

            cur.execute(
                sql.SQL("REVOKE EXECUTE ON FUNCTION okg.{}() FROM PUBLIC").format(
                    identifier
                )
            )
            restricted = target_row(cur)
            assert restricted != implicit_default
        finally:
            cur.execute("RESET ROLE")
            cur.execute(sql.SQL("DROP OWNED BY {}").format(owner))
            cur.execute(sql.SQL("DROP ROLE {}").format(owner))


def test_backup_disposable_database_cleanup_failure_blocks_migration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    calls = 0

    def connect(*args, **kwargs):
        nonlocal calls
        calls += 1
        # Connects on the no-timescaledb path: (1) source extension
        # probe, (2) maintenance CREATE DATABASE, (3) disposable
        # ephemeral-marker stamp, (4) disposable identity checks,
        # (5) maintenance cleanup.
        if calls == 5:
            raise RuntimeError("induced cleanup failure")
        return _BackupVerifyConnection(owner="dep")

    monkeypatch.setattr(psycopg, "connect", connect)
    result = _actual_verify_backup_restore(
        SimpleNamespace(
            dsn="postgresql://user:secret@host/db",
            env={},
            options=SimpleNamespace(deployment="dep"),
            command_runner=lambda argv, env: CommandResult(0, "; archive listing\n", ""),
        ),
        tmp_path / "backup.dump",
    )

    assert result["ok"] is False
    assert result["reason"] == "backup verification cleanup failed"
    assert result["cleanup"]["disposable_database_dropped"] is False


def test_backup_restore_interrupt_still_drops_disposable_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    queries: list[str] = []
    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *args, **kwargs: _BackupVerifyConnection(queries=queries),
    )

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if argv[:2] == ["pg_restore", "--list"]:
            return CommandResult(0, "; archive listing\n", "")
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        _actual_verify_backup_restore(
            SimpleNamespace(
                dsn="postgresql://user:secret@host/db",
                env={},
                options=SimpleNamespace(deployment="dep"),
                command_runner=runner,
            ),
            tmp_path / "backup.dump",
        )

    assert any("DROP DATABASE" in query for query in queries)


def test_restore_toc_filter_drops_server_pinned_extension_entries() -> None:
    from okg.substrate.deployment_upgrade import (
        _restore_toc_without_server_pinned_extensions,
    )

    listing = "\n".join([
        ";",
        "; Archive created at 2026-07-14 19:38:00 UTC",
        "2; 3079 16385 EXTENSION - timescaledb ",
        "2647; 0 0 COMMENT - EXTENSION timescaledb ",
        "3; 3080 16400 EXTENSION - pg_cron ",
        "2648; 0 0 COMMENT - EXTENSION pg_cron ",
        "219; 1259 17010 TABLE DATA cron job postgres",
        "220; 1259 17011 SEQUENCE SET cron jobid_seq postgres",
        "223; 3080 17014 EXTENSION - pg_cron_extra",
        "224; 0 0 COMMENT - EXTENSION pg_cron_extra",
        "225; 1259 17015 TABLE DATA cron_archive job postgres",
        "285; 1259 17016 TABLE public okg_ephemeral_database postgres",
        "6736; 0 17016 TABLE DATA public okg_ephemeral_database postgres",
        "5886; 2606 17017 CONSTRAINT public okg_ephemeral_database "
        + "okg_ephemeral_database_pkey postgres",
        "286; 1259 17018 TABLE public okg_ephemeral_database_archive postgres",
        "221; 1259 17012 TABLE DATA _timescaledb_catalog dimension_slice postgres",
        "222; 1259 17013 TABLE DATA okg node_facts postgres",
    ])

    filtered = _restore_toc_without_server_pinned_extensions(
        listing, timescaledb_prestaged=True
    )
    filtered_lines = set(filtered.splitlines())
    assert "EXTENSION - timescaledb" not in filtered
    assert "COMMENT - EXTENSION timescaledb" not in filtered
    assert "3; 3080 16400 EXTENSION - pg_cron " not in filtered_lines
    assert "2648; 0 0 COMMENT - EXTENSION pg_cron " not in filtered_lines
    assert "TABLE DATA cron job" not in filtered
    assert "SEQUENCE SET cron jobid_seq" not in filtered
    assert " public okg_ephemeral_database " not in filtered
    # Anchored names only: similarly named extensions/schemas are ordinary
    # archive content and must not be dropped by the disposable-restore filter.
    assert "EXTENSION - pg_cron_extra" in filtered
    assert "COMMENT - EXTENSION pg_cron_extra" in filtered
    assert "TABLE DATA cron_archive job" in filtered
    assert "okg_ephemeral_database_archive" in filtered
    # Timescale catalog config data restores under pre_restore mode and
    # MUST be kept — dropping it would verify a hollow database.
    assert "TABLE DATA _timescaledb_catalog dimension_slice" in filtered
    assert "TABLE DATA okg node_facts" in filtered

    # Without prestaging, the dump's own timescaledb creation stays.
    unstaged = _restore_toc_without_server_pinned_extensions(
        listing, timescaledb_prestaged=False
    )
    assert "EXTENSION - timescaledb" in unstaged
    assert "3; 3080 16400 EXTENSION - pg_cron " not in unstaged.splitlines()


class _RecordingBackupVerifyCursor(_BackupVerifyCursor):
    def __init__(self, log: list[str], **kwargs) -> None:
        super().__init__(**kwargs)
        self._log = log

    def execute(self, query, params=None) -> None:
        super().execute(query, params)
        self._log.append(str(query))

    def fetchone(self):
        if "pg_extension" in self.last_query:
            return ("2.17.2",)
        return super().fetchone()


class _RecordingBackupVerifyConnection(_BackupVerifyConnection):
    def __init__(self, log: list[str], **kwargs) -> None:
        super().__init__(**kwargs)
        self._log = log

    def cursor(self) -> _RecordingBackupVerifyCursor:
        return _RecordingBackupVerifyCursor(self._log, owner=self.owner)


def test_backup_disposable_restore_prestages_timescaledb(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    executed: list[str] = []
    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *args, **kwargs: _RecordingBackupVerifyConnection(executed, owner="dep"),
    )
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        commands.append(argv)
        if argv[:2] == ["pg_restore", "--list"]:
            return CommandResult(
                0,
                "2; 3079 16385 EXTENSION - timescaledb\n"
                "221; 1259 17012 TABLE DATA _timescaledb_catalog dimension_slice postgres\n",
                "",
            )
        return CommandResult(0, "", "")

    result = _actual_verify_backup_restore(
        SimpleNamespace(
            dsn="postgresql://user:secret@host/db",
            env={},
            options=SimpleNamespace(deployment="dep"),
            command_runner=runner,
        ),
        tmp_path / "backup.dump",
    )

    assert result["ok"] is True
    joined = "\n".join(executed)
    # The fake logs psycopg Composed reprs, not rendered SQL.
    assert "CREATE EXTENSION IF NOT EXISTS timescaledb VERSION" in joined
    assert "2.17.2" in joined
    pre = next(i for i, q in enumerate(executed) if "timescaledb_pre_restore" in q)
    post = next(i for i, q in enumerate(executed) if "timescaledb_post_restore" in q)
    assert pre < post
    restore = next(argv for argv in commands if "--use-list" in argv)
    toc_text = Path(restore[restore.index("--use-list") + 1]).read_text() \
        if Path(restore[restore.index("--use-list") + 1]).exists() else ""
    # The TOC file is unlinked after the run; assert via the filter's
    # contract instead when it is already gone.
    if toc_text:
        assert "EXTENSION - timescaledb" not in toc_text
        assert "dimension_slice" in toc_text


def test_deployment_smoke_proof_accepts_real_updates_but_not_zero_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.publish_proof import assert_publish_proof

    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count",
        lambda dsn, **_kwargs: (0, {}),
    )
    updated = {
        "status": "published",
        "generation_id": "gen-updated",
        "nodes_added": 0,
        "edges_added": 0,
        "nodes_updated": 12,
        "edges_updated": 19,
        "nodes_retracted": 0,
        "edges_retracted": 0,
        "timing_details": _healthy_lock_timing_details(),
    }

    strict = assert_publish_proof(updated, dsn="postgres://unused")
    smoke = assert_publish_proof(
        updated,
        dsn="postgres://unused",
        allow_updated_rows=True,
    )
    assert strict["ok"] is False
    assert smoke["ok"] is True
    assert smoke["nodes_changed"] == 12
    assert smoke["edges_changed"] == 19

    zero = dict(updated, nodes_updated=0, edges_updated=0)
    refused = assert_publish_proof(
        zero,
        dsn="postgres://unused",
        allow_updated_rows=True,
    )
    assert refused["ok"] is False
    assert "did not change live" in refused["reason"]

    for payload in (
        {
            "status": "published",
            "generation_id": "gen-added",
            "nodes_added": 2,
            "edges_added": 3,
            "timing_details": _healthy_lock_timing_details(),
        },
        {
            "status": "published",
            "generation_id": "gen-retracted",
            "nodes_retracted": 2,
            "edges_retracted": 3,
            "timing_details": _healthy_lock_timing_details(),
        },
        {
            "publishes": [
                {
                    "status": "published",
                    "generation_id": "gen-nested",
                    "nodes_updated": 2,
                    "edges_updated": 3,
                    "timing_details": _healthy_lock_timing_details(),
                }
            ]
        },
    ):
        assert assert_publish_proof(
            payload,
            dsn="postgres://unused",
            allow_updated_rows=True,
        )["ok"] is True

    malformed = dict(updated, nodes_updated="not-a-count")
    assert assert_publish_proof(
        malformed,
        dsn="postgres://unused",
        allow_updated_rows=True,
    )["ok"] is False
    missing_generation = dict(updated)
    missing_generation.pop("generation_id")
    assert assert_publish_proof(
        missing_generation,
        dsn="postgres://unused",
        allow_updated_rows=True,
    )["ok"] is False


def test_deployment_smoke_proof_uses_readiness_runtime_partition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.deployment_upgrade import _smoke_publish_proof
    from okg.substrate.publish_proof import assert_publish_proof
    from okg.substrate.runtime_queue_health import (
        DEPLOYMENT_READINESS_DELEGATED_RUNTIME_WORKFLOW_NAMES,
    )

    observed: list[set[str] | None] = []

    def fake_dlq_count(
        _dsn: str,
        *,
        ignored_workflow_names: set[str] | None = None,
    ) -> tuple[int, dict[str, int]]:
        observed.append(ignored_workflow_names)
        return 0, {}

    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count",
        fake_dlq_count,
    )
    ctx = SimpleNamespace(dsn="postgres://unused")

    published = _smoke_publish_proof(ctx, _published("gen-smoke"))
    no_source_work = _smoke_publish_proof(
        ctx,
        {
            "ordered": ["pact"],
            "results": [{"name": "pact", "status": "OK"}],
            "publishes": [{
                "status": "skipped",
                "publish_skipped": True,
                "reason": "no_source_work",
            }],
        },
    )
    generic = assert_publish_proof(
        _published("gen-generic"),
        dsn="postgres://unused",
    )

    expected = set(
        DEPLOYMENT_READINESS_DELEGATED_RUNTIME_WORKFLOW_NAMES
    )
    assert published["ok"] is True
    assert no_source_work["ok"] is True
    assert generic["ok"] is True
    assert observed == [expected, expected, None]


# --- caught-up authoritative-replay proof-of-life (bound-deployment-backup-latency)


def _real_zero_output_verdict(
    source: str,
    *,
    reason: str = "authoritative_replay",
    completed_scope: bool = True,
    health: str = "ok",
    rejected_fact_count: int = 0,
) -> dict:
    """Build a real per-source terminal admission verdict.

    Uses the production ``_zero_output_admission_verdict`` so these gate tests
    are coupled to the actual verdict field names/values the ingest layer
    surfaces on OK result rows — a rename of ``terminal_state_authorized`` /
    ``completed_scope`` / ``reason`` breaks them rather than passing vacuously.
    """
    from okg.substrate.ingest.service import _zero_output_admission_verdict

    if reason == "authoritative_empty_snapshot":
        next_cursor: dict = {"record_set": {"records": {}}}
        run_mode = "scope_complete"
    elif reason == "unchanged":
        next_cursor = {}
        run_mode = "no_change"
    else:
        # authoritative_replay (non-empty declared record set, zero appended) and
        # the rejected/partial/degraded variants all start from a non-empty set.
        next_cursor = {"record_set": {"records": {"r1": {"node_id": "n1"}}}}
        run_mode = "scope_complete"
    return _zero_output_admission_verdict(
        source=source,
        run_id="run-1",
        admission_mode="fast_track",
        completed_scope=completed_scope,
        source_health_status=health,
        run_mode=run_mode,
        next_cursor=next_cursor,
        rejected_fact_count=rejected_fact_count,
    )


def _caught_up_ok_row(
    source: str, *, admission_status: str = "accepted", **verdict_kwargs
) -> dict:
    return {
        "name": source,
        "status": "OK",
        "nodes_appended": 0,
        "edges_appended": 0,
        "retract_n": 0,
        "retract_e": 0,
        "admission_status": admission_status,
        "admission_verdict": _real_zero_output_verdict(source, **verdict_kwargs),
    }


def _caught_up_payload(rows: list[dict], *, status: str = "published") -> dict:
    """A committed head generation whose live-row differential is empty."""
    return {
        "ordered": [row["name"] for row in rows],
        "results": rows,
        "publishes": [
            {
                "label": "commit",
                "status": status,
                "generation_id": "gen-head",
                "nodes_added": 0,
                "edges_added": 0,
                "nodes_updated": 0,
                "edges_updated": 0,
                "nodes_retracted": 0,
                "edges_retracted": 0,
                "timing_details": _healthy_lock_timing_details(),
            }
        ],
    }


def test_smoke_proof_accepts_caught_up_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.publish_proof import assert_publish_proof

    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count", lambda dsn, **_kwargs: (0, {})
    )
    for reason in (
        "authoritative_replay",
        "authoritative_empty_snapshot",
        "unchanged",
    ):
        payload = _caught_up_payload([_caught_up_ok_row("svc.pact", reason=reason)])
        # Sanity: the real producer authorized this terminal state.
        policy = payload["results"][0]["admission_verdict"]["producer_policy"]
        assert policy["terminal_state_authorized"] is True, reason
        assert policy["reason"] == reason

        verdict = assert_publish_proof(
            payload,
            dsn="postgres://unused",
            allow_updated_rows=True,
            allow_no_source_work=True,
            allow_caught_up_replay=True,
        )
        assert verdict["ok"] is True, (reason, verdict)
        assert verdict["caught_up_replay"] is True
        assert verdict["generation_id"] == "gen-head"
        assert verdict["nodes_changed"] == 0
        assert verdict["edges_changed"] == 0


def test_caught_up_replay_refused_without_optin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.publish_proof import assert_publish_proof

    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count", lambda dsn, **_kwargs: (0, {})
    )
    payload = _caught_up_payload([_caught_up_ok_row("svc.pact")])
    # Without the smoke phase's explicit opt-in the zero-delta publish still fails
    # (bootstrap-verify / provision stay strict).
    verdict = assert_publish_proof(
        payload, dsn="postgres://unused", allow_updated_rows=True
    )
    assert verdict["ok"] is False
    assert "did not change live" in verdict["reason"]


def test_caught_up_replay_refuses_wedged_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.publish_proof import assert_publish_proof

    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count", lambda dsn, **_kwargs: (0, {})
    )

    def refused(rows: list[dict], *, ordered=None) -> dict:
        payload = _caught_up_payload(rows)
        if ordered is not None:
            payload["ordered"] = ordered
        return assert_publish_proof(
            payload,
            dsn="postgres://unused",
            allow_updated_rows=True,
            allow_no_source_work=True,
            allow_caught_up_replay=True,
        )

    # A source that partial-scoped, dropped facts, or is degraded is NOT an
    # authorized terminal state — the real producer refuses to authorize it, so
    # the caught-up gate keeps the strict failure. This is the wedge case.
    for kwargs in (
        {"completed_scope": False},        # -> reason "partial"
        {"rejected_fact_count": 1},        # -> reason "facts_rejected"
        {"health": "failed"},              # -> reason "failed_or_degraded"
    ):
        row = _caught_up_ok_row("svc.pact", **kwargs)
        assert row["admission_verdict"]["producer_policy"][
            "terminal_state_authorized"
        ] is False, kwargs
        assert refused([row])["ok"] is False, kwargs

    # A FAILED source is never a caught-up signal even if a stale verdict says so.
    failed_row = _caught_up_ok_row("svc.pact")
    failed_row["status"] = "FAIL"
    assert refused([failed_row])["ok"] is False

    # A review-mode source can reach an AUTHORIZED terminal verdict while its
    # envelope is only "pending" (submitted for review, not published). It is not
    # "current", so the caught-up gate refuses it despite the authorized verdict.
    pending = _caught_up_ok_row("svc.pact", admission_status="pending")
    assert pending["admission_verdict"]["producer_policy"][
        "terminal_state_authorized"
    ] is True
    assert refused([pending])["ok"] is False

    # A missing / unreadable verdict fails closed rather than assuming currency.
    no_verdict = _caught_up_ok_row("svc.pact")
    no_verdict["admission_verdict"] = {}
    assert refused([no_verdict])["ok"] is False
    del no_verdict["admission_verdict"]
    assert refused([no_verdict])["ok"] is False

    # A hand-forged reason with terminal_state_authorized False is rejected even
    # when the reason string looks authoritative.
    forged = _caught_up_ok_row("svc.pact")
    forged["admission_verdict"]["producer_policy"][
        "terminal_state_authorized"
    ] = False
    assert refused([forged])["ok"] is False

    # A requested source absent from results (empty / mismatched ordered) fails.
    assert refused([_caught_up_ok_row("svc.pact")], ordered=[])["ok"] is False
    assert refused(
        [_caught_up_ok_row("svc.pact")], ordered=["svc.pact", "svc.other"]
    )["ok"] is False


def test_caught_up_replay_still_enforces_queue_and_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.publish_proof import assert_publish_proof

    # A blocked generation fails before the delta check even with the opt-in.
    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count", lambda dsn, **_kwargs: (0, {})
    )
    blocked = _caught_up_payload(
        [_caught_up_ok_row("svc.pact")], status="blocked"
    )
    assert assert_publish_proof(
        blocked,
        dsn="postgres://unused",
        allow_updated_rows=True,
        allow_caught_up_replay=True,
    )["ok"] is False

    # A caught-up no-op over a poisoned dead-letter queue still fails.
    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count", lambda dsn, **_kwargs: (3, {})
    )
    poisoned = assert_publish_proof(
        _caught_up_payload([_caught_up_ok_row("svc.pact")]),
        dsn="postgres://unused",
        allow_updated_rows=True,
        allow_caught_up_replay=True,
    )
    assert poisoned["ok"] is False
    assert "dead-letter" in poisoned["reason"]


def test_caught_up_replay_multi_source_requires_all_authorized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.publish_proof import assert_publish_proof

    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count", lambda dsn, **_kwargs: (0, {})
    )
    both_ok = _caught_up_payload([
        _caught_up_ok_row("svc.pact"),
        _caught_up_ok_row("svc.sessions", reason="authoritative_empty_snapshot"),
    ])
    assert assert_publish_proof(
        both_ok,
        dsn="postgres://unused",
        allow_updated_rows=True,
        allow_caught_up_replay=True,
    )["ok"] is True

    one_wedged = _caught_up_payload([
        _caught_up_ok_row("svc.pact"),
        _caught_up_ok_row("svc.sessions", completed_scope=False),
    ])
    assert assert_publish_proof(
        one_wedged,
        dsn="postgres://unused",
        allow_updated_rows=True,
        allow_caught_up_replay=True,
    )["ok"] is False


def _no_source_work_skip_payload() -> dict:
    """A caught-up smoke whose commit publish was SKIPPED (no generation)."""
    return {
        "ordered": ["svc.pact"],
        "results": [_caught_up_ok_row("svc.pact")],
        "publishes": [
            {
                "label": "commit",
                "status": "skipped",
                "publish_skipped": True,
                "reason": "no_source_work",
            }
        ],
    }


def test_is_caught_up_noop_classifies_skip_and_replay_but_not_wedge() -> None:
    from okg.substrate.publish_proof import is_caught_up_noop

    # A validated no_source_work skip (no generation minted).
    assert is_caught_up_noop(_no_source_work_skip_payload()) is True
    # A committed zero-delta authoritative replay.
    assert is_caught_up_noop(
        _caught_up_payload([_caught_up_ok_row("svc.pact")])
    ) is True
    # A wedged source is not caught up; an unreadable payload is not either.
    assert is_caught_up_noop(
        _caught_up_payload([_caught_up_ok_row("svc.pact", completed_scope=False)])
    ) is False
    assert is_caught_up_noop({}) is False


def test_bound_smoke_generation_binds_head_on_caught_up_noop() -> None:
    from okg.substrate import deployment_upgrade as du

    caught_up = SimpleNamespace(
        data={"smoke_publish": _no_source_work_skip_payload()}
    )
    # A caught-up no-op that minted no generation binds to the readiness head,
    # so readiness / MCP / budget align on the head the no-op confirmed current.
    assert du._bound_smoke_generation(caught_up, None, "gen-head") == "gen-head"
    # A committed smoke generation is used verbatim (no binding).
    assert du._bound_smoke_generation(caught_up, "gen-x", "gen-head") == "gen-x"
    # No head resolved -> stays None (fail closed, readiness still refuses).
    assert du._bound_smoke_generation(caught_up, None, None) is None
    # A wedged smoke does NOT bind to a head even when readiness resolved one.
    wedged = SimpleNamespace(
        data={
            "smoke_publish": _caught_up_payload(
                [_caught_up_ok_row("svc.pact", completed_scope=False)]
            )
        }
    )
    assert du._bound_smoke_generation(wedged, None, "gen-head") is None


def test_upgrade_state_identity_invalidates_completed_state_for_new_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    deployment_dir = tmp_path / "dep-root"
    deployment_dir.mkdir()
    (deployment_dir / "deployment.yaml").write_text("name: dep\n")
    monkeypatch.setattr(
        du,
        "_resolve_target",
        lambda options: {
            "dsn": "postgres://user@host/db",
            "target": {"database": "db", "identity": "ok"},
        },
    )
    monkeypatch.setattr(
        du,
        "_resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(
            deployment_dir=deployment_dir,
            release={"channel": "dev"},
        ),
    )
    revision = ["revision-old"]
    monkeypatch.setattr(du, "_upgrade_state_revision", lambda ctx: revision[0])
    state_file = tmp_path / "up-state.json"
    options = UpgradeOptions(deployment="dep", state_file=state_file)
    ctx = du._build_context(options, command_runner=lambda argv, env: _ok())
    old_key = du._target_state_key(ctx)
    state_file.write_text(json.dumps({
        "deployment": "dep",
        "profile": "prod",
        "target_key": old_key,
            "plan_only": False,
            "lifecycle_contract_digest": CANONICAL_CONTRACT.digest,
            "lifecycle_contract_version": CANONICAL_CONTRACT.contract_version,
            "completed": True,
        "completed_phases": list(du.PHASES),
        "phase_results": [],
    }))

    same_revision_fresh = du._load_or_initialize_state(ctx)
    assert same_revision_fresh["completed"] is False
    assert same_revision_fresh["completed_phases"] == []

    revision[0] = "revision-new"
    fresh = du._load_or_initialize_state(ctx)
    assert fresh["completed"] is False
    assert fresh["completed_phases"] == []
    assert fresh["target_key"]["code_revision"] == "revision-new"

    changed_policy_ctx = du._build_context(
        UpgradeOptions(
            deployment="dep",
            state_file=state_file,
            readiness_profile="ci",
        ),
        command_runner=lambda argv, env: _ok(),
    )
    assert (
        du._target_state_key(changed_policy_ctx)["lifecycle_policy"]
        == fresh["target_key"]["lifecycle_policy"]
    )
    assert changed_policy_ctx.options.readiness_profile == "ci"


def test_resume_invalidates_only_phases_whose_operator_inputs_changed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    state_file = tmp_path / "state.json"
    target_key = {"identity": "same-target"}
    monkeypatch.setattr(du, "_target_state_key", lambda ctx: target_key)

    def write_state() -> None:
        state_file.write_text(json.dumps({
            "deployment": "dep",
            "profile": "prod",
            "target_key": target_key,
            "plan_only": False,
            "lifecycle_contract_digest": du.CANONICAL_CONTRACT.digest,
            "lifecycle_contract_version": (
                du.CANONICAL_CONTRACT.contract_version
            ),
            "completed": False,
            "completed_phases": list(du.PHASES),
            "phase_results": [],
            "phase_inputs": {
                "smoke_publish": {"include": []},
                "readiness": {"profile": "dogfood"},
            },
        }))

    write_state()
    smoke_changed = SimpleNamespace(
        options=UpgradeOptions(
            deployment="dep",
            include=("pact",),
            readiness_profile="dogfood",
            state_file=state_file,
        ),
        state_file=state_file,
        data={},
    )
    smoke_state = du._load_or_initialize_state(smoke_changed)
    assert smoke_state["failed_phase"] == "smoke_publish"
    assert "smoke_publish" not in smoke_state["completed_phases"]
    assert "readiness" not in smoke_state["completed_phases"]
    assert "backup" in smoke_state["completed_phases"]

    write_state()
    readiness_changed = SimpleNamespace(
        options=UpgradeOptions(
            deployment="dep",
            readiness_profile="ci",
            state_file=state_file,
        ),
        state_file=state_file,
        data={},
    )
    readiness_state = du._load_or_initialize_state(readiness_changed)
    assert readiness_state["failed_phase"] == "readiness"
    assert "smoke_publish" in readiness_state["completed_phases"]
    assert "readiness" not in readiness_state["completed_phases"]


def test_upgrade_resume_command_preserves_effective_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    options = UpgradeOptions(
        deployment="dep",
        profile="dev",
        dsn="postgresql://user:secret@host/okg_ci_dev",
        include=("pact",),
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path / "backups",
        lifecycle_plan=tmp_path / "plan.json",
        verified_backup_state=tmp_path / "verified-backup.json",
        verified_backup_source_revision="revision-before-checkout",
        readiness_profile="ci",
        apply_services=False,
        sync_code=False,
        sync_deps=False,
        resume=False,
    )
    monkeypatch.setenv(
        "OKG_CI_DEV_DSN",
        "postgresql://user:secret@host/okg_ci_dev",
    )
    command = du._next_command(SimpleNamespace(options=options))

    # renamed-flag compat (decision 310): a pinned pre-rename checkout only
    # parses the old spellings, so the recovery command keeps them for one
    # release and silences the warning new code would print.
    assert command.startswith("OKG_RENAMED_FLAG_WARNINGS=0 ")
    assert "--target" not in command and "--checks" not in command
    for expected in (
        "--profile dev",
        "--readiness-profile ci",
        "--no-apply-services",
        "--no-sync-code",
        "--no-sync-deps",
        "--no-resume",
        "--include pact",
        f"--lifecycle-plan {tmp_path / 'plan.json'}",
        f"--verified-backup-state {tmp_path / 'verified-backup.json'}",
        "--verified-backup-source-revision revision-before-checkout",
    ):
        assert expected in command
    assert "secret" not in command
    assert "***" not in command
    assert '--dsn "${OKG_CI_DEV_DSN:?' in command


def test_backup_verify_cli_dispatch_preserves_resume_and_reset_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.cli import deployment as cli_deployment

    captured: list[UpgradeOptions] = []
    emitted: list[dict] = []
    monkeypatch.setattr(
        du,
        "run_deployment_backup_verify",
        lambda options: captured.append(options) or {"ok": True},
    )
    monkeypatch.setattr(
        cli_deployment,
        "_emit_ok",
        lambda payload, as_json: emitted.append(payload),
    )
    result = cli_deployment._cmd_deployment_backup(
        SimpleNamespace(
            backup_subcmd="verify",
            deployment="dep",
            profile="prod",
            dsn=None,
            state_file=str(tmp_path / "verify-state.json"),
            backup_dir=str(tmp_path / "backups"),
            backup_keep=2,
            resume=False,
            reset_state=True,
        ),
        as_json=True,
    )

    assert result == 0
    assert emitted == [{"ok": True}]
    assert captured[0].resume is False
    assert captured[0].reset_state is True
    assert captured[0].independent_backup_verify is True


def test_upgrade_seeds_nomos_baseline_before_smoke_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_verified_backup_boundary(monkeypatch)
    from okg.substrate import deployment_upgrade

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": "postgres://user@host/db",
            "target": {"database": "db", "identity": "ok"},
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(deployment_dir=tmp_path / "dep-root"),
    )
    calls: list[str] = []

    def nomos_baseline(ctx) -> dict:
        assert ctx.dsn == "postgres://user@host/db"
        calls.append("nomos")
        return {"ok": True, "result": {"active_policy": True}}

    monkeypatch.setattr(
        deployment_upgrade,
        "_phase_nomos_baseline",
        nomos_baseline,
    )
    monkeypatch.setattr(
        deployment_upgrade,
        "_phase_source_convergence",
        lambda ctx: {"ok": True, "state": "ready"},
    )

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if argv[:4] == running_okg_argv():
            if "prepare-effective-attribute-recovery" in argv:
                return _ok({
                    "ok": True,
                    "prepared": False,
                    "classification": _no_published_recovery(),
                })
            if "migrations" in argv:
                return _ok({"pending": [], "destructive_pending": []})
            if "catalog" in argv:
                calls.append("catalog")
            if "ingest" in argv:
                calls.append("smoke")
                return _ok(_published("gen-nomos-baseline"))
            if "ready" in argv:
                return _ok({
                    "generation": {"generation_id": "gen-nomos-baseline"}
                })
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "up-state.json",
            skip_backup=True,
        ),
        command_runner=runner,
    )

    assert result["ok"] is True, json.dumps(result, indent=2)
    assert calls == ["catalog", "nomos", "smoke"]


def test_upgrade_forwards_multi_include_to_ingest_as_comma_list(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_verified_backup_boundary(monkeypatch)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": "postgres://user@host/db",
            "target": {"database": "db", "identity": "ok"},
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_generation_recovery",
        lambda ctx: {"ok": True, "recovered": False},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_source_convergence",
        lambda ctx: {"ok": True, "state": "ready"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(deployment_dir=tmp_path / "dep-root"),
    )
    ingest_calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if argv[:4] == running_okg_argv():
            if "migrations" in argv:
                return _ok({"pending": [], "destructive_pending": []})
            if "ingest" in argv:
                ingest_calls.append(argv)
                return _ok(_published("gen-multi-include"))
            if "ready" in argv:
                return _ok({"generation": {"generation_id": "gen-multi-include"}})
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "up-state.json",
            skip_backup=True,
            include=("git-history", "pact", "package-manifests"),
        ),
        command_runner=runner,
    )

    assert result["ok"] is True
    assert len(ingest_calls) == 1
    include_index = ingest_calls[0].index("--include")
    assert ingest_calls[0][include_index + 1] == ("git-history,pact,package-manifests")


def test_upgrade_installs_required_extensions_before_migrate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_recovery_phases(monkeypatch)
    _stub_verified_backup_boundary(monkeypatch)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": "postgres://user@host/db",
            "target": {"database": "db", "identity": "ok"},
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(deployment_dir=tmp_path / "dep-root"),
    )
    calls: list[str] = []

    def fake_extensions(ctx) -> dict[str, list[str]]:
        calls.append(f"extensions:{ctx.dsn}")
        return {"created": ["vector"], "already_present": ["pg_trgm"]}

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._ensure_upgrade_extensions",
        fake_extensions,
    )

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if argv[:4] == running_okg_argv():
            if "migrations" in argv:
                return _ok({"pending": [], "destructive_pending": []})
            if "migrate" in argv:
                calls.append("migrate")
            if "ingest" in argv:
                return _ok(_published("gen-extension-bootstrap"))
            if "ready" in argv:
                return _ok({
                    "generation": {"generation_id": "gen-extension-bootstrap"}
                })
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "up-state.json",
            skip_backup=True,
        ),
        command_runner=runner,
    )

    migrate_phase = next(row for row in result["phases"] if row["phase"] == "migrate")
    assert result["ok"] is True
    assert calls[:2] == ["extensions:postgres://user@host/db", "migrate"]
    assert migrate_phase["extensions"] == {
        "created": ["vector"],
        "already_present": ["pg_trgm"],
    }


def test_upgrade_defers_views_registry_until_after_smoke_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_recovery_phases(monkeypatch)
    _stub_verified_backup_boundary(monkeypatch)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": "postgres://user@host/db",
            "target": {"database": "db", "identity": "ok"},
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(deployment_dir=tmp_path / "dep-root"),
    )
    catalog_attempts = 0
    calls: list[str] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        nonlocal catalog_attempts
        if argv[:4] == running_okg_argv():
            if "migrations" in argv:
                return _ok({"pending": [], "destructive_pending": []})
            if "catalog" in argv:
                catalog_attempts += 1
                calls.append(f"catalog:{catalog_attempts}")
                if "smoke" not in calls:
                    return CommandResult(
                        1,
                        json.dumps(
                            {
                                "error": {
                                    "message": (
                                        "no published generation for deployment "
                                        "'dep'; run `okg run` to establish a "
                                        "baseline generation before loading the "
                                        "views registry"
                                    )
                                }
                            }
                        ),
                        "",
                    )
                return _ok(
                    {
                        "applied": True,
                        "views_registry": {
                            "applied": True,
                            "generation_id": 42,
                        },
                    }
                )
            if "ingest" in argv:
                calls.append("smoke")
                return _ok(_published("gen-smoke"))
            if "ready" in argv:
                return _ok({"generation": {"generation_id": "gen-smoke"}})
        return _ok()

    state_file = tmp_path / "up-state.json"
    first = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=state_file,
            skip_backup=True,
            stop_after="catalog_apply",
        ),
        command_runner=runner,
    )
    assert first["ok"] is False
    assert first["stopped_after"] == "catalog_apply"
    assert calls == ["catalog:1"]

    calls.clear()
    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=state_file,
            skip_backup=True,
        ),
        command_runner=runner,
    )

    catalog_phase = next(
        row for row in result["phases"] if row["phase"] == "catalog_apply"
    )
    smoke_phase = next(
        row for row in result["phases"] if row["phase"] == "smoke_publish"
    )
    assert result["ok"] is True
    assert catalog_phase["views_registry_deferred"] is True
    assert smoke_phase["result"]["publish"]["generation_id"] == "gen-smoke"
    assert (
        smoke_phase["result"]["views_registry_apply"]["views_registry"]["generation_id"]
        == 42
    )
    assert calls == ["catalog:2", "smoke", "catalog:3"]


def test_upgrade_resume_continues_after_completed_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_dependency_sync_checkout(monkeypatch)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": "postgres://user@host/db",
            "target": {
                "identity": "ok",
                "database": "db",
                "expected_database": "db",
            },
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_generation_recovery",
        lambda ctx: {"ok": True, "classification": "not_required"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_source_convergence",
        lambda ctx: {"ok": True, "state": "ready"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: _full_restore_manifest(tmp_path / "dep-root"),
    )
    executed: list[str] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if backup := _backup_command(argv):
            executed.append(
                "pg_dump" if argv and argv[0] == "pg_dump" else "archive_validation"
            )
            return backup
        if argv[:4] == running_okg_argv():
            if "migrations" in argv:
                executed.append(
                    "preflight" if "preflight" not in executed else "migrate_status"
                )
                return _ok({"pending": [], "destructive_pending": []})
            if "migrate" in argv:
                executed.append("migrate")
            elif "catalog" in argv:
                executed.append("catalog")
            elif "ingest" in argv:
                executed.append("smoke")
                return _ok(_published("gen-resume"))
            elif "release" in argv:
                executed.append("release")
            elif "ready" in argv:
                executed.append("ready")
                return _ok({"generation": {"generation_id": "gen-resume"}})
            return _ok()
        return _ok()

    state_file = tmp_path / "up-state.json"
    first = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=state_file,
            backup_dir=tmp_path / "backups",
            stop_after="backup",
        ),
        command_runner=runner,
    )
    assert first["ok"] is False
    assert first["stopped_after"] == "backup"

    executed.clear()
    second = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=state_file,
            backup_dir=tmp_path / "backups",
        ),
        command_runner=runner,
    )

    assert second["ok"] is True
    assert "pg_dump" not in executed
    assert "archive_validation" in executed
    assert second["phases"][0]["phase"] == "preflight"
    assert second["phases"][1]["phase"] == "backup"
    assert second["phases"][-1]["phase"] == "readiness"


def test_ordinary_repair_rebinds_after_authorized_snapshot_backfill(
    tmp_path: Path,
) -> None:
    from okg.substrate.deployment_upgrade import (
        UpgradeContext,
        UpgradeOptions,
        _phase_generation_recovery,
    )

    original = {
        "schema": "okg.effective_attribute_recovery_classification.v1",
        "classification": "ordinary_repair",
        "deployment": "dep",
        "branch": "default",
        "branch_id": "default",
        "generation_id": "gen:1",
        "ordinal": 7,
        "checkpoint_counts": {"node": 3, "edge": 2},
        "pinned_counts": {"node": 3, "edge": 2},
        "snapshot_identity": {"row_count": 0, "fingerprint": "sha256:old"},
        "interval_identity": {"row_count": 6, "fingerprint": "sha256:interval"},
        "payload_match": False,
        "integrity_receipt": {"ok": False, "reason": "receipt_dirty"},
        "count_stamp_recovery": None,
        "binding_digest": "sha256:before",
    }
    rebound = {
        **original,
        "snapshot_identity": {"row_count": 5, "fingerprint": "sha256:backfilled"},
        "binding_digest": "sha256:after",
    }
    calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        calls.append(argv)
        if "inspect-effective-attribute-recovery" in argv:
            return _ok(rebound)
        if "prepare-effective-attribute-recovery" in argv:
            assert argv[argv.index("--expected-binding-digest") + 1] == (
                "sha256:after"
            )
            expected = json.loads(
                argv[argv.index("--expected-classification") + 1]
            )
            assert expected == rebound
            return _ok(
                {
                    "ok": True,
                    "prepared": False,
                    "classification": rebound,
                }
            )
        raise AssertionError(argv)

    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="dep", skip_backup=True),
        dsn="postgres://user@host/db",
        resolution={},
        env={},
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path,
        command_runner=runner,
        data={"attribute_recovery_classification": original},
    )

    result = _phase_generation_recovery(ctx)

    assert result["ok"] is True
    assert result["binding_rebased"] is True
    assert result["revalidated_classification"] == rebound
    assert ctx.data["attribute_recovery_classification"] == rebound
    assert len(calls) == 2


def test_ordinary_repair_rebinds_to_monotonic_post_backup_successor(
    tmp_path: Path,
) -> None:
    from okg.substrate.deployment_upgrade import (
        UpgradeContext,
        UpgradeOptions,
        _phase_generation_recovery,
    )

    original = {
        "schema": "okg.effective_attribute_recovery_classification.v1",
        "classification": "ordinary_repair",
        "deployment": "dep",
        "branch": "default",
        "branch_id": "default",
        "generation_id": "gen:1",
        "ordinal": 7,
        "checkpoint_counts": {"node": 3, "edge": 2},
        "pinned_counts": {"node": 3, "edge": 2},
        "count_stamp_recovery": None,
        "binding_digest": "sha256:before",
    }
    observed = {
        **original,
        "generation_id": "gen:2",
        "ordinal": 8,
        "binding_digest": "sha256:after",
        "checkpoint_counts": {"node": 4, "edge": 3},
        "pinned_counts": {"node": 4, "edge": 3},
    }
    calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        calls.append(argv)
        if "inspect-effective-attribute-recovery" in argv:
            return _ok(observed)
        if "prepare-effective-attribute-recovery" in argv:
            expected = json.loads(
                argv[argv.index("--expected-classification") + 1]
            )
            assert expected == observed
            assert argv[argv.index("--expected-binding-digest") + 1] == (
                "sha256:after"
            )
            return _ok(
                {
                    "ok": True,
                    "prepared": False,
                    "classification": observed,
                }
            )
        raise AssertionError(argv)

    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="dep", skip_backup=True),
        dsn="postgres://user@host/db",
        resolution={},
        env={},
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path,
        command_runner=runner,
        data={"attribute_recovery_classification": original},
    )

    result = _phase_generation_recovery(ctx)

    assert result["ok"] is True
    assert result["graph_anchor_rebased"] is True
    assert result["monotonic_ordinary_successor"] is True
    assert result["revalidated_classification"] == observed
    assert ctx.data["attribute_recovery_classification"] == observed
    assert len(calls) == 2


@pytest.mark.parametrize(
    ("observed_update", "reason"),
    [
        ({"deployment": "other"}, "deployment drift"),
        ({"branch_id": "other"}, "branch drift"),
        ({"branch": None}, "branch is missing"),
        ({"generation_id": "gen:1"}, "generation did not change"),
        ({"ordinal": 7}, "ordinal did not advance"),
        ({"ordinal": "8"}, "ordinal is malformed"),
        (
            {
                "checkpoint_counts": {"node": 4, "edge": 3},
                "pinned_counts": {"node": 3, "edge": 3},
            },
            "current counts are not exact",
        ),
        (
            {"count_stamp_recovery": {"schema": "unexpected"}},
            "count-stamp recovery appeared",
        ),
        (
            {"classification": "corrupt_state_recovery"},
            "classification became corrupt",
        ),
    ],
)
def test_ordinary_repair_rebind_refuses_unsafe_post_backup_successor(
    tmp_path: Path,
    observed_update: dict[str, object],
    reason: str,
) -> None:
    from okg.substrate.deployment_upgrade import (
        UpgradeContext,
        UpgradeOptions,
        _phase_generation_recovery,
    )

    original = {
        "schema": "okg.effective_attribute_recovery_classification.v1",
        "classification": "ordinary_repair",
        "deployment": "dep",
        "branch": "default",
        "branch_id": "default",
        "generation_id": "gen:1",
        "ordinal": 7,
        "checkpoint_counts": {"node": 3, "edge": 2},
        "pinned_counts": {"node": 3, "edge": 2},
        "count_stamp_recovery": None,
        "binding_digest": "sha256:before",
    }
    observed = {
        **original,
        "generation_id": "gen:2",
        "ordinal": 8,
        "checkpoint_counts": {"node": 4, "edge": 3},
        "pinned_counts": {"node": 4, "edge": 3},
        "binding_digest": "sha256:after",
        **observed_update,
    }

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        assert "inspect-effective-attribute-recovery" in argv, reason
        return _ok(observed)

    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="dep", skip_backup=True),
        dsn="postgres://user@host/db",
        resolution={},
        env={},
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path,
        command_runner=runner,
        data={"attribute_recovery_classification": original},
    )

    result = _phase_generation_recovery(ctx)

    assert result["ok"] is False, reason
    assert result["reason"] == (
        "attribute recovery graph anchor changed after preflight"
    )
    assert result["graph_anchor_matches"] is False
    assert result["monotonic_ordinary_successor"] is False


def test_ordinary_repair_rebind_refuses_drift_after_current_inspection(
    tmp_path: Path,
) -> None:
    from okg.substrate.deployment_upgrade import (
        UpgradeContext,
        UpgradeOptions,
        _phase_generation_recovery,
    )

    original = {
        "schema": "okg.effective_attribute_recovery_classification.v1",
        "classification": "ordinary_repair",
        "deployment": "dep",
        "branch": "default",
        "branch_id": "default",
        "generation_id": "gen:1",
        "ordinal": 7,
        "checkpoint_counts": {"node": 3, "edge": 2},
        "pinned_counts": {"node": 3, "edge": 2},
        "count_stamp_recovery": None,
        "binding_digest": "sha256:before",
    }
    observed = {
        **original,
        "generation_id": "gen:2",
        "ordinal": 8,
        "checkpoint_counts": {"node": 4, "edge": 3},
        "pinned_counts": {"node": 4, "edge": 3},
        "binding_digest": "sha256:after",
    }

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if "inspect-effective-attribute-recovery" in argv:
            return _ok(observed)
        if "prepare-effective-attribute-recovery" in argv:
            return CommandResult(
                2,
                json.dumps({"ok": False, "reason": "binding changed"}),
                "",
            )
        raise AssertionError(argv)

    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="dep", skip_backup=True),
        dsn="postgres://user@host/db",
        resolution={},
        env={},
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path,
        command_runner=runner,
        data={"attribute_recovery_classification": original},
    )

    result = _phase_generation_recovery(ctx)

    assert result["ok"] is False
    assert result["reason"] == (
        "attribute recovery no-op binding revalidation failed"
    )


def test_upgrade_resume_restarts_at_earliest_failed_live_postcondition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A durable phase name cannot conceal database drift after interruption."""
    _stub_dependency_sync_checkout(monkeypatch)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {"dsn": "postgres://user@host/db", "target": {}},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(deployment_dir=tmp_path / "dep-root"),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    recovery_runs = 0

    def recovery(ctx) -> dict:
        nonlocal recovery_runs
        recovery_runs += 1
        return {"ok": True, "recovered": False}

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_generation_recovery", recovery
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_source_convergence",
        lambda ctx: {"ok": True, "state": "ready"},
    )
    def backup(ctx) -> dict:
        ctx.data["safety_plan"] = {
            "schema": "okg.deployment_safety_plan.v1",
            "external_effects": {"present": False},
            "restore_point": {"source": "unit", "verified": True},
            "confirmation": {
                "required": False,
                "digest": "sha256:unit",
                "provided": False,
                "exact": False,
            },
        }
        result = {
            "ok": True,
            "skipped": "unit backup boundary",
            "verification_mode": "current_full_restore",
        }
        ctx.data["backup_result"] = result
        return result

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_backup",
        backup,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._revalidate_backup_before_mutation",
        lambda ctx: {
            "ok": True,
            "verification_mode": "current_full_restore",
            "archive_revalidated": True,
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._revalidate_effect_authority",
        lambda ctx, backup: {
            "ok": True,
            "target_revision_stable": True,
            "effect_digest_stable": True,
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._revalidate_resumed_safety_contract",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_attribute_read_model",
        lambda ctx: {"ok": True, "status": "unit"},
    )

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if "migrations" in argv:
            return _ok({"pending": [], "destructive_pending": []})
        if "release" in argv:
            return _ok({"ok": True})
        if "ready" in argv:
            return _ok({
                "ok": False,
                "nomos_audit": {"ok": True},
                "nomos_runtime": {"ok": True},
                "generation": {
                    "status": "published",
                    "count_integrity": {"ok": False},
                },
                "source_sync_liveness": {"ok": True},
                "source_sync_status": {
                    "convergence": {"ok": True, "state": "ready"}
                },
            })
        return _ok()

    state_file = tmp_path / "up-state.json"
    first = run_deployment_upgrade(
            UpgradeOptions(
                deployment="dep",
                state_file=state_file,
                stop_after="source_convergence",
            ),
        command_runner=runner,
    )
    assert first["stopped_after"] == "source_convergence"
    assert recovery_runs == 1

    second = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=state_file,
            stop_after="generation_recovery",
        ),
        command_runner=runner,
    )

    assert second["stopped_after"] == "generation_recovery"
    assert recovery_runs == 2
    persisted = json.loads(state_file.read_text())
    assert persisted["resume_revalidation"]["earliest_unproven_phase"] == (
        "generation_recovery"
    )
    assert any(
        row["phase"] == "generation_recovery"
        for row in persisted["resume_revalidation"]["superseded_phase_results"]
    )


def test_resume_always_reexecutes_stop_services_without_current_process_authority(
    tmp_path: Path,
) -> None:
    from okg.substrate import deployment_upgrade as du

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        raise AssertionError((argv, env))

    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="dep"),
        dsn="postgres://user@host/db",
        resolution={},
        env={},
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path / "backups",
        command_runner=runner,
    )
    rows, completed, audit = du._revalidate_resume_postconditions(
        ctx,
        phase_results=[{"phase": "stop_services", "ok": True}],
        completed={"stop_services"},
    )

    assert "stop_services" not in completed
    assert audit["earliest_unproven_phase"] == "stop_services"
    assert rows == []


def test_resume_dependency_check_preserves_receipt_under_retained_runtime_fence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_dependency_sync_checkout(monkeypatch)
    from okg.substrate import deployment_upgrade as du

    monkeypatch.setattr(
        du,
        "_resolve_upgrade_deployment",
        lambda _ctx: SimpleNamespace(
            deployment_dir=tmp_path / "dep-root",
            runtime=SimpleNamespace(python_extras=("quality", "code")),
        ),
    )
    fence_calls: list[set[str]] = []
    monkeypatch.setattr(
        du,
        "_revalidate_runtime_fence_resume",
        lambda _ctx, *, completed: (
            fence_calls.append(set(completed))
            or {"ok": True, "promotion_id": "deploy-owner"}
        ),
    )
    calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        calls.append(argv)
        return _ok()

    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="dep"),
        dsn="postgres://user@host/db",
        resolution={},
        env={},
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path / "backups",
        command_runner=runner,
        data={"runtime_enabled": True},
    )
    rows, completed, audit = du._revalidate_resume_postconditions(
        ctx,
        phase_results=[{"phase": "dependency_sync", "ok": True}],
        completed={"sync_code", "dependency_sync"},
    )

    project_root = _planner_project_root()
    expected = [
        "uv",
        "sync",
        "--project",
        str(project_root),
        "--check",
        "--frozen",
        "--extra",
        "quality",
        "--extra",
        "code",
    ]
    assert calls == [expected]
    assert fence_calls == [{"sync_code", "dependency_sync"}]
    assert completed == {"sync_code", "dependency_sync"}
    assert rows == [{"phase": "dependency_sync", "ok": True}]
    assert audit["checks"]["runtime_fence"] is True
    assert audit["evidence"]["runtime_fence"] == {
        "ok": True,
        "promotion_id": "deploy-owner",
    }
    assert audit["checks"]["dependency_sync"] is True
    assert audit["evidence"]["dependency_sync"] == {
        "returncode": 0,
        "command": (
            f"uv sync --project {project_root} --check --frozen "
            "--extra quality --extra code"
        ),
        "project_root": str(project_root),
        "environment": str(project_root / ".venv"),
        "python_extras": ["quality", "code"],
    }


def test_upgrade_admits_candidate_bound_precheckout_backup_without_redump(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    _stub_recovery_phases(monkeypatch)
    repo = tmp_path / "repo"
    deployment_dir = repo / "deployments/dep"
    deployment_dir.mkdir(parents=True)

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", *args],
            cwd=repo,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    git("init")
    git("config", "user.email", "test@example.com")
    git("config", "user.name", "Test")
    (deployment_dir / "deployment.yaml").write_text(
        "name: dep\nrelease:\n  channel: dev\n"
    )
    # The fixture repository stands in for the okg source checkout (see the
    # patched planner module below), so it owns the project file whose lock a
    # dependency sync applies.
    (repo / "pyproject.toml").write_text(
        '[project]\nname = "okg-precheckout-fixture"\nversion = "0.0.0"\n',
        encoding="utf-8",
    )
    git("add", ".")
    git("commit", "-m", "installed")
    old_revision = git("rev-parse", "HEAD")
    # Only the planner's bytes matter here (the receipt binds their digest),
    # so the candidate commits synthetic planner bytes rather than a copy of
    # the okg-ci deployment's real planner, which the public copy lacks.
    planner_bytes = b"# candidate lifecycle planner the receipt binds by digest\n"
    candidate_planner = repo / du.PLANNER_REPO_PATH
    candidate_planner.parent.mkdir(parents=True)
    candidate_planner.write_bytes(planner_bytes)
    # The fixture repository stands in for the okg source checkout the planner
    # is running out of, so the code-revision authority resolves to it. Only the
    # directory has to exist -- the module is never read from disk -- which
    # keeps the candidate planner blob absent from the installed revision.
    planner_module = repo / _PLANNER_MODULE_RELPATH
    planner_module.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(du, "__file__", str(planner_module))
    (deployment_dir / "deployment.yaml").write_text(
        "name: dep\nrelease:\n  channel: dev\ndescription: candidate\n"
        "host_storage:\n  warn_available_pct: 15\n"
        "  blocker_available_pct: 6\n"
    )
    git("add", ".")
    git("commit", "-m", "candidate")
    new_revision = git("rev-parse", "HEAD")
    git("checkout", "--detach", old_revision)
    lifecycle_contract = {
        "schema": CANONICAL_CONTRACT.schema,
        "contract_version": CANONICAL_CONTRACT.contract_version,
        "phases": list(CANONICAL_CONTRACT.phases),
    }
    lifecycle_plan = tmp_path / "lifecycle-plan.json"
    lifecycle_plan.write_text(
        json.dumps(
            {
                "schema": "okg.okg_ci_lifecycle_plan.v2",
                "action": "full_lifecycle",
                "candidate_revision": new_revision,
                "candidate_planner_digest": (
                    "sha256:" + hashlib.sha256(planner_bytes).hexdigest()
                ),
                "selected_phases": list(CANONICAL_CONTRACT.phases),
                "candidate_contract": lifecycle_contract,
                "candidate_contract_digest": contract_digest(lifecycle_contract),
            }
        )
    )
    lifecycle_plan_digest = (
        "sha256:" + hashlib.sha256(lifecycle_plan.read_bytes()).hexdigest()
    )
    target = {
        "deployment": "dep",
        "host": "host",
        "port": 5432,
        "database": "db",
        "expected_database": "db",
        "identity": "ok",
    }
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {"dsn": "postgres://user@host/db", "target": target},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_catalog_apply",
        lambda ctx: {"ok": True, "status": "unit"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: _full_restore_manifest(deployment_dir),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._estimate_source_db_bytes",
        lambda ctx: 68 * 1024**3,
    )
    def snapshot(ctx, toolchain) -> dict:
        del toolchain
        return {
            "target": {
                "deployment": "dep",
                "host": "host",
                "port": 5432,
                "database": "db",
            },
            "database_owner": {
                "deployment": "dep",
                "uuid": "test-owner",
                "schema_fingerprint": "test-schema",
            },
            "cluster_system_identifier": "test-cluster",
            "server_version": "170010",
            "extension_versions": [],
            "schema_migrations_digest": "sha256:test-schema-migrations",
            "schema_catalog_digest": "sha256:test-schema-catalog",
            "candidate_plan_digest": du._migration_plan_digest(
                ctx.data["migrations_before"]
            ),
            "archive_format": "postgresql-custom",
            "timescaledb": {
                "extension_version": None,
                "hypertables": [],
                "continuous_aggregates": [],
                "hypertable_chunk_counts": [],
            },
            "dump_tool_major": 17,
            "restore_tool_major": 17,
        }

    restore_calls = 0

    def restore(ctx, path: Path, **_) -> dict:
        nonlocal restore_calls
        del ctx, path
        restore_calls += 1
        return {
            "ok": True,
            "restore_verified": True,
            "schema_ok": True,
            "ownership_ok": True,
            "observed_restored_size_bytes": 70 * 1024**3,
        }

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._backup_compatibility_snapshot",
        snapshot,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._verify_backup_restore",
        restore,
    )
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    archive = backup_dir / "verified.dump"
    archive.write_bytes(b"verified backup")
    archive.chmod(0o600)
    digest = "sha256:" + hashlib.sha256(archive.read_bytes()).hexdigest()
    receipt_path = tmp_path / "preflight.json"
    preflight_receipt = {
        "ok": True,
        "migration_status": {
            "pending": [],
            "destructive_pending": [],
        },
    }
    backup_receipt = {
        "ok": True,
        "backup_path": str(archive),
        "backup_digest": digest,
        "effect_inventory": {
            "effect_digest": "sha256:test-effect",
        },
        "backup_size_bytes": archive.stat().st_size,
        "backup_mode": "0600",
        "command": "pg_dump --format=custom --file verified.dump --dbname db",
        "archive_readable": True,
        "ownership_ok": True,
        "schema_ok": True,
        "restore_verified": True,
    }
    def preflight(ctx) -> dict:
        from okg.substrate import fastyaml

        candidate_manifest = fastyaml.safe_load(
            git("show", f"{new_revision}:deployments/dep/deployment.yaml")
        )
        ctx.data["effect_inventory_static"] = {
            "target_revision": new_revision,
            "raw_manifest": candidate_manifest,
        }
        return dict(preflight_receipt)

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_preflight",
        preflight,
    )

    def backup(ctx) -> dict:
        from okg.substrate.runtime import host_headroom

        monkeypatch.setattr(
            host_headroom,
            "probe_filesystem",
            lambda _path: (100 * 1024**3, 80 * 1024**3),
        )
        report = _actual_backup_headroom_preflight(
            ctx,
            needed_bytes=68 * 1024**3,
        )
        assert report["refuse"] is False
        assert report["thresholds"] == {
            "warn_available_pct": 15.0,
            "blocker_available_pct": 6.0,
        }
        assert report["threshold_authority"] == {
            "kind": "candidate_manifest",
            "target_revision": new_revision,
            "manifest_revision": new_revision,
            "effect_inventory_target_revision": new_revision,
            "target_manifest_digest": du._upgrade_manifest_digest(ctx),
            "manifest_bound_by": (
                "precheckout_authority.target_manifest_digest"
            ),
        }
        return {**backup_receipt, "headroom": report}

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_backup",
        backup,
    )
    def collected_effects(ctx, expected_digest=None):
        del ctx, expected_digest
        checks = _closed_effect_checks()
        return checks, (), {
            "target_revision": new_revision,
            "effect_digest": "sha256:test-effect",
            "execution_checkout_exact": True,
            "smoke": {"selected_source_digest": "sha256:test-smoke"},
            "checks": checks.to_dict(),
        }

    monkeypatch.setattr(du, "_collect_effect_inventory", collected_effects)
    calls: list[list[str]] = []
    migration_reads = 0

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        nonlocal migration_reads
        del env
        calls.append(argv)
        if backup := _backup_command(argv):
            assert argv[:2] == ["pg_dump", "--version"] or argv[0] != "pg_dump"
            return backup
        if "migrations" in argv:
            migration_reads += 1
            return _ok(
                {
                    "pending": (
                        [] if migration_reads == 1 else [{"migration_id": "new-plan"}]
                    ),
                    "destructive_pending": [],
                }
            )
        if "inspect-effective-attribute-recovery" in argv:
            return _ok(_no_published_recovery())
        if "ingest" in argv:
            return _ok({"publishes": [_published("gen-recovered")]})
        if "ready" in argv:
            return _ok({"generation": {"generation_id": "gen-recovered"}})
        if argv[-4:-2] == ["release", "dep"]:
            return _ok({"ok": True})
        return _ok()

    # A target revision without the planner (the installed revision here, and
    # every revision of the public copy) refuses and names the planner path.
    with pytest.raises(
        SubstrateError, match="precheckout candidate lifecycle planner is unreadable"
    ) as no_planner:
        run_deployment_upgrade(
            UpgradeOptions(
                deployment="dep",
                profile="prod",
                state_file=tmp_path / "no-planner-state.json",
                backup_dir=backup_dir,
                precheckout_target_revision=old_revision,
                lifecycle_plan=lifecycle_plan,
                stop_after="backup",
                skip_destructive_migrations=True,
                apply_services=False,
                sync_code=False,
                sync_deps=False,
                reset_state=True,
            ),
            command_runner=runner,
        )
    assert no_planner.value.data["path"] == du.PLANNER_REPO_PATH
    assert "does not exist" in no_planner.value.data["git_error"]
    assert du.PLANNER_REPO_PATH in (no_planner.value.fix_hint or "")

    precheckout = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            profile="prod",
            state_file=receipt_path,
            backup_dir=backup_dir,
            precheckout_target_revision=new_revision,
            lifecycle_plan=lifecycle_plan,
            stop_after="backup",
            skip_destructive_migrations=True,
            apply_services=False,
            sync_code=False,
            sync_deps=False,
            reset_state=True,
        ),
        command_runner=runner,
    )
    assert precheckout["stopped_after"] == "backup"
    produced = json.loads(receipt_path.read_text())
    assert produced["precheckout_authority"]["planner_revision"] == old_revision
    assert produced["precheckout_authority"]["target_revision"] == new_revision
    assert produced["precheckout_authority"]["lifecycle_plan_digest"] == (
        lifecycle_plan_digest
    )
    precheckout_backup = next(
        row for row in produced["phase_results"] if row["phase"] == "backup"
    )
    assert (
        precheckout_backup["headroom"]["threshold_authority"][
            "target_manifest_digest"
        ]
        == produced["precheckout_authority"]["target_manifest_digest"]
    )
    with pytest.raises(
        SubstrateError,
        match="deployment lifecycle plan candidate does not match this checkout",
    ) as old_checkout:
        run_deployment_upgrade(
            UpgradeOptions(
                deployment="dep",
                state_file=tmp_path / "old-checkout-state.json",
                backup_dir=backup_dir,
                verified_backup_state=receipt_path,
                verified_backup_source_revision=old_revision,
                lifecycle_plan=lifecycle_plan,
                reset_state=True,
            ),
            command_runner=runner,
        )
    assert old_checkout.value.data == {
        "planned": new_revision,
        "observed": old_revision,
    }

    git("checkout", "--detach", new_revision)

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "state.json",
            backup_dir=backup_dir,
            verified_backup_state=receipt_path,
            verified_backup_source_revision=old_revision,
            lifecycle_plan=lifecycle_plan,
            reset_state=True,
        ),
        command_runner=runner,
    )

    assert result["ok"] is True, json.dumps(result, indent=2)
    assert not any(call[0] == "pg_dump" and "--file" in call for call in calls)
    assert [row["phase"] for row in result["phases"][:2]] == [
        "preflight",
        "backup",
    ]
    backup = next(row for row in result["phases"] if row["phase"] == "backup")
    assert backup["admission"]["source_revision"] == old_revision
    assert backup["admission"]["target_revision"] == new_revision
    assert all(backup["admission"]["checks"].values())
    migrate = next(row for row in result["phases"] if row["phase"] == "migrate")
    assert migrate["backup_revalidation"]["candidate_plan_stable"] is True
    assert migrate["backup_revalidation"]["verification_mode"] == (
        "current_full_restore"
    )
    assert restore_calls == 0


def test_upgrade_defers_destructive_migrations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_recovery_phases(monkeypatch)
    _stub_verified_backup_boundary(monkeypatch)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {"dsn": "postgres://user@host/db", "target": {}},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(deployment_dir=tmp_path / "dep-root"),
    )
    destructive = {
        "migration_id": "retire_legacy_runtime",
        "destructive": True,
    }
    calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        calls.append(argv)
        if argv[:4] == running_okg_argv():
            if "migrations" in argv:
                return _ok(
                    {
                        "pending": [destructive],
                        "destructive_pending": [destructive],
                    }
                )
            if "ingest" in argv:
                return _ok(_published("gen-destructive-deferred"))
            if "ready" in argv:
                return _ok({
                    "generation": {"generation_id": "gen-destructive-deferred"}
                })
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "up-state.json",
            skip_backup=True,
        ),
        command_runner=runner,
    )

    assert result["ok"] is True
    assert not any("retire-legacy-runtime" in call for call in calls)
    migrate_phase = next(row for row in result["phases"] if row["phase"] == "migrate")
    assert migrate_phase["destructive_migrations"] == []
    assert migrate_phase["destructive_migrations_deferred"] == [destructive]


def test_upgrade_applies_reviewed_destructive_adapter_only_after_confirmation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_recovery_phases(monkeypatch)
    _stub_verified_backup_boundary(monkeypatch)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {"dsn": "postgres://user@host/db", "target": {}},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(deployment_dir=tmp_path / "dep-root"),
    )
    destructive = {
        "migration_id": "0002_remove_job_runtime_subsystem",
        "destructive": True,
    }
    calls: list[list[str]] = []
    migration_reads = 0

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        nonlocal migration_reads
        del env
        calls.append(argv)
        if argv[:4] == running_okg_argv():
            if "migrations" in argv:
                migration_reads += 1
                return _ok(
                    {
                        "pending": [destructive] if migration_reads == 1 else [],
                        "destructive_pending": (
                            [destructive] if migration_reads == 1 else []
                        ),
                    }
                )
            if "ingest" in argv:
                return _ok(_published("gen-destructive-applied"))
            if "ready" in argv:
                return _ok({
                    "generation": {"generation_id": "gen-destructive-applied"}
                })
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "up-state.json",
            skip_backup=True,
            confirm_destructive_migrations=True,
        ),
        command_runner=runner,
    )

    assert result["ok"] is True
    cleanup_index = next(
        i for i, call in enumerate(calls) if "remove-job-runtime-subsystem" in call
    )
    migrate_index = next(i for i, call in enumerate(calls) if "migrate" in call)
    assert cleanup_index < migrate_index
    migrate_phase = next(row for row in result["phases"] if row["phase"] == "migrate")
    assert migrate_phase["destructive_migrations"][0]["migration_id"] == (
        "0002_remove_job_runtime_subsystem"
    )
    assert migrate_phase["destructive_migrations_deferred"] == []


def test_upgrade_refuses_unknown_destructive_migration_adapter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_recovery_phases(monkeypatch)
    _stub_verified_backup_boundary(monkeypatch)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {"dsn": "postgres://user@host/db", "target": {}},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(deployment_dir=tmp_path / "dep-root"),
    )
    destructive = {"migration_id": "9999_unknown_destructive", "destructive": True}

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if "migrations" in argv:
            return _ok({"pending": [destructive], "destructive_pending": [destructive]})
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "up-state.json",
            skip_backup=True,
            confirm_destructive_migrations=True,
        ),
        command_runner=runner,
    )

    assert result["ok"] is False
    assert result["failed_phase"] == "migrate"
    assert result["phases"][-1]["reason"] == (
        "destructive migration has no deployment-up adapter"
    )
    assert result["phases"][-1]["migration_id"] == "9999_unknown_destructive"


def test_dev_upgrade_bootstraps_isolated_database_before_migration_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade

    dsn = "postgres://postgres:okg@127.0.0.1:5433/okg_workspace_abc123"
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": dsn,
            "authority": {"mode": "dev_isolated"},
            "target": {"database": "okg_workspace_abc123", "identity": "isolated"},
        },
    )
    order: list[str] = []

    def bootstrap(ctx) -> dict:
        assert ctx.options.profile == "dev"
        order.append("bootstrap")
        return {
            "ok": True,
            "database": "okg_workspace_abc123",
            "created": True,
            "maintenance_database": "postgres",
        }

    monkeypatch.setattr(
        deployment_upgrade,
        "_ensure_dev_isolated_database",
        bootstrap,
    )
    # A freshly-created isolated clone has no okg.schema_migrations ledger, so
    # the schema-init step installs the schema before the migration preflight.
    import psycopg

    monkeypatch.setattr(
        psycopg, "connect", lambda *a, **k: _SchemaProbeConnection(None)
    )

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        assert env["OKG_DSN"] == dsn
        if argv[:4] == running_okg_argv():
            if "migrations" in argv:
                order.append("migrations")
                assert order == ["bootstrap", "schema_install", "migrations"]
                return _ok({"pending": [], "destructive_pending": []})
            if "migrate" in argv:
                order.append("schema_install")
                assert order == ["bootstrap", "schema_install"]
                return _ok()
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            profile="dev",
            state_file=tmp_path / "up-state.json",
            skip_backup=True,
            stop_after="preflight",
        ),
        command_runner=runner,
    )

    assert result["stopped_after"] == "preflight"
    preflight = result["phases"][0]
    assert preflight["ok"] is True
    assert preflight["dev_database"]["created"] is True
    assert preflight["dev_schema"]["schema_installed"] is True
    assert order == ["bootstrap", "schema_install", "migrations"]


def test_dev_database_bootstrap_creates_missing_isolated_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade

    executed: list[tuple[str, tuple | None]] = []

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=None) -> None:
            executed.append((str(query), params))

        def fetchone(self):
            return None

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

    monkeypatch.setattr("psycopg.connect", lambda *args, **kwargs: FakeConn())
    ctx = SimpleNamespace(
        options=UpgradeOptions(deployment="dep", profile="dev"),
        dsn="postgres://postgres:okg@127.0.0.1:5433/okg_workspace_abc123",
        resolution={
            "target": {
                "database": "okg_workspace_abc123",
                "identity": "isolated",
            },
        },
    )

    result = deployment_upgrade._ensure_dev_isolated_database(ctx)

    assert result == {
        "ok": True,
        "database": "okg_workspace_abc123",
        "created": True,
        "maintenance_database": "postgres",
    }
    assert executed[0] == (
        "SELECT 1 FROM pg_database WHERE datname = %s",
        ("okg_workspace_abc123",),
    )
    assert "CREATE DATABASE" in executed[1][0]


def test_dev_database_bootstrap_refuses_protected_database() -> None:
    from okg.substrate import deployment_upgrade

    ctx = SimpleNamespace(
        options=UpgradeOptions(deployment="dep", profile="dev"),
        dsn="postgres://postgres:okg@127.0.0.1:5433/okg_workspace",
        resolution={
            "target": {
                "database": "okg_workspace",
                "identity": "ok",
            },
        },
    )

    result = deployment_upgrade._ensure_dev_isolated_database(ctx)

    assert result["ok"] is False
    assert result["reason"] == "dev database target is not isolated"
    assert result["database"] == "okg_workspace"


def test_dev_database_bootstrap_refuses_mismatch_without_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade

    def fail_connect(*args, **kwargs):
        raise AssertionError("non-isolated targets must fail before connecting")

    monkeypatch.setattr("psycopg.connect", fail_connect)
    ctx = SimpleNamespace(
        options=UpgradeOptions(deployment="dep", profile="dev"),
        dsn="postgres://postgres:okg@127.0.0.1:5433/not_an_isolated_clone",
        resolution={
            "target": {
                "database": "not_an_isolated_clone",
                "identity": "mismatch",
            },
        },
    )

    result = deployment_upgrade._ensure_dev_isolated_database(ctx)

    assert result["ok"] is False
    assert result["reason"] == "dev database target is not isolated"
    assert result["target"]["identity"] == "mismatch"


def test_dev_database_bootstrap_protects_the_configured_databases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Extra protected databases come from repository configuration (#2823)."""
    from okg.substrate import deployment_upgrade
    from okg.substrate.deployment_default import DeploymentFleet

    def fail_connect(*args, **kwargs):
        raise AssertionError("a protected database must fail before connecting")

    monkeypatch.setattr("psycopg.connect", fail_connect)

    def ctx_for(database: str) -> SimpleNamespace:
        return SimpleNamespace(
            options=UpgradeOptions(deployment="dep", profile="dev"),
            dsn=f"postgres://postgres:okg@127.0.0.1:5433/{database}",
            resolution={"target": {"database": database, "identity": "isolated"}},
        )

    monkeypatch.setattr(
        deployment_upgrade,
        "_code_checkout_fleet",
        lambda: DeploymentFleet(source="cfg", protected_databases=("team_graph",)),
    )
    for database in ("team_graph", "postgres", "okg"):
        result = deployment_upgrade._ensure_dev_isolated_database(ctx_for(database))
        assert result["ok"] is False, database
        assert result["reason"] == "dev database auto-create target is not isolated"
        assert result["protected_databases_config"] == "cfg"

    monkeypatch.setattr(
        deployment_upgrade, "_code_checkout_fleet", lambda: DeploymentFleet()
    )
    # With nothing configured only the server databases are protected: the
    # name is no longer built into the package.
    result = deployment_upgrade._ensure_dev_isolated_database(ctx_for("postgres"))
    assert result["ok"] is False
    assert result["protected_databases_config"] == "none configured"


def test_okg_up_all_refuses_without_a_configured_kept_fleet(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    from okg.substrate.cli.main import main

    def no_run(_options):
        raise AssertionError("an unconfigured fleet must not be swept")

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade.run_deployment_fleet_upgrade", no_run
    )
    monkeypatch.chdir(tmp_path)

    rc = main(["up", "--all", "--target", "dev", "--skip-backup", "--json"])

    captured = capsys.readouterr()
    assert rc != 0
    error = json.loads(captured.err or captured.out)["error"]
    assert error["message"] == "okg up --all has no kept fleet to upgrade"
    assert "deployment_fleet" in error["fix_hint"]


def test_dev_plan_preflight_does_not_connect_or_create_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_connect(*args, **kwargs):
        raise AssertionError("plan-only dev preflight must not connect")

    monkeypatch.setattr("psycopg.connect", fail_connect)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": "postgres://postgres:okg@127.0.0.1:5433/okg_workspace_plan",
            "authority": {"mode": "dev_isolated"},
            "target": {
                "deployment": "dep",
                "authority_mode": "dev_isolated",
                "database": "okg_workspace_plan",
                "identity": "isolated",
                "expected_database": "okg_workspace",
            },
        },
    )

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        raise AssertionError(f"plan-only preflight should not run commands: {argv}")

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            profile="dev",
            plan_only=True,
            apply_services=False,
            sync_code=False,
            sync_deps=False,
            state_file=tmp_path / "up-state.json",
            stop_after="preflight",
        ),
        command_runner=runner,
    )

    assert result["stopped_after"] == "preflight"
    preflight = result["phases"][0]
    assert preflight["ok"] is True
    assert preflight["dev_database"]["skipped"] == (
        "dev database bootstrap skipped (--plan)"
    )
    assert preflight["migration_status"]["planned"] is True


def test_upgrade_resume_restarts_when_resolved_target_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_file = tmp_path / "up-state.json"
    state_file.write_text(
        json.dumps(
            {
                "deployment": "dep",
                "profile": "dev",
                "target_key": {
                    "deployment": "dep",
                    "authority_mode": "dev_isolated",
                    "database": "old_isolated",
                    "identity": "isolated",
                },
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
                "completed": False,
                "completed_phases": ["preflight"],
                "phase_results": [{"phase": "preflight", "ok": True, "old": True}],
                "failed_phase": None,
                "next_phase": "backup",
                "lifecycle_contract_digest": CANONICAL_CONTRACT.digest,
                "lifecycle_contract_version": CANONICAL_CONTRACT.contract_version,
            }
        )
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": "postgres://postgres:okg@127.0.0.1:5433/new_isolated",
            "authority": {"mode": "dev_isolated"},
            "target": {
                "deployment": "dep",
                "authority_mode": "dev_isolated",
                "database": "new_isolated",
                "identity": "isolated",
            },
        },
    )
    preflight_ran = False

    def bootstrap(ctx) -> dict:
        nonlocal preflight_ran
        preflight_ran = True
        return {
            "ok": True,
            "database": "new_isolated",
            "created": False,
            "maintenance_database": "postgres",
        }

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._ensure_dev_isolated_database",
        bootstrap,
    )

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        if "migrations" in argv:
            return _ok({"pending": [], "destructive_pending": []})
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            profile="dev",
            state_file=state_file,
            skip_backup=True,
            stop_after="preflight",
        ),
        command_runner=runner,
    )

    assert preflight_ran is True
    assert len(result["phases"]) == 1
    assert result["phases"][0]["phase"] == "preflight"
    assert "old" not in result["phases"][0]


def test_upgrade_apply_does_not_resume_completed_plan_phases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_file = tmp_path / "up-state.json"
    target_key = {
        "deployment": "dep",
        "authority_mode": "dev_isolated",
        "database": "okg_workspace_plan",
        "identity": "isolated",
    }
    state_file.write_text(
        json.dumps(
            {
                "deployment": "dep",
                "profile": "dev",
                "target_key": target_key,
                "plan_only": True,
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
                "completed": True,
                "completed_phases": ["preflight"],
                "phase_results": [
                    {
                        "phase": "preflight",
                        "ok": True,
                        "planned": "CREATE DATABASE okg_workspace_plan if missing",
                    }
                ],
                "failed_phase": None,
                "next_phase": None,
                "lifecycle_contract_digest": CANONICAL_CONTRACT.digest,
                "lifecycle_contract_version": CANONICAL_CONTRACT.contract_version,
            }
        )
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": "postgres://postgres:okg@127.0.0.1:5433/okg_workspace_plan",
            "authority": {"mode": "dev_isolated"},
            "target": target_key,
        },
    )
    preflight_ran = False

    def bootstrap(ctx) -> dict:
        nonlocal preflight_ran
        preflight_ran = True
        assert ctx.options.plan_only is False
        return {
            "ok": True,
            "database": "okg_workspace_plan",
            "created": False,
            "maintenance_database": "postgres",
        }

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._ensure_dev_isolated_database",
        bootstrap,
    )

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        if "migrations" in argv:
            return _ok({"pending": [], "destructive_pending": []})
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            profile="dev",
            state_file=state_file,
            skip_backup=True,
            stop_after="preflight",
        ),
        command_runner=runner,
    )

    assert preflight_ran is True
    assert len(result["phases"]) == 1
    assert result["phases"][0]["phase"] == "preflight"
    assert "planned" not in result["phases"][0]


def test_upgrade_refuses_unversioned_completed_phase_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_file = tmp_path / "up-state.json"
    target_key = {
        "deployment": "dep",
        "authority_mode": "dev_isolated",
        "database": "okg_workspace_old_state",
        "identity": "isolated",
    }
    state_file.write_text(
        json.dumps(
            {
                "deployment": "dep",
                "profile": "dev",
                "target_key": target_key,
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
                "completed": False,
                "completed_phases": ["preflight"],
                "phase_results": [{"phase": "preflight", "ok": True}],
                "failed_phase": None,
                "next_phase": "backup",
            }
        )
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": "postgres://postgres:okg@127.0.0.1:5433/okg_workspace_old_state",
            "authority": {"mode": "dev_isolated"},
            "target": target_key,
        },
    )
    preflight_ran = False

    def bootstrap(ctx) -> dict:
        nonlocal preflight_ran
        preflight_ran = True
        return {
            "ok": True,
            "database": "okg_workspace_old_state",
            "created": False,
            "maintenance_database": "postgres",
        }

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._ensure_dev_isolated_database",
        bootstrap,
    )

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        if "migrations" in argv:
            return _ok({"pending": [], "destructive_pending": []})
        return _ok()

    with pytest.raises(
        SubstrateError,
        match="unknown or changed lifecycle contract",
    ):
        run_deployment_upgrade(
            UpgradeOptions(
                deployment="dep",
                profile="dev",
                state_file=state_file,
                skip_backup=True,
                stop_after="preflight",
            ),
            command_runner=runner,
        )
    assert preflight_ran is False


@pytest.fixture
def identity_deployments(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Three synthetic deployments that declare their databases.

    ``staging-demo`` requires the exact declared database, the way a
    dedicated staging deployment does; ``prod-demo`` and ``dev-demo`` do not.
    They replace this repository's own deployments as the vehicle, so the
    public copy runs the identity rules too.
    """
    root = tmp_path / "deployments"
    for name, database, exact in (
        ("staging-demo", "staging_demo", True),
        ("prod-demo", "prod_demo", False),
        ("dev-demo", "dev_demo", False),
    ):
        (root / name).mkdir(parents=True)
        document = {"name": name, "postgres": {"dsn": "${OKG_DSN}", "database": database}}
        if exact:
            document["deployment_up"] = {"require_exact_declared_database": True}
        (root / name / "deployment.yaml").write_text(yaml.safe_dump(document))
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    return root


def test_dev_upgrade_resolves_isolated_target_identity(
    monkeypatch: pytest.MonkeyPatch,
    identity_deployments: Path,
) -> None:
    from okg.substrate import deployment_upgrade

    monkeypatch.setattr(
        "okg.substrate.catalog.ownership.suggested_worktree_dsn",
        lambda deployment_name: (
            "postgres://postgres:okg@127.0.0.1:5433/dev_demo_abc123"
        ),
    )

    result = deployment_upgrade._resolve_target(
        UpgradeOptions(deployment="dev-demo", profile="dev"),
    )

    assert result["authority"] == {"mode": "dev_isolated"}
    assert result["target"]["database"] == "dev_demo_abc123"
    assert result["target"]["expected_database"] == "dev_demo"
    assert result["target"]["identity"] == "isolated"


def test_prod_upgrade_resolves_deployment_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade

    captured: dict[str, object] = {}

    def fake_resolve(
        deployment: str,
        *,
        explicit_dsn: str | None = None,
        local_profile_authoritative: bool = False,
    ) -> dict[str, object]:
        captured["deployment"] = deployment
        captured["explicit_dsn"] = explicit_dsn
        captured["local_profile_authoritative"] = local_profile_authoritative
        return {"dsn": "postgres://authority/dep", "target": {"identity": "ok"}}

    monkeypatch.setattr(
        "okg.substrate.deployment_authority.resolve_deployment_dsn_authority",
        fake_resolve,
    )

    result = deployment_upgrade._resolve_target(
        UpgradeOptions(
            deployment="dep",
            profile="prod",
            dsn="postgres://override/dep",
        ),
    )

    assert result["dsn"] == "postgres://authority/dep"
    assert captured == {
        "deployment": "dep",
        "explicit_dsn": "postgres://override/dep",
        "local_profile_authoritative": False,
    }


def test_dedicated_staging_upgrade_requires_exact_declared_database(
    identity_deployments: Path,
) -> None:
    from okg.substrate import deployment_upgrade

    exact = deployment_upgrade._resolve_target(
        UpgradeOptions(
            deployment="staging-demo",
            profile="prod",
            dsn="postgresql://okg@127.0.0.1:55437/staging_demo",
        ),
    )
    isolated = deployment_upgrade._resolve_target(
        UpgradeOptions(
            deployment="staging-demo",
            profile="prod",
            dsn="postgresql://okg@127.0.0.1:55437/staging_demo_pr702",
        ),
    )
    mismatched = deployment_upgrade._resolve_target(
        UpgradeOptions(
            deployment="staging-demo",
            profile="prod",
            dsn="postgresql://okg@127.0.0.1:55437/other",
        ),
    )

    assert exact["dsn"].endswith("/staging_demo")
    assert exact["target"]["identity"] == "ok"
    assert isolated["dsn"] is None
    assert isolated["target"]["identity"] == "isolated"
    assert "requires the exact declared database" in isolated["reason"]
    assert mismatched["dsn"] is None
    assert mismatched["target"]["identity"] == "mismatch"


def test_generic_prod_upgrade_still_accepts_isolated_identity(
    identity_deployments: Path,
) -> None:
    from okg.substrate import deployment_upgrade

    result = deployment_upgrade._resolve_target(
        UpgradeOptions(
            deployment="prod-demo",
            profile="prod",
            dsn="postgresql://okg@127.0.0.1:55436/prod_demo_pr702",
        ),
    )

    assert result["dsn"].endswith("/prod_demo_pr702")
    assert result["target"]["identity"] == "isolated"


def test_okg_ci_dev_is_the_deployment_that_requires_its_exact_database() -> None:
    """The real configuration the synthetic staging-demo above stands in for.

    okg-ci-dev is this repository's own deployment; the public copy deselects
    this test (release/public-mirror/overlay/tests/mirror_deselect.txt).
    """
    from okg.substrate import fastyaml

    manifest = fastyaml.safe_load(
        (Path(__file__).resolve().parents[2] / "deployments/okg-ci-dev/deployment.yaml").read_text()
    )
    assert manifest["deployment_up"]["require_exact_declared_database"] is True
    assert manifest["postgres"]["database"] == "okg_ci_dev"


def test_fleet_upgrade_reports_each_deployment_without_aborting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_recovery_phases(monkeypatch)
    _stub_verified_backup_boundary(monkeypatch)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": f"postgres://user@host/{options.deployment}",
            "target": {"database": options.deployment},
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(
            deployment_dir=tmp_path / ctx.options.deployment,
        ),
    )
    seen_releases: list[str] = []

    def _deployment_after(token: str, argv: list[str]) -> str:
        return argv[argv.index(token) + 1]

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if argv[:4] == running_okg_argv():
            if "release" in argv:
                deployment = _deployment_after("release", argv)
                seen_releases.append(deployment)
                return _ok({"ok": True, "deployment": deployment, "channel": "dev"})
            if "migrations" in argv:
                return _ok({"pending": [], "destructive_pending": []})
            if "migrate" in argv:
                deployment = _deployment_after("--deployment", argv)
                if deployment == "bad":
                    return CommandResult(
                        2,
                        json.dumps({"ok": False, "deployment": deployment}),
                        "migrate failed",
                    )
                return _ok({"ok": True, "deployment": deployment})
            if "ingest" in argv:
                deployment = _deployment_after("--deployment", argv)
                return _ok(_published(f"gen-{deployment}"))
            if "ready" in argv:
                deployment = _deployment_after("ready", argv)
                return _ok({
                    "ok": True,
                    "deployment": deployment,
                    "generation": {"generation_id": f"gen-{deployment}"},
                })
        return _ok()

    result = run_deployment_fleet_upgrade(
        FleetUpgradeOptions(
            deployments=("good", "bad", "good2"),
            profile="prod",
            skip_backup=True,
            state_dir=tmp_path / "state",
        ),
        command_runner=runner,
    )

    assert result["ok"] is False
    assert result["summary"] == {
        "total": 3,
        "succeeded": 2,
        "failed": 1,
        "stopped": 0,
        "retired": 0,
        "waived": 0,
        "succeeded_deployments": ["good", "good2"],
        "failed_deployments": ["bad"],
        "stopped_deployments": [],
        "retired_deployments": [],
        "waived_deployments": [],
    }
    assert [row["deployment"] for row in result["deployments"]] == [
        "good",
        "bad",
        "good2",
    ]
    bad = result["deployments"][1]
    assert bad["ok"] is False
    assert bad["status"] == "failed"
    assert bad["upgrade"]["failed_phase"] == "migrate"
    assert result["deployments"][2]["smoke_generation"] == "gen-good2"
    assert seen_releases.count("good") == 2
    assert seen_releases.count("bad") == 1
    assert seen_releases.count("good2") == 2


def test_fleet_upgrade_records_retired_and_waived_without_running(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_recovery_phases(monkeypatch)
    _stub_verified_backup_boundary(monkeypatch)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": f"postgres://user@host/{options.deployment}",
            "target": {"database": options.deployment},
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(
            deployment_dir=tmp_path / ctx.options.deployment,
        ),
    )
    calls: list[str] = []

    def _deployment_after(token: str, argv: list[str]) -> str:
        return argv[argv.index(token) + 1]

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if argv[:4] == running_okg_argv():
            if "release" in argv:
                deployment = _deployment_after("release", argv)
                calls.append(f"release:{deployment}")
                return _ok({"ok": True, "deployment": deployment, "channel": "dev"})
            if "migrations" in argv:
                return _ok({"pending": [], "destructive_pending": []})
            if "migrate" in argv:
                return _ok({"ok": True})
            if "ingest" in argv:
                deployment = _deployment_after("--deployment", argv)
                return _ok(_published(f"gen-{deployment}"))
            if "ready" in argv:
                deployment = _deployment_after("ready", argv)
                return _ok({
                    "ok": True,
                    "deployment": deployment,
                    "generation": {"generation_id": f"gen-{deployment}"},
                })
        return _ok()

    result = run_deployment_fleet_upgrade(
        FleetUpgradeOptions(
            deployments=("good", "retired", "waived"),
            profile="prod",
            skip_backup=True,
            state_dir=tmp_path / "state",
            dispositions={
                "retired": FleetDisposition(
                    status="retired",
                    reason="deployment was intentionally decommissioned",
                    evidence="pact://retired",
                ),
                "waived": FleetDisposition(
                    status="waived",
                    reason="database is unavailable in this operator environment",
                ),
            },
        ),
        command_runner=runner,
    )

    assert result["ok"] is True
    assert result["summary"] == {
        "total": 3,
        "succeeded": 1,
        "failed": 0,
        "stopped": 0,
        "retired": 1,
        "waived": 1,
        "succeeded_deployments": ["good"],
        "failed_deployments": [],
        "stopped_deployments": [],
        "retired_deployments": ["retired"],
        "waived_deployments": ["waived"],
    }
    assert [row["status"] for row in result["deployments"]] == [
        "succeeded",
        "retired",
        "waived",
    ]
    assert result["deployments"][1]["disposition"]["evidence"] == "pact://retired"
    assert calls == ["release:good", "release:good"]


def test_load_fleet_dispositions_requires_reason(tmp_path: Path) -> None:
    path = tmp_path / "fleet-dispositions.yaml"
    path.write_text("deployments:\n  retired:\n    status: retired\n")

    with pytest.raises(Exception, match="requires a reason"):
        load_fleet_dispositions(path)


def test_top_level_okg_up_dispatches_to_upgrade_runner(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from okg.substrate.cli.main import main

    captured = {}

    def fake_run(options):
        captured["options"] = options
        return {
            "ok": True,
            "deployment": options.deployment,
            "profile": options.profile,
            "phases": [],
        }

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade.run_deployment_upgrade",
        fake_run,
    )

    rc = main(
        [
            "up",
            "dep",
            "--target",
            "dev",
            "--skip-backup",
            "--json",
        ]
    )

    assert rc == 0
    assert captured["options"].deployment == "dep"
    assert captured["options"].profile == "dev"
    assert captured["options"].skip_backup is True
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True


def test_up_aliases_expose_identical_restore_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from okg.substrate.cli.main import main

    captured: list[UpgradeOptions] = []

    def fake_run(options: UpgradeOptions) -> dict:
        captured.append(options)
        return {"ok": True, "deployment": options.deployment, "phases": []}

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade.run_deployment_upgrade", fake_run
    )
    receipt = tmp_path / "receipt.json"
    lifecycle = tmp_path / "lifecycle.json"
    common = [
        "dep",
        "--verified-backup-state", str(receipt),
        "--verified-backup-source-revision", "deadbeef",
        "--lifecycle-plan", str(lifecycle),
        "--confirm-safety-plan", "sha256:plan",
        "--skip-backup",
        "--json",
    ]

    assert main(["up", *common]) == 0
    capsys.readouterr()
    assert main(["deployment", "up", *common]) == 0
    capsys.readouterr()

    assert len(captured) == 2
    fields = (
        "verified_backup_state",
        "verified_backup_source_revision",
        "lifecycle_plan",
        "confirm_safety_plan",
        "skip_backup",
    )
    assert {field: getattr(captured[0], field) for field in fields} == {
        field: getattr(captured[1], field) for field in fields
    }


def test_top_level_okg_up_all_dispatches_to_fleet_runner(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    from okg.substrate.cli.main import main

    # The kept fleet is repository configuration (#2823).
    config = tmp_path / ".okg" / "coordination.yaml"
    config.parent.mkdir()
    config.write_text(
        "deployment_fleet:\n  kept: [alpha, kdflex]\n", encoding="utf-8"
    )
    monkeypatch.chdir(tmp_path)
    captured = {}

    def fake_run(options):
        captured["options"] = options
        return {
            "ok": True,
            "mode": "fleet",
            "deployments": [],
            "summary": {"total": 0, "succeeded": 0, "failed": 0},
        }

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade.run_deployment_fleet_upgrade",
        fake_run,
    )
    dispositions = tmp_path / "fleet-dispositions.yaml"
    dispositions.write_text(
        "deployments:\n"
        "  kdflex:\n"
        "    status: waived\n"
        "    reason: unavailable in this environment\n"
    )

    rc = main(
        [
            "up",
            "--all",
            "--target",
            "dev",
            "--skip-backup",
            "--fleet-dispositions",
            str(dispositions),
            "--json",
        ]
    )

    assert rc == 0
    assert captured["options"].deployments == ("alpha", "kdflex")
    assert captured["options"].profile == "dev"
    assert captured["options"].skip_backup is True
    assert captured["options"].dispositions["kdflex"].status == "waived"
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "fleet"


# ---------------------------------------------------------------------------
# req.proof.fail-closed-output — unparseable gate output is a FAILURE
# ---------------------------------------------------------------------------


def _patch_common(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _stub_recovery_phases(monkeypatch)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": "postgres://user@host/db",
            "target": {"database": "db", "identity": "ok"},
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(deployment_dir=tmp_path / "dep-root"),
    )


def test_upgrade_preflight_fails_closed_on_unparseable_migration_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A zero-exit migration status whose JSON does not parse must HALT the
    gate with a could-not-read reason — never coerce to an empty payload that
    silently reports 'no destructive migrations'."""
    _patch_common(monkeypatch, tmp_path)

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if "migrations" in argv:
            # Exit 0, but the stdout is NOT valid JSON.
            return CommandResult(0, "definitely not json {{{", "")
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "up-state.json",
            skip_backup=True,
        ),
        command_runner=runner,
    )

    assert result["ok"] is False
    assert result["failed_phase"] == "preflight"
    preflight = result["phases"][-1]
    assert preflight["phase"] == "preflight"
    assert preflight["ok"] is False
    assert preflight["reason"] == "could not read migration status"
    assert preflight.get("unparseable_output") is True
    # Crucially: it did NOT report a (coerced-empty) "no destructive migrations".
    assert "destructive_pending" not in preflight


def test_upgrade_smoke_publish_fails_closed_on_unparseable_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_verified_backup_boundary(monkeypatch)
    """A zero-exit smoke publish whose JSON does not parse must HALT the gate."""
    _patch_common(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if "migrations" in argv:
            return _ok({"pending": [], "destructive_pending": []})
        if "ingest" in argv:
            return CommandResult(0, "not json at all", "")
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "up-state.json",
            skip_backup=True,
        ),
        command_runner=runner,
    )

    assert result["ok"] is False
    assert result["failed_phase"] == "smoke_publish"
    smoke = result["phases"][-1]
    assert smoke["ok"] is False
    assert smoke["reason"] == "could not read smoke publish output"


def test_upgrade_smoke_publish_fails_on_blocked_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_verified_backup_boundary(monkeypatch)
    """The uniform real-publish gate fails a blocked (not published) generation,
    even when the publish command exited 0."""
    _patch_common(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count",
        lambda dsn, **_kwargs: (0, {}),
    )

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if "migrations" in argv:
            return _ok({"pending": [], "destructive_pending": []})
        if "ingest" in argv:
            return _ok({
                "publishes": [
                    {"status": "blocked", "generation_id": "gen-blocked"},
                ]
            })
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "up-state.json",
            skip_backup=True,
        ),
        command_runner=runner,
    )

    assert result["ok"] is False
    assert result["failed_phase"] == "smoke_publish"
    smoke = result["phases"][-1]
    assert smoke["ok"] is False
    assert "was not published" in smoke["reason"]
    assert smoke["publish_proof"]["status"] == "blocked"


# ---------------------------------------------------------------------------
# req.proof.honest-up — sync ON by default; partial status on any skip
# ---------------------------------------------------------------------------


def test_upgrade_default_applies_sync_phases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_verified_backup_boundary(monkeypatch)
    """A default `okg up` (sync ON) applies sync_code and sync_deps and reports
    ok:true / status:applied — it does NOT leave the worker on stale code."""
    _patch_common(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count",
        lambda dsn, **_kwargs: (0, {}),
    )
    ran: list[str] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if argv[:2] == ["uv", "sync"]:
            ran.append("uv-sync")
            return _ok()
        if argv[:4] == running_okg_argv():
            if "migrations" in argv:
                return _ok({"pending": [], "destructive_pending": []})
            if "ingest" in argv:
                return _ok(_published("gen-applied"))
            if "ready" in argv:
                return _ok({"generation": {"generation_id": "gen-applied"}})
        return _ok()

    # Defaults: sync_code/sync_deps/apply_services all ON.
    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "up-state.json",
            skip_backup=True,
        ),
        command_runner=runner,
    )

    assert result["ok"] is True
    assert result["status"] == "applied"
    assert "uv-sync" in ran
    dep_sync = next(
        row for row in result["phases"] if row["phase"] == "dependency_sync"
    )
    project_root = _planner_project_root()
    assert dep_sync["command"] == f"uv sync --project {project_root} --frozen"
    assert dep_sync["project_root"] == str(project_root)
    assert dep_sync["environment"] == str(project_root / ".venv")
    assert "skipped" not in dep_sync


def test_upgrade_syncs_deployment_owned_python_extras(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_verified_backup_boundary(monkeypatch)
    _patch_common(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(
            deployment_dir=tmp_path / "dep-root",
            runtime=SimpleNamespace(
                python_extras=("quality", "code"),
                smoke_sources=("pact",),
                smoke_reset_cursor=True,
            ),
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count",
        lambda dsn, **_kwargs: (0, {}),
    )
    calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        calls.append(argv)
        if argv[:4] == running_okg_argv():
            if "migrations" in argv:
                return _ok({"pending": [], "destructive_pending": []})
            if "ingest" in argv:
                return _ok(_published("gen-extras"))
            if "ready" in argv:
                return _ok({"generation": {"generation_id": "gen-extras"}})
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "up-state.json",
            skip_backup=True,
        ),
        command_runner=runner,
    )

    assert result["ok"] is True
    project_root = _planner_project_root()
    assert [
        "uv", "sync", "--project", str(project_root), "--frozen",
        "--extra", "quality", "--extra", "code",
    ] in calls
    dep_sync = next(
        row for row in result["phases"] if row["phase"] == "dependency_sync"
    )
    assert dep_sync["python_extras"] == ["quality", "code"]
    ingest_call = next(call for call in calls if "ingest" in call)
    assert ingest_call[-3:] == ["--include", "pact", "--reset-cursor"]


def test_upgrade_skipped_mutate_phase_yields_partial_with_finishing_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_verified_backup_boundary(monkeypatch)
    """Deliberately skipping a mutate phase (here dependency sync) yields
    ok:false / status:partial with the exact finishing command, so a bare
    `okg up` can never report full success while leaving stale deps."""
    _patch_common(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _applied_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count",
        lambda dsn, **_kwargs: (0, {}),
    )

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if argv[:4] == running_okg_argv():
            if "migrations" in argv:
                return _ok({"pending": [], "destructive_pending": []})
            if "ingest" in argv:
                return _ok(_published("gen-partial"))
            if "ready" in argv:
                return _ok({"generation": {"generation_id": "gen-partial"}})
        return _ok()

    state_file = tmp_path / "up-state.json"
    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=state_file,
            skip_backup=True,
            sync_deps=False,  # operator opted out of dependency sync
        ),
        command_runner=runner,
    )

    assert result["ok"] is False
    assert result["status"] == "partial"
    assert "dependency_sync" in result["skipped_mutate_phases"]
    finishing = result["next_command"]
    assert "--sync-deps" in finishing
    assert "deployment up dep" in finishing
    assert "uv run" not in finishing
    # The skipped phase itself completed (it just did not mutate).
    dep_sync = next(
        row for row in result["phases"] if row["phase"] == "dependency_sync"
    )
    assert dep_sync["ok"] is True
    assert "skipped" in dep_sync


def test_upgrade_plan_only_is_a_preview_not_a_partial(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`--plan` (plan_only) is the opt-in preview: every mutate phase reports
    what it would do without applying, and the run is a plan (ok:true,
    status:plan), NOT a partial failure."""
    _patch_common(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        _planned_sync_code,
    )
    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count",
        lambda dsn, **_kwargs: (0, {}),
    )
    ran: list[str] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if argv[:2] == ["uv", "sync"]:
            ran.append("uv-sync")
            return _ok()
        if argv[:4] == running_okg_argv():
            if "migrations" in argv:
                return _ok({"pending": [], "destructive_pending": []})
            if "ingest" in argv:
                return _ok(_published("gen-plan"))
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "up-state.json",
            skip_backup=True,
            plan_only=True,
            apply_services=False,
            sync_code=False,
            sync_deps=False,
        ),
        command_runner=runner,
    )

    assert result["ok"] is True
    assert result["status"] == "plan"
    assert "skipped_mutate_phases" not in result
    # The plan preview did NOT run the mutating uv sync.
    assert "uv-sync" not in ran
    dep_sync = next(
        row for row in result["phases"] if row["phase"] == "dependency_sync"
    )
    project_root = _planner_project_root()
    assert dep_sync["planned"] == f"uv sync --project {project_root} --frozen"
    assert dep_sync["project_root"] == str(project_root)


def test_top_level_okg_up_plan_flag_sets_plan_only_and_disables_mutates(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`okg up <dep> --plan` maps to plan_only with mutate phases disabled."""
    from okg.substrate.cli.main import main

    captured: dict[str, object] = {}

    def fake_run(options):
        captured["options"] = options
        return {"ok": True, "status": "plan", "deployment": options.deployment,
                "phases": []}

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade.run_deployment_upgrade",
        fake_run,
    )

    rc = main(["up", "dep", "--target", "dev", "--plan", "--json"])

    assert rc == 0
    opts = captured["options"]
    assert opts.plan_only is True
    assert opts.sync_code is False
    assert opts.sync_deps is False
    assert opts.apply_services is False


def test_top_level_okg_up_defaults_sync_on(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A bare `okg up <dep>` defaults sync_code/sync_deps/apply_services ON."""
    from okg.substrate.cli.main import main

    captured: dict[str, object] = {}

    def fake_run(options):
        captured["options"] = options
        return {"ok": True, "status": "applied", "deployment": options.deployment,
                "phases": []}

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade.run_deployment_upgrade",
        fake_run,
    )

    rc = main(["up", "dep", "--target", "dev", "--skip-backup", "--json"])

    assert rc == 0
    opts = captured["options"]
    assert opts.plan_only is False
    assert opts.sync_code is True
    assert opts.sync_deps is True
    assert opts.apply_services is True


class _SchemaProbeCursor:
    """Minimal cursor stub for the `to_regclass` schema probe."""

    def __init__(self, ledger: object) -> None:
        self._ledger = ledger
        self.queries: list[str] = []

    def __enter__(self) -> "_SchemaProbeCursor":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def execute(self, query: str, *args: object) -> None:
        self.queries.append(query)

    def fetchone(self) -> tuple[object]:
        return (self._ledger,)


class _SchemaProbeConnection:
    def __init__(self, ledger: object) -> None:
        self._ledger = ledger

    def __enter__(self) -> "_SchemaProbeConnection":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def cursor(self) -> _SchemaProbeCursor:
        return _SchemaProbeCursor(self._ledger)


def _dev_isolated_schema_ctx(
    commands: list[list[str]],
    *,
    profile: str = "dev",
    identity: str = "isolated",
    plan_only: bool = False,
    migrate_ok: bool = True,
) -> SimpleNamespace:
    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        commands.append(argv)
        return CommandResult(
            0 if migrate_ok else 1, json.dumps({"ok": migrate_ok}), ""
        )

    return SimpleNamespace(
        options=SimpleNamespace(
            profile=profile,
            plan_only=plan_only,
            deployment="okg-workspace",
        ),
        resolution={
            "target": {"identity": identity, "database": "okg_workspace_abc"},
        },
        dsn="postgresql://user:secret@host/okg_workspace_abc",
        env={},
        command_runner=runner,
        data={},
    )


def _migrate_commands(commands: list[list[str]]) -> list[list[str]]:
    return [c for c in commands if c and "migrate" in c]


def test_dev_isolated_schema_installs_on_empty_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A provably-empty isolated dev clone is migrated before classification."""
    from okg.substrate import deployment_upgrade as du
    import psycopg

    # `to_regclass('okg.schema_migrations')` IS NULL -> never migrated.
    monkeypatch.setattr(
        psycopg, "connect", lambda *a, **k: _SchemaProbeConnection(None)
    )
    commands: list[list[str]] = []
    ctx = _dev_isolated_schema_ctx(commands)

    result = du._ensure_dev_isolated_schema(ctx)

    assert result == {
        "ok": True,
        "initialized": True,
        "schema_installed": True,
        "database": "okg_workspace_abc",
    }
    migrates = _migrate_commands(commands)
    assert len(migrates) == 1
    assert migrates[0][-3:] == ["--deployment", "okg-workspace", "--apply"]


def test_dev_isolated_schema_partial_database_not_repaired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ledger-present (partial or full) clone is never auto-repaired here."""
    from okg.substrate import deployment_upgrade as du
    import psycopg

    # `to_regclass` returns a non-NULL oid -> ledger already exists.
    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *a, **k: _SchemaProbeConnection("okg.schema_migrations"),
    )
    commands: list[list[str]] = []
    ctx = _dev_isolated_schema_ctx(commands)

    result = du._ensure_dev_isolated_schema(ctx)

    assert result == {
        "ok": True,
        "initialized": False,
        "schema_installed": False,
    }
    assert _migrate_commands(commands) == []


def test_dev_isolated_schema_canonical_target_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Canonical/prod (non-dev, non-isolated) targets never reach install."""
    from okg.substrate import deployment_upgrade as du
    import psycopg

    def forbidden_connect(*a: object, **k: object) -> object:
        raise AssertionError("canonical target must not probe the database")

    monkeypatch.setattr(psycopg, "connect", forbidden_connect)
    commands: list[list[str]] = []

    prod = du._ensure_dev_isolated_schema(
        _dev_isolated_schema_ctx(commands, profile="prod")
    )
    assert prod is None

    # Even under `--profile dev`, a non-isolated target is left untouched.
    canonical = du._ensure_dev_isolated_schema(
        _dev_isolated_schema_ctx(commands, identity="canonical")
    )
    assert canonical is None
    assert _migrate_commands(commands) == []


def test_dev_isolated_schema_probe_failure_refuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unreachable database surfaces a typed refusal, never a silent migrate."""
    from okg.substrate import deployment_upgrade as du
    import psycopg

    def broken_connect(*a: object, **k: object) -> object:
        raise RuntimeError("connection refused")

    monkeypatch.setattr(psycopg, "connect", broken_connect)
    commands: list[list[str]] = []
    ctx = _dev_isolated_schema_ctx(commands)

    result = du._ensure_dev_isolated_schema(ctx)

    assert result["ok"] is False
    assert result["reason"] == "dev database schema probe failed"
    assert result["error"]["type"] == "RuntimeError"
    assert _migrate_commands(commands) == []


def test_dev_isolated_schema_plan_only_is_noop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`--plan` never probes or migrates; it only reports the planned step."""
    from okg.substrate import deployment_upgrade as du
    import psycopg

    def forbidden_connect(*a: object, **k: object) -> object:
        raise AssertionError("plan-only must not probe the database")

    monkeypatch.setattr(psycopg, "connect", forbidden_connect)
    commands: list[list[str]] = []
    ctx = _dev_isolated_schema_ctx(commands, plan_only=True)

    result = du._ensure_dev_isolated_schema(ctx)

    assert result["ok"] is True
    assert result["schema_installed"] is False
    assert "planned" in result
    assert _migrate_commands(commands) == []


def test_dev_isolated_schema_preflight_initializes_empty_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`_phase_preflight` installs the schema on an empty isolated dev clone."""
    from okg.substrate import deployment_upgrade as du
    import psycopg

    monkeypatch.setattr(
        du,
        "_ensure_dev_isolated_database",
        lambda ctx: {
            "ok": True,
            "database": "okg_workspace_abc",
            "created": True,
        },
    )
    monkeypatch.setattr(
        psycopg, "connect", lambda *a, **k: _SchemaProbeConnection(None)
    )
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        commands.append(argv)
        if "migrate" in argv:
            return CommandResult(0, json.dumps({"ok": True}), "")
        if "inspect-effective-attribute-recovery" in argv:
            return _ok()
        # `okg db migrations` preflight after the schema is installed.
        return CommandResult(
            0,
            json.dumps({"pending": [], "destructive_pending": []}),
            "",
        )

    ctx = SimpleNamespace(
        options=SimpleNamespace(
            profile="dev",
            plan_only=False,
            deployment="okg-workspace",
            skip_destructive_migrations=False,
        ),
        resolution={
            "target": {"identity": "isolated", "database": "okg_workspace_abc"},
        },
        dsn="postgresql://user:secret@host/okg_workspace_abc",
        env={},
        command_runner=runner,
        data={},
    )

    result = du._phase_preflight(ctx)

    assert result["ok"] is True, result
    assert ctx.data["dev_schema"]["schema_installed"] is True
    assert any("migrate" in c for c in commands)
    assert any("inspect-effective-attribute-recovery" in c for c in commands)


# --- okg up restore defects (2026-09-03 okg-workspace restore) --------------
#
# Three defects observed while restoring the okg-workspace deployment: a
# receipt from a destroyed cluster sized the backup, resume revalidation
# raised a raw driver error against a database with no DBOS schema, and the
# stop phase could not read the dogfood status contract it depends on.


def _restore_receipt_snapshot() -> dict[str, object]:
    return {
        "target": {
            "deployment": "dep",
            "host": "host",
            "port": 5432,
            "database": "db",
        },
        "database_owner": {
            "deployment": "dep",
            "uuid": "6fa80e2a",
            "schema_fingerprint": "schema",
        },
        "cluster_system_identifier": "7679830469088677916",
        "server_version": "170010",
        "extension_versions": [["vector", "0.8"]],
        "schema_migrations_digest": "sha256:schema",
        "schema_catalog_digest": "sha256:schema-catalog",
        "candidate_plan_digest": "sha256:plan",
        "archive_format": "postgresql-custom",
        "timescaledb": {
            "extension_version": None,
            "hypertables": [],
            "continuous_aggregates": [],
            "hypertable_chunk_counts": [],
        },
        "dump_tool_major": 17,
        "restore_tool_major": 17,
        "backup_tool_executable_identity": {
            "pg_dump": {"resolved_path": "/usr/bin/pg_dump", "sha256": "d"},
            "pg_restore": {
                "resolved_path": "/usr/bin/pg_restore",
                "sha256": "r",
            },
        },
    }


def test_restore_drill_receipt_from_a_destroyed_cluster_does_not_size_backup(
    tmp_path: Path,
) -> None:
    from okg.substrate import deployment_upgrade as du

    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    snapshot = _restore_receipt_snapshot()
    policy = {"restore_drill_max_age_seconds": 86400}
    ctx = SimpleNamespace(
        options=SimpleNamespace(deployment="dep"),
        backup_dir=backup_dir,
    )
    du._write_restore_drill_receipt(
        ctx,
        snapshot,
        {"observed_restored_size_bytes": 70765311667},
    )

    _, _, matched_report, matched_prior = du._restore_drill_receipt_evidence(
        ctx, snapshot, policy
    )
    assert matched_prior == 70765311667
    assert "prior_size_discarded" not in matched_report

    # The volume was wiped and re-created: same host, port, database and
    # deployment, different cluster and database owner.
    live = copy.deepcopy(snapshot)
    live["cluster_system_identifier"] = "7647260204777271323"
    live["database_owner"] = {
        "deployment": "dep",
        "uuid": "1387c404",
        "schema_fingerprint": "schema",
    }
    _, _, report, prior = du._restore_drill_receipt_evidence(ctx, live, policy)

    assert prior is None, "a receipt from another cluster must not size a backup"
    assert report["prior_size_discarded"]["observed_restored_size_bytes"] == (
        70765311667
    )
    assert report["prior_size_discarded"]["reason"] == [
        "cluster_system_identifier_equal",
        "database_owner_identity_equal",
    ]


def test_restore_drill_receipt_survives_a_catalog_schema_fingerprint_change(
    tmp_path: Path,
) -> None:
    """A catalog change must not silently SHRINK the headroom demand.

    ``database_owner`` carries the schema fingerprint that catalog_apply
    rewrites. Comparing the whole row would drop the size prior on the run
    right after a catalog change, and a dropped prior only ever lowers the
    headroom the backup phase asks for.
    """
    from okg.substrate import deployment_upgrade as du

    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    snapshot = _restore_receipt_snapshot()
    policy = {"restore_drill_max_age_seconds": 86400}
    ctx = SimpleNamespace(
        options=SimpleNamespace(deployment="dep"),
        backup_dir=backup_dir,
    )
    du._write_restore_drill_receipt(
        ctx,
        snapshot,
        {"observed_restored_size_bytes": 22345395},
    )

    recatalogued = copy.deepcopy(snapshot)
    recatalogued["database_owner"] = {
        "deployment": "dep",
        "uuid": "6fa80e2a",
        "schema_fingerprint": "schema-after-catalog-apply",
    }
    _, _, report, prior = du._restore_drill_receipt_evidence(
        ctx, recatalogued, policy
    )

    assert prior == 22345395
    assert "prior_size_discarded" not in report


def test_disaster_recovery_receipt_from_a_destroyed_cluster_does_not_size_backup(
    tmp_path: Path,
) -> None:
    from okg.substrate import deployment_upgrade as du

    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    archive_path = backup_dir / "dep-current.dump"
    archive_path.write_bytes(b"verified archive")
    archive_path.chmod(0o600)
    digest = "sha256:" + hashlib.sha256(archive_path.read_bytes()).hexdigest()
    snapshot = _restore_receipt_snapshot()
    policy = {
        "disaster_recovery_archive_max_age_seconds": 86400,
        "disaster_recovery_restore_max_age_seconds": 604800,
        "recovery_point_objective_seconds": 86400.0,
    }
    ctx = SimpleNamespace(
        options=SimpleNamespace(deployment="dep", independent_backup_verify=True),
        backup_dir=backup_dir,
    )
    du._write_disaster_recovery_receipt(
        ctx,
        backup_path=archive_path,
        archive={
            "backup_digest": digest,
            "backup_size_bytes": archive_path.stat().st_size,
        },
        snapshot=snapshot,
        restore={"observed_restored_size_bytes": 70765311667},
        policy=policy,
    )

    _, _, matched_report, matched_prior = du._disaster_recovery_receipt_evidence(
        ctx, snapshot, policy
    )
    assert matched_prior == 70765311667
    assert "prior_size_discarded" not in matched_report

    live = copy.deepcopy(snapshot)
    live["cluster_system_identifier"] = "7647260204777271323"
    _, _, report, prior = du._disaster_recovery_receipt_evidence(
        ctx, live, policy
    )

    assert prior is None
    assert report["prior_size_discarded"]["reason"] == [
        "cluster_system_identifier_equal"
    ]
    assert report["prior_size_discarded"]["observed_restored_size_bytes"] == (
        70765311667
    )


def _runtime_ctx(tmp_path: Path, **data: object) -> UpgradeContext:
    state_file = tmp_path / "state.json"
    if not state_file.exists():
        state_file.write_text("{}", encoding="utf-8")
    return UpgradeContext(
        options=UpgradeOptions(deployment="okg-workspace"),
        dsn="postgres://test",
        resolution={"target": {"identity": "ok"}},
        env={"OKG_DSN": "postgres://test"},
        state_file=state_file,
        backup_dir=tmp_path,
        command_runner=lambda argv, env: _ok({"ok": True}),
        data={"runtime_enabled": True, **data},
    )


def test_runtime_quiescence_is_not_required_without_a_dbos_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    probes: list[str] = []

    def probe(ctx: UpgradeContext) -> bool:
        probes.append(ctx.options.deployment)
        return False

    monkeypatch.setattr(du, "_probe_runtime_bootstrapped", probe)
    ctx = _runtime_ctx(tmp_path)

    assert du._runtime_quiescence_disposition(ctx) == "not_bootstrapped"
    assert du._runtime_quiescence_required(ctx) is False
    report = du._runtime_not_bootstrapped_report(ctx)
    assert report["status"] == "not_bootstrapped"
    assert "okg runtime bootstrap" in report["fix_hint"]
    # Probed once and cached: `okg up` bootstraps the runtime itself when it
    # restarts services, and a requirement that flipped mid-run would have
    # readiness release a fence stop never acquired.
    assert du._runtime_quiescence_required(ctx) is False
    assert probes == ["okg-workspace"]
    assert ctx.data["runtime_bootstrapped"] is False


def test_runtime_quiescence_fails_closed_when_the_probe_cannot_answer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    def unreachable(dsn: str, **kwargs: object) -> object:
        raise OSError("connection refused")

    monkeypatch.setattr("psycopg.connect", unreachable)
    ctx = _runtime_ctx(tmp_path)

    assert du._runtime_quiescence_required(ctx) is True
    assert du._runtime_not_bootstrapped_report(ctx) is None


def test_resume_revalidation_reports_not_bootstrapped_instead_of_raising(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    def refuse(**kwargs: object) -> object:
        raise AssertionError("no queue read may run without a dbos schema")

    monkeypatch.setattr(du, "_probe_runtime_bootstrapped", lambda ctx: False)
    monkeypatch.setattr("okg.substrate.runtime.admin.client_drain", refuse)
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.promotion_fence_status",
        refuse,
    )
    ctx = _runtime_ctx(tmp_path)

    _, _, revalidation = du._revalidate_resume_postconditions(
        ctx,
        phase_results=[],
        completed=set(),
    )

    assert revalidation["ok"] is True
    quiescence = revalidation["evidence"]["runtime_fence"]["runtime_quiescence"]
    assert quiescence["status"] == "not_bootstrapped"
    assert "okg runtime bootstrap" in quiescence["fix_hint"]


def test_resume_revalidation_refuses_structurally_when_a_queue_read_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    class UndefinedTable(Exception):
        pass

    def raise_undefined(**kwargs: object) -> object:
        raise UndefinedTable('relation "dbos.workflow_status" does not exist')

    monkeypatch.setattr(du, "_probe_runtime_bootstrapped", lambda ctx: True)
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_drain", raise_undefined
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.promotion_fence_status",
        lambda **kwargs: {
            "status": "fenced",
            "promotion_id": "deploy-owner",
            "metadata": {},
        },
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.mark_promotion_recovery_blocked",
        lambda **kwargs: {"status": "recovery_blocked"},
    )
    ctx = _runtime_ctx(tmp_path, runtime_fence_owner="deploy-owner")

    with pytest.raises(SubstrateError) as excinfo:
        du._revalidate_runtime_fence_resume(ctx, completed=set())

    blocker = excinfo.value.data
    assert blocker["checks"]["queue_drained_or_owner_bound"] is False
    assert blocker["queue"]["drain_error"]["type"] == "UndefinedTable"


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (
            {
                "schema": "okg.dogfood.runtime_status.v1",
                "runtime_worker": {"status": "process_observed_no_heartbeat"},
                "runtime_processes": {
                    "ok": True,
                    "status": "single_executor",
                    "process_count": 3,
                    "root_count": 1,
                },
            },
            "running",
        ),
        (
            {
                "schema": "okg.dogfood.runtime_status.v1",
                "runtime_processes": {
                    "ok": False,
                    "status": "split_executor",
                    "root_count": 2,
                    "split_executor": True,
                },
            },
            "running",
        ),
        (
            {
                "schema": "okg.dogfood.runtime_status.v1",
                "runtime_processes": {
                    "ok": False,
                    "status": "not_observed",
                    "process_count": 0,
                    "root_count": 0,
                },
            },
            "stopped",
        ),
        (
            {
                "schema": "okg.dogfood.runtime_status.v1",
                "runtime_processes": {
                    "ok": None,
                    "status": "unavailable",
                    "reason": "ps returned 1",
                    "root_count": 0,
                },
            },
            "unknown",
        ),
        ({"schema": "okg.some.other.contract.v1", "root_count": 1}, "unknown"),
        ({"worker_state": "running"}, "unknown"),
    ],
)
def test_service_state_reads_the_dogfood_runtime_process_contract(
    payload: dict[str, object],
    expected: str,
) -> None:
    from okg.substrate import deployment_upgrade as du

    assert du._service_state_from_dogfood_status(payload) == expected


def test_observe_service_state_reads_a_blocked_dogfood_status_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    # `dogfood status` exits 1 whenever the deployment has blockers -- the
    # state a stop is called for -- while still printing its typed payload.
    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        assert "status" in argv
        return CommandResult(
            1,
            json.dumps({
                "schema": "okg.dogfood.runtime_status.v1",
                "ok": False,
                "trust_level": "blocked",
                "runtime_worker": {"status": "process_observed_no_heartbeat"},
                "runtime_processes": {
                    "ok": True,
                    "status": "single_executor",
                    "root_count": 1,
                },
            }),
            "",
        )

    def stopped_runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        return CommandResult(
            1,
            json.dumps({
                "schema": "okg.dogfood.runtime_status.v1",
                "ok": False,
                "runtime_processes": {
                    "ok": False,
                    "status": "not_observed",
                    "process_count": 0,
                    "root_count": 0,
                },
            }),
            "",
        )

    def observe(command_runner) -> str:
        monkeypatch.setattr(du, "_default_command_runner", command_runner)
        return du._observe_service_state(SimpleNamespace(
            options=SimpleNamespace(deployment="okg-workspace"),
            env={},
            state_file=tmp_path / "state.json",
            command_runner=command_runner,
            data={"production_orchestrator": True},
        ))

    assert observe(runner) == "running"
    # "stopped" is reachable only through the status contract: every
    # compatibility fallback in _observe_service_state returns "running".
    assert observe(stopped_runner) == "stopped"


def test_stop_services_reports_not_bootstrapped_and_still_stops(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    def refuse(**kwargs: object) -> object:
        raise AssertionError("no fence or drain without a dbos schema")

    monkeypatch.setattr(du, "_probe_runtime_bootstrapped", lambda ctx: False)
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.acquire_promotion_fence", refuse
    )
    monkeypatch.setattr("okg.substrate.runtime.admin.client_drain", refuse)
    stops: list[list[str]] = []
    ctx = _runtime_ctx(tmp_path)
    ctx.command_runner = lambda argv, env: (
        stops.append(argv) or _ok({"ok": True, "final_state": "stopped"})
    )

    result = du._phase_stop_services(ctx)

    assert result["ok"] is True, json.dumps(result, indent=2)
    assert result["runtime_quiescence"]["status"] == "not_bootstrapped"
    assert any("down" in argv for argv in stops)


def test_stop_services_releases_the_fence_when_state_is_unobservable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.runtime.admin import DrainResult

    releases: list[dict[str, object]] = []
    stops: list[list[str]] = []
    monkeypatch.setattr(du, "_runtime_quiescence_required", lambda ctx: True)
    monkeypatch.setattr(du, "_runtime_fence_metadata", lambda ctx: {})
    monkeypatch.setattr(du, "_observe_service_state", lambda ctx: "unknown")
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.acquire_promotion_fence",
        lambda **kwargs: {
            "status": "fenced",
            "promotion_id": kwargs["promotion_id"],
            "schedule_snapshot_digest": "sha256:snapshot",
        },
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_drain",
        lambda **kwargs: DrainResult(
            deployment=kwargs["deployment"],
            drained=True,
            in_flight_at_timeout=0,
            elapsed_seconds=36.0,
            pending_workflows=[],
        ),
    )

    def release(**kwargs: object) -> dict[str, object]:
        releases.append(kwargs)
        return {
            "status": "released",
            "released": True,
            "schedule_disposition": kwargs["schedule_disposition"],
        }

    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.release_promotion_fence", release
    )
    ctx = _runtime_ctx(tmp_path, runtime_fence_owner="deploy-owner")
    ctx.command_runner = lambda argv, env: (
        stops.append(argv) or _ok({"ok": True})
    )

    result = du._phase_stop_services(ctx)

    assert result["ok"] is False
    assert result["reason"] == "could not observe service state before stop"
    assert stops == [], "no service was stopped, so nothing was mutated"
    # The worker's scheduler waits on this fence; a refusal that keeps it
    # wedges the deployment until someone releases it by hand.
    assert releases[0]["promotion_id"] == "deploy-owner"
    assert releases[0]["schedule_disposition"] == "restore"
    reopen = result["runtime_quiescence"]["pre_mutation_reopen"]
    assert reopen["released"] is True


def test_stop_services_releases_the_fence_when_the_stop_command_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.runtime.admin import DrainResult

    releases: list[dict[str, object]] = []
    monkeypatch.setattr(du, "_runtime_quiescence_required", lambda ctx: True)
    monkeypatch.setattr(du, "_runtime_fence_metadata", lambda ctx: {})
    monkeypatch.setattr(du, "_observe_service_state", lambda ctx: "running")
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.acquire_promotion_fence",
        lambda **kwargs: {
            "status": "fenced",
            "promotion_id": kwargs["promotion_id"],
            "schedule_snapshot_digest": "sha256:snapshot",
        },
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_drain",
        lambda **kwargs: DrainResult(
            deployment=kwargs["deployment"],
            drained=True,
            in_flight_at_timeout=0,
            elapsed_seconds=1.0,
            pending_workflows=[],
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.release_promotion_fence",
        lambda **kwargs: releases.append(kwargs) or {
            "status": "released",
            "released": True,
            "schedule_disposition": kwargs["schedule_disposition"],
        },
    )
    ctx = _runtime_ctx(tmp_path, runtime_fence_owner="deploy-owner")
    ctx.command_runner = lambda argv, env: CommandResult(
        1, "", "launchctl bootout failed"
    )

    result = du._phase_stop_services(ctx)

    assert result["ok"] is False
    assert result["reason"] == "stop services failed"
    assert releases[0]["schedule_disposition"] == "restore"
    assert result["runtime_quiescence"]["post_stop_reopen"]["released"] is True


def test_stop_services_releases_the_fence_when_a_later_step_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A raised error after acquisition must not leave the fence held.

    _run_phase turns an escaping exception into a durable failure row, so a
    stop phase that raises after fencing would wedge the worker exactly as a
    silent return would.
    """
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.runtime.admin import DrainResult

    releases: list[dict[str, object]] = []
    monkeypatch.setattr(du, "_runtime_quiescence_required", lambda ctx: True)
    monkeypatch.setattr(du, "_runtime_fence_metadata", lambda ctx: {})
    monkeypatch.setattr(du, "_observe_service_state", lambda ctx: "running")

    def raise_obligation(*args: object, **kwargs: object) -> None:
        raise RuntimeError("compensation ledger is unreachable")

    monkeypatch.setattr(du, "_persist_cleanup_obligation", raise_obligation)
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.acquire_promotion_fence",
        lambda **kwargs: {
            "status": "fenced",
            "promotion_id": kwargs["promotion_id"],
            "schedule_snapshot_digest": "sha256:snapshot",
        },
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_drain",
        lambda **kwargs: DrainResult(
            deployment=kwargs["deployment"],
            drained=True,
            in_flight_at_timeout=0,
            elapsed_seconds=1.0,
            pending_workflows=[],
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.release_promotion_fence",
        lambda **kwargs: releases.append(kwargs) or {
            "status": "released",
            "released": True,
            "schedule_disposition": kwargs["schedule_disposition"],
        },
    )
    stops: list[list[str]] = []
    ctx = _runtime_ctx(tmp_path, runtime_fence_owner="deploy-owner")
    ctx.command_runner = lambda argv, env: (
        stops.append(argv) or _ok({"ok": True})
    )

    result = du._phase_stop_services(ctx)

    assert result["ok"] is False
    assert result["error"]["type"] == "RuntimeError"
    assert stops == []
    assert releases[0]["schedule_disposition"] == "restore"
    assert result["runtime_quiescence"]["pre_mutation_reopen"]["released"] is True


def test_stop_services_reraises_when_no_fence_was_taken(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    monkeypatch.setattr(du, "_runtime_quiescence_required", lambda ctx: False)
    monkeypatch.setattr(du, "_runtime_not_bootstrapped_report", lambda ctx: None)
    monkeypatch.setattr(du, "_observe_service_state", lambda ctx: "running")

    def raise_obligation(*args: object, **kwargs: object) -> None:
        raise RuntimeError("compensation ledger is unreachable")

    monkeypatch.setattr(du, "_persist_cleanup_obligation", raise_obligation)
    ctx = _runtime_ctx(tmp_path)

    with pytest.raises(RuntimeError, match="compensation ledger"):
        du._phase_stop_services(ctx)


# ---------- #1179 slice 6: provider provenance across an upgrade ----------


def _provenance_entry(module_id: str, digest: str) -> dict:
    return {
        "module_id": module_id,
        "owner": module_id.split(":", 1)[0],
        "provider_kind": "package",
        "package_release_digest": digest,
        "object_digests": [digest],
    }


def test_upgrade_effect_inventory_reports_provider_changes() -> None:
    """The report the effect inventory embeds under `catalog.module_providers`."""
    from okg.substrate.deployment_upgrade import _module_provider_report

    report = _module_provider_report(
        [_provenance_entry("archi:core", "sha256:aa")],
        [
            _provenance_entry("archi:core", "sha256:bb"),
            _provenance_entry("archi:extra", "sha256:bb"),
        ],
    )

    assert report is not None
    assert report["changes"]["added"] == ["archi:extra"]
    assert report["changes"]["removed"] == []
    assert [item["module_id"] for item in report["changes"]["digest_changed"]] == [
        "archi:core"
    ]
    assert [entry["module_id"] for entry in report["target"]] == [
        "archi:core",
        "archi:extra",
    ]


def test_upgrade_refuses_to_drop_a_packaged_owner() -> None:
    from okg.substrate.catalog.module_provenance import (
        UpgradeModuleProviderDropped,
    )
    from okg.substrate.deployment_upgrade import _module_provider_report

    with pytest.raises(UpgradeModuleProviderDropped) as excinfo:
        _module_provider_report(
            [_provenance_entry("archi:core", "sha256:aa")],
            [],
        )

    assert excinfo.value.code == "upgrade_module_provider_owner_dropped"
    assert excinfo.value.data["dropped_owners"] == ["archi"]


def test_a_builtins_only_upgrade_records_nothing_new() -> None:
    """No packaged module on either side: the inventory digest is unchanged."""
    from okg.substrate.deployment_upgrade import _module_provider_report

    assert _module_provider_report(None, []) is None
    assert _module_provider_report([], ()) is None


def test_effect_inventory_surfaces_the_owner_drop_as_a_coded_refusal(
    monkeypatch,
) -> None:
    """The refusal reaches the operator as its code, not a class name."""
    from okg.substrate.catalog.module_provenance import (
        UpgradeModuleProviderDropped,
    )

    def drop(*_args, **_kwargs):
        raise UpgradeModuleProviderDropped(
            "upgrade would drop packaged module owner(s) archi",
            data={"phase": "pre_upgrade", "dropped_owners": ["archi"]},
        )

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment", drop,
    )
    checks, errors, details = _actual_collect_effect_inventory(
        SimpleNamespace(options=SimpleNamespace(deployment="demo"), data={}),
    )

    assert checks is None
    assert errors == ("upgrade_module_provider_owner_dropped",)
    assert details["status"] == "refused"
    assert details["error"]["dropped_owners"] == ["archi"]


# ---------------------------------------------------------------------------
# #1791: a refused backup admission names the publish recovery.
# ---------------------------------------------------------------------------

def _publish_recovery_stub(**overrides):
    plan = {
        "schema": "okg.publish_recovery_plan.v1",
        "ok": True,
        "deployment": "dep",
        "branch": "default",
        "state": "staged_work_unpublished",
        "okg_up_applies": False,
        "recovery_command": "uv run okg ingest --deployment dep --json",
        "explanation": "staged records wait for their publish",
    }
    plan.update(overrides)
    return plan


def _run_refused_backup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    effects,
    plan,
    plan_only: bool = False,
    run_name: str = "run",
) -> dict:
    from okg.substrate import deployment_upgrade as du

    _patch_risk_admission(monkeypatch, tmp_path, effects=effects)
    run_dir = tmp_path / run_name
    run_dir.mkdir()
    seen: list[object] = []

    def planner(ctx):
        seen.append(ctx.options.deployment)
        return plan

    monkeypatch.setattr(du, "_publish_recovery_plan", planner)

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if backup := _backup_command(argv):
            return backup
        if "migrations" in argv:
            return _ok({"pending": [], "destructive_pending": []})
        return _ok()

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="dep",
            state_file=run_dir / "state.json",
            backup_dir=run_dir / "backups",
            readiness_profile="dogfood",
            stop_after="backup",
            plan_only=plan_only,
        ),
        command_runner=runner,
    )
    assert seen == ["dep"], "the plan was not consulted exactly once"
    return result


def test_backup_refusal_names_the_publish_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The scaffold case (#1791): manifest checks fail, publish was refused."""
    plan = _publish_recovery_stub()
    result = _run_refused_backup(
        monkeypatch, tmp_path,
        effects=_closed_effect_checks(
            smoke_source_versioned=False,
            attribute_read_model_rebuildable=False,
        ),
        plan=plan,
    )

    backup = result["phases"][-1]
    assert backup["ok"] is False
    assert backup["reason"] == "physical rollback admission refused mutation"
    assert backup["failed_checks"] == [
        "smoke_source_versioned", "attribute_read_model_rebuildable",
    ]
    assert backup["publish_recovery"] == plan
    assert set(backup["failed_check_hints"]["manifest_configuration"]) == {
        "smoke_source_versioned", "attribute_read_model_rebuildable",
    }
    assert backup["failed_check_hints"]["authority"] == {}
    assert "runtime.smoke_sources" in (
        backup["failed_check_hints"]["manifest_configuration"]["smoke_source_versioned"]
    )
    hint = backup["fix_hint"]
    assert "not the recovery for a refused publish" in hint
    assert "do not delete the deployment or database" in hint
    assert "staged_work_unpublished: staged records wait for their publish." in hint
    assert "Run uv run okg ingest --deployment dep --json." in hint
    assert hint.endswith("before upgrading")
    # Hoisted next to `next_command`, which still names the okg up resume.
    assert result["failed_phase"] == "backup"
    assert result["publish_recovery"] == plan
    assert result["fix_hint"] == hint
    assert "deployment up" in result["next_command"]
    json.dumps(result, default=str)


def test_backup_refusal_keeps_upgrade_hints_when_the_deployment_is_healthy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _publish_recovery_stub(
        state="healthy", okg_up_applies=True, recovery_command=None,
    )
    manifest_only = _run_refused_backup(
        monkeypatch, tmp_path,
        effects=_closed_effect_checks(smoke_source_versioned=False),
        plan=plan,
    )["phases"][-1]
    assert manifest_only["publish_recovery"] == plan
    assert manifest_only["fix_hint"] == (
        "declare the manifest settings named in "
        "failed_check_hints.manifest_configuration, then re-run"
    )

    with_authority = _run_refused_backup(
        monkeypatch, tmp_path,
        effects=_closed_effect_checks(
            smoke_source_versioned=False, planner_trusted=False,
        ),
        plan=plan,
        run_name="with-authority",
    )["phases"][-1]
    assert with_authority["failed_check_hints"]["authority"] == {
        "planner_trusted": (
            "the planner's own tracked files differ from the pinned revision "
            "(uncommitted edits under src/okg)"
        ),
    }
    assert with_authority["fix_hint"] == (
        "resolve the target/source authority failure before creating or "
        "verifying a physical backup"
    )


def test_backup_refusal_surfaces_a_plan_read_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unreadable plan is reported, never a verdict and never a crash."""
    plan = {
        "schema": "okg.publish_recovery_plan.v1",
        "ok": False,
        "state": "unknown",
        "okg_up_applies": False,
        "recovery_command": None,
        "error": {"type": "OperationalError", "message": "connection refused"},
    }
    backup = _run_refused_backup(
        monkeypatch, tmp_path,
        effects=_closed_effect_checks(target_revision_pinned=False),
        plan=plan,
    )["phases"][-1]

    assert backup["ok"] is False
    assert backup["publish_recovery"]["error"]["type"] == "OperationalError"
    assert backup["fix_hint"] == (
        "resolve the target/source authority failure before creating or "
        "verifying a physical backup"
    )


def test_plan_only_backup_refusal_carries_the_publish_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _publish_recovery_stub()
    result = _run_refused_backup(
        monkeypatch, tmp_path,
        effects=_closed_effect_checks(smoke_source_versioned=False),
        plan=plan,
        plan_only=True,
    )
    backup = result["phases"][-1]

    assert backup["ok"] is True
    assert backup["mutation_authorized"] is False
    assert backup["planned"] == "backup admission remains refused"
    assert backup["publish_recovery"] == plan
    assert "uv run okg ingest --deployment dep --json" in backup["fix_hint"]


# ---------------------------------------------------------------------------
# req.dependency-sync.names-its-target /
# req.dependency-sync.refuses-an-unnamed-target  (#1929)
# ---------------------------------------------------------------------------

_UV_VIRTUAL_PROJECT = """\
[project]
name = "{name}"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = []

[project.optional-dependencies]
quality = []

[tool.uv]
package = false
"""

_PLANTED_RECORD = """\
okg_sync_marker.py,,
okg_sync_marker-1.0.dist-info/METADATA,,
okg_sync_marker-1.0.dist-info/INSTALLER,,
okg_sync_marker-1.0.dist-info/RECORD,,
"""


def _uv_env() -> dict[str, str]:
    """A hermetic uv environment: the running interpreter, and no network.

    The scratch projects below declare `package = false` and no dependencies,
    so a sync needs nothing from an index and no build backend. `UV_OFFLINE`
    makes that a guarantee rather than a hope about the runner's cache.
    """
    env = dict(os.environ)
    env.pop("UV_PROJECT_ENVIRONMENT", None)
    env.pop("VIRTUAL_ENV", None)
    env["UV_PYTHON"] = sys.executable
    env["UV_OFFLINE"] = "1"
    return env


def _write_uv_project(root: Path, *, name: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "pyproject.toml").write_text(
        _UV_VIRTUAL_PROJECT.format(name=name), encoding="utf-8",
    )


def _uv(argv: list[str], *, cwd: Path, env: dict[str, str]) -> None:
    done = subprocess.run(
        ["uv", *argv], cwd=cwd, env=env,
        capture_output=True, text=True, check=False,
    )
    assert done.returncode == 0, (done.stdout, done.stderr)


def _framework_checkout(root: Path) -> None:
    """A repository the revision authority accepts as a framework checkout."""
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    planner = root / "src" / "okg" / "substrate"
    planner.mkdir(parents=True, exist_ok=True)
    (planner / "deployment_upgrade.py").write_text("", encoding="utf-8")


def _plant_unmanaged_distribution(venv_root: Path) -> Path:
    """Install, by hand, a distribution the project's lock does not name.

    `uv sync` converges an environment by REMOVING whatever the lock does not
    name, so a planted distribution is visible proof of which environment a
    sync actually converged.
    """
    site_packages = next(venv_root.glob("lib/python*/site-packages"))
    module = site_packages / "okg_sync_marker.py"
    module.write_text("PLANTED = True\n", encoding="utf-8")
    dist_info = site_packages / "okg_sync_marker-1.0.dist-info"
    dist_info.mkdir(parents=True)
    (dist_info / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: okg-sync-marker\nVersion: 1.0\n",
        encoding="utf-8",
    )
    (dist_info / "INSTALLER").write_text("uv\n", encoding="utf-8")
    (dist_info / "RECORD").write_text(_PLANTED_RECORD, encoding="utf-8")
    return module


def _dependency_sync_context(
    *,
    tmp_path: Path,
    env: dict[str, str],
    runner,
    sync_deps: bool = True,
    plan_only: bool = False,
) -> UpgradeContext:
    return UpgradeContext(
        options=UpgradeOptions(
            deployment="dep",
            state_file=tmp_path / "up-state.json",
            skip_backup=True,
            sync_deps=sync_deps,
            plan_only=plan_only,
        ),
        dsn="postgres://unused",
        resolution={"target": {}},
        env=env,
        state_file=tmp_path / "up-state.json",
        backup_dir=tmp_path / "backups",
        command_runner=runner,
    )


def test_dependency_sync_converges_the_deployment_project_not_the_working_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#1929. `uv sync` with no named project resolves one by walking up from
    the PROCESS working directory and then prunes it. `okg up` runs from
    whoever typed the command, so the phase converged the caller's environment
    instead of the deployment's -- in CI, the interpreter running this suite.
    """
    from okg.substrate import deployment_upgrade as du

    env = _uv_env()
    project = tmp_path / "deployment-project"
    caller = tmp_path / "caller-project"
    _write_uv_project(project, name="deployment-project")
    _write_uv_project(caller, name="caller-project")
    _uv(["lock"], cwd=project, env=env)
    _uv(["lock"], cwd=caller, env=env)
    _framework_checkout(project)
    deployment_dir = project / "deployments" / "dep"
    deployment_dir.mkdir(parents=True)

    # The caller's environment exists and holds a distribution its own lock
    # does not name; a sync that converges it will remove that distribution.
    _uv(["sync", "--frozen", "--extra", "quality"], cwd=caller, env=env)
    planted = _plant_unmanaged_distribution(caller / ".venv")

    monkeypatch.setattr(
        du,
        "_resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(
            deployment_dir=deployment_dir,
            release={"pin": {"code_repository": str(project)}},
            runtime=SimpleNamespace(python_extras=("quality",)),
        ),
    )
    monkeypatch.chdir(caller)
    ctx = _dependency_sync_context(
        tmp_path=tmp_path, env=env, runner=du._default_command_runner,
    )

    result = du._phase_dependency_sync(ctx)

    assert result["ok"] is True, result
    # The working directory's environment was left alone: the distribution its
    # own lock does not name is still installed.
    assert planted.exists(), (
        "the dependency sync pruned the working directory's environment: "
        f"{result}"
    )
    # The deployment's own environment is the one that was converged.
    assert (project / ".venv" / "bin" / "python").exists(), result
    assert result["project_root"] == str(project.resolve()), result
    assert result["environment"] == str(project.resolve() / ".venv"), result
    assert result["python_extras"] == ["quality"], result


def test_dependency_sync_plan_reports_the_target_it_would_converge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An operator must be able to read what a run will mutate before it runs.
    The skipped/planned shape therefore carries the same resolved target as the
    applied shape."""
    from okg.substrate import deployment_upgrade as du

    project = tmp_path / "deployment-project"
    _write_uv_project(project, name="deployment-project")
    _framework_checkout(project)
    deployment_dir = project / "deployments" / "dep"
    deployment_dir.mkdir(parents=True)
    calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        calls.append(argv)
        return _ok()

    monkeypatch.setattr(
        du,
        "_resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(
            deployment_dir=deployment_dir,
            release={"pin": {"code_repository": str(project)}},
            runtime=SimpleNamespace(python_extras=("quality",)),
        ),
    )
    ctx = _dependency_sync_context(
        tmp_path=tmp_path, env={}, runner=runner, sync_deps=False,
    )

    result = du._phase_dependency_sync(ctx)

    assert result["ok"] is True, result
    assert result["project_root"] == str(project.resolve()), result
    assert result["environment"] == str(project.resolve() / ".venv"), result
    assert result["planned"] == (
        f"uv sync --project {project.resolve()} --frozen --extra quality"
    ), result
    assert calls == []


def test_dependency_sync_refuses_a_target_it_cannot_name(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pinned framework checkout that does not resolve is refused out loud.
    Falling back to an unqualified `uv sync` is what converges (and prunes) the
    caller's working directory, so there is no fallback."""
    from okg.substrate import deployment_upgrade as du

    project = tmp_path / "deployment-project"
    _write_uv_project(project, name="deployment-project")
    _framework_checkout(project)
    deployment_dir = project / "deployments" / "dep"
    deployment_dir.mkdir(parents=True)
    missing = tmp_path / "not-a-repository"
    missing.mkdir()
    calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        calls.append(argv)
        return _ok()

    monkeypatch.setattr(
        du,
        "_resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(
            deployment_dir=deployment_dir,
            release={"pin": {"code_repository": str(missing)}},
            runtime=SimpleNamespace(python_extras=()),
        ),
    )
    ctx = _dependency_sync_context(tmp_path=tmp_path, env={}, runner=runner)

    row = du._run_phase(ctx, "dependency_sync")

    assert row["ok"] is False, row
    assert row["reason"] == "dependency sync target repository unresolved", row
    assert row["error"]["data"]["code_repository"] == str(missing), row
    assert "--no-sync-deps" in row["error"]["fix_hint"], row
    assert calls == [], calls


def test_dependency_sync_refuses_a_target_that_is_not_a_uv_project(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An installed deployment has no lock to apply. Syncing anyway would
    converge whatever project the caller's directory resolves to."""
    from okg.substrate import deployment_upgrade as du

    project = tmp_path / "installed-checkout"
    project.mkdir()
    _framework_checkout(project)
    deployment_dir = project / "deployments" / "dep"
    deployment_dir.mkdir(parents=True)
    calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        calls.append(argv)
        return _ok()

    monkeypatch.setattr(
        du,
        "_resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(
            deployment_dir=deployment_dir,
            release={"pin": {"code_repository": str(project)}},
            runtime=SimpleNamespace(python_extras=()),
        ),
    )
    ctx = _dependency_sync_context(tmp_path=tmp_path, env={}, runner=runner)

    row = du._run_phase(ctx, "dependency_sync")

    assert row["ok"] is False, row
    assert row["reason"] == "dependency sync target is not a uv project", row
    assert row["error"]["data"]["project_root"] == str(project.resolve()), row
    assert "--no-sync-deps" in row["error"]["fix_hint"], row
    assert calls == [], calls


def test_dependency_sync_opt_out_discloses_an_unnameable_target_without_failing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The opt-out mutates nothing, so a target with no name is DISCLOSED, not
    failed. `--no-sync-deps` is what the refusal's own fix hint tells an
    operator to run, and it is the documented path for an installed,
    non-checkout planner; failing it would close the hatch and would break the
    read-only `--plan` that worked before this change."""
    from okg.substrate import deployment_upgrade as du

    outside = tmp_path / "installed-deployment"
    outside.mkdir()
    calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        calls.append(argv)
        return _ok()

    monkeypatch.setattr(
        du,
        "_resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(
            deployment_dir=outside,
            runtime=SimpleNamespace(python_extras=()),
        ),
    )

    for plan_only in (False, True):
        ctx = _dependency_sync_context(
            tmp_path=tmp_path, env={}, runner=runner,
            sync_deps=False, plan_only=plan_only,
        )
        row = du._run_phase(ctx, "dependency_sync")

        assert row["ok"] is True, row
        assert row["skipped"], row
        # The reason is carried, not swallowed: an operator reading the run
        # sees exactly why no target could be named.
        unresolved = row["target_unresolved"]
        assert unresolved["message"] == (
            "dependency sync target repository unresolved"
        ), row
        assert unresolved["data"]["deployment_dir"] == str(outside), row
        assert "planned" not in row, row
        assert calls == [], calls

    # And the same deployment, asked to actually sync, still refuses.
    ctx = _dependency_sync_context(tmp_path=tmp_path, env={}, runner=runner)
    applied = du._run_phase(ctx, "dependency_sync")
    assert applied["ok"] is False, applied
    assert calls == [], calls


def test_dependency_sync_resolves_the_same_repository_as_sync_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The unpinned path -- the one every in-repo deployment takes -- must
    answer with the repository `sync_code` checks out, not with a second
    opinion derived some other way. This asserts the product's answer against
    `_resolve_repo_authorities` itself, which is the helper the code-sync
    phase's authority calls, so a re-introduced private walk fails here."""
    from okg.substrate import deployment_upgrade as du

    # A real, unpinned, in-repo deployment directory: no `code_repository`.
    deployment_dir = _planner_project_root() / "deployments"
    expected, _deployment_repo_root = du._resolve_repo_authorities(deployment_dir)
    calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        calls.append(argv)
        return _ok()

    monkeypatch.setattr(
        du,
        "_resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(
            deployment_dir=deployment_dir,
            runtime=SimpleNamespace(python_extras=()),
        ),
    )
    ctx = _dependency_sync_context(tmp_path=tmp_path, env={}, runner=runner)

    row = du._run_phase(ctx, "dependency_sync")

    assert row["ok"] is True, row
    assert row["project_root"] == str(expected), row
    assert calls == [["uv", "sync", "--project", str(expected), "--frozen"]], calls


def test_dependency_sync_reports_the_environment_a_redirect_moves_it_to(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`UV_PROJECT_ENVIRONMENT` moves the environment uv converges and has no
    command-line equivalent, so the phase must read it from the same env the
    command runs with and report where the mutation will land. The lifecycle
    arm depends on this: it is how that arm keeps a real dependency sync from
    converging the interpreter the test suite runs on."""
    from okg.substrate import deployment_upgrade as du

    project = tmp_path / "deployment-project"
    _write_uv_project(project, name="deployment-project")
    _framework_checkout(project)
    deployment_dir = project / "deployments" / "dep"
    deployment_dir.mkdir(parents=True)
    scratch = tmp_path / "scratch-env"
    calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        calls.append(argv)
        # The redirect reaches uv through the environment, not through argv.
        assert env["UV_PROJECT_ENVIRONMENT"] == str(scratch)
        return _ok()

    monkeypatch.setattr(
        du,
        "_resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(
            deployment_dir=deployment_dir,
            release={"pin": {"code_repository": str(project)}},
            runtime=SimpleNamespace(python_extras=()),
        ),
    )
    ctx = _dependency_sync_context(
        tmp_path=tmp_path,
        env={"UV_PROJECT_ENVIRONMENT": str(scratch)},
        runner=runner,
    )

    row = du._run_phase(ctx, "dependency_sync")

    assert row["ok"] is True, row
    assert row["project_root"] == str(project.resolve()), row
    assert row["environment"] == str(scratch), row
    assert row["environment"] != str(project.resolve() / ".venv"), row
    assert len(calls) == 1, calls


# --- issue #2022: verification databases must not inherit template1 ---------
#
# Both regression tests below live in this file rather than beside
# `deployment_promotion_operations`: `tests/qa_lanes.yaml` enrols
# `tests/substrate/test_deployment_upgrade.py` in a per-PR Postgres lane,
# while `tests/substrate/test_deployment_promotion_operations.py` has no
# per-PR owner, so a Postgres row added there would be refused by
# lane-scope as an unowned node.


def _issue2022_dsn_for(base_dsn: str, dbname: str) -> str:
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    info = dict(conninfo_to_dict(base_dsn))
    info["dbname"] = dbname
    return make_conninfo(**info)


def _issue2022_timescaledb_version(dsn: str) -> str | None:
    import psycopg

    with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
        row = conn.execute(
            "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
        ).fetchone()
    return None if row is None else str(row[0])


def _issue2022_drop(base_dsn: str, dbname: str) -> None:
    import psycopg
    from psycopg import sql as psql

    with psycopg.connect(
        _issue2022_dsn_for(base_dsn, "postgres"), autocommit=True, connect_timeout=5
    ) as conn:
        conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (dbname,),
        )
        conn.execute(
            psql.SQL("DROP DATABASE IF EXISTS {}").format(psql.Identifier(dbname))
        )


def _issue2022_cluster_template_version(base_dsn: str) -> str:
    """Return template1's TimescaleDB version, or skip when it has none."""
    version = _issue2022_timescaledb_version(_issue2022_dsn_for(base_dsn, "template1"))
    if version is None:
        pytest.skip(
            "this cluster's template1 carries no TimescaleDB, so a database "
            "cloned from it cannot inherit one and issue #2022 cannot be "
            "reproduced here; use an ops/pg image built with "
            "ops/pg/initdb/01-template1-extensions.sql"
        )
    return version


@pytest.mark.postgres
def test_verification_databases_are_created_from_template0(okg_dsn: str) -> None:
    """Neither verification database may inherit the cluster's TimescaleDB.

    Regression for issue #2022. `ops/pg/initdb` installs TimescaleDB in
    `template1` (issue #1972), so a bare `CREATE DATABASE` hands the
    verifier a database that already has the extension at the CLUSTER's
    version. Both production creation helpers must therefore use
    `TEMPLATE template0`, which `initdb` never touches, and hand back a
    database with no `timescaledb` row at all -- the extension is
    installed afterwards, at the SOURCE deployment's version.
    """
    from uuid import uuid4

    from okg.substrate import deployment_promotion_operations as dpo
    from okg.substrate import deployment_upgrade as du

    cluster_version = _issue2022_cluster_template_version(okg_dsn)
    maintenance_dsn = _issue2022_dsn_for(okg_dsn, "postgres")

    upgrade_db = f"okg_upgrade_verify_{uuid4().hex[:16]}"
    try:
        du._create_verification_database(maintenance_dsn, upgrade_db)
        inherited = _issue2022_timescaledb_version(
            _issue2022_dsn_for(okg_dsn, upgrade_db)
        )
        assert inherited is None, (
            "the upgrade verification database inherited TimescaleDB "
            f"{inherited} from template1 ({cluster_version}); a later "
            "version-pinned CREATE EXTENSION would no-op and the verifier "
            "would refuse"
        )
    finally:
        _issue2022_drop(okg_dsn, upgrade_db)

    promotion_dsn, promotion_db = dpo._create_disposable_database(okg_dsn)
    try:
        assert promotion_db.startswith("okg_promotion_verify_"), promotion_db
        inherited = _issue2022_timescaledb_version(promotion_dsn)
        assert inherited is None, (
            "the promotion verification database inherited TimescaleDB "
            f"{inherited} from template1 ({cluster_version})"
        )
    finally:
        _issue2022_drop(okg_dsn, promotion_db)


@pytest.mark.postgres
def test_verification_databases_accept_a_source_timescaledb_version_that_differs(
    okg_dsn: str,
) -> None:
    """Reproduce issue #2022's refusal: source version != cluster version.

    The source deployment's TimescaleDB version pins the verification
    database's extension. When that version differs from the one
    `template1` carries -- a TimescaleDB bump on either side -- a
    verification database cloned from `template1` refuses with
    `disposable TimescaleDB version does not match source` (upgrade) or
    `TimescaleDB restore target version does not match the source`
    (promotion). Created from `template0` both pin the source's version
    and proceed.
    """
    import psycopg
    from psycopg import sql as psql
    from uuid import uuid4

    from okg.substrate import deployment_promotion_operations as dpo
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.postgres_restore import timescaledb_restore_context

    cluster_version = _issue2022_cluster_template_version(okg_dsn)
    maintenance_dsn = _issue2022_dsn_for(okg_dsn, "postgres")

    with psycopg.connect(maintenance_dsn, autocommit=True, connect_timeout=5) as conn:
        available = [
            str(row[0])
            for row in conn.execute(
                "SELECT version FROM pg_available_extension_versions "
                "WHERE name = 'timescaledb' AND version <> %s",
                (cluster_version,),
            ).fetchall()
        ]

    def _order(version: str) -> tuple[int, tuple[int, ...], str]:
        parts = version.split(".")
        if all(part.isdigit() for part in parts):
            return (1, tuple(int(part) for part in parts), version)
        return (0, (), version)

    candidates = sorted(available, key=_order, reverse=True)

    # Pick the input: the newest other version this build will actually
    # install. Only the SELECTION of the version tolerates a failure here;
    # every assertion below runs unguarded.
    source_version: str | None = None
    for candidate in candidates:
        probe_db = f"okg_test_ts_probe_{uuid4().hex[:12]}"
        try:
            # Test-owned scaffolding, deliberately NOT the production
            # helper: version discovery must not depend on the behaviour
            # this test exists to check, or a regression would present as
            # a skip instead of a failure.
            with psycopg.connect(
                maintenance_dsn, autocommit=True, connect_timeout=5
            ) as conn:
                conn.execute(
                    psql.SQL("CREATE DATABASE {} TEMPLATE template0").format(
                        psql.Identifier(probe_db)
                    )
                )
            probe_dsn = _issue2022_dsn_for(okg_dsn, probe_db)
            with psycopg.connect(
                probe_dsn, autocommit=True, connect_timeout=5
            ) as conn:
                conn.execute(
                    psql.SQL("CREATE EXTENSION timescaledb VERSION {}").format(
                        psql.Literal(candidate)
                    )
                )
            source_version = candidate
        except psycopg.Error:
            source_version = None
        finally:
            _issue2022_drop(okg_dsn, probe_db)
        if source_version is not None:
            break

    if source_version is None:
        pytest.skip(
            "this build installs only TimescaleDB "
            f"{cluster_version}, so a genuine source/cluster version "
            f"mismatch cannot be produced here (candidates tried: "
            f"{candidates or 'none'})"
        )

    upgrade_db = f"okg_upgrade_verify_{uuid4().hex[:16]}"
    try:
        du._create_verification_database(maintenance_dsn, upgrade_db)
        upgrade_dsn = _issue2022_dsn_for(okg_dsn, upgrade_db)
        du._prestage_source_timescaledb(upgrade_dsn, source_version)
        assert _issue2022_timescaledb_version(upgrade_dsn) == source_version
    finally:
        _issue2022_drop(okg_dsn, upgrade_db)

    promotion_dsn, promotion_db = dpo._create_disposable_database(okg_dsn)
    try:
        with timescaledb_restore_context(promotion_dsn, version=source_version):
            assert _issue2022_timescaledb_version(promotion_dsn) == source_version
    finally:
        _issue2022_drop(okg_dsn, promotion_db)


# --- #1983: `okg up` refuses a standalone deployment before any Git call ---


def _issue1983_deployment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    release: dict[str, object],
) -> str:
    """Write a minimal named deployment whose manifest carries ``release``."""
    root = tmp_path / "deployments"
    deployment_dir = root / "standalone-dep"
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "standalone-dep", "release": release}),
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.delenv("OKG_ENVIRONMENT_DESCRIPTOR", raising=False)
    return "standalone-dep"


def _issue1983_forbid_git(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    """Record, and fail, every `git` subprocess started during the call."""
    git_calls: list[list[str]] = []
    real_init = subprocess.Popen.__init__

    def guarded_init(self, args, *rest, **kwargs):
        argv = [str(part) for part in args] if isinstance(args, (list, tuple)) else [str(args)]
        if argv and Path(argv[0]).name == "git":
            git_calls.append(argv)
            raise AssertionError(f"git subprocess started: {argv}")
        return real_init(self, args, *rest, **kwargs)

    monkeypatch.setattr(subprocess.Popen, "__init__", guarded_init)
    return git_calls


class _Issue1983ContextBuilt(Exception):
    pass


@pytest.mark.parametrize("plan_only", [False, True])
@pytest.mark.parametrize(
    "release",
    [
        {"channel": "dev", "kind": "standalone"},
        {"channel": "prod", "kind": "standalone", "pin": {"release_kind": "standalone"}},
    ],
)
def test_issue1983_up_routes_standalone_before_any_git_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    plan_only: bool,
    release: dict[str, object],
) -> None:
    """#3415 replaced the #1983 refusal: `okg up` routes a standalone
    deployment to the installed-package upgrade before any Git work."""
    from okg.substrate import deployment_installed_upgrade as diu
    from okg.substrate import deployment_upgrade as du

    deployment = _issue1983_deployment(tmp_path, monkeypatch, release)
    git_calls = _issue1983_forbid_git(monkeypatch)

    def forbidden(*args, **kwargs):
        raise _Issue1983ContextBuilt((args, kwargs))

    monkeypatch.setattr(du, "_build_context", forbidden)
    monkeypatch.setattr(du, "_git_target_revision", forbidden)
    monkeypatch.setattr(du, "_resolve_repo_authorities", forbidden)
    routed: list[tuple[object, dict[str, object]]] = []

    def installed(options, route, *, command_runner=None):
        routed.append((options, dict(route)))
        return {"ok": True, "status": "routed"}

    monkeypatch.setattr(diu, "run_installed_upgrade", installed)
    runner_calls: list[list[str]] = []

    def runner(cmd, env):
        runner_calls.append(list(cmd))
        raise AssertionError(cmd)

    payload = run_deployment_upgrade(
        UpgradeOptions(deployment=deployment, plan_only=plan_only),
        command_runner=runner,
    )

    assert payload == {"ok": True, "status": "routed"}
    ((options, route),) = routed
    assert options.deployment == deployment
    assert route["release_kind"] == "standalone"
    assert route["decided_by"] == "declared"
    assert git_calls == []
    assert runner_calls == []


def test_issue1983_up_checkout_deployment_is_not_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deployment without the declaration proceeds to the ordinary path."""
    from okg.substrate import deployment_upgrade as du

    deployment = _issue1983_deployment(tmp_path, monkeypatch, {"channel": "dev"})

    def context_reached(*args, **kwargs):
        raise _Issue1983ContextBuilt((args, kwargs))

    monkeypatch.setattr(du, "_build_context", context_reached)
    with pytest.raises(_Issue1983ContextBuilt):
        run_deployment_upgrade(UpgradeOptions(deployment=deployment))


def _issue1983_forbid_all_subprocesses(
    monkeypatch: pytest.MonkeyPatch,
) -> list[list[str]]:
    """Record, and fail, every subprocess (git, pg_dump, pg_restore...)."""
    started: list[list[str]] = []

    def guarded_init(self, args, *rest, **kwargs):
        argv = [str(part) for part in args] if isinstance(args, (list, tuple)) else [str(args)]
        started.append(argv)
        raise AssertionError(f"subprocess started: {argv}")

    monkeypatch.setattr(subprocess.Popen, "__init__", guarded_init)
    return started


@pytest.mark.parametrize("via_upgrade_runner", [False, True])
def test_issue1983_backup_verify_refuses_standalone_before_any_dump(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    via_upgrade_runner: bool,
) -> None:
    """Backup verify on a standalone deployment refuses before any backup.

    After its dump and restore, backup verify collects the effect inventory,
    which resolves a Git revision, so on a standalone deployment it could only
    fail late with a Git error. It must refuse up front instead: no subprocess
    (no dump, no restore, no git), no command-runner call, no context built.
    """
    from okg.substrate import deployment_upgrade as du

    deployment = _issue1983_deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"}
    )
    started = _issue1983_forbid_all_subprocesses(monkeypatch)

    def forbidden(*args, **kwargs):
        raise _Issue1983ContextBuilt((args, kwargs))

    for name in (
        "_build_context",
        "_git_target_revision",
        "_resolve_repo_authorities",
        "_collect_effect_inventory",
        "_phase_backup",
    ):
        monkeypatch.setattr(du, name, forbidden)
    runner_calls: list[list[str]] = []

    def runner(cmd, env):
        runner_calls.append(list(cmd))
        raise AssertionError(cmd)

    options = UpgradeOptions(
        deployment=deployment,
        independent_backup_verify=True,
        state_file=tmp_path / "verify-state.json",
        backup_dir=tmp_path / "backups",
    )
    call = (
        du.run_deployment_upgrade
        if via_upgrade_runner
        else du.run_deployment_backup_verify
    )
    with pytest.raises(du.StandaloneUpgradeUnsupported) as excinfo:
        call(options, command_runner=runner)

    error = excinfo.value.to_json()["error"]
    assert error["code"] == "standalone_upgrade_unsupported"
    assert error["data"]["operation"] == "backup_verify"
    assert error["data"]["deployment"] == deployment
    assert "backup verify" in error["message"]
    assert "pg_dump" in error["fix_hint"]
    assert started == []
    assert runner_calls == []
    assert not (tmp_path / "verify-state.json").exists()
    assert not (tmp_path / "backups").exists()


def test_issue1983_backup_verify_of_checkout_deployment_is_not_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backup verify of a deployment without the declaration proceeds."""
    from okg.substrate import deployment_upgrade as du

    deployment = _issue1983_deployment(tmp_path, monkeypatch, {"channel": "dev"})

    def context_reached(*args, **kwargs):
        raise _Issue1983ContextBuilt((args, kwargs))

    monkeypatch.setattr(du, "_build_context", context_reached)
    with pytest.raises(_Issue1983ContextBuilt):
        du.run_deployment_backup_verify(
            UpgradeOptions(
                deployment=deployment,
                independent_backup_verify=True,
                state_file=tmp_path / "verify-state.json",
            )
        )


def test_issue1983_fleet_up_reports_standalone_as_failed_row(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`okg up --all` records a standalone deployment as one failed row."""
    from okg.substrate import deployment_upgrade as du

    deployment = _issue1983_deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"}
    )
    git_calls = _issue1983_forbid_git(monkeypatch)

    def forbidden(*args, **kwargs):
        raise _Issue1983ContextBuilt((args, kwargs))

    monkeypatch.setattr(du, "_build_context", forbidden)
    monkeypatch.setattr(
        du, "_fleet_release_snapshot", lambda *args, **kwargs: {"ok": True}
    )
    payload = du.run_deployment_fleet_upgrade(
        du.FleetUpgradeOptions(deployments=(deployment,), profile="dev")
    )

    assert payload["ok"] is False
    assert payload["summary"]["failed_deployments"] == [deployment]
    (row,) = payload["deployments"]
    assert row["status"] == "failed"
    # #3415: the standalone member is routed to the installed-package
    # upgrade, which refuses the dev target (a worktree-isolated database is
    # a Git-checkout concept) before touching anything.
    assert row["error"]["code"] == "installed_upgrade_refused"
    assert row["error"]["data"]["options"] == ["--target dev"]
    assert git_calls == []


# ── renamed-flag compat (decision 310) ──────────────────────────────────
# `deployment ready` children may run the checked-out release pin, which a
# rollback can set to a pre-rename commit. They pass the spelling both old
# and new code accept and silence the rename warning in the child only.


def _record_ready_calls(calls: list[tuple[list[str], dict[str, str]]]):
    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        if "ready" in argv:
            calls.append((list(argv), dict(env)))
            return _ok({"ok": True, "source_sync_status": {
                "convergence": {"ok": True, "state": "ready"},
            }})
        return _ok({"ok": True})
    return runner


def _assert_compat_ready_child(
    calls: list[tuple[list[str], dict[str, str]]], profile: str,
) -> None:
    assert calls, "no deployment ready child ran"
    for argv, env in calls:
        at = argv.index("ready")
        assert argv[at + 2:at + 4] == ["--profile", profile], argv
        assert "--checks" not in argv
        assert env["OKG_RENAMED_FLAG_WARNINGS"] == "0"


def test_source_convergence_ready_child_uses_compat_spelling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    calls: list[tuple[list[str], dict[str, str]]] = []
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"completed": False, "phase_results": []}))
    dep_dir = tmp_path / "deployment"
    dep_dir.mkdir()
    monkeypatch.setattr(
        du, "_resolve_upgrade_deployment",
        lambda ctx: SimpleNamespace(deployment_dir=dep_dir),
    )
    ctx = UpgradeContext(
        options=UpgradeOptions(
            deployment="dep", skip_backup=True, readiness_profile="dogfood",
        ),
        dsn="postgres://test",
        resolution={"target": {"database": "test", "identity": "isolated"}},
        env={"OKG_DSN": "postgres://test"},
        state_file=state_file,
        backup_dir=tmp_path,
        command_runner=_record_ready_calls(calls),
        data={"runtime_fence_owner": "deploy-owner"},
    )
    result = du._phase_source_convergence(ctx)
    assert result["ok"] is True, result
    _assert_compat_ready_child(calls, "dogfood")
    # Only the child is silenced; the parent's environment is untouched.
    assert "OKG_RENAMED_FLAG_WARNINGS" not in ctx.env


def test_resume_revalidation_ready_child_uses_compat_spelling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_upgrade as du

    def refuse(**kwargs: object) -> object:
        raise AssertionError("no queue read may run without a dbos schema")

    monkeypatch.setattr(du, "_probe_runtime_bootstrapped", lambda ctx: False)
    monkeypatch.setattr("okg.substrate.runtime.admin.client_drain", refuse)
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.promotion_fence_status", refuse,
    )
    calls: list[tuple[list[str], dict[str, str]]] = []
    base = _runtime_ctx(tmp_path)
    ctx = UpgradeContext(
        options=base.options,
        dsn=base.dsn,
        resolution=base.resolution,
        env=base.env,
        state_file=base.state_file,
        backup_dir=base.backup_dir,
        command_runner=_record_ready_calls(calls),
        data=base.data,
    )
    du._revalidate_resume_postconditions(
        ctx,
        phase_results=[{"phase": "readiness", "ok": True}],
        completed={"readiness"},
    )
    _assert_compat_ready_child(calls, "general")
