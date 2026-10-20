"""A deployment that turns its runtime off on purpose is not "unavailable".

PACT `trust-level-runtime-disabled` (issues #2987, #2502). A bundle-installed
deployment such as cms-kb declares ``runtime: {enabled: false}`` and ingests
by hand or by reconcile. On dev, ``okg dogfood status`` still demanded a
runtime worker process and DBOS queue history for it, so ``trust_level`` read
``unavailable`` and every chat site showed a red health banner.

Every test here drives the real status command (``_cmd_dogfood_status``, the
function ``okg dogfood status --json`` runs) with the live probes stubbed to
the shape lane A captured from the real cms-kb deployment on 2026-09-29
(okg dev f5f7c81ca3, deployment ``lane-a-cmskb``). The runtime declaration is
read from a real ``deployment.yaml`` on disk.
"""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from okg.substrate.chat.health_banner import (
    CONDITION_LABELS,
    STATE_DEGRADED,
    STATE_HEALTHY,
    read_health,
)
from okg.substrate.cli import dogfood as dogfood_cli

DEPLOYMENT = "lane-a-cmskb"
GENERATION_ID = (
    "okg:lane-a-cmskb:branch:default:gen:20260928T030402054545Z:6de225750f0f"
)

# --- The captured shape (trimmed to the fields the status builder reads) ----

NO_WORKER = {
    "ok": False,
    "status": "not_observed",
    "process_count": 0,
    "root_count": 0,
    "roots": [],
    "processes": [],
    "split_executor": False,
}
ONE_WORKER = {
    "ok": True,
    "status": "single_executor",
    "process_count": 1,
    "root_count": 1,
    "roots": [{
        "pid": 4242,
        "ppid": 1,
        "command": f"okg runtime worker --deployment {DEPLOYMENT}",
    }],
    "processes": [],
    "split_executor": False,
}
AUDIT_FAILED = {
    "ok": None,
    "status": "unavailable",
    "reason": "ps returned 1",
    "process_count": 0,
    "root_count": 0,
    "split_executor": False,
    "processes": [],
    "roots": [],
}
SPLIT_WORKERS = {
    **ONE_WORKER,
    "ok": False,
    "status": "split_executor",
    "root_count": 2,
    "split_executor": True,
}

# DBOS never ran here, so its workflow table does not exist. The queue view
# then returns one row of zeros (db/views.sql, runtime_queue_health.py), so
# every depth below is zero -- the only shape production produces today.
DBOS_ABSENT_QUEUES = {
    "active_queue_depth": 0,
    "dlq_count": 0,
    "dlq_status": "measured",
    "max_active_queue_depth": 0,
    "ok": False,
    "queue_depth": {"embed": 0, "enrich": 0, "ingest": 0, "link": 0, "publish": 0},
    "queue_health_measured": True,
    "reason": "dbos.workflow_status missing",
    "runtime_status_counts": {"unavailable": 1},
    "source": "okg.v_runtime_queue_health",
    "status": "unavailable",
}

HEALTHY_DRIFT = {
    "ok": True,
    "status": "current",
    "commits_behind": 0,
    "problems": [],
}
NO_SYNC_ROOT_DRIFT = {
    "ok": False,
    "status": "misconfigured",
    "commits_behind": None,
    "problems": [f"deployment {DEPLOYMENT!r} declares no sync_root"],
}


def _blocker(check_id: str, message: str, details: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": check_id,
        "status": "blocker",
        "ok": False,
        "message": message,
        "details": details,
    }


QUEUES_UNAVAILABLE = _blocker(
    "queues.unavailable",
    "runtime queue health is unavailable",
    DBOS_ABSENT_QUEUES,
)
# Nomos runtime ENFORCEMENT is off, so source-lineage policy is bypassed on
# every read surface. This is a Nomos policy signal, not a runtime-worker one.
SOURCE_LINEAGE = _blocker(
    "nomos.source_lineage",
    "Nomos source-lineage enforcement is not operational",
    {
        "ok": False,
        "mode": "bypassed",
        "runtime_enforcement": False,
        "runtime_enforcement_mode": "off",
        "errors": [{"code": "nomos_source_lineage_runtime_bypassed"}],
        "read_surfaces": {"mcp": "compatibility_bypass"},
    },
)
ACTIVE_STATE = _blocker(
    "nomos.active_state",
    "production readiness requires Nomos enforce mode; effective mode is off",
    {"ok": False, "status": "failed", "effective_mode": "off"},
)
CODE_DRIFT = _blocker(
    "deployment.code_drift",
    f"deployment {DEPLOYMENT!r} declares no sync_root",
    NO_SYNC_ROOT_DRIFT,
)
APPLY_REFUSED = {
    "ok": False,
    "managed": True,
    "status": "unavailable",
    "reason": "deployment apply refused: catalog ownership belongs to another writer",
    "receipt": None,
}
APPLY_OK = {"ok": True, "managed": True, "status": "ready"}

# The six blocker ids the lane-A deployment showed on dev, in payload order.
LANE_A_DEV_BLOCKERS = [
    "deployment.code_drift",
    "nomos.source_lineage",
    "queues.unavailable",
    "nomos.active_state",
    "deployment.apply",
    "runtime.worker_processes",
]


def _live(
    *,
    blockers: list[dict[str, Any]],
    queues: dict[str, Any] | None = None,
    drift: dict[str, Any] | None = None,
    generation_published: bool = True,
    mcp_ok: bool = True,
) -> dict[str, Any]:
    generation = (
        {"generation_id": GENERATION_ID, "status": "published"}
        if generation_published
        else {"generation_id": None, "status": "missing"}
    )
    return {
        "checks": [],
        "blockers": copy.deepcopy(blockers),
        "warnings": [],
        "unknowns": [],
        "generation": generation,
        "mcp": {
            "ok": mcp_ok,
            "pinned_generation_id": GENERATION_ID if mcp_ok else None,
        },
        "queues": copy.deepcopy(queues if queues is not None else DBOS_ABSENT_QUEUES),
        "code_drift": copy.deepcopy(drift or HEALTHY_DRIFT),
        "source_freshness": {},
        "doctor": {"ok": None, "status": "skipped"},
    }


def _lane_a_live(**overrides: Any) -> dict[str, Any]:
    """The captured deployment: five blockers from the probes."""
    return _live(
        blockers=[CODE_DRIFT, SOURCE_LINEAGE, QUEUES_UNAVAILABLE, ACTIVE_STATE],
        drift=NO_SYNC_ROOT_DRIFT,
        **overrides,
    )


def _status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    *,
    runtime_block: Any,
    live: dict[str, Any],
    processes: dict[str, Any] = NO_WORKER,
    apply: dict[str, Any] = APPLY_OK,
) -> dict[str, Any]:
    deployment_dir = tmp_path / "deployments" / DEPLOYMENT
    deployment_dir.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {"name": DEPLOYMENT}
    if runtime_block is not None:
        manifest["runtime"] = runtime_block
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump(manifest), encoding="utf-8",
    )
    monkeypatch.setattr(
        dogfood_cli,
        "_deployment_ready_env_status",
        lambda _args, **_kw: (
            {"ok": True, "deployment_dir": str(deployment_dir)},
            "postgres://okg@127.0.0.1:1/okg_test_never_dialled",
        ),
    )
    monkeypatch.setattr(
        dogfood_cli, "_deployment_ready_live_checks", lambda **_kw: live,
    )
    monkeypatch.setattr(
        dogfood_cli,
        "_plan_stale_source_sync_work_recovery",
        lambda **_kw: None,
    )
    monkeypatch.setattr(
        dogfood_cli,
        "_attach_nomos_active_state",
        lambda live, **_kw: dict(live),
    )
    monkeypatch.setattr(
        dogfood_cli,
        "_attach_worker_code_identity",
        lambda live, **_kw: dict(live),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_apply.deployment_apply_health",
        lambda **_kw: dict(apply),
    )
    monkeypatch.setattr(
        dogfood_cli,
        "_runtime_worker_process_audit",
        lambda _deployment: copy.deepcopy(processes),
    )
    args = argparse.Namespace(
        deployment=DEPLOYMENT,
        dogfood_subcmd="status",
        details=False,
        skip_doctor=True,
        show_values=False,
        max_active_queue_depth=0,
        max_source_age_seconds=None,
        max_producer_age_seconds=None,
    )
    rc = dogfood_cli._cmd_dogfood_status(args, as_json=True)
    payload = json.loads(capsys.readouterr().out)
    assert rc == (0 if payload["trust_level"] == "ready" else 1)
    return payload


def _blocker_ids(payload: dict[str, Any]) -> list[str]:
    return [row["id"] for row in payload["blockers"]]


def _warning_ids(payload: dict[str, Any]) -> list[str]:
    return [row["id"] for row in payload["warnings"]]


DISABLED = {"enabled": False}
ENABLED = {"enabled": True}


# --- The captured deployment ------------------------------------------------


def test_captured_deployment_keeps_only_its_real_blockers(
    tmp_path, monkeypatch, capsys,
) -> None:
    """The four non-runtime blockers remain; the two runtime ones go.

    On dev this payload listed all six ids in LANE_A_DEV_BLOCKERS.
    """
    payload = _status(
        tmp_path, monkeypatch, capsys,
        runtime_block=DISABLED, live=_lane_a_live(), apply=APPLY_REFUSED,
    )
    assert payload["trust_level"] == "unavailable"
    assert _blocker_ids(payload) == [
        "deployment.code_drift",
        "nomos.source_lineage",
        "nomos.active_state",
        "deployment.apply",
    ]
    worker = payload["runtime_worker"]
    assert worker["status"] == "disabled_by_config"
    assert "runtime.enabled is false" in worker["note"]
    assert worker["not_applicable_checks"] == [
        "runtime.worker_processes",
        "queues.unavailable",
    ]
    assert payload["runtime_config"]["status"] == "disabled"
    assert payload["next_action"]["code"] != "start_or_restart_worker"


def test_captured_deployment_with_runtime_enabled_is_unchanged_from_dev(
    tmp_path, monkeypatch, capsys,
) -> None:
    payload = _status(
        tmp_path, monkeypatch, capsys,
        runtime_block=ENABLED, live=_lane_a_live(), apply=APPLY_REFUSED,
    )
    assert payload["trust_level"] == "unavailable"
    assert _blocker_ids(payload) == LANE_A_DEV_BLOCKERS
    assert payload["next_action"]["code"] == "start_or_restart_worker"


def test_disabled_runtime_without_lane_a_blockers_is_stale_on_nomos_lineage(
    tmp_path, monkeypatch, capsys,
) -> None:
    """Remove the three lane-A items; the Nomos lineage bypass still shows.

    It is a policy signal (reads bypass source-lineage policy because Nomos
    enforcement is off), so it stays a blocker and yields stale_context.
    """
    live = _live(blockers=[SOURCE_LINEAGE, QUEUES_UNAVAILABLE])
    payload = _status(
        tmp_path, monkeypatch, capsys, runtime_block=DISABLED, live=live,
    )
    assert payload["trust_level"] == "stale_context"
    assert _blocker_ids(payload) == ["nomos.source_lineage"]


def test_disabled_runtime_with_healthy_graph_is_ready(
    tmp_path, monkeypatch, capsys,
) -> None:
    live = _live(blockers=[QUEUES_UNAVAILABLE])
    payload = _status(
        tmp_path, monkeypatch, capsys, runtime_block=DISABLED, live=live,
    )
    assert payload["trust_level"] == "ready"
    assert payload["ok"] is True
    assert payload["blockers"] == []
    assert payload["read_authority"]["ok"] is True
    assert payload["runtime_worker"]["status"] == "disabled_by_config"


def test_disabled_runtime_with_code_drift_behind_is_stale(
    tmp_path, monkeypatch, capsys,
) -> None:
    live = _live(
        blockers=[QUEUES_UNAVAILABLE],
        drift={"ok": True, "status": "behind", "commits_behind": 3, "problems": []},
    )
    payload = _status(
        tmp_path, monkeypatch, capsys, runtime_block=DISABLED, live=live,
    )
    assert payload["trust_level"] == "stale_context"


@pytest.mark.parametrize(
    ("case", "extra_blocker", "apply"),
    [
        ("nomos.active_state", ACTIVE_STATE, APPLY_OK),
        ("deployment.code_drift", CODE_DRIFT, APPLY_OK),
        ("deployment.apply", None, APPLY_REFUSED),
    ],
)
def test_each_lane_a_blocker_alone_still_makes_it_unavailable(
    tmp_path, monkeypatch, capsys, case, extra_blocker, apply,
) -> None:
    blockers = [QUEUES_UNAVAILABLE]
    if extra_blocker is not None:
        blockers.append(extra_blocker)
    drift = NO_SYNC_ROOT_DRIFT if case == "deployment.code_drift" else None
    payload = _status(
        tmp_path, monkeypatch, capsys,
        runtime_block=DISABLED,
        live=_live(blockers=blockers, drift=drift),
        apply=apply,
    )
    assert payload["trust_level"] == "unavailable"
    assert _blocker_ids(payload) == [case]


# --- Real failures stay red with the runtime disabled ------------------------


def test_disabled_runtime_with_database_down_is_unavailable(
    tmp_path, monkeypatch, capsys,
) -> None:
    live = {
        "checks": [],
        "blockers": [_blocker(
            "database.connectivity",
            "could not connect to the deployment database",
            {"error": "OperationalError"},
        )],
        "warnings": [],
        "unknowns": [],
        "code_drift": dict(HEALTHY_DRIFT),
    }
    payload = _status(
        tmp_path, monkeypatch, capsys, runtime_block=DISABLED, live=live,
    )
    assert payload["trust_level"] == "unavailable"
    assert "database.connectivity" in _blocker_ids(payload)


def test_disabled_runtime_without_published_generation_is_unavailable(
    tmp_path, monkeypatch, capsys,
) -> None:
    live = _live(
        blockers=[
            QUEUES_UNAVAILABLE,
            _blocker(
                "generation.latest_published",
                "no latest published generation is available",
                {"status": "missing"},
            ),
        ],
        generation_published=False,
        mcp_ok=False,
    )
    payload = _status(
        tmp_path, monkeypatch, capsys, runtime_block=DISABLED, live=live,
    )
    assert payload["trust_level"] == "unavailable"
    assert _blocker_ids(payload) == ["generation.latest_published"]


def test_disabled_runtime_with_mcp_read_failing_is_unavailable(
    tmp_path, monkeypatch, capsys,
) -> None:
    live = _live(
        blockers=[
            QUEUES_UNAVAILABLE,
            _blocker("mcp.graph_read", "MCP graph-read alignment failed", {}),
        ],
        mcp_ok=False,
    )
    payload = _status(
        tmp_path, monkeypatch, capsys, runtime_block=DISABLED, live=live,
    )
    assert payload["trust_level"] == "unavailable"


def test_disabled_runtime_keeps_other_queue_failures(
    tmp_path, monkeypatch, capsys,
) -> None:
    """Only the absent-DBOS-history shape is not applicable."""
    broken = copy.deepcopy(DBOS_ABSENT_QUEUES)
    broken["reason"] = "okg.v_runtime_queue_health query failed"
    live = _live(
        blockers=[_blocker(
            "queues.unavailable", "runtime queue health is unavailable", broken,
        )],
        queues=broken,
    )
    payload = _status(
        tmp_path, monkeypatch, capsys, runtime_block=DISABLED, live=live,
    )
    assert payload["trust_level"] == "unavailable"
    assert _blocker_ids(payload) == ["queues.unavailable"]


def test_disabled_runtime_guard_keeps_queued_work_beside_missing_history(
    tmp_path, monkeypatch, capsys,
) -> None:
    """A guard, not a production shape.

    With the DBOS table missing, today's queue view always reports depth 0.
    If a later view reports queued work beside missing DBOS history, that
    work must not vanish with the not-applicable row.
    """
    queued = copy.deepcopy(DBOS_ABSENT_QUEUES)
    queued["active_queue_depth"] = 3
    queued["queue_depth"]["ingest"] = 3
    live = _live(
        blockers=[_blocker(
            "queues.unavailable", "runtime queue health is unavailable", queued,
        )],
        queues=queued,
    )
    payload = _status(
        tmp_path, monkeypatch, capsys, runtime_block=DISABLED, live=live,
    )
    assert payload["trust_level"] == "stale_context"
    assert _blocker_ids(payload) == ["queues.active_depth"]
    assert "runtime.enabled is false" in payload["blockers"][0]["message"]


def test_disabled_runtime_with_a_worker_running_is_a_warning(
    tmp_path, monkeypatch, capsys,
) -> None:
    live = _live(blockers=[QUEUES_UNAVAILABLE])
    payload = _status(
        tmp_path, monkeypatch, capsys,
        runtime_block=DISABLED, live=live, processes=ONE_WORKER,
    )
    assert payload["trust_level"] == "ready"
    assert _warning_ids(payload) == ["runtime.disabled_but_running"]


def test_disabled_runtime_with_failed_process_audit_is_a_warning(
    tmp_path, monkeypatch, capsys,
) -> None:
    """A stray worker could go unseen, so say so -- without turning red."""
    live = _live(blockers=[QUEUES_UNAVAILABLE])
    payload = _status(
        tmp_path, monkeypatch, capsys,
        runtime_block=DISABLED, live=live, processes=AUDIT_FAILED,
    )
    assert payload["trust_level"] == "ready"
    assert payload["blockers"] == []
    assert _warning_ids(payload) == ["runtime.process_audit_unavailable"]
    assert "ps returned 1" in payload["warnings"][0]["message"]
    assert payload["warnings"][0]["status"] == "warning"


def test_disabled_runtime_with_split_workers_is_still_a_blocker(
    tmp_path, monkeypatch, capsys,
) -> None:
    live = _live(blockers=[QUEUES_UNAVAILABLE])
    payload = _status(
        tmp_path, monkeypatch, capsys,
        runtime_block=DISABLED, live=live, processes=SPLIT_WORKERS,
    )
    assert payload["trust_level"] == "unavailable"
    assert _blocker_ids(payload) == ["runtime.worker_processes"]
    assert payload["runtime_worker"]["not_applicable_checks"] == [
        "queues.unavailable",
    ]
    assert payload["next_action"]["code"] == "resolve_split_runtime_worker"


# --- Enabled or undeclared runtime: dev behaviour ----------------------------


@pytest.mark.parametrize(
    "runtime_block",
    [ENABLED, None, {"dbos_pool_size": 4}, {"enabled": "no"}],
    ids=["enabled", "no-runtime-block", "enabled-key-absent", "unreadable"],
)
def test_runtime_not_declared_disabled_still_requires_a_worker(
    tmp_path, monkeypatch, capsys, runtime_block,
) -> None:
    live = _live(blockers=[])
    payload = _status(
        tmp_path, monkeypatch, capsys, runtime_block=runtime_block, live=live,
    )
    assert payload["trust_level"] == "unavailable"
    assert _blocker_ids(payload) == ["runtime.worker_processes"]
    assert payload["runtime_worker"]["status"] != "disabled_by_config"
    assert payload["next_action"]["code"] == "start_or_restart_worker"


def test_unreadable_runtime_block_is_reported(
    tmp_path, monkeypatch, capsys,
) -> None:
    payload = _status(
        tmp_path, monkeypatch, capsys,
        runtime_block={"enabled": "no"}, live=_live(blockers=[]),
    )
    assert payload["runtime_config"]["status"] == "unreadable"
    assert "enabled" in payload["runtime_config"]["error"]


# --- The chat banner ---------------------------------------------------------


def test_banner_is_healthy_for_a_disabled_runtime(
    tmp_path, monkeypatch, capsys,
) -> None:
    payload = _status(
        tmp_path, monkeypatch, capsys,
        runtime_block=DISABLED, live=_live(blockers=[QUEUES_UNAVAILABLE]),
    )
    reading = read_health(payload)
    assert reading.state == STATE_HEALTHY
    assert reading.severity is None


def test_degraded_banner_does_not_call_a_disabled_dlq_missing(
    tmp_path, monkeypatch, capsys,
) -> None:
    payload = _status(
        tmp_path, monkeypatch, capsys,
        runtime_block=DISABLED,
        live=_live(blockers=[SOURCE_LINEAGE, QUEUES_UNAVAILABLE]),
    )
    reading = read_health(payload)
    assert reading.state == STATE_DEGRADED
    assert CONDITION_LABELS["dlq_unreported"] not in reading.conditions


def test_degraded_banner_still_names_a_missing_dlq_when_runtime_enabled(
    tmp_path, monkeypatch, capsys,
) -> None:
    payload = _status(
        tmp_path, monkeypatch, capsys,
        runtime_block=ENABLED,
        live=_live(blockers=[SOURCE_LINEAGE], queues={
            **DBOS_ABSENT_QUEUES, "ok": True, "status": "available",
        }),
        processes=ONE_WORKER,
    )
    reading = read_health(payload)
    assert reading.state == STATE_DEGRADED
    assert CONDITION_LABELS["dlq_unreported"] in reading.conditions
