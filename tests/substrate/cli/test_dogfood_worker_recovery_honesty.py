"""Dogfood worker recovery must act, and must not misreport worker liveness.

Covers PACT `bound-dogfood-worker-recovery-honesty` (issue #944):

* recommended supervisor-control commands actually apply,
* a plan-only control run is labeled `planned`, not reported as done,
* an unmeasured heartbeat next to a proven live worker process is a telemetry
  gap, not a down worker, and must not trigger a restart recommendation.
"""
from __future__ import annotations

import json

import pytest

from okg.substrate.cli.dogfood import (
    _default_recovery_commands,
    _next_action,
    _status_payload,
)
from okg.substrate.cli.main import main


_SINGLE_EXECUTOR_AUDIT = {
    "ok": True,
    "status": "single_executor",
    "process_count": 2,
    "root_count": 1,
    "split_executor": False,
    "roots": [{
        "pid": 39474,
        "ppid": 1,
        "command": (
            "uv run --module okg.substrate.cli.main runtime worker "
            "--deployment cooper"
        ),
    }],
    "processes": [],
}


@pytest.fixture(autouse=True)
def _ready_deployment_apply(monkeypatch):
    monkeypatch.setattr(
        "okg.substrate.deployment_apply.deployment_apply_health",
        lambda **kwargs: {
            "ok": True,
            "status": "ready",
            "managed": True,
            "deployment": kwargs["deployment"],
        },
    )


def _status(live, *, deployment="cooper"):
    return _status_payload(
        deployment=deployment,
        environment={"ok": True},
        live=live,
        resolved_dsn=True,
        include_details=False,
    )


# --- req.dogfood.recovery-command-acts ---------------------------------------


def test_recommended_recovery_commands_apply(monkeypatch) -> None:
    """The commands `dogfood status` hands an operator must change state."""
    commands = _default_recovery_commands("cooper")
    for key in ("dogfood_up", "dogfood_down", "dogfood_restart"):
        assert "--apply" in commands[key], key
    # Plan-only forms remain available under distinct keys.
    for key in ("dogfood_up_plan", "dogfood_down_plan", "dogfood_restart_plan"):
        assert "--apply" not in commands[key], key

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda _deployment: {
            "ok": False,
            "status": "not_observed",
            "process_count": 0,
            "root_count": 0,
            "split_executor": False,
            "roots": [],
            "processes": [],
        },
    )
    payload = _status({})
    assert payload["next_action"]["code"] == "start_or_restart_worker"
    assert "--apply" in payload["next_action"]["command"]


# --- req.dogfood.control-dry-run-legible -------------------------------------


def test_control_plan_only_run_is_labeled_planned(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._run_control_commands",
        lambda commands: pytest.fail("plan-only run must not execute commands"),
    )

    rc = main([
        "dogfood",
        "restart",
        "--deployment",
        "cooper",
        "--adapter",
        "systemd",
        "--systemd-scope",
        "user",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["apply"] is False
    assert payload["applied"] is False
    assert payload["status"] == "planned"
    assert "--apply" in payload["apply_command"]
    assert "restart" in payload["apply_command"]


def test_control_plan_only_text_output_says_nothing_ran(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._run_control_commands",
        lambda commands: pytest.fail("plan-only run must not execute commands"),
    )

    rc = main([
        "dogfood",
        "up",
        "--deployment",
        "cooper",
        "--adapter",
        "systemd",
        "--systemd-scope",
        "user",
    ])

    assert rc == 0
    out = capsys.readouterr().out
    assert "NOTHING WAS EXECUTED" in out
    assert "--apply" in out


def test_control_applied_run_is_labeled_applied(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._run_control_commands",
        lambda commands: [
            {
                "command": command["display"],
                "returncode": 0,
                "stdout": "",
                "stderr": "",
            }
            for command in commands
        ],
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda _deployment: _SINGLE_EXECUTOR_AUDIT,
    )

    rc = main([
        "dogfood",
        "up",
        "--deployment",
        "cooper",
        "--adapter",
        "systemd",
        "--systemd-scope",
        "user",
        "--apply",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["apply"] is True
    assert payload["applied"] is True
    assert payload["status"] == "applied"


def test_control_failed_apply_is_labeled_failed(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._run_control_commands",
        lambda commands: [
            {
                "command": command["display"],
                "returncode": 1,
                "stdout": "",
                "stderr": "boom",
            }
            for command in commands
        ],
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda _deployment: _SINGLE_EXECUTOR_AUDIT,
    )

    rc = main([
        "dogfood",
        "up",
        "--deployment",
        "cooper",
        "--adapter",
        "systemd",
        "--systemd-scope",
        "user",
        "--apply",
        "--json",
    ])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["applied"] is True
    assert payload["status"] == "failed"


# --- req.dogfood.worker-liveness-measured ------------------------------------


def test_process_observed_worker_is_not_reported_down(monkeypatch) -> None:
    """Issue #944's exact shape: worker running, heartbeat unreadable."""
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda _deployment: _SINGLE_EXECUTOR_AUDIT,
    )

    payload = _status({"source_sync_status": {"runtime_liveness": {}}})

    worker = payload["runtime_worker"]
    assert worker["measured"] is False
    assert worker["status"] == "process_observed_no_heartbeat"
    assert worker["ok"] is None
    assert worker["heartbeat_status"] == "not_observed"
    assert worker["process_evidence"]["root_pids"] == [39474]
    assert worker["requires"]

    next_action = payload["next_action"]
    assert next_action["code"] == "diagnose_runtime_worker_telemetry"
    assert next_action["code"] != "start_or_restart_worker"


def test_unmeasured_worker_without_process_evidence_stays_unmeasured(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda _deployment: {
            "ok": False,
            "status": "not_observed",
            "process_count": 0,
            "root_count": 0,
            "split_executor": False,
            "roots": [],
            "processes": [],
        },
    )

    payload = _status({"source_sync_status": {"runtime_liveness": {}}})

    worker = payload["runtime_worker"]
    assert worker["measured"] is False
    assert worker["status"] == "not_observed"
    # With no live worker root, the process audit is the authority and a
    # start/restart is the correct recommendation.
    assert payload["next_action"]["code"] == "start_or_restart_worker"


@pytest.mark.parametrize(
    "worker",
    [
        # A probe exception (stopped Postgres — issue #944's actual blocker)
        # collapses to ok:False/"unhealthy" upstream. That is not a
        # measurement of the worker; it is a failure to measure.
        {
            "ok": False,
            "status": "unhealthy",
            "code": "runtime_services_probe_failed",
        },
        {
            "ok": True,
            "status": "not_observed",
            "code": "runtime_services_other_deployment",
        },
        {
            "ok": True,
            "status": "not_observed",
            "code": "runtime_services_not_observed",
        },
    ],
    ids=["probe_failed", "other_deployment", "not_observed"],
)
def test_failed_probe_is_unmeasured_not_a_down_worker(
    monkeypatch,
    worker,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda _deployment: _SINGLE_EXECUTOR_AUDIT,
    )

    payload = _status({
        "source_sync_status": {"runtime_liveness": {"worker": worker}},
    })

    assert payload["runtime_worker"]["measured"] is False
    assert payload["runtime_worker"]["status"] == (
        "process_observed_no_heartbeat"
    )
    assert payload["next_action"]["code"] == (
        "diagnose_runtime_worker_telemetry"
    )


def test_stale_heartbeat_stays_measured_and_restarts(monkeypatch) -> None:
    """A stale overlay row IS an observation — it must keep its down-path."""
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda _deployment: _SINGLE_EXECUTOR_AUDIT,
    )

    payload = _status({
        "source_sync_status": {
            "runtime_liveness": {
                "worker": {
                    "ok": False,
                    "status": "stale",
                    "code": "runtime_services_overlay_stale",
                },
            },
        },
    })

    assert payload["runtime_worker"]["measured"] is True
    assert payload["next_action"]["code"] == "start_or_restart_worker"
    assert "--apply" in payload["next_action"]["command"]


def test_already_absent_down_is_not_reported_as_applied(
    monkeypatch,
    capsys,
) -> None:
    """A short-circuited no-op must not read as an executed action."""
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._launchd_target_is_absent",
        lambda plan: (True, {"status": "absent"}),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._run_control_commands",
        lambda commands: pytest.fail("already-absent down must execute nothing"),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda _deployment: {
            "ok": False,
            "status": "not_observed",
            "process_count": 0,
            "root_count": 0,
            "split_executor": False,
            "roots": [],
            "processes": [],
        },
    )

    rc = main([
        "dogfood",
        "down",
        "--deployment",
        "cooper",
        "--adapter",
        "launchd",
        "--apply",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["applied"] is False
    assert payload["status"] == "already_satisfied"
    assert payload["final_state"] == "already_absent"


def test_rendered_launchd_control_commands_apply(tmp_path) -> None:
    """The commands handed over right after `service install` must act too."""
    from okg.substrate.cli.dogfood import _render_launchd_adapter

    rendered = _render_launchd_adapter(
        deployment="cooper",
        repo_root=tmp_path,
        okg_bin="uv",
        dsn="postgres://localhost/cooper",
    )

    assert set(rendered["control_commands"]) == {"up", "down", "restart"}
    for action, command in rendered["control_commands"].items():
        assert "--apply" in command, action
        assert f"dogfood {action}" in command, action


def test_measured_down_worker_still_recommends_restart() -> None:
    payload = {
        "deployment": "cooper",
        "trust_level": "unavailable",
        "environment": {"ok": True},
        "runtime_processes": {"ok": True, "status": "single_executor"},
        "runtime_worker": {
            "ok": False,
            "status": "down",
            "measured": True,
        },
        "recovery_commands": _default_recovery_commands("cooper"),
        "blockers": [],
    }

    next_action = _next_action(payload)

    assert next_action["code"] == "start_or_restart_worker"
    assert "--apply" in next_action["command"]
