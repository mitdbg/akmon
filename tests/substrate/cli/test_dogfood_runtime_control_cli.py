from __future__ import annotations

import json
import os
from pathlib import Path
import signal

import pytest

from okg.substrate.cli.main import main
from okg.substrate.cli.dogfood import (
    WORKER_GRACEFUL_SHUTDOWN_SECONDS,
    _process_alive,
    _runtime_worker_command_matches,
    _runtime_worker_process_audit,
    _status_payload,
    _terminate_runtime_worker_roots,
)


@pytest.fixture(autouse=True)
def _ready_deployment_apply(monkeypatch):
    """Keep legacy dogfood tests focused on their explicitly mocked gate."""
    monkeypatch.setattr(
        "okg.substrate.deployment_apply.deployment_apply_health",
        lambda **kwargs: {
            "ok": True,
            "status": "ready",
            "managed": True,
            "deployment": kwargs["deployment"],
        },
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda _deployment: {
            "ok": True,
            "status": "single_executor",
            "process_count": 1,
            "root_count": 1,
            "split_executor": False,
            "roots": [{
                "pid": 101,
                "ppid": 1,
                "command": "okg runtime worker --deployment okg-workspace",
            }],
            "processes": [],
        },
    )


def test_status_payload_surfaces_deployment_apply_finding(monkeypatch) -> None:
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda _deployment: {
            "ok": True,
            "status": "single_executor",
            "process_count": 1,
            "root_count": 1,
            "split_executor": False,
            "roots": [],
        },
    )
    finding = {"ok": False, "status": "blocked", "problems": ["apply_failed"]}
    payload = _status_payload(
        deployment="test",
        environment={"ok": False},
        live={"deployment_apply": finding},
        resolved_dsn=False,
        include_details=False,
    )
    assert payload["deployment_apply"] == finding


def test_runtime_worker_command_matcher_accepts_supported_entrypoints() -> None:
    deployment = "okg-workspace"
    accepted = (
        "/opt/homebrew/bin/uv run okg runtime worker --deployment okg-workspace",
        (
            "/opt/homebrew/bin/uv run --module okg.substrate.cli.main "
            "runtime worker --deployment okg-workspace"
        ),
        (
            "/repo/.venv/bin/python -m okg.substrate.cli.main runtime worker "
            "--deployment=okg-workspace"
        ),
        (
            "/Python /repo/.venv/bin/okg runtime worker "
            "--deployment okg-workspace"
        ),
        "/repo/.venv/bin/okg runtime worker --deployment okg-workspace",
    )

    assert all(
        _runtime_worker_command_matches(command, deployment)
        for command in accepted
    )


def test_runtime_worker_command_matcher_accepts_the_worker_module_form() -> None:
    """The sealed-release adapter runs the worker MODULE, not the CLI subcommand.

    Its argv carries ``okg.substrate.runtime.worker`` as one dotted token, so a
    scan for the adjacent ``runtime worker`` pair never fires. Before #1327 this
    made every sealed-release deployment report ``process_count: 0`` while its
    worker was running, which in turn made ``split_executor`` — the
    two-workers-on-one-database hazard — permanently undetectable.
    """
    deployment = "okg-workspace"
    accepted = (
        # Verbatim from okg-workspace production, pid 37231.
        (
            "/Users/example/.local/share/okg/artifacts/5bbad4ce/venv/bin/python3 -I "
            "-m okg.substrate.runtime.worker --deployment okg-workspace"
        ),
        (
            "/repo/.venv/bin/python -m okg.substrate.runtime.worker "
            "--deployment=okg-workspace"
        ),
        (
            "/opt/homebrew/bin/uv run --module okg.substrate.runtime.worker "
            "--deployment okg-workspace"
        ),
    )

    for command in accepted:
        assert _runtime_worker_command_matches(command, deployment), command


def test_runtime_worker_module_form_still_rejects_decoys() -> None:
    deployment = "okg-workspace"
    rejected = (
        # Another deployment's worker must not be attributed to this one.
        (
            "/repo/.venv/bin/python -I -m okg.substrate.runtime.worker "
            "--deployment another-deployment"
        ),
        # A --deployment flag with no value selects nothing.
        "/repo/.venv/bin/python -m okg.substrate.runtime.worker --deployment",
        # A neighbouring module is not the worker.
        (
            "/repo/.venv/bin/python -m okg.substrate.runtime.monitor "
            "--deployment okg-workspace"
        ),
        # Text mentioning the module is not an invocation of it.
        (
            "rg -m okg.substrate.runtime.worker --deployment okg-workspace"
        ),
    )

    for command in rejected:
        assert not _runtime_worker_command_matches(command, deployment), command


def test_runtime_worker_command_matcher_rejects_decoys() -> None:
    deployment = "okg-workspace"
    rejected = (
        "rg okg runtime worker --deployment okg-workspace",
        "echo okg runtime worker --deployment okg-workspace",
        "uv run okg runtime worker --deployment another-deployment",
        "uv run okg runtime monitor --deployment okg-workspace",
        "python verifier.py okg runtime worker --deployment okg-workspace",
        "uv run echo okg runtime worker --deployment okg-workspace",
        "uv run python -c okg runtime worker --deployment okg-workspace",
        "uv run okg runtime worker --deployment",
    )

    assert not any(
        _runtime_worker_command_matches(command, deployment)
        for command in rejected
    )


def test_runtime_worker_process_audit_groups_uv_and_python_module_rows(
    monkeypatch,
) -> None:
    class Result:
        returncode = 0
        stderr = ""
        stdout = "\n".join(
            (
                "100 1 /opt/homebrew/bin/uv run --module "
                "okg.substrate.cli.main runtime worker --deployment okg-workspace",
                "101 100 /Framework/Python -m okg.substrate.cli.main "
                "runtime worker --deployment okg-workspace",
                "102 1 rg okg runtime worker --deployment okg-workspace",
            )
        )

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood.subprocess.run",
        lambda *args, **kwargs: Result(),
    )

    audit = _runtime_worker_process_audit("okg-workspace")

    assert audit["status"] == "single_executor"
    assert audit["ok"] is True
    assert audit["process_count"] == 2
    assert audit["root_count"] == 1
    assert audit["roots"][0]["pid"] == 100


@pytest.mark.parametrize(
    ("audit", "message"),
    (
        (
            {
                "ok": False,
                "status": "not_observed",
                "process_count": 0,
                "root_count": 0,
                "split_executor": False,
                "roots": [],
                "processes": [],
            },
            "no runtime worker process root is active",
        ),
        (
            {
                "ok": None,
                "status": "unavailable",
                "reason": "ps unavailable",
                "process_count": 0,
                "root_count": 0,
                "split_executor": False,
                "roots": [],
                "processes": [],
            },
            "runtime worker process authority could not be measured",
        ),
    ),
)
def test_dogfood_runtime_authority_requires_one_observed_process_root(
    monkeypatch,
    record_property,
    audit,
    message,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.readiness test.runtime-authority.readiness",
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda _deployment: audit,
    )

    payload = _status_payload(
        deployment="okg-workspace",
        environment={"ok": True},
        live=_published_live_payload(blocked=False),
        resolved_dsn=True,
        include_details=False,
    )

    process_blocker = next(
        blocker for blocker in payload["blockers"]
        if blocker["id"] == "runtime.worker_processes"
    )
    assert payload["trust_level"] == "unavailable"
    assert payload["read_authority"]["ok"] is False
    assert process_blocker["message"] == message
    assert payload["next_action"]["code"] == "start_or_restart_worker"


def test_orphan_cleanup_honors_worker_graceful_shutdown_budget(monkeypatch) -> None:
    clock = [0.0]
    signals: list[int] = []
    audit = {
        "roots": [
            {
                "pid": 101,
                "ppid": 1,
                "command": "uv run okg runtime worker --deployment okg-workspace",
            }
        ]
    }

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood.os.kill",
        lambda _pid, sent: signals.append(sent),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood.time.monotonic",
        lambda: clock[0],
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood.time.sleep",
        lambda seconds: clock.__setitem__(0, clock[0] + max(seconds, 10.0)),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._process_alive",
        lambda _pid, **_kwargs: clock[0] < 30.0,
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_identity",
        lambda _pid: {
            "available": True,
            "alive": True,
            "state": "S",
            "start_token": "Thu Jul 10 01:00:00 2026",
            "command": audit["roots"][0]["command"],
        },
    )

    result = _terminate_runtime_worker_roots(audit)

    assert WORKER_GRACEFUL_SHUTDOWN_SECONDS == 120.0
    assert signals == [signal.SIGTERM]
    assert result["signal"] == "SIGTERM"
    assert result["terminated"] == [101]
    assert result["still_running"] == []


def test_process_alive_treats_zombie_as_exited(monkeypatch) -> None:
    class Result:
        returncode = 0
        stdout = "Z+\n"

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood.os.kill",
        lambda _pid, _signal: None,
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood.subprocess.run",
        lambda *_args, **_kwargs: Result(),
    )

    assert _process_alive(101) is False


def test_orphan_cleanup_never_kills_reused_pid(monkeypatch) -> None:
    expected_command = "uv run okg runtime worker --deployment okg-workspace"
    identities = iter(
        (
            {
                "available": True,
                "alive": True,
                "state": "S",
                "start_token": "Thu Jul 10 01:00:00 2026",
                "command": expected_command,
            },
            {
                "available": True,
                "alive": True,
                "state": "S",
                "start_token": "Thu Jul 10 01:00:01 2026",
                "command": expected_command,
            },
        )
    )
    sent: list[int] = []
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_identity",
        lambda _pid: next(identities),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood.os.kill",
        lambda _pid, signal_number: sent.append(signal_number),
    )

    result = _terminate_runtime_worker_roots(
        {
            "roots": [
                {
                    "pid": 101,
                    "ppid": 1,
                    "command": expected_command,
                }
            ]
        }
    )

    assert sent == [signal.SIGTERM, 0]
    assert signal.SIGKILL not in sent
    assert result["signal"] == "SIGTERM"
    assert result["still_running"] == []


def _env_payload(tmp_path: Path, *, ok: bool = True) -> dict:
    return {
        "ok": ok,
        "deployment": "okg-workspace",
        "manifest_path": str(tmp_path / "deployment.yaml"),
        "deployment_dir": str(tmp_path),
        "local_env_path": str(tmp_path / ".env"),
        "postgres": {
            "declared": True,
            "resolved": ok,
            "required_env": ["OKG_DSN"],
            "resolved_dsn": "<redacted-dsn>" if ok else None,
        },
        "environment": [{
            "name": "OKG_DSN",
            "required": True,
            "set": ok,
            "source": "local_profile" if ok else "missing",
            "value": "<redacted-dsn>" if ok else None,
        }],
        "unresolved": [] if ok else ["OKG_DSN"],
        "recommended_commands": (
            []
            if ok
            else [
                "uv run okg deployment env okg-workspace "
                "--write-local --env-name OKG_DSN --dsn postgres://..."
            ]
        ),
    }


def _published_live_payload(*, blocked: bool) -> dict:
    source_freshness = {
        "ok": not blocked,
        "missing": [],
        "stale": ["okg-workspace.codex-sessions"] if blocked else [],
    }
    runtime_worker = (
        {
            "ok": False,
            "status": "stale",
            "code": "runtime_services_overlay_stale",
            "message": "scheduled-doctor verdict is stale",
        }
        if blocked
        else {"ok": True, "status": "healthy"}
    )
    source_sync_status = {
        "schema": "okg.source_sync.status.v1",
        "deployment": "okg-workspace",
        "ok": not blocked,
        "status": "blocked" if blocked else "healthy",
        "blockers": (
            [{"code": "source_sync_runtime_unhealthy"}] if blocked else []
        ),
        "runtime_liveness": {
            "ok": not blocked,
            "status": "blocked" if blocked else "healthy",
            "worker": runtime_worker,
        },
        "source_freshness": source_freshness,
        "wakeups": {
            "ok": True,
            "status": "healthy",
            "dbos_workflows": {
                "available": True,
                "queue_name": "okg-okg-workspace",
                "active_count": 30 if blocked else 0,
                "active_rows": (
                    [
                        {"status": "ENQUEUED", "count": 29},
                        {"status": "PENDING", "count": 1},
                    ]
                    if blocked
                    else []
                ),
                "terminal_history_count": 180 if blocked else 0,
                "oldest_active_age_seconds": 900 if blocked else None,
                "newest_active_age_seconds": 30 if blocked else None,
                "stale_after_seconds": 600,
                "stale_replaceable_work_present": blocked,
            },
        },
        "recovery_commands": {
            "start_supervised_worker": (
                "uv run okg runtime worker --deployment okg-workspace"
            ),
            "refresh_poll_backstop": (
                "uv run okg sync poll --deployment okg-workspace --json"
            ),
        },
    }
    blockers = []
    if blocked:
        blockers = [
            {
                "id": "sources.freshness",
                "status": "blocker",
                "ok": False,
                "message": "required source is stale",
                "details": source_freshness,
            },
            {
                "id": "sources.sync_liveness",
                "status": "blocker",
                "ok": False,
                "message": "required producer is stale",
                "details": source_sync_status,
            },
        ]
    return {
        "checks": [],
        "blockers": blockers,
        "warnings": [],
        "database": {"missing_relations": []},
        "generation": {
            "generation_id": "okg:okg-workspace:gen:test",
            "status": "published",
        },
        "mcp": {
            "ok": True,
            "pinned_generation_id": "okg:okg-workspace:gen:test",
        },
        "doctor": {"ok": True},
        "queues": {
            "active_queue_depth": 0,
            "dlq_count": 0,
        },
        "source_freshness": source_freshness,
        "source_sync_status": source_sync_status,
    }


def test_dogfood_exact_runtime_authority_drift_is_unavailable(
    monkeypatch,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.readiness test.runtime-authority.readiness",
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda _deployment: {
            "ok": True,
            "status": "single_executor",
            "process_count": 1,
            "root_count": 1,
            "split_executor": False,
            "roots": [],
        },
    )
    live = _published_live_payload(blocked=False)
    apply_health = {
        "ok": False,
        "status": "blocked",
        "problems": ["stage_needed:worker_reload"],
    }
    live["deployment_apply"] = apply_health
    live["blockers"] = [{
        "id": "deployment.apply",
        "status": "blocker",
        "ok": False,
        "message": "deployment apply is incomplete, failed, or stale",
        "details": apply_health,
    }]

    payload = _status_payload(
        deployment="okg-workspace",
        environment={"ok": True},
        live=live,
        resolved_dsn=True,
        include_details=False,
    )

    assert payload["trust_level"] == "unavailable"
    assert payload["read_authority"]["ok"] is False


def test_dogfood_upstream_commit_drift_is_stale_context(
    monkeypatch,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.readiness test.runtime-authority.readiness",
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda _deployment: {
            "ok": True,
            "status": "single_executor",
            "process_count": 1,
            "root_count": 1,
            "split_executor": False,
            "roots": [],
        },
    )
    live = _published_live_payload(blocked=False)
    live["code_drift"] = {
        "ok": True,
        "status": "ok",
        "base_ref": "origin/dev",
        "commits_behind": 1,
        "commits_ahead": 0,
    }

    payload = _status_payload(
        deployment="okg-workspace",
        environment={"ok": True},
        live=live,
        resolved_dsn=True,
        include_details=False,
    )

    assert payload["trust_level"] == "stale_context"
    assert payload["read_authority"]["ok"] is True
    assert payload["code_drift"]["commits_behind"] == 1


def test_dogfood_runtime_authority_unmeasurable_code_drift_is_unavailable(
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.readiness test.runtime-authority.readiness",
    )
    live = _published_live_payload(blocked=False)
    code_drift = {
        "ok": False,
        "status": "error",
        "code": "code_drift_probe_failed",
        "message": "probe exploded",
    }
    live["code_drift"] = code_drift
    live["blockers"] = [{
        "id": "deployment.code_drift",
        "status": "blocker",
        "ok": False,
        "message": "code-drift probe failed: probe exploded",
        "details": code_drift,
    }]

    payload = _status_payload(
        deployment="okg-workspace",
        environment={"ok": True},
        live=live,
        resolved_dsn=True,
        include_details=False,
    )

    assert payload["trust_level"] == "unavailable"
    assert payload["read_authority"]["ok"] is False
    assert payload["code_drift"]["status"] == "error"


def test_dogfood_status_reports_stale_context_for_stale_worker(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    calls: dict[str, object] = {}

    def fake_env(args, *, show_values: bool):
        calls["show_values"] = show_values
        return _env_payload(tmp_path), "postgres://okg/test"

    def fake_live_checks(**kwargs):
        calls.update(kwargs)
        return _published_live_payload(blocked=True)

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_env_status",
        fake_env,
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_live_checks",
        fake_live_checks,
    )

    rc = main(["dogfood", "status", "--deployment", "okg-workspace", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert calls["readiness_profile"] == "dogfood"
    assert calls["skip_doctor"] is True
    assert calls["max_source_age_seconds"] == 60 * 60
    assert calls["max_producer_age_seconds"] == 15 * 60
    assert payload["schema"] == "okg.dogfood.runtime_status.v1"
    assert payload["trust_level"] == "stale_context"
    assert payload["read_authority"]["ok"] is True
    assert payload["runtime_worker"]["status"] == "stale"
    assert payload["queues"]["source_sync"]["dbos_workflows"][
        "active_count"
    ] == 30
    workflow_recovery = payload["workflow_recovery"]
    expected_recovery = {
        "ok": False,
        "status": "stale_replaceable_work_present",
        "workflow_name": "okg_source_sync",
        "queue_name": "okg-okg-workspace",
        "active_queue_names": [],
        "active_count": 30,
        "active_rows": [
            {"status": "ENQUEUED", "count": 29},
            {"status": "PENDING", "count": 1},
        ],
        "terminal_history_count": 180,
        "pending_count": 0,
        "waiting_count": 0,
        "fresh_pending_count": 0,
        "oldest_active_age_seconds": 900,
        "newest_active_age_seconds": 30,
        "oldest_pending_update_age_seconds": None,
        "oldest_waiting_age_seconds": None,
        "stale_pending_work_present": False,
        "stale_waiting_work_present": False,
        "stale_replaceable_work_present": True,
        "stale_after_seconds": 600,
        "safe_recovery": "dry_run_then_confirm_apply",
        "dry_run_command": (
            "env -u OKG_DSN uv run okg db dbos-gc --deployment okg-workspace "
            "--stale-after-seconds 600 --queue-name okg-okg-workspace "
            "--workflow-name okg_source_sync --status ENQUEUED --json"
        ),
        "apply_command": (
            "env -u OKG_DSN uv run okg db dbos-gc --deployment okg-workspace "
            "--stale-after-seconds 600 --queue-name okg-okg-workspace "
            "--workflow-name okg_source_sync --status ENQUEUED "
            "--confirm --apply --json"
        ),
    }
    assert {
        key: workflow_recovery.get(key)
        for key in expected_recovery
    } == expected_recovery
    assert payload["recovery_commands"]["source_sync_dbos_gc_dry_run"] == (
        "env -u OKG_DSN uv run okg db dbos-gc --deployment okg-workspace "
        "--stale-after-seconds 600 --queue-name okg-okg-workspace "
        "--workflow-name okg_source_sync --status ENQUEUED --json"
    )
    # The recommended command must carry `--apply`: the flagless form only
    # prints a plan, so an operator following the advice would see no change
    # and conclude the restart failed (issue #944).
    assert payload["next_action"] == {
        "code": "start_or_restart_worker",
        "owner": "operator",
        "command": (
            "env -u OKG_DSN uv run okg dogfood up "
            "--deployment okg-workspace --apply --json"
        ),
    }


def test_dogfood_status_blocks_on_nomos_active_state_mismatch(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    (tmp_path / "deployment.yaml").write_text(
        "name: okg-workspace\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        "nomos:\n"
        "  runtime_enforcement: enforce\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_env_status",
        lambda args, *, show_values: (
            _env_payload(tmp_path),
            "postgres://okg/test",
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_live_checks",
        lambda **kwargs: _published_live_payload(blocked=False),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._ready_nomos_active_state_status",
        lambda **kwargs: {
            "schema": "okg.nomos.runtime_active_state.v1",
            "ok": False,
            "status": "failed",
            "deployment": kwargs["deployment"],
            "effective_mode": "enforce",
            "blocks_actions": True,
            "findings": [{
                "code": "nomos.runtime.active_policy_mismatch",
                "reason_code": "nomos.runtime.active_policy_mismatch",
                "message": (
                    "active Nomos policy does not match the deployment "
                    "rollout baseline derived from this manifest"
                ),
                "expected": "sha256:expected",
                "actual": "sha256:actual",
            }],
        },
    )

    rc = main(["dogfood", "status", "--deployment", "okg-workspace", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["trust_level"] == "unavailable"
    assert payload["read_authority"]["ok"] is False
    assert payload["read_authority"]["mcp_graph_read"] is False
    assert payload["nomos_runtime"]["findings"][0]["code"] == (
        "nomos.runtime.active_policy_mismatch"
    )
    assert [blocker["id"] for blocker in payload["blockers"]] == [
        "nomos.active_state"
    ]
    # Issue #1053: the activation is present but stale. Reporting it as
    # "missing" sends the reader hunting for state the database already holds.
    blocker_message = payload["blockers"][0]["message"]
    assert "missing" not in blocker_message
    assert "does not match the deployment rollout baseline" in blocker_message


def test_dogfood_status_details_runs_doctor(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    seen: dict[str, object] = {}

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_env_status",
        lambda args, *, show_values: (
            _env_payload(tmp_path),
            "postgres://okg/test",
        ),
    )

    def fake_live_checks(**kwargs):
        seen.update(kwargs)
        return _published_live_payload(blocked=False)

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_live_checks",
        fake_live_checks,
    )

    rc = main([
        "dogfood",
        "status",
        "--deployment",
        "okg-workspace",
        "--details",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert seen["skip_doctor"] is False
    assert "details" in payload


def test_dogfood_status_prefers_source_sync_recovery_when_work_active(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    def fake_live_checks(**kwargs):
        payload = _published_live_payload(blocked=True)
        payload["source_sync_status"]["runtime_liveness"]["ok"] = True
        payload["source_sync_status"]["runtime_liveness"]["status"] = "healthy"
        payload["source_sync_status"]["runtime_liveness"]["worker"] = {
            "ok": True,
            "status": "healthy",
            "code": "runtime_services_healthy",
        }
        payload["source_sync_status"]["blockers"] = [
            {"code": "source_freshness_not_ready", "stale_count": 1},
        ]
        payload["source_sync_status"]["ok"] = False
        return payload

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_env_status",
        lambda args, *, show_values: (
            _env_payload(tmp_path),
            "postgres://okg/test",
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_live_checks",
        fake_live_checks,
    )

    rc = main(["dogfood", "status", "--deployment", "okg-workspace", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["runtime_worker"]["status"] == "healthy"
    assert payload["workflow_recovery"]["active_count"] == 30
    assert payload["next_action"] == {
        "code": "inspect_or_recover_source_sync_work",
        "owner": "runtime-operator",
        "command": (
            "env -u OKG_DSN uv run okg db dbos-gc --deployment okg-workspace "
            "--stale-after-seconds 600 --queue-name okg-okg-workspace "
            "--workflow-name okg_source_sync --status ENQUEUED --json"
        ),
    }


def test_dogfood_status_downgrades_for_blocked_source_sync_contract(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    def fake_live_checks(**kwargs):
        payload = _published_live_payload(blocked=False)
        payload["source_sync_status"]["ok"] = False
        payload["source_sync_status"]["status"] = "blocked"
        payload["source_sync_status"]["blockers"] = [{
            "code": "source_sync_backlog_stale_replaceable",
            "active_count": 30,
            "oldest_active_age_seconds": 900,
            "stale_after_seconds": 600,
        }]
        payload["source_sync_status"]["backlog"] = {
            "ok": False,
            "status": "stale_replaceable_work_present",
            "active_count": 30,
            "dbos_active_count": 30,
            "terminal_history_count": 180,
            "dlq_count": 0,
            "oldest_active_age_seconds": 900,
            "newest_active_age_seconds": 10,
            "stale_after_seconds": 600,
            "stale_replaceable_work_present": True,
        }
        payload["source_sync_status"]["wakeups"]["dbos_workflows"] = {
            "available": True,
            "queue_name": "okg-okg-workspace",
            "active_count": 30,
            "active_rows": [
                {"status": "ENQUEUED", "count": 29},
                {"status": "PENDING", "count": 1},
            ],
            "terminal_history_count": 180,
        }
        return payload

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_env_status",
        lambda args, *, show_values: (
            _env_payload(tmp_path),
            "postgres://okg/test",
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_live_checks",
        fake_live_checks,
    )
    monkeypatch.setattr(
        "okg.substrate.db.management.dbos_backlog_gc",
        lambda *args, **kwargs: {
            "present": True,
            "dry_run": False,
            "eligible_count": 0,
            "cancelled_count": 0,
            "queue_name": kwargs["queue_name"],
            "workflow_name": kwargs["workflow_name"],
        },
    )

    rc = main(["dogfood", "status", "--deployment", "okg-workspace", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["trust_level"] == "stale_context"
    assert payload["blockers"][0]["id"] == "sources.sync_liveness"
    assert payload["blockers"][0]["details"]["backlog"]["status"] == (
        "stale_replaceable_work_present"
    )
    assert payload["next_action"] == {
        "code": "inspect_or_recover_source_sync_work",
        "owner": "runtime-operator",
        "command": (
            "env -u OKG_DSN uv run okg db dbos-gc --deployment okg-workspace "
            "--stale-after-seconds 600 --queue-name okg-okg-workspace "
            "--workflow-name okg_source_sync --status ENQUEUED --json"
        ),
    }


def test_dogfood_status_reports_recovery_plan_without_applying_gc(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    live_calls = []

    def stale_payload() -> dict:
        payload = _published_live_payload(blocked=False)
        source_sync = payload["source_sync_status"]
        source_sync["ok"] = False
        source_sync["status"] = "blocked"
        source_sync["blockers"] = [{
            "code": "source_sync_backlog_stale_replaceable",
            "active_count": 4,
            "oldest_active_age_seconds": 1200,
            "stale_after_seconds": 600,
        }]
        source_sync["backlog"] = {
            "ok": False,
            "status": "stale_replaceable_work_present",
            "active_count": 4,
            "dbos_active_count": 4,
            "terminal_history_count": 10,
            "dlq_count": 0,
            "oldest_active_age_seconds": 1200,
            "newest_active_age_seconds": 700,
            "stale_after_seconds": 600,
            "stale_replaceable_work_present": True,
        }
        source_sync["wakeups"]["dbos_workflows"] = {
            "available": True,
            "queue_name": "okg-okg-workspace-source-sync",
            "active_count": 4,
            "active_rows": [
                {"status": "ENQUEUED", "count": 3},
                {"status": "PENDING", "count": 1},
            ],
            "terminal_history_count": 10,
            "stale_after_seconds": 600,
            "stale_replaceable_work_present": True,
        }
        payload["blockers"] = [{
            "id": "sources.sync_liveness",
            "status": "blocker",
            "ok": False,
            "message": "source-sync status is blocked",
            "details": source_sync,
        }]
        return payload

    def fake_live_checks(**kwargs):
        live_calls.append(kwargs)
        return stale_payload()

    gc_calls = []

    def fake_gc(dsn: str, **kwargs):
        gc_calls.append({"dsn": dsn, **kwargs})
        return {
            "present": True,
            "dry_run": True,
            "eligible_count": 4,
            "cancelled_count": 0,
            "queue_name": kwargs["queue_name"],
            "workflow_name": kwargs["workflow_name"],
            "sample": [{"workflow_uuid": "w1"}],
        }

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_env_status",
        lambda args, *, show_values: (
            _env_payload(tmp_path),
            "postgres://okg/test",
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_live_checks",
        fake_live_checks,
    )
    monkeypatch.setattr(
        "okg.substrate.db.management.dbos_backlog_gc",
        fake_gc,
    )

    rc = main(["dogfood", "status", "--deployment", "okg-workspace", "--json"])

    assert rc == 1
    assert len(live_calls) == 1
    assert gc_calls == [{
        "dsn": "postgres://okg/test",
        "stale_after_seconds": 600,
        "confirm": False,
        "queue_name": "okg-okg-workspace-source-sync",
        "workflow_name": "okg_source_sync",
        "statuses": ("ENQUEUED",),
        "sample_limit": 20,
    }]
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert payload["trust_level"] == "stale_context"
    assert "source_sync_auto_recovery" not in payload
    assert payload["source_sync_recovery_plan"]["dry_run"] is True
    assert payload["source_sync_recovery_plan"]["eligible_count"] == 4
    assert payload["source_sync_recovery_plan"]["cancelled_count"] == 0
    assert payload["workflow_recovery"]["recovery_plan"]["eligible_count"] == 4
    assert payload["next_action"] == {
        "code": "inspect_or_recover_source_sync_work",
        "owner": "runtime-operator",
        "command": (
            "env -u OKG_DSN uv run okg db dbos-gc --deployment okg-workspace "
            "--stale-after-seconds 600 "
            "--queue-name okg-okg-workspace-source-sync "
            "--workflow-name okg_source_sync --status ENQUEUED --json"
        ),
    }


def test_dogfood_status_reports_ready_when_gate_is_clean(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_env_status",
        lambda args, *, show_values: (
            _env_payload(tmp_path),
            "postgres://okg/test",
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_live_checks",
        lambda **kwargs: _published_live_payload(blocked=False),
    )

    rc = main(["dogfood", "status", "--deployment", "okg-workspace", "--json"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["deployment"] == "okg-workspace"
    assert payload["ok"] is True
    assert payload["trust_level"] == "ready"
    assert payload["next_action"]["code"] == "none"


def test_dogfood_status_blocks_split_runtime_worker_processes(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_env_status",
        lambda args, *, show_values: (
            _env_payload(tmp_path),
            "postgres://okg/test",
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_live_checks",
        lambda **kwargs: _published_live_payload(blocked=False),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda deployment: {
            "ok": False,
            "status": "split_executor",
            "process_count": 3,
            "root_count": 2,
            "split_executor": True,
            "roots": [
                {
                    "pid": 101,
                    "ppid": 1,
                    "command": (
                        "uv run okg runtime worker --deployment okg-workspace"
                    ),
                },
                {
                    "pid": 202,
                    "ppid": 1,
                    "command": (
                        "python -m okg runtime worker --deployment okg-workspace"
                    ),
                },
            ],
            "processes": [],
        },
    )

    rc = main(["dogfood", "status", "--deployment", "okg-workspace", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["trust_level"] == "unavailable"
    assert payload["runtime_processes"]["status"] == "split_executor"
    assert payload["blockers"] == [{
        "id": "runtime.worker_processes",
        "status": "blocker",
        "ok": False,
        "message": "multiple independent runtime worker process roots are active",
        "details": {
            "ok": False,
            "status": "split_executor",
            "process_count": 3,
            "root_count": 2,
            "split_executor": True,
            "roots": [
                {
                    "pid": 101,
                    "ppid": 1,
                    "command": (
                        "uv run okg runtime worker --deployment okg-workspace"
                    ),
                },
                {
                    "pid": 202,
                    "ppid": 1,
                    "command": (
                        "python -m okg runtime worker --deployment okg-workspace"
                    ),
                },
            ],
        },
    }]
    assert payload["next_action"] == {
        "code": "resolve_split_runtime_worker",
        "owner": "operator",
        "command": (
            "uv run okg dogfood down --deployment okg-workspace --apply --json"
        ),
    }


def test_dogfood_status_reports_unavailable_when_environment_unresolved(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    live_called = False

    def fake_live_checks(**kwargs):
        nonlocal live_called
        live_called = True
        return {}

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_env_status",
        lambda args, *, show_values: (_env_payload(tmp_path, ok=False), None),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_live_checks",
        fake_live_checks,
    )

    rc = main(["dogfood", "status", "--deployment", "okg-workspace", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert live_called is False
    assert payload["trust_level"] == "unavailable"
    assert payload["read_authority"]["ok"] is False
    assert payload["next_action"]["code"] == "configure_environment"


def test_dogfood_control_surface_plans_worker_actions(capsys) -> None:
    rc = main([
        "dogfood",
        "restart",
        "--deployment",
        "okg-workspace",
        "--adapter",
        "systemd",
        "--systemd-scope",
        "user",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "okg.dogfood.runtime_control.v1"
    assert payload["action"] == "restart"
    assert payload["adapter"] == "systemd"
    assert payload["apply"] is False
    assert payload["supported"] is True
    assert payload["commands"] == [{
        "argv": [
            "systemctl",
            "--user",
            "restart",
            "okg-runtime-worker@okg-workspace.service",
        ],
        "display": (
            "systemctl --user restart "
            "okg-runtime-worker@okg-workspace.service"
        ),
    }]
    assert payload["service_adapter"]["render"] == (
        "uv run okg dogfood service render --deployment okg-workspace "
        "--adapter systemd --json"
    )


def test_dogfood_launchd_restart_uses_down_up_sequence(capsys) -> None:
    rc = main([
        "dogfood",
        "restart",
        "--deployment",
        "okg-workspace",
        "--adapter",
        "launchd",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    displays = [row["display"] for row in payload["commands"]]
    plist = (
        Path.home()
        / "Library"
        / "LaunchAgents"
        / "org.okg.runtime-worker.okg-workspace.plist"
    )
    assert displays == [
        f"launchctl bootout gui/$(id -u) {plist}",
        f"launchctl bootstrap gui/$(id -u) {plist}",
        (
            f"launchctl enable gui/{os.getuid()}/"
            "org.okg.runtime-worker.okg-workspace"
        ),
    ]
    assert not any("kickstart" in display for display in displays)


def test_dogfood_launchd_restart_bootstraps_installed_but_absent_service(
    tmp_path,
    monkeypatch,
    capsys,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.reconcile test.runtime-authority.reconcile",
    )
    plist = tmp_path / "installed.plist"
    plist.write_text("<plist/>")
    command_batches: list[list[str]] = []
    audits = [
        {
            "ok": False,
            "status": "not_observed",
            "process_count": 0,
            "root_count": 0,
            "split_executor": False,
            "roots": [],
            "processes": [],
        },
        {
            "ok": True,
            "status": "single_executor",
            "process_count": 2,
            "root_count": 1,
            "split_executor": False,
            "roots": [{
                "pid": 303,
                "ppid": 1,
                "command": (
                    "uv run okg runtime worker --deployment cooper"
                ),
            }],
            "processes": [],
        },
    ]

    def run_commands(commands):
        displays = [row["display"] for row in commands]
        command_batches.append(displays)
        if displays[0].startswith("launchctl print"):
            return [{
                "command": displays[0],
                "returncode": 113,
                "stdout": "",
                "stderr": "Could not find service",
            }]
        return [
            {
                "command": display,
                "returncode": 0,
                "stdout": "",
                "stderr": "",
            }
            for display in displays
        ]

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._run_control_commands",
        run_commands,
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda deployment: audits.pop(0),
    )

    rc = main([
        "dogfood", "restart", "--deployment", "cooper",
        "--adapter", "launchd",
        "--launchd-plist", str(plist),
        "--apply", "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["restart_start_mode"] == "bootstrap_from_absent"
    assert payload["service_absence_probe"]["returncode"] == 113
    assert len(command_batches) == 2
    assert command_batches[0][0].startswith("launchctl print")
    assert command_batches[1][0].startswith("launchctl bootstrap")
    assert command_batches[1][1].startswith("launchctl enable")
    assert not any(
        display.startswith("launchctl bootout")
        for batch in command_batches
        for display in batch
    )


def test_dogfood_launchd_restart_blocks_when_worker_absence_is_unmeasurable(
    tmp_path,
    monkeypatch,
    capsys,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.reconcile test.runtime-authority.reconcile",
    )
    plist = tmp_path / "installed.plist"
    plist.write_text("<plist/>")
    command_batches: list[list[str]] = []

    def run_commands(commands):
        displays = [row["display"] for row in commands]
        command_batches.append(displays)
        return [{
            "command": displays[0],
            "returncode": 113,
            "stdout": "",
            "stderr": "Could not find service",
        }]

    unavailable = {
        "ok": None,
        "status": "unavailable",
        "reason": "ps unavailable",
        "process_count": 0,
        "root_count": 0,
        "split_executor": False,
        "roots": [],
        "processes": [],
    }
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._run_control_commands",
        run_commands,
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda deployment: dict(unavailable),
    )

    rc = main([
        "dogfood", "restart", "--deployment", "cooper",
        "--adapter", "launchd",
        "--launchd-plist", str(plist),
        "--apply", "--json",
    ])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert payload["restart_start_mode"] == "bootstrap_from_absent"
    assert payload["bootstrap_skipped"] == "runtime_worker_absence_unproven"
    assert payload["process_state"] == "runtime_worker_absence_unproven"
    assert payload["process_audit"]["status"] == "unavailable"
    assert len(command_batches) == 1
    assert command_batches[0][0].startswith("launchctl print")
    assert not any(
        display.startswith("launchctl bootstrap")
        for batch in command_batches
        for display in batch
    )


def test_dogfood_restart_apply_reports_split_runtime_worker(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._run_control_commands",
        lambda commands: [{
            "command": commands[0]["display"],
            "returncode": 0,
            "stdout": "",
            "stderr": "",
        }],
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda deployment: {
            "ok": False,
            "status": "split_executor",
            "process_count": 3,
            "root_count": 2,
            "split_executor": True,
            "roots": [
                {
                    "pid": 101,
                    "ppid": 1,
                    "command": (
                        "uv run okg runtime worker --deployment okg-workspace"
                    ),
                },
                {
                    "pid": 202,
                    "ppid": 1,
                    "command": (
                        "python -m okg runtime worker --deployment okg-workspace"
                    ),
                },
            ],
            "processes": [],
        },
    )

    rc = main([
        "dogfood",
        "restart",
        "--deployment",
        "okg-workspace",
        "--adapter",
        "systemd",
        "--systemd-scope",
        "user",
        "--apply",
        "--json",
    ])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert payload["process_state"] == "unexpected_runtime_worker_processes"
    assert payload["process_audit"]["status"] == "split_executor"
    assert payload["recovery_command"] == (
        "uv run okg dogfood down --deployment okg-workspace --apply --json"
    )


def test_dogfood_launchd_restart_cleans_orphans_before_bootstrap(
    monkeypatch,
    capsys,
) -> None:
    command_batches = []

    def fake_run(commands):
        command_batches.append([row["display"] for row in commands])
        return [
            {
                "command": row["display"],
                "returncode": 0,
                "stdout": "",
                "stderr": "",
            }
            for row in commands
        ]

    audits = [
        {
            "ok": False,
            "status": "single_executor",
            "process_count": 1,
            "root_count": 1,
            "split_executor": False,
            "roots": [{
                "pid": 101,
                "ppid": 1,
                "command": (
                    "uv run okg runtime worker --deployment okg-workspace"
                ),
            }],
            "processes": [],
        },
        {
            "ok": True,
            "status": "not_observed",
            "process_count": 0,
            "root_count": 0,
            "split_executor": False,
            "roots": [],
            "processes": [],
        },
        {
            "ok": True,
            "status": "single_executor",
            "process_count": 2,
            "root_count": 1,
            "split_executor": False,
            "roots": [{
                "pid": 303,
                "ppid": 1,
                "command": (
                    "uv run okg runtime worker --deployment okg-workspace"
                ),
            }],
            "processes": [],
        },
    ]

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._run_control_commands",
        fake_run,
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda deployment: audits.pop(0),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._terminate_runtime_worker_roots",
        lambda audit: {
            "attempted": True,
            "signal": "SIGTERM",
            "pids": [101],
            "terminated": [101],
            "still_running": [],
            "errors": [],
        },
    )

    rc = main([
        "dogfood",
        "restart",
        "--deployment",
        "okg-workspace",
        "--adapter",
        "launchd",
        "--apply",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["orphan_recovery"]["pids"] == [101]
    assert payload["process_audit"]["status"] == "single_executor"
    assert command_batches[0][0].startswith("launchctl print")
    assert command_batches[1][0].startswith("launchctl bootout")
    assert command_batches[2][0].startswith("launchctl bootstrap")
    assert command_batches[2][1].startswith("launchctl enable")


def test_dogfood_launchd_restart_refuses_bootstrap_while_old_worker_lingers(
    monkeypatch,
    capsys,
) -> None:
    command_batches: list[list[str]] = []

    def fake_run(commands):
        command_batches.append([row["display"] for row in commands])
        return [
            {
                "command": row["display"],
                "returncode": 0,
                "stdout": "",
                "stderr": "",
            }
            for row in commands
        ]

    lingering = {
        "ok": True,
        "status": "single_executor",
        "process_count": 1,
        "root_count": 1,
        "split_executor": False,
        "roots": [
            {
                "pid": 101,
                "ppid": 1,
                "command": (
                    "uv run okg runtime worker --deployment okg-workspace"
                ),
            }
        ],
        "processes": [],
    }
    audits = [dict(lingering), dict(lingering), dict(lingering)]
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._run_control_commands",
        fake_run,
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda _deployment: audits.pop(0),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._terminate_runtime_worker_roots",
        lambda _audit: {
            "attempted": True,
            "signal": "SIGTERM",
            "pids": [101],
            "terminated": [],
            "still_running": [101],
            "identity_mismatches": [],
            "errors": [],
        },
    )

    rc = main(
        [
            "dogfood",
            "restart",
            "--deployment",
            "okg-workspace",
            "--adapter",
            "launchd",
            "--apply",
            "--json",
        ]
    )

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert payload["bootstrap_skipped"] == "runtime_worker_still_observed"
    assert payload["process_state"] == "old_runtime_worker_still_observed"
    assert len(command_batches) == 2
    assert command_batches[0][0].startswith("launchctl print")
    assert command_batches[1][0].startswith("launchctl bootout")
    assert not any(
        "launchctl bootstrap" in command
        for batch in command_batches
        for command in batch
    )


def test_dogfood_down_apply_terminates_orphan_worker_roots(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._run_control_commands",
        lambda commands: [{
            "command": commands[0]["display"],
            "returncode": 0,
            "stdout": "",
            "stderr": "",
        }],
    )
    audits = [
        {
            "ok": False,
            "status": "split_executor",
            "process_count": 2,
            "root_count": 2,
            "split_executor": True,
            "roots": [
                {
                    "pid": 101,
                    "ppid": 1,
                    "command": (
                        "uv run okg runtime worker --deployment okg-workspace"
                    ),
                },
                {
                    "pid": 202,
                    "ppid": 1,
                    "command": (
                        "python -m okg runtime worker --deployment okg-workspace"
                    ),
                },
            ],
            "processes": [],
        },
        {
            "ok": True,
            "status": "not_observed",
            "process_count": 0,
            "root_count": 0,
            "split_executor": False,
            "roots": [],
            "processes": [],
        },
    ]

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda deployment: audits.pop(0),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._terminate_runtime_worker_roots",
        lambda audit: {
            "attempted": True,
            "signal": "SIGTERM",
            "pids": [101, 202],
            "terminated": [101, 202],
            "still_running": [],
            "errors": [],
        },
    )

    rc = main([
        "dogfood",
        "down",
        "--deployment",
        "okg-workspace",
        "--adapter",
        "systemd",
        "--systemd-scope",
        "user",
        "--apply",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["orphan_recovery"]["pids"] == [101, 202]
    assert payload["process_audit"]["process_count"] == 0


def test_dogfood_systemd_down_apply_uses_stop(
    monkeypatch,
    capsys,
) -> None:
    command_batches: list[list[dict[str, object]]] = []

    def fake_run(commands):
        command_batches.append(commands)
        return [{
            "command": commands[0]["display"],
            "returncode": 0,
            "stdout": "",
            "stderr": "",
        }]

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._run_control_commands",
        fake_run,
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda deployment: {
            "ok": True,
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
        "ci-example",
        "--adapter",
        "systemd",
        "--apply",
        "--json",
    ])

    assert rc == 0
    assert command_batches == [[{
        "argv": [
            "systemctl",
            "stop",
            "okg-runtime-worker@ci-example.service",
        ],
        "display": "systemctl stop okg-runtime-worker@ci-example.service",
    }]]
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True


def test_dogfood_down_apply_does_not_hide_failed_command_with_stopped_state(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._run_control_commands",
        lambda commands: [{
            "command": commands[0]["display"],
            "returncode": 5,
            "stdout": "",
            "stderr": "Boot-out failed: 5: Input/output error",
        }],
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda deployment: {
            "ok": True,
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
        "okg-workspace",
        "--adapter",
        "systemd",
        "--systemd-scope",
        "user",
        "--apply",
        "--json",
    ])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert payload["final_state"] == "stopped_after_failed_command"
    assert payload["process_audit"]["status"] == "not_observed"


def test_dogfood_down_launchd_treats_proven_absence_as_success(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    command_batches: list[list[str]] = []

    def run_commands(commands):
        command_batches.append([row["display"] for row in commands])
        return [{
            "command": commands[0]["display"],
            "returncode": 113,
            "stdout": "",
            "stderr": "Could not find service",
        }]

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._run_control_commands",
        run_commands,
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda deployment: {
            "ok": True,
            "status": "not_observed",
            "process_count": 0,
            "root_count": 0,
            "split_executor": False,
            "roots": [],
            "processes": [],
        },
    )

    rc = main([
        "dogfood", "down", "--deployment", "cooper",
        "--adapter", "launchd",
        "--launchd-plist", str(tmp_path / "never-installed.plist"),
        "--apply", "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["final_state"] == "already_absent"
    assert payload["results"] == []
    assert len(command_batches) == 1
    assert command_batches[0][0].startswith("launchctl print")


def test_dogfood_down_launchd_does_not_treat_unknown_status_as_absence(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    command_batches: list[list[str]] = []

    def run_commands(commands):
        command_batches.append([row["display"] for row in commands])
        return [{
            "command": commands[0]["display"],
            "returncode": 5,
            "stdout": "",
            "stderr": "Input/output error",
        }]

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._run_control_commands",
        run_commands,
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda deployment: {
            "ok": True,
            "status": "not_observed",
            "process_count": 0,
            "root_count": 0,
            "split_executor": False,
            "roots": [],
            "processes": [],
        },
    )

    rc = main([
        "dogfood", "down", "--deployment", "cooper",
        "--adapter", "launchd",
        "--launchd-plist", str(tmp_path / "missing.plist"),
        "--apply", "--json",
    ])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert payload["final_state"] == "stopped_after_failed_command"
    # Pre-probe, failed bootout, then the post-failure not-loaded probe —
    # which sees the same unknown error and correctly refuses to call it
    # absence.
    assert len(command_batches) == 3
    assert command_batches[0][0].startswith("launchctl print")
    assert command_batches[1][0].startswith("launchctl bootout")
    assert command_batches[2][0].startswith("launchctl print")


def test_dogfood_down_launchd_not_loaded_after_failed_bootout_succeeds(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    """A bootout that fails because the service was never loaded is a
    successful stop: the plist exists on disk, launchctl print proves the
    service is not loaded, and no worker processes remain (the 2026-07-22
    deployment-up stop-phase failure)."""
    plist = tmp_path / "installed.plist"
    plist.write_text("<plist/>")
    command_batches: list[list[str]] = []

    def run_commands(commands):
        display = commands[0]["display"]
        command_batches.append([row["display"] for row in commands])
        if "bootout" in display:
            return [{
                "command": display,
                "returncode": 5,
                "stdout": "",
                "stderr": "Boot-out failed: 5: Input/output error",
            }]
        return [{
            "command": display,
            "returncode": 113,
            "stdout": "",
            "stderr": "Could not find service",
        }]

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._run_control_commands",
        run_commands,
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda deployment: {
            "ok": True,
            "status": "not_observed",
            "process_count": 0,
            "root_count": 0,
            "split_executor": False,
            "roots": [],
            "processes": [],
        },
    )

    rc = main([
        "dogfood", "down", "--deployment", "cooper",
        "--adapter", "launchd",
        "--launchd-plist", str(plist),
        "--apply", "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["final_state"] == "absent_after_failed_command"
    assert payload["service_absence_probe_after_failure"]["returncode"] == 113
    assert command_batches[0][0].startswith("launchctl bootout")
    assert command_batches[1][0].startswith("launchctl print")


def test_dogfood_down_launchd_absence_with_racing_worker_fails(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._run_control_commands",
        lambda commands: [{
            "command": commands[0]["display"],
            "returncode": 113,
            "stdout": "",
            "stderr": "Could not find service",
        }],
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda deployment: {
            "ok": False,
            "status": "single_executor",
            "process_count": 1,
            "root_count": 1,
            "split_executor": False,
            "roots": [{"pid": 101, "ppid": 1, "command": "worker"}],
            "processes": [{"pid": 101, "ppid": 1, "command": "worker"}],
        },
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._terminate_runtime_worker_roots",
        lambda audit: pytest.fail("proven-absent path must not hide a racing worker"),
    )

    rc = main([
        "dogfood", "down", "--deployment", "cooper",
        "--adapter", "launchd",
        "--launchd-plist", str(tmp_path / "missing.plist"),
        "--apply", "--json",
    ])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert payload["process_state"] == "unexpected_runtime_worker_processes"


def test_dogfood_doctor_uses_status_contract_with_details(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    seen: dict[str, object] = {}

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_env_status",
        lambda args, *, show_values: (
            _env_payload(tmp_path),
            "postgres://okg/test",
        ),
    )

    def fake_live_checks(**kwargs):
        seen.update(kwargs)
        return _published_live_payload(blocked=False)

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._deployment_ready_live_checks",
        fake_live_checks,
    )

    rc = main(["dogfood", "doctor", "--deployment", "okg-workspace", "--json"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "okg.dogfood.runtime_status.v1"
    assert payload["ok"] is True
    assert "details" in payload
    assert seen["readiness_profile"] == "dogfood"

def test_runtime_worker_process_audit_reports_old_and_new_roots_as_split(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Result:
        returncode = 0
        stderr = ""
        stdout = "\n".join(
            (
                "100 1 /artifact/venv/bin/python3 -I -m "
                "okg.substrate.runtime.worker --deployment okg-workspace",
                "200 1 /opt/homebrew/bin/uv run --module "
                "okg.substrate.cli.main runtime worker --deployment okg-workspace",
                "201 200 /repo/.venv/bin/python -m okg.substrate.cli.main "
                "runtime worker --deployment okg-workspace",
            )
        )

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood.subprocess.run",
        lambda *args, **kwargs: Result(),
    )
    audit = _runtime_worker_process_audit("okg-workspace")
    assert audit["ok"] is False
    assert audit["status"] == "split_executor"
    assert audit["split_executor"] is True
    assert audit["root_count"] == 2
    assert [row["pid"] for row in audit["roots"]] == [100, 200]
