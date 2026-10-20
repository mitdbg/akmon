from __future__ import annotations

import json

from okg.substrate.cli.dogfood import _default_recovery_commands
from okg.substrate.cli.main import main
from okg.substrate.runtime.health_policy import runtime_health_policy


def test_dbos_gc_dry_run_explains_source_sync_without_mutating(
    monkeypatch,
    capsys,
) -> None:
    calls: dict[str, object] = {}

    def fake_gc(dsn: str, **kwargs):
        calls["dsn"] = dsn
        calls.update(kwargs)
        return {
            "present": True,
            "dry_run": not kwargs["confirm"],
            "eligible_count": 2,
            "cancelled_count": 0,
            "queue_name": kwargs["queue_name"],
            "workflow_name": kwargs["workflow_name"],
        }

    monkeypatch.setattr(
        "okg.substrate.cli.main._resolve_deployment_dsn",
        lambda args: "postgres://okg/test",
    )
    monkeypatch.setattr(
        "okg.substrate.db.management.dbos_backlog_gc",
        fake_gc,
    )

    rc = main([
        "--json",
        "db",
        "dbos-gc",
        "--deployment",
        "okg-workspace",
        "--stale-after-seconds",
        "600",
        "--queue-name",
        "okg-okg-workspace",
        "--workflow-name",
        "okg_source_sync",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["eligible_count"] == 2
    assert calls == {
        "dsn": "postgres://okg/test",
        "stale_after_seconds": 600,
        "confirm": False,
        "queue_name": "okg-okg-workspace",
        "workflow_name": "okg_source_sync",
        "statuses": ("ENQUEUED",),
        "sample_limit": 20,
    }


def test_runtime_health_policy_excludes_retired_job_runtime_repair() -> None:
    commands = _default_recovery_commands("okg-workspace")
    policy = runtime_health_policy(commands=commands)

    assert "job_runtime_reconcile_dry_run" not in commands
    assert "job_runtime_reconcile_apply" not in commands
    assert "job_runtime_drift" not in policy


def test_dbos_gc_confirm_requires_apply(monkeypatch, capsys) -> None:
    called = False

    def fake_gc(*args, **kwargs):
        nonlocal called
        called = True
        return {}

    monkeypatch.setattr(
        "okg.substrate.cli.main._resolve_deployment_dsn",
        lambda args: "postgres://okg/test",
    )
    monkeypatch.setattr(
        "okg.substrate.db.management.dbos_backlog_gc",
        fake_gc,
    )

    rc = main([
        "--json",
        "db",
        "dbos-gc",
        "--deployment",
        "okg-workspace",
        "--confirm",
    ])

    assert rc == 1
    assert called is False
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["message"] == "`okg db dbos-gc --confirm` requires --apply"


def test_dbos_gc_apply_requires_confirm(monkeypatch, capsys) -> None:
    called = False

    def fake_gc(*args, **kwargs):
        nonlocal called
        called = True
        return {}

    monkeypatch.setattr(
        "okg.substrate.cli.main._resolve_deployment_dsn",
        lambda args: "postgres://okg/test",
    )
    monkeypatch.setattr(
        "okg.substrate.db.management.dbos_backlog_gc",
        fake_gc,
    )

    rc = main([
        "--json",
        "db",
        "dbos-gc",
        "--deployment",
        "okg-workspace",
        "--apply",
    ])

    assert rc == 1
    assert called is False
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["message"] == "`okg db dbos-gc --apply` requires --confirm"


def test_dbos_gc_confirm_apply_runs_confirmed_recovery(
    monkeypatch,
    capsys,
) -> None:
    calls: dict[str, object] = {}

    def fake_gc(dsn: str, **kwargs):
        calls["dsn"] = dsn
        calls.update(kwargs)
        return {
            "present": True,
            "dry_run": not kwargs["confirm"],
            "eligible_count": 1,
            "cancelled_count": 1,
        }

    monkeypatch.setattr(
        "okg.substrate.cli.main._resolve_deployment_dsn",
        lambda args: "postgres://okg/test",
    )
    monkeypatch.setattr(
        "okg.substrate.db.management.dbos_backlog_gc",
        fake_gc,
    )

    rc = main([
        "--json",
        "db",
        "dbos-gc",
        "--deployment",
        "okg-workspace",
        "--queue-name",
        "okg-okg-workspace",
        "--workflow-name",
        "okg_source_sync",
        "--confirm",
        "--apply",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is False
    assert payload["cancelled_count"] == 1
    assert calls["confirm"] is True
    assert calls["queue_name"] == "okg-okg-workspace"
    assert calls["workflow_name"] == "okg_source_sync"
