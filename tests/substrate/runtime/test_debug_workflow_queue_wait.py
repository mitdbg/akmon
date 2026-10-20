"""Tests for `restore-publisher-perf-and-profiling` §C.

Verifies that `okg runtime debug-workflow` derives lifetime, work,
and queue_wait from the per-phase event durations + workflow row
timestamps.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone



def test_coerce_dt_handles_iso_string():
    from okg.substrate.cli.main import _coerce_dt

    out = _coerce_dt("2026-05-11T18:30:00+00:00")
    assert out.tzinfo is not None
    assert out.year == 2026 and out.minute == 30


def test_coerce_dt_handles_epoch_ms_int():
    from okg.substrate.cli.main import _coerce_dt

    # 2026-05-11 18:30:00 UTC ≈ 1778524200000 ms
    out = _coerce_dt(1778524200000)
    assert out.year == 2026 and out.minute == 30


def test_coerce_dt_handles_none():
    from okg.substrate.cli.main import _coerce_dt

    assert _coerce_dt(None) is None


def test_find_profile_for_workflow_returns_none_when_no_match(
    monkeypatch,
):
    from okg.substrate.cli import main as cli

    monkeypatch.setattr(
        "okg.substrate.profiling.list_profiles",
        lambda: [
            {"request_path": "some-other-uuid", "_path": "/tmp/x"},
        ],
    )
    out = cli._find_profile_for_workflow(workflow_id="target-uuid")
    assert out is None


def test_find_profile_for_workflow_returns_match(monkeypatch):
    from okg.substrate.cli import main as cli

    monkeypatch.setattr(
        "okg.substrate.profiling.list_profiles",
        lambda: [
            {"request_path": "target-uuid", "_path": "/tmp/target.json"},
            {"request_path": "older-uuid", "_path": "/tmp/older.json"},
        ],
    )
    out = cli._find_profile_for_workflow(workflow_id="target-uuid")
    assert out == "/tmp/target.json"


def test_queue_wait_derivation_in_payload(monkeypatch, capsys):
    """End-to-end: stub the workflow report + phase events, run the
    handler, assert work_ms / lifetime_ms / queue_wait_ms are present
    and correct in --json output."""
    from okg.substrate.cli import main as cli

    # Fake workflow row: 10s lifetime.
    started = datetime(2026, 5, 11, 18, 30, 0, tzinfo=timezone.utc)
    completed = datetime(2026, 5, 11, 18, 30, 10, tzinfo=timezone.utc)

    class _Report:
        workflow_id = "wf-test-123"
        name = "okg_publisher_cycle"
        status = "SUCCESS"
        started_at = started.isoformat()
        completed_at = completed.isoformat()
        recovery_attempts = 0
        queue_name = "okg-test"
        steps: list = []
        error = None

    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_debug_workflow",
        lambda dsn, workflow_id: _Report(),
    )
    # 3 phase events summing to 1.5s; queue_wait should be 8.5s.
    monkeypatch.setattr(
        cli, "_read_publisher_phase_events",
        lambda dsn, workflow_id: [
            {"name": "reserve_complete",
             "payload": {"duration_ms": 100, "generation_id": 1}},
            {"name": "phase_a_complete",
             "payload": {"duration_ms": 500, "converged": True}},
            {"name": "phase_b_complete",
             "payload": {"duration_ms": 900, "blocked": False}},
        ],
    )
    # No artifact link.
    monkeypatch.setattr(
        cli, "_find_profile_for_workflow", lambda workflow_id: None,
    )

    import argparse
    args = argparse.Namespace(
        rt_subcmd="debug-workflow",
        deployment="test",
        dsn="postgres://test",
        workflow_id="wf-test-123",
    )

    # The cli's _cmd_runtime resolves the manifest first; stub that
    # too to avoid touching the filesystem.
    class _Manifest:
        runtime = type("R", (), {"enabled": True})()
        postgres_dsn = "postgres://test"

    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda name: _Manifest(),
    )
    monkeypatch.setattr(
        cli, "_resolve_deployment_dsn",
        lambda args: "postgres://test",
    )

    rc = cli._cmd_runtime(args, as_json=True)
    assert rc == 0

    out = json.loads(capsys.readouterr().out)
    assert out["work_ms"] == 1500
    assert out["lifetime_ms"] == 10000
    assert out["queue_wait_ms"] == 8500
    assert out["profile_path"] is None
    # Phase events still present.
    assert len(out["phase_events"]) == 3


def test_client_debug_workflow_falls_back_to_db_for_started_at(
    monkeypatch,
):
    """When DBOSClient's workflow object has started_at=None, the
    helper should fall back to dbos.workflow_status.created_at so
    the CLI's queue-wait derivation has a value to work with."""
    from okg.substrate.runtime import admin

    class _FakeWf:
        workflow_uuid = "wf-xyz"
        name = "okg_publisher_cycle"
        status = "SUCCESS"
        started_at = None
        completed_at = None
        recovery_attempts = 0
        queue_name = "okg-test"

    class _FakeClient:
        def list_workflows(self, workflow_ids=None):
            return [_FakeWf()]

        def list_workflow_steps(self, workflow_id):
            return []

        def destroy(self):
            return None

    monkeypatch.setattr(
        admin, "_build_client", lambda dsn: _FakeClient(),
    )

    class _Cursor:
        def __init__(self, conn):
            self._conn = conn

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            self._conn._next = (
                1778524200000, 1778524210000, "SUCCESS",
            )

        def fetchone(self):
            return self._conn._next

    class _Conn:
        def __init__(self):
            self._next = None

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def cursor(self):
            return _Cursor(self)

    monkeypatch.setattr(
        "psycopg.connect", lambda *a, **kw: _Conn(),
    )

    out = admin.client_debug_workflow(
        dsn="postgres://test", workflow_id="wf-xyz",
    )
    assert out.started_at is not None
    assert "2026-05-11T18:30:00" in out.started_at
    assert out.completed_at is not None
    assert "2026-05-11T18:30:10" in out.completed_at


def test_client_debug_workflow_does_not_fill_completed_at_for_pending(
    monkeypatch,
):
    """A workflow with status='PENDING' should get started_at filled
    in from created_at, but completed_at must stay None — any value
    would be misleading because the workflow hasn't finished."""
    from okg.substrate.runtime import admin

    class _FakeWf:
        workflow_uuid = "wf-pending"
        name = "okg_publisher_cycle"
        status = "PENDING"
        started_at = None
        completed_at = None
        recovery_attempts = 0
        queue_name = "okg-test"

    class _FakeClient:
        def list_workflows(self, workflow_ids=None):
            return [_FakeWf()]

        def list_workflow_steps(self, workflow_id):
            return []

        def destroy(self):
            return None

    monkeypatch.setattr(
        admin, "_build_client", lambda dsn: _FakeClient(),
    )

    class _Cursor:
        def __init__(self, conn):
            self._conn = conn

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            self._conn._next = (
                1778524200000, 1778524210000, "PENDING",
            )

        def fetchone(self):
            return self._conn._next

    class _Conn:
        def __init__(self):
            self._next = None

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def cursor(self):
            return _Cursor(self)

    monkeypatch.setattr(
        "psycopg.connect", lambda *a, **kw: _Conn(),
    )

    out = admin.client_debug_workflow(
        dsn="postgres://test", workflow_id="wf-pending",
    )
    assert out.started_at is not None
    # PENDING → completed_at must NOT be filled, even though
    # updated_at exists in the DB.
    assert out.completed_at is None


def test_client_debug_workflow_releases_dbos_client(monkeypatch):
    """A short-lived debug command must not leave DBOS resources alive."""
    from okg.substrate.runtime import admin

    destroyed: list[bool] = []

    class _FakeClient:
        def list_workflows(self, workflow_ids=None):
            return []

        def destroy(self):
            destroyed.append(True)

    monkeypatch.setattr(
        admin, "_build_client", lambda dsn: _FakeClient(),
    )

    report = admin.client_debug_workflow(
        dsn="postgres://test", workflow_id="missing",
    )

    assert report.status == "<not found>"
    assert destroyed == [True]


def test_queue_wait_in_flight_workflow_returns_null(monkeypatch, capsys):
    """A workflow still PENDING (no completed_at) should report
    lifetime_ms but queue_wait_ms=None."""
    from okg.substrate.cli import main as cli

    started = datetime(2026, 5, 11, 18, 30, 0, tzinfo=timezone.utc)

    class _Report:
        workflow_id = "wf-running-456"
        name = "okg_publisher_cycle"
        status = "PENDING"
        started_at = started.isoformat()
        completed_at = None
        recovery_attempts = 0
        queue_name = "okg-test"
        steps: list = []
        error = None

    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_debug_workflow",
        lambda dsn, workflow_id: _Report(),
    )
    monkeypatch.setattr(
        cli, "_read_publisher_phase_events",
        lambda dsn, workflow_id: [],
    )
    monkeypatch.setattr(
        cli, "_find_profile_for_workflow", lambda workflow_id: None,
    )

    import argparse
    args = argparse.Namespace(
        rt_subcmd="debug-workflow",
        deployment="test",
        dsn="postgres://test",
        workflow_id="wf-running-456",
    )

    class _Manifest:
        runtime = type("R", (), {"enabled": True})()
        postgres_dsn = "postgres://test"

    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda name: _Manifest(),
    )
    monkeypatch.setattr(
        cli, "_resolve_deployment_dsn",
        lambda args: "postgres://test",
    )

    rc = cli._cmd_runtime(args, as_json=True)
    assert rc == 0

    out = json.loads(capsys.readouterr().out)
    # lifetime_ms computed from started_at to now (will be >0).
    assert out["lifetime_ms"] is not None and out["lifetime_ms"] >= 0
    # queue_wait_ms left None because completed_at is missing.
    assert out["queue_wait_ms"] is None
