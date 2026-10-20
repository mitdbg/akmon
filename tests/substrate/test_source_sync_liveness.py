from __future__ import annotations

import base64
import json
import pickle
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest

from okg.substrate.source_sync import (
    SourceSyncCapabilities,
    SourceSyncPoll,
    SourceSyncWatch,
    SourceSyncWebhook,
)
from okg.substrate.db.migrate import _views_sql
from okg.substrate.source_sync_producers import (
    _source_sync_dbos_recoverable_terminal_error,
    record_source_sync_producer_health,
    source_sync_producer_health,
)
from okg.substrate.watch import Watcher, WatchSource


def _entry(sync: SourceSyncCapabilities) -> SimpleNamespace:
    return SimpleNamespace(sync=sync)


def _poll_sync() -> SourceSyncCapabilities:
    return SourceSyncCapabilities(
        triggers=("poll", "manual", "reconcile"),
        poll=SourceSyncPoll(probe="change_probe", interval_seconds=300),
    )


def _watch_sync() -> SourceSyncCapabilities:
    return SourceSyncCapabilities(
        triggers=("fs_event", "manual", "reconcile"),
        watch=SourceSyncWatch(roots=("/tmp",), include_globs=("**/*",)),
    )


def _webhook_sync() -> SourceSyncCapabilities:
    return SourceSyncCapabilities(
        triggers=("webhook", "manual", "reconcile"),
        webhook=SourceSyncWebhook(
            path="/hooks/sync-demo/github",
            secret_ref="SYNC_DEMO_SECRET",
        ),
    )


class _DbosQueueCursor:
    def __init__(
        self,
        *,
        pending_fresh: bool = True,
        pending_started: bool = False,
        waiting_priority: int | None = None,
    ) -> None:
        self._rows: list[tuple] = []
        self.pending_fresh = pending_fresh
        self.pending_started = pending_started
        self.waiting_priority = waiting_priority

    def execute(self, sql, params=None):
        sql_text = str(sql)
        if "to_regclass" in sql_text:
            self._rows = [("dbos.workflow_status",)]
        elif "information_schema.columns" in sql_text:
            self._rows = [
                ("created_at",),
                ("updated_at",),
                ("started_at_epoch_ms",),
                ("application_version",),
                ("priority",),
            ]
        elif "SELECT application_version" in sql_text:
            assert params == (
                "okg-sync-demo-source-sync",
                [
                    "DELAYED",
                    "ENQUEUED",
                    "ERROR",
                    "FAILED",
                    "MAX_RECOVERY_ATTEMPTS",
                    "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
                    "PENDING",
                    "SUCCESS",
                ],
            )
            self._rows = [("current-version",)]
        elif "upper(status) = 'SUCCESS'" in sql_text:
            assert params == (
                "okg-sync-demo-source-sync",
                "current-version",
            )
            self._rows = [(None,)]
        elif (
            "upper(status) = ANY" in sql_text
            and "application_version = %s" in sql_text
        ):
            assert params == (
                "okg-sync-demo-source-sync",
                [
                    "CANCELLED",
                    "ERROR",
                    "FAILED",
                    "MAX_RECOVERY_ATTEMPTS",
                    "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
                ],
                "current-version",
            )
            self._rows = []
        elif "latest_success" in sql_text:
            assert params == (
                "okg-sync-demo-source-sync",
                "okg-sync-demo-source-sync",
                [
                    "CANCELLED",
                    "ERROR",
                    "FAILED",
                    "MAX_RECOVERY_ATTEMPTS",
                    "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
                ],
            )
            self._rows = []
        elif "GROUP BY queue_name, status" in sql_text:
            assert params == ("okg-sync-demo-source-sync",)
            self._rows = [
                ("okg-sync-demo-source-sync", "CANCELLED", 4),
                ("okg-sync-demo-source-sync", "ENQUEUED", 2),
                ("okg-sync-demo-source-sync", "ERROR", 1),
                ("okg-sync-demo-source-sync", "PENDING", 1),
                ("okg-sync-demo-source-sync", "SUCCESS", 3),
            ]
        elif "oldest_raw" in sql_text and "newest_raw" in sql_text:
            assert params == (
                "okg-sync-demo-source-sync",
                ["DELAYED", "ENQUEUED", "PENDING"],
            )
            now = datetime.now(timezone.utc)
            self._rows = [(now - timedelta(seconds=700), now)]
        elif "observed_raw" in sql_text:
            assert params == (
                "okg-sync-demo-source-sync",
                ["DELAYED", "ENQUEUED", "PENDING"],
            )
            now = datetime.now(timezone.utc)
            pending_age = 30 if self.pending_fresh else 700
            pending_observed = now - timedelta(seconds=pending_age)
            pending_started = pending_observed if self.pending_started else None
            self._rows = [
                (
                    "ENQUEUED",
                    now - timedelta(seconds=700),
                    None,
                    self.waiting_priority,
                ),
                (
                    "ENQUEUED",
                    now - timedelta(seconds=650),
                    None,
                    self.waiting_priority,
                ),
                (
                    "PENDING",
                    pending_observed,
                    pending_started,
                    None,
                ),
            ]
        else:
            raise AssertionError(f"unexpected SQL: {sql_text}")

    def fetchone(self):
        return self._rows[0]

    def fetchall(self):
        return self._rows


def test_dbos_queue_status_separates_active_backlog_from_history() -> None:
    from okg.substrate.source_sync_producers import (
        _source_sync_dbos_queue_status,
    )

    status = _source_sync_dbos_queue_status(
        _DbosQueueCursor(pending_fresh=True),
        deployment="sync-demo",
    )

    assert status["rows"] == [
        {
            "queue_name": "okg-sync-demo-source-sync",
            "status": "ENQUEUED",
            "count": 2,
        },
        {
            "queue_name": "okg-sync-demo-source-sync",
            "status": "PENDING",
            "count": 1,
        },
    ]
    assert status["queue_name"] == "okg-sync-demo-source-sync"
    assert status["active_queue_names"] == ["okg-sync-demo-source-sync"]
    assert status["active_count"] == 3
    assert status["terminal_history_rows"] == [
        {
            "queue_name": "okg-sync-demo-source-sync",
            "status": "CANCELLED",
            "count": 4,
        },
        {
            "queue_name": "okg-sync-demo-source-sync",
            "status": "ERROR",
            "count": 1,
        },
        {
            "queue_name": "okg-sync-demo-source-sync",
            "status": "SUCCESS",
            "count": 3,
        },
    ]
    assert status["terminal_history_count"] == 8
    assert status["failed_terminal_application_version"] == "current-version"
    assert status["failed_terminal_rows"] == []
    assert status["failed_terminal_count"] == 0
    assert status["stale_after_seconds"] == 600
    assert status["oldest_active_age_seconds"] is not None
    assert status["oldest_active_age_seconds"] >= 600
    assert status["newest_active_age_seconds"] is not None
    assert status["pending_count"] == 1
    assert status["waiting_count"] == 2
    assert status["fresh_pending_count"] == 1
    assert status["running_pending_count"] == 0
    assert status["replaceable_waiting_count"] == 2
    assert status["oldest_pending_update_age_seconds"] is not None
    assert status["oldest_pending_update_age_seconds"] < 600
    assert status["oldest_waiting_age_seconds"] is not None
    assert status["oldest_waiting_age_seconds"] >= 600
    assert status["oldest_replaceable_waiting_age_seconds"] is not None
    assert status["oldest_replaceable_waiting_age_seconds"] >= 600
    assert status["stale_pending_work_present"] is False
    assert status["stale_waiting_work_present"] is True
    assert status["stale_replaceable_work_present"] is True


def test_recoverable_source_sync_terminal_errors_do_not_count_as_dlq() -> None:
    recoverable = RuntimeError(
        "source-sync workflow: publish failed. First error: "
        "publish commit status=fail: source-sync publish work is active"
    )
    encoded = base64.b64encode(pickle.dumps(recoverable)).decode("ascii")

    assert _source_sync_dbos_recoverable_terminal_error(encoded) is True
    assert _source_sync_dbos_recoverable_terminal_error(
        "ordinary source adapter failure"
    ) is False


def test_dbos_queue_status_flags_stale_work_without_fresh_pending() -> None:
    from okg.substrate.source_sync_producers import (
        _source_sync_dbos_queue_status,
    )

    status = _source_sync_dbos_queue_status(
        _DbosQueueCursor(pending_fresh=False),
        deployment="sync-demo",
    )

    assert status["fresh_pending_count"] == 0
    assert status["oldest_pending_update_age_seconds"] is not None
    assert status["oldest_pending_update_age_seconds"] >= 600
    assert status["stale_pending_work_present"] is True
    assert status["stale_waiting_work_present"] is True
    assert status["stale_replaceable_work_present"] is True


def test_dbos_queue_status_does_not_flag_priority_waiting_as_replaceable() -> None:
    from okg.substrate.source_sync_producers import (
        _source_sync_dbos_queue_status,
    )

    status = _source_sync_dbos_queue_status(
        _DbosQueueCursor(pending_fresh=True, waiting_priority=20),
        deployment="sync-demo",
    )

    assert status["waiting_count"] == 2
    assert status["replaceable_waiting_count"] == 0
    assert status["oldest_waiting_age_seconds"] is not None
    assert status["oldest_waiting_age_seconds"] >= 600
    assert status["oldest_replaceable_waiting_age_seconds"] is None
    assert status["stale_pending_work_present"] is False
    assert status["stale_waiting_work_present"] is False
    assert status["stale_replaceable_work_present"] is False


def test_dbos_queue_status_treats_started_pending_as_running() -> None:
    from okg.substrate.source_sync_producers import (
        _source_sync_dbos_queue_status,
    )

    status = _source_sync_dbos_queue_status(
        _DbosQueueCursor(
            pending_fresh=False,
            pending_started=True,
            waiting_priority=20,
        ),
        deployment="sync-demo",
    )

    assert status["pending_count"] == 1
    assert status["fresh_pending_count"] == 1
    assert status["running_pending_count"] == 1
    assert status["oldest_pending_update_age_seconds"] is not None
    assert status["oldest_pending_update_age_seconds"] >= 600
    assert status["oldest_started_pending_update_age_seconds"] is not None
    assert status["oldest_started_pending_update_age_seconds"] >= 600
    assert status["stale_started_pending_work_present"] is True
    assert status["stale_pending_work_present"] is False
    assert status["stale_waiting_work_present"] is False
    assert status["stale_replaceable_work_present"] is False


def test_canonical_views_expose_only_live_runtime_queue_health() -> None:
    views = _views_sql()

    assert "okg.v_job_queue" not in views
    assert "CREATE OR REPLACE VIEW okg.v_runtime_queue_health" in views
    assert "CREATE OR REPLACE VIEW okg.v_compute_host_metrics" in views


@pytest.mark.postgres
def test_runtime_queue_health_distinguishes_idle_and_unavailable(
    clean_runtime: str,
) -> None:
    conn = psycopg.connect(clean_runtime)
    try:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
            cur.execute(
                """
                SELECT status, availability, reason, active_count, dlq_count
                  FROM okg.v_runtime_queue_health
                """
            )
            assert cur.fetchall() == [(
                "unavailable",
                "unavailable",
                "dbos.workflow_status missing",
                0,
                0,
            )]

            cur.execute("CREATE SCHEMA dbos")
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                  workflow_uuid text PRIMARY KEY,
                  queue_name text,
                  status text NOT NULL,
                  created_at timestamptz,
                  updated_at timestamptz,
                  started_at_epoch_ms bigint,
                  priority int
                )
                """
            )
            cur.execute(
                """
                SELECT queue_name, status, availability, active_count, dlq_count
                  FROM okg.v_runtime_queue_health
                """
            )
            assert cur.fetchall() == [(None, "idle", "available", 0, 0)]

            cur.execute(
                """
                INSERT INTO dbos.workflow_status (
                  workflow_uuid, queue_name, status, created_at, updated_at,
                  started_at_epoch_ms, priority
                )
                VALUES
                  (
                    'wf-running', 'q-running', 'PENDING',
                    now(), now(),
                    (extract(epoch FROM now()) * 1000)::bigint, 20
                  ),
                  (
                    'wf-stale', 'q-stale', 'ENQUEUED',
                    now() - interval '20 minutes',
                    now() - interval '20 minutes',
                    NULL, 100
                  ),
                  (
                    'wf-failed', 'q-failed', 'ERROR',
                    now(), now(), NULL, 100
                  ),
                  (
                    'wf-cancelled', 'q-cancelled', 'CANCELLED',
                    now(), now(), NULL, 100
                  )
                """
            )
            cur.execute(
                """
                SELECT
                  queue_name,
                  status,
                  active_count,
                  running_count,
                  failed_count,
                  cancelled_count,
                  dlq_count,
                  stale_active_work_present
                FROM okg.v_runtime_queue_health
                ORDER BY queue_name
                """
            )
            rows = {row[0]: row[1:] for row in cur.fetchall()}
            cur.execute("SET LOCAL ROLE app_ro")
            cur.execute(
                """
                SELECT status, active_count, running_count
                  FROM okg.v_runtime_queue_health
                 WHERE queue_name = 'q-running'
                """
            )
            app_ro_row = cur.fetchone()
            cur.execute("RESET ROLE")

        assert rows["q-running"] == ("running", 1, 1, 0, 0, 0, False)
        assert app_ro_row == ("running", 1, 1)
        assert rows["q-stale"] == ("stale", 1, 0, 0, 0, 0, True)
        assert rows["q-failed"] == ("failed", 0, 0, 1, 0, 1, False)
        assert rows["q-cancelled"] == (
            "cancelled",
            0,
            0,
            0,
            1,
            1,
            False,
        )
    finally:
        conn.rollback()
        conn.close()


@pytest.mark.postgres
def test_runtime_queue_health_unknown_active_age_is_unavailable(
    clean_runtime: str,
) -> None:
    conn = psycopg.connect(clean_runtime)
    try:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
            cur.execute("CREATE SCHEMA dbos")
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                  workflow_uuid text PRIMARY KEY,
                  queue_name text,
                  status text NOT NULL
                )
                """
            )
            cur.execute(
                """
                INSERT INTO dbos.workflow_status (
                  workflow_uuid, queue_name, status
                )
                VALUES ('wf-unknown-age', 'q-unknown-age', 'ENQUEUED')
                """
            )
            cur.execute(
                """
                SELECT status, active_count, unknown_active_age_count
                  FROM okg.v_runtime_queue_health
                 WHERE queue_name = 'q-unknown-age'
                """
            )
            assert cur.fetchone() == ("unavailable", 1, 1)
    finally:
        conn.rollback()
        conn.close()


@pytest.mark.postgres
def test_missing_producers_fail_dogfood_readiness(clean_runtime: str) -> None:
    registry = {
        "docs": _entry(_poll_sync()),
        "pact": _entry(_watch_sync()),
    }

    health = source_sync_producer_health(
        clean_runtime,
        deployment="sync-demo",
        registry=registry,
        now=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
    )

    assert health["ok"] is False
    assert health["producer_count"] == 0
    assert health["expected_producer_count"] == 2
    assert health["source_sync_status"]["schema"] == "okg.source_sync.status.v1"
    assert health["source_sync_status"]["runtime_liveness"]["services"][
        "watch"
    ]["status"] == "missing"
    assert {
        (row["source_name"], row["producer"])
        for row in health["missing_producers"]
    } == {("docs", "poll"), ("pact", "watch")}


@pytest.mark.postgres
def test_health_distinguishes_healthy_and_stale_producers(
    clean_runtime: str,
) -> None:
    registry = {"docs": _entry(_poll_sync())}
    now = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
    record_source_sync_producer_health(
        clean_runtime,
        deployment="sync-demo",
        source_name="docs",
        producer="poll",
        enqueue_count=0,
        error_count=0,
    )

    health = source_sync_producer_health(
        clean_runtime,
        deployment="sync-demo",
        registry=registry,
        now=datetime.now(timezone.utc),
    )

    # health["ok"] is now the aggregate source-sync-status contract, which
    # fails closed (source_sync_dlq_status_unknown) without a live DBOS queue
    # status. These tests exercise the producer-liveness slice, so assert
    # health["producer_liveness_ok"] — the missing/stale producer signal.
    assert health["producer_liveness_ok"] is True
    assert health["missing_producers"] == []
    assert health["stale_producers"] == []

    old = now - timedelta(hours=2)
    value = json.dumps({
        "last_run_at": old.isoformat(),
        "last_heartbeat_at": old.isoformat(),
        "enqueue_count": 0,
        "error_count": 0,
    })
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (deployment, source_name, key)
            DO UPDATE SET value = EXCLUDED.value,
                          updated_at = EXCLUDED.updated_at
            """,
            ("sync-demo", "docs", "sync.producer.poll", value, old),
        )

    stale = source_sync_producer_health(
        clean_runtime,
        deployment="sync-demo",
        registry=registry,
        max_producer_age_seconds=900,
        now=now,
    )

    assert stale["producer_liveness_ok"] is False
    assert stale["missing_producers"] == []
    assert stale["stale_producers"][0]["source_name"] == "docs"
    assert stale["stale_producers"][0]["producer"] == "poll"


@pytest.mark.postgres
def test_required_producers_are_running_or_explicitly_optional(
    clean_runtime: str,
) -> None:
    registry = {
        "docs": _entry(_poll_sync()),
        "pact": _entry(_watch_sync()),
        "github": _entry(_webhook_sync()),
    }
    now = datetime.now(timezone.utc)
    record_source_sync_producer_health(
        clean_runtime,
        deployment="sync-demo",
        source_name="docs",
        producer="poll",
        enqueue_count=0,
        error_count=0,
    )
    watcher = Watcher(
        clean_runtime,
        "sync-demo",
        roots=[Path("/tmp")],
        source_scopes=(
            WatchSource(
                source_name="pact",
                roots=(Path("/tmp"),),
            ),
        ),
    )
    watcher.record_heartbeat()

    health = source_sync_producer_health(
        clean_runtime,
        deployment="sync-demo",
        registry=registry,
        runtime_source_sync={
            "watch": {"enabled": True},
            "poll": {"enabled": True},
            "webhook": {"enabled": False},
        },
        now=now,
    )

    # health["ok"] / source_sync_status["ok"] are the aggregate contract,
    # which fails closed (source_sync_dlq_status_unknown) without a live
    # DBOS queue status in this bare fixture. This test exercises the
    # producer-liveness slice (missing/stale/disabled producers), so assert
    # health["producer_liveness_ok"] for that signal; the DLQ backlog count
    # the producers contribute is still zero.
    assert health["producer_liveness_ok"] is True
    assert health["source_sync_status"]["backlog"]["dlq_count"] == 0
    assert health["expected_producer_count"] == 2
    assert health["disabled_producer_count"] == 1
    assert health["missing_producers"] == []
    assert health["stale_producers"] == []
    assert health["disabled_producers"] == [
        {
            "source_name": "github",
            "producer": "webhook",
            "trigger_kinds": ["webhook"],
            "path": "/hooks/sync-demo/github",
            "max_age_seconds": 900,
            "status": "disabled",
            "owner": "deployment.runtime.source_sync",
            "reason": "runtime.source_sync.webhook.enabled=false",
            "recovery_command": (
                "enable runtime.source_sync.webhook in deployment.yaml "
                "or remove the corresponding trigger from source_registry.yaml"
            ),
        }
    ]
