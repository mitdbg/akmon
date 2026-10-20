from __future__ import annotations

import psycopg
import pytest

from okg.substrate.runtime.partition_queue import source_sync_queue_name
from okg.substrate.source_sync import source_sync_desired_frontier_state
from okg.substrate.source_sync_producers import _source_sync_dbos_queue_status
from okg.substrate.source_sync_status import (
    SOURCE_SYNC_STATUS_SCHEMA,
    build_source_sync_status_contract,
    source_sync_status_for_source,
)


def test_status_model_separates_wakeups_liveness_freshness_and_backlog() -> None:
    health = {
        "deployment": "demo",
        "producer_count": 2,
        "expected_producer_count": 3,
        "disabled_producer_count": 1,
        "producers": [
            {
                "source_name": "docs",
                "producer": "poll",
                "age_seconds": 20,
            },
            {
                "source_name": "github",
                "producer": "webhook",
                "age_seconds": 5_000,
            },
        ],
        "expected_producers": [
            {
                "source_name": "docs",
                "producer": "poll",
                "trigger_kinds": ["poll"],
                "max_age_seconds": 600,
                "status": "healthy",
                "observed": {"age_seconds": 20},
            },
            {
                "source_name": "pact",
                "producer": "watch",
                "trigger_kinds": ["fs_event"],
                "max_age_seconds": 900,
                "status": "missing",
            },
            {
                "source_name": "github",
                "producer": "webhook",
                "trigger_kinds": ["webhook"],
                "max_age_seconds": 900,
                "status": "stale",
                "observed": {"age_seconds": 5_000},
            },
        ],
        "disabled_producers": [
            {
                "source_name": "legacy",
                "producer": "webhook",
                "status": "disabled",
            }
        ],
        "missing_producers": [
            {"source_name": "pact", "producer": "watch"}
        ],
        "stale_producers": [
            {"source_name": "github", "producer": "webhook"}
        ],
        "config_warnings": [
            {
                "source_name": "notes",
                "code": "poll_trigger_without_schedule",
            }
        ],
        "queue_depth": [],
        "dbos_queue_depth": {
            "available": True,
            "active_count": 3,
            "terminal_history_count": 9,
            "rows": [{"status": "PENDING", "count": 3}],
            "terminal_history_rows": [
                {"status": "SUCCESS", "count": 8},
                {"status": "ERROR", "count": 1},
            ],
            "failed_terminal_count": 1,
            "failed_terminal_rows": [{"status": "ERROR", "count": 1}],
        },
        "recovery_commands": {
            "start_supervised_worker": (
                "uv run okg runtime worker --deployment demo"
            )
        },
    }
    freshness = {
        "ok": False,
        "max_source_age_seconds": 3600,
        "missing": ["demo.pact"],
        "stale": [],
        "groups": {
            "docs": {
                "ok": True,
                "sources": [
                    {
                        "source": "demo.docs",
                        "status": "fresh",
                        "age_seconds": 30,
                    }
                ],
            }
        },
    }

    status = build_source_sync_status_contract(
        health,
        source_freshness=freshness,
    )

    assert status["schema"] == SOURCE_SYNC_STATUS_SCHEMA
    assert status["status"] == "blocked"
    assert set(status) >= {
        "wakeups",
        "runtime_liveness",
        "source_freshness",
        "backlog",
    }
    assert "legacy_jobs" not in status["wakeups"]
    assert status["wakeups"]["dbos_workflows"]["active_count"] == 3
    assert status["backlog"]["active_count"] == 3
    assert status["backlog"]["terminal_history_count"] == 9
    assert status["backlog"]["dlq_count"] == 1
    assert status["backlog"]["failed_terminal_rows"] == [
        {"status": "ERROR", "count": 1}
    ]
    assert status["runtime_liveness"]["worker"]["status"] == (
        "not_directly_observed"
    )
    assert status["runtime_liveness"]["poll_probes"]["status"] == "healthy"
    assert status["runtime_liveness"]["services"]["watch"]["status"] == "missing"
    assert status["runtime_liveness"]["services"]["webhook"]["status"] == "stale"
    assert status["source_freshness"]["status"] == "blocked"
    assert status["recovery_commands"]["start_supervised_worker"] == (
        "uv run okg runtime worker --deployment demo"
    )
    assert {blocker["code"] for blocker in status["blockers"]} == {
        "source_sync_config_warning",
        "source_sync_producer_missing",
        "source_sync_producer_stale",
        "source_sync_dlq_nonempty",
        "source_freshness_not_ready",
        "source_sync_convergence_not_ready",
    }


def test_status_model_does_not_block_on_optional_stale_producer() -> None:
    status = build_source_sync_status_contract(
        {
            "deployment": "demo",
            "producers": [],
            "expected_producers": [
                {
                    "source_name": "docs",
                    "producer": "poll",
                    "trigger_kinds": ["poll"],
                    "status": "healthy",
                    "max_age_seconds": 900,
                    "observed": {"age_seconds": 12},
                },
                {
                    "source_name": "optional-monitor",
                    "producer": "watch",
                    "trigger_kinds": ["fs_event"],
                    "status": "stale",
                    "max_age_seconds": 900,
                    "observed": {"age_seconds": 5000},
                },
            ],
            "disabled_producers": [],
            "missing_producers": [],
            "stale_producers": [
                {"source_name": "optional-monitor", "producer": "watch"}
            ],
            "config_warnings": [],
            "dbos_queue_depth": {"available": True, "active_count": 0},
            "dlq_count": 0,
        },
        source_inventory=[
            {
                "deployment": "demo",
                "source_name": "docs",
                "runtime": {"enabled": True, "source_sync_enabled": True},
                "sync": {
                    "declared": True,
                    "reconcile_run_mode": "scope_complete",
                },
                "freshness_policy": {
                    "readiness_required": True,
                    "completed_scope_required_for_readiness": True,
                },
                "constraints": {},
            },
            {
                "deployment": "demo",
                "source_name": "optional-monitor",
                "runtime": {"enabled": True, "source_sync_enabled": True},
                "sync": {
                    "declared": True,
                    "reconcile_run_mode": "scope_complete",
                },
                "freshness_policy": {"readiness_required": False},
                "constraints": {},
            },
        ],
        source_freshness={
            "ok": True,
            "missing": [],
            "stale": [],
            "groups": {
                "docs": {
                    "ok": True,
                    "sources": [{
                        "source": "demo.docs",
                        "status": "fresh",
                        "age_seconds": 12,
                    }],
                },
            },
        },
        generation={"status": "published", "generation_id": "g1"},
        mcp={"ok": True, "pinned_generation_id": "g1"},
    )

    assert status["ok"] is True
    assert status["status"] == "healthy"
    assert status["blockers"] == []
    impact = status["producer_liveness_impact"]
    assert impact["blocking_stale_producers"] == []
    assert impact["advisory_stale_producers"] == [
        {"source_name": "optional-monitor", "producer": "watch"}
    ]
    assert status["runtime_liveness"]["status"] == "blocked"


def test_status_model_publishes_explicit_runtime_health_policy() -> None:
    status = build_source_sync_status_contract(
        {
            "deployment": "demo",
            "producers": [],
            "expected_producers": [],
            "disabled_producers": [],
            "missing_producers": [],
            "stale_producers": [],
            "config_warnings": [],
            "dbos_queue_depth": {
                "available": True,
                "active_count": 0,
                "terminal_history_count": 42,
                "failed_terminal_count": 0,
                "terminal_history_rows": [
                    {"status": "SUCCESS", "count": 40},
                    {"status": "CANCELLED", "count": 2},
                ],
            },
        },
        source_inventory=[],
        source_freshness={"ok": True, "missing": [], "stale": [], "groups": {}},
        generation={"status": "published", "generation_id": "g1"},
        mcp={"ok": True, "pinned_generation_id": "g1"},
    )

    assert status["ok"] is True
    assert status["backlog"]["terminal_history_count"] == 42
    policy = status["policy"]
    assert policy["schema"] == "okg.runtime_health_policy.v1"
    backlog_policy = policy["source_sync_backlog"]
    assert backlog_policy["terminal_history"]["readiness"] == "nonblocking"
    assert backlog_policy["terminal_history"]["safe_gc"] is False
    assert backlog_policy["failed_terminal_history"]["readiness"] == "blocker"
    assert backlog_policy["stale_replaceable_work"] == {
        "readiness": "blocker",
        "operator_action": "dry_run_then_confirm_apply",
        "safe_gc": True,
        "dry_run_command_key": "source_sync_dbos_gc_dry_run",
        "apply_command_key": "source_sync_dbos_gc_apply",
        "description": (
            "Old replaceable source-sync wakeups block readiness and may be "
            "cancelled by DBOS GC after dry-run review."
        ),
    }
    assert status["wakeups"]["dbos_workflows"]["terminal_history_rows"] == [
        {"status": "SUCCESS", "count": 40},
        {"status": "CANCELLED", "count": 2},
    ]


def test_status_model_blocks_when_dbos_dlq_status_is_unknown() -> None:
    status = build_source_sync_status_contract(
        {
            "deployment": "demo",
            "producers": [],
            "expected_producers": [],
            "disabled_producers": [],
            "missing_producers": [],
            "stale_producers": [],
            "config_warnings": [],
            "queue_depth": [],
            "dbos_queue_depth": {
                "available": False,
                "reason": "dbos.workflow_status missing",
            },
        },
        source_freshness={"ok": True, "missing": [], "stale": [], "groups": {}},
    )

    assert status["ok"] is False
    assert status["wakeups"]["status"] == "blocked"
    assert status["backlog"]["dlq_status"] == "unknown"
    assert {blocker["code"] for blocker in status["blockers"]} == {
        "source_sync_dlq_status_unknown"
    }


@pytest.mark.postgres
def test_dbos_queue_status_counts_exhausted_recovery_as_dlq(
    clean_runtime: str,
) -> None:
    queue_name = source_sync_queue_name("demo")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
            cur.execute("CREATE SCHEMA dbos")
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                  workflow_uuid text PRIMARY KEY,
                  queue_name text NOT NULL,
                  name text NOT NULL,
                  status text NOT NULL,
                  created_at timestamptz NOT NULL DEFAULT now(),
                  updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                INSERT INTO dbos.workflow_status
                  (workflow_uuid, queue_name, name, status)
                VALUES
                  ('wf-exhausted', %s, 'okg_source_sync',
                   'MAX_RECOVERY_ATTEMPTS_EXCEEDED')
                """,
                (queue_name,),
            )
            status = _source_sync_dbos_queue_status(cur, deployment="demo")

    assert status["failed_terminal_count"] == 1
    assert status["failed_terminal_rows"] == [{
        "queue_name": queue_name,
        "status": "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
        "count": 1,
    }]


@pytest.mark.postgres
def test_dbos_queue_status_ignores_stale_failed_history_after_later_success(
    clean_runtime: str,
) -> None:
    queue_name = source_sync_queue_name("demo")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
            cur.execute("CREATE SCHEMA dbos")
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                  workflow_uuid text PRIMARY KEY,
                  queue_name text NOT NULL,
                  name text NOT NULL,
                  status text NOT NULL,
                  application_version text,
                  error text,
                  created_at timestamptz NOT NULL DEFAULT now(),
                  updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                INSERT INTO dbos.workflow_status
                  (workflow_uuid, queue_name, name, status, application_version,
                   error, updated_at)
                VALUES
                  ('wf-old-error', %s, 'okg_source_sync',
                   'error', 'old-version', 'old failure',
                   now() - interval '10 minutes'),
                  ('wf-current-success', %s, 'okg_source_sync',
                   'success', 'current-version', NULL,
                   now() - interval '1 minute')
                """,
                (queue_name, queue_name),
            )
            status = _source_sync_dbos_queue_status(cur, deployment="demo")

    assert status["failed_terminal_application_version"] == "current-version"
    assert status["terminal_history_rows"] == [
        {"queue_name": queue_name, "status": "error", "count": 1},
        {"queue_name": queue_name, "status": "success", "count": 1},
    ]
    assert status["failed_terminal_count"] == 0
    assert status["failed_terminal_rows"] == []


@pytest.mark.postgres
def test_dbos_queue_status_ignores_failed_history_after_later_success_without_application_version(
    clean_runtime: str,
) -> None:
    queue_name = source_sync_queue_name("demo")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
            cur.execute("CREATE SCHEMA dbos")
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                  workflow_uuid text PRIMARY KEY,
                  queue_name text NOT NULL,
                  name text NOT NULL,
                  status text NOT NULL,
                  created_at timestamptz NOT NULL DEFAULT now(),
                  updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                INSERT INTO dbos.workflow_status
                  (workflow_uuid, queue_name, name, status, updated_at)
                VALUES
                  ('wf-error', %s, 'okg_source_sync',
                   'ERROR', now() - interval '10 minutes'),
                  ('wf-success', %s, 'okg_source_sync',
                   'SUCCESS', now() - interval '1 minute')
                """,
                (queue_name, queue_name),
            )
            status = _source_sync_dbos_queue_status(cur, deployment="demo")

    assert status["terminal_history_rows"] == [
        {"queue_name": queue_name, "status": "ERROR", "count": 1},
        {"queue_name": queue_name, "status": "SUCCESS", "count": 1},
    ]
    assert status["failed_terminal_count"] == 0
    assert status["failed_terminal_rows"] == []


@pytest.mark.postgres
def test_dbos_queue_status_counts_current_failed_version_without_success(
    clean_runtime: str,
) -> None:
    queue_name = source_sync_queue_name("demo")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
            cur.execute("CREATE SCHEMA dbos")
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                  workflow_uuid text PRIMARY KEY,
                  queue_name text NOT NULL,
                  name text NOT NULL,
                  status text NOT NULL,
                  application_version text,
                  error text,
                  created_at timestamptz NOT NULL DEFAULT now(),
                  updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                INSERT INTO dbos.workflow_status
                  (workflow_uuid, queue_name, name, status, application_version,
                   error, updated_at)
                VALUES
                  ('wf-old-success', %s, 'okg_source_sync',
                   'SUCCESS', 'old-version', NULL,
                   now() - interval '10 minutes'),
                  ('wf-current-error', %s, 'okg_source_sync',
                   'ERROR', 'current-version', 'adapter failed',
                   now() - interval '1 minute')
                """,
                (queue_name, queue_name),
            )
            status = _source_sync_dbos_queue_status(cur, deployment="demo")

    assert status["failed_terminal_application_version"] == "current-version"
    assert status["failed_terminal_count"] == 1
    assert status["failed_terminal_rows"] == [{
        "queue_name": queue_name,
        "status": "ERROR",
        "count": 1,
    }]


@pytest.mark.postgres
def test_dbos_queue_status_ignores_current_failure_superseded_by_success(
    clean_runtime: str,
) -> None:
    queue_name = source_sync_queue_name("demo")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
            cur.execute("CREATE SCHEMA dbos")
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                  workflow_uuid text PRIMARY KEY,
                  queue_name text NOT NULL,
                  name text NOT NULL,
                  status text NOT NULL,
                  application_version text,
                  error text,
                  created_at timestamptz NOT NULL DEFAULT now(),
                  updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                INSERT INTO dbos.workflow_status
                  (workflow_uuid, queue_name, name, status, application_version,
                   error, updated_at)
                VALUES
                  ('wf-current-error', %s, 'okg_source_sync',
                   'ERROR', 'current-version', 'adapter failed',
                   now() - interval '10 minutes'),
                  ('wf-current-success', %s, 'okg_source_sync',
                   'SUCCESS', 'current-version', NULL,
                   now() - interval '1 minute')
                """,
                (queue_name, queue_name),
            )
            status = _source_sync_dbos_queue_status(cur, deployment="demo")

    assert status["failed_terminal_application_version"] == "current-version"
    assert status["failed_terminal_count"] == 0
    assert status["failed_terminal_rows"] == []


@pytest.mark.postgres
def test_dbos_queue_status_current_failure_beats_newer_old_cancelled(
    clean_runtime: str,
) -> None:
    queue_name = source_sync_queue_name("demo")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
            cur.execute("CREATE SCHEMA dbos")
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                  workflow_uuid text PRIMARY KEY,
                  queue_name text NOT NULL,
                  name text NOT NULL,
                  status text NOT NULL,
                  application_version text,
                  error text,
                  created_at timestamptz NOT NULL DEFAULT now(),
                  updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                INSERT INTO dbos.workflow_status
                  (workflow_uuid, queue_name, name, status, application_version,
                   error, updated_at)
                VALUES
                  ('wf-current-error', %s, 'okg_source_sync',
                   'ERROR', 'current-version', 'adapter failed',
                   now() - interval '2 minutes'),
                  ('wf-old-cancelled', %s, 'okg_source_sync',
                   'CANCELLED', 'old-version', NULL,
                   now() - interval '1 minute')
                """,
                (queue_name, queue_name),
            )
            status = _source_sync_dbos_queue_status(cur, deployment="demo")

    assert status["failed_terminal_application_version"] == "current-version"
    assert status["failed_terminal_count"] == 1
    assert status["failed_terminal_rows"] == [{
        "queue_name": queue_name,
        "status": "ERROR",
        "count": 1,
    }]


@pytest.mark.postgres
def test_dbos_queue_status_cancelled_rows_need_error_to_count_as_dlq(
    clean_runtime: str,
) -> None:
    queue_name = source_sync_queue_name("demo")
    recoverable_error = (
        "DBOSWorkflowCancelledError: stale replaceable source-sync wakeup"
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
            cur.execute("CREATE SCHEMA dbos")
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                  workflow_uuid text PRIMARY KEY,
                  queue_name text NOT NULL,
                  name text NOT NULL,
                  status text NOT NULL,
                  application_version text,
                  error text,
                  created_at timestamptz NOT NULL DEFAULT now(),
                  updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                INSERT INTO dbos.workflow_status
                  (workflow_uuid, queue_name, name, status, application_version,
                   error, updated_at)
                VALUES
                  ('wf-cancelled-no-error', %s, 'okg_source_sync',
                   'CANCELLED', 'current-version', NULL,
                   now() - interval '3 minutes'),
                  ('wf-cancelled-recoverable', %s, 'okg_source_sync',
                   'CANCELLED', 'current-version', %s,
                   now() - interval '2 minutes'),
                  ('wf-cancelled-with-error', %s, 'okg_source_sync',
                   'CANCELLED', 'current-version', 'ordinary cancellation error',
                   now() - interval '1 minute')
                """,
                (
                    queue_name,
                    queue_name,
                    recoverable_error,
                    queue_name,
                ),
            )
            status = _source_sync_dbos_queue_status(cur, deployment="demo")

    assert status["failed_terminal_application_version"] == "current-version"
    assert status["failed_terminal_count"] == 1
    assert status["failed_terminal_rows"] == [{
        "queue_name": queue_name,
        "status": "CANCELLED",
        "count": 1,
    }]


def test_desired_frontier_state_never_completes_dbos_wakeups() -> None:
    pending = source_sync_desired_frontier_state(
        source_name="pact",
        run_mode="scope_complete",
        workflow_id="wf-pending",
        workflow_status="PENDING",
        started_age_seconds=15,
    )
    waiting = source_sync_desired_frontier_state(
        source_name="pact",
        run_mode="scope_complete",
        workflow_id="wf-waiting",
        workflow_status="ENQUEUED",
    )

    assert pending["state"] == "running"
    assert waiting["state"] == "waiting"
    for frontier in (pending, waiting):
        assert frontier["completed_scope_run_mode"] is True
        assert frontier["completed"] is False
        assert frontier["completion_state"] == "not_completed"
        assert frontier["completion_basis"] == (
            "dbos_wakeup_is_not_completion_evidence"
        )
        assert "published_generation_visible_to_read_side" in (
            frontier["completion_requires"]
        )


def test_status_contract_exposes_active_frontiers_without_completion() -> None:
    status = build_source_sync_status_contract(
        {
            "deployment": "demo",
            "producers": [],
            "expected_producers": [],
            "disabled_producers": [],
            "missing_producers": [],
            "stale_producers": [],
            "config_warnings": [],
            "queue_depth": [],
            "dbos_queue_depth": {"active_count": 1},
            "dlq_count": 0,
            "active_source_sync_workflows": {
                "pact": {
                    "workflow_id": "wf-pending",
                    "status": "PENDING",
                    "run_mode": "scope_complete",
                    "stale": False,
                    "started_age_seconds": 20,
                }
            },
        },
        source_freshness={
            "ok": False,
            "missing": [],
            "stale": ["demo.pact"],
            "groups": {},
        },
    )

    frontier = status["desired_frontiers"]["sources"]["pact"]
    assert status["desired_frontiers"]["status"] == "advancing"
    assert status["desired_frontiers"]["completed_from_active_work_count"] == 0
    assert frontier["state"] == "running"
    assert frontier["completed"] is False
    assert frontier["completion_state"] == "not_completed"
    source_status = source_sync_status_for_source(status, "pact")
    assert source_status["desired_frontier"]["workflow_id"] == "wf-pending"
    assert source_status["desired_frontier"]["completed"] is False


def test_ci_source_progress_freshness_satisfies_convergence() -> None:
    inventory = [
        {
            "deployment": "okg-ci",
            "source_name": "test-reports",
            "runtime": {"enabled": True, "source_sync_enabled": True},
            "sync": {
                "declared": True,
                "triggers": ["poll", "manual", "reconcile"],
                "default_event_mode": "scope_incomplete",
                "reconcile_mode": "scope_incomplete",
                "reconcile_run_mode": "scope_incomplete",
            },
            "freshness_policy": {
                "policy": "source_progress",
                "readiness_role": "agent_context_required",
                "readiness_profiles": ["ci"],
                "readiness_required": True,
                "completed_scope_required_for_readiness": False,
                "progress_freshness_required_for_readiness": True,
                "completed_scope_capable": False,
            },
            "constraints": {},
        }
    ]
    status = build_source_sync_status_contract(
        {
            "deployment": "okg-ci",
            "producers": [],
            "expected_producers": [],
            "disabled_producers": [],
            "missing_producers": [],
            "stale_producers": [],
            "config_warnings": [],
            "dbos_queue_depth": {"active_count": 0},
            "dlq_count": 0,
        },
        source_inventory=inventory,
        source_freshness={
            "ok": True,
            "missing": [],
            "stale": [],
            "groups": {
                "test_artifacts": {
                    "ok": True,
                    "sources": [
                        {
                            "source": "okg-ci.test-reports",
                            "status": "fresh",
                            "freshness_kind": "source_progress",
                            "last_reconcile_at": "2026-06-16T21:46:03Z",
                            "watermark_age_seconds": 145000,
                            "last_progress_at": "2026-06-18T14:05:11Z",
                            "age_seconds": 120,
                        }
                    ],
                }
            },
        },
        generation={"status": "published", "generation_id": "g-ci"},
        mcp={"ok": True, "pinned_generation_id": "g-ci"},
    )

    convergence = status["convergence"]
    assert convergence["ok"] is True
    row = convergence["sources"][0]
    assert row["source_name"] == "test-reports"
    assert row["state"] == "fresh"
    assert row["reason"] == "source_progress_freshness_fresh"
    assert row["freshness"]["freshness_kind"] == "source_progress"

    stale = build_source_sync_status_contract(
        {
            "deployment": "okg-ci",
            "producers": [],
            "expected_producers": [],
            "disabled_producers": [],
            "missing_producers": [],
            "stale_producers": [],
            "config_warnings": [],
            "dbos_queue_depth": {"active_count": 0},
            "dlq_count": 0,
        },
        source_inventory=inventory,
        source_freshness={
            "ok": False,
            "missing": [],
            "stale": ["okg-ci.test-reports"],
            "groups": {
                "test_artifacts": {
                    "ok": False,
                    "sources": [
                        {
                            "source": "okg-ci.test-reports",
                            "status": "stale",
                            "freshness_kind": "source_progress",
                            "age_seconds": 145000,
                        }
                    ],
                }
            },
        },
    )

    stale_row = stale["convergence"]["sources"][0]
    assert stale_row["state"] == "blocked"
    assert stale_row["reason"] == "source_progress_freshness_stale"
    assert stale_row["recovery_action"]["action_kind"] == (
        "source_progress_reconcile"
    )
    assert stale_row["recovery_action"]["run_mode"] == "scope_incomplete"


def test_status_model_marks_stale_replaceable_backlog_not_healthy() -> None:
    status = build_source_sync_status_contract(
        {
            "deployment": "demo",
            "producers": [],
            "expected_producers": [],
            "disabled_producers": [],
            "missing_producers": [],
            "stale_producers": [],
            "config_warnings": [],
            "dbos_queue_depth": {
                "available": True,
                "active_count": 2,
                "active_rows": [{"status": "ENQUEUED", "count": 2}],
                "terminal_history_count": 5,
                "pending_count": 0,
                "waiting_count": 2,
                "fresh_pending_count": 0,
                "oldest_active_age_seconds": 900,
                "newest_active_age_seconds": 650,
                "oldest_pending_update_age_seconds": None,
                "oldest_waiting_age_seconds": 900,
                "stale_after_seconds": 600,
                "stale_pending_work_present": False,
                "stale_waiting_work_present": True,
                "stale_replaceable_work_present": True,
            },
            "dlq_count": 0,
        },
        source_freshness={
            "ok": True,
            "missing": [],
            "stale": [],
            "groups": {},
        },
    )

    assert status["ok"] is False
    assert status["status"] == "blocked"
    assert status["backlog"] == {
        "ok": False,
        "status": "stale_replaceable_work_present",
        "active_count": 2,
        "dbos_active_count": 2,
        "terminal_history_count": 5,
        "dlq_count": 0,
        "dlq_status": "known",
        "failed_terminal_rows": None,
        "stale_after_seconds": 600,
        "pending_count": 0,
        "waiting_count": 2,
        "fresh_pending_count": 0,
        "running_pending_count": None,
        "replaceable_waiting_count": None,
        "oldest_active_age_seconds": 900,
        "newest_active_age_seconds": 650,
        "oldest_pending_update_age_seconds": None,
        "oldest_waiting_age_seconds": 900,
        "oldest_replaceable_waiting_age_seconds": None,
        "stale_pending_work_present": False,
        "stale_waiting_work_present": True,
        "stale_replaceable_work_present": True,
    }
    assert {blocker["code"] for blocker in status["blockers"]} == {
        "source_sync_backlog_stale_replaceable"
    }


def test_source_sync_status_slice_uses_same_schema_for_mcp_source_status() -> None:
    status = build_source_sync_status_contract(
        {
            "deployment": "demo",
            "producers": [],
            "expected_producers": [
                {
                    "source_name": "pact",
                    "producer": "watch",
                    "status": "missing",
                }
            ],
            "disabled_producers": [],
            "missing_producers": [
                {"source_name": "pact", "producer": "watch"}
            ],
            "stale_producers": [],
            "config_warnings": [],
            "queue_depth": [],
            "dbos_queue_depth": {},
            "dlq_count": 0,
            "recovery_commands": {
                "check_dogfood_ready": (
                    "uv run okg deployment ready demo --checks dogfood --json"
                )
            },
        },
        source_freshness={
            "ok": False,
            "missing": ["demo.pact"],
            "stale": [],
            "groups": {},
        },
    )

    source_status = source_sync_status_for_source(status, "pact")

    assert source_status["schema"] == SOURCE_SYNC_STATUS_SCHEMA
    assert source_status["status"] == "missing"
    assert source_status["runtime_liveness"]["status"] == "missing"
    assert source_status["source_freshness"]["status"] == "missing"
    assert source_status["convergence"]["source_name"] == "pact"
    assert source_status["convergence"]["state"] == "unsupported"
    assert source_status["convergence"]["blocking_stage"] == (
        "runtime_orchestration"
    )
    assert source_status["recovery_commands"]["check_dogfood_ready"] == (
        "uv run okg deployment ready demo --checks dogfood --json"
    )


def test_runtime_liveness_is_service_scoped() -> None:
    status = build_source_sync_status_contract({
        "deployment": "demo",
        "producers": [],
        "expected_producers": [
            {
                "source_name": "pact",
                "producer": "watch",
                "status": "healthy",
            },
            {
                "source_name": "docs",
                "producer": "poll",
                "status": "missing",
            },
        ],
        "disabled_producers": [],
        "missing_producers": [
            {"source_name": "docs", "producer": "poll"}
        ],
        "stale_producers": [],
        "config_warnings": [],
        "queue_depth": [],
        "dbos_queue_depth": {},
        "dlq_count": 0,
        "runtime_services_health": {
            "ok": True,
            "code": "runtime_services_healthy",
            "source": "scheduled_doctor_event",
            "watcher": {"enabled": True, "alive": True},
            "webhook": {"enabled": False, "alive": False},
        },
    })

    assert status["runtime_liveness"]["worker"]["status"] == "healthy"
    assert status["runtime_liveness"]["services"]["watch"]["status"] == "healthy"
    assert status["runtime_liveness"]["services"]["webhook"]["status"] == (
        "not_configured"
    )
    assert status["runtime_liveness"]["poll_probes"]["status"] == "missing"
    assert status["runtime_liveness"]["sources"]["docs"]["status"] == "missing"


def test_fresh_producers_do_not_hide_stale_doctor_runtime_overlay(
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.readiness test.runtime-authority.readiness",
    )
    status = build_source_sync_status_contract({
        "deployment": "demo",
        "producers": [
            {
                "source_name": "pact",
                "producer": "watch",
                "age_seconds": 10,
            }
        ],
        "expected_producers": [
            {
                "source_name": "pact",
                "producer": "watch",
                "status": "healthy",
                "max_age_seconds": 900,
                "observed": {"age_seconds": 10},
            }
        ],
        "disabled_producers": [],
        "missing_producers": [],
        "stale_producers": [],
        "config_warnings": [],
        "queue_depth": [],
        "dbos_queue_depth": {},
        "dlq_count": 0,
        "runtime_services_health": {
            "ok": False,
            "code": "runtime_services_overlay_stale",
            "source": "scheduled_doctor_event",
            "message": "watcher: thread not alive",
            "watcher": {"enabled": True, "alive": False},
            "webhook": {"enabled": False, "alive": False},
        },
    })

    assert status["ok"] is False
    assert status["blockers"] == [{
        "code": "source_sync_runtime_unhealthy",
        "status": "blocked",
    }]
    assert status["runtime_liveness"]["status"] == "blocked"
    assert status["runtime_liveness"]["worker"]["code"] == (
        "runtime_services_overlay_stale"
    )
    assert status["runtime_liveness"]["services"]["watch"]["status"] == "down"


def test_runtime_services_down_blocks_even_with_fresh_producers() -> None:
    status = build_source_sync_status_contract({
        "deployment": "demo",
        "producers": [
            {
                "source_name": "pact",
                "producer": "watch",
                "age_seconds": 10,
            }
        ],
        "expected_producers": [
            {
                "source_name": "pact",
                "producer": "watch",
                "status": "healthy",
                "max_age_seconds": 900,
                "observed": {"age_seconds": 10},
            }
        ],
        "disabled_producers": [],
        "missing_producers": [],
        "stale_producers": [],
        "config_warnings": [],
        "queue_depth": [],
        "dbos_queue_depth": {},
        "dlq_count": 0,
        "runtime_services_health": {
            "ok": False,
            "code": "runtime_services_down",
            "source": "scheduled_doctor_event",
            "message": "worker: systemd unit failed",
            "watcher": {"enabled": True, "alive": False},
            "webhook": {"enabled": False, "alive": False},
        },
    })

    assert status["ok"] is False
    assert status["blockers"] == [{
        "code": "source_sync_runtime_unhealthy",
        "status": "blocked",
    }]
    assert status["runtime_liveness"]["status"] == "blocked"
    assert status["runtime_liveness"]["worker"]["status"] == "down"


def test_cli_readiness_and_mcp_share_source_sync_status_contract() -> None:
    status = build_source_sync_status_contract(
        {
            "deployment": "demo",
            "producers": [],
            "expected_producers": [
                {
                    "source_name": "docs",
                    "producer": "poll",
                    "status": "healthy",
                }
            ],
            "disabled_producers": [],
            "missing_producers": [],
            "stale_producers": [],
            "config_warnings": [],
            "queue_depth": [],
            "dbos_queue_depth": {"active_count": 0},
            "dlq_count": 0,
        },
        source_freshness={
            "ok": True,
            "missing": [],
            "stale": [],
            "groups": {
                "docs": {
                    "ok": True,
                    "sources": [{"source": "demo.docs", "status": "fresh"}],
                }
            },
        },
    )
    mcp_slice = source_sync_status_for_source(status, "docs")

    assert status["schema"] == SOURCE_SYNC_STATUS_SCHEMA
    assert status["status"] == "healthy"
    assert mcp_slice["schema"] == status["schema"]
    assert mcp_slice["deployment"] == status["deployment"]
    assert mcp_slice["status"] == "healthy"


def test_evaluated_convergence_failure_blocks_source_sync_status() -> None:
    status = build_source_sync_status_contract(
        {
            "deployment": "demo",
            "producers": [],
            "expected_producers": [],
            "disabled_producers": [],
            "missing_producers": [],
            "stale_producers": [],
            "config_warnings": [],
            "queue_depth": [],
            "dbos_queue_depth": {"active_count": 0},
            "dlq_count": 0,
        },
        source_inventory=[{
            "deployment": "demo",
            "source_name": "docs",
            "runtime": {"enabled": True, "source_sync_enabled": True},
            "sync": {"declared": True, "reconcile_run_mode": "scope_complete"},
            "freshness_policy": {
                "readiness_required": True,
                "completed_scope_required_for_readiness": True,
            },
            "constraints": {},
        }],
        source_freshness={
            "ok": True,
            "missing": [],
            "stale": [],
            "groups": {
                "docs": {
                    "ok": True,
                    "sources": [{"source": "demo.docs", "status": "fresh"}],
                }
            },
        },
        generation={"status": "missing", "generation_id": None},
        mcp={"ok": False},
    )

    assert status["ok"] is False
    assert status["status"] == "blocked"
    assert any(
        blocker.get("code") == "source_sync_convergence_not_ready"
        for blocker in status["blockers"]
    )


def test_convergence_failure_blocks_when_source_freshness_not_evaluated() -> None:
    status = build_source_sync_status_contract(
        {
            "deployment": "demo",
            "producers": [],
            "expected_producers": [],
            "disabled_producers": [],
            "missing_producers": [],
            "stale_producers": [],
            "config_warnings": [],
            "queue_depth": [],
            "dbos_queue_depth": {"active_count": 0},
            "dlq_count": 0,
        },
        source_inventory=[{
            "deployment": "demo",
            "source_name": "docs",
            "runtime": {"enabled": True, "source_sync_enabled": True},
            "sync": {"declared": True, "reconcile_run_mode": "scope_complete"},
            "freshness_policy": {
                "readiness_required": True,
                "completed_scope_required_for_readiness": True,
            },
            "constraints": {},
        }],
        generation={"status": "missing", "generation_id": None},
        mcp={"ok": False},
    )

    assert status["ok"] is False
    assert status["status"] == "blocked"
    assert any(
        blocker.get("code") == "source_sync_convergence_not_ready"
        for blocker in status["blockers"]
    )
    assert {
        blocker["code"] for blocker in status["blockers"]
    } == {"source_sync_convergence_not_ready"}
    assert status["convergence"]["state"] == "blocked"
    assert status["convergence"]["blockers"][0]["reason"] == (
        "completed_scope_watermark_not_evaluated"
    )
