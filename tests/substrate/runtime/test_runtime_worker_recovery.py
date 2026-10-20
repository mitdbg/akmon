from __future__ import annotations

from types import SimpleNamespace

import psycopg
import pytest


def test_worker_recovery_and_shutdown_are_explicit(monkeypatch) -> None:
    from okg.substrate.runtime import admin

    class FakeClient:
        def list_queued_workflows(self, *, queue_name: str):
            if queue_name == "okg-okg-workspace":
                return [
                    SimpleNamespace(
                        workflow_id="wf-publisher",
                        name="okg_publisher_cycle",
                        status="ENQUEUED",
                        queue_name=queue_name,
                        recovery_attempts=2,
                    ),
                ]
            if queue_name == "okg-okg-workspace-source-sync":
                return [
                    {
                        "workflow_id": "wf-source-sync",
                        "name": "okg_source_sync",
                        "status": "PENDING",
                        "queue_name": queue_name,
                    },
                ]
            if queue_name == "okg-okg-workspace-index-maintenance":
                # okg#3112 step 2: the index-maintenance lane, idle here.
                return []
            if queue_name == "_dbos_internal_queue":
                return [
                    {
                        "workflow_id": "wf-scheduled-doctor",
                        "name": "doctor_workflow",
                        "status": "ENQUEUED",
                        "queue_name": queue_name,
                    },
                ]
            raise AssertionError(f"unexpected queue {queue_name!r}")

    monkeypatch.setattr(admin, "_build_client", lambda *, dsn: FakeClient())

    result = admin.client_drain(
        deployment="okg-workspace",
        dsn="postgres://unused",
        max_wait_seconds=0,
        stable_zero_seconds=0,
    )

    assert result.drained is False
    assert result.in_flight_at_timeout == 3
    assert result.pending_workflows == [
        {
            "workflow_id": "wf-publisher",
            "name": "okg_publisher_cycle",
            "status": "ENQUEUED",
            "queue_name": "okg-okg-workspace",
            "recovery_attempts": 2,
        },
        {
            "workflow_id": "wf-source-sync",
            "name": "okg_source_sync",
            "status": "PENDING",
            "queue_name": "okg-okg-workspace-source-sync",
        },
        {
            "workflow_id": "wf-scheduled-doctor",
            "name": "doctor_workflow",
            "status": "ENQUEUED",
            "queue_name": "_dbos_internal_queue",
        },
    ]


@pytest.mark.postgres
def test_operator_drain_includes_source_sync_queue(clean_runtime: str) -> None:
    from okg.substrate.runtime.worker import _drain_operator_queues

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
        cur.execute("CREATE SCHEMA IF NOT EXISTS dbos")
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
            VALUES (
              'wf-source-sync-active',
              'okg-okg-workspace-source-sync',
              'PENDING'
            )
            """
        )

    blocked = _drain_operator_queues(
        deployment="okg-workspace",
        dsn=clean_runtime,
        max_wait_seconds=0,
        poll_interval_seconds=0,
    )

    assert blocked.drained is False
    assert blocked.in_flight_at_timeout == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            UPDATE dbos.workflow_status
               SET status = 'SUCCESS'
             WHERE workflow_uuid = 'wf-source-sync-active'
            """
        )

    drained = _drain_operator_queues(
        deployment="okg-workspace",
        dsn=clean_runtime,
        max_wait_seconds=0,
        poll_interval_seconds=0,
    )

    assert drained.drained is True
    assert drained.in_flight_at_timeout == 0


@pytest.mark.postgres
def test_stamps_unversioned_deployment_enqueued_workflows(
    clean_runtime: str,
) -> None:
    from okg.substrate.runtime.services import (
        stamp_unversioned_deployment_enqueued,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
        cur.execute("CREATE SCHEMA IF NOT EXISTS dbos")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dbos.workflow_status (
              workflow_uuid text PRIMARY KEY,
              status text NOT NULL,
              name text NOT NULL,
              queue_name text,
              application_version text,
              created_at bigint,
              updated_at bigint
            )
            """
        )
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid, status, name, queue_name, application_version,
              created_at, updated_at
            )
            VALUES
              ('wf-null', 'ENQUEUED', 'okg_source_sync',
               'okg-okg-workspace', NULL, 100, 100),
              ('wf-source-sync-lane-null', 'ENQUEUED', 'okg_source_sync',
               'okg-okg-workspace-source-sync', NULL, 100, 100),
              ('wf-other-queue', 'ENQUEUED', 'okg_source_sync',
               'okg-other', NULL, 100, 100),
              ('wf-versioned', 'ENQUEUED', 'okg_source_sync',
               'okg-okg-workspace', 'app-old', 100, 100)
            """
        )

    result = stamp_unversioned_deployment_enqueued(
        dsn=clean_runtime,
        deployment="okg-workspace",
        live_version="app-live",
    )

    assert result["stamped_count"] == 2
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT workflow_uuid, application_version
              FROM dbos.workflow_status
             WHERE workflow_uuid IN (
               'wf-null', 'wf-other-queue',
               'wf-source-sync-lane-null', 'wf-versioned'
             )
             ORDER BY workflow_uuid
            """
        )
        rows = dict(cur.fetchall())

    assert rows == {
        "wf-null": "app-live",
        "wf-other-queue": None,
        "wf-source-sync-lane-null": "app-live",
        "wf-versioned": "app-old",
    }


@pytest.mark.postgres
def test_stamps_replaceable_deployment_enqueued_workflows(
    clean_runtime: str,
) -> None:
    from okg.substrate.runtime.services import (
        stamp_replaceable_deployment_enqueued,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
        cur.execute("CREATE SCHEMA IF NOT EXISTS dbos")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dbos.workflow_status (
              workflow_uuid text PRIMARY KEY,
              status text NOT NULL,
              name text NOT NULL,
              queue_name text,
              application_version text,
              created_at bigint,
              updated_at bigint
            )
            """
        )
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid, status, name, queue_name, application_version,
              created_at, updated_at
            )
            VALUES
              ('wf-source-sync', 'ENQUEUED', 'okg_source_sync',
               'okg-okg-workspace', 'app-old', 100, 100),
              ('wf-source-sync-lane', 'ENQUEUED', 'okg_source_sync',
               'okg-okg-workspace-source-sync', 'app-old', 100, 100),
              ('wf-source-poll', 'DELAYED', 'okg_source_poll_git-files',
               'okg-okg-workspace', NULL, 100, 100),
              ('wf-custom', 'ENQUEUED', 'custom_workflow',
               'okg-okg-workspace', 'app-old', 100, 100),
              ('wf-pending', 'PENDING', 'okg_source_sync',
               'okg-okg-workspace', 'app-old', 100, 100),
              ('wf-other-queue', 'ENQUEUED', 'okg_source_sync',
               'okg-other', 'app-old', 100, 100)
            """
        )

    result = stamp_replaceable_deployment_enqueued(
        dsn=clean_runtime,
        deployment="okg-workspace",
        live_version="app-live",
    )

    assert result["stamped_count"] == 3
    assert {
        row["workflow_uuid"] for row in result["stamped_sample"]
    } == {
        "wf-source-sync",
        "wf-source-poll",
        "wf-source-sync-lane",
    }
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT workflow_uuid, application_version
              FROM dbos.workflow_status
             ORDER BY workflow_uuid
            """
        )
        rows = dict(cur.fetchall())

    assert rows == {
        "wf-custom": "app-old",
        "wf-other-queue": "app-old",
        "wf-pending": "app-old",
        "wf-source-poll": "app-live",
        "wf-source-sync": "app-live",
        "wf-source-sync-lane": "app-live",
    }
