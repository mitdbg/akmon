from __future__ import annotations

import time

import psycopg
import pytest

from okg.substrate.db.management import dbos_backlog_gc
from okg.substrate.errors import RequiresConfirm


pytestmark = pytest.mark.postgres


def _reset_dbos_schema(cur) -> None:
    cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
    cur.execute("CREATE SCHEMA dbos")
    cur.execute(
        """
        CREATE TABLE dbos.workflow_status (
          workflow_uuid text PRIMARY KEY,
          status text NOT NULL,
          name text,
          queue_name text,
          created_at bigint NOT NULL,
          updated_at bigint NOT NULL
        )
        """
    )


def test_dbos_backlog_gc_only_cancels_stale_target_queue(
    clean_runtime: str,
) -> None:
    old_ms = int((time.time() - 7200) * 1000)
    fresh_ms = int(time.time() * 1000)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _reset_dbos_schema(cur)
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid, status, name, queue_name, created_at, updated_at
            )
            VALUES
              (
                'wf-stale-target', 'ENQUEUED', 'okg_publisher_cycle',
                'okg-okg-workspace', %s, %s
              ),
              (
                'wf-fresh-target', 'ENQUEUED', 'okg_publisher_cycle',
                'okg-okg-workspace', %s, %s
              ),
              (
                'wf-stale-other-queue', 'ENQUEUED', 'okg_publisher_cycle',
                'okg-other', %s, %s
              ),
              (
                'wf-stale-success', 'SUCCESS', 'okg_publisher_cycle',
                'okg-okg-workspace', %s, %s
              )
            """,
            (
                old_ms,
                old_ms,
                fresh_ms,
                fresh_ms,
                old_ms,
                old_ms,
                old_ms,
                old_ms,
            ),
        )

    try:
        confirmed = dbos_backlog_gc(
            clean_runtime,
            stale_after_seconds=600,
            confirm=True,
            queue_name="okg-okg-workspace",
        )

        assert confirmed["eligible_count"] == 1
        assert confirmed["cancelled_count"] == 1

        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT workflow_uuid, status
                  FROM dbos.workflow_status
                 ORDER BY workflow_uuid
                """
            )
            assert cur.fetchall() == [
                ("wf-fresh-target", "ENQUEUED"),
                ("wf-stale-other-queue", "ENQUEUED"),
                ("wf-stale-success", "SUCCESS"),
                ("wf-stale-target", "CANCELLED"),
            ]
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


def test_dbos_backlog_gc_refuses_cancel_of_unconfirmed_pending_execution(
    clean_runtime: str,
) -> None:
    old_ms = int((time.time() - 7200) * 1000)
    fresh_ms = int(time.time() * 1000)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
        cur.execute("CREATE SCHEMA dbos")
        cur.execute(
            """
            CREATE TABLE dbos.workflow_status (
              workflow_uuid text PRIMARY KEY,
              status text NOT NULL,
              name text,
              queue_name text,
              priority integer,
              created_at bigint NOT NULL,
              updated_at bigint NOT NULL,
              started_at_epoch_ms bigint,
              recovery_attempts integer
            )
            """
        )
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid, status, name, queue_name, priority,
              created_at, updated_at, started_at_epoch_ms, recovery_attempts
            )
            VALUES
              (
                'wf-fresh-started', 'PENDING', 'okg_source_sync',
                'okg-okg-workspace-source-sync', 20,
                %s, %s, %s, 1
              ),
              (
                'wf-stale-pending', 'PENDING', 'okg_source_sync',
                'okg-okg-workspace-source-sync', 100,
                %s, %s, %s, 1
              )
            """,
            (
                old_ms,
                old_ms,
                fresh_ms,
                old_ms,
                old_ms,
                old_ms,
            ),
        )

    try:
        dry_run = dbos_backlog_gc(
            clean_runtime,
            stale_after_seconds=600,
            confirm=False,
            queue_name="okg-okg-workspace-source-sync",
            workflow_name="okg_source_sync",
            statuses=("PENDING", "ENQUEUED"),
        )

        assert dry_run["eligible_count"] == 1
        assert dry_run["cancelled_count"] == 0
        assert dry_run["unconfirmed_pending_count"] == 1
        assert dry_run["execution_termination_confirmed"] is False
        assert dry_run["replacement_safe"] is False
        assert dry_run["stale_column"] == (
            "greatest(updated_at,created_at,started_at_epoch_ms)"
        )
        with pytest.raises(
            RequiresConfirm,
            match="does not confirm Python execution terminated",
        ):
            dbos_backlog_gc(
                clean_runtime,
                stale_after_seconds=600,
                confirm=True,
                queue_name="okg-okg-workspace-source-sync",
                workflow_name="okg_source_sync",
                statuses=("PENDING", "ENQUEUED"),
            )
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT workflow_uuid, status
                  FROM dbos.workflow_status
                 ORDER BY workflow_uuid
                """
            )
            assert cur.fetchall() == [
                ("wf-fresh-started", "PENDING"),
                ("wf-stale-pending", "PENDING"),
            ]
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


def test_dbos_backlog_gc_preserves_priority_source_sync_enqueued(
    clean_runtime: str,
) -> None:
    old_ms = int((time.time() - 7200) * 1000)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
        cur.execute("CREATE SCHEMA dbos")
        cur.execute(
            """
            CREATE TABLE dbos.workflow_status (
              workflow_uuid text PRIMARY KEY,
              status text NOT NULL,
              name text,
              queue_name text,
              priority integer,
              created_at bigint NOT NULL,
              updated_at bigint NOT NULL
            )
            """
        )
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid, status, name, queue_name, priority,
              created_at, updated_at
            )
            VALUES
              (
                'wf-readiness', 'ENQUEUED', 'okg_source_sync',
                'okg-okg-workspace-source-sync', 20, %s, %s
              ),
              (
                'wf-routine', 'ENQUEUED', 'okg_source_sync',
                'okg-okg-workspace-source-sync', 100, %s, %s
              )
            """,
            (old_ms, old_ms, old_ms, old_ms),
        )

    try:
        confirmed = dbos_backlog_gc(
            clean_runtime,
            stale_after_seconds=600,
            confirm=True,
            queue_name="okg-okg-workspace-source-sync",
            workflow_name="okg_source_sync",
        )

        assert confirmed["eligible_count"] == 1
        assert confirmed["cancelled_count"] == 1
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT workflow_uuid, status
                  FROM dbos.workflow_status
                 ORDER BY workflow_uuid
                """
            )
            assert cur.fetchall() == [
                ("wf-readiness", "ENQUEUED"),
                ("wf-routine", "CANCELLED"),
            ]
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
