from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import psycopg
import pytest

from okg.substrate.source_sync_diagnostics import source_frontier_report


pytestmark = pytest.mark.postgres


def _publish_generation(cur: psycopg.Cursor, deployment: str = "svc") -> None:
    cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
    cur.execute(
        """
        INSERT INTO okg.graph_generations (
          generation_id,
          deployment_name,
          status,
          catalog_version_id,
          published_at,
          correlation_id
        )
        VALUES (101, %s, 'published', 1, %s, 'frontier-test-gen')
        ON CONFLICT (generation_id) DO UPDATE SET
          deployment_name = EXCLUDED.deployment_name,
          status = EXCLUDED.status,
          published_at = EXCLUDED.published_at
        """,
        (deployment, datetime(2026, 6, 20, tzinfo=timezone.utc)),
    )
    cur.execute(
        """
        INSERT INTO public._chronos_branch_interval_checkpoints (
          checkpoint_id,
          branch_id,
          segment_id,
          created_at,
          metadata
        )
        -- The checkpoint names the branch's head segment: a real published
        -- state with no graph write after it, so the view can judge it
        -- (issue #3024). A segment that does not exist reads
        -- published_state_unavailable.
        SELECT
          '101',
          'default',
          b.current_segment_id,
          '2026-06-20T00:00:00+00:00',
          %s
        FROM public._chronos_branch_interval_branches b
        WHERE b.branch_id = 'default'
        ON CONFLICT (checkpoint_id) DO UPDATE SET
          metadata = EXCLUDED.metadata,
          created_at = EXCLUDED.created_at
        """,
        (
            json.dumps({
                "kind": "okg_generation",
                "status": "published",
                "deployment_name": deployment,
                "catalog_version_id": 1,
            }),
        ),
    )


def _insert_completed_frontier(
    cur: psycopg.Cursor,
    *,
    deployment: str = "svc",
    registry_source_name: str = "docs",
    projection_cursor: int = 42,
) -> None:
    source_name = f"{deployment}.{registry_source_name}"
    cur.execute(
        """
        INSERT INTO okg.source_watermarks (
          source,
          cutover_watermark,
          last_applied_rev,
          last_published_rev,
          projection_cursor,
          last_reconcile_at
        )
        VALUES (
          %s,
          '{}'::jsonb,
          %s::jsonb,
          %s::jsonb,
          %s,
          now() - interval '5 minutes'
        )
        """,
        (
            source_name,
            json.dumps({
                "cursor": "applied-42",
                "record_set_summary": {
                    "schema": "okg.source_runner.record_set_summary.v1",
                    "record_count": 2,
                    "node_count": 3,
                    "edge_count": 4,
                },
            }),
            json.dumps({"cursor": "published-42"}),
            projection_cursor,
        ),
    )
    cur.execute(
        """
        INSERT INTO okg.source_progress (
          deployment,
          source_name,
          key,
          value,
          updated_at
        )
        VALUES (
          %s,
          %s,
          'sync.last_run',
          %s,
          now() - interval '4 minutes'
        )
        """,
        (
            deployment,
            source_name,
            json.dumps({
                "schema": "okg.source_runner.last_run.v1",
                "run_id": f"run-{registry_source_name}-1",
                "status": "OK",
                "mode": "scope_complete",
                "completed_scope": True,
                "nodes_appended": 3,
                "edges_appended": 4,
            }),
        ),
    )


def _create_dbos_workflow_status(cur: psycopg.Cursor) -> None:
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
          deduplication_id text,
          created_at bigint NOT NULL,
          updated_at bigint NOT NULL
        )
        """
    )


def test_source_frontier_view_reports_published_visible_state(
    clean_runtime: str,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.source-frontier.view test.source-frontier.real-state "
        "task.source-frontier.view",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _publish_generation(cur)
        _insert_completed_frontier(cur)
        cur.execute(
            """
            SELECT deployment,
                   source_name,
                   registry_source_name,
                   latest_published_generation_id,
                   last_run_id,
                   last_run_completed_scope,
                   nodes_appended,
                   edges_appended,
                   record_set_state,
                   record_set_node_count,
                   read_visibility_state,
                   frontier_state
              FROM okg.v_source_frontier_state
             WHERE deployment = 'svc'
               AND registry_source_name = 'docs'
            """
        )
        row = cur.fetchone()

    assert row == (
        "svc",
        "svc.docs",
        "docs",
        "101",
        "run-docs-1",
        True,
        3,
        4,
        "summary_only",
        3,
        "read_visible",
        "fresh",
    )


def test_source_frontier_view_redacts_raw_watermark_json(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema = 'okg'
               AND table_name = 'v_source_frontier_state'
            """
        )
        columns = {row[0] for row in cur.fetchall()}

    assert "cutover_watermark" not in columns
    assert "last_applied_rev" not in columns
    assert "last_published_rev" not in columns


def test_source_frontier_view_does_not_mark_active_work_read_visible(
    clean_runtime: str,
) -> None:
    now_ms = int(time.time() * 1000)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _publish_generation(cur)
        _insert_completed_frontier(cur)
        _create_dbos_workflow_status(cur)
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid,
              status,
              name,
              queue_name,
              priority,
              deduplication_id,
              created_at,
              updated_at
            )
            VALUES (
              'wf-docs-active',
              'ENQUEUED',
              'okg_source_sync',
              'okg-svc-source-sync',
              20,
              'sync:svc:poll:scope_complete:docs:closure:1',
              %s,
              %s
            )
            """,
            (now_ms - 1_000, now_ms - 1_000),
        )
        cur.execute(
            """
            SELECT active_workflow_id,
                   workflow_status,
                   workflow_run_mode,
                   runtime_state,
                   read_visibility_state,
                   frontier_state
              FROM okg.v_source_frontier_state
             WHERE deployment = 'svc'
               AND registry_source_name = 'docs'
            """
        )
        row = cur.fetchone()

    assert row == (
        "wf-docs-active",
        "ENQUEUED",
        "scope_complete",
        "active",
        "frontier_advancing",
        "replaying",
    )
    # A run in flight needs nobody to act (issue #3024).
    report = source_frontier_report(clean_runtime, deployment="svc")
    assert report["status"] == "ready", report
    assert report["blocked_count"] == 0


def test_source_frontier_view_blocks_unattributed_active_work(
    clean_runtime: str,
) -> None:
    now_ms = int(time.time() * 1000)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _publish_generation(cur)
        _insert_completed_frontier(cur)
        _create_dbos_workflow_status(cur)
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid,
              status,
              name,
              queue_name,
              priority,
              deduplication_id,
              created_at,
              updated_at
            )
            VALUES
              (
                'wf-poll-batch',
                'ENQUEUED',
                'okg_source_sync',
                'okg-svc-source-sync',
                100,
                'sync:svc:poll:scope_complete:_poll_batch:closure:1',
                %s,
                %s
              ),
              (
                'wf-watch-undecoded',
                'PENDING',
                'okg_source_sync',
                'okg-svc-source-sync',
                100,
                NULL,
                %s,
                %s
              )
            """,
            (now_ms - 1_000, now_ms - 1_000, now_ms - 1_000, now_ms - 1_000),
        )
        cur.execute(
            """
            SELECT active_workflow_id,
                   runtime_state,
                   read_visibility_state,
                   frontier_state
              FROM okg.v_source_frontier_state
             WHERE deployment = 'svc'
               AND registry_source_name = 'docs'
            """
        )
        row = cur.fetchone()

    assert row == (
        None,
        "active_unattributed",
        "frontier_advancing",
        "replaying",
    )


def test_source_frontier_view_ranks_duplicate_active_workflows(
    clean_runtime: str,
) -> None:
    now_ms = int(time.time() * 1000)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _publish_generation(cur)
        _insert_completed_frontier(cur)
        _create_dbos_workflow_status(cur)
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid,
              status,
              name,
              queue_name,
              priority,
              deduplication_id,
              created_at,
              updated_at
            )
            VALUES
              (
                'wf-stale-incomplete',
                'ENQUEUED',
                'okg_source_sync',
                'okg-svc-source-sync',
                100,
                'sync:svc:watch:scope_incomplete:docs:direct:1',
                %s,
                %s
              ),
              (
                'wf-fresh-complete',
                'ENQUEUED',
                'okg_source_sync',
                'okg-svc-source-sync',
                20,
                'sync:svc:poll:scope_complete:docs:closure:2',
                %s,
                %s
              )
            """,
            (
                now_ms - 900_000,
                now_ms - 900_000,
                now_ms - 1_000,
                now_ms - 1_000,
            ),
        )
        cur.execute(
            """
            SELECT count(*)::int, max(active_workflow_id)
              FROM okg.v_source_frontier_state
             WHERE deployment = 'svc'
               AND registry_source_name = 'docs'
            """
        )
        row = cur.fetchone()

    assert row == (1, "wf-fresh-complete")


def test_source_frontier_view_surfaces_missing_record_set_blob(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _publish_generation(cur)
        cur.execute(
            """
            INSERT INTO okg.source_watermarks (
              source,
              last_applied_rev,
              last_published_rev,
              projection_cursor,
              last_reconcile_at
            )
            VALUES (
              'svc.docs',
              %s::jsonb,
              '{"cursor":"published"}'::jsonb,
              42,
              now()
            )
            """,
            (
                json.dumps({
                    "record_set_ref": {
                        "storage": "okg.source_record_set_blobs",
                        "sha256": "missing-sha",
                    }
                }),
            ),
        )
        cur.execute(
            """
            SELECT record_set_state, read_visibility_state, frontier_state
              FROM okg.v_source_frontier_state
             WHERE deployment = 'svc'
               AND registry_source_name = 'docs'
            """
        )
        row = cur.fetchone()

    assert row == ("external_blob_missing", "record_set_blob_missing", "partial")


def test_source_frontier_view_blocks_while_replay_is_required(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _publish_generation(cur)
        _insert_completed_frontier(cur)
        cur.execute(
            """
            UPDATE okg.source_watermarks
               SET replay_required = jsonb_build_object(
                     'run_id', 'interrupted-run',
                     'obligation', 'authoritative_full_replay',
                     'target_fingerprint', 'repair-target'
                   ),
                   replay_required_at = now()
             WHERE source = 'svc.docs'
            """
        )
        cur.execute(
            """
            SELECT read_visibility_state, frontier_state
              FROM okg.v_source_frontier_state
             WHERE deployment = 'svc'
               AND registry_source_name = 'docs'
            """
        )
        row = cur.fetchone()

    assert row == ("replay_required", "blocked")


def test_source_frontier_report_uses_operational_view(
    clean_runtime: str,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.source-frontier.view test.source-frontier.real-state "
        "task.source-frontier.status",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _publish_generation(cur)
        _insert_completed_frontier(cur)

    report = source_frontier_report(
        clean_runtime,
        deployment="svc",
        source="docs",
    )

    assert report["schema"] == "okg.source_sync.frontier_report.v1"
    assert report["ok"] is True
    assert report["row_count"] == 1
    assert report["returned_row_count"] == 1
    assert report["frontier_state_counts"] == {"fresh": 1}
    assert report["read_visibility_state_counts"] == {"read_visible": 1}
    assert report["rows"][0]["source_name"] == "svc.docs"


def test_source_frontier_report_fails_closed_without_rows(
    clean_runtime: str,
) -> None:
    report = source_frontier_report(
        clean_runtime,
        deployment="missing-deployment",
    )

    assert report["ok"] is False
    assert report["status"] == "no_frontier_rows"
    assert report["row_count"] == 0


def test_source_frontier_report_status_uses_unlimited_rows(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _publish_generation(cur)
        _insert_completed_frontier(cur, registry_source_name="docs")
        cur.execute(
            """
            INSERT INTO okg.source_watermarks (
              source,
              last_applied_rev,
              last_published_rev,
              projection_cursor,
              last_reconcile_at
            )
            VALUES (
              'svc.zzz-blocked',
              %s::jsonb,
              '{"cursor":"published"}'::jsonb,
              99,
              now()
            )
            """,
            (
                json.dumps({
                    "record_set_ref": {
                        "storage": "okg.source_record_set_blobs",
                        "sha256": "missing-sha",
                    }
                }),
            ),
        )

    report = source_frontier_report(
        clean_runtime,
        deployment="svc",
        limit=1,
    )

    assert report["ok"] is False
    assert report["row_count"] == 2
    assert report["returned_row_count"] == 1
    assert report["read_visibility_state_counts"] == {
        "read_visible": 1,
        "record_set_blob_missing": 1,
    }
