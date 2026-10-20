from __future__ import annotations

import time

import psycopg
import pytest

from okg.substrate.db.management import dbos_backlog_gc
from okg.substrate.source_sync import SourceSyncEvent, SourceSyncJob
from okg.substrate.source_sync_enqueue import _source_sync_workflow_args


pytestmark = pytest.mark.postgres


def test_dbos_gc_decodes_source_sync_deployment_and_stage_timing(
    clean_runtime: str,
) -> None:
    from dbos._serialization import (
        DBOSDefaultSerializer,
        WorkflowSerializationFormat,
        serialize_args,
    )

    job = SourceSyncJob(
        deployment="okg-workspace",
        sources=("git-files",),
        run_mode="scope_incomplete",
        events=(
            SourceSyncEvent(
                deployment="okg-workspace",
                source_name="git-files",
                trigger_kind="fs_event",
                scope_key="src/okg/substrate/source_sync.py",
            ),
        ),
        dependent_fanout="none",
    )
    inputs, serialization = serialize_args(
        _source_sync_workflow_args(job),
        {},
        WorkflowSerializationFormat.NATIVE,
        DBOSDefaultSerializer,
    )
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
              deduplication_id text,
              created_at bigint NOT NULL,
              updated_at bigint NOT NULL,
              inputs text,
              serialization text
            )
            """
        )
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid, status, name, queue_name, deduplication_id,
              created_at, updated_at, inputs, serialization
            )
            VALUES (
              'wf-stale-sync', 'ENQUEUED', 'okg_source_sync',
              'okg-okg-workspace-source-sync',
              'sync:okg-workspace:watch:scope_incomplete:git-files:none:1',
              %s, %s, %s, %s
            )
            """,
            (old_ms, old_ms, inputs, serialization),
        )

    try:
        report = dbos_backlog_gc(
            clean_runtime,
            stale_after_seconds=600,
            queue_name="okg-okg-workspace-source-sync",
            workflow_name="okg_source_sync",
        )

        assert report["eligible_count"] == 1
        sample = report["sample"][0]["source_sync"]
        assert sample["decode_status"] == "decoded"
        assert sample["deployment"] == "okg-workspace"
        assert sample["sources"] == ["git-files"]
        assert sample["run_mode"] == "scope_incomplete"
        assert sample["scope_keys_sample"] == [
            "src/okg/substrate/source_sync.py"
        ]
        assert sample["stage_timing"]["queue_wait"]["status"] == "observable"
        assert sample["stage_timing"]["setup"]["status"] == "observable"
        assert sample["stage_timing"]["source_execution"]["status"] == "pending"
        assert sample["stage_timing"]["source_stage"]["status"] == "pending"
        assert sample["stage_timing"]["publish"]["status"] == "pending"
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
