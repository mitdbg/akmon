from __future__ import annotations

import time

import psycopg
import pytest

from okg.substrate.runtime.dbos_enqueue import DBOSWorkflowEnqueueResult
from okg.substrate.source_sync import (
    SourceSyncCompaction,
    SourceSyncEvent,
    SourceSyncJob,
)
from okg.substrate.source_sync_enqueue import (
    active_dbos_source_sync_workflows_by_source,
    enqueue_compacted_source_sync_via_dbos,
)


pytestmark = pytest.mark.postgres


def _serialize_source_sync_job(job: SourceSyncJob) -> tuple[str, str]:
    from dbos._serialization import (
        DBOSDefaultSerializer,
        WorkflowSerializationFormat,
        serialize_args,
    )

    from okg.substrate.source_sync_enqueue import _source_sync_workflow_args

    return serialize_args(
        _source_sync_workflow_args(job),
        {},
        WorkflowSerializationFormat.NATIVE,
        DBOSDefaultSerializer,
    )


def _deserialize_source_sync_args(
    inputs: str,
    serialization: str,
) -> dict[str, object]:
    from dbos._serialization import (
        DBOSDefaultSerializer,
        deserialize_args,
    )

    return deserialize_args(
        inputs,
        serialization,
        DBOSDefaultSerializer,
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
          updated_at bigint NOT NULL,
          inputs text,
          serialization text
        )
        """
    )


def _insert_source_sync_workflow(
    cur: psycopg.Cursor,
    *,
    workflow_uuid: str,
    job: SourceSyncJob,
    status: str = "ENQUEUED",
    priority: int = 100,
    deduplication_id: str,
    created_at: int,
    updated_at: int | None = None,
) -> None:
    inputs, serialization = _serialize_source_sync_job(job)
    cur.execute(
        """
        INSERT INTO dbos.workflow_status (
          workflow_uuid, status, name, queue_name, priority,
          deduplication_id,
          created_at, updated_at, inputs, serialization
        )
        VALUES (
          %s, %s, 'okg_source_sync',
          'okg-test-source-sync', %s,
          %s, %s, %s, %s, %s
        )
        """,
        (
            workflow_uuid,
            status,
            priority,
            deduplication_id,
            created_at,
            updated_at if updated_at is not None else created_at,
            inputs,
            serialization,
        ),
    )


def test_equivalent_active_source_sync_work_is_bounded(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dbos._serialization import (
        DBOSDefaultSerializer,
        WorkflowSerializationFormat,
        deserialize_args,
        serialize_args,
    )

    from okg.substrate.source_sync_enqueue import _source_sync_workflow_args

    def fail_if_enqueue_called(**_kwargs):
        raise AssertionError("expected equivalent queued work to be compacted")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "_enqueue_source_sync_via_dbos_result",
        fail_if_enqueue_called,
    )

    compaction = SourceSyncCompaction(
        window_seconds=60,
        max_events=10,
        max_scope_keys=10,
    )
    existing_job = SourceSyncJob(
        deployment="test",
        sources=("docs",),
        run_mode="scope_complete",
        events=(
            SourceSyncEvent(
                deployment="test",
                source_name="docs",
                trigger_kind="poll",
                scope_key="first.md",
            ),
        ),
        dependent_fanout="closure",
    )
    incoming_job = SourceSyncJob(
        deployment="test",
        sources=("docs",),
        run_mode="scope_complete",
        events=(
            SourceSyncEvent(
                deployment="test",
                source_name="docs",
                trigger_kind="poll",
                scope_key="second.md",
            ),
        ),
        dependent_fanout="closure",
    )
    inputs, serialization = serialize_args(
        _source_sync_workflow_args(existing_job),
        {},
        WorkflowSerializationFormat.NATIVE,
        DBOSDefaultSerializer,
    )
    now_ms = int(time.time() * 1000)

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
              workflow_uuid, status, name, queue_name, priority,
              deduplication_id,
              created_at, updated_at, inputs, serialization
            )
            VALUES (
              'wf-existing', 'ENQUEUED', 'okg_source_sync',
              'okg-test-source-sync', 100,
              'sync:test:poll:scope_complete:_poll_batch:closure:1',
              %s, %s, %s, %s
            )
            """,
            (now_ms, now_ms, inputs, serialization),
        )

    try:
        workflow_id = enqueue_compacted_source_sync_via_dbos(
            deployment="test",
            dsn=clean_runtime,
            job=incoming_job,
            deduplication_id=(
                "sync:test:poll:scope_complete:_poll_batch:closure:2"
            ),
            compaction=compaction,
        )

        assert workflow_id == "wf-existing"
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT inputs, serialization, priority
                  FROM dbos.workflow_status
                 WHERE workflow_uuid = 'wf-existing'
                """
            )
            row = cur.fetchone()
            cur.execute("SELECT count(*) FROM dbos.workflow_status")
            assert cur.fetchone()[0] == 1
        workflow_inputs = deserialize_args(
            row[0],
            row[1],
            DBOSDefaultSerializer,
        )
        assert row[2] == 100
        events = workflow_inputs["args"][4]
        assert [event["scope_key"] for event in events] == [
            "first.md",
            "second.md",
        ]
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


def test_compatible_waiting_work_compacts_across_dedupe_buckets(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_if_enqueue_called(**_kwargs):
        raise AssertionError("expected compatible queued work to be compacted")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "_enqueue_source_sync_via_dbos_result",
        fail_if_enqueue_called,
    )

    compaction = SourceSyncCompaction(
        window_seconds=60,
        max_events=10,
        max_scope_keys=10,
    )
    first_job = SourceSyncJob(
        deployment="test",
        sources=("docs",),
        run_mode="scope_complete",
        events=(
            SourceSyncEvent(
                deployment="test",
                source_name="docs",
                trigger_kind="poll",
                scope_key="first.md",
            ),
        ),
        dependent_fanout="closure",
    )
    second_job = SourceSyncJob(
        deployment="test",
        sources=("notes",),
        run_mode="scope_complete",
        events=(
            SourceSyncEvent(
                deployment="test",
                source_name="notes",
                trigger_kind="poll",
                scope_key="second.md",
            ),
        ),
        dependent_fanout="closure",
    )
    incoming_job = SourceSyncJob(
        deployment="test",
        sources=("plans",),
        run_mode="scope_complete",
        events=(
            SourceSyncEvent(
                deployment="test",
                source_name="plans",
                trigger_kind="poll",
                scope_key="third.md",
            ),
        ),
        dependent_fanout="closure",
    )
    now_ms = int(time.time() * 1000)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _create_dbos_workflow_status(cur)
        _insert_source_sync_workflow(
            cur,
            workflow_uuid="wf-first",
            job=first_job,
            deduplication_id=(
                "sync:test:poll:scope_complete:docs:closure:1"
            ),
            created_at=now_ms - 20_000,
        )
        _insert_source_sync_workflow(
            cur,
            workflow_uuid="wf-second",
            job=second_job,
            deduplication_id=(
                "sync:test:poll:scope_complete:notes:closure:2"
            ),
            created_at=now_ms - 10_000,
        )

    try:
        workflow_id = enqueue_compacted_source_sync_via_dbos(
            deployment="test",
            dsn=clean_runtime,
            job=incoming_job,
            deduplication_id=(
                "sync:test:poll:scope_complete:plans:closure:3"
            ),
            compaction=compaction,
        )

        assert workflow_id == "wf-first"
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT workflow_uuid, status, inputs, serialization
                  FROM dbos.workflow_status
                 ORDER BY workflow_uuid
                """
            )
            rows = cur.fetchall()
        assert [(row[0], row[1]) for row in rows] == [
            ("wf-first", "ENQUEUED"),
            ("wf-second", "CANCELLED"),
        ]
        workflow_inputs = _deserialize_source_sync_args(rows[0][2], rows[0][3])
        assert workflow_inputs["args"][0] == ["docs", "notes", "plans"]
        assert [
            event["scope_key"] for event in workflow_inputs["args"][4]
        ] == ["first.md", "second.md", "third.md"]
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


def test_cross_source_compaction_preserves_trigger_family(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def fake_enqueue(**kwargs):
        calls.append(dict(kwargs))
        return DBOSWorkflowEnqueueResult("wf-incoming")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "_enqueue_source_sync_via_dbos_result",
        fake_enqueue,
    )

    compaction = SourceSyncCompaction(
        window_seconds=60,
        max_events=10,
        max_scope_keys=10,
    )
    existing_job = SourceSyncJob(
        deployment="test",
        sources=("compute-env-local",),
        run_mode="scope_complete",
        events=(
            SourceSyncEvent(
                deployment="test",
                source_name="compute-env-local",
                trigger_kind="poll",
            ),
        ),
        dependent_fanout="none",
    )
    incoming_job = SourceSyncJob(
        deployment="test",
        sources=("doc-corpus",),
        run_mode="scope_complete",
        events=(
            SourceSyncEvent(
                deployment="test",
                source_name="doc-corpus",
                trigger_kind="fs_event",
                scope_key="pact/changes/demo/evidence/report.md",
            ),
        ),
        dependent_fanout="none",
    )
    now_ms = int(time.time() * 1000)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _create_dbos_workflow_status(cur)
        _insert_source_sync_workflow(
            cur,
            workflow_uuid="wf-poll",
            job=existing_job,
            deduplication_id=(
                "sync:test:poll:scope_complete:compute-env-local:none:1"
            ),
            created_at=now_ms - 10_000,
        )

    try:
        workflow_id = enqueue_compacted_source_sync_via_dbos(
            deployment="test",
            dsn=clean_runtime,
            job=incoming_job,
            deduplication_id=(
                "sync:test:watch:scope_complete:doc-corpus:none:2"
            ),
            compaction=compaction,
        )

        assert workflow_id == "wf-incoming"
        assert len(calls) == 1
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT workflow_uuid, status, inputs, serialization
                  FROM dbos.workflow_status
                 ORDER BY workflow_uuid
                """
            )
            rows = cur.fetchall()
        assert [(row[0], row[1]) for row in rows] == [
            ("wf-poll", "ENQUEUED"),
        ]
        workflow_inputs = _deserialize_source_sync_args(rows[0][2], rows[0][3])
        assert workflow_inputs["args"][0] == ["compute-env-local"]
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


def test_same_source_compaction_preserves_trigger_family(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def fake_enqueue(**kwargs):
        calls.append(dict(kwargs))
        return DBOSWorkflowEnqueueResult("wf-reconcile")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "_enqueue_source_sync_via_dbos_result",
        fake_enqueue,
    )

    compaction = SourceSyncCompaction(
        window_seconds=60,
        max_events=10,
        max_scope_keys=10,
    )
    existing_job = SourceSyncJob(
        deployment="test",
        sources=("doc-corpus",),
        run_mode="scope_complete",
        events=(
            SourceSyncEvent(
                deployment="test",
                source_name="doc-corpus",
                trigger_kind="fs_event",
                scope_key="pact/changes/demo/evidence/report.md",
            ),
        ),
        dependent_fanout="none",
    )
    incoming_job = SourceSyncJob(
        deployment="test",
        sources=("doc-corpus",),
        run_mode="scope_complete",
        events=(
            SourceSyncEvent(
                deployment="test",
                source_name="doc-corpus",
                trigger_kind="reconcile",
                scope_key="pact/changes/demo/evidence/report.md.tmp",
            ),
        ),
        dependent_fanout="none",
    )
    now_ms = int(time.time() * 1000)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _create_dbos_workflow_status(cur)
        _insert_source_sync_workflow(
            cur,
            workflow_uuid="wf-fs-event",
            job=existing_job,
            deduplication_id=(
                "sync:test:watch:scope_complete:doc-corpus:none:1"
            ),
            created_at=now_ms - 10_000,
        )

    try:
        workflow_id = enqueue_compacted_source_sync_via_dbos(
            deployment="test",
            dsn=clean_runtime,
            job=incoming_job,
            deduplication_id=(
                "sync:test:reconcile:scope_complete:doc-corpus:none:2"
            ),
            compaction=compaction,
        )

        assert workflow_id == "wf-reconcile"
        assert len(calls) == 1
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT workflow_uuid, status, inputs, serialization
                  FROM dbos.workflow_status
                 ORDER BY workflow_uuid
                """
            )
            rows = cur.fetchall()
        assert [(row[0], row[1]) for row in rows] == [
            ("wf-fs-event", "ENQUEUED"),
        ]
        workflow_inputs = _deserialize_source_sync_args(rows[0][2], rows[0][3])
        assert workflow_inputs["args"][0] == ["doc-corpus"]
        assert [
            event["trigger_kind"] for event in workflow_inputs["args"][4]
        ] == ["fs_event"]
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


def test_readiness_recovery_does_not_compact_into_unrelated_waiting_work(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def fake_enqueue(**kwargs):
        calls.append(dict(kwargs))
        return DBOSWorkflowEnqueueResult("wf-readiness")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "_enqueue_source_sync_via_dbos_result",
        fake_enqueue,
    )

    compaction = SourceSyncCompaction(
        window_seconds=60,
        max_events=10,
        max_scope_keys=10,
    )
    existing_job = SourceSyncJob(
        deployment="okg-workspace",
        sources=("github-issues",),
        run_mode="reconcile",
    )
    readiness_job = SourceSyncJob(
        deployment="okg-workspace",
        sources=("coordination-native",),
        run_mode="reconcile",
    )
    now_ms = int(time.time() * 1000)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _create_dbos_workflow_status(cur)
        _insert_source_sync_workflow(
            cur,
            workflow_uuid="wf-existing",
            job=existing_job,
            deduplication_id=(
                "sync:okg-workspace:poll:reconcile:github-issues:none:1"
            ),
            created_at=now_ms - 10_000,
        )

    try:
        workflow_id = enqueue_compacted_source_sync_via_dbos(
            deployment="okg-workspace",
            dsn=clean_runtime,
            job=readiness_job,
            deduplication_id=(
                "sync:okg-workspace:poll:reconcile:"
                "coordination-native:none:2"
            ),
            compaction=compaction,
        )

        assert workflow_id == "wf-readiness"
        assert len(calls) == 1
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT workflow_uuid, status, inputs, serialization
                  FROM dbos.workflow_status
                 ORDER BY workflow_uuid
                """
            )
            rows = cur.fetchall()
        assert [(row[0], row[1]) for row in rows] == [
            ("wf-existing", "ENQUEUED"),
        ]
        workflow_inputs = _deserialize_source_sync_args(rows[0][2], rows[0][3])
        assert workflow_inputs["args"][0] == ["github-issues"]
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


def test_readiness_recovery_compacts_compatible_readiness_sources(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_if_enqueue_called(**_kwargs):
        raise AssertionError(
            "expected compatible readiness work to be compacted"
        )

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "_enqueue_source_sync_via_dbos_result",
        fail_if_enqueue_called,
    )

    compaction = SourceSyncCompaction(
        window_seconds=60,
        max_events=10,
        max_scope_keys=10,
    )
    existing_job = SourceSyncJob(
        deployment="test",
        sources=("coordination-config",),
        run_mode="scope_complete",
        include_predecessors=True,
    )
    incoming_job = SourceSyncJob(
        deployment="test",
        sources=("pact",),
        run_mode="scope_complete",
        include_predecessors=True,
    )
    now_ms = int(time.time() * 1000)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _create_dbos_workflow_status(cur)
        _insert_source_sync_workflow(
            cur,
            workflow_uuid="wf-readiness",
            job=existing_job,
            priority=20,
            deduplication_id=(
                "sync:test:poll:scope_complete:"
                "_poll_batch:none:1"
            ),
            created_at=now_ms - 10_000,
        )

    try:
        workflow_id = enqueue_compacted_source_sync_via_dbos(
            deployment="test",
            dsn=clean_runtime,
            job=incoming_job,
            deduplication_id=(
                "sync:test:poll:scope_complete:"
                "_poll_batch:none:2"
            ),
            compaction=compaction,
        )

        assert workflow_id == "wf-readiness"
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT workflow_uuid, status, inputs, serialization, priority
                  FROM dbos.workflow_status
                 ORDER BY workflow_uuid
                """
            )
            rows = cur.fetchall()
        assert [(row[0], row[1]) for row in rows] == [
            ("wf-readiness", "ENQUEUED"),
        ]
        workflow_inputs = _deserialize_source_sync_args(rows[0][2], rows[0][3])
        assert workflow_inputs["args"][0] == [
            "coordination-config",
            "pact",
        ]
        assert rows[0][4] == 20
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


def test_exact_compaction_collapses_duplicate_waiting_siblings(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_if_enqueue_called(**_kwargs):
        raise AssertionError("expected duplicate queued work to be compacted")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "_enqueue_source_sync_via_dbos_result",
        fail_if_enqueue_called,
    )

    compaction = SourceSyncCompaction(
        window_seconds=60,
        max_events=10,
        max_scope_keys=10,
    )
    first_job = SourceSyncJob(
        deployment="test",
        sources=("docs",),
        run_mode="scope_complete",
        events=(
            SourceSyncEvent(
                deployment="test",
                source_name="docs",
                trigger_kind="poll",
                scope_key="first.md",
            ),
        ),
        dependent_fanout="closure",
    )
    second_job = SourceSyncJob(
        deployment="test",
        sources=("docs",),
        run_mode="scope_complete",
        events=(
            SourceSyncEvent(
                deployment="test",
                source_name="docs",
                trigger_kind="poll",
                scope_key="second.md",
            ),
        ),
        dependent_fanout="closure",
    )
    incoming_job = SourceSyncJob(
        deployment="test",
        sources=("docs",),
        run_mode="scope_complete",
        events=(
            SourceSyncEvent(
                deployment="test",
                source_name="docs",
                trigger_kind="poll",
                scope_key="third.md",
            ),
        ),
        dependent_fanout="closure",
    )
    now_ms = int(time.time() * 1000)
    dedupe = "sync:test:poll:scope_complete:docs:closure:successor"

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _create_dbos_workflow_status(cur)
        _insert_source_sync_workflow(
            cur,
            workflow_uuid="wf-first",
            job=first_job,
            deduplication_id=dedupe,
            created_at=now_ms - 20_000,
        )
        _insert_source_sync_workflow(
            cur,
            workflow_uuid="wf-second",
            job=second_job,
            deduplication_id=dedupe,
            created_at=now_ms - 10_000,
        )

    try:
        workflow_id = enqueue_compacted_source_sync_via_dbos(
            deployment="test",
            dsn=clean_runtime,
            job=incoming_job,
            deduplication_id=dedupe,
            compaction=compaction,
        )

        assert workflow_id == "wf-first"
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT workflow_uuid, status, inputs, serialization
                  FROM dbos.workflow_status
                 ORDER BY workflow_uuid
                """
            )
            rows = cur.fetchall()
        assert [(row[0], row[1]) for row in rows] == [
            ("wf-first", "ENQUEUED"),
            ("wf-second", "CANCELLED"),
        ]
        workflow_inputs = _deserialize_source_sync_args(rows[0][2], rows[0][3])
        assert [
            event["scope_key"] for event in workflow_inputs["args"][4]
        ] == ["first.md", "second.md", "third.md"]
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


def test_compatible_compaction_does_not_cancel_pending_work(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def fake_enqueue(**kwargs):
        calls.append(dict(kwargs))
        return DBOSWorkflowEnqueueResult("wf-new")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "_enqueue_source_sync_via_dbos_result",
        fake_enqueue,
    )

    compaction = SourceSyncCompaction(
        window_seconds=60,
        max_events=10,
        max_scope_keys=10,
    )
    pending_job = SourceSyncJob(
        deployment="test",
        sources=("docs",),
        run_mode="scope_complete",
        dependent_fanout="closure",
    )
    incoming_job = SourceSyncJob(
        deployment="test",
        sources=("notes",),
        run_mode="scope_complete",
        dependent_fanout="closure",
    )
    now_ms = int(time.time() * 1000)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _create_dbos_workflow_status(cur)
        _insert_source_sync_workflow(
            cur,
            workflow_uuid="wf-pending",
            job=pending_job,
            status="PENDING",
            deduplication_id=(
                "sync:test:poll:scope_complete:docs:closure:1"
            ),
            created_at=now_ms - 20_000,
        )

    try:
        workflow_id = enqueue_compacted_source_sync_via_dbos(
            deployment="test",
            dsn=clean_runtime,
            job=incoming_job,
            deduplication_id=(
                "sync:test:watch:scope_complete:notes:closure:2"
            ),
            compaction=compaction,
        )

        assert workflow_id == "wf-new"
        assert len(calls) == 1
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT workflow_uuid, status
                  FROM dbos.workflow_status
                 ORDER BY workflow_uuid
                """
            )
            assert cur.fetchall() == [("wf-pending", "PENDING")]
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


def test_active_work_selection_prefers_fresh_completed_scope_recovery(
    clean_runtime: str,
) -> None:
    now_ms = int(time.time() * 1000)
    incomplete_job = SourceSyncJob(
        deployment="test",
        sources=("pact",),
        run_mode="scope_incomplete",
        dependent_fanout="direct",
    )
    completed_job = SourceSyncJob(
        deployment="test",
        sources=("pact",),
        run_mode="scope_complete",
        dependent_fanout="closure",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _create_dbos_workflow_status(cur)
        _insert_source_sync_workflow(
            cur,
            workflow_uuid="wf-incomplete",
            job=incomplete_job,
            deduplication_id=(
                "sync:test:watch:scope_incomplete:pact:direct:1"
            ),
            created_at=now_ms - 900_000,
            updated_at=now_ms - 900_000,
        )
        _insert_source_sync_workflow(
            cur,
            workflow_uuid="wf-complete",
            job=completed_job,
            priority=20,
            deduplication_id=(
                "sync:test:poll:scope_complete:pact:closure:2"
            ),
            created_at=now_ms - 60_000,
            updated_at=now_ms - 60_000,
        )

    try:
        active = active_dbos_source_sync_workflows_by_source(
            dsn=clean_runtime,
            deployment="test",
            stale_after_seconds=600,
        )

        pact = active["pact"]
        assert pact["workflow_id"] == "wf-complete"
        assert pact["run_mode"] == "scope_complete"
        assert pact["stale"] is False
        assert pact["candidate_count"] == 2
        assert pact["selection_reason"] == "preferred_fresh_completed_scope"
        assert set(pact["candidate_workflow_ids"]) == {
            "wf-incomplete",
            "wf-complete",
        }
        candidates = {
            candidate["workflow_id"]: candidate
            for candidate in pact["active_work_candidates"]
        }
        assert candidates["wf-incomplete"]["stale"] is True
        assert candidates["wf-complete"]["stale"] is False
        frontier = pact["desired_frontier"]
        assert frontier["state"] == "waiting"
        assert frontier["completed_scope_run_mode"] is True
        assert frontier["completed"] is False
        assert frontier["completion_state"] == "not_completed"
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


def test_compaction_upgrades_existing_source_sync_priority_for_profile(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dbos._serialization import (
        DBOSDefaultSerializer,
        WorkflowSerializationFormat,
        deserialize_args,
        serialize_args,
    )

    from okg.substrate.source_sync_enqueue import _source_sync_workflow_args

    def fail_if_enqueue_called(**_kwargs):
        raise AssertionError("expected equivalent queued work to be compacted")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "_enqueue_source_sync_via_dbos_result",
        fail_if_enqueue_called,
    )

    compaction = SourceSyncCompaction(
        window_seconds=60,
        max_events=10,
        max_scope_keys=10,
    )
    existing_job = SourceSyncJob(
        deployment="test",
        sources=("docs",),
        run_mode="scope_complete",
        dependent_fanout="closure",
    )
    incoming_job = SourceSyncJob(
        deployment="test",
        sources=("docs",),
        run_mode="scope_complete",
        dependent_fanout="closure",
        profile_force_deep=True,
        profile_collectors=("rootless-speedscope",),
    )
    inputs, serialization = serialize_args(
        _source_sync_workflow_args(existing_job),
        {},
        WorkflowSerializationFormat.NATIVE,
        DBOSDefaultSerializer,
    )
    now_ms = int(time.time() * 1000)

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
              workflow_uuid, status, name, queue_name, priority,
              deduplication_id,
              created_at, updated_at, inputs, serialization
            )
            VALUES (
              'wf-existing', 'ENQUEUED', 'okg_source_sync',
              'okg-test-source-sync', 100,
              'sync:test:poll:scope_complete:_poll_batch:closure:1',
              %s, %s, %s, %s
            )
            """,
            (now_ms, now_ms, inputs, serialization),
        )

    try:
        workflow_id = enqueue_compacted_source_sync_via_dbos(
            deployment="test",
            dsn=clean_runtime,
            job=incoming_job,
            deduplication_id=(
                "sync:test:poll:scope_complete:_poll_batch:closure:2"
            ),
            compaction=compaction,
        )

        assert workflow_id == "wf-existing"
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT inputs, serialization, priority
                  FROM dbos.workflow_status
                 WHERE workflow_uuid = 'wf-existing'
                """
            )
            row = cur.fetchone()
        workflow_inputs = deserialize_args(
            row[0],
            row[1],
            DBOSDefaultSerializer,
        )
        assert row[2] == 1
        assert workflow_inputs["args"][7] is True
        assert workflow_inputs["args"][8] == ["rootless-speedscope"]
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
