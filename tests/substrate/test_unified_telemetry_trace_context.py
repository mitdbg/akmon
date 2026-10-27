from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import types

import psycopg
import pytest

from okg.substrate.db.pool import get_pool
from okg.substrate.ingest import SourceRunner
from okg.substrate.ingest.service import IngestOptions, run_ingest
from okg.substrate.library.sources.base import SourceRun
from okg.substrate.llm.tracker import record_llm_call
from okg.substrate.mcp.tracing import trace_tool_call
from okg.substrate.publisher.publisher import Publisher
from okg.substrate.telemetry import (
    TelemetrySpan,
    child_span_identity,
    current_trace_identity,
    new_trace_id,
    record_span,
    use_trace,
)
from okg.substrate.telemetry.operator import telemetry_coverage
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer


pytestmark = pytest.mark.postgres


class _EmptySourceAdapter:
    name = "telemetry-context-source"
    profile = "discovery_crawl"

    def run(self, run_id: str, *, mode: str = "cursor") -> SourceRun:
        return SourceRun(
            facts=[],
            completed_scope=True,
            next_cursor={"run_id": run_id},
            run_mode=mode,
        )


def test_ingest_service_binds_one_trace_across_multiple_sources(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """Every source of one ingest records its spans under the ingest's trace.

    Each source runs in its own ``spawn`` child, so the trace has to cross
    the process boundary. The fixture deployment's two sources load a real
    library adapter by module name, as a child must.
    """
    from tests.pytest.catalog_fixture_builders import seed_catalog
    from tests.substrate.test_ingest_service import (
        _claim_layered_fixture_catalog,
        _write_layered_fixture_deployment,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    deployment_dir = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(clean_runtime, deployment_dir)

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=deployment_dir,
            publish=False,
        ),
    )

    assert outcome.any_failed is False, [
        (result.get("name"), result.get("stage"), result.get("error"))
        for result in outcome.report["results"]
    ]
    assert [result["name"] for result in outcome.report["results"]] == [
        "first",
        "second",
    ]
    trace_ids = {result["trace_id"] for result in outcome.report["results"]}
    assert len(trace_ids) == 1
    trace_id = next(iter(trace_ids))

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT source_name, array_agg(DISTINCT trace_id)
              FROM okg.telemetry_spans
             WHERE source_name = ANY(%s)
             GROUP BY source_name
            """,
            (["svc.first", "svc.second"],),
        )
        spans = {row[0]: set(row[1]) for row in cur.fetchall()}
    assert spans == {"svc.first": {trace_id}, "svc.second": {trace_id}}


def test_trace_context_joins_source_generation_mcp_and_llm(
    clean_runtime: str,
) -> None:
    trace_id = new_trace_id()

    with psycopg.connect(clean_runtime) as conn:
        seed_catalog_version(conn)
        conn.commit()

    with use_trace(trace_id) as root:
        runner = SourceRunner(
            clean_runtime,
            source_name=_EmptySourceAdapter.name,
            deployment_name="default",
            write_chronos_graph=False,
            write_fact_log=True,
        )
        run_result = runner.run(_EmptySourceAdapter())

        publisher = Publisher(
            clean_runtime,
            "default",
            catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
        )
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.autocommit = False
            try:
                reserved = publisher.reserve(conn)
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE okg.graph_generations
                           SET status = 'published',
                               published_at = now()
                         WHERE generation_id = %s
                        """,
                        (reserved.generation_id,),
                    )
            finally:
                conn.commit()

        trace_tool_call(
            get_pool(clean_runtime),
            deployment_name="default",
            tool_name="inspect",
            request={"target": {"kind": "graph"}},
            response={"node_count": 0, "total_count": 1},
            duration_ms=7,
            generation_id=reserved.generation_id,
            session_id="telemetry-context-session",
        )

        record_llm_call(
            clean_runtime,
            caller="telemetry-context",
            model="test-model",
            fragment_id="fragment-1",
            prompt_tokens=2,
            completion_tokens=3,
            total_tokens=5,
            cost_usd=0.001,
            latency_ms=11,
            success=True,
            deployment="default",
            generation_id=reserved.generation_id,
            extras={"test": "trace-context"},
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value
                  FROM okg.source_progress
                 WHERE deployment = 'default'
                   AND source_name = %s
                   AND key = 'sync.last_run'
                """,
                (_EmptySourceAdapter.name,),
            )
            source_payload = json.loads(cur.fetchone()[0])
            assert source_payload["trace_id"] == trace_id
            assert source_payload["span_id"] != root.span_id
            assert source_payload["parent_span_id"] == root.span_id
            assert source_payload["run_id"] == run_result.run_id
            assert (
                source_payload["unavailable"]["admission_id"]
                == "source_run_context_missing_admission_id"
            )
            assert (
                source_payload["unavailable"]["workflow_id"]
                == "source_run_context_missing_dbos_workflow_id"
            )

            cur.execute(
                """
                SELECT trace_id, status
                  FROM okg.graph_generations
                 WHERE generation_id = %s
                """,
                (reserved.generation_id,),
            )
            assert cur.fetchone() == (trace_id, "published")

            cur.execute(
                """
                SELECT span_name, parent_span_id, run_id
                  FROM okg.telemetry_spans
                 WHERE trace_id = %s
                 ORDER BY span_name
                """,
                (trace_id,),
            )
            spans = cur.fetchall()
            by_name = {row[0]: row for row in spans}
            assert by_name["source.telemetry-context-source"][2] == (
                run_result.run_id
            )
            assert by_name["mcp.inspect"][1] == root.span_id
            assert by_name["llm.telemetry-context"][1] == root.span_id

            cur.execute(
                """
                SELECT generation_id, source_name, session_id, span_count
                  FROM okg.v_telemetry_trace_summary
                 WHERE trace_id = %s
                """,
                (trace_id,),
            )
            generation_id, source_name, session_id, span_count = cur.fetchone()
            assert generation_id == str(reserved.generation_id)
            assert source_name == _EmptySourceAdapter.name
            assert session_id == "telemetry-context-session"
            assert span_count >= 3


def test_source_sync_workflow_records_dbos_workflow_root_and_child_trace(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.deployment.testing import mock_dbos_context
    from okg.substrate.ingest.service import IngestRunResult
    from okg.substrate.runtime import source_sync_workflow as workflow

    workflow_id = "wf-source-sync-telemetry"

    monkeypatch.setattr(
        workflow,
        "_resolve_context_cached",
        lambda deployment: (
            # No declared sync root: the workflow keeps the undeclared-root path.
            types.SimpleNamespace(profiling=None, sync_root=None),
            clean_runtime,
            tmp_path,
        ),
    )
    monkeypatch.setattr(
        workflow,
        "_source_sync_start_usefulness",
        lambda **_kwargs: {
            "useful": True,
            "reason": "selected_frontier",
            "workflow_id": workflow_id,
        },
    )
    # The workflow no longer plans or completes a separate job runtime (the
    # OKG job runtime subsystem was removed); it builds the
    # `source_sync_runtime` payload from the ingest report itself.

    def fake_run_ingest(_options):
        identity = current_trace_identity()
        assert identity.is_contextual
        assert identity.parent_span_id is None
        child = child_span_identity()
        now = datetime.now(timezone.utc)
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            record_span(
                conn,
                TelemetrySpan(
                    trace_id=child.trace_id,
                    span_id=child.span_id,
                    parent_span_id=child.parent_span_id,
                    span_name="source.workflow-child",
                    span_kind="internal",
                    status_code="ok",
                    deployment_name="okg-workspace",
                    workflow_id=workflow_id,
                    started_at=now,
                    ended_at=now,
                    attributes={"component": "source-runner"},
                    root_name="dbos.workflow.okg_source_sync",
                    root_kind="dbos_workflow",
                ),
            )
        return IngestRunResult(
            any_failed=False,
            report={
                "results": [{"source": "pact", "status": "OK"}],
                "publishes": [],
            },
        )

    monkeypatch.setattr(
        "okg.substrate.ingest.service.run_ingest",
        fake_run_ingest,
    )

    with mock_dbos_context() as mock:
        mock.workflow_id = workflow_id
        out = workflow._run_one_ingest(
            deployment="okg-workspace",
            sources=["pact"],
            run_mode="scope_complete",
            include_dependents=True,
            dependent_fanout=None,
            events=[],
            publish=True,
        )

    assert out["source_sync_runtime"]["status"] == "completed"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT trace_id, span_id, workflow_id, attributes
              FROM okg.telemetry_spans
             WHERE deployment_name = 'okg-workspace'
               AND span_name = 'dbos.workflow.okg_source_sync'
               AND workflow_id = %s
             ORDER BY event_time DESC
             LIMIT 1
            """,
            (workflow_id,),
        )
        root = cur.fetchone()
        assert root is not None
        trace_id, root_span_id, stored_workflow_id, attrs = root
        assert stored_workflow_id == workflow_id
        assert attrs["dbos.workflow.name"] == "okg_source_sync"
        assert attrs["dbos.workflow.id"] == workflow_id
        assert attrs["dbos.workflow.queue_name"] == "okg-okg-workspace-source-sync"
        assert attrs["unavailable"]["queue_wait_ms"] == (
            "dbos_workflow_queue_wait_not_available"
        )

        cur.execute(
            """
            SELECT parent_span_id, workflow_id
              FROM okg.telemetry_spans
             WHERE deployment_name = 'okg-workspace'
               AND trace_id = %s
               AND span_name = 'source.workflow-child'
            """,
            (trace_id,),
        )
        assert cur.fetchone() == (root_span_id, workflow_id)

        cur.execute(
            """
            SELECT root_name, root_kind, workflow_id
              FROM okg.v_telemetry_trace_summary
             WHERE deployment_name = 'okg-workspace'
               AND trace_id = %s
            """,
            (trace_id,),
        )
        assert cur.fetchone() == (
            "dbos.workflow.okg_source_sync",
            "dbos_workflow",
            workflow_id,
        )


def test_publisher_workflow_records_dbos_workflow_root_and_child_trace(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.deployment.testing import mock_dbos_context
    from okg.substrate.ingest import service as ingest_service
    from okg.substrate.runtime import publisher_workflow
    import okg.substrate.deployments as deployments
    import okg.substrate.publisher.deployment_stack as stack_mod

    from tests.pytest.catalog_fixture_builders import (
        bind_factory_test_deployment,
    )
    from tests.pytest.dbos_system_schema import create_dbos_system_schema

    workflow_id = "wf-publisher-telemetry"
    # The workflow builds its publisher through build_deployment_publisher,
    # which refuses without catalog writer authority and without a deployment
    # directory inside a committed repository. This test stubs the deployment
    # and its stack, so bind the explicit pytest catalog writer and hand the
    # stub stack a committed synthetic deployment directory.
    fixture_dir = bind_factory_test_deployment(
        monkeypatch, tmp_path, "okg-workspace",
    )

    class _Stack:
        deployment_name = "okg-workspace"
        deployment_dir = fixture_dir

        def publisher_kwargs(self):
            return {}

    class _Publisher:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def cycle(self, *, force_enrichers=False):
            assert force_enrichers is False
            identity = current_trace_identity()
            assert identity.is_contextual
            child = child_span_identity()
            now = datetime.now(timezone.utc)
            with psycopg.connect(clean_runtime, autocommit=True) as conn:
                record_span(
                    conn,
                    TelemetrySpan(
                        trace_id=child.trace_id,
                        span_id=child.span_id,
                        parent_span_id=child.parent_span_id,
                        span_name="publisher.workflow-child",
                        span_kind="internal",
                        status_code="ok",
                        deployment_name="okg-workspace",
                        workflow_id=workflow_id,
                        started_at=now,
                        ended_at=now,
                        attributes={"component": "publisher"},
                        root_name="dbos.workflow.okg_publisher_cycle",
                        root_kind="dbos_workflow",
                    ),
                )
            return {"generation_id": "1", "status": "published"}

    monkeypatch.setattr(
        deployments,
        "resolve_deployment",
        lambda _deployment: object(),
    )
    monkeypatch.setattr(
        deployments,
        "require_postgres_dsn",
        lambda _manifest, *, deployment: clean_runtime,
    )
    monkeypatch.setattr(
        publisher_workflow,
        "_active_source_sync_workflow_id_for_publisher",
        lambda *, deployment, dsn: None,
    )
    monkeypatch.setattr(
        stack_mod,
        "build_deployment_publisher_stack",
        lambda **_kwargs: _Stack(),
    )
    monkeypatch.setattr(stack_mod, "publisher_kwargs_from_env", lambda: {})
    monkeypatch.setattr(
        "okg.substrate.publisher.publisher.Publisher",
        _Publisher,
    )
    monkeypatch.setattr(
        ingest_service,
        "persist_publish_stage_timings",
        lambda **_kwargs: None,
    )

    # The cycle reads dbos.workflow_status to tell a cron-enqueued cycle from
    # a manual one (_is_scheduled_cycle). A real worker always has that schema;
    # build it here so the test does not depend on an earlier test on the same
    # worker having created it.
    create_dbos_system_schema(clean_runtime)

    with mock_dbos_context() as mock:
        mock.workflow_id = workflow_id
        summary = publisher_workflow._run_one_cycle(
            deployment="okg-workspace",
            branch_name="default",
        )

    assert summary["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT trace_id, span_id, attributes
              FROM okg.telemetry_spans
             WHERE deployment_name = 'okg-workspace'
               AND span_name = 'dbos.workflow.okg_publisher_cycle'
               AND workflow_id = %s
             ORDER BY event_time DESC
             LIMIT 1
            """,
            (workflow_id,),
        )
        root = cur.fetchone()
        assert root is not None
        trace_id, root_span_id, attrs = root
        assert attrs["dbos.workflow.name"] == "okg_publisher_cycle"
        assert attrs["dbos.workflow.queue_name"] == "okg-okg-workspace"

        cur.execute(
            """
            SELECT parent_span_id, workflow_id
              FROM okg.telemetry_spans
             WHERE deployment_name = 'okg-workspace'
               AND trace_id = %s
               AND span_name = 'publisher.workflow-child'
            """,
            (trace_id,),
        )
        assert cur.fetchone() == (root_span_id, workflow_id)


def test_dbos_workflow_root_updates_queue_retry_and_parallel_children(
    clean_runtime: str,
) -> None:
    from okg.substrate.runtime.workflow_telemetry import dbos_workflow_trace

    workflow_id = "00000000-0000-0000-0000-000000000123"
    root_started_at = datetime.now(timezone.utc) - timedelta(milliseconds=100)
    started_at_ms = int(root_started_at.timestamp() * 1000)
    root_started_at = datetime.fromtimestamp(started_at_ms / 1000.0, tz=timezone.utc)
    created_at_ms = started_at_ms - 4_000
    completed_at_ms = started_at_ms + 250
    expected_created_at = datetime.fromtimestamp(
        created_at_ms / 1000.0,
        tz=timezone.utc,
    )
    expected_completed_at = datetime.fromtimestamp(
        completed_at_ms / 1000.0,
        tz=timezone.utc,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dbos")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dbos.workflow_status (
              workflow_uuid text
            )
            """
        )
        for column, column_type in (
            ("status", "text"),
            ("name", "text"),
            ("queue_name", "text"),
            ("created_at", "bigint"),
            ("updated_at", "bigint"),
            ("started_at_epoch_ms", "bigint"),
            ("recovery_attempts", "bigint"),
            ("completed_at", "bigint"),
            ("executor_id", "text"),
            ("application_id", "text"),
        ):
            cur.execute(
                f"ALTER TABLE dbos.workflow_status "
                f"ADD COLUMN IF NOT EXISTS {column} {column_type}"
            )
        cur.execute(
            "DELETE FROM dbos.workflow_status WHERE workflow_uuid::text = %s",
            (workflow_id,),
        )
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid, status, name, queue_name, created_at,
              updated_at, started_at_epoch_ms, recovery_attempts, completed_at,
              executor_id, application_id
            )
            VALUES (%s, 'SUCCESS', 'okg_source_sync',
                    'okg-okg-workspace-source-sync', %s, %s, %s, 2, %s,
                    'dbos-executor-status', 'dbos-app-status')
            """,
            (
                workflow_id,
                created_at_ms,
                started_at_ms + 100,
                started_at_ms,
                completed_at_ms,
            ),
        )

    with dbos_workflow_trace(
        dsn=clean_runtime,
        deployment_name="okg-workspace",
        workflow_name="okg_source_sync",
        workflow_id=workflow_id,
        queue_name="okg-okg-workspace-source-sync",
        started_at=root_started_at,
    ) as identity:
        assert identity is not None
        child_a = child_span_identity()
        child_b = child_span_identity()
        child_started_at = root_started_at + timedelta(milliseconds=10)
        child_ended_at = root_started_at + timedelta(milliseconds=50)
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            for span_name, child in (
                ("chronos.checkpoint.parallel-a", child_a),
                ("sql.query.parallel-b", child_b),
            ):
                record_span(
                    conn,
                    TelemetrySpan(
                        trace_id=child.trace_id,
                        span_id=child.span_id,
                        parent_span_id=child.parent_span_id,
                        span_name=span_name,
                        span_kind="internal",
                        status_code="ok",
                        deployment_name="okg-workspace",
                        workflow_id=workflow_id,
                        started_at=child_started_at,
                        ended_at=child_ended_at,
                        attributes={"component": span_name.split(".", 1)[0]},
                        root_name="dbos.workflow.okg_source_sync",
                        root_kind="dbos_workflow",
                    ),
                )

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT trace_id, span_id, status_code, started_at, ended_at,
                   duration_ms, attributes
              FROM okg.telemetry_spans
             WHERE deployment_name = 'okg-workspace'
               AND span_name = 'dbos.workflow.okg_source_sync'
               AND workflow_id = %s
             ORDER BY event_time DESC
             LIMIT 1
            """,
            (workflow_id,),
        )
        root = cur.fetchone()
        assert root is not None
        (
            trace_id,
            root_span_id,
            status_code,
            stored_started_at,
            stored_ended_at,
            duration_ms,
            attrs,
        ) = root
        assert status_code == "ok"
        assert abs((stored_started_at - expected_created_at).total_seconds()) < 0.001
        assert abs((stored_ended_at - expected_completed_at).total_seconds()) < 0.001
        assert duration_ms >= 4_250
        assert attrs["dbos.workflow.status"] == "SUCCESS"
        assert attrs["dbos.workflow.recovery_attempts"] == 2
        assert attrs["dbos.workflow.queue_wait_ms"] == 4000
        assert attrs["dbos.executor.id"] == "dbos-executor-status"
        assert attrs["dbos.application.id"] == "dbos-app-status"
        # DBOS supplied queue wait and retries, so their markers are gone
        # (#2320). This workflow runs without a trace handoff, so the
        # admission-wait marker from change issue-877-source-sync-spans stays.
        assert attrs["unavailable"] == {
            "source_sync.admission_lock.wait": (
                "source_sync_trace_handoff_not_available_legacy_input"
            ),
        }

        cur.execute(
            """
            SELECT started_at, duration_ms, workflow_id
              FROM okg.telemetry_traces
             WHERE deployment_name = 'okg-workspace'
               AND trace_id = %s
            """,
            (trace_id,),
        )
        trace_row = cur.fetchone()
        assert trace_row is not None
        trace_started_at, trace_duration_ms, trace_workflow_id = trace_row
        assert abs((trace_started_at - expected_created_at).total_seconds()) < 0.001
        assert trace_duration_ms >= 4_250
        assert trace_workflow_id == workflow_id

        cur.execute(
            """
            SELECT span_name, parent_span_id, workflow_id
              FROM okg.telemetry_spans
             WHERE deployment_name = 'okg-workspace'
               AND trace_id = %s
               AND span_name IN (
                 'chronos.checkpoint.parallel-a',
                 'sql.query.parallel-b'
               )
             ORDER BY span_name
            """,
            (trace_id,),
        )
        rows = cur.fetchall()
        assert rows == [
            ("chronos.checkpoint.parallel-a", root_span_id, workflow_id),
            ("sql.query.parallel-b", root_span_id, workflow_id),
        ]

        coverage = telemetry_coverage(
            conn,
            deployment="okg-workspace",
            start_at=expected_created_at - timedelta(milliseconds=1),
            end_at=stored_ended_at + timedelta(milliseconds=1),
            scope="workflow",
            workflow_id=workflow_id,
        )

    # The queue-wait gap is still proven, through coverage's own gap list
    # below: `telemetry_timeline` rendered the same gap as a synthetic span
    # and went with the Performance view's route.
    root_coverage = next(
        row for row in coverage["records"]
        if row["span_id"] == root_span_id
    )
    assert [gap["cause"] for gap in root_coverage["gaps"]] == [
        "queue_wait",
        "before_first_child",
        "after_last_child",
    ]
    assert [gap["duration_ms"] for gap in root_coverage["gaps"]] == [
        4_000.0,
        10.0,
        200.0,
    ]
    assert root_coverage["direct_child_covered_ms"] == 40.0
    assert root_coverage["work_time_ms"] == 80.0
    assert root_coverage["parallel_overflow_ms"] == 40.0
