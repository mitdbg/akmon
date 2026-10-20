"""Live DBOS/source-sync checks for the okg-workspace source health view."""
from __future__ import annotations

import importlib
import time
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

from tests.substrate._deployment_views import isolated_deployment_views


pytestmark = pytest.mark.postgres

REPO = Path(__file__).resolve().parents[2]
DEPLOYMENT_DIR = REPO / "deployments" / "okg-workspace"


@pytest.fixture(autouse=True, scope="module")
def _deployment_on_sys_path() -> Iterator[None]:
    # A bare `if p not in sys.path: insert` lost to a `views` package cached
    # by an earlier test on the same xdist worker (nightly shard 0).
    with isolated_deployment_views(DEPLOYMENT_DIR):
        yield


def test_source_health_sql_joins_decoded_active_workflow(
    clean_runtime: str,
) -> None:
    from dbos._serialization import (
        DBOSDefaultSerializer,
        WorkflowSerializationFormat,
        serialize_args,
    )

    from okg.substrate.source_sync import SourceSyncEvent, SourceSyncJob
    from okg.substrate.source_sync_enqueue import _source_sync_workflow_args

    mod = importlib.import_module("views.source_performance_health")
    job = SourceSyncJob(
        deployment="okg-workspace",
        sources=("pact",),
        run_mode="scope_complete",
        events=(
            SourceSyncEvent(
                deployment="okg-workspace",
                source_name="pact",
                trigger_kind="poll",
                scope_key="pact/changes/prove-source-incremental-sync",
            ),
        ),
        dependent_fanout="closure",
    )
    inputs, serialization = serialize_args(
        _source_sync_workflow_args(job),
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
              started_at_epoch_ms bigint,
              inputs text,
              serialization text
            )
            """
        )
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid, status, name, queue_name, priority,
              deduplication_id, created_at, updated_at, inputs, serialization
            )
            VALUES (
              'wf-pact-active', 'ENQUEUED', 'okg_source_sync',
              'okg-okg-workspace-source-sync', 20,
              'sync:okg-workspace:poll:scope_complete:pact:closure:1',
              %s, %s, %s, %s
            )
            """,
            (now_ms - 5000, now_ms - 5000, inputs, serialization),
        )

    with psycopg.connect(clean_runtime) as conn:
        mod.build_corpus(conn, generation_id=1)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {mod.PERFORMANCE_EVIDENCE_TEMP_TABLE} (
                  source_name,
                  source_runner_ms,
                  admission_publish_ms,
                  publish_total_ms,
                  event_to_generation_ms,
                  query_visibility_ms,
                  throughput_records_per_second,
                  input_count,
                  output_count,
                  nodes_appended,
                  edges_appended,
                  observed_delta_nodes_added,
                  observed_delta_edges_added,
                  latest_measurement_at,
                  latest_proof_artifact,
                  measured_generation_id,
                  any_failed,
                  any_passed,
                  measurement_count,
                  profile_stage_count
                )
                VALUES (
                  'pact',
                  999.9,
                  NULL,
                  NULL,
                  NULL,
                  NULL,
                  NULL,
                  NULL,
                  NULL,
                  NULL,
                  NULL,
                  NULL,
                  NULL,
                  now() - interval '1 day',
                  'older-benchmark.json',
                  'old-generation',
                  FALSE,
                  TRUE,
                  1,
                  0
                )
                """
            )
            cur.execute(
                mod.SOURCE_HEALTH_ROWS_SQL,
                {
                    "deployment_name": "okg-workspace",
                    "target_generation_id": 1,
                    "view_id": mod.VIEW_ID,
                },
            )
            columns = [desc.name for desc in cur.description]
            rows = [dict(zip(columns, row, strict=True)) for row in cur]
    pact = next(row for row in rows if row["source_name"] == "pact")
    assert pact["active_workflow_id"] == "wf-pact-active"
    assert pact["workflow_status"] == "ENQUEUED"
    assert pact["run_mode"] == "scope_complete"
    assert pact["event_count"] == 1
    assert pact["active_work_stale"] is False
    assert pact["active_work_candidate_count"] == 1
    assert pact["active_work_selection_reason"] == "single_active_workflow"
    assert pact["workflow_lifetime_ms"] >= 0
    assert pact["queue_dwell_ms"] >= 0
    # Present-arm tripwire for the staged queue aggregate (pact
    # guard-source-health-dbos-read): a mis-scoped queue name or a swapped
    # FILTER predicate would zero these without touching the fields above.
    assert pact["dbos_telemetry_available"] is True
    assert pact["global_active_workflow_count"] == 1


def test_source_health_sql_renders_disclosed_without_dbos(
    clean_runtime: str,
) -> None:
    """A DBOS-less database (worker never started) renders the view with the
    queue-telemetry gap DISCLOSED — dbos_telemetry_available=false and NULL
    (never zero) queue counts — instead of crashing the materializer with
    UndefinedTable on dbos.workflow_status
    (pact guard-source-health-dbos-read)."""
    mod = importlib.import_module("views.source_performance_health")

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")

    with psycopg.connect(clean_runtime) as conn:
        mod.build_corpus(conn, generation_id=1)
        with conn.cursor() as cur:
            cur.execute(
                mod.SOURCE_HEALTH_ROWS_SQL,
                {
                    "deployment_name": "okg-workspace",
                    "target_generation_id": 1,
                    "view_id": mod.VIEW_ID,
                },
            )
            columns = [desc.name for desc in cur.description]
            rows = [dict(zip(columns, row, strict=True)) for row in cur]

    assert rows, "the registry rows must render even without DBOS telemetry"
    for row in rows:
        assert row["dbos_telemetry_available"] is False
        assert row["global_active_workflow_count"] is None  # never a fabricated 0
        assert row["workflow_status"] == "none_observed"


def test_source_health_sql_uses_record_set_summary_counts(
    clean_runtime: str,
) -> None:
    mod = importlib.import_module("views.source_performance_health")
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_watermarks (
              source,
              last_applied_rev,
              last_reconcile_at
            )
            VALUES (
              'okg-workspace.pact',
              '{
                "record_set_summary": {
                  "schema": "okg.source_runner.record_set_summary.v1",
                  "source_name": "okg-workspace.pact",
                  "record_count": 2,
                  "node_count": 3,
                  "edge_count": 4,
                  "chronos_graph_verified": true
                }
              }'::jsonb,
              now()
            )
            ON CONFLICT (source) DO UPDATE
              SET last_applied_rev = EXCLUDED.last_applied_rev,
                  last_reconcile_at = EXCLUDED.last_reconcile_at
            """
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
              'okg-workspace',
              'okg-workspace.pact',
              'sync.last_run',
              '{
                "schema": "okg.source_runner.last_run.v1",
                "source_name": "pact",
                "mode": "scope_complete",
                "source_runner_ms": 123.4,
                "nodes_appended": 0,
                "edges_appended": 0,
                "nodes_retracted": 0,
                "edges_retracted": 0,
                "completed_scope": true
              }',
              now()
            )
            ON CONFLICT (deployment, source_name, key) DO UPDATE
              SET value = EXCLUDED.value,
                  updated_at = EXCLUDED.updated_at
            """
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
            VALUES
              (
                'okg-workspace',
                'okg-workspace.pact',
                'sync.run.pact-nonzero',
                jsonb_build_object(
                  'schema', 'okg.source_runner.last_run.v1',
                  'run_id', 'pact-nonzero',
                  'source_name', 'pact',
                  'mode', 'scope_incomplete',
                  'finished_at', (now() - interval '20 minutes')::text,
                  'source_runner_ms', 111.1,
                  'nodes_appended', 5,
                  'edges_appended', 6,
                  'nodes_retracted', 1,
                  'edges_retracted', 2,
                  'completed_scope', false
                )::text,
                now() - interval '20 minutes'
              ),
              (
                'okg-workspace',
                'okg-workspace.pact',
                'sync.run.pact-noop',
                jsonb_build_object(
                  'schema', 'okg.source_runner.last_run.v1',
                  'run_id', 'pact-noop',
                  'source_name', 'pact',
                  'mode', 'scope_incomplete',
                  'finished_at', now()::text,
                  'source_runner_ms', 123.4,
                  'nodes_appended', 0,
                  'edges_appended', 0,
                  'nodes_retracted', 0,
                  'edges_retracted', 0,
                  'completed_scope', false
                )::text,
                now()
              )
            ON CONFLICT (deployment, source_name, key) DO UPDATE
              SET value = EXCLUDED.value,
                  updated_at = EXCLUDED.updated_at
            """
        )

    with psycopg.connect(clean_runtime) as conn:
        mod.build_corpus(conn, generation_id=1)
        with conn.cursor() as cur:
            cur.execute(
                mod.SOURCE_HEALTH_ROWS_SQL,
                {
                    "deployment_name": "okg-workspace",
                    "target_generation_id": 1,
                    "view_id": mod.VIEW_ID,
                },
            )
            columns = [desc.name for desc in cur.description]
            rows = [dict(zip(columns, row, strict=True)) for row in cur]
    pact = next(row for row in rows if row["source_name"] == "pact")
    assert pact["node_count"] == 3
    assert pact["edge_count"] == 4
    assert pact["fact_count"] == 7
    assert pact["live_count"] == 7
    assert pact["graph_yield_ratio"] == 1.0
    assert pact["last_graph_observed_at"] is not None
    # Live timing is status-like, but delta windows come from run history.
    assert pact["source_runner_ms"] == pytest.approx(123.4)
    assert pact["nodes_added_1h"] == 5
    assert pact["edges_added_1h"] == 6
    assert pact["nodes_added_24h"] == 5
    assert pact["edges_added_24h"] == 6
    assert pact["live_row_delta"] == 11
