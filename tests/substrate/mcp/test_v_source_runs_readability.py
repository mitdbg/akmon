from __future__ import annotations

import json
from datetime import datetime, timezone

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.mcp import open_session
from okg.substrate.mcp.run_sql import run_sql
from okg.substrate.mcp.sql_safety import RunSqlInvalid
from tests.substrate._db_helpers import (
    ensure_published_generation,
    seed_catalog_version,
)


pytestmark = pytest.mark.postgres


def _seed_source_run(conn: psycopg.Connection) -> None:
    observed_at = datetime(2026, 6, 11, 2, 0, 0, tzinfo=timezone.utc)
    payload = {
        "schema": "okg.source_sync.stage_timings.last_run.v1",
        "deployment": "default",
        "source_name": "default.doc-corpus",
        "registry_source_name": "doc-corpus",
        "run_id": "run-doc-corpus-1",
        "status": "OK",
        "timing_status": "available",
        "run_mode": "scope_complete",
        "input_count": 1,
        "output_count": 2,
        "nodes_appended": 1,
        "edges_appended": 1,
        "nodes_retracted": 0,
        "edges_retracted": 0,
        "sync_scope": {"paths": ["docs/source-sync.md"]},
        "source_stats": {"files_seen": 1, "files_changed": 1},
        "timings_ms": {
            "total": 12.5,
            "source_runner": 10.0,
            "source_runner.adapter_run": 8.0,
        },
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES
              ('default', 'default.doc-corpus',
               'sync.stage_timings.run.run-doc-corpus-1', %s, %s)
            """,
            (json.dumps(payload), observed_at),
        )


def test_run_sql_reads_source_run_diagnostics_view(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(
            conn,
            generation_id=11,
            deployment_name="default",
        )
        _seed_source_run(conn)

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        out = run_sql(
            conn,
            session,
            """
            SELECT registry_source_name, run_id, total_ms, source_runner_ms,
                   adapter_run_ms, slowest_phase, source_stats, sync_scope
              FROM okg.v_source_run_diagnostics
             WHERE registry_source_name = 'doc-corpus'
             ORDER BY observed_at DESC
             LIMIT 1
            """,
        )

    assert out["query_plan_summary"] != "Chronos checkout"
    assert out["columns"] == [
        "registry_source_name",
        "run_id",
        "total_ms",
        "source_runner_ms",
        "adapter_run_ms",
        "slowest_phase",
        "source_stats",
        "sync_scope",
    ]
    assert out["rows"] == [[
        "doc-corpus",
        "run-doc-corpus-1",
        12.5,
        10.0,
        8.0,
        # `source_runner` is the whole-call rollup around every
        # `source_runner.<phase>`, so it can never discriminate. The view
        # reports the slowest TERMINAL phase (issue #1325 item 4).
        "source_runner.adapter_run",
        {"files_seen": 1, "files_changed": 1},
        {"paths": ["docs/source-sync.md"]},
    ]]


def test_run_sql_reads_compat_source_runs_view(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(
            conn,
            generation_id=12,
            deployment_name="default",
        )
        _seed_source_run(conn)

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        out = run_sql(
            conn,
            session,
            """
            SELECT source_name, run_id, nodes_appended, edges_appended
              FROM okg.v_source_runs
             WHERE source_name = 'default.doc-corpus'
             ORDER BY run_id
            """,
        )

    assert out["query_plan_summary"] != "Chronos checkout"
    assert out["columns"] == [
        "source_name",
        "run_id",
        "nodes_appended",
        "edges_appended",
    ]
    assert out["rows"] == [["default.doc-corpus", "run-doc-corpus-1", 1, 1]]


def test_v_source_runs_returns_nothing_for_watermark_only_source(
    clean_runtime: str,
) -> None:
    """A source with only an `okg.source_watermarks` row (no `sync.run.%`
    payload, no stage-timing diagnostic, no fact-log rows) must project ZERO
    rows — "no runs recorded" — never a run-shaped placeholder row whose
    run_id and count columns are all NULL. Pins PACT
    fix-v-source-runs-null-placeholder; scripts/check_view_reads.py cannot
    catch this because the columns are non-NULL in the evidenced arms."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(
            conn,
            generation_id=14,
            deployment_name="default",
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.source_watermarks (source, last_reconcile_at)
                VALUES ('default.watermark-only', %s)
                """,
                (datetime(2026, 6, 11, 3, 0, 0, tzinfo=timezone.utc),),
            )
            # A malformed sync.run payload with no run_id is "no evidence",
            # not a run — it must not project a NULL-run_id row either.
            cur.execute(
                """
                INSERT INTO okg.source_progress
                  (deployment, source_name, key, value)
                VALUES
                  ('default', 'default.orphan-payload', 'sync.run.orphan',
                   %s)
                """,
                (json.dumps({"schema": "okg.source_runner.last_run.v1"}),),
            )

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        watermark_only = run_sql(
            conn,
            session,
            """
            SELECT source_name, run_id, nodes_appended
              FROM okg.v_source_runs
             WHERE source_name = 'default.watermark-only'
            """,
        )
        null_run_ids = run_sql(
            conn,
            session,
            "SELECT source_name FROM okg.v_source_runs WHERE run_id IS NULL",
        )

    assert watermark_only["rows"] == []
    assert null_run_ids["rows"] == []


def test_mcp_query_default_session_preserves_compat_pin_for_operational_view(
    clean_runtime: str,
) -> None:
    pytest.importorskip("mcp")
    from okg.substrate.mcp.server import build_server

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(
            conn,
            generation_id=13,
            deployment_name="default",
        )
        _seed_source_run(conn)

    mcp = build_server(dsn=clean_runtime)
    try:
        out = mcp._tool_manager._tools["query"].fn(
            sql=(
                "SELECT registry_source_name "
                "FROM okg.v_source_run_diagnostics "
                "LIMIT 1"
            ),
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert out["query_plan_summary"] != "Chronos checkout"
    assert out["pinned_generation_id"] == 13
    assert out["columns"] == ["registry_source_name"]
    assert out["rows"] == [["doc-corpus"]]


def test_operational_run_sql_refuses_string_chronos_pin_without_silent_zero_fallback(
    clean_runtime: str,
) -> None:
    generation_id = "okg:default:branch:default:gen:no-compat"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn).checkpoint_generation(
            generation_id,
            deployment_name="default",
            branch="default",
            catalog_version_id=cv,
        )
        _seed_source_run(conn)

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        assert session.generation_id == generation_id
        assert session.compat_generation_id is None
        with pytest.raises(RunSqlInvalid, match="legacy integer pin"):
            run_sql(
                conn,
                session,
                """
                SELECT registry_source_name
                  FROM okg.v_source_run_diagnostics
                 LIMIT 1
                """,
            )
