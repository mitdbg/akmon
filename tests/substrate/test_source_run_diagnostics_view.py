from __future__ import annotations

import json
from datetime import datetime, timezone

import psycopg
import pytest


pytestmark = pytest.mark.postgres


def test_source_run_diagnostics_uses_stage_timing_progress(
    clean_runtime: str,
) -> None:
    observed_at = datetime(2026, 6, 9, 13, 0, 0, tzinfo=timezone.utc)
    payload = {
        "schema": "okg.source_sync.stage_timings.last_run.v1",
        "deployment": "svc",
        "source_name": "svc.git-files",
        "registry_source_name": "git-files",
        "run_id": "run-git-files-1",
        "status": "OK",
        "timing_status": "available",
        "run_mode": "scope_incomplete",
        "input_count": 2,
        "output_count": 7,
        "nodes_appended": 3,
        "edges_appended": 4,
        "nodes_retracted": 0,
        "edges_retracted": 0,
        "source_stats": {"changed_paths": 2},
        "timings_ms": {
            "total": 150.0,
            "source_runner": 120.0,
            "source_runner.adapter_run": 90.0,
            "source_runner.flush.dedupe_persisted": 25.0,
            "source_runner.flush.chronos_apply": 5.0,
        },
    }

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES
              ('svc', 'svc.git-files', 'sync.stage_timings.run.run-git-files-1',
               %s, %s)
            """,
            (json.dumps(payload), observed_at),
        )
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, cutover_watermark, last_applied_rev,
               last_published_rev, projection_cursor, last_reconcile_at)
            VALUES
              ('svc.watermark-only', '{}'::jsonb, '{}'::jsonb,
               '{}'::jsonb, 0, %s)
            """,
            (observed_at,),
        )

        cur.execute(
            """
            SELECT deployment, source_name, registry_source_name, run_id,
                   status, run_mode, timing_status, input_count,
                   output_count, nodes_appended, edges_appended,
                   sync_scope,
                   total_ms, source_runner_ms, adapter_run_ms,
                   dedupe_persisted_ms, slowest_phase, slowest_phase_ms,
                   timings_ms, source_stats
              FROM okg.v_source_run_diagnostics
             WHERE deployment = 'svc'
            """
        )
        row = cur.fetchone()

        cur.execute(
            """
            SELECT source_name, run_id, nodes_appended, edges_appended
              FROM okg.v_source_runs
             WHERE source_name LIKE 'svc.%'
             ORDER BY source_name
            """
        )
        compat_rows = cur.fetchall()

    assert row is not None
    assert row[0] == "svc"
    assert row[1] == "svc.git-files"
    assert row[2] == "git-files"
    assert row[3] == "run-git-files-1"
    assert row[4] == "OK"
    assert row[5] == "scope_incomplete"
    assert row[6] == "available"
    assert row[7] == 2
    assert row[8] == 7
    assert row[9] == 3
    assert row[10] == 4
    assert row[11] == {}
    assert row[12] == pytest.approx(150.0)
    assert row[13] == pytest.approx(120.0)
    assert row[14] == pytest.approx(90.0)
    assert row[15] == pytest.approx(25.0)
    # `source_runner` is the whole-call rollup, so it is >= every phase inside
    # it and can never discriminate. The bottleneck must name a terminal phase
    # (issue #1325 item 4).
    assert row[16] == "source_runner.adapter_run"
    assert row[17] == pytest.approx(90.0)
    assert row[18]["source_runner.adapter_run"] == 90.0
    assert row[19]["changed_paths"] == 2

    # `svc.watermark-only` has no per-run evidence, so it projects NO row —
    # not a run-shaped placeholder of NULLs (PACT
    # fix-v-source-runs-null-placeholder).
    assert compat_rows == [
        ("svc.git-files", "run-git-files-1", 3, 4),
    ]


def test_source_run_diagnostics_marks_unavailable_payloads(
    clean_runtime: str,
) -> None:
    payload = {
        "schema": "okg.source_sync.stage_timings.last_run.v1",
        "deployment": "svc",
        "source_name": "svc.doc-corpus",
        "registry_source_name": "doc-corpus",
        "run_id": "run-doc-1",
        "status": "SKIPPED",
        "timing_status": "unavailable",
        "input_count": 0,
        "output_count": 0,
        "timings_ms": {},
    }

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value)
            VALUES
              ('svc', 'svc.doc-corpus', 'sync.stage_timings.run.run-doc-1',
               %s)
            """,
            (json.dumps(payload),),
        )
        cur.execute(
            """
            SELECT timing_status, timing_unavailable_reason, total_ms,
                   slowest_phase
              FROM okg.v_source_run_diagnostics
             WHERE deployment = 'svc'
               AND run_id = 'run-doc-1'
            """
        )
        row = cur.fetchone()

    assert row == (
        "unavailable",
        "stage_timing_payload_unavailable",
        None,
        None,
    )


def _insert_stage_timing_run(
    cur,
    *,
    source: str,
    run_id: str,
    timings: dict[str, float],
) -> None:
    payload = {
        "schema": "okg.source_sync.stage_timings.last_run.v1",
        "deployment": "svc",
        "source_name": f"svc.{source}",
        "registry_source_name": source,
        "run_id": run_id,
        "status": "OK",
        "timing_status": "available",
        "timings_ms": timings,
    }
    cur.execute(
        """
        INSERT INTO okg.source_progress
          (deployment, source_name, key, value)
        VALUES (%s, %s, %s, %s)
        """,
        (
            "svc",
            f"svc.{source}",
            f"sync.stage_timings.run.{run_id}",
            json.dumps(payload),
        ),
    )


def test_slowest_phase_discriminates_between_sources(
    clean_runtime: str,
) -> None:
    """Issue #1325 item 4: the bottleneck column must vary with the bottleneck.

    Every production row reported ``source_runner`` -- a 0.3 s no-op and a
    937 s bulk load alike -- because the payload carries a whole-call rollup
    under that key plus every phase beneath it. These four runs have four
    different real bottlenecks and one carries no breakdown at all.
    """
    # Shapes taken from the run records the issue measured.
    runs = {
        # adapter-bound: a fixed pre-emission scan dominates.
        "semgrep-data-flow": {
            "total": 16003.0,
            "source_runner": 15980.0,
            "source_runner.adapter_run": 15120.0,
            "source_runner.stream_walk": 700.0,
            "source_runner.flush_chunks": 690.0,
            "source_runner.flush.chronos_apply": 410.0,
            "source_runner.flush.dedupe_persisted": 180.0,
        },
        # chronos-bound: 14.5 s of a 21 s run to write 9 nodes.
        "instrumentation-log": {
            "total": 21004.0,
            "source_runner": 20950.0,
            "source_runner.adapter_run": 5200.0,
            "source_runner.stream_walk": 15300.0,
            "source_runner.flush_chunks": 15100.0,
            "source_runner.flush.chronos_apply": 14500.0,
            "source_runner.flush.dedupe_persisted": 320.0,
            # Backend sub-measurements are NOT a partition of chronos_apply;
            # ranking one of them against a sibling phase compares unlike
            # things, so depth-4 keys stay out of the ranking.
            "source_runner.flush.chronos_apply.node_copy": 9000.0,
            "source_runner.flush.chronos_apply.edge_copy": 4100.0,
        },
        # dedupe-bound.
        "junit-xml": {
            "total": 20900.0,
            "source_runner": 20880.0,
            "source_runner.adapter_run": 3100.0,
            "source_runner.flush.dedupe_persisted": 12400.0,
            "source_runner.flush.chronos_apply": 4200.0,
        },
        # No breakdown at all: rollups only. Absence must read as absence.
        "rollup-only": {
            "total": 300.0,
            "source_runner": 280.0,
            "source_runner.total": 280.0,
        },
    }

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        for source, timings in runs.items():
            _insert_stage_timing_run(
                cur,
                source=source,
                run_id=f"run-{source}",
                timings=timings,
            )
        cur.execute(
            """
            SELECT registry_source_name, slowest_phase, slowest_phase_ms
              FROM okg.v_source_run_diagnostics
             WHERE deployment = 'svc'
             ORDER BY registry_source_name
            """
        )
        rows = cur.fetchall()

    assert rows == [
        ("instrumentation-log", "source_runner.flush.chronos_apply", 14500.0),
        ("junit-xml", "source_runner.flush.dedupe_persisted", 12400.0),
        ("rollup-only", None, None),
        ("semgrep-data-flow", "source_runner.adapter_run", 15120.0),
    ]

    # The failure this replaces: one value for every row.
    reported = {row[1] for row in rows}
    assert len(reported) == len(rows)
