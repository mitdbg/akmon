from __future__ import annotations

import time
from datetime import datetime
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from okg.substrate.db.management import (
    REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID,
    apply_remove_job_runtime_subsystem,
    database_health_findings,
    dbos_backlog_gc,
    inspect_database,
    migration_status,
)
from okg.substrate.db.migrate import migrate
from okg.substrate.errors import SubstrateError


pytestmark = pytest.mark.postgres


def _reset_dbos_schema(cur) -> None:
    cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
    cur.execute("CREATE SCHEMA dbos")


def _seed_obsolete_job_runtime_schema(
    dsn: str,
    *,
    status: str = "succeeded",
    legacy_status: str = "done",
) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM okg.schema_migrations WHERE migration_id = %s",
            (REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID,),
        )
        cur.execute("CREATE TABLE okg.job_definitions (job_name text PRIMARY KEY)")
        cur.execute("CREATE TABLE okg.job_dependencies (dependency_id bigint)")
        for table in ("job_runs", "job_chunks", "job_items"):
            cur.execute(
                sql.SQL("CREATE TABLE okg.{} (id bigint, status text)").format(
                    sql.Identifier(table)
                )
            )
        cur.execute("CREATE TABLE okg.job_item_frontiers (frontier_id bigint)")
        cur.execute("CREATE TABLE okg.job_external_calls (call_id bigint)")
        cur.execute("CREATE TABLE okg.job_runtime_ledger_retirement_marker (id int)")
        cur.execute("CREATE TABLE okg.jobs (id bigint, status text)")
        cur.execute("CREATE TABLE okg.publisher_leases (id bigint)")
        cur.execute("CREATE TABLE okg.legacy_runtime_retirement_marker (id int)")
        cur.execute(
            "CREATE INDEX node_facts_workload_source_fact_idx "
            "ON okg.node_facts (fact_id)"
        )
        cur.execute(
            "CREATE INDEX edge_facts_workload_source_fact_idx "
            "ON okg.edge_facts (fact_id)"
        )
        cur.execute(
            "CREATE INDEX edge_facts_source_pattern_idx "
            "ON okg.edge_facts (source text_pattern_ops)"
        )
        cur.execute(
            "CREATE VIEW okg.v_job_runtime_reconciliation AS SELECT 1 AS row_count"
        )
        cur.execute("CREATE VIEW okg.v_job_queue AS SELECT 1 AS row_count")
        cur.execute(
            "CREATE FUNCTION okg.job_runtime_reconciliation() "
            "RETURNS integer LANGUAGE sql AS 'SELECT 1'"
        )
        cur.execute("INSERT INTO okg.job_runs (id, status) VALUES (1, %s)", (status,))
        cur.execute("INSERT INTO okg.jobs (id, status) VALUES (1, %s)", (legacy_status,))


def _seed_empty_dbos_workflow_status(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dbos")
        cur.execute(
            "CREATE TABLE dbos.workflow_status (name text, status text)"
        )


def test_database_status_reports_runtime_authority_and_dbos(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            _reset_dbos_schema(cur)
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                  workflow_uuid text PRIMARY KEY,
                  status text NOT NULL,
                  created_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                INSERT INTO dbos.workflow_status (
                  workflow_uuid, status, created_at
                )
                VALUES (
                  'wf-db-health-test', 'PENDING',
                  now() - interval '1 hour'
                )
                ON CONFLICT (workflow_uuid) DO UPDATE SET
                  status = EXCLUDED.status,
                  created_at = EXCLUDED.created_at
                """
            )

    try:
        status = inspect_database(clean_runtime)
        dbos_rows = status["dbos_workflows"]["by_status"]
        assert status["obsolete_job_runtime"] == {
            "cleanup_required": False,
            "objects_present": [],
            "cron_schedules_present": [],
        }
        assert status["execution_authority"] == "dbos.workflow_status"
        assert status["source_progress_authority"] == (
            "okg.source_sync_progress_and_watermarks"
        )
        assert any(
            row["status"] == "PENDING" and row["workflow_count"] >= 1
            for row in dbos_rows
        )
        pending = next(row for row in dbos_rows if row["status"] == "PENDING")
        assert pending["observed_at_column"] == "created_at"
        assert isinstance(pending["oldest_observed_at"], str)
        assert pending["oldest_observed_age_seconds"] >= 0
        assert status["dbos_workflows"]["workflow_status_total_bytes"] > 0
        queue_rows = status["runtime_queue_health"]["rows"]
        assert queue_rows[0]["queue_name"] == "default"
        assert queue_rows[0]["status"] == "stale"
        assert queue_rows[0]["active_count"] == 1
        assert queue_rows[0]["stale_active_work_present"] is True
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


def test_database_health_classifies_dbos_backlog_without_obsolete_runtime(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            _reset_dbos_schema(cur)
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                  workflow_uuid text PRIMARY KEY,
                  status text NOT NULL,
                  created_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                INSERT INTO dbos.workflow_status (
                  workflow_uuid, status, created_at
                )
                VALUES (
                  'wf-db-health-finding-test', 'ENQUEUED',
                  now() - interval '1 hour'
                )
                """
            )

    try:
        findings = database_health_findings(inspect_database(clean_runtime))
        subsystems = {finding["subsystem"] for finding in findings}
        assert "obsolete_job_runtime" not in subsystems
        assert "dbos" in subsystems
        assert "runtime_queue" in subsystems
        dbos_finding = next(
            finding for finding in findings if finding["subsystem"] == "dbos"
        )
        assert dbos_finding["observed"]["oldest_observed_age_seconds"] >= 0
        assert dbos_finding["observed"]["workflow_status_total_bytes"] > 0
        queue_finding = next(
            finding for finding in findings
            if finding["subsystem"] == "runtime_queue"
        )
        assert queue_finding["observed"]["status"] == "stale"
        assert queue_finding["observed"]["active_count"] == 1
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


def test_database_health_reports_runtime_queue_view_errors() -> None:
    findings = database_health_findings({
        "database": "okg_test",
        "generation": {"detected_deployments": ["bootstrap"]},
        "obsolete_job_runtime": {"cleanup_required": False},
        "dbos_workflows": {"present": True, "by_status": []},
        "runtime_queue_health": {
            "present": True,
            "rows": [],
            "status_counts": {},
            "error": "UndefinedColumn",
            "message": "column workflow_status.status does not exist",
        },
        "migration_status": {"ledger_exists": True},
        "timescale": {"hypertables": []},
    })

    finding = next(
        finding for finding in findings
        if finding["code"] == "runtime_queue_health_unreadable"
    )
    assert finding["severity"] == "warning"
    assert finding["subsystem"] == "runtime_queue"
    assert finding["observed"]["error"] == "UndefinedColumn"


def test_database_health_requires_obsolete_runtime_cleanup() -> None:
    findings = database_health_findings({
        "database": "okg_test",
        "generation": {"detected_deployments": ["bootstrap"]},
        "obsolete_job_runtime": {
            "cleanup_required": True,
            "objects_present": ["okg.jobs", "okg.job_runs"],
            "cron_schedules_present": [],
        },
        "dbos_workflows": {"present": False, "by_status": []},
        "migration_status": {"ledger_exists": True},
        "timescale": {"hypertables": []},
    })

    obsolete_runtime = next(
        finding for finding in findings
        if finding["code"] == "obsolete_job_runtime_cleanup_required"
    )
    assert obsolete_runtime["severity"] == "error"
    assert obsolete_runtime["subsystem"] == "obsolete_job_runtime"


def test_database_status_normalizes_dbos_epoch_millisecond_columns(
    clean_runtime: str,
) -> None:
    oldest_created_ms = int((time.time() - 3600) * 1000)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            _reset_dbos_schema(cur)
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                  workflow_uuid text PRIMARY KEY,
                  status text NOT NULL,
                  created_at bigint NOT NULL
                )
                """
            )
            cur.execute(
                """
                INSERT INTO dbos.workflow_status (
                  workflow_uuid, status, created_at
                )
                VALUES ('wf-db-health-epoch-test', 'ENQUEUED', %s)
                """,
                (oldest_created_ms,),
            )

    try:
        status = inspect_database(clean_runtime)
        rows = status["dbos_workflows"]["by_status"]
        enqueued = next(row for row in rows if row["status"] == "ENQUEUED")
        observed = datetime.fromisoformat(enqueued["oldest_observed_at"])
        assert int(observed.timestamp() * 1000) == pytest.approx(
            oldest_created_ms,
            abs=1000,
        )
        assert enqueued["oldest_observed_age_seconds"] >= 3000
        assert enqueued["observed_at_column"] == "created_at"
        assert status["dbos_workflows"]["workflow_status_total_bytes"] > 0
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


def test_dbos_backlog_gc_dry_run_and_confirm(clean_runtime: str) -> None:
    old_ms = int((time.time() - 172800) * 1000)
    fresh_ms = int(time.time() * 1000)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            _reset_dbos_schema(cur)
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
            cur.execute(
                """
                INSERT INTO dbos.workflow_status (
                  workflow_uuid, status, name, queue_name,
                  created_at, updated_at
                )
                VALUES
                  (
                    'wf-stale-pending', 'PENDING', 'okg_source_sync',
                    'okg-okg-workspace', %s, %s
                  ),
                  (
                    'wf-stale-enqueued', 'ENQUEUED', 'okg_publisher_cycle',
                    'okg-okg-workspace', %s, %s
                  ),
                  (
                    'wf-fresh-enqueued', 'ENQUEUED', 'okg_publisher_cycle',
                    'okg-okg-workspace', %s, %s
                  ),
                  (
                    'wf-stale-success', 'SUCCESS', 'okg_publisher_cycle',
                    'okg-okg-workspace', %s, %s
                  )
                """,
                (
                    old_ms,
                    old_ms,
                    old_ms,
                    old_ms,
                    fresh_ms,
                    fresh_ms,
                    old_ms,
                    old_ms,
                ),
            )

    try:
        dry_run = dbos_backlog_gc(
            clean_runtime,
            stale_after_seconds=86400,
            confirm=False,
        )
        assert dry_run["dry_run"] is True
        assert dry_run["eligible_count"] == 1
        assert dry_run["cancelled_count"] == 0

        confirmed = dbos_backlog_gc(
            clean_runtime,
            stale_after_seconds=86400,
            confirm=True,
        )
        assert confirmed["dry_run"] is False
        assert confirmed["eligible_count"] == 1
        assert confirmed["cancelled_count"] == 1

        with psycopg.connect(clean_runtime) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, count(*)::int
                      FROM dbos.workflow_status
                     GROUP BY status
                     ORDER BY status
                    """
                )
                assert cur.fetchall() == [
                    ("CANCELLED", 1),
                    ("ENQUEUED", 1),
                    ("PENDING", 1),
                    ("SUCCESS", 1),
                ]
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


def test_migration_status_reports_checksum_mismatch(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.schema_migrations
                   SET checksum = 'definitely-not-current'
                 WHERE migration_id = '0000_schema_snapshot'
                """
            )

    report = migration_status(clean_runtime)
    assert report["ledger_exists"] is True
    assert report["checksum_mismatches"]
    assert report["checksum_mismatches"][0]["migration_id"] == (
        "0000_schema_snapshot"
    )


def test_remove_job_runtime_subsystem_records_migration_ledger(
    okg_dsn: str,
) -> None:
    dbname = f"okg_codex_remove_job_runtime_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        _seed_obsolete_job_runtime_schema(dsn)
        _seed_empty_dbos_workflow_status(dsn)

        result = apply_remove_job_runtime_subsystem(dsn, confirm=True)
        postcondition = result["postcondition"]
        assert postcondition["retirement_marker_absent"] is True
        assert postcondition["reconciliation_view_absent"] is True
        assert postcondition["job_queue_view_absent"] is True
        assert postcondition["reconciliation_function_absent"] is True
        assert postcondition["legacy_jobs_absent"] is True
        assert postcondition["publisher_leases_absent"] is True
        assert postcondition["legacy_jobs_dedupe_index_absent"] is True
        assert postcondition["workload_node_source_index_absent"] is True
        assert postcondition["workload_edge_source_index_absent"] is True
        assert postcondition["workload_source_pattern_index_absent"] is True
        assert postcondition["legacy_retirement_marker_absent"] is True
        assert postcondition["job_definitions_absent"] is True
        assert postcondition["job_dependencies_absent"] is True
        assert postcondition["job_runs_absent"] is True
        assert postcondition["job_chunks_absent"] is True
        assert postcondition["job_items_absent"] is True
        assert postcondition["job_item_frontiers_absent"] is True
        assert postcondition["job_external_calls_absent"] is True
        assert postcondition["lock_inventory_present"] is True
        assert postcondition["stale_lock_view_present"] is True
        assert postcondition["job_chunk_inventory_absent"] is True
        assert postcondition["legacy_cron_schedules_absent"] is True

        migrate(dsn)
        status = migration_status(dsn)
        assert status["checksum_mismatches"] == []
        assert {
            row["migration_id"] for row in status["applied"]
        } >= {"0000_schema_snapshot", REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID}

        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      to_regclass('okg.job_runs') IS NULL,
                      to_regclass('okg.job_items') IS NULL,
                      to_regclass('okg.v_job_runtime_reconciliation') IS NULL,
                      to_regclass('okg.job_runtime_ledger_retirement_marker') IS NULL
                    """
                )
                assert cur.fetchone() == (True, True, True, True)
    finally:
        _drop_disposable_database(okg_dsn, dbname)


def test_remove_job_runtime_subsystem_refuses_active_legacy_rows(
    okg_dsn: str,
) -> None:
    dbname = f"okg_codex_active_job_runtime_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        _seed_obsolete_job_runtime_schema(dsn, status="running")
        _seed_empty_dbos_workflow_status(dsn)

        with pytest.raises(Exception, match="refuses active legacy rows"):
            apply_remove_job_runtime_subsystem(dsn, confirm=True)

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.job_runs')")
            assert cur.fetchone()[0] == "okg.job_runs"
    finally:
        _drop_disposable_database(okg_dsn, dbname)


def test_doctor_reports_a_false_destructive_postcondition_as_an_error(
    okg_dsn: str,
) -> None:
    """Reporting drift without failing anything leaves #1054 as invisible.

    The ledger claiming `job_runs_absent` while okg.job_runs holds rows must be
    an ERROR finding on the surface operators actually gate on, not a note in a
    JSON payload nobody reads.
    """
    dbname = f"okg_codex_drift_doctor_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        # The literal okg-workspace shape: 0001 recorded the tables absent,
        # 0002 has not run, and the tables are back and populated.
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM okg.schema_migrations WHERE migration_id = %s",
                (REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID,),
            )
            cur.execute("CREATE TABLE okg.job_runs (id bigint, status text)")
            cur.execute("INSERT INTO okg.job_runs (id, status) VALUES (1, 'retryable')")
            cur.execute(
                """
                INSERT INTO okg.schema_migrations (
                  migration_id, checksum, description, destructive,
                  status, result_metadata
                ) VALUES (
                  '0001_retire_job_runtime_ledger', %s,
                  'retire the OKG job-runtime ledger', true, 'applied',
                  '{"postcondition": {"job_runs_absent": true}}'::jsonb
                )
                """,
                ("7" * 64,),
            )

        findings = database_health_findings(inspect_database(dsn))
        drift = [
            finding
            for finding in findings
            if finding["code"] == "schema_migration_postcondition_drift"
        ]
        assert drift, [finding["code"] for finding in findings]
        assert drift[0]["severity"] == "error"
        observed = next(
            entry
            for entry in drift[0]["observed"]
            if entry.get("flag") == "job_runs_absent"
        )
        assert observed["migration_id"] == "0001_retire_job_runtime_ledger"
        assert observed["row_count"] == 1
    finally:
        _drop_disposable_database(okg_dsn, dbname)


def test_remove_job_runtime_subsystem_refusal_names_the_decision_flag(
    okg_dsn: str,
) -> None:
    """The refusal must be actionable (issue #1054).

    The retired subsystem has no worker left to drain these rows, so a refusal
    that only says "drain them" is a dead end. It has to name the way out.
    """
    dbname = f"okg_codex_refusal_hint_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        _seed_obsolete_job_runtime_schema(dsn, status="retryable")
        _seed_empty_dbos_workflow_status(dsn)

        with pytest.raises(SubstrateError) as excinfo:
            apply_remove_job_runtime_subsystem(dsn, confirm=True)
        fix_hint = str(getattr(excinfo.value, "fix_hint", ""))
        assert "accept_retired_ledger_rows" in fix_hint
        assert "dump" in fix_hint
    finally:
        _drop_disposable_database(okg_dsn, dbname)


@pytest.mark.parametrize(
    ("dump_ref", "match"),
    [
        (None, "pre-drop dump reference"),
        ("   ", "pre-drop dump reference"),
        ("/nonexistent/path/to/nothing.sql", "does not name a readable file"),
    ],
)
def test_remove_job_runtime_subsystem_override_requires_a_usable_dump(
    okg_dsn: str,
    dump_ref: str | None,
    match: str,
) -> None:
    """The override must not destroy rows without a real recorded way back.

    An unchecked reference string is not a way back -- it is the same
    "recorded claim nobody verified" shape as the postcondition in #1054.
    """
    dbname = f"okg_codex_override_nodump_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        _seed_obsolete_job_runtime_schema(dsn, status="retryable")
        _seed_empty_dbos_workflow_status(dsn)

        with pytest.raises(SubstrateError, match=match):
            apply_remove_job_runtime_subsystem(
                dsn,
                confirm=True,
                accept_retired_ledger_rows=True,
                retired_rows_dump_ref=dump_ref,
            )

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.job_runs')")
            assert cur.fetchone()[0] == "okg.job_runs"
    finally:
        _drop_disposable_database(okg_dsn, dbname)


def test_remove_job_runtime_subsystem_override_refuses_an_empty_dump(
    okg_dsn: str,
    tmp_path,
) -> None:
    empty = tmp_path / "empty.sql"
    empty.write_text("")
    dbname = f"okg_codex_override_emptydump_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        _seed_obsolete_job_runtime_schema(dsn, status="retryable")
        _seed_empty_dbos_workflow_status(dsn)
        with pytest.raises(SubstrateError, match="empty file"):
            apply_remove_job_runtime_subsystem(
                dsn,
                confirm=True,
                accept_retired_ledger_rows=True,
                retired_rows_dump_ref=str(empty),
            )
    finally:
        _drop_disposable_database(okg_dsn, dbname)


def test_remove_job_runtime_subsystem_override_records_destroyed_rows(
    okg_dsn: str,
    tmp_path,
) -> None:
    """Taking the decision applies AND records everything it destroyed.

    Not just the active rows that triggered the refusal: 0002 drops whole
    tables, so the untouched rows in `job_definitions` and friends go too. An
    audit record listing only the blocking rows understates the cost.
    """
    dbname = f"okg_codex_override_apply_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    dump = tmp_path / "legacy-job-runtime.sql"
    dump.write_text("-- pretend dump\n")
    try:
        migrate(dsn)
        _seed_obsolete_job_runtime_schema(dsn, status="retryable")
        _seed_empty_dbos_workflow_status(dsn)
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("INSERT INTO okg.job_definitions (job_name) VALUES ('a')")
            cur.execute("INSERT INTO okg.job_item_frontiers (frontier_id) VALUES (7)")

        result = apply_remove_job_runtime_subsystem(
            dsn,
            confirm=True,
            accept_retired_ledger_rows=True,
            retired_rows_dump_ref=str(dump),
        )
        assert result["applied"] is True
        accepted = result["accepted_retired_ledger_rows"]
        assert {entry["relation"] for entry in accepted} == {"okg.job_runs"}
        assert accepted[0]["status"] == "retryable"
        assert accepted[0]["row_count"] == 1

        destroyed = {
            entry["relation"]: entry["row_count"]
            for entry in result["destroyed_row_counts"]
        }
        # Rows nothing refused, destroyed all the same -- these must be recorded.
        assert destroyed["okg.job_definitions"] == 1
        assert destroyed["okg.job_item_frontiers"] == 1
        assert destroyed["okg.job_runs"] == 1

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT result_metadata FROM okg.schema_migrations "
                "WHERE migration_id = %s",
                (REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID,),
            )
            metadata = cur.fetchone()[0]
            assert metadata["retired_rows_dump_ref"] == str(dump)
            assert metadata["accepted_retired_ledger_rows"][0]["relation"] == (
                "okg.job_runs"
            )
            recorded = {
                entry["relation"]: entry["row_count"]
                for entry in metadata["destroyed_row_counts"]
            }
            assert recorded["okg.job_definitions"] == 1
            assert recorded["okg.job_item_frontiers"] == 1
            cur.execute("SELECT to_regclass('okg.job_runs')")
            assert cur.fetchone()[0] is None
    finally:
        _drop_disposable_database(okg_dsn, dbname)


def test_remove_job_runtime_subsystem_refuses_queued_old_runtime_rows(
    okg_dsn: str,
) -> None:
    dbname = f"okg_codex_active_old_jobs_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        _seed_obsolete_job_runtime_schema(dsn, legacy_status="queued")
        _seed_empty_dbos_workflow_status(dsn)

        with pytest.raises(Exception, match="refuses active legacy rows"):
            apply_remove_job_runtime_subsystem(dsn, confirm=True)

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.jobs')")
            assert cur.fetchone()[0] == "okg.jobs"
    finally:
        _drop_disposable_database(okg_dsn, dbname)


def test_remove_job_runtime_subsystem_refuses_active_dbos_workflows(
    okg_dsn: str,
) -> None:
    dbname = f"okg_codex_active_dbos_runtime_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        _seed_obsolete_job_runtime_schema(dsn)
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA dbos")
            cur.execute(
                "CREATE TABLE dbos.workflow_status (name text, status text)"
            )
            cur.execute(
                "INSERT INTO dbos.workflow_status VALUES "
                "('okg_source_sync', 'ENQUEUED')"
            )

        with pytest.raises(Exception, match="requires DBOS runtime work to be drained"):
            apply_remove_job_runtime_subsystem(dsn, confirm=True)

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.job_runs')")
            assert cur.fetchone()[0] == "okg.job_runs"
    finally:
        _drop_disposable_database(okg_dsn, dbname)


def test_remove_job_runtime_subsystem_requires_dbos_authority(
    okg_dsn: str,
) -> None:
    dbname = f"okg_codex_missing_dbos_runtime_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        _seed_obsolete_job_runtime_schema(dsn)

        with pytest.raises(Exception, match="cannot verify DBOS workflow state"):
            apply_remove_job_runtime_subsystem(dsn, confirm=True)

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.job_runs')")
            assert cur.fetchone()[0] == "okg.job_runs"
    finally:
        _drop_disposable_database(okg_dsn, dbname)


def test_remove_job_runtime_subsystem_is_noop_when_already_absent(
    okg_dsn: str,
) -> None:
    dbname = f"okg_codex_absent_job_runtime_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)

        result = apply_remove_job_runtime_subsystem(dsn, confirm=True)

        assert result["applied"] is False
        assert result["already_absent"] is True
        assert result["preflight"] == {"required": False}
        assert result["postcondition"] == {
            "job_runtime_subsystem_absent": True
        }
    finally:
        _drop_disposable_database(okg_dsn, dbname)


def test_remove_job_runtime_subsystem_refuses_unknown_dependents(
    okg_dsn: str,
) -> None:
    dbname = f"okg_codex_job_runtime_dependency_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        _seed_obsolete_job_runtime_schema(dsn)
        _seed_empty_dbos_workflow_status(dsn)
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "CREATE VIEW okg.unclassified_job_runtime_dependency "
                "AS SELECT * FROM okg.job_runs"
            )

        with pytest.raises(psycopg.errors.DependentObjectsStillExist):
            apply_remove_job_runtime_subsystem(dsn, confirm=True)

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.job_runs')")
            assert cur.fetchone()[0] == "okg.job_runs"
            cur.execute(
                "SELECT to_regclass('okg.unclassified_job_runtime_dependency')"
            )
            assert cur.fetchone()[0] == (
                "okg.unclassified_job_runtime_dependency"
            )
    finally:
        _drop_disposable_database(okg_dsn, dbname)


def test_fresh_database_has_no_okg_job_runtime_surface(
    okg_dsn: str,
) -> None:
    dbname = f"okg_codex_runtime_contract_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      to_regclass('okg.jobs') IS NULL,
                      to_regclass('okg.publisher_leases') IS NULL,
                      to_regclass('okg.jobs_live_dedupe_uidx') IS NULL,
                      to_regclass('okg.v_job_queue') IS NULL,
                      to_regclass('okg.node_facts_workload_source_fact_idx') IS NULL,
                      to_regclass('okg.edge_facts_workload_source_fact_idx') IS NULL,
                      to_regclass('okg.edge_facts_source_pattern_idx') IS NULL,
                      to_regclass('okg.v_runtime_queue_health') IS NOT NULL
                    """
                )
                assert cur.fetchone() == (
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                )

    finally:
        _drop_disposable_database(okg_dsn, dbname)


def _create_disposable_database(base_dsn: str, dbname: str) -> str:
    info = conninfo_to_dict(base_dsn)
    info["dbname"] = "postgres"
    maintenance_dsn = make_conninfo(**info)
    with psycopg.connect(
        maintenance_dsn, autocommit=True, connect_timeout=2,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname))
            )
    info["dbname"] = dbname
    dsn = make_conninfo(**info)
    with psycopg.connect(dsn, autocommit=True, connect_timeout=2) as conn:
        with conn.cursor() as cur:
            for extension in (
                "btree_gist",
                "timescaledb",
                "vector",
                "pg_textsearch",
                "pg_trgm",
                "fuzzystrmatch",
            ):
                cur.execute(
                    sql.SQL("CREATE EXTENSION IF NOT EXISTS {}").format(
                        sql.Identifier(extension)
                    )
                )
    return dsn


def _drop_disposable_database(base_dsn: str, dbname: str) -> None:
    info = conninfo_to_dict(base_dsn)
    info["dbname"] = "postgres"
    maintenance_dsn = make_conninfo(**info)
    with psycopg.connect(
        maintenance_dsn, autocommit=True, connect_timeout=2,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pg_terminate_backend(pid)
                  FROM pg_stat_activity
                 WHERE datname = %s
                   AND pid <> pg_backend_pid()
                """,
                (dbname,),
            )
            cur.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(
                    sql.Identifier(dbname)
                )
            )
