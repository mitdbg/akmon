"""Generic ordered-migration runner: pending NNNN files run exactly once.

Covers req.migrate-idempotent.ordered-runner of ``make-migrate-idempotent``
(#669 regression item 3), design ruling 2. With a pending ordered migration
present (``0003_deployment_promotion_fence``, which the pre-fix code enumerates
but NEVER records — so it is pending forever and blocks release), an upgrade
runs only it and records it in the ledger via ``record_known_migration`` in the
same transaction; a second run executes nothing; a destructive migration
(``0002``) stays behind its reviewed adapter and is not auto-run; an
applied-but-edited migration is a typed refusal.
"""
from __future__ import annotations

from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from okg.substrate.db.management import (
    REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID,
    migration_status,
    run_pending_ordered_migrations,
)
from okg.substrate.db.migrate import migrate
from okg.substrate.deployment_release import load_database_release_state
from okg.substrate.errors import SubstrateError

FENCE_ID = "0003_deployment_promotion_fence"
LEGACY_FENCE_CHECKSUM = (
    "62d3e8b167509386144464ddf5270f4b43765211bb2f1bd77346b375e5f823bc"
)


def _ledger(dsn: str) -> dict[str, dict]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT migration_id, applied_at, result_metadata
              FROM okg.schema_migrations
            """
        )
        return {
            row[0]: {"applied_at": row[1], "metadata": row[2]}
            for row in cur.fetchall()
        }


@pytest.mark.postgres
def test_pending_ordered_migration_runs_and_is_recorded(okg_dsn: str) -> None:
    dbname = f"okg_runner_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        ledger = _ledger(dsn)
        # The pending non-destructive ordered migration was executed and
        # recorded. Pre-fix code never records 0003 -> it stays pending forever
        # and blocks deployment release.
        assert FENCE_ID in ledger, "0003 ordered migration was not recorded"
        assert ledger[FENCE_ID]["metadata"].get("applied_via") == "ordered_runner"
        # Its effect exists (the table it creates).
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.deployment_promotion_fences')")
            assert cur.fetchone()[0] is not None
    finally:
        _drop_disposable_database(okg_dsn, dbname)


@pytest.mark.postgres
def test_second_run_executes_no_ordered_migration(okg_dsn: str) -> None:
    dbname = f"okg_runner_idem_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        first = _ledger(dsn)[FENCE_ID]["applied_at"]
        # A direct second runner pass finds nothing pending.
        with psycopg.connect(dsn) as conn:
            out = run_pending_ordered_migrations(
                conn,
                schema_sql=_schema_text(),
                views_sql=_views_text(),
            )
            conn.commit()
        assert out["executed"] == []
        assert _ledger(dsn)[FENCE_ID]["applied_at"] == first, (
            "an already-applied ordered migration was re-recorded"
        )
    finally:
        _drop_disposable_database(okg_dsn, dbname)


@pytest.mark.postgres
def test_ordered_migration_restores_session_settings(okg_dsn: str) -> None:
    dbname = f"okg_runner_settings_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM okg.schema_migrations WHERE migration_id = %s",
                (FENCE_ID,),
            )

        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SET lock_timeout = '17s'")
                cur.execute("SET statement_timeout = '43s'")
            out = run_pending_ordered_migrations(
                conn,
                schema_sql=_schema_text(),
                views_sql=_views_text(),
            )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute("SHOW lock_timeout")
                lock_timeout = cur.fetchone()[0]
                cur.execute("SHOW statement_timeout")
                statement_timeout = cur.fetchone()[0]

        assert out["executed"] == [FENCE_ID]
        assert lock_timeout == "17s"
        assert statement_timeout == "43s"
    finally:
        _drop_disposable_database(okg_dsn, dbname)


@pytest.mark.postgres
def test_audited_legacy_migration_checksum_is_compatible(okg_dsn: str) -> None:
    dbname = f"okg_runner_legacy_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.schema_migrations SET checksum = %s "
                "WHERE migration_id = %s",
                (LEGACY_FENCE_CHECKSUM, FENCE_ID),
            )

        with psycopg.connect(dsn) as conn:
            out = run_pending_ordered_migrations(
                conn,
                schema_sql=_schema_text(),
                views_sql=_views_text(),
            )

        assert out["executed"] == []
        assert [
            row["migration_id"] for row in out["checksum_compatibilities"]
        ] == [FENCE_ID]
        status = migration_status(dsn)
        assert status["ok"] is True
        assert status["checksum_mismatches"] == []
        assert status["checksum_compatibilities"][0]["recorded_checksum"] == (
            LEGACY_FENCE_CHECKSUM
        )
        release_state = load_database_release_state(dsn).to_dict()
        assert release_state["checksum_compatibilities"] == (
            status["checksum_compatibilities"]
        )
    finally:
        _drop_disposable_database(okg_dsn, dbname)


@pytest.mark.postgres
def test_destructive_migration_is_not_auto_run(okg_dsn: str) -> None:
    dbname = f"okg_runner_destr_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        ledger = _ledger(dsn)
        # 0002 is destructive: it is recorded via the reviewed schema-baseline
        # adapter path, NOT the generic runner.
        assert REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID in ledger
        assert (
            ledger[REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID]["metadata"].get(
                "applied_via"
            )
            != "ordered_runner"
        )
        # And the runner reports it as skipped-destructive if it were pending.
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM okg.schema_migrations WHERE migration_id = %s",
                (REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID,),
            )
        with psycopg.connect(dsn) as conn:
            out = run_pending_ordered_migrations(
                conn,
                schema_sql=_schema_text(),
                views_sql=_views_text(),
            )
            conn.commit()
        skipped_ids = [row["migration_id"] for row in out["skipped_destructive"]]
        assert REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID in skipped_ids
        assert out["executed"] == []
    finally:
        _drop_disposable_database(okg_dsn, dbname)


@pytest.mark.postgres
def test_edited_applied_migration_is_a_typed_refusal(okg_dsn: str) -> None:
    dbname = f"okg_runner_edit_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        # Corrupt the recorded checksum of an applied ordered migration: the
        # runner must refuse loudly BEFORE executing anything.
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.schema_migrations SET checksum = %s "
                "WHERE migration_id = %s",
                ("tampered", FENCE_ID),
            )
        with psycopg.connect(dsn) as conn:
            with pytest.raises(SubstrateError) as excinfo:
                run_pending_ordered_migrations(
                    conn,
                    schema_sql=_schema_text(),
                    views_sql=_views_text(),
                )
        assert "checksum changed" in str(excinfo.value)
    finally:
        _drop_disposable_database(okg_dsn, dbname)


# --------------------------------------------------------------------------
# Helpers.
# --------------------------------------------------------------------------


def _schema_text() -> str:
    import importlib.resources

    return (
        importlib.resources.files("okg.substrate.db")
        .joinpath("schema.sql")
        .read_text()
    )


def _views_text() -> str:
    import importlib.resources

    return (
        importlib.resources.files("okg.substrate.db")
        .joinpath("views.sql")
        .read_text()
    )


def _create_disposable_database(base_dsn: str, dbname: str) -> str:
    info = conninfo_to_dict(base_dsn)
    info["dbname"] = "postgres"
    maintenance_dsn = make_conninfo(**info)
    with psycopg.connect(
        maintenance_dsn, autocommit=True, connect_timeout=5,
    ) as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    info["dbname"] = dbname
    dsn = make_conninfo(**info)
    with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn, \
            conn.cursor() as cur:
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
        maintenance_dsn, autocommit=True, connect_timeout=5,
    ) as conn, conn.cursor() as cur:
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
            sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(dbname))
        )
