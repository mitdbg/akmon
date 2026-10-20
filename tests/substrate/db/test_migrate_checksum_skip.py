"""Checksum fast-path: an unchanged schema body is not replayed.

Covers req.migrate-idempotent.checksum-skip of ``make-migrate-idempotent``
(#688/#669 regression items 1-2). The ratified design (reports/design-review.md
ruling 1) fixes the skip as: when the applied ``executable_schema_checksum``
(the post-transform ``schema_without_legacy_live_table_compatibility`` text)
matches the computed one, ``migrate`` does NOT execute the schema body, while
view installation, ownership, compat checks, Chronos ensures, and the ordered
runner still run. A checksum mismatch (or a legacy row without the field, which
is fail-closed) still replays. The observable shape assertion is the design's
own invariant: **ledger mutation <=> body execution** — on a skip the 0000
snapshot row is NOT rewritten (its ``applied_at`` is stable), and on a replay it
is.
"""
from __future__ import annotations

from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from okg.substrate.db.management import (
    SCHEMA_SNAPSHOT_MIGRATION_ID,
    migration_status,
)
from okg.substrate.db.migrate import migrate

# The result_metadata field the fast path stores/consults. Inlined (not
# imported) so this regression collects and runs behaviorally against the
# pre-fix tree, which has no such constant.
EXECUTABLE_SCHEMA_CHECKSUM_KEY = "executable_schema_checksum"


def _snapshot_row(dsn: str) -> dict:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT applied_at, checksum, result_metadata
              FROM okg.schema_migrations
             WHERE migration_id = %s
            """,
            (SCHEMA_SNAPSHOT_MIGRATION_ID,),
        )
        applied_at, checksum, metadata = cur.fetchone()
    return {"applied_at": applied_at, "checksum": checksum, "metadata": metadata}


@pytest.mark.postgres
def test_unchanged_schema_body_is_not_replayed(okg_dsn: str) -> None:
    dbname = f"okg_ckskip_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        first = _snapshot_row(dsn)
        # The executable-schema checksum was recorded on the first (executing)
        # run so the second run can decide the skip.
        assert first["metadata"].get(EXECUTABLE_SCHEMA_CHECKSUM_KEY)

        migrate(dsn)
        second = _snapshot_row(dsn)

        # Skip => the ledger row is NOT rewritten (design invariant: ledger
        # mutation <=> body execution). Pre-fix code rewrites applied_at on
        # every run because it always executes the body and always records.
        assert second["applied_at"] == first["applied_at"], (
            "schema body was replayed on an unchanged upgrade "
            "(0000 snapshot row was rewritten)"
        )
    finally:
        _drop_disposable_database(okg_dsn, dbname)


@pytest.mark.postgres
def test_views_still_install_on_the_skip_path(okg_dsn: str) -> None:
    dbname = f"okg_ckskip_views_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        # Drop an agent-facing view; the skip path must still reinstall it
        # (views are outside the schema-body checksum and always run).
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("DROP VIEW IF EXISTS okg.v_nodes CASCADE")
            cur.execute("SELECT to_regclass('okg.v_nodes')")
            assert cur.fetchone()[0] is None

        migrate(dsn)  # unchanged -> body skipped, but views reinstall

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.v_nodes')")
            assert cur.fetchone()[0] is not None, (
                "views were not reinstalled on the checksum-skip path"
            )
    finally:
        _drop_disposable_database(okg_dsn, dbname)


@pytest.mark.postgres
def test_mutated_checksum_still_replays(okg_dsn: str) -> None:
    dbname = f"okg_ckskip_mut_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        first = _snapshot_row(dsn)

        # Corrupt the recorded executable-schema checksum: the next run must
        # treat it as a mismatch and REPLAY the body (rewriting the row).
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.schema_migrations
                   SET result_metadata =
                       jsonb_set(result_metadata, %s, '"deadbeef"')
                 WHERE migration_id = %s
                """,
                ([EXECUTABLE_SCHEMA_CHECKSUM_KEY], SCHEMA_SNAPSHOT_MIGRATION_ID),
            )

        migrate(dsn)
        second = _snapshot_row(dsn)

        assert second["applied_at"] != first["applied_at"], (
            "a mismatched executable checksum must replay the schema body"
        )
        # And the correct checksum is restored (not left corrupt).
        assert (
            second["metadata"].get(EXECUTABLE_SCHEMA_CHECKSUM_KEY) != "deadbeef"
        )
    finally:
        _drop_disposable_database(okg_dsn, dbname)


@pytest.mark.postgres
def test_legacy_row_without_field_replays(okg_dsn: str) -> None:
    dbname = f"okg_ckskip_legacy_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        first = _snapshot_row(dsn)

        # Simulate a pre-fix ledger row that never recorded the executable
        # checksum field: absence must fail closed (replay), never skip.
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.schema_migrations
                   SET result_metadata = result_metadata - %s
                 WHERE migration_id = %s
                """,
                (EXECUTABLE_SCHEMA_CHECKSUM_KEY, SCHEMA_SNAPSHOT_MIGRATION_ID),
            )

        migrate(dsn)
        second = _snapshot_row(dsn)
        assert second["applied_at"] != first["applied_at"], (
            "a legacy row missing the executable checksum must replay (fail-closed)"
        )
        assert second["metadata"].get(EXECUTABLE_SCHEMA_CHECKSUM_KEY)
    finally:
        _drop_disposable_database(okg_dsn, dbname)


@pytest.mark.postgres
def test_views_only_change_keeps_migration_status_ok(okg_dsn: str) -> None:
    """A views-only release skips the body but must NOT desync the joint
    schema+views checksum (finding 1).

    The executable-body checksum (the skip key) deliberately excludes views, so
    a views-only edit skips ``cur.execute(executable_sql)``. But the 0000 row's
    ``checksum`` column is the JOINT schema+views hash that ``migration_status``
    (and ``deployment_release`` / doctor) consume. If the skip path does not
    refresh that column, a views-only release leaves it stale, producing a
    spurious, permanent ``schema_checksum_mismatch`` release-block. This test
    simulates a stale joint checksum on the skip path and asserts migrate
    refreshes it in place — without replaying the body (``applied_at`` stable).

    Fail-before is behavioral against the branch's PRE-finding-1 code: there the
    skip branch leaves the stale checksum untouched, so ``migration_status.ok``
    stays False. (On ``origin/dev`` there is no skip at all — the body always
    re-records the current joint checksum — so this is a fast-path-only
    invariant.)
    """
    dbname = f"okg_ckskip_views_only_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        first = _snapshot_row(dsn)

        # Simulate a views-only release: the recorded JOINT checksum predates a
        # views edit (stale), while the executable-body checksum is current (so
        # the next migrate skips the body). Only the joint ``checksum`` column
        # is corrupted; ``result_metadata`` (the skip key) is left intact.
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.schema_migrations
                   SET checksum = %s
                 WHERE migration_id = %s
                """,
                ("stale-joint-checksum-views-only", SCHEMA_SNAPSHOT_MIGRATION_ID),
            )

        # The stale joint checksum is a release-blocking mismatch right now.
        assert migration_status(dsn)["ok"] is False, (
            "precondition: a stale joint checksum must read as a mismatch"
        )

        migrate(dsn)  # unchanged body -> skipped; joint checksum must refresh
        second = _snapshot_row(dsn)

        # Body was NOT replayed (skip invariant preserved) ...
        assert second["applied_at"] == first["applied_at"], (
            "the schema body was replayed on a views-only skip "
            "(0000 applied_at was bumped)"
        )
        # ... but the joint checksum was refreshed, clearing the false block.
        assert second["checksum"] == first["checksum"], (
            "the joint schema+views checksum was not refreshed on the skip path"
        )
        assert migration_status(dsn)["ok"] is True, (
            "a views-only skip left migration_status in a false "
            "schema_checksum_mismatch release-block"
        )
    finally:
        _drop_disposable_database(okg_dsn, dbname)


# --------------------------------------------------------------------------
# Helpers (mirrors tests/substrate/db/test_ordered_migrations.py).
# --------------------------------------------------------------------------


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
