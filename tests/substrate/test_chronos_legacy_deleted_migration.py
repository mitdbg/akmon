"""Regression tests for the Chronos interval-backend legacy-`deleted` migration.

A deployment provisioned by an older Chronos has its interval backing tables
shaped with an INTEGER `deleted` tombstone, a partial visibility index
`... WHERE (deleted = 0)`, and no `writer_segment_id`. Upgrading to the current
Chronos must convert those tables in place:

  * `deleted` INTEGER -> BOOLEAN (0 -> false, non-zero -> true), and
  * add `writer_segment_id`,

WITHOUT failing on `operator does not exist: integer = boolean` (which Postgres
raises when it re-validates the `deleted = 0` partial-index predicate against
the new BOOLEAN type). The fix drops any `deleted`-predicate partial index
before the ALTER (the consumer recreates it with a boolean predicate), and the
okg migrate path invokes the column-ensure explicitly so an already-registered
(read-skip) DB still gets upgraded.

This was the root cause of okg-workspace's long-standing
`operator does not exist: integer = boolean` projection breakage and it blocked
deploying current dev to the okg-ci deployment.
"""
from __future__ import annotations

import json
import uuid

from psycopg import sql
import pytest

from okg.substrate.db.migrate import _drop_chronos_checkpoint_dependent_views
from okg.substrate.db.connection import connect as db_connect
from okg.substrate.graph_store import ChronosGraphStore


pytestmark = [pytest.mark.postgres, pytest.mark.serial_db]


def test_column_ensure_converts_legacy_integer_deleted_with_blocking_index(
    clean_runtime: str,
) -> None:
    """The core fix: `_ensure_interval_physical_storage_columns` upgrades a
    legacy-shaped interval table (integer `deleted` + `WHERE (deleted = 0)`
    partial index + no `writer_segment_id`) cleanly, preserving tombstone data.

    The whole test runs in one uncommitted transaction that is rolled back, so
    the throwaway table never persists (no contamination of the shared DB)."""
    store = ChronosGraphStore.connect(clean_runtime)
    backend = store.context._backend
    db = backend.db
    phys = f"_test_legacy_interval_{uuid.uuid4().hex[:10]}"
    idx = f"idx_{phys}_visible"
    try:
        assert backend.name == "interval"
        # Legacy provisioning shape: integer `deleted`, no writer_segment_id,
        # and a partial index whose predicate references the integer column.
        db.execute(
            f'CREATE TABLE "{phys}" ('
            "node_id TEXT NOT NULL, live_lo BIGINT NOT NULL, "
            "live_hi BIGINT NOT NULL, deleted INTEGER NOT NULL DEFAULT 0, "
            "PRIMARY KEY (node_id, live_lo))"
        )
        db.execute(
            f'INSERT INTO "{phys}" (node_id, live_lo, live_hi, deleted) '
            "VALUES ('live', 0, 100, 0), ('tombstoned', 0, 100, 1)"
        )
        db.execute(
            f'CREATE INDEX "{idx}" ON "{phys}" (node_id, live_lo, live_hi) '
            "WHERE deleted = 0"
        )

        # Without the fix this raises: operator does not exist: integer = boolean
        # (the `deleted = 0` partial index blocks ALTER COLUMN deleted TYPE BOOLEAN).
        backend._ensure_interval_physical_storage_columns(phys)

        cols, defs = db.table_defs(phys)
        assert "writer_segment_id" in cols
        assert "BOOL" in defs[cols.index("deleted")].upper(), defs[cols.index("deleted")]

        # Tombstone data preserved: 0 -> false (live), 1 -> true (deleted);
        # writer_segment_id backfilled to 1.
        rows = db.execute(
            f'SELECT node_id, deleted, writer_segment_id FROM "{phys}" ORDER BY node_id'
        ).fetchall()
        assert [
            (r["node_id"], r["deleted"], r["writer_segment_id"]) for r in rows
        ] == [("live", False, 1), ("tombstoned", True, 1)]

        # The integer-predicate index was dropped (the consumer re-creates a
        # boolean-predicate one); no `deleted`-predicate index remains.
        blocking = db.execute(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename = ? AND indexdef ILIKE ?",
            (phys, "%WHERE%deleted%"),
        ).fetchall()
        assert blocking == [], blocking

        # The boolean predicate the bug broke now works.
        counted = db.execute(
            f'SELECT count(*) AS n FROM "{phys}" WHERE deleted = FALSE'
        ).fetchall()
        assert counted[0]["n"] == 1

        # Idempotent: a second pass over the now-boolean table is a no-op.
        backend._ensure_interval_physical_storage_columns(phys)
    finally:
        db.rollback()
        store.close()


def test_ensure_interval_storage_upgrade_runs_clean_on_graph_tables(
    clean_runtime: str,
) -> None:
    """The migrate-path entry point runs the column-ensure on the registered
    okg graph tables (bypassing ensure_schema's read-only early-return) and is
    idempotent + error-free on an already-upgraded DB."""
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        # Runs register_table + the visibility-index ensure for the graph
        # tables. On the migrated test DB these are already correct, so this is
        # a no-op migration — but it must complete without error, twice.
        store.ensure_interval_storage_upgrade()
        store.ensure_interval_storage_upgrade()

        backend = store.context._backend
        for logical in ("okg.graph_nodes", "okg.graph_edges"):
            meta = backend.tables[logical]
            cols, defs = backend.db.table_defs(meta.physical_name)
            assert "writer_segment_id" in cols
            assert "BOOL" in defs[cols.index("deleted")].upper()
    finally:
        store.close()


def test_graph_column_upgrade_repairs_registered_graph_embedding_drift(
    clean_runtime: str,
) -> None:
    """Registered Chronos graphs still need additive OKG graph columns.

    ``ensure_schema`` intentionally skips DDL once graph tables are registered,
    so migrate calls this writer-only helper to repair logical tables, interval
    physical tables, and Chronos metadata after additive column drift.
    """
    store = ChronosGraphStore.connect(clean_runtime)
    store.close()

    try:
        with db_connect(clean_runtime) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT physical_table, columns, column_defs
                      FROM _chronos_branch_tables
                     WHERE table_name = 'okg.graph_nodes'
                       AND backend = 'interval'
                    """
                )
                physical_table, columns_raw, defs_raw = cur.fetchone()
                columns = json.loads(columns_raw)
                defs = json.loads(defs_raw)
                kept = [
                    (column, definition)
                    for column, definition in zip(columns, defs)
                    if column != "embedding"
                ]

                # A pre-embedding deployment also predates the views that read
                # the column; CASCADE removes them so the drift simulation can
                # drop the column. Migrate (in the finally) reinstalls them.
                cur.execute(
                    "ALTER TABLE okg.graph_nodes "
                    "DROP COLUMN IF EXISTS embedding CASCADE"
                )
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {} DROP COLUMN IF EXISTS embedding CASCADE"
                    ).format(sql.Identifier(str(physical_table)))
                )
                cur.execute(
                    """
                    UPDATE _chronos_branch_tables
                       SET columns = %s,
                           column_defs = %s
                     WHERE table_name = 'okg.graph_nodes'
                       AND backend = 'interval'
                    """,
                    (
                        json.dumps([column for column, _definition in kept]),
                        json.dumps([definition for _column, definition in kept]),
                    ),
                )
            conn.commit()

        # The registry surgery above bypassed Chronos's process-level
        # registry cache; drop it so the reconnect below sees the
        # simulated drift instead of the cached pre-drop columns.
        from chronos_core.branching._common import clear_all_registry_caches

        clear_all_registry_caches()

        store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
        try:
            store.ensure_graph_column_upgrade()
        finally:
            store.close()

        with db_connect(clean_runtime) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT physical_table, columns
                      FROM _chronos_branch_tables
                     WHERE table_name = 'okg.graph_nodes'
                       AND backend = 'interval'
                    """
                )
                physical_table, columns_raw = cur.fetchone()
                assert "embedding" in json.loads(columns_raw)

                cur.execute(
                    """
                    SELECT column_name
                      FROM information_schema.columns
                     WHERE table_schema = 'okg'
                       AND table_name = 'graph_nodes'
                    """
                )
                assert "embedding" in {row[0] for row in cur.fetchall()}

                cur.execute(
                    """
                    SELECT column_name
                      FROM information_schema.columns
                     WHERE table_schema = 'public'
                       AND table_name = %s
                    """,
                    (str(physical_table),),
                )
                assert "embedding" in {row[0] for row in cur.fetchall()}
    finally:
        # Reinstall the substrate views the CASCADE drop removed so
        # sibling tests sharing this session-scoped DB keep their read
        # surface.
        from okg.substrate.db.migrate import migrate

        migrate(clean_runtime)


def test_migrate_drops_generation_view_before_chronos_segment_id_rebuild(
    clean_runtime: str,
) -> None:
    """OKG's generated generation view must not block Chronos table rebuilds."""
    with db_connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP VIEW IF EXISTS okg.v_generations CASCADE")
            cur.execute(
                """
                CREATE VIEW okg.v_generations AS
                SELECT checkpoint_id AS id
                  FROM public._chronos_branch_interval_checkpoints
                """
            )
            cur.execute("SELECT to_regclass('okg.v_generations')")
            assert cur.fetchone()[0] == "okg.v_generations"

            _drop_chronos_checkpoint_dependent_views(cur)

            cur.execute("SELECT to_regclass('okg.v_generations')")
            assert cur.fetchone()[0] is None
        conn.rollback()
