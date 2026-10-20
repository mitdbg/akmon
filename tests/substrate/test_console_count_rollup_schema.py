"""Step-1 gate for console-count-rollup (#444 slice 2).

The maintained-derivation count rollup table (okg.console_count_rollup) must be
CREATED *and* Chronos-REGISTERED on both provisioning paths:

* fresh provision (ensure_schema) -- a brand-new deployment gets it;
* migration (ensure_console_count_rollup_table / ensure_interval_storage_upgrade)
  -- a deployment provisioned BEFORE this table existed gets it, even though
  ensure_schema early-returns on an already-registered graph.

Registration (not just CREATE) is the load-bearing property: only a registered
Chronos interval table reconstructs the checkpoint-consistent rows for an
as-of-generation read.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.attributes import ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE
from okg.substrate.graph_store import CONSOLE_COUNT_ROLLUP_TABLE, ChronosGraphStore
from okg.substrate.graph_store_internal.generation_ordinal import (
    GENERATION_ORDINAL_TABLE,
)

pytestmark = pytest.mark.postgres

# ``ensure_generation_ordinal_schema`` creates this view beside the ordinal
# table, so the current migration chain always leaves it depending on
# okg.generation_ordinals. Kept as a literal (the source builds it inline in
# a CREATE OR REPLACE VIEW body, so there is no constant to import).
GENERATION_INTERVAL_STAB_VIEW = "okg.v_generation_interval_stab"


def _table_exists(conn, qualified: str) -> bool:
    return bool(
        conn.execute(
            "SELECT to_regclass(%s) IS NOT NULL", (qualified,)
        ).fetchone()[0]
    )


def _view_exists(conn, qualified: str) -> bool:
    """True when ``qualified`` exists AND is actually a view (relkind 'v')."""
    row = conn.execute(
        "SELECT relkind FROM pg_class WHERE oid = to_regclass(%s)",
        (qualified,),
    ).fetchone()
    return row is not None and row[0] == "v"


def _interval_registration(conn, logical: str):
    """The (physical_table) registration row for a logical interval table, or None."""
    return conn.execute(
        "SELECT physical_table FROM _chronos_branch_tables "
        "WHERE table_name = %s AND backend = 'interval'",
        (logical,),
    ).fetchone()


def test_fresh_provision_creates_and_registers_count_rollup(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn, ensure_schema=True)
        assert _table_exists(conn, CONSOLE_COUNT_ROLLUP_TABLE)
        assert _interval_registration(conn, CONSOLE_COUNT_ROLLUP_TABLE) is not None


def test_ensure_count_rollup_table_is_idempotent(clean_runtime: str) -> None:
    """The migration entry points run clean + twice on an already-provisioned DB."""
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.ensure_console_count_rollup_table()
        store.ensure_console_count_rollup_table()
        store.ensure_interval_storage_upgrade()
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            assert _table_exists(conn, CONSOLE_COUNT_ROLLUP_TABLE)
            assert _interval_registration(conn, CONSOLE_COUNT_ROLLUP_TABLE) is not None
    finally:
        store.close()


def test_migration_adds_count_rollup_to_legacy_registered_graph(
    clean_runtime: str,
) -> None:
    """Simulate a deployment provisioned before okg.console_count_rollup existed:
    drop the table + its registration. ensure_schema must NOT re-add it (it
    early-returns on a registered graph -- the exact bug the explicit migration
    exists for), but ensure_console_count_rollup_table() must."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn, ensure_schema=True)
        reg = _interval_registration(conn, CONSOLE_COUNT_ROLLUP_TABLE)
        assert reg is not None, "fixture precondition: rollup registered after provision"
        physical = str(reg[0])

        # Tear it down to mimic a pre-table deployment.
        conn.execute(f"DROP TABLE IF EXISTS {CONSOLE_COUNT_ROLLUP_TABLE} CASCADE")
        conn.execute(f'DROP TABLE IF EXISTS "{physical}" CASCADE')
        conn.execute(
            "DELETE FROM _chronos_branch_tables WHERE table_name = %s",
            (CONSOLE_COUNT_ROLLUP_TABLE,),
        )
        assert not _table_exists(conn, CONSOLE_COUNT_ROLLUP_TABLE)
        assert _interval_registration(conn, CONSOLE_COUNT_ROLLUP_TABLE) is None

    # The teardown above dropped the rollup table + its registration OUT OF BAND
    # (raw SQL), which does NOT invalidate the process-level registry cache that
    # perf-reimpl (#448) keeps for production (where the schema is stable per
    # process). Without this, the fresh store below would still find the rollup in
    # its cached `self.tables` and register_table() would CREATE INDEX on the
    # just-dropped physical interval table. Re-apply the same reset the conftest
    # autouse fixture runs around every test, so the migration below sees the
    # rollup as unregistered -- exactly as a real legacy deployment does when it
    # migrates in a cold process.
    from chronos_core.branching._common import clear_all_registry_caches
    from okg.substrate.graph_store_internal.common import clear_schema_ensured_cache
    from okg.substrate.graph_store_internal.schema import _clear_mcp_grant_guard

    clear_all_registry_caches()
    clear_schema_ensured_cache()
    _clear_mcp_grant_guard()

    # ensure_schema alone is a no-op on the still-registered graph (early-return):
    # the legacy deployment would never get the table this way.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn, ensure_schema=True)
        assert _interval_registration(conn, CONSOLE_COUNT_ROLLUP_TABLE) is None, (
            "ensure_schema must early-return on a registered graph -- "
            "the explicit migration is what re-adds the rollup"
        )

    # The explicit migration entry point restores it (create + register).
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.ensure_console_count_rollup_table()
    finally:
        store.close()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _table_exists(conn, CONSOLE_COUNT_ROLLUP_TABLE)
        assert _interval_registration(conn, CONSOLE_COUNT_ROLLUP_TABLE) is not None


def test_interval_cutover_schema_migration_runs_on_legacy_registered_graph(
    schema_destructive_runtime: str,
) -> None:
    """A deployment registered before the interval cutover tables existed still
    gets those OKG-owned side tables from the explicit migration path."""
    clean_runtime = schema_destructive_runtime
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn, ensure_schema=True)
        assert _view_exists(conn, GENERATION_INTERVAL_STAB_VIEW), (
            "fixture precondition: the migrated database has the stab view"
        )

        # A pre-cutover deployment predates these side tables AND everything the
        # current chain builds on top of them. okg.v_generation_interval_stab is
        # a view over okg.generation_ordinals, so an honest legacy simulation
        # drops the dependents too; a bare DROP TABLE instead raises
        # DependentObjectsStillExist on a fully migrated database.
        conn.execute(
            f"DROP TABLE IF EXISTS {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} CASCADE"
        )
        conn.execute(f"DROP TABLE IF EXISTS {GENERATION_ORDINAL_TABLE} CASCADE")
        assert not _view_exists(conn, GENERATION_INTERVAL_STAB_VIEW), (
            "CASCADE must take the dependent view with the table -- otherwise "
            "the migration below is proved against a half-legacy schema"
        )

    from chronos_core.branching._common import clear_all_registry_caches
    from okg.substrate.graph_store_internal.common import clear_schema_ensured_cache
    from okg.substrate.graph_store_internal.schema import _clear_mcp_grant_guard

    clear_all_registry_caches()
    clear_schema_ensured_cache()
    _clear_mcp_grant_guard()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn, ensure_schema=True)
        assert not _table_exists(conn, ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE)
        assert not _table_exists(conn, GENERATION_ORDINAL_TABLE)
        assert not _view_exists(conn, GENERATION_INTERVAL_STAB_VIEW)

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.ensure_interval_storage_upgrade()
        store.ensure_interval_storage_upgrade()
    finally:
        store.close()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _table_exists(conn, ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE)
        assert _table_exists(conn, GENERATION_ORDINAL_TABLE)
        # The dependent view is part of the cutover, not collateral: the
        # migration must rebuild what CASCADE removed, or the MCP sidecar
        # (which refuses loudly on a missing stab view) stays broken after a
        # nominally successful migrate.
        assert _view_exists(conn, GENERATION_INTERVAL_STAB_VIEW)
