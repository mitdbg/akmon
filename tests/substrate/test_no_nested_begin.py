"""No okg or Chronos code sends BEGIN inside an open transaction (lane C 238a).

A psycopg connection with autocommit off sends its own BEGIN before the first
statement of a transaction. Code that then executed "BEGIN" as that statement
sent a second BEGIN inside psycopg's: PostgreSQL ignored it and logged
"WARNING: there is already a transaction in progress". One CI bootstrap of
okg-workspace logged 92 of them (Tests run 36937244747): every Chronos
``BranchSession.transaction()`` on a Chronos-owned session (each source run's
graph store) did it, and so did run_sql's operational-view path and the MCP
schema digest.

The ignored BEGIN also meant run_sql's final ROLLBACK ended a caller's own
transaction when the caller already held one, discarding the caller's writes.

Each test captures the server's warnings on the connection itself, so it fails
on the code that sends the nested BEGIN, not on a log scrape. The static guard
against a new explicit BEGIN is tests/substrate/test_no_begin_statement_static.py.
"""
from __future__ import annotations

import psycopg
import pytest
from psycopg.pq import TransactionStatus

from tests.substrate._db_helpers import (
    ensure_published_generation,
    seed_catalog_version,
)

NESTED_BEGIN = "there is already a transaction in progress"


def _capture_notices(conn: psycopg.Connection) -> list[str]:
    seen: list[str] = []
    conn.add_notice_handler(
        lambda diag: seen.append(
            f"{diag.severity_nonlocalized}: {diag.message_primary}"
        )
    )
    return seen


def _nested_begin_warnings(seen: list[str]) -> list[str]:
    return [line for line in seen if NESTED_BEGIN in line]


# ---------------------------------------------------------------- Chronos


@pytest.mark.postgres
def test_chronos_adapter_begin_opens_one_transaction(okg_migrated: str) -> None:
    """A Chronos-owned session (autocommit off) begins without a second BEGIN."""
    from chronos_core.branching.sql_adapters import PostgresDatabaseAdapter

    adapter = PostgresDatabaseAdapter.connect(okg_migrated)
    try:
        conn = adapter.raw_connection
        assert conn.autocommit is False
        seen = _capture_notices(conn)

        adapter.begin()
        # Open now, not at the next statement: callers branch on this.
        assert conn.info.transaction_status == TransactionStatus.INTRANS
        assert adapter.in_transaction is True
        adapter.execute("SELECT 1")
        adapter.commit()

        assert conn.info.transaction_status == TransactionStatus.IDLE
        assert _nested_begin_warnings(seen) == []
    finally:
        adapter.close()


@pytest.mark.postgres
def test_chronos_adapter_begin_on_autocommit_connection(okg_migrated: str) -> None:
    """On an autocommit connection only an explicit BEGIN opens a transaction."""
    from chronos_core.branching.sql_adapters import PostgresDatabaseAdapter

    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        seen = _capture_notices(conn)
        adapter = PostgresDatabaseAdapter(conn)
        adapter.begin()
        assert conn.info.transaction_status == TransactionStatus.INTRANS
        adapter.rollback()
        assert conn.info.transaction_status == TransactionStatus.IDLE
        assert _nested_begin_warnings(seen) == []


@pytest.mark.postgres
def test_chronos_adapter_begin_refuses_an_open_transaction(okg_migrated: str) -> None:
    """A BEGIN inside a transaction starts nothing, so begin() refuses loudly."""
    from chronos_core.branching.sql_adapters import (
        PostgresDatabaseAdapter,
        SQLAdapterError,
    )

    adapter = PostgresDatabaseAdapter.connect(okg_migrated)
    try:
        conn = adapter.raw_connection
        seen = _capture_notices(conn)
        adapter.execute("SELECT 1")  # psycopg opens a transaction here
        assert conn.info.transaction_status == TransactionStatus.INTRANS

        with pytest.raises(SQLAdapterError, match="needs an idle session"):
            adapter.begin()
        assert _nested_begin_warnings(seen) == []
        adapter.rollback()
    finally:
        adapter.close()


@pytest.mark.postgres
def test_chronos_session_transaction_sends_no_nested_begin(
    clean_runtime: str,
) -> None:
    """The real path: a graph store's branch-session transaction, as a source run uses it."""
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        conn = store.context.db.raw_connection
        seen = _capture_notices(conn)
        session = store.context.checkout("main")
        for _ in range(2):
            with session.transaction():
                assert conn.info.transaction_status == TransactionStatus.INTRANS
                session.query("SELECT 1 AS one")
        assert _nested_begin_warnings(seen) == []
    finally:
        store.close()


# ---------------------------------------------------------------- run_sql


_OPERATIONAL_SQL = "SELECT count(*) AS runs FROM okg.v_source_runs"


def _seed_operational_view(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(
            conn, generation_id=21, deployment_name="default",
        )


@pytest.mark.postgres
def test_run_sql_operational_path_sends_no_nested_begin(clean_runtime: str) -> None:
    """The MCP pool's autocommit connection: run_sql owns and ends its transaction."""
    from okg.substrate.mcp import open_session
    from okg.substrate.mcp.run_sql import run_sql

    _seed_operational_view(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seen = _capture_notices(conn)
        session = open_session(conn, deployment_name="default")
        out = run_sql(conn, session, _OPERATIONAL_SQL)

        assert out["query_plan_summary"] != "Chronos checkout"
        assert out["rows"] == [[0]]
        assert conn.autocommit is True
        assert conn.info.transaction_status == TransactionStatus.IDLE
        assert _nested_begin_warnings(seen) == []


@pytest.mark.postgres
def test_run_sql_operational_path_keeps_the_callers_transaction(
    clean_runtime: str,
) -> None:
    """A caller's open transaction survives run_sql, with its writes and settings."""
    from okg.substrate.mcp import open_session
    from okg.substrate.mcp.run_sql import run_sql

    _seed_operational_view(clean_runtime)
    with psycopg.connect(clean_runtime) as conn:
        seen = _capture_notices(conn)
        session = open_session(conn, deployment_name="default")
        with conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE caller_marker_238a (x int)")
            cur.execute("INSERT INTO caller_marker_238a VALUES (1)")
            cur.execute("SELECT current_user")
            caller_role = cur.fetchone()[0]
        assert conn.info.transaction_status == TransactionStatus.INTRANS

        out = run_sql(conn, session, _OPERATIONAL_SQL)

        assert out["rows"] == [[0]]
        assert conn.info.transaction_status == TransactionStatus.INTRANS
        with conn.cursor() as cur:
            cur.execute("SELECT x FROM caller_marker_238a")
            assert cur.fetchall() == [(1,)]
            # run_sql's SET LOCAL read-only, role, and timeout are undone.
            cur.execute(
                "SELECT current_user, current_setting('transaction_read_only'), "
                "current_setting('statement_timeout')"
            )
            role, read_only, _timeout = cur.fetchone()
        assert role == caller_role
        assert read_only == "off"
        assert _nested_begin_warnings(seen) == []
        conn.rollback()


@pytest.mark.postgres
def test_run_sql_operational_error_keeps_the_callers_transaction(
    clean_runtime: str,
) -> None:
    """A refused query rolls back only run_sql's savepoint, not the caller's work."""
    from okg.substrate.mcp import open_session
    from okg.substrate.mcp.run_sql import run_sql
    from okg.substrate.mcp.sql_safety import RunSqlInvalid

    _seed_operational_view(clean_runtime)
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        with conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE caller_marker_238a (x int)")
            cur.execute("INSERT INTO caller_marker_238a VALUES (2)")

        with pytest.raises(RunSqlInvalid):
            run_sql(
                conn, session,
                "SELECT no_such_column_238a FROM okg.v_source_runs",
            )

        assert conn.info.transaction_status == TransactionStatus.INTRANS
        with conn.cursor() as cur:
            cur.execute("SELECT x FROM caller_marker_238a")
            assert cur.fetchall() == [(2,)]
        conn.rollback()
