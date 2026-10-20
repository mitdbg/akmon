"""Steady-state ledger access must execute zero DDL statements.

Regression for the 2026-07-22 incident: the decision-ledger ensure ran
``ALTER TABLE okg.pact_intents ADD COLUMN IF NOT EXISTS ...`` on every ledger
touch. Those statements take ACCESS EXCLUSIVE even when they change
nothing, and one such touch from a background agent killed a live
6-minute ``pg_dump`` (twice) during ``okg deployment up``.

The ledger relations moved into ``okg`` under
``schema-namespace-qualification``; the guard also became finer-grained
there, so a stripped writer grant is repaired with a GRANT alone rather than
by replaying the table DDL. That distinction is asserted below.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.pact import decisions as d

pytestmark = pytest.mark.postgres

_DDL_MARKERS = ("CREATE ", "ALTER ", "GRANT ", "REVOKE ", "DO $")


class _RecordingCursor:
    """Cursor proxy that records every SQL string passed to execute()."""

    def __init__(self, cursor, statements: list[str]) -> None:
        self._cursor = cursor
        self._statements = statements

    def execute(self, query, params=None):
        self._statements.append(str(query))
        return (
            self._cursor.execute(query)
            if params is None
            else self._cursor.execute(query, params)
        )

    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._cursor.__exit__(*exc)
        return False


def _fresh_conn(dsn: str):
    return psycopg.connect(dsn)


def _ensure(conn) -> list[str]:
    """Run the ensure exactly as require_ledger_connection does, recording."""
    statements: list[str] = []
    with conn.cursor() as raw:
        cur = _RecordingCursor(raw, statements)
        d.ensure_ledger_schema(cur)
    conn.commit()
    return statements


def _drop_ledger_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"DROP TABLE IF EXISTS {d.COORDINATION_EVENT_TABLE} CASCADE"
        )
        cur.execute(f"DROP TABLE IF EXISTS {d.LEDGER_TABLE} CASCADE")
        cur.execute(f"DROP TABLE IF EXISTS {d.INTENT_TABLE} CASCADE")
    conn.commit()


def test_second_ensure_executes_zero_ddl(okg_migrated: str) -> None:
    with _fresh_conn(okg_migrated) as conn:
        _drop_ledger_schema(conn)
        first = _ensure(conn)
        assert any(
            m in stmt for stmt in first for m in _DDL_MARKERS
        ), "bootstrap ensure must create the schema"
        second = _ensure(conn)
        ddl = [
            stmt for stmt in second
            if any(m in stmt for m in _DDL_MARKERS)
        ]
        assert ddl == [], (
            "steady-state ledger ensure must take no DDL locks; "
            f"executed: {ddl}"
        )


def test_missing_column_still_repairs(okg_migrated: str) -> None:
    with _fresh_conn(okg_migrated) as conn:
        _drop_ledger_schema(conn)
        _ensure(conn)
        with conn.cursor() as cur:
            cur.execute(f"ALTER TABLE {d.INTENT_TABLE} DROP COLUMN session_id")
        conn.commit()
        repaired = _ensure(conn)
        assert any("ALTER" in stmt or "CREATE" in stmt for stmt in repaired)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pg_attribute "
                f"WHERE attrelid = to_regclass('{d.INTENT_TABLE}') "
                "  AND attname = 'session_id' AND NOT attisdropped"
            )
            assert cur.fetchone()[0] == 1


def test_missing_table_still_bootstraps(okg_migrated: str) -> None:
    with _fresh_conn(okg_migrated) as conn:
        _drop_ledger_schema(conn)
        with conn.cursor() as cur:
            assert d._ledger_schema_current(cur) is False
        _ensure(conn)
        with conn.cursor() as cur:
            assert d._ledger_schema_current(cur) is True


def test_missing_writer_grants_still_repair(okg_migrated: str) -> None:
    with _fresh_conn(okg_migrated) as conn:
        _drop_ledger_schema(conn)
        _ensure(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS ("
                "SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp'"
                ")"
            )
            assert cur.fetchone()[0], "migrations must provision okg_mcp"
            cur.execute(
                f"REVOKE SELECT ON {d.COORDINATION_EVENT_TABLE} FROM okg_mcp"
            )
            cur.execute(
                "REVOKE ALL ON SEQUENCE "
                "okg.pact_coordination_events_id_seq FROM okg_mcp"
            )
            assert d._ledger_schema_current(cur) is False
        conn.commit()

        repaired = _ensure(conn)
        assert any(
            marker in statement
            for statement in repaired
            for marker in _DDL_MARKERS
        )
        # The repair is a GRANT, not a table-DDL replay: re-running CREATE /
        # ALTER TABLE here would take ACCESS EXCLUSIVE on a live ledger, which
        # is the failure this module exists to prevent.
        assert not any(
            "CREATE TABLE" in statement or "ALTER TABLE" in statement
            for statement in repaired
        ), repaired
        with conn.cursor() as cur:
            cur.execute(
                "SELECT "
                "has_table_privilege("
                "  'okg_mcp', 'okg.pact_coordination_events', 'SELECT'"
                "), "
                "has_sequence_privilege("
                "  'okg_mcp', 'okg.pact_coordination_events_id_seq', 'USAGE'"
                "), "
                "has_sequence_privilege("
                "  'okg_mcp', 'okg.pact_coordination_events_id_seq', 'SELECT'"
                ")"
            )
            assert cur.fetchone() == (True, True, True)
            cur.execute("SET LOCAL ROLE okg_mcp")
            cur.execute(
                f"INSERT INTO {d.COORDINATION_EVENT_TABLE} "
                "(event_type, change_id) VALUES ('test', 'grant-repair') "
                "RETURNING id, occurred_at"
            )
            assert cur.fetchone() is not None
            cur.execute("RESET ROLE")
