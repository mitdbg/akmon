"""Migration 0043_attr_index_teardown (okg#3112 step 4b, PACT lane-h-3112-step4b).

Native PostgreSQL. A fresh install (which still runs 0021 and 0036, then 0043)
carries none of the five objects. A database at the state before 0043 -- the
four tables re-created by the bodies of 0021 and 0036 and seeded with rows,
plus ``okg.attr_index_try_ts`` -- loses all five when the 0043 body runs, and
the census raises one WARNING per table naming its estimate, its size and that
it held rows. A second run changes nothing. Failure case: while another
session holds a lock on ``okg.attr_index``, 0043 stops on its 5 s lock
timeout and leaves every object in place.
"""
from __future__ import annotations

import importlib.resources
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")

pytestmark = pytest.mark.postgres

MIGRATION_ID = "0043_attr_index_teardown"
TABLES = (
    "okg.attr_index",
    "okg.attr_index_coverage",
    "okg.attr_index_rebuild_requests",
    "okg.attr_index_derive_front",
)
FUNCTION = "okg.attr_index_try_ts(text)"


def _migration(name: str) -> str:
    return (
        importlib.resources.files("okg.substrate.db")
        .joinpath(f"migrations/{name}.sql")
        .read_text()
    )


def _present(conn: Any) -> dict[str, bool]:
    out = {
        rel: conn.execute("SELECT to_regclass(%s) IS NOT NULL", (rel,)).fetchone()[0]
        for rel in TABLES
    }
    out[FUNCTION] = conn.execute(
        "SELECT to_regprocedure(%s) IS NOT NULL", (FUNCTION,)
    ).fetchone()[0]
    return out


def _run_in_transaction(conn: Any, body: str) -> list[str]:
    """Run a migration body the way the runner does (one transaction, so its
    SET LOCALs apply) and return the WARNINGs it raised."""
    warnings: list[str] = []

    def handler(diag: Any) -> None:
        if diag.severity_nonlocalized == "WARNING":
            warnings.append(diag.message_primary or "")

    conn.add_notice_handler(handler)
    try:
        with conn.transaction():
            conn.execute(body)
    finally:
        conn.remove_notice_handler(handler)
    return warnings


def _restore_pre_0043_state(conn: Any) -> None:
    """The objects 0021 and 0036 author, each seeded with a row."""
    for name in ("0021_attr_filter_index", "0036_attr_index_coverage"):
        _run_in_transaction(conn, _migration(name))
    conn.execute(
        "INSERT INTO okg.attr_index (attr_id, branch_id, namespace, target_kind, "
        "target_id, from_ord, to_ord, value_text) "
        "VALUES ('path', 'default', 'default', 'node', 'doc:1', 0, "
        "9223372036854775807, 'a.py')"
    )
    conn.execute(
        "INSERT INTO okg.attr_index_coverage (branch_name, target_kind, type_name, "
        "attr_key, declaration_digest, covered_from_ordinal, covered_through_ordinal) "
        "VALUES ('default', 'node', 'doc', 'path', 'sha256:x', 0, 0)"
    )
    conn.execute(
        "INSERT INTO okg.attr_index_rebuild_requests (branch_name, attr_key, reason) "
        "VALUES ('default', 'path', 'okg#3112 step 4b test')"
    )
    conn.execute(
        "INSERT INTO okg.attr_index_derive_front (branch_name, derived_through_ordinal) "
        "VALUES ('default', 0)"
    )
    conn.execute("ANALYZE okg.attr_index")


def test_clone_still_purges_the_four_dropped_tables_and_refuses_an_unlisted_one() -> None:
    """A source database at 0036-0042 still has the four tables; the clone
    allowlist fails closed on an unlisted okg table, so the names stay as
    purge-if-present (ruling 10-04). Failure case: a name not on the list is
    ``unknown``, which refuses the whole clone."""
    from okg.substrate.environment_clone import clone_table_disposition

    for rel in TABLES:
        assert clone_table_disposition("okg", rel.split(".", 1)[1]) == "purge", rel
    assert clone_table_disposition("okg", "attr_index_not_a_real_table") == "unknown"


def test_a_fresh_install_has_none_of_the_objects_and_records_0043(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _present(conn) == {name: False for name in (*TABLES, FUNCTION)}
        recorded = conn.execute(
            "SELECT count(*) FROM okg.schema_migrations WHERE migration_id = %s",
            (MIGRATION_ID,),
        ).fetchone()[0]
    assert recorded == 1


def test_0043_drops_seeded_tables_and_the_function_and_warns_per_table(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _restore_pre_0043_state(conn)
        assert _present(conn) == {name: True for name in (*TABLES, FUNCTION)}
        for rel in TABLES:
            assert conn.execute(f"SELECT count(*) FROM {rel}").fetchone()[0] == 1, rel

        warnings = _run_in_transaction(conn, _migration(MIGRATION_ID))

        assert _present(conn) == {name: False for name in (*TABLES, FUNCTION)}
        census = [w for w in warnings if "okg#3112 step 4b drops" in w]
        assert len(census) == len(TABLES), warnings
        for rel in TABLES:
            (line,) = [w for w in census if f"drops {rel}:" in w]
            assert "reltuples estimate" in line and "total bytes" in line, line
            assert line.endswith("holds rows true"), line

        # A second run is a no-op: nothing to drop, each table named absent.
        again = _run_in_transaction(conn, _migration(MIGRATION_ID))
        assert sorted(again) == sorted(
            f"okg#3112 step 4b: {rel} is absent; nothing to drop" for rel in TABLES
        )
        assert _present(conn) == {name: False for name in (*TABLES, FUNCTION)}


def test_0043_stops_on_its_lock_timeout_and_changes_nothing(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _restore_pre_0043_state(conn)
    holder = psycopg.connect(clean_runtime)
    try:
        # A step-3 reader: a transaction that has read okg.attr_index.
        holder.execute("SELECT count(*) FROM okg.attr_index").fetchone()
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            with pytest.raises(psycopg.errors.LockNotAvailable):
                _run_in_transaction(conn, _migration(MIGRATION_ID))
            assert _present(conn) == {name: True for name in (*TABLES, FUNCTION)}
    finally:
        holder.rollback()
        holder.close()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _run_in_transaction(conn, _migration(MIGRATION_ID))
        assert _present(conn) == {name: False for name in (*TABLES, FUNCTION)}
