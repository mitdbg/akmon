"""A database created from ``template1`` carries the substrate extensions.

Issue #1972. The contributor recipe in ``docs/develop/index.md`` starts the
``ops/pg`` image with ``ops/pg/initdb`` mounted. Those scripts used to create
the extensions only in the ``okg`` database, so every scratch database a test
made with a bare ``CREATE DATABASE`` -- which copies ``template1`` -- had only
``plpgsql``. The first BM25 index the migration creates then failed with
``access method "bm25" does not exist``; 29 errors in
``tests/substrate/runtime`` on 2026-09-18.

``ops/pg/initdb/01-template1-extensions.sql`` now creates them in
``template1`` as well. ``pg_cron`` is excluded on purpose: it can live in
exactly one database per instance (the one ``cron.database_name`` names), and
``CREATE EXTENSION pg_cron`` anywhere else raises
``can only create extension in database okg``.

The scripts' own shape is guarded without a database in
``tests/docs/test_ops_pg_initdb_contract.py``, which the fast lane runs on
every pull request.
"""

from __future__ import annotations

from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from tests.substrate.conftest import dsn_with_dbname, require_okg_test_dsn

pytestmark = [pytest.mark.postgres, pytest.mark.serial_db]

#: Everything the substrate needs in a freshly created database. ``pg_cron``
#: is instance-scoped and deliberately not here.
TEMPLATE1_EXTENSIONS = frozenset(
    {
        "btree_gist",
        "fuzzystrmatch",
        "pg_textsearch",
        "pg_trgm",
        "timescaledb",
        "vector",
    }
)


def test_database_created_from_template1_has_the_substrate_extensions() -> None:
    """The real proof: make a scratch database the way the failing tests do.

    A bare ``CREATE DATABASE`` copies ``template1``. This asserts the copy
    carries the extensions AND that the ``bm25`` access method the migration
    needs resolves -- that access method is what the 29 errors named.
    """

    maintenance = dsn_with_dbname(require_okg_test_dsn(), "postgres")
    name = "okg_test_template1_" + uuid4().hex[:12]
    try:
        with psycopg.connect(maintenance, autocommit=True, connect_timeout=10) as admin:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        scratch = dsn_with_dbname(maintenance, name)
        with psycopg.connect(scratch, connect_timeout=10) as conn:
            installed = {
                row[0]
                for row in conn.execute("SELECT extname FROM pg_extension").fetchall()
            }
            access_methods = {
                row[0]
                for row in conn.execute("SELECT amname FROM pg_am").fetchall()
            }
    finally:
        with psycopg.connect(maintenance, autocommit=True, connect_timeout=10) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(name)
                )
            )

    missing = set(TEMPLATE1_EXTENSIONS) - installed
    assert not missing, (
        f"database created from template1 is missing {sorted(missing)}; "
        f"start the ops/pg image with ops/pg/initdb mounted so "
        f"01-template1-extensions.sql runs"
    )
    assert "bm25" in access_methods, (
        "the bm25 access method (pg_textsearch) must resolve in a database "
        "copied from template1; its absence is the reported failure"
    )
    assert "pg_cron" not in installed, (
        "pg_cron must NOT be inherited from template1: it is valid only in "
        "the database cron.database_name names"
    )
