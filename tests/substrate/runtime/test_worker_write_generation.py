"""Tests for the worker-write generation allocator (Phase 2 §1.2).

Verifies:
  - Allocation creates a graph_generations row with status='worker_write'.
  - The returned generation_id is strictly monotonic (each call gives
    a larger id than the previous).
  - The allocator shares the bigserial sequence with the publisher's
    reserve() flow, so a subsequent publisher reservation gets a
    strictly-greater id (no clash on the primary key).
  - Allocation requires an applied catalog_version_id.
  - The correlation_id is operator-friendly.

Marked ``dbos_integration`` because the allocator writes to real
Postgres via psycopg.
"""
from __future__ import annotations


import psycopg
import pytest
from tests.substrate.runtime._helpers import _swap_dbname


pytestmark = pytest.mark.dbos_integration


@pytest.fixture
def test_db_dsn(admin_dsn: str) -> str:
    """Create a per-test database with the minimum substrate state the
    allocator needs: okg schemas, graph_generations table,
    catalog_versions table with one applied row, btree_gist for the
    escalation_schema GIST exclusion."""
    db_name = "okg_ww_gen_test"
    dsn = _swap_dbname(admin_dsn, db_name)

    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) "
                "FROM pg_stat_activity WHERE datname = %s "
                "AND pid <> pg_backend_pid()",
                (db_name,),
            )
            cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
            cur.execute(f'CREATE DATABASE "{db_name}"')

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
            cur.execute("CREATE SCHEMA okg")
            cur.execute(
                """
                CREATE TABLE okg.catalog_versions (
                  catalog_version_id BIGSERIAL PRIMARY KEY,
                  status TEXT NOT NULL CHECK (status IN
                    ('applied','pending','retired')),
                  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                "INSERT INTO okg.catalog_versions (status) VALUES ('applied')"
            )
            cur.execute(
                """
                CREATE TABLE okg.graph_generations (
                  generation_id BIGSERIAL PRIMARY KEY,
                  deployment_name TEXT,
                  status TEXT NOT NULL
                    CHECK (status IN
                      ('pending','published','blocked','worker_write')),
                  fact_watermark BIGINT,
                  edge_fact_watermark BIGINT,
                  catalog_version_id BIGINT NOT NULL
                    REFERENCES okg.catalog_versions(catalog_version_id),
                  correlation_id TEXT NOT NULL,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )

    yield dsn

    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) "
                "FROM pg_stat_activity WHERE datname = %s "
                "AND pid <> pg_backend_pid()",
                (db_name,),
            )
            cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')


def test_allocate_returns_row_with_worker_write_status(test_db_dsn) -> None:
    from okg.substrate.runtime.generation import (
        WORKER_WRITE_STATUS,
        allocate_worker_write_generation,
    )

    with psycopg.connect(test_db_dsn) as conn:
        result = allocate_worker_write_generation(
            conn, deployment="test"
        )

    assert result.generation_id > 0
    assert result.deployment_name == "test"
    assert result.correlation_id.startswith("ww:test:")
    assert str(result.generation_id) in result.correlation_id

    with psycopg.connect(test_db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, deployment_name, catalog_version_id "
                "FROM okg.graph_generations "
                "WHERE generation_id = %s",
                (result.generation_id,),
            )
            row = cur.fetchone()

    assert row is not None
    status, dep, cv = row
    assert status == WORKER_WRITE_STATUS
    assert dep == "test"
    assert cv == result.catalog_version_id


def test_allocations_are_monotonic(test_db_dsn) -> None:
    """Two back-to-back allocations must produce strictly-increasing
    generation_ids."""
    from okg.substrate.runtime.generation import (
        allocate_worker_write_generation,
    )

    with psycopg.connect(test_db_dsn) as conn:
        a = allocate_worker_write_generation(conn, deployment="test")
        b = allocate_worker_write_generation(conn, deployment="test")

    assert b.generation_id > a.generation_id


def test_publisher_nextval_sees_strictly_greater_id(test_db_dsn) -> None:
    """After a worker-write allocation, a publisher-style nextval()
    call must produce a strictly-greater id. Both share the bigserial
    sequence, so the publisher's reservation never collides with a
    worker-write row's id."""
    from okg.substrate.runtime.generation import (
        allocate_worker_write_generation,
    )

    with psycopg.connect(test_db_dsn) as conn:
        ww = allocate_worker_write_generation(conn, deployment="test")

        with conn.cursor() as cur:
            cur.execute(
                "SELECT nextval(pg_get_serial_sequence("
                "  'okg.graph_generations', 'generation_id'))"
            )
            pub_next = cur.fetchone()[0]

    assert pub_next > ww.generation_id


def test_raises_without_applied_catalog(admin_dsn) -> None:
    """No applied catalog row → ValueError with a hint."""
    from okg.substrate.runtime.generation import (

        allocate_worker_write_generation,
    )

    db_name = "okg_ww_gen_test_nocat"
    dsn = _swap_dbname(admin_dsn, db_name)

    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
            cur.execute(f'CREATE DATABASE "{db_name}"')

    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA okg")
                cur.execute(
                    "CREATE TABLE okg.catalog_versions ("
                    "catalog_version_id BIGSERIAL PRIMARY KEY, "
                    "status TEXT NOT NULL)"
                )
                cur.execute(
                    """
                    CREATE TABLE okg.graph_generations (
                      generation_id BIGSERIAL PRIMARY KEY,
                      deployment_name TEXT,
                      status TEXT NOT NULL,
                      catalog_version_id BIGINT NOT NULL,
                      correlation_id TEXT NOT NULL,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )

        with psycopg.connect(dsn) as conn:
            with pytest.raises(ValueError, match="no applied catalog_version"):
                allocate_worker_write_generation(conn, deployment="test")
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_terminate_backend(pid) "
                    "FROM pg_stat_activity WHERE datname = %s "
                    "AND pid <> pg_backend_pid()",
                    (db_name,),
                )
                cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
