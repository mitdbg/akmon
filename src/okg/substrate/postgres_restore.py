"""Shared PostgreSQL extension restore guards."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from okg.substrate.db.connection import connect as db_connect
from psycopg import sql

from okg.substrate.errors import DeploymentEnvironmentInvalid


def timescaledb_extension_version(dsn: str) -> str:
    """Return the installed TimescaleDB version for a source database."""
    with db_connect(dsn, connect_timeout=5) as conn, conn.cursor() as cur:
        cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
        row = cur.fetchone()
    if row is None or not isinstance(row[0], str) or not row[0]:
        raise DeploymentEnvironmentInvalid(
            "source database does not have a TimescaleDB version"
        )
    return row[0]


@contextmanager
def timescaledb_restore_context(
    dsn: str,
    *,
    version: str,
) -> Iterator[None]:
    """Pin TimescaleDB to the source version and bracket a physical restore."""
    if not version:
        raise DeploymentEnvironmentInvalid(
            "TimescaleDB restore version is required"
        )
    with db_connect(
        dsn,
        autocommit=True,
        connect_timeout=5,
        options="-c default_transaction_read_only=off",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE EXTENSION IF NOT EXISTS timescaledb VERSION {}").format(
                    sql.Literal(version)
                )
            )
            cur.execute(
                "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
            )
            installed = cur.fetchone()
            if installed is None or installed[0] != version:
                raise DeploymentEnvironmentInvalid(
                    "TimescaleDB restore target version does not match the source",
                    data={
                        "expected": version,
                        "observed": installed[0] if installed else None,
                    },
                )
            cur.execute("SELECT timescaledb_pre_restore()")
    try:
        yield
    finally:
        with db_connect(
            dsn,
            autocommit=True,
            connect_timeout=5,
            options="-c default_transaction_read_only=off",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT timescaledb_post_restore()")
