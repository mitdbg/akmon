"""Build a real DBOS system schema in a test database.

Tests that assert against `dbos.workflow_status` used to hand-write a
`CREATE TABLE` with the handful of columns the assertion touched. That stub
silently drifts behind the pinned `dbos` release: the runtime reads the queue
through `DBOSClient.list_queued_workflows`, which SELECTs *every* column the
installed SDK declares, so a dependency bump that adds a column (2.26.0 has 35)
turns into an `UndefinedColumn` error inside whatever phase happened to drain
the queue.

So don't hand-write the schema — run DBOS's own migrations, the same ones the
worker runs when it boots against a fresh deployment database. The fixture is
then the real schema by construction, including the server defaults
(`created_at`, `updated_at`, `priority`, `recovery_attempts`) that the
SQLAlchemy `SystemSchema` table objects do not carry, the
`(queue_name, deduplication_id)` unique constraint, and the foreign keys.

`sqlalchemy` is imported below without being a declared dependency of this
repo. It is guaranteed present wherever this helper can run: `dbos` requires
it, and `dbos._migration` — which this helper calls — imports it itself.
"""
from __future__ import annotations

__all__ = ["create_dbos_system_schema"]

#: The worker is what creates the DBOS schema in a real deployment, and
#: `build_dbos_config` leaves `use_listen_notify` at its `True` default, so the
#: production schema carries the LISTEN/NOTIFY triggers. This is deliberately
#: NOT a parameter: `False` produces the same 35 columns and 11 tables *without*
#: those triggers while still stamping `dbos_migrations.version`, so DBOS's own
#: `should_migrate` would never repair the difference. A fixture that could be
#: built either way is a fixture that can silently diverge from production.
_PRODUCTION_USE_LISTEN_NOTIFY = True


def create_dbos_system_schema(dsn: str, *, schema: str = "dbos") -> None:
    """Drop and rebuild the DBOS system schema at ``schema`` in ``dsn``.

    Always drops first, which is what a per-test fixture wants: the shared
    PostgreSQL fixtures reset public substrate state but DBOS owns a separate
    schema, so a test that seeds workflow rows must start from a known-empty
    one to stay order-independent.
    """
    import sqlalchemy as sa
    from dbos._migration import (  # type: ignore[import-untyped]
        ensure_dbos_schema,
        run_dbos_migrations,
    )

    # DBOS builds its own engine this way (`_sys_db_postgres._create_engine`);
    # mirror it so the migrations run over the driver they were written for.
    url = sa.make_url(dsn).set(drivername="postgresql+psycopg")
    engine = sa.create_engine(url)
    try:
        with engine.begin() as conn:
            conn.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        ensure_dbos_schema(engine, schema)
        run_dbos_migrations(engine, schema, _PRODUCTION_USE_LISTEN_NOTIFY)
    finally:
        engine.dispose()
