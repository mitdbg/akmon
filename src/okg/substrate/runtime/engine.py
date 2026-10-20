"""DBOS singleton lifecycle helpers — construct, launch, destroy.

Per Decision 10, OKG runs one DBOS singleton per worker process per
deployment. Per Decision 11, both ``system_database_url`` and
``database_url`` point to the same deployment DSN. Per `sdk-design.md`
§4.10, pool sizing is per-deployment-database with substrate-side
read/write/dbos pools.

This module owns the construction of the :class:`DBOSConfig` from a
deployment manifest's :class:`RuntimeConfig`. It does not own the
worker-process loop itself — see :mod:`okg.substrate.runtime.worker`
for that (Phase 1 §2.4).
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .config import RuntimeConfig
from .source_sync_constants import SOURCE_SYNC_APPLICATION_VERSION

if TYPE_CHECKING:
    from dbos import DBOSConfig


__all__ = [
    "build_dbos_config",
    "defensive_destroy",
    "release_connection_demand",
    "with_dbos_singleton",
]


def _split_dbos_pool_budget(total: int) -> tuple[int, int]:
    """Split one role-level budget across DBOS's two independent pools."""
    if total < 2:
        raise ValueError(
            "runtime.dbos_pool_size is a combined DBOS connection budget "
            "and must be at least 2"
        )
    system_pool_size = total // 2
    application_pool_size = total - system_pool_size
    return system_pool_size, application_pool_size


def defensive_destroy() -> None:
    """``DBOS.destroy()`` with the broad-catch idiom that lets a
    pre-construction call succeed regardless of whether a prior
    singleton exists. Used everywhere the runtime needs to clear
    a possibly-stale process-global singleton before constructing
    a fresh one (most common during pytest reuse, but also during
    `dev.reset_dbos` and `recovery_test.*` round-trips).

    Replaces the 5-place ``try: DBOS.destroy() / except Exception:
    pass`` pattern previously duplicated in worker.py, engine.py,
    dev.py, and recovery_test.py.
    """
    try:
        from dbos import DBOS
        DBOS.destroy()
    except Exception:  # noqa: BLE001 — defensive; the prior singleton may not exist
        pass


log = logging.getLogger(__name__)


def _export_chronos_widths() -> None:
    """Publish the chronos executor widths into the environment chronos reads.

    ``chronos_core`` is deliberately independent of ``okg.substrate`` and sizes
    its executors from ``CHRONOS_*_WORKERS``. Rather than invert that
    dependency, the registry stays the source of truth and this exports the
    resolved widths at boot, so the chronos executors are counted in the same
    budget as everything else without chronos importing the substrate.

    An operator-set environment value already wins inside ``width()``, so
    exporting the resolved value back is idempotent rather than overriding.
    """
    import os

    from okg.substrate.db.concurrency_budget import term, width

    for name in ("chronos.schema_index", "chronos.interval_gc"):
        env_var = term(name).env_var
        if env_var:
            os.environ[env_var] = str(width(name))


def _admit_connection_demand(dsn: str, deployment: str) -> None:
    """Refuse to boot a worker whose declared widths cannot fit the server.

    Runs after the manifest widths are applied and before any pool is opened,
    so an over-subscribed deployment is named at boot instead of surfacing as
    a PoolTimeout mid-publish.

    A BudgetError or post-insert ReservationLifecycleError propagates — that
    is the fail-closed contract. Any pre-acquisition infrastructure failure
    (server unreachable, insufficient privileges to read
    pg_stat_activity) is logged and allowed through: the admission check is a
    guard against our own over-declaration, not a second liveness probe, and
    refusing to boot because the check itself could not run would convert a
    diagnostic into an outage. The worker's real connection attempts fail
    loudly a moment later if the server is genuinely unreachable. Once a row
    is recorded, failure to start its heartbeat owner is never downgraded.
    """
    from okg.substrate.db.capacity import (
        ReservationLifecycleError,
        admit_process_demand,
        reserve_process_demand,
    )
    from okg.substrate.db.concurrency_budget import BudgetError

    try:
        admit_process_demand(dsn)
        # Then claim it host-wide. The check above asks "does my demand fit
        # what is free"; this asks "does it fit alongside every other process
        # that has already claimed", which is the question two simultaneous
        # boots get wrong.
        reserve_process_demand(
            dsn, application_name=f"okg-worker-{deployment}",
            deployment=deployment,
        )
    except (BudgetError, ReservationLifecycleError):
        raise
    except Exception as exc:  # noqa: BLE001 — see docstring
        log.warning(
            "connection admission check could not run (%s): booting without "
            "it. This does NOT mean the declared widths fit.", exc,
        )


def release_connection_demand(deployment: str) -> None:
    """Release the runtime worker reservation acquired during config build."""
    from okg.substrate.db.capacity import release_process_reservations

    release_process_reservations(
        application_name=f"okg-worker-{deployment}",
    )


def build_dbos_config(
    *,
    deployment: str,
    dsn: str,
    runtime: RuntimeConfig,
) -> "DBOSConfig":
    """Construct a :class:`DBOSConfig` for a deployment.

    Per Decision 11 both ``system_database_url`` and ``database_url``
    are set to ``dsn`` — atomicity between DBOS step checkpoints and
    OKG fact writes via ``@DBOS.transaction`` requires this.

    Per `sdk-design.md` §4.10 DBOS has a combined budget of 6 connections
    per deployment-database, overridable in the manifest's
    ``runtime.dbos_pool_size``. That value, and the OKG-side shared pool
    size, are both pushed onto the concurrency registry here so the
    process's total connection demand is computable from one place
    rather than reconstructed by reading five files.

    DBOS owns a system-database engine and an application-database engine
    even when both URLs point at the same database, so the role-level budget
    is split across them and both are configured explicitly. Left alone DBOS
    defaults each engine to 20 connections, which defeats the deployment
    budget entirely. The OKG-side pools (read 8, write 4) are not configured
    here — they live with the substrate's existing psycopg pool plumbing.
    """
    from dbos import DBOSConfig

    # DBOS app name is namespaced by deployment so multiple OKG
    # deployments on one Postgres instance can be distinguished in
    # the workflow_status executor_id column.
    app_name = f"okg-{deployment}"
    system_pool_size, application_pool_size = _split_dbos_pool_budget(
        runtime.dbos_pool_size
    )

    # Push the manifest's declared widths onto the concurrency registry before
    # anything constructs a pool. Doing it here means a bad width fails at
    # boot, by name, rather than as a PoolTimeout mid-publish attributed to
    # whichever lane happened to ask last.
    runtime.apply_to_concurrency_budget()
    _export_chronos_widths()
    _admit_connection_demand(dsn, deployment)

    # The DBOS admin/health HTTP server binds one TCP port per worker
    # process. When several deployments run runtime workers on one host
    # they must use distinct admin ports (or disable the server) — else
    # the second worker to boot fails with `Errno 48 Address already in
    # use`. `admin_port=None` lets DBOS use its built-in default (3001),
    # which is fine for a single co-located worker. See `runtime.admin_port`
    # / `runtime.run_admin_server` in config.py.
    cfg = DBOSConfig(
        name=app_name,
        system_database_url=dsn,
        database_url=dsn,
        application_version=SOURCE_SYNC_APPLICATION_VERSION,
        # Name the system schema explicitly so DBOS and okg's own readers of
        # the DBOS tables cannot drift apart; `runtime.dbos_schema` is
        # validated to equal `DBOS_SYSTEM_SCHEMA` at manifest load.
        dbos_system_schema=runtime.dbos_schema,
        # DBOS 2.x names these `sys_db_pool_size` and `db_engine_kwargs`.
        # There is no `app_db_pool_size` key in DBOSConfig (verified against
        # dbos 2.26.0), so passing one applies nothing and leaves BOTH engines
        # at their 20-connection defaults -- while the log line below still
        # reports the split this function computed. See `sdk-design.md` §4.10
        # for the budget math against Postgres max_connections.
        sys_db_pool_size=system_pool_size,
        db_engine_kwargs={
            "pool_size": application_pool_size,
            "max_overflow": 0,
        },
        run_admin_server=runtime.run_admin_server,
    )
    if runtime.admin_port is not None:
        cfg["admin_port"] = runtime.admin_port

    log.info(
        "DBOSConfig built: deployment=%s system_pool=%d app_pool=%d schema=%s "
        "sdk_version=%s admin_port=%s run_admin_server=%s",
        deployment,
        system_pool_size,
        application_pool_size,
        runtime.dbos_schema,
        runtime.sdk_version,
        runtime.admin_port,
        runtime.run_admin_server,
    )
    return cfg


def with_dbos_singleton(
    *,
    deployment: str,
    dsn: str,
    runtime: RuntimeConfig,
):
    """Context manager that constructs, launches, and destroys a DBOS
    singleton.

    Use only for short-lived processes (smoke tests, ad-hoc operator
    commands). Long-lived workers should use the worker entry point
    in :mod:`okg.substrate.runtime.worker` which lives for the
    duration of the process.

    Yields nothing — DBOS is a process-global singleton, accessed via
    the :class:`DBOS` class methods after :meth:`DBOS.launch` returns.
    """
    from contextlib import contextmanager
    from dbos import DBOS

    @contextmanager
    def _ctx():
        # Defensive destroy in case a previous singleton lingers
        # (most common during pytest reuse).
        defensive_destroy()

        try:
            cfg = build_dbos_config(
                deployment=deployment,
                dsn=dsn,
                runtime=runtime,
            )
            DBOS(config=cfg)
            DBOS.launch()
            yield
        finally:
            try:
                DBOS.destroy()
            finally:
                release_connection_demand(deployment)

    return _ctx()
