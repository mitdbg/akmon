"""Warn when TimescaleDB can run out of background-worker slots (issue #3143).

Every background process comes from one pool of ``max_worker_processes``
slots. The pool must hold the launchers, up to ``max_parallel_workers``
parallel workers, and up to ``timescaledb.max_background_workers`` TimescaleDB
schedulers and jobs. When it cannot, job launches fail with ``failed to start
a background worker``; okg's jobs have ``max_retries = -1``, so they retry
forever and each failure adds a row to the job history. With PostgreSQL's
default of 8 slots, five okg databases looped that way until a disk filled.

The check reads only the deployment's own connection. It is a warning, never
a blocker. A part it cannot read is reported as unreadable, with the
privilege that would let it read it; the other parts are still judged.
"""

from __future__ import annotations

from typing import Any

SCHEMA = "okg.timescaledb_worker_health/v3"
RECOMMENDED_MAX_WORKER_PROCESSES = 32
FAILED_RUNS_WARN_PER_HOUR = 10
HISTORY_BYTES_WARN = 128 * 1024 * 1024
HISTORY_TABLE = "_timescaledb_internal.bgw_job_stat_history"
SCHEDULER = "TimescaleDB Background Worker Scheduler"
# pg_monitor includes both pg_read_all_settings and pg_read_all_stats.
PRIVILEGE_HINT = "grant pg_monitor to the readiness role"


def preloaded(shared_preload_libraries: str) -> set[str]:
    """Library names as PostgreSQL loads them: no quotes, directory or .so."""
    names = set()
    for entry in shared_preload_libraries.split(","):
        name = entry.strip().strip('"').rsplit("/", 1)[-1]
        names.add(name.removesuffix(".so"))
    return names - {""}


def launcher_count(shared_preload_libraries: str, max_logical_replication_workers: int) -> int:
    """Launchers that hold a slot for the life of the server."""
    libraries = preloaded(shared_preload_libraries)
    return (
        int("timescaledb" in libraries)
        + int("pg_cron" in libraries)
        # PostgreSQL registers its logical replication launcher unless
        # max_logical_replication_workers is 0.
        + int(max_logical_replication_workers > 0)
    )


def evaluate(
    *,
    max_worker_processes: int,
    max_parallel_workers: int,
    timescaledb_workers: int,
    launchers: int | None,
    schedulers: int | None,
    own_scheduler: bool | None,
    failed_runs_last_hour: int | None,
    history_bytes: int | None,
    unreadable: tuple[str, ...] = (),
) -> tuple[str, str, dict[str, Any]]:
    """Return the readiness row ``(level, message, details)``.

    ``None`` means the value was not read; its part is skipped and the reason
    is in ``unreadable``.
    """
    problems: list[str] = []
    required = headroom = None
    if launchers is not None:
        required = launchers + max_parallel_workers + timescaledb_workers
        headroom = max_worker_processes - required
        if headroom < 0:
            problems.append(
                f"max_worker_processes is {max_worker_processes} but "
                f"{required} slots can be needed ({launchers} launchers + "
                f"{max_parallel_workers} parallel workers + "
                f"{timescaledb_workers} TimescaleDB workers): TimescaleDB jobs "
                "fail to start and retry without limit; set it to at least "
                f"{required} (ops/pg uses {RECOMMENDED_MAX_WORKER_PROCESSES}) "
                "and restart PostgreSQL"
            )
    if schedulers is not None and schedulers + 1 >= timescaledb_workers:
        problems.append(
            f"{schedulers} TimescaleDB schedulers are running and "
            f"timescaledb.max_background_workers is {timescaledb_workers}, "
            "leaving no worker for jobs; raise it (and max_worker_processes "
            "with it) or move databases to another instance"
        )
    if own_scheduler is False:
        problems.append(
            "this database has TimescaleDB but no running scheduler, so its "
            "jobs never run"
        )
    if failed_runs_last_hour is not None and (
        failed_runs_last_hour >= FAILED_RUNS_WARN_PER_HOUR
    ):
        problems.append(
            f"{failed_runs_last_hour} TimescaleDB job runs failed in the "
            "last hour in this database"
        )
    if history_bytes is not None and history_bytes >= HISTORY_BYTES_WARN:
        problems.append(
            f"{HISTORY_TABLE} is {history_bytes:,} bytes; after fixing the "
            "cause, a superuser can DELETE its rows WHERE succeeded IS FALSE "
            "and VACUUM FULL it"
        )
    notes = [f"could not read {part}" for part in unreadable]
    details = {
        "schema": SCHEMA,
        "status": "warning" if problems else "unreadable" if notes else "ok",
        "max_worker_processes": max_worker_processes,
        "launchers": launchers,
        "max_parallel_workers": max_parallel_workers,
        "timescaledb_max_background_workers": timescaledb_workers,
        "required_worker_slots": required,
        "worker_slot_headroom": headroom,
        "running_schedulers": schedulers,
        "own_scheduler_running": own_scheduler,
        "failed_runs_last_hour": failed_runs_last_hour,
        "history_bytes": history_bytes,
        "problems": problems,
        "unreadable": notes,
    }
    if problems or notes:
        return "warning", "; ".join(problems + notes), details
    return "pass", "TimescaleDB has worker headroom and no failing jobs", details


def _why(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {(str(exc).splitlines() or [''])[0][:200]}"


def timescaledb_worker_health(conn: Any) -> tuple[str, str, dict[str, Any]]:
    """Read the deployment database and judge it; never raises."""
    unreadable: list[str] = []
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_extension "
                "WHERE extname = 'timescaledb')"
            )
            if not cur.fetchone()[0]:
                return "skipped", "the deployment database has no TimescaleDB", {
                    "schema": SCHEMA, "status": "not_applicable",
                }
            # Readable by every role.
            cur.execute(
                """
                SELECT current_setting('max_worker_processes')::int,
                       current_setting('max_parallel_workers')::int,
                       current_setting('timescaledb.max_background_workers')::int,
                       current_setting('max_logical_replication_workers')::int
                """
            )
            mwp, parallel, ts_workers, logical = cur.fetchone()
    except Exception as exc:  # noqa: BLE001 - reported as unreadable.
        message = f"TimescaleDB worker health could not be read: {_why(exc)}"
        return "warning", message, {
            "schema": SCHEMA, "status": "unreadable", "unreadable": [message],
        }

    launchers = schedulers = own = failed = size = None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_setting('shared_preload_libraries')")
            launchers = launcher_count(cur.fetchone()[0], logical)
    except Exception as exc:  # noqa: BLE001 - reported as unreadable.
        unreadable.append(
            f"shared_preload_libraries for the slot count ({_why(exc)}; "
            f"{PRIVILEGE_HINT})"
        )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER'), "
                "count(*) FILTER (WHERE backend_type = %(s)s), "
                "coalesce(bool_or(backend_type = %(s)s "
                "AND datname = current_database()), false) "
                "FROM pg_stat_activity",
                {"s": SCHEDULER},
            )
            visible, count, mine = cur.fetchone()
            if visible:
                schedulers, own = int(count), bool(mine)
            else:
                unreadable.append(
                    "the running TimescaleDB schedulers (this role cannot see "
                    f"other sessions; {PRIVILEGE_HINT})"
                )
    except Exception as exc:  # noqa: BLE001 - reported as unreadable.
        unreadable.append(f"the running TimescaleDB schedulers ({_why(exc)})")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT (SELECT pg_total_relation_size(c.oid) FROM pg_class c
                          JOIN pg_namespace n ON n.oid = c.relnamespace
                         WHERE n.nspname = '_timescaledb_internal'
                           AND c.relname = 'bgw_job_stat_history'),
                       pg_has_role(current_user, d.datdba, 'MEMBER')
                       OR NOT EXISTS (
                           SELECT 1 FROM timescaledb_information.jobs j
                            WHERE NOT pg_has_role(current_user, j.owner, 'MEMBER'))
                  FROM pg_database d
                 WHERE d.datname = current_database()
                """
            )
            size, sees_all = cur.fetchone()
            if size is None:
                unreadable.append(f"{HISTORY_TABLE} (not found)")
            if not sees_all:
                unreadable.append(
                    "failed job runs (this role cannot see every row of "
                    "timescaledb_information.job_history)"
                )
            else:
                cur.execute(
                    "SELECT count(*) FROM timescaledb_information.job_history "
                    "WHERE start_time > now() - interval '1 hour' "
                    "AND succeeded IS FALSE"
                )
                failed = int(cur.fetchone()[0])
    except Exception as exc:  # noqa: BLE001 - reported as unreadable.
        unreadable.append(f"the job history ({_why(exc)})")
    return evaluate(
        max_worker_processes=mwp,
        max_parallel_workers=parallel,
        timescaledb_workers=ts_workers,
        launchers=launchers,
        schedulers=schedulers,
        own_scheduler=own,
        failed_runs_last_hour=failed,
        history_bytes=None if size is None else int(size),
        unreadable=tuple(unreadable),
    )
