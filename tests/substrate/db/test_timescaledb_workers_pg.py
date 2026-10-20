"""The TimescaleDB worker check on a real server and through readiness (#3143).

Needs a test database with TimescaleDB (the ops/pg image installs it in
template1). Failure cases change only what each test undoes: inserted history
rows are rolled back, and the login role is dropped.
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from okg.substrate.db import timescaledb_workers as tw
from tests.substrate.conftest import require_okg_test_dsn

pytestmark = pytest.mark.postgres

CHECK_ID = "database.timescaledb_workers"


@pytest.fixture
def dsn() -> str:
    return require_okg_test_dsn()


def _require_timescaledb(conn: psycopg.Connection) -> None:
    if not conn.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')"
    ).fetchone()[0]:
        pytest.fail("the test database has no TimescaleDB; use the ops/pg image")


def test_the_probe_matches_the_server(dsn: str) -> None:
    # One transaction: pg_stat_activity is read from one snapshot per
    # transaction, so the probe and the comparison see the same sessions.
    with psycopg.connect(dsn) as conn:
        _require_timescaledb(conn)
        level, message, details = tw.timescaledb_worker_health(conn)
        live = conn.execute(
            "SELECT current_setting('max_worker_processes')::int, "
            "current_setting('max_parallel_workers')::int, "
            "current_setting('timescaledb.max_background_workers')::int, "
            "(SELECT count(*) FROM pg_stat_activity WHERE backend_type IN ("
            "'TimescaleDB Background Worker Launcher', "
            "'logical replication launcher', 'pg_cron launcher')), "
            "(SELECT count(*) FROM pg_stat_activity WHERE backend_type = "
            "'TimescaleDB Background Worker Scheduler')"
        ).fetchone()
    assert details["status"] in {"ok", "warning"}, message
    assert (
        details["max_worker_processes"],
        details["max_parallel_workers"],
        details["timescaledb_max_background_workers"],
    ) == live[:3]
    # As a superuser the running launchers are visible: the settings-derived
    # count must equal what the server really runs.
    assert details["launchers"] == live[3]
    assert details["running_schedulers"] == live[4]
    assert details["history_bytes"] > 0
    assert level in {"pass", "warning"}


def test_recent_failed_runs_are_counted(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        _require_timescaledb(conn)
        try:
            job_id = conn.execute(
                "SELECT min(job_id) FROM timescaledb_information.jobs"
            ).fetchone()[0]
            assert job_id is not None, "the test database schedules no job"
            before = tw.timescaledb_worker_health(conn)[2]
            # Failed runs count; successful runs and runs older than an hour
            # do not.
            for succeeded, age, rows in (
                (False, "0 minutes", tw.FAILED_RUNS_WARN_PER_HOUR),
                (True, "0 minutes", 5),
                (False, "2 hours", 5),
            ):
                conn.execute(
                    "INSERT INTO _timescaledb_internal.bgw_job_stat_history "
                    "(job_id, execution_start, execution_finish, succeeded, data) "
                    "SELECT %s, now() - %s::interval, now(), %s, '{}' "
                    "FROM generate_series(1, %s)",
                    (job_id, age, succeeded, rows),
                )
            level, message, after = tw.timescaledb_worker_health(conn)
        finally:
            conn.rollback()
    assert (
        after["failed_runs_last_hour"] - before["failed_runs_last_hour"]
        == tw.FAILED_RUNS_WARN_PER_HOUR
    )
    assert level == "warning"
    assert "job runs failed in the last hour" in message


def test_a_plain_login_role_gets_unreadable_not_a_zero(dsn: str) -> None:
    role = f"okg_test_tw_{uuid.uuid4().hex[:8]}"
    password = uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        _require_timescaledb(admin)
        admin.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(role), sql.Literal(password),
            )
        )
        try:
            plain_dsn = make_conninfo(dsn, user=role, password=password)
            with psycopg.connect(plain_dsn, autocommit=True) as plain:
                level, message, details = tw.timescaledb_worker_health(plain)
        finally:
            admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
    assert (level, details["status"]) == ("warning", "unreadable")
    assert details["launchers"] is None
    assert details["running_schedulers"] is None
    assert details["failed_runs_last_hour"] is None
    # What a plain role can read is still reported.
    assert details["max_worker_processes"] > 0
    assert details["history_bytes"] > 0
    assert "shared_preload_libraries" in message
    assert message.count("pg_monitor") == 2


def test_databases_without_timescaledb_do_not_count(dsn: str) -> None:
    names = [f"okg_test_tw_bare_{uuid.uuid4().hex[:8]}" for _ in range(2)]
    with psycopg.connect(dsn, autocommit=True) as admin:
        _require_timescaledb(admin)
        try:
            for name in names:
                admin.execute(
                    sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(
                        sql.Identifier(name)
                    )
                )
            with psycopg.connect(dsn) as conn:  # one stats snapshot
                details = tw.timescaledb_worker_health(conn)[2]
                schedulers = conn.execute(
                    "SELECT count(*), count(*) FILTER (WHERE datname = ANY(%s)) "
                    "FROM pg_stat_activity WHERE backend_type = "
                    "'TimescaleDB Background Worker Scheduler'",
                    (names,),
                ).fetchone()
                databases = conn.execute(
                    "SELECT count(*) FROM pg_database WHERE NOT datistemplate"
                ).fetchone()[0]
        finally:
            for name in names:
                admin.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(name))
                )
    # The bare databases run no scheduler, and the check counts schedulers,
    # not databases.
    assert schedulers[1] == 0
    assert details["running_schedulers"] == schedulers[0] < databases
    capped = details["running_schedulers"] + 1 >= details["timescaledb_max_background_workers"]
    assert any("schedulers are running" in p for p in details["problems"]) == capped


SCHEDULER_SQL = (
    "SELECT count(*) FILTER (WHERE datname = %s), count(*) "
    "FROM pg_stat_activity WHERE backend_type = "
    "'TimescaleDB Background Worker Scheduler'"
)


def test_a_database_whose_scheduler_is_not_running_is_flagged(dsn: str) -> None:
    """timescaledb.restoring = on (the documented restore hook) keeps one
    database's scheduler from running while other databases' schedulers run."""
    name = f"okg_test_tw_nosched_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        _require_timescaledb(admin)
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            admin.execute(
                sql.SQL("ALTER DATABASE {} SET timescaledb.restoring = on").format(
                    sql.Identifier(name)
                )
            )
            for _ in range(50):
                admin.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = %s AND backend_type = "
                    "'TimescaleDB Background Worker Scheduler'",
                    (name,),
                )
                time.sleep(0.2)
                if admin.execute(SCHEDULER_SQL, (name,)).fetchone()[0] == 0:
                    break
            with psycopg.connect(make_conninfo(dsn, dbname=name)) as conn:
                level, message, details = tw.timescaledb_worker_health(conn)
                own, everywhere = conn.execute(SCHEDULER_SQL, (name,)).fetchone()
                conn.rollback()
        finally:
            admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(name)
                )
            )
    assert (own, details["own_scheduler_running"]) == (0, False)
    # Other databases' schedulers are running, so "any scheduler" would not
    # have been enough to pass.
    assert everywhere >= 1
    assert level == "warning"
    assert "no running scheduler" in message


# --- through the readiness entry point -----------------------------------------


def _deployment(root: Path) -> Path:
    import yaml

    deployment_dir = root / "tw-demo"
    (deployment_dir / "schemas").mkdir(parents=True)
    (deployment_dir / "source_registry.yaml").write_text("sources: {}\n")
    (deployment_dir / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "tw-demo",
        "description": "TimescaleDB worker check fixture",
        "postgres": {"dsn": "${TW_DEMO_DSN}"},
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "modules": [],
        "enrichers": [],
    }))
    return deployment_dir


def _ready(clean_runtime: str, deployment_dir: Path) -> dict:
    from okg.substrate.cli.deployment import _deployment_ready_live_checks
    from okg.substrate.cli.deployment_readiness import ReadinessDeadline

    return _deployment_ready_live_checks(
        deployment="tw-demo",
        deployment_dir=deployment_dir,
        dsn=clean_runtime,
        max_active_queue_depth=0,
        max_source_age_seconds=86400,
        readiness_profile="general",
        skip_doctor=True,
        deadline=ReadinessDeadline(total_seconds=120.0, per_probe_seconds=10.0),
    )


def _verdict(live: dict, *, without: str | None = None) -> tuple[object, object]:
    from okg.substrate.cli.deployment import _ready_finalize_payload_state

    def keep(rows: list) -> list:
        return [row for row in rows if row.get("id") != without]

    payload = {
        key: keep(list(live.get(key) or []))
        for key in ("checks", "blockers", "warnings", "unknowns")
    }
    _ready_finalize_payload_state(payload)
    return payload["ok"], payload["status"]


@pytest.mark.parametrize("forced", [None, "warning"])
def test_readiness_reports_the_check_as_a_warning_only(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    forced: str | None,
) -> None:
    if forced is not None:
        monkeypatch.setattr(
            tw, "timescaledb_worker_health",
            lambda conn: ("warning", "forced by the test", {"status": "unreadable"}),
        )
    live = _ready(clean_runtime, _deployment(tmp_path))
    rows = [row for row in live["checks"] if row.get("id") == CHECK_ID]
    assert len(rows) == 1, [row.get("id") for row in live["checks"]]
    row = rows[0]
    assert row["status"] in {"pass", "warning"}
    assert row not in (live.get("blockers") or [])
    assert live["database"]["timescaledb_workers"] == row["details"]
    if forced is not None:
        assert row["status"] == "warning"
        assert row in live["warnings"]
        assert "forced by the test" in row["message"]
    # The check never changes readiness's own verdict.
    assert _verdict(live) == _verdict(live, without=CHECK_ID)
