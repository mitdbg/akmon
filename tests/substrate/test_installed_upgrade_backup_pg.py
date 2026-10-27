"""The installed-upgrade backup and its restore verb on a real database (#3415).

Drives the real `_step_backup` (pg_dump, restore into a disposable database,
fingerprint comparison, drop, sidecar) and the real
`okg deployment backup restore --apply` path (restore through
`restore_database_dump`, owner and grant copy, fence, drain, rename swap)
against an isolated clone of the migrated test database. The runtime worker
audit is the only stub: no worker runs here.

Needs pg_dump and pg_restore on PATH. CI fails loudly without them; a
developer machine skips.
"""

from __future__ import annotations

import os
import shutil
import stat
from pathlib import Path
from typing import Any

import pytest
import yaml

from okg.substrate import deployment_installed_restore as dir_
from okg.substrate import deployment_installed_upgrade as diu

pytestmark = pytest.mark.postgres

NAME = "installed-pg"


def _require_tools() -> None:
    missing = [tool for tool in ("pg_dump", "pg_restore") if shutil.which(tool) is None]
    if missing and not os.environ.get("CI"):
        pytest.skip(f"{', '.join(missing)} not on PATH")
    assert not missing, f"{missing} must be on PATH in CI"


def _database_exists(dsn: str, name: str) -> bool:
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        return conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (name,),
        ).fetchone() is not None


def _drop(dsn: str, name: str) -> None:
    import psycopg
    from psycopg import sql

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
        )


def _maintenance(dsn: str) -> str:
    from okg.substrate.db import dsn as dsn_mod

    return dsn_mod.parse(dsn).with_dbname("postgres").render()


@pytest.fixture
def installed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, okg_workspace_isolated_runtime: str,
) -> dict[str, Any]:
    _require_tools()
    from okg.substrate.db.pool import close_all

    dsn = okg_workspace_isolated_runtime
    root = tmp_path / "deployments"
    deployment_dir = root / NAME
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump({"name": NAME, "release": {"channel": "dev", "kind": "standalone"}}),
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "user-home"))
    monkeypatch.setattr(diu, "_resolve_dsn", lambda options: dsn)
    monkeypatch.setattr(
        diu, "_worker_audit",
        lambda deployment: {"status": "not_observed", "root_count": 0, "roots": []},
    )
    close_all()
    leftovers: list[str] = []
    roles: list[str] = []
    yield {
        "dsn": dsn, "deployment_dir": deployment_dir, "leftovers": leftovers,
        "roles": roles,
    }
    close_all()
    for name in leftovers:
        _drop(_maintenance(dsn), name)
    _drop_roles(dsn, roles)


def _drop_roles(dsn: str, roles: list[str]) -> None:
    import psycopg
    from psycopg import sql

    if not roles:
        return
    with psycopg.connect(dsn, autocommit=True) as conn:
        for role in roles:
            conn.execute(
                sql.SQL("ALTER DATABASE {} OWNER TO CURRENT_USER").format(
                    sql.Identifier(conn.info.dbname)
                )
            )
            conn.execute(
                sql.SQL("REASSIGN OWNED BY {} TO CURRENT_USER").format(sql.Identifier(role))
            )
            conn.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
            conn.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


def _bless(archive: str) -> None:
    """Record ``archive`` as the pre-upgrade archive, as `okg up` does."""
    diu._write_private_json(
        diu.receipt_path(NAME),
        {"schema": diu.RECEIPT_SCHEMA, "upgrade": {"pre_upgrade_archive": archive}},
    )


def _backup(installed: dict[str, Any]) -> dict[str, Any]:
    options = type("Options", (), {"backup_dir": None})()
    row = diu._step_backup(
        options, NAME, installed["deployment_dir"], installed["dsn"],
        None, {"okg_version": "9.9.9"},
    )
    if row.get("verify_database_kept"):
        installed["leftovers"].append(row["verify_database"])
    return row


def test_real_backup_verifies_drops_the_copy_and_writes_a_private_sidecar(
    installed: dict[str, Any],
) -> None:
    row = _backup(installed)
    assert row["ok"] is True, row
    archive = Path(row["archive"])
    assert archive.parent == diu.backups_dir(NAME)
    assert stat.S_IMODE(archive.parent.stat().st_mode) == 0o700
    assert row["source"] == row["restored"]
    assert row["verify_database_dropped"] is True
    assert not _database_exists(_maintenance(installed["dsn"]), row["verify_database"])
    sidecar = diu._read_private_json(diu.sidecar_path(archive))[0]
    assert stat.S_IMODE(diu.sidecar_path(archive).stat().st_mode) == 0o600
    assert sidecar["archive_sha256"] == diu._file_sha256(archive)
    assert sidecar["database"] == diu._database_identity(installed["dsn"])
    assert sidecar["fingerprint"]["schema_migrations"] > 0


def test_failed_comparison_keeps_and_names_the_disposable_database(
    installed: dict[str, Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    real = diu._backup_fingerprint

    def skewed(dsn: str, deployment: str) -> dict[str, Any]:
        result = real(dsn, deployment)
        if diu.VERIFY_DATABASE_PREFIX in dsn:
            result = {**result, "catalog_versions": result["catalog_versions"] + 1}
        return result

    monkeypatch.setattr(diu, "_backup_fingerprint", skewed)
    row = _backup(installed)
    assert row["ok"] is False
    assert row["verify_database_kept"] is True
    assert row["verify_database"] in row["reason"]
    assert _database_exists(_maintenance(installed["dsn"]), row["verify_database"])
    assert not diu.sidecar_path(row["archive"]).exists()


def test_restore_apply_swaps_the_backup_in_and_keeps_the_live_database(
    installed: dict[str, Any],
) -> None:
    import psycopg

    from okg.substrate.db.pool import close_all

    dsn = installed["dsn"]
    row = _backup(installed)
    assert row["ok"] is True, row
    before = diu._backup_fingerprint(dsn, NAME)
    # The "upgrade": a change the restore must undo.
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at, deployment_name, "
            "catalog_fingerprint) VALUES ('applied', now(), %s, 'after-upgrade')",
            (NAME,),
        )
    assert diu._backup_fingerprint(dsn, NAME) != before
    close_all()

    _bless(row["archive"])
    plan = dir_.run_installed_restore(NAME, row["archive"], apply=False)
    assert plan["status"] == "planned"
    report = dir_.run_installed_restore(
        NAME, row["archive"], apply=True,
        command_runner=lambda argv, env: pytest.fail(f"no child expected: {argv}"),
    )
    kept = report["kept_database"]
    installed["leftovers"].append(kept)
    installed["leftovers"].append(report["restored_database"])
    assert report["ok"] is True, report
    assert diu._backup_fingerprint(dsn, NAME) == before
    from okg.substrate.db import dsn as dsn_mod

    kept_dsn = dsn_mod.parse(dsn).with_dbname(kept).render()
    with psycopg.connect(kept_dsn, autocommit=True) as conn:
        upgraded_rows = conn.execute(
            "SELECT count(*) FROM okg.catalog_versions WHERE catalog_fingerprint = 'after-upgrade'"
        ).fetchone()[0]
    assert upgraded_rows == 1
    assert not _database_exists(_maintenance(dsn), report["restored_database"])
    copy = next(step for step in report["steps"] if step["step"] == "copy_access")
    assert copy["ok"] is True, copy


def test_restore_refuses_while_a_client_session_stays_connected(
    installed: dict[str, Any],
) -> None:
    import psycopg

    from okg.substrate.db.pool import close_all

    dsn = installed["dsn"]
    row = _backup(installed)
    assert row["ok"] is True, row
    close_all()
    _bless(row["archive"])
    with psycopg.connect(dsn, autocommit=True) as holder:
        holder.execute("SELECT 1")
        report = dir_.run_installed_restore(
            NAME, row["archive"], apply=True, drain_timeout_seconds=1.0,
            command_runner=lambda argv, env: pytest.fail(f"no child expected: {argv}"),
        )
    installed["leftovers"].append(report["restored_database"])
    assert report["ok"] is False
    assert report["failed_step"] == "swap"
    assert report["swapped"] is False
    remaining = report["steps"][-1]["remaining_sessions"]
    assert any(session["backend_type"] == "client backend" for session in remaining)
    # Both databases were reopened: the live one still takes connections.
    with psycopg.connect(dsn, autocommit=True) as conn:
        assert conn.execute("SELECT 1").fetchone() == (1,)


def _access_checks(dsn: str) -> dict[str, Any]:
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        return {
            "mcp_usage": conn.execute(
                "SELECT has_schema_privilege('okg_mcp', 'okg', 'USAGE')"
            ).fetchone()[0],
            "drift_rows": conn.execute(
                "SELECT count(*) FROM okg.grant_registry_drift()"
            ).fetchone()[0],
            "public_hmac": conn.execute(
                "SELECT has_function_privilege('public', "
                "'okg.external_action_hmac_sha256(bytea,bytea)', 'EXECUTE')"
            ).fetchone()[0],
            "settings": sorted(row[0] for row in conn.execute(
                "SELECT unnest(s.setconfig) FROM pg_db_role_setting AS s "
                "JOIN pg_database AS d ON d.oid = s.setdatabase "
                "WHERE d.datname = current_database() AND s.setrole = 0"
            ).fetchall()),
            "owners": dict(conn.execute(
                """
                SELECT 'schema okg', pg_get_userbyid(nspowner) FROM pg_namespace
                 WHERE nspname = 'okg'
                UNION ALL
                SELECT 'table okg.catalog_versions', pg_get_userbyid(relowner)
                  FROM pg_class WHERE oid = 'okg.catalog_versions'::regclass
                UNION ALL
                SELECT 'function hmac', pg_get_userbyid(proowner) FROM pg_proc
                 WHERE oid = 'okg.external_action_hmac_sha256(bytea,bytea)'::regprocedure
                """
            ).fetchall()),
        }


def test_restore_keeps_grants_owners_and_public_revokes(installed: dict[str, Any]) -> None:
    """HIGH-1 of the #3434 review: role grants, owners and PUBLIC revokes survive."""
    import psycopg
    from psycopg import sql

    from okg.substrate.db.pool import close_all

    dsn = installed["dsn"]
    owner = "okg_restore_owner_test"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(owner)))
        conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(owner)))
        installed["roles"].append(owner)
        for statement in (
            "ALTER SCHEMA okg OWNER TO {}",
            "ALTER TABLE okg.catalog_versions OWNER TO {}",
            "ALTER FUNCTION okg.external_action_hmac_sha256(bytea, bytea) OWNER TO {}",
        ):
            conn.execute(sql.SQL(statement).format(sql.Identifier(owner)))
        database = sql.Identifier(conn.info.dbname)
        for statement in (
            "ALTER DATABASE {} SET search_path TO okg, public",
            "ALTER DATABASE {} SET statement_timeout TO 30000",
            "ALTER DATABASE {} SET work_mem TO '65536kB'",
        ):
            conn.execute(sql.SQL(statement).format(database))
        # A role= setting must never be copied (nor applied to the restore's
        # own session); this role never logs in, so it changes nothing here.
        conn.execute(
            sql.SQL("ALTER ROLE {} IN DATABASE {} SET role TO {}").format(
                sql.Identifier(owner), database, sql.Identifier(owner),
            )
        )
    before = _access_checks(dsn)
    assert before["settings"] == [
        "search_path=okg, public", "statement_timeout=30000", "work_mem=65536kB",
    ]
    assert before["mcp_usage"] is True
    assert before["drift_rows"] == 0
    assert before["public_hmac"] is False
    assert set(before["owners"].values()) == {owner}

    row = _backup(installed)
    assert row["ok"] is True, row.get("error") or row.get("reason")
    close_all()
    _bless(row["archive"])
    report = dir_.run_installed_restore(
        NAME, row["archive"], apply=True,
        command_runner=lambda argv, env: pytest.fail(f"no child expected: {argv}"),
    )
    installed["leftovers"].extend([report["kept_database"], report["restored_database"]])
    assert report["ok"] is True, report
    assert _access_checks(dsn) == before
    copy = next(step for step in report["steps"] if step["step"] == "copy_access")
    assert copy["settings_not_copied"] == [f"{owner}: role"]
    with psycopg.connect(dsn, autocommit=True) as conn:
        role_settings = conn.execute(
            "SELECT count(*) FROM pg_db_role_setting AS s JOIN pg_database AS d "
            "ON d.oid = s.setdatabase WHERE d.datname = current_database() "
            "AND s.setrole <> 0"
        ).fetchone()[0]
    assert role_settings == 0


def test_restore_refuses_a_role_without_timescaledb_restore_rights(
    installed: dict[str, Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HIGH-2: a non-superuser owner is refused before anything changes."""
    import secrets as _secrets

    import psycopg
    from psycopg import sql
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    from okg.substrate.db.pool import close_all

    dsn = installed["dsn"]
    with psycopg.connect(dsn, autocommit=True) as conn:
        if conn.execute(
            "SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'"
        ).fetchone() is None:
            pytest.skip("the test database has no TimescaleDB")
    row = _backup(installed)
    assert row["ok"] is True, row.get("error") or row.get("reason")
    role, password = "okg_restore_nosuper_test", _secrets.token_hex(16)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))
        conn.execute(
            sql.SQL("CREATE ROLE {} LOGIN CREATEDB PASSWORD {}").format(
                sql.Identifier(role), sql.Literal(password),
            )
        )
        installed["roles"].append(role)
        conn.execute(
            sql.SQL("ALTER DATABASE {} OWNER TO {}").format(
                sql.Identifier(conn.info.dbname), sql.Identifier(role),
            )
        )
    info = conninfo_to_dict(dsn)
    role_dsn = make_conninfo(**{**info, "user": role, "password": password})
    monkeypatch.setattr(diu, "_resolve_dsn", lambda options: role_dsn)
    close_all()
    _bless(row["archive"])
    with pytest.raises(dir_.InstalledRestoreRefused) as excinfo:
        dir_.run_installed_restore(NAME, row["archive"], apply=False)
    assert "TimescaleDB" in " ".join(excinfo.value.data["problems"])
    assert "not a superuser" in " ".join(excinfo.value.data["problems"])


def test_hostile_text_in_the_archive_is_never_run(installed: dict[str, Any]) -> None:
    """H1 of the third review: a planted line in a function body or a comment."""
    import psycopg
    from psycopg import sql

    from okg.substrate.db.pool import close_all

    dsn = installed["dsn"]
    owner = "okg_restore_hostile_test"
    planted = (
        "ALTER TABLE public.hostile_t OWNER TO {owner}; "
        "CREATE TABLE public.pwned_by_{where}(x int);"
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(owner)))
        conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(owner)))
        installed["roles"].append(owner)
        conn.execute("CREATE TABLE public.hostile_t(x int)")
        body = "\n" + planted.format(owner=owner, where="body") + "\nSELECT 1;\n"
        conn.execute(
            sql.SQL("CREATE FUNCTION public.hostile_f() RETURNS int LANGUAGE sql AS {}").format(
                sql.Literal(body)
            )
        )
        conn.execute(
            sql.SQL("COMMENT ON TABLE public.hostile_t IS {}").format(
                sql.Literal("x\n" + planted.format(owner=owner, where="comment") + " --")
            )
        )
        for statement in (
            "ALTER TABLE public.hostile_t OWNER TO {}",
            "ALTER FUNCTION public.hostile_f() OWNER TO {}",
        ):
            conn.execute(sql.SQL(statement).format(sql.Identifier(owner)))

    row = _backup(installed)
    assert row["ok"] is True, row.get("error") or row.get("reason") or row
    close_all()
    _bless(row["archive"])
    report = dir_.run_installed_restore(
        NAME, row["archive"], apply=True,
        command_runner=lambda argv, env: pytest.fail(f"no child expected: {argv}"),
    )
    installed["leftovers"].extend([report["kept_database"], report["restored_database"]])
    assert report["ok"] is True, report
    with psycopg.connect(dsn, autocommit=True) as conn:
        planted_tables = conn.execute(
            "SELECT relname FROM pg_class WHERE relname LIKE 'pwned%'"
        ).fetchall()
        owners = conn.execute(
            "SELECT pg_get_userbyid(relowner) FROM pg_class "
            "WHERE oid = 'public.hostile_t'::regclass "
            "UNION ALL SELECT pg_get_userbyid(proowner) FROM pg_proc "
            "WHERE oid = 'public.hostile_f()'::regprocedure"
        ).fetchall()
    assert planted_tables == []
    assert [row[0] for row in owners] == [owner, owner]


def test_chunks_compressed_by_a_job_do_not_change_the_access_fingerprint(
    installed: dict[str, Any],
) -> None:
    """H2: TimescaleDB's own jobs create compressed chunks in a restored copy."""
    import psycopg

    dsn = installed["dsn"]
    with psycopg.connect(dsn, autocommit=True) as conn:
        if conn.execute(
            "SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'"
        ).fetchone() is None:
            pytest.skip("the test database has no TimescaleDB")
        conn.execute("CREATE TABLE public.ht(t timestamptz NOT NULL, v int)")
        conn.execute("SELECT create_hypertable('public.ht', 't')")
        conn.execute("ALTER TABLE public.ht SET (timescaledb.compress)")
        conn.execute(
            "INSERT INTO public.ht SELECT now() - interval '60 days' - i * interval '1 hour', i "
            "FROM generate_series(1, 50) AS i"
        )
    before = diu.access_fingerprint(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn:
        compressed = conn.execute(
            "SELECT count(compress_chunk(c)) FROM show_chunks('public.ht') AS c"
        ).fetchone()[0]
        relations_after = conn.execute(
            "SELECT count(*) FROM pg_class AS c JOIN pg_namespace AS n "
            "ON n.oid = c.relnamespace WHERE n.nspname = '_timescaledb_internal' "
            "AND c.relname LIKE 'compress%'"
        ).fetchone()[0]
    assert compressed >= 1
    assert relations_after >= 1
    assert diu.access_fingerprint(dsn)["digest"] == before["digest"]
    assert any(row[2] == "ht" for row in before["entries"])


def _retention_runs_before_the_replay(
    monkeypatch: pytest.MonkeyPatch, table: str,
) -> list[tuple[int, int, bool]]:
    """Run ``table``'s retention job in every restored copy before its replay.

    In a restored copy TimescaleDB's scheduler runs the retention job when it
    gets a background worker: maybe before the owner and grant replays, maybe
    after the restore returns, maybe never. A server with about 15 or more
    TimescaleDB databases has no free worker for jobs (ops/pg/README.md), and
    on 2026-10-22 a CI run read all 21 chunks back after the restore (#4096).
    Running the job here, in the test's own session, makes the replays meet
    the dropped chunks every time, with or without a free worker. Returns
    (chunks before, chunks after, job scheduled) for each copy, so the test
    still sees a restore that leaves the job unscheduled.
    """
    import psycopg

    real_replay = diu.replay_archive_access
    schema, name = table.split(".", 1)
    runs: list[tuple[int, int, bool]] = []

    def replay_after_retention(target_dsn: str, archive: str | Path) -> dict[str, Any]:
        with psycopg.connect(target_dsn, autocommit=True) as conn:
            before = int(conn.execute(
                "SELECT count(*) FROM show_chunks(%s)", (table,),
            ).fetchone()[0])
            job = conn.execute(
                "SELECT job_id, scheduled FROM timescaledb_information.jobs "
                "WHERE proc_name = 'policy_retention' "
                "AND hypertable_schema = %s AND hypertable_name = %s",
                (schema, name),
            ).fetchone()
            assert job is not None, f"the restored copy has no retention job for {table}"
            conn.execute("CALL run_job(%s)", (job[0],))
            after = int(conn.execute(
                "SELECT count(*) FROM show_chunks(%s)", (table,),
            ).fetchone()[0])
        runs.append((before, after, bool(job[1])))
        return real_replay(target_dsn, archive)

    monkeypatch.setattr(diu, "replay_archive_access", replay_after_retention)
    return runs


def _assert_retention_ran_in_both_copies(
    runs: list[tuple[int, int, bool]], chunks_before: int,
) -> None:
    """Both copies kept the job scheduled and lost chunks before the replay.

    The copies are the backup's check copy and the restore's copy.
    """
    assert len(runs) == 2, runs
    assert all(scheduled for _, _, scheduled in runs), (
        f"a restored copy left the retention job unscheduled: {runs}"
    )
    assert all(after < chunks_before for _, after, _ in runs), (runs, chunks_before)


def test_retention_in_the_restored_copy_does_not_break_the_replay(
    installed: dict[str, Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HIGH (round 4): retention drops chunks in the copy before the replay.

    In a restored copy TimescaleDB's jobs have no run history, so the
    retention job runs as soon as the scheduler starts and drops every chunk
    past its limit. The owner and grant replays must not need those chunks.
    The test runs the job itself before each replay
    (``_retention_runs_before_the_replay``), so it does not depend on when,
    or whether, the scheduler gets a background worker.
    """
    import psycopg

    from okg.substrate.db.pool import close_all

    dsn = installed["dsn"]
    runs = _retention_runs_before_the_replay(monkeypatch, "public.rt")
    with psycopg.connect(dsn, autocommit=True) as conn:
        if conn.execute(
            "SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'"
        ).fetchone() is None:
            pytest.skip("the test database has no TimescaleDB")
        conn.execute("CREATE TABLE public.rt(t timestamptz NOT NULL, v int)")
        conn.execute(
            "SELECT create_hypertable('public.rt', 't', chunk_time_interval => interval '1 day')"
        )
        job = conn.execute(
            "SELECT add_retention_policy('public.rt', INTERVAL '7 days')"
        ).fetchone()[0]
        conn.execute("SELECT alter_job(%s, scheduled => false)", (job,))
        conn.execute(
            "INSERT INTO public.rt SELECT now() - i * interval '1 day', i "
            "FROM generate_series(0, 20) AS i"
        )
        conn.execute(
            "SELECT alter_job(%s, scheduled => true, next_start => now() + interval '1 day')",
            (job,),
        )
        conn.execute("GRANT SELECT ON public.rt TO okg_mcp")
        chunks_before = conn.execute(
            "SELECT count(*) FROM show_chunks('public.rt')"
        ).fetchone()[0]
    assert chunks_before >= 15

    row = _backup(installed)
    assert row["ok"] is True, row.get("error") or row.get("reason") or row
    assert row["access_replay"]["chunk_owners_left_to_timescaledb"] >= chunks_before
    close_all()
    _bless(row["archive"])
    report = dir_.run_installed_restore(
        NAME, row["archive"], apply=True,
        command_runner=lambda argv, env: pytest.fail(f"no child expected: {argv}"),
    )
    installed["leftovers"].extend([report["kept_database"], report["restored_database"]])
    assert report["ok"] is True, report
    with psycopg.connect(dsn, autocommit=True) as conn:
        assert conn.execute(
            "SELECT has_table_privilege('okg_mcp', 'public.rt', 'SELECT')"
        ).fetchone()[0] is True
        # The live database is now the restored copy: retention ran there
        # before the replay and dropped chunks, so the replays met missing
        # chunks.
        chunks_after = conn.execute(
            "SELECT count(*) FROM show_chunks('public.rt')"
        ).fetchone()[0]
    _assert_retention_ran_in_both_copies(runs, chunks_before)
    assert chunks_after < chunks_before


def _retention_hypertable(
    conn: Any,
    table: str,
    *,
    associated_schema: str | None = None,
    compress: bool = False,
) -> int:
    """A hypertable with 21 daily chunks, 7-day retention due at once in a copy."""
    conn.execute(f"CREATE TABLE {table}(t timestamptz NOT NULL, v int, w int)")
    if associated_schema:
        conn.execute(f"CREATE SCHEMA {associated_schema}")
        conn.execute(
            "SELECT create_hypertable(%s, 't', chunk_time_interval => interval '1 day', "
            "associated_schema_name => %s)",
            (table, associated_schema),
        )
    else:
        conn.execute(
            "SELECT create_hypertable(%s, 't', chunk_time_interval => interval '1 day')",
            (table,),
        )
    job = conn.execute(
        "SELECT add_retention_policy(%s, INTERVAL '7 days')", (table,),
    ).fetchone()[0]
    conn.execute("SELECT alter_job(%s, scheduled => false)", (job,))
    conn.execute(
        f"INSERT INTO {table} SELECT now() - i * interval '1 day', i, i "
        "FROM generate_series(0, 20) AS i"
    )
    if compress:
        conn.execute(f"ALTER TABLE {table} SET (timescaledb.compress)")
        conn.execute(
            "SELECT count(compress_chunk(c)) FROM show_chunks(%s, older_than => interval '3 days') AS c",
            (table,),
        )
    conn.execute(
        "SELECT alter_job(%s, scheduled => true, next_start => now() + interval '1 day')",
        (job,),
    )
    return int(conn.execute("SELECT count(*) FROM show_chunks(%s)", (table,)).fetchone()[0])


def _skip_without_timescaledb(conn: Any) -> None:
    if conn.execute(
        "SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'"
    ).fetchone() is None:
        pytest.skip("the test database has no TimescaleDB")


def _backup_and_restore(installed: dict[str, Any]) -> dict[str, Any]:
    from okg.substrate.db.pool import close_all

    row = _backup(installed)
    assert row["ok"] is True, row.get("error") or row.get("reason") or row
    close_all()
    _bless(row["archive"])
    report = dir_.run_installed_restore(
        NAME, row["archive"], apply=True,
        command_runner=lambda argv, env: pytest.fail(f"no child expected: {argv}"),
    )
    installed["leftovers"].extend([report["kept_database"], report["restored_database"]])
    assert report["ok"] is True, report
    return row


def test_compressed_chunks_in_an_own_chunk_schema_survive_retention_in_the_copy(
    installed: dict[str, Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Round 5: TimescaleDB 2.29 names compressed chunks _hyper_N_M_chunk_compressed."""
    import psycopg

    dsn = installed["dsn"]
    runs = _retention_runs_before_the_replay(monkeypatch, "public.rc")
    with psycopg.connect(dsn, autocommit=True) as conn:
        _skip_without_timescaledb(conn)
        chunks_before = _retention_hypertable(
            conn, "public.rc", associated_schema="rc_chunks", compress=True,
        )
        compressed = conn.execute(
            "SELECT count(*) FROM pg_class AS c JOIN pg_namespace AS n "
            "ON n.oid = c.relnamespace WHERE n.nspname = 'rc_chunks' "
            "AND c.relname LIKE '%%_chunk_compressed'"
        ).fetchone()[0]
        conn.execute("GRANT SELECT ON public.rc TO okg_mcp")
        # 2.29: the compressed chunk carries its uncompressed chunk's name and
        # hypertable prefix, and the chunk catalog does not list it.
        uncatalogued = conn.execute(
            "SELECT count(*) FROM pg_class AS c JOIN pg_namespace AS n "
            "ON n.oid = c.relnamespace WHERE n.nspname = 'rc_chunks' "
            "AND c.relname LIKE '%%_chunk_compressed' AND NOT EXISTS ("
            "SELECT 1 FROM _timescaledb_catalog.chunk AS k WHERE k.relid = c.oid)"
        ).fetchone()[0]
        prefix = conn.execute(
            "SELECT associated_table_prefix FROM _timescaledb_catalog.hypertable "
            "WHERE table_name = 'rc'"
        ).fetchone()[0]
        chunks = diu.chunk_names(conn, {
            ("rc_chunks", f"{prefix}_10_chunk"),
            ("rc_chunks", f"{prefix}_10_chunk_compressed"),
            ("_timescaledb_internal", "_hyper_1_1_chunk"),
            ("rc_chunks", f"{prefix}_10_chunk_compressed_x"),
            ("rc_chunks", "_hyper_999_1_chunk"),
            ("public", f"{prefix}_10_chunk"),
            ("rc_chunks", "users"),
        })
    assert compressed >= 1
    assert uncatalogued == compressed
    assert chunks == {
        ("rc_chunks", f"{prefix}_10_chunk"),
        ("rc_chunks", f"{prefix}_10_chunk_compressed"),
        ("_timescaledb_internal", "_hyper_1_1_chunk"),
    }
    _backup_and_restore(installed)
    with psycopg.connect(dsn, autocommit=True) as conn:
        assert conn.execute(
            "SELECT has_table_privilege('okg_mcp', 'public.rc', 'SELECT')"
        ).fetchone()[0] is True
        chunks_after = conn.execute(
            "SELECT count(*) FROM show_chunks('public.rc')"
        ).fetchone()[0]
    _assert_retention_ran_in_both_copies(runs, chunks_before)
    assert chunks_after < chunks_before


def test_column_grant_on_chunks_survives_retention_in_the_copy(
    installed: dict[str, Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Round 5: GRANT SELECT (column) is copied onto chunks; dropped chunks keep none."""
    import psycopg

    dsn = installed["dsn"]
    runs = _retention_runs_before_the_replay(monkeypatch, "public.rcol")
    with psycopg.connect(dsn, autocommit=True) as conn:
        _skip_without_timescaledb(conn)
        chunks_before = _retention_hypertable(conn, "public.rcol")
        conn.execute("GRANT SELECT (v) ON public.rcol TO okg_mcp")
        chunk_column_grants = conn.execute(
            "SELECT count(*) FROM pg_attribute AS a JOIN pg_class AS c ON c.oid = a.attrelid "
            "JOIN pg_namespace AS n ON n.oid = c.relnamespace "
            "WHERE n.nspname = '_timescaledb_internal' AND a.attname = 'v' "
            "AND a.attacl IS NOT NULL"
        ).fetchone()[0]
    assert chunk_column_grants >= 1
    _backup_and_restore(installed)
    with psycopg.connect(dsn, autocommit=True) as conn:
        assert conn.execute(
            "SELECT has_column_privilege('okg_mcp', 'public.rcol', 'v', 'SELECT')"
        ).fetchone()[0] is True
        chunks_after = conn.execute(
            "SELECT count(*) FROM show_chunks('public.rcol')"
        ).fetchone()[0]
    _assert_retention_ran_in_both_copies(runs, chunks_before)
    assert chunks_after < chunks_before
