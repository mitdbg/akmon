"""`okg deployment backup restore`: go back to a pre-upgrade backup (#3415).

The way back after `okg up` upgraded a deployment that runs from an
installed okg package (``deployment_installed_upgrade``). Jason's decision
`upgradeback B` (2026-10-06): one verb, run with the NEWER okg (the one that
took the backup), plans by default and changes nothing until ``--apply``.

It restores only the archive recorded as the one taken before the upgrade
began (the unfinished-upgrade marker's or the receipt's
``pre_upgrade_archive``), with its owner-only sidecar ``<archive>.json``. It
needs a superuser when the database uses TimescaleDB, and otherwise a role
with CREATEDB and membership in the database owner's role.

With ``--apply`` it:

1. stops the deployment's runtime worker (`okg dogfood down`);
2. restores the archive into a fresh database on the same server through
   the verified restore path (``postgres_clone.restore_database_dump``:
   fenced target, filtered table of contents, TimescaleDB restore hooks);
3. replays the archive's owners, ACLs and default ACLs, which that path
   leaves out (``--no-owner --no-privileges``);
4. checks the restored copy against the sidecar: the data fingerprint
   (migrations, catalog versions, published generations, node and edge
   counts) and the access fingerprint (owners and privileges);
5. gives the restored database the live database's owner, database-level
   grants and settings;
6. fences both databases, waits for sessions to leave, ends only
   TimescaleDB background workers, and refuses (reopening both) when any
   other session stays connected; whatever happens, both are reopened;
7. swaps by renaming: the live database becomes ``<db>_pre_restore_<stamp>``
   and the restored one takes its name. Nothing is dropped;
8. starts TimescaleDB's background workers for the database again;
9. checks both fingerprints again under the live name, puts the receipt the
   deployment had before the upgrade back in place (the current one is
   moved aside), and writes a restore record.

It does not restart the worker: the old okg wheel must be reinstalled
first. The report's ``next_action`` says how.
"""

from __future__ import annotations

import json
import os
import stat
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from okg.substrate import deployment_installed_upgrade as diu
from okg.substrate.errors import SubstrateError

RESTORE_SCHEMA = "okg.installed_restore.v1"
RESTORES_NAME = "restores"
DRAIN_TIMEOUT_SECONDS = 30.0
SCHEDULER_START_TIMEOUT_SECONDS = 20.0
_MAX_IDENTIFIER_BYTES = 63


class InstalledRestoreRefused(SubstrateError):
    """`okg deployment backup restore` will not restore as asked."""

    code = "installed_restore_refused"


def _refuse(message: str, *, fix_hint: str, **data: Any) -> InstalledRestoreRefused:
    return InstalledRestoreRefused(message, data=data, fix_hint=fix_hint)


def run_installed_restore(
    deployment: str,
    archive: str | Path,
    *,
    apply: bool,
    command_runner: diu.CommandRunner | None = None,
    drain_timeout_seconds: float = DRAIN_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Plan (default) or apply the restore of one pre-upgrade backup."""
    from okg.substrate.db import dsn as dsn_mod
    from okg.substrate.deployment_upgrade import UpgradeOptions

    route = diu.installed_upgrade_route(deployment)
    if route is None:
        raise _refuse(
            "this deployment does not run from an installed okg package",
            deployment=deployment,
            fix_hint=(
                "backup restore restores archives taken by `okg up` for an "
                "installed deployment; a Git-checkout deployment has its own "
                "recovery path (docs/operate/lifecycle.md)"
            ),
        )
    diu.state_dir(deployment)
    deployment_dir = Path(str(route["deployment_dir"]))
    archive_path = Path(archive).expanduser().resolve()
    sidecar = _read_sidecar(deployment, deployment_dir, archive_path)
    marker = _raw_state(diu.marker_path(deployment))
    _require_pre_upgrade_archive(deployment, archive_path, sidecar, marker)
    dsn = diu._resolve_dsn(UpgradeOptions(deployment=deployment))
    live = diu._database_identity(dsn)
    recorded = dict(sidecar.get("database") or {})
    if (
        live.get("system_identifier") != recorded.get("system_identifier")
        or live.get("database") != recorded.get("database")
        or live.get("port") != recorded.get("port")
    ):
        raise _refuse(
            "the archive was taken from a different database than the one "
            "this deployment uses now",
            deployment=deployment,
            archive=str(archive_path),
            recorded=recorded,
            live=live,
            fix_hint=(
                "restore it with `okg db clone --restore-dump` into a "
                "separate database instead, or check the deployment's "
                "postgres.dsn"
            ),
        )
    stamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    live_db = str(live["database"])
    restored_db = _derived_name(live_db, f"_restore_{stamp}")
    kept_db = _derived_name(live_db, f"_pre_restore_{stamp}")
    access = _database_access(dsn, live_db, (restored_db, kept_db))
    upgrade_stopped_worker = bool((marker or {}).get("worker_stopped_by_upgrade"))
    report: dict[str, Any] = {
        "schema": RESTORE_SCHEMA,
        "deployment": deployment,
        "mode": "apply" if apply else "plan",
        "archive": str(archive_path),
        "sidecar": str(diu.sidecar_path(archive_path)),
        "backup": {
            key: sidecar.get(key)
            for key in (
                "created_at", "from_version", "to_version", "archive_sha256",
                "archive_bytes", "resumed_upgrade",
            )
        },
        "expected_fingerprint": diu._public_graph(sidecar["fingerprint"]),
        "expected_access": {
            "digest": sidecar["access_fingerprint"]["digest"],
            "rows": sidecar["access_fingerprint"]["rows"],
        },
        "database": live,
        "restored_database": restored_db,
        "kept_database": kept_db,
        "restore_actor": access["actor"],
        "restore_actor_superuser": access["superuser"],
        "connected_sessions": access["sessions"],
        "worker": _worker_summary(deployment),
        "upgrade_stopped_worker": upgrade_stopped_worker,
        "notes": [
            (
                "the restore needs a superuser when the database uses "
                "TimescaleDB; otherwise a role with CREATEDB and membership "
                "in the database owner's role (whose ACL replay still fails "
                "on a grant made by a role other than the object's owner)"
            ),
            (
                "owners, grants, PUBLIC revokes and default privileges are "
                "replayed from the archive and checked against the backup's "
                "access fingerprint; database owner, database grants and "
                "database settings are copied from the live database"
            ),
            "the restore does not restart the runtime worker",
        ],
        "started_at": diu._now(),
        "steps": [],
    }
    problems = access["problems"]
    if problems:
        raise _refuse(
            "the restore cannot run with these database rights",
            deployment=deployment,
            problems=problems,
            fix_hint=(
                "run it as a superuser (required when the database uses "
                "TimescaleDB), or as a role that has CREATEDB and is a member "
                "of the database owner's role"
            ),
        )
    # Before the plan answers and before the worker stops (issue #3587).
    tools = diu._client_tools_check(dsn, ("pg_restore",))
    if not tools["ok"]:
        raise _refuse(
            tools["reason"],
            deployment=deployment,
            client_tools=tools,
            fix_hint=tools["fix_hint"],
        )
    if not apply:
        report.update({
            "ok": True,
            "status": "planned",
            "planned_steps": [
                "stop_worker", "restore", "replay_access", "verify_restored",
                "copy_access", "swap", "timescaledb_workers", "verify_live",
                "receipt",
            ],
            "text": (
                f"plan: restore {deployment} ({live_db}) from {archive_path.name} "
                f"(taken before the upgrade from okg {sidecar.get('from_version')} "
                f"to {sidecar.get('to_version')}); the live database is kept as "
                f"{kept_db}. Nothing changed; rerun with --apply"
            ),
        })
        return report

    runner = command_runner or diu._default_runner()
    env = diu._child_env(deployment, dsn)

    def start_command() -> str:
        return diu.start_worker_command(
            deployment, deployment_dir=deployment_dir, resolve_dsn=lambda: dsn,
        )
    restored_dsn = dsn_mod.parse(dsn).with_dbname(restored_db).render()
    expected = json.loads(json.dumps(sidecar["fingerprint"]))
    expected_access = sidecar["access_fingerprint"]
    state: dict[str, Any] = {
        "swapped": False,
        "restored_created": False,
        "observed": None,
    }

    def stop_worker() -> dict[str, Any]:
        return diu._step_stop_worker(deployment, runner, env)

    def restore() -> dict[str, Any]:
        from okg.substrate.postgres_clone import restore_database_dump

        state["restored_created"] = True
        result = restore_database_dump(
            dsn, restored_dsn, archive_path,
            drain_timeout_seconds=drain_timeout_seconds,
        )
        return {"step": "restore", "ok": True, "operation": result.get("operation")}

    def replay_access() -> dict[str, Any]:
        return {
            "step": "replay_access",
            "ok": True,
            **diu.replay_archive_access(restored_dsn, archive_path),
        }

    def verify(step: str, target_dsn: str, where: str) -> dict[str, Any]:
        observed = json.loads(json.dumps(diu._backup_fingerprint(target_dsn, deployment)))
        observed_access = diu.access_fingerprint(target_dsn)
        row: dict[str, Any] = {
            "step": step,
            "fingerprint": diu._public_graph(observed),
            "access_digest": observed_access["digest"],
            "access_rows": observed_access["rows"],
        }
        problems: list[str] = []
        if observed != expected:
            problems.append("the data does not match the backup's recorded fingerprint")
        if observed_access["digest"] != expected_access["digest"]:
            problems.append("owners or privileges do not match the backup's record")
            row["access_differences"] = diu.access_differences(
                expected_access, observed_access,
            )
        if problems:
            return {**row, "ok": False, "reason": f"{where}: " + "; ".join(problems)}
        return {**row, "ok": True}

    def copy_access() -> dict[str, Any]:
        return _copy_database_access(dsn, live_db, restored_db)

    def fence_and_swap() -> dict[str, Any]:
        row = _fence_drain_swap(
            dsn, live_db, restored_db, kept_db,
            drain_timeout_seconds=drain_timeout_seconds,
        )
        state["swapped"] = bool(row.get("swapped"))
        state["observed"] = row.get("observed_databases")
        return row

    def timescaledb_workers() -> dict[str, Any]:
        return _start_timescaledb_workers(dsn, live_db)

    def receipt() -> dict[str, Any]:
        row = _put_back_receipt(deployment, sidecar, stamp)
        state["receipt"] = row
        return row

    sequence: list[tuple[str, Callable[[], dict[str, Any]]]] = [
        ("stop_worker", stop_worker),
        ("restore", restore),
        ("replay_access", replay_access),
        ("verify_restored", lambda: verify(
            "verify_restored", restored_dsn, f"the restored copy {restored_db}",
        )),
        ("copy_access", copy_access),
        ("swap", fence_and_swap),
        # Right after the swap: the deployment's state must describe the
        # database now live, even if a later check fails.
        ("receipt", receipt),
        ("timescaledb_workers", timescaledb_workers),
        ("verify_live", lambda: verify(
            "verify_live", dsn,
            f"after the swap {live_db} (the database before the restore is "
            f"kept as {kept_db})",
        )),
    ]
    stopped_now = False
    failed: str | None = None
    failures: list[str] = []
    for name, step in sequence:
        if failed is not None and not state["swapped"]:
            break
        row = diu._run_step(name, step)
        report["steps"].append(row)
        if name == "stop_worker" and row.get("stopped"):
            stopped_now = True
        if name == "timescaledb_workers":
            state["timescaledb_workers"] = row
        # Before the swap the first failure stops the restore. After it,
        # every remaining step still runs: the receipt must describe the
        # database now live, and TimescaleDB's scheduler, which the swap
        # ended, must be started again whatever else failed.
        if not row.get("ok"):
            failures.append(name)
            if failed is None:
                failed = name
    if failed is not None:
        worker_stopped = stopped_now or upgrade_stopped_worker
        report.update({
            "ok": False,
            "status": "failed",
            "failed_step": failed,
            "failed_steps": failures,
            "swapped": state["swapped"],
            "observed_databases": state["observed"],
            "worker_stopped": worker_stopped,
            "finished_at": diu._now(),
            "next_action": _next_action_on_failure(
                deployment, failures, state, live_db, restored_db, kept_db,
                worker_stopped=worker_stopped,
                start_command=start_command() if worker_stopped else "",
            ),
        })
        _record(deployment, stamp, report)
        return report
    worker_stopped = stopped_now or upgrade_stopped_worker
    report.update({
        "ok": True,
        "status": "restored",
        "swapped": True,
        "worker_stopped": worker_stopped,
        "finished_at": diu._now(),
        "text": (
            f"restored {deployment}: {live_db} now holds the backup taken "
            f"before the upgrade to okg {sidecar.get('to_version')}; the "
            f"upgraded database is kept as {kept_db}"
        ),
        "next_action": _next_action_on_success(
            deployment, sidecar, worker_stopped,
            start_command=start_command() if worker_stopped else "",
        ),
    })
    _record(deployment, stamp, report)
    return report


# ------------------------------------------------------------------ sidecar


def _raw_state(path: Path) -> dict[str, Any] | None:
    """A state file as JSON, or None; the restore is a recovery path, so a
    damaged receipt or marker does not block it (it is moved aside)."""
    payload, reason = diu._read_private_json(path)
    if reason is not None or not isinstance(payload, Mapping):
        return None
    return dict(payload)


def _require_pre_upgrade_archive(
    deployment: str,
    archive: Path,
    sidecar: Mapping[str, Any],
    marker: Mapping[str, Any] | None,
) -> None:
    """Only the archive taken before the upgrade began may be restored.

    A backup taken by a resumed `okg up` can already hold part of the
    upgrade while its name still says the old version.
    """
    receipt = _raw_state(diu.receipt_path(deployment)) or {}
    candidates = [
        (marker or {}).get("pre_upgrade_archive"),
        (receipt.get("upgrade") or {}).get("pre_upgrade_archive")
        if isinstance(receipt.get("upgrade"), Mapping) else None,
    ]
    allowed = {
        Path(str(value)).expanduser().resolve() for value in candidates if value
    }
    if archive in allowed:
        return
    raise _refuse(
        "this archive is not recorded as the backup taken before the upgrade began",
        deployment=deployment,
        archive=str(archive),
        pre_upgrade_archives=sorted(str(path) for path in allowed),
        resumed_upgrade=sidecar.get("resumed_upgrade"),
        applied_migrations=list(sidecar.get("applied_migrations") or ()),
        fix_hint=(
            "restore the pre_upgrade_archive named in the `okg up` report, "
            "the unfinished-upgrade marker or the receipt; to inspect another "
            "archive, restore it into a separate database with `okg db clone "
            "--restore-dump`"
        ),
    )


def _read_sidecar(
    deployment: str, deployment_dir: Path, archive: Path,
) -> dict[str, Any]:
    if not archive.is_file():
        raise _refuse(
            "the archive does not exist",
            archive=str(archive),
            fix_hint="pass the archive path named by the `okg up` report or receipt",
        )
    path = diu.sidecar_path(archive)
    payload, reason = diu._read_private_json(path)
    if payload is None and reason is None:
        raise _refuse(
            "the archive has no sidecar describing it",
            archive=str(archive),
            sidecar=str(path),
            fix_hint=(
                "only archives taken by `okg up` (which writes <archive>.json) "
                "can be restored in place; restore another archive into a "
                "separate database with `okg db clone --restore-dump`"
            ),
        )
    if reason is None:
        reason = _sidecar_mode_problem(path)
    if reason is None:
        reason = _sidecar_problem(payload, deployment, deployment_dir, archive)
    if reason is not None:
        raise _refuse(
            "the archive's sidecar cannot be trusted",
            archive=str(archive),
            sidecar=str(path),
            reason=reason,
            fix_hint=(
                "restore the archive this sidecar was written for, from the "
                "account that took it; the sidecar must be readable only by you"
            ),
        )
    return dict(payload)


def _sidecar_mode_problem(path: Path) -> str | None:
    info = path.stat()
    if info.st_uid != os.getuid():
        return f"the sidecar is owned by uid {info.st_uid}, not by you"
    if stat.S_IMODE(info.st_mode) & 0o077:
        return f"the sidecar is readable or writable by others ({oct(stat.S_IMODE(info.st_mode))})"
    return None


def _sidecar_problem(
    payload: Any, deployment: str, deployment_dir: Path, archive: Path,
) -> str | None:
    if not isinstance(payload, Mapping) or payload.get("schema") != diu.BACKUP_SCHEMA:
        return "not a backup sidecar"
    if payload.get("deployment") != deployment:
        return f"it names deployment {payload.get('deployment')!r}"
    if payload.get("deployment_dir") != str(deployment_dir):
        return f"it names deployment directory {payload.get('deployment_dir')}"
    if payload.get("archive") != archive.name:
        return f"it describes {payload.get('archive')!r}"
    if not isinstance(payload.get("fingerprint"), Mapping):
        return "it records no fingerprint"
    access = payload.get("access_fingerprint")
    if not isinstance(access, Mapping) or not isinstance(access.get("digest"), str):
        return "it records no access fingerprint (taken by an okg before the restore existed)"
    if not isinstance(payload.get("database"), Mapping):
        return "it records no database identity"
    previous = payload.get("previous_receipt")
    if previous is not None:
        problem = diu._receipt_problem(previous, deployment, deployment_dir)
        if problem is not None:
            return f"its previous receipt cannot be put back: {problem}"
    if archive.stat().st_size != payload.get("archive_bytes"):
        return "the archive's size differs from the recorded one"
    if diu._file_sha256(archive) != payload.get("archive_sha256"):
        return "the archive's sha256 differs from the recorded one"
    return None


# ----------------------------------------------------------------- database


def _derived_name(database: str, suffix: str) -> str:
    """``<database><suffix>``, cut to PostgreSQL's 63-byte identifier limit."""
    room = _MAX_IDENTIFIER_BYTES - len(suffix.encode("utf-8"))
    prefix = database.encode("utf-8")[:room].decode("utf-8", errors="ignore")
    return prefix + suffix


def _maintenance(dsn: str) -> str:
    from okg.substrate.db import dsn as dsn_mod

    return dsn_mod.parse(dsn).with_dbname("postgres").render()


def _database_access(
    dsn: str, live_db: str, new_names: tuple[str, ...],
) -> dict[str, Any]:
    """Who restores, whether that role may, and who else is connected."""
    from okg.substrate.db.connection import connect as db_connect

    rights = diu.restore_rights(dsn, live_db)
    problems = list(rights["problems"])
    with db_connect(_maintenance(dsn), autocommit=True, connect_timeout=10) as conn:
        for name in new_names:
            exists = conn.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (name,),
            ).fetchone()
            if exists:
                problems.append(f"database {name} already exists")
        sessions = _sessions(conn, (live_db,))
    return {**rights, "sessions": sessions, "problems": problems}


def _sessions(conn: Any, databases: tuple[str, ...]) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT a.pid, a.datname, a.usename, a.application_name, a.backend_type,
               a.state, COALESCE(r.rolsuper, false)
          FROM pg_stat_activity AS a
          LEFT JOIN pg_roles AS r ON r.oid = a.usesysid
         WHERE a.datname = ANY(%s) AND a.pid <> pg_backend_pid()
         ORDER BY a.pid
        """,
        (list(databases),),
    ).fetchall()
    sessions = []
    for row in rows:
        session = {
            "pid": int(row[0]),
            "database": str(row[1]),
            "user": str(row[2] or ""),
            "application": str(row[3] or ""),
            "backend_type": str(row[4] or ""),
            "state": str(row[5] or ""),
            "superuser": bool(row[6]),
        }
        session["kind"] = _session_kind(session)
        sessions.append(session)
    return sessions


def _session_kind(session: Mapping[str, Any]) -> str:
    """``timescaledb_worker``, ``client`` or ``background_worker``.

    TimescaleDB's workers name themselves in ``backend_type``, which a
    superuser sees (a restore with TimescaleDB runs as a superuser). Only
    ``timescaledb_worker`` sessions are ended. An empty ``backend_type`` is
    never taken for one: to a non-superuser every superuser session looks
    like that, a psql session included.
    """
    backend = str(session.get("backend_type") or "")
    if backend == "client backend":
        return "client"
    if backend.lower().startswith("timescaledb"):
        return "timescaledb_worker"
    return "background_worker"


# Settings whose value is a list: copied through the server's own parser so
# they stay lists. Every other setting is copied as its stored text.
_LIST_SETTINGS = frozenset({
    "search_path", "temp_tablespaces", "session_preload_libraries",
    "local_preload_libraries",
})
# Never copied, and never applied to the maintenance session.
_SKIPPED_SETTINGS = frozenset({"role", "session_authorization"})


def _copy_database_access(dsn: str, live_db: str, restored_db: str) -> dict[str, Any]:
    """Give the restored database the live one's owner, grants and settings."""
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.postgres_clone import _database_privileges, _load_spec
    from psycopg import sql

    with db_connect(_maintenance(dsn), autocommit=True, connect_timeout=10) as conn:
        live = _load_spec(conn, live_db)
        if live is None:
            return {"step": "copy_access", "ok": False, "reason": f"{live_db} is gone"}
        target = sql.Identifier(restored_db)
        conn.execute(
            sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM PUBLIC").format(target)
        )
        conn.execute(
            sql.SQL("ALTER DATABASE {} OWNER TO {}").format(
                target, sql.Identifier(live.owner),
            )
        )
        for privilege in live.privileges:
            grantee = (
                sql.SQL("PUBLIC") if privilege.grantee == "PUBLIC"
                else sql.Identifier(privilege.grantee)
            )
            conn.execute(
                sql.SQL("GRANT {} ON DATABASE {} TO {}{}").format(
                    sql.SQL(privilege.privilege),
                    target,
                    grantee,
                    sql.SQL(" WITH GRANT OPTION") if privilege.grantable else sql.SQL(""),
                )
            )
        settings = conn.execute(
            """
            SELECT CASE WHEN s.setrole = 0 THEN NULL ELSE r.rolname END,
                   unnest(s.setconfig)
              FROM pg_db_role_setting AS s
              JOIN pg_database AS d ON d.oid = s.setdatabase
              LEFT JOIN pg_roles AS r ON r.oid = s.setrole
             WHERE d.datname = %s
            """,
            (live_db,),
        ).fetchall()
        skipped: list[str] = []
        for role, setting in settings:
            name, _, value = str(setting).partition("=")
            if name in _SKIPPED_SETTINGS:
                skipped.append(f"{role or '(database)'}: {name}")
                continue
            scope = (
                sql.SQL("ALTER DATABASE {} SET {}").format(target, sql.Identifier(name))
                if role is None else
                sql.SQL("ALTER ROLE {} IN DATABASE {} SET {}").format(
                    sql.Identifier(role), target, sql.Identifier(name),
                )
            )
            if name in _LIST_SETTINGS:
                # set_config parses the list the way the server does; FROM
                # CURRENT stores it as a list again.
                with conn.transaction():
                    conn.execute("SELECT set_config(%s, %s, true)", (name, value))
                    conn.execute(sql.SQL("{} FROM CURRENT").format(scope))
            else:
                # The stored text, unchanged (30000 stays 30000, not 30s).
                conn.execute(sql.SQL("{} TO {}").format(scope, sql.Literal(value)))
        live_settings = _database_settings(conn, live_db)
        restored_settings = _database_settings(conn, restored_db)
        live_acl = set(_database_privileges(conn, live_db))
        restored_acl = set(_database_privileges(conn, restored_db))
        restored = _load_spec(conn, restored_db)
    row: dict[str, Any] = {
        "step": "copy_access",
        "owner": live.owner,
        "grants": len(live_acl),
        "settings": len(settings) - len(skipped),
    }
    if skipped:
        row["settings_not_copied"] = skipped
    if (
        restored is None or restored.owner != live.owner
        or restored_acl != live_acl or restored_settings != live_settings
    ):
        return {
            **row,
            "ok": False,
            "reason": (
                f"{restored_db} did not take {live_db}'s owner, grants and "
                "settings; nothing was swapped"
            ),
            "missing_grants": sorted(str(p) for p in live_acl - restored_acl),
            "extra_grants": sorted(str(p) for p in restored_acl - live_acl),
            "live_settings": live_settings,
            "restored_settings": restored_settings,
        }
    return {**row, "ok": True}


def _database_settings(conn: Any, database: str) -> list[list[str]]:
    rows = conn.execute(
        """
        SELECT COALESCE(r.rolname, ''), unnest(s.setconfig)
          FROM pg_db_role_setting AS s
          JOIN pg_database AS d ON d.oid = s.setdatabase
          LEFT JOIN pg_roles AS r ON r.oid = s.setrole
         WHERE d.datname = %s
        """,
        (database,),
    ).fetchall()
    return sorted(
        [str(row[0]), str(row[1])] for row in rows
        if str(row[1]).partition("=")[0] not in _SKIPPED_SETTINGS
    )


def _set_allow(conn: Any, database: str, value: bool) -> None:
    from psycopg import sql

    conn.execute(
        sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS {}").format(
            sql.Identifier(database), sql.SQL("true" if value else "false"),
        )
    )


def _rename(conn: Any, old: str, new: str) -> None:
    from psycopg import sql

    conn.execute(
        sql.SQL("ALTER DATABASE {} RENAME TO {}").format(
            sql.Identifier(old), sql.Identifier(new),
        )
    )


def _terminate(conn: Any, pid: int) -> None:
    conn.execute("SELECT pg_terminate_backend(%s)", (pid,))


def _allow_connections(conn: Any, database: str) -> bool:
    return bool(conn.execute(
        "SELECT datallowconn FROM pg_database WHERE datname = %s", (database,),
    ).fetchone()[0])


def _reopen_observed(dsn: str, wanted: Mapping[str, bool | None]) -> dict[str, Any]:
    """Reopen every named database whose original state is known; report all.

    Runs on a fresh connection, so a broken swap connection cannot leave
    the databases closed to connections. A database whose original
    ALLOW_CONNECTIONS is unknown (None) is reported and left as it is, so a
    database the operator had closed is never opened.
    """
    from okg.substrate.db.connection import connect as db_connect

    observed: dict[str, Any] = {}
    with db_connect(_maintenance(dsn), autocommit=True, connect_timeout=10) as conn:
        for name, allow in wanted.items():
            row = conn.execute(
                "SELECT datallowconn FROM pg_database WHERE datname = %s", (name,),
            ).fetchone()
            if row is None:
                observed[name] = {"exists": False}
                continue
            current = bool(row[0])
            if allow is not None and current != allow:
                _set_allow(conn, name, allow)
                current = allow
            observed[name] = {"exists": True, "allow_connections": current}
    return observed


def _fence_drain_swap(
    dsn: str,
    live_db: str,
    restored_db: str,
    kept_db: str,
    *,
    drain_timeout_seconds: float,
) -> dict[str, Any]:
    """Fence both databases, drain, swap by rename; refuse if clients stay.

    Whatever happens after the fence, both databases are put back to the
    connection setting they had and the report names the databases
    actually there.
    """
    from okg.substrate.db.connection import connect as db_connect

    row: dict[str, Any] = {"step": "swap", "swapped": False}
    live_allowed: bool | None = None
    restored_allowed: bool | None = None
    result: dict[str, Any]
    try:
        with db_connect(_maintenance(dsn), autocommit=True, connect_timeout=10) as conn:
            live_allowed = _allow_connections(conn, live_db)
            restored_allowed = _allow_connections(conn, restored_db)
            _set_allow(conn, live_db, False)
            _set_allow(conn, restored_db, False)
            result = _drain_and_rename(
                conn, row, live_db, restored_db, kept_db,
                drain_timeout_seconds=drain_timeout_seconds,
            )
    except Exception as exc:  # noqa: BLE001 - reported; databases reopened below.
        result = {
            **row,
            "ok": False,
            "reason": f"the swap raised {type(exc).__name__}",
            "error": diu._error_summary(exc),
        }
    finally:
        try:
            observed = _reopen_observed(dsn, {
                live_db: live_allowed, kept_db: live_allowed,
                restored_db: restored_allowed,
            })
        except Exception as exc:  # noqa: BLE001 - reported, never hidden.
            observed = {"reopen_error": diu._error_summary(exc)}
    result["observed_databases"] = observed
    if "reopen_error" not in observed:
        result["swapped"] = (
            observed[live_db]["exists"] and observed[kept_db]["exists"]
            and not observed[restored_db]["exists"]
        )
    if result.get("ok") and not result.get("swapped"):
        result.update({"ok": False, "reason": "the rename did not leave the expected databases"})
    return result


def _drain_and_rename(
    conn: Any,
    row: Mapping[str, Any],
    live_db: str,
    restored_db: str,
    kept_db: str,
    *,
    drain_timeout_seconds: float,
) -> dict[str, Any]:
    terminated: dict[int, dict[str, Any]] = {}
    deadline = time.monotonic() + drain_timeout_seconds
    while True:
        sessions = _sessions(conn, (live_db, restored_db))
        for session in sessions:
            if session["kind"] == "timescaledb_worker" and session["pid"] not in terminated:
                _terminate(conn, session["pid"])
                terminated[session["pid"]] = session
        others = [s for s in sessions if s["kind"] != "timescaledb_worker"]
        if not sessions:
            break
        if time.monotonic() >= deadline:
            kinds = sorted({s["kind"] for s in others}) or ["timescaledb_worker"]
            return {
                **row,
                "ok": False,
                "reason": (
                    f"sessions stayed connected ({', '.join(kinds)}); both "
                    "databases were reopened and nothing was swapped. Stop "
                    "the clients (for example `okg mcp-serve`) and run the "
                    "restore again"
                ),
                "remaining_sessions": others or sessions,
                "terminated_timescaledb_workers": list(terminated.values()),
            }
        time.sleep(0.2)
    _rename(conn, live_db, kept_db)
    try:
        _rename(conn, restored_db, live_db)
    except Exception as exc:  # noqa: BLE001 - undone, then reported.
        _rename(conn, kept_db, live_db)
        return {
            **row,
            "ok": False,
            "reason": (
                f"renaming {restored_db} to {live_db} failed; {live_db} was "
                "given its name back"
            ),
            "error": diu._error_summary(exc),
            "terminated_timescaledb_workers": list(terminated.values()),
        }
    return {
        **row,
        "ok": True,
        "swapped": True,
        "terminated_timescaledb_workers": list(terminated.values()),
    }


def _start_timescaledb_workers(dsn: str, live_db: str) -> dict[str, Any]:
    """Start TimescaleDB's scheduler for the swapped-in database, and see it."""
    from okg.substrate.db.connection import connect as db_connect

    row: dict[str, Any] = {"step": "timescaledb_workers"}
    with db_connect(dsn, autocommit=True, connect_timeout=10) as conn:
        if conn.execute(
            "SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'"
        ).fetchone() is None:
            return {**row, "ok": True, "skipped": "the database has no TimescaleDB"}
        conn.execute("SELECT _timescaledb_functions.start_background_workers()")
    deadline = time.monotonic() + SCHEDULER_START_TIMEOUT_SECONDS
    with db_connect(_maintenance(dsn), autocommit=True, connect_timeout=10) as conn:
        while True:
            workers = [
                s for s in _sessions(conn, (live_db,))
                if s["kind"] == "timescaledb_worker"
            ]
            if workers:
                return {**row, "ok": True, "workers": workers}
            if time.monotonic() >= deadline:
                return {
                    **row,
                    "ok": False,
                    "reason": (
                        f"TimescaleDB's scheduler did not start for {live_db}; "
                        "run `SELECT _timescaledb_functions.start_background_workers()` "
                        "in it, or restart PostgreSQL"
                    ),
                }
            time.sleep(0.5)


# ------------------------------------------------------------------ receipt


def _put_back_receipt(
    deployment: str, sidecar: Mapping[str, Any], stamp: str,
) -> dict[str, Any]:
    """The receipt as it was before the upgrade; the current one moved aside."""
    current = diu.receipt_path(deployment)
    row: dict[str, Any] = {"step": "receipt"}
    if current.exists():
        aside = current.with_name(f"installed-release.pre-restore-{stamp}.json")
        current.replace(aside)
        row["moved_aside"] = str(aside)
    previous = sidecar.get("previous_receipt")
    if isinstance(previous, Mapping):
        diu._write_private_json(current, previous)
        row["restored_receipt_version"] = previous.get("okg_version")
    else:
        row["restored_receipt_version"] = None
        row["note"] = (
            "the deployment had no receipt before the upgrade; the next `okg "
            "up` needs --from-contract or --accept-unknown-compat"
        )
    marker = diu.marker_path(deployment)
    if marker.exists():
        aside = marker.with_name(f"upgrade-in-progress.pre-restore-{stamp}.json")
        marker.replace(aside)
        row["marker_moved_aside"] = str(aside)
    return {**row, "ok": True}


def _record(deployment: str, stamp: str, report: dict[str, Any]) -> None:
    """Write the restore record; a failure to write it is reported, not fatal."""
    path = diu.state_dir(deployment) / RESTORES_NAME / f"restore-{stamp}.json"
    try:
        report["record"] = str(diu._write_private_json(path, report))
    except Exception as exc:  # noqa: BLE001 - the restore itself is unaffected.
        report["record"] = None
        report["record_error"] = diu._error_summary(exc)


def _worker_summary(deployment: str) -> dict[str, Any]:
    audit = diu._worker_audit(deployment)
    return {
        "status": audit.get("status"),
        "running": bool(audit.get("root_count")),
    }


def _next_action_on_success(
    deployment: str, sidecar: Mapping[str, Any], worker_stopped: bool,
    *, start_command: str,
) -> str:
    parts = [
        diu._go_back_hint(sidecar.get("from_version")),
        f"then check it with `okg deployment ready {deployment}`",
    ]
    if worker_stopped:
        parts.append("then start the worker with " + start_command)
    parts.append("drop the kept database once you no longer need it")
    return "; ".join(parts)


def _next_action_on_failure(
    deployment: str,
    steps: list[str],
    state: Mapping[str, Any],
    live_db: str,
    restored_db: str,
    kept_db: str,
    *,
    worker_stopped: bool,
    start_command: str,
) -> str:
    parts = [
        f"the restore stopped at {steps[0]}" if len(steps) == 1
        else "these restore steps failed: " + ", ".join(steps)
    ]
    observed = state.get("observed")
    if isinstance(observed, Mapping) and "reopen_error" in observed:
        parts.append(
            "the databases could not be checked or reopened after the swap "
            f"failed; look for {live_db}, {kept_db} and {restored_db} and "
            "their ALLOW_CONNECTIONS setting by hand"
        )
    elif isinstance(observed, Mapping) and not observed.get(live_db, {}).get("exists"):
        present = sorted(name for name, row in observed.items() if row.get("exists"))
        parts.append(
            f"no database is named {live_db} now (present: "
            f"{', '.join(present) or 'none'}); rename {kept_db} back to "
            f"{live_db} before anything else connects"
        )
    elif state.get("swapped"):
        parts.append(
            f"{live_db} now holds the restored copy and the database before "
            f"the restore is kept as {kept_db}"
        )
        workers = state.get("timescaledb_workers") or {}
        if workers.get("ok"):
            parts.append(
                "TimescaleDB's scheduler is running again"
                if not workers.get("skipped") else "the database has no TimescaleDB"
            )
        else:
            parts.append(
                f"TimescaleDB's scheduler is NOT running for {live_db}: run "
                "`SELECT _timescaledb_functions.start_background_workers()` in it, "
                "or restart PostgreSQL"
            )
        receipt = state.get("receipt") or {}
        if receipt.get("ok"):
            parts.append(
                "the receipt from before the upgrade is back in place "
                f"(okg {receipt.get('restored_receipt_version') or 'none recorded'}) "
                "and the unfinished-upgrade marker is moved aside, so a later "
                "`okg up` sees the restored database"
            )
        else:
            parts.append(
                "the receipt and the unfinished-upgrade marker were NOT put "
                "back: do not run `okg up` before fixing them by hand (the "
                "previous receipt is in the archive's sidecar)"
            )
    else:
        parts.append(f"{live_db} was not changed")
        if state.get("restored_created"):
            parts.append(
                f"the partial or unmatched copy {restored_db} is kept for "
                "inspection; drop it before trying again"
            )
    if worker_stopped:
        parts.append(
            "the worker is stopped; if you stay on this okg, start it with "
            + start_command
        )
    return "; ".join(parts)
