"""Safe PostgreSQL dump/restore cloning for development databases.

The helper deliberately never uses a live database as a CREATE DATABASE
template and never signals a target backend.  TimescaleDB workers are stopped
through the extension's supported restore hooks; pg_cron state is server-bound
and is filtered from the restore archive.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import psycopg
from okg.substrate.db.connection import connect as db_connect
from psycopg import sql
from psycopg.pq import Conninfo

from okg.substrate.db import dsn as dsn_mod
from okg.substrate.errors import DatabaseCloneError, SubstrateError
from okg.substrate.postgres_restore import timescaledb_restore_context


_MAINTENANCE_DATABASES = frozenset({"postgres", "template0", "template1"})
_CRON_TOC_PATTERNS = (
    re.compile(r" EXTENSION - pg_cron(?: |$)"),
    re.compile(r" COMMENT - EXTENSION pg_cron(?: |$)"),
    re.compile(
        r" (?:SCHEMA|ACL|COMMENT|SECURITY LABEL) - "
        r"(?:SCHEMA )?cron(?: |$)"
    ),
    re.compile(
        r" (?:TABLE|TABLE DATA|TABLE ATTACH|SEQUENCE|SEQUENCE SET|"
        r"SEQUENCE OWNED BY|FUNCTION|PROCEDURE|TYPE|DOMAIN|VIEW|"
        r"MATERIALIZED VIEW|INDEX|CONSTRAINT|FK CONSTRAINT|TRIGGER|"
        r"RULE|POLICY|ACL|DEFAULT ACL|COMMENT|SECURITY LABEL) "
        r"cron(?: |$)"
    ),
)
_TIMESCALE_TOC_PATTERNS = (
    re.compile(r" EXTENSION - timescaledb(?: |$)"),
    re.compile(r" COMMENT - EXTENSION timescaledb(?: |$)"),
)
_DATABASE_PRIVILEGES = frozenset({"CONNECT", "CREATE", "TEMPORARY"})


@dataclass(frozen=True)
class ServerIdentity:
    system_identifier: str
    address: str
    port: int


@dataclass(frozen=True)
class DatabasePrivilege:
    grantee: str
    privilege: str
    grantable: bool


@dataclass(frozen=True)
class DatabaseSpec:
    database: str
    owner: str
    encoding: str
    collate: str
    ctype: str
    allow_connections: bool
    timescaledb_version: str | None
    privileges: tuple[DatabasePrivilege, ...]
    server: ServerIdentity


@dataclass(frozen=True)
class ToolRoute:
    conninfo: str
    env: Mapping[str, str]
    server: ServerIdentity


@dataclass(frozen=True)
class TargetContext:
    target_dsn: str
    target_maintenance_dsn: str
    maintenance_dsn: str
    database: str
    restore_actor: str
    intended_owner: str
    intended_allow_connections: bool
    intended_privileges: tuple[DatabasePrivilege, ...]
    existing: DatabaseSpec | None
    template: DatabaseSpec
    server: ServerIdentity


def _error(message: str, *, stage: str, **data: Any) -> DatabaseCloneError:
    return DatabaseCloneError(message, data={"stage": stage, **data})


def _same_postgres_cluster(left: ServerIdentity, right: ServerIdentity) -> bool:
    return left.system_identifier == right.system_identifier


def _database_name(dsn: str, *, stage: str) -> str:
    try:
        name = str(dsn_mod.parse(dsn).dbname or "")
    except dsn_mod.DsnError as exc:
        raise _error("database DSN is invalid", stage=stage) from exc
    if not name:
        raise _error("database DSN does not name a database", stage=stage)
    if name in _MAINTENANCE_DATABASES:
        raise _error(
            "refusing to clone a maintenance database",
            stage=stage,
            database=name,
        )
    return name


def _maintenance_dsn(dsn: str) -> str:
    """``dsn`` pointed at ``postgres``, same form, every other parameter kept."""
    return dsn_mod.parse(dsn).with_dbname("postgres").render()


def _keyword_conninfo(parameters: Mapping[str, str]) -> str:
    """Resolved connection parameters as a keyword-form conninfo for libpq."""
    return dsn_mod.Dsn(form="keyword", scheme=None, params=parameters).render()


def _server_identity(conn: psycopg.Connection[Any]) -> ServerIdentity:
    row = conn.execute(
        "SELECT system_identifier::text, "
        "COALESCE(inet_server_addr()::text, '<local>'), "
        "inet_server_port() FROM pg_control_system()"
    ).fetchone()
    if row is None or row[2] is None:
        raise _error("cannot identify PostgreSQL server", stage="preflight")
    return ServerIdentity(
        system_identifier=str(row[0]),
        address=str(row[1]),
        port=int(row[2]),
    )


def _database_privileges(
    conn: psycopg.Connection[Any],
    database: str,
) -> tuple[DatabasePrivilege, ...]:
    rows = conn.execute(
        """
        SELECT CASE
                   WHEN acl.grantee = 0 THEN 'PUBLIC'
                   ELSE role.rolname
               END,
               acl.privilege_type,
               acl.is_grantable
          FROM pg_database AS database
          CROSS JOIN LATERAL aclexplode(
              COALESCE(
                  database.datacl,
                  acldefault('d', database.datdba)
              )
          ) AS acl
          LEFT JOIN pg_roles AS role ON role.oid = acl.grantee
         WHERE database.datname = %s
         ORDER BY 1, 2, 3
        """,
        (database,),
    ).fetchall()
    privileges: list[DatabasePrivilege] = []
    for grantee, privilege, grantable in rows:
        if not grantee:
            raise _error(
                "database ACL refers to a role that no longer exists",
                stage="acl-preflight",
                database=database,
            )
        privilege_name = str(privilege)
        if privilege_name not in _DATABASE_PRIVILEGES:
            raise _error(
                "database ACL contains an unsupported privilege",
                stage="acl-preflight",
                database=database,
                privilege=privilege_name,
            )
        privileges.append(
            DatabasePrivilege(
                grantee=str(grantee),
                privilege=privilege_name,
                grantable=bool(grantable),
            )
        )
    return tuple(privileges)


def _new_database_privileges() -> tuple[DatabasePrivilege, ...]:
    return (
        DatabasePrivilege("PUBLIC", "CONNECT", False),
        DatabasePrivilege("PUBLIC", "TEMPORARY", False),
    )


def _load_spec(
    conn: psycopg.Connection[Any],
    database: str,
) -> DatabaseSpec | None:
    row = conn.execute(
        """
        SELECT pg_get_userbyid(datdba),
               pg_encoding_to_char(encoding),
               datcollate,
               datctype,
               datallowconn
          FROM pg_database
         WHERE datname = %s
        """,
        (database,),
    ).fetchone()
    if row is None:
        return None
    server = _server_identity(conn)
    privileges = _database_privileges(conn, database)
    version: str | None = None
    if database == conn.info.dbname:
        extension = conn.execute(
            "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
        ).fetchone()
        version = str(extension[0]) if extension else None
    return DatabaseSpec(
        database=database,
        owner=str(row[0]),
        encoding=str(row[1]),
        collate=str(row[2]),
        ctype=str(row[3]),
        allow_connections=bool(row[4]),
        timescaledb_version=version,
        privileges=privileges,
        server=server,
    )


def _source_spec(source_dsn: str) -> DatabaseSpec:
    database = _database_name(source_dsn, stage="source-preflight")
    try:
        with db_connect(
            source_dsn,
            autocommit=True,
            connect_timeout=5,
        ) as conn:
            spec = _load_spec(conn, database)
    except psycopg.Error as exc:
        raise _error(
            "cannot inspect source database",
            stage="source-preflight",
            database=database,
        ) from exc
    if spec is None:
        raise _error(
            "source database does not exist",
            stage="source-preflight",
            database=database,
        )
    return spec


def _target_context(
    target_dsn: str,
    *,
    admin_dsn: str | None,
) -> TargetContext:
    database = _database_name(target_dsn, stage="target-preflight")
    target_maintenance_dsn = _maintenance_dsn(target_dsn)
    maintenance_dsn = (
        _maintenance_dsn(admin_dsn) if admin_dsn else target_maintenance_dsn
    )
    try:
        with db_connect(
            target_maintenance_dsn,
            autocommit=True,
            connect_timeout=5,
        ) as target_connection:
            target_server = _server_identity(target_connection)
            restore_actor = str(
                target_connection.execute("SELECT current_user").fetchone()[0]
            )
            pinned_target_dsn = _pinned_python_dsn(
                target_connection,
                database=database,
            )
            pinned_target_maintenance_dsn = _pinned_python_dsn(
                target_connection,
                database="postgres",
            )
        with db_connect(
            maintenance_dsn,
            autocommit=True,
            connect_timeout=5,
        ) as conn:
            admin_server = _server_identity(conn)
            if not _same_postgres_cluster(admin_server, target_server):
                raise _error(
                    "admin and target DSNs resolve to different PostgreSQL servers",
                    stage="target-preflight",
                    target_server=target_server.__dict__,
                    admin_server=admin_server.__dict__,
                )
            pinned_maintenance_dsn = _pinned_python_dsn(
                conn,
                database="postgres",
            )
            role = conn.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s)",
                (restore_actor,),
            ).fetchone()
            if role is None or not bool(role[0]):
                raise _error(
                    "target restore role does not exist",
                    stage="target-preflight",
                    role=restore_actor,
                )
            existing = _load_spec(conn, database)
            template = _load_spec(conn, "template0")
            if template is None:
                raise _error(
                    "target server has no template0 database",
                    stage="target-preflight",
                )
            cron_row = conn.execute(
                "SELECT current_setting('cron.database_name', true)"
            ).fetchone()
            cron_database = str(cron_row[0] or "") if cron_row else ""
            if cron_database == database:
                raise _error(
                    "target database is bound to the pg_cron background worker",
                    stage="target-preflight",
                    database=database,
                    fix="clone into a database that is not cron.database_name",
                )
    except DatabaseCloneError:
        raise
    except psycopg.Error as exc:
        raise _error(
            "cannot inspect target server",
            stage="target-preflight",
            database=database,
        ) from exc
    return TargetContext(
        target_dsn=pinned_target_dsn,
        target_maintenance_dsn=pinned_target_maintenance_dsn,
        maintenance_dsn=pinned_maintenance_dsn,
        database=database,
        restore_actor=restore_actor,
        intended_owner=existing.owner if existing is not None else restore_actor,
        intended_allow_connections=(
            existing.allow_connections if existing is not None else True
        ),
        intended_privileges=(
            existing.privileges
            if existing is not None
            else _new_database_privileges()
        ),
        existing=existing,
        template=template,
        server=target_server,
    )


def _require_distinct(source: DatabaseSpec, target: TargetContext) -> None:
    if source.database == target.database and _same_postgres_cluster(
        source.server,
        target.server,
    ):
        raise _error(
            "source and target resolve to the same database",
            stage="preflight",
            database=source.database,
            server=source.server.__dict__,
        )


def _require_timescaledb_version(
    target: TargetContext,
    version: str | None,
) -> None:
    if version is None:
        return
    try:
        with db_connect(
            target.maintenance_dsn,
            autocommit=True,
            connect_timeout=5,
        ) as conn:
            row = conn.execute(
                "SELECT EXISTS ("
                "SELECT 1 FROM pg_available_extension_versions "
                "WHERE name = 'timescaledb' AND version = %s)",
                (version,),
            ).fetchone()
    except psycopg.Error as exc:
        raise _error(
            "cannot inspect target TimescaleDB versions",
            stage="version-preflight",
        ) from exc
    if row is None or not bool(row[0]):
        raise _error(
            "source TimescaleDB version is unavailable on target server",
            stage="version-preflight",
            version=version,
        )


#: The PostgreSQL client tools backups, restores and clones run.
CLIENT_TOOLS = ("pg_dump", "pg_restore")
#: How to get them; issue #3587 found no doc or error that said.
CLIENT_TOOLS_FIX_HINT = (
    "install the PostgreSQL client tools (pg_dump and pg_restore) of the "
    "database server's major version or newer, PostgreSQL 17 for okg's "
    "bundled database, and put them on PATH: on macOS `brew install libpq` "
    "and add its bin folder to PATH; on Debian or Ubuntu the "
    "postgresql-client-17 package from apt.postgresql.org; on RHEL-like "
    "systems the postgresql17 package from the PostgreSQL yum repository"
)
_TOOL_VERSION = re.compile(r"\(PostgreSQL\)\s+(\d+)(?:\.(\d+))?")


def _missing_tool_message(name: str) -> str:
    return (
        f"{name} is not on PATH; install the PostgreSQL client tools "
        "(pg_dump and pg_restore)"
    )


def _tool(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        raise DatabaseCloneError(
            _missing_tool_message(name),
            data={"stage": "tool-preflight", "tool": name},
            fix_hint=CLIENT_TOOLS_FIX_HINT,
        )
    return path


def client_tool_major(path: str) -> int | None:
    """The major version ``path --version`` reports, or None if unreadable."""
    try:
        done = subprocess.run(
            [path, "--version"], capture_output=True, text=True, timeout=30,
            stdin=subprocess.DEVNULL, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    match = _TOOL_VERSION.search(done.stdout or "")
    return int(match.group(1)) if match else None


def server_major(dsn: str) -> int | None:
    """The server's major version, or None when it cannot be asked."""
    try:
        with db_connect(dsn, autocommit=True, connect_timeout=5) as conn:
            row = conn.execute("SHOW server_version_num").fetchone()
    except psycopg.Error:
        return None
    try:
        return int(str(row[0])) // 10000 if row else None
    except (TypeError, ValueError):
        return None


def client_tool_problems(
    required_major: int | None,
    tools: tuple[str, ...] = CLIENT_TOOLS,
) -> list[dict[str, Any]]:
    """Each tool that is missing from PATH or older than ``required_major``.

    pg_dump refuses a server newer than itself and pg_restore an archive
    from a newer pg_dump, so both must be at least the server's major
    version. ``required_major`` None checks presence only. Each problem
    carries ``message``, a sentence naming the tool.
    """
    return client_tool_report(required_major, tools)["problems"]


def client_tool_report(
    required_major: int | None,
    tools: tuple[str, ...] = CLIENT_TOOLS,
) -> dict[str, Any]:
    """``problems`` (see ``client_tool_problems``) plus each tool's version.

    A version that cannot be read is reported as ``None`` and never blocks:
    builds such as EDB's print their own format. pg_restore older than the
    pg_dump beside it is a problem too, because it cannot read the archive
    that pg_dump writes.
    """
    problems: list[dict[str, Any]] = []
    versions: dict[str, int | None] = {}
    paths: dict[str, str] = {}
    for name in tools:
        path = shutil.which(name)
        if path is None:
            problems.append({
                "tool": name, "problem": "missing",
                "message": _missing_tool_message(name),
            })
            continue
        paths[name] = path
        major = versions[name] = client_tool_major(path)
        if required_major is not None and major is not None and major < required_major:
            problems.append({
                "tool": name, "problem": "too_old", "path": path,
                "major": major, "required_major": required_major,
                "message": (
                    f"{name} at {path} is PostgreSQL {major}; the database "
                    f"server is PostgreSQL {required_major}, and {name} must "
                    "be that version or newer"
                ),
            })
    dump, restore = versions.get("pg_dump"), versions.get("pg_restore")
    if (
        dump is not None and restore is not None and restore < dump
        and not any(p["tool"] == "pg_restore" for p in problems)
    ):
        problems.append({
            "tool": "pg_restore", "problem": "older_than_pg_dump",
            "path": paths["pg_restore"], "major": restore, "pg_dump_major": dump,
            "message": (
                f"pg_restore at {paths['pg_restore']} is PostgreSQL {restore} "
                f"but pg_dump at {paths['pg_dump']} is PostgreSQL {dump}; "
                "pg_restore cannot read an archive from a newer pg_dump, so "
                "put tools of one version on PATH"
            ),
        })
    return {
        "problems": problems,
        "versions": versions,
        "unknown_version": sorted(
            name for name, major in versions.items() if major is None
        ),
    }


_TOOL_SECRET_PARAMETERS = frozenset(
    {
        "sslpassword",
        "oauth_client_secret",
        "scram_client_key",
        "scram_server_key",
    }
)


def describe_client_tool_problems(problems: list[dict[str, Any]]) -> str:
    """One sentence for the problems ``client_tool_problems`` returned."""
    missing = [str(p["tool"]) for p in problems if p["problem"] == "missing"]
    parts = []
    if missing:
        parts.append(
            " and ".join(missing)
            + (" are" if len(missing) > 1 else " is")
            + " not on PATH"
        )
    parts.extend(str(p["message"]) for p in problems if p["problem"] != "missing")
    return "; ".join(parts) + "; install the PostgreSQL client tools (pg_dump and pg_restore)"


def require_client_tools(
    *dsns: str, tools: tuple[str, ...] = CLIENT_TOOLS,
) -> None:
    """Refuse, naming the tool, before a missing or too-old one is needed.

    The required version is the newest server among ``dsns`` that answers.
    """
    majors = [major for major in (server_major(dsn) for dsn in dsns) if major]
    problems = client_tool_problems(max(majors) if majors else None, tools)
    if problems:
        raise DatabaseCloneError(
            describe_client_tool_problems(problems),
            data={
                "stage": "tool-preflight",
                "tool": problems[0]["tool"],
                "problems": problems,
            },
            fix_hint=CLIENT_TOOLS_FIX_HINT,
        )


def _pinned_connection_parameters(
    conn: psycopg.Connection[Any],
    *,
    database: str,
) -> tuple[dict[str, str], str]:
    parameters = conn.info.get_parameters()
    unsupported_secrets = sorted(
        key for key in _TOOL_SECRET_PARAMETERS if parameters.get(key)
    )
    if unsupported_secrets:
        raise _error(
            "PostgreSQL route uses credentials that cannot be preserved safely",
            stage="tool-preflight",
            unsupported_parameters=unsupported_secrets,
        )
    parameters.pop("service", None)
    parameters["dbname"] = database
    parameters["host"] = conn.info.host
    if conn.info.hostaddr:
        parameters["hostaddr"] = conn.info.hostaddr
    else:
        parameters.pop("hostaddr", None)
    parameters["port"] = str(conn.info.port)
    parameters["user"] = conn.info.user
    return parameters, str(conn.info.password or "")


def _pinned_python_dsn(
    conn: psycopg.Connection[Any],
    *,
    database: str,
) -> str:
    parameters, password = _pinned_connection_parameters(
        conn,
        database=database,
    )
    if password and not parameters.get("passfile"):
        parameters["password"] = password
    return _keyword_conninfo(parameters)


def _resolved_tool_route(
    dsn: str,
    *,
    database: str,
    probe_database: str,
    expected_server: ServerIdentity,
) -> ToolRoute:
    """Resolve every libpq option, then pin tools to the probed endpoint."""
    probe_dsn = dsn_mod.parse(dsn).with_dbname(probe_database).render()
    try:
        with db_connect(
            probe_dsn,
            autocommit=True,
            connect_timeout=5,
        ) as conn:
            observed_server = _server_identity(conn)
            if not _same_postgres_cluster(observed_server, expected_server):
                raise _error(
                    "PostgreSQL tool route resolves to an unexpected server",
                    stage="tool-preflight",
                    expected_server=expected_server.__dict__,
                    observed_server=observed_server.__dict__,
                )
            # A service or multi-host DSN is fully expanded by libpq above.
            # Remove the indirection and pin the exact endpoint that supplied
            # the verified system identifier, while retaining every other
            # effective non-default connection option.
            parameters, password = _pinned_connection_parameters(
                conn,
                database=database,
            )
    except DatabaseCloneError:
        raise
    except psycopg.Error as exc:
        raise _error(
            "cannot resolve PostgreSQL tool route",
            stage="tool-preflight",
            database=database,
        ) from exc

    env = dict(os.environ)
    for option in Conninfo.get_defaults():
        if option.envvar:
            env.pop(option.envvar.decode(), None)
    env.pop("PGSERVICEFILE", None)
    env.pop("PGSYSCONFDIR", None)
    if password:
        env["PGPASSWORD"] = password
    return ToolRoute(
        conninfo=_keyword_conninfo(parameters),
        env=env,
        server=observed_server,
    )


def _run(
    argv: list[str],
    *,
    operation: str,
    env: Mapping[str, str] | None = None,
    redact_values: frozenset[str] = frozenset(),
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        argv,
        text=True,
        capture_output=True,
        check=False,
        env=dict(env) if env is not None else None,
    )
    if result.returncode != 0:
        raise _error(
            f"{operation} failed",
            stage=operation,
            command=[
                "<verified-conninfo>"
                if item in redact_values
                else "<redacted-dsn>"
                if "://" in item
                else item
                for item in argv
            ],
            returncode=result.returncode,
            stdout=result.stdout[-4000:],
            stderr=result.stderr[-4000:],
        )
    return result


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _filtered_restore_toc(
    listing: str,
    *,
    timescaledb_prestaged: bool,
) -> str:
    patterns = list(_CRON_TOC_PATTERNS)
    if timescaledb_prestaged:
        patterns.extend(_TIMESCALE_TOC_PATTERNS)
    return "\n".join(
        line
        for line in listing.splitlines()
        if not any(pattern.search(line) for pattern in patterns)
    ) + "\n"


def _archive_extensions(listing: str) -> frozenset[str]:
    return frozenset(
        match.group(1)
        for line in listing.splitlines()
        if (match := re.search(r" EXTENSION - ([^\s]+)(?: |$)", line))
    )


def _prepare_restore_toc(
    dump_path: Path,
    *,
    timescaledb_prestaged: bool,
) -> tuple[Path, str]:
    if not dump_path.is_file() or dump_path.stat().st_size == 0:
        raise _error(
            "clone dump is missing or empty",
            stage="dump-preflight",
            path=str(dump_path),
        )
    listing = _run(
        [_tool("pg_restore"), "--list", str(dump_path)],
        operation="dump-preflight",
    ).stdout
    archive_extensions = _archive_extensions(listing)
    archive_has_timescaledb = "timescaledb" in archive_extensions
    if archive_has_timescaledb != timescaledb_prestaged:
        raise _error(
            "clone dump TimescaleDB metadata does not match the live source",
            stage="dump-preflight",
            archive_has_timescaledb=archive_has_timescaledb,
            source_has_timescaledb=timescaledb_prestaged,
        )
    filtered = _filtered_restore_toc(
        listing,
        timescaledb_prestaged=timescaledb_prestaged,
    )
    if not any(line and not line.startswith(";") for line in filtered.splitlines()):
        raise _error(
            "clone dump has no restorable entries",
            stage="dump-preflight",
            path=str(dump_path),
        )
    fd, name = tempfile.mkstemp(prefix="okg-clone-", suffix=".toc")
    os.close(fd)
    toc_path = Path(name)
    toc_path.write_text(filtered, encoding="utf-8")
    toc_path.chmod(0o600)
    return toc_path, _file_digest(dump_path)


def _remaining_sessions(
    conn: psycopg.Connection[Any],
    *,
    database: str,
    guard_pid: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT pid, usename, application_name, backend_type, state
          FROM pg_stat_activity
         WHERE datname = %s AND pid <> %s
         ORDER BY pid
        """,
        (database, guard_pid),
    ).fetchall()
    return [
        {
            "pid": int(row[0]),
            "user": str(row[1] or ""),
            "application": str(row[2] or ""),
            "backend_type": str(row[3] or ""),
            "state": str(row[4] or ""),
        }
        for row in rows
    ]


def _compensate_original_guard(
    target: TargetContext,
    *,
    guard: psycopg.Connection[Any] | None,
    pre_restore_used: bool,
    target_state: str,
) -> tuple[str, str | None]:
    if guard is None or not pre_restore_used:
        return target_state, None
    try:
        guard.execute("SELECT timescaledb_post_restore()")
        if target_state == "original-quiesced":
            return "original-open", None
        return target_state, None
    except psycopg.Error as cleanup_exc:
        cleanup_error = str(cleanup_exc)
    if target_state == "original-fenced":
        return target_state, cleanup_error
    try:
        with db_connect(
            target.target_maintenance_dsn,
            autocommit=True,
            connect_timeout=5,
        ) as maintenance:
            cleanup_server = _server_identity(maintenance)
            if not _same_postgres_cluster(cleanup_server, target.server):
                raise _error(
                    "cleanup maintenance connection changed PostgreSQL servers",
                    stage="target-quiesce-cleanup",
                    expected_server=target.server.__dict__,
                    observed_server=cleanup_server.__dict__,
                )
            maintenance.execute(
                sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(
                    sql.Identifier(target.database)
                )
            )
        return "original-fenced", cleanup_error
    except (DatabaseCloneError, psycopg.Error) as fence_exc:
        return (
            "original-cleanup-uncertain",
            f"{cleanup_error}; refence failed: {fence_exc}",
        )


def _replace_database_fenced(
    target: TargetContext,
    *,
    source_shape: DatabaseSpec,
    drain_timeout_seconds: float,
) -> bool:
    if drain_timeout_seconds <= 0:
        raise _error(
            "database drain timeout must be positive",
            stage="target-preflight",
        )
    existed = target.existing is not None
    guard: psycopg.Connection[Any] | None = None
    pre_restore_used = False
    target_state = (
        "original-open"
        if existed and target.existing and target.existing.allow_connections
        else "original-fenced"
        if existed
        else "target-absent"
    )
    try:
        if existed and target.existing and target.existing.allow_connections:
            guard = db_connect(
                target.target_dsn,
                autocommit=True,
                connect_timeout=5,
            )
            guard_server = _server_identity(guard)
            if not _same_postgres_cluster(guard_server, target.server):
                raise _error(
                    "target guard connection changed PostgreSQL servers",
                    stage="target-quiesce",
                    target_state=target_state,
                    target_fenced=False,
                    expected_server=target.server.__dict__,
                    observed_server=guard_server.__dict__,
                )
            extension = guard.execute(
                "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
            ).fetchone()
            if extension is not None:
                guard.execute("SELECT timescaledb_pre_restore()")
                pre_restore_used = True
                target_state = "original-quiesced"
        with db_connect(
            target.maintenance_dsn,
            autocommit=True,
            connect_timeout=5,
        ) as maintenance:
            maintenance_server = _server_identity(maintenance)
            if not _same_postgres_cluster(maintenance_server, target.server):
                raise _error(
                    "maintenance connection changed PostgreSQL servers",
                    stage="target-recreate",
                    target_state=target_state,
                    target_fenced=target_state == "original-fenced",
                    expected_server=target.server.__dict__,
                    observed_server=maintenance_server.__dict__,
                )
            if existed:
                maintenance.execute(
                    sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(
                        sql.Identifier(target.database)
                    )
                )
                target_state = "original-fenced"
                deadline = time.monotonic() + drain_timeout_seconds
                remaining = _remaining_sessions(
                    maintenance,
                    database=target.database,
                    guard_pid=(
                        guard.info.backend_pid
                        if guard is not None
                        else maintenance.info.backend_pid
                    ),
                )
                while remaining and time.monotonic() < deadline:
                    time.sleep(min(0.1, drain_timeout_seconds))
                    remaining = _remaining_sessions(
                        maintenance,
                        database=target.database,
                        guard_pid=(
                            guard.info.backend_pid
                            if guard is not None
                            else maintenance.info.backend_pid
                        ),
                    )
                if remaining:
                    cleanup_error: str | None = None
                    if pre_restore_used:
                        try:
                            guard.execute("SELECT timescaledb_post_restore()")
                            pre_restore_used = False
                        except psycopg.Error as exc:
                            cleanup_error = str(exc)
                    if (
                        cleanup_error is None
                        and target.existing
                        and target.existing.allow_connections
                    ):
                        try:
                            maintenance.execute(
                                sql.SQL(
                                    "ALTER DATABASE {} ALLOW_CONNECTIONS true"
                                ).format(sql.Identifier(target.database))
                            )
                            target_state = "original-open"
                        except psycopg.Error as exc:
                            cleanup_error = str(exc)
                    exists_row = maintenance.execute(
                        "SELECT EXISTS ("
                        "SELECT 1 FROM pg_database WHERE datname = %s)",
                        (target.database,),
                    ).fetchone()
                    raise _error(
                        "target database sessions did not drain",
                        stage="target-drain",
                        database=target.database,
                        remaining_sessions=remaining,
                        cleanup_error=cleanup_error,
                        original_target_exists=bool(exists_row and exists_row[0]),
                        target_state=target_state,
                        target_fenced=target_state == "original-fenced",
                    )
                if guard is not None:
                    guard.close()
                    guard = None
                try:
                    maintenance.execute(
                        sql.SQL("DROP DATABASE {}").format(
                            sql.Identifier(target.database)
                        )
                    )
                except psycopg.Error as drop_exc:
                    cleanup_error: str | None = None
                    try:
                        if target.existing and target.existing.allow_connections:
                            maintenance.execute(
                                sql.SQL(
                                    "ALTER DATABASE {} ALLOW_CONNECTIONS true"
                                ).format(sql.Identifier(target.database))
                            )
                            with db_connect(
                                target.target_dsn,
                                autocommit=True,
                                connect_timeout=5,
                            ) as cleanup:
                                cleanup_server = _server_identity(cleanup)
                                if not _same_postgres_cluster(
                                    cleanup_server,
                                    target.server,
                                ):
                                    raise _error(
                                        "cleanup connection changed PostgreSQL servers",
                                        stage="target-drop-cleanup",
                                        target_state="original-fenced",
                                        target_fenced=True,
                                    )
                                if pre_restore_used:
                                    cleanup.execute(
                                        "SELECT timescaledb_post_restore()"
                                    )
                                    pre_restore_used = False
                            target_state = "original-open"
                        else:
                            target_state = "original-fenced"
                    except (DatabaseCloneError, psycopg.Error) as cleanup_exc:
                        cleanup_error = str(cleanup_exc)
                        try:
                            maintenance.execute(
                                sql.SQL(
                                    "ALTER DATABASE {} ALLOW_CONNECTIONS false"
                                ).format(sql.Identifier(target.database))
                            )
                            target_state = "original-fenced"
                        except psycopg.Error as fence_exc:
                            cleanup_error = (
                                f"{cleanup_error}; refence failed: {fence_exc}"
                            )
                            target_state = "original-cleanup-uncertain"
                    raise _error(
                        "target database drop failed",
                        stage="target-drop",
                        database=target.database,
                        target_state=target_state,
                        target_fenced=(
                            True
                            if target_state == "original-fenced"
                            else False
                            if target_state == "original-open"
                            else None
                        ),
                        cleanup_error=cleanup_error,
                    ) from drop_exc
                target_state = "target-absent"
                pre_restore_used = False
            maintenance.execute(
                sql.SQL(
                    "CREATE DATABASE {} WITH OWNER {} ENCODING {} "
                    "LC_COLLATE {} LC_CTYPE {} TEMPLATE template0 "
                    "ALLOW_CONNECTIONS false"
                ).format(
                    sql.Identifier(target.database),
                    sql.Identifier(target.restore_actor),
                    sql.Literal(source_shape.encoding),
                    sql.Literal(source_shape.collate),
                    sql.Literal(source_shape.ctype),
                )
            )
            target_state = "replacement-hard-fenced"
            _grant_restore_access(
                maintenance,
                target=target,
            )
    except DatabaseCloneError as exc:
        compensated_state, cleanup_error = _compensate_original_guard(
            target,
            guard=guard,
            pre_restore_used=pre_restore_used,
            target_state=target_state,
        )
        if compensated_state == target_state and cleanup_error is None:
            raise
        raise _error(
            exc.message,
            stage=str(exc.data.get("stage") or "target-recreate"),
            cause=exc.data,
            database=target.database,
            target_state=compensated_state,
            target_fenced=(
                True
                if compensated_state == "original-fenced"
                else False
                if compensated_state == "original-open"
                else None
            ),
            cleanup_error=cleanup_error,
        ) from exc
    except psycopg.Error as exc:
        target_state, cleanup_error = _compensate_original_guard(
            target,
            guard=guard,
            pre_restore_used=pre_restore_used,
            target_state=target_state,
        )
        raise _error(
            "target database replacement failed",
            stage="target-recreate",
            database=target.database,
            target_state=target_state,
            target_fenced=target_state in {
                "original-fenced",
                "replacement-hard-fenced",
                "replacement-access-fenced",
            },
            cleanup_error=cleanup_error,
        ) from exc
    finally:
        if guard is not None:
            guard.close()
    return existed


def _grant_restore_access(
    conn: psycopg.Connection[Any],
    *,
    target: TargetContext,
) -> None:
    conn.execute(
        sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM PUBLIC").format(
            sql.Identifier(target.database)
        )
    )
    conn.execute(
        sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            sql.Identifier(target.database),
            sql.Identifier(target.restore_actor),
        )
    )
    conn.execute(
        sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS true").format(
            sql.Identifier(target.database)
        )
    )


def _release_database(target: TargetContext) -> None:
    try:
        with db_connect(
            target.maintenance_dsn,
            autocommit=True,
            connect_timeout=5,
        ) as conn:
            release_server = _server_identity(conn)
            if not _same_postgres_cluster(release_server, target.server):
                raise _error(
                    "release connection changed PostgreSQL servers",
                    stage="target-release",
                    target_state="replacement-access-fenced",
                    target_fenced=True,
                )
            conn.execute(
                sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(
                    sql.Identifier(target.database)
                )
            )
            conn.execute(
                sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM PUBLIC").format(
                    sql.Identifier(target.database)
                )
            )
            conn.execute(
                sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM {}").format(
                    sql.Identifier(target.database),
                    sql.Identifier(target.restore_actor),
                )
            )
            conn.execute(
                sql.SQL("ALTER DATABASE {} OWNER TO {}").format(
                    sql.Identifier(target.database),
                    sql.Identifier(target.intended_owner),
                )
            )
            for privilege in target.intended_privileges:
                grantee = (
                    sql.SQL("PUBLIC")
                    if privilege.grantee == "PUBLIC"
                    else sql.Identifier(privilege.grantee)
                )
                grant_option = (
                    sql.SQL(" WITH GRANT OPTION")
                    if privilege.grantable
                    else sql.SQL("")
                )
                conn.execute(
                    sql.SQL("GRANT {} ON DATABASE {} TO {}{}").format(
                        sql.SQL(privilege.privilege),
                        sql.Identifier(target.database),
                        grantee,
                        grant_option,
                    )
                )
            if target.intended_allow_connections:
                conn.execute(
                    sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS true").format(
                        sql.Identifier(target.database)
                    )
                )
    except DatabaseCloneError:
        raise
    except psycopg.Error as exc:
        raise _error(
            "restored database could not be released",
            stage="target-release",
            database=target.database,
            target_state="replacement-hard-fenced",
            target_fenced=True,
        ) from exc


def _run_restore(
    target: TargetContext,
    *,
    dump_path: Path,
    toc_path: Path,
) -> None:
    route = _resolved_tool_route(
        target.target_dsn,
        database=target.database,
        probe_database=target.database,
        expected_server=target.server,
    )
    _run(
        [
            _tool("pg_restore"),
            "--exit-on-error",
            "--no-owner",
            "--no-privileges",
            "--use-list",
            str(toc_path),
            "--dbname",
            route.conninfo,
            str(dump_path),
        ],
        operation="pg_restore",
        env=route.env,
        redact_values=frozenset({route.conninfo}),
    )


def _verify_restored_database(
    target: TargetContext,
    *,
    timescaledb_version: str | None,
) -> None:
    try:
        with db_connect(
            target.target_dsn,
            autocommit=True,
            connect_timeout=5,
        ) as conn:
            verify_server = _server_identity(conn)
            if not _same_postgres_cluster(verify_server, target.server):
                raise _error(
                    "verification connection changed PostgreSQL servers",
                    stage="restore-verification",
                    expected_server=target.server.__dict__,
                    observed_server=verify_server.__dict__,
                    target_state="replacement-access-fenced",
                    target_fenced=True,
                )
            rows = dict(
                conn.execute(
                    "SELECT extname, extversion FROM pg_extension "
                    "WHERE extname IN ('timescaledb', 'pg_cron')"
                ).fetchall()
            )
            cron_schema = conn.execute(
                "SELECT to_regnamespace('cron')::text"
            ).fetchone()[0]
    except DatabaseCloneError:
        raise
    except psycopg.Error as exc:
        raise _error(
            "cannot verify restored database",
            stage="restore-verification",
            database=target.database,
            target_state="replacement-access-fenced",
            target_fenced=True,
        ) from exc
    if "pg_cron" in rows or cron_schema is not None:
        raise _error(
            "restored database contains server-bound pg_cron state",
            stage="restore-verification",
            database=target.database,
            pg_cron_extension="pg_cron" in rows,
            cron_schema=cron_schema,
            target_state="replacement-access-fenced",
            target_fenced=True,
        )
    observed = rows.get("timescaledb")
    if observed != timescaledb_version:
        raise _error(
            "restored TimescaleDB version does not match source",
            stage="restore-verification",
            expected=timescaledb_version,
            observed=observed,
            target_state="replacement-access-fenced",
            target_fenced=True,
        )


def safe_recreate_database(
    target_dsn: str,
    *,
    expected_database: str,
    admin_dsn: str | None = None,
    drain_timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    """Replace a disposable database without terminating connected backends."""
    target = _target_context(target_dsn, admin_dsn=admin_dsn)
    if target.database != expected_database:
        raise _error(
            "target database does not match the expected disposable database",
            stage="target-preflight",
            expected=expected_database,
            observed=target.database,
        )
    shape = target.existing or target.template
    existed = _replace_database_fenced(
        target,
        source_shape=shape,
        drain_timeout_seconds=drain_timeout_seconds,
    )
    _release_database(target)
    return {
        "ok": True,
        "operation": "safe-recreate",
        "database": target.database,
        "replaced_existing": existed,
        "template": "template0",
        "backend_termination": False,
    }


def create_clone_dump(
    source_dsn: str,
    dump_path: str | Path,
    *,
    source: DatabaseSpec | None = None,
) -> Path:
    """Write a complete custom-format dump without mutating the source."""
    path = Path(dump_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    source_spec = source or _source_spec(source_dsn)
    route = _resolved_tool_route(
        source_dsn,
        database=source_spec.database,
        probe_database=source_spec.database,
        expected_server=source_spec.server,
    )
    _run(
        [
            _tool("pg_dump"),
            "--format=custom",
            "--compress=6",
            "--no-owner",
            "--no-privileges",
            "--file",
            str(path),
            "--dbname",
            route.conninfo,
        ],
        operation="pg_dump",
        env=route.env,
        redact_values=frozenset({route.conninfo}),
    )
    path.chmod(0o600)
    return path


def restore_database_dump(
    source_dsn: str,
    target_dsn: str,
    dump_path: str | Path,
    *,
    admin_dsn: str | None = None,
    drain_timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    """Restore a validated dump through the safe development clone boundary."""
    source = _source_spec(source_dsn)
    target = _target_context(target_dsn, admin_dsn=admin_dsn)
    _require_distinct(source, target)
    _require_timescaledb_version(target, source.timescaledb_version)
    require_client_tools(source_dsn, target.maintenance_dsn, tools=("pg_restore",))
    path = Path(dump_path).expanduser().resolve()
    toc_path, digest = _prepare_restore_toc(
        path,
        timescaledb_prestaged=source.timescaledb_version is not None,
    )
    try:
        existed = _replace_database_fenced(
            target,
            source_shape=source,
            drain_timeout_seconds=drain_timeout_seconds,
        )
        try:
            if source.timescaledb_version is not None:
                with timescaledb_restore_context(
                    target.target_dsn,
                    version=source.timescaledb_version,
                ):
                    _run_restore(target, dump_path=path, toc_path=toc_path)
            else:
                _run_restore(target, dump_path=path, toc_path=toc_path)
            _verify_restored_database(
                target,
                timescaledb_version=source.timescaledb_version,
            )
            _release_database(target)
        except DatabaseCloneError as exc:
            if "target_state" in exc.data:
                raise
            raise _error(
                exc.message,
                stage=str(exc.data.get("stage") or "restore"),
                cause=exc.data,
                target_state="replacement-access-fenced",
                target_fenced=True,
            ) from exc
        except (psycopg.Error, SubstrateError) as exc:
            raise _error(
                "database restore or cleanup failed",
                stage="restore",
                cause_type=type(exc).__name__,
                target_state="replacement-access-fenced",
                target_fenced=True,
            ) from exc
    finally:
        toc_path.unlink(missing_ok=True)
    return {
        "ok": True,
        "operation": "database-clone",
        "source_database": source.database,
        "target_database": target.database,
        "source_server": source.server.__dict__,
        "target_server": target.server.__dict__,
        "replaced_existing": existed,
        "dump_digest": digest,
        "timescaledb_version": source.timescaledb_version,
        "excluded_extensions": ["pg_cron"],
        "template": "template0",
        "backend_termination": False,
    }


def clone_database(
    source_dsn: str,
    target_dsn: str,
    *,
    admin_dsn: str | None = None,
    dump_file: str | Path | None = None,
    keep_dump: bool = False,
    drain_timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    """Dump and restore a database through the sanctioned clone sequence."""
    temporary = dump_file is None
    if dump_file is None:
        fd, name = tempfile.mkstemp(prefix="okg-clone-", suffix=".dump")
        os.close(fd)
        path = Path(name)
    else:
        path = Path(dump_file).expanduser().resolve()
    try:
        source = _source_spec(source_dsn)
        require_client_tools(source_dsn)
        create_clone_dump(source_dsn, path, source=source)
        return restore_database_dump(
            source_dsn,
            target_dsn,
            path,
            admin_dsn=admin_dsn,
            drain_timeout_seconds=drain_timeout_seconds,
        )
    finally:
        if temporary and not keep_dump:
            path.unlink(missing_ok=True)
