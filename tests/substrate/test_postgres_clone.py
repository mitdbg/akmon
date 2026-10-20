from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import psycopg
import pytest

import okg.substrate.postgres_clone as clone
from okg.substrate.db import dsn
from okg.substrate.errors import DatabaseCloneError
from okg.substrate.cli.main import _build_parser, _cmd_db


_REAL_REQUIRE_CLIENT_TOOLS = getattr(clone, "require_client_tools", None)


@pytest.fixture(autouse=True)
def _client_tools_present(monkeypatch):
    """Unit tests here do not depend on the host's pg_dump/pg_restore."""
    monkeypatch.setattr(clone, "require_client_tools", lambda *_a, **_k: None)


def _fake_tool(directory: Path, name: str, version: str) -> None:
    script = directory / name
    script.write_text(f"#!/bin/sh\necho '{name} (PostgreSQL) {version}'\n")
    script.chmod(0o755)


def test_a_missing_clone_tool_is_named_with_how_to_install_it(monkeypatch):
    """Issue #3587: the error said only 'required ... tool is missing'."""
    monkeypatch.setattr(clone.shutil, "which", lambda name: None)
    with pytest.raises(DatabaseCloneError) as caught:
        clone._tool("pg_dump")
    assert str(caught.value) == (
        "pg_dump is not on PATH; install the PostgreSQL client tools "
        "(pg_dump and pg_restore)"
    )
    assert caught.value.data["tool"] == "pg_dump"
    assert "brew install libpq" in caught.value.fix_hint
    assert "postgresql-client-17" in caught.value.fix_hint


def test_client_tool_problems_names_missing_and_too_old_tools(
    monkeypatch, tmp_path: Path,
):
    _fake_tool(tmp_path, "pg_dump", "15.4")
    monkeypatch.setenv("PATH", str(tmp_path))

    problems = clone.client_tool_problems(17)

    assert [(p["tool"], p["problem"]) for p in problems] == [
        ("pg_dump", "too_old"), ("pg_restore", "missing"),
    ]
    assert problems[0]["message"] == (
        f"pg_dump at {tmp_path / 'pg_dump'} is PostgreSQL 15; the database "
        "server is PostgreSQL 17, and pg_dump must be that version or newer"
    )
    assert problems[1]["message"].startswith("pg_restore is not on PATH")


def test_client_tools_at_or_above_the_server_version_pass(
    monkeypatch, tmp_path: Path,
):
    _fake_tool(tmp_path, "pg_dump", "17.2")
    _fake_tool(tmp_path, "pg_restore", "18.0")
    monkeypatch.setenv("PATH", str(tmp_path))
    assert clone.client_tool_problems(17) == []
    assert clone.client_tool_problems(None) == []


def test_pg_restore_older_than_pg_dump_is_a_problem(monkeypatch, tmp_path: Path):
    """pg_restore 17 cannot read the archive pg_dump 18 writes."""
    _fake_tool(tmp_path, "pg_dump", "18.1")
    _fake_tool(tmp_path, "pg_restore", "17.2")
    monkeypatch.setenv("PATH", str(tmp_path))

    problems = clone.client_tool_problems(17)

    assert [(p["tool"], p["problem"]) for p in problems] == [
        ("pg_restore", "older_than_pg_dump"),
    ]
    assert "cannot read an archive from a newer pg_dump" in problems[0]["message"]


def test_an_unreadable_tool_version_is_reported_and_never_blocks(
    monkeypatch, tmp_path: Path,
):
    script = tmp_path / "pg_dump"
    script.write_text("#!/bin/sh\necho 'pg_dump (EnterpriseDB) 17.2'\n")
    script.chmod(0o755)
    _fake_tool(tmp_path, "pg_restore", "17.2")
    monkeypatch.setenv("PATH", str(tmp_path))

    report = clone.client_tool_report(17)

    assert report["problems"] == []
    assert report["unknown_version"] == ["pg_dump"]


def test_clone_checks_the_tools_before_it_dumps(monkeypatch, tmp_path: Path):
    events: list[str] = []
    monkeypatch.setattr(clone, "_source_spec", lambda _dsn: _spec())
    monkeypatch.setattr(
        clone, "require_client_tools",
        lambda *_a, **_k: events.append("tools")
        or (_ for _ in ()).throw(DatabaseCloneError("pg_dump is not on PATH")),
    )
    monkeypatch.setattr(
        clone, "create_clone_dump", lambda *_a, **_k: events.append("dump"),
    )
    with pytest.raises(DatabaseCloneError):
        clone.clone_database("source", "target", dump_file=tmp_path / "x.dump")
    assert events == ["tools"]


def test_restore_dump_names_a_missing_pg_restore_before_touching_the_target(
    monkeypatch, tmp_path: Path,
):
    """`okg db clone --restore-dump` on a host without pg_restore (#3587)."""
    events: list[str] = []
    monkeypatch.setattr(clone, "require_client_tools", _REAL_REQUIRE_CLIENT_TOOLS)
    monkeypatch.setattr(clone, "server_major", lambda _dsn: 17)
    monkeypatch.setattr(clone.shutil, "which", lambda name: None)
    monkeypatch.setattr(clone, "_source_spec", lambda _dsn: _spec())
    monkeypatch.setattr(clone, "_target_context", lambda *_a, **_k: _target())
    monkeypatch.setattr(clone, "_require_distinct", lambda *_a: None)
    monkeypatch.setattr(clone, "_require_timescaledb_version", lambda *_a: None)
    monkeypatch.setattr(
        clone, "_prepare_restore_toc",
        lambda *_a, **_k: events.append("dump") or (tmp_path / "t", "sha"),
    )
    monkeypatch.setattr(
        clone, "_replace_database_fenced",
        lambda *_a, **_k: events.append("replace") or True,
    )

    with pytest.raises(DatabaseCloneError) as caught:
        clone.restore_database_dump("source", "target", tmp_path / "x.dump")

    assert str(caught.value) == (
        "pg_restore is not on PATH; install the PostgreSQL client tools "
        "(pg_dump and pg_restore)"
    )
    assert caught.value.data["tool"] == "pg_restore"
    assert caught.value.fix_hint == clone.CLIENT_TOOLS_FIX_HINT
    assert events == []


def _server(
    *,
    system_identifier: str = "cluster-a",
    address: str = "127.0.0.1",
    port: int = 5432,
):
    return clone.ServerIdentity(system_identifier, address, port)


def _privileges():
    return (
        clone.DatabasePrivilege("PUBLIC", "CONNECT", False),
        clone.DatabasePrivilege("app_reader", "CONNECT", True),
        clone.DatabasePrivilege("app_writer", "TEMPORARY", False),
    )


def _spec(*, database: str = "source", version: str | None = "2.29.0"):
    return clone.DatabaseSpec(
        database=database,
        owner="postgres",
        encoding="UTF8",
        collate="C",
        ctype="C",
        allow_connections=True,
        timescaledb_version=version,
        privileges=_privileges(),
        server=_server(),
    )


def _target(*, existing: bool = True):
    return clone.TargetContext(
        target_dsn="postgresql://postgres@127.0.0.1:5432/target",
        target_maintenance_dsn="postgresql://postgres@127.0.0.1:5432/postgres",
        maintenance_dsn="postgresql://postgres@127.0.0.1:5432/postgres",
        database="target",
        restore_actor="postgres",
        intended_owner="postgres",
        intended_allow_connections=True,
        intended_privileges=_privileges(),
        existing=_spec(database="target") if existing else None,
        template=_spec(database="template0", version=None),
        server=_server(),
    )


class _Result:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None):
        self.rows = rows or []

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(
        self,
        name: str,
        log: list[str],
        *,
        fail_post_restore: bool = False,
        server: clone.ServerIdentity | None = None,
        parameters: dict[str, str] | None = None,
        available_version: bool = True,
        cron_database: str = "okg",
        fail_on: str | None = None,
    ) -> None:
        self.name = name
        self.log = log
        self.fail_post_restore = fail_post_restore
        self.server = server or _server()
        self.available_version = available_version
        self.cron_database = cron_database
        self.fail_on = fail_on
        self.info = SimpleNamespace(
            backend_pid=911,
            dbname="target",
            host="db.internal",
            hostaddr="10.0.0.4",
            port=5432,
            user="postgres",
            password="test-password",
            get_parameters=lambda: dict(parameters or {}),
        )

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()

    def close(self):
        self.log.append(f"{self.name}:close")

    def execute(self, query, _params=None):
        rendered = str(query)
        self.log.append(f"{self.name}:{rendered}")
        if self.fail_on and self.fail_on in rendered:
            raise psycopg.OperationalError(f"failed on {self.fail_on}")
        if "extversion FROM pg_extension" in rendered:
            return _Result([("2.29.0",)])
        if "timescaledb_post_restore" in rendered and self.fail_post_restore:
            raise psycopg.OperationalError("post-restore failed")
        if "pg_available_extension_versions" in rendered:
            return _Result([(self.available_version,)])
        if "SELECT EXISTS" in rendered:
            return _Result([(True,)])
        if "SELECT current_user" in rendered:
            return _Result([("postgres",)])
        if "current_setting('cron.database_name'" in rendered:
            return _Result([(self.cron_database,)])
        return _Result()


def test_restore_toc_excludes_server_bound_extensions_but_keeps_timescale_data():
    listing = """;
1; 3079 1 EXTENSION - pg_cron
2; 0 0 COMMENT - EXTENSION pg_cron
3; 0 0 TABLE DATA cron job postgres
4; 0 0 SEQUENCE SET cron jobid_seq postgres
5; 3079 2 EXTENSION - timescaledb
6; 0 0 COMMENT - EXTENSION timescaledb
7; 0 0 TABLE DATA _timescaledb_catalog dimension postgres
8; 0 0 TABLE DATA public ordinary postgres
9; 0 0 ACL - SCHEMA cron postgres
10; 0 0 ACL cron TABLE job postgres
"""

    filtered = clone._filtered_restore_toc(
        listing,
        timescaledb_prestaged=True,
    )

    assert "pg_cron" not in filtered
    assert " EXTENSION - timescaledb" not in filtered
    assert "_timescaledb_catalog dimension" in filtered
    assert "public ordinary" in filtered


@pytest.mark.parametrize("database", ["postgres", "template0", "template1"])
def test_maintenance_databases_are_refused_before_connect(database: str):
    with pytest.raises(DatabaseCloneError, match="maintenance database"):
        clone._database_name(
            f"postgresql://postgres@localhost/{database}",
            stage="test",
        )


def test_same_server_and_database_are_refused():
    target = _target()
    source = _spec(database=target.database)

    with pytest.raises(DatabaseCloneError, match="same database"):
        clone._require_distinct(source, target)


def test_explicit_admin_dsn_must_reach_target_server(monkeypatch):
    target_connection = _Connection("target", [], server=_server())
    admin_connection = _Connection(
        "admin",
        [],
        server=_server(system_identifier="cluster-b", address="10.0.0.9"),
    )
    connections = iter([target_connection, admin_connection])
    monkeypatch.setattr(clone.psycopg, "connect", lambda *_a, **_k: next(connections))
    monkeypatch.setattr(clone, "_server_identity", lambda conn: conn.server)

    with pytest.raises(DatabaseCloneError, match="different PostgreSQL servers"):
        clone._target_context(
            "postgresql://postgres@target/target",
            admin_dsn="postgresql://postgres@admin/postgres",
        )


def test_tool_route_expands_service_and_pins_effective_endpoint(monkeypatch):
    parameters = {
        "service": "safe-clone",
        "host": "primary,secondary",
        "port": "5432,5432",
        "dbname": "source",
        "load_balance_hosts": "random",
        "sslmode": "verify-full",
        "sslcert": "/etc/postgresql/client.crt",
        "sslkey": "/etc/postgresql/client.key",
        "sslrootcert": "/etc/postgresql/root.crt",
        "sslcrl": "/etc/postgresql/root.crl",
        "target_session_attrs": "read-write",
        "passfile": "/tmp/safe-clone.pass",
        "options": "-c statement_timeout=0",
    }
    connection = _Connection("route", [], parameters=parameters)
    monkeypatch.setattr(clone.psycopg, "connect", lambda *_a, **_k: connection)
    monkeypatch.setattr(clone, "_server_identity", lambda conn: conn.server)
    monkeypatch.setenv("PGHOST", "wrong-host")
    monkeypatch.setenv("PGSERVICE", "wrong-service")
    monkeypatch.setenv("PGDATABASE", "wrong-database")

    route = clone._resolved_tool_route(
        "service=safe-clone dbname=source",
        database="source",
        probe_database="source",
        expected_server=_server(),
    )
    parsed = dsn.parse(route.conninfo).params

    assert "service" not in parsed
    assert parsed["host"] == "db.internal"
    assert parsed["hostaddr"] == "10.0.0.4"
    assert parsed["load_balance_hosts"] == "random"
    assert parsed["sslmode"] == "verify-full"
    assert parsed["sslcert"] == "/etc/postgresql/client.crt"
    assert parsed["sslkey"] == "/etc/postgresql/client.key"
    assert parsed["sslrootcert"] == "/etc/postgresql/root.crt"
    assert parsed["sslcrl"] == "/etc/postgresql/root.crl"
    assert parsed["target_session_attrs"] == "read-write"
    assert parsed["passfile"] == "/tmp/safe-clone.pass"
    assert parsed["options"] == "-c statement_timeout=0"
    assert route.env["PGPASSWORD"] == "test-password"
    assert "PGHOST" not in route.env
    assert "PGSERVICE" not in route.env
    assert "PGDATABASE" not in route.env


@pytest.mark.parametrize("secret", sorted(clone._TOOL_SECRET_PARAMETERS))
def test_tool_route_refuses_each_unpreservable_secret(secret: str):
    connection = _Connection(
        "route",
        [],
        parameters={secret: "sensitive-value"},
    )

    with pytest.raises(DatabaseCloneError, match="cannot be preserved safely") as caught:
        clone._pinned_connection_parameters(connection, database="source")

    assert caught.value.data["unsupported_parameters"] == [secret]


def test_tool_route_refuses_changed_server(monkeypatch):
    connection = _Connection(
        "route",
        [],
        server=_server(system_identifier="cluster-b"),
    )
    monkeypatch.setattr(clone.psycopg, "connect", lambda *_a, **_k: connection)
    monkeypatch.setattr(clone, "_server_identity", lambda conn: conn.server)

    with pytest.raises(DatabaseCloneError, match="unexpected server"):
        clone._resolved_tool_route(
            "postgresql://postgres@db/source",
            database="source",
            probe_database="source",
            expected_server=_server(),
        )


def test_unavailable_source_timescaledb_version_is_refused(monkeypatch):
    connection = _Connection("maintenance", [], available_version=False)
    monkeypatch.setattr(clone.psycopg, "connect", lambda *_a, **_k: connection)

    with pytest.raises(DatabaseCloneError, match="version is unavailable"):
        clone._require_timescaledb_version(_target(), "2.29.0")


def test_pg_cron_worker_database_is_refused_as_target(monkeypatch):
    target_connection = _Connection("target", [], cron_database="target")
    admin_connection = _Connection("admin", [], cron_database="target")
    connections = iter([target_connection, admin_connection])
    monkeypatch.setattr(clone.psycopg, "connect", lambda *_a, **_k: next(connections))
    monkeypatch.setattr(clone, "_server_identity", lambda conn: conn.server)
    monkeypatch.setattr(
        clone,
        "_load_spec",
        lambda _conn, database: _spec(database=database),
    )

    with pytest.raises(DatabaseCloneError, match="pg_cron background worker"):
        clone._target_context(
            "postgresql://postgres@target/target",
            admin_dsn=None,
        )


@pytest.mark.parametrize(
    ("listing", "source_has_timescaledb"),
    [
        ("1; 3079 1 EXTENSION - timescaledb postgres\n", False),
        ("1; 1259 1 TABLE public ordinary postgres\n", True),
    ],
)
def test_archive_timescaledb_presence_must_match_source(
    monkeypatch,
    tmp_path: Path,
    listing: str,
    source_has_timescaledb: bool,
):
    dump = tmp_path / "clone.dump"
    dump.write_bytes(b"dump")
    monkeypatch.setattr(
        clone,
        "_run",
        lambda *_a, **_k: SimpleNamespace(stdout=listing),
    )
    monkeypatch.setattr(clone, "_tool", lambda name: name)

    with pytest.raises(DatabaseCloneError, match="does not match the live source"):
        clone._prepare_restore_toc(
            dump,
            timescaledb_prestaged=source_has_timescaledb,
        )


def test_cli_requires_explicit_apply_before_clone(capsys):
    args = _build_parser().parse_args(
        [
            "db",
            "clone",
            "--source-dsn",
            "postgresql://postgres@localhost/source",
            "--target-dsn",
            "postgresql://postgres@localhost/target",
            "--json",
        ]
    )

    assert _cmd_db(args, as_json=False) == 1
    assert "requires --apply" in capsys.readouterr().err


def test_cli_reads_admin_dsn_from_environment(monkeypatch, capsys):
    observed: dict[str, Any] = {}

    def restore(*_args, **kwargs):
        observed.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(clone, "restore_database_dump", restore)
    monkeypatch.setenv("OKG_CLONE_ADMIN_DSN", "postgresql://admin@db/postgres")
    args = _build_parser().parse_args(
        [
            "db",
            "clone",
            "--source-dsn",
            "postgresql://source@db/source",
            "--target-dsn",
            "postgresql://target@db/target",
            "--restore-dump",
            "/tmp/clone.dump",
            "--apply",
            "--json",
        ]
    )

    assert _cmd_db(args, as_json=False) == 0
    assert observed["admin_dsn"] == "postgresql://admin@db/postgres"
    assert '"ok": true' in capsys.readouterr().out.lower()


def test_recreate_quiesces_then_denies_drains_drops_and_creates_from_template0(
    monkeypatch,
):
    log: list[str] = []
    guard = _Connection("guard", log)
    maintenance = _Connection("maintenance", log)
    connections = iter([guard, maintenance])
    monkeypatch.setattr(clone.psycopg, "connect", lambda *_a, **_k: next(connections))
    monkeypatch.setattr(clone, "_server_identity", lambda _conn: _server())
    monkeypatch.setattr(clone, "_remaining_sessions", lambda *_a, **_k: [])

    assert clone._replace_database_fenced(
        _target(),
        source_shape=_spec(),
        drain_timeout_seconds=1,
    ) is True

    joined = "\n".join(log)
    assert joined.index("timescaledb_pre_restore") < joined.index(
        "ALLOW_CONNECTIONS false"
    )
    assert joined.index("ALLOW_CONNECTIONS false") < joined.index("DROP DATABASE")
    assert joined.index("DROP DATABASE") < joined.index("CREATE DATABASE")
    assert joined.index("TEMPLATE template0") < joined.index("REVOKE ALL PRIVILEGES")
    assert joined.index("REVOKE ALL PRIVILEGES") < joined.index("GRANT CONNECT")
    assert "WITH (FORCE)" not in joined
    assert "pg_terminate_backend" not in joined


def test_post_quiesce_identity_refusal_runs_post_restore(monkeypatch):
    log: list[str] = []
    guard = _Connection("guard", log, server=_server())
    maintenance = _Connection(
        "maintenance",
        log,
        server=_server(system_identifier="cluster-b"),
    )
    connections = iter([guard, maintenance])
    monkeypatch.setattr(clone.psycopg, "connect", lambda *_a, **_k: next(connections))
    monkeypatch.setattr(clone, "_server_identity", lambda conn: conn.server)

    with pytest.raises(DatabaseCloneError, match="changed PostgreSQL servers") as caught:
        clone._replace_database_fenced(
            _target(),
            source_shape=_spec(),
            drain_timeout_seconds=1,
        )

    joined = "\n".join(log)
    assert joined.index("timescaledb_pre_restore") < joined.index(
        "timescaledb_post_restore"
    )
    assert "DROP DATABASE" not in joined
    assert caught.value.data["target_state"] == "original-open"
    assert caught.value.data["target_fenced"] is False


def test_post_quiesce_cleanup_failure_refences_original(monkeypatch):
    log: list[str] = []
    guard = _Connection("guard", log, fail_post_restore=True)
    changed_maintenance = _Connection(
        "changed-maintenance",
        log,
        server=_server(system_identifier="cluster-b"),
    )
    cleanup_maintenance = _Connection("cleanup-maintenance", log)
    connections = iter([guard, changed_maintenance, cleanup_maintenance])
    monkeypatch.setattr(clone.psycopg, "connect", lambda *_a, **_k: next(connections))
    monkeypatch.setattr(clone, "_server_identity", lambda conn: conn.server)

    with pytest.raises(DatabaseCloneError, match="changed PostgreSQL servers") as caught:
        clone._replace_database_fenced(
            _target(),
            source_shape=_spec(),
            drain_timeout_seconds=1,
        )

    joined = "\n".join(log)
    assert "timescaledb_post_restore" in joined
    assert "cleanup-maintenance:Composed" in joined
    assert caught.value.data["target_state"] == "original-fenced"
    assert caught.value.data["target_fenced"] is True
    assert "post-restore failed" in caught.value.data["cleanup_error"]


def test_post_quiesce_refence_failure_reports_uncertain_state(monkeypatch):
    log: list[str] = []
    guard = _Connection("guard", log, fail_post_restore=True)
    changed_maintenance = _Connection(
        "changed-maintenance",
        log,
        server=_server(system_identifier="cluster-b"),
    )
    cleanup_maintenance = _Connection(
        "cleanup-maintenance",
        log,
        fail_on="ALLOW_CONNECTIONS false",
    )
    connections = iter([guard, changed_maintenance, cleanup_maintenance])
    monkeypatch.setattr(clone.psycopg, "connect", lambda *_a, **_k: next(connections))
    monkeypatch.setattr(clone, "_server_identity", lambda conn: conn.server)

    with pytest.raises(DatabaseCloneError, match="changed PostgreSQL servers") as caught:
        clone._replace_database_fenced(
            _target(),
            source_shape=_spec(),
            drain_timeout_seconds=1,
        )

    assert caught.value.data["target_state"] == "original-cleanup-uncertain"
    assert caught.value.data["target_fenced"] is None
    assert "refence failed" in caught.value.data["cleanup_error"]


def test_drain_timeout_cleans_up_while_fenced_then_reenables(monkeypatch):
    log: list[str] = []
    guard = _Connection("guard", log)
    maintenance = _Connection("maintenance", log)
    connections = iter([guard, maintenance])
    monkeypatch.setattr(clone.psycopg, "connect", lambda *_a, **_k: next(connections))
    monkeypatch.setattr(clone, "_server_identity", lambda _conn: _server())
    monkeypatch.setattr(
        clone,
        "_remaining_sessions",
        lambda *_a, **_k: [{"pid": 42, "backend_type": "client backend"}],
    )
    moments = iter([0.0, 2.0])
    monkeypatch.setattr(clone.time, "monotonic", lambda: next(moments))

    with pytest.raises(DatabaseCloneError, match="did not drain") as caught:
        clone._replace_database_fenced(
            _target(),
            source_shape=_spec(),
            drain_timeout_seconds=1,
        )

    joined = "\n".join(log)
    assert joined.index("ALLOW_CONNECTIONS false") < joined.index(
        "timescaledb_post_restore"
    )
    assert joined.index("timescaledb_post_restore") < joined.index(
        "ALLOW_CONNECTIONS true"
    )
    assert "DROP DATABASE" not in joined
    assert caught.value.data["target_fenced"] is False
    assert caught.value.data["original_target_exists"] is True
    assert caught.value.data["target_state"] == "original-open"


def test_drain_cleanup_failure_keeps_original_target_fenced(monkeypatch):
    log: list[str] = []
    guard = _Connection("guard", log, fail_post_restore=True)
    maintenance = _Connection("maintenance", log)
    connections = iter([guard, maintenance])
    monkeypatch.setattr(clone.psycopg, "connect", lambda *_a, **_k: next(connections))
    monkeypatch.setattr(clone, "_server_identity", lambda _conn: _server())
    monkeypatch.setattr(
        clone,
        "_remaining_sessions",
        lambda *_a, **_k: [{"pid": 42, "backend_type": "client backend"}],
    )
    moments = iter([0.0, 2.0])
    monkeypatch.setattr(clone.time, "monotonic", lambda: next(moments))

    with pytest.raises(DatabaseCloneError) as caught:
        clone._replace_database_fenced(
            _target(),
            source_shape=_spec(),
            drain_timeout_seconds=1,
        )

    assert caught.value.data["target_fenced"] is True
    assert caught.value.data["target_state"] == "original-fenced"
    assert "post-restore failed" in caught.value.data["cleanup_error"]
    assert "ALLOW_CONNECTIONS true" not in "\n".join(log)


def test_drop_failure_restores_original_target_state(monkeypatch):
    log: list[str] = []
    guard = _Connection("guard", log)
    maintenance = _Connection("maintenance", log, fail_on="DROP DATABASE")
    cleanup = _Connection("cleanup", log)
    connections = iter([guard, maintenance, cleanup])
    monkeypatch.setattr(clone.psycopg, "connect", lambda *_a, **_k: next(connections))
    monkeypatch.setattr(clone, "_server_identity", lambda _conn: _server())
    monkeypatch.setattr(clone, "_remaining_sessions", lambda *_a, **_k: [])

    with pytest.raises(DatabaseCloneError, match="drop failed") as caught:
        clone._replace_database_fenced(
            _target(),
            source_shape=_spec(),
            drain_timeout_seconds=1,
        )

    assert caught.value.data["target_state"] == "original-open"
    assert caught.value.data["target_fenced"] is False
    joined = "\n".join(log)
    assert joined.index("DROP DATABASE") < joined.index("timescaledb_post_restore")
    assert "CREATE DATABASE" not in joined


def test_release_replays_database_acl_and_removes_temporary_actor_grant(monkeypatch):
    log: list[str] = []
    maintenance = _Connection("maintenance", log)
    monkeypatch.setattr(clone.psycopg, "connect", lambda *_a, **_k: maintenance)
    monkeypatch.setattr(clone, "_server_identity", lambda _conn: _server())

    clone._release_database(_target())

    joined = "\n".join(log)
    assert "REVOKE ALL PRIVILEGES" in joined
    assert joined.index("ALLOW_CONNECTIONS false") < joined.index(
        "REVOKE ALL PRIVILEGES"
    )
    assert joined.index("REVOKE ALL PRIVILEGES") < joined.index("OWNER TO")
    assert "app_reader" in joined
    assert "WITH GRANT OPTION" in joined
    assert "app_writer" in joined
    assert joined.index("app_writer") < joined.index("ALLOW_CONNECTIONS true")


def test_restore_preflights_before_recreate_and_releases_only_after_verify(
    monkeypatch,
    tmp_path: Path,
):
    dump = tmp_path / "clone.dump"
    toc = tmp_path / "clone.toc"
    dump.write_bytes(b"dump")
    toc.write_text("toc")
    events: list[str] = []
    source = _spec()
    target = _target()

    monkeypatch.setattr(clone, "_source_spec", lambda _dsn: events.append("source") or source)
    monkeypatch.setattr(
        clone,
        "_target_context",
        lambda *_a, **_k: events.append("target") or target,
    )
    monkeypatch.setattr(clone, "_require_distinct", lambda *_a: events.append("distinct"))
    monkeypatch.setattr(
        clone,
        "_require_timescaledb_version",
        lambda *_a: events.append("version"),
    )
    monkeypatch.setattr(
        clone,
        "_prepare_restore_toc",
        lambda *_a, **_k: (events.append("dump") or toc, "sha256:test"),
    )
    monkeypatch.setattr(
        clone,
        "_replace_database_fenced",
        lambda *_a, **_k: events.append("replace") or True,
    )

    @contextmanager
    def restore_context(*_a, **_k):
        events.append("pre")
        try:
            yield
        finally:
            events.append("post")

    monkeypatch.setattr(clone, "timescaledb_restore_context", restore_context)
    monkeypatch.setattr(clone, "_run_restore", lambda *_a, **_k: events.append("restore"))
    monkeypatch.setattr(
        clone,
        "_verify_restored_database",
        lambda *_a, **_k: events.append("verify"),
    )
    monkeypatch.setattr(clone, "_release_database", lambda *_a: events.append("release"))

    report = clone.restore_database_dump("source", "target", dump)

    assert report["ok"] is True
    assert events == [
        "source",
        "target",
        "distinct",
        "version",
        "dump",
        "replace",
        "pre",
        "restore",
        "post",
        "verify",
        "release",
    ]


def test_restore_failure_runs_post_hook_and_never_releases(monkeypatch, tmp_path: Path):
    dump = tmp_path / "clone.dump"
    toc = tmp_path / "clone.toc"
    dump.write_bytes(b"dump")
    toc.write_text("toc")
    events: list[str] = []
    monkeypatch.setattr(clone, "_source_spec", lambda _dsn: _spec())
    monkeypatch.setattr(clone, "_target_context", lambda *_a, **_k: _target())
    monkeypatch.setattr(clone, "_require_distinct", lambda *_a: None)
    monkeypatch.setattr(clone, "_require_timescaledb_version", lambda *_a: None)
    monkeypatch.setattr(
        clone,
        "_prepare_restore_toc",
        lambda *_a, **_k: (toc, "sha256:test"),
    )
    monkeypatch.setattr(clone, "_replace_database_fenced", lambda *_a, **_k: True)

    @contextmanager
    def restore_context(*_a, **_k):
        events.append("pre")
        try:
            yield
        finally:
            events.append("post")

    monkeypatch.setattr(clone, "timescaledb_restore_context", restore_context)

    def fail_restore(*_a, **_k):
        events.append("restore")
        raise DatabaseCloneError("boom", data={"stage": "pg_restore"})

    monkeypatch.setattr(clone, "_run_restore", fail_restore)
    monkeypatch.setattr(clone, "_release_database", lambda *_a: events.append("release"))

    with pytest.raises(DatabaseCloneError, match="boom"):
        clone.restore_database_dump("source", "target", dump)

    assert events == ["pre", "restore", "post"]


def test_post_restore_failure_keeps_replacement_fenced(monkeypatch, tmp_path: Path):
    dump = tmp_path / "clone.dump"
    toc = tmp_path / "clone.toc"
    dump.write_bytes(b"dump")
    toc.write_text("toc")
    events: list[str] = []
    monkeypatch.setattr(clone, "_source_spec", lambda _dsn: _spec())
    monkeypatch.setattr(clone, "_target_context", lambda *_a, **_k: _target())
    monkeypatch.setattr(clone, "_require_distinct", lambda *_a: None)
    monkeypatch.setattr(clone, "_require_timescaledb_version", lambda *_a: None)
    monkeypatch.setattr(
        clone,
        "_prepare_restore_toc",
        lambda *_a, **_k: (toc, "sha256:test"),
    )
    monkeypatch.setattr(clone, "_replace_database_fenced", lambda *_a, **_k: True)

    @contextmanager
    def failing_post_context(*_a, **_k):
        events.append("pre")
        try:
            yield
        finally:
            events.append("post")
            raise psycopg.OperationalError("post failed")

    monkeypatch.setattr(clone, "timescaledb_restore_context", failing_post_context)
    monkeypatch.setattr(clone, "_run_restore", lambda *_a, **_k: events.append("restore"))
    monkeypatch.setattr(clone, "_release_database", lambda *_a: events.append("release"))

    with pytest.raises(DatabaseCloneError, match="cleanup failed") as caught:
        clone.restore_database_dump("source", "target", dump)

    assert caught.value.data["target_state"] == "replacement-access-fenced"
    assert caught.value.data["target_fenced"] is True
    assert events == ["pre", "restore", "post"]


def test_restore_without_timescaledb_uses_same_fenced_path_without_hooks(
    monkeypatch,
    tmp_path: Path,
):
    dump = tmp_path / "plain.dump"
    toc = tmp_path / "plain.toc"
    dump.write_bytes(b"dump")
    toc.write_text("toc")
    events: list[str] = []
    monkeypatch.setattr(clone, "_source_spec", lambda _dsn: _spec(version=None))
    monkeypatch.setattr(clone, "_target_context", lambda *_a, **_k: _target())
    monkeypatch.setattr(clone, "_require_distinct", lambda *_a: None)
    monkeypatch.setattr(clone, "_require_timescaledb_version", lambda *_a: None)
    monkeypatch.setattr(
        clone,
        "_prepare_restore_toc",
        lambda *_a, **_k: (toc, "sha256:test"),
    )
    monkeypatch.setattr(
        clone,
        "_replace_database_fenced",
        lambda *_a, **_k: events.append("replace") or True,
    )
    monkeypatch.setattr(
        clone,
        "timescaledb_restore_context",
        lambda *_a, **_k: pytest.fail("plain restore entered TimescaleDB mode"),
    )
    monkeypatch.setattr(clone, "_run_restore", lambda *_a, **_k: events.append("restore"))
    monkeypatch.setattr(
        clone,
        "_verify_restored_database",
        lambda *_a, **_k: events.append("verify"),
    )
    monkeypatch.setattr(clone, "_release_database", lambda *_a: events.append("release"))

    report = clone.restore_database_dump("source", "target", dump)

    assert report["timescaledb_version"] is None
    assert events == ["replace", "restore", "verify", "release"]
