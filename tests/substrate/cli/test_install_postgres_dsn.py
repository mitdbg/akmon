"""`okg install --postgres-dsn` uses the database it names. Issue #2210.

The flag existed only as a side effect: `_maybe_inject_profile_flags` in
`cli/main.py` turns each profile `init_question` into a `--<id>` flag, so
`--postgres-dsn` was accepted whenever `--profile` appeared in argv -- and
absent from `okg install --help`, which carries no `--profile`. Nothing then
read either slot: `_cmd_install` built its bootstrap namespace with a literal
`postgres_dsn=None` and `use_local_postgres=False`, so every install took the
Docker branch, minted its own DSN, and wrote that into `deployment.yaml`. An
operator with an existing PostgreSQL got a container they did not ask for and
no error.

The class this guards is "an accepted flag that reaches no decision". Each
test below pins one hop of the chain the flag has to survive: the parser
(`--help`), the refusal that runs before anything is written, the branch that
decides whether Docker runs, the child process that writes the deployment,
and the plan `--dry-run` prints.

No test here starts a container or needs a live PostgreSQL. The two that
reach a database do so at a closed loopback port and assert the refusal.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import okg.substrate.cli.main as cli
from okg.substrate.cli.profile_cli_loader import get_profile_cli

pi_cli = get_profile_cli("academic-researcher").install

#: A DSN whose password must never appear in output, at a port nothing
#: listens on so a reachability probe refuses immediately.
UNREACHABLE_DSN = "postgresql://postgres:hunter2@127.0.0.1:1/okg_probe"


def _install_args(**overrides):
    defaults = {
        "orcid": None,
        "name": "dsnprobe",
        "pi_name": "Test PI",
        "postgres_port": None,
        "postgres_dsn": None,
        "mcp_port": 15431,
        "no_claude_config": True,
        "claude_config_only": False,
        "dry_run": False,
        "git_repos": None,
        "dblp_pid": None,
        "embed_worker": False,
        "deployments_root": None,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _bootstrap_args(**overrides):
    """The namespace `_cmd_install` hands the provision + scaffold step."""
    defaults = {
        "deployment_name": "dsnprobe",
        "orcid": None,
        "postgres_dsn": None,
        "postgres_port": 15435,
        "use_local_postgres": False,
        "mcp_port": 15431,
        "papers_dir": None,
        "cv_path": None,
        "group_webpage": None,
        "pi_name": None,
        "dblp_pid": None,
        "git_repos": None,
        "reset": False,
        "yes": True,
        "force": False,
        "deployments_root_resolved": None,
        "_invoked_from_install": True,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


class _Done:
    returncode = 0
    stdout = b""
    stderr = ""


# ---------- the parser ----------


def test_help_lists_the_flag() -> None:
    """`okg install --help` carries no `--profile`, so the injected flag
    was invisible there while a real install still accepted it. The flag
    is now declared by the profile's own `add_install_arguments`, which
    every `install` parser calls."""
    parser = cli._build_parser(["install"])
    install = next(
        action.choices["install"]
        for action in parser._actions
        if isinstance(action, argparse._SubParsersAction)
    )
    action = next(
        a for a in install._actions if "--postgres-dsn" in a.option_strings
    )
    assert action.dest == "postgres_dsn"
    assert action.help, "a flag with no help text is a flag with no --help entry"
    assert "--postgres-dsn" in install.format_help()


def test_the_profile_question_maps_onto_the_native_flag() -> None:
    """One flag, one dest. The injector must see the name collision and
    reuse the declared flag instead of adding a second `--postgres-dsn`
    (which argparse would refuse) or a second dest (which would let the
    two disagree)."""
    parser = cli._build_parser(["install"])
    flag_map = cli._maybe_inject_profile_flags(
        parser, ["install", "--profile", "academic-researcher"],
    )
    assert flag_map["postgres_dsn"] == "postgres_dsn"
    args = parser.parse_args([
        "install", "--profile", "academic-researcher",
        "--postgres-dsn", UNREACHABLE_DSN,
    ])
    assert args.postgres_dsn == UNREACHABLE_DSN
    assert getattr(args, "answer_postgres_dsn", None) is None


# ---------- the refusal, before anything is written ----------


def test_an_unreachable_dsn_is_refused_before_the_scaffold(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """A supplied database is not provisioned here, so nothing else proves
    it answers before the scaffold, the migration and the publish assume it
    does. The refusal must also not print the operator's password."""
    ran: list[list[str]] = []
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        pi_cli, "_resolve_git_repos", lambda explicit: None,
    )
    monkeypatch.setattr(
        pi_cli,
        "_provision_postgres_and_scaffold",
        lambda a, **k: ran.append(["scaffold"]) or 0,
    )

    rc = pi_cli._cmd_install(
        _install_args(
            postgres_dsn=UNREACHABLE_DSN,
            deployments_root=str(tmp_path / "deployments"),
        ),
        as_json=True,
    )

    assert rc != 0
    assert ran == [], "the scaffold ran despite an unreachable database"
    captured = capsys.readouterr()
    envelope = captured.out + captured.err
    assert "install_postgres_dsn_unreachable" in envelope
    assert "127.0.0.1:1" in envelope, "the refusal must name the DSN"
    assert "hunter2" not in envelope, "the password reached the operator's terminal"


def test_the_probe_runs_one_select_and_refuses_a_wrong_answer(
    monkeypatch,
) -> None:
    """The probe is a connect plus `SELECT 1`, and a database that answers
    anything else is not a database this install can use."""
    from okg.substrate.errors import SubstrateError

    executed: list[str] = []

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql):
            executed.append(sql)

        def fetchone(self):
            return (0,)

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def cursor(self):
            return _Cursor()

    import psycopg

    monkeypatch.setattr(psycopg, "connect", lambda *a, **k: _Conn())
    with pytest.raises(SubstrateError) as raised:
        pi_cli._require_reachable_postgres(UNREACHABLE_DSN)
    assert executed == ["SELECT 1"]
    assert raised.value.code == "install_postgres_dsn_unreachable"
    assert raised.value.data["stage"] == "select"
    assert "hunter2" not in str(raised.value)


# ---------- the branch that decides whether Docker runs ----------


def test_a_supplied_dsn_reaches_the_provision_step(
    tmp_path: Path, monkeypatch,
) -> None:
    """The regression itself. `_cmd_install` used to hand the provision
    step a literal `postgres_dsn=None, use_local_postgres=False`."""
    seen: list[argparse.Namespace] = []
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(pi_cli, "_resolve_git_repos", lambda explicit: None)
    monkeypatch.setattr(
        pi_cli, "_require_reachable_postgres", lambda dsn: None,
    )

    def _capture(bootstrap_args, *, as_json):  # noqa: ANN001
        seen.append(bootstrap_args)
        return 1  # stop the install here; the handoff is what is under test

    monkeypatch.setattr(
        pi_cli, "_provision_postgres_and_scaffold", _capture,
    )

    pi_cli._cmd_install(
        _install_args(
            postgres_dsn=UNREACHABLE_DSN,
            deployments_root=str(tmp_path / "deployments"),
        ),
        as_json=True,
    )
    assert len(seen) == 1
    assert seen[0].postgres_dsn == UNREACHABLE_DSN
    assert seen[0].use_local_postgres is True

    seen.clear()
    pi_cli._cmd_install(
        _install_args(deployments_root=str(tmp_path / "deployments2")),
        as_json=True,
    )
    assert len(seen) == 1
    assert seen[0].postgres_dsn is None
    assert seen[0].use_local_postgres is False


def test_docker_is_provisioned_only_without_a_supplied_dsn(
    tmp_path: Path, monkeypatch,
) -> None:
    """The pair that gives the branch meaning: with a DSN no Docker command
    runs at all; without one, `docker compose ... up` does."""
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: tmp_path)

    def _run_recorder(recorded):
        def _run(cmd, **_kw):  # noqa: ANN001
            recorded.append(list(cmd))
            return _Done()
        return _run

    with_dsn: list[list[str]] = []
    monkeypatch.setattr(pi_cli.subprocess, "run", _run_recorder(with_dsn))
    rc = pi_cli._provision_postgres_and_scaffold(
        _bootstrap_args(
            postgres_dsn=UNREACHABLE_DSN,
            use_local_postgres=True,
            deployments_root_resolved=tmp_path / "a",
        ),
        as_json=True,
    )
    assert rc == 0
    assert [c for c in with_dsn if c and c[0] == "docker"] == []
    scaffold = [c for c in with_dsn if "--scaffold-only" in c]
    assert len(scaffold) == 1
    argv = scaffold[0]
    assert argv[argv.index("--postgres-dsn") + 1] == UNREACHABLE_DSN

    without_dsn: list[list[str]] = []
    monkeypatch.setattr(pi_cli.subprocess, "run", _run_recorder(without_dsn))
    monkeypatch.setattr(pi_cli.shutil, "which", lambda name: "/usr/bin/docker")
    rc = pi_cli._provision_postgres_and_scaffold(
        _bootstrap_args(deployments_root_resolved=tmp_path / "b"),
        as_json=True,
    )
    assert rc == 0
    compose_up = [
        c for c in without_dsn
        if c[:2] == ["docker", "compose"] and "up" in c
    ]
    assert compose_up, without_dsn


# ---------- what the probe proves beyond "something answered" ----------


class _ProbeCursor:
    """A cursor that answers the probe's catalog queries from a script.

    The probe asks four questions on every run and a fifth only when an
    extension still has to be created, so the script is keyed by what
    each statement selects rather than by call order -- a test that
    pinned the order would have to change every time a question moves.
    """

    def __init__(
        self,
        *,
        select_one=(1,),
        database=(True, True, False, True),
        available=None,
        installed="all",
        roles="admin",
        public_create=(True,),
    ):
        #: `(CREATE on database, public exists, rolsuper, rolcreaterole)`.
        self.select_one = select_one
        self.database = database
        self.available = available
        self.installed = installed
        self.roles = roles
        self.public_create = public_create
        self.executed: list[str] = []
        self._last = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def _extension_rows(self, wanted):
        names = wanted if self.available is None else self.available
        installed = wanted if self.installed == "all" else self.installed
        return [(name, name in installed) for name in names]

    def _role_rows(self, wanted):
        if self.roles == "admin":
            return [(name, True) for name in wanted]
        if self.roles == "no-admin":
            return [(name, False) for name in wanted]
        # An explicit mapping: absent names are absent roles.
        return [(name, self.roles[name]) for name in wanted if name in self.roles]

    def execute(self, sql, params=None):  # noqa: ANN001
        self.executed.append(" ".join(sql.split()))
        if sql.startswith("SELECT 1"):
            self._last = self.select_one
        elif "has_database_privilege" in sql:
            self._last = self.database
        elif "pg_available_extensions" in sql:
            self._last = self._extension_rows(list(params[0]))
        elif "pg_has_role" in sql:
            self._last = self._role_rows(list(params[0]))
        elif "has_schema_privilege" in sql:
            self._last = self.public_create
        else:  # pragma: no cover - a new query must be scripted here
            raise AssertionError(f"unscripted probe query: {sql}")

    def fetchone(self):
        return self._last

    def fetchall(self):
        return self._last


class _ProbeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return self._cursor


def _run_probe(monkeypatch, cursor):
    import psycopg

    monkeypatch.setattr(psycopg, "connect", lambda *a, **k: _ProbeConn(cursor))
    return pi_cli._require_reachable_postgres(UNREACHABLE_DSN)


def test_a_usable_database_passes_the_probe(monkeypatch) -> None:
    """The positive half of the pair below: a role that may create, on a
    server that carries every required extension and every schema role
    this role administers, is not refused -- and the probe asks the four
    questions it claims to ask. The fifth is conditional and has its own
    pair of tests."""
    cursor = _ProbeCursor()
    assert _run_probe(monkeypatch, cursor) is None
    assert len(cursor.executed) == 4
    assert cursor.executed[0] == "SELECT 1"
    assert "has_database_privilege" in cursor.executed[1]
    assert "pg_namespace" in cursor.executed[1]
    assert "rolcreaterole" in cursor.executed[1]
    assert "pg_available_extensions" in cursor.executed[2]
    assert "pg_has_role" in cursor.executed[3]


def test_a_role_that_cannot_create_in_the_database_is_refused(
    monkeypatch,
) -> None:
    """Reachability is not usability. Reproduced against a real database
    owned by a role with CREATE revoked: `SELECT 1` passed, the install
    continued, and the migration failed AFTER deployment.yaml, the
    schemas and the source registry were on disk. CREATE on the database
    is what `CREATE SCHEMA okg` needs, so it is asked on every run."""
    from okg.substrate.errors import SubstrateError

    with pytest.raises(SubstrateError) as raised:
        _run_probe(monkeypatch, _ProbeCursor(database=(False, True, False, True)))
    assert raised.value.code == "install_postgres_dsn_unusable"
    assert raised.value.data["stage"] == "privileges"
    assert raised.value.data["missing_privileges"] == ["CREATE on the database"]
    assert "CREATE on the database" in str(raised.value)
    assert "hunter2" not in str(raised.value), "the password reached the refusal"


def test_a_server_without_the_required_extensions_is_refused(
    monkeypatch,
) -> None:
    """A stock PostgreSQL answers, and lets this role create, and still
    cannot run the migration: `CREATE EXTENSION timescaledb` needs the
    extension to be installed on the server. The names come from the
    migrator's own `REQUIRED_EXTENSIONS`, so a new requirement there
    cannot leave this probe behind."""
    from okg.substrate.db.migrate import REQUIRED_EXTENSIONS
    from okg.substrate.errors import SubstrateError

    present = sorted(set(REQUIRED_EXTENSIONS) - {"timescaledb", "vector"})
    with pytest.raises(SubstrateError) as raised:
        _run_probe(monkeypatch, _ProbeCursor(available=present))
    assert raised.value.code == "install_postgres_dsn_unusable"
    assert raised.value.data["stage"] == "extensions"
    assert raised.value.data["missing_extensions"] == ["timescaledb", "vector"]
    assert "hunter2" not in str(raised.value)


# ---------- what the probe must NOT refuse (review finding B2) ----------


def test_a_role_without_create_on_public_passes_when_extensions_exist(
    monkeypatch,
) -> None:
    """The shape a managed PostgreSQL hands an operator: a DBA creates
    the database, installs the extensions, and grants the role CREATE on
    the database and nothing on `public`. On PostgreSQL 15 and later
    that is the DEFAULT, because CREATE on `public` is no longer granted
    to PUBLIC.

    Every `CREATE TABLE` in `schema.sql` is qualified into `okg` or
    `okg_derived`, and `ensure_required_extensions` skips `CREATE
    EXTENSION` for every extension already present, so nothing in the
    migration touches `public`. Asking for CREATE on `public` anyway
    refused this operator outright -- reproduced live on PostgreSQL
    17.10, where that same role then created both schemas and a table
    with a `vector` column. So the probe must not even ASK: no
    `has_schema_privilege` statement is issued here."""
    cursor = _ProbeCursor(database=(True, True, False, True), public_create=(False,))
    assert _run_probe(monkeypatch, cursor) is None
    assert not [sql for sql in cursor.executed if "has_schema_privilege" in sql]


def test_create_on_public_is_required_only_for_extensions_still_to_install(
    monkeypatch,
) -> None:
    """The other side of the same condition: when an extension is NOT
    installed yet, the unqualified `CREATE EXTENSION` does land in
    `public`, so the probe asks and refuses -- naming the extensions
    that made the privilege necessary."""
    from okg.substrate.errors import SubstrateError

    cursor = _ProbeCursor(installed=["timescaledb"], public_create=(False,))
    with pytest.raises(SubstrateError) as raised:
        _run_probe(monkeypatch, cursor)
    assert raised.value.code == "install_postgres_dsn_unusable"
    assert raised.value.data["stage"] == "privileges"
    assert raised.value.data["missing_privileges"] == ["CREATE on schema public"]
    assert "vector" in raised.value.data["missing_extensions"]
    assert "hunter2" not in str(raised.value)


# ---------- the roles the migration creates and grants (finding B3) ----------


def test_a_role_without_createrole_is_refused_when_a_schema_role_is_absent(
    monkeypatch,
) -> None:
    """`_ensure_schema_roles` and `views.sql` issue `CREATE ROLE` for
    every name in `migrate.SCHEMA_ROLES`. A role without CREATEROLE
    cannot, and the install used to discover that only after the
    deployment directory existed."""
    from okg.substrate.db.migrate import SCHEMA_ROLES
    from okg.substrate.errors import SubstrateError

    present = {name: True for name in SCHEMA_ROLES if name != "app_rw"}
    with pytest.raises(SubstrateError) as raised:
        _run_probe(
            monkeypatch,
            _ProbeCursor(database=(True, True, False, False), roles=present),
        )
    assert raised.value.code == "install_postgres_dsn_unusable"
    assert raised.value.data["stage"] == "roles"
    assert raised.value.data["missing_privileges"] == ["CREATEROLE"]
    assert raised.value.data["missing_roles"] == ["app_rw"]
    assert "hunter2" not in str(raised.value)


def test_a_role_without_admin_option_on_an_existing_schema_role_is_refused(
    monkeypatch,
) -> None:
    """The case that survived the first probe and that a real migration
    failed on: every schema role already exists, so CREATEROLE is never
    needed, and `schema.sql` then runs `GRANT app_rw, catalog_admin TO
    projection_remediator`. On PostgreSQL 16 and later that needs ADMIN
    OPTION on each granted role -- CREATEROLE does not carry it over a
    role this one did not create. Reproduced live on PostgreSQL 17.10:
    the old probe passed and `okg migrate` died with `permission denied
    to grant role "app_rw"`."""
    from okg.substrate.db.migrate import SCHEMA_ROLES_NEEDING_ADMIN_OPTION
    from okg.substrate.errors import SubstrateError

    with pytest.raises(SubstrateError) as raised:
        _run_probe(
            monkeypatch,
            _ProbeCursor(database=(True, True, False, True), roles="no-admin"),
        )
    assert raised.value.code == "install_postgres_dsn_unusable"
    assert raised.value.data["stage"] == "roles"
    assert raised.value.data["missing_roles"] == sorted(
        SCHEMA_ROLES_NEEDING_ADMIN_OPTION
    )
    assert "ADMIN OPTION on app_rw" in raised.value.data["missing_privileges"]
    assert "hunter2" not in str(raised.value)


def test_a_superuser_is_not_asked_about_roles(monkeypatch) -> None:
    """A superuser may create and grant any role, so neither role check
    applies -- the guard that keeps the two checks above from refusing
    the ordinary `postgres` install."""
    cursor = _ProbeCursor(database=(True, True, True, False), roles="no-admin")
    assert _run_probe(monkeypatch, cursor) is None


def test_the_schema_role_list_matches_the_sql_that_creates_those_roles() -> None:
    """The drift guard for both constants. `SCHEMA_ROLES` exists so the
    probe asks about the roles the migration actually creates; a role
    added to `schema.sql` or `views.sql` and not to the constant would
    leave the probe quietly incomplete, which is the defect class this
    change is about."""
    import re

    from okg.substrate.db import migrate as migrate_mod
    from okg.substrate.db.migrate import (
        SCHEMA_ROLES,
        SCHEMA_ROLES_NEEDING_ADMIN_OPTION,
    )

    sql_dir = Path(migrate_mod.__file__).parent
    sql = "\n".join(
        (sql_dir / name).read_text() for name in ("schema.sql", "views.sql")
    )
    sql += migrate_mod._ensure_schema_roles.__doc__ or ""
    import inspect

    sql += inspect.getsource(migrate_mod._ensure_schema_roles)

    created = set(re.findall(r"CREATE ROLE ([a-z_][a-z0-9_]*)", sql))
    altered = set(re.findall(r"ALTER ROLE ([a-z_][a-z0-9_]*)", sql))
    granted = set()
    for names in re.findall(
        r"GRANT ((?:[a-z_][a-z0-9_]*)(?:\s*,\s*[a-z_][a-z0-9_]*)*) TO ", sql
    ):
        granted.update(part.strip() for part in names.split(","))

    assert created - set(SCHEMA_ROLES) == set(), (
        "a role the migration creates is missing from SCHEMA_ROLES"
    )
    assert (altered | granted) - set(SCHEMA_ROLES_NEEDING_ADMIN_OPTION) == set(), (
        "a role the migration grants or alters is missing from "
        "SCHEMA_ROLES_NEEDING_ADMIN_OPTION"
    )


# ---------- a dropped `public` is not an unreachable database (S4) ----------


def test_a_missing_public_schema_is_not_reported_as_unreachable(
    monkeypatch,
) -> None:
    """`has_schema_privilege(current_user,'public','CREATE')` raises
    `InvalidSchemaName` when `public` has been dropped. Inside the
    probe's connect block that landed in the broad `except` and was
    reported as `--postgres-dsn is not reachable` on a database that had
    answered -- reproduced live. The probe now reads `pg_namespace`
    first and, when no extension has to be installed, never asks: the
    migration does not need `public`."""
    cursor = _ProbeCursor(database=(True, False, False, True))
    assert _run_probe(monkeypatch, cursor) is None
    assert not [sql for sql in cursor.executed if "has_schema_privilege" in sql]


def test_a_missing_public_schema_is_refused_as_unusable_when_needed(
    monkeypatch,
) -> None:
    """When an extension still has to be created, the unqualified
    `CREATE EXTENSION` needs `public`, so its absence IS a refusal --
    but an `unusable` one naming the schema, not `unreachable`."""
    from okg.substrate.errors import SubstrateError

    with pytest.raises(SubstrateError) as raised:
        _run_probe(
            monkeypatch,
            _ProbeCursor(database=(True, False, False, True), installed=[]),
        )
    assert raised.value.code == "install_postgres_dsn_unusable"
    assert raised.value.data["stage"] == "schema"
    assert raised.value.data["missing_schema"] == "public"
    assert "hunter2" not in str(raised.value)


# ---------- the three hops a mutant used to survive ----------


def test_the_bootstrap_complete_line_masks_the_password(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """Guards mutant R4. `bootstrap_complete` is the installer's last
    word and it printed the DSN whole -- the generated password before,
    and the operator's own once the flag is honored. Mutating the
    redaction away here left the whole focused set green."""
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        pi_cli.subprocess, "run", lambda cmd, **kw: _Done(),
    )

    rc = pi_cli._provision_postgres_and_scaffold(
        _bootstrap_args(
            postgres_dsn=UNREACHABLE_DSN,
            use_local_postgres=True,
            deployments_root_resolved=tmp_path / "boot",
        ),
        as_json=True,
    )

    assert rc == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["status"] == "bootstrap_complete"
    assert "hunter2" not in captured.out + captured.err
    assert payload["postgres_dsn"] != UNREACHABLE_DSN
    # Redaction, not deletion: the operator still has to recognize which
    # database the installer used.
    assert "127.0.0.1:1" in payload["postgres_dsn"]
    # Issue #2331: the scaffold report names the same first step as the
    # final install report, the command that adds a first source.
    assert payload["next_steps"][0].startswith(
        "okg deployment add-source ",
    )


def test_the_profile_answer_slot_is_read_too() -> None:
    """Guards mutant R5. `okg init --profile academic-researcher` puts the DSN in
    `answer_postgres_dsn`, not `postgres_dsn` -- the injector only maps it
    onto the native dest when the native flag exists on that parser.
    Dropping the second slot left the whole focused set green, because
    every other test builds the namespace with the native dest."""
    only_answer = argparse.Namespace(answer_postgres_dsn=UNREACHABLE_DSN)
    assert pi_cli._operator_postgres_dsn(only_answer) == UNREACHABLE_DSN

    only_native = argparse.Namespace(postgres_dsn=UNREACHABLE_DSN)
    assert pi_cli._operator_postgres_dsn(only_native) == UNREACHABLE_DSN

    assert pi_cli._operator_postgres_dsn(argparse.Namespace()) is None
    assert pi_cli._operator_postgres_dsn(
        argparse.Namespace(postgres_dsn=None, answer_postgres_dsn=None),
    ) is None


class _StopAfterRegistration(BaseException):
    """Not an `Exception`: `_cmd_install` wraps the registration call in
    `except Exception`, so anything narrower would be swallowed and the
    install would carry on past the assertion point."""


def test_the_registered_agent_runtime_gets_the_operator_dsn(
    tmp_path: Path, monkeypatch,
) -> None:
    """Guards mutant R6, the worst of the three. Falling back to the
    Docker-shaped DSN registers the MCP server against
    `postgres://postgres:okg@localhost:<port>/okg_<name>` -- a database
    that does not exist on this path -- and the operator finds out only
    when the assistant cannot read. No test covered it because every
    other test passes `--no-claude-config`."""
    from okg.substrate.library.agent_runtimes import registry

    seen: list[str] = []

    def _register(*, deployment, dsn, extra_env):  # noqa: ANN001
        seen.append(dsn)
        raise _StopAfterRegistration

    monkeypatch.setattr(registry, "register_with_all_detected", _register)
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(pi_cli, "_resolve_git_repos", lambda explicit: None)
    monkeypatch.setattr(pi_cli, "_require_reachable_postgres", lambda dsn: None)
    monkeypatch.setattr(
        pi_cli, "_provision_postgres_and_scaffold", lambda a, **k: 0,
    )
    monkeypatch.setattr(
        pi_cli, "_resolve_existing_dep_dir", lambda *a, **k: tmp_path / "dep",
    )

    with pytest.raises(_StopAfterRegistration):
        pi_cli._cmd_install(
            _install_args(
                postgres_dsn=UNREACHABLE_DSN,
                no_claude_config=False,
                deployments_root=str(tmp_path / "deployments"),
            ),
            as_json=True,
        )

    assert seen == [UNREACHABLE_DSN]


# ---------- the deployment the child process writes ----------


def test_the_written_deployment_carries_the_supplied_dsn(
    tmp_path: Path, monkeypatch,
) -> None:
    """End of the chain. The scaffold argv the installer builds is run for
    real, in-process, and the file it writes is read back. Nothing here
    asserts against a value the test itself supplied to the writer."""
    from okg.substrate import fastyaml

    root = (tmp_path / "deployments").resolve()
    dsn = "postgresql://postgres:hunter2@127.0.0.1:1/fix_s14_2210_written"
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: tmp_path)
    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("OKG_DEPLOYMENTS_DIR", raising=False)

    # `pi_cli.subprocess` IS the stdlib module, so the patch is global for
    # the duration: anything that is not the scaffold child (the scaffold
    # itself shells out to `git`) has to reach the real implementation.
    real_run = pi_cli.subprocess.run
    scaffolded: list[list[str]] = []

    # The child is `<this interpreter> -P -m okg.substrate.cli.main install
    # --scaffold-only ...` (issue #2211); everything after the module name
    # is the CLI argv, which runs here in-process.
    child_prefix = [sys.executable, "-P", "-m", "okg.substrate.cli.main"]

    def _run(cmd, **kwargs):  # noqa: ANN001
        argv = list(cmd)
        if argv[:4] != child_prefix or "--scaffold-only" not in argv:
            return real_run(cmd, **kwargs)
        scaffolded.append(argv)
        assert cli.main(argv[4:] + ["--no-publish", "--non-interactive"]) == 0
        return _Done()

    monkeypatch.setattr(pi_cli.subprocess, "run", _run)

    rc = pi_cli._provision_postgres_and_scaffold(
        _bootstrap_args(
            postgres_dsn=dsn,
            use_local_postgres=True,
            deployments_root_resolved=root,
        ),
        as_json=True,
    )

    assert rc == 0
    assert len(scaffolded) == 1, scaffolded
    written = fastyaml.safe_load(
        (root / "dsnprobe" / "deployment.yaml").read_text(encoding="utf-8")
    )
    # `postgres.dsn` is the key every later command reads; the
    # `profile.init_answers` copy is a redacted display value.
    assert written["postgres"]["dsn"] == dsn
    answers = (written.get("profile") or {}).get("init_answers") or {}
    assert "hunter2" not in answers["postgres_dsn"]
    # No compose stack exists, so a `docker:` block would point the
    # lifecycle verbs at a project that was never created.
    assert "docker" not in written


# ---------- the plan ----------


def test_the_dry_run_plan_reports_and_reproduces_the_dsn(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """`next_step` dropped `--profile` and `--postgres-dsn`, so running the
    printed line did something other than what the plan described. The plan
    also has to say which database it would use -- with the password
    masked, because a plan is printed and piped."""
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(pi_cli, "_resolve_git_repos", lambda explicit: None)
    monkeypatch.setattr(
        pi_cli,
        "_docker_state",
        lambda: {
            "docker_on_path": False,
            "daemon_reachable": False,
            "image_present": False,
            "image_build_needed": True,
        },
    )
    root = tmp_path / "deployments"

    rc = pi_cli._cmd_install(
        _install_args(
            postgres_dsn=UNREACHABLE_DSN,
            dry_run=True,
            deployments_root=str(root),
        ),
        as_json=True,
    )
    plan = json.loads(capsys.readouterr().out)

    assert rc == 0, plan
    # An unreachable Docker daemon blocks nothing when no container would
    # be started; blocking here would refuse the one path Docker-free.
    assert plan["blockers"] == []
    assert plan["postgres_provisioning"] == "operator_supplied_dsn"
    assert plan["postgres_dsn"] == "postgresql://postgres:***@127.0.0.1:1/okg_probe"
    assert plan["ports"]["postgres"] is None
    assert "hunter2" not in json.dumps(plan)
    assert "--profile academic-researcher" in plan["next_step"]
    assert "--postgres-dsn" in plan["next_step"]
    # A plan writes nothing.
    assert not root.exists()


def test_the_dry_run_plan_without_a_dsn_still_needs_docker(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """The mirror: the Docker blocker is dropped for the supplied-DSN path
    only, not removed."""
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(pi_cli, "_resolve_git_repos", lambda explicit: None)
    monkeypatch.setattr(
        pi_cli,
        "_docker_state",
        lambda: {
            "docker_on_path": False,
            "daemon_reachable": False,
            "image_present": False,
            "image_build_needed": True,
        },
    )

    rc = pi_cli._cmd_install(
        _install_args(
            dry_run=True,
            deployments_root=str(tmp_path / "deployments"),
        ),
        as_json=True,
    )
    plan = json.loads(capsys.readouterr().out)

    assert rc == 1
    assert any("Docker daemon unreachable" in b for b in plan["blockers"])
    assert plan["postgres_provisioning"] == "docker"
    assert plan["postgres_dsn"] is None


@pytest.mark.parametrize("orcid", [None, "0000-0002-1234-5678"])
def test_the_dry_run_next_step_is_a_command_okg_parses(
    tmp_path: Path, monkeypatch, capsys, orcid,
) -> None:
    """Issue #2277: with no ORCID the next step printed `--orcid None`.
    It is now built by `okg_command` (#2575): an unset answer is left out,
    every flag the plan depends on is kept, the DSN is masked, and the line
    parses with the real okg parser."""
    import shlex

    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(pi_cli, "_resolve_git_repos", lambda explicit: None)
    monkeypatch.setattr(
        pi_cli,
        "_docker_state",
        lambda: {
            "docker_on_path": False,
            "daemon_reachable": False,
            "image_present": False,
            "image_build_needed": True,
        },
    )
    root = tmp_path / "deployments"

    rc = pi_cli._cmd_install(
        _install_args(
            orcid=orcid,
            postgres_dsn=UNREACHABLE_DSN,
            dry_run=True,
            deployments_root=str(root),
        ),
        as_json=True,
    )
    plan = json.loads(capsys.readouterr().out)

    assert rc == 0, plan
    next_step = plan["next_step"]
    assert "None" not in next_step
    assert "hunter2" not in next_step
    tokens = shlex.split(next_step)
    assert tokens[:2] == ["okg", "install"]
    args = cli._build_parser().parse_args(tokens[1:])
    assert args.profile == "academic-researcher"
    assert args.name == "dsnprobe"
    assert args.orcid == orcid
    assert args.postgres_dsn == "postgresql://postgres:***@127.0.0.1:1/okg_probe"
    assert Path(args.deployments_root) == root
    assert ("--orcid" in tokens) is (orcid is not None)


@pytest.mark.parametrize("supplied", [True, False])
def test_the_docker_block_is_written_only_for_a_docker_install(
    tmp_path: Path, monkeypatch, capsys, supplied: bool,
) -> None:
    """A full `_cmd_install` run, scaffold stubbed, to the final payload.

    `deployment.yaml`'s `docker:` block tells `okg start/stop/restart` which
    compose project to drive. With an operator-supplied database there is no
    compose project, so writing the block points those verbs at a stack that
    does not exist. This is the one hop the provision-level tests above
    cannot see, because the block is written by `_cmd_install` after the
    provision step returns.
    """
    root = (tmp_path / "deployments").resolve()
    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("OKG_DEPLOYMENTS_DIR", raising=False)
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(pi_cli, "_resolve_git_repos", lambda explicit: None)
    monkeypatch.setattr(
        pi_cli, "_require_reachable_postgres", lambda dsn: None,
    )
    monkeypatch.setattr(
        pi_cli,
        "_maybe_start_embed_worker",
        lambda **_kw: {"started": False, "reason": "not_requested"},
    )
    # The database step is not under test here; it has its own tests.
    monkeypatch.setattr(
        pi_cli,
        "_retire_legacy_storage_and_migrate",
        lambda *_a, **_kw: {"ran": True, "status": "ok", "retirement_plan": {}},
    )
    docker_blocks: list[object] = []
    monkeypatch.setattr(
        pi_cli,
        "_write_deployment_docker_block",
        lambda *a, **k: docker_blocks.append(k),
    )

    def _scaffold(bootstrap_args, *, as_json):  # noqa: ANN001
        target = bootstrap_args.deployments_root_resolved / "dsnprobe"
        target.mkdir(parents=True)
        (target / "deployment.yaml").write_text(
            "name: dsnprobe\nrelease:\n  channel: dev\n", encoding="utf-8",
        )
        (target / "source_registry.yaml").write_text(
            "sources: {}\n", encoding="utf-8",
        )
        bootstrap_args.postgres_port = 15435
        return 0

    monkeypatch.setattr(
        pi_cli, "_provision_postgres_and_scaffold", _scaffold,
    )

    rc = pi_cli._cmd_install(
        _install_args(
            postgres_dsn=UNREACHABLE_DSN if supplied else None,
            deployments_root=str(root),
        ),
        as_json=True,
    )
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0, payload
    assert payload["status"] == "install_complete"
    if supplied:
        assert docker_blocks == [], (
            "a `docker:` block was written for an install that created no "
            "compose project"
        )
        assert payload["postgres_provisioning"] == "operator_supplied_dsn"
        assert payload["postgres_dsn"] == (
            "postgresql://postgres:***@127.0.0.1:1/okg_probe"
        )
        assert payload["postgres_port"] is None
        assert "hunter2" not in json.dumps(payload)
    else:
        assert len(docker_blocks) == 1
        assert docker_blocks[0]["host_port"] == 15435
        assert payload["postgres_provisioning"] == "docker"
        assert payload["postgres_dsn"] is None


# ---------- the probe uses the substrate connection factory ----------


def test_the_probe_opens_its_connection_through_the_substrate_factory(
    monkeypatch,
) -> None:
    """A raw `psycopg.connect` would give the probe none of the session
    posture every other substrate connection carries. Route it through
    `okg.substrate.db.connection.connect` and the keepalives, the finite
    connect timeout, the pinned `search_path` and the
    idle-in-transaction bound all come with it -- and the connection
    names itself in `pg_stat_activity`.

    The probe runs BEFORE any deployment directory is written, so it
    names no deployment; this asserts the factory still serves it.
    """
    from chronos_core.pg_session import SEARCH_PATH_OPTION

    seen: list[tuple[tuple, dict]] = []
    cursor = _ProbeCursor()

    import psycopg

    def _record(*args, **kwargs):
        seen.append((args, kwargs))
        return _ProbeConn(cursor)

    monkeypatch.setattr(psycopg, "connect", _record)
    assert pi_cli._require_reachable_postgres(UNREACHABLE_DSN) is None

    assert len(seen) == 1
    args, kwargs = seen[0]
    assert args == (UNREACHABLE_DSN,)
    assert kwargs["application_name"] == "okg-install-postgres-probe"
    assert kwargs["connect_timeout"] == 5, (
        "the probe must fail fast; the operator is staring at a blank "
        "terminal while it runs"
    )
    assert kwargs["autocommit"] is True, (
        "three catalog reads must not leave an idle transaction on an "
        "operator-supplied server"
    )
    # The posture a raw `psycopg.connect(dsn, connect_timeout=10)` did
    # not carry.
    assert kwargs["keepalives"] == 1
    assert SEARCH_PATH_OPTION in kwargs["options"]
    assert "deployment" not in kwargs, (
        "the factory, not psycopg, consumes `deployment`"
    )


# ---------- the scaffold child that fails ----------


def test_a_failing_scaffold_child_never_prints_the_operator_password(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """Review B1. Honoring `--postgres-dsn` puts the operator's OWN password
    on the scaffold child's argv, and a failing child echoes that argv back:
    argparse repeats the arguments it refused, and the stale-entrypoint
    refusal repeats the whole command in `fix_hint` and
    `data.expected_command`. The installer relays the child's output tail,
    so before the redaction in #2225 the password reached the operator's
    JSON envelope twice. This runs the real `okg install` entry point with
    a sentinel password, makes the child fail with both echoes, and reads
    every byte the installer printed."""
    sentinel = "S14x2210SentinelPw"
    dsn = f"postgresql://okg_operator:{sentinel}@127.0.0.1:1/fix_s14_2210_child"
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(pi_cli, "_resolve_git_repos", lambda explicit: None)
    # The probe is covered above; this test is about the child's output.
    monkeypatch.setattr(pi_cli, "_require_reachable_postgres", lambda d: None)
    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("OKG_DEPLOYMENTS_DIR", raising=False)

    real_run = pi_cli.subprocess.run
    refused: list[list[str]] = []

    def _run(cmd, **kwargs):  # noqa: ANN001
        argv = list(cmd)
        if "--scaffold-only" not in argv:
            return real_run(cmd, **kwargs)
        refused.append(argv)
        tail = argv[argv.index("install") + 1:]
        rendered = " ".join(pi_cli.shlex.quote(a) for a in tail)
        expected = f"cd /checkout && uv run okg install {rendered}"
        # The two shapes a real refusing child prints: the substrate JSON
        # envelope on stdout and an argparse echo on stderr.
        envelope = json.dumps(
            {
                "code": "substrate_error",
                "error": "bare okg is not running this checkout's code",
                "fix_hint": f"use `{expected}`",
                "data": {"expected_command": expected},
            },
            indent=2,
        )
        raise pi_cli.subprocess.CalledProcessError(
            2, argv, output=envelope,
            stderr=(
                "usage: okg [-h] ...\n"
                f"okg: error: unrecognized arguments: {rendered}"
            ),
        )

    monkeypatch.setattr(pi_cli.subprocess, "run", _run)

    rc = cli.main([
        "install", "--profile", "academic-researcher", "--name", "dsnchild",
        "--postgres-dsn", dsn,
        "--deployments-root", str(tmp_path / "deployments"),
        "--no-claude-config", "--yes", "--json",
    ])
    captured = capsys.readouterr()

    assert rc != 0
    assert len(refused) == 1, refused
    # The child really was handed the password -- otherwise the absence
    # below would prove nothing.
    assert dsn in refused[0], refused[0]
    assert sentinel not in captured.out, captured.out
    assert sentinel not in captured.err, captured.err
    # Masked, not dropped: the report still carries the child's echo.
    assert "unrecognized arguments:" in captured.out + captured.err
    assert "expected_command" in captured.out + captured.err
    assert "<redacted-dsn>" in captured.out + captured.err


# ---------- the ${OKG_DSN} placeholder (issue #3480) ----------


def test_the_placeholder_dsn_is_expanded_for_the_probe_only(
    tmp_path: Path, monkeypatch,
) -> None:
    """`--postgres-dsn '${OKG_DSN}'` keeps the password off the command
    line. The default profile expands it; academic-researcher handed the
    literal to libpq and refused `install_postgres_dsn_unreachable`. The
    probe now gets the expanded value; the scaffold child still gets the
    placeholder, so deployment.yaml never holds the password."""
    probed: list[str] = []
    seen: list[argparse.Namespace] = []
    monkeypatch.setenv("OKG_DSN", UNREACHABLE_DSN)
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(pi_cli, "_resolve_git_repos", lambda explicit: None)
    monkeypatch.setattr(pi_cli, "_require_reachable_postgres", probed.append)
    monkeypatch.setattr(
        pi_cli,
        "_provision_postgres_and_scaffold",
        lambda bootstrap_args, *, as_json: seen.append(bootstrap_args) or 1,
    )

    pi_cli._cmd_install(
        _install_args(
            postgres_dsn="${OKG_DSN}",
            deployments_root=str(tmp_path / "deployments"),
        ),
        as_json=True,
    )

    assert probed == [UNREACHABLE_DSN]
    assert [a.postgres_dsn for a in seen] == ["${OKG_DSN}"]


def test_an_unset_placeholder_variable_is_refused_by_name(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """Fail closed: an unset variable refuses before any connection or
    scaffold, naming the variable, instead of reaching libpq as text."""
    ran: list[str] = []
    monkeypatch.delenv("OKG_NIF_UNSET_DSN", raising=False)
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(pi_cli, "_resolve_git_repos", lambda explicit: None)
    monkeypatch.setattr(
        pi_cli, "_require_reachable_postgres", lambda dsn: ran.append("probe"),
    )
    monkeypatch.setattr(
        pi_cli,
        "_provision_postgres_and_scaffold",
        lambda a, **k: ran.append("scaffold") or 0,
    )

    rc = pi_cli._cmd_install(
        _install_args(
            postgres_dsn="${OKG_NIF_UNSET_DSN}",
            deployments_root=str(tmp_path / "deployments"),
        ),
        as_json=True,
    )

    assert rc != 0
    assert ran == []
    captured = capsys.readouterr()
    assert "OKG_NIF_UNSET_DSN" in captured.out + captured.err


def test_the_deployment_dsn_is_expanded_before_a_connection(monkeypatch) -> None:
    """deployment.yaml keeps `${OKG_DSN}`; the post-install steps that open
    connections (migrate, add, counts) expand it first."""
    monkeypatch.setenv("OKG_DSN", UNREACHABLE_DSN)
    assert pi_cli._expanded_dsn("${OKG_DSN}") == UNREACHABLE_DSN
    assert pi_cli._expanded_dsn(UNREACHABLE_DSN) == UNREACHABLE_DSN
