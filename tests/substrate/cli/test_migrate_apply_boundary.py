"""`okg migrate` must not write without `--apply` — issue #1288.

The defect: migration safety depended on ``OKG_AGENT``. Outside agent
mode the CLI called ``migrate()``/``install_views()`` without ever
consulting ``args.apply``, so the same command had two different
mutation semantics and a command reached for as a read-only probe
executed schema and view writes.

These tests measure the DATABASE, not the flag plumbing: each one runs
the CLI against a real, disposable PostgreSQL database and asserts the
observable effect the write would have had is absent.
"""
from __future__ import annotations

import argparse
import json
import uuid

import psycopg
import pytest
from psycopg import conninfo, sql

from okg.substrate.cli.main import main
from okg.substrate.db.management import stamp_ephemeral_database
from okg.substrate.dsn_form import dsn_with_dbname

pytestmark = pytest.mark.postgres


def _maintenance_dsn(okg_dsn: str) -> str:
    info = conninfo.conninfo_to_dict(okg_dsn)
    info["dbname"] = "postgres"
    return conninfo.make_conninfo(**info)


def _with_dbname(okg_dsn: str, dbname: str) -> str:
    return dsn_with_dbname(okg_dsn, dbname)


@pytest.fixture()
def blank_database(okg_dsn: str):
    """A real, empty, disposable database that `okg migrate` would fill."""
    name = f"okg_test_apply_{uuid.uuid4().hex[:10]}"
    maintenance = _maintenance_dsn(okg_dsn)
    with psycopg.connect(
        maintenance, autocommit=True, connect_timeout=5,
    ) as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    dsn = _with_dbname(okg_dsn, name)
    try:
        with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
            stamp_ephemeral_database(
                conn,
                registered_by="tests.substrate.cli.test_migrate_apply_boundary",
                purpose="migrate --apply boundary witness",
            )
        yield dsn
    finally:
        with psycopg.connect(
            maintenance, autocommit=True, connect_timeout=5,
        ) as conn, conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(name)
                )
            )


def _okg_schema_present(dsn: str) -> bool:
    with psycopg.connect(dsn, connect_timeout=5) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'okg'"
        )
        return cur.fetchone() is not None


def _relations(dsn: str) -> set[str]:
    with psycopg.connect(dsn, connect_timeout=5) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'okg'"
        )
        return {row[0] for row in cur.fetchall()}


def _last_json(capsys) -> dict:
    captured = capsys.readouterr()
    stream = captured.err.strip() or captured.out.strip()
    return json.loads(stream.splitlines()[-1])


@pytest.mark.parametrize("agent_mode", [False, True])
def test_migrate_without_apply_leaves_a_blank_database_untouched(
    blank_database: str, capsys, monkeypatch, agent_mode: bool,
) -> None:
    """The defect, measured: no `--apply`, no schema, in EITHER mode."""
    if agent_mode:
        monkeypatch.setenv("OKG_AGENT", "1")
    else:
        monkeypatch.delenv("OKG_AGENT", raising=False)
    assert not _okg_schema_present(blank_database)

    rc = main(["migrate", "--dsn", blank_database, "--json"])

    assert rc != 0
    assert not _okg_schema_present(blank_database), (
        "`okg migrate` without --apply created the okg schema"
    )
    payload = _last_json(capsys)
    data = payload["error"]["data"]
    assert data["mutation_requested"] is False
    assert data["mutation_started"] is False
    assert data["views_only"] is False
    assert "--apply" in payload["error"]["fix_hint"]


def test_migrate_views_only_without_apply_leaves_a_blank_database_untouched(
    blank_database: str, capsys, monkeypatch,
) -> None:
    """`--views-only` replaces views and grants; it is a mutation too."""
    monkeypatch.delenv("OKG_AGENT", raising=False)
    assert not _okg_schema_present(blank_database)

    rc = main([
        "migrate", "--dsn", blank_database, "--views-only", "--json",
    ])

    assert rc != 0
    assert not _okg_schema_present(blank_database), (
        "`okg migrate --views-only` without --apply wrote to the database"
    )
    payload = _last_json(capsys)
    data = payload["error"]["data"]
    assert data["mutation_requested"] is False
    assert data["mutation_started"] is False
    assert data["views_only"] is True


def test_migrate_refusal_precedes_dsn_resolution(
    capsys, monkeypatch,
) -> None:
    """The refusal cannot depend on the environment it protects.

    With no DSN reachable at all, the missing `--apply` is still the
    reported cause — proving nothing was resolved or connected first.
    """
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.delenv("OKG_DSN", raising=False)

    rc = main(["migrate", "--json"])

    assert rc != 0
    payload = _last_json(capsys)
    assert "--apply" in payload["error"]["message"]
    assert payload["error"]["data"]["mutation_started"] is False


def test_migrate_with_apply_migrates_and_reports_the_mutation(
    blank_database: str, capsys, monkeypatch,
) -> None:
    """The write path still works and says so."""
    monkeypatch.delenv("OKG_AGENT", raising=False)
    from okg.substrate.db import migrate as migrate_mod

    # The shared test cluster has no pg_cron; the substrate schema does not
    # need it (mirrors the `okg_migrated` session fixture).
    original = dict(migrate_mod.REQUIRED_EXTENSIONS)
    migrate_mod.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    try:
        # `CREATE EXTENSION` is the documented step before migrate; a blank
        # database has none of them (docs/operate/deployments/database.md).
        migrate_mod.ensure_required_extensions(blank_database)
        rc = main(["migrate", "--dsn", blank_database, "--apply", "--json"])
    finally:
        migrate_mod.REQUIRED_EXTENSIONS.clear()
        migrate_mod.REQUIRED_EXTENSIONS.update(original)

    assert rc == 0, capsys.readouterr()
    payload = _last_json(capsys)
    assert payload["mutation_requested"] is True
    assert payload["mutation_started"] is True
    assert payload["statements_executed"] > 0
    assert _okg_schema_present(blank_database)
    assert _relations(blank_database), "migrate --apply created no relations"


def test_migrate_apply_flag_is_not_documented_as_agent_only() -> None:
    """The help text was the other half of the defect."""
    from okg.substrate.cli.main import _build_parser

    parser = _build_parser()
    sub = next(
        action for action in parser._actions
        if isinstance(action, argparse._SubParsersAction)
    )
    migrate_parser = sub.choices["migrate"]
    apply_action = next(
        action for action in migrate_parser._actions
        if "--apply" in action.option_strings
    )
    assert "OKG_AGENT" not in (apply_action.help or "")
    assert "Required" in (apply_action.help or "")


def test_okg_agent_env_does_not_change_the_refusal(
    blank_database: str, capsys, monkeypatch,
) -> None:
    """One command, one contract: the two modes must agree verbatim."""
    monkeypatch.delenv("OKG_AGENT", raising=False)
    assert main(["migrate", "--dsn", blank_database, "--json"]) != 0
    human = _last_json(capsys)

    monkeypatch.setenv("OKG_AGENT", "1")
    assert main(["migrate", "--dsn", blank_database, "--json"]) != 0
    agent = _last_json(capsys)

    assert human["error"]["message"] == agent["error"]["message"]
    assert human["error"]["data"] == agent["error"]["data"]
    assert not _okg_schema_present(blank_database)


def test_environment_is_not_consulted_before_refusing(monkeypatch) -> None:
    """Belt and braces: no connection is attempted on the refusal path."""
    monkeypatch.delenv("OKG_AGENT", raising=False)
    import psycopg as _psycopg

    def _explode(*args, **kwargs):  # pragma: no cover - must not run
        raise AssertionError("migrate opened a connection without --apply")

    monkeypatch.setattr(_psycopg, "connect", _explode)
    # A fixed, deliberately unusable value: the assertion is that nothing
    # connects, so the DSN's content is irrelevant and reading OKG_TEST_DSN
    # here only spread the defaulting pattern issue #1913 removed.
    monkeypatch.setenv("OKG_DSN", "postgres:///x")

    assert main(["migrate", "--json"]) != 0
