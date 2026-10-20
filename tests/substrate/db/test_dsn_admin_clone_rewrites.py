"""DSN rewrites on the admin and clone paths keep every other parameter and the form.

Sprint 14 DSN migration, part A. Six files used to rewrite a DSN privately:
`psycopg.conninfo.make_conninfo` (which turns a URL into keyword form),
`urlsplit` (which dropped the query string in `deployment_isolation._admin_dsn`),
or a hand-kept environment map (which dropped `options` for `pg_dump`). They
now go through `okg.substrate.db.dsn`. Each test below feeds one rewrite a DSN
carrying `sslmode`, `options`, `application_name` and a percent-encoded
password, in both forms, and checks that only the named parameter changed.
"""

from __future__ import annotations

import importlib.util
import json
import traceback
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import psycopg
import pytest

import okg.substrate.deployment_isolation as isolation
import okg.substrate.environment_clone as env_clone
import okg.substrate.postgres_clone as pg_clone
from okg.substrate.db import dsn
from okg.substrate.db import management
from okg.substrate.db import migrate as migrate_mod
from okg.substrate.errors import (
    DatabaseCloneError,
    DeploymentEnvironmentInvalid,
    MigrateAuthorityUnresolvable,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
SEED_SCRIPT = (
    REPO_ROOT / "src" / "okg" / "operator_ui" / "web" / "audit" / "seed_fixture.py"
)

# `p@ss/w:rd` percent-encoded: a raw `@`, `/` or `:` would change the URL.
ENCODED_PASSWORD = "p%40ss%2Fw%3Ard"
URL_DSN = (
    f"postgresql://okg:{ENCODED_PASSWORD}@db.example:5433/okg_main"
    "?sslmode=disable&options=-c%20statement_timeout%3D5000"
    "&application_name=okg-admin"
)
KEYWORD_DSN = (
    "host=db.example port=5433 user=okg password='p@ss w:rd' dbname=okg_main "
    "sslmode=disable options='-c statement_timeout=5000' application_name=okg-admin"
)
FIXTURE_URL = "postgresql://postgres:okg@127.0.0.1:5544/okg_audit_fixture"
FORMS = pytest.mark.parametrize(
    "source", [URL_DSN, KEYWORD_DSN], ids=["url", "keyword"]
)


def _assert_only_changed(source: str, rewritten: str, **changes: str) -> None:
    """``rewritten`` is ``source`` in the same form with only ``changes`` applied."""
    assert dsn.form_of(rewritten) == dsn.form_of(source), dsn.redact(rewritten)
    before = dict(dsn.parse(source).params)
    after = dict(dsn.parse(rewritten).params)
    assert after == {**before, **changes}
    if dsn.form_of(source) == "url":
        # The password stays percent-encoded and the query string survives.
        assert f"okg:{ENCODED_PASSWORD}@" in rewritten
        assert "sslmode=disable" in rewritten
        assert "application_name=okg-admin" in rewritten


class _Refused(Exception):
    """Stops a function at its first connect, after recording the DSN."""


def _recording_connect(seen: list[str], error: type[BaseException] = _Refused):
    def connect(conninfo: str, *_args: Any, **_kwargs: Any):
        seen.append(conninfo)
        raise error("stop here")

    return connect


# -- db/management.py -----------------------------------------------------


@FORMS
def test_management_dsn_for_database_keeps_parameters_and_form(source: str) -> None:
    _assert_only_changed(
        source, management.dsn_for_database(source, "okg_other"), dbname="okg_other"
    )


@FORMS
def test_management_ephemeral_probe_keeps_parameters_and_form(
    source: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[str] = []
    monkeypatch.setattr(
        management, "db_connect", _recording_connect(seen, psycopg.OperationalError)
    )
    assert management.database_is_registered_ephemeral(source, "okg_scratch") is None
    _assert_only_changed(source, seen[0], dbname="okg_scratch")


def test_management_database_name_from_dsn_reads_both_forms() -> None:
    assert management.database_name_from_dsn(URL_DSN) == "okg_main"
    assert management.database_name_from_dsn(KEYWORD_DSN) == "okg_main"
    assert management.database_name_from_dsn("not a dsn") is None


# -- postgres_clone.py ----------------------------------------------------


@FORMS
def test_postgres_clone_maintenance_dsn_keeps_parameters_and_form(source: str) -> None:
    _assert_only_changed(source, pg_clone._maintenance_dsn(source), dbname="postgres")


@FORMS
def test_postgres_clone_probe_dsn_keeps_parameters_and_form(
    source: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[str] = []
    monkeypatch.setattr(
        pg_clone, "db_connect", _recording_connect(seen, psycopg.OperationalError)
    )
    with pytest.raises(
        DatabaseCloneError, match="cannot resolve PostgreSQL tool route"
    ):
        pg_clone._resolved_tool_route(
            source,
            database="okg_main",
            probe_database="okg_probe",
            expected_server=SimpleNamespace(),
        )
    _assert_only_changed(source, seen[0], dbname="okg_probe")


def test_postgres_clone_invalid_dsn_is_a_typed_refusal() -> None:
    with pytest.raises(DatabaseCloneError, match="database DSN is invalid"):
        pg_clone._database_name("host=h password=my secret", stage="source-preflight")


# -- environment_clone.py -------------------------------------------------


@FORMS
def test_environment_clone_writable_dsn_appends_to_options(source: str) -> None:
    _assert_only_changed(
        source,
        env_clone._writable_clone_dsn(source),
        options="-c statement_timeout=5000 -c default_transaction_read_only=off",
    )


@FORMS
@pytest.mark.parametrize(
    "function",
    [env_clone._recreate_database, env_clone._clear_database_read_only_default],
    ids=["recreate", "clear-read-only"],
)
def test_environment_clone_maintenance_connect_keeps_parameters_and_form(
    source: str, function: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[str] = []
    monkeypatch.setattr(env_clone, "db_connect", _recording_connect(seen))
    with pytest.raises(_Refused):
        function(source, expected_database="okg_main")
    _assert_only_changed(source, seen[0], dbname="postgres")


@FORMS
def test_environment_clone_role_dsn_keeps_parameters_and_form(
    source: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    roles = ("app_ro", "app_rw", "okg_mcp")
    credentials = tmp_path / "roles.json"
    credentials.write_text(
        json.dumps(
            {
                "schema": "okg.environment_database_role_credentials/v1",
                "environment_id": "staging",
                "passwords": {role: f"{role} pw/@:" for role in roles},
            }
        ),
        encoding="utf-8",
    )
    credentials.chmod(0o600)
    target = SimpleNamespace(id="staging", database=SimpleNamespace(dsn=source))
    seen: list[str] = []
    monkeypatch.setattr(env_clone, "db_connect", _recording_connect(seen))
    with pytest.raises(_Refused):
        env_clone._verified_database_role_count(target, credentials)
    assert dsn.form_of(seen[0]) == dsn.form_of(source)
    assert dict(dsn.parse(seen[0]).params) == {
        **dsn.parse(source).params,
        "user": "app_ro",
        "password": "app_ro pw/@:",
    }


@FORMS
def test_environment_clone_child_env_keeps_options(
    source: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`pg_dump`/`pg_restore` children used to lose `options` and `application_name`."""
    monkeypatch.delenv("PGOPTIONS", raising=False)
    monkeypatch.delenv("PGAPPNAME", raising=False)
    database, env = env_clone._libpq_command_target(source)
    assert database == "okg_main"
    assert env["PGOPTIONS"] == "-c statement_timeout=5000"
    assert env["PGAPPNAME"] == "okg-admin"
    assert env["PGSSLMODE"] == "disable"
    assert env["PGPASSWORD"] == ("p@ss/w:rd" if source == URL_DSN else "p@ss w:rd")
    assert env["PGDATABASE"] == "okg_main"
    assert (env["PGHOST"], env["PGPORT"], env["PGUSER"]) == (
        "db.example",
        "5433",
        "okg",
    )


def test_environment_clone_child_env_passes_protocol_version_bounds() -> None:
    """libpq 18 reads these from PGMINPROTOCOLVERSION and PGMAXPROTOCOLVERSION."""
    _database, env = env_clone._libpq_command_target(
        URL_DSN + "&min_protocol_version=3.0&max_protocol_version=latest"
    )
    assert env["PGMINPROTOCOLVERSION"] == "3.0"
    assert env["PGMAXPROTOCOLVERSION"] == "latest"


def test_environment_clone_child_env_refuses_a_parameter_it_cannot_pass() -> None:
    with pytest.raises(DeploymentEnvironmentInvalid, match="cannot read") as caught:
        env_clone._libpq_command_target(URL_DSN + "&keepalives=1")
    assert caught.value.data["parameters"] == ["keepalives"]


# -- deployment_isolation.py ----------------------------------------------


@FORMS
def test_isolation_admin_dsn_keeps_the_query_string_and_form(source: str) -> None:
    _assert_only_changed(source, isolation._admin_dsn(source), dbname="postgres")


def test_isolation_server_key_reads_both_forms() -> None:
    assert isolation._server_key(URL_DSN) == "db.example:5433"
    assert isolation._server_key(KEYWORD_DSN) == "db.example:5433"


def test_isolation_diagnostic_cuts_at_a_secret_starting_with_the_mask() -> None:
    detail = isolation._bounded_inventory_diagnostic("git failed PGPASSWORD=**xy tail")
    assert detail == "git failed PGPASSWORD=<redacted>"


def test_isolation_diagnostic_shows_a_passfile_path() -> None:
    """A passfile path is not a secret to the shared masker; the old copy hid it."""
    detail = isolation._bounded_inventory_diagnostic("git failed PGPASSFILE=/p/.pgpass")
    assert detail == "git failed PGPASSFILE=/p/.pgpass"


@pytest.mark.parametrize("action", ["list", "gc"])
@pytest.mark.parametrize(
    "bad_dsn",
    ["postgresql//u:s3cr3tL@h/okg_demo", "host=h dbname=okg_demo password='s3cr3tL"],
    ids=["no-colon-after-scheme", "unterminated-quote"],
)
def test_isolation_malformed_dsn_is_a_json_refusal(
    action: str, bad_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review of PR 2425, should-fix 3: a malformed DSN raised a traceback."""
    import okg.substrate.catalog.ownership as ownership

    monkeypatch.setattr(isolation, "_declared_database", lambda _: "okg_demo")
    monkeypatch.setattr(isolation, "live_worktree_suffixes", lambda start=None: {})
    monkeypatch.setattr(
        ownership,
        "suggested_worktree_dsn",
        lambda **_kwargs: "postgresql://postgres@127.0.0.1:5433/okg_demo_clone",
    )
    connects: list[str] = []
    monkeypatch.setattr(
        psycopg, "connect", lambda conninfo, **_k: connects.append(conninfo)
    )
    if action == "list":
        report = isolation.list_isolated("demo", bad_dsn)
    else:
        report = isolation.gc_isolated("demo", bad_dsn, yes=True)
    assert report["ok"] is False
    assert report["code"] == "dsn_parse_error"
    assert report["deployment"] == "demo"
    assert "s3cr3tL" not in json.dumps(report)
    assert connects == []


# -- operator_ui/web/audit/seed_fixture.py --------------------------------


@pytest.fixture(scope="module")
def seed_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "seed_fixture_dsn_rewrites", SEED_SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@FORMS
def test_seed_fixture_reset_admin_dsn_keeps_parameters_and_form(
    source: str, seed_module: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture_source = source.replace("okg_main", "okg_audit_fixture")
    seen: list[str] = []
    monkeypatch.setattr(seed_module, "_connect", _recording_connect(seen))
    with pytest.raises(_Refused):
        seed_module._reset_db(fixture_source)
    _assert_only_changed(fixture_source, seen[0], dbname="postgres")


@pytest.mark.parametrize(
    "candidate",
    [
        "postgresql://postgres:WRONGSECRET@127.0.0.1:5544/okg_audit_fixture",
        # Review of PR 2425, should-fix 2: extra parameters were printed verbatim.
        FIXTURE_URL + "?sslpassword=WRONGSECRET&sslmode=require",
        FIXTURE_URL + "?passfile=/home/WRONGSECRET/.pgpass",
    ],
    ids=["password", "sslpassword", "passfile"],
)
def test_seed_fixture_refusal_does_not_print_the_password(
    candidate: str, seed_module: ModuleType
) -> None:
    with pytest.raises(
        ValueError, match="refusing destructive fixture authority"
    ) as caught:
        seed_module._fixture_dsn_parts(candidate)
    assert "WRONGSECRET" not in str(caught.value)
    assert dsn.MASK in str(caught.value)


def test_seed_fixture_refusal_still_names_non_secret_extra_parameters(
    seed_module: ModuleType,
) -> None:
    with pytest.raises(ValueError) as caught:
        seed_module._fixture_dsn_parts(
            "postgresql://postgres:okg@127.0.0.1:5544/okg_audit_fixture?sslmode=require"
        )
    assert "'sslmode': 'require'" in str(caught.value)


@pytest.mark.parametrize(
    "candidate",
    [
        "postgresql://postgres:okg@127.0.0.1:5544/okg_audit_fixture?sslmode=disable",
        "host=127.0.0.1 port=5544 user=postgres password=okg dbname=okg_audit_fixture",
        "postgresql://postgres:okg@127.0.0.1:5545/okg_audit_fixture",
        "postgresql://postgres:okg@127.0.0.1:notaport/okg_audit_fixture",
    ],
    ids=["extra-parameter", "keyword-form", "wrong-port", "malformed"],
)
def test_seed_fixture_refuses_anything_but_the_exact_fixture(
    candidate: str, seed_module: ModuleType
) -> None:
    with pytest.raises(ValueError, match="refusing destructive fixture authority"):
        seed_module._fixture_dsn_parts(candidate)


def test_seed_fixture_accepts_the_exact_fixture(seed_module: ModuleType) -> None:
    parts = seed_module._fixture_dsn_parts(
        "postgresql://postgres:okg@127.0.0.1:5544/okg_audit_fixture"
    )
    assert (parts["database"], parts["port"]) == ("okg_audit_fixture", 5544)


# -- db/migrate.py ---------------------------------------------------------


def test_migrate_gate_parse_error_does_not_echo_password_fragments() -> None:
    """libpq quotes the token it chokes on; for `password=my secret` that is the password."""
    with pytest.raises(MigrateAuthorityUnresolvable) as caught:
        migrate_mod._database_from_dsn("host=h password=my hunter2 dbname=x")
    error = caught.value
    # The message, the data and every chained exception; not the source lines.
    rendered = "".join(traceback.format_exception_only(error)) + json.dumps(error.data)
    assert "hunter2" not in rendered
    assert "DSN failed conninfo parsing" in str(error)
    assert error.__cause__ is None and error.__context__ is None
