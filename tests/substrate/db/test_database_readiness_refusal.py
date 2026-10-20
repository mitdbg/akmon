"""The one readiness answer the entry commands share (issue #2128).

These cases need no database. The real-database class guard -- the entry
commands run against an unmigrated PostgreSQL database -- lives in
``tests/substrate/test_migrate.py``, which the per-PR database lane owns.
"""

from __future__ import annotations

import json

import pytest

from okg.substrate.db import readiness as rd
from okg.substrate.errors import ExtensionMissing, SubstrateError


# ---------------------------------------------------------------------------
# The command the refusal names must be one a WHEEL user can run.
# ---------------------------------------------------------------------------


def test_migrate_command_names_the_deployment_when_there_is_one() -> None:
    assert rd.migrate_command("walkpi") == (
        "okg migrate --deployment walkpi --apply"
    )


@pytest.mark.parametrize("deployment", [None, "", rd.AD_HOC_DEPLOYMENT_NAME])
def test_migrate_command_falls_back_to_the_dsn_form(deployment) -> None:
    """`default` is the ad-hoc `--dsn` path, not a manifest.

    `okg migrate --deployment default --apply` is a command the operator
    cannot run, so the placeholder name must not reach the hint.
    """
    assert rd.migrate_command(deployment) == (
        "okg migrate --dsn <the deployment DSN> --apply"
    )


# ---------------------------------------------------------------------------
# What "ready" means.
# ---------------------------------------------------------------------------


def _readiness(**overrides) -> rd.DatabaseReadiness:
    base = {
        "schema_present": True,
        "schema_applied": True,
        "ledger_present": True,
    }
    base.update(overrides)
    return rd.DatabaseReadiness(**base)


@pytest.mark.parametrize(
    "absent", ["schema_present", "schema_applied", "ledger_present"]
)
def test_any_missing_schema_fact_means_not_migrated(absent: str) -> None:
    assert not _readiness(**{absent: False}).migrated


def test_a_database_behind_by_migrations_is_still_migrated() -> None:
    """Pending migrations are reported, never refused on.

    A deployment one migration behind is a routine working state; turning
    it into a hard stop would break every deployment mid-roll.
    """
    ready = _readiness(pending_migrations=("0033_x",), pending_migration_count=1)
    assert ready.migrated
    assert ready.as_dict()["pending_migrations"] == ["0033_x"]


def test_an_absent_role_is_not_reported_as_unusable() -> None:
    """Only a role that EXISTS and cannot authenticate is a finding.

    A deployment may legitimately run without the least-privilege split,
    and the schema check already covers "nothing is provisioned".
    """
    absent = rd.RoleLoginState(role="okg_mcp", present=False, can_login=False)
    nologin = rd.RoleLoginState(role="app_ro", present=True, can_login=False)
    usable = rd.RoleLoginState(role="app_rw", present=True, can_login=True)
    ready = _readiness(roles=(absent, nologin, usable))
    assert [state.role for state in ready.unusable_roles] == ["app_ro"]


# ---------------------------------------------------------------------------
# The refusals: named code, JSON envelope, actionable hint.
# ---------------------------------------------------------------------------


def _envelope(err: SubstrateError) -> dict:
    """The exact dict the CLI prints under `--json`."""
    return json.loads(json.dumps(err.to_json()))["error"]


def test_unmigrated_refusal_is_a_named_envelope_naming_okg_migrate() -> None:
    err = rd._not_migrated_error(
        _readiness(schema_present=False, schema_applied=False, ledger_present=False),
        surface="okg run --once",
        deployment="walkpi",
    )
    payload = _envelope(err)
    assert payload["code"] == "database_not_migrated"
    assert "okg migrate --deployment walkpi --apply" in payload["fix_hint"]
    assert payload["data"]["surface"] == "okg run --once"
    assert payload["data"]["migrated"] is False
    # The measured defect was a bare traceback: the message must say what
    # is wrong in words, not repeat an exception class name.
    assert "InvalidSchemaName" not in payload["message"]
    assert "not migrated" in payload["message"]


def test_unmigrated_refusal_distinguishes_the_three_ways_to_be_unready() -> None:
    no_schema = _envelope(
        rd._not_migrated_error(
            _readiness(schema_present=False, schema_applied=False, ledger_present=False),
            surface="s",
            deployment=None,
        )
    )["message"]
    no_relations = _envelope(
        rd._not_migrated_error(
            _readiness(schema_applied=False, ledger_present=False),
            surface="s",
            deployment=None,
        )
    )["message"]
    no_ledger = _envelope(
        rd._not_migrated_error(
            _readiness(ledger_present=False), surface="s", deployment=None
        )
    )["message"]
    assert len({no_schema, no_relations, no_ledger}) == 3
    assert "`okg` schema does not exist" in no_schema
    assert "core relations are missing" in no_relations
    assert "schema_migrations` ledger" in no_ledger


def test_role_refusal_names_the_role_and_the_provisioning_route() -> None:
    """The fix_hint must state the DESIGNED route, not `ALTER ROLE`.

    Migration creates these roles NOLOGIN so it never invents a
    credential, and they are cluster-wide objects; the operator supplies
    the credential through `OKG_MCP_RO_DSN` and `okg runtime bootstrap`
    verifies it connects.
    """
    nologin = rd.RoleLoginState(role="okg_mcp", present=True, can_login=False)
    err = rd._role_cannot_login_error(
        (nologin,),
        _readiness(roles=(nologin,)),
        surface="okg mcp-serve",
        deployment="walkrole",
    )
    payload = _envelope(err)
    assert payload["code"] == "mcp_role_cannot_login"
    assert payload["data"]["roles_unusable"] == ["okg_mcp"]
    assert payload["data"]["roles"] == [
        {"role": "okg_mcp", "present": True, "rolcanlogin": False}
    ]
    assert "OKG_MCP_RO_DSN" in payload["fix_hint"]
    assert "okg runtime bootstrap --deployment walkrole" in payload["fix_hint"]
    assert "rolcanlogin = false" in payload["message"]


def test_role_refusal_hint_never_invents_a_password_or_an_alter() -> None:
    nologin = rd.RoleLoginState(role="okg_mcp", present=True, can_login=False)
    hint = rd._role_cannot_login_error(
        (nologin,), _readiness(roles=(nologin,)), surface="s", deployment=None
    ).fix_hint
    assert "ALTER ROLE" not in hint
    # No literal credential: the operator supplies one.
    assert "PASSWORD '" not in hint
    assert "--deployment <name>" in hint


def test_unreachable_refusal_names_host_and_port_and_omits_the_dsn() -> None:
    err = rd._unreachable_error(
        OSError("boom"),
        dsn="postgresql://someone:s3cret@db.example:6000/graph",
        surface="okg run --once",
    )
    payload = _envelope(err)
    assert payload["code"] == "database_unreachable"
    assert payload["data"]["host"] == "db.example"
    assert payload["data"]["port"] == "6000"
    # A refusal is operator-visible output; the DSN can carry a password.
    assert "s3cret" not in json.dumps(payload)


def test_no_readiness_hint_points_into_a_source_checkout() -> None:
    """A wheel user has no `ops/` directory (issue #2128, finding F7)."""
    nologin = rd.RoleLoginState(role="okg_mcp", present=True, can_login=False)
    hints = [
        rd._not_migrated_error(
            _readiness(schema_present=False), surface="s", deployment="d"
        ).fix_hint,
        rd._role_cannot_login_error(
            (nologin,), _readiness(roles=(nologin,)), surface="s", deployment="d"
        ).fix_hint,
        rd._unreachable_error(
            OSError("x"), dsn="postgresql://h/db", surface="s"
        ).fix_hint,
    ]
    for hint in hints:
        assert "ops/pg" not in hint
        assert "docker compose" not in hint


# ---------------------------------------------------------------------------
# The override supersedes the derived role, so the role is not checked.
# ---------------------------------------------------------------------------


def test_explicit_mcp_read_dsn_suppresses_the_role_check(monkeypatch) -> None:
    monkeypatch.setenv(rd.MCP_RO_DSN_ENV, "postgresql://reader@h/db")
    assert rd.mcp_read_roles_to_check() == ()


def test_without_an_override_the_default_read_role_is_checked(monkeypatch) -> None:
    monkeypatch.delenv(rd.MCP_RO_DSN_ENV, raising=False)
    assert rd.mcp_read_roles_to_check() == ("okg_mcp",)


# ---------------------------------------------------------------------------
# The ledger read needs a dict-row cursor. Measured: reading it through a
# default tuple cursor raised `AttributeError: 'list' object has no
# attribute 'get'` from inside the ledger scan.
# ---------------------------------------------------------------------------


def test_the_migration_ledger_is_read_through_a_dict_row_cursor() -> None:
    from psycopg.rows import dict_row

    requested: list[object] = []

    class _Cursor:
        def __init__(self, row_factory=None) -> None:
            self.row_factory = row_factory
            self._rows: list[object] = []

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql, params=None):
            if "information_schema.schemata" in sql:
                self._rows = [(1,)]
            elif "to_regclass" in sql:
                self._rows = [(None,)]
            else:
                self._rows = []

        def fetchone(self):
            return self._rows[0] if self._rows else None

    class _Conn:
        def cursor(self, row_factory=None):
            requested.append(row_factory)
            return _Cursor(row_factory)

    readiness = rd.read_database_readiness(_Conn())
    assert readiness.schema_present is True
    assert readiness.schema_applied is False
    assert dict_row in requested


# ---------------------------------------------------------------------------
# The extension refusal must also name a wheel-runnable command.
# ---------------------------------------------------------------------------


def test_missing_extension_hint_leads_with_a_command_a_wheel_user_can_run() -> None:
    from okg.substrate.db import migrate as migrate_mod

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql, params=None):
            self.rows = [
                (name, version)
                for name, version in migrate_mod.REQUIRED_EXTENSIONS.items()
                if name != "vector"
            ]

        def fetchall(self):
            return self.rows

    class _Conn:
        def cursor(self):
            return _Cursor()

    with pytest.raises(ExtensionMissing) as caught:
        migrate_mod._verify_extensions(_Conn())
    hint = caught.value.fix_hint
    assert "CREATE EXTENSION IF NOT EXISTS" in hint
    assert "psql" in hint
    # The compose route may still be offered, but only AFTER the runnable
    # command and only labelled as checkout-only.
    assert hint.index("psql") < hint.index("docker compose")
    assert "checkout only" in hint


# ---------------------------------------------------------------------------
# `okg jobs status` says whether its own verdict blocks anything (F16).
# ---------------------------------------------------------------------------


def test_every_jobs_status_verdict_has_a_plain_reading() -> None:
    from okg.substrate.cli import jobs

    assert set(jobs._STATUS_MEANINGS) == {
        "unavailable",
        "cleanup_required",
        "clean",
    }


def test_jobs_status_unavailable_says_it_does_not_block() -> None:
    from okg.substrate.cli import jobs

    meaning = jobs._status_meaning("unavailable")
    assert "does NOT block" in meaning
    assert "okg run --once --apply" in meaning


def test_an_unknown_jobs_status_is_reported_as_unknown_not_as_clean() -> None:
    from okg.substrate.cli import jobs

    assert "unrecognized" in jobs._status_meaning("something_new")


# ---------------------------------------------------------------------------
# The scaffold's next list names migration before the first run (F7).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("function_name", "marker"),
    [
        ("_cmd_init_from_profile", "next_commands.extend(["),
        ("_cmd_init_from_template", "next_commands = template_meta.get"),
    ],
)
def test_scaffold_next_list_orders_migrate_before_run_once(
    function_name: str, marker: str
) -> None:
    """Both scaffold paths must name migration before the first run.

    The scaffold writes files; it migrates the database only when the
    operator pinned a DSN at init time. Every other operator followed the
    printed next list straight into `okg run --once --apply` against an
    unmigrated database, which is exactly how finding F7 was produced.

    Asserted on the LIST LITERAL, not on the function body: the profile
    path already mentioned `okg migrate` inside an unrelated recovery
    fix_hint further up, so a whole-body search passes vacuously.
    """
    import inspect

    from okg.substrate.cli import init as init_mod

    source = inspect.getsource(getattr(init_mod, function_name))
    block = source[source.index(marker) :]
    block = block[: block.index("]")]
    assert "okg migrate" in block, block
    assert block.index("okg migrate") < block.index("okg run --once"), block


# ---------------------------------------------------------------------------
# The count and the list are ONE fact (review NOTE-1). Real `okg status
# --json` output on an empty database read
# `"pending_migration_count": 33, "pending_migrations": []`.
# ---------------------------------------------------------------------------


def _fake_conn():
    """A connection stub whose ledger scan is the one thing under test."""

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql, params=None):
            self._rows = [(1,)] if "information_schema" in sql else [(None,)]

        def fetchone(self):
            return self._rows[0] if self._rows else None

    class _Conn:
        def cursor(self, row_factory=None):
            return _Cursor()

    return _Conn()


@pytest.mark.parametrize("ledger_exists", [True, False])
def test_the_pending_count_always_matches_the_pending_list(
    monkeypatch, ledger_exists: bool
) -> None:
    """One fact, two keys: they may never disagree.

    Suppressing the identifiers without suppressing the count produced a
    payload that said "33 pending" beside an empty list. Every reporting
    surface derives its line from the count, so a count with nothing to
    name is a line the operator cannot act on.
    """
    from okg.substrate.db import management as mgmt

    monkeypatch.setattr(
        mgmt,
        "_migration_status_from_cursor",
        lambda cur: {
            "ledger_exists": ledger_exists,
            "pending": [{"migration_id": "0033_x"}, {"migration_id": "0034_y"}],
        },
    )
    readiness = rd.read_database_readiness(_fake_conn())
    payload = readiness.as_dict()
    assert payload["pending_migration_count"] == len(
        payload["pending_migrations"]
    )
    assert readiness.pending_migration_count == (2 if ledger_exists else 0)


# ---------------------------------------------------------------------------
# Reporting pending migrations -- the other half of the 2026-09-21
# decision. Both readiness call sites used to discard the result.
# ---------------------------------------------------------------------------


def test_nothing_is_reported_when_nothing_is_pending() -> None:
    ready = _readiness()
    assert rd.pending_migration_report(ready, deployment="d") is None
    assert rd.pending_migration_warning(ready, surface="s", deployment="d") is None


def test_the_pending_report_names_the_count_the_ids_and_the_command() -> None:
    ready = _readiness(
        pending_migrations=("0033_x",), pending_migration_count=1
    )
    block = rd.pending_migration_report(ready, deployment="walkpi")
    assert block["pending_migration_count"] == 1
    assert block["pending_migrations"] == ["0033_x"]
    assert block["migrate_command"] == "okg migrate --deployment walkpi --apply"
    line = rd.pending_migration_warning(
        ready, surface="okg run --once", deployment="walkpi"
    )
    assert line.startswith("okg run --once:")
    assert "behind by 1 migration" in line
    assert "0033_x" in line
    assert "okg migrate --deployment walkpi --apply" in line


def test_the_pending_line_counts_in_plural_when_there_is_more_than_one() -> None:
    ready = _readiness(
        pending_migrations=("0033_x", "0034_y"), pending_migration_count=2
    )
    line = rd.pending_migration_warning(ready, surface="s", deployment=None)
    assert "behind by 2 migrations" in line
    assert "0034_y" in line


# ---------------------------------------------------------------------------
# A privilege failure is not an outage (review BLOCKER-2).
# ---------------------------------------------------------------------------


def test_a_privilege_failure_is_not_reported_as_unreachable() -> None:
    import psycopg

    exc = psycopg.errors.InsufficientPrivilege(
        "permission denied for table schema_migrations"
    )
    err = rd._readiness_unreadable_error(
        exc,
        dsn="postgresql://reader:s3cret@db.example:6000/graph",
        surface="okg run --once",
    )
    payload = _envelope(err)
    assert payload["code"] == "database_readiness_unreadable"
    assert payload["data"]["role"] == "reader"
    assert payload["data"]["dbname"] == "graph"
    # The operator action is a grant, not restarting a server.
    assert "GRANT USAGE ON SCHEMA okg TO reader" in payload["fix_hint"]
    assert "start or reach the Postgres server" not in payload["fix_hint"]
    assert "privilege gap, not an outage" in payload["message"]
    assert "s3cret" not in json.dumps(payload)


def test_the_probe_classifies_insufficient_privilege_before_unreachable(
    monkeypatch,
) -> None:
    """The classification is in `check_database_readiness`, not in a caller.

    Measured on a real database: a login role holding the substrate's own
    `app_ro` on a fully MIGRATED database was refused with
    `database_unreachable` and told to start Postgres.
    """
    import psycopg

    from okg.substrate.db import connection as conn_mod

    def _raise(*args, **kwargs):
        raise psycopg.errors.InsufficientPrivilege(
            "permission denied for table schema_migrations"
        )

    monkeypatch.setattr(conn_mod, "connect", _raise)
    with pytest.raises(SubstrateError) as caught:
        rd.check_database_readiness(
            "postgresql://reader@h:5432/graph", surface="okg run --once"
        )
    assert caught.value.code == "database_readiness_unreadable"


# ---------------------------------------------------------------------------
# The OKG_MCP_RO_DSN override is PROBED, not trusted (review SHOULD-FIX-1).
# ---------------------------------------------------------------------------


def test_no_override_means_no_extra_connection(monkeypatch) -> None:
    monkeypatch.delenv(rd.MCP_RO_DSN_ENV, raising=False)

    def _never(*args, **kwargs):
        raise AssertionError("the override probe must not connect")

    from okg.substrate.db import connection as conn_mod

    monkeypatch.setattr(conn_mod, "connect", _never)
    assert rd.require_mcp_read_dsn_usable(surface="okg mcp-serve") is None


def test_a_wrong_override_dsn_is_refused_by_name(monkeypatch) -> None:
    """The escape hatch must be a WORKING credential, not a set variable.

    Before this, a non-empty `OKG_MCP_RO_DSN` suppressed the role check
    on its mere presence, so a DSN naming a NOLOGIN role restored the
    pre-fix behaviour: exit 0, "starting anyway", every read refused.
    """
    import psycopg

    from okg.substrate.db import connection as conn_mod

    monkeypatch.setenv(
        rd.MCP_RO_DSN_ENV,
        "postgresql://okg_mcp:stale_wrong@127.0.0.1:6000/graph",
    )

    def _raise(*args, **kwargs):
        raise psycopg.OperationalError(
            'connection failed: FATAL:  role "okg_mcp" is not permitted '
            "to log in (stale_wrong)"
        )

    monkeypatch.setattr(conn_mod, "connect", _raise)
    with pytest.raises(SubstrateError) as caught:
        rd.require_mcp_read_dsn_usable(
            surface="okg mcp-serve", deployment="walkrole"
        )
    payload = _envelope(caught.value)
    assert payload["code"] == "mcp_read_dsn_unusable"
    assert payload["data"]["role"] == "okg_mcp"
    assert payload["data"]["env_var"] == "OKG_MCP_RO_DSN"
    assert "okg runtime bootstrap --deployment walkrole" in payload["fix_hint"]
    # A driver message can echo the connection string back.
    assert "stale_wrong" not in json.dumps(payload)
    assert "***" in payload["data"]["detail"]


# ---------------------------------------------------------------------------
# CLI wiring. These drive the real dispatcher with the readiness PROBE
# stubbed, so they need no database: what is under test is which
# question each surface asks and whether it passes the answer on.
# ---------------------------------------------------------------------------


#: A DSN that can never connect, so a surface that gets past the stubbed
#: readiness fails fast instead of waiting on a timeout.
_DEAD_DSN = "postgresql://nobody@127.0.0.1:1/nothing"


def _write_deployment(root, name: str, *, runtime: bool = False) -> None:
    target = root / name
    (target / "schemas").mkdir(parents=True)
    (target / "source_registry.yaml").write_text("sources: {}\n")
    body = (
        f"name: {name}\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "postgres:\n"
        f"  dsn: {_DEAD_DSN}\n"
    )
    if runtime:
        body += "runtime:\n  enabled: true\n"
    (target / "deployment.yaml").write_text(body)


def _pending_readiness() -> rd.DatabaseReadiness:
    return rd.DatabaseReadiness(
        schema_present=True,
        schema_applied=True,
        ledger_present=True,
        pending_migrations=("0033_external_automation_run_identity",),
        pending_migration_count=1,
    )


def test_mcp_serve_asks_whether_the_derived_read_role_can_log_in(
    tmp_path, monkeypatch, capsys
) -> None:
    """Mutant M6: `roles=()` at the call site left every test green.

    The role half is stated about `okg mcp-serve`, but the only case
    that exercised it called `require_database_ready` directly, so
    nothing proved the command asks about `okg_mcp` at all.
    """
    from okg.substrate.cli import main as cli_main

    seen: dict = {}

    def _record(dsn, **kwargs):
        seen.update(kwargs)
        err = SubstrateError("stubbed refusal")
        err.code = "stubbed"
        raise err

    monkeypatch.delenv(rd.MCP_RO_DSN_ENV, raising=False)
    monkeypatch.setattr(rd, "require_database_ready", _record)
    root = tmp_path / "deployments"
    root.mkdir()
    _write_deployment(root, "rolewire")
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    rc = cli_main.main(["--json", "mcp-serve", "--deployment", "rolewire"])
    capsys.readouterr()

    assert rc == 1
    assert seen["roles"] == ("okg_mcp",)
    assert seen["surface"] == "okg mcp-serve"


@pytest.mark.parametrize(
    ("argv", "surface", "runtime"),
    [
        (["run", "--once", "--apply"], "okg run --once", False),
        (["mcp-serve"], "okg mcp-serve", False),
        (["runtime", "worker"], "okg runtime worker", True),
    ],
)
def test_every_readiness_surface_reports_pending_migrations(
    tmp_path, monkeypatch, capsys, argv, surface: str, runtime: bool
) -> None:
    """The 2026-09-21 decision, at each call site.

    Both original call sites discarded the returned readiness, so a
    database running behind its ledger was reported nowhere, in text or
    in JSON. Each surface must say it, and say which command fixes it.
    What the command does AFTER the report is not this case's business.
    """
    import contextlib

    from okg.substrate.cli import main as cli_main

    monkeypatch.delenv(rd.MCP_RO_DSN_ENV, raising=False)
    monkeypatch.setattr(
        rd, "require_database_ready", lambda dsn, **kw: _pending_readiness()
    )
    root = tmp_path / "deployments"
    root.mkdir()
    _write_deployment(root, "pendwire", runtime=runtime)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    with contextlib.suppress(BaseException):
        cli_main.main([*argv, "--deployment", "pendwire"])
    captured = capsys.readouterr()

    assert f"{surface}: this database is behind by 1 migration" in captured.err
    assert "0033_external_automation_run_identity" in captured.err
    assert "okg migrate --deployment pendwire --apply" in captured.err


def test_the_run_once_json_payload_carries_the_pending_block() -> None:
    """The report travels in the machine-readable payload too.

    Asserted on the shared emitter rather than on a full publish cycle:
    `okg run --once` reaches its payload only after a real publish.
    """
    from okg.substrate.cli import main as cli_main

    block = cli_main._report_pending_migrations(
        _pending_readiness(), surface="okg run --once", deployment="pendwire"
    )
    assert block["pending_migration_count"] == 1
    assert block["pending_migrations"] == [
        "0033_external_automation_run_identity"
    ]
    assert block["migrate_command"] == "okg migrate --deployment pendwire --apply"
    assert (
        cli_main._report_pending_migrations(
            _readiness(), surface="okg run --once", deployment="pendwire"
        )
        is None
    )


# ---------------------------------------------------------------------------
# Issue #3311: indexes `okg migrate --apply` builds through ensure_schema are
# pending work too. The ledger does not list them, so a database read
# "0 pending" while grep refused every call.
# ---------------------------------------------------------------------------


def _missing(name="idx_x_node_text_trgm_visible", effect="refused",
             required_by='search(method="grep")', state="absent"):
    return rd.MissingSchemaObject(
        name=name, table="okg.graph_nodes", state=state,
        required_by=required_by, effect=effect,
    )


def test_a_missing_tool_index_is_pending_work_with_no_migration_behind() -> None:
    ready = _readiness(missing_schema_objects=(_missing(),))
    assert ready.pending_migration_count == 0
    assert ready.pending_work_count == 1
    payload = ready.as_dict()
    assert payload["pending_schema_object_count"] == 1
    assert payload["pending_work_count"] == 1
    assert payload["pending_schema_objects"] == [{
        "index": "idx_x_node_text_trgm_visible",
        "table": "okg.graph_nodes",
        "state": "absent",
        "required_by": 'search(method="grep")',
        "effect": "refused",
    }]
    block = rd.pending_migration_report(ready, deployment="cms")
    assert block is not None
    assert block["migrate_command"] == "okg migrate --deployment cms --apply"


def test_the_warning_names_the_index_the_tool_and_the_command() -> None:
    ready = _readiness(missing_schema_objects=(
        _missing(),
        _missing(name="idx_x_node_path_prefix_visible", effect="sequential_scan",
                 required_by='search(method="grep") path_prefix'),
    ))
    line = rd.pending_migration_warning(ready, surface="okg status", deployment="cms")
    assert line.startswith(
        "okg status: 2 indexes that `okg migrate --apply` builds are missing or invalid"
    )
    assert "idx_x_node_text_trgm_visible" in line
    assert 'search(method="grep") will refuse every call' in line
    assert "path_prefix will scan instead of using an index" in line
    assert "okg migrate --deployment cms --apply" in line
    # never the "one migration behind is a working state" reassurance
    assert "working state" not in line


def test_the_warning_says_both_when_migrations_and_indexes_are_pending() -> None:
    ready = _readiness(
        pending_migrations=("0041_blob_store",), pending_migration_count=1,
        missing_schema_objects=(_missing(),),
    )
    line = rd.pending_migration_warning(ready, surface="s", deployment=None)
    assert "behind by 1 migration (0041_blob_store), and 1 index" in line
    assert ready.pending_work_count == 2


def test_tool_index_names_come_from_the_spec_ensure_schema_builds() -> None:
    from okg.substrate.graph_store_internal.common import (
        EDGE_TABLE, NODE_TABLE, chronos_interval_index_name,
    )
    from okg.substrate.graph_store_internal.schema import tool_index_names

    tables = {
        NODE_TABLE: {"physical_table": "_chronos_n1", "columns": ("node_id", "attrs")},
        EDGE_TABLE: {"physical_table": "_chronos_e1", "columns": ("src", "dst")},
    }
    names = tool_index_names(tables)
    suffixes = [spec.suffix for _name, _table, spec in names]
    assert suffixes == [
        "node_text_bm25_visible", "node_text_trgm_visible",
        "node_path_prefix_visible", "node_path_trgm_visible",
    ]
    assert all(table == NODE_TABLE for _name, table, _spec in names)
    assert names[1][0] == chronos_interval_index_name("_chronos_n1", "node_text_trgm_visible")
    # a node table without attrs gets none, as ensure_schema skips it
    tables[NODE_TABLE]["columns"] = ("node_id",)
    assert tool_index_names(tables) == []


def _index_cursor(registered: bool, present: dict[str, bool]):
    """A dict-row cursor answering the three readiness index queries."""
    from okg.substrate.graph_store_internal.common import NODE_TABLE

    class _Cursor:
        def __init__(self):
            self._rows = []

        def execute(self, sql, params=None):
            if "to_regclass('public._chronos_branch_tables')" in sql:
                self._rows = [{"reg": "x" if registered else None}]
            elif "FROM public._chronos_branch_tables" in sql:
                self._rows = [{
                    "table_name": NODE_TABLE, "physical_table": "_chronos_n1",
                    "columns": '["node_id", "attrs"]',
                }]
            elif "pg_index" in sql:
                self._rows = [
                    {"name": n, "valid": v} for n, v in present.items() if n in params[0]
                ]
            else:
                raise AssertionError(sql)

        def fetchone(self):
            return self._rows[0] if self._rows else None

        def fetchall(self):
            return list(self._rows)

    return _Cursor()


def test_absent_and_invalid_tool_indexes_are_both_missing() -> None:
    from okg.substrate.graph_store_internal.common import chronos_interval_index_name

    def name(suffix):
        return chronos_interval_index_name("_chronos_n1", suffix)

    present = {
        name("node_text_bm25_visible"): True,
        name("node_text_trgm_visible"): False,  # a failed concurrent build
        name("node_path_prefix_visible"): True,
    }
    missing = rd._missing_tool_indexes(_index_cursor(True, present))
    # An invalid index is still in pg_indexes, so the tool's guard accepts it
    # and the planner scans: it scans, it does not refuse.
    assert [(m.name, m.state, m.effect) for m in missing] == [
        (name("node_text_trgm_visible"), "invalid", "sequential_scan"),
        (name("node_path_trgm_visible"), "absent", "refused"),
    ]
    # non-vacuity: with every index present and valid nothing is missing
    every = {n: True for n in [*present, name("node_path_trgm_visible")]}
    assert rd._missing_tool_indexes(_index_cursor(True, every)) == ()
    # a database whose graph tables are not registered yet expects none
    assert rd._missing_tool_indexes(_index_cursor(False, {})) == ()


def test_an_invalid_index_hint_drops_it_before_migrate() -> None:
    """`ensure_schema` uses CREATE INDEX IF NOT EXISTS, which keeps an
    invalid index, so "run okg migrate" alone would never clear it."""
    ready = _readiness(missing_schema_objects=(
        _missing(state="invalid", effect="sequential_scan"),
    ))
    line = rd.pending_migration_warning(ready, surface="s", deployment="cms")
    assert 'First drop the invalid index (DROP INDEX public."idx_x_node_text_trgm_visible"), then run' in line
    assert "okg migrate --deployment cms --apply" in line
    # an absent one needs no drop
    absent = rd.pending_migration_warning(
        _readiness(missing_schema_objects=(_missing(),)), surface="s", deployment="cms",
    )
    assert "DROP INDEX" not in absent


def test_a_sealed_release_still_reports_missing_tool_indexes(capsys) -> None:
    """Review finding: with a declared release, `okg mcp-serve` and the
    runtime worker print only the release verdict, which is built from the
    migration ledger, so a sealed deployment missing grep's index started
    silently -- #3311's own scenario."""
    from okg.substrate.cli import main as cli_main

    ready = _readiness(
        pending_migrations=("0041_blob_store",), pending_migration_count=1,
        missing_schema_objects=(_missing(),),
    )
    cli_main._report_database_behind(
        ready, {"verdict": "serve", "verdict_message": "release note"},
        surface="okg mcp-serve", deployment="cms",
    )
    err = capsys.readouterr().err
    assert "note: okg mcp-serve: release note" in err
    assert 'search(method="grep") will refuse every call' in err
    # the verdict already speaks for the migrations; they are not repeated
    assert "0041_blob_store" not in err
    # non-vacuity: with nothing missing, only the verdict is printed
    cli_main._report_database_behind(
        _readiness(), {"verdict": "serve", "verdict_message": "release note"},
        surface="okg mcp-serve", deployment="cms",
    )
    assert "warning" not in capsys.readouterr().err
