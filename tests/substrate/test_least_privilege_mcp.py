"""Least-privilege MCP connection model (PACT least-privilege-mcp).

The MCP read pool connects as the non-superuser okg_mcp role; writes are
operator-gated (off by default, agent cannot self-grant) and run over a
separate app_rw pool; okg doctor flags a superuser runtime role.
"""
from __future__ import annotations

import json
from pathlib import Path

import psycopg
import pytest

pytest.importorskip("mcp")

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.server import _derive_role_dsn, build_server
from tests.substrate.conftest import alter_role_under_cluster_lock


postgres = pytest.mark.postgres


# ---------------------------------------------------------------------------
# Unit: role-DSN derivation
# ---------------------------------------------------------------------------


def test_derive_role_dsn_rewrites_user_and_password(monkeypatch, tmp_path):
    # The built-in password is derived only behind the single-user dev flag
    # (issue #3273); test_local_database_roles.py pins the refusal without it.
    monkeypatch.delenv("OKG_MCP_RO_DSN", raising=False)
    monkeypatch.setenv("OKG_ALLOW_BUILTIN_ROLE_PASSWORDS", "1")
    monkeypatch.setenv("OKG_ROLE_SECRETS_FILE", str(tmp_path / "absent.json"))
    dsn = _derive_role_dsn(
        "postgres://postgres:secret@localhost:5433/okg_x",
        role="okg_mcp",
        env_var="OKG_MCP_RO_DSN",
    )
    info = psycopg.conninfo.conninfo_to_dict(dsn)
    assert info["user"] == "okg_mcp"
    assert info["password"] == "okg_mcp"
    assert info["dbname"] == "okg_x"
    assert info["host"] == "localhost"
    assert "secret" not in dsn


def test_derive_role_dsn_env_override_wins(monkeypatch):
    monkeypatch.setenv("OKG_MCP_RO_DSN", "postgres://managed/ro")
    dsn = _derive_role_dsn(
        "postgres://postgres:secret@localhost/okg",
        role="okg_mcp",
        env_var="OKG_MCP_RO_DSN",
    )
    assert dsn == "postgres://managed/ro"


def test_derive_role_dsn_requires_override_for_remote_hosts(monkeypatch):
    monkeypatch.delenv("OKG_MCP_RO_DSN", raising=False)
    with pytest.raises(SubstrateError) as exc_info:
        _derive_role_dsn(
            "postgres://postgres:secret@db.internal.example/okg",
            role="okg_mcp",
            env_var="OKG_MCP_RO_DSN",
        )
    assert exc_info.value.code == "substrate_error"
    assert exc_info.value.data["env_var"] == "OKG_MCP_RO_DSN"
    assert "OKG_MCP_RO_DSN" in exc_info.value.fix_hint


def test_runtime_register_translates_extra_env_for_write_mode():
    """The agent-runtime entry builder translates extra_env, so a
    profile whose setup is agent-driven registers a write-enabled MCP
    server without persisting DSNs or write capability as ambient env."""
    from okg.substrate.library.agent_runtimes.claude_code import (
        ClaudeCodeRuntime,
    )
    from okg.substrate.library.agent_runtimes.codex_cli import (
        CodexCliRuntime,
    )

    ro_dsn = "postgres://managed/ro"
    rw_dsn = "postgres://managed/rw"

    entry = ClaudeCodeRuntime()._build_entry(
        deployment="d", dsn="postgres://x/d",
        extra_env={
            "OKG_MCP_ALLOW_WRITES": "1",
            "OKG_MCP_RO_DSN": ro_dsn,
            "OKG_MCP_RW_DSN": rw_dsn,
        },
    )
    assert "OKG_DSN" not in entry["env"]
    assert "OKG_MCP_ALLOW_WRITES" not in entry["env"]
    assert entry["env"]["OKG_AGENT_RUNTIME_PROFILE"] == "deploy"
    assert entry["env"]["OKG_MCP_RO_DSN"] == ro_dsn
    assert entry["env"]["OKG_MCP_RW_DSN"] == rw_dsn
    separator = entry["args"].index("--")
    child = entry["args"][separator + 1:]
    assert "mcp-serve" in child
    assert "--allow-writes" in child

    # Default (no extra_env) stays read-only with no write flag in env/args.
    ro_entry = ClaudeCodeRuntime()._build_entry(
        deployment="d", dsn="postgres://x/d",
        extra_env={
            "OKG_MCP_RO_DSN": ro_dsn,
            "OKG_MCP_RW_DSN": rw_dsn,
        },
    )
    assert ro_entry["env"]["OKG_AGENT_RUNTIME_PROFILE"] == "read-only"
    assert "OKG_MCP_ALLOW_WRITES" not in ro_entry["env"]
    assert ro_entry["env"]["OKG_MCP_RO_DSN"] == ro_dsn
    assert "OKG_MCP_RW_DSN" not in ro_entry["env"]
    ro_child = ro_entry["args"][ro_entry["args"].index("--") + 1:]
    assert "--allow-writes" not in ro_child

    # codex builds the entry inline via _upsert; verify translation there too.
    root: dict = {}
    CodexCliRuntime()._upsert(
        root, "okg-d", deployment="d", dsn="postgres://x/d",
        extra_env={
            "OKG_MCP_ALLOW_WRITES": "1",
            "OKG_MCP_RO_DSN": ro_dsn,
            "OKG_MCP_RW_DSN": rw_dsn,
        },
    )
    codex_entry = root["mcp_servers"]["okg-d"]
    assert "OKG_DSN" not in codex_entry["env"]
    assert "OKG_MCP_ALLOW_WRITES" not in codex_entry["env"]
    assert codex_entry["env"]["OKG_MCP_RO_DSN"] == ro_dsn
    assert codex_entry["env"]["OKG_MCP_RW_DSN"] == rw_dsn
    codex_child = codex_entry["args"][codex_entry["args"].index("--") + 1:]
    assert "--allow-writes" in codex_child


# ---------------------------------------------------------------------------
# Postgres: role posture, pool split, write gate, doctor
# ---------------------------------------------------------------------------

def _mcp_dsn(owner_dsn: str) -> str:
    # The owner DSN with okg_mcp's credentials swapped in, so a socket host
    # (`?host=/path`) survives as well as a TCP one.
    return psycopg.conninfo.make_conninfo(
        owner_dsn, user="okg_mcp", password="okg_mcp",
    )


def _seed_published(dsn: str) -> str:
    """Checkpoint one ``doc:lp`` node for deployment ``lp``.

    The checkpoint names an applied catalog version that declares
    ``document``. Without one, every session read of the generation is
    refused with ``GenerationPinInvalid`` (issue #2248). This stays a direct
    checkpoint rather than a ``Publisher`` cycle on purpose: these tests
    prove role grants on the tables the owner connection creates, and a
    publish would add catalog-ownership and invariant work they do not
    exercise.
    """
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from tests.pytest.catalog_fixture_builders import seed_catalog

    with psycopg.connect(dsn, autocommit=True) as conn:
        catalog_version_id = seed_catalog(conn, subtypes=["document"])
    store = ChronosGraphStore.connect(dsn, ensure_schema=True)
    try:
        store.apply_facts(
            [NodeFact("doc:lp", "document", {"text": "lp"}, {}, {})],
            branch="default", source="lp.src",
        )
        return store.checkpoint_generation(
            deployment_name="lp",
            branch="default",
            catalog_version_id=catalog_version_id,
        )
    finally:
        store.close()


@postgres
def test_okg_mcp_role_is_least_privilege(clean_runtime: str):
    """okg_mcp: not a superuser; reads named graph/catalog surfaces;
    cannot read sensitive raw tables, write fact tables, or run DDL."""
    _seed_published(clean_runtime)
    with psycopg.connect(_mcp_dsn(clean_runtime), autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT rolsuper FROM pg_roles WHERE rolname = current_user"
            )
            assert cur.fetchone()[0] is False
            cur.execute("SELECT count(*) FROM okg.graph_nodes")  # read OK
            cur.fetchone()
            cur.execute("SELECT count(*) FROM okg.catalog_versions")  # read OK
            cur.fetchone()
            cur.execute("SELECT count(*) FROM okg.schema_migrations")  # read OK
            cur.fetchone()
            for relation in (
                "okg.nomos_policy_bundles",
                "okg.nomos_policy_activations",
                "okg.nomos_identity_providers",
                "okg.nomos_subject_bindings",
                "okg.nomos_permission_grants",
            ):
                cur.execute(f"SELECT count(*) FROM {relation}")
                cur.fetchone()
            for relation in (
                "okg.node_facts",
                "okg.edge_facts",
                "okg.nomos_config_sets",
                "okg.poros_outbox_messages",
                "okg.federation_mailbox_messages",
                "okg.source_cursor_blobs",
            ):
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    cur.execute(f"SELECT count(*) FROM {relation}")
            conn.rollback()
        # Chronos checkout read works as okg_mcp.
        from okg.substrate.graph_store import ChronosGraphStore

        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        rows = store.checkout("default").query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id = 'doc:lp'"
        )
        assert [r["node_id"] for r in rows] == ["doc:lp"]
        # Fact-table write denied.
        with conn.cursor() as cur:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                cur.execute(
                    "INSERT INTO okg.node_facts (node_id, subtype, attrs, "
                    "source, source_record_id, source_revision, observed_at, "
                    "dedupe_key, run_id, op) VALUES ('x','y','{}','s','{}',"
                    "'{}',now(),'k',gen_random_uuid(),'I')"
                )
            conn.rollback()
        # DDL denied.
        with conn.cursor() as cur:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                cur.execute("DROP TABLE okg.graph_nodes")
            conn.rollback()

    with psycopg.connect(clean_runtime, autocommit=True) as owner:
        with owner.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS public.mcp_default_leak")
            cur.execute("CREATE TABLE public.mcp_default_leak (id int)")
            cur.execute(
                "SELECT has_table_privilege("
                "'okg_mcp', 'public.mcp_default_leak', 'SELECT')"
            )
            assert cur.fetchone()[0] is False
            cur.execute("DROP TABLE public.mcp_default_leak")


@postgres
def test_run_sql_set_role_app_ro_downgrade_works_as_okg_mcp(clean_runtime: str):
    """run_sql's `SET LOCAL ROLE app_ro` must resolve from okg_mcp
    (okg_mcp is granted app_ro)."""
    _seed_published(clean_runtime)
    with psycopg.connect(_mcp_dsn(clean_runtime), autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("BEGIN")
            cur.execute("SET LOCAL ROLE app_ro")
            cur.execute("SELECT current_user")
            assert cur.fetchone()[0] == "app_ro"
            cur.execute("ROLLBACK")


@postgres
def test_source_admin_read_paths_survive_hidden_fact_logs(
    clean_runtime: str,
    monkeypatch,
    tmp_path: Path,
):
    """MCP source-admin reads must not require raw fact-log grants.

    The okg_mcp role intentionally cannot SELECT okg.node_facts/edge_facts.
    list_sources/source_status should keep returning source envelopes with
    unavailable count authority rather than failing the read-path tool.
    """
    from okg.substrate.mcp import source_admin, source_lifecycle

    _seed_published(clean_runtime)
    registry_path = tmp_path / "source_registry.yaml"
    registry = {
        "sources": {
            "lp.src": {
                "ownership_id": "lp.src",
                "enabled_by_default": True,
            }
        }
    }
    registry_path.write_text("sources:\n  lp.src:\n    ownership_id: lp.src\n")

    def _fake_registry(deployment: str):  # noqa: ARG001
        return registry_path, registry

    monkeypatch.setattr(source_admin, "_load_registry_yaml", _fake_registry)
    monkeypatch.setattr(source_lifecycle, "_load_registry_yaml", _fake_registry)
    monkeypatch.setattr(
        source_lifecycle,
        "_source_sync_status_for_source",
        lambda *args, **kwargs: {
            "schema": "okg.source_sync.status.v1",
            "deployment": "lp",
            "source_name": "lp.src",
            "status": "unavailable",
        },
    )

    with psycopg.connect(_mcp_dsn(clean_runtime)) as conn:
        listed = source_lifecycle.list_sources(conn, "lp")
        assert listed["status"] == "ok"
        assert listed["sources"][0]["source_id"] == "lp.src"
        assert listed["sources"][0]["count_authority"] == "unavailable"

        status = source_lifecycle.source_status(conn, "lp", "lp.src")
        assert status["status"] == "ok"
        assert status["source_id"] == "lp.src"
        assert status["count_authority"] == "unavailable"


@postgres
def test_read_pool_is_okg_mcp_and_writes_off_by_default(clean_runtime: str):
    gen = _seed_published(clean_runtime)
    mcp = build_server(dsn=clean_runtime, deployment_name="lp")
    try:
        assert mcp._okg_write_pool is None
        assert mcp._okg_allow_writes is False
        with mcp._okg_pool.connection() as c, c.cursor() as cur:
            cur.execute(
                "SELECT rolsuper FROM pg_roles WHERE rolname = current_user"
            )
            assert cur.fetchone()[0] is False
        # A read still works through the server's session machinery.
        from okg.substrate.mcp import open_session
        from okg.substrate.mcp.run_sql import run_sql

        with mcp._okg_pool.connection() as c:
            sess = open_session(c, deployment_name="lp")
            res = run_sql(
                c, sess,
                "SELECT node_id FROM okg.v_nodes WHERE node_id='doc:lp'",
            )
            assert res["rows"] == [["doc:lp"]]
        del gen
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


@postgres
def test_write_admin_tools_absent_when_write_mode_off(clean_runtime: str):
    """A default server does not register write/admin tools; an agent cannot
    reach them by forging authority fields.
    """
    _seed_published(clean_runtime)
    mcp = build_server(dsn=clean_runtime, deployment_name="lp")
    try:
        for tool in (
            "record",
            "propose",
            "add_source",
            "remove_source",
            "commit_setup",
            "connector_ingest_batch",
        ):
            assert tool not in mcp._tool_manager._tools
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


@postgres
def test_write_mode_on_uses_separate_app_rw_pool(clean_runtime: str):
    _seed_published(clean_runtime)
    mcp = build_server(dsn=clean_runtime, deployment_name="lp", allow_writes=True)
    try:
        assert mcp._okg_write_pool is not None
        with mcp._okg_write_pool.connection() as c, c.cursor() as cur:
            cur.execute("SELECT current_user")
            assert cur.fetchone()[0] == "app_rw"
        # Read pool stays okg_mcp.
        with mcp._okg_pool.connection() as c, c.cursor() as cur:
            cur.execute("SELECT current_user")
            assert cur.fetchone()[0] == "okg_mcp"
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


@postgres
def test_env_allow_writes_enables_write_mode(clean_runtime, monkeypatch):
    _seed_published(clean_runtime)
    monkeypatch.setenv("OKG_MCP_ALLOW_WRITES", "1")
    mcp = build_server(dsn=clean_runtime, deployment_name="lp")
    try:
        assert mcp._okg_allow_writes is True
        assert mcp._okg_write_pool is not None
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


@postgres
def test_doctor_flags_superuser_runtime_role(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
):
    """role_has_superuser fires when okg_mcp is a superuser, and clears
    when it is NOSUPERUSER."""
    import argparse
    import okg.substrate.cli.doctor as dmod

    # _cmd_doctor emits through the `_emit_ok` bound on the doctor module,
    # which `_load_main_helpers()` fills once per process and then leaves
    # alone. Load it, then patch that binding with monkeypatch so it is
    # restored at teardown. Rebinding it by hand without restoring it left
    # every later in-process `okg --json doctor` in the same pytest worker
    # writing nothing to stdout (the substrate-db-full nightly failures in
    # test_catalog_modules_load and test_embedding_config_e2e).
    dmod._load_main_helpers()

    def _run() -> dict:
        captured = {}

        def _capture(payload, *, as_json):  # noqa: ANN001
            captured["payload"] = payload
            return 0

        monkeypatch.setattr(dmod, "_emit_ok", _capture)
        args = argparse.Namespace(
            dsn=clean_runtime, deployment=None, check=None,
        )
        dmod._cmd_doctor(args, as_json=True)
        return captured["payload"]

    # Baseline: okg_mcp is NOSUPERUSER → no role_has_superuser error.
    payload = _run()
    assert payload["role_superuser"]["okg_mcp"] is False
    assert not [
        e for e in payload["errors"] if e.get("code") == "role_has_superuser"
    ]

    # Make okg_mcp a superuser → doctor must flag it.
    # okg_mcp is one cluster-wide role: alter it under the role lock every
    # other writer takes, or a concurrent ALTER fails one of them (#1985).
    alter_role_under_cluster_lock(
        clean_runtime,
        psycopg.sql.SQL("ALTER ROLE okg_mcp SUPERUSER"),
        lock_role="okg_mcp",
    )
    try:
        payload = _run()
        assert payload["role_superuser"]["okg_mcp"] is True
        errs = [
            e for e in payload["errors"]
            if e.get("code") == "role_has_superuser" and e.get("role") == "okg_mcp"
        ]
        assert errs and "fix_hint" in errs[0]
        assert payload["ok"] is False
    finally:
        alter_role_under_cluster_lock(
            clean_runtime,
            psycopg.sql.SQL("ALTER ROLE okg_mcp NOSUPERUSER"),
            lock_role="okg_mcp",
        )


@postgres
def test_doctor_json_reaches_stdout_after_the_capture_test(
    clean_runtime: str, capsys: pytest.CaptureFixture[str],
):
    """`okg --json doctor` still prints its report after the test above.

    Runs after test_doctor_flags_superuser_runtime_role in file order, so
    in the same process (xdist --dist=loadscope keeps a module on one
    worker). If that test leaves the doctor module's `_emit_ok` patched,
    this call writes nothing to stdout. Run alone, it proves nothing.
    """
    from okg.substrate.cli.main import main

    main(["--json", "doctor", "--dsn", clean_runtime])
    lines = capsys.readouterr().out.strip().splitlines()
    assert lines, (
        "okg --json doctor wrote nothing to stdout: a previous test left "
        "okg.substrate.cli.doctor._emit_ok patched"
    )
    # Only the leak is checked: the report reaches stdout as JSON. Whether
    # the doctor finds this database healthy is not this test's business.
    assert isinstance(json.loads(lines[-1]), dict)


_GRANT_SCHEMAS = ("public", "okg")


def _app_rw_privileges(cur) -> tuple[set, set] | None:
    """app_rw's explicit relation and schema privileges in public and okg.

    Returns None when the role does not exist yet.
    """
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'app_rw'")
    if cur.fetchone() is None:
        return None
    cur.execute(
        "SELECT n.nspname, c.relname, c.relkind, a.privilege_type, a.is_grantable "
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace, "
        "LATERAL aclexplode(c.relacl) a "
        "WHERE a.grantee = 'app_rw'::regrole AND n.nspname = ANY(%s)",
        (list(_GRANT_SCHEMAS),),
    )
    relations = set(cur.fetchall())
    cur.execute(
        "SELECT n.nspname, a.privilege_type, a.is_grantable "
        "FROM pg_namespace n, LATERAL aclexplode(n.nspacl) a "
        "WHERE a.grantee = 'app_rw'::regrole AND n.nspname = ANY(%s)",
        (list(_GRANT_SCHEMAS),),
    )
    schemas = set(cur.fetchall())
    return relations, schemas


def _restore_app_rw_privileges(cur, snapshot: tuple[set, set] | None) -> None:
    """Revoke app_rw's relation and schema privileges, then re-grant a snapshot."""
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'app_rw'")
    if cur.fetchone() is None:
        return
    for schema in _GRANT_SCHEMAS:
        cur.execute(f"REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM app_rw")
        cur.execute(f"REVOKE ALL ON ALL SEQUENCES IN SCHEMA {schema} FROM app_rw")
        cur.execute(f"REVOKE ALL ON SCHEMA {schema} FROM app_rw")
    if snapshot is None:
        return
    relations, schemas = snapshot
    for schema, privilege, grantable in sorted(schemas):
        option = " WITH GRANT OPTION" if grantable else ""
        cur.execute(f'GRANT {privilege} ON SCHEMA "{schema}" TO app_rw{option}')
    for schema, name, relkind, privilege, grantable in sorted(relations):
        cur.execute("SELECT to_regclass(%s)", (f'"{schema}"."{name}"',))
        if cur.fetchone()[0] is None:
            continue
        kind = "SEQUENCE" if relkind == "S" else "TABLE"
        option = " WITH GRANT OPTION" if grantable else ""
        cur.execute(
            f'GRANT {privilege} ON {kind} "{schema}"."{name}" TO app_rw{option}'
        )


@pytest.fixture
def _writer_chronos_tables_back_to_owner(clean_runtime: str):
    """Return the database to its pre-test shape after the writer test.

    The test below drops every Chronos table and lets app_rw recreate them,
    so they stay app_rw-owned in this worker's database after it ends.
    clean_runtime truncates rows but keeps tables, so a later test that
    needs owner-owned default-branch tables then fails, for example
    test_merge_app_rw_refusal.py in the same promoted-db worker (#2612).
    Its DROP ... CASCADE also removes views that depend on those tables,
    such as okg.v_generations (test_legacy_graph_retirement.py expects it),
    so migrate runs again afterwards to recreate them. The test also grants
    app_rw write access on every table in public and okg; those grants are
    put back to what app_rw held before the test, or else
    tests/substrate/db/test_sql_assertion_runner.py later finds app_rw
    allowed to write catalog tables such as okg.subtypes.
    """
    from okg.substrate.db import migrate as _migrate_mod

    with psycopg.connect(clean_runtime, autocommit=True) as owner:
        with owner.cursor() as cur:
            before = _app_rw_privileges(cur)
    yield
    with psycopg.connect(clean_runtime, autocommit=True) as owner:
        with owner.cursor() as cur:
            cur.execute(
                "SELECT schemaname, tablename FROM pg_tables "
                "WHERE tablename LIKE '\\_chronos%' AND tableowner = 'app_rw'"
            )
            for schema, table in cur.fetchall():
                cur.execute(
                    f'ALTER TABLE "{schema}"."{table}" OWNER TO CURRENT_USER'
                )
    original = dict(_migrate_mod.REQUIRED_EXTENSIONS)
    _migrate_mod.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    try:
        _migrate_mod.migrate(clean_runtime)
    finally:
        _migrate_mod.REQUIRED_EXTENSIONS.clear()
        _migrate_mod.REQUIRED_EXTENSIONS.update(original)
    with psycopg.connect(clean_runtime, autocommit=True) as owner:
        with owner.cursor() as cur:
            _restore_app_rw_privileges(cur, before)


@postgres
def test_writer_created_chronos_tables_readable_by_okg_mcp(
    clean_runtime: str, _writer_chronos_tables_back_to_owner: None,
):
    """P0 regression (2026-06-11): Chronos branch tables created at
    RUNTIME by a writer role (app_rw / okg_substrate) were invisible to
    okg_mcp, because default privileges are per-creating-role and
    views.sql's grants only covered owner-created tables. Every MCP
    graph read on a fresh role-split deployment failed with
    "permission denied for _chronos_branch_tables"."""
    from okg.substrate.db import migrate as _migrate_mod
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    info = psycopg.conninfo.conninfo_to_dict(clean_runtime)
    writer_dsn = "postgres://app_rw:okg_rw@{host}:{port}/{db}".format(
        host=info.get("host", "localhost"),
        port=info.get("port", "5432"),
        db=info["dbname"],
    )

    with psycopg.connect(clean_runtime, autocommit=True) as owner:
        with owner.cursor() as cur:
            # Writer role as the academic-researcher initdb creates it.
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'app_rw'")
            if cur.fetchone() is None:
                cur.execute("CREATE ROLE app_rw LOGIN PASSWORD 'okg_rw'")
            # The default branch's tables are instantiated by migrate
            # (postgres-owned); writer-created tables only appear when a
            # NEW branch is first written, which is what the publish on
            # the dedicated branch below exercises.

    # Re-run migrate so views.sql's FOR ROLE app_rw default-privileges
    # block executes now that the role exists (same pg_cron handling as
    # the okg_migrated fixture).
    original = dict(_migrate_mod.REQUIRED_EXTENSIONS)
    _migrate_mod.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    try:
        _migrate_mod.migrate(clean_runtime)
    finally:
        _migrate_mod.REQUIRED_EXTENSIONS.clear()
        _migrate_mod.REQUIRED_EXTENSIONS.update(original)

    # Writer grants AFTER migrate, matching the real ordering (initdb /
    # role_split grant the writer once the schema exists). Then drop ALL
    # Chronos state so the writer's first publish runs the lazy ensure
    # path and owns every chronos table — the fresh-deployment scenario
    # in which the 2026-06-11 sessions hit permission denied.
    with psycopg.connect(clean_runtime, autocommit=True) as owner:
        with owner.cursor() as cur:
            cur.execute(
                "SELECT schemaname, tablename FROM pg_tables "
                "WHERE tablename LIKE '\\_chronos%'"
            )
            for schema, table in cur.fetchall():
                cur.execute(
                    f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE'
                )
            for schema in ("public", "okg"):
                cur.execute(
                    f"GRANT USAGE, CREATE ON SCHEMA {schema} TO app_rw"
                )
                cur.execute(
                    "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES "
                    f"IN SCHEMA {schema} TO app_rw"
                )
                cur.execute(
                    "GRANT USAGE, SELECT ON ALL SEQUENCES "
                    f"IN SCHEMA {schema} TO app_rw"
                )

    # The raw DROP above bypasses Chronos' register path, which is what normally
    # invalidates the process-level registry/schema/grant caches. perf-reimpl
    # caches the table registry for production (where the schema is stable per
    # process), so without an explicit invalidation `self.tables` still lists the
    # just-dropped tables and register_table() below would index a physical table
    # that no longer exists. Drop all three caches here — the same contract the
    # conftest autouse fixture applies around every test, re-applied because this
    # test mutates chronos schema state mid-body.
    from chronos_core.branching._common import clear_all_registry_caches
    from okg.substrate.graph_store_internal.common import clear_schema_ensured_cache
    from okg.substrate.graph_store_internal.schema import _clear_mcp_grant_guard

    clear_all_registry_caches()
    clear_schema_ensured_cache()
    _clear_mcp_grant_guard()

    # Publish AS THE WRITER ROLE. ensure_schema=False because app_rw must NOT run
    # the okg-schema bootstrap (CREATE SCHEMA okg needs database-level CREATE, an
    # owner privilege the writer lacks by design); the okg schema already exists,
    # only chronos state was dropped above. Instead the writer provisions just the
    # Chronos metadata (root branch/binding tables live in public, which app_rw
    # CAN create), so every chronos table is created app_rw-owned — the fresh-
    # deployment worker scenario. perf-reimpl no longer creates this lazily on the
    # first branching op, so provision it explicitly here, on the writer's clean
    # connection (a fresh snapshot, so create_branch's FOR UPDATE sees seeded main).
    store = ChronosGraphStore.connect(writer_dsn, ensure_schema=False)
    try:
        store.context.ensure_metadata()
        store.context.register_table("okg.graph_nodes", ["node_id"])
        store.context.register_table("okg.graph_edges", ["edge_id"])
        store.ensure_branch("default")
        store.apply_facts(
            [NodeFact("doc:rw", "document", {"text": "rw"}, {}, {})],
            branch="default", source="rw.src",
        )
        store.checkpoint_generation(deployment_name="rw", branch="default")
    finally:
        store.close()

    # Test-validity guard: at least one Chronos table must really be
    # writer-owned, otherwise this test would pass for the accidental
    # canonical reason (owner-created tables were always covered).
    with psycopg.connect(clean_runtime, autocommit=True) as owner:
        with owner.cursor() as cur:
            cur.execute(
                "SELECT tablename FROM pg_tables "
                "WHERE tablename LIKE '\\_chronos%' "
                "AND tableowner = 'app_rw'"
            )
            writer_tables = [row[0] for row in cur.fetchall()]
            assert writer_tables, (
                "writer publish created no app_rw-owned chronos tables; "
                "test setup no longer exercises the regression"
            )

    # The actual regression: okg_mcp must be able to read the
    # writer-created tables and resolve a Chronos checkout over them.
    with psycopg.connect(_mcp_dsn(clean_runtime), autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM _chronos_branch_tables")
            assert cur.fetchone()[0] >= 1
            for table in writer_tables:
                cur.execute(f'SELECT count(*) FROM "{table}"')
                cur.fetchone()
        mcp_store = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        )
        rows = mcp_store.checkout("default").query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id = 'doc:rw'"
        )
        assert [r["node_id"] for r in rows] == ["doc:rw"]


# ---------------------------------------------------------------------------
# Server-owned admission mode: a caller-asserted fast_track must not reach the
# admission policy builder on ANY submit path (record + propose). This is the
# end-to-end proof that the prior helper-only test (adversarial-review finding)
# missed: it drives the actual tools through to the workflow boundary.
# ---------------------------------------------------------------------------


def _capture_submitted_authority(monkeypatch):
    """Patch the admission submit workflow to record the authority it receives
    (the value AFTER _submit_admission_payload's central sanitization) instead
    of running the real workflow + DB writes."""
    captured: dict = {}

    def _fake_workflow(payload, producer_id, producer_kind, intent,
                       idempotency_key, authority, check_summary):
        captured["authority"] = authority
        return {"status": "pending", "admission_id": "adm:test"}

    monkeypatch.setattr(
        "okg.substrate.runtime.admission_workflows.admission_submit_workflow",
        lambda: _fake_workflow,
    )
    return captured


@postgres
def test_record_tool_downgrades_caller_fast_track(clean_runtime, monkeypatch):
    _seed_published(clean_runtime)
    captured = _capture_submitted_authority(monkeypatch)
    mcp = build_server(dsn=clean_runtime, deployment_name="lp", allow_writes=True)
    try:
        record = mcp._tool_manager._tools["record"].fn
        result = record(
            kind="observation",
            payload={"summary": "Closure agrees within tolerance."},
            authority={"producer_policy": {"admission_mode": "fast_track"}},
        )
        # The submit reached the workflow (not refused before it) and the
        # caller's fast_track was downgraded to review server-side.
        assert "authority" in captured, f"record never submitted: {result}"
        assert (
            captured["authority"]["producer_policy"]["admission_mode"] == "review"
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


@postgres
def test_propose_tool_downgrades_caller_fast_track(clean_runtime, monkeypatch):
    _seed_published(clean_runtime)
    captured = _capture_submitted_authority(monkeypatch)
    mcp = build_server(dsn=clean_runtime, deployment_name="lp", allow_writes=True)
    try:
        propose = mcp._tool_manager._tools["propose"].fn
        result = propose(
            action="submit",
            payload={"intent": "review_proposal", "nodes": [], "edges": []},
            authority={"admission_mode": "fast_track"},
        )
        assert "authority" in captured, f"propose never submitted: {result}"
        assert captured["authority"]["admission_mode"] == "review"
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


# ---------------------------------------------------------------------------
# Issue #3273: random role passwords, the secrets file, the doctor warning
# ---------------------------------------------------------------------------
#
# okg_mcp and app_rw are one row each on the shared test server, and other
# workers log in as them with the built-in password, so these tests never
# rotate them. The rotation itself runs on a private role here, and end to end
# on a disposable server in the PACT evidence.


@postgres
def test_set_role_passwords_stores_only_a_scram_verifier(
    clean_runtime: str, record_property,
):
    """The new password logs in, the old one does not, and the server holds a
    SCRAM verifier -- the ALTER statement never carried the password."""
    import secrets as _secrets
    import uuid

    from okg.substrate.local_database_roles import set_role_passwords

    role = f"t3273_{uuid.uuid4().hex[:12]}"
    old_password = _secrets.token_urlsafe(16)
    new_password = _secrets.token_urlsafe(32)
    alter_role_under_cluster_lock(
        clean_runtime,
        psycopg.sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
            psycopg.sql.Identifier(role), psycopg.sql.Literal(old_password),
        ),
        lock_role=None,
    )
    sent: list[str] = []

    class _Recording:
        """Proxy that records every statement set_role_passwords sends."""

        def __init__(self, conn):
            self._conn = conn

        def __getattr__(self, name):
            return getattr(self._conn, name)

        def execute(self, query, *args, **kwargs):
            text = query if isinstance(query, str) else query.as_string(self._conn)
            sent.append(text)
            return self._conn.execute(query, *args, **kwargs)

    try:
        with psycopg.connect(clean_runtime) as conn:
            set_role_passwords(_Recording(conn), {role: new_password})
        alters = [q for q in sent if q.startswith("ALTER ROLE")]
        assert len(alters) == 1
        # The statement itself carries the verifier, never the password, so
        # no statement log can record the password.
        assert new_password not in alters[0]
        assert "'SCRAM-SHA-256$" in alters[0]
        info = psycopg.conninfo.conninfo_to_dict(clean_runtime)

        def _login(password: str) -> bool:
            dsn = psycopg.conninfo.make_conninfo(
                clean_runtime, user=role, password=password,
            )
            try:
                with psycopg.connect(dsn, connect_timeout=5):
                    return True
            except psycopg.OperationalError:
                return False

        assert info.get("dbname")
        assert _login(new_password) is True
        with psycopg.connect(clean_runtime) as conn:
            stored = conn.execute(
                "SELECT rolpassword FROM pg_authid WHERE rolname = %s", (role,),
            ).fetchone()[0]
        assert stored.startswith("SCRAM-SHA-256$")
        assert new_password not in stored
        if _server_checks_passwords(clean_runtime, role):
            record_property("old_password_refused", "checked")
            assert _login(old_password) is False
        else:
            record_property("old_password_refused", _TRUST_REASON)
    finally:
        alter_role_under_cluster_lock(
            clean_runtime,
            psycopg.sql.SQL("DROP ROLE IF EXISTS {}").format(
                psycopg.sql.Identifier(role)
            ),
            lock_role=None,
        )


@postgres
def test_mcp_read_dsn_comes_from_the_secrets_file(
    okg_mcp_can_log_in: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    """Without the dev flag the read pool's DSN is the one recorded in the
    owner-only file, and it opens a real okg_mcp session."""
    from okg.substrate.local_database_roles import write_role_secrets
    from okg.substrate.mcp.server import _derive_role_dsn

    dsn = okg_mcp_can_log_in
    monkeypatch.delenv("OKG_MCP_RO_DSN", raising=False)
    monkeypatch.delenv("OKG_ALLOW_BUILTIN_ROLE_PASSWORDS", raising=False)
    monkeypatch.setenv("OKG_ROLE_SECRETS_FILE", str(tmp_path / "s.json"))
    with pytest.raises(SubstrateError) as caught:
        _derive_role_dsn(dsn, role="okg_mcp", env_var="OKG_MCP_RO_DSN")
    assert caught.value.code == "role_password_not_provisioned"

    write_role_secrets(dsn, {"okg_mcp": _mcp_dsn(dsn)})
    read_dsn = _derive_role_dsn(dsn, role="okg_mcp", env_var="OKG_MCP_RO_DSN")
    with psycopg.connect(read_dsn, connect_timeout=5) as conn:
        assert conn.execute("SELECT current_user").fetchone()[0] == "okg_mcp"


@postgres
def test_doctor_warns_on_a_builtin_role_password(
    okg_mcp_can_log_in: str, monkeypatch: pytest.MonkeyPatch,
):
    """okg_mcp holds its built-in password on the test server, so the doctor
    names it and the rotation command; the dev flag silences the warning."""
    import argparse
    import okg.substrate.cli.doctor as dmod

    dmod._load_main_helpers()

    def _run() -> dict:
        captured = {}

        def _capture(payload, *, as_json):  # noqa: ANN001
            captured["payload"] = payload
            return 0

        monkeypatch.setattr(dmod, "_emit_ok", _capture)
        args = argparse.Namespace(
            dsn=okg_mcp_can_log_in, deployment=None, check=None,
        )
        dmod._cmd_doctor(args, as_json=True)
        return captured["payload"]

    monkeypatch.delenv("OKG_ALLOW_BUILTIN_ROLE_PASSWORDS", raising=False)
    payload = _run()
    assert "okg_mcp" in payload["role_passwords"]["builtin_password_roles"]
    warnings = [
        w for w in payload.get("warnings", [])
        if w.get("role") == "okg_mcp"
        and w.get("code") in {"builtin_role_password", "role_password_not_checked"}
    ]
    assert warnings, payload.get("warnings")
    if warnings[0]["code"] == "builtin_role_password":
        assert "okg db role-passwords --apply" in warnings[0]["fix_hint"]

    monkeypatch.setenv("OKG_ALLOW_BUILTIN_ROLE_PASSWORDS", "1")
    payload = _run()
    assert payload["role_passwords"]["builtin_passwords_allowed"] is True
    assert not [
        w for w in payload.get("warnings", [])
        if w.get("code") == "builtin_role_password"
    ]


def _server_checks_passwords(owner_dsn: str, role: str) -> bool:
    """False when the server lets ``role`` in with a WRONG password.

    Such a server trusts this connection path (``trust`` in pg_hba.conf, as
    the official image used by CI's database does for 127.0.0.1), so no
    password, old or new, is ever refused. An assertion that an old password
    is refused can only run where this is True; on a trusting server it is
    recorded as not applicable (a JUnit property) instead. Every other
    assertion still runs.
    """
    import secrets as _secrets

    wrong = psycopg.conninfo.make_conninfo(
        owner_dsn, user=role, password=_secrets.token_urlsafe(24),
    )
    try:
        with psycopg.connect(wrong, connect_timeout=5):
            return False
    except psycopg.OperationalError:
        return True


_TRUST_REASON = (
    "not applicable: the server accepts any password for this role (trust "
    "authentication), so a replaced password cannot be refused"
)


def _old_password_is_refused(
    owner_dsn: str, role: str, old_dsn: str, record_property,
) -> None:
    """Assert ``old_dsn`` no longer logs in, where the server checks passwords."""
    if not _server_checks_passwords(owner_dsn, role):
        record_property("old_password_refused", _TRUST_REASON)
        return
    record_property("old_password_refused", "checked")
    with pytest.raises(psycopg.OperationalError):
        psycopg.connect(old_dsn, connect_timeout=5).close()


def _private_roles(clean_runtime: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Two throwaway roles standing in for okg_mcp (needs LOGIN) and app_ro
    (rotated only with LOGIN), so provisioning can run for real without
    touching the cluster-wide roles other workers log in as."""
    import uuid

    from okg.substrate import local_database_roles as roles

    suffix = uuid.uuid4().hex[:10]
    login_role, idle_role = f"t3273_login_{suffix}", f"t3273_idle_{suffix}"
    for role in (login_role, idle_role):
        alter_role_under_cluster_lock(
            clean_runtime,
            psycopg.sql.SQL("CREATE ROLE {} NOLOGIN").format(
                psycopg.sql.Identifier(role)
            ),
            lock_role=None,
        )
    monkeypatch.setattr(roles, "MANAGED_ROLES", (login_role, idle_role))
    monkeypatch.setattr(roles, "ROLE_DSN_ENV", {login_role: "T3273_UNSET_DSN"})
    monkeypatch.delenv("T3273_UNSET_DSN", raising=False)
    monkeypatch.setenv("OKG_ROLE_SECRETS_FILE", str(tmp_path / "rs" / "s.json"))
    return roles, login_role, idle_role


def _drop_roles(clean_runtime: str, *names: str) -> None:
    for role in names:
        alter_role_under_cluster_lock(
            clean_runtime,
            psycopg.sql.SQL("DROP ROLE IF EXISTS {}").format(
                psycopg.sql.Identifier(role)
            ),
            lock_role=None,
        )


@postgres
def test_provision_sets_a_random_login_and_records_it(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
    record_property,
):
    """`okg db role-passwords --apply` end to end on private roles: LOGIN is
    granted only to the role okg logs in as, the password is random and
    recorded 0600, the report carries no password, and the recorded DSN logs
    in."""
    import stat as _stat

    roles, login_role, idle_role = _private_roles(
        clean_runtime, monkeypatch, tmp_path,
    )
    try:
        report = roles.provision_role_passwords(clean_runtime)
        path = tmp_path / "rs" / "s.json"
        assert _stat.S_IMODE(path.stat().st_mode) == 0o600
        recorded = json.loads(path.read_text())["roles"]
        assert set(recorded) == {login_role}
        password = psycopg.conninfo.conninfo_to_dict(recorded[login_role])["password"]
        assert len(password) >= 40
        assert password not in json.dumps(report)
        assert report["roles"][login_role]["login_verified"] is True
        assert report["roles"][idle_role] == {"action": "skipped", "reason": "nologin"}
        with psycopg.connect(recorded[login_role], connect_timeout=5) as conn:
            assert conn.execute("SELECT current_user").fetchone()[0] == login_role
        # A second rotation replaces the password; the old one stops working.
        roles.provision_role_passwords(clean_runtime)
        again = json.loads(path.read_text())["roles"][login_role]
        assert again != recorded[login_role]
        _old_password_is_refused(
            clean_runtime, login_role, recorded[login_role], record_property,
        )
    finally:
        _drop_roles(clean_runtime, login_role, idle_role)


@postgres
def test_provision_failure_never_leaves_a_stale_entry(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    """Review S1: a failed login check drops the role's old entry, and an
    unsafe secrets directory is refused BEFORE any password changes."""
    import os as _os

    roles, login_role, idle_role = _private_roles(
        clean_runtime, monkeypatch, tmp_path,
    )
    try:
        roles.provision_role_passwords(clean_runtime)
        path = tmp_path / "rs" / "s.json"
        first = json.loads(path.read_text())["roles"][login_role]

        monkeypatch.setattr(roles, "_probe_login", lambda dsn: None)
        with pytest.raises(SubstrateError) as caught:
            roles.provision_role_passwords(clean_runtime)
        assert caught.value.code == "role_password_login_failed"
        assert login_role not in json.loads(path.read_text())["roles"]
        monkeypatch.undo()

        monkeypatch.setattr(roles, "MANAGED_ROLES", (login_role, idle_role))
        monkeypatch.setattr(roles, "ROLE_DSN_ENV", {login_role: "T3273_UNSET_DSN"})
        monkeypatch.setenv("OKG_ROLE_SECRETS_FILE", str(path))
        roles.provision_role_passwords(clean_runtime)
        working = json.loads(path.read_text())["roles"][login_role]
        assert working != first
        _os.chmod(path.parent, 0o777)
        try:
            with pytest.raises(SubstrateError) as caught:
                roles.provision_role_passwords(clean_runtime)
            assert "writable by other accounts" in str(caught.value)
        finally:
            _os.chmod(path.parent, 0o700)
        # Refused before the ALTER: the recorded password still logs in.
        with psycopg.connect(working, connect_timeout=5) as conn:
            assert conn.execute("SELECT current_user").fetchone()[0] == login_role
    finally:
        _drop_roles(clean_runtime, login_role, idle_role)
