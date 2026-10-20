"""Upgrade-path proof: a stored view CREATE OR REPLACE cannot rewrite.

Covers req.view-install-converges and
req.view-convergence-refuses-unowned-dependents.

The defect these tests pin: PostgreSQL's ``CREATE OR REPLACE VIEW`` may
only APPEND output columns. A database whose STORED view carries an
older column order therefore aborts the whole ``views.sql`` script -- and
with it ``migrate()`` -- with ``InvalidTableDefinition``. A fresh install
never shows it, so only a test that first builds the older stored shape
can see the bug at all.

Each fixture below builds that older shape by SQL on a disposable
database created from a real fresh install, which is the operator's
acceptance recipe with the old-checkout step replaced by the shape it
would have produced. Every convergence test asserts NON-VACUITY first:
it executes the real ``views.sql`` against the planted shape and requires
PostgreSQL to reject it, so a test that stopped reproducing the defect
fails instead of passing quietly.
"""
from __future__ import annotations

import importlib.util
import logging
import sys
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

import okg.substrate.db.migrate as migrate_module
from okg.substrate.db import view_convergence
from okg.substrate.errors import SubstrateError

REPO_ROOT = Path(__file__).resolve().parents[3]
CONVERGENCE_SCRIPT = REPO_ROOT / "scripts" / "check_migration_convergence.py"


def _load_convergence_module():
    """The disposable-database helpers, reused from the convergence lane.

    ``scripts/`` is not a package, so it is loaded by path -- the same
    pattern ``test_migration_convergence_lane.py`` uses.
    """
    spec = importlib.util.spec_from_file_location(
        "check_migration_convergence", CONVERGENCE_SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_migration_convergence"] = module
    spec.loader.exec_module(module)
    return module


conv = _load_convergence_module()


# ---------------------------------------------------------------------------
# Hermetic: which views the mechanism considers its own.
# ---------------------------------------------------------------------------


def test_declared_views_covers_views_sql_and_excludes_prose() -> None:
    declared = view_convergence.declared_views([migrate_module._views_sql()])

    assert ("okg", "v_admission_envelopes") in declared, (
        "the view that broke the upgrade path must be in the covered set"
    )
    # Generality, not one hand-guarded view: the whole read layer.
    assert len(declared) > 40
    assert ("okg", "v_nodes") in declared
    assert ("okg", "v_generations") in declared
    # Coverage spans the whole file, not just its first section: v_mcp_calls
    # is declared far below the app_ro grant block, after the mcp_call_log
    # table it reads.
    assert ("okg", "v_mcp_calls") in declared
    assert ("okg", "v_ir_programs") in declared

    # The scan is not an `okg.v_*` name filter in disguise. views.sql happens
    # to declare only v_-prefixed views today — okg.projectable_node_facts was
    # the last exception until `schema-canonical-read-path` deleted it — so the
    # property is asserted against a synthetic script rather than against a
    # coincidence of the current file.
    assert view_convergence.declared_views(
        ["CREATE OR REPLACE VIEW okg.projectable_node_facts AS SELECT 1;\n"]
    ) == [("okg", "projectable_node_facts")]

    # views.sql discusses this failure mode in comments. A comment is not
    # a declaration, and dropping a relation named by prose would be a
    # spectacular way to fail.
    schemas = {schema for schema, _ in declared}
    assert schemas == {"okg"}
    assert len(declared) == len(set(declared))
    assert not view_convergence.unqualified_view_declarations(
        [migrate_module._views_sql()]
    )


def test_only_the_frozen_view_shape_family_routes_to_convergence() -> None:
    """SQLSTATE 42P16 alone is too broad to key on.

    `migrate` hands anything this predicate accepts to the convergence
    pass INSTEAD of wrapping it with the deployment file that caused it,
    so a deployment author's broken CREATE TABLE (also 42P16) must not
    look like a frozen view shape.
    """
    frozen = [
        'cannot change name of view column "a" to "b"',
        'cannot change data type of view column "a" from text to integer',
        'cannot change collation of view column "a" from "C" to "en_US"',
        "cannot drop columns from view",
    ]
    for message in frozen:
        assert view_convergence.is_unreplaceable_view_error(
            psycopg.errors.InvalidTableDefinition(message)
        ), message

    assert not view_convergence.is_unreplaceable_view_error(
        psycopg.errors.InvalidTableDefinition(
            'multiple primary keys for table "t" are not allowed'
        )
    )
    assert not view_convergence.is_unreplaceable_view_error(
        psycopg.errors.UndefinedFunction("no such function")
    )


def test_declared_views_ignores_materialized_views() -> None:
    script = (
        "CREATE OR REPLACE VIEW okg.v_plain AS SELECT 1 AS a;\n"
        "CREATE MATERIALIZED VIEW okg.m_rows AS SELECT 1 AS a;\n"
        "-- CREATE OR REPLACE VIEW okg.v_commented AS SELECT 1 AS a;\n"
    )
    assert view_convergence.declared_views([script]) == [("okg", "v_plain")]


# ---------------------------------------------------------------------------
# PostgreSQL: the upgrade path itself.
# ---------------------------------------------------------------------------

#: `okg.v_admission_envelopes` as a pre-D2 database still stores it: the
#: retired BIGINT `target_generation_id` sits at stored column 11, ahead of
#: `idempotency_key`.
#:
#: This fixture was repointed when schema-identity-convergence (D2) landed.
#: It previously planted the shape from BEFORE `target_generation_id` was
#: added, which was then one column SHORT of canonical -- but D2 removes that
#: column, so that old plant became byte-identical to the new canonical view
#: and the non-vacuity probe correctly reported there was nothing to converge.
#: The hazard it pinned is real and now points the other way, so the plant
#: does too: a stored view still carrying the retired column cannot be reached
#: by CREATE OR REPLACE (which can neither rename column 11 nor drop the
#: trailing column), which is exactly the upgrade D2 itself creates.
#:
#: `target_generation_id` is synthesized rather than selected: migration 0023
#: has already dropped it from `okg.admission_envelopes`, so only the stored
#: VIEW can still carry it -- which is precisely the state being simulated.
_LEGACY_ADMISSION_VIEW = """
CREATE VIEW okg.v_admission_envelopes AS
SELECT
  admission_id::text AS admission_id,
  deployment_name,
  producer_id,
  producer_kind,
  intent,
  admission_mode,
  status,
  catalog_version_id,
  source,
  run_id::text AS run_id,
  NULL::bigint AS target_generation_id,
  idempotency_key,
  authority,
  check_summary,
  verdict,
  workflow_ref,
  created_at,
  updated_at,
  decided_at,
  published_at,
  target_generation_ref,
  NULLIF(btrim(target_generation_ref), '') AS canonical_generation_ref
FROM okg.admission_envelopes
"""

#: `okg.v_nodes` with its leading column renamed. Chosen because most of
#: the read layer selects from it, so converging it requires peeling
#: script-owned dependants first.
_LEGACY_NODES_VIEW = """
CREATE VIEW okg.v_nodes AS
SELECT node_id AS id, subtype, attrs
FROM okg.graph_nodes
"""


def _view_columns(dsn: str, view: str) -> list[tuple[str, str]]:
    """``(column_name, type)`` in stored ordinal order for one okg view."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.attname, format_type(a.atttypid, a.atttypmod)
              FROM pg_attribute a
              JOIN pg_class c ON c.oid = a.attrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'okg'
               AND c.relname = %s
               AND a.attnum > 0
               AND NOT a.attisdropped
             ORDER BY a.attnum
            """,
            (view,),
        )
        return [(row[0], row[1]) for row in cur.fetchall()]


def _all_view_columns(dsn: str) -> dict[str, list[tuple[str, str]]]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod)
              FROM pg_attribute a
              JOIN pg_class c ON c.oid = a.attrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'okg'
               AND c.relkind = 'v'
               AND a.attnum > 0
               AND NOT a.attisdropped
             ORDER BY c.relname, a.attnum
            """
        )
        shape: dict[str, list[tuple[str, str]]] = {}
        for relname, attname, attype in cur.fetchall():
            shape.setdefault(relname, []).append((attname, attype))
        return shape


def _readable_views(dsn: str) -> set[str]:
    """Views `app_ro` may SELECT from.

    Dropping and recreating a view drops its grants with it. The complete
    install boundary reapplies the shared registry, but "the columns came
    back" would not notice if it had not -- and a silently unreadable read
    layer is exactly the kind of damage this mechanism must not do.
    """
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.relname
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'okg'
               AND c.relkind = 'v'
               AND has_table_privilege('app_ro', c.oid, 'SELECT')
            """
        )
        return {row[0] for row in cur.fetchall()}


def _okg_mcp_readable_relations(dsn: str) -> set[str]:
    """The bounded table-like relation set the issue-1169 probe compares."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.nspname || '.' || c.relname
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname IN ('okg', 'public')
               AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
               AND has_table_privilege('okg_mcp', c.oid, 'SELECT')
             ORDER BY 1
            """
        )
        return {row[0] for row in cur.fetchall()}


def _plant_registered_legacy_view(dsn: str, view: str) -> str:
    """Plant a readable legacy shape, proving recreation strips its grant."""
    relation = f"okg.{view}"
    entry_ref = f"grant:app_ro:{relation}"
    legacy_sql = (
        f"CREATE VIEW {relation} AS "
        "SELECT status, admission_id FROM okg.admission_envelopes"
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(legacy_sql)
        conn.execute(
            """
            INSERT INTO okg.grant_registry
              (entry_ref, relation, object_kind, role_name, privileges, source)
            VALUES (%s, %s, 'TABLE', 'app_ro', 'SELECT', 'deployment')
            """,
            (entry_ref, relation),
        )
        conn.execute("SELECT okg.apply_grant_registry()")
        assert conn.execute(
            "SELECT has_table_privilege('okg_mcp', %s, 'SELECT')", (relation,)
        ).fetchone()[0]

        # PostgreSQL stores relation privileges on the object. Recreate the
        # exact same legacy shape and prove that destructive fact directly,
        # before applying the registry once to establish the migration baseline.
        conn.execute(f"DROP VIEW {relation}")
        conn.execute(legacy_sql)
        assert not conn.execute(
            "SELECT has_table_privilege('okg_mcp', %s, 'SELECT')", (relation,)
        ).fetchone()[0]
        conn.execute("SELECT okg.apply_grant_registry()")
        assert conn.execute(
            "SELECT has_table_privilege('okg_mcp', %s, 'SELECT')", (relation,)
        ).fetchone()[0]
    return entry_ref


def _drop_registered_probe(dsn: str, view: str, entry_ref: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"DROP VIEW IF EXISTS okg.{view}")
        conn.execute(
            "DELETE FROM okg.grant_registry WHERE entry_ref = %s", (entry_ref,)
        )


def _assert_views_sql_is_rejected(dsn: str) -> None:
    """Non-vacuity: the planted shape really does break the install.

    Run in a transaction that is always rolled back, so the probe cannot
    itself change the database under test.
    """
    with psycopg.connect(dsn) as conn:
        executable = migrate_module._executable_views_sql(
            conn, migrate_module._views_sql()
        )
        with conn.cursor() as cur:
            with pytest.raises(psycopg.errors.InvalidTableDefinition):
                cur.execute(executable)
        conn.rollback()


@pytest.fixture(scope="module")
def converged_dsn(okg_dsn: str) -> Iterator[str]:
    """A disposable database at the current fresh-install shape.

    Module-scoped because a fresh install is expensive and every test
    here ends by converging the database back to exactly this shape --
    which is the property under test, so reuse is self-checking rather
    than a shortcut.
    """
    dbname = f"okg_test_viewconv_{uuid4().hex[:12]}"
    try:
        dsn = conv.build_fresh_install(okg_dsn, dbname)
        yield dsn
    finally:
        conv.drop_database(okg_dsn, dbname)


@pytest.fixture(scope="module")
def fresh_view_shape(converged_dsn: str) -> dict[str, list[tuple[str, str]]]:
    return _all_view_columns(converged_dsn)


@pytest.fixture(scope="module")
def fresh_readable_views(converged_dsn: str) -> set[str]:
    return _readable_views(converged_dsn)


def _plant(dsn: str, *statements: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)


@pytest.mark.postgres
def test_nothing_outside_views_sql_depends_on_a_views_sql_view(
    converged_dsn: str,
) -> None:
    """Pin the condition that keeps the refusal path from firing wrongly.

    The restrictive drop can only peel dependants the scripts also
    declare. If a view defined in schema.sql (or by a runtime ensure)
    ever reads one of these, convergence would refuse on a database that
    is perfectly fine to converge -- and it would refuse for a reason no
    operator can act on, since they do not own either object. Catch that
    the day the dependency is introduced, not the day an upgrade breaks.
    """
    with psycopg.connect(converged_dsn) as conn:
        executable = migrate_module._executable_views_sql(
            conn, migrate_module._views_sql()
        )
    owned = {f"okg.{name}" for _, name in view_convergence.declared_views(
        [executable]
    )}

    with psycopg.connect(converged_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT
                   src_ns.nspname || '.' || src.relname,
                   dep_ns.nspname || '.' || dep.relname
              FROM pg_depend d
              JOIN pg_rewrite r ON d.objid = r.oid
              JOIN pg_class dep ON r.ev_class = dep.oid
              JOIN pg_class src ON d.refobjid = src.oid
              JOIN pg_namespace dep_ns ON dep.relnamespace = dep_ns.oid
              JOIN pg_namespace src_ns ON src.relnamespace = src_ns.oid
             WHERE src_ns.nspname = 'okg'
               AND src.relkind = 'v'
               AND dep.oid <> src.oid
            """
        )
        edges = cur.fetchall()

    foreign = sorted(
        f"{dependant} reads {source}"
        for source, dependant in edges
        if source in owned and dependant not in owned
    )
    assert not foreign, (
        "a relation outside views.sql now depends on a views.sql view; the "
        "restrictive drop can never peel it, so frozen-view-shape recovery "
        f"would refuse on healthy databases: {foreign}"
    )
    assert edges, "expected some view-to-view dependencies; probe is suspect"


@pytest.mark.postgres
def test_installing_views_twice_leaves_the_same_shape(
    converged_dsn: str,
    fresh_view_shape: dict[str, list[tuple[str, str]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ordinary path: idempotent, and it never enters convergence.

    A converged database must pay nothing for this mechanism -- no probe,
    no drop, no recreate. The monkeypatch turns "it silently dropped the
    read layer anyway" into a failure instead of an invisible cost.
    """

    def _must_not_run(*args: object, **kwargs: object) -> object:
        raise AssertionError(
            "convergence dropped views on an already-converged database"
        )

    monkeypatch.setattr(view_convergence, "_drop_declared_views", _must_not_run)

    migrate_module.install_views(converged_dsn)
    migrate_module.install_views(converged_dsn)

    assert _all_view_columns(converged_dsn) == fresh_view_shape


@pytest.mark.postgres
def test_migrate_converges_the_legacy_admission_view_order(
    converged_dsn: str,
    fresh_view_shape: dict[str, list[tuple[str, str]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The reported defect, end to end, on a real database."""
    _plant(
        converged_dsn,
        "DROP VIEW okg.v_admission_envelopes",
        _LEGACY_ADMISSION_VIEW,
    )
    planted = [name for name, _ in _view_columns(
        converged_dsn, "v_admission_envelopes"
    )]
    # The plant really is the pre-D2 shape: the retired column is stored at
    # position 11, and canonical no longer has it anywhere.
    assert planted[10] == "target_generation_id"
    assert "target_generation_id" not in [
        name for name, _ in fresh_view_shape["v_admission_envelopes"]
    ]
    _assert_views_sql_is_rejected(converged_dsn)

    with caplog.at_level(logging.INFO, logger="okg.substrate.db.view_convergence"):
        result = migrate_module.migrate(converged_dsn)

    assert (
        _view_columns(converged_dsn, "v_admission_envelopes")
        == fresh_view_shape["v_admission_envelopes"]
    )
    assert _all_view_columns(converged_dsn) == fresh_view_shape

    # Destructive replacement remains observable, but registered relation
    # grants are restored automatically at the complete install boundary.
    assert "okg.v_admission_envelopes" in result.converged_views
    log_output = "\n".join(
        record.getMessage()
        for record in caplog.records
        if record.levelno >= logging.INFO
    )
    assert "RE-APPLY BY HAND" not in log_output
    assert "okg.v_admission_envelopes" in log_output


@pytest.mark.postgres
def test_migrate_converges_a_view_its_own_dependants_read(
    converged_dsn: str,
    fresh_view_shape: dict[str, list[tuple[str, str]]],
    fresh_readable_views: set[str],
) -> None:
    """Generality: the divergent view is one the read layer sits on.

    Converging it is only possible by dropping the dependants first, so
    this is the proof that the mechanism peels in dependency order rather
    than handling one leaf view.
    """
    _plant(
        converged_dsn,
        "DROP VIEW okg.v_nodes CASCADE",
        _LEGACY_NODES_VIEW,
        # A script-owned dependant, restored on top of the divergent
        # view: convergence must drop this before it can drop v_nodes.
        "CREATE VIEW okg.v_subtypes AS SELECT subtype FROM okg.v_nodes",
    )
    assert _view_columns(converged_dsn, "v_nodes")[0][0] == "id"
    _assert_views_sql_is_rejected(converged_dsn)

    migrate_module.migrate(converged_dsn)

    assert _all_view_columns(converged_dsn) == fresh_view_shape
    assert _readable_views(converged_dsn) == fresh_readable_views


@pytest.mark.postgres
def test_convergence_refuses_a_dependant_views_sql_does_not_own(
    converged_dsn: str,
    fresh_view_shape: dict[str, list[tuple[str, str]]],
) -> None:
    """Never CASCADE, never skip: name the foreign object and stop."""
    _plant(
        converged_dsn,
        "DROP VIEW okg.v_admission_envelopes",
        _LEGACY_ADMISSION_VIEW,
        "CREATE VIEW okg.operator_admission_digest AS "
        "SELECT admission_id, status FROM okg.v_admission_envelopes",
    )

    with pytest.raises(SubstrateError) as raised:
        migrate_module.migrate(converged_dsn)

    message = str(raised.value)
    assert "okg.v_admission_envelopes" in message
    assert "okg.operator_admission_digest" in message
    blocked = raised.value.data["blocked_views"]
    assert "okg.operator_admission_digest" in blocked["okg.v_admission_envelopes"]

    # PostgreSQL's own DETAIL is reported, not just our catalog query --
    # the server is authoritative about what blocked the drop.
    detail = raised.value.data["postgres_dependency_detail"]
    assert "operator_admission_digest" in detail["okg.v_admission_envelopes"]
    assert "postgres said:" in message

    # The refusal left the operator's object alone.
    with psycopg.connect(converged_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('okg.operator_admission_digest')")
        assert cur.fetchone()[0] is not None

    # Removing the foreign dependant is all it takes to converge.
    _plant(converged_dsn, "DROP VIEW okg.operator_admission_digest")
    migrate_module.migrate(converged_dsn)
    assert _all_view_columns(converged_dsn) == fresh_view_shape


# ---------------------------------------------------------------------------
# The deployment-owned views.sql, which installs through the same path.
# ---------------------------------------------------------------------------


def _install(
    dsn: str,
    *,
    deployment_views_sql: str | None = None,
) -> list[str]:
    with psycopg.connect(dsn) as conn:
        executable = migrate_module._executable_views_sql(
            conn, migrate_module._views_sql()
        )
        with conn.cursor() as cur:
            dropped = migrate_module._install_views_sql(
                cur,
                executable,
                deployment_name="probe",
                deployment_views_sql=deployment_views_sql,
                deployment_views_path=Path("deployments/probe/views.sql"),
            )
        conn.commit()
    return dropped


@pytest.mark.postgres
def test_broken_deployment_views_still_name_their_file(
    converged_dsn: str,
    fresh_view_shape: dict[str, list[tuple[str, str]]],
) -> None:
    """Ordinary bad SQL in a deployment file keeps its own diagnosis.

    The convergence pass sits between the caller and that error now, so
    the message that tells an operator WHICH file to fix has to survive
    the trip.
    """
    with pytest.raises(SubstrateError) as raised:
        _install(converged_dsn, deployment_views_sql="SELECT no_such_function();")

    assert "deployment views.sql failed" in str(raised.value)
    assert "deployments/probe/views.sql" in str(raised.value)
    assert _all_view_columns(converged_dsn) == fresh_view_shape


@pytest.mark.postgres
def test_a_deployment_view_with_a_frozen_shape_converges_too(
    converged_dsn: str,
    fresh_view_shape: dict[str, list[tuple[str, str]]],
) -> None:
    """A deployment's own view gets the same convergence, not a wrap.

    Wrapping this failure as "your deployment SQL is broken" would be a
    wrong diagnosis AND would hide it from the pass that can fix it.
    """
    deployment_sql = (
        "CREATE OR REPLACE VIEW okg.v_probe_local AS "
        "SELECT admission_id, status FROM okg.admission_envelopes;"
    )
    _install(converged_dsn, deployment_views_sql=deployment_sql)

    # Reorder the stored deployment view so CREATE OR REPLACE is refused.
    _plant(
        converged_dsn,
        "DROP VIEW okg.v_probe_local",
        "CREATE VIEW okg.v_probe_local AS "
        "SELECT status, admission_id FROM okg.admission_envelopes",
    )

    dropped = _install(converged_dsn, deployment_views_sql=deployment_sql)

    assert "okg.v_probe_local" in dropped
    assert _view_columns(converged_dsn, "v_probe_local") == [
        ("admission_id", "uuid"),
        ("status", "text"),
    ]

    _plant(converged_dsn, "DROP VIEW okg.v_probe_local")
    assert _all_view_columns(converged_dsn) == fresh_view_shape


@pytest.mark.postgres
@pytest.mark.parametrize("entrypoint", ["migrate", "install_views"])
def test_registered_grants_survive_deployment_view_recreation(
    converged_dsn: str,
    fresh_view_shape: dict[str, list[tuple[str, str]]],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    entrypoint: str,
) -> None:
    """Issue #1169: a real destructive install has zero read-surface delta."""
    view = f"v_issue1169_{entrypoint}_{uuid4().hex[:8]}"
    relation = f"okg.{view}"
    canonical_sql = (
        f"CREATE OR REPLACE VIEW {relation} AS "
        "SELECT admission_id, status FROM okg.admission_envelopes;"
    )
    entry_ref = _plant_registered_legacy_view(converged_dsn, view)
    before = _okg_mcp_readable_relations(converged_dsn)
    assert relation in before
    monkeypatch.setattr(
        migrate_module,
        "_load_deployment_views",
        lambda _deployment: (
            canonical_sql,
            Path("deployments/issue1169/views.sql"),
        ),
    )

    try:
        with caplog.at_level(
            logging.INFO, logger="okg.substrate.db.view_convergence"
        ):
            result = getattr(migrate_module, entrypoint)(converged_dsn)

        after = _okg_mcp_readable_relations(converged_dsn)
        assert after == before
        assert len(after) - len(before) == 0
        assert relation in result.converged_views
        assert _view_columns(converged_dsn, view) == [
            ("admission_id", "uuid"),
            ("status", "text"),
        ]
        with psycopg.connect(converged_dsn) as conn:
            conn.execute("SET LOCAL ROLE okg_mcp")
            conn.execute(f"SELECT * FROM {relation} LIMIT 1").fetchall()
        with psycopg.connect(converged_dsn) as conn:
            missing = conn.execute(
                "SELECT relation FROM okg.grant_registry_drift() "
                "WHERE finding_kind = 'missing_grant'"
            ).fetchall()
            pending = conn.execute(
                "SELECT statement FROM okg.pending_grants"
            ).fetchall()
        assert missing == []
        assert pending == []
        assert "RE-APPLY BY HAND" not in "\n".join(
            record.getMessage() for record in caplog.records
        )
    finally:
        _drop_registered_probe(converged_dsn, view, entry_ref)
    assert _all_view_columns(converged_dsn) == fresh_view_shape


@pytest.mark.postgres
def test_new_registered_deployment_view_is_granted_after_complete_install(
    converged_dsn: str,
    fresh_view_shape: dict[str, list[tuple[str, str]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The final apply is unconditional, not shape-recovery-only."""
    view = f"v_issue1169_new_{uuid4().hex[:8]}"
    relation = f"okg.{view}"
    entry_ref = f"grant:app_ro:{relation}"
    canonical_sql = f"CREATE VIEW {relation} AS SELECT 1 AS probe;"
    with psycopg.connect(converged_dsn, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO okg.grant_registry
              (entry_ref, relation, object_kind, role_name, privileges, source)
            VALUES (%s, %s, 'TABLE', 'app_ro', 'SELECT', 'deployment')
            """,
            (entry_ref, relation),
        )
        assert conn.execute("SELECT to_regclass(%s)", (relation,)).fetchone()[0] is None
    monkeypatch.setattr(
        migrate_module,
        "_load_deployment_views",
        lambda _deployment: (
            canonical_sql,
            Path("deployments/issue1169/views.sql"),
        ),
    )

    try:
        result = migrate_module.install_views(converged_dsn)
        assert result.converged_views == ()
        with psycopg.connect(converged_dsn) as conn:
            assert conn.execute(
                "SELECT has_table_privilege('okg_mcp', %s, 'SELECT')",
                (relation,),
            ).fetchone()[0]
            conn.execute("SET LOCAL ROLE okg_mcp")
            assert conn.execute(f"SELECT probe FROM {relation}").fetchone()[0] == 1
    finally:
        _drop_registered_probe(converged_dsn, view, entry_ref)
    assert _all_view_columns(converged_dsn) == fresh_view_shape


@pytest.mark.postgres
def test_partial_registry_apply_rolls_back_view_recreation(
    converged_dsn: str,
    fresh_view_shape: dict[str, list[tuple[str, str]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A positive applier count cannot hide a queued/missing target grant."""
    view = f"v_issue1169_partial_{uuid4().hex[:8]}"
    relation = f"okg.{view}"
    canonical_sql = (
        f"CREATE OR REPLACE VIEW {relation} AS "
        "SELECT admission_id, status FROM okg.admission_envelopes;"
    )
    entry_ref = _plant_registered_legacy_view(converged_dsn, view)
    before = _okg_mcp_readable_relations(converged_dsn)
    forced_statement = f"GRANT SELECT ON TABLE {relation} TO app_ro"
    partial_applier = f"""
    CREATE OR REPLACE FUNCTION okg.apply_grant_registry()
    RETURNS integer LANGUAGE plpgsql AS $issue1169_partial$
    BEGIN
      INSERT INTO okg.pending_grants (statement, last_error)
      VALUES ({forced_statement!r}, 'issue1169 forced partial apply')
      ON CONFLICT (statement) DO UPDATE SET last_error = EXCLUDED.last_error;
      RETURN 1;
    END
    $issue1169_partial$;
    """
    monkeypatch.setattr(
        migrate_module,
        "_load_deployment_views",
        lambda _deployment: (
            canonical_sql + partial_applier,
            Path("deployments/issue1169/views.sql"),
        ),
    )

    try:
        with pytest.raises(SubstrateError) as raised:
            migrate_module.migrate(converged_dsn)
        assert "rolled back" in str(raised.value)
        assert [row["relation"] for row in raised.value.data["missing_grants"]] == [
            relation
        ]
        assert [row["statement"] for row in raised.value.data["pending_grants"]] == [
            forced_statement
        ]

        # The final reconciliation and the deployment's no-op applier lived in
        # the same transaction as the destructive replacement: all rolled back.
        assert _view_columns(converged_dsn, view) == [
            ("status", "text"),
            ("admission_id", "uuid"),
        ]
        assert _okg_mcp_readable_relations(converged_dsn) == before
        with psycopg.connect(converged_dsn) as conn:
            assert conn.execute(
                "SELECT count(*) FROM okg.pending_grants"
            ).fetchone()[0] == 0
    finally:
        _drop_registered_probe(converged_dsn, view, entry_ref)
    assert _all_view_columns(converged_dsn) == fresh_view_shape
