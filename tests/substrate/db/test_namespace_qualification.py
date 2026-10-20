"""The namespace contract, proved against a real database.

PACT ``schema-namespace-qualification``:
``req.schema-namespace-qualification.pact-tables-migrated`` and
``req.schema-namespace-qualification.enforcement`` (runtime arm).

Three things are proved here that a file scan cannot prove:

1. A fresh install puts the PACT ledger in ``okg``, and the documented audit
   query over ``pg_class`` returns no application relation in ``public``
   outside the enumerated exemption registry.
2. A pre-qualification deployment -- ledger in ``public``, with rows --
   migrates with ``ALTER TABLE ... SET SCHEMA`` and loses nothing: pre/post
   counts are equal per table. The ledger then round-trips **as okg_mcp**,
   not as the owner, after the full ``migrate()`` that re-runs views.sql's
   ``REVOKE ALL ON ALL TABLES IN SCHEMA okg FROM okg_mcp``. That REVOKE is
   the known cross-PACT trap: the ledger lives in ``okg`` now, so every
   migrate strips the writer grant, and something has to put it back.
3. A DDL-capturing event trigger records what a live session actually
   creates, and the ledger bootstrap is run underneath it. The static check
   reads source text; a runtime path can still assemble an unqualified name.
   Scope, stated plainly: the trigger is wired around the ledger ensure path
   and around hand-written probes, NOT around a whole `migrate()`, so it is
   a coverage arm for the forms the static scanner cannot see -- notably
   `CREATE TABLE AS` and `SELECT INTO` -- rather than a full audit of every
   runtime DDL site in the tree.

Every test provisions its own ``okg_test_*`` scratch database and drops it at
teardown -- no shared-database ordering effects.

Sessions here come from okg's own connection factory
(``okg.substrate.db.connection.connect``), which pins the session
``search_path`` to ``public`` (issue #1968). A raw ``psycopg.connect`` would
inherit PostgreSQL's default ``"$user", public``, so under a login role named
``okg`` -- a schema this migration creates -- unqualified names in these
tests would resolve into ``okg`` and the file would report failures that are
about the test harness rather than about the namespace contract (issue
#1973). The one place that genuinely needs an unqualified name to land in
``public`` is ``_seed_legacy_public_ledger``, and it says so explicitly.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from typing import Callable, Iterator
import uuid
from urllib.parse import urlsplit, urlunsplit

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.substrate.pact import decisions as ledger  # noqa: E402
from okg.substrate.db.connection import connect as db_connect  # noqa: E402


pytestmark = pytest.mark.postgres

_SCRATCH_EXTENSIONS = (
    "btree_gist", "timescaledb", "vector", "pg_textsearch", "pg_trgm",
    "fuzzystrmatch",
)

_LEDGER_RELATIONS = ("pact_decisions", "pact_intents", "pact_coordination_events")

ROOT = Path(__file__).resolve().parents[3]


def _load_namespace_checker():
    spec = importlib.util.spec_from_file_location(
        "check_schema_namespace",
        ROOT / "scripts" / "check_schema_namespace.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_schema_namespace"] = module
    spec.loader.exec_module(module)
    return module


NAMESPACE = _load_namespace_checker()


#: The audit query the namespace contract names. Every application relation
#: living in `public`, with extension-owned objects excluded via pg_depend --
#: an extension's tables are the extension's business, not ours.
NAMESPACE_AUDIT_SQL = """
SELECT c.relname
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'public'
   AND c.relkind IN ('r', 'p')
   AND NOT EXISTS (
       SELECT 1 FROM pg_depend d
        WHERE d.objid = c.oid AND d.deptype = 'e'
   )
 ORDER BY c.relname
"""


# --------------------------------------------------------------------------- #
# Scratch databases                                                            #
# --------------------------------------------------------------------------- #
def _swap_dbname(base_dsn: str, dbname: str) -> str:
    parts = urlsplit(base_dsn)
    return urlunsplit(parts._replace(path="/" + dbname))


def _create_scratch_db(
    base_dsn: str, dbname: str, *, extensions: bool = True
) -> str:
    """Mint a scratch database; install the substrate extensions only if asked.

    ``extensions=False`` matters for the lane budget, not for tidiness. Only
    the tests that call ``migrate()`` need timescaledb/vector/textsearch; the
    event-trigger and guard tests need an empty database and nothing else, and
    installing six extensions apiece is most of their wall time.
    """
    if not dbname.startswith("okg_test_"):  # match the ephemeral-DB guard
        raise AssertionError(f"unsafe scratch db name {dbname!r}")
    maint = _swap_dbname(base_dsn, "postgres")
    with db_connect(maint, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            " WHERE datname = %s AND pid <> pg_backend_pid()",
            (dbname,),
        )
        cur.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        cur.execute(f'CREATE DATABASE "{dbname}"')
    scratch = _swap_dbname(base_dsn, dbname)
    if extensions:
        with db_connect(
            scratch, autocommit=True
        ) as conn, conn.cursor() as cur:
            for ext in _SCRATCH_EXTENSIONS:
                cur.execute(f"CREATE EXTENSION IF NOT EXISTS {ext}")
    return scratch


def _drop_scratch_db(base_dsn: str, dbname: str) -> None:
    from okg.substrate.db.pool import close_all

    close_all()
    maint = _swap_dbname(base_dsn, "postgres")
    with db_connect(maint, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            " WHERE datname = %s AND pid <> pg_backend_pid()",
            (dbname,),
        )
        cur.execute(f'DROP DATABASE IF EXISTS "{dbname}"')


@pytest.fixture
def scratch_db(okg_dsn: str) -> Iterator[Callable[[], str]]:
    """Factory minting a fresh, uniquely named okg_test_* scratch database.

    Deliberately built on ``okg_dsn`` (the DSN string) rather than
    ``okg_migrated`` (the shared, session-migrated database): every test here
    provisions its own database from scratch, so depending on the shared one
    would import its state and its failures for no benefit.
    """
    base_dsn = okg_dsn
    try:
        with db_connect(
            _swap_dbname(base_dsn, "postgres"), connect_timeout=5
        ):
            pass
    except Exception as exc:  # noqa: BLE001 - no Postgres, nothing to prove
        pytest.skip(f"Postgres not reachable at {base_dsn}: {exc}")
    created: list[str] = []

    def _make(*, extensions: bool = True) -> str:
        dbname = f"okg_test_ns_{uuid.uuid4().hex[:10]}"
        created.append(dbname)
        return _create_scratch_db(base_dsn, dbname, extensions=extensions)

    try:
        yield _make
    finally:
        # Drop every database, then report what could not be dropped. A bare
        # `except: pass` here leaks okg_test_ns_* databases onto the dev
        # cluster silently, one per failed teardown, forever. Collecting and
        # re-raising keeps a mid-loop failure from stranding the rest AND
        # keeps the leak visible.
        leaked: list[str] = []
        for dbname in created:
            try:
                _drop_scratch_db(base_dsn, dbname)
            except Exception as exc:  # noqa: BLE001
                leaked.append(f"{dbname}: {exc}")
        if leaked:
            raise RuntimeError(
                "scratch databases were not dropped and are leaking on the "
                "test cluster: " + "; ".join(leaked)
            )


def _migrate(dsn: str) -> None:
    """Run the real migrate, minus the pg_cron extension requirement."""
    from okg.substrate.db import migrate as migrate_mod

    original = dict(migrate_mod.REQUIRED_EXTENSIONS)
    migrate_mod.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    try:
        migrate_mod.migrate(dsn)
    finally:
        migrate_mod.REQUIRED_EXTENSIONS.clear()
        migrate_mod.REQUIRED_EXTENSIONS.update(original)


def _relation_schema(cur, relation: str) -> str | None:
    cur.execute(
        "SELECT n.nspname FROM pg_class c "
        "  JOIN pg_namespace n ON n.oid = c.relnamespace "
        " WHERE c.relname = %s AND c.relkind = 'r'",
        (relation,),
    )
    row = cur.fetchone()
    return row[0] if row else None


#: The pre-qualification ledger, verbatim in shape: unqualified CREATEs that
#: search_path resolves into `public`. This is what every deployment
#: provisioned before this PACT is carrying right now.
#:
#: The unqualified spelling is the point, so `_seed_legacy_public_ledger`
#: states the search_path it needs instead of inheriting one. A session that
#: took PostgreSQL's default `"$user", public` under a login role named `okg`
#: would resolve these CREATEs into the `okg` schema and build the MIGRATED
#: shape while claiming to build the legacy one (issue #1973).
_LEGACY_PUBLIC_LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS pact_decisions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    change_id TEXT NOT NULL,
    verdict TEXT NOT NULL,
    note TEXT,
    target TEXT,
    principal TEXT NOT NULL,
    authority TEXT NOT NULL DEFAULT 'operator',
    intent_digest TEXT,
    from_state TEXT,
    to_state TEXT,
    decided_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS pact_decisions_change_idx
    ON pact_decisions (change_id, id);
CREATE TABLE IF NOT EXISTS pact_intents (
    change_id TEXT NOT NULL,
    worktree_suffix TEXT NOT NULL,
    worktree_path TEXT,
    branch TEXT,
    head_sha TEXT,
    session_id TEXT,
    intent_digest TEXT,
    derived_state TEXT,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_event_at TIMESTAMPTZ,
    PRIMARY KEY (change_id, worktree_suffix)
);
CREATE TABLE IF NOT EXISTS pact_coordination_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type TEXT NOT NULL,
    change_id TEXT NOT NULL,
    task_id TEXT,
    worktree_suffix TEXT,
    worktree_path TEXT,
    branch TEXT,
    head_sha TEXT,
    session_id TEXT,
    principal TEXT,
    intent_digest TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS pact_coordination_events_change_idx
    ON pact_coordination_events (change_id, id);
CREATE INDEX IF NOT EXISTS pact_coordination_events_worktree_idx
    ON pact_coordination_events (worktree_suffix, occurred_at DESC);
"""


def _seed_legacy_public_ledger(dsn: str) -> dict[str, int]:
    """Build a pre-qualification ledger with rows; return per-table counts.

    The DDL below is deliberately unqualified, so where it lands is decided by
    `search_path` and nothing else. This is the one place in this file that
    depends on that resolution, and it sets the path itself: `okg` exists by
    the next statement, and under a login role named `okg` an inherited
    default path would send the legacy tables into `okg` -- silently seeding
    the post-migration shape and proving nothing.
    """
    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SET search_path TO public")
        cur.execute("CREATE SCHEMA IF NOT EXISTS okg")
        cur.execute(_LEGACY_PUBLIC_LEDGER_DDL)
        for index in range(7):
            cur.execute(
                "INSERT INTO public.pact_decisions "
                "(change_id, verdict, principal, note) VALUES (%s,%s,%s,%s)",
                (f"change-{index}", "approve", "operator", f"note {index}"),
            )
        for index in range(3):
            cur.execute(
                "INSERT INTO public.pact_intents "
                "(change_id, worktree_suffix, branch) VALUES (%s,%s,%s)",
                (f"change-{index}", f"wt{index}", "dev"),
            )
        for index in range(5):
            cur.execute(
                "INSERT INTO public.pact_coordination_events "
                "(event_type, change_id) VALUES (%s,%s)",
                ("intent", f"change-{index}"),
            )
        conn.commit()
        counts = {}
        for relation in _LEDGER_RELATIONS:
            # Name the precondition rather than discovering it as a confusing
            # downstream failure: a legacy seed that is not in `public` is not
            # a legacy seed.
            assert _relation_schema(cur, relation) == "public", (
                f"{relation} was seeded outside public; the legacy "
                "reproduction did not happen"
            )
            cur.execute(f"SELECT count(*) FROM public.{relation}")
            counts[relation] = int(cur.fetchone()[0])
    return counts


# --------------------------------------------------------------------------- #
# req.pact-tables-migrated — fresh install                                     #
# --------------------------------------------------------------------------- #
def test_fresh_install_creates_the_pact_ledger_in_okg(scratch_db) -> None:
    dsn = scratch_db()
    _migrate(dsn)

    with db_connect(dsn) as conn, conn.cursor() as cur:
        for relation in _LEDGER_RELATIONS:
            assert _relation_schema(cur, relation) == "okg", relation
            cur.execute(f"SELECT to_regclass('public.{relation}')")
            assert cur.fetchone()[0] is None, (
                f"{relation} must not exist in public on a fresh install"
            )


def test_fresh_install_passes_the_documented_namespace_audit(
    scratch_db,
) -> None:
    dsn = scratch_db()
    _migrate(dsn)

    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(NAMESPACE_AUDIT_SQL)
        public_relations = [row[0] for row in cur.fetchall()]

    unexpected = [
        name
        for name in public_relations
        if not NAMESPACE.registry_relation_is_exempt(name)
    ]
    assert unexpected == [], (
        "application relations in public outside the exemption registry: "
        f"{unexpected}"
    )
    # The audit must have something to say -- chronos really does put its
    # physicals in public, and an empty result would mean the query stopped
    # looking rather than that the tree is clean.
    assert public_relations, "audit query found nothing at all; it is broken"


# --------------------------------------------------------------------------- #
# req.pact-tables-migrated — the move                                          #
# --------------------------------------------------------------------------- #
def test_set_schema_migration_moves_every_row(scratch_db) -> None:
    dsn = scratch_db()
    before = _seed_legacy_public_ledger(dsn)
    assert before == {
        "pact_decisions": 7,
        "pact_intents": 3,
        "pact_coordination_events": 5,
    }

    _migrate(dsn)

    with db_connect(dsn) as conn, conn.cursor() as cur:
        after = {}
        for relation in _LEDGER_RELATIONS:
            assert _relation_schema(cur, relation) == "okg", relation
            cur.execute(f"SELECT to_regclass('public.{relation}')")
            assert cur.fetchone()[0] is None
            cur.execute(f"SELECT count(*) FROM okg.{relation}")
            after[relation] = int(cur.fetchone()[0])

    assert after == before

    # The migration is recorded, not merely effective.
    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.schema_migrations "
            " WHERE migration_id = %s",
            ("0022_pact_ledger_schema_qualification",),
        )
        assert cur.fetchone()[0] == 1


def test_migration_sql_is_idempotent_and_refuses_ambiguity(scratch_db) -> None:
    """Execute the migration body directly, twice, and then into a conflict.

    The ordered runner records a migration and never re-runs it, so a full
    ``migrate()`` cannot tell an idempotent migration from one that merely got
    to run once. Run the SQL itself: twice in a row must be a no-op the second
    time, and the both-schemas state must raise rather than pick a copy.
    """
    migration = (
        ROOT / "src" / "okg" / "substrate" / "db" / "migrations"
        / "0022_pact_ledger_schema_qualification.sql"
    ).read_text(encoding="utf-8")

    dsn = scratch_db(extensions=False)
    before = _seed_legacy_public_ledger(dsn)

    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(migration)
        conn.commit()
        cur.execute(migration)  # second application: nothing left to move
        conn.commit()
        after = {}
        for relation in _LEDGER_RELATIONS:
            assert _relation_schema(cur, relation) == "okg", relation
            cur.execute(f"SELECT count(*) FROM okg.{relation}")
            after[relation] = int(cur.fetchone()[0])
    assert after == before

    # On a database with nothing in public it is a no-op, not an error.
    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(migration)
        conn.commit()

    # Ambiguity: a relation present in BOTH schemas must refuse, not guess.
    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("CREATE TABLE public.pact_decisions (id BIGINT)")
        conn.commit()
        with pytest.raises(psycopg.errors.DuplicateTable) as excinfo:
            cur.execute(migration)
        conn.rollback()
    assert "BOTH public and okg" in str(excinfo.value)


def test_adopted_ledger_converges_with_a_fresh_install(scratch_db) -> None:
    """The two paths must land on the same shape.

    ``migration-convergence-linux`` restores a pinned dump, migrates it, and
    demands an empty schema diff against a fresh install. A ledger that
    arrived by SET SCHEMA and one authored by the snapshot have to be
    indistinguishable afterwards, down to column order, types, indexes, and
    the identity sequence -- otherwise that lane goes red and the failure
    reads as a mystery rather than as this migration.
    """
    fresh = scratch_db()
    _migrate(fresh)

    adopted = scratch_db()
    _seed_legacy_public_ledger(adopted)
    _migrate(adopted)

    columns_sql = """
        SELECT table_name, ordinal_position, column_name, data_type,
               is_nullable, column_default, is_identity
          FROM information_schema.columns
         WHERE table_schema = 'okg' AND table_name LIKE 'pact\\_%'
         ORDER BY table_name, ordinal_position
    """
    indexes_sql = """
        SELECT tablename, indexname, indexdef
          FROM pg_indexes
         WHERE schemaname = 'okg' AND tablename LIKE 'pact\\_%'
         ORDER BY tablename, indexname
    """

    def _shape(dsn: str) -> tuple[list, list]:
        with db_connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(columns_sql)
            columns = cur.fetchall()
            cur.execute(indexes_sql)
            indexes = cur.fetchall()
        return columns, indexes

    fresh_columns, fresh_indexes = _shape(fresh)
    adopted_columns, adopted_indexes = _shape(adopted)

    assert fresh_columns, "the snapshot created no pact_* columns at all"
    assert adopted_columns == fresh_columns
    assert adopted_indexes == fresh_indexes


def test_migrated_ledger_round_trips_as_okg_mcp(scratch_db) -> None:
    """The acceptance clause, run as the constrained writer role.

    views.sql revokes okg_mcp on every table in `okg` on every migrate, and the
    ledger lives in `okg`, so that REVOKE reaches the writer grant.

    What changed (`schema-grant-coherence`): migrate now PUTS IT BACK, because
    okg.grant_registry carries the three ledger relations and
    okg.apply_grant_registry() runs at the end of the same file. The REVOKE and
    the re-GRANT are one script, in that order.

    That closes a real trap rather than tidying one. The repair below is
    PROBE-SKIPPED when the ledger objects already exist, which is the steady
    state of every established deployment — so on those databases nothing ran,
    and the schema-wide REVOKE won by default until someone hit a permission
    denied writing a coordination event.

    The repair path is still exercised, from a hand-stripped grant: it is the
    bootstrap route for a database that has never run migrate() and therefore
    has no registry to apply. It must repair without replaying the table DDL,
    because that would take ACCESS EXCLUSIVE on a live ledger (the 2026-07-22
    pg_dump incident).
    """
    dsn = scratch_db()
    before = _seed_legacy_public_ledger(dsn)
    _migrate(dsn)

    # A second full migrate re-runs the schema-wide REVOKE against a ledger
    # that is already in okg -- the exact state the trap describes.
    _migrate(dsn)

    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp'")
        assert cur.fetchone(), "migrate must provision okg_mcp"
        objects, grants, unadopted = ledger._ledger_state(cur)
        assert objects is True
        assert unadopted == []
        assert grants is True, (
            "migrate left the writer grant stripped: okg.grant_registry is "
            "supposed to re-apply it in the same file as the schema-wide "
            "REVOKE, and the ledger's own repair is probe-skipped here"
        )

        # The write works straight out of migrate, with no repair call.
        cur.execute("SET LOCAL ROLE okg_mcp")
        cur.execute(
            "INSERT INTO okg.pact_coordination_events "
            "(event_type, change_id) VALUES ('intent', 'post-migrate')"
        )
        cur.execute("RESET ROLE")
        conn.commit()

        # Now the bootstrap route, from a genuinely stripped grant. Strip it by
        # hand: this is the state of a database that has never run migrate, and
        # the only remaining reason ensure_ledger_schema issues grants at all.
        cur.execute(
            "REVOKE INSERT, SELECT ON okg.pact_coordination_events "
            "FROM okg_mcp"
        )
        conn.commit()

        # Before the repair the write must actually FAIL. Without this the
        # test proves only that an INSERT works after a no-op, which is what
        # it would also show if the grant had never been stripped.
        cur.execute("SET LOCAL ROLE okg_mcp")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            cur.execute(
                "INSERT INTO okg.pact_coordination_events "
                "(event_type, change_id) VALUES ('intent', 'pre-repair')"
            )
        conn.rollback()

        repair = ledger.ensure_ledger_schema(cur)
        assert repair == "writer_grants"
        conn.commit()

        # Steady state: nothing left to repair, and no DDL lock taken.
        assert ledger.ensure_ledger_schema(cur) is None

        cur.execute("SET LOCAL ROLE okg_mcp")
        cur.execute(
            "INSERT INTO okg.pact_coordination_events "
            "(event_type, change_id) VALUES ('intent', 'post-move') "
            "RETURNING id"
        )
        assert cur.fetchone() is not None
        cur.execute("RESET ROLE")
        conn.commit()

    # decide + view against the moved ledger, through the real API. These run
    # as the OWNER, not okg_mcp, and that is the production shape: `okg pact
    # decide` connects with the deployment DSN, and okg_mcp holds INSERT on
    # the coordination table ONLY -- it has no privilege on pact_decisions by
    # design, so running this half under that role would assert the opposite
    # of the least-privilege contract. The okg_mcp write path is proved above.
    with db_connect(dsn) as conn:
        recorded = ledger.record_decision(
            conn,
            change_id="schema-namespace-qualification",
            verdict="approve",
            principal="operator",
            note="D4: okg for application tables; _chronos* stays in public",
        )
        assert recorded["decision_id"] > 0

        events = ledger.load_coordination_events(conn, change_id="post-move")
        assert len(events) == 1

        decisions = ledger.load_decisions(
            conn, "schema-namespace-qualification"
        )
        assert [d.verdict for d in decisions] == ["approve"]

        watermark = ledger.load_decision_watermark(conn)
        assert watermark["count"] == before["pact_decisions"] + 1


def test_deployment_role_cannot_read_the_moved_ledger(scratch_db) -> None:
    """A namespace change must not quietly redraw a privilege boundary.

    role_split.sql grants every `okg_deployment_<name>` role SELECT on ALL
    TABLES IN SCHEMA okg. That grant is scoped to a schema, so moving the
    ledger INTO okg hands the operator's verdicts and the worktree-claim read
    model to every deployment workflow, for free, as a side effect of a
    rename. It was not readable before the move; it must not be after.
    """
    dsn = scratch_db()
    _migrate(dsn)

    role = "okg_deployment_nstest"
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "DO $$ BEGIN "
            "  IF NOT EXISTS ("
            "    SELECT 1 FROM pg_roles WHERE rolname = 'okg_deployment_nstest'"
            "  ) THEN CREATE ROLE okg_deployment_nstest NOLOGIN; END IF; "
            "END $$;"
        )
        # Reproduce the schema-wide grant, then the compensating REVOKE that
        # role_split.sql now issues.
        cur.execute(f"GRANT USAGE ON SCHEMA okg TO {role}")
        cur.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA okg TO {role}")
        for _bare, qualified in ledger.LEDGER_RELATIONS:
            cur.execute(f"REVOKE ALL ON {qualified} FROM {role}")

        for _bare, qualified in ledger.LEDGER_RELATIONS:
            cur.execute(
                "SELECT has_table_privilege(%s, %s, 'SELECT')",
                (role, qualified),
            )
            assert cur.fetchone()[0] is False, qualified
        # The revoke is targeted, not a blanket downgrade: ordinary substrate
        # reads must survive it.
        cur.execute(
            "SELECT has_table_privilege(%s, 'okg.subtypes', 'SELECT')",
            (role,),
        )
        assert cur.fetchone()[0] is True


def test_role_split_revokes_the_ledger_from_deployment_roles() -> None:
    """The SQL that ships must contain the compensating revoke."""
    role_split = (
        ROOT / "src" / "okg" / "substrate" / "runtime" / "sql"
        / "role_split.sql"
    ).read_text(encoding="utf-8")

    for _bare, qualified in ledger.LEDGER_RELATIONS:
        assert f"'{qualified}'" in role_split, qualified
    assert "REVOKE ALL ON %s FROM %I" in role_split


def test_ensure_refuses_an_unadopted_public_ledger(scratch_db) -> None:
    """Never bootstrap an empty okg ledger over a populated public one."""
    dsn = scratch_db(extensions=False)
    _seed_legacy_public_ledger(dsn)

    with db_connect(dsn) as conn, conn.cursor() as cur:
        with pytest.raises(SubstrateError) as excinfo:
            ledger.ensure_ledger_schema(cur)

    message = str(excinfo.value)
    assert "public" in message
    assert "0022_pact_ledger_schema_qualification" in (
        excinfo.value.fix_hint or ""
    )
    assert set(excinfo.value.data["public_ledger_relations"]) == set(
        _LEDGER_RELATIONS
    )
    assert excinfo.value.data["cause"] == "ledger_not_adopted"


def test_ensure_refuses_a_ledger_shadowed_by_public(scratch_db) -> None:
    """Split brain: rows in `public`, an empty shell in `okg`.

    This is the state the guard exists for and the one it used to be blind
    to. An old checkout of decisions.py, or a restored pre-qualification dump,
    re-creates `public.pact_decisions` next to the migrated `okg` copy. Every
    other health fact then reads fine and the CLI serves the EMPTY table --
    the operator's decisions are gone from view with nothing said.
    """
    dsn = scratch_db()
    _migrate(dsn)  # ledger lands in okg, empty

    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE public.pact_decisions "
            "(id BIGINT, change_id TEXT, verdict TEXT)"
        )
        cur.execute(
            "INSERT INTO public.pact_decisions VALUES (1, 'old', 'approve')"
        )
        conn.commit()

        objects_present, _grants, in_public = ledger._ledger_state(cur)
        assert objects_present is True, (
            "the okg copy is healthy — which is exactly why the old "
            "okg-is-NULL condition could not see this state"
        )
        assert in_public == ["pact_decisions"]

        with pytest.raises(SubstrateError) as excinfo:
            ledger.ensure_ledger_schema(cur)

    assert excinfo.value.data["cause"] == "ledger_shadowed_by_public"
    assert excinfo.value.data["okg_ledger_present"] is True
    assert "BOTH" in str(excinfo.value)


# --------------------------------------------------------------------------- #
# req.enforcement — runtime arm (DDL-capturing event trigger)                  #
# --------------------------------------------------------------------------- #
_EVENT_TRIGGER_SQL = """
CREATE TABLE IF NOT EXISTS public.namespace_ddl_log (
    object_identity TEXT NOT NULL,
    schema_name     TEXT,
    object_name     TEXT
);
CREATE OR REPLACE FUNCTION public.namespace_ddl_capture()
RETURNS event_trigger LANGUAGE plpgsql AS $capture$
DECLARE
    rec record;
BEGIN
    FOR rec IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        -- Three tags, not one. `CREATE TABLE AS` and `SELECT INTO` create
        -- real, persistent tables and arrive under their own command tags; a
        -- trigger watching only 'CREATE TABLE' reports a database that just
        -- grew two public tables as clean. `SELECT INTO` matters especially
        -- here because the STATIC arm deliberately does not chase it (in
        -- PL/pgSQL the same spelling is variable assignment), so this trigger
        -- is its ONLY coverage.
        IF rec.command_tag IN (
            'CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO'
        ) THEN
            INSERT INTO public.namespace_ddl_log
                (object_identity, schema_name, object_name)
            VALUES (
                rec.object_identity,
                rec.schema_name,
                split_part(rec.object_identity, '.', 2)
            );
        END IF;
    END LOOP;
END
$capture$;
CREATE EVENT TRIGGER namespace_ddl_capture ON ddl_command_end
    EXECUTE FUNCTION public.namespace_ddl_capture();
"""


def _install_ddl_capture(dsn: str) -> None:
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(_EVENT_TRIGGER_SQL)


def _captured(dsn: str) -> list[tuple[str, str]]:
    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT schema_name, object_name FROM public.namespace_ddl_log "
            " WHERE object_name <> 'namespace_ddl_log' ORDER BY object_identity"
        )
        return [(row[0], row[1]) for row in cur.fetchall()]


def _runtime_violations(dsn: str) -> list[tuple[str, str]]:
    return [
        (schema, name)
        for schema, name in _captured(dsn)
        if schema not in NAMESPACE.OWNED_SCHEMAS
        and not NAMESPACE.registry_relation_is_exempt(name)
    ]


def test_runtime_ledger_bootstrap_creates_only_okg_tables(scratch_db) -> None:
    dsn = scratch_db(extensions=False)
    _install_ddl_capture(dsn)

    with db_connect(dsn) as conn, conn.cursor() as cur:
        assert ledger.ensure_ledger_schema(cur) == "bootstrap"
        conn.commit()

    captured = _captured(dsn)
    assert sorted(name for _, name in captured) == sorted(_LEDGER_RELATIONS)
    assert {schema for schema, _ in captured} == {"okg"}
    assert _runtime_violations(dsn) == []


def test_runtime_trigger_catches_an_unqualified_create(scratch_db) -> None:
    """The red arm: the trigger must actually fire on a public-schema table."""
    dsn = scratch_db(extensions=False)
    _install_ddl_capture(dsn)

    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("CREATE TABLE ledger_shadow (id BIGINT)")
        conn.commit()

    assert _runtime_violations(dsn) == [("public", "ledger_shadow")]


def test_runtime_trigger_catches_ctas_and_select_into(scratch_db) -> None:
    """The two forms that create a table without saying `CREATE TABLE`.

    `SELECT ... INTO` has no static coverage at all by design (the static
    scanner cannot tell it from PL/pgSQL variable assignment), so if this
    trigger misses it nothing in the repo sees it.
    """
    dsn = scratch_db(extensions=False)
    _install_ddl_capture(dsn)

    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("CREATE TABLE ctas_shadow AS SELECT 1 AS id")
        cur.execute("SELECT 2 AS id INTO into_shadow")
        conn.commit()

    assert _runtime_violations(dsn) == [
        ("public", "ctas_shadow"),
        ("public", "into_shadow"),
    ]


def test_runtime_trigger_admits_the_registered_chronos_family(
    scratch_db,
) -> None:
    dsn = scratch_db(extensions=False)
    _install_ddl_capture(dsn)

    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("CREATE TABLE _chronos_branch_tables (id BIGINT)")
        # A real interval-physical name, as chronos actually mints it:
        # `_chronos_b_interval_<logical schema>_<logical table>_<hash>`.
        cur.execute(
            "CREATE TABLE _chronos_b_interval_okg_graph_nodes_1c8e1fc23f "
            "(id BIGINT)"
        )
        conn.commit()

    assert _runtime_violations(dsn) == []
    assert len(_captured(dsn)) == 2
