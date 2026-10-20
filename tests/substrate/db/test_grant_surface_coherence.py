"""The grant surface is a registry, and the two scripts that apply it commute.

PACT ``schema-grant-coherence``. Three requirements, three sections:

* ``req.scoped-catchup`` — ``role_split.sql`` no longer blanket-grants okg_mcp,
  and a grant the runtime cannot apply is queued rather than lost.
* ``req.registry-not-convention`` — app_ro reads what ``okg.grant_registry``
  says it reads, not what a view happens to be NAMED, and an unregistered view
  is both unreadable and named by the drift check.
* ``req.commutativity-gate`` — the two scripts produce the same privileges in
  either order, on existing relations AND in ``pg_default_acl``, and a fresh
  okg_substrate-created table stays unreadable to okg_mcp.

The hermetic tests run everywhere; the Postgres ones need a migrated database;
the commutativity tests build their own throwaway databases and are the
``grant-commutativity`` lane's payload.
"""
from __future__ import annotations

import argparse
import importlib.util
import re
import sys
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
ROLE_SPLIT_SQL = (
    REPO_ROOT / "src" / "okg" / "substrate" / "runtime" / "sql" / "role_split.sql"
)
VIEWS_SQL = REPO_ROOT / "src" / "okg" / "substrate" / "db" / "views.sql"
SCHEMA_SQL = REPO_ROOT / "src" / "okg" / "substrate" / "db" / "schema.sql"
COMMUTATIVITY_SCRIPT = REPO_ROOT / "scripts" / "check_grant_commutativity.py"

VIEW_DECL_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(okg\.v_[a-z0-9_]+)", re.IGNORECASE
)


def _load_commutativity_module():
    """scripts-as-modules: scripts/ is not a package."""
    spec = importlib.util.spec_from_file_location(
        "check_grant_commutativity", COMMUTATIVITY_SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_grant_commutativity"] = module
    spec.loader.exec_module(module)
    return module


def _sql_without_line_comments(path: Path) -> str:
    return "\n".join(
        line
        for line in path.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("--")
    )


def _registry_seeded_relations() -> set[str]:
    """Relations named in views.sql's substrate seed.

    Read from the file rather than from a database so the hermetic tests below
    can gate a PR without Postgres.
    """
    body = _sql_without_line_comments(VIEWS_SQL)
    start = body.index("DO $okg_grant_registry_seed$")
    end = body.index("$okg_grant_registry_seed$;", start)
    seed = body[start:end]
    return set(re.findall(r"'(okg\.[a-z0-9_]+)'", seed))


# ---------------------------------------------------------------------------
# req.scoped-catchup — hermetic
# ---------------------------------------------------------------------------


def test_role_split_does_not_blanket_grant_the_mcp_role() -> None:
    """The catch-up is scoped. This is the F5 finding, asserted on the file.

    A DB-level assertion alone would not catch a re-introduction guarded on a
    role or table that the test database happens not to have.
    """
    body = _sql_without_line_comments(ROLE_SPLIT_SQL)
    normalized = " ".join(body.split())

    blanket = re.findall(
        r"GRANT\s+[^;]*?ON\s+ALL\s+TABLES\s+IN\s+SCHEMA\s+[^;]*?okg_mcp",
        normalized,
        re.IGNORECASE,
    )
    assert not blanket, (
        "role_split.sql blanket-grants okg_mcp again; whichever of "
        "role_split.sql / views.sql runs last would decide whether the MCP "
        f"read pool can read raw fact tables: {blanket}"
    )

    # The worse half: a default ACL is invisible in every relation's grants and
    # survives every REVOKE aimed at existing tables.
    default_grants = re.findall(
        r"ALTER\s+DEFAULT\s+PRIVILEGES\s+FOR\s+ROLE\s+okg_substrate"
        r"[^;]*?GRANT[^;]*?okg_mcp",
        normalized,
        re.IGNORECASE,
    )
    assert not default_grants, (
        "role_split.sql re-adds an okg_substrate-scoped default privilege for "
        f"okg_mcp; every FUTURE okg_substrate table would be readable: "
        f"{default_grants}"
    )


def test_role_split_applies_the_registry() -> None:
    body = _sql_without_line_comments(ROLE_SPLIT_SQL)
    assert "okg.apply_grant_registry()" in body, (
        "role_split.sql must apply the same registry views.sql applies, or the "
        "two scripts have no shared definition of the grant surface"
    )


def test_role_split_converges_the_stale_default_acl() -> None:
    """Deleting the old statements converges a FRESH database, not an existing
    one: a default ACL entry persists until it is explicitly revoked."""
    body = _sql_without_line_comments(ROLE_SPLIT_SQL)
    normalized = " ".join(body.split())
    assert re.search(
        r"ALTER DEFAULT PRIVILEGES FOR ROLE okg_substrate IN SCHEMA okg "
        r"REVOKE SELECT ON TABLES FROM okg_mcp",
        normalized,
    ), "role_split.sql must revoke the default ACL its earlier version created"
    assert re.search(
        r"ALTER DEFAULT PRIVILEGES FOR ROLE okg_substrate IN SCHEMA public "
        r"REVOKE SELECT ON TABLES FROM okg_mcp",
        normalized,
    ), "the public-schema default ACL needs the same revoke"


def test_grant_catchup_enqueues_instead_of_warning_and_forgetting() -> None:
    """The exhaustion arm writes to the durable queue.

    Source-level because the three-consecutive-collision path is not reachable
    on demand; the behavioural half is covered by the Postgres tests below and
    by tests/substrate/test_mcp_grant_concurrency.py.
    """
    from okg.substrate.graph_store_internal import schema as chronos_schema

    body = Path(chronos_schema.__file__).read_text(encoding="utf-8")
    assert "INSERT INTO okg.pending_grants" in body, (
        "grant-retry exhaustion must enqueue a durable retry; a RAISE WARNING "
        "alone is how a dropped privilege resurfaces later as an unrelated "
        "permission-denied"
    )
    assert "FOR rec IN SELECT statement FROM okg.pending_grants" in body, (
        "the queue must be drained at the top of the block, or an enqueued "
        "grant is never retried"
    )


def test_grant_dedup_never_reorders_the_statement_list() -> None:
    """ORDER IS LOCK ORDER, and this one already bit.

    Every GRANT takes a row lock on the target relation's ``pg_class`` tuple.
    Concurrent sources run this block as separate PROCESSES, so two of them
    issuing the same grants in DIFFERENT orders deadlock (40P01) instead of
    serialising. Adding the pending-grant queue introduced duplicates, and the
    obvious fix -- ``array_agg(DISTINCT ...)`` -- SORTS, which reordered the
    list and deadlocked ``test_parallel_grant_catchups_do_not_raise``.

    The failure is invisible to any single-session test: the privilege picture
    a sorted apply produces is identical. Hence a source assertion, plus the
    concurrent-apply probe in scripts/check_grant_commutativity.py.
    """
    from okg.substrate.graph_store_internal import schema as chronos_schema

    body = Path(chronos_schema.__file__).read_text(encoding="utf-8")
    # Comment lines are stripped: the block deliberately NAMES the rejected
    # form to explain why it is rejected, and a naive substring check would
    # then fail on the very comment that documents the trap.
    executable = "\n".join(
        line
        for line in body.splitlines()
        if not line.lstrip().startswith(("--", "#"))
    )
    assert "array_agg(DISTINCT" not in executable, (
        "array_agg(DISTINCT ...) sorts the grant list; concurrent grant "
        "catch-ups then take pg_class locks in a different order and deadlock"
    )
    assert "WITH ORDINALITY" in executable, (
        "deduplication must preserve first-occurrence order"
    )


def test_the_applier_refuses_a_zero_grant_pass() -> None:
    """A vacuous apply must RAISE, not return 0.

    Both callers do `PERFORM okg.apply_grant_registry()` and discard the count,
    and the schema-wide REVOKEs run immediately before it. So a run that
    granted nothing -- wrong session role, every statement denied -- would
    otherwise be indistinguishable from a successful one and would leave the
    read roles stripped. Before this PACT the equivalent statements were bare
    GRANTs that failed the install loudly; that property is kept.
    """
    body = _sql_without_line_comments(VIEWS_SQL)
    start = body.index("CREATE OR REPLACE FUNCTION okg.apply_grant_registry()")
    end = body.index("$okg_apply_grant_registry$;", start)
    applier = body[start:end]
    assert "IF applied = 0 AND EXISTS" in applier, (
        "the applier must refuse a zero-grant pass against a non-empty registry"
    )
    assert "RAISE EXCEPTION" in applier


def test_denial_is_not_treated_as_nothing_to_do() -> None:
    """`insufficient_privilege` must not share a branch with `undefined_table`.

    They mean opposite things: one is "the object is gone, nothing to grant",
    the other is "the grant did not land". Sharing the branch made a denial
    fall through to the dequeue and erase its own queue row, so an unapplied
    grant became invisible -- in BOTH appliers.
    """
    from okg.substrate.graph_store_internal import schema as chronos_schema

    sources = {
        "chronos catch-up": Path(chronos_schema.__file__).read_text(
            encoding="utf-8"
        ),
        "registry applier": VIEWS_SQL.read_text(encoding="utf-8"),
    }
    for label, body in sources.items():
        executable = "\n".join(
            line
            for line in body.splitlines()
            if not line.lstrip().startswith(("--", "#"))
        )
        assert "insufficient_privilege OR undefined_table" not in executable, (
            f"{label}: a denial is being handled as 'nothing to do'"
        )
        assert "WHEN insufficient_privilege THEN" in executable, (
            f"{label}: denial needs its own branch that queues the statement"
        )


def test_chronos_enumeration_loops_are_ordered() -> None:
    """Both catalog scans must ORDER BY, for the same reason the dedup must not
    sort: the emitted order is the lock-acquisition order."""
    from okg.substrate.graph_store_internal import schema as chronos_schema

    body = Path(chronos_schema.__file__).read_text(encoding="utf-8")
    executable = "\n".join(
        line
        for line in body.splitlines()
        if not line.lstrip().startswith(("--", "#"))
    )
    scans = executable.count("left(c.relname, 8) = '_chronos'")
    ordered = executable.count("ORDER BY 1")
    assert scans >= 2, "expected the table and sequence scans"
    assert ordered >= scans, (
        f"{scans} chronos catalog scans but only {ordered} ORDER BY clauses; "
        "an unordered scan emits grants in catalog order, which two sessions "
        "need not agree on"
    )


def test_the_registry_applier_iterates_in_a_fixed_order() -> None:
    """Same property, on the other applier: two sessions applying the registry
    concurrently must take the same locks in the same order."""
    body = _sql_without_line_comments(VIEWS_SQL)
    start = body.index("CREATE OR REPLACE FUNCTION okg.apply_grant_registry()")
    end = body.index("$okg_apply_grant_registry$;", start)
    applier = body[start:end]
    assert "ORDER BY g.entry_ref" in applier, (
        "the registry applier must iterate in a deterministic order, or two "
        "sessions applying it at once deadlock on pg_class"
    )


def test_concurrent_probe_bounds_every_worker_lifetime() -> None:
    body = _sql_without_line_comments(COMMUTATIVITY_SCRIPT)
    assert "connect_timeout=10" in body
    assert "SET statement_timeout = '90s'" in body
    assert "SET lock_timeout = '60s'" in body
    assert "threading.Thread(target=_apply, daemon=True)" in body, (
        "a wedged worker must not keep the checker process alive after its "
        "join budget expires"
    )


# ---------------------------------------------------------------------------
# req.registry-not-convention — hermetic
# ---------------------------------------------------------------------------


def test_views_sql_has_no_wildcard_view_grant_loop() -> None:
    body = _sql_without_line_comments(VIEWS_SQL)
    normalized = " ".join(body.split())
    assert "GRANT SELECT ON okg.%I TO app_ro" not in normalized, (
        "the v_% auto-grant loop is back; it reduces the audited allow-list to "
        "a naming convention"
    )
    leftover = re.findall(
        r"GRANT\s+SELECT\s+ON\s+okg\.v_[a-z0-9_]+\s+TO\s+app_ro",
        normalized,
        re.IGNORECASE,
    )
    assert not leftover, (
        "view grants must come from okg.grant_registry, not from statements "
        f"role_split.sql cannot read: {leftover}"
    )


def test_every_substrate_v_view_has_a_registry_row() -> None:
    """The registry is complete against the source of views.

    This is the hermetic half of the drift check: a PR that adds a view without
    a registry row fails here, in the fast lane, rather than only against a
    migrated database.

    LIMIT, stated rather than left to be discovered: it reads `CREATE ... VIEW`
    text, so it cannot see a view built by dynamic SQL — including
    `management.ensure_kept_table_honest_read_view`'s `okg.v_current_*`, which
    is composed at runtime from a table name. Those are covered behaviourally
    instead, by ::test_drift_check_is_empty_on_a_migrated_database and
    ::test_a_deployment_registry_row_survives_the_substrate_seed against a real
    database where they exist. A static check that silently skipped the dynamic
    cases while reading as full coverage is the trap here, so the behavioural
    backstop is not optional.
    """
    declared: set[str] = set()
    for path in (VIEWS_SQL, SCHEMA_SQL):
        declared |= set(VIEW_DECL_RE.findall(_sql_without_line_comments(path)))
    migrations = REPO_ROOT / "src" / "okg" / "substrate" / "db" / "migrations"
    for path in sorted(migrations.glob("*.sql")):
        declared |= set(VIEW_DECL_RE.findall(_sql_without_line_comments(path)))

    seeded = _registry_seeded_relations()
    missing = sorted(v for v in declared if v not in seeded)
    assert not missing, (
        "these okg.v_* views have no okg.grant_registry row, so app_ro cannot "
        "read them. Add the name to the app_ro array in views.sql's "
        f"okg_grant_registry_seed block, or state why it is internal: {missing}"
    )


def test_registry_owns_the_pact_ledger_grants() -> None:
    """#1064's handoff: the schema-wide REVOKE strips these, and the ledger's
    own ensure path is probe-skipped, so nothing put them back."""
    seeded = _registry_seeded_relations()
    for relation in (
        "okg.pact_coordination_events",
        "okg.pact_decisions",
        "okg.pact_intents",
    ):
        assert relation in seeded, (
            f"{relation} is not in the grant registry; "
            "REVOKE ALL ON ALL TABLES IN SCHEMA okg FROM okg_mcp would strip "
            "MCP coordination access with nothing to restore it"
        )


def test_mirror_tables_are_classified_internal() -> None:
    """v1.0 design §2.2: never granted, delta-maintenance is the precondition
    of ever exposing them."""
    body = _sql_without_line_comments(VIEWS_SQL)
    for relation in (
        "okg.chronos_graph_nodes_current",
        "okg.chronos_graph_edges_current",
    ):
        assert f"'internal:{relation}'" in body, (
            f"{relation} must carry an internal classification row so the "
            "drift check treats any read privilege on it as a finding"
        )


# ---------------------------------------------------------------------------
# req.registry-not-convention / req.scoped-catchup — against a migrated database
# ---------------------------------------------------------------------------


@pytest.mark.postgres
def test_drift_check_is_empty_on_a_migrated_database(okg_migrated: str) -> None:
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT finding_kind, relation, role_name, detail"
            "  FROM okg.grant_registry_drift() ORDER BY 1, 2, 3"
        ).fetchall()
    assert rows == [], (
        "the live privilege surface disagrees with okg.grant_registry:\n"
        + "\n".join(f"  {row}" for row in rows)
    )


@pytest.mark.postgres
def test_registered_views_are_readable(okg_migrated: str) -> None:
    """Regression sweep over the whole registered app_ro surface."""
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        rows = conn.execute(
            """
            SELECT g.relation
              FROM okg.grant_registry g
             WHERE g.role_name = 'app_ro'
               AND g.classification = 'contract'
               AND to_regclass(g.relation) IS NOT NULL
               AND NOT has_table_privilege('app_ro', to_regclass(g.relation),
                                           'SELECT')
             ORDER BY 1
            """
        ).fetchall()
    assert rows == [], f"registered views app_ro cannot read: {rows}"


@pytest.mark.postgres
def test_unregistered_view_is_unreadable_and_named_by_the_drift_check(
    okg_migrated: str,
) -> None:
    view = f"v_test_unregistered_{uuid4().hex[:10]}"
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        conn.execute(f"CREATE VIEW okg.{view} AS SELECT 1 AS probe")
        try:
            readable = conn.execute(
                "SELECT has_table_privilege('app_ro', %s, 'SELECT')",
                (f"okg.{view}",),
            ).fetchone()[0]
            assert not readable, (
                f"okg.{view} is readable by app_ro without a registry row — "
                "the naming convention is back"
            )
            named = conn.execute(
                """
                SELECT finding_kind, relation
                  FROM okg.grant_registry_drift()
                 WHERE relation = %s
                """,
                (f"okg.{view}",),
            ).fetchall()
            assert [row[0] for row in named] == ["unregistered_view"], (
                "an unregistered v_ view must be a loud finding, not a silent "
                f"omission; drift reported {named}"
            )
        finally:
            conn.execute(f"DROP VIEW okg.{view}")


@pytest.mark.postgres
def test_unregistered_materialized_view_is_named_by_the_drift_check(
    okg_migrated: str,
) -> None:
    view = f"v_test_unregistered_mat_{uuid4().hex[:10]}"
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        conn.execute(f"CREATE MATERIALIZED VIEW okg.{view} AS SELECT 1 AS probe")
        try:
            named = conn.execute(
                "SELECT finding_kind FROM okg.grant_registry_drift() "
                "WHERE relation = %s",
                (f"okg.{view}",),
            ).fetchall()
            assert [row[0] for row in named] == ["unregistered_view"], (
                "an unregistered v_ materialized view must be as loud as an "
                f"ordinary view; drift reported {named}"
            )
        finally:
            conn.execute(f"DROP MATERIALIZED VIEW okg.{view}")


@pytest.mark.postgres
def test_public_grant_is_effective_drift_for_both_read_roles(
    okg_migrated: str,
) -> None:
    relation = "okg.node_facts"
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        conn.execute(f"GRANT SELECT ON {relation} TO PUBLIC")
        try:
            findings = conn.execute(
                "SELECT role_name FROM okg.grant_registry_drift() "
                "WHERE finding_kind = 'unregistered_grant' AND relation = %s "
                "AND detail LIKE '%%inherited via PUBLIC%%' ORDER BY role_name",
                (relation,),
            ).fetchall()
            assert [row[0] for row in findings] == ["app_ro", "okg_mcp"], (
                "PUBLIC exposes the raw relation to both governed read roles; "
                f"drift reported {findings}"
            )
        finally:
            conn.execute(f"REVOKE SELECT ON {relation} FROM PUBLIC")


@pytest.mark.postgres
@pytest.mark.serial_db
def test_inherited_role_grant_is_effective_drift_for_both_read_roles(
    okg_migrated: str,
) -> None:
    relation = "okg.node_facts"
    parent_role = f"grant_probe_parent_{uuid4().hex[:10]}"
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        conn.execute(f"CREATE ROLE {parent_role}")
        try:
            conn.execute(f"GRANT {parent_role} TO app_ro")
            conn.execute(f"GRANT SELECT ON {relation} TO {parent_role}")
            findings = conn.execute(
                "SELECT role_name FROM okg.grant_registry_drift() "
                "WHERE finding_kind = 'unregistered_grant' AND relation = %s "
                "AND detail LIKE %s ORDER BY role_name",
                (relation, f"%inherited via {parent_role}%"),
            ).fetchall()
            assert [row[0] for row in findings] == ["app_ro", "okg_mcp"], (
                "a parent role exposes the raw relation through app_ro's "
                f"membership chain; drift reported {findings}"
            )
        finally:
            conn.execute(f"REVOKE SELECT ON {relation} FROM {parent_role}")
            conn.execute(f"REVOKE {parent_role} FROM app_ro")
            conn.execute(f"DROP ROLE {parent_role}")


@pytest.mark.postgres
def test_revoked_pattern_grant_is_reported_for_the_live_relation(
    okg_migrated: str,
) -> None:
    table = f"_chronos_grant_probe_{uuid4().hex[:10]}"
    relation = f"public.{table}"
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        conn.execute(f"CREATE TABLE {relation} (probe integer)")
        try:
            conn.execute("SELECT okg.apply_grant_registry()")
            assert conn.execute(
                "SELECT has_table_privilege('okg_mcp', %s, 'SELECT')",
                (relation,),
            ).fetchone()[0], "the pattern registry row did not grant the probe"
            conn.execute(f"REVOKE SELECT ON {relation} FROM okg_mcp")
            findings = conn.execute(
                "SELECT finding_kind, relation FROM okg.grant_registry_drift() "
                "WHERE finding_kind = 'missing_grant' AND relation = %s",
                (relation,),
            ).fetchall()
            assert findings == [("missing_grant", relation)], (
                "revoking one live relation matched by the Chronos pattern "
                f"must be loud; drift reported {findings}"
            )
        finally:
            conn.execute(f"DROP TABLE {relation}")


@pytest.mark.postgres
def test_a_deployment_registry_row_survives_the_substrate_seed(
    okg_migrated: str,
) -> None:
    """The extension point.

    The substrate seed DELETEs and re-INSERTs only `source = 'substrate'`, so a
    deployment (or the runtime kept-table path, which registers its
    `okg.v_current_*` honest-read views) keeps its rows across every migrate
    and gets them re-granted after the schema-wide REVOKE.
    """
    from okg.substrate.db.migrate import install_views

    view = f"v_test_registered_{uuid4().hex[:10]}"
    entry = f"grant:app_ro:okg.{view}"
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        conn.execute(f"CREATE VIEW okg.{view} AS SELECT 1 AS probe")
        conn.execute(
            """
            INSERT INTO okg.grant_registry
              (entry_ref, relation, object_kind, role_name, privileges, source)
            VALUES (%s, %s, 'TABLE', 'app_ro', 'SELECT', 'deployment')
            """,
            (entry, f"okg.{view}"),
        )
    try:
        install_views(okg_migrated)
        with psycopg.connect(okg_migrated, autocommit=True) as conn:
            survived = conn.execute(
                "SELECT count(*) FROM okg.grant_registry WHERE entry_ref = %s",
                (entry,),
            ).fetchone()[0]
            assert survived == 1, (
                "the substrate seed deleted a deployment-owned row; deployments "
                "would lose their grants on every migrate"
            )
            readable = conn.execute(
                "SELECT has_table_privilege('app_ro', %s, 'SELECT')",
                (f"okg.{view}",),
            ).fetchone()[0]
            assert readable, (
                "a registered deployment view is not readable after a views "
                "re-apply; the schema-wide REVOKE stripped it and the registry "
                "did not put it back"
            )
            drift = conn.execute(
                "SELECT count(*) FROM okg.grant_registry_drift()"
                " WHERE relation = %s",
                (f"okg.{view}",),
            ).fetchone()[0]
            assert drift == 0, "a registered view must not read as drift"
    finally:
        with psycopg.connect(okg_migrated, autocommit=True) as conn:
            conn.execute(f"DROP VIEW IF EXISTS okg.{view}")
            conn.execute(
                "DELETE FROM okg.grant_registry WHERE entry_ref = %s", (entry,)
            )


@pytest.mark.postgres
@pytest.mark.serial_db
def test_internal_classification_fires_when_a_mirror_becomes_readable(
    okg_migrated: str,
) -> None:
    """Non-vacuity for the `internal_exposed` arm.

    The mirror tables are created by the publisher (TRUNCATE + re-insert each
    publish), so a fresh install has neither of them and the arm would sit at
    zero findings forever -- passing without ever having been exercised. Create
    one, grant it, and require the finding.
    """
    relation = "okg.chronos_graph_nodes_current"
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        pre_existing = conn.execute(
            "SELECT to_regclass(%s) IS NOT NULL", (relation,)
        ).fetchone()[0]
        registered_internal = conn.execute(
            "SELECT count(*) FROM okg.grant_registry"
            " WHERE relation = %s AND classification = 'internal'",
            (relation,),
        ).fetchone()[0]
        assert registered_internal == 1, (
            f"{relation} has no internal classification row, so this test "
            "would prove nothing"
        )
        if not pre_existing:
            conn.execute(f"CREATE TABLE {relation} (node_id text)")
    try:
        with psycopg.connect(okg_migrated, autocommit=True) as conn:
            clean = conn.execute(
                "SELECT count(*) FROM okg.grant_registry_drift()"
                " WHERE finding_kind = 'internal_exposed' AND relation = %s",
                (relation,),
            ).fetchone()[0]
            assert clean == 0, (
                "the mirror table is already readable by a governed role "
                "before this test granted anything"
            )

            conn.execute(f"GRANT SELECT ON {relation} TO app_ro")
            findings = conn.execute(
                "SELECT role_name FROM okg.grant_registry_drift()"
                " WHERE finding_kind = 'internal_exposed' AND relation = %s"
                " ORDER BY role_name",
                (relation,),
            ).fetchall()
            roles = [row[0] for row in findings]
            # okg_mcp is a member of app_ro, so granting app_ro exposes the
            # relation to BOTH governed roles. The arm uses has_table_privilege
            # precisely so an inherited exposure is not missed.
            assert roles == ["app_ro", "okg_mcp"], (
                "a readable internal-classified relation must be a finding for "
                f"every governed role that can reach it; got {roles}"
            )
    finally:
        with psycopg.connect(okg_migrated, autocommit=True) as conn:
            if pre_existing:
                conn.execute(f"REVOKE SELECT ON {relation} FROM app_ro")
            else:
                conn.execute(f"DROP TABLE IF EXISTS {relation}")


@pytest.mark.postgres
def test_okg_mcp_cannot_read_the_raw_write_side(okg_migrated: str) -> None:
    """The #366 allow-list, checked as an outcome rather than as a statement."""
    forbidden = (
        "okg.node_facts",
        "okg.edge_facts",
        "okg.generation_ordinals",
        "okg.admission_events",
        "okg.deployment_apply_runs",
    )
    leaks: list[str] = []
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        for relation in forbidden:
            present = conn.execute(
                "SELECT to_regclass(%s) IS NOT NULL", (relation,)
            ).fetchone()[0]
            if not present:
                continue
            readable = conn.execute(
                "SELECT has_table_privilege('okg_mcp', %s, 'SELECT')",
                (relation,),
            ).fetchone()[0]
            if readable:
                leaks.append(relation)
    assert not leaks, f"okg_mcp can read the raw write side: {leaks}"


@pytest.mark.postgres
def test_pact_coordination_grants_survive_a_views_reapply(
    okg_migrated: str,
) -> None:
    """The cross-PACT trap: views.sql revokes, and the ledger's probe-skipped
    ensure never puts the grant back. The registry has to own it."""
    from okg.substrate.db.migrate import install_views

    install_views(okg_migrated)
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        present = conn.execute(
            "SELECT to_regclass('okg.pact_coordination_events') IS NOT NULL"
        ).fetchone()[0]
        assert present, "the ledger table is absent; this test would be vacuous"
        for privilege in ("INSERT", "SELECT"):
            granted = conn.execute(
                "SELECT has_table_privilege('okg_mcp', "
                "'okg.pact_coordination_events', %s)",
                (privilege,),
            ).fetchone()[0]
            assert granted, (
                f"okg_mcp lost {privilege} on okg.pact_coordination_events "
                "after a views.sql re-apply; `okg pact` coordination writes "
                "would start failing with permission denied"
            )


# ---------------------------------------------------------------------------
# req.scoped-catchup — the durable pending-grant queue
# ---------------------------------------------------------------------------


#: A statement the drain loop will execute and that always raises the
#: concurrency SQLSTATE, so the exhaustion arm is reachable on demand. The real
#: path needs three consecutive lost races against a competing catalog writer.
ALWAYS_CONTENDED = (
    "DO $probe$ BEGIN RAISE EXCEPTION USING ERRCODE = 'XX000', "
    "MESSAGE = 'tuple concurrently updated'; END $probe$"
)


def _run_grant_catchup(dsn: str) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store_internal.schema import _clear_mcp_grant_guard

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        # The per-process epoch memo would skip the DO block outright.
        _clear_mcp_grant_guard()
        store._grant_chronos_public_tables_to_mcp_if_present()


@pytest.mark.postgres
@pytest.mark.serial_db
def test_exhausted_grant_stays_queued_and_doctor_reports_it(
    clean_runtime: str,
) -> None:
    from okg.substrate.cli.doctor import _pending_grant_findings

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO okg.pending_grants (statement, attempts) "
            "VALUES (%s, 1) ON CONFLICT (statement) DO NOTHING",
            (ALWAYS_CONTENDED,),
        )
    try:
        _run_grant_catchup(clean_runtime)
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            row = conn.execute(
                "SELECT attempts, last_error FROM okg.pending_grants "
                " WHERE statement = %s",
                (ALWAYS_CONTENDED,),
            ).fetchone()
            assert row is not None, (
                "a statement that exhausted its retries was dropped from the "
                "queue; that is the warn-and-forget behaviour this PACT removed"
            )
            assert row[0] > 1, (
                "the retry was not recorded, so the queue cannot distinguish a "
                "first failure from a persistent one"
            )
            assert row[1], "the failure reason must be durable, not log-only"

            findings = _pending_grant_findings(conn)
            assert [f["statement"] for f in findings] == [ALWAYS_CONTENDED], (
                f"doctor does not surface the queued grant: {findings}"
            )
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute(
                "DELETE FROM okg.pending_grants WHERE statement = %s",
                (ALWAYS_CONTENDED,),
            )


@pytest.mark.postgres
@pytest.mark.serial_db
def test_queued_grant_is_applied_on_the_next_epoch(clean_runtime: str) -> None:
    """The queue is a retry, not a log: the next run applies it and clears it."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        target = conn.execute(
            """
            SELECT n.nspname || '.' || c.relname
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'public'
               AND c.relkind IN ('r', 'p')
               AND c.relname LIKE '\\_chronos%'
             ORDER BY 1 LIMIT 1
            """
        ).fetchone()
        assert target, (
            "no public._chronos* table present; the queue-drain assertion "
            "would be vacuous"
        )
        relation = target[0]
        # BYTE-IDENTICAL to what the catch-up's own enumeration produces, or
        # the dedup path is never exercised and the queued row is simply a
        # second, differently-spelled statement. The block builds its target as
        # quote_ident(nspname) || '.' || quote_ident(relname), and quote_ident
        # leaves a lowercase `_chronos…` name UNQUOTED -- so `public."x"` (the
        # spelling this test used first) never collides with `public.x`.
        # Asserted below rather than trusted.
        bare_name = relation.split(".", 1)[1]
        statement = conn.execute(
            "SELECT 'GRANT SELECT ON TABLE ' || quote_ident('public') || '.'"
            "    || quote_ident(%s) || ' TO okg_mcp'",
            (bare_name,),
        ).fetchone()[0]
        assert '"' not in statement, (
            "quote_ident quoted this relation name, so the spelling below no "
            f"longer matches the catch-up's derived statement: {statement}"
        )
        conn.execute(f"REVOKE SELECT ON TABLE {relation} FROM okg_mcp")
        readable = conn.execute(
            "SELECT has_table_privilege('okg_mcp', %s, 'SELECT')", (relation,)
        ).fetchone()[0]
        assert not readable, (
            "the REVOKE did not take; 'granted afterwards' would be "
            "tautological"
        )
        conn.execute(
            "INSERT INTO okg.pending_grants (statement) VALUES (%s) "
            "ON CONFLICT (statement) DO NOTHING",
            (statement,),
        )

    _run_grant_catchup(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        readable = conn.execute(
            "SELECT has_table_privilege('okg_mcp', %s, 'SELECT')", (relation,)
        ).fetchone()[0]
        assert readable, "the queued grant was not re-applied on the next run"
        remaining = conn.execute(
            "SELECT count(*) FROM okg.pending_grants WHERE statement = %s",
            (statement,),
        ).fetchone()[0]
        assert remaining == 0, (
            "an applied grant must leave the queue, or doctor reports a "
            "permanent finding for a privilege that is present"
        )


@pytest.mark.postgres
def test_doctor_grant_registry_drift_reports_clean(okg_migrated: str) -> None:
    from okg.substrate.cli.doctor import _doctor_grant_registry_drift

    args = argparse.Namespace(dsn=okg_migrated, deployment=None)
    assert _doctor_grant_registry_drift(args, as_json=True) == 0


# ---------------------------------------------------------------------------
# req.commutativity-gate
# ---------------------------------------------------------------------------


@pytest.mark.postgres
@pytest.mark.slow
def test_the_two_scripts_commute(okg_dsn: str) -> None:
    commute = _load_commutativity_module()
    report = commute.check_commutativity(okg_dsn, prefix="okg_gc_ok")
    assert report["ok"], commute._render_text(report)
    # Non-negotiable coverage: a gate that only compared current ACLs would
    # report green on the default-ACL bug.
    sections = {diff["section"] for diff in report["diffs"]}
    assert "default_acl" in sections
    assert "relation_privileges" in sections
    for side, probe in report["probes"].items():
        assert probe["ok"], f"new-table probe leaked on the {side} database"
    for side, findings in report["registry_drift"].items():
        assert findings == [], f"absolute privilege drift on {side}: {findings}"
    # Concurrent application is part of the gate, not a separate exercise: a
    # reordered apply produces an IDENTICAL privilege picture and deadlocks
    # only under a race, so the diff above cannot see it.
    assert report["concurrent_apply"]["ok"], (
        "concurrent registry application raised: "
        f"{report['concurrent_apply']['errors']}"
    )


@pytest.mark.postgres
@pytest.mark.slow
@pytest.mark.parametrize(
    ("mutation", "expected_section"),
    [
        ("grant", "relation_privileges"),
        ("default-acl", "default_acl"),
        ("schema-usage", "schema_privileges"),
    ],
)
def test_an_order_dependent_statement_turns_the_gate_red(
    okg_dsn: str, mutation: str, expected_section: str
) -> None:
    """Non-vacuity, once per failure mode.

    ``grant`` is visible in the relation ACLs. ``default-acl`` is NOT: it shows
    up only in pg_default_acl and in the behavioural probe, which is the whole
    argument for those two arms of the check.
    """
    commute = _load_commutativity_module()
    report = commute.check_commutativity(
        okg_dsn, prefix="okg_gc_mut", mutation=mutation
    )
    assert not report["ok"], (
        f"the {mutation} mutation did not turn the commutativity gate red; "
        "the gate proves nothing"
    )
    assert expected_section in report["failing_sections"], (
        f"the {mutation} mutation was expected to diverge in "
        f"{expected_section}, got {report['failing_sections']}"
    )
    if mutation == "default-acl":
        assert report["failing_sections"] == ["default_acl"], (
            "an order-dependent default ACL must be invisible to the "
            "existing-relation diff — that is why the pg_default_acl arm and "
            "the new-table probe exist"
        )
        assert not report["probes"]["views_first"]["ok"], (
            "the behavioural probe must catch the leaked default ACL"
        )
    if mutation == "schema-usage":
        # Same argument, third failure mode: revoking USAGE ON SCHEMA leaves
        # every table ACL byte-identical while locking the role out entirely.
        # has_table_privilege answers only the table question, so a gate
        # without the schema-ACL arm and without a real SELECT reports OK here.
        assert report["failing_sections"] == ["schema_privileges"], (
            "an order-dependent schema privilege must be invisible to the "
            f"relation/column/function diffs, got {report['failing_sections']}"
        )
        probe = report["probes"]["views_first"]
        assert not probe["ok"]
        assert not probe["app_ro_can_read_a_registered_view"], (
            "the probe's positive control must fail: app_ro cannot reach a "
            "relation it is registered for once schema USAGE is gone"
        )
        assert not probe["okg_mcp_can_read_new_substrate_table"], (
            "the negative half must still hold"
        )


@pytest.mark.postgres
@pytest.mark.slow
def test_an_order_independent_public_grant_turns_the_gate_red(
    okg_dsn: str,
) -> None:
    """Commutativity is necessary but cannot prove an absolute allow-list."""
    commute = _load_commutativity_module()
    report = commute.check_commutativity(
        okg_dsn, prefix="okg_gc_public", mutation="public-grant"
    )
    assert not report["ok"], (
        "the same PUBLIC leak in both script orders must fail the absolute "
        "registry-drift arm"
    )
    assert report["failing_sections"] == [], (
        "the fault is intentionally order-independent; a comparative diff "
        f"cannot see it, got {report['failing_sections']}"
    )
    for side, findings in report["registry_drift"].items():
        leaked = [
            row
            for row in findings
            if row[0] == "unregistered_grant" and row[1] == "okg.node_facts"
        ]
        assert leaked, f"PUBLIC leak was not named on {side}: {findings}"
