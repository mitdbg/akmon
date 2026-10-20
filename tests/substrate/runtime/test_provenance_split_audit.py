"""Regression tests for the provenance-split audit (BLOCKER A fix).

Phase 1 §2.10 review found that the ``information_schema``-based audit
had a blind spot: it only saw *direct* grants on the named role, so it
missed role-inheritance leaks (``GRANT okg_substrate TO okg_dbos``),
PUBLIC grants, and materialized default-privilege drift.

The fix rewrote the audit to use Postgres' effective-privilege
functions (``has_table_privilege`` / ``has_column_privilege``) plus an
explicit lookup in ``information_schema.table_privileges`` for PUBLIC.

These tests live-verify the fix against a real Postgres database. They
are marked ``dbos_integration`` because the role machinery requires a
Postgres instance; CI runs them on the nightly job, not on every PR.
"""
from __future__ import annotations


import contextlib
import psycopg
import pytest
from tests.substrate.runtime._helpers import _swap_dbname


pytestmark = pytest.mark.dbos_integration


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _clear_okg_role_memberships(dsn: str) -> None:
    """Revoke all role memberships involving the OKG service roles.

    Postgres roles are cluster-scoped — they persist across DROP
    DATABASE. A test that injects ``GRANT okg_substrate TO okg_dbos``
    would leak that membership into the next test. We can't safely
    DROP these roles cluster-wide because they may own objects in
    *other* databases (e.g. okg_dbos_phase1), so instead we enumerate
    pg_auth_members rows touching the service roles and REVOKE them.

    Also: drop the per-deployment role for this test if it exists,
    since its name (okg_deployment_test) is test-specific. This is
    safe because the per-deployment role only owns objects inside the
    test DB, which we're about to drop anyway.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Revoke memberships graphwise.
            cur.execute(
                """
                SELECT g.rolname AS granted, m.rolname AS member
                FROM pg_auth_members am
                JOIN pg_roles g ON g.oid = am.roleid
                JOIN pg_roles m ON m.oid = am.member
                WHERE g.rolname IN (
                    'okg_substrate', 'okg_dbos', 'okg_deployment_test'
                )
                   OR m.rolname IN (
                    'okg_substrate', 'okg_dbos', 'okg_deployment_test'
                )
                """
            )
            for granted, member in cur.fetchall():
                # Quote both names defensively (they're trusted but the
                # quoting is cheap).
                cur.execute(f'REVOKE "{granted}" FROM "{member}"')

            # Drop the per-deployment role (test-local; safe to drop).
            cur.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = 'okg_deployment_test'"
            )
            if cur.fetchone() is not None:
                # Best-effort: REASSIGN/DROP OWNED in this DB.
                cur.execute(
                    "REASSIGN OWNED BY okg_deployment_test TO postgres"
                )
                cur.execute("DROP OWNED BY okg_deployment_test")
                cur.execute("DROP ROLE okg_deployment_test")


@pytest.fixture
def test_db_dsn(admin_dsn: str) -> str:
    """Create a clean per-test database (and per-test role set) and
    yield its DSN.

    Postgres roles are cluster-scoped, so the fixture explicitly drops
    okg_substrate / okg_dbos / okg_deployment_test in both setup and
    teardown — otherwise a ``GRANT okg_substrate TO okg_dbos`` from one
    test would leak into the next.
    """
    db_name = "okg_audit_test"
    dsn = _swap_dbname(admin_dsn, db_name)

    def _drop_db() -> None:
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_terminate_backend(pid) "
                    "FROM pg_stat_activity "
                    "WHERE datname = %s AND pid <> pg_backend_pid()",
                    (db_name,),
                )
                cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')

    def _create_db() -> None:
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f'CREATE DATABASE "{db_name}"')

    # Setup: fresh DB. Roles cleaned after CREATE so we have a DB to
    # run REASSIGN OWNED against.
    _drop_db()
    _create_db()
    _clear_okg_role_memberships(dsn)

    yield dsn

    # Teardown: clear test-local role state (memberships + the
    # per-deployment role) before dropping the DB. Best-effort: if a
    # test left the DB in a state where REASSIGN fails, the DROP
    # DATABASE below is the safety net.
    with contextlib.suppress(psycopg.Error):
        _clear_okg_role_memberships(dsn)
    _drop_db()


def _build_stub_schemas(dsn: str) -> None:
    """Materialize the OKG schema-and-table shape the audit inspects.

    Installs ``btree_gist`` because Phase 2's ``escalation_schema.sql``
    (which ``apply_role_split`` invokes) adds a GIST exclusion over
    ``(text escalation_id, int8range generation_window)`` to
    ``okg.escalation_live``. In production ``btree_gist`` is installed
    by ``okg migrate`` before the runtime bootstrap; in tests we
    install it directly because we skip migrate.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
            cur.execute("CREATE SCHEMA okg")
            cur.execute("CREATE SCHEMA okg_derived")
            cur.execute("CREATE SCHEMA dbos")
            for tbl in (
                "graph_generations",
                "source_watermarks",
                "narrowings",
                "catalog_versions",
            ):
                cur.execute(
                    f"CREATE TABLE okg.{tbl} (id bigserial PRIMARY KEY, "
                    "attrs jsonb)"
                )
            cur.execute(
                "CREATE TABLE okg.node_facts ("
                "id bigserial PRIMARY KEY, attrs jsonb)"
            )
            cur.execute(
                "CREATE TABLE okg.edge_facts ("
                "id bigserial PRIMARY KEY, attrs jsonb)"
            )
            # NOTE: this is a self-contained STUB schema the audit/role-split
            # logic inspects in isolation — not the migrated substrate schema.
            # role_split.sql still GRANT/REVOKEs UPDATE on okg.nodes_live /
            # okg.edges_live unconditionally (its cutover cleanup is a tracked
            # source-side remainder), so the stub keeps those two tables for
            # apply_role_split to run. The column-level UPDATE audit (Check 4)
            # itself was retargeted by the cutover to okg.escalation_live; see
            # test_audit_catches_broad_column_grant_on_escalation_live.
            cur.execute(
                "CREATE TABLE okg.nodes_live ("
                "node_id text PRIMARY KEY, attrs jsonb, "
                "valid_from_gen bigint, valid_to_gen bigint)"
            )
            cur.execute(
                "CREATE TABLE okg.edges_live ("
                "edge_id text PRIMARY KEY, attrs jsonb, "
                "valid_from_gen bigint, valid_to_gen bigint)"
            )
            cur.execute(
                "CREATE TABLE dbos.workflow_status ("
                "workflow_uuid text PRIMARY KEY, status text)"
            )


def _build_chronos_stub_schemas(dsn: str) -> None:
    """Materialize the Chronos-era schema shape without legacy live tables."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
            cur.execute("CREATE SCHEMA okg")
            cur.execute("CREATE SCHEMA okg_derived")
            cur.execute("CREATE SCHEMA dbos")
            for tbl in (
                "graph_generations",
                "source_watermarks",
                "narrowings",
                "catalog_versions",
            ):
                cur.execute(
                    f"CREATE TABLE okg.{tbl} (id bigserial PRIMARY KEY, "
                    "attrs jsonb)"
                )
            cur.execute(
                "CREATE TABLE okg.node_facts ("
                "id bigserial PRIMARY KEY, attrs jsonb)"
            )
            cur.execute(
                "CREATE TABLE okg.edge_facts ("
                "id bigserial PRIMARY KEY, attrs jsonb)"
            )
            cur.execute(
                "CREATE TABLE okg.graph_nodes ("
                "node_id text PRIMARY KEY, subtype text, attrs jsonb)"
            )
            cur.execute(
                "CREATE TABLE okg.graph_edges ("
                "edge_id text PRIMARY KEY, src text, dst text, "
                "edge_type text, attrs jsonb)"
            )
            cur.execute(
                "CREATE TABLE dbos.workflow_status ("
                "workflow_uuid text PRIMARY KEY, status text)"
            )


def _apply_role_split(dsn: str, *, deployment: str = "test") -> None:
    from okg.substrate.runtime.bootstrap import apply_role_split

    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_role_split(conn, deployment=deployment, dbos_schema="dbos")


def _audit(dsn: str, *, deployment: str = "test"):
    from okg.substrate.runtime.audit import audit_provenance_split

    return audit_provenance_split(deployment=deployment, dsn=dsn)


def _exec(dsn: str, sql: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_audit_clean_on_freshly_applied_role_split(test_db_dsn: str) -> None:
    """Baseline: a freshly-applied role split produces no audit
    violations."""
    _build_stub_schemas(test_db_dsn)
    _apply_role_split(test_db_dsn)

    verdict = _audit(test_db_dsn)

    assert verdict.ok, (
        f"baseline audit should pass; got violations: {verdict.violations}"
    )
    assert verdict.violations == []


def test_role_split_applies_without_legacy_live_tables(
    test_db_dsn: str,
) -> None:
    """Chronos-native deployments dropped nodes_live/edges_live.

    Runtime bootstrap must still converge the role split and marker instead
    of requiring retired bitemporal tables.
    """
    _build_chronos_stub_schemas(test_db_dsn)
    _apply_role_split(test_db_dsn)

    with psycopg.connect(test_db_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT to_regclass('okg.nodes_live'),
                   to_regclass('okg.edges_live'),
                   to_regclass('okg.role_split_marker')
            """
        )
        nodes_live, edges_live, marker = cur.fetchone()
        cur.execute(
            """
            SELECT has_table_privilege(
                       'okg_deployment_test',
                       'okg.graph_nodes',
                       'UPDATE'
                   )
            """
        )
        can_update_graph_nodes = bool(cur.fetchone()[0])

    assert nodes_live is None
    assert edges_live is None
    assert marker == "okg.role_split_marker"
    assert can_update_graph_nodes is False


def test_audit_catches_public_grant(test_db_dsn: str) -> None:
    """PUBLIC grant on okg.* must be caught.

    Regression test for BLOCKER A: ``information_schema.role_table_grants``
    with ``grantee LIKE 'okg_deployment_%'`` skipped grantee='PUBLIC'.
    The new audit queries ``information_schema.table_privileges`` for
    PUBLIC and via ``has_table_privilege`` for effective privileges
    (PUBLIC grants raise every role's effective privilege).
    """
    _build_stub_schemas(test_db_dsn)
    _apply_role_split(test_db_dsn)
    _exec(test_db_dsn, "GRANT INSERT ON okg.nodes_live TO PUBLIC")

    verdict = _audit(test_db_dsn)

    assert not verdict.ok, "PUBLIC drift should fail the audit"
    # Must include the explicit PUBLIC violation.
    assert any("PUBLIC" in v for v in verdict.violations), (
        f"audit must flag PUBLIC explicitly; got: {verdict.violations}"
    )


def test_audit_catches_role_inheritance_leak(test_db_dsn: str) -> None:
    """``GRANT okg_substrate TO okg_dbos`` gives ``okg_dbos`` effective
    writes on OKG schemas via inheritance. The old audit missed this
    because ``information_schema.role_table_grants`` records the grant
    under ``okg_substrate``, not ``okg_dbos``.

    The new audit uses ``has_table_privilege('okg_dbos', ...)`` which
    walks the role hierarchy and surfaces the effective leak.
    """
    _build_stub_schemas(test_db_dsn)
    _apply_role_split(test_db_dsn)
    _exec(test_db_dsn, "GRANT okg_substrate TO okg_dbos")

    verdict = _audit(test_db_dsn)

    assert not verdict.ok, "role-inheritance drift should fail the audit"
    # Must report effective writes on OKG tables under okg_dbos's name.
    assert any(
        "okg_dbos" in v and "effective" in v for v in verdict.violations
    ), f"audit must flag okg_dbos's effective writes; got: {verdict.violations}"


def test_audit_catches_broad_column_grant_on_escalation_live(
    test_db_dsn: str,
) -> None:
    """A deployment role with UPDATE on any column of escalation_live
    other than ``valid_to_gen`` violates the column-level grant policy.
    Uses ``has_column_privilege`` for the check.

    The chronos-native cutover dropped okg.nodes_live / okg.edges_live;
    the column-level UPDATE audit (Check 4) now targets the surviving
    okg.escalation_live, which apply_role_split creates with the same
    valid_to_gen-only column-grant contract."""
    _build_stub_schemas(test_db_dsn)
    _apply_role_split(test_db_dsn)
    _exec(
        test_db_dsn,
        "GRANT UPDATE (attrs) ON okg.escalation_live TO okg_deployment_test",
    )

    verdict = _audit(test_db_dsn)

    assert not verdict.ok, "column-grant drift should fail the audit"
    assert any(
        "attrs" in v and "escalation_live" in v for v in verdict.violations
    ), f"audit must flag attrs-column UPDATE drift; got: {verdict.violations}"


def test_role_split_reapply_converges_drift(test_db_dsn: str) -> None:
    """Re-applying role_split.sql after injected drift must converge
    the audit to OK. Regression test for the BLOCKER A companion fix:
    role_split.sql gained REVOKE statements that fire on every apply,
    so a drifted DB heals on re-bootstrap."""
    _build_stub_schemas(test_db_dsn)
    _apply_role_split(test_db_dsn)

    # Inject drift: PUBLIC grant.
    _exec(test_db_dsn, "GRANT INSERT ON okg.nodes_live TO PUBLIC")
    pre_recovery = _audit(test_db_dsn)
    assert not pre_recovery.ok, (
        "audit must see the injected PUBLIC drift before recovery"
    )

    # Re-apply role_split.sql.
    _apply_role_split(test_db_dsn)

    post_recovery = _audit(test_db_dsn)
    assert post_recovery.ok, (
        f"role_split.sql re-apply must converge audit to OK; "
        f"remaining violations: {post_recovery.violations}"
    )


def test_audit_emits_diagnostics_for_service_role_memberships(
    test_db_dsn: str,
) -> None:
    """Service-role memberships are diagnostics, not violations on
    their own — they help operators diagnose where a leak came from."""
    _build_stub_schemas(test_db_dsn)
    _apply_role_split(test_db_dsn)
    _exec(test_db_dsn, "GRANT okg_substrate TO okg_dbos")

    verdict = _audit(test_db_dsn)

    # Diagnostics surface the membership.
    assert any(
        "okg_dbos" in d and "okg_substrate" in d
        for d in verdict.diagnostics
    ), (
        f"diagnostics must surface the okg_dbos -> okg_substrate "
        f"membership; got: {verdict.diagnostics}"
    )
