"""Privilege regression test for `harden-mcp-run-sql-safety`.

Asserts the binding contract: every relation readable by
`app_ro` is a view, and the substrate `okg.v_*` allow-list is
readable.

This is the load-bearing test for the default-deny privilege
model. Schema migrations that GRANT `app_ro` on a non-view
table fail this test loudly — the pre-`harden` state had five
non-view tables silently readable to `app_ro`, contradicting
the spec.

The previous SQL-script test (`tests/sql/roles/app_ro_read_only.sql`)
exercises the role from the agent's perspective (try to do
forbidden things, observe denials). This test exercises the
contract symbolically (inspect the `pg_class` × privilege
matrix). Both stay in the suite — they test the same property
two different ways.
"""
from __future__ import annotations

import psycopg
import pytest

from tests.substrate.conftest import require_okg_test_dsn


pytestmark = pytest.mark.postgres


def _dsn() -> str:
    """Resolve the test substrate DSN.

    Prefer ``OKG_TEST_DSN`` so this regression runs against a
    migrated test database, not an operator's live ``OKG_DSN``.
    """
    # No fallback to OKG_DSN (the operator's live deployment) and no
    # hardcoded default (issue #1913): skip when nothing is configured.
    return require_okg_test_dsn()


@pytest.fixture(scope="module")
def live_conn():
    """Module-scoped read-only connection. Skips gracefully if
    the test substrate DSN is unreachable — the test is
    meaningful only against a freshly migrated DB."""
    dsn = _dsn()
    try:
        conn = psycopg.connect(dsn, autocommit=True, connect_timeout=2)
    except Exception as e:
        pytest.skip(f"substrate DSN {dsn} unreachable: {e}")
    yield conn
    conn.close()


def test_app_ro_can_read_only_view_layer(live_conn):
    """For every relation in schema `okg`, `has_table_privilege`
    SHALL never be true for a physical table.

    Per `harden-mcp-run-sql-safety` task 1.4 acceptance gate.
    """
    conn = live_conn
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              c.relname,
              c.relkind,
              has_table_privilege('app_ro', c.oid, 'SELECT') AS can_read
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'okg'
              AND c.relkind IN ('r', 'v', 'p')
              -- Exclude TimescaleDB internal partition / chunk
              -- relations; they're not directly queryable. Their
              -- parent hypertable view is what app_ro sees.
              AND c.relname NOT LIKE '_hyper_%'
              AND c.relname NOT LIKE '_compressed_hypertable_%'
            ORDER BY c.relname
            """
        )
        rows = cur.fetchall()

    assert rows, "no relations found in schema okg — migrate ran?"

    leaks: list[tuple[str, str]] = []
    for relname, relkind, can_read in rows:
        if can_read and relkind != "v":
            leaks.append((relname, relkind))

    assert not leaks, (
        "app_ro privilege contract violated. Per "
        "harden-mcp-run-sql-safety, app_ro SHALL NOT have SELECT "
        "on physical tables. These readable relations are not "
        "views:\n"
        + "\n".join(
            f"  - okg.{relname}: relkind={relkind}"
            for relname, relkind in leaks
        )
        + "\n\n"
        "If a deployment needs an additional table queryable via "
        "run_sql, add a matching `okg` view in views.sql and "
        "GRANT SELECT on the view, not on the underlying table."
    )


def test_app_ro_grant_set_matches_the_registry(live_conn):
    """Symmetric assertion: every REGISTERED app_ro relation SHALL be readable.

    This used to assert that every `okg.v_*` view is readable, which encoded
    the naming convention `schema-grant-coherence` removed: under that rule any
    migration- or deployment-created view called `v_something` silently joined
    the published read surface. The registry (`okg.grant_registry`) is the
    allow-list now, so the symmetric property is registry-vs-catalog. A view
    with NO registry row is covered by the drift check, which names it —
    see tests/substrate/db/test_grant_surface_coherence.py.
    """
    conn = live_conn
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('okg.grant_registry') IS NOT NULL")
        if not cur.fetchone()[0]:
            pytest.skip(
                "okg.grant_registry absent: database predates migration "
                "0027_grant_registry"
            )
        cur.execute(
            """
            SELECT g.relation,
                   has_table_privilege('app_ro', to_regclass(g.relation),
                                       'SELECT') AS can_read
              FROM okg.grant_registry g
             WHERE g.role_name = 'app_ro'
               AND g.classification = 'contract'
               AND g.object_kind = 'TABLE'
               AND NOT g.is_pattern
               AND to_regclass(g.relation) IS NOT NULL
             ORDER BY g.relation
            """
        )
        rows = cur.fetchall()

    assert rows, (
        "no app_ro rows in okg.grant_registry — views.sql's seed did not run"
    )
    missing_grants = [relation for relation, can_read in rows if not can_read]
    assert not missing_grants, (
        "Some registered relations are not readable to app_ro:\n"
        + "\n".join(f"  - {r}" for r in missing_grants)
        + "\n\nRe-run `okg up` (views.sql applies okg.apply_grant_registry()), "
        "or add the missing row to views.sql's okg_grant_registry_seed block."
    )
