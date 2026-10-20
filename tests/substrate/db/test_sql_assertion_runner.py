from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import psycopg
import pytest

from okg.substrate.db.migrate import migrate


pytestmark = pytest.mark.postgres

ROOT = Path(__file__).resolve().parents[3]
RUNNER_PATH = ROOT / "scripts" / "run_sql_assertions.py"


def _load_runner():
    spec = importlib.util.spec_from_file_location("run_sql_assertions", RUNNER_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_sql_assertions"] = module
    spec.loader.exec_module(module)
    return module


RUNNER = _load_runner()


def test_sql_assertion_runner_executes_default_sql_files(
    okg_migrated: str,
) -> None:
    files = RUNNER.resolve_sql_files()

    result = RUNNER.run_assertions(dsn=okg_migrated, sql_files=files)

    assert result["ok"] is True, result["failures"]
    assert result["failures"] == []
    assert {
        path.relative_to(ROOT).as_posix() for path in files
    } == {
        "tests/sql/roles/app_ro_read_only.sql",
        "tests/sql/roles/app_rw_grants.sql",
        "tests/sql/roles/catalog_admin_grants.sql",
        "tests/sql/schema/schema_contracts.sql",
    }


@pytest.mark.xfail(
    reason=(
        "Asserts migrate() revokes app_rw catalog-write grants — the "
        "constrain-MCP-and-role-bootstrap behavior from PR #366 (F-04, "
        "candidate #5), not yet merged to dev. dev's migrate() still leaves the "
        "over-broad grants. Flips to xpass when #366 lands. Tracked by "
        "build-schema-quality-scorecard."
    ),
    strict=False,
)
def test_migrate_revokes_legacy_app_rw_catalog_write_grants(
    okg_migrated: str,
) -> None:
    catalog_tables = (
        "okg.subtypes",
        "okg.narrowings",
        "okg.aliases",
        "okg.invariants",
        "okg.identifier_patterns",
        "okg.catalog_versions",
    )
    stale_grant_sql = "GRANT INSERT, UPDATE, DELETE ON " + ", ".join(
        catalog_tables
    ) + " TO app_rw"

    try:
        with psycopg.connect(okg_migrated, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("GRANT CREATE ON SCHEMA okg TO app_rw")
                cur.execute(stale_grant_sql)
                cur.execute(
                    """
                    SELECT count(*)
                    FROM information_schema.role_table_grants
                    WHERE table_schema = 'okg'
                      AND grantee = 'app_rw'
                      AND table_name IN (
                        'subtypes', 'narrowings', 'aliases', 'invariants',
                        'identifier_patterns', 'catalog_versions'
                      )
                      AND privilege_type IN ('INSERT', 'UPDATE', 'DELETE')
                    """
                )
                assert cur.fetchone()[0] > 0
                cur.execute("SELECT has_schema_privilege('app_rw', 'okg', 'CREATE')")
                assert cur.fetchone()[0] is True

        migrate(okg_migrated)

        with psycopg.connect(okg_migrated) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT count(*)
                    FROM information_schema.role_table_grants
                    WHERE table_schema = 'okg'
                      AND grantee = 'app_rw'
                      AND table_name IN (
                        'subtypes', 'narrowings', 'aliases', 'invariants',
                        'identifier_patterns', 'catalog_versions'
                      )
                      AND privilege_type IN ('INSERT', 'UPDATE', 'DELETE')
                    """
                )
                assert cur.fetchone()[0] == 0
                cur.execute(
                    """
                    SELECT count(DISTINCT table_name)
                    FROM information_schema.role_table_grants
                    WHERE table_schema = 'okg'
                      AND grantee = 'app_rw'
                      AND table_name IN (
                        'subtypes', 'narrowings', 'aliases', 'invariants',
                        'identifier_patterns', 'catalog_versions'
                      )
                      AND privilege_type = 'SELECT'
                    """
                )
                assert cur.fetchone()[0] == len(catalog_tables)
                cur.execute("SELECT has_schema_privilege('app_rw', 'okg', 'CREATE')")
                assert cur.fetchone()[0] is False

        result = RUNNER.run_assertions(
            dsn=okg_migrated,
            sql_files=RUNNER.resolve_sql_files(),
        )

        assert result["ok"] is True
        assert result["failures"] == []
    finally:
        # migrate() intentionally does NOT revoke these stale grants — that is
        # the #366 active-revocation gap this xfail documents — so restore the
        # clean migrated state explicitly. Otherwise this test pollutes the
        # shared session DB and breaks sibling role-assertion tests.
        revoke_sql = "REVOKE INSERT, UPDATE, DELETE ON " + ", ".join(
            catalog_tables
        ) + " FROM app_rw"
        with psycopg.connect(okg_migrated, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(revoke_sql)
                cur.execute("REVOKE CREATE ON SCHEMA okg FROM app_rw")
