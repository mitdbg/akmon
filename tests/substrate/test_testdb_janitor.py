"""Witnesses for the test-database janitor (`okg db gc`, gc_sweep).

bound-host-storage-pressure req.host.workspace-hygiene, artifact class 2:
the sweep drops ONLY databases matching the exact allowlisted prefixes
(okg_test_, okg_upgrade_verify_, okg_golden) on an explicit instance DSN,
always reports per-database sizes, and never touches a non-allowlisted
database — a live-named database (okg_workspace) present on the same
instance survives, and its presence does not abort the allowlisted drops.

The live test creates and drops ONLY its own allowlist-prefixed scratch
databases on the OKG_TEST_DSN instance.
"""
from __future__ import annotations

from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from tests.substrate.conftest import dsn_with_dbname, require_okg_test_dsn

from okg.substrate.db.management import (
    TEST_DATABASE_GC_ALLOWLIST_PREFIXES,
    allowlisted_test_databases,
    gc_sweep,
    stamp_ephemeral_database,
)

pytestmark = [pytest.mark.postgres, pytest.mark.serial_db]


def test_allowlist_prefix_matching_is_exact() -> None:
    names = [
        "okg_test_hygiene",       # allowlisted (okg_test_ prefix)
        "okg_test",               # bare default dev DB — survives
        "okg_testdata",           # missing underscore — survives
        "okg_upgrade_verify_ab12",  # allowlisted
        "okg_upgrade_verifyX",    # missing underscore — survives
        "okg_golden",             # allowlisted (exact prefix)
        "okg_golden_path",        # allowlisted
        "okg_workspace",          # live deployment — survives
        "postgres",               # maintenance — survives
        "template1",
    ]
    assert allowlisted_test_databases(names) == [
        "okg_golden",
        "okg_golden_path",
        "okg_test_hygiene",
        "okg_upgrade_verify_ab12",
    ]
    # The allowlist itself is the ratified triple — a drift here is a
    # deliberate policy change, not a refactor.
    assert TEST_DATABASE_GC_ALLOWLIST_PREFIXES == (
        "okg_test_",
        "okg_upgrade_verify_",
        "okg_golden",
    )


def _maintenance_dsn() -> str:
    # This sweep DROPs databases, so it must never guess at an instance:
    # require_okg_test_dsn skips when nothing is configured (issue #1913).
    return dsn_with_dbname(require_okg_test_dsn(), "postgres")


def _database_names(dsn: str) -> set[str]:
    with psycopg.connect(dsn, connect_timeout=5) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT datname FROM pg_database WHERE datistemplate = false"
        )
        return {row[0] for row in cur.fetchall()}


def _create_database(dsn: str, name: str, *, register: bool = True) -> None:
    with psycopg.connect(
        dsn, autocommit=True, connect_timeout=5
    ) as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    if register:
        with psycopg.connect(
            dsn_with_dbname(dsn, name),
            autocommit=True,
            connect_timeout=5,
        ) as conn:
            stamp_ephemeral_database(
                conn,
                registered_by="tests.substrate.test_testdb_janitor",
                purpose="janitor witness scratch database",
            )


def _drop_if_exists(dsn: str, name: str) -> None:
    with psycopg.connect(
        dsn, autocommit=True, connect_timeout=5
    ) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(name))
        )


def test_janitor_drops_only_allowlisted_and_live_db_survives() -> None:
    maintenance = _maintenance_dsn()
    try:
        _database_names(maintenance)  # reachability probe
    except psycopg.OperationalError as exc:
        pytest.skip(f"test Postgres instance unreachable: {exc}")

    scratch = [
        f"okg_test_janitor_{uuid4().hex[:10]}",
        f"okg_upgrade_verify_janitor_{uuid4().hex[:10]}",
    ]
    # THE operator-hardening witness: a database whose NAME matches the
    # allowlist but which carries NO ephemeral marker must survive --apply
    # and be reported. Prefixes filter candidates; the marker is authority.
    unregistered = f"okg_test_unregistered_{uuid4().hex[:10]}"
    try:
        for name in scratch:
            _create_database(maintenance, name)
        _create_database(maintenance, unregistered, register=False)

        before = _database_names(maintenance)
        assert set(scratch) <= before
        assert unregistered in before
        non_allowlisted_before = set(before) - set(
            allowlisted_test_databases(before)
        )
        # The maintenance DB is always a non-allowlisted survivor witness;
        # okg_workspace strengthens the witness when the instance hosts it.
        assert "postgres" in non_allowlisted_before
        live_named_present = "okg_workspace" in non_allowlisted_before

        # Dry-run (the default): candidates + sizes reported, nothing dropped.
        dry = gc_sweep(cluster_dsn=maintenance, apply=False)
        assert dry["dry_run"] is True
        assert dry["applied"] is False
        dry_candidates = {
            row["database"]: row["size_bytes"]
            for row in dry["allowlist_candidates"]
        }
        assert set(scratch) <= set(dry_candidates)
        assert all(size > 0 for size in dry_candidates.values())
        survivors_reported = {
            row["database"] for row in dry["non_allowlisted_survivors"]
        }
        assert non_allowlisted_before <= survivors_reported
        assert set(scratch) <= _database_names(maintenance)

        # Apply: only allowlist-prefixed databases are dropped; sizes are
        # reported before (candidates) and after (databases_after).
        applied = gc_sweep(cluster_dsn=maintenance, apply=True)
        assert applied["applied"] is True
        dropped = {row["database"] for row in applied["dropped"]}
        assert set(scratch) <= dropped
        assert dropped <= set(allowlisted_test_databases(dropped))
        # The unregistered prefix-match was NOT dropped and is loudly
        # reported with its reason.
        assert unregistered not in dropped
        unreg_reported = {
            row["database"]: row["reason"]
            for row in applied["unregistered_prefix_matches"]
        }
        assert unreg_reported.get(unregistered) == "no ephemeral marker"
        assert applied["freed_bytes"] > 0
        after_reported = {
            row["database"]: row["size_bytes"]
            for row in applied["databases_after"]
        }

        after = _database_names(maintenance)
        assert not (set(scratch) & after)
        # THE witnesses: every non-allowlisted database AND the
        # unregistered prefix-match survive the applied sweep.
        assert unregistered in after
        assert non_allowlisted_before <= after
        assert non_allowlisted_before <= set(after_reported)
        if live_named_present:
            assert "okg_workspace" in after
            assert after_reported["okg_workspace"] > 0
    finally:
        for name in scratch + [unregistered]:
            _drop_if_exists(maintenance, name)
