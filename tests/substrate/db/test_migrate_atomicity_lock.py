"""migrate() is atomic, serialized, and time-bounded.

Covers req.schema-migration-hardening.atomic-locked-apply
(`schema-migration-hardening`, audit F1/F2/F3):

  1. Single-commit contract — an injected failure AFTER the schema body and
     the Chronos ensures leaves NO committed DDL and NO ledger delta. Pre-fix,
     the Chronos ensure path committed mid-migrate
     (graph_store_internal/schema.py db.commit() sites), so a later failure
     stranded committed DDL with no ledger row: these tests are red without
     the commit-suppressed ensure connection.
  2. Serialization — two concurrent migrate() calls against one database both
     succeed, with exactly one ledger row per migration (the loser observes
     the winner's committed checksums under the advisory lock and no-ops).
  3. Bounded timeouts — a held conflicting lock (the advisory migration lock
     itself, or an ACCESS SHARE on a view the body drops) produces a typed
     `MigrationLockTimeout` after a clean rollback within the configured
     budget, never a wedge; a re-run after release succeeds.
"""
from __future__ import annotations

import threading
import time
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from okg.substrate.db.migrate import (
    MIGRATE_ADVISORY_LOCK_NAMESPACE,
    MIGRATE_LOCK_ATTEMPTS_ENV,
    MIGRATE_LOCK_TIMEOUT_ENV,
    migrate,
)
from okg.substrate.errors import MigrationLockTimeout, SubstrateError


# --------------------------------------------------------------------------
# Helpers.
# --------------------------------------------------------------------------


def _create_disposable_database(base_dsn: str, dbname: str) -> str:
    info = conninfo_to_dict(base_dsn)
    info["dbname"] = "postgres"
    maintenance_dsn = make_conninfo(**info)
    with psycopg.connect(
        maintenance_dsn, autocommit=True, connect_timeout=5,
    ) as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    info["dbname"] = dbname
    dsn = make_conninfo(**info)
    with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn, \
            conn.cursor() as cur:
        for extension in (
            "btree_gist",
            "timescaledb",
            "vector",
            "pg_textsearch",
            "pg_trgm",
            "fuzzystrmatch",
        ):
            cur.execute(
                sql.SQL("CREATE EXTENSION IF NOT EXISTS {}").format(
                    sql.Identifier(extension)
                )
            )
    return dsn


def _drop_disposable_database(base_dsn: str, dbname: str) -> None:
    info = conninfo_to_dict(base_dsn)
    info["dbname"] = "postgres"
    maintenance_dsn = make_conninfo(**info)
    with psycopg.connect(
        maintenance_dsn, autocommit=True, connect_timeout=5,
    ) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_terminate_backend(pid)
              FROM pg_stat_activity
             WHERE datname = %s
               AND pid <> pg_backend_pid()
            """,
            (dbname,),
        )
        cur.execute(
            sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(dbname))
        )


def _ledger_snapshot(dsn: str) -> dict[str, tuple]:
    """(checksum, applied_at, applied_via) per ledger row, or {} if absent."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('okg.schema_migrations')")
        if cur.fetchone()[0] is None:
            return {}
        cur.execute(
            """
            SELECT migration_id, checksum, applied_at,
                   result_metadata->>'applied_via'
              FROM okg.schema_migrations
            """
        )
        return {row[0]: (row[1], row[2], row[3]) for row in cur.fetchall()}


def _okg_schema_exists(dsn: str) -> bool:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'okg')"
        )
        return bool(cur.fetchone()[0])


def _regclass(dsn: str, relation: str):
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (relation,))
        return cur.fetchone()[0]


# --------------------------------------------------------------------------
# 1. Single-commit contract: injected failure -> no committed DDL, no ledger.
# --------------------------------------------------------------------------


@pytest.mark.postgres
def test_injected_failure_after_ensures_leaves_no_committed_ddl(
    okg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failure after the schema body + Chronos ensures rolls EVERYTHING
    back: no okg schema, no graph tables, no ledger. Red without the
    commit-suppressed ensure connection (the Chronos ensures used to commit
    mid-migrate, stranding committed DDL with no ledger row — audit F1).

    The injection point is the normalized-alias backfill, the last step
    inside the single transaction. It used to be the view install, which no
    longer belongs to the FIRST pass: while a declared multi-phase migration
    is pending, the read layer is deferred to the post-driver pass
    (phase-interval-narrow-split-migration), so a failure injected there
    would fire after pass one had legitimately committed and would be
    asserting about the wrong transaction.
    """
    from okg.substrate.catalog import aliases as aliases_mod

    dbname = f"okg_test_atomic_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        real_backfill = aliases_mod.backfill_normalized_aliases

        def _boom(*args, **kwargs):
            raise RuntimeError("injected failure after chronos ensures")

        monkeypatch.setattr(
            aliases_mod, "backfill_normalized_aliases", _boom
        )
        with pytest.raises(RuntimeError, match="injected failure"):
            migrate(dsn)

        # The whole transaction rolled back: nothing committed.
        assert _ledger_snapshot(dsn) == {}, (
            "ledger rows survived a failed migrate"
        )
        assert not _okg_schema_exists(dsn), (
            "okg schema (DDL) was committed by a failed migrate"
        )
        assert _regclass(dsn, "okg.graph_nodes") is None
        assert _regclass(dsn, "okg.graph_edges") is None

        # Recovery: the same process re-runs migrate successfully once the
        # injected failure is removed (process-level bootstrap caches must
        # not wedge the retry).
        monkeypatch.setattr(
            aliases_mod, "backfill_normalized_aliases", real_backfill
        )
        migrate(dsn)
        ledger = _ledger_snapshot(dsn)
        assert "0000_schema_snapshot" in ledger
        assert _regclass(dsn, "okg.graph_nodes") is not None
    finally:
        _drop_disposable_database(okg_dsn, dbname)


@pytest.mark.postgres
def test_injected_failure_on_converged_db_leaves_no_ledger_delta(
    okg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On an already-converged database, a failure late in the migrate body
    leaves the ledger byte-identical (no applied_at bump, no checksum
    change) and the re-run succeeds."""
    import okg.substrate.catalog.aliases as aliases_mod

    dbname = f"okg_test_atomic_conv_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        before = _ledger_snapshot(dsn)
        assert before, "converged database has ledger rows"

        def _boom(conn):
            raise RuntimeError("injected failure before the single commit")

        monkeypatch.setattr(aliases_mod, "backfill_normalized_aliases", _boom)
        with pytest.raises(RuntimeError, match="injected failure"):
            migrate(dsn)
        assert _ledger_snapshot(dsn) == before, (
            "a failed migrate mutated the ledger"
        )

        monkeypatch.undo()
        migrate(dsn)
        assert _ledger_snapshot(dsn)["0000_schema_snapshot"] == (
            before["0000_schema_snapshot"]
        ), "a converged re-run replayed the schema body"
    finally:
        _drop_disposable_database(okg_dsn, dbname)


# --------------------------------------------------------------------------
# 2. Serialization: two concurrent migrates, one applier, one clean no-op.
# --------------------------------------------------------------------------


@pytest.mark.postgres
def test_concurrent_migrates_serialize_to_one_ledger_row_each(
    okg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dbname = f"okg_test_race_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    # The loser waits for the winner's WHOLE fresh-install body under the
    # advisory lock; give the wait a generous bound so the test asserts
    # serialization, not scheduling luck.
    monkeypatch.setenv(MIGRATE_LOCK_TIMEOUT_ENV, "300000")
    errors: list[Exception] = []

    def _run() -> None:
        try:
            migrate(dsn)
        except Exception as exc:  # noqa: BLE001 - collected for assertion
            errors.append(exc)

    try:
        threads = [threading.Thread(target=_run) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=600)
            assert not thread.is_alive(), "a concurrent migrate wedged"
        assert errors == [], (
            f"a concurrent migrate failed instead of no-opping: {errors!r}"
        )

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT migration_id, count(*)
                  FROM okg.schema_migrations
                 GROUP BY migration_id
                HAVING count(*) <> 1
                """
            )
            assert cur.fetchall() == []
            cur.execute(
                "SELECT count(*) FROM okg.schema_migrations "
                "WHERE migration_id = '0000_schema_snapshot'"
            )
            assert cur.fetchone()[0] == 1
            # Every ordered migration executed exactly once, via the runner.
            cur.execute(
                """
                SELECT count(*) FROM okg.schema_migrations
                 WHERE result_metadata->>'applied_via' = 'ordered_runner'
                """
            )
            assert cur.fetchone()[0] >= 1
    finally:
        _drop_disposable_database(okg_dsn, dbname)


# --------------------------------------------------------------------------
# 3. Bounded timeouts: held locks -> typed, clean-rollback error, then a
#    successful re-run. Never a wedge.
# --------------------------------------------------------------------------


@pytest.mark.postgres
def test_held_advisory_lock_yields_typed_timeout_then_rerun_succeeds(
    okg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dbname = f"okg_test_lockwait_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        before = _ledger_snapshot(dsn)

        monkeypatch.setenv(MIGRATE_LOCK_TIMEOUT_ENV, "300")
        monkeypatch.setenv(MIGRATE_LOCK_ATTEMPTS_ENV, "2")
        blocker = psycopg.connect(dsn)
        try:
            blocker.execute(
                "SELECT pg_advisory_xact_lock("
                "hashtextextended(%s || current_database(), 0))",
                (MIGRATE_ADVISORY_LOCK_NAMESPACE,),
            )
            started = time.monotonic()
            with pytest.raises(MigrationLockTimeout) as excinfo:
                migrate(dsn)
            elapsed = time.monotonic() - started
        finally:
            blocker.rollback()
            blocker.close()

        assert excinfo.value.data["attempts"] == 2
        assert excinfo.value.data["lock_timeout_ms"] == 300
        # Bounded: 2 attempts x 300ms + backoff + converged-body overhead.
        assert elapsed < 60, f"timeout was not bounded ({elapsed:.1f}s)"
        assert _ledger_snapshot(dsn) == before, (
            "a timed-out migrate mutated the ledger"
        )

        # The lock is released: a re-run succeeds.
        migrate(dsn)
    finally:
        _drop_disposable_database(okg_dsn, dbname)


@pytest.mark.postgres
def test_held_access_share_blocker_yields_typed_timeout_not_a_wedge(
    okg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reader holding ACCESS SHARE on okg.v_generations (which the migrate
    body drops and recreates) used to wedge migrate indefinitely (audit F3).
    Now: typed MigrationLockTimeout, clean rollback, successful re-run."""
    dbname = f"okg_test_reader_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        before = _ledger_snapshot(dsn)

        monkeypatch.setenv(MIGRATE_LOCK_TIMEOUT_ENV, "300")
        monkeypatch.setenv(MIGRATE_LOCK_ATTEMPTS_ENV, "2")
        blocker = psycopg.connect(dsn)
        try:
            # Open transaction holding ACCESS SHARE on the view.
            blocker.execute("SELECT * FROM okg.v_generations LIMIT 1")
            with pytest.raises(MigrationLockTimeout):
                migrate(dsn)
        finally:
            blocker.rollback()
            blocker.close()

        assert _ledger_snapshot(dsn) == before
        assert _regclass(dsn, "okg.v_generations") is not None, (
            "the timed-out migrate left the view dropped (partial rollback)"
        )
        migrate(dsn)
    finally:
        _drop_disposable_database(okg_dsn, dbname)


# --------------------------------------------------------------------------
# 4. Env knobs fail loudly (P0 rule: no silent default on a set-but-invalid
#    value). Hermetic: raises before any connection.
# --------------------------------------------------------------------------


def test_invalid_timeout_env_is_refused_not_defaulted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(MIGRATE_LOCK_TIMEOUT_ENV, "banana")
    with pytest.raises(SubstrateError, match="positive integer"):
        migrate("dbname=okg_test_nonexistent host=127.0.0.1 port=5433")


def test_swallowed_rollback_in_ensure_path_fails_loud() -> None:
    """A chronos helper that rolls back migrate's transaction and CONTINUES
    (rollback-and-retry loops do this) must convert into a typed refusal at
    the post-ensure check, never a silently torn commit."""
    from okg.substrate.db.migrate import _CommitSuppressedConnection

    class _StubConn:
        def __init__(self) -> None:
            self.rolled_back = 0

        def rollback(self) -> None:
            self.rolled_back += 1

    stub = _StubConn()
    proxy = _CommitSuppressedConnection(stub)  # type: ignore[arg-type]
    proxy.assert_no_swallowed_rollback()  # clean path: no raise
    proxy.commit()
    assert proxy.suppressed_commits == 1

    proxy.rollback()
    assert stub.rolled_back == 1, "rollback must still reach the real conn"
    with pytest.raises(SubstrateError, match="rolled back"):
        proxy.assert_no_swallowed_rollback()


def test_nonpositive_attempts_env_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(MIGRATE_LOCK_ATTEMPTS_ENV, "0")
    with pytest.raises(SubstrateError, match="positive integer"):
        migrate("dbname=okg_test_nonexistent host=127.0.0.1 port=5433")
