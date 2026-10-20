"""PACT phase-interval-narrow-split-migration: executed proof (#1143/#1114).

The original 0020 was one atomic DO block that never completed on a real
database. These tests prove the rephased shape end to end on real Postgres:

- req.multiphase-0020 — a populated wide-shape database converges to the
  narrow shape THROUGH migrate()'s orchestration (driver, not inline), with
  full hash parity, validated FKs, preserved row counts, and a ledger row
  recorded by the multi-phase runner; a SIGKILL mid-backfill leaves a
  labelled, resumable state and the resumed run converges identically; an
  already-narrow database is a fast no-op.
- req.migrate-orchestration — ordinals behind pending 0020 are deferred and
  applied by the second ordered pass (NNNN order preserved); the schema
  ensure refuses a POPULATED wide database with a typed error (SQLSTATE
  OKG20) and zero data motion, converges an EMPTY wide database inline, and
  tolerates the wide shape only under migrate's explicit flag; a ledger row
  recorded by the old atomic 0020 is accepted through the audited checksum
  compatibility.

The wide fixture is derived by DEVOLVING a freshly migrated database
(drop the FK, re-add attrs/raw_attrs, drop attrs_hash, delete the ledger
row) — the inverse of exactly the columns/constraints 0020 touches — so the
fixture's provenance is the current schema, not a hand-written guess.
"""
from __future__ import annotations

import subprocess
import sys
import uuid
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg import sql  # noqa: E402
from psycopg.conninfo import conninfo_to_dict, make_conninfo  # noqa: E402
from psycopg.types.json import Jsonb  # noqa: E402

from okg.substrate.attributes import ensure_attribute_schema  # noqa: E402
from okg.substrate.db.interval_narrow_split_migration import (  # noqa: E402
    MIGRATION_ID,
    build_spec,
)
from okg.substrate.db.management import (  # noqa: E402
    current_known_migrations,
    migration_status,
)
from okg.substrate.db.migrate import migrate  # noqa: E402
from okg.substrate.db.multiphase import run_multiphase_migration  # noqa: E402

pytestmark = pytest.mark.postgres

_VIEW = "okg.attribute_effective_view"
_INTERVALS = "okg.attribute_effective_view_intervals"
_VERSIONS = "okg.attribute_versions"

_OLD_ATOMIC_0020_CHECKSUM = (
    "a1cfd4ca74a6bb96b5ae4ecf01b200019293aac441925b86f5d6b28ca87f9968"
)


def _create_disposable_database(base_dsn: str, dbname: str) -> str:
    info = conninfo_to_dict(base_dsn)
    info["dbname"] = "postgres"
    with psycopg.connect(
        make_conninfo(**info), autocommit=True, connect_timeout=5
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
    with psycopg.connect(
        make_conninfo(**info), autocommit=True, connect_timeout=5
    ) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_terminate_backend(pid)
              FROM pg_stat_activity
             WHERE datname = %s AND pid <> pg_backend_pid()
            """,
            (dbname,),
        )
        cur.execute(
            sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(dbname))
        )


@pytest.fixture()
def scratch_dsn(okg_dsn: str):
    dbname = f"okg_mp0020_{uuid.uuid4().hex[:10]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        yield dsn
    finally:
        _drop_disposable_database(okg_dsn, dbname)


def _devolve_to_wide(
    dsn: str, *, also_unapply: tuple[str, ...] = ()
) -> None:
    """Reverse exactly what 0020 does: back to the pre-0020 wide shape.

    Existing narrow rows get their payloads copied BACK from the version
    store (the true inverse) before the store is truncated, so a database
    with published data devolves to a faithful populated wide shape.
    """
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        for table, check in (
            (_VIEW, "attribute_effective_view_version_fkey"),
            (_INTERVALS, "attribute_effective_view_intervals_version_fkey"),
        ):
            cur.execute(
                f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {check}"
            )
            cur.execute(
                f"ALTER TABLE {table} "
                "ADD COLUMN attrs JSONB, ADD COLUMN raw_attrs JSONB"
            )
            cur.execute(
                f"""
                UPDATE {table} AS t
                   SET attrs = v.attrs, raw_attrs = v.raw_attrs
                  FROM {_VERSIONS} v
                 WHERE v.target_kind = t.target_kind
                   AND v.target_id = t.target_id
                   AND v.attrs_hash = t.attrs_hash
                """
            )
            cur.execute(f"ALTER TABLE {table} DROP COLUMN attrs_hash")
        # The dead jsonb GIN index a real pre-0020 database carries; the
        # migration must retire it (recorded, never rebuilt) rather than
        # pay per-row maintenance on it during the backfill.
        cur.execute(
            "CREATE INDEX attribute_effective_view_attrs_gin_idx "
            f"ON {_VIEW} USING gin (attrs)"
        )
        cur.execute("TRUNCATE okg.attribute_versions")
        cur.execute(
            "DELETE FROM okg.schema_migrations WHERE migration_id = ANY(%s)",
            ([MIGRATION_ID, *also_unapply],),
        )
        cur.execute(
            "DELETE FROM okg.schema_migration_phases WHERE migration_id = %s",
            (MIGRATION_ID,),
        )
        conn.commit()


def _seed_wide_rows(dsn: str, *, targets: int) -> None:
    """Rows covering the payload cases: distinct raw, raw == attrs, raw NULL."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        for i in range(targets):
            attrs = {"name": f"target {i}", "rank": i, "nested": {"k": [i, i + 1]}}
            if i % 3 == 0:
                raw = None
            elif i % 3 == 1:
                raw = dict(attrs)  # legacy raw == attrs duplicate
            else:
                raw = {**attrs, "raw_only": True}
            cur.execute(
                f"""
                INSERT INTO {_VIEW}
                  (target_kind, target_id, generation_id, namespace,
                   subtype, overlay_count, attrs, raw_attrs)
                VALUES ('node', %s, 'okg:test:gen:1', 'default',
                        'doc', 0, %s, %s)
                """,
                (f"doc:{i}", Jsonb(attrs), Jsonb(raw) if raw else None),
            )
            cur.execute(
                f"""
                INSERT INTO {_INTERVALS}
                  (target_kind, target_id, namespace, branch_id, from_ord,
                   to_ord, subtype, overlay_count, attrs, raw_attrs)
                VALUES ('node', %s, 'default', 'branch-1', 1,
                        9223372036854775807, 'doc', 0, %s, %s)
                """,
                (f"doc:{i}", Jsonb(attrs), Jsonb(raw) if raw else None),
            )
        conn.commit()


def _assert_converged(dsn: str, *, view_rows: int, interval_rows: int) -> None:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        for table, count in ((_VIEW, view_rows), (_INTERVALS, interval_rows)):
            name = table.split(".")[1]
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                 WHERE table_schema = 'okg' AND table_name = %s
                """,
                (name,),
            )
            columns = {row[0] for row in cur.fetchall()}
            assert "attrs" not in columns and "raw_attrs" not in columns, table
            assert "attrs_hash" in columns, table
            cur.execute(f"SELECT count(*) FROM {table}")
            assert cur.fetchone()[0] == count, table
            # NOT NULL survived; the helper check did not (fresh == upgraded).
            cur.execute(
                """
                SELECT is_nullable FROM information_schema.columns
                 WHERE table_schema = 'okg' AND table_name = %s
                   AND column_name = 'attrs_hash'
                """,
                (name,),
            )
            assert cur.fetchone()[0] == "NO", table
            cur.execute(
                "SELECT count(*) FROM pg_constraint "
                "WHERE conrelid = %s::regclass AND contype = 'c' "
                "AND conname LIKE '%%attrs_hash_not_null%%'",
                (table,),
            )
            assert cur.fetchone()[0] == 0, f"helper check left behind on {table}"
            cur.execute(
                "SELECT convalidated FROM pg_constraint "
                "WHERE conrelid = %s::regclass AND contype = 'f'",
                (table,),
            )
            validated = [row[0] for row in cur.fetchall()]
            assert validated and all(validated), (table, validated)
        # FULL hash parity, both directions, via one aggregate scan per table:
        # every row's stored hash equals the one recomputed from the version
        # store payload, and every referenced version row exists.
        for table in (_VIEW, _INTERVALS):
            cur.execute(
                f"""
                SELECT count(*) FROM {table} t
                  JOIN {_VERSIONS} v
                    ON v.target_kind = t.target_kind
                   AND v.target_id = t.target_id
                   AND v.attrs_hash = t.attrs_hash
                 WHERE okg.attribute_content_hash(v.attrs, v.raw_attrs)
                       IS DISTINCT FROM t.attrs_hash
                """
            )
            assert cur.fetchone()[0] == 0, f"hash parity violated on {table}"
        # Legacy raw == attrs duplicates were normalized to NULL (presence
        # means overlay), never stored as a redundant copy.
        cur.execute(
            f"SELECT count(*) FROM {_VERSIONS} WHERE raw_attrs = attrs"
        )
        assert cur.fetchone()[0] == 0
        # Secondary indexes were rebuilt after the backfill; the dead jsonb
        # GIN index was retired (its column is gone), not recreated.
        cur.execute(
            "SELECT c.relname FROM pg_index i "
            "JOIN pg_class c ON c.oid = i.indexrelid "
            f"WHERE i.indrelid = '{_VIEW}'::regclass"
        )
        view_indexes = {row[0] for row in cur.fetchall()}
        assert "attribute_effective_view_node_subtype_idx" in view_indexes
        assert "attribute_effective_view_edge_type_idx" in view_indexes
        assert "attribute_effective_view_attrs_gin_idx" not in view_indexes
        # The autovacuum pause was reset on every involved table.
        for table in (_VIEW, _INTERVALS, _VERSIONS):
            cur.execute(
                "SELECT COALESCE(reloptions::text, '') FROM pg_class "
                "WHERE oid = %s::regclass",
                (table,),
            )
            assert "autovacuum_enabled" not in cur.fetchone()[0], table


def _ledger_row(dsn: str, migration_id: str):
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status, result_metadata FROM okg.schema_migrations "
            "WHERE migration_id = %s",
            (migration_id,),
        )
        return cur.fetchone()


def test_populated_wide_database_converges_via_migrate(scratch_dsn: str) -> None:
    migrate(scratch_dsn)
    _devolve_to_wide(scratch_dsn)
    _seed_wide_rows(scratch_dsn, targets=120)

    result = migrate(scratch_dsn)

    assert result.pending_multiphase == ()
    assert [row["migration_id"] for row in result.multiphase_applied] == [
        MIGRATION_ID
    ]
    assert result.multiphase_applied[0]["status"] == "applied"
    phases = result.multiphase_applied[0]["phases"]
    # The backfill did batched work; every phase has a recorded count.
    assert phases["backfill_attribute_effective_view"] >= 1
    assert phases["backfill_attribute_effective_view_intervals"] >= 1
    _assert_converged(scratch_dsn, view_rows=120, interval_rows=120)
    row = _ledger_row(scratch_dsn, MIGRATION_ID)
    assert row is not None and row[0] == "applied"
    assert row[1]["applied_via"] == "multiphase_runner"
    status = migration_status(scratch_dsn)
    assert status["multiphase_pending"] == []
    assert status["multiphase_in_progress"] == []
    assert status["multiphase_complete_unrecorded"] == []


def test_barrier_defers_post_0020_ordinals_to_second_pass(
    scratch_dsn: str,
) -> None:
    migrate(scratch_dsn)
    later = [
        migration.migration_id
        for migration in current_known_migrations()
        if migration.migration_id > MIGRATION_ID
    ]
    assert later, "expected ordinals after 0020 in the shipped tree"
    _devolve_to_wide(scratch_dsn, also_unapply=tuple(later))
    _seed_wide_rows(scratch_dsn, targets=30)

    result = migrate(scratch_dsn)

    # Pass 1 deferred every ordinal behind the pending 0020 (NNNN order);
    # the driver + second pass applied all of them.
    assert list(result.deferred_post_multiphase) == sorted(later)
    assert [row["migration_id"] for row in result.multiphase_applied] == [
        MIGRATION_ID
    ]
    _assert_converged(scratch_dsn, view_rows=30, interval_rows=30)
    for migration_id in later:
        row = _ledger_row(scratch_dsn, migration_id)
        assert row is not None and row[0] == "applied", migration_id
    assert migration_status(scratch_dsn)["pending"] == []


def test_narrow_database_driver_run_is_noop_already_applied(
    scratch_dsn: str,
) -> None:
    migrate(scratch_dsn)  # fresh: orchestration already ran the driver
    row = _ledger_row(scratch_dsn, MIGRATION_ID)
    assert row is not None and row[1]["applied_via"] == "multiphase_runner"
    migration = next(
        m
        for m in current_known_migrations()
        if m.migration_id == MIGRATION_ID
    )
    outcome = run_multiphase_migration(scratch_dsn, migration, build_spec())
    assert outcome["status"] == "already_applied"


def test_ensure_refuses_populated_wide_and_converges_empty_wide(
    scratch_dsn: str,
) -> None:
    migrate(scratch_dsn)
    _devolve_to_wide(scratch_dsn)
    _seed_wide_rows(scratch_dsn, targets=5)

    with psycopg.connect(scratch_dsn) as conn:
        with pytest.raises(psycopg.errors.Error) as excinfo:
            ensure_attribute_schema(conn)
        assert excinfo.value.sqlstate == "OKG20"
        assert "0020_interval_narrow_split" in str(excinfo.value)
        conn.rollback()

    # The refusal performed NO data motion: still wide, rows intact.
    with psycopg.connect(scratch_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FROM information_schema.columns
             WHERE table_schema = 'okg'
               AND table_name = 'attribute_effective_view'
               AND column_name IN ('attrs', 'raw_attrs')
            """
        )
        assert cur.fetchone()[0] == 2
        cur.execute(f"SELECT count(*) FROM {_VIEW}")
        assert cur.fetchone()[0] == 5

    # migrate's tolerance flag observes the same shape without refusing.
    with psycopg.connect(scratch_dsn) as conn:
        ensure_attribute_schema(conn, tolerate_pre_narrow_split=True)
        conn.commit()

    # An EMPTY wide database converges inline (zero data motion).
    with psycopg.connect(scratch_dsn) as conn, conn.cursor() as cur:
        cur.execute(f"DELETE FROM {_VIEW}")
        cur.execute(f"DELETE FROM {_INTERVALS}")
        conn.commit()
    with psycopg.connect(scratch_dsn) as conn:
        ensure_attribute_schema(conn)
        conn.commit()
    with psycopg.connect(scratch_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FROM information_schema.columns
             WHERE table_schema = 'okg'
               AND table_name = 'attribute_effective_view'
               AND column_name IN ('attrs', 'raw_attrs')
            """
        )
        assert cur.fetchone()[0] == 0


_CRASH_BATCH_ROWS = 50
_CRASH_TARGETS = 500
_KILL_AFTER_BATCHES = 3

# The crash arm runs OUR spec with a small batch size and SIGKILLs itself
# after N committed backfill batches — mirroring the driver's reference
# crash test, but through the shipped 0020 phases.
_CRASH_SUBPROCESS = """
import os
import signal

from okg.substrate.db import interval_narrow_split_migration as m
from okg.substrate.db.management import current_known_migrations
from okg.substrate.db.multiphase import (
    MultiphasePhase,
    MultiphaseSpec,
    run_multiphase_migration,
)

m.BATCH_ROWS = {batch_rows}
spec = m.build_spec()
state = {{"batches": 0}}

def _wrap(run_batch):
    def wrapped(conn, watermark):
        result = run_batch(conn, watermark)
        if result is not None:
            state["batches"] += 1
            if state["batches"] > {kill_after}:
                os.kill(os.getpid(), signal.SIGKILL)
        return result
    return wrapped

phases = tuple(
    MultiphasePhase(name=p.name, run_batch=_wrap(p.run_batch))
    if p.name == "backfill_attribute_effective_view"
    else p
    for p in spec.phases
)
migration = next(
    x for x in current_known_migrations()
    if x.migration_id == m.MIGRATION_ID
)
run_multiphase_migration(
    {dsn!r}, migration, MultiphaseSpec(m.MIGRATION_ID, phases)
)
raise SystemExit("unreachable: the run above must SIGKILL itself")
"""


# Honesty note: because the batch SQL filters `attrs_hash IS NULL`, the
# final batch-count assertion cannot distinguish a watermark resume from a
# floor restart that re-scans and skips the already-hashed rows — both give
# one-pass totals. What this test DOES pin: batch atomicity (exactly the
# committed rows durable, the killed batch's work gone), a labelled
# resumable mid-state visible in migration_status, no double-processing,
# and an identical converged end state. Keyset watermark arithmetic itself
# is the driver's contract, proven by its reference kill test.
def test_kill9_mid_backfill_leaves_labelled_state_and_resume_converges(
    scratch_dsn: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migrate(scratch_dsn)
    _devolve_to_wide(scratch_dsn)
    _seed_wide_rows(scratch_dsn, targets=_CRASH_TARGETS)

    script = tmp_path / "narrow_split_crash_arm.py"
    script.write_text(
        _CRASH_SUBPROCESS.format(
            batch_rows=_CRASH_BATCH_ROWS,
            kill_after=_KILL_AFTER_BATCHES,
            dsn=scratch_dsn,
        ),
        encoding="utf-8",
    )
    completed = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert completed.returncode == -9, (
        f"crash arm did not die by SIGKILL: rc={completed.returncode}, "
        f"stderr={completed.stderr[-2000:]}"
    )

    # Labelled mid-state: exactly the committed batches, durable + visible.
    expected_rows = _KILL_AFTER_BATCHES * _CRASH_BATCH_ROWS
    with psycopg.connect(scratch_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {_VIEW} WHERE attrs_hash IS NOT NULL"
        )
        assert cur.fetchone()[0] == expected_rows, (
            "committed rows must equal the durable watermark; the killed "
            "batch's uncommitted work must be gone"
        )
        cur.execute(
            "SELECT count(*) FROM okg.schema_migrations "
            "WHERE migration_id = %s",
            (MIGRATION_ID,),
        )
        assert cur.fetchone()[0] == 0, "no completion record mid-run"
    status = migration_status(scratch_dsn)
    mid = status["multiphase_in_progress"]
    assert [row["migration_id"] for row in mid] == [MIGRATION_ID]
    assert mid[0]["phase"] == "backfill_attribute_effective_view"
    assert mid[0]["batches_committed"] == _KILL_AFTER_BATCHES

    # Resume through the FULL orchestration (migrate), small batches kept.
    from okg.substrate.db import interval_narrow_split_migration as spec_module

    monkeypatch.setattr(spec_module, "BATCH_ROWS", _CRASH_BATCH_ROWS)
    result = migrate(scratch_dsn)
    assert [row["migration_id"] for row in result.multiphase_applied] == [
        MIGRATION_ID
    ]
    phases = result.multiphase_applied[0]["phases"]
    # Continued from the checkpoint: total committed backfill batches equal
    # one full pass (targets / batch), never the restart-from-zero total.
    assert phases["backfill_attribute_effective_view"] == (
        _CRASH_TARGETS // _CRASH_BATCH_ROWS
    )
    _assert_converged(
        scratch_dsn, view_rows=_CRASH_TARGETS, interval_rows=_CRASH_TARGETS
    )


def test_old_atomic_0020_ledger_row_is_checksum_compatible(
    scratch_dsn: str,
) -> None:
    migrate(scratch_dsn)
    with psycopg.connect(scratch_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.schema_migrations
               SET checksum = %s,
                   result_metadata = '{"applied_via": "ordered_runner"}'::jsonb
             WHERE migration_id = %s
            """,
            (_OLD_ATOMIC_0020_CHECKSUM, MIGRATION_ID),
        )
        conn.commit()
    status = migration_status(scratch_dsn)
    assert status["ok"] is True, status["checksum_mismatches"]
    assert status["checksum_mismatches"] == []
    compat = {
        row["migration_id"]: row for row in status["checksum_compatibilities"]
    }
    assert MIGRATION_ID in compat
    # And a re-migrate neither refuses nor re-executes it.
    result = migrate(scratch_dsn)
    assert result.pending_multiphase == ()
    assert result.multiphase_applied == ()
