"""The reference multi-phase migration used by the multiphase tests.

Shared between the in-process test and the kill -9 subprocess so the
crashed run and the resuming run execute IDENTICAL batch logic — a
resume that "works" against different batch code proves nothing.

The migration is a benign batched backfill on a scratch table in the
PUBLIC schema (``okg_multiphase_scratch``): it touches no real substrate
relation and no real schema semantics. See the fixture SQL for why it
lives in the test tree instead of the shipped migrations directory.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import psycopg

REFERENCE_MIGRATION_ID = "0099_multiphase_reference_backfill"
FIXTURE_SQL_PATH = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "multiphase"
    / f"{REFERENCE_MIGRATION_ID}.sql"
)
TOTAL_ROWS = 500
BATCH_SIZE = 50
SCRATCH_TABLE = "okg_multiphase_scratch"
BACKFILL_PHASE = "backfill_dst"


def fixture_sql() -> str:
    return FIXTURE_SQL_PATH.read_text(encoding="utf-8")


def fixture_checksum() -> str:
    return hashlib.sha256(fixture_sql().encode("utf-8")).hexdigest()


def manifest_entry(*, declare_multiphase: bool) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "migration_id": REFERENCE_MIGRATION_ID,
        "checksum_sha256": fixture_checksum(),
        "destructive": False,
    }
    if declare_multiphase:
        entry["class"] = "multi_phase"
    return entry


def known_migration():
    from okg.substrate.db.management import KnownMigration

    return KnownMigration(
        migration_id=REFERENCE_MIGRATION_ID,
        description=f"ordered migration {REFERENCE_MIGRATION_ID}",
        checksum=fixture_checksum(),
        destructive=False,
    )


def seed_scratch(conn: psycopg.Connection) -> None:
    """Create + populate the scratch table (idempotent per test DB)."""
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {SCRATCH_TABLE}")
        cur.execute(
            f"""
            CREATE TABLE {SCRATCH_TABLE} (
              id          BIGINT PRIMARY KEY,
              src         BIGINT NOT NULL,
              dst         BIGINT,
              touch_count BIGINT NOT NULL DEFAULT 0
            )
            """
        )
        cur.execute(
            f"""
            INSERT INTO {SCRATCH_TABLE} (id, src)
            SELECT g, g * 10 FROM generate_series(1, %s) AS g
            """,
            (TOTAL_ROWS,),
        )
    conn.commit()


def run_backfill_batch(
    conn: psycopg.Connection, watermark: Any | None
) -> int | None:
    """One bounded batch: rows (watermark, watermark+BATCH_SIZE].

    ``touch_count`` increments on every touch, so a resume that
    restarted from zero instead of the checkpoint would leave
    ``touch_count = 2`` on re-processed rows — the correctness signal
    the test asserts on.
    """
    start = int(watermark or 0)
    if start >= TOTAL_ROWS:
        return None
    end = start + BATCH_SIZE
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {SCRATCH_TABLE}
               SET dst = src * 2,
                   touch_count = touch_count + 1
             WHERE id > %s AND id <= %s
            """,
            (start, end),
        )
    return end


def build_spec(*, kill_after_batches: int | None = None):
    """The reference phase spec; optionally SIGKILL mid-batch.

    ``kill_after_batches=N`` performs batch N+1's data motion and then
    SIGKILLs the process BEFORE the driver can commit it — the crash
    leaves N committed checkpoints plus discarded uncommitted work, the
    worst-case crash point.
    """
    from okg.substrate.db.multiphase import MultiphasePhase, MultiphaseSpec

    state = {"batches": 0}

    def run_batch(conn: psycopg.Connection, watermark: Any | None):
        result = run_backfill_batch(conn, watermark)
        if (
            kill_after_batches is not None
            and result is not None
            and state["batches"] >= kill_after_batches
        ):
            import os
            import signal

            os.kill(os.getpid(), signal.SIGKILL)
        state["batches"] += 1
        return result

    return MultiphaseSpec(
        migration_id=REFERENCE_MIGRATION_ID,
        phases=(
            MultiphasePhase(name=BACKFILL_PHASE, run_batch=run_batch),
        ),
    )
