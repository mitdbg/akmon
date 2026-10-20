"""Tests for source-blob orphan GC (PACT bound-okg-workspace-disk-growth, task 6).

The blob tables keep one row per (source, sha256) content version a source has
ever produced; only the sha256 in that source's current watermark rev is ever
hydrated. ``prune_orphaned_source_blobs`` deletes the unreachable rest. The
safety-critical property -- a referenced blob is NEVER dropped -- is asserted by
checking the exact surviving sha set after a confirmed prune.
"""
from __future__ import annotations

import hashlib
import json

import psycopg
import pytest

from okg.substrate.db.management import prune_orphaned_source_blobs
from okg.substrate.errors import RequiresConfirm


def _sha(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


# Referenced by alpha's watermark.
REF_CUR = _sha("alpha-cursor-referenced")
REF_RS = _sha("alpha-rs-referenced")
# Orphans (no watermark of their own source mentions them).
ORPH_CUR = _sha("alpha-cursor-orphan-old")
ORPH_RS = _sha("alpha-rs-orphan-old")
RECENT_CUR = _sha("alpha-cursor-orphan-recent")
# Referenced by ALPHA's watermark, but the blob row belongs to BETA, whose own
# watermark does not mention it -- proves reachability is scoped per source.
CROSS = _sha("beta-cursor-cross-source")


def _seed(dsn: str) -> None:
    """Clean-slate the three tables and insert the fixture rows."""
    with psycopg.connect(dsn) as conn:
        conn.execute("DELETE FROM okg.source_cursor_blobs")
        conn.execute("DELETE FROM okg.source_record_set_blobs")
        conn.execute("DELETE FROM okg.source_watermarks")

        # alpha's watermark references REF_CUR, REF_RS, and (incidentally) CROSS.
        rev = {
            "cursor": {"c": REF_CUR},
            "record_set_ref": {"sha256": REF_RS},
            "note": CROSS,
        }
        conn.execute(
            """
            INSERT INTO okg.source_watermarks (source, last_applied_rev)
            VALUES ('alpha', %s::jsonb)
            """,
            (json.dumps(rev),),
        )
        # No watermark row for 'beta' -> beta's blobs are all orphaned.

        cur_rows = [
            ("alpha", "c", REF_CUR, "2 days"),    # referenced + old -> KEEP
            ("alpha", "c", ORPH_CUR, "2 days"),   # orphan + old     -> DELETE
            ("alpha", "c", RECENT_CUR, "0 secs"), # orphan but young -> KEEP (grace)
            ("beta", "c", CROSS, "2 days"),       # cross-source orphan -> DELETE
        ]
        for source, key, sha, age in cur_rows:
            conn.execute(
                """
                INSERT INTO okg.source_cursor_blobs
                  (source, cursor_key, sha256, value, size_bytes, updated_at)
                VALUES (%s, %s, %s, '{}'::jsonb, 0, now() - %s::interval)
                """,
                (source, key, sha, age),
            )

        rs_rows = [
            ("alpha", REF_RS, "2 days"),   # referenced + old -> KEEP
            ("alpha", ORPH_RS, "2 days"),  # orphan + old     -> DELETE
        ]
        for source, sha, age in rs_rows:
            conn.execute(
                """
                INSERT INTO okg.source_record_set_blobs
                  (source, sha256, record_set, updated_at)
                VALUES (%s, %s, '{}'::jsonb, now() - %s::interval)
                """,
                (source, sha, age),
            )


def _surviving_shas(dsn: str) -> set[str]:
    with psycopg.connect(dsn) as conn:
        cur = conn.execute("SELECT sha256 FROM okg.source_cursor_blobs")
        shas = {row[0] for row in cur.fetchall()}
        rs = conn.execute("SELECT sha256 FROM okg.source_record_set_blobs")
        shas |= {row[0] for row in rs.fetchall()}
    return shas


# --- pure: bad input loud-fails -------------------------------------------

def test_prune_negative_grace_raises():
    with pytest.raises(RequiresConfirm):
        prune_orphaned_source_blobs(
            "postgresql://nobody@127.0.0.1:1/x",
            grace_seconds=-1,
        )


# --- DB-backed correctness -------------------------------------------------

@pytest.mark.postgres
def test_prune_dry_run_counts_orphans_without_deleting(okg_migrated: str):
    dsn = okg_migrated
    _seed(dsn)
    before = _surviving_shas(dsn)

    report = prune_orphaned_source_blobs(dsn, confirm=False, grace_seconds=86400)

    assert report["present"] is True
    assert report["dry_run"] is True
    # 2 cursor orphans (ORPH_CUR + cross-source CROSS) + 1 record-set orphan.
    assert report["orphaned_count"] == 3
    assert report["deleted_count"] == 0
    by_table = {t["table"]: t for t in report["tables"]}
    assert by_table["source_cursor_blobs"]["orphaned_count"] == 2
    assert by_table["source_record_set_blobs"]["orphaned_count"] == 1
    # Dry-run must not mutate.
    assert _surviving_shas(dsn) == before


@pytest.mark.postgres
def test_prune_grace_window_protects_young_orphans(okg_migrated: str):
    dsn = okg_migrated
    _seed(dsn)
    # grace=0 -> the young orphan (RECENT_CUR) is no longer protected, so the
    # orphan count rises by exactly one versus the 1-day window.
    zero = prune_orphaned_source_blobs(dsn, confirm=False, grace_seconds=0)
    assert zero["orphaned_count"] == 4


@pytest.mark.postgres
def test_prune_confirm_drops_orphans_keeps_referenced(okg_migrated: str):
    dsn = okg_migrated
    _seed(dsn)

    report = prune_orphaned_source_blobs(dsn, confirm=True, grace_seconds=86400)

    assert report["dry_run"] is False
    assert report["deleted_count"] == 3
    # The safety property: every referenced blob survives; only old orphans go.
    # RECENT_CUR survives on the grace window, not on reachability.
    assert _surviving_shas(dsn) == {REF_CUR, REF_RS, RECENT_CUR}
    # Idempotent: a second pass finds nothing new.
    again = prune_orphaned_source_blobs(dsn, confirm=True, grace_seconds=86400)
    assert again["deleted_count"] == 0


@pytest.mark.postgres
def test_prune_keeps_blobs_referenced_via_cutover_and_published_revs(
    okg_migrated: str,
):
    # The keep-set spans all three watermark rev columns, not just
    # last_applied_rev: a record-set blob referenced only by last_published_rev
    # (publish lagging the cursor advance) and a cursor blob referenced only by
    # cutover_watermark must both survive.
    dsn = okg_migrated
    cut_sha = _sha("gamma-cursor-cutover-only")
    pub_sha = _sha("gamma-rs-published-only")
    orphan = _sha("gamma-cursor-orphan")
    with psycopg.connect(dsn) as conn:
        conn.execute("DELETE FROM okg.source_cursor_blobs")
        conn.execute("DELETE FROM okg.source_record_set_blobs")
        conn.execute("DELETE FROM okg.source_watermarks")
        conn.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, cutover_watermark, last_published_rev)
            VALUES ('gamma', '{}'::jsonb, %s::jsonb, %s::jsonb)
            """,
            (json.dumps({"cur": cut_sha}), json.dumps({"rs": pub_sha})),
        )
        for sha in (cut_sha, orphan):
            conn.execute(
                """
                INSERT INTO okg.source_cursor_blobs
                  (source, cursor_key, sha256, value, size_bytes, updated_at)
                VALUES ('gamma', 'c', %s, '{}'::jsonb, 0, now() - '2 days'::interval)
                """,
                (sha,),
            )
        conn.execute(
            """
            INSERT INTO okg.source_record_set_blobs
              (source, sha256, record_set, updated_at)
            VALUES ('gamma', %s, '{}'::jsonb, now() - '2 days'::interval)
            """,
            (pub_sha,),
        )

    report = prune_orphaned_source_blobs(dsn, confirm=True, grace_seconds=86400)

    assert report["deleted_count"] == 1  # only the unreferenced orphan
    assert _surviving_shas(dsn) == {cut_sha, pub_sha}
