"""Per-``(visual, signature)`` caption-failure counter — bounds VLM retry cost.

A figure whose caption attempt fails is retried on the next publish: the
:class:`VisualCaptionEnricher` records the failure and holds its watermark so
the figure is re-processed rather than silently losing its caption. Without a
bound, a *permanently*-failing figure — e.g. one the VLM always returns an empty
reply for — is retried, and re-billed a VLM call, every cycle forever.

This table records a persistent failure count per ``(visual_id,
vision_signature)``. After ``max_attempts`` VLM-call failures the enricher
QUARANTINES that figure: it stops calling the VLM and lets the watermark
advance, so the cost is bounded to ``max_attempts`` calls. The failure is NOT a
silent drop — it stays queryable here (``SELECT * FROM
okg.visual_caption_failures WHERE fail_count >= N``) and is logged loudly.

Only failures that actually COST a VLM call count toward quarantine (empty
reply, VLM error, degenerate no-result). Infrastructure failures that fail
*before* the VLM call — a missing crop, a malformed discovery node — are NOT
counted: they hold the watermark and retry indefinitely, because they signal a
staging problem to fix (re-ingest restages the crop), not a figure to give up
on. This also avoids a false quarantine: a crop restaged under the same
``render_sha256`` after a transient miss must not stay quarantined.

Keying on ``vision_signature`` means a model/prompt change resets the budget — a
new model may succeed where the old one failed. A successful caption
:func:`clear`\\ s the row.

The table is DECLARED in ``schema.sql`` (alongside ``okg.visual_crop_blobs``)
and granted to ``app_rw`` there; :func:`ensure_table` issues an idempotent
``CREATE TABLE IF NOT EXISTS`` as a belt-and-suspenders for databases
provisioned before it was added to the schema.

A signature change (new content/model/prompt) is keyed to a *new* row, leaving
the superseded ``(visual_id, old_signature)`` row behind — the same
orphaned-row shape as a superseded crop blob. Reclaiming these is out of scope
here and tracked in the same ``automate-source-blob-gc`` family; the rows are
tiny (a text key + an int) and bounded by figures-that-failed, so they are not
a growth hazard in the interim.
"""
from __future__ import annotations

import psycopg

__all__ = [
    "TABLE",
    "ensure_table",
    "load_counts",
    "record_failure",
    "clear",
]

TABLE = "okg.visual_caption_failures"


def ensure_table(conn: psycopg.Connection) -> None:
    """Create ``okg.visual_caption_failures`` if absent (idempotent)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS okg.visual_caption_failures (
              visual_id         TEXT NOT NULL,
              vision_signature  TEXT NOT NULL,
              fail_count        INT  NOT NULL DEFAULT 0,
              last_error        TEXT,
              updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
              PRIMARY KEY (visual_id, vision_signature)
            )
            """
        )


def load_counts(conn: psycopg.Connection) -> dict[tuple[str, str], int]:
    """Return ``{(visual_id, vision_signature): fail_count}`` for all rows.

    Read once at the start of a run so the per-candidate quarantine check needs
    no per-figure round-trip.
    """
    # Literal table name in the SQL (not an f-string) — matches crop_store and
    # keeps the write-callsite inventory from classifying this as a `{dynamic}`
    # runtime-target write. TABLE is the single source of truth for callers.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT visual_id, vision_signature, fail_count "
            "FROM okg.visual_caption_failures"
        )
        return {(r[0], r[1]): r[2] for r in cur.fetchall()}


def record_failure(
    conn: psycopg.Connection,
    *,
    visual_id: str,
    vision_signature: str,
    error: str,
) -> int:
    """Increment (or start at 1) the failure count for one figure attempt.

    Returns the new count. Called only for failures that cost a VLM call, so
    the count is a true "how many times we paid and got nothing" tally.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.visual_caption_failures
              (visual_id, vision_signature, fail_count, last_error, updated_at)
            VALUES (%s, %s, 1, %s, now())
            ON CONFLICT (visual_id, vision_signature) DO UPDATE
              SET fail_count = okg.visual_caption_failures.fail_count + 1,
                  last_error = EXCLUDED.last_error,
                  updated_at = now()
            RETURNING fail_count
            """,
            (visual_id, vision_signature, error[:2000]),
        )
        return int(cur.fetchone()[0])


def clear(
    conn: psycopg.Connection, *, visual_id: str, vision_signature: str,
) -> None:
    """Drop the failure row after a successful caption (a prior transient
    failure that later succeeded must not count against a future re-caption)."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM okg.visual_caption_failures "
            "WHERE visual_id = %s AND vision_signature = %s",
            (visual_id, vision_signature),
        )
