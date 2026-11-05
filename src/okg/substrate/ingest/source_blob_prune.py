"""Delete a source's superseded record-set and cursor blobs after an accepted run.

PACT record-set-blob-autoprune. Every sync of a source with a large record
set (or a large cursor field) writes a new row to
``okg.source_record_set_blobs`` (pieces in
``okg.source_record_set_blob_pieces``) or ``okg.source_cursor_blobs``,
because each record carries the run id and so every sha256 is new. Only the
blobs the source's watermark names are ever read again. This module deletes
the rest right after a run's acceptance commits, on the direct ingest path and
on the held-run approval path.

A blob is kept when any of these holds:

* its sha256 appears in the source's ``okg.source_watermarks`` row
  (``last_applied_rev``, ``cutover_watermark``, ``last_published_rev``), or
  inside a cursor blob so named (a large ``record_set_ref`` field can itself
  be stored as a cursor blob);
* its sha256 appears under ``verdict->'source_terminal_state'`` of an
  admission envelope of the source that is still ``pending`` or ``blocked``
  (a held run's blobs, until it is approved or rejected);
* its ``updated_at`` is newer than the grace window, checked again by the
  delete itself. A run that has written its blobs but is not accepted yet is
  protected this way, and so is the run-start blob a rejected, no-op or
  failed run restores (the parent restores a dead worker child's run-start
  state after the child's lock is gone): every run calls
  :func:`pin_snapshot_blobs`, which refreshes ``updated_at`` of the blobs its
  run-start snapshot names. A run that outlives the window loses this.

The prune runs only while it holds the source's run lock
(:func:`okg.substrate.ingest.source_jobs.run_lock_key`): a worker-child run
already holds it for its whole life; any other caller takes it with a
try-lock for the prune's own transaction and reports ``skipped_locked`` when
another holder has it. Each prune is bounded by a row cap and a byte cap and
runs with ``SET LOCAL lock_timeout``. It never raises into the caller: a
failure is returned in the report and recorded in ``okg.source_progress``
under :data:`ERROR_KEY`, which ``okg doctor`` reports and the next successful
prune clears.

The manual ``okg db prune-source-blobs`` (``db/management.py``) is unchanged.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Mapping

log = logging.getLogger(__name__)

#: The deployment.yaml key; ``0`` turns the automatic prune off.
GRACE_KEY = "source_blob_prune_grace_seconds"
DEFAULT_GRACE_SECONDS = 6 * 3600
#: The most blob rows one prune deletes, over both tables.
MAX_ROWS = 1000
#: A prune stops deleting once the bytes it deleted reach this cap.
MAX_BYTES = 256 * 1024 * 1024
#: The same bound the manual prune uses.
LOCK_TIMEOUT = "5s"
#: The okg.source_progress key a failed prune writes.
ERROR_KEY = "sync.record_set_blob_prune.last_error"
ACCEPTED_STATUSES = frozenset({"accepted", "published"})

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_TABLES = ("source_record_set_blobs", "source_cursor_blobs")


def grace_seconds_for_deployment(deployment_ref: str) -> int:
    """The grace window of a deployment, by name or directory.

    Both prune call sites read it here, through the same loader and the same
    composed manifest (``effective_raw``).
    """
    from okg.substrate.deployments import resolve_deployment

    return grace_seconds_from_manifest(resolve_deployment(deployment_ref).effective_raw)


def grace_seconds_from_manifest(raw: Mapping[str, Any] | None) -> int:
    """The grace window in seconds from a deployment manifest; default 6 h."""
    value = (raw or {}).get("source_blob_prune_grace_seconds")
    if value is None:
        return DEFAULT_GRACE_SECONDS
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(
            f"deployment.yaml {GRACE_KEY} must be a whole number of seconds "
            f">= 0 (0 turns the automatic blob prune off); got {value!r}"
        )
    return value


def _shas(text: str) -> list[str]:
    # A sha256 is 64 lowercase hex characters; any such run in a rev keeps a
    # blob, so a match can only over-keep, never delete a named blob.
    return sorted(set(_SHA256_RE.findall(text or "")))


def _existing_tables(cur) -> list[str]:
    cur.execute(
        "SELECT to_regclass('okg.source_record_set_blobs') IS NOT NULL,"
        " to_regclass('okg.source_cursor_blobs') IS NOT NULL"
    )
    row = cur.fetchone()
    return [table for table, present in zip(_TABLES, row) if present]


def _with_nested(cur, source: str, shas: list[str], tables: list[str]) -> list[str]:
    # A cursor field above OKG_SOURCE_RUNNER_INLINE_CURSOR_FIELD_MAX_BYTES is
    # itself stored as a cursor blob, so a record_set_ref can sit inside one.
    # Add the sha256s named inside the cursor blobs already kept.
    if not shas or "source_cursor_blobs" not in tables:
        return shas
    cur.execute(
        "SELECT value::text FROM okg.source_cursor_blobs"
        " WHERE source = %s AND sha256 = ANY(%s::text[])",
        (source, shas),
    )
    nested = _shas(" ".join(str(row[0] or "") for row in cur.fetchall()))
    return sorted(set(shas) | set(nested))


def pin_snapshot_blobs(dsn: str, snapshot: Any) -> None:
    """Refresh ``updated_at`` of the blobs a run-start snapshot names.

    A run that is rejected, accepted as a no-op or fails writes its run-start
    watermark back -- for a worker child that died, the parent does so after
    the child's run lock is gone, so another run's prune may already have
    run. Touching the blobs here keeps them inside the grace window for the
    length of the run; a run longer than the window is not covered.
    """
    from okg.substrate.db.pool import GENERAL, pooled_connection

    watermark = getattr(snapshot, "watermark", None)
    shas = _shas(json.dumps(watermark, default=str)) if watermark else []
    if not shas:
        return
    with pooled_connection(dsn, purpose=GENERAL) as conn:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute("SELECT set_config('lock_timeout', %s, true)", (LOCK_TIMEOUT,))
            tables = _existing_tables(cur)
            shas = _with_nested(cur, snapshot.source, shas, tables)
            if "source_record_set_blobs" in tables:
                cur.execute(
                    "UPDATE okg.source_record_set_blobs SET updated_at = now()"
                    " WHERE source = %s AND sha256 = ANY(%s::text[])",
                    (snapshot.source, shas),
                )
            if "source_cursor_blobs" in tables:
                cur.execute(
                    "UPDATE okg.source_cursor_blobs SET updated_at = now()"
                    " WHERE source = %s AND sha256 = ANY(%s::text[])",
                    (snapshot.source, shas),
                )


def _keep_shas(cur, deployment: str, source: str, tables: list[str]) -> list[str]:
    # The envelope filter leads with (deployment_name, status), the columns
    # of admission_envelopes_deployment_status_idx, so this never scans every
    # envelope the deployment has ever had.
    cur.execute(
        """
        SELECT concat_ws(' ', last_applied_rev::text, cutover_watermark::text,
                         last_published_rev::text)
          FROM okg.source_watermarks
         WHERE source = %s
        UNION ALL
        SELECT (verdict -> 'source_terminal_state')::text
          FROM okg.admission_envelopes
         WHERE deployment_name = %s
           AND status IN ('pending', 'blocked')
           AND source = %s
        """,
        (source, deployment, source),
    )
    named = _shas(" ".join(str(row[0] or "") for row in cur.fetchall()))
    return _with_nested(cur, source, named, tables)


# Candidates oldest first, at most %(rows)s; a row is deleted while the bytes
# deleted before it are under %(bytes)s, so one row larger than the cap still
# goes (otherwise it would never be collected). Bytes are the stored sizes of
# the payload columns (pg_column_size of a TOASTed value reads its pointer, not
# the value) plus the pieces' bytes; cursor blobs carry their own size_bytes.
# The DELETE re-checks age: a run may have refreshed a row since it was chosen.
_DELETE_SQL = {
    "source_record_set_blobs": """
        WITH c AS (
          SELECT b.sha256, b.updated_at,
                 (coalesce(pg_column_size(b.record_set), 0)
                  + coalesce(pg_column_size(b.record_set_text), 0)
                  + coalesce(pg_column_size(b.record_set_summary), 0)
                  + coalesce(pg_column_size(b.chronos_proof), 0)
                  + coalesce(b.piece_bytes, 0))::bigint AS bytes,
                 coalesce(b.piece_count, 0) AS pieces
            FROM okg.source_record_set_blobs b
           WHERE b.source = %(source)s
             AND b.updated_at < now() - make_interval(secs => %(grace)s)
             AND NOT (b.sha256 = ANY(%(keep)s::text[]))
           ORDER BY b.updated_at, b.sha256
           LIMIT %(rows)s
        ), r AS (
          SELECT sha256, bytes, pieces,
                 sum(bytes) OVER (ORDER BY updated_at, sha256) - bytes AS before
            FROM c
        )
        DELETE FROM okg.source_record_set_blobs b
         USING r
         WHERE b.source = %(source)s AND b.sha256 = r.sha256
           AND b.updated_at < now() - make_interval(secs => %(grace)s)
           AND r.before < %(bytes)s
        RETURNING r.bytes, r.pieces
    """,
    "source_cursor_blobs": """
        WITH c AS (
          SELECT b.cursor_key, b.sha256, b.updated_at,
                 b.size_bytes::bigint AS bytes
            FROM okg.source_cursor_blobs b
           WHERE b.source = %(source)s
             AND b.updated_at < now() - make_interval(secs => %(grace)s)
             AND NOT (b.sha256 = ANY(%(keep)s::text[]))
           ORDER BY b.updated_at, b.cursor_key, b.sha256
           LIMIT %(rows)s
        ), r AS (
          SELECT cursor_key, sha256, bytes,
                 sum(bytes) OVER (ORDER BY updated_at, cursor_key, sha256)
                   - bytes AS before
            FROM c
        )
        DELETE FROM okg.source_cursor_blobs b
         USING r
         WHERE b.source = %(source)s AND b.cursor_key = r.cursor_key
           AND b.sha256 = r.sha256
           AND b.updated_at < now() - make_interval(secs => %(grace)s)
           AND r.before < %(bytes)s
        RETURNING r.bytes, 0
    """,
}


def prune_source_blobs_after_accept(
    conn_or_dsn: Any,
    *,
    deployment: str,
    source: str,
    grace_seconds: int,
    max_rows: int = MAX_ROWS,
    max_bytes: int = MAX_BYTES,
    lock_held: bool = False,
) -> dict[str, Any]:
    """Delete ``source``'s unneeded blob rows older than ``grace_seconds``.

    ``lock_held`` says the caller's process already holds the source's run
    lock (a worker-child run); otherwise the prune try-locks it for its own
    transaction and returns ``{"status": "skipped_locked"}`` when another
    holder has it. ``grace_seconds=0`` deletes every unneeded row. A
    connection passed in must not be inside a transaction. Raises on a
    database error; :func:`after_accept` is the never-raising wrapper.
    """
    from contextlib import nullcontext

    from okg.substrate.db.pool import GENERAL, pooled_connection
    from okg.substrate.ingest.source_jobs import run_lock_key

    scope = (
        pooled_connection(conn_or_dsn, purpose=GENERAL)
        if isinstance(conn_or_dsn, str)
        else nullcontext(conn_or_dsn)
    )
    report: dict[str, Any] = {
        "status": "ok",
        "grace_seconds": grace_seconds,
        "lock": "held_by_run" if lock_held else "try_lock",
    }
    with scope as conn, conn.transaction(), conn.cursor() as cur:
        cur.execute("SELECT set_config('lock_timeout', %s, true)", (LOCK_TIMEOUT,))
        if not lock_held:
            cur.execute(
                "SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0))",
                (run_lock_key(deployment, source),),
            )
            if not cur.fetchone()[0]:
                return {**report, "status": "skipped_locked"}
        tables = _existing_tables(cur)
        keep = _keep_shas(cur, deployment, source, tables)
        report["kept_named"] = len(keep)
        rows_left, bytes_left = max_rows, max_bytes
        deleted: dict[str, dict[str, int]] = {}
        for table in tables:
            counts = {"rows": 0, "bytes": 0}
            if rows_left > 0 and bytes_left > 0:
                cur.execute(
                    _DELETE_SQL[table],
                    {
                        "source": source, "grace": grace_seconds, "keep": keep,
                        "rows": rows_left, "bytes": bytes_left,
                    },
                )
                returned = cur.fetchall()
                counts["rows"] = len(returned)
                counts["bytes"] = sum(int(row[0]) for row in returned)
                if table == "source_record_set_blobs":
                    counts["pieces"] = sum(int(row[1]) for row in returned)
            rows_left -= counts["rows"]
            bytes_left -= counts["bytes"]
            deleted[table] = counts
        report["deleted"] = deleted
        report["deleted_rows"] = sum(c["rows"] for c in deleted.values())
        report["deleted_bytes"] = sum(c["bytes"] for c in deleted.values())
        report["capped"] = rows_left <= 0 or bytes_left <= 0
        cur.execute(
            "DELETE FROM okg.source_progress"
            " WHERE deployment = %s AND source_name = %s AND key = %s",
            (deployment, source, ERROR_KEY),
        )
    return report


def _record_error(dsn: str, *, deployment: str, source: str, error: str) -> None:
    from okg.substrate.db.pool import GENERAL, pooled_connection

    value = json.dumps({
        "source": source,
        "at": datetime.now(timezone.utc).isoformat(),
        "error": error,
    })
    with pooled_connection(dsn, purpose=GENERAL) as conn:
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO okg.source_progress
                  (deployment, source_name, key, value, updated_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (deployment, source_name, key) DO UPDATE
                  SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
                """,
                (deployment, source, ERROR_KEY, value),
            )


def after_accept(
    dsn: str,
    *,
    deployment: str,
    source: str,
    admission_status: str | None,
    deployment_ref: str | None = None,
    lock_held: bool = False,
) -> dict[str, Any] | None:
    """Run the automatic prune after an acceptance; never raises.

    Returns ``None`` when the run was not accepted. ``deployment_ref`` names
    the deployment for :func:`grace_seconds_for_deployment` (the ingest
    path passes its deployment directory); ``None`` uses ``deployment``.
    """
    if admission_status not in ACCEPTED_STATUSES:
        return None
    try:
        grace = grace_seconds_for_deployment(deployment_ref or deployment)
        if grace == 0:
            return {"status": "disabled", "grace_seconds": 0}
        return prune_source_blobs_after_accept(
            dsn, deployment=deployment, source=source, grace_seconds=grace,
            lock_held=lock_held,
        )
    except Exception as exc:  # noqa: BLE001 - reported and recorded, never raised into the acceptance.
        error = f"{type(exc).__name__}: {exc}"
        log.warning("automatic blob prune of source %s failed: %s", source, error)
        report: dict[str, Any] = {"status": "error", "error": error}
        try:
            _record_error(dsn, deployment=deployment, source=source, error=error)
        except Exception as record_exc:  # noqa: BLE001 - the report still carries both errors.
            report["error_record_failed"] = f"{type(record_exc).__name__}: {record_exc}"
        return report


def report_fields(report: Mapping[str, Any] | None) -> dict[str, Any]:
    """The per-source ingest report field for an automatic prune, if one ran."""
    return {} if report is None else {"record_set_blob_prune": dict(report)}


def recorded_errors(conn, deployment: str) -> list[dict[str, Any]]:
    """Every source of ``deployment`` whose last automatic prune failed."""
    rows = conn.execute(
        "SELECT source_name, value, updated_at FROM okg.source_progress"
        " WHERE deployment = %s AND key = %s ORDER BY source_name",
        (deployment, ERROR_KEY),
    ).fetchall()
    found: list[dict[str, Any]] = []
    for source_name, value, updated_at in rows:
        try:
            detail = json.loads(value)
        except ValueError:
            detail = None
        if not isinstance(detail, dict):
            detail = {"error": value}
        found.append({
            "source": str(source_name),
            "at": str(detail.get("at") or updated_at),
            "error": str(detail.get("error") or value),
        })
    return found
