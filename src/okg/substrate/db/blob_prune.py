"""Delete ``okg.blobs`` rows nothing references any more: ``okg db prune-blobs``.

PACT first-class-images, req.first-class-images.cleanup (issue #3046).

Cleanup runs per kind. Each kind in :data:`okg.substrate.db.blob_store.BLOB_KINDS`
declares, in :func:`blob_kind_references`, how its references are found; a
row of that kind is deleted only when both hold:

* no reference to its id was found (see the kind's scan), and
* its ``last_used_at`` is older than the grace window.

The work is split in two so the race guard can be tested on its own:

1. :func:`plan_blob_prune` runs the kind's reference scan and reads the
   candidate rows inside ONE ``REPEATABLE READ`` read-only transaction, so
   every bounded scan statement and the candidate read see the same
   snapshot. A row copied from a scanned row after the snapshot (a branch
   merge, a declarer taking over a node) therefore still had its source row
   in the snapshot, and was seen.
2. :func:`apply_blob_prune` deletes the candidates in bounded batches, each
   in its own short transaction under ``SET LOCAL lock_timeout``. The DELETE
   re-checks ``last_used_at < now() - grace`` itself. An ingest that reuses a
   row calls ``blob_store.touch`` (or ``put``) BEFORE it writes the value
   that references it, so a reuse that lands after the scan refreshes the
   row and the DELETE skips it. If the DELETE wins instead, the ingest's
   ``touch`` finds no row and the ingest renders and stores it again.

Two guards bound what ``--apply`` may delete:

* The grace window is at least :data:`MIN_GRACE_SECONDS` (one hour), in this
  library and in the CLI. A row an in-flight ingest has just touched is
  referenced only once that run's write is published and scanned; until then
  only the grace window keeps it. A shorter window is refused with
  ``blob_prune_grace_too_short``.
* ``--apply`` refuses with ``blob_prune_no_graph`` when no graph node storage
  is registered (``ReferenceScan.graph_registered`` is False). With no graph
  to scan, every row past grace reads as unreferenced; on a fresh environment
  clone, which purges the Chronos tables but keeps ``okg.blobs``, that would
  delete every preserved row. A dry run still reports, with that reason.

A dry run (the default) plans and reports what would be deleted; it never
deletes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Any, Callable, Iterable, Mapping, Sequence

import psycopg
from psycopg import sql

from okg.substrate.db.blob_store import BLOB_KINDS
from okg.substrate.db.connection import connect as db_connect
from okg.substrate.errors import SubstrateError

__all__ = [
    "BlobKindReferences",
    "BlobPruneGraceTooShort",
    "BlobPruneNoGraph",
    "BlobPrunePlan",
    "MIN_GRACE_SECONDS",
    "ReferenceScan",
    "ScanSource",
    "apply_blob_prune",
    "blob_kind_references",
    "plan_blob_prune",
    "prune_blobs",
    "relation_primary_key",
    "require_grace_seconds",
    "scan_json_references",
]

DEFAULT_GRACE_SECONDS = 86400
#: The smallest grace window cleanup accepts. An ingest touches a row it
#: reuses, then writes the value that references it and publishes; until the
#: publish lands, the reference is not in any scanned table. The grace window
#: is what keeps that row, so it must outlast a run's write-to-publish window.
MIN_GRACE_SECONDS = 3600
DEFAULT_SCAN_BATCH_ROWS = 5000
DEFAULT_DELETE_BATCH_SIZE = 500
DEFAULT_LOCK_TIMEOUT = "5s"
DEFAULT_SAMPLE_LIMIT = 20


# ── the reference-scan contract every kind implements ─────────────────

@dataclass(frozen=True)
class ScanSource:
    """One relation a kind's references are read from.

    ``relation`` is the name to read (a Chronos physical table, or an okg
    table); ``logical`` is the name an operator knows it by, and
    ``storage`` says which of the two it is (``chronos_interval`` or
    ``table``). ``column`` holds the JSON to search and ``paths`` are SQL/JSON
    paths over it whose results are the referenced ids (as JSON strings).
    """

    logical: str
    relation: str
    storage: str
    column: str
    paths: tuple[str, ...]


@dataclass
class ReferenceScan:
    """What a kind's reference scan found.

    ``graph_registered`` is False when the database has no registered graph
    node storage at all (a fresh database, or a fresh environment clone):
    then nothing reads as referenced, so ``apply_blob_prune`` refuses with
    ``blob_prune_no_graph`` instead of deleting every row past grace.
    """

    referenced: set[str] = field(default_factory=set)
    sources: list[dict[str, Any]] = field(default_factory=list)
    graph_registered: bool = False


@dataclass(frozen=True)
class BlobKindReferences:
    """How one blob kind's references are found."""

    kind: str
    description: str
    scan: Callable[[psycopg.Cursor[Any], int], ReferenceScan]


def blob_kind_references() -> dict[str, BlobKindReferences]:
    """The registry: one entry per kind ``okg.blobs`` accepts.

    Imported lazily so the kind modules may import this module's helpers.
    """
    from okg.substrate.images.prune import IMAGE_REFERENCES

    registry = {IMAGE_REFERENCES.kind: IMAGE_REFERENCES}
    if set(registry) != set(BLOB_KINDS):
        # A kind the table accepts with no declared reference scan could
        # only be pruned by guessing; refuse instead.
        raise SubstrateError(
            "blob kinds and blob reference scans disagree",
            data={
                "blob_kinds": sorted(BLOB_KINDS),
                "reference_scans": sorted(registry),
            },
            fix_hint="declare a reference scan for every okg.blobs kind",
        )
    return registry


def relation_primary_key(cur: psycopg.Cursor[Any], relation: str) -> list[str]:
    """The primary-key columns of `relation`, in key order."""
    cur.execute(
        """
        SELECT a.attname
          FROM pg_index AS i
          JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
            ON true
          JOIN pg_attribute AS a
            ON a.attrelid = i.indrelid AND a.attnum = k.attnum
         WHERE i.indrelid = %s::regclass AND i.indisprimary
         ORDER BY k.ord
        """,
        (relation,),
    )
    columns = [str(row[0]) for row in cur.fetchall()]
    if not columns:
        # Keyset batching needs a total order; a relation without one cannot
        # be scanned in bounded statements, and skipping it could miss a
        # reference.
        raise SubstrateError(
            f"cannot scan {relation} for blob references: it has no primary key",
            data={"relation": relation},
        )
    return columns


def _identifier(relation: str) -> sql.Identifier:
    return sql.Identifier(*relation.split(".", 1))


def scan_json_references(
    cur: psycopg.Cursor[Any],
    source: ScanSource,
    *,
    batch_rows: int,
) -> tuple[set[str], dict[str, Any]]:
    """Every id `source`'s paths yield, read in primary-key keyset batches.

    Each statement reads at most `batch_rows` rows. Returns the ids and the
    scan's own report (rows, batches, ids found, seconds).
    """
    pk = relation_primary_key(cur, source.relation)
    pk_list = sql.SQL(", ").join(sql.Identifier(c) for c in pk)
    relation = _identifier(source.relation)
    extract = sql.SQL(" UNION ALL ").join(
        # Not .format()ed, so the braces are literal SQL.
        sql.SQL("SELECT jsonb_path_query(b.payload, %s::jsonpath) #>> '{}'")
        for _ in source.paths
    )
    query = sql.SQL(
        """
        WITH batch AS (
          SELECT {pk_list}, {column} AS payload
            FROM {relation}
           WHERE {after}
           ORDER BY {pk_list}
           LIMIT %s
        )
        SELECT {last_pk},
               (SELECT count(*) FROM batch) AS row_count,
               (SELECT array_agg(DISTINCT f.id)
                  FROM batch AS b, LATERAL ({extract}) AS f(id)) AS ids
          FROM (SELECT {pk_list} FROM batch ORDER BY {pk_desc} LIMIT 1) AS last
        """
    )
    started = time.monotonic()
    found: set[str] = set()
    rows = 0
    batches = 0
    last_key: Sequence[Any] | None = None
    while True:
        if last_key is None:
            after = sql.SQL("true")
            after_params: list[Any] = []
        else:
            after = sql.SQL("({}) > ({})").format(
                pk_list, sql.SQL(", ").join(sql.Placeholder() * len(pk))
            )
            after_params = list(last_key)
        statement = query.format(
            pk_list=pk_list,
            column=sql.Identifier(source.column),
            relation=relation,
            after=after,
            last_pk=sql.SQL(", ").join(
                sql.SQL("last.{}").format(sql.Identifier(c)) for c in pk
            ),
            extract=extract,
            pk_desc=sql.SQL(", ").join(
                sql.SQL("{} DESC").format(sql.Identifier(c)) for c in pk
            ),
        )
        cur.execute(statement, [*after_params, batch_rows, *source.paths])
        record = cur.fetchone()
        if record is None:
            break
        batches += 1
        last_key = tuple(record[: len(pk)])
        rows += int(record[len(pk)])
        ids = record[len(pk) + 1]
        if ids:
            found.update(str(i) for i in ids if i is not None)
        if int(record[len(pk)]) < batch_rows:
            break
    return found, {
        "relation": source.logical,
        "storage": source.storage,
        "physical_table": source.relation,
        "column": source.column,
        "rows_scanned": rows,
        "batches": batches,
        "ids_found": len(found),
        "seconds": round(time.monotonic() - started, 3),
    }


# ── plan and apply ─────────────────────────────────────────────────────

@dataclass
class BlobPrunePlan:
    """The outcome of a scan: which rows of a kind could be deleted.

    ``candidates`` maps id to ``size_bytes`` (0 for a failure row) for every
    unreferenced row older than the grace window, as of the scan snapshot.
    """

    kind: str
    grace_seconds: int
    scan: ReferenceScan
    blobs_total: int
    kept_referenced: int
    kept_within_grace: int
    candidates: dict[str, int]
    scanned_at: Any = None


class BlobPruneGraceTooShort(SubstrateError):
    """The grace window is shorter than `MIN_GRACE_SECONDS`.

    A row an in-flight ingest has just touched is referenced only once that
    run publishes; a shorter window could delete it in between.
    """

    code = "blob_prune_grace_too_short"


class BlobPruneNoGraph(SubstrateError):
    """``--apply`` on a database with no registered graph node storage.

    With no graph to scan, nothing reads as referenced, so every row past
    grace would be deleted. On a fresh environment clone the Chronos tables
    are purged while ``okg.blobs`` is kept, so that would delete every
    preserved row the first publish would reuse.
    """

    code = "blob_prune_no_graph"


def require_grace_seconds(grace_seconds: int) -> None:
    """Refuse a grace window under `MIN_GRACE_SECONDS` (or not an int) with
    `BlobPruneGraceTooShort`. The library and the CLI both call this."""
    minimum = MIN_GRACE_SECONDS
    if isinstance(grace_seconds, bool) or not isinstance(grace_seconds, int) or (
        grace_seconds < minimum
    ):
        raise BlobPruneGraceTooShort(
            f"blob cleanup needs a grace window of at least {minimum} seconds, "
            f"got {grace_seconds!r}",
            data={
                "argument": "grace_seconds",
                "grace_seconds": grace_seconds,
                "minimum_seconds": minimum,
            },
            fix_hint=f"pass --grace-seconds {minimum} or more (default {DEFAULT_GRACE_SECONDS})",
        )


def _require_kind(kind: str) -> BlobKindReferences:
    registry = blob_kind_references()
    if kind not in registry:
        raise SubstrateError(
            f"unknown blob kind {kind!r}; okg.blobs accepts {sorted(registry)}",
            data={"reason": "blob_kind_unknown", "kind": kind},
            fix_hint=f"pass --kind {' or '.join(sorted(registry))}",
        )
    return registry[kind]


def plan_blob_prune(
    conn: psycopg.Connection[Any],
    *,
    kind: str,
    grace_seconds: int = DEFAULT_GRACE_SECONDS,
    scan_batch_rows: int = DEFAULT_SCAN_BATCH_ROWS,
    lock_timeout: str = DEFAULT_LOCK_TIMEOUT,
) -> BlobPrunePlan:
    """Scan `kind`'s references and read its candidate rows in one snapshot.

    `conn` must not be inside a transaction; the plan's own read-only
    REPEATABLE READ transaction is committed before this returns.
    """
    require_grace_seconds(grace_seconds)
    if scan_batch_rows < 1:
        raise SubstrateError(
            "scan_batch_rows must be at least 1",
            data={"scan_batch_rows": scan_batch_rows},
        )
    references = _require_kind(kind)
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        raise SubstrateError(
            "plan_blob_prune needs a connection outside a transaction",
            data={"transaction_status": conn.info.transaction_status.name},
        )
    with conn.transaction(), conn.cursor() as cur:
        # The first statement of the transaction, so it takes effect: one
        # snapshot for every scan batch and the candidate read.
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cur.execute(
            "SELECT set_config('lock_timeout', %s, true)", (lock_timeout,)
        )
        # The planner cannot estimate jsonb_path_query's row count, prices
        # every batch statement high enough to JIT-compile it, and the
        # compile then costs more than the batch itself (measured ~160 ms
        # of a ~190 ms 5,000-row statement).
        cur.execute("SELECT set_config('jit', 'off', true)")
        scan = references.scan(cur, scan_batch_rows)
        cur.execute("SELECT now()")
        scanned_at = cur.fetchone()[0]
        blobs_total = kept_referenced = kept_within_grace = 0
        candidates: dict[str, int] = {}
        # Facts only, never bytes. now() is the snapshot's transaction start.
        cur.execute(
            """
            SELECT id, COALESCE(size_bytes, 0),
                   last_used_at < now() - make_interval(secs => %s)
              FROM okg.blobs
             WHERE kind = %s
            """,
            (grace_seconds, kind),
        )
        for blob_id, size_bytes, past_grace in cur:
            blobs_total += 1
            if blob_id in scan.referenced:
                kept_referenced += 1
            elif not past_grace:
                kept_within_grace += 1
            else:
                candidates[str(blob_id)] = int(size_bytes)
    return BlobPrunePlan(
        kind=kind,
        grace_seconds=grace_seconds,
        scan=scan,
        blobs_total=blobs_total,
        kept_referenced=kept_referenced,
        kept_within_grace=kept_within_grace,
        candidates=candidates,
        scanned_at=scanned_at,
    )


_NO_GRAPH_NOTE = (
    "no graph node storage is registered in this database, so no row reads "
    "as referenced; --apply refuses (blob_prune_no_graph) rather than delete "
    "every row past grace"
)


def _require_graph(plan: BlobPrunePlan) -> None:
    if plan.scan.graph_registered:
        return
    raise BlobPruneNoGraph(
        f"refusing to delete {plan.kind} blob rows: no graph node storage is "
        "registered in this database, so the reference scan saw no graph and "
        "every row past grace would be deleted",
        data={
            "kind": plan.kind,
            "blobs_total": plan.blobs_total,
            "candidate_count": len(plan.candidates),
        },
        fix_hint=(
            "publish a generation first (a fresh environment clone keeps "
            "okg.blobs but not the graph), then re-run the cleanup"
        ),
    )


def _chunks(items: Sequence[str], size: int) -> Iterable[list[str]]:
    for start in range(0, len(items), size):
        yield list(items[start:start + size])


def apply_blob_prune(
    conn: psycopg.Connection[Any],
    plan: BlobPrunePlan,
    *,
    delete_batch_size: int = DEFAULT_DELETE_BATCH_SIZE,
    lock_timeout: str = DEFAULT_LOCK_TIMEOUT,
) -> dict[str, Any]:
    """Delete `plan`'s candidates in bounded batches; return what happened.

    Each batch is its own transaction. Its DELETE re-checks the grace window,
    so a candidate refreshed since the scan (a cache hit's ``touch``) is
    kept and counted as ``refreshed_since_scan``.
    """
    if delete_batch_size < 1:
        raise SubstrateError(
            "delete_batch_size must be at least 1",
            data={"delete_batch_size": delete_batch_size},
        )
    _require_graph(plan)
    deleted: dict[str, int] = {}
    ids = sorted(plan.candidates)
    for chunk in _chunks(ids, delete_batch_size):
        try:
            with conn.transaction(), conn.cursor() as cur:
                cur.execute(
                    "SELECT set_config('lock_timeout', %s, true)", (lock_timeout,)
                )
                cur.execute(
                    """
                    DELETE FROM okg.blobs
                     WHERE kind = %s
                       AND id = ANY(%s)
                       AND last_used_at < now() - make_interval(secs => %s)
                    RETURNING id, COALESCE(size_bytes, 0)
                    """,
                    (plan.kind, chunk, plan.grace_seconds),
                )
                for blob_id, size_bytes in cur.fetchall():
                    deleted[str(blob_id)] = int(size_bytes)
        except psycopg.errors.LockNotAvailable as exc:
            # Earlier batches are committed; say how far this run got.
            raise SubstrateError(
                f"blob cleanup stopped: a {plan.kind} row stayed locked past "
                f"lock_timeout={lock_timeout}",
                data={
                    "reason": "blob_prune_lock_timeout",
                    "kind": plan.kind,
                    "deleted_count": len(deleted),
                    "deleted_bytes": sum(deleted.values()),
                    "candidates_left": len(ids) - len(deleted),
                },
                fix_hint="re-run the command; committed batches stay deleted",
            ) from exc
    skipped = sorted(set(ids) - set(deleted))
    refreshed: list[str] = []
    if skipped:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM okg.blobs WHERE kind = %s AND id = ANY(%s)",
                (plan.kind, skipped),
            )
            refreshed = sorted(str(row[0]) for row in cur.fetchall())
    return {
        "deleted": deleted,
        "refreshed_since_scan": refreshed,
        "gone_since_scan": sorted(set(skipped) - set(refreshed)),
    }


def _sample(ids: Iterable[str], limit: int) -> list[str]:
    return sorted(ids)[:limit]


def plan_report(
    plan: BlobPrunePlan,
    *,
    dry_run: bool,
    applied: Mapping[str, Any] | None,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
) -> dict[str, Any]:
    """The JSON report ``okg db prune-blobs`` prints."""
    report: dict[str, Any] = {
        "present": True,
        "kind": plan.kind,
        "dry_run": dry_run,
        "grace_seconds": plan.grace_seconds,
        "graph_registered": plan.scan.graph_registered,
        "scanned": plan.scan.sources,
        "rows_scanned": sum(int(s["rows_scanned"]) for s in plan.scan.sources),
        "scan_seconds": round(sum(float(s["seconds"]) for s in plan.scan.sources), 3),
        "referenced_ids": len(plan.scan.referenced),
        "blobs_total": plan.blobs_total,
        "kept": {
            "referenced": plan.kept_referenced,
            "within_grace": plan.kept_within_grace,
        },
        "candidate_count": len(plan.candidates),
        "candidate_bytes": sum(plan.candidates.values()),
    }
    if not plan.scan.graph_registered:
        report["note"] = _NO_GRAPH_NOTE
        report["apply_refused"] = BlobPruneNoGraph.code
    if applied is None:
        report.update(
            deleted_count=0,
            deleted_bytes=0,
            would_delete_sample=_sample(plan.candidates, sample_limit),
        )
        return report
    deleted: Mapping[str, int] = applied["deleted"]
    refreshed = list(applied["refreshed_since_scan"])
    report["kept"]["refreshed_since_scan"] = len(refreshed)
    report.update(
        deleted_count=len(deleted),
        deleted_bytes=sum(deleted.values()),
        deleted_sample=_sample(deleted, sample_limit),
        refreshed_since_scan_sample=_sample(refreshed, sample_limit),
        gone_since_scan=len(applied["gone_since_scan"]),
    )
    return report


def prune_blobs(
    dsn: str,
    *,
    kind: str,
    confirm: bool = False,
    grace_seconds: int = DEFAULT_GRACE_SECONDS,
    scan_batch_rows: int = DEFAULT_SCAN_BATCH_ROWS,
    delete_batch_size: int = DEFAULT_DELETE_BATCH_SIZE,
    lock_timeout: str = DEFAULT_LOCK_TIMEOUT,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
) -> dict[str, Any]:
    """Dry-run or delete unreferenced ``okg.blobs`` rows of `kind`.

    Dry run unless `confirm` is set.
    """
    require_grace_seconds(grace_seconds)
    _require_kind(kind)
    started = time.monotonic()
    with db_connect(dsn, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.blobs')")
            present = cur.fetchone()[0] is not None
        conn.commit()
        if not present:
            # Nothing to prune before migration 0037 has run.
            return {
                "present": False,
                "kind": kind,
                "dry_run": not confirm,
                "grace_seconds": grace_seconds,
                "candidate_count": 0,
                "deleted_count": 0,
            }
        plan = plan_blob_prune(
            conn,
            kind=kind,
            grace_seconds=grace_seconds,
            scan_batch_rows=scan_batch_rows,
            lock_timeout=lock_timeout,
        )
        applied = (
            apply_blob_prune(
                conn,
                plan,
                delete_batch_size=delete_batch_size,
                lock_timeout=lock_timeout,
            )
            if confirm
            else None
        )
    report = plan_report(
        plan, dry_run=not confirm, applied=applied, sample_limit=sample_limit
    )
    report["seconds"] = round(time.monotonic() - started, 3)
    return report
