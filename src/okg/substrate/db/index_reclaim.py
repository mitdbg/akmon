"""Reclaim index allocation that outlived its rows: ``okg db reclaim-index-space``.

okg#3112. A full publish writes ``okg.attribute_effective_view_intervals``
inside the one publication transaction. When that
transaction fails or is cancelled, PostgreSQL rolls the rows back but keeps
every btree page they allocated; VACUUM then truncates the heaps and never
shrinks an index. The okg#1324 detector (``okg doctor --check disk``, finding
``index_larger_than_table``) reports such tables and prints a REINDEX command.
This module is the supported way to run it:

* it selects exactly the okg-schema tables the detector flags, by the
  detector's own query and thresholds;
* the default is a dry run that lists the tables, their bytes and the
  statements it would run, and fails (``dry_run_blocked``) when ``--apply``
  would be refused now;
* ``apply=True`` holds the deployment's publisher lock for the whole run (the
  lock ``history_compaction.offline_reclaim_history_storage`` takes), so it
  refuses while a publish holds it and no publish can start under it (an
  ingest's end-of-run publish then gets ``PublisherLeaseHeld``); before any
  read it refuses while a source run of the deployment is in progress
  (``ingest.run_base.runs_in_progress``, the signal that refuses a publish),
  because a run writes graph rows without the publisher lock; a writer that
  starts later is caught by the REINDEX's bounded wait (``reindex_failed``,
  after which the verb drops the invalid ``*_ccnew`` index it left) or by the
  fingerprint (``fingerprint_changed``); it refuses when any target table
  carries an invalid index (for example a
  ``*_ccnew`` left by an interrupted ``REINDEX CONCURRENTLY``); it bounds
  every lock wait with ``lock_timeout``;
* it runs ``REINDEX TABLE CONCURRENTLY`` one table at a time, outside any
  transaction, so readers and writers are never blocked;
* it reads an authority fingerprint before and after and raises with both
  values on any difference, naming the tables it already rebuilt (a rebuilt
  table stays rebuilt; nothing is claimed as a success).

The fingerprint covers, per target table: every index definition and its
validity, readiness, uniqueness, primary, clustered and replica-identity
flags, index comments, owners, reloptions and tablespaces; every constraint
definition and its validation and deferral flags; user triggers; the table
owner, access list, reloptions, persistence and comment; and the row count
plus a digest of every row. Beyond the targets it digests every row of
``okg.attribute_versions``, ``okg.generation_ordinals``, the Chronos
checkpoint table, the record-set and record-authority tables
(``okg.source_record_set*``, ``okg.source_record_authorit*``) and the graph
row tables (``_chronos_b_interval_okg_graph_*``).

A row digest is ``count(*)`` and two 64-bit sums over ``md5(row::text)``. A
sum is order-independent and constant-memory, so the digest scales to
deployments whose rows would not fit one ``string_agg``. It reads every row
of every table above twice: on a large deployment that is minutes of I/O.
A source run that starts after the in-progress check still writes graph
rows; the fingerprint then differs and the verb reports it (the rebuild
itself does not change rows), so run it when no ingest is scheduled.

:func:`rebuild_indexes_after_publish` is the automatic counterpart (okg#3112
step 1): after a successful publish it rebuilds, one ``REINDEX INDEX
CONCURRENTLY`` at a time, the indexes of the publish-written tables that are
well over their estimated packed size. It takes the publisher lock only for
one statement at a time, only without waiting, and not at all while a
publish waits for it, so it never lengthens a publish's own lock hold,
never runs beside a publish, and delays a starting publish by one statement
at most. It reads no
fingerprint: it holds no lock across its run, and a rebuild changes no row.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from contextlib import ExitStack
from typing import Any, Callable, Mapping, Sequence

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from okg.substrate.db.connection import connect as db_connect
from okg.substrate.errors import SubstrateError

_log = logging.getLogger(__name__)

RECLAIM_OPERATION = "index space reclaim"
DEFAULT_PUBLISH_LOCK_WAIT_SECONDS = 5.0
DEFAULT_LOCK_TIMEOUT_SECONDS = 30.0

__all__ = [
    "DEFAULT_LOCK_TIMEOUT_SECONDS",
    "DEFAULT_POST_PUBLISH_REBUILD_MIN_BYTES",
    "DEFAULT_POST_PUBLISH_REBUILD_RATIO",
    "DEFAULT_PUBLISH_LOCK_WAIT_SECONDS",
    "POST_PUBLISH_REBUILD_ENV",
    "POST_PUBLISH_REBUILD_MIN_BYTES_ENV",
    "POST_PUBLISH_REBUILD_RATIO_ENV",
    "authority_fingerprint",
    "fingerprint_differences",
    "judge_indexes",
    "packed_btree_bytes",
    "rebuild_indexes_after_publish",
    "reclaim_index_space",
]

# Authority tables outside the targets, found by name so a deployment that
# has not created one (no record authority yet, say) is reported as such
# rather than failing. The checkpoint and graph tables live in the Chronos
# schema (``public`` on dev), so they are matched in any schema.
_AUTHORITY_TABLES_SQL = r"""
SELECT n.nspname AS schema, c.relname AS relation,
       CASE
         WHEN n.nspname = 'okg' AND c.relname = 'attribute_versions'
           THEN 'attribute_versions'
         WHEN n.nspname = 'okg' AND c.relname = 'generation_ordinals'
           THEN 'generation_ordinals'
         WHEN c.relname = '_chronos_branch_interval_checkpoints'
           THEN 'checkpoints'
         WHEN n.nspname = 'okg'
              AND (c.relname LIKE 'source\_record\_set%'
                   OR c.relname LIKE 'source\_record\_authorit%')
           THEN 'record_authority'
         ELSE 'graph_rows'
       END AS authority
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relkind = 'r'
   AND (
        (n.nspname = 'okg'
         AND c.relname IN ('attribute_versions', 'generation_ordinals'))
     OR c.relname = '_chronos_branch_interval_checkpoints'
     OR (n.nspname = 'okg'
         AND (c.relname LIKE 'source\_record\_set%'
              OR c.relname LIKE 'source\_record\_authorit%'))
     OR c.relname LIKE '\_chronos\_b\_interval\_okg\_graph\_%'
   )
 ORDER BY 3, 1, 2
"""

#: Authority classes the fingerprint must find on a migrated deployment.
_REQUIRED_AUTHORITY = ("attribute_versions", "generation_ordinals", "checkpoints")


def _text(value: Any) -> Any:
    """JSON-stable scalar: keep None and bool, render everything else as text."""
    if value is None or isinstance(value, bool):
        return value
    return str(value)


def _row_digest(cur: Any, schema: str, relation: str) -> dict[str, Any]:
    cur.execute(
        sql.SQL(
            """
            SELECT count(*) AS n,
                   coalesce(sum(('x' || substr(h, 1, 16))::bit(64)::bigint::numeric), 0)::text AS hi,
                   coalesce(sum(('x' || substr(h, 17, 16))::bit(64)::bigint::numeric), 0)::text AS lo
              FROM (SELECT md5(t::text) AS h FROM {} t) s
            """
        ).format(sql.Identifier(schema, relation))
    )
    row = cur.fetchone()
    return {"rows": int(row["n"]), "digest": f"{row['hi']}:{row['lo']}"}


def _table_definition(cur: Any, schema: str, relation: str) -> dict[str, Any]:
    qualified = sql.Identifier(schema, relation).as_string(cur.connection)
    cur.execute(
        """
        SELECT ic.relname AS name,
               pg_get_indexdef(i.indexrelid) AS definition,
               i.indisvalid AS valid, i.indisready AS ready, i.indislive AS live,
               i.indisunique AS is_unique, i.indisprimary AS is_primary,
               i.indisclustered AS clustered, i.indisreplident AS replident,
               obj_description(i.indexrelid, 'pg_class') AS comment,
               pg_get_userbyid(ic.relowner) AS owner,
               ic.reloptions::text AS reloptions,
               coalesce(ts.spcname, '') AS tablespace
          FROM pg_index i
          JOIN pg_class ic ON ic.oid = i.indexrelid
          LEFT JOIN pg_tablespace ts ON ts.oid = ic.reltablespace
         WHERE i.indrelid = %s::regclass
         ORDER BY ic.relname
        """,
        (qualified,),
    )
    indexes = [{k: _text(v) for k, v in row.items()} for row in cur.fetchall()]
    cur.execute(
        """
        SELECT conname AS name, contype AS type,
               pg_get_constraintdef(oid) AS definition,
               convalidated AS validated, condeferrable AS deferrable,
               condeferred AS deferred
          FROM pg_constraint
         WHERE conrelid = %s::regclass
         ORDER BY conname
        """,
        (qualified,),
    )
    constraints = [{k: _text(v) for k, v in row.items()} for row in cur.fetchall()]
    cur.execute(
        """
        SELECT tgname AS name, pg_get_triggerdef(oid) AS definition,
               tgenabled AS enabled
          FROM pg_trigger
         WHERE tgrelid = %s::regclass AND NOT tgisinternal
         ORDER BY tgname
        """,
        (qualified,),
    )
    triggers = [{k: _text(v) for k, v in row.items()} for row in cur.fetchall()]
    cur.execute(
        """
        SELECT pg_get_userbyid(relowner) AS owner, relacl::text AS acl,
               reloptions::text AS reloptions, relpersistence AS persistence,
               obj_description(oid, 'pg_class') AS comment
          FROM pg_class WHERE oid = %s::regclass
        """,
        (qualified,),
    )
    table = {k: _text(v) for k, v in cur.fetchone().items()}
    return {
        "table": table,
        "indexes": indexes,
        "constraints": constraints,
        "triggers": triggers,
        **_row_digest(cur, schema, relation),
    }


def authority_fingerprint(
    cur: Any, targets: Sequence[tuple[str, str]]
) -> dict[str, Any]:
    """Read the definitions and row digests a rebuild must leave unchanged.

    ``cur`` must use ``dict_row``. ``targets`` are ``(schema, relation)``
    pairs: each gets the full definition fingerprint. Every authority table
    (module docstring) gets a row digest.
    """
    tables = {
        f"{schema}.{relation}": _table_definition(cur, schema, relation)
        for schema, relation in targets
    }
    cur.execute(_AUTHORITY_TABLES_SQL)
    found = [dict(row) for row in cur.fetchall()]
    authority: dict[str, Any] = {}
    for row in found:
        name = f"{row['schema']}.{row['relation']}"
        authority.setdefault(str(row["authority"]), {})[name] = _row_digest(
            cur, str(row["schema"]), str(row["relation"])
        )
    missing = [cls for cls in _REQUIRED_AUTHORITY if cls not in authority]
    if missing:
        raise SubstrateError(
            "index reclaim cannot read its authority fingerprint",
            data={"reason": "authority_table_missing", "missing": missing},
            fix_hint="run `okg migrate` for this deployment, then retry",
        )
    return {"tables": tables, "authority": authority}


def fingerprint_differences(
    before: Any, after: Any, path: str = ""
) -> list[dict[str, Any]]:
    """Every leaf where ``before`` and ``after`` differ, with both values."""
    if isinstance(before, Mapping) and isinstance(after, Mapping):
        out: list[dict[str, Any]] = []
        for key in sorted(set(before) | set(after), key=str):
            sub = f"{path}.{key}" if path else str(key)
            if key not in before or key not in after:
                out.append({
                    "path": sub,
                    "before": before.get(key, "<absent>"),
                    "after": after.get(key, "<absent>"),
                })
            else:
                out.extend(fingerprint_differences(before[key], after[key], sub))
        return out
    if before != after:
        return [{"path": path, "before": before, "after": after}]
    return []


def _digest(value: Any) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _invalid_indexes(cur: Any, schema: str, relation: str) -> list[dict[str, Any]]:
    qualified = sql.Identifier(schema, relation).as_string(cur.connection)
    cur.execute(
        """
        SELECT n.nspname AS schema, ic.relname AS index,
               i.indisvalid AS valid, i.indisready AS ready, i.indislive AS live,
               pg_total_relation_size(i.indexrelid) AS index_bytes
          FROM pg_index i
          JOIN pg_class ic ON ic.oid = i.indexrelid
          JOIN pg_namespace n ON n.oid = ic.relnamespace
         WHERE i.indrelid = %s::regclass
           AND NOT (i.indisvalid AND i.indisready AND i.indislive)
         ORDER BY ic.relname
        """,
        (qualified,),
    )
    out = []
    for row in cur.fetchall():
        index = sql.Identifier(str(row["schema"]), str(row["index"])).as_string(
            cur.connection
        )
        out.append({
            "relation": f"{schema}.{relation}",
            "index": f"{row['schema']}.{row['index']}",
            "valid": bool(row["valid"]),
            "ready": bool(row["ready"]),
            "live": bool(row["live"]),
            "index_bytes": int(row["index_bytes"]),
            "drop_command": f"DROP INDEX CONCURRENTLY {index};",
        })
    return out


def _sizes(cur: Any, schema: str, relation: str) -> dict[str, int]:
    qualified = sql.Identifier(schema, relation).as_string(cur.connection)
    cur.execute(
        "SELECT pg_table_size(%(r)s::regclass) AS table_bytes, "
        "pg_indexes_size(%(r)s::regclass) AS index_bytes",
        {"r": qualified},
    )
    row = cur.fetchone()
    return {"table_bytes": int(row["table_bytes"]), "index_bytes": int(row["index_bytes"])}


def _select_targets(
    cur: Any, *, ratio: float | None, min_index_bytes: int | None
) -> list[dict[str, Any]]:
    from okg.substrate.db.management import _disk_index_bloat

    flagged = _disk_index_bloat(
        cur, schema="okg", ratio=ratio, min_index_bytes=min_index_bytes
    )
    out = []
    for table in flagged:
        schema, relation = str(table["schema"]), str(table["relation"])
        command = (
            "REINDEX TABLE CONCURRENTLY "
            + sql.Identifier(schema, relation).as_string(cur.connection)
            + ";"
        )
        out.append({
            "schema": schema,
            "relation": relation,
            "qualified": f"{schema}.{relation}",
            "table_bytes": int(table["table_bytes"]),
            "index_bytes": int(table["index_bytes"]),
            "est_rows": table["est_rows"],
            "indexes": table["indexes"],
            "command": command,
        })
    return out


def _thresholds(ratio: float | None, min_index_bytes: int | None) -> dict[str, Any]:
    from okg.substrate.db import management

    return {
        "ratio": float(
            ratio if ratio is not None else management._DISK_INDEX_BLOAT_RATIO
        ),
        "min_index_bytes": int(
            min_index_bytes
            if min_index_bytes is not None
            else management._DISK_INDEX_BLOAT_MIN_INDEX_BYTES
        ),
    }


def _reindex_table(cur: Any, schema: str, relation: str) -> None:
    """Run one ``REINDEX TABLE CONCURRENTLY`` (autocommit, outside a transaction)."""
    cur.execute(
        sql.SQL("REINDEX TABLE CONCURRENTLY {}").format(
            sql.Identifier(schema, relation)
        )
    )


def _rerun_hint(deployment: str | None) -> str:
    return (
        f"okg db reclaim-index-space --deployment {deployment or '<name>'} --apply"
    )


def _try_publisher_lock(cur: Any, deployment: str) -> bool:
    """True when nobody holds the publisher lock (taken and released at once)."""
    from okg.substrate.runtime.source_sync_priority import (
        publisher_publish_advisory_keys,
    )

    classid, objid = publisher_publish_advisory_keys(deployment)
    cur.execute("SELECT pg_try_advisory_lock(%s, %s) AS got", (classid, objid))
    got = bool(cur.fetchone()["got"])
    if got:
        cur.execute("SELECT pg_advisory_unlock(%s, %s)", (classid, objid))
    return got


def _source_runs(conn: psycopg.Connection, deployment: str) -> list[dict[str, Any]]:
    from okg.substrate.ingest.run_base import runs_in_progress

    return runs_in_progress(conn, deployment=deployment)


def _source_run_refusal(deployment: str, running: list[dict[str, Any]]) -> SubstrateError:
    first = running[0]
    return SubstrateError(
        "index reclaim refused: a source run is still writing graph rows",
        data={
            "reason": "source_run_in_progress",
            "deployment": deployment,
            "in_progress": running[:10],
        },
        fix_hint=(
            f"run {first.get('run_id')} of source {first.get('source')} writes "
            "rows the fingerprint reads; wait for it to finish (its own "
            "end-of-run publish included), then rerun "
            f"`{_rerun_hint(deployment)}`"
        ),
    )


def _blockers(
    conn: psycopg.Connection,
    cur: Any,
    *,
    deployment: str | None,
    targets: Sequence[Mapping[str, Any]],
) -> tuple[list[str], dict[str, Any]]:
    """What would refuse ``--apply`` right now (dry run; changes nothing)."""
    blocked: list[str] = []
    detail: dict[str, Any] = {}
    invalid = [
        bad for t in targets for bad in _all_invalid_indexes(cur, t["schema"], t["relation"])
    ]
    detail["invalid_indexes"] = invalid
    blocked += [f"invalid_index:{bad['index']}" for bad in invalid]
    if not deployment:
        return blocked, detail
    if not _try_publisher_lock(cur, deployment):
        blocked.append("publisher_lock_held")
    running = _source_runs(conn, deployment)
    detail["source_runs_in_progress"] = running[:10]
    blocked += [f"source_run_in_progress:{item['run_id']}" for item in running]
    return blocked, detail


def _reset_session(cur: Any) -> None:
    """Undo the rebuild's session settings when the connection still works.

    Never raises: a failure here must not replace the error that is already
    on its way out, and the settings are local to a connection the verb
    closes anyway.
    """
    conn = cur.connection
    if conn.closed or conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR:
        return
    try:
        cur.execute("RESET lock_timeout")
        cur.execute("RESET statement_timeout")
    except psycopg.Error as exc:  # noqa: BLE001 -- reported, never masks
        _log.warning("index reclaim could not reset its session settings: %s", exc)


def reclaim_index_space(
    dsn: str,
    *,
    deployment: str | None,
    apply: bool = False,
    ratio: float | None = None,
    min_index_bytes: int | None = None,
    publish_lock_wait_seconds: float = DEFAULT_PUBLISH_LOCK_WAIT_SECONDS,
    lock_timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
    on_progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Dry-run or apply ``REINDEX TABLE CONCURRENTLY`` on detector-flagged tables.

    Returns the payload ``okg db reclaim-index-space`` prints. Raises
    :class:`SubstrateError` on every refusal and failure, with
    ``data["reason"]`` one of: ``dry_run_blocked`` (a dry run found what
    would refuse ``--apply``; ``data["blocked_by"]`` names it),
    ``deployment_required``, ``publisher_lock_held``,
    ``source_run_in_progress``,
    ``invalid_index_present``, ``reindex_failed``, ``fingerprint_changed``
    or ``authority_table_missing``.
    """
    if lock_timeout_seconds <= 0:
        raise SubstrateError(
            "index reclaim needs a positive lock timeout",
            data={"reason": "lock_timeout_invalid", "lock_timeout_seconds": lock_timeout_seconds},
        )
    thresholds = _thresholds(ratio, min_index_bytes)
    say = on_progress or (lambda _msg: None)
    with db_connect(dsn, connect_timeout=5, autocommit=True) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT current_database() AS db")
            database = str(cur.fetchone()["db"])
            if not apply:
                targets = _select_targets(cur, **thresholds)
                blocked, detail = _blockers(
                    conn, cur, deployment=deployment, targets=targets
                )
                cur.execute(_AUTHORITY_TABLES_SQL)
                scope = [f"{r['schema']}.{r['relation']}" for r in cur.fetchall()]
                payload = {
                    "ok": not blocked,
                    "check": "reclaim-index-space",
                    "database": database,
                    "deployment": deployment,
                    "dry_run": True,
                    "applied": False,
                    "thresholds": thresholds,
                    "tables": targets,
                    "would_run": [t["command"] for t in targets],
                    **detail,
                    "blocked_by": blocked,
                    "fingerprint_scope": {
                        "tables": [t["qualified"] for t in targets],
                        "authority": scope,
                    },
                    "index_bytes": sum(t["index_bytes"] for t in targets),
                }
                if blocked:
                    raise SubstrateError(
                        "index reclaim dry run: --apply would be refused now",
                        data={"reason": "dry_run_blocked", **payload},
                        fix_hint=(
                            "resolve each blocked_by entry (see the fields "
                            "named after it), then rerun the dry run"
                        ),
                    )
                return payload
            if not deployment:
                raise SubstrateError(
                    "index reclaim --apply needs the deployment name for its publisher lock",
                    data={"reason": "deployment_required"},
                    fix_hint=_rerun_hint(None),
                )
            return _apply(
                conn,
                cur,
                database=database,
                deployment=deployment,
                thresholds=thresholds,
                publish_lock_wait_seconds=publish_lock_wait_seconds,
                lock_timeout_seconds=lock_timeout_seconds,
                say=say,
            )


def _apply(
    conn: psycopg.Connection,
    cur: Any,
    *,
    database: str,
    deployment: str,
    thresholds: Mapping[str, Any],
    publish_lock_wait_seconds: float,
    lock_timeout_seconds: float,
    say: Callable[[str], None],
) -> dict[str, Any]:
    from okg.substrate.runtime.source_sync_priority import (
        publisher_publish_lock_on_connection,
    )

    with ExitStack() as stack:
        publish_lock = publisher_publish_lock_on_connection(
            conn=conn,
            deployment=deployment,
            wait_seconds=publish_lock_wait_seconds,
            operation=RECLAIM_OPERATION,
        )
        try:
            publish_lock.__enter__()
        except SubstrateError as exc:
            raise SubstrateError(
                "index reclaim refused: a publish holds the deployment's publisher lock",
                data={"reason": "publisher_lock_held", **exc.data},
                fix_hint=(
                    "a publish is running (the publisher worker or an "
                    "ingest's end-of-run publish); wait for it to finish, "
                    f"then rerun `{_rerun_hint(deployment)}`, or raise "
                    "--publish-lock-wait-seconds"
                ),
            ) from exc
        # The shared helper's release runs an unlock that raises on a lost
        # connection; released here so it can never replace the verb's own
        # error (closing the session releases the lock anyway).
        stack.callback(_release_publish_lock_quietly, publish_lock)
        # A source run writes graph rows and run-base checkpoints without the
        # publisher lock; refuse before any read rather than rebuild every
        # table and then fail on a fingerprint the run changed.
        running = _source_runs(conn, deployment)
        if running:
            raise _source_run_refusal(deployment, running)
        # Select under the lock: a publish that finished while we waited may
        # have changed which tables the detector flags.
        targets = _select_targets(cur, **thresholds)
        invalid = [
            bad for t in targets for bad in _all_invalid_indexes(cur, t["schema"], t["relation"])
        ]
        if invalid:
            raise SubstrateError(
                "index reclaim refused: a target table has an invalid index",
                data={"reason": "invalid_index_present", "invalid_indexes": invalid},
                fix_hint=(
                    "an earlier REINDEX CONCURRENTLY or CREATE INDEX CONCURRENTLY "
                    "was interrupted; confirm nothing is building it, run each "
                    "drop_command, then retry"
                ),
            )
        base = {
            "ok": True,
            "check": "reclaim-index-space",
            "database": database,
            "deployment": deployment,
            "dry_run": False,
            "thresholds": dict(thresholds),
        }
        if not targets:
            return {**base, "applied": False, "tables": [], "reason": "nothing_flagged"}
        pairs = [(t["schema"], t["relation"]) for t in targets]
        cur.execute(
            sql.SQL("SET lock_timeout = {}").format(
                sql.Literal(f"{int(lock_timeout_seconds * 1000)}ms")
            )
        )
        # A rebuild interrupted by a statement timeout leaves an invalid
        # *_ccnew index behind; the lock wait is what must be bounded.
        cur.execute("SET statement_timeout = 0")
        seconds: dict[str, float] = {}
        try:
            say("reading the authority fingerprint (before)")
            before = authority_fingerprint(cur, pairs)
            sizes_before = {t["qualified"]: _sizes(cur, *p) for t, p in zip(targets, pairs)}
            for target, (schema, relation) in zip(targets, pairs):
                say(f"REINDEX TABLE CONCURRENTLY {target['qualified']}")
                started = time.monotonic()
                existing = _index_names(cur, schema, relation)
                try:
                    _reindex_table(cur, schema, relation)
                except psycopg.Error as exc:
                    raise _reindex_failure(
                        cur, exc, target=target, targets=targets, seconds=seconds,
                        schema=schema, relation=relation, existing=existing,
                    ) from exc
                seconds[target["qualified"]] = round(time.monotonic() - started, 3)
            say("reading the authority fingerprint (after)")
            after = authority_fingerprint(cur, pairs)
        finally:
            _reset_session(cur)
        differences = fingerprint_differences(before, after)
        if differences:
            raise SubstrateError(
                "index reclaim changed the authority fingerprint after "
                f"rebuilding {', '.join(seconds)}",
                data={
                    "reason": "fingerprint_changed",
                    "rebuilt": list(seconds),
                    "differences": differences,
                    "fingerprint_before": before,
                    "fingerprint_after": after,
                },
                fix_hint=(
                    "the tables named in rebuilt are already rebuilt; do not "
                    "trust them until the differences are explained: check for "
                    "a concurrent ingest or DDL before publishing"
                ),
            )
        sizes_after = {t["qualified"]: _sizes(cur, *p) for t, p in zip(targets, pairs)}
        from okg.substrate.db.management import _disk_index_bloat

        still_flagged = [
            str(f["qualified"])
            for f in _disk_index_bloat(cur, schema="okg", **thresholds)
            if str(f["qualified"]) in sizes_after
        ]
    before_bytes = sum(v["index_bytes"] for v in sizes_before.values())
    after_bytes = sum(v["index_bytes"] for v in sizes_after.values())
    return {
        **base,
        "applied": True,
        "tables": [
            {
                "relation": t["qualified"],
                "command": t["command"],
                "table_bytes": sizes_after[t["qualified"]]["table_bytes"],
                "index_bytes_before": sizes_before[t["qualified"]]["index_bytes"],
                "index_bytes_after": sizes_after[t["qualified"]]["index_bytes"],
                "seconds": seconds[t["qualified"]],
            }
            for t in targets
        ],
        "index_bytes_before": before_bytes,
        "index_bytes_after": after_bytes,
        "reclaimed_bytes": before_bytes - after_bytes,
        "fingerprint": {
            "equal": True,
            "digest": _digest(before),
            "tables": sorted(before["tables"]),
            "authority": {k: sorted(v) for k, v in before["authority"].items()},
        },
        "still_flagged": still_flagged,
    }


def _reindex_failure(
    cur: Any,
    exc: psycopg.Error,
    *,
    target: Mapping[str, Any],
    targets: Sequence[Mapping[str, Any]],
    seconds: Mapping[str, float],
    schema: str,
    relation: str,
    existing: set[str],
) -> SubstrateError:
    """Describe a failed rebuild, dropping the invalid indexes it left.

    Never raises over the original error. Only indexes this run created (not
    in ``existing``, taken just before the REINDEX) are dropped; the verb
    refuses to start while any invalid index exists, so every invalid index
    here is this run's ``*_ccnew``.
    """
    data: dict[str, Any] = {
        "reason": "reindex_failed",
        "relation": target["qualified"],
        "error": f"{type(exc).__name__}: {exc}",
        "rebuilt": list(seconds),
        "not_started": [
            t["qualified"] for t in targets
            if t["qualified"] not in seconds and t["qualified"] != target["qualified"]
        ],
    }
    conn = cur.connection
    if conn.closed or conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR:
        data["invalid_indexes_left"] = None
        data["invalid_index_check_error"] = "connection unusable after the failure"
    else:
        try:
            dropped, left = _drop_own_invalid_indexes(cur, schema, relation, existing)
            data["invalid_indexes_dropped"] = dropped
            data["invalid_indexes_left"] = left
            data["toast_invalid_indexes_left"] = _toast_invalid_indexes(
                cur, schema, relation
            )
        except psycopg.Error as check:  # noqa: BLE001 -- reported, never masks
            data["invalid_indexes_left"] = None
            data["invalid_index_check_error"] = f"{type(check).__name__}: {check}"
    return SubstrateError(
        f"REINDEX TABLE CONCURRENTLY failed on {target['qualified']}",
        data=data,
        fix_hint=(
            "the tables named in rebuilt are rebuilt; this one is not. The "
            "verb dropped the invalid indexes it left (invalid_indexes_dropped); "
            "run each drop_command in invalid_indexes_left (or list invalid "
            "indexes when it is null), then retry when no long transaction "
            "holds the table"
        ),
    )


def _index_names(cur: Any, schema: str, relation: str) -> set[str]:
    qualified = sql.Identifier(schema, relation).as_string(cur.connection)
    cur.execute(
        "SELECT n.nspname || '.' || ic.relname AS name FROM pg_index i "
        "JOIN pg_class ic ON ic.oid = i.indexrelid "
        "JOIN pg_namespace n ON n.oid = ic.relnamespace "
        "WHERE i.indrelid = %s::regclass",
        (qualified,),
    )
    return {str(row["name"]) for row in cur.fetchall()}


#: PostgreSQL names REINDEX CONCURRENTLY's transient copies after the index,
#: ``<name>_ccnew``/``<name>_ccold`` plus a number when that name is taken,
#: truncating ``<name>`` so the whole fits NAMEDATALEN - 1 (63) bytes.
_REINDEX_ARTIFACT = re.compile(r"^(?P<base>.+)_cc(?:new|old)\d*$")
_MAX_NAME_BYTES = 63


def is_reindex_artifact(candidate: str, existing: set[str]) -> bool:
    """True when ``candidate`` (``schema.name``) is a REINDEX CONCURRENTLY copy
    of an index in ``existing`` (``schema.name``, recorded before the REINDEX).

    Anything else -- a third party's CREATE INDEX CONCURRENTLY still being
    built, say -- is not this run's and is never dropped.
    """
    schema, _, name = candidate.partition(".")
    match = _REINDEX_ARTIFACT.match(name)
    if match is None or candidate in existing:
        return False
    base = match.group("base")
    for original in existing:
        o_schema, _, o_name = original.partition(".")
        if o_schema != schema:
            continue
        if o_name == base:
            return True
        # Truncated base: only when the full name would not have fit.
        suffix = name[len(base):]
        if (
            o_name.startswith(base)
            and len((o_name + suffix).encode("utf-8")) > _MAX_NAME_BYTES
        ):
            return True
    return False


def _drop_own_invalid_indexes(
    cur: Any, schema: str, relation: str, existing: set[str]
) -> tuple[list[str], list[dict[str, Any]]]:
    """DROP INDEX CONCURRENTLY each invalid REINDEX copy of an index in ``existing``.

    Only names that :func:`is_reindex_artifact` matches are dropped.
    Autocommit, outside any transaction, one statement per index under the
    session's bounded ``lock_timeout``. Returns the dropped names and every
    invalid index still present (with its drop command), including any this
    could not drop and any that is not this run's.
    """
    dropped: list[str] = []
    for bad in _invalid_indexes(cur, schema, relation):
        if not is_reindex_artifact(bad["index"], existing):
            continue
        try:
            cur.execute(bad["drop_command"].rstrip(";"))
        except psycopg.Error as exc:  # noqa: BLE001 -- reported as left
            _log.warning("could not drop %s: %s", bad["index"], exc)
            continue
        dropped.append(bad["index"])
    return dropped, _invalid_indexes(cur, schema, relation)


def _all_invalid_indexes(cur: Any, schema: str, relation: str) -> list[dict[str, Any]]:
    """Invalid indexes on the table and on its TOAST relation; either blocks a run."""
    return _invalid_indexes(cur, schema, relation) + _toast_invalid_indexes(
        cur, schema, relation
    )


def _toast_invalid_indexes(cur: Any, schema: str, relation: str) -> list[dict[str, Any]]:
    """Invalid indexes on the table's TOAST relation (a leftover TOAST
    ``_ccnew``), reported with their drop command and never dropped here."""
    qualified = sql.Identifier(schema, relation).as_string(cur.connection)
    cur.execute(
        """
        SELECT tn.nspname AS schema, tc.relname AS toast
          FROM pg_class c
          JOIN pg_class tc ON tc.oid = c.reltoastrelid
          JOIN pg_namespace tn ON tn.oid = tc.relnamespace
         WHERE c.oid = %s::regclass
        """,
        (qualified,),
    )
    row = cur.fetchone()
    if row is None:
        return []
    return _invalid_indexes(cur, str(row["schema"]), str(row["toast"]))


def _release_publish_lock_quietly(publish_lock: Any) -> None:
    """Leave the shared publisher-lock context; never raises."""
    try:
        publish_lock.__exit__(None, None, None)
    except Exception as exc:  # noqa: BLE001 -- reported, never masks
        _log.warning(
            "index reclaim could not release the publisher lock (the session "
            "close releases it): %s", exc,
        )


# --------------------------------------------------------------------------- #
# Rebuild after a successful publish (okg#3112 step 1, PACT lane-h-3112-step1) #
# --------------------------------------------------------------------------- #

#: Kill switch: ``0``, ``false``, ``off`` or ``no`` turns the post-publish
#: rebuild off for the process.
POST_PUBLISH_REBUILD_ENV = "OKG_POST_PUBLISH_INDEX_REBUILD"
#: An index is rebuilt when its size is at least this multiple of its
#: estimated packed size ...
POST_PUBLISH_REBUILD_RATIO_ENV = "OKG_POST_PUBLISH_INDEX_REBUILD_RATIO"
DEFAULT_POST_PUBLISH_REBUILD_RATIO = 1.5
#: ... and the rebuild would return at least this many bytes.
POST_PUBLISH_REBUILD_MIN_BYTES_ENV = "OKG_POST_PUBLISH_INDEX_REBUILD_MIN_BYTES"
DEFAULT_POST_PUBLISH_REBUILD_MIN_BYTES = 64 * 1024 * 1024
#: Lock wait of every rebuild statement. Short: each step holds the
#: publisher lock, and a REINDEX/CREATE/DROP INDEX CONCURRENTLY waits for
#: older transactions under it.
POST_PUBLISH_REBUILD_LOCK_TIMEOUT_ENV = "OKG_POST_PUBLISH_INDEX_REBUILD_LOCK_TIMEOUT_SECONDS"
DEFAULT_POST_PUBLISH_REBUILD_LOCK_TIMEOUT_SECONDS = 5.0
#: No new step starts after this many seconds; the next successful publish
#: resumes. A queued publish waits at most this budget plus one step.
POST_PUBLISH_REBUILD_BUDGET_ENV = "OKG_POST_PUBLISH_INDEX_REBUILD_BUDGET_SECONDS"
DEFAULT_POST_PUBLISH_REBUILD_BUDGET_SECONDS = 30.0

#: The okg-schema tables a publish writes in bulk; the two Chronos graph
#: tables are added by name at run time.
_POST_PUBLISH_OKG_TABLES = (
    "okg.attribute_effective_view_intervals",
    "okg.attribute_effective_view",
    "okg.attribute_versions",
)
_PAGE_BYTES = 8192
#: A btree page minus its page header (24 bytes) and btree special space (16).
_PAGE_USABLE_BYTES = _PAGE_BYTES - 24 - 16
#: A partial index whose rows are fewer than this share of its table's is not
#: judged: the table-wide column widths it would be estimated from may not fit
#: its own rows (measured: the snapshot's per-kind indexes).
_PARTIAL_INDEX_MIN_SHARE = 0.9

_INDEX_ESTIMATE_SQL = """
SELECT tn.nspname AS table_schema, tc.relname AS table_name,
       n.nspname AS index_schema, ic.relname AS index_name,
       am.amname AS access_method,
       (i.indisvalid AND i.indisready AND i.indislive) AS valid,
       i.indpred IS NOT NULL AS partial,
       pg_relation_size(i.indexrelid) AS bytes,
       ic.reltuples::float8 AS index_tuples,
       tc.reltuples::float8 AS table_tuples,
       coalesce((SELECT split_part(o, '=', 2)::int
                   FROM unnest(ic.reloptions) AS o
                  WHERE o LIKE 'fillfactor=%%'), 90) AS fillfactor,
       ARRAY(
         SELECT CASE WHEN i.indkey[k - 1] <> 0 THEN
                  (SELECT (s.avg_width * (1 - s.null_frac))::float8
                     FROM pg_attribute AS a
                     JOIN pg_stats AS s
                       ON s.schemaname = tn.nspname AND s.tablename = tc.relname
                      AND s.attname = a.attname AND NOT s.inherited
                    WHERE a.attrelid = i.indrelid AND a.attnum = i.indkey[k - 1])
                ELSE
                  (SELECT (s.avg_width * (1 - s.null_frac))::float8
                     FROM pg_attribute AS a
                     JOIN pg_stats AS s
                       ON s.schemaname = n.nspname AND s.tablename = ic.relname
                      AND s.attname = a.attname
                    WHERE a.attrelid = i.indexrelid AND a.attnum = k)
                END
           FROM generate_series(1, i.indnatts) AS k
          ORDER BY k
       ) AS widths
  FROM pg_index AS i
  JOIN pg_class AS ic ON ic.oid = i.indexrelid
  JOIN pg_namespace AS n ON n.oid = ic.relnamespace
  JOIN pg_class AS tc ON tc.oid = i.indrelid
  JOIN pg_namespace AS tn ON tn.oid = tc.relnamespace
  JOIN pg_am AS am ON am.oid = ic.relam
 WHERE i.indrelid = ANY(%s::regclass[])
   -- okg#3112 step 2 (design section 4.3 rule 4): the declared-attribute
   -- indexes, and a REINDEX copy of one, belong to their reconciler. By name,
   -- so this works before its manifest table exists.
   AND ic.relname NOT LIKE 'okg\\_dattr\\_%%'
 ORDER BY tn.nspname, tc.relname, ic.relname
"""


def packed_btree_bytes(entries: float, widths: Sequence[float], fillfactor: int) -> int:
    """Estimated size of a freshly built btree holding ``entries`` rows.

    Each entry costs its 8-byte tuple header, the columns' average stored
    widths (from ``pg_stats``), 4 bytes of average alignment padding and a
    4-byte line pointer; leaves fill to ``fillfactor`` percent, as a build
    leaves them; inner pages add about half a percent plus the metapage.
    Deduplication of equal keys is not modelled, so the estimate is high for
    an index with many duplicates, which only makes a rebuild less likely.
    Measured right after ``REINDEX`` on lane H's krama-shaped graph: actual
    size / estimate 0.90-1.11 for every full-table btree.
    """
    per_entry = 8.0 + float(sum(widths)) + 4.0 + 4.0
    usable = _PAGE_USABLE_BYTES * max(10, min(100, int(fillfactor))) / 100.0
    leaves = -(-max(0.0, float(entries)) * per_entry // usable)
    return int((1 + -(-leaves * 1.005 // 1)) * _PAGE_BYTES)


def _post_publish_enabled() -> bool:
    raw = os.environ.get(POST_PUBLISH_REBUILD_ENV)
    if raw is None:
        return True
    return raw.strip().lower() not in {"0", "false", "off", "no"}


def _env_number(name: str, default: float, *, minimum: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise SubstrateError(
            f"{name} must be a number",
            data={"reason": "invalid_setting", "variable": name, "value": raw},
        ) from exc
    if value < minimum:
        raise SubstrateError(
            f"{name} must be at least {minimum}",
            data={"reason": "invalid_setting", "variable": name, "value": raw},
        )
    return value


def _successful_publish(summary: Any) -> tuple[bool, str]:
    """(True, "") for a publish that wrote a new generation, else the reason."""
    if not isinstance(summary, Mapping):
        return False, "no_publish_summary"
    if summary.get("status") != "published":
        return False, f"publish_status_{summary.get('status')}"
    if summary.get("noop") or summary.get("reused_generation"):
        return False, "noop_publish"
    return True, ""


def _post_publish_tables(cur: Any) -> list[str]:
    """Qualified names of the publish-written tables that exist here."""
    cur.execute(
        """
        SELECT n.nspname || '.' || c.relname AS qualified
          FROM pg_class AS c
          JOIN pg_namespace AS n ON n.oid = c.relnamespace
         WHERE c.relkind = 'r'
           AND ((n.nspname || '.' || c.relname) = ANY(%s)
                OR (n.nspname = 'public'
                    AND c.relname LIKE '\\_chronos\\_b\\_interval\\_okg\\_graph\\_%%'))
         ORDER BY 1
        """,
        (list(_POST_PUBLISH_OKG_TABLES),),
    )
    return [str(row["qualified"]) for row in cur.fetchall()]


def judge_indexes(
    cur: Any,
    tables: Sequence[str],
    *,
    ratio: float,
    min_reclaim_bytes: int,
    analyzed: bool = False,
) -> list[dict[str, Any]]:
    """Every index of ``tables`` with its size, estimate and verdict.

    ``verdict`` is ``rebuild`` (at least ``ratio`` times its estimated packed
    size and at least ``min_reclaim_bytes`` over it), ``keep``,
    ``needs_statistics`` (a table never analyzed, or a non-empty one last
    analyzed while empty, with an index big enough to matter: decide after
    one ANALYZE), or a reason it is not judged: ``not_btree``, ``invalid``,
    ``partial_index`` or ``no_statistics`` (a column still without
    ``pg_stats`` right after ANALYZE).

    An empty table (no rows after ANALYZE) is judged against an empty index,
    so a failed publish's allocation over an empty heap is rebuilt once and
    then kept; it is never analyzed again on every publish. ``analyzed``
    says the tables were just analyzed: a row count PostgreSQL still reports
    as unknown then means empty.
    """
    if not tables:
        return []
    cur.execute(_INDEX_ESTIMATE_SQL, (list(tables),))
    out: list[dict[str, Any]] = []
    for row in cur.fetchall():
        item: dict[str, Any] = {
            "table": f"{row['table_schema']}.{row['table_name']}",
            "index": f"{row['index_schema']}.{row['index_name']}",
            "bytes": int(row["bytes"]),
            "estimate_bytes": None,
            "ratio": None,
        }
        index_tuples = float(row["index_tuples"])
        table_tuples = float(row["table_tuples"])
        widths = list(row["widths"] or [])
        if row["access_method"] != "btree":
            item["verdict"] = "not_btree"
        elif not row["valid"]:
            item["verdict"] = "invalid"
        elif (table_tuples < 0 or index_tuples < 0) and not analyzed:
            # Never analyzed: decide after one ANALYZE.
            item["verdict"] = (
                "needs_statistics" if item["bytes"] >= min_reclaim_bytes else "keep"
            )
        elif table_tuples <= 0 or index_tuples <= 0:
            # Empty: a packed index is one page; widths do not matter.
            _judge(item, packed_btree_bytes(0, [], int(row["fillfactor"])),
                   ratio=ratio, min_reclaim_bytes=min_reclaim_bytes)
        elif any(w is None for w in widths):
            # A column without statistics on a non-empty table: the table
            # was last analyzed while empty (VACUUM since updated its row
            # count). Analyze once; if a column still has none after that
            # (statistics turned off for it), it is not judged.
            if analyzed:
                item["verdict"] = "no_statistics"
            else:
                item["verdict"] = (
                    "needs_statistics" if item["bytes"] >= min_reclaim_bytes else "keep"
                )
        elif (
            row["partial"]
            and table_tuples > 0
            and index_tuples < _PARTIAL_INDEX_MIN_SHARE * table_tuples
        ):
            item["verdict"] = "partial_index"
        else:
            _judge(
                item,
                packed_btree_bytes(index_tuples, widths, int(row["fillfactor"])),
                ratio=ratio,
                min_reclaim_bytes=min_reclaim_bytes,
            )
        out.append(item)
    return out


def _judge(item: dict[str, Any], estimate: int, *, ratio: float, min_reclaim_bytes: int) -> None:
    item["estimate_bytes"] = estimate
    item["ratio"] = round(item["bytes"] / estimate, 3) if estimate else None
    over = item["bytes"] - estimate
    item["verdict"] = (
        "rebuild"
        if item["bytes"] >= ratio * estimate and over >= min_reclaim_bytes
        else "keep"
    )


def _reindex_index(cur: Any, schema: str, index: str) -> None:
    """Run one ``REINDEX INDEX CONCURRENTLY`` (autocommit, outside a transaction)."""
    cur.execute(
        sql.SQL("REINDEX INDEX CONCURRENTLY {}").format(sql.Identifier(schema, index))
    )


class _ReindexFailed(Exception):
    """A rebuild failed; its own invalid copies were dropped under the lock."""

    def __init__(self, error: psycopg.Error, dropped: list[str], left: list[dict[str, Any]]) -> None:
        super().__init__(str(error))
        self.error = error
        self.dropped = dropped
        self.left = left


def _invalid_index_oids(cur: Any, schema: str, relation: str) -> set[int]:
    qualified = sql.Identifier(schema, relation).as_string(cur.connection)
    cur.execute(
        "SELECT indexrelid::bigint AS oid FROM pg_index WHERE indrelid = %s::regclass "
        "AND NOT (indisvalid AND indisready AND indislive)",
        (qualified,),
    )
    return {int(row["oid"]) for row in cur.fetchall()}


def _reindex_and_clean_up(
    cur: Any, *, schema: str, relation: str, index_schema: str, index_name: str
) -> None:
    """One ``REINDEX INDEX CONCURRENTLY``; on failure, drop exactly the
    invalid copies it made, while the caller still holds the publisher lock.

    A copy is this run's when it is invalid, was not there before the
    REINDEX (by OID), and is named ``<index>_ccnew``/``_ccold`` for this
    index. Another session cannot be rebuilding the same index meanwhile:
    two concurrent rebuilds of one table conflict on its lock.
    """
    before = _invalid_index_oids(cur, schema, relation)
    try:
        _reindex_index(cur, index_schema, index_name)
    except psycopg.Error as exc:
        conn = cur.connection
        if conn.closed or conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR:
            raise _ReindexFailed(exc, [], []) from exc
        own = {f"{index_schema}.{index_name}"}
        dropped: list[str] = []
        qualified = sql.Identifier(schema, relation).as_string(conn)
        cur.execute(
            """
            SELECT i.indexrelid::bigint AS oid, n.nspname || '.' || c.relname AS name
              FROM pg_index i
              JOIN pg_class c ON c.oid = i.indexrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE i.indrelid = %s::regclass
               AND NOT (i.indisvalid AND i.indisready AND i.indislive)
            """,
            (qualified,),
        )
        for row in cur.fetchall():
            name = str(row["name"])
            if int(row["oid"]) in before or not is_reindex_artifact(name, own):
                continue
            ischema, _, iname = name.partition(".")
            try:
                cur.execute(
                    sql.SQL("DROP INDEX CONCURRENTLY {}").format(
                        sql.Identifier(ischema, iname)
                    )
                )
                dropped.append(name)
            except psycopg.Error as drop_exc:  # noqa: BLE001 -- reported as left
                _log.warning("could not drop %s: %s", name, drop_exc)
        raise _ReindexFailed(exc, dropped, _invalid_indexes(cur, schema, relation)) from exc


def _rebuild_one(cur: Any, target: Mapping[str, Any]) -> None:
    schema, _, relation = str(target["table"]).partition(".")
    index_schema, _, index_name = str(target["index"]).partition(".")
    _reindex_and_clean_up(
        cur, schema=schema, relation=relation,
        index_schema=index_schema, index_name=index_name,
    )


class _StopRebuild(Exception):
    """A guard found a publish, a source run or a rebuild: stop, retry later."""

    def __init__(self, reason: str, detail: Mapping[str, Any] | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.detail = dict(detail or {})


def _publish_waiting(cur: Any, deployment: str) -> bool:
    """True when a session waits for the publisher lock.

    The publisher (and every ``publisher_publish_lock_on_connection`` user)
    polls with ``pg_try_advisory_lock`` and holds the shared intent key while
    it polls (``PublisherWaitIntent``); a blocking waiter would show in
    ``pg_locks`` as not granted. Either one means a publish wants the lock.
    """
    from okg.substrate.runtime.source_sync_priority import (
        publisher_publish_advisory_keys,
        publisher_publish_intent_advisory_keys,
    )

    intent = publisher_publish_intent_advisory_keys(deployment)
    cur.execute("SELECT pg_try_advisory_lock(%s, %s) AS got", intent)
    if not bool(cur.fetchone()["got"]):
        return True
    cur.execute("SELECT pg_advisory_unlock(%s, %s)", intent)
    classid, objid = publisher_publish_advisory_keys(deployment)
    cur.execute(
        """
        SELECT count(*) AS n FROM pg_locks
         WHERE locktype = 'advisory' AND NOT granted
           AND classid = (%s::int4)::oid AND objid = (%s::int4)::oid
           AND objsubid = 2
        """,
        (classid, objid),
    )
    return int(cur.fetchone()["n"]) > 0


def stats_visible(cur: Any) -> bool:
    """True when this role can read other roles' sessions in
    ``pg_stat_activity`` and ``pg_stat_progress_create_index`` (superuser or
    a member of ``pg_read_all_stats``). Without it those views show
    ``xact_start``, ``backend_type``, ``relid`` and ``phase`` as NULL for
    other roles' sessions; ``backend_xmin``, ``datname`` and ``pid`` stay
    visible."""
    cur.execute(
        """
        SELECT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)
               OR pg_has_role(current_user, 'pg_read_all_stats', 'USAGE') AS visible
        """
    )
    return bool(cur.fetchone()["visible"])


def _old_snapshots(cur: Any, older_than_seconds: float) -> list[dict[str, Any]]:
    """Other sessions of this database holding a snapshot older than ours.

    A ``REINDEX``/``CREATE``/``DROP INDEX CONCURRENTLY`` started now would
    wait for each of them, so the step would only run into its lock timeout
    while holding the publisher lock. The test rests on ``backend_xmin``,
    which every role can read: a session whose ``backend_xmin`` precedes
    this session's own snapshot ``xmin`` holds a snapshot older than every
    transaction still running when ours was taken. When ``xact_start`` is
    readable (a privileged role, or the same role) a session must also be
    older than ``older_than_seconds``, so a short query racing ours is not
    counted; when it is not readable, the ``backend_xmin`` test decides
    alone. A reader with no write since it began has an ``xmin`` equal to
    ours and is not seen; the step then waits for it at most the lock
    timeout per lock wait.
    """
    cur.execute(
        """
        SELECT a.pid, a.state,
               round(extract(epoch FROM clock_timestamp() - a.xact_start)::numeric, 1)::float8
                 AS xact_seconds,
               age(a.backend_xmin) AS xmin_age
          FROM pg_stat_activity a
          JOIN pg_stat_activity me ON me.pid = pg_backend_pid()
         WHERE a.datname = current_database()
           AND a.pid <> pg_backend_pid()
           AND a.backend_xmin IS NOT NULL
           AND me.backend_xmin IS NOT NULL
           AND age(a.backend_xmin) > age(me.backend_xmin)
           AND (a.xact_start IS NULL
                OR a.xact_start < clock_timestamp() - make_interval(secs => %s))
         ORDER BY age(a.backend_xmin) DESC
         LIMIT 10
        """,
        (float(older_than_seconds),),
    )
    return [dict(row) for row in cur.fetchall()]


def _take_step_lock(
    conn: psycopg.Connection,
    cur: Any,
    deployment: str,
    *,
    deadline: float | None = None,
    snapshot_age_seconds: float | None = None,
) -> tuple[int, int]:
    """Take the publisher lock without waiting, then re-check the other guards.

    Held for one step (one ANALYZE, one index build or drop, or one index
    rebuild) and released by the caller. Before each step it yields to a
    session that is waiting for the publisher lock (:func:`_publish_waiting`)
    and stops the rebuild; the next successful publish resumes it. So a
    publish that starts during the rebuild waits for one step at most, and a
    REINDEX never runs beside a publish, whose ``LOCK TABLE ... SHARE ROW
    EXCLUSIVE`` on the interval table would otherwise queue behind it.
    """
    from okg.substrate.runtime.source_sync_priority import (
        publisher_publish_advisory_keys,
    )

    if deadline is not None and time.monotonic() >= deadline:
        raise _StopRebuild("time_budget")
    if _publish_waiting(cur, deployment):
        raise _StopRebuild("publish_waiting")
    if snapshot_age_seconds is not None:
        old = _old_snapshots(cur, snapshot_age_seconds)
        if old:
            raise _StopRebuild("old_snapshot_open", {"old_snapshots": old})
    key = publisher_publish_advisory_keys(deployment)
    cur.execute("SELECT pg_try_advisory_lock(%s, %s) AS got", key)
    if not bool(cur.fetchone()["got"]):
        raise _StopRebuild("publish_running")
    try:
        running = _source_runs(conn, deployment)
        if running:
            raise _StopRebuild("source_run_in_progress", {"in_progress": [
                {key: _text(run.get(key)) for key in ("run_id", "source", "branch", "run_base")}
                for run in running[:10]
            ]})
    except BaseException:
        _release_step_lock(cur, key)
        raise
    return key


def _release_step_lock(cur: Any, key: tuple[int, int]) -> None:
    conn = cur.connection
    if conn.closed or conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR:
        return
    try:
        cur.execute("SELECT pg_advisory_unlock(%s, %s)", key)
    except psycopg.Error as exc:  # noqa: BLE001 -- the session close releases it
        _log.warning("post-publish index rebuild could not release the publisher lock: %s", exc)


def rebuild_indexes_after_publish(
    dsn: str,
    *,
    deployment: str,
    summary: Any,
    ratio: float | None = None,
    min_reclaim_bytes: int | None = None,
    lock_timeout_seconds: float | None = None,
    budget_seconds: float | None = None,
) -> dict[str, Any]:
    """Rebuild the bloated indexes of the publish-written tables; never raises.

    Runs after a successful publish has committed and released its locks:
    ``Publisher.cycle()`` calls it for a caller that opted in
    (``post_publish_index_rebuild_in_cycle``: the publisher worker, which
    also serves ``okg run --once`` without ``--inline``); inline ``okg run
    --once`` and merge-back call it after releasing their fence; ``okg
    ingest`` calls it once after its last layer. Request paths (the MCP
    connector ingest, the exchange receiver) do not run it; the next worker
    or run cycle does. It does nothing when the publish failed, was blocked
    or reused the previous generation, or when
    ``OKG_POST_PUBLISH_INDEX_REBUILD`` turns it off.

    Each lock wait lasts at most ``lock_timeout_seconds``
    (``OKG_POST_PUBLISH_INDEX_REBUILD_LOCK_TIMEOUT_SECONDS``, default 5).
    That bounds each wait, not the statement: one ``REINDEX CONCURRENTLY``
    waits several times, and a failed one is followed by its cleanup drop
    (lane H measured 2.0 s under the publisher lock at a 1 s timeout). No
    step starts after ``budget_seconds``
    (``OKG_POST_PUBLISH_INDEX_REBUILD_BUDGET_SECONDS``, default 30), and no
    step starts while another session holds a snapshot older than ours
    (``old_snapshot_open``, see :func:`_old_snapshots`): every concurrent
    index statement would wait for it under the publisher lock. Background
    workers (pg_cron, TimescaleDB jobs) are not seen by that guard; a wait
    for them stays bounded by the lock timeout. Each run first drops an
    invalid ``<index>_ccnew``/``_ccold`` copy an earlier rebuild of the same
    table left (a crashed worker, a cleanup that timed out), unless an index
    build is in progress on that table, or one runs that this role cannot
    attribute to a table (``unverifiable``; see :func:`stats_visible`).

    It first finishes any graph read-index change a deferred ``okg migrate``
    left pending (``cold_history.converge_on_cursor``), one build or drop
    per step, except an extended spec index (the GIN index on node attrs),
    which only ``okg migrate`` builds.

    An index is rebuilt when it is at least ``ratio`` times its estimated
    packed size (:func:`packed_btree_bytes`) and that is at least
    ``min_reclaim_bytes``. The estimate comes from ``pg_class.reltuples``
    and ``pg_stats``; the tables whose indexes look bloated are analyzed
    first, so stale statistics after a big publish cannot trigger a rebuild.

    Each step (one ANALYZE, one index build or drop, one ``REINDEX INDEX
    CONCURRENTLY``) first checks that no session is waiting for the
    publisher lock, then takes it without waiting, re-checks that no source
    run is writing (the guards of ``okg db reclaim-index-space``), runs, and
    releases the lock.
    When any guard is busy it stops and leaves the rest to the next
    successful publish, so a publish that starts during the rebuild waits
    for one step at most. A table with an invalid index (a leftover
    ``*_ccnew``/``*_ccold``) is not touched; the leftover is reported with
    its drop command. A failed rebuild drops, still under the lock, only the
    invalid copies it made itself (new OIDs named after that index).

    Returns the payload stored in the publish summary under
    ``post_publish_index_rebuild``; ``status`` is one of ``disabled``,
    ``skipped``, ``nothing_to_rebuild``, ``rebuilt``, ``stopped`` or
    ``error``.
    """
    started = time.monotonic()
    if not _post_publish_enabled():
        return {"status": "disabled", "reason": f"{POST_PUBLISH_REBUILD_ENV} is off"}
    ok, why = _successful_publish(summary)
    if not ok:
        return {"status": "skipped", "reason": why}
    try:
        payload = _rebuild_after_publish(
            dsn,
            deployment=deployment,
            ratio=(
                float(ratio) if ratio is not None else _env_number(
                    POST_PUBLISH_REBUILD_RATIO_ENV,
                    DEFAULT_POST_PUBLISH_REBUILD_RATIO,
                    minimum=1.0,
                )
            ),
            min_reclaim_bytes=int(
                min_reclaim_bytes if min_reclaim_bytes is not None else _env_number(
                    POST_PUBLISH_REBUILD_MIN_BYTES_ENV,
                    DEFAULT_POST_PUBLISH_REBUILD_MIN_BYTES,
                    minimum=0,
                )
            ),
            lock_timeout_seconds=float(
                lock_timeout_seconds if lock_timeout_seconds is not None else _env_number(
                    POST_PUBLISH_REBUILD_LOCK_TIMEOUT_ENV,
                    DEFAULT_POST_PUBLISH_REBUILD_LOCK_TIMEOUT_SECONDS,
                    minimum=0.1,
                )
            ),
            budget_seconds=float(
                budget_seconds if budget_seconds is not None else _env_number(
                    POST_PUBLISH_REBUILD_BUDGET_ENV,
                    DEFAULT_POST_PUBLISH_REBUILD_BUDGET_SECONDS,
                    minimum=0.0,
                )
            ),
            started=started,
        )
    except Exception as exc:  # noqa: BLE001 -- post-publish maintenance is isolated
        _log.exception("post-publish index rebuild failed: deployment=%s", deployment)
        payload = {
            "status": "error",
            "error": f"{type(exc).__name__}: {exc}",
            "data": json.loads(json.dumps(getattr(exc, "data", None), default=str)),
        }
    payload["seconds"] = round(time.monotonic() - started, 3)
    _note_old_snapshot_streak(dsn, deployment, payload)
    _log_post_publish(deployment, payload)
    return payload


def rebuild_indexes_after_ingest(options: Any, result: Any) -> Any:
    """After an ingest and its publishes finished, rebuild bloated indexes of
    the publish-written tables, once, for the last successful publish.

    ``options`` is the ingest's ``IngestOptions`` and ``result`` its
    ``IngestRunResult``; returns ``result``. Never raises; the payload lands
    in ``result.report``. Lives here, not in ingest/service.py, to keep that
    module inside its line budget."""
    if result.any_failed:
        return result
    publishes = [
        item for item in (result.report.get("publishes") or [])
        if isinstance(item, Mapping)
    ]
    if not publishes:
        return result
    successful = [
        item for item in publishes
        if item.get("status") == "published"
        and not item.get("noop")
        and not item.get("reused_generation")
    ]
    result.report["post_publish_index_rebuild"] = rebuild_indexes_after_publish(
        options.dsn,
        deployment=options.deployment,
        summary=successful[-1] if successful else publishes[-1],
    )
    return result


#: Consecutive runs stopped by ``old_snapshot_open``, per database and
#: deployment, in this process (the publisher worker lives long; a one-shot
#: ``okg run --once`` starts at zero and logs each stop at INFO).
_OLD_SNAPSHOT_STREAK: dict[tuple[str, str], int] = {}
OLD_SNAPSHOT_WARN_AFTER = 3


def _note_old_snapshot_streak(dsn: str, deployment: str, payload: dict[str, Any]) -> None:
    from okg.substrate.db import dsn as dsn_module

    # An unparsable DSN still counts, keyed by its redacted text.
    parsed = dsn_module.try_parse(dsn)
    database = str(parsed.dbname) if parsed is not None and parsed.dbname else dsn_module.redact(dsn)
    key = (database, deployment)
    if payload.get("reason") != "old_snapshot_open":
        _OLD_SNAPSHOT_STREAK.pop(key, None)
        return
    streak = _OLD_SNAPSHOT_STREAK.get(key, 0) + 1
    _OLD_SNAPSHOT_STREAK[key] = streak
    payload["old_snapshot_streak"] = streak
    if streak >= OLD_SNAPSHOT_WARN_AFTER:
        _log.warning(
            "post-publish index rebuild starved: %d consecutive runs stopped "
            "by old_snapshot_open: deployment=%s blocking=%s; end or shorten "
            "the long transaction (pid, xact_seconds; xact_seconds is null "
            "without pg_read_all_stats)",
            streak,
            deployment,
            [
                {"pid": item.get("pid"), "xact_seconds": item.get("xact_seconds")}
                for item in payload.get("old_snapshots") or []
            ],
        )


def _log_post_publish(deployment: str, payload: Mapping[str, Any]) -> None:
    status = payload.get("status")
    if status in {"disabled", "skipped", "nothing_to_rebuild"} and not payload.get(
        "leftover_invalid_indexes"
    ):
        _log.debug("post-publish index rebuild: deployment=%s status=%s", deployment, status)
        return
    level = logging.WARNING if status == "error" or payload.get(
        "leftover_invalid_indexes"
    ) or payload.get("failure") else logging.INFO
    _log.log(
        level,
        "post-publish index rebuild: deployment=%s status=%s reason=%s "
        "indexes_rebuilt=%d reclaimed_bytes=%d seconds=%.3f "
        "max_publisher_lock_hold_seconds=%.3f leftover_invalid_indexes=%s",
        deployment,
        status,
        payload.get("reason"),
        len(payload.get("rebuilt") or []),
        int(payload.get("reclaimed_bytes") or 0),
        float(payload.get("seconds") or 0.0),
        float(payload.get("max_publisher_lock_hold_seconds") or 0.0),
        [item["index"] for item in payload.get("leftover_invalid_indexes") or []],
    )


def _rebuild_after_publish(
    dsn: str,
    *,
    deployment: str,
    ratio: float,
    min_reclaim_bytes: int,
    lock_timeout_seconds: float,
    budget_seconds: float,
    started: float,
) -> dict[str, Any]:
    base: dict[str, Any] = {
        "deployment": deployment,
        "thresholds": {
            "ratio": ratio,
            "min_reclaim_bytes": min_reclaim_bytes,
            "lock_timeout_seconds": lock_timeout_seconds,
            "budget_seconds": budget_seconds,
        },
        "rebuilt": [],
        "analyzed": [],
        "reclaimed_bytes": 0,
        "max_publisher_lock_hold_seconds": 0.0,
        "leftover_invalid_indexes": [],
    }
    with db_connect(dsn, connect_timeout=5, autocommit=True) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            # Whether the old-snapshot and build-in-progress guards can see
            # other roles' sessions in full (see stats_visible).
            base["stats_visible"] = stats_visible(cur)
            cur.execute(
                sql.SQL("SET lock_timeout = {}").format(
                    sql.Literal(f"{int(lock_timeout_seconds * 1000)}ms")
                )
            )
            # A rebuild stopped by a statement timeout leaves an invalid
            # *_ccnew index; the lock wait is what must be bounded.
            cur.execute("SET statement_timeout = 0")
            try:
                return _rebuild_steps(
                    conn,
                    cur,
                    base=base,
                    deployment=deployment,
                    ratio=ratio,
                    min_reclaim_bytes=min_reclaim_bytes,
                    deadline=started + budget_seconds,
                    snapshot_age_seconds=lock_timeout_seconds,
                )
            finally:
                _reset_session(cur)


def _tables_with_index_build_in_progress(cur: Any) -> tuple[set[str], int]:
    """Tables (``schema.name``) with a CREATE INDEX or REINDEX running now,
    and how many running builds this role cannot attribute to a table
    (another role's session seen without ``pg_read_all_stats``: its
    ``relid`` reads NULL)."""
    cur.execute(
        """
        SELECT n.nspname || '.' || c.relname AS qualified
          FROM pg_stat_progress_create_index p
          LEFT JOIN pg_class c ON c.oid = p.relid
          LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE p.datname = current_database() AND p.pid <> pg_backend_pid()
        """
    )
    rows = [row["qualified"] for row in cur.fetchall()]
    return {str(q) for q in rows if q is not None}, sum(1 for q in rows if q is None)


def _drop_own_leftovers(
    cur: Any,
    schema: str,
    relation: str,
    invalid: list[dict[str, Any]],
    *,
    step: Callable[[Callable[[], Any]], Any],
    base: dict[str, Any],
) -> list[dict[str, Any]]:
    """Drop invalid ``<index>_ccnew``/``_ccold`` copies a rebuild of this
    table left (a crashed worker, a cleanup that hit its lock timeout).

    Called only when no index build is in progress on the table, so the copy
    is no live rebuild's. A copy is ours by name and table: named after a
    valid index of the same table (:func:`is_reindex_artifact`). Each drop is
    one step under the publisher lock with the short lock timeout; one that
    fails is reported and retried on the next run. Returns the invalid
    indexes still present (TOAST ones and other sessions' are never dropped).
    """
    qualified = sql.Identifier(schema, relation).as_string(cur.connection)
    cur.execute(
        """
        SELECT n.nspname || '.' || c.relname AS name
          FROM pg_index i
          JOIN pg_class c ON c.oid = i.indexrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE i.indrelid = %s::regclass
           AND i.indisvalid AND i.indisready AND i.indislive
        """,
        (qualified,),
    )
    valid = {str(row["name"]) for row in cur.fetchall()}
    for bad in invalid:
        name = str(bad["index"])
        if not is_reindex_artifact(name, valid):
            continue
        ischema, _, iname = name.partition(".")
        try:
            step(lambda s=ischema, i=iname: cur.execute(
                sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {}").format(sql.Identifier(s, i))
            ))
            base["cleaned_up_leftovers"].append(name)
        except psycopg.Error as exc:  # noqa: BLE001 -- reported; next run retries
            base.setdefault("leftover_cleanup_failed", []).append(
                {"index": name, "error": f"{type(exc).__name__}: {exc}"}
            )
    return _all_invalid_indexes(cur, schema, relation)


def _finish_graph_index_convergence(
    cur: Any, *, step: Callable[[Callable[[], Any]], Any]
) -> dict[str, Any] | None:
    """Finish what a deferred migrate left of the graph read-index change.

    None when nothing is pending (every run but the first after a deferred
    migrate). Otherwise it runs the migrate step's own convergence
    (``cold_history.converge_on_cursor``) under the same rules as a rebuild:
    each build or drop is one step under the publisher lock, yielding to a
    waiting publish. It takes the shared DDL lock without waiting, so it
    defers to a running migrate or cold-history rotation.

    It never builds an extended spec index (PACT lane-s-redesign-s1: the GIN
    index on node attrs, for instance after the code is deployed without
    ``okg migrate``): that build is one long statement, which would hold the
    publisher lock for all of it. A pending one is reported under
    ``skipped_extended`` (status ``partial``) and the rebuild goes on; only
    ``okg migrate`` builds it, and doctor reports it missing until then.
    """
    from okg.substrate.db import cold_history

    tables = cold_history.registered_graph_tables(cur)
    if tables is None:
        return None
    pending = cold_history.pending_interval_index_changes(cur, tables)
    if not pending["build"] and not pending["drop"]:
        return None
    extended = cold_history.extended_spec_index_names(tables)
    skipped = [name for name in pending["build"] if name in extended]
    if skipped and len(skipped) == len(pending["build"]) and not pending["drop"]:
        _log.info(
            "post-publish index rebuild: %s pending; only `okg migrate` builds it",
            skipped,
        )
        return {
            "status": "partial",
            "reason": "extended_builds_need_migrate",
            "created": [],
            "dropped": [],
            "skipped_extended": skipped,
            "pending": pending,
        }
    cur.execute(
        "SELECT pg_try_advisory_lock(hashtext(%s)) AS got", (cold_history._INDEX_DDL_LOCK,)
    )
    if not bool(cur.fetchone()["got"]):
        return {"status": "deferred", "reason": "another_index_change_running", "pending": pending}
    try:
        return cold_history.converge_on_cursor(
            cur,
            tables=tables,
            say=lambda msg: _log.info("post-publish index rebuild: %s", msg),
            step=step,
            build_extended=False,
        )
    finally:
        cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (cold_history._INDEX_DDL_LOCK,))


def _rebuild_steps(
    conn: psycopg.Connection,
    cur: Any,
    *,
    base: dict[str, Any],
    deployment: str,
    ratio: float,
    min_reclaim_bytes: int,
    deadline: float | None = None,
    snapshot_age_seconds: float | None = None,
) -> dict[str, Any]:
    def step(action: Callable[[], Any]) -> Any:
        key = _take_step_lock(
            conn, cur, deployment,
            deadline=deadline, snapshot_age_seconds=snapshot_age_seconds,
        )
        held = time.monotonic()
        try:
            return action()
        finally:
            _release_step_lock(cur, key)
            base["max_publisher_lock_hold_seconds"] = round(
                max(base["max_publisher_lock_hold_seconds"], time.monotonic() - held), 3
            )

    try:
        convergence = _finish_graph_index_convergence(cur, step=step)
    except _StopRebuild as stop:
        return {**base, "status": "stopped", "reason": stop.reason, **stop.detail}
    if convergence is not None:
        base["graph_index_convergence"] = convergence
        if convergence.get("status") == "deferred":
            return {**base, "status": "stopped", "reason": "graph_index_convergence_deferred"}

    tables = _post_publish_tables(cur)
    leftovers: list[dict[str, Any]] = []
    clean: list[str] = []
    building, unattributed = _tables_with_index_build_in_progress(cur)
    base["cleaned_up_leftovers"] = []
    base["builds_in_progress"] = []
    base["unverifiable"] = []
    try:
        for qualified in tables:
            schema, _, relation = qualified.partition(".")
            if qualified in building:
                # Someone's concurrent build (a migrate, `okg db
                # cold-history`): its index is invalid until it finishes. Not
                # a leftover, and this table waits for the next run.
                base["builds_in_progress"].append(qualified)
                continue
            bad = _all_invalid_indexes(cur, schema, relation)
            if bad and unattributed:
                # A build runs that this role cannot attribute to a table
                # (no pg_read_all_stats): any invalid index may be its. Not
                # reported as a leftover, not dropped; the table waits.
                base["unverifiable"].extend(item["index"] for item in bad)
                continue
            if bad:
                bad = _drop_own_leftovers(cur, schema, relation, bad, step=step, base=base)
            leftovers.extend(bad)
            if not bad:
                clean.append(qualified)
    except _StopRebuild as stop:
        base["leftover_invalid_indexes"] = leftovers
        return {**base, "status": "stopped", "reason": stop.reason, **stop.detail}
    base["leftover_invalid_indexes"] = leftovers
    judged = judge_indexes(cur, clean, ratio=ratio, min_reclaim_bytes=min_reclaim_bytes)
    suspects = sorted({
        item["table"] for item in judged
        if item["verdict"] in {"rebuild", "needs_statistics"}
    })
    if not suspects:
        return {**base, "status": "nothing_to_rebuild"}

    base["deferred_graph_tables"] = []

    def graph_step(qualified: str, action: Callable[[], Any]) -> bool:
        """Run ``action`` as one step; on a graph table only while nobody
        holds the graph-index DDL lock (okg#3112 step 2, design section 4.3
        rule 1). A declared-attribute index build holds it, possibly for
        minutes in CREATE INDEX CONCURRENTLY, whose lock this ANALYZE or
        REINDEX would queue behind; the table waits for the next run. False
        when deferred."""
        if not _is_graph_table(qualified):
            step(action)
            return True

        def guarded() -> bool:
            if not _try_index_ddl_lock(cur):
                return False
            try:
                action()
            finally:
                _release_index_ddl_lock(cur)
            return True

        if step(guarded):
            return True
        if qualified not in base["deferred_graph_tables"]:
            base["deferred_graph_tables"].append(qualified)
        return False

    try:
        # Statistics first: reltuples lags a big publish until autovacuum
        # analyzes, and an estimate from stale counts would look bloated.
        for qualified in list(suspects):
            schema, _, relation = qualified.partition(".")
            if not graph_step(qualified, lambda s=schema, r=relation: cur.execute(
                sql.SQL("ANALYZE {}").format(sql.Identifier(s, r))
            )):
                suspects.remove(qualified)
                continue
            base["analyzed"].append(qualified)
        if not suspects:
            return {**base, "status": "nothing_to_rebuild"}
        judged = judge_indexes(
            cur, suspects, ratio=ratio, min_reclaim_bytes=min_reclaim_bytes, analyzed=True,
        )
        targets = sorted(
            (item for item in judged if item["verdict"] == "rebuild"),
            key=lambda item: item["bytes"] - int(item["estimate_bytes"] or 0),
            reverse=True,
        )
        base["judged"] = judged
        if not targets:
            return {**base, "status": "nothing_to_rebuild"}
        for target in targets:
            schema, _, relation = target["table"].partition(".")
            index_schema, _, index_name = target["index"].partition(".")
            if target["table"] in base["deferred_graph_tables"]:
                continue
            begun = time.monotonic()
            try:
                if not graph_step(target["table"], lambda t=target: _rebuild_one(cur, t)):
                    continue
            except _ReindexFailed as failed:
                base["failure"] = {
                    "index": target["index"],
                    "error": f"{type(failed.error).__name__}: {failed.error}",
                    "invalid_indexes_dropped": failed.dropped,
                    "invalid_indexes_left": failed.left,
                }
                return {**base, "status": "stopped", "reason": "reindex_failed"}
            cur.execute(
                "SELECT pg_relation_size(%s::regclass) AS b",
                (sql.Identifier(index_schema, index_name).as_string(conn),),
            )
            after = int(cur.fetchone()["b"])
            base["rebuilt"].append({
                **target,
                "bytes_after": after,
                "reclaimed_bytes": target["bytes"] - after,
                "seconds": round(time.monotonic() - begun, 3),
            })
            base["reclaimed_bytes"] += target["bytes"] - after
    except _StopRebuild as stop:
        return {**base, "status": "stopped", "reason": stop.reason, **stop.detail}
    except psycopg.Error as exc:
        # An ANALYZE that waited past the short lock timeout (a concurrent
        # index build or a long transaction holds the table): yield; the next
        # publish retries.
        if getattr(exc, "sqlstate", None) not in {"55P03", "57014"}:
            raise
        return {
            **base, "status": "stopped", "reason": "lock_timeout",
            "error": f"{type(exc).__name__}: {exc}",
        }
    if not base["rebuilt"] and base["deferred_graph_tables"]:
        return {**base, "status": "stopped", "reason": "index_ddl_lock_held"}
    return {**base, "status": "rebuilt"}


def _is_graph_table(qualified: str) -> bool:
    return qualified.startswith("public._chronos_b_interval_okg_graph_")


def _try_index_ddl_lock(cur: Any) -> bool:
    from okg.substrate.db import cold_history

    cur.execute(
        "SELECT pg_try_advisory_lock(hashtext(%s)) AS got", (cold_history._INDEX_DDL_LOCK,)
    )
    return bool(cur.fetchone()["got"])


def _release_index_ddl_lock(cur: Any) -> None:
    from okg.substrate.db import cold_history

    conn = cur.connection
    if conn.closed or conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR:
        return
    cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (cold_history._INDEX_DDL_LOCK,))
