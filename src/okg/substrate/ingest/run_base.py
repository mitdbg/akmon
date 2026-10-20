"""Run-base checkpoints: what a failed source run is undone to (issue #2993).

A source run commits a chunk every 10,000 facts, so a run that fails partway
has already changed the working branch. Compensation used to delete every row
stamped with the failed run's id. That never put back a row the run deleted,
and it deleted a pre-existing row the run had only updated, because an update
re-stamps the row with the run's id. The next publish of any other source
then showed readers the damage.

At the start of each run the runner now cuts a private Chronos checkpoint of
the branch it writes, the "run base". It is never published and never a
generation: its metadata kind is ``okg_run_base``, not ``okg_generation``, so
every generation lookup that filters on the kind skips it, and the lookups
that did not filter now say so explicitly. On failure, compensation writes
every key the run touched back to its run-base version
(``compensation.undo_run_to_run_base``). The checkpoint is deleted when the
run ends: after the run's admission is finalized, or when the undo completes,
so it does not pin history.

An undo that cannot finish leaves durable state: the checkpoint is marked
``undo.state = pending`` before the undo starts (and a run with admission
receipts leaves them ``compensating``). While either exists,
:func:`refuse_publish_while_undo_unfinished` refuses every publish of the
deployment with ``source_run_undo_unfinished``, and
``okg runtime finish-undo`` is the way out.
"""
from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterable
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

from psycopg.rows import tuple_row

from okg.substrate.errors import (
    SourceRerunRequired,
    SourceRunInProgress,
    SourceRunUndoUnfinished,
    SubstrateError,
)
from okg.substrate.fact_batch import FactBatchError

log = logging.getLogger(__name__)

RUN_BASE_KIND = "okg_run_base"
RUN_BASE_PREFIX = "okg-run-base:"
UNDO_PENDING = "pending"

#: SQL fragment every checkpoint lookup that does not filter on the
#: ``okg_generation`` kind uses to leave run-base checkpoints out. The id
#: prefix is checked, not the parsed metadata, so no row is parsed for it.
EXCLUDE_RUN_BASE_SQL = "checkpoint_id NOT LIKE 'okg-run-base:%%'"


def run_base_checkpoint_id(branch: str, run_id: str) -> str:
    return f"{RUN_BASE_PREFIX}{branch}:{run_id}"


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def undo_resolve_hint(deployment: str | None, sources: list[str]) -> str:
    """The exact operator commands that clear an unfinished undo."""
    dep = deployment or "<deployment>"
    include = ",".join(sorted(set(sources))) or "<source>"
    return (
        "a failed source run's undo did not finish, so every publish of this "
        "deployment is refused until it does. Retry the undo with "
        f"`okg runtime finish-undo --deployment {dep} --apply`. If it is "
        "refused again because another writer changed the same rows, keep "
        "the rows as they are now with `okg runtime finish-undo --deployment "
        f"{dep} --accept-current --reason \"<why>\" --apply`, then rerun the "
        f"source with `okg ingest --deployment {dep} --include {include}`"
    )


def take_run_base(
    store: Any,
    *,
    deployment: str | None,
    branch: str,
    source: str,
    run_id: str,
    admission_id: str | None = None,
    progress_since: str | None = None,
) -> str | None:
    """Cut the run-base checkpoint of ``branch`` before the run writes.

    The runner calls this just before the run's first graph write, not at
    run start, so a run that writes nothing leaves the branch's segments as
    they were. Returns the checkpoint id, or None when no run base is cut: a
    backend other than the PostgreSQL interval backend (no production
    deployment runs one), a branch that does not exist yet, a runner with no
    deployment, or a branch with no published generation of the deployment
    yet (see the comment below).

    Stale run bases on the branch are deleted first: those whose run has
    ended (no process holds its liveness lock) and whose admission was
    accepted, published or blocked, so no undo can need them. A run base of
    a run that is still going, or that ended without a final admission, is
    never deleted here; the second is an unfinished undo (see
    :func:`pending_undos`).

    ``admission_id`` is recorded in the metadata so an undo can be finished
    for a run that ended without one (``okg runtime finish-undo``).
    ``progress_since`` is the database time the run started; a run with no
    admission saves its first chunk's progress markers before this cut, so
    the undo forgets progress saved since then (default: the cut's time).
    """
    context = store.context
    db = context.db
    if (
        getattr(context, "backend_name", None) != "interval"
        or getattr(db, "dialect", None) != "postgres"
    ):
        return None
    branch_row = db.execute(
        "SELECT 1 AS present FROM _chronos_branch_interval_branches "
        "WHERE branch_id = ?",
        (branch,),
    ).fetchone()
    if branch_row is None:
        return None
    # Before the deployment's first published generation on this branch no
    # reader has seen anything, and the publisher's first publish reads only
    # the branch's current segment as its change set: a run-base split
    # before it would hide earlier runs' rows from that publish. Such a run
    # keeps the compensation from before issue #2993 (it retracts the rows
    # it wrote; ``compensation._retract_run_rows``).
    if deployment is None or store.latest_generation(
        deployment_name=deployment, branch=branch, status="published",
    ) is None:
        return None
    for stale_id in stale_run_base_ids(db.raw_connection, branch=branch):
        db.execute(
            "DELETE FROM _chronos_branch_interval_checkpoints "
            "WHERE checkpoint_id = ?",
            (stale_id,),
        )
    checkpoint_id = run_base_checkpoint_id(branch, run_id)
    checkpoint_digest = db.execute(
        "SELECT md5(last_applied_rev::text) AS digest "
        "FROM okg.source_watermarks WHERE source = ?",
        (source,),
    ).fetchone()
    cut_at = db.execute("SELECT now()::text AS cut_at").fetchone()["cut_at"]
    existing = db.execute(
        "SELECT metadata FROM _chronos_branch_interval_checkpoints "
        "WHERE checkpoint_id = ?",
        (checkpoint_id,),
    ).fetchone()
    if existing is not None:
        # A retried attempt of the same run (a worker's DBOS recovery reruns
        # the workflow with the same run id): an earlier attempt cut this run
        # base before the run's first write, so it is still the state to
        # undo to, and the caller now holds the run's liveness lock. Reuse
        # it -- unless an undo of the run already started, which the retry
        # must not write over.
        if _undo_pending(_metadata(existing["metadata"])):
            raise SubstrateError(
                "this source run's undo started and did not finish; the "
                "retried attempt cannot write until it does",
                data={"checkpoint_id": checkpoint_id, "run_id": run_id},
                fix_hint=undo_resolve_hint(deployment, [source]),
            )
        return checkpoint_id
    context.create_checkpoint(
        checkpoint_id,
        branch=branch,
        metadata={
            "kind": RUN_BASE_KIND,
            "deployment_name": deployment,
            "branch": branch,
            "source": source,
            "run_id": run_id,
            "admission_id": admission_id,
            "status": "run_base",
            "created_at": _utcnow(),
            # Issue #3592: the source's checkpoint as the run found it. The
            # undo compares it with the checkpoint then stored to tell
            # whether a later run saved one that relies on this run's rows.
            CHECKPOINT_DIGEST_KEY: (
                None if checkpoint_digest is None
                else checkpoint_digest["digest"]
            ),
            # Database time at or before the run's first progress marker
            # (see forget_progress_after_undo).
            CUT_AT_KEY: progress_since or cut_at,
        },
    )
    return checkpoint_id


#: Run-base metadata key holding the md5 of the source's stored checkpoint
#: (``okg.source_watermarks.last_applied_rev``) when the run base was cut.
CHECKPOINT_DIGEST_KEY = "source_checkpoint_digest"

#: Run-base metadata key holding a database time at or before the run's
#: first progress marker: its start, or the cut.
CUT_AT_KEY = "db_cut_at"

#: The runner's record-set proof key in a stored checkpoint (the runner's
#: ``_RECORD_SET_CHRONOS_PROOF_KEY``): its ``ok`` says the record set the
#: checkpoint declares was checked against the graph.
RECORD_SET_PROOF_KEY = "record_set_chronos_proof"


def invalidate_checkpoint_after_undo(
    conn: Any, *, source: str, run_id: str, checkpoint_id: str,
) -> bool:
    """Mark the source's record-set proof unverified when a run saved the
    source's checkpoint after the undone run's run base was cut (#3592).

    That checkpoint was checked against a graph that still had the undone
    run's rows. Left verified, the next run trusts it, skips the full
    coverage check and emits nothing for records the undo just removed: the
    source publishes with them missing. Unverified, the next run proves the
    whole record set against the graph and refuses if any record is
    missing. Runs on the undo's own transaction; returns whether it changed
    the checkpoint. Call it before the run base is deleted; a run base cut
    before its digest was recorded counts as moved.
    """
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT metadata FROM public._chronos_branch_interval_checkpoints "
            "WHERE checkpoint_id = %s",
            (checkpoint_id,),
        )
        row = cur.fetchone()
        digest = _metadata(row[0] if row else None).get(CHECKPOINT_DIGEST_KEY)
        cur.execute(
            """
            UPDATE okg.source_watermarks
               SET last_applied_rev = jsonb_set(
                     last_applied_rev, %s,
                     (last_applied_rev -> %s) || jsonb_build_object(
                       'ok', false, 'invalidated_by_undo_of_run', %s::text
                     )
                   )
             WHERE source = %s
               AND jsonb_typeof(last_applied_rev -> %s) = 'object'
               AND md5(last_applied_rev::text) IS DISTINCT FROM %s
            """,
            (
                [RECORD_SET_PROOF_KEY], RECORD_SET_PROOF_KEY, run_id, source,
                RECORD_SET_PROOF_KEY, digest,
            ),
        )
        return cur.rowcount == 1


def forget_progress_after_undo(
    conn: Any, *, checkpoint_id: str,
) -> int:
    """Delete the source's per-record progress saved since the undone run's
    run base was cut (#3592); returns how many keys were deleted.

    A run saves a ``record:<id>`` progress marker in the same transaction as
    each chunk it writes, so a killed run leaves the markers of every chunk
    it committed. The undo deletes those chunks' rows; a marker left behind
    tells the source's next run the record is done, and it is never written
    again. Without a marker the next run reads the record and writes what
    the graph lacks. Runner-owned ``sync.`` keys are kept. A run base cut
    before its cut time was recorded forgets all of the source's progress.
    Runs on the undo's own transaction, before the run base is deleted.
    """
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT metadata FROM public._chronos_branch_interval_checkpoints "
            "WHERE checkpoint_id = %s",
            (checkpoint_id,),
        )
        row = cur.fetchone()
        metadata = _metadata(row[0] if row else None)
        deployment = metadata.get("deployment_name")
        source = metadata.get("source")
        if not deployment or not source:
            return 0
        cur.execute(
            """
            DELETE FROM okg.source_progress
             WHERE deployment = %s
               AND source_name = %s
               AND key NOT LIKE 'sync.%%'
               AND (%s::timestamptz IS NULL OR updated_at >= %s::timestamptz)
            """,
            (
                deployment, source,
                metadata.get(CUT_AT_KEY), metadata.get(CUT_AT_KEY),
            ),
        )
        return int(cur.rowcount or 0)


def rerun_undo_hint(deployment: str | None, pending: list[dict[str, Any]]) -> str:
    dep = deployment or "<deployment>"
    run_id = pending[0]["run_id"] if pending else "<run id>"
    source = pending[0].get("source") if pending else "<source>"
    return (
        f"an earlier run of {source} (run {run_id}) ended before its undo "
        "finished, and its rows are still on the branch. A rerun now would "
        "skip records that match those rows and save its checkpoint, and the "
        "undo would then delete them. If that run's process is gone, finish "
        f"the undo with `okg runtime finish-undo --deployment {dep} --run-id "
        f"{run_id} --apply`, then rerun the source. If finish-undo refuses "
        "it, follow its own hint (for example keep the rows with "
        "--accept-current --reason \"<why>\" --apply), then rerun"
    )


def refuse_rerun_while_undo_unfinished(
    conn: Any, *, deployment: str, source: str, run_id: str,
) -> None:
    """The runner's guard before a source run reads anything (#3592).

    Refuses while another run of the same source has an unfinished undo:
    the new run would dedupe against rows the undo is about to delete and
    still save a checkpoint claiming them. Other sources may still run;
    each publish stays refused until the undo finishes. A retried attempt
    of the same run (same run id) is left to ``take_run_base``.
    """
    pending = [
        item for item in pending_undos(conn, deployment=deployment)
        if item.get("source") == source and item["run_id"] != str(run_id)
    ]
    if not pending:
        return
    raise SourceRunUndoUnfinished(
        f"source run refused: an earlier run of {source} has an unfinished "
        "undo",
        data={
            "deployment": deployment,
            "source": source,
            "pending_count": len(pending),
            "pending": pending[:10],
        },
        fix_hint=rerun_undo_hint(deployment, pending),
    )


#: Admission statuses under which a run's graph writes stand, so its run
#: base can never be needed by an undo.
ADMISSION_WRITES_STAND = frozenset({"accepted", "published", "blocked"})


def _liveness_key_sql(placeholder: str = "%s") -> str:
    return f"hashtextextended({placeholder}, 0)"


def hold_run_liveness(dsn: str, checkpoint_id: str) -> Any:
    """Take the run's liveness lock on a connection of its own; return it.

    A session advisory lock keyed on the run-base checkpoint id, held from
    before the checkpoint is cut until the run's admission is final and its
    run base released (or its undo done). A crashed or killed process loses
    its connection and so the lock: that is how a leftover run base tells a
    run that is still going from one that ended without finishing (issue
    #2993 review). The caller closes the returned connection to release it.
    """
    from okg.substrate.db.connection import connect as db_connect

    conn = db_connect(dsn, autocommit=True)
    try:
        got = conn.execute(
            f"SELECT pg_try_advisory_lock({_liveness_key_sql()})",
            (checkpoint_id,),
        ).fetchone()[0]
    except BaseException:
        conn.close()
        raise
    if not got:
        conn.close()
        raise SubstrateError(
            "another process already holds this source run's liveness lock",
            data={"checkpoint_id": checkpoint_id},
            fix_hint="a run id is used once; start the source run again",
        )
    return conn


def run_is_live(conn: Any, checkpoint_id: str) -> bool:
    """True while some process holds the run's liveness lock."""
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            f"SELECT pg_try_advisory_lock({_liveness_key_sql()})",
            (checkpoint_id,),
        )
        got = bool(cur.fetchone()[0])
        if got:
            cur.execute(
                f"SELECT pg_advisory_unlock({_liveness_key_sql()})",
                (checkpoint_id,),
            )
    return not got


def _admission_status(conn: Any, admission_id: str | None) -> str | None:
    if not admission_id:
        return None
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT status FROM okg.admission_envelopes "
            "WHERE admission_id = %s::uuid",
            (str(admission_id),),
        )
        row = cur.fetchone()
    return None if row is None else str(row[0])


def _ended_run_bases(
    conn: Any, *, branch: str | None = None, deployment: str | None = None,
) -> list[tuple[str, dict[str, Any], str | None]]:
    """Run bases whose run has ended, not marked undo-pending.

    Each is ``(checkpoint_id, metadata, admission status)``. ``deployment``
    also matches a run base recorded with no deployment.
    """
    sql = (
        "SELECT checkpoint_id, metadata "
        "FROM public._chronos_branch_interval_checkpoints "
        "WHERE checkpoint_id LIKE %s"
    )
    params: list[Any] = [RUN_BASE_PREFIX + "%"]
    if branch is not None:
        sql += " AND branch_id = %s"
        params.append(branch)
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(sql, params)
        rows = list(cur.fetchall())
    ended: list[tuple[str, dict[str, Any], str | None]] = []
    for checkpoint_id, raw in rows:
        metadata = _metadata(raw)
        if _undo_pending(metadata):
            continue
        if deployment is not None and metadata.get("deployment_name") not in (
            None, deployment,
        ):
            continue
        if run_is_live(conn, str(checkpoint_id)):
            continue
        ended.append((
            str(checkpoint_id),
            metadata,
            _admission_status(conn, metadata.get("admission_id")),
        ))
    return ended


def stale_run_base_ids(
    conn: Any, *, branch: str | None = None, deployment: str | None = None,
) -> list[str]:
    """Run bases no undo can need: their run ended and its writes stand."""
    return [
        checkpoint_id
        for checkpoint_id, _metadata_, status in _ended_run_bases(
            conn, branch=branch, deployment=deployment,
        )
        if status in ADMISSION_WRITES_STAND
    ]


def branch_has_published_generation(
    conn: Any, *, branch: str, deployment: str | None,
) -> bool:
    """Whether a run on ``branch`` would have cut a run base (see
    :func:`take_run_base`): the deployment has a published generation there,
    or, with no deployment known, any deployment does."""
    sql = (
        "SELECT 1 FROM public._chronos_branch_interval_checkpoints "
        "WHERE branch_id = %s "
        "AND metadata::jsonb ->> 'kind' = 'okg_generation' "
        "AND metadata::jsonb ->> 'status' = 'published'"
    )
    params: list[Any] = [branch]
    if deployment is not None:
        sql += " AND metadata::jsonb ->> 'deployment_name' = %s"
        params.append(deployment)
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(sql + " LIMIT 1", params)
        return cur.fetchone() is not None


def _metadata(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    try:
        value = json.loads(raw or "{}")
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _undo_pending(metadata: dict[str, Any]) -> bool:
    undo = metadata.get("undo")
    return isinstance(undo, dict) and undo.get("state") == UNDO_PENDING


def mark_undo_pending(
    conn: Any, checkpoint_id: str, *, admission_id: str | None,
) -> None:
    """Record, before any undo write, that this run's undo has started.

    The caller commits it on its own connection, so it survives a crash or a
    rolled-back undo; only a completed undo deletes the checkpoint with it.
    """
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            """
            UPDATE public._chronos_branch_interval_checkpoints
               SET metadata = jsonb_set(
                     metadata::jsonb, '{undo}', %s::jsonb, true
                   )::text
             WHERE checkpoint_id = %s
            """,
            (
                json.dumps({
                    "state": UNDO_PENDING,
                    "started_at": _utcnow(),
                    "admission_id": admission_id,
                }),
                checkpoint_id,
            ),
        )


def run_base_exists(conn: Any, checkpoint_id: str) -> bool:
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT 1 FROM public._chronos_branch_interval_checkpoints "
            "WHERE checkpoint_id = %s",
            (checkpoint_id,),
        )
        return cur.fetchone() is not None


def delete_run_base(conn: Any, checkpoint_id: str, *, force: bool) -> bool:
    """Delete a run base; unless ``force``, keep one whose undo is pending."""
    sql = (
        "DELETE FROM public._chronos_branch_interval_checkpoints "
        "WHERE checkpoint_id = %s"
    )
    if not force:
        sql += (
            " AND COALESCE(metadata::jsonb #>> '{undo,state}', '') <> "
            f"'{UNDO_PENDING}'"
        )
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(sql, (checkpoint_id,))
        return cur.rowcount == 1


def release_run_base(dsn: str, *, branch: str, run_id: str) -> str | None:
    """Delete a finished run's run base; never raises.

    Called when a run ends without needing its undo (success, or a failure
    that is not compensated). A failure here must not turn an accepted run
    into a failed one -- the caller would then undo it -- so it is logged and
    returned instead. Once the run's liveness lock is released, a leftover
    whose admission was accepted is stale: the next run on the branch or
    ``okg runtime finish-undo --apply`` deletes it. (A leftover from a run
    with no admission cannot be told from a crashed run and is listed as an
    unfinished undo.)
    """
    from okg.substrate.db.pool import GENERAL, pooled_connection

    checkpoint_id = run_base_checkpoint_id(branch, run_id)
    try:
        with pooled_connection(dsn, purpose=GENERAL) as conn:
            with conn.transaction():
                delete_run_base(conn, checkpoint_id, force=False)
    except Exception as exc:  # noqa: BLE001 - reported, see docstring
        log.warning(
            "run-base checkpoint %s was not deleted (%s: %s); the source's "
            "next run deletes it",
            checkpoint_id, type(exc).__name__, exc,
        )
        return f"{type(exc).__name__}: {exc}"
    return None


def pending_undos(conn: Any, *, deployment: str) -> list[dict[str, Any]]:
    """Every source run of ``deployment`` whose undo has not finished.

    Three durable signals: an admission receipt stuck in ``compensating``
    (any compensation, with or without a run base); a run base marked
    ``undo.state = pending`` (which also covers runs with no admission); and
    a run base whose run ended (no process holds its liveness lock) before
    its admission was accepted, published or blocked. A run base with no
    deployment name belongs to a runner used without one; it is counted for
    every deployment, because it changed the same graph. A run that is
    still going holds its liveness lock and is not counted.
    """
    pending: dict[tuple[str, str], dict[str, Any]] = {}
    with conn.cursor(row_factory=tuple_row) as cur:
        # A database that has never had a source run (or a Chronos branch)
        # has neither table yet, and so nothing unfinished.
        cur.execute(
            "SELECT to_regclass('okg.fact_batch_receipts') IS NOT NULL, "
            "to_regclass('public._chronos_branch_interval_checkpoints') "
            "IS NOT NULL"
        )
        has_receipts, has_checkpoints = cur.fetchone()
    with conn.cursor(row_factory=tuple_row) as cur:
        if has_receipts:
            cur.execute(
                """
                SELECT DISTINCT r.run_id::text, r.target_branch,
                       e.source, e.admission_id::text
                  FROM okg.fact_batch_receipts AS r
                  JOIN okg.admission_envelopes AS e
                    ON e.admission_id = r.admission_id
                 WHERE r.state = 'compensating'
                   AND e.deployment_name = %s
                """,
                (deployment,),
            )
            for run_id, branch, source, admission_id in cur.fetchall():
                pending[(str(branch), str(run_id))] = {
                    "run_id": str(run_id),
                    "branch": str(branch),
                    "source": source,
                    "admission_id": admission_id,
                    "signal": "receipts_compensating",
                }
        if has_checkpoints:
            cur.execute(
                """
                SELECT checkpoint_id, metadata
                  FROM public._chronos_branch_interval_checkpoints
                 WHERE checkpoint_id LIKE %s
                   AND metadata::jsonb #>> '{undo,state}' = %s
                   AND COALESCE(metadata::jsonb ->> 'deployment_name', %s) = %s
                """,
                (RUN_BASE_PREFIX + "%", UNDO_PENDING, deployment, deployment),
            )
            for checkpoint_id, raw in cur.fetchall():
                metadata = _metadata(raw)
                key = (str(metadata.get("branch")), str(metadata.get("run_id")))
                entry = pending.setdefault(key, {
                    "run_id": key[1],
                    "branch": key[0],
                    "source": metadata.get("source"),
                    "admission_id": (metadata.get("undo") or {}).get("admission_id"),
                })
                entry["signal"] = (
                    "both" if entry.get("signal") == "receipts_compensating"
                    else "run_base_pending"
                )
                entry["run_base"] = str(checkpoint_id)
            # A run base whose run ended -- crashed, killed, or failed with
            # no compensation -- before its admission was accepted: the run's
            # committed chunks are still on the branch and nothing will undo
            # them, so it is an unfinished undo too.
            for checkpoint_id, metadata, status in _ended_run_bases(
                conn, deployment=deployment,
            ):
                if status in ADMISSION_WRITES_STAND:
                    continue
                key = (str(metadata.get("branch")), str(metadata.get("run_id")))
                entry = pending.setdefault(key, {
                    "run_id": key[1],
                    "branch": key[0],
                    "source": metadata.get("source"),
                    "admission_id": metadata.get("admission_id"),
                    "signal": "run_ended_without_undo",
                    "admission_status": status,
                })
                entry["run_base"] = checkpoint_id
    return sorted(pending.values(), key=lambda item: (item["branch"], item["run_id"]))


def refuse_publish_while_undo_unfinished(
    conn: Any,
    *,
    deployment: str,
    own_run_ids: Iterable[str] = (),
) -> None:
    """The publisher's guard: no publish while any source run of the
    deployment other than the publisher's own is in progress, while any
    source undo is unfinished, or while a source whose partial run an
    operator kept must rerun.

    A run in progress has committed chunks on the working branch that are
    not yet a complete run (issue #2993 round 6: a publish in that window
    showed readers another run's half-written records). A half-undone run
    leaves the branch partly changed. ``finish-undo --accept-current`` keeps
    rows that are still a failed run's partial state, so the block stays, as
    "rerun required", until the source's next run succeeds or it is retired.
    ``own_run_ids`` are the runs whose publish this is, named by run id.
    Keyed on durable rows and session locks, so it holds across restarts.
    """
    block = publish_block(conn, deployment=deployment, own_run_ids=own_run_ids)
    if block is None:
        return
    error = {
        "source_run_undo_unfinished": SourceRunUndoUnfinished,
        "source_run_in_progress": SourceRunInProgress,
    }.get(block["code"], SourceRerunRequired)
    raise error(block["message"], data=block["data"], fix_hint=block["fix_hint"])


# The publish refusals an unfinished, running or rerun-required source run causes.
UNDO_BLOCK_CODES = frozenset({
    "source_run_undo_unfinished",
    "source_rerun_required",
    "source_run_in_progress",
})


def publish_block(
    conn: Any,
    *,
    deployment: str,
    own_run_ids: Iterable[str] = (),
    include_in_progress: bool = True,
) -> dict[str, Any] | None:
    """Why every publish of ``deployment`` is refused now, or None.

    One report for the publisher's guard, ``okg doctor`` and ``okg
    deployment ready``: the code, a message naming the sources, the exact
    commands, and the rows behind it. ``include_in_progress=False`` leaves
    out runs in progress, which are transient (the health reports list them
    as warnings instead).
    """
    pending = pending_undos(conn, deployment=deployment)
    if pending:
        sources = sorted({str(item.get("source") or "") for item in pending})
        return {
            "code": "source_run_undo_unfinished",
            "message": (
                "publish refused: a failed source run's undo has not finished "
                f"(source: {', '.join(sources)})"
            ),
            "sources": sources,
            "data": {
                "deployment": deployment,
                "pending_count": len(pending),
                "pending": pending[:10],
            },
            "fix_hint": undo_resolve_hint(deployment, sources),
        }
    running = (
        runs_in_progress(conn, deployment=deployment, own_run_ids=own_run_ids)
        if include_in_progress else []
    )
    if running:
        sources = sorted({str(item.get("source") or "") for item in running})
        named = ", ".join(
            f"run {item['run_id']} of {item.get('source')} (owner "
            f"{item['owner']['state']}, pid {item['owner'].get('pid')}, "
            f"connected since {item['owner'].get('backend_start')})"
            for item in running[:5]
        )
        return {
            "code": "source_run_in_progress",
            "message": (
                "publish refused: another source run is in progress and has "
                f"not finished writing ({named})"
            ),
            "sources": sources,
            "data": {
                "deployment": deployment,
                "in_progress_count": len(running),
                "in_progress": running[:10],
            },
            "fix_hint": in_progress_hint(deployment, running),
        }
    rerun = rerun_required(conn, deployment=deployment)
    if rerun:
        sources = sorted({str(item["source"]) for item in rerun})
        return {
            "code": "source_rerun_required",
            "message": (
                "publish refused: the partial rows of a failed run were kept "
                "(finish-undo --accept-current), so the source must rerun "
                f"successfully first (source: {', '.join(sources)})"
            ),
            "sources": sources,
            "data": {"deployment": deployment, "rerun_required": rerun},
            "fix_hint": rerun_hint(deployment, sources),
        }
    return None


def in_progress_hint(deployment: str | None, running: list[dict[str, Any]]) -> str:
    dep = deployment or "<deployment>"
    first = running[0] if running else {}
    owner = first.get("owner") or {}
    return (
        f"run {first.get('run_id')} of source {first.get('source')} is still "
        f"writing (its owner session, pid {owner.get('pid')}, is alive), and a "
        "publish now would show readers its unfinished records. Wait for it "
        "to finish, then publish again. If that process is gone but its "
        f"database session is not (a hung connection), end the session with "
        f"`SELECT pg_terminate_backend({owner.get('pid')})`: the run then reads "
        f"as ended, and `okg runtime finish-undo --deployment {dep} "
        f"--run-id {first.get('run_id')} --apply` undoes its records"
    )


def _liveness_owner(conn: Any, checkpoint_id: str) -> dict[str, Any] | None:
    """The session holding a run's liveness lock, or None when none does.

    A session advisory lock cannot outlive its session, so a run base whose
    process crashed, whose host died, or that came back from a database
    restore has no holder here and never reads as in progress.
    """
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            f"""
            SELECT l.pid, a.backend_start, a.application_name, a.client_addr
              FROM pg_locks l
              LEFT JOIN pg_stat_activity a ON a.pid = l.pid
             WHERE l.locktype = 'advisory'
               AND l.granted
               AND l.objsubid = 1
               AND l.database = (SELECT oid FROM pg_database
                                  WHERE datname = current_database())
               AND l.classid = ((({_liveness_key_sql()}) >> 32)
                                & 4294967295)::oid
               AND l.objid = (({_liveness_key_sql()}) & 4294967295)::oid
             LIMIT 1
            """,
            (checkpoint_id, checkpoint_id),
        )
        row = cur.fetchone()
    if row is None:
        return None
    pid, backend_start, application, client = row
    return {
        "state": "live",
        "pid": int(pid),
        "backend_start": (
            backend_start.isoformat() if backend_start is not None else None
        ),
        "application_name": application,
        "client_addr": None if client is None else str(client),
    }


def runs_in_progress(
    conn: Any,
    *,
    deployment: str,
    own_run_ids: Iterable[str] = (),
) -> list[dict[str, Any]]:
    """Source runs of ``deployment`` still writing, except ``own_run_ids``.

    A run is in progress while its run base exists and a live session holds
    its liveness lock. A run base with no holder is not in progress: its run
    ended, and it is either an unfinished undo (``pending_undos``) or a stale
    leftover whose writes stand (``stale_run_bases``).
    """
    own = {str(run_id) for run_id in own_run_ids}
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SELECT to_regclass('public._chronos_branch_interval_checkpoints')")
        if cur.fetchone()[0] is None:
            return []
        cur.execute(
            "SELECT checkpoint_id, metadata "
            "FROM public._chronos_branch_interval_checkpoints "
            "WHERE checkpoint_id LIKE %s "
            "AND COALESCE(metadata::jsonb ->> 'deployment_name', %s) = %s "
            "ORDER BY checkpoint_id",
            (RUN_BASE_PREFIX + "%", deployment, deployment),
        )
        rows = list(cur.fetchall())
    running: list[dict[str, Any]] = []
    for checkpoint_id, raw in rows:
        metadata = _metadata(raw)
        if str(metadata.get("run_id")) in own:
            continue
        owner = _liveness_owner(conn, str(checkpoint_id))
        if owner is None:
            continue
        running.append({
            "run_id": str(metadata.get("run_id")),
            "source": metadata.get("source"),
            "branch": metadata.get("branch"),
            "admission_id": metadata.get("admission_id"),
            "run_base": str(checkpoint_id),
            "owner": owner,
        })
    return running


def stale_run_bases(conn: Any, *, deployment: str) -> list[dict[str, Any]]:
    """Run bases of ended runs whose writes stand: no undo needs them and
    they block nothing, but they pin history until deleted."""
    return [
        {
            "run_id": str(metadata.get("run_id")),
            "source": metadata.get("source"),
            "run_base": checkpoint_id,
            "admission_status": status,
            "owner": {"state": "gone"},
            "fix_hint": (
                f"`okg runtime finish-undo --deployment {deployment} --apply` "
                "deletes it"
            ),
        }
        for checkpoint_id, metadata, status in _ended_run_bases(
            conn, deployment=deployment,
        )
        if status in ADMISSION_WRITES_STAND
    ]


def wait_for_source_runs(
    dsn: str,
    *,
    deployment: str,
    own_run_ids: Iterable[str],
    timeout_seconds: float | None = None,
    poll_seconds: float = 2.0,
) -> dict[str, Any]:
    """Wait, bounded, until no other source run of ``deployment`` is in
    progress. Used by an ingest's own run-end publish before it takes any
    lock, so the run it waits for can finish (that run waits on nothing of
    the caller's). Returns what it waited for; the publisher's guard still
    decides, and refuses if a run is still in progress at the bound.
    """
    import time

    from okg.substrate.db.connection import connect as db_connect

    bound = (
        publish_wait_seconds() if timeout_seconds is None else timeout_seconds
    )
    started = time.monotonic()
    seen: dict[str, dict[str, Any]] = {}
    with db_connect(dsn, autocommit=True) as conn:
        while True:
            running = runs_in_progress(
                conn, deployment=deployment, own_run_ids=own_run_ids,
            )
            for item in running:
                seen[item["run_id"]] = item
            waited = time.monotonic() - started
            if not running or waited >= bound:
                break
            time.sleep(min(poll_seconds, max(0.0, bound - waited)))
    if seen:
        log.warning(
            "publish of %s waited %.1f s for source runs in progress: %s%s",
            deployment, waited, ", ".join(sorted(seen)),
            " (still running at the bound)" if running else "",
        )
    return {
        "waited_seconds": round(waited, 3),
        "waited_for": sorted(seen),
        "still_running": [item["run_id"] for item in running],
        "bound_seconds": bound,
    }


#: The run ids of the source runs whose publish the current ingest runs (its
#: "own" runs, excluded from the in-progress check). Set by the ingest before
#: its run-end publish. Only live runs are ever checked, and run ids are
#: unique, so a value left over for a later publisher in the same context
#: names only ended runs and excludes nothing.
INGEST_OWN_RUN_IDS: ContextVar[tuple[str, ...]] = ContextVar(
    "okg_ingest_own_run_ids", default=(),
)


#: Upper bound, in seconds, on how long an ingest's run-end publish waits for
#: another source run to finish before the publisher refuses it.
PUBLISH_WAIT_ENV = "OKG_PUBLISH_WAIT_FOR_SOURCE_RUNS_SECONDS"
PUBLISH_WAIT_SECONDS = 600.0


def publish_wait_seconds() -> float:
    raw = os.environ.get(PUBLISH_WAIT_ENV)
    if raw is None or not raw.strip():
        return PUBLISH_WAIT_SECONDS
    try:
        value = float(raw)
    except ValueError as exc:
        raise SubstrateError(
            f"{PUBLISH_WAIT_ENV} must be a number of seconds, got {raw!r}",
        ) from exc
    if value < 0:
        raise SubstrateError(f"{PUBLISH_WAIT_ENV} must not be negative")
    return value


def publish_block_report(dsn: str, deployment: str) -> dict[str, Any]:
    """``publish_block`` on a connection of its own, as a status mapping."""
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn, autocommit=True) as conn:
        block = publish_block(
            conn, deployment=deployment, include_in_progress=False,
        )
    if block is None:
        return {"blocked": False}
    return {"blocked": True, **block}


def source_run_health(dsn: str, deployment: str) -> dict[str, Any]:
    """Runs in progress (each publish waits or is refused while one is) and
    stale run bases, for ``okg doctor`` and ``okg deployment ready``."""
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn, autocommit=True) as conn:
        return {
            "in_progress": runs_in_progress(conn, deployment=deployment),
            "stale_run_bases": stale_run_bases(conn, deployment=deployment),
        }


#: ``okg.source_progress`` key recording that a source must rerun before any
#: publish (``finish-undo --accept-current``). Under ``sync.`` so a source's
#: own progress snapshot and restore leave it alone.
RERUN_REQUIRED_KEY = "sync.rerun_required"


def rerun_hint(deployment: str | None, sources: list[str]) -> str:
    dep = deployment or "<deployment>"
    names = sorted(set(sources))
    include = ",".join(names) or "<source>"
    one = names[0] if len(names) == 1 else "<source>"
    return (
        "every publish of this deployment is refused until the source reruns "
        f"successfully: run `okg ingest --deployment {dep} --include {include}`. "
        "If it keeps failing, fix the source and run that again, or remove the "
        f"source: `okg remediate retire-source --source {one} "
        f"--deployment-name {dep}` (a dry run that prints plan_digest and "
        "registry_source_id; it works whether or not the source is still in "
        "source_registry.yaml), then the same command with "
        "`--expected-plan-digest <plan_digest> --confirm-source "
        "<registry_source_id> --apply`, which removes all of its records and "
        "lifts this block; then delete its source_registry.yaml entry if it is "
        f"still there, run `okg catalog ownership claim --deployment {dep}` and "
        f"publish with `okg run --once --deployment {dep} --apply`"
    )


def mark_rerun_required(
    conn: Any,
    *,
    deployment: str,
    source: str,
    run_id: str,
    reason: str | None,
    actor: str,
) -> None:
    """Record that ``source`` must rerun before any publish; keeps the reason."""
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (deployment, source_name, key) DO UPDATE
              SET value = EXCLUDED.value, updated_at = now()
            """,
            (
                deployment, source, RERUN_REQUIRED_KEY,
                json.dumps({
                    "run_id": run_id,
                    "reason": reason,
                    "actor": actor,
                    "set_at": _utcnow(),
                }),
            ),
        )


def clear_rerun_required(
    conn: Any, *, deployment: str, sources: list[str],
) -> int:
    """Lift the rerun-required block of ``sources``; returns rows removed."""
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "DELETE FROM okg.source_progress WHERE deployment = %s "
            "AND source_name = ANY(%s) AND key = %s",
            (deployment, list(sources), RERUN_REQUIRED_KEY),
        )
        return int(cur.rowcount or 0)


def rerun_required(conn: Any, *, deployment: str) -> list[dict[str, Any]]:
    """Sources that must rerun before any publish, with the kept run's id,
    the operator's reason and who accepted it."""
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SELECT to_regclass('okg.source_progress') IS NOT NULL")
        if not cur.fetchone()[0]:
            return []
        cur.execute(
            "SELECT source_name, value FROM okg.source_progress "
            "WHERE deployment = %s AND key = %s ORDER BY source_name",
            (deployment, RERUN_REQUIRED_KEY),
        )
        return [
            {"source": str(source), **_metadata(value)}
            for source, value in cur.fetchall()
        ]


def rerun_may_proceed(redrive: dict[str, Any], selected: set[str]) -> bool:
    """Whether an ingest may start its sources although its pre-ingest
    publish was refused: only for a rerun-required block, and only when this
    ingest runs every source the block names (its success lifts the block)."""
    if redrive.get("code") != "source_rerun_required":
        return False
    data = redrive.get("error_data") or {}
    needed = {
        str(item.get("source"))
        for item in (data.get("rerun_required") or [])
        if isinstance(item, dict)
    }
    return bool(needed) and needed <= set(selected)


def finish_run_admission(
    dsn: str,
    *,
    branch: str,
    run_id: str,
    deployment: str | None,
    source: str | None,
    admission_status: str | None,
) -> None:
    """After a run's admission is final: release its run base, and when the
    run was accepted, lift a rerun-required block of its source.

    Never raises, like ``release_run_base``: the admission is already final,
    and turning an accepted run into a failed one would undo it. A block
    that could not be lifted stays in force -- every publish is still
    refused with ``source_rerun_required`` and ``okg doctor`` still names
    the source -- so the failure is logged and the next rerun lifts it.
    """
    release_run_base(dsn, branch=branch, run_id=run_id)
    if not deployment or not source:
        return
    if admission_status not in {"accepted", "published"}:
        return
    from okg.substrate.db.pool import GENERAL, pooled_connection

    try:
        with pooled_connection(dsn, purpose=GENERAL) as conn:
            with conn.transaction():
                lifted = clear_rerun_required(
                    conn, deployment=deployment, sources=[source],
                )
    except Exception as exc:  # noqa: BLE001 - reported, see docstring
        log.warning(
            "source %s ran successfully (run %s) but its rerun-required block "
            "was not checked or lifted (%s: %s); publishing stays refused "
            "until its next successful run", source, run_id,
            type(exc).__name__, exc,
        )
        return
    if lifted:
        log.warning(
            "source %s reran successfully (run %s); its rerun-required block "
            "is lifted", source, run_id,
        )


def finish_pending_undos(
    *,
    dsn: str,
    deployment: str,
    run_id: str | None = None,
    accept_current: bool = False,
    reason: str | None = None,
    actor: str = "operator",
    apply: bool = False,
) -> dict[str, Any]:
    """``okg runtime finish-undo``: retry, or retire, unfinished undos.

    Without ``accept_current`` each pending run's undo is run again
    (``retract_chronos_run`` with the same admission, source and run id);
    one that is refused again stays pending and is reported. With
    ``accept_current`` (and a ``reason``) nothing in the graph is written:
    the runs' ``compensating`` receipts are carried to ``compensated`` and
    their run bases deleted, which records that the operator keeps the rows
    as they are. Those rows are still the failed run's partial state, so
    the source is marked rerun-required and every publish stays refused
    until its next run succeeds or it is retired.
    """
    from okg.substrate.db.connection import connect as db_connect

    if accept_current and not (reason or "").strip():
        raise SubstrateError(
            "finish-undo --accept-current requires --reason",
            fix_hint="say why the current rows are kept, e.g. --reason "
                     "\"another source rewrote these rows\"",
        )
    with db_connect(dsn, autocommit=True) as conn:
        pending = pending_undos(conn, deployment=deployment)
        stale = stale_run_base_ids(conn, deployment=deployment)
    if run_id is not None:
        pending = [item for item in pending if item["run_id"] == run_id]
    report: dict[str, Any] = {
        "deployment": deployment,
        "mode": "accept_current" if accept_current else "retry",
        "applied": bool(apply),
        "pending": pending,
        # Run bases of ended runs whose writes were accepted: no undo needs
        # them; --apply deletes them so they stop pinning history.
        "stale_run_bases": stale,
        "results": [],
    }
    if apply and stale:
        with db_connect(dsn) as conn, conn.transaction():
            report["stale_run_bases_deleted"] = sum(
                delete_run_base(conn, checkpoint_id, force=False)
                for checkpoint_id in stale
            )
    if not apply:
        report["next_command"] = (
            f"okg runtime finish-undo --deployment {deployment}"
            + (" --accept-current --reason \"<why>\"" if accept_current else "")
            + " --apply"
        )
        return report
    report["skipped"] = []
    for item in pending:
        branch = item["branch"]
        result: dict[str, Any] = {"run_id": item["run_id"], "source": item["source"]}
        skip = _skip_run_that_may_be_alive(dsn, item, named=run_id is not None)
        if skip is not None:
            report["skipped"].append({**result, "why": skip})
            continue
        try:
            if accept_current:
                _accept_current(
                    dsn, item, deployment=deployment, reason=reason,
                    actor=actor,
                )
                result["status"] = "accepted_current"
                result["rerun_required"] = True
                result["fix_hint"] = rerun_hint(deployment, [str(item["source"])])
                log.warning(
                    "finish-undo: %s kept the current rows for run %s of %s "
                    "(%s)", actor, item["run_id"], item["source"], reason,
                )
            else:
                from okg.substrate.ingest.compensation import retract_chronos_run

                stats = retract_chronos_run(
                    dsn,
                    admission_id=item.get("admission_id"),
                    source=str(item["source"]),
                    run_id=str(item["run_id"]),
                    branch=branch,
                )
                result["status"] = "undone"
                result["stats"] = stats
                result["admission"] = _fail_undone_admission(
                    dsn, item.get("admission_id"), actor=actor,
                )
        except SubstrateError as exc:
            result["status"] = "refused"
            result["error"] = {"code": exc.code, "message": str(exc), "data": exc.data}
        except FactBatchError as exc:
            # A chunk receipt still 'prepared': the process died between
            # writing a chunk to the graph and recording it. Compensation
            # refuses such a run rather than guess what the chunk applied.
            result["status"] = "refused"
            result["error"] = {
                "code": "fact_batch_recovery_blocked",
                "message": f"{type(exc).__name__}: {exc}",
                "fix_hint": (
                    "a chunk of this run was written but never recorded, so "
                    "it cannot be undone exactly; keep the rows as they are "
                    "now with --accept-current --reason \"<why>\" --apply, "
                    "then rerun the source"
                ),
            }
        result["reason"] = reason
        result["actor"] = actor
        report["results"].append(result)
    with db_connect(dsn, autocommit=True) as conn:
        report["still_pending"] = pending_undos(conn, deployment=deployment)
    return report


#: A run whose last sign of life (its run base, or its latest chunk receipt)
#: is this recent is not undone by ``finish-undo`` unless named with
#: ``--run-id``: its liveness session may have been lost while the run goes on.
FINISH_UNDO_QUIET_SECONDS = 600


def _skip_run_that_may_be_alive(
    dsn: str, item: dict[str, Any], *, named: bool,
) -> str | None:
    """Why ``finish-undo`` must leave this run alone now, or None.

    Only for a run that ended without its undo (no undo was ever started):
    its liveness lock is checked again just before the undo, and a run
    active in the last ``FINISH_UNDO_QUIET_SECONDS`` is skipped unless the
    operator named it -- a lost liveness connection makes a live run read as
    ended (issue #2993 review).
    """
    from okg.substrate.db.connection import connect as db_connect

    if item.get("signal") != "run_ended_without_undo":
        return None
    checkpoint_id = item.get("run_base")
    with db_connect(dsn, autocommit=True) as conn:
        if checkpoint_id and run_is_live(conn, str(checkpoint_id)):
            return "the run holds its liveness lock again"
        if named:
            return None
        with conn.cursor(row_factory=tuple_row) as cur:
            cur.execute(
                """
                SELECT extract(epoch FROM now() - greatest(
                         (SELECT created_at::timestamptz
                            FROM public._chronos_branch_interval_checkpoints
                           WHERE checkpoint_id = %s),
                         (SELECT max(updated_at)
                            FROM okg.fact_batch_receipts
                           WHERE admission_id = %s::uuid)
                       ))
                """,
                (checkpoint_id, item.get("admission_id")),
            )
            idle = cur.fetchone()[0]
    if idle is not None and float(idle) < FINISH_UNDO_QUIET_SECONDS:
        return (
            f"the run was active {int(float(idle))} s ago (under "
            f"{FINISH_UNDO_QUIET_SECONDS} s): it may still be going with a "
            "lost liveness connection. Retry later, or name it with "
            f"--run-id {item['run_id']} if its process is gone"
        )
    return None


def _fail_undone_admission(
    dsn: str, admission_id: str | None, *, actor: str,
) -> str | None:
    """Close the admission of a run whose undo finish-undo just completed.

    A run that ended without its undo left its admission ``pending``; once
    its receipts are ``compensated`` it is a failed admission, and leaving
    it pending would let a staged-work sweep take it for unpublished work.
    Returns the admission's status afterwards.
    """
    if not admission_id:
        return None
    from okg.substrate.admission import transition_envelope
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn) as conn, conn.transaction():
        status = _admission_status(conn, admission_id)
        if status != "pending":
            return status
        transition_envelope(
            conn,
            admission_id=admission_id,
            status="failed",
            actor=actor,
            reason="source run ended before its undo; okg runtime finish-undo "
                   "undid it",
        )
    return "failed"


def _accept_current(
    dsn: str,
    item: dict[str, Any],
    *,
    deployment: str,
    reason: str | None,
    actor: str,
) -> None:
    """Keep the current rows of a run whose undo did not finish.

    The receipts go to compensated and the run base is deleted; in the same
    transaction the source is marked rerun-required, which keeps every
    publish refused and records the operator's reason durably.
    """
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.fact_batch import PostgresApplicationReceiptStore

    with db_connect(dsn) as conn:
        with conn.transaction():
            if item.get("admission_id"):
                receipts = PostgresApplicationReceiptStore(conn)
                with conn.cursor(row_factory=tuple_row) as cur:
                    cur.execute(
                        """
                        SELECT run_id, chunk_ordinal
                          FROM okg.fact_batch_receipts
                         WHERE admission_id = %s::uuid
                           AND state = 'compensating'
                         ORDER BY run_id, chunk_ordinal
                           FOR UPDATE
                        """,
                        (item["admission_id"],),
                    )
                    keys = list(cur.fetchall())
                for receipt_run_id, ordinal in keys:
                    receipt = receipts.get(
                        item["admission_id"], receipt_run_id, ordinal,
                    )
                    if receipt is not None and receipt.state == "compensating":
                        receipts.transition(receipt.key, "compensated")
            delete_run_base(
                conn,
                run_base_checkpoint_id(item["branch"], item["run_id"]),
                force=True,
            )
            mark_rerun_required(
                conn,
                deployment=deployment,
                source=str(item["source"]),
                run_id=str(item["run_id"]),
                reason=reason,
                actor=actor,
            )
