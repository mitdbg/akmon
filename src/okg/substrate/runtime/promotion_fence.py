"""Database-authoritative writer and scheduler fence for promotions."""
from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from okg.substrate.db.connection import connect as db_connect
from okg.substrate.errors import DeploymentEnvironmentInvalid


FENCE_TABLE = "okg.deployment_promotion_fences"
RECOVERY_CONTEXT_KIND = "runtime_dlq_witness"
RECOVERY_CONTEXT_KIND_KEY = "okg_recovery_kind"
RECOVERY_CONTEXT_PROMOTION_KEY = "okg_promotion_id"
# Fence sessions were the ones observed stranded in issue #1310, so they take
# the shared factory's kwargs: TCP keepalives reap the backend when the client
# process dies, and `idle_in_transaction_session_timeout` bounds a transaction
# a live client forgot to close. Both were absent from the bare
# `psycopg.connect(dsn, connect_timeout=5)` calls this replaced.
_FENCE_APPLICATION_NAME = "okg-promotion-fence"


def _fence_connect(*, deployment: str, dsn: str) -> psycopg.Connection:
    """Open a fence session through the shared connection factory.

    This used to assemble `connection_kwargs` here and call this module's own
    `psycopg.connect`, so that the no-database tests of the cutover boundary
    (`tests/substrate/runtime/test_orphan_schedule_cleanup.py`) could stub
    `promotion_fence.psycopg`. The seam moved rather than disappeared: those
    tests now stub `promotion_fence.db_connect`, which is the name this module
    calls, so a placeholder DSN is still never resolved for real.
    """
    return db_connect(
        dsn,
        connect_timeout=5,
        deployment=deployment,
        application_name=_FENCE_APPLICATION_NAME,
    )


def promotion_id_from_recovery_context(context: Any) -> str | None:
    """Return the owner only from the one-shot fenced-recovery context."""
    if not isinstance(context, Mapping):
        return None
    if context.get(RECOVERY_CONTEXT_KIND_KEY) != RECOVERY_CONTEXT_KIND:
        return None
    value = str(context.get(RECOVERY_CONTEXT_PROMOTION_KEY) or "").strip()
    return value or None


def fenced_recovery_context(*, promotion_id: str) -> dict[str, str]:
    """Build the explicit context consumed by fenced recovery workflows."""
    value = promotion_id.strip()
    if not value:
        raise ValueError("promotion_id must be non-empty")
    return {
        RECOVERY_CONTEXT_KIND_KEY: RECOVERY_CONTEXT_KIND,
        RECOVERY_CONTEXT_PROMOTION_KEY: value,
    }


@contextmanager
def deployment_admission_lock(
    *,
    deployment: str,
    dsn: str,
    bound_idle_in_transaction: bool = True,
) -> Iterator[psycopg.Connection]:
    """Hold the deployment-wide producer lock for a DB transaction.

    Source producers and promotion transitions use the same transaction-scoped
    advisory lock.  The replay-boundary cutover also holds this lock through
    ``DBOS.launch()`` so a producer cannot enqueue between the old-history
    sweep and registration of the replacement application version.

    ``bound_idle_in_transaction=False`` lifts the connection factory's
    idle-in-transaction bound for this one transaction. That launch-through
    hold is the single case where a long idle transaction is the intended
    behavior rather than the leak issue #1310 is about, and where being
    terminated mid-hold would refuse worker readiness with a message about
    the cutover instead of about a timeout. Every other caller stays bounded.
    """
    with _fence_connect(deployment=deployment, dsn=dsn) as conn:
        with conn.cursor() as cur:
            if not bound_idle_in_transaction:
                # SET LOCAL, so the exemption dies with this transaction and
                # cannot leak onto a later one over the same connection.
                cur.execute(
                    "SELECT set_config("
                    "'idle_in_transaction_session_timeout', '0', true)"
                )
            _lock_deployment(cur, deployment)
        yield conn


@contextmanager
def promotion_enqueue_permit(
    *,
    deployment: str,
    dsn: str,
    promotion_id: str | None = None,
) -> Iterator[None]:
    """Hold an enqueue permit across the caller's DBOS enqueue.

    Promotion fence acquisition uses the same transaction-scoped advisory
    lock. Once acquisition commits, every later producer observes the fence;
    producers already holding a permit finish before the controller drains.
    """
    with deployment_admission_lock(deployment=deployment, dsn=dsn) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            _require_fence_table(cur)
            cur.execute(
                f"""
                SELECT promotion_id, status
                  FROM {FENCE_TABLE}
                 WHERE deployment_name = %s
                """,
                (deployment,),
            )
            row = cur.fetchone()
            if (
                row is not None
                and row["status"] != "released"
                and row["promotion_id"] != promotion_id
            ):
                raise _fence_error(
                    "deployment enqueues are fenced for an active promotion",
                    deployment=deployment,
                    promotion_id=row["promotion_id"],
                    fence_status=row["status"],
                )
            yield


def assert_deployment_admitted(
    *,
    deployment: str,
    dsn: str,
    promotion_id: str | None = None,
) -> None:
    """Refuse now if a promotion owns the fence, then release the transaction.

    Same admission decision as :func:`promotion_enqueue_permit`, without
    holding the permit afterwards. Use it for a caller whose body is a long
    non-database wait — a subprocess, a model call, a sleep — where the permit
    would keep a transaction and its advisory lock open for the whole wait.

    That is a weaker guarantee, deliberately, and the difference is a caller
    obligation this function cannot check:

    * A promotion CAN now fence the deployment while the body is still
      running, and the body will not notice. Under the permit,
      :func:`acquire_promotion_fence` took the same advisory lock and so
      blocked until the body finished.
    * The caller must therefore ensure its body enqueues nothing and mutates
      nothing a promotion has to drain. Anything that enqueues work must still
      take :func:`promotion_enqueue_permit` around the enqueue itself, so the
      fence and the enqueue are decided together.
    * A body whose own DBOS row is outside
      ``replaceable_cutover_workflow_policies`` and is still ``PENDING`` at
      the replay boundary makes the cutover refuse
      (``RuntimeCutoverBlocked``). That is a bounded, loud failure in place of
      an unbounded pool jam, which is the trade this function exists to make
      — but it is a trade, not a free win.
    """
    with promotion_enqueue_permit(
        deployment=deployment, dsn=dsn, promotion_id=promotion_id,
    ):
        return


def acquire_promotion_fence(
    *,
    deployment: str,
    dsn: str,
    promotion_id: str,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Claim durable promotion ownership and remove all DBOS schedules."""
    with _fence_connect(deployment=deployment, dsn=dsn) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            _lock_deployment(cur, deployment)
            _require_fence_table(cur)
            cur.execute(
                f"""
                SELECT promotion_id, status, fenced_at, schedules_removed,
                       schedule_snapshot, metadata
                  FROM {FENCE_TABLE}
                 WHERE deployment_name = %s
                 FOR UPDATE
                """,
                (deployment,),
            )
            existing = cur.fetchone()
            if (
                existing is not None
                and existing["status"] != "released"
                and existing["promotion_id"] != promotion_id
            ):
                raise _fence_error(
                    "another promotion owns the production writer fence",
                    deployment=deployment,
                    requested_promotion_id=promotion_id,
                    owning_promotion_id=existing["promotion_id"],
                    fence_status=existing["status"],
                )

            schedule_snapshot = _capture_and_remove_dbos_schedules(cur)
            removed_now = len(schedule_snapshot)
            replacing_released = (
                existing is not None and existing["status"] == "released"
            )
            resumed = existing is not None and not replacing_released
            if existing is None or replacing_released:
                cur.execute(
                    f"""
                    INSERT INTO {FENCE_TABLE} (
                      deployment_name, promotion_id, status, schedules_removed,
                      schedule_snapshot, fenced_at, updated_at, blocker, metadata
                    )
                    VALUES (%s, %s, 'fenced', %s, %s, now(), now(), NULL, %s)
                    ON CONFLICT (deployment_name) DO UPDATE SET
                      promotion_id = EXCLUDED.promotion_id,
                      status = 'fenced',
                      schedules_removed = EXCLUDED.schedules_removed,
                      schedule_snapshot = EXCLUDED.schedule_snapshot,
                      fenced_at = now(),
                      updated_at = now(),
                      blocker = NULL,
                      metadata = EXCLUDED.metadata
                    RETURNING deployment_name, promotion_id, status,
                              schedules_removed, schedule_snapshot,
                              fenced_at, updated_at, metadata
                    """,
                    (
                        deployment,
                        promotion_id,
                        removed_now,
                        Jsonb(schedule_snapshot),
                        Jsonb(dict(metadata or {})),
                    ),
                )
            else:
                # Reacquisition by the same active owner is idempotent. It may
                # remove schedules registered by a restarted candidate worker,
                # but it must never replace the predecessor snapshot captured
                # at the original fence boundary, including an exactly empty
                # snapshot.
                cur.execute(
                    f"""
                    UPDATE {FENCE_TABLE}
                       SET status = 'fenced',
                           schedules_removed = schedules_removed + %s,
                           updated_at = now(),
                           blocker = NULL,
                           metadata = metadata || %s
                     WHERE deployment_name = %s
                       AND promotion_id = %s
                    RETURNING deployment_name, promotion_id, status,
                              schedules_removed, schedule_snapshot,
                              fenced_at, updated_at, metadata
                    """,
                    (
                        removed_now,
                        Jsonb(dict(metadata or {})),
                        deployment,
                        promotion_id,
                    ),
                )
            row = dict(cur.fetchone())
            row["schedules_removed_now"] = removed_now
            row["resumed"] = resumed
            row["replaced_release_receipt"] = replacing_released
            row["schedule_snapshot_digest"] = promotion_schedule_snapshot_digest(
                row.pop("schedule_snapshot")
            )
            row["receipt_digest"] = _digest(row)
            return _jsonable(row)


def promotion_fence_status(*, deployment: str, dsn: str) -> dict[str, Any] | None:
    with _fence_connect(deployment=deployment, dsn=dsn) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            _require_fence_table(cur)
            cur.execute(
                f"""
                SELECT deployment_name, promotion_id, status, schedules_removed,
                       schedule_snapshot, fenced_at, updated_at, blocker, metadata
                  FROM {FENCE_TABLE}
                 WHERE deployment_name = %s
                """,
                (deployment,),
            )
            row = cur.fetchone()
    return _jsonable(dict(row)) if row is not None else None


def mark_promotion_recovery_blocked(
    *,
    deployment: str,
    dsn: str,
    promotion_id: str,
    blocker: Mapping[str, Any],
) -> dict[str, Any]:
    with _fence_connect(deployment=deployment, dsn=dsn) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            _lock_deployment(cur, deployment)
            _require_fence_table(cur)
            cur.execute(
                f"""
                UPDATE {FENCE_TABLE}
                   SET status = 'recovery_blocked',
                       blocker = %s,
                       updated_at = now()
                 WHERE deployment_name = %s
                   AND promotion_id = %s
                RETURNING deployment_name, promotion_id, status,
                          schedules_removed, fenced_at, updated_at, blocker
                """,
                (Jsonb(dict(blocker)), deployment, promotion_id),
            )
            row = cur.fetchone()
            if row is None:
                raise _fence_error(
                    "cannot mark a promotion fence not owned by this promotion",
                    deployment=deployment,
                    promotion_id=promotion_id,
                )
            return _jsonable(dict(row))


def release_promotion_fence(
    *,
    deployment: str,
    dsn: str,
    promotion_id: str,
    schedule_disposition: str = "restore",
) -> dict[str, Any]:
    """Durably reopen writers after applying the requested schedule policy.

    ``restore`` reinstates the predecessor snapshot and is used when an
    operation aborts before mutation. ``keep`` preserves schedules registered
    by the candidate and is used only after generation-bound readiness.
    """
    if schedule_disposition not in {"restore", "keep"}:
        raise _fence_error(
            "promotion fence release has an invalid schedule disposition",
            deployment=deployment,
            promotion_id=promotion_id,
            schedule_disposition=schedule_disposition,
        )
    with _fence_connect(deployment=deployment, dsn=dsn) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            _lock_deployment(cur, deployment)
            _require_fence_table(cur)
            cur.execute(
                f"""
                SELECT promotion_id, status, schedules_removed,
                       schedule_snapshot, fenced_at, metadata
                  FROM {FENCE_TABLE}
                 WHERE deployment_name = %s
                 FOR UPDATE
                """,
                (deployment,),
            )
            existing = cur.fetchone()
            if existing is None:
                receipt = {
                    "deployment_name": deployment,
                    "promotion_id": promotion_id,
                    "status": "absent",
                    "schedules_removed": 0,
                    "released": False,
                    "was_present": False,
                    "schedule_disposition": schedule_disposition,
                }
                receipt["receipt_digest"] = _digest(receipt)
                return receipt
            if existing["promotion_id"] != promotion_id:
                raise _fence_error(
                    "cannot release a promotion fence not owned by this promotion",
                    deployment=deployment,
                    promotion_id=promotion_id,
                    owning_promotion_id=existing["promotion_id"],
                )
            if existing["status"] == "released":
                release_receipt = (
                    existing.get("metadata") or {}
                ).get("release_receipt")
                if not isinstance(release_receipt, dict):
                    raise _fence_error(
                        "released promotion fence is missing its durable receipt",
                        deployment=deployment,
                        promotion_id=promotion_id,
                    )
                if (
                    release_receipt.get("schedule_disposition")
                    != schedule_disposition
                ):
                    raise _fence_error(
                        "promotion fence was already released with a different schedule policy",
                        deployment=deployment,
                        promotion_id=promotion_id,
                        requested_disposition=schedule_disposition,
                        released_disposition=release_receipt.get(
                            "schedule_disposition"
                        ),
                    )
                return dict(release_receipt)

            snapshot = list(existing.get("schedule_snapshot") or ())
            if not _released_status_supported(cur):
                if schedule_disposition != "restore":
                    raise _fence_error(
                        "durable promotion release schema is not applied",
                        deployment=deployment,
                        promotion_id=promotion_id,
                        required_migration=(
                            "0018_promotion_fence_release_receipt"
                        ),
                    )
                # The fence is acquired before the candidate's migration phase.
                # On that legacy schema, the only legal release is a
                # pre-mutation abort: restore the exact predecessor schedules
                # and delete the fence so the still-running predecessor worker
                # can enqueue again. Successful readiness runs after migration
                # and must take the durable tombstone path below.
                restored = _restore_dbos_schedules(cur, snapshot)
                receipt = {
                    "deployment_name": deployment,
                    "promotion_id": promotion_id,
                    "status": "released",
                    "schedules_removed": int(existing["schedules_removed"]),
                    "schedule_snapshot_digest": (
                        promotion_schedule_snapshot_digest(snapshot)
                    ),
                    "fenced_at": existing["fenced_at"],
                    "released_at": datetime.now(timezone.utc).isoformat(),
                    "released": True,
                    "was_present": True,
                    "durable_release_receipt": False,
                    "legacy_schema": True,
                    "schedule_disposition": "restore",
                    "schedules_restored": restored,
                    "schedules_kept": 0,
                }
                receipt = _jsonable(receipt)
                receipt["receipt_digest"] = _digest(receipt)
                cur.execute(
                    f"""
                    DELETE FROM {FENCE_TABLE}
                     WHERE deployment_name = %s
                       AND promotion_id = %s
                    """,
                    (deployment, promotion_id),
                )
                return receipt
            restored = (
                _restore_dbos_schedules(cur, snapshot)
                if schedule_disposition == "restore"
                else 0
            )
            candidate_schedules = (
                _capture_dbos_schedules(cur)
                if schedule_disposition == "keep"
                else []
            )
            kept = len(candidate_schedules)
            removed = int(existing["schedules_removed"])
            # `keep` means "the candidate registered its own schedules, so
            # leave the live table alone". Fence acquisition already deleted
            # the predecessor's rows, so releasing with keep while the
            # candidate is missing any of them silently ends that scheduled
            # work — and the digest of [] is a well-formed sha256, so a
            # digest-shape check cannot catch the total case.
            #
            # Compare schedule_name sets rather than counts: a candidate that
            # registered 1 of 3 is just as broken as one that registered 0,
            # and only the names say which. Compare against the snapshot
            # itself, not the cumulative `schedules_removed` counter, which
            # sums across resumes and would refuse a genuinely empty
            # predecessor. Refusing here rolls the transaction back, so the
            # fence stays held with its snapshot intact and the operation can
            # still be released with `restore`.
            if schedule_disposition == "keep":
                snapshot_names = {
                    str(row["schedule_name"])
                    for row in snapshot
                    if row.get("schedule_name") is not None
                }
                candidate_names = {
                    str(row["schedule_name"])
                    for row in candidate_schedules
                    if row.get("schedule_name") is not None
                }
                dropped = sorted(snapshot_names - candidate_names)
                if dropped:
                    raise _fence_error(
                        "refusing to release the promotion fence with "
                        "schedule_disposition=keep: the candidate did not "
                        "register every schedule the fence removed, so "
                        "keeping would leave the deployment without them",
                        deployment=deployment,
                        promotion_id=promotion_id,
                        dropped_schedule_names=dropped,
                        schedules_removed=removed,
                        schedules_kept=kept,
                        remedy=(
                            "release with schedule_disposition='restore' to "
                            "recover the predecessor's schedules"
                        ),
                    )
            released_at = datetime.now(timezone.utc).isoformat()
            receipt = {
                "deployment_name": deployment,
                "promotion_id": promotion_id,
                "status": "released",
                "schedules_removed": removed,
                "schedule_snapshot_digest": (
                    promotion_schedule_snapshot_digest(snapshot)
                ),
                "fenced_at": existing["fenced_at"],
                "released_at": released_at,
                "released": True,
                "was_present": True,
                "durable_release_receipt": True,
                "schedule_disposition": schedule_disposition,
                "schedules_restored": restored,
                "schedules_kept": kept,
                "candidate_schedule_snapshot_digest": (
                    promotion_schedule_snapshot_digest(candidate_schedules)
                ),
                "candidate_schedule_ids": [
                    str(row["schedule_id"]) for row in candidate_schedules
                ],
            }
            receipt = _jsonable(receipt)
            receipt["receipt_digest"] = _digest(receipt)
            metadata = dict(existing.get("metadata") or {})
            metadata["release_receipt"] = receipt
            cur.execute(
                f"""
                UPDATE {FENCE_TABLE}
                   SET status = 'released',
                       blocker = NULL,
                       metadata = %s,
                       updated_at = now()
                 WHERE deployment_name = %s
                   AND promotion_id = %s
                """,
                (Jsonb(metadata), deployment, promotion_id),
            )
            return receipt


def _lock_deployment(cur: psycopg.Cursor, deployment: str) -> None:
    cur.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (f"okg:deployment-promotion:{deployment}",),
    )


def _require_fence_table(cur: psycopg.Cursor) -> None:
    cur.execute("SELECT to_regclass(%s) AS fence_table", (FENCE_TABLE,))
    if cur.fetchone()["fence_table"] is None:
        raise _fence_error(
            "deployment promotion fence schema is missing",
            required_relation=FENCE_TABLE,
        )


def _capture_and_remove_dbos_schedules(cur: psycopg.Cursor) -> list[dict[str, Any]]:
    cur.execute("SELECT to_regclass('dbos.workflow_schedules') AS schedules")
    if cur.fetchone()["schedules"] is None:
        return []
    cur.execute(
        """
        SELECT schedule_id, schedule_name, workflow_name, workflow_class_name,
               schedule, status, context, last_fired_at, automatic_backfill,
               cron_timezone, queue_name
          FROM dbos.workflow_schedules
         ORDER BY schedule_id
         FOR UPDATE
        """
    )
    snapshot = [dict(row) for row in cur.fetchall()]
    cur.execute("DELETE FROM dbos.workflow_schedules")
    return snapshot


def _restore_dbos_schedules(
    cur: psycopg.Cursor,
    snapshot: list[dict[str, Any]],
) -> int:
    cur.execute("SELECT to_regclass('dbos.workflow_schedules') AS schedules")
    if cur.fetchone()["schedules"] is None:
        if not snapshot:
            return 0
        raise _fence_error(
            "cannot restore promotion-fenced DBOS schedules",
            missing_relation="dbos.workflow_schedules",
        )
    cur.execute("DELETE FROM dbos.workflow_schedules")
    if not snapshot:
        return 0
    cur.executemany(
        """
        INSERT INTO dbos.workflow_schedules (
          schedule_id, schedule_name, workflow_name, workflow_class_name,
          schedule, status, context, last_fired_at, automatic_backfill,
          cron_timezone, queue_name
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (schedule_id) DO UPDATE SET
          schedule_name = EXCLUDED.schedule_name,
          workflow_name = EXCLUDED.workflow_name,
          workflow_class_name = EXCLUDED.workflow_class_name,
          schedule = EXCLUDED.schedule,
          status = EXCLUDED.status,
          context = EXCLUDED.context,
          last_fired_at = EXCLUDED.last_fired_at,
          automatic_backfill = EXCLUDED.automatic_backfill,
          cron_timezone = EXCLUDED.cron_timezone,
          queue_name = EXCLUDED.queue_name
        """,
        [
            (
                row["schedule_id"],
                row["schedule_name"],
                row["workflow_name"],
                row.get("workflow_class_name"),
                row["schedule"],
                row["status"],
                row["context"],
                row.get("last_fired_at"),
                bool(row.get("automatic_backfill", False)),
                row.get("cron_timezone"),
                row.get("queue_name"),
            )
            for row in snapshot
        ],
    )
    return len(snapshot)


def _capture_dbos_schedules(cur: psycopg.Cursor) -> list[dict[str, Any]]:
    cur.execute("SELECT to_regclass('dbos.workflow_schedules') AS schedules")
    if cur.fetchone()["schedules"] is None:
        return []
    cur.execute(
        """
        SELECT schedule_id, schedule_name, workflow_name, workflow_class_name,
               schedule, status, context, last_fired_at, automatic_backfill,
               cron_timezone, queue_name
          FROM dbos.workflow_schedules
         ORDER BY schedule_id
         FOR UPDATE
        """
    )
    return [dict(row) for row in cur.fetchall()]


def _released_status_supported(cur: psycopg.Cursor) -> bool:
    cur.execute(
        """
        SELECT pg_get_constraintdef(oid) AS definition
          FROM pg_constraint
         WHERE conrelid = %s::regclass
           AND contype = 'c'
           AND conname = 'deployment_promotion_fences_status_check'
        """,
        (FENCE_TABLE,),
    )
    row = cur.fetchone()
    return row is not None and "'released'::text" in str(row["definition"])


def _jsonable(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: item.isoformat() if isinstance(item, datetime) else item
        for key, item in value.items()
    }


def _digest(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        _jsonable(value), sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _digest_value(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def promotion_schedule_snapshot_digest(value: Any) -> str:
    """Return the canonical digest used by fence and release receipts."""
    return _digest_value(value)


def _fence_error(message: str, **data: Any) -> DeploymentEnvironmentInvalid:
    return DeploymentEnvironmentInvalid(
        message,
        data=data,
        fix_hint=(
            "finish or recover the owning promotion before accepting new work; "
            "run okg migrate first if the fence relation is missing"
        ),
    )
