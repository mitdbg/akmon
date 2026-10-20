"""Clear exactly one stuck DBOS workflow from a deployment queue.

A deployment partition queue runs one workflow at a time. Two observed
failures leave a single row in a non-final state on that queue forever, so
the one slot is never released and publishing stops while every read-side
check stays green:

  * internal issue 1257 — a publisher cycle that raised stayed ``PENDING`` and
    held the slot; every later scheduled cycle enqueued behind it and never
    started.
  * internal issue 1254 — a scheduled workflow left ``ENQUEUED`` by the previous
    application version blocked worker readiness, and only a booted worker
    could have consumed it.

Both recoveries used to require hand-written SQL against
``dbos.workflow_status`` or the destructive ``okg runtime reset --confirm``.
This module is the one mechanism behind both remedies:

  * :func:`cancel_deployment_workflow` — the scoped operator verb
    (``okg runtime cancel-workflow``). It cancels exactly one named
    non-final workflow on a queue the named deployment owns, and refuses
    without writing anything otherwise.
  * :func:`cleanup_deadline_expired_pending_workflows` — the automatic
    bound. DBOS records a ``workflow_deadline_epoch_ms`` for every workflow
    enqueued with a timeout and promises to stop the workflow at it. A row
    still ``PENDING`` past that recorded deadline therefore has no executor
    honoring the contract, so cancelling it enforces the deadline that was
    already declared rather than inventing a new one.

Both paths write the same terminal ``CANCELLED`` status the substrate
already uses for a replaceable workflow it retires, which is what releases
the queue slot: DBOS counts a queue's occupancy as the rows on it whose
status is ``PENDING``. Neither path touches a workflow that is already final.

Both also clear ``deduplication_id`` and stamp ``completed_at``, as DBOS's
own cancel does. Without clearing the dedup key the cancelled row keeps
holding its ``(queue_name, deduplication_id)`` slot in a unique index, and
the next enqueue inside the same dedup bucket collides with a workflow that
is no longer running. ``queue_name`` and ``started_at_epoch_ms`` are kept,
unlike DBOS's cancel, because an operator reading the cancelled row after
the fact needs to know which queue was wedged and when the run began.

Scope note. The automatic bound reaches only workflows DBOS recorded a
deadline for, which today means the publisher cycle: it is the one
substrate schedule that declares a workflow timeout. Anything else wedged on
the same queue is cleared through the operator verb.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from okg.substrate.db.connection import connect as db_connect

from okg.substrate.errors import SubstrateError
from okg.substrate.support.user_hints import okg_prefix


__all__ = [
    "CANCELLABLE_STATUSES",
    "CANCELLED_STATUS",
    "WorkflowCancelResult",
    "cancel_deployment_workflow",
    "cancel_workflow_remedy_command",
    "cleanup_deadline_expired_pending_workflows",
    "deadline_expired_pending_workflows",
]


log = logging.getLogger(__name__)


#: The non-final statuses that still occupy a queue slot. Anything else is
#: terminal DBOS history and must never be rewritten by this module.
CANCELLABLE_STATUSES: tuple[str, ...] = ("ENQUEUED", "PENDING", "DELAYED")

CANCELLED_STATUS = "CANCELLED"

_DEADLINE_CANDIDATE_FIELDS: tuple[str, ...] = (
    "workflow_uuid",
    "name",
    "queue_name",
    "application_version",
    "started_at_epoch_ms",
    "workflow_deadline_epoch_ms",
)


def cancel_workflow_remedy_command(
    *, deployment: str, workflow_id: str,
) -> str:
    """Return the exact operator command that clears one workflow.

    Every diagnostic that names a blocking workflow uses this so the message
    names an action instead of only a row.
    """
    return (
        f"{okg_prefix()} runtime cancel-workflow --deployment "
        f"{deployment} {workflow_id}"
    )


@dataclass(frozen=True)
class WorkflowCancelResult:
    """What one cancellation actually did, read back from the row.

    ``queue_slot_released`` is derived from the row that was cancelled, not
    assumed: a cancellation can clear a version-cutover blocker without
    freeing any concurrency slot.
    """

    deployment: str
    workflow_id: str
    name: str
    queue_name: str | None
    previous_status: str
    status: str
    application_version: str | None
    recovery_attempts: int | None
    queue_slot_released: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "deployment": self.deployment,
            "workflow_id": self.workflow_id,
            "name": self.name,
            "queue_name": self.queue_name,
            "previous_status": self.previous_status,
            "status": self.status,
            "application_version": self.application_version,
            "recovery_attempts": self.recovery_attempts,
            "queue_slot_released": self.queue_slot_released,
        }


def _refuse(code: str, message: str, **data: Any) -> SubstrateError:
    err = SubstrateError(message, data=dict(data))
    err.code = code
    return err


#: DBOS runs a schedule whose declared queue is ``None`` through its own
#: internal queue. The substrate's doctor schedule and every deployment-author
#: schedule are registered that way, so a workflow stranded there belongs to
#: this deployment even though its queue name does not carry the deployment.
#: ``admin.client_drain`` already treats it as part of the deployment's queue
#: set for the same reason.
DBOS_INTERNAL_QUEUE = "_dbos_internal_queue"

def _deployment_queue_names(
    deployment: str, *, include_internal: bool,
) -> tuple[str, ...]:
    """Return the DBOS queues this deployment owns.

    ``include_internal`` adds DBOS's internal queue. The operator verb needs
    it, because a workflow stranded there is exactly what the cutover refusal
    names. The deadline sweep does not: that queue has no bounded concurrency,
    so nothing there can wedge a slot, and ``services`` already sweeps it.
    """
    from okg.substrate.runtime.partition_queue import (
        deployment_runtime_queue_names,
    )

    names = tuple(
        str(name)
        for name in deployment_runtime_queue_names(deployment)
        if name
    )
    if not names:
        # Fail closed. An empty ownership set would make the scope check
        # below vacuous and let this verb cancel any deployment's workflow.
        # Defensive: today the queue-name helper always returns two names.
        raise _refuse(
            "deployment_queues_unresolved",
            "cannot resolve the DBOS queues owned by deployment "
            f"{deployment!r}; refusing to cancel a workflow without a "
            "deployment scope",
            deployment=deployment,
        )
    return (*names, DBOS_INTERNAL_QUEUE) if include_internal else names


def cancel_deployment_workflow(
    *,
    dsn: str,
    deployment: str,
    workflow_id: str,
    expected_status: str | None = None,
) -> WorkflowCancelResult:
    """Cancel one non-final workflow on a queue ``deployment`` owns.

    This is the scoped, non-destructive remedy for a wedged publish queue.
    It writes exactly one row and only when every check below holds; each
    failure raises rather than reporting a cancellation that did not happen.

    ``expected_status`` narrows the cancel to a row in exactly that
    status. ``client_enqueue_publisher_cycle`` passes ``"ENQUEUED"`` so a
    cycle a worker has already started is never cancelled out from under
    it (issue #2502). A row in any other status is refused with
    ``workflow_status_changed`` and nothing is written; the write itself
    is conditioned on the status read, so a transition between the read
    and the write is refused the same way.

    Refuses when:

      * the DBOS schema does not exist (``dbos_schema_absent``),
      * no workflow has that id (``workflow_not_found``),
      * the workflow is already final (``workflow_already_terminal``) — its
        history is the record of what happened and holds no queue slot,
      * the workflow runs on a queue this deployment does not own
        (``workflow_outside_deployment``), which also covers a workflow with
        no queue at all, since such a workflow occupies no slot,
      * the status changed between the read and the write
        (``workflow_status_changed``), so a concurrent transition is never
        reported as this command's doing.
    """
    queue_names = _deployment_queue_names(deployment, include_internal=True)
    with db_connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", ("dbos.workflow_status",))
            relation = cur.fetchone()
            if relation is None or relation[0] is None:
                raise _refuse(
                    "dbos_schema_absent",
                    "the DBOS workflow table does not exist on this "
                    "deployment database; nothing to cancel for "
                    f"{deployment!r}",
                    deployment=deployment,
                    workflow_id=workflow_id,
                )
            cur.execute(
                "SELECT name, status, queue_name, application_version, "
                "       recovery_attempts "
                "  FROM dbos.workflow_status "
                " WHERE workflow_uuid = %s",
                (workflow_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise _refuse(
                    "workflow_not_found",
                    f"no DBOS workflow with id {workflow_id!r} on deployment "
                    f"{deployment!r}",
                    deployment=deployment,
                    workflow_id=workflow_id,
                )
            name, status, queue_name, application_version, attempts = row
            status = str(status)
            if expected_status is not None and status != expected_status:
                raise _refuse(
                    "workflow_status_changed",
                    f"workflow {workflow_id!r} is {status}, not "
                    f"{expected_status}; nothing was cancelled",
                    deployment=deployment,
                    workflow_id=workflow_id,
                    observed_status=expected_status,
                    current_status=status,
                )
            if status not in CANCELLABLE_STATUSES:
                raise _refuse(
                    "workflow_already_terminal",
                    f"workflow {workflow_id!r} is {status}, which is terminal "
                    "DBOS history holding no queue slot; this verb clears "
                    "only " + ", ".join(CANCELLABLE_STATUSES),
                    deployment=deployment,
                    workflow_id=workflow_id,
                    name=name,
                    status=status,
                    queue_name=queue_name,
                )
            # A NULL queue is accepted. DBOS records one for a workflow that
            # never reached a queue, and the application-version cutover
            # refusal names such rows as blockers, so refusing them here would
            # make the remedy that refusal advertises a dead end (#1254).
            if queue_name is not None and queue_name not in queue_names:
                raise _refuse(
                    "workflow_outside_deployment",
                    f"workflow {workflow_id!r} runs on queue "
                    f"{queue_name!r}, which deployment {deployment!r} does "
                    f"not own ({', '.join(queue_names)}); refusing to cancel "
                    "it under this deployment",
                    deployment=deployment,
                    workflow_id=workflow_id,
                    name=name,
                    status=status,
                    queue_name=queue_name,
                    deployment_queues=list(queue_names),
                )
            cur.execute(
                "UPDATE dbos.workflow_status "
                "   SET status = %s, updated_at = %s, completed_at = %s, "
                "       deduplication_id = NULL "
                " WHERE workflow_uuid = %s "
                "   AND status = %s "
                "RETURNING workflow_uuid, name, queue_name, status",
                (
                    CANCELLED_STATUS,
                    int(time.time() * 1000),
                    int(time.time() * 1000),
                    workflow_id,
                    status,
                ),
            )
            updated = cur.fetchone()
            if updated is None:
                cur.execute(
                    "SELECT status FROM dbos.workflow_status "
                    " WHERE workflow_uuid = %s",
                    (workflow_id,),
                )
                current = cur.fetchone()
                raise _refuse(
                    "workflow_status_changed",
                    f"workflow {workflow_id!r} moved from {status} to "
                    f"{current[0] if current else '<deleted>'} while this "
                    "command ran; nothing was cancelled",
                    deployment=deployment,
                    workflow_id=workflow_id,
                    observed_status=status,
                    current_status=(str(current[0]) if current else None),
                )
        conn.commit()
    # Derived, not asserted. DBOS counts a bounded queue as occupied by its
    # PENDING rows, so only a PENDING row on one of this deployment's bounded
    # queues was holding a slot. Cancelling an ENQUEUED or DELAYED row, or any
    # row on DBOS's unbounded internal queue, unblocks a version cutover
    # without freeing a slot, and saying otherwise would overstate the result.
    slot_released = (
        status == "PENDING"
        and queue_name is not None
        and queue_name != DBOS_INTERNAL_QUEUE
    )
    log.warning(
        "cancelled stuck workflow: deployment=%s workflow_id=%s name=%s "
        "queue=%s previous_status=%s queue_slot_released=%s",
        deployment, workflow_id, name, queue_name, status, slot_released,
    )
    return WorkflowCancelResult(
        deployment=deployment,
        workflow_id=str(workflow_id),
        name=str(name),
        queue_name=(str(queue_name) if queue_name is not None else None),
        previous_status=status,
        status=CANCELLED_STATUS,
        application_version=(
            str(application_version)
            if application_version is not None
            else None
        ),
        recovery_attempts=(int(attempts) if attempts is not None else None),
        queue_slot_released=slot_released,
    )


def deadline_expired_pending_workflows(
    rows: Iterable[Mapping[str, Any] | Sequence[Any]],
    *,
    now_ms: int,
) -> tuple[dict[str, Any], ...]:
    """Select the PENDING rows whose own recorded deadline has passed.

    Pure decision, kept out of SQL so it is checkable without a database.
    Each row supplies ``workflow_uuid``, ``name``, ``queue_name``,
    ``application_version``, ``started_at_epoch_ms`` and
    ``workflow_deadline_epoch_ms``, either as a mapping or in that order.

    A row is expired only when DBOS recorded a deadline for it and that
    deadline is strictly in the past. A row with no recorded deadline is
    never expired here: this bound enforces a promise DBOS made, and
    inventing one for a workflow that never carried a timeout is exactly the
    guess that would cancel legitimate long-running work.
    """
    expired: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, Mapping):
            # Require every field. Tolerating a missing workflow_uuid would
            # produce a record that no row can match, which the caller then
            # reads as "it left PENDING on its own" — a malformed row dressed
            # up as a clean outcome.
            #
            # Phrasing note: this function performs no SQL. The wording avoids
            # naming a write verb next to an identifier, which the epoch
            # boundary scanner in scripts/check_formal_dbos_epoch_supersession
            # reads as a real mutation and would inventory as a fenced write.
            absent = [
                field
                for field in _DEADLINE_CANDIDATE_FIELDS
                if field not in row
            ]
            if absent:
                raise ValueError(
                    "deadline candidate row is missing required field(s) "
                    f"{absent}; expected {list(_DEADLINE_CANDIDATE_FIELDS)}"
                )
            record = {
                field: row[field] for field in _DEADLINE_CANDIDATE_FIELDS
            }
        else:
            values = list(row)
            if len(values) != len(_DEADLINE_CANDIDATE_FIELDS):
                raise ValueError(
                    "deadline candidate row must carry exactly "
                    f"{len(_DEADLINE_CANDIDATE_FIELDS)} values "
                    f"{_DEADLINE_CANDIDATE_FIELDS}, got {len(values)}"
                )
            record = dict(zip(_DEADLINE_CANDIDATE_FIELDS, values))
        deadline = record.get("workflow_deadline_epoch_ms")
        if deadline is None:
            continue
        deadline_ms = int(deadline)
        if deadline_ms >= now_ms:
            continue
        record["workflow_deadline_epoch_ms"] = deadline_ms
        record["overdue_seconds"] = (now_ms - deadline_ms) / 1000.0
        expired.append(record)
    return tuple(expired)


def cleanup_deadline_expired_pending_workflows(
    *,
    dsn: str,
    deployment: str,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Release queue slots held by PENDING workflows past their deadline.

    This is the automatic half of internal issue 1257. The stale-PENDING sweep
    deliberately leaves a workflow that already started alone, because a live
    executor may still be working. That protection is what let a raising
    publisher cycle hold the only slot indefinitely. A workflow past the
    deadline DBOS itself recorded is a different case: the executor that was
    supposed to stop it at that deadline is demonstrably not doing so, so the
    row is cancelled and the slot released.

    Returns the cancelled rows. A cancelled workflow is reported, never
    silently dropped, because it means a cycle failed to complete.
    """
    now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    queue_names = _deployment_queue_names(deployment, include_internal=False)
    with db_connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", ("dbos.workflow_status",))
            relation = cur.fetchone()
            if relation is None or relation[0] is None:
                # Say so rather than returning a zero that reads like a clean
                # sweep: "no DBOS schema" and "nothing was overdue" are
                # different answers and only one of them is reassuring.
                log.info(
                    "deadline-expired PENDING sweep found no DBOS workflow "
                    "table: deployment=%s — nothing was examined",
                    deployment,
                )
                return {
                    "deployment": deployment,
                    "cancelled_count": 0,
                    "cancelled": [],
                    "schema_initialized": False,
                    "now_ms": now_ms,
                }
            cur.execute(
                "SELECT workflow_uuid, name, queue_name, "
                "       application_version, started_at_epoch_ms, "
                "       workflow_deadline_epoch_ms "
                "  FROM dbos.workflow_status "
                " WHERE status = 'PENDING' "
                "   AND queue_name = ANY(%s) "
                "   AND workflow_deadline_epoch_ms IS NOT NULL",
                (list(queue_names),),
            )
            candidates = cur.fetchall()
            expired = deadline_expired_pending_workflows(
                candidates, now_ms=now_ms,
            )
            cancelled: list[dict[str, Any]] = []
            for record in expired:
                cur.execute(
                    "UPDATE dbos.workflow_status "
                    "   SET status = %s, updated_at = %s, completed_at = %s, "
                    "       deduplication_id = NULL "
                    " WHERE workflow_uuid = %s "
                    "   AND status = 'PENDING' "
                    "RETURNING workflow_uuid",
                    (
                        CANCELLED_STATUS,
                        now_ms,
                        now_ms,
                        record["workflow_uuid"],
                    ),
                )
                if cur.fetchone() is None:
                    # It left PENDING on its own between the read and the
                    # write. That is the outcome we wanted; do not report it
                    # as this sweep's doing.
                    continue
                cancelled.append(record)
        conn.commit()
    if cancelled:
        log.warning(
            "cancelled %d PENDING workflow(s) past the deadline DBOS "
            "recorded for them: deployment=%s rows=%s — each held a queue "
            "slot that is now released",
            len(cancelled), deployment, cancelled,
        )
    return {
        "deployment": deployment,
        "cancelled_count": len(cancelled),
        "cancelled": cancelled,
        "schema_initialized": True,
        "now_ms": now_ms,
    }
