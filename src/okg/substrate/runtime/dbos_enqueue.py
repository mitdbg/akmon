"""Small DBOS enqueue helpers for runtime queue producers."""
from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Sequence
from typing import Any

from okg.substrate.db.connection import connect as db_connect

from okg.substrate.runtime.partition_queue import partition_queue_name
from okg.substrate.runtime.promotion_fence import promotion_enqueue_permit


@dataclass(frozen=True)
class DBOSWorkflowEnqueueResult:
    workflow_id: str
    deduplicated: bool = False


class TerminalDeduplicationCollision(RuntimeError):
    """DBOS rejected a dedupe key but no active workflow owns it."""

    def __init__(
        self,
        *,
        queue_name: str,
        deduplication_id: str,
        terminal_workflow_id: str | None = None,
    ) -> None:
        detail = (
            f"; latest terminal workflow is {terminal_workflow_id}"
            if terminal_workflow_id
            else ""
        )
        super().__init__(
            "DBOS deduplicated "
            f"{deduplication_id!r} on queue {queue_name!r}, "
            f"but no active workflow row exists{detail}"
        )
        self.queue_name = queue_name
        self.deduplication_id = deduplication_id
        self.terminal_workflow_id = terminal_workflow_id


def enqueue_deployment_workflow(
    *,
    deployment: str,
    dsn: str,
    workflow_name: str,
    args: tuple[Any, ...] = (),
    deduplication_id: str | None = None,
    workflow_timeout: float | None = None,
    queue_name: str | None = None,
    priority: int | None = None,
    promotion_id: str | None = None,
    app_version: str | None = None,
) -> DBOSWorkflowEnqueueResult:
    """Enqueue one workflow on a deployment queue.

    When ``deduplication_id`` is set, DBOS keeps at most one active row
    for that logical work item. On duplicate, return the active workflow
    id instead of treating the enqueue as a producer failure.

    ``app_version`` pins the row to one registered application version.
    Pass the value a caller has already read strictly, so the row is
    pinned to the version that caller checked rather than to a second,
    best-effort read that can silently come back empty. Omit it and the
    helper takes its own best-effort reading.
    """
    from dbos import DBOSClient
    from dbos._error import DBOSQueueDeduplicatedError

    queue_name = queue_name or partition_queue_name(deployment)
    options: dict[str, Any] = {
        "queue_name": queue_name,
        "workflow_name": workflow_name,
    }
    if deduplication_id:
        options["deduplication_id"] = deduplication_id
    if workflow_timeout is not None:
        options["workflow_timeout"] = workflow_timeout
    if priority is not None:
        options["priority"] = priority
    if app_version is None:
        app_version = latest_registered_app_version(dsn)
    if app_version is not None:
        options["app_version"] = app_version

    client = DBOSClient(
        database_url=dsn,
        system_database_url=dsn,
        application_database_url=dsn,
    )
    with promotion_enqueue_permit(
        deployment=deployment,
        dsn=dsn,
        promotion_id=promotion_id,
    ):
        try:
            handle = client.enqueue(options, *args)
        except DBOSQueueDeduplicatedError as exc:
            if not deduplication_id:
                raise
            existing = active_workflow_id_for_deduplication(
                dsn=dsn,
                queue_name=queue_name,
                deduplication_id=deduplication_id,
            )
            if existing is None:
                terminal = workflow_id_for_deduplication(
                    dsn=dsn,
                    queue_name=queue_name,
                    deduplication_id=deduplication_id,
                )
                raise TerminalDeduplicationCollision(
                    queue_name=queue_name,
                    deduplication_id=deduplication_id,
                    terminal_workflow_id=terminal,
                ) from exc
            return DBOSWorkflowEnqueueResult(
                workflow_id=existing,
                deduplicated=True,
            )
    return DBOSWorkflowEnqueueResult(workflow_id=handle.workflow_id)


def registered_app_version(
    dsn: str,
    *,
    application_name: str = "okg-enqueue-guard",
    connect_timeout: int = 5,
) -> str | None:
    """Return the newest registered DBOS application version, strictly.

    ``None`` means one thing only: no worker has ever booted against
    this database, so nothing would dispatch a row enqueued here.
    Connection and query failures propagate unchanged, because "the
    database is unreachable" and "no worker has ever booted here" are
    different answers and must not collapse into one refusal.

    Callers that only want to decorate an enqueue with a hint use
    :func:`latest_registered_app_version`, which swallows the
    difference on purpose.
    """
    # The substrate direct-connect wrapper rather than a raw
    # psycopg.connect: it sets TCP keepalives and an application_name,
    # so this read is identifiable in pg_stat_activity.
    from okg.substrate.db.connection import connect

    with connect(
        dsn,
        application_name=application_name,
        connect_timeout=connect_timeout,
    ) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('dbos.application_versions')")
        if cur.fetchone()[0] is None:
            return None
        cur.execute(
            """
            SELECT version_name
              FROM dbos.application_versions
             ORDER BY version_timestamp DESC NULLS LAST,
                      created_at DESC NULLS LAST
             LIMIT 1
            """
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return str(row[0])


def latest_registered_app_version(dsn: str) -> str | None:
    """Return the newest DBOS app version, or None on any failure.

    A queue row stamped with a version is dispatched only by a worker
    on that version; an unstamped row is dispatched by any worker. Pin
    operator enqueues to the registered version so they follow the same
    dispatch and restamping rules as scheduled work. okg's worker boot
    path bumps the live version's timestamp
    (``services.bump_live_version_to_latest``); this reads that value.

    Best-effort by design: this only decorates an enqueue with a hint, so
    a read failure degrades to an unstamped row rather than a producer
    error. Do NOT reuse it as a refusal predicate -- use
    :func:`registered_app_version` for that.
    """
    try:
        return registered_app_version(
            dsn,
            application_name="okg-enqueue-hint",
            connect_timeout=2,
        )
    except Exception:  # noqa: BLE001 - enqueue stays best-effort.
        return None


def active_workflow_id_for_deduplication(
    *,
    dsn: str,
    queue_name: str,
    deduplication_id: str,
) -> str | None:
    return workflow_id_for_deduplication(
        dsn=dsn,
        queue_name=queue_name,
        deduplication_id=deduplication_id,
        statuses=("ENQUEUED", "PENDING", "DELAYED"),
    )


def active_workflow_id_for_name(
    *,
    dsn: str,
    queue_name: str,
    workflow_name: str,
    statuses: Sequence[str] = ("ENQUEUED", "PENDING", "DELAYED"),
) -> str | None:
    """Return the oldest active workflow for ``workflow_name``.

    Scheduled substrate wakeups are replaceable. If one is already queued or
    running, enqueueing another wakeup with the same workflow name only adds
    serialized queue latency; source state remains the authority.
    """
    if not statuses:
        return None
    with db_connect(dsn, connect_timeout=5) as conn, \
            conn.cursor() as cur:
        cur.execute("SELECT to_regclass('dbos.workflow_status')")
        if cur.fetchone()[0] is None:
            return None
        cur.execute(
            """
            SELECT workflow_uuid
              FROM dbos.workflow_status
             WHERE queue_name = %s
               AND name = %s
               AND status = ANY(%s)
             ORDER BY created_at ASC, workflow_uuid ASC
             LIMIT 1
            """,
            (queue_name, workflow_name, list(statuses)),
        )
        row = cur.fetchone()
    return str(row[0]) if row is not None else None


def workflow_id_for_deduplication(
    *,
    dsn: str,
    queue_name: str,
    deduplication_id: str,
    statuses: Sequence[str] | None = None,
) -> str | None:
    with db_connect(dsn, connect_timeout=5) as conn, \
            conn.cursor() as cur:
        cur.execute("SELECT to_regclass('dbos.workflow_status')")
        if cur.fetchone()[0] is None:
            return None
        if statuses is None:
            cur.execute(
                """
                SELECT workflow_uuid
                  FROM dbos.workflow_status
                 WHERE queue_name = %s
                   AND deduplication_id = %s
                 ORDER BY created_at DESC, workflow_uuid DESC
                 LIMIT 1
                """,
                (queue_name, deduplication_id),
            )
        else:
            cur.execute(
                """
                SELECT workflow_uuid
                  FROM dbos.workflow_status
                 WHERE queue_name = %s
                   AND deduplication_id = %s
                   AND status = ANY(%s)
                 ORDER BY created_at, workflow_uuid
                 LIMIT 1
                """,
                (queue_name, deduplication_id, list(statuses)),
            )
        row = cur.fetchone()
    return str(row[0]) if row is not None else None
