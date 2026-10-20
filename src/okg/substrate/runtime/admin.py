"""DBOSClient-backed admin operations for short-lived OKG CLI commands.

Per Decision 10, short-lived OKG CLI commands MUST NOT instantiate the
DBOS singleton. They use :class:`DBOSClient` to enqueue workflows,
trigger schedules, and read workflow events from outside a DBOS context.

This module wraps the DBOSClient surface in helpers that take a
deployment + DSN + runtime config rather than a raw client; the CLI
plumbing in :mod:`okg.substrate.cli.main` calls these.
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
import time
from typing import Any

from okg.substrate.errors import SubstrateError

from .config import RuntimeConfig
from .dbos_enqueue import (
    TerminalDeduplicationCollision,
    enqueue_deployment_workflow,
    latest_registered_app_version,
    registered_app_version,
    workflow_id_for_deduplication,
)
from .partition_queue import (
    deployment_runtime_queue_names,
    partition_queue_name,
    source_sync_enqueue_priority,
    source_sync_queue_name,
)
from .promotion_fence import (
    fenced_recovery_context,
    promotion_enqueue_permit,
    promotion_fence_status,
)
from .workflow_cancel import cancel_deployment_workflow


__all__ = [
    "PUBLISHER_CYCLE_DEQUEUE_TIMEOUT_SECONDS",
    "PublisherCycleNotDequeued",
    "ScheduleEntry",
    "SchedulePauseResult",
    "WorkflowDebugReport",
    "client_debug_workflow",
    "client_enqueue_admission_decide",
    "client_enqueue_admission_submit",
    "client_enqueue_fenced_recovery",
    "client_enqueue_publisher_cycle",
    "client_enqueue_source_sync",
    "client_drain",
    "client_get_event",
    "client_list_schedules",
    "client_pause_schedule",
    "client_resume_schedule",
    "client_trigger_schedule",
]


log = logging.getLogger(__name__)


FENCED_RECOVERY_WORKFLOWS = frozenset({
    "doctor_workflow",
    "okg_ci_git_history_reconcile",
    "okg_ci_resource_monitor",
})


@dataclass(frozen=True)
class ScheduleEntry:
    schedule_id: str
    cron: str
    workflow_name: str
    paused: bool


@dataclass(frozen=True)
class SchedulePauseResult:
    schedule_id: str
    persisted_paused: bool
    scheduler_fenced: bool
    active_scheduled_workflows_at_timeout: int
    elapsed_seconds: float


def _build_client(*, dsn: str):
    """Construct a :class:`DBOSClient` against the deployment's DSN.

    Per Decision 11 both ``system_database_url`` and ``database_url``
    point to the same deployment DSN. Pass them explicitly because
    DBOSClient otherwise derives a separate system DB name by
    appending ``_dbos_sys`` to the application DB, which would point
    at a non-existent database for our co-located topology.
    """
    from dbos import DBOSClient

    return DBOSClient(
        database_url=dsn,
        system_database_url=dsn,
        application_database_url=dsn,
    )


def client_list_schedules(
    *,
    deployment: str,
    dsn: str,
    runtime: RuntimeConfig,
) -> list[ScheduleEntry]:
    """Return the list of scheduled workflows currently registered for
    the deployment's DBOS singleton.

    Prefer DBOS's persisted schedule table via DBOSClient. Fall back to
    the manifest/default projection only when the DBOS admin path is
    unavailable so diagnostics can still report the intended baseline.
    """
    try:
        client = _build_client(dsn=dsn)
        schedules = client.list_schedules()
    except Exception:  # noqa: BLE001
        log.exception(
            "failed to read DBOS schedules; falling back to manifest "
            "projection: deployment=%s",
            deployment,
        )
        return _manifest_schedule_entries(runtime=runtime)

    entries: list[ScheduleEntry] = []
    for raw in schedules:
        schedule_name = _schedule_value(raw, "schedule_name")
        cron = _schedule_value(raw, "schedule")
        workflow_name = _schedule_value(raw, "workflow_name")
        status = (_schedule_value(raw, "status") or "").upper()
        if not schedule_name:
            continue
        entries.append(
            ScheduleEntry(
                schedule_id=schedule_name,
                cron=cron or "",
                workflow_name=workflow_name or "",
                paused=status == "PAUSED",
            )
        )
    entries.sort(key=lambda entry: entry.schedule_id)
    return entries


def _schedule_value(raw: object, key: str) -> str | None:
    if isinstance(raw, dict):
        value = raw.get(key)
    else:
        value = getattr(raw, key, None)
    if value is None:
        return None
    return str(value)


def _manifest_schedule_entries(*, runtime: RuntimeConfig) -> list[ScheduleEntry]:
    from .scheduled import DOCTOR_SCHEDULE_ID, DOCTOR_DEFAULT_CRON

    entries: list[ScheduleEntry] = []
    # Substrate-owned schedules.
    doctor_cron = runtime.schedules.get(
        DOCTOR_SCHEDULE_ID, DOCTOR_DEFAULT_CRON,
    )
    entries.append(
        ScheduleEntry(
            schedule_id=DOCTOR_SCHEDULE_ID,
            cron=doctor_cron,
            workflow_name="okg_substrate_doctor",
            paused=False,
        )
    )
    # Manifest-declared deployment-author overrides for unknown schedule_ids
    # are listed for visibility — they only take effect after Phase 2 ships
    # SDK decorators that register with those schedule_ids.
    for sched_id, cron in runtime.schedules.items():
        if sched_id == DOCTOR_SCHEDULE_ID:
            continue
        entries.append(
            ScheduleEntry(
                schedule_id=sched_id,
                cron=cron,
                workflow_name="<deployment-author SDK; Phase 2>",
                paused=False,
            )
        )
    return entries


def client_trigger_schedule(
    *,
    deployment: str,
    dsn: str,
    runtime: RuntimeConfig,
    schedule_id: str,
) -> str:
    """Trigger a registered schedule once, out-of-cycle.

    Returns the workflow_id of the resulting workflow execution.

    DBOSClient exposes ``trigger_schedule`` for out-of-process callers.
    This path covers substrate-owned, deployment-authored, and
    per-source poll schedules uniformly.
    """
    client = _build_client(dsn=dsn)
    with promotion_enqueue_permit(deployment=deployment, dsn=dsn):
        handle = client.trigger_schedule(schedule_id)
    log.info(
        "triggered schedule %s for deployment=%s workflow_id=%s",
        schedule_id, deployment, handle.workflow_id,
    )
    return handle.workflow_id


def client_enqueue_fenced_recovery(
    *,
    deployment: str,
    dsn: str,
    workflow_name: str,
    promotion_id: str,
    deduplication_id: str | None = None,
) -> str:
    """Enqueue one idempotent recovery witness under the active fence owner."""
    if workflow_name not in FENCED_RECOVERY_WORKFLOWS:
        raise ValueError(
            "fenced recovery workflow must be one of: "
            + ", ".join(sorted(FENCED_RECOVERY_WORKFLOWS))
        )
    fence = promotion_fence_status(deployment=deployment, dsn=dsn)
    if fence is None or fence.get("status") == "released":
        raise RuntimeError("fenced recovery requires an active promotion fence")
    if fence.get("promotion_id") != promotion_id:
        raise RuntimeError("fenced recovery promotion does not own the active fence")
    queue_name = partition_queue_name(deployment)
    if deduplication_id:
        existing = workflow_id_for_deduplication(
            dsn=dsn,
            queue_name=queue_name,
            deduplication_id=deduplication_id,
        )
        if existing is not None:
            return existing
    context = fenced_recovery_context(promotion_id=promotion_id)
    try:
        result = enqueue_deployment_workflow(
            deployment=deployment,
            dsn=dsn,
            workflow_name=workflow_name,
            args=(None, context),
            deduplication_id=deduplication_id,
            promotion_id=promotion_id,
        )
    except TerminalDeduplicationCollision as exc:
        if exc.terminal_workflow_id is None:
            raise
        return exc.terminal_workflow_id
    return result.workflow_id


def client_pause_schedule(
    *,
    deployment: str,
    dsn: str,
    schedule_id: str,
    max_wait_seconds: float = 120.0,
    min_settle_seconds: float = 35.0,
    poll_interval_seconds: float = 1.0,
) -> SchedulePauseResult:
    """Pause one persisted DBOS schedule for a deployment.

    DBOS's pause call does not fail when the name is absent, and the live
    scheduler observes persisted changes on a polling interval.  Verify the
    exact row before and after the mutation, then wait through one scheduler
    poll window with no nonterminal workflow for this schedule.  Only after
    this fence is it safe for a caller to drain the deployment queues.
    """
    if max_wait_seconds <= 0:
        raise ValueError("max_wait_seconds must be positive")
    if min_settle_seconds < 0 or min_settle_seconds >= max_wait_seconds:
        raise ValueError(
            "min_settle_seconds must be non-negative and below max_wait_seconds"
        )
    if poll_interval_seconds <= 0:
        raise ValueError("poll_interval_seconds must be positive")

    client = _build_client(dsn=dsn)
    existing = client.get_schedule(schedule_id)
    if existing is None:
        raise RuntimeError(
            f"cannot pause missing DBOS schedule {schedule_id!r} "
            f"for deployment {deployment!r}"
        )
    if _schedule_value(existing, "schedule_name") != schedule_id:
        raise RuntimeError(
            f"DBOS schedule lookup returned the wrong row for {schedule_id!r}"
        )
    client.pause_schedule(schedule_id)
    persisted = client.get_schedule(schedule_id)
    persisted_status = (
        _schedule_value(persisted, "status") or ""
    ).upper()
    if persisted is None or persisted_status != "PAUSED":
        raise RuntimeError(
            f"DBOS schedule {schedule_id!r} did not persist PAUSED state"
        )

    started = time.monotonic()
    deadline = started + max_wait_seconds
    settle_deadline = started + min_settle_seconds
    last_active_count = 0
    while True:
        active = client.list_workflows(
            schedule_name=schedule_id,
            status=["PENDING", "ENQUEUED", "DELAYED"],
            limit=100,
            load_input=False,
            load_output=False,
        )
        last_active_count = len(active)
        now = time.monotonic()
        if now >= settle_deadline and last_active_count == 0:
            final = client.get_schedule(schedule_id)
            final_status = (_schedule_value(final, "status") or "").upper()
            if final is None or final_status != "PAUSED":
                raise RuntimeError(
                    f"DBOS schedule {schedule_id!r} left PAUSED state "
                    "while fencing its scheduler"
                )
            elapsed = now - started
            log.info(
                "paused and fenced schedule %s for deployment=%s "
                "elapsed_seconds=%.3f",
                schedule_id,
                deployment,
                elapsed,
            )
            return SchedulePauseResult(
                schedule_id=schedule_id,
                persisted_paused=True,
                scheduler_fenced=True,
                active_scheduled_workflows_at_timeout=0,
                elapsed_seconds=elapsed,
            )
        if now >= deadline:
            raise TimeoutError(
                f"DBOS schedule {schedule_id!r} did not reach a fenced "
                f"PAUSED state within {max_wait_seconds:g}s; "
                f"active_scheduled_workflows={last_active_count}"
            )
        time.sleep(min(poll_interval_seconds, deadline - now))


def client_resume_schedule(
    *,
    deployment: str,
    dsn: str,
    schedule_id: str,
) -> None:
    """Resume one persisted DBOS schedule and verify the active state."""
    client = _build_client(dsn=dsn)
    existing = client.get_schedule(schedule_id)
    if existing is None:
        raise RuntimeError(
            f"cannot resume missing DBOS schedule {schedule_id!r} "
            f"for deployment {deployment!r}"
        )
    client.resume_schedule(schedule_id)
    resumed = client.get_schedule(schedule_id)
    resumed_status = (_schedule_value(resumed, "status") or "").upper()
    if resumed is None or resumed_status != "ACTIVE":
        raise RuntimeError(
            f"DBOS schedule {schedule_id!r} did not persist ACTIVE state"
        )
    log.info(
        "resumed schedule %s for deployment=%s",
        schedule_id,
        deployment,
    )


def client_get_event(
    *,
    dsn: str,
    workflow_id: str,
    key: str,
    timeout_seconds: float = 10.0,
) -> Any:
    """Read a workflow event by (workflow_id, key).

    Wraps :meth:`DBOSClient.get_event`. Returns None if no value has
    been published for the key within ``timeout_seconds``.

    The client is shared per process and DSN (issue #2547): the console reads
    doctor verdicts on every Health and Producers request, and a client per
    read built a new connection pool each time and never closed it. A read
    that raises drops the shared client, so the next read builds a new one,
    and the error still reaches the caller.
    """
    client_key, client = _shared_event_client(dsn=dsn)
    try:
        return client.get_event(
            workflow_id=workflow_id,
            key=key,
            timeout_seconds=timeout_seconds,
        )
    except Exception:
        _drop_shared_event_client(client_key, client)
        raise


#: DBOS clients shared by `client_get_event`, keyed by DSN and by the builder
#: that made them, so a builder swapped in (by a test double) never receives a
#: client another builder made.
_EVENT_CLIENTS: dict[tuple[str, Any], Any] = {}
_EVENT_CLIENTS_LOCK = threading.Lock()


def _shared_event_client(*, dsn: str) -> tuple[tuple[str, Any], Any]:
    builder = _build_client
    client_key = (dsn, builder)
    with _EVENT_CLIENTS_LOCK:
        client = _EVENT_CLIENTS.get(client_key)
    if client is not None:
        return client_key, client
    # Built OUTSIDE the lock: construction connects to the database, and one
    # hung connect must not stall every other thread's read on every DSN.
    built = builder(dsn=dsn)
    with _EVENT_CLIENTS_LOCK:
        client = _EVENT_CLIENTS.setdefault(client_key, built)
    if client is not built:
        # Another thread built one first; keep theirs and close this one.
        _destroy_event_client(built)
    return client_key, client


def _drop_shared_event_client(client_key: tuple[str, Any], client: Any) -> None:
    with _EVENT_CLIENTS_LOCK:
        if _EVENT_CLIENTS.get(client_key) is client:
            del _EVENT_CLIENTS[client_key]
        else:
            return
    _destroy_event_client(client)


def _destroy_event_client(client: Any) -> None:
    destroy = getattr(client, "destroy", None)
    if callable(destroy):
        try:
            destroy()
        except Exception:  # noqa: BLE001 - the caller's own outcome stands.
            log.warning("closing a DBOS event client raised", exc_info=True)


# ---------------------------------------------------------------------------
# debug-workflow — operator-facing introspection
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkflowDebugReport:
    workflow_id: str
    name: str
    status: str
    started_at: Any
    completed_at: Any
    recovery_attempts: int
    queue_name: str | None
    steps: list[dict]
    created_at: Any = None
    updated_at: Any = None
    inputs: str | None = None
    serialization: str | None = None
    deduplication_id: str | None = None
    executor_id: str | None = None
    application_version: str | None = None
    output: Any = None
    error: str | None = None


def client_debug_workflow(
    *,
    dsn: str,
    workflow_id: str,
) -> WorkflowDebugReport:
    """Return a structured debug report for a workflow.

    Operators call this when a workflow appears stuck or is
    unexpectedly slow. The report consolidates the workflow's status,
    its steps (with start/complete timestamps and per-step latencies),
    and its recovery-attempt count — the data DBOS has but does not
    surface in a single CLI invocation by default.

    Phase 1 §2.4 / Item 5 of the principal-engineer review's
    operational-affordances list. Wraps ``DBOSClient.list_workflows``
    and ``DBOSClient.list_workflow_steps``.
    """
    client = _build_client(dsn=dsn)
    try:
        return _client_debug_workflow(
            client=client,
            dsn=dsn,
            workflow_id=workflow_id,
        )
    finally:
        # DBOSClient owns database resources that a short-lived CLI process
        # must release explicitly. Without this, `okg runtime debug-workflow`
        # can print its report but keep the child process alive, leaving
        # deployment-up's source-convergence poll blocked in communicate().
        client.destroy()


def _client_debug_workflow(
    *,
    client: Any,
    dsn: str,
    workflow_id: str,
) -> WorkflowDebugReport:
    workflows = client.list_workflows(workflow_ids=[workflow_id])
    if not workflows:
        return WorkflowDebugReport(
            workflow_id=workflow_id,
            name="<not found>",
            status="<not found>",
            started_at=None,
            completed_at=None,
            recovery_attempts=0,
            queue_name=None,
            steps=[],
            error=f"no workflow with id {workflow_id!r}",
        )

    wf = workflows[0]
    try:
        step_records = client.list_workflow_steps(
            workflow_id=workflow_id,
        )
    except Exception as e:
        log.warning(
            "list_workflow_steps failed for %s: %s",
            workflow_id, e,
        )
        step_records = []

    steps: list[dict] = []
    for s in step_records:
        steps.append(
            {
                "step_id": getattr(s, "step_id", None),
                "name": getattr(s, "name", None),
                "started_at": getattr(s, "started_at", None),
                "completed_at": getattr(s, "completed_at", None),
                "output_present": (
                    getattr(s, "output", None) is not None
                ),
                "error": getattr(s, "error", None),
            }
        )

    started_at = getattr(wf, "started_at", None)
    completed_at = getattr(wf, "completed_at", None)
    created_at = getattr(wf, "created_at", None)
    updated_at = getattr(wf, "updated_at", None)
    inputs = getattr(wf, "inputs", None)
    serialization = getattr(wf, "serialization", None)
    deduplication_id = getattr(wf, "deduplication_id", None)
    executor_id = getattr(wf, "executor_id", None)
    application_version = getattr(wf, "application_version", None)
    queue_name = getattr(wf, "queue_name", None)
    output = _jsonish(getattr(wf, "output", None))
    # `restore-publisher-perf-and-profiling` §C polish: DBOSClient's
    # workflow object doesn't always populate `started_at` /
    # `completed_at` (varies by DBOS version + workflow state). Fall
    # back to a direct read of `dbos.workflow_status.created_at` /
    # `updated_at` so the debug-workflow CLI can always compute
    # lifetime + queue_wait derivations.
    if (
        started_at is None
        or completed_at is None
        or inputs is None
        or serialization is None
        or deduplication_id is None
        or executor_id is None
        or application_version is None
    ):
        try:
            from datetime import datetime, timezone
            row = _workflow_status_row(dsn=dsn, workflow_id=workflow_id)
            if row is not None:
                created_ms = row[0] if len(row) > 0 else None
                updated_ms = row[1] if len(row) > 1 else None
                db_status = row[2] if len(row) > 2 else None
                if len(row) > 3 and inputs is None:
                    inputs = row[3]
                if len(row) > 4 and serialization is None:
                    serialization = row[4]
                if len(row) > 5 and deduplication_id is None:
                    deduplication_id = row[5]
                if len(row) > 6 and executor_id is None:
                    executor_id = row[6]
                if len(row) > 7 and application_version is None:
                    application_version = row[7]
                if len(row) > 8 and queue_name is None:
                    queue_name = row[8]
                if started_at is None and created_ms is not None:
                    started_at = datetime.fromtimestamp(
                        created_ms / 1000.0, tz=timezone.utc,
                    ).isoformat()
                if created_at is None and created_ms is not None:
                    created_at = datetime.fromtimestamp(
                        created_ms / 1000.0, tz=timezone.utc,
                    ).isoformat()
                if updated_at is None and updated_ms is not None:
                    updated_at = datetime.fromtimestamp(
                        updated_ms / 1000.0, tz=timezone.utc,
                    ).isoformat()
                # Only fill completed_at if the workflow has
                # actually terminated. PENDING/ENQUEUED don't have
                # a meaningful "completion" yet.
                if (
                    completed_at is None
                    and updated_ms is not None
                    and db_status in ("SUCCESS", "ERROR", "CANCELLED")
                ):
                    completed_at = datetime.fromtimestamp(
                        updated_ms / 1000.0, tz=timezone.utc,
                    ).isoformat()
        except Exception:  # noqa: BLE001
            # Best-effort fallback; if it fails the caller still
            # gets the original DBOSClient-provided values.
            pass

    return WorkflowDebugReport(
        workflow_id=workflow_id,
        name=getattr(wf, "name", "<unknown>"),
        status=getattr(wf, "status", "<unknown>"),
        started_at=started_at,
        completed_at=completed_at,
        recovery_attempts=getattr(wf, "recovery_attempts", 0),
        queue_name=queue_name,
        steps=steps,
        created_at=created_at,
        updated_at=updated_at,
        inputs=inputs,
        serialization=serialization,
        deduplication_id=deduplication_id,
        executor_id=executor_id,
        application_version=application_version,
        output=output,
    )


def _jsonish(value: Any) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, dict):
        return {str(k): _jsonish(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonish(v) for v in value]
    return str(value)


def _workflow_status_row(*, dsn: str, workflow_id: str) -> tuple | None:
    from okg.substrate.db.connection import connect as db_connect

    queries = (
        (
            "SELECT created_at, updated_at, status, inputs, "
            "       serialization, deduplication_id, executor_id, "
            "       application_version, queue_name "
            "  FROM dbos.workflow_status "
            " WHERE workflow_uuid = %s"
        ),
        (
            "SELECT created_at, updated_at, status "
            "  FROM dbos.workflow_status "
            " WHERE workflow_uuid = %s"
        ),
    )
    last_error: Exception | None = None
    for sql in queries:
        try:
            with db_connect(dsn) as conn, conn.cursor() as cur:
                cur.execute(sql, (workflow_id,))
                return cur.fetchone()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            continue
    if last_error is not None:
        raise last_error
    return None


# ---------------------------------------------------------------------------
# enqueue_publisher_cycle — `okg run --once` synchronous-via-DBOS path
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EnqueuePublisherResult:
    """Outcome of ``client_enqueue_publisher_cycle``."""

    workflow_id: str
    status: str            # "SUCCESS" | "ERROR" | "PENDING" (on timeout)
    summary: dict | None   # workflow output when status == SUCCESS
    error: str | None      # workflow error_message when status == ERROR
    elapsed_seconds: float


#: How long ``client_enqueue_publisher_cycle`` lets its row sit ``ENQUEUED``
#: on an idle queue before deciding no worker is consuming the queue. A
#: running worker polls its queues about once a second, so a row it can
#: take leaves ``ENQUEUED`` well inside this window (issue #2502).
PUBLISHER_CYCLE_DEQUEUE_TIMEOUT_SECONDS = 30.0


class PublisherCycleNotDequeued(SubstrateError):
    """No worker took the enqueued publisher cycle within the dequeue window.

    Raised by ``client_enqueue_publisher_cycle`` after it has cancelled its
    own still-``ENQUEUED`` row, so the refusal leaves nothing queued behind.
    """

    code = "publisher_cycle_not_dequeued"


def client_enqueue_publisher_cycle(
    *,
    deployment: str,
    dsn: str,
    branch_name: str = "default",
    force_enrichers: bool = False,
    timeout_seconds: float = 600.0,
    poll_interval_seconds: float = 2.0,
    promotion_id: str | None = None,
    registration_wait_seconds: float = 0.0,
    dequeue_timeout_seconds: float | None = None,
) -> EnqueuePublisherResult:
    """Enqueue a publisher cycle workflow and wait for terminal state.

    Used by ``okg run --once`` when the deployment has
    ``runtime.enabled: true``. The enqueue lands on the deployment's
    partition queue (``okg-<deployment>``, concurrency=1); a concurrent
    enqueue from another operator or from the scheduled cron serializes
    naturally — the second cycle starts after the first completes.

    Polling shape mirrors ``client_drain``: poll workflow status every
    ``poll_interval_seconds`` until terminal or the timeout fires. On
    timeout, returns ``status='PENDING'`` so the caller can decide to
    abandon or keep watching via ``okg runtime debug-workflow``.

    Refuses, before enqueuing anything, when the deployment's database
    has never had a DBOS application version registered. No worker has
    ever booted against that database, so nothing would claim the row;
    without the refusal this call queues durable work and then polls it
    to its own timeout and reports ``PENDING``, which reads as "still
    running" rather than "nothing will run this".

    This is a has-ever-run check, not a liveness check. A stopped worker
    is caught by the dequeue window described below, not here.

    ``registration_wait_seconds`` polls for that first registration
    instead of refusing immediately. Default 0 -- an operator running
    this by hand should be told to start the worker, not made to wait.
    A caller that has just started the worker itself (the promotion
    publish step) passes a bounded window so a first-ever boot that has
    not yet registered is not mistaken for one that never will.

    The enqueued row is also pinned to that registered version, so it
    follows the same dispatch and restamping rules as scheduled work
    rather than staying unpinned.

    The has-ever-run check passes for any database a worker once booted
    against, including one whose worker is stopped now. So that case does
    not poll to the full timeout (issue #2502), a caller may pass
    ``dequeue_timeout_seconds``: the row then gets that long to leave
    ``ENQUEUED``. The default ``None`` sets no window, because a caller
    that has just restarted the worker (promotion) must not fail on a
    slow boot; ``okg run --once`` passes
    ``PUBLISHER_CYCLE_DEQUEUE_TIMEOUT_SECONDS``. The window only
    runs while the queue is idle: when another workflow on the
    deployment's partition queue is ``PENDING``, the row is waiting its
    turn behind it, and the window restarts. If the window passes with
    the row still ``ENQUEUED`` and nothing ahead of it, this call cancels
    its own row and raises :class:`PublisherCycleNotDequeued`. It never
    falls back to running the cycle in-process. Once a worker takes the
    row, the overall ``timeout_seconds`` wait applies unchanged.

    Raises:
        RuntimeError: no DBOS application version has ever been
            registered for this deployment's database.
        PublisherCycleNotDequeued: no worker took the row within the
            dequeue window; the row has been cancelled.
        Exception: connection failures reading the registered version
            propagate unchanged, so an unreachable database is never
            reported as a missing worker.
    """
    import time

    from okg.substrate.runtime.publisher_workflow import (
        PUBLISHER_WORKFLOW_NAME,
    )

    if registration_wait_seconds < 0:
        raise ValueError("registration_wait_seconds must not be negative")
    if dequeue_timeout_seconds is not None and dequeue_timeout_seconds < 0:
        raise ValueError("dequeue_timeout_seconds must not be negative")

    app_version = registered_app_version(dsn)
    if app_version is None and registration_wait_seconds > 0:
        deadline = time.monotonic() + registration_wait_seconds
        while app_version is None and time.monotonic() < deadline:
            time.sleep(min(1.0, max(poll_interval_seconds, 0.1)))
            app_version = registered_app_version(dsn)
    if app_version is None:
        raise RuntimeError(
            f"deployment {deployment!r} has no registered DBOS "
            "application version, so no worker has ever booted against "
            "its database; refusing to enqueue "
            f"{PUBLISHER_WORKFLOW_NAME} because nothing would claim the "
            "row. Start the deployment's worker and retry."
        )

    # Route through the shared helper rather than a raw DBOSClient. The
    # version just read strictly is handed to it, so the row is pinned
    # to the version this call actually checked instead of to a second,
    # best-effort read that can come back empty without saying so. The
    # helper holds the promotion enqueue permit itself.
    enqueued = enqueue_deployment_workflow(
        deployment=deployment,
        dsn=dsn,
        workflow_name=PUBLISHER_WORKFLOW_NAME,
        args=(branch_name, force_enrichers),
        promotion_id=promotion_id,
        app_version=app_version,
    )
    workflow_id = enqueued.workflow_id
    client = _build_client(dsn=dsn)
    log.info(
        "enqueued publisher cycle: deployment=%s branch=%s workflow_id=%s",
        deployment, branch_name, workflow_id,
    )

    queue_name = partition_queue_name(deployment)
    started = time.time()
    # No window (None) behaves as if the row were already dequeued: only
    # the overall timeout applies, exactly as before #2502.
    dequeued = dequeue_timeout_seconds is None
    window = dequeue_timeout_seconds if dequeue_timeout_seconds is not None else 0.0
    dequeue_deadline = time.monotonic() + window
    while True:
        elapsed = time.time() - started
        try:
            workflows = client.list_workflows(
                workflow_ids=[workflow_id],
            )
        except Exception as e:  # noqa: BLE001 — defensive
            log.warning(
                "list_workflows poll failed for %s: %s",
                workflow_id, e,
            )
            workflows = []

        if workflows:
            wf = workflows[0]
            status = getattr(wf, "status", "PENDING") or "PENDING"
            if status != "ENQUEUED":
                dequeued = True
            elif not dequeued and time.monotonic() >= dequeue_deadline:
                if _publisher_queue_busy(
                    client, queue_name=queue_name, workflow_id=workflow_id,
                ):
                    # Waiting behind a running cycle is not a missing
                    # worker; give the row a fresh window once it is next.
                    dequeue_deadline = time.monotonic() + window
                elif _cancel_undequeued_publisher_cycle(
                    dsn=dsn, deployment=deployment, workflow_id=workflow_id,
                ):
                    raise PublisherCycleNotDequeued(
                        f"no worker took publisher cycle {workflow_id!r} "
                        f"off queue {queue_name!r} within "
                        f"{window:g}s, so no worker is "
                        f"consuming deployment {deployment!r}'s queue; "
                        "the enqueued cycle was cancelled before it "
                        "started and nothing was published",
                        data={
                            "deployment": deployment,
                            "workflow_id": workflow_id,
                            "queue_name": queue_name,
                            "dequeue_timeout_seconds": window,
                            "cancelled": True,
                        },
                        fix_hint=(
                            "start the deployment's worker with "
                            f"`okg runtime worker --deployment {deployment}` "
                            "and retry, or rerun with --inline to publish "
                            "in this process"
                        ),
                    )
                else:
                    # A worker took the row between the poll and the
                    # cancel; from here the overall wait applies.
                    dequeued = True
            if status in ("SUCCESS", "ERROR", "CANCELLED",
                          "MAX_RECOVERY_ATTEMPTS_EXCEEDED"):
                summary = None
                # dbos.WorkflowStatus uses .error (a stringified
                # Exception), not .error_message.
                err = getattr(wf, "error", None)
                error_str = str(err) if err else None
                if status == "SUCCESS":
                    summary = getattr(wf, "output", None)
                return EnqueuePublisherResult(
                    workflow_id=workflow_id,
                    status=status,
                    summary=(
                        summary if isinstance(summary, dict) else None
                    ),
                    error=error_str,
                    elapsed_seconds=elapsed,
                )

        if elapsed > timeout_seconds:
            return EnqueuePublisherResult(
                workflow_id=workflow_id,
                status="PENDING",
                summary=None,
                error=None,
                elapsed_seconds=elapsed,
            )

        time.sleep(poll_interval_seconds)


def _publisher_queue_busy(
    client: Any, *, queue_name: str, workflow_id: str,
) -> bool:
    """Whether another workflow is running on the publisher's queue.

    A partition queue runs one workflow at a time, so a row behind a
    ``PENDING`` one is waiting its turn rather than stranded. A failed
    read counts as busy: the refusal cancels durable work, so it is
    taken only on a positive reading that the queue is idle.
    """
    try:
        running = client.list_workflows(
            queue_name=queue_name,
            status="PENDING",
            load_input=False,
            load_output=False,
        )
    except Exception as e:  # noqa: BLE001 — never cancel on an unread queue
        log.warning(
            "could not read queue %s while waiting on %s: %s",
            queue_name, workflow_id, e,
        )
        return True
    return any(
        getattr(wf, "workflow_id", None) != workflow_id for wf in running
    )


def _cancel_undequeued_publisher_cycle(
    *, dsn: str, deployment: str, workflow_id: str,
) -> bool:
    """Cancel the caller's own row only while it is still ``ENQUEUED``.

    The cancel is restricted to ``ENQUEUED``: a row a worker has already
    started (``PENDING``) is never cancelled, because the publisher cycle
    has no DBOS steps to stop at and would keep running, and the freed
    queue slot would let a second cycle run beside it. Returns False when
    the row has left ``ENQUEUED`` before the read or between the read and
    the write; nothing is written then. Any other failure propagates, so
    a cancellation that did not happen is never reported as one.
    """
    try:
        cancel_deployment_workflow(
            dsn=dsn,
            deployment=deployment,
            workflow_id=workflow_id,
            expected_status="ENQUEUED",
        )
    except SubstrateError as e:
        if e.code == "workflow_status_changed":
            return False
        raise
    return True


# ---------------------------------------------------------------------------
# enqueue_source_sync — `okg ingest` synchronous-via-DBOS path
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EnqueueSourceSyncResult:
    """Outcome of ``client_enqueue_source_sync``."""

    workflow_id: str
    status: str
    report: dict | None
    any_failed: bool
    error: str | None
    elapsed_seconds: float
    error_code: str | None = None
    error_data: dict | None = None
    error_fix_hint: str | None = None


@dataclass(frozen=True)
class EnqueueAdmissionResult:
    """Outcome of an admission submit/decide enqueue."""

    workflow_id: str
    status: str
    output: dict | None
    error: str | None
    elapsed_seconds: float


def client_enqueue_source_sync(
    *,
    deployment: str,
    dsn: str,
    sources: list[str] | None = None,
    run_mode: str = "scope_complete",
    include_predecessors: bool = True,
    include_dependents: bool = True,
    dependent_fanout: str | None = None,
    events: list[dict] | None = None,
    publish: bool = True,
    profile_force_deep: bool = False,
    profile_collectors: list[str] | tuple[str, ...] = (),
    force_enrichers: bool = False,
    run_mode_explicit: bool = False,
    wait_for_result: bool = True,
    timeout_seconds: float = 3600.0,
    poll_interval_seconds: float = 2.0,
    promotion_id: str | None = None,
    sync_wait_seconds: float | None = None,
) -> EnqueueSourceSyncResult:
    """Enqueue the source-sync workflow and optionally wait for terminal state.

    Used by ``okg ingest`` when the deployment has ``runtime.enabled:
    true``. Mirrors ``client_enqueue_publisher_cycle``'s shape: enqueue
    onto the source-sync lane. Source-sync is intentionally separate from
    the control/publisher lane so bursty wakeups cannot starve recovery
    work; publisher-level database locking serializes publish phases across
    both lanes. Poll ``dbos.workflow_status`` until terminal, return the
    workflow's output.

    Long default timeout (1h) because real ingest cycles process
    substantial work. Operators can set ``wait_for_result=False`` to
    submit durable work and follow it through ``runtime debug-workflow``.
    """
    import time

    from okg.substrate.runtime.source_sync_workflow import (
        SOURCE_SYNC_WORKFLOW_NAME,
    )

    options: dict[str, Any] = {
        "queue_name": source_sync_queue_name(deployment),
        "workflow_name": SOURCE_SYNC_WORKFLOW_NAME,
        "priority": source_sync_enqueue_priority(
            operator_requested=True,
            profile_force_deep=profile_force_deep,
            profile_collectors=list(profile_collectors),
        ),
    }
    app_version = latest_registered_app_version(dsn)
    if app_version is not None:
        options["app_version"] = app_version

    from okg.substrate.runtime.source_sync_priority import (
        source_sync_admission_intent_lock,
        source_sync_run_wait_seconds,
    )

    client = _build_client(dsn=dsn)
    # #3003: take the deployment's source-sync signal, as the runtime
    # producers do, so this enqueue waits (bounded, then a named error)
    # while a command-line run holds it instead of starting a worker sync
    # beside that run.
    with source_sync_admission_intent_lock(
        dsn=dsn,
        deployment=deployment,
        wait_seconds=source_sync_run_wait_seconds(sync_wait_seconds),
    ), promotion_enqueue_permit(
        deployment=deployment, dsn=dsn, promotion_id=promotion_id
    ):
        handle = client.enqueue(
            options,
            list(sources) if sources else None,
            run_mode,
            include_dependents,
            dependent_fanout,
            list(events) if events else None,
            publish,
            include_predecessors,
            profile_force_deep,
            list(profile_collectors),
            force_enrichers,
            # Keyword, and only when set: a typed `--mode` must reach the
            # runner as typed (issue #2486). Leaving it out otherwise keeps
            # the enqueued input identical to a workflow that predates it.
            **({"run_mode_explicit": True} if run_mode_explicit else {}),
        )
    workflow_id = handle.workflow_id
    log.info(
        "enqueued source-sync workflow: deployment=%s workflow_id=%s "
        "sources=%s run_mode=%s",
        deployment, workflow_id,
        sources if sources else "<all>", run_mode,
    )

    if not wait_for_result:
        return EnqueueSourceSyncResult(
            workflow_id=workflow_id,
            status="ENQUEUED",
            report=None,
            any_failed=False,
            error=None,
            elapsed_seconds=0.0,
        )

    started = time.time()
    while True:
        elapsed = time.time() - started
        try:
            workflows = client.list_workflows(
                workflow_ids=[workflow_id],
            )
        except Exception as e:  # noqa: BLE001
            log.warning(
                "list_workflows poll failed for %s: %s",
                workflow_id, e,
            )
            workflows = []

        if workflows:
            wf = workflows[0]
            status = getattr(wf, "status", "PENDING") or "PENDING"
            if status in ("SUCCESS", "ERROR", "CANCELLED",
                          "MAX_RECOVERY_ATTEMPTS_EXCEEDED"):
                output = getattr(wf, "output", None)
                report = (
                    output.get("report")
                    if isinstance(output, dict) else None
                )
                any_failed = bool(
                    output.get("any_failed")
                    if isinstance(output, dict) else status != "SUCCESS"
                )
                err = getattr(wf, "error", None)
                from okg.substrate.errors import SubstrateError

                typed_error = err if isinstance(err, SubstrateError) else None
                return EnqueueSourceSyncResult(
                    workflow_id=workflow_id,
                    status=status,
                    report=report,
                    any_failed=any_failed,
                    error=str(err) if err else None,
                    elapsed_seconds=elapsed,
                    error_code=(typed_error.code if typed_error else None),
                    error_data=(typed_error.data if typed_error else None),
                    error_fix_hint=(
                        typed_error.fix_hint if typed_error else None
                    ),
                )

        if elapsed > timeout_seconds:
            return EnqueueSourceSyncResult(
                workflow_id=workflow_id,
                status="PENDING",
                report=None,
                any_failed=False,
                error=None,
                elapsed_seconds=elapsed,
            )

        time.sleep(poll_interval_seconds)


# ---------------------------------------------------------------------------
# enqueue_admission_* — review-mode admission DBOS paths
# ---------------------------------------------------------------------------


def client_enqueue_admission_submit(
    *,
    deployment: str,
    dsn: str,
    payload: dict,
    producer_id: str,
    producer_kind: str = "agent",
    intent: str = "review_proposal",
    idempotency_key: str | None = None,
    authority: dict | None = None,
    check_summary: dict | None = None,
    timeout_seconds: float = 600.0,
    poll_interval_seconds: float = 2.0,
    promotion_id: str | None = None,
) -> EnqueueAdmissionResult:
    """Enqueue ``okg_admission_submit`` and wait for terminal state."""
    from okg.substrate.runtime.config import validate_admission_intent

    intent = validate_admission_intent(intent)
    from okg.substrate.runtime.admission_workflows import (
        ADMISSION_SUBMIT_WORKFLOW_NAME,
    )

    client = _build_client(dsn=dsn)
    with promotion_enqueue_permit(
        deployment=deployment, dsn=dsn, promotion_id=promotion_id
    ):
        handle = client.enqueue(
            {
                "queue_name": f"okg-{deployment}",
                "workflow_name": ADMISSION_SUBMIT_WORKFLOW_NAME,
            },
            dict(payload),
            producer_id,
            producer_kind,
            intent,
            idempotency_key,
            dict(authority) if authority else None,
            dict(check_summary) if check_summary else None,
        )
    log.info(
        "enqueued admission submit: deployment=%s workflow_id=%s "
        "producer_id=%s",
        deployment, handle.workflow_id, producer_id,
    )
    return _poll_admission_workflow(
        client=client,
        workflow_id=handle.workflow_id,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )


def client_enqueue_admission_decide(
    *,
    deployment: str,
    dsn: str,
    admission_id: str,
    decision: str,
    actor: str = "operator",
    reason: str | None = None,
    verdict: dict | None = None,
    timeout_seconds: float = 600.0,
    poll_interval_seconds: float = 2.0,
    promotion_id: str | None = None,
) -> EnqueueAdmissionResult:
    """Enqueue ``okg_admission_decide`` and wait for terminal state."""
    from okg.substrate.runtime.admission_workflows import (
        ADMISSION_DECIDE_WORKFLOW_NAME,
    )

    client = _build_client(dsn=dsn)
    with promotion_enqueue_permit(
        deployment=deployment, dsn=dsn, promotion_id=promotion_id
    ):
        handle = client.enqueue(
            {
                "queue_name": f"okg-{deployment}",
                "workflow_name": ADMISSION_DECIDE_WORKFLOW_NAME,
            },
            admission_id,
            decision,
            actor,
            reason,
            dict(verdict) if verdict else None,
        )
    log.info(
        "enqueued admission decision: deployment=%s workflow_id=%s "
        "admission_id=%s decision=%s",
        deployment, handle.workflow_id, admission_id, decision,
    )
    return _poll_admission_workflow(
        client=client,
        workflow_id=handle.workflow_id,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )


def _poll_admission_workflow(
    *,
    client,
    workflow_id: str,
    timeout_seconds: float,
    poll_interval_seconds: float,
) -> EnqueueAdmissionResult:
    import time

    started = time.time()
    while True:
        elapsed = time.time() - started
        try:
            workflows = client.list_workflows(
                workflow_ids=[workflow_id],
            )
        except Exception as e:  # noqa: BLE001
            log.warning(
                "list_workflows poll failed for %s: %s",
                workflow_id, e,
            )
            workflows = []

        if workflows:
            wf = workflows[0]
            status = getattr(wf, "status", "PENDING") or "PENDING"
            if status in ("SUCCESS", "ERROR", "CANCELLED",
                          "MAX_RECOVERY_ATTEMPTS_EXCEEDED"):
                output = getattr(wf, "output", None)
                err = getattr(wf, "error", None)
                return EnqueueAdmissionResult(
                    workflow_id=workflow_id,
                    status=status,
                    output=output if isinstance(output, dict) else None,
                    error=str(err) if err else None,
                    elapsed_seconds=elapsed,
                )

        if elapsed > timeout_seconds:
            return EnqueueAdmissionResult(
                workflow_id=workflow_id,
                status="PENDING",
                output=None,
                error=None,
                elapsed_seconds=elapsed,
            )

        time.sleep(poll_interval_seconds)


# ---------------------------------------------------------------------------
# drain — wait for in-flight workflows on a deployment's queue to finish
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DrainResult:
    deployment: str
    drained: bool
    in_flight_at_timeout: int
    elapsed_seconds: float
    pending_workflows: list[dict[str, Any]]


def _summarize_queued_workflow(raw: Any) -> dict[str, Any]:
    """Best-effort, JSON-safe summary of a DBOS queued workflow row."""
    if isinstance(raw, dict):
        getter = raw.get
    else:
        def getter(key, default=None):
            return getattr(raw, key, default)
    return {
        key: getter(key)
        for key in (
            "workflow_id",
            "name",
            "status",
            "queue_name",
            "created_at",
            "started_at",
            "updated_at",
            "recovery_attempts",
            "deduplication_id",
        )
        if getter(key) is not None
    }


def client_drain(
    *,
    deployment: str,
    dsn: str,
    max_wait_seconds: float = 600.0,
    poll_interval_seconds: float = 2.0,
    stable_zero_seconds: float,
) -> DrainResult:
    """Wait until in-flight workflows for ``deployment`` finish, or
    timeout.

    Polls ``DBOSClient.list_queued_workflows`` for every substrate-owned
    deployment queue. Returns success when all queues are empty; returns
    drained=False with the count of in-flight workflows when the timeout
    fires.

    Operators run this before restarting the worker process or
    deploying new code. Per `sdk-design.md` §4.11 / Item 5 of the
    principal-engineer review.

    Phase 1 limitation: this drain *waits* for workflows to finish,
    but it does NOT refuse new workflows from being enqueued
    concurrently. Refusing new enqueues requires the SDK's
    workflow-version pinning (Phase 2). In Phase 1, operators should
    pause upstream sources before invoking drain.

    ``stable_zero_seconds`` has no default on purpose. Queues can read
    empty for an instant while a workflow transitions between states, so
    a zero window turns this into a single sample and a caller that
    forgets the argument silently gets a fail-open drain. Every caller
    must state its window: pass the settle window when this gates a
    mutation, or an explicit ``0.0`` when an instantaneous observation is
    what is actually wanted.
    """
    import time

    client = _build_client(dsn=dsn)
    # DBOS schedules whose declared queue is None execute through the internal
    # queue. Fence acquisition removes their schedule rows, but the predecessor
    # scheduler may need one polling interval to stop its in-memory threads.
    queue_names = (
        *deployment_runtime_queue_names(deployment),
        "_dbos_internal_queue",
    )

    t0 = time.monotonic()
    deadline = t0 + max_wait_seconds
    zero_since: float | None = None

    def _pending_workflows() -> list[Any]:
        # Every queue read must succeed. `list_queued_workflows` is a filtered
        # SELECT over the workflow table, so a queue with no workflows returns
        # [] rather than raising — there is no "queue does not exist" case to
        # tolerate. Any exception is a real fault (auth, network, schema
        # drift), and treating it as an empty queue would report drained=True
        # with work still in flight. Since this drain gates deployment
        # mutation, that is the exact failure it exists to prevent.
        pending: list[Any] = []
        for queue_name in queue_names:
            pending.extend(
                client.list_queued_workflows(queue_name=queue_name),
            )
        return pending

    while True:
        in_flight = _pending_workflows()

        if not in_flight:
            now = time.monotonic()
            if zero_since is None:
                zero_since = now
            if now - zero_since >= stable_zero_seconds:
                return DrainResult(
                    deployment=deployment,
                    drained=True,
                    in_flight_at_timeout=0,
                    elapsed_seconds=now - t0,
                    pending_workflows=[],
                )
        else:
            zero_since = None

        if time.monotonic() >= deadline:
            break

        time.sleep(poll_interval_seconds)

    # Timed out with workflows still in flight.
    final_in_flight_rows = _pending_workflows()
    return DrainResult(
        deployment=deployment,
        drained=False,
        in_flight_at_timeout=len(final_in_flight_rows),
        elapsed_seconds=time.monotonic() - t0,
        pending_workflows=[
            _summarize_queued_workflow(row)
            for row in final_in_flight_rows[:10]
        ],
    )
