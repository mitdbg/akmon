"""In-worker supervisor for source-sync trigger producers.

Per ``consolidate-deployment-runtime``. The DBOS runtime worker
process is the single supervisor for everything a deployment needs
to be source-fresh: DBOS workflows + the filesystem Watcher (for
fs_event triggers) + the webhook HTTP server (for webhook triggers).

This module owns:

  * ``start_supervised_services(deployment, dsn, runtime_cfg)`` —
    called by the worker after ``DBOS.launch()`` but before the
    SIGTERM block; reads ``runtime.source_sync`` and starts
    whichever trigger paths are enabled as daemon threads inside
    this process.
  * ``stop_supervised_services()`` — called by the SIGTERM handler
    to bring threads down before ``DBOS.destroy()``.
  * ``get_services_state()`` — read-only view used by the doctor
    probe and any other introspection. Returns alive flags +
    counters per service.

Threads are not orchestrated by DBOS — they're vanilla Python
daemon threads. The Watcher already supports start/stop via its
own ``_stop`` Event; the webhook server is a stdlib
``ThreadingHTTPServer`` that supports ``shutdown()``. So the
supervision contract is small: start, stop, alive-check.

Race notes:

  * Multiple worker boots in the same Python process (e.g. test
    fixtures) clear the registry on entry to ``start_supervised_services``.
  * Service threads never call ``DBOS.destroy()``; only the main
    thread / SIGTERM handler does.
"""
from __future__ import annotations

from contextlib import ExitStack
import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from collections.abc import Iterator
from typing import Any, Mapping, Optional

from okg.substrate.db.connection import connect as db_connect
from okg.substrate.support.user_hints import okg_prefix


__all__ = [
    "ServicesStopResult",
    "ServicesState",
    "WatcherServiceState",
    "WebhookServiceState",
    "get_services_state",
    "source_sync_replay_boundary",
    "source_sync_replay_boundary_rollback_inventory",
    "ReplaceableCutoverWorkflowPolicy",
    "RuntimeCutoverBlocked",
    "replaceable_cutover_workflow_policies",
    "supersede_incompatible_source_sync_workflows",
    "stamp_replaceable_deployment_enqueued",
    "stamp_unversioned_deployment_enqueued",
    "start_supervised_services",
    "stop_supervised_services",
    "supervised_services_reconciliation",
]


log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReplaceableCutoverWorkflowPolicy:
    """Exact identity and PENDING safety rule for one cutover-replaceable row."""

    workflow_name: str
    queue_name: str
    allow_started_pending: bool
    # False: ``workflow_name`` is the exact row name. True: it is a name
    # prefix, used only for the source-poll family on DBOS's internal queue
    # (see ``replaceable_cutover_workflow_policies``).
    name_is_prefix: bool = False


def replaceable_cutover_workflow_policies(
    deployment: str,
) -> tuple[ReplaceableCutoverWorkflowPolicy, ...]:
    """Return the complete exact allowlist for application-version cutovers."""
    from okg.substrate.runtime.declared_attr_index_schedule import (
        DECLARED_ATTR_INDEX_SCHEDULE_ID,
    )
    from okg.substrate.runtime.declared_attr_index_workflow import (
        DECLARED_ATTR_INDEX_WORKFLOW_NAME,
    )
    from okg.substrate.runtime.embed_workflow import (
        EMBED_SCHEDULE_ID,
        EMBED_WORKFLOW_NAME,
    )
    from okg.substrate.runtime.escalation_sweep_schedule import (
        ESCALATION_SWEEP_SCHEDULE_ID,
    )
    from okg.substrate.runtime.escalation_sweep_workflow import (
        ESCALATION_SWEEP_WORKFLOW_NAME,
    )
    from okg.substrate.runtime.gates_reevaluate_schedule import (
        GATES_REEVALUATE_SCHEDULE_ID,
    )
    from okg.substrate.runtime.gates_reevaluate_workflow import (
        GATES_REEVALUATE_WORKFLOW_NAME,
    )
    from okg.substrate.runtime.lens_putback_schedule import (
        LENS_PUTBACK_SCHEDULE_ID,
    )
    from okg.substrate.runtime.lens_putback_workflow import (
        LENS_PUTBACK_WORKFLOW_NAME,
    )
    from okg.substrate.runtime.partition_queue import (
        index_maintenance_queue_name,
        partition_queue_name,
        source_sync_queue_name,
    )
    from okg.substrate.runtime.publisher_schedule import (
        PUBLISHER_SCHEDULE_ID,
    )
    from okg.substrate.runtime.publisher_workflow import (
        PUBLISHER_WORKFLOW_NAME,
    )
    from okg.substrate.runtime.source_poll_schedule import (
        SOURCE_POLL_SCHEDULE_PREFIX,
    )
    from okg.substrate.runtime.source_sync_constants import (
        SOURCE_SYNC_WORKFLOW_NAME,
    )
    from okg.substrate.runtime.workflow_cancel import DBOS_INTERNAL_QUEUE

    control_queue = partition_queue_name(deployment)
    # Issue #3519 (operator decision cutoverwide A): the substrate's other
    # periodic work. Each schedule re-enqueues a fresh run on its next tick
    # (30 seconds to five minutes), and each run re-reads its own durable
    # state, so cancelling a run that has not started loses nothing. A
    # started run keeps ``allow_started_pending=False``, as for gate
    # re-evaluation: it may have a live executor, so it still blocks and the
    # operator clears it with ``okg runtime cancel-workflow``.
    periodic_on_control_queue = tuple(
        ReplaceableCutoverWorkflowPolicy(
            workflow_name=workflow_name,
            queue_name=control_queue,
            allow_started_pending=False,
        )
        for workflow_name in (
            PUBLISHER_WORKFLOW_NAME,
            ESCALATION_SWEEP_WORKFLOW_NAME,
            LENS_PUTBACK_WORKFLOW_NAME,
            # The embed schedule enqueues onto ``okg-<deployment>``, the
            # control queue (embed_workflow.register_embed_schedule).
            EMBED_WORKFLOW_NAME,
        )
    )
    # The DBOS scheduler runs each ``<schedule_id>_enqueue`` wrapper on its
    # internal queue, tagged with the then-latest application version. A pod
    # killed before the wrapper ran leaves that row ENQUEUED with the old
    # version, and the boundary below runs before the post-launch reaper
    # (``reap_stale_internal_enqueued``) could cancel it. These are the exact
    # wrapper names the substrate registers; the next cron tick recreates
    # each one. Matching them here, under the boundary's lock, is used instead
    # of moving that reaper ahead of the boundary: the reaper cancels every
    # old ENQUEUED row on the internal queue whatever its name, which would
    # also silently cancel an operator-resumed old workflow that the
    # boundary must refuse, and it runs outside the admission lock and the
    # first-launch schema check.
    scheduler_wrappers = tuple(
        ReplaceableCutoverWorkflowPolicy(
            workflow_name=f"{schedule_id}_enqueue",
            queue_name=DBOS_INTERNAL_QUEUE,
            allow_started_pending=False,
        )
        for schedule_id in (
            PUBLISHER_SCHEDULE_ID,
            ESCALATION_SWEEP_SCHEDULE_ID,
            LENS_PUTBACK_SCHEDULE_ID,
            EMBED_SCHEDULE_ID,
            GATES_REEVALUATE_SCHEDULE_ID,
            DECLARED_ATTR_INDEX_SCHEDULE_ID,
        )
    )
    # Source-poll workflows are scheduled directly on the internal queue
    # under ``okg_source_poll_<group>`` (``<group>`` is a cadence group such
    # as ``interval_300s``; older releases used one row per source). The
    # names a stale row carries were derived from the OLD release's registry
    # and grouping, which the new worker cannot reconstruct at the boundary,
    # so an exact per-source list would miss them. This one family therefore
    # matches by prefix, and only on the internal queue.
    # The substrate doctor (runtime/scheduled.py) is scheduled directly, not
    # through an ``_enqueue`` wrapper, so DBOS records its rows under the
    # workflow's own registered name on the internal queue. Each tick runs a
    # fresh doctor; an unstarted one is a stale health probe.
    #
    # Deployment-author schedules (``@deployment_scheduled``, applied by
    # ``apply_deployment_author_schedules``) are deliberately NOT listed.
    # Their names exist only in the deployment's own workflow modules, which
    # the worker has imported before this boundary but the promotion path
    # (deployment_promotion_operations) has not, so a registry-derived list
    # would give the two callers different policies. An old release may also
    # have registered different names, and whether skipping one tick of an
    # author's workflow is harmless is the author's call, not the
    # substrate's. Such a row still refuses the cutover with a named
    # ``okg runtime cancel-workflow`` remedy.
    doctor = ReplaceableCutoverWorkflowPolicy(
        workflow_name="doctor_workflow",
        queue_name=DBOS_INTERNAL_QUEUE,
        allow_started_pending=False,
    )
    source_poll_family = ReplaceableCutoverWorkflowPolicy(
        workflow_name=f"{SOURCE_POLL_SCHEDULE_PREFIX}_",
        queue_name=DBOS_INTERNAL_QUEUE,
        allow_started_pending=False,
        name_is_prefix=True,
    )

    return (
        ReplaceableCutoverWorkflowPolicy(
            workflow_name=SOURCE_SYNC_WORKFLOW_NAME,
            queue_name=source_sync_queue_name(deployment),
            # Preserve the existing source-sync replay-boundary contract. Issue
            # 1254 does not widen the separate generic stale-PENDING policy.
            allow_started_pending=True,
        ),
        ReplaceableCutoverWorkflowPolicy(
            workflow_name=GATES_REEVALUATE_WORKFLOW_NAME,
            queue_name=partition_queue_name(deployment),
            # A non-null start marker may still have a live executor, so this
            # stays False. Issue 1257 bounded the started-PENDING case by the
            # deadline DBOS records on dequeue, but that only reaches rows
            # carrying a workflow timeout and this schedule declares none. A
            # started gate re-evaluation is therefore cleared by the operator
            # verb (`okg runtime cancel-workflow`), not at the cutover
            # boundary, where cancelling it could race a live executor.
            allow_started_pending=False,
        ),
        ReplaceableCutoverWorkflowPolicy(
            workflow_name=DECLARED_ATTR_INDEX_WORKFLOW_NAME,
            queue_name=index_maintenance_queue_name(deployment),
            # Issue 3514: the five-minute schedule keeps one of these enqueued
            # almost always, so leaving it out refused nearly every upgrade.
            # An unstarted run is replaceable: each run re-reads the catalog
            # and the index manifest, and the schedule enqueues a fresh one
            # within five minutes. A started run stays False, as for gate
            # re-evaluation: it may be inside CREATE INDEX CONCURRENTLY with a
            # live executor, and cancelling it here could race that build. The
            # operator verb (`okg runtime cancel-workflow`) clears it; the next
            # run then marks the build it left `building` as failed and drops
            # the leftover index.
            allow_started_pending=False,
        ),
        *periodic_on_control_queue,
        *scheduler_wrappers,
        doctor,
        source_poll_family,
    )


def _replaceable_cutover_predicate(
    policies: tuple[ReplaceableCutoverWorkflowPolicy, ...],
) -> tuple[str, tuple[str, ...]]:
    clauses: list[str] = []
    params: list[str] = []
    for policy in policies:
        pending_guard = (
            ""
            if policy.allow_started_pending
            else " AND (status <> 'PENDING' OR started_at_epoch_ms IS NULL)"
        )
        # Every clause must be TRUE or FALSE, never NULL: the guard
        # query negates the whole predicate, and a NULL would hide a row
        # from both the refusal and the cancellation.
        name_match = (
            "COALESCE(starts_with(name, %s), FALSE)"
            if policy.name_is_prefix
            else "name IS NOT DISTINCT FROM %s"
        )
        clauses.append(
            "(queue_name IS NOT DISTINCT FROM %s "
            "AND " + name_match + pending_guard + ")"
        )
        params.extend((policy.queue_name, policy.workflow_name))
    if not clauses:
        raise RuntimeError("runtime cutover replaceable-workflow policy is empty")
    return "(" + " OR ".join(clauses) + ")", tuple(params)


class RuntimeCutoverBlocked(RuntimeError):
    """Typed fail-closed diagnostic for old-version active workflow rows."""

    code = "runtime_cutover_blocked"

    def __init__(
        self,
        *,
        deployment: str,
        live_version: str,
        blocker_rows: list[tuple[Any, ...]],
    ) -> None:
        from okg.substrate.runtime.workflow_cancel import (
            cancel_workflow_remedy_command,
        )

        self.deployment = deployment
        self.live_version = live_version
        self.blockers = tuple(
            {
                "workflow_id": str(row[0]),
                "name": row[1],
                "queue_name": row[2],
                "status": str(row[3]),
                "application_version": row[4],
                "started_at_epoch_ms": row[5],
                "inspection_command": (
                    f"{okg_prefix()} runtime debug-workflow --deployment "
                    f"{deployment} {row[0]}"
                ),
                # Issue #1254: this refusal used to name the blocking row and
                # offer nothing, so clearing one stale scheduled row meant raw
                # SQL or a destructive reset. Name the scoped remedy instead.
                "remedy_command": cancel_workflow_remedy_command(
                    deployment=deployment, workflow_id=str(row[0]),
                ),
            }
            for row in blocker_rows
        )
        details = "; ".join(
            f"{row['workflow_id']} {row['name']} {row['queue_name']} "
            f"{row['status']} version={row['application_version']!r}; "
            f"inspect with: {row['inspection_command']}; "
            f"clear it with: {row['remedy_command']}"
            for row in self.blockers
        )
        super().__init__(
            "DBOS application-version cutover is blocked by active workflow "
            f"history that is not safely replaceable ({self.code}): {details}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "deployment": self.deployment,
            "live_application_version": self.live_version,
            "blockers": [dict(row) for row in self.blockers],
        }


def source_sync_replay_boundary_rollback_inventory(
    *,
    deployment: str,
    live_version: str,
) -> dict[str, Any]:
    """Describe the only supported rollback for an application-version cutover.

    Cancelled DBOS history is terminal evidence, not an in-place reversible
    record. A rollback therefore restores the verified database backup and the
    previous sealed release as one transaction, then proves the old release
    ready before reopening writers.
    """
    return {
        "deployment": deployment,
        "live_application_version": live_version,
        "rollback_mode": "verified_database_restore_and_previous_release",
        "in_place_history_reversal": False,
        "restore_required": True,
        "sequence": [
            "keep the promotion writer fence held",
            "restore the verified pre-cutover database backup",
            "activate the captured previous sealed release",
            "restart the previous services",
            "prove previous-release readiness and generation alignment",
            "reopen writers only after readiness succeeds",
        ],
    }


@contextmanager
def source_sync_replay_boundary(
    *,
    dsn: str,
    deployment: str,
    live_version: str,
) -> Iterator[dict[str, Any]]:
    """Open the application-version replay boundary through worker launch.

    The function name is retained for compatibility with promotion receipts and
    existing callers. The boundary now owns every exact workflow policy returned
    by :func:`replaceable_cutover_workflow_policies`.

    DBOS filters old-version work out of recovery, but its single-concurrency
    queue still counts an old ``PENDING`` row while deciding whether new work
    may start. The cutover therefore takes the same deployment-wide advisory
    lock as every producer and holds a conflicting ``workflow_status`` table
    lock until the caller has launched DBOS. This closes the otherwise unsafe
    migration-commit-to-launch window, including producers that call DBOS
    directly instead of using the normal producer helper.

    Source state and watermarks remain authoritative, so a later producer
    wakeup can recreate any cancelled replaceable work. If launch fails, the
    transaction rolls back and the worker refuses readiness.
    """
    from okg.substrate.runtime.promotion_fence import deployment_admission_lock

    policies = replaceable_cutover_workflow_policies(deployment)
    source_sync_policy = policies[0]
    policy_predicate, policy_params = _replaceable_cutover_predicate(policies)
    # This is the one hold that is SUPPOSED to sit idle in transaction: the
    # lock and the `workflow_status` table lock below stay held through the
    # caller's `DBOS.launch()`, which on a first launch runs migrations and
    # workflow recovery. Under the factory's idle-in-transaction bound
    # (issue #1310) a slow launch would terminate this backend, roll the
    # boundary back, and refuse worker readiness with a message about the
    # cutover rather than about a timeout. Exempt this transaction alone.
    with deployment_admission_lock(
        deployment=deployment, dsn=dsn, bound_idle_in_transaction=False,
    ) as conn:
        with conn.cursor() as cur:
            # DBOS creates its system schema during its first launch. There is
            # no persisted history to migrate on a clean deployment, so let
            # that first launch initialize the schema while the deployment
            # admission lock still excludes normal producers. Every later boot
            # takes the table lock below before DBOS can recover or dispatch.
            cur.execute("SELECT to_regclass(%s)", ("dbos.workflow_status",))
            workflow_status_relation = cur.fetchone()
            if workflow_status_relation is None or workflow_status_relation[0] is None:
                # DBOS's first-launch migrations include CREATE INDEX
                # CONCURRENTLY, which cannot run while this read transaction
                # remains open. There is no workflow table or history for a
                # producer to race here, so release the transaction before
                # yielding to first launch.
                conn.commit()
                yield {
                    "deployment": deployment,
                    "queue_name": source_sync_policy.queue_name,
                    "workflow_name": source_sync_policy.workflow_name,
                    "live_application_version": live_version,
                    "schema_initialized": False,
                    "admission_lock_held": False,
                    "status": "schema_not_initialized",
                    "cancelled_count": 0,
                    "cancelled_workflows": [],
                }
                return

            # Serialize the guard and cancellation against DBOS inserts. The
            # lock is held until the caller returns from DBOS.launch().
            cur.execute(
                "LOCK TABLE dbos.workflow_status IN SHARE ROW EXCLUSIVE MODE"
            )
            # DBOS application versions are process-wide, not workflow-specific.
            # Refuse before mutation if any old active row is outside the exact
            # replaceable policy, including a gate reevaluation that has started.
            cur.execute(
                f"""
                /* runtime_cutover_replaceable */
                SELECT workflow_uuid, name, queue_name, status,
                       application_version, started_at_epoch_ms
                  FROM dbos.workflow_status
                 WHERE status IN ('ENQUEUED', 'DELAYED', 'PENDING')
                   AND application_version IS DISTINCT FROM %s
                   AND NOT {policy_predicate}
                 LIMIT 20
                """,
                (live_version, *policy_params),
            )
            incompatible_other_workflows = cur.fetchall()
            if incompatible_other_workflows:
                raise RuntimeCutoverBlocked(
                    deployment=deployment,
                    live_version=live_version,
                    blocker_rows=incompatible_other_workflows,
                )
            cur.execute(
                f"""
                /* runtime_cutover_replaceable */
                UPDATE dbos.workflow_status
                   SET status = 'CANCELLED',
                       -- As DBOS's own cancel does: a cancelled row must not
                       -- keep its deduplication key, or the next enqueue in
                       -- the same bucket fails as a terminal collision.
                       deduplication_id = NULL
                 WHERE status IN ('ENQUEUED', 'DELAYED', 'PENDING')
                   AND application_version IS DISTINCT FROM %s
                   AND {policy_predicate}
                RETURNING workflow_uuid, name, queue_name, status,
                          application_version, started_at_epoch_ms
                """,
                (live_version, *policy_params),
            )
            rows = cur.fetchall()
        yield {
            "deployment": deployment,
            "queue_name": source_sync_policy.queue_name,
            "workflow_name": source_sync_policy.workflow_name,
            "live_application_version": live_version,
            "schema_initialized": True,
            "admission_lock_held": True,
            "status": "cutover_applied",
            "cancelled_count": len(rows),
            "cancelled_workflows": [
                {
                    "workflow_id": row[0],
                    "name": row[1],
                    "queue_name": row[2],
                    "status": row[3],
                    "application_version": row[4],
                    "started_at_epoch_ms": row[5],
                }
                for row in rows
            ],
        }


def supersede_incompatible_source_sync_workflows(
    *,
    dsn: str,
    deployment: str,
    live_version: str,
) -> dict[str, Any]:
    """Commit the exact replaceable-workflow application-version boundary.

    The legacy name is retained for deployment-promotion receipt compatibility.
    """
    with source_sync_replay_boundary(
        dsn=dsn,
        deployment=deployment,
        live_version=live_version,
    ) as result:
        return result


@dataclass(frozen=True)
class ServicesStopResult:
    """Truthful result of the bounded producer-stop phase."""

    ok: bool
    maintenance_stopped: bool
    watcher_stopped: bool
    webhook_stopped: bool
    errors: tuple[str, ...] = ()
    observations: tuple[Mapping[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "maintenance_stopped": self.maintenance_stopped,
            "watcher_stopped": self.watcher_stopped,
            "webhook_stopped": self.webhook_stopped,
            "errors": list(self.errors),
            "observations": [dict(item) for item in self.observations],
        }


@dataclass
class WatcherServiceState:
    """Per-worker view of the embedded Watcher service.

    The Watcher itself manages its own threads (observer +
    debounce loop). This dataclass holds the live instance plus
    metadata for introspection.
    """

    enabled: bool = False
    instance: Optional[Any] = None  # okg.substrate.watch.Watcher
    started_at: Optional[float] = None
    config: dict[str, Any] = field(default_factory=dict)
    last_error: Optional[str] = None

    def alive(self) -> bool:
        if not self.enabled or self.instance is None:
            return False
        observer = getattr(self.instance, "_observer", None)
        loop_thread = getattr(self.instance, "_loop_thread", None)
        if observer is None or loop_thread is None:
            return False
        return bool(loop_thread.is_alive())

    def to_dict(self) -> dict[str, Any]:
        stats = (
            self.instance.stats.as_dict()
            if self.instance is not None and self.instance.stats
            else {}
        )
        return {
            "enabled": self.enabled,
            "alive": self.alive(),
            "started_at": self.started_at,
            "config": dict(self.config),
            "last_error": self.last_error,
            **stats,
        }


@dataclass
class WebhookServiceState:
    """Per-worker view of the embedded webhook HTTP server."""

    enabled: bool = False
    server: Optional[Any] = None  # ThreadingHTTPServer
    thread: Optional[threading.Thread] = None
    stats: Optional[Any] = None  # WebhookServerStats
    started_at: Optional[float] = None
    config: dict[str, Any] = field(default_factory=dict)
    last_error: Optional[str] = None

    def alive(self) -> bool:
        if not self.enabled or self.thread is None:
            return False
        return bool(self.thread.is_alive())

    def to_dict(self) -> dict[str, Any]:
        stats_dict = (
            self.stats.to_dict() if self.stats is not None else {}
        )
        return {
            "enabled": self.enabled,
            "alive": self.alive(),
            "started_at": self.started_at,
            "config": dict(self.config),
            "last_error": self.last_error,
            **stats_dict,
        }


@dataclass
class MaintenanceServiceState:
    """Per-worker view of source-sync runtime maintenance."""

    enabled: bool = False
    thread: Optional[threading.Thread] = None
    stop_event: Optional[threading.Event] = None
    started_at: Optional[float] = None
    config: dict[str, Any] = field(default_factory=dict)
    last_error: Optional[str] = None
    last_cleanup_at: Optional[float] = None
    cleanup_runs: int = 0
    cancelled_count: int = 0

    def alive(self) -> bool:
        if not self.enabled or self.thread is None:
            return False
        return bool(self.thread.is_alive())

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "alive": self.alive(),
            "started_at": self.started_at,
            "config": dict(self.config),
            "last_error": self.last_error,
            "last_cleanup_at": self.last_cleanup_at,
            "cleanup_runs": self.cleanup_runs,
            "cancelled_count": self.cancelled_count,
        }


@dataclass
class ServicesState:
    """Process-wide registry of supervised services."""

    watcher: WatcherServiceState = field(default_factory=WatcherServiceState)
    webhook: WebhookServiceState = field(default_factory=WebhookServiceState)
    maintenance: MaintenanceServiceState = field(
        default_factory=MaintenanceServiceState,
    )
    deployment: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "deployment": self.deployment,
            "watcher_thread": self.watcher.to_dict(),
            "webhook_thread": self.webhook.to_dict(),
            "maintenance_thread": self.maintenance.to_dict(),
        }


_STATE = ServicesState()


def get_services_state() -> ServicesState:
    """Return the process-wide services registry (read-only view)."""
    return _STATE


def _reset_state(deployment: str) -> None:
    """Clear the registry on a fresh worker boot in the same process.

    Test harnesses re-boot the worker in the same Python process;
    a stale Watcher instance from a prior boot would falsely report
    `alive: true`.
    """
    global _STATE
    _STATE = ServicesState(deployment=deployment)


def start_supervised_services(
    *,
    deployment: str,
    dsn: str,
    runtime_cfg,
) -> None:
    """Start whichever trigger producers the manifest declares.

    Called from the worker after ``DBOS.launch()``. Reads
    ``runtime.source_sync`` (via :func:`resolve_source_sync`),
    constructs the Watcher and/or webhook server, starts each on
    a daemon thread, and stashes the live instances in the
    module-level registry for introspection.

    Failures starting one service don't block the other (the worker
    continues running with whichever services did come up; the doctor
    surfaces the failure).
    """
    from okg.substrate.runtime.config import resolve_source_sync

    _reset_state(deployment)
    services = resolve_source_sync(runtime_cfg)

    watch_cfg = services.get("watch", {})
    if watch_cfg.get("enabled", True):
        try:
            _start_watcher(
                deployment=deployment,
                dsn=dsn,
                watch_cfg=watch_cfg,
            )
        except Exception as exc:  # noqa: BLE001
            log.exception(
                "failed to start embedded Watcher; continuing "
                "without fs_event triggers",
            )
            _STATE.watcher.enabled = True
            _STATE.watcher.last_error = (
                f"{type(exc).__name__}: {exc}"
            )

    webhook_cfg = services.get("webhook", {})
    if webhook_cfg.get("enabled", False):
        try:
            _start_webhook_server(
                deployment=deployment,
                dsn=dsn,
                webhook_cfg=webhook_cfg,
            )
        except Exception as exc:  # noqa: BLE001
            log.exception(
                "failed to start embedded webhook server; continuing "
                "without webhook triggers",
            )
            _STATE.webhook.enabled = True
            _STATE.webhook.last_error = (
                f"{type(exc).__name__}: {exc}"
            )

    maintenance_cfg = services.get("maintenance", {})
    if maintenance_cfg.get("enabled", True):
        try:
            _start_maintenance_service(
                deployment=deployment,
                dsn=dsn,
                maintenance_cfg=maintenance_cfg,
            )
        except Exception as exc:  # noqa: BLE001
            log.exception(
                "failed to start source-sync maintenance service; "
                "continuing without steady-state cleanup",
            )
            _STATE.maintenance.enabled = True
            _STATE.maintenance.last_error = (
                f"{type(exc).__name__}: {exc}"
            )


def supervised_services_reconciliation(
    *,
    deployment: str,
    runtime_cfg,
    registry: Mapping[str, Any],
) -> dict[str, Any]:
    """Report whether every expected in-worker producer actually started."""
    from okg.substrate.runtime.config import resolve_source_sync

    trigger_kinds: set[str] = set()
    for entry in registry.values():
        sync = getattr(entry, "sync", None)
        trigger_kinds.update(
            str(kind) for kind in getattr(sync, "triggers", ()) or ()
        )

    configured = resolve_source_sync(runtime_cfg)
    expected = {
        "watcher": bool(
            configured.get("watch", {}).get("enabled", True)
            and trigger_kinds.intersection({"fs_event", "git_ref_change"})
        ),
        "webhook": bool(
            configured.get("webhook", {}).get("enabled", False)
            and "webhook" in trigger_kinds
        ),
        "maintenance": True,
    }
    state = get_services_state()
    observations = {
        "watcher": state.watcher.to_dict(),
        "webhook": state.webhook.to_dict(),
        "maintenance": state.maintenance.to_dict(),
    }
    problems: list[str] = []
    if state.deployment != deployment:
        problems.append(
            f"deployment_mismatch:{state.deployment or 'unobserved'}"
        )
    for name, required in expected.items():
        if not required:
            continue
        observed = observations[name]
        if observed.get("last_error"):
            problems.append(f"{name}:start_failed:{observed['last_error']}")
        if not observed.get("alive"):
            problems.append(f"{name}:not_alive")
    return {
        "schema": "okg.runtime.supervised_services_reconciliation.v1",
        "ok": not problems,
        "deployment": deployment,
        "trigger_kinds": sorted(trigger_kinds),
        "expected": expected,
        "observed": observations,
        "problems": problems,
    }


def _start_watcher(
    *, deployment: str, dsn: str, watch_cfg: dict[str, Any],
) -> None:
    """Construct + start the deployment's Watcher.

    Loads the source registry, builds WatchSource entries for every
    source that declares ``fs_event`` or ``git_ref_change``, hands
    them to a ``Watcher``, which always enqueues through DBOS.
    """
    from okg.substrate.deployments import resolve_deployment
    from okg.substrate.sources.registry import load_source_registry
    from okg.substrate.watch import Watcher, WatchSource

    manifest = resolve_deployment(deployment)
    registry_path = manifest.source_registry
    if not registry_path.is_file():
        log.info(
            "Skipping embedded watcher: no source_registry.yaml at %s",
            registry_path,
        )
        return

    registry = load_source_registry(registry_path)
    roots: set[Path] = set()
    source_scopes: list[WatchSource] = []
    for name, entry in registry.items():
        cap = getattr(entry, "sync", None)
        if (
            cap is None
            or cap.watch is None
            or not (
                {"fs_event", "git_ref_change"} & set(cap.triggers)
            )
        ):
            continue
        scope_roots = tuple(
            Path(root).expanduser().resolve()
            for root in cap.watch.roots
            if Path(root).expanduser().is_dir()
        )
        if not scope_roots:
            continue
        roots.update(scope_roots)
        source_scopes.append(
            WatchSource(
                source_name=name,
                roots=scope_roots,
                include_globs=cap.watch.include_globs,
                exclude_globs=cap.watch.exclude_globs,
                run_mode=cap.default_event_mode,
                trigger_kind=(
                    "git_ref_change"
                    if "git_ref_change" in cap.triggers
                    else "fs_event"
                ),
                deletes_require_reconcile=(
                    cap.deletes_require_reconcile
                ),
                reconcile_mode=cap.reconcile_mode,
                scoped_delete_mode=cap.scoped_delete_mode,
                dependent_fanout=cap.dependents.on_event,
                reconcile_dependent_fanout=(
                    cap.dependents.on_reconcile
                ),
                scoped_delete_dependent_fanout=(
                    cap.scoped_delete_dependent_fanout
                ),
                compaction_window_seconds=(
                    cap.compaction.window_seconds
                    if cap.compaction else 0
                ),
                compaction_max_events=(
                    cap.compaction.max_events
                    if cap.compaction else 500
                ),
                compaction_max_scope_keys=(
                    cap.compaction.max_scope_keys
                    if cap.compaction else 500
                ),
            )
        )

    if not roots:
        log.info(
            "Skipping embedded watcher: no source declares "
            "fs_event/git_ref_change triggers",
        )
        return

    debounce_s = float(watch_cfg.get("debounce_seconds", 5))
    heartbeat_s = float(watch_cfg.get("heartbeat_seconds", 60))
    watcher = Watcher(
        dsn, deployment,
        roots=sorted(roots),
        source_scopes=tuple(source_scopes),
        debounce_s=debounce_s,
        heartbeat_s=heartbeat_s,
    )
    # Transfer ownership before starting any observer/thread.  Watcher.start()
    # is sequential and may fail after an earlier handle is live; retaining the
    # instance lets the SIGTERM path stop and join that partial startup.
    _STATE.watcher.enabled = True
    _STATE.watcher.instance = watcher
    watcher.start()
    _STATE.watcher.started_at = time.time()
    _STATE.watcher.config = dict(watch_cfg)
    log.info(
        "embedded Watcher started: deployment=%s n_roots=%d "
        "n_sources=%d debounce_s=%s",
        deployment, len(roots), len(source_scopes), debounce_s,
    )


def _start_webhook_server(
    *, deployment: str, dsn: str, webhook_cfg: dict[str, Any],
) -> None:
    """Construct + start the deployment's webhook HTTP server."""
    from okg.substrate.deployments import find_deployment_dir
    from okg.substrate.source_sync_producers import (
        make_webhook_http_server,
    )
    from okg.substrate.sources.registry import load_source_registry

    dep_dir = find_deployment_dir(deployment)
    if dep_dir is None:
        raise RuntimeError(
            f"deployment dir for {deployment!r} not found"
        )
    registry_path = dep_dir / "source_registry.yaml"
    if not registry_path.is_file():
        log.info(
            "Skipping embedded webhook server: no "
            "source_registry.yaml at %s",
            registry_path,
        )
        return
    registry = load_source_registry(registry_path)

    host = webhook_cfg.get("host", "127.0.0.1")
    port = int(webhook_cfg.get("port", 8088))

    server, stats = make_webhook_http_server(
        dsn,
        deployment=deployment,
        registry=registry,
        host=host,
        port=port,
        publish=True,
    )

    def _serve():
        try:
            server.serve_forever()
        except Exception:  # noqa: BLE001
            log.exception(
                "embedded webhook server thread exited unexpectedly",
            )

    thread = threading.Thread(
        target=_serve,
        name=f"okg-webhook-{deployment}",
        daemon=True,
    )
    thread.start()
    _STATE.webhook.enabled = True
    _STATE.webhook.server = server
    _STATE.webhook.thread = thread
    _STATE.webhook.stats = stats
    _STATE.webhook.started_at = time.time()
    _STATE.webhook.config = dict(webhook_cfg)
    log.info(
        "embedded webhook server started: deployment=%s "
        "host=%s port=%d", deployment, host, port,
    )


def _start_maintenance_service(
    *,
    deployment: str,
    dsn: str,
    maintenance_cfg: dict[str, Any],
) -> None:
    interval_s = max(
        1.0,
        float(maintenance_cfg.get("interval_seconds", 60)),
    )
    stale_after_seconds = max(
        1,
        int(maintenance_cfg.get("stale_enqueued_after_seconds", 300)),
    )
    stop_event = threading.Event()

    thread = threading.Thread(
        target=_maintenance_loop,
        name=f"okg-source-sync-maintenance-{deployment}",
        kwargs={
            "deployment": deployment,
            "dsn": dsn,
            "interval_s": interval_s,
            "stale_after_seconds": stale_after_seconds,
            "stop_event": stop_event,
        },
        daemon=True,
    )
    _STATE.maintenance.enabled = True
    _STATE.maintenance.thread = thread
    _STATE.maintenance.stop_event = stop_event
    _STATE.maintenance.started_at = time.time()
    _STATE.maintenance.config = {
        "interval_seconds": interval_s,
        "stale_enqueued_after_seconds": stale_after_seconds,
    }
    thread.start()
    log.info(
        "source-sync maintenance started: deployment=%s "
        "interval_s=%s stale_enqueued_after_seconds=%s",
        deployment,
        interval_s,
        stale_after_seconds,
    )


def _maintenance_loop(
    *,
    deployment: str,
    dsn: str,
    interval_s: float,
    stale_after_seconds: int,
    stop_event: threading.Event,
) -> None:
    while not stop_event.wait(timeout=interval_s):
        _run_maintenance_once(
            deployment=deployment,
            dsn=dsn,
            stale_after_seconds=stale_after_seconds,
        )


def _run_maintenance_once(
    *,
    deployment: str,
    dsn: str,
    stale_after_seconds: int,
) -> dict[str, Any]:
    from okg.substrate.runtime.workflow_cancel import (
        cleanup_deadline_expired_pending_workflows,
    )

    try:
        pending_report = cleanup_stale_pending_workflows(
            dsn=dsn,
            deployment=deployment,
            cancel_started_pending=False,
        )
        # Issue #1257. The sweep above deliberately spares a workflow that
        # already started, so a publisher cycle that raised held the only
        # queue slot forever. A row still PENDING past the deadline DBOS
        # itself recorded has no executor honoring that deadline, so cancel
        # it and let the next scheduled cycle start.
        #
        # Its own try: this sweep was added after the two beside it, and a
        # failure here must not take the long-standing ENQUEUED cleanup down
        # with it. The failure is reported in the returned dict AND recorded
        # as the service's last_error, which is the only surface a doctor
        # probe reads; leaving that None would let a sweep that has never
        # succeeded report a healthy maintenance service.
        deadline_error: str | None = None
        try:
            deadline_report: dict[str, Any] = (
                cleanup_deadline_expired_pending_workflows(
                    dsn=dsn,
                    deployment=deployment,
                )
            )
        except Exception as exc:  # noqa: BLE001
            deadline_error = f"{type(exc).__name__}: {exc}"
            deadline_report = {
                "cancelled_count": 0,
                "error": type(exc).__name__,
                "message": str(exc),
            }
            log.exception(
                "deadline-expired PENDING cleanup failed: deployment=%s; a "
                "workflow past its own deadline may still hold the "
                "concurrency=1 queue slot",
                deployment,
            )
        enqueued_report = cleanup_stale_enqueued_workflows(
            dsn=dsn,
            deployment=deployment,
            stale_after_seconds=stale_after_seconds,
        )
        _STATE.maintenance.last_cleanup_at = time.time()
        _STATE.maintenance.cleanup_runs += 1
        _STATE.maintenance.last_error = deadline_error
        cancelled = (
            int(pending_report.get("cancelled_count") or 0)
            + int(deadline_report.get("cancelled_count") or 0)
            + int(enqueued_report.get("cancelled_count") or 0)
        )
        _STATE.maintenance.cancelled_count += cancelled
        report = {
            "cancelled_count": cancelled,
            "pending": pending_report,
            "deadline_expired": deadline_report,
            "enqueued": enqueued_report,
        }
        if cancelled:
            log.warning(
                "source-sync maintenance cancelled %d stale "
                "replaceable workflow(s): deployment=%s report=%s",
                cancelled,
                deployment,
                report,
            )
        else:
            log.debug(
                "source-sync maintenance found no stale ENQUEUED "
                "replaceable workflows: deployment=%s",
                deployment,
            )
        return report
    except Exception as exc:  # noqa: BLE001
        _STATE.maintenance.last_cleanup_at = time.time()
        _STATE.maintenance.cleanup_runs += 1
        _STATE.maintenance.last_error = f"{type(exc).__name__}: {exc}"
        log.exception(
            "source-sync maintenance failed: deployment=%s",
            deployment,
        )
        return {
            "cancelled_count": 0,
            "error": type(exc).__name__,
            "message": str(exc),
        }


def stop_supervised_services(
    *, timeout_s: float = 10.0,
) -> ServicesStopResult:
    """Stop all supervised services. Called from the SIGTERM handler
    before ``DBOS.destroy()``.

    All potentially blocking stop calls run concurrently against one
    monotonic deadline. Exceptions and live threads are reported instead of
    being flattened into a successful shutdown; controlled recovery may only
    proceed when ``ok`` is true.
    """
    state = _STATE
    deadline = time.monotonic() + max(0.0, timeout_s)
    failures: list[str] = []
    stop_threads: list[
        tuple[str, threading.Thread, dict[str, BaseException]]
    ] = []

    def start_stop(name: str, callback) -> None:
        result: dict[str, BaseException] = {}

        def run() -> None:
            def capture_failure(
                _exc_type: type[BaseException] | None,
                exc: BaseException | None,
                _traceback: object,
            ) -> bool:
                if exc is None:
                    return False
                result["error"] = exc
                return True

            with ExitStack() as failure_boundary:
                # The callback records and suppresses every BaseException only
                # after transferring it to the supervisor result.
                failure_boundary.push(capture_failure)
                callback()

        thread = threading.Thread(
            target=run,
            name=f"okg-stop-{name}",
            daemon=True,
        )
        thread.start()
        stop_threads.append((name, thread, result))

    if state.maintenance.stop_event is not None:
        state.maintenance.stop_event.set()
    if state.watcher.instance is not None:
        # Reserve a small margin for the wrapper thread to return and for the
        # supervisor's post-stop liveness audit.  The Watcher spends the rest as
        # one shared join budget, rather than four unrelated fixed waits.
        watcher_timeout = max(
            0.0,
            deadline - time.monotonic() - min(0.25, max(0.0, timeout_s) / 10),
        )
        start_stop(
            "watcher",
            lambda: state.watcher.instance.stop(timeout_s=watcher_timeout),
        )
    if state.webhook.server is not None:
        def stop_webhook() -> None:
            state.webhook.server.shutdown()
            state.webhook.server.server_close()

        start_stop("webhook", stop_webhook)

    wait_threads: list[tuple[str, threading.Thread]] = []
    if state.maintenance.thread is not None:
        wait_threads.append(("maintenance", state.maintenance.thread))
    wait_threads.extend(
        (name, thread) for name, thread, _result in stop_threads
    )
    if state.webhook.thread is not None:
        wait_threads.append(("webhook-server", state.webhook.thread))
    for name, thread in wait_threads:
        thread.join(timeout=max(0.0, deadline - time.monotonic()))
        if thread.is_alive():
            failures.append(f"{name}:stop_timeout")

    stop_wrapper_alive = {
        name: thread.is_alive() for name, thread, _result in stop_threads
    }
    stop_wrapper_failed: dict[str, bool] = {}
    for name, _thread, result in stop_threads:
        error = result.get("error")
        stop_wrapper_failed[name] = error is not None
        if error is not None:
            failures.append(f"{name}:{type(error).__name__}")
            log.error(
                "error stopping supervised %s: %s",
                name,
                error,
            )

    maintenance_started = (
        state.maintenance.thread is not None
        or state.maintenance.stop_event is not None
    )
    maintenance_handle_identified = (
        state.maintenance.thread is not None
        and callable(getattr(state.maintenance.thread, "join", None))
        and callable(getattr(state.maintenance.thread, "is_alive", None))
    )
    maintenance_stop_signalled = (
        state.maintenance.stop_event is not None
        and state.maintenance.stop_event.is_set()
    )
    maintenance_alive = bool(
        maintenance_handle_identified
        and state.maintenance.thread.is_alive()
    )
    maintenance_stopped = not maintenance_started or (
        maintenance_stop_signalled
        and maintenance_handle_identified
        and not maintenance_alive
    )

    watcher_started = state.watcher.instance is not None
    watcher_required_attrs = (
        "_observer",
        "_loop_thread",
        "_heartbeat_thread",
    )
    watcher_required_handles = (
        tuple(
            getattr(state.watcher.instance, attr, None)
            for attr in watcher_required_attrs
        )
        if watcher_started
        else ()
    )
    watcher_head_handle = (
        getattr(state.watcher.instance, "_head_poller_thread", None)
        if watcher_started
        else None
    )
    watcher_handles = watcher_required_handles + (
        (watcher_head_handle,) if watcher_head_handle is not None else ()
    )
    watcher_existing_handles = tuple(
        handle for handle in watcher_handles if handle is not None
    )
    watcher_partial_start = bool(watcher_started and state.watcher.last_error)
    watcher_handle_identified = bool(
        watcher_started
        and (watcher_partial_start or all(handle is not None for handle in watcher_required_handles))
        and all(
            callable(getattr(handle, "join", None))
            and callable(getattr(handle, "is_alive", None))
            for handle in watcher_existing_handles
        )
    )
    watcher_alive = bool(
        watcher_handle_identified
        and any(thread.is_alive() for thread in watcher_existing_handles)
    )
    watcher_wrapper_succeeded = (
        watcher_started
        and not stop_wrapper_alive.get("watcher", True)
        and not stop_wrapper_failed.get("watcher", True)
    )
    watcher_stopped = not watcher_started or (
        watcher_wrapper_succeeded
        and watcher_handle_identified
        and not watcher_alive
    )

    webhook_started = (
        state.webhook.server is not None or state.webhook.thread is not None
    )
    webhook_handle_identified = (
        state.webhook.server is not None
        and state.webhook.thread is not None
        and callable(getattr(state.webhook.thread, "join", None))
        and callable(getattr(state.webhook.thread, "is_alive", None))
    )
    webhook_alive = bool(
        webhook_handle_identified
        and state.webhook.thread.is_alive()
    )
    webhook_wrapper_succeeded = (
        webhook_started
        and not stop_wrapper_alive.get("webhook", True)
        and not stop_wrapper_failed.get("webhook", True)
    )
    webhook_stopped = not webhook_started or (
        webhook_wrapper_succeeded
        and webhook_handle_identified
        and not webhook_alive
    )

    if maintenance_started and not maintenance_stop_signalled:
        failures.append("maintenance:stop_not_signalled")
    if maintenance_started and not maintenance_handle_identified:
        failures.append("maintenance:underlying_handle_missing")
    if maintenance_alive:
        failures.append("maintenance:thread_alive")
    if watcher_started and not watcher_handle_identified:
        failures.append("watcher:underlying_handle_missing")
    if watcher_alive:
        failures.append("watcher:thread_alive")
    if watcher_started and not watcher_wrapper_succeeded:
        failures.append("watcher:stop_incomplete")
    if webhook_started and not webhook_handle_identified:
        failures.append("webhook:underlying_handle_missing")
    if webhook_alive:
        failures.append("webhook:thread_alive")
    if webhook_started and not webhook_wrapper_succeeded:
        failures.append("webhook:stop_incomplete")
    failures = sorted(set(failures))

    role_states = (
        (
            "maintenance",
            maintenance_started,
            maintenance_stop_signalled,
            maintenance_handle_identified,
            maintenance_handle_identified,
            maintenance_stopped,
            maintenance_alive,
        ),
        (
            "watcher",
            watcher_started,
            watcher_wrapper_succeeded,
            watcher_handle_identified,
            watcher_started,
            watcher_stopped,
            watcher_alive,
        ),
        (
            "webhook",
            webhook_started,
            webhook_wrapper_succeeded,
            webhook_handle_identified,
            webhook_handle_identified,
            webhook_stopped,
            webhook_alive,
        ),
    )
    observations = tuple(
        {
            "sequence": sequence,
            "role": role,
            "state": (
                "not_started"
                if not started
                else "stopped_joined"
                if stopped
                else "stop_incomplete"
            ),
            "stop_succeeded": stop_succeeded,
            "underlying_handle_identified": handle_identified,
            "join_attempted": join_attempted,
            "join_completed": bool(started and stopped),
            "alive_after": alive_after,
        }
        for sequence, (
            role,
            started,
            stop_succeeded,
            handle_identified,
            join_attempted,
            stopped,
            alive_after,
        ) in enumerate(role_states, start=1)
    )
    result = ServicesStopResult(
        ok=(
            maintenance_stopped
            and watcher_stopped
            and webhook_stopped
            and not failures
        ),
        maintenance_stopped=maintenance_stopped,
        watcher_stopped=watcher_stopped,
        webhook_stopped=webhook_stopped,
        errors=tuple(failures),
        observations=observations,
    )
    if result.ok:
        log.info("supervised source-sync services stopped")
    else:
        log.error("supervised service stop incomplete: %s", result.to_dict())
    return result


# ---------------------------------------------------------------------------
# Conditional schedule registration helpers
# ---------------------------------------------------------------------------


def should_register_embed(manifest) -> bool:
    """True iff the admitted typed contract enables embedding with a backend
    the worker may run.

    A paid backend (``openrouter:``) is never run by the worker: it has no
    spend cap and records no cost. Such a deployment embeds with
    ``okg db embed-backfill --max-usd`` (PACT openrouter-embeddings-hnsw).
    """
    config = getattr(manifest, "embedding_config", None)
    if config is None:
        raise TypeError(
            "embedding registration requires manifest.embedding_config; "
            "raw or legacy manifest objects are not admitted"
        )
    if config.enabled is not True:
        return False
    from okg.substrate.embeddings.identity import is_paid_model_id, model_id_for_spec

    # A `sets` form backend is read as a set's (`hash@<dims>`, OpenRouter
    # widths above 2560: PACT vector-set-pgvector-widths).
    return not is_paid_model_id(
        model_id_for_spec(config.backend, vector_set=bool(config.sets)),
    )


def should_register_lens_putback(*, dsn: str) -> bool:
    """True iff ``okg.lens_definitions`` has any rows OR query fails.

    The substrate's lens API writes a row to ``lens_definitions`` on
    first register. So "no row" = no deployment has ever declared a
    lens against this DSN. Skipping the sweep is safe because there's
    nothing for it to find.

    Conservative bias on query failure: register the schedule.
    """
    try:
        with db_connect(dsn) as conn, \
                conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('okg.lens_definitions')"
            )
            if cur.fetchone()[0] is None:
                return False
            cur.execute(
                "SELECT 1 FROM okg.lens_definitions LIMIT 1"
            )
            return cur.fetchone() is not None
    except Exception:  # noqa: BLE001
        return True


def should_register_escalation_sweep(*, dsn: str) -> bool:
    """True iff ``okg.escalation_live`` has any row OR query fails.

    "No row" = no deployment has ever opened an escalation against
    this DSN. Skipping the sweep is safe.

    Conservative bias on query failure: register the schedule.
    """
    try:
        with db_connect(dsn) as conn, \
                conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('okg.escalation_live')"
            )
            if cur.fetchone()[0] is None:
                return False
            cur.execute(
                "SELECT 1 FROM okg.escalation_live LIMIT 1"
            )
            return cur.fetchone() is not None
    except Exception:  # noqa: BLE001
        return True


# ---------------------------------------------------------------------------
# Orphan-schedule cleanup at boot
# ---------------------------------------------------------------------------


def cleanup_disabled_feature_schedules(
    *,
    dsn: str,
    disabled: dict[str, list[str]],
) -> dict[str, dict[str, int]]:
    """Delete stale schedule rows + cancel orphan ENQUEUED/PENDING
    workflows for features whose gate is False at boot.

    Per ``cleanup-orphan-runtime-schedules``. Background: when a
    deployment had a feature enabled in a previous worker boot (e.g.
    lens_putback while ``okg.lens_definitions`` had rows) and the
    feature is now disabled (rows emptied, or operator turned it off),
    the previous boot's ``dbos.workflow_schedules`` row persists and
    DBOS keeps firing it on its cron. The new boot doesn't register
    the matching workflow function (because the gate is False), so
    DBOS errors every fire — and with ``concurrency=1`` on the
    partition queue, one such error-stuck PENDING blocks the entire
    queue from draining.

    This helper, invoked from ``run_worker`` after the gates are
    computed but before launch, deletes those stale rows AND cancels
    any already-ENQUEUED-or-PENDING workflows under the matching
    names, so the queue starts clean.

    Parameters
    ----------
    dsn : str
        Postgres DSN.
    disabled : dict[str, list[str]]
        Maps a schedule_id to the list of workflow names that
        feature can emit. Both the inner workflow and its ``_enqueue``
        wrapper need cancellation, because either can be stuck.

    Returns
    -------
    dict[str, dict]
        Per-schedule report: ``{schedule_id: {"schedule_deleted": N,
        "workflows_cancelled": N}}``. Used for INFO-level logging
        so operators can see what was cleaned up at boot.
    """
    out: dict[str, dict[str, int]] = {}
    if not disabled:
        return out
    with db_connect(dsn) as conn, \
            conn.cursor() as cur:
        for schedule_id, workflow_names in disabled.items():
            cur.execute(
                "DELETE FROM dbos.workflow_schedules "
                "WHERE schedule_name = %s",
                (schedule_id,),
            )
            schedule_deleted = cur.rowcount
            workflows_cancelled = 0
            for name in workflow_names:
                cur.execute(
                    "UPDATE dbos.workflow_status "
                    "   SET status = 'CANCELLED' "
                    " WHERE name = %s "
                    "   AND status IN ('ENQUEUED', 'PENDING')",
                    (name,),
                )
                workflows_cancelled += cur.rowcount
            out[schedule_id] = {
                "schedule_deleted": schedule_deleted,
                "workflows_cancelled": workflows_cancelled,
            }
        conn.commit()
    return out


def reap_stale_internal_enqueued(
    *,
    dsn: str,
    live_version: str,
) -> dict[str, int]:
    """Cancel ENQUEUED workflows on `_dbos_internal_queue` whose
    ``application_version`` doesn't match the live worker's version.

    Background: DBOS's `@DBOS.scheduled` enqueue code
    (``_scheduler.py:_enqueue_scheduled_workflow``) deliberately tags
    every enqueued cron with ``sys_db.get_latest_application_version()``,
    NOT with the running worker's ``GlobalParams.app_version``. The
    intent is rolling-deploy support: "if a newer version registered,
    let it pick up the schedules."

    Failure mode (observed 2026-05-11): an earlier worker process
    registered version V_old, died, and never deregistered. The live
    worker boots with version V_live (different code hash) but
    ``get_latest_application_version()`` still returns V_old because
    its DB row was registered later in wallclock time. Every scheduled
    enqueue from the live worker is tagged V_old. The dispatcher
    (``_sys_db.py:start_queued_workflows``) filters by
    ``application_version == V_live OR application_version IS NULL``,
    so it rejects every V_old row. Result: scheduled crons pile up
    forever on `_dbos_internal_queue` and never run.

    The companion fix is to call ``bump_live_version_to_latest``
    AFTER ``DBOS.launch()`` so the scheduler tags NEW enqueues with
    the live version. This helper cleans up the rows enqueued BEFORE
    that bump took effect (or that came from any other dead worker).
    """
    with db_connect(dsn) as conn, \
            conn.cursor() as cur:
        cur.execute(
            "UPDATE dbos.workflow_status "
            "   SET status = 'CANCELLED' "
            " WHERE queue_name = '_dbos_internal_queue' "
            "   AND status = 'ENQUEUED' "
            "   AND application_version IS NOT NULL "
            "   AND application_version != %s",
            (live_version,),
        )
        cancelled = cur.rowcount
        conn.commit()
    return {"cancelled_count": cancelled}


def bump_live_version_to_latest(*, dsn: str, live_version: str) -> None:
    """Make ``live_version`` the most-recently-registered entry in
    ``dbos.application_versions`` so DBOS's scheduler picks it for
    new enqueues. See ``reap_stale_internal_enqueued`` for the bug
    this works around.

    Idempotent. Safe to re-run at every worker boot.
    """
    import time as _time
    now_ms = int(_time.time() * 1000)
    with db_connect(dsn) as conn, \
            conn.cursor() as cur:
        cur.execute(
            "UPDATE dbos.application_versions "
            "   SET version_timestamp = %s "
            " WHERE version_name = %s",
            (now_ms, live_version),
        )
        conn.commit()


def _runtime_queue_names(deployment: str) -> tuple[str, str]:
    from okg.substrate.runtime.partition_queue import (
        deployment_runtime_queue_names,
    )

    return deployment_runtime_queue_names(deployment)


def stamp_unversioned_deployment_enqueued(
    *,
    dsn: str,
    deployment: str,
    live_version: str,
) -> dict[str, int | list[dict[str, Any]]]:
    """Stamp ENQUEUED deployment workflows that lack an app version.

    DBOSClient enqueue can create rows with ``application_version`` NULL.
    Some DBOS versions do not dispatch those rows from a versioned worker,
    leaving durable wakeups stranded. The worker has already made its live
    version the newest registered version before this runs, so stamping the
    deployment queue's unversioned ENQUEUED/DELAYED rows is the least
    destructive repair: it preserves the work and makes it dispatchable.
    """
    with db_connect(dsn) as conn, \
            conn.cursor() as cur:
        rows = []
        for queue_name in _runtime_queue_names(deployment):
            cur.execute(
                "UPDATE dbos.workflow_status "
                "   SET application_version = %s "
                " WHERE queue_name = %s "
                "   AND status IN ('ENQUEUED', 'DELAYED') "
                "   AND application_version IS NULL "
                "RETURNING workflow_uuid, name",
                (live_version, queue_name),
            )
            rows.extend(cur.fetchall())
        conn.commit()
    return {
        "stamped_count": len(rows),
        "stamped_sample": [
            {"workflow_uuid": row[0], "name": row[1]}
            for row in rows[:5]
        ],
    }


def stamp_replaceable_deployment_enqueued(
    *,
    dsn: str,
    deployment: str,
    live_version: str,
) -> dict[str, int | list[dict[str, Any]]]:
    """Stamp replaceable deployment wakeups with the live app version.

    A short-lived producer can enqueue DBOS wakeups just before a worker
    launches. If the worker's code hash changes, those wakeups are pinned to
    the prior app version and DBOS will not dispatch them. These workflow
    kinds are substrate-owned wakeups whose source state remains authoritative,
    so preserving and restamping the work is safer than cancelling it.
    """
    with db_connect(dsn) as conn, \
            conn.cursor() as cur:
        rows = []
        for queue_name in _runtime_queue_names(deployment):
            cur.execute(
                """
                WITH candidates AS (
                    SELECT workflow_uuid, name,
                           application_version AS old_application_version
                      FROM dbos.workflow_status
                     WHERE queue_name = %s
                       AND status IN ('ENQUEUED', 'DELAYED')
                       AND (
                            application_version IS NULL
                            OR application_version != %s
                       )
                       AND (
                            name = 'okg_publisher_cycle'
                            OR name = 'okg_source_sync'
                            OR name LIKE 'okg_source_poll_%%'
                       )
                     FOR UPDATE
                ), updated AS (
                    UPDATE dbos.workflow_status AS workflow_status
                       SET application_version = %s
                      FROM candidates
                     WHERE workflow_status.workflow_uuid =
                           candidates.workflow_uuid
                     RETURNING workflow_status.workflow_uuid,
                               workflow_status.name,
                               candidates.old_application_version
                )
                SELECT workflow_uuid, name, old_application_version
                  FROM updated
                """,
                (queue_name, live_version, live_version),
            )
            rows.extend(cur.fetchall())
        conn.commit()
    return {
        "stamped_count": len(rows),
        "stamped_sample": [
            {
                "workflow_uuid": row[0],
                "name": row[1],
                "old_application_version": row[2],
            }
            for row in rows[:5]
        ],
    }


def cleanup_stale_pending_workflows(
    *,
    dsn: str,
    deployment: str,
    stale_after_seconds: int = 600,
    recovered_wakeup_stale_after_seconds: int = 300,
    cancel_started_pending: bool = True,
) -> dict[str, int | list[dict]]:
    """Cancel PENDING workflows on the deployment's partition queue
    whose age is older than ``stale_after_seconds``.

    Background: when a worker is SIGKILL'd or otherwise dies
    mid-execution, the in-flight workflow stays at status='PENDING'.
    The next worker's DBOS recovery is supposed to resume it, but in
    practice this fails silently in several cases:

      * The dying worker had a different `application_version` than
        the new one (any code change between boots).
      * The workflow function isn't registered in the new boot (e.g.
        a conditional feature gate that flipped to False).
      * DBOS's recovery dispatcher itself is wedged (observed several
        times during the perf-restoration work).

    With `concurrency=1` on the partition queue, ONE such zombie PENDING
    blocks all subsequent dispatch. This helper runs before the worker is
    launched. A recovered workflow is not stale just because it waited in
    the queue before DBOS actually started it. The short recovered-wakeup
    window applies only to recovered rows that never restarted execution;
    once `started_at_epoch_ms` is present, use the normal stale window.
    That prevents maintenance from cancelling a legitimate long source-sync
    run just because DBOS does not update the workflow row during the source
    stage. Automatic boot and post-launch maintenance both pass
    ``cancel_started_pending=False`` so they only cancel never-started
    recovered wakeups. A diagnosed abandoned started run remains an explicit
    operator decision through ``okg db dbos-gc ... --confirm --apply``.

    Returns counts + a sample of the cancelled workflow UUIDs for
    operator log review.
    """
    import time as _time
    cutoff_ms = int((_time.time() - stale_after_seconds) * 1000)
    recovered_cutoff_ms = int(
        (_time.time() - recovered_wakeup_stale_after_seconds) * 1000
    )
    started_pending_guard = (
        ""
        if cancel_started_pending
        else " AND started_at_epoch_ms IS NULL /* okg_no_started_pending_cancel */ "
    )
    with db_connect(dsn) as conn, \
            conn.cursor() as cur:
        cancelled = []
        for queue_name in _runtime_queue_names(deployment):
            cur.execute(
                "UPDATE dbos.workflow_status "
                "   SET status = 'CANCELLED' "
                " WHERE queue_name = %s "
                "   AND status = 'PENDING' "
                "   AND ("
                "        (GREATEST("
                "           updated_at, "
                "           COALESCE(created_at, updated_at), "
                "           COALESCE(started_at_epoch_ms, updated_at)"
                "         ) < %s "
                f"{started_pending_guard}"
                "        ) "
                "        OR ("
                "          recovery_attempts > 0 "
                "          AND started_at_epoch_ms IS NULL "
                "          AND COALESCE(created_at, updated_at) < %s "
                "          AND GREATEST("
                "            updated_at, "
                "            COALESCE(started_at_epoch_ms, updated_at)"
                "          ) < %s "
                "          AND ("
                "            name = 'okg_publisher_cycle' "
                "            OR name = 'okg_source_sync' "
                "            OR name LIKE 'okg_source_poll_%%'"
                "          )"
                "        )"
                "       ) "
                "RETURNING workflow_uuid, name, "
                "          application_version, updated_at, created_at, "
                "          started_at_epoch_ms, "
                "          GREATEST("
                "            updated_at, "
                "            COALESCE(created_at, updated_at), "
                "            COALESCE(started_at_epoch_ms, updated_at)"
                "          ) AS stale_anchor_ms",
                (
                    queue_name,
                    cutoff_ms,
                    recovered_cutoff_ms,
                    recovered_cutoff_ms,
                ),
            )
            cancelled.extend(cur.fetchall())
        conn.commit()
    now_ms = int(_time.time() * 1000)
    return {
        "cancelled_count": len(cancelled),
        "stale_after_seconds": stale_after_seconds,
        "recovered_wakeup_stale_after_seconds": (
            recovered_wakeup_stale_after_seconds
        ),
        "cancel_started_pending": bool(cancel_started_pending),
        # Cap to a handful for operator log readability.
        "cancelled_sample": [
            {
                "workflow_uuid": row[0],
                "name": row[1],
                "application_version": row[2],
                "stale_seconds": int(
                    (now_ms - row[6]) / 1000
                ) if row[6] else None,
                "updated_stale_seconds": int(
                    (now_ms - row[3]) / 1000
                ) if row[3] else None,
                "workflow_age_seconds": int(
                    (now_ms - row[6]) / 1000
                ) if row[6] else None,
                "started_age_seconds": int(
                    (now_ms - row[5]) / 1000
                ) if row[5] else None,
            }
            for row in cancelled[:5]
        ],
    }


def cleanup_stale_enqueued_workflows(
    *,
    dsn: str,
    deployment: str,
    stale_after_seconds: int = 300,
) -> dict[str, int | list[dict]]:
    """Cancel stale replaceable wakeups on the deployment queue.

    Source-sync events and source-poll workflows are wakeups: source state,
    cursors, and record sets remain the truth. Publisher cycles are likewise
    periodic maintenance ticks. If a stopped worker leaves old ENQUEUED rows
    behind, a new worker can spend its single-concurrency partition queue on
    stale wakeups before it reaches current work. Cancel only substrate-owned,
    replaceable wakeup workflows here; do not touch arbitrary deployment
    workflows.
    """
    import time as _time
    cutoff_ms = int((_time.time() - stale_after_seconds) * 1000)
    with db_connect(dsn) as conn, \
            conn.cursor() as cur:
        cancelled = []
        for queue_name in _runtime_queue_names(deployment):
            cur.execute(
                "UPDATE dbos.workflow_status "
                "   SET status = 'CANCELLED' "
                " WHERE queue_name = %s "
                "   AND status = 'ENQUEUED' "
                "   AND updated_at < %s "
                "   AND ("
                "        name != 'okg_source_sync' "
                "        OR COALESCE(priority, 100) >= 100"
                "       ) "
                "   AND ("
                "        name = 'okg_publisher_cycle' "
                "        OR name = 'okg_source_sync' "
                "        OR name LIKE 'okg_source_poll_%%'"
                "       ) "
                "RETURNING workflow_uuid, name, application_version, "
                "          created_at, updated_at, deduplication_id",
                (queue_name, cutoff_ms),
            )
            cancelled.extend(cur.fetchall())
        conn.commit()
    now_ms = int(_time.time() * 1000)
    return {
        "cancelled_count": len(cancelled),
        "stale_after_seconds": stale_after_seconds,
        "cancelled_sample": [
            {
                "workflow_uuid": row[0],
                "name": row[1],
                "application_version": row[2],
                "age_seconds": int(
                    (now_ms - (row[3] or row[4])) / 1000
                ) if (row[3] or row[4]) else None,
                "deduplication_id": row[5],
            }
            for row in cancelled[:5]
        ],
    }


def cleanup_stale_internal_pending_workflows(
    *,
    dsn: str,
    stale_after_seconds: int = 600,
) -> dict[str, int | list[dict]]:
    """Cancel stale PENDING workflows on DBOS's internal queue.

    Scheduled workflow wrappers run on ``_dbos_internal_queue`` before
    enqueueing real deployment work. If a worker dies after DBOS marks
    one of those wrappers PENDING, later short-lived operator workers
    can otherwise wait forever for old scheduler residue that DBOS no
    longer recovers.

    Fresh PENDING rows are left alone; legitimate in-flight scheduler
    work gets the same 10-minute grace period as deployment-queue
    PENDING cleanup.
    """
    import time as _time
    cutoff_ms = int((_time.time() - stale_after_seconds) * 1000)
    with db_connect(dsn) as conn, \
            conn.cursor() as cur:
        cur.execute(
            "UPDATE dbos.workflow_status "
            "   SET status = 'CANCELLED' "
            " WHERE queue_name = %s "
            "   AND status = 'PENDING' "
            "   AND updated_at < %s "
            "RETURNING workflow_uuid, name, "
            "          application_version, updated_at",
            ("_dbos_internal_queue", cutoff_ms),
        )
        cancelled = cur.fetchall()
        conn.commit()
    return {
        "cancelled_count": len(cancelled),
        "stale_after_seconds": stale_after_seconds,
        "cancelled_sample": [
            {
                "workflow_uuid": row[0],
                "name": row[1],
                "application_version": row[2],
                "stale_seconds": int(
                    (_time.time() * 1000 - row[3]) / 1000
                ) if row[3] else None,
            }
            for row in cancelled[:5]
        ],
    }
