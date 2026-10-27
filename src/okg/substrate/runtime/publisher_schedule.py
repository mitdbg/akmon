"""DBOS scheduled enqueue for the publisher workflow.

Replaces the legacy ``okg-publish`` pg_cron entry. A
``@DBOS.scheduled`` workflow fires on a per-deployment cron and
enqueues the publisher workflow onto the deployment's partition
queue. The partition queue's ``concurrency=1`` provides the
mutual-exclusion the lease used to.

Post-Wave-D-20 cleanup: rewritten as a thin wrapper around
``runtime._schedule_factory.make_enqueue_schedule``; see that
module for the shared enqueue template. Publisher passes a
non-None ``workflow_timeout_seconds`` (1800s = 30 min) so the
factory adds ``workflow_timeout`` to its enqueue payload.

Post-merge-dev-into-cleanup-wave (2026-05-28): also passes
``deduplication_id_fn=publisher_deduplication_id`` so the
factory routes through ``dbos_enqueue.enqueue_deployment_workflow``
and collapses bursty cron fires into one active workflow per
2-minute bucket (per dev's workspace-stabilization wave).

PACT publisher-starvation-4039: the tick yields to a queued or running
source sync only while the publisher is *current*, meaning the newest of its
last non-skipped cycle and the branch's newest published generation is
younger than ``publisher.max_source_sync_yield_seconds``. Past that bound the
tick enqueues the cycle, and the cycle publishes past the sync's workflow rows
(``publisher_workflow._run_one_cycle``), keeping every other guard.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from okg.substrate.runtime._schedule_factory import (
    make_enqueue_schedule,
)


__all__ = [
    "PUBLISHER_SCHEDULE_ID",
    "PUBLISHER_DEFAULT_CRON",
    "PUBLISHER_WORKFLOW_TIMEOUT_SECONDS",
    "PublisherFreshness",
    "apply_publisher_schedule",
    "generation_cut_at",
    "publisher_blocking_source_sync_workflow_id",
    "publisher_deduplication_id",
    "publisher_yield_bound_seconds",
    "read_publisher_freshness",
    "register_publisher_schedule",
    "source_sync_workflow_row",
]


log = logging.getLogger(__name__)


PUBLISHER_SCHEDULE_ID = "okg_publisher_cycle"
# Same cadence as the legacy ``okg-publish`` pg_cron entry (every
# 30 seconds). Manifest-overridable via
# ``runtime.schedules.okg_publisher_cycle: "*/2 * * * *"`` etc.
PUBLISHER_DEFAULT_CRON = "*/30 * * * * *"  # 6-field: seconds resolution

# Per-workflow timeout. DBOS marks a workflow as CANCELLED if its
# deadline passes; the worker thread keeps running until the body
# returns, but at least the workflow_status row reflects reality
# and the operator can see the stuck cycle. 30 minutes is much
# larger than any observed legitimate cycle (~115s on
# okg-workspace at 187K nodes; expect proportionally larger on
# cms/memex but still < 10 min) and small enough to give a clear
# "this is broken" signal. The complementary in-process fixes
# (psycopg.connect_timeout, project_batch's statement_timeout)
# address the actual hang root cause — connect-time TCP stalls
# and unbounded single-SQL stalls inside pool workers.
PUBLISHER_WORKFLOW_TIMEOUT_SECONDS = 1800.0

# Outcome ``details`` key naming the branch a cycle ran on.
BRANCH_DETAIL_KEY = "branch_name"


def publisher_deduplication_id(
    deployment: str,
    *,
    scheduled_time: datetime | None = None,
    bucket_seconds: int = 120,
) -> str:
    """Build the dedup key the scheduler hands to DBOS so bursty
    cron fires collapse into one active workflow per bucket.

    Returned shape: ``publisher:<deployment>:cycle[:<bucket>]``.
    Each cron tick computes ``bucket = floor(scheduled_time.ts /
    bucket_seconds)`` so adjacent ticks share the same key and DBOS
    enqueue is deduplicated. The 2-minute default matches dev's
    workspace-stabilization choice.
    """
    base = f"publisher:{deployment}:cycle"
    if scheduled_time is None:
        return base
    bucket = int(scheduled_time.timestamp() / max(bucket_seconds, 1))
    return f"{base}:{bucket}"


def publisher_yield_bound_seconds(deployment: str) -> float:
    """``publisher.max_source_sync_yield_seconds`` of the deployment manifest.

    A malformed value was refused when the manifest loaded; this never
    defaults a value that failed to load.
    """
    from okg.substrate.deployments import resolve_deployment

    return float(
        resolve_deployment(deployment).publisher.max_source_sync_yield_seconds
    )


@dataclass(frozen=True)
class PublisherFreshness:
    """How current the scheduled publisher is on one branch.

    ``last_cycle_at`` is the end of the newest publisher cycle whose outcome is
    not ``skipped`` (a publish, a blocked checkpoint or a no-op that confirmed
    the graph is current). ``last_generation_at`` is the commit time of the
    branch's newest published generation, which also counts a source sync's own
    publish. A skip is a DBOS SUCCESS but proves neither, so it never counts.
    """

    deployment: str
    branch_name: str
    bound_seconds: float
    last_cycle_at: datetime | None
    last_generation_at: datetime | None
    checked_at: datetime

    @property
    def freshness_at(self) -> datetime | None:
        moments = [
            moment for moment in (self.last_cycle_at, self.last_generation_at)
            if moment is not None
        ]
        return max(moments) if moments else None

    @property
    def has_generation(self) -> bool:
        """False while the branch has no published generation, the one state
        in which the run-base guard cannot see a run (design section 2)."""
        return self.last_generation_at is not None

    @property
    def age_seconds(self) -> float | None:
        moment = self.freshness_at
        if moment is None:
            return None
        return max(0.0, (self.checked_at - moment).total_seconds())

    @property
    def overdue(self) -> bool:
        age = self.age_seconds
        return age is None or age > self.bound_seconds

    def describe(self) -> str:
        age = self.age_seconds
        return (
            f"freshness_age_seconds="
            f"{'never' if age is None else f'{age:.0f}'} "
            f"bound_seconds={self.bound_seconds:g}"
        )


def _as_utc(moment: datetime | str) -> datetime:
    """A timestamp as an aware UTC datetime. The checkpoint ledger hands back
    ``created_at`` as ISO text on some backends and as a datetime on others."""
    if isinstance(moment, str):
        moment = datetime.fromisoformat(moment)
    return (
        moment.replace(tzinfo=timezone.utc)
        if moment.tzinfo is None else moment.astimezone(timezone.utc)
    )


def generation_cut_at(generation: object) -> datetime:
    """When a published generation's checkpoint was cut, as aware UTC."""
    return _as_utc(generation.created_at)  # type: ignore[attr-defined]


def read_publisher_freshness(
    *,
    deployment: str,
    dsn: str,
    branch_name: str = "default",
    bound_seconds: float | None = None,
    now: datetime | None = None,
) -> PublisherFreshness:
    """Read the publisher's freshness from durable rows.

    Two reads, one row each: the newest publisher outcome that is not a skip
    (walks the registrations' ``deployment, workflow, queue, registered_at``
    index newest first and stops at the first non-skip) and the
    branch's newest published generation (the checkpoint ledger's own bounded
    walk). Neither reads ``dbos.workflow_status``. A failed read raises.
    """
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.runtime.partition_queue import partition_queue_name
    from okg.substrate.runtime.workflow_operational_outcomes import (
        PUBLISHER_WORKFLOW_NAME,
    )

    bound = (
        publisher_yield_bound_seconds(deployment)
        if bound_seconds is None else float(bound_seconds)
    )
    with db_connect(dsn, connect_timeout=5, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT o.recorded_at
                  FROM okg.workflow_operational_registrations r
                  JOIN okg.workflow_operational_outcomes o USING (workflow_uuid)
                 WHERE r.deployment_name = %s
                   AND r.workflow_name = %s
                   AND r.queue_name = %s
                   AND o.outcome <> 'skipped'
                   AND COALESCE(o.details ->> %s, %s) = %s
                 ORDER BY r.registered_at DESC
                 LIMIT 1
                """,
                (
                    deployment, PUBLISHER_WORKFLOW_NAME,
                    partition_queue_name(deployment),
                    BRANCH_DETAIL_KEY, "default", branch_name,
                ),
            )
            row = cur.fetchone()
        last_cycle_at = _as_utc(row[0]) if row is not None else None
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        latest = store.latest_generation(
            deployment_name=deployment, branch=branch_name, status="published",
        )
    last_generation_at = (
        generation_cut_at(latest) if latest is not None else None
    )
    return PublisherFreshness(
        deployment=deployment,
        branch_name=branch_name,
        bound_seconds=bound,
        last_cycle_at=last_cycle_at,
        last_generation_at=last_generation_at,
        checked_at=now if now is not None else datetime.now(timezone.utc),
    )


def source_sync_workflow_row(
    *, dsn: str, workflow_id: str,
) -> tuple[str | None, float | None]:
    """Status and age in seconds of one workflow row, by primary key."""
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn, connect_timeout=5, autocommit=True) as conn:
        row = conn.execute(
            "SELECT status, created_at FROM dbos.workflow_status"
            " WHERE workflow_uuid = %s",
            (workflow_id,),
        ).fetchone()
    if row is None:
        return None, None
    status, created_at_ms = row
    age = (
        None if created_at_ms is None
        else max(
            0.0,
            datetime.now(timezone.utc).timestamp() - float(created_at_ms) / 1000.0,
        )
    )
    return (None if status is None else str(status)), age


def publisher_blocking_source_sync_workflow_id(
    *,
    deployment: str,
    dsn: str,
    workflow_name: str,  # noqa: ARG001 - shared schedule hook signature
    scheduled_time: datetime | None = None,  # noqa: ARG001
) -> str | None:
    """Return an active source-sync workflow that should own publishing.

    ``None`` (enqueue the cycle) when no sync row is active, or when the
    publisher is overdue: the newest of its last non-skipped cycle and the
    branch's newest published generation is older than
    ``publisher.max_source_sync_yield_seconds``. A yield logs at INFO; a
    forced enqueue logs at WARNING. Both name the sync, its status, the
    freshness age and the bound. A cycle already ENQUEUED or PENDING on the
    control queue stops the tick before this runs (the schedule factory).
    """
    from okg.substrate.runtime.dbos_enqueue import active_workflow_id_for_name
    from okg.substrate.runtime.partition_queue import source_sync_queue_name
    from okg.substrate.runtime.source_sync_constants import (
        SOURCE_SYNC_WORKFLOW_NAME,
    )

    blocking = active_workflow_id_for_name(
        dsn=dsn,
        queue_name=source_sync_queue_name(deployment),
        workflow_name=SOURCE_SYNC_WORKFLOW_NAME,
    )
    if blocking is None:
        return None
    freshness = read_publisher_freshness(deployment=deployment, dsn=dsn)
    status, age = source_sync_workflow_row(dsn=dsn, workflow_id=blocking)
    detail = (
        f"deployment={deployment} source_sync_workflow_id={blocking} "
        f"source_sync_status={status} "
        f"source_sync_age_seconds={'unknown' if age is None else f'{age:.0f}'} "
        f"{freshness.describe()}"
    )
    if freshness.overdue:
        log.warning(
            "scheduled publisher overdue; enqueueing the cycle past an "
            "active source sync: %s", detail,
        )
        return None
    log.info("scheduled publisher yields to an active source sync: %s", detail)
    return blocking


(
    register_publisher_schedule,
    apply_publisher_schedule,
    _DECORATED,  # noqa: F401
) = make_enqueue_schedule(
    schedule_id=PUBLISHER_SCHEDULE_ID,
    default_cron=PUBLISHER_DEFAULT_CRON,
    workflow_module="okg.substrate.runtime.publisher_workflow",
    workflow_name_attr="PUBLISHER_WORKFLOW_NAME",
    log_label="publisher",
    workflow_timeout_seconds=PUBLISHER_WORKFLOW_TIMEOUT_SECONDS,
    deduplication_id_fn=publisher_deduplication_id,
    active_blocking_workflow_id_fn=(
        publisher_blocking_source_sync_workflow_id
    ),
)
