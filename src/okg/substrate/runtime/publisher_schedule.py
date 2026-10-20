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
"""
from __future__ import annotations

from datetime import datetime

from okg.substrate.runtime._schedule_factory import (
    make_enqueue_schedule,
)


__all__ = [
    "PUBLISHER_SCHEDULE_ID",
    "PUBLISHER_DEFAULT_CRON",
    "PUBLISHER_WORKFLOW_TIMEOUT_SECONDS",
    "apply_publisher_schedule",
    "publisher_blocking_source_sync_workflow_id",
    "publisher_deduplication_id",
    "register_publisher_schedule",
]


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


def publisher_blocking_source_sync_workflow_id(
    *,
    deployment: str,
    dsn: str,
    workflow_name: str,  # noqa: ARG001 - shared schedule hook signature
    scheduled_time: datetime | None = None,  # noqa: ARG001
) -> str | None:
    """Return an active source-sync workflow that should own publishing."""
    from okg.substrate.runtime.dbos_enqueue import active_workflow_id_for_name
    from okg.substrate.runtime.partition_queue import source_sync_queue_name
    from okg.substrate.runtime.source_sync_constants import (
        SOURCE_SYNC_WORKFLOW_NAME,
    )

    return active_workflow_id_for_name(
        dsn=dsn,
        queue_name=source_sync_queue_name(deployment),
        workflow_name=SOURCE_SYNC_WORKFLOW_NAME,
    )


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
