"""Per-deployment queue registration.

Decision 5 of `rebuild-substrate-on-dbos` retires
`coordination/lease.py` in favor of a DBOS queue with
``concurrency=1`` per deployment. The mutual-exclusion property:
only one workflow consumes from the queue at a time. With one queue
per deployment, deployment A's workflows do not block deployment B's
— the per-deployment naming (``okg-<deployment>``) provides isolation
without needing DBOS's ``partition_queue`` feature (which is a
sharding mechanism for partition-key-bearing workloads, not
deployment-isolation).

The queue needs to be **registered** with the DBOS singleton before
``DBOS.launch()``. Calling `Queue(name, concurrency=1)` is the
registration mechanism; the constructor wires the queue into the
singleton's queue manager. Subsequent enqueue calls (via DBOSClient
or DBOS-side helpers) find the queue by name and the singleton's
worker threads consume from it.

Phase 1 §1.1.3 of `migrate-substrate-runtime-to-dbos`: the worker
entry point calls :func:`register_deployment_partition_queue` after
``DBOS()`` is constructed and before ``DBOS.launch()``. Idempotent
across worker restarts in the same process.

Naming note: the function is still called ``register_deployment_partition_queue``
for continuity with the umbrella's "partition queue" terminology
(meaning "per-deployment queue"), even though the DBOS ``partition_queue=True``
feature is NOT used. Renaming the function would churn callers for
no operational benefit; the docstring is the canonical reference.
"""
from __future__ import annotations

import logging
from typing import Any


__all__ = [
    "SOURCE_SYNC_OPERATOR_PRIORITY",
    "SOURCE_SYNC_PROFILE_PRIORITY",
    "SOURCE_SYNC_ROUTINE_PRIORITY",
    "deployment_runtime_queue_names",
    "index_maintenance_queue_name",
    "partition_queue_name",
    "register_deployment_partition_queue",
    "register_index_maintenance_queue",
    "register_source_sync_partition_queue",
    "source_sync_enqueue_priority",
    "source_sync_queue_name",
]


log = logging.getLogger(__name__)


# Module-level cache so re-registration in the same process (e.g.
# test fixtures) doesn't create duplicate Queue objects.
_REGISTERED: dict[str, Any] = {}


SOURCE_SYNC_PROFILE_PRIORITY = 1
SOURCE_SYNC_OPERATOR_PRIORITY = 10
SOURCE_SYNC_READINESS_PRIORITY = 20
SOURCE_SYNC_ROUTINE_PRIORITY = 100


def source_sync_enqueue_priority(
    *,
    operator_requested: bool = False,
    profile_force_deep: bool = False,
    profile_collectors: tuple[str, ...] | list[str] = (),
    readiness_recovery: bool = False,
) -> int:
    """Return DBOS source-sync queue priority for a workflow enqueue.

    DBOS orders lower priority values first when the queue has priority enabled.
    Priority 0 is DBOS's implicit default, so OKG uses explicit positive values
    that keep operator profile captures ahead of routine poll/watch work while
    preserving the source-sync lane's concurrency=1 mutation contract.
    """
    if profile_force_deep or profile_collectors:
        return SOURCE_SYNC_PROFILE_PRIORITY
    if operator_requested:
        return SOURCE_SYNC_OPERATOR_PRIORITY
    if readiness_recovery:
        return SOURCE_SYNC_READINESS_PRIORITY
    return SOURCE_SYNC_ROUTINE_PRIORITY


def partition_queue_name(deployment: str) -> str:
    """The canonical control-queue name for a deployment.

    Convention: ``okg-<deployment>``. Used by:

      * `publisher_schedule._enqueue_publisher_workflow` (enqueue side).
      * `hooks.enqueue_hooks` (enqueue side).
      * `admin.client_drain` (poll side).
      * This module's `register_deployment_partition_queue` (consume
        side).

    Changing the convention requires updating all four; keep the
    constant here as the single source of truth.
    """
    return f"okg-{deployment}"


def source_sync_queue_name(deployment: str) -> str:
    """The source-sync lane for a deployment.

    Source-sync wakeups are replaceable and can be high volume. Keeping them
    off the control queue prevents one stale ``okg_source_sync`` workflow from
    blocking publisher, admission, doctor, and recovery/control work.
    """
    return f"okg-{deployment}-source-sync"


def index_maintenance_queue_name(deployment: str) -> str:
    """The index-maintenance lane for a deployment (okg#3112 step 2).

    The declared-attribute index reconciler waits up to ten minutes for a
    ``CREATE INDEX CONCURRENTLY``; on its own queue that wait never holds the
    control queue, so publishes keep running beside it.
    """
    return f"okg-{deployment}-index-maintenance"


def deployment_runtime_queue_names(deployment: str) -> tuple[str, ...]:
    """Return all substrate-owned DBOS queues for a deployment."""
    return (
        partition_queue_name(deployment),
        source_sync_queue_name(deployment),
        index_maintenance_queue_name(deployment),
    )


def register_deployment_partition_queue(
    *,
    deployment: str,
    concurrency: int = 1,
) -> Any:
    """Register the deployment's partition queue with the DBOS
    singleton.

    Must be called BEFORE ``DBOS.launch()``. The Queue constructor
    wires the queue into the singleton's queue manager; after launch,
    the singleton's worker threads consume from it.

    Idempotent: a second call with the same deployment returns the
    cached Queue instance rather than constructing a duplicate.

    Parameters
    ----------
    deployment : str
        Deployment name; the queue name is ``okg-<deployment>``.
    concurrency : int, default 1
        Maximum simultaneous workflows consumed from this queue.
        Default 1 is the publisher's mutual-exclusion contract; raise
        it only when you're sure the workload supports parallel
        execution.
    """
    name = partition_queue_name(deployment)
    if name in _REGISTERED:
        log.debug(
            "partition queue %r already registered; reusing", name,
        )
        return _REGISTERED[name]
    return _register_queue(name=name, concurrency=concurrency)


def register_source_sync_partition_queue(
    *,
    deployment: str,
    concurrency: int = 1,
) -> Any:
    """Register the deployment's source-sync lane."""
    return _register_queue(
        name=source_sync_queue_name(deployment),
        concurrency=concurrency,
        priority_enabled=True,
    )


def register_index_maintenance_queue(
    *,
    deployment: str,
    concurrency: int = 1,
) -> Any:
    """Register the deployment's index-maintenance lane (concurrency 1: one
    index-DDL actor per database)."""
    return _register_queue(
        name=index_maintenance_queue_name(deployment),
        concurrency=concurrency,
    )


def _register_queue(
    *,
    name: str,
    concurrency: int,
    priority_enabled: bool = False,
) -> Any:
    if name in _REGISTERED:
        log.debug("partition queue %r already registered; reusing", name)
        return _REGISTERED[name]

    from dbos import Queue

    # polling_interval_sec drives the queue-worker thread's dequeue
    # cadence. Default is 1.0s; at p50 that's a ~500ms scheduling
    # latency between enqueue and step 1 actually starting — directly
    # observed via `dbos.operation_outputs.started_at_epoch_ms` minus
    # `dbos.workflow_status.created_at`. For the publish path where
    # an event-driven workflow needs to start promptly, 50ms is the
    # right cadence: low enough that p95 dispatch is well under
    # 100ms, high enough that the idle CPU cost is negligible
    # (one cheap SELECT every 50ms). See .scratch/dbos_framework_gap.py
    # for the empirical measurement that motivated this value.
    queue = Queue(
        name,
        concurrency=concurrency,
        priority_enabled=priority_enabled,
        polling_interval_sec=0.05,
    )
    _REGISTERED[name] = queue
    log.info(
        "registered deployment queue: name=%s concurrency=%d",
        name, concurrency,
    )
    return queue


# `_clear_registry` (test-only cache-reset helper) deleted —
# zero callers in src/ or tests/.
