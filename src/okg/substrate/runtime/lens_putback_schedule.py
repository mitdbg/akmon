"""DBOS scheduled enqueue for the lens-putback sweep workflow.

Per ``deepen-dbos-integration`` Phase 0, the substrate runs a
``@DBOS.scheduled`` cron that enqueues ``okg_lens_putback_sweep``
onto the deployment's partition queue every 5 minutes (default).
The partition queue's ``concurrency=1`` serializes the sweep
against publisher cycles and other substrate workflows on the
same queue — deliberate; lens-putback writes touch the same
lens-overlay tables a publisher cycle could read during
projection.

Post-Wave-D-20 cleanup: the 4 substrate schedule modules
(publisher, lens_putback, escalation_sweep, gates_reevaluate)
shared ~140 LOC each of nearly-identical enqueue-template code.
Collapsed to a one-call wrapper around
``runtime._schedule_factory.make_enqueue_schedule``.
"""
from __future__ import annotations

from okg.substrate.runtime._schedule_factory import (
    make_enqueue_schedule,
)


__all__ = [
    "LENS_PUTBACK_SCHEDULE_ID",
    "LENS_PUTBACK_DEFAULT_CRON",
    "apply_lens_putback_schedule",
    "register_lens_putback_schedule",
]


LENS_PUTBACK_SCHEDULE_ID = "okg_lens_putback_sweep"
# Every 5 minutes. Sweep cost is bounded by the count of stale
# `prepared` rows, which is typically zero — operators can pull
# this down to `*/1 * * * *` if they have a high-volume putback
# workload.
LENS_PUTBACK_DEFAULT_CRON = "*/5 * * * *"


(
    register_lens_putback_schedule,
    apply_lens_putback_schedule,
    _DECORATED,  # exposed for the historical reset_decorated_cache_for_tests path  # noqa: F401
) = make_enqueue_schedule(
    schedule_id=LENS_PUTBACK_SCHEDULE_ID,
    default_cron=LENS_PUTBACK_DEFAULT_CRON,
    workflow_module="okg.substrate.runtime.lens_putback_workflow",
    workflow_name_attr="LENS_PUTBACK_WORKFLOW_NAME",
    log_label="lens-putback",
)
