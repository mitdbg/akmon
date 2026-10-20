"""DBOS scheduled enqueue for the escalation timeout-sweep workflow.

Per ``deepen-dbos-integration`` Phase 0: every minute, enqueue
one ``okg_escalation_timeout_sweep`` workflow onto the
deployment's partition queue. A 1-minute cron is deliberate:
escalation TTLs are typically minutes-to-hours, so a 1-minute
sweep granularity matches the resolution operators expect when
they say "this should time out after 10 minutes." A coarser
cron would push observed timeouts later than the declared TTL
by up to the cron interval.

Post-Wave-D-20 cleanup: rewritten as a thin wrapper around
``runtime._schedule_factory.make_enqueue_schedule``; see that
module for the shared enqueue template.
"""
from __future__ import annotations

from okg.substrate.runtime._schedule_factory import (
    make_enqueue_schedule,
)


__all__ = [
    "ESCALATION_SWEEP_SCHEDULE_ID",
    "ESCALATION_SWEEP_DEFAULT_CRON",
    "apply_escalation_sweep_schedule",
    "register_escalation_sweep_schedule",
]


ESCALATION_SWEEP_SCHEDULE_ID = "okg_escalation_timeout_sweep"
ESCALATION_SWEEP_DEFAULT_CRON = "*/1 * * * *"


(
    register_escalation_sweep_schedule,
    apply_escalation_sweep_schedule,
    _DECORATED,  # noqa: F401
) = make_enqueue_schedule(
    schedule_id=ESCALATION_SWEEP_SCHEDULE_ID,
    default_cron=ESCALATION_SWEEP_DEFAULT_CRON,
    workflow_module="okg.substrate.runtime.escalation_sweep_workflow",
    workflow_name_attr="ESCALATION_SWEEP_WORKFLOW_NAME",
    log_label="escalation sweep",
)
