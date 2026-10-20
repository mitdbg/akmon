"""Publisher-workflow health probe for `okg doctor` (Phase 1 §1.4).

Replaces the legacy `publisher_lease_held_too_long` doctor check.
That check looked for a stranded lease row; in DBOS mode there is no
lease — the partition queue handles mutual exclusion and a stranded
workflow is observable as a long-running entry in
``dbos.workflow_status``. The new check answers a different but
equivalent operator question: **"has the publisher actually completed
a cycle recently?"**

The probe queries `dbos.workflow_status` for the most-recent
``okg_publisher_cycle`` workflow for the deployment's partition queue
and flags two conditions as failures:

  1. **No completed cycle ever.** The deployment was bootstrapped but
     the scheduled enqueue has not yet produced a SUCCESS-status row.
     This is normal during the first few minutes after enabling
     `runtime.enabled: true`; it becomes a failure if it persists past
     the cron's first expected fire.
  2. **Most recent completion older than 2x the cron interval.** A
     successful cycle hasn't happened in two cron periods. Surfaces a
     stalled worker, a paused queue, or a cron-config drift.

The 2x-cron-interval threshold is deliberately conservative; one
missed tick can be transient (network blip, GC pause). Two missed
ticks consistently is the operator's signal.
"""
from __future__ import annotations


__all__ = [
    "PAUSED_LONG_SECONDS",
    "PAUSED_WARNING_CODES",
    "PublisherHealthVerdict",
    "QueueDepthVerdict",
    "LensPutbackBacklogVerdict",
    "EscalationOpenBacklogVerdict",
    "GenerationEventSubscribersVerdict",
    "DeploymentDoctorHealthVerdict",
    "RuntimeServicesHealthVerdict",
    "check_publisher_workflow_health",
    "check_source_sync_workflow_health",
    "check_embed_workflow_health",
    "check_queue_depth",
    "check_lens_putback_sweep_health",
    "check_lens_putback_backlog",
    "check_escalation_sweep_health",
    "check_escalation_open_backlog",
    "check_generation_event_subscribers",
    "check_deployment_doctor_health",
    "check_runtime_services_health",
    "read_worker_code_identity_overlay",
    "_DEFAULT_CRON_SECONDS",
    "_check_workflow_health",
    "_parse_cron_interval_seconds",
    "_try_doctor_verdict_overlay",
    "InternalQueueUnconsumedVerdict",
    "ManifestDsnResolvableVerdict",
    "check_internal_queue_unconsumed",
    "check_manifest_dsn_resolvable",
]


# ------------------------------------------------------
# Workflow-liveness probes moved to
# `publisher_health_workflow_liveness.py` by
# `extract-publisher-health-workflow-liveness`. Re-export
# at module-import time so doctor.py / scheduled.py /
# tests/* keep importing from publisher_health.
# ------------------------------------------------------
from okg.substrate.runtime.publisher_health_workflow_liveness import (  # noqa: F401, E402
    _DEFAULT_CRON_SECONDS,
    PAUSED_LONG_SECONDS,
    PAUSED_WARNING_CODES,
    PublisherHealthVerdict,
    _check_workflow_health,
    _parse_cron_interval_seconds,
    check_embed_workflow_health,
    check_escalation_sweep_health,
    check_lens_putback_sweep_health,
    check_publisher_workflow_health,
    check_source_sync_workflow_health,
)



# ------------------------------------------------------
# Backlog/queue probes moved to
# `publisher_health_backlog.py` by
# `extract-publisher-health-backlog`. Re-export at
# module-import time so doctor.py / tests/* keep
# importing from publisher_health.
# ------------------------------------------------------
from okg.substrate.runtime.publisher_health_backlog import (  # noqa: F401, E402
    EscalationOpenBacklogVerdict,
    GenerationEventSubscribersVerdict,
    LensPutbackBacklogVerdict,
    QueueDepthVerdict,
    check_escalation_open_backlog,
    check_generation_event_subscribers,
    check_lens_putback_backlog,
    check_queue_depth,
)


# ------------------------------------------------------
# Doctor-overlay probes moved to
# `publisher_health_doctor_overlay.py` by
# `extract-publisher-health-doctor-overlay`. Re-export
# at module-import time so doctor.py / tests/* keep
# importing from publisher_health.
# ------------------------------------------------------
from okg.substrate.runtime.publisher_health_doctor_overlay import (  # noqa: F401, E402
    DeploymentDoctorHealthVerdict,
    RuntimeServicesHealthVerdict,
    _try_doctor_verdict_overlay,
    check_deployment_doctor_health,
    check_runtime_services_health,
    read_worker_code_identity_overlay,
)


# ------------------------------------------------------
# Silent-failure backstop probes moved to
# `publisher_health_silent_failures.py` by
# `extract-publisher-health-silent-failures`. Re-export
# at module-import time so doctor.py's existing imports
# from publisher_health keep working.
# ------------------------------------------------------
from okg.substrate.runtime.publisher_health_silent_failures import (  # noqa: F401, E402
    InternalQueueUnconsumedVerdict,
    ManifestDsnResolvableVerdict,
    check_internal_queue_unconsumed,
    check_manifest_dsn_resolvable,
)
