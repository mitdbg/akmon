"""DBOS scheduled enqueue for the declared-attribute index maintenance.

Every five minutes, enqueue one ``okg_declared_attr_index_maintenance``
workflow onto the deployment's index-maintenance queue
(``okg-<deployment>-index-maintenance``), unless one is already enqueued or
running there. okg#3112 step 2, design section 4.4.
"""
from __future__ import annotations

from okg.substrate.runtime._schedule_factory import (
    make_enqueue_schedule,
)
from okg.substrate.runtime.partition_queue import index_maintenance_queue_name


__all__ = [
    "DECLARED_ATTR_INDEX_SCHEDULE_ID",
    "DECLARED_ATTR_INDEX_DEFAULT_CRON",
    "apply_declared_attr_index_schedule",
    "register_declared_attr_index_schedule",
]


DECLARED_ATTR_INDEX_SCHEDULE_ID = "okg_declared_attr_index_maintenance"
DECLARED_ATTR_INDEX_DEFAULT_CRON = "*/5 * * * *"


(
    register_declared_attr_index_schedule,
    apply_declared_attr_index_schedule,
    _DECORATED,  # noqa: F401
) = make_enqueue_schedule(
    schedule_id=DECLARED_ATTR_INDEX_SCHEDULE_ID,
    default_cron=DECLARED_ATTR_INDEX_DEFAULT_CRON,
    workflow_module="okg.substrate.runtime.declared_attr_index_workflow",
    workflow_name_attr="DECLARED_ATTR_INDEX_WORKFLOW_NAME",
    log_label="declared attr index maintenance",
    queue_name_fn=index_maintenance_queue_name,
)
