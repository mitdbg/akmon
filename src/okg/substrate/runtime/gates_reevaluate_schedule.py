"""DBOS scheduled enqueue for the gates-reevaluate workflow.

Per ``harden-runtime-supervisor`` §B. Fires every 30 minutes by
default; manifest-overridable via
``runtime.schedules.okg_runtime_gates_reevaluate``.

Post-Wave-D-20 cleanup: rewritten as a thin wrapper around
``runtime._schedule_factory.make_enqueue_schedule``; see that
module for the shared enqueue template. Retains the
``reset_decorated_cache_for_tests`` helper at module top-level
for tests that import it directly.
"""
from __future__ import annotations

from okg.substrate.runtime._schedule_factory import (
    make_enqueue_schedule,
)


__all__ = [
    "GATES_REEVALUATE_SCHEDULE_ID",
    "GATES_REEVALUATE_DEFAULT_CRON",
    "register_gates_reevaluate_schedule",
    "apply_gates_reevaluate_schedule",
    "reset_decorated_cache_for_tests",
]


GATES_REEVALUATE_SCHEDULE_ID = "okg_runtime_gates_reevaluate"
GATES_REEVALUATE_DEFAULT_CRON = "*/30 * * * *"


(
    register_gates_reevaluate_schedule,
    apply_gates_reevaluate_schedule,
    _DECORATED,
) = make_enqueue_schedule(
    schedule_id=GATES_REEVALUATE_SCHEDULE_ID,
    default_cron=GATES_REEVALUATE_DEFAULT_CRON,
    workflow_module="okg.substrate.runtime.gates_reevaluate_workflow",
    workflow_name_attr="GATES_REEVALUATE_WORKFLOW_NAME",
    log_label="gates-reevaluate",
)


def reset_decorated_cache_for_tests() -> None:
    """Clear the cache so a fresh worker boot picks up a new
    decorator binding. Retained as a public test-only helper for
    tests that import it from this module."""
    _DECORATED.clear()
