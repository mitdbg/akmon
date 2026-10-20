"""Runtime health policy shared by readiness and dogfood surfaces."""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

RUNTIME_HEALTH_POLICY_SCHEMA = "okg.runtime_health_policy.v1"

_POLICY: dict[str, Any] = {
    "schema": RUNTIME_HEALTH_POLICY_SCHEMA,
    "source_sync_backlog": {
        "fresh_active_work": {
            "readiness": "nonblocking",
            "operator_action": "monitor",
            "safe_gc": False,
            "description": (
                "PENDING/ENQUEUED/DELAYED source-sync work below the stale "
                "window is expected transient DBOS work."
            ),
        },
        "running_pending_work": {
            "readiness": "nonblocking",
            "operator_action": "monitor_worker_liveness",
            "safe_gc": False,
            "description": (
                "A started PENDING DBOS row is treated as running until a "
                "stronger runtime signal proves it is abandoned."
            ),
        },
        "stale_replaceable_work": {
            "readiness": "blocker",
            "operator_action": "dry_run_then_confirm_apply",
            "safe_gc": True,
            "dry_run_command_key": "source_sync_dbos_gc_dry_run",
            "apply_command_key": "source_sync_dbos_gc_apply",
            "description": (
                "Old replaceable source-sync wakeups block readiness and may "
                "be cancelled by DBOS GC after dry-run review."
            ),
        },
        "terminal_history": {
            "readiness": "nonblocking",
            "operator_action": "report",
            "safe_gc": False,
            "description": (
                "Historical SUCCESS and benign CANCELLED/ERROR rows remain "
                "visible as DBOS audit history and do not count as active work."
            ),
        },
        "failed_terminal_history": {
            "readiness": "blocker",
            "operator_action": "operator_acknowledge_or_fix",
            "safe_gc": False,
            "description": (
                "Current-version failed terminal source-sync rows are DLQ-like "
                "history and block readiness until acknowledged or fixed."
            ),
        },
    },
}


def runtime_health_policy(
    *,
    commands: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Return a copy of the runtime health policy.

    ``commands`` resolves command-key fields into concrete deployment-specific
    command strings for operator-facing dogfood payloads.
    """
    policy = deepcopy(_POLICY)
    if commands:
        _resolve_commands(policy, commands)
    return policy


def _resolve_commands(
    value: Any,
    commands: Mapping[str, str],
) -> None:
    if isinstance(value, dict):
        dry_key = value.get("dry_run_command_key")
        if isinstance(dry_key, str) and commands.get(dry_key):
            value["dry_run_command"] = commands[dry_key]
        apply_key = value.get("apply_command_key")
        if isinstance(apply_key, str) and commands.get(apply_key):
            value["apply_command"] = commands[apply_key]
        for child in value.values():
            _resolve_commands(child, commands)
    elif isinstance(value, list):
        for child in value:
            _resolve_commands(child, commands)


__all__ = [
    "RUNTIME_HEALTH_POLICY_SCHEMA",
    "runtime_health_policy",
]
