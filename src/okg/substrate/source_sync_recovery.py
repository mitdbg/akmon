"""Shared source-sync readiness recovery helpers."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any


SOURCE_SYNC_RECOVERY_SCHEMA = "okg.source_sync.recovery_plan.v1"
SOURCE_SYNC_RECOVERY_STALE_AFTER_SECONDS = 600
SOURCE_SYNC_RECOVERY_WORKFLOW_NAME = "okg_source_sync"
# Only waiting work is replaceable. A PENDING row may still have a Python
# execution in flight, and a ledger update cannot interrupt that body.
SOURCE_SYNC_RECOVERY_STATUSES = ("ENQUEUED",)


def plan_stale_source_sync_work_recovery(
    *,
    deployment: str,
    dsn: str,
    source_sync_status: Mapping[str, Any],
) -> dict[str, Any]:
    """Dry-run stale replaceable source-sync wakeup recovery.

    Readiness and status surfaces must not mutate DBOS state. They can surface
    the same bounded candidate set that the explicit dbos-gc apply command
    would operate on.
    """
    if not source_sync_status_is_stale_dbos_only(source_sync_status):
        return {}
    queue_name = source_sync_recovery_queue_name(
        deployment,
        source_sync_dbos_workflows(source_sync_status),
    )
    try:
        from okg.substrate.db.management import dbos_backlog_gc

        report = dbos_backlog_gc(
            dsn,
            stale_after_seconds=SOURCE_SYNC_RECOVERY_STALE_AFTER_SECONDS,
            confirm=False,
            queue_name=queue_name,
            workflow_name=SOURCE_SYNC_RECOVERY_WORKFLOW_NAME,
            statuses=SOURCE_SYNC_RECOVERY_STATUSES,
            sample_limit=20,
        )
    except Exception as exc:  # noqa: BLE001 - readiness must surface recovery failures.
        return {
            "schema": SOURCE_SYNC_RECOVERY_SCHEMA,
            "attempted": True,
            "ok": False,
            "queue_name": queue_name,
            "workflow_name": SOURCE_SYNC_RECOVERY_WORKFLOW_NAME,
            "stale_after_seconds": SOURCE_SYNC_RECOVERY_STALE_AFTER_SECONDS,
            "error": {
                "type": type(exc).__name__,
                "message": str(exc),
            },
        }
    return {
        "schema": SOURCE_SYNC_RECOVERY_SCHEMA,
        "attempted": True,
        "ok": True,
        "queue_name": queue_name,
        "workflow_name": SOURCE_SYNC_RECOVERY_WORKFLOW_NAME,
        "stale_after_seconds": SOURCE_SYNC_RECOVERY_STALE_AFTER_SECONDS,
        "eligible_count": int(report.get("eligible_count") or 0),
        "cancelled_count": int(report.get("cancelled_count") or 0),
        "dry_run": bool(report.get("dry_run")),
        "report": report,
    }


def source_sync_status_is_stale_dbos_only(
    source_sync_status: Mapping[str, Any],
) -> bool:
    """Return whether the only readiness blocker is stale replaceable DBOS work."""
    if not source_sync_status:
        return False
    runtime_liveness = _mapping(source_sync_status.get("runtime_liveness"))
    if runtime_liveness.get("ok") is False:
        return False
    freshness = _mapping(source_sync_status.get("source_freshness"))
    if freshness.get("ok") is False:
        return False
    dbos = source_sync_dbos_workflows(source_sync_status)
    backlog = _mapping(source_sync_status.get("backlog"))
    stale_replaceable = bool(
        dbos.get("stale_replaceable_work_present")
        or backlog.get("stale_replaceable_work_present")
    )
    if not stale_replaceable:
        return False
    blockers = _list_of_mappings(source_sync_status.get("blockers"))
    if not blockers:
        return False
    return all(
        str(blocker.get("code") or "") == "source_sync_backlog_stale_replaceable"
        for blocker in blockers
    )


def source_sync_dbos_workflows(
    source_sync_status: Mapping[str, Any],
) -> dict[str, Any]:
    wakeups = _mapping(source_sync_status.get("wakeups"))
    return _mapping(wakeups.get("dbos_workflows"))


def source_sync_recovery_queue_name(
    deployment: str,
    dbos: Mapping[str, Any],
) -> str:
    active_queue_names = list(dbos.get("active_queue_names") or [])
    if active_queue_names:
        return str(active_queue_names[0])
    if dbos.get("queue_name"):
        return str(dbos.get("queue_name"))
    return f"okg-{deployment}-source-sync"


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list_of_mappings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(row) for row in value if isinstance(row, Mapping)]
