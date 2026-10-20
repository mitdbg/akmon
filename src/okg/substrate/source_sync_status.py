"""Pure source-sync status policy over supplied observations.

This module intentionally owns no observation acquisition.  Callers provide
producer, queue, runtime, freshness, convergence, and recovery observations;
the functions here normalize and compose those values into the stable
source-sync status contract.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from okg.substrate.runtime.health_policy import runtime_health_policy
from okg.substrate.source_sync import (
    source_sync_desired_frontier_state,
    source_sync_entry_is_explicitly_dormant,
)

DEFAULT_PRODUCER_HEALTH_MAX_AGE_SECONDS = 15 * 60
_SOURCE_SYNC_STALE_BACKLOG_SECONDS = 600
SOURCE_SYNC_STATUS_SCHEMA = "okg.source_sync.status.v1"


def build_source_sync_status_contract(
    health: Mapping[str, Any],
    *,
    source_freshness: Mapping[str, Any] | None = None,
    runtime_services_health: Mapping[str, Any] | None = None,
    source_inventory: Sequence[Mapping[str, Any]] | None = None,
    active_work_by_source: Mapping[str, Any] | None = None,
    generation: Mapping[str, Any] | None = None,
    mcp: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the authoritative source-sync health view.

    The contract separates the four concepts that were previously conflated
    in readiness and reports: durable DBOS wakeups, producer liveness, source
    freshness, and backlog/error state.
    """
    deployment = str(health.get("deployment") or "")
    expected_rows = _list_of_mappings(health.get("expected_producers"))
    disabled_rows = _list_of_mappings(health.get("disabled_producers"))
    producer_rows = _list_of_mappings(health.get("producers"))
    missing_rows = _list_of_mappings(health.get("missing_producers"))
    stale_rows = _list_of_mappings(health.get("stale_producers"))
    config_warnings = _list_of_mappings(health.get("config_warnings"))
    recovery_commands = health.get("recovery_commands")
    recovery_commands = (
        dict(recovery_commands) if isinstance(recovery_commands, Mapping) else {}
    )
    dbos_queue = health.get("dbos_queue_depth")
    dbos_queue = dbos_queue if isinstance(dbos_queue, Mapping) else {}
    if runtime_services_health is None:
        runtime_services_health = health.get("runtime_services_health")
    runtime_services_health = (
        runtime_services_health
        if isinstance(runtime_services_health, Mapping)
        else None
    )

    dbos_available = dbos_queue.get("available")
    dbos_active_count = int(dbos_queue.get("active_count") or 0)
    terminal_history_count = int(dbos_queue.get("terminal_history_count") or 0)
    dlq_unknown = (
        dbos_available is False
        or (
            dbos_available is None
            and "dlq_count" not in health
            and "failed_terminal_count" not in dbos_queue
        )
    )
    if "failed_terminal_count" in dbos_queue:
        dlq_count = int(dbos_queue.get("failed_terminal_count") or 0)
    else:
        dlq_count = int(health.get("dlq_count") or 0)
    stale_replaceable_work = bool(
        dbos_queue.get("stale_replaceable_work_present")
    )
    policy = runtime_health_policy()

    runtime_liveness = _runtime_liveness_contract(
        expected_rows=expected_rows,
        producer_rows=producer_rows,
        missing_rows=missing_rows,
        stale_rows=stale_rows,
        config_warnings=config_warnings,
        runtime_services_health=runtime_services_health,
    )
    freshness = _source_freshness_contract(source_freshness)
    if source_inventory is None:
        raw_inventory = health.get("source_convergence_inventory")
        source_inventory = (
            tuple(row for row in raw_inventory if isinstance(row, Mapping))
            if isinstance(raw_inventory, (list, tuple)) else ()
        )
    if active_work_by_source is None:
        raw_active = health.get("active_source_sync_workflows")
        active_work_by_source = (
            raw_active if isinstance(raw_active, Mapping) else {}
        )
    desired_frontiers = _desired_frontiers_contract(active_work_by_source)
    producer_impact = _producer_liveness_impact(
        missing_rows=missing_rows,
        stale_rows=stale_rows,
        source_inventory=source_inventory,
    )
    blocking_missing_rows = producer_impact["blocking_missing_producers"]
    blocking_stale_rows = producer_impact["blocking_stale_producers"]
    wakeups_ok = not dlq_unknown and dlq_count == 0
    backlog_ok = not dlq_unknown and dlq_count == 0 and not stale_replaceable_work

    blockers: list[dict[str, Any]] = []
    if config_warnings:
        blockers.append({
            "code": "source_sync_config_warning",
            "count": len(config_warnings),
        })
    if blocking_missing_rows:
        blockers.append({
            "code": "source_sync_producer_missing",
            "count": len(blocking_missing_rows),
        })
    if blocking_stale_rows:
        blockers.append({
            "code": "source_sync_producer_stale",
            "count": len(blocking_stale_rows),
        })
    if dlq_count:
        blockers.append({
            "code": "source_sync_dlq_nonempty",
            "count": dlq_count,
            "failed_terminal_rows": dbos_queue.get("failed_terminal_rows"),
        })
    if dlq_unknown:
        blockers.append({
            "code": "source_sync_dlq_status_unknown",
            "reason": (
                dbos_queue.get("reason")
                or "dbos workflow status unavailable"
            ),
        })
    if stale_replaceable_work:
        blockers.append({
            "code": "source_sync_backlog_stale_replaceable",
            "active_count": dbos_active_count,
            "pending_count": dbos_queue.get("pending_count"),
            "waiting_count": dbos_queue.get("waiting_count"),
            "oldest_active_age_seconds": dbos_queue.get(
                "oldest_active_age_seconds"
            ),
            "oldest_pending_update_age_seconds": dbos_queue.get(
                "oldest_pending_update_age_seconds"
            ),
            "oldest_waiting_age_seconds": dbos_queue.get(
                "oldest_waiting_age_seconds"
            ),
            "stale_after_seconds": dbos_queue.get(
                "stale_after_seconds"
            ) or _SOURCE_SYNC_STALE_BACKLOG_SECONDS,
            "stale_pending_work_present": bool(
                dbos_queue.get("stale_pending_work_present")
            ),
            "stale_waiting_work_present": bool(
                dbos_queue.get("stale_waiting_work_present")
            ),
        })
    runtime_worker = runtime_liveness.get("worker")
    runtime_services = runtime_liveness.get("services")
    service_statuses: list[str] = []
    if isinstance(runtime_services, Mapping):
        for service in runtime_services.values():
            if isinstance(service, Mapping):
                service_statuses.append(str(service.get("status") or ""))
    if (
        isinstance(runtime_worker, Mapping)
        and runtime_worker.get("ok") is False
    ) or any(status in {"down", "unhealthy"} for status in service_statuses):
        blockers.append({
            "code": "source_sync_runtime_unhealthy",
            "status": runtime_liveness.get("status"),
        })
    if freshness.get("ok") is False:
        blockers.append({
            "code": "source_freshness_not_ready",
            "missing_count": len(freshness.get("missing") or []),
            "stale_count": len(freshness.get("stale") or []),
        })

    status = {
        "schema": SOURCE_SYNC_STATUS_SCHEMA,
        "deployment": deployment,
        "ok": not blockers,
        "status": "healthy" if not blockers else "blocked",
        "blockers": blockers,
        "policy": policy,
        "desired_plan": {
            "expected_producer_count": len(expected_rows),
            "disabled_producer_count": len(disabled_rows),
            "expected_producers": expected_rows,
            "disabled_producers": disabled_rows,
            "config_warnings": config_warnings,
        },
        "recovery_commands": recovery_commands,
        "wakeups": {
            "ok": wakeups_ok,
            "status": "healthy" if wakeups_ok else "blocked",
            "dbos_workflows": dict(dbos_queue),
        },
        "desired_frontiers": desired_frontiers,
        "runtime_liveness": runtime_liveness,
        "producer_liveness_impact": producer_impact,
        "source_freshness": freshness,
        "backlog": {
            "ok": backlog_ok,
            "status": (
                "healthy"
                if backlog_ok else (
                    "stale_replaceable_work_present"
                    if stale_replaceable_work else "blocked"
                )
            ),
            "active_count": dbos_active_count,
            "dbos_active_count": dbos_active_count,
            "terminal_history_count": terminal_history_count,
            "dlq_count": dlq_count,
            "dlq_status": "unknown" if dlq_unknown else "known",
            "failed_terminal_rows": dbos_queue.get("failed_terminal_rows"),
            "stale_after_seconds": dbos_queue.get(
                "stale_after_seconds"
            ) or _SOURCE_SYNC_STALE_BACKLOG_SECONDS,
            "pending_count": dbos_queue.get("pending_count"),
            "waiting_count": dbos_queue.get("waiting_count"),
            "fresh_pending_count": dbos_queue.get("fresh_pending_count"),
            "running_pending_count": dbos_queue.get("running_pending_count"),
            "replaceable_waiting_count": dbos_queue.get(
                "replaceable_waiting_count"
            ),
            "oldest_active_age_seconds": dbos_queue.get(
                "oldest_active_age_seconds"
            ),
            "newest_active_age_seconds": dbos_queue.get(
                "newest_active_age_seconds"
            ),
            "oldest_pending_update_age_seconds": dbos_queue.get(
                "oldest_pending_update_age_seconds"
            ),
            "oldest_waiting_age_seconds": dbos_queue.get(
                "oldest_waiting_age_seconds"
            ),
            "oldest_replaceable_waiting_age_seconds": dbos_queue.get(
                "oldest_replaceable_waiting_age_seconds"
            ),
            "stale_pending_work_present": bool(
                dbos_queue.get("stale_pending_work_present")
            ),
            "stale_waiting_work_present": bool(
                dbos_queue.get("stale_waiting_work_present")
            ),
            "stale_replaceable_work_present": stale_replaceable_work,
        },
    }
    try:
        from okg.substrate.source_sync_convergence import (
            build_convergence_status,
        )

        status["convergence"] = build_convergence_status(
            deployment=deployment,
            source_inventory=source_inventory,
            source_sync_status=status,
            source_freshness=source_freshness,
            active_work_by_source=active_work_by_source,
            generation=generation,
            mcp=mcp,
        )
    except Exception as exc:  # noqa: BLE001 - keep base health readable.
        status["convergence"] = {
            "schema": "okg.source_sync.convergence.v1",
            "deployment": deployment,
            "state": "unsupported",
            "ok": False,
            "reason": "convergence_status_error",
            "error": type(exc).__name__,
            "message": str(exc),
        }
    convergence = status.get("convergence")
    if (
        isinstance(convergence, Mapping)
        and convergence.get("ok") is False
        and int(convergence.get("readiness_required_source_count") or 0) > 0
    ):
        blockers.append({
            "code": "source_sync_convergence_not_ready",
            "state": convergence.get("state"),
            "next_action": convergence.get("next_action"),
        })
        status["blockers"] = blockers
        status["ok"] = False
        status["status"] = "blocked"
    return status


def _producer_liveness_impact(
    *,
    missing_rows: Sequence[Mapping[str, Any]],
    stale_rows: Sequence[Mapping[str, Any]],
    source_inventory: Sequence[Mapping[str, Any]] | None,
) -> dict[str, Any]:
    """Split stale/missing producer rows into readiness blockers/advisories."""

    has_inventory = bool(source_inventory)
    required_sources = _readiness_required_source_names(source_inventory)
    if not has_inventory:
        blocking_missing = [dict(row) for row in missing_rows]
        blocking_stale = [dict(row) for row in stale_rows]
        advisory_missing: list[dict[str, Any]] = []
        advisory_stale: list[dict[str, Any]] = []
    else:
        blocking_missing = [
            dict(row) for row in missing_rows
            if _producer_row_is_required(row, required_sources)
        ]
        blocking_stale = [
            dict(row) for row in stale_rows
            if _producer_row_is_required(row, required_sources)
        ]
        advisory_missing = [
            dict(row) for row in missing_rows
            if not _producer_row_is_required(row, required_sources)
        ]
        advisory_stale = [
            dict(row) for row in stale_rows
            if not _producer_row_is_required(row, required_sources)
        ]
    return {
        "source_inventory_evaluated": has_inventory,
        "readiness_required_sources": sorted(required_sources),
        "blocking_missing_producers": blocking_missing,
        "blocking_stale_producers": blocking_stale,
        "advisory_missing_producers": advisory_missing,
        "advisory_stale_producers": advisory_stale,
    }


def _readiness_required_source_names(
    source_inventory: Sequence[Mapping[str, Any]] | None,
) -> set[str]:
    out: set[str] = set()
    for row in source_inventory or ():
        if not isinstance(row, Mapping):
            continue
        policy = row.get("freshness_policy")
        policy = policy if isinstance(policy, Mapping) else {}
        if policy.get("readiness_required") is not True:
            continue
        source_name = str(row.get("source_name") or "").strip()
        if source_name:
            out.add(source_name)
    return out


def _producer_row_is_required(
    row: Mapping[str, Any],
    required_sources: set[str],
) -> bool:
    source_name = str(
        row.get("source_name")
        or row.get("source")
        or ""
    ).strip()
    if not source_name:
        return False
    if source_name in required_sources:
        return True
    short_name = source_name.rsplit(".", 1)[-1]
    return short_name in required_sources


def source_sync_status_for_source(
    status: Mapping[str, Any],
    source_name: str,
) -> dict[str, Any]:
    runtime = status.get("runtime_liveness")
    runtime = runtime if isinstance(runtime, Mapping) else {}
    sources = runtime.get("sources")
    sources = sources if isinstance(sources, Mapping) else {}
    runtime_source = sources.get(source_name)
    runtime_source = (
        runtime_source if isinstance(runtime_source, Mapping) else {
            "source_name": source_name,
            "status": "not_configured",
            "producers": [],
        }
    )
    freshness = _source_freshness_for_source(
        status.get("source_freshness"),
        source_name,
    )
    recovery_commands = status.get("recovery_commands")
    recovery_commands = (
        dict(recovery_commands) if isinstance(recovery_commands, Mapping) else {}
    )
    convergence = status.get("convergence")
    convergence = (
        convergence if isinstance(convergence, Mapping) else {}
    )
    desired_frontiers = status.get("desired_frontiers")
    desired_frontiers = (
        desired_frontiers if isinstance(desired_frontiers, Mapping) else {}
    )
    frontier_sources = desired_frontiers.get("sources")
    frontier_sources = (
        frontier_sources if isinstance(frontier_sources, Mapping) else {}
    )
    raw_desired_frontier = frontier_sources.get(source_name)
    desired_frontier = (
        raw_desired_frontier
        if isinstance(raw_desired_frontier, Mapping) else {}
    )
    try:
        from okg.substrate.source_sync_convergence import (
            source_convergence_for_source,
        )

        source_convergence = source_convergence_for_source(
            convergence,
            source_name,
        )
    except Exception:  # noqa: BLE001 - source status stays readable.
        source_convergence = {}
    return {
        "schema": status.get("schema") or SOURCE_SYNC_STATUS_SCHEMA,
        "deployment": status.get("deployment"),
        "source_name": source_name,
        "status": _rollup_status(
            [
                str(runtime_source.get("status") or "not_configured"),
                str(freshness.get("status") or "not_evaluated"),
            ]
        ),
        "runtime_liveness": dict(runtime_source),
        "desired_frontier": dict(desired_frontier),
        "source_freshness": freshness,
        "convergence": source_convergence,
        "recovery_commands": recovery_commands,
    }


def _desired_frontiers_contract(
    active_work_by_source: Mapping[str, Any],
) -> dict[str, Any]:
    sources: dict[str, dict[str, Any]] = {}
    for source_name, active in sorted(active_work_by_source.items()):
        if not isinstance(active, Mapping):
            continue
        desired = active.get("desired_frontier")
        if isinstance(desired, Mapping):
            frontier = dict(desired)
        else:
            run_mode = str(active.get("run_mode") or "")
            if not run_mode:
                continue
            frontier = source_sync_desired_frontier_state(
                source_name=str(source_name),
                run_mode=run_mode,
                workflow_id=(
                    str(active.get("workflow_id"))
                    if active.get("workflow_id") is not None else None
                ),
                workflow_status=(
                    str(active.get("status"))
                    if active.get("status") is not None else None
                ),
                stale=bool(active.get("stale")),
                started_age_seconds=_optional_number(
                    active.get("started_age_seconds"),
                ),
            )
        sources[str(source_name)] = frontier

    active_count = len(sources)
    stale_count = sum(
        1 for row in sources.values() if row.get("workflow_stale")
    )
    return {
        "schema": "okg.source_sync.desired_frontiers.v1",
        "status": (
            "idle"
            if active_count == 0 else (
                "stale_active_work" if stale_count else "advancing"
            )
        ),
        "active_count": active_count,
        "stale_count": stale_count,
        "completed_from_active_work_count": 0,
        "completion_basis": (
            "source_freshness_and_publish_visibility_not_dbos_wakeups"
        ),
        "sources": sources,
    }


def _runtime_liveness_contract(
    *,
    expected_rows: list[dict[str, Any]],
    producer_rows: list[dict[str, Any]],
    missing_rows: list[dict[str, Any]],
    stale_rows: list[dict[str, Any]],
    config_warnings: list[dict[str, Any]],
    runtime_services_health: Mapping[str, Any] | None,
) -> dict[str, Any]:
    sources: dict[str, dict[str, Any]] = {}
    for row in expected_rows:
        source_name = str(row.get("source_name") or "")
        producer = str(row.get("producer") or "")
        if not source_name or not producer:
            continue
        source = sources.setdefault(source_name, {
            "source_name": source_name,
            "status": "not_configured",
            "producers": [],
        })
        producer_status = str(row.get("status") or "unknown")
        item = {
            "producer": producer,
            "status": producer_status,
            "max_age_seconds": row.get("max_age_seconds"),
            "trigger_kinds": list(row.get("trigger_kinds") or []),
        }
        if isinstance(row.get("observed"), Mapping):
            item["observed"] = dict(row["observed"])
        source["producers"].append(item)
    for source in sources.values():
        source["status"] = _rollup_status(
            str(row.get("status") or "") for row in source["producers"]
        )

    worker_runtime_health = runtime_services_health
    service_runtime_health = runtime_services_health

    worker = _runtime_worker_contract(worker_runtime_health)
    runtime_services_ok = (
        worker["ok"] is not False
        and not _runtime_service_down(service_runtime_health, "watcher")
        and not _runtime_service_down(service_runtime_health, "webhook")
    )
    ok = (
        not missing_rows
        and not stale_rows
        and not config_warnings
        and runtime_services_ok
    )
    return {
        "ok": ok,
        "status": "healthy" if ok else "blocked",
        "worker": {
            **worker,
            "producer_row_count": len(producer_rows),
        },
        "services": {
            "watch": _producer_kind_contract(
                "watch",
                sources,
                runtime_service=(
                    _runtime_service_view(service_runtime_health, "watcher")
                ),
            ),
            "webhook": _producer_kind_contract(
                "webhook",
                sources,
                runtime_service=(
                    _runtime_service_view(service_runtime_health, "webhook")
                ),
            ),
        },
        "poll_probes": _producer_kind_contract("poll", sources),
        "sources": sources,
        "missing_producers": missing_rows,
        "stale_producers": stale_rows,
        "config_warnings": config_warnings,
    }


def _producer_kind_contract(
    producer: str,
    sources: Mapping[str, Mapping[str, Any]],
    *,
    runtime_service: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for source_name, source in sorted(sources.items()):
        producers = source.get("producers")
        if not isinstance(producers, list):
            continue
        matches = [
            row for row in producers
            if isinstance(row, Mapping) and row.get("producer") == producer
        ]
        for row in matches:
            rows.append({
                "source_name": source_name,
                "status": row.get("status"),
                "max_age_seconds": row.get("max_age_seconds"),
                "trigger_kinds": list(row.get("trigger_kinds") or []),
            })
    status = _rollup_status(str(row.get("status") or "") for row in rows)
    service_status: str | None = None
    if runtime_service is not None:
        service_status = _runtime_service_status(runtime_service)
        status = _rollup_status([status, service_status])
    return {
        "ok": status in {"healthy", "not_configured"},
        "status": status,
        "expected_source_count": len(rows),
        "sources": rows,
        "runtime_service": dict(runtime_service) if runtime_service else None,
    }


def _runtime_worker_contract(
    runtime_services_health: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if runtime_services_health is None:
        return {
            "ok": None,
            "status": "not_directly_observed",
            "code": "runtime_services_not_requested",
            "evidence": (
                "producer/service telemetry only; durable queue rows are "
                "reported under wakeups and backlog"
            ),
        }
    ok = runtime_services_health.get("ok")
    code = str(runtime_services_health.get("code") or "runtime_services_unknown")
    if ok is True and code == "runtime_services_not_observed":
        status = "not_observed"
    elif ok is True and code == "runtime_services_other_deployment":
        status = "not_observed"
    elif ok is True:
        status = "healthy"
    elif code == "runtime_services_overlay_stale":
        status = "stale"
    elif code == "runtime_services_down":
        status = "down"
    else:
        status = "unhealthy"
    return {
        "ok": bool(ok),
        "status": status,
        "code": code,
        "source": runtime_services_health.get("source"),
        "message": runtime_services_health.get("message"),
        "last_observed_at": runtime_services_health.get("last_observed_at"),
        "observed_minutes_ago": runtime_services_health.get(
            "observed_minutes_ago"
        ),
    }


def _runtime_service_view(
    runtime_services_health: Mapping[str, Any] | None,
    service: str,
) -> Mapping[str, Any] | None:
    if not isinstance(runtime_services_health, Mapping):
        return None
    view = runtime_services_health.get(service)
    return view if isinstance(view, Mapping) else None


def _runtime_service_down(
    runtime_services_health: Mapping[str, Any] | None,
    service: str,
) -> bool:
    view = _runtime_service_view(runtime_services_health, service)
    return bool(view and view.get("enabled") and not view.get("alive"))


def _runtime_service_status(runtime_service: Mapping[str, Any]) -> str:
    if not runtime_service.get("enabled"):
        return "not_configured"
    if runtime_service.get("alive"):
        return "healthy"
    return "down"


def _source_freshness_contract(
    source_freshness: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if source_freshness is None:
        return {
            "ok": None,
            "status": "not_evaluated",
            "reason": "source watermarks were not provided to this status view",
            "missing": [],
            "stale": [],
            "groups": {},
        }
    out = dict(source_freshness)
    ok = out.get("ok")
    out["ok"] = bool(ok)
    out["status"] = "healthy" if ok else "blocked"
    out.setdefault("missing", [])
    out.setdefault("stale", [])
    out.setdefault("groups", {})
    return out


def _source_freshness_for_source(
    source_freshness: Any,
    source_name: str,
) -> dict[str, Any]:
    if not isinstance(source_freshness, Mapping):
        return {
            "status": "not_evaluated",
            "source_name": source_name,
        }
    if source_freshness.get("status") == "not_evaluated":
        return {
            "status": "not_evaluated",
            "source_name": source_name,
        }
    for key, status in (("missing", "missing"), ("stale", "stale")):
        rows = source_freshness.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            row_source = str(row)
            if row_source == source_name or row_source.endswith(f".{source_name}"):
                return {
                    "status": status,
                    "source": row_source,
                    "source_name": source_name,
                }
    groups = source_freshness.get("groups")
    if isinstance(groups, Mapping):
        for group_name, group in groups.items():
            if not isinstance(group, Mapping):
                continue
            rows = group.get("sources")
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                row_source = str(row.get("source") or "")
                if row_source in {source_name, f".{source_name}"}:
                    return {
                        **dict(row),
                        "group": group_name,
                    }
                if row_source.endswith(f".{source_name}"):
                    return {
                        **dict(row),
                        "group": group_name,
                    }
    return {
        "status": "not_configured",
        "source_name": source_name,
    }


def _list_of_mappings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(row) for row in value if isinstance(row, Mapping)]


def _optional_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _rollup_status(statuses: Any) -> str:
    rows = [str(status) for status in statuses if str(status)]
    if not rows:
        return "not_configured"
    for status in (
        "blocked",
        "down",
        "missing",
        "stale",
        "unhealthy",
        "unavailable",
        "warning",
    ):
        if status in rows:
            return status
    if any(status == "healthy" for status in rows):
        return "healthy"
    if all(status == "not_evaluated" for status in rows):
        return "not_evaluated"
    if all(status == "not_configured" for status in rows):
        return "not_configured"
    return rows[0]


def expected_source_sync_producers(
    registry: Mapping[str, Any],
    *,
    max_producer_age_seconds: int = DEFAULT_PRODUCER_HEALTH_MAX_AGE_SECONDS,
    runtime_source_sync: Mapping[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    expected, _disabled, warnings = source_sync_producer_expectations(
        registry,
        max_producer_age_seconds=max_producer_age_seconds,
        runtime_source_sync=runtime_source_sync,
    )
    return expected, warnings


def source_sync_producer_expectations(
    registry: Mapping[str, Any],
    *,
    max_producer_age_seconds: int = DEFAULT_PRODUCER_HEALTH_MAX_AGE_SECONDS,
    runtime_source_sync: Mapping[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    return _expected_source_sync_producers(
        registry,
        max_producer_age_seconds=max_producer_age_seconds,
        runtime_source_sync=runtime_source_sync,
    )


def _expected_source_sync_producers(
    registry: Mapping[str, Any],
    *,
    max_producer_age_seconds: int,
    runtime_source_sync: Mapping[str, Any] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    expected: list[dict[str, Any]] = []
    disabled: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    for source_name, entry in sorted(registry.items()):
        if source_sync_entry_is_explicitly_dormant(entry):
            continue
        sync = getattr(entry, "sync", None)
        if sync is None:
            continue
        triggers = set(sync.triggers)
        if sync.watch is not None and triggers & {"fs_event", "git_ref_change"}:
            row = {
                "source_name": str(source_name),
                "producer": "watch",
                "trigger_kinds": sorted(triggers & {"fs_event", "git_ref_change"}),
                "max_age_seconds": int(max_producer_age_seconds),
            }
            _append_expected_or_disabled(
                row,
                expected=expected,
                disabled=disabled,
                runtime_source_sync=runtime_source_sync,
            )
        if "poll" in triggers:
            if sync.poll is None:
                warnings.append({
                    "source_name": str(source_name),
                    "code": "poll_trigger_without_schedule",
                    "message": (
                        "source declares poll trigger but no sync.poll schedule"
                    ),
                })
            else:
                interval = int(sync.poll.interval_seconds)
                row = {
                    "source_name": str(source_name),
                    "producer": "poll",
                    "trigger_kinds": ["poll"],
                    "interval_seconds": interval,
                    "max_age_seconds": max(
                        int(max_producer_age_seconds),
                        interval * 2,
                    ),
                }
                _append_expected_or_disabled(
                    row,
                    expected=expected,
                    disabled=disabled,
                    runtime_source_sync=runtime_source_sync,
                )
        if sync.webhook is not None and "webhook" in triggers:
            row = {
                "source_name": str(source_name),
                "producer": "webhook",
                "trigger_kinds": ["webhook"],
                "path": sync.webhook.path,
                "max_age_seconds": int(max_producer_age_seconds),
            }
            _append_expected_or_disabled(
                row,
                expected=expected,
                disabled=disabled,
                runtime_source_sync=runtime_source_sync,
            )
    return expected, disabled, warnings


def _append_expected_or_disabled(
    row: dict[str, Any],
    *,
    expected: list[dict[str, Any]],
    disabled: list[dict[str, Any]],
    runtime_source_sync: Mapping[str, Any] | None,
) -> None:
    if _runtime_producer_enabled(
        str(row["producer"]),
        runtime_source_sync=runtime_source_sync,
    ):
        expected.append(row)
        return
    producer = str(row["producer"])
    disabled.append({
        **row,
        "status": "disabled",
        "owner": "deployment.runtime.source_sync",
        "reason": f"runtime.source_sync.{producer}.enabled=false",
        "recovery_command": (
            f"enable runtime.source_sync.{producer} in deployment.yaml "
            "or remove the corresponding trigger from source_registry.yaml"
        ),
    })


def _runtime_producer_enabled(
    producer: str,
    *,
    runtime_source_sync: Mapping[str, Any] | None,
) -> bool:
    if runtime_source_sync is None:
        return True
    block = runtime_source_sync.get(producer)
    if not isinstance(block, Mapping):
        return True
    return bool(block.get("enabled", True))


def _producer_observed_at(
    payload: Mapping[str, Any],
    updated_at: datetime,
) -> datetime:
    for key in (
        "last_heartbeat_at",
        "last_enqueue_at",
        "last_event_at",
        "last_run_at",
    ):
        value = payload.get(key)
        if isinstance(value, str) and value:
            parsed = _parse_datetime(value)
            if parsed is not None:
                return parsed
    return updated_at if updated_at.tzinfo else updated_at.replace(tzinfo=timezone.utc)


def _parse_datetime(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _age_seconds(now: datetime, observed_at: datetime) -> float:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    return max(0.0, (now - observed_at).total_seconds())


__all__ = [
    "DEFAULT_PRODUCER_HEALTH_MAX_AGE_SECONDS",
    "SOURCE_SYNC_STATUS_SCHEMA",
    "build_source_sync_status_contract",
    "expected_source_sync_producers",
    "source_sync_producer_expectations",
    "source_sync_status_for_source",
]
