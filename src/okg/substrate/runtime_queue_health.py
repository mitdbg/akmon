"""Shared adapters for the DBOS-backed runtime queue health surface.

Field semantics for ``summarize_runtime_queue_health`` (read by publish-proof,
deployment readiness, console health, and both doctor snapshots):

* ``dlq_count`` / ``runtime_failed_count`` — the **current** failure count from
  ``current_runtime_dlq``. This is the gate-visible count: it excludes failures
  from old application versions and source-sync failures explicitly superseded
  by a same-responsibility success.
* ``terminal_history_dlq_count`` — all retained terminal failure-class rows
  (``ERROR`` / ``FAILED`` / ``MAX_RECOVERY_ATTEMPTS`` /
  ``MAX_RECOVERY_ATTEMPTS_EXCEEDED``) from the view's ``failed_count``. This is
  audit data only and never reddens the current runtime.
* ``runtime_cancelled_count`` — ``CANCELLED`` rows, tracked **separately** and
  reported as data only. Cancels are steady-state by design: worker hygiene
  cancels stale replaceable wakeups (``okg_publisher_cycle`` /
  ``okg_source_sync`` / ``okg_source_poll_*``) and never deletes the rows, so a
  healthy okg-workspace holds thousands of ``CANCELLED`` rows. They never
  redden the verdict.
* ``terminal_history_count`` — all terminal rows (success + failure + cancel),
  reported as data only.

This mirrors the source-sync precedent (``source_sync_producers`` counts
``CANCELLED`` as DLQ only when DBOS recorded an ordinary-error payload, and its
primary failure set excludes ``CANCELLED``): a bare ``CANCELLED`` row is
superseded work, not a dead letter.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from okg.substrate.runtime_dlq import current_runtime_dlq

QUEUE_DEPTH_KINDS: tuple[str, ...] = (
    "ingest",
    "link",
    "enrich",
    "publish",
    "embed",
)

RUNTIME_WORKFLOW_NAMES_BY_JOB_KIND: dict[str, frozenset[str]] = {
    "embed": frozenset({"okg_embed_batch", "okg_embed_batch_enqueue"}),
}

# Deployment readiness validates these workflows through dedicated authorities:
# doctor state and source-sync convergence. Keeping the partition named and
# shared prevents deployment smoke from reclassifying their retained DBOS audit
# history as generic queue failures after readiness already accepted them.
DEPLOYMENT_READINESS_DELEGATED_RUNTIME_WORKFLOW_NAMES: frozenset[str] = frozenset({
    "doctor_workflow",
    "okg_source_poll_interval_300s",
    "okg_source_sync",
})

_RUNTIME_QUEUE_HEALTH_COLUMNS: tuple[str, ...] = (
    "queue_name",
    "status",
    "availability",
    "active_count",
    "pending_count",
    "waiting_count",
    "delayed_count",
    "running_count",
    "failed_count",
    "cancelled_count",
    "dlq_count",
    "terminal_history_count",
    "unknown_active_age_count",
    "oldest_active_age_seconds",
    "newest_active_age_seconds",
    "stale_after_seconds",
    "stale_active_work_present",
    "generated_at",
    "reason",
    "workflow_name",
)


def runtime_queue_health_rows(cur: Any) -> list[dict[str, Any]]:
    """Return rows from the live runtime queue authority."""
    cur.execute(
        """
        SELECT EXISTS (
          SELECT 1
            FROM information_schema.columns
           WHERE table_schema = 'okg'
             AND table_name = 'v_runtime_queue_health'
             AND column_name = 'workflow_name'
        ) AS present
        """
    )
    raw_has_workflow_name = cur.fetchone()
    has_workflow_name = bool(
        raw_has_workflow_name.get("present")
        if isinstance(raw_has_workflow_name, Mapping)
        else raw_has_workflow_name[0]
    )
    workflow_select = "workflow_name" if has_workflow_name else "NULL::text"
    cur.execute(
        f"""
        SELECT queue_name, status, availability, active_count,
               pending_count, waiting_count, delayed_count,
               running_count, failed_count, cancelled_count,
               dlq_count, terminal_history_count,
               unknown_active_age_count,
               oldest_active_age_seconds, newest_active_age_seconds,
               stale_after_seconds, stale_active_work_present,
               generated_at, reason, {workflow_select} AS workflow_name
          FROM okg.v_runtime_queue_health
         ORDER BY queue_name NULLS FIRST, workflow_name NULLS FIRST
        """
    )
    rows: list[dict[str, Any]] = []
    for raw in cur.fetchall():
        if isinstance(raw, Mapping):
            row = {name: raw.get(name) for name in _RUNTIME_QUEUE_HEALTH_COLUMNS}
        else:
            row = dict(zip(_RUNTIME_QUEUE_HEALTH_COLUMNS, raw, strict=True))
        rows.append(row)
    return rows


def summarize_runtime_queue_health(
    cur: Any,
    *,
    ignored_job_kinds: set[str] | None = None,
    ignored_workflow_names: set[str] | None = None,
    include_terminal_history_dlq: bool = True,
    current_dlq: Mapping[str, Any] | None = None,
    dlq_unknown: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Project runtime queue rows into the legacy readiness/metrics shape.

    The old queue APIs expose a fixed job-kind map. DBOS rows are grouped by
    runtime queue and workflow name, so known optional workflows can keep their
    old job-kind bucket while unknown runtime work is counted as ``ingest`` for
    backward-compatible bounds.

    ``dlq_count`` is the current failure count (see the module docstring for the
    full field semantics). ``include_terminal_history_dlq`` is retained for API
    stability but no longer changes the verdict or any count — it is a no-op.

    Issue #1290. ``current_dlq`` injects an already-read dead-letter result so
    the expensive history scan runs once per readiness pass instead of twice.
    ``dlq_unknown`` says that scan was ATTEMPTED and did not finish inside its
    deadline: the dead-letter count is an input to this verdict, so the result
    then reports ``ok=None``, ``status="unknown"`` and ``dlq_count=None``
    while still returning every queue measurement that did succeed. Passing
    neither keeps the original behavior — the DLQ is read here and a failure
    propagates.
    """
    rows = runtime_queue_health_rows(cur)
    ignored = ignored_job_kinds or set()
    ignored_workflows = (
        _ignored_runtime_workflow_names(ignored)
        | (ignored_workflow_names or set())
    )
    counted_rows = [
        row for row in rows
        if str(row.get("workflow_name") or "") not in ignored_workflows
    ]

    total_pending = sum(_int(row.get("pending_count")) for row in counted_rows)
    total_waiting = sum(_int(row.get("waiting_count")) for row in counted_rows)
    total_delayed = sum(_int(row.get("delayed_count")) for row in counted_rows)
    total_running = sum(_int(row.get("running_count")) for row in counted_rows)
    # The SQL view intentionally retains all DBOS history. Keep that raw count
    # for audit, but derive the gate-visible count from current-runtime
    # authority so an old application-version failure does not make readiness
    # permanently red.
    terminal_history_failed = sum(
        _int(row.get("failed_count")) for row in counted_rows
    )
    # Issue #1290: readiness reads the DLQ under a statement timeout and
    # passes the outcome in, so the expensive history scan runs ONCE and a
    # cancelled scan is reported as unmeasured rather than raised through
    # queue health. Callers that pass neither keep the original behavior:
    # the DLQ is read here and any failure propagates.
    if dlq_unknown is not None:
        current_dlq = {}
    elif current_dlq is None:
        current_dlq = current_runtime_dlq(cur)
    current_failures = [
        failure
        for failure in current_dlq.get("current_failures", ())
        if str(failure.get("workflow_name") or "") not in ignored_workflows
    ]
    total_failed = len(current_failures)
    total_cancelled = sum(
        _int(row.get("cancelled_count")) for row in counted_rows
    )
    total_terminal = sum(
        _int(row.get("terminal_history_count")) for row in counted_rows
    )
    # F5: ``include_terminal_history_dlq`` is retained for API stability but is
    # now a no-op. There is no remaining False caller, the verdict never
    # depends on it, and ``dlq_count`` is always the current failure sum.
    del include_terminal_history_dlq

    queue_depth = {kind: 0 for kind in QUEUE_DEPTH_KINDS}
    for row in counted_rows:
        kind = _runtime_job_kind(row)
        queue_depth[kind] += _int(row.get("active_count"))
    active_queue_depth = sum(
        count for kind, count in queue_depth.items() if kind not in ignored
    )

    unavailable_rows = [
        row for row in counted_rows
        if str(row.get("availability") or "").lower() != "available"
    ]
    stale_rows = [
        row for row in counted_rows
        if _bool(row.get("stale_active_work_present"))
        or str(row.get("status") or "").lower() == "stale"
    ]
    status_counts: dict[str, int] = {}
    for row in counted_rows:
        status = str(row.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

    oldest_active_age = _max_present(
        row.get("oldest_active_age_seconds") for row in counted_rows
    )
    newest_active_age = _min_present(
        row.get("newest_active_age_seconds") for row in counted_rows
    )
    # Terminal FAILURE-class runtime work (ERROR / FAILED / MAX_RECOVERY*) is a
    # real health failure, not a footnote to an otherwise-green queue. Fold the
    # current failure authority into the verdict at this single point.
    # Historical/superseded failures and CANCELLED worker hygiene remain visible
    # as audit data but must never redden the verdict.
    dead_letter_present = total_failed > 0
    unhealthy = bool(unavailable_rows or dead_letter_present or stale_rows)
    if dlq_unknown is not None:
        # The dead-letter count is an INPUT to this verdict. Without it the
        # queue cannot be called healthy, and it must not be called
        # dead-lettered either. What was measured is still reported.
        unhealthy = bool(unavailable_rows or stale_rows)
        status = (
            "unavailable" if unavailable_rows
            else "stale" if stale_rows
            else "unknown"
        )
    elif unavailable_rows:
        status = "unavailable"
    elif dead_letter_present:
        status = "dead_letter"
    elif stale_rows:
        status = "stale"
    else:
        status = "available"
    if unavailable_rows:
        reason = (
            unavailable_rows[0].get("reason")
            or "runtime_queue_health_unavailable"
        )
    elif dead_letter_present:
        reason = (
            f"runtime_queue_dead_letter ({total_failed} terminal failure-class "
            f"rows [ERROR/FAILED/MAX_RECOVERY*]; {total_cancelled} cancelled "
            f"rows tracked separately, not counted)"
        )
    elif stale_rows:
        reason = stale_rows[0].get("reason") or "runtime_queue_health_stale"
    else:
        reason = None
    return {
        "ok": None if dlq_unknown is not None else not unhealthy,
        "status": status,
        "dlq_status": "unknown" if dlq_unknown is not None else "measured",
        **({"dlq_unknown": dict(dlq_unknown)} if dlq_unknown is not None else {}),
        "source": "okg.v_runtime_queue_health",
        "queue_depth": queue_depth,
        "ignored_job_kinds": sorted(ignored),
        "ignored_runtime_workflow_names": sorted(ignored_workflows),
        "active_queue_depth": active_queue_depth,
        # Gate-visible failure-class counts (drive the verdict + refusals).
        "dlq_count": None if dlq_unknown is not None else total_failed,
        "terminal_history_dlq_count": terminal_history_failed,
        "runtime_failed_count": (
            None if dlq_unknown is not None else total_failed
        ),
        "current_runtime_dlq": (
            dict(dlq_unknown)
            if dlq_unknown is not None
            else {
                **current_dlq,
                "current_count": total_failed,
                "current_failures": current_failures,
            }
        ),
        # Cancels: reported as data only, never folded into the red verdict.
        "runtime_cancelled_count": total_cancelled,
        "runtime_queue_health": rows,
        "runtime_status_counts": status_counts,
        "runtime_pending_count": total_pending,
        "runtime_waiting_count": total_waiting,
        "runtime_delayed_count": total_delayed,
        "runtime_running_count": total_running,
        "terminal_history_count": total_terminal,
        "oldest_active_age_seconds": oldest_active_age,
        "newest_active_age_seconds": newest_active_age,
        **({"reason": reason} if reason is not None else {}),
    }


def queue_status_counts_for_viz(
    summary: Mapping[str, Any],
) -> dict[str, int]:
    """Translate runtime queue counts into the legacy viz status buckets."""
    active = int(summary.get("active_queue_depth") or 0)
    running = int(summary.get("runtime_running_count") or 0)
    queued = max(0, active - running)
    dlq = int(summary.get("dlq_count") or 0)
    terminal = int(summary.get("terminal_history_count") or 0)
    return {
        "queued": queued,
        "leased": running,
        "done": max(0, terminal - dlq),
        "dead_letter": dlq,
    }


def _int(value: Any) -> int:
    if value is None:
        return 0
    return int(value)


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).lower() in {"1", "t", "true", "yes", "y"}


def _ignored_runtime_workflow_names(ignored_job_kinds: set[str]) -> set[str]:
    names: set[str] = set()
    for kind in ignored_job_kinds:
        names.update(RUNTIME_WORKFLOW_NAMES_BY_JOB_KIND.get(kind, ()))
    return names


def _runtime_job_kind(row: Mapping[str, Any]) -> str:
    workflow_name = str(row.get("workflow_name") or "")
    for kind, workflow_names in RUNTIME_WORKFLOW_NAMES_BY_JOB_KIND.items():
        if workflow_name in workflow_names:
            return kind
    return "ingest"


def _max_present(values: Any) -> float | None:
    present = [float(value) for value in values if value is not None]
    return max(present) if present else None


def _min_present(values: Any) -> float | None:
    present = [float(value) for value in values if value is not None]
    return min(present) if present else None
