"""Factory for DBOS scheduled enqueue + apply functions.

Generates the (register_fn, apply_fn) pair that every
`runtime/*_schedule.py` module ships. The 4 schedule modules
(publisher, lens_putback, escalation_sweep, gates_reevaluate)
were 127-164 LOC each of nearly-identical template code per the
post-Wave-D runtime-subagent audit; this factory collapses the
template to one place.

Each call returns a fresh closure-scoped `_DECORATED` cache, so
the generated `register_fn` clears + populates an isolated cache
distinct from every other factory invocation.

Usage from a schedule module:

    SCHEDULE_ID = "okg_some_sweep"
    DEFAULT_CRON = "*/5 * * * *"
    register_some_schedule, apply_some_schedule = make_enqueue_schedule(
        schedule_id=SCHEDULE_ID,
        default_cron=DEFAULT_CRON,
        workflow_module="okg.substrate.runtime.some_workflow",
        workflow_name_attr="SOME_WORKFLOW_NAME",
        log_label="some sweep",
    )
"""
from __future__ import annotations

import importlib
import logging
from datetime import datetime
from typing import Any, Callable


log = logging.getLogger(__name__)


def make_enqueue_schedule(
    *,
    schedule_id: str,
    default_cron: str,
    workflow_module: str,
    workflow_name_attr: str,
    log_label: str,
    workflow_timeout_seconds: float | None = None,
    deduplication_id_fn: Callable[..., str | None] | None = None,
    active_blocking_workflow_id_fn: Callable[..., str | None] | None = None,
    queue_name_fn: Callable[[str], str] | None = None,
) -> tuple[
    Callable[..., None],
    Callable[..., None],
    dict[str, Any],
]:
    """Generate the (register, apply, decorated_cache) triple for one
    scheduled-enqueue workflow.

    Returns:
      register_fn: ``register(*, deployment: str, dsn: str) -> None``.
        Decorates a `@DBOS.workflow(name=f"{schedule_id}_enqueue")`
        wrapper. Call BEFORE `DBOS.launch()`. Clears the cache on
        entry so a fresh worker boot in the same Python process
        (e.g., pytest fixtures) doesn't pick up a stale closure.
      apply_fn: ``apply(*, schedule_overrides: dict[str, str]) -> None``.
        Persists the schedule via `DBOS.apply_schedules`. Call
        AFTER `DBOS.launch()`. Idempotent.
      ``queue_name_fn`` maps the deployment to the queue the workflow
        runs on; the default is the control queue ``okg-<deployment>``.
      decorated: the closure-scoped cache, exposed so tests can
        call ``decorated.clear()`` for repeat-boot scenarios. (The
        prior modules exported a `reset_decorated_cache_for_tests`
        function; that helper is still added at each call site
        for callers that want it.)
    """
    decorated: dict[str, Any] = {}

    def _register(*, deployment: str, dsn: str) -> None:
        from dbos import DBOS

        decorated.clear()

        @DBOS.workflow(name=f"{schedule_id}_enqueue")
        def _enqueue(
            scheduled_time: datetime,
            context: Any,  # noqa: ARG001 — DBOS scheduled-fn signature
        ) -> str | None:
            """Fired by the scheduler. Enqueues one workflow on the
            deployment's partition queue, returns the workflow_id.

            When ``deduplication_id_fn`` is provided, the scheduler
            routes through ``dbos_enqueue.enqueue_deployment_workflow``
            so DBOS's dedupe path collapses bursty cron fires into
            one active workflow per logical bucket (see Wave PACT
            v3 stabilization in dev).
            """
            try:
                mod = importlib.import_module(workflow_module)
                workflow_name = getattr(mod, workflow_name_attr)
                from okg.substrate.runtime.dbos_enqueue import (
                    active_workflow_id_for_name,
                )
                queue_name = (
                    queue_name_fn(deployment)
                    if queue_name_fn is not None
                    else f"okg-{deployment}"
                )
                try:
                    active = active_workflow_id_for_name(
                        dsn=dsn,
                        queue_name=queue_name,
                        workflow_name=workflow_name,
                    )
                except Exception as exc:  # noqa: BLE001
                    active = None
                    log.warning(
                        "scheduled %s active-workflow probe failed: "
                        "deployment=%s scheduled=%s workflow_name=%s "
                        "error=%s; enqueue will proceed",
                        log_label, deployment, scheduled_time,
                        workflow_name, exc,
                    )
                if active is not None:
                    log.info(
                        "scheduled %s enqueue skipped: deployment=%s "
                        "scheduled=%s workflow_name=%s active_workflow_id=%s",
                        log_label, deployment, scheduled_time,
                        workflow_name, active,
                    )
                    return active
                if active_blocking_workflow_id_fn is not None:
                    try:
                        blocking = active_blocking_workflow_id_fn(
                            deployment=deployment,
                            dsn=dsn,
                            workflow_name=workflow_name,
                            scheduled_time=scheduled_time,
                        )
                    except Exception as exc:  # noqa: BLE001
                        blocking = None
                        log.warning(
                            "scheduled %s blocking-workflow probe failed: "
                            "deployment=%s scheduled=%s workflow_name=%s "
                            "error=%s; enqueue will proceed",
                            log_label, deployment, scheduled_time,
                            workflow_name, exc,
                        )
                    if blocking is not None:
                        log.info(
                            "scheduled %s enqueue skipped: deployment=%s "
                            "scheduled=%s workflow_name=%s "
                            "blocking_workflow_id=%s",
                            log_label, deployment, scheduled_time,
                            workflow_name, blocking,
                        )
                        return blocking
                if deduplication_id_fn is not None:
                    from okg.substrate.runtime.dbos_enqueue import (
                        enqueue_deployment_workflow,
                    )
                    dedup_id = deduplication_id_fn(
                        deployment, scheduled_time=scheduled_time,
                    )
                    result = enqueue_deployment_workflow(
                        deployment=deployment,
                        dsn=dsn,
                        workflow_name=workflow_name,
                        deduplication_id=dedup_id,
                        workflow_timeout=workflow_timeout_seconds,
                        queue_name=queue_name,
                    )
                    log.info(
                        "scheduled %s enqueue: deployment=%s "
                        "scheduled=%s workflow_id=%s deduplicated=%s",
                        log_label, deployment, scheduled_time,
                        result.workflow_id, result.deduplicated,
                    )
                    return result.workflow_id

                # Non-dedupe path: keep the lighter-weight direct
                # DBOSClient enqueue for the 3 schedules that don't
                # need bucketed dedupe (lens_putback, escalation_sweep,
                # gates_reevaluate).
                from dbos import DBOSClient
                from okg.substrate.runtime.promotion_fence import (
                    promotion_enqueue_permit,
                )

                client = DBOSClient(
                    database_url=dsn,
                    system_database_url=dsn,
                    application_database_url=dsn,
                )
                enqueue_args: dict[str, Any] = {
                    "queue_name": queue_name,
                    "workflow_name": workflow_name,
                }
                if workflow_timeout_seconds is not None:
                    enqueue_args["workflow_timeout"] = workflow_timeout_seconds
                with promotion_enqueue_permit(deployment=deployment, dsn=dsn):
                    handle = client.enqueue(enqueue_args)
                log.info(
                    "scheduled %s enqueue: deployment=%s "
                    "scheduled=%s workflow_id=%s",
                    log_label, deployment, scheduled_time,
                    handle.workflow_id,
                )
                return handle.workflow_id
            except Exception as e:  # noqa: BLE001 — best-effort, retried next tick
                # Don't crash the scheduler if enqueue fails — log
                # and let the next tick try again. The partition
                # queue's concurrency=1 means duplicate enqueues
                # serialize naturally.
                log.error(
                    "%s enqueue failed: deployment=%s error=%s",
                    log_label, deployment, e,
                )
                return None

        decorated[schedule_id] = _enqueue
        log.info("decorated %s schedule: %s", log_label, schedule_id)

    def _apply(*, schedule_overrides: dict[str, str]) -> None:
        from dbos import DBOS

        if schedule_id not in decorated:
            raise RuntimeError(
                f"apply_{log_label.replace(' ', '_')}_schedule called "
                f"before register_{log_label.replace(' ', '_')}_schedule"
            )

        cron = schedule_overrides.get(schedule_id, default_cron)

        DBOS.apply_schedules([
            {
                "schedule_name": schedule_id,
                "workflow_fn": decorated[schedule_id],
                "schedule": cron,
                "context": None,
                "automatic_backfill": False,
                "cron_timezone": None,
                "queue_name": None,
            },
        ])
        log.info(
            "applied %s schedule: %s cron=%r",
            log_label, schedule_id, cron,
        )

    return _register, _apply, decorated
