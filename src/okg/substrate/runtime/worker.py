"""Long-lived OKG-runtime worker process.

Per Decision 10, OKG runs one DBOS singleton per worker process per
deployment. This module is the entry point invoked by
``okg runtime worker --deployment X``: it constructs the singleton,
registers substrate-owned scheduled workflows, imports any
manifest-declared deployment workflow modules (Phase 2 SDK), calls
``DBOS.launch()`` to bring everything online, and blocks the process
forever waiting for SIGTERM.

The worker process is what runs the doctor schedule, the publish
workflow (Phase 6), the verifier workflow (Phase 4), etc. Short-lived
operator commands enqueue via ``DBOSClient`` from outside this process.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
import importlib
import json
import logging
import math
import os
from pathlib import Path
import signal
import sys
import threading
import time
from typing import Callable

from .config import RuntimeConfig
from .engine import build_dbos_config
from .scheduled import (
    apply_deployment_author_schedules,
    apply_substrate_schedules,
    register_substrate_schedules,
)


__all__ = ["run_worker"]


log = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkerRunMode:
    name: str
    apply_schedules: bool
    start_supervised_services: bool
    drain_and_exit: bool
    suppress_dynamic_scheduler: bool


@dataclass(frozen=True)
class OperatorDrainResult:
    drained: bool
    in_flight_at_timeout: int
    elapsed_seconds: float


@dataclass(frozen=True)
class DBOSShutdownResult:
    """Truthful state at the DBOS teardown boundary."""

    destroyed: bool
    dispatch_stopped: bool
    active_workflow_ids: tuple[str, ...]
    active_workflows_observed: bool = True
    observations: tuple[dict[str, object], ...] = ()


class DBOSActiveWorkflowInspectionError(RuntimeError):
    """The shutdown path could not prove the active DBOS workflow set."""


WORKER_SHUTDOWN_INCOMPLETE_EXIT_CODE = 6
WORKER_RECOVERY_UNATTESTED_EXIT_CODE = 7
SUPERVISED_PRODUCER_STOP_SECONDS = 5.0
SUPERVISED_DISPATCH_STOP_SECONDS = 3.0
SUPERVISED_ACTIVE_WORKFLOW_WAIT_SECONDS = 10.0
SUPERVISED_DBOS_DESTROY_SECONDS = 3.0


def _operator_skip_destroy_on_exit(
    *,
    drain: OperatorDrainResult | None,
    drain_failed: bool,
) -> bool:
    """Return whether operator shutdown should avoid DBOS.destroy.

    Operator mode is allowed to fail or time out while a workflow is already
    active. Calling DBOS.destroy in that state tears down the system database
    under active workflow threads; the safer behavior is to stop dispatch,
    wait for the active set, and skip destroy if it still has work.
    """
    if drain_failed:
        return True
    return bool(
        drain is not None and not drain.drained and drain.in_flight_at_timeout > 0
    )


def _worker_run_mode(*, operator: bool) -> WorkerRunMode:
    if operator:
        return WorkerRunMode(
            name="operator",
            apply_schedules=False,
            start_supervised_services=False,
            drain_and_exit=True,
            suppress_dynamic_scheduler=True,
        )
    return WorkerRunMode(
        name="supervised",
        apply_schedules=True,
        start_supervised_services=True,
        drain_and_exit=False,
        suppress_dynamic_scheduler=False,
    )


def _require_supervised_services_reconciled(
    *,
    deployment: str,
    runtime_cfg: RuntimeConfig,
    registry,
) -> dict[str, object]:
    """Refuse the authoritative worker ack until producers are live."""
    from okg.substrate.runtime.services import (
        supervised_services_reconciliation,
    )

    report = supervised_services_reconciliation(
        deployment=deployment,
        runtime_cfg=runtime_cfg,
        registry=registry,
    )
    if not report["ok"]:
        raise RuntimeError(
            f"supervised source-sync services did not converge: {report['problems']}"
        )
    return report


def _workflow_completion_timeout_seconds(max_wait_seconds: float) -> int:
    return max(1, int(math.ceil(max_wait_seconds)))


def _dbos_instance_or_none() -> object | None:
    try:
        from dbos._dbos import _get_dbos_instance  # type: ignore[import-not-found]

        return _get_dbos_instance()
    except Exception:  # noqa: BLE001
        return None


def _active_dbos_workflow_ids(instance: object | None) -> list[str]:
    if instance is None:
        raise DBOSActiveWorkflowInspectionError(
            "DBOS singleton is unavailable during active-workflow inspection"
        )
    active = getattr(instance, "_active_workflows_set", None)
    if active is None:
        raise DBOSActiveWorkflowInspectionError(
            "DBOS active-workflow set is unavailable"
        )
    active_list = getattr(active, "activeList", None)
    if not callable(active_list):
        raise DBOSActiveWorkflowInspectionError(
            "DBOS active-workflow inspector is unavailable"
        )
    try:
        return [str(workflow_id) for workflow_id in active_list()]
    except Exception as exc:  # noqa: BLE001
        log.exception("failed to inspect DBOS active workflow set")
        raise DBOSActiveWorkflowInspectionError(
            "DBOS active-workflow inspection failed"
        ) from exc


def _stop_dbos_background_dispatch(
    instance: object | None,
    *,
    timeout_seconds: float = 5.0,
    observations: list[dict[str, object]] | None = None,
) -> bool:
    """Signal and join every dedicated DBOS dispatch thread."""
    observation: dict[str, object] = {
        "role": "dbos_dispatch",
        "state": "stop_failed",
        "events_signalled": 0,
        "event_count": 0,
        "thread_count": 0,
        "threads_joined": 0,
        "alive_after": [],
        "errors": [],
    }
    if observations is not None:
        observations.append(observation)
    if instance is None:
        log.error("cannot stop DBOS dispatch: singleton is unavailable")
        observation["errors"] = ["singleton_unavailable"]
        return False

    errors: list[str] = []
    events: list[tuple[str, object]] = []
    for attr in ("poller_stop_events", "background_thread_stop_events"):
        if not hasattr(instance, attr):
            errors.append(f"{attr}:inventory_missing")
            continue
        raw_events = getattr(instance, attr)
        if not isinstance(raw_events, (list, tuple)):
            errors.append(f"{attr}:inventory_invalid")
            continue
        for event in raw_events:
            events.append((attr, event))
    observation["event_count"] = len(events)
    launched = bool(getattr(instance, "_launched", False))
    if launched and not events:
        errors.append("stop_event_inventory_empty")

    events_signalled = 0
    for attr, event in events:
        setter = getattr(event, "set", None)
        is_set = getattr(event, "is_set", None)
        if not callable(setter) or not callable(is_set):
            errors.append(f"{attr}:event_handle_invalid")
            continue
        try:
            setter()
            if bool(is_set()):
                events_signalled += 1
            else:
                errors.append(f"{attr}:event_not_set")
        except Exception:  # noqa: BLE001
            errors.append(f"{attr}:event_stop_failed")
            log.exception("failed to stop DBOS dispatch event")
    observation["events_signalled"] = events_signalled

    raw_threads = getattr(instance, "_background_threads", None)
    if not isinstance(raw_threads, (list, tuple)):
        errors.append("background_thread_inventory_missing")
        background_threads: tuple[object, ...] = ()
    else:
        background_threads = tuple(raw_threads)
    event_count = len(events)
    dispatch_threads: tuple[object, ...] = ()
    if event_count or launched:
        # Some stop events belong to executor-owned startup recovery or
        # post-launch pollers and therefore have no matching entry in
        # _background_threads.  Every dedicated handle that DBOS does expose
        # must still be joined; executor-owned tasks are frozen and drained at
        # the next shutdown boundary.
        if not background_threads:
            errors.append("background_thread_inventory_empty")
        dispatch_threads = background_threads
    if launched:
        system_database = getattr(instance, "_sys_db_field", None)
        cleanup_connections = getattr(system_database, "_cleanup_connections", None)
        if system_database is None or not hasattr(
            system_database, "_run_background_processes"
        ):
            errors.append("notification_listener_stop_authority_missing")
        elif not callable(cleanup_connections):
            errors.append("notification_listener_cleanup_missing")
        else:
            try:
                system_database._run_background_processes = False
                cleanup_connections()
            except Exception:  # noqa: BLE001
                errors.append("notification_listener_stop_failed")
                log.exception("failed to stop DBOS notification listener")
    observation["thread_count"] = len(dispatch_threads)

    valid_threads: list[object] = []
    for thread in dispatch_threads:
        if not callable(getattr(thread, "join", None)) or not callable(
            getattr(thread, "is_alive", None)
        ):
            errors.append("dispatch_thread_handle_invalid")
            continue
        valid_threads.append(thread)

    deadline = time.monotonic() + max(timeout_seconds, 0.0)
    threads_joined = 0
    alive_after: list[str] = []
    for thread in valid_threads:
        try:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
            if thread.is_alive():
                alive_after.append(str(getattr(thread, "name", "unknown")))
            else:
                threads_joined += 1
        except Exception:  # noqa: BLE001
            errors.append("dispatch_thread_join_failed")
            log.exception("failed to join DBOS dispatch thread")
    if alive_after:
        errors.append("dispatch_thread_alive")
    observation["threads_joined"] = threads_joined
    observation["alive_after"] = sorted(alive_after)
    observation["errors"] = sorted(set(errors))

    stopped = not errors
    stopped = bool(
        stopped
        and event_count > 0
        and events_signalled == event_count
        and threads_joined == len(dispatch_threads)
        and bool(dispatch_threads)
    )
    observation["state"] = "stopped_joined" if stopped else "stop_failed"
    if not stopped:
        log.error("DBOS dispatch stop incomplete: %s", observation)
    return stopped


def _freeze_dbos_executor(
    instance: object | None,
    *,
    observation: dict[str, object],
) -> tuple[bool, tuple[object, ...]]:
    """Freeze submissions and cancel work that has not begun executing."""
    observation["executor_frozen"] = False
    observation["executor_thread_count"] = 0
    observation["executor_threads_joined"] = 0
    observation["executor_alive_after"] = []
    if instance is None:
        return False, ()
    launched = bool(getattr(instance, "_launched", False))
    executor = getattr(instance, "_executor_field", None)
    if executor is None:
        # Pre-launch test/operator instances have no executor to freeze.  A
        # launched singleton always owns one and must expose it.
        observation["executor_frozen"] = not launched
        return (not launched), ()
    shutdown = getattr(executor, "shutdown", None)
    if not callable(shutdown):
        return False, ()
    try:
        shutdown(wait=False, cancel_futures=True)
    except Exception:  # noqa: BLE001
        log.exception("failed to freeze DBOS executor")
        return False, ()
    frozen = bool(getattr(executor, "_shutdown", False))
    # ThreadPoolExecutor publishes new worker handles while holding the same
    # shutdown lock that prevents later submissions.  Snapshot only after
    # shutdown returns so a racing submit cannot create an unjoined thread
    # between inventory and the freeze boundary.
    raw_threads = getattr(executor, "_threads", None)
    if not isinstance(raw_threads, (set, list, tuple)):
        return False, ()
    threads = tuple(raw_threads)
    if any(
        not callable(getattr(thread, "join", None))
        or not callable(getattr(thread, "is_alive", None))
        for thread in threads
    ):
        return False, ()
    observation["executor_frozen"] = frozen
    observation["executor_thread_count"] = len(threads)
    return frozen, threads


def _join_dbos_executor_threads(
    threads: tuple[object, ...],
    *,
    timeout_seconds: float,
    observation: dict[str, object],
    resignal: Callable[[], bool] | None = None,
) -> bool:
    """Prove no executor task can enter the active set after inspection."""
    if not threads:
        resignal_ok = resignal is None or resignal()
        observation["executor_threads_joined"] = 0
        observation["executor_alive_after"] = []
        if not resignal_ok:
            observation.setdefault("errors", []).append("late_stop_event_signal_failed")
        return resignal_ok
    deadline = time.monotonic() + max(timeout_seconds, 0.0)
    broken: set[int] = set()
    while True:
        if resignal is not None and not resignal():
            observation.setdefault("errors", []).append("late_stop_event_signal_failed")
            break
        alive_threads = [
            thread
            for index, thread in enumerate(threads)
            if index not in broken and thread.is_alive()
        ]
        if not alive_threads or time.monotonic() >= deadline:
            break
        for index, thread in enumerate(threads):
            if thread not in alive_threads:
                continue
            try:
                thread.join(timeout=min(0.05, max(0.0, deadline - time.monotonic())))
            except Exception:  # noqa: BLE001
                broken.add(index)
                log.exception("failed to join DBOS executor thread")
    if resignal is not None and not resignal():
        observation.setdefault("errors", []).append("late_stop_event_signal_failed")
    alive = [
        str(getattr(thread, "name", "unknown"))
        for index, thread in enumerate(threads)
        if index in broken or thread.is_alive()
    ]
    joined = len(threads) - len(alive)
    observation["executor_threads_joined"] = joined
    observation["executor_alive_after"] = sorted(alive)
    return not alive and joined == len(threads)


def _resignal_dbos_stop_events(
    instance: object | None,
    *,
    observation: dict[str, object],
) -> bool:
    """Signal a stable post-freeze inventory, including late recovery events."""
    if instance is None:
        return False
    events: list[object] = []
    for attr in ("poller_stop_events", "background_thread_stop_events"):
        raw = getattr(instance, attr, None)
        if not isinstance(raw, (list, tuple)):
            return False
        events.extend(raw)
    signalled = 0
    try:
        for event in events:
            setter = getattr(event, "set", None)
            is_set = getattr(event, "is_set", None)
            if not callable(setter) or not callable(is_set):
                return False
            setter()
            if not bool(is_set()):
                return False
            signalled += 1
    except Exception:  # noqa: BLE001
        log.exception("failed to signal late DBOS stop event")
        return False
    observation["event_count"] = len(events)
    observation["events_signalled"] = signalled
    return bool(events and signalled == len(events))


def _wait_for_active_dbos_workflows(
    *,
    deployment: str,
    instance: object | None,
    poll_interval_seconds: float = 1.0,
    timeout_seconds: float | None = 30.0,
) -> list[str]:
    deadline = (
        None
        if timeout_seconds is None
        else time.monotonic() + max(timeout_seconds, 0.0)
    )
    while True:
        active = _active_dbos_workflow_ids(instance)
        if not active:
            return []
        now = time.monotonic() if deadline is not None else None
        if deadline is not None and now is not None and now >= deadline:
            log.warning(
                "timed out waiting for %d active DBOS workflow(s) "
                "before destroy: deployment=%s timeout_seconds=%.3f "
                "workflow_ids=%s",
                len(active),
                deployment,
                timeout_seconds,
                active,
            )
            return active
        log.info(
            "waiting for %d active DBOS workflow(s) before destroy: "
            "deployment=%s workflow_ids=%s",
            len(active),
            deployment,
            active,
        )
        if deadline is None or now is None:
            time.sleep(poll_interval_seconds)
        else:
            time.sleep(min(poll_interval_seconds, max(deadline - now, 0.0)))


def _destroy_dbos_gracefully(
    *,
    deployment: str,
    dsn: str | None = None,
    dbos_schema: str = "dbos",
    active_wait_timeout_seconds: float | None = 30.0,
    poll_interval_seconds: float = 1.0,
    dbos_destroy_timeout_seconds: float = 5.0,
    skip_destroy_on_active_timeout: bool = False,
    dispatch_stop_timeout_seconds: float = 5.0,
) -> bool:
    return _shutdown_dbos_gracefully(
        deployment=deployment,
        dsn=dsn,
        dbos_schema=dbos_schema,
        active_wait_timeout_seconds=active_wait_timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        dbos_destroy_timeout_seconds=dbos_destroy_timeout_seconds,
        skip_destroy_on_active_timeout=skip_destroy_on_active_timeout,
        dispatch_stop_timeout_seconds=dispatch_stop_timeout_seconds,
    ).destroyed


def _shutdown_dbos_gracefully(
    *,
    deployment: str,
    dsn: str | None = None,
    dbos_schema: str = "dbos",
    active_wait_timeout_seconds: float | None = 30.0,
    poll_interval_seconds: float = 1.0,
    dbos_destroy_timeout_seconds: float = 5.0,
    skip_destroy_on_active_timeout: bool = False,
    dispatch_stop_timeout_seconds: float = 5.0,
    observation_sequence_start: int = 1,
) -> DBOSShutdownResult:
    from dbos import DBOS

    instance = _dbos_instance_or_none()
    observations: list[dict[str, object]] = []
    dispatch_stopped = _stop_dbos_background_dispatch(
        instance,
        timeout_seconds=dispatch_stop_timeout_seconds,
        observations=observations,
    )
    executor_threads: tuple[object, ...] = ()
    if dispatch_stopped:
        executor_frozen, executor_threads = _freeze_dbos_executor(
            instance,
            observation=observations[0],
        )
        if not executor_frozen:
            errors = observations[0].get("errors")
            if isinstance(errors, list):
                errors.append("executor_freeze_failed")
            observations[0]["state"] = "stop_failed"
            dispatch_stopped = False
    observations[0]["sequence"] = observation_sequence_start
    if not dispatch_stopped:
        log.error(
            "refusing DBOS workflow inspection and destroy because dispatch "
            "stop was not attested: deployment=%s",
            deployment,
        )
        observations.extend(
            (
                {
                    "sequence": observation_sequence_start + 1,
                    "role": "dbos_active_workflows",
                    "state": "not_inspected",
                    "task_ids": [],
                },
                {
                    "sequence": observation_sequence_start + 2,
                    "role": "dbos_teardown",
                    "state": "not_attempted",
                },
            )
        )
        return DBOSShutdownResult(
            destroyed=False,
            dispatch_stopped=False,
            active_workflow_ids=(),
            active_workflows_observed=False,
            observations=tuple(observations),
        )
    active_observed = True
    try:
        drained_active = _wait_for_active_dbos_workflows(
            deployment=deployment,
            instance=instance,
            poll_interval_seconds=poll_interval_seconds,
            timeout_seconds=active_wait_timeout_seconds,
        )
    except DBOSActiveWorkflowInspectionError:
        drained_active = []
        active_observed = False
        log.exception(
            "pre-drain DBOS workflow observation failed; the post-join "
            "authoritative readback must still decide teardown: "
            "deployment=%s",
            deployment,
        )

    # A workflow that is already active owns one of the executor threads.  It
    # cannot be joined while DBOS is deliberately left intact for supervised
    # crash recovery.  Dedicated dispatch is nevertheless stopped and the
    # executor is frozen, so return that truthful boundary without turning a
    # live workflow into a false dispatch-stop failure.  The caller may then
    # persist the controlled-recovery attestation and terminate atomically.
    if active_observed and drained_active:
        observations[0]["state"] = "stopped_active_recovery_pending"
        observations[0]["executor_alive_after"] = sorted(
            str(getattr(thread, "name", "unknown"))
            for thread in executor_threads
            if thread.is_alive()
        )
        observations.append(
            {
                "sequence": observation_sequence_start + 1,
                "role": "dbos_active_workflows",
                "state": "remaining",
                "task_ids": list(sorted(drained_active)),
            }
        )
        observations.append(
            {
                "sequence": observation_sequence_start + 2,
                "role": "dbos_teardown",
                "state": "not_attempted",
            }
        )
        return DBOSShutdownResult(
            destroyed=False,
            dispatch_stopped=True,
            active_workflow_ids=tuple(sorted(drained_active)),
            active_workflows_observed=True,
            observations=tuple(observations),
        )

    executor_drained = _join_dbos_executor_threads(
        executor_threads,
        timeout_seconds=dispatch_stop_timeout_seconds,
        observation=observations[0],
        resignal=lambda: _resignal_dbos_stop_events(
            instance,
            observation=observations[0],
        ),
    )
    observations[0]["state"] = (
        "stopped_joined" if executor_drained else "stopped_executor_live"
    )
    try:
        final_active = _active_dbos_workflow_ids(instance)
    except DBOSActiveWorkflowInspectionError:
        final_active = []
        active_observed = False
    observations.append(
        {
            "sequence": observation_sequence_start + 1,
            "role": "dbos_active_workflows",
            "state": (
                "inspection_unknown"
                if not active_observed
                else "remaining"
                if final_active
                else "empty"
            ),
            "task_ids": list(sorted(final_active)),
        }
    )
    if not executor_drained or not active_observed or final_active:
        observations.append(
            {
                "sequence": observation_sequence_start + 2,
                "role": "dbos_teardown",
                "state": "not_attempted",
            }
        )
        return DBOSShutdownResult(
            destroyed=False,
            dispatch_stopped=dispatch_stopped,
            active_workflow_ids=tuple(sorted(final_active)),
            active_workflows_observed=active_observed,
            observations=tuple(observations),
        )
    try:
        if dsn is None:
            if bool(getattr(instance, "_launched", False)):
                raise DBOSActiveWorkflowInspectionError(
                    "launched DBOS shutdown lacks durable workflow readback authority"
                )
            durable_nonterminal: list[str] = []
        else:
            durable_nonterminal = _durable_nonterminal_runtime_workflow_ids(
                dsn=dsn,
                deployment=deployment,
                dbos_schema=dbos_schema,
            )
    except Exception:  # noqa: BLE001 - durable uncertainty is fail-closed.
        log.exception(
            "refusing DBOS destroy because durable workflow state is unknown: "
            "deployment=%s",
            deployment,
        )
        observations[-1] = {
            **observations[-1],
            "state": "durable_inspection_unknown",
            "task_ids": [],
        }
        observations.append(
            {
                "sequence": observation_sequence_start + 2,
                "role": "dbos_teardown",
                "state": "not_attempted",
            }
        )
        return DBOSShutdownResult(
            destroyed=False,
            dispatch_stopped=dispatch_stopped,
            active_workflow_ids=(),
            active_workflows_observed=False,
            observations=tuple(observations),
        )
    if durable_nonterminal:
        observations[-1] = {
            **observations[-1],
            "state": "durable_remaining",
            "task_ids": list(sorted(durable_nonterminal)),
        }
        observations.append(
            {
                "sequence": observation_sequence_start + 2,
                "role": "dbos_teardown",
                "state": "not_attempted",
            }
        )
        return DBOSShutdownResult(
            destroyed=False,
            dispatch_stopped=dispatch_stopped,
            active_workflow_ids=tuple(sorted(durable_nonterminal)),
            observations=tuple(observations),
        )
    observations[-1] = {**observations[-1], "durable_state": "empty"}
    try:
        destroyed = _call_dbos_destroy_with_timeout(
            DBOS,
            timeout_seconds=dbos_destroy_timeout_seconds,
        )
    except Exception:  # noqa: BLE001
        destroyed = False
        log.exception("DBOS.destroy failed: deployment=%s", deployment)
    observations.append(
        {
            "sequence": observation_sequence_start + 2,
            "role": "dbos_teardown",
            "state": "destroyed" if destroyed else "destroy_incomplete",
        }
    )
    return DBOSShutdownResult(
        destroyed=destroyed,
        dispatch_stopped=dispatch_stopped,
        active_workflow_ids=tuple(sorted(final_active)),
        observations=tuple(observations),
    )


def _durable_nonterminal_runtime_workflow_ids(
    *,
    dsn: str,
    deployment: str,
    dbos_schema: str,
) -> list[str]:
    """Read back durable work that a frozen executor may have cancelled."""
    from okg.substrate.db.connection import connect as db_connect
    from psycopg import sql

    from okg.substrate.runtime.partition_queue import (
        deployment_runtime_queue_names,
    )

    query = sql.SQL(
        """
        SELECT workflow_uuid
          FROM {}.workflow_status
         WHERE queue_name = ANY(%s)
           AND status IN ('ENQUEUED', 'PENDING', 'DELAYED')
         ORDER BY workflow_uuid
        """
    ).format(sql.Identifier(dbos_schema))
    with db_connect(dsn, connect_timeout=5) as conn, conn.cursor() as cur:
        cur.execute(query, (list(deployment_runtime_queue_names(deployment)),))
        return [str(row[0]) for row in cur.fetchall()]


def _attest_supervised_controlled_recovery(
    *,
    deployment: str,
    producers_stopped: bool,
    shutdown: DBOSShutdownResult,
) -> bool:
    """Attest the narrow worker-owned crash-recovery boundary.

    Absence, staleness, or mismatch of a verifier request is a refusal, never
    an implicit opt-in. The response and unbuffered marker must both persist
    before the CLI is allowed to own the final ``os._exit``.
    """
    if (
        not shutdown.active_workflow_ids
        or not shutdown.active_workflows_observed
        or not producers_stopped
        or not shutdown.dispatch_stopped
        or shutdown.destroyed
    ):
        return False
    from okg.substrate.runtime.recovery_handoff import (
        ControlledRecoveryHandoffError,
        attest_controlled_recovery_response,
        read_controlled_recovery_request,
    )

    try:
        request = read_controlled_recovery_request(
            deployment,
            os.getpid(),
        )
        if request is None:
            log.error(
                "controlled recovery refused: no request for deployment=%s pid=%d",
                deployment,
                os.getpid(),
            )
            return False
        attest_controlled_recovery_response(
            request,
            active_workflow_ids=shutdown.active_workflow_ids,
            producers_stopped=producers_stopped,
            dispatch_stopped=shutdown.dispatch_stopped,
            dbos_destroyed=False,
        )
    except ControlledRecoveryHandoffError as exc:
        log.error("controlled recovery refused: %s", exc.code)
        return False
    return True


def _supervised_shutdown_returncode(
    *,
    producers_stopped: bool,
    shutdown: DBOSShutdownResult,
    recovery_attested: bool,
) -> int:
    if recovery_attested:
        from okg.substrate.runtime.recovery_handoff import (
            CONTROLLED_RECOVERY_EXIT_CODE,
        )

        return CONTROLLED_RECOVERY_EXIT_CODE
    if shutdown.active_workflows_observed and shutdown.active_workflow_ids:
        return WORKER_RECOVERY_UNATTESTED_EXIT_CODE
    if not (
        producers_stopped
        and shutdown.dispatch_stopped
        and shutdown.destroyed
        and shutdown.active_workflows_observed
    ):
        return WORKER_SHUTDOWN_INCOMPLETE_EXIT_CODE
    return 0


def _call_dbos_destroy_with_timeout(
    dbos_cls: object,
    *,
    timeout_seconds: float,
) -> bool:
    """Destroy DBOS synchronously and attest whether it met the budget.

    A Python thread cannot be safely cancelled. Returning while a daemon
    helper still owns DBOS resources allowed publisher work to encounter a
    half-destroyed singleton. The timeout is therefore a reporting threshold,
    not permission to abandon teardown in the background.
    """
    started = time.monotonic()
    destroy = getattr(dbos_cls, "destroy")
    destroy(workflow_completion_timeout_sec=1)
    elapsed = time.monotonic() - started
    if elapsed > max(timeout_seconds, 0.0):
        log.error(
            "DBOS.destroy finished after its %.3fs shutdown budget "
            "(elapsed_seconds=%.3f)",
            timeout_seconds,
            elapsed,
        )
        return False
    return True


def _shutdown_observation_payload(
    *,
    deployment: str,
    producer_observations: tuple[object, ...],
    shutdown: DBOSShutdownResult,
) -> dict[str, object]:
    observations = [dict(item) for item in producer_observations]
    producer_count = len(observations)
    for item in shutdown.observations:
        row = dict(item)
        row["sequence"] = int(row.get("sequence", 0)) + producer_count
        observations.append(row)
    return {
        "schema": "okg.worker_shutdown.v1",
        "deployment": deployment,
        "clean": bool(
            shutdown.dispatch_stopped
            and shutdown.active_workflows_observed
            and not shutdown.active_workflow_ids
            and shutdown.destroyed
            and all(
                row.get("state") in {"not_started", "stopped_joined"}
                for row in observations[:producer_count]
            )
        ),
        "observations": observations,
    }


def _shutdown_dbos_after_producers(
    *,
    deployment: str,
    producers_stopped: bool,
    dsn: str | None = None,
    dbos_schema: str = "dbos",
) -> DBOSShutdownResult:
    """Enforce the producer-to-DBOS teardown authority boundary."""
    if producers_stopped:
        return _shutdown_dbos_gracefully(
            deployment=deployment,
            dsn=dsn,
            dbos_schema=dbos_schema,
            active_wait_timeout_seconds=(SUPERVISED_ACTIVE_WORKFLOW_WAIT_SECONDS),
            dispatch_stop_timeout_seconds=SUPERVISED_DISPATCH_STOP_SECONDS,
            dbos_destroy_timeout_seconds=SUPERVISED_DBOS_DESTROY_SECONDS,
            skip_destroy_on_active_timeout=True,
        )
    log.error(
        "refusing DBOS dispatch stop, active-workflow inspection, and "
        "destroy because supervised producer stop was not attested: "
        "deployment=%s",
        deployment,
    )
    return DBOSShutdownResult(
        destroyed=False,
        dispatch_stopped=False,
        active_workflow_ids=(),
        active_workflows_observed=False,
        observations=(
            {
                "sequence": 1,
                "role": "dbos_dispatch",
                "state": "not_attempted_producers_live",
            },
            {
                "sequence": 2,
                "role": "dbos_active_workflows",
                "state": "not_inspected",
                "task_ids": [],
            },
            {
                "sequence": 3,
                "role": "dbos_teardown",
                "state": "not_attempted",
            },
        ),
    )


def _cleanup_stale_runtime_wakeups(
    *,
    dsn: str,
    deployment: str,
    phase: str,
) -> dict[str, object]:
    from okg.substrate.runtime.services import (
        cleanup_stale_enqueued_workflows,
        cleanup_stale_pending_workflows,
    )

    report: dict[str, object] = {}
    try:
        pending = cleanup_stale_pending_workflows(
            dsn=dsn,
            deployment=deployment,
            cancel_started_pending=False,
        )
        report["pending"] = pending
        if pending["cancelled_count"]:
            log.warning(
                "stale-PENDING cleanup at %s: cancelled %d zombie workflow(s) — %s",
                phase,
                pending["cancelled_count"],
                pending,
            )
        else:
            log.info("stale-PENDING cleanup at %s: nothing stale", phase)
    except Exception as exc:  # noqa: BLE001
        report["pending_error"] = type(exc).__name__
        log.exception(
            "stale-PENDING cleanup at %s failed; concurrency=1 queue "
            "may be blocked by an unrecoverable PENDING workflow",
            phase,
        )
    # Issue #1257. The sweep above spares any workflow that already started,
    # which is what let a raising publisher cycle hold the only queue slot
    # indefinitely. A row still PENDING past the deadline DBOS recorded for it
    # has no executor honoring that deadline, so release the slot here.
    try:
        from okg.substrate.runtime.workflow_cancel import (
            cleanup_deadline_expired_pending_workflows,
        )

        deadline_expired = cleanup_deadline_expired_pending_workflows(
            dsn=dsn,
            deployment=deployment,
        )
        report["deadline_expired"] = deadline_expired
        if deadline_expired["cancelled_count"]:
            log.warning(
                "deadline-expired PENDING cleanup at %s: cancelled %d "
                "workflow(s) past their own recorded deadline — %s",
                phase,
                deadline_expired["cancelled_count"],
                deadline_expired,
            )
    except Exception as exc:  # noqa: BLE001
        report["deadline_expired_error"] = type(exc).__name__
        log.exception(
            "deadline-expired PENDING cleanup at %s failed; a workflow past "
            "its own deadline may still be holding the concurrency=1 queue "
            "slot",
            phase,
        )
    try:
        enqueued = cleanup_stale_enqueued_workflows(
            dsn=dsn,
            deployment=deployment,
        )
        report["enqueued"] = enqueued
        if enqueued["cancelled_count"]:
            log.warning(
                "stale-ENQUEUED cleanup at %s: cancelled %d old "
                "replaceable wakeup workflow(s) — %s",
                phase,
                enqueued["cancelled_count"],
                enqueued,
            )
        else:
            log.info("stale-ENQUEUED cleanup at %s: nothing stale", phase)
    except Exception as exc:  # noqa: BLE001
        report["enqueued_error"] = type(exc).__name__
        log.exception(
            "stale-ENQUEUED cleanup at %s failed; deployment queue may "
            "drain old replaceable wakeups before current work",
            phase,
        )
    return report


def _drain_operator_queues(
    *,
    deployment: str,
    dsn: str,
    max_wait_seconds: float,
    poll_interval_seconds: float = 1.0,
) -> OperatorDrainResult:
    """Drain explicit operator work on DBOS internal + deployment queues."""
    import time

    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.runtime.partition_queue import (
        partition_queue_name,
        source_sync_queue_name,
    )

    queues = (
        "_dbos_internal_queue",
        partition_queue_name(deployment),
        source_sync_queue_name(deployment),
    )
    deadline = time.monotonic() + max_wait_seconds
    started = time.monotonic()
    last_count = 0
    while True:
        with db_connect(dsn, connect_timeout=5) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM dbos.workflow_status
                 WHERE queue_name = ANY(%s)
                   AND status IN ('ENQUEUED', 'PENDING', 'DELAYED')
                """,
                (list(queues),),
            )
            last_count = int(cur.fetchone()[0])
        if last_count == 0:
            return OperatorDrainResult(
                drained=True,
                in_flight_at_timeout=0,
                elapsed_seconds=time.monotonic() - started,
            )
        if time.monotonic() >= deadline:
            return OperatorDrainResult(
                drained=False,
                in_flight_at_timeout=last_count,
                elapsed_seconds=time.monotonic() - started,
            )
        time.sleep(poll_interval_seconds)


@contextmanager
def _suppress_dbos_dynamic_scheduler(
    *,
    enabled: bool,
    deployment: str | None = None,
    dsn: str | None = None,
    wait_for_fence_release: bool = False,
    expected_fence_owner: str | None = None,
    expected_schedule_snapshot_digest: str | None = None,
):
    """Process-local suppression or fence gate for DBOS's scheduler loop.

    DBOS currently starts its dynamic scheduler unconditionally during
    ``DBOS.launch()``. Operator workers need the queue dispatcher without
    recurring schedule fanout. A candidate worker may register its intended
    schedule rows while a deployment fence is active, but its scheduler thread
    waits until the durable release receipt commits before admitting a tick.
    """
    if not enabled:
        yield
        return

    import dbos._dbos as dbos_module  # type: ignore[import-not-found]

    original = dbos_module.dynamic_scheduler_loop

    def _quiet_dynamic_scheduler_loop(stop_event, polling_interval_sec):
        if wait_for_fence_release:
            assert deployment is not None and dsn is not None
            from okg.substrate.runtime.promotion_fence import (
                promotion_fence_status,
            )

            log.info(
                "DBOS dynamic scheduler waiting for deployment fence release: "
                "deployment=%s",
                deployment,
            )
            while not stop_event.is_set():
                try:
                    fence = promotion_fence_status(
                        deployment=deployment,
                        dsn=dsn,
                    )
                except Exception:  # noqa: BLE001
                    # Losing fence authority must fail closed. The worker stays
                    # available for owner-bound queue recovery, but no schedule
                    # tick is admitted until the database proves reopen.
                    log.exception(
                        "could not revalidate runtime fence; scheduler remains gated: "
                        "deployment=%s",
                        deployment,
                    )
                    fence = {"status": "unreadable"}
                metadata = fence.get("metadata") if isinstance(fence, dict) else None
                release_receipt = (
                    metadata.get("release_receipt")
                    if isinstance(metadata, dict)
                    else None
                )
                release_proven = (
                    isinstance(fence, dict)
                    and fence.get("status") == "released"
                    and fence.get("promotion_id") == expected_fence_owner
                    and isinstance(release_receipt, dict)
                    and release_receipt.get("durable_release_receipt") is True
                    and release_receipt.get("promotion_id") == expected_fence_owner
                    and release_receipt.get("schedule_snapshot_digest")
                    == expected_schedule_snapshot_digest
                )
                if release_proven:
                    log.info(
                        "deployment fence released; starting DBOS dynamic scheduler: "
                        "deployment=%s",
                        deployment,
                    )
                    original(stop_event, polling_interval_sec)
                    return
                stop_event.wait(min(max(float(polling_interval_sec), 0.1), 2.0))
            return
        log.info(
            "DBOS dynamic scheduler suppressed for operator worker (poll_interval=%s)",
            polling_interval_sec,
        )
        stop_event.wait()

    dbos_module.dynamic_scheduler_loop = _quiet_dynamic_scheduler_loop
    try:
        yield
    finally:
        dbos_module.dynamic_scheduler_loop = original


def _worker_code_watch_roots(deployment: str) -> list[Path]:
    """Directories outside the ``okg`` package whose modules to watch.

    A deployment's source adapters usually live inside the checkout (and
    are therefore already covered), but a deployment directory resolved
    elsewhere would otherwise be invisible to the staleness scan.
    Resolution failure returns an empty list — the scan then covers the
    package/checkout roots only, and says so via the roots it reports.
    """
    try:
        from okg.substrate.deployments import find_deployment_dir

        deployment_dir = find_deployment_dir(deployment)
    except Exception:  # noqa: BLE001 - never block worker boot on this.
        return []
    return [deployment_dir] if deployment_dir is not None else []


@contextmanager
def _unwind_default_sigterm():
    """Make setup-time default SIGTERM unwind the reservation boundary."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    previous = signal.getsignal(signal.SIGTERM)
    installed = previous is signal.SIG_DFL

    def _raise_system_exit(signum, _frame) -> None:
        raise SystemExit(128 + signum)

    if installed:
        signal.signal(signal.SIGTERM, _raise_system_exit)
    try:
        yield
    finally:
        # run_worker installs its long-lived graceful handler once setup is
        # complete. Do not replace that newer handler with stale default state.
        if installed and signal.getsignal(signal.SIGTERM) is _raise_system_exit:
            signal.signal(signal.SIGTERM, previous)


def _own_connection_reservation(run):
    """Wrap the inventoried worker boundary with reservation cleanup."""

    @wraps(run)
    def _wrapped(
        *,
        deployment: str,
        dsn: str,
        runtime: RuntimeConfig,
        operator: bool = False,
        max_wait_seconds: float = 600.0,
    ) -> int:
        from okg.substrate.deployments.admission import require_active_deployment

        from okg.substrate.deployments import DeploymentResolutionError

        try:
            require_active_deployment(deployment)
        except DeploymentResolutionError as exc:
            log.error("worker deployment authority is invalid: %s", exc)
            return 2
        # Embedding configuration and actual backend capability are admitted
        # before signal setup, wrapper cleanup, DBOS imports/registration, or
        # graph access. A refusal therefore cannot mutate runtime state.
        embedding_plan = None
        if runtime.enabled:
            embedding_plan = _preflight_embedding_plan(deployment)
        with _unwind_default_sigterm():
            try:
                return run(
                    deployment=deployment,
                    dsn=dsn,
                    runtime=runtime,
                    operator=operator,
                    max_wait_seconds=max_wait_seconds,
                    _embedding_plan=embedding_plan,
                )
            finally:
                from okg.substrate.db.capacity import release_process_reservations

                release_process_reservations(
                    application_name=f"okg-worker-{deployment}",
                )

    return _wrapped


def _preflight_embedding_plan(deployment: str):
    """Resolve and probe the one embedding plan before worker effects."""
    from okg.substrate.deployments import resolve_deployment
    from okg.substrate.embeddings import build_embedding_execution_plan

    manifest = resolve_deployment(deployment)
    # The worker never embeds with a paid backend (should_register_embed), so
    # it must not pay for an unrecorded capability probe either.
    return build_embedding_execution_plan(
        manifest.embedding_config, paid_backend="absent",
    )


def _sync_edge_precedence(*, deployment: str, dsn: str) -> None:
    """Copy the deployment's edge ranking into ``okg.edge_precedence``.

    Item 92a: the graph writer stores the highest-ranked producer's version
    of a shared edge and reads the ranking from that table, because it is
    built in places that never see deployment.yaml. ``okg migrate`` copies it
    too; doing it at boot means a ranking edit takes effect with the restart
    that loads the rest of the manifest. A malformed block refuses the start.
    """
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store_internal.edge_declarers import (
        apply_deployment_edge_precedence,
    )

    # One transaction: the ranking and the removal of the declarations it no
    # longer covers commit together, so a refused removal leaves the old
    # ranking in place and the next start tries again (item 92a review).
    with db_connect(dsn, deployment=deployment) as conn:
        store = ChronosGraphStore.from_connection(
            conn, ensure_schema=False, autocommit=False,
        )
        outcome = apply_deployment_edge_precedence(conn, store, deployment)
        conn.commit()
    if outcome is None:
        log.warning(
            "edge precedence not synced: no deployment directory resolved "
            "for %s; okg.edge_precedence keeps what okg migrate last wrote",
            deployment,
        )
    elif outcome.get("changed") or outcome.get("declarations_purged"):
        log.info(
            "edge precedence for %s ranks %s (was %s); removed %s "
            "declarations the ranking does not cover",
            deployment,
            outcome.get("edge_types"),
            outcome.get("previous_edge_types"),
            outcome.get("declarations_purged"),
        )


def _preflight_enricher_contracts(*, deployment: str, dsn: str) -> None:
    """Validate the active enricher contracts before DBOS admits any work.

    Loads the same enricher set a publisher cycle would and checks the
    static derived-node liveness contract over it, so a node-writing
    enricher with no valid ``support_policy`` declaration (issue #1256)
    refuses the worker start instead of raising inside
    ``okg_publisher_cycle``, where it holds the publish lock. The rest of
    the write-envelope contract is unchanged: it is enforced at Publisher
    construction, one cycle later.

    Two limits are deliberate and NOT covered here. The enrichers are
    constructed once for this check and discarded, so a constructor with
    side effects runs one extra time at boot. And ``find_deployment_dir``
    resolves the release active at boot: activating a different release
    later swaps in enricher code this preflight never saw, and
    ``activate-release`` does not restart the worker, so that path is
    covered only by the per-cycle validation in ``Publisher.__init__``.
    """
    from okg.substrate.deployments import find_deployment_dir
    from okg.substrate.publisher.enricher_loader import (
        active_enricher_profiles_from_env,
        load_enrichers,
        validate_enricher_support_policies,
    )

    deployment_dir = find_deployment_dir(deployment)
    if deployment_dir is None:
        # Not silent: the worker paths that require a resolved deployment
        # directory raise on their own, but an unchecked enricher set must
        # be visible in the log rather than read as agreement.
        log.warning(
            "enricher contract preflight skipped: no deployment directory "
            "resolved for %s; enricher contracts are validated at the first "
            "publisher cycle instead",
            deployment,
        )
        return
    validate_enricher_support_policies(load_enrichers(
        deployment_dir,
        active_profiles=active_enricher_profiles_from_env(),
        runtime_dsn=dsn,
        deployment=deployment,
    ))


@_own_connection_reservation
def run_worker(
    *,
    deployment: str,
    dsn: str,
    runtime: RuntimeConfig,
    operator: bool = False,
    max_wait_seconds: float = 600.0,
    _embedding_plan=None,
) -> int:
    """Run the worker process for ``deployment`` until SIGTERM.

    Sequence per the documented DBOS init lifecycle (constructor →
    decorate → launch):

      1. Validate the deployment's registered enricher contracts before
         DBOS can admit any work (issue #1256).
      2. Construct the :class:`DBOS` singleton with the deployment's DSN.
      3. Register substrate-owned scheduled workflows (the doctor
         schedule).
      4. Import deployment-author workflow modules listed in the
         manifest's ``runtime.workflows``. SDK decorators in those
         modules register their workflows with the singleton.
         (Phase 1: ``runtime.workflows`` is typically empty since the
         SDK lands in Phase 2; this branch is forward-compatible.)
      5. Call :meth:`DBOS.launch` to start workers. In supervised mode,
         also bring schedules and supervised services online. In operator
         mode, suppress recurring schedules, drain explicit queued work,
         and exit.
      6. In supervised mode, block on a ``threading.Event`` until
         SIGTERM/SIGINT, then call :meth:`DBOS.destroy` and return.

    Returns the process exit code.
    """
    from okg.substrate.deployments.admission import require_active_deployment

    require_active_deployment(deployment)
    if not runtime.enabled:
        log.error(
            "runtime is not enabled for deployment %s; cannot run worker",
            deployment,
        )
        return 2
    if _embedding_plan is None:
        raise RuntimeError(
            "worker embedding preflight did not produce an execution plan"
        )

    # A stable launcher verifies the complete tree before this candidate module
    # is imported. Repeat the environment/artifact/root binding here before the
    # first DBOS import or mutation. A partially populated sealed environment is
    # an error, never permission to fall back to checkout authority.
    from okg.substrate.errors import SealedReleaseInvalid
    from okg.substrate.runtime.release_authority import (
        verify_worker_release_authority,
    )

    try:
        sealed_authority = verify_worker_release_authority(
            deployment=deployment,
            dsn=dsn,
        )
    except SealedReleaseInvalid as exc:
        log.error("refusing sealed worker start: %s", exc)
        return 2

    from okg.substrate.deployments import resolve_deployment
    from okg.substrate.deployments.sync_root import (
        require_resolved_sync_root,
        require_usable_source_configuration_authority,
    )
    from okg.substrate.errors import SubstrateError

    _manifest_for_gates = resolve_deployment(deployment)
    try:
        # Source configuration is loaded during worker boot for poll schedule
        # registration and deployment-apply acknowledgment.  Distinguish an
        # unresolved declaration from an intentional absence, then require a
        # usable canonical root before the sealed authority report, code
        # identity, DBOS, registry, or producer can write anything.
        require_resolved_sync_root(
            _manifest_for_gates,
            command="source-sync worker startup",
        )
        _source_registry_repo_root = require_usable_source_configuration_authority(
            _manifest_for_gates.sync_root,
            command="source-sync worker startup",
        )
    except SubstrateError as exc:
        log.error(
            "refusing worker start [%s]: %s | fix: %s | data: %s",
            exc.code,
            exc,
            exc.fix_hint,
            exc.data,
        )
        return 2

    if sealed_authority is None:
        # Tracking/dev workers retain the Tier W checkout guard. Production-like
        # environment descriptors require sealed mode and enter the branch above.
        from okg.substrate.deployment_authority import check_publish_provenance

        provenance = check_publish_provenance(deployment, dsn)
        if not provenance.get("allowed"):
            log.error(
                "refusing worker start: %s | legal moves: %s",
                provenance.get("reason"),
                "; ".join(provenance.get("legal_moves") or [])
                or "start the worker from the registered sync root",
            )
            return 2
        if provenance.get("warning"):
            log.warning(provenance["warning"])
        log.info(
            "worker provenance: mode=%s checkout=%s sync_root=%s "
            "sync_root_source=%s identity=%s",
            provenance.get("mode"),
            provenance.get("checkout"),
            provenance.get("sync_root"),
            # Untracked (`local_profile`) Tier W authority must be visible in
            # the worker log, not only in the manifest diff (issue #1014).
            provenance.get("sync_root_source"),
            (provenance.get("target") or {}).get("identity"),
        )

    # Issue #945: freeze what this process is about to load. The worker
    # holds its imported modules for life, so an edit to the checkout it
    # runs from changes nothing it executes; without this snapshot no
    # check can tell the two apart. Recorded BEFORE the substrate's
    # workflow modules are imported so the import observer sees them.
    from okg.substrate.runtime.code_identity import (
        record_worker_code_identity,
    )

    record_worker_code_identity(
        deployment=deployment,
        extra_roots=_worker_code_watch_roots(deployment),
    )

    # Issue #1256: a node-writing enricher with no declared support policy
    # refuses the boot here, naming the enricher, instead of raising once
    # per candidate inside a publisher cycle that holds the publish lock.
    from okg.substrate.errors import SubstrateError as _SubstrateError

    try:
        _preflight_enricher_contracts(deployment=deployment, dsn=dsn)
    except _SubstrateError as exc:
        log.error(
            "refusing worker start [%s]: %s | fix: %s | data: %s",
            exc.code,
            exc,
            exc.fix_hint,
            exc.data,
        )
        return 2

    from dbos import DBOS

    mode = _worker_run_mode(operator=operator)
    log.info(
        "worker starting: deployment=%s schema=%s sdk_version=%s mode=%s",
        deployment,
        runtime.dbos_schema,
        runtime.sdk_version,
        mode.name,
    )

    # Phase 1: defensive destroy (no-op if no singleton live).
    from okg.substrate.runtime.engine import defensive_destroy

    defensive_destroy()

    # Item 92a: the graph writer reads the deployment's edge ranking from
    # okg.edge_precedence; copy it before any work runs. A malformed block
    # refuses the start.
    import psycopg as _psycopg

    try:
        _sync_edge_precedence(deployment=deployment, dsn=dsn)
    except _SubstrateError as exc:
        log.error(
            "refusing worker start [%s]: %s | fix: %s | data: %s",
            exc.code,
            exc,
            exc.fix_hint,
            exc.data,
        )
        return 2
    except _psycopg.Error as exc:
        # A database error while copying the ranking (a lock timeout, a lost
        # connection) refuses the start like any other, without a traceback.
        log.error(
            "refusing worker start [edge_precedence_sync_failed]: %s: %s | "
            "fix: check the database and restart the worker; the ranking "
            "and its purge commit together, so nothing was half-applied",
            type(exc).__name__,
            exc,
        )
        return 2

    cfg = build_dbos_config(
        deployment=deployment,
        dsn=dsn,
        runtime=runtime,
    )
    DBOS(config=cfg)

    # Phase 2: decorate substrate-owned workflows.
    # Per the documented DBOS init lifecycle, all @DBOS.workflow
    # decorations must complete before DBOS.launch().
    register_substrate_schedules(deployment=deployment, dsn=dsn)

    # Phase 2b (migrate-substrate-runtime-to-dbos §1.1.3): register
    # the deployment's partition queue (concurrency=1) as the
    # mutual-exclusion mechanism, then decorate the publisher cycle
    # workflow + its scheduled enqueue.
    from okg.substrate.runtime.partition_queue import (
        register_deployment_partition_queue,
        register_source_sync_partition_queue,
    )
    from okg.substrate.runtime.publisher_workflow import (
        register_publisher_workflow,
    )
    from okg.substrate.runtime.publisher_schedule import (
        register_publisher_schedule,
    )

    register_deployment_partition_queue(deployment=deployment)
    register_source_sync_partition_queue(deployment=deployment)
    register_publisher_workflow(deployment=deployment)
    register_publisher_schedule(deployment=deployment, dsn=dsn)

    # Phase 2c (migrate-substrate-runtime-to-dbos §2.1): decorate the
    # source-sync workflow. No scheduled enqueue here — source-sync
    # firings are typically operator-driven via `okg ingest` rather
    # than cron, though deployments can add a `@deployment_scheduled`
    # wrapper in their own workflow modules if they want a cadence.
    from okg.substrate.runtime.source_sync_workflow import (
        register_source_sync_workflow,
    )

    register_source_sync_workflow(deployment=deployment)

    # Phase 2c.1 (`add-admission-envelope`): review-mode admission
    # workflows. Trusted source admission stays inside okg_source_sync;
    # these two workflows cover submit/decide for human and agent
    # proposals.
    from okg.substrate.runtime.admission_workflows import (
        register_admission_workflows,
    )

    register_admission_workflows(
        deployment=deployment,
        approver_config=runtime.admission,
    )

    # Phase 2d (migrate-substrate-runtime-to-dbos §3.1): decorate the
    # embed batch workflow + its scheduled enqueue. Replaces the
    # retired embed poll loop.
    #
    # `consolidate-deployment-runtime`: conditional registration —
    # skip when manifest.embeddings.enabled=false (saves 288 no-op
    # workflows/day per deployment with embedding disabled).
    from okg.substrate.runtime.services import (
        should_register_embed,
        should_register_escalation_sweep,
        should_register_lens_putback,
    )

    # `harden-runtime-supervisor` §E: no defensive try/except here.
    # The manifest was already resolved successfully via build_dbos_config
    # above; if it stopped resolving between then and now, DBOS init is
    # broken anyway and the worker should hard-fail rather than register
    # a "conservative" superset of schedules. Silent-fallback-to-defaults
    # is a P0 anti-pattern in this project.
    # Invalidate a prior process's success before any registry/schedule work,
    # so a stale ack cannot make a failed boot look converged.
    #
    # Only a mode that can re-establish the ack may clear it.  An operator
    # drain runs with `apply_schedules=False` and therefore never reaches
    # `record_worker_acknowledgment` below, so clearing here would strip a
    # healthy deployment's ack with no path to restore it.
    #
    # A failure here is logged and tolerated rather than fatal: refusing to
    # boot over apply bookkeeping would take down publish and source sync,
    # which is a strictly worse outcome than a readiness gate reporting a
    # stale ack.  Withholding the ack is the conservative failure.
    if mode.apply_schedules:
        try:
            from okg.substrate.deployment_apply import (
                clear_worker_acknowledgment,
            )

            clear_worker_acknowledgment(dsn=dsn, deployment=deployment)
        except Exception:  # noqa: BLE001
            log.exception(
                "failed to invalidate the prior deployment-apply worker ack; "
                "continuing startup: %s",
                deployment,
            )
    _register_embed = should_register_embed(_manifest_for_gates)
    if _manifest_for_gates.embedding_config != _embedding_plan.config:
        raise RuntimeError(
            "worker embedding plan disagrees with the current admitted "
            "manifest configuration"
        )
    _register_lens_putback = should_register_lens_putback(dsn=dsn)
    _register_escalation_sweep = should_register_escalation_sweep(
        dsn=dsn,
    )

    # `cleanup-orphan-runtime-schedules`: when a gate is False at this
    # boot but was True at a previous boot, the prior schedule row in
    # `dbos.workflow_schedules` keeps firing on its cron — and the
    # workflow function isn't registered this boot, so DBOS errors
    # every fire. With `concurrency=1` on the partition queue, ONE
    # stuck-PENDING orphan blocks all dispatch. Clean both the
    # schedule row and any orphan ENQUEUED/PENDING workflows under
    # the matching names.
    from okg.substrate.runtime.services import (
        cleanup_disabled_feature_schedules,
    )

    _disabled: dict[str, list[str]] = {}
    if not _register_embed:
        _disabled["okg_embed_batch"] = [
            "okg_embed_batch",
            "okg_embed_batch_enqueue",
        ]
    if not _register_lens_putback:
        _disabled["okg_lens_putback_sweep"] = [
            "okg_lens_putback_sweep",
            "okg_lens_putback_sweep_enqueue",
        ]
    if not _register_escalation_sweep:
        _disabled["okg_escalation_timeout_sweep"] = [
            "okg_escalation_timeout_sweep",
            "okg_escalation_timeout_sweep_enqueue",
        ]
    if _disabled:
        try:
            _cleanup_report = cleanup_disabled_feature_schedules(
                dsn=dsn,
                disabled=_disabled,
            )
            log.info(
                "orphan-schedule cleanup at boot: %s",
                _cleanup_report,
            )
        except Exception:  # noqa: BLE001
            log.exception(
                "orphan-schedule cleanup failed; queue may have "
                "stuck PENDING workflows from a previous boot",
            )

    # `cleanup-orphan-runtime-schedules` follow-up: cancel only stale
    # wakeups which never started. A started PENDING workflow may be a
    # legitimate long run and is never cancelled automatically; the
    # explicit `okg db dbos-gc ... --confirm` path remains available to
    # operators for a diagnosed stuck run.
    from okg.substrate.runtime.services import (
        cleanup_stale_internal_pending_workflows,
    )

    _cleanup_stale_runtime_wakeups(
        dsn=dsn,
        deployment=deployment,
        phase="boot",
    )
    try:
        _stale_internal_report = cleanup_stale_internal_pending_workflows(
            dsn=dsn,
        )
        if _stale_internal_report["cancelled_count"]:
            log.warning(
                "stale-internal-PENDING cleanup at boot: cancelled %d "
                "zombie scheduled workflow(s) — %s",
                _stale_internal_report["cancelled_count"],
                _stale_internal_report,
            )
        else:
            log.info("stale-internal-PENDING cleanup at boot: nothing stale")
    except Exception:  # noqa: BLE001
        log.exception(
            "stale-internal-PENDING cleanup failed; operator drain may "
            "wait on unrecoverable scheduled workflow residue",
        )

    if _register_embed:
        from okg.substrate.runtime.embed_workflow import (
            register_embed_workflow,
            register_embed_schedule,
        )

        register_embed_workflow(
            deployment=deployment,
            embedding_plan=_embedding_plan,
        )
        register_embed_schedule(deployment=deployment, dsn=dsn)
    elif _manifest_for_gates.embedding_config.enabled:
        log.warning(
            "Skipping okg_embed_batch: embedding backend %s is paid; the "
            "worker makes no paid calls. Embed with `okg db embed-backfill "
            "--deployment %s --max-usd <usd>`.",
            _manifest_for_gates.embedding_config.backend, deployment,
        )
    else:
        log.info(
            "Skipping okg_embed_batch: manifest.embeddings.enabled=false",
        )

    # Phase 2e (deepen-dbos-integration Phase 0): decorate the
    # lens-putback sweep workflow + its scheduled enqueue. Drains
    # stale `prepared` rows from `okg.lens_putback_requests` that
    # readers never committed.
    if _register_lens_putback:
        from okg.substrate.runtime.lens_putback_workflow import (
            register_lens_putback_workflow,
        )
        from okg.substrate.runtime.lens_putback_schedule import (
            register_lens_putback_schedule,
        )

        register_lens_putback_workflow(deployment=deployment)
        register_lens_putback_schedule(
            deployment=deployment,
            dsn=dsn,
        )
    else:
        log.info(
            "Skipping okg_lens_putback_sweep: no lens_definitions rows in this DSN",
        )

    # Phase 2f (deepen-dbos-integration Phase 0): decorate the
    # escalation timeout-sweep workflow + its scheduled enqueue.
    # transition_to_timed_out() existed but was never invoked;
    # this wires the missing scheduler.
    if _register_escalation_sweep:
        from okg.substrate.runtime.escalation_sweep_workflow import (
            register_escalation_sweep_workflow,
        )
        from okg.substrate.runtime.escalation_sweep_schedule import (
            register_escalation_sweep_schedule,
        )

        register_escalation_sweep_workflow(deployment=deployment)
        register_escalation_sweep_schedule(
            deployment=deployment,
            dsn=dsn,
        )
    else:
        log.info(
            "Skipping okg_escalation_timeout_sweep: no "
            "escalation_live rows in this DSN",
        )

    # Phase 2i (harden-runtime-supervisor §B): register the gates-
    # reevaluate workflow + schedule. Always registered (it's the
    # late-registration mechanism for the other three conditional
    # schedules), regardless of which feature gates were open at
    # boot.
    from okg.substrate.runtime.gates_reevaluate_workflow import (
        register_gates_reevaluate_workflow,
        reset_decorated_cache_for_tests as _gr_reset,
    )
    from okg.substrate.runtime.gates_reevaluate_schedule import (
        register_gates_reevaluate_schedule,
        reset_decorated_cache_for_tests as _grs_reset,
    )

    _gr_reset()
    _grs_reset()
    register_gates_reevaluate_workflow(deployment=deployment)
    register_gates_reevaluate_schedule(deployment=deployment, dsn=dsn)

    # okg#3112 step 2: the declared-attribute index maintenance runs on its
    # own queue (concurrency 1), so a CREATE INDEX CONCURRENTLY that waits
    # for an older transaction never holds the control queue. Always
    # registered; a run does nothing until an operator's first
    # `okg db attr-index reconcile --apply`.
    from okg.substrate.runtime.partition_queue import (
        register_index_maintenance_queue,
    )
    from okg.substrate.runtime.declared_attr_index_workflow import (
        register_declared_attr_index_workflow,
    )
    from okg.substrate.runtime.declared_attr_index_schedule import (
        register_declared_attr_index_schedule,
    )

    register_index_maintenance_queue(deployment=deployment)
    register_declared_attr_index_workflow(deployment=deployment)
    register_declared_attr_index_schedule(deployment=deployment, dsn=dsn)

    # Phase 2g (migrate-source-sync-triggers-to-dbos): decorate poll
    # workflows for sources that declare `sync.poll`, grouped by cadence
    # so one scheduled tick can evaluate multiple per-source probes
    # without adding one queued DBOS workflow per source.
    _registered_poll_schedules: list[str] = []
    _applied_poll_schedules: list[str] = []
    _source_poll_reconcile_ok = True
    try:
        from okg.substrate.deployments import find_deployment_dir
        from okg.substrate.runtime.source_poll_schedule import (
            cleanup_unregistered_source_poll_schedules,
            register_source_poll_schedules,
        )
        from okg.substrate.sources.registry import (
            load_source_registry,
        )
        from okg.substrate.runtime.config import resolve_source_sync

        _source_sync_cfg = resolve_source_sync(runtime)
        _poll_cfg = _source_sync_cfg.get("poll", {})
        if not _poll_cfg.get("enabled", True):
            _source_poll_cleanup = cleanup_unregistered_source_poll_schedules(
                dsn=dsn,
                registered_schedule_ids=[],
            )
            if _source_poll_cleanup:
                log.info(
                    "source-poll cleanup at boot because "
                    "runtime.source_sync.poll.enabled=false: %s",
                    _source_poll_cleanup,
                )
        elif (_dep_dir := find_deployment_dir(deployment)) is not None:
            _registry_path = _manifest_for_gates.source_registry
            if _registry_path.is_file():
                _registry = load_source_registry(
                    _registry_path,
                    **(
                        {"repo_root_override": _source_registry_repo_root}
                        if _source_registry_repo_root is not None
                        else {}
                    ),
                )
                _poll_backstop_multiplier = int(
                    _poll_cfg.get("backstop_multiplier") or 5,
                )
                _poll_backstop_dependent_fanout = _poll_cfg.get(
                    "backstop_dependent_fanout",
                )
                if not isinstance(_poll_backstop_dependent_fanout, str):
                    _poll_backstop_dependent_fanout = None
                _registered_poll_schedules = register_source_poll_schedules(
                    deployment=deployment,
                    dsn=dsn,
                    registry=_registry,
                    backstop_multiplier=_poll_backstop_multiplier,
                    backstop_dependent_fanout=(_poll_backstop_dependent_fanout),
                    schedule_overrides=dict(runtime.schedules),
                    source_registry_repo_root_override=(_source_registry_repo_root),
                )
                _source_poll_cleanup = cleanup_unregistered_source_poll_schedules(
                    dsn=dsn,
                    registered_schedule_ids=_registered_poll_schedules,
                )
                if _source_poll_cleanup:
                    log.info(
                        "source-poll orphan cleanup at boot: %s",
                        _source_poll_cleanup,
                    )
    except Exception:  # noqa: BLE001
        _source_poll_reconcile_ok = False
        log.exception(
            "failed to register per-source poll schedules — falling "
            "back to no polling for deployment %s",
            deployment,
        )

    # Phase 3: import deployment-author workflow modules. SDK-decorated
    # workflows register with the singleton at import time and record their
    # @deployment_scheduled entries in the author-schedule registry (applied
    # durably in Phase 5c). Reset the registry first so a fresh boot starts
    # from a clean slate rather than any residue from an earlier in-process boot.
    from okg.deployment._internal.decorators import (
        configure_deployment_scheduled_guard,
        reset_deployment_scheduled,
    )
    from okg.substrate.runtime.promotion_fence import (
        promotion_enqueue_permit,
    )

    reset_deployment_scheduled()
    configure_deployment_scheduled_guard(
        lambda promotion_id: promotion_enqueue_permit(
            deployment=deployment,
            dsn=dsn,
            promotion_id=promotion_id,
        )
    )
    for module_path in runtime.workflows:
        try:
            log.info(
                "importing deployment workflow module: %s",
                module_path,
            )
            importlib.import_module(module_path)
        except Exception:  # noqa: BLE001 — workflow-import failure is fatal: log + DBOS.destroy() + exit 3 (refuses to launch with partial workflow set).
            log.exception(
                "failed to import deployment workflow module %s; "
                "refusing to launch DBOS to avoid running with a "
                "partial workflow set",
                module_path,
            )
            DBOS.destroy()
            return 3

    # Phase 4: launch. Candidate schedule definitions may be persisted while
    # fenced, but the DBOS scheduler must not create even an internal scheduled
    # workflow execution until the readiness tail commits the release receipt.
    try:
        from okg.substrate.runtime.promotion_fence import (
            promotion_schedule_snapshot_digest,
            promotion_fence_status,
        )

        runtime_fence = promotion_fence_status(
            deployment=deployment,
            dsn=dsn,
        )
    except Exception:  # noqa: BLE001
        log.exception(
            "could not determine runtime fence authority before worker launch: "
            "deployment=%s",
            deployment,
        )
        DBOS.destroy()
        return 3
    wait_for_fence_release = (
        runtime_fence is not None and runtime_fence.get("status") != "released"
    )
    runtime_fence_owner = (
        str(runtime_fence.get("promotion_id"))
        if wait_for_fence_release and runtime_fence is not None
        else None
    )
    runtime_fence_snapshot_digest = (
        promotion_schedule_snapshot_digest(runtime_fence.get("schedule_snapshot") or [])
        if wait_for_fence_release and runtime_fence is not None
        else None
    )
    # The explicit application-version boundary is a pre-launch cutover.
    # DBOS must not recover or dispatch a workflow before incompatible
    # replaceable wakeups are removed. Since DBOS versions the whole worker,
    # the same transaction refuses readiness if it finds another active row
    # outside the exact cutover policy.
    from dbos._utils import GlobalParams

    live_version = GlobalParams.app_version
    try:
        from okg.substrate.runtime.services import source_sync_replay_boundary

        with source_sync_replay_boundary(
            dsn=dsn,
            deployment=deployment,
            live_version=live_version,
        ) as _source_sync_boundary:
            if _source_sync_boundary["cancelled_count"]:
                log.warning(
                    "application-version replay boundary before launch: "
                    "cancelled %d "
                    "incompatible wakeup(s)",
                    _source_sync_boundary["cancelled_count"],
                )
            with _suppress_dbos_dynamic_scheduler(
                enabled=mode.suppress_dynamic_scheduler or wait_for_fence_release,
                deployment=deployment,
                dsn=dsn,
                wait_for_fence_release=(
                    wait_for_fence_release and not mode.suppress_dynamic_scheduler
                ),
                expected_fence_owner=runtime_fence_owner,
                expected_schedule_snapshot_digest=runtime_fence_snapshot_digest,
            ):
                DBOS.launch()
    except Exception:  # noqa: BLE001
        log.exception(
            "application-version replay boundary failed; refusing worker "
            "readiness: "
            "deployment=%s",
            deployment,
        )
        try:
            DBOS.destroy()
        except Exception:  # noqa: BLE001
            log.exception("DBOS cleanup failed after replay-boundary failure")
        return 3
    log.info("worker launched: deployment=%s", deployment)

    # Phase 4a (2026-05-11 — application_version zombie scheduling):
    # DBOS's scheduled-workflow enqueue path tags each cron firing
    # with ``sys_db.get_latest_application_version()`` (see
    # ``_scheduler.py:_enqueue_scheduled_workflow``), not with the
    # live worker's ``GlobalParams.app_version``. If another worker
    # (now dead) registered a later version_timestamp in
    # ``dbos.application_versions``, every scheduled enqueue from THIS
    # worker gets tagged with that dead version, and the dispatcher's
    # version filter then drops every one. Result: `_dbos_internal_queue`
    # accumulates ENQUEUED zombies forever and crons silently stop
    # running.
    #
    # Fix: at boot, after launch (when GlobalParams.app_version is
    # finalized and registered), bump THIS version's timestamp so it
    # wins ``get_latest_application_version``. Then restamp queued
    # substrate wakeups that were produced just before this worker booted
    # and reap any zombies left from prior worker boots.
    try:
        from okg.substrate.runtime.services import (
            bump_live_version_to_latest,
            reap_stale_internal_enqueued,
            stamp_replaceable_deployment_enqueued,
        )

        bump_live_version_to_latest(dsn=dsn, live_version=live_version)
        _stamped = stamp_replaceable_deployment_enqueued(
            dsn=dsn,
            deployment=deployment,
            live_version=live_version,
        )
        if _stamped["stamped_count"]:
            log.warning(
                "version repair at boot: stamped %d replaceable "
                "deployment wakeup(s) with live application_version",
                _stamped["stamped_count"],
            )
        _reaped = reap_stale_internal_enqueued(
            dsn=dsn,
            live_version=live_version,
        )
        if _reaped["cancelled_count"]:
            log.warning(
                "version-zombie cleanup at boot: cancelled %d ENQUEUED "
                "scheduled workflow(s) from prior worker versions",
                _reaped["cancelled_count"],
            )
    except Exception:  # noqa: BLE001
        log.exception(
            "version-bump / zombie-reap failed at boot; scheduled "
            "crons may not fire if a prior worker's version is the "
            "latest in dbos.application_versions",
        )

    if mode.drain_and_exit:
        drain: OperatorDrainResult | None = None
        drain_failed = False
        try:
            drain = _drain_operator_queues(
                deployment=deployment,
                dsn=dsn,
                max_wait_seconds=max_wait_seconds,
            )
            if drain.drained:
                log.info(
                    "operator worker drained queue: deployment=%s elapsed_seconds=%.3f",
                    deployment,
                    drain.elapsed_seconds,
                )
                return 0
            log.error(
                "operator worker timed out while draining queue: "
                "deployment=%s in_flight=%d elapsed_seconds=%.3f",
                deployment,
                drain.in_flight_at_timeout,
                drain.elapsed_seconds,
            )
            return 4
        except Exception:  # noqa: BLE001
            drain_failed = True
            log.exception(
                "operator worker failed while draining queue: deployment=%s",
                deployment,
            )
            return 5
        finally:
            log.info("operator worker shutting down: deployment=%s", deployment)
            skip_destroy = _operator_skip_destroy_on_exit(
                drain=drain,
                drain_failed=drain_failed,
            )
            destroyed = _destroy_dbos_gracefully(
                deployment=deployment,
                dsn=dsn,
                dbos_schema=runtime.dbos_schema,
                active_wait_timeout_seconds=None if skip_destroy else 30.0,
                skip_destroy_on_active_timeout=skip_destroy,
            )
            if destroyed:
                log.info("operator worker exited: deployment=%s", deployment)
            else:
                log.error(
                    "operator worker shutdown incomplete: deployment=%s",
                    deployment,
                )

    # Phase 4b (consolidate-deployment-runtime): start supervised
    # source-sync trigger producers (embedded Watcher + webhook server)
    # as daemon threads in this same process. Reads
    # `runtime.source_sync` to decide which to start. Failures starting
    # one don't block the worker — the doctor surfaces them.
    if mode.start_supervised_services:
        from okg.substrate.runtime.services import (
            start_supervised_services,
        )

        try:
            start_supervised_services(
                deployment=deployment,
                dsn=dsn,
                runtime_cfg=runtime,
            )
        except Exception:  # noqa: BLE001
            log.exception(
                "start_supervised_services raised at top level; worker "
                "continues running without supervised triggers",
            )
    else:
        log.info("operator worker skipping supervised source-sync services")

    # Phase 5: persist substrate-owned schedules. apply_schedules
    # requires the system DB to be live, so this MUST run after launch.
    # Manifest-overridable cron per Decision 14.
    if mode.apply_schedules:
        apply_substrate_schedules(
            schedule_overrides=dict(runtime.schedules),
        )

        # Phase 5b: persist the publisher schedule (separate from the
        # doctor schedule so each module owns its own register+apply pair).
        from okg.substrate.runtime.publisher_schedule import (
            apply_publisher_schedule,
        )

        apply_publisher_schedule(schedule_overrides=dict(runtime.schedules))

        # Phase 5c (harden-okg-ci-runtime): persist durable schedule rows for
        # deployment-author @deployment_scheduled workflows imported in Phase 3.
        # Without this walk the author schedule gets no workflow_schedules row
        # and never runs — the prior decorator armed DBOS's native scheduler,
        # which errored every tick (okg_ci_resource_monitor, 2026-06-16).
        _author_schedule_ids = apply_deployment_author_schedules(
            schedule_overrides=dict(runtime.schedules),
        )
        if _author_schedule_ids:
            log.info(
                "applied %d deployment-author schedule(s): %s",
                len(_author_schedule_ids),
                _author_schedule_ids,
            )

        # Phase 5c (migrate-substrate-runtime-to-dbos §3.1): persist the
        # embed batch schedule. Skipped when register was skipped.
        if _register_embed:
            from okg.substrate.runtime.embed_workflow import (
                apply_embed_schedule,
            )

            apply_embed_schedule(
                schedule_overrides=dict(runtime.schedules),
            )

        # Phase 5d (deepen-dbos-integration Phase 0): persist the
        # lens-putback sweep schedule.
        if _register_lens_putback:
            from okg.substrate.runtime.lens_putback_schedule import (
                apply_lens_putback_schedule,
            )

            apply_lens_putback_schedule(
                schedule_overrides=dict(runtime.schedules),
            )

        # Phase 5e (deepen-dbos-integration Phase 0): persist the
        # escalation timeout-sweep schedule.
        if _register_escalation_sweep:
            from okg.substrate.runtime.escalation_sweep_schedule import (
                apply_escalation_sweep_schedule,
            )

            apply_escalation_sweep_schedule(
                schedule_overrides=dict(runtime.schedules),
            )

        # Phase 5h (harden-runtime-supervisor §B): persist the gates-
        # reevaluate schedule. Always applied — the workflow's job is
        # to register the OTHER conditional schedules if their gates
        # flipped on after boot, so it must run even on a deployment
        # with all features currently disabled.
        from okg.substrate.runtime.gates_reevaluate_schedule import (
            apply_gates_reevaluate_schedule,
        )

        apply_gates_reevaluate_schedule(
            schedule_overrides=dict(runtime.schedules),
        )

        # okg#3112 step 2: persist the declared-attribute index
        # maintenance schedule (a no-op run until an operator opts in).
        from okg.substrate.runtime.declared_attr_index_schedule import (
            apply_declared_attr_index_schedule,
        )

        apply_declared_attr_index_schedule(
            schedule_overrides=dict(runtime.schedules),
        )

        # Phase 5f (migrate-source-sync-triggers-to-dbos): persist the
        # source-poll schedules registered above.
        if _registered_poll_schedules:
            try:
                from okg.substrate.runtime.source_poll_schedule import (
                    apply_source_poll_schedules,
                )

                _applied_poll_schedules = apply_source_poll_schedules(
                    schedule_overrides=dict(runtime.schedules),
                )
                if sorted(_applied_poll_schedules) != sorted(
                    _registered_poll_schedules
                ):
                    raise RuntimeError(
                        "source-poll schedule apply returned an incomplete set"
                    )
            except Exception:  # noqa: BLE001
                _source_poll_reconcile_ok = False
                log.exception(
                    "failed to apply per-source poll schedules for deployment %s",
                    deployment,
                )
    else:
        if _registered_poll_schedules:
            _source_poll_reconcile_ok = False
        log.info("operator worker skipping recurring schedule application")

    # Deployment-apply acknowledgment: emitted only after the replacement
    # worker has loaded the current registry and persisted its source-poll
    # schedule set.  Apply waits for this exact desired-state digest; a stale
    # or missing acknowledgment therefore cannot look converged.
    try:
        if not _source_poll_reconcile_ok:
            raise RuntimeError(
                "source-poll registration, cleanup, or apply did not converge"
            )
        from okg.substrate.deployment_apply import (
            record_worker_acknowledgment,
        )
        from okg.substrate.sources.registry import load_source_registry

        _ack_registry = load_source_registry(
            _manifest_for_gates.source_registry,
            **(
                {"repo_root_override": _source_registry_repo_root}
                if _source_registry_repo_root is not None
                else {}
            ),
        )
        _require_supervised_services_reconciled(
            deployment=deployment,
            runtime_cfg=runtime,
            registry=_ack_registry,
        )
        record_worker_acknowledgment(
            dsn=dsn,
            deployment=deployment,
            deployment_dir=_manifest_for_gates.deployment_dir,
            active_sources=sorted(str(name) for name in _ack_registry),
            active_schedules=sorted(_applied_poll_schedules),
        )
    except Exception:  # noqa: BLE001 - apply/readiness surface the failed ack.
        log.exception(
            "failed to persist deployment-apply worker acknowledgment: %s",
            deployment,
        )

    # Phase 5: block until SIGTERM/SIGINT.
    shutdown = threading.Event()

    def _on_signal(signum, _frame):
        log.info(
            "worker received signal %s; shutting down",
            signum,
        )
        shutdown.set()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    # Publish worker authority only after initialization, producer/schedule
    # reconciliation, and installation of the graceful signal handlers.  The
    # service launcher receipt proves process identity; this second receipt is
    # the mechanical readiness boundary used by restart convergence.
    if sealed_authority is not None:
        from okg.substrate.runtime.release_authority import (
            write_worker_release_authority,
        )

        authority_path = write_worker_release_authority(sealed_authority)
        log.info(
            "worker release authority ready: environment=%s artifact=%s "
            "executable=%s package=%s definition=%s database=%s report=%s",
            sealed_authority["environment_id"],
            sealed_authority["artifact_id"],
            sealed_authority["executable"],
            sealed_authority["package_file"],
            sealed_authority["definition_digest"],
            sealed_authority["database_identity"],
            authority_path,
        )

    try:
        # Block. The DBOS workers process workflows in their own
        # threads; this main thread just waits for shutdown.
        shutdown.wait()
    finally:
        log.info("worker shutting down: deployment=%s", deployment)
        # Bring supervised source-sync threads down before DBOS so
        # they stop pushing new work onto the runtime mid-shutdown.
        producers_stopped = False
        producer_observations: tuple[object, ...] = ()
        try:
            from okg.substrate.runtime.services import (
                stop_supervised_services,
            )

            service_stop = stop_supervised_services(
                timeout_s=SUPERVISED_PRODUCER_STOP_SECONDS
            )
            producers_stopped = service_stop.ok
            producer_observations = tuple(service_stop.observations)
        except Exception:  # noqa: BLE001
            log.exception(
                "stop_supervised_services raised; DBOS teardown will not start",
            )
        # Do only bounded readback on the signal path. Database maintenance has
        # its own waits and cannot be bounded by the supervisor's stop deadline;
        # durable DBOS rows are left for startup/operator recovery instead of
        # risking a systemd SIGKILL here.
        dbos_shutdown = _shutdown_dbos_after_producers(
            deployment=deployment,
            producers_stopped=producers_stopped,
            dsn=dsn,
            dbos_schema=runtime.dbos_schema,
        )
        shutdown_payload = _shutdown_observation_payload(
            deployment=deployment,
            producer_observations=producer_observations,
            shutdown=dbos_shutdown,
        )
        log.info(
            "worker shutdown observation: %s",
            json.dumps(shutdown_payload, sort_keys=True),
        )
        recovery_attested = _attest_supervised_controlled_recovery(
            deployment=deployment,
            producers_stopped=producers_stopped,
            shutdown=dbos_shutdown,
        )
        shutdown_returncode = _supervised_shutdown_returncode(
            producers_stopped=producers_stopped,
            shutdown=dbos_shutdown,
            recovery_attested=recovery_attested,
        )
        if shutdown_returncode == WORKER_RECOVERY_UNATTESTED_EXIT_CODE:
            log.error(
                "worker recovery interruption was not attested: "
                "deployment=%s active_workflow_ids=%s",
                deployment,
                dbos_shutdown.active_workflow_ids,
            )
            return shutdown_returncode
        if shutdown_returncode == WORKER_SHUTDOWN_INCOMPLETE_EXIT_CODE:
            log.error(
                "worker shutdown incomplete: deployment=%s "
                "producers_stopped=%s dispatch_stopped=%s dbos_destroyed=%s "
                "active_workflows_observed=%s",
                deployment,
                producers_stopped,
                dbos_shutdown.dispatch_stopped,
                dbos_shutdown.destroyed,
                dbos_shutdown.active_workflows_observed,
            )
            return shutdown_returncode
        if shutdown_returncode:
            return shutdown_returncode
        log.info("worker exited cleanly: deployment=%s", deployment)

    return 0


def _cli_entry() -> int:
    """Argparse entry for `python -m okg.substrate.runtime.worker`."""
    import argparse
    from okg.substrate.bytecode_cache import enter_user_bytecode_cache
    from okg.substrate.deployments import resolve_deployment

    # Before any deployment module is imported: keep its bytecode cache out
    # of the deployment repository (okg.substrate.bytecode_cache).
    enter_user_bytecode_cache()

    parser = argparse.ArgumentParser(prog="okg-runtime-worker")
    parser.add_argument("--deployment", required=True)
    parser.add_argument("--dsn", required=False)
    parser.add_argument(
        "--operator",
        action="store_true",
        help=(
            "run short-lived operator mode: suppress recurring schedules, "
            "drain queued deployment work, and exit"
        ),
    )
    parser.add_argument(
        "--max-wait-seconds",
        type=float,
        default=600.0,
        help="operator mode drain timeout (default 600s)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    manifest = resolve_deployment(args.deployment)
    dsn = args.dsn or manifest.postgres_dsn
    if not dsn:
        log.error(
            "no DSN: pass --dsn or set runtime.postgres_dsn in the deployment manifest"
        )
        return 2

    returncode = run_worker(
        deployment=args.deployment,
        dsn=dsn,
        runtime=manifest.runtime,
        operator=args.operator,
        max_wait_seconds=args.max_wait_seconds,
    )
    if returncode != 0 and not args.operator:
        os._exit(returncode)
    return returncode


if __name__ == "__main__":
    sys.exit(_cli_entry())
