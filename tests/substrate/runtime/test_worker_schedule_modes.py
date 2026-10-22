from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
import threading

import pytest


class _DirectBaseFailure(BaseException):
    pass


class _StoppedThread:
    name = "stopped-dispatch"

    def join(self, timeout: float | None = None) -> None:
        del timeout

    def is_alive(self) -> bool:
        return False


def _stopped_dispatch_instance(active_workflows: object) -> SimpleNamespace:
    return SimpleNamespace(
        _active_workflows_set=active_workflows,
        poller_stop_events=[threading.Event()],
        background_thread_stop_events=[],
        _background_threads=[_StoppedThread()],
    )


def test_worker_run_mode_defaults_to_supervised() -> None:
    from okg.substrate.runtime.worker import _worker_run_mode

    mode = _worker_run_mode(operator=False)

    assert mode.name == "supervised"
    assert mode.apply_schedules is True
    assert mode.start_supervised_services is True
    assert mode.drain_and_exit is False
    assert mode.suppress_dynamic_scheduler is False


def test_worker_run_mode_operator_is_quiet_and_short_lived() -> None:
    from okg.substrate.runtime.worker import _worker_run_mode

    mode = _worker_run_mode(operator=True)

    assert mode.name == "operator"
    assert mode.apply_schedules is False
    assert mode.start_supervised_services is False
    assert mode.drain_and_exit is True
    assert mode.suppress_dynamic_scheduler is True


@pytest.mark.parametrize(
    ("failed_service", "source_sync", "trigger"),
    [
        (
            "watcher",
            {"watch": {"enabled": True}, "webhook": {"enabled": False}},
            "fs_event",
        ),
        (
            "webhook",
            {"watch": {"enabled": False}, "webhook": {"enabled": True}},
            "webhook",
        ),
        (
            "maintenance",
            {"watch": {"enabled": False}, "webhook": {"enabled": False}},
            None,
        ),
    ],
)
def test_worker_ack_is_withheld_when_supervised_service_start_fails(
    monkeypatch: pytest.MonkeyPatch,
    failed_service: str,
    source_sync: dict[str, object],
    trigger: str | None,
) -> None:
    from okg.substrate.runtime import services
    from okg.substrate.runtime.config import RuntimeConfig
    from okg.substrate.runtime.worker import (
        _require_supervised_services_reconciled,
    )

    def fail_start(**_kwargs) -> None:
        raise RuntimeError(f"injected {failed_service} startup failure")

    def start_maintenance(**_kwargs) -> None:
        state = services.get_services_state().maintenance
        state.enabled = True
        state.thread = SimpleNamespace(is_alive=lambda: True)

    starter_name = {
        "watcher": "_start_watcher",
        "webhook": "_start_webhook_server",
        "maintenance": "_start_maintenance_service",
    }[failed_service]
    monkeypatch.setattr(services, starter_name, fail_start)
    if failed_service != "maintenance":
        monkeypatch.setattr(
            services,
            "_start_maintenance_service",
            start_maintenance,
        )

    runtime_cfg = RuntimeConfig(source_sync=source_sync)
    services.start_supervised_services(
        deployment="okg-worker-ack-test",
        dsn="postgresql://not-consulted",
        runtime_cfg=runtime_cfg,
    )
    registry = (
        {
            "docs": SimpleNamespace(
                sync=SimpleNamespace(triggers=(trigger,)),
            ),
        }
        if trigger is not None
        else {}
    )

    report = services.supervised_services_reconciliation(
        deployment="okg-worker-ack-test",
        runtime_cfg=runtime_cfg,
        registry=registry,
    )
    assert report["ok"] is False
    assert any(
        problem.startswith(f"{failed_service}:start_failed:")
        for problem in report["problems"]
    )
    with pytest.raises(RuntimeError, match="did not converge"):
        _require_supervised_services_reconciled(
            deployment="okg-worker-ack-test",
            runtime_cfg=runtime_cfg,
            registry=registry,
        )


def test_workflow_completion_timeout_is_at_least_one_second() -> None:
    from okg.substrate.runtime.worker import (
        _workflow_completion_timeout_seconds,
    )

    assert _workflow_completion_timeout_seconds(0) == 1
    assert _workflow_completion_timeout_seconds(0.1) == 1
    assert _workflow_completion_timeout_seconds(12.5) == 13


def test_operator_shutdown_skips_destroy_after_drain_failure() -> None:
    from okg.substrate.runtime.worker import _operator_skip_destroy_on_exit

    assert _operator_skip_destroy_on_exit(
        drain=None,
        drain_failed=True,
    ) is True


def test_operator_shutdown_skips_destroy_after_active_timeout() -> None:
    from okg.substrate.runtime.worker import (
        OperatorDrainResult,
        _operator_skip_destroy_on_exit,
    )

    drain = OperatorDrainResult(
        drained=False,
        in_flight_at_timeout=1,
        elapsed_seconds=10.0,
    )

    assert _operator_skip_destroy_on_exit(
        drain=drain,
        drain_failed=False,
    ) is True


def test_operator_shutdown_allows_destroy_after_clean_drain() -> None:
    from okg.substrate.runtime.worker import (
        OperatorDrainResult,
        _operator_skip_destroy_on_exit,
    )

    drain = OperatorDrainResult(
        drained=True,
        in_flight_at_timeout=0,
        elapsed_seconds=0.1,
    )

    assert _operator_skip_destroy_on_exit(
        drain=drain,
        drain_failed=False,
    ) is False


def test_graceful_destroy_waits_for_active_workflows_before_dbos_destroy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dbos
    from okg.substrate.runtime import worker

    class ActiveWorkflows:
        def __init__(self) -> None:
            self.calls = 0

        def activeList(self) -> list[str]:
            self.calls += 1
            if self.calls == 1:
                return ["wf-active"]
            return []

    poller_stop = threading.Event()
    background_stop = threading.Event()
    instance = SimpleNamespace(
        _active_workflows_set=ActiveWorkflows(),
        poller_stop_events=[poller_stop],
        background_thread_stop_events=[background_stop],
        _background_threads=[_StoppedThread(), _StoppedThread()],
    )
    sleeps: list[float] = []
    destroy_calls: list[dict[str, object]] = []

    monkeypatch.setattr(worker, "_dbos_instance_or_none", lambda: instance)
    monkeypatch.setattr(
        worker,
        "_durable_nonterminal_runtime_workflow_ids",
        lambda **_kwargs: [],
    )
    monkeypatch.setattr(worker.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(
        dbos.DBOS,
        "destroy",
        lambda *args, **kwargs: destroy_calls.append(dict(kwargs)),
    )

    worker._destroy_dbos_gracefully(deployment="okg-workspace")

    assert poller_stop.is_set()
    assert background_stop.is_set()
    assert sleeps == [1.0]
    assert destroy_calls == [{"workflow_completion_timeout_sec": 1}]


def test_graceful_destroy_bounds_active_workflow_wait_without_destroy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dbos
    from okg.substrate.runtime import worker

    class ActiveWorkflows:
        def activeList(self) -> list[str]:
            return ["wf-stuck"]

    instance = _stopped_dispatch_instance(ActiveWorkflows())
    sleeps: list[float] = []
    destroy_calls: list[dict[str, object]] = []
    # Dispatch thread joining consumes two monotonic reads before the active
    # workflow wait begins.
    monotonic_values = iter([0.0, 0.0, 0.0, 0.0, 2.0])

    monkeypatch.setattr(worker, "_dbos_instance_or_none", lambda: instance)
    monkeypatch.setattr(worker.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(worker.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(
        dbos.DBOS,
        "destroy",
        lambda *args, **kwargs: destroy_calls.append(dict(kwargs)),
    )

    worker._destroy_dbos_gracefully(
        deployment="okg-workspace",
        active_wait_timeout_seconds=1.0,
        poll_interval_seconds=0.5,
    )

    assert sleeps == [0.5]
    assert destroy_calls == []


def test_detailed_shutdown_attests_dispatch_active_ids_and_destroy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dbos
    from okg.substrate.runtime import worker

    poller_stop = threading.Event()
    instance = SimpleNamespace(
        _active_workflows_set=SimpleNamespace(
            activeList=lambda: ["wf-b", "wf-a"],
        ),
        poller_stop_events=[poller_stop],
        background_thread_stop_events=[],
        _background_threads=[_StoppedThread()],
    )
    destroy_calls: list[dict[str, object]] = []

    monkeypatch.setattr(worker, "_dbos_instance_or_none", lambda: instance)
    monkeypatch.setattr(
        dbos.DBOS,
        "destroy",
        lambda *args, **kwargs: destroy_calls.append(dict(kwargs)),
    )

    result = worker._shutdown_dbos_gracefully(
        deployment="okg-workspace",
        active_wait_timeout_seconds=0.0,
    )

    assert result.destroyed is False
    assert result.dispatch_stopped is True
    assert result.active_workflow_ids == ("wf-a", "wf-b")
    assert result.active_workflows_observed is True
    assert poller_stop.is_set()
    assert destroy_calls == []


def test_dispatch_stop_failure_is_truthful() -> None:
    from okg.substrate.runtime.worker import _stop_dbos_background_dispatch

    class BrokenEvent:
        def set(self) -> None:
            raise RuntimeError("cannot stop")

    instance = SimpleNamespace(
        poller_stop_events=[BrokenEvent()],
        background_thread_stop_events=[],
    )

    assert _stop_dbos_background_dispatch(instance) is False


def test_dispatch_threads_join_before_active_workflow_inspection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dbos
    from okg.substrate.runtime import worker

    stop_event = threading.Event()
    dispatch_thread = threading.Thread(
        target=stop_event.wait,
        name="dbos-dispatch-proof",
        daemon=True,
    )
    dispatch_thread.start()

    class ActiveWorkflows:
        def activeList(self) -> list[str]:
            assert stop_event.is_set()
            assert not dispatch_thread.is_alive()
            return []

    instance = SimpleNamespace(
        _active_workflows_set=ActiveWorkflows(),
        poller_stop_events=[stop_event],
        background_thread_stop_events=[],
        _background_threads=[dispatch_thread],
    )
    destroy_calls: list[dict[str, object]] = []
    monkeypatch.setattr(worker, "_dbos_instance_or_none", lambda: instance)
    monkeypatch.setattr(
        worker,
        "_durable_nonterminal_runtime_workflow_ids",
        lambda **_kwargs: [],
    )
    monkeypatch.setattr(
        dbos.DBOS,
        "destroy",
        lambda *args, **kwargs: destroy_calls.append(dict(kwargs)),
    )

    result = worker._shutdown_dbos_gracefully(
        deployment="okg-workspace",
    )

    assert result.destroyed is True
    assert result.dispatch_stopped is True
    assert result.active_workflow_ids == ()
    assert destroy_calls == [{"workflow_completion_timeout_sec": 1}]


def test_pinned_dbos_startup_recovery_event_is_executor_owned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dbos
    from dbos._recovery import startup_recovery_thread
    from okg.substrate.runtime import worker

    class SystemDatabase:
        _run_background_processes = True

        def _cleanup_connections(self) -> None:
            self._run_background_processes = False

    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dbos-executor-")
    instance = SimpleNamespace(
        _launched=True,
        _active_workflows_set=SimpleNamespace(activeList=lambda: []),
        poller_stop_events=[],
        background_thread_stop_events=[threading.Event()],
        _background_threads=[_StoppedThread()],
        _sys_db_field=SystemDatabase(),
        _executor_field=executor,
    )
    executor.submit(startup_recovery_thread, instance, []).result(timeout=1.0)
    assert len(instance.background_thread_stop_events) == 2
    assert len(instance._background_threads) == 1
    destroy_calls: list[dict[str, object]] = []
    monkeypatch.setattr(worker, "_dbos_instance_or_none", lambda: instance)
    monkeypatch.setattr(
        worker,
        "_durable_nonterminal_runtime_workflow_ids",
        lambda **_kwargs: [],
    )
    monkeypatch.setattr(
        dbos.DBOS,
        "destroy",
        lambda *args, **kwargs: destroy_calls.append(dict(kwargs)),
    )

    result = worker._shutdown_dbos_gracefully(
        deployment="okg-workspace",
        dsn="postgresql://shutdown-readback",
    )

    assert result.destroyed is True
    assert result.dispatch_stopped is True
    assert destroy_calls == [{"workflow_completion_timeout_sec": 1}]
    dispatch = result.observations[0]
    assert dispatch["event_count"] == 2
    assert dispatch["thread_count"] == 1
    assert dispatch["executor_frozen"] is True
    assert dispatch["executor_threads_joined"] == 1


def test_executor_future_cannot_cross_empty_active_readback_into_destroy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dbos
    from okg.substrate.runtime import worker

    release_before_active = threading.Event()
    entered_active = threading.Event()
    finish_workflow = threading.Event()
    queued_ran = threading.Event()
    active_ids: set[str] = set()

    def latent_workflow() -> None:
        release_before_active.wait()
        active_ids.add("wf-latent")
        entered_active.set()
        finish_workflow.wait()
        active_ids.remove("wf-latent")

    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dbos-executor-")
    latent_future = executor.submit(latent_workflow)
    queued_future = executor.submit(queued_ran.set)

    class ActiveWorkflows:
        calls = 0

        def activeList(self) -> list[str]:
            self.calls += 1
            if self.calls == 1:
                release_before_active.set()
                return []
            assert entered_active.wait(timeout=1.0)
            return sorted(active_ids)

    instance = SimpleNamespace(
        _active_workflows_set=ActiveWorkflows(),
        poller_stop_events=[threading.Event()],
        background_thread_stop_events=[],
        _background_threads=[_StoppedThread()],
        _executor_field=executor,
    )
    destroy_calls: list[dict[str, object]] = []
    monkeypatch.setattr(worker, "_dbos_instance_or_none", lambda: instance)
    monkeypatch.setattr(
        dbos.DBOS,
        "destroy",
        lambda *args, **kwargs: destroy_calls.append(dict(kwargs)),
    )

    result = worker._shutdown_dbos_gracefully(
        deployment="okg-workspace",
        active_wait_timeout_seconds=0.0,
        dispatch_stop_timeout_seconds=0.0,
    )

    assert result.destroyed is False
    assert result.dispatch_stopped is True
    assert result.active_workflow_ids == ("wf-latent",)
    assert result.observations[0]["state"] == "stopped_executor_live"
    assert destroy_calls == []
    assert queued_future.cancelled()
    assert queued_ran.is_set() is False
    finish_workflow.set()
    latent_future.result(timeout=1.0)


def test_cancelled_executor_future_with_durable_pending_row_blocks_destroy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dbos
    from okg.substrate.runtime import worker

    release_running = threading.Event()
    running_started = threading.Event()
    queued_ran = threading.Event()

    def running_before_active() -> None:
        running_started.set()
        release_running.wait()

    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dbos-executor-")
    running_future = executor.submit(running_before_active)
    assert running_started.wait(timeout=1.0)
    queued_future = executor.submit(queued_ran.set)

    class ActiveWorkflows:
        def activeList(self) -> list[str]:
            release_running.set()
            return []

    instance = SimpleNamespace(
        _active_workflows_set=ActiveWorkflows(),
        poller_stop_events=[threading.Event()],
        background_thread_stop_events=[],
        _background_threads=[_StoppedThread()],
        _executor_field=executor,
    )
    destroy_calls: list[dict[str, object]] = []
    durable_calls: list[dict[str, str]] = []
    monkeypatch.setattr(worker, "_dbos_instance_or_none", lambda: instance)

    def durable_readback(**kwargs: str) -> list[str]:
        durable_calls.append(dict(kwargs))
        return ["wf-queued-pending"]

    monkeypatch.setattr(
        worker,
        "_durable_nonterminal_runtime_workflow_ids",
        durable_readback,
    )
    monkeypatch.setattr(
        dbos.DBOS,
        "destroy",
        lambda *args, **kwargs: destroy_calls.append(dict(kwargs)),
    )

    result = worker._shutdown_dbos_gracefully(
        deployment="okg-workspace",
        dsn="postgresql://shutdown-readback",
        dbos_schema="runtime_dbos",
    )

    assert result.destroyed is False
    assert result.active_workflows_observed is True
    assert result.active_workflow_ids == ("wf-queued-pending",)
    assert result.observations[1]["state"] == "durable_remaining"
    assert destroy_calls == []
    assert durable_calls == [
        {
            "dsn": "postgresql://shutdown-readback",
            "deployment": "okg-workspace",
            "dbos_schema": "runtime_dbos",
        }
    ]
    assert queued_future.cancelled()
    assert queued_ran.is_set() is False
    running_future.result(timeout=1.0)


def test_executor_freeze_includes_thread_published_during_shutdown() -> None:
    from okg.substrate.runtime import worker

    original = _StoppedThread()
    late = _StoppedThread()

    class RacingExecutor:
        def __init__(self) -> None:
            self._threads = {original}
            self._shutdown = False

        def shutdown(self, *, wait: bool, cancel_futures: bool) -> None:
            assert wait is False
            assert cancel_futures is True
            self._threads.add(late)
            self._shutdown = True

    observation: dict[str, object] = {}
    frozen, threads = worker._freeze_dbos_executor(
        SimpleNamespace(_launched=True, _executor_field=RacingExecutor()),
        observation=observation,
    )

    assert frozen is True
    assert set(threads) == {original, late}
    assert observation["executor_thread_count"] == 2


def test_shutdown_signals_stop_event_published_after_executor_freeze(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dbos
    from okg.substrate.runtime import worker

    late_event = threading.Event()
    publish_late_event = threading.Event()
    instance = SimpleNamespace()

    def startup_recovery() -> None:
        assert publish_late_event.wait(timeout=2)
        instance.background_thread_stop_events.append(late_event)
        assert late_event.wait(timeout=2)

    executor_thread = threading.Thread(target=startup_recovery)
    executor_thread.start()

    class RacingExecutor:
        _shutdown = False
        _threads = {executor_thread}

        def shutdown(self, *, wait: bool, cancel_futures: bool) -> None:
            assert wait is False
            assert cancel_futures is True
            self._shutdown = True
            publish_late_event.set()

    class SystemDatabase:
        _run_background_processes = True

        def _cleanup_connections(self) -> None:
            self._run_background_processes = False

    instance._launched = True
    instance._active_workflows_set = SimpleNamespace(activeList=lambda: [])
    instance.poller_stop_events = []
    instance.background_thread_stop_events = [threading.Event()]
    instance._background_threads = [_StoppedThread()]
    instance._sys_db_field = SystemDatabase()
    instance._executor_field = RacingExecutor()
    destroyed: list[bool] = []
    monkeypatch.setattr(worker, "_dbos_instance_or_none", lambda: instance)
    monkeypatch.setattr(
        worker,
        "_durable_nonterminal_runtime_workflow_ids",
        lambda **_kwargs: [],
    )
    monkeypatch.setattr(
        dbos.DBOS,
        "destroy",
        lambda **_kwargs: destroyed.append(True),
    )

    result = worker._shutdown_dbos_gracefully(
        deployment="okg-workspace",
        dsn="postgresql://shutdown-readback",
        dispatch_stop_timeout_seconds=2,
    )

    executor_thread.join(timeout=2)
    assert not executor_thread.is_alive()
    assert late_event.is_set()
    assert result.destroyed is True
    assert result.dispatch_stopped is True
    assert result.observations[0]["event_count"] == 2
    assert destroyed == [True]


def test_missing_dispatch_thread_inventory_blocks_inspection_and_destroy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dbos
    from okg.substrate.runtime import worker

    inspected = False

    def active_list() -> list[str]:
        nonlocal inspected
        inspected = True
        return []

    instance = SimpleNamespace(
        _active_workflows_set=SimpleNamespace(activeList=active_list),
        poller_stop_events=[threading.Event()],
        background_thread_stop_events=[],
    )
    destroy_calls: list[dict[str, object]] = []
    monkeypatch.setattr(worker, "_dbos_instance_or_none", lambda: instance)
    monkeypatch.setattr(
        dbos.DBOS,
        "destroy",
        lambda *args, **kwargs: destroy_calls.append(dict(kwargs)),
    )

    result = worker._shutdown_dbos_gracefully(
        deployment="okg-workspace",
        dispatch_stop_timeout_seconds=0.0,
    )

    assert result.dispatch_stopped is False
    assert result.active_workflows_observed is False
    assert inspected is False
    assert destroy_calls == []


def test_launched_dbos_requires_notification_listener_in_thread_inventory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dbos
    from okg.substrate.runtime import worker

    instance = SimpleNamespace(
        _launched=True,
        _active_workflows_set=SimpleNamespace(activeList=lambda: []),
        poller_stop_events=[threading.Event()],
        background_thread_stop_events=[],
        _background_threads=[_StoppedThread()],
    )
    destroy_calls: list[dict[str, object]] = []
    monkeypatch.setattr(worker, "_dbos_instance_or_none", lambda: instance)
    monkeypatch.setattr(
        dbos.DBOS,
        "destroy",
        lambda *args, **kwargs: destroy_calls.append(dict(kwargs)),
    )

    result = worker._shutdown_dbos_gracefully(
        deployment="okg-workspace",
    )

    assert result.dispatch_stopped is False
    assert result.active_workflows_observed is False
    assert destroy_calls == []


def test_live_dispatch_thread_blocks_inspection_and_destroy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dbos
    from okg.substrate.runtime import worker

    stop_event = threading.Event()
    release = threading.Event()
    dispatch_thread = threading.Thread(
        target=release.wait,
        name="stuck-dbos-dispatch",
        daemon=True,
    )
    dispatch_thread.start()
    inspected = False

    def active_list() -> list[str]:
        nonlocal inspected
        inspected = True
        return []

    instance = SimpleNamespace(
        _active_workflows_set=SimpleNamespace(activeList=active_list),
        poller_stop_events=[stop_event],
        background_thread_stop_events=[],
        _background_threads=[dispatch_thread],
    )
    destroy_calls: list[dict[str, object]] = []
    monkeypatch.setattr(worker, "_dbos_instance_or_none", lambda: instance)
    monkeypatch.setattr(
        dbos.DBOS,
        "destroy",
        lambda *args, **kwargs: destroy_calls.append(dict(kwargs)),
    )

    result = worker._shutdown_dbos_gracefully(
        deployment="okg-workspace",
        dispatch_stop_timeout_seconds=0.0,
    )

    assert result.dispatch_stopped is False
    assert inspected is False
    assert destroy_calls == []
    release.set()
    dispatch_thread.join(timeout=1.0)


def test_active_workflow_inspection_failure_skips_destroy_and_is_nonzero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dbos
    from okg.substrate.runtime import worker

    class BrokenActiveWorkflows:
        def activeList(self) -> list[str]:
            raise RuntimeError("inspection failed")

    instance = _stopped_dispatch_instance(BrokenActiveWorkflows())
    destroy_calls: list[dict[str, object]] = []
    monkeypatch.setattr(worker, "_dbos_instance_or_none", lambda: instance)
    monkeypatch.setattr(
        dbos.DBOS,
        "destroy",
        lambda *args, **kwargs: destroy_calls.append(dict(kwargs)),
    )

    result = worker._shutdown_dbos_gracefully(deployment="okg-workspace")

    assert result.destroyed is False
    assert result.dispatch_stopped is True
    assert result.active_workflow_ids == ()
    assert result.active_workflows_observed is False
    assert destroy_calls == []
    assert worker._supervised_shutdown_returncode(
        producers_stopped=True,
        shutdown=result,
        recovery_attested=False,
    ) == worker.WORKER_SHUTDOWN_INCOMPLETE_EXIT_CODE


def test_graceful_destroy_can_skip_destroy_when_active_workflows_remain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dbos
    from okg.substrate.runtime import worker

    class ActiveWorkflows:
        def activeList(self) -> list[str]:
            return ["wf-stuck"]

    instance = _stopped_dispatch_instance(ActiveWorkflows())
    sleeps: list[float] = []
    destroy_calls: list[dict[str, object]] = []
    # Dispatch thread joining consumes two monotonic reads before the active
    # workflow wait begins.
    monotonic_values = iter([0.0, 0.0, 0.0, 0.0, 2.0])

    monkeypatch.setattr(worker, "_dbos_instance_or_none", lambda: instance)
    monkeypatch.setattr(worker.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(worker.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(
        dbos.DBOS,
        "destroy",
        lambda *args, **kwargs: destroy_calls.append(dict(kwargs)),
    )

    destroyed = worker._destroy_dbos_gracefully(
        deployment="okg-workspace",
        active_wait_timeout_seconds=1.0,
        poll_interval_seconds=0.5,
        skip_destroy_on_active_timeout=True,
    )

    assert destroyed is False
    assert sleeps == [0.5]
    assert destroy_calls == []


def test_operator_destroy_waits_unbounded_until_active_workflows_clear(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dbos
    from okg.substrate.runtime import worker

    class ActiveWorkflows:
        def __init__(self) -> None:
            self.calls = 0

        def activeList(self) -> list[str]:
            self.calls += 1
            if self.calls <= 2:
                return ["wf-active"]
            return []

    instance = _stopped_dispatch_instance(ActiveWorkflows())
    sleeps: list[float] = []
    destroy_calls: list[dict[str, object]] = []

    monkeypatch.setattr(worker, "_dbos_instance_or_none", lambda: instance)
    monkeypatch.setattr(worker.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(
        dbos.DBOS,
        "destroy",
        lambda *args, **kwargs: destroy_calls.append(dict(kwargs)),
    )

    destroyed = worker._destroy_dbos_gracefully(
        deployment="okg-workspace",
        active_wait_timeout_seconds=None,
        poll_interval_seconds=0.25,
        skip_destroy_on_active_timeout=True,
    )

    assert destroyed is True
    assert sleeps == [0.25, 0.25]
    assert destroy_calls == [{"workflow_completion_timeout_sec": 1}]


def test_graceful_destroy_waits_for_dbos_destroy_and_reports_budget_overrun(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dbos
    from okg.substrate.runtime import worker

    instance = _stopped_dispatch_instance(
        SimpleNamespace(activeList=lambda: [])
    )
    entered = threading.Event()

    def slow_destroy(*args, **kwargs):
        entered.set()
        threading.Event().wait(0.02)

    monkeypatch.setattr(worker, "_dbos_instance_or_none", lambda: instance)
    monkeypatch.setattr(dbos.DBOS, "destroy", slow_destroy)

    started = worker.time.monotonic()
    destroyed = worker._destroy_dbos_gracefully(
        deployment="okg-workspace",
        dbos_destroy_timeout_seconds=0.005,
    )
    elapsed = worker.time.monotonic() - started

    assert entered.is_set()
    assert destroyed is False
    assert elapsed >= 0.02
    assert not any(
        thread.name == "okg-dbos-destroy"
        for thread in threading.enumerate()
    )


def test_shutdown_cleanup_cancels_stale_runtime_wakeups(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import services, worker

    calls: list[tuple[str, dict[str, object]]] = []

    def cleanup_pending(**kwargs):
        calls.append(("pending", kwargs))
        return {"cancelled_count": 1}

    def cleanup_enqueued(**kwargs):
        calls.append(("enqueued", kwargs))
        return {"cancelled_count": 2}

    # Issue #1257: the same cleanup releases slots held by workflows past the
    # deadline DBOS recorded for them, which the started-PENDING sweep spares.
    def cleanup_deadline_expired(**kwargs):
        calls.append(("deadline_expired", kwargs))
        return {"cancelled_count": 3, "cancelled": []}

    from okg.substrate.runtime import workflow_cancel

    monkeypatch.setattr(
        workflow_cancel,
        "cleanup_deadline_expired_pending_workflows",
        cleanup_deadline_expired,
    )
    monkeypatch.setattr(
        services,
        "cleanup_stale_pending_workflows",
        cleanup_pending,
    )
    monkeypatch.setattr(
        services,
        "cleanup_stale_enqueued_workflows",
        cleanup_enqueued,
    )

    report = worker._cleanup_stale_runtime_wakeups(
        dsn="postgres://test",
        deployment="okg-workspace",
        phase="shutdown",
    )

    assert calls == [
        (
            "pending",
            {
                "dsn": "postgres://test",
                "deployment": "okg-workspace",
                "cancel_started_pending": False,
            },
        ),
        (
            "deadline_expired",
            {"dsn": "postgres://test", "deployment": "okg-workspace"},
        ),
        (
            "enqueued",
            {"dsn": "postgres://test", "deployment": "okg-workspace"},
        ),
    ]
    assert report == {
        "pending": {"cancelled_count": 1},
        "deadline_expired": {"cancelled_count": 3, "cancelled": []},
        "enqueued": {"cancelled_count": 2},
    }


def test_worker_boot_cleanup_preserves_started_pending_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import services, worker

    calls: list[tuple[str, dict[str, object]]] = []

    def cleanup_pending(**kwargs):
        calls.append(("pending", kwargs))
        return {"cancelled_count": 0}

    monkeypatch.setattr(
        services,
        "cleanup_stale_pending_workflows",
        cleanup_pending,
    )
    monkeypatch.setattr(
        services,
        "cleanup_stale_enqueued_workflows",
        lambda **_kwargs: {"cancelled_count": 0},
    )

    worker._cleanup_stale_runtime_wakeups(
        dsn="postgres://test",
        deployment="okg-workspace",
        phase="boot",
    )

    assert calls == [(
        "pending",
        {
            "dsn": "postgres://test",
            "deployment": "okg-workspace",
            "cancel_started_pending": False,
        },
    )]


def test_operator_scheduler_suppression_is_process_local(monkeypatch) -> None:
    import dbos._dbos as dbos_module  # type: ignore[import-not-found]
    from okg.substrate.runtime.worker import _suppress_dbos_dynamic_scheduler

    calls: list[str] = []

    def original_scheduler(stop_event, polling_interval_sec):
        calls.append(f"original:{polling_interval_sec}")

    monkeypatch.setattr(
        dbos_module,
        "dynamic_scheduler_loop",
        original_scheduler,
    )

    with _suppress_dbos_dynamic_scheduler(enabled=True):
        quiet_scheduler = dbos_module.dynamic_scheduler_loop
        assert quiet_scheduler is not original_scheduler
        event = threading.Event()
        event.set()
        quiet_scheduler(event, 1.5)
        assert calls == []

    assert dbos_module.dynamic_scheduler_loop is original_scheduler


def test_candidate_scheduler_waits_for_durable_fence_release(
    monkeypatch,
) -> None:
    import dbos._dbos as dbos_module  # type: ignore[import-not-found]
    from okg.substrate.runtime.worker import _suppress_dbos_dynamic_scheduler

    fence_read = threading.Event()
    original_started = threading.Event()
    release_state = {"value": "fenced"}

    def fence_status(**kwargs):
        assert kwargs == {
            "deployment": "okg-ci",
            "dsn": "postgres://test",
        }
        fence_read.set()
        if release_state["value"] == "absent":
            return None
        if release_state["value"] == "mismatched":
            return {
                "status": "released",
                "promotion_id": "other-owner",
                "metadata": {
                    "release_receipt": {
                        "durable_release_receipt": True,
                        "promotion_id": "other-owner",
                        "schedule_snapshot_digest": "sha256:other",
                    },
                },
            }
        if release_state["value"] == "released":
            return {
                "status": "released",
                "promotion_id": "deploy-owner",
                "metadata": {
                    "release_receipt": {
                        "durable_release_receipt": True,
                        "promotion_id": "deploy-owner",
                        "schedule_snapshot_digest": "sha256:predecessor",
                    },
                },
            }
        return {"status": "fenced", "promotion_id": "deploy-owner"}

    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.promotion_fence_status",
        fence_status,
    )

    def original_scheduler(stop_event, polling_interval_sec):
        del stop_event, polling_interval_sec
        original_started.set()

    monkeypatch.setattr(
        dbos_module,
        "dynamic_scheduler_loop",
        original_scheduler,
    )
    stop = threading.Event()
    with _suppress_dbos_dynamic_scheduler(
        enabled=True,
        deployment="okg-ci",
        dsn="postgres://test",
        wait_for_fence_release=True,
        expected_fence_owner="deploy-owner",
        expected_schedule_snapshot_digest="sha256:predecessor",
    ):
        gated_scheduler = dbos_module.dynamic_scheduler_loop
        thread = threading.Thread(
            target=gated_scheduler,
            args=(stop, 0.01),
            daemon=True,
        )
        thread.start()
        assert fence_read.wait(timeout=1)
        assert original_started.is_set() is False
        release_state["value"] = "absent"
        threading.Event().wait(0.03)
        assert original_started.is_set() is False
        release_state["value"] = "mismatched"
        threading.Event().wait(0.03)
        assert original_started.is_set() is False
        release_state["value"] = "released"
        assert original_started.wait(timeout=1)
        thread.join(timeout=1)

    assert thread.is_alive() is False


def test_runtime_worker_argparse_exposes_operator_mode() -> None:
    from okg.substrate.cli.main import _build_parser

    args = _build_parser().parse_args([
        "runtime",
        "worker",
        "--deployment",
        "okg-workspace",
        "--operator",
        "--max-wait-seconds",
        "12.5",
    ])

    assert args.rt_subcmd == "worker"
    assert args.operator is True
    assert args.max_wait_seconds == 12.5


def test_runtime_worker_dispatch_passes_operator_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli import main
    import okg.substrate.deployments as deployments
    import okg.substrate.deployment_release as deployment_release
    from okg.substrate.runtime import worker

    runtime = SimpleNamespace(enabled=True)
    # The worker dispatch path runs the release-contract guard, which
    # re-resolves the manifest and reads `.release`. With release=None the
    # guard short-circuits without touching the DB. The guard binds
    # resolve_deployment from okg.substrate.deployment_release, so patch it
    # there too (not just in okg.substrate.deployments).
    manifest = SimpleNamespace(runtime=runtime, release=None)
    captured: dict[str, object] = {}

    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.setattr(
        deployments,
        "resolve_deployment",
        lambda deployment: manifest,
    )
    monkeypatch.setattr(
        deployment_release,
        "resolve_deployment",
        lambda deployment: manifest,
    )

    from okg.substrate.db import readiness as db_readiness

    # Issue #2128 answers database readiness before the worker branch
    # does any release work, and nothing listens on `postgres://example`.
    # These cases are about release and dispatch, not readiness, so the
    # probe is answered with a migrated database. The gate on this branch
    # is pinned against a real server by
    # `test_runtime_worker_refuses_an_unmigrated_database_by_name` in
    # tests/substrate/test_migrate.py.
    monkeypatch.setattr(
        db_readiness,
        "require_database_ready",
        lambda dsn, **kwargs: db_readiness.DatabaseReadiness(
            schema_present=True, schema_applied=True, ledger_present=True
        ),
    )

    def fake_run_worker(**kwargs):
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(worker, "run_worker", fake_run_worker)

    args = SimpleNamespace(
        rt_subcmd="worker",
        deployment="okg-workspace",
        dsn="postgres://example",
        operator=True,
        max_wait_seconds=7.0,
    )

    assert main._cmd_runtime(args, as_json=True) == 0
    assert captured == {
        "deployment": "okg-workspace",
        "dsn": "postgres://example",
        "runtime": runtime,
        "operator": True,
        "max_wait_seconds": 7.0,
    }


def test_runtime_cli_hard_exits_only_supervised_codes_recovery_and_seven(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Supervised mode hard-exits for the attested handoff and for code 7.

    Operator mode and every other code return normally (issue #3861 added
    code 7 to the supervised hard exits).
    """
    from okg.substrate.cli import runtime
    from okg.substrate.runtime.recovery_handoff import (
        CONTROLLED_RECOVERY_EXIT_CODE,
    )
    from okg.substrate.runtime.worker import WORKER_RECOVERY_UNATTESTED_EXIT_CODE

    exits: list[int] = []
    monkeypatch.setattr(runtime.os, "_exit", lambda code: exits.append(code))
    # The code 7 path closes logging before exiting; keep pytest's handlers.
    import logging

    monkeypatch.setattr(logging, "shutdown", lambda *a, **k: None)

    assert runtime._complete_runtime_worker_process(
        CONTROLLED_RECOVERY_EXIT_CODE,
        operator=False,
    ) == CONTROLLED_RECOVERY_EXIT_CODE
    assert exits == [CONTROLLED_RECOVERY_EXIT_CODE]

    exits.clear()
    assert runtime._complete_runtime_worker_process(
        CONTROLLED_RECOVERY_EXIT_CODE,
        operator=True,
    ) == CONTROLLED_RECOVERY_EXIT_CODE
    assert runtime._complete_runtime_worker_process(6, operator=False) == 6
    # Code 7 in operator mode returns normally; only supervised mode exits.
    assert runtime._complete_runtime_worker_process(
        WORKER_RECOVERY_UNATTESTED_EXIT_CODE,
        operator=True,
    ) == WORKER_RECOVERY_UNATTESTED_EXIT_CODE
    assert exits == []

    assert runtime._complete_runtime_worker_process(
        WORKER_RECOVERY_UNATTESTED_EXIT_CODE,
        operator=False,
    ) == WORKER_RECOVERY_UNATTESTED_EXIT_CODE
    assert exits == [WORKER_RECOVERY_UNATTESTED_EXIT_CODE]

def test_supervised_shutdown_budget_fits_legacy_systemd_timeout() -> None:
    from okg.substrate.runtime.worker import (
        SUPERVISED_ACTIVE_WORKFLOW_WAIT_SECONDS,
        SUPERVISED_DBOS_DESTROY_SECONDS,
        SUPERVISED_DISPATCH_STOP_SECONDS,
        SUPERVISED_PRODUCER_STOP_SECONDS,
    )

    assert (
        SUPERVISED_PRODUCER_STOP_SECONDS
        + SUPERVISED_DISPATCH_STOP_SECONDS
        + SUPERVISED_ACTIVE_WORKFLOW_WAIT_SECONDS
        + SUPERVISED_DBOS_DESTROY_SECONDS
    ) < 30.0


def test_live_producer_blocks_every_dbos_shutdown_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import worker

    calls: list[str] = []
    monkeypatch.setattr(
        worker,
        "_shutdown_dbos_gracefully",
        lambda **_kwargs: calls.append("dbos") or pytest.fail(
            "DBOS shutdown must not start while a producer is live"
        ),
    )

    result = worker._shutdown_dbos_after_producers(
        deployment="okg-workspace",
        producers_stopped=False,
    )

    assert calls == []
    assert result.destroyed is False
    assert result.dispatch_stopped is False
    assert result.active_workflows_observed is False
    assert [row["state"] for row in result.observations] == [
        "not_attempted_producers_live",
        "not_inspected",
        "not_attempted",
    ]


@pytest.mark.parametrize(
    ("producers_stopped", "shutdown"),
    [
        (
            False,
            SimpleNamespace(
                active_workflow_ids=("wf",),
                active_workflows_observed=True,
                dispatch_stopped=True,
                destroyed=True,
            ),
        ),
        (
            True,
            SimpleNamespace(
                active_workflow_ids=("wf",),
                active_workflows_observed=True,
                dispatch_stopped=False,
                destroyed=True,
            ),
        ),
        (
            True,
            SimpleNamespace(
                active_workflow_ids=("wf",),
                active_workflows_observed=True,
                dispatch_stopped=True,
                destroyed=False,
            ),
        ),
        (
            True,
            SimpleNamespace(
                active_workflow_ids=(),
                active_workflows_observed=True,
                dispatch_stopped=True,
                destroyed=True,
            ),
        ),
    ],
)


def test_controlled_recovery_refuses_incomplete_or_clean_shutdown(
    producers_stopped: bool,
    shutdown: SimpleNamespace,
) -> None:
    from okg.substrate.runtime.worker import (
        _attest_supervised_controlled_recovery,
    )

    assert _attest_supervised_controlled_recovery(
        deployment="okg-workspace",
        producers_stopped=producers_stopped,
        shutdown=shutdown,
    ) is False


def test_active_unattested_shutdown_is_nonzero_and_not_clean() -> None:
    from okg.substrate.runtime.worker import (
        DBOSShutdownResult,
        WORKER_RECOVERY_UNATTESTED_EXIT_CODE,
        _supervised_shutdown_returncode,
    )

    assert _supervised_shutdown_returncode(
        producers_stopped=True,
        shutdown=DBOSShutdownResult(
            destroyed=True,
            dispatch_stopped=True,
            active_workflow_ids=("wf-live",),
        ),
        recovery_attested=False,
    ) == WORKER_RECOVERY_UNATTESTED_EXIT_CODE


def test_clean_drain_returns_normally() -> None:
    from okg.substrate.runtime.worker import (
        DBOSShutdownResult,
        _supervised_shutdown_returncode,
    )

    assert _supervised_shutdown_returncode(
        producers_stopped=True,
        shutdown=DBOSShutdownResult(
            destroyed=True,
            dispatch_stopped=True,
            active_workflow_ids=(),
        ),
        recovery_attested=False,
    ) == 0


def test_supervised_service_stop_reports_live_producer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import services

    services._reset_state("okg-workspace")
    release = threading.Event()
    thread = threading.Thread(target=release.wait, daemon=True)
    thread.start()
    state = services.get_services_state()
    state.maintenance.thread = thread
    state.maintenance.stop_event = threading.Event()

    monkeypatch.setattr(services.time, "monotonic", lambda: 0.0)
    result = services.stop_supervised_services(timeout_s=0.0)

    assert result.ok is False
    assert result.maintenance_stopped is False
    assert "maintenance:thread_alive" in result.errors
    assert "maintenance:stop_timeout" in result.errors
    release.set()
    thread.join(timeout=1.0)
    services._reset_state("")
    services.get_services_state().deployment = None


def test_supervised_service_stop_joins_webhook_server_thread() -> None:
    from okg.substrate.runtime import services

    services._reset_state("okg-workspace")
    release = threading.Event()
    server_closed = threading.Event()
    server_thread = threading.Thread(target=release.wait, daemon=True)
    server_thread.start()

    class Server:
        def shutdown(self) -> None:
            release.set()

        def server_close(self) -> None:
            server_closed.set()

    state = services.get_services_state()
    state.webhook.server = Server()
    state.webhook.thread = server_thread

    result = services.stop_supervised_services(timeout_s=1.0)

    assert result.ok is True
    assert result.webhook_stopped is True
    assert not server_thread.is_alive()
    assert server_closed.is_set()
    services._reset_state("")
    services.get_services_state().deployment = None


def test_supervised_service_stop_attests_complete_watcher_inventory() -> None:
    from okg.substrate.runtime import services

    class Watcher:
        _observer = _StoppedThread()
        _loop_thread = _StoppedThread()
        _heartbeat_thread = _StoppedThread()
        _head_poller_thread = None

        def stop(self, *, timeout_s: float) -> None:
            del timeout_s

    services._reset_state("okg-workspace")
    state = services.get_services_state()
    state.watcher.instance = Watcher()

    result = services.stop_supervised_services(timeout_s=1.0)

    assert result.ok is True
    assert result.watcher_stopped is True
    watcher_observation = next(
        row for row in result.observations if row["role"] == "watcher"
    )
    assert watcher_observation["state"] == "stopped_joined"
    assert watcher_observation["alive_after"] is False
    services._reset_state("")
    services.get_services_state().deployment = None


@pytest.mark.parametrize(
    "failure_factory",
    [
        lambda: RuntimeError("ordinary failure"),
        lambda: BaseExceptionGroup("grouped failure", [KeyboardInterrupt()]),
        lambda: _DirectBaseFailure("direct base failure"),
    ],
    ids=("exception", "base-exception-group", "direct-base-subclass"),
)
def test_supervised_service_stop_reports_every_base_exception(
    failure_factory,
) -> None:
    from okg.substrate.runtime import services

    failure = failure_factory()

    class Server:
        def shutdown(self) -> None:
            raise failure

        def server_close(self) -> None:
            pytest.fail("server_close must not run after shutdown fails")

    services._reset_state("okg-workspace")
    state = services.get_services_state()
    state.webhook.server = Server()

    result = services.stop_supervised_services(timeout_s=1.0)

    assert result.ok is False
    assert f"webhook:{type(failure).__name__}" in result.errors
    services._reset_state("")
    services.get_services_state().deployment = None


def test_supervised_service_stop_refuses_missing_watcher_thread_handle() -> None:
    from okg.substrate.runtime import services

    class PartialWatcher:
        _observer = _StoppedThread()
        _loop_thread = _StoppedThread()
        _heartbeat_thread = None
        _head_poller_thread = None

        def stop(self, *, timeout_s: float) -> None:
            del timeout_s

    services._reset_state("okg-workspace")
    state = services.get_services_state()
    state.watcher.instance = PartialWatcher()

    result = services.stop_supervised_services(timeout_s=1.0)

    assert result.ok is False
    assert result.watcher_stopped is False
    assert "watcher:underlying_handle_missing" in result.errors
    watcher_observation = next(
        row for row in result.observations if row["role"] == "watcher"
    )
    assert watcher_observation["state"] == "stop_incomplete"
    services._reset_state("")
    services.get_services_state().deployment = None


def test_supervised_service_stop_joins_handles_from_partial_watcher_start() -> None:
    from okg.substrate.runtime import services

    class PartialWatcher:
        _observer = _StoppedThread()
        _loop_thread = _StoppedThread()
        _heartbeat_thread = None
        _head_poller_thread = None

        def stop(self, *, timeout_s: float) -> None:
            del timeout_s

    services._reset_state("okg-workspace")
    state = services.get_services_state()
    state.watcher.instance = PartialWatcher()
    state.watcher.last_error = "RuntimeError: heartbeat start failed"

    result = services.stop_supervised_services(timeout_s=1.0)

    assert result.ok is True
    assert result.watcher_stopped is True
    assert "watcher:underlying_handle_missing" not in result.errors
    services._reset_state("")
    services.get_services_state().deployment = None


def test_supervised_service_stop_refuses_missing_webhook_thread_handle() -> None:
    from okg.substrate.runtime import services

    class Server:
        def shutdown(self) -> None:
            return None

        def server_close(self) -> None:
            return None

    services._reset_state("okg-workspace")
    state = services.get_services_state()
    state.webhook.server = Server()

    result = services.stop_supervised_services(timeout_s=1.0)

    assert result.ok is False
    assert result.webhook_stopped is False
    assert "webhook:underlying_handle_missing" in result.errors
    services._reset_state("")
    services.get_services_state().deployment = None


def test_worker_refuses_start_outside_sync_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Tier W at the worker boundary: a foreign-checkout worker against
    # the canonical database must refuse to START, not just to ingest —
    # a supervised worker outliving the ingest guard was the live
    # bypass (launchd pinning a stale worktree, 2026-06-10).
    import okg.substrate.deployment_authority as authority
    from okg.substrate.runtime.config import RuntimeConfig
    from okg.substrate.runtime.worker import run_worker

    monkeypatch.setattr(
        authority,
        "check_publish_provenance",
        lambda deployment, dsn: {
            "allowed": False,
            "mode": "foreign_checkout",
            "reason": "checkout /elsewhere is not the registered sync root",
            "legal_moves": ["start the worker from the sync root"],
        },
    )

    # kubernetes-smoke ships; the guard, not the deployment, is under test.
    assert run_worker(
        deployment="kubernetes-smoke",
        dsn="postgres://example",
        runtime=RuntimeConfig(enabled=True),
    ) == 2


def test_worker_refuses_missing_source_root_before_runtime_writes(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from okg.substrate.runtime import worker
    from okg.substrate.runtime.config import RuntimeConfig

    missing_root = tmp_path / "foreign-host-root"
    monkeypatch.setattr(
        worker,
        "_preflight_embedding_plan",
        lambda _deployment: object(),
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.release_authority."
        "verify_worker_release_authority",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_authority.check_publish_provenance",
        lambda *_args, **_kwargs: {"allowed": True, "mode": "isolated"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: SimpleNamespace(
            sync_root=missing_root,
            sync_root_unresolved=(),
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.code_identity.record_worker_code_identity",
        lambda **_kwargs: pytest.fail(
            "worker wrote code identity before source-root refusal",
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.db.capacity.release_process_reservations",
        lambda **_kwargs: None,
    )

    assert worker.run_worker(
        deployment="demo",
        dsn="postgres://unused",
        runtime=RuntimeConfig(enabled=True),
    ) == 2
    assert "source_configuration_authority_invalid" in caplog.text
    assert "foreign-host-root" in caplog.text
    assert "fix:" in caplog.text


@pytest.mark.parametrize(
    "configured",
    ["~okg-user-that-does-not-exist/source", "/tmp/source\0root"],
)
def test_worker_refuses_exceptional_manifest_root_with_typed_envelope(
    configured: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from okg.substrate.deployments.sync_root import resolve_declared_sync_root
    from okg.substrate.runtime import worker
    from okg.substrate.runtime.config import RuntimeConfig

    monkeypatch.setattr(
        worker,
        "_preflight_embedding_plan",
        lambda _deployment: object(),
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.release_authority."
        "verify_worker_release_authority",
        lambda **_kwargs: None,
    )

    def resolve_manifest(_deployment):
        resolution = resolve_declared_sync_root(
            {"sync_root": configured},
            deployment_dir=tmp_path,
        )
        return SimpleNamespace(
            sync_root=resolution.path,
            sync_root_unresolved=(),
        )

    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        resolve_manifest,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_authority.check_publish_provenance",
        lambda *_args, **_kwargs: pytest.fail(
            "provenance ran before source-root authority refusal",
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.code_identity.record_worker_code_identity",
        lambda **_kwargs: pytest.fail(
            "worker wrote code identity before source-root refusal",
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.db.capacity.release_process_reservations",
        lambda **_kwargs: None,
    )

    assert worker.run_worker(
        deployment="demo",
        dsn="postgres://unused",
        runtime=RuntimeConfig(enabled=True),
    ) == 2
    assert "source_configuration_authority_invalid" in caplog.text
    assert "fix:" in caplog.text


def test_worker_refuses_unresolved_root_on_isolated_database(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from okg.substrate.runtime import worker
    from okg.substrate.runtime.config import RuntimeConfig

    monkeypatch.setattr(
        worker,
        "_preflight_embedding_plan",
        lambda _deployment: object(),
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.release_authority."
        "verify_worker_release_authority",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: SimpleNamespace(
            sync_root=None,
            sync_root_unresolved=("OKG_DEMO_SYNC_ROOT",),
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_authority.check_publish_provenance",
        lambda *_args, **_kwargs: pytest.fail(
            "isolated provenance admitted an unresolved source root",
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.code_identity.record_worker_code_identity",
        lambda **_kwargs: pytest.fail(
            "worker wrote code identity before unresolved-root refusal",
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.db.capacity.release_process_reservations",
        lambda **_kwargs: None,
    )

    assert worker.run_worker(
        deployment="demo",
        dsn="postgres://isolated/unused",
        runtime=RuntimeConfig(enabled=True),
    ) == 2
    assert "cannot be resolved on this machine" in caplog.text
    assert "OKG_DEMO_SYNC_ROOT" in caplog.text


def test_sealed_worker_refuses_bad_root_before_authority_report_write(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import worker
    from okg.substrate.runtime.config import RuntimeConfig

    monkeypatch.setattr(
        worker,
        "_preflight_embedding_plan",
        lambda _deployment: object(),
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.release_authority."
        "verify_worker_release_authority",
        lambda **_kwargs: {"artifact_id": "sealed-fixture"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: SimpleNamespace(
            sync_root=tmp_path / "missing-sealed-root",
            sync_root_unresolved=(),
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.release_authority."
        "write_worker_release_authority",
        lambda _authority: pytest.fail(
            "sealed worker wrote authority report before source-root refusal",
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.db.capacity.release_process_reservations",
        lambda **_kwargs: None,
    )

    assert worker.run_worker(
        deployment="demo",
        dsn="postgres://unused",
        runtime=RuntimeConfig(enabled=True),
    ) == 2


def test_worker_start_passes_guard_from_sync_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The allowed path proceeds past the guard: stub the next startup
    # step (defensive_destroy) with a sentinel so the test stops there
    # without bringing up DBOS.
    import okg.substrate.deployment_authority as authority
    import okg.substrate.runtime.engine as engine
    from okg.substrate.runtime.config import RuntimeConfig
    from okg.substrate.runtime.worker import run_worker

    monkeypatch.setattr(
        authority,
        "check_publish_provenance",
        lambda deployment, dsn: {"allowed": True, "mode": "sync_root"},
    )
    class _Sentinel(Exception):
        pass

    def _stop() -> None:
        raise _Sentinel

    monkeypatch.setattr(engine, "defensive_destroy", _stop)

    # kubernetes-smoke ships and declares no sync_root, so no sync-root
    # environment variable is needed; the stubbed guard allows the start.
    with pytest.raises(_Sentinel):
        run_worker(
            deployment="kubernetes-smoke",
            dsn="postgres://example",
            runtime=RuntimeConfig(enabled=True),
        )
