from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import time

import pytest

from okg.substrate.runtime.recovery_handoff import (
    CONTROLLED_RECOVERY_EXIT_CODE,
    CONTROLLED_RECOVERY_MARKER_SCHEMA,
    ControlledRecoveryHandoffError,
    attest_controlled_recovery_response,
    cleanup_controlled_recovery_handoff,
    controlled_recovery_request_path,
    controlled_recovery_response_path,
    create_controlled_recovery_request,
    parse_controlled_recovery_marker,
    read_controlled_recovery_request,
    read_controlled_recovery_response,
)


class _DirectBaseFailure(BaseException):
    pass


@pytest.mark.parametrize(
    "failure_factory",
    [
        lambda: RuntimeError("ordinary failure"),
        lambda: BaseExceptionGroup("grouped failure", [KeyboardInterrupt()]),
        lambda: _DirectBaseFailure("direct base failure"),
    ],
    ids=("exception", "base-exception-group", "direct-base-subclass"),
)
def test_exclusive_write_cleans_up_every_base_exception(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure_factory,
) -> None:
    from okg.substrate.runtime import recovery_handoff

    path = tmp_path / "handoff.json"
    failure = failure_factory()

    def fail_write(_descriptor: int, _payload: bytes) -> None:
        raise failure

    monkeypatch.setattr(recovery_handoff, "_write_all", fail_write)

    with pytest.raises(ControlledRecoveryHandoffError) as raised:
        recovery_handoff._write_exclusive(path, {"payload": "value"})

    assert raised.value.code == "handoff_create_failed"
    assert raised.value.__cause__ is failure
    assert not path.exists()


@pytest.mark.parametrize(
    "failure_factory",
    [
        lambda: RuntimeError("ordinary failure"),
        lambda: BaseExceptionGroup("grouped failure", [KeyboardInterrupt()]),
        lambda: _DirectBaseFailure("direct base failure"),
    ],
    ids=("exception", "base-exception-group", "direct-base-subclass"),
)
def test_response_publish_cleans_up_every_base_exception(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure_factory,
) -> None:
    from okg.substrate.runtime import recovery_handoff

    directory = tmp_path / "control"
    request = create_controlled_recovery_request(
        deployment="okg-workspace",
        worker_pid=1234,
        restart_cutoff_epoch_ms=987654,
        expected_active_workflow_ids=("wf",),
        nonce="n" * 32,
        directory=directory,
    )
    failure = failure_factory()

    def fail_marker(*_args, **_kwargs) -> None:
        raise failure

    monkeypatch.setattr(
        recovery_handoff,
        "emit_controlled_recovery_marker",
        fail_marker,
    )

    with pytest.raises(ControlledRecoveryHandoffError) as raised:
        attest_controlled_recovery_response(
            request,
            active_workflow_ids=("wf",),
            producers_stopped=True,
            dispatch_stopped=True,
            dbos_destroyed=True,
            directory=directory,
        )

    assert raised.value.code == "handoff_publish_failed"
    assert raised.value.__cause__ is failure
    assert not controlled_recovery_response_path(
        request.deployment,
        request.worker_pid,
        directory=directory,
    ).exists()


def test_exclusive_write_preserves_controlled_error_identity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from okg.substrate.runtime import recovery_handoff

    path = tmp_path / "handoff.json"
    failure = ControlledRecoveryHandoffError(
        "injected_controlled_write_failure",
        "controlled write failed",
    )

    def fail_write(_descriptor: int, _payload: bytes) -> None:
        raise failure

    monkeypatch.setattr(recovery_handoff, "_write_all", fail_write)

    with pytest.raises(ControlledRecoveryHandoffError) as raised:
        recovery_handoff._write_exclusive(path, {"payload": "value"})

    assert raised.value is failure
    assert not path.exists()


def test_secure_handoff_round_trip_and_cleanup(tmp_path: Path) -> None:
    directory = tmp_path / "control"
    request = create_controlled_recovery_request(
        deployment="okg-workspace",
        worker_pid=1234,
        restart_cutoff_epoch_ms=987654,
        expected_active_workflow_ids=("wf-b", "wf-a"),
        nonce="n" * 32,
        now_epoch_ms=1_000_000,
        directory=directory,
    )

    assert stat.S_IMODE(directory.stat().st_mode) == 0o700
    assert (
        stat.S_IMODE(
            controlled_recovery_request_path("okg-workspace", 1234, directory=directory)
            .stat()
            .st_mode
        )
        == 0o600
    )
    assert (
        read_controlled_recovery_request(
            "okg-workspace",
            1234,
            now_epoch_ms=1_000_001,
            directory=directory,
        )
        == request
    )

    response = attest_controlled_recovery_response(
        request,
        active_workflow_ids=("wf-a", "wf-b"),
        producers_stopped=True,
        dispatch_stopped=True,
        dbos_destroyed=True,
        now_epoch_ms=1_000_002,
        directory=directory,
    )

    response_path = controlled_recovery_response_path(
        "okg-workspace", 1234, directory=directory
    )
    assert stat.S_IMODE(response_path.stat().st_mode) == 0o600
    assert (
        read_controlled_recovery_response(
            "okg-workspace",
            1234,
            expected_request=request,
            expected_marker=response,
            directory=directory,
        )
        == response
    )
    cleanup = cleanup_controlled_recovery_handoff(
        "okg-workspace",
        1234,
        expected_nonce=request.nonce,
        directory=directory,
    )
    assert len(cleanup["removed"]) == 2
    assert list(directory.iterdir()) == []


def test_request_reader_rejects_symlink(tmp_path: Path) -> None:
    directory = tmp_path / "control"
    request = create_controlled_recovery_request(
        deployment="okg-workspace",
        worker_pid=1234,
        restart_cutoff_epoch_ms=987654,
        expected_active_workflow_ids=("wf",),
        nonce="n" * 32,
        directory=directory,
    )
    request_path = controlled_recovery_request_path(
        request.deployment, request.worker_pid, directory=directory
    )
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    request_path.unlink()
    request_path.symlink_to(target)

    with pytest.raises(
        ControlledRecoveryHandoffError,
        match="0600 regular file",
    ):
        read_controlled_recovery_request(
            request.deployment,
            request.worker_pid,
            directory=directory,
        )


def test_request_reader_rejects_stale_request(tmp_path: Path) -> None:
    directory = tmp_path / "control"
    create_controlled_recovery_request(
        deployment="okg-workspace",
        worker_pid=1234,
        restart_cutoff_epoch_ms=987654,
        expected_active_workflow_ids=("wf",),
        ttl_seconds=1.0,
        nonce="n" * 32,
        now_epoch_ms=1_000_000,
        directory=directory,
    )

    with pytest.raises(ControlledRecoveryHandoffError) as raised:
        read_controlled_recovery_request(
            "okg-workspace",
            1234,
            now_epoch_ms=1_001_001,
            directory=directory,
        )
    assert raised.value.code == "handoff_request_stale"


def test_response_requires_exact_active_workflow_ids(tmp_path: Path) -> None:
    request = create_controlled_recovery_request(
        deployment="okg-workspace",
        worker_pid=1234,
        restart_cutoff_epoch_ms=987654,
        expected_active_workflow_ids=("wf-expected",),
        nonce="n" * 32,
        directory=tmp_path / "control",
    )

    with pytest.raises(ControlledRecoveryHandoffError) as raised:
        attest_controlled_recovery_response(
            request,
            active_workflow_ids=("wf-other",),
            producers_stopped=True,
            dispatch_stopped=True,
            dbos_destroyed=True,
            directory=tmp_path / "control",
        )
    assert raised.value.code == "handoff_active_workflows_mismatch"


def test_request_directory_fsync_failure_returns_exact_readable_request(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from okg.substrate.runtime import recovery_handoff

    directory = tmp_path / "control"
    real_fsync = recovery_handoff._fsync_directory
    monkeypatch.setattr(
        recovery_handoff,
        "_fsync_directory",
        lambda _path: (_ for _ in ()).throw(OSError("fsync failed")),
    )

    request = create_controlled_recovery_request(
        deployment="okg-workspace",
        worker_pid=1234,
        restart_cutoff_epoch_ms=987654,
        expected_active_workflow_ids=("wf",),
        nonce="n" * 32,
        directory=directory,
    )

    assert (
        read_controlled_recovery_request(
            request.deployment,
            request.worker_pid,
            directory=directory,
        )
        == request
    )
    monkeypatch.setattr(
        recovery_handoff,
        "_fsync_directory",
        real_fsync,
    )
    cleanup_controlled_recovery_handoff(
        request.deployment,
        request.worker_pid,
        expected_nonce=request.nonce,
        directory=directory,
    )


def test_marker_failure_leaves_no_admissible_response(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from okg.substrate.runtime import recovery_handoff

    directory = tmp_path / "control"
    request = create_controlled_recovery_request(
        deployment="okg-workspace",
        worker_pid=1234,
        restart_cutoff_epoch_ms=987654,
        expected_active_workflow_ids=("wf",),
        nonce="n" * 32,
        directory=directory,
    )

    failure = ControlledRecoveryHandoffError(
        "handoff_marker_write_failed", "marker failed"
    )

    def fail_marker(*_args, **_kwargs) -> None:
        raise failure

    monkeypatch.setattr(
        recovery_handoff,
        "emit_controlled_recovery_marker",
        fail_marker,
    )

    with pytest.raises(ControlledRecoveryHandoffError) as raised:
        attest_controlled_recovery_response(
            request,
            active_workflow_ids=("wf",),
            producers_stopped=True,
            dispatch_stopped=True,
            dbos_destroyed=True,
            directory=directory,
        )

    assert raised.value is failure
    assert not controlled_recovery_response_path(
        "okg-workspace", 1234, directory=directory
    ).exists()


def test_response_final_fsync_failure_is_not_admissible(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from okg.substrate.runtime import recovery_handoff

    directory = tmp_path / "control"
    request = create_controlled_recovery_request(
        deployment="okg-workspace",
        worker_pid=1234,
        restart_cutoff_epoch_ms=987654,
        expected_active_workflow_ids=("wf",),
        nonce="n" * 32,
        directory=directory,
    )
    real_fsync = recovery_handoff._fsync_directory
    calls = 0

    def fail_final_fsync(path: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("final fsync failed")
        real_fsync(path)

    monkeypatch.setattr(
        recovery_handoff,
        "_fsync_directory",
        fail_final_fsync,
    )

    with pytest.raises(ControlledRecoveryHandoffError):
        attest_controlled_recovery_response(
            request,
            active_workflow_ids=("wf",),
            producers_stopped=True,
            dispatch_stopped=True,
            dbos_destroyed=True,
            directory=directory,
        )

    assert not controlled_recovery_response_path(
        "okg-workspace", 1234, directory=directory
    ).exists()


def test_response_cleanup_double_failure_has_no_terminal_marker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from okg.substrate.runtime import recovery_handoff

    directory = tmp_path / "control"
    request = create_controlled_recovery_request(
        deployment="okg-workspace",
        worker_pid=1234,
        restart_cutoff_epoch_ms=987654,
        expected_active_workflow_ids=("wf",),
        nonce="n" * 32,
        directory=directory,
    )
    response_path = controlled_recovery_response_path(
        request.deployment, request.worker_pid, directory=directory
    )
    real_fsync = recovery_handoff._fsync_directory
    real_unlink = recovery_handoff.os.unlink
    fsync_calls = 0

    def fail_final_fsync(path: Path) -> None:
        nonlocal fsync_calls
        fsync_calls += 1
        if fsync_calls == 2:
            raise OSError("final fsync failed")
        real_fsync(path)

    def fail_final_unlink(path) -> None:
        if Path(path) == response_path:
            raise OSError("final unlink failed")
        real_unlink(path)

    monkeypatch.setattr(
        recovery_handoff,
        "_fsync_directory",
        fail_final_fsync,
    )
    monkeypatch.setattr(recovery_handoff.os, "unlink", fail_final_unlink)
    read_fd, write_fd = os.pipe()
    try:
        with pytest.raises(ControlledRecoveryHandoffError):
            attest_controlled_recovery_response(
                request,
                active_workflow_ids=("wf",),
                producers_stopped=True,
                dispatch_stopped=True,
                dbos_destroyed=True,
                directory=directory,
                marker_descriptor=write_fd,
            )
    finally:
        os.close(write_fd)
    marker_bytes = os.read(read_fd, 4096)
    os.close(read_fd)

    assert response_path.exists()
    assert marker_bytes == b""
    with pytest.raises(ControlledRecoveryHandoffError) as raised:
        parse_controlled_recovery_marker(
            marker_bytes,
            expected_request=request,
        )
    assert raised.value.code == "handoff_marker_invalid"

    monkeypatch.setattr(recovery_handoff.os, "unlink", real_unlink)
    monkeypatch.setattr(
        recovery_handoff,
        "_fsync_directory",
        real_fsync,
    )
    cleanup_controlled_recovery_handoff(
        request.deployment,
        request.worker_pid,
        expected_nonce=request.nonce,
        directory=directory,
    )


def test_cleanup_nonce_mismatch_preserves_both_artifacts(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "control"
    request = create_controlled_recovery_request(
        deployment="okg-workspace",
        worker_pid=1234,
        restart_cutoff_epoch_ms=987654,
        expected_active_workflow_ids=("wf",),
        nonce="n" * 32,
        directory=directory,
    )
    attest_controlled_recovery_response(
        request,
        active_workflow_ids=("wf",),
        producers_stopped=True,
        dispatch_stopped=True,
        dbos_destroyed=True,
        directory=directory,
    )
    response_path = controlled_recovery_response_path(
        request.deployment, request.worker_pid, directory=directory
    )
    payload = json.loads(response_path.read_text(encoding="utf-8"))
    payload["nonce"] = "x" * 32
    response_path.write_text(json.dumps(payload), encoding="utf-8")
    request_path = controlled_recovery_request_path(
        request.deployment, request.worker_pid, directory=directory
    )

    with pytest.raises(ControlledRecoveryHandoffError) as raised:
        cleanup_controlled_recovery_handoff(
            request.deployment,
            request.worker_pid,
            expected_nonce=request.nonce,
            directory=directory,
        )

    assert raised.value.code == "handoff_cleanup_nonce_mismatch"
    assert request_path.exists()
    assert response_path.exists()


def test_response_reader_rejects_hardlinked_artifact(tmp_path: Path) -> None:
    directory = tmp_path / "control"
    request = create_controlled_recovery_request(
        deployment="okg-workspace",
        worker_pid=1234,
        restart_cutoff_epoch_ms=987654,
        expected_active_workflow_ids=("wf",),
        nonce="n" * 32,
        directory=directory,
    )
    response = attest_controlled_recovery_response(
        request,
        active_workflow_ids=("wf",),
        producers_stopped=True,
        dispatch_stopped=True,
        dbos_destroyed=True,
        directory=directory,
    )
    response_path = controlled_recovery_response_path(
        request.deployment, request.worker_pid, directory=directory
    )
    os.link(response_path, tmp_path / "second-link")

    with pytest.raises(ControlledRecoveryHandoffError) as raised:
        read_controlled_recovery_response(
            request.deployment,
            request.worker_pid,
            expected_request=request,
            expected_marker=response,
            directory=directory,
        )
    assert raised.value.code == "handoff_file_insecure"


def test_attested_cli_exit_terminates_live_non_daemon_dbos_executor(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "control"
    script = r"""
from concurrent.futures import ThreadPoolExecutor
import os
import threading

from okg.substrate.cli.runtime import _complete_runtime_worker_process
from okg.substrate.runtime.recovery_handoff import (
    CONTROLLED_RECOVERY_EXIT_CODE,
    create_controlled_recovery_request,
)
from okg.substrate.runtime import worker

deployment = "okg-workspace"
workflow_id = "wf-live"
create_controlled_recovery_request(
    deployment=deployment,
    worker_pid=os.getpid(),
    restart_cutoff_epoch_ms=123456,
    expected_active_workflow_ids=(workflow_id,),
)
blocked = threading.Event()
executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dbos-executor-")
executor.submit(blocked.wait)
dispatch_stop = threading.Event()
dispatch = threading.Thread(
    target=dispatch_stop.wait,
    name="dbos-dispatch-proof",
    daemon=True,
)
dispatch.start()

class ActiveWorkflows:
    def activeList(self):
        return [workflow_id]

instance = type("Instance", (), {})()
instance._active_workflows_set = ActiveWorkflows()
instance.poller_stop_events = [dispatch_stop]
instance.background_thread_stop_events = []
instance._background_threads = [dispatch]
instance._executor_field = executor
worker._dbos_instance_or_none = lambda: instance

shutdown = worker._shutdown_dbos_gracefully(
    deployment=deployment,
    active_wait_timeout_seconds=0.0,
    dispatch_stop_timeout_seconds=1.0,
)
assert shutdown.destroyed is False
assert shutdown.dispatch_stopped is True
assert shutdown.active_workflow_ids == (workflow_id,)
assert not dispatch.is_alive()
assert any(
    thread.name.startswith("dbos-executor-")
    and thread.is_alive()
    and not thread.daemon
    for thread in threading.enumerate()
)
assert worker._attest_supervised_controlled_recovery(
    deployment=deployment,
    producers_stopped=True,
    shutdown=shutdown,
) is True
_complete_runtime_worker_process(
    CONTROLLED_RECOVERY_EXIT_CODE,
    operator=False,
)
raise AssertionError("controlled exit returned")
"""
    env = dict(os.environ)
    env["OKG_CONTROLLED_RECOVERY_DIRECTORY"] = str(directory)

    try:
        completed = subprocess.run(
            [sys.executable, "-c", script],
            check=False,
            capture_output=True,
            text=True,
            env=env,
            timeout=15,
        )
    except subprocess.TimeoutExpired as exc:
        pytest.fail(
            "controlled recovery process required forced termination; "
            f"stdout={exc.stdout!r} stderr={exc.stderr!r}"
        )

    assert completed.returncode == CONTROLLED_RECOVERY_EXIT_CODE
    marker_line = completed.stderr.strip().splitlines()[-1]
    marker = json.loads(marker_line)
    assert marker["schema"] == CONTROLLED_RECOVERY_MARKER_SCHEMA
    assert marker["event"] == "controlled_recovery_exit"
    response_payload = marker["response"]
    worker_pid = response_payload["worker_pid"]
    request = read_controlled_recovery_request(
        "okg-workspace",
        worker_pid,
        directory=directory,
    )
    assert request is not None
    marker_response = parse_controlled_recovery_marker(
        marker_line,
        expected_request=request,
    )
    response = read_controlled_recovery_response(
        "okg-workspace",
        worker_pid,
        expected_request=request,
        expected_marker=marker_response,
        directory=directory,
    )
    assert response is not None
    assert response.active_workflow_ids == ("wf-live",)


def test_sigterm_stops_producer_and_dispatch_before_synchronous_destroy(
    tmp_path: Path,
) -> None:
    ready_path = tmp_path / "ready"
    script = r"""
import json
import os
from pathlib import Path
import signal
import sys
import threading

import dbos
from okg.substrate.runtime import services, worker

phases = []
producer_stop = threading.Event()

def producer():
    producer_stop.wait()
    phases.append("producer_stopped")

producer_thread = threading.Thread(
    target=producer,
    name="publisher-producer",
    daemon=True,
)
producer_thread.start()
services._reset_state("okg-workspace")
service_state = services.get_services_state()
service_state.maintenance.thread = producer_thread
service_state.maintenance.stop_event = producer_stop

dispatch_stop = threading.Event()

def dispatch():
    dispatch_stop.wait()
    phases.append("dispatch_stopped")

dispatch_thread = threading.Thread(
    target=dispatch,
    name="dbos-dispatch",
    daemon=True,
)
dispatch_thread.start()

class ActiveWorkflows:
    def activeList(self):
        assert not producer_thread.is_alive()
        assert not dispatch_thread.is_alive()
        phases.append("active_empty")
        return []

instance = type("Instance", (), {})()
instance._active_workflows_set = ActiveWorkflows()
instance.poller_stop_events = [dispatch_stop]
instance.background_thread_stop_events = []
instance._background_threads = [dispatch_thread]
worker._dbos_instance_or_none = lambda: instance

def destroy(*, workflow_completion_timeout_sec):
    assert workflow_completion_timeout_sec == 1
    assert phases == [
        "producer_stopped",
        "dispatch_stopped",
        "active_empty",
        "active_empty",
    ]
    phases.append("destroyed")

dbos.DBOS.destroy = staticmethod(destroy)
received = threading.Event()
signal.signal(signal.SIGTERM, lambda _signum, _frame: received.set())
Path(os.environ["OKG_SIGTERM_READY_PATH"]).write_text("ready", encoding="utf-8")
received.wait()

service_stop = services.stop_supervised_services(timeout_s=1.0)
shutdown = worker._shutdown_dbos_after_producers(
    deployment="okg-workspace",
    producers_stopped=service_stop.ok,
)
payload = worker._shutdown_observation_payload(
    deployment="okg-workspace",
    producer_observations=tuple(service_stop.observations),
    shutdown=shutdown,
)
print(json.dumps({"phases": phases, "payload": payload}), flush=True)
sys.exit(
    worker._supervised_shutdown_returncode(
        producers_stopped=service_stop.ok,
        shutdown=shutdown,
        recovery_attested=False,
    )
)
"""
    env = dict(os.environ)
    env["OKG_SIGTERM_READY_PATH"] = str(ready_path)
    process = subprocess.Popen(
        [sys.executable, "-c", script],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        start_new_session=True,
    )
    deadline = time.monotonic() + 15.0
    while not ready_path.exists() and process.poll() is None:
        if time.monotonic() >= deadline:
            process.kill()
            stdout, stderr = process.communicate(timeout=5.0)
            pytest.fail(
                "SIGTERM proof process did not become ready within 15 seconds; "
                f"stdout={stdout!r} stderr={stderr!r}"
            )
        time.sleep(0.01)
    if process.poll() is not None:
        stdout, stderr = process.communicate(timeout=5.0)
        pytest.fail(
            "SIGTERM proof process exited before readiness; "
            f"returncode={process.returncode} stdout={stdout!r} stderr={stderr!r}"
        )

    os.kill(process.pid, signal.SIGTERM)
    stdout, stderr = process.communicate(timeout=5.0)

    assert process.returncode == 0, stderr
    proof = json.loads(stdout.strip().splitlines()[-1])
    assert proof["phases"] == [
        "producer_stopped",
        "dispatch_stopped",
        "active_empty",
        "active_empty",
        "destroyed",
    ]
    assert proof["payload"]["clean"] is True


@pytest.mark.parametrize("inventories_present", [False, True])
def test_launched_dbos_refuses_missing_or_empty_dispatch_inventory(
    inventories_present: bool,
) -> None:
    from okg.substrate.runtime import worker

    class Thread:
        name = "notification-listener"

        def join(self, *, timeout: float) -> None:
            assert timeout >= 0

        def is_alive(self) -> bool:
            return False

    class SystemDatabase:
        _run_background_processes = True

        def _cleanup_connections(self) -> None:
            self._run_background_processes = False

    instance = type("Instance", (), {})()
    instance._launched = True
    instance._background_threads = [Thread()]
    instance._sys_db_field = SystemDatabase()
    if inventories_present:
        instance.poller_stop_events = []
        instance.background_thread_stop_events = []
    observations: list[dict[str, object]] = []

    assert worker._stop_dbos_background_dispatch(
        instance,
        timeout_seconds=0,
        observations=observations,
    ) is False
    errors = observations[0]["errors"]
    assert isinstance(errors, list)
    assert (
        "stop_event_inventory_empty" in errors
        if inventories_present
        else any(str(error).endswith("inventory_missing") for error in errors)
    )
