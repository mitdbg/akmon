"""A stopped runtime worker must not keep publishing or wait out a publish (#3861)."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.publisher import Publisher
from okg.substrate.publisher import publisher as publisher_module
from okg.substrate.runtime.stop_request import clear_stop, request_stop


@pytest.fixture(autouse=True)
def _reset_stop_flag():
    clear_stop()
    yield
    clear_stop()


def _racing_publisher(monkeypatch: pytest.MonkeyPatch) -> tuple[Publisher, list[int]]:
    """A publisher whose every attempt is refused as a mid-publish write race."""
    monkeypatch.setattr(
        publisher_module, "_write_race_backoff_seconds", lambda _attempt: 0.0
    )
    publisher = object.__new__(Publisher)
    publisher.own_source_run_ids = None
    attempts: list[int] = []

    def refused(self, **_kwargs):
        attempts.append(len(attempts) + 1)
        err = SubstrateError("branch head written during publish", data={"stage": "x"})
        err.code = "graph_written_during_publish"
        raise err

    monkeypatch.setattr(Publisher, "_cycle_chronos_native", refused)
    return publisher, attempts


def test_write_race_retry_runs_all_attempts_when_no_stop_was_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publisher, attempts = _racing_publisher(monkeypatch)
    with pytest.raises(SubstrateError) as raised:
        publisher.cycle()
    assert raised.value.code == "graph_written_during_publish"
    assert attempts == [1, 2, 3]
    assert "stopped_before_retry" not in raised.value.data


def test_write_race_retry_does_not_start_another_attempt_after_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publisher, attempts = _racing_publisher(monkeypatch)
    request_stop()
    with pytest.raises(SubstrateError) as raised:
        publisher.cycle()
    assert raised.value.code == "graph_written_during_publish"
    assert attempts == [1]
    assert raised.value.data["stopped_before_retry"] is True


_EXIT_SCRIPT = textwrap.dedent(
    """
    import threading, time
    from okg.substrate.cli.runtime import _complete_runtime_worker_process
    from okg.substrate.runtime.worker import WORKER_RECOVERY_UNATTESTED_EXIT_CODE

    # A non-daemon thread stands in for the executor thread running a publish.
    threading.Thread(target=time.sleep, args=(60,), daemon=False).start()
    _complete_runtime_worker_process(
        WORKER_RECOVERY_UNATTESTED_EXIT_CODE, operator=False
    )
    """
)


def test_unattested_exit_with_running_workflow_does_not_wait_for_it() -> None:
    started = time.monotonic()
    process = subprocess.Popen(
        [sys.executable, "-c", _EXIT_SCRIPT],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=dict(os.environ),
        start_new_session=True,
    )
    try:
        _stdout, _stderr = process.communicate(timeout=20.0)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate(timeout=5.0)
        pytest.fail("worker process waited on its running workflow thread")
    assert time.monotonic() - started < 20.0
    from okg.substrate.runtime.worker import WORKER_RECOVERY_UNATTESTED_EXIT_CODE

    assert process.returncode == WORKER_RECOVERY_UNATTESTED_EXIT_CODE
