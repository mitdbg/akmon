from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

from okg.substrate.runtime import environment_exec


def test_launchd_wrapper_forwards_sigterm_and_records_exact_clean_exit(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "service.env"
    env_file.write_text("OKG_SERVICE_ID=org.okg.worker.fixture\n", encoding="utf-8")
    receipt = tmp_path / "supervision.json"
    ready = tmp_path / "child-ready"
    child_code = (
        "import pathlib,signal,time\n"
        "def stop(*_args):\n"
        "    raise SystemExit(0)\n"
        "signal.signal(signal.SIGTERM, stop)\n"
        f"pathlib.Path({str(ready)!r}).write_text('ready')\n"
        "time.sleep(60)\n"
    )
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "okg.substrate.runtime.environment_exec",
            "--env-file",
            str(env_file),
            "--supervision-receipt",
            str(receipt),
            "--",
            sys.executable,
            "-c",
            child_code,
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    deadline = time.monotonic() + 5
    running: dict[str, object] | None = None
    while time.monotonic() < deadline:
        if receipt.is_file():
            candidate = json.loads(receipt.read_text(encoding="utf-8"))
            if candidate.get("state") == "running":
                running = candidate
                break
        time.sleep(0.01)
    assert running is not None
    assert running["supervisor_pid"] == process.pid
    child_pid = int(running["child_pid"])
    assert child_pid != process.pid
    ready_deadline = time.monotonic() + 5
    while not ready.is_file() and time.monotonic() < ready_deadline:
        time.sleep(0.01)
    assert ready.is_file()

    process.send_signal(signal.SIGTERM)
    assert process.wait(timeout=5) == 0
    terminal = json.loads(receipt.read_text(encoding="utf-8"))
    assert terminal == {
        **running,
        "state": "exited",
        "exited_at": terminal["exited_at"],
        "exit_code": 0,
        "term_signal": None,
    }
    assert stat_mode(receipt) == 0o600
    try:
        os.kill(child_pid, 0)
    except ProcessLookupError:
        pass
    else:
        raise AssertionError("launchd wrapper left its child alive")


def test_launchd_wrapper_reaps_child_when_running_receipt_write_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt = tmp_path / "supervision.json"
    publication_error = OSError("simulated running receipt publication failure")
    observed_child_pid: int | None = None

    def fail_running_receipt(
        _path: Path,
        payload: dict[str, object],
    ) -> None:
        nonlocal observed_child_pid
        assert payload["state"] == "running"
        observed_child_pid = int(payload["child_pid"])
        raise publication_error

    monkeypatch.setattr(
        environment_exec,
        "_atomic_write_json",
        fail_running_receipt,
    )
    with pytest.raises(OSError) as refusal:
        environment_exec._run_supervised_launchd_process(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            environment={"OKG_SERVICE_ID": "org.okg.worker.fixture"},
            receipt_path=receipt,
        )

    assert refusal.value is publication_error
    assert observed_child_pid is not None
    assert not receipt.exists()
    try:
        os.kill(observed_child_pid, 0)
    except ProcessLookupError:
        pass
    else:
        raise AssertionError("receipt publication failure left its child alive")


def stat_mode(path: Path) -> int:
    return path.stat().st_mode & 0o777
