"""Stand-in child entry points for the source-job process tests.

Each function has the signature of ``source_jobs.child_main`` --
``(result_conn, lifeline, payload)`` -- so ``SpawnedSourceJob`` can start it
with the ``spawn`` start method. They exercise the process mechanics (pipes,
lifeline, memory, exit status) without a database-backed source run.
"""
from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from typing import Any

from okg.substrate.ingest import source_jobs


def send_large_result(result_conn: Any, lifeline: Any, payload: dict) -> None:
    """Send a summary larger than any pipe buffer, then exit cleanly."""
    result_conn.send(("started", {}))
    result_conn.send((
        "result",
        {
            "name": payload["name"],
            "status": "OK",
            "blob": "x" * int(payload["size"]),
        },
    ))
    result_conn.close()
    os._exit(0)


def sleep_then_result(result_conn: Any, lifeline: Any, payload: dict) -> None:
    """Report ready, sleep, then report success; records its own timing."""
    started = time.time()
    result_conn.send(("started", {}))
    time.sleep(float(payload["seconds"]))
    result_conn.send((
        "result",
        {
            "name": payload["name"],
            "status": "OK",
            "child_started": started,
            "child_finished": time.time(),
        },
    ))
    result_conn.close()
    os._exit(0)


def hog_memory(result_conn: Any, lifeline: Any, payload: dict) -> None:
    """Touch ``megabytes`` of memory and hold it."""
    result_conn.send(("started", {}))
    block = bytearray(int(payload["megabytes"]) * 1024 * 1024)
    for offset in range(0, len(block), 4096):
        block[offset] = 1
    time.sleep(float(payload.get("hold_seconds", 60)))
    result_conn.send(("result", {"name": payload["name"], "status": "OK"}))
    os._exit(0)


def crash_after_started(result_conn: Any, lifeline: Any, payload: dict) -> None:
    """Report the pre-run state as a real child does, then die mid-run."""
    result_conn.send(("started", dict(payload["started"])))
    time.sleep(0.2)
    os._exit(int(payload.get("exit_status", 3)))


def report_connections_then_sleep(result_conn: Any, lifeline: Any, payload: dict) -> None:
    """Report ready and how many connections it holds, then run a while."""
    started = time.time()
    held = int(payload["held"])
    result_conn.send(("started", {}))
    result_conn.send(("connections", {"current": held, "peak": held}))
    time.sleep(float(payload["seconds"]))
    result_conn.send((
        "result",
        {
            "name": payload["name"],
            "status": "OK",
            "child_started": started,
            "child_finished": time.time(),
        },
    ))
    result_conn.close()
    os._exit(0)


def result_then_hog(result_conn: Any, lifeline: Any, payload: dict) -> None:
    """Deliver the result first, then grow past the limit before exiting.

    With ``result_sent_marker`` set, create that file once both messages are
    in the pipe, so a test can wait for the result to be sent rather than
    assume the two sends happen back to back.
    """
    result_conn.send(("started", {}))
    result_conn.send(("result", {"name": payload["name"], "status": "OK"}))
    marker = payload.get("result_sent_marker")
    if marker:
        Path(marker).touch()
    block = bytearray(int(payload["megabytes"]) * 1024 * 1024)
    for offset in range(0, len(block), 4096):
        block[offset] = 1
    time.sleep(float(payload.get("hold_seconds", 2)))
    os._exit(0)


def hold_run_lock(result_conn: Any, lifeline: Any, payload: dict) -> None:
    """A real source child's guards: the lifeline watcher and the run lock."""
    threading.Thread(
        target=source_jobs._watch_lifeline,
        args=(lifeline,),
        daemon=True,
    ).start()
    from okg.substrate.db.pool import configure_source_process_pool

    configure_source_process_pool()
    hooks = source_jobs.ChildJobHooks(
        dsn=payload["dsn"],
        deployment=payload["deployment"],
        result_conn=result_conn,
        lock_wait_seconds=5.0,
    )
    hooks.acquire_run_lock(payload["source"])
    hooks.started(lock_key=hooks.lock_key)
    time.sleep(600)
    os._exit(0)


def orphaning_parent(payload: dict) -> None:
    """A coordinator stand-in that starts one lock-holding child and waits.

    The test kills this process with SIGKILL; its child must then exit on its
    own (the lifeline closes) and release the run lock.
    """
    job = source_jobs.SpawnedSourceJob(
        name=payload["source"],
        payload=payload,
        memory_limit=1 << 40,
        on_crash=lambda job: {"name": job.name, "status": "FAIL"},
        target=hold_run_lock,
    )
    kind, _message = job.conn.recv()
    assert kind == "started"
    Path(payload["pid_file"]).write_text(str(job.pid))
    time.sleep(600)


def report_deployment_lookup(result_conn: Any, lifeline: Any, payload: dict) -> None:
    """Run ``child_main``'s process setup, then send what a name lookup of
    ``payload["name"]`` finds in this child."""
    from okg.substrate.deployments import find_deployment_dir

    source_jobs.prepare_child_process(payload)
    found = find_deployment_dir(payload["name"])
    result_conn.send(("result", {"found": None if found is None else str(found)}))
    result_conn.close()


def report_run_guard(result_conn: Any, lifeline: Any, payload: dict) -> None:
    """Run ``child_main``'s process setup, then send whether this child
    checks a parent's source-sync hold for its deployment (#3003 round 3)."""
    from okg.substrate.runtime.source_sync_priority import (
        source_sync_run_guard_held,
    )

    source_jobs.prepare_child_process(payload)
    held = source_sync_run_guard_held(str(payload["options"]["deployment"]))
    result_conn.send(("result", {"held": held}))
    result_conn.close()
