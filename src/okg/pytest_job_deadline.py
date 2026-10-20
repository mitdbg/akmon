"""Dump every thread's stack before a CI job's time limit ends the job.

A CI job has one time limit: its ``timeout-minutes``. When a hung test holds a
job to that limit, GitHub cancels the job, and without help the log says
nothing about which test hung. ``scripts/qa.py`` loads this plugin into every
pytest process it starts (through ``PYTEST_PLUGINS``, so pytest-xdist workers
load it too) and gives it two ways to print the stacks:

* ``OKG_QA_STACK_DUMP_AT`` (Unix seconds): a timer prints every thread's
  stack once, at that moment. ``scripts/qa.py`` sets it a few minutes before
  the job's limit; see ``scripts/qa.py::_job_stack_dump_at`` for where the
  limit and the job's start come from.
* ``SIGUSR1``: prints every thread's stack immediately (C-level faulthandler,
  so it works even when the interpreter is stuck). ``scripts/qa.py`` sends it
  to the pytest process group when it is itself interrupted or terminated.

Neither path stops or fails a test; the job limit does that. The timer runs in
a Python thread, so a hang in C code that never releases the GIL delays it;
the SIGUSR1 path has no such limit.
"""

from __future__ import annotations

import faulthandler
import os
import signal
import sys
import threading
import time

import pytest

STACK_DUMP_AT_ENV = "OKG_QA_STACK_DUMP_AT"
MARKER = "[okg-job-deadline]"

_timer_key = pytest.StashKey[threading.Timer]()
_stream_key = pytest.StashKey[object]()


def _stderr_fileno() -> int:
    try:
        fileno = sys.stderr.fileno()
        if fileno == -1:
            raise AttributeError
        return fileno
    except (AttributeError, ValueError):
        assert sys.__stderr__ is not None
        return sys.__stderr__.fileno()


def _label() -> str:
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    return f"pid {os.getpid()}" + (f" (xdist {worker})" if worker else "")


def parse_dump_at(raw: str) -> float:
    """The dump moment, or a ValueError naming the variable."""
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{STACK_DUMP_AT_ENV} must be Unix seconds, not {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{STACK_DUMP_AT_ENV} must be positive, not {raw!r}")
    return value


def pytest_configure(config: pytest.Config) -> None:
    stream = os.fdopen(os.dup(_stderr_fileno()), "w", buffering=1)
    config.stash[_stream_key] = stream
    if hasattr(signal, "SIGUSR1"):
        faulthandler.register(signal.SIGUSR1, file=stream, all_threads=True, chain=False)
    raw = os.environ.get(STACK_DUMP_AT_ENV)
    if not raw:
        return
    dump_at = parse_dump_at(raw)

    def _dump() -> None:
        stream.write(
            f"\n{MARKER} {_label()}: the job's time limit is near; "
            "stacks of every thread follow\n"
        )
        stream.flush()
        faulthandler.dump_traceback(file=stream, all_threads=True)
        stream.flush()

    timer = threading.Timer(max(0.0, dump_at - time.time()), _dump)
    timer.daemon = True
    timer.name = "okg-job-deadline"
    timer.start()
    config.stash[_timer_key] = timer


def pytest_unconfigure(config: pytest.Config) -> None:
    timer = config.stash.get(_timer_key, None)
    if timer is not None:
        timer.cancel()
    if hasattr(signal, "SIGUSR1"):
        faulthandler.unregister(signal.SIGUSR1)
    stream = config.stash.get(_stream_key, None)
    if stream is not None:
        stream.close()
