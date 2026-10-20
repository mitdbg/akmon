"""A source child that kills the whole provision at one service function.

Source runs happen in ``spawn`` child processes, so a crash point inside the
source run (``_finalize_source_admission``) has to be installed in the child.
Killing only the child would be a source failure the coordinator recovers
from; the #1889 scenario is the whole process dying (an out-of-memory kill of
the init container). The stand-in therefore SIGKILLs its parent, and then
exits itself when the lifeline pipe closes, as any orphaned child does.
"""
from __future__ import annotations

import os
import signal
import sys
import time
from typing import Any

from okg.substrate.ingest import source_jobs

#: Crash points that run inside the source child.
SOURCE_CHILD_CRASH_POINTS = frozenset({"_finalize_source_admission"})
_ENV = "OKG_TEST_SOURCE_CHILD_CRASH_POINT"
_REAL_CHILD_MAIN = source_jobs.child_main


def crashing_child_main(result_conn: Any, lifeline: Any, payload: dict) -> None:
    import okg.substrate.ingest.service as service

    crash_point = os.environ[_ENV]

    def _hard_crash(*_args: Any, **_kwargs: Any) -> None:
        sys.stdout.flush()
        sys.stderr.write(f"SIMULATED HARD CRASH at {crash_point}\n")
        sys.stderr.flush()
        os.kill(os.getppid(), signal.SIGKILL)
        time.sleep(60)  # the lifeline watcher ends this child

    setattr(service, crash_point, _hard_crash)
    _REAL_CHILD_MAIN(result_conn, lifeline, payload)


def install(crash_point: str) -> None:
    """Make every source child of this process crash at ``crash_point``."""
    os.environ[_ENV] = crash_point
    source_jobs.child_main = crashing_child_main
