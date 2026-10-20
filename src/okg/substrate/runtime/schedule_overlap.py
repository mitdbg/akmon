"""Keep a scheduled workflow from running on top of its own previous run.

A cron cadence is a statement about how often work should START, not a
promise that the previous run has finished. On a large graph that gap gets
wide: okg-workspace (2026-09-04) ran ``doctor_workflow`` and
``okg_workspace_resource_monitor`` on ``*/5 * * * *`` crons while each run
averaged ~1390 s and ~1319 s. Every tick started another copy of work the
worker was already doing, so several runs of the same schedule were
permanently in flight, holding database connections the whole time. A
five-hour repair run then died at its final publish preflight on a
``PoolTimeout``.

This module is the cheap half of the remedy: a schedule claims its own name
before it runs, and a tick that arrives while the claim is held is SKIPPED
with a recorded reason rather than stacked on top. The reservation in
`okg.substrate.db.pool` is the other half — it bounds what a long run can
take even when it is the only one.

The claim is process-local, and that is the honest scope. A deployment's
scheduled workflows run in that deployment's worker process, so overlap of a
schedule with itself is overlap within one process. A cross-process claim
would need a durable lock, and a lock a crashed worker still holds is a worse
failure than a duplicated tick: it wedges the schedule until an operator
notices. A skip is reported, never silent, so a schedule that is chronically
skipping is visible as an over-tight cron rather than hidden as smooth
operation.
"""
from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Iterator


__all__ = [
    "OverlapSkipped",
    "claim_schedule_run",
    "reset_schedule_claims",
    "skipped_run_counts",
]


_LOCK = threading.Lock()
#: schedule_id -> True while a run holds the claim.
_ACTIVE: dict[str, bool] = {}
#: schedule_id -> ticks skipped because the previous run was still active.
_SKIPPED: dict[str, int] = {}


class OverlapSkipped(Exception):
    """Raised inside :func:`claim_schedule_run` when a run is already active.

    Carries the schedule id and how many ticks that schedule has skipped in
    this process, so a caller can report the skip instead of swallowing it.
    """

    def __init__(self, schedule_id: str, skipped_count: int) -> None:
        super().__init__(
            f"scheduled workflow {schedule_id!r} is still running from an "
            f"earlier tick; skipping this one ({skipped_count} skipped in "
            f"this worker). A schedule that skips repeatedly is running "
            f"longer than its cron period — widen the period rather than "
            f"letting the runs stack up."
        )
        self.schedule_id = schedule_id
        self.skipped_count = skipped_count


def reset_schedule_claims() -> None:
    """Drop every claim and skip count. Worker boot and test hook."""
    with _LOCK:
        _ACTIVE.clear()
        _SKIPPED.clear()


def skipped_run_counts() -> dict[str, int]:
    """Ticks skipped per schedule id in this process."""
    with _LOCK:
        return dict(_SKIPPED)


@contextmanager
def claim_schedule_run(schedule_id: str) -> Iterator[None]:
    """Hold the single-run claim for `schedule_id` for the body's duration.

    Raises :class:`OverlapSkipped` — without running the body — when a run of
    the same schedule is already in flight in this process. The claim is
    released on the way out whether the body succeeded or raised, so one
    failing run cannot wedge the schedule.
    """
    with _LOCK:
        if _ACTIVE.get(schedule_id):
            skipped = _SKIPPED.get(schedule_id, 0) + 1
            _SKIPPED[schedule_id] = skipped
            raise OverlapSkipped(schedule_id, skipped)
        _ACTIVE[schedule_id] = True
    try:
        yield
    finally:
        with _LOCK:
            _ACTIVE.pop(schedule_id, None)
