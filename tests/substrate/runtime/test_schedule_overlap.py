"""A scheduled workflow does not run on top of its own previous run
(issue #1598).

okg-workspace ran `doctor_workflow` and `okg_workspace_resource_monitor` on
`*/5 * * * *` crons while each run averaged over 20 minutes. Every tick
started another copy of work already in flight, and each copy held database
connections for its whole run.
"""
from __future__ import annotations

import threading

import pytest

from okg.substrate.runtime.schedule_overlap import (
    OverlapSkipped,
    claim_schedule_run,
    reset_schedule_claims,
    skipped_run_counts,
)


@pytest.fixture(autouse=True)
def _clean_claims():
    reset_schedule_claims()
    yield
    reset_schedule_claims()


def test_a_tick_arriving_during_a_run_is_skipped():
    with claim_schedule_run("okg_substrate_doctor"):
        with pytest.raises(OverlapSkipped) as exc:
            with claim_schedule_run("okg_substrate_doctor"):
                pytest.fail("the overlapping tick ran anyway")
    assert exc.value.schedule_id == "okg_substrate_doctor"
    assert exc.value.skipped_count == 1
    assert "still running from an earlier tick" in str(exc.value)


def test_the_skip_is_counted_so_a_too_tight_cron_is_visible():
    with claim_schedule_run("monitor"):
        for _ in range(3):
            with pytest.raises(OverlapSkipped):
                with claim_schedule_run("monitor"):
                    pytest.fail("unreachable")
    assert skipped_run_counts() == {"monitor": 3}


def test_different_schedules_do_not_block_each_other():
    with claim_schedule_run("doctor"):
        with claim_schedule_run("monitor"):
            pass
    assert skipped_run_counts() == {}


def test_a_failing_run_releases_the_claim():
    """A raised body must not wedge the schedule forever."""
    def _failing_run():
        with claim_schedule_run("doctor"):
            raise RuntimeError("the run failed")

    with pytest.raises(RuntimeError):
        _failing_run()
    with claim_schedule_run("doctor"):
        pass
    assert skipped_run_counts() == {}


def test_the_claim_is_held_across_threads():
    started = threading.Event()
    release = threading.Event()
    outcome: list[str] = []

    def _long_run():
        with claim_schedule_run("doctor"):
            started.set()
            release.wait(timeout=5)
            outcome.append("finished")

    worker = threading.Thread(target=_long_run)
    worker.start()
    try:
        assert started.wait(timeout=5)
        with pytest.raises(OverlapSkipped):
            with claim_schedule_run("doctor"):
                pytest.fail("a second thread ran the same schedule")
    finally:
        release.set()
        worker.join(timeout=5)
    assert outcome == ["finished"]


def test_deployment_author_schedule_skips_instead_of_stacking(monkeypatch):
    """The resource monitors reach this guard through the SDK decorator."""
    import sys

    from okg.deployment._internal import decorators

    class _FakeDBOS:
        @staticmethod
        def workflow(*, name):
            def _wrap(fn):
                return fn
            return _wrap

    monkeypatch.setitem(
        sys.modules, "dbos", type("M", (), {"DBOS": _FakeDBOS}),
    )
    # The registry is process-global and other deployment modules register
    # into it at import time. Snapshot and restore rather than resetting it,
    # so this test cannot strand a schedule for whatever runs next.
    saved = dict(decorators._DEPLOYMENT_SCHEDULED)
    started = threading.Event()
    release = threading.Event()

    @decorators.deployment_scheduled(
        "fake_monitor", default_cron="*/5 * * * *", guard_body=False,
    )
    def _monitor(scheduled_time=None, context=None):
        started.set()
        release.wait(timeout=5)
        return "ran"

    try:
        long_run = threading.Thread(target=lambda: _monitor(None, None))
        long_run.start()
        assert started.wait(timeout=5)
        # A second tick arrives while the first is still running.
        result = _monitor(None, None)
        release.set()
        long_run.join(timeout=5)
    finally:
        release.set()
        decorators._DEPLOYMENT_SCHEDULED.clear()
        decorators._DEPLOYMENT_SCHEDULED.update(saved)

    assert result["status"] == "skipped_previous_run_active"
    assert result["schedule_id"] == "fake_monitor"
    assert result["skipped_runs"] == 1


def test_a_body_that_raises_overlapskipped_is_not_reported_as_a_skip():
    """The guard reports a refusal to START, not a failure midway.

    Catching the whole body would let a nested skip — from some other
    schedule the body touches — be reported as "this run never started",
    hiding work that ran and then aborted.
    """
    import sys

    from okg.deployment._internal import decorators

    class _FakeDBOS:
        @staticmethod
        def workflow(*, name):
            def _wrap(fn):
                return fn
            return _wrap

    saved_module = sys.modules.get("dbos")
    sys.modules["dbos"] = type("M", (), {"DBOS": _FakeDBOS})
    saved = dict(decorators._DEPLOYMENT_SCHEDULED)
    ran = []
    try:
        @decorators.deployment_scheduled(
            "outer_schedule", default_cron="*/5 * * * *", guard_body=False,
        )
        def _outer(scheduled_time=None, context=None):
            ran.append(1)
            with claim_schedule_run("some_other_schedule"):
                with claim_schedule_run("some_other_schedule"):
                    pass

        with pytest.raises(OverlapSkipped):
            _outer(None, None)
    finally:
        decorators._DEPLOYMENT_SCHEDULED.clear()
        decorators._DEPLOYMENT_SCHEDULED.update(saved)
        if saved_module is None:
            sys.modules.pop("dbos", None)
        else:
            sys.modules["dbos"] = saved_module

    assert ran == [1], "the body ran; it must not be reported as skipped"
    # The outer schedule released its own claim on the way out.
    with claim_schedule_run("outer_schedule"):
        pass
