"""harden-okg-ci-runtime / req.fleet-monitor-registration.

A ``@deployment_scheduled`` workflow must register a *durable* schedule the
substrate worker applies after launch — not arm DBOS's native in-process
scheduler. The native path fired ``okg_ci_resource_monitor`` by name every tick
but the function was unresolvable there, so it raised
``WorkflowFunctionNotFoundError`` every 5 min and fleet monitoring never ran
(observed live on okg-ci 2026-06-16; no ``workflow_schedules`` row existed).

The pre-existing okg-workspace test only asserted the manifest lists the
schedule + the decorator stamps ``_okg_schedule_id`` — a mock-the-sink test that
stayed green while the live schedule was broken. These tests assert the contract
that actually matters: the decorator registers a *named* ``@DBOS.workflow`` (and
does NOT call ``DBOS.scheduled``), and ``apply_deployment_author_schedules``
persists one ``apply_schedules`` row per author workflow with the manifest cron.
"""
from __future__ import annotations

from contextlib import contextmanager

import pytest


@pytest.fixture
def fake_dbos(monkeypatch):
    """Replace DBOS.workflow / DBOS.scheduled / DBOS.apply_schedules with
    capturing fakes so the decorator + apply walk can be exercised with no
    constructed DBOS singleton or database."""
    import dbos

    captured: dict[str, object] = {
        "workflow_names": [],
        "scheduled_called": False,
        "applied": None,
    }

    def fake_workflow(*_args, **kwargs):
        name = kwargs.get("name")

        def _decorator(fn):
            captured["workflow_names"].append(name)  # type: ignore[union-attr]
            return fn

        return _decorator

    def fake_scheduled(*_args, **_kwargs):
        # The decorator must NOT use the native scheduler — flag if it does.
        captured["scheduled_called"] = True

        def _decorator(fn):
            return fn

        return _decorator

    def fake_apply_schedules(specs):
        captured["applied"] = specs

    monkeypatch.setattr(dbos.DBOS, "workflow", staticmethod(fake_workflow))
    monkeypatch.setattr(dbos.DBOS, "scheduled", staticmethod(fake_scheduled))
    monkeypatch.setattr(
        dbos.DBOS, "apply_schedules", staticmethod(fake_apply_schedules),
    )
    return captured


def _define(schedule_id="okg_demo_monitor", cron="*/7 * * * *", backfill=False):
    from okg.deployment._internal.decorators import deployment_scheduled

    @deployment_scheduled(
        schedule_id=schedule_id,
        default_cron=cron,
        automatic_backfill=backfill,
    )
    def _wf(scheduled_time, context):  # noqa: ANN001, ARG001
        return {"ok": True}

    return _wf


def test_decorator_registers_named_workflow_not_native_scheduler(fake_dbos):
    from okg.deployment._internal.decorators import (
        iter_deployment_scheduled,
        reset_deployment_scheduled,
    )

    reset_deployment_scheduled()
    wf = _define()

    # Registered as a NAMED workflow, resolvable by schedule_id...
    assert "okg_demo_monitor" in fake_dbos["workflow_names"]
    # ...and did NOT arm DBOS's native scheduler (the live bug).
    assert fake_dbos["scheduled_called"] is False
    # Recorded for the post-launch apply walk.
    entries = {e["schedule_id"]: e for e in iter_deployment_scheduled()}
    assert entries["okg_demo_monitor"]["default_cron"] == "*/7 * * * *"
    assert entries["okg_demo_monitor"]["automatic_backfill"] is False
    # Back-compat metadata stamps preserved.
    assert getattr(wf, "_okg_schedule_id", None) == "okg_demo_monitor"


def test_apply_persists_durable_row_with_manifest_cron_override(fake_dbos):
    from okg.deployment._internal.decorators import reset_deployment_scheduled
    from okg.substrate.runtime.scheduled import (
        apply_deployment_author_schedules,
    )

    reset_deployment_scheduled()
    _define()

    # Manifest overrides cadence (Decision 14); default was */7.
    applied = apply_deployment_author_schedules(
        schedule_overrides={"okg_demo_monitor": "*/15 * * * *"},
    )
    assert applied == ["okg_demo_monitor"]

    specs = fake_dbos["applied"]
    assert specs is not None and len(specs) == 1
    spec = specs[0]
    assert spec["schedule_name"] == "okg_demo_monitor"
    assert spec["schedule"] == "*/15 * * * *"  # override wins over default */7
    assert spec["queue_name"] is None  # runs directly on cron, like doctor
    assert spec["automatic_backfill"] is False
    assert callable(spec["workflow_fn"])


def test_apply_uses_default_cron_without_override(fake_dbos):
    from okg.deployment._internal.decorators import reset_deployment_scheduled
    from okg.substrate.runtime.scheduled import (
        apply_deployment_author_schedules,
    )

    reset_deployment_scheduled()
    _define(cron="*/9 * * * *")
    apply_deployment_author_schedules(schedule_overrides={})
    assert fake_dbos["applied"][0]["schedule"] == "*/9 * * * *"


def test_apply_is_noop_without_author_schedules(fake_dbos):
    from okg.deployment._internal.decorators import reset_deployment_scheduled
    from okg.substrate.runtime.scheduled import (
        apply_deployment_author_schedules,
    )

    reset_deployment_scheduled()
    assert apply_deployment_author_schedules(schedule_overrides={}) == []
    assert fake_dbos["applied"] is None  # apply_schedules not called


def test_deployment_author_schedule_body_requires_runtime_writer_permit(
    fake_dbos,
):
    from okg.deployment._internal.decorators import (
        configure_deployment_scheduled_guard,
        reset_deployment_scheduled,
    )

    reset_deployment_scheduled()
    events: list[str] = []

    @contextmanager
    def deny_while_fenced(promotion_id):
        events.append(f"permit:{promotion_id}")
        raise RuntimeError("deployment writers are fenced")
        yield  # pragma: no cover

    configure_deployment_scheduled_guard(deny_while_fenced)

    from okg.deployment._internal.decorators import deployment_scheduled

    @deployment_scheduled(
        schedule_id="guarded-author-schedule",
        default_cron="*/5 * * * *",
    )
    def guarded_author_schedule(scheduled_time, context):
        del scheduled_time, context
        events.append("body")

    with pytest.raises(RuntimeError, match="writers are fenced"):
        guarded_author_schedule(None, None)

    assert events == ["permit:None"]


def test_deployment_author_recovery_context_reaches_writer_guard(fake_dbos):
    from okg.deployment._internal.decorators import (
        configure_deployment_scheduled_guard,
        deployment_scheduled,
        reset_deployment_scheduled,
    )

    reset_deployment_scheduled()
    observed: list[str | None] = []

    @contextmanager
    def permit(promotion_id):
        observed.append(promotion_id)
        yield

    configure_deployment_scheduled_guard(permit)

    @deployment_scheduled(
        schedule_id="recovery-author-schedule",
        default_cron="*/5 * * * *",
    )
    def recovery_author_schedule(scheduled_time, context):
        del scheduled_time, context

    recovery_author_schedule(
        None,
        {
            "okg_recovery_kind": "runtime_dlq_witness",
            "okg_promotion_id": "promotion-owner",
        },
    )

    assert observed == ["promotion-owner"]


def test_enqueue_only_author_workflow_does_not_hold_outer_writer_guard(fake_dbos):
    from okg.deployment._internal.decorators import (
        configure_deployment_scheduled_guard,
        deployment_scheduled,
        reset_deployment_scheduled,
    )

    reset_deployment_scheduled()
    events: list[str] = []

    @contextmanager
    def outer_guard(promotion_id):
        events.append(f"outer:{promotion_id}")
        yield

    configure_deployment_scheduled_guard(outer_guard)

    @deployment_scheduled(
        schedule_id="enqueue-only-recovery",
        default_cron="*/5 * * * *",
        guard_body=False,
    )
    def enqueue_only_recovery(scheduled_time, context):
        del scheduled_time, context
        events.append("child-enqueue-completed")

    enqueue_only_recovery(
        None,
        {
            "okg_recovery_kind": "runtime_dlq_witness",
            "okg_promotion_id": "promotion-owner",
        },
    )

    assert events == ["child-enqueue-completed"]


@pytest.mark.postgres
def test_enqueue_only_recovery_child_acquires_active_fence_without_deadlock(
    fake_dbos,
    clean_runtime: str,
):
    import psycopg

    from okg.deployment._internal.decorators import (
        configure_deployment_scheduled_guard,
        deployment_scheduled,
        reset_deployment_scheduled,
    )
    from okg.substrate.runtime.promotion_fence import promotion_enqueue_permit

    deployment = "enqueue-only-deadlock-proof"
    promotion_id = "promotion-owner"
    separator = "&" if "?" in clean_runtime else "?"
    bounded_dsn = (
        f"{clean_runtime}{separator}options=-clock_timeout%3D1000ms"
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO okg.deployment_promotion_fences "
            "(deployment_name, promotion_id, status) "
            "VALUES (%s, %s, 'fenced') "
            "ON CONFLICT (deployment_name) DO UPDATE SET "
            "promotion_id = EXCLUDED.promotion_id, status = EXCLUDED.status",
            (deployment, promotion_id),
        )

    reset_deployment_scheduled()
    configure_deployment_scheduled_guard(
        lambda observed_owner: promotion_enqueue_permit(
            deployment=deployment,
            dsn=bounded_dsn,
            promotion_id=observed_owner,
        )
    )
    events: list[str] = []

    @deployment_scheduled(
        schedule_id="enqueue-only-deadlock-proof",
        default_cron="*/5 * * * *",
        guard_body=False,
    )
    def enqueue_only_recovery(scheduled_time, context):
        del scheduled_time
        with promotion_enqueue_permit(
            deployment=deployment,
            dsn=bounded_dsn,
            promotion_id=context["okg_promotion_id"],
        ):
            events.append("child-enqueue-completed")

    enqueue_only_recovery(
        None,
        {
            "okg_recovery_kind": "runtime_dlq_witness",
            "okg_promotion_id": promotion_id,
        },
    )

    assert events == ["child-enqueue-completed"]


# ---------------------------------------------------------------------------
# Issue #1338 -- hermetic reproduction of the failure that made
# `okg_workspace_self_play` fail every run it ever had.
#
# The live error was `psycopg.errors.IdleInTransactionSessionTimeout`
# (SQLSTATE 25P03) about six minutes into a run, against a five-minute
# server-side bound. The cause is not the bound: it is that the SDK's default
# `guard_body=True` wraps the workflow body in the deployment writer permit,
# which is a `pg_advisory_xact_lock` and therefore an OPEN Postgres
# transaction, and the body is a long non-database wait (a `subprocess.run` of
# the self-play runner, bounded at 1800 seconds). No bound the operator could
# reasonably pick would let that succeed.
#
# These two tests are the same body under the two shapes. They run against a
# real Postgres with a one-second idle-in-transaction bound so the six-minute
# live case fits in a test: the guarded shape dies with the live SQLSTATE, the
# enqueue-only shape does not.
# ---------------------------------------------------------------------------


@pytest.fixture
def restored_schedule_registry():
    """Save and restore the process-wide author-schedule registry and guard.

    `configure_deployment_scheduled_guard` sets a module global with no
    teardown of its own. A test that leaves a guard bound to a dead DSN makes
    an unrelated later test fail depending only on collection order.
    """
    from okg.deployment._internal import decorators

    saved_registry = dict(decorators._DEPLOYMENT_SCHEDULED)
    saved_guard = decorators._DEPLOYMENT_SCHEDULED_GUARD
    try:
        yield
    finally:
        decorators._DEPLOYMENT_SCHEDULED.clear()
        decorators._DEPLOYMENT_SCHEDULED.update(saved_registry)
        decorators._DEPLOYMENT_SCHEDULED_GUARD = saved_guard


def _slow_body_schedule(schedule_id: str, *, guard_body: bool, dsn: str):
    """Register one author schedule whose body waits without touching the DB."""
    import time

    from okg.deployment._internal.decorators import (
        configure_deployment_scheduled_guard,
        deployment_scheduled,
        reset_deployment_scheduled,
    )
    from okg.substrate.runtime.promotion_fence import promotion_enqueue_permit

    deployment = f"idle-bound-{schedule_id}"

    reset_deployment_scheduled()
    configure_deployment_scheduled_guard(
        lambda promotion_id: promotion_enqueue_permit(
            deployment=deployment, dsn=dsn, promotion_id=promotion_id,
        )
    )

    @deployment_scheduled(
        schedule_id=schedule_id,
        default_cron="*/5 * * * *",
        automatic_backfill=False,
        guard_body=guard_body,
    )
    def _slow(scheduled_time, context):
        del scheduled_time, context
        # Stands in for the self-play runner subprocess: a wait that touches
        # no database at all, and therefore cannot itself be at fault.
        time.sleep(2.5)
        return {"ok": True}

    return _slow


@pytest.mark.postgres
def test_guarded_body_dies_with_the_live_idle_in_transaction_error(
    fake_dbos,
    clean_runtime: str,
    monkeypatch,
    restored_schedule_registry,
) -> None:
    """The shape `okg_workspace_self_play` shipped with, and why it never ran."""
    import psycopg

    from okg.substrate.db.connection import IDLE_IN_TRANSACTION_TIMEOUT_ENV

    monkeypatch.setenv(IDLE_IN_TRANSACTION_TIMEOUT_ENV, "1000")

    guarded = _slow_body_schedule(
        "idle-bound-guarded", guard_body=True, dsn=clean_runtime,
    )

    with pytest.raises(psycopg.errors.IdleInTransactionSessionTimeout):
        guarded(None, None)


@pytest.mark.postgres
def test_enqueue_only_body_survives_a_wait_longer_than_the_idle_bound(
    fake_dbos,
    clean_runtime: str,
    monkeypatch,
    restored_schedule_registry,
) -> None:
    """The shape it ships with now: the same wait, no transaction held."""
    from okg.substrate.db.connection import IDLE_IN_TRANSACTION_TIMEOUT_ENV

    monkeypatch.setenv(IDLE_IN_TRANSACTION_TIMEOUT_ENV, "1000")

    enqueue_only = _slow_body_schedule(
        "idle-bound-enqueue-only", guard_body=False, dsn=clean_runtime,
    )

    assert enqueue_only(None, None) == {"ok": True}


@pytest.mark.postgres
def test_the_shipped_shape_admits_then_survives_a_wait_past_the_bound(
    fake_dbos,
    clean_runtime: str,
    monkeypatch,
    restored_schedule_registry,
) -> None:
    """The shape actually shipped, not just its control.

    The enqueue-only test above never touches the guard at all -- with
    guard_body=False the SDK returns the body directly -- so it would pass
    with the promotion fence deleted. What the fixed schedules really do is
    take the admission decision, release it, and then wait. That is the
    sequence under test here.
    """
    import time

    import psycopg

    from okg.substrate.db.connection import IDLE_IN_TRANSACTION_TIMEOUT_ENV
    from okg.substrate.runtime.promotion_fence import assert_deployment_admitted

    monkeypatch.setenv(IDLE_IN_TRANSACTION_TIMEOUT_ENV, "1000")
    deployment = "idle-bound-admit-then-wait"

    assert_deployment_admitted(deployment=deployment, dsn=clean_runtime)
    time.sleep(2.5)

    # The decision is still available afterwards: nothing was left holding a
    # session the server has since terminated.
    assert_deployment_admitted(deployment=deployment, dsn=clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM pg_stat_activity
             WHERE pid <> pg_backend_pid()
               AND datname = current_database()
               AND state = 'idle in transaction'
               AND query ILIKE '%deployment_promotion_fences%'
            """
        )
        assert int(cur.fetchone()[0]) == 0
