"""A deliberately paused publisher reports "paused", not "stale".

``okg runtime pause-schedule`` stops the publisher's cron, so its last
SUCCESS only gets older. The health probe used to call that stale once the
age passed twice the cron period, which made ``okg doctor``, ``okg
deployment ready`` and the Kubernetes readiness probe all report a
deliberately paused deployment as not ready (issue #2554).

These cases run the real probe against the real DBOS system schema, and
pause the schedule through the DBOS client the CLI uses. The pause excuses
only the age: a newest cycle that failed, or a publisher that never
succeeded, still fails.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from okg.substrate.runtime import admin
from okg.substrate.runtime.publisher_health import (
    PAUSED_LONG_SECONDS,
    PAUSED_WARNING_CODES,
    check_embed_workflow_health,
    check_publisher_workflow_health,
)
from tests.pytest.dbos_system_schema import create_dbos_system_schema

pytestmark = pytest.mark.postgres

_SCHEDULE = "okg_publisher_cycle"
#: A five-minute cron: the probe's limit is twice that, 600 s.
_CRON = {"okg_publisher_cycle": "*/5 * * * *"}


def _deployment() -> str:
    return f"paused-{uuid.uuid4().hex[:8]}"


_OWN_QUEUE = object()


def _ms_ago(age_seconds: float) -> int:
    return int(
        (datetime.now(timezone.utc) - timedelta(seconds=age_seconds)).timestamp()
        * 1000
    )


def _add_cycle(
    dsn: str,
    deployment: str,
    *,
    status: str | None,
    age_seconds: float | None = None,
    at_ms: int | None = None,
    name: str = "okg_publisher_cycle",
    queue: object = _OWN_QUEUE,
    workflow_id: str | None = None,
    started: bool = False,
    deadline_ms: int | None = None,
    recovery_attempts: int = 0,
    application_version: str | None = None,
) -> str:
    """Insert one cycle row.

    ``queue=None`` writes the NULL queue DBOS's cancel leaves; ``started``
    stamps ``started_at_epoch_ms`` as a worker does when it dequeues a cycle.
    """
    at = at_ms if at_ms is not None else _ms_ago(age_seconds or 0.0)
    workflow_id = workflow_id or f"cycle-{uuid.uuid4().hex}"
    queue_name = f"okg-{deployment}" if queue is _OWN_QUEUE else queue
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO dbos.workflow_status "
            "(workflow_uuid, name, status, queue_name, created_at, updated_at, "
            "started_at_epoch_ms, workflow_deadline_epoch_ms, recovery_attempts, "
            "application_version) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                workflow_id,
                name,
                status,
                queue_name,
                at,
                at,
                at if started else None,
                deadline_ms,
                recovery_attempts,
                application_version,
            ),
        )
    return workflow_id


def _add_schedule(
    dsn: str, *, last_fired_at: str | None = None, schedule: str = _SCHEDULE
) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO dbos.workflow_schedules "
            "(schedule_id, schedule_name, workflow_name, schedule, status, "
            "context, last_fired_at) "
            "VALUES (%s, %s, %s, '*/5 * * * *', 'ACTIVE', 'null', %s)",
            (uuid.uuid4().hex, schedule, f"{schedule}_enqueue", last_fired_at),
        )


def _pause(dsn: str, schedule: str = _SCHEDULE) -> None:
    """Pause the way `okg runtime pause-schedule` does: through DBOS."""
    client = admin._build_client(dsn=dsn)
    client.pause_schedule(schedule)
    assert str(client.get_schedule(schedule)["status"]).upper() == "PAUSED"


@pytest.fixture
def dbos_dsn(clean_runtime: str) -> str:
    create_dbos_system_schema(clean_runtime)
    return clean_runtime


def _probe(dsn: str, deployment: str):
    return check_publisher_workflow_health(
        deployment=deployment, dsn=dsn, schedule_overrides=_CRON
    )


def test_an_active_schedule_with_an_old_publish_is_still_stale(dbos_dsn: str) -> None:
    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=1300)

    verdict = _probe(dbos_dsn, deployment)

    assert (verdict.ok, verdict.code) == (False, "publisher_workflow_stale")
    assert verdict.schedule_paused is False


def test_a_paused_schedule_with_an_old_publish_reports_paused(dbos_dsn: str) -> None:
    """The nightly shape: last publish 1300 s ago under a 600 s limit."""
    deployment = _deployment()
    _add_schedule(dbos_dsn, last_fired_at="2026-10-12T09:45:00+00:00")
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=1300)
    _pause(dbos_dsn)

    verdict = _probe(dbos_dsn, deployment)

    assert (verdict.ok, verdict.code) == (True, "publisher_workflow_paused")
    assert verdict.code in PAUSED_WARNING_CODES
    assert verdict.schedule_paused is True
    assert verdict.schedule_last_fired_at == "2026-10-12T09:45:00+00:00"
    assert "is paused" in verdict.message
    assert "its last cycle to complete did so" in verdict.message
    assert (
        f"okg runtime resume-schedule okg_publisher_cycle --deployment {deployment}"
        in verdict.message
    )
    payload = verdict.to_dict()
    assert payload["schedule_paused"] is True
    assert payload["seconds_since_last_success"] > 1200


def test_resuming_the_schedule_brings_the_stale_verdict_back(dbos_dsn: str) -> None:
    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=1300)
    _pause(dbos_dsn)
    assert _probe(dbos_dsn, deployment).code == "publisher_workflow_paused"

    admin._build_client(dsn=dbos_dsn).resume_schedule(_SCHEDULE)

    verdict = _probe(dbos_dsn, deployment)
    assert (verdict.ok, verdict.code) == (False, "publisher_workflow_stale")


def test_a_pause_of_more_than_a_day_gets_stronger_wording_and_stays_ok(
    dbos_dsn: str,
) -> None:
    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(
        dbos_dsn, deployment, status="SUCCESS", age_seconds=PAUSED_LONG_SECONDS + 7200
    )
    _pause(dbos_dsn)

    verdict = _probe(dbos_dsn, deployment)

    assert (verdict.ok, verdict.code) == (True, "publisher_workflow_paused_long")
    assert verdict.code in PAUSED_WARNING_CODES
    assert "nothing has been published for 26h" in verdict.message


@pytest.mark.parametrize("failed", ["ERROR", "MAX_RECOVERY_ATTEMPTS_EXCEEDED"])
def test_a_pause_does_not_excuse_a_failed_newest_cycle(
    dbos_dsn: str, failed: str
) -> None:
    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=1300)
    _add_cycle(dbos_dsn, deployment, status=failed, age_seconds=900)
    _pause(dbos_dsn)

    verdict = _probe(dbos_dsn, deployment)

    assert verdict.ok is False
    assert verdict.code == "publisher_workflow_paused_last_cycle_failed"
    assert verdict.code not in PAUSED_WARNING_CODES
    assert f"that was not cancelled ended {failed}" in verdict.message
    assert verdict.schedule_paused is True


@pytest.mark.parametrize("in_flight", ["PENDING", "ENQUEUED", "DELAYED"])
def test_a_pause_does_not_excuse_a_cycle_that_never_finishes(
    dbos_dsn: str, in_flight: str
) -> None:
    """A cycle caught in flight by a dead worker must not hide behind the pause."""
    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=2000)
    _add_cycle(dbos_dsn, deployment, status=in_flight, age_seconds=900)
    _pause(dbos_dsn)

    verdict = _probe(dbos_dsn, deployment)

    assert verdict.ok is False
    assert verdict.code == "publisher_workflow_paused_cycle_not_finished"
    assert f"has been {in_flight} without change" in verdict.message


def test_a_manual_cycle_in_flight_under_a_pause_is_still_paused(
    dbos_dsn: str,
) -> None:
    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=1300)
    _add_cycle(dbos_dsn, deployment, status="PENDING", age_seconds=20)
    _pause(dbos_dsn)

    verdict = _probe(dbos_dsn, deployment)

    assert (verdict.ok, verdict.code) == (True, "publisher_workflow_paused")
    assert "its newest cycle is PENDING" in verdict.message


def test_a_cancelled_newest_cycle_is_named_but_does_not_block(dbos_dsn: str) -> None:
    """okg cancels queued cycles at worker boot; that is not a failed publish."""
    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=1300)
    _add_cycle(dbos_dsn, deployment, status="CANCELLED", age_seconds=900)
    _pause(dbos_dsn)

    verdict = _probe(dbos_dsn, deployment)

    assert (verdict.ok, verdict.code) == (True, "publisher_workflow_paused")
    assert (
        "okg cancelled its newest cycle, and the newest one before that is SUCCESS"
        in verdict.message
    )


def test_the_paused_message_names_the_judged_status_not_the_cancelled_one(
    dbos_dsn: str,
) -> None:
    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=1300)
    _add_cycle(dbos_dsn, deployment, status="PENDING", age_seconds=60)
    _add_cycle(dbos_dsn, deployment, status="CANCELLED", age_seconds=20)
    _pause(dbos_dsn)

    verdict = _probe(dbos_dsn, deployment)

    assert (verdict.ok, verdict.code) == (True, "publisher_workflow_paused")
    assert "the newest one before that is PENDING" in verdict.message
    assert "its newest cycle is CANCELLED" not in verdict.message


def test_a_cycle_dbos_cancelled_after_the_last_success_fails_under_a_pause(
    dbos_dsn: str,
) -> None:
    """A hung cycle hits its timeout: DBOS cancels it and clears its queue.

    The cancel goes through DBOS's own client, which runs the same
    ``cancel_workflows`` its workflow timeout runs.
    """
    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=2000)
    hung = _add_cycle(dbos_dsn, deployment, status="PENDING", age_seconds=1900)
    _pause(dbos_dsn)
    assert _probe(dbos_dsn, deployment).code == (
        "publisher_workflow_paused_cycle_not_finished"
    )

    admin._build_client(dsn=dbos_dsn).cancel_workflow(hung)
    with psycopg.connect(dbos_dsn) as conn:
        row = conn.execute(
            "SELECT status, queue_name FROM dbos.workflow_status "
            "WHERE workflow_uuid = %s",
            (hung,),
        ).fetchone()
    assert row == ("CANCELLED", None)

    verdict = _probe(dbos_dsn, deployment)

    assert (verdict.ok, verdict.code) == (
        False,
        "publisher_workflow_paused_last_cycle_failed",
    )
    assert "on its timeout or by an operator through DBOS" in verdict.message
    assert verdict.code not in PAUSED_WARNING_CODES


def test_a_dbos_cancel_older_than_the_last_success_stays_paused(
    dbos_dsn: str,
) -> None:
    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="CANCELLED", age_seconds=2000, queue=None)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=1300)
    _pause(dbos_dsn)

    verdict = _probe(dbos_dsn, deployment)

    assert (verdict.ok, verdict.code) == (True, "publisher_workflow_paused")


@pytest.mark.parametrize("odd_status", [None, "RETRIES_EXCEEDED"])
def test_a_pause_does_not_excuse_a_null_or_unknown_status(
    dbos_dsn: str, odd_status: str | None
) -> None:
    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=1300)
    _add_cycle(dbos_dsn, deployment, status=odd_status, age_seconds=900)
    _pause(dbos_dsn)

    verdict = _probe(dbos_dsn, deployment)

    assert (verdict.ok, verdict.code) == (
        False,
        "publisher_workflow_paused_last_cycle_failed",
    )
    assert f"has status {odd_status!r}" in verdict.message


def test_a_cancelled_cycle_does_not_shield_a_null_status_before_it(
    dbos_dsn: str,
) -> None:
    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=2000)
    _add_cycle(dbos_dsn, deployment, status=None, age_seconds=1200)
    _add_cycle(dbos_dsn, deployment, status="CANCELLED", age_seconds=900)
    _pause(dbos_dsn)

    verdict = _probe(dbos_dsn, deployment)

    assert (verdict.ok, verdict.code) == (
        False,
        "publisher_workflow_paused_last_cycle_failed",
    )
    assert "has status None" in verdict.message


@pytest.mark.parametrize("winner_first", [True, False])
@pytest.mark.parametrize(
    ("error_id", "success_id", "expected"),
    [
        ("cycle-0", "cycle-f", (True, "publisher_workflow_paused")),
        ("cycle-f", "cycle-0", (False, "publisher_workflow_paused_last_cycle_failed")),
    ],
)
def test_a_tie_on_updated_at_is_broken_by_workflow_id(
    dbos_dsn: str,
    error_id: str,
    success_id: str,
    expected: tuple[bool, str],
    winner_first: bool,
) -> None:
    """Two rows stamped the same millisecond: the higher workflow id is newest.

    Both insertion orders run, so the result does not come from insert
    order, and repeated probes agree.
    """
    deployment = _deployment()
    _add_schedule(dbos_dsn)
    at = _ms_ago(1300)
    first, second = sorted(
        [(error_id, "ERROR"), (success_id, "SUCCESS")], reverse=winner_first
    )
    for workflow_id, status in (first, second):
        _add_cycle(
            dbos_dsn,
            deployment,
            status=status,
            at_ms=at,
            workflow_id=f"{workflow_id}-{deployment}",
        )
    _pause(dbos_dsn)

    verdicts = {(v.ok, v.code) for v in (_probe(dbos_dsn, deployment) for _ in range(5))}

    assert verdicts == {expected}


def test_an_active_schedule_ignores_a_dbos_cancel_as_on_dev(dbos_dsn: str) -> None:
    """Not paused: the NULL-queue row is not read; the verdict is dev's."""
    recent = _deployment()
    old = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, recent, status="SUCCESS", age_seconds=30)
    _add_cycle(dbos_dsn, old, status="SUCCESS", age_seconds=1300)
    _add_cycle(dbos_dsn, recent, status="CANCELLED", age_seconds=10, queue=None)

    healthy = _probe(dbos_dsn, recent)
    stale = _probe(dbos_dsn, old)

    assert (healthy.ok, healthy.code) == (True, "publisher_workflow_healthy")
    assert (stale.ok, stale.code) == (False, "publisher_workflow_stale")
    assert stale.schedule_paused is False


def test_a_cancelled_cycle_does_not_shield_the_failed_one_before_it(
    dbos_dsn: str,
) -> None:
    """A cycle fails, a later one is queued, a worker boot cancels the queued one."""
    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=2000)
    _add_cycle(dbos_dsn, deployment, status="ERROR", age_seconds=1200)
    _add_cycle(dbos_dsn, deployment, status="CANCELLED", age_seconds=900)
    _pause(dbos_dsn)

    verdict = _probe(dbos_dsn, deployment)

    assert verdict.ok is False
    assert verdict.code == "publisher_workflow_paused_last_cycle_failed"
    assert "that was not cancelled ended ERROR" in verdict.message


def test_a_cancelled_cycle_does_not_shield_a_stuck_one_before_it(
    dbos_dsn: str,
) -> None:
    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=2000)
    _add_cycle(dbos_dsn, deployment, status="PENDING", age_seconds=1200)
    _add_cycle(dbos_dsn, deployment, status="CANCELLED", age_seconds=900)
    _pause(dbos_dsn)

    verdict = _probe(dbos_dsn, deployment)

    assert (verdict.ok, verdict.code) == (
        False,
        "publisher_workflow_paused_cycle_not_finished",
    )


def test_a_pause_does_not_excuse_a_publisher_that_never_succeeded(
    dbos_dsn: str,
) -> None:
    never_ran = _deployment()
    only_failed = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, only_failed, status="ERROR", age_seconds=900)
    _pause(dbos_dsn)

    first = _probe(dbos_dsn, never_ran)
    assert (first.ok, first.code) == (False, "publisher_workflow_never_ran")
    assert "its schedule is paused" in first.message

    second = _probe(dbos_dsn, only_failed)
    assert (second.ok, second.code) == (False, "publisher_workflow_no_success_yet")
    assert "its schedule is paused" in second.message


def test_a_paused_schedule_with_a_recent_publish_is_healthy(dbos_dsn: str) -> None:
    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=30)
    _pause(dbos_dsn)

    verdict = _probe(dbos_dsn, deployment)

    assert (verdict.ok, verdict.code) == (True, "publisher_workflow_healthy")
    assert verdict.schedule_paused is True


def test_no_schedule_row_means_no_pause(dbos_dsn: str) -> None:
    deployment = _deployment()
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=1300)

    verdict = _probe(dbos_dsn, deployment)

    assert (verdict.ok, verdict.code) == (False, "publisher_workflow_stale")
    assert verdict.schedule_paused is False


def test_no_schedules_table_means_no_pause(dbos_dsn: str) -> None:
    deployment = _deployment()
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=1300)
    with psycopg.connect(dbos_dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE dbos.workflow_schedules")

    verdict = _probe(dbos_dsn, deployment)

    assert (verdict.ok, verdict.code) == (False, "publisher_workflow_stale")
    assert verdict.schedule_paused is False


def test_only_the_publisher_probe_reads_the_pause(dbos_dsn: str) -> None:
    """The embed probe has a schedule too; pausing it changes nothing."""
    from okg.substrate.runtime.embed_workflow import EMBED_SCHEDULE_ID

    deployment = _deployment()
    _add_schedule(dbos_dsn, schedule=EMBED_SCHEDULE_ID)
    _add_cycle(
        dbos_dsn,
        deployment,
        status="SUCCESS",
        age_seconds=7 * 24 * 3600,
        name="okg_embed_batch",
    )
    _pause(dbos_dsn, EMBED_SCHEDULE_ID)

    verdict = check_embed_workflow_health(deployment=deployment, dsn=dbos_dsn)

    assert verdict.code == "embed_workflow_stale"
    assert verdict.schedule_paused is False
    assert "paused" not in verdict.code


def _row(dsn: str, workflow_id: str) -> tuple:
    with psycopg.connect(dsn) as conn:
        return conn.execute(
            "SELECT status, queue_name, started_at_epoch_ms IS NOT NULL "
            "FROM dbos.workflow_status WHERE workflow_uuid = %s",
            (workflow_id,),
        ).fetchone()


def test_okgs_deadline_sweep_of_a_started_cycle_fails_under_a_pause(
    dbos_dsn: str,
) -> None:
    """A worker restart sweeps a hung cycle past its deadline; the queue stays."""
    from okg.substrate.runtime.workflow_cancel import (
        cleanup_deadline_expired_pending_workflows,
    )

    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=2500)
    hung = _add_cycle(
        dbos_dsn,
        deployment,
        status="PENDING",
        age_seconds=2000,
        started=True,
        deadline_ms=_ms_ago(200),
    )
    _pause(dbos_dsn)

    swept = cleanup_deadline_expired_pending_workflows(
        dsn=dbos_dsn, deployment=deployment
    )

    assert swept["cancelled_count"] == 1
    assert _row(dbos_dsn, hung) == ("CANCELLED", f"okg-{deployment}", True)
    verdict = _probe(dbos_dsn, deployment)
    assert (verdict.ok, verdict.code) == (
        False,
        "publisher_workflow_paused_last_cycle_failed",
    )
    assert "after it had started (deadline sweep or operator cancel" in verdict.message
    assert "row last updated at" in verdict.message


def test_operator_cancel_of_a_started_cycle_fails_under_a_pause(
    dbos_dsn: str,
) -> None:
    """`okg runtime cancel-workflow` on a started cycle: the same verdict."""
    from okg.substrate.runtime.workflow_cancel import cancel_deployment_workflow

    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=1300)
    running = _add_cycle(
        dbos_dsn, deployment, status="PENDING", age_seconds=60, started=True
    )
    _pause(dbos_dsn)
    assert _probe(dbos_dsn, deployment).code == "publisher_workflow_paused"

    cancel_deployment_workflow(
        dsn=dbos_dsn, deployment=deployment, workflow_id=running
    )

    assert _row(dbos_dsn, running) == ("CANCELLED", f"okg-{deployment}", True)
    verdict = _probe(dbos_dsn, deployment)
    assert (verdict.ok, verdict.code) == (
        False,
        "publisher_workflow_paused_last_cycle_failed",
    )
    assert "after it had started (deadline sweep or operator cancel" in verdict.message
    assert "row last updated at" in verdict.message


def test_a_queued_cycle_cancelled_before_it_started_is_skipped(
    dbos_dsn: str,
) -> None:
    """okg withdrawing a queued cycle (as `okg run --once` does) is not a failure."""
    from okg.substrate.runtime.workflow_cancel import cancel_deployment_workflow

    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=1300)
    queued = _add_cycle(dbos_dsn, deployment, status="ENQUEUED", age_seconds=60)
    _pause(dbos_dsn)

    cancel_deployment_workflow(
        dsn=dbos_dsn,
        deployment=deployment,
        workflow_id=queued,
        expected_status="ENQUEUED",
    )

    assert _row(dbos_dsn, queued) == ("CANCELLED", f"okg-{deployment}", False)
    verdict = _probe(dbos_dsn, deployment)
    assert (verdict.ok, verdict.code) == (True, "publisher_workflow_paused")
    assert "okg cancelled its newest cycle" in verdict.message


def test_an_upgrade_refuses_a_started_cycle_and_its_cancel_fails_under_a_pause(
    dbos_dsn: str,
) -> None:
    """The version cutover refuses a started cycle; an operator then cancels it.

    Under a pause no fresh cycle follows, so the deployment stays not ready
    until a cycle succeeds. That fails closed on purpose.
    """
    from okg.substrate.runtime.services import (
        RuntimeCutoverBlocked,
        supersede_incompatible_source_sync_workflows,
    )
    from okg.substrate.runtime.workflow_cancel import cancel_deployment_workflow

    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=1300)
    running = _add_cycle(
        dbos_dsn,
        deployment,
        status="PENDING",
        age_seconds=1000,
        started=True,
        recovery_attempts=1,
        application_version="old-version",
    )
    _pause(dbos_dsn)

    with pytest.raises(RuntimeCutoverBlocked) as refusal:
        supersede_incompatible_source_sync_workflows(
            dsn=dbos_dsn, deployment=deployment, live_version="new-version"
        )
    assert [b["workflow_id"] for b in refusal.value.blockers] == [running]
    assert _row(dbos_dsn, running) == ("PENDING", f"okg-{deployment}", True)

    cancel_deployment_workflow(
        dsn=dbos_dsn, deployment=deployment, workflow_id=running
    )

    blocked = _probe(dbos_dsn, deployment)
    assert (blocked.ok, blocked.code) == (
        False,
        "publisher_workflow_paused_last_cycle_failed",
    )
    assert "after it had started (deadline sweep or operator cancel" in blocked.message

    # A manual cycle succeeds after the cancel: ready again.
    with psycopg.connect(dbos_dsn, autocommit=True) as conn:
        cancelled_at = conn.execute(
            "SELECT updated_at FROM dbos.workflow_status WHERE workflow_uuid = %s",
            (running,),
        ).fetchone()[0]
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", at_ms=cancelled_at + 1)
    assert _probe(dbos_dsn, deployment).code == "publisher_workflow_healthy"


@pytest.mark.parametrize(
    ("recovery_attempts", "expected", "expected_text"),
    [
        (
            1,
            (False, "publisher_workflow_paused_last_cycle_failed"),
            "that DBOS recovery had put back on the queue was cancelled by okg (stale sweep or version cutover; row last updated at",
        ),
        (
            0,
            (True, "publisher_workflow_paused"),
            "okg cancelled its newest cycle, and the newest one before that is SUCCESS",
        ),
    ],
)
def test_a_recovered_cycle_swept_as_stale_enqueued_fails_under_a_pause(
    dbos_dsn: str,
    recovery_attempts: int,
    expected: tuple[bool, str],
    expected_text: str,
) -> None:
    """DBOS recovery puts a started cycle back as ENQUEUED and clears its start time.

    ``recovery_attempts`` still says a worker began it. okg's stale-ENQUEUED
    sweep then cancels it and keeps the queue. A never-started row (0) is a
    harmless okg cancel and is skipped.
    """
    from okg.substrate.runtime.services import cleanup_stale_enqueued_workflows

    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=1300)
    recovered = _add_cycle(
        dbos_dsn,
        deployment,
        status="ENQUEUED",
        age_seconds=400,
        recovery_attempts=recovery_attempts,
    )
    _pause(dbos_dsn)

    swept = cleanup_stale_enqueued_workflows(dsn=dbos_dsn, deployment=deployment)

    assert swept["cancelled_count"] == 1
    assert _row(dbos_dsn, recovered) == ("CANCELLED", f"okg-{deployment}", False)
    verdict = _probe(dbos_dsn, deployment)
    assert (verdict.ok, verdict.code) == expected
    assert expected_text in verdict.message


def test_max_recovery_attempts_exceeded_without_a_queue_fails_under_a_pause(
    dbos_dsn: str,
) -> None:
    """DBOS 2.26 clears the queue (and the start time) when it marks this status."""
    deployment = _deployment()
    _add_schedule(dbos_dsn)
    _add_cycle(dbos_dsn, deployment, status="SUCCESS", age_seconds=1300)
    _add_cycle(
        dbos_dsn,
        deployment,
        status="MAX_RECOVERY_ATTEMPTS_EXCEEDED",
        age_seconds=900,
        queue=None,
    )
    _pause(dbos_dsn)

    verdict = _probe(dbos_dsn, deployment)

    assert (verdict.ok, verdict.code) == (
        False,
        "publisher_workflow_paused_last_cycle_failed",
    )
    assert "ended MAX_RECOVERY_ATTEMPTS_EXCEEDED" in verdict.message
