"""Workflow-liveness probes for `okg doctor`.

Extracted from ``runtime/publisher_health.py`` by
``extract-publisher-health-workflow-liveness`` (2026-05) per the
cleanup audit. 5 cron-driven workflow probes
(`check_publisher_workflow_health`,
`check_source_sync_workflow_health`,
`check_embed_workflow_health`,
`check_escalation_sweep_health`,
`check_lens_putback_sweep_health`) plus their shared generic
`_check_workflow_health` factory + `PublisherHealthVerdict`
dataclass + `_parse_cron_interval_seconds` helper.

Each public probe answers the same operator question: "has the
workflow actually completed a SUCCESS-status row recently?"
``_check_workflow_health`` reads ``dbos.workflow_status`` for the
most-recent row for ``(workflow_name, queue_name)`` and flags
two conditions as failures: never-ran (no SUCCESS row yet) and
stale (most-recent SUCCESS older than N x cron interval). Both
``never_ran_is_fatal`` and ``stale_is_fatal`` are per-call
toggles for the deployments where "no run yet" or "no recent
run" are informational rather than failures (e.g. operator-
driven source-sync, manifest-disabled embed).

``runtime/publisher_health.py`` re-exports these names at
module-import time so doctor.py + scheduled.py + the test suite
continue to import from `publisher_health` without changes.

The publisher probe also knows about a deliberate pause. ``okg runtime
pause-schedule`` sets ``status = 'PAUSED'`` on the schedule's row in
``dbos.workflow_schedules``, and from then on no cycle runs, so the last
SUCCESS only gets older. That is the operator's intent, not a stalled
worker: a paused publisher whose last cycle succeeded reports
``publisher_workflow_paused`` (``ok``, a warning), or
``publisher_workflow_paused_long`` once nothing has been published for
:data:`PAUSED_LONG_SECONDS`. The pause excuses only the age. A paused
publisher whose newest cycle failed, has an unknown status, is stuck in
flight, or was cancelled after it started (by DBOS's timeout, okg's
deadline sweep or an operator) after the last SUCCESS,
or that never succeeded, is still a failure. A cycle
that completes with a blocked publish has status SUCCESS here; the
publish-block checks report that, with or without a pause. The row records
neither who paused the schedule nor when. A DBOS-cancelled row has no queue,
so that read assumes one ``dbos`` schema serves one deployment.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from okg.substrate.db.connection import connect as db_connect


__all__ = [
    "PAUSED_LONG_SECONDS",
    "PAUSED_WARNING_CODES",
    "PublisherHealthVerdict",
    "check_publisher_workflow_health",
    "check_source_sync_workflow_health",
    "check_embed_workflow_health",
    "check_escalation_sweep_health",
    "check_lens_putback_sweep_health",
]


# Mirror of the schedule's default cron. Used when the manifest
# doesn't override it. Format is the substrate's 6-field cron; we
# parse only what we need (the seconds-or-minutes interval).
_DEFAULT_CRON_SECONDS = 30

#: A paused publisher that has published nothing for this long gets the
#: stronger ``publisher_workflow_paused_long`` wording. It stays ``ok``: a
#: pause alone never makes a deployment not ready.
PAUSED_LONG_SECONDS = 24 * 60 * 60.0

#: Verdict codes that are ``ok`` but that ``okg doctor`` lists as warnings.
PAUSED_WARNING_CODES = frozenset(
    {"publisher_workflow_paused", "publisher_workflow_paused_long"}
)

#: Newest-cycle statuses a pause does not excuse: the cycle ran and failed.
#: Any other status a pause does not excuse (see
#: ``_PAUSE_EXCUSABLE_STATUSES``) fails the same way; these two get the
#: "ended" wording. A ``CANCELLED`` row that never started is okg clearing
#: a queued cycle (a worker boot, ``okg run --once`` withdrawing its own).
#: Under a pause no later cycle replaces that row, so counting it would turn
#: a restarted, paused deployment not ready for good; the warning names it,
#: and the newest cycle before it is judged. A cycle cancelled after it
#: started, by DBOS or by okg, is judged as a failure.
_FAILED_CYCLE_STATUSES = frozenset({"ERROR", "MAX_RECOVERY_ATTEMPTS_EXCEEDED"})

#: A cycle in flight. A manual trigger still runs under a paused schedule, so
#: one of these is expected for a while; one that has not moved for longer
#: than the staleness limit is stuck, and the pause does not excuse that.
_IN_FLIGHT_CYCLE_STATUSES = frozenset({"PENDING", "ENQUEUED", "DELAYED"})

#: The only statuses of the judged cycle a pause can excuse (a recent
#: in-flight one only while it is recent). Anything else, including a NULL
#: or unknown status, is not excused.
_PAUSE_EXCUSABLE_STATUSES = frozenset({"SUCCESS"}) | _IN_FLIGHT_CYCLE_STATUSES


@dataclass(frozen=True)
class PublisherHealthVerdict:
    """Outcome of the publisher-health probe."""

    deployment: str
    ok: bool
    code: str
    message: str
    last_success_at: datetime | None
    seconds_since_last_success: float | None
    threshold_seconds: float
    #: True when the workflow's schedule is persisted as paused. Only the
    #: publisher probe reads the schedule, so it is False everywhere else.
    schedule_paused: bool = False
    #: The schedule's last cron fire, as DBOS stores it. The pause began at
    #: or after this moment; DBOS does not record the pause itself.
    schedule_last_fired_at: str | None = None

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "code": self.code,
            "message": self.message,
            "deployment": self.deployment,
            "last_success_at": (
                self.last_success_at.isoformat()
                if self.last_success_at else None
            ),
            "seconds_since_last_success": (
                self.seconds_since_last_success
            ),
            "threshold_seconds": self.threshold_seconds,
            "schedule_paused": self.schedule_paused,
            "schedule_last_fired_at": self.schedule_last_fired_at,
        }


def _parse_cron_interval_seconds(
    cron: str,
    default_seconds: int = _DEFAULT_CRON_SECONDS,
) -> int:
    """Best-effort parse of the cron interval to seconds.

    Recognizes the common forms the substrate uses:

      * ``"*/30 * * * * *"`` (6-field, seconds) — 30s.
      * ``"*/5 * * * *"`` (5-field, minutes) — 300s.
      * ``"*/1 * * * *"`` — 60s.
      * ``"0 3 * * *"`` (5-field fixed-time daily) — 86400s. The
        substrate's hygiene sweeps fire once per day at a fixed hour;
        without this the daily cron fell through to ``default_seconds``,
        making any daily-cadence liveness threshold absurdly tight.

    Falls back to ``default_seconds`` when the cron doesn't match a
    recognized pattern.
    """
    parts = cron.strip().split()
    try:
        if len(parts) == 6 and parts[0].startswith("*/"):
            return int(parts[0][2:])
        if len(parts) == 5 and parts[0].startswith("*/"):
            return int(parts[0][2:]) * 60
        # Fixed-time daily: concrete minute + hour, wildcard
        # day/month/weekday (e.g. "0 3 * * *", "30 3 * * *") fires once
        # per day. (A wildcard hour like "0 * * * *" is hourly, not
        # daily, so it must NOT match here.)
        if (
            len(parts) == 5
            and parts[0].isdigit()
            and parts[1].isdigit()
            and parts[2:] == ["*", "*", "*"]
        ):
            return 86400
    except (ValueError, IndexError):
        # Non-integer cron fields use the default interval.
        pass
    return default_seconds


def check_publisher_workflow_health(
    *,
    deployment: str,
    dsn: str,
    schedule_overrides: dict[str, str] | None = None,
    threshold_multiplier: float = 2.0,
) -> PublisherHealthVerdict:
    """Publisher-workflow liveness check."""
    from okg.substrate.runtime.publisher_schedule import (
        PUBLISHER_DEFAULT_CRON, PUBLISHER_SCHEDULE_ID,
    )
    return _check_workflow_health(
        deployment=deployment,
        dsn=dsn,
        workflow_name="okg_publisher_cycle",
        schedule_id=PUBLISHER_SCHEDULE_ID,
        default_cron=PUBLISHER_DEFAULT_CRON,
        schedule_overrides=schedule_overrides,
        threshold_multiplier=threshold_multiplier,
        result_prefix="publisher_workflow",
        pause_aware=True,
    )


def check_source_sync_workflow_health(
    *,
    deployment: str,
    dsn: str,
    schedule_overrides: dict[str, str] | None = None,
    threshold_multiplier: float = 4.0,
) -> PublisherHealthVerdict:
    """Source-sync workflow liveness check.

    No scheduled enqueue — source-sync workflows are operator-driven
    (`okg ingest`) rather than cron-driven. The "recent" threshold is
    therefore informational only: a stale source-sync just means no
    operator has run `okg ingest` recently, not that something is
    broken. Both ``never_ran`` and ``stale`` return non-fatal verdicts;
    only an ERROR-status last entry would constitute a failure (and
    that's surfaced via the regular doctor error-report path).
    """
    return _check_workflow_health(
        deployment=deployment,
        dsn=dsn,
        workflow_name="okg_source_sync",
        schedule_id=None,
        default_cron=None,
        schedule_overrides=schedule_overrides,
        threshold_multiplier=threshold_multiplier,
        result_prefix="source_sync_workflow",
        never_ran_is_fatal=False,
        stale_is_fatal=False,
    )


def check_embed_workflow_health(
    *,
    deployment: str,
    dsn: str,
    schedule_overrides: dict[str, str] | None = None,
    threshold_multiplier: float = 2.0,
    expected_to_run: bool = True,
) -> PublisherHealthVerdict:
    """Embed batch workflow liveness check.

    Returns OK (never_ran_is_fatal=False) when no embed workflow has
    run yet — that's normal for deployments with
    ``embeddings.enabled: false`` because the workflow is not registered and
    therefore cannot land a SUCCESS row that updates the timestamp.
    """
    from okg.substrate.runtime.embed_workflow import (
        EMBED_DEFAULT_CRON, EMBED_SCHEDULE_ID,
    )
    return _check_workflow_health(
        deployment=deployment,
        dsn=dsn,
        workflow_name="okg_embed_batch",
        schedule_id=EMBED_SCHEDULE_ID,
        default_cron=EMBED_DEFAULT_CRON,
        schedule_overrides=schedule_overrides,
        threshold_multiplier=threshold_multiplier,
        result_prefix="embed_workflow",
        never_ran_is_fatal=False,
        expected_to_run=expected_to_run,
    )


def check_escalation_sweep_health(
    *,
    deployment: str,
    dsn: str,
    schedule_overrides: dict[str, str] | None = None,
    threshold_multiplier: float = 4.0,
    expected_to_run: bool = True,
) -> PublisherHealthVerdict:
    """Escalation timeout-sweep workflow liveness check.

    Returns OK on `never_ran` because deployments without escalations
    never see a successful sweep (the cron fires but the body's
    SELECT returns zero rows, and DBOS persists the success anyway —
    in practice this will fire fast). ``stale_is_fatal=True``: a
    cron-driven workflow not landing SUCCESS rows means the worker
    stopped processing.
    """
    from okg.substrate.runtime.escalation_sweep_schedule import (
        ESCALATION_SWEEP_DEFAULT_CRON, ESCALATION_SWEEP_SCHEDULE_ID,
    )
    return _check_workflow_health(
        deployment=deployment,
        dsn=dsn,
        workflow_name="okg_escalation_timeout_sweep",
        schedule_id=ESCALATION_SWEEP_SCHEDULE_ID,
        default_cron=ESCALATION_SWEEP_DEFAULT_CRON,
        schedule_overrides=schedule_overrides,
        threshold_multiplier=threshold_multiplier,
        result_prefix="escalation_sweep",
        never_ran_is_fatal=False,
        expected_to_run=expected_to_run,
    )


def check_lens_putback_sweep_health(
    *,
    deployment: str,
    dsn: str,
    schedule_overrides: dict[str, str] | None = None,
    threshold_multiplier: float = 4.0,
    expected_to_run: bool = True,
) -> PublisherHealthVerdict:
    """Lens-putback sweep workflow liveness check.

    Returns OK on `never_ran` because deployments without lenses or
    without any deferred-commit usage simply never write rows for the
    sweeper to find — the cron fires but the workflow body's first
    INSERT/UPDATE never happens, so `dbos.workflow_status` may not
    show a SUCCESS row for it. ``stale_is_fatal=True`` is preserved
    because the cron firing IS visible (the schedule's enqueue
    workflow always runs and lands a SUCCESS row) — stale means the
    worker stopped processing.
    """
    from okg.substrate.runtime.lens_putback_schedule import (
        LENS_PUTBACK_DEFAULT_CRON, LENS_PUTBACK_SCHEDULE_ID,
    )
    return _check_workflow_health(
        deployment=deployment,
        dsn=dsn,
        workflow_name="okg_lens_putback_sweep",
        schedule_id=LENS_PUTBACK_SCHEDULE_ID,
        default_cron=LENS_PUTBACK_DEFAULT_CRON,
        schedule_overrides=schedule_overrides,
        threshold_multiplier=threshold_multiplier,
        result_prefix="lens_putback_sweep",
        never_ran_is_fatal=False,
        expected_to_run=expected_to_run,
    )


def _check_workflow_health(
    *,
    deployment: str,
    dsn: str,
    workflow_name: str,
    schedule_id: str | None,
    default_cron: str | None,
    schedule_overrides: dict[str, str] | None,
    threshold_multiplier: float,
    result_prefix: str,
    never_ran_is_fatal: bool = True,
    stale_is_fatal: bool = True,
    expected_to_run: bool = True,
    pause_aware: bool = False,
) -> PublisherHealthVerdict:
    """Generic workflow-recent-success probe.

    Parameters identify which workflow + schedule to look up; the
    result-prefix names the verdict's code (``<prefix>_healthy`` /
    ``<prefix>_stale`` / etc.). ``never_ran_is_fatal=False`` lets a
    deployment that hasn't yet exercised the workflow (e.g. embed on
    a deployment with embedding disabled) pass cleanly.
    ``stale_is_fatal=False`` is for operator-driven workflows like
    source-sync where "no recent run" is informational, not a failure.
    ``pause_aware=True`` (the publisher) also reads the schedule's row: a
    stale last SUCCESS under a persisted pause is reported as paused, not
    stale, unless the newest cycle itself failed.
    """
    overrides = schedule_overrides or {}
    if schedule_id and default_cron:
        cron = overrides.get(schedule_id, default_cron)
        interval_seconds = _parse_cron_interval_seconds(cron)
    else:
        interval_seconds = _DEFAULT_CRON_SECONDS
    threshold_seconds = interval_seconds * threshold_multiplier

    if not expected_to_run:
        return PublisherHealthVerdict(
            deployment=deployment,
            ok=True,
            code=f"{result_prefix}_not_expected",
            message=(
                f"{workflow_name} is not expected to run for "
                f"{deployment!r}; its feature gate is currently closed"
            ),
            last_success_at=None,
            seconds_since_last_success=None,
            threshold_seconds=threshold_seconds,
        )

    with db_connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT workflow_uuid, status,
                       updated_at / 1000.0 AS updated_at_secs,
                       queue_name
                  FROM dbos.workflow_status
                 WHERE name = %s
                   AND queue_name = %s
                 ORDER BY updated_at DESC, workflow_uuid DESC
                 LIMIT 1
                """,
                (workflow_name, f"okg-{deployment}"),
            )
            row = cur.fetchone()
            paused, last_fired_at = (
                _schedule_pause_state(cur, schedule_id)
                if pause_aware and schedule_id
                else (False, None)
            )

    now = datetime.now(timezone.utc)
    paused_note = "; its schedule is paused" if paused else ""

    if row is None:
        return PublisherHealthVerdict(
            deployment=deployment,
            ok=not never_ran_is_fatal,
            code=f"{result_prefix}_never_ran",
            message=(
                f"no {workflow_name} rows for deployment "
                f"{deployment!r}; the scheduled enqueue has not "
                f"fired or the worker is not running{paused_note}"
            ),
            last_success_at=None,
            seconds_since_last_success=None,
            threshold_seconds=threshold_seconds,
            schedule_paused=paused,
            schedule_last_fired_at=last_fired_at,
        )

    _wf_id, status, updated_at_secs, _queue = row

    if status != "SUCCESS":
        with db_connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT updated_at / 1000.0
                      FROM dbos.workflow_status
                     WHERE name = %s
                       AND queue_name = %s
                       AND status = 'SUCCESS'
                     ORDER BY updated_at DESC, workflow_uuid DESC
                     LIMIT 1
                    """,
                    (workflow_name, f"okg-{deployment}"),
                )
                success_row = cur.fetchone()
        if success_row is None:
            return PublisherHealthVerdict(
                deployment=deployment,
                ok=not never_ran_is_fatal,
                code=f"{result_prefix}_no_success_yet",
                message=(
                    f"{workflow_name} exists for "
                    f"{deployment!r} but has not produced a "
                    f"SUCCESS-status row yet (current top status: "
                    f"{status!r}){paused_note}"
                ),
                last_success_at=None,
                seconds_since_last_success=None,
                threshold_seconds=threshold_seconds,
                schedule_paused=paused,
                schedule_last_fired_at=last_fired_at,
            )
        last_success_secs = float(success_row[0])
    else:
        last_success_secs = float(updated_at_secs)

    last_success_at = datetime.fromtimestamp(
        last_success_secs, tz=timezone.utc,
    )
    seconds_since = (now - last_success_at).total_seconds()
    if seconds_since > threshold_seconds and paused:
        # The schedule is paused on purpose, so the age alone says nothing.
        # It excuses nothing else: a cycle that failed is still a failure,
        # and the pause must not hide it.
        def _paused_failure(code_suffix: str, message: str) -> PublisherHealthVerdict:
            return PublisherHealthVerdict(
                deployment=deployment,
                ok=False,
                code=f"{result_prefix}_{code_suffix}",
                message=message,
                last_success_at=last_success_at,
                seconds_since_last_success=seconds_since,
                threshold_seconds=threshold_seconds,
                schedule_paused=True,
                schedule_last_fired_at=last_fired_at,
            )

        # A cycle that started and was then taken away from its worker is a
        # failed cycle, whoever ended it. Two shapes of row record that:
        #
        # * No queue. DBOS's own cancel (``cancel_workflows``, which its
        #   workflow timeout calls, as does a cancel through DBOS's client)
        #   sets CANCELLED and ``queue_name = NULL``; DBOS 2.26 also clears
        #   the queue when it marks a row MAX_RECOVERY_ATTEMPTS_EXCEEDED.
        #   Such a row has left every query above. It carries no deployment
        #   name, so this read assumes one ``dbos`` schema serves one
        #   deployment; if several share one, another deployment's row
        #   counts here too.
        # * Queue kept, and the cycle had started: ``started_at_epoch_ms``
        #   set, or ``recovery_attempts > 0``. DBOS writes 0 on enqueue and
        #   adds 1 each time a worker begins running the cycle; its recovery
        #   puts a started cycle back as ENQUEUED with ``started_at_epoch_ms``
        #   cleared but leaves the count, and okg's cancels leave both alone.
        #   okg's deadline sweep (``cleanup_deadline_expired_pending_workflows``,
        #   at worker boot and in maintenance), ``okg runtime cancel-workflow``
        #   on a started cycle, and the stale-ENQUEUED and stale-PENDING sweeps
        #   on a recovered one write this. A version cutover refuses a started
        #   cycle that is still PENDING rather than cancelling it; that cycle
        #   is then cleared by the deadline sweep or an operator. A recovered
        #   cycle still waiting in the queue (ENQUEUED) is cancelled by the
        #   cutover, which also fails readiness while paused. Some of these
        #   cancels leave ``updated_at`` alone, so the message gives it as
        #   the time the row was last updated. Under a pause no fresh cycle
        #   follows, so the deployment stays not ready until the schedule
        #   resumes or a manual cycle succeeds; that fails closed on purpose.
        #
        # Only such a row newer than the last SUCCESS counts. A queued cycle
        # okg cancelled before any worker began it (a worker boot clearing
        # the queue) has neither marker and is handled below.
        with db_connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, queue_name IS NULL,
                           updated_at / 1000.0,
                           started_at_epoch_ms IS NOT NULL
                      FROM dbos.workflow_status
                     WHERE name = %s
                       AND (
                            (queue_name IS NULL
                             AND status IN (
                                 'CANCELLED', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED'
                             ))
                         OR (queue_name = %s
                             AND status = 'CANCELLED'
                             AND (started_at_epoch_ms IS NOT NULL
                                  OR COALESCE(recovery_attempts, 0) > 0))
                       )
                     ORDER BY updated_at DESC, workflow_uuid DESC
                     LIMIT 1
                    """,
                    (workflow_name, f"okg-{deployment}"),
                )
                ended_early = cur.fetchone()
        if ended_early is not None and float(ended_early[2]) > last_success_secs:
            ended_status, no_queue, ended_secs, has_start_time = ended_early
            ended_at = datetime.fromtimestamp(
                float(ended_secs), tz=timezone.utc,
            ).isoformat(timespec="seconds")
            last_updated = f"row last updated at {ended_at}"
            if ended_status == "MAX_RECOVERY_ATTEMPTS_EXCEEDED":
                how = (
                    f"ended MAX_RECOVERY_ATTEMPTS_EXCEEDED (DBOS removed it "
                    f"from the queue; {last_updated})"
                )
            elif no_queue:
                how = (
                    f"was CANCELLED by DBOS, on its timeout or by an operator "
                    f"through DBOS ({last_updated})"
                )
            elif has_start_time:
                how = (
                    f"was CANCELLED after it had started (deadline sweep or "
                    f"operator cancel; {last_updated})"
                )
            else:
                how = (
                    f"that DBOS recovery had put back on the queue was "
                    f"cancelled by okg (stale sweep or version cutover; "
                    f"{last_updated})"
                )
            return _paused_failure(
                "paused_last_cycle_failed",
                f"{workflow_name} is paused for {deployment!r}, and a cycle "
                f"{how}, later than the last SUCCESS ({seconds_since:.0f}s ago); "
                f"the pause does not excuse it. Resume the schedule or run a "
                f"cycle that succeeds",
            )

        # A cycle okg cancelled before it started is not judged itself, and
        # it must not shield the cycle before it either: a worker boot that
        # cancels a queued cycle would otherwise bury the failed one that ran
        # just before. (A started one was handled above, when newer than the
        # last SUCCESS.)
        # Judge the newest cycle that was not cancelled. ``IS DISTINCT
        # FROM`` keeps a row with a NULL status in view.
        judged_status, judged_updated_at = status, updated_at_secs
        if status == "CANCELLED":
            with db_connect(dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT status, updated_at / 1000.0
                          FROM dbos.workflow_status
                         WHERE name = %s
                           AND queue_name = %s
                           AND status IS DISTINCT FROM 'CANCELLED'
                         ORDER BY updated_at DESC, workflow_uuid DESC
                         LIMIT 1
                        """,
                        (workflow_name, f"okg-{deployment}"),
                    )
                    judged = cur.fetchone()
            if judged is not None:
                judged_status, judged_updated_at = judged
        if judged_status not in _PAUSE_EXCUSABLE_STATUSES:
            judged_at = datetime.fromtimestamp(
                float(judged_updated_at), tz=timezone.utc,
            ).isoformat(timespec="seconds")
            return _paused_failure(
                "paused_last_cycle_failed",
                (
                    f"{workflow_name} is paused for {deployment!r}, and "
                    f"its newest cycle that was not cancelled ended "
                    f"{judged_status} at {judged_at}"
                    if judged_status in _FAILED_CYCLE_STATUSES
                    else f"{workflow_name} is paused for {deployment!r}, and "
                    f"its newest cycle that was not cancelled has status "
                    f"{judged_status!r} at {judged_at}, which is neither "
                    f"complete nor in flight"
                )
                + f"; the last SUCCESS was {seconds_since:.0f}s ago. Fix "
                f"the failed publish; the pause does not excuse it",
            )
        newest_age = (
            now
            - datetime.fromtimestamp(float(judged_updated_at), tz=timezone.utc)
        ).total_seconds()
        if (
            judged_status in _IN_FLIGHT_CYCLE_STATUSES
            and newest_age > threshold_seconds
        ):
            return _paused_failure(
                "paused_cycle_not_finished",
                f"{workflow_name} is paused for {deployment!r}, and "
                f"its newest cycle has been {judged_status} without change "
                f"for {newest_age:.0f}s (limit: "
                f"{threshold_seconds:.0f}s); the last cycle to "
                f"complete did so {seconds_since:.0f}s ago. The pause "
                f"does not excuse a cycle that never finishes",
            )
        # Name the status that was judged. When okg cancelled the newest
        # row, say so as well, so the warning is not silent about it.
        if status == "CANCELLED":
            newest_note = (
                f"; okg cancelled its newest cycle, and the newest one "
                f"before that is {judged_status}"
            )
        elif judged_status == "SUCCESS":
            newest_note = ""
        else:
            newest_note = f"; its newest cycle is {judged_status}"
        long_pause = seconds_since > PAUSED_LONG_SECONDS
        resume = (
            f"resume with `okg runtime resume-schedule {schedule_id} "
            f"--deployment {deployment}`"
        )
        return PublisherHealthVerdict(
            deployment=deployment,
            ok=True,
            code=(
                f"{result_prefix}_paused_long"
                if long_pause
                else f"{result_prefix}_paused"
            ),
            message=(
                (
                    f"{workflow_name} is paused for {deployment!r} and "
                    f"nothing has been published for "
                    f"{seconds_since / 3600.0:.0f}h{newest_note}; the "
                    f"graph being served is that old; {resume}"
                )
                if long_pause
                else (
                    f"{workflow_name} is paused for {deployment!r}; its "
                    f"last cycle to complete did so {seconds_since:.0f}s "
                    f"ago{newest_note}; {resume}"
                )
            ),
            last_success_at=last_success_at,
            seconds_since_last_success=seconds_since,
            threshold_seconds=threshold_seconds,
            schedule_paused=True,
            schedule_last_fired_at=last_fired_at,
        )
    if seconds_since > threshold_seconds:
        return PublisherHealthVerdict(
            deployment=deployment,
            ok=not stale_is_fatal,
            code=f"{result_prefix}_stale",
            message=(
                f"most-recent {workflow_name} SUCCESS for "
                f"{deployment!r} was {seconds_since:.0f}s ago "
                f"(threshold: {threshold_seconds:.0f}s = "
                f"{threshold_multiplier}x cron interval "
                f"{interval_seconds}s)"
            ),
            last_success_at=last_success_at,
            seconds_since_last_success=seconds_since,
            threshold_seconds=threshold_seconds,
        )

    return PublisherHealthVerdict(
        deployment=deployment,
        ok=True,
        code=f"{result_prefix}_healthy",
        message=(
            f"{workflow_name} last succeeded "
            f"{seconds_since:.0f}s ago{paused_note}"
        ),
        last_success_at=last_success_at,
        seconds_since_last_success=seconds_since,
        threshold_seconds=threshold_seconds,
        schedule_paused=paused,
        schedule_last_fired_at=last_fired_at,
    )


def _schedule_pause_state(cur, schedule_id: str) -> tuple[bool, str | None]:
    """Is ``schedule_id`` persisted as paused, and when did it last fire?

    ``okg runtime pause-schedule`` is the only okg path that sets
    ``PAUSED``; DBOS keeps it across worker restarts. A database with no
    ``dbos.workflow_schedules`` table, or no row for this schedule (a worker
    that has not registered it yet, or a promotion fence that removed the
    schedules), has no pause to report.
    """
    cur.execute("SELECT to_regclass('dbos.workflow_schedules')")
    if cur.fetchone()[0] is None:
        return False, None
    cur.execute(
        """
        SELECT status, last_fired_at
          FROM dbos.workflow_schedules
         WHERE schedule_name = %s
        """,
        (schedule_id,),
    )
    schedule = cur.fetchone()
    if schedule is None:
        return False, None
    status, last_fired_at = schedule
    return (
        str(status or "").upper() == "PAUSED",
        None if last_fired_at is None else str(last_fired_at),
    )
