"""Backlog/queue probes for `okg doctor`.

Extracted from ``runtime/publisher_health.py`` by
``extract-publisher-health-backlog`` (2026-05) per the cleanup
audit. 4 backlog-runaway probes:

  - ``check_queue_depth(deployment, dsn, ...)`` — DBOS
    partition-queue depth (caught a cron firing faster than the
    cycle completes).
  - ``check_lens_putback_backlog(deployment, dsn, ...)`` —
    `okg.lens_putback_requests` rows stuck at ``prepared``
    longer than the configured age floor.
  - ``check_escalation_open_backlog(deployment, dsn, ...)`` —
    `okg.escalation_live` rows in ``pending`` / ``routed``.
  - ``check_generation_event_subscribers(deployment, dsn)`` —
    `pg_stat_activity` heuristic counting LISTENers on the
    deployment's published-generation channel.

Each probe returns a frozen verdict dataclass with ``ok`` +
``code`` + ``message`` and probe-specific extra fields. Healthy
states are explicit; runaway states are explicit; thresholds
are caller-overridable kwargs.

``runtime/publisher_health.py`` re-exports these names at
module-import time so doctor.py + tests/* keep importing from
`publisher_health` without changes.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from okg.substrate.db.connection import connect as db_connect


__all__ = [
    "QueueDepthVerdict",
    "LensPutbackBacklogVerdict",
    "EscalationOpenBacklogVerdict",
    "GenerationEventSubscribersVerdict",
    "check_queue_depth",
    "check_lens_putback_backlog",
    "check_escalation_open_backlog",
    "check_generation_event_subscribers",
]


@dataclass(frozen=True)
class QueueDepthVerdict:
    """Outcome of the partition-queue depth probe.

    Surfaces queue-backlog runaway: the publisher cron fires at a
    fixed cadence regardless of whether the previous cycle has
    finished, so if cycle time grows past the cron interval the
    `concurrency=1` queue depth grows linearly. The probe catches
    that condition before it spirals.
    """

    deployment: str
    ok: bool
    code: str
    message: str
    enqueued_count: int
    pending_count: int
    threshold: int

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "code": self.code,
            "message": self.message,
            "deployment": self.deployment,
            "enqueued_count": self.enqueued_count,
            "pending_count": self.pending_count,
            "threshold": self.threshold,
        }


def check_queue_depth(
    *,
    deployment: str,
    dsn: str,
    workflow_name: str = "okg_publisher_cycle",
    threshold: int = 3,
) -> QueueDepthVerdict:
    """Flag runaway queue depth for a deployment's partition queue.

    ``concurrency=1`` means at most one workflow is PENDING at any
    time, plus N ENQUEUED waiting. Healthy steady state is enqueued
    ≤ 1; cron fires faster than cycle time produces enqueued ~= N
    growing without bound.

    The probe reports ``ok=False`` when ``enqueued_count > threshold``.
    The default threshold (3) tolerates one missed tick of slack
    without false-positive churn.
    """
    queue_name = f"okg-{deployment}"
    with db_connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  COUNT(*) FILTER (WHERE status='ENQUEUED'),
                  COUNT(*) FILTER (WHERE status='PENDING')
                  FROM dbos.workflow_status
                 WHERE name = %s AND queue_name = %s
                """,
                (workflow_name, queue_name),
            )
            row = cur.fetchone()
    enqueued = int(row[0]) if row else 0
    pending = int(row[1]) if row else 0

    if enqueued > threshold:
        return QueueDepthVerdict(
            deployment=deployment,
            ok=False,
            code="queue_depth_runaway",
            message=(
                f"{workflow_name} on queue {queue_name!r} has "
                f"{enqueued} ENQUEUED (+ {pending} PENDING); "
                f"threshold is {threshold}. The cron is firing "
                f"faster than the cycle completes — slow the cron "
                f"or speed up the cycle."
            ),
            enqueued_count=enqueued,
            pending_count=pending,
            threshold=threshold,
        )

    return QueueDepthVerdict(
        deployment=deployment,
        ok=True,
        code="queue_depth_healthy",
        message=(
            f"{workflow_name} on queue {queue_name!r}: "
            f"enqueued={enqueued} pending={pending} "
            f"(threshold {threshold})"
        ),
        enqueued_count=enqueued,
        pending_count=pending,
        threshold=threshold,
    )


@dataclass(frozen=True)
class LensPutbackBacklogVerdict:
    """Outcome of the lens-putback backlog probe.

    Three-zone reporting (healthy / warn / runaway) matches the
    behavior described in
    `add-lens-putback-applier-workflow`'s spec deltas.
    """

    deployment: str
    ok: bool
    code: str
    message: str
    count: int
    warn_threshold: int
    runaway_threshold: int
    age_floor_seconds: int

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "code": self.code,
            "message": self.message,
            "deployment": self.deployment,
            "count": self.count,
            "warn_threshold": self.warn_threshold,
            "runaway_threshold": self.runaway_threshold,
            "age_floor_seconds": self.age_floor_seconds,
        }


def check_lens_putback_backlog(
    *,
    deployment: str,
    dsn: str,
    warn_threshold: int = 10,
    runaway_threshold: int = 100,
    age_floor_seconds: int = 600,
) -> LensPutbackBacklogVerdict:
    """Count prepared putback requests stuck older than ``age_floor``.

    The age floor (default 10 minutes) keeps the probe quiet on the
    typical viz path where ``prepare_lens_putback`` + ``commit_lens_overlay``
    run in the same HTTP request: a row briefly sits at ``prepared``
    between the two SQL transactions, but it's not stuck.

    Three-zone reporting:

      * count ≤ ``warn_threshold`` → ``lens_putback_backlog_healthy``, ok=True.
      * ``warn_threshold`` < count ≤ ``runaway_threshold``
        → ``lens_putback_backlog_warn``, ok=True (operator should see it
        in warnings but it's not fatal).
      * count > ``runaway_threshold`` → ``lens_putback_backlog_runaway``,
        ok=False.

    Deployments that don't use lenses report 0 rows and pass cleanly
    (the WHERE clause filters to the named deployment).
    """
    with db_connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                  FROM okg.lens_putback_requests
                 WHERE deployment_name = %s
                   AND status = 'prepared'
                   AND prepared_at IS NOT NULL
                   AND prepared_at < now()
                       - make_interval(secs => %s)
                """,
                (deployment, age_floor_seconds),
            )
            row = cur.fetchone()
    count = int(row[0]) if row else 0

    if count > runaway_threshold:
        return LensPutbackBacklogVerdict(
            deployment=deployment,
            ok=False,
            code="lens_putback_backlog_runaway",
            message=(
                f"{count} prepared lens-putback requests older "
                f"than {age_floor_seconds}s for {deployment!r}; "
                f"runaway threshold is {runaway_threshold}. The "
                f"sweep workflow may be stuck — check "
                f"`lens_putback_sweep_health`."
            ),
            count=count,
            warn_threshold=warn_threshold,
            runaway_threshold=runaway_threshold,
            age_floor_seconds=age_floor_seconds,
        )
    if count > warn_threshold:
        return LensPutbackBacklogVerdict(
            deployment=deployment,
            ok=True,
            code="lens_putback_backlog_warn",
            message=(
                f"{count} prepared lens-putback requests older "
                f"than {age_floor_seconds}s for {deployment!r}; "
                f"warn threshold is {warn_threshold}. Sweep "
                f"workflow will clear them next tick."
            ),
            count=count,
            warn_threshold=warn_threshold,
            runaway_threshold=runaway_threshold,
            age_floor_seconds=age_floor_seconds,
        )
    return LensPutbackBacklogVerdict(
        deployment=deployment,
        ok=True,
        code="lens_putback_backlog_healthy",
        message=(
            f"lens-putback backlog for {deployment!r}: "
            f"{count} stale prepared rows "
            f"(warn={warn_threshold}, runaway={runaway_threshold})"
        ),
        count=count,
        warn_threshold=warn_threshold,
        runaway_threshold=runaway_threshold,
        age_floor_seconds=age_floor_seconds,
    )


@dataclass(frozen=True)
class EscalationOpenBacklogVerdict:
    """Outcome of the open-escalation backlog probe.

    Open escalations are normal HITL state — an operator is being
    asked to decide. The probe is informational only; large open
    counts are surfaced as warnings, not errors. A genuinely-stuck
    escalation surfaces via the `escalation_sweep_health` probe
    instead (its `transitioned` count would be zero across cycles
    when there's a backlog).
    """

    deployment: str
    ok: bool
    code: str
    message: str
    count: int
    warn_threshold: int

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "code": self.code,
            "message": self.message,
            "deployment": self.deployment,
            "count": self.count,
            "warn_threshold": self.warn_threshold,
        }


def check_escalation_open_backlog(
    *,
    deployment: str,
    dsn: str,
    warn_threshold: int = 50,
) -> EscalationOpenBacklogVerdict:
    """Count currently-open escalations for the deployment.

    Informational only — open is the normal state for HITL requests.
    Above ``warn_threshold`` the doctor surfaces a `warnings` entry
    so operators see the backlog without blocking the doctor's
    overall ok=True.
    """
    with db_connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                  FROM okg.escalation_live el
                  JOIN okg.graph_generations gg
                    ON gg.generation_id = el.valid_from_gen
                 WHERE el.valid_to_gen IS NULL
                   AND gg.deployment_name = %s
                   AND (el.attrs->>'status') IN ('pending', 'routed')
                """,
                (deployment,),
            )
            row = cur.fetchone()
    count = int(row[0]) if row else 0

    if count > warn_threshold:
        return EscalationOpenBacklogVerdict(
            deployment=deployment,
            ok=True,
            code="escalation_open_backlog_high",
            message=(
                f"{count} open escalations for {deployment!r} "
                f"(threshold {warn_threshold}). Normal HITL state, "
                f"but check whether operators are reachable."
            ),
            count=count,
            warn_threshold=warn_threshold,
        )
    return EscalationOpenBacklogVerdict(
        deployment=deployment,
        ok=True,
        code="escalation_open_backlog_healthy",
        message=(
            f"open escalations for {deployment!r}: {count} "
            f"(warn at {warn_threshold})"
        ),
        count=count,
        warn_threshold=warn_threshold,
    )


@dataclass(frozen=True)
class GenerationEventSubscribersVerdict:
    """Outcome of the generation-event-subscribers probe.

    Per `publish-generation-events-via-dbos-streams`
    (deepen-dbos-integration Phase 1). Informational: zero subscribers
    is acceptable (no consumer running). Reports count + a sample of
    LISTENer session metadata so operators can correlate with
    application identities.
    """

    deployment: str
    ok: bool
    code: str
    message: str
    channel: str
    count: int
    listeners: list[dict[str, Any]]

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "code": self.code,
            "message": self.message,
            "deployment": self.deployment,
            "channel": self.channel,
            "count": self.count,
            "listeners": self.listeners,
        }


def check_generation_event_subscribers(
    *,
    deployment: str,
    dsn: str,
) -> GenerationEventSubscribersVerdict:
    """Count active `LISTEN`ers on the deployment's published-generation
    channel.

    Cross-references `pg_stat_activity` against the channel name via
    `pg_listening_channels()`. Postgres exposes which channels each
    backend is listening to via the `pg_listening_channels()`
    function — but only for the CURRENT backend. So we instead
    query `pg_stat_activity` filtered by `query LIKE 'LISTEN %'`
    or look at `application_name` patterns operators set on their
    LISTEN sessions. Both are heuristics; not exact.

    For an exact count, run inside a session that has already
    LISTENed and call `pg_listening_channels()` — but that's the
    consumer's job, not the doctor's. The doctor's job is to give
    the operator a "≥ 1 consumer observed" signal.

    Returns ``ok=True`` always (zero subscribers is informational).
    """
    from okg.substrate.publisher.publisher import (
        generation_published_channel,
    )

    channel = generation_published_channel(deployment)
    listeners: list[dict[str, Any]] = []

    # Heuristic: look for backends with `LISTEN <channel>` recorded
    # in their last query. This catches operator `psql -c "LISTEN ..."`
    # sessions and library-based listeners that issued a LISTEN
    # statement at session start.
    with db_connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pid, application_name, client_addr, state,
                       to_char(backend_start,
                               'YYYY-MM-DD HH24:MI:SS TZ')
                  FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND (
                     query ILIKE %s
                     OR query ILIKE %s
                   )
                """,
                (
                    f"%LISTEN {channel}%",
                    f"%LISTEN \"{channel}\"%",
                ),
            )
            for row in cur.fetchall():
                listeners.append({
                    "pid": row[0],
                    "application_name": row[1],
                    "client_addr": (
                        str(row[2]) if row[2] is not None else None
                    ),
                    "state": row[3],
                    "backend_start": row[4],
                })

    count = len(listeners)
    return GenerationEventSubscribersVerdict(
        deployment=deployment,
        ok=True,
        code=(
            "generation_event_subscribers_observed"
            if count > 0
            else "generation_event_subscribers_none"
        ),
        message=(
            f"{count} LISTEN session(s) on channel {channel!r} "
            f"for deployment {deployment!r}"
        ),
        channel=channel,
        count=count,
        listeners=listeners,
    )


