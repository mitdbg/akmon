"""Source-sync workflow enqueue helpers.

This module keeps source-sync compaction at the producer boundary. DBOS queue
deduplication protects the durable workflow lane; source-sync producers decide
when compatible payloads may be merged into an already queued workflow.
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Mapping

import psycopg
from okg.substrate.db.connection import connect as db_connect
from psycopg import sql

from okg.substrate.runtime.dbos_enqueue import (
    DBOSWorkflowEnqueueResult,
    TerminalDeduplicationCollision,
    active_workflow_id_for_name,
    enqueue_deployment_workflow,
)
from okg.substrate.runtime.partition_queue import (
    partition_queue_name,
    source_sync_enqueue_priority,
    source_sync_queue_name,
)
from okg.substrate.runtime.promotion_fence import promotion_enqueue_permit
from okg.substrate.runtime.source_sync_priority import (
    publisher_publish_lock_holder_id,
    source_sync_admission_intent_lock,
)
from okg.substrate.source_sync import (
    SourceSyncCompaction,
    SourceSyncJob,
    source_sync_desired_frontier_state,
)
from okg.substrate.source_sync_convergence import (
    COMPLETED_SCOPE_RUN_MODES,
    READY_SOURCES,
)
from okg.substrate.telemetry import new_span_id, new_trace_id


log = logging.getLogger(__name__)

_PUBLISHER_WORKFLOW_NAME = "okg_publisher_cycle"
_PUBLISHER_WAIT_TIMEOUT_ENV = (
    "OKG_SOURCE_SYNC_PUBLISHER_WAIT_TIMEOUT_SECONDS"
)
_PUBLISHER_WAIT_INTERVAL_ENV = (
    "OKG_SOURCE_SYNC_PUBLISHER_WAIT_INTERVAL_SECONDS"
)
_DEFAULT_PUBLISHER_WAIT_TIMEOUT_SECONDS = 600.0
_DEFAULT_PUBLISHER_WAIT_INTERVAL_SECONDS = 0.5


class _SourceSyncCompactionRefused(ValueError):
    """The bounded merge cannot certify preservation of executable scope."""


def _merge_source_sync_payloads(
    existing: dict[str, Any],
    incoming: dict[str, Any],
    *,
    compaction: SourceSyncCompaction,
) -> dict[str, Any]:
    sources: list[str] = []
    seen_sources: set[str] = set()
    for source in [
        *(existing.get("sources") or ()),
        *(incoming.get("sources") or ()),
    ]:
        if not isinstance(source, str) or source in seen_sources:
            continue
        seen_sources.add(source)
        sources.append(source)

    for payload in (existing, incoming):
        if int((payload.get("source_sync_compaction") or {}).get(
            "dropped_event_count", 0,
        )) > 0:
            raise _SourceSyncCompactionRefused(
                "previously dropped source events require separate reconciliation"
            )

    existing_events = list(existing.get("events") or ())
    incoming_events = list(incoming.get("events") or ())
    total = int(
        (existing.get("source_sync_compaction") or {}).get(
            "event_count", len(existing_events),
        )
    ) + len(incoming_events)
    events: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for event in [*existing_events, *incoming_events]:
        if not isinstance(event, dict):
            continue
        ident = _event_identity(event)
        if ident in seen:
            continue
        seen.add(ident)
        if len(events) >= compaction.max_events:
            raise _SourceSyncCompactionRefused(
                "distinct source events exceed the compaction limit"
            )
        events.append(event)

    scope_keys = []
    seen_scope_keys: set[str] = set()
    for event in events:
        if not isinstance(event, dict):
            continue
        scope_key = event.get("scope_key")
        if not scope_key or scope_key in seen_scope_keys:
            continue
        seen_scope_keys.add(str(scope_key))
        if len(scope_keys) < compaction.max_scope_keys:
            scope_keys.append(str(scope_key))

    merged = dict(existing)
    merged["sources"] = sources
    merged["events"] = events
    merged["profile_force_deep"] = bool(
        existing.get("profile_force_deep")
        or incoming.get("profile_force_deep")
    )
    collectors: list[str] = []
    seen_collectors: set[str] = set()
    for collector in [
        *(existing.get("profile_collectors") or ()),
        *(incoming.get("profile_collectors") or ()),
    ]:
        if not isinstance(collector, str) or collector in seen_collectors:
            continue
        seen_collectors.add(collector)
        collectors.append(collector)
    merged["profile_collectors"] = collectors
    merged["source_sync_compaction"] = {
        "event_count": total,
        "stored_event_count": len(events),
        "dropped_event_count": 0,
        "scope_keys_sample": scope_keys,
    }
    return merged


def _event_identity(event: dict[str, Any]) -> tuple[Any, ...]:
    return (
        event.get("event_id"),
        event.get("source_name"),
        event.get("trigger_kind"),
        event.get("scope_key"),
        event.get("revision_hint"),
        event.get("reason"),
    )


def enqueue_source_sync_via_dbos(
    *,
    deployment: str,
    dsn: str,
    job: SourceSyncJob,
    deduplication_id: str | None = None,
    promotion_id: str | None = None,
) -> str:
    """Enqueue an ``okg_source_sync`` workflow via DBOSClient.

    Used by the watcher / poll / webhook producers so source-sync triggers
    route through DBOS.

    The args list (sources, run_mode, include_dependents,
    dependent_fanout, events, publish, include_predecessors) mirrors what
    ``source_sync_workflow_impl`` accepts in
    ``runtime.source_sync_workflow``.

    Returns the workflow_id. Raises if DBOS isn't reachable or the
    queue isn't registered.
    """
    return _enqueue_source_sync_via_dbos_result(
        deployment=deployment,
        dsn=dsn,
        job=job,
        deduplication_id=deduplication_id,
        promotion_id=promotion_id,
    ).workflow_id


def enqueue_compacted_source_sync_via_dbos(
    *,
    deployment: str,
    dsn: str,
    job: SourceSyncJob,
    deduplication_id: str | None,
    compaction: SourceSyncCompaction | None,
    promotion_id: str | None = None,
) -> str:
    """Enqueue or compact a DBOS source-sync workflow.

    DBOS queue dedupe alone is not enough for filesystem watcher events:
    a duplicate against a running workflow would acknowledge the new event
    without preserving its scope. The DBOS producer preserves that behavior:

    * compact into an existing queued workflow when safe;
    * enqueue a stable successor while the matching workflow is running;
    * use a unique final fallback only if both logical slots are active.
    """
    if (
        not deduplication_id
        or compaction is None
        or compaction.window_seconds <= 0
    ):
        return enqueue_source_sync_via_dbos(
            deployment=deployment,
            dsn=dsn,
            job=job,
            deduplication_id=deduplication_id,
            promotion_id=promotion_id,
        )

    # Compaction mutates an existing queued workflow's inputs, so it is a
    # writer admission path just like a fresh enqueue. Hold the promotion
    # permit for the complete mutation attempt, then release it before the
    # separately guarded enqueue path to avoid a nested advisory-lock wait.
    with promotion_enqueue_permit(
        deployment=deployment,
        dsn=dsn,
        promotion_id=promotion_id,
    ):
        compacted = _compact_dbos_queued_source_sync(
            dsn=dsn,
            deployment=deployment,
            deduplication_id=deduplication_id,
            incoming=job.to_dict(),
            compaction=compaction,
        )
        if compacted is not None:
            return compacted
        deduplication_prefix = _source_sync_deduplication_prefix(
            deduplication_id
        )
        if deduplication_prefix is not None:
            compacted = _compact_dbos_queued_source_sync_by_prefix(
                dsn=dsn,
                deployment=deployment,
                deduplication_prefix=deduplication_prefix,
                incoming=job.to_dict(),
                compaction=compaction,
            )
            if compacted is not None:
                return compacted
        compacted = _compact_dbos_queued_source_sync_by_compatibility(
            dsn=dsn,
            deployment=deployment,
            incoming=job.to_dict(),
            compaction=compaction,
        )
        if compacted is not None:
            return compacted

    result = _enqueue_source_sync_via_dbos_result(
        deployment=deployment,
        dsn=dsn,
        job=job,
        deduplication_id=deduplication_id,
        promotion_id=promotion_id,
    )
    if not result.deduplicated:
        return result.workflow_id

    # Race case: another producer may have inserted an ENQUEUED row after
    # the first compaction attempt but before DBOS rejected our enqueue.
    with promotion_enqueue_permit(
        deployment=deployment,
        dsn=dsn,
        promotion_id=promotion_id,
    ):
        compacted = _compact_dbos_queued_source_sync(
            dsn=dsn,
            deployment=deployment,
            deduplication_id=deduplication_id,
            incoming=job.to_dict(),
            compaction=compaction,
        )
        if compacted is not None:
            return compacted

        compacted = _compact_dbos_queued_source_sync_by_compatibility(
            dsn=dsn,
            deployment=deployment,
            incoming=job.to_dict(),
            compaction=compaction,
        )
        if compacted is not None:
            return compacted

    return _enqueue_dbos_successor_source_sync(
        deployment=deployment,
        dsn=dsn,
        job=job,
        deduplication_id=deduplication_id,
        compaction=compaction,
        promotion_id=promotion_id,
    )


def _enqueue_source_sync_via_dbos_result(
    *,
    deployment: str,
    dsn: str,
    job: SourceSyncJob,
    deduplication_id: str | None = None,
    promotion_id: str | None = None,
) -> DBOSWorkflowEnqueueResult:
    try:
        return _enqueue_source_sync_once(
            deployment=deployment,
            dsn=dsn,
            job=job,
            deduplication_id=deduplication_id,
            promotion_id=promotion_id,
        )
    except TerminalDeduplicationCollision:
        if deduplication_id is None:
            raise
        return _enqueue_source_sync_once(
            deployment=deployment,
            dsn=dsn,
            job=job,
            deduplication_id=_terminal_successor_deduplication_id(
                deduplication_id
            ),
            promotion_id=promotion_id,
        )


def _enqueue_source_sync_once(
    *,
    deployment: str,
    dsn: str,
    job: SourceSyncJob,
    deduplication_id: str | None,
    promotion_id: str | None = None,
) -> DBOSWorkflowEnqueueResult:
    trace_id = new_trace_id()
    root_span_id = new_span_id()
    wait_span_id = new_span_id()
    admission_wait_started_at: datetime | None = None
    while True:
        # A publisher may legitimately run for minutes.  Never hold the
        # deployment-wide source-sync admission intent while sleeping for it:
        # every poller would otherwise convoy behind this producer.
        _wait_for_running_publisher_before_source_sync_enqueue(
            deployment=deployment,
            dsn=dsn,
        )
        if admission_wait_started_at is None:
            admission_wait_started_at = datetime.now(timezone.utc)
        with source_sync_admission_intent_lock(
            deployment=deployment,
            dsn=dsn,
        ):
            admission_wait_ended_at = datetime.now(timezone.utc)
            # Close the race between the outside wait and acquiring the
            # admission-intent lock.  If a publisher entered first, release
            # the intent immediately and wait outside it on the next loop.
            if _running_publisher_workflow_id(
                deployment=deployment,
                dsn=dsn,
            ) is not None:
                continue
            return enqueue_deployment_workflow(
                deployment=deployment,
                dsn=dsn,
                workflow_name="okg_source_sync",
                deduplication_id=deduplication_id,
                queue_name=source_sync_queue_name(deployment),
                priority=_source_sync_priority_from_job(job),
                args=_source_sync_workflow_args(
                    job,
                    workflow_telemetry={
                        "schema": "okg.source_sync.trace.v1",
                        "trace_id": trace_id,
                        "root_span_id": root_span_id,
                        "wait_span_id": wait_span_id,
                        "started_at": admission_wait_started_at.isoformat(),
                        "ended_at": admission_wait_ended_at.isoformat(),
                    },
                ),
                promotion_id=promotion_id,
            )


def _wait_for_running_publisher_before_source_sync_enqueue(
    *,
    deployment: str,
    dsn: str,
) -> None:
    """Avoid creating source-sync work while a publisher cycle is running.

    Publisher cycles also yield when source-sync is already queued/running.
    This producer-side guard closes the opposite race: source-sync arriving
    immediately after the publisher's post-lock probe. A held publisher
    advisory lock or a PENDING publisher blocks enqueue. An ENQUEUED publisher
    can still yield to the source-sync workflow when it starts.
    """
    timeout_seconds = _float_env(
        _PUBLISHER_WAIT_TIMEOUT_ENV,
        _DEFAULT_PUBLISHER_WAIT_TIMEOUT_SECONDS,
    )
    interval_seconds = _float_env(
        _PUBLISHER_WAIT_INTERVAL_ENV,
        _DEFAULT_PUBLISHER_WAIT_INTERVAL_SECONDS,
    )
    deadline = time.monotonic() + timeout_seconds
    logged_wait = False
    while True:
        blocking_workflow_id = _running_publisher_workflow_id(
            deployment=deployment,
            dsn=dsn,
        )
        if blocking_workflow_id is None:
            return
        if not logged_wait:
            log.info(
                "Delaying source-sync enqueue for deployment %s because "
                "publisher workflow %s is running",
                deployment,
                blocking_workflow_id,
            )
            logged_wait = True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError(
                "source-sync enqueue is blocked by running publisher "
                f"workflow {blocking_workflow_id!r} for deployment "
                f"{deployment!r}"
            )
        time.sleep(min(interval_seconds, remaining))


def _running_publisher_workflow_id(
    *,
    deployment: str,
    dsn: str,
) -> str | None:
    blocking_workflow_id = publisher_publish_lock_holder_id(
        dsn=dsn,
        deployment=deployment,
    )
    if blocking_workflow_id is not None:
        return blocking_workflow_id
    return active_workflow_id_for_name(
        dsn=dsn,
        queue_name=partition_queue_name(deployment),
        workflow_name=_PUBLISHER_WORKFLOW_NAME,
        statuses=("PENDING",),
    )


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        log.warning("Ignoring invalid %s=%r; using %.3f", name, raw, default)
        return default
    if value < 0:
        log.warning("Ignoring negative %s=%r; using %.3f", name, raw, default)
        return default
    return value


def _terminal_successor_deduplication_id(deduplication_id: str) -> str:
    return f"{deduplication_id}:terminal-successor:{uuid.uuid4().hex}"


def _source_sync_deduplication_prefix(deduplication_id: str) -> str | None:
    """Return the logical source-sync prefix before the time bucket."""
    head, sep, tail = deduplication_id.rpartition(":")
    if not sep or not tail.isdigit():
        return None
    return f"{head}:"


def _enqueue_dbos_successor_source_sync(
    *,
    deployment: str,
    dsn: str,
    job: SourceSyncJob,
    deduplication_id: str,
    compaction: SourceSyncCompaction,
    promotion_id: str | None = None,
) -> str:
    deduplication_prefix = _source_sync_deduplication_prefix(
        deduplication_id
    )
    with promotion_enqueue_permit(
        deployment=deployment,
        dsn=dsn,
        promotion_id=promotion_id,
    ):
        if deduplication_prefix is not None:
            compacted = _compact_dbos_queued_source_sync_by_prefix(
                dsn=dsn,
                deployment=deployment,
                deduplication_prefix=deduplication_prefix,
                incoming=job.to_dict(),
                compaction=compaction,
            )
            if compacted is not None:
                return compacted
        compacted = _compact_dbos_queued_source_sync_by_compatibility(
            dsn=dsn,
            deployment=deployment,
            incoming=job.to_dict(),
            compaction=compaction,
        )
        if compacted is not None:
            return compacted
    successor_id = (
        f"{deduplication_prefix}successor"
        if deduplication_prefix is not None
        else f"{deduplication_id}:successor"
    )
    with promotion_enqueue_permit(
        deployment=deployment,
        dsn=dsn,
        promotion_id=promotion_id,
    ):
        compacted = _compact_dbos_queued_source_sync(
            dsn=dsn,
            deployment=deployment,
            deduplication_id=successor_id,
            incoming=job.to_dict(),
            compaction=compaction,
        )
        if compacted is not None:
            return compacted

    result = _enqueue_source_sync_via_dbos_result(
        deployment=deployment,
        dsn=dsn,
        job=job,
        deduplication_id=successor_id,
        promotion_id=promotion_id,
    )
    if not result.deduplicated:
        return result.workflow_id

    with promotion_enqueue_permit(
        deployment=deployment,
        dsn=dsn,
        promotion_id=promotion_id,
    ):
        compacted = _compact_dbos_queued_source_sync(
            dsn=dsn,
            deployment=deployment,
            deduplication_id=successor_id,
            incoming=job.to_dict(),
            compaction=compaction,
        )
        if compacted is not None:
            return compacted

    unique_id = f"{successor_id}:{uuid.uuid4().hex}"
    return _enqueue_source_sync_via_dbos_result(
        deployment=deployment,
        dsn=dsn,
        job=job,
        deduplication_id=unique_id,
        promotion_id=promotion_id,
    ).workflow_id


def _compact_dbos_queued_source_sync_by_prefix(
    *,
    dsn: str,
    deployment: str,
    deduplication_prefix: str,
    incoming: dict[str, Any],
    compaction: SourceSyncCompaction,
) -> str | None:
    queue_name = source_sync_queue_name(deployment)
    with db_connect(dsn, connect_timeout=5) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('dbos.workflow_status')")
                if cur.fetchone()[0] is None:
                    return None
                cur.execute(
                    """
                    SELECT workflow_uuid, inputs, serialization, priority
                      FROM dbos.workflow_status
                     WHERE queue_name = %s
                       AND name = 'okg_source_sync'
                       AND deduplication_id IS NOT NULL
                       AND left(deduplication_id, %s) = %s
                       AND status = 'ENQUEUED'
                     ORDER BY priority NULLS LAST, created_at, workflow_uuid
                     FOR UPDATE SKIP LOCKED
                    """,
                    (
                        queue_name,
                        len(deduplication_prefix),
                        deduplication_prefix,
                    ),
                )
                return _compact_dbos_queued_source_sync_candidates(
                    cur=cur,
                    deployment=deployment,
                    rows=cur.fetchall(),
                    incoming=incoming,
                    compaction=compaction,
                )


def _compact_dbos_queued_source_sync(
    *,
    dsn: str,
    deployment: str,
    deduplication_id: str,
    incoming: dict[str, Any],
    compaction: SourceSyncCompaction,
) -> str | None:
    queue_name = source_sync_queue_name(deployment)
    with db_connect(dsn, connect_timeout=5) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('dbos.workflow_status')")
                if cur.fetchone()[0] is None:
                    return None
                cur.execute(
                    """
                    SELECT workflow_uuid, inputs, serialization, priority
                      FROM dbos.workflow_status
                     WHERE queue_name = %s
                       AND name = 'okg_source_sync'
                       AND deduplication_id = %s
                       AND status = 'ENQUEUED'
                     ORDER BY priority NULLS LAST, created_at, workflow_uuid
                     FOR UPDATE SKIP LOCKED
                    """,
                    (queue_name, deduplication_id),
                )
                return _compact_dbos_queued_source_sync_candidates(
                    cur=cur,
                    deployment=deployment,
                    rows=cur.fetchall(),
                    incoming=incoming,
                    compaction=compaction,
                )


def _compact_dbos_queued_source_sync_by_compatibility(
    *,
    dsn: str,
    deployment: str,
    incoming: dict[str, Any],
    compaction: SourceSyncCompaction,
) -> str | None:
    """Merge compatible waiting source-sync wakeups across dedupe buckets.

    Exact and prefix compaction catch the common path. This path handles the
    concurrency-1 queue case where several compatible ENQUEUED successors have
    different source-key dedupe prefixes while one PENDING workflow owns the
    lane. Only ENQUEUED rows are compacted; running PENDING work is never
    cancelled here.
    """
    queue_name = source_sync_queue_name(deployment)
    with db_connect(dsn, connect_timeout=5) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('dbos.workflow_status')")
                if cur.fetchone()[0] is None:
                    return None
                cur.execute(
                    """
                    SELECT workflow_uuid, inputs, serialization, priority
                      FROM dbos.workflow_status
                     WHERE queue_name = %s
                       AND name = 'okg_source_sync'
                       AND status = 'ENQUEUED'
                     ORDER BY priority NULLS LAST, created_at, workflow_uuid
                     FOR UPDATE SKIP LOCKED
                    """,
                    (queue_name,),
                )
                return _compact_dbos_queued_source_sync_candidates(
                    cur=cur,
                    deployment=deployment,
                    rows=cur.fetchall(),
                    incoming=incoming,
                    compaction=compaction,
                )


def _compact_dbos_queued_source_sync_candidates(
    *,
    cur: psycopg.Cursor,
    deployment: str,
    rows: list[tuple[Any, str | None, str | None, Any]],
    incoming: dict[str, Any],
    compaction: SourceSyncCompaction,
) -> str | None:
    candidates: list[
        tuple[Any, str | None, str | None, Any, dict[str, Any]]
    ] = []
    for workflow_uuid, inputs, serialization, priority in rows:
        existing = _source_sync_payload_from_dbos_inputs(
            deployment=deployment,
            inputs=inputs,
            serialization=serialization,
        )
        if existing is None:
            continue
        if not _source_sync_payloads_compatible(existing, incoming):
            continue
        candidates.append((
            workflow_uuid,
            inputs,
            serialization,
            priority,
            existing,
        ))
    if not candidates:
        return None

    (
        canonical_workflow_uuid,
        _canonical_inputs,
        canonical_serialization,
        _canonical_priority,
        merged,
    ) = candidates[0]
    try:
        for _workflow_uuid, _inputs, _serialization, _priority, payload in (
            candidates[1:]
        ):
            merged = _merge_source_sync_payloads(
                merged,
                payload,
                compaction=compaction,
            )
        merged = _merge_source_sync_payloads(
            merged,
            incoming,
            compaction=compaction,
        )
    except _SourceSyncCompactionRefused:
        # No queued inputs have changed yet. Keep every sibling intact and let
        # the existing enqueue/successor path retain the incoming work separately.
        return None
    merged_job = SourceSyncJob.from_dict(merged)
    serialized_inputs, updated_serialization = (
        _serialize_source_sync_dbos_args(
            args=_source_sync_workflow_args(
                merged_job,
                force_enrichers=bool(merged.get("force_enrichers")),
                workflow_telemetry=merged.get("workflow_telemetry"),
            ),
            serialization=canonical_serialization,
        )
    )
    now_ms = int(time.time() * 1000)
    cur.execute(
        """
        UPDATE dbos.workflow_status
           SET inputs = %s,
               serialization = %s,
               priority = %s,
               updated_at = %s
         WHERE workflow_uuid = %s
           AND status = 'ENQUEUED'
        """,
        (
            serialized_inputs,
            updated_serialization,
            _compacted_source_sync_priority_from_candidates(
                existing_priorities=[
                    candidate[3] for candidate in candidates
                ],
                incoming=incoming,
            ),
            now_ms,
            canonical_workflow_uuid,
        ),
    )
    for workflow_uuid, *_rest in candidates[1:]:
        cur.execute(
            """
            UPDATE dbos.workflow_status
               SET status = 'CANCELLED',
                   updated_at = %s
             WHERE workflow_uuid = %s
               AND status = 'ENQUEUED'
            """,
            (now_ms, workflow_uuid),
        )
    return str(canonical_workflow_uuid)


def _source_sync_payloads_compatible(
    existing: dict[str, Any],
    incoming: dict[str, Any],
) -> bool:
    if str(existing.get("deployment") or "") != str(
        incoming.get("deployment") or ""
    ):
        return False
    comparable_fields = (
        "run_mode",
        "dependent_fanout",
        "include_dependents",
        "include_predecessors",
        "publish",
    )
    for field in comparable_fields:
        if existing.get(field) != incoming.get(field):
            return False
    # An operator's typed `okg ingest --mode` runs as typed (#2486).
    # Compaction rebuilds merged inputs without that flag, so typed work is
    # never merged: it keeps its own workflow.
    if existing.get("run_mode_explicit") or incoming.get("run_mode_explicit"):
        return False
    existing_triggers = _source_sync_payload_trigger_kinds(existing)
    incoming_triggers = _source_sync_payload_trigger_kinds(incoming)
    if existing_triggers or incoming_triggers:
        if existing_triggers != incoming_triggers:
            return False
    existing_readiness = _is_readiness_recovery_payload(existing)
    incoming_readiness = _is_readiness_recovery_payload(incoming)
    if existing_readiness or incoming_readiness:
        existing_sources = _source_sync_payload_source_set(existing)
        incoming_sources = _source_sync_payload_source_set(incoming)
        if existing_sources == incoming_sources:
            return True
        return (
            _is_readiness_only_recovery_payload(existing)
            and _is_readiness_only_recovery_payload(incoming)
        )
    existing_sources = _source_sync_payload_source_set(existing)
    incoming_sources = _source_sync_payload_source_set(incoming)
    if existing_sources != incoming_sources:
        if not existing_triggers:
            return False
    return True


def _source_sync_payload_source_set(payload: dict[str, Any]) -> frozenset[str]:
    sources = payload.get("sources") or ()
    if not isinstance(sources, (list, tuple, set)):
        return frozenset()
    return frozenset(str(source) for source in sources)


def _source_sync_payload_trigger_kinds(
    payload: dict[str, Any],
) -> frozenset[str]:
    events = payload.get("events") or ()
    if not isinstance(events, (list, tuple)):
        return frozenset()
    triggers: set[str] = set()
    for event in events:
        if not isinstance(event, dict):
            continue
        trigger = event.get("trigger_kind")
        if isinstance(trigger, str) and trigger:
            triggers.add(trigger)
    return frozenset(triggers)


def _source_sync_priority_from_payload(payload: dict[str, Any]) -> int:
    collectors = payload.get("profile_collectors") or ()
    if not isinstance(collectors, (list, tuple)):
        collectors = ()
    return source_sync_enqueue_priority(
        operator_requested=_is_operator_requested_payload(payload),
        profile_force_deep=bool(payload.get("profile_force_deep")),
        profile_collectors=[str(value) for value in collectors],
        readiness_recovery=_is_readiness_recovery_payload(payload),
    )


def _source_sync_priority_from_job(job: SourceSyncJob) -> int:
    return source_sync_enqueue_priority(
        operator_requested=_is_operator_requested_job(job),
        profile_force_deep=job.profile_force_deep,
        profile_collectors=list(job.profile_collectors),
        readiness_recovery=_is_readiness_recovery_job(job),
    )


def _is_operator_requested_job(job: SourceSyncJob) -> bool:
    return any(
        event.reason == "operator_scope"
        for event in job.events
    )


def _is_operator_requested_payload(payload: dict[str, Any]) -> bool:
    events = payload.get("events") or ()
    if not isinstance(events, (list, tuple)):
        return False
    return any(
        isinstance(event, dict)
        and event.get("reason") == "operator_scope"
        for event in events
    )


def _is_readiness_recovery_job(job: SourceSyncJob) -> bool:
    if str(job.run_mode) not in COMPLETED_SCOPE_RUN_MODES:
        return False
    return any(source in READY_SOURCES for source in job.sources)


def _is_readiness_recovery_payload(payload: dict[str, Any]) -> bool:
    if str(payload.get("run_mode") or "") not in COMPLETED_SCOPE_RUN_MODES:
        return False
    sources = payload.get("sources") or ()
    if not isinstance(sources, (list, tuple, set)):
        return False
    return any(str(source) in READY_SOURCES for source in sources)


def _is_readiness_only_recovery_payload(payload: dict[str, Any]) -> bool:
    if str(payload.get("run_mode") or "") not in COMPLETED_SCOPE_RUN_MODES:
        return False
    sources = _source_sync_payload_source_set(payload)
    return bool(sources) and sources.issubset(READY_SOURCES)


def _compacted_source_sync_priority(
    *,
    existing_priority: Any,
    incoming: dict[str, Any],
) -> int:
    incoming_priority = _source_sync_priority_from_payload(incoming)
    try:
        current = int(existing_priority)
    except (TypeError, ValueError):
        return incoming_priority
    if current <= 0:
        return incoming_priority
    return min(current, incoming_priority)


def _compacted_source_sync_priority_from_candidates(
    *,
    existing_priorities: list[Any],
    incoming: dict[str, Any],
) -> int:
    priority = _source_sync_priority_from_payload(incoming)
    for existing_priority in existing_priorities:
        try:
            current = int(existing_priority)
        except (TypeError, ValueError):
            continue
        if current > 0:
            priority = min(priority, current)
    return priority


def _source_sync_workflow_args(
    job: SourceSyncJob,
    *,
    force_enrichers: bool = False,
    workflow_telemetry: Mapping[str, Any] | None = None,
) -> tuple[Any, ...]:
    legacy_args: tuple[Any, ...] = (
        list(job.sources),
        job.run_mode,
        job.include_dependents,
        job.dependent_fanout,
        [e.to_dict() for e in job.events],
        job.publish,
        job.include_predecessors,
        job.profile_force_deep,
        list(job.profile_collectors),
        job.supersedes_workflow_id,
    )
    if workflow_telemetry is None and not force_enrichers:
        return legacy_args
    return (
        *legacy_args,
        force_enrichers,
        dict(workflow_telemetry) if workflow_telemetry is not None else None,
    )


def active_dbos_source_sync_workflow_for_source(
    *,
    dsn: str,
    deployment: str,
    source_name: str,
) -> str | None:
    """Return one active DBOS source-sync workflow id for ``source_name``.

    Poll backstops must not enqueue another reconcile while the same source is
    already queued or running on the DBOS source-sync lane.
    """
    status = active_dbos_source_sync_workflow_status_for_source(
        dsn=dsn,
        deployment=deployment,
        source_name=source_name,
    )
    if status is None:
        return None
    return str(status["workflow_id"])


def active_dbos_source_sync_workflow_status_for_source(
    *,
    dsn: str,
    deployment: str,
    source_name: str,
    stale_after_seconds: int = 600,
) -> dict[str, Any] | None:
    """Return active DBOS source-sync workflow state for ``source_name``.

    Poll backstops need the workflow id for suppression, but they also need
    liveness evidence. A stale queued workflow must not hide a stale source
    watermark forever.
    """
    return active_dbos_source_sync_workflows_by_source(
        dsn=dsn,
        deployment=deployment,
        stale_after_seconds=stale_after_seconds,
    ).get(source_name)


def active_dbos_source_sync_workflows_by_source(
    *,
    dsn: str,
    deployment: str,
    stale_after_seconds: int = 600,
) -> dict[str, dict[str, Any]]:
    """Return active DBOS source-sync workflow state keyed by source.

    The decoded run mode is part of the contract: readiness convergence must
    know whether active work can move `last_reconcile_at` or is merely
    `scope_incomplete` event work.
    """
    queue_name = source_sync_queue_name(deployment)
    with db_connect(dsn, connect_timeout=5) as conn, conn.cursor() as cur:
        rows = _active_dbos_source_sync_workflow_rows(
            cur,
            queue_name=queue_name,
        )

    candidates_by_source: dict[str, list[dict[str, Any]]] = {}
    for (
        workflow_uuid,
        status,
        inputs,
        serialization,
        created_raw,
        updated_raw,
        started_raw,
        priority,
    ) in rows:
        payload = source_sync_payload_from_dbos_inputs(
            deployment=deployment,
            inputs=inputs,
            serialization=serialization,
        )
        if payload is None:
            continue
        created_age_seconds = _dbos_workflow_age_seconds(created_raw)
        updated_age_seconds = _dbos_workflow_age_seconds(updated_raw)
        started_age_seconds = _dbos_workflow_age_seconds(started_raw)
        age_seconds = updated_age_seconds
        if age_seconds is None:
            age_seconds = created_age_seconds
        queue_dwell_seconds = created_age_seconds
        if (
            created_age_seconds is not None
            and started_age_seconds is not None
        ):
            queue_dwell_seconds = max(
                0.0,
                created_age_seconds - started_age_seconds,
            )
        stale = (
            age_seconds is not None
            and age_seconds >= int(stale_after_seconds)
        )
        base = {
            "workflow_id": str(workflow_uuid),
            "status": str(status).upper(),
            "age_seconds": age_seconds,
            "created_age_seconds": created_age_seconds,
            "updated_age_seconds": updated_age_seconds,
            "started_age_seconds": started_age_seconds,
            "workflow_lifetime_seconds": created_age_seconds,
            "queue_dwell_seconds": queue_dwell_seconds,
            "stale_after_seconds": int(stale_after_seconds),
            "stale": stale,
            "run_mode": payload.get("run_mode"),
            "publish": payload.get("publish"),
            "dependent_fanout": payload.get("dependent_fanout"),
            "include_predecessors": payload.get("include_predecessors"),
            "include_dependents": payload.get("include_dependents"),
            "event_count": len(payload.get("events") or []),
            "trigger_kinds": sorted({
                str(event.get("trigger_kind"))
                for event in payload.get("events") or []
                if isinstance(event, dict) and event.get("trigger_kind")
            }),
            "priority": priority,
        }
        for source in payload.get("sources") or ():
            source_name = str(source)
            candidate = dict(base)
            candidate["desired_frontier"] = source_sync_desired_frontier_state(
                source_name=source_name,
                run_mode=str(payload.get("run_mode") or ""),
                workflow_id=str(workflow_uuid),
                workflow_status=str(status).upper(),
                stale=stale,
                started_age_seconds=started_age_seconds,
            )
            candidates_by_source.setdefault(source_name, []).append(candidate)
    by_source: dict[str, dict[str, Any]] = {}
    for source_name, candidates in candidates_by_source.items():
        selected = dict(min(
            candidates,
            key=_source_sync_active_work_rank,
        ))
        selected["candidate_count"] = len(candidates)
        selected["selection_reason"] = _source_sync_active_work_selection_reason(
            selected,
            candidates,
        )
        selected["candidate_workflow_ids"] = [
            str(candidate.get("workflow_id")) for candidate in candidates
        ]
        selected["active_work_candidates"] = [
            _source_sync_active_work_candidate_summary(candidate)
            for candidate in candidates
        ]
        by_source[source_name] = selected
    return by_source


def _source_sync_active_work_rank(candidate: dict[str, Any]) -> tuple[Any, ...]:
    run_mode = str(candidate.get("run_mode") or "")
    completed = run_mode in COMPLETED_SCOPE_RUN_MODES
    stale = bool(candidate.get("stale"))
    if completed and not stale:
        semantic_rank = 0
    elif completed and stale:
        semantic_rank = 1
    elif not stale:
        semantic_rank = 2
    else:
        semantic_rank = 3
    status_rank = {
        "PENDING": 0,
        "ENQUEUED": 1,
        "DELAYED": 2,
    }.get(str(candidate.get("status") or "").upper(), 9)
    try:
        priority = int(candidate.get("priority"))
    except (TypeError, ValueError):
        priority = 100
    if priority <= 0:
        priority = 100
    age = candidate.get("age_seconds")
    if not isinstance(age, (int, float)):
        age = float("inf")
    return (
        semantic_rank,
        status_rank,
        priority,
        float(age),
        str(candidate.get("workflow_id") or ""),
    )


def _source_sync_active_work_selection_reason(
    selected: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> str:
    if len(candidates) == 1:
        return "single_active_workflow"
    run_mode = str(selected.get("run_mode") or "")
    if run_mode in COMPLETED_SCOPE_RUN_MODES and not selected.get("stale"):
        return "preferred_fresh_completed_scope"
    if run_mode in COMPLETED_SCOPE_RUN_MODES:
        return "preferred_stale_completed_scope"
    if not selected.get("stale"):
        return "preferred_fresh_incomplete_work"
    return "preferred_oldest_available_work"


def _source_sync_active_work_candidate_summary(
    candidate: dict[str, Any],
) -> dict[str, Any]:
    return {
        "workflow_id": candidate.get("workflow_id"),
        "status": candidate.get("status"),
        "run_mode": candidate.get("run_mode"),
        "publish": candidate.get("publish"),
        "dependent_fanout": candidate.get("dependent_fanout"),
        "trigger_kinds": list(candidate.get("trigger_kinds") or []),
        "stale": candidate.get("stale"),
        "age_seconds": candidate.get("age_seconds"),
        "workflow_lifetime_seconds": candidate.get(
            "workflow_lifetime_seconds",
        ),
        "queue_dwell_seconds": candidate.get("queue_dwell_seconds"),
        "priority": candidate.get("priority"),
        "desired_frontier": dict(candidate.get("desired_frontier") or {}),
    }


def _active_dbos_source_sync_workflow_rows(
    cur: psycopg.Cursor,
    *,
    queue_name: str,
) -> list[tuple[Any, ...]]:
    cur.execute("SELECT to_regclass('dbos.workflow_status')")
    if cur.fetchone()[0] is None:
        return []
    columns = _dbos_workflow_status_columns(cur)
    created_expr = _optional_dbos_workflow_column_expr(
        columns,
        "created_at",
        null_type="bigint",
    )
    updated_expr = _optional_dbos_workflow_column_expr(
        columns,
        "updated_at",
        null_type="bigint",
    )
    started_expr = _optional_dbos_workflow_column_expr(
        columns,
        "started_at_epoch_ms",
        null_type="bigint",
    )
    priority_expr = _optional_dbos_workflow_column_expr(
        columns,
        "priority",
        null_type="integer",
    )
    order_expr = (
        sql.Identifier("created_at")
        if "created_at" in columns else
        sql.Identifier("workflow_uuid")
    )
    cur.execute(
        sql.SQL(
            """
            SELECT workflow_uuid,
                   status,
                   inputs,
                   serialization,
                   {created_expr} AS created_raw,
                   {updated_expr} AS updated_raw,
                   {started_expr} AS started_raw,
                   {priority_expr} AS priority
              FROM dbos.workflow_status
             WHERE queue_name = %s
               AND name = 'okg_source_sync'
               AND status IN ('ENQUEUED', 'PENDING', 'DELAYED')
             ORDER BY {order_expr}, workflow_uuid
            """
        ).format(
            created_expr=created_expr,
            updated_expr=updated_expr,
            started_expr=started_expr,
            priority_expr=priority_expr,
            order_expr=order_expr,
        ),
        (queue_name,),
    )
    return list(cur.fetchall())


def _dbos_workflow_status_columns(cur: psycopg.Cursor) -> set[str]:
    cur.execute(
        """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = 'dbos'
           AND table_name = 'workflow_status'
        """
    )
    return {str(row[0]) for row in cur.fetchall()}


def _optional_dbos_workflow_column_expr(
    columns: set[str],
    column: str,
    *,
    null_type: str,
) -> sql.Composable:
    if column in columns:
        return sql.Identifier(column)
    return sql.SQL("NULL::{}").format(sql.SQL(null_type))


def _dbos_workflow_age_seconds(raw: Any) -> float | None:
    if raw is None:
        return None
    now = datetime.now(timezone.utc)
    if isinstance(raw, datetime):
        observed = raw if raw.tzinfo is not None else raw.replace(
            tzinfo=timezone.utc,
        )
        return max(0.0, (now - observed).total_seconds())
    try:
        timestamp = float(raw)
    except (TypeError, ValueError):
        return None
    if timestamp > 10_000_000_000:
        timestamp /= 1000.0
    return max(0.0, time.time() - timestamp)


def _source_sync_payload_from_dbos_inputs(
    *,
    deployment: str,
    inputs: str | None,
    serialization: str | None,
) -> dict[str, Any] | None:
    return source_sync_payload_from_dbos_inputs(
        deployment=deployment,
        inputs=inputs,
        serialization=serialization,
    )


def source_sync_payload_from_dbos_inputs(
    *,
    deployment: str,
    inputs: str | None,
    serialization: str | None,
) -> dict[str, Any] | None:
    """Decode DBOS workflow inputs for an ``okg_source_sync`` workflow.

    This is used by diagnostics and recovery tooling. It returns ``None`` when
    the DBOS payload cannot be decoded by the current SDK/schema version.
    """
    if inputs is None:
        return None
    try:
        from dbos._serialization import (  # type: ignore[import-untyped]
            DBOSDefaultSerializer,
            deserialize_args,
        )

        workflow_inputs = deserialize_args(
            inputs,
            serialization,
            DBOSDefaultSerializer,
        )
    except Exception:  # noqa: BLE001 — DBOS payload may have shifted between writer and reader versions; warn-log and skip this compaction tick.
        log.warning(
            "source-sync DBOS compaction could not deserialize inputs",
            exc_info=True,
        )
        return None

    if not isinstance(workflow_inputs, dict):
        return None
    args = tuple(workflow_inputs.get("args") or ())
    kwargs = dict(workflow_inputs.get("kwargs") or {})
    sources = kwargs.get("sources", args[0] if len(args) > 0 else None)
    run_mode = kwargs.get("run_mode", args[1] if len(args) > 1 else None)
    include_dependents = kwargs.get(
        "include_dependents",
        args[2] if len(args) > 2 else True,
    )
    dependent_fanout = kwargs.get(
        "dependent_fanout",
        args[3] if len(args) > 3 else None,
    )
    events = kwargs.get("events", args[4] if len(args) > 4 else ())
    publish = kwargs.get("publish", args[5] if len(args) > 5 else True)
    include_predecessors = kwargs.get(
        "include_predecessors",
        args[6] if len(args) > 6 else True,
    )
    profile_force_deep = kwargs.get(
        "profile_force_deep",
        args[7] if len(args) > 7 else False,
    )
    profile_collectors = kwargs.get(
        "profile_collectors",
        args[8] if len(args) > 8 else (),
    )
    supersedes_workflow_id = kwargs.get(
        "supersedes_workflow_id",
        args[9] if len(args) > 9 else None,
    )
    force_enrichers = kwargs.get(
        "force_enrichers",
        args[10] if len(args) > 10 else False,
    )
    workflow_telemetry = kwargs.get(
        "workflow_telemetry",
        args[11] if len(args) > 11 else None,
    )
    if supersedes_workflow_id is False or (
        isinstance(supersedes_workflow_id, str)
        and not supersedes_workflow_id.strip()
    ):
        # Pre-null writers encoded an absent optional supersession id as an
        # empty positional string or boolean false. Diagnostics must preserve
        # that historical compatibility without turning every retained
        # workflow into a traceback.
        supersedes_workflow_id = None
    try:
        payload = SourceSyncJob(
            deployment=deployment,
            sources=tuple(sources or ()),
            run_mode=run_mode,
            events=tuple(events or ()),
            include_predecessors=bool(include_predecessors),
            include_dependents=bool(include_dependents),
            dependent_fanout=dependent_fanout,
            publish=bool(publish),
            profile_force_deep=bool(profile_force_deep),
            profile_collectors=profile_collectors or (),
            supersedes_workflow_id=supersedes_workflow_id,
        ).to_dict()
        payload["force_enrichers"] = bool(force_enrichers)
        if kwargs.get("run_mode_explicit"):
            payload["run_mode_explicit"] = True
        if isinstance(workflow_telemetry, Mapping):
            payload["workflow_telemetry"] = dict(workflow_telemetry)
        return payload
    except Exception:  # noqa: BLE001 — workflow_inputs shape may be incompatible with current SourceSyncJob schema; warn-log and skip.
        log.warning(
            "source-sync DBOS compaction found invalid workflow inputs",
            exc_info=True,
        )
        return None


def _serialize_source_sync_dbos_args(
    *,
    args: tuple[Any, ...],
    serialization: str | None,
) -> tuple[str, str]:
    from dbos._serialization import (  # type: ignore[import-untyped]
        DBOSDefaultSerializer,
        DBOSPortableJSON,
        WorkflowSerializationFormat,
        serialize_args,
    )

    serialization_type = (
        WorkflowSerializationFormat.PORTABLE
        if serialization == DBOSPortableJSON.name()
        else WorkflowSerializationFormat.NATIVE
    )
    return serialize_args(
        args,
        {},
        serialization_type,
        DBOSDefaultSerializer,
    )


__all__ = [
    "active_dbos_source_sync_workflow_for_source",
    "active_dbos_source_sync_workflow_status_for_source",
    "active_dbos_source_sync_workflows_by_source",
    "enqueue_compacted_source_sync_via_dbos",
    "enqueue_source_sync_via_dbos",
    "source_sync_payload_from_dbos_inputs",
]
