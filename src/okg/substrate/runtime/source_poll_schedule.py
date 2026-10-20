"""DBOS-scheduled source poll workflows.

Part of ``migrate-source-sync-triggers-to-dbos`` (deepen-dbos-integration
Phase 0).

For each cadence represented in ``source_registry.yaml`` ``sync.poll``
blocks, the worker registers one ``@DBOS.scheduled`` workflow at boot.
That workflow calls ``run_poll_once(...)`` once per source in the group,
preserving per-source probe and backstop semantics while avoiding one
queued DBOS poll workflow per source per tick. When a poll detects a
change (cheap-probe fingerprint advanced), it enqueues an
``okg_source_sync`` workflow via DBOSClient (no more legacy ``okg.jobs``).

This is the "per-source poll" half of the proposal — paired with the
watcher's DBOS routing and the webhook receiver's DBOS routing, it
covers the three trigger types the substrate's source registry declares
but didn't auto-fire (``fs_event``, ``poll``, ``webhook``) so that
"freshness" lives at the substrate level, not at operator-typed
commands.

Two-step pattern mirrors :mod:`runtime.publisher_schedule`:

  1. :func:`register_source_poll_schedules` decorates one workflow per
     poll cadence group. Runs BEFORE ``DBOS.launch()``.
  2. :func:`apply_source_poll_schedules` calls ``DBOS.apply_schedules``
     for each registered workflow. Runs AFTER ``DBOS.launch()``.

Manifest override: grouped cron via
``runtime.schedules.okg_source_poll_interval_<N>s``. Existing per-source
overrides via ``runtime.schedules.okg_source_poll_<source_name>`` remain
supported by placing that source into its own override-specific group.
When unset, the cron is derived from the registry's
``sync.poll.interval_seconds``: sub-minute intervals → 6-field cron
``*/N * * * * *``; minute-or-longer intervals → 5-field cron
``*/N * * * *`` where N is minutes.
"""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from okg.substrate.source_sync import (
    source_sync_entry_is_explicitly_dormant,
)


__all__ = [
    "SOURCE_POLL_SCHEDULE_PREFIX",
    "apply_source_poll_schedules",
    "cleanup_unregistered_source_poll_schedules",
    "register_source_poll_schedules",
    "source_poll_deduplication_id",
    "source_poll_group_key",
    "source_poll_schedule_id",
    "source_poll_schedule_ids",
    "source_poll_workflow_name",
    "cron_from_interval_seconds",
]


log = logging.getLogger(__name__)


# Schedule + workflow names are prefixed with this constant so operators
# tell apart "this is the cron-fired enqueue scheduler" from "this is
# the worker that actually does the polling".
SOURCE_POLL_SCHEDULE_PREFIX = "okg_source_poll"


def source_poll_schedule_id(source_name: str) -> str:
    """Stable schedule id used in `dbos.workflow_schedules` and
    `runtime.schedules.<id>` manifest overrides."""
    return f"{SOURCE_POLL_SCHEDULE_PREFIX}_{source_name}"


def source_poll_workflow_name(source_name: str) -> str:
    """Stable workflow name observed in `dbos.workflow_status`."""
    return f"{SOURCE_POLL_SCHEDULE_PREFIX}_{source_name}"


def source_poll_group_key(
    *,
    interval_seconds: int,
    cron: str | None = None,
) -> str:
    """Stable group key for sources polled on the same cadence.

    The common case keeps a readable ``interval_300s`` suffix. A
    per-source cron override receives a short hash so it does not merge
    with the default-cadence group for the same interval.
    """
    interval = int(interval_seconds)
    base = f"interval_{interval}s"
    if cron is None or cron == cron_from_interval_seconds(interval):
        return base
    digest = hashlib.sha1(cron.encode("utf-8")).hexdigest()[:8]
    return f"{base}_{digest}"


def source_poll_deduplication_id(
    *,
    deployment: str,
    source_name: str,
    scheduled_time: datetime | None = None,
    bucket_seconds: int = 300,
) -> str:
    """Logical active-work key for one source-poll workflow."""
    base = f"source-poll:{deployment}:{source_name}"
    if scheduled_time is None:
        return base
    bucket = int(scheduled_time.timestamp() / max(bucket_seconds, 1))
    return f"{base}:{bucket}"


def _disabled_source_poll_schedule_map(
    *,
    existing_schedule_ids: set[str],
    registered_schedule_ids: set[str],
) -> dict[str, list[str]]:
    """Return stale source-poll schedule ids and emitted workflow names."""
    disabled: dict[str, list[str]] = {}
    prefix = f"{SOURCE_POLL_SCHEDULE_PREFIX}_"
    for schedule_id in sorted(existing_schedule_ids - registered_schedule_ids):
        if not schedule_id.startswith(prefix):
            continue
        source_name = schedule_id[len(prefix):]
        if not source_name:
            continue
        disabled[schedule_id] = [
            source_poll_workflow_name(source_name),
            f"{schedule_id}_enqueue",
        ]
    return disabled


def cron_from_interval_seconds(interval_seconds: int) -> str:
    """Pick a cron expression matching the polled interval.

    Sub-minute intervals (1..59 seconds) use the 6-field DBOS cron
    format ``*/N * * * * *`` so the seconds field is honored. Minute-
    or-longer intervals use a 5-field ``*/N * * * *`` where N is
    minutes. Operators can override per-source via manifest if these
    defaults aren't right for a given workload.
    """
    if interval_seconds < 1:
        raise ValueError(
            f"interval_seconds must be ≥ 1, got {interval_seconds}"
        )
    if interval_seconds < 60:
        return f"*/{interval_seconds} * * * * *"
    minutes = interval_seconds // 60
    if minutes >= 60:
        # Hourly+; fall back to 1-minute cron because DBOS's cron parser
        # may not accept very long intervals and missing a sweep is
        # cheaper than not polling. Operators with hour-scale polls
        # should override explicitly.
        return "*/1 * * * *"
    return f"*/{minutes} * * * *"


# Module-level handle cache so apply_source_poll_schedules can find
# the workflows the registration call constructed.
_DECORATED: dict[str, Any] = {}
# Map schedule_id → cron, so apply_source_poll_schedules knows the
# default cron without re-walking the registry.
_SCHEDULE_CRONS: dict[str, str] = {}


def _source_poll_groups(
    registry: Mapping[str, Any],
    *,
    schedule_overrides: Mapping[str, str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Build the pure schedule plan shared by apply and worker startup."""
    groups: dict[str, dict[str, Any]] = {}
    overrides = dict(schedule_overrides or {})
    for source_name, entry in sorted(registry.items()):
        if source_sync_entry_is_explicitly_dormant(entry):
            # Profile registries retain dormant setup candidates.  They are
            # not runnable until activation flips the explicit flag; polling
            # them would bypass that boundary and can hit an adapter whose
            # required credentials/configuration are intentionally absent.
            continue
        sync = getattr(entry, "sync", None)
        if sync is None or "poll" not in set(sync.triggers):
            continue
        if sync.poll is None or sync.poll.interval_seconds < 1:
            continue
        interval_seconds = int(sync.poll.interval_seconds)
        default_cron = cron_from_interval_seconds(interval_seconds)
        per_source_schedule_id = source_poll_schedule_id(source_name)
        group_key = source_poll_group_key(
            interval_seconds=interval_seconds,
            cron=overrides.get(per_source_schedule_id, default_cron),
        )
        group = groups.setdefault(
            group_key,
            {
                "cron": default_cron,
                "interval_seconds": interval_seconds,
                "sources": [],
            },
        )
        group["sources"].append(source_name)
        if per_source_schedule_id in overrides:
            group["cron"] = overrides[per_source_schedule_id]
    return groups


def source_poll_schedule_ids(
    registry: Mapping[str, Any],
    *,
    schedule_overrides: Mapping[str, str] | None = None,
) -> list[str]:
    """Return the exact durable poll-schedule ids for a registry/config."""
    return [
        source_poll_schedule_id(group_key)
        for group_key in sorted(
            _source_poll_groups(
                registry,
                schedule_overrides=schedule_overrides,
            )
        )
    ]


def register_source_poll_schedules(
    *,
    deployment: str,
    dsn: str,
    registry: dict[str, Any],
    backstop_multiplier: int = 5,
    backstop_dependent_fanout: str | None = None,
    schedule_overrides: Mapping[str, str] | None = None,
    source_registry_repo_root_override: Path | None = None,
) -> list[str]:
    """Decorate one ``@DBOS.scheduled``-enqueueing workflow per poll
    cadence group, plus its corresponding ``@DBOS.workflow`` that does
    the actual polling for that group.

    Called from the worker entry point AFTER ``DBOS()`` but BEFORE
    ``DBOS.launch()``. Returns the list of registered schedule ids for
    logging.

    Clears the module-level caches on entry so a fresh worker boot in
    the same Python process (e.g. test fixtures) doesn't pick up
    stale closures.
    """
    from dbos import DBOS

    _DECORATED.clear()
    _SCHEDULE_CRONS.clear()

    schedule_ids: list[str] = []
    overrides = dict(schedule_overrides or {})

    resolved_backstop_multiplier = max(int(backstop_multiplier), 1)
    resolved_backstop_dependent_fanout = (
        str(backstop_dependent_fanout)
        if backstop_dependent_fanout is not None
        else None
    )

    groups = _source_poll_groups(
        registry,
        schedule_overrides=overrides,
    )
    def _record_poll_heartbeats(source_names) -> None:
        from okg.substrate.source_sync_producers import (
            record_source_sync_producer_health,
        )

        for source_name in source_names:
            try:
                record_source_sync_producer_health(
                    dsn,
                    deployment=deployment,
                    source_name=source_name,
                    producer="poll",
                    enqueue_count=0,
                    error_count=0,
                    event_count=0,
                )
            except Exception:  # noqa: BLE001
                log.exception(
                    "failed to record source-poll schedule heartbeat: "
                    "deployment=%s source=%s",
                    deployment, source_name,
                )

    _record_poll_heartbeats(
        source_name
        for source_name, entry in sorted(registry.items())
        if not source_sync_entry_is_explicitly_dormant(entry)
        and getattr(entry, "sync", None) is not None
        and "poll" in set(entry.sync.triggers)
        and entry.sync.poll is not None
        and entry.sync.poll.interval_seconds >= 1
    )

    def _poll_source_group_body(
        *,
        scheduled_time: datetime | None,
        source_names: tuple[str, ...],
        group_key: str,
    ) -> dict:
        from okg.substrate.source_sync_producers import run_poll_once
        from okg.substrate.deployments import find_deployment_dir
        from okg.substrate.deployments.sync_root import (
            require_usable_source_configuration_authority,
        )
        from okg.substrate.sources.registry import (
            load_source_registry,
        )

        dep_dir = find_deployment_dir(deployment)
        if dep_dir is None:
            raise RuntimeError(
                f"deployment dir for {deployment!r} not found "
                f"at poll-workflow execution time"
            )
        canonical_source_root = (
            require_usable_source_configuration_authority(
                source_registry_repo_root_override,
                command="scheduled source poll",
            )
        )
        current_registry = load_source_registry(
            dep_dir / "source_registry.yaml",
            **(
                {
                    "repo_root_override": canonical_source_root
                }
                if canonical_source_root is not None
                else {}
            ),
        )
        absent_sources = tuple(
            name for name in source_names if name not in current_registry
        )
        dormant_sources = tuple(
            name
            for name in source_names
            if name in current_registry
            and source_sync_entry_is_explicitly_dormant(
                current_registry[name],
            )
        )
        active_sources = tuple(
            name
            for name in source_names
            if name in current_registry
            and name not in dormant_sources
        )
        group_registry = {
            name: current_registry[name] for name in active_sources
        }
        source_disagreements = [
            {
                "source_name": name,
                "reason": "absent_from_source_registry",
            }
            for name in absent_sources
        ]
        if source_disagreements:
            log.warning(
                "scheduled source poll registry disagreement: "
                "deployment=%s group=%s absent_sources=%s",
                deployment,
                group_key,
                absent_sources,
                extra={"source_disagreements": source_disagreements},
            )
        strict_document = any(
            getattr(entry, "admission_contract", None) == "strict_v1"
            for entry in current_registry.values()
        )
        outcome = run_poll_once(
            dsn,
            deployment=deployment,
            registry=(current_registry if strict_document else group_registry),
            **(
                {
                    "sources": tuple(group_registry),
                    "registry_path": dep_dir / "source_registry.yaml",
                    "registry_repo_root_override": canonical_source_root,
                }
                if strict_document else {}
            ),
            publish=True,
            # Always DBOS-route from this path — if the worker
            # is running, the deployment is runtime-enabled by
            # definition.
            backstop_multiplier=resolved_backstop_multiplier,
            backstop_dependent_fanout=(
                resolved_backstop_dependent_fanout
            ),
        )
        return {
            **outcome,
            "group": group_key,
            "scheduled_time": (
                scheduled_time.isoformat()
                if scheduled_time is not None else None
            ),
            "sources": list(source_names),
            "active_sources": list(active_sources),
            "dormant_sources": list(dormant_sources),
            "source_disagreements": source_disagreements,
        }

    for group_key, group in sorted(groups.items()):
        workflow_name = source_poll_workflow_name(group_key)
        schedule_id = source_poll_schedule_id(group_key)
        source_names = tuple(str(name) for name in group["sources"])
        interval_seconds = int(group["interval_seconds"])
        default_cron = str(group["cron"])

        @DBOS.workflow(name=workflow_name, max_recovery_attempts=10)
        def _poll_source_group(
            scheduled_time: datetime | None = None,
            context: Any | None = None,
            source_names: tuple[str, ...] = source_names,
            group_key: str = group_key,
        ) -> dict:
            """Run one poll cycle for each source in the bound group.

            Body imports the producer at call-time because the
            substrate's eager-import order is sensitive; we only need
            the function here when the schedule fires.

            A tick that arrives while this group's previous poll is still
            running is skipped and logged: on a large repository a busy
            worker otherwise started a new poll every tick, and none of
            them finished.
            """
            from okg.substrate.runtime.schedule_overlap import (
                OverlapSkipped,
                claim_schedule_run,
            )

            claim = claim_schedule_run(source_poll_schedule_id(group_key))
            try:
                claim.__enter__()
            except OverlapSkipped as skip:
                log.warning(
                    "source poll skipped: deployment=%s group=%s: %s",
                    deployment, group_key, skip,
                )
                # The producer is alive, only busy: keep its heartbeat fresh
                # so a long poll does not make every other source read stale.
                _record_poll_heartbeats(source_names)
                return {
                    "deployment": deployment,
                    "producer": "poll",
                    "status": "skipped_previous_run_active",
                    "reason": str(skip),
                    "skipped_runs": skip.skipped_count,
                    "group": group_key,
                    "scheduled_time": (
                        scheduled_time.isoformat()
                        if scheduled_time is not None else None
                    ),
                    "sources": list(source_names),
                    "results": [],
                    "errors": [],
                }
            try:
                return _poll_source_group_body(
                    scheduled_time=scheduled_time,
                    source_names=source_names,
                    group_key=group_key,
                )
            finally:
                claim.__exit__(None, None, None)

        # Stash both the worker-workflow handle (so DBOS knows about
        # it for recovery) and the schedule handle (same stable name)
        # so apply_schedules can wire it directly. Poll is a cheap
        # producer; the durable deployment-queue item is only the
        # source-sync workflow it enqueues when a source changed or a
        # backstop reconcile is due.
        _DECORATED[workflow_name] = _poll_source_group
        _DECORATED[schedule_id] = _poll_source_group
        _SCHEDULE_CRONS[schedule_id] = default_cron
        schedule_ids.append(schedule_id)
        log.info(
            "decorated source-poll schedule: deployment=%s group=%s "
            "sources=%s interval_seconds=%s cron=%r",
            deployment, group_key, source_names, interval_seconds,
            default_cron,
        )

    return schedule_ids


def cleanup_unregistered_source_poll_schedules(
    *,
    dsn: str,
    registered_schedule_ids: list[str],
) -> dict[str, dict[str, int]]:
    """Remove stale per-source poll schedules no longer in the registry.

    DBOS persists schedule rows across boots. If a source removes its
    ``sync.poll`` block, the old ``okg_source_poll_<source>`` row would
    otherwise keep firing even though this worker no longer registers
    that workflow.
    """
    from okg.substrate.db.connection import connect as db_connect

    prefix = f"{SOURCE_POLL_SCHEDULE_PREFIX}_%"
    with db_connect(dsn) as conn, \
            conn.cursor() as cur:
        cur.execute(
            "SELECT schedule_name FROM dbos.workflow_schedules "
            "WHERE schedule_name LIKE %s",
            (prefix,),
        )
        existing = {row[0] for row in cur.fetchall()}

    disabled = _disabled_source_poll_schedule_map(
        existing_schedule_ids=existing,
        registered_schedule_ids=set(registered_schedule_ids),
    )
    if not disabled:
        return {}

    from okg.substrate.runtime.services import (
        cleanup_disabled_feature_schedules,
    )
    return cleanup_disabled_feature_schedules(dsn=dsn, disabled=disabled)


def apply_source_poll_schedules(
    *,
    schedule_overrides: dict[str, str],
) -> list[str]:
    """Persist all registered per-source poll schedules via
    ``DBOS.apply_schedules``.

    Called AFTER ``DBOS.launch()``. Returns the list of applied
    schedule ids. Idempotent.

    Per-source override via
    ``runtime.schedules.okg_source_poll_<source_name>``.
    """
    from dbos import DBOS

    applied: list[str] = []
    for schedule_id, default_cron in _SCHEDULE_CRONS.items():
        cron = schedule_overrides.get(schedule_id, default_cron)
        enqueue_fn = _DECORATED.get(schedule_id)
        if enqueue_fn is None:
            log.warning(
                "skipping apply for %s — no decorated enqueue "
                "workflow registered", schedule_id,
            )
            continue
        DBOS.apply_schedules([
            {
                "schedule_name": schedule_id,
                "workflow_fn": enqueue_fn,
                "schedule": cron,
                "context": None,
                "automatic_backfill": False,
                "cron_timezone": None,
                "queue_name": None,
            },
        ])
        applied.append(schedule_id)
        log.info(
            "applied source-poll schedule: %s cron=%r",
            schedule_id, cron,
        )

    return applied
