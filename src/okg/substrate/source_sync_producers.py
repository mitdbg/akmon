"""Generic source-sync producers: webhook and poll."""
from __future__ import annotations

import base64
import binascii
import fnmatch
import hashlib
import hmac
import importlib
import json
import logging
import os
import stat
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import psycopg
from okg.substrate.db.connection import connect as db_connect
from psycopg import sql

from okg.substrate.errors import SubstrateError
from okg.substrate.ingest.adapter_factory import (
    ConstructedSourceAdapter,
    construct_source_adapter,
    construct_source_adapter_from_plan,
    plan_source_registry_bindings,
)
from okg.substrate.ingest.source_construction import (
    classify_source_construction_failure,
    evict_module_tree,
)
from okg.substrate.ingest.change_identity import identity_fingerprint
from okg.substrate.ingest.runner import PollProbeDecision, SourceRunner
from okg.substrate.library.sources.change_probe import CURSOR_TOKEN_KEY
from okg.substrate.runtime.partition_queue import source_sync_queue_name
from okg.substrate.source_sync import (
    SourceSyncCapabilities,
    SourceSyncEvent,
    SourceSyncJob,
    source_row_candidates,
    source_sync_entry_is_explicitly_dormant,
)
from okg.substrate.sources.registry import (
    STRICT_ADMISSION_CONTRACT,
    SourceRegistryAdmission,
    load_source_registry_admission,
)
from okg.substrate.watch.watcher import Watcher

from okg.substrate.source_sync_status import (
    DEFAULT_PRODUCER_HEALTH_MAX_AGE_SECONDS as DEFAULT_PRODUCER_HEALTH_MAX_AGE_SECONDS,
    SOURCE_SYNC_STATUS_SCHEMA as SOURCE_SYNC_STATUS_SCHEMA,
    _SOURCE_SYNC_STALE_BACKLOG_SECONDS as _SOURCE_SYNC_STALE_BACKLOG_SECONDS,
    _age_seconds as _age_seconds,
    _expected_source_sync_producers as _expected_source_sync_producers,
    _producer_observed_at as _producer_observed_at,
    build_source_sync_status_contract as build_source_sync_status_contract,
    expected_source_sync_producers as expected_source_sync_producers,
    source_sync_status_for_source as source_sync_status_for_source,
)

_SOURCE_SYNC_STATUS_COMPATIBILITY_EXPORTS = frozenset({
    "_append_expected_or_disabled",
    "_desired_frontiers_contract",
    "_list_of_mappings",
    "_optional_number",
    "_parse_datetime",
    "_producer_kind_contract",
    "_producer_liveness_impact",
    "_producer_row_is_required",
    "_readiness_required_source_names",
    "_rollup_status",
    "_runtime_liveness_contract",
    "_runtime_producer_enabled",
    "_runtime_service_down",
    "_runtime_service_status",
    "_runtime_service_view",
    "_runtime_worker_contract",
    "_source_freshness_contract",
    "_source_freshness_for_source",
    "source_sync_producer_expectations",
})


def __getattr__(name: str) -> Any:
    """Resolve moved status helpers as exact owner identities."""
    if name not in _SOURCE_SYNC_STATUS_COMPATIBILITY_EXPORTS:
        raise AttributeError(
            f"module {__name__!r} has no attribute {name!r}"
        )
    value = getattr(
        importlib.import_module("okg.substrate.source_sync_status"),
        name,
    )
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Expose lazy compatibility names to API introspection."""
    return sorted(
        set(globals())
        | set(__all__)
        | _SOURCE_SYNC_STATUS_COMPATIBILITY_EXPORTS
    )

_DBOS_ACTIVE_QUEUE_STATUSES = {"ENQUEUED", "PENDING", "DELAYED"}
_DBOS_FAILED_TERMINAL_STATUSES = {
    "CANCELLED",
    "ERROR",
    "FAILED",
    "MAX_RECOVERY_ATTEMPTS",
    "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
}
_DBOS_CURRENT_VERSION_PRIMARY_STATUSES = (
    _DBOS_ACTIVE_QUEUE_STATUSES
    | {"SUCCESS"}
    | (_DBOS_FAILED_TERMINAL_STATUSES - {"CANCELLED"})
)
_DBOS_CURRENT_VERSION_FALLBACK_STATUSES = {"CANCELLED"}
_DBOS_RECOVERABLE_TERMINAL_ERROR_MARKERS = (
    b"DBOSAwaitedWorkflowCancelledError",
    b"DBOSWorkflowCancelledError",
    b"source-sync publish work is active",
)
log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProducerEnqueueResult:
    source_name: str
    workflow_id: str
    dedupe_key: str
    event_count: int
    run_mode: str
    dependent_fanout: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_name": self.source_name,
            "workflow_id": self.workflow_id,
            "dedupe_key": self.dedupe_key,
            "event_count": self.event_count,
            "run_mode": self.run_mode,
            "dependent_fanout": self.dependent_fanout,
        }


@dataclass(frozen=True)
class PollSuppressedObservation:
    """Exact poll observation delegated to one active watch workflow."""

    workflow_id: str
    revision_hint: str
    effective_token: Mapping[str, Any]
    observed_at: datetime


@dataclass
class WebhookServerStats:
    request_count: int = 0
    enqueue_count: int = 0
    error_count: int = 0
    last_result: dict[str, Any] | None = None
    last_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_count": self.request_count,
            "enqueue_count": self.enqueue_count,
            "error_count": self.error_count,
            "last_result": self.last_result,
            "last_error": self.last_error,
        }


def make_webhook_http_server(
    dsn: str,
    *,
    deployment: str,
    registry: Mapping[str, Any],
    host: str = "127.0.0.1",
    port: int = 8088,
    publish: bool = True,
) -> tuple[ThreadingHTTPServer, WebhookServerStats]:
    """Build a small HTTP webhook receiver for declared webhook paths."""
    stats = WebhookServerStats()
    path_to_source = _webhook_path_map(registry)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - stdlib callback name
            if self.path != "/__okg/health":
                self._send_json(404, {"error": "unknown webhook path"})
                return
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "embedded_webhook",
                    "deployment": deployment,
                    "pid": os.getpid(),
                },
            )

        def do_POST(self) -> None:  # noqa: N802 - stdlib callback name
            stats.request_count += 1
            source_name = path_to_source.get(self.path)
            if source_name is None:
                self._send_json(404, {"error": "unknown webhook path"})
                return
            length = int(self.headers.get("Content-Length") or "0")
            body = self.rfile.read(length)
            try:
                payload = json.loads(body.decode("utf-8") or "{}")
                if not isinstance(payload, dict):
                    raise ValueError("payload must be a JSON object")
                result = handle_webhook_payload(
                    dsn,
                    deployment=deployment,
                    registry=registry,
                    source_name=source_name,
                    payload=payload,
                    body=body,
                    headers={
                        key: value for key, value in self.headers.items()
                    },
                    publish=publish,
                )
                stats.enqueue_count += 1
                stats.last_result = result
                stats.last_error = None
                self._send_json(202, result)
            except Exception as exc:
                stats.error_count += 1
                stats.last_error = f"{type(exc).__name__}: {exc}"
                code = getattr(exc, "code", "source_sync_webhook_failed")
                self._send_json(400, {
                    "error": str(exc),
                    "code": code,
                })

        def log_message(self, _format: str, *_args: Any) -> None:
            return

        def _send_json(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer((host, int(port)), Handler)
    return server, stats


def run_webhook_http_server(
    dsn: str,
    *,
    deployment: str,
    registry: Mapping[str, Any],
    host: str = "127.0.0.1",
    port: int = 8088,
    max_requests: int | None = None,
    publish: bool = True,
) -> dict[str, Any]:
    server, stats = make_webhook_http_server(
        dsn,
        deployment=deployment,
        registry=registry,
        host=host,
        port=port,
        publish=publish,
    )
    try:
        if max_requests is None:
            server.serve_forever()
        else:
            for _ in range(max_requests):
                server.handle_request()
    finally:
        server.server_close()
    return stats.to_dict()


def enqueue_events_for_source(
    dsn: str,
    *,
    deployment: str,
    source_name: str,
    sync: SourceSyncCapabilities,
    events: tuple[SourceSyncEvent, ...],
    producer: str,
    publish: bool = True,
    run_mode: str | None = None,
    dependent_fanout: str | None = None,
) -> ProducerEnqueueResult:
    """Enqueue one registry source's sync events as a DBOS workflow."""
    run_mode = run_mode or sync.default_event_mode
    fanout = dependent_fanout or sync.dependents.on_event
    window = sync.compaction.window_seconds if sync.compaction else 0
    bucket = int(time.time() / max(window or 1, 1))
    source_key = source_name
    if (
        producer == "poll"
        and sync.compaction is not None
        and sync.compaction.window_seconds > 0
    ):
        source_key = "_poll_batch"
    dedupe_key = (
        f"sync:{deployment}:{producer}:{run_mode}:"
        f"{source_key}:{fanout}:{bucket}"
    )
    job = SourceSyncJob(
        deployment=deployment,
        sources=(source_name,),
        run_mode=run_mode,
        events=events,
        dependent_fanout=fanout,
        publish=publish,
    )
    from okg.substrate.source_sync_enqueue import (
        enqueue_compacted_source_sync_via_dbos,
    )
    workflow_id = enqueue_compacted_source_sync_via_dbos(
        deployment=deployment,
        dsn=dsn,
        job=job,
        deduplication_id=dedupe_key if producer == "poll" else None,
        compaction=sync.compaction,
    )
    return ProducerEnqueueResult(
        source_name=source_name,
        workflow_id=str(workflow_id),
        dedupe_key=dedupe_key,
        event_count=len(events),
        run_mode=run_mode,
        dependent_fanout=fanout,
    )


def webhook_events_from_payload(
    *,
    deployment: str,
    source_name: str,
    payload: Mapping[str, Any],
) -> tuple[SourceSyncEvent, ...]:
    """Map common webhook payload shapes to source sync events."""
    path_events = _payload_path_events(payload)
    revision_hint = _optional_payload_str(payload, "revision_hint")
    if revision_hint is None:
        revision_hint = _optional_payload_str(payload, "after")
    event_id = _optional_payload_str(payload, "event_id")
    default_reason = _optional_payload_str(payload, "reason") or "webhook"
    observed_at = datetime.now(timezone.utc).isoformat()
    if not path_events:
        path_events = ((None, default_reason),)
    return tuple(
        SourceSyncEvent(
            deployment=deployment,
            source_name=source_name,
            trigger_kind="webhook",
            scope_key=scope_key,
            revision_hint=revision_hint,
            event_id=event_id,
            observed_at=observed_at,
            reason=reason,
        )
        for scope_key, reason in path_events
    )


def handle_webhook_payload(
    dsn: str,
    *,
    deployment: str,
    registry: Mapping[str, Any],
    source_name: str,
    payload: Mapping[str, Any],
    body: bytes | None = None,
    headers: Mapping[str, str] | None = None,
    publish: bool = True,
) -> dict[str, Any]:
    """Validate a declared webhook payload and enqueue source sync."""
    entry = registry.get(source_name)
    if entry is None:
        raise _producer_error(
            "source_sync_source_not_found",
            f"source {source_name!r} not found",
        )
    sync = getattr(entry, "sync", None)
    if sync is None or "webhook" not in set(sync.triggers):
        raise _producer_error(
            "source_sync_webhook_not_declared",
            f"source {source_name!r} does not declare webhook sync",
        )
    _verify_webhook_signature(
        sync,
        body=body,
        headers=headers or {},
    )
    events = webhook_events_from_payload(
        deployment=deployment,
        source_name=source_name,
        payload=payload,
    )
    run_mode = sync.default_event_mode
    fanout = sync.dependents.on_event
    has_delete = any(
        event.reason in {"webhook:removed", "webhook:deleted"}
        for event in events
    )
    if has_delete and sync.scoped_delete_mode is not None:
        run_mode = sync.scoped_delete_mode
        fanout = (
            sync.scoped_delete_dependent_fanout
            or sync.dependents.on_event
        )
    elif sync.deletes_require_reconcile and has_delete:
        run_mode = sync.reconcile_mode or "scope_complete"
        fanout = sync.dependents.on_reconcile
        events = tuple(
            replace(event, trigger_kind="reconcile") for event in events
        )
    result = enqueue_events_for_source(
        dsn,
        deployment=deployment,
        source_name=source_name,
        sync=sync,
        events=events,
        producer="webhook",
        publish=publish,
        run_mode=run_mode,
        dependent_fanout=fanout,
    )
    record_source_sync_producer_health(
        dsn,
        deployment=deployment,
        source_name=source_name,
        producer="webhook",
        enqueue_count=1,
        error_count=0,
        event_count=len(events),
    )
    return {
        "deployment": deployment,
        "producer": "webhook",
        "result": result.to_dict(),
    }


def run_poll_once(
    dsn: str,
    *,
    deployment: str,
    registry: Mapping[str, Any] | SourceRegistryAdmission,
    source: str | None = None,
    sources: Collection[str] | None = None,
    registry_path: Path | None = None,
    registry_repo_root_override: Path | str | None = None,
    publish: bool = True,
    backstop_multiplier: int = 5,
    backstop_dependent_fanout: str | None = None,
) -> dict[str, Any]:
    """Reuse the runner probe/cursor/eligibility contract before enqueue."""
    if source is not None and sources is not None:
        raise ValueError("source and sources are mutually exclusive")
    admission = (
        registry if isinstance(registry, SourceRegistryAdmission) else None
    )
    if admission is None and any(
        getattr(entry, "admission_contract", None)
        == STRICT_ADMISSION_CONTRACT
        for entry in registry.values()
    ):
        # File loading already crossed strict schema/semantic admission. Keep
        # the complete loaded mapping so grouped schedules still plan every
        # entry before constructing their selected poll adapters.
        admission = SourceRegistryAdmission(
            contract=STRICT_ADMISSION_CONTRACT,
            entries=registry,
            registry_path=Path("<admitted-source-registry>"),
        )
    entries = admission.entries if admission is not None else registry
    selected_names = (
        {source}
        if source is not None else
        set(sources) if sources is not None else
        set(entries)
    )
    def reselect_poll_names(
        fresh_entries: Mapping[str, Any],
    ) -> Collection[str]:
        if source is not None:
            return {source}
        if sources is not None:
            return set(sources)
        return set(fresh_entries)

    strict_constructed: dict[str, ConstructedSourceAdapter] = {}
    if admission is not None and admission.strict:
        admission, strict_constructed = _construct_strict_poll_bindings(
            admission=admission,
            dsn=dsn,
            deployment=deployment,
            selected_names=selected_names,
            registry_path=registry_path,
            repo_root_override=registry_repo_root_override,
            reselect_names=reselect_poll_names,
        )
        entries = admission.entries
        selected_names = set(reselect_poll_names(entries))
    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for source_name, entry in sorted(entries.items()):
        if source_sync_entry_is_explicitly_dormant(entry):
            continue
        if source_name not in selected_names:
            continue
        sync = getattr(entry, "sync", None)
        if sync is None or "poll" not in set(sync.triggers):
            continue
        ownership_id = getattr(entry, "ownership_id", None)
        probe = "missing"
        backstop: dict[str, Any] = {"due": True, "reason": "status_error"}
        backstop_due = True
        try:
            probe = _probe_name(sync)
            backstop = _poll_backstop_reconcile_due(
                dsn,
                deployment=deployment,
                source_name=source_name,
                ownership_id=ownership_id,
                interval_seconds=sync.poll.interval_seconds,
                backstop_multiplier=backstop_multiplier,
            )
            backstop_due = bool(backstop["due"])
            if probe == "always_changed":
                decision = PollProbeDecision(
                    enqueue=True,
                    changed=True,
                    reason="always_changed",
                    revision_hint=(
                        "always_changed:"
                        + identity_fingerprint({
                            "deployment": deployment,
                            "source": source_name,
                        })
                    ),
                )
            elif probe == "change_probe":
                constructed = strict_constructed.get(source_name)
                if constructed is None:
                    constructed = construct_source_adapter(
                        entry,
                        dsn=dsn,
                        deployment=deployment,
                    )
                decision = SourceRunner(
                    dsn,
                    source_name=entry.ownership_id,
                    deployment_name=deployment,
                    graph_branch=constructed.graph_branch,
                    **(
                        {
                            "executable_profile": (
                                constructed.executable_profile
                            ),
                            "registry_name": constructed.registry_name,
                            "ownership_id": constructed.ownership_id,
                        }
                        if admission is not None and admission.strict
                        else {}
                    ),
                    registry_binding_fingerprint=(
                        constructed.registry_binding_fingerprint
                    ),
                ).evaluate_poll(
                    constructed.adapter,
                    mode=sync.default_event_mode,
                    reconcile_due=backstop_due,
                )
            else:
                raise _producer_error(
                    "source_sync_poll_probe_unknown",
                    f"unsupported poll probe {probe!r}",
                )

            prior_poll_fingerprint, prior_poll_observed_at = (
                _read_poll_fingerprint_state(
                    dsn,
                    deployment=deployment,
                    source_name=source_name,
                    probe=probe,
                )
            )
            suppressed_observation = _read_poll_suppressed_observation(
                dsn,
                deployment=deployment,
                source_name=source_name,
                probe=probe,
            )
            suppressed_observation_confirmed = False
            if (
                not decision.enqueue
                and probe == "change_probe"
                and decision.effective_token is not None
                and suppressed_observation is not None
            ):
                suppressed_observation_confirmed = (
                    _watch_backed_poll_change_already_ingested(
                        dsn,
                        deployment=deployment,
                        source_name=source_name,
                        ownership_id=ownership_id,
                        sync=sync,
                        suppressed_observation=suppressed_observation,
                        current_revision_hint=decision.revision_hint,
                        current_effective_token=decision.effective_token,
                    )
                )
                if not suppressed_observation_confirmed:
                    decision = replace(
                        decision,
                        enqueue=True,
                        changed=True,
                        reason="suppressed_watch_unconfirmed",
                    )
            active_watch = None
            if decision.enqueue and decision.changed and not backstop_due:
                active_watch = _watch_backed_poll_change_in_flight(
                    dsn,
                    deployment=deployment,
                    source_name=source_name,
                    sync=sync,
                    publish=publish,
                )
            if active_watch is not None:
                if (
                    probe == "change_probe"
                    and decision.effective_token is not None
                ):
                    _write_poll_suppressed_observation(
                        dsn,
                        deployment=deployment,
                        source_name=source_name,
                        probe=probe,
                        observation={
                            "workflow_id": active_watch["workflow_id"],
                            "revision_hint": decision.revision_hint,
                            "effective_token": decision.effective_token,
                        },
                    )
                results.append({
                    "source_name": source_name,
                    "changed": True,
                    "backstop_reconcile": None,
                    "probe": probe,
                    "decision_reason": decision.reason,
                    "enqueue_suppressed": {
                        "reason": "watch_backed_active_source_sync",
                        **active_watch,
                    },
                })
                _record_producer_health(
                    dsn,
                    deployment=deployment,
                    source_name=source_name,
                    producer="poll",
                    enqueue_count=0,
                    error_count=0,
                )
                continue

            if (
                decision.enqueue
                and decision.changed
                and not backstop_due
                and probe == "change_probe"
                and decision.effective_token is not None
                and prior_poll_fingerprint is not None
                and suppressed_observation is not None
                and _watch_backed_poll_change_already_ingested(
                    dsn,
                    deployment=deployment,
                    source_name=source_name,
                    ownership_id=ownership_id,
                    sync=sync,
                    suppressed_observation=suppressed_observation,
                    current_revision_hint=decision.revision_hint,
                    current_effective_token=decision.effective_token,
                )
            ):
                _write_poll_fingerprint(
                    dsn,
                    deployment=deployment,
                    source_name=source_name,
                    ownership_id=ownership_id,
                    probe=probe,
                    fingerprint=decision.revision_hint,
                    effective_token=decision.effective_token,
                )
                results.append({
                    "source_name": source_name,
                    "changed": False,
                    "backstop_reconcile": None,
                    "probe": probe,
                    "decision_reason": decision.reason,
                    "poll_baseline_advanced": {
                        "reason": "watch_backed_source_run",
                        "previous_fingerprint_observed_at": (
                            prior_poll_observed_at.isoformat()
                            if prior_poll_observed_at is not None
                            else None
                        ),
                    },
                })
                _record_producer_health(
                    dsn,
                    deployment=deployment,
                    source_name=source_name,
                    producer="poll",
                    enqueue_count=0,
                    error_count=0,
                )
                continue

            if decision.enqueue:
                trigger_kind = "reconcile" if backstop_due else "poll"
                run_mode = (
                    (sync.reconcile_mode or "scope_complete")
                    if backstop_due
                    else sync.default_event_mode
                )
                fanout = (
                    (
                        backstop_dependent_fanout
                        if backstop_dependent_fanout is not None
                        else sync.dependents.on_reconcile
                    )
                    if backstop_due
                    else sync.dependents.on_event
                )
                reason = (
                    (
                        f"poll_backstop:{probe}:{backstop['reason']}:changed"
                        if decision.changed
                        else f"poll_backstop:{probe}:{backstop['reason']}"
                    )
                    if backstop_due
                    else f"poll:{probe}:{decision.reason}"
                )
                event = SourceSyncEvent(
                    deployment=deployment,
                    source_name=source_name,
                    trigger_kind=trigger_kind,
                    revision_hint=decision.revision_hint,
                    observed_at=datetime.now(timezone.utc).isoformat(),
                    reason=reason,
                )
                enqueue_result = enqueue_events_for_source(
                    dsn,
                    deployment=deployment,
                    source_name=source_name,
                    sync=sync,
                    events=(event,),
                    producer="poll",
                    publish=publish,
                    run_mode=run_mode,
                    dependent_fanout=fanout,
                )
                if suppressed_observation is not None:
                    _write_poll_suppressed_observation(
                        dsn,
                        deployment=deployment,
                        source_name=source_name,
                        probe=probe,
                        observation=None,
                    )
                results.append({
                    "source_name": source_name,
                    "changed": decision.changed,
                    "backstop_reconcile": backstop if backstop_due else None,
                    "probe": probe,
                    "decision_reason": decision.reason,
                    "enqueue": enqueue_result.to_dict(),
                })
            else:
                _write_poll_fingerprint(
                    dsn,
                    deployment=deployment,
                    source_name=source_name,
                    ownership_id=ownership_id,
                    probe=probe,
                    fingerprint=decision.revision_hint,
                )
                results.append({
                    "source_name": source_name,
                    "changed": False,
                    "backstop_reconcile": None,
                    "probe": probe,
                    "decision_reason": decision.reason,
                    **(
                        {
                            "poll_baseline_advanced": {
                                "reason": "watch_backed_source_run",
                                "previous_fingerprint_observed_at": (
                                    prior_poll_observed_at.isoformat()
                                    if prior_poll_observed_at is not None
                                    else None
                                ),
                            },
                        }
                        if suppressed_observation_confirmed
                        else {}
                    ),
                })
            _record_producer_health(
                dsn,
                deployment=deployment,
                source_name=source_name,
                producer="poll",
                enqueue_count=1 if decision.enqueue else 0,
                error_count=0,
            )
        except Exception as exc:
            # Probe/import/construction/cursor/content failures are wakeups,
            # never zero-enqueue defaults. Persist only a stable category; do
            # not expose exception messages or token contents.
            category = type(exc).__name__
            try:
                event = SourceSyncEvent(
                    deployment=deployment,
                    source_name=source_name,
                    trigger_kind=("reconcile" if backstop_due else "poll"),
                    revision_hint=(
                        "change_probe_error:"
                        + identity_fingerprint({
                            "deployment": deployment,
                            "source": source_name,
                            "category": category,
                        })
                    ),
                    observed_at=datetime.now(timezone.utc).isoformat(),
                    reason=f"poll:{probe}:error:{category}",
                )
                enqueue_result = enqueue_events_for_source(
                    dsn,
                    deployment=deployment,
                    source_name=source_name,
                    sync=sync,
                    events=(event,),
                    producer="poll",
                    publish=publish,
                    run_mode=(
                        (sync.reconcile_mode or "scope_complete")
                        if backstop_due else sync.default_event_mode
                    ),
                    dependent_fanout=(
                        (
                            backstop_dependent_fanout
                            if backstop_dependent_fanout is not None
                            else sync.dependents.on_reconcile
                        )
                        if backstop_due else sync.dependents.on_event
                    ),
                )
                results.append({
                    "source_name": source_name,
                    "changed": True,
                    "backstop_reconcile": (
                        backstop if backstop_due else None
                    ),
                    "probe": probe,
                    "decision_reason": f"error:{category}",
                    "enqueue": enqueue_result.to_dict(),
                })
                _record_producer_health(
                    dsn,
                    deployment=deployment,
                    source_name=source_name,
                    producer="poll",
                    enqueue_count=1,
                    error_count=1,
                )
            except Exception as enqueue_exc:
                enqueue_category = type(enqueue_exc).__name__
                errors.append({
                    "source_name": source_name,
                    "error_category": category,
                    "enqueue_error_category": enqueue_category,
                })
                _record_producer_health(
                    dsn,
                    deployment=deployment,
                    source_name=source_name,
                    producer="poll",
                    enqueue_count=0,
                    error_count=1,
                )
    return {
        "deployment": deployment,
        "producer": "poll",
        "results": results,
        "errors": errors,
    }


def _construct_strict_poll_bindings(
    *,
    admission: SourceRegistryAdmission,
    dsn: str,
    deployment: str,
    selected_names: Collection[str],
    registry_path: Path | None,
    repo_root_override: Path | str | None,
    reselect_names: Callable[[Mapping[str, Any]], Collection[str]],
    allow_stale_retry: bool = True,
) -> tuple[SourceRegistryAdmission, dict[str, ConstructedSourceAdapter]]:
    """Cross the complete strict plan/constructor barrier before poll I/O."""
    try:
        plans = plan_source_registry_bindings(
            admission,
            dsn=dsn,
            deployment=deployment,
        ).plans
    except (AttributeError, ImportError, TypeError, ValueError):
        retry_path = registry_path or admission.registry_path
        if not allow_stale_retry or not retry_path.is_file():
            raise
        fresh = _reload_strict_poll_admission(
            admission=admission,
            registry_path=retry_path,
            repo_root_override=repo_root_override,
        )
        return _construct_strict_poll_bindings(
            admission=fresh,
            dsn=dsn,
            deployment=deployment,
            selected_names=reselect_names(fresh.entries),
            registry_path=retry_path,
            repo_root_override=repo_root_override,
            reselect_names=reselect_names,
            allow_stale_retry=False,
        )
    constructed: dict[str, ConstructedSourceAdapter] = {}
    for source_name in sorted(selected_names):
        entry = admission.entries.get(source_name)
        if entry is None or source_sync_entry_is_explicitly_dormant(entry):
            continue
        sync = entry.sync
        if (
            sync is None
            or "poll" not in set(sync.triggers)
        ):
            continue
        plan = plans[source_name]
        try:
            constructed[source_name] = construct_source_adapter_from_plan(plan)
        except TypeError as exc:
            diagnosis = classify_source_construction_failure(
                exc=exc,
                stage="construct",
                module_name=str(entry.raw.get("module") or ""),
                class_name=str(entry.raw.get("class") or ""),
                params=plan.init_params,
            )
            retry_path = registry_path or admission.registry_path
            if (
                not allow_stale_retry
                or diagnosis is None
                or diagnosis.get("cause") != "stale_loaded_code"
                or not retry_path.is_file()
            ):
                raise
            fresh = _reload_strict_poll_admission(
                admission=admission,
                registry_path=retry_path,
                repo_root_override=repo_root_override,
            )
            return _construct_strict_poll_bindings(
                admission=fresh,
                dsn=dsn,
                deployment=deployment,
                selected_names=reselect_names(fresh.entries),
                registry_path=retry_path,
                repo_root_override=repo_root_override,
                reselect_names=reselect_names,
                allow_stale_retry=False,
            )
    return admission, constructed


def _reload_strict_poll_admission(
    *,
    admission: SourceRegistryAdmission,
    registry_path: Path,
    repo_root_override: Path | str | None,
) -> SourceRegistryAdmission:
    """Reload one strict poll document after evicting every planned class."""
    from okg.substrate.deployments import deployment_module_import_lock

    with deployment_module_import_lock():
        for entry in admission.entries.values():
            module_name = str(entry.raw.get("module") or "")
            if module_name:
                evict_module_tree(module_name)
        importlib.invalidate_caches()
        return load_source_registry_admission(
            registry_path,
            repo_root_override=repo_root_override,
            force_reload=True,
        )


def source_sync_producer_health(
    dsn: str,
    *,
    deployment: str,
    registry: Mapping[str, Any] | None = None,
    max_producer_age_seconds: int = DEFAULT_PRODUCER_HEALTH_MAX_AGE_SECONDS,
    runtime_source_sync: Mapping[str, Any] | None = None,
    runtime_enabled: bool | None = None,
    readiness_profiles: Sequence[str] | None = None,
    readiness_source_groups: Mapping[str, Sequence[str]] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT source_name, key, value, updated_at
              FROM okg.source_progress
             WHERE deployment = %s
               AND key LIKE 'sync.producer.%%'
             ORDER BY updated_at DESC, source_name, key
            """,
            (deployment,),
        )
        rows = cur.fetchall()
        dbos_queue = _source_sync_dbos_queue_status(cur, deployment=deployment)
    producers = []
    for source_name, key, value, updated_at in rows:
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            parsed = {"raw": value}
        observed_at = _producer_observed_at(parsed, updated_at)
        age_seconds = _age_seconds(now, observed_at)
        producers.append({
            "source_name": source_name,
            "producer": key.removeprefix("sync.producer."),
            "updated_at": updated_at.isoformat(),
            "age_seconds": age_seconds,
            **parsed,
        })
    expected, disabled, config_warnings = _expected_source_sync_producers(
        registry or {},
        max_producer_age_seconds=max_producer_age_seconds,
        runtime_source_sync=runtime_source_sync,
    )
    source_convergence_inventory: list[dict[str, Any]] = []
    active_work_by_source: dict[str, Any] = {}
    try:
        from okg.substrate.source_sync_convergence import (
            source_inventory_for_registry,
        )

        source_convergence_inventory = source_inventory_for_registry(
            deployment=deployment,
            registry=registry or {},
            runtime_source_sync=runtime_source_sync,
            runtime_enabled=bool(runtime_enabled),
            readiness_profiles=readiness_profiles,
            readiness_source_groups=readiness_source_groups,
        )
    except Exception:  # noqa: BLE001 - health must remain readable.
        source_convergence_inventory = []
    try:
        from okg.substrate.source_sync_enqueue import (
            active_dbos_source_sync_workflows_by_source,
        )

        active_work_by_source = active_dbos_source_sync_workflows_by_source(
            dsn=dsn,
            deployment=deployment,
        )
    except Exception:  # noqa: BLE001 - convergence reports absence explicitly.
        active_work_by_source = {}
    observed = {
        (str(row["source_name"]), str(row["producer"])): row
        for row in producers
    }
    expected_rows: list[dict[str, Any]] = []
    missing: list[dict[str, Any]] = []
    stale: list[dict[str, Any]] = []
    for item in expected:
        key = (str(item["source_name"]), str(item["producer"]))
        row = observed.get(key)
        status = "healthy"
        detail = dict(item)
        if row is None:
            status = "missing"
            missing.append(detail)
        else:
            age = row.get("age_seconds")
            detail["observed"] = {
                "updated_at": row.get("updated_at"),
                "last_run_at": row.get("last_run_at"),
                "last_heartbeat_at": row.get("last_heartbeat_at"),
                "last_event_at": row.get("last_event_at"),
                "last_enqueue_at": row.get("last_enqueue_at"),
                "age_seconds": age,
                "enqueue_count": row.get("enqueue_count"),
                "error_count": row.get("error_count"),
            }
            if isinstance(age, (int, float)) and age > item["max_age_seconds"]:
                status = "stale"
                stale.append(detail)
        detail["status"] = status
        expected_rows.append(detail)
    runtime_services_health = _runtime_services_health_status(
        dsn,
        deployment=deployment,
    )
    producer_liveness_ok = not missing and not stale
    result = {
        "deployment": deployment,
        "ok": producer_liveness_ok,
        "producer_liveness_ok": producer_liveness_ok,
        "producer_count": len(producers),
        "expected_producer_count": len(expected_rows),
        "disabled_producer_count": len(disabled),
        "producers": producers,
        "expected_producers": expected_rows,
        "disabled_producers": disabled,
        "missing_producers": missing,
        "stale_producers": stale,
        "config_warnings": config_warnings,
        "queue_depth": [],
        "dbos_queue_depth": dbos_queue,
        "dlq_count": (
            dbos_queue.get("failed_terminal_count")
            if dbos_queue.get("available") is True else None
        ),
        "dlq_status": (
            "known" if dbos_queue.get("available") is True else "unknown"
        ),
        "runtime_services_health": runtime_services_health,
        "source_convergence_inventory": source_convergence_inventory,
        "active_source_sync_workflows": active_work_by_source,
    }
    result["source_sync_status"] = build_source_sync_status_contract(result)
    result["ok"] = bool(result["source_sync_status"].get("ok"))
    return result


def _runtime_services_health_status(
    dsn: str,
    *,
    deployment: str,
) -> dict[str, Any]:
    try:
        from okg.substrate.runtime.publisher_health import (
            check_runtime_services_health,
        )

        return check_runtime_services_health(
            deployment=deployment,
            dsn=dsn,
        ).to_dict()
    except Exception as exc:  # noqa: BLE001 - sync health stays readable.
        return {
            "deployment": deployment,
            "ok": False,
            "code": "runtime_services_probe_failed",
            "message": str(exc),
            "error": type(exc).__name__,
        }


def _verify_webhook_signature(
    sync: SourceSyncCapabilities,
    *,
    body: bytes | None,
    headers: Mapping[str, str],
) -> None:
    webhook = sync.webhook
    if webhook is None or webhook.secret_ref is None:
        return
    secret = os.environ.get(webhook.secret_ref)
    if not secret:
        raise _producer_error(
            "source_sync_webhook_secret_missing",
            f"webhook secret env var {webhook.secret_ref!r} is not set",
        )
    signature = headers.get(webhook.signature_header)
    if signature is None:
        wanted = webhook.signature_header.lower()
        signature = next(
            (
                value for key, value in headers.items()
                if key.lower() == wanted
            ),
            None,
        )
    if not signature:
        raise _producer_error(
            "source_sync_webhook_signature_missing",
            f"missing webhook signature header {webhook.signature_header!r}",
        )
    expected = hmac.new(
        secret.encode("utf-8"),
        body or b"",
        hashlib.sha256,
    ).hexdigest()
    actual = signature.removeprefix("sha256=").strip()
    if not hmac.compare_digest(actual, expected):
        raise _producer_error(
            "source_sync_webhook_signature_invalid",
            "webhook signature did not match",
        )


def _webhook_path_map(registry: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for source_name, entry in registry.items():
        sync = getattr(entry, "sync", None)
        if (
            sync is None
            or sync.webhook is None
            or sync.webhook.path is None
            or "webhook" not in set(sync.triggers)
        ):
            continue
        out[sync.webhook.path] = source_name
    return out


def _payload_path_events(
    payload: Mapping[str, Any],
) -> tuple[tuple[str | None, str], ...]:
    default_reason = _optional_payload_str(payload, "reason") or "webhook"
    delete_reason = (
        "webhook:deleted"
        if payload.get("deleted") is True
        or payload.get("action") in {"deleted", "removed"}
        else default_reason
    )
    if isinstance(payload.get("scope_key"), str):
        return ((str(payload["scope_key"]), delete_reason),)
    for key in ("scope_keys", "paths"):
        raw = payload.get(key)
        if isinstance(raw, list):
            return tuple(
                (str(item), delete_reason)
                for item in raw
                if isinstance(item, str)
            )
    commits = payload.get("commits")
    if isinstance(commits, list):
        events: set[tuple[str, str]] = set()
        for commit in commits:
            if not isinstance(commit, Mapping):
                continue
            for field in ("added", "modified"):
                raw_paths = commit.get(field)
                if isinstance(raw_paths, list):
                    events.update(
                        (str(item), "webhook:modified")
                        for item in raw_paths
                        if isinstance(item, str)
                    )
            raw_removed = commit.get("removed")
            if isinstance(raw_removed, list):
                events.update(
                    (str(item), "webhook:removed")
                    for item in raw_removed
                    if isinstance(item, str)
                )
        return tuple(sorted(events))
    return ()


def _optional_payload_str(
    payload: Mapping[str, Any],
    key: str,
) -> str | None:
    value = payload.get(key)
    return value if isinstance(value, str) else None


def _probe_name(sync: SourceSyncCapabilities) -> str:
    if sync.poll is not None and sync.poll.probe:
        return sync.poll.probe
    if sync.cheap_probe:
        return sync.cheap_probe
    raise _producer_error(
        "source_sync_poll_probe_missing",
        "poll sync requires sync.poll.probe or sync.cheap_probe",
    )


def _run_probe(probe: str, sync: SourceSyncCapabilities) -> str:
    if probe == "ttl":
        return _stable_poll_probe_fingerprint(probe, sync)
    if probe == "remote_etag":
        if sync.watch is not None:
            return f"remote_etag:content_hash:{_content_tree_fingerprint(sync)}"
        return _stable_poll_probe_fingerprint(probe, sync)
    if probe == "content_hash":
        return _content_tree_fingerprint(sync)
    if probe == "stat_tree":
        # The banned mtime/size stat-digest tier (issue #323): a mtime-preserving
        # in-place edit is a silent false-unchanged at the poll/enqueue layer,
        # BEFORE the sound runner-gate change-probe ever runs. Kept runnable for
        # back-compat during the poll-probe migration, but deployment_lint flags
        # it (deployment.source_registry.poll_probe_unsound) and registries
        # should migrate to `content_hash`.
        return _stat_tree_fingerprint(sync)
    raise _producer_error(
        "source_sync_poll_probe_unknown",
        f"unknown source sync poll probe {probe!r}",
    )


def _stable_poll_probe_fingerprint(
    probe: str,
    sync: SourceSyncCapabilities,
) -> str:
    payload = sync.to_dict()
    payload["probe"] = probe
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    return f"{probe}:capability:{hashlib.sha256(encoded).hexdigest()}"


def _stat_tree_fingerprint(sync: SourceSyncCapabilities) -> str:
    if sync.watch is None:
        raise _producer_error(
            "source_sync_poll_probe_unscoped",
            "stat_tree poll probe requires sync.watch roots",
        )
    h = hashlib.sha256()
    for _root, path, rel in _iter_stat_tree_files(sync):
        try:
            st = path.stat()
        except OSError:
            continue
        if not stat.S_ISREG(st.st_mode):
            continue
        h.update(rel.encode("utf-8"))
        h.update(str(st.st_size).encode("ascii"))
        h.update(str(st.st_mtime_ns).encode("ascii"))
    return h.hexdigest()


def _content_tree_fingerprint(sync: SourceSyncCapabilities) -> str:
    """A SOUND poll fingerprint over the watch scope (issue #323).

    Hashes the file CONTENT (sha256 of bytes) of every in-scope watched file,
    not its mtime/size, so a mtime-preserving in-place edit busts the poll
    fingerprint just as it busts the runner-gate change-probe. This closes the
    poll/enqueue-layer false-unchanged that `stat_tree` leaves open. The scope is
    the same `sync.watch` include/exclude set `stat_tree` walks, so a deleted
    file drops from the digest (membership-sensitive) and an added/edited file
    busts it. A file that vanishes between enumeration and read is skipped (the
    next poll observes the membership drop). It is O(content) over the watch
    scope — the inherent price of a sound poll signal for a non-git file scope;
    keep the watch globs tight.
    """
    if sync.watch is None:
        raise _producer_error(
            "source_sync_poll_probe_unscoped",
            "content_hash poll probe requires sync.watch roots",
        )
    h = hashlib.sha256()
    for _root, path, rel in _iter_stat_tree_files(sync):
        try:
            st = path.stat()
            if not stat.S_ISREG(st.st_mode):
                continue
            with open(path, "rb") as handle:
                data = handle.read()
        except OSError:
            continue
        h.update(rel.encode("utf-8"))
        h.update(b"\x00")
        h.update(hashlib.sha256(data).hexdigest().encode("ascii"))
    return h.hexdigest()


def _iter_stat_tree_files(
    sync: SourceSyncCapabilities,
) -> tuple[tuple[Path, Path, str], ...]:
    """Return included stat-tree files while pruning excluded dirs."""
    if sync.watch is None:
        return ()
    out: list[tuple[Path, Path, str]] = []
    includes = sync.watch.include_globs
    excludes = sync.watch.exclude_globs
    for root_raw in sync.watch.roots:
        root = Path(root_raw).expanduser().resolve()
        if not root.is_dir():
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            dir_path = Path(dirpath)
            kept_dirs = []
            for dirname in sorted(dirnames):
                child = dir_path / dirname
                rel = _stat_tree_rel(root, child)
                if _stat_tree_dir_excluded(rel, excludes):
                    continue
                if not _stat_tree_dir_could_match_include(rel, includes):
                    continue
                kept_dirs.append(dirname)
            dirnames[:] = kept_dirs
            for filename in sorted(filenames):
                path = dir_path / filename
                rel = _stat_tree_rel(root, path)
                if any(Watcher._glob_matches(rel, ex) for ex in excludes):
                    continue
                if not any(Watcher._glob_matches(rel, inc) for inc in includes):
                    continue
                out.append((root, path, rel))
    return tuple(out)


def _stat_tree_rel(root: Path, path: Path) -> str:
    return str(path.relative_to(root)).replace("\\", "/")


def _stat_tree_dir_excluded(rel: str, excludes: Sequence[str]) -> bool:
    rel = rel.replace("\\", "/").strip("/")
    if rel == ".claude" or rel.startswith(".claude/"):
        return True
    return any(Watcher._glob_matches(rel, ex) for ex in excludes)


def _stat_tree_dir_could_match_include(rel: str, includes: Sequence[str]) -> bool:
    rel = rel.replace("\\", "/").strip("/")
    if not rel:
        return True
    for raw_pattern in includes or ("**/*",):
        pattern = str(raw_pattern).replace("\\", "/").strip("/")
        if pattern in {"", "*", "**", "**/*"}:
            return True
        if pattern.startswith("**/"):
            return True
        first_magic = _stat_tree_first_glob_index(pattern)
        first_slash = pattern.find("/")
        if first_magic >= 0 and (first_slash < 0 or first_magic < first_slash):
            # Watcher._glob_matches uses fnmatch semantics where ``*`` may span
            # ``/``. Even so, any match must begin with the literal characters
            # before the first wildcard. Prune siblings that cannot satisfy that
            # prefix; once it matches, stay conservative unless the entire segment
            # is the provably prefix-star shape.
            first_pattern = pattern.split("/", 1)[0]
            literal_prefix = first_pattern[:first_magic]
            if not literal_prefix:
                return True
            first_rel = rel.split("/", 1)[0]
            if not first_rel.startswith(literal_prefix):
                continue
            if _stat_tree_prefix_star_segment(first_pattern):
                if fnmatch.fnmatchcase(first_rel, first_pattern):
                    return True
                continue
            return True
        literal_prefix = _stat_tree_literal_prefix(pattern).strip("/")
        if not literal_prefix:
            return True
        literal_dir = literal_prefix.rstrip("/")
        if (
            rel == literal_dir
            or rel.startswith(f"{literal_dir}/")
            # The first wildcard can begin inside a deeper path segment, and
            # Watcher._glob_matches uses fnmatch semantics where that wildcard
            # may then span ``/``.  For ``root/prefix*/tail.json``, the
            # directory ``root/prefix-x`` is therefore a viable prefix (as are
            # all of its descendants), even though ``root/prefix`` is not a
            # complete directory segment.
            or (first_magic >= 0 and rel.startswith(literal_dir))
            or literal_dir.startswith(f"{rel}/")
        ):
            return True
    return False


def _stat_tree_prefix_star_segment(segment: str) -> bool:
    first_magic = _stat_tree_first_glob_index(segment)
    if first_magic <= 0:
        return False
    return set(segment[first_magic:]) == {"*"}


def _stat_tree_first_glob_index(pattern: str) -> int:
    indexes = [
        idx
        for idx in (pattern.find("*"), pattern.find("?"), pattern.find("["))
        if idx >= 0
    ]
    return min(indexes) if indexes else -1


def _stat_tree_literal_prefix(pattern: str) -> str:
    for idx, char in enumerate(pattern):
        if char in "*?[":
            return pattern[:idx]
    return pattern


def _read_poll_fingerprint(
    dsn: str,
    *,
    deployment: str,
    source_name: str,
    probe: str,
) -> str | None:
    value, _updated_at = _read_poll_fingerprint_state(
        dsn,
        deployment=deployment,
        source_name=source_name,
        probe=probe,
    )
    return value


def _read_poll_fingerprint_state(
    dsn: str,
    *,
    deployment: str,
    source_name: str,
    probe: str,
) -> tuple[str | None, datetime | None]:
    key = f"sync.poll.{probe}.fingerprint"
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT value, updated_at
              FROM okg.source_progress
             WHERE deployment = %s
               AND source_name = %s
               AND key = %s
            """,
            (deployment, source_name, key),
        )
        row = cur.fetchone()
    if row is None:
        return None, None
    return str(row[0]), row[1]


def _poll_suppressed_observation_key(probe: str) -> str:
    return f"sync.poll.{probe}.suppressed_observation"


def _read_poll_suppressed_observation(
    dsn: str,
    *,
    deployment: str,
    source_name: str,
    probe: str,
) -> PollSuppressedObservation | None:
    key = _poll_suppressed_observation_key(probe)
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT value, updated_at
              FROM okg.source_progress
             WHERE deployment = %s
               AND source_name = %s
               AND key = %s
            """,
            (deployment, source_name, key),
        )
        row = cur.fetchone()
    if row is None or not isinstance(row[1], datetime):
        return None
    raw = row[0]
    try:
        payload = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, Mapping):
        return None
    if payload.get("schema") != "okg.source_sync.poll_suppression.v1":
        return None
    workflow_id = payload.get("workflow_id")
    revision_hint = payload.get("revision_hint")
    effective_token = payload.get("effective_token")
    if (
        not isinstance(workflow_id, str)
        or not workflow_id
        or not isinstance(revision_hint, str)
        or not isinstance(effective_token, Mapping)
    ):
        return None
    return PollSuppressedObservation(
        workflow_id=workflow_id,
        revision_hint=revision_hint,
        effective_token=dict(effective_token),
        observed_at=row[1],
    )


def _write_poll_suppressed_observation(
    dsn: str,
    *,
    deployment: str,
    source_name: str,
    probe: str,
    observation: Mapping[str, Any] | None,
) -> None:
    key = _poll_suppressed_observation_key(probe)
    with db_connect(dsn) as conn, conn.cursor() as cur:
        if observation is None:
            cur.execute(
                """
                DELETE FROM okg.source_progress
                 WHERE deployment = %s
                   AND source_name = %s
                   AND key = %s
                """,
                (deployment, source_name, key),
            )
            return
        workflow_id = observation.get("workflow_id")
        revision_hint = observation.get("revision_hint")
        effective_token = observation.get("effective_token")
        if (
            not isinstance(workflow_id, str)
            or not workflow_id
            or not isinstance(revision_hint, str)
            or not isinstance(effective_token, Mapping)
        ):
            raise ValueError("poll suppression observation is incomplete")
        value = json.dumps(
            {
                "schema": "okg.source_sync.poll_suppression.v1",
                "workflow_id": workflow_id,
                "revision_hint": revision_hint,
                "effective_token": dict(effective_token),
            },
            sort_keys=True,
        )
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (deployment, source_name, key)
            DO UPDATE SET value = EXCLUDED.value,
                          updated_at = EXCLUDED.updated_at
            """,
            (deployment, source_name, key, value),
        )


def _watch_backed_poll_change_already_ingested(
    dsn: str,
    *,
    deployment: str,
    source_name: str,
    sync: SourceSyncCapabilities,
    suppressed_observation: PollSuppressedObservation,
    current_revision_hint: str,
    current_effective_token: Mapping[str, Any],
    ownership_id: str | None,
) -> bool:
    if sync.watch is None or "fs_event" not in set(sync.triggers):
        return False
    if dict(current_effective_token) != dict(
        suppressed_observation.effective_token
    ):
        return False
    if current_revision_hint != suppressed_observation.revision_hint:
        return False
    candidates = source_row_candidates(
        deployment, source_name, ownership_id
    )
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT max(NULLIF(value::jsonb->>'finished_at', '')::timestamptz)
              FROM okg.source_progress
             WHERE deployment = %s
               AND source_name = ANY(%s)
               AND key = 'sync.last_run'
               AND value::jsonb->>'workflow_id' = %s
            """,
            (
                deployment,
                list(candidates),
                suppressed_observation.workflow_id,
            ),
        )
        last_run_row = cur.fetchone()
        cur.execute(
            """
            SELECT last_applied_rev
              FROM okg.source_watermarks
             WHERE source = ANY(%s)
             ORDER BY CASE WHEN source = %s THEN 0 ELSE 1 END
            """,
            (list(candidates), candidates[0]),
        )
        watermark_rows = cur.fetchall()
    if (
        last_run_row is None
        or last_run_row[0] is None
        or len(watermark_rows) != 1
    ):
        return False
    cursor = watermark_rows[0][0]
    applied_effective_token = (
        cursor.get(CURSOR_TOKEN_KEY)
        if isinstance(cursor, Mapping)
        else None
    )
    applied_token_matches = (
        isinstance(applied_effective_token, Mapping)
        and dict(applied_effective_token)
        == dict(suppressed_observation.effective_token)
    )
    return applied_token_matches and _dbos_watch_workflow_completed_after(
        dsn,
        workflow_id=suppressed_observation.workflow_id,
        observed_at=suppressed_observation.observed_at,
    )


def _dbos_watch_workflow_completed_after(
    dsn: str,
    *,
    workflow_id: str,
    observed_at: datetime,
) -> bool:
    try:
        with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT status,
                       to_timestamp(completed_at / 1000.0)
                  FROM dbos.workflow_status
                 WHERE workflow_uuid = %s
                """,
                (workflow_id,),
            )
            row = cur.fetchone()
    except psycopg.Error:
        return False
    return bool(
        row is not None
        and str(row[0]).upper() == "SUCCESS"
        and row[1] is not None
        and row[1] > observed_at
    )


def _watch_backed_poll_change_in_flight(
    dsn: str,
    *,
    deployment: str,
    source_name: str,
    sync: SourceSyncCapabilities,
    publish: bool,
) -> dict[str, Any] | None:
    """Return compatible active watch work that already covers poll."""
    if sync.watch is None or "fs_event" not in set(sync.triggers):
        return None
    from okg.substrate.source_sync_enqueue import (
        active_dbos_source_sync_workflow_status_for_source,
    )

    active = active_dbos_source_sync_workflow_status_for_source(
        dsn=dsn,
        deployment=deployment,
        source_name=source_name,
    )
    if active is None:
        return None
    candidates = [active, *(active.get("active_work_candidates") or [])]
    compatible: list[dict[str, Any]] = []
    seen: set[str] = set()
    for candidate in candidates:
        workflow_id = str(candidate.get("workflow_id") or "")
        if not workflow_id or workflow_id in seen:
            continue
        seen.add(workflow_id)
        if bool(candidate.get("stale")):
            continue
        if str(candidate.get("status") or "").upper() not in {
            "ENQUEUED",
            "PENDING",
            "DELAYED",
        }:
            continue
        if "fs_event" not in set(candidate.get("trigger_kinds") or []):
            continue
        if str(candidate.get("run_mode") or "") != sync.default_event_mode:
            continue
        if (
            str(candidate.get("dependent_fanout") or "")
            != sync.dependents.on_event
        ):
            continue
        if candidate.get("publish") is not publish:
            continue
        compatible.append(candidate)
    if not compatible:
        return None
    selected = min(
        compatible,
        key=lambda candidate: str(candidate.get("workflow_id") or ""),
    )
    return {
        "workflow_id": str(selected["workflow_id"]),
        "workflow_status": str(selected["status"]),
        "workflow_run_mode": str(selected["run_mode"]),
        "workflow_dependent_fanout": str(selected["dependent_fanout"]),
        "workflow_publish": bool(selected["publish"]),
    }


def _poll_backstop_reconcile_due(
    dsn: str,
    *,
    deployment: str,
    source_name: str,
    interval_seconds: int,
    backstop_multiplier: int,
    ownership_id: str | None,
) -> dict[str, Any]:
    max_age_seconds = max(
        int(interval_seconds) * max(int(backstop_multiplier), 1),
        int(interval_seconds),
    )
    candidates = source_row_candidates(
        deployment, source_name, ownership_id
    )
    stale_active_workflow: dict[str, Any] | None = None
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        from okg.substrate.source_sync_enqueue import (
            active_dbos_source_sync_workflow_status_for_source,
        )
        active_workflow = active_dbos_source_sync_workflow_status_for_source(
            dsn=dsn,
            deployment=deployment,
            source_name=source_name,
            stale_after_seconds=max_age_seconds,
        )
        if active_workflow is not None and not active_workflow["stale"]:
            from okg.substrate.source_sync_convergence import (
                COMPLETED_SCOPE_RUN_MODES,
            )
            active_run_mode = str(active_workflow.get("run_mode") or "")
            active_is_event_work = "fs_event" in set(
                active_workflow.get("trigger_kinds") or []
            )
            if (
                active_run_mode in COMPLETED_SCOPE_RUN_MODES
                and not active_is_event_work
            ):
                return {
                    "due": False,
                    "reason": "active_dbos_source_sync_workflow",
                    "source": candidates[0],
                    "workflow_id": active_workflow["workflow_id"],
                    "workflow_status": active_workflow["status"],
                    "workflow_run_mode": active_run_mode,
                    "workflow_age_seconds": active_workflow["age_seconds"],
                    "last_reconcile_at": None,
                    "age_seconds": None,
                    "max_age_seconds": max_age_seconds,
                }
        if active_workflow is not None:
            stale_active_workflow = active_workflow
        cur.execute(
            """
            SELECT source,
                   last_reconcile_at,
                   EXTRACT(EPOCH FROM now() - last_reconcile_at)
              FROM okg.source_watermarks
             WHERE source = ANY(%s)
             ORDER BY CASE WHEN source = %s THEN 0 ELSE 1 END
             LIMIT 1
            """,
            (list(candidates), candidates[0]),
        )
        row = cur.fetchone()
    if row is None or row[1] is None:
        return {
            "due": True,
            "reason": "missing_watermark",
            "source": candidates[0],
            "last_reconcile_at": None,
            "age_seconds": None,
            "max_age_seconds": max_age_seconds,
            "stale_active_workflow": (
                stale_active_workflow if (
                    stale_active_workflow is not None
                    and stale_active_workflow.get("stale")
                ) else None
            ),
            "incompatible_active_workflow": (
                stale_active_workflow if (
                    stale_active_workflow is not None
                    and not stale_active_workflow.get("stale")
                ) else None
            ),
        }
    age_seconds = float(row[2])
    due = age_seconds > max_age_seconds
    return {
        "due": due,
        "reason": "stale_watermark" if due else "fresh_watermark",
        "source": row[0],
        "last_reconcile_at": row[1].isoformat(),
        "age_seconds": age_seconds,
        "max_age_seconds": max_age_seconds,
        "stale_active_workflow": (
            stale_active_workflow
            if due
            and stale_active_workflow is not None
            and stale_active_workflow.get("stale")
            else None
        ),
        "incompatible_active_workflow": (
            stale_active_workflow
            if due
            and stale_active_workflow is not None
            and not stale_active_workflow.get("stale")
            else None
        ),
    }


def _write_poll_fingerprint(
    dsn: str,
    *,
    deployment: str,
    source_name: str,
    probe: str,
    fingerprint: str,
    effective_token: Mapping[str, Any] | None = None,
    ownership_id: str | None,
) -> None:
    key = f"sync.poll.{probe}.fingerprint"
    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (deployment, source_name, key)
            DO UPDATE SET value = EXCLUDED.value,
                          updated_at = EXCLUDED.updated_at
            """,
            (deployment, source_name, key, fingerprint),
        )
        if effective_token is not None:
            candidates = source_row_candidates(
                deployment, source_name, ownership_id
            )
            cur.execute(
                """
                UPDATE okg.source_watermarks
                   SET last_applied_rev =
                         coalesce(last_applied_rev, '{}'::jsonb)
                         || jsonb_build_object(
                              'change_probe_token', %s::jsonb
                            )
                 WHERE source = ANY(%s)
                """,
                (json.dumps(dict(effective_token)), list(candidates)),
            )
            if cur.rowcount != 1:
                raise _producer_error(
                    "source_sync_poll_baseline_target_ambiguous",
                    "watch-backed poll baseline must update exactly one source "
                    f"watermark; matched {cur.rowcount}",
                )
        cur.execute(
            """
            DELETE FROM okg.source_progress
             WHERE deployment = %s
               AND source_name = %s
               AND key = %s
            """,
            (
                deployment,
                source_name,
                _poll_suppressed_observation_key(probe),
            ),
        )


def record_source_sync_producer_health(
    dsn: str,
    *,
    deployment: str,
    source_name: str,
    producer: str,
    enqueue_count: int,
    error_count: int,
    event_count: int | None = None,
    last_event_at: str | None = None,
    last_enqueue_at: str | None = None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    value = json.dumps({
        "last_run_at": now,
        "last_heartbeat_at": now,
        "last_event_at": last_event_at,
        "last_enqueue_at": last_enqueue_at,
        "enqueue_count": enqueue_count,
        "error_count": error_count,
        "event_count": event_count,
    }, sort_keys=True)
    try:
        with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.source_progress
                  (deployment, source_name, key, value, updated_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (deployment, source_name, key)
                DO UPDATE SET value = EXCLUDED.value,
                              updated_at = EXCLUDED.updated_at
                """,
                (deployment, source_name, f"sync.producer.{producer}", value),
            )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "failed to record source-sync producer health: deployment=%s "
            "source=%s producer=%s error=%s",
            deployment,
            source_name,
            producer,
            exc,
        )


def _record_producer_health(
    dsn: str,
    *,
    deployment: str,
    source_name: str,
    producer: str,
    enqueue_count: int,
    error_count: int,
) -> None:
    record_source_sync_producer_health(
        dsn,
        deployment=deployment,
        source_name=source_name,
        producer=producer,
        enqueue_count=enqueue_count,
        error_count=error_count,
    )


def _source_sync_dbos_queue_status(
    cur: psycopg.Cursor,
    *,
    deployment: str,
) -> dict[str, Any]:
    queue_name = source_sync_queue_name(deployment)
    cur.execute("SELECT to_regclass('dbos.workflow_status')")
    if cur.fetchone()[0] is None:
        return {
            "available": False,
            "queue_name": queue_name,
            "reason": "dbos.workflow_status missing",
            "rows": [],
        }
    workflow_status_columns = _source_sync_dbos_workflow_status_columns(cur)
    cur.execute(
        """
        SELECT queue_name, status, count(*)
          FROM dbos.workflow_status
         WHERE queue_name = %s
           AND name = 'okg_source_sync'
         GROUP BY queue_name, status
         ORDER BY queue_name, status
        """,
        (queue_name,),
    )
    all_rows = [
        {"queue_name": row[0], "status": row[1], "count": int(row[2])}
        for row in cur.fetchall()
    ]
    active_rows = [
        row for row in all_rows
        if str(row["status"]).upper() in _DBOS_ACTIVE_QUEUE_STATUSES
    ]
    terminal_history_rows = [
        row for row in all_rows
        if str(row["status"]).upper() not in _DBOS_ACTIVE_QUEUE_STATUSES
    ]
    current_application_version = None
    observed_column = (
        "updated_at" if "updated_at" in workflow_status_columns
        else "created_at" if "created_at" in workflow_status_columns
        else None
    )
    if (
        "application_version" in workflow_status_columns
        and observed_column is not None
    ):
        current_application_version = (
            _latest_dbos_application_version(
                cur,
                queue_name=queue_name,
                observed_column=observed_column,
                statuses=_DBOS_CURRENT_VERSION_PRIMARY_STATUSES,
            )
            or _latest_dbos_application_version(
                cur,
                queue_name=queue_name,
                observed_column=observed_column,
                statuses=_DBOS_CURRENT_VERSION_FALLBACK_STATUSES,
            )
        )
    latest_success_observed = None
    if observed_column is not None:
        observed_expr = sql.Identifier(observed_column)
        success_version_filter = sql.SQL("")
        success_params: tuple[Any, ...] = (queue_name,)
        if current_application_version is not None:
            success_version_filter = sql.SQL("AND application_version = %s")
            success_params = (*success_params, current_application_version)
        cur.execute(
            sql.SQL(
                """
                SELECT {}
                  FROM dbos.workflow_status
                 WHERE queue_name = %s
                   AND name = 'okg_source_sync'
                   AND upper(status) = 'SUCCESS'
                   {}
                   AND {} IS NOT NULL
                 ORDER BY {} DESC
                 LIMIT 1
                """
            ).format(
                observed_expr,
                success_version_filter,
                observed_expr,
                observed_expr,
            ),
            success_params,
        )
        row = cur.fetchone()
        latest_success_observed = row[0] if row and row[0] is not None else None
    failed_version_filter = sql.SQL("")
    failed_params: tuple[Any, ...] = (
        queue_name,
        sorted(_DBOS_FAILED_TERMINAL_STATUSES),
    )
    if current_application_version is not None:
        failed_version_filter = sql.SQL("AND application_version = %s")
        failed_params = (*failed_params, current_application_version)
    error_expr = (
        sql.Identifier("error")
        if "error" in workflow_status_columns
        else sql.SQL("NULL::text")
    )
    observed_expr = (
        sql.Identifier(observed_column)
        if observed_column is not None
        else sql.SQL("NULL")
    )
    cur.execute(
        sql.SQL(
            """
            SELECT queue_name, status, {} AS error, {} AS observed_at
              FROM dbos.workflow_status
             WHERE queue_name = %s
               AND name = 'okg_source_sync'
               AND upper(status) = ANY(%s)
               {}
             ORDER BY queue_name, status
            """
        ).format(error_expr, observed_expr, failed_version_filter),
        failed_params,
    )
    failed_terminal_counts: dict[tuple[str, str], int] = {}
    for row in cur.fetchall():
        status = str(row[1])
        observed_at = row[3]
        if (
            latest_success_observed is not None
            and observed_at is not None
            and observed_at <= latest_success_observed
        ):
            continue
        if (
            status.upper() == "CANCELLED"
            and row[2] is None
        ) or _source_sync_dbos_recoverable_terminal_error(row[2]):
            continue
        key = (str(row[0]), status)
        failed_terminal_counts[key] = failed_terminal_counts.get(key, 0) + 1
    failed_terminal_rows = [
        {"queue_name": queue, "status": status, "count": count}
        for (queue, status), count in sorted(failed_terminal_counts.items())
    ]
    active_age = _source_sync_dbos_active_age(cur, queue_name=queue_name)
    liveness = _source_sync_dbos_active_liveness(
        cur, queue_name=queue_name,
    )
    oldest_active_age = active_age.get("oldest_active_age_seconds")
    return {
        "available": True,
        "queue_name": queue_name,
        "active_queue_names": sorted({
            str(row["queue_name"])
            for row in active_rows
            if row.get("queue_name")
        }),
        # Back-compat field, but now it means actual non-terminal queue
        # backlog rather than the complete workflow_status audit history.
        "rows": active_rows,
        "active_rows": active_rows,
        "terminal_history_rows": terminal_history_rows,
        "failed_terminal_rows": failed_terminal_rows,
        "failed_terminal_application_version": current_application_version,
        "active_count": sum(int(row["count"]) for row in active_rows),
        "terminal_history_count": sum(
            int(row["count"]) for row in terminal_history_rows
        ),
        "failed_terminal_count": sum(
            int(row["count"]) for row in failed_terminal_rows
        ),
        "stale_after_seconds": _SOURCE_SYNC_STALE_BACKLOG_SECONDS,
        "oldest_active_age_seconds": oldest_active_age,
        "newest_active_age_seconds": active_age.get(
            "newest_active_age_seconds"
        ),
        **liveness,
    }


def _source_sync_failed_terminal_rows(
    rows: Sequence[Sequence[Any]],
) -> list[dict[str, Any]]:
    failed_terminal_counts: dict[tuple[str, str], int] = {}
    for row in rows:
        status = str(row[1])
        error = row[2] if len(row) > 2 else None
        if (
            status.upper() == "CANCELLED"
            and error is None
        ) or _source_sync_dbos_recoverable_terminal_error(error):
            continue
        key = (str(row[0]), status)
        failed_terminal_counts[key] = failed_terminal_counts.get(key, 0) + 1
    return [
        {"queue_name": queue, "status": status, "count": count}
        for (queue, status), count in sorted(failed_terminal_counts.items())
    ]


def _latest_dbos_application_version(
    cur: psycopg.Cursor,
    *,
    queue_name: str,
    observed_column: str,
    statuses: set[str],
) -> str | None:
    cur.execute(
        sql.SQL(
            """
            SELECT application_version
              FROM dbos.workflow_status
             WHERE queue_name = %s
               AND name = 'okg_source_sync'
               AND application_version IS NOT NULL
               AND application_version <> ''
               AND upper(status) = ANY(%s)
             ORDER BY {} DESC
             LIMIT 1
            """
        ).format(sql.Identifier(observed_column)),
        (queue_name, sorted(statuses)),
    )
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def _source_sync_dbos_recoverable_terminal_error(raw: Any) -> bool:
    if raw is None:
        return False
    if isinstance(raw, bytes):
        encoded = raw
    else:
        encoded = str(raw).encode("utf-8", errors="ignore")
    payloads = [encoded]
    decoded = None
    try:
        decoded = base64.b64decode(encoded, validate=False)
    except (binascii.Error, ValueError):
        # Some DBOS terminal errors are stored as plain strings, not pickled
        # base64 payloads; the original encoded bytes are still checked below.
        decoded = None
    if decoded:
        payloads.append(decoded)
    return any(
        marker in payload
        for payload in payloads
        for marker in _DBOS_RECOVERABLE_TERMINAL_ERROR_MARKERS
    )


def _source_sync_dbos_active_age(
    cur: psycopg.Cursor,
    *,
    queue_name: str,
) -> dict[str, Any]:
    stale_column = _source_sync_dbos_time_column(cur)
    if stale_column is None:
        return {
            "oldest_active_age_seconds": None,
            "newest_active_age_seconds": None,
        }
    cur.execute(
        sql.SQL(
            """
            SELECT min({stale_column}) AS oldest_raw,
                   max({stale_column}) AS newest_raw
              FROM dbos.workflow_status
             WHERE queue_name = %s
               AND name = 'okg_source_sync'
               AND upper(status) = ANY(%s)
            """
        ).format(stale_column=sql.Identifier(stale_column)),
        (queue_name, sorted(_DBOS_ACTIVE_QUEUE_STATUSES)),
    )
    row = cur.fetchone()
    if row is None:
        return {
            "oldest_active_age_seconds": None,
            "newest_active_age_seconds": None,
        }
    return {
        "oldest_active_age_seconds": _source_sync_dbos_age_seconds(row[0]),
        "newest_active_age_seconds": _source_sync_dbos_age_seconds(row[1]),
    }


def _source_sync_dbos_active_liveness(
    cur: psycopg.Cursor,
    *,
    queue_name: str,
) -> dict[str, Any]:
    columns = _source_sync_dbos_workflow_status_columns(cur)
    if "updated_at" in columns:
        time_column = "updated_at"
    elif "created_at" in columns:
        time_column = "created_at"
    else:
        time_column = None
    if time_column is None:
        return {
            "pending_count": 0,
            "waiting_count": 0,
            "fresh_pending_count": 0,
            "running_pending_count": 0,
            "stale_started_pending_work_present": False,
            "oldest_started_pending_update_age_seconds": None,
            "replaceable_waiting_count": 0,
            "oldest_pending_update_age_seconds": None,
            "oldest_waiting_age_seconds": None,
            "oldest_replaceable_waiting_age_seconds": None,
            "stale_pending_work_present": False,
            "stale_waiting_work_present": False,
            "stale_replaceable_work_present": False,
        }
    started_expr = _source_sync_dbos_optional_column_expr(
        columns,
        "started_at_epoch_ms",
        null_type="bigint",
    )
    priority_expr = _source_sync_dbos_optional_column_expr(
        columns,
        "priority",
        null_type="integer",
    )
    cur.execute(
        sql.SQL(
            """
            SELECT upper(status) AS status,
                   {time_column} AS observed_raw,
                   {started_expr} AS started_raw,
                   {priority_expr} AS priority
              FROM dbos.workflow_status
             WHERE queue_name = %s
               AND name = 'okg_source_sync'
               AND upper(status) = ANY(%s)
             ORDER BY {time_column}, workflow_uuid
            """
        ).format(
            time_column=sql.Identifier(time_column),
            started_expr=started_expr,
            priority_expr=priority_expr,
        ),
        (queue_name, sorted(_DBOS_ACTIVE_QUEUE_STATUSES)),
    )
    pending_count = 0
    waiting_count = 0
    fresh_pending_count = 0
    running_pending_count = 0
    stale_started_pending = False
    oldest_started_pending_age: float | None = None
    replaceable_waiting_count = 0
    oldest_pending_age: float | None = None
    oldest_waiting_age: float | None = None
    oldest_replaceable_waiting_age: float | None = None
    stale_pending = False
    stale_waiting = False
    for status, observed_raw, started_raw, priority_raw in cur.fetchall():
        age = _source_sync_dbos_age_seconds(observed_raw)
        if str(status).upper() == "PENDING":
            pending_count += 1
            if age is not None and (
                oldest_pending_age is None or age > oldest_pending_age
            ):
                oldest_pending_age = age
            # DBOS does not heartbeat workflow_status.updated_at while a
            # source-sync workflow is executing. A started row is therefore
            # running, not replaceable stale work, until a stronger run-state
            # signal says otherwise.
            if started_raw is not None:
                running_pending_count += 1
                fresh_pending_count += 1
                if age is not None and (
                    oldest_started_pending_age is None
                    or age > oldest_started_pending_age
                ):
                    oldest_started_pending_age = age
                if (
                    age is not None
                    and age >= _SOURCE_SYNC_STALE_BACKLOG_SECONDS
                ):
                    stale_started_pending = True
            elif age is not None and age < _SOURCE_SYNC_STALE_BACKLOG_SECONDS:
                fresh_pending_count += 1
            elif age is not None:
                stale_pending = True
            continue
        if str(status).upper() in {"ENQUEUED", "DELAYED"}:
            waiting_count += 1
            if age is not None and (
                oldest_waiting_age is None or age > oldest_waiting_age
            ):
                oldest_waiting_age = age
            priority = _source_sync_priority_value(priority_raw)
            replaceable_waiting = (
                priority is None
                or priority <= 0
                or priority >= 100
            )
            if replaceable_waiting:
                replaceable_waiting_count += 1
                if age is not None and (
                    oldest_replaceable_waiting_age is None
                    or age > oldest_replaceable_waiting_age
                ):
                    oldest_replaceable_waiting_age = age
                if (
                    age is not None
                    and age >= _SOURCE_SYNC_STALE_BACKLOG_SECONDS
                ):
                    stale_waiting = True
    return {
        "pending_count": pending_count,
        "waiting_count": waiting_count,
        "fresh_pending_count": fresh_pending_count,
        "running_pending_count": running_pending_count,
        "stale_started_pending_work_present": stale_started_pending,
        "oldest_started_pending_update_age_seconds": (
            oldest_started_pending_age
        ),
        "replaceable_waiting_count": replaceable_waiting_count,
        "oldest_pending_update_age_seconds": oldest_pending_age,
        "oldest_waiting_age_seconds": oldest_waiting_age,
        "oldest_replaceable_waiting_age_seconds": (
            oldest_replaceable_waiting_age
        ),
        "stale_pending_work_present": stale_pending,
        "stale_waiting_work_present": stale_waiting,
        "stale_replaceable_work_present": stale_pending or stale_waiting,
    }


def _source_sync_dbos_time_column(cur: psycopg.Cursor) -> str | None:
    columns = _source_sync_dbos_workflow_status_columns(cur)
    if "updated_at" in columns:
        return "updated_at"
    if "created_at" in columns:
        return "created_at"
    return None


def _source_sync_dbos_workflow_status_columns(
    cur: psycopg.Cursor,
) -> set[str]:
    cur.execute(
        """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = 'dbos'
           AND table_name = 'workflow_status'
        """
    )
    return {str(row[0]) for row in cur.fetchall()}


def _source_sync_dbos_optional_column_expr(
    columns: set[str],
    column: str,
    *,
    null_type: str,
) -> sql.Composable:
    if column in columns:
        return sql.Identifier(column)
    return sql.SQL("NULL::{}").format(sql.SQL(null_type))


def _source_sync_priority_value(raw: Any) -> int | None:
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _source_sync_dbos_age_seconds(raw: Any) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        observed = raw
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=timezone.utc)
        return round(
            max(0.0, (datetime.now(timezone.utc) - observed).total_seconds()),
            3,
        )
    try:
        observed_ts = float(raw)
    except (TypeError, ValueError):
        return None
    if observed_ts > 10_000_000_000:
        observed_ts = observed_ts / 1000.0
    return round(max(0.0, time.time() - observed_ts), 3)


def _producer_error(
    code: str,
    message: str,
    *,
    fix_hint: str | None = None,
) -> SubstrateError:
    err = SubstrateError(message, fix_hint=fix_hint)
    err.code = code
    return err


__all__ = [
    "DEFAULT_PRODUCER_HEALTH_MAX_AGE_SECONDS",
    "SOURCE_SYNC_STATUS_SCHEMA",
    "build_source_sync_status_contract",
    "expected_source_sync_producers",
    "handle_webhook_payload",
    "make_webhook_http_server",
    "record_source_sync_producer_health",
    "run_webhook_http_server",
    "run_poll_once",
    "source_sync_producer_health",
    "source_sync_status_for_source",
    "webhook_events_from_payload",
]
