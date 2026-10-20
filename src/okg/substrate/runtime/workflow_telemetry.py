"""Telemetry context helpers for DBOS workflow wrappers."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
from typing import Any, Iterator, Mapping

from okg.substrate.db.connection import connect as db_connect

from okg.substrate.telemetry import (
    TelemetrySpan,
    child_span_identity,
    current_trace_identity,
    is_valid_span_id,
    is_valid_trace_id,
    record_context_span_if_missing,
    record_span,
    use_trace,
)


log = logging.getLogger(__name__)

_SOURCE_SYNC_ADMISSION_SPAN = "source_sync.admission_lock.wait"
_SOURCE_SYNC_PUBLISH_SPAN = "source_sync.publish.commit"
_SOURCE_SYNC_TRACE_HANDOFF_SCHEMA = "okg.source_sync.trace.v1"
# An ``unavailable`` marker names a value by a short key; the value itself is
# recorded under a different attribute key. A marker listed here is dropped as
# soon as its value is known (#2320). Markers with no value key, such as the
# source-sync admission marker, persist until something else replaces them.
_UNAVAILABLE_MARKER_VALUE_KEYS: dict[str, str] = {
    "queue_wait_ms": "dbos.workflow.queue_wait_ms",
    "recovery_attempts": "dbos.workflow.recovery_attempts",
}


@dataclass
class SourceSyncPublishSpan:
    """Mutable outcome recorded by ``source_sync_publish_span`` on exit."""

    status_code: str = "ok"
    status_message: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)

    def observe_result(self, result: Mapping[str, Any]) -> None:
        """Derive span status from the publisher's returned summary."""
        publish_status = str(result.get("status") or "unknown")
        self.attributes["okg.source_sync.publish.status"] = publish_status
        code = result.get("code")
        if code is not None:
            self.attributes["okg.source_sync.publish.code"] = str(code)[:128]
        if publish_status == "published":
            return
        self.status_code = "error"
        message = (
            result.get("error")
            or result.get("blocked_reason")
            or result.get("reason")
            or code
            or f"publish returned status {publish_status}"
        )
        self.status_message = str(message)[:512]


@contextmanager
def source_sync_publish_span(
    *,
    dsn: str,
    deployment_name: str,
    workflow_id: str | None,
    branch_name: str = "default",
) -> Iterator[SourceSyncPublishSpan]:
    """Persist one child span for one actual source-sync publish call."""
    outcome = SourceSyncPublishSpan()
    parent = current_trace_identity()
    if not workflow_id or not parent.is_contextual:
        yield outcome
        return

    identity = child_span_identity()
    started_at = datetime.now(timezone.utc)
    try:
        yield outcome
    except Exception as exc:
        outcome.status_code = "error"
        outcome.status_message = str(exc)[:512]
        raise
    finally:
        ended_at = datetime.now(timezone.utc)
        try:
            with db_connect(dsn, autocommit=True) as conn:
                record_span(
                    conn,
                    TelemetrySpan(
                        trace_id=identity.trace_id,
                        span_id=identity.span_id,
                        parent_span_id=identity.parent_span_id,
                        span_name=_SOURCE_SYNC_PUBLISH_SPAN,
                        span_kind="internal",
                        status_code=outcome.status_code,
                        status_message=outcome.status_message,
                        deployment_name=deployment_name,
                        branch_name=branch_name,
                        workflow_id=workflow_id,
                        started_at=started_at,
                        ended_at=ended_at,
                        attributes={
                            "component": "source-sync",
                            "okg.component": "source-sync",
                            "okg.source_sync.phase": "publish_commit",
                            **outcome.attributes,
                        },
                        root_name="dbos.workflow.okg_source_sync",
                        root_kind="dbos_workflow",
                    ),
                )
        except Exception as exc:  # noqa: BLE001 - telemetry is advisory.
            log.warning(
                "failed to record source-sync publish phase telemetry: "
                "deployment=%s workflow_id=%s error=%s",
                deployment_name,
                workflow_id,
                exc,
            )


@contextmanager
def dbos_workflow_trace(
    *,
    dsn: str,
    deployment_name: str,
    workflow_name: str,
    workflow_id: str | None,
    branch_name: str = "default",
    queue_name: str | None = None,
    started_at: datetime | None = None,
    attributes: Mapping[str, Any] | None = None,
    source_sync_trace_handoff: Mapping[str, Any] | None = None,
) -> Iterator[Any | None]:
    """Bind and persist a workflow-root trace for DBOS-owned work.

    The DBOS SDK exposes workflow identity through runtime state, but AKMON's
    span writers need an active local trace context so source, publisher,
    Chronos, SQL, enricher, view, and gate spans attach under one lifetime root.
    Missing DBOS status details are recorded as explicit unavailable metadata.
    """
    if not workflow_id:
        yield None
        return

    handoff, handoff_unavailable = (
        _source_sync_trace_handoff(source_sync_trace_handoff)
        if workflow_name == "okg_source_sync"
        else (None, None)
    )
    root_started_at = _as_utc(started_at or datetime.now(timezone.utc))
    if handoff is not None:
        root_started_at = min(root_started_at, handoff["started_at"])
    workflow_attrs = _workflow_attributes(
        workflow_name=workflow_name,
        workflow_id=workflow_id,
        queue_name=queue_name,
        attributes=attributes,
    )
    if handoff_unavailable is not None:
        unavailable = dict(workflow_attrs.get("unavailable") or {})
        unavailable[_SOURCE_SYNC_ADMISSION_SPAN] = handoff_unavailable
        workflow_attrs["unavailable"] = unavailable
    runtime_attrs = _dbos_runtime_attributes(dsn=dsn, workflow_id=workflow_id)
    _merge_workflow_attributes(workflow_attrs, runtime_attrs)
    root_started_at = _workflow_lifetime_started_at(root_started_at, workflow_attrs)
    with use_trace(
        handoff["trace_id"] if handoff is not None else None,
        span_id=handoff["root_span_id"] if handoff is not None else None,
    ) as identity:
        _record_workflow_root(
            dsn=dsn,
            identity=identity,
            deployment_name=deployment_name,
            branch_name=branch_name,
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            started_at=root_started_at,
            ended_at=root_started_at,
            status_code="unset",
            attributes=workflow_attrs,
        )
        if handoff is not None:
            _record_source_sync_admission_wait(
                dsn=dsn,
                identity=identity,
                deployment_name=deployment_name,
                branch_name=branch_name,
                workflow_id=workflow_id,
                handoff=handoff,
            )
        status_code = "ok"
        status_message = None
        try:
            yield identity
        except Exception as exc:
            status_code = "error"
            status_message = str(exc)[:512]
            raise
        finally:
            runtime_attrs = _dbos_runtime_attributes(
                dsn=dsn,
                workflow_id=workflow_id,
            )
            final_attrs = dict(workflow_attrs)
            _merge_workflow_attributes(final_attrs, runtime_attrs)
            final_started_at = _workflow_lifetime_started_at(
                root_started_at,
                final_attrs,
            )
            final_ended_at = _workflow_lifetime_ended_at(
                datetime.now(timezone.utc),
                final_attrs,
                started_at=final_started_at,
            )
            _record_workflow_root(
                dsn=dsn,
                identity=identity,
                deployment_name=deployment_name,
                branch_name=branch_name,
                workflow_id=workflow_id,
                workflow_name=workflow_name,
                started_at=final_started_at,
                ended_at=final_ended_at,
                status_code=status_code,
                status_message=status_message,
                attributes=final_attrs,
            )


def _record_workflow_root(
    *,
    dsn: str,
    identity: Any,
    deployment_name: str,
    branch_name: str,
    workflow_id: str,
    workflow_name: str,
    started_at: datetime,
    ended_at: datetime,
    status_code: str,
    status_message: str | None = None,
    attributes: Mapping[str, Any],
) -> None:
    try:
        with db_connect(dsn, autocommit=True) as conn:
            record_context_span_if_missing(
                conn,
                identity,
                deployment_name=deployment_name,
                branch_name=branch_name,
                span_name=f"dbos.workflow.{workflow_name}",
                root_kind="dbos_workflow",
                started_at=started_at,
                ended_at=ended_at,
                status_code=status_code,
                status_message=status_message,
                workflow_id=workflow_id,
                attributes=attributes,
            )
    except Exception as exc:  # noqa: BLE001 - workflow telemetry is advisory.
        log.warning(
            "failed to record DBOS workflow telemetry root: "
            "deployment=%s workflow_name=%s workflow_id=%s error=%s",
            deployment_name,
            workflow_name,
            workflow_id,
            exc,
        )


def _workflow_attributes(
    *,
    workflow_name: str,
    workflow_id: str,
    queue_name: str | None,
    attributes: Mapping[str, Any] | None,
) -> dict[str, Any]:
    unavailable: dict[str, str] = {
        "queue_wait_ms": "dbos_workflow_queue_wait_not_available",
        "recovery_attempts": "dbos_workflow_recovery_attempts_not_available",
    }
    out: dict[str, Any] = {
        "component": "dbos-workflow",
        "okg.component": "dbos-workflow",
        "dbos.workflow.name": workflow_name,
        "okg.workflow.name": workflow_name,
        "dbos.workflow.id": workflow_id,
        "okg.workflow.id": workflow_id,
        "unavailable": unavailable,
    }
    if queue_name:
        out["dbos.workflow.queue_name"] = queue_name
        out["dbos.queue.name"] = queue_name
    if attributes:
        _merge_workflow_attributes(out, attributes)
    return out


def _merge_workflow_attributes(
    target: dict[str, Any],
    incoming: Mapping[str, Any],
) -> None:
    """Merge ``incoming`` into ``target``; a known value beats an ``unavailable`` marker.

    Markers are a union, so a partial update never erases a marker for a value
    that is still missing (change ``issue-877-source-sync-spans``). A marker is
    dropped when the merged attributes hold a non-null value for it, whether
    the value arrived now or was already present (#2320).
    """
    incoming_attrs = dict(incoming)
    incoming_unavailable = incoming_attrs.pop("unavailable", None)
    target.update(incoming_attrs)
    if "unavailable" not in target and not isinstance(incoming_unavailable, Mapping):
        return
    unavailable = dict(target.get("unavailable") or {})
    if isinstance(incoming_unavailable, Mapping):
        unavailable.update(
            {str(key): str(value) for key, value in incoming_unavailable.items()}
        )
    for marker, value_key in _UNAVAILABLE_MARKER_VALUE_KEYS.items():
        if target.get(value_key) is not None:
            unavailable.pop(marker, None)
    target["unavailable"] = unavailable


def _source_sync_trace_handoff(
    raw: Mapping[str, Any] | None,
) -> tuple[dict[str, Any] | None, str | None]:
    if raw is None:
        return None, "source_sync_trace_handoff_not_available_legacy_input"
    try:
        if raw.get("schema") != _SOURCE_SYNC_TRACE_HANDOFF_SCHEMA:
            raise ValueError("unsupported schema")
        trace_id = str(raw.get("trace_id") or "")
        root_span_id = str(raw.get("root_span_id") or "")
        wait_span_id = str(raw.get("wait_span_id") or "")
        if not is_valid_trace_id(trace_id):
            raise ValueError("invalid trace id")
        if not is_valid_span_id(root_span_id):
            raise ValueError("invalid root span id")
        if not is_valid_span_id(wait_span_id):
            raise ValueError("invalid wait span id")
        started_at = _handoff_datetime(raw.get("started_at"))
        ended_at = _handoff_datetime(raw.get("ended_at"))
        if ended_at < started_at:
            raise ValueError("wait ended before it started")
    except (TypeError, ValueError):
        return None, "source_sync_trace_handoff_invalid"
    return {
        "trace_id": trace_id,
        "root_span_id": root_span_id,
        "wait_span_id": wait_span_id,
        "started_at": started_at,
        "ended_at": ended_at,
    }, None


def _handoff_datetime(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        return _as_utc(raw)
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("missing timestamp")
    return _as_utc(datetime.fromisoformat(raw))


def _record_source_sync_admission_wait(
    *,
    dsn: str,
    identity: Any,
    deployment_name: str,
    branch_name: str,
    workflow_id: str,
    handoff: Mapping[str, Any],
) -> None:
    """Insert the producer wait once, even if DBOS recovers the step."""
    try:
        with db_connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                      FROM okg.telemetry_spans
                     WHERE deployment_name = %s
                       AND trace_id = %s
                       AND span_id = %s
                     LIMIT 1
                    """,
                    (
                        deployment_name,
                        identity.trace_id,
                        handoff["wait_span_id"],
                    ),
                )
                if cur.fetchone() is not None:
                    return
            record_span(
                conn,
                TelemetrySpan(
                    trace_id=identity.trace_id,
                    span_id=str(handoff["wait_span_id"]),
                    parent_span_id=identity.span_id,
                    span_name=_SOURCE_SYNC_ADMISSION_SPAN,
                    span_kind="internal",
                    status_code="ok",
                    deployment_name=deployment_name,
                    branch_name=branch_name,
                    workflow_id=workflow_id,
                    started_at=handoff["started_at"],
                    ended_at=handoff["ended_at"],
                    attributes={
                        "component": "source-sync",
                        "okg.component": "source-sync",
                        "okg.source_sync.phase": "admission_lock_wait",
                    },
                    root_name="dbos.workflow.okg_source_sync",
                    root_kind="dbos_workflow",
                ),
            )
    except Exception as exc:  # noqa: BLE001 - telemetry is advisory.
        log.warning(
            "failed to record source-sync admission phase telemetry: "
            "deployment=%s workflow_id=%s error=%s",
            deployment_name,
            workflow_id,
            exc,
        )


def _dbos_runtime_attributes(*, dsn: str, workflow_id: str) -> dict[str, Any]:
    try:
        with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("SELECT to_regclass('dbos.workflow_status')")
            if cur.fetchone()[0] is None:
                return {}
            cur.execute(
                """
                SELECT column_name
                  FROM information_schema.columns
                 WHERE table_schema = 'dbos'
                   AND table_name = 'workflow_status'
                """
            )
            columns = {str(row[0]) for row in cur.fetchall()}
            select_exprs = [
                _optional_dbos_column(columns, "status", "text"),
                _optional_dbos_column(columns, "queue_name", "text"),
                _optional_dbos_column(columns, "recovery_attempts", "bigint"),
                _optional_dbos_column(columns, "created_at", "bigint"),
                _optional_dbos_column(columns, "started_at_epoch_ms", "bigint"),
                _optional_dbos_column(columns, "completed_at", "bigint"),
                _optional_dbos_column(columns, "parent_workflow_id", "text"),
                _optional_dbos_column(columns, "executor_id", "text"),
                _optional_dbos_column(columns, "application_id", "text"),
            ]
            cur.execute(
                f"""
                SELECT {", ".join(select_exprs)}
                  FROM dbos.workflow_status
                 WHERE workflow_uuid::text = %s
                 LIMIT 1
                """,
                (workflow_id,),
            )
            row = cur.fetchone()
    except Exception:  # noqa: BLE001 - DBOS metadata is optional telemetry.
        return {}
    if row is None:
        return {}
    attrs: dict[str, Any] = {}
    unavailable: dict[str, str] = {}
    (
        status,
        queue_name,
        recovery_attempts,
        created_at,
        started_at_epoch_ms,
        completed_at,
        parent_workflow_id,
        executor_id,
        application_id,
    ) = row
    if status:
        attrs["dbos.workflow.status"] = str(status)
    if queue_name:
        attrs["dbos.workflow.queue_name"] = str(queue_name)
        attrs["dbos.queue.name"] = str(queue_name)
    if recovery_attempts is not None:
        attrs["dbos.workflow.recovery_attempts"] = int(recovery_attempts)
    else:
        unavailable["recovery_attempts"] = (
            "dbos_workflow_recovery_attempts_not_available"
        )
    if created_at is not None:
        attrs["dbos.workflow.created_at_epoch_ms"] = int(created_at)
    if started_at_epoch_ms is not None:
        attrs["dbos.workflow.started_at_epoch_ms"] = int(started_at_epoch_ms)
    if completed_at is not None:
        attrs["dbos.workflow.completed_at_epoch_ms"] = int(completed_at)
    if created_at is not None and started_at_epoch_ms is not None:
        attrs["dbos.workflow.queue_wait_ms"] = max(
            0,
            int(started_at_epoch_ms) - int(created_at),
        )
    else:
        unavailable["queue_wait_ms"] = "dbos_workflow_queue_wait_not_available"
    if parent_workflow_id:
        attrs["dbos.workflow.parent_workflow_id"] = str(parent_workflow_id)
    if executor_id:
        attrs["dbos.executor.id"] = str(executor_id)
    if application_id:
        attrs["dbos.application.id"] = str(application_id)
    attrs["unavailable"] = unavailable
    return attrs


def _workflow_lifetime_started_at(
    fallback: datetime,
    attrs: Mapping[str, Any],
) -> datetime:
    candidates = [_as_utc(fallback)]
    for key in (
        "dbos.workflow.created_at_epoch_ms",
        "dbos.workflow.started_at_epoch_ms",
    ):
        candidate = _epoch_ms_datetime(attrs.get(key))
        if candidate is not None:
            candidates.append(candidate)
    return min(candidates)


def _workflow_lifetime_ended_at(
    fallback: datetime,
    attrs: Mapping[str, Any],
    *,
    started_at: datetime,
) -> datetime:
    completed = _epoch_ms_datetime(attrs.get("dbos.workflow.completed_at_epoch_ms"))
    if completed is None:
        return _as_utc(fallback)
    return max(_as_utc(started_at), completed)


def _epoch_ms_datetime(raw: Any) -> datetime | None:
    try:
        epoch_ms = float(raw)
    except (TypeError, ValueError):
        return None
    if epoch_ms <= 0:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)


def _optional_dbos_column(
    columns: set[str],
    name: str,
    cast_type: str,
) -> str:
    if name in columns:
        return name
    return f"NULL::{cast_type} AS {name}"


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
