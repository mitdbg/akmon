"""Durable unified telemetry writes.

OpenTelemetry is the instrumentation API. This module is the first
OKG-local storage adapter: it can derive ids from the active OTel
context when present, generate W3C-shaped ids otherwise, and write a
bounded trace/span pair into the OKG telemetry tables.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import lru_cache
import hashlib
import json
import os
import re
import socket
import sys
import threading
import time
import uuid
from typing import Any, Iterator, Mapping

try:  # OpenTelemetry is declared in pyproject; keep import failure non-fatal.
    from opentelemetry import trace
except Exception:  # pragma: no cover - defensive for stripped-down environments.
    trace = None  # type: ignore[assignment]

_MAX_PAYLOAD_SUMMARY_BYTES = 8 * 1024
_MAX_DB_STATEMENT_SUMMARY_CHARS = 256
_CURRENT_TRACE_IDENTITY: ContextVar["TraceIdentity | None"] = ContextVar(
    "okg_telemetry_trace_identity",
    default=None,
)
_TRACE_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_SPAN_ID_RE = re.compile(r"^[0-9a-f]{16}$")
_SQL_SINGLE_QUOTED_RE = re.compile(r"'(?:''|[^'])*'")
_SQL_DOUBLE_QUOTED_RE = re.compile(r'"(?:""|[^"])*"')
_SQL_NUMERIC_RE = re.compile(r"\b\d+(?:\.\d+)?\b")
_SQL_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
_DB_TIMING_TOKENS = (
    "query",
    "sql",
    "tx",
    "transaction",
    "copy",
    "insert",
    "upsert",
    "select",
    "update",
    "delete",
    "checkpoint_generation.operation",
    "fact_log",
    "progress_upsert",
)
_DBOS_EXECUTOR_ENV_KEYS = (
    "DBOS__VMID",
    "DBOS_EXECUTOR_ID",
    "DBOS__EXECUTOR_ID",
)
_DBOS_APP_ENV_KEYS = (
    "DBOS__APPID",
    "DBOS_APP_ID",
    "DBOS_APPLICATION_ID",
)
_DBOS_WORKER_ENV_KEYS = (
    "DBOS_WORKER_ID",
    "DBOS__WORKER_ID",
    "OKG_WORKER_ID",
)


@dataclass(frozen=True)
class TraceIdentity:
    trace_id: str
    span_id: str
    parent_span_id: str | None = None
    from_otel_context: bool = False
    from_local_context: bool = False

    @property
    def is_contextual(self) -> bool:
        return self.from_otel_context or self.from_local_context


@dataclass(frozen=True)
class TelemetrySpan:
    trace_id: str
    span_id: str
    span_name: str
    deployment_name: str
    started_at: datetime
    ended_at: datetime | None = None
    parent_span_id: str | None = None
    span_kind: str = "internal"
    status_code: str = "unset"
    status_message: str | None = None
    branch_name: str = "default"
    generation_id: str | None = None
    correlation_id: str | None = None
    source_name: str | None = None
    workflow_id: str | None = None
    run_id: str | None = None
    tool_name: str | None = None
    route_id: str | None = None
    session_id: str | None = None
    attributes: Mapping[str, Any] = field(default_factory=dict)
    payload_summary: Mapping[str, Any] = field(default_factory=dict)
    unavailable_reason: str | None = None
    root_name: str | None = None
    root_kind: str | None = None

    @property
    def duration_ms(self) -> int | None:
        if self.ended_at is None:
            return None
        return max(
            0,
            int((self.ended_at - self.started_at).total_seconds() * 1000),
        )
@dataclass(frozen=True)
class TelemetryMetric:
    metric_name: str
    metric_kind: str
    deployment_name: str
    metric_time: datetime | None = None
    value_double: float | None = None
    value_int: int | None = None
    unit: str | None = None
    branch_name: str = "default"
    trace_id: str | None = None
    span_id: str | None = None
    labels: Mapping[str, Any] = field(default_factory=dict)


def new_trace_id() -> str:
    """Return a non-zero 16-byte trace id as 32 lowercase hex chars."""
    value = uuid.uuid4().hex
    return value if value != "0" * 32 else uuid.uuid4().hex


def new_span_id() -> str:
    """Return a non-zero 8-byte span id as 16 lowercase hex chars."""
    value = uuid.uuid4().hex[:16]
    return value if value != "0" * 16 else uuid.uuid4().hex[:16]


def is_valid_trace_id(value: str | None) -> bool:
    """Return true for W3C/OTel 16-byte lowercase non-zero trace ids."""
    return (
        isinstance(value, str)
        and bool(_TRACE_ID_RE.fullmatch(value))
        and value != "0" * 32
    )


def is_valid_span_id(value: str | None) -> bool:
    """Return true for W3C/OTel 8-byte lowercase non-zero span ids."""
    return (
        isinstance(value, str)
        and bool(_SPAN_ID_RE.fullmatch(value))
        and value != "0" * 16
    )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def current_trace_identity() -> TraceIdentity:
    """Read the active OTel span context, or generate a root identity."""
    local_identity = _CURRENT_TRACE_IDENTITY.get()
    if local_identity is not None:
        return local_identity
    if trace is not None:
        try:
            current = trace.get_current_span()
            ctx = current.get_span_context()
            if ctx is not None and ctx.is_valid:
                return TraceIdentity(
                    trace_id=f"{ctx.trace_id:032x}",
                    span_id=f"{ctx.span_id:016x}",
                    from_otel_context=True,
                )
        except Exception:
            # Telemetry context lookup must never prevent local trace ids.
            pass
    return TraceIdentity(trace_id=new_trace_id(), span_id=new_span_id())


def child_span_identity() -> TraceIdentity:
    """Return an identity for a new span under the active trace."""
    identity = current_trace_identity()
    if identity.is_contextual:
        return TraceIdentity(
            trace_id=identity.trace_id,
            span_id=new_span_id(),
            parent_span_id=identity.span_id,
            from_local_context=identity.from_local_context,
            from_otel_context=identity.from_otel_context,
        )
    return identity


def span_identity_for_trace(
    trace_id: str,
    *,
    parent_span_id: str | None = None,
) -> TraceIdentity:
    """Create a local child identity for a known durable trace id."""
    return TraceIdentity(
        trace_id=trace_id,
        span_id=new_span_id(),
        parent_span_id=parent_span_id,
        from_local_context=True,
    )


def deep_span_capture_enabled() -> bool:
    """Return whether detailed timing labels should become child span rows."""
    value = os.environ.get("OKG_TELEMETRY_DEEP_SPANS", "1")
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def _identity_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first_env(keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = _identity_text(os.environ.get(key))
        if value:
            return value
    return None


@lru_cache(maxsize=1)
def _process_identity_attributes() -> dict[str, Any]:
    host = _identity_text(socket.gethostname()) or "unknown-host"
    pid = os.getpid()
    return {
        "host.name": host,
        "process.pid": pid,
        "okg.executor.hostname": host,
        "okg.executor.pid": str(pid),
        "okg.executor.id": f"{host}:{pid}",
    }


def _thread_identity_attributes() -> dict[str, Any]:
    thread = threading.current_thread()
    native_id = getattr(threading, "get_native_id", lambda: None)()
    thread_id = _identity_text(native_id) or _identity_text(thread.ident)
    thread_name = _identity_text(thread.name)
    attrs: dict[str, Any] = {}
    if thread_id:
        attrs["thread.id"] = thread_id
        attrs["okg.worker.thread.id"] = thread_id
    if thread_name:
        attrs["thread.name"] = thread_name
        attrs["okg.worker.thread.name"] = thread_name
        attrs["okg.worker.id"] = thread_name
        if (
            thread_name.startswith("dbos-executor")
            or thread_name.startswith("queue-worker")
        ):
            attrs["dbos.worker.id"] = thread_name
            if thread_id:
                attrs["dbos.worker.thread.id"] = thread_id
    elif thread_id:
        attrs["okg.worker.id"] = thread_id
    return attrs


def _dbos_process_identity() -> tuple[Any, Any, Any]:
    """This thread's DBOS context, and DBOS's executor id and application
    version for the process.

    Read from DBOS when this process has loaded it. A process that never
    loaded DBOS (a one-shot `okg` read command) has no DBOS context, and
    DBOS would hold the two values it reads from the environment when it is
    imported (`GlobalParams` in `dbos/_utils.py`), so they are read from the
    environment here instead: importing DBOS takes about half a second, and
    the attributes are the same.
    """
    if "dbos" not in sys.modules:
        return (
            None,
            os.environ.get("DBOS__VMID", "local"),
            os.environ.get("DBOS__APPVERSION", ""),
        )
    from dbos import _context as dbos_context  # type: ignore

    global_params = getattr(dbos_context, "GlobalParams", None)
    return (
        dbos_context.get_local_dbos_context(),
        getattr(global_params, "executor_id", None),
        getattr(global_params, "app_version", None),
    )


def _dbos_identity_attributes() -> dict[str, Any]:
    attrs: dict[str, Any] = {}

    executor_id = _first_env(_DBOS_EXECUTOR_ENV_KEYS)
    app_id = _first_env(_DBOS_APP_ENV_KEYS)
    worker_id = _first_env(_DBOS_WORKER_ENV_KEYS)

    try:
        ctx, global_executor, global_app_version = _dbos_process_identity()
        if ctx is not None:
            executor_id = _identity_text(
                getattr(ctx, "executor_id", None)
            ) or executor_id
            app_id = _identity_text(getattr(ctx, "app_id", None)) or app_id
            workflow_id = _identity_text(getattr(ctx, "workflow_id", None))
            if workflow_id:
                attrs["dbos.workflow.id"] = workflow_id
                attrs["okg.workflow.id"] = workflow_id
        if not executor_id:
            global_executor_id = _identity_text(global_executor)
            if global_executor_id and global_executor_id != "local":
                executor_id = global_executor_id
        app_version = _identity_text(global_app_version)
        if app_version:
            attrs["dbos.application.version"] = app_version
    except Exception:
        # DBOS identity is additive telemetry. Missing SDK context must not
        # prevent span persistence.
        pass

    if executor_id:
        attrs["dbos.executor.id"] = executor_id
        attrs["executor.id"] = executor_id
    if app_id:
        attrs["dbos.application.id"] = app_id
    if worker_id:
        attrs["dbos.worker.id"] = worker_id
        attrs["okg.worker.id"] = worker_id
        attrs["worker.id"] = worker_id
    return attrs


def _runtime_identity_attributes() -> dict[str, Any]:
    attrs = dict(_process_identity_attributes())
    attrs.update(_thread_identity_attributes())
    attrs.update(_dbos_identity_attributes())
    return attrs


def _span_storage_attributes(
    attributes: Mapping[str, Any] | None,
    *,
    workflow_id: str | None = None,
) -> dict[str, Any]:
    out = _runtime_identity_attributes()
    if workflow_id:
        out["dbos.workflow.id"] = str(workflow_id)
        out["okg.workflow.id"] = str(workflow_id)
    out.update(dict(attributes or {}))
    return out


def record_context_span_if_missing(
    conn: Any,
    identity: TraceIdentity,
    *,
    deployment_name: str,
    branch_name: str = "default",
    span_name: str = "telemetry.context",
    root_kind: str = "context",
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
    status_code: str = "ok",
    status_message: str | None = None,
    workflow_id: str | None = None,
    attributes: Mapping[str, Any] | None = None,
) -> None:
    """Persist a durable root for an adopted local/OTel context if needed."""
    if not identity.is_contextual:
        return
    storage_attrs = _span_storage_attributes(attributes, workflow_id=workflow_id)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT started_at, ended_at
              FROM okg.telemetry_spans
             WHERE deployment_name = %s
               AND trace_id = %s
               AND span_id = %s
             LIMIT 1
            """,
            (deployment_name, identity.trace_id, identity.span_id),
        )
        if cur.fetchone() is not None:
            if started_at is not None and ended_at is not None:
                cur.execute(
                    """
                    UPDATE okg.telemetry_spans
                       SET started_at = LEAST(started_at, %s),
                           ended_at = CASE
                             WHEN ended_at IS NULL THEN %s
                             ELSE GREATEST(ended_at, %s)
                           END,
                           status_code = CASE
                             WHEN status_code = 'error' OR %s = 'error'
                               THEN 'error'
                             WHEN %s <> 'unset' THEN %s
                             ELSE status_code
                           END,
                           status_message = CASE
                             WHEN %s::text IS NOT NULL THEN %s::text
                             ELSE status_message
                           END,
                           workflow_id = COALESCE(workflow_id, %s),
                           attributes = COALESCE(attributes, '{}'::jsonb)
                             || %s::jsonb,
                           duration_ms = GREATEST(
                             0,
                             (
                               EXTRACT(EPOCH FROM (
                                 CASE
                                   WHEN ended_at IS NULL THEN %s
                                   ELSE GREATEST(ended_at, %s)
                                 END
                                 - LEAST(started_at, %s)
                               )) * 1000
                             )::integer
                           )
                     WHERE deployment_name = %s
                       AND trace_id = %s
                       AND span_id = %s
                    """,
                    (
                        started_at,
                        ended_at,
                        ended_at,
                        status_code,
                        status_code,
                        status_code,
                        status_message,
                        status_message,
                        workflow_id,
                        bounded_jsonb(storage_attrs),
                        ended_at,
                        ended_at,
                        started_at,
                        deployment_name,
                        identity.trace_id,
                        identity.span_id,
                    ),
                )
                cur.execute(
                    """
                    UPDATE okg.telemetry_traces
                       SET started_at = LEAST(started_at, %s),
                           ended_at = CASE
                             WHEN ended_at IS NULL THEN %s
                             ELSE GREATEST(ended_at, %s)
                           END,
                           status_code = CASE
                             WHEN status_code = 'error' OR %s = 'error'
                               THEN 'error'
                             WHEN %s <> 'unset' THEN %s
                             ELSE status_code
                           END,
                           status_message = CASE
                             WHEN %s::text IS NOT NULL THEN %s::text
                             ELSE status_message
                           END,
                           workflow_id = COALESCE(workflow_id, %s),
                           attributes = COALESCE(attributes, '{}'::jsonb)
                             || %s::jsonb,
                           duration_ms = GREATEST(
                             0,
                             (
                               EXTRACT(EPOCH FROM (
                                 CASE
                                   WHEN ended_at IS NULL THEN %s
                                   ELSE GREATEST(ended_at, %s)
                                 END
                                 - LEAST(started_at, %s)
                               )) * 1000
                             )::integer
                           ),
                           updated_at = now()
                     WHERE deployment_name = %s
                       AND trace_id = %s
                    """,
                    (
                        started_at,
                        ended_at,
                        ended_at,
                        status_code,
                        status_code,
                        status_code,
                        status_message,
                        status_message,
                        workflow_id,
                        bounded_jsonb(storage_attrs),
                        ended_at,
                        ended_at,
                        started_at,
                        deployment_name,
                        identity.trace_id,
                    ),
                )
            return

        parent_span_id = identity.parent_span_id
        parent_missing = False
        if parent_span_id is not None:
            cur.execute(
                """
                SELECT 1
                  FROM okg.telemetry_spans
                 WHERE deployment_name = %s
                   AND trace_id = %s
                   AND span_id = %s
                 LIMIT 1
                """,
                (deployment_name, identity.trace_id, parent_span_id),
            )
            parent_missing = cur.fetchone() is None
            if parent_missing:
                parent_span_id = None

    now = utc_now()
    started = started_at or now
    ended = ended_at or started
    span_attrs: dict[str, Any] = {
        "component": "telemetry-context",
        "okg.component": "telemetry-context",
        "okg.deployment.name": deployment_name,
        "okg.branch.name": branch_name,
        "okg.telemetry.context.local": identity.from_local_context,
        "okg.telemetry.context.otel": identity.from_otel_context,
    }
    if attributes:
        span_attrs.update(dict(attributes))
    if parent_missing:
        span_attrs["unavailable"] = {
            "parent_span_id": "telemetry.context_parent_unrecorded",
        }

    record_span(
        conn,
        TelemetrySpan(
            trace_id=identity.trace_id,
            span_id=identity.span_id,
            parent_span_id=parent_span_id,
            span_name=span_name,
            span_kind="internal",
            status_code=status_code,
            status_message=status_message,
            deployment_name=deployment_name,
            branch_name=branch_name,
            workflow_id=workflow_id,
            started_at=started,
            ended_at=ended,
            attributes=span_attrs,
            unavailable_reason=(
                "telemetry.context_parent_unrecorded"
                if parent_missing else None
            ),
            root_name=span_name,
            root_kind=root_kind,
        ),
    )


def trace_identity_from_generation(
    conn: Any,
    *,
    deployment_name: str,
    generation_id: int | str | None,
) -> TraceIdentity | None:
    """Adopt the trace stamped on a generation or telemetry summary."""
    if generation_id is None:
        return None
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT trace_id
              FROM okg.graph_generations
             WHERE deployment_name = %s
               AND generation_id::text = %s
               AND trace_id IS NOT NULL
             ORDER BY created_at DESC
             LIMIT 1
            """,
            (deployment_name, str(generation_id)),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute(
                """
                SELECT trace_id
                  FROM okg.telemetry_traces
                 WHERE deployment_name = %s
                   AND generation_id = %s
                 ORDER BY updated_at DESC
                 LIMIT 1
                """,
                (deployment_name, str(generation_id)),
            )
            row = cur.fetchone()
    if row is None or not row[0]:
        return None
    return span_identity_for_trace(str(row[0]))


def trace_identity_from_latest_source_progress(
    conn: Any,
    *,
    deployment_name: str,
) -> TraceIdentity | None:
    """Adopt the newest source-run trace for a deployment.

    Publisher reservations can run outside an explicit local/OTel
    context. This fallback links the generation row to the latest
    source-sync diagnostic payload rather than minting an unrelated
    trace id.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT value::jsonb->>'trace_id',
                   value::jsonb->>'span_id',
                   value::jsonb->>'parent_span_id'
              FROM okg.source_progress
             WHERE deployment = %s
               AND key = 'sync.last_run'
               AND value IS NOT NULL
               AND value::jsonb ? 'trace_id'
             ORDER BY updated_at DESC
             LIMIT 1
            """,
            (deployment_name,),
        )
        row = cur.fetchone()
    if row is None or not row[0]:
        return None
    parent_span_id = row[2] or row[1] or None
    return span_identity_for_trace(str(row[0]), parent_span_id=parent_span_id)


@contextmanager
def use_trace(
    trace_id: str | None = None,
    *,
    span_id: str | None = None,
    parent_span_id: str | None = None,
) -> Iterator[TraceIdentity]:
    """Bind a trace identity for code paths that do not yet run under OTel."""
    identity = TraceIdentity(
        trace_id=trace_id or new_trace_id(),
        span_id=span_id or new_span_id(),
        parent_span_id=parent_span_id,
        from_local_context=True,
    )
    token = _CURRENT_TRACE_IDENTITY.set(identity)
    try:
        yield identity
    finally:
        _CURRENT_TRACE_IDENTITY.reset(token)


def bounded_jsonb(value: Mapping[str, Any] | None) -> str:
    """Serialize a JSONB payload and truncate by replacing with metadata.

    The default telemetry path stores summaries, not unbounded payloads.
    If the encoded mapping exceeds the configured limit, keep only size
    metadata plus a truncated marker.
    """
    payload: Mapping[str, Any] = value or {}
    encoded = json.dumps(payload, default=str, sort_keys=True)
    stored_bytes = len(encoded.encode("utf-8"))
    if stored_bytes <= _MAX_PAYLOAD_SUMMARY_BYTES:
        return encoded
    return json.dumps(
        {
            "payload_truncated": True,
            "payload_original_bytes": stored_bytes,
            "payload_stored_bytes": 0,
        },
        sort_keys=True,
    )


def sanitized_db_statement_summary(
    statement: str | None,
    *,
    max_chars: int = _MAX_DB_STATEMENT_SUMMARY_CHARS,
) -> str | None:
    """Return a bounded, low-cardinality SQL statement summary.

    The summary keeps operation/table shape but removes common literal
    values. It is suitable for operator diagnostics, not query replay.
    """
    if statement is None:
        return None
    text = str(statement)
    text = _SQL_UUID_RE.sub("?", text)
    text = _SQL_SINGLE_QUOTED_RE.sub("?", text)
    text = _SQL_DOUBLE_QUOTED_RE.sub("?", text)
    text = _SQL_NUMERIC_RE.sub("?", text)
    text = " ".join(text.split())
    if len(text) <= max_chars:
        return text
    return f"{text[:max(0, max_chars - 12)].rstrip()}...<truncated>"


def db_span_attributes(
    operation_name: str,
    *,
    namespace: str = "okg",
    system: str = "postgresql",
    statement: str | None = None,
) -> dict[str, Any]:
    """Return OpenTelemetry-shaped database attributes for one DB span."""
    normalized_operation = str(operation_name or "EXECUTE").upper()
    attributes: dict[str, Any] = {
        "db.system.name": system,
        "db.operation.name": normalized_operation,
        "db.namespace": namespace,
    }
    if statement is not None:
        summary = sanitized_db_statement_summary(statement)
        attributes["db.query.summary"] = summary
        attributes["db.query.text_hash"] = hashlib.sha256(
            str(statement).encode("utf-8", errors="replace"),
        ).hexdigest()
        attributes["okg.telemetry.sql.redacted"] = True
    return attributes


def telemetry_scope_attributes(
    *,
    deployment_name: str,
    branch_name: str = "default",
    component: str | None = None,
    source_name: str | None = None,
    generation_id: str | None = None,
    workflow_id: str | None = None,
    run_id: str | None = None,
    dbos_step: str | None = None,
) -> dict[str, Any]:
    """Return bounded resource/scope attributes shared by durable spans."""
    attributes: dict[str, Any] = {
        "service.name": "akmon",
        "okg.deployment.name": deployment_name,
        "okg.branch.name": branch_name,
    }
    if component:
        attributes["component"] = component
        attributes["okg.component"] = component
    if source_name:
        attributes["okg.source.name"] = source_name
    if generation_id:
        attributes["okg.generation.id"] = generation_id
    if workflow_id:
        attributes["dbos.workflow.id"] = workflow_id
        attributes["okg.workflow.id"] = workflow_id
    if run_id:
        attributes["okg.run.id"] = run_id
    if dbos_step:
        attributes["dbos.step.name"] = dbos_step
    return attributes


def timing_span_attributes(
    label: str,
    *,
    deployment_name: str,
    branch_name: str = "default",
    component: str,
    source_name: str | None = None,
    generation_id: str | None = None,
    workflow_id: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Return OTel-shaped attributes for a measured AKMON timing label."""
    normalized = str(label)
    segments = [part for part in normalized.split(".") if part]
    attributes = telemetry_scope_attributes(
        deployment_name=deployment_name,
        branch_name=branch_name,
        component=component,
        source_name=source_name,
        generation_id=generation_id,
        workflow_id=workflow_id,
        run_id=run_id,
    )
    attributes.update({
        "okg.operation.name": normalized,
        "okg.phase": segments[0] if segments else normalized,
        "okg.span.source": "measured_timing_scope",
    })
    lowered = normalized.lower()
    if "chronos" in lowered:
        attributes["okg.subcomponent"] = "chronos"
        attributes["okg.chronos.operation"] = normalized
    if _timing_label_is_db(normalized):
        attributes.update(
            db_span_attributes(
                _db_operation_name_for_timing_label(normalized),
                statement=normalized,
            )
        )
    return attributes


def timing_span_kind(label: str) -> str:
    """Return an OTel span kind for a measured timing label."""
    return "client" if _timing_label_is_db(label) else "internal"


def _timing_label_is_db(label: str) -> bool:
    lowered = str(label).lower()
    return any(token in lowered for token in _DB_TIMING_TOKENS)


def _db_operation_name_for_timing_label(label: str) -> str:
    lowered = str(label).lower()
    if "select" in lowered or "query" in lowered:
        return "SELECT"
    if "insert" in lowered or "upsert" in lowered or "copy" in lowered:
        return "INSERT"
    if "update" in lowered:
        return "UPDATE"
    if "delete" in lowered:
        return "DELETE"
    if "tx" in lowered or "transaction" in lowered:
        return "TRANSACTION"
    return "EXECUTE"


@dataclass(frozen=True)
class _TimingEvent:
    label: str
    started_perf: float
    ended_perf: float
    attributes: Mapping[str, Any] = field(default_factory=dict)
    unavailable_reason: str | None = None


class SpanTimingRecorder:
    """Accumulate measured timing scopes as durable child spans.

    The source runner and publisher already measure many operations with
    ``perf_counter``. This recorder preserves those measured start/end
    boundaries as span rows and adds hierarchy based on dot-separated labels.
    """

    def __init__(
        self,
        *,
        trace_id: str,
        root_span_id: str,
        root_span_name: str,
        root_started_at: datetime,
        base_perf_counter: float,
        deployment_name: str,
        branch_name: str = "default",
        component: str,
        source_name: str | None = None,
        generation_id: str | None = None,
        workflow_id: str | None = None,
        run_id: str | None = None,
        root_kind: str | None = None,
    ) -> None:
        self.trace_id = trace_id
        self.root_span_id = root_span_id
        self.root_span_name = root_span_name
        self.root_started_at = _as_utc(root_started_at)
        self.base_perf_counter = base_perf_counter
        self.deployment_name = deployment_name
        self.branch_name = branch_name
        self.component = component
        self.source_name = source_name
        self.generation_id = generation_id
        self.workflow_id = workflow_id
        self.run_id = run_id
        self.root_kind = root_kind or component
        self._events: list[_TimingEvent] = []

    def record(
        self,
        label: str,
        started_perf: float,
        *,
        ended_perf: float | None = None,
        attributes: Mapping[str, Any] | None = None,
        unavailable_reason: str | None = None,
    ) -> None:
        ended = time.perf_counter() if ended_perf is None else ended_perf
        self._events.append(
            _TimingEvent(
                label=str(label),
                started_perf=float(started_perf),
                ended_perf=max(float(started_perf), float(ended)),
                attributes=dict(attributes or {}),
                unavailable_reason=unavailable_reason,
            )
        )

    def spans(self) -> list[TelemetrySpan]:
        if not self._events:
            return []
        events = sorted(
            self._events,
            key=lambda event: (event.started_perf, event.ended_perf, event.label),
        )
        group_ranges: dict[tuple[str, ...], tuple[float, float]] = {}
        for event in events:
            parts = _span_label_parts(event.label)
            for depth in range(1, len(parts)):
                prefix = tuple(parts[:depth])
                if not _span_group_prefix_enabled(prefix):
                    continue
                existing = group_ranges.get(prefix)
                if existing is None:
                    group_ranges[prefix] = (event.started_perf, event.ended_perf)
                else:
                    group_ranges[prefix] = (
                        min(existing[0], event.started_perf),
                        max(existing[1], event.ended_perf),
                    )

        events_by_parts = {
            tuple(_span_label_parts(event.label)): event
            for event in events
        }
        group_ids = {prefix: new_span_id() for prefix in group_ranges}

        def parent_id_for(prefix: tuple[str, ...]) -> str:
            for depth in range(len(prefix), 0, -1):
                parent_span_id = group_ids.get(prefix[:depth])
                if parent_span_id:
                    return parent_span_id
            return self.root_span_id

        spans: list[TelemetrySpan] = []
        for prefix in sorted(group_ranges, key=lambda item: (len(item), item)):
            started_perf, ended_perf = group_ranges[prefix]
            exact_event = events_by_parts.get(prefix)
            if exact_event is not None:
                started_perf = min(started_perf, exact_event.started_perf)
                ended_perf = max(ended_perf, exact_event.ended_perf)
            parent_span_id = parent_id_for(prefix[:-1])
            label = ".".join(prefix)
            attributes = {
                "okg.span.role": "group",
                "okg.operation.name": label,
            }
            span_kind = "internal"
            unavailable_reason = None
            if exact_event is not None:
                attributes.update(timing_span_attributes(
                    exact_event.label,
                    deployment_name=self.deployment_name,
                    branch_name=self.branch_name,
                    component=self.component,
                    source_name=self.source_name,
                    generation_id=self.generation_id,
                    workflow_id=self.workflow_id,
                    run_id=self.run_id,
                ))
                attributes.update(dict(exact_event.attributes))
                attributes["okg.span.role"] = "group"
                attributes["okg.operation.name"] = label
                span_kind = timing_span_kind(exact_event.label)
                unavailable_reason = exact_event.unavailable_reason
            spans.append(
                self._span(
                    span_id=group_ids[prefix],
                    parent_span_id=parent_span_id,
                    label=label,
                    started_perf=started_perf,
                    ended_perf=ended_perf,
                    span_kind=span_kind,
                    attributes=attributes,
                    unavailable_reason=unavailable_reason,
                )
            )

        for event in events:
            parts = _span_label_parts(event.label)
            if tuple(parts) in group_ranges:
                continue
            parent_span_id = parent_id_for(tuple(parts[:-1]))
            attributes = timing_span_attributes(
                event.label,
                deployment_name=self.deployment_name,
                branch_name=self.branch_name,
                component=self.component,
                source_name=self.source_name,
                generation_id=self.generation_id,
                workflow_id=self.workflow_id,
                run_id=self.run_id,
            )
            attributes.update(dict(event.attributes))
            spans.append(
                self._span(
                    span_id=new_span_id(),
                    parent_span_id=parent_span_id,
                    label=event.label,
                    started_perf=event.started_perf,
                    ended_perf=event.ended_perf,
                    span_kind=timing_span_kind(event.label),
                    attributes=attributes,
                    unavailable_reason=event.unavailable_reason,
                )
            )
        return spans

    def _span(
        self,
        *,
        span_id: str,
        parent_span_id: str,
        label: str,
        started_perf: float,
        ended_perf: float,
        span_kind: str,
        attributes: Mapping[str, Any],
        unavailable_reason: str | None = None,
    ) -> TelemetrySpan:
        started_at = self._datetime_from_perf(started_perf)
        ended_at = self._datetime_from_perf(ended_perf)
        scope_attrs = telemetry_scope_attributes(
            deployment_name=self.deployment_name,
            branch_name=self.branch_name,
            component=self.component,
            source_name=self.source_name,
            generation_id=self.generation_id,
            workflow_id=self.workflow_id,
            run_id=self.run_id,
        )
        scope_attrs.update(dict(attributes))
        return TelemetrySpan(
            trace_id=self.trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            span_name=f"{self.root_span_name}.{label}",
            span_kind=span_kind,
            status_code="ok",
            deployment_name=self.deployment_name,
            branch_name=self.branch_name,
            generation_id=self.generation_id,
            source_name=self.source_name,
            workflow_id=self.workflow_id,
            run_id=self.run_id,
            started_at=started_at,
            ended_at=ended_at,
            attributes=scope_attrs,
            unavailable_reason=unavailable_reason,
            root_name=self.root_span_name,
            root_kind=self.root_kind,
        )

    def _datetime_from_perf(self, perf_value: float) -> datetime:
        delta_seconds = max(0.0, float(perf_value) - self.base_perf_counter)
        return self.root_started_at + timedelta(seconds=delta_seconds)


def _span_label_parts(label: str) -> list[str]:
    parts = [part.strip() for part in str(label).split(".") if part.strip()]
    return parts or ["operation"]


def _span_group_prefix_enabled(prefix: tuple[str, ...]) -> bool:
    if not prefix:
        return False
    # Phase events are lifecycle markers emitted at several points across a
    # publisher cycle. Treating their namespace as a span creates a misleading
    # envelope that looks like nearly the whole cycle went to event emission.
    # Dirty-scope queries can run multiple times in one publisher cycle; a
    # synthetic namespace span would cover the gap between those runs.
    return prefix[0] not in {"phase_event", "chronos_dirty_scope"}


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def record_span(conn: Any, span: TelemetrySpan) -> None:
    """Insert or update one trace and append one span row.

    The caller owns transaction boundaries. The function accepts any
    psycopg-like connection with ``cursor()``.
    """
    root_name = span.root_name or span.span_name
    root_kind = span.root_kind or span.span_kind
    storage_attrs = _span_storage_attributes(
        span.attributes,
        workflow_id=span.workflow_id,
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.telemetry_traces (
              trace_id, deployment_name, branch_name, root_name, root_kind,
              status_code, status_message, started_at, ended_at, duration_ms,
              generation_id, correlation_id, source_name, workflow_id,
              session_id, attributes, updated_at
            ) VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s::jsonb, now()
            )
            ON CONFLICT (deployment_name, trace_id) DO UPDATE SET
              status_code = CASE
                WHEN okg.telemetry_traces.status_code = 'error'
                  OR EXCLUDED.status_code = 'error'
                  THEN 'error'
                WHEN EXCLUDED.status_code <> 'unset'
                  THEN EXCLUDED.status_code
                ELSE okg.telemetry_traces.status_code
              END,
              status_message = CASE
                WHEN EXCLUDED.status_code = 'error'
                  THEN EXCLUDED.status_message
                ELSE COALESCE(okg.telemetry_traces.status_message,
                              EXCLUDED.status_message)
              END,
              started_at = LEAST(okg.telemetry_traces.started_at,
                                 EXCLUDED.started_at),
              ended_at = CASE
                WHEN okg.telemetry_traces.ended_at IS NULL
                  THEN EXCLUDED.ended_at
                WHEN EXCLUDED.ended_at IS NULL
                  THEN okg.telemetry_traces.ended_at
                ELSE GREATEST(okg.telemetry_traces.ended_at,
                              EXCLUDED.ended_at)
              END,
              duration_ms = CASE
                WHEN EXCLUDED.duration_ms IS NULL
                  THEN okg.telemetry_traces.duration_ms
                ELSE GREATEST(
                  0,
                  (
                    EXTRACT(EPOCH FROM (
                      GREATEST(
                        COALESCE(okg.telemetry_traces.ended_at,
                                 EXCLUDED.ended_at),
                        EXCLUDED.ended_at
                      )
                      - LEAST(okg.telemetry_traces.started_at,
                              EXCLUDED.started_at)
                    )) * 1000
                  )::integer
                )
              END,
              generation_id = COALESCE(EXCLUDED.generation_id,
                                       okg.telemetry_traces.generation_id),
              correlation_id = COALESCE(EXCLUDED.correlation_id,
                                        okg.telemetry_traces.correlation_id),
              source_name = COALESCE(EXCLUDED.source_name,
                                     okg.telemetry_traces.source_name),
              workflow_id = COALESCE(EXCLUDED.workflow_id,
                                     okg.telemetry_traces.workflow_id),
              session_id = COALESCE(EXCLUDED.session_id,
                                    okg.telemetry_traces.session_id),
              updated_at = now()
            """,
            (
                span.trace_id,
                span.deployment_name,
                span.branch_name,
                root_name,
                root_kind,
                span.status_code,
                span.status_message,
                span.started_at,
                span.ended_at,
                span.duration_ms,
                span.generation_id,
                span.correlation_id,
                span.source_name,
                span.workflow_id,
                span.session_id,
                bounded_jsonb(storage_attrs),
            ),
        )
        cur.execute(
            """
            INSERT INTO okg.telemetry_spans (
              event_time, trace_id, span_id, parent_span_id, span_name,
              span_kind, status_code, status_message, started_at, ended_at,
              duration_ms, deployment_name, branch_name, generation_id,
              correlation_id, source_name, workflow_id, run_id, tool_name,
              route_id, session_id, attributes, payload_summary,
              unavailable_reason
            ) VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s
            )
            """,
            (
                span.started_at,
                span.trace_id, span.span_id, span.parent_span_id, span.span_name,
                span.span_kind, span.status_code, span.status_message,
                span.started_at, span.ended_at, span.duration_ms,
                span.deployment_name, span.branch_name, span.generation_id,
                span.correlation_id, span.source_name, span.workflow_id,
                span.run_id, span.tool_name, span.route_id, span.session_id,
                bounded_jsonb(storage_attrs), bounded_jsonb(dict(span.payload_summary)),
                span.unavailable_reason,
            ),
        )


def record_graph_effect_manifest(
    conn: Any,
    *,
    deployment_name: str,
    trace_id: str,
    span_id: str,
    branch_name: str,
    producer_key: str | None,
    manifest: Mapping[str, Any],
) -> None:
    """Persist one bounded graph-effect manifest and its pageable identities."""
    items = [item for item in (manifest.get("items") or []) if isinstance(item, Mapping)]
    counts = {str(key): int(value or 0) for key, value in (manifest.get("counts") or {}).items()}
    total_count = sum(max(0, value) for value in counts.values())
    coverage = str(manifest.get("coverage") or "missing")
    if coverage not in {"exact", "sampled", "aggregate_only", "missing"}:
        coverage = "missing"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.telemetry_graph_effect_manifests (
              deployment_name, trace_id, span_id, branch_name, producer_key,
              input_generation_id, output_generation_id, coverage,
              unavailable_reason, total_count, retained_count, retention_limit,
              counts, recorded_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s::jsonb, now())
            ON CONFLICT (deployment_name, trace_id, span_id) DO UPDATE SET
              branch_name = EXCLUDED.branch_name,
              producer_key = EXCLUDED.producer_key,
              input_generation_id = EXCLUDED.input_generation_id,
              output_generation_id = EXCLUDED.output_generation_id,
              coverage = EXCLUDED.coverage,
              unavailable_reason = EXCLUDED.unavailable_reason,
              total_count = EXCLUDED.total_count,
              retained_count = EXCLUDED.retained_count,
              retention_limit = EXCLUDED.retention_limit,
              counts = EXCLUDED.counts,
              recorded_at = now()
            """,
            (deployment_name, trace_id, span_id, branch_name, producer_key,
             manifest.get("input_generation_id"), manifest.get("output_generation_id"),
             coverage, manifest.get("reason"), total_count, len(items),
             int(manifest.get("limit") or 0), json.dumps(counts, sort_keys=True)),
        )
        cur.execute(
            """DELETE FROM okg.telemetry_graph_effects
                WHERE deployment_name = %s AND trace_id = %s AND span_id = %s
                  AND sequence >= %s""",
            (deployment_name, trace_id, span_id, len(items)),
        )
        for sequence, item in enumerate(items):
            cur.execute(
                """
                INSERT INTO okg.telemetry_graph_effects (
                  deployment_name, trace_id, span_id, sequence, object_kind,
                  object_id, change_kind, object_type, source_id, target_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (deployment_name, trace_id, span_id, sequence)
                DO UPDATE SET object_kind = EXCLUDED.object_kind,
                              object_id = EXCLUDED.object_id,
                              change_kind = EXCLUDED.change_kind,
                              object_type = EXCLUDED.object_type,
                              source_id = EXCLUDED.source_id,
                              target_id = EXCLUDED.target_id
                """,
                (deployment_name, trace_id, span_id, sequence,
                 item.get("object_kind"), str(item.get("object_id")),
                 item.get("change_kind"), item.get("object_type"),
                 item.get("source_id"), item.get("target_id")),
            )


def record_metric(conn: Any, metric: TelemetryMetric) -> None:
    """Append one metric sample to the unified telemetry metric table."""
    metric_time = metric.metric_time or utc_now()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.telemetry_metrics (
              metric_time, metric_name, metric_kind, value_double,
              value_int, unit, deployment_name, branch_name, trace_id,
              span_id, labels
            ) VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
            )
            """,
            (
                metric_time,
                metric.metric_name,
                metric.metric_kind,
                metric.value_double,
                metric.value_int,
                metric.unit,
                metric.deployment_name,
                metric.branch_name,
                metric.trace_id,
                metric.span_id,
                bounded_jsonb(dict(metric.labels)),
            ),
        )
