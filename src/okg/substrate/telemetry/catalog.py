from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import time
from typing import Any

import psycopg

from okg.substrate.telemetry import (
    TelemetryMetric,
    TelemetrySpan,
    new_span_id,
    new_trace_id,
    record_metric,
    record_span,
)
from okg.substrate.db.connection import connect as db_connect

DEFAULT_SINCE_SECONDS = 24 * 60 * 60
DEFAULT_WRITE_BUDGET_MS = 5.0
DEFAULT_PAYLOAD_BUDGET_BYTES = 8 * 1024


def _metric(
    name: str,
    *,
    category: str,
    unit: str | None,
    value: int | float | None,
    status: str = "ok",
    source: str,
    unavailable_reason: str | None = None,
    labels: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "category": category,
        "unit": unit,
        "value": value,
        "status": status,
        "source": source,
        "unavailable_reason": unavailable_reason,
        "labels": labels or {},
    }


def _scalar(
    cur: psycopg.Cursor,
    sql: str,
    params: tuple[Any, ...],
    default: Any = 0,
) -> Any:
    cur.execute(sql, params)
    row = cur.fetchone()
    if row is None or row[0] is None:
        return default
    return row[0]


def _optional_relation_ready(
    cur: psycopg.Cursor,
    relation: str,
    required_columns: set[str],
) -> tuple[bool, str | None]:
    cur.execute(
        """
        SELECT attname
          FROM pg_attribute
         WHERE attrelid = to_regclass(%s)
           AND attnum > 0
           AND NOT attisdropped
        """,
        (relation,),
    )
    columns = {str(row[0]) for row in cur.fetchall()}
    if not columns:
        return False, f"{relation}_not_present"
    missing = sorted(required_columns - columns)
    if missing:
        return False, f"{relation}_missing_columns:{','.join(missing)}"
    return True, None


def _unavailable_optional_metrics(
    definitions: list[tuple[str, str, str | None]],
    *,
    source: str,
    reason: str,
) -> list[dict[str, Any]]:
    return [
        _metric(
            name,
            category=category,
            unit=unit,
            value=None,
            status="unavailable",
            source=source,
            unavailable_reason=reason,
        )
        for name, category, unit in definitions
    ]


def _llm_metrics(
    cur: psycopg.Cursor,
    *,
    deployment: str | None,
    window_start: datetime,
    window_end: datetime,
) -> list[dict[str, Any]]:
    """The four ``llm.*`` metrics, through the shared ``llm_usage_summary``.

    Issue #2573 slice 2 (closes #1739). ``llm.calls.count`` counts every call,
    failed ones included, as it always has. Tokens, cost and latency are read
    over successful calls, as every other usage reader does: a failed call
    records no usage, so counting it would call every window with one failure
    "unpriced". A measure some successful call did not record is ``value:
    null`` with ``status: "unknown"`` and its reason in
    ``unavailable_reason`` (for cost, ``unpriced_calls:<n>_of_<m>``), and the
    labels carry the recorded part -- never a sum that reads as complete.
    """
    from okg.substrate.telemetry.llm_usage import (
        LlmUsageFilters,
        llm_usage_summary,
    )
    from okg.substrate.unknown import Unknown

    window = {
        "deployment": deployment,
        "window_start": window_start,
        "window_end": window_end,
    }
    (every_call,) = llm_usage_summary(
        cur, filters=LlmUsageFilters(success_only=False, **window)
    )
    (usage,) = llm_usage_summary(cur, filters=LlmUsageFilters(**window))

    def measured(
        name: str,
        unit: str,
        value: Any,
        labels: dict[str, Any],
    ) -> dict[str, Any]:
        if isinstance(value, Unknown):
            return _metric(
                name,
                category="llm",
                unit=unit,
                value=None,
                status="unknown",
                source="okg.llm_calls",
                unavailable_reason=value.reason,
                labels={"successful_calls": usage.calls, **labels},
            )
        return _metric(
            name,
            category="llm",
            unit=unit,
            value=value,
            source="okg.llm_calls",
        )

    return [
        _metric(
            "llm.calls.count",
            category="llm",
            unit="count",
            value=every_call.calls,
            source="okg.llm_calls",
        ),
        measured(
            "llm.tokens.total",
            "tokens",
            usage.tokens("total_tokens"),
            {
                "missing_calls": int(usage.missing_tokens["total_tokens"]),
                "recorded_total_tokens": int(usage.recorded_tokens["total_tokens"]),
            },
        ),
        measured(
            "llm.cost.usd",
            "usd",
            usage.cost_usd,
            {
                "unpriced_calls": usage.unpriced_calls,
                "priced_cost_usd": usage.priced_cost_usd,
            },
        ),
        measured(
            "llm.latency.avg_ms",
            "ms",
            usage.latency_avg_ms,
            {"missing_calls": usage.latency_missing},
        ),
    ]


def telemetry_metrics_catalog(
    conn: psycopg.Connection,
    *,
    deployment: str,
    since_seconds: int = DEFAULT_SINCE_SECONDS,
    start_at: datetime | None = None,
    end_at: datetime | None = None,
) -> dict[str, Any]:
    """Return the unified operator metric catalog from durable state."""
    generated_at = datetime.now(timezone.utc)
    window_end = end_at or generated_at
    window_start = start_at or window_end - timedelta(seconds=max(1, int(since_seconds)))
    metrics: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        metrics.extend([
            _metric(
                "telemetry.traces.count",
                category="telemetry",
                unit="count",
                value=int(_scalar(
                    cur,
                    """
                    SELECT count(*)
                      FROM okg.telemetry_traces
                     WHERE deployment_name = %s
                       AND started_at < %s
                       AND COALESCE(ended_at, started_at) > %s
                    """,
                    (deployment, window_end, window_start),
                )),
                source="okg.telemetry_traces",
            ),
            _metric(
                "telemetry.spans.count",
                category="telemetry",
                unit="count",
                value=int(_scalar(
                    cur,
                    """
                    SELECT count(*)
                      FROM okg.telemetry_spans
                     WHERE deployment_name = %s
                       AND started_at < %s
                       AND COALESCE(ended_at, started_at) > %s
                    """,
                    (deployment, window_end, window_start),
                )),
                source="okg.telemetry_spans",
            ),
            _metric(
                "telemetry.spans.payload_bytes",
                category="telemetry",
                unit="bytes",
                value=int(_scalar(
                    cur,
                    """
                    SELECT COALESCE(sum(
                      length(attributes::text) + length(payload_summary::text)
                    ), 0)
                      FROM okg.telemetry_spans
                     WHERE deployment_name = %s
                       AND started_at < %s
                       AND COALESCE(ended_at, started_at) > %s
                    """,
                    (deployment, window_end, window_start),
                )),
                source="okg.telemetry_spans",
            ),
            _metric(
                "telemetry.metrics.count",
                category="telemetry",
                unit="count",
                value=int(_scalar(
                    cur,
                    """
                    SELECT count(*)
                      FROM okg.telemetry_metrics
                     WHERE deployment_name = %s
                       AND metric_time >= %s
                       AND metric_time < %s
                    """,
                    (deployment, window_start, window_end),
                )),
                source="okg.telemetry_metrics",
            ),
            _metric(
                "profile.artifacts.count",
                category="profile",
                unit="count",
                value=int(_scalar(
                    cur,
                    """
                    SELECT count(*)
                      FROM okg.telemetry_artifacts a
                      JOIN okg.telemetry_traces t
                        ON t.deployment_name = a.deployment_name
                       AND t.trace_id = a.trace_id
                     WHERE t.deployment_name = %s
                       AND a.created_at >= %s
                       AND a.created_at < %s
                    """,
                    (deployment, window_start, window_end),
                )),
                source="okg.telemetry_artifacts",
            ),
        ])

        cur.execute(
            """
            SELECT
              count(*),
              COALESCE(avg(duration_ms), 0),
              COALESCE(sum((payload_summary->>'response_bytes')::bigint)
                FILTER (WHERE payload_summary ? 'response_bytes'), 0),
              COALESCE(sum(CASE WHEN status_code = 'error' THEN 1 ELSE 0 END), 0)
              FROM okg.telemetry_spans
             WHERE deployment_name = %s
               AND tool_name IS NOT NULL
               AND started_at < %s
               AND COALESCE(ended_at, started_at) > %s
            """,
            (deployment, window_end, window_start),
        )
        mcp_count, mcp_avg_ms, mcp_response_bytes, mcp_errors = cur.fetchone()
        metrics.extend([
            _metric(
                "mcp.calls.count",
                category="mcp",
                unit="count",
                value=int(mcp_count or 0),
                source="okg.telemetry_spans",
            ),
            _metric(
                "mcp.errors.count",
                category="mcp",
                unit="count",
                value=int(mcp_errors or 0),
                source="okg.telemetry_spans",
            ),
            _metric(
                "mcp.latency.avg_ms",
                category="mcp",
                unit="ms",
                value=float(mcp_avg_ms or 0.0),
                source="okg.telemetry_spans",
            ),
            _metric(
                "mcp.response.bytes",
                category="mcp",
                unit="bytes",
                value=int(mcp_response_bytes or 0),
                source="okg.telemetry_spans",
            ),
        ])

        llm_metrics = [
            ("llm.calls.count", "llm", "count"),
            ("llm.tokens.total", "llm", "tokens"),
            ("llm.cost.usd", "llm", "usd"),
            ("llm.latency.avg_ms", "llm", "ms"),
        ]
        llm_ready, llm_reason = _optional_relation_ready(
            cur,
            "okg.llm_calls",
            {
                "deployment_name", "ts", "success", "prompt_tokens",
                "completion_tokens", "total_tokens", "cost_usd", "latency_ms",
            },
        )
        if llm_ready:
            metrics.extend(_llm_metrics(
                cur,
                deployment=deployment,
                window_start=window_start,
                window_end=window_end,
            ))
        else:
            metrics.extend(_unavailable_optional_metrics(
                llm_metrics,
                source="okg.llm_calls",
                reason=llm_reason or "okg.llm_calls_unavailable",
            ))

        cur.execute(
            """
            SELECT
              count(*),
              COALESCE(sum(duration_ms), 0)
              FROM okg.telemetry_spans
             WHERE deployment_name = %s
               AND source_name IS NOT NULL
               AND started_at < %s
               AND COALESCE(ended_at, started_at) > %s
            """,
            (deployment, window_end, window_start),
        )
        source_count, source_duration_ms = cur.fetchone()
        metrics.extend([
            _metric(
                "source.runs.count",
                category="source",
                unit="count",
                value=int(source_count or 0),
                source="okg.telemetry_spans",
            ),
            _metric(
                "source.runs.duration_ms",
                category="source",
                unit="ms",
                value=int(source_duration_ms or 0),
                source="okg.telemetry_spans",
            ),
        ])

        source_ready, source_reason = _optional_relation_ready(
            cur,
            "okg.source_progress",
            {"deployment"},
        )
        if source_ready:
            metrics.append(_metric(
                "source.freshness.rows",
                category="source",
                unit="count",
                value=int(_scalar(
                    cur,
                    """
                    SELECT count(*)
                      FROM okg.source_progress
                     WHERE deployment = %s
                    """,
                    (deployment,),
                )),
                source="okg.source_progress",
            ))
        else:
            metrics.extend(_unavailable_optional_metrics(
                [("source.freshness.rows", "source", "count")],
                source="okg.source_progress",
                reason=source_reason or "okg.source_progress_unavailable",
            ))

        generation_metrics = [
            ("publisher.generations.count", "publisher", "count"),
            ("publisher.generations.published", "publisher", "count"),
            ("generation.delta.nodes", "publisher", "count"),
            ("generation.delta.edges", "publisher", "count"),
        ]
        generation_ready, generation_reason = _optional_relation_ready(
            cur,
            "okg.graph_generations",
            {
                "deployment_name",
                "created_at",
                "status",
                "nodes_added",
                "nodes_retracted",
                "edges_added",
                "edges_retracted",
            },
        )
        if generation_ready:
            cur.execute(
                """
                SELECT
                  count(*),
                  COALESCE(
                    sum(CASE WHEN status = 'published' THEN 1 ELSE 0 END),
                    0
                  ),
                  COALESCE(sum(nodes_added + nodes_retracted), 0),
                  COALESCE(sum(edges_added + edges_retracted), 0)
                  FROM okg.graph_generations
                 WHERE deployment_name = %s
                   AND created_at >= %s
                   AND created_at < %s
                """,
                (deployment, window_start, window_end),
            )
            generation_count, published_count, node_delta, edge_delta = (
                cur.fetchone()
            )
            metrics.extend([
                _metric(
                    "publisher.generations.count",
                    category="publisher",
                    unit="count",
                    value=int(generation_count or 0),
                    source="okg.graph_generations",
                ),
                _metric(
                    "publisher.generations.published",
                    category="publisher",
                    unit="count",
                    value=int(published_count or 0),
                    source="okg.graph_generations",
                ),
                _metric(
                    "generation.delta.nodes",
                    category="publisher",
                    unit="count",
                    value=int(node_delta or 0),
                    source="okg.graph_generations",
                ),
                _metric(
                    "generation.delta.edges",
                    category="publisher",
                    unit="count",
                    value=int(edge_delta or 0),
                    source="okg.graph_generations",
                ),
            ])
        else:
            metrics.extend(_unavailable_optional_metrics(
                generation_metrics,
                source="okg.graph_generations",
                reason=(
                    generation_reason
                    or "okg.graph_generations_unavailable"
                ),
            ))

        latest_overhead = _scalar(
            cur,
            """
            SELECT value_double
              FROM okg.telemetry_metrics
             WHERE deployment_name = %s
               AND metric_name = 'telemetry.overhead.write_ms'
             ORDER BY metric_time DESC
             LIMIT 1
            """,
            (deployment,),
            None,
        )
        metrics.append(_metric(
            "telemetry.overhead.latest_write_ms",
            category="telemetry",
            unit="ms",
            value=float(latest_overhead) if latest_overhead is not None else None,
            status="ok" if latest_overhead is not None else "unavailable",
            source="okg.telemetry_metrics",
            unavailable_reason=(
                None if latest_overhead is not None
                else "telemetry_overhead_benchmark_not_run"
            ),
        ))

    metrics.extend([
        _metric(
            "queue.depth",
            category="runtime",
            unit="count",
            value=None,
            status="unavailable",
            source="dbos_queue",
            unavailable_reason="dbos_queue_metric_collector_not_configured",
        ),
        _metric(
            "queue.dlq.count",
            category="runtime",
            unit="count",
            value=None,
            status="unavailable",
            source="dbos_queue",
            unavailable_reason="dbos_dlq_metric_collector_not_configured",
        ),
        _metric(
            "workflow.dwell_ms",
            category="runtime",
            unit="ms",
            value=None,
            status="unavailable",
            source="dbos_workflow",
            unavailable_reason="dbos_workflow_metric_collector_not_configured",
        ),
        _metric(
            "query.latency.avg_ms",
            category="query",
            unit="ms",
            value=None,
            status="unavailable",
            source="query_instrumentation",
            unavailable_reason="query_timing_collector_not_configured",
        ),
    ])

    return {
        "schema": "okg.telemetry.metrics_catalog.v1",
        "deployment": deployment,
        "since_seconds": since_seconds,
        "generated_at": generated_at.isoformat(),
        "metric_count": len(metrics),
        "metrics": metrics,
        "unavailable_count": sum(
            1 for metric in metrics if metric["status"] == "unavailable"
        ),
        "unknown_count": sum(
            1 for metric in metrics if metric["status"] == "unknown"
        ),
    }


def telemetry_contract_report() -> dict[str, Any]:
    return {
        "schema": "okg.telemetry.contract_report.v1",
        "ok": True,
        "instrumentation_api": "OpenTelemetry",
        "durable_store": "okg.telemetry_traces/spans/events/metrics/artifacts",
        "payload_summary_limit_bytes": DEFAULT_PAYLOAD_BUDGET_BYTES,
        "retention": {
            "spans": "30 days",
            "events": "30 days",
            "metrics": "30 days",
            "compressed_after": "7 days",
        },
        "normal_correlation_requires_raw_payloads": False,
        "current_producers": [
            "SourceRunner",
            "Publisher generation metadata",
            "MCP tool tracing",
            "LLM call tracking",
        ],
        "unavailable_collectors": [
            "DBOS workflow queue/dwell metrics",
            "query timing collector",
            "system/database flamegraphs",
        ],
    }


def telemetry_overhead_benchmark(
    dsn: str,
    *,
    deployment: str,
    write_budget_ms: float = DEFAULT_WRITE_BUDGET_MS,
    payload_budget_bytes: int = DEFAULT_PAYLOAD_BUDGET_BYTES,
) -> dict[str, Any]:
    scenarios = [
        ("source_run", "source.telemetry-bench", "internal"),
        ("mcp_call", "mcp.telemetry_bench", "server"),
    ]
    rows: list[dict[str, Any]] = []
    with db_connect(dsn, autocommit=True) as conn:
        for scenario, span_name, span_kind in scenarios:
            trace_id = new_trace_id()
            span_id = new_span_id()
            payload = {
                "scenario": scenario,
                "records": 1,
                "response_bytes": 128 if scenario == "mcp_call" else None,
            }
            minimal_cpu_start = time.process_time()
            minimal_start = time.perf_counter()
            _write_minimal_runtime_probe(
                conn,
                deployment=deployment,
                scenario=scenario,
                trace_id=trace_id,
                payload=payload,
            )
            minimal_ms = (time.perf_counter() - minimal_start) * 1000
            minimal_cpu_ms = (time.process_time() - minimal_cpu_start) * 1000

            before_spans = _count_rows(
                conn, "okg.telemetry_spans", deployment=deployment
            )
            before_traces = _count_rows(
                conn, "okg.telemetry_traces", deployment=deployment
            )
            write_cpu_start = time.process_time()
            write_start = time.perf_counter()
            now = datetime.now(timezone.utc)
            _write_minimal_runtime_probe(
                conn,
                deployment=deployment,
                scenario=scenario,
                trace_id=trace_id,
                payload=payload,
            )
            record_span(
                conn,
                TelemetrySpan(
                    trace_id=trace_id,
                    span_id=span_id,
                    span_name=span_name,
                    span_kind=span_kind,
                    status_code="ok",
                    deployment_name=deployment,
                    source_name=(
                        "telemetry-bench" if scenario == "source_run"
                        else None
                    ),
                    tool_name=(
                        "telemetry_bench" if scenario == "mcp_call"
                        else None
                    ),
                    started_at=now,
                    ended_at=now,
                    attributes={
                        "component": "telemetry-benchmark",
                        "scenario": scenario,
                    },
                    payload_summary=payload,
                    root_name=span_name,
                    root_kind=scenario,
                ),
            )
            path_with_telemetry_ms = (time.perf_counter() - write_start) * 1000
            write_cpu_ms = (time.process_time() - write_cpu_start) * 1000
            telemetry_write_ms = max(0.0, path_with_telemetry_ms - minimal_ms)
            telemetry_cpu_ms = max(0.0, write_cpu_ms - minimal_cpu_ms)
            after_spans = _count_rows(
                conn, "okg.telemetry_spans", deployment=deployment
            )
            after_traces = _count_rows(
                conn, "okg.telemetry_traces", deployment=deployment
            )
            payload_bytes = _span_payload_bytes(conn, trace_id, span_id)
            inserted_rows = (
                (after_spans - before_spans)
                + (after_traces - before_traces)
            )
            status = (
                "pass"
                if telemetry_write_ms <= write_budget_ms
                and payload_bytes <= payload_budget_bytes
                and inserted_rows >= 2
                else "fail"
            )
            row = {
                "scenario": scenario,
                "status": status,
                "minimal_ms": round(minimal_ms, 3),
                "minimal_cpu_ms": round(minimal_cpu_ms, 3),
                "path_with_telemetry_ms": round(path_with_telemetry_ms, 3),
                "telemetry_write_ms": round(telemetry_write_ms, 3),
                "telemetry_cpu_ms": round(telemetry_cpu_ms, 3),
                "latency_overhead_ms": round(telemetry_write_ms, 3),
                "cpu_overhead_ms": round(telemetry_cpu_ms, 3),
                "inserted_rows": inserted_rows,
                "payload_bytes": payload_bytes,
                "write_budget_ms": write_budget_ms,
                "payload_budget_bytes": payload_budget_bytes,
                "trace_id": trace_id,
                "span_id": span_id,
            }
            rows.append(row)
            record_metric(
                conn,
                TelemetryMetric(
                    metric_name="telemetry.overhead.write_ms",
                    metric_kind="gauge",
                    value_double=telemetry_write_ms,
                    unit="ms",
                    deployment_name=deployment,
                    trace_id=trace_id,
                    span_id=span_id,
                    labels={"scenario": scenario},
                ),
            )
            record_metric(
                conn,
                TelemetryMetric(
                    metric_name="telemetry.overhead.payload_bytes",
                    metric_kind="gauge",
                    value_int=payload_bytes,
                    unit="bytes",
                    deployment_name=deployment,
                    trace_id=trace_id,
                    span_id=span_id,
                    labels={"scenario": scenario},
                ),
            )
    return {
        "schema": "okg.telemetry.overhead_benchmark.v1",
        "ok": all(row["status"] == "pass" for row in rows),
        "deployment": deployment,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows": rows,
        "row_count": len(rows),
        "write_budget_ms": write_budget_ms,
        "payload_budget_bytes": payload_budget_bytes,
    }


def _write_minimal_runtime_probe(
    conn: psycopg.Connection,
    *,
    deployment: str,
    scenario: str,
    trace_id: str,
    payload: dict[str, Any],
) -> None:
    if scenario == "source_run":
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.source_progress
                  (deployment, source_name, key, value)
                VALUES (%s, 'telemetry-bench', %s, %s)
                ON CONFLICT (deployment, source_name, key)
                  DO UPDATE SET value = EXCLUDED.value,
                                updated_at = now()
                """,
                (
                    deployment,
                    f"sync.bench.{trace_id}",
                    json.dumps(payload, sort_keys=True),
                ),
            )
        return
    if scenario == "mcp_call":
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.mcp_trace_events (
                  session_id, deployment_name, generation_id, tool_name,
                  question_label, request_json, response_json, error_text,
                  duration_ms, branch_name
                ) VALUES (%s, %s, NULL, 'telemetry_bench', NULL,
                          %s::jsonb, %s::jsonb, NULL, 1, 'default')
                """,
                (
                    f"telemetry-bench-{trace_id[:12]}",
                    deployment,
                    json.dumps({"scenario": scenario}, sort_keys=True),
                    json.dumps(payload, sort_keys=True),
                ),
            )
        return
    raise ValueError(f"unknown telemetry benchmark scenario: {scenario}")


def _count_rows(
    conn: psycopg.Connection,
    relation: str,
    *,
    deployment: str,
) -> int:
    column = "deployment_name"
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {relation} WHERE {column} = %s",
            (deployment,),
        )
        return int(cur.fetchone()[0])


def _span_payload_bytes(
    conn: psycopg.Connection,
    trace_id: str,
    span_id: str,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT length(attributes::text) + length(payload_summary::text)
              FROM okg.telemetry_spans
             WHERE trace_id = %s AND span_id = %s
             ORDER BY event_time DESC
             LIMIT 1
            """,
            (trace_id, span_id),
        )
        row = cur.fetchone()
        return int(row[0] if row and row[0] is not None else 0)
