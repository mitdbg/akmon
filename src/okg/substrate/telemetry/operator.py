from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg

from okg.substrate.telemetry.catalog import telemetry_metrics_catalog
from okg.substrate.telemetry.profile import profile_report
from okg.substrate.telemetry.store import TelemetryMetric, record_metric

_PERFORMANCE_REQUIRED_RELATIONS = (
    "okg.v_telemetry_trace_summary",
    "okg.v_telemetry_span_tree",
    "okg.telemetry_artifacts",
)

_GAP_CAUSE_KIND = {
    "before_first_child": "span_shape",
    "between_children": "span_shape",
    "after_last_child": "span_shape",
    "leaf_operation": "expected_leaf",
    "missing_child_span": "instrumentation_debt",
    "queue_wait": "wait",
    "external_wait": "wait",
    "profiler_only": "profile_only",
    "parallel_normalization": "normalization",
    "unknown": "instrumentation_debt",
}
_GAP_CAUSE_DEBT = {"missing_child_span", "unknown"}
_TELEMETRY_COVERAGE_RECORD_METRIC_LIMIT = 50


def trace_list(
    conn: psycopg.Connection,
    *,
    deployment: str,
    slow: bool = False,
    limit: int = 20,
) -> dict[str, Any]:
    order = "slowest_span_ms DESC NULLS LAST, started_at DESC" if slow else (
        "started_at DESC"
    )
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT trace_id, root_name, root_kind, status_code,
                   started_at, ended_at, duration_ms, generation_id,
                   source_name, workflow_id, session_id, span_count,
                   slowest_span_ms, slowest_span_name, artifact_count
              FROM okg.v_telemetry_trace_summary
             WHERE deployment_name = %s
             ORDER BY {order}
             LIMIT %s
            """,
            (deployment, int(limit)),
        )
        rows = cur.fetchall()
    return {
        "schema": "okg.telemetry.trace_list.v1",
        "deployment": deployment,
        "slow": slow,
        "trace_count": len(rows),
        "traces": [
            {
                "trace_id": row[0],
                "root_name": row[1],
                "root_kind": row[2],
                "status_code": row[3],
                "started_at": row[4].isoformat() if row[4] else None,
                "ended_at": row[5].isoformat() if row[5] else None,
                "duration_ms": row[6],
                "generation_id": row[7],
                "source_name": row[8],
                "workflow_id": row[9],
                "session_id": row[10],
                "span_count": int(row[11] or 0),
                "slowest_span_ms": row[12],
                "slowest_span_name": row[13],
                "artifact_count": int(row[14] or 0),
            }
            for row in rows
        ],
    }


def telemetry_coverage(
    conn: psycopg.Connection,
    *,
    deployment: str,
    since_seconds: int = 24 * 60 * 60,
    limit: int | None = None,
    start_at: datetime | None = None,
    end_at: datetime | None = None,
    scope: str = "system",
    trace_id: str | None = None,
    workflow_id: str | None = None,
    generation_id: str | None = None,
    source_name: str | None = None,
    run_id: str | None = None,
    component: str | None = None,
    unknown_gap_budget_ms: float = 0.0,
    instrumentation_debt_budget_ms: float = 0.0,
    unknown_gap_budget_count: int = 0,
    budget_action: str = "warn",
) -> dict[str, Any]:
    """Return parent/child wall-clock coverage for telemetry spans."""
    effective_scope = _normalize_telemetry_scope(scope)
    _validate_scope_filter(
        scope=effective_scope,
        trace_id=trace_id,
        workflow_id=workflow_id,
        generation_id=generation_id,
        source_name=source_name,
        run_id=run_id,
        component=component,
    )
    missing_relations = _missing_performance_relations(conn)
    window_start, window_end = _flamegraph_window(
        since_seconds=since_seconds,
        start_at=start_at,
        end_at=end_at,
    )
    base_payload = {
        "schema": "okg.telemetry.coverage.v1",
        "deployment": deployment,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "since_seconds": int(since_seconds),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "limit": int(limit) if limit is not None else None,
        "scope": effective_scope,
        "filters": _scope_filter_payload(
            trace_id=trace_id,
            workflow_id=workflow_id,
            generation_id=generation_id,
            source_name=source_name,
            run_id=run_id,
            component=component,
        ),
    }
    if missing_relations:
        summary = _coverage_summary([])
        budget = _coverage_budget(
            summary,
            unknown_gap_budget_ms=unknown_gap_budget_ms,
            instrumentation_debt_budget_ms=instrumentation_debt_budget_ms,
            unknown_gap_budget_count=unknown_gap_budget_count,
            budget_action=budget_action,
        )
        return {
            **base_payload,
            "status": "unavailable",
            "unavailable_reason": "telemetry_relation_not_present",
            "summary": summary,
            "budget": budget,
            "budget_status": budget["status"],
            "trace_count": 0,
            "span_count": 0,
            "candidate_span_count": 0,
            "record_count": 0,
            "truncated": False,
            "omitted_span_count": 0,
            "records": [],
            "top_unattributed": [],
            "unavailable": [
                {"kind": "schema", "name": relation,
                 "reason": "telemetry_relation_not_present"}
                for relation in missing_relations
            ],
        }

    spans = _window_spans(
        conn,
        deployment=deployment,
        window_start=window_start,
        window_end=window_end,
        limit=limit,
        scope=effective_scope,
        trace_id=trace_id,
        workflow_id=workflow_id,
        generation_id=generation_id,
        source_name=source_name,
        run_id=run_id,
        component=component,
    )
    records = _coverage_records_for_spans(
        spans,
        window_start=window_start,
        window_end=window_end,
    )
    summary = _coverage_summary(records)
    budget = _coverage_budget(
        summary,
        unknown_gap_budget_ms=unknown_gap_budget_ms,
        instrumentation_debt_budget_ms=instrumentation_debt_budget_ms,
        unknown_gap_budget_count=unknown_gap_budget_count,
        budget_action=budget_action,
    )
    trace_count = len({
        str(span.get("trace_id"))
        for span in spans
        if span.get("trace_id") is not None
    })
    candidate_span_count = _window_span_count(
        conn,
        deployment=deployment,
        window_start=window_start,
        window_end=window_end,
        scope=effective_scope,
        trace_id=trace_id,
        workflow_id=workflow_id,
        generation_id=generation_id,
        source_name=source_name,
        run_id=run_id,
        component=component,
    )
    truncated = limit is not None and candidate_span_count > max(1, int(limit))
    status = "available" if records else "unavailable"
    return {
        **base_payload,
        "status": status,
        "unavailable_reason": None if status == "available" else (
            "no_telemetry_spans_in_window"
        ),
        "summary": summary,
        "budget": budget,
        "budget_status": budget["status"],
        "trace_count": trace_count,
        "span_count": len(spans),
        "record_count": len(records),
        "candidate_span_count": candidate_span_count,
        "truncated": truncated,
        "omitted_span_count": (
            max(0, candidate_span_count - len(spans)) if truncated else 0
        ),
        "records": records,
        "top_unattributed": sorted(
            records,
            key=lambda row: (
                -float(row.get("self_unattributed_ms") or 0.0),
                str(row.get("span_name") or ""),
            ),
        )[:10],
        "unavailable": [],
    }


def record_telemetry_coverage_metrics(
    conn: psycopg.Connection,
    payload: dict[str, Any],
    *,
    branch_name: str = "default",
) -> int:
    """Persist compact coverage facts as queryable telemetry metric rows."""
    if payload.get("schema") != "okg.telemetry.coverage.v1":
        raise ValueError("coverage metrics require okg.telemetry.coverage.v1 payload")
    if payload.get("status") != "available":
        return 0
    summary = payload.get("summary") or {}
    labels = {
        "schema": payload.get("schema"),
        "scope": payload.get("scope"),
        "filters": payload.get("filters") or {},
        "window_start": payload.get("window_start"),
        "window_end": payload.get("window_end"),
        "trace_count": payload.get("trace_count"),
        "span_count": payload.get("span_count"),
        "record_count": payload.get("record_count"),
        "top_gap_causes": summary.get("top_gap_causes") or [],
    }
    deployment = str(payload.get("deployment") or "")
    metric_time = _as_utc_datetime(
        datetime.fromisoformat(
            str(payload.get("generated_at")).replace("Z", "+00:00")
        )
    ) if payload.get("generated_at") else datetime.now(timezone.utc)
    metrics = [
        ("telemetry.coverage.coverage_pct", "gauge", "percent", summary.get("coverage_pct")),
        ("telemetry.coverage.parent_duration_ms", "gauge", "ms", summary.get("parent_duration_ms")),
        ("telemetry.coverage.direct_child_covered_ms", "gauge", "ms", summary.get("direct_child_covered_ms")),
        ("telemetry.coverage.self_unattributed_ms", "gauge", "ms", summary.get("self_unattributed_ms")),
        ("telemetry.coverage.parallel_overflow_ms", "gauge", "ms", summary.get("parallel_overflow_ms")),
        ("telemetry.coverage.instrumentation_debt_ms", "gauge", "ms", summary.get("instrumentation_debt_ms")),
        ("telemetry.coverage.unknown_gap_count", "gauge", "count", summary.get("unknown_gap_count")),
        ("telemetry.coverage.gap_count", "gauge", "count", summary.get("gap_count")),
    ]
    count = 0
    for name, kind, unit, value in metrics:
        if value is None:
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        record_metric(
            conn,
            TelemetryMetric(
                metric_name=name,
                metric_kind=kind,
                metric_time=metric_time,
                value_double=numeric if unit != "count" else None,
                value_int=int(numeric) if unit == "count" else None,
                unit=unit,
                deployment_name=deployment,
                branch_name=branch_name,
                labels=labels,
            ),
        )
        count += 1
    for record in sorted(
        payload.get("records") or [],
        key=lambda row: (
            -float(row.get("self_unattributed_ms") or 0.0),
            str(row.get("span_name") or ""),
        ),
    )[:_TELEMETRY_COVERAGE_RECORD_METRIC_LIMIT]:
        record_labels = {
            **labels,
            "metric_grain": "coverage_record",
            "trace_id": record.get("trace_id"),
            "span_id": record.get("span_id"),
            "span_name": record.get("span_name"),
            "component": record.get("component"),
            "workflow_id": record.get("workflow_id"),
            "generation_id": record.get("generation_id"),
            "source_name": record.get("source_name"),
            "run_id": record.get("run_id"),
            "gap_causes": [
                item.get("cause")
                for item in record.get("gaps") or []
                if item.get("cause")
            ],
        }
        for name, unit, value in (
            (
                "telemetry.coverage.record.coverage_pct",
                "percent",
                record.get("coverage_pct"),
            ),
            (
                "telemetry.coverage.record.self_unattributed_ms",
                "ms",
                record.get("self_unattributed_ms"),
            ),
            (
                "telemetry.coverage.record.instrumentation_debt_ms",
                "ms",
                _record_instrumentation_debt_ms(record),
            ),
            (
                "telemetry.coverage.record.unknown_gap_count",
                "count",
                _record_unknown_gap_count(record),
            ),
        ):
            if value is None:
                continue
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                continue
            record_metric(
                conn,
                TelemetryMetric(
                    metric_name=name,
                    metric_kind="gauge",
                    metric_time=metric_time,
                    value_double=numeric if unit != "count" else None,
                    value_int=int(numeric) if unit == "count" else None,
                    unit=unit,
                    deployment_name=deployment,
                    branch_name=branch_name,
                    labels=record_labels,
                ),
            )
            count += 1
    payload["metrics_recorded"] = count
    return count


def _record_instrumentation_debt_ms(record: dict[str, Any]) -> float:
    return round(
        sum(
            float(item.get("duration_ms") or 0.0)
            for item in [
                *(record.get("gaps") or []),
                *(record.get("normalization") or []),
            ]
            if item.get("instrumentation_debt")
        ),
        3,
    )


def _record_unknown_gap_count(record: dict[str, Any]) -> int:
    return int(sum(
        1
        for item in record.get("gaps") or []
        if item.get("cause") == "unknown"
    ))


def _validate_scope_filter(
    *,
    scope: str,
    trace_id: str | None,
    workflow_id: str | None,
    generation_id: str | None,
    source_name: str | None,
    run_id: str | None,
    component: str | None = None,
) -> None:
    if scope == "trace" and not trace_id:
        raise ValueError("telemetry trace scope requires trace_id")
    if scope == "workflow" and not workflow_id:
        raise ValueError("telemetry workflow scope requires workflow_id")
    if scope == "generation" and not generation_id:
        raise ValueError("telemetry generation scope requires generation_id")
    if scope == "source_run" and not (source_name or run_id):
        raise ValueError("telemetry source_run scope requires source_name or run_id")
    if scope == "component" and not component:
        raise ValueError("telemetry component scope requires component")


def _window_spans(
    conn: psycopg.Connection,
    *,
    deployment: str,
    window_start: datetime,
    window_end: datetime,
    limit: int | None,
    scope: str,
    trace_id: str | None,
    workflow_id: str | None,
    generation_id: str | None,
    source_name: str | None,
    run_id: str | None,
    component: str | None = None,
) -> list[dict[str, Any]]:
    scope_conditions, scope_params = _span_scope_conditions(
        span_alias="s",
        trace_id=trace_id if scope == "trace" or trace_id else None,
        workflow_id=workflow_id,
        generation_id=generation_id,
        source_name=source_name,
        run_id=run_id,
    )
    component_conditions, component_params = _component_sql_condition(
        span_alias="s",
        component=component,
    )
    where_parts = [
        "s.deployment_name = %s",
        "s.started_at < %s",
        "COALESCE(s.ended_at, s.started_at) > %s",
        *scope_conditions,
        *component_conditions,
    ]
    params: list[Any] = [
        deployment,
        window_end,
        window_start,
        *scope_params,
        *component_params,
    ]
    limit_clause = "" if limit is None else "LIMIT %s"
    if limit is not None:
        params.append(max(1, int(limit)))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT trace_id, span_id, parent_span_id, span_name, span_kind,
                   status_code, started_at, ended_at, duration_ms,
                   generation_id, source_name, workflow_id, run_id,
                   tool_name, route_id, session_id, attributes,
                   payload_summary, unavailable_reason
              FROM okg.v_telemetry_span_tree s
             WHERE {' AND '.join(where_parts)}
             ORDER BY started_at, trace_id, span_name
             {limit_clause}
            """,
            tuple(params),
        )
        rows = cur.fetchall()
    selected_ids = {(str(row[0]), str(row[1])) for row in rows}
    spans: list[dict[str, Any]] = []
    for row in rows:
        parent_span_id = row[2]
        detached_parent_span_id = None
        if (
            parent_span_id is not None
            and (str(row[0]), str(parent_span_id)) not in selected_ids
        ):
            detached_parent_span_id = parent_span_id
            parent_span_id = None
        spans.append({
            "trace_id": row[0],
            "span_id": row[1],
            "parent_span_id": parent_span_id,
            **(
                {"detached_parent_span_id": detached_parent_span_id}
                if detached_parent_span_id is not None else {}
            ),
            "span_name": row[3],
            "span_kind": row[4],
            "status_code": row[5],
            "started_at": row[6],
            "ended_at": row[7],
            "duration_ms": _to_number(row[8]) or 0.0,
            "generation_id": row[9],
            "source_name": row[10],
            "workflow_id": row[11],
            "run_id": row[12],
            "tool_name": row[13],
            "route_id": row[14],
            "session_id": row[15],
            "attributes": row[16] or {},
            "payload_summary": row[17] or {},
            "unavailable_reason": row[18],
        })
    if component:
        spans = [
            span for span in spans
            if _span_component_matches(span, component)
        ]
        if limit is not None:
            spans = spans[:max(1, int(limit))]
    return _detach_missing_parent_links(spans)


def _window_span_count(
    conn: psycopg.Connection,
    *,
    deployment: str,
    window_start: datetime,
    window_end: datetime,
    scope: str,
    trace_id: str | None,
    workflow_id: str | None,
    generation_id: str | None,
    source_name: str | None,
    run_id: str | None,
    component: str | None = None,
) -> int:
    scope_conditions, scope_params = _span_scope_conditions(
        span_alias="s",
        trace_id=trace_id if scope == "trace" or trace_id else None,
        workflow_id=workflow_id,
        generation_id=generation_id,
        source_name=source_name,
        run_id=run_id,
    )
    component_conditions, component_params = _component_sql_condition(
        span_alias="s",
        component=component,
    )
    where_parts = [
        "s.deployment_name = %s",
        "s.started_at < %s",
        "COALESCE(s.ended_at, s.started_at) > %s",
        *scope_conditions,
        *component_conditions,
    ]
    params: list[Any] = [
        deployment,
        window_end,
        window_start,
        *scope_params,
        *component_params,
    ]
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT count(*)
              FROM okg.v_telemetry_span_tree s
             WHERE {' AND '.join(where_parts)}
            """,
            tuple(params),
        )
        row = cur.fetchone()
    return int(row[0] if row else 0)


def _detach_missing_parent_links(spans: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected_ids = {
        (str(span.get("trace_id")), str(span.get("span_id")))
        for span in spans
    }
    out: list[dict[str, Any]] = []
    for span in spans:
        parent_span_id = span.get("parent_span_id")
        if (
            parent_span_id is not None
            and (str(span.get("trace_id")), str(parent_span_id)) not in selected_ids
        ):
            span = {
                **span,
                "detached_parent_span_id": parent_span_id,
                "parent_span_id": None,
            }
        out.append(span)
    return out


def _coverage_records_for_spans(
    spans: list[dict[str, Any]],
    *,
    window_start: datetime,
    window_end: datetime,
) -> list[dict[str, Any]]:
    children_by_parent: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for span in spans:
        parent_span_id = span.get("parent_span_id")
        if parent_span_id is None:
            continue
        children_by_parent[
            (str(span.get("trace_id") or ""), str(parent_span_id))
        ].append(span)

    records: list[dict[str, Any]] = []
    for span in spans:
        span_start = span.get("started_at")
        if span_start is None:
            continue
        parent_start, parent_end = _clipped_interval(
            span_start,
            span.get("ended_at") or span_start,
            window_start,
            window_end,
        )
        parent_duration_ms = _interval_duration_ms(parent_start, parent_end)
        direct_children = children_by_parent.get(
            (str(span.get("trace_id") or ""), str(span.get("span_id") or "")),
            [],
        )
        child_intervals: list[tuple[datetime, datetime]] = []
        child_refs: list[dict[str, Any]] = []
        work_time_ms = 0.0
        for child in direct_children:
            child_start = child.get("started_at")
            if child_start is None:
                continue
            clipped_start, clipped_end = _clipped_interval(
                child_start,
                child.get("ended_at") or child_start,
                parent_start,
                parent_end,
            )
            duration_ms = _interval_duration_ms(clipped_start, clipped_end)
            if duration_ms <= 0:
                continue
            child_intervals.append((clipped_start, clipped_end))
            work_time_ms += duration_ms
            child_refs.append({
                "trace_id": child.get("trace_id"),
                "span_id": child.get("span_id"),
                "span_name": child.get("span_name"),
                "duration_ms": round(duration_ms, 3),
            })
        merged = _merge_intervals(child_intervals)
        direct_child_covered_ms = round(
            sum(_interval_duration_ms(start, end) for start, end in merged),
            3,
        )
        parent_duration_ms = round(parent_duration_ms, 3)
        work_time_ms = round(work_time_ms, 3)
        self_unattributed_ms = round(
            max(0.0, parent_duration_ms - direct_child_covered_ms),
            3,
        )
        coverage_pct = (
            round((direct_child_covered_ms / parent_duration_ms) * 100.0, 3)
            if parent_duration_ms > 0
            else 100.0
        )
        component = _span_component(span)
        gaps = _coverage_gaps(
            span=span,
            component=str(component),
            parent_start=parent_start,
            parent_end=parent_end,
            parent_duration_ms=parent_duration_ms,
            merged_child_intervals=merged,
            window_start=window_start,
        )
        parallel_overflow_ms = round(
            max(0.0, work_time_ms - direct_child_covered_ms),
            3,
        )
        normalization = _coverage_normalization_records(
            span=span,
            component=str(component),
            parent_duration_ms=parent_duration_ms,
            parallel_overflow_ms=parallel_overflow_ms,
        )
        records.append({
            "trace_id": span.get("trace_id"),
            "span_id": span.get("span_id"),
            "parent_span_id": span.get("parent_span_id"),
            **(
                {"detached_parent_span_id": span.get("detached_parent_span_id")}
                if span.get("detached_parent_span_id") is not None else {}
            ),
            "span_name": span.get("span_name"),
            "span_kind": span.get("span_kind"),
            "status_code": span.get("status_code"),
            "component": component,
            "started_at": parent_start.isoformat(),
            "ended_at": parent_end.isoformat(),
            "parent_duration_ms": parent_duration_ms,
            "direct_child_covered_ms": direct_child_covered_ms,
            "self_unattributed_ms": self_unattributed_ms,
            "coverage_pct": coverage_pct,
            "gap_count": len(gaps),
            "gaps": gaps,
            "work_time_ms": work_time_ms,
            "parallel_overflow_ms": parallel_overflow_ms,
            "normalization": normalization,
            "child_count": len(direct_children),
            "covered_child_count": len(child_refs),
            "children": child_refs,
            "workflow_id": span.get("workflow_id"),
            "generation_id": span.get("generation_id"),
            "source_name": span.get("source_name"),
            "run_id": span.get("run_id"),
            "tool_name": span.get("tool_name"),
            "route_id": span.get("route_id"),
            "session_id": span.get("session_id"),
            "unavailable_reason": span.get("unavailable_reason"),
        })
    return records


def _coverage_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    headline_records = [
        row for row in records
        if row.get("parent_span_id") is None
    ] or records
    relationship_parent_duration_ms = round(
        sum(float(row.get("parent_duration_ms") or 0.0) for row in records),
        3,
    )
    relationship_direct_child_covered_ms = round(
        sum(float(row.get("direct_child_covered_ms") or 0.0) for row in records),
        3,
    )
    parent_duration_ms = round(
        sum(float(row.get("parent_duration_ms") or 0.0) for row in headline_records),
        3,
    )
    direct_child_covered_ms = round(
        sum(float(row.get("direct_child_covered_ms") or 0.0) for row in headline_records),
        3,
    )
    self_unattributed_ms = round(
        sum(float(row.get("self_unattributed_ms") or 0.0) for row in headline_records),
        3,
    )
    work_time_ms = round(
        sum(float(row.get("work_time_ms") or 0.0) for row in headline_records),
        3,
    )
    cause_summary = _coverage_gap_cause_summary(records)
    unknown_gap_ms = round(
        sum(
            float(item.get("duration_ms") or 0.0)
            for item in cause_summary
            if item.get("cause") == "unknown"
        ),
        3,
    )
    return {
        "parent_duration_ms": parent_duration_ms,
        "direct_child_covered_ms": direct_child_covered_ms,
        "self_unattributed_ms": self_unattributed_ms,
        "coverage_pct": (
            round((direct_child_covered_ms / parent_duration_ms) * 100.0, 3)
            if parent_duration_ms > 0 else 100.0
        ),
        "work_time_ms": work_time_ms,
        "parallel_overflow_ms": round(
            sum(float(row.get("parallel_overflow_ms") or 0.0) for row in headline_records),
            3,
        ),
        "gap_count": int(sum(int(row.get("gap_count") or 0) for row in headline_records)),
        "unknown_gap_count": int(sum(
            1
            for row in headline_records
            for gap in row.get("gaps") or []
            if gap.get("cause") == "unknown"
        )),
        "unknown_gap_ms": unknown_gap_ms,
        "instrumentation_debt_ms": round(
            sum(
                float(item.get("duration_ms") or 0.0)
                for item in cause_summary
                if item.get("kind") == "instrumentation_debt"
            ),
            3,
        ),
        "top_gap_causes": cause_summary[:10],
        "relationship_parent_duration_ms": relationship_parent_duration_ms,
        "relationship_direct_child_covered_ms": relationship_direct_child_covered_ms,
        "relationship_coverage_pct": (
            round(
                (relationship_direct_child_covered_ms / relationship_parent_duration_ms) * 100.0,
                3,
            )
            if relationship_parent_duration_ms > 0 else 100.0
        ),
    }


def _coverage_budget(
    summary: dict[str, Any],
    *,
    unknown_gap_budget_ms: float,
    instrumentation_debt_budget_ms: float,
    unknown_gap_budget_count: int,
    budget_action: str,
) -> dict[str, Any]:
    action = str(budget_action or "warn").strip().lower()
    if action not in {"warn", "fail"}:
        raise ValueError("telemetry coverage budget_action must be warn or fail")
    thresholds = {
        "unknown_gap_ms": max(0.0, float(unknown_gap_budget_ms)),
        "instrumentation_debt_ms": max(0.0, float(instrumentation_debt_budget_ms)),
        "unknown_gap_count": max(0, int(unknown_gap_budget_count)),
    }
    values = {
        "unknown_gap_ms": float(summary.get("unknown_gap_ms") or 0.0),
        "instrumentation_debt_ms": float(summary.get("instrumentation_debt_ms") or 0.0),
        "unknown_gap_count": int(summary.get("unknown_gap_count") or 0),
    }
    violations: list[dict[str, Any]] = []
    for metric, value in values.items():
        budget = thresholds[metric]
        if value <= budget:
            continue
        violations.append({
            "metric": metric,
            "value": round(float(value), 3),
            "budget": budget,
            "action": action,
            "message": (
                f"{metric}={value} exceeds configured coverage budget {budget}"
            ),
        })
    return {
        "status": action if violations else "ok",
        "action": action,
        "thresholds": thresholds,
        "violations": violations,
    }


def _clipped_interval(
    started_at: datetime,
    ended_at: datetime,
    clip_start: datetime,
    clip_end: datetime,
) -> tuple[datetime, datetime]:
    start = max(_as_utc_datetime(started_at), clip_start)
    end = min(_as_utc_datetime(ended_at), clip_end)
    if end < start:
        end = start
    return start, end


def _interval_duration_ms(start: datetime, end: datetime) -> float:
    return max(0.0, (end - start).total_seconds() * 1000)


def _merge_intervals(
    intervals: list[tuple[datetime, datetime]],
) -> list[tuple[datetime, datetime]]:
    merged: list[tuple[datetime, datetime]] = []
    for start, end in sorted(intervals, key=lambda item: (item[0], item[1])):
        if end <= start:
            continue
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    return merged


def _coverage_gaps(
    *,
    span: dict[str, Any],
    component: str,
    parent_start: datetime,
    parent_end: datetime,
    parent_duration_ms: float,
    merged_child_intervals: list[tuple[datetime, datetime]],
    window_start: datetime,
) -> list[dict[str, Any]]:
    if parent_end <= parent_start:
        return []
    gaps: list[tuple[datetime, datetime, str]] = []
    cursor = parent_start
    if not merged_child_intervals:
        _append_coverage_gap(
            gaps,
            span=span,
            default_cause="leaf_operation",
            gap_start=parent_start,
            gap_end=parent_end,
            parent_start=parent_start,
        )
    else:
        for index, (child_start, child_end) in enumerate(merged_child_intervals):
            if child_start > cursor:
                default_cause = (
                    "before_first_child" if index == 0 else "between_children"
                )
                _append_coverage_gap(
                    gaps,
                    span=span,
                    default_cause=default_cause,
                    gap_start=cursor,
                    gap_end=child_start,
                    parent_start=parent_start,
                )
            cursor = max(cursor, child_end)
        if cursor < parent_end:
            _append_coverage_gap(
                gaps,
                span=span,
                default_cause="after_last_child",
                gap_start=cursor,
                gap_end=parent_end,
                parent_start=parent_start,
            )
    return [
        _coverage_gap_payload(
            span=span,
            component=component,
            started_at=start,
            ended_at=end,
            window_start=window_start,
            parent_duration_ms=parent_duration_ms,
            cause=cause,
        )
        for start, end, cause in gaps
        if end > start
    ]


def _append_coverage_gap(
    gaps: list[tuple[datetime, datetime, str]],
    *,
    span: dict[str, Any],
    default_cause: str,
    gap_start: datetime,
    gap_end: datetime,
    parent_start: datetime,
) -> None:
    if gap_end <= gap_start:
        return
    attrs = span.get("attributes") or {}
    if _explicit_gap_cause(attrs):
        gaps.append((
            gap_start,
            gap_end,
            _classify_coverage_gap(
                span=span,
                default_cause=default_cause,
                gap_start=gap_start,
                parent_start=parent_start,
            ),
        ))
        return

    queue_wait_interval = _dbos_queue_wait_interval(attrs)
    if queue_wait_interval is None:
        gaps.append((
            gap_start,
            gap_end,
            _classify_coverage_gap(
                span=span,
                default_cause=default_cause,
                gap_start=gap_start,
                parent_start=parent_start,
            ),
        ))
        return

    queue_start, queue_end = queue_wait_interval
    cursor = gap_start
    if cursor < min(gap_end, queue_start):
        normal_end = min(gap_end, queue_start)
        gaps.append((
            cursor,
            normal_end,
            _classify_coverage_gap(
                span=span,
                default_cause=default_cause,
                gap_start=cursor,
                parent_start=parent_start,
            ),
        ))
        cursor = normal_end

    overlap_start = max(gap_start, queue_start)
    overlap_end = min(gap_end, queue_end)
    if overlap_start < overlap_end:
        gaps.append((overlap_start, overlap_end, "queue_wait"))
        cursor = max(cursor, overlap_end)

    if cursor < gap_end:
        gaps.append((
            cursor,
            gap_end,
            _classify_coverage_gap(
                span=span,
                default_cause=default_cause,
                gap_start=cursor,
                parent_start=parent_start,
            ),
        ))


def _classify_coverage_gap(
    *,
    span: dict[str, Any],
    default_cause: str,
    gap_start: datetime,
    parent_start: datetime,
) -> str:
    attrs = span.get("attributes") or {}
    explicit = _explicit_gap_cause(attrs)
    if explicit:
        return explicit
    unavailable_reason = str(span.get("unavailable_reason") or "")
    if unavailable_reason:
        return "missing_child_span"
    span_name = str(span.get("span_name") or "").lower()
    if _truthy_attr(attrs, "okg.telemetry.profiler_only", "telemetry.profiler_only"):
        return "profiler_only"
    if _truthy_attr(attrs, "okg.telemetry.external_wait", "telemetry.external_wait"):
        return "external_wait"
    if (
        default_cause == "before_first_child"
        and gap_start == parent_start
        and _numeric_attr(attrs, "dbos.workflow.queue_wait_ms") > 0
        and _dbos_queue_wait_interval(attrs) is None
    ):
        return "queue_wait"
    if default_cause == "leaf_operation":
        if _span_expects_child_instrumentation(span_name, attrs):
            return "missing_child_span"
        if _span_is_external_wait(span_name, attrs):
            return "external_wait"
    return default_cause if default_cause in _GAP_CAUSE_KIND else "unknown"


def _explicit_gap_cause(attrs: dict[str, Any]) -> str | None:
    for key in (
        "okg.telemetry.gap_cause",
        "telemetry.gap.cause",
        "okg.gap_cause",
    ):
        raw = attrs.get(key)
        if raw is None:
            continue
        normalized = str(raw).strip().lower().replace("-", "_")
        if normalized in _GAP_CAUSE_KIND:
            return normalized
        return "unknown"
    return None


def _dbos_queue_wait_interval(
    attrs: dict[str, Any],
) -> tuple[datetime, datetime] | None:
    created_at = _epoch_ms_attr(attrs, "dbos.workflow.created_at_epoch_ms")
    started_at = _epoch_ms_attr(attrs, "dbos.workflow.started_at_epoch_ms")
    if created_at is None or started_at is None or started_at <= created_at:
        return None
    return created_at, started_at


def _epoch_ms_attr(attrs: dict[str, Any], key: str) -> datetime | None:
    value = attrs.get(key)
    try:
        epoch_ms = float(value)
    except (TypeError, ValueError):
        return None
    if epoch_ms <= 0:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)


def _span_expects_child_instrumentation(
    span_name: str,
    attrs: dict[str, Any],
) -> bool:
    if _truthy_attr(
        attrs,
        "okg.telemetry.expect_children",
        "telemetry.expect_children",
    ):
        return True
    return span_name in {
        "ingest.run",
        "publisher.cycle",
        "publisher.cycle.enrichers",
        "publisher.cycle.views",
        "publisher.cycle.native_views",
        "publisher.cycle.gates",
    } or span_name.startswith("dbos.workflow.")


def _span_is_external_wait(span_name: str, attrs: dict[str, Any]) -> bool:
    if attrs.get("http.url") or attrs.get("url.full") or attrs.get("server.address"):
        return True
    return any(token in span_name for token in ("http.", "external.", "tool.", "mcp."))


def _truthy_attr(attrs: dict[str, Any], *keys: str) -> bool:
    for key in keys:
        value = attrs.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def _numeric_attr(attrs: dict[str, Any], key: str) -> float:
    value = attrs.get(key)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _coverage_gap_payload(
    *,
    span: dict[str, Any],
    component: str,
    started_at: datetime,
    ended_at: datetime,
    window_start: datetime,
    parent_duration_ms: float,
    cause: str,
) -> dict[str, Any]:
    duration_ms = round(_interval_duration_ms(started_at, ended_at), 3)
    stable_cause = cause if cause in _GAP_CAUSE_KIND else "unknown"
    return {
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "start_ms": round((started_at - window_start).total_seconds() * 1000, 3),
        "duration_ms": duration_ms,
        "percent_of_parent": (
            round((duration_ms / parent_duration_ms) * 100.0, 3)
            if parent_duration_ms > 0 else 0.0
        ),
        "cause": stable_cause,
        "kind": _GAP_CAUSE_KIND.get(stable_cause, "instrumentation_debt"),
        "instrumentation_debt": stable_cause in _GAP_CAUSE_DEBT,
        "parent_span_id": span.get("span_id"),
        "parent_span_name": span.get("span_name"),
        "parent_component": component,
    }


def _coverage_normalization_records(
    *,
    span: dict[str, Any],
    component: str,
    parent_duration_ms: float,
    parallel_overflow_ms: float,
) -> list[dict[str, Any]]:
    if parallel_overflow_ms <= 0:
        return []
    return [{
        "duration_ms": parallel_overflow_ms,
        "percent_of_parent": (
            round((parallel_overflow_ms / parent_duration_ms) * 100.0, 3)
            if parent_duration_ms > 0 else 0.0
        ),
        "cause": "parallel_normalization",
        "kind": _GAP_CAUSE_KIND["parallel_normalization"],
        "instrumentation_debt": False,
        "parent_span_id": span.get("span_id"),
        "parent_span_name": span.get("span_name"),
        "parent_component": component,
    }]


def _coverage_gap_cause_summary(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    totals: dict[str, dict[str, Any]] = {}
    for record in records:
        for item in [*(record.get("gaps") or []), *(record.get("normalization") or [])]:
            cause = str(item.get("cause") or "unknown")
            row = totals.setdefault(
                cause,
                {
                    "cause": cause,
                    "kind": item.get("kind") or _GAP_CAUSE_KIND.get(cause, "instrumentation_debt"),
                    "duration_ms": 0.0,
                    "count": 0,
                    "instrumentation_debt": bool(item.get("instrumentation_debt")),
                },
            )
            row["duration_ms"] += float(item.get("duration_ms") or 0.0)
            row["count"] += 1
            row["instrumentation_debt"] = (
                bool(row.get("instrumentation_debt"))
                or bool(item.get("instrumentation_debt"))
            )
    return sorted(
        (
            {**row, "duration_ms": round(float(row["duration_ms"]), 3)}
            for row in totals.values()
        ),
        key=lambda row: (-float(row["duration_ms"]), str(row["cause"])),
    )


def _normalize_telemetry_scope(scope: str | None) -> str:
    normalized = str(scope or "system").strip().lower().replace("-", "_")
    if normalized in {"all", "window"}:
        normalized = "system"
    if normalized in {"selected_trace"}:
        normalized = "trace"
    if normalized in {"dbos", "dbos_workflow"}:
        normalized = "workflow"
    if normalized in {"source", "source_run", "run"}:
        normalized = "source_run"
    if normalized in {"component", "stage"}:
        normalized = "component"
    if normalized not in {
        "system",
        "trace",
        "workflow",
        "generation",
        "source_run",
        "component",
    }:
        raise ValueError(f"unsupported telemetry scope: {scope}")
    return normalized


def _scope_filter_payload(
    *,
    trace_id: str | None = None,
    workflow_id: str | None = None,
    generation_id: str | None = None,
    source_name: str | None = None,
    run_id: str | None = None,
    component: str | None = None,
) -> dict[str, str]:
    return {
        key: value
        for key, value in {
            "trace_id": trace_id,
            "workflow_id": workflow_id,
            "generation_id": generation_id,
            "source_name": source_name,
            "run_id": run_id,
            "component": component,
        }.items()
        if value
    }


def _span_scope_conditions(
    *,
    span_alias: str,
    trace_id: str | None = None,
    workflow_id: str | None = None,
    generation_id: str | None = None,
    source_name: str | None = None,
    run_id: str | None = None,
) -> tuple[list[str], list[Any]]:
    conditions: list[str] = []
    params: list[Any] = []
    for column, value in (
        ("trace_id", trace_id),
        ("generation_id", generation_id),
        ("source_name", source_name),
        ("run_id", run_id),
    ):
        if value:
            conditions.append(f"{span_alias}.{column} = %s")
            params.append(value)
    if workflow_id:
        conditions.append(
            "("
            f"{span_alias}.workflow_id = %s OR EXISTS ("
            "SELECT 1 FROM okg.v_telemetry_trace_summary workflow_trace "
            f"WHERE workflow_trace.deployment_name = {span_alias}.deployment_name "
            f"AND workflow_trace.trace_id = {span_alias}.trace_id "
            "AND workflow_trace.root_name = %s))"
        )
        params.extend([workflow_id, workflow_id])
    return conditions, params


def _component_sql_condition(
    *,
    span_alias: str,
    component: str | None,
) -> tuple[list[str], list[Any]]:
    expected = str(component or "").strip().lower()
    if not expected:
        return [], []
    normalized = _normalize_component_token(expected)
    attr_exprs = [
        f"{span_alias}.attributes->>'okg.component'",
        f"{span_alias}.attributes->>'component'",
        f"{span_alias}.attributes->>'okg.subcomponent'",
    ]
    conditions: list[str] = []
    params: list[Any] = []
    for expr in attr_exprs:
        conditions.append(f"lower(coalesce({expr}, '')) = %s")
        params.append(expected)
        conditions.append(
            "lower(replace(replace(replace(coalesce("
            f"{expr}, ''), '-', '_'), ' ', '_'), '/', '_')) = %s"
        )
        params.append(normalized)
    safe_like = expected.replace("%", "\\%").replace("_", "\\_")
    conditions.append(f"lower({span_alias}.span_name) LIKE %s ESCAPE '\\'")
    params.append(f"%{safe_like}%")
    return [f"({' OR '.join(conditions)})"], params


def _normalize_component_token(value: str) -> str:
    return "".join(
        char if char.isalnum() else "_"
        for char in str(value or "").strip().lower()
    ).strip("_")


def _span_component(span: dict[str, Any]) -> str:
    attrs = span.get("attributes") or {}
    return str(
        attrs.get("okg.subcomponent")
        or attrs.get("okg.component")
        or attrs.get("component")
        or _stage_category_for_frame(str(span.get("span_name") or ""))[0]
        or ""
    )


def _span_component_matches(span: dict[str, Any], component: str) -> bool:
    expected = str(component or "").strip().lower()
    if not expected:
        return True
    normalized_expected = _normalize_component_token(expected)
    attrs = span.get("attributes") or {}
    stage_component = _stage_category_for_frame(str(span.get("span_name") or ""))[0]
    candidates = [
        attrs.get("okg.component"),
        attrs.get("component"),
        attrs.get("okg.subcomponent"),
        stage_component,
        _span_component(span),
    ]
    for candidate in candidates:
        actual = str(candidate or "").strip().lower()
        if not actual:
            continue
        if actual == expected:
            return True
        normalized_actual = _normalize_component_token(actual)
        if normalized_actual == normalized_expected:
            return True
    return False


def _missing_performance_relations(conn: psycopg.Connection) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT relname
              FROM unnest(%s::text[]) AS rels(relname)
             WHERE to_regclass(relname) IS NULL
             ORDER BY relname
            """,
            (list(_PERFORMANCE_REQUIRED_RELATIONS),),
        )
        return [str(row[0]) for row in cur.fetchall()]


def _flamegraph_window(
    *,
    since_seconds: int,
    start_at: datetime | None,
    end_at: datetime | None,
) -> tuple[datetime, datetime]:
    end = _as_utc_datetime(end_at) if end_at is not None else datetime.now(timezone.utc)
    start = (
        _as_utc_datetime(start_at)
        if start_at is not None
        else end - timedelta(seconds=max(1, int(since_seconds)))
    )
    if end <= start:
        raise ValueError("telemetry flamegraph end_at must be after start_at")
    return start, end


def _as_utc_datetime(value: Any) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _stage_category_for_frame(name: str) -> tuple[str, str]:
    normalized = name.lower()
    if name == "unattributed workload time":
        return (
            "Other / gap",
            "Trace lifetime without a more specific recorded child span",
        )
    if (
        "chronos" in normalized
        or "db." in normalized
        or " sql" in normalized
        or ".sql" in normalized
        or "transaction" in normalized
        or "dedupe_persisted" in normalized
        or "delta_counts" in normalized
        or "dirty_scope" in normalized
        or "table_lookup" in normalized
        or normalized.endswith("_query")
        or ".query" in normalized
    ):
        return (
            "Chronos / SQL",
            "Chronos graph-store and database query work",
        )
    if "checkpoint_generation" in normalized or "checkpoint" in normalized:
        return ("Checkpoint", "Generation checkpoint work")
    if (
        ".enrichers" in normalized
        or ".enrich." in normalized
        or normalized.endswith(".enrich")
        or "enricher" in normalized
    ):
        return ("Enrichers", "Derived graph enrichment work")
    if (
        normalized
        in {
            "views",
            "native views",
            "effective attributes",
            "other view time",
        }
        or "native_views" in normalized
        or "attribute_effective_view" in normalized
        or "effective_attributes" in normalized
        or "view_derived" in normalized
        or "view_cite" in normalized
    ):
        return ("Views", "Native view and read-side materialization work")
    if (
        normalized == "gates"
        or "consistency_gates" in normalized
        or normalized.endswith(".gates")
        or ".gates." in normalized
        or "gate_" in normalized
    ):
        return ("Gates", "Publish gate evaluation work")
    if normalized.startswith("source."):
        return ("Sources", "Source adapter and source-run work")
    if normalized.startswith(("mcp.", "tool.", "tools.")):
        return ("Tools", "Tool and MCP request work")
    if normalized.startswith("publisher.") or normalized == "publisher overhead":
        return ("Publisher", "Publisher orchestration work")
    if "unattributed" in normalized or "gap" in normalized:
        return (
            "Other / gap",
            "Trace lifetime without a more specific recorded child span",
        )
    return ("Other / gap", "Telemetry frames outside the standard stage groups")


def trace_show(
    conn: psycopg.Connection,
    *,
    deployment: str,
    trace_id: str,
    include_profile: bool = False,
) -> dict[str, Any]:
    """One trace, its spans and its artifacts.

    A read. ``include_profile`` adds the profile report if one has been built;
    it does NOT build one, because building writes files and artifact rows and
    this command is not authorized to write.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT trace_id, root_name, root_kind, status_code,
                   started_at, ended_at, duration_ms, generation_id,
                   source_name, workflow_id, session_id, span_count,
                   slowest_span_ms, slowest_span_name, artifact_count
              FROM okg.v_telemetry_trace_summary
             WHERE deployment_name = %s
               AND trace_id = %s
            """,
            (deployment, trace_id),
        )
        summary = cur.fetchone()
        if summary is None:
            raise ValueError(f"trace not found for deployment={deployment}: {trace_id}")
        cur.execute(
            """
            SELECT span_id, parent_span_id, span_name, span_kind,
                   status_code, started_at, ended_at, duration_ms,
                   generation_id, source_name, workflow_id, run_id,
                   tool_name, route_id, session_id, attributes,
                   payload_summary, unavailable_reason
              FROM okg.v_telemetry_span_tree
             WHERE deployment_name = %s
               AND trace_id = %s
             ORDER BY started_at, span_name
            """,
            (deployment, trace_id),
        )
        span_rows = cur.fetchall()
        cur.execute(
            """
            SELECT artifact_id, artifact_kind, path, format,
                   collector, collector_status, unavailable_reason, summary
              FROM okg.telemetry_artifacts
             WHERE deployment_name = %s
               AND trace_id = %s
             ORDER BY created_at DESC
            """,
            (deployment, trace_id),
        )
        artifact_rows = cur.fetchall()
    spans = [
        {
            "span_id": row[0],
            "parent_span_id": row[1],
            "span_name": row[2],
            "span_kind": row[3],
            "status_code": row[4],
            "started_at": row[5].isoformat() if row[5] else None,
            "ended_at": row[6].isoformat() if row[6] else None,
            "duration_ms": row[7],
            "generation_id": row[8],
            "source_name": row[9],
            "workflow_id": row[10],
            "run_id": row[11],
            "tool_name": row[12],
            "route_id": row[13],
            "session_id": row[14],
            "attributes": row[15] or {},
            "payload_summary": row[16] or {},
            "unavailable_reason": row[17],
        }
        for row in span_rows
    ]
    payload = {
        "schema": "okg.telemetry.trace_show.v1",
        "deployment": deployment,
        "trace": {
            "trace_id": summary[0],
            "root_name": summary[1],
            "root_kind": summary[2],
            "status_code": summary[3],
            "started_at": summary[4].isoformat() if summary[4] else None,
            "ended_at": summary[5].isoformat() if summary[5] else None,
            "duration_ms": summary[6],
            "generation_id": summary[7],
            "source_name": summary[8],
            "workflow_id": summary[9],
            "session_id": summary[10],
            "span_count": int(summary[11] or 0),
            "slowest_span_ms": summary[12],
            "slowest_span_name": summary[13],
            "artifact_count": int(summary[14] or 0),
        },
        "spans": spans,
        "artifacts": [
            {
                "artifact_id": row[0],
                "artifact_kind": row[1],
                "path": row[2],
                "format": row[3],
                "collector": row[4],
                "collector_status": row[5],
                "unavailable_reason": row[6],
                "summary": row[7] or {},
            }
            for row in artifact_rows
        ],
        "unavailable_segments": [
            {
                "span_id": span["span_id"],
                "span_name": span["span_name"],
                "reason": span["unavailable_reason"],
            }
            for span in spans
            if span.get("unavailable_reason")
        ],
    }
    if include_profile:
        payload["profile_report"] = profile_report(
            conn,
            deployment=deployment,
            trace_id=trace_id,
        )
    return payload


def operator_report(
    conn: psycopg.Connection,
    *,
    deployment: str,
    trace_id: str,
) -> dict[str, Any]:
    # A report reports. It includes the profile if one has been built and says
    # so when none has; it does not build one, for the same reason `trace show`
    # does not. `build_profile_bundle` remains available to the command whose
    # job is building.
    trace = trace_show(
        conn,
        deployment=deployment,
        trace_id=trace_id,
        include_profile=True,
    )
    metrics = telemetry_metrics_catalog(conn, deployment=deployment)
    slowest = trace["trace"]["slowest_span_name"]
    return {
        "schema": "okg.telemetry.operator_report.v1",
        "deployment": deployment,
        "trace_id": trace_id,
        "summary": trace["trace"],
        "slowest_stage": {
            "span_name": slowest,
            "duration_ms": trace["trace"]["slowest_span_ms"],
        },
        "profile": trace.get("profile_report"),
        "metrics": {
            "metric_count": metrics["metric_count"],
            "unavailable_count": metrics["unavailable_count"],
            "selected": [
                metric for metric in metrics["metrics"]
                if metric["name"] in {
                    "telemetry.overhead.latest_write_ms",
                    "mcp.calls.count",
                    "source.runs.count",
                    "llm.calls.count",
                    "queue.dlq.count",
                    "query.latency.avg_ms",
                }
            ],
        },
        "unavailable_segments": trace["unavailable_segments"],
        "artifact_count": len(trace["artifacts"]),
        "artifacts": trace["artifacts"],
    }


def _to_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
