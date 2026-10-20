from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import shlex
import uuid
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.profiling.artifacts import profile_root, write_json_atomic


def build_profile_bundle(
    conn: psycopg.Connection,
    *,
    deployment: str,
    trace_id: str,
    deep: bool = False,
) -> dict[str, Any]:
    spans = _trace_spans(conn, deployment=deployment, trace_id=trace_id)
    if not spans:
        raise ValueError(f"trace not found for deployment={deployment!r}: {trace_id}")
    timeline = _timeline(spans)
    speedscope = _speedscope(trace_id, timeline)
    critical_path = _critical_path(timeline)
    query_timings = _query_timings(timeline)
    overhead = _overhead_summary(conn, deployment=deployment, trace_id=trace_id)
    collectors = _collector_matrix(
        has_spans=bool(spans),
        has_queries=bool(query_timings),
        deep=deep,
    )
    bundle = {
        "schema": "okg.telemetry.profile_bundle.v1",
        "deployment": deployment,
        "trace_id": trace_id,
        "deep": deep,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "span_count": len(spans),
        "timeline": timeline,
        "critical_path": critical_path,
        "query_timings": query_timings,
        "collector_availability": collectors,
        "overhead": overhead,
    }
    root = profile_root() / deployment / f"telemetry-{trace_id}"
    root.mkdir(parents=True, exist_ok=True)
    write_json_atomic(root / "bundle.json", bundle)
    write_json_atomic(root / "timeline.json", {"trace_id": trace_id, "spans": timeline})
    write_json_atomic(root / "speedscope.json", speedscope)
    write_json_atomic(root / "report.json", _report_payload(bundle, root))
    artifacts = [
        _record_artifact(
            conn,
            deployment=deployment,
            trace_id=trace_id,
            artifact_kind="profile-bundle",
            path=root / "bundle.json",
            fmt="json",
            collector="telemetry-spans",
            collector_status="available",
            summary={"span_count": len(spans)},
        ),
        _record_artifact(
            conn,
            deployment=deployment,
            trace_id=trace_id,
            artifact_kind="speedscope",
            path=root / "speedscope.json",
            fmt="speedscope-json",
            collector="telemetry-spans",
            collector_status="available",
            summary={
                "profile_type": "evented",
                "sample_source": "telemetry_spans",
                "python_samples": False,
            },
        ),
        _record_artifact(
            conn,
            deployment=deployment,
            trace_id=trace_id,
            artifact_kind="profile-report",
            path=root / "report.json",
            fmt="json",
            collector="telemetry-profile",
            collector_status="available",
            summary={"critical_path": critical_path},
        ),
    ]
    bundle["artifact_dir"] = str(root)
    bundle["artifacts"] = artifacts
    write_json_atomic(root / "bundle.json", bundle)
    write_json_atomic(root / "report.json", _report_payload(bundle, root))
    return bundle


def profile_report(
    conn: psycopg.Connection,
    *,
    deployment: str,
    trace_id: str,
) -> dict[str, Any]:
    """Read the profile report for a trace without building one."""
    artifacts = _artifact_rows(conn, deployment=deployment, trace_id=trace_id)
    bundle_path = next(
        (Path(row["path"]) for row in artifacts if row["artifact_kind"] == "profile-bundle"),
        None,
    )
    if bundle_path is None or not bundle_path.is_file():
        return _unbuilt_report_payload(
            deployment=deployment,
            trace_id=trace_id,
            artifacts=artifacts,
            recorded=bundle_path is not None,
        )
    else:
        bundle = json.loads(bundle_path.read_text())
        if "artifacts" not in bundle:
            bundle["artifacts"] = artifacts
    return _report_payload(
        bundle,
        Path(bundle.get("artifact_dir") or bundle_path.parent),
    )


def _unbuilt_report_payload(
    *,
    deployment: str,
    trace_id: str,
    artifacts: list[dict[str, Any]],
    recorded: bool,
) -> dict[str, Any]:
    """A profile report that does not exist yet, said as data.

    ``recorded`` separates the two ways it can be absent: no artifact row names
    a bundle at all, or a row names one whose file is gone. An operator chasing
    a missing profile needs to know which.
    """
    return {
        "schema": "okg.telemetry.profile_report.v1",
        "deployment": deployment,
        "trace_id": trace_id,
        "available": False,
        "unavailable_reason": (
            "profile_bundle_file_missing" if recorded else "profile_bundle_not_built"
        ),
        "next_check": shlex.join([
            "okg", "telemetry", "profile",
            "--deployment", deployment,
            "--trace-id", trace_id,
            "--apply",
        ]),
        "artifact_dir": None,
        "span_count": None,
        "critical_path": [],
        "collector_availability": [],
        "overhead": None,
        "artifacts": artifacts,
    }


def _trace_spans(
    conn: psycopg.Connection,
    *,
    deployment: str,
    trace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT span_id, parent_span_id, span_name, span_kind,
                   status_code, started_at, ended_at, duration_ms,
                   generation_id, source_name, workflow_id, run_id,
                   tool_name, route_id, session_id, attributes,
                   payload_summary, unavailable_reason
              FROM okg.telemetry_spans
             WHERE deployment_name = %s
               AND trace_id = %s
             ORDER BY started_at, event_time, span_name
            """,
            (deployment, trace_id),
        )
        rows = cur.fetchall()
    spans: list[dict[str, Any]] = []
    for row in rows:
        started_at = row[5]
        ended_at = row[6] or started_at
        duration_ms = row[7]
        if duration_ms is None:
            duration_ms = max(0, int((ended_at - started_at).total_seconds() * 1000))
        spans.append({
            "span_id": row[0],
            "parent_span_id": row[1],
            "span_name": row[2],
            "span_kind": row[3],
            "status_code": row[4],
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_ms": int(duration_ms),
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
        })
    return spans


def _timeline(spans: list[dict[str, Any]]) -> list[dict[str, Any]]:
    first = min(span["started_at"] for span in spans)
    out: list[dict[str, Any]] = []
    for span in spans:
        start_ms = (span["started_at"] - first).total_seconds() * 1000
        out.append({
            **{
                key: value for key, value in span.items()
                if key not in {"started_at", "ended_at"}
            },
            "started_at": span["started_at"].isoformat(),
            "ended_at": span["ended_at"].isoformat(),
            "start_ms": round(start_ms, 3),
            "end_ms": round(start_ms + span["duration_ms"], 3),
        })
    return out


def _speedscope(trace_id: str, timeline: list[dict[str, Any]]) -> dict[str, Any]:
    frames = [{"name": span["span_name"]} for span in timeline]
    events: list[dict[str, Any]] = []
    end_value = 0.0
    for idx, span in enumerate(timeline):
        start = float(span["start_ms"])
        end = max(start + 0.001, float(span["end_ms"]))
        end_value = max(end_value, end)
        events.append({"type": "O", "frame": idx, "at": start})
        events.append({"type": "C", "frame": idx, "at": end})
    return {
        "$schema": "https://www.speedscope.app/file-format-schema.json",
        "shared": {"frames": frames},
        "profiles": [{
            "type": "evented",
            "name": f"okg trace {trace_id}",
            "unit": "milliseconds",
            "startValue": 0,
            "endValue": end_value,
            "events": sorted(events, key=lambda item: (item["at"], item["type"])),
        }],
    }


def _critical_path(timeline: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not timeline:
        return None
    span = max(timeline, key=lambda item: item.get("duration_ms") or 0)
    return {
        "span_id": span["span_id"],
        "span_name": span["span_name"],
        "duration_ms": span["duration_ms"],
        "status_code": span["status_code"],
    }


def _query_timings(timeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for span in timeline:
        payload = span.get("payload_summary") or {}
        if payload.get("query_plan_summary"):
            out.append({
                "span_id": span["span_id"],
                "span_name": span["span_name"],
                "duration_ms": span["duration_ms"],
                "query_plan_summary": payload["query_plan_summary"],
            })
    return out


def _overhead_summary(
    conn: psycopg.Connection,
    *,
    deployment: str,
    trace_id: str,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT metric_name, value_double, value_int, unit, labels
              FROM okg.telemetry_metrics
             WHERE deployment_name = %s
               AND trace_id = %s
             ORDER BY metric_time, metric_name
            """,
            (deployment, trace_id),
        )
        rows = cur.fetchall()
    return {
        "metric_count": len(rows),
        "metrics": [
            {
                "metric_name": row[0],
                "value": row[1] if row[1] is not None else row[2],
                "unit": row[3],
                "labels": row[4] or {},
            }
            for row in rows
        ],
    }


def _collector_matrix(
    *,
    has_spans: bool,
    has_queries: bool,
    deep: bool,
) -> list[dict[str, Any]]:
    return [
        {
            "collector": "telemetry-spans",
            "status": "available" if has_spans else "unavailable",
            "reason": None if has_spans else "trace_has_no_spans",
        },
        {
            "collector": "speedscope-span-timeline",
            "status": "available" if has_spans else "unavailable",
            "reason": (
                "generated_from_telemetry_spans"
                if has_spans else "trace_has_no_spans"
            ),
        },
        {
            "collector": "python-sampler",
            "status": "unavailable",
            "reason": "python_sampler_not_configured",
        },
        {
            "collector": "query-timing",
            "status": "available" if has_queries else "unavailable",
            "reason": None if has_queries else "query_timing_collector_not_configured",
        },
        {
            "collector": "postgres-flamegraph",
            "status": "unavailable",
            "reason": "postgres_flamegraph_collector_not_configured",
        },
        {
            "collector": "system-flamegraph",
            "status": "unavailable",
            "reason": "system_flamegraph_collector_not_configured",
        },
        {
            "collector": "deep-profile-requested",
            "status": "unavailable",
            "reason": (
                "deep_profile_request_recorded_without_sampler"
                if deep else "deep_profile_not_requested"
            ),
        },
    ]


def _record_artifact(
    conn: psycopg.Connection,
    *,
    deployment: str,
    trace_id: str,
    artifact_kind: str,
    path: Path,
    fmt: str,
    collector: str,
    collector_status: str,
    summary: dict[str, Any],
) -> dict[str, Any]:
    artifact_id = f"telemetry:{trace_id}:{artifact_kind}:{uuid.uuid4().hex[:12]}"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.telemetry_artifacts (
              artifact_id, deployment_name, trace_id, artifact_kind, path, format,
              collector, collector_status, summary
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                artifact_id,
                deployment,
                trace_id,
                artifact_kind,
                str(path),
                fmt,
                collector,
                collector_status,
                Jsonb(summary),
            ),
        )
    return {
        "artifact_id": artifact_id,
        "artifact_kind": artifact_kind,
        "path": str(path),
        "format": fmt,
        "collector": collector,
        "collector_status": collector_status,
    }


def _artifact_rows(
    conn: psycopg.Connection,
    *,
    deployment: str,
    trace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
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
        rows = cur.fetchall()
    return [
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
        for row in rows
    ]


def _report_payload(bundle: dict[str, Any], root: Path) -> dict[str, Any]:
    return {
        "schema": "okg.telemetry.profile_report.v1",
        "deployment": bundle["deployment"],
        "trace_id": bundle["trace_id"],
        "available": True,
        "unavailable_reason": None,
        "artifact_dir": str(root),
        "span_count": bundle["span_count"],
        "critical_path": bundle["critical_path"],
        "collector_availability": bundle["collector_availability"],
        "overhead": bundle["overhead"],
        "artifacts": bundle.get("artifacts", []),
    }
