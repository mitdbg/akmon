"""Bounded, read-only producer time series for the Health workspace."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
import math
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping

from okg.substrate.console_views.enricher_diagnostics import (
    _edge_fact_history_for_name,
    _run_from_metadata,
    _run_name,
    compose_enricher_diagnostics,
)
from okg.substrate.console_views.source_diagnostics import _latest_runs_by_source
from okg.substrate.console_views.sources import compose_sources
from okg.substrate.console_views.view_diagnostics import compose_view_diagnostics
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.mcp.session import Session
from okg.substrate.views.registry import load_views_registry


logger = logging.getLogger(__name__)

_METRICS = ("duration", "output", "throughput", "failures", "observation_lag")

# The enricher-run read (issue #2113). It used to call
# `store.list_generations`, which moved every checkpoint's full metadata to
# find the runs inside a chart's span. It now reads a bounded window of the
# newest checkpoints at or after the span's start, projecting only these keys:
# the two places a publisher records enricher runs, plus the two
# `_run_from_metadata` prints (it lives in `enricher_diagnostics`, so the
# projection guard, which stays inside one module, cannot see them).
ENRICHER_RUN_METADATA_KEYS: tuple[str, ...] = (
    "status",
    "target_generation_id",
    "enricher_runs",
    "timing_details",
)
ENRICHER_RUN_SCAN_WINDOW = 200
# `created_at` is text ordered lexically, so the scan's lower bound carries
# the same two days of margin the attempts chart uses: a stamp written in
# another UTC offset still lands inside the scan, and the exact in-span test
# in `_collect_enricher_runs` is still the one that decides.
_ENRICHER_SCAN_MARGIN = timedelta(days=2)
_PERIODS = {"1m": 60, "5m": 300, "1h": 3600, "1d": 86400}
_UNITS = {
    "duration": "time",
    "output": "count",
    "throughput": "rate",
    "failures": "count",
    "observation_lag": "time",
}
_LABELS = {
    "duration": "Run duration",
    "output": "Output",
    "throughput": "Throughput",
    "failures": "Failures",
    "observation_lag": "Observation lag",
}

_SELECTED_SOURCE_RUNS_SQL = """
WITH ranked AS (
  SELECT
    registry_source_name,
    run_id,
    status,
    observed_at,
    started_at,
    finished_at,
    nodes_appended,
    edges_appended,
    total_ms,
    row_number() OVER (
      PARTITION BY registry_source_name
      ORDER BY COALESCE(observed_at, finished_at, started_at) DESC NULLS LAST, run_id DESC
    ) AS rn
  FROM okg.v_source_run_diagnostics
  WHERE deployment = %(deployment)s
    AND registry_source_name = ANY(%(source_names)s)
    AND COALESCE(observed_at, finished_at, started_at) >= %(start)s
    AND COALESCE(observed_at, finished_at, started_at) <= %(end)s
)
SELECT registry_source_name, run_id, status, observed_at,
       started_at, finished_at, nodes_appended, edges_appended, total_ms
FROM ranked
WHERE rn <= %(limit)s
ORDER BY registry_source_name, COALESCE(observed_at, finished_at, started_at)
"""

_SELECTED_VIEW_RUNS_SQL = """
WITH ranked AS (
  SELECT
    d.definition_id,
    r.run_id,
    r.status,
    r.metrics,
    r.started_at,
    r.completed_at,
    row_number() OVER (
      PARTITION BY d.definition_id
      ORDER BY r.completed_at DESC NULLS LAST, r.started_at DESC
    ) AS rn
  FROM okg.derived_artifact_definitions d
  JOIN okg.derived_artifact_runs r ON r.definition_id = d.definition_id
  WHERE d.deployment_name = %(deployment)s
    AND d.artifact_kind = 'view'
    AND d.definition_id = ANY(%(definition_ids)s)
    AND COALESCE(r.completed_at, r.started_at) >= %(start)s
    AND COALESCE(r.completed_at, r.started_at) <= %(end)s
)
SELECT
  ranked.definition_id,
  ranked.run_id,
  ranked.status,
  ranked.metrics,
  ranked.started_at,
  ranked.completed_at,
  (
    SELECT sum(s.row_count)
    FROM okg.derived_output_manifests m
    JOIN okg.derived_output_slots s ON s.manifest_id = m.manifest_id
    WHERE m.run_id = ranked.run_id
  ) AS row_count
FROM ranked
WHERE ranked.rn <= %(limit)s
ORDER BY ranked.definition_id, COALESCE(ranked.completed_at, ranked.started_at)
"""


def _datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    return None


def _rows_as_dicts(cur: Any) -> list[dict[str, Any]]:
    columns = [description.name for description in cur.description]
    return [dict(zip(columns, raw)) for raw in cur.fetchall()]


def _selected_source_runs(
    conn: Any,
    *,
    deployment_name: str,
    names: list[str],
    start: datetime,
    end: datetime,
    limit: int,
) -> dict[str, list[dict[str, Any]]]:
    if not names:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            _SELECTED_SOURCE_RUNS_SQL,
            {
                "deployment": deployment_name,
                "source_names": names,
                "start": start,
                "end": end,
                "limit": limit,
            },
        )
        rows = _rows_as_dicts(cur)
    runs = {name: [] for name in names}
    for row in rows:
        runs.setdefault(str(row.get("registry_source_name") or ""), []).append(row)
    return runs


def _selected_enricher_runs(
    conn: Any,
    *,
    deployment_name: str,
    session: Session,
    names: list[str],
    start: datetime,
    end: datetime,
    limit: int,
) -> dict[str, list[dict[str, Any]]]:
    if not names:
        return {}
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    scan_start = start if start.tzinfo else start.replace(tzinfo=timezone.utc)
    recent = store.recent_generations(
        deployment_name=deployment_name,
        branch=session.branch_name,
        status=None,
        keys=ENRICHER_RUN_METADATA_KEYS,
        scan_window=ENRICHER_RUN_SCAN_WINDOW,
        since=(
            scan_start.astimezone(timezone.utc) - _ENRICHER_SCAN_MARGIN
        ).isoformat(),
    )
    runs, satisfied = _collect_enricher_runs(
        recent.rows, names=names, start=start, end=end, limit=limit,
    )
    if not satisfied and recent.window_exhausted:
        # The window filled before every requested enricher had `limit` runs,
        # so older in-span checkpoints exist that it never saw. Reading them
        # is the only way to serve what the unbounded read served.
        logger.warning(
            "producer_metrics_enricher_runs_outside_window: the %d newest "
            "checkpoints since %s on branch %r did not hold %d runs for every "
            "requested enricher; reading the whole history",
            recent.scanned, scan_start.isoformat(), session.branch_name, limit,
        )
        generations = store.list_generations(
            deployment_name=deployment_name,
            branch=session.branch_name,
            status=None,
        )
        runs, _ = _collect_enricher_runs(
            reversed(generations), names=names, start=start, end=end, limit=limit,
        )
    for name in names:
        if runs[name]:
            runs[name].reverse()
            continue
        fallback = _edge_fact_history_for_name(conn, name=name, limit=limit)
        runs[name] = [
            row for row in reversed(fallback)
            if (observed := _datetime(row.get("observed_at"))) is not None
            and start <= observed <= end
        ]
    return runs


def _collect_enricher_runs(
    newest_first: Iterable[Any],
    *,
    names: list[str],
    start: datetime,
    end: datetime,
    limit: int,
) -> tuple[dict[str, list[dict[str, Any]]], bool]:
    """Each name's newest in-span runs, newest first, and whether all filled.

    The second value is True when every name reached `limit` -- the only case
    in which stopping early proves no older run was missed.
    """
    selected = set(names)
    runs: dict[str, list[dict[str, Any]]] = {name: [] for name in names}
    for checkpoint in newest_first:
        observed = _datetime(getattr(checkpoint, "created_at", None))
        if observed is None or observed < start or observed > end:
            continue
        metadata = getattr(checkpoint, "metadata", None) or {}
        raw_runs = (
            metadata.get("enricher_runs")
            or (metadata.get("timing_details") or {}).get("enrichers")
            or []
        )
        if not isinstance(raw_runs, list):
            continue
        for raw in raw_runs:
            if not isinstance(raw, Mapping):
                continue
            name = _run_name(raw)
            if name in selected and len(runs[name]) < limit:
                runs[name].append(_run_from_metadata(checkpoint, raw))
        if all(len(runs[name]) >= limit for name in names):
            return runs, True
    return runs, False


def _view_elapsed_ms(row: Mapping[str, Any]) -> float | None:
    metrics = row.get("metrics")
    if isinstance(metrics, Mapping):
        for key in ("elapsed_ms", "scheduler_elapsed_ms", "total_ms"):
            value = _number(metrics.get(key))
            if value is not None:
                return value
    started = _datetime(row.get("started_at"))
    completed = _datetime(row.get("completed_at"))
    if started is not None and completed is not None:
        return max(0.0, (completed - started).total_seconds() * 1000)
    return None


def _selected_view_runs(
    conn: Any,
    *,
    deployment_name: str,
    definition_ids: list[str],
    start: datetime,
    end: datetime,
    limit: int,
) -> dict[str, list[dict[str, Any]]]:
    if not definition_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            _SELECTED_VIEW_RUNS_SQL,
            {
                "deployment": deployment_name,
                "definition_ids": definition_ids,
                "start": start,
                "end": end,
                "limit": limit,
            },
        )
        rows = _rows_as_dicts(cur)
    runs = {definition_id: [] for definition_id in definition_ids}
    for row in rows:
        definition_id = str(row.get("definition_id") or "")
        runs.setdefault(definition_id, []).append({
            "run_id": str(row.get("run_id") or ""),
            "status": row.get("status"),
            "observed_at": row.get("completed_at") or row.get("started_at"),
            "row_count": (
                int(row["row_count"])
                if row.get("row_count") is not None
                else None
            ),
            "total_ms": _view_elapsed_ms(row),
        })
    return runs


def _source_observation(run: Mapping[str, Any]) -> dict[str, Any] | None:
    observed = _datetime(run.get("observed_at") or run.get("finished_at") or run.get("started_at"))
    if observed is None:
        return None
    nodes_appended = _number(run.get("nodes_appended"))
    edges_appended = _number(run.get("edges_appended"))
    output = (
        nodes_appended + edges_appended
        if nodes_appended is not None and edges_appended is not None
        else None
    )
    duration = _number(run.get("total_ms"))
    status = str(run.get("status") or "").strip().lower()
    return {
        "observed_at": observed,
        "run_id": str(run.get("run_id") or ""),
        "duration": duration,
        "output": output,
        "throughput": output / (duration / 1000) if output is not None and duration and duration > 0 else None,
        "failures": None if not status else 1.0 if status in {"failed", "error", "failed_run"} else 0.0,
    }


def _derived_observation(run: Mapping[str, Any], *, output_key: str) -> dict[str, Any] | None:
    observed = _datetime(run.get("observed_at"))
    if observed is None:
        return None
    output = _number(run.get(output_key))
    duration = _number(run.get("total_ms"))
    status = str(run.get("status") or "").strip().lower()
    return {
        "observed_at": observed,
        "run_id": str(run.get("run_id") or run.get("generation_id") or ""),
        "duration": duration,
        "output": output,
        "throughput": output / (duration / 1000) if output is not None and duration and duration > 0 else None,
        "failures": None if not status else 1.0 if status in {"failed", "blocked", "error"} else 0.0,
    }


def _resolve_period(start: datetime, end: datetime, requested: str, max_buckets: int) -> tuple[str, int, str | None]:
    span_seconds = max(1.0, (end - start).total_seconds())
    minimum = max(1, math.ceil(span_seconds / max_buckets))
    candidates = [(label, seconds) for label, seconds in _PERIODS.items() if seconds >= minimum]
    resolved_label, resolved_seconds = candidates[0] if candidates else ("1d", 86400)
    if requested == "auto":
        return resolved_label, resolved_seconds, None
    requested_seconds = _PERIODS.get(requested)
    if requested_seconds is None:
        return resolved_label, resolved_seconds, "invalid_period_resolved"
    if requested_seconds < minimum:
        return resolved_label, resolved_seconds, f"{requested} exceeds the {max_buckets}-bucket limit; using {resolved_label}."
    return requested, requested_seconds, None


def _aggregate(values: list[float], statistic: str) -> float:
    if statistic == "count":
        return float(len(values))
    if statistic == "sum":
        return float(sum(values))
    if statistic == "max":
        return float(max(values))
    ordered = sorted(values)
    if statistic == "p50":
        return float(median(ordered))
    position = (len(ordered) - 1) * 0.95
    lower = math.floor(position)
    upper = min(lower + 1, len(ordered) - 1)
    return float(ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower))


def _series(
    producer: Mapping[str, Any],
    observations: Iterable[Mapping[str, Any]],
    *,
    metric: str,
    start: datetime,
    end: datetime,
    period_seconds: int,
    statistic: str,
    max_buckets: int,
) -> dict[str, Any]:
    bucket_count = min(max_buckets, max(1, math.ceil((end - start).total_seconds() / period_seconds)))
    buckets: list[list[tuple[float, str]]] = [[] for _ in range(bucket_count)]
    observed_any = False
    for observation in observations:
        observed = observation["observed_at"]
        if not isinstance(observed, datetime) or observed < start or observed > end:
            continue
        value = (end - observed).total_seconds() * 1000 if metric == "observation_lag" else _number(observation.get(metric))
        if value is None:
            continue
        index = min(bucket_count - 1, max(0, int((observed - start).total_seconds() // period_seconds)))
        buckets[index].append((value, str(observation.get("run_id") or "")))
        observed_any = True
    points = []
    for index, bucket in enumerate(buckets):
        timestamp = start + timedelta(seconds=index * period_seconds)
        if bucket:
            points.append({
                "observed_at": timestamp.isoformat(),
                "value": _aggregate([value for value, _ in bucket], statistic),
                "run_id": bucket[-1][1],
            })
        elif points and points[-1]["value"] is not None:
            # One explicit null is sufficient to preserve a line break. Do not
            # serialize hundreds of identical empty buckets for sparse series.
            points.append({
                "observed_at": timestamp.isoformat(),
                "value": None,
                "run_id": None,
            })
    key = str(producer["key"])
    return {
        "key": f"{key}:{metric}",
        "producer_key": key,
        "producer_name": producer["name"],
        "producer_kind": producer["kind"],
        "producer_class": "primary" if producer["kind"] == "source" else "derived",
        "metric": metric,
        "metric_label": _LABELS[metric],
        "unit": _UNITS[metric],
        "status": "available" if observed_any else "not_observed",
        "reason": None if observed_any else "No observations were recorded in this time range.",
        "points": points if observed_any else [],
    }


def _selected_producer_observations(
    conn: Any,
    *,
    deployment_name: str,
    deployment_dir: Path,
    session: Session,
    selected: set[str],
    start: datetime,
    end: datetime,
    run_limit: int,
) -> list[tuple[dict[str, Any], list[dict[str, Any]]]]:
    source_keys = sorted(key for key in selected if not key.startswith(("_enricher:", "view:")))
    enricher_keys = sorted(key for key in selected if key.startswith("_enricher:"))
    view_keys = sorted(key for key in selected if key.startswith("view:"))

    source_names = [
        key.removeprefix(f"{deployment_name}.")
        for key in source_keys
    ]
    source_runs = _selected_source_runs(
        conn,
        deployment_name=deployment_name,
        names=source_names,
        start=start,
        end=end,
        limit=run_limit,
    )

    enricher_names = [key.removeprefix("_enricher:") for key in enricher_keys]
    enricher_runs = _selected_enricher_runs(
        conn,
        deployment_name=deployment_name,
        session=session,
        names=enricher_names,
        start=start,
        end=end,
        limit=run_limit,
    )

    view_ids = [key.removeprefix("view:") for key in view_keys]
    view_runs = _selected_view_runs(
        conn,
        deployment_name=deployment_name,
        definition_ids=view_ids,
        start=start,
        end=end,
        limit=run_limit,
    )
    selected_view_ids = set(view_ids)
    view_names = {
        entry.view_def_id: entry.view_name
        for entry in (load_views_registry(Path(deployment_dir)) if view_ids else [])
        if entry.view_def_id in selected_view_ids
    }

    producers: list[tuple[dict[str, Any], list[dict[str, Any]]]] = []
    for key, name in zip(source_keys, source_names):
        observations = [
            item for run in source_runs.get(name, [])
            if (item := _source_observation(run))
        ]
        producers.append(({"key": key, "name": name, "kind": "source"}, observations))
    for key, name in zip(enricher_keys, enricher_names):
        observations = [
            item for run in enricher_runs.get(name, [])
            if (item := _derived_observation(run, output_key="n_edges_emitted"))
        ]
        producers.append(({"key": key, "name": name, "kind": "enricher"}, observations))
    for key, definition_id in zip(view_keys, view_ids):
        observations = [
            item for run in view_runs.get(definition_id, [])
            if (item := _derived_observation(run, output_key="row_count"))
        ]
        producers.append(({
            "key": key,
            "name": view_names.get(definition_id, definition_id),
            "kind": "view",
        }, observations))
    return producers


class ProducerMetricsParameterError(ValueError):
    """A request parameter this read refuses, named.

    The endpoint serves ``parameter`` and ``detail`` in its 400. ``detail`` is
    a sentence this module writes about the parameter, never an exception's
    text, so it is safe to serve; any other failure serves no text (#2130).
    """

    def __init__(self, parameter: str, detail: str) -> None:
        super().__init__(detail)
        self.parameter = parameter
        self.detail = detail


def compose_producer_metrics(
    conn: Any,
    *,
    deployment_name: str,
    deployment_dir: Path,
    session: Session,
    producer_keys: Iterable[str] = (),
    metrics: Iterable[str] = _METRICS,
    start: datetime | None = None,
    end: datetime | None = None,
    period: str = "auto",
    statistic: str = "p95",
    max_buckets: int = 720,
    run_limit: int = 50,
) -> dict[str, Any]:
    """Compose a registry-complete, bounded producer metric response."""
    end = end or datetime.now(timezone.utc)
    start = start or end - timedelta(hours=24)
    if start >= end:
        raise ProducerMetricsParameterError(
            "start", "producer metric start must be before end",
        )
    max_buckets = max(1, min(int(max_buckets), 720))
    run_limit = max(1, min(int(run_limit), 50))
    requested_metrics = [metric for metric in metrics if metric in _METRICS]
    if not requested_metrics:
        requested_metrics = list(_METRICS)
    selected = {key for key in producer_keys if key}
    resolved_label, resolved_seconds, resolution_note = _resolve_period(start, end, period, max_buckets)

    if selected:
        producers = _selected_producer_observations(
            conn,
            deployment_name=deployment_name,
            deployment_dir=deployment_dir,
            session=session,
            selected=selected,
            start=start,
            end=end,
            run_limit=run_limit,
        )
    else:
        source_rows = list(compose_sources(conn, deployment_name=deployment_name, session=session).get("sources") or [])
        source_runs, _ = _latest_runs_by_source(
            conn,
            deployment_name=deployment_name,
            source_rows=source_rows,
            limit=run_limit,
        )
        enricher_payload = compose_enricher_diagnostics(
            conn,
            deployment_name=deployment_name,
            session=session,
            run_limit=run_limit,
            run_scope="all",
        )
        view_payload = compose_view_diagnostics(
            conn,
            deployment_name=deployment_name,
            deployment_dir=deployment_dir,
            session=session,
            run_limit=run_limit,
        )
        producers = []
        for row in source_rows:
            name = str(row.get("name") or row.get("source_key") or "Source")
            key = str(row.get("source_key") or name)
            observations = [item for run in source_runs.get(name, []) if (item := _source_observation(run))]
            producers.append(({"key": key, "name": name, "kind": "source"}, observations))
        for row in enricher_payload.get("enrichers") or []:
            name = str(row.get("name") or "Enricher")
            key = str(row.get("producer_key") or f"_enricher:{name}")
            latest = row.get("latest_run")
            raw_runs = [latest] if isinstance(latest, Mapping) else []
            observations = [
                item for run in raw_runs
                if (item := _derived_observation(run, output_key="n_edges_emitted"))
            ]
            producers.append(({"key": key, "name": name, "kind": "enricher"}, observations))
        for row in view_payload.get("views") or []:
            name = str(row.get("display_name") or row.get("name") or "View")
            key = str(row.get("producer_key") or f"view:{row.get('name')}")
            observations = [item for run in row.get("run_history") or [] if (item := _derived_observation(run, output_key="row_count"))]
            producers.append(({"key": key, "name": name, "kind": "view"}, observations))

    series = [
        _series(
            producer,
            observations,
            metric=metric,
            start=start,
            end=end,
            period_seconds=resolved_seconds,
            statistic=statistic,
            max_buckets=max_buckets,
        )
        for producer, observations in producers
        for metric in requested_metrics
    ]
    return {
        "generation_id": session.generation_id,
        "observed_start": start.isoformat(),
        "observed_end": end.isoformat(),
        "resolved_period": resolved_label,
        "resolved_period_seconds": resolved_seconds,
        "statistic": statistic,
        "max_buckets": max_buckets,
        "series": series,
        "omissions": [resolution_note] if resolution_note else [],
    }


__all__ = ["ProducerMetricsParameterError", "compose_producer_metrics"]
