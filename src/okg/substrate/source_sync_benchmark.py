"""Source-sync benchmark artifact validation and normalization."""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from okg.substrate.support.user_hints import okg_prefix


LATENCY_METRICS = (
    "event_to_enqueue_latency_ms",
    "enqueue_to_ingest_latency_ms",
    "ingest_to_publish_latency_ms",
    "event_to_generation_latency_ms",
    "query_visibility_latency_ms",
)
LATENCY_PERCENTILES = ("p50", "p95", "p99")
_PRE_RUN_EXPECTED_DELTA_SOURCES = frozenset({
    "explicit",
    "harness_declared",
})
_SELF_CONFIRMING_EXPECTED_DELTA_SOURCES = frozenset({
    "defaulted_from_observed_delta",
    "observed_delta",
    "observed_delta_default",
    "post_run_observed_delta",
    "self_confirming",
})
_KNOWN_EXPECTED_DELTA_SOURCES = (
    _PRE_RUN_EXPECTED_DELTA_SOURCES
    | _SELF_CONFIRMING_EXPECTED_DELTA_SOURCES
    | {"missing_or_empty"}
)

_LATENCY_ALIASES: Mapping[str, tuple[str, ...]] = {
    "event_to_enqueue_latency_ms": (
        "event_to_enqueue_latency_ms",
        "event_to_enqueue_ms",
        "event_to_enqueue",
    ),
    "enqueue_to_ingest_latency_ms": (
        "enqueue_to_ingest_latency_ms",
        "enqueue_to_ingest_ms",
        "enqueue_to_ingest",
    ),
    "ingest_to_publish_latency_ms": (
        "ingest_to_publish_latency_ms",
        "ingest_to_publish_ms",
        "ingest_to_publish",
    ),
    "event_to_generation_latency_ms": (
        "event_to_generation_latency_ms",
        "event_to_generation_ms",
        "event_to_generation",
    ),
    "query_visibility_latency_ms": (
        "query_visibility_latency_ms",
        "publish_to_query_visibility_latency_ms",
        "generation_to_query_visibility_latency_ms",
        "publish_to_query_visibility_ms",
        "query_visibility_ms",
    ),
}


def load_source_sync_benchmark(path: Path | str) -> dict[str, Any]:
    raw = Path(path).read_text(encoding="utf-8")
    doc = json.loads(raw)
    if not isinstance(doc, dict):
        raise ValueError("source-sync benchmark artifact must be a JSON object")
    return doc


def is_source_sync_benchmark(doc: Mapping[str, Any]) -> bool:
    kind = str(doc.get("kind") or doc.get("artifact_kind") or "").replace("-", "_")
    return kind in {
        "source_sync_benchmark",
        "okg.source_sync_benchmark",
    } or doc.get("source_sync_benchmark") is True


def validate_source_sync_benchmark(
    doc: Mapping[str, Any],
    *,
    artifact: str = "",
    deployment: str | None = None,
    known_sources: Sequence[str] = (),
    latency_budgets_ms: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    if not is_source_sync_benchmark(doc):
        errors.append({
            "code": "source_sync_benchmark_invalid_kind",
            "field": "kind",
            "message": "artifact kind must be source_sync_benchmark",
        })

    deployment_value = str(doc.get("deployment") or deployment or "")
    if not deployment_value:
        errors.append(_missing_required("deployment"))
    elif deployment is not None and deployment_value != deployment:
        errors.append({
            "code": "deployment_mismatch",
            "field": "deployment",
            "expected": deployment,
            "actual": deployment_value,
        })

    scenario = str(doc.get("scenario") or "")
    if not scenario:
        errors.append(_missing_required("scenario"))
        scenario = "source_sync"
    source_name = str(doc.get("source_name") or doc.get("source") or "")
    if not source_name:
        errors.append(_missing_required("source_name"))
    elif known_sources and not _source_known(
        source_name,
        source_names=_source_names(doc),
        deployment=deployment_value,
        known_sources=known_sources,
    ):
        errors.append({
            "code": "unknown_source",
            "field": "source_name",
            "source_name": source_name,
        })

    latency_ms = _latency_metrics(doc)
    for metric in LATENCY_METRICS:
        if metric not in latency_ms:
            errors.append({
                "code": "missing_latency_metric",
                "field": metric,
                "message": f"missing {metric}",
            })
    budgets = _latency_budgets(doc, latency_budgets_ms or {})
    for metric, value in latency_ms.items():
        if value < 0:
            errors.append({
                "code": "invalid_latency_metric",
                "field": metric,
                "actual": value,
                "message": "latency must be non-negative",
            })
        threshold = budgets.get(metric)
        if threshold is not None and value > threshold:
            errors.append({
                "code": "latency_budget_exceeded",
                "field": metric,
                "actual": value,
                "threshold": threshold,
            })

    percentiles_ms = _latency_percentiles(doc)
    for percentile in LATENCY_PERCENTILES:
        if percentile not in percentiles_ms:
            errors.append({
                "code": "missing_latency_percentile",
                "field": f"latency_percentiles_ms.{percentile}",
                "message": f"missing latency percentile {percentile}",
            })

    queue = _queue_state(doc)
    for field in ("queue_depth", "dlq_count"):
        if field not in queue:
            errors.append({
                "code": "missing_queue_field",
                "field": field,
                "message": f"missing queue {field}",
            })
    if queue.get("dlq_count", 0.0) > 0:
        errors.append({
            "code": "source_sync_dlq_not_empty",
            "field": "dlq_count",
            "actual": queue["dlq_count"],
        })

    expected_delta_source = _expected_delta_source(doc)
    expected_delta = _numeric_mapping(doc.get("expected_delta"))
    observed_delta = _numeric_mapping(doc.get("observed_delta"))
    if not expected_delta_source and expected_delta:
        expected_delta_source = "explicit"
    invalid_expected_delta_source = (
        expected_delta_source
        and expected_delta_source not in _KNOWN_EXPECTED_DELTA_SOURCES
    )
    if invalid_expected_delta_source:
        errors.append({
            "code": "invalid_expected_delta_source",
            "field": "capture.expected_delta_source",
            "actual": expected_delta_source,
        })
    if not expected_delta:
        errors.append({
            "code": "missing_expected_delta",
            "field": "expected_delta",
        })
    elif expected_delta_source in _SELF_CONFIRMING_EXPECTED_DELTA_SOURCES:
        errors.append({
            "code": "self_confirming_expected_delta",
            "field": "capture.expected_delta_source",
            "actual": expected_delta_source,
            "message": (
                "expected_delta must come from a pre-run contract, not "
                "observed_delta"
            ),
        })
    elif (
        not invalid_expected_delta_source
        and expected_delta_source not in _PRE_RUN_EXPECTED_DELTA_SOURCES
    ):
        errors.append({
            "code": "invalid_expected_delta_source",
            "field": "capture.expected_delta_source",
            "actual": expected_delta_source or None,
            "message": (
                "expected_delta_source must identify a pre-run expected "
                "delta source"
            ),
        })
    if not observed_delta:
        errors.append({
            "code": "missing_observed_delta",
            "field": "observed_delta",
        })
    self_confirming_expected_delta = (
        bool(expected_delta)
        and expected_delta_source in _SELF_CONFIRMING_EXPECTED_DELTA_SOURCES
    )
    delta_mismatches = _delta_mismatches(expected_delta, observed_delta)
    errors.extend(delta_mismatches)

    work = _work_metrics(doc)
    for metric, value in work.items():
        if value < 0:
            errors.append({
                "code": "invalid_work_metric",
                "field": f"work.{metric}",
                "actual": value,
                "message": "work metrics must be non-negative",
            })

    query_visible = doc.get("query_visible")
    if query_visible is None:
        query_visible = doc.get("published_generation_query_visible")
    if query_visible is None:
        errors.append({
            "code": "query_visibility_missing",
            "field": "query_visible",
        })
    elif query_visible is not True:
        errors.append({
            "code": "query_visibility_false",
            "field": "query_visible",
            "actual": query_visible,
        })

    expected_digest = str(doc.get("state_digest_expected") or "")
    observed_digest = str(doc.get("state_digest_observed") or "")
    if expected_digest and observed_digest and expected_digest != observed_digest:
        errors.append({
            "code": "state_digest_mismatch",
            "field": "state_digest",
            "expected": expected_digest,
            "actual": observed_digest,
        })

    generation_id = doc.get("published_generation_id") or doc.get("generation_id")
    target_generation_id = doc.get("target_generation_id")
    if generation_id is None and target_generation_id is None:
        errors.append(_missing_required("generation_id"))

    report = {
        "kind": "source_sync_benchmark_validation",
        "ok": not errors,
        "artifact": artifact,
        "deployment": deployment_value,
        "scenario": scenario,
        "source_name": source_name,
        "generation_id": generation_id,
        "target_generation_id": target_generation_id,
        "latency_ms": latency_ms,
        "latency_percentiles_ms": percentiles_ms,
        "latency_budgets_ms": budgets,
        "queue": queue,
        "consistency": {
            "ok": bool(expected_delta) and bool(observed_delta) and not delta_mismatches
                  and not self_confirming_expected_delta
                  and query_visible is True
                  and not (
                      expected_digest
                      and observed_digest
                      and expected_digest != observed_digest
                  ),
            "expected_delta": expected_delta,
            "observed_delta": observed_delta,
            "expected_delta_source": expected_delta_source,
            "query_visible": query_visible,
            "state_digest_expected": expected_digest,
            "state_digest_observed": observed_digest,
        },
        "work": work,
        "errors": errors,
        "warnings": warnings,
        "profile_measurements": _profile_measurements_hint(
            deployment=deployment_value,
            artifact=artifact,
        ),
    }
    report["measurements"] = source_sync_benchmark_measurement_rows(
        repo=str(doc.get("repo") or ""),
        artifact=artifact,
        doc=doc,
        validation=report,
    )
    return report


def build_source_sync_benchmark_artifact(
    debug_workflow: Mapping[str, Any],
    *,
    deployment: str | None = None,
    repo: str = "",
    scenario: str | None = None,
    expected_delta: Mapping[str, Any] | None = None,
    observed_delta: Mapping[str, Any] | None = None,
    query_visible: bool | None = None,
    query_visibility_latency_ms: float | int | None = None,
    queue: Mapping[str, Any] | None = None,
    profile_run_id: str | None = None,
    readiness_ms: float | int | None = None,
    mcp_graph_read_ms: float | int | None = None,
) -> dict[str, Any]:
    """Build a source-sync benchmark artifact from debug/readiness payloads.

    The artifact is intentionally plain JSON so it can be reviewed, validated
    with ``okg sync benchmark``, and ingested by ``profile-measurements``.
    """
    source_sync = (
        debug_workflow.get("source_sync")
        if isinstance(debug_workflow.get("source_sync"), Mapping) else {}
    )
    stage_timing = (
        source_sync.get("stage_timing")
        if isinstance(source_sync.get("stage_timing"), Mapping) else {}
    )
    _require_terminal_source_sync_debug(
        debug_workflow,
        source_sync=source_sync,
        stage_timing=stage_timing,
    )
    dbos = (
        debug_workflow.get("dbos")
        if isinstance(debug_workflow.get("dbos"), Mapping) else {}
    )
    deployment_value = str(
        deployment
        or source_sync.get("deployment")
        or debug_workflow.get("deployment")
        or ""
    )
    workflow_id = str(debug_workflow.get("workflow_id") or "")
    scenario_value = str(
        scenario
        or source_sync.get("scenario")
        or f"source_sync.{workflow_id or 'workflow'}"
    )
    sources = source_sync.get("sources")
    if not isinstance(sources, Sequence) or isinstance(sources, str):
        sources = []
    source_name = (
        str(sources[0])
        if len(sources) == 1 else
        "multi-source"
        if sources else
        ""
    )
    source_names = [str(source) for source in sources if source]

    created_at = (
        _parse_dt(debug_workflow.get("created_at"))
        or _parse_dt(debug_workflow.get("started_at"))
    )
    completed_at = _parse_dt(debug_workflow.get("completed_at"))
    event_observed_at = _first_event_observed_at(source_sync)
    source_event_id = _first_event_field(source_sync, "event_id")
    queue_dwell_raw = _number(source_sync.get("queue_dwell_ms"))
    queue_dwell_ms = queue_dwell_raw or 0.0
    lifetime_ms = _number(debug_workflow.get("lifetime_ms"))
    if lifetime_ms is None and created_at is not None and completed_at is not None:
        lifetime_ms = max(0.0, (completed_at - created_at).total_seconds() * 1000)
    if lifetime_ms is None:
        lifetime_ms = _sum_stage_total_ms(stage_timing)
    event_to_enqueue_ms = 0.0
    if created_at is not None and event_observed_at is not None:
        event_to_enqueue_ms = max(
            0.0,
            (created_at - event_observed_at).total_seconds() * 1000,
        )
    event_to_generation_ms = event_to_enqueue_ms + (lifetime_ms or 0.0)
    publish_total_ms = _sum_publish_total_ms(stage_timing)
    ingest_to_publish_ms = (
        publish_total_ms
        if publish_total_ms > 0 else
        max(0.0, (lifetime_ms or 0.0) - queue_dwell_ms)
    )
    query_visibility_ms = _number(query_visibility_latency_ms)
    if query_visibility_ms is None:
        query_visibility_ms = 0.0

    observed = dict(observed_delta or _observed_delta_from_publishes(stage_timing))
    expected = dict(expected_delta or {})
    queue_state = {
        "queue_depth": _number((queue or {}).get("queue_depth")) or 0.0,
        "dlq_count": _number((queue or {}).get("dlq_count")) or 0.0,
    }
    profile_id = (
        profile_run_id
        or _profile_run_id_from_path(debug_workflow.get("profile_path"))
        or ""
    )
    published_generation_id = _last_published_generation(stage_timing)
    artifact = {
        "kind": "source_sync_benchmark",
        "deployment": deployment_value,
        "repo": repo,
        "scenario": scenario_value,
        "source_name": source_name,
        "source_names": source_names,
        "source_event_id": source_event_id,
        "workflow_id": workflow_id,
        "owner_id": "workflow:source_sync:" + deployment_value if deployment_value else "",
        "published_generation_id": published_generation_id,
        "profile_run_id": profile_id,
        "latency_ms": {
            "event_to_enqueue_latency_ms": event_to_enqueue_ms,
            "enqueue_to_ingest_latency_ms": queue_dwell_ms,
            "ingest_to_publish_latency_ms": ingest_to_publish_ms,
            "event_to_generation_latency_ms": event_to_generation_ms,
            "query_visibility_latency_ms": query_visibility_ms,
        },
        "latency_percentiles_ms": _single_sample_percentiles(
            event_to_generation_ms,
        ),
        "queue": queue_state,
        "expected_delta": expected,
        "observed_delta": observed,
        "work": _work_summary_from_debug(
            source_sync,
            stage_timing,
            expected_delta=expected,
            observed_delta=observed,
        ),
        "query_visible": bool(query_visible),
        "stage_measurements": _stage_measurements_from_debug(
            debug_workflow,
            readiness_ms=readiness_ms,
            mcp_graph_read_ms=mcp_graph_read_ms,
            query_visibility_latency_ms=query_visibility_ms,
        ),
        "capture": {
            "latency_sample_count": 1,
            "percentile_method": "single_sample",
            "workflow_status": debug_workflow.get("status"),
            "queue_name": source_sync.get("dbos", {}).get("queue_name")
            if isinstance(source_sync.get("dbos"), Mapping)
            else dbos.get("queue_name"),
            "deduplication_id": dbos.get("deduplication_id"),
            "application_version": dbos.get("application_version"),
            "event_observed_at": (
                event_observed_at.isoformat()
                if event_observed_at is not None else None
            ),
            "source_event_id": source_event_id,
            "created_at": (
                created_at.isoformat() if created_at is not None else None
            ),
            "completed_at": (
                completed_at.isoformat() if completed_at is not None else None
            ),
            "stage_timing_status": stage_timing.get("status"),
            "stage_timing_unavailable_reason": stage_timing.get(
                "unavailable_reason",
            ),
            "expected_delta_source": (
                "explicit" if expected else "missing_or_empty"
            ),
        },
    }
    unavailable = _unavailable_measurements(
        debug_workflow,
        created_at,
        event_observed_at,
        queue_dwell_raw,
    )
    if unavailable:
        artifact["unavailable_measurements"] = unavailable
    return artifact


def _require_terminal_source_sync_debug(
    debug_workflow: Mapping[str, Any],
    *,
    source_sync: Mapping[str, Any],
    stage_timing: Mapping[str, Any],
) -> None:
    """Reject debug payloads that cannot support a real benchmark sample."""
    workflow_name = str(debug_workflow.get("name") or "")
    if workflow_name and workflow_name != "okg_source_sync":
        raise ValueError(
            "source-sync benchmark capture requires okg_source_sync workflow; "
            f"got name={workflow_name!r}",
        )
    if not source_sync:
        raise ValueError("source-sync benchmark capture requires source_sync details")
    status = str(debug_workflow.get("status") or "").upper()
    if status != "SUCCESS":
        raise ValueError(
            "source-sync benchmark capture requires terminal SUCCESS workflow; "
            f"got status={status or 'missing'!r}",
        )
    stage_status = str(stage_timing.get("status") or "")
    if stage_status != "available":
        raise ValueError(
            "source-sync benchmark capture requires available source stage timing; "
            f"got status={stage_status or 'missing'!r}",
        )
    sources = stage_timing.get("sources")
    if not isinstance(sources, Sequence) or isinstance(sources, str) or not sources:
        raise ValueError(
            "source-sync benchmark capture requires at least one timed source",
        )
    timed_source_count = _number(stage_timing.get("timed_source_count"))
    if timed_source_count is not None and timed_source_count <= 0:
        raise ValueError(
            "source-sync benchmark capture requires timed_source_count > 0; "
            f"got {timed_source_count:g}",
        )
    if _last_published_generation(stage_timing) is None:
        raise ValueError(
            "source-sync benchmark capture requires a published generation",
        )


def source_sync_benchmark_measurement_rows(
    *,
    repo: str,
    artifact: str,
    doc: Mapping[str, Any],
    validation: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    report = validation or validate_source_sync_benchmark(doc, artifact=artifact)
    scenario = str(report.get("scenario") or doc.get("scenario") or "source_sync")
    generation_id = report.get("target_generation_id") or report.get("generation_id")
    profile_run_id = str(doc.get("profile_run_id") or "")
    latency_budgets = report.get("latency_budgets_ms")
    if not isinstance(latency_budgets, Mapping):
        latency_budgets = {}
    base_attrs = _dimension_attrs(doc)
    unavailable_metrics = _unavailable_metric_set(doc)

    rows: list[dict[str, Any]] = []
    for metric, value in sorted(_as_float_mapping(report.get("latency_ms")).items()):
        status = _field_status(report, metric)
        if metric in unavailable_metrics:
            status = "unavailable"
        rows.append(_measurement_row(
            repo=repo,
            artifact=artifact,
            scenario=scenario,
            metric=metric,
            value=value,
            unit="ms",
            status=status,
            generation_id=generation_id,
            profile_run_id=profile_run_id,
            threshold=latency_budgets.get(metric),
            extra_attrs=base_attrs,
        ))
    for percentile, value in sorted(_as_float_mapping(
        report.get("latency_percentiles_ms")
    ).items()):
        rows.append(_measurement_row(
            repo=repo,
            artifact=artifact,
            scenario=scenario,
            metric=f"source_sync_latency_{percentile}_ms",
            value=value,
            unit="ms",
            status=_field_status(report, f"latency_percentiles_ms.{percentile}"),
            generation_id=generation_id,
            profile_run_id=profile_run_id,
            extra_attrs={**base_attrs, "percentile": percentile},
        ))
    consistency = report.get("consistency")
    if not isinstance(consistency, Mapping):
        consistency = {}
    expected_delta = _as_float_mapping(consistency.get("expected_delta"))
    observed_delta = _as_float_mapping(consistency.get("observed_delta"))
    consistency_ok = bool(consistency.get("ok"))
    for key, value in sorted(expected_delta.items()):
        rows.append(_measurement_row(
            repo=repo,
            artifact=artifact,
            scenario=scenario,
            metric=f"expected_delta_{key}",
            value=value,
            unit="count",
            status="passed" if consistency_ok else "failed",
            generation_id=generation_id,
            profile_run_id=profile_run_id,
            extra_attrs=base_attrs,
        ))
    for key, value in sorted(observed_delta.items()):
        rows.append(_measurement_row(
            repo=repo,
            artifact=artifact,
            scenario=scenario,
            metric=f"observed_delta_{key}",
            value=value,
            unit="count",
            status="passed" if consistency_ok else "failed",
            generation_id=generation_id,
            profile_run_id=profile_run_id,
            extra_attrs=base_attrs,
        ))
    if expected_delta or observed_delta:
        rows.append(_measurement_row(
            repo=repo,
            artifact=artifact,
            scenario=scenario,
            metric="expected_delta_consistency",
            value=1.0 if consistency_ok else 0.0,
            unit="boolean",
            status="passed" if consistency_ok else "failed",
            generation_id=generation_id,
            profile_run_id=profile_run_id,
            extra_attrs=base_attrs,
        ))
    work = _as_float_mapping(report.get("work"))
    for metric, value in sorted(work.items()):
        rows.append(_measurement_row(
            repo=repo,
            artifact=artifact,
            scenario=scenario,
            metric=metric,
            value=value,
            unit="ratio" if metric.endswith("_ratio") else "count",
            status=_field_status(report, f"work.{metric}"),
            generation_id=generation_id,
            profile_run_id=profile_run_id,
            extra_attrs=base_attrs,
        ))
    queue = _as_float_mapping(report.get("queue"))
    for metric, value in sorted(queue.items()):
        rows.append(_measurement_row(
            repo=repo,
            artifact=artifact,
            scenario=scenario,
            metric=metric,
            value=value,
            unit="count",
            status="failed" if metric == "dlq_count" and value > 0 else "passed",
            generation_id=generation_id,
            profile_run_id=profile_run_id,
            extra_attrs=base_attrs,
        ))
    stage_measurements = doc.get("stage_measurements") or doc.get("stages")
    if isinstance(stage_measurements, Sequence) and not isinstance(
        stage_measurements, str,
    ):
        for idx, raw in enumerate(stage_measurements):
            if not isinstance(raw, Mapping):
                continue
            metric = str(raw.get("metric") or raw.get("name") or "")
            value = _number(raw.get("value"))
            if value is None:
                value = _number(raw.get("duration_ms"))
            if not metric or value is None:
                continue
            rows.append(_measurement_row(
                repo=repo,
                artifact=artifact,
                scenario=str(raw.get("scenario") or scenario),
                metric=metric,
                value=value,
                unit=str(raw.get("unit") or "ms"),
                status=str(raw.get("status") or "passed"),
                generation_id=raw.get("generation_id") or generation_id,
                profile_run_id=str(raw.get("profile_run_id") or profile_run_id),
                threshold=raw.get("threshold"),
                extra_attrs={
                    **base_attrs,
                    **_dimension_attrs(raw),
                    "stage_measurement_index": idx,
                },
            ))
    return rows


def _latency_metrics(doc: Mapping[str, Any]) -> dict[str, float]:
    sources = [
        doc.get("latency_ms"),
        doc.get("latencies_ms"),
        doc.get("latency"),
        doc,
    ]
    out: dict[str, float] = {}
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        for metric, aliases in _LATENCY_ALIASES.items():
            if metric in out:
                continue
            value = _first_numeric(source, aliases)
            if value is not None:
                out[metric] = value
    return out


def _latency_percentiles(doc: Mapping[str, Any]) -> dict[str, float]:
    raw = (
        doc.get("latency_percentiles_ms")
        or doc.get("percentiles_ms")
        or doc.get("latency_percentiles")
    )
    out: dict[str, float] = {}
    if isinstance(raw, Mapping):
        for percentile in LATENCY_PERCENTILES:
            value = _number(raw.get(percentile))
            if value is not None:
                out[percentile] = value
    for percentile in LATENCY_PERCENTILES:
        if percentile in out:
            continue
        value = _number(doc.get(f"{percentile}_latency_ms"))
        if value is not None:
            out[percentile] = value
    return out


def _latency_budgets(
    doc: Mapping[str, Any],
    explicit: Mapping[str, float],
) -> dict[str, float]:
    raw = doc.get("latency_budgets_ms") or doc.get("budgets_ms") or {}
    out: dict[str, float] = {}
    if isinstance(raw, Mapping):
        for key, value in raw.items():
            parsed = _number(value)
            if parsed is not None:
                out[str(key)] = parsed
    for key, value in explicit.items():
        parsed = _number(value)
        if parsed is not None:
            out[str(key)] = parsed
    return out


def _queue_state(doc: Mapping[str, Any]) -> dict[str, float]:
    raw = doc.get("queue") or doc.get("queue_state") or {}
    out: dict[str, float] = {}
    if isinstance(raw, Mapping):
        for target, aliases in {
            "queue_depth": ("queue_depth", "active_queue_depth"),
            "dlq_count": ("dlq_count", "dead_letter_count"),
        }.items():
            value = _first_numeric(raw, aliases)
            if value is not None:
                out[target] = value
    for target in ("queue_depth", "dlq_count"):
        if target not in out:
            value = _number(doc.get(target))
            if value is not None:
                out[target] = value
    return out


def _numeric_mapping(value: Any) -> dict[str, float]:
    if not isinstance(value, Mapping):
        return {}
    return _as_float_mapping(value)


def _expected_delta_source(doc: Mapping[str, Any]) -> str:
    raw = doc.get("expected_delta_source")
    capture = doc.get("capture")
    if raw in (None, "") and isinstance(capture, Mapping):
        raw = capture.get("expected_delta_source")
    return str(raw or "").strip().lower()


def _work_metrics(doc: Mapping[str, Any]) -> dict[str, float]:
    raw = doc.get("work") or doc.get("work_measurements") or {}
    return _as_float_mapping(raw)


def _as_float_mapping(value: Any) -> dict[str, float]:
    out: dict[str, float] = {}
    if not isinstance(value, Mapping):
        return out
    for key, raw in value.items():
        parsed = _number(raw)
        if parsed is not None:
            out[str(key)] = parsed
    return out


def _delta_mismatches(
    expected_delta: Mapping[str, float],
    observed_delta: Mapping[str, float],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for key, expected in sorted(expected_delta.items()):
        if key not in observed_delta:
            out.append({
                "code": "expected_delta_missing_observed",
                "field": f"observed_delta.{key}",
                "expected": expected,
            })
            continue
        observed = observed_delta[key]
        if observed != expected:
            out.append({
                "code": "expected_delta_mismatch",
                "field": f"observed_delta.{key}",
                "expected": expected,
                "actual": observed,
            })
    return out


def _measurement_row(
    *,
    repo: str,
    artifact: str,
    scenario: str,
    metric: str,
    value: float,
    unit: str,
    status: str,
    generation_id: Any,
    profile_run_id: str,
    threshold: Any = None,
    extra_attrs: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    measurement_id = "benchmark:" + _sha(
        json.dumps(
            [repo, artifact, scenario, metric, extra_attrs or {}],
            sort_keys=True,
            default=str,
        )
    )
    row = {
        "measurement_id": measurement_id,
        "repo": repo,
        "scenario": scenario,
        "metric": metric,
        "value": value,
        "unit": unit,
        "status": status,
        "threshold": threshold,
        "generation_id": generation_id,
        "profile_run_id": profile_run_id,
        "artifact": artifact,
    }
    if extra_attrs:
        row.update({
            str(key): value
            for key, value in extra_attrs.items()
            if value not in (None, "")
        })
    return row


def _dimension_attrs(source: Mapping[str, Any]) -> dict[str, Any]:
    attrs: dict[str, Any] = {}
    for key in (
        "source_name",
        "source",
        "stage_name",
        "stage",
        "enricher_name",
        "event_id",
        "source_event_id",
        "workflow_id",
        "request_path",
        "owner_id",
        "owner",
        "next_action",
        "input_count",
        "output_count",
        "percentile",
        "timing_status",
        "timing_unavailable_reason",
        "unavailable_reason",
        "nodes_appended",
        "edges_appended",
        "retract_n",
        "retract_e",
        "queue_depth",
        "dlq_count",
        "generation_id",
    ):
        value = source.get(key)
        if value not in (None, ""):
            target = {
                "source": "source_name",
                "stage": "stage_name",
                "request_path": "workflow_id",
                "owner": "owner_id",
            }.get(key, key)
            attrs[target] = value
    return attrs


def _stage_measurements_from_debug(
    debug_workflow: Mapping[str, Any],
    *,
    readiness_ms: float | int | None,
    mcp_graph_read_ms: float | int | None,
    query_visibility_latency_ms: float,
) -> list[dict[str, Any]]:
    source_sync = (
        debug_workflow.get("source_sync")
        if isinstance(debug_workflow.get("source_sync"), Mapping) else {}
    )
    stage_timing = (
        source_sync.get("stage_timing")
        if isinstance(source_sync.get("stage_timing"), Mapping) else {}
    )
    workflow_id = str(debug_workflow.get("workflow_id") or "")
    profile_run_id = _profile_run_id_from_path(debug_workflow.get("profile_path")) or ""
    rows: list[dict[str, Any]] = []
    for source in stage_timing.get("sources") or ():
        if not isinstance(source, Mapping):
            continue
        timings = (
            source.get("timings_ms")
            if isinstance(source.get("timings_ms"), Mapping) else {}
        )
        for stage_name, raw_value in sorted(timings.items()):
            value = _number(raw_value)
            if value is None:
                continue
            rows.append({
                "metric": f"{stage_name}_ms",
                "value": value,
                "unit": "ms",
                "source_name": source.get("name"),
                "stage_name": stage_name,
                "workflow_id": workflow_id,
                "profile_run_id": profile_run_id,
                "input_count": _source_input_count(source),
                "output_count": _source_output_count(source),
                "nodes_appended": source.get("nodes_appended"),
                "edges_appended": source.get("edges_appended"),
                "timing_status": source.get("timing_status"),
                "timing_unavailable_reason": source.get(
                    "timing_unavailable_reason",
                ),
                "status": (
                    "passed"
                    if not source.get("timing_unavailable_reason")
                    else "unavailable"
                ),
            })
    for publish in stage_timing.get("publishes") or ():
        if not isinstance(publish, Mapping):
            continue
        label = str(publish.get("label") or "publish")
        total_ms = _number(publish.get("total_duration_ms"))
        generation_id = publish.get("generation_id")
        if total_ms is not None:
            rows.append({
                "metric": "publish_total_ms",
                "value": total_ms,
                "unit": "ms",
                "stage_name": label,
                "workflow_id": workflow_id,
                "generation_id": generation_id,
                "nodes_appended": publish.get("nodes_added"),
                "edges_appended": publish.get("edges_added"),
                "status": publish.get("status") or "passed",
            })
        for enricher in publish.get("enrichers") or ():
            if not isinstance(enricher, Mapping):
                continue
            rows.append({
                "metric": "enricher_record_count",
                "value": _number(enricher.get("record_count")) or 0.0,
                "unit": "count",
                "stage_name": "enricher",
                "enricher_name": enricher.get("source")
                or enricher.get("producer_id"),
                "workflow_id": workflow_id,
                "generation_id": generation_id,
                "status": enricher.get("status") or "passed",
            })
    for metric, value in (
        ("readiness_ms", _number(readiness_ms)),
        ("mcp_graph_read_ms", _number(mcp_graph_read_ms)),
        ("query_visibility_latency_ms", _number(query_visibility_latency_ms)),
    ):
        if value is None:
            continue
        rows.append({
            "metric": metric,
            "value": value,
            "unit": "ms",
            "stage_name": metric.removesuffix("_ms"),
            "workflow_id": workflow_id,
            "status": "passed",
        })
    return rows


def _work_summary_from_debug(
    source_sync: Mapping[str, Any],
    stage_timing: Mapping[str, Any],
    *,
    expected_delta: Mapping[str, Any],
    observed_delta: Mapping[str, Any],
) -> dict[str, float]:
    rows = stage_timing.get("sources") or ()
    sources = [
        source for source in rows
        if isinstance(source, Mapping)
    ] if isinstance(rows, Sequence) and not isinstance(rows, str) else []
    publishes = stage_timing.get("publishes") or ()
    publish_count = (
        sum(1 for item in publishes if isinstance(item, Mapping))
        if isinstance(publishes, Sequence) and not isinstance(publishes, str)
        else 0
    )
    out: dict[str, float] = {}
    event_count = _number(source_sync.get("event_count"))
    if event_count is not None:
        out["source_sync_event_count"] = event_count
    if sources:
        out["source_count"] = float(len(sources))
    if publish_count:
        out["publish_count"] = float(publish_count)

    input_count = 0.0
    input_seen = False
    output_count = 0.0
    nodes_appended = 0.0
    edges_appended = 0.0
    nodes_retracted = 0.0
    edges_retracted = 0.0
    for source in sources:
        source_input = _source_input_count(source)
        if source_input is not None:
            input_seen = True
            input_count += source_input
        output_count += _source_output_count(source)
        nodes_appended += _first_numeric(
            source, ("nodes_appended", "nodes_added"),
        ) or 0.0
        edges_appended += _first_numeric(
            source, ("edges_appended", "edges_added"),
        ) or 0.0
        nodes_retracted += _first_numeric(
            source, ("retract_n", "retractions_node", "nodes_retracted"),
        ) or 0.0
        edges_retracted += _first_numeric(
            source, ("retract_e", "retractions_edge", "edges_retracted"),
        ) or 0.0

    if input_seen:
        out["source_input_count"] = input_count
    if sources:
        out["source_output_count"] = output_count
        out["source_work_units"] = output_count
        out["nodes_appended"] = nodes_appended
        out["edges_appended"] = edges_appended
        out["nodes_retracted"] = nodes_retracted
        out["edges_retracted"] = edges_retracted

    expected_units = _delta_units(expected_delta)
    observed_units = _delta_units(observed_delta)
    if expected_delta:
        out["expected_delta_units"] = expected_units
    if observed_delta:
        out["observed_delta_units"] = observed_units
    if observed_units > 0 and sources:
        out["work_amplification_ratio"] = output_count / observed_units
    if observed_units > 0 and input_seen:
        out["scan_amplification_ratio"] = input_count / observed_units
    if input_seen and input_count > 0 and sources:
        out["emit_amplification_ratio"] = output_count / input_count
    return out


def _delta_units(delta: Mapping[str, Any]) -> float:
    return sum(_as_float_mapping(delta).values())


def _observed_delta_from_publishes(stage_timing: Mapping[str, Any]) -> dict[str, float]:
    nodes_added = 0.0
    edges_added = 0.0
    for publish in stage_timing.get("publishes") or ():
        if not isinstance(publish, Mapping):
            continue
        nodes_added += _number(publish.get("nodes_added")) or 0.0
        edges_added += _number(publish.get("edges_added")) or 0.0
    return {"nodes_added": nodes_added, "edges_added": edges_added}


def _sum_publish_total_ms(stage_timing: Mapping[str, Any]) -> float:
    total = 0.0
    for publish in stage_timing.get("publishes") or ():
        if isinstance(publish, Mapping):
            total += _number(publish.get("total_duration_ms")) or 0.0
    return total


def _sum_stage_total_ms(stage_timing: Mapping[str, Any]) -> float:
    total = 0.0
    for source in stage_timing.get("sources") or ():
        if not isinstance(source, Mapping):
            continue
        timings = (
            source.get("timings_ms")
            if isinstance(source.get("timings_ms"), Mapping) else {}
        )
        total += _number(timings.get("total")) or 0.0
    total += _sum_publish_total_ms(stage_timing)
    return total


def _last_published_generation(stage_timing: Mapping[str, Any]) -> Any:
    generation_id = None
    for publish in stage_timing.get("publishes") or ():
        if isinstance(publish, Mapping) and publish.get("generation_id"):
            generation_id = publish.get("generation_id")
    return generation_id


def _single_sample_percentiles(value: float) -> dict[str, float]:
    return {percentile: value for percentile in LATENCY_PERCENTILES}


def _source_output_count(source: Mapping[str, Any]) -> float:
    explicit = _number(source.get("output_count"))
    if explicit is not None:
        return explicit
    return (
        (_first_numeric(source, ("nodes_appended", "nodes_added")) or 0.0)
        + (_first_numeric(source, ("edges_appended", "edges_added")) or 0.0)
        + (_first_numeric(
            source, ("retract_n", "retractions_node", "nodes_retracted"),
        ) or 0.0)
        + (_first_numeric(
            source, ("retract_e", "retractions_edge", "edges_retracted"),
        ) or 0.0)
    )


def _source_input_count(source: Mapping[str, Any]) -> float | None:
    explicit = _number(source.get("input_count"))
    if explicit is not None:
        return explicit
    source_stats = source.get("source_stats")
    if isinstance(source_stats, Mapping):
        return _first_numeric(
            source_stats,
            (
                "input_count",
                "record_count",
                "records",
                "records_scanned",
                "records_read",
            ),
        )
    return None


def _first_event_observed_at(source_sync: Mapping[str, Any]) -> datetime | None:
    candidates: list[datetime] = []
    for event in source_sync.get("event_sample") or ():
        if isinstance(event, Mapping):
            observed = _parse_dt(event.get("observed_at"))
            if observed is not None:
                candidates.append(observed)
    return min(candidates) if candidates else None


def _first_event_field(source_sync: Mapping[str, Any], field: str) -> str:
    for event in source_sync.get("event_sample") or ():
        if isinstance(event, Mapping) and event.get(field):
            return str(event[field])
    return ""


def _parse_dt(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _profile_run_id_from_path(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    parts = Path(value).parts
    if len(parts) >= 4 and parts[-4] == "profiles" and parts[-1] == "profile.json":
        return f"profile-run:{parts[-3]}:{parts[-2]}"
    return None


def _unavailable_measurements(
    debug_workflow: Mapping[str, Any],
    created_at: datetime | None,
    event_observed_at: datetime | None,
    queue_dwell_ms: float | None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if event_observed_at is None:
        out.append({
            "metric": "event_to_enqueue_latency_ms",
            "reason": "source event observed_at unavailable in debug payload",
        })
    elif created_at is None:
        out.append({
            "metric": "event_to_enqueue_latency_ms",
            "reason": "workflow created_at/started_at unavailable in debug payload",
        })
    if queue_dwell_ms is None:
        out.append({
            "metric": "enqueue_to_ingest_latency_ms",
            "reason": "source-sync queue_dwell_ms unavailable in debug payload",
        })
    source_sync = (
        debug_workflow.get("source_sync")
        if isinstance(debug_workflow.get("source_sync"), Mapping) else {}
    )
    stage_timing = (
        source_sync.get("stage_timing")
        if isinstance(source_sync.get("stage_timing"), Mapping) else {}
    )
    reason = stage_timing.get("unavailable_reason")
    if reason:
        out.append({
            "metric": "stage_timing",
            "reason": reason,
        })
    return out


def _field_status(report: Mapping[str, Any], field: str) -> str:
    errors = report.get("errors")
    if isinstance(errors, Sequence):
        for error in errors:
            if isinstance(error, Mapping) and error.get("field") == field:
                return "failed"
    return "passed"


def _profile_measurements_hint(
    *,
    deployment: str,
    artifact: str,
) -> dict[str, Any]:
    hint = {
        "source": "profile-measurements",
        "artifact_globs": [
            "reports/**/*benchmark*.json",
            ".artifacts/**/*benchmark*.json",
        ],
    }
    if deployment and artifact:
        hint["ingest_command"] = (
            f"{okg_prefix()} ingest --deployment {deployment} "
            f"--include profile-measurements --mode scope_complete "
            f"--inline --json --no-predecessors"
        )
    return hint


def _source_known(
    source_name: str,
    *,
    source_names: Sequence[str] = (),
    deployment: str,
    known_sources: Sequence[str],
) -> bool:
    if source_name == "multi-source":
        return bool(source_names) and all(
            _source_known(
                str(source),
                deployment=deployment,
                known_sources=known_sources,
            )
            for source in source_names
        )
    known = {str(source) for source in known_sources}
    if source_name in known:
        return True
    if deployment and source_name.startswith(f"{deployment}."):
        return source_name.removeprefix(f"{deployment}.") in known
    return deployment and f"{deployment}.{source_name}" in known


def _source_names(doc: Mapping[str, Any]) -> tuple[str, ...]:
    raw = doc.get("source_names") or doc.get("sources") or ()
    if not isinstance(raw, Sequence) or isinstance(raw, str):
        return ()
    return tuple(str(source) for source in raw if source)


def _unavailable_metric_set(doc: Mapping[str, Any]) -> set[str]:
    raw = doc.get("unavailable_measurements") or ()
    if not isinstance(raw, Sequence) or isinstance(raw, str):
        return set()
    metrics: set[str] = set()
    for item in raw:
        if isinstance(item, Mapping) and item.get("metric"):
            metrics.add(str(item["metric"]))
    return metrics


def _first_numeric(source: Mapping[str, Any], aliases: Sequence[str]) -> float | None:
    for alias in aliases:
        value = _number(source.get(alias))
        if value is not None:
            return value
    return None


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _missing_required(field: str) -> dict[str, Any]:
    return {
        "code": "missing_required_field",
        "field": field,
        "message": f"missing required field {field}",
    }


def _sha(value: str, n: int = 16) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:n]


__all__ = [
    "LATENCY_METRICS",
    "LATENCY_PERCENTILES",
    "build_source_sync_benchmark_artifact",
    "is_source_sync_benchmark",
    "load_source_sync_benchmark",
    "source_sync_benchmark_measurement_rows",
    "validate_source_sync_benchmark",
]
