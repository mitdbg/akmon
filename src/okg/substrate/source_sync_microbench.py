"""Source-sync microbenchmark inventory contracts."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from glob import glob
from pathlib import Path
from typing import Any

from okg.substrate.source_sync import SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES
from okg.substrate.source_sync_convergence import READY_SOURCES
from okg.substrate.source_sync_optimization_ledger import (
    HOT_WAVE_SOURCE_ORDER as _HOT_WAVE_SOURCE_ORDER,
    MICROBENCH_OPTIMIZATION_LEDGER_ROWS_SCHEMA,
    MICROBENCH_OPTIMIZATION_LEDGER_SCHEMA,
    SUBSECOND_LATENCY_BUDGET_MS,
    build_source_optimization_ledger,
    rank_source_optimization_rows as _rank_source_optimization_rows,
    source_optimization_ledger_relation_rows,
)
from okg.substrate.source_sync_status import (
    source_sync_producer_expectations,
)
from okg.substrate.sources.registry import SourceEntry


# Preserve the legacy facade names without defining a second policy owner.
globals()["HOT_WAVE_SOURCE_ORDER"] = _HOT_WAVE_SOURCE_ORDER
globals()["rank_source_optimization_rows"] = _rank_source_optimization_rows

MICROBENCH_INVENTORY_SCHEMA = "okg.source_sync.microbench.inventory.v1"
MICROBENCH_VIEW_ROWS_SCHEMA = "okg.source_sync.microbench.inventory.rows.v1"
MICROBENCH_RUN_SCHEMA = "okg.source_sync.microbench.run.v1"
MICROBENCH_FULL_PIPELINE_PROGRESS_SCHEMA = (
    "okg.source_sync.microbench.full_pipeline.last_run.v1"
)
MICROBENCH_FULL_PIPELINE_PROGRESS_KEY = "sync.microbench.full_pipeline.last_run"
MICROBENCH_LEDGER_VIEW_ID = "okg_workspace.source_optimization_ledger"
MICROBENCH_LEDGER_VIEW_DEFINITION_ID = f"view:{MICROBENCH_LEDGER_VIEW_ID}"
MICROBENCH_LEDGER_RELATION_DEFINITION_ID = (
    f"view-relation:{MICROBENCH_LEDGER_VIEW_ID}:relation"
)
MICROBENCH_LEDGER_DIRTY_REASON = "source_progress_updated"
MICROBENCH_LEDGER_DIRTY_DEPENDENCY_KIND = "source_progress"
SOURCE_ONLY_SCENARIO = "source-only"
FULL_PIPELINE_SCENARIO = "full-pipeline"
PUBLISH_LOCK_CONTENTION_THRESHOLD_MS = 250.0
MAX_WORK_AMPLIFICATION_RATIO = 3.0
EXPECTED_DELTA_SOURCE_EXPLICIT = "explicit"
EXPECTED_DELTA_SOURCE_HARNESS_DECLARED = "harness_declared"
EXPECTED_DELTA_SOURCE_MISSING = "missing_or_empty"
EXPECTED_DELTA_ACCEPTED_SOURCES = frozenset({
    EXPECTED_DELTA_SOURCE_EXPLICIT,
    EXPECTED_DELTA_SOURCE_HARNESS_DECLARED,
})

MICROBENCH_SCENARIOS = ("no-change", "single-delta")
MICROBENCH_PIPELINE_MODES = (SOURCE_ONLY_SCENARIO, FULL_PIPELINE_SCENARIO)
MICROBENCH_SOURCE_SETS = frozenset(
    (
        "all",
        "automatic",
        "hot-wave",
        "policy-wave",
        "readiness",
        "supporting-wave",
    )
)
# Sources excluded from full-pipeline mode under `--sources all` because a
# publish over their whole scope is expensive or externally coupled. Pass
# `--allow-quarantined-full-pipeline` to run them anyway.
#
# The criterion for membership is full-pipeline publish cost, not whether a
# source has a cheap probe: agent-sessions and github-issues are poll-enrolled
# with their own probes and stay here.
#
# doc-corpus was removed from this set (#1325). It is roughly 64% of a sync
# pass, so leaving it quarantined meant the `no-change` scenario -- the one
# this benchmark exists to measure -- never measured the source the
# optimization is for, and in that scenario its full-pipeline cost is the
# adapter's bounded content-hash probe walk, not a publish. The un-quarantine
# is scenario-blind, so the full-pipeline `single-delta` arm is un-skipped
# too; that arm is bounded to one sampled path scope key, and no CI lane runs
# this benchmark.
FULL_PIPELINE_ALL_SOURCE_QUARANTINE = frozenset(
    (
        "agent-sessions",
        "codex-sessions",
        "code-data-flow",
        "compute-env-remote",
        "coordination-native",
        "coordination-packets",
        "github-issues",
    )
)


def build_microbenchmark_inventory(
    *,
    deployment: str,
    registry: Mapping[str, SourceEntry],
    deployment_dir: Path | str | None = None,
    runtime_source_sync: Mapping[str, Any] | None = None,
    runtime_enabled: bool = False,
    dsn: str | None = None,
    include_live: bool = True,
    enrichers: Sequence[Mapping[str, Any]] | None = None,
    max_producer_age_seconds: int = 15 * 60,
    source_names: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Build the source/enricher coverage matrix for sync microbenchmarks.

    Registry and deployment config are authoritative for coverage shape. Live
    Postgres state is optional and best-effort; callers without a DSN still get
    a complete benchmark matrix that can be unit-tested.
    """

    runtime_source_sync = (
        runtime_source_sync if isinstance(runtime_source_sync, Mapping) else {}
    )
    source_filter = (
        frozenset(str(name) for name in source_names if str(name))
        if source_names is not None
        else None
    )
    filtered_registry = {
        name: entry
        for name, entry in registry.items()
        if source_filter is None or name in source_filter
    }
    live_state = (
        _load_live_state(dsn, deployment=deployment, source_names=source_filter)
        if include_live and dsn
        else _empty_live_state("not_loaded")
    )
    expected, disabled, config_warnings = source_sync_producer_expectations(
        filtered_registry,
        max_producer_age_seconds=max_producer_age_seconds,
        runtime_source_sync=runtime_source_sync,
    )
    producers_by_source = _group_producers(expected)
    disabled_by_source = _group_producers(disabled)
    rows = [
        _source_row(
            deployment=deployment,
            source_name=name,
            entry=entry,
            runtime_enabled=runtime_enabled,
            runtime_source_sync=runtime_source_sync,
            expected_producers=producers_by_source.get(name, ()),
            disabled_producers=disabled_by_source.get(name, ()),
            live_state=live_state,
        )
        for name, entry in sorted(filtered_registry.items())
    ]
    enricher_rows = [
        _enricher_row(deployment=deployment, index=index, raw=raw)
        for index, raw in enumerate(enrichers or (), start=1)
        if isinstance(raw, Mapping)
    ]
    coverage_counts: dict[str, int] = {}
    for row in rows:
        key = str(row["coverage_class"])
        coverage_counts[key] = coverage_counts.get(key, 0) + 1
    return {
        "schema": MICROBENCH_INVENTORY_SCHEMA,
        "deployment": deployment,
        "deployment_dir": str(deployment_dir or ""),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runtime_enabled": bool(runtime_enabled),
        "source_count": len(rows),
        "enricher_count": len(enricher_rows),
        "automatic_hot_path_count": coverage_counts.get(
            "automatic_hot_path",
            0,
        ),
        "coverage_counts": coverage_counts,
        "live_state": {
            "status": live_state.get("status"),
            "error": live_state.get("error"),
            "source_progress_loaded": bool(live_state.get("progress")),
            "source_watermarks_loaded": bool(live_state.get("watermarks")),
        },
        "producer_config_warnings": list(config_warnings),
        "sources": rows,
        "enrichers": enricher_rows,
    }


def microbenchmark_inventory_relation_rows(
    inventory: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Flatten an inventory report into relation rows for native views."""

    rows: list[dict[str, Any]] = []
    for item in inventory.get("sources") or ():
        if not isinstance(item, Mapping):
            continue
        rows.append(
            {
                "row_key": f"source:{item.get('source_name')}",
                "row_kind": "source",
                "name": item.get("source_name"),
                "owner": item.get("owner"),
                "coverage_class": item.get("coverage_class"),
                "readiness_required": item.get("readiness_required"),
                "trigger_kinds": ",".join(item.get("trigger_kinds") or ()),
                "default_event_mode": item.get("default_event_mode"),
                "reconcile_mode": item.get("reconcile_mode"),
                "scope_accepts": ",".join(item.get("scope_accepts") or ()),
                "cursor_support_status": item.get("cursor_support_status"),
                "scope_support_status": item.get("scope_support_status"),
                "producer_status": item.get("producer_status"),
                "expected_producer_count": item.get("expected_producer_count"),
                "disabled_producer_count": item.get("disabled_producer_count"),
                "benchmark_scenarios": ",".join(
                    item.get("benchmark_scenarios") or (),
                ),
                "latest_source_runner_ms": item.get("latest_source_runner_ms"),
                "latest_stage_total_ms": item.get("latest_stage_total_ms"),
                "latest_stage_admission_prepare_ms": item.get(
                    "latest_stage_admission_prepare_ms",
                ),
                "latest_stage_import_ms": item.get("latest_stage_import_ms"),
                "latest_stage_construct_ms": item.get(
                    "latest_stage_construct_ms",
                ),
                "latest_stage_source_runner_ms": item.get(
                    "latest_stage_source_runner_ms",
                ),
                "latest_stage_source_state_snapshot_ms": item.get(
                    "latest_stage_source_state_snapshot_ms",
                ),
                "latest_stage_admission_finalize_ms": item.get(
                    "latest_stage_admission_finalize_ms",
                ),
                "latest_full_pipeline_scenario": item.get(
                    "latest_full_pipeline_scenario",
                ),
                "latest_full_pipeline_ok": item.get("latest_full_pipeline_ok"),
                "latest_full_pipeline_latency_p95_ms": item.get(
                    "latest_full_pipeline_latency_p95_ms",
                ),
                "latest_full_pipeline_rows_appended": item.get(
                    "latest_full_pipeline_rows_appended",
                ),
                "latest_full_pipeline_changed_graph_rows": item.get(
                    "latest_full_pipeline_changed_graph_rows",
                ),
                "latest_full_pipeline_expected_delta": item.get(
                    "latest_full_pipeline_expected_delta",
                ),
                "latest_full_pipeline_expected_delta_source": item.get(
                    "latest_full_pipeline_expected_delta_source",
                ),
                "latest_full_pipeline_expected_delta_units": item.get(
                    "latest_full_pipeline_expected_delta_units",
                ),
                "latest_full_pipeline_observed_delta_units": item.get(
                    "latest_full_pipeline_observed_delta_units",
                ),
                "latest_full_pipeline_source_work_units": item.get(
                    "latest_full_pipeline_source_work_units",
                ),
                "latest_full_pipeline_work_amplification_ratio": item.get(
                    "latest_full_pipeline_work_amplification_ratio",
                ),
                "latest_full_pipeline_work_amplification_budget": item.get(
                    "latest_full_pipeline_work_amplification_budget",
                ),
                "latest_full_pipeline_observability_status": item.get(
                    "latest_full_pipeline_observability_status",
                ),
                "latest_full_pipeline_observability_gaps": item.get(
                    "latest_full_pipeline_observability_gaps",
                ),
                "latest_full_pipeline_no_change_dominant": item.get(
                    "latest_full_pipeline_no_change_dominant",
                ),
                "latest_full_pipeline_top_widening_stage": item.get(
                    "latest_full_pipeline_top_widening_stage",
                ),
                "latest_full_pipeline_top_widening_owner": item.get(
                    "latest_full_pipeline_top_widening_owner",
                ),
                "latest_full_pipeline_top_widening_reason": item.get(
                    "latest_full_pipeline_top_widening_reason",
                ),
                "latest_full_pipeline_top_widening_ms": item.get(
                    "latest_full_pipeline_top_widening_ms",
                ),
                "latest_full_pipeline_lock_wait_p95_ms": item.get(
                    "latest_full_pipeline_lock_wait_p95_ms",
                ),
                "latest_full_pipeline_lock_hold_p95_ms": item.get(
                    "latest_full_pipeline_lock_hold_p95_ms",
                ),
                "latest_full_pipeline_environment_contention": item.get(
                    "latest_full_pipeline_environment_contention",
                ),
                "latest_full_pipeline_environment_stage": item.get(
                    "latest_full_pipeline_environment_stage",
                ),
                "latest_full_pipeline_environment_reason": item.get(
                    "latest_full_pipeline_environment_reason",
                ),
                "latest_full_pipeline_readiness_impact": item.get(
                    "latest_full_pipeline_readiness_impact",
                ),
                "latest_full_pipeline_artifact": item.get(
                    "latest_full_pipeline_artifact",
                ),
                "latest_full_pipeline_generated_at": item.get(
                    "latest_full_pipeline_generated_at",
                ),
                "latest_source_files_seen": item.get(
                    "latest_source_files_seen",
                ),
                "latest_source_files_scanned": item.get(
                    "latest_source_files_scanned",
                ),
                "latest_source_files_scanned_full": item.get(
                    "latest_source_files_scanned_full",
                ),
                "latest_source_files_scanned_append_delta": item.get(
                    "latest_source_files_scanned_append_delta",
                ),
                "latest_source_files_skipped_unchanged": item.get(
                    "latest_source_files_skipped_unchanged",
                ),
                "latest_source_files_skipped_cwd_filter": item.get(
                    "latest_source_files_skipped_cwd_filter",
                ),
                "latest_source_bytes_delta": item.get(
                    "latest_source_bytes_delta",
                ),
                "latest_source_lines_delta": item.get(
                    "latest_source_lines_delta",
                ),
                "node_count": item.get("node_count"),
                "edge_count": item.get("edge_count"),
                "nodes_added_1h": item.get("nodes_added_1h"),
                "edges_added_1h": item.get("edges_added_1h"),
                "nodes_added_24h": item.get("nodes_added_24h"),
                "edges_added_24h": item.get("edges_added_24h"),
                "last_run_observed_at": item.get("last_run_observed_at"),
                "next_action": item.get("next_action"),
                "blocker": item.get("blocker"),
                "source_class": item.get("source_class"),
                "module": item.get("module"),
                "class_name": item.get("class_name"),
            }
        )
    for item in inventory.get("enrichers") or ():
        if not isinstance(item, Mapping):
            continue
        rows.append(
            {
                "row_key": f"enricher:{item.get('name')}",
                "row_kind": "enricher",
                "name": item.get("name"),
                "owner": item.get("owner"),
                "coverage_class": item.get("coverage_class"),
                "readiness_required": False,
                "trigger_kinds": "publish",
                "default_event_mode": None,
                "reconcile_mode": None,
                "scope_accepts": ",".join(item.get("scope_accepts") or ()),
                "cursor_support_status": "not_applicable",
                "scope_support_status": item.get("scope_support_status"),
                "producer_status": "not_applicable",
                "expected_producer_count": 0,
                "disabled_producer_count": 0,
                "benchmark_scenarios": ",".join(
                    item.get("benchmark_scenarios") or (),
                ),
                "latest_source_runner_ms": None,
                "latest_stage_total_ms": None,
                "latest_stage_admission_prepare_ms": None,
                "latest_stage_import_ms": None,
                "latest_stage_construct_ms": None,
                "latest_stage_source_runner_ms": None,
                "latest_stage_source_state_snapshot_ms": None,
                "latest_stage_admission_finalize_ms": None,
                "node_count": None,
                "edge_count": None,
                "nodes_added_1h": None,
                "edges_added_1h": None,
                "nodes_added_24h": None,
                "edges_added_24h": None,
                "last_run_observed_at": None,
                "next_action": item.get("next_action"),
                "blocker": item.get("blocker"),
                "source_class": "enricher",
                "module": item.get("module"),
                "class_name": item.get("class_name"),
            }
        )
    return rows




def build_source_sync_microbenchmark_report(
    *,
    deployment: str,
    deployment_dir: Path | str,
    registry: Mapping[str, SourceEntry],
    dsn: str,
    scenario: str,
    sources: Sequence[str] | str = ("all",),
    repeat: int = 1,
    pipeline_modes: Sequence[str] = MICROBENCH_PIPELINE_MODES,
    run_mode: str = "scope_incomplete",
    include_predecessors: bool = False,
    include_dependents: bool = False,
    dependent_fanout: str | None = None,
    max_source_workers: int = 1,
    scope_key: str | None = None,
    unscoped: bool = False,
    expected_delta: Mapping[str, Any] | None = None,
    output_path: Path | str | None = None,
    allow_quarantined_full_pipeline: bool = False,
    source_registry_repo_root_override: Path | str | None = None,
    run_ingest_fn: Any | None = None,
) -> dict[str, Any]:
    """Run a stage-attributed source-sync microbenchmark.

    This intentionally drives the same ``run_ingest`` path used by the
    worker/CLI instead of a synthetic adapter loop. The report distinguishes
    source-only samples (publish disabled) from full-pipeline samples (publish
    enabled) and records the scoped event used for ``single-delta`` attempts.
    """

    if scenario not in MICROBENCH_SCENARIOS:
        raise ValueError(f"unsupported microbench scenario: {scenario!r}")
    repeat = max(1, int(repeat or 1))
    modes = _normalize_pipeline_modes(pipeline_modes)
    source_selector = _source_selector_payload(sources)
    all_source_selector = _is_all_source_selector(source_selector)
    selected_sources = _resolve_microbench_sources(registry, sources)
    dep_dir = Path(deployment_dir)

    if run_ingest_fn is None:
        from okg.substrate.ingest.service import run_ingest as run_ingest_fn

    rows: list[dict[str, Any]] = []
    samples: list[dict[str, Any]] = []
    declared_expected_delta_by_source: dict[str, dict[str, Any]] = {}
    for source_name in selected_sources:
        entry = registry[source_name]
        row_expected_delta = expected_delta
        row_expected_delta_source = (
            EXPECTED_DELTA_SOURCE_EXPLICIT
            if expected_delta else EXPECTED_DELTA_SOURCE_MISSING
        )
        if not row_expected_delta:
            declared_expected_delta = _declared_microbench_expected_delta(
                entry=entry,
                scenario=scenario,
            )
            if declared_expected_delta:
                row_expected_delta = declared_expected_delta
                row_expected_delta_source = EXPECTED_DELTA_SOURCE_HARNESS_DECLARED
                declared_expected_delta_by_source[source_name] = (
                    declared_expected_delta
                )
        event_plan = _microbench_event_plan(
            deployment=deployment,
            source_name=source_name,
            entry=entry,
            deployment_dir=dep_dir,
            dsn=dsn,
            scenario=scenario,
            scope_key=scope_key,
            unscoped=unscoped,
        )
        for pipeline_mode in modes:
            missing_scope_sample = scenario == "single-delta" and event_plan.get(
                "scope_status"
            ) in {
                "missing_path_scope_sample",
                "missing_warm_cursor_scope_sample",
            }
            if missing_scope_sample:
                rows.append(
                    _skipped_microbench_row(
                        deployment=deployment,
                        source_name=source_name,
                        scenario=scenario,
                        pipeline_mode=pipeline_mode,
                        run_mode=run_mode,
                        event_plan=event_plan,
                        reason=str(event_plan.get("scope_status")),
                    )
                )
                continue
            if scenario == "no-change" and int(event_plan.get("event_count") or 0) == 0:
                rows.append(
                    _no_event_microbench_row(
                        deployment=deployment,
                        source_name=source_name,
                        scenario=scenario,
                        pipeline_mode=pipeline_mode,
                        run_mode=run_mode,
                        event_plan=event_plan,
                    )
                )
                continue
            if _quarantine_full_pipeline_all_source(
                source_name=source_name,
                pipeline_mode=pipeline_mode,
                all_source_selector=all_source_selector,
                allow_quarantined_full_pipeline=(allow_quarantined_full_pipeline),
            ):
                rows.append(
                    _skipped_microbench_row(
                        deployment=deployment,
                        source_name=source_name,
                        scenario=scenario,
                        pipeline_mode=pipeline_mode,
                        run_mode=run_mode,
                        event_plan=event_plan,
                        reason="quarantined_full_pipeline_all_source",
                    )
                )
                continue
            sample_rows = []
            for iteration in range(1, repeat + 1):
                sample = _run_microbench_sample(
                    deployment=deployment,
                    deployment_dir=dep_dir,
                    dsn=dsn,
                    source_name=source_name,
                    scenario=scenario,
                    pipeline_mode=pipeline_mode,
                    iteration=iteration,
                    repeat=repeat,
                    run_mode=run_mode,
                    include_predecessors=include_predecessors,
                    include_dependents=include_dependents,
                    dependent_fanout=dependent_fanout,
                    max_source_workers=max_source_workers,
                    event_plan=event_plan,
                    run_ingest_fn=run_ingest_fn,
                    source_registry_repo_root_override=(
                        source_registry_repo_root_override
                    ),
                )
                sample_rows.append(sample)
                samples.append(sample)
            rows.append(
                _summarize_microbench_samples(
                    deployment=deployment,
                    source_name=source_name,
                    scenario=scenario,
                    pipeline_mode=pipeline_mode,
                    run_mode=run_mode,
                    event_plan=event_plan,
                    samples=sample_rows,
                    expected_delta=row_expected_delta,
                    expected_delta_source=row_expected_delta_source,
                )
            )

    report = {
        "schema": MICROBENCH_RUN_SCHEMA,
        "deployment": deployment,
        "deployment_dir": str(dep_dir),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scenario": scenario,
        "pipeline_modes": list(modes),
        "source_selector": source_selector,
        "source_count": len(selected_sources),
        "sources": list(selected_sources),
        "repeat": repeat,
        "run_mode": run_mode,
        "include_predecessors": bool(include_predecessors),
        "include_dependents": bool(include_dependents),
        "dependent_fanout": dependent_fanout
        or ("closure" if include_dependents else "none"),
        "max_source_workers": int(max_source_workers or 1),
        "expected_delta": dict(expected_delta or {}),
        "expected_delta_source": (
            EXPECTED_DELTA_SOURCE_EXPLICIT
            if expected_delta else (
                EXPECTED_DELTA_SOURCE_HARNESS_DECLARED
                if declared_expected_delta_by_source
                else EXPECTED_DELTA_SOURCE_MISSING
            )
        ),
        "declared_expected_delta_by_source": declared_expected_delta_by_source,
        "scope_selection": "unscoped" if unscoped else "auto",
        "allow_quarantined_full_pipeline": bool(allow_quarantined_full_pipeline),
        "quarantined_full_pipeline_sources": sorted(
            FULL_PIPELINE_ALL_SOURCE_QUARANTINE,
        ),
        "ok": all(row["ok"] for row in rows),
        "row_count": len(rows),
        "skipped_row_count": sum(1 for row in rows if row.get("skipped")),
        "sample_count": len(samples),
        "rows": rows,
        "samples": samples,
    }
    if output_path is not None:
        report["artifact"] = str(output_path)
    _persist_microbench_full_pipeline_progress(
        dsn=dsn,
        deployment=deployment,
        rows=rows,
        generated_at=report["generated_at"],
        artifact=report.get("artifact"),
    )
    if output_path is not None:
        Path(output_path).write_text(
            json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
    return report


def _normalize_pipeline_modes(values: Sequence[str]) -> tuple[str, ...]:
    raw = [str(value) for value in values or ()]
    if not raw or raw == ["both"]:
        return (SOURCE_ONLY_SCENARIO, FULL_PIPELINE_SCENARIO)
    if len(raw) == 1 and raw[0] == "both":
        return (SOURCE_ONLY_SCENARIO, FULL_PIPELINE_SCENARIO)
    modes: list[str] = []
    for value in raw:
        if value == "both":
            for mode in (SOURCE_ONLY_SCENARIO, FULL_PIPELINE_SCENARIO):
                if mode not in modes:
                    modes.append(mode)
            continue
        if value not in MICROBENCH_PIPELINE_MODES:
            raise ValueError(f"unsupported pipeline mode: {value!r}")
        if value not in modes:
            modes.append(value)
    return tuple(modes)


def _declared_microbench_expected_delta(
    *,
    entry: SourceEntry,
    scenario: str,
) -> dict[str, Any]:
    raw = getattr(entry, "raw", {})
    microbench = raw.get("microbench") if isinstance(raw, Mapping) else None
    if not isinstance(microbench, Mapping):
        return {}
    expected_delta = microbench.get("expected_delta")
    if not isinstance(expected_delta, Mapping):
        return {}

    scenario_delta = expected_delta.get(scenario)
    if isinstance(scenario_delta, Mapping):
        return {
            str(key): value
            for key, value in scenario_delta.items()
            if _as_float(value) is not None
        }

    if all(_as_float(value) is not None for value in expected_delta.values()):
        return {
            str(key): value
            for key, value in expected_delta.items()
            if _as_float(value) is not None
        }
    return {}


def _declared_microbench_expected_delta_is_no_change(
    *,
    entry: SourceEntry,
    scenario: str,
) -> bool:
    delta = _declared_microbench_expected_delta(entry=entry, scenario=scenario)
    return _expected_delta_is_all_zero(delta)


def _expected_delta_is_all_zero(delta: Mapping[str, Any] | None) -> bool:
    if not delta:
        return False
    units = [_as_float(value) for value in delta.values()]
    return bool(units) and all(value == 0.0 for value in units)


def _resolve_microbench_sources(
    registry: Mapping[str, SourceEntry],
    sources: Sequence[str] | str,
) -> tuple[str, ...]:
    if isinstance(sources, str):
        raw_values = [item.strip() for item in sources.split(",")]
    else:
        raw_values = []
        for source in sources:
            raw_values.extend(str(source).split(","))
        raw_values = [item.strip() for item in raw_values]
    requested = tuple(item for item in raw_values if item)
    if not requested or requested == ("all",):
        return tuple(_registry_source_names(registry))
    by_owner = {
        entry.ownership_id: name
        for name, entry in registry.items()
        if entry.ownership_id
    }
    out: list[str] = []
    missing: list[str] = []
    for raw in requested:
        if raw in MICROBENCH_SOURCE_SETS:
            for name in _registry_source_names(registry, source_set=raw):
                if name not in out:
                    out.append(name)
            continue
        name = raw if raw in registry else by_owner.get(raw)
        if name is None:
            missing.append(raw)
            continue
        entry = registry[name]
        if entry.legacy or entry.is_registry_only:
            continue
        if name not in out:
            out.append(name)
    if missing:
        known_sets = ", ".join(sorted(MICROBENCH_SOURCE_SETS))
        raise ValueError(
            f"unknown source(s): {', '.join(missing)}; known source sets: {known_sets}",
        )
    return tuple(out)


def _registry_source_names(
    registry: Mapping[str, SourceEntry],
    *,
    source_set: str = "all",
) -> tuple[str, ...]:
    names: list[str] = []
    for name, entry in sorted(registry.items()):
        if entry.legacy or entry.is_registry_only:
            continue
        if source_set == "all":
            names.append(name)
            continue
        if source_set == "automatic" and _is_automatic_source(entry):
            names.append(name)
            continue
        if (
            source_set == "readiness"
            and name in READY_SOURCES
            and _is_automatic_source(entry)
        ):
            names.append(name)
            continue
        if (
            source_set == "hot-wave"
            and name in READY_SOURCES
            and _is_automatic_source(entry)
        ):
            names.append(name)
            continue
        if (
            source_set == "supporting-wave"
            and name not in READY_SOURCES
            and _is_automatic_source(entry)
            and not _is_policy_source(entry)
        ):
            names.append(name)
            continue
        if source_set == "policy-wave" and _is_policy_source(entry):
            names.append(name)
            continue
    return tuple(names)


def _is_automatic_source(entry: SourceEntry) -> bool:
    if entry.credential_refs or entry.sync is None:
        return False
    return bool(set(entry.sync.triggers) - {"manual", "reconcile"})


def _is_policy_source(entry: SourceEntry) -> bool:
    if entry.credential_refs:
        return True
    if entry.sync is None:
        return False
    trigger_set = set(entry.sync.triggers)
    return not trigger_set or trigger_set <= {"manual", "reconcile"}


def _source_selector_payload(sources: Sequence[str] | str) -> list[str]:
    if isinstance(sources, str):
        return [item.strip() for item in sources.split(",") if item.strip()]
    out: list[str] = []
    for source in sources:
        out.extend(item.strip() for item in str(source).split(",") if item.strip())
    return out or ["all"]


def _is_all_source_selector(source_selector: Sequence[str]) -> bool:
    return not source_selector or "all" in set(source_selector)


def _quarantine_full_pipeline_all_source(
    *,
    source_name: str,
    pipeline_mode: str,
    all_source_selector: bool,
    allow_quarantined_full_pipeline: bool,
) -> bool:
    return (
        pipeline_mode == FULL_PIPELINE_SCENARIO
        and all_source_selector
        and not allow_quarantined_full_pipeline
        and source_name in FULL_PIPELINE_ALL_SOURCE_QUARANTINE
    )


def _microbench_event_plan(
    *,
    deployment: str,
    source_name: str,
    entry: SourceEntry,
    deployment_dir: Path,
    dsn: str | None,
    scenario: str,
    scope_key: str | None,
    unscoped: bool = False,
) -> dict[str, Any]:
    if unscoped and scope_key:
        raise ValueError("--unscoped cannot be combined with --scope-key")
    sync = entry.sync
    scope_accepts = tuple(getattr(getattr(sync, "scope", None), "accepts", ()) or ())
    selected_scope_key = scope_key
    scope_status = (
        "forced_unscoped"
        if unscoped
        else "provided_scope_key" if scope_key else "unscoped"
    )
    prefer_no_change_scope = (
        scenario == "no-change"
        or _declared_microbench_expected_delta_is_no_change(
            entry=entry,
            scenario=scenario,
        )
    )
    if selected_scope_key is None and not unscoped and "path" in scope_accepts:
        if source_name in {"agent-sessions", "codex-sessions"}:
            selected_scope_key = _sample_append_cursor_warm_scope_key(
                deployment=deployment,
                source_name=source_name,
                entry=entry,
                deployment_dir=deployment_dir,
                dsn=dsn,
                scenario=scenario,
                prefer_no_change=prefer_no_change_scope,
            )
            scope_status = (
                "sampled_warm_cursor_scope_key"
                if selected_scope_key
                else "missing_warm_cursor_scope_sample"
            )
        else:
            selected_scope_key = _sample_path_scope_key(
                entry,
                deployment_dir=deployment_dir,
                prefer_no_change=prefer_no_change_scope,
            )
            scope_status = (
                "sampled_path_scope_key"
                if selected_scope_key
                else "missing_path_scope_sample"
            )
    revision_hint = None
    scope_kind = "path" if selected_scope_key else "none"
    if (
        selected_scope_key is None
        and "revision_hint" not in scope_accepts
        and scope_status
        in {
            "missing_path_scope_sample",
            "missing_warm_cursor_scope_sample",
        }
    ):
        return {
            "scenario": scenario,
            "event_count": 0,
            "scope_kind": "none",
            "scope_key": None,
            "revision_hint": None,
            "scope_status": scope_status,
            "events": [],
        }
    if selected_scope_key is None and not unscoped and "revision_hint" in scope_accepts:
        revision_hint = f"microbench:{deployment}:{source_name}"
        scope_kind = "revision_hint"
        scope_status = "synthetic_revision_hint"
    event = _source_sync_event(
        deployment=deployment,
        source_name=source_name,
        trigger_kind="fs_event" if selected_scope_key else "manual",
        scope_key=selected_scope_key,
        revision_hint=revision_hint,
        reason=f"microbench:{scenario}",
    )
    return {
        "scenario": scenario,
        "event_count": 1,
        "scope_kind": scope_kind,
        "scope_key": selected_scope_key,
        "revision_hint": revision_hint,
        "scope_status": scope_status,
        "events": [event.to_dict()],
    }


def _source_sync_event(**kwargs: Any) -> Any:
    from okg.substrate.source_sync import SourceSyncEvent

    return SourceSyncEvent(**kwargs)


_APPEND_CURSOR_SCHEMAS_BY_SOURCE = {
    "agent-sessions": ("okg.agent_sessions.append_cursor.v1",),
    "codex-sessions": ("okg.codex_sessions.append_cursor.v1",),
}


def _sample_append_cursor_warm_scope_key(
    *,
    deployment: str,
    source_name: str,
    entry: SourceEntry,
    deployment_dir: Path,
    dsn: str | None,
    scenario: str,
    prefer_no_change: bool = False,
) -> str | None:
    if not dsn:
        return None
    cursor = _load_source_watermark_cursor(
        dsn,
        _source_watermark_names(
            deployment=deployment,
            source_name=source_name,
            entry=entry,
        ),
    )
    if not isinstance(cursor, Mapping):
        return None
    sync = entry.sync
    watch = getattr(sync, "watch", None) if sync is not None else None
    roots = tuple(getattr(watch, "roots", ()) or ())
    include_globs = tuple(getattr(watch, "include_globs", ()) or ("**/*",))
    exclude_globs = tuple(getattr(watch, "exclude_globs", ()) or ())
    candidates: list[tuple[int, int, str]] = []
    accepted_schemas = _APPEND_CURSOR_SCHEMAS_BY_SOURCE.get(
        source_name,
        ("okg.agent_sessions.append_cursor.v1",),
    )
    for key, raw_state in _iter_append_cursor_states(cursor):
        append_cursor = raw_state.get("append_cursor")
        if not isinstance(append_cursor, Mapping):
            continue
        if append_cursor.get("schema") not in accepted_schemas:
            continue
        if not append_cursor.get("append_safe"):
            continue
        if not append_cursor.get("last_line_hash"):
            continue
        try:
            cursor_bytes = int(append_cursor.get("bytes") or 0)
        except (TypeError, ValueError):
            continue
        if cursor_bytes <= 0:
            continue
        if not _matches_any_glob(key, include_globs):
            continue
        if _matches_any_glob(key, exclude_globs):
            continue
        for raw_root in roots:
            root = _resolve_scope_root(raw_root, deployment_dir=deployment_dir)
            if root is None:
                continue
            path = root / key
            try:
                stat = path.stat()
            except OSError:
                continue
            delta = int(stat.st_size) - cursor_bytes
            if prefer_no_change:
                if delta != 0:
                    continue
                try:
                    cursor_mtime_ns = int(append_cursor.get("mtime_ns"))
                except (TypeError, ValueError):
                    continue
                if cursor_mtime_ns != int(stat.st_mtime_ns):
                    continue
                candidates.append(
                    (
                        0,
                        0
                        if _append_cursor_scope_matches_cwd_filter(
                            key,
                            entry=entry,
                            append_cursor=append_cursor,
                            source_name=source_name,
                        )
                        else 1,
                        key,
                    )
                )
                break
            if delta <= 0:
                continue
            candidates.append((delta, 0, key))
            break
    if not candidates:
        return None
    candidates.sort()
    return candidates[0][2]


def _iter_append_cursor_states(
    cursor: Mapping[str, Any],
) -> Iterable[tuple[str, Mapping[str, Any]]]:
    skip_keys = {
        "change_probe_skips",
        "change_probe_token",
        "cursor_blob_summary",
        "record_set",
    }
    for key, raw_state in cursor.items():
        if not isinstance(key, str) or key in skip_keys:
            continue
        if key.startswith("record_set"):
            continue
        if not isinstance(raw_state, Mapping):
            continue
        if isinstance(raw_state.get("append_cursor"), Mapping):
            yield key, raw_state
            continue
        for nested_key, nested_state in raw_state.items():
            if not isinstance(nested_key, str):
                continue
            if not isinstance(nested_state, Mapping):
                continue
            if isinstance(nested_state.get("append_cursor"), Mapping):
                yield nested_key, nested_state


def _append_cursor_scope_matches_cwd_filter(
    scope_key: str,
    *,
    entry: SourceEntry,
    append_cursor: Mapping[str, Any],
    source_name: str,
) -> bool:
    raw = entry.raw if isinstance(getattr(entry, "raw", None), Mapping) else {}
    params = raw.get("params") if isinstance(raw.get("params"), Mapping) else {}
    if source_name == "codex-sessions":
        meta = append_cursor.get("meta")
        cwd = str(meta.get("cwd") or "") if isinstance(meta, Mapping) else ""
        if not cwd:
            return False
        return _path_matches_any_cwd_filter(cwd, params)
    for raw_filter in _param_sequence(params.get("cwd_filter")):
        try:
            project_key = _claude_project_key(raw_filter)
        except (TypeError, ValueError):
            continue
        if scope_key == project_key or scope_key.startswith(f"{project_key}/"):
            return True
    return False


def _path_matches_any_cwd_filter(path: Any, params: Mapping[str, Any]) -> bool:
    try:
        candidate = Path(str(path)).expanduser().resolve(strict=False)
    except (TypeError, ValueError):
        return False
    for raw_filter in _param_sequence(params.get("cwd_filter")):
        try:
            filter_path = Path(str(raw_filter)).expanduser().resolve(strict=False)
        except (TypeError, ValueError):
            continue
        if candidate == filter_path or filter_path in candidate.parents:
            return True
    return False


def _claude_project_key(path: Any) -> str:
    expanded = Path(str(path)).expanduser()
    absolute = expanded if expanded.is_absolute() else expanded.resolve(strict=False)
    return absolute.as_posix().replace("/", "-").replace("\\", "-")


def _source_watermark_names(
    *,
    deployment: str,
    source_name: str,
    entry: SourceEntry,
) -> tuple[str, ...]:
    out: list[str] = []
    for candidate in (
        entry.ownership_id,
        f"{deployment}.{source_name}",
        source_name,
    ):
        if candidate and candidate not in out:
            out.append(candidate)
    return tuple(out)


def _load_source_watermark_cursor(
    dsn: str,
    source_names: Sequence[str],
) -> Mapping[str, Any] | None:
    if not source_names:
        return None
    try:
        from okg.substrate.db.connection import connect as db_connect

        with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT last_applied_rev
                  FROM okg.source_watermarks
                 WHERE source = ANY(%s)
                 ORDER BY array_position(%s, source)
                 LIMIT 1
                """,
                (list(source_names), list(source_names)),
            )
            row = cur.fetchone()
    except Exception:
        return None
    if row is None:
        return None
    cursor = row[0]
    if not isinstance(cursor, Mapping):
        return None
    return _hydrate_source_cursor_blob_refs(dsn, cursor)


def _hydrate_source_cursor_blob_refs(
    dsn: str,
    cursor: Mapping[str, Any],
) -> Mapping[str, Any]:
    refs = {
        key: value
        for key, value in cursor.items()
        if _is_source_cursor_blob_ref(value)
    }
    if not refs:
        return cursor
    hydrated = dict(cursor)
    try:
        from okg.substrate.db.connection import connect as db_connect

        with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            for key, ref in refs.items():
                cur.execute(
                    """
                    SELECT value
                      FROM okg.source_cursor_blobs
                     WHERE source = %s
                       AND cursor_key = %s
                       AND sha256 = %s
                     LIMIT 1
                    """,
                    (
                        str(ref.get("source") or ""),
                        str(ref.get("key") or ""),
                        str(ref.get("sha256") or ""),
                    ),
                )
                row = cur.fetchone()
                if row is not None and isinstance(row[0], Mapping):
                    hydrated[key] = row[0]
    except Exception:
        return cursor
    return hydrated


def _is_source_cursor_blob_ref(value: Any) -> bool:
    return (
        isinstance(value, Mapping)
        and value.get("schema") == "okg.source_runner.cursor_blob_ref.v1"
        and value.get("storage") == "okg.source_cursor_blobs"
        and value.get("source")
        and value.get("key")
        and value.get("sha256")
    )


def _sample_path_scope_key(
    entry: SourceEntry,
    *,
    deployment_dir: Path,
    prefer_no_change: bool = False,
    max_paths_examined: int = 20_000,
) -> str | None:
    if prefer_no_change:
        tracked = _sample_git_tracked_path_scope_key(
            entry,
            deployment_dir=deployment_dir,
            max_paths_examined=max_paths_examined,
        )
        if tracked:
            return tracked
        return None
    sync = entry.sync
    watch = getattr(sync, "watch", None) if sync is not None else None
    roots = tuple(getattr(watch, "roots", ()) or ())
    include_globs = tuple(getattr(watch, "include_globs", ()) or ("**/*",))
    exclude_globs = tuple(getattr(watch, "exclude_globs", ()) or ())
    for raw_root in roots:
        root = _resolve_scope_root(raw_root, deployment_dir=deployment_dir)
        if root is None or not root.is_dir():
            continue
        examined = 0
        for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
            examined += 1
            if examined > max_paths_examined:
                break
            if not path.is_file():
                continue
            try:
                rel = path.relative_to(root).as_posix()
            except ValueError:
                continue
            if not _matches_any_glob(rel, include_globs):
                continue
            if _matches_any_glob(rel, exclude_globs):
                continue
            return rel
    return _sample_configured_path_scope_key(
        entry,
        deployment_dir=deployment_dir,
        max_paths_examined=max_paths_examined,
    )


def _sample_git_tracked_path_scope_key(
    entry: SourceEntry,
    *,
    deployment_dir: Path,
    max_paths_examined: int,
) -> str | None:
    raw = entry.raw if isinstance(getattr(entry, "raw", None), Mapping) else {}
    params = raw.get("params") if isinstance(raw.get("params"), Mapping) else {}
    try:
        from okg.substrate.library.sources._git_helpers import run_git
    except Exception:
        return None

    sync = entry.sync
    watch = getattr(sync, "watch", None) if sync is not None else None
    raw_roots = tuple(getattr(watch, "roots", ()) or ())
    repo_root = _microbench_git_repo_root(
        params,
        raw_roots=raw_roots,
        deployment_dir=deployment_dir,
        run_git=run_git,
    )
    if repo_root is None or not repo_root.is_dir():
        return None

    try:
        output = run_git(repo_root, ["ls-files", "--cached"])
    except Exception:  # noqa: BLE001 - no-change proof requires tracked input.
        return None
    raw_roots = raw_roots or (str(repo_root),)
    include_globs = tuple(getattr(watch, "include_globs", ()) or ("**/*",))
    exclude_globs = tuple(getattr(watch, "exclude_globs", ()) or ())
    examined = 0
    for rel in sorted(line.strip() for line in output.splitlines() if line.strip()):
        examined += 1
        if examined > max_paths_examined:
            break
        candidate = repo_root / rel
        if not candidate.is_file():
            continue
        for raw_root in raw_roots:
            root = _resolve_scope_root(raw_root, deployment_dir=deployment_dir)
            if root is None:
                continue
            try:
                scope_key = candidate.relative_to(root).as_posix()
            except ValueError:
                continue
            if not _matches_any_glob(scope_key, include_globs):
                continue
            if _matches_any_glob(scope_key, exclude_globs):
                continue
            return scope_key
    return None


def _microbench_git_repo_root(
    params: Mapping[str, Any],
    *,
    raw_roots: Sequence[Any],
    deployment_dir: Path,
    run_git: Any,
) -> Path | None:
    if params.get("repo_root") is not None:
        repo_root = _configured_repo_root(params, deployment_dir=deployment_dir)
        return repo_root if repo_root.is_dir() else None
    for raw_root in raw_roots:
        root = _resolve_scope_root(raw_root, deployment_dir=deployment_dir)
        if root is None or not root.exists():
            continue
        try:
            output = run_git(root, ["rev-parse", "--show-toplevel"])
        except Exception:
            continue
        top = Path(str(output).strip()).expanduser()
        if top.is_dir():
            return top.resolve()
    return None


def _sample_configured_path_scope_key(
    entry: SourceEntry,
    *,
    deployment_dir: Path,
    max_paths_examined: int,
) -> str | None:
    raw = entry.raw if isinstance(entry.raw, Mapping) else {}
    params = raw.get("params") if isinstance(raw.get("params"), Mapping) else {}
    if not params:
        return None
    candidates = _configured_path_candidates(
        params,
        deployment_dir=deployment_dir,
        max_paths_examined=max_paths_examined,
    )
    if not candidates:
        return None
    repo_root = _configured_repo_root(params, deployment_dir=deployment_dir)
    roots = _configured_scope_roots(
        params,
        deployment_dir=deployment_dir,
        repo_root=repo_root,
    )
    for path in sorted(candidates, key=lambda item: item.as_posix()):
        for root in roots:
            try:
                rel = path.relative_to(root).as_posix()
            except ValueError:
                continue
            if rel and rel != ".":
                return rel
    return candidates[0].as_posix()


def _configured_path_candidates(
    params: Mapping[str, Any],
    *,
    deployment_dir: Path,
    max_paths_examined: int,
) -> list[Path]:
    repo_root = _configured_repo_root(params, deployment_dir=deployment_dir)
    out: list[Path] = []
    for key in (
        "artifact_paths",
        "benchmark_paths",
        "packet_paths",
        "report_paths",
    ):
        for raw in _param_sequence(params.get(key)):
            path = _resolve_configured_path(
                raw,
                deployment_dir=deployment_dir,
                repo_root=repo_root,
            )
            if path.is_file():
                out.append(path)
    for key, suffixes in (
        ("packet_dirs", (".json", ".jsonl")),
        ("report_dirs", (".json",)),
    ):
        for raw in _param_sequence(params.get(key)):
            directory = _resolve_configured_path(
                raw,
                deployment_dir=deployment_dir,
                repo_root=repo_root,
            )
            if not directory.is_dir():
                continue
            examined = 0
            for path in sorted(
                directory.iterdir(),
                key=lambda item: item.as_posix(),
            ):
                examined += 1
                if examined > max_paths_examined:
                    break
                if path.is_file() and path.suffix.lower() in suffixes:
                    out.append(path.resolve())
    for key in (
        "artifact_globs",
        "benchmark_globs",
        "packet_globs",
        "report_globs",
    ):
        for pattern in _param_sequence(params.get(key)):
            examined = 0
            for path in _configured_glob_paths(
                pattern,
                deployment_dir=deployment_dir,
                repo_root=repo_root,
            ):
                examined += 1
                if examined > max_paths_examined:
                    break
                if path.is_file():
                    out.append(path.resolve())
    deduped: dict[str, Path] = {}
    for path in out:
        deduped[path.resolve().as_posix()] = path.resolve()
    return list(deduped.values())


def _configured_scope_roots(
    params: Mapping[str, Any],
    *,
    deployment_dir: Path,
    repo_root: Path | None = None,
) -> tuple[Path, ...]:
    repo_root = repo_root or _configured_repo_root(
        params,
        deployment_dir=deployment_dir,
    )
    roots: list[Path] = []
    for key in (
        "artifact_paths",
        "benchmark_paths",
        "packet_paths",
        "report_paths",
    ):
        for raw in _param_sequence(params.get(key)):
            path = _resolve_configured_path(
                raw,
                deployment_dir=deployment_dir,
                repo_root=repo_root,
            )
            roots.append((path if path.is_dir() else path.parent).resolve())
    for key in ("packet_dirs", "report_dirs"):
        for raw in _param_sequence(params.get(key)):
            roots.append(
                _resolve_configured_path(
                    raw,
                    deployment_dir=deployment_dir,
                    repo_root=repo_root,
                ).resolve(),
            )
    roots.append(repo_root)
    deduped: dict[str, Path] = {}
    for root in roots:
        deduped[root.as_posix()] = root
    return tuple(deduped.values())


def _param_sequence(value: Any) -> tuple[Any, ...]:
    if value is None:
        return ()
    if isinstance(value, (str, Path)):
        return (value,)
    if isinstance(value, Sequence):
        return tuple(value)
    return ()


def _configured_repo_root(
    params: Mapping[str, Any],
    *,
    deployment_dir: Path,
) -> Path:
    fallback = (
        deployment_dir.parent.parent
        if deployment_dir.parent.name == "deployments"
        else Path.cwd()
    ).resolve()
    raw = params.get("repo_root")
    if raw is None:
        return fallback
    text = str(raw)
    text = text.replace("${deployment_dir}", str(deployment_dir))
    text = text.replace("${repo_root}", str(fallback))
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = fallback / path
    return path.resolve()


def _resolve_configured_path(
    raw: Any,
    *,
    deployment_dir: Path,
    repo_root: Path,
) -> Path:
    text = str(raw)
    text = text.replace("${deployment_dir}", str(deployment_dir))
    text = text.replace("${repo_root}", str(repo_root))
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = repo_root / path
    return path.resolve()


def _configured_glob_paths(
    raw_pattern: Any,
    *,
    deployment_dir: Path,
    repo_root: Path,
) -> tuple[Path, ...]:
    text = str(raw_pattern)
    text = text.replace("${deployment_dir}", str(deployment_dir))
    text = text.replace("${repo_root}", str(repo_root))
    pattern = Path(text).expanduser()
    if not pattern.is_absolute():
        pattern = repo_root / pattern
    return tuple(
        sorted(
            (Path(item).resolve() for item in glob(str(pattern))),
            key=lambda item: item.as_posix(),
        ),
    )


def _resolve_scope_root(raw_root: Any, *, deployment_dir: Path) -> Path | None:
    if raw_root is None:
        return None
    text = str(raw_root)
    if not text:
        return None
    text = text.replace("${deployment_dir}", str(deployment_dir))
    text = text.replace("${repo_root}", str(Path.cwd()))
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = deployment_dir / path
    return path.resolve()


def _matches_any_glob(path: str, patterns: Sequence[str]) -> bool:
    if not patterns:
        return False
    from okg.substrate.library.sources._path_scope import glob_matches_path

    return any(glob_matches_path(path, str(pattern)) for pattern in patterns)


def _run_microbench_sample(
    *,
    deployment: str,
    deployment_dir: Path,
    dsn: str,
    source_name: str,
    scenario: str,
    pipeline_mode: str,
    iteration: int,
    repeat: int,
    run_mode: str,
    include_predecessors: bool,
    include_dependents: bool,
    dependent_fanout: str | None,
    max_source_workers: int,
    event_plan: Mapping[str, Any],
    run_ingest_fn: Any,
    source_registry_repo_root_override: Path | str | None = None,
) -> dict[str, Any]:
    from okg.substrate.ingest.service import IngestOptions
    from okg.substrate.source_sync import (
        SourceSyncEvent,
        source_sync_events_by_source,
    )

    events = tuple(
        SourceSyncEvent.from_dict(raw)
        for raw in (event_plan.get("events") or ())
        if isinstance(raw, Mapping)
    )
    publish = pipeline_mode == FULL_PIPELINE_SCENARIO
    started = time.perf_counter()
    error = None
    outcome = None
    try:
        outcome = run_ingest_fn(
            IngestOptions(
                dsn=dsn,
                deployment=deployment,
                deployment_dir=deployment_dir,
                include={source_name},
                include_predecessors=include_predecessors,
                include_dependents=include_dependents,
                dependent_fanout=dependent_fanout
                or ("closure" if include_dependents else "none"),
                source_events=source_sync_events_by_source(events),
                run_mode=run_mode,
                publish=publish,
                owner_id=f"microbench:{deployment}:{source_name}",
                owner_meta={
                    "schema": MICROBENCH_RUN_SCHEMA,
                    "scenario": scenario,
                    "pipeline_mode": pipeline_mode,
                    "iteration": iteration,
                    "repeat": repeat,
                    "event_plan": {
                        key: value
                        for key, value in event_plan.items()
                        if key != "events"
                    },
                },
                max_source_workers=max(1, int(max_source_workers or 1)),
                source_registry_repo_root_override=(
                    source_registry_repo_root_override
                ),
            ),
        )
    except Exception as exc:  # noqa: BLE001 - report, don't hide failures.
        error = f"{type(exc).__name__}: {exc}"
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    report = getattr(outcome, "report", None) if outcome is not None else {}
    if not isinstance(report, Mapping):
        report = {}
    ingest_timings_ms = {
        str(key): value
        for key, value in (report.get("timings_ms") or {}).items()
        if isinstance(value, (int, float))
    }
    source_result = _microbench_source_result(report, source_name)
    publishes = [
        dict(item) for item in report.get("publishes", ()) if isinstance(item, Mapping)
    ]
    graph_read_visibility = (
        _measure_graph_read_visibility(dsn) if publish and publishes else None
    )
    publish_timings_ms = _merged_publish_timings(publishes)
    publish_timing_details = _publish_timing_details(
        publishes,
        graph_read_visibility=graph_read_visibility,
    )
    publish_unattributed_ms = _publish_unattributed_ms(publish_timings_ms)
    publish_failed = any(
        item.get("status") != "published" and not bool(item.get("publish_skipped"))
        for item in publishes
    )
    stage_total_ms = _as_float(
        (source_result.get("timings_ms") or {}).get("total"),
    )
    ingest_overhead_ms = _as_float(
        ingest_timings_ms.get("orchestration_overhead"),
    )
    publish_wall_ms = _as_float(ingest_timings_ms.get("publish_wall"))
    if ingest_overhead_ms is not None:
        pipeline_overhead_ms = max(0.0, ingest_overhead_ms)
    elif stage_total_ms is not None:
        pipeline_overhead_ms = max(
            0.0,
            elapsed_ms - stage_total_ms - (publish_wall_ms or 0.0),
        )
    else:
        pipeline_overhead_ms = None
    ok = (
        error is None
        and outcome is not None
        and not bool(getattr(outcome, "any_failed", False))
        and not publish_failed
        and str(source_result.get("status") or "OK") == "OK"
    )
    return {
        "deployment": deployment,
        "source_name": source_name,
        "scenario": scenario,
        "pipeline_mode": pipeline_mode,
        "iteration": iteration,
        "repeat": repeat,
        "ok": ok,
        "error": error or source_result.get("error"),
        "elapsed_ms": round(elapsed_ms, 3),
        "publish": publish,
        "publish_count": len(publishes),
        "publish_failed": publish_failed,
        "publishes": publishes,
        "publish_timings_ms": publish_timings_ms,
        "publish_timing_details": publish_timing_details,
        "publish_timing_details_flat_ms": (publish_timing_details.get("flat_ms", {})),
        "graph_read_visibility": graph_read_visibility,
        "graph_read_visibility_ms": _round_or_none(
            _as_float(
                (graph_read_visibility or {}).get("elapsed_ms")
                if isinstance(graph_read_visibility, Mapping)
                else None
            ),
        ),
        "publish_unattributed_ms": _round_or_none(publish_unattributed_ms),
        "pipeline_overhead_ms": _round_or_none(pipeline_overhead_ms),
        "ingest_timings_ms": ingest_timings_ms,
        "source_result": source_result,
        "stage_timings_ms": dict(source_result.get("timings_ms") or {}),
        "source_stats": dict(source_result.get("source_stats") or {}),
        "nodes_appended": _as_int(source_result.get("nodes_appended")) or 0,
        "edges_appended": _as_int(source_result.get("edges_appended")) or 0,
        "nodes_retracted": _as_int(source_result.get("retract_n")) or 0,
        "edges_retracted": _as_int(source_result.get("retract_e")) or 0,
        "run_mode": source_result.get("run_mode") or run_mode,
        "sync_scope": source_result.get("sync_scope") or {},
        "event_plan": {
            key: value for key, value in event_plan.items() if key != "events"
        },
    }


def _skipped_microbench_row(
    *,
    deployment: str,
    source_name: str,
    scenario: str,
    pipeline_mode: str,
    run_mode: str,
    event_plan: Mapping[str, Any],
    reason: str,
) -> dict[str, Any]:
    return {
        "deployment": deployment,
        "source_name": source_name,
        "scenario": scenario,
        "pipeline_mode": pipeline_mode,
        "run_mode": run_mode,
        "sample_count": 0,
        "ok": True,
        "skipped": True,
        "skip_reason": reason,
        "error_count": 0,
        "latency_ms": {"min": None, "p50": None, "p95": None, "max": None},
        "source_runner_ms": {"p50": None, "p95": None},
        "pipeline_overhead_ms": {"p50": None, "p95": None},
        "stage_p95_ms": {},
        "publish_stage_p95_ms": {},
        "publish_timing_details_p95_ms": {},
        "graph_read_visibility_ms": {"p50": None, "p95": None},
        "publish_unattributed_ms": {"p50": None, "p95": None},
        "nodes_appended_total": 0,
        "edges_appended_total": 0,
        "rows_appended_total": 0,
        "throughput_rows_per_second": None,
        "event_plan": {
            key: value for key, value in event_plan.items() if key != "events"
        },
    }


def _no_event_microbench_row(
    *,
    deployment: str,
    source_name: str,
    scenario: str,
    pipeline_mode: str,
    run_mode: str,
    event_plan: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "deployment": deployment,
        "source_name": source_name,
        "scenario": scenario,
        "pipeline_mode": pipeline_mode,
        "run_mode": run_mode,
        "sample_count": 0,
        "ok": True,
        "no_event_noop": True,
        "error_count": 0,
        "latency_ms": {"min": 0.0, "p50": 0.0, "p95": 0.0, "max": 0.0},
        "source_runner_ms": {"p50": 0.0, "p95": 0.0},
        "pipeline_overhead_ms": {"p50": 0.0, "p95": 0.0},
        "stage_p95_ms": {
            "admission_finalize": 0.0,
            "admission_prepare": 0.0,
            "construct": 0.0,
            "import": 0.0,
            "source_runner": 0.0,
            "source_state_snapshot": 0.0,
            "total": 0.0,
        },
        "publish_stage_p95_ms": {},
        "publish_timing_details_p95_ms": {},
        "graph_read_visibility_ms": {"p50": None, "p95": None},
        "publish_unattributed_ms": {"p50": 0.0, "p95": 0.0},
        "nodes_appended_total": 0,
        "edges_appended_total": 0,
        "rows_appended_total": 0,
        "throughput_rows_per_second": 0.0,
        "event_plan": {
            key: value for key, value in event_plan.items() if key != "events"
        },
    }


def _microbench_source_result(
    report: Mapping[str, Any],
    source_name: str,
) -> dict[str, Any]:
    for item in report.get("results", ()) or ():
        if not isinstance(item, Mapping):
            continue
        if item.get("name") == source_name:
            return dict(item)
    return {}


def _merged_publish_timings(
    publishes: Sequence[Mapping[str, Any]],
) -> dict[str, float]:
    merged: dict[str, float] = {}
    for publish in publishes:
        timings = publish.get("timings_ms")
        if not isinstance(timings, Mapping):
            continue
        for key, value in timings.items():
            number = _as_float(value)
            if number is None:
                continue
            merged[str(key)] = round(merged.get(str(key), 0.0) + number, 3)
    return merged


def _publish_timing_details(
    publishes: Sequence[Mapping[str, Any]],
    *,
    graph_read_visibility: Mapping[str, Any] | None,
) -> dict[str, Any]:
    top_level_ms = _merged_publish_timings(publishes)
    flat_ms: dict[str, float] = {}
    # Issue #1291: `flat_ms` SUMS each key across every publish in the sample.
    # That is the right shape for cost, and the wrong shape for a per-cycle
    # bound: the publisher lock-hold gate asks "did any single cycle come close
    # to the statement timeout", so the worst single hold is recorded beside
    # the total rather than derived from it.
    max_flat_ms: dict[str, float] = {}
    per_publish: list[dict[str, Any]] = []
    for index, publish in enumerate(publishes, start=1):
        details = publish.get("timing_details")
        if isinstance(details, Mapping):
            detail_payload = dict(details)
            detail_flat = details.get("flat_ms")
            if isinstance(detail_flat, Mapping):
                for key, value in detail_flat.items():
                    number = _as_float(value)
                    if number is None:
                        continue
                    flat_ms[str(key)] = round(
                        flat_ms.get(str(key), 0.0) + number,
                        3,
                    )
                    max_flat_ms[str(key)] = round(
                        max(max_flat_ms.get(str(key), number), number),
                        3,
                    )
        else:
            publish_timings = (
                dict(publish.get("timings_ms"))
                if isinstance(publish.get("timings_ms"), Mapping)
                else {}
            )
            for key, value in publish_timings.items():
                number = _as_float(value)
                if number is None:
                    continue
                flat_ms[f"stage.{key}"] = round(
                    flat_ms.get(f"stage.{key}", 0.0) + number,
                    3,
                )
            detail_payload = {
                "schema": "okg.publisher.timing_details.legacy.v1",
                "top_level_ms": publish_timings,
                "flat_ms": {
                    f"stage.{key}": value for key, value in publish_timings.items()
                },
                "lock": {},
                "enrichers": [],
                "consistency_gates": publish.get("consistency_gates") or [],
                "native_views": publish.get("views") or [],
                "lenses": publish.get("lenses") or [],
            }
        per_publish.append(
            {
                "index": index,
                "label": publish.get("label"),
                "status": publish.get("status"),
                "generation_id": publish.get("generation_id"),
                "timing_details": detail_payload,
            }
        )

    graph_visibility_payload = (
        dict(graph_read_visibility)
        if isinstance(graph_read_visibility, Mapping)
        else None
    )
    graph_visibility_ms = _as_float(
        graph_visibility_payload.get("elapsed_ms")
        if isinstance(graph_visibility_payload, Mapping)
        else None
    )
    if graph_visibility_ms is not None:
        flat_ms["graph_read_visibility"] = round(graph_visibility_ms, 3)

    if "lock.hold" in max_flat_ms:
        flat_ms["lock.hold.max"] = max_flat_ms["lock.hold"]

    return {
        "schema": "okg.source_sync.publish_timing_details.v1",
        "publish_count": len(publishes),
        "top_level_ms": top_level_ms,
        "flat_ms": dict(sorted(flat_ms.items())),
        "per_publish": per_publish,
        "graph_read_visibility": graph_visibility_payload,
    }


def _measure_graph_read_visibility(dsn: str) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        from okg.substrate.db.connection import connect as db_connect
        from okg.substrate.graph_store import ChronosGraphStore

        with db_connect(dsn, autocommit=True) as conn:
            checkout = ChronosGraphStore.from_connection(conn).checkout(
                "default",
            )
            rows = checkout.query("SELECT node_id FROM okg.graph_nodes LIMIT 1")
        return {
            "status": "ok",
            "elapsed_ms": _round_or_none(
                (time.perf_counter() - started) * 1000.0,
            ),
            "row_observed": bool(rows),
            "read_path": "chronos_checkout",
            "branch": "default",
        }
    except Exception as exc:  # noqa: BLE001 - benchmark evidence, not publish fate.
        return {
            "status": "error",
            "elapsed_ms": _round_or_none(
                (time.perf_counter() - started) * 1000.0,
            ),
            "error": f"{type(exc).__name__}: {exc}",
        }


def _publish_unattributed_ms(
    timings_ms: Mapping[str, Any],
) -> float | None:
    total = _as_float(timings_ms.get("total"))
    if total is None:
        return None
    attributed = 0.0
    for key, value in timings_ms.items():
        if str(key) == "total":
            continue
        number = _as_float(value)
        if number is not None:
            attributed += number
    return max(0.0, total - attributed)


def _summarize_microbench_samples(
    *,
    deployment: str,
    source_name: str,
    scenario: str,
    pipeline_mode: str,
    run_mode: str,
    event_plan: Mapping[str, Any],
    samples: Sequence[Mapping[str, Any]],
    expected_delta: Mapping[str, Any] | None = None,
    expected_delta_source: str = EXPECTED_DELTA_SOURCE_MISSING,
) -> dict[str, Any]:
    elapsed = [
        float(sample["elapsed_ms"])
        for sample in samples
        if _as_float(sample.get("elapsed_ms")) is not None
    ]
    source_runner = [
        _as_float((sample.get("stage_timings_ms") or {}).get("source_runner"))
        for sample in samples
    ]
    source_runner = [value for value in source_runner if value is not None]
    stage_p95 = _stage_percentiles(samples, percentile=95)
    publish_stage_p95 = _timing_field_percentiles(
        samples,
        field="publish_timings_ms",
        percentile=95,
    )
    publish_timing_details_p95 = _timing_field_percentiles(
        samples,
        field="publish_timing_details_flat_ms",
        percentile=95,
    )
    nodes_total = sum(_as_int(sample.get("nodes_appended")) or 0 for sample in samples)
    edges_total = sum(_as_int(sample.get("edges_appended")) or 0 for sample in samples)
    rows_total = nodes_total + edges_total
    elapsed_total_ms = sum(elapsed)
    pipeline_overheads = [
        _as_float(sample.get("pipeline_overhead_ms")) for sample in samples
    ]
    pipeline_overheads = [value for value in pipeline_overheads if value is not None]
    publish_unattributed = [
        _as_float(sample.get("publish_unattributed_ms")) for sample in samples
    ]
    publish_unattributed = [
        value for value in publish_unattributed if value is not None
    ]
    graph_read_visibility = [
        _as_float(sample.get("graph_read_visibility_ms")) for sample in samples
    ]
    graph_read_visibility = [
        value for value in graph_read_visibility if value is not None
    ]
    latency_p95 = _round_or_none(_percentile(elapsed, 95))
    publish_scope = _publish_scope_summary(
        event_plan=event_plan,
        samples=samples,
        publish_stage_p95=publish_stage_p95,
        publish_timing_details_p95=publish_timing_details_p95,
        latency_p95_ms=latency_p95,
        rows_appended_total=rows_total,
    )
    observability = _microbench_observability_terms(
        expected_delta=expected_delta,
        expected_delta_source=expected_delta_source,
        publish_scope=publish_scope,
        latency_p95_ms=latency_p95,
        sample_count=len(samples),
        source_work_units=rows_total,
    )
    return {
        "deployment": deployment,
        "source_name": source_name,
        "scenario": scenario,
        "pipeline_mode": pipeline_mode,
        "run_mode": run_mode,
        "sample_count": len(samples),
        "ok": all(bool(sample.get("ok")) for sample in samples),
        "error_count": sum(1 for sample in samples if sample.get("error")),
        "latency_ms": {
            "min": _round_or_none(min(elapsed) if elapsed else None),
            "p50": _round_or_none(_percentile(elapsed, 50)),
            "p95": latency_p95,
            "max": _round_or_none(max(elapsed) if elapsed else None),
        },
        "source_runner_ms": {
            "p50": _round_or_none(_percentile(source_runner, 50)),
            "p95": _round_or_none(_percentile(source_runner, 95)),
        },
        "pipeline_overhead_ms": {
            "p50": _round_or_none(_percentile(pipeline_overheads, 50)),
            "p95": _round_or_none(_percentile(pipeline_overheads, 95)),
        },
        "stage_p95_ms": stage_p95,
        "publish_stage_p95_ms": publish_stage_p95,
        "publish_timing_details_p95_ms": publish_timing_details_p95,
        "graph_read_visibility_ms": {
            "p50": _round_or_none(_percentile(graph_read_visibility, 50)),
            "p95": _round_or_none(_percentile(graph_read_visibility, 95)),
        },
        "publish_unattributed_ms": {
            "p50": _round_or_none(_percentile(publish_unattributed, 50)),
            "p95": _round_or_none(_percentile(publish_unattributed, 95)),
        },
        "nodes_appended_total": nodes_total,
        "edges_appended_total": edges_total,
        "rows_appended_total": rows_total,
        "throughput_rows_per_second": _round_or_none(
            (rows_total / (elapsed_total_ms / 1000.0)) if elapsed_total_ms > 0 else None
        ),
        "publish_scope": publish_scope,
        **observability,
        "event_plan": {
            key: value for key, value in event_plan.items() if key != "events"
        },
    }


def _microbench_observability_terms(
    *,
    expected_delta: Mapping[str, Any] | None,
    expected_delta_source: str,
    publish_scope: Mapping[str, Any],
    latency_p95_ms: float | None,
    sample_count: int,
    source_work_units: int,
) -> dict[str, Any]:
    expected = {
        str(key): value
        for key, value in (expected_delta or {}).items()
        if _as_float(value) is not None
    }
    expected_units = sum(float(_as_float(value) or 0.0) for value in expected.values())
    observed_delta = {
        str(key): float(value)
        for key, value in (
            publish_scope.get("publish_delta_totals") or {}
        ).items()
        if _as_float(value) is not None
    }
    observed_units = sum(max(0.0, value) for value in observed_delta.values())
    amplification_ratio = (
        (float(source_work_units) / observed_units)
        if observed_units > 0 else None
    )
    gaps: list[str] = []
    expected_source = (
        str(expected_delta_source or EXPECTED_DELTA_SOURCE_MISSING)
        if expected else EXPECTED_DELTA_SOURCE_MISSING
    )
    # No-change-dominant sources: a source that DECLARES an explicit all-zero
    # expected_delta (a non-empty {field: 0, ...} block from an accepted
    # source) is asserting that a representative single change to the
    # deployment does not touch it, so producing 0 observed delta is CORRECT
    # — not missing evidence. Work-amplification is undefined (no delta to
    # amplify), so only the latency of its no-change pipeline path is graded.
    # PACT okg-workspace-recovery-tail: optimization-budget no-change gating
    # for file-based READY_SOURCES whose single-delta is genuinely zero.
    expected_all_zero = _expected_delta_is_all_zero(expected)
    no_change_dominant = (
        bool(expected)
        and expected_all_zero
        and observed_units <= 0
        and expected_source in EXPECTED_DELTA_ACCEPTED_SOURCES
    )
    if not no_change_dominant:
        if (
            expected_source not in EXPECTED_DELTA_ACCEPTED_SOURCES
            or expected_units <= 0
        ):
            gaps.append("missing_expected_delta")
        if observed_units <= 0:
            gaps.append("missing_observed_delta")
        if (
            expected_units > 0
            and observed_units > 0
            and expected_units != observed_units
        ):
            gaps.append("expected_delta_mismatch")
        if amplification_ratio is None:
            gaps.append("missing_work_amplification_ratio")
        elif amplification_ratio > MAX_WORK_AMPLIFICATION_RATIO:
            gaps.append("work_amplification_exceeded")
    elif amplification_ratio is not None and (
        amplification_ratio > MAX_WORK_AMPLIFICATION_RATIO
    ):
        # Defensive: if a "no-change" source ever does emit work, still flag
        # runaway amplification rather than silently passing it.
        gaps.append("work_amplification_exceeded")
    if sample_count <= 0:
        gaps.append("missing_measurement_rows")
    if latency_p95_ms is None:
        gaps.append("missing_latency_measurement")
    return {
        "expected_delta": expected,
        "expected_delta_source": expected_source,
        "expected_delta_units": _round_or_none(expected_units) if expected else None,
        "observed_delta": observed_delta,
        "observed_delta_units": _round_or_none(observed_units),
        "source_work_units": float(source_work_units),
        "work_amplification_ratio": _round_or_none(amplification_ratio),
        "work_amplification_budget": MAX_WORK_AMPLIFICATION_RATIO,
        "observability_status": "pass" if not gaps else "fail",
        "observability_gaps": gaps,
        "no_change_dominant": no_change_dominant,
    }


def _publish_scope_summary(
    *,
    event_plan: Mapping[str, Any],
    samples: Sequence[Mapping[str, Any]],
    publish_stage_p95: Mapping[str, float],
    publish_timing_details_p95: Mapping[str, float],
    latency_p95_ms: float | None,
    rows_appended_total: int,
) -> dict[str, Any]:
    publish_delta_fields = (
        "nodes_added",
        "nodes_retracted",
        "nodes_updated",
        "edges_added",
        "edges_retracted",
        "edges_updated",
    )
    publish_delta_totals = {key: 0 for key in publish_delta_fields}
    publish_count = 0
    generation_ids: list[str] = []
    max_enricher_dirty_nodes = 0
    max_enricher_dirty_edges = 0
    max_view_refreshed_fragments = 0
    for sample in samples:
        for publish in sample.get("publishes") or ():
            if not isinstance(publish, Mapping):
                continue
            publish_count += 1
            generation_id = str(publish.get("generation_id") or "")
            if generation_id:
                generation_ids.append(generation_id)
            for field in publish_delta_fields:
                publish_delta_totals[field] += _as_int(publish.get(field)) or 0
        details = sample.get("publish_timing_details")
        if not isinstance(details, Mapping):
            continue
        for per_publish in details.get("per_publish") or ():
            if not isinstance(per_publish, Mapping):
                continue
            timing_details = per_publish.get("timing_details")
            if not isinstance(timing_details, Mapping):
                continue
            for enricher in timing_details.get("enrichers") or ():
                if not isinstance(enricher, Mapping):
                    continue
                max_enricher_dirty_nodes = max(
                    max_enricher_dirty_nodes,
                    _as_int(enricher.get("dirty_node_count")) or 0,
                )
                max_enricher_dirty_edges = max(
                    max_enricher_dirty_edges,
                    _as_int(enricher.get("dirty_edge_count")) or 0,
                )
            for view in timing_details.get("native_views") or ():
                if not isinstance(view, Mapping):
                    continue
                max_view_refreshed_fragments = max(
                    max_view_refreshed_fragments,
                    _as_int(view.get("fragments_refreshed")) or 0,
                )

    widened_stages: list[dict[str, Any]] = []
    for stage, reason, owner in (
        ("enrichers", "declared_enricher_dependency_scope", "enrichers"),
        ("gates", "declared_gate_dependency_scope", "gates"),
        (
            "consistency_gates",
            "declared_gate_dependency_scope",
            "consistency_gates",
        ),
        (
            "native_views",
            "declared_view_dependency_scope",
            "native_views",
        ),
        (
            "delta_counts_before_views",
            "graph_delta_accounting",
            "chronos_delta",
        ),
        (
            "delta_counts_after_views",
            "view_delta_accounting",
            "chronos_delta",
        ),
        (
            "checkpoint_generation",
            "published_generation_boundary",
            "chronos_checkpoint",
        ),
    ):
        elapsed_ms = _as_float(publish_stage_p95.get(stage))
        if elapsed_ms is None or elapsed_ms <= 0:
            continue
        widened_stages.append(
            {
                "stage": stage,
                "owner": owner,
                "reason": reason,
                "elapsed_p95_ms": _round_or_none(elapsed_ms),
            }
        )
    top_widening = (
        max(
            widened_stages,
            key=lambda item: float(item.get("elapsed_p95_ms") or 0.0),
        )
        if widened_stages
        else None
    )
    lock_wait_p95_ms = (
        _as_float(publish_timing_details_p95.get("lock.acquire"))
        or _as_float(publish_stage_p95.get("locked_connection"))
        or _as_float(publish_timing_details_p95.get("stage.locked_connection"))
    )
    # Prefer the worst SINGLE hold (issue #1291). `lock.hold` is the sample
    # total across every publish in the run, so comparing it to a per-cycle
    # statement-timeout bound would over-report a multi-publish scenario. The
    # older key remains the only value on samples recorded before the max was
    # emitted, and the basis is reported rather than assumed.
    lock_hold_max_p95_ms = _as_float(
        publish_timing_details_p95.get("lock.hold.max"),
    )
    lock_hold_total_p95_ms = _as_float(
        publish_timing_details_p95.get("lock.hold"),
    )
    lock_hold_p95_ms = (
        lock_hold_max_p95_ms
        if lock_hold_max_p95_ms is not None
        else lock_hold_total_p95_ms
    )
    lock_hold_basis = (
        "worst_single_publish"
        if lock_hold_max_p95_ms is not None
        else ("sample_total" if lock_hold_total_p95_ms is not None else None)
    )
    lock_contention = (
        latency_p95_ms is not None
        and latency_p95_ms > SUBSECOND_LATENCY_BUDGET_MS
        and lock_wait_p95_ms is not None
        and lock_wait_p95_ms >= PUBLISH_LOCK_CONTENTION_THRESHOLD_MS
    )
    changed_graph_rows = sum(max(0, value) for value in publish_delta_totals.values())
    readiness_impact = (
        "over_budget"
        if latency_p95_ms is not None and latency_p95_ms > SUBSECOND_LATENCY_BUDGET_MS
        else "within_budget"
    )
    return {
        "schema": "okg.source_sync.publish_scope.v1",
        "changed_inputs": {
            "scope_kind": event_plan.get("scope_kind"),
            "scope_key": event_plan.get("scope_key"),
            "revision_hint": event_plan.get("revision_hint"),
            "scope_status": event_plan.get("scope_status"),
            "event_count": event_plan.get("event_count"),
        },
        "publish_count": publish_count,
        "generation_ids": sorted(set(generation_ids)),
        "source_rows_appended_total": rows_appended_total,
        "changed_graph_rows": changed_graph_rows,
        "publish_delta_totals": publish_delta_totals,
        "widened_stage_count": len(widened_stages),
        "widened_stages": widened_stages,
        "top_widening_stage": (top_widening.get("stage") if top_widening else None),
        "top_widening_owner": (top_widening.get("owner") if top_widening else None),
        "top_widening_reason": (top_widening.get("reason") if top_widening else None),
        "top_widening_elapsed_p95_ms": (
            top_widening.get("elapsed_p95_ms") if top_widening else None
        ),
        "publish_lock_wait_p95_ms": _round_or_none(lock_wait_p95_ms),
        "publish_lock_hold_p95_ms": _round_or_none(lock_hold_p95_ms),
        "publish_lock_hold_basis": lock_hold_basis,
        "environment_contention": bool(lock_contention),
        "environment_contention_stage": (
            "publisher.lock_acquire" if lock_contention else None
        ),
        "environment_contention_reason": (
            "concurrent_publisher_lock_contention" if lock_contention else None
        ),
        "max_enricher_dirty_nodes": max_enricher_dirty_nodes,
        "max_enricher_dirty_edges": max_enricher_dirty_edges,
        "max_view_refreshed_fragments": max_view_refreshed_fragments,
        "readiness_impact": readiness_impact,
        "publish_timing_detail_keys": sorted(publish_timing_details_p95),
    }


def _stage_percentiles(
    samples: Sequence[Mapping[str, Any]],
    *,
    percentile: int,
) -> dict[str, float]:
    return _timing_field_percentiles(
        samples,
        field="stage_timings_ms",
        percentile=percentile,
    )


def _timing_field_percentiles(
    samples: Sequence[Mapping[str, Any]],
    *,
    field: str,
    percentile: int,
) -> dict[str, float]:
    by_stage: dict[str, list[float]] = {}
    for sample in samples:
        timings = sample.get(field) or {}
        if not isinstance(timings, Mapping):
            continue
        for key, value in timings.items():
            number = _as_float(value)
            if number is None:
                continue
            by_stage.setdefault(str(key), []).append(number)
    return {
        key: _round_or_none(_percentile(values, percentile)) or 0.0
        for key, values in sorted(by_stage.items())
    }


def _percentile(values: Sequence[float], percentile: int) -> float | None:
    clean = sorted(float(value) for value in values)
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    rank = (percentile / 100.0) * (len(clean) - 1)
    lower = int(rank)
    upper = min(lower + 1, len(clean) - 1)
    fraction = rank - lower
    return clean[lower] + (clean[upper] - clean[lower]) * fraction


def _round_or_none(value: float | None) -> float | None:
    return round(float(value), 3) if value is not None else None




def _source_row(
    *,
    deployment: str,
    source_name: str,
    entry: SourceEntry,
    runtime_enabled: bool,
    runtime_source_sync: Mapping[str, Any],
    expected_producers: Sequence[Mapping[str, Any]],
    disabled_producers: Sequence[Mapping[str, Any]],
    live_state: Mapping[str, Any],
) -> dict[str, Any]:
    sync = entry.sync
    raw = entry.raw if isinstance(entry.raw, Mapping) else {}
    trigger_kinds = [str(item) for item in getattr(sync, "triggers", ()) or ()]
    scope_accepts = [
        str(item) for item in getattr(getattr(sync, "scope", None), "accepts", ()) or ()
    ]
    default_event_mode = (
        str(getattr(sync, "default_event_mode", "") or "") if sync is not None else ""
    )
    reconcile_mode = (
        str(getattr(sync, "reconcile_mode", "") or "") if sync is not None else ""
    )
    progress = _source_live(live_state, "progress", source_name)
    watermark = _source_live(live_state, "watermarks", source_name)
    recent = _source_live(live_state, "recent_deltas", source_name)
    stage_timings = _source_live(live_state, "stage_timings", source_name)
    full_pipeline = _source_live(
        live_state,
        "full_pipeline_microbench",
        source_name,
    )
    source_stats = (
        stage_timings.get("source_stats")
        if isinstance(stage_timings.get("source_stats"), Mapping)
        else {}
    )
    coverage_class = _coverage_class(
        source_name=source_name,
        entry=entry,
        sync_declared=sync is not None,
        trigger_kinds=trigger_kinds,
        expected_producer_count=len(expected_producers),
        disabled_producer_count=len(disabled_producers),
    )
    cursor_status = _cursor_support_status(
        sync_declared=sync is not None,
        default_event_mode=default_event_mode,
        reconcile_mode=reconcile_mode,
        measured_files_scanned=(
            _as_int(source_stats.get("files_scanned")) if source_stats else None
        ),
        probe_short_circuited=(
            isinstance(source_stats, Mapping)
            and source_stats.get("scope_status") == "probe_short_circuit"
        ),
    )
    scope_status = _scope_support_status(
        sync_declared=sync is not None,
        scope_accepts=scope_accepts,
        trigger_kinds=trigger_kinds,
    )
    producer_status = _producer_status(
        sync_declared=sync is not None,
        expected_producer_count=len(expected_producers),
        disabled_producer_count=len(disabled_producers),
        runtime_enabled=runtime_enabled,
        trigger_kinds=trigger_kinds,
    )
    scenarios = _benchmark_scenarios(coverage_class)
    latest_runner_ms = (
        _as_float(stage_timings.get("source_runner")) if stage_timings else None
    )
    if latest_runner_ms is None:
        latest_runner_ms = _as_float(progress.get("source_runner_ms"))
    blocker = _blocker(
        coverage_class=coverage_class,
        cursor_support_status=cursor_status,
        scope_support_status=scope_status,
        producer_status=producer_status,
    )
    return {
        "deployment": deployment,
        "source_name": source_name,
        "owner": str(entry.ownership_id or source_name),
        "source_class": entry.source_class,
        "module": str(raw.get("module") or ""),
        "class_name": str(raw.get("class") or ""),
        "predecessor_source": str(raw.get("predecessor_source") or ""),
        "coverage_class": coverage_class,
        "readiness_required": source_name in READY_SOURCES,
        "required_for_baseline": bool(entry.required_for_baseline),
        "credential_refs": list(entry.credential_refs or ()),
        "trigger_kinds": trigger_kinds,
        "default_event_mode": default_event_mode or None,
        "reconcile_mode": reconcile_mode or None,
        "deletes_require_reconcile": bool(
            getattr(sync, "deletes_require_reconcile", False)
            if sync is not None
            else False
        ),
        "scope_accepts": scope_accepts,
        "unsupported_scope_fallback": (
            getattr(getattr(sync, "scope", None), "unsupported_fallback", None)
            if sync is not None
            else None
        ),
        "cursor_support_status": cursor_status,
        "scope_support_status": scope_status,
        "producer_status": producer_status,
        "expected_producer_count": len(expected_producers),
        "expected_producers": [dict(row) for row in expected_producers],
        "disabled_producer_count": len(disabled_producers),
        "disabled_producers": [dict(row) for row in disabled_producers],
        "benchmark_scenarios": scenarios,
        "latest_source_runner_ms": latest_runner_ms,
        "latest_stage_total_ms": _as_float(stage_timings.get("total")),
        "latest_stage_admission_prepare_ms": _as_float(
            stage_timings.get("admission_prepare"),
        ),
        "latest_stage_import_ms": _as_float(stage_timings.get("import")),
        "latest_stage_construct_ms": _as_float(
            stage_timings.get("construct"),
        ),
        "latest_stage_source_runner_ms": _as_float(
            stage_timings.get("source_runner"),
        ),
        "latest_stage_source_state_snapshot_ms": _as_float(
            stage_timings.get("source_state_snapshot"),
        ),
        "latest_stage_admission_finalize_ms": _as_float(
            stage_timings.get("admission_finalize"),
        ),
        "latest_full_pipeline_scenario": full_pipeline.get("scenario"),
        "latest_full_pipeline_ok": full_pipeline.get("ok"),
        "latest_full_pipeline_latency_p95_ms": _as_float(
            full_pipeline.get("latency_p95_ms"),
        ),
        "latest_full_pipeline_rows_appended": _as_int(
            full_pipeline.get("rows_appended_total"),
        ),
        "latest_full_pipeline_changed_graph_rows": _as_int(
            full_pipeline.get("changed_graph_rows"),
        ),
        "latest_full_pipeline_expected_delta": full_pipeline.get("expected_delta")
        or {},
        "latest_full_pipeline_expected_delta_source": full_pipeline.get(
            "expected_delta_source",
        ),
        "latest_full_pipeline_expected_delta_units": _as_float(
            full_pipeline.get("expected_delta_units"),
        ),
        "latest_full_pipeline_observed_delta_units": _as_float(
            full_pipeline.get("observed_delta_units"),
        ),
        "latest_full_pipeline_source_work_units": _as_float(
            full_pipeline.get("source_work_units"),
        ),
        "latest_full_pipeline_work_amplification_ratio": _as_float(
            full_pipeline.get("work_amplification_ratio"),
        ),
        "latest_full_pipeline_work_amplification_budget": _as_float(
            full_pipeline.get("work_amplification_budget"),
        ),
        "latest_full_pipeline_observability_status": full_pipeline.get(
            "observability_status",
        ),
        "latest_full_pipeline_observability_gaps": list(
            full_pipeline.get("observability_gaps") or (),
        ),
        "latest_full_pipeline_no_change_dominant": bool(
            full_pipeline.get("no_change_dominant"),
        ),
        "latest_full_pipeline_top_widening_stage": full_pipeline.get(
            "top_widening_stage",
        ),
        "latest_full_pipeline_top_widening_owner": full_pipeline.get(
            "top_widening_owner",
        ),
        "latest_full_pipeline_top_widening_reason": full_pipeline.get(
            "top_widening_reason",
        ),
        "latest_full_pipeline_top_widening_ms": _as_float(
            full_pipeline.get("top_widening_elapsed_p95_ms"),
        ),
        "latest_full_pipeline_lock_wait_p95_ms": _as_float(
            full_pipeline.get("publish_lock_wait_p95_ms"),
        ),
        "latest_full_pipeline_lock_hold_p95_ms": _as_float(
            full_pipeline.get("publish_lock_hold_p95_ms"),
        ),
        "latest_full_pipeline_environment_contention": bool(
            full_pipeline.get("environment_contention"),
        ),
        "latest_full_pipeline_environment_stage": full_pipeline.get(
            "environment_contention_stage",
        ),
        "latest_full_pipeline_environment_reason": full_pipeline.get(
            "environment_contention_reason",
        ),
        "latest_full_pipeline_readiness_impact": full_pipeline.get(
            "readiness_impact",
        ),
        "latest_full_pipeline_artifact": full_pipeline.get("artifact"),
        "latest_full_pipeline_generated_at": full_pipeline.get(
            "generated_at",
        ),
        "latest_source_files_seen": _as_int(source_stats.get("files_seen")),
        "latest_source_files_scanned": _as_int(
            source_stats.get("files_scanned"),
        ),
        "latest_source_files_scanned_full": _as_int(
            source_stats.get("files_scanned_full"),
        ),
        "latest_source_files_scanned_append_delta": _as_int(
            source_stats.get("files_scanned_append_delta"),
        ),
        "latest_source_files_skipped_unchanged": _as_int(
            source_stats.get("files_skipped_unchanged"),
        ),
        "latest_source_files_skipped_cwd_filter": _as_int(
            source_stats.get("files_skipped_cwd_filter"),
        ),
        "latest_source_bytes_delta": _as_int(
            source_stats.get("bytes_delta"),
        ),
        "latest_source_lines_delta": _as_int(
            source_stats.get("lines_delta"),
        ),
        "latest_source_facts_evaluated": _as_int(
            source_stats.get("facts_evaluated"),
        ),
        "latest_source_facts_emitted": _as_int(
            source_stats.get("facts_emitted"),
        ),
        "latest_source_facts_filtered_unchanged": _as_int(
            source_stats.get("facts_filtered_unchanged"),
        ),
        "latest_nodes_appended": _as_int(progress.get("nodes_appended")),
        "latest_edges_appended": _as_int(progress.get("edges_appended")),
        "latest_dedupe_drops_node": _as_int(
            progress.get("dedupe_drops_node"),
        ),
        "latest_dedupe_drops_edge": _as_int(
            progress.get("dedupe_drops_edge"),
        ),
        "last_run_observed_at": progress.get("observed_at"),
        "last_reconcile_at": watermark.get("last_reconcile_at"),
        "node_count": _as_int(watermark.get("node_count")),
        "edge_count": _as_int(watermark.get("edge_count")),
        "nodes_added_1h": _as_int(recent.get("nodes_added_1h")),
        "edges_added_1h": _as_int(recent.get("edges_added_1h")),
        "nodes_added_24h": _as_int(recent.get("nodes_added_24h")),
        "edges_added_24h": _as_int(recent.get("edges_added_24h")),
        "next_action": _next_action(
            coverage_class=coverage_class,
            benchmark_scenarios=scenarios,
            blocker=blocker,
        ),
        "blocker": blocker,
    }


def _enricher_row(
    *,
    deployment: str,
    index: int,
    raw: Mapping[str, Any],
) -> dict[str, Any]:
    class_path = str(raw.get("class") or "")
    config = raw.get("config") if isinstance(raw.get("config"), Mapping) else {}
    name = str(config.get("name") or class_path.rsplit(".", 1)[-1] or index)
    authority = raw.get("authority")
    scope = {}
    if isinstance(authority, Mapping):
        scope = authority.get("authority_scope") or {}
        if not isinstance(scope, Mapping):
            scope = {}
    module, _, class_name = class_path.rpartition(".")
    return {
        "deployment": deployment,
        "name": name,
        "owner": str(scope.get("source_family") or f"_enricher:{name}"),
        "module": module,
        "class_name": class_name or class_path,
        "coverage_class": "source_backed_enricher",
        "scope_support_status": "needs_dirty_scope_audit",
        "scope_accepts": [],
        "benchmark_scenarios": [FULL_PIPELINE_SCENARIO],
        "next_action": "include_in_full_pipeline_microbench",
        "blocker": None,
    }


def _coverage_class(
    *,
    source_name: str,
    entry: SourceEntry,
    sync_declared: bool,
    trigger_kinds: Sequence[str],
    expected_producer_count: int,
    disabled_producer_count: int,
) -> str:
    if entry.credential_refs:
        return "manual_full_rebuild"
    if not sync_declared:
        return "unsupported"
    if disabled_producer_count and not expected_producer_count:
        return "disabled"
    trigger_set = set(trigger_kinds)
    if not trigger_set or trigger_set <= {"manual", "reconcile"}:
        return "manual_full_rebuild"
    if source_name in READY_SOURCES:
        return "automatic_hot_path"
    return "useful_automatic"


def _cursor_support_status(
    *,
    sync_declared: bool,
    default_event_mode: str,
    reconcile_mode: str,
    measured_files_scanned: int | None = None,
    probe_short_circuited: bool = False,
) -> str:
    if not sync_declared:
        return "unsupported"
    # Churn-measured promotion (issue #323): a change-probe short-circuit — or a
    # measured no-change run that scanned zero files — proves the source did zero
    # scope work, so the status is a real admission check, not a declared-mode
    # guess. This is what flips `completed_scope_requires_audit` to verified.
    if probe_short_circuited or measured_files_scanned == 0:
        return "incremental_verified"
    modes = {default_event_mode, reconcile_mode}
    if "cursor" in modes or "append" in modes:
        return "declared_cursor_mode"
    if modes & SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES:
        return "completed_scope_requires_audit"
    return "needs_adapter_audit"


def _producer_status(
    *,
    sync_declared: bool,
    expected_producer_count: int,
    disabled_producer_count: int,
    runtime_enabled: bool,
    trigger_kinds: Sequence[str],
) -> str:
    if not sync_declared:
        return "not_configured"
    if not runtime_enabled and set(trigger_kinds) - {"manual", "reconcile"}:
        return "runtime_disabled"
    if expected_producer_count:
        return "configured"
    if disabled_producer_count:
        return "disabled"
    return "manual_only"


def _scope_support_status(
    *,
    sync_declared: bool,
    scope_accepts: Sequence[str],
    trigger_kinds: Sequence[str],
) -> str:
    if not sync_declared:
        return "unsupported"
    if scope_accepts:
        return "declared"
    trigger_set = set(trigger_kinds)
    if trigger_set and trigger_set <= {"poll", "manual", "reconcile"}:
        return "unscoped_poll_reconcile"
    return "not_declared"


def _benchmark_scenarios(coverage_class: str) -> list[str]:
    if coverage_class in {"automatic_hot_path", "useful_automatic"}:
        return [SOURCE_ONLY_SCENARIO, FULL_PIPELINE_SCENARIO]
    if coverage_class == "manual_full_rebuild":
        return [SOURCE_ONLY_SCENARIO]
    return []


def _blocker(
    *,
    coverage_class: str,
    cursor_support_status: str,
    scope_support_status: str,
    producer_status: str,
) -> str | None:
    if coverage_class == "unsupported":
        return "source_sync_not_declared"
    if coverage_class == "disabled":
        return "runtime_producer_disabled"
    if producer_status == "runtime_disabled":
        return "runtime_disabled"
    if coverage_class in {"automatic_hot_path", "useful_automatic"}:
        if scope_support_status not in {"declared", "unscoped_poll_reconcile"}:
            return "scope_support_missing"
        if cursor_support_status in {"needs_adapter_audit", "unsupported"}:
            return "cursor_support_unproven"
    return None


def _next_action(
    *,
    coverage_class: str,
    benchmark_scenarios: Sequence[str],
    blocker: str | None,
) -> str:
    if blocker:
        return "fix_inventory_blocker"
    if benchmark_scenarios:
        return "run_microbenchmarks"
    if coverage_class in {"manual_full_rebuild", "unsupported", "disabled"}:
        return "confirm_non_hot_path_classification"
    return "inspect"


def _group_producers(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, list[Mapping[str, Any]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        source_name = str(row.get("source_name") or "")
        if source_name:
            grouped.setdefault(source_name, []).append(row)
    return grouped


def _load_live_state(
    dsn: str,
    *,
    deployment: str,
    source_names: Sequence[str] | None = None,
) -> dict[str, Any]:
    from okg.substrate.db.connection import connect as db_connect

    state = _empty_live_state("loaded")
    try:
        with db_connect(dsn, autocommit=True) as conn:
            state["progress"] = _load_last_run_progress(
                conn,
                deployment,
                source_names=source_names,
            )
            state["watermarks"] = _load_watermarks(
                conn,
                deployment,
                source_names=source_names,
            )
            state["recent_deltas"] = _load_recent_deltas(
                conn,
                deployment,
                source_names=source_names,
            )
            state["stage_timings"] = _load_stage_timings(
                conn,
                deployment,
                source_names=source_names,
            )
            state["full_pipeline_microbench"] = _load_full_pipeline_microbench(
                conn,
                deployment,
                source_names=source_names,
            )
    except Exception as exc:  # noqa: BLE001 - inventory remains useful.
        state["status"] = "error"
        state["error"] = f"{type(exc).__name__}: {exc}"
    return state


def _empty_live_state(status: str) -> dict[str, Any]:
    return {
        "status": status,
        "error": None,
        "progress": {},
        "watermarks": {},
        "recent_deltas": {},
        "stage_timings": {},
        "full_pipeline_microbench": {},
    }


def _source_filter_sql(
    source_names: Sequence[str] | None,
    *,
    prefix: str = "WHERE",
) -> str:
    if not source_names:
        return ""
    return f"{prefix} source_name = ANY(%(source_names)s)"


def _load_last_run_progress(
    conn: Any,
    deployment: str,
    *,
    source_names: Sequence[str] | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    source_filter_sql = _source_filter_sql(source_names)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH runs AS (
              SELECT
                CASE
                  WHEN source_name LIKE %(deployment)s || '.%%'
                    THEN substring(source_name FROM length(%(deployment)s) + 2)
                  ELSE source_name
                END AS source_name,
                updated_at,
                value
              FROM okg.source_progress
              WHERE deployment = %(deployment)s
                AND key = 'sync.last_run'
            )
            SELECT DISTINCT ON (source_name)
                   source_name,
                   updated_at,
                   value
              FROM runs
             {source_filter_sql}
             ORDER BY source_name, updated_at DESC
            """,
            {
                "deployment": deployment,
                "source_names": list(source_names or ()),
            },
        )
        for source_name, updated_at, value in cur.fetchall():
            payload = _json_obj(value)
            payload.setdefault("observed_at", updated_at)
            out[str(source_name)] = payload
    return out


def _load_watermarks(
    conn: Any,
    deployment: str,
    *,
    source_names: Sequence[str] | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    source_filter_sql = _source_filter_sql(source_names)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH watermarks AS (
              SELECT
                CASE
                  WHEN source LIKE %(deployment)s || '.%%'
                    THEN substring(source FROM length(%(deployment)s) + 2)
                  ELSE source
                END AS source_name,
                last_reconcile_at,
                NULLIF(
                  last_applied_rev->'record_set_summary'->>'node_count', ''
                )::bigint AS node_count,
                NULLIF(
                  last_applied_rev->'record_set_summary'->>'edge_count', ''
                )::bigint AS edge_count
              FROM okg.source_watermarks
              WHERE source = %(deployment)s
                 OR source LIKE %(deployment)s || '.%%'
                 OR source NOT LIKE '%%.%%'
            )
            SELECT source_name, last_reconcile_at, node_count, edge_count
              FROM watermarks
             {source_filter_sql}
            """,
            {
                "deployment": deployment,
                "source_names": list(source_names or ()),
            },
        )
        for source_name, last_reconcile_at, node_count, edge_count in cur.fetchall():
            out[str(source_name)] = {
                "last_reconcile_at": last_reconcile_at,
                "node_count": node_count,
                "edge_count": edge_count,
            }
    return out


def _load_recent_deltas(
    conn: Any,
    deployment: str,
    *,
    source_names: Sequence[str] | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    source_filter_sql = _source_filter_sql(source_names, prefix="WHERE")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH runs AS (
              SELECT
                CASE
                  WHEN source_name LIKE %(deployment)s || '.%%'
                    THEN substring(source_name FROM length(%(deployment)s) + 2)
                  ELSE source_name
                END AS source_name,
                coalesce(
                  NULLIF(value::jsonb->>'finished_at', '')::timestamptz,
                  updated_at
                ) AS observed_at,
                coalesce(
                  NULLIF(value::jsonb->>'nodes_appended', '')::bigint, 0
                ) AS nodes_appended,
                coalesce(
                  NULLIF(value::jsonb->>'edges_appended', '')::bigint, 0
                ) AS edges_appended
              FROM okg.source_progress
              WHERE deployment = %(deployment)s
                AND key LIKE 'sync.run.%%'
            )
            SELECT source_name,
                   sum(nodes_appended) FILTER (
                     WHERE observed_at >= now() - interval '1 hour'
                   )::bigint AS nodes_added_1h,
                   sum(edges_appended) FILTER (
                     WHERE observed_at >= now() - interval '1 hour'
                   )::bigint AS edges_added_1h,
                   sum(nodes_appended) FILTER (
                     WHERE observed_at >= now() - interval '24 hours'
                   )::bigint AS nodes_added_24h,
                   sum(edges_appended) FILTER (
                     WHERE observed_at >= now() - interval '24 hours'
                   )::bigint AS edges_added_24h
              FROM runs
             {source_filter_sql}
             {"AND" if source_filter_sql else "WHERE"} observed_at >= now() - interval '24 hours'
             GROUP BY source_name
            """,
            {
                "deployment": deployment,
                "source_names": list(source_names or ()),
            },
        )
        for row in cur.fetchall():
            out[str(row[0])] = {
                "nodes_added_1h": row[1],
                "edges_added_1h": row[2],
                "nodes_added_24h": row[3],
                "edges_added_24h": row[4],
            }
    return out


def _load_stage_timings(
    conn: Any,
    deployment: str,
    *,
    source_names: Sequence[str] | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    source_filter_sql = _source_filter_sql(source_names)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH runs AS (
              SELECT
                CASE
                  WHEN source_name LIKE %(deployment)s || '.%%'
                    THEN substring(source_name FROM length(%(deployment)s) + 2)
                  ELSE source_name
                END AS source_name,
                updated_at,
                value
              FROM okg.source_progress
              WHERE deployment = %(deployment)s
                AND key = 'sync.stage_timings.last_run'
            )
            SELECT DISTINCT ON (source_name)
                   source_name,
                   updated_at,
                   value
              FROM runs
             {source_filter_sql}
             ORDER BY source_name, updated_at DESC
            """,
            {
                "deployment": deployment,
                "source_names": list(source_names or ()),
            },
        )
        for source_name, _updated_at, value in cur.fetchall():
            payload = _json_obj(value)
            timings = payload.get("timings_ms")
            if isinstance(timings, Mapping):
                row = {
                    key: _as_float(val)
                    for key, val in timings.items()
                    if _as_float(val) is not None
                }
                source_stats = payload.get("source_stats")
                if isinstance(source_stats, Mapping):
                    row["source_stats"] = dict(source_stats)
                out[str(source_name)] = row
    return out


def _load_full_pipeline_microbench(
    conn: Any,
    deployment: str,
    *,
    source_names: Sequence[str] | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    source_filter_sql = _source_filter_sql(source_names)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH runs AS (
              SELECT
                CASE
                  WHEN source_name LIKE %(deployment)s || '.%%'
                    THEN substring(source_name FROM length(%(deployment)s) + 2)
                  ELSE source_name
                END AS source_name,
                updated_at,
                value
              FROM okg.source_progress
              WHERE deployment = %(deployment)s
                AND key = %(key)s
            )
            SELECT DISTINCT ON (source_name)
                   source_name,
                   updated_at,
                   value
              FROM runs
             {source_filter_sql}
             ORDER BY source_name, updated_at DESC
            """,
            {
                "deployment": deployment,
                "key": MICROBENCH_FULL_PIPELINE_PROGRESS_KEY,
                "source_names": list(source_names or ()),
            },
        )
        for source_name, _updated_at, value in cur.fetchall():
            payload = _json_obj(value)
            if payload.get("schema") != MICROBENCH_FULL_PIPELINE_PROGRESS_SCHEMA:
                continue
            out[str(source_name)] = payload
    return out


def _persist_microbench_full_pipeline_progress(
    *,
    dsn: str | None,
    deployment: str,
    rows: Sequence[Mapping[str, Any]],
    generated_at: str,
    artifact: Any,
) -> None:
    if not dsn:
        return
    progress_rows: list[dict[str, str]] = []
    for row in rows:
        if row.get("pipeline_mode") != FULL_PIPELINE_SCENARIO:
            continue
        if row.get("skipped") or row.get("no_event_noop"):
            continue
        if not row.get("sample_count"):
            continue
        source_name = str(row.get("source_name") or "")
        if not source_name:
            continue
        publish_scope = (
            dict(row.get("publish_scope"))
            if isinstance(row.get("publish_scope"), Mapping)
            else {}
        )
        payload = {
            "schema": MICROBENCH_FULL_PIPELINE_PROGRESS_SCHEMA,
            "deployment": deployment,
            "source_name": source_name,
            "scenario": row.get("scenario"),
            "pipeline_mode": row.get("pipeline_mode"),
            "generated_at": generated_at,
            "artifact": artifact,
            "ok": bool(row.get("ok")),
            "sample_count": row.get("sample_count"),
            "latency_p95_ms": (
                (row.get("latency_ms") or {}).get("p95")
                if isinstance(row.get("latency_ms"), Mapping)
                else None
            ),
            "source_runner_p95_ms": (
                (row.get("source_runner_ms") or {}).get("p95")
                if isinstance(row.get("source_runner_ms"), Mapping)
                else None
            ),
            "pipeline_overhead_p95_ms": (
                (row.get("pipeline_overhead_ms") or {}).get("p95")
                if isinstance(row.get("pipeline_overhead_ms"), Mapping)
                else None
            ),
            "rows_appended_total": row.get("rows_appended_total"),
            "nodes_appended_total": row.get("nodes_appended_total"),
            "edges_appended_total": row.get("edges_appended_total"),
            "changed_graph_rows": publish_scope.get("changed_graph_rows"),
            "expected_delta": row.get("expected_delta") or {},
            "expected_delta_source": row.get("expected_delta_source"),
            "expected_delta_units": row.get("expected_delta_units"),
            "observed_delta": row.get("observed_delta") or {},
            "observed_delta_units": row.get("observed_delta_units"),
            "source_work_units": row.get("source_work_units"),
            "work_amplification_ratio": row.get("work_amplification_ratio"),
            "work_amplification_budget": row.get("work_amplification_budget"),
            "observability_status": row.get("observability_status"),
            "observability_gaps": row.get("observability_gaps") or [],
            "no_change_dominant": bool(row.get("no_change_dominant")),
            "top_widening_stage": publish_scope.get("top_widening_stage"),
            "top_widening_owner": publish_scope.get("top_widening_owner"),
            "top_widening_reason": publish_scope.get("top_widening_reason"),
            "top_widening_elapsed_p95_ms": publish_scope.get(
                "top_widening_elapsed_p95_ms",
            ),
            "publish_lock_wait_p95_ms": publish_scope.get(
                "publish_lock_wait_p95_ms",
            ),
            "publish_lock_hold_p95_ms": publish_scope.get(
                "publish_lock_hold_p95_ms",
            ),
            "environment_contention": bool(
                publish_scope.get("environment_contention"),
            ),
            "environment_contention_stage": publish_scope.get(
                "environment_contention_stage",
            ),
            "environment_contention_reason": publish_scope.get(
                "environment_contention_reason",
            ),
            "readiness_impact": publish_scope.get("readiness_impact"),
            "publish_scope": publish_scope,
            "event_plan": row.get("event_plan") or {},
        }
        progress_rows.append(
            {
                "deployment": deployment,
                "source_name": source_name,
                "source_progress_name": f"{deployment}.{source_name}",
                "key": MICROBENCH_FULL_PIPELINE_PROGRESS_KEY,
                "value": json.dumps(payload, sort_keys=True, default=str),
            }
        )
    if not progress_rows:
        return
    try:
        from okg.substrate.db.connection import connect as db_connect

        with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO okg.source_progress
                  (deployment, source_name, key, value, updated_at)
                VALUES (
                  %(deployment)s, %(source_progress_name)s, %(key)s,
                  %(value)s, now()
                )
                ON CONFLICT (deployment, source_name, key)
                  DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = EXCLUDED.updated_at
                """,
                progress_rows,
            )
            _enqueue_microbench_ledger_progress_dirty_scopes(
                cur,
                deployment=deployment,
                progress_rows=progress_rows,
            )
    except Exception:
        return


def _enqueue_microbench_ledger_progress_dirty_scopes(
    cur: Any,
    *,
    deployment: str,
    progress_rows: Sequence[Mapping[str, str]],
) -> None:
    """Invalidate the source optimization ledger when benchmark proof changes."""

    if not progress_rows:
        return
    cur.execute(
        """
        SELECT COALESCE(max(generation_id), 0)
          FROM okg.graph_generations
         WHERE deployment_name = %(deployment)s
           AND status = 'published'
        """,
        {"deployment": deployment},
    )
    row = cur.fetchone()
    target_generation_id = int((row or [0])[0] or 0)
    view_dependency_hash = hashlib.sha256(
        "\n".join(
            sorted(str(item.get("value") or "") for item in progress_rows)
        ).encode("utf-8")
    ).hexdigest()
    dirty_rows: list[dict[str, Any]] = [
        {
            "definition_id": MICROBENCH_LEDGER_VIEW_DEFINITION_ID,
            "target_generation_id": target_generation_id,
            "scope_key": f"publisher:view:{MICROBENCH_LEDGER_VIEW_ID}",
            "dirty_reason": MICROBENCH_LEDGER_DIRTY_REASON,
            "dependency_kind": MICROBENCH_LEDGER_DIRTY_DEPENDENCY_KIND,
            "dependency_ref": (
                f"source_progress:{deployment}:{MICROBENCH_FULL_PIPELINE_PROGRESS_KEY}"
            ),
            "dependency_hash": view_dependency_hash,
        }
    ]
    for item in progress_rows:
        source_name = str(item.get("source_name") or "")
        if not source_name:
            continue
        payload = str(item.get("value") or "")
        dirty_rows.append(
            {
                "definition_id": MICROBENCH_LEDGER_RELATION_DEFINITION_ID,
                "target_generation_id": target_generation_id,
                "scope_key": source_name,
                "dirty_reason": MICROBENCH_LEDGER_DIRTY_REASON,
                "dependency_kind": MICROBENCH_LEDGER_DIRTY_DEPENDENCY_KIND,
                "dependency_ref": (
                    f"source_progress:{item.get('source_progress_name')}:"
                    f"{MICROBENCH_FULL_PIPELINE_PROGRESS_KEY}"
                ),
                "dependency_hash": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            }
        )
    cur.executemany(
        """
        INSERT INTO okg.derived_dirty_scopes (
          definition_id, target_generation_id, scope_key,
          dirty_reason, dependency_kind, dependency_ref, dependency_hash
        )
        SELECT
          %(definition_id)s, %(target_generation_id)s, %(scope_key)s,
          %(dirty_reason)s, %(dependency_kind)s, %(dependency_ref)s,
          %(dependency_hash)s
        WHERE EXISTS (
          SELECT 1
            FROM okg.derived_artifact_definitions
           WHERE definition_id = %(definition_id)s
        )
        ON CONFLICT (
          definition_id, target_generation_id, scope_key,
          dependency_kind, dependency_ref
        ) DO UPDATE SET
          dirty_reason = EXCLUDED.dirty_reason,
          dependency_hash = EXCLUDED.dependency_hash,
          queued_at = now(),
          consumed_run_id = NULL,
          consumed_at = NULL
        """,
        dirty_rows,
    )


def _source_live(
    live_state: Mapping[str, Any],
    section: str,
    source_name: str,
) -> dict[str, Any]:
    rows = live_state.get(section)
    if not isinstance(rows, Mapping):
        return {}
    item = rows.get(source_name)
    return dict(item) if isinstance(item, Mapping) else {}


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(loaded) if isinstance(loaded, Mapping) else {}
    return {}


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    number = _as_float(value)
    if number is None:
        return None
    return int(number)


__all__ = [
    "MICROBENCH_INVENTORY_SCHEMA",
    "MICROBENCH_OPTIMIZATION_LEDGER_ROWS_SCHEMA",
    "MICROBENCH_OPTIMIZATION_LEDGER_SCHEMA",
    "MICROBENCH_RUN_SCHEMA",
    "MICROBENCH_VIEW_ROWS_SCHEMA",
    "build_source_optimization_ledger",
    "build_microbenchmark_inventory",
    "build_source_sync_microbenchmark_report",
    "microbenchmark_inventory_relation_rows",
    "source_optimization_ledger_relation_rows",
]
