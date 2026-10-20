#!/usr/bin/env python3
"""Extract the standardized metrics CSV for one bench experiment.

Inputs (per experiment <id>):
  - bench/results/<id>.raw.csv               (commit_replay.py output)
  - okg_bench_<id> Postgres DB                (Chronos generation checkpoints
                                               or legacy gens, graph counts,
                                               DBOS workflow timings)

Output:
  - bench/results/<id>.csv  with columns:
    i, commit, subject, files_changed, lines_added, lines_deleted,
    e2e_ms, cycle_ms, nodes_added, edges_added,
    nodes_live, edges_live, gen_id, db_size_bytes,
    wf_a0_ms, wf_setup_ms, wf_ingest_ms, wf_close_ms,
    source_run_count, source_diag_status, source_diag_reason,
    source_total_ms, slowest_source, slowest_phase, slowest_phase_ms,
    top_sources, top_phases,
    projection_ms, enrichment_ms, gate_ms, publish_ms,
    view_materialization_ms, publish_total_ms,         (per-commit publish
                                                        phase decomposition)
    peak_rss_bytes, chronos_segments, chronos_branches (run-level resource)

Phase definitions:
  wf_a0_ms      = workflow.started_at - workflow.created_at
                  (dispatch wait — partition queue + DBOS poller)
  wf_setup_ms   = workflow_output.report.source_sync_workflow_timings_ms
                  .pre_ingest_setup when present; otherwise the legacy
                  setup_probe.started_at - workflow.started_at fallback
  wf_ingest_ms  = workflow_output.report.source_sync_workflow_timings_ms
                  .run_ingest when present; otherwise the legacy terminal
                  DBOS operation fallback
  wf_close_ms   = workflow output post_ingest_total when present; otherwise
                  workflow.updated_at - terminal_step.completed_at
"""
from __future__ import annotations

import csv
import base64
import json
import math
import os
import pickle
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Optional

import psycopg

BENCH = Path(os.environ.get("OKG_BENCH_ROOT", Path.cwd() / "bench")).resolve()
ROOT = BENCH.parent
RESULTS = BENCH / "results"

# Re-use the orchestrator's loader so the schema is in one place.
from okg.bench.orchestrator import (  # noqa: E402
    ENGINE_SENSITIVITY_CONFIG,
    build_engine_sensitivity_plan,
    _load_manifest,
)


AGGREGATE_PHASES = {"total", "source_runner.total"}
# Rollup keys a source-run bottleneck must never name (issue #1325 item 4).
# `_run_source` stamps a whole-call rollup under `source_runner` and republishes
# every SourceRunner label as `source_runner.<label>`, and `stream_walk` /
# `flush_chunks` / `flush_final` / `stream_footprint_tail_flush` / `flush` wrap
# the `flush.*` keys. A rollup is
# by construction >= each of its parts, so ranking the raw map named the same
# phase for every source. Keep this set in step with the `slowest_phase`
# vocabulary documented at `okg.v_source_run_diagnostics`
# (src/okg/substrate/db/views.sql): the view and this extractor publish the
# same column name from the same payload and must not disagree.
_SOURCE_RUN_ROLLUP_LEAVES = (
    "flush",
    "flush_chunks",
    "flush_final",
    "stream_footprint_tail_flush",
    "stream_walk",
    "total",
)
SOURCE_RUN_ROLLUP_PHASES = AGGREGATE_PHASES | {"source_runner"} | {
    prefix + leaf
    # `_source_run_from_workflow_report_result` reads workflow report items,
    # not only progress rows, so a runner map can arrive unprefixed. Exclude
    # both spellings rather than fail open on the bare one.
    for prefix in ("", "source_runner.")
    for leaf in _SOURCE_RUN_ROLLUP_LEAVES
}
# Depth floor for a rankable phase. `source_runner.flush.chronos_apply` is three
# segments; anything deeper is a Chronos backend sub-measurement, and those are
# not a partition of their phase, so ranking one against a sibling phase
# compares unlike things.
SOURCE_RUN_PHASE_MAX_DEPTH = 3
PUBLISH_VIEW_PHASES = (
    "native_views",
    "lenses",
    "view_derived_fact_load",
    "view_cite_retractions",
    "view_derived_fact_apply",
)
NON_DURATION_PHASE_SUFFIXES = (
    ".count",
    "_count",
    "_count_max",
    ".row_count",
    "_row_count",
    ".rows",
    "_rows",
    ".bytes",
    "_bytes",
    ".bytes_read",
    "_bytes_read",
    ".bytes_written",
    "_bytes_written",
    ".size",
    "_size",
    ".marker_batch_size",
    "_batch_size",
    ".operation_count",
    "_operation_count",
    ".operations",
    "_operations",
)
NON_DURATION_PHASE_NAMESPACES = (
    "node_change_class",
    "node_change_fields",
)
NON_DURATION_PHASE_NAMES = {
    "payload_bytes",
    "payload_size",
    "max_payload_bytes",
    "max_payload_size",
    "bytes_read",
    "bytes_written",
    "row_count",
    "rows_read",
    "rows_written",
    "operation_count",
}

REPLAY_TIMING_FIELDS = (
    "bench_reset_started_epoch_ms",
    "bench_reset_completed_epoch_ms",
    "bench_prev_gen_query_ms",
    "bench_reset_ms",
    "bench_wait_new_gen_ms",
    "bench_generation_notify_count",
    "bench_latest_gen_poll_count",
    "bench_latest_gen_query_ms",
    "bench_latest_gen_query_max_ms",
    "bench_queue_drain_ms",
    "bench_queue_poll_count",
    "bench_queue_query_ms",
    "bench_gen_metrics_ms",
    "bench_db_size_ms",
    "bench_preamble_ms",
    "bench_gen_observed_epoch_ms",
)

REPLAY_WORKFLOW_TIMING_FIELDS = (
    "bench_reset_to_workflow_created_ms",
    "bench_reset_to_workflow_started_ms",
    "bench_workflow_created_to_published_ms",
    "bench_workflow_started_to_published_ms",
)


def _experiment(exp_id: str):
    for e in _load_manifest():
        if e.id == exp_id:
            return e
    sensitivity_config = Path(
        os.environ.get("OKG_BENCH_SENSITIVITY_CONFIG")
        or ENGINE_SENSITIVITY_CONFIG
    )
    if sensitivity_config.exists():
        try:
            _plan, experiments = build_engine_sensitivity_plan(sensitivity_config)
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(
                f"could not load sensitivity config {sensitivity_config}: {exc}"
            )
        for e in experiments:
            if e.id == exp_id:
                return e
    raise SystemExit(f"unknown experiment id: {exp_id}")


def _load_raw_csv(path: Path) -> list[dict]:
    if not path.exists():
        sys.exit(f"raw CSV missing: {path}")
    with path.open() as fh:
        return list(csv.DictReader(fh))


def _epoch_ms(value) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return int(datetime.fromisoformat(text).timestamp() * 1000)
    except ValueError:
        return None


def _int_or_none(value) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value) -> Optional[float]:
    try:
        if value in (None, "", "NaN"):
            return None
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number):
        return None
    return number


def _replay_timing_from_raw_row(raw: dict) -> dict[str, float | int | None]:
    """Extract benchmark-side timing fields from a raw replay row.

    Older raw CSVs do not contain these columns; callers get None for those
    fields so old artifacts remain readable and visibly incomplete.
    """

    out: dict[str, float | int | None] = {}
    for field in REPLAY_TIMING_FIELDS:
        value = raw.get(field)
        if field.endswith("_count"):
            out[field] = _int_or_none(value)
        else:
            out[field] = _float_or_none(value)
    # Leak-free e2e window anchored on what the bench ACTUALLY observed
    # (reset start → NOTIFY-observed gen), independent of the chronos
    # checkpoint created_at the extract-side window uses. The gap between this
    # and the checkpoint-anchored window is the post-NOTIFY publish tail that
    # otherwise leaks across adjacent commit windows and shows up as residual.
    observed = out.get("bench_gen_observed_epoch_ms")
    reset_started = out.get("bench_reset_started_epoch_ms")
    out["bench_e2e_observed_window_ms"] = (
        observed - reset_started
        if observed is not None and reset_started is not None
        else None
    )
    return out


def _replay_workflow_timing(
    *,
    replay_timing: dict[str, float | int | None],
    workflow: dict | None,
    generation_published_ms: int | None,
) -> dict[str, float | int | None]:
    """Derive replay producer/workflow boundary timings.

    These fields make the missing wall-clock bucket explicit:
    reset-start → DBOS workflow row creation is watcher/debounce/enqueue
    latency, while workflow-created/started → generation-published is
    the durable workflow plus ingest/publish body.
    """
    reset_started = replay_timing.get("bench_reset_started_epoch_ms")
    if reset_started is None or workflow is None:
        return {field: None for field in REPLAY_WORKFLOW_TIMING_FIELDS}
    created = workflow.get("created")
    started = workflow.get("started")
    out: dict[str, float | int | None] = {
        "bench_reset_to_workflow_created_ms": None,
        "bench_reset_to_workflow_started_ms": None,
        "bench_workflow_created_to_published_ms": None,
        "bench_workflow_started_to_published_ms": None,
    }
    if created is not None:
        out["bench_reset_to_workflow_created_ms"] = created - reset_started
        if generation_published_ms is not None:
            out["bench_workflow_created_to_published_ms"] = (
                generation_published_ms - created
            )
    if started is not None:
        out["bench_reset_to_workflow_started_ms"] = started - reset_started
        if generation_published_ms is not None:
            out["bench_workflow_started_to_published_ms"] = (
                generation_published_ms - started
            )
    return out


def _workflow_pairing_timestamp(workflow: dict) -> int | None:
    created = workflow.get("created")
    if created is not None:
        return int(created)
    started = workflow.get("started")
    if started is not None:
        return int(started)
    return None


def _decode_dbos_workflow_output(
    raw: Any,
    *,
    serialization: str | None,
) -> dict[str, Any]:
    """Decode benchmark-local DBOS workflow output payloads.

    DBOS persists the Python workflow return value as base64-encoded pickle in
    this test harness. The extractor only uses this for local benchmark
    diagnostics, and failures intentionally degrade to an empty payload so old
    or differently serialized DBOS rows remain readable.
    """

    if raw in (None, "", "None"):
        return {}
    if isinstance(raw, Mapping):
        return dict(raw)
    if serialization not in (None, "", "py_pickle"):
        return {}
    try:
        payload = pickle.loads(base64.b64decode(str(raw).encode("ascii")))
    except Exception:  # noqa: BLE001 - diagnostic best effort for old rows.
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _numeric_mapping(raw: Any) -> dict[str, float]:
    if not isinstance(raw, Mapping):
        return {}
    out: dict[str, float] = {}
    for key, value in raw.items():
        if isinstance(value, (int, float)) and math.isfinite(float(value)):
            out[str(key)] = round(float(value), 3)
    return out


def _workflow_ingest_report_fields(
    raw_output: Any,
    *,
    serialization: str | None,
) -> tuple[
    dict[str, float],
    dict[str, float],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    int | None,
]:
    output = _decode_dbos_workflow_output(
        raw_output,
        serialization=serialization,
    )
    report = output.get("report")
    if not isinstance(report, Mapping):
        return {}, {}, [], [], [], None
    # Deterministic DB round-trip count for the whole workflow (per commit),
    # emitted by the ingest service from the active profiling context.
    # count-based-perf-gate / req.counts.emit.
    query_count = _int_or_none(report.get("query_count"))
    timings = _numeric_mapping(report.get("timings_ms"))
    workflow_timings = _numeric_mapping(
        report.get("source_sync_workflow_timings_ms"),
    )
    layers_raw = report.get("layer_timings")
    layers: list[dict[str, Any]] = []
    if isinstance(layers_raw, list):
        for item in layers_raw:
            if isinstance(item, Mapping):
                layers.append(dict(item))
    results_raw = report.get("results")
    results: list[dict[str, Any]] = []
    if isinstance(results_raw, list):
        for item in results_raw:
            if isinstance(item, Mapping):
                results.append(dict(item))
    publishes_raw = report.get("publishes")
    publishes: list[dict[str, Any]] = []
    if isinstance(publishes_raw, list):
        for item in publishes_raw:
            if isinstance(item, Mapping):
                publishes.append(dict(item))
    return timings, workflow_timings, layers, results, publishes, query_count


def _pair_replay_workflow(
    workflows: list[dict],
    *,
    replay_timing: dict[str, float | int | None],
    generation_published_ms: int | None,
    previous_generation_published_ms: int | None = None,
    tolerance_ms: int = 5,
) -> tuple[dict | None, str]:
    """Pair a replay row to the workflow created for that reset window.

    Positional pairing is unsafe after a replay emits baseline/no-change
    workflows or skipped publish cycles: it can assign an older workflow to a
    later commit and produce impossible negative reset→workflow timings. The
    benchmark writes the reset start time and the extractor knows the final
    generation publish time, so those wall-clock boundaries are the durable
    correlation key.
    """

    reset_started = replay_timing.get("bench_reset_started_epoch_ms")
    if reset_started is None:
        return None, "reset_start_missing"
    if generation_published_ms is None:
        return None, "generation_publish_time_missing"
    lower = float(reset_started) - tolerance_ms
    upper = float(generation_published_ms) + tolerance_ms
    if upper < lower:
        return None, "invalid_time_window"

    candidates: list[dict] = []
    for workflow in workflows:
        marker = _workflow_pairing_timestamp(workflow)
        if marker is None:
            continue
        if marker < lower or marker > upper:
            continue
        if (
            previous_generation_published_ms is not None
            and marker <= previous_generation_published_ms + tolerance_ms
        ):
            continue
        candidates.append(workflow)

    if not candidates:
        return None, "workflow_not_in_replay_window"
    candidates.sort(key=lambda item: (
        _workflow_pairing_timestamp(item) or 0,
        str(item.get("workflow_uuid") or ""),
    ))
    if len(candidates) > 1:
        # More than one successful source-sync in one commit's window (e.g. a
        # split sync): the earliest still supplies timing context, but its
        # query_count is only part of the commit's work. The reason marks the
        # row so the count gate refuses it instead of gating a partial count.
        return candidates[0], "ambiguous_time_window"
    return candidates[0], "time_window"


def _stage_bottleneck(timings: dict[str, float]) -> tuple[str | None, float | None]:
    candidates = [
        (phase, value)
        for phase, value in timings.items()
        if _is_terminal_source_phase(phase)
    ]
    if not candidates:
        return None, None
    # Tie-break on the smaller key, matching `ORDER BY ... DESC, kv.key` in
    # `okg.v_source_run_diagnostics`. The two publishers of this column must
    # not disagree on equal timings.
    phase, value = sorted(candidates, key=lambda item: (-item[1], item[0]))[0]
    return phase, value


def _is_terminal_source_phase(phase: str) -> bool:
    """True when `phase` is a rankable leaf of a source-run timing map.

    Rollups and backend sub-measurements are excluded so the reported
    bottleneck discriminates between sources instead of always naming the
    whole-call wrapper. Returning nothing is the honest answer for a run record
    that carries no breakdown.
    """
    if phase in SOURCE_RUN_ROLLUP_PHASES:
        return False
    if len(phase.split(".")) > SOURCE_RUN_PHASE_MAX_DEPTH:
        return False
    return _is_duration_phase(phase)


def _is_duration_phase(phase: str) -> bool:
    if phase in AGGREGATE_PHASES:
        return False
    if phase in NON_DURATION_PHASE_NAMES:
        return False
    parts = phase.split(".")
    if any(namespace in parts for namespace in NON_DURATION_PHASE_NAMESPACES):
        return False
    return not any(phase.endswith(suffix) for suffix in NON_DURATION_PHASE_SUFFIXES)


def _as_timing_map(value) -> dict[str, float]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, float] = {}
    for key, raw in value.items():
        number = _float_or_none(raw)
        if number is not None:
            out[str(key)] = number
    return out


def _progress_registry_source(
    deployment_name: str,
    source_progress_name: str,
) -> str:
    prefix = f"{deployment_name}."
    if source_progress_name.startswith(prefix):
        return source_progress_name[len(prefix):]
    return source_progress_name


def _source_run_from_progress_row(
    *,
    deployment_name: str,
    source_progress_name: str,
    key: str,
    value: str,
    updated_at,
) -> dict | None:
    try:
        payload = json.loads(value)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    timings = _as_timing_map(payload.get("timings_ms"))
    slowest_phase, slowest_phase_ms = _stage_bottleneck(timings)
    source_name = str(payload.get("source_name") or source_progress_name)
    registry_source = str(
        payload.get("registry_source_name")
        or _progress_registry_source(deployment_name, source_progress_name)
    )
    run_id = str(
        payload.get("run_id")
        or key.removeprefix("sync.stage_timings.run.")
    )
    return {
        "deployment": deployment_name,
        "source_name": source_name,
        "registry_source_name": registry_source,
        "run_id": run_id,
        "status": payload.get("status"),
        "run_mode": payload.get("run_mode"),
        "timing_status": payload.get("timing_status") or "unavailable",
        "timing_unavailable_reason": payload.get(
            "timing_unavailable_reason"
        ),
        "updated_ms": _epoch_ms(updated_at),
        "updated_at": str(updated_at) if updated_at is not None else None,
        "input_count": _int_or_none(payload.get("input_count")) or 0,
        "output_count": _int_or_none(payload.get("output_count")) or 0,
        "nodes_appended": _int_or_none(payload.get("nodes_appended")) or 0,
        "edges_appended": _int_or_none(payload.get("edges_appended")) or 0,
        "nodes_retracted": _int_or_none(payload.get("nodes_retracted")) or 0,
        "edges_retracted": _int_or_none(payload.get("edges_retracted")) or 0,
        "timings_ms": timings,
        "source_stats": (
            payload.get("source_stats")
            if isinstance(payload.get("source_stats"), dict) else {}
        ),
        "total_ms": timings.get("total"),
        "source_runner_ms": timings.get("source_runner"),
        "slowest_phase": slowest_phase,
        "slowest_phase_ms": slowest_phase_ms,
    }


def _source_run_from_workflow_report_result(
    *,
    deployment_name: str,
    workflow_uuid: str,
    index: int,
    item: Mapping[str, Any],
    updated_ms: int | None,
) -> dict | None:
    if not isinstance(item, Mapping):
        return None
    timings = _as_timing_map(item.get("timings_ms"))
    slowest_phase, slowest_phase_ms = _stage_bottleneck(timings)
    registry_source = str(
        item.get("name")
        or item.get("registry_source_name")
        or item.get("source")
        or ""
    )
    if not registry_source:
        return None
    source_name = str(
        item.get("source")
        or item.get("source_name")
        or f"{deployment_name}.{registry_source}"
    )
    return {
        "deployment": deployment_name,
        "source_name": source_name,
        "registry_source_name": registry_source,
        "run_id": str(
            item.get("run_id") or f"workflow.{workflow_uuid}.source.{index}"
        ),
        "status": item.get("status"),
        "run_mode": item.get("run_mode"),
        "timing_status": item.get("timing_status") or "available",
        "timing_unavailable_reason": item.get(
            "timing_unavailable_reason"
        ),
        "updated_ms": updated_ms,
        "updated_at": None,
        "input_count": _int_or_none(item.get("input_count")) or 0,
        "output_count": (
            (_int_or_none(item.get("nodes_appended")) or 0)
            + (_int_or_none(item.get("edges_appended")) or 0)
            + (_int_or_none(item.get("retract_n")) or 0)
            + (_int_or_none(item.get("retract_e")) or 0)
        ),
        "nodes_appended": _int_or_none(item.get("nodes_appended")) or 0,
        "edges_appended": _int_or_none(item.get("edges_appended")) or 0,
        "nodes_retracted": _int_or_none(item.get("retract_n")) or 0,
        "edges_retracted": _int_or_none(item.get("retract_e")) or 0,
        "timings_ms": timings,
        "source_stats": (
            item.get("source_stats")
            if isinstance(item.get("source_stats"), dict) else {}
        ),
        "total_ms": timings.get("total"),
        "source_runner_ms": timings.get("source_runner"),
        "slowest_phase": slowest_phase,
        "slowest_phase_ms": slowest_phase_ms,
    }


def _source_run_diagnostics(cur, deployment_name: str) -> list[dict]:
    cur.execute(
        """
        SELECT source_name, key, value, updated_at
          FROM okg.source_progress
         WHERE deployment = %s
           AND key LIKE 'sync.stage_timings.run.%%'
         ORDER BY updated_at, key
        """,
        (deployment_name,),
    )
    out: list[dict] = []
    for source_name, key, value, updated_at in cur.fetchall():
        item = _source_run_from_progress_row(
            deployment_name=deployment_name,
            source_progress_name=str(source_name),
            key=str(key),
            value=value,
            updated_at=updated_at,
        )
        if item is not None:
            out.append(item)
    return out


# --- publish-cycle per-phase timings (bench-okg-internals) -----------------
# Maps the publisher's flat timings_ms phase keys into the five
# substrate-internal phases the bench reports. A phase is the sum of its
# present component keys; a phase with NO present component is reported NaN
# (not 0) so a skipped phase is distinguishable from a zero-cost one.
_PUBLISH_PHASE_COMPONENTS = {
    "projection_ms": ("extractor", "derived_fact_load", "derived_fact_apply"),
    "enrichment_ms": ("enrichers",),
    "gate_ms": ("gates", "consistency_gates"),
    "publish_ms": (
        "checkpoint_generation", "notify",
        "nomos_policy_pin", "nomos_generation_publish",
    ),
    "view_materialization_ms": (
        "native_views", "lenses", "view_derived_fact_load",
        "view_cite_retractions", "view_derived_fact_apply",
        "attribute_effective_view_materialize",
        "attribute_effective_attrs_materialize", "lens_promotion",
    ),
    # Fixed per-publish setup overhead, re-paid on EVERY per-layer publish
    # (Chronos store construct + ensure_branch + connection lock + watermark
    # / catalog reads + gate cache key). Surfaced explicitly because it is the
    # single largest publish cost the old 5-bucket rollup dropped on the floor
    # (`store_setup` alone ~13% of publish time).
    "setup_overhead_ms": (
        "store_setup", "branch_checkout", "locked_connection",
        "fact_watermarks", "catalog_version", "gate_cache_key",
        "producer_profiles",
    ),
    # Pre-view delta/work probes (scoped + generation delta-count queries run
    # before view materialization).
    "delta_probe_ms": ("delta_counts_before_views", "workload_delta_probe"),
    # console-count-rollup (#444 slice 2): the maintained-derivation publish-stage
    # cost. "console_count_rollup" and "ontology_rollup" are the flat timings_ms keys
    # the publisher emits for the two maintain stages; the ontology one runs on every
    # deployment since #1843, so leaving it out silently moved that cost into
    # unattributed_ms. O(changed) per cycle, so it lives at per-commit
    # granularity only (the run-level aggregate sums view/total, not this). None->NaN
    # (absence is not zero cost).
    "maintained_derivation_ms": ("console_count_rollup", "ontology_rollup"),
}


def _publish_stage_timings(cur, deployment_name: str) -> dict[str, dict]:
    """Read persisted publish-cycle timings keyed by generation_id.

    Populated only when the worker ran with OKG_PERSIST_PUBLISH_TIMINGS=1
    (the bench orchestrator sets it). Returns {generation_id_str: payload}.
    """
    cur.execute(
        """
        SELECT value
          FROM okg.source_progress
         WHERE deployment = %s
           AND key LIKE 'sync.publish_stage_timings.run.%%'
        """,
        (deployment_name,),
    )
    out: dict[str, dict] = {}
    for (value,) in cur.fetchall():
        try:
            payload = value if isinstance(value, dict) else json.loads(value)
        except (TypeError, ValueError):
            continue
        gen = payload.get("generation_id")
        if gen:
            out[str(gen)] = payload
    return out


def _rollup_publish_phases(payload: Optional[dict]) -> dict:
    """Roll a publish timings payload into the reported phases + total.

    Every component key is assigned to exactly one phase bucket; whatever
    `total` the buckets do not cover lands in `unattributed_ms`, so the buckets
    plus `unattributed_ms` always reconcile to `publish_total_ms`. This makes
    the publish decomposition EXHAUSTIVE: a new publisher phase shows up as
    `unattributed_ms` instead of silently vanishing from the rollup (the old
    5-bucket map captured only ~65% of publish time).
    Values are float ms or None (written as NaN)."""
    keys = ("projection_ms", "enrichment_ms", "gate_ms", "publish_ms",
            "view_materialization_ms", "setup_overhead_ms", "delta_probe_ms",
            "maintained_derivation_ms", "unattributed_ms", "publish_total_ms")
    if not payload:
        return {k: None for k in keys}
    timings = payload.get("timings_ms") or {}

    def _num(k):
        v = timings.get(k)
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    out: dict[str, Optional[float]] = {}
    for phase, comps in _PUBLISH_PHASE_COMPONENTS.items():
        vals = [_num(c) for c in comps if _num(c) is not None]
        out[phase] = round(sum(vals), 3) if vals else None
    total = _num("total")
    if total is None:
        raw_total = payload.get("total_duration_ms")
        try:
            total = float(raw_total) if raw_total is not None else None
        except (TypeError, ValueError):
            total = None
    # Exhaustiveness net: time in `total` not captured by any named bucket.
    bucket_sum = sum(v for v in out.values() if v is not None)
    out["unattributed_ms"] = (
        round(total - bucket_sum, 3) if total is not None else None
    )
    out["publish_total_ms"] = round(total, 3) if total is not None else None
    return out


def _chronos_resource_counts(cur) -> tuple[Optional[int], Optional[int]]:
    """Final chronos segment + branch counts (a run-level resource metric)."""
    def _count(sql: str) -> Optional[int]:
        try:
            cur.execute(sql)
            return int(cur.fetchone()[0])
        except Exception:  # noqa: BLE001 - metric absence is non-fatal
            return None

    return (
        _count("SELECT count(*) FROM _chronos_branch_interval_segments"),
        _count("SELECT count(*) FROM _chronos_branch_interval_branches"),
    )


def _read_peak_rss_bytes(exp) -> Optional[int]:
    """Peak worker RSS for the run, written by cmd_run to a sidecar."""
    p = exp.run_metrics_json
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text())
        v = data.get("peak_rss_bytes")
        return int(v) if v is not None else None
    except (ValueError, TypeError, OSError):
        return None


def _publish_timing_from_progress_row(
    *,
    key: str,
    value: str,
    updated_at,
) -> dict | None:
    try:
        payload = json.loads(value)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("schema") != "okg.publish.stage_timings.run.v1":
        return None
    timings = _as_timing_map(payload.get("timings_ms"))
    total_ms = _float_or_none(payload.get("total_duration_ms"))
    if total_ms is None:
        total_ms = timings.get("total")
    view_ms = sum(
        float(timings.get(phase) or 0.0)
        for phase in PUBLISH_VIEW_PHASES
    )
    return {
        "key": key,
        "updated_ms": _epoch_ms(updated_at),
        "updated_at": str(updated_at) if updated_at is not None else None,
        "label": str(payload.get("label") or ""),
        "status": str(payload.get("status") or ""),
        "generation_id": _coerce_gen_id(payload.get("generation_id")),
        "publish_skipped": bool(payload.get("publish_skipped")),
        "reason": payload.get("reason"),
        "native_view_refresh": payload.get("native_view_refresh"),
        "attribute_effective_view_refresh": payload.get(
            "attribute_effective_view_refresh"
        ),
        "native_views": payload.get("native_views"),
        "lenses": payload.get("lenses"),
        "enricher_governance": payload.get("enricher_governance"),
        "extractor_edges_emitted": payload.get("extractor_edges_emitted"),
        "derived_fact_summary": payload.get("derived_fact_summary"),
        "total_ms": total_ms,
        "view_materialization_ms": view_ms,
        "timings_ms": timings,
    }


def _publish_timing_from_workflow_report_item(
    item: Mapping[str, Any],
    *,
    workflow_uuid: str,
    index: int,
    updated_ms: int | None,
) -> dict | None:
    if not isinstance(item, Mapping):
        return None
    timings = _as_timing_map(item.get("timings_ms"))
    total_ms = _float_or_none(item.get("total_duration_ms"))
    if total_ms is None:
        total_ms = timings.get("total")
    view_ms = sum(
        float(timings.get(phase) or 0.0)
        for phase in PUBLISH_VIEW_PHASES
    )
    return {
        "key": f"workflow.{workflow_uuid}.publish.{index}",
        "updated_ms": updated_ms,
        "updated_at": None,
        "label": str(item.get("label") or ""),
        "status": str(item.get("status") or ""),
        "generation_id": _coerce_gen_id(item.get("generation_id")),
        "publish_skipped": bool(item.get("publish_skipped")),
        "reason": item.get("reason"),
        "native_view_refresh": item.get("native_view_refresh"),
        "attribute_effective_view_refresh": item.get(
            "attribute_effective_view_refresh"
        ),
        "native_views": item.get("native_views"),
        "lenses": item.get("lenses"),
        "enricher_governance": item.get("enricher_governance"),
        "extractor_edges_emitted": item.get("extractor_edges_emitted"),
        "derived_fact_summary": item.get("derived_fact_summary"),
        "total_ms": total_ms,
        "view_materialization_ms": view_ms,
        "timings_ms": timings,
    }


def _publish_stage_timing_events(cur, deployment_name: str) -> list[dict]:
    cur.execute(
        """
        SELECT key, value, updated_at
          FROM okg.source_progress
         WHERE deployment = %s
           AND key LIKE 'sync.publish_stage_timings.run.%%'
         ORDER BY updated_at, key
        """,
        (deployment_name,),
    )
    out: list[dict] = []
    for key, value, updated_at in cur.fetchall():
        item = _publish_timing_from_progress_row(
            key=str(key),
            value=value,
            updated_at=updated_at,
        )
        if item is not None:
            out.append(item)
    return out


def _aggregate_publish_timings(
    publish_timings: list[dict],
    *,
    start_ms: int | None,
    end_ms: int | None,
    final_gen_id: int | str | None,
    previous_gen_id: int | str | None = None,
) -> dict:
    if start_ms is None or end_ms is None:
        return {
            "publish_diag_status": "unavailable",
            "publish_diag_reason": "missing_time_window",
            "publish_event_count": 0,
            "publish_skipped_count": 0,
            "published_count": 0,
            "intermediate_publish_count": 0,
            "publish_total_ms": None,
            "final_publish_total_ms": None,
            "intermediate_publish_total_ms": None,
            "view_materialization_ms": None,
            "intermediate_view_materialization_ms": None,
            "publish_labels": "",
            "publishes": [],
        }
    selected = [
        item for item in publish_timings
        if item.get("updated_ms") is not None
        and start_ms < int(item["updated_ms"]) <= end_ms
    ]
    if previous_gen_id is not None:
        selected = [
            item for item in selected
            if item.get("generation_id") != previous_gen_id
        ]
    if final_gen_id is not None:
        selected_ids = {id(item) for item in selected}
        selected.extend(
            item for item in publish_timings
            if item.get("generation_id") == final_gen_id
            and id(item) not in selected_ids
        )
    if not selected:
        return {
            "publish_diag_status": "unavailable",
            "publish_diag_reason": "no_publish_timings_in_generation_window",
            "publish_event_count": 0,
            "publish_skipped_count": 0,
            "published_count": 0,
            "intermediate_publish_count": 0,
            "publish_total_ms": 0.0,
            "final_publish_total_ms": None,
            "intermediate_publish_total_ms": 0.0,
            "view_materialization_ms": 0.0,
            "intermediate_view_materialization_ms": 0.0,
            "publish_labels": "",
            "publishes": [],
        }
    published = [
        item for item in selected
        if item.get("status") == "published"
        and not item.get("publish_skipped")
    ]
    skipped = [item for item in selected if item.get("publish_skipped")]
    final_publishes = [
        item for item in published
        if final_gen_id is not None and item.get("generation_id") == final_gen_id
    ]
    intermediate = [
        item for item in published
        if not (
            final_gen_id is not None
            and item.get("generation_id") == final_gen_id
        )
    ]

    def _sum(items: list[dict], key: str) -> float:
        return sum(float(item.get(key) or 0.0) for item in items)

    label_counts = Counter(str(item.get("label") or "") for item in selected)
    return {
        "publish_diag_status": "available",
        "publish_diag_reason": "time_window",
        "publish_event_count": len(selected),
        "publish_skipped_count": len(skipped),
        "published_count": len(published),
        "intermediate_publish_count": len(intermediate),
        "publish_total_ms": _sum(published, "total_ms"),
        "final_publish_total_ms": (
            _sum(final_publishes, "total_ms") if final_publishes else None
        ),
        "intermediate_publish_total_ms": _sum(intermediate, "total_ms"),
        "view_materialization_ms": _sum(
            published, "view_materialization_ms",
        ),
        "intermediate_view_materialization_ms": _sum(
            intermediate, "view_materialization_ms",
        ),
        "publish_labels": ";".join(
            f"{label}={count}"
            for label, count in sorted(label_counts.items())
        ),
        "publishes": selected,
    }


def _stats(values: list[float]) -> dict:
    return {
        "n": len(values),
        "p50_ms": _percentile(values, 0.50),
        "p95_ms": _percentile(values, 0.95),
        "max_ms": max(values) if values else None,
        "sum_ms": round(sum(values), 3),
    }


def _publish_run_summary(publish_timings: list[dict]) -> dict:
    by_label: defaultdict[str, list[float]] = defaultdict(list)
    by_stage: defaultdict[str, list[float]] = defaultdict(list)
    skipped_count = 0
    published_count = 0
    for item in publish_timings:
        if item.get("publish_skipped"):
            skipped_count += 1
            continue
        if item.get("status") != "published":
            continue
        published_count += 1
        label = str(item.get("label") or "publish")
        total = _float_or_none(item.get("total_ms"))
        if total is not None:
            by_label[label].append(total)
        for stage, value in (item.get("timings_ms") or {}).items():
            if not _is_duration_phase(str(stage)):
                continue
            number = _float_or_none(value)
            if number is not None:
                by_stage[str(stage)].append(number)
    return {
        "publish_event_count": len(publish_timings),
        "published_count": published_count,
        "publish_skipped_count": skipped_count,
        "labels": {
            label: _stats(values)
            for label, values in sorted(by_label.items())
        },
        "stages": {
            stage: _stats(values)
            for stage, values in sorted(by_stage.items())
        },
    }


def _format_ranked(items: list[tuple[str, float]], *, limit: int = 3) -> str:
    return ";".join(
        f"{name}={value:.3f}"
        for name, value in items[:limit]
    )


def _aggregate_source_runs(
    source_runs: list[dict],
    *,
    start_ms: int | None,
    end_ms: int | None,
) -> dict:
    if start_ms is None or end_ms is None:
        return {
            "source_run_count": 0,
            "source_diag_status": "unavailable",
            "source_diag_reason": "missing_time_window",
            "source_total_ms": None,
            "chronos_apply_ms": None,
            "slowest_source": None,
            "slowest_phase": None,
            "slowest_phase_ms": None,
            "top_sources": "",
            "top_phases": "",
            "runs": [],
        }
    selected = [
        run for run in source_runs
        if run.get("updated_ms") is not None
        and start_ms < int(run["updated_ms"]) <= end_ms
    ]
    if not selected:
        return {
            "source_run_count": 0,
            "source_diag_status": "unavailable",
            "source_diag_reason": "no_source_runs_in_generation_window",
            "source_total_ms": 0.0,
            "chronos_apply_ms": None,
            "slowest_source": None,
            "slowest_phase": None,
            "slowest_phase_ms": None,
            "top_sources": "",
            "top_phases": "",
            "runs": [],
        }
    source_totals: defaultdict[str, float] = defaultdict(float)
    phase_totals: defaultdict[str, float] = defaultdict(float)
    slowest = {
        "source": None,
        "phase": None,
        "ms": -1.0,
    }
    total_ms = 0.0
    missing_timing = 0
    for run in selected:
        source = str(run.get("registry_source_name") or run.get("source_name"))
        run_total = _float_or_none(run.get("total_ms"))
        if run_total is None:
            run_total = _float_or_none(run.get("source_runner_ms"))
        if run_total is None:
            # No timing recorded for this run. Do NOT silently treat it as a
            # zero-cost run: summing a 0 under an "available" label would
            # under-count source_total_ms and mask the data loss (a P0
            # silent-fallback). Skip it and flag the window partial-coverage.
            missing_timing += 1
            continue
        total_ms += run_total
        source_totals[source] += run_total
        for phase, value in (run.get("timings_ms") or {}).items():
            # Same vocabulary as `slowest_phase` below. Ranking rollups here
            # would make `top_phases` read `source_runner;
            # source_runner.stream_walk; source_runner.flush_chunks` -- three
            # wrappers of the same work -- while the column beside it names a
            # real phase (issue #1325 item 4).
            if not _is_terminal_source_phase(str(phase)):
                continue
            phase_totals[str(phase)] += float(value)
        phase = run.get("slowest_phase")
        phase_ms = _float_or_none(run.get("slowest_phase_ms"))
        if phase and phase_ms is not None and phase_ms > slowest["ms"]:
            slowest = {
                "source": source,
                "phase": phase,
                "ms": phase_ms,
            }
    top_sources = sorted(
        source_totals.items(), key=lambda item: (-item[1], item[0])
    )
    top_phases = sorted(
        phase_totals.items(), key=lambda item: (-item[1], item[0])
    )
    # Graph-delta-write time for this commit: the source-side flush that
    # applies the node/edge delta into the Chronos store
    # (`source_runner.flush.chronos_apply`), summed across the commit's source
    # runs (its dependency layers). This is the "useful work" numerator of
    # work_ratio — the real graph write the per-commit cost is supposed to be
    # spent on. count-based-perf-gate / req.counts.emit.
    chronos_apply_ms = sum(
        value for phase, value in phase_totals.items()
        if str(phase).split(".")[-1] == "chronos_apply"
    )
    return {
        "source_run_count": len(selected),
        "source_diag_status": (
            "available" if missing_timing == 0 else "partial"),
        "source_diag_reason": (
            "time_window" if missing_timing == 0
            else f"missing_timing_for_{missing_timing}_of_{len(selected)}_runs"),
        "source_total_ms": total_ms,
        "chronos_apply_ms": chronos_apply_ms,
        "slowest_source": slowest["source"],
        "slowest_phase": slowest["phase"],
        "slowest_phase_ms": (
            slowest["ms"] if slowest["ms"] >= 0 else None
        ),
        "top_sources": _format_ranked(top_sources),
        "top_phases": _format_ranked(top_phases),
        "runs": selected,
    }


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(len(ordered) * q) - 1))
    return ordered[idx]


def _source_run_summary(source_runs: list[dict]) -> dict:
    by_source: defaultdict[str, list[float]] = defaultdict(list)
    by_phase: defaultdict[str, list[float]] = defaultdict(list)
    for run in source_runs:
        source = str(run.get("registry_source_name") or run.get("source_name"))
        total = _float_or_none(run.get("total_ms"))
        if total is None:
            total = _float_or_none(run.get("source_runner_ms"))
        if total is not None:
            by_source[source].append(total)
        for phase, value in (run.get("timings_ms") or {}).items():
            if not _is_duration_phase(str(phase)):
                continue
            by_phase[str(phase)].append(float(value))
    return {
        "sources": {
            source: {
                "n": len(values),
                "p50_ms": _percentile(values, 0.50),
                "p95_ms": _percentile(values, 0.95),
                "max_ms": max(values) if values else None,
            }
            for source, values in sorted(by_source.items())
        },
        "phases": {
            phase: {
                "n": len(values),
                "p50_ms": _percentile(values, 0.50),
                "p95_ms": _percentile(values, 0.95),
                "max_ms": max(values) if values else None,
            }
            for phase, values in sorted(by_phase.items())
        },
    }


def _measurement_id(*parts: object) -> str:
    import hashlib

    text = json.dumps(parts, sort_keys=True, default=str)
    return "benchmark:source-sync-diagnostics:" + hashlib.sha256(
        text.encode("utf-8")
    ).hexdigest()[:20]


def _diagnostic_measurements(
    *,
    exp_id: str,
    summary: dict,
    publish_summary: dict | None = None,
) -> list[dict]:
    measurements: list[dict] = []
    for source, stats in (summary.get("sources") or {}).items():
        for metric, key, unit, percentile in (
            ("source_total_p50_ms", "p50_ms", "ms", "p50"),
            ("source_total_p95_ms", "p95_ms", "ms", "p95"),
            ("source_total_max_ms", "max_ms", "ms", "max"),
            ("source_run_count", "n", "runs", ""),
        ):
            value = stats.get(key)
            if value is None:
                continue
            measurements.append({
                "measurement_id": _measurement_id(
                    exp_id, "source", source, metric,
                ),
                "scenario": f"source_sync.{source}",
                "metric": metric,
                "value": value,
                "unit": unit,
                "status": "informational",
                "source_name": source,
                "stage_name": "source_runner",
                "percentile": percentile,
            })
    for phase, stats in (summary.get("phases") or {}).items():
        for metric, key, unit, percentile in (
            ("phase_p50_ms", "p50_ms", "ms", "p50"),
            ("phase_p95_ms", "p95_ms", "ms", "p95"),
            ("phase_max_ms", "max_ms", "ms", "max"),
            ("phase_sample_count", "n", "runs", ""),
        ):
            value = stats.get(key)
            if value is None:
                continue
            measurements.append({
                "measurement_id": _measurement_id(
                    exp_id, "phase", phase, metric,
                ),
                "scenario": f"source_sync.phase.{phase}",
                "metric": metric,
                "value": value,
                "unit": unit,
                "status": "informational",
                "source_name": "",
                "stage_name": phase,
                "percentile": percentile,
            })
    if publish_summary:
        for label, stats in (publish_summary.get("labels") or {}).items():
            for metric, key, unit, percentile in (
                ("publish_label_p50_ms", "p50_ms", "ms", "p50"),
                ("publish_label_p95_ms", "p95_ms", "ms", "p95"),
                ("publish_label_max_ms", "max_ms", "ms", "max"),
                ("publish_label_sum_ms", "sum_ms", "ms", "sum"),
                ("publish_label_count", "n", "runs", ""),
            ):
                value = stats.get(key)
                if value is None:
                    continue
                measurements.append({
                    "measurement_id": _measurement_id(
                        exp_id, "publish_label", label, metric,
                    ),
                    "scenario": f"source_sync.publish.label.{label}",
                    "metric": metric,
                    "value": value,
                    "unit": unit,
                    "status": "informational",
                    "source_name": "",
                    "stage_name": label,
                    "percentile": percentile,
                })
        for stage, stats in (publish_summary.get("stages") or {}).items():
            if not _is_duration_phase(str(stage)):
                continue
            for metric, key, unit, percentile in (
                ("publish_stage_p50_ms", "p50_ms", "ms", "p50"),
                ("publish_stage_p95_ms", "p95_ms", "ms", "p95"),
                ("publish_stage_max_ms", "max_ms", "ms", "max"),
                ("publish_stage_sum_ms", "sum_ms", "ms", "sum"),
                ("publish_stage_count", "n", "runs", ""),
            ):
                value = stats.get(key)
                if value is None:
                    continue
                measurements.append({
                    "measurement_id": _measurement_id(
                        exp_id, "publish_stage", stage, metric,
                    ),
                    "scenario": f"source_sync.publish.stage.{stage}",
                    "metric": metric,
                    "value": value,
                    "unit": unit,
                    "status": "informational",
                    "source_name": "",
                    "stage_name": stage,
                    "percentile": percentile,
                })
    return measurements


def _coerce_gen_id(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == "None":
        return None
    return int(text) if text.isdigit() else text


def _gen_metadata(
    cur, deployment_name: str,
) -> list[tuple[int | str, int, int, Optional[int], Optional[int]]]:
    """Return [(gen_id, created_ms, published_ms), ...] for every
    published gen of the given deployment, in chronological order."""
    cur.execute(
        "SELECT to_regclass('public._chronos_branch_interval_checkpoints')"
    )
    if cur.fetchone()[0] is not None:
        cur.execute(
            """
            SELECT checkpoint_id,
                   created_at,
                   metadata::jsonb,
                   CASE
                     WHEN metadata::jsonb->>'target_generation_id' ~ '^[0-9]+$'
                     THEN (metadata::jsonb->>'target_generation_id')::numeric
                     ELSE NULL
                   END AS target_generation_id
              FROM public._chronos_branch_interval_checkpoints
             WHERE metadata::jsonb->>'kind' = 'okg_generation'
               AND metadata::jsonb->>'deployment_name' = %s
               AND metadata::jsonb->>'status' = 'published'
             ORDER BY target_generation_id NULLS LAST,
                      created_at,
                      checkpoint_id;
            """,
            (deployment_name,),
        )
        out = []
        for checkpoint_id, checkpoint_created, metadata, _target in cur.fetchall():
            created_ms = (
                _epoch_ms(metadata.get("created_at"))
                or _epoch_ms(checkpoint_created)
                or 0
            )
            published_ms = _epoch_ms(checkpoint_created) or created_ms
            out.append((
                str(checkpoint_id),
                created_ms,
                published_ms,
                _int_or_none(metadata.get("node_count")),
                _int_or_none(metadata.get("edge_count")),
            ))
        if out:
            return out

    cur.execute(
        """
        SELECT generation_id,
               (extract(epoch from created_at) * 1000)::bigint AS c_ms,
               (extract(epoch from published_at) * 1000)::bigint AS p_ms
          FROM okg.graph_generations
         WHERE deployment_name = %s
           AND status = 'published'
         ORDER BY generation_id;
        """,
        (deployment_name,),
    )
    return [(int(g), int(c), int(p), None, None) for g, c, p in cur.fetchall()]


def _live_counts_at(cur, gen_id: int | str) -> tuple[int, int]:
    # Chronos checkpoint ids ("okg:...") resolve directly; a numeric id
    # resolves as the literal "generation:<int>" checkpoint and raises
    # KeyError if no such checkpoint exists. The legacy bitemporal live
    # tables no longer exist post-cutover.
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(
        cur.connection, ensure_schema=False,
    )
    graph = store.checkout_generation(gen_id)
    nodes = graph.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
    edges = graph.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]
    return int(nodes or 0), int(edges or 0)


def _read_baseline_gen(exp):
    """Read baseline_gen.txt that commit_replay.py wrote alongside the
    raw CSV. Returns None if the file is missing (older runs)."""
    p = exp.raw_csv.parent / exp.id / "baseline_gen.txt"
    if not p.exists():
        return None
    try:
        return _coerce_gen_id(p.read_text())
    except OSError:
        return None


def _workflow_phases(
    cur, deployment_name: str,
) -> list[dict]:
    """Return one dict per source_sync workflow (chrono order) with
    the timestamps needed to compute dispatch/setup/ingest/close.

    Keys, all in epoch_ms:
        created               workflow row INSERT time
        started               body started exec time
        updated               final row UPDATE (DBOS done)
        setup_probe_started   started_at of okg_source_sync_setup_done step (if present)
        setup_probe_completed completed_at of same step (if present)
        end_step_started      started_at of LAST non-probe step in the workflow
        end_step_completed    completed_at of the LAST step
    """
    cur.execute(
        """
        WITH terminal_candidates AS (
          -- Within each workflow, rank non-probe steps by function_id.
          -- Some DBOS versions/paths do not store DBOS.setEvent in
          -- operation_outputs; in that case there is no terminal row
          -- and ingest/close phase splits are unavailable.
          SELECT o.workflow_uuid,
                 o.function_id,
                 o.function_name,
                 o.started_at_epoch_ms   AS s,
                 o.completed_at_epoch_ms AS c,
                 ROW_NUMBER() OVER (
                   PARTITION BY o.workflow_uuid
                   ORDER BY o.function_id DESC
                 ) AS rn_desc
            FROM dbos.operation_outputs o
           WHERE o.function_name NOT IN (
                 'okg_source_sync_observed_at',
                 'okg_source_sync_setup_done'
           )
        ),
        end_step AS (
          SELECT workflow_uuid, s AS end_started, c AS end_completed
            FROM terminal_candidates WHERE rn_desc = 1
        ),
        setup_probe AS (
          SELECT workflow_uuid,
                 started_at_epoch_ms   AS setup_started,
                 completed_at_epoch_ms AS setup_completed
            FROM dbos.operation_outputs
           WHERE function_name = 'okg_source_sync_setup_done'
        )
        SELECT w.workflow_uuid,
               w.created_at,
               COALESCE(w.started_at_epoch_ms, w.created_at) AS started,
               w.updated_at,
               sp.setup_started, sp.setup_completed,
               es.end_started,   es.end_completed,
               w.output, w.serialization
         FROM dbos.workflow_status w
          LEFT JOIN setup_probe sp ON sp.workflow_uuid = w.workflow_uuid
          LEFT JOIN end_step    es ON es.workflow_uuid = w.workflow_uuid
         WHERE w.name = 'okg_source_sync'
           AND w.status = 'SUCCESS'
         ORDER BY w.created_at;
        """,
    )
    out = []
    for row in cur.fetchall():
        (_uuid, created, started, updated,
         setup_s, setup_c, end_s, end_c,
         output_raw, serialization) = row
        (
            ingest_timings,
            workflow_timings,
            layer_timings,
            source_reports,
            publish_reports,
            workflow_query_count,
        ) = _workflow_ingest_report_fields(
            output_raw,
            serialization=str(serialization or ""),
        )
        source_events = [
            event
            for index, item in enumerate(source_reports, start=1)
            if (
                event := _source_run_from_workflow_report_result(
                    deployment_name=deployment_name,
                    workflow_uuid=str(_uuid),
                    index=index,
                    item=item,
                    updated_ms=int(updated) if updated is not None else None,
                )
            ) is not None
        ]
        publish_events = [
            event
            for index, item in enumerate(publish_reports, start=1)
            if (
                event := _publish_timing_from_workflow_report_item(
                    item,
                    workflow_uuid=str(_uuid),
                    index=index,
                    updated_ms=int(updated) if updated is not None else None,
                )
            ) is not None
        ]
        out.append({
            "workflow_uuid": str(_uuid),
            "created": int(created),
            "started": int(started),
            "updated": int(updated),
            "setup_probe_started":   int(setup_s) if setup_s   is not None else None,
            "setup_probe_completed": int(setup_c) if setup_c   is not None else None,
            "end_step_started":      int(end_s)   if end_s     is not None else None,
            "end_step_completed":    int(end_c)   if end_c     is not None else None,
            "ingest_timings_ms": ingest_timings,
            "workflow_timings_ms": workflow_timings,
            "ingest_layer_timings": layer_timings,
            "source_report_events": source_events,
            "publish_report_events": publish_events,
            "query_count": workflow_query_count,
        })
    return out


def extract(exp_id: str) -> None:
    exp = _experiment(exp_id)
    raw_csv = exp.raw_csv
    out_csv = RESULTS / f"{exp_id}.csv"
    rows = _load_raw_csv(raw_csv)
    print(f"[{exp_id}] loaded {len(rows)} raw rows from {raw_csv.name}")

    with psycopg.connect(exp.dsn, autocommit=True) as conn, conn.cursor() as cur:
        gens = _gen_metadata(cur, exp.id)
        gen_by_id = {g: (c, p, n, e) for g, c, p, n, e in gens}
        print(f"[{exp_id}] {len(gens)} published gens in DB")
        wfs = _workflow_phases(cur, exp.id)
        print(f"[{exp_id}] {len(wfs)} source_sync workflows in DB")
        source_runs = _source_run_diagnostics(cur, exp.id)
        print(f"[{exp_id}] {len(source_runs)} source stage timing runs in DB")
        publish_timing_payloads = _publish_stage_timings(cur, exp.id)
        print(
            f"[{exp_id}] {len(publish_timing_payloads)} "
            "publish stage timing runs in DB"
        )
        publish_timing_events = _publish_stage_timing_events(cur, exp.id)
        print(
            f"[{exp_id}] {len(publish_timing_events)} "
            "publish timing event rows in DB"
        )
        seg_count, branch_count = _chronos_resource_counts(cur)
        peak_rss = _read_peak_rss_bytes(exp)

        # Pairing strategy (in priority order):
        #   1. If the raw CSV row has gen_id, use that directly (new
        #      bench script writes it).
        #   2. Else if baseline_gen.txt exists,
        #      gens[baseline_offset + replay_index - 1] is the replayed
        #      commit's gen. `replay_index` comes from raw `i`, not the CSV
        #      row number, so tail-window rows keep their full-history index.
        #   3. Else fall back to positional pairing (gens[i]).
        baseline_gen = _read_baseline_gen(exp)
        gen_ids_sorted = [g for g, _, _, _, _ in gens]
        if baseline_gen in gen_ids_sorted:
            baseline_offset = gen_ids_sorted.index(baseline_gen) + 1
        elif isinstance(baseline_gen, int):
            baseline_offset = sum(
                1 for g in gen_ids_sorted
                if isinstance(g, int) and g <= baseline_gen
            )
        else:
            baseline_offset = 1  # legacy: assume gens[0] is baseline
        previous_published_ms = None
        if baseline_gen in gen_by_id:
            previous_published_ms = gen_by_id[baseline_gen][1]
        out = []
        per_commit_diagnostics = []
        for i, raw in enumerate(rows, start=1):
            replay_index = _int_or_none(raw.get("i")) or i
            # 1. Prefer explicit gen_id from raw CSV.
            raw_gen = raw.get("gen_id", "")
            if raw_gen not in (None, "", "None"):
                gen_id = _coerce_gen_id(raw_gen)
            else:
                gen_id = None
            if gen_id is None:
                # 2. Fall back to positional pairing via baseline_offset.
                gen_idx = baseline_offset + replay_index - 1
                if 0 <= gen_idx < len(gens):
                    gen_id = gens[gen_idx][0]
            if gen_id is not None and gen_id in gen_by_id:
                _gen_c, gen_p, n_live, e_live = gen_by_id[gen_id]
                if n_live is None or e_live is None:
                    n_live, e_live = _live_counts_at(cur, gen_id)
            else:
                gen_p = None
                n_live, e_live = (None, None)
            raw_prev_gen = _coerce_gen_id(raw.get("prev_gen_id"))
            window_start_ms = previous_published_ms
            if raw_prev_gen in gen_by_id:
                window_start_ms = gen_by_id[raw_prev_gen][1]
            replay_timing = _replay_timing_from_raw_row(raw)
            paired_workflow, workflow_pairing_reason = _pair_replay_workflow(
                wfs,
                replay_timing=replay_timing,
                generation_published_ms=gen_p,
                previous_generation_published_ms=window_start_ms,
            )
            wf_a0 = wf_setup = wf_ingest = wf_close = None
            wf_phase_status = "unavailable"
            wf_phase_unavailable_reason = workflow_pairing_reason
            wf_uuid = None
            workflow_runtime_timings: Mapping[str, float] = {}
            if paired_workflow is not None:
                wf = paired_workflow
                wf_uuid = wf.get("workflow_uuid")
                wf_a0 = wf["started"] - wf["created"]
                runtime_timings = wf.get("workflow_timings_ms")
                if isinstance(runtime_timings, Mapping) and runtime_timings:
                    workflow_runtime_timings = runtime_timings
                    wf_setup = runtime_timings.get("pre_ingest_setup")
                    wf_ingest = runtime_timings.get("run_ingest")
                    wf_close = runtime_timings.get("post_ingest_total")
                    wf_phase_status = "available"
                    wf_phase_unavailable_reason = None
                # Legacy setup span: workflow start → setup_done step start.
                # Newer runs prefer workflow output timings above so the hot
                # path no longer pays for a DBOS no-op marker step.
                elif wf["setup_probe_started"] is not None:
                    wf_setup = wf["setup_probe_started"] - wf["started"]
                    if (
                        wf["end_step_started"] is not None
                        and wf["setup_probe_completed"] is not None
                        and wf["end_step_started"]
                        >= wf["setup_probe_completed"]
                    ):
                        wf_ingest = (
                            wf["end_step_started"]
                            - wf["setup_probe_completed"]
                        )
                        wf_phase_status = "available"
                        wf_phase_unavailable_reason = None
                elif wf["end_step_started"] is not None:
                    # Legacy: the old A1 — body-up-to-only-step. No way
                    # to split setup from ingest without the probe.
                    wf_setup = wf["end_step_started"] - wf["started"]
                    wf_ingest = None
                    wf_phase_status = "partial"
                    wf_phase_unavailable_reason = "setup_probe_missing"
                elif wf["setup_probe_started"] is not None:
                    wf_phase_status = "partial"
                    wf_phase_unavailable_reason = "terminal_operation_missing"
                if (
                    wf["end_step_completed"] is not None
                    and wf["updated"] >= wf["end_step_completed"]
                ):
                    wf_close = wf["updated"] - wf["end_step_completed"]
                elif wf_phase_status == "available":
                    wf_phase_status = "partial"
                    wf_phase_unavailable_reason = "terminal_close_missing"
            workflow_source_events = (
                paired_workflow.get("source_report_events")
                if paired_workflow is not None
                and isinstance(
                    paired_workflow.get("source_report_events"),
                    list,
                )
                else []
            )
            if workflow_source_events:
                source_diag = _aggregate_source_runs(
                    workflow_source_events,
                    start_ms=paired_workflow["created"] - 1,
                    end_ms=paired_workflow["updated"] + 1,
                )
            else:
                source_diag = _aggregate_source_runs(
                    source_runs,
                    start_ms=window_start_ms,
                    end_ms=gen_p,
                )
            workflow_publish_events = (
                paired_workflow.get("publish_report_events")
                if paired_workflow is not None
                and isinstance(
                    paired_workflow.get("publish_report_events"),
                    list,
                )
                else []
            )
            if workflow_publish_events:
                publish_diag = _aggregate_publish_timings(
                    workflow_publish_events,
                    start_ms=paired_workflow["created"] - 1,
                    end_ms=paired_workflow["updated"] + 1,
                    final_gen_id=gen_id,
                    previous_gen_id=raw_prev_gen,
                )
            else:
                publish_diag = _aggregate_publish_timings(
                    publish_timing_events,
                    start_ms=window_start_ms,
                    end_ms=gen_p,
                    final_gen_id=gen_id,
                    previous_gen_id=raw_prev_gen,
                )
            e2e_s = float(raw.get("end_to_end_s") or 0)
            raw_db_size = raw.get("db_size_bytes", "")
            db_size = (
                int(raw_db_size)
                if raw_db_size not in (None, "", "None")
                else None
            )
            workflow_ingest_timings = (
                paired_workflow.get("ingest_timings_ms")
                if paired_workflow is not None
                and isinstance(paired_workflow.get("ingest_timings_ms"), Mapping)
                else {}
            )
            replay_workflow_timing = _replay_workflow_timing(
                replay_timing=replay_timing,
                workflow=paired_workflow,
                generation_published_ms=gen_p,
            )
            # Per-commit publish-cycle phase decomposition, joined on the
            # commit's final published generation id (bench-okg-internals).
            phase_payload = (
                publish_timing_payloads.get(str(gen_id))
                if gen_id is not None else None
            )
            if phase_payload is None and workflow_publish_events:
                for event in workflow_publish_events:
                    if (
                        event.get("status") == "published"
                        and not event.get("publish_skipped")
                        and (
                            gen_id is None
                            or event.get("generation_id") == gen_id
                        )
                    ):
                        phase_payload = {
                            "timings_ms": event.get("timings_ms") or {},
                            "total_duration_ms": event.get("total_ms"),
                        }
                        break
            phases = _rollup_publish_phases(phase_payload)

            def _nan(v):
                return v if v is not None else "NaN"

            def _wf_timing(name: str):
                if not isinstance(workflow_ingest_timings, Mapping):
                    return "NaN"
                return workflow_ingest_timings.get(name, "NaN")

            def _runtime_timing(name: str):
                if not isinstance(workflow_runtime_timings, Mapping):
                    return "NaN"
                return workflow_runtime_timings.get(name, "NaN")

            # --- count-based incrementality metrics (count-based-perf-gate) ---
            # query_count: deterministic DB round-trips for this commit's
            # workflow, emitted by the ingest service from the profiling
            # context. queries_per_changed_file: the incrementality ratio that
            # a future orchestration-bound regression inflates regardless of
            # wall time. work_ratio: graph-delta-write (chronos_apply) ms over
            # the total per-commit wall (e2e), i.e. the fraction of the commit
            # spent on the real graph write vs orchestration.
            files_changed_n = (
                int(raw["files_changed"]) if raw["files_changed"] else 0
            )
            commit_query_count = (
                paired_workflow.get("query_count")
                if paired_workflow is not None else None
            )
            commit_query_count = _int_or_none(commit_query_count)
            queries_per_changed_file = (
                round(commit_query_count / max(1, files_changed_n), 3)
                if commit_query_count is not None else None
            )
            chronos_apply_ms = _float_or_none(source_diag.get("chronos_apply_ms"))
            total_commit_ms = float(e2e_s * 1000) if e2e_s else None
            work_ratio = (
                round(chronos_apply_ms / total_commit_ms, 4)
                if (
                    chronos_apply_ms is not None
                    and total_commit_ms is not None
                    and total_commit_ms > 0
                )
                else None
            )

            out.append({
                "i":              int(raw["i"]),
                "commit":         raw["commit"],
                "subject":        raw["subject"],
                "files_changed":  int(raw["files_changed"]) if raw["files_changed"] else 0,
                "lines_added":    int(raw["lines_added"]) if raw["lines_added"] else 0,
                "lines_deleted":  int(raw["lines_deleted"]) if raw["lines_deleted"] else 0,
                "e2e_ms":         int(round(e2e_s * 1000)),
                "cycle_ms":       int(raw["cycle_ms"]) if raw["cycle_ms"] else 0,
                "nodes_added":    int(raw["nodes_added"]) if raw["nodes_added"] else 0,
                "edges_added":    int(raw["edges_added"]) if raw["edges_added"] else 0,
                "nodes_live":     n_live if n_live is not None else "NaN",
                "edges_live":     e_live if e_live is not None else "NaN",
                "gen_id":         gen_id if gen_id is not None else "NaN",
                "prev_gen_id":    raw_prev_gen if raw_prev_gen is not None else "",
                "db_size_bytes":  db_size if db_size is not None else "NaN",
                **{
                    field: (
                        value if value is not None else "NaN"
                    )
                    for field, value in replay_timing.items()
                },
                "bench_generation_wait_strategy": (
                    raw.get("bench_generation_wait_strategy") or ""
                ),
                **{
                    field: (
                        value if value is not None else "NaN"
                    )
                    for field, value in replay_workflow_timing.items()
                },
                "wf_a0_ms":       wf_a0 if wf_a0 is not None else "NaN",
                "wf_setup_ms":    wf_setup if wf_setup is not None else "NaN",
                "wf_ingest_ms":   wf_ingest if wf_ingest is not None else "NaN",
                "wf_close_ms":    wf_close if wf_close is not None else "NaN",
                "wf_phase_status": wf_phase_status,
                "wf_phase_unavailable_reason": (
                    wf_phase_unavailable_reason or ""
                ),
                "wf_pairing_reason": workflow_pairing_reason,
                "ingest_total_ms": _wf_timing("total"),
                "ingest_source_wall_ms": _wf_timing("source_wall"),
                "ingest_source_reported_total_ms": _wf_timing(
                    "source_reported_total",
                ),
                "ingest_publish_wall_ms": _wf_timing("publish_wall"),
                "ingest_publish_reported_total_ms": _wf_timing(
                    "publish_reported_total",
                ),
                "ingest_orchestration_overhead_ms": _wf_timing(
                    "orchestration_overhead",
                ),
                "ingest_orchestration_unattributed_ms": _wf_timing(
                    "orchestration_unattributed",
                ),
                "ingest_setup_total_ms": _wf_timing("setup.total"),
                "workflow_pre_ingest_setup_ms": _runtime_timing(
                    "pre_ingest_setup",
                ),
                "workflow_run_ingest_ms": _runtime_timing("run_ingest"),
                "workflow_post_ingest_total_ms": _runtime_timing(
                    "post_ingest_total",
                ),
                "workflow_complete_runtime_ms": _runtime_timing(
                    "complete_runtime",
                ),
                "workflow_total_ms": _runtime_timing("total"),
                "workflow_setup_resolve_context_ms": _runtime_timing(
                    "resolve_context",
                ),
                "workflow_setup_start_usefulness_ms": _runtime_timing(
                    "start_usefulness",
                ),
                "workflow_setup_plan_runtime_ms": _runtime_timing(
                    "plan_runtime",
                ),
                "source_run_count": source_diag["source_run_count"],
                "source_diag_status": source_diag["source_diag_status"],
                "source_diag_reason": source_diag["source_diag_reason"],
                "source_total_ms": (
                    round(source_diag["source_total_ms"], 3)
                    if source_diag["source_total_ms"] is not None else "NaN"
                ),
                # count-based incrementality metrics (count-based-perf-gate).
                "query_count": _nan(commit_query_count),
                "queries_per_changed_file": _nan(queries_per_changed_file),
                "chronos_apply_ms": (
                    round(chronos_apply_ms, 3)
                    if chronos_apply_ms is not None else "NaN"
                ),
                "work_ratio": _nan(work_ratio),
                "slowest_source": source_diag["slowest_source"] or "",
                "slowest_phase": source_diag["slowest_phase"] or "",
                "slowest_phase_ms": (
                    round(source_diag["slowest_phase_ms"], 3)
                    if source_diag["slowest_phase_ms"] is not None else "NaN"
                ),
                "top_sources": source_diag["top_sources"],
                "top_phases": source_diag["top_phases"],
                # publish-cycle substrate-internal phase decomposition (per
                # commit, from the persisted publish timings).
                "projection_ms": _nan(phases["projection_ms"]),
                "enrichment_ms": _nan(phases["enrichment_ms"]),
                "gate_ms": _nan(phases["gate_ms"]),
                "publish_ms": _nan(phases["publish_ms"]),
                "view_materialization_ms": _nan(phases["view_materialization_ms"]),
                "setup_overhead_ms": _nan(phases["setup_overhead_ms"]),
                "delta_probe_ms": _nan(phases["delta_probe_ms"]),
                "maintained_derivation_ms": _nan(phases["maintained_derivation_ms"]),
                "unattributed_ms": _nan(phases["unattributed_ms"]),
                "publish_total_ms": _nan(phases["publish_total_ms"]),
                # run-level resource metrics (constant across rows: peak worker
                # RSS over the run, final chronos segment + branch counts).
                "peak_rss_bytes": _nan(peak_rss),
                "chronos_segments": _nan(seg_count),
                "chronos_branches": _nan(branch_count),
                "publish_diag_status": publish_diag["publish_diag_status"],
                "publish_diag_reason": publish_diag["publish_diag_reason"],
                "publish_event_count": publish_diag["publish_event_count"],
                "publish_skipped_count": publish_diag["publish_skipped_count"],
                "published_count": publish_diag["published_count"],
                "intermediate_publish_count": (
                    publish_diag["intermediate_publish_count"]
                ),
                "publish_window_total_ms": (
                    round(publish_diag["publish_total_ms"], 3)
                    if publish_diag["publish_total_ms"] is not None else "NaN"
                ),
                "final_publish_total_ms": (
                    round(publish_diag["final_publish_total_ms"], 3)
                    if publish_diag["final_publish_total_ms"] is not None
                    else "NaN"
                ),
                "intermediate_publish_total_ms": (
                    round(publish_diag["intermediate_publish_total_ms"], 3)
                    if publish_diag["intermediate_publish_total_ms"] is not None
                    else "NaN"
                ),
                "publish_window_view_materialization_ms": (
                    round(publish_diag["view_materialization_ms"], 3)
                    if publish_diag["view_materialization_ms"] is not None
                    else "NaN"
                ),
                "intermediate_view_materialization_ms": (
                    round(
                        publish_diag["intermediate_view_materialization_ms"], 3,
                    )
                    if publish_diag["intermediate_view_materialization_ms"]
                    is not None else "NaN"
                ),
                "publish_labels": publish_diag["publish_labels"],
            })
            per_commit_diagnostics.append({
                "i": int(raw["i"]),
                "commit": raw["commit"],
                "gen_id": gen_id,
                "prev_gen_id": raw_prev_gen,
                "window_start_ms": window_start_ms,
                "window_end_ms": gen_p,
                "workflow_uuid": wf_uuid,
                "workflow_pairing_reason": workflow_pairing_reason,
                "workflow_ingest_timings_ms": (
                    dict(workflow_ingest_timings)
                    if isinstance(workflow_ingest_timings, Mapping) else {}
                ),
                "workflow_runtime_timings_ms": (
                    dict(workflow_runtime_timings)
                    if isinstance(workflow_runtime_timings, Mapping) else {}
                ),
                "bench_replay": replay_timing,
                "bench_workflow": replay_workflow_timing,
                "source_diag_status": source_diag["source_diag_status"],
                "source_diag_reason": source_diag["source_diag_reason"],
                "source_run_count": source_diag["source_run_count"],
                "source_total_ms": source_diag["source_total_ms"],
                # count-based incrementality metrics (count-based-perf-gate).
                "query_count": commit_query_count,
                "files_changed": files_changed_n,
                "queries_per_changed_file": queries_per_changed_file,
                "chronos_apply_ms": chronos_apply_ms,
                "work_ratio": work_ratio,
                "slowest_source": source_diag["slowest_source"],
                "slowest_phase": source_diag["slowest_phase"],
                "slowest_phase_ms": source_diag["slowest_phase_ms"],
                "top_sources": source_diag["top_sources"],
                "top_phases": source_diag["top_phases"],
                "publish_diag_status": publish_diag["publish_diag_status"],
                "publish_diag_reason": publish_diag["publish_diag_reason"],
                "publish_event_count": publish_diag["publish_event_count"],
                "publish_skipped_count": publish_diag["publish_skipped_count"],
                "published_count": publish_diag["published_count"],
                "intermediate_publish_count": (
                    publish_diag["intermediate_publish_count"]
                ),
                "publish_total_ms": publish_diag["publish_total_ms"],
                "final_publish_total_ms": (
                    publish_diag["final_publish_total_ms"]
                ),
                "intermediate_publish_total_ms": (
                    publish_diag["intermediate_publish_total_ms"]
                ),
                "view_materialization_ms": (
                    publish_diag["view_materialization_ms"]
                ),
                "intermediate_view_materialization_ms": (
                    publish_diag["intermediate_view_materialization_ms"]
                ),
                "publish_labels": publish_diag["publish_labels"],
                "publishes": [
                    {
                        "label": item.get("label"),
                        "status": item.get("status"),
                        "generation_id": item.get("generation_id"),
                        "publish_skipped": item.get("publish_skipped"),
                        "reason": item.get("reason"),
                        "updated_at": item.get("updated_at"),
                        "total_ms": item.get("total_ms"),
                        "view_materialization_ms": (
                            item.get("view_materialization_ms")
                        ),
                        "timings_ms": item.get("timings_ms"),
                        "native_view_refresh": item.get("native_view_refresh"),
                        "attribute_effective_view_refresh": item.get(
                            "attribute_effective_view_refresh"
                        ),
                        "native_views": item.get("native_views"),
                        "lenses": item.get("lenses"),
                        "enricher_governance": item.get(
                            "enricher_governance"
                        ),
                        "extractor_edges_emitted": (
                            item.get("extractor_edges_emitted")
                        ),
                        "derived_fact_summary": item.get(
                            "derived_fact_summary"
                        ),
                    }
                    for item in publish_diag["publishes"]
                ],
                "source_runs": [
                    {
                        "source_name": run.get("source_name"),
                        "registry_source_name": run.get("registry_source_name"),
                        "run_id": run.get("run_id"),
                        "status": run.get("status"),
                        "run_mode": run.get("run_mode"),
                        "timing_status": run.get("timing_status"),
                        "timing_unavailable_reason": (
                            run.get("timing_unavailable_reason")
                        ),
                        "updated_at": run.get("updated_at"),
                        "input_count": run.get("input_count"),
                        "total_ms": run.get("total_ms"),
                        "source_runner_ms": run.get("source_runner_ms"),
                        "slowest_phase": run.get("slowest_phase"),
                        "slowest_phase_ms": run.get("slowest_phase_ms"),
                        "timings_ms": run.get("timings_ms"),
                        "source_stats": run.get("source_stats"),
                        "nodes_appended": run.get("nodes_appended"),
                        "edges_appended": run.get("edges_appended"),
                        "nodes_retracted": run.get("nodes_retracted"),
                        "edges_retracted": run.get("edges_retracted"),
                        "output_count": run.get("output_count"),
                    }
                    for run in source_diag["runs"]
                ],
            })
            if gen_p is not None:
                previous_published_ms = gen_p

    fields = list(out[0].keys()) if out else []
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(out)
    print(f"[{exp_id}] wrote {len(out)} rows → {out_csv}")
    diagnostics_path = RESULTS / f"{exp_id}.diagnostics.json"
    summary = _source_run_summary(source_runs)
    publish_summary = _publish_run_summary(publish_timing_events)
    diagnostics = {
        "schema": "okg.bench.source_sync_diagnostics.v1",
        "experiment_id": exp_id,
        "source_run_count": len(source_runs),
        "publish_event_count": len(publish_timing_events),
        "correlation_method": "generation_publish_time_window",
        "source_run_summary": summary,
        "publish_run_summary": publish_summary,
        "measurements": _diagnostic_measurements(
            exp_id=exp_id,
            summary=summary,
            publish_summary=publish_summary,
        ),
        "row_status_counts": dict(Counter(
            row["source_diag_status"] for row in out
        )),
        "per_commit": per_commit_diagnostics,
    }
    diagnostics_path.write_text(
        json.dumps(diagnostics, indent=2, sort_keys=True, default=str)
    )
    print(f"[{exp_id}] wrote diagnostics → {diagnostics_path}")


def main() -> int:
    if len(sys.argv) < 2:
        sys.exit("usage: extract.py <experiment_id>")
    extract(sys.argv[1])
    return 0


if __name__ == "__main__":
    sys.exit(main())
