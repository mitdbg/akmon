"""Deterministic source-sync optimization ledger policy."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any


MICROBENCH_OPTIMIZATION_LEDGER_SCHEMA = (
    "okg.source_sync.microbench.optimization_ledger.v1"
)
MICROBENCH_OPTIMIZATION_LEDGER_ROWS_SCHEMA = (
    "okg.source_sync.microbench.optimization_ledger.rows.v1"
)
SUBSECOND_LATENCY_BUDGET_MS = 1000.0
HOT_WAVE_SOURCE_ORDER = (
    "code-data-flow",
    "agent-sessions",
    "code-structure",
    "profile-measurements",
    "interfaces",
    "compute-env-remote",
    "codex-sessions",
    "openspec",
    "package-manifests",
    "pact",
    "git-files",
)


def build_source_optimization_ledger(
    inventory: Mapping[str, Any],
    *,
    latency_budget_ms: float = SUBSECOND_LATENCY_BUDGET_MS,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build the source-by-source optimization work queue.

    The ledger is deliberately derived from the live inventory so that PACT,
    CLI output, and native views share one source ordering and vocabulary.
    It is not benchmark proof; rows with only source_progress timings remain
    marked as missing benchmark evidence.
    """

    if generated_at is None:
        generated_at = datetime.now(timezone.utc).isoformat()

    source_rows = [
        _optimization_source_row(
            item,
            latency_budget_ms=latency_budget_ms,
        )
        for item in inventory.get("sources") or ()
        if isinstance(item, Mapping)
    ]
    enricher_rows = [
        _optimization_enricher_row(item)
        for item in inventory.get("enrichers") or ()
        if isinstance(item, Mapping)
    ]
    rows = sorted(
        [*source_rows, *enricher_rows],
        key=_optimization_sort_key,
    )
    for index, row in enumerate(rows, start=1):
        row["priority_rank"] = index

    status_counts: dict[str, int] = {}
    wave_counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("optimization_status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        wave = str(row.get("optimization_wave") or "unknown")
        wave_counts[wave] = wave_counts.get(wave, 0) + 1

    return {
        "schema": MICROBENCH_OPTIMIZATION_LEDGER_SCHEMA,
        "deployment": inventory.get("deployment"),
        "generated_at": generated_at,
        "source_count": inventory.get("source_count", 0),
        "enricher_count": inventory.get("enricher_count", 0),
        "row_count": len(rows),
        "latency_budget_ms": latency_budget_ms,
        "inventory_schema": inventory.get("schema"),
        "inventory_generated_at": inventory.get("generated_at"),
        "live_state": inventory.get("live_state") or {},
        "coverage_counts": inventory.get("coverage_counts") or {},
        "optimization_status_counts": status_counts,
        "optimization_wave_counts": wave_counts,
        "rows": rows,
    }


def rank_source_optimization_rows(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Return optimization ledger rows in canonical priority order."""

    ranked = sorted(
        [dict(row) for row in rows if isinstance(row, Mapping)],
        key=_optimization_sort_key,
    )
    for index, row in enumerate(ranked, start=1):
        row["priority_rank"] = index
    return ranked


def source_optimization_ledger_relation_rows(
    ledger: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Flatten an optimization ledger into native-view relation rows."""

    rows: list[dict[str, Any]] = []
    for item in ledger.get("rows") or ():
        if not isinstance(item, Mapping):
            continue
        rows.append(
            {
                "row_key": item.get("row_key"),
                "row_kind": item.get("row_kind"),
                "name": item.get("name"),
                "owner": item.get("owner"),
                "priority_rank": item.get("priority_rank"),
                "optimization_wave": item.get("optimization_wave"),
                "optimization_status": item.get("optimization_status"),
                "coverage_class": item.get("coverage_class"),
                "readiness_required": item.get("readiness_required"),
                "latency_budget_ms": item.get("latency_budget_ms"),
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
                "latest_source_files_seen": item.get("latest_source_files_seen"),
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
                "latest_source_facts_evaluated": item.get(
                    "latest_source_facts_evaluated",
                ),
                "latest_source_facts_emitted": item.get(
                    "latest_source_facts_emitted",
                ),
                "latest_source_facts_filtered_unchanged": item.get(
                    "latest_source_facts_filtered_unchanged",
                ),
                "latency_status": item.get("latency_status"),
                "node_count": item.get("node_count"),
                "edge_count": item.get("edge_count"),
                "nodes_added_1h": item.get("nodes_added_1h"),
                "edges_added_1h": item.get("edges_added_1h"),
                "nodes_added_24h": item.get("nodes_added_24h"),
                "edges_added_24h": item.get("edges_added_24h"),
                "latest_nodes_appended": item.get("latest_nodes_appended"),
                "latest_edges_appended": item.get("latest_edges_appended"),
                "latest_dedupe_drops_node": item.get(
                    "latest_dedupe_drops_node",
                ),
                "latest_dedupe_drops_edge": item.get(
                    "latest_dedupe_drops_edge",
                ),
                "amplification_signal": item.get("amplification_signal"),
                "bottleneck_stage": item.get("bottleneck_stage"),
                "root_cause_class": item.get("root_cause_class"),
                "fix_decision": item.get("fix_decision"),
                "proof_status": item.get("proof_status"),
                "next_action": item.get("next_action"),
                "next_command": item.get("next_command"),
                "blocker": item.get("blocker"),
                "scope_support_status": item.get("scope_support_status"),
                "cursor_support_status": item.get("cursor_support_status"),
                "producer_status": item.get("producer_status"),
                "benchmark_scenarios": item.get("benchmark_scenarios"),
                "source_class": item.get("source_class"),
                "module": item.get("module"),
                "class_name": item.get("class_name"),
            }
        )
    return rows


def _optimization_source_row(
    item: Mapping[str, Any],
    *,
    latency_budget_ms: float,
) -> dict[str, Any]:
    source_name = str(item.get("source_name") or item.get("name") or "")
    coverage_class = str(item.get("coverage_class") or "")
    blocker = item.get("blocker")
    latest_ms = _as_float(item.get("latest_source_runner_ms"))
    stage_timings = _stage_timings_from_item(item)
    full_pipeline = _full_pipeline_proof_from_item(item)
    nodes_appended = _as_int(item.get("latest_nodes_appended")) or 0
    edges_appended = _as_int(item.get("latest_edges_appended")) or 0
    dedupe_node = _as_int(item.get("latest_dedupe_drops_node")) or 0
    dedupe_edge = _as_int(item.get("latest_dedupe_drops_edge")) or 0
    facts_emitted = _as_int(item.get("latest_source_facts_emitted")) or 0
    readiness_required = bool(item.get("readiness_required"))
    wave = _source_wave(
        source_name,
        coverage_class,
        readiness_required=readiness_required,
    )
    latency_status = _latency_status(
        latest_ms,
        coverage_class=coverage_class,
        wave=wave,
        stage_total_ms=_as_float(item.get("latest_stage_total_ms")),
        full_pipeline_ms=_as_float(
            full_pipeline.get("latency_p95_ms"),
        ),
        latency_budget_ms=latency_budget_ms,
    )
    amplification_signal = _amplification_signal(
        latest_ms=latest_ms,
        nodes_appended=nodes_appended,
        edges_appended=edges_appended,
        dedupe_node=dedupe_node,
        dedupe_edge=dedupe_edge,
        facts_emitted=facts_emitted,
        latency_budget_ms=latency_budget_ms,
    )
    bottleneck_stage = _bottleneck_stage(
        blocker=str(blocker or ""),
        coverage_class=coverage_class,
        latest_ms=latest_ms,
        stage_timings=stage_timings,
        amplification_signal=amplification_signal,
        latency_budget_ms=latency_budget_ms,
    )
    root_cause = _root_cause_class(
        blocker=str(blocker or ""),
        coverage_class=coverage_class,
        latest_ms=latest_ms,
        stage_total_ms=_as_float(item.get("latest_stage_total_ms")),
        amplification_signal=amplification_signal,
        latency_budget_ms=latency_budget_ms,
    )
    optimization_status = _optimization_status(
        blocker=str(blocker or ""),
        coverage_class=coverage_class,
        full_pipeline=full_pipeline,
        latest_ms=latest_ms,
        latency_status=latency_status,
        latency_budget_ms=latency_budget_ms,
    )
    fix_decision = _fix_decision(
        optimization_status=optimization_status,
        root_cause=root_cause,
    )
    proof_status = _proof_status(
        blocker=str(blocker or ""),
        full_pipeline=full_pipeline,
        latest_ms=latest_ms,
        optimization_status=optimization_status,
        latency_budget_ms=latency_budget_ms,
    )
    next_action = _ledger_next_action(
        blocker=str(blocker or ""),
        optimization_status=optimization_status,
        fix_decision=fix_decision,
    )
    return {
        "row_key": f"source:{source_name}",
        "row_kind": "source",
        "name": source_name,
        "owner": item.get("owner"),
        "priority_rank": None,
        "optimization_wave": wave,
        "optimization_status": optimization_status,
        "coverage_class": coverage_class,
        "readiness_required": readiness_required,
        "latency_budget_ms": (latency_budget_ms if wave == "hot_wave" else None),
        "latest_source_runner_ms": latest_ms,
        "latest_stage_total_ms": item.get("latest_stage_total_ms"),
        "latest_stage_admission_prepare_ms": item.get(
            "latest_stage_admission_prepare_ms",
        ),
        "latest_stage_import_ms": item.get("latest_stage_import_ms"),
        "latest_stage_construct_ms": item.get("latest_stage_construct_ms"),
        "latest_stage_source_runner_ms": item.get(
            "latest_stage_source_runner_ms",
        ),
        "latest_stage_source_state_snapshot_ms": item.get(
            "latest_stage_source_state_snapshot_ms",
        ),
        "latest_stage_admission_finalize_ms": item.get(
            "latest_stage_admission_finalize_ms",
        ),
        "latest_full_pipeline_scenario": full_pipeline.get("scenario"),
        "latest_full_pipeline_ok": full_pipeline.get("ok"),
        "latest_full_pipeline_latency_p95_ms": full_pipeline.get(
            "latency_p95_ms",
        ),
        "latest_full_pipeline_rows_appended": full_pipeline.get(
            "rows_appended_total",
        ),
        "latest_full_pipeline_changed_graph_rows": full_pipeline.get(
            "changed_graph_rows",
        ),
        "latest_full_pipeline_expected_delta": full_pipeline.get("expected_delta")
        or {},
        "latest_full_pipeline_expected_delta_source": full_pipeline.get(
            "expected_delta_source",
        ),
        "latest_full_pipeline_expected_delta_units": full_pipeline.get(
            "expected_delta_units",
        ),
        "latest_full_pipeline_observed_delta_units": full_pipeline.get(
            "observed_delta_units",
        ),
        "latest_full_pipeline_source_work_units": full_pipeline.get(
            "source_work_units",
        ),
        "latest_full_pipeline_work_amplification_ratio": full_pipeline.get(
            "work_amplification_ratio",
        ),
        "latest_full_pipeline_work_amplification_budget": full_pipeline.get(
            "work_amplification_budget",
        ),
        "latest_full_pipeline_observability_status": full_pipeline.get(
            "observability_status",
        ),
        "latest_full_pipeline_observability_gaps": full_pipeline.get(
            "observability_gaps",
        ),
        "latest_full_pipeline_no_change_dominant": full_pipeline.get(
            "no_change_dominant",
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
        "latest_full_pipeline_top_widening_ms": full_pipeline.get(
            "top_widening_elapsed_p95_ms",
        ),
        "latest_full_pipeline_lock_wait_p95_ms": full_pipeline.get(
            "publish_lock_wait_p95_ms",
        ),
        "latest_full_pipeline_lock_hold_p95_ms": full_pipeline.get(
            "publish_lock_hold_p95_ms",
        ),
        "latest_full_pipeline_environment_contention": full_pipeline.get(
            "environment_contention",
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
        "latest_full_pipeline_generated_at": full_pipeline.get("generated_at"),
        "latest_source_files_seen": item.get("latest_source_files_seen"),
        "latest_source_files_scanned": item.get("latest_source_files_scanned"),
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
        "latest_source_bytes_delta": item.get("latest_source_bytes_delta"),
        "latest_source_lines_delta": item.get("latest_source_lines_delta"),
        "latest_source_facts_evaluated": item.get(
            "latest_source_facts_evaluated",
        ),
        "latest_source_facts_emitted": item.get(
            "latest_source_facts_emitted",
        ),
        "latest_source_facts_filtered_unchanged": item.get(
            "latest_source_facts_filtered_unchanged",
        ),
        "latency_status": latency_status,
        "node_count": item.get("node_count"),
        "edge_count": item.get("edge_count"),
        "nodes_added_1h": item.get("nodes_added_1h"),
        "edges_added_1h": item.get("edges_added_1h"),
        "nodes_added_24h": item.get("nodes_added_24h"),
        "edges_added_24h": item.get("edges_added_24h"),
        "latest_nodes_appended": item.get("latest_nodes_appended"),
        "latest_edges_appended": item.get("latest_edges_appended"),
        "latest_dedupe_drops_node": item.get("latest_dedupe_drops_node"),
        "latest_dedupe_drops_edge": item.get("latest_dedupe_drops_edge"),
        "amplification_signal": amplification_signal,
        "bottleneck_stage": bottleneck_stage,
        "root_cause_class": root_cause,
        "fix_decision": fix_decision,
        "proof_status": proof_status,
        "next_action": next_action,
        "next_command": _ledger_next_command(
            source_name=source_name,
            blocker=str(blocker or ""),
            optimization_status=optimization_status,
        ),
        "blocker": blocker,
        "scope_support_status": item.get("scope_support_status"),
        "cursor_support_status": item.get("cursor_support_status"),
        "producer_status": item.get("producer_status"),
        "benchmark_scenarios": ",".join(
            item.get("benchmark_scenarios") or (),
        ),
        "source_class": item.get("source_class"),
        "module": item.get("module"),
        "class_name": item.get("class_name"),
    }


def _optimization_enricher_row(item: Mapping[str, Any]) -> dict[str, Any]:
    name = str(item.get("name") or "")
    return {
        "row_key": f"enricher:{name}",
        "row_kind": "enricher",
        "name": name,
        "owner": item.get("owner"),
        "priority_rank": None,
        "optimization_wave": "enricher_wave",
        "optimization_status": "needs_full_pipeline_audit",
        "coverage_class": item.get("coverage_class"),
        "readiness_required": False,
        "latency_budget_ms": None,
        "latest_source_runner_ms": None,
        "latest_stage_total_ms": None,
        "latest_stage_admission_prepare_ms": None,
        "latest_stage_import_ms": None,
        "latest_stage_construct_ms": None,
        "latest_stage_source_runner_ms": None,
        "latest_stage_source_state_snapshot_ms": None,
        "latest_stage_admission_finalize_ms": None,
        "latest_source_files_seen": None,
        "latest_source_files_scanned": None,
        "latest_source_files_scanned_full": None,
        "latest_source_files_scanned_append_delta": None,
        "latest_source_files_skipped_unchanged": None,
        "latest_source_files_skipped_cwd_filter": None,
        "latest_source_bytes_delta": None,
        "latest_source_lines_delta": None,
        "latency_status": "unmeasured",
        "node_count": None,
        "edge_count": None,
        "nodes_added_1h": None,
        "edges_added_1h": None,
        "nodes_added_24h": None,
        "edges_added_24h": None,
        "latest_nodes_appended": None,
        "latest_edges_appended": None,
        "latest_dedupe_drops_node": None,
        "latest_dedupe_drops_edge": None,
        "amplification_signal": "full_pipeline_unknown",
        "bottleneck_stage": "enrichment_or_view_refresh",
        "root_cause_class": "source_backed_enricher_scope_unknown",
        "fix_decision": "include_in_full_pipeline_harness",
        "proof_status": "missing_full_pipeline_benchmark",
        "next_action": "measure_full_pipeline_enricher_scope",
        "next_command": (
            "env -u OKG_DSN uv run okg sync microbench ledger "
            "--deployment okg-workspace --json"
        ),
        "blocker": item.get("blocker"),
        "scope_support_status": item.get("scope_support_status"),
        "cursor_support_status": "not_applicable",
        "producer_status": "not_applicable",
        "benchmark_scenarios": ",".join(
            item.get("benchmark_scenarios") or (),
        ),
        "source_class": "enricher",
        "module": item.get("module"),
        "class_name": item.get("class_name"),
    }


def _optimization_sort_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    wave = str(row.get("optimization_wave") or "")
    name = str(row.get("name") or "")
    wave_order = {
        "hot_wave": 0,
        "supporting_wave": 1,
        "policy_wave": 2,
        "enricher_wave": 3,
    }.get(wave, 9)
    if name in HOT_WAVE_SOURCE_ORDER:
        name_order = HOT_WAVE_SOURCE_ORDER.index(name)
    else:
        name_order = 999
    latest_ms = _as_float(row.get("latest_source_runner_ms"))
    latency_sort = -(latest_ms or 0.0)
    return (
        wave_order,
        name_order,
        _status_sort_rank(str(row.get("optimization_status") or "")),
        latency_sort,
        name,
    )


def _status_sort_rank(status: str) -> int:
    return {
        "blocked_inventory": 0,
        "failing_budget": 1,
        "slow_useful": 2,
        "needs_benchmark": 3,
        "confirm_policy": 4,
        "needs_full_pipeline_benchmark": 5,
        "passing_latest_runner_sample": 6,
        "passing_full_pipeline_benchmark": 7,
    }.get(status, 9)


def _source_wave(
    source_name: str,
    coverage_class: str,
    *,
    readiness_required: bool = False,
) -> str:
    if coverage_class in {"manual_full_rebuild", "unsupported", "disabled"}:
        return "policy_wave"
    if readiness_required:
        return "hot_wave"
    if source_name in HOT_WAVE_SOURCE_ORDER:
        return "supporting_wave"
    return "supporting_wave"


def _latency_status(
    latest_ms: float | None,
    *,
    coverage_class: str,
    wave: str,
    stage_total_ms: float | None = None,
    full_pipeline_ms: float | None = None,
    latency_budget_ms: float,
) -> str:
    effective_ms = (
        max(
            value
            for value in (latest_ms, stage_total_ms, full_pipeline_ms)
            if value is not None
        )
        if (
            latest_ms is not None
            or stage_total_ms is not None
            or full_pipeline_ms is not None
        )
        else None
    )
    if effective_ms is None:
        return "unmeasured"
    if wave == "hot_wave":
        return "over_budget" if effective_ms > latency_budget_ms else "under_budget"
    if coverage_class in {"manual_full_rebuild", "unsupported", "disabled"}:
        return "policy_classification_required"
    return "slow" if effective_ms > latency_budget_ms else "bounded_latest_sample"


def _amplification_signal(
    *,
    latest_ms: float | None,
    nodes_appended: int,
    edges_appended: int,
    dedupe_node: int,
    dedupe_edge: int,
    facts_emitted: int,
    latency_budget_ms: float,
) -> str:
    if latest_ms is None:
        return "unmeasured"
    appended_total = max(0, nodes_appended) + max(0, edges_appended)
    if max(0, facts_emitted) > 0 and appended_total == 0:
        return "full_emission_zero_append"
    if _dedupe_drop_amplification(
        nodes_appended=nodes_appended,
        edges_appended=edges_appended,
        dedupe_node=dedupe_node,
        dedupe_edge=dedupe_edge,
    ):
        return "dedupe_drops_after_source_work"
    if latest_ms > latency_budget_ms and not (nodes_appended or edges_appended):
        return "zero_delta_fixed_cost"
    if latest_ms > latency_budget_ms:
        return "high_runner_time"
    return "bounded_latest_sample"


def _dedupe_drop_amplification(
    *,
    nodes_appended: int,
    edges_appended: int,
    dedupe_node: int,
    dedupe_edge: int,
) -> bool:
    dedupe_total = max(0, dedupe_node) + max(0, dedupe_edge)
    if dedupe_total == 0:
        return False
    appended_total = max(0, nodes_appended) + max(0, edges_appended)
    if appended_total == 0:
        return True
    return dedupe_total >= max(10, int(appended_total * 0.05))


def _bottleneck_stage(
    *,
    blocker: str,
    coverage_class: str,
    latest_ms: float | None,
    stage_timings: Mapping[str, float],
    amplification_signal: str,
    latency_budget_ms: float,
) -> str:
    if blocker == "scope_support_missing":
        return "source_contract.scope"
    if blocker:
        return "source_contract"
    if coverage_class in {"manual_full_rebuild", "unsupported", "disabled"}:
        return "source_policy"
    stage_bottleneck = _stage_bottleneck(stage_timings)
    if stage_bottleneck is not None:
        return stage_bottleneck
    stage_total_ms = _as_float(stage_timings.get("total"))
    if (
        stage_total_ms is not None
        and stage_total_ms > latency_budget_ms
        and (latest_ms is None or latest_ms <= latency_budget_ms)
    ):
        return "ingest.multi_stage_overhead"
    if latest_ms is None:
        return "unmeasured"
    if amplification_signal == "dedupe_drops_after_source_work":
        return "source_runner.reemission_or_dedupe"
    if amplification_signal == "full_emission_zero_append":
        return "source_runner.full_emission_zero_append"
    if amplification_signal == "zero_delta_fixed_cost":
        return "source_runner.noop_fixed_overhead"
    if latest_ms > latency_budget_ms:
        return "source_runner.adapter"
    return "pending_stage_timer"


def _stage_timings_from_item(item: Mapping[str, Any]) -> dict[str, float]:
    keys = {
        "total": "latest_stage_total_ms",
        "admission_prepare": "latest_stage_admission_prepare_ms",
        "import": "latest_stage_import_ms",
        "construct": "latest_stage_construct_ms",
        "source_state_snapshot": "latest_stage_source_state_snapshot_ms",
        "admission_finalize": "latest_stage_admission_finalize_ms",
        "source_runner": "latest_stage_source_runner_ms",
    }
    out: dict[str, float] = {}
    for label, key in keys.items():
        value = _as_float(item.get(key))
        if value is not None:
            out[label] = value
    return out


def _stage_bottleneck(stage_timings: Mapping[str, float]) -> str | None:
    candidates = {
        label: value
        for label, value in stage_timings.items()
        if label != "total" and value is not None
    }
    if not candidates:
        return None
    label, value = max(candidates.items(), key=lambda item: item[1])
    if value <= 0:
        return None
    if label == "source_runner":
        return None
    return {
        "admission_prepare": "admission.prepare",
        "import": "adapter.import",
        "construct": "adapter.construct",
        "source_state_snapshot": "ingest.source_state_snapshot",
        "admission_finalize": "admission.finalize",
    }.get(label, f"ingest.{label}")


def _full_pipeline_proof_from_item(item: Mapping[str, Any]) -> dict[str, Any]:
    scenario = item.get("latest_full_pipeline_scenario")
    latency_p95_ms = _as_float(item.get("latest_full_pipeline_latency_p95_ms"))
    if not scenario and latency_p95_ms is None:
        return {}
    return {
        "scenario": scenario,
        "ok": item.get("latest_full_pipeline_ok"),
        "latency_p95_ms": latency_p95_ms,
        "rows_appended_total": _as_int(
            item.get("latest_full_pipeline_rows_appended"),
        ),
        "changed_graph_rows": _as_int(
            item.get("latest_full_pipeline_changed_graph_rows"),
        ),
        "expected_delta": item.get("latest_full_pipeline_expected_delta") or {},
        "expected_delta_source": item.get(
            "latest_full_pipeline_expected_delta_source",
        ),
        "expected_delta_units": _as_float(
            item.get("latest_full_pipeline_expected_delta_units"),
        ),
        "observed_delta_units": _as_float(
            item.get("latest_full_pipeline_observed_delta_units"),
        ),
        "source_work_units": _as_float(
            item.get("latest_full_pipeline_source_work_units"),
        ),
        "work_amplification_ratio": _as_float(
            item.get("latest_full_pipeline_work_amplification_ratio"),
        ),
        "work_amplification_budget": _as_float(
            item.get("latest_full_pipeline_work_amplification_budget"),
        ),
        "observability_status": item.get(
            "latest_full_pipeline_observability_status",
        ),
        "observability_gaps": item.get(
            "latest_full_pipeline_observability_gaps",
        ),
        "no_change_dominant": bool(
            item.get("latest_full_pipeline_no_change_dominant"),
        ),
        "top_widening_stage": item.get(
            "latest_full_pipeline_top_widening_stage",
        ),
        "top_widening_owner": item.get(
            "latest_full_pipeline_top_widening_owner",
        ),
        "top_widening_reason": item.get(
            "latest_full_pipeline_top_widening_reason",
        ),
        "top_widening_elapsed_p95_ms": _as_float(
            item.get("latest_full_pipeline_top_widening_ms"),
        ),
        "publish_lock_wait_p95_ms": _as_float(
            item.get("latest_full_pipeline_lock_wait_p95_ms"),
        ),
        "publish_lock_hold_p95_ms": _as_float(
            item.get("latest_full_pipeline_lock_hold_p95_ms"),
        ),
        "environment_contention": bool(
            item.get("latest_full_pipeline_environment_contention"),
        ),
        "environment_contention_stage": item.get(
            "latest_full_pipeline_environment_stage",
        ),
        "environment_contention_reason": item.get(
            "latest_full_pipeline_environment_reason",
        ),
        "readiness_impact": item.get(
            "latest_full_pipeline_readiness_impact",
        ),
        "artifact": item.get("latest_full_pipeline_artifact"),
        "generated_at": item.get("latest_full_pipeline_generated_at"),
    }


def _root_cause_class(
    *,
    blocker: str,
    coverage_class: str,
    latest_ms: float | None,
    stage_total_ms: float | None,
    amplification_signal: str,
    latency_budget_ms: float,
) -> str:
    if blocker == "scope_support_missing":
        return "missing_scope_support"
    if blocker == "source_sync_not_declared":
        return "source_sync_not_declared"
    if blocker == "runtime_producer_disabled":
        return "runtime_producer_disabled"
    if blocker:
        return "inventory_blocker"
    if coverage_class in {"manual_full_rebuild", "unsupported", "disabled"}:
        return "non_automatic_policy"
    if latest_ms is None:
        return "missing_current_sample"
    if (
        stage_total_ms is not None
        and stage_total_ms > latency_budget_ms
        and latest_ms <= latency_budget_ms
    ):
        return "ingest_stage_overhead"
    if amplification_signal == "dedupe_drops_after_source_work":
        return "source_reemits_stable_facts"
    if amplification_signal == "full_emission_zero_append":
        return "source_reemits_stable_facts"
    if amplification_signal == "zero_delta_fixed_cost":
        return "hidden_full_scope_or_noop_work"
    if latest_ms > latency_budget_ms:
        return "high_source_runner_latency"
    return "needs_stage_attributed_proof"


def _optimization_status(
    *,
    blocker: str,
    coverage_class: str,
    full_pipeline: Mapping[str, Any],
    latest_ms: float | None,
    latency_status: str,
    latency_budget_ms: float,
) -> str:
    if blocker:
        return "blocked_inventory"
    if coverage_class in {"manual_full_rebuild", "unsupported", "disabled"}:
        return "confirm_policy"
    full_pipeline_ms = _as_float(full_pipeline.get("latency_p95_ms"))
    if full_pipeline:
        if bool(full_pipeline.get("ok")) and (
            full_pipeline_ms is None or full_pipeline_ms <= latency_budget_ms
        ):
            return "passing_full_pipeline_benchmark"
        return "failing_budget"
    if latest_ms is None:
        return "needs_benchmark"
    if latency_status == "over_budget":
        return "failing_budget"
    if latency_status == "slow":
        return "slow_useful"
    return "needs_full_pipeline_benchmark"


def _fix_decision(*, optimization_status: str, root_cause: str) -> str:
    if optimization_status == "blocked_inventory":
        if root_cause == "missing_scope_support":
            return "add_scope_support_or_reclassify"
        return "fix_inventory_blocker"
    if optimization_status == "confirm_policy":
        return "confirm_manual_or_disabled_policy"
    if optimization_status == "passing_full_pipeline_benchmark":
        return "monitor_full_pipeline_budget"
    if root_cause == "source_reemits_stable_facts":
        return "stop_reemitting_before_dedupe"
    if root_cause == "hidden_full_scope_or_noop_work":
        return "eliminate_noop_fixed_cost"
    if root_cause == "ingest_stage_overhead":
        return "eliminate_stage_overhead"
    if optimization_status in {"failing_budget", "slow_useful"}:
        return "optimize_adapter_or_reclassify"
    return "run_stage_attributed_benchmarks"


def _proof_status(
    *,
    blocker: str,
    full_pipeline: Mapping[str, Any],
    latest_ms: float | None,
    optimization_status: str,
    latency_budget_ms: float,
) -> str:
    if blocker:
        return "blocked_before_benchmark"
    full_pipeline_ms = _as_float(full_pipeline.get("latency_p95_ms"))
    if full_pipeline:
        if bool(full_pipeline.get("ok")) and (
            full_pipeline_ms is None or full_pipeline_ms <= latency_budget_ms
        ):
            return "full_pipeline_benchmark_passed"
        return "full_pipeline_benchmark_failed"
    if latest_ms is None:
        return "missing_source_runner_sample"
    if optimization_status in {"confirm_policy", "needs_benchmark"}:
        return "missing_benchmark"
    return "latest_runner_sample_only"


def _ledger_next_action(
    *,
    blocker: str,
    optimization_status: str,
    fix_decision: str,
) -> str:
    if blocker:
        return fix_decision
    if optimization_status == "confirm_policy":
        return "confirm_non_hot_path_classification"
    if optimization_status == "passing_full_pipeline_benchmark":
        return "monitor_live_full_pipeline"
    if optimization_status in {"failing_budget", "slow_useful"}:
        return "run_stage_benchmark_then_fix"
    return "run_no_change_and_single_delta_benchmarks"


def _ledger_next_command(
    *,
    source_name: str,
    blocker: str,
    optimization_status: str,
) -> str:
    if blocker:
        return f'rg "{source_name}:" deployments/okg-workspace/source_registry.yaml -n'
    if optimization_status == "confirm_policy":
        return (
            "env -u OKG_DSN uv run okg sync microbench ledger "
            "--deployment okg-workspace --json"
        )
    if optimization_status == "passing_full_pipeline_benchmark":
        return (
            "env -u OKG_DSN uv run okg view inspect --deployment "
            "okg-workspace --view okg_workspace.source_optimization_ledger "
            "--json"
        )
    return (
        "env -u OKG_DSN uv run okg sync microbench inventory "
        "--deployment okg-workspace --json"
    )


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
    "HOT_WAVE_SOURCE_ORDER",
    "MICROBENCH_OPTIMIZATION_LEDGER_ROWS_SCHEMA",
    "MICROBENCH_OPTIMIZATION_LEDGER_SCHEMA",
    "SUBSECOND_LATENCY_BUDGET_MS",
    "build_source_optimization_ledger",
    "rank_source_optimization_rows",
    "source_optimization_ledger_relation_rows",
]
