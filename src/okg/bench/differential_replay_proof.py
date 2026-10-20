"""Delta-proof artifact helpers for commit replay benchmarks.

The replay harness can only prove scopes that existing instrumentation exposes.
This module makes that boundary explicit: available facts are recorded with
values, while missing collectors become loud unavailable/fallback records rather
than zero-like defaults.
"""
from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


DELTA_PROOF_SCHEMA = "okg.bench.differential_delta_proof.v1"
WORK_SEGMENT_SCHEMA = "okg.bench.incremental_work_segment.v1"

BOUNDARY_KEYS = (
    "repo_scope",
    "source_runs",
    "graph_delta",
    "chronos_dirty_scope",
    "extractor_scope",
    "enricher_scope",
    "gate_scope",
    "derived_relation_scope",
    "native_view_scope",
    "lens_scope",
    "maintained_derivation_scope",
    "read_side_operator_scope",
)

REQUIRED_WORK_SEGMENT_KINDS = (
    "source_sync",
    "source_adapter",
    "chronos_graph_apply",
    "extractor",
    "enricher",
    "consistency_gate",
    "mcp_read_side_view",
    "lens",
    "derived_relation",
    "derived_artifact",
    "publisher",
    "read_side_operator",
)

WORK_SEGMENT_REQUIRED_FIELDS = (
    "segment_id",
    "segment_kind",
    "execution_surface",
    "status",
    "dirty_scope_count",
    "reused_scope_count",
    "skipped_scope_count",
    "fallback_reason",
    "output_count",
    "elapsed_ms",
    "runtime_workflow_ref",
    "unavailable_reason",
)

WORK_SEGMENT_STATUSES = frozenset({"available", "fallback", "unavailable"})


def maintained_derivation_boundary(
    console_count_rollup: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Build the ``maintained_derivation_scope`` boundary from the per-generation
    ``checkpoint_metadata["console_count_rollup"]`` summary (console-count-rollup /
    #444 slice 2). The summary carries the maintain mode + forcing reason + the
    rows-touched count (``dirty_bucket_count`` -- the O(changed) numerator, NOT the
    compact-table scan). Absent summary => a LOUD all-None unavailable record (the
    rollup was not maintained for this generation, e.g. the flag was off), NEVER a
    zero-like default. Derived relation application is measured separately as
    ``derived_relation_scope``; this boundary is only for maintained artifacts
    such as ``console_count_rollup``.
    """
    if not console_count_rollup:
        return unavailable_boundary(
            "console_count_rollup_not_maintained_for_generation",
            summary=(
                "the console count rollup was not maintained for this generation "
                "(flag off / blocked) -- no marginal-cost scope to record"
            ),
            rows_touched=None,
            mode=None,
            forcing_reason=None,
        )
    summary = dict(console_count_rollup)
    mode = summary.get("mode")
    return available_boundary(
        f"console count rollup maintained ({mode})",
        rows_touched=summary.get("dirty_bucket_count"),
        mode=mode,
        forcing_reason=summary.get("forcing_reason"),
        rows_written=summary.get("rows_written"),
        rows_deleted=summary.get("rows_deleted"),
    )


def changed_paths_between(
    repo_root: Path,
    before: str,
    after: str,
) -> tuple[str, ...]:
    """Return renamed-aware changed paths between two commits."""
    result = subprocess.run(
        [
            "git",
            "diff",
            "--name-only",
            "--find-renames",
            before,
            after,
        ],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return tuple(sorted(line for line in result.stdout.splitlines() if line))


def available_boundary(
    summary: str,
    **fields: Any,
) -> dict[str, Any]:
    return {"status": "available", "summary": summary, **fields}


def unavailable_boundary(
    reason: str,
    *,
    summary: str | None = None,
    **fields: Any,
) -> dict[str, Any]:
    return {
        "status": "unavailable",
        "reason": reason,
        "summary": summary or reason,
        **fields,
    }


def fallback_boundary(
    reason: str,
    *,
    fallback_to: str,
    summary: str | None = None,
    **fields: Any,
) -> dict[str, Any]:
    return {
        "status": "fallback",
        "reason": reason,
        "fallback_to": fallback_to,
        "summary": summary or reason,
        **fields,
    }


def available_work_segment(
    *,
    segment_id: str,
    segment_kind: str,
    execution_surface: str,
    dirty_scope_count: int,
    reused_scope_count: int,
    skipped_scope_count: int,
    output_count: int,
    elapsed_ms: float | int,
    runtime_workflow_ref: str | None = None,
    fallback_reason: str | None = None,
) -> dict[str, Any]:
    return _work_segment(
        segment_id=segment_id,
        segment_kind=segment_kind,
        execution_surface=execution_surface,
        status="available",
        dirty_scope_count=dirty_scope_count,
        reused_scope_count=reused_scope_count,
        skipped_scope_count=skipped_scope_count,
        fallback_reason=fallback_reason,
        output_count=output_count,
        elapsed_ms=elapsed_ms,
        runtime_workflow_ref=runtime_workflow_ref,
        unavailable_reason=None,
    )


def fallback_work_segment(
    *,
    segment_id: str,
    segment_kind: str,
    execution_surface: str,
    dirty_scope_count: int,
    reused_scope_count: int,
    skipped_scope_count: int,
    fallback_reason: str,
    output_count: int,
    elapsed_ms: float | int,
    runtime_workflow_ref: str | None = None,
) -> dict[str, Any]:
    return _work_segment(
        segment_id=segment_id,
        segment_kind=segment_kind,
        execution_surface=execution_surface,
        status="fallback",
        dirty_scope_count=dirty_scope_count,
        reused_scope_count=reused_scope_count,
        skipped_scope_count=skipped_scope_count,
        fallback_reason=fallback_reason,
        output_count=output_count,
        elapsed_ms=elapsed_ms,
        runtime_workflow_ref=runtime_workflow_ref,
        unavailable_reason=None,
    )


def unavailable_work_segment(
    *,
    segment_id: str,
    segment_kind: str,
    execution_surface: str,
    unavailable_reason: str,
    runtime_workflow_ref: str | None = None,
) -> dict[str, Any]:
    return _work_segment(
        segment_id=segment_id,
        segment_kind=segment_kind,
        execution_surface=execution_surface,
        status="unavailable",
        dirty_scope_count=None,
        reused_scope_count=None,
        skipped_scope_count=None,
        fallback_reason=None,
        output_count=None,
        elapsed_ms=None,
        runtime_workflow_ref=runtime_workflow_ref,
        unavailable_reason=unavailable_reason,
    )


def build_work_segment_report(
    segments: Sequence[Mapping[str, Any]],
    *,
    required_kinds: Sequence[str] = REQUIRED_WORK_SEGMENT_KINDS,
) -> dict[str, Any]:
    rows = [dict(segment) for segment in segments]
    violations = validate_work_segments(rows, required_kinds=required_kinds)
    return {
        "schema": WORK_SEGMENT_SCHEMA,
        "ok": not violations,
        "required_segment_kinds": list(required_kinds),
        "segment_count": len(rows),
        "segments": rows,
        "violations": violations,
    }


def work_segments_from_delta_row(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    boundaries = (
        row.get("boundaries")
        if isinstance(row.get("boundaries"), Mapping)
        else {}
    )
    repo_scope = _boundary(boundaries, "repo_scope")
    graph_delta = _boundary(boundaries, "graph_delta")
    source_runs = _boundary(boundaries, "source_runs")
    chronos_dirty_scope = _boundary(boundaries, "chronos_dirty_scope")
    enricher_scope = _boundary(boundaries, "enricher_scope")
    gate_scope = _boundary(boundaries, "gate_scope")
    native_view_scope = _boundary(boundaries, "native_view_scope")
    derived_relation_scope = _boundary(boundaries, "derived_relation_scope")
    maintained_scope = _boundary(boundaries, "maintained_derivation_scope")
    extractor_scope = _boundary(boundaries, "extractor_scope")
    lens_scope = _boundary(boundaries, "lens_scope")
    read_side_operator_scope = _boundary(boundaries, "read_side_operator_scope")

    changed_count = _int_or_none(repo_scope.get("scope_count")) or 0
    graph_dirty_count = _dirty_graph_scope_count(
        chronos_dirty_scope,
        default=changed_count,
    )
    e2e_ms = _seconds_to_ms(row.get("end_to_end_s"))
    source_sync_ref = row.get("source_sync_workflow_ref")

    return [
        _source_sync_work_segment(row, changed_count, e2e_ms, source_sync_ref),
        _source_adapter_work_segment(row, source_runs),
        _chronos_apply_work_segment(row, graph_delta, graph_dirty_count),
        _boundary_work_segment(
            row,
            segment_kind="extractor",
            execution_surface="ChronosExtractorStage",
            boundary=extractor_scope,
            unavailable_reason="extractor_scope_not_collected_for_replay_row",
        ),
        _boundary_work_segment(
            row,
            segment_kind="enricher",
            execution_surface="deployment_enricher_chain",
            boundary=enricher_scope,
            unavailable_reason="enricher_incremental_scope_not_collected",
        ),
        _boundary_work_segment(
            row,
            segment_kind="consistency_gate",
            execution_surface="TransformConsistencyGate",
            boundary=gate_scope,
            unavailable_reason="consistency_gate_scope_mode_not_collected",
        ),
        _boundary_work_segment(
            row,
            segment_kind="mcp_read_side_view",
            execution_surface="MCP/read-side views",
            boundary=native_view_scope,
            unavailable_reason="native_view_dirty_scope_not_collected",
        ),
        _boundary_work_segment(
            row,
            segment_kind="lens",
            execution_surface="src/okg/substrate/lenses",
            boundary=lens_scope,
            unavailable_reason="lens_scope_not_collected_for_replay_row",
        ),
        _boundary_work_segment(
            row,
            segment_kind="derived_relation",
            execution_surface="publisher.derived_fact_apply",
            boundary=derived_relation_scope,
            unavailable_reason="derived_relation_scope_not_collected_for_replay_row",
        ),
        _derived_artifact_work_segment(row, maintained_scope),
        _publisher_work_segment(row, graph_delta, graph_dirty_count),
        _boundary_work_segment(
            row,
            segment_kind="read_side_operator",
            execution_surface="AKMON operator read surface",
            boundary=read_side_operator_scope,
            unavailable_reason="read_side_operator_scope_not_collected_for_replay_row",
        ),
    ]


def assert_work_segment_report_valid(
    segments: Sequence[Mapping[str, Any]],
    *,
    required_kinds: Sequence[str] = REQUIRED_WORK_SEGMENT_KINDS,
) -> None:
    violations = validate_work_segments(segments, required_kinds=required_kinds)
    if violations:
        raise ValueError("; ".join(violations))


def validate_work_segments(
    segments: Sequence[Mapping[str, Any]],
    *,
    required_kinds: Sequence[str] = REQUIRED_WORK_SEGMENT_KINDS,
) -> list[str]:
    violations: list[str] = []
    seen_kinds: set[str] = set()
    seen_ids: set[str] = set()

    for index, raw_segment in enumerate(segments):
        segment = dict(raw_segment)
        label = str(segment.get("segment_id") or f"index:{index}")
        missing_fields = [
            field
            for field in WORK_SEGMENT_REQUIRED_FIELDS
            if field not in segment
        ]
        for field in missing_fields:
            violations.append(f"{label}: missing required field {field}")

        if _contains_not_collected(segment):
            violations.append(f"{label}: 'not collected' is not valid evidence")

        segment_id = segment.get("segment_id")
        if isinstance(segment_id, str) and segment_id:
            if segment_id in seen_ids:
                violations.append(f"{label}: duplicate segment_id")
            seen_ids.add(segment_id)
        else:
            violations.append(f"{label}: segment_id must be non-empty text")

        kind = segment.get("segment_kind")
        if isinstance(kind, str) and kind:
            seen_kinds.add(kind)
        else:
            violations.append(f"{label}: segment_kind must be non-empty text")

        status = segment.get("status")
        if status not in WORK_SEGMENT_STATUSES:
            violations.append(f"{label}: invalid status {status!r}")

        execution_surface = segment.get("execution_surface")
        if not isinstance(execution_surface, str) or not execution_surface:
            violations.append(f"{label}: execution_surface must be non-empty text")

        if status == "fallback" and not segment.get("fallback_reason"):
            violations.append(f"{label}: fallback segment needs fallback_reason")
        if status == "fallback":
            dirty_scope_count = _int_or_none(segment.get("dirty_scope_count"))
            output_count = _int_or_none(segment.get("output_count"))
            if dirty_scope_count is None:
                violations.append(
                    f"{label}: fallback segment needs measured dirty_scope_count"
                )
            elif dirty_scope_count == 0 and (output_count or 0) > 0:
                violations.append(
                    f"{label}: fallback segment with output needs nonzero "
                    "dirty_scope_count"
                )
        if status == "unavailable" and not segment.get("unavailable_reason"):
            violations.append(
                f"{label}: unavailable segment needs unavailable_reason"
            )
        if status == "available" and segment.get("unavailable_reason"):
            violations.append(
                f"{label}: available segment cannot carry unavailable_reason"
            )

    for required_kind in required_kinds:
        if required_kind not in seen_kinds:
            violations.append(f"missing required segment kind {required_kind}")

    return violations


def source_runs_boundary_from_diagnostics_commit(
    commit_row: Mapping[str, Any],
) -> dict[str, Any]:
    """Build a source-run proof boundary from extract.py per-commit diagnostics.

    ``commit_replay.py`` writes the initial delta proof before source-progress
    rows are correlated to replay commits. ``bench/extract.py`` performs that
    correlation later, so the orchestrator can use this helper to enrich the
    proof without weakening the replay-time contract.
    """

    diag_status = str(commit_row.get("source_diag_status") or "")
    diag_reason = str(commit_row.get("source_diag_reason") or "")
    raw_runs = commit_row.get("source_runs")
    if diag_status == "unavailable" or not isinstance(raw_runs, list):
        return unavailable_boundary(
            diag_reason or "source_run_diagnostics_unavailable",
            summary=(
                "source-run emitted node/edge counts were not available "
                "for this replay row"
            ),
            emitted_node_count=None,
            emitted_edge_count=None,
            dirty_scopes=None,
        )

    source_breakdown: dict[str, dict[str, Any]] = {}
    dirty_scopes: set[str] = set()
    emitted_node_count = 0
    emitted_edge_count = 0
    retracted_node_count = 0
    retracted_edge_count = 0
    output_count = 0
    input_count = 0

    for raw_run in raw_runs:
        if not isinstance(raw_run, Mapping):
            continue
        source = str(
            raw_run.get("registry_source_name")
            or raw_run.get("source_name")
            or "unknown"
        )
        stats = (
            raw_run.get("source_stats")
            if isinstance(raw_run.get("source_stats"), Mapping)
            else {}
        )
        source_row = source_breakdown.setdefault(
            source,
            {
                "run_count": 0,
                "statuses": {},
                "scope_statuses": {},
                "emitted_node_count": 0,
                "emitted_edge_count": 0,
                "retracted_node_count": 0,
                "retracted_edge_count": 0,
                "output_count": 0,
                "input_count": 0,
                "total_ms": 0.0,
                "max_lsp_requests": 0,
                "max_changed_files": 0,
            },
        )
        source_row["run_count"] += 1
        status = str(raw_run.get("status") or "unknown")
        source_row["statuses"][status] = source_row["statuses"].get(status, 0) + 1
        scope_status = str(stats.get("scope_status") or raw_run.get("run_mode") or "")
        if scope_status:
            source_row["scope_statuses"][scope_status] = (
                source_row["scope_statuses"].get(scope_status, 0) + 1
            )

        nodes = _int_value(raw_run.get("nodes_appended"))
        edges = _int_value(raw_run.get("edges_appended"))
        node_retractions = _int_value(raw_run.get("nodes_retracted"))
        edge_retractions = _int_value(raw_run.get("edges_retracted"))
        outputs = _int_value(raw_run.get("output_count"))
        inputs = _int_value(raw_run.get("input_count"))
        emitted_node_count += nodes
        emitted_edge_count += edges
        retracted_node_count += node_retractions
        retracted_edge_count += edge_retractions
        output_count += outputs
        input_count += inputs
        source_row["emitted_node_count"] += nodes
        source_row["emitted_edge_count"] += edges
        source_row["retracted_node_count"] += node_retractions
        source_row["retracted_edge_count"] += edge_retractions
        source_row["output_count"] += outputs
        source_row["input_count"] += inputs
        source_row["total_ms"] = round(
            float(source_row["total_ms"])
            + (_float_value(raw_run.get("total_ms")) or 0.0),
            3,
        )
        source_row["max_lsp_requests"] = max(
            int(source_row["max_lsp_requests"]),
            _int_value(stats.get("lsp_requests")),
        )
        source_row["max_changed_files"] = max(
            int(source_row["max_changed_files"]),
            _int_value(stats.get("changed_file_count")),
        )

        scope_keys = stats.get("scope_keys")
        if isinstance(scope_keys, str):
            scope_key_iterable: Sequence[Any] = (scope_keys,)
        elif isinstance(scope_keys, Sequence):
            scope_key_iterable = scope_keys
        else:
            scope_key_iterable = ()
        for scope_key in scope_key_iterable:
            text = str(scope_key)
            if text:
                dirty_scopes.add(text)

    breakdown_rows = [
        {"source": source, **values}
        for source, values in sorted(source_breakdown.items())
    ]
    return available_boundary(
        "source-run emitted fact counts joined from replay diagnostics",
        source_diag_status=diag_status or "available",
        source_diag_reason=diag_reason,
        run_count=sum(int(row["run_count"]) for row in source_breakdown.values()),
        source_count=len(source_breakdown),
        emitted_node_count=emitted_node_count,
        emitted_edge_count=emitted_edge_count,
        retracted_node_count=retracted_node_count,
        retracted_edge_count=retracted_edge_count,
        output_count=output_count,
        input_count=input_count,
        source_total_ms=_float_value(commit_row.get("source_total_ms")),
        dirty_scope_count=len(dirty_scopes),
        dirty_scopes=sorted(dirty_scopes)[:50],
        source_breakdown=breakdown_rows,
    )


def chronos_dirty_scope_boundary_from_projection_deltas(
    projection_deltas: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(projection_deltas, Mapping):
        return unavailable_boundary(
            "projection_deltas_not_persisted_for_generation",
            dirty_node_count=None,
            dirty_edge_count=None,
            dirty_nodes=None,
            dirty_edges=None,
        )
    dirty_node_count = (
        _int_value(projection_deltas.get("nodes_added"))
        + _int_value(projection_deltas.get("nodes_updated"))
        + _int_value(projection_deltas.get("nodes_retracted"))
    )
    dirty_edge_count = (
        _int_value(projection_deltas.get("edges_added"))
        + _int_value(projection_deltas.get("edges_updated"))
        + _int_value(projection_deltas.get("edges_retracted"))
    )
    return available_boundary(
        "Chronos projection deltas persisted on the generation checkpoint",
        dirty_node_count=dirty_node_count,
        dirty_edge_count=dirty_edge_count,
        dirty_nodes=None,
        dirty_edges=None,
        projection_deltas=dict(projection_deltas),
    )


def _gate_coverage_is_certified(result: Mapping[str, Any]) -> bool:
    """True iff this gate result certifies exact coverage of what it attempted.

    Two shapes qualify, and the distinction is the point of this helper:

    * ``coverage_complete = true`` -- the run swept the whole candidate universe.
    * SCOPED-EXACT -- the run swept a delta, exactly, and says so on the gate row
      (``details.scoped_exact``). ``coverage_complete`` is FALSE on such a run by
      the schema's ``CHECK (NOT (coverage_complete AND scoped))``, so reading
      ``coverage_complete is not True`` as "incomplete coverage" downgraded the
      consistency-gate boundary to ``fallback`` on exactly the publishes where
      the gate WAS incremental -- the runs this proof exists to certify.

    Anything else (a missing field, a non-exact basis, a heuristic or sampled
    run) is NOT certified: absence is never read as agreement here.
    """

    if result.get("coverage_complete") is True:
        return True
    details = result.get("details")
    if not isinstance(details, Mapping):
        return False
    return (
        details.get("scoped") is True
        and details.get("scoped_exact") is True
        # Same exact-basis vocabulary the consistency gate algebra uses
        # (okg.substrate.consistency.observations.EXACT_COVERAGE_BASES).
        and str(details.get("scope_coverage_basis") or "")
        in ("exact_postgres", "exact_snapshot")
    )


def consistency_gate_boundary_from_invariant_summary(
    invariant_summary: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(invariant_summary, Mapping):
        return unavailable_boundary(
            "invariant_summary_not_persisted_for_generation",
            scope_mode=None,
            fallback_reason=None,
        )
    raw_gate_summary = invariant_summary.get("consistency_gates")
    gate_summary = (
        raw_gate_summary if isinstance(raw_gate_summary, Mapping) else {}
    )
    raw_results = gate_summary.get("results")
    if not isinstance(raw_results, Sequence) or isinstance(
        raw_results,
        (str, bytes),
    ):
        return unavailable_boundary(
            "consistency_gate_results_not_persisted_for_generation",
            scope_mode=None,
            fallback_reason=None,
        )

    results = [
        dict(result)
        for result in raw_results
        if isinstance(result, Mapping)
    ]
    if not results:
        return unavailable_boundary(
            "consistency_gate_results_empty_for_generation",
            scope_mode=None,
            fallback_reason=None,
        )

    elapsed_ms = 0.0
    dirty_node_count = 0
    dirty_edge_count = 0
    scoped_edge_count = 0
    missing_scope = False
    incomplete_coverage = False
    universe_complete = True
    scoped_exact_count = 0
    scope_modes: set[str] = set()
    for result in results:
        elapsed_ms += _float_value(result.get("elapsed_ms")) or 0.0
        details = (
            result.get("details")
            if isinstance(result.get("details"), Mapping)
            else {}
        )
        scope_mode = str(details.get("scope_mode") or "")
        if scope_mode:
            scope_modes.add(scope_mode)
        preflight = (
            result.get("preflight")
            if isinstance(result.get("preflight"), Mapping)
            else {}
        )
        node_count = _int_or_none(preflight.get("dirty_node_count"))
        edge_count = _int_or_none(preflight.get("dirty_edge_count"))
        scoped_count = _int_or_none(preflight.get("scoped_edge_count"))
        if node_count is None or edge_count is None:
            missing_scope = True
        else:
            dirty_node_count += node_count
            dirty_edge_count += edge_count
        if scoped_count is not None:
            scoped_edge_count += scoped_count
        if not _gate_coverage_is_certified(result):
            incomplete_coverage = True
        if result.get("coverage_complete") is not True:
            universe_complete = False
            if _gate_coverage_is_certified(result):
                scoped_exact_count += 1

    output_count = len(results)
    if missing_scope or incomplete_coverage:
        reason = (
            "consistency_gate_dirty_scope_missing"
            if missing_scope
            else "consistency_gate_coverage_incomplete"
        )
        return fallback_boundary(
            reason,
            fallback_to="gate_result_count",
            summary=(
                "consistency gate result was persisted, but the checkpoint did "
                "not prove a complete dirty scope"
            ),
            scope_mode=_scope_mode_label(scope_modes),
            dirty_scope_count=dirty_node_count + dirty_edge_count,
            reused_scope_count=0,
            skipped_scope_count=0,
            output_count=output_count,
            elapsed_ms=round(elapsed_ms, 3),
            gate_count=output_count,
            scoped_edge_count=scoped_edge_count,
            passed_count=_int_value(gate_summary.get("passed")),
            blocked_count=_int_value(gate_summary.get("blocked")),
        )

    return available_boundary(
        "consistency gates evaluated against persisted Chronos dirty scope",
        scope_mode=_scope_mode_label(scope_modes),
        dirty_scope_count=dirty_node_count + dirty_edge_count,
        dirty_node_count=dirty_node_count,
        dirty_edge_count=dirty_edge_count,
        reused_scope_count=0,
        skipped_scope_count=0,
        output_count=output_count,
        elapsed_ms=round(elapsed_ms, 3),
        gate_count=output_count,
        scoped_edge_count=scoped_edge_count,
        passed_count=_int_value(gate_summary.get("passed")),
        blocked_count=_int_value(gate_summary.get("blocked")),
        # MEASURED, not hardcoded. Reaching this branch means every result
        # certified exact coverage; it does NOT mean every result swept the whole
        # universe, and reporting `coverage_complete=True` for a scoped-exact run
        # would restate the very over-claim the gate stopped making.
        coverage_complete=universe_complete,
        scoped_exact_count=scoped_exact_count,
    )


def enricher_scope_boundary_from_governance(
    governance: Mapping[str, Any] | None,
    *,
    dirty_scope_count: int | None = None,
    elapsed_ms: float | int | None = None,
) -> dict[str, Any]:
    if not isinstance(governance, Mapping):
        return unavailable_boundary(
            "enricher_governance_not_persisted_for_generation",
            scope_mode=None,
            derived_edge_count=None,
            fallback_reason=None,
        )
    raw_records = governance.get("records")
    if not isinstance(raw_records, Sequence) or isinstance(
        raw_records,
        (str, bytes),
    ):
        return unavailable_boundary(
            "enricher_governance_records_not_persisted_for_generation",
            scope_mode=None,
            derived_edge_count=None,
            fallback_reason=None,
        )
    records = [
        dict(record)
        for record in raw_records
        if isinstance(record, Mapping)
    ]
    if not records:
        return available_boundary(
            "enricher governance recorded zero output for this generation",
            scope_mode="governance_records",
            dirty_scope_count=0,
            reused_scope_count=0,
            skipped_scope_count=1,
            derived_edge_count=0,
            output_count=0,
            elapsed_ms=0.0,
            record_count=0,
        )

    emitted = 0
    skipped = 0
    fallback_reasons: dict[str, int] = {}
    for record in records:
        edge_counts = (
            record.get("edge_counts")
            if isinstance(record.get("edge_counts"), Mapping)
            else {}
        )
        emitted += _int_value(edge_counts.get("emitted"))
        skipped += _int_value(edge_counts.get("skipped"))
        reason = record.get("fallback_reason") or record.get("failure_reason")
        if reason:
            text = str(reason)
            fallback_reasons[text] = fallback_reasons.get(text, 0) + 1

    return fallback_boundary(
        "enricher_dirty_scope_not_persisted",
        fallback_to="chronos_dirty_scope+enricher_governance_edge_counts",
        summary=(
            "enricher governance records report emitted/skipped outputs, but "
            "do not yet persist a narrower enricher-specific dirty input scope"
        ),
        scope_mode="governance_records",
        dirty_scope_count=dirty_scope_count,
        reused_scope_count=0,
        skipped_scope_count=skipped,
        derived_edge_count=emitted,
        output_count=emitted + skipped,
        elapsed_ms=_float_value(elapsed_ms) or 0.0,
        record_count=len(records),
        fallback_reasons=fallback_reasons,
    )


def native_view_scope_boundary_from_diagnostics_commit(
    commit_row: Mapping[str, Any],
) -> dict[str, Any]:
    publish = _select_commit_publish(commit_row)
    if publish is None:
        return unavailable_boundary(
            "publish_diagnostics_unavailable_for_replay_row",
            dirty_scope_count=None,
            reused_scope_count=None,
            rewritten_output_count=None,
        )
    attr_refresh = (
        publish.get("attribute_effective_view_refresh")
        if isinstance(publish.get("attribute_effective_view_refresh"), Mapping)
        else {}
    )
    timings = (
        publish.get("timings_ms")
        if isinstance(publish.get("timings_ms"), Mapping)
        else {}
    )
    attr_status = str(attr_refresh.get("status") or "")
    node_rows = _int_or_none(attr_refresh.get("node_rows"))
    edge_rows = _int_or_none(attr_refresh.get("edge_rows"))
    if attr_status == "materialized" and node_rows is not None and edge_rows is not None:
        output_count = node_rows + edge_rows
        native_rows = _native_view_rows(publish.get("native_views"))
        elapsed_ms = (
            (_float_value(timings.get("attribute_effective_view_materialize")) or 0.0)
            + (
                _float_value(timings.get("attribute_effective_attrs_materialize"))
                or 0.0
            )
            + (_float_value(timings.get("native_views")) or 0.0)
        )
        return fallback_boundary(
            "attribute_effective_full_materialization",
            fallback_to="attribute_effective_rows",
            summary="attribute-effective read-side view materialized for generation",
            dirty_scope_count=output_count + native_rows["dirty_scope_count"],
            reused_scope_count=native_rows["reused_scope_count"],
            skipped_scope_count=0,
            rewritten_output_count=output_count + native_rows["output_count"],
            output_count=output_count + native_rows["output_count"],
            elapsed_ms=round(elapsed_ms, 3),
            attribute_effective_view_refresh=dict(attr_refresh),
            native_view_refresh=(
                dict(publish.get("native_view_refresh"))
                if isinstance(publish.get("native_view_refresh"), Mapping)
                else {}
            ),
            native_view_count=native_rows["view_count"],
        )

    return unavailable_boundary(
        "attribute_effective_view_not_materialized_for_replay_row",
        summary=(
            "publish diagnostics did not contain a materialized "
            "attribute-effective read-side view for this replay row"
        ),
        dirty_scope_count=None,
        reused_scope_count=None,
        rewritten_output_count=None,
        attribute_effective_status=attr_status or None,
        attribute_effective_reason=attr_refresh.get("reason"),
    )


def extractor_scope_boundary_from_diagnostics_commit(
    commit_row: Mapping[str, Any],
) -> dict[str, Any]:
    publish = _select_commit_publish(commit_row)
    if publish is None:
        return unavailable_boundary(
            "publish_diagnostics_unavailable_for_replay_row",
            dirty_scope_count=None,
            reused_scope_count=None,
            skipped_scope_count=None,
            output_count=None,
        )
    timings = _publish_timings(publish)
    check_ms = _float_value(timings.get("extractor_dirty_input_check"))
    run_ms = _float_value(timings.get("extractor"))
    emitted = _int_or_none(publish.get("extractor_edges_emitted"))
    if run_ms is not None:
        return fallback_boundary(
            "extractor_dirty_input_scope_not_persisted",
            fallback_to="extractor_edges_emitted",
            summary=(
                "publisher reported extractor execution and emitted edges, "
                "but not the dirty input row count"
            ),
            dirty_scope_count=0,
            reused_scope_count=0,
            skipped_scope_count=0,
            output_count=emitted or 0,
            elapsed_ms=round(run_ms + (check_ms or 0.0), 3),
            extractor_edges_emitted=emitted,
        )
    if check_ms is not None:
        return available_boundary(
            "extractor dirty-input probe found no extractor work for this publish",
            dirty_scope_count=0,
            reused_scope_count=0,
            skipped_scope_count=1,
            output_count=0,
            elapsed_ms=round(check_ms, 3),
            extractor_edges_emitted=emitted or 0,
        )
    return unavailable_boundary(
        "extractor_diagnostics_not_persisted_for_replay_row",
        dirty_scope_count=None,
        reused_scope_count=None,
        skipped_scope_count=None,
        output_count=None,
    )


def lens_scope_boundary_from_diagnostics_commit(
    commit_row: Mapping[str, Any],
) -> dict[str, Any]:
    publish = _select_commit_publish(commit_row)
    if publish is None:
        return unavailable_boundary(
            "publish_diagnostics_unavailable_for_replay_row",
            dirty_scope_count=None,
            reused_scope_count=None,
            skipped_scope_count=None,
            output_count=None,
        )
    timings = _publish_timings(publish)
    lens_ms = _float_value(timings.get("lenses"))
    promotion_ms = _float_value(timings.get("lens_promotion"))
    raw_lenses = publish.get("lenses")
    lenses = (
        [dict(item) for item in raw_lenses if isinstance(item, Mapping)]
        if isinstance(raw_lenses, Sequence) and not isinstance(raw_lenses, (str, bytes))
        else []
    )
    elapsed_ms = round((lens_ms or 0.0) + (promotion_ms or 0.0), 3)
    if lenses:
        return available_boundary(
            "publisher registered configured lenses for this generation",
            dirty_scope_count=len(lenses),
            reused_scope_count=0,
            skipped_scope_count=0,
            output_count=len(lenses),
            elapsed_ms=elapsed_ms,
            lens_count=len(lenses),
            lenses=lenses,
        )
    if lens_ms is not None or promotion_ms is not None:
        return available_boundary(
            "publisher lens phases ran with no configured or pending lens work",
            dirty_scope_count=0,
            reused_scope_count=0,
            skipped_scope_count=1,
            output_count=0,
            elapsed_ms=elapsed_ms,
            lens_count=0,
        )
    return unavailable_boundary(
        "lens_diagnostics_not_persisted_for_replay_row",
        dirty_scope_count=None,
        reused_scope_count=None,
        skipped_scope_count=None,
        output_count=None,
    )


def derived_relation_scope_boundary_from_diagnostics_commit(
    commit_row: Mapping[str, Any],
) -> dict[str, Any]:
    publish = _select_commit_publish(commit_row)
    if publish is None:
        return unavailable_boundary(
            "publish_diagnostics_unavailable_for_replay_row",
            dirty_scope_count=None,
            reused_scope_count=None,
            rewritten_output_count=None,
        )
    summary = (
        publish.get("derived_fact_summary")
        if isinstance(publish.get("derived_fact_summary"), Mapping)
        else {}
    )
    total_loaded = _int_or_none(summary.get("total_loaded_count"))
    total_applied = _int_or_none(summary.get("total_applied_count"))
    timings = _publish_timings(publish)
    elapsed_ms = sum(
        _float_value(timings.get(key)) or 0.0
        for key in (
            "derived_fact_load",
            "derived_fact_apply",
            "view_derived_fact_load",
            "view_derived_fact_apply",
        )
    )
    if total_loaded is not None and total_applied is not None:
        return available_boundary(
            "publisher reported derived fact load/apply counts",
            dirty_scope_count=total_loaded,
            reused_scope_count=0,
            skipped_scope_count=0,
            rewritten_output_count=total_applied,
            output_count=total_applied,
            elapsed_ms=round(elapsed_ms, 3),
            derived_fact_summary=dict(summary),
        )
    if elapsed_ms:
        return fallback_boundary(
            "derived_fact_counts_not_persisted",
            fallback_to="derived_fact_phase_timing",
            dirty_scope_count=0,
            reused_scope_count=0,
            skipped_scope_count=0,
            rewritten_output_count=0,
            output_count=0,
            elapsed_ms=round(elapsed_ms, 3),
        )
    return unavailable_boundary(
        "derived_relation_diagnostics_not_persisted_for_replay_row",
        dirty_scope_count=None,
        reused_scope_count=None,
        rewritten_output_count=None,
    )


def read_side_operator_boundary_from_report(
    report: Mapping[str, Any] | None,
    *,
    target_generation_id: Any | None = None,
) -> dict[str, Any]:
    if not isinstance(report, Mapping):
        return unavailable_boundary(
            "read_side_report_not_persisted_for_replay",
            dirty_scope_count=None,
            reused_scope_count=None,
            skipped_scope_count=None,
            output_count=None,
        )
    if (
        target_generation_id is not None
        and str(report.get("generation_id") or "") != str(target_generation_id)
    ):
        return unavailable_boundary(
            "read_side_report_generation_mismatch",
            summary="operator read-side benchmark was not run for this replay row",
            dirty_scope_count=0,
            reused_scope_count=0,
            skipped_scope_count=1,
            output_count=0,
            elapsed_ms=0.0,
            generation_id=target_generation_id,
            benchmark_generation_id=report.get("generation_id"),
        )
    raw_reads = report.get("reads")
    if not isinstance(raw_reads, Mapping):
        return unavailable_boundary(
            "read_side_report_missing_reads",
            dirty_scope_count=None,
            reused_scope_count=None,
            skipped_scope_count=None,
            output_count=None,
        )
    reads = {
        str(name): dict(value)
        for name, value in raw_reads.items()
        if isinstance(value, Mapping)
    }
    ok_reads = {
        name: value
        for name, value in reads.items()
        if value.get("status") == "ok"
    }
    skipped_reads = {
        name: value
        for name, value in reads.items()
        if value.get("status") == "skipped"
    }
    output_count = sum(_int_value(value.get("rows")) for value in ok_reads.values())
    elapsed_ms = sum(
        _float_value(value.get("p50_ms")) or 0.0
        for value in ok_reads.values()
    )
    return available_boundary(
        "operator read-side benchmark executed against final generation",
        dirty_scope_count=len(ok_reads),
        reused_scope_count=0,
        skipped_scope_count=len(skipped_reads),
        output_count=output_count,
        elapsed_ms=round(elapsed_ms, 3),
        generation_id=report.get("generation_id"),
        read_count=len(ok_reads),
        skipped_read_count=len(skipped_reads),
        reads=reads,
    )


def build_delta_proof_row(
    *,
    i: int,
    commit_sha: str,
    previous_commit_sha: str,
    subject: str,
    changed_paths: Sequence[str],
    line_delta: Mapping[str, Any],
    previous_generation_id: int | str | None,
    generation_id: int | str | None,
    generation_metrics: Mapping[str, Any] | None = None,
    end_to_end_s: float | None = None,
    db_size_bytes: int | None = None,
    source_runs: Mapping[str, Any] | None = None,
    chronos_dirty_scope: Mapping[str, Any] | None = None,
    enricher_scope: Mapping[str, Any] | None = None,
    gate_scope: Mapping[str, Any] | None = None,
    derived_relation_scope: Mapping[str, Any] | None = None,
    native_view_scope: Mapping[str, Any] | None = None,
    maintained_derivation_scope: Mapping[str, Any] | None = None,
    extractor_scope: Mapping[str, Any] | None = None,
    lens_scope: Mapping[str, Any] | None = None,
    read_side_operator_scope: Mapping[str, Any] | None = None,
    source_sync_workflow_ref: str | None = None,
) -> dict[str, Any]:
    """Build one per-commit proof row.

    Optional boundary mappings allow future collectors to slot in without
    changing the artifact schema. Missing mappings are explicit
    ``unavailable`` records.
    """
    normalized_paths = tuple(sorted(str(path) for path in changed_paths))
    metrics = dict(generation_metrics or {})
    boundaries: dict[str, Any] = {
        "repo_scope": available_boundary(
            "git diff path scope between replay commits",
            previous_commit=previous_commit_sha,
            current_commit=commit_sha,
            changed_paths=list(normalized_paths),
            scope_count=len(normalized_paths),
        ),
        "source_runs": dict(source_runs) if source_runs is not None else (
            unavailable_boundary(
                "source_run_emitted_fact_scope_not_collected",
                summary=(
                    "source-run emitted node/edge counts are not yet joined "
                    "to commit replay rows"
                ),
                emitted_node_count=None,
                emitted_edge_count=None,
                dirty_scopes=None,
            )
        ),
        "graph_delta": _graph_delta_boundary(generation_id, metrics),
        "chronos_dirty_scope": (
            dict(chronos_dirty_scope)
            if chronos_dirty_scope is not None
            else unavailable_boundary(
                "chronos_dirty_scope_not_persisted_for_replay_row",
                dirty_node_count=None,
                dirty_edge_count=None,
                dirty_nodes=None,
                dirty_edges=None,
            )
        ),
        "extractor_scope": (
            dict(extractor_scope)
            if extractor_scope is not None
            else unavailable_boundary(
                "extractor_scope_not_collected_for_replay_row",
                dirty_scope_count=None,
                reused_scope_count=None,
                skipped_scope_count=None,
                output_count=None,
            )
        ),
        "enricher_scope": dict(enricher_scope) if enricher_scope is not None else (
            unavailable_boundary(
                "enricher_incremental_scope_not_collected",
                scope_mode=None,
                derived_edge_count=None,
                fallback_reason=None,
            )
        ),
        "gate_scope": dict(gate_scope) if gate_scope is not None else (
            unavailable_boundary(
                "consistency_gate_scope_mode_not_collected",
                scope_mode=None,
                fallback_reason=None,
            )
        ),
        "derived_relation_scope": (
            dict(derived_relation_scope)
            if derived_relation_scope is not None
            else unavailable_boundary(
                "derived_relation_dirty_scope_not_collected",
                dirty_scope_count=None,
                reused_scope_count=None,
                rewritten_output_count=None,
            )
        ),
        "native_view_scope": (
            dict(native_view_scope)
            if native_view_scope is not None
            else unavailable_boundary(
                "native_view_dirty_scope_not_collected",
                dirty_scope_count=None,
                reused_scope_count=None,
                rewritten_output_count=None,
            )
        ),
        "lens_scope": (
            dict(lens_scope)
            if lens_scope is not None
            else unavailable_boundary(
                "lens_scope_not_collected_for_replay_row",
                dirty_scope_count=None,
                reused_scope_count=None,
                skipped_scope_count=None,
                output_count=None,
            )
        ),
        # console-count-rollup (#444 slice 2): the maintained-derivation marginal-cost
        # scope. Unlike native_view_scope / derived_relation_scope above (declared but
        # never passed -> permanently unavailable), the commit-replay harness DOES pass
        # this from the per-generation checkpoint metadata, so it records an `available`
        # rows-touched marginal-cost record (`dirty_bucket_count`, the O(changed)
        # numerator) on a maintained cycle. Absent => loud all-None.
        "maintained_derivation_scope": (
            dict(maintained_derivation_scope)
            if maintained_derivation_scope is not None
            else unavailable_boundary(
                "maintained_derivation_scope_not_collected",
                rows_touched=None,
                mode=None,
                forcing_reason=None,
            )
        ),
        "read_side_operator_scope": (
            dict(read_side_operator_scope)
            if read_side_operator_scope is not None
            else unavailable_boundary(
                "read_side_operator_scope_not_collected_for_replay_row",
                dirty_scope_count=None,
                reused_scope_count=None,
                skipped_scope_count=None,
                output_count=None,
            )
        ),
    }

    row = {
        "i": i,
        "commit": commit_sha[:10],
        "commit_sha": commit_sha,
        "previous_commit_sha": previous_commit_sha,
        "subject": subject,
        "previous_generation_id": previous_generation_id,
        "generation_id": generation_id,
        "line_delta": {
            "files_changed": line_delta.get("files_changed"),
            "lines_added": line_delta.get("lines_added"),
            "lines_deleted": line_delta.get("lines_deleted"),
        },
        "end_to_end_s": end_to_end_s,
        "db_size_bytes": db_size_bytes,
        "source_sync_workflow_ref": source_sync_workflow_ref,
        "boundaries": boundaries,
    }
    row["work_segments"] = work_segments_from_delta_row(row)
    row["fallbacks"] = _fallbacks_for(boundaries)
    return row


def build_delta_proof(
    *,
    experiment_id: str,
    deployment: str,
    repo_root: Path,
    baseline_commit: str,
    baseline_generation_id: int | str | None,
    rows: Sequence[Mapping[str, Any]],
    generated_at: str | None = None,
) -> dict[str, Any]:
    proof_rows = [dict(row) for row in rows]
    return {
        "schema": DELTA_PROOF_SCHEMA,
        "experiment_id": experiment_id,
        "deployment": deployment,
        "repo_root": str(repo_root),
        "baseline_commit": baseline_commit,
        "baseline_generation_id": baseline_generation_id,
        "generated_at": generated_at or datetime.now(
            timezone.utc
        ).isoformat(),
        "rows": proof_rows,
        "summary": summarize_delta_proof_rows(proof_rows),
    }


def summarize_delta_proof_rows(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    status_counts: dict[str, dict[str, int]] = {
        key: {} for key in BOUNDARY_KEYS
    }
    unavailable = 0
    fallback = 0
    for row in rows:
        boundaries = row.get("boundaries", {})
        if not isinstance(boundaries, Mapping):
            continue
        for key in BOUNDARY_KEYS:
            boundary = boundaries.get(key)
            if not isinstance(boundary, Mapping):
                status = "missing"
            else:
                status = str(boundary.get("status") or "missing")
            status_counts[key][status] = status_counts[key].get(status, 0) + 1
            if status == "unavailable":
                unavailable += 1
            elif status == "fallback":
                fallback += 1
    return {
        "row_count": len(rows),
        "unavailable_boundary_count": unavailable,
        "fallback_boundary_count": fallback,
        "boundary_status_counts": status_counts,
        "work_segments": _work_segment_summary(rows),
        "work_amplification": _work_amplification_summary(rows),
        "final_generation_id": _last_non_empty(
            row.get("generation_id") for row in rows
        ),
    }


def write_delta_proof(path: Path, proof: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(proof, indent=2, sort_keys=True) + "\n")
    return path


def _graph_delta_boundary(
    generation_id: int | str | None,
    metrics: Mapping[str, Any],
) -> dict[str, Any]:
    if generation_id is None:
        return unavailable_boundary(
            "generation_not_published_for_replay_row",
            nodes_added=None,
            edges_added=None,
            cycle_ms=None,
        )
    nodes_added = metrics.get("nodes_added")
    edges_added = metrics.get("edges_added")
    cycle_ms = metrics.get("cycle_ms")
    if nodes_added is None or edges_added is None:
        return unavailable_boundary(
            "generation_delta_metrics_not_collected",
            generation_id=generation_id,
            nodes_added=None,
            edges_added=None,
            cycle_ms=cycle_ms,
        )
    return available_boundary(
        "published generation node/edge delta",
        generation_id=generation_id,
        nodes_added=nodes_added,
        edges_added=edges_added,
        cycle_ms=cycle_ms,
    )


def _fallbacks_for(boundaries: Mapping[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for key, value in boundaries.items():
        if not isinstance(value, Mapping):
            continue
        if value.get("status") != "fallback":
            continue
        out.append({
            "boundary": key,
            "reason": value.get("reason"),
            "fallback_to": value.get("fallback_to"),
        })
    return out


def _work_segment_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    segments = _all_work_segments(rows)
    status_counts: dict[str, int] = {}
    kind_counts: dict[str, int] = {}
    fallback_reasons: dict[str, int] = {}
    unavailable_reasons: dict[str, int] = {}
    measured_kinds: set[str] = set()
    unavailable_kinds: set[str] = set()
    for segment in segments:
        status = str(segment.get("status") or "missing")
        status_counts[status] = status_counts.get(status, 0) + 1
        kind = str(segment.get("segment_kind") or "missing")
        kind_counts[kind] = kind_counts.get(kind, 0) + 1
        if status in {"available", "fallback"}:
            measured_kinds.add(kind)
        if status == "unavailable":
            unavailable_kinds.add(kind)
        fallback_reason = segment.get("fallback_reason")
        if fallback_reason:
            reason = str(fallback_reason)
            fallback_reasons[reason] = fallback_reasons.get(reason, 0) + 1
        unavailable_reason = segment.get("unavailable_reason")
        if unavailable_reason:
            reason = str(unavailable_reason)
            unavailable_reasons[reason] = unavailable_reasons.get(reason, 0) + 1
    report = build_work_segment_report(segments)
    bounded_work_complete = report["ok"] and not unavailable_kinds
    return {
        "schema": WORK_SEGMENT_SCHEMA,
        "ok": report["ok"],
        "bounded_work_complete": bounded_work_complete,
        "segment_count": len(segments),
        "status_counts": status_counts,
        "kind_counts": kind_counts,
        "measured_kinds": sorted(measured_kinds),
        "unavailable_kinds": sorted(unavailable_kinds),
        "fallback_reasons": fallback_reasons,
        "unavailable_reasons": unavailable_reasons,
        "violations": report["violations"],
    }


def _work_amplification_summary(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    changed_source_records = 0
    denominator_sources: dict[str, int] = {}
    for row in rows:
        boundaries = row.get("boundaries")
        if not isinstance(boundaries, Mapping):
            continue
        source_runs = _boundary(boundaries, "source_runs")
        repo_scope = _boundary(boundaries, "repo_scope")
        source_dirty = (
            _int_or_none(source_runs.get("dirty_scope_count"))
            if source_runs.get("status") == "available"
            else None
        )
        repo_dirty = (
            _int_or_none(repo_scope.get("scope_count"))
            if repo_scope.get("status") == "available"
            else None
        )
        if source_dirty is not None:
            changed_source_records += source_dirty
            denominator_sources["source_runs.dirty_scope_count"] = (
                denominator_sources.get("source_runs.dirty_scope_count", 0) + 1
            )
        elif repo_dirty is not None:
            changed_source_records += repo_dirty
            denominator_sources["repo_scope.scope_count"] = (
                denominator_sources.get("repo_scope.scope_count", 0) + 1
            )

    measured_work_outputs = 0
    measured_segment_count = 0
    included_segment_kinds: set[str] = set()
    for segment in _all_work_segments(rows):
        status = segment.get("status")
        if status not in {"available", "fallback"}:
            continue
        output_count = _int_or_none(segment.get("output_count"))
        if output_count is None:
            continue
        measured_segment_count += 1
        measured_work_outputs += output_count
        kind = segment.get("segment_kind")
        if kind:
            included_segment_kinds.add(str(kind))

    ratio = (
        None
        if changed_source_records == 0
        else round(measured_work_outputs / changed_source_records, 3)
    )
    return {
        "changed_source_record_count": changed_source_records,
        "changed_source_record_sources": denominator_sources,
        "measured_work_output_count": measured_work_outputs,
        "measured_segment_count": measured_segment_count,
        "included_segment_kinds": sorted(included_segment_kinds),
        "ratio": ratio,
        "ratio_name": "measured_output_per_changed_source_record",
        "ratio_formula": (
            "measured available/fallback segment output_count / "
            "changed source records"
        ),
        "measurement_scope": (
            "complete"
            if len(included_segment_kinds) == len(REQUIRED_WORK_SEGMENT_KINDS)
            else "partial"
        ),
    }


def _all_work_segments(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    segments: list[dict[str, Any]] = []
    for row in rows:
        raw_segments = row.get("work_segments")
        if not isinstance(raw_segments, Sequence) or isinstance(
            raw_segments,
            (str, bytes),
        ):
            continue
        for segment in raw_segments:
            if isinstance(segment, Mapping):
                segments.append(dict(segment))
    return segments


def _source_sync_work_segment(
    row: Mapping[str, Any],
    dirty_scope_count: int,
    elapsed_ms: float | None,
    runtime_workflow_ref: Any,
) -> dict[str, Any]:
    if row.get("generation_id") in (None, ""):
        return unavailable_work_segment(
            segment_id=_segment_id(row, "source_sync"),
            segment_kind="source_sync",
            execution_surface="okg_source_sync",
            unavailable_reason="generation_not_published_for_replay_row",
            runtime_workflow_ref=(
                str(runtime_workflow_ref) if runtime_workflow_ref else None
            ),
        )
    return available_work_segment(
        segment_id=_segment_id(row, "source_sync"),
        segment_kind="source_sync",
        execution_surface="okg_source_sync",
        dirty_scope_count=dirty_scope_count,
        reused_scope_count=0,
        skipped_scope_count=0,
        output_count=1,
        elapsed_ms=elapsed_ms or 0.0,
        runtime_workflow_ref=(
            str(runtime_workflow_ref) if runtime_workflow_ref else None
        ),
    )


def _source_adapter_work_segment(
    row: Mapping[str, Any],
    boundary: Mapping[str, Any],
) -> dict[str, Any]:
    if boundary.get("status") == "available":
        return available_work_segment(
            segment_id=_segment_id(row, "source_adapter"),
            segment_kind="source_adapter",
            execution_surface="SourceRunner/source adapters",
            dirty_scope_count=_int_or_none(boundary.get("dirty_scope_count")) or 0,
            reused_scope_count=0,
            skipped_scope_count=0,
            output_count=(
                (_int_or_none(boundary.get("emitted_node_count")) or 0)
                + (_int_or_none(boundary.get("emitted_edge_count")) or 0)
                + (_int_or_none(boundary.get("retracted_node_count")) or 0)
                + (_int_or_none(boundary.get("retracted_edge_count")) or 0)
            ),
            elapsed_ms=_float_value(boundary.get("source_total_ms")) or 0.0,
        )
    return unavailable_work_segment(
        segment_id=_segment_id(row, "source_adapter"),
        segment_kind="source_adapter",
        execution_surface="SourceRunner/source adapters",
        unavailable_reason=str(
            boundary.get("reason") or "source_run_diagnostics_unavailable"
        ),
    )


def _chronos_apply_work_segment(
    row: Mapping[str, Any],
    boundary: Mapping[str, Any],
    dirty_scope_count: int,
) -> dict[str, Any]:
    if boundary.get("status") == "available":
        return available_work_segment(
            segment_id=str(
                "chronos_graph_apply:"
                f"{boundary.get('generation_id') or 'unknown'}"
            ),
            segment_kind="chronos_graph_apply",
            execution_surface="ChronosGraphStore.apply_facts",
            dirty_scope_count=dirty_scope_count,
            reused_scope_count=0,
            skipped_scope_count=0,
            output_count=(
                (_int_or_none(boundary.get("nodes_added")) or 0)
                + (_int_or_none(boundary.get("edges_added")) or 0)
            ),
            elapsed_ms=_float_value(boundary.get("cycle_ms")) or 0.0,
        )
    return unavailable_work_segment(
        segment_id=_segment_id(row, "chronos_graph_apply"),
        segment_kind="chronos_graph_apply",
        execution_surface="ChronosGraphStore.apply_facts",
        unavailable_reason=str(
            boundary.get("reason") or "chronos_graph_apply_not_collected"
        ),
    )


def _boundary_work_segment(
    row: Mapping[str, Any],
    *,
    segment_kind: str,
    execution_surface: str,
    boundary: Mapping[str, Any],
    unavailable_reason: str,
) -> dict[str, Any]:
    segment_id = _segment_id(row, segment_kind)
    status = boundary.get("status")
    if status == "available":
        return available_work_segment(
            segment_id=segment_id,
            segment_kind=segment_kind,
            execution_surface=execution_surface,
            dirty_scope_count=_int_or_none(boundary.get("dirty_scope_count")) or 0,
            reused_scope_count=_int_or_none(boundary.get("reused_scope_count")) or 0,
            skipped_scope_count=(
                _int_or_none(boundary.get("skipped_scope_count")) or 0
            ),
            output_count=(
                _int_or_none(boundary.get("rewritten_output_count"))
                or _int_or_none(boundary.get("derived_edge_count"))
                or _int_or_none(boundary.get("output_count"))
                or 0
            ),
            elapsed_ms=_float_value(boundary.get("elapsed_ms")) or 0.0,
        )
    if status == "fallback":
        return fallback_work_segment(
            segment_id=segment_id,
            segment_kind=segment_kind,
            execution_surface=execution_surface,
            dirty_scope_count=_int_or_none(boundary.get("dirty_scope_count")) or 0,
            reused_scope_count=_int_or_none(boundary.get("reused_scope_count")) or 0,
            skipped_scope_count=(
                _int_or_none(boundary.get("skipped_scope_count")) or 0
            ),
            fallback_reason=str(boundary.get("reason") or unavailable_reason),
            output_count=_int_or_none(boundary.get("output_count")) or 0,
            elapsed_ms=_float_value(boundary.get("elapsed_ms")) or 0.0,
        )
    return unavailable_work_segment(
        segment_id=segment_id,
        segment_kind=segment_kind,
        execution_surface=execution_surface,
        unavailable_reason=str(boundary.get("reason") or unavailable_reason),
    )


def _derived_artifact_work_segment(
    row: Mapping[str, Any],
    maintained_scope: Mapping[str, Any],
) -> dict[str, Any]:
    if maintained_scope.get("status") == "available":
        return available_work_segment(
            segment_id=_segment_id(row, "derived_artifact"),
            segment_kind="derived_artifact",
            execution_surface="derived_artifacts/console_count_rollup",
            dirty_scope_count=(
                _int_or_none(maintained_scope.get("rows_touched")) or 0
            ),
            reused_scope_count=0,
            skipped_scope_count=0,
            output_count=_int_or_none(maintained_scope.get("rows_written")) or 0,
            elapsed_ms=_float_value(maintained_scope.get("elapsed_ms")) or 0.0,
        )
    if maintained_scope.get("status") == "fallback":
        return fallback_work_segment(
            segment_id=_segment_id(row, "derived_artifact"),
            segment_kind="derived_artifact",
            execution_surface="derived_artifacts/console_count_rollup",
            dirty_scope_count=(
                _int_or_none(maintained_scope.get("rows_touched"))
                or _int_or_none(maintained_scope.get("dirty_scope_count"))
                or 0
            ),
            reused_scope_count=_int_or_none(maintained_scope.get("reused_scope_count")) or 0,
            skipped_scope_count=(
                _int_or_none(maintained_scope.get("skipped_scope_count")) or 0
            ),
            fallback_reason=str(
                maintained_scope.get("reason")
                or "derived_artifact_scope_fallback"
            ),
            output_count=(
                _int_or_none(maintained_scope.get("rows_written"))
                or _int_or_none(maintained_scope.get("output_count"))
                or 0
            ),
            elapsed_ms=_float_value(maintained_scope.get("elapsed_ms")) or 0.0,
        )
    return unavailable_work_segment(
        segment_id=_segment_id(row, "derived_artifact"),
        segment_kind="derived_artifact",
        execution_surface="derived_artifacts/console_count_rollup",
        unavailable_reason=str(
            maintained_scope.get("reason")
            or "derived_artifact_scope_not_collected_for_replay_row"
        ),
    )


def _publisher_work_segment(
    row: Mapping[str, Any],
    graph_delta: Mapping[str, Any],
    dirty_scope_count: int,
) -> dict[str, Any]:
    if graph_delta.get("status") == "available":
        return available_work_segment(
            segment_id=_segment_id(row, "publisher"),
            segment_kind="publisher",
            execution_surface="Publisher.cycle",
            dirty_scope_count=dirty_scope_count,
            reused_scope_count=0,
            skipped_scope_count=0,
            output_count=1,
            elapsed_ms=_float_value(graph_delta.get("cycle_ms")) or 0.0,
        )
    return unavailable_work_segment(
        segment_id=_segment_id(row, "publisher"),
        segment_kind="publisher",
        execution_surface="Publisher.cycle",
        unavailable_reason=str(
            graph_delta.get("reason") or "publisher_generation_metrics_unavailable"
        ),
    )


def _work_segment(
    *,
    segment_id: str,
    segment_kind: str,
    execution_surface: str,
    status: str,
    dirty_scope_count: int | None,
    reused_scope_count: int | None,
    skipped_scope_count: int | None,
    fallback_reason: str | None,
    output_count: int | None,
    elapsed_ms: float | int | None,
    runtime_workflow_ref: str | None,
    unavailable_reason: str | None,
) -> dict[str, Any]:
    return {
        "segment_id": segment_id,
        "segment_kind": segment_kind,
        "execution_surface": execution_surface,
        "status": status,
        "dirty_scope_count": dirty_scope_count,
        "reused_scope_count": reused_scope_count,
        "skipped_scope_count": skipped_scope_count,
        "fallback_reason": fallback_reason,
        "output_count": output_count,
        "elapsed_ms": elapsed_ms,
        "runtime_workflow_ref": runtime_workflow_ref,
        "unavailable_reason": unavailable_reason,
    }


def _contains_not_collected(value: Any) -> bool:
    if isinstance(value, str):
        normalized = value.strip().lower().replace("_", " ")
        return normalized == "not collected"
    if isinstance(value, Mapping):
        return any(_contains_not_collected(nested) for nested in value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return any(_contains_not_collected(nested) for nested in value)
    return False


def _boundary(boundaries: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = boundaries.get(key)
    return value if isinstance(value, Mapping) else {}


def _segment_id(row: Mapping[str, Any], segment_kind: str) -> str:
    commit = str(row.get("commit") or row.get("commit_sha") or "row")
    return f"{segment_kind}:{commit[:10]}"


def _dirty_graph_scope_count(
    boundary: Mapping[str, Any],
    *,
    default: int,
) -> int:
    if boundary.get("status") != "available":
        return default
    return (
        _int_value(boundary.get("dirty_node_count"))
        + _int_value(boundary.get("dirty_edge_count"))
    )


def _scope_mode_label(scope_modes: set[str]) -> str | None:
    if not scope_modes:
        return None
    if len(scope_modes) == 1:
        return next(iter(scope_modes))
    return "mixed:" + ",".join(sorted(scope_modes))


def _select_commit_publish(
    commit_row: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    raw_publishes = commit_row.get("publishes")
    if not isinstance(raw_publishes, Sequence) or isinstance(
        raw_publishes,
        (str, bytes),
    ):
        return None
    publishes = [
        publish
        for publish in raw_publishes
        if isinstance(publish, Mapping)
    ]
    if not publishes:
        return None
    commit_publishes = [
        publish
        for publish in publishes
        if publish.get("label") == "commit" and publish.get("status") == "published"
    ]
    published = [
        publish
        for publish in publishes
        if publish.get("status") == "published"
    ]
    return (commit_publishes or published or publishes)[-1]


def _publish_timings(publish: Mapping[str, Any]) -> Mapping[str, Any]:
    timings = publish.get("timings_ms")
    return timings if isinstance(timings, Mapping) else {}


def _native_view_rows(raw_native_views: Any) -> dict[str, int]:
    if not isinstance(raw_native_views, Sequence) or isinstance(
        raw_native_views,
        (str, bytes),
    ):
        return {
            "view_count": 0,
            "dirty_scope_count": 0,
            "reused_scope_count": 0,
            "output_count": 0,
        }
    view_count = 0
    dirty_scope_count = 0
    reused_scope_count = 0
    output_count = 0
    for raw_view in raw_native_views:
        if not isinstance(raw_view, Mapping):
            continue
        view_count += 1
        dirty = _int_or_none(raw_view.get("dirty_scope_count"))
        if dirty is None:
            dirty = (
                _int_value(raw_view.get("fragments_refreshed"))
                + _int_value(raw_view.get("fragments_added"))
                + _int_value(raw_view.get("fragments_dropped"))
            )
        dirty_scope_count += dirty
        reused_scope_count += _int_value(raw_view.get("fragments_reused"))
        output_count += (
            _int_value(raw_view.get("fragments_refreshed"))
            + _int_value(raw_view.get("fragments_added"))
        )
    return {
        "view_count": view_count,
        "dirty_scope_count": dirty_scope_count,
        "reused_scope_count": reused_scope_count,
        "output_count": output_count,
    }


def _int_or_none(value: Any) -> int | None:
    try:
        if value in (None, "", "NaN"):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _seconds_to_ms(value: Any) -> float | None:
    number = _float_value(value)
    if number is None:
        return None
    return round(number * 1000, 3)


def _last_non_empty(values: Sequence[Any] | Any) -> Any:
    last = None
    for value in values:
        if value not in (None, ""):
            last = value
    return last


def _int_value(value: Any) -> int:
    try:
        if value in (None, "", "NaN"):
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _float_value(value: Any) -> float | None:
    try:
        if value in (None, "", "NaN"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
