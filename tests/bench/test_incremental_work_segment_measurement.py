from __future__ import annotations

from pathlib import Path

import pytest

from okg.bench.differential_replay_proof import (
    REQUIRED_WORK_SEGMENT_KINDS,
    WORK_SEGMENT_REQUIRED_FIELDS,
    WORK_SEGMENT_SCHEMA,
    assert_work_segment_report_valid,
    available_boundary,
    available_work_segment,
    build_delta_proof,
    build_delta_proof_row,
    build_work_segment_report,
    fallback_work_segment,
    unavailable_work_segment,
    validate_work_segments,
    work_segments_from_delta_row,
)


def test_required_work_segment_report_passes_with_one_row_per_kind() -> None:
    segments = [
        available_work_segment(
            segment_id=f"{kind}:demo",
            segment_kind=kind,
            execution_surface=f"surface.{kind}",
            dirty_scope_count=1,
            reused_scope_count=2,
            skipped_scope_count=3,
            output_count=4,
            elapsed_ms=5.5,
            runtime_workflow_ref=(
                "dbos:workflow:demo" if kind == "source_sync" else None
            ),
        )
        for kind in REQUIRED_WORK_SEGMENT_KINDS
    ]

    report = build_work_segment_report(segments)

    assert report["schema"] == WORK_SEGMENT_SCHEMA
    assert report["ok"] is True
    assert report["violations"] == []
    assert report["required_segment_kinds"] == list(REQUIRED_WORK_SEGMENT_KINDS)
    assert set(report["segments"][0]) == set(WORK_SEGMENT_REQUIRED_FIELDS)


def test_missing_required_work_segment_kind_fails_report() -> None:
    segments = [
        available_work_segment(
            segment_id=f"{kind}:demo",
            segment_kind=kind,
            execution_surface=f"surface.{kind}",
            dirty_scope_count=1,
            reused_scope_count=0,
            skipped_scope_count=0,
            output_count=1,
            elapsed_ms=1.0,
        )
        for kind in REQUIRED_WORK_SEGMENT_KINDS
        if kind != "lens"
    ]

    violations = validate_work_segments(segments)

    assert "missing required segment kind lens" in violations


def test_not_collected_is_rejected_for_required_work_segment() -> None:
    segments = [
        available_work_segment(
            segment_id=f"{kind}:demo",
            segment_kind=kind,
            execution_surface=f"surface.{kind}",
            dirty_scope_count=1,
            reused_scope_count=0,
            skipped_scope_count=0,
            output_count=1,
            elapsed_ms=1.0,
        )
        for kind in REQUIRED_WORK_SEGMENT_KINDS
    ]
    segments[0]["dirty_scope_count"] = "not collected"

    with pytest.raises(ValueError, match="not collected"):
        assert_work_segment_report_valid(segments)


def test_unavailable_segment_requires_named_reason() -> None:
    segment = unavailable_work_segment(
        segment_id="lens:demo",
        segment_kind="lens",
        execution_surface="src/okg/substrate/lenses",
        unavailable_reason="deployment_has_no_lens_runtime",
    )
    assert validate_work_segments(
        [segment],
        required_kinds=("lens",),
    ) == []

    segment["unavailable_reason"] = ""
    assert validate_work_segments(
        [segment],
        required_kinds=("lens",),
    ) == ["lens:demo: unavailable segment needs unavailable_reason"]


def test_silent_fullscan_fails_proof() -> None:
    segment = fallback_work_segment(
        segment_id="consistency_gate:demo",
        segment_kind="consistency_gate",
        execution_surface="TransformConsistencyGate",
        dirty_scope_count=10,
        reused_scope_count=0,
        skipped_scope_count=0,
        fallback_reason="",
        output_count=10,
        elapsed_ms=8.0,
    )

    assert validate_work_segments(
        [segment],
        required_kinds=("consistency_gate",),
    ) == ["consistency_gate:demo: fallback segment needs fallback_reason"]


def test_delta_proof_row_includes_required_work_segments() -> None:
    row = build_delta_proof_row(
        i=1,
        commit_sha="abc123def456",
        previous_commit_sha="000000000000",
        subject="demo",
        changed_paths=("README.md",),
        line_delta={
            "files_changed": 1,
            "lines_added": 2,
            "lines_deleted": 0,
        },
        previous_generation_id="gen-0",
        generation_id="gen-1",
        generation_metrics={
            "cycle_ms": 25,
            "nodes_added": 3,
            "edges_added": 4,
        },
        end_to_end_s=0.125,
        source_sync_workflow_ref="dbos:workflow:source-sync-1",
        source_runs=available_boundary(
            "source diagnostics",
            dirty_scope_count=1,
            emitted_node_count=2,
            emitted_edge_count=1,
            retracted_node_count=0,
            retracted_edge_count=0,
            source_total_ms=8.5,
        ),
        maintained_derivation_scope=available_boundary(
            "maintained rollup",
            rows_touched=1,
            rows_written=1,
        ),
    )

    segments = row["work_segments"]

    assert {segment["segment_kind"] for segment in segments} == set(
        REQUIRED_WORK_SEGMENT_KINDS
    )
    assert validate_work_segments(segments) == []
    source_sync = _segment(segments, "source_sync")
    assert source_sync["runtime_workflow_ref"] == "dbos:workflow:source-sync-1"
    source_adapter = _segment(segments, "source_adapter")
    assert source_adapter["status"] == "available"
    assert source_adapter["output_count"] == 3
    extractor = _segment(segments, "extractor")
    assert extractor["status"] == "unavailable"
    assert extractor["unavailable_reason"] == (
        "extractor_scope_not_collected_for_replay_row"
    )


def test_delta_proof_summary_reports_work_segment_validation() -> None:
    row = build_delta_proof_row(
        i=1,
        commit_sha="abc123def456",
        previous_commit_sha="000000000000",
        subject="demo",
        changed_paths=("README.md",),
        line_delta={
            "files_changed": 1,
            "lines_added": 2,
            "lines_deleted": 0,
        },
        previous_generation_id="gen-0",
        generation_id="gen-1",
        generation_metrics={
            "cycle_ms": 25,
            "nodes_added": 3,
            "edges_added": 4,
        },
    )

    proof = build_delta_proof(
        experiment_id="demo",
        deployment="demo",
        repo_root=Path("."),
        baseline_commit="000000000000",
        baseline_generation_id="gen-0",
        rows=[row],
        generated_at="2026-01-01T00:00:00+00:00",
    )

    work_segments = proof["summary"]["work_segments"]
    assert work_segments["schema"] == WORK_SEGMENT_SCHEMA
    assert work_segments["ok"] is True
    assert work_segments["bounded_work_complete"] is False
    assert work_segments["segment_count"] == len(REQUIRED_WORK_SEGMENT_KINDS)
    assert work_segments["kind_counts"]["publisher"] == 1
    assert work_segments["status_counts"]["unavailable"] >= 1
    assert "extractor" in work_segments["unavailable_kinds"]


def test_work_segments_refresh_after_source_run_boundary_join() -> None:
    row = build_delta_proof_row(
        i=1,
        commit_sha="abc123def456",
        previous_commit_sha="000000000000",
        subject="demo",
        changed_paths=("README.md",),
        line_delta={
            "files_changed": 1,
            "lines_added": 2,
            "lines_deleted": 0,
        },
        previous_generation_id="gen-0",
        generation_id="gen-1",
        generation_metrics={
            "cycle_ms": 25,
            "nodes_added": 3,
            "edges_added": 4,
        },
    )
    assert _segment(row["work_segments"], "source_adapter")["status"] == (
        "unavailable"
    )

    row["boundaries"]["source_runs"] = available_boundary(
        "source diagnostics joined",
        dirty_scope_count=2,
        emitted_node_count=4,
        emitted_edge_count=5,
        retracted_node_count=1,
        retracted_edge_count=0,
        source_total_ms=13.0,
    )
    row["work_segments"] = work_segments_from_delta_row(row)

    source_adapter = _segment(row["work_segments"], "source_adapter")
    assert source_adapter["status"] == "available"
    assert source_adapter["dirty_scope_count"] == 2
    assert source_adapter["output_count"] == 10


def _segment(
    segments: list[dict[str, object]],
    kind: str,
) -> dict[str, object]:
    return next(segment for segment in segments if segment["segment_kind"] == kind)
