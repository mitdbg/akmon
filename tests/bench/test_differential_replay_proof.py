from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import okg.bench.orchestrator as orchestrator
from okg.bench.differential_replay_fixture import (
    SYNTHETIC_REPLAY_EXPERIMENT_ID,
    SYNTHETIC_REPO_NAME,
    create_synthetic_differential_repo,
)
from okg.bench.differential_replay_proof import (
    DELTA_PROOF_SCHEMA,
    available_boundary,
    build_delta_proof,
    build_delta_proof_row,
    changed_paths_between,
    chronos_dirty_scope_boundary_from_projection_deltas,
    consistency_gate_boundary_from_invariant_summary,
    derived_relation_scope_boundary_from_diagnostics_commit,
    enricher_scope_boundary_from_governance,
    extractor_scope_boundary_from_diagnostics_commit,
    fallback_boundary,
    lens_scope_boundary_from_diagnostics_commit,
    native_view_scope_boundary_from_diagnostics_commit,
    read_side_operator_boundary_from_report,
    source_runs_boundary_from_diagnostics_commit,
    validate_work_segments,
    write_delta_proof,
)


def test_delta_proof_row_records_git_scope_and_generation_delta(
    tmp_path: Path,
) -> None:
    fixture = create_synthetic_differential_repo(tmp_path)
    commit = fixture.commits[0]
    row = build_delta_proof_row(
        i=1,
        commit_sha=commit.sha,
        previous_commit_sha=fixture.baseline_sha,
        subject=commit.subject,
        changed_paths=changed_paths_between(
            fixture.repo_root,
            fixture.baseline_sha,
            commit.sha,
        ),
        line_delta={
            "files_changed": 1,
            "lines_added": 2,
            "lines_deleted": 1,
        },
        previous_generation_id="gen-baseline",
        generation_id="gen-1",
        generation_metrics={
            "cycle_ms": 17,
            "nodes_added": 4,
            "edges_added": 3,
        },
        source_runs=available_boundary(
            "test-provided source-run fact counts",
            emitted_node_count=4,
            emitted_edge_count=3,
            dirty_scopes=["README.md"],
        ),
    )

    assert row["commit"] == commit.sha[:10]
    assert row["boundaries"]["repo_scope"]["changed_paths"] == ["README.md"]
    assert row["boundaries"]["source_runs"]["status"] == "available"
    assert row["boundaries"]["graph_delta"] == {
        "status": "available",
        "summary": "published generation node/edge delta",
        "generation_id": "gen-1",
        "nodes_added": 4,
        "edges_added": 3,
        "cycle_ms": 17,
    }
    assert row["fallbacks"] == []


def test_delta_proof_json_summarizes_rows_and_round_trips(
    tmp_path: Path,
) -> None:
    fixture = create_synthetic_differential_repo(tmp_path)
    rows = [
        build_delta_proof_row(
            i=1,
            commit_sha=fixture.commits[0].sha,
            previous_commit_sha=fixture.baseline_sha,
            subject=fixture.commits[0].subject,
            changed_paths=fixture.commits[0].expected_changed_paths,
            line_delta={
                "files_changed": 1,
                "lines_added": 2,
                "lines_deleted": 1,
            },
            previous_generation_id="gen-baseline",
            generation_id="gen-1",
            generation_metrics={
                "cycle_ms": 10,
                "nodes_added": 2,
                "edges_added": 1,
            },
        ),
        build_delta_proof_row(
            i=2,
            commit_sha=fixture.commits[1].sha,
            previous_commit_sha=fixture.commits[0].sha,
            subject=fixture.commits[1].subject,
            changed_paths=fixture.commits[1].expected_changed_paths,
            line_delta={
                "files_changed": 1,
                "lines_added": 9,
                "lines_deleted": 2,
            },
            previous_generation_id="gen-1",
            generation_id="gen-2",
            generation_metrics={
                "cycle_ms": 12,
                "nodes_added": 7,
                "edges_added": 4,
            },
        ),
    ]

    proof = build_delta_proof(
        experiment_id=SYNTHETIC_REPLAY_EXPERIMENT_ID,
        deployment=SYNTHETIC_REPLAY_EXPERIMENT_ID,
        repo_root=fixture.repo_root,
        baseline_commit=fixture.baseline_sha,
        baseline_generation_id="gen-baseline",
        rows=rows,
        generated_at="2026-01-01T00:00:00+00:00",
    )
    path = write_delta_proof(tmp_path / "delta_proof.json", proof)
    loaded = json.loads(path.read_text())

    assert loaded["schema"] == DELTA_PROOF_SCHEMA
    assert loaded["summary"]["row_count"] == 2
    assert loaded["summary"]["final_generation_id"] == "gen-2"
    assert (
        loaded["summary"]["boundary_status_counts"]["repo_scope"]["available"]
        == 2
    )
    assert loaded["summary"]["work_amplification"]["changed_source_record_count"] == 2
    assert loaded["summary"]["work_amplification"]["measured_segment_count"] == 6
    assert loaded["summary"]["work_amplification"]["measurement_scope"] == "partial"
    assert loaded["rows"][0]["generation_id"] == "gen-1"


def test_delta_proof_summary_reports_work_amplification_from_source_records(
    tmp_path: Path,
) -> None:
    fixture = create_synthetic_differential_repo(tmp_path)
    row = build_delta_proof_row(
        i=1,
        commit_sha=fixture.commits[0].sha,
        previous_commit_sha=fixture.baseline_sha,
        subject=fixture.commits[0].subject,
        changed_paths=("README.md", "docs/demo.md", "unused.py"),
        line_delta={
            "files_changed": 3,
            "lines_added": 2,
            "lines_deleted": 1,
        },
        previous_generation_id="gen-baseline",
        generation_id="gen-1",
        generation_metrics={
            "cycle_ms": 17,
            "nodes_added": 4,
            "edges_added": 3,
        },
        source_runs=available_boundary(
            "source-run diagnostics",
            dirty_scope_count=2,
            emitted_node_count=4,
            emitted_edge_count=1,
            retracted_node_count=0,
            retracted_edge_count=0,
            source_total_ms=5.0,
        ),
        gate_scope=fallback_boundary(
            "dirty_gate_scope_missing",
            fallback_to="full_graph_gate",
            dirty_scope_count=2,
            output_count=11,
            elapsed_ms=3.0,
        ),
    )
    proof = build_delta_proof(
        experiment_id=SYNTHETIC_REPLAY_EXPERIMENT_ID,
        deployment=SYNTHETIC_REPLAY_EXPERIMENT_ID,
        repo_root=fixture.repo_root,
        baseline_commit=fixture.baseline_sha,
        baseline_generation_id="gen-baseline",
        rows=[row],
        generated_at="2026-01-01T00:00:00+00:00",
    )

    amplification = proof["summary"]["work_amplification"]
    assert amplification["changed_source_record_count"] == 2
    assert amplification["changed_source_record_sources"] == {
        "source_runs.dirty_scope_count": 1
    }
    # source-sync(1) + source-adapter(5) + chronos(7) + gate fallback(11)
    # + publisher(1) = 25 measured work outputs over 2 changed source records.
    assert amplification["measured_work_output_count"] == 25
    assert amplification["ratio"] == 12.5
    assert amplification["ratio_name"] == "measured_output_per_changed_source_record"
    assert amplification["included_segment_kinds"] == [
        "chronos_graph_apply",
        "consistency_gate",
        "publisher",
        "source_adapter",
        "source_sync",
    ]
    assert amplification["measurement_scope"] == "partial"
    assert proof["summary"]["work_segments"]["fallback_reasons"] == {
        "dirty_gate_scope_missing": 1
    }


def test_source_runs_boundary_from_diagnostics_commit_aggregates_runs() -> None:
    boundary = source_runs_boundary_from_diagnostics_commit({
        "source_diag_status": "available",
        "source_diag_reason": "time_window",
        "source_total_ms": 42.5,
        "source_runs": [
            {
                "registry_source_name": "git-files",
                "status": "OK",
                "input_count": 2,
                "output_count": 3,
                "nodes_appended": 2,
                "edges_appended": 1,
                "nodes_retracted": 0,
                "edges_retracted": 0,
                "total_ms": 12.5,
                "source_stats": {
                    "scope_status": "scoped_emit",
                    "scope_keys": ["README.md"],
                },
            },
            {
                "registry_source_name": "code-lsp-resolution",
                "status": "OK",
                "input_count": 1,
                "output_count": 4,
                "nodes_appended": 0,
                "edges_appended": 4,
                "nodes_retracted": 0,
                "edges_retracted": 1,
                "total_ms": 30.0,
                "source_stats": {
                    "changed_file_count": 1,
                    "lsp_requests": 7,
                    "scope_status": "scoped",
                    "scope_keys": ["tinydb/table.py"],
                },
            },
        ],
    })

    assert boundary["status"] == "available"
    assert boundary["run_count"] == 2
    assert boundary["source_count"] == 2
    assert boundary["emitted_node_count"] == 2
    assert boundary["emitted_edge_count"] == 5
    assert boundary["retracted_edge_count"] == 1
    assert boundary["source_total_ms"] == 42.5
    assert boundary["dirty_scope_count"] == 2
    assert boundary["dirty_scopes"] == ["README.md", "tinydb/table.py"]
    lsp = next(
        row for row in boundary["source_breakdown"]
        if row["source"] == "code-lsp-resolution"
    )
    assert lsp["max_lsp_requests"] == 7
    assert lsp["max_changed_files"] == 1


def test_gate_scope_boundary_from_invariant_summary_measures_dirty_scope(
    tmp_path: Path,
) -> None:
    fixture = create_synthetic_differential_repo(tmp_path)
    row = build_delta_proof_row(
        i=1,
        commit_sha=fixture.commits[0].sha,
        previous_commit_sha=fixture.baseline_sha,
        subject=fixture.commits[0].subject,
        changed_paths=("README.md",),
        line_delta={
            "files_changed": 1,
            "lines_added": 2,
            "lines_deleted": 1,
        },
        previous_generation_id="gen-baseline",
        generation_id="gen-1",
        generation_metrics={
            "cycle_ms": 17,
            "nodes_added": 4,
            "edges_added": 3,
        },
        gate_scope=consistency_gate_boundary_from_invariant_summary({
            "consistency_gates": {
                "passed": 1,
                "blocked": 0,
                "results": [
                    {
                        "details": {
                            "scope_mode": "dirty_edges+incident_dirty_nodes",
                        },
                        "preflight": {
                            "dirty_node_count": 4,
                            "dirty_edge_count": 3,
                            "scoped_edge_count": 3,
                        },
                        "elapsed_ms": 13,
                        "coverage_complete": True,
                    },
                ],
            },
        }),
    )

    gate = _segment(row["work_segments"], "consistency_gate")
    assert gate["status"] == "available"
    assert gate["dirty_scope_count"] == 7
    assert gate["output_count"] == 1
    assert gate["elapsed_ms"] == 13


def _scoped_exact_gate_result() -> dict[str, Any]:
    """A gate result shaped like the one a real SCOPED incremental publish writes.

    Not a hardcoded ``coverage_complete: True`` fixture: an incremental run has
    ``coverage_complete=False`` (the schema's
    ``CHECK (NOT (coverage_complete AND scoped))`` forbids anything else) and
    certifies its coverage with ``details.scoped_exact``.
    """

    return {
        "gate_name": "transform_consistency_gate",
        "result": "evaluated",
        "blocks_publish": False,
        "reason": "no_violations",
        "coverage_complete": False,
        "coverage_basis": "chronos_dirty_edges+incident_dirty_nodes",
        "details": {
            "scope_mode": "dirty_edges+incident_dirty_nodes",
            "scoped": True,
            "scoped_exact": True,
            "scope_coverage_basis": "exact_postgres",
            "verdict_universe_valid_by_induction": True,
        },
        "preflight": {
            "dirty_node_count": 4,
            "dirty_edge_count": 3,
            "scoped_edge_count": 3,
        },
        "elapsed_ms": 13,
    }


def test_scoped_exact_gate_result_is_available_not_incomplete_coverage() -> None:
    """A scoped-EXACT gate run must NOT be reported as unproven coverage.

    This boundary exists to certify that the consistency gate ran incrementally.
    Reading ``coverage_complete is not True`` as "incomplete coverage" flipped it
    to ``fallback`` on exactly the publishes where the gate IS incremental, and
    the work-segment builder then downgraded the consistency_gate segment and
    dropped its dirty-scope counts -- the surface reported the thing it exists to
    prove as unproven.
    """

    boundary = consistency_gate_boundary_from_invariant_summary({
        "consistency_gates": {
            "passed": 1,
            "blocked": 0,
            "results": [_scoped_exact_gate_result()],
        },
    })

    assert boundary["status"] == "available", boundary
    assert "reason" not in boundary, boundary
    # The measured dirty scope SURVIVES (the fallback path zeroed it).
    assert boundary["dirty_scope_count"] == 7, boundary
    assert boundary["dirty_node_count"] == 4, boundary
    assert boundary["dirty_edge_count"] == 3, boundary
    assert boundary["scoped_edge_count"] == 3, boundary
    # ...and the boundary does not restate the over-claim the gate stopped
    # making: exact coverage of a delta is not universe-completeness.
    assert boundary["coverage_complete"] is False, boundary
    assert boundary["scoped_exact_count"] == 1, boundary


def test_scoped_exact_gate_result_keeps_its_work_segment_available() -> None:
    """The downgrade reached the work segment, so the fix must too."""

    row = build_delta_proof_row(
        i=1,
        commit_sha="abc123def456",
        previous_commit_sha="000000000000",
        subject="scoped incremental publish",
        changed_paths=("README.md",),
        line_delta={"files_changed": 1, "lines_added": 2, "lines_deleted": 1},
        previous_generation_id="gen-baseline",
        generation_id="gen-1",
        generation_metrics={"cycle_ms": 17, "nodes_added": 4, "edges_added": 3},
        gate_scope=consistency_gate_boundary_from_invariant_summary({
            "consistency_gates": {
                "passed": 1,
                "blocked": 0,
                "results": [_scoped_exact_gate_result()],
            },
        }),
    )

    gate = _segment(row["work_segments"], "consistency_gate")
    assert gate["status"] == "available", gate
    assert gate["dirty_scope_count"] == 7, gate
    assert gate["output_count"] == 1, gate
    assert gate["elapsed_ms"] == 13, gate


def test_uncertified_incomplete_coverage_still_falls_back() -> None:
    """The new vocabulary must not become a blanket pass.

    A scoped run that does NOT certify exactness (no ``scoped_exact``, or a
    non-exact basis) is genuinely unproven coverage and must still degrade --
    absence of the field is never read as agreement.
    """

    for details in (
        {"scope_mode": "dirty_edges+incident_dirty_nodes"},
        {"scope_mode": "x", "scoped": True, "scoped_exact": False},
        {
            "scope_mode": "x",
            "scoped": True,
            "scoped_exact": True,
            "scope_coverage_basis": "sampled_monitoring",
        },
    ):
        result = _scoped_exact_gate_result()
        result["details"] = details
        boundary = consistency_gate_boundary_from_invariant_summary({
            "consistency_gates": {
                "passed": 1,
                "blocked": 0,
                "results": [result],
            },
        })
        assert boundary["status"] == "fallback", (details, boundary)
        assert boundary["reason"] == "consistency_gate_coverage_incomplete", (
            details,
            boundary,
        )


def test_empty_enricher_governance_is_measured_zero_work() -> None:
    boundary = enricher_scope_boundary_from_governance({
        "schema": "okg.enricher_judgment_governance.v1",
        "records": [],
        "record_count": 0,
        "status_counts": {},
    })

    assert boundary["status"] == "available"
    assert boundary["dirty_scope_count"] == 0
    assert boundary["skipped_scope_count"] == 1
    assert boundary["output_count"] == 0


def test_enricher_fallback_uses_measured_dirty_scope() -> None:
    boundary = enricher_scope_boundary_from_governance(
        {
            "schema": "okg.enricher_judgment_governance.v1",
            "records": [
                {
                    "enricher_name": "code_reference",
                    "edge_counts": {"emitted": 8, "skipped": 0},
                }
            ],
        },
        dirty_scope_count=11,
        elapsed_ms=3.5,
    )
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
        enricher_scope=boundary,
    )

    enricher = _segment(row["work_segments"], "enricher")
    assert enricher["status"] == "fallback"
    assert enricher["dirty_scope_count"] == 11
    assert enricher["output_count"] == 8
    assert validate_work_segments(row["work_segments"]) == []


def test_projection_deltas_feed_chronos_dirty_scope() -> None:
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
        chronos_dirty_scope=chronos_dirty_scope_boundary_from_projection_deltas({
            "nodes_added": 1,
            "nodes_updated": 2,
            "nodes_retracted": 3,
            "edges_added": 4,
            "edges_updated": 5,
            "edges_retracted": 6,
        }),
    )

    chronos = _segment(row["work_segments"], "chronos_graph_apply")
    publisher = _segment(row["work_segments"], "publisher")
    assert chronos["dirty_scope_count"] == 21
    assert publisher["dirty_scope_count"] == 21


def test_native_view_boundary_from_publish_diagnostics_refreshes_segment(
    tmp_path: Path,
) -> None:
    fixture = create_synthetic_differential_repo(tmp_path)
    row = build_delta_proof_row(
        i=1,
        commit_sha=fixture.commits[0].sha,
        previous_commit_sha=fixture.baseline_sha,
        subject=fixture.commits[0].subject,
        changed_paths=("README.md",),
        line_delta={
            "files_changed": 1,
            "lines_added": 2,
            "lines_deleted": 1,
        },
        previous_generation_id="gen-baseline",
        generation_id="gen-1",
        generation_metrics={
            "cycle_ms": 17,
            "nodes_added": 4,
            "edges_added": 3,
        },
        native_view_scope=native_view_scope_boundary_from_diagnostics_commit({
            "publishes": [
                {
                    "label": "commit",
                    "status": "published",
                    "attribute_effective_view_refresh": {
                        "status": "materialized",
                        "node_rows": 4,
                        "edge_rows": 3,
                    },
                    "native_view_refresh": {
                        "status": "materialized",
                    },
                    "native_views": [
                        {
                            "dirty_scope_count": 2,
                            "fragments_reused": 1,
                            "fragments_refreshed": 1,
                            "fragments_added": 1,
                        },
                    ],
                    "timings_ms": {
                        "attribute_effective_view_materialize": 5.0,
                        "attribute_effective_attrs_materialize": 2.5,
                        "native_views": 1.0,
                    },
                },
            ],
        }),
    )

    view = _segment(row["work_segments"], "mcp_read_side_view")
    assert view["status"] == "fallback"
    assert view["fallback_reason"] == "attribute_effective_full_materialization"
    assert view["dirty_scope_count"] == 9
    assert view["reused_scope_count"] == 1
    assert view["output_count"] == 9
    assert view["elapsed_ms"] == 8.5


def test_publish_diagnostics_refresh_remaining_runtime_segments(
    tmp_path: Path,
) -> None:
    fixture = create_synthetic_differential_repo(tmp_path)
    row = build_delta_proof_row(
        i=1,
        commit_sha=fixture.commits[0].sha,
        previous_commit_sha=fixture.baseline_sha,
        subject=fixture.commits[0].subject,
        changed_paths=("README.md",),
        line_delta={
            "files_changed": 1,
            "lines_added": 2,
            "lines_deleted": 1,
        },
        previous_generation_id="gen-baseline",
        generation_id="gen-1",
        generation_metrics={
            "cycle_ms": 17,
            "nodes_added": 4,
            "edges_added": 3,
        },
        extractor_scope=extractor_scope_boundary_from_diagnostics_commit({
            "publishes": [{
                "label": "commit",
                "status": "published",
                "extractor_edges_emitted": 0,
                "timings_ms": {"extractor_dirty_input_check": 0.25},
            }],
        }),
        lens_scope=lens_scope_boundary_from_diagnostics_commit({
            "publishes": [{
                "label": "commit",
                "status": "published",
                "lenses": [],
                "timings_ms": {"lens_promotion": 0.5, "lenses": 0.1},
            }],
        }),
        derived_relation_scope=(
            derived_relation_scope_boundary_from_diagnostics_commit({
                "publishes": [{
                    "label": "commit",
                    "status": "published",
                    "derived_fact_summary": {
                        "total_loaded_count": 5,
                        "total_applied_count": 3,
                    },
                    "timings_ms": {
                        "derived_fact_load": 1.0,
                        "derived_fact_apply": 2.0,
                        "view_derived_fact_load": 3.0,
                        "view_derived_fact_apply": 4.0,
                    },
                }],
            })
        ),
        maintained_derivation_scope=available_boundary(
            "console count rollup maintained",
            rows_touched=2,
            rows_written=1,
            elapsed_ms=6.0,
        ),
        read_side_operator_scope=read_side_operator_boundary_from_report({
            "generation_id": "gen-1",
            "reads": {
                "search": {"status": "ok", "rows": 7, "p50_ms": 2.5},
                "rollup": {"status": "skipped", "reason": "no_anchor"},
            },
        }, target_generation_id="gen-1"),
    )

    extractor = _segment(row["work_segments"], "extractor")
    assert extractor["status"] == "available"
    assert extractor["skipped_scope_count"] == 1
    assert extractor["output_count"] == 0
    lens = _segment(row["work_segments"], "lens")
    assert lens["status"] == "available"
    assert lens["skipped_scope_count"] == 1
    derived_relation = _segment(row["work_segments"], "derived_relation")
    assert derived_relation["status"] == "available"
    assert derived_relation["dirty_scope_count"] == 5
    assert derived_relation["output_count"] == 3
    assert derived_relation["elapsed_ms"] == 10.0
    derived_artifact = _segment(row["work_segments"], "derived_artifact")
    assert derived_artifact["status"] == "available"
    assert derived_artifact["dirty_scope_count"] == 2
    assert derived_artifact["output_count"] == 1
    assert derived_artifact["elapsed_ms"] == 6.0
    read_side = _segment(row["work_segments"], "read_side_operator")
    assert read_side["status"] == "available"
    assert read_side["dirty_scope_count"] == 1
    assert read_side["skipped_scope_count"] == 1
    assert read_side["output_count"] == 7


def test_derived_artifact_does_not_borrow_derived_relation_scope() -> None:
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
        derived_relation_scope=available_boundary(
            "publisher reported derived relation apply counts",
            dirty_scope_count=5,
            output_count=3,
            elapsed_ms=10.0,
        ),
    )

    derived_relation = _segment(row["work_segments"], "derived_relation")
    assert derived_relation["status"] == "available"
    assert derived_relation["output_count"] == 3

    derived_artifact = _segment(row["work_segments"], "derived_artifact")
    assert derived_artifact["status"] == "unavailable"
    assert derived_artifact["unavailable_reason"] == (
        "maintained_derivation_scope_not_collected"
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
    assert work_segments["bounded_work_complete"] is False
    assert "derived_artifact" in work_segments["unavailable_kinds"]


def test_read_side_operator_boundary_skips_non_matching_generation() -> None:
    boundary = read_side_operator_boundary_from_report(
        {
            "generation_id": "gen-final",
            "reads": {
                "search": {"status": "ok", "rows": 7, "p50_ms": 2.5},
            },
        },
        target_generation_id="gen-earlier",
    )

    assert boundary["status"] == "unavailable"
    assert boundary["dirty_scope_count"] == 0
    assert boundary["skipped_scope_count"] == 1
    assert boundary["output_count"] == 0
    assert boundary["generation_id"] == "gen-earlier"
    assert boundary["benchmark_generation_id"] == "gen-final"


def test_unavailable_boundaries_are_loud_and_not_zero_like(
    tmp_path: Path,
) -> None:
    fixture = create_synthetic_differential_repo(tmp_path)
    commit = fixture.commits[0]
    row = build_delta_proof_row(
        i=1,
        commit_sha=commit.sha,
        previous_commit_sha=fixture.baseline_sha,
        subject=commit.subject,
        changed_paths=commit.expected_changed_paths,
        line_delta={
            "files_changed": 1,
            "lines_added": 2,
            "lines_deleted": 1,
        },
        previous_generation_id="gen-baseline",
        generation_id="gen-1",
        generation_metrics={"cycle_ms": 17},
    )

    graph_delta = row["boundaries"]["graph_delta"]
    assert graph_delta["status"] == "unavailable"
    assert graph_delta["reason"] == "generation_delta_metrics_not_collected"
    assert graph_delta["nodes_added"] is None
    assert graph_delta["edges_added"] is None

    for boundary in row["boundaries"].values():
        if boundary["status"] == "unavailable":
            _assert_no_zero_counts(boundary)


def test_fallback_boundaries_are_reported_in_row_and_summary(
    tmp_path: Path,
) -> None:
    fixture = create_synthetic_differential_repo(tmp_path)
    commit = fixture.commits[1]
    row = build_delta_proof_row(
        i=2,
        commit_sha=commit.sha,
        previous_commit_sha=fixture.commits[0].sha,
        subject=commit.subject,
        changed_paths=commit.expected_changed_paths,
        line_delta={
            "files_changed": 1,
            "lines_added": 9,
            "lines_deleted": 2,
        },
        previous_generation_id="gen-1",
        generation_id="gen-2",
        generation_metrics={
            "cycle_ms": 11,
            "nodes_added": 5,
            "edges_added": 4,
        },
        gate_scope=fallback_boundary(
            "dirty_gate_scope_missing",
            fallback_to="full_graph_gate",
            scope_mode="full",
        ),
    )
    proof = build_delta_proof(
        experiment_id=SYNTHETIC_REPLAY_EXPERIMENT_ID,
        deployment=SYNTHETIC_REPLAY_EXPERIMENT_ID,
        repo_root=fixture.repo_root,
        baseline_commit=fixture.baseline_sha,
        baseline_generation_id="gen-baseline",
        rows=[row],
        generated_at="2026-01-01T00:00:00+00:00",
    )

    assert row["fallbacks"] == [
        {
            "boundary": "gate_scope",
            "reason": "dirty_gate_scope_missing",
            "fallback_to": "full_graph_gate",
        }
    ]
    assert proof["summary"]["fallback_boundary_count"] == 1
    assert (
        proof["summary"]["boundary_status_counts"]["gate_scope"]["fallback"]
        == 1
    )


def test_orchestrator_generates_synthetic_repo_and_commit_list(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(orchestrator, "BENCH_REPOS_DIR", tmp_path / "repos")
    exp = orchestrator.Experiment(
        id=SYNTHETIC_REPLAY_EXPERIMENT_ID,
        repo=orchestrator.Repo(
            name=SYNTHETIC_REPO_NAME,
            url="synthetic:differential",
        ),
        source_set="tier1-enriched",
        replay=orchestrator.Replay(strategy="full_history"),
    )

    orchestrator._ensure_repo(exp)
    commits = orchestrator._commits_for(exp)

    assert exp.repo_root.is_dir()
    assert len(commits) == 4
    assert orchestrator._baseline_commit(exp) not in commits
    assert exp.delta_proof_json.name == (
        f"{SYNTHETIC_REPLAY_EXPERIMENT_ID}.delta_proof.json"
    )


def _segment(
    segments: list[dict[str, object]],
    kind: str,
) -> dict[str, object]:
    return next(segment for segment in segments if segment["segment_kind"] == kind)


def _assert_no_zero_counts(value: Mapping[str, Any]) -> None:
    for key, nested in value.items():
        if key.endswith("_count") or key in {"nodes_added", "edges_added"}:
            assert nested != 0
        if isinstance(nested, Mapping):
            _assert_no_zero_counts(nested)
