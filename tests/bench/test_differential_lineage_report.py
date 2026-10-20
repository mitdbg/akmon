from __future__ import annotations

from pathlib import Path

import okg.bench.orchestrator as orchestrator
from okg.bench.differential_replay_fixture import (
    SYNTHETIC_REPLAY_EXPERIMENT_ID,
    SYNTHETIC_REPO_NAME,
    create_synthetic_differential_repo,
)
from okg.bench.differential_replay_proof import (
    build_delta_proof,
    build_delta_proof_row,
    changed_paths_between,
)


def test_lineage_replay_report_tracks_dirty_skipped_deleted_and_rebuild(
    tmp_path: Path,
) -> None:
    fixture = create_synthetic_differential_repo(tmp_path)
    rows = []
    previous_sha = fixture.baseline_sha
    previous_generation_id = "gen-baseline"
    for i, commit in enumerate(fixture.commits, start=1):
        generation_id = f"gen-{i}"
        rows.append(build_delta_proof_row(
            i=i,
            commit_sha=commit.sha,
            previous_commit_sha=previous_sha,
            subject=commit.subject,
            changed_paths=changed_paths_between(
                fixture.repo_root,
                previous_sha,
                commit.sha,
            ),
            line_delta={
                "files_changed": len(commit.expected_changed_paths),
                "lines_added": i,
                "lines_deleted": 1,
            },
            previous_generation_id=previous_generation_id,
            generation_id=generation_id,
            generation_metrics={
                "nodes_added": i,
                "edges_added": i + 1,
                "cycle_ms": i,
            },
        ))
        previous_sha = commit.sha
        previous_generation_id = generation_id

    delta_proof = build_delta_proof(
        experiment_id=SYNTHETIC_REPLAY_EXPERIMENT_ID,
        deployment=SYNTHETIC_REPLAY_EXPERIMENT_ID,
        repo_root=fixture.repo_root,
        baseline_commit=fixture.baseline_sha,
        baseline_generation_id="gen-baseline",
        rows=rows,
        generated_at="2026-01-01T00:00:00+00:00",
    )
    exp = orchestrator.Experiment(
        id=SYNTHETIC_REPLAY_EXPERIMENT_ID,
        repo=orchestrator.Repo(
            name=SYNTHETIC_REPO_NAME,
            url="synthetic:differential",
        ),
        source_set="tier1-enriched",
        replay=orchestrator.Replay(strategy="full_history"),
    )

    report = orchestrator._build_lineage_replay_report(
        exp=exp,
        delta_proof=delta_proof,
        rebuild_equivalence={
            "ok": True,
            "checks": {"node_digest_equal": True},
        },
    )

    assert report["ok"] is True
    assert report["summary"]["lineage_envelope_count"] == 6
    assert report["summary"]["deleted_output_count"] == 2
    assert report["summary"]["skipped_output_count"] > 0
    assert report["summary"]["updated_output_count"] > 0
    assert report["summary"]["fallback_status"] == "none"
    assert report["checks"]["graph_rebuild_equivalence"] is True
    assert report["checks"]["transform_edit_updates_all_component_outputs"] is True
    assert report["reverse_index_probes"]["source_edit"]["dirty_output_ids"] == [
        "index:repo",
        "page:README.md",
    ]
    assert report["reverse_index_probes"]["transform_edit"]["skipped_output_ids"] == []
