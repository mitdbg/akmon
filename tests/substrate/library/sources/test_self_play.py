from __future__ import annotations

import json
from pathlib import Path

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.self_play import SelfPlayReportSource


def test_self_play_report_source_emits_run_scenarios_findings(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    report_dir = repo / "deployments" / "okg-workspace" / "self_play" / "reports"
    report_dir.mkdir(parents=True)
    report_path = report_dir / "baseline-gen9-scripted.json"
    report_path.write_text(json.dumps({
        "report_format": "okg-workspace-self-play/v1",
        "deployment": "okg-workspace",
        "generation_id": 9,
        "catalog_version_id": 4,
        "created_at": "2026-05-02T21:00:00Z",
        "graph_summary": {"node_count": 100, "edge_count": 200},
        "scenario_results": [
            {
                "scenario_id": "code-lookup",
                "question_class": "code_lookup",
                "substrate_stage_target": "read_side_surfacing",
                "question": "Where is run_sql implemented?",
                "answerability": {
                    "score": 0.7,
                    "status": "scripted_probe_only",
                    "manual_review_required": True,
                },
                "expected_evidence_subtypes": [
                    "code_symbol", "test_case",
                ],
                "observed_subtype_counts": {
                    "code_symbol": 3,
                    "test_case": 0,
                },
                "evidence_edge_paths": [
                    {
                        "name": "test-to-symbol",
                        "path": ["references:out"],
                        "from_subtype": "test_case",
                        "to_subtype": "code_symbol",
                        "observed_edge_count": 0,
                    },
                    {
                        "name": "not-checked-multi-hop",
                        "path": ["contains:out", "contains:out"],
                        "from_subtype": "open_spec_change",
                        "to_subtype": "proposal_task",
                        "observed": "not_checked_multi_hop",
                    }
                ],
                "evidence_node_ids": [
                    "symbol:demo:src/app.py:main:1",
                    "file:demo:src/app.py",
                    "task:change:1.1",
                    "unknown:ignored",
                ],
                "tool_calls": [{"tool": "run_sql", "ok": False, "error": "boom"}],
                "shell_fallbacks": [{"reason": "needed rg"}],
                "failed_assumptions": ["expected direct test link"],
                "manual_observations": [
                    {
                        "finding_id": "stable-manual-gap",
                        "kind": "manual_gap",
                        "stage": "read_side_surfacing",
                        "severity": "medium",
                        "status": "resolved",
                        "introduced_generation_id": 6,
                        "resolved_generation_id": 9,
                        "summary": "manual observation",
                        "node_ids": ["file:demo:src/app.py"],
                    }
                ],
            },
            {
                "scenario_id": "healthy",
                "question_class": "triage",
                "substrate_stage_target": "source_coverage",
                "question": "What passed?",
                "answerability": {"score": 1.0},
                "expected_evidence_subtypes": ["code_symbol"],
                "observed_subtype_counts": {"code_symbol": 3},
                "evidence_edge_paths": [
                    {
                        "name": "old-unchecked-hop",
                        "path": ["contains:out", "contains:out"],
                        "from_subtype": "open_spec_change",
                        "to_subtype": "proposal_task",
                        "observed": "not_checked_multi_hop",
                    }
                ],
                "evidence_node_ids": [],
                "tool_calls": [],
            },
        ],
    }), encoding="utf-8")

    source = SelfPlayReportSource(
        repo="demo",
        repo_root=repo,
        report_globs=["deployments/okg-workspace/self_play/reports/*.json"],
    )
    facts = list(source.run("source-run-1", mode="scope_complete").facts)

    assert source.validation_warnings == []
    nodes = [f for f in facts if isinstance(f, NodeFact)]
    edges = [f for f in facts if isinstance(f, EdgeFact)]
    subtypes = {f.subtype for f in nodes}
    assert {
        "self_play_run",
        "self_play_scenario",
        "self_play_finding",
    } <= subtypes
    assert sum(1 for f in nodes if f.subtype == "self_play_run") == 1
    assert sum(1 for f in nodes if f.subtype == "self_play_scenario") == 2
    run = next(f for f in nodes if f.subtype == "self_play_run")
    assert run.attrs["answerability_score_avg"] == 0.85
    assert run.attrs["answerability_score_min"] == 0.7
    assert run.attrs["answerability_score_max"] == 1.0
    assert run.attrs["shell_fallback_count"] == 1
    assert run.attrs["failed_assumption_count"] == 1
    assert run.attrs["tool_failure_count"] == 1
    assert run.attrs["manual_review_required_count"] == 1
    findings = [f for f in nodes if f.subtype == "self_play_finding"]
    # Missing edge path + missing subtype + low score + shell fallback
    # + failed assumption + tool failure + manual observation.
    assert len(findings) == 7
    assert {
        "stage",
        "severity",
        "status",
        "evidence_summary",
        "artifact_path",
        "introduced_generation_id",
    } <= findings[0].attrs.keys()
    assert findings[0].attrs["introduced_generation_id"] == 9
    manual = next(f for f in findings if f.attrs["kind"] == "manual_gap")
    run_key = run.node_id.rsplit(":", 1)[-1]
    assert (
        manual.node_id
        == f"self-play-finding:{run_key}:code-lookup:"
        "stable-manual-gap-65ab9f61e08f"
    )
    assert manual.attrs["status"] == "resolved"
    assert manual.attrs["introduced_generation_id"] == 6
    assert manual.attrs["resolved_generation_id"] == 9
    assert any(
        e.edge_type == "contains"
        and e.src.startswith("self-play-run:")
        and e.dst.startswith("self-play-scenario:")
        for e in edges
    )
    assert any(
        e.edge_type == "contains"
        and e.src.startswith("self-play-scenario:")
        and e.dst.startswith("self-play-finding:")
        for e in edges
    )
    assert any(
        e.edge_type == "references"
        and e.src.startswith("self-play-finding:")
        and e.dst == "file:demo:src/app.py"
        for e in edges
    )
    assert all(e.dst != "unknown:ignored" for e in edges)


def test_self_play_report_source_supersedes_old_open_findings(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    report_dir = repo / "deployments" / "okg-workspace" / "self_play" / "reports"
    report_dir.mkdir(parents=True)
    base = {
        "report_format": "okg-workspace-self-play/v1",
        "deployment": "okg-workspace",
        "catalog_version_id": 4,
        "graph_summary": {"node_count": 100, "edge_count": 200},
    }
    old_report = {
        **base,
        "generation_id": 9,
        "scenario_results": [
            {
                "scenario_id": "code-lookup",
                "question_class": "code_lookup",
                "substrate_stage_target": "read_side_surfacing",
                "question": "Where is run_sql implemented?",
                "answerability": {"score": 0.5},
                "expected_evidence_subtypes": ["code_symbol"],
                "observed_subtype_counts": {"code_symbol": 3},
                "evidence_edge_paths": [
                    {
                        "name": "old-unchecked-hop",
                        "path": ["contains:out", "contains:out"],
                        "from_subtype": "open_spec_change",
                        "to_subtype": "proposal_task",
                        "observed": "not_checked_multi_hop",
                    }
                ],
                "evidence_node_ids": ["symbol:demo:src/app.py:main:1"],
                "tool_calls": [],
            },
        ],
    }
    new_report = {
        **base,
        "generation_id": 10,
        "scenario_results": [
            {
                "scenario_id": "code-lookup",
                "question_class": "code_lookup",
                "substrate_stage_target": "read_side_surfacing",
                "question": "Where is run_sql implemented?",
                "answerability": {"score": 1.0},
                "expected_evidence_subtypes": ["code_symbol"],
                "observed_subtype_counts": {"code_symbol": 3},
                "evidence_edge_paths": [],
                "evidence_node_ids": ["symbol:demo:src/app.py:main:1"],
                "tool_calls": [],
            },
        ],
    }
    (report_dir / "baseline-gen9-scripted.json").write_text(
        json.dumps(old_report),
        encoding="utf-8",
    )
    (report_dir / "baseline-gen10-scripted.json").write_text(
        json.dumps(new_report),
        encoding="utf-8",
    )

    source = SelfPlayReportSource(
        repo="demo",
        repo_root=repo,
        report_globs=["deployments/okg-workspace/self_play/reports/*.json"],
    )
    facts = list(source.run("source-run-1", mode="scope_complete").facts)

    findings = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "self_play_finding"
    ]
    assert len(findings) == 2
    assert {f.attrs["status"] for f in findings} == {"superseded"}
    assert {f.attrs["introduced_generation_id"] for f in findings} == {9}
    assert {f.attrs["superseded_generation_id"] for f in findings} == {10}


def test_self_play_report_source_scopes_to_configured_report(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    report_a = _write_self_play_report(
        repo / "deployments" / "okg-workspace" / "self_play" / "reports" / "a.json",
        scenario_id="scenario-a",
        generation_id=9,
        score=0.5,
    )
    report_b = _write_self_play_report(
        repo / "deployments" / "okg-workspace" / "self_play" / "reports" / "b.json",
        scenario_id="scenario-b",
        generation_id=10,
        score=0.6,
    )
    unconfigured = _write_self_play_report(
        repo / "other" / "self-play.json",
        scenario_id="scenario-c",
        generation_id=10,
        score=0.2,
    )
    source = SelfPlayReportSource(
        repo="demo",
        repo_root=repo,
        report_globs=["deployments/okg-workspace/self_play/reports/*.json"],
    )

    run = source.run(
        "source-run-scoped",
        mode="scope_incomplete",
        sync_scope={
            "scope_keys": [
                report_b.relative_to(repo).as_posix(),
                "../escape.json",
            ],
        },
    )
    facts = list(run.facts)
    nodes = [fact for fact in facts if isinstance(fact, NodeFact)]

    assert run.completed_scope is False
    assert source.validation_warnings == []
    assert {
        node.source_record_id.get("path")
        for node in nodes
        if node.subtype in {
            "self_play_run",
            "self_play_scenario",
            "self_play_finding",
        }
    } == {"deployments/okg-workspace/self_play/reports/b.json"}
    assert any(node.subtype == "self_play_finding" for node in nodes)

    ignored = source.run(
        "source-run-ignored",
        mode="scope_incomplete",
        sync_scope={"scope_keys": [unconfigured.relative_to(repo).as_posix()]},
    )
    assert list(ignored.facts) == []
    assert ignored.completed_scope is False
    assert report_a.is_file()


def _write_self_play_report(
    path: Path,
    *,
    scenario_id: str,
    generation_id: int,
    score: float,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({
            "report_format": "okg-workspace-self-play/v1",
            "deployment": "okg-workspace",
            "generation_id": generation_id,
            "catalog_version_id": 4,
            "created_at": "2026-05-02T21:00:00Z",
            "graph_summary": {"node_count": 100, "edge_count": 200},
            "scenario_results": [
                {
                    "scenario_id": scenario_id,
                    "question_class": "code_lookup",
                    "substrate_stage_target": "read_side_surfacing",
                    "question": "Where is run_sql implemented?",
                    "answerability": {"score": score},
                    "expected_evidence_subtypes": ["code_symbol"],
                    "observed_subtype_counts": {"code_symbol": 1},
                    "evidence_edge_paths": [],
                    "evidence_node_ids": [],
                    "tool_calls": [],
                },
            ],
        }),
        encoding="utf-8",
    )
    return path
